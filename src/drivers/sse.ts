import { Driver } from './driver.js';
import { EventEmitter } from '../utils/emitter.js';

/**
 * Server-Sent Events ([SSE](https://developer.mozilla.org/docs/Web/API/Server-sent_events))
 * signaling driver.
 *
 * SSE is a unidirectional communication protocol that allows servers to push real-time
 * updates to clients over a single HTTP connection. This driver uses SSE to receive
 * updates from the server. To send messages to the server, it uses standard HTTP POST
 * requests. This protocol can be used as an alternative to WebSocket-based transport.
 *
 * > This driver requires a compatible server-side implementation to work.
 *
 * @group Drivers
 *
 * @example
 *
 * Client-side code (browser with SSE support):
 * ```javascript
 * const driver = new SseDriver({
 *   url: 'http://localhost:8080/api/sse',
 *   withCredentials: false
 * });
 * ```
 *
 * Server-side code (Node.js with Express and CORS modules):
 * ```javascript
 * const express = require('express');
 * const cors = require('cors');
 *
 * const app = express();
 * app.use(express.text());
 * app.use(cors({ origin: true, credentials: true }));
 *
 * const subscribers = new Map();
 *
 * // route for outgoing messages
 * app.get('/api/sse', (req, res) => {
 *   const ns = req.query.ns;
 *   if (!ns) return res.status(400).end();
 *   const clients = subscribers.get(ns) || new Set();
 *   clients.add(res);
 *   subscribers.set(ns, clients);
 *   // set headers to establish the SSE stream
 *   res.setHeader('Content-Type', 'text/event-stream');
 *   res.setHeader('Cache-Control', 'no-cache');
 *   res.setHeader('Connection', 'keep-alive');
 *   res.flushHeaders();
 *   // clean up if the browser closes the page or disconnects
 *   req.on('close', () => {
 *     clients.delete(res);
 *     if (!clients.size) subscribers.delete(ns);
 *     res.end();
 *   });
 * });
 *
 * // route for incoming messages
 * app.post('/api/sse', (req, res) => {
 *   const ns = req.query.ns;
 *   const data = req.body || '';
 *   if (ns) {
 *     const clients = subscribers.get(ns);
 *     if (clients) {
 *       for (const client of clients) {
 *         client.write(`data: ${data}\n\n`);
 *       }
 *     }
 *   }
 *   res.status(200).end();
 * });
 *
 * app.listen(8080);
 * ```
 */
export class SseDriver extends Driver {
  #emitter: EventEmitter<Record<string, [number[]]>>;
  #eventSources: Map<string, EventSource>;
  #url: string;
  #withCredentials: boolean;

  /**
   * Creates a new instance of the driver.
   *
   * @param options Optional configuration for the driver.
   * @param options.url URL to connect to via SSE. Defaults to '/api/sse'.
   * @param options.withCredentials Whether to include credentials in requests. Defaults to false.
   */
  constructor(options?: { url?: string; withCredentials?: boolean }) {
    super();
    const { url = '/api/sse', withCredentials = false } = options || {};
    this.#emitter = new EventEmitter();
    this.#url = url;
    this.#withCredentials = withCredentials;
    this.active = false;
    this.#eventSources = new Map();
  }

  async subscribe(namespace: string[], handler: (data: number[]) => void) {
    const ns = this.#getNS(namespace);
    const hasSubscribers = this.#emitter.has(ns);
    this.#emitter.on(ns, handler);
    if (!hasSubscribers) {
      try {
        await this.#createEventSource(ns);
      } catch (error) {
        this.#emitter.off(ns, handler);
        throw error;
      }
    }
  }

  async unsubscribe(namespace: string[], handler: (data: number[]) => void) {
    const ns = this.#getNS(namespace);
    this.#emitter.off(ns, handler);
    const hasSubscribers = this.#emitter.has(ns);
    if (!hasSubscribers) {
      this.#closeEventSource(ns);
    }
  }

  async dispatch(namespace: string[], data: number[]) {
    const ns = this.#getNS(namespace);
    await this.#send(ns, data);
  }

  /**
   * Closes the connection and cleans up resources.
   */
  destroy() {
    for (const ns of this.#eventSources.keys()) {
      this.#closeEventSource(ns);
    }
    this.active = false;
  }

  /**
   * Constructs a namespace string from an array of namespace segments.
   *
   * @param namespace Array of namespace segments.
   * @returns The constructed namespace string.
   */
  #getNS(namespace: string[]): string {
    return namespace.join(',');
  }

  /**
   * Builds a request URL for the given namespace while preserving any existing
   * query parameters on the configured base URL.
   *
   * @param ns The namespace string.
   * @returns The fully qualified request URL.
   */
  #makeUrl(ns: string): string {
    const url = new URL(this.#url, location.href);
    url.searchParams.set('ns', ns);
    return url.toString();
  }

  /**
   * Creates a new EventSource connection for the given namespace.
   *
   * @param ns The namespace string to connect to.
   */
  async #createEventSource(ns: string) {
    return new Promise<void>((resolve) => {
      const eventSource = new EventSource(this.#makeUrl(ns), {
        withCredentials: this.#withCredentials,
      });
      eventSource.onmessage = (event) => {
        try {
          const data = atob(event.data)
            .split('')
            .map((char) => char.charCodeAt(0));
          this.#emitter.emit(ns, data);
        } catch (error) {
          this.emit('error', error);
        }
      };
      eventSource.onerror = (error) => {
        this.emit('error', error);
        if (eventSource.readyState === EventSource.CLOSED) {
          this.active = false;
        }
      };
      eventSource.onopen = () => {
        this.active = true;
        resolve();
      };
      this.#eventSources.set(ns, eventSource);
    });
  }

  /**
   * Closes the EventSource connection for the given namespace.
   *
   * @param ns The namespace string to close the connection for.
   */
  #closeEventSource(ns: string) {
    const eventSource = this.#eventSources.get(ns);
    if (eventSource) {
      eventSource.close();
      this.#eventSources.delete(ns);
    }
  }

  /**
   * Sends a request to the server to subscribe/unsubscribe or dispatch data.
   *
   * @param ns The namespace string.
   * @param data Optional data to send.
   */
  async #send(ns: string, data?: number[]) {
    const body = data
      ? btoa(data.reduce((acc, byte) => acc + String.fromCharCode(byte), ''))
      : '';
    const res = await fetch(this.#makeUrl(ns), {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain' },
      credentials: this.#withCredentials ? 'include' : 'omit',
      body,
    });
    if (!res.ok) {
      throw new Error(`SSE backend error: ${res.statusText}`);
    }
  }
}
