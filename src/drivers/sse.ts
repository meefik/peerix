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
 *   const { event } = req.query;
 *   if (!event) return res.status(400).end();
 *   const clients = subscribers.get(event) || new Set();
 *   clients.add(res);
 *   subscribers.set(event, clients);
 *   // set headers to establish the SSE stream
 *   res.setHeader('Content-Type', 'text/event-stream');
 *   res.setHeader('Cache-Control', 'no-cache');
 *   res.setHeader('Connection', 'keep-alive');
 *   res.flushHeaders();
 *   // clean up if the browser closes the page or disconnects
 *   req.on('close', () => {
 *     clients.delete(res);
 *     if (!clients.size) subscribers.delete(event);
 *     res.end();
 *   });
 * });
 *
 * // route for incoming messages
 * app.post('/api/sse', (req, res) => {
 *   const { event } = req.query;
 *   const data = req.body || '';
 *   if (event) {
 *     const clients = subscribers.get(event);
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
    this.#eventSources = new Map();
  }

  async subscribe(namespace: string[], handler: (data: number[]) => void) {
    const [event] = namespace.slice(-1);
    const hasSubscribers = this.#emitter.has(event);
    this.#emitter.on(event, handler);
    if (!hasSubscribers) {
      await this.#createEventSource(event);
    }
  }

  async unsubscribe(namespace: string[], handler: (data: number[]) => void) {
    const [event] = namespace.slice(-1);
    this.#emitter.off(event, handler);
    const hasSubscribers = this.#emitter.has(event);
    if (!hasSubscribers) {
      this.#closeEventSource(event);
    }
  }

  async dispatch(namespace: string[], data: number[]) {
    const [event] = namespace.slice(-1);
    await this.#send(event, data);
  }

  destroy() {
    super.destroy();
    this.#emitter.clear();

    for (const event of this.#eventSources.keys()) {
      this.#closeEventSource(event);
    }
  }

  /**
   * Builds a request URL for the given event while preserving any existing
   * query parameters on the configured base URL.
   *
   * @param event The event string.
   * @returns The fully qualified request URL.
   */
  #makeUrl(event: string): string {
    const url = new URL(this.#url, location.href);
    url.searchParams.set('event', event);
    return url.toString();
  }

  /**
   * Checks if all EventSource connections are open.
   *
   * @returns True if all EventSource connections are open, false otherwise.
   */
  #isOpen(): boolean {
    const eventSources = Array.from(this.#eventSources.values());
    return (
      eventSources.length > 0 &&
      eventSources.every((es) => es.readyState === EventSource.OPEN)
    );
  }

  /**
   * Creates a new EventSource connection for the given event.
   *
   * @param event The event string to connect to.
   */
  async #createEventSource(event: string) {
    let opened = false;
    await new Promise<void>((resolve, reject) => {
      const eventSource = new EventSource(this.#makeUrl(event), {
        withCredentials: this.#withCredentials,
      });
      eventSource.onmessage = (e) => {
        try {
          const data = atob(e.data)
            .split('')
            .map((char) => char.charCodeAt(0));
          this.#emitter.emit(event, data);
        } catch (error) {
          this.emit('error', error);
        }
      };
      eventSource.onerror = (error) => {
        if (!opened && eventSource.readyState === EventSource.CLOSED) {
          reject(error);
        } else {
          this.emit('error', error);
        }
        const open = this.#isOpen();
        if (!open) this.active = false;
      };
      eventSource.onopen = () => {
        if (!opened) resolve();
        opened = true;
        const open = this.#isOpen();
        if (open) this.active = true;
      };
      this.#eventSources.set(event, eventSource);
    });
  }

  /**
   * Closes the EventSource connection for the given event.
   *
   * @param event The event string to close the connection for.
   */
  #closeEventSource(event: string) {
    const eventSource = this.#eventSources.get(event);
    if (eventSource) {
      eventSource.close();
      this.#eventSources.delete(event);
      this.active = this.#isOpen();
    }
  }

  /**
   * Sends a request to the server to subscribe/unsubscribe or dispatch data.
   *
   * @param event The event string.
   * @param data Optional data to send.
   */
  async #send(event: string, data?: number[]) {
    const body = data
      ? btoa(data.reduce((acc, byte) => acc + String.fromCharCode(byte), ''))
      : '';
    const res = await fetch(this.#makeUrl(event), {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain' },
      credentials: this.#withCredentials ? 'include' : 'omit',
      body,
    });
    if (!res.ok) {
      throw new Error(`SSE backend error: ${res.statusText} (${res.status})`);
    }
  }
}
