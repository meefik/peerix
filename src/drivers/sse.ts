import { Driver } from "./driver.js";
import { EventEmitter } from "../utils/emitter.js";

/**
 * Server-Sent Events ([SSE](https://developer.mozilla.org/docs/Web/API/Server-sent_events))
 * signaling driver.
 *
 * SSE is a unidirectional communication protocol that allows servers to push real-time
 * updates to clients over a single HTTP connection. This driver uses SSE to receive
 * updates from the server. To send messages to the server, it uses standard HTTP POST
 * requests. This protocol can be used as an alternative to WebSocket-based transport.
 *
 * ```mermaid
 * sequenceDiagram
 *   participant A as Peer A
 *   participant S as SSE Server
 *   participant B as Peer B
 *   A->>S: GET /.well-known/mercure?topic=... (open SSE stream)
 *   B->>S: GET /.well-known/mercure?topic=... (open SSE stream)
 *   A->>S: POST /.well-known/mercure?topic=... (signal payload)
 *   S-->>B: SSE event: data: payload
 *   B->>S: POST /.well-known/mercure?topic=... (signal payload)
 *   S-->>A: SSE event: data: payload
 * ```
 *
 * > This driver requires a [Mercure](https://mercure.rocks/) server or compatible
 * > server-side implementation to work.
 *
 * @group Drivers
 *
 * @example
 *
 * Client-side code (browser with SSE support):
 * ```js
 * const publisherJwtKey = "mercure-publisher-jwt-key";
 * const driver = new SseDriver({
 *   url: "http://localhost:8080/.well-known/mercure",
 *   publisher: {
 *     headers: {
 *       Authorization: `Bearer ${publisherJwtKey}`,
 *     },
 *   },
 * });
 * ```
 *
 * You can use this JWT publisher key with the Mercure server's default configuration for testing purposes:
 * ```
 * eyJhbGciOiJIUzI1NiJ9.eyJtZXJjdXJlIjp7InB1Ymxpc2giOlsiKiJdLCJzdWJzY3JpYmUiOlsiKiJdfX0.bVXdlWXwfw9ySx7-iV5OpUSHo34RkjUdVzDLBcc6l_g
 * ```
 *
 * Running a local Mercure server for testing:
 * ```sh
 * docker run --rm -p 8080:80 \
 *   -e SERVER_NAME=':80' \
 *   -e MERCURE_PUBLISHER_JWT_KEY='!ChangeThisMercureHubJWTSecretKey!' \
 *   -e MERCURE_SUBSCRIBER_JWT_KEY='!ChangeThisMercureHubJWTSecretKey!' \
 *   dunglas/mercure:latest caddy run --config /etc/caddy/dev.Caddyfile
 * ```
 *
 * Instead of using Mercure, you can use a Node.js server:
 * ```js
 * const express = require("express");
 * const cors = require("cors");
 *
 * const app = express();
 * app.use(express.urlencoded({ extended: true }));
 * app.use(cors({ origin: true, credentials: true }));
 *
 * const namespaces = new Map();
 *
 * // route for outgoing messages (subscribe/stream)
 * app.get("/.well-known/mercure", (req, res) => {
 *   const { topic } = req.query;
 *   if (!topic) return res.status(400).end();
 *   // support multiple topics in a single request
 *   const topics = Array.isArray(topic) ? topic : [topic];
 *   for (const t of topics) {
 *     const clients = namespaces.get(t) || new Set();
 *     namespaces.set(t, clients);
 *     clients.add(res);
 *   }
 *   // set headers to establish the SSE stream
 *   res.setHeader("Content-Type", "text/event-stream");
 *   res.setHeader("Cache-Control", "no-cache");
 *   res.setHeader("Connection", "keep-alive");
 *   res.flushHeaders();
 *   // clean up if the browser closes the page or disconnects
 *   req.on("close", () => {
 *     for (const t of topics) {
 *       const clients = namespaces.get(t);
 *       if (clients) {
 *         clients.delete(res);
 *         if (!clients.size) namespaces.delete(t);
 *       }
 *     }
 *     res.end();
 *   });
 * });
 *
 * // route for incoming messages (publish)
 * app.post("/.well-known/mercure", (req, res) => {
 *   const { topic, data = "" } = req.body || {};
 *   if (topic) {
 *     const topics = Array.isArray(topic) ? topic : [topic];
 *     for (const t of topics) {
 *       const clients = namespaces.get(t);
 *       if (clients) {
 *         for (const client of clients) {
 *           client.write(`data: ${data}\n\n`);
 *         }
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
  #subscriberOptions: EventSourceInit;
  #publisherOptions: RequestInit;

  /**
   * Creates a new instance of the driver.
   *
   * @param options Optional configuration for the driver.
   * @param options.url URL to connect to via SSE. Defaults to "/.well-known/mercure".
   * @param options.subscriber Subscriber options for the EventSource instance.
   * @param options.publisher Publisher options for the HTTP requests (fetch).
   */
  constructor(options?: {
    url?: string;
    subscriber: EventSourceInit;
    publisher: RequestInit;
  }) {
    super();
    const {
      url = "/.well-known/mercure",
      subscriber = {},
      publisher = {},
    } = options ?? {};
    this.#emitter = new EventEmitter();
    this.#url = url;
    this.#eventSources = new Map();
    this.#subscriberOptions = subscriber;
    this.#publisherOptions = publisher;
  }

  override async subscribe(
    namespace: string,
    handler: (data: number[]) => void,
  ): Promise<void> {
    super.subscribe(namespace, handler);

    if (!this.#emitter.has(namespace)) {
      await this.#createEventSource(namespace);
    }

    this.#emitter.on(namespace, handler);
  }

  override async unsubscribe(
    namespace: string,
    handler: (data: number[]) => void,
  ): Promise<void> {
    super.unsubscribe(namespace, handler);

    this.#emitter.off(namespace, handler);

    if (!this.#emitter.has(namespace)) {
      this.#closeEventSource(namespace);
    }
  }

  override async publish(namespace: string, data: number[]): Promise<void> {
    super.publish(namespace, data);

    await this.#send(namespace, data);
  }

  override destroy(): void {
    super.destroy();

    this.#emitter.clear();

    for (const topic of this.#eventSources.keys()) {
      this.#closeEventSource(topic);
    }
  }

  /**
   * Builds a request URL for the given topic while preserving any existing
   * query parameters on the configured base URL.
   *
   * @param topic The topic string.
   * @returns The fully qualified request URL.
   */
  #makeUrl(topic: string): string {
    const url = new URL(this.#url, location.href);
    url.searchParams.append("topic", topic);
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
   * Creates a new EventSource connection for the given topic.
   *
   * @param topic The topic string to connect to.
   */
  async #createEventSource(topic: string): Promise<void> {
    let opened = false;
    await new Promise<void>((resolve, reject) => {
      const eventSource = new EventSource(
        this.#makeUrl(topic),
        this.#subscriberOptions,
      );
      eventSource.onmessage = (e) => {
        try {
          const data = atob(e.data)
            .split("")
            .map((char) => char.charCodeAt(0));
          this.#emitter.emit(topic, data);
        } catch (error) {
          this.emit("error", error);
        }
      };
      eventSource.onerror = (error) => {
        if (!opened && eventSource.readyState === EventSource.CLOSED) {
          reject(error);
        } else {
          this.emit("error", error);
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
      this.#eventSources.set(topic, eventSource);
    });
  }

  /**
   * Closes the EventSource connection for the given topic.
   *
   * @param topic The topic string to close the connection for.
   */
  #closeEventSource(topic: string): void {
    const eventSource = this.#eventSources.get(topic);
    if (eventSource) {
      eventSource.close();
      this.#eventSources.delete(topic);
      this.active = this.#isOpen();
    }
  }

  /**
   * Sends data to the server via an HTTP POST request.
   *
   * @param topic The topic string.
   * @param data Optional data to send.
   */
  async #send(topic: string, data?: number[]): Promise<void> {
    const res = await fetch(this.#makeUrl(topic), {
      ...this.#publisherOptions,
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        ...this.#publisherOptions?.headers,
      },
      body: new URLSearchParams({
        topic,
        data: data
          ? btoa(
              data.reduce((acc, byte) => acc + String.fromCharCode(byte), ""),
            )
          : "",
      }),
    });
    if (!res.ok) {
      throw new Error(`SSE backend error: ${res.statusText} (${res.status})`);
    }
  }
}
