import { Driver } from "./driver.js";
import { EventEmitter } from "../utils/emitter.js";

/**
 * Socket.IO-based signaling driver.
 *
 * This driver uses [Socket.IO](https://socket.io/) to relay signaling messages
 * between clients via your own WebSocket server.
 *
 * Expected Socket.IO events:
 * - Client -> Server: `prefix:subscribe`, `prefix:unsubscribe`, `prefix:publish`
 * - Server -> Client: `prefix:message`
 *
 * > This driver requires the [`socket.io-client`](https://www.npmjs.com/package/socket.io-client)
 * > module in the browser and the [`socket.io`](https://www.npmjs.com/package/socket.io)
 * > module for server-side use in Node.js.
 *
 * @group Drivers
 *
 * @example
 *
 * Client-side code (browser with Socket.IO client):
 * ```js
 * import { io } from "socket.io-client";
 *
 * // connect to a Socket.IO server (e.g. at localhost:8080)
 * const socket = io("http://localhost:8080");
 *
 * // create a new driver instance
 * const driver = new SocketIoDriver({ socket, prefix: "peerix:" });
 * ```
 *
 * Server-side code (Node.js with Socket.IO):
 * ```js
 * const { Server } = require("socket.io");
 * const io = new Server(8080, { cors: { origin: "*" } });
 *
 * io.on("connection", (socket) => {
 *   socket.on("peerix:subscribe", (namespace, callback) => {
 *     socket.join(namespace);
 *     if (callback) callback();
 *   });
 *
 *   socket.on("peerix:unsubscribe", (namespace, callback) => {
 *     socket.leave(namespace);
 *     if (callback) callback();
 *   });
 *
 *   socket.on("peerix:publish", (namespace, data, callback) => {
 *     socket.broadcast.to(namespace).emit("peerix:message", namespace, data);
 *     if (callback) callback();
 *   });
 * });
 * ```
 */
export class SocketIoDriver extends Driver {
  #emitter: EventEmitter<Record<string, [number[]]>>;
  #socket: SocketIoClient | null;
  #prefix: string;
  #ackTimeout: number;
  #onConnect: () => void;
  #onDisconnect: () => void;
  #onMessage: (namespace: string, data: number[]) => void;
  #onError: (error: unknown) => void;

  /**
   * Creates a new instance of the driver.
   *
   * @param options Configuration options for the driver.
   * @param options.socket Socket.IO socket instance.
   * @param options.prefix Optional prefix for event names.
   * @param options.ackTimeout Optional timeout for acknowledgements (default: 10000ms).
   */
  constructor(options: {
    socket: SocketIoClient;
    prefix?: string;
    ackTimeout?: number;
  }) {
    super();
    const { socket, prefix = "", ackTimeout = 10000 } = options ?? {};

    if (
      !socket ||
      typeof socket.on !== "function" ||
      typeof socket.off !== "function" ||
      typeof socket.emit !== "function" ||
      typeof socket.timeout !== "function"
    ) {
      throw new TypeError("SocketIoDriver requires a valid Socket.IO client");
    }

    this.#socket = socket;
    this.#prefix = `${prefix}`;
    this.#ackTimeout = Number(ackTimeout);
    this.#emitter = new EventEmitter();

    this.#onConnect = async () => {
      // resubscribe to all namespaces after socket reconnects
      try {
        await Promise.all(
          Array.from(this.#emitter.keys()).map((event) =>
            this.#socketEmit("subscribe", event),
          ),
        );
        this.active = true;
      } catch (error) {
        this.emit("error", error);
        this.active = false;
      }
    };

    this.#onDisconnect = () => {
      this.active = false;
    };

    this.#onError = (error: unknown) => {
      this.emit("error", error);
    };

    this.#onMessage = (namespace, data) => {
      this.#emitter.emit(namespace, data);
    };

    this.#socket.on("connect", this.#onConnect);
    this.#socket.on("disconnect", this.#onDisconnect);
    this.#socket.on("connect_error", this.#onError);
    this.#socket.on("error", this.#onError);
    this.#socket.on(`${this.#prefix}message`, this.#onMessage);

    this.active = !!this.#socket.connected;
  }

  override async subscribe(
    namespace: string[],
    handler: (data: number[]) => void,
  ): Promise<void> {
    if (!this.#socket) return;

    const [event] = namespace.slice(-1);
    const isFirstSubscription = !this.#emitter.has(event);
    this.#emitter.on(event, handler);

    if (isFirstSubscription) {
      try {
        await this.#socketEmit("subscribe", event);
      } catch (error) {
        this.#emitter.off(event, handler);
        throw error;
      }
    }
  }

  override async unsubscribe(
    namespace: string[],
    handler: (data: number[]) => void,
  ): Promise<void> {
    if (!this.#socket) return;

    const [event] = namespace.slice(-1);
    this.#emitter.off(event, handler);

    if (!this.#emitter.has(event)) {
      await this.#socketEmit("unsubscribe", event);
    }
  }

  override async publish(namespace: string[], data: number[]): Promise<void> {
    if (!this.#socket) return;

    const [event] = namespace.slice(-1);
    await this.#socketEmit("publish", event, data);
  }

  override destroy(): void {
    super.destroy();
    this.#emitter.clear();

    if (this.#socket) {
      this.#socket.off("connect", this.#onConnect);
      this.#socket.off("disconnect", this.#onDisconnect);
      this.#socket.off("connect_error", this.#onError);
      this.#socket.off("error", this.#onError);
      this.#socket.off(`${this.#prefix}message`, this.#onMessage);
      this.#socket = null;
    }
  }

  /**
   * Emits an event to the Socket.IO server with acknowledgement support.
   *
   * @param event The event name to emit.
   * @param args The arguments to send with the event.
   */
  async #socketEmit(event: string, ...args: any[]): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      if (!this.#socket) return resolve();
      const socket =
        this.#ackTimeout > 0
          ? this.#socket.timeout(this.#ackTimeout)
          : this.#socket;
      socket.emit(`${this.#prefix}${event}`, ...args, (error: unknown) => {
        if (error) reject(error);
        else resolve();
      });
    });
  }
}

/**
 * Interface representing a Socket.IO client instance.
 *
 * @internal
 * @group Drivers
 */
export interface SocketIoClient {
  on: (event: string, handler: (...args: any[]) => void) => void;
  off: (event: string, handler: (...args: any[]) => void) => void;
  emit: (event: string, ...args: any[]) => void;
  timeout: (ms: number) => SocketIoClient;
  connected: boolean;
}
