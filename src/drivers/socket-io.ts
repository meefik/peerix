import { Driver } from './driver.js';

/**
 * Socket.IO-based signaling driver for distributed communication across multiple
 * browsers and devices.
 *
 * Expected Socket.IO events:
 * - Client -> Server: `prefix:subscribe`, `prefix:unsubscribe`, `prefix:dispatch`
 * - Server -> Client: `prefix:message`
 *
 * > This driver requires the `socket.io-client` module in the browser, 
 * > and the `socket.io` module for server-side implementation in Node.js.
 * 
 * @group Drivers
 * 
 * @example
 * 
 * Client-side code (browser with Socket.IO client):
 * ```javascript
 * import { io } from 'socket.io-client';
 *
 * // connect to a Socket.IO server (e.g. at localhost:8080)
 * const socket = io('http://localhost:8080');
 * 
 * // create a new driver instance
 * const driver = new SocketIoDriver({ socket, prefix: 'peerix' });
 * ```
 * 
 * Server-side code (Node.js with Socket.IO):
 * ```javascript
 * const { Server } = require('socket.io');
 * const io = new Server(8080, { cors: { origin: '*' } });
 *
 * io.on('connection', (socket) => {
 *   socket.on('peerix:subscribe', (namespace, callback) => {
 *     socket.join(namespace);
 *     callback();
 *   });
 *
 *   socket.on('peerix:unsubscribe', (namespace, callback) => {
 *     socket.leave(namespace);
 *     callback();
 *   });
 *
 *   socket.on('peerix:dispatch', (namespace, payload) => {
 *     socket.broadcast.to(namespace).emit('peerix:message', namespace, payload);
 *   });
 * });
 * ```
 */
export class SocketIoDriver extends Driver {
  #handlers: Map<string, Set<(payload: number[]) => void>>;
  #socket: { on: Function; off: Function; emit: Function; connected: boolean; } | null;
  #prefix: string;
  #onConnect: () => void;
  #onDisconnect: () => void;
  #onMessage: (namespace: string, payload: any) => void;
  #onError: (error: unknown) => void;

  /**
   * Creates a new instance of the driver.
   *
   * @param options Configuration options for the driver.
   * @param options.socket Socket.IO client instance.
   * @param options.prefix Optional namespace prefix for event names (default: 'peerix').
   */
  constructor(options: { socket: { on: Function; off: Function; emit: Function; connected: boolean; }; prefix?: string; }) {
    super();
    const { socket, prefix = 'peerix' } = options || {};

    if (!socket || typeof socket.on !== 'function'
      || typeof socket.off !== 'function' || typeof socket.emit !== 'function') {
      throw new TypeError('SocketIoDriver requires a valid Socket.IO client');
    }

    this.#socket = socket;
    this.#prefix = prefix;
    this.#handlers = new Map();

    this.#onConnect = () => {
      this.active = true;
      // re-subscribe to all namespaces to restore message flow after reconnecting
      const event = this.#getNamespace('subscribe');
      for (const namespace of this.#handlers.keys()) {
        this.#socket?.emit(event, namespace, () => { });
      }
    };

    this.#onDisconnect = () => {
      this.active = false;
    };

    this.#onError = (error: unknown) => {
      this.emit('error', error);
    };

    this.#onMessage = (namespace, payload) => {
      const handlers = this.#handlers.get(namespace);
      if (!handlers?.size) return;

      for (const handler of handlers) {
        setTimeout(() => handler(payload), 0);
      }
    };

    this.#socket.on('connect', this.#onConnect);
    this.#socket.on('disconnect', this.#onDisconnect);
    this.#socket.on('connect_error', this.#onError);
    this.#socket.on('error', this.#onError);
    this.#socket.on(this.#getNamespace('message'), this.#onMessage);

    this.active = !!this.#socket.connected;
  }

  async subscribe(namespace: string[], handler: (payload: number[]) => void) {
    const ns = this.#getNamespace(...namespace);
    let handlers = this.#handlers.get(ns);
    const isFirstSubscription = !handlers;
    if (!handlers) {
      handlers = new Set();
      this.#handlers.set(ns, handlers);
    }
    handlers.add(handler);

    if (isFirstSubscription) {
      await new Promise(resolve => {
        if (!this.#socket) return resolve(null);
        this.#socket.emit(this.#getNamespace('subscribe'), ns, () => resolve(null));
      });
    }
  }

  async unsubscribe(namespace: string[], handler: (payload: number[]) => void) {
    const ns = this.#getNamespace(...namespace);
    const handlers = this.#handlers.get(ns);
    if (handlers) {
      handlers.delete(handler);
      if (!handlers.size) {
        this.#handlers.delete(ns);

        await new Promise(resolve => {
          if (!this.#socket) return resolve(null);
          this.#socket.emit(this.#getNamespace('unsubscribe'), ns, () => resolve(null));
        });
      }
    }
  }

  async dispatch(namespace: string[], payload: number[]) {
    const ns = this.#getNamespace(...namespace);
    this.#socket?.emit(this.#getNamespace('dispatch'), ns, payload);
  }

  /**
   * Cleans up all event listeners and marks the driver as inactive.
   */
  destroy() {
    if (this.#socket) {
      this.active = false;
      this.#socket.off('connect', this.#onConnect);
      this.#socket.off('disconnect', this.#onDisconnect);
      this.#socket.off('connect_error', this.#onError);
      this.#socket.off('error', this.#onError);
      this.#socket.off(this.#getNamespace('message'), this.#onMessage);
      this.#handlers.clear();
      this.#socket = null;
    }
  }

  /**
   * Builds a full event name by combining the prefix with the provided namespace segments.
   * 
   * @param namespaces Segments to combine with the prefix for the event name.
   * @returns The combined event name string.
   */
  #getNamespace(...namespaces: string[]) {
    return [this.#prefix, ...namespaces].filter(Boolean).join(':');
  }
}
