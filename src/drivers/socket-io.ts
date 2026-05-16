import { Driver } from './driver.js';
import { EventEmitter } from '../utils/emitter.js';

/**
 * Socket.IO-based signaling driver for distributed communication across multiple
 * browsers and devices.
 * 
 * This driver uses [Socket.IO](https://socket.io/) to relay signaling messages 
 * between clients through your own WebSocket server.
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
 *   socket.on('peerix:dispatch', (namespace, data) => {
 *     socket.broadcast.to(namespace).emit('peerix:message', namespace, data);
 *   });
 * });
 * ```
 */
export class SocketIoDriver extends Driver {
  #emitter: EventEmitter<Record<string, [number[]]>>;
  #socket: { on: Function; off: Function; emit: Function; connected: boolean; } | null;
  #prefix: string;
  #onConnect: () => void;
  #onDisconnect: () => void;
  #onMessage: (namespace: string, data: number[]) => void;
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
    this.#prefix = String(prefix);
    this.#emitter = new EventEmitter();

    this.#onConnect = () => {
      this.active = true;
      // re-subscribe to all namespaces to restore message flow after reconnecting
      const event = this.#getNS('subscribe');
      for (const namespace of this.#emitter.keys()) {
        this.#socket?.emit(event, namespace, () => { });
      }
    };

    this.#onDisconnect = () => {
      this.active = false;
    };

    this.#onError = (error: unknown) => {
      this.emit('error', error);
    };

    this.#onMessage = (namespace, data) => {
      this.#emitter.emit(namespace, data);
    };

    this.#socket.on('connect', this.#onConnect);
    this.#socket.on('disconnect', this.#onDisconnect);
    this.#socket.on('connect_error', this.#onError);
    this.#socket.on('error', this.#onError);
    this.#socket.on(this.#getNS('message'), this.#onMessage);

    this.active = !!this.#socket.connected;
  }

  async subscribe(namespace: string[], handler: (data: number[]) => void) {
    const ns = this.#getNS(...namespace);
    const isFirstSubscription = !this.#emitter.has(ns);
    this.#emitter.on(ns, handler);

    if (isFirstSubscription) {
      await new Promise(resolve => {
        if (!this.#socket) return resolve(null);
        this.#socket.emit(this.#getNS('subscribe'), ns, () => resolve(null));
      });
    }
  }

  async unsubscribe(namespace: string[], handler: (data: number[]) => void) {
    const ns = this.#getNS(...namespace);
    this.#emitter.off(ns, handler);

    if (!this.#emitter.has(ns)) {
      await new Promise(resolve => {
        if (!this.#socket) return resolve(null);
        this.#socket.emit(this.#getNS('unsubscribe'), ns, () => resolve(null));
      });
    }
  }

  async dispatch(namespace: string[], data: number[]) {
    const ns = this.#getNS(...namespace);
    this.#socket?.emit(this.#getNS('dispatch'), ns, data);
  }

  /**
   * Cleans up all event listeners and marks the driver as inactive.
   */
  destroy() {
    if (this.#socket) {
      this.active = false;
      this.#emitter.clear();
      this.#socket.off('connect', this.#onConnect);
      this.#socket.off('disconnect', this.#onDisconnect);
      this.#socket.off('connect_error', this.#onError);
      this.#socket.off('error', this.#onError);
      this.#socket.off(this.#getNS('message'), this.#onMessage);
      this.#socket = null;
    }
  }

  /**
   * Constructs a namespace string from an array of namespace segments.
   * 
   * @param namespaces Array of namespace segments.
   * @returns Constructed namespace string.
   */
  #getNS(...namespaces: string[]) {
    return [this.#prefix, ...namespaces].filter(Boolean).join(':');
  }
}
