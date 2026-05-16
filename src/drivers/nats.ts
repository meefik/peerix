import { Driver } from './driver.js';
import { EventEmitter } from '../utils/emitter.js';

/**
 * NATS-based signaling driver for distributed communication across multiple 
 * browsers and devices.
 *
 * This driver uses [NATS](https://nats.io/) as the underlying messaging system, 
 * allowing for distributed signaling across multiple browsers and devices. 
 * 
 * > This driver requires the `@nats-io/nats-core` module for WebSocket-based 
 * > NATS connections directly in the browser.
 * 
 * @group Drivers
 * 
 * @example
 * ```javascript
 * import { wsconnect } from '@nats-io/nats-core';
 *
 * // connect to a NATS server (e.g. the public demo server) 
 * const nc = await wsconnect({ servers: ['wss://demo.nats.io:8443'], noEcho: true });
 * 
 * // create a new driver instance
 * const driver = new NatsDriver({ nc, prefix: 'peerix' });
 * ```
 */
export class NatsDriver extends Driver {
  #emitter: EventEmitter<Record<string, [number[]]>>;
  #subscriptions: Map<string, { unsubscribe: () => void; }>;
  #prefix: string;
  #nc: { subscribe: Function; publish: Function; status: Function; } | null;
  #statusIterator: AsyncIterator<any> | null;

  /**
   * Creates a new instance of the driver.
   *
   * @param options Configuration options for the driver.
   * @param options.nc A NATS connection instance.
   * @param options.prefix An optional prefix for NATS subjects (default: 'peerix').
   */
  constructor(options: { nc: { subscribe: Function; publish: Function; status: Function; }; prefix?: string; }) {
    super();
    const { nc, prefix = 'peerix' } = options || {};

    if (!nc || typeof nc.subscribe !== 'function'
      || typeof nc.publish !== 'function' || typeof nc.status !== 'function') {
      throw new TypeError('NatsDriver requires a valid NATS client connection');
    }

    this.#nc = nc;
    this.#prefix = String(prefix);
    this.#emitter = new EventEmitter();
    this.#subscriptions = new Map();
    this.#statusIterator = null;
    this.#trackConnectionStatus();
  }

  async subscribe(namespace: string[], handler: (data: number[]) => void) {
    const ns = this.#getNS(namespace);
    this.#emitter.on(ns, handler);

    if (!this.#subscriptions.has(ns)) {
      const sub = this.#nc?.subscribe(ns, {
        callback: (error: Error, msg: any) => {
          if (error) return this.emit('error', error);
          this.#emitter.emit(ns, msg.data);
        },
      });
      if (sub) {
        this.#subscriptions.set(ns, sub);
      }
    }
  }

  async unsubscribe(namespace: string[], handler: (data: number[]) => void) {
    const ns = this.#getNS(namespace);
    this.#emitter.off(ns, handler);

    const sub = this.#subscriptions.get(ns);
    if (sub) {
      sub.unsubscribe();
      this.#subscriptions.delete(ns);
    }
  }

  async dispatch(namespace: string[], data: number[]) {
    const ns = this.#getNS(namespace);
    this.#nc?.publish(ns, new Uint8Array(data));
  }

  /**
   * Cleans up all event listeners and marks the driver as inactive.
   */
  destroy() {
    if (this.#nc) {
      this.active = false;
      this.#statusIterator?.return?.();
      this.#statusIterator = null;
      this.#emitter.clear();
      this.#subscriptions.forEach(sub => sub.unsubscribe());
      this.#subscriptions.clear();
      this.#nc = null;
    }
  }

  /**
   * Constructs a namespace string from an array of namespace segments.
   * 
   * @param namespace Array of namespace segments.
   * @returns Constructed namespace string.
   */
  #getNS(namespace: string[]) {
    return [this.#prefix, ...namespace].filter(Boolean).join('.');
  }

  /**
   * Listens for NATS connection status events.
   *
   * The NATS client exposes an async-iterable status stream. We keep
   * a reference to the iterator so we can cancel it during `destroy()`.
   */
  async #trackConnectionStatus() {
    try {
      const statusIterable = this.#nc?.status?.();
      if (!statusIterable || typeof statusIterable[Symbol.asyncIterator] !== 'function') return;
      this.#statusIterator = statusIterable[Symbol.asyncIterator]();
      for await (const s of statusIterable as AsyncIterable<any>) {
        if (!this.#nc) break;
        if (s.type === 'reconnect') this.active = true;
        else if (s.type === 'disconnect') this.active = false;
        else if (s.type === 'error') this.emit('error', s.data);
      }
    }
    catch (err) {
      this.emit('error', err);
    }
    finally {
      this.#statusIterator = null;
    }
  }
}
