import { Driver } from './driver.js';

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
  #handlers: Map<string, Map<(payload: number[]) => void, { unsubscribe: () => void; }>>;
  #nc: { subscribe: Function; publish: Function; status: Function; } | null;
  #prefix: string;
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
    this.#prefix = prefix;
    this.#handlers = new Map();
    this.#statusIterator = null;
    this.#trackConnectionStatus();
  }

  async subscribe(namespace: string[], handler: (payload: number[]) => void) {
    const ns = namespace.join(':');
    let handlers = this.#handlers.get(ns);
    if (!handlers) {
      handlers = new Map();
      this.#handlers.set(ns, handlers);
    }

    if (handlers.has(handler)) {
      return;
    }

    const subject = this.#getSubject(namespace);
    const sub = this.#nc?.subscribe(subject, {
      callback: (error: Error, msg: any) => {
        if (error) return this.emit('error', error);
        setTimeout(() => handler(msg.data), 0);
      },
    });

    handlers.set(handler, sub);
  }

  async unsubscribe(namespace: string[], handler: (payload: number[]) => void) {
    const ns = namespace.join(':');
    const handlers = this.#handlers.get(ns);
    if (handlers) {
      const sub = handlers.get(handler);
      handlers.delete(handler);
      if (!handlers?.size) {
        this.#handlers.delete(ns);
      }
      if (sub) {
        sub.unsubscribe();
      }
    }
  }

  async dispatch(namespace: string[], payload: number[]) {
    const subject = this.#getSubject(namespace);
    this.#nc?.publish(subject, payload);
  }

  /**
   * Cleans up all event listeners and marks the driver as inactive.
   */
  destroy() {
    if (this.#nc) {
      this.active = false;
      this.#statusIterator?.return?.();
      this.#statusIterator = null;
      this.#handlers.forEach((subs) => subs.forEach((sub) => sub.unsubscribe()));
      this.#handlers.clear();
      this.#nc = null;
    }
  }

  /**
   * Constructs the NATS subject for a given namespace.
   * 
   * @param namespace The namespace array to construct the subject from.
   * @returns The constructed NATS subject string.
   */
  #getSubject(namespace: string[]) {
    return [this.#prefix, ...namespace].filter(Boolean).join('.');
  }

  /**
   * Listens for NATS connection status events.
   */
  async #trackConnectionStatus() {
    try {
      this.#statusIterator = this.#nc?.status()[Symbol.asyncIterator]();
      for await (const s of { [Symbol.asyncIterator]: () => this.#statusIterator! }) {
        if (!this.#nc) break;
        if (s.type === 'reconnect') {
          this.active = true;
        }
        if (s.type === 'disconnect') {
          this.active = false;
        }
        if (s.type === 'error') {
          this.emit('error', s.data);
        }
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
