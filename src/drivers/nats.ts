import { Driver } from './driver.js';

/**
 * NATS-based signaling driver for inter-process communication.
 *
 * This driver uses [NATS](https://nats.io/) as the underlying messaging system, 
 * allowing for distributed signaling across multiple browsers and devices. 
 * 
 * > This driver requires the `nats.ws` library for WebSocket-based NATS connections 
 * > directly in the browser.
 * 
 * @group Drivers
 * @example
 * ```javascript
 * import { connect } from 'https://esm.sh/nats.ws';
 *
 * // connect to a NATS server (e.g. the public demo server) 
 * const nc = await connect({ servers: ['wss://demo.nats.io:8443'] });
 * 
 * // create a new driver instance and start it
 * const driver = new NatsDriver({ nc, prefix: 'peerix' });
 * driver.start();
 * ```
 */
export class NatsDriver extends Driver {
  #handlers: Map<string, Map<(message: Uint8Array) => void, any>>;
  #nc: any;
  #prefix: string;
  #started = false;

  /**
   * Creates a new instance of the driver.
   *
   * @param options Configuration options for the driver.
   * @param options.nc A NATS connection instance.
   * @param options.prefix An optional prefix for NATS subjects.
   */
  constructor(options: { nc: any; prefix?: string; }) {
    super();
    const { nc, prefix = '' } = options || {};
    this.#nc = nc;
    this.#prefix = prefix;
    this.#handlers = new Map();
    this.active = false;
  }

  /**
   * Starts the driver and begins tracking NATS connection status.
   */
  async start() {
    if (this.#started) return;
    this.#started = true;
    this.#trackConnectionStatus();
    this.active = true;
  }

  /**
   * Stops the driver.
   */
  async stop() {
    if (!this.#started) return;
    this.#started = false;
    this.active = false;
  }

  async subscribe(namespace: string[], handler: (message: Uint8Array) => void) {
    const subject = this.#getSubject(namespace);
    const sub = this.#nc.subscribe(subject, {
      callback: async (error: Error, msg: any) => {
        if (error) return this.emit('error', error);
        setTimeout(() => handler(msg.data), 0);
      },
    });
    const ns = namespace.join(':');
    let handlers = this.#handlers.get(ns);
    if (!handlers) {
      handlers = new Map();
      this.#handlers.set(ns, handlers);
    }
    handlers.set(handler, sub);
  }

  async unsubscribe(namespace: string[], handler: (message: Uint8Array) => void) {
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

  async dispatch(namespace: string[], message: Uint8Array) {
    const subject = this.#getSubject(namespace);
    this.#nc.publish(subject, message);
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
      for await (const s of this.#nc.status()) {
        if (!this.#started) break;
        if (s.type === 'reconnect') {
          this.active = true;
        }
        if (s.type === 'disconnect') {
          this.active = false;
        }
      }
    }
    catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      this.emit('error', error);
    }
  }
}
