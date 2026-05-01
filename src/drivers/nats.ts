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
 * const driver = new NatsDriver({
 *   connect: async () => await connect({ servers: ['wss://demo.nats.io:8443'] }),
 *   prefix: 'peerix',
 * });
 * 
 * await driver.open();
 * ```
 */
export class NatsDriver extends Driver {
  #handlers: Map<string, Map<(message: Uint8Array) => void, any>>;
  #connect: (config?: any) => Promise<any>;
  #nc?: any;
  #prefix: string;

  /**
   * Create a new instance of the driver.
   *
   * @param options Configuration options for the driver.
   * @param options.connect A function that returns a promise resolving to a NATS connection instance.
   * @param options.prefix An optional prefix for NATS subjects.
   */
  constructor(options: { connect: (config?: any) => Promise<any>; prefix?: string; }) {
    super();
    const { connect, prefix = '' } = options || {};
    this.#connect = connect;
    this.#prefix = prefix;
    this.#handlers = new Map();
    this.active = false;
  }

  /**
   * Opens the connection to the NATS server.
   * 
   * @param config Optional configuration options.
   */
  async open(config?: any) {
    if (this.#nc) return;
    this.#nc = await this.#connect(config);
    this.#trackConnectionStatus();
    this.emit('active');
  }

  /**
   * Closes the connection to the NATS server.
   */
  async close() {
    if (!this.#nc) return;
    await this.#nc.close();
    this.#nc = undefined;
    if (this.active) {
      this.emit('inactive');
    }
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
        if (s.type === 'reconnect') {
          this.emit('active');
        }
        if (s.type === 'disconnect') {
          this.emit('inactive');
        }
      }
    }
    catch (error) {
      this.emit('error', error);
    }
  }
}
