import { Driver } from './driver.js';

/**
 * In-memory signaling driver for intra-process communication.
 *
 * This driver is useful for testing and debugging purposes, but is not suitable
 * for production use due to its limitations (e.g. single-process scope).
 * 
 * @group Drivers
 * @example
 * ```javascript
 * const driver = new MemoryDriver();
 * ```
 */
export class MemoryDriver extends Driver {
  #handlers: Map<string, Set<(message: Uint8Array) => void>>;
  #delay: number;

  /**
   * Creates a new instance of the driver.
   * 
   * @param options Optional configuration for the driver.
   * @param options.delay Delay (in milliseconds) for message delivery to simulate network latency. The delay will be a random value between 75% and 125% of the specified delay.
   */
  constructor(options?: { delay?: number; }) {
    super();
    this.#handlers = new Map();
    this.#delay = options?.delay || 0;
  }

  async subscribe(namespace: string[], handler: (message: Uint8Array) => void) {
    const ns = namespace.join(':');
    let handlers = this.#handlers.get(ns);
    if (!handlers) {
      handlers = new Set();
      this.#handlers.set(ns, handlers);
    }
    handlers.add(handler);
  }

  async unsubscribe(namespace: string[], handler: (message: Uint8Array) => void) {
    const ns = namespace.join(':');
    const handlers = this.#handlers.get(ns);
    if (handlers) {
      handlers.delete(handler);
      if (!handlers.size) {
        this.#handlers.delete(ns);
      }
    }
  }

  async dispatch(namespace: string[], message: Uint8Array) {
    const ns = namespace.join(':');
    const handlers = this.#handlers.get(ns);
    if (!handlers) return;
    for (const handler of handlers) {
      const delay = ~~(0.5 * Math.random() * this.#delay + 0.75 * this.#delay);
      setTimeout(() => handler(message), delay);
    }
  }
}
