import type { SignalingDriver } from '../types/signaling.js';

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
export class MemoryDriver implements SignalingDriver {
  #events: Map<string, Set<(data: any) => void>>;
  #delay: number;

  /**
   * Create a new instance of the driver.
   * 
   * @param options Optional configuration for the driver.
   * @param options.delay Delay (in milliseconds) for message delivery to simulate network latency. The delay will be a random value between 75% and 125% of the specified delay.
   */
  constructor(options?: { delay?: number }) {
    this.#events = new Map();
    this.#delay = options?.delay || 0;
  }

  on(namespace: string[], handler: (data: any) => void) {
    const ns = namespace.join(':') as string;
    let handlers = this.#events.get(ns);
    if (!handlers) {
      handlers = new Set();
      this.#events.set(ns, handlers);
    }
    handlers.add(handler);
  }

  off(namespace: string[], handler: (data: any) => void) {
    const ns = namespace.join(':') as string;
    const handlers = this.#events.get(ns);
    if (handlers) {
      if (handler) handlers.delete(handler);
      else handlers.clear();
      if (!handlers.size) this.#events.delete(ns);
    }
  }

  emit(namespace: string[], message: any) {
    const ns = namespace.join(':');
    const handlers = this.#events.get(ns);
    if (!handlers) return;
    for (const handler of handlers) {
      const delay = ~~(0.5 * Math.random() * this.#delay + 0.75 * this.#delay);
      setTimeout(() => handler(message), delay);
    }
  }
}
