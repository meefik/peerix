import { Driver } from './driver.js';

/**
 * BroadcastChannel-based signaling driver for intra-origin communication.
 *
 * This driver is useful for testing and debugging purposes, but is not suitable
 * for production use due to its limitations (e.g. same-origin restriction).
 * 
 * @group Drivers
 * @example
 * ```javascript
 * const driver = new BroadcastChannelDriver('peerix');
 * ```
 */
export class BroadcastChannelDriver extends Driver {
  #handlers: Map<string, Set<(message?: any) => void>>;
  #bc: BroadcastChannel;

  /**
   * Create a new instance of the driver.
   *
   * @param channelName Optional BroadcastChannel name (defaults to 'peerix').
   */
  constructor(channelName: string) {
    super();
    this.#handlers = new Map();
    this.#bc = new BroadcastChannel(channelName || 'peerix');
    this.#bc.onmessage = (e) => {
      const [ns, message] = e.data;
      const handlers = this.#handlers.get(ns);
      if (!ns || !handlers) return;
      for (const handler of handlers) {
        setTimeout(() => handler(message), 0);
      }
    };
  }

  async subscribe(namespace: string[], handler: (message?: any) => void) {
    const ns = namespace.join(':');
    let handlers = this.#handlers.get(ns);
    if (!handlers) {
      handlers = new Set();
      this.#handlers.set(ns, handlers);
    }
    handlers.add(handler);
  }

  async unsubscribe(namespace: string[], handler: (message?: any) => void) {
    const ns = namespace.join(':');
    const handlers = this.#handlers.get(ns);
    if (handlers) {
      handlers.delete(handler);
      if (!handlers.size) {
        this.#handlers.delete(ns);
      }
    }
  }

  async dispatch(namespace: string[], message?: any) {
    const ns = namespace.join(':');
    this.#bc.postMessage([ns, message]);
  }
}
