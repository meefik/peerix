import type { SignalingDriver } from '../types/signaling.js';

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
export class BroadcastChannelDriver implements SignalingDriver {
  #events: Map<string, Set<(message?: any) => void>>;
  #bc: BroadcastChannel;

  /**
   * Create a new instance of the driver.
   *
   * @param channelName Optional BroadcastChannel name (defaults to 'peerix').
   */
  constructor(channelName: string) {
    this.#events = new Map();
    this.#bc = new BroadcastChannel(channelName || 'peerix');
    this.#bc.onmessage = (e) => {
      const { ns, msg } = e.data;
      const handlers = this.#events.get(ns);
      if (!ns || !handlers) return;
      for (const handler of handlers) {
        setTimeout(() => handler(msg), 0);
      }
    };
  }

  on(namespace: string[], handler: (message?: any) => void) {
    const ns = namespace.join(':');
    let handlers = this.#events.get(ns);
    if (!handlers) {
      handlers = new Set();
      this.#events.set(ns, handlers);
    }
    handlers.add(handler);
  }

  off(namespace: string[], handler: (message?: any) => void) {
    const ns = namespace.join(':');
    const handlers = this.#events.get(ns);
    if (handlers) {
      handlers.delete(handler);
      if (!handlers.size) {
        this.#events.delete(ns);
      }
    }
  }

  emit(namespace: string[], message?: any) {
    const ns = namespace.join(':');
    const [event] = namespace;
    if (event === 'message') {
      this.#bc.postMessage({ ns, msg: message });
    }
    else {
      const handlers = this.#events.get(ns);
      if (handlers) {
        for (const handler of handlers) {
          setTimeout(() => handler(message), 0);
        }
      }
    }
  }
}
