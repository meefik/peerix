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
 * const driver = new BroadcastChannelDriver('my-channel');
 * ```
 */
export class BroadcastChannelDriver implements SignalingDriver {
  #events: Map<string, Set<(data: any) => void>>;
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
      const { ns, data } = e.data;
      const handlers = this.#events.get(ns);
      if (!ns || !handlers) return;
      for (const handler of handlers) {
        try {
          handler(data);
        }
        catch (err) {
          /* swallow errors */
        }
      }
    };
  }

  on(namespace: string[], handler: (data: any) => void) {
    const ns = namespace.join(':');
    let handlers = this.#events.get(ns);
    if (!handlers) {
      handlers = new Set();
      this.#events.set(ns, handlers);
    }
    handlers.add(handler);
  }

  off(namespace: string[], handler: (data: any) => void) {
    const ns = namespace.join(':');
    const handlers = this.#events.get(ns);
    if (handlers) {
      if (handler) handlers.delete(handler);
      else handlers.clear();
      if (!handlers.size) this.#events.delete(ns);
    }
  }

  emit(namespace: string[], data: any) {
    const ns = namespace.join(':');
    this.#bc.postMessage({ ns, data });
  }
}
