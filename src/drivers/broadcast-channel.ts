import type { SignalingDriver } from '../types/signaling.js';

/**
 * BroadcastChannel-based signaling driver for intra-origin communication.
 *
 * This driver is useful for testing and debugging purposes, but is not suitable
 * for production use due to its limitations (e.g. same-origin restriction).
 */
export class BroadcastChannelDriver extends Map implements SignalingDriver {
  /**
   * BroadcastChannel instance used for message exchange.
   */
  readonly bc: BroadcastChannel;

  /**
   * Create a new BroadcastChannelDriver instance.
   *
   * @param channelName Optional BroadcastChannel name (defaults to 'peerix').
   */
  constructor(channelName: string) {
    super();
    this.bc = new BroadcastChannel(channelName || 'peerix');
    this.bc.onmessage = (e) => {
      const { ns, data } = e.data;
      if (!ns || !this.has(ns)) return;
      for (const handler of this.get(ns)) {
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
    if (!this.has(ns)) this.set(ns, new Set());
    this.get(ns).add(handler);
  }

  off(namespace: string[], handler: (data: any) => void) {
    const ns = namespace.join(':');
    if (this.has(ns)) {
      if (handler) this.get(ns).delete(handler);
      else this.get(ns).clear();
      if (!this.get(ns).size) this.delete(ns);
    }
  }

  emit(namespace: string[], data: any) {
    const ns = namespace.join(':');
    this.bc.postMessage({ ns, data });
  }
}
