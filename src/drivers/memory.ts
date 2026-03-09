import { SignalingDriver } from '../types/signaling.js';

/**
 * In-memory signaling driver for intra-process communication.
 *
 * This driver is useful for testing and debugging purposes, but is not suitable
 * for production use due to its limitations (e.g. single-process scope).
 */
export class MemoryDriver extends Map implements SignalingDriver {
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

  emit(namespace: string[], message: any) {
    const ns = namespace.join(':');
    if (!this.has(ns)) return;
    for (const handler of this.get(ns)) {
      try {
        handler(message);
      }
      catch (err) {
        /* swallow errors */
      }
    }
  }
}
