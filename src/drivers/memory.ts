import { SignalingDriver } from './signaling.js';

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
