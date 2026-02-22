export class MemoryDriver extends Map {
  on(namespace, handler) {
    const ns = namespace.join(':');
    if (!this.has(ns)) this.set(ns, new Set());
    this.get(ns).add(handler);
  }

  off(namespace, handler) {
    const ns = namespace.join(':');
    if (this.has(ns)) {
      if (handler) this.get(ns).delete(handler);
      else this.get(ns).clear();
      if (!this.get(ns).size) this.delete(ns);
    }
  }

  emit(namespace, message) {
    const ns = namespace.join(':');
    if (!this.has(ns)) return;
    for (const handler of this.get(ns)) {
      try {
        handler(message);
      }
      // eslint-disable-next-line no-unused-vars
      catch (err) {
        /* swallow errors */
      }
    }
  }
}
