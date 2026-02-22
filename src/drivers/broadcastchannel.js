export class BroadcastChannelDriver extends Map {
  constructor(channelName) {
    super();
    this.bc = new BroadcastChannel(channelName || 'peerix');
    this.bc.onmessage = (e) => {
      const { ns, data } = e.data;
      if (!ns || !this.has(ns)) return;
      for (const handler of this.get(ns)) {
        try {
          handler(data);
        }
        // eslint-disable-next-line no-unused-vars
        catch (err) {
          /* swallow errors */
        }
      }
    };
  }

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

  emit(namespace, data) {
    const ns = namespace.join(':');
    this.bc.postMessage({ ns, data });
  }
}
