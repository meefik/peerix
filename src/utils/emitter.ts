export default class EventEmitter extends Map {
  context: any;

  constructor(context?: any) {
    super();
    if (context) {
      this.context = context;
    }
  }

  on(event: string | string[], handler: (...args: any[]) => void) {
    if (event && handler) {
      const events = Array.isArray(event) ? event : [event];
      for (let ev of events) {
        if (!this.has(ev)) {
          this.set(ev, new Map());
        }
        this.get(ev).set(handler, false);
      }
    }
  }

  once(event: string | string[], handler: (...args: any[]) => void) {
    if (event && handler) {
      const events = Array.isArray(event) ? event : [event];
      for (let ev of events) {
        if (!this.has(ev)) {
          this.set(ev, new Map());
        }
        this.get(ev).set(handler, true);
      }
    }
  }

  off(event: string | string[], handler?: (...args: any[]) => void) {
    if (event) {
      const events = Array.isArray(event) ? event : [event];
      for (let ev of events) {
        if (this.has(ev)) {
          if (handler) {
            this.get(ev).delete(handler);
            if (!this.get(ev).size) {
              this.delete(ev);
            }
          }
          else {
            this.get(ev).clear();
            this.delete(ev);
          }
        }
      }
    }
  }

  emit(event: string | string[], ...args: any[]) {
    if (event) {
      const events = Array.isArray(event) ? event : [event];
      for (let ev of events) {
        if (this.has(ev)) {
          for (let [handler, once] of this.get(ev)) {
            if (once) {
              this.off(ev, handler);
            }
            try {
              handler.apply(this.context || this, args);
            }
            catch (err) {
              console.error(err);
            }
          }
        }
      }
    }
  }
}
