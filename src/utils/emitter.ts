/**
 * EventEmitter class for managing event listeners and emitting events.
 *
 * This class provides a simple implementation of an event emitter that allows
 * registering event handlers with `on` and `once`, removing handlers with `off`,
 * and emitting events with `emit`. It supports multiple events and handlers, as
 * well as a context for handler execution.
 */
export default class EventEmitter extends Map {
  /**
   * Optional context for handler execution. If set, handlers will be invoked with
   * this context instead of the emitter instance.
   */
  readonly context: any;

  /**
   * Create a new EventEmitter instance.
   *
   * @param context Optional context for handler execution.
   */
  constructor(context?: any) {
    super();
    if (context) {
      this.context = context;
    }
  }

  /**
   * Subscribe to one or more events.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
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

  /**
   * Subscribe to one or more events for a single invocation.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
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

  /**
   * Unsubscribe from one or more events.
   *
   * @param event Event name or list of event names.
   * @param handler Optional event handler to remove. If not provided, all handlers for the event(s) will be removed.
   */
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

  /**
   * Emit one or more events.
   *
   * @param event Event name or list of event names.
   * @param args Arguments to pass to the event handlers.
   */
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
