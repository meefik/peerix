/**
 * Manages event listeners and emits events.
 *
 * Provides a simple event emitter implementation that supports registering
 * event handlers with `on` and `once`, removing handlers with `off`,
 * and emitting events with `emit`. Supports multiple events and handlers,
 * as well as a custom execution context.
 */
export class EventEmitter<T extends { [K in keyof T]: any[] }> extends Map<keyof T, Map<(...args: any[]) => void, boolean>> {
  #context: any;
  #delay: number;

  /**
   * Creates a new EventEmitter instance.
   *
   * @param context Optional context for handler execution.
   * @param options Optional configuration for the emitter.
   * @param options.delay Delay (in milliseconds) for event handler execution.
   */
  constructor(context?: any, options?: { delay?: number; }) {
    super();
    this.#context = context;
    const { delay = 0 } = options || {};
    this.#delay = delay;
  }

  /**
   * Subscribes to one or more events.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
  on<K extends keyof T>(event: K | K[], handler: (...args: T[K]) => void) {
    if (event && handler) {
      const events = Array.isArray(event) ? event : [event];
      for (const ev of events) {
        if (!this.has(ev)) {
          this.set(ev, new Map());
        }
        this.get(ev)?.set(handler, false);
      }
    }
  }

  /**
   * Subscribes to one or more events for a single invocation.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
  once<K extends keyof T>(event: K | K[], handler: (...args: T[K]) => void) {
    if (event && handler) {
      const events = Array.isArray(event) ? event : [event];
      for (const ev of events) {
        if (!this.has(ev)) {
          this.set(ev, new Map());
        }
        this.get(ev)?.set(handler, true);
      }
    }
  }

  /**
   * Unsubscribes from one or more events.
   *
   * @param event Event name or list of event names.
   * @param handler Optional event handler to remove. If not provided, all handlers for the event(s) will be removed.
   */
  off<K extends keyof T>(event: K | K[], handler?: (...args: T[K]) => void) {
    if (event) {
      const events = Array.isArray(event) ? event : [event];
      for (const ev of events) {
        if (this.has(ev)) {
          if (handler) {
            this.get(ev)?.delete(handler);
            if (!this.get(ev)?.size) {
              this.delete(ev);
            }
          }
          else {
            this.get(ev)?.clear();
            this.delete(ev);
          }
        }
      }
    }
  }

  /**
   * Emits one or more events.
   *
   * @param event Event name or list of event names.
   * @param args Arguments to pass to the event handlers.
   */
  emit<K extends keyof T>(event: K | K[], ...args: T[K]) {
    if (event) {
      const events = Array.isArray(event) ? event : [event];
      const context = this.#context || this;
      const delay = this.#delay;
      for (const ev of events) {
        if (this.has(ev)) {
          for (const [handler, once] of this.get(ev) || []) {
            if (once) {
              this.off(ev, handler);
            }
            setTimeout(() => handler.apply(context, args), delay);
          }
        }
      }
    }
  }
}
