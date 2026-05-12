import { Driver } from './driver.js';

/**
 * Supabase-based signaling driver for distributed communication across multiple
 * browsers and devices.
 *
 * > This driver requires the `@supabase/supabase-js` module in the browser.
 * 
 * @group Drivers
 * 
 * @example
 * ```javascript
 * import { createClient } from '@supabase/supabase-js';
 *
 * // connect to a Supabase server
 * const supabase = createClient('your_project_url', 'your_supabase_api_key');
 * 
 * // create a new driver instance
 * const driver = new SupabaseDriver({ supabase, prefix: 'peerix' });
 * ```
 */
export class SupabaseDriver extends Driver {
  #handlers: Map<string, Set<(payload: number[]) => void>>;
  #prefix: string;
  #supabase: { channel: Function, removeChannel: Function; } | null;
  #channel: { state: string; on: Function, send: Function, subscribe: Function, unsubscribe: Function; } | null;
  #onBroadcast: (message: { payload: [string, any]; }) => void;

  /**
   * Creates a new instance of the driver.
   *
   * @param options Configuration options for the driver.
   * @param options.supabase Supabase client instance for communication.
   * @param options.prefix Optional prefix for channel namespacing (default: 'peerix').
   */
  constructor(options: { supabase: { channel: Function, removeChannel: Function; }; prefix?: string; }) {
    super();
    const { supabase, prefix = 'peerix' } = options || {};

    if (!supabase || typeof supabase.channel !== 'function' || typeof supabase.removeChannel !== 'function') {
      throw new TypeError('SupabaseDriver requires a valid Supabase client instance');
    }

    this.#supabase = supabase;
    this.#prefix = prefix;
    this.#handlers = new Map();

    this.#onBroadcast = (message) => {
      const [namespace, payload] = message.payload || [];
      if (!namespace) return;

      const handlers = this.#handlers.get(namespace);
      if (!handlers?.size) return;

      for (const handler of handlers) {
        setTimeout(() => handler(payload), 0);
      }
    };

    this.#channel = supabase.channel(this.#prefix)
      .on('broadcast', { event: 'message' }, this.#onBroadcast);
  }

  async subscribe(namespace: string[], handler: (payload: number[]) => void) {
    const ns = namespace.join(':');
    let handlers = this.#handlers.get(ns);
    if (!handlers) {
      handlers = new Set();
      this.#handlers.set(ns, handlers);
    }
    handlers.add(handler);

    await this.#subscribeToChannel();
  }

  async unsubscribe(namespace: string[], handler: (payload: number[]) => void) {
    const ns = namespace.join(':');
    const handlers = this.#handlers.get(ns);
    if (handlers) {
      handlers.delete(handler);
      if (!handlers.size) {
        this.#handlers.delete(ns);
      }
    }

    if (!this.#handlers.size) {
      await this.#unsubscribeFromChannel();
    }
  }

  async dispatch(namespace: string[], payload: number[]) {
    const ns = namespace.join(':');

    await this.#channel?.send({
      type: 'broadcast',
      event: 'message',
      payload: [ns, payload],
    });
  }

  /**
   * Cleans up all event listeners and marks the driver as inactive.
   */
  destroy() {
    if (this.#supabase) {
      this.active = false;

      if (this.#channel) {
        this.#unsubscribeFromChannel().catch(() => { });
        this.#supabase.removeChannel(this.#channel);
        this.#channel = null;
      }

      this.#handlers.clear();
      this.#supabase = null;
    }
  }

  /**
   * Subscribes to the Supabase channel for receiving broadcast messages.
   */
  async #subscribeToChannel() {
    if (!this.#channel || this.#channel.state !== 'closed') return;

    await new Promise((resolve, reject) => {
      if (!this.#channel) return resolve(null);
      this.#channel.subscribe((status: string) => {
        if (status === 'SUBSCRIBED') {
          resolve(null);
        }
        else {
          reject(new Error(`Failed to subscribe to Supabase channel: ${status}`));
        }
      });
    });
  }

  /**
   * Unsubscribes from the Supabase channel for receiving broadcast messages.
   */
  async #unsubscribeFromChannel() {
    if (!this.#channel || this.#channel.state === 'closed') return;

    await this.#channel.unsubscribe();
  }
}
