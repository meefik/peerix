import { Driver } from './driver.js';
import { EventEmitter } from '../utils/emitter.js';

/**
 * Supabase-based signaling driver for distributed communication across multiple
 * browsers and devices.
 * 
 * This driver uses [Supabase Realtime](https://supabase.com/docs/guides/realtime) 
 * to relay signaling messages between clients through your Supabase server.
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
  #emitter: EventEmitter<{ [namespace: string]: [number[]]; }>;
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
    this.#prefix = String(prefix);
    this.#emitter = new EventEmitter();

    this.#onBroadcast = (message) => {
      const [ns, payload] = message.payload || [];
      if (!ns) return;
      this.#emitter.emit(ns, payload);
    };

    this.#channel = supabase.channel(this.#prefix)
      .on('broadcast', { event: 'message' }, this.#onBroadcast);
  }

  async subscribe(namespace: string[], handler: (payload: number[]) => void) {
    const ns = this.#getNS(namespace);
    this.#emitter.on(ns, handler);

    await this.#subscribeToChannel();
  }

  async unsubscribe(namespace: string[], handler: (payload: number[]) => void) {
    const ns = this.#getNS(namespace);
    this.#emitter.off(ns, handler);

    if (!this.#emitter.size) {
      await this.#unsubscribeFromChannel();
    }
  }

  async dispatch(namespace: string[], payload: number[]) {
    const ns = this.#getNS(namespace);

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
      this.#emitter.clear();

      if (this.#channel) {
        this.#unsubscribeFromChannel().catch(() => { });
        this.#supabase.removeChannel(this.#channel);
        this.#channel = null;
      }

      this.#supabase = null;
    }
  }

  /**
   * Constructs a namespace string from an array of namespace segments.
   * 
   * @param namespace Array of namespace segments.
   * @returns Constructed namespace string.
   */
  #getNS(namespace: string[]) {
    return namespace.join(':');
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
