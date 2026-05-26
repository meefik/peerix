import { Driver } from './driver.js';
import { EventEmitter } from '../utils/emitter.js';

/**
 * Supabase-based signaling driver.
 *
 * This driver uses [Supabase Realtime](https://supabase.com/docs/guides/realtime)
 * to relay signaling messages between clients through your Supabase server.
 *
 * > This driver requires the [`@supabase/supabase-js`](https://www.npmjs.com/package/@supabase/supabase-js)
 * > module in the browser.
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
 * const driver = new SupabaseDriver({ supabase });
 * ```
 */
export class SupabaseDriver extends Driver {
  #emitter: EventEmitter<Record<string, [number[]]>>;
  #prefix: string;
  #supabase: SupabaseClient | null;
  #channels: Map<string, SupabaseChannel>;
  #onBroadcast: (message: { payload?: any }) => void;

  /**
   * Creates a new instance of the driver.
   *
   * @param options Configuration options for the driver.
   * @param options.supabase Supabase client instance.
   * @param options.prefix Optional Supabase channel prefix.
   */
  constructor(options: { supabase: SupabaseClient; prefix?: string }) {
    super();
    const { supabase, prefix = '' } = options || {};

    if (
      !supabase ||
      typeof supabase.channel !== 'function' ||
      typeof supabase.removeChannel !== 'function'
    ) {
      throw new TypeError('SupabaseDriver requires a valid Supabase client');
    }

    this.#supabase = supabase;
    this.#prefix = `${prefix}`;
    this.#emitter = new EventEmitter();
    this.#channels = new Map();

    this.#onBroadcast = (message) => {
      const [event, data] = message.payload;
      this.#emitter.emit(event, data);
    };
  }

  async subscribe(namespace: string[], handler: (data: number[]) => void) {
    if (!this.#supabase) return;

    const [channelName] = namespace;
    const [event] = namespace.slice(-1);
    this.#emitter.on(event, handler);

    try {
      this.#subscribeToChannel(channelName);
    } catch (error) {
      this.#emitter.off(event, handler);
      throw error;
    }
  }

  async unsubscribe(namespace: string[], handler: (data: number[]) => void) {
    if (!this.#supabase) return;

    const [channelName] = namespace;
    const [event] = namespace.slice(-1);
    this.#emitter.off(event, handler);

    this.#unsubscribeFromChannel(channelName);
  }

  async dispatch(namespace: string[], data: number[]) {
    if (!this.#supabase) return;

    const [channelName] = namespace;
    const [event] = namespace.slice(-1);

    await this.#sendToChannel(channelName, event, data);
  }

  destroy() {
    super.destroy();
    this.#emitter.clear();

    if (this.#supabase) {
      for (const channel of this.#channels.values()) {
        this.#supabase.removeChannel(channel);
      }
      this.#channels.clear();
      this.#supabase = null;
    }
  }

  /**
   * Subscribes to the Supabase channel for receiving broadcast messages.
   */
  #subscribeToChannel(channelName: string) {
    let channel = this.#channels.get(channelName);
    if (!this.#supabase || channel) return;

    channel = this.#supabase.channel(`${this.#prefix}${channelName}`);
    channel.on('broadcast', { event: 'message' }, this.#onBroadcast);
    channel.subscribe((status) => {
      const ready = this.#isAllChannelsReady();
      if (status === 'SUBSCRIBED') {
        if (ready) this.active = true;
      } else if (status === 'CHANNEL_ERROR' || status === 'TIMED_OUT') {
        const error = new Error(`Supabase channel error: ${status}`);
        this.emit('error', error);
        if (!ready) this.active = false;
      } else if (status === 'CLOSED') {
        if (!ready) this.active = false;
      }
    });
    this.#channels.set(channelName, channel);
  }

  /**
   * Unsubscribes from the Supabase channel used to receive broadcast messages.
   */
  #unsubscribeFromChannel(channelName: string) {
    const channel = this.#channels.get(channelName);
    if (!this.#supabase || !channel) return;

    this.#supabase.removeChannel(channel);
    this.#channels.delete(channelName);
  }

  /**
   * Sends a message to the specified Supabase channel to be broadcasted to other clients.
   *
   * @param channelName The name of the channel to send the message to.
   * @param event The event name to emit.
   * @param data The data payload to send with the event.
   */
  async #sendToChannel(channelName: string, event: string, data: number[]) {
    const channel = this.#channels.get(channelName);
    if (!this.#supabase || !channel) return;

    await channel.send({
      type: 'broadcast',
      event: 'message',
      payload: [event, data],
    });
  }

  /**
   * Checks if all subscribed channels are in the 'joined' state.
   */
  #isAllChannelsReady() {
    let ready = false;
    for (const channel of this.#channels.values()) {
      if (channel.state !== 'joined') return false;
      ready = true;
    }
    return ready;
  }
}

/**
 * Interface representing a Supabase client instance.
 *
 * @internal
 * @group Drivers
 */
export interface SupabaseClient {
  channel: (channelName: string) => SupabaseChannel;
  removeChannel: (channel: SupabaseChannel) => void;
}

/**
 * Interface representing a Supabase Realtime channel.
 *
 * @internal
 * @group Drivers
 */
export interface SupabaseChannel {
  on: (
    event: string,
    filter: { event: string },
    handler: (message: { payload?: [string, number[]] }) => void,
  ) => any;
  send: (payload: {
    type: string;
    event: string;
    payload?: any;
  }) => Promise<void>;
  subscribe: (callback: (status: string) => void) => void;
  unsubscribe: () => Promise<void>;
  state: string;
}
