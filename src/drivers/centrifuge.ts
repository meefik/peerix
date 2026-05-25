import { Driver } from './driver.js';
import { EventEmitter } from '../utils/emitter.js';

/**
 * Centrifuge-based signaling driver.
 *
 * This driver uses [Centrifugo](https://centrifugal.dev/) as the underlying
 * messaging system, allowing signaling messages to be exchanged through
 * a Centrifuge-based server.
 *
 * > This driver requires the [`centrifuge`](https://www.npmjs.com/package/centrifuge)
 * > module in the browser.
 *
 * @group Drivers
 *
 * @example
 * ```javascript
 * import { Centrifuge } from 'centrifuge';
 *
 * // connect to a Centrifuge server
 * const centrifuge = new Centrifuge('ws://localhost:8000/connection/websocket');
 * centrifuge.connect();
 *
 * // create a new driver instance
 * const driver = new CentrifugeDriver({ centrifuge });
 * ```
 *
 * Running a Centrifugo server locally for testing:
 * ```sh
 * docker run --rm --ulimit nofile=65536:65536 -p 8000:8000 \
 *   -e 'CENTRIFUGO_CLIENT_ALLOWED_ORIGINS=*' \
 *   -e 'CENTRIFUGO_CLIENT_INSECURE=1' \
 *   centrifugo/centrifugo centrifugo
 * ```
 */
export class CentrifugeDriver extends Driver {
  #emitter: EventEmitter<Record<string, [number[]]>>;
  #centrifuge: CentrifugeClient | null;
  #prefix: string;
  #onConnect: () => void;
  #onDisconnect: () => void;
  #onError: (error: unknown) => void;
  #subscriptions: Map<string, CentrifugeSubscription>;

  /**
   * Creates a new instance of the driver.
   *
   * @param options Optional configuration for the driver.
   * @param options.centrifuge The Centrifuge client instance.
   * @param options.prefix Optional prefix for Centrifuge channels.
   */
  constructor(options: { centrifuge: CentrifugeClient; prefix?: string }) {
    super();
    const { centrifuge, prefix = '' } = options || {};

    if (
      !centrifuge ||
      typeof centrifuge.on !== 'function' ||
      typeof centrifuge.off !== 'function' ||
      typeof centrifuge.newSubscription !== 'function' ||
      typeof centrifuge.removeSubscription !== 'function' ||
      typeof centrifuge.publish !== 'function'
    ) {
      throw new TypeError(
        'CentrifugeDriver requires a valid Centrifuge client',
      );
    }

    this.#centrifuge = centrifuge;
    this.#prefix = `${prefix}`;
    this.#emitter = new EventEmitter();
    this.#subscriptions = new Map();

    this.#onConnect = () => {
      this.active = true;
    };

    this.#onDisconnect = () => {
      this.active = false;
    };

    this.#onError = (error: unknown) => {
      this.emit('error', error);
    };

    this.#centrifuge.on('connected', this.#onConnect);
    this.#centrifuge.on('disconnected', this.#onDisconnect);
    this.#centrifuge.on('error', this.#onError);

    this.active = this.#centrifuge.state === 'connected';
  }

  async subscribe(namespace: string[], handler: (data: number[]) => void) {
    if (!this.#centrifuge) return;

    const channel = this.#getChannelName(namespace);
    const isFirstSubscription = !this.#emitter.has(channel);
    this.#emitter.on(channel, handler);

    if (isFirstSubscription) {
      let sub: CentrifugeSubscription | undefined;
      try {
        sub =
          this.#subscriptions.get(channel) ||
          this.#centrifuge.newSubscription(channel);
        this.#subscriptions.set(channel, sub);

        sub.on('publication', (ctx: any) => {
          const data = ctx?.data;
          if (data) this.#emitter.emit(channel, data);
        });
        sub.on('error', (error: unknown) => {
          this.emit('error', error);
        });
        sub.subscribe();
      } catch (error) {
        this.#subscriptions.delete(channel);
        sub?.removeAllListeners();
        this.#centrifuge.removeSubscription(channel);
        this.#emitter.off(channel, handler);
        throw error;
      }
    }
  }

  async unsubscribe(namespace: string[], handler: (data: number[]) => void) {
    if (!this.#centrifuge) return;

    const channel = this.#getChannelName(namespace);
    this.#emitter.off(channel, handler);

    const sub = this.#subscriptions.get(channel);
    if (!this.#emitter.has(channel) && sub) {
      this.#subscriptions.delete(channel);
      sub.unsubscribe();
      sub.removeAllListeners();
      this.#centrifuge.removeSubscription(channel);
    }
  }

  async dispatch(namespace: string[], data: number[]) {
    if (!this.#centrifuge) return;

    const channel = this.#getChannelName(namespace);
    await this.#centrifuge.publish(channel, data);
  }

  destroy() {
    super.destroy();
    this.#emitter.clear();

    if (this.#centrifuge) {
      this.#centrifuge.off('connected', this.#onConnect);
      this.#centrifuge.off('disconnected', this.#onDisconnect);
      this.#centrifuge.off('error', this.#onError);
      for (const [channel, sub] of this.#subscriptions.entries()) {
        sub.unsubscribe();
        sub.removeAllListeners();
        this.#centrifuge.removeSubscription(channel);
      }
      this.#subscriptions.clear();
      this.#centrifuge = null;
    }
  }

  /**
   * Utility method to get the full channel name with prefix.
   *
   * @param namespace The base namespace for the channel.
   * @returns The full channel name with prefix applied.
   */
  #getChannelName(namespace: string[]) {
    const [event] = namespace.slice(-1);
    return `${this.#prefix}${event}`;
  }
}

/**
 * Interface representing a Centrifuge client instance.
 *
 * @internal
 * @group Drivers
 */
export interface CentrifugeClient {
  on: (event: string, handler: (ctx: any) => void) => void;
  off: (event: string, handler: (ctx: any) => void) => void;
  newSubscription: (namespace: string) => CentrifugeSubscription;
  removeSubscription: (namespace: string) => void;
  publish: (namespace: string, data: any) => Promise<void>;
  state?: string;
}

/**
 * Interface representing a Centrifuge subscription.
 *
 * @internal
 * @group Drivers
 */
export interface CentrifugeSubscription {
  on: (event: string, handler: (ctx: any) => void) => void;
  subscribe: () => void;
  unsubscribe: () => void;
  removeAllListeners: () => void;
}
