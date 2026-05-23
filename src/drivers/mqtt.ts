import { Driver } from './driver.js';
import { EventEmitter } from '../utils/emitter.js';

/**
 * MQTT-based signaling driver.
 *
 * This driver uses [MQTT](https://mqtt.org/) as the underlying messaging
 * system, allowing signaling messages to be exchanged through an MQTT broker
 * such as [Mosquitto](https://mosquitto.org/).
 *
 * > This driver requires the [`mqtt`](https://www.npmjs.com/package/mqtt)
 * > module in the browser.
 *
 * @group Drivers
 *
 * @example
 * ```javascript
 * import { connect } from 'mqtt';
 *
 * // connect to an MQTT server (e.g., the public test server)
 * const client = connect('wss://test.mosquitto.org:8081/mqtt');
 *
 * // create a new driver instance
 * const driver = new MqttDriver({ client, prefix: 'peerix' });
 * ```
 */
export class MqttDriver extends Driver {
  #emitter: EventEmitter<Record<string, [number[]]>>;
  #client: {
    on: Function;
    off: Function;
    subscribe: Function;
    unsubscribe: Function;
    publish: Function;
    connected?: boolean;
  } | null;
  #prefix: string;
  #onConnect: () => void;
  #onDisconnect: () => void;
  #onMessage: (namespace: string, payload: ArrayLike<number>) => void;
  #onError: (error: unknown) => void;

  /**
   * Creates a new instance of the driver.
   *
   * @param options Optional configuration for the driver.
   * @param options.client The MQTT client instance.
   * @param options.prefix An optional prefix for MQTT topics (default: 'peerix').
   */
  constructor(options: {
    client: {
      on: Function;
      off: Function;
      subscribe: Function;
      unsubscribe: Function;
      publish: Function;
      connected?: boolean;
    };
    prefix?: string;
  }) {
    super();
    const { client, prefix = 'peerix' } = options || {};

    if (
      !client ||
      typeof client.on !== 'function' ||
      typeof client.off !== 'function' ||
      typeof client.subscribe !== 'function' ||
      typeof client.unsubscribe !== 'function' ||
      typeof client.publish !== 'function'
    ) {
      throw new TypeError('MqttDriver requires a valid MQTT client instance');
    }

    this.#client = client;
    this.#prefix = String(prefix);
    this.#emitter = new EventEmitter();

    this.#onConnect = () => {
      this.active = true;
      // restore broker subscriptions after reconnecting
      for (const ns of this.#emitter.keys()) {
        this.#client?.subscribe(ns, (error: unknown) => {
          if (error) this.emit('error', error);
        });
      }
    };

    this.#onDisconnect = () => {
      this.active = false;
    };

    this.#onError = (error: unknown) => {
      this.emit('error', error);
    };

    this.#onMessage = (namespace, payload) => {
      this.#emitter.emit(namespace, Array.from(payload));
    };

    this.#client.on('connect', this.#onConnect);
    this.#client.on('reconnect', this.#onConnect);
    this.#client.on('offline', this.#onDisconnect);
    this.#client.on('close', this.#onDisconnect);
    this.#client.on('error', this.#onError);
    this.#client.on('message', this.#onMessage);

    this.active = !!this.#client.connected;
  }

  async subscribe(namespace: string[], handler: (data: number[]) => void) {
    const ns = this.#getNS(namespace);
    const isFirstSubscription = !this.#emitter.has(ns);
    this.#emitter.on(ns, handler);

    if (isFirstSubscription) {
      await new Promise<void>((resolve, reject) => {
        if (!this.#client) return resolve();
        this.#client.subscribe(ns, (error: unknown) => {
          if (error) {
            this.#emitter.off(ns, handler);
            reject(error);
          } else {
            resolve();
          }
        });
      });
    }
  }

  async unsubscribe(namespace: string[], handler: (data: number[]) => void) {
    const ns = this.#getNS(namespace);
    this.#emitter.off(ns, handler);

    if (!this.#emitter.has(ns)) {
      await new Promise<void>((resolve, reject) => {
        if (!this.#client) return resolve();
        this.#client.unsubscribe(ns, (error: unknown) => {
          if (error) reject(error);
          else resolve();
        });
      });
    }
  }

  async dispatch(namespace: string[], data: number[]) {
    const ns = this.#getNS(namespace);
    await new Promise<void>((resolve, reject) => {
      if (!this.#client) return resolve();
      this.#client.publish(ns, new Uint8Array(data), (error: unknown) => {
        if (!error) {
          resolve();
          return;
        }
        reject(error);
      });
    });
  }

  /**
   * Destroys the driver instance, cleaning up any resources.
   */
  destroy() {
    if (this.#client) {
      this.active = false;
      this.#emitter.clear();
      this.#client.off('connect', this.#onConnect);
      this.#client.off('reconnect', this.#onConnect);
      this.#client.off('offline', this.#onDisconnect);
      this.#client.off('close', this.#onDisconnect);
      this.#client.off('error', this.#onError);
      this.#client.off('message', this.#onMessage);
      this.#client = null;
    }
  }

  /**
   * Constructs a namespace string from an array of namespace segments.
   *
   * @param namespace Array of namespace segments.
   * @returns Constructed namespace string.
   */
  #getNS(namespace: string[]): string {
    return [this.#prefix, ...namespace].filter(Boolean).join('/');
  }
}
