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
 * // connect to an MQTT server (e.g., the local server)
 * const client = connect('ws://localhost:9001/mqtt');
 *
 * // create a new driver instance
 * const driver = new MqttDriver({ client });
 * ```
 *
 * Running a local MQTT broker with WebSocket support for testing:
 * ```sh
 * cat << EOF > /tmp/mosquitto.conf
 * listener 9001
 * protocol websockets
 * allow_anonymous true
 * EOF
 *
 * docker run --rm -p 1883:1883 -p 9001:9001 \
 *   -v /tmp/mosquitto.conf:/mosquitto.conf \
 *   eclipse-mosquitto:latest \
 *   mosquitto -c /mosquitto.conf
 * ```
 */
export class MqttDriver extends Driver {
  #emitter: EventEmitter<Record<string, [number[]]>>;
  #client: MqttClient | null;
  #prefix: string;
  #onConnect: (connack: any) => void;
  #onDisconnect: () => void;
  #onMessage: (topic: string, payload: ArrayLike<number>) => void;
  #onError: (error: unknown) => void;

  /**
   * Creates a new instance of the driver.
   *
   * @param options Optional configuration for the driver.
   * @param options.client The MQTT client instance.
   * @param options.prefix Optional prefix for MQTT topics.
   */
  constructor(options: { client: MqttClient; prefix?: string }) {
    super();
    const { client, prefix = '' } = options || {};

    if (
      !client ||
      typeof client.on !== 'function' ||
      typeof client.off !== 'function' ||
      typeof client.subscribe !== 'function' ||
      typeof client.unsubscribe !== 'function' ||
      typeof client.publish !== 'function'
    ) {
      throw new TypeError('MqttDriver requires a valid MQTT client');
    }

    this.#client = client;
    this.#prefix = `${prefix}`;
    this.#emitter = new EventEmitter();

    this.#onConnect = (connack) => {
      if (!connack.sessionPresent) {
        void this.#restoreSubscriptions();
      } else {
        this.active = true;
      }
    };

    this.#onDisconnect = () => {
      this.active = false;
    };

    this.#onError = (error: unknown) => {
      this.emit('error', error);
    };

    this.#onMessage = (topic, payload) => {
      this.#emitter.emit(topic, Array.from(payload));
    };

    this.#client.on('connect', this.#onConnect);
    this.#client.on('offline', this.#onDisconnect);
    this.#client.on('end', this.#onDisconnect);
    this.#client.on('error', this.#onError);
    this.#client.on('message', this.#onMessage);

    this.active = !!this.#client.connected;
  }

  async subscribe(namespace: string[], handler: (data: number[]) => void) {
    if (!this.#client) return;

    const topic = this.#getTopic(namespace);
    const isFirstSubscription = !this.#emitter.has(topic);
    this.#emitter.on(topic, handler);

    if (isFirstSubscription) {
      try {
        await this.#mqttSubscribe(topic);
      } catch (error) {
        this.#emitter.off(topic, handler);
        throw error;
      }
    }
  }

  async unsubscribe(namespace: string[], handler: (data: number[]) => void) {
    if (!this.#client) return;

    const topic = this.#getTopic(namespace);
    this.#emitter.off(topic, handler);

    if (!this.#emitter.has(topic)) {
      await this.#mqttUnsubscribe(topic);
    }
  }

  async dispatch(namespace: string[], data: number[]) {
    if (!this.#client) return;

    const topic = this.#getTopic(namespace);
    await this.#mqttPublish(topic, new Uint8Array(data));
  }

  destroy() {
    super.destroy();

    const topics = Array.from(this.#emitter.keys());
    this.#emitter.clear();

    if (this.#client) {
      this.#client.off('connect', this.#onConnect);
      this.#client.off('offline', this.#onDisconnect);
      this.#client.off('end', this.#onDisconnect);
      this.#client.off('error', this.#onError);
      this.#client.off('message', this.#onMessage);
      for (const topic of topics) {
        this.#mqttUnsubscribe(topic).catch(() => {});
      }
      this.#client = null;
    }
  }

  /**
   * Generates the full MQTT topic name for a given namespace.
   *
   * @param namespace The namespace to generate the topic for.
   * @returns The full MQTT topic name with the prefix applied.
   */
  #getTopic(namespace: string[]) {
    const [event] = namespace.slice(-1);
    return `${this.#prefix}${event}`;
  }

  /**
   * Re-subscribe the MQTT client to all topics currently tracked in the
   * internal emitter. Sets `active` to true on success, emits `error` and
   * sets `active` to false on failure.
   */
  async #restoreSubscriptions() {
    try {
      await Promise.all(
        Array.from(this.#emitter.keys()).map((topic) =>
          this.#mqttSubscribe(topic),
        ),
      );
      this.active = true;
    } catch (error) {
      this.emit('error', error);
      this.active = false;
    }
  }

  /**
   * Subscribe the underlying client to a specific topic.
   * Resolves when the subscribe completes or rejects on error.
   *
   * @param topic MQTT topic to subscribe to.
   */
  async #mqttSubscribe(topic: string) {
    await new Promise<void>((resolve, reject) => {
      if (!this.#client) return resolve();
      this.#client.subscribe(topic, { nl: true }, (error: unknown) => {
        if (error) reject(error);
        else resolve();
      });
    });
  }

  /**
   * Unsubscribe the underlying client from a specific topic.
   * Resolves when the unsubscribe completes or rejects on error.
   *
   * @param topic MQTT topic to unsubscribe from.
   */
  async #mqttUnsubscribe(topic: string) {
    await new Promise<void>((resolve, reject) => {
      if (!this.#client) return resolve();
      this.#client.unsubscribe(topic, (error: unknown) => {
        if (error) reject(error);
        else resolve();
      });
    });
  }

  /**
   * Publish a message to a specific MQTT topic.
   *
   * @param topic MQTT topic to publish to.
   * @param payload The message payload as a Uint8Array.
   */
  async #mqttPublish(topic: string, payload: Uint8Array) {
    await new Promise<void>((resolve, reject) => {
      if (!this.#client) return resolve();
      this.#client.publish(topic, payload, (error: unknown) => {
        if (error) reject(error);
        else resolve();
      });
    });
  }
}

/**
 * Interface representing a minimal MQTT client instance.
 *
 * @internal
 * @group Drivers
 */
export interface MqttClient {
  on: (event: string, handler: (...args: any[]) => void) => void;
  off: (event: string, handler: (...args: any[]) => void) => void;
  subscribe: (
    topic: string,
    options: { nl?: boolean },
    callback: (error: unknown) => void,
  ) => void;
  unsubscribe: (topic: string, callback: (error: unknown) => void) => void;
  publish: (
    topic: string,
    payload: Uint8Array,
    callback: (error: unknown) => void,
  ) => void;
  connected?: boolean;
}
