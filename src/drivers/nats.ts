import { Driver } from "./driver.js";
import { EventEmitter } from "../utils/emitter.js";

/**
 * NATS-based signaling driver.
 *
 * This driver uses [NATS](https://nats.io/) as the underlying messaging system,
 * enabling distributed signaling across multiple browsers and devices.
 *
 * > This driver requires the [`@nats-io/nats-core`](https://www.npmjs.com/package/@nats-io/nats-core)
 * > module for WebSocket-based NATS connections directly in the browser.
 *
 * @group Drivers
 *
 * @example
 * ```js
 * import { wsconnect } from "@nats-io/nats-core";
 *
 * // connect to a NATS server (e.g., the local server)
 * const nc = await wsconnect({ servers: ["ws://localhost:8080"], noEcho: true });
 *
 * // create a new driver instance
 * const driver = new NatsDriver({ nc });
 * ```
 *
 * Running a local NATS server with WebSocket support for testing:
 * ```sh
 * cat << EOF > /tmp/nats-server.conf
 * websocket: {
 *   port: 8080,
 *   no_tls: true,
 *   same_origin: false
 * }
 * EOF
 *
 * docker run --rm -p 4222:4222 -p 8080:8080 \
 *   -v /tmp/nats-server.conf:/nats-server.conf \
 *   nats:latest -c /nats-server.conf
 * ```
 */
export class NatsDriver extends Driver {
  #emitter: EventEmitter<Record<string, [number[]]>>;
  #subscriptions: Map<string, NatsSubscription>;
  #prefix: string;
  #nc: NatsConnection | null;
  #statusIterator: AsyncIterator<any> | null;

  /**
   * Creates a new instance of the driver.
   *
   * @param options Configuration options for the driver.
   * @param options.nc NATS connection instance.
   * @param options.prefix Optional prefix for NATS subjects.
   */
  constructor(options: { nc: NatsConnection; prefix?: string }) {
    super();
    const { nc, prefix = "" } = options ?? {};

    if (
      !nc ||
      typeof nc.subscribe !== "function" ||
      typeof nc.publish !== "function" ||
      typeof nc.status !== "function"
    ) {
      throw new TypeError("NatsDriver requires a valid NATS connection");
    }

    this.#nc = nc;
    this.#prefix = `${prefix}`;
    this.#emitter = new EventEmitter();
    this.#subscriptions = new Map();
    this.#statusIterator = null;
    this.#trackConnectionStatus();
    this.active = true;
  }

  override async subscribe(
    namespace: string[],
    handler: (data: number[]) => void,
  ): Promise<void> {
    if (!this.#nc) return;

    const subject = this.#getSubject(namespace);
    this.#emitter.on(subject, handler);

    if (!this.#subscriptions.has(subject)) {
      try {
        const sub = this.#nc.subscribe(subject, {
          callback: (error: Error, msg: any) => {
            if (error) return this.emit("error", error);
            this.#emitter.emit(subject, Array.from(msg?.data ?? []));
          },
        });
        this.#subscriptions.set(subject, sub);
      } catch (error) {
        this.#emitter.off(subject, handler);
        throw error;
      }
    }
  }

  override async unsubscribe(
    namespace: string[],
    handler: (data: number[]) => void,
  ): Promise<void> {
    if (!this.#nc) return;

    const subject = this.#getSubject(namespace);
    this.#emitter.off(subject, handler);

    const sub = this.#subscriptions.get(subject);
    if (!this.#emitter.has(subject) && sub) {
      this.#subscriptions.delete(subject);
      sub.unsubscribe();
    }
  }

  override async publish(namespace: string[], data: number[]): Promise<void> {
    if (!this.#nc) return;

    const subject = this.#getSubject(namespace);
    this.#nc.publish(subject, new Uint8Array(data));
  }

  override destroy(): void {
    super.destroy();
    this.#emitter.clear();

    if (this.#nc) {
      this.#statusIterator?.return?.();
      this.#statusIterator = null;
      this.#subscriptions.forEach((sub) => sub.unsubscribe());
      this.#subscriptions.clear();
      this.#nc = null;
    }
  }

  /**
   * Constructs the full NATS subject name for a given namespace.
   *
   * @param namespace The namespace to construct the subject for.
   * @returns The full NATS subject name with the prefix applied.
   */
  #getSubject(namespace: string[]): string {
    const [event] = namespace.slice(-1);
    return `${this.#prefix}${event}`;
  }

  /**
   * Listens for NATS connection status events.
   *
   * The NATS client exposes an async-iterable status stream. We keep
   * a reference to the iterator so we can cancel it during `destroy()`.
   */
  async #trackConnectionStatus(): Promise<void> {
    try {
      const statusIterable = this.#nc?.status?.();
      if (
        !statusIterable ||
        typeof statusIterable[Symbol.asyncIterator] !== "function"
      )
        return;
      this.#statusIterator = statusIterable[Symbol.asyncIterator]();
      while (this.#statusIterator) {
        const { value: s, done } = await this.#statusIterator.next();
        if (done || !this.#nc) break;
        if (s.type === "reconnect") this.active = true;
        else if (s.type === "disconnect") this.active = false;
        else if (s.type === "error") this.emit("error", s.data);
      }
    } catch (err) {
      this.emit("error", err);
    } finally {
      this.#statusIterator = null;
    }
  }
}

/**
 * Interface representing a NATS client instance.
 *
 * @internal
 * @group Drivers
 */
export interface NatsConnection {
  subscribe: (
    subject: string,
    options: { callback: (error: Error, msg: any) => void },
  ) => NatsSubscription;
  publish: (subject: string, data: Uint8Array) => void;
  status: () => AsyncIterable<any>;
}

/**
 * Interface representing a NATS subscription.
 *
 * @internal
 * @group Drivers
 */
export interface NatsSubscription {
  unsubscribe: () => void;
}
