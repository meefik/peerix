import { EventEmitter } from "../utils/emitter.js";

/**
 * Base class for signaling drivers.
 *
 * Drivers are responsible for sending and receiving signaling messages
 * between peers. They should implement the subscribe, unsubscribe, and
 * publish methods to handle message routing based on namespaces.
 *
 * A namespace is an array of strings representing the hierarchical path
 * for routing messages between peers. It looks like this:
 * `["room-id", "peer-id"]`.
 *
 * Drivers typically use a string event name instead of an array. Since
 * each part of the namespace is globally unique, drivers can simply use
 * the last part of the namespace as the event name for routing messages
 * to minimize the event name length. In complex cases, drivers can also
 * use other parts of the namespace or concatenate multiple parts.
 *
 * The active property indicates whether the driver is currently connected
 * to the signaling backend and able to send/receive messages.
 *
 * @group Drivers
 *
 * @example
 * ```typescript
 * class MyDriver extends Driver {
 *   async subscribe(namespace: string, handler: (data: number[]) => void) {
 *     super.subscribe(namespace, handler);
 *   }
 *   async unsubscribe(namespace: string, handler: (data: number[]) => void) {
 *     super.unsubscribe(namespace, handler);
 *   }
 *   async publish(namespace: string, data: number[]) {
 *     super.publish(namespace, data);
 *   }
 * }
 * ```
 */
export class Driver {
  #active: boolean;
  #emitter: EventEmitter<DriverEvents>;

  /** Indicates whether the driver is currently active. */
  get active(): boolean {
    return this.#active;
  }

  /** Sets the active state of the driver and emits corresponding events. */
  set active(value: boolean) {
    if (this.#active !== value) {
      this.#active = value;
      this.emit(value ? "active" : "inactive");
    }
  }

  /**
   * Creates a new driver instance.
   */
  constructor() {
    this.#emitter = new EventEmitter(this);
    this.#active = false;
  }

  /**
   * Registers an event handler for the specified internal event.
   *
   * @param event The event name.
   * @param handler The event handler function.
   */
  on<K extends keyof DriverEvents>(
    event: K,
    handler: (...args: DriverEvents[K]) => void,
  ): void {
    this.#emitter.on(event, handler);
  }

  /**
   * Unregisters an event handler for the specified internal event.
   *
   * @param event The event name.
   * @param handler The event handler function to remove.
   */
  off<K extends keyof DriverEvents>(
    event: K,
    handler: (...args: DriverEvents[K]) => void,
  ): void {
    this.#emitter.off(event, handler);
  }

  /**
   * Emits an internal event with optional data.
   *
   * @param event The event name.
   * @param args The data to pass to event handlers.
   */
  emit<K extends keyof DriverEvents>(event: K, ...args: DriverEvents[K]): void {
    this.#emitter.emit(event, ...args);
  }

  /**
   * Subscribes to signaling messages for the specified namespace.
   *
   * @param namespace The namespace to subscribe to.
   * @param handler The handler function to call when a message is received.
   */
  subscribe(
    namespace: string,
    handler: (data: number[]) => void,
  ): void | Promise<void> {
    this.#emitter.emit("subscribe", namespace, handler);
  }

  /**
   * Unsubscribes from signaling messages for the specified namespace.
   *
   * @param namespace The namespace to unsubscribe from.
   * @param handler The handler function to remove.
   */
  unsubscribe(
    namespace: string,
    handler: (data: number[]) => void,
  ): void | Promise<void> {
    this.#emitter.emit("unsubscribe", namespace, handler);
  }

  /**
   * Publishes a signaling message to the specified namespace.
   *
   * @param namespace The namespace to publish the message to.
   * @param data The message data to publish.
   */
  publish(namespace: string, data: number[]): void | Promise<void> {
    this.#emitter.emit("publish", namespace, data);
  }

  /**
   * Destroys the driver instance, cleaning up any resources.
   */
  destroy(): void {
    this.#emitter.clear();
    this.active = false;
  }
}

/**
 * Defines the internal events emitted by the {@link Driver} class.
 *
 * @group Drivers
 */
export interface DriverEvents {
  /** The driver has become active. */
  active: [];
  /** The driver has become inactive. */
  inactive: [];
  /** An error occurred within the driver. */
  error: [any];
  /** A subscription was added. */
  subscribe: [string, (data: number[]) => void];
  /** A subscription was removed. */
  unsubscribe: [string, (data: number[]) => void];
  /** A message was published. */
  publish: [string, number[]];
}
