import { EventEmitter } from "../utils/emitter.js";

/**
 * Base class for Peerix signaling drivers.
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
   * @returns Nothing directly; resolves once the subscription is established (async drivers may return a promise).
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
   * @returns Nothing directly; resolves once the unsubscription is confirmed (async drivers may return a promise).
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
   * @returns Nothing directly; resolves once the message is delivered (async drivers may return a promise).
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
