import { EventEmitter } from '../utils/emitter';

/**
 * Base class for signaling drivers.
 *
 * Drivers are responsible for sending and receiving signaling messages
 * between peers. They should implement the subscribe, unsubscribe, and
 * dispatch methods to handle message routing based on namespaces.
 *
 * @group Drivers
 *
 * @example
 * ```javascript
 * class MyDriver extends Driver {
 *   async subscribe(namespace, handler) {
 *     // subscribe to messages for the given namespace and call the handler on each message
 *   }
 *
 *   async unsubscribe(namespace, handler) {
 *     // unsubscribe the handler from messages for the given namespace
 *   }
 *
 *   async dispatch(namespace, payload) {
 *     // dispatch a message to the given namespace
 *   }
 * }
 * ```
 */
export class Driver {
  #active = true;
  #emitter: EventEmitter<DriverEvents>;

  /** Indicates whether the driver is currently active. */
  get active() {
    return this.#active;
  }

  /** Sets the active state of the driver and emits corresponding events. */
  set active(value: boolean) {
    if (this.#active !== value) {
      this.#active = value;
      this.emit(value ? 'active' : 'inactive');
    }
  }

  /**
   * Creates a new driver instance.
   */
  constructor() {
    this.#emitter = new EventEmitter(this);
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
  ) {
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
  ) {
    this.#emitter.off(event, handler);
  }

  /**
   * Emits an internal event with optional data.
   *
   * @param event The event name.
   * @param args The data to pass to event handlers.
   */
  emit<K extends keyof DriverEvents>(event: K, ...args: DriverEvents[K]) {
    this.#emitter.emit(event, ...args);
  }

  /**
   * Subscribes to signaling messages for the specified namespace.
   *
   * @param namespace The namespace to subscribe to.
   * @param handler The handler function to call when a message is received.
   */
  async subscribe(namespace: string[], handler: (data: number[]) => void) {
    // Base implementation is intentionally empty.
  }

  /**
   * Unsubscribes from signaling messages for the specified namespace.
   *
   * @param namespace The namespace to unsubscribe from.
   * @param handler The handler function to remove.
   */
  async unsubscribe(namespace: string[], handler: (data: number[]) => void) {
    // Base implementation is intentionally empty.
  }

  /**
   * Dispatches a signaling message to the specified namespace.
   *
   * @param namespace The namespace to dispatch the message to.
   * @param data The message data to dispatch.
   */
  async dispatch(namespace: string[], data: number[]) {
    // Base implementation is intentionally empty.
  }
}

/**
 * Defines the internal events emitted by the {@link Driver} class.
 *
 * @group Drivers
 */
export interface DriverEvents {
  /** Emitted when the driver becomes active. */
  active: [];
  /** Emitted when the driver becomes inactive. */
  inactive: [];
  /** Emitted when an error occurs within the driver. */
  error: [any];
  /** Allows for additional custom events. */
  [event: string]: any[];
}
