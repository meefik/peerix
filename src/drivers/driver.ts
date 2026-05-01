import { EventEmitter } from '../utils/emitter';

/**
 * Base class for signaling drivers.
 * 
 * Drivers are responsible for sending and receiving signaling messages 
 * between peers. They should implement the subscribe, unsubscribe and 
 * dispatch methods to handle message routing based on namespaces.
 * 
 * @example
 * ```javascript
 * class MyDriver extends Driver {
 *   async subscribe(namespace, handler) {
 *     // Subscribe to messages for the given namespace and call handler on message
 *   }
 * 
 *   async unsubscribe(namespace, handler) {
 *     // Unsubscribe from messages for the given namespace and handler
 *   }
 * 
 *   async dispatch(namespace, message) {
 *     // Dispatch a message to the given namespace
 *   }
 * }
 * ```
 * @group Drivers
 */
export class Driver {
  /**
   * Indicates whether the driver is currently active.
   * Drivers should emit 'active' and 'inactive' events to update this state accordingly.
   */
  active = true;
  /**
   * Internal event emitter for managing driver events.
   */
  #emitter: EventEmitter<{ [key: string]: any; }>;

  /**
   * Creates a new driver instance.
   */
  constructor() {
    this.#emitter = new EventEmitter(this);
    this.on('active', () => (this.active = true));
    this.on('inactive', () => (this.active = false));
  }

  /**
   * Registers an event handler for the specified internal event.
   * 
   * @param event The event name.
   * @param handler The event handler function.
   */
  on(event: string, handler: (data?: any) => void) {
    this.#emitter.on(event, handler);
  }

  /**
   * Unregisters an event handler for the specified internal event.
   * 
   * @param event The event name.
   * @param handler The event handler function to remove.
   */
  off(event: string, handler: (data?: any) => void) {
    this.#emitter.off(event, handler);
  }

  /**
   * Emits an internal event with optional data.
   * 
   * @param event The event name.
   * @param data The data to pass to event handlers.
   */
  emit(event: string, data?: any) {
    this.#emitter.emit(event, data);
  }

  /**
   * Subscribes to signaling messages for the specified namespace.
   * 
   * @param namespace The namespace to subscribe to.
   * @param handler The handler function to call when a message is received.
   */
  async subscribe(namespace: string[], handler: (message: Uint8Array) => void) {
    // stub method to be implemented by concrete drivers
  }

  /**
   * Unsubscribes from signaling messages for the specified namespace.
   * 
   * @param namespace The namespace to unsubscribe from.
   * @param handler The handler function to remove.
   */
  async unsubscribe(namespace: string[], handler: (message: Uint8Array) => void) {
    // stub method to be implemented by concrete drivers
  }

  /**
   * Dispatches a signaling message to the specified namespace.
   * 
   * @param namespace The namespace to dispatch the message to.
   * @param message The message to dispatch.
   */
  async dispatch(namespace: string[], message: Uint8Array) {
    // stub method to be implemented by concrete drivers
  }
}
