import type { Driver } from './drivers/driver.js';
import { MemoryDriver } from './drivers/memory.js';

// All peers without a driver will share the same in-memory signaling bus
const defaultDriver = new MemoryDriver();

/**
 * Signaler class for managing signaling between peers.
 */
export class Signaler {
  /**
   * Driver used for signaling.
   */
  #driver: Driver;
  /**
   * Handler function that will be called when a signaling message is received.
   */
  #handler: (message?: any) => void;

  /**
   * Creates a new Signaler instance.
   * 
   * @param options Options for the Signaler.
   * @param options.driver Optional driver for signaling. If not provided, a default in-memory driver will be used.
   * @param options.handler Handler function that will be called when a signaling message is received.
   */
  constructor(options: { driver?: Driver; handler: (message?: any) => void; }) {
    const { driver = defaultDriver, handler } = options;
    this.#driver = driver;
    this.#handler = handler;
  }

  /**
   * Subscribes to the specified namespaces for signaling messages.
   * 
   * @param namespaces Namespaces to subscribe to.
   */
  async subscribe(...namespaces: string[][]) {
    for (const namespace of namespaces) {
      await this.#driver.subscribe(namespace, this.#handler);
    }
  }
  /**
   * Unsubscribes from the specified namespaces for signaling messages.
   * 
   * @param namespaces Namespaces to unsubscribe from.
   */
  async unsubscribe(...namespaces: string[][]) {
    for (const namespace of namespaces) {
      await this.#driver.unsubscribe(namespace, this.#handler);
    }
  }
  /**
   * Dispatches a signaling message to the specified namespace.
   * 
   * @param namespace The namespace to dispatch the message to.
   * @param message The message to dispatch.
   */
  async dispatch(namespace: string[], message?: any) {
    const driverActive = this.#driver.active;
    if (driverActive || driverActive === undefined) {
      await this.#driver.dispatch(namespace, message);
    }
  }
}
