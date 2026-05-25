import { Driver } from './driver.js';
import { EventEmitter } from '../utils/emitter.js';

/**
 * In-memory signaling driver for intra-process communication.
 *
 * This driver is useful for testing and debugging purposes, but is not suitable
 * for production use due to its limitations (e.g., a single-process scope).
 *
 * @group Drivers
 *
 * @example
 * ```javascript
 * const driver = new MemoryDriver();
 * ```
 */
export class MemoryDriver extends Driver {
  #emitter: EventEmitter<Record<string, [number[]]>>;

  /**
   * Creates a new instance of the driver.
   *
   * @param options Optional configuration for the driver.
   * @param options.delay Delay (in milliseconds) for message delivery to simulate network latency. The delay will be a random value between 75% and 125% of the specified delay.
   */
  constructor(options?: { delay?: number }) {
    super();
    const { delay = 0 } = options || {};
    const randomizedDelay =
      delay > 0 ? Math.floor(delay * (0.75 + 0.5 * Math.random())) : 0;
    this.#emitter = new EventEmitter(null, { delay: randomizedDelay });
    this.active = true;
  }

  async subscribe(namespace: string[], handler: (data: number[]) => void) {
    const [event] = namespace.slice(-1);
    this.#emitter.on(event, handler);
  }

  async unsubscribe(namespace: string[], handler: (data: number[]) => void) {
    const [event] = namespace.slice(-1);
    this.#emitter.off(event, handler);
  }

  async dispatch(namespace: string[], data: number[]) {
    const [event] = namespace.slice(-1);
    this.#emitter.emit(event, data);
  }
}
