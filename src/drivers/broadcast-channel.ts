import { Driver } from './driver.js';
import { EventEmitter } from '../utils/emitter.js';

/**
 * BroadcastChannel-based signaling driver for intra-origin communication.
 *
 * This driver is useful for testing and debugging purposes, but is not suitable
 * for production use due to its limitations (e.g. same-origin restriction).
 * 
 * @group Drivers
 * @example
 * ```javascript
 * const driver = new BroadcastChannelDriver('peerix');
 * ```
 */
export class BroadcastChannelDriver extends Driver {
  #emitter: EventEmitter<{ [namespace: string]: [number[]]; }>;
  #bc: BroadcastChannel;

  /**
   * Creates a new instance of the driver.
   *
   * @param channelName Optional BroadcastChannel name (defaults to 'peerix').
   */
  constructor(channelName: string) {
    super();
    this.#emitter = new EventEmitter();
    this.#bc = new BroadcastChannel(channelName || 'peerix');
    this.#bc.onmessage = (e) => {
      const [ns, payload] = e.data;
      if (!ns) return;
      this.#emitter.emit(ns, payload);
    };
  }

  async subscribe(namespace: string[], handler: (payload: number[]) => void) {
    const ns = this.#getNS(namespace);
    this.#emitter.on(ns, handler);
  }

  async unsubscribe(namespace: string[], handler: (payload: number[]) => void) {
    const ns = this.#getNS(namespace);
    this.#emitter.off(ns, handler);
  }

  async dispatch(namespace: string[], payload: number[]) {
    const ns = this.#getNS(namespace);
    this.#bc.postMessage([ns, payload]);
  }

  /**
   * Constructs a namespace string from an array of namespace segments.
   * 
   * @param namespace Array of namespace segments.
   * @returns Constructed namespace string.
   */
  #getNS(namespace: string[]) {
    return namespace.join(':');
  }
}
