import { Driver } from './driver.js';
import { EventEmitter } from '../utils/emitter.js';

/**
 * BroadcastChannel-based signaling driver for intra-origin communication.
 *
 * This driver uses the [BroadcastChannel API](https://developer.mozilla.org/docs/Web/API/BroadcastChannel)
 * to relay signaling messages between browser contexts (e.g., tabs, windows, iframes)
 * that share the same origin.
 *
 * It is useful for testing and debugging purposes, but is not suitable
 * for production use due to its limitations (e.g., same-origin restrictions).
 *
 * @group Drivers
 *
 * @example
 * ```javascript
 * const driver = new BroadcastChannelDriver('peerix');
 * ```
 */
export class BroadcastChannelDriver extends Driver {
  #emitter: EventEmitter<Record<string, [number[]]>>;
  #bc: BroadcastChannel;

  /**
   * Creates a new instance of the driver.
   *
   * @param channelName BroadcastChannel name (defaults to 'peerix').
   */
  constructor(channelName: string = 'peerix') {
    super();
    this.#emitter = new EventEmitter();
    this.#bc = new BroadcastChannel(channelName);
    this.#bc.onmessage = (e) => {
      const [event, data] = e.data;
      this.#emitter.emit(event, data);
    };
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
    this.#bc.postMessage([event, data]);
  }
}
