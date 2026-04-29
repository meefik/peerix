import type { Peer } from '../peer.js';
import { EventEmitter } from '../utils/emitter.js';

/**
 * Base class for Peerix addons.
 *
 * Addons are modular extensions that can be attached to a Peer instance to
 * provide additional functionality or integrate with external services.
 * 
 * @group Addons
 */
export class Addon {
  #emitter: EventEmitter<{ [key: string]: any[]; }>;

  /**
   * Create a new Addon instance.
   */
  constructor() {
    this.#emitter = new EventEmitter(this);
  }

  /**
   * Subscribe to one or more events emitted by the addon.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
  on(event: string, handler: (...args: any[]) => void) {
    this.#emitter.on(event, handler);
  }

  /**
   * Subscribe to one or more events emitted by the addon for a single invocation.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
  once(event: string, handler: (...args: any[]) => void) {
    this.#emitter.once(event, handler);
  }

  /**
   * Unsubscribe from one or more events emitted by the addon.
   *
   * @param event Event name or list of event names.
   * @param handler Optional event handler to remove. If not provided, all handlers for the event(s) will be removed.
   */
  off(event: string, handler?: (...args: any[]) => void) {
    this.#emitter.off(event, handler);
  }

  /**
   * Emit one or more events from the addon.
   *
   * @param event Event name or list of event names.
   * @param args Arguments to pass to the event handlers.
   */
  emit(event: string, ...args: any[]) {
    this.#emitter.emit(event, ...args);
  }

  /**
   * Attach the addon to a Peer instance. This method is called when the addon is
   * added to a peer using `peer.attach(addon)`.
   *
   * @param peer The Peer instance to attach to.
   */
  async attach(peer: Peer) {
    // stub method to be implemented by concrete addons
  }

  /**
   * Detach the addon from a Peer instance. This method is called when the addon is
   * removed from a peer using `peer.detach(addon)`.
   *
   * @param peer The Peer instance to detach from.
   */
  async detach(peer: Peer) {
    // stub method to be implemented by concrete addons
  }
}
