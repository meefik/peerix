import EventEmitter from '../utils/emitter.js';
import { Peer } from '../peer.js';

/**
 * Base class for Peerix addons.
 *
 * Addons are modular extensions that can be attached to a Peer instance to
 * provide additional functionality or integrate with external services.
 */
export class Addon {
  private _emitter: EventEmitter;

  constructor() {
    this._emitter = new EventEmitter(this);
  }

  /**
   * Attach the addon to a Peer instance. This method is called when the addon is
   * added to a peer using `peer.attach(addon)`.
   *
   * @param peer The Peer instance to attach to.
   */
  async attach(peer: Peer) {
    // stub
  }

  /**
   * Detach the addon from a Peer instance. This method is called when the addon is
   * removed from a peer using `peer.detach(addon)`.
   *
   * @param peer The Peer instance to detach from.
   */
  async detach(peer: Peer) {
    // stub
  }

  /**
   * Subscribe to one or more events emitted by the addon.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
  on(event: string | string[], handler: (...args: any[]) => void) {
    this._emitter.on(event, handler);
  }

  /**
   * Subscribe to one or more events emitted by the addon for a single invocation.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
  once(event: string | string[], handler: (...args: any[]) => void) {
    this._emitter.once(event, handler);
  }

  /**
   * Unsubscribe from one or more events emitted by the addon.
   *
   * @param event Event name or list of event names.
   * @param handler Optional event handler to remove. If not provided, all handlers for the event(s) will be removed.
   */
  off(event: string | string[], handler?: (...args: any[]) => void) {
    this._emitter.off(event, handler);
  }

  /**
   * Emit one or more events from the addon.
   *
   * @param event Event name or list of event names.
   * @param args Arguments to pass to the event handlers.
   */
  emit(event: string | string[], ...args: any[]) {
    this._emitter.emit(event, ...args);
  }
}
