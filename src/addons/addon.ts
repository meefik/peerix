import type { Room } from "../room.js";
import { EventEmitter } from "../utils/emitter.js";

/**
 * Base class for Peerix addons.
 *
 * Addons are modular extensions that can be attached to a Peer instance to
 * provide additional functionality or integrate with external services.
 *
 * @group Addons
 */
export class Addon {
  #emitter: EventEmitter<Record<string, any[]>>;

  /**
   * Creates a new Addon instance.
   */
  constructor() {
    this.#emitter = new EventEmitter(this);
  }

  /**
   * Subscribes to one or more events emitted by the addon.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
  on(event: string, handler: (...args: any[]) => void): void {
    this.#emitter.on(event, handler);
  }

  /**
   * Subscribes to one or more events emitted by the addon for a single invocation.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
  once(event: string, handler: (...args: any[]) => void): void {
    this.#emitter.once(event, handler);
  }

  /**
   * Unsubscribes from one or more events emitted by the addon.
   *
   * @param event Event name or list of event names.
   * @param handler Optional event handler to remove. If not provided, all handlers for the event(s) will be removed.
   */
  off(event: string, handler?: (...args: any[]) => void): void {
    this.#emitter.off(event, handler);
  }

  /**
   * Emits one or more events from the addon.
   *
   * @param event Event name or list of event names.
   * @param args Arguments to pass to the event handlers.
   */
  emit(event: string, ...args: any[]): void {
    this.#emitter.emit(event, ...args);
  }

  /**
   * Attaches the addon to a Peer instance. This method is called when the addon is
   * added to a peer using `peer.attach(addon)`.
   *
   * @param room The Room instance to attach to.
   */
  async attach(room: Room): Promise<void> {
    // Base implementation is intentionally empty.
  }

  /**
   * Detaches the addon from a Peer instance. This method is called when the addon is
   * removed from a peer using `peer.detach(addon)`.
   *
   * @param room The Room instance to detach from.
   */
  async detach(room: Room): Promise<void> {
    // Base implementation is intentionally empty.
  }
}
