/**
 * Addons are modular extensions that can be attached to a Room instance to
 * provide additional functionality or integrate with external services.
 *
 * You can create your own custom addons by extending the `Addon` class and
 * implementing the desired functionality. This allows you to tailor the
 * behavior of Peerix to fit the specific needs of your application. For
 * example, you could create an addon that provides a custom data sync
 * mechanism, or one that integrates with a third-party service for
 * additional features.
 *
 * Define your custom addon:
 *
 * ```js
 * import { Addon } from "peerix";
 *
 * class CustomAddon extends Addon {
 *   async attach(room) {
 *     // handle attaching the addon to the room instance
 *   }
 *   async detach(room) {
 *     // handle detaching the addon from the room instance
 *   }
 * }
 * ```
 *
 * Then use it:
 *
 * ```js
 * const addon = new CustomAddon();
 * await room.attach(addon);
 * // await room.detach(addon); // later, if needed
 * ```
 *
 * @module Addons
 */

export * from "./addon.js";
