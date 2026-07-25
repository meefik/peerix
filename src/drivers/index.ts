/**
 * Drivers route signaling messages between peers over any transport
 * (MQTT, NATS, SSE, etc.). Implement `subscribe`, `unsubscribe`, and
 * `publish` to handle message routing by namespace. The `active`
 * property reflects the connection state.
 *
 * Peerix minimizes signaling overhead by migrating connections to a
 * dedicated data channel after handshake, using binary messages,
 * debouncing ICE candidates, compressing payloads, and supporting
 * built-in namespace hashing and end-to-end encryption (E2EE).
 *
 * ```mermaid
 * sequenceDiagram
 *   Peer A -->> Peer B: broadcast announce
 *   Peer B ->> Peer A: invoke
 *   Peer A ->> Peer B: SDP offer
 *   Peer B ->> Peer A: SDP answer
 *   Peer B <<->> Peer A: ICE candidates
 * ```
 *
 * This flow runs automatically. Connecting two peers takes about
 * **4 messages** per peer, roughly **1 KB** with compression and
 * encryption enabled.
 *
 * | Signaling Type         | Sent Size  | Overhead |
 * | ---------------------- | ---------- | -------- |
 * | Raw Signaling          | 1468 bytes | 0%       |
 * | Encrypted Signaling    | 1551 bytes | +5%      |
 * | Compressed Signaling   | 806 bytes  | -45%     |
 * | Compressed & Encrypted | 891 bytes  | -40%     |
 *
 * Sometimes the built-in signaling drivers may not fit your specific use case or
 * requirements. In such cases you can implement your own custom driver by extending
 * this class and overriding `subscribe`, `unsubscribe`, and `publish`:
 *
 * ```js
 * import { Driver } from "peerix";
 *
 * class MyDriver extends Driver {
 *   async subscribe(namespace: string, handler: (data: number[]) => void) {
 *     // implement subscription logic for the given namespace and handler
 *   }
 *   async unsubscribe(namespace: string, handler: (data: number[]) => void) {
 *     // implement unsubscription logic for the given namespace and handler
 *   }
 *   async publish(namespace: string, data: number[]) {
 *     // implement publish logic for the given namespace and message
 *   }
 * }
 * ```
 *
 * Then use it:
 *
 * ```js
 * const driver = new MyDriver();
 * ```
 *
 * @module Drivers
 */

export * from "./driver.js";
export * from "./broadcast-channel.js";
export * from "./memory.js";
export * from "./nats.js";
export * from "./socket-io.js";
export * from "./supabase.js";
export * from "./sse.js";
export * from "./mqtt.js";
export * from "./centrifuge.js";
