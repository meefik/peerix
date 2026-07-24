# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `Room` class as the top-level entry point for managing a room and its peers.
- `me` object returned by the `join()` method contains the `id` and `metadata` of the local peer.

### Changed

- **Breaking:** Room identifier is now set via `id` option in the `Room` constructor instead of passed to `join()`.
- **Breaking:** `join()` no longer accepts a room name and now returns local peer information (`{ id, metadata }`).
- **Breaking:** `Peer` class replaced the previous `RemotePeer` — it is now a peer connection handler created by `Room`.
- **Breaking:** `remote` property on event payloads renamed to `peer` across all events.
- **Breaking:** Stream and track events emitted from `Room` for local sharing have an undefined `peer`, allowing consumers to distinguish local from remote updates.
- **Breaking:** `connections` property renamed to `peers`.

### Removed

- **Breaking:** Local events (`local:join`, `local:leave`, `local:share`, `local:unshare`, `local:open`, `local:close`). Use the `stream` and `track` events without a `peer` property for local stream sharing instead.

## [0.6.0] - 2026-07-17

### Added

- `local:join` and `local:leave` events emitted when `join()` / `leave()` methods are called.
- `local:share` and `local:unshare` events emitted when `share()` / `unshare()` methods are called.
- `local:open` and `local:close` events emitted when `open()` / `close()` methods are called.
- Streams with all tracks ended are automatically unshared (unless `managed` is `true`).

### Changed

- **Breaking:** `namespace` option in signaling drivers is now a `string` instead of `string[]`.
- **Breaking:** `share()` / `unshare()` no longer return a `MediaStream`.
- **Breaking:** `managed` now means the stream is externally managed and will not be stopped or unshared automatically.

## [0.5.0] - 2026-06-25

### Added

- Support for `ReadableStream` data in the `send()` method.
- Optional metadata (`info` field) when sending messages.
- Abort signal support to stop message transmission early.
- `toJSON()` method on peer classes to serialize peer information.

### Changed

- **Breaking:** `send()` now returns an async iterator (for tracking transmission progress) or a `Promise` (for delivery confirmation).
- **Breaking:** Incoming data is now delivered as a `ReadableStream` or a `Promise` — read the stream to track receiving progress, or await the Promise to receive the full message.
- Chunking and buffering for large message payloads via data channels.
- Data is sent through a single data channel (using the `default` label when `label` is omitted).

## [0.4.0] - 2026-05-28

### Added

- `MqttDriver` for signaling over MQTT via WebSockets.
- `CentrifugeDriver` for signaling with a Centrifuge backend.
- `ackTimeout` option in `SocketIoDriver` to configure server acknowledgment timeout.
- `iceCandidateDebounce` option in `Peer` to configure ICE candidate debounce interval.

### Changed

- **Breaking:** Renamed `dispatch` to `publish` in the `Driver` interface for consistency with messaging terminology.
- **Breaking:** Renamed `publish` / `unpublish` to `share` / `unshare` in `Peer` and `RemotePeer` to avoid confusion with the driver `publish()` method.
- Signaling messages now use a Protobuf-like format instead of the previous custom binary format.
- `SseDriver` defaults to a Mercure-compatible endpoint (`/.well-known/mercure`).
- `active` property in drivers defaults to `false`.
- Room property is escaped when namespace hashing is disabled.
- Drivers use peer or room identifiers as event names instead of concatenated strings, simplifying event names.
- Some drivers use empty prefixes by default — specify a prefix explicitly if needed.
- Some drivers with a `prefix` option concatenate it directly to the event name without a separator — include a separator in the prefix if desired.

## [0.3.0] - 2026-05-23

### Added

- `SseDriver` for signaling via Server-Sent Events (SSE).
- Debouncing for ICE candidates sent via signaling to reduce message volume.

### Changed

- Encryption keys are now derived automatically instead of using manually specified secrets.
- Signaling encryption is enabled by default.
- Peer IDs use the compressed public key format.
- Minimum build target is ES2020 to support modern JavaScript features such as `BigInt`.
- Renamed `signalingHashing` option to `namespaceHashing`.
- License changed from GPL-3.0 to Apache-2.0.

## [0.2.0] - 2026-05-13

### Added

- `SocketIoDriver` for signaling via Socket.IO.
- `SupabaseDriver` for signaling via Supabase Realtime.
- `destroy()` method in supported drivers.

### Changed

- NATS driver updated to use `nats-core` instead of `nats.ws`.
- Namespace hashing is enabled by default for improved privacy and compatibility.
- Refactored error codes for clarity.
- Driver interface uses plain arrays instead of TypedArrays for improved serialization.

## [0.1.0] - 2026-05-06

### Added

- Initial release.
- Core WebRTC peer connections, signaling, media streams, and data channels.
- Pluggable signaling drivers with a unified interface.

---

[unreleased]: https://github.com/meefik/peerix/compare/v0.6.0...HEAD
[0.6.0]: https://github.com/meefik/peerix/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/meefik/peerix/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/meefik/peerix/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/meefik/peerix/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/meefik/peerix/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/meefik/peerix/releases/tag/v0.1.0
