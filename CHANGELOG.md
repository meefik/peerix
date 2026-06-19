---
title: Changelog
---

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Add unit tests for utilities and some drivers.
- Add support for `ReadableStream` data in the `send` method.
- Add optional additional metadata (`info` field) when sending messages.
- Add `toJSON` method to serialize peer information for local and remote peers.
- Add `AGENTS.md` file with rules for AI agents interacting with this project.

### Changed

- Chunk and buffer large messages during transmission via data channels to support larger messages out of the box.

## [0.4.0] - 2026-05-28

### Added

- Add `MqttDriver` for signaling using MQTT over WebSockets.
- Add `CentrifugeDriver` for signaling using a Centrifuge-based backend.
- Add `ackTimeout` option to `SocketIoDriver` to specify the timeout for acknowledgments from the server.
- Add `iceCandidateDebounce` option to `Peer` to specify the debounce time for sending ICE candidates via signaling.
- Add example code snippets showing how to run some backends locally for testing using Docker.

### Changed

- Rename `dispatch` method to `publish` in the `Driver` interface for better clarity and consistency with common messaging terminology.
- Rename `publish` and `unpublish` methods in the `Peer` and `RemotePeer` classes to `share` and `unshare` respectively to avoid confusion with the `publish` method in the `Driver` interface.
- The `SseDriver` uses a Mercure-compatible endpoint by default (`/.well-known/mercure`).
- The `Driver` class sets the `active` accessor to `false` by default.
- The `room` property is escaped when namespace hashing is disabled.
- Most drivers use peer or room identifiers as event names instead of concatenating them, which simplifies implementation and shortens event names.
- Some drivers use empty prefixes by default, so if you want to use a prefix, you need to specify one in the options.
- Some drivers with a `prefix` property add an additional string to the beginning of the event name without any separators, so if you want to separate the prefix from the event name, you need to include a separator in the prefix.
- Use a Protobuf-like format for signaling messages instead of a custom binary format.

## [0.3.0] - 2026-05-23

### Added

- Add `SseDriver` for signaling using Server-Sent Events (SSE).
- Add debouncing for ICE candidates sent via signaling to reduce the number of messages.

### Changed

- Use derived encryption keys for signaling instead of manually specified secret keys.
- Enable signaling encryption by default.
- Use the compressed public key as the peer ID.
- The minimum build target is ES2020 to allow the use of modern JavaScript features such as BigInt.
- Rename 'signalingHashing' to 'namespaceHashing' for clarity.
- The license has been changed from GPL-3.0 to Apache-2.0 to allow more permissive use of the library in both open-source and commercial projects.

## [0.2.0] - 2026-05-13

### Added

- Add `SocketIoDriver` for signaling using Socket.IO.
- Add `SupabaseDriver` for signaling using Supabase Realtime.
- Add a `destroy` method to some drivers.

### Changed

- Update the NATS driver to use `nats-core` instead of `nats.ws`.
- Refactor error codes for clarity.
- Refactor the driver interface to use plain arrays instead of typed arrays for improved serialization.
- Enable namespace hashing by default to improve privacy and avoid issues with unsupported characters in namespaces.

## [0.1.0] - 2026-05-06

### Added

- Initial release of the project.
- TypeScript support.
- Core functionality: peer connections, signaling, media streams, and data channels.
- Basic documentation and API reference.
- Automated tests for core features.
- Logging for better debugging.
- CI/CD pipeline for automated testing and deployment.
- Example code snippets and usage examples.
