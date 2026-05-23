---
title: Changelog
---

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2024-05-23

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

## [0.2.0] - 2024-05-13

### Added

- Add `SocketIoDriver` for signaling using Socket.IO.
- Add `SupabaseDriver` for signaling using Supabase Realtime.
- Add a `destroy` method to some drivers.

### Changed

- Update the NATS driver to use `nats-core` instead of `nats.ws`.
- Refactor error codes for clarity.
- Refactor the driver interface to use plain arrays instead of typed arrays for improved serialization.
- Enable namespace hashing by default to improve privacy and avoid issues with unsupported characters in namespaces.

## [0.1.0] - 2024-05-06

### Added

- Initial release of the project.
- TypeScript support.
- Core functionality: peer connections, signaling, media streams, and data channels.
- Basic documentation and API reference.
- Automated tests for core features.
- Logging for better debugging.
- CI/CD pipeline for automated testing and deployment.
- Example code snippets and usage examples.
