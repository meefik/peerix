# Peerix

Peerix is a TypeScript library for peer-to-peer room-based media and data sharing over WebRTC. It abstracts WebRTC complexity into a simple API, handling peer discovery and connection management through pluggable signaling drivers without vendor lock-in.

**Try it out**: [Peerix Talk](https://talk.peerix.dev) | [Source](https://github.com/meefik/peerix-talk)

Read the full documentation and access the source code:

- 📖 [Documentation](https://peerix.dev)
- 🧑‍💻 [Source Code](https://github.com/meefik/peerix)
- 👾 [Issues](https://github.com/meefik/peerix/issues)
- 💬 [Discussions](https://github.com/meefik/peerix/discussions)

## Features

- **Pluggable signaling drivers** — MQTT, NATS, SSE, SocketIO, Centrifuge, Supabase, BroadcastChannel, Memory — or write your own
- **Room & state management** out of the box for complex peer-to-peer applications
- **Single peer connection** — multiplexed media streams and data channels over one RTCPeerConnection
- **End-to-end encryption & compression** for all signaling data (enabled by default)
- **Zero dependencies**, fully typed, cross-browser compatible

## Quick Start

Install the library:

```sh
npm install peerix
```

Create a room, listen for messages, and join:

```js
import { Room, BroadcastChannelDriver } from "peerix";

const driver = new BroadcastChannelDriver();
const room = new Room({ id: "my-room", driver });

room.on("channel:message", async (e) => {
  const message = await e.data;
  console.log(`Message from ${e.peer.id}:`, message);
});

await room.join({ name: "Alice" });

room.on("channel:open", (e) => {
  e.peer.send("Hello!", { label: e.label });
});

await room.open({ label: "chat" });
```

The room `id` must be the same for all peers that want to connect with each other. `BroadcastChannelDriver` works across tabs in the same browser; see [Signaling Drivers](#signaling-drivers) for cross-device options.

## Signaling Drivers

Drivers handle peer discovery and initial connection setup. After the handshake, each peer connection negotiates its own data channel for signaling, so the external backend is only needed during discovery. Messages use a compact binary format with compression and end-to-end encryption.

### Built-in Drivers

| Driver                   | Use Case                  | Backend Required                       |
| ------------------------ | ------------------------- | -------------------------------------- |
| `MemoryDriver`           | Single-page testing       | None                                   |
| `BroadcastChannelDriver` | Multi-tab communication   | Same browser origin                    |
| `NatsDriver`             | Distributed / performance | [NATS](https://nats.io/)               |
| `MqttDriver`             | Lightweight broker        | [MQTT broker](https://mqtt.org/)       |
| `CentrifugeDriver`       | Real-time messaging       | [Centrifuge](https://centrifugal.dev/) |
| `SseDriver`              | Server-Sent Events + POST | [Mercure](https://mercure.rocks/)      |
| `SupabaseDriver`         | Database + real-time      | [Supabase](https://supabase.com/)      |
| `SocketIoDriver`         | WebSocket-based signaling | [Socket.IO](https://socket.io/) server |

If no driver is specified, the default is `MemoryDriver`, which only works within a single page. Drivers require their respective client libraries to be installed by you — Peerix itself has zero runtime dependencies.

### Custom Driver

Extend `Driver` and implement three methods:

```js
import { Driver } from "peerix";

class MyDriver extends Driver {
  async subscribe(namespace, handler) {
    /* listen for messages */
  }
  async unsubscribe(namespace, handler) {
    /* stop listening */
  }
  async publish(namespace, message) {
    /* send binary message as number array */
  }
}
```

### Example: NATS Driver

```js
import { NatsDriver } from "peerix";
import { wsconnect } from "@nats-io/nats-core";

const nc = await wsconnect({
  servers: ["wss://demo.nats.io:8443"],
  noEcho: true,
});

const driver = new NatsDriver({ nc });
```

## Room Configuration

Create a `Room` and configure connection behavior:

```js
import { Room } from "peerix";

const room = new Room({
  id: "my-room",
  driver,
  iceServers: [{ urls: "stun:stun.l.google.com:19302" }],
});

await room.join();
```

### Options

| Option                 | Default        | Description                                              |
| ---------------------- | -------------- | -------------------------------------------------------- |
| `id`                   | `"default"`    | Room identifier                                          |
| `driver`               | `MemoryDriver` | Signaling driver instance                                |
| `iceServers`           | `[]`           | STUN/TURN servers for NAT traversal                      |
| `iceTransportPolicy`   | `"all"`        | Set to `"relay"` to enforce TURN-only connectivity       |
| `connectionTimeout`    | `15`           | Seconds before disconnecting failed peers (`0` disables) |
| `iceCandidateDebounce` | `50`           | Milliseconds to batch ICE candidates                     |
| `namespaceHashing`     | `true`         | Hash room namespaces in signaling for privacy            |
| `signalingCompression` | `true`         | Compress signaling messages                              |
| `signalingEncryption`  | `true`         | Encrypt signaling with AES-GCM                           |
| `verify`               | —              | Callback to accept or reject incoming peers              |

Peerix handles collision resolution internally when two peers initiate connection simultaneously, using a polite/rude negotiation strategy.

### Accessing Peers via `room.peers`

After joining, connected peers are available through `room.peers`, a read-only `Map<string, Peer>` indexed by peer ID. Each `Peer` exposes:

- **`id`** — unique identifier (the Map key)
- **`metadata`** — optional metadata shared at join time
- **`state`** — connection state (`new`, `connecting`, `connected`, `disconnected`, `failed`, `closed`)
- **`connection`** — the underlying `RTCPeerConnection`
- **`send()`** — direct messaging to that peer (bypasses broadcast)
- **`share()` / `unshare()`** — per-peer stream control
- **`on()` / `off()`** — subscribe to peer-specific events

```js
// iterate over all connected peers
for (const [id, peer] of room.peers) {
  if (peer.state === "connected") {
    console.log(`Peer ${id} is online`);
  }
}

// look up a specific peer by ID and send directly
const target = room.peers.get(somePeerId);
if (target && target.metadata?.name) {
  await target.send(`Hello, ${target.metadata.name}!`, { label: "chat" });
}
```

## Data Channels

Data channels provide reliable messaging between peers. Open a channel with `room.open()`, send messages with `room.send()` (broadcast) or `peer.send()` (direct), and close with `room.close()`:

```js
await room.open({ label: "chat" });

room.on("channel:message", async (e) => {
  const message = await e.data;
  console.log(`Msg from ${e.peer.id}:`, message);
});
```

Messages are automatically decoded based on the sender's type: strings, JSON objects, Blobs, or raw bytes. Use `to` to target specific peers instead of broadcasting:

```js
await room.send("Hello!", { label: "chat", to: "peer.id" });
```

### File Transfer with Progress Tracking

`send()` returns a stream you can iterate over to track upload progress. On the receiving side, binary data arrives as chunks:

```js
// sender
const transfer = room.send(file, {
  label: "chat",
  info: { name: file.name, size: file.size },
  signal: AbortSignal.timeout(60_000),
});

for await (const p of transfer) {
  console.log(`${Math.round((p.current / p.total) * 100)}%`);
}
```

```js
// receiver — reassemble file from chunks
room.on("channel:message", async (e) => {
  const chunks = [];
  for await (const chunk of e.data) chunks.push(chunk);
  const file = new File(chunks, e.info?.name);
  console.log(file);
});
```

## Media Streams

Share camera, microphone, screen capture, or any `MediaStream` with peers using `room.share()`. Each stream is identified by a label:

```js
const stream = await navigator.mediaDevices.getUserMedia({
  video: true,
  audio: true,
});
await room.share({ label: "camera", stream });
```

Listen for remote stream changes:

```js
room.on("stream:add", (e) => {
  document.getElementById("remote-video").srcObject = e.stream;
});

room.on("stream:remove", (e) => {
  console.log(`${e.peer.id} stopped sharing`);
});
```

Call `share()` again with the same label to replace a stream. Old tracks are removed and new ones added automatically across all connected peers. Call `unshare()` to stop sharing entirely:

```js
await room.unshare({ label: "camera" });
```

### Encoding Parameters

Control codec settings per stream:

```js
await room.share({
  label: "camera",
  stream,
  audioParameters: { maxBitrate: 16_000 },
  videoParameters: { maxBitrate: 128_000, maxFramerate: 15 },
});
```

## Addons

Addons are modular extensions that attach to a Room for additional functionality. Create custom addons by extending `Addon`:

```js
import { Addon } from "peerix";

class MyAddon extends Addon {
  async attach(room) {
    /* setup */
  }
  async detach(room) {
    /* cleanup */
  }
}

const addon = new MyAddon();
await room.attach(addon);
// await room.detach(addon); // later, if needed
```

## Lifecycle Events

Peerix emits typed events for all state changes. Subscribe using `room.on()` with a group prefix (e.g., `"connection"` matches any connection event) or a specific name:

| Prefix       | Suffix          | Meaning                           |
| ------------ | --------------- | --------------------------------- |
| `connection` | `:new`          | New peer detected                 |
|              | `:connecting`   | Negotiation in progress           |
|              | `:connected`    | Peer connection established       |
|              | `:disconnected` | Peer disconnected (may reconnect) |
|              | `:failed`       | Connection attempt failed         |
|              | `:closed`       | Connection permanently closed     |
| `channel`    | `:new`          | Data channel created              |
|              | `:open`         | Channel ready for messaging       |
|              | `:close`        | Channel closed                    |
|              | `:message`      | Message received                  |
|              | `:error`        | Channel error occurred            |
| `stream`     | `:add`          | Peer shared a media stream        |
|              | `:remove`       | Peer stopped sharing a stream     |
| `track`      | `:add`          | Track added to a shared stream    |
|              | `:remove`       | Track removed from a stream       |

The `"error"` event catches general signaling and connection failures. Subscribe with `room.on("error", ...)`.

## ICE Servers

STUN servers help peers discover their public address, while TURN servers relay traffic when direct connection is not possible. Use TURN for better connectivity in restrictive networks:

```js
const room = new Room({
  id: "my-room",
  driver,
  iceServers: [
    { urls: "stun:stun.l.google.com:19302" },
    {
      urls: "turn:turn.example.com:3478",
      username: "user",
      credential: "pass",
    },
  ],
});
```

Set `iceTransportPolicy` to `"relay"` to force traffic through TURN servers only, enforcing privacy in sensitive environments. No ICE servers limits connections to the local network.

## Troubleshooting

**Peers cannot connect across networks.** — Add STUN/TURN servers via `iceServers`.

**I get a `SIGNALING_ERROR`.** — The driver failed to send or receive messages. Verify your signaling client is connected before creating the Room. Listen on `room.on("error", ...)` for details.

**Camera/microphone access fails.** — Ensure the page is served over HTTPS (or localhost). Check that no other tab holds an exclusive lock on the device.

**`send()` throws before I can message a peer.** — Wait for the `channel:open` event before calling `peer.send(...)`.

**Peers disconnect during negotiation.** — Increase `connectionTimeout` (default 15 seconds), or set it to `0` to disable.

**Two peers in the same room don't see each other.** — Verify both use the same driver connected to the same backend, call `room.join()` on both sides, and that the room `id` matches exactly. BroadcastChannel only works within the same origin.

**Signaling messages fail after disabling encryption.** — Encryption is enabled by default. If you disable it (`signalingEncryption: false`), all peers in the room must use the same setting.

## License

Copyright (C) 2026 Anton Skshidlevsky (aka meefik)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
