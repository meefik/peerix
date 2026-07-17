# Peerix

Peerix is a JavaScript library for peer-to-peer room-based media and data sharing over WebRTC. It abstracts the complexity of WebRTC into a simple API for building real-time applications, handling peer discovery and connection management through pluggable signaling drivers without vendor lock-in.

Read the full documentation and API reference on the official websites:

- 📖 [Documentation](https://peerix.dev/docs)
- 📑 [API Reference](https://api.peerix.dev)
- 🧑‍💻 [Source Code](https://github.com/meefik/peerix)
- 👾 [Issues](https://github.com/meefik/peerix/issues)
- 💬 [Discussions](https://github.com/meefik/peerix/discussions)

## How It Works

Peerix is a front-end library that runs entirely in the browser, allowing low-latency media streaming and data sharing between peers. It is designed to work in a decentralized manner, allowing peers to connect directly to each other without relying on a central server for media relay. However, it does require a signaling server for peers to discover each other and establish connections. You can use various built-in signaling drivers, or you can implement a custom driver to fit your application's needs.

Peerix is composed of several key components:

- **Peers**: The core components that manage connections between peers, including:
  - **Lifecycle Events**: Track connection state changes and peer availability.
  - **Media Streams**: Handle audio and video streaming between peers.
  - **Data Channels**: Enable message exchange and data sharing between peers.
- **Signaling Drivers**: Facilitate peer discovery and connection management through various signaling servers (NATS, MQTT, SSE, SocketIO, and more).
- **STUN/TURN servers**: Enable NAT traversal and media relay in restrictive network environments.
- **Add-ons**: Optional extensions and utilities for enhanced functionality.

Together, these components work to abstract the complexities of WebRTC and provide a simple API for building real-time peer-to-peer applications.

Peerix uses ICE (Interactive Connectivity Establishment) to establish peer-to-peer connections. Public STUN servers can be used for NAT traversal, but for better connectivity and performance—especially in restrictive network environments—you should use your own TURN server or a reputable third-party TURN service.

Peerix is not an SFU (Selective Forwarding Unit) or MCU (Multipoint Control Unit), and it does not provide server-side media processing or routing capabilities. Instead, it focuses on enabling direct peer-to-peer communication between clients, allowing you to build applications that leverage the full potential of WebRTC without the need for a central media server.

## Quick Start

Install the Peerix library via NPM:

```sh
npm install peerix
```

Use the library in your JavaScript or TypeScript code to create peer-to-peer connections, exchange messages, and share media streams:

```js
import { Peer, BroadcastChannelDriver } from "peerix";

// create a signaling driver
const driver = new BroadcastChannelDriver();

// create the Peer instance
const peer = new Peer({ driver });

// listen for peer connection state changes
peer.on("connection", (e) => {
  const { remote, state } = e;
  console.log(`Peer "${remote.id}" state changed to "${state}"`);
});

// listen for peer errors
peer.on("error", (e) => {
  const { error } = e;
  console.error("Peer error:", error);
});

// join a room
await peer.join({
  room: "room-id",
  metadata: {/* optional metadata */},
});

// later, if you want to leave the room
// await peer.leave();
```

> The room identifier can be any string, but it should be the same for all peers that want to connect with each other.

Work with data channels to exchange messages with other peers:

```js
// listen for open channel event
peer.on("channel:open", (e) => {
  const { remote, label } = e;
  console.log(`Channel ${label} opened with peer ${remote.id}`);
  // send a message to the remote peer
  remote.send("Hello, peer!", { label });
});

// listen for close channel event
peer.on("channel:close", (e) => {
  const { remote, label } = e;
  console.log(`Channel ${label} closed with peer ${remote.id}`);
});

// listen for incoming messages
peer.on("channel:message", async (e) => {
  const { remote, label, data } = e;
  // you must await the `data` to read its content
  const message = await data;
  console.log(`Msg from ${remote.id} on ${label}:`, message);
});

// open a data channel with a specific label
await peer.open({ label: "chat" });

// later, if you want to close the data channel
// await peer.close({ label: "chat" });
```

The `channel:message` event fires when the first chunk of data is received on a channel. The `data` is a `ReadableStream` but it also can be consumed as a promise. The `send` method returns an iterable transfer object to track its progress and a promise that resolves when the data is received by the remote peer.

Sending a large file via a data channel and tracking its progress:

```js
const file = new File([new Uint8Array(1024 * 1024)], "example.dat");
const transfer = peer.send(file, {
  label: "chat", // channel label
  info: { name: file.name, size: file.size }, // metadata
  signal: AbortSignal.timeout(10000), // abort signal
});
// track the progress of the transfer
for await (const progress of transfer) {
  const { id, label, current, total } = progress;
  const percent = Math.round((current / total) * 100);
  console.log(`[${id}:${label}] Sending... ${percent}%`);
}
```

You can use `AbortSignal` to abort the transfer after a specified time or cancel it manually with an abort controller.

Receiving the file and tracking its progress:

```js
peer.on("channel:message", async (e) => {
  const { remote, label, data, info } = e;
  let current = 0;
  const chunks = [];
  // read data by chunks
  for await (const chunk of data) {
    chunks.push(chunk);
    current += chunk.length;
    const percent = Math.round((current / info.size) * 100);
    console.log(`[${remote.id}:${label}] Receiving... ${percent}%`);
  }
  const file = new File(chunks, info.name);
  console.log("Received:", file);
});
```

> The channel label can be any string and should be unique for each data channel.

Work with media streams to share audio and video with other peers:

```js
// listen for a remote peer sharing a stream
peer.on("stream:add", (e) => {
  const { remote, stream, label } = e;
  console.log(`${remote.id} shared ${stream.id} (${label})`);
});

// listen for a remote peer unsharing a stream
peer.on("stream:remove", (e) => {
  const { remote, stream, label } = e;
  console.log(`${remote.id} unshared ${stream.id} (${label})`);
});

// get a media stream from the user's camera and microphone
const stream = await navigator.mediaDevices.getUserMedia({
  video: true,
  audio: true,
});

// start sharing the stream with the room
await peer.share({ label: "camera", stream });

// later, if you no longer want to share the stream, you can unshare it
// await peer.unshare({ label: "camera" });
```

> The stream label can be any string and should be unique for each media stream.

In addition to stream-level events, you can also listen for track-level events to get more granular information about the media tracks being added or removed from the stream:

```js
// listen for a remote peer adding a track to the stream
peer.on("track:add", (e) => {
  const { remote, stream, track, label } = e;
  console.log(`${remote.id}: added track ${track.id} to stream ${stream.id} (${label})`);
});

// listen for a remote peer removing a track from the stream
peer.on("track:remove", (e) => {
  const { remote, stream, track, label } = e;
  console.log(`${remote.id}: removed track ${track.id} from stream ${stream.id} (${label})`);
});
```

You can reshare a new stream with the same label to update the media being shared with other peers:

```js
// get a new media stream from the user's camera without microphone
const newStream = await navigator.mediaDevices.getUserMedia({
  video: true,
  audio: false,
});

// reshare the new stream with the same label to update the media
await peer.share({ label: "camera", stream: newStream });
```

In this case, the tracks from the old stream will be removed and replaced with the tracks from the new stream for all connected peers and new peers that join the room. On the remote peers, you will receive a `track:remove` event for the old tracks and a `track:add` event for the new tracks. This allows you to easily switch between different media sources or update the media being shared without having to manage individual tracks manually.

> Peerix automatically resolves all collisions and race conditions that may occur when multiple peers share streams or open data channels at the same time.

By default, Peerix manages the lifecycle of shared stream tracks: when a stream is unshared or replaced, the tracks are stopped automatically. You can opt out of this behavior by setting `managed` to `true`, which tells Peerix that the stream is managed externally and its tracks should not be stopped or cleaned up automatically:

```js
// share a stream without Peerix managing its tracks
await peer.share({ label: "camera", stream, managed: true });
```

When a shared stream's tracks all end naturally (e.g. the camera is disconnected), Peerix automatically unshares the stream unless `managed` is set to `true`.

Peerix emits various lifecycle events that allow you to track the state of peer connections, media streams, and data channels. You can listen for these events to manage your application's behavior based on the connection state and media availability.

Lifecycle events include:

- `local:join`/`local:leave`: fired when the local peer joins or leaves a room.
- `local:share`/`local:unshare`: fired when a media stream is shared or unshared on the local peer.
- `local:open`/`local:close`: fired when a data channel is opened or closed on the local peer.
- `connection[:new,:connecting,:connected,:disconnected,:failed,:closed]`: fired when a peer's connection state changes.
- `channel[:new,:open,:close,:message,:error]`: fired for data channel state changes and incoming messages.
- `stream[:add,:remove]`: fired when a remote peer shares or unshares a media stream.
- `track[:add,:remove]`: fired when a track is added or removed from a media stream.
- `error`: fired when an error occurs with a peer connection, media stream, data channel, or signaling.

You can subscribe to group events (e.g. `local`, `connection`, `channel`, `stream`, `track`) to receive all events in a category, or subscribe to individual events using the `:event` suffix.

## Signaling Drivers

Signaling is a crucial part of establishing peer-to-peer connections in WebRTC. It involves the exchange of messages between peers to negotiate connection parameters, exchange ICE candidates, and manage the connection lifecycle. Peerix provides a flexible signaling mechanism that allows you to choose from several built-in drivers or implement your own custom driver.

Peerix uses several techniques to secure and minimize the number and size of signaling messages required to establish and maintain peer connections while negotiating multiple media streams and data channels:

- Each peer connection negotiates a data channel for signaling after the initial connection is established, eliminating the need for a signaling server for the lifetime of the connection.
- Uses a binary format instead of JSON for signaling messages, minimizing message overhead.
- Reduces the frequency of candidate exchanges and the number of signaling messages by debouncing ICE candidates.
- Uses compression to reduce the size of signaling messages, further lowering overhead and load on the signaling server.
- Provides built-in namespace hashing and E2EE for signaling messages to protect sensitive information during transmission.

Peerix supports multiple signaling drivers for peer discovery and negotiation purposes. You can choose the driver that best fits your application's needs:

- `MemoryDriver`: A simple in-memory driver for testing and development. It allows several peer instances to discover each other within one browser page.
- `BroadcastChannelDriver`: Uses [BroadcastChannel API](https://developer.mozilla.org/docs/Web/API/BroadcastChannel) for communication between tabs in the same browser.
- `NatsDriver`: Uses [NATS](https://nats.io/) messaging system for communication between peers across different browsers and devices over the internet.
- `MqttDriver`: Uses [MQTT](https://mqtt.org/) protocol for communication between peers through an MQTT broker.
- `CentrifugeDriver`: Uses [Centrifuge](https://centrifugal.dev/) real-time messaging server for communication between peers.
- `SseDriver`: Uses [Server-Sent Events (SSE)](https://developer.mozilla.org/docs/Web/API/Server-sent_events) and POST requests for communication between peers through a [Mercure](https://mercure.rocks/) compatible server.
- `SupabaseDriver`: Uses [Supabase](https://supabase.com/) database and real-time features for communication between peers.
- `SocketIoDriver`: Uses [Socket.IO](https://socket.io/) client for communication between peers through a Socket.IO server.

If no driver is provided when creating a `Peer`, Peerix uses an in-memory `MemoryDriver` by default, which is useful for single-page development and quick tests. For multi-tab testing, use `BroadcastChannelDriver`. For production server-side signaling, use `SocketIoDriver`, `SseDriver`, or your own custom driver; for distributed signaling, use `NatsDriver`, `MqttDriver`, or `CentrifugeDriver`.

You can implement your own custom signaling driver by extending the `Driver` class and implementing the required methods:

```js
import { Driver } from "peerix";

class MyDriver extends Driver {
  async subscribe(namespace, handler) {
    // subscribe to the given namespace (string) and call handler on messages
  }
  async unsubscribe(namespace, handler) {
    // unsubscribe from the given namespace and remove the handler
  }
  async publish(namespace, message) {
    // publish a binary message (number array) to the given namespace
  }
}
```

This driver interface allows you to integrate Peerix with any signaling mechanism you prefer.

If you do not want to create your own signaling server, you may prefer to use one of the built-in drivers. For example, you can use the NATS driver. Using NATS allows you to use Peerix without writing any backend code, as all signaling is handled through NATS servers directly from the browser.

Here's how you can set up the NATS driver:

```js
import { NatsDriver } from "peerix";
import { wsconnect } from "@nats-io/nats-core";

// connect to a NATS server (e.g. the public demo server)
const nc = await wsconnect({
  servers: ["wss://demo.nats.io:8443"],
  noEcho: true,
});

// create a new driver instance
const driver = new NatsDriver({ nc });
```

You should install the [`@nats-io/nats-core`](https://www.npmjs.com/package/@nats-io/nats-core) package to use the NATS driver.

## ICE Servers

ICE (Interactive Connectivity Establishment) is a framework used in WebRTC to find the best path to connect peers. It involves using STUN (Session Traversal Utilities for NAT) servers for NAT traversal and TURN (Traversal Using Relays around NAT) servers for relaying media when direct peer-to-peer connections are not possible.

> Use TURN servers for better connectivity in restrictive network environments.

Peerix allows you to specify ICE servers for better connectivity and performance, especially in restrictive network environments. Use the `iceServers` option when creating the `Peer` instance to provide a list of STUN and TURN servers:

```js
// create the Peer instance with custom ICE servers
const peer = new Peer({
  // use signaling driver, such as NATS
  driver,
  // specify custom ICE servers for better connectivity
  iceServers: [
    // public STUN server
    { urls: "stun:stun.l.google.com:19302" },
    // custom TURN server (replace with your own server)
    {
      urls: "turn:turn.example.com:3478",
      username: "user",
      credential: "pass",
    },
  ],
});
```

## License

Copyright (C) 2026 Peerix

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
