# Peerix

Peerix is a peer-to-peer media and data sharing JavaScript library. Peerix uses WebRTC for peer-to-peer communication and relies on a signaling mechanism to facilitate peer discovery and connection management. The library abstracts away the complexities of WebRTC and provides a minimalistic API for developers to create real-time applications with media streaming and data sharing capabilities.

Read the full documentation and API reference on the official website:

- 📚 [Documentation](https://peerix.dev/docs)
- 📑 [API Reference](https://api.peerix.dev)
- 💻 [Source Code](https://github.com/peerix-dev/peerix)
- 👾 [Issues](https://github.com/peerix-dev/peerix/issues)
- 💬 [Discussions](https://github.com/peerix-dev/peerix/discussions)

## How It Works

Peerix is a front-end library that runs entirely in the browser, allowing for low-latency media streaming and data sharing between peers. It is designed to work in a decentralized manner, allowing peers to connect directly to each other without relying on a central server for media relay. However, it does require a signaling server for peers to discover each other and establish connections. You can use various built-in signaling drivers, or you can implement your own custom driver to fit your application's needs.

Peerix is composed of several key components:

- **Peers**: The core components that manage connections between peers, including:
  - **Lifecycle Events**: Track connection state changes and peer availability.
  - **Media Streams**: Handle audio and video streaming between peers.
  - **Data Channels**: Enable message exchange and data sharing between peers.
- **Signaling Drivers**: Facilitate peer discovery and connection management through various signaling servers (such as NATS, BroadcastChannel, or custom implementations).
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
import { Peer, BroadcastChannelDriver } from 'peerix';

// create a signaling driver
const driver = new BroadcastChannelDriver();

// create the Peer instance
const peer = new Peer({ driver });

// listen for peer connection state changes
peer.on('connection', (e) => {
  const { remote } = e;
  console.log(
    'State changed for peer:',
    remote.id,
    'with metadata:',
    remote.metadata,
    'state:',
    remote.state,
  );
});

// listen for peer errors
peer.on('error', (e) => {
  const { error } = e;
  console.error('Peer error:', error);
});

// join a room
peer.join({
  room: 'room-id',
  metadata: {
    /* optional metadata */
  },
});

// later, if you want to leave the room
// peer.leave();
```

> The room identifier can be any string, but it should be the same for all peers that want to connect with each other.

Work with data channels to exchange messages with other peers:

```js
// listen for open channel event
peer.on('channel:open', (e) => {
  const { remote, channel } = e;
  console.log(
    'Channel opened with peer:',
    remote.id,
    'channel:',
    channel.label,
  );
  // send a message to the connected peer
  channel.send('Hello, peer!');
});

// listen for close channel event
peer.on('channel:close', (e) => {
  const { remote, channel } = e;
  console.log(
    'Channel closed with peer:',
    remote.id,
    'channel:',
    channel.label,
  );
});

// listen for incoming messages
peer.on('channel:message', (e) => {
  const { remote, channel, data } = e;
  console.log(
    'Received message from peer:',
    remote.id,
    'channel:',
    channel.label,
    'data:',
    data,
  );
});

// open a data channel with a specific label
peer.open({ label: 'chat' });

// send a message to each connected peer via a specific data channel
peer.send('Hello, peers!', { label: 'chat' });

// later, if you want to close the data channel
// peer.close({ label: 'chat' });
```

> The channel label can be any string and should be unique for each data channel.

Work with media streams to share audio and video with other peers:

```js
// listen for a remote peer publishing a stream
peer.on('stream:add', (e) => {
  const { remote, stream, label } = e;
  console.log(
    'Peer:',
    remote.id,
    'published a stream with label:',
    label,
    'stream state:',
    stream.active,
  );
});

// listen for a remote peer unpublishing a stream
peer.on('stream:remove', (e) => {
  const { remote, stream, label } = e;
  console.log(
    'Peer:',
    remote.id,
    'unpublished a stream with label:',
    label,
    'stream state:',
    stream.active,
  );
});

// get a media stream from the user's camera and microphone
const stream = await navigator.mediaDevices.getUserMedia({
  video: true,
  audio: true,
});

// start sharing the stream with the room
peer.publish({ label: 'camera', stream });

// later, if you no longer want to share the stream, you can unpublish it
// peer.unpublish({ label: 'camera' });
```

> The stream label can be any string and should be unique for each media stream.

In addition to stream-level events, you can also listen for track-level events to get more granular information about the media tracks being added or removed from the stream:

```js
// listen for a remote peer adding a track to the stream
peer.on('track:add', (e) => {
  const { remote, stream, track, label } = e;
  console.log(
    'Peer:',
    remote.id,
    'published a track:',
    track.id,
    'in stream:',
    stream.id,
    'with label:',
    label,
  );
});

// listen for a remote peer removing a track from the stream
peer.on('track:remove', (e) => {
  const { remote, stream, track, label } = e;
  console.log(
    'Peer:',
    remote.id,
    'unpublished a track:',
    track.id,
    'from stream:',
    stream.id,
    'with label:',
    label,
  );
});
```

You can republish a new stream with the same label to update the media being shared with other peers:

```js
// get a new media stream from the user's camera without microphone
const newStream = await navigator.mediaDevices.getUserMedia({
  video: true,
  audio: false,
});

// republish the new stream with the same label to update the media
peer.publish({ label: 'camera', stream: newStream });
```

In this case, the tracks from the old stream will be removed and replaced with the tracks from the new stream for all connected peers and new peers that join the room. On the remote peers, you will receive a `track:remove` event for the old tracks and a `track:add` event for the new tracks. This allows you to easily switch between different media sources or update the media being shared without having to manage individual tracks manually.

> Peerix automatically resolves all collisions and race conditions that may occur when multiple peers publish streams or open data channels at the same time.

Peerix emits various lifecycle events that allow you to track the state of peer connections, media streams, and data channels. You can listen for these events to manage your application's behavior based on the connection state and media availability.

Lifecycle events include:

- `connection[:new,:connecting,:connected,:disconnected,:failed,:closed]`: a peer's connection state changes.
- `channel[:new,:open,:close,:message,:error]`: a data channel's state changes or it receives a message.
- `stream[:add,:remove]`: a remote peer publishes or unpublishes a media stream.
- `track[:add,:remove]`: a track is added or removed from a media stream by a remote peer.
- `error`: an error occurs with a peer connection, media stream, data channel, or signaling.

You can subscribe to either group or specific events using the `:event` suffix.

## Signaling Drivers

Peerix supports multiple signaling drivers for peer discovery and negotiation purposes. You can choose the driver that best fits your application's needs:

- `MemoryDriver`: A simple in-memory driver for testing and development. It allows several peer instances to discover each other within one browser page.
- `BroadcastChannelDriver`: Uses [BroadcastChannel API](https://developer.mozilla.org/docs/Web/API/BroadcastChannel) for communication between tabs in the same browser.
- `NatsDriver`: Uses [NATS](https://nats.io/) messaging system for communication between peers across different browsers and devices over the internet.
- `SocketIoDriver`: Uses [Socket.IO](https://socket.io/) client for communication between peers through a Socket.IO server.
- `SupabaseDriver`: Uses [Supabase](https://supabase.com/) database and real-time features for communication between peers.

If no driver is provided when creating a `Peer`, Peerix will use an in-memory `MemoryDriver` by default — useful for single-page development and quick tests. For multi-tab testing use `BroadcastChannelDriver`, and for distributed signaling use `NatsDriver`, `SocketIoDriver`, `SupabaseDriver`, or your own custom driver in production scenarios.

You can also implement your own custom signaling driver by extending the `Driver` class and implementing the required methods:

```js
import { Driver } from 'peerix';

class MyDriver extends Driver {
  async subscribe(namespace, handler) {
    // implement subscription logic for the given namespace and handler
  }
  async unsubscribe(namespace, handler) {
    // implement unsubscription logic for the given namespace and handler
  }
  async dispatch(namespace, message) {
    // implement dispatch logic for the given namespace and message
  }
}
```

This driver interface allows you to integrate Peerix with any signaling mechanism you prefer.

> Consider using NATS for production applications. NATS is a high-performance messaging system that enables efficient signaling between peers from the browser.

If you do not want to create your own signaling server, you can use the NATS driver with a public NATS server or set up your own NATS server for better performance and reliability. Using NATS allows you to use Peerix without any server-side code because all signaling is handled through NATS servers directly from the browser.

```js
import { NatsDriver } from 'peerix';
import { wsconnect } from '@nats-io/nats-core';

// connect to a NATS server (e.g. the public demo server)
const nc = await wsconnect({
  servers: ['wss://demo.nats.io:8443'],
  noEcho: true,
});

// create a new driver instance
const driver = new NatsDriver({ nc, prefix: 'peerix' });
```

You should install the `@nats-io/nats-core` package to use the NATS Driver, as it provides a WebSocket client for connecting to NATS servers from the browser.

Peerix uses several techniques to reduce the number of signaling messages required to establish and maintain peer connections. Each peer connection begins with a negotiated data channel for signaling after the initial connection is established, eliminating the need for a signaling server during the lifetime of the peer connection. Additionally, Peerix uses compression to minimize the size of signaling messages, further reducing the overhead and load on the signaling server.

Peerix supports end-to-end encryption (E2EE) for signaling messages, ensuring the privacy of these messages by ensuring that only peers with the correct encryption key can read them. You can also enable namespace hashing before sending the data to the signaling server to prevent the server from knowing the actual namespace being used.

## ICE Servers

ICE (Interactive Connectivity Establishment) is a framework used in WebRTC to find the best path to connect peers. It involves using STUN (Session Traversal Utilities for NAT) servers for NAT traversal and TURN (Traversal Using Relays around NAT) servers for relaying media when direct peer-to-peer connections are not possible.

> Use TURN servers for better connectivity in restrictive network environments.

Peerix allows you to specify ICE servers for better connectivity and performance, especially in restrictive network environments. Use `iceServers` option when creating the `Peer` instance to provide custom STUN and TURN servers:

```js
// create the Peer instance with custom ICE servers
const peer = new Peer({
  // use signaling driver, such as NATS
  driver,
  // specify custom ICE servers for better connectivity
  iceServers: [
    // public STUN server
    { urls: 'stun:stun.l.google.com:19302' },
    // custom TURN server (replace with your own server)
    {
      urls: 'turn:turn.example.com:3478',
      username: 'user',
      credential: 'pass',
    },
  ],
});
```

## License

### Open Source License

Peerix is a WebRTC peer-to-peer JavaScript/TypeScript library.

Copyright (C) 2026 Peerix

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License, version 3, published by the Free Software Foundation.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.

### Commercial License

For proprietary applications or if you do not wish to comply with the GPL license, please contact the [Peerix Team](https://peerix.dev/contact) to discuss commercial licensing options.
