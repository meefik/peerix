# Peerix

It is a peer-to-peer media and data sharing JavaScript library. Peerix uses WebRTC for peer-to-peer communication and relies on a signaling mechanism to facilitate peer discovery and connection management. The library abstracts away the complexities of WebRTC and provides a simple API for developers to create real-time applications with media streaming and data sharing capabilities.

## Content

- [Getting Started](#getting-started)
- [Using with CDN](#using-with-cdn)
- [Signaling Drivers](#signaling-drivers)
- [Peer Instance](#peer-instance)
- [Connection Management](#connection-management)
- [Media Streams](#media-streams)
- [Data Channels](#data-channels)
- [Add-ons](#add-ons)
- [Server Infrastructure](#server-infrastructure)
- [License](#license)

## Getting Started

Install the Peerix library via NPM:

```sh
npm install peerix
```

Use the library in your JavaScript or TypeScript code:

```js
import { Peer } from 'peerix';
import { BroadcastChannelDriver } from 'peerix/drivers';

// create a signaling driver
const driver = new BroadcastChannelDriver();

// create the Peer instance
const peer = new Peer(driver);

// listen for open channel event
peer.on('open', (e) => {
  const { channel } = e;
  // send a message to the connected peer
  peer.send('Hello, peer!', channel.id);
});

// listen for incoming messages
peer.on('message', (e) => {
  console.log('Received message:', e.data);
});

// open a data channel with default id (0)
peer.open();

// connect to a room
peer.connect('room-id');
```

You can run the above code in multiple browser tabs to see the peer-to-peer communication in action. Each tab will represent a peer that can connect to the same room and exchange messages via WebRTC data channels.

## Using with CDN

If you prefer to use Peerix via a CDN, you can include the following script tag in your HTML file:

```html
<script src="https://unpkg.com/peerix"></script>
<script>
  const { Peer, BroadcastChannelDriver } = window.peerix;
</script>
```

Or, if you want to use ES modules with a CDN, you can import the library as follows:

```html
<script type="module">
  import { Peer, BroadcastChannelDriver } from 'https://esm.sh/peerix';
</script>
```

## Signaling Drivers

Peerix supports multiple signaling drivers for peer discovery and connection management. You can choose the driver that best fits your application's needs:

- `MemoryDriver`: A simple in-memory driver for testing and development. It allows several peer instances to discover each other within one browser page.
- `BroadcastChannelDriver`: Uses the BroadcastChannel API for communication between tabs in the same browser.
- `NatsDriver`: Uses NATS messaging system for communication between peers across different browsers and devices over the internet. It supports E2EE to protect the privacy of your users' data. You can use the public NATS server at `demo.nats.io:4222` for testing purposes, but it is recommended to set up your own NATS server for production applications.

You can import and use any of the built-in drivers as follows:

```js
import { MemoryDriver } from 'peerix/drivers';

const driver = new MemoryDriver();
```

You can also implement your own custom signaling driver by adhering to the following interface:

```js
class CustomDriver {
  on(namespace, handler) { /* ... */ }
  off(namespace, handler) { /* ... */ }
  emit(namespace, message) { /* ... */ }
}
```

This driver interface allows you to integrate Peerix with any signaling mechanism you prefer, such as WebSockets or even a REST API.

To better illustrate how to implement a custom signaling driver, here is an example of a minimal in-memory driver that can be used for testing purposes:

```js
// Minimal in-memory signaling driver
class MemoryDriver extends Map {
  constructor() { super(); }
  on(namespace, handler) {
    const k = namespace.join(':');
    if (!this.has(k)) {
      this.set(k, new Set());
    }
    this.get(k).add(handler);
  }
  off(namespace, handler) {
    const k = namespace.join(':');
    this.get(k)?.delete(handler);
  }
  emit(namespace, message) {
    const k = namespace.join(':');
    if (!this.has(k)) return;
    for (const handler of this.get(k)) {
      try {
        handler(message);
      } catch (e) {
        /* swallow errors */
      }
    }
  }
}
```

## Peer Instance

The `Peer` class is the core of the Peerix library. It manages peer connections, media streams, and data channels. You can create an instance of `Peer` by providing a signaling driver and optional configuration parameters:

```js
import { Peer } from 'peerix';

// create the Peer instance
const peer = new Peer(driver, {
  iceServers: [{ urls: 'stun:stun.l.google.com:19302' }],
  quality: 'auto', // 'low', 'medium', 'high', 'auto'
});
```

By default, Peerix uses a STUN server at `stun.l.google.com:19302` for NAT traversal. However, for better connectivity, especially in restrictive network environments, it is recommended to use a TURN server. You can specify your own TURN/STUN servers in the `iceServers` configuration option.

The `quality` option allows you to specify the desired quality level for media streams. The library will automatically adjust the bitrate and resolution of the published streams based on the selected quality level and network conditions.

## Connection Management

Peerix will automatically handle peer discovery, connection management, and media stream negotiation. To connect to a room and start sharing media streams or sending messages, simply call the `connect` method with the desired room ID:

```js
// connect to a room with an optional metadata
peer.connect('room-id', { /* metadata */ });

// later, if you want to disconnect from the room
peer.disconnect();

// listen for peer connections in the room
peer.on('connect', (e) => {
  const { peer, metadata } = e;
  console.log('Connected to peer:', peer.id, 'with metadata:', metadata);
});

// listen for peer disconnections in the room
peer.on('disconnect', (e) => {
  const { peer, metadata } = e;
  console.log('Disconnected from peer:', peer.id, 'with metadata:', metadata);
});
```

Optionally, you can provide metadata that will be shared with other peers in the room. This can include information such as the peer's name, avatar, or any other relevant data.

You can call the `connect` method before or after publishing media streams or opening data channels. If you publish a stream or open a data channel before connecting to a room, the library will automatically handle the negotiation and sharing of the stream once you connect. If you will do it after connecting, the library will immediately share the stream or data channel with all connected peers in the room and start re-negotiating the connections as needed.

> Peerix allows you to use a single connection with each other peer in the room to share multiple media streams and data channels in two directions. This means that you can publish multiple media streams and open multiple data channels with the same peer without needing to establish separate connections for each stream or channel. The library will manage the negotiation and sharing of all streams and channels over the single connection, optimizing the communication between peers and reducing load on client resources, signaling, and TURN servers.


```js
const remotePeer = peer.get(peerId);
remotePeer.open(channelId);
remotePeer.send('Hello, peer!', channelId);
remotePeer.publish(mediaStream);

// Data channel management

// default behavior
peer.open(); // opens a data channel with default id (0)
peer.close(); // closes the default data channel (0)
peer.send('Hello, peer!'); // to all channels
peer.send('Hello, peer!', CHANNEL_ID); // to a specific channel
peer.send('Hello, peer!', channelInstance); // to a specific RTCDataChannel instance
peer.send('Hello, peer!', (peer, channel) => channel.id === CHANNEL_ID); // to filtered channels


const remotePeer = {
  id: string,
  metadata: any,
  channels: RTCDataChannel[],
  streams: MediaStream[],
};

peer.connections.forEach(remotePeer => {
  remotePeer.channels.forEach(channel => {
    if (channel.id === CHANNEL_ID && channel.readyState === 'open') {
      channel.send('Hello, peer!');
    }
  });
});


const CHANNEL_ID = 0;
// 1 version
peer.open({ id: CHANNEL_ID, label: 'chat', filter: (peer) => true });
peer.close({ id: CHANNEL_ID });
// 2 version
peer.open(CHANNEL_ID, { label: 'chat', filter: (peer) => true });
peer.close(CHANNEL_ID);

// Media stream management

// default behavior
peer.publish(mediaStream); // id defaults to mediaStream.id
peer.unpublish(mediaStream); // unpublish the stream by its instance
// 1 version
peer.publish(mediaStream, { id: 'camera', filter: (peer) => true });
peer.unpublish({ id: 'camera' });
// 2 version
peer.publish(mediaStream, 'camera', { filter: (peer) => true });
peer.unpublish('camera');
```

## Media Streams

You can also publish media streams to the room using the `publish` method. This allows other peers in the room to subscribe to your media streams and view or listen to them.

```js
// get a media stream from the user's camera and microphone
const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });

// publish the stream to the room with an optional stream ID
peer.publish(stream, 'camera', { /* options */ });

// get another media stream from the user's microphone only
const newStream = await navigator.mediaDevices.getUserMedia({ video: false, audio: true });
// update the existing stream with new tracks
peer.publish(newStream, 'camera', { /* options */ });

// later, if you want to stop sharing the stream, you can unpublish it
peer.unpublish('camera');

// listen for peer publishing a track in a stream
peer.on('publish', (e) => {
  const { peer, stream, track } = e;
  console.log('Peer published a track:', track.id, 'in stream:', stream.id);
});

// listen for peer unpublishing a track in a stream
peer.on('unpublish', (e) => {
  const { peer, stream, track } = e;
  console.log('Peer unpublished a track:', track.id, 'from stream:', stream.id);
});
```

You can publish multiple streams with different IDs, and you can also update an existing stream by publishing a new stream with the same ID. The library will automatically handle the negotiation and sharing of the updated stream with all connected peers in the room.

## Data Channels

You can open data channels to exchange arbitrary data with other peers in the room. Data channels are useful for sending messages, files, or any other type of data that does not fit into media streams.

```js
// define a channel ID for the channel
const CHANNEL_ID = 0;

// open a data channel with a specific ID
peer.open(CHANNEL_ID, { /* options */ });

// later, if you want to close the data channel
peer.close(CHANNEL_ID);

// listen for incoming messages on the data channel
peer.on('message', (e) => {
  const { peer, channel, data } = e;
  console.log('Received message from peer:', peer.id, 'on channel:', channel.id, 'data:', data);
});

// listen for data channel open event
peer.on('open', (e) => {
  const { peer, channel } = e;
  // send a message to the connected peer over the data channel
  peer.send('Hello, peer!', channel.id);
});

// listen for data channel close event
peer.on('close', (e) => {
  const { peer, channel } = e;
  console.log('Data channel closed with peer:', peer.id, 'channel:', channel.id);
});
```

Note that you should open a data channel on each peer manually with the same channel ID to establish a connection between them. Peerix does not automatically open data channels between peers to avoid racing conditions when multiple peers try to open channels simultaneously.

Peerix allows you to open multiple data channels with different IDs, and you can also specify options for each channel, such as the ordered or unordered delivery of messages and other channel-specific settings.

> The data channel ID is a number between 0 and 65535. However, you can set the short string instead that will be hashed to a number in that range. This allows you to use more meaningful identifiers for your data channels while still adhering to the WebRTC specification for channel IDs. In rare cases, this approach may lead to collisions where different string IDs hash to the same channel ID, so it is recommended to use numeric IDs to avoid conflicts.

## Add-ons

Peerix supports add-ons that can extend the functionality of the core library. Add-ons are separate modules that can be imported and used alongside the main `Peer` class to provide additional features such as recording, storage, or synchronization. You can find the available add-ons in the `peerix/addons` directory. To use an add-on, simply import it and use it with your `Peer` instance:

```js
import { Addon } from 'peerix/addons';

// create the add-on instance
const addon = new Addon({ /* options */ });
// attach the add-on to the peer instance
peer.attach(addon);
// later, if you want to detach the add-on from the peer instance
peer.detach(addon);
```

## Server Infrastructure

Peerix is designed to work in a decentralized manner, allowing peers to connect directly to each other without relying on a central server for media relay. However, it does require a signaling server for peers to discover each other and establish connections. For better connectivity and performance, especially in restrictive network environments, it is recommended to use a TURN server for media relay when direct peer-to-peer connections are not possible.

You can set up open-source self-hosted NATS and TURN servers using the following Docker Compose configuration:

```yaml
services:
  nats:
    image: nats:latest
    ports:
      - '4222:4222'
  coturn:
    image: coturn/coturn:latest
    ports:
      - '3478:3478'
      - '3478:3478/udp'
```

> Peerix provides NATS and TURN servers that you can use in your applications with production-ready performance and reliability. Visit the [Peerix Cloud](https://peerix.tech) website for more information on how to access and use these servers in your applications.

## License

### Open Source License

Peerix - A decentralized peer-to-peer communication library.

Copyright (C) 2026 Peerix

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

### Commercial License

For proprietary applications or if you do not wish to comply with the GPL license, please contact the [Peerix team](https://peerix.dev) for more information.

## Roadmap

- [ ] TypeScript
- [ ] NATS Driver
