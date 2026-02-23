# Peerix

A peer-to-peer media and data sharing library built on top of WebRTC. It provides a simple API for connecting to rooms, publishing media streams, and sending messages between peers.

```js
import { Peer } from 'peerix';
import { Recorder, Store } from 'peerix/addons';
import { MemoryDriver, BroadcastChannelDriver, NatsDriver } from 'peerix/drivers';

// signaling driver
const driver = new MemoryDriver();

// Peer instance
const peer = new Peer(driver, {
  iceServers: [{ urls: 'turn:turn.peerix.app:3478' }],
  quality: 'auto', // 'low', 'medium', 'high', 'auto'
});

// connect to a room
peer.connect('room-id', { /* metadata */ });
// peer.disconnect();

// publish a stream
// const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
peer.publish(stream, 'camera'); // stream id is optional, defaults to stream.id
// peer.unpublish('camera');

// peer.publish(stream); // publish with stream.id as id
// peer.unpublish(stream); // unpublish by stream object

// const newStream = await navigator.mediaDevices.getUserMedia({ video: false, audio: true });
peer.publish(newStream, 'camera'); // update existing stream with new tracks

// open a channel
peer.open(0, { /* options */ });
// peer.close(0);
peer.send(data, 0);

// peer connect
peer.on('connect', (e) => {
  const { peer, metadata } = e;
});
// peer.off('connect', handler);

// peer disconnect
peer.on('disconnect', (e) => {
  const { peer, metadata } = e;
});

// peer publish stream
peer.on('publish', (e) => {
  const { peer, stream } = e;
});

// peer unpublish stream
peer.on('unpublish', (e) => {
  const { peer, stream } = e;
});

// channel message
peer.on('message', (e) => {
  const { peer, channel, data } = e;
});

// open channel
peer.on('open', (e) => {
  const { peer, channel } = e;
});

// close channel
peer.on('close', (e) => {
  const { peer, channel } = e;
});

// error (peer, channel, or general)
peer.on('error', (e) => {
  const { peer, channel, error } = e;
});
```
