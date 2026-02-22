# Peerix

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
// const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
peer.publish(stream);
// peer.unpublish(stream);

// open a channel
peer.open('chat', { /* options */ });
// peer.close('chat');
peer.send(data, 'chat');

// peer connect
peer.on('connect', (e) => {
  const { peer, metadata } = e;
});

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
