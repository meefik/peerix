import { test, expect } from '@playwright/test';
import type { Peer } from '../src/index.js';

const { DEBUG } = process.env;

test('peer connections', async ({ page }) => {
  await page.goto('./tests/sandbox.html');

  if (DEBUG) {
    page.on('console', (msg) => {
      console.log(`CONSOLE: ${msg.text()}`);
    });
  }

  const [peer1, peer2] = await page.evaluate(async () => {
    const { Peer } = await import('../src/index.js');

    localStorage.debug = 'peerix:*';

    const peer1 = new Peer({ id: '1' });
    const peer2 = new Peer({ id: '2' });

    let connected = 0;

    const createPeerPromise = (peer: Peer, quorum: number) => {
      return Promise.all([
        // new connection event
        new Promise((resolve: (value: any) => void) => peer.on('state', (e) => {
          const { remote, state } = e;
          if (state === 'new') {
            resolve({
              connections: peer.connections.size,
              remote: { id: remote.id, metadata: remote.metadata },
              state,
            });
          }
        })),
        // connecting event
        new Promise((resolve: (value: any) => void) => peer.on('state', (e) => {
          const { remote, state } = e;
          if (state === 'connecting') {
            resolve({
              connections: peer.connections.size,
              remote: { id: remote.id, metadata: remote.metadata },
              state,
            });
          }
        })),
        // connected event
        new Promise((resolve: (value: any) => void) => peer.on('state', (e) => {
          const { remote, state } = e;
          if (state === 'connected') {
            resolve({
              connections: peer.connections.size,
              remote: { id: remote.id, metadata: remote.metadata },
              state,
            });
            if (++connected >= quorum) {
              setTimeout(() => peer.leave(), 100);
            }
          }
        })),
        // disconnected event
        new Promise((resolve: (value: any) => void) => peer.on('state', (e) => {
          const { remote, state } = e;
          if (state === 'closed') {
            resolve({
              connections: peer.connections.size,
              remote: { id: remote.id, metadata: remote.metadata },
              state,
            });
          }
        })),
      ]);
    };

    const peer1Promise = createPeerPromise(peer1, 2);
    const peer2Promise = createPeerPromise(peer2, 2);

    await Promise.all([
      peer1.join({ room: 'test', metadata: { name: 'peer1' } }),
      peer2.join({ room: 'test', metadata: { name: 'peer2' } }),
    ]);

    await Promise.all([
      peer1.open(0),
      peer2.open(0),
    ]);

    return [await peer1Promise, await peer2Promise];
  });

  expect({ peer1, peer2 }).toEqual({
    peer1: [
      { connections: 0, remote: { id: '2', metadata: { name: 'peer2' } }, state: 'new' },
      { connections: 1, remote: { id: '2', metadata: { name: 'peer2' } }, state: 'connecting' },
      { connections: 1, remote: { id: '2', metadata: { name: 'peer2' } }, state: 'connected' },
      { connections: 0, remote: { id: '2', metadata: { name: 'peer2' } }, state: 'closed' },
    ],
    peer2: [
      { connections: 0, remote: { id: '1', metadata: { name: 'peer1' } }, state: 'new' },
      { connections: 1, remote: { id: '1', metadata: { name: 'peer1' } }, state: 'connecting' },
      { connections: 1, remote: { id: '1', metadata: { name: 'peer1' } }, state: 'connected' },
      { connections: 0, remote: { id: '1', metadata: { name: 'peer1' } }, state: 'closed' },
    ],
  });
});

test('data channels', async ({ page }) => {
  await page.goto('./tests/sandbox.html');

  if (DEBUG) {
    page.on('console', (msg) => {
      console.log(`CONSOLE: ${msg.text()}`);
    });
  }

  const [peer1, peer2] = await page.evaluate(async () => {
    const { Peer } = await import('../src/index.js');

    localStorage.debug = 'peerix:*';

    const peer1 = new Peer({ id: '1' });
    const peer2 = new Peer({ id: '2' });

    const createPeerPromise = (peer: Peer, quorum: number) => {
      const messages = [] as any[];
      const openedChannels = [] as any[];
      const closedChannels = [] as any[];

      return Promise.all([
        // data channel open event
        new Promise((resolve: (value: any) => void) => peer.on('open', (e) => {
          const { remote, channel } = e;

          openedChannels.push({
            event: 'open',
            channels: peer.channels.size,
            remote: { id: remote.id, metadata: remote.metadata },
            channel: { id: channel.id, label: channel.label },
          });

          // wait for all channels to be opened before sending messages
          if (openedChannels.length >= quorum) {
            resolve(openedChannels);

            // send message by channel id
            peer.send(JSON.stringify({ type: 'by-id', peer: peer.id, channel: 0 }), { id: 0 });
            peer.send(JSON.stringify({ type: 'by-id', peer: peer.id, channel: 1 }), { id: 1 });
            // send message by channel label
            peer.send(JSON.stringify({ type: 'by-label', peer: peer.id, channel: 'channel0' }), { label: 'channel0' });
            peer.send(JSON.stringify({ type: 'by-label', peer: peer.id, channel: 'channel1' }), { label: 'channel1' });
            // send message to all channels
            peer.send(JSON.stringify({ type: 'to-all', peer: peer.id }));
          }
        })),
        // data channel message event
        new Promise((resolve: (value: any) => void) => peer.on('message', (e) => {
          const { remote, channel, data: rawData } = e;
          const data = JSON.parse(rawData);

          messages.push({
            event: 'message',
            remote: { id: remote.id, metadata: remote.metadata },
            channel: { id: channel.id, label: channel.label },
            data,
          });

          // wait for all messages to be received before resolving
          if (messages.length >= 6) {
            resolve(messages);
            peer.close({ id: 0 });
            peer.close({ id: 1 });
          }
        })),
        // data channel close event
        new Promise((resolve: (value: any) => void) => peer.on('close', (e) => {
          const { remote, channel } = e;

          closedChannels.push({
            event: 'close',
            channels: peer.channels.size,
            remote: { id: remote.id, metadata: remote.metadata },
            channel: { id: channel.id, label: channel.label },
          });

          // wait for all channels to be closed before resolving
          if (closedChannels.length >= quorum) {
            resolve(closedChannels);
          }
        })),
      ]);
    };

    const peer1Promise = createPeerPromise(peer1, 2);
    const peer2Promise = createPeerPromise(peer2, 2);

    peer1.open({ id: 0, label: 'channel0' });
    peer2.open({ id: 0, label: 'channel0' });
    peer1.open({ id: 1, label: 'channel1' });
    peer2.open({ id: 1, label: 'channel1' });

    peer1.join({ room: 'test', metadata: { name: 'peer1' } });
    peer2.join({ room: 'test', metadata: { name: 'peer2' } });

    return [await peer1Promise, await peer2Promise];
  });

  expect({ peer1, peer2 }).toEqual({
    peer1: [
      [
        {
          event: 'open',
          channels: 2,
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { id: 0, label: 'channel0' },
        },
        {
          event: 'open',
          channels: 2,
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { id: 1, label: 'channel1' },
        },
      ],
      [
        {
          event: 'message',
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { id: 0, label: 'channel0' },
          data: { type: 'by-id', peer: '2', channel: 0 },
        },
        {
          event: 'message',
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { id: 1, label: 'channel1' },
          data: { type: 'by-id', peer: '2', channel: 1 },
        },
        {
          event: 'message',
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { id: 0, label: 'channel0' },
          data: { type: 'by-label', peer: '2', channel: 'channel0' },
        },
        {
          event: 'message',
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { id: 1, label: 'channel1' },
          data: { type: 'by-label', peer: '2', channel: 'channel1' },
        },
        {
          event: 'message',
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { id: 0, label: 'channel0' },
          data: { type: 'to-all', peer: '2' },
        },
        {
          event: 'message',
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { id: 1, label: 'channel1' },
          data: { type: 'to-all', peer: '2' },
        },
      ],
      [
        {
          event: 'close',
          channels: 0,
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { id: 0, label: 'channel0' },
        },
        {
          event: 'close',
          channels: 0,
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { id: 1, label: 'channel1' },
        },
      ],
    ],
    peer2: [
      [
        {
          event: 'open',
          channels: 2,
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { id: 0, label: 'channel0' },
        },
        {
          event: 'open',
          channels: 2,
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { id: 1, label: 'channel1' },
        },
      ],
      [
        {
          event: 'message',
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { id: 0, label: 'channel0' },
          data: { type: 'by-id', peer: '1', channel: 0 },
        },
        {
          event: 'message',
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { id: 1, label: 'channel1' },
          data: { type: 'by-id', peer: '1', channel: 1 },
        },
        {
          event: 'message',
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { id: 0, label: 'channel0' },
          data: { type: 'by-label', peer: '1', channel: 'channel0' },
        },
        {
          event: 'message',
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { id: 1, label: 'channel1' },
          data: { type: 'by-label', peer: '1', channel: 'channel1' },
        },
        {
          event: 'message',
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { id: 0, label: 'channel0' },
          data: { type: 'to-all', peer: '1' },
        },
        {
          event: 'message',
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { id: 1, label: 'channel1' },
          data: { type: 'to-all', peer: '1' },
        },
      ],
      [
        {
          event: 'close',
          channels: 0,
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { id: 0, label: 'channel0' },
        },
        {
          event: 'close',
          channels: 0,
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { id: 1, label: 'channel1' },
        },
      ],
    ],
  });
});

test('media streams', async ({ page }) => {
  await page.goto('./tests/sandbox.html');

});
