import { test, expect } from '@playwright/test';
import type { Peer } from '../src/index.js';
import { count } from 'node:console';
import { channel } from 'node:diagnostics_channel';

test('peer connection', async ({ page }) => {
  await page.goto('./tests/sandbox.html');

  page.on('console', (msg) => {
    console.log(`CONSOLE: ${msg.text()}`);
  });

  const [peer1, peer2] = await page.evaluate(async () => {
    const { Peer } = await import('../src/index.js');

    localStorage.debug = '*';

    const peer1 = new Peer({ id: '1' });
    const peer2 = new Peer({ id: '2' });

    peer1.on('error', ({ error }) => console.error('peer1 error', error));
    peer2.on('error', ({ error }) => console.error('peer2 error', error));

    let connected = 0;

    const createPeerPromise = (peer: Peer, quorum: number) => {
      return Promise.all([
        // new connection event
        new Promise((resolve: (value: any) => void) => peer.on('state', (e) => {
          const { remote, state } = e;
          console.log('state changed', { peerId: peer.id, remoteId: remote.id, state, connections: peer.connections.size });
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
            if (++connected >= quorum) peer.leave();
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

  const [peer1, peer2] = await page.evaluate(async () => {
    const { Peer } = await import('../src/index.js');

    const peer1 = new Peer({ id: '1' });
    const peer2 = new Peer({ id: '2' });

    const createPeerPromise = (peer: Peer) => {
      const messages = [] as any[];
      const openedChannels = [] as any[];
      const closedChannels = [] as any[];

      return Promise.all([
        // data channel open event
        new Promise((resolve: (value: any) => void) => peer.on('open', (e) => {
          const { remote, channel } = e;

          console.log('channel opened', { peerId: peer.id, channelId: channel.id, channelLabel: channel.label, channels: peer.channels.size });

          // 2
          // 0 -> 0
          // 1 -> 1
          // channel.send(JSON.stringify({ type: 'reply', id: peer.id }));
          // // 2
          // // 0 -> 0
          // // 1 -> 1
          // peer.send(JSON.stringify({ type: 'channel_id', id: peer.id }), { id: channel.id as number });
          // // 2
          // // 0 -> 0
          // // 1 -> 1
          // peer.send(JSON.stringify({ type: 'channel_label', id: peer.id }), { label: channel.label });
          // // 4
          // // 0 -> 0,1
          // // 1 -> 0,1
          // console.log('send all', { channelId: channel.id, channelLabel: channel.label });
          peer.send(JSON.stringify({ type: 'all', id: peer.id }));

          openedChannels.push({
            event: 'open',
            channels: peer.channels.size,
            remote: { id: remote.id, metadata: remote.metadata },
            channel: { id: channel.id, label: channel.label },
          });
          if (openedChannels.length >= 2) {
            resolve(openedChannels);
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
          console.log(messages.length, JSON.stringify(messages));

          if (messages.length >= 1) {
            resolve([]);
            peer.close({ id: 0 });
            peer.close({ id: 1 });
          }
        })),
        // data channel close event
        new Promise((resolve: (value: any) => void) => peer.on('close', (e) => {
          const { remote, channel } = e;

          console.log('channel closed', { peerId: peer.id, channelId: channel.id, channelLabel: channel.label, channels: peer.channels.size });


          closedChannels.push({
            event: 'close',
            channels: peer.channels.size,
            remote: { id: remote.id, metadata: remote.metadata },
            channel: { id: channel.id, label: channel.label },
          });
          if (closedChannels.length >= 2) {
            resolve(closedChannels);
          }
        })),
      ]);
    };

    const peer1Promise = createPeerPromise(peer1);
    const peer2Promise = createPeerPromise(peer2);

    peer1.open({ id: 0, label: 'channel1' });
    peer2.open({ id: 0, label: 'channel1' });
    peer1.open({ id: 1, label: 'channel2' });
    peer2.open({ id: 1, label: 'channel2' });

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
          channel: { id: 0, label: 'channel1' },
        },
        {
          event: 'open',
          channels: 2,
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { id: 1, label: 'channel2' },
        },
      ],
      [],
      // [
      //   {
      //     event: 'message',
      //     remote: { id: '2', metadata: { name: 'peer2' } },
      //     channel: { id: 0, label: 'channel1' },
      //     data: { type: 'reply', id: '2' },
      //   },
      //   {
      //     event: 'message',
      //     remote: { id: '2', metadata: { name: 'peer2' } },
      //     channel: { id: 0, label: 'channel1' },
      //     data: { type: 'channel_id', id: '2' },
      //   },
      //   {
      //     event: 'message',
      //     remote: { id: '2', metadata: { name: 'peer2' } },
      //     channel: { id: 0, label: 'channel1' },
      //     data: { type: 'all', id: '2' },
      //   },
      //   {
      //     event: 'message',
      //     remote: { id: '2', metadata: { name: 'peer2' } },
      //     channel: { id: 1, label: 'channel2' },
      //     data: { type: 'reply', id: '2' },
      //   },
      //   {
      //     event: 'message',
      //     remote: { id: '2', metadata: { name: 'peer2' } },
      //     channel: { id: 1, label: 'channel2' },
      //     data: { type: 'channel_id', id: '2' },
      //   },
      //   {
      //     event: 'message',
      //     remote: { id: '2', metadata: { name: 'peer2' } },
      //     channel: { id: 0, label: 'channel1' },
      //     data: { type: 'all', id: '2' },
      //   },
      //   {
      //     event: 'message',
      //     remote: { id: '2', metadata: { name: 'peer2' } },
      //     channel: { id: 1, label: 'channel2' },
      //     data: { type: 'all', id: '2' },
      //   },
      // ],
      [
        {
          event: 'close',
          channels: 0,
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { id: 0, label: 'channel1' },
        },
        {
          event: 'close',
          channels: 0,
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { id: 1, label: 'channel2' },
        },
      ],
    ],
    peer2: [
      [
        {
          event: 'open',
          channels: 2,
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { id: 0, label: 'channel1' },
        },
        {
          event: 'open',
          channels: 2,
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { id: 1, label: 'channel2' },
        },
      ],
      [],
      // [
      //   {
      //     event: 'message',
      //     remote: { id: '1', metadata: { name: 'peer1' } },
      //     channel: { id: 0, label: 'channel1' },
      //     data: { type: 'reply', id: '1' },
      //   },
      //   {
      //     event: 'message',
      //     remote: { id: '1', metadata: { name: 'peer1' } },
      //     channel: { id: 0, label: 'channel1' },
      //     data: { type: 'channel_id', id: '1' },
      //   },
      //   {
      //     event: 'message',
      //     remote: { id: '1', metadata: { name: 'peer1' } },
      //     channel: { id: 0, label: 'channel1' },
      //     data: { type: 'all', id: '1' },
      //   },
      //   {
      //     event: 'message',
      //     remote: { id: '1', metadata: { name: 'peer1' } },
      //     channel: { id: 1, label: 'channel2' },
      //     data: { type: 'reply', id: '1' },
      //   },
      //   {
      //     event: 'message',
      //     remote: { id: '1', metadata: { name: 'peer1' } },
      //     channel: { id: 1, label: 'channel2' },
      //     data: { type: 'channel_id', id: '1' },
      //   },
      //   {
      //     event: 'message',
      //     remote: { id: '1', metadata: { name: 'peer1' } },
      //     channel: { id: 0, label: 'channel1' },
      //     data: { type: 'all', id: '1' },
      //   },
      //   {
      //     event: 'message',
      //     remote: { id: '1', metadata: { name: 'peer1' } },
      //     channel: { id: 1, label: 'channel2' },
      //     data: { type: 'all', id: '1' },
      //   },
      // ],
      [
        {
          event: 'close',
          channels: 0,
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { id: 0, label: 'channel1' },
        },
        {
          event: 'close',
          channels: 0,
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { id: 1, label: 'channel2' },
        },
      ],
    ],
  });
});

test('media streams', async ({ page }) => {
  await page.goto('./tests/sandbox.html');

});
