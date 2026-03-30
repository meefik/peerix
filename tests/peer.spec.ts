import { test, expect } from '@playwright/test';
import type { Peer } from '../src/index.js';

test('peer connection', async ({ page }) => {
  await page.goto('./tests/sandbox.html');

  const [peer1, peer2] = await page.evaluate(async () => {
    const { Peer } = await import('../src/index.js');

    const peer1 = new Peer({ id: '1' });
    const peer2 = new Peer({ id: '2' });

    let connected = 0;

    const createPeerPromise = (peer: Peer, quorum: number) => {
      return Promise.all([
        // new connection event
        new Promise((resolve: (value: any) => void) => peer.on('state', (e) => {
          const { remote, state } = e;
          if (state === 'new') {
            resolve({ id: remote.id, metadata: remote.metadata, state: remote.state });
          }
        })),
        // connecting event
        new Promise((resolve: (value: any) => void) => peer.on('state', (e) => {
          const { remote, state } = e;
          if (state === 'connecting') {
            resolve({ id: remote.id, metadata: remote.metadata, state });
          }
        })),
        // connected event
        new Promise((resolve: (value: any) => void) => peer.on('state', (e) => {
          const { remote, state } = e;
          if (state === 'connected') {
            resolve({ id: remote.id, metadata: remote.metadata, state });
            if (++connected >= quorum) peer.leave();
          }
        })),
        // disconnected event
        new Promise((resolve: (value: any) => void) => peer.on('state', (e) => {
          const { remote, state } = e;
          if (state === 'closed') {
            resolve({ id: remote.id, metadata: remote.metadata, state });
          }
        })),
      ]);
    };

    const peer1Promise = createPeerPromise(peer1, 2);
    const peer2Promise = createPeerPromise(peer2, 2);

    peer1.open(0);
    peer2.open(0);

    peer1.join({ room: 'test', metadata: { name: 'peer1' } });
    peer2.join({ room: 'test', metadata: { name: 'peer2' } });

    return [await peer1Promise, await peer2Promise];
  });

  expect({ peer1, peer2 }).toEqual({
    peer1: [
      { id: '2', state: 'new', metadata: { name: 'peer2' } },
      { id: '2', state: 'connecting', metadata: { name: 'peer2' } },
      { id: '2', state: 'connected', metadata: { name: 'peer2' } },
      { id: '2', state: 'closed', metadata: { name: 'peer2' } },
    ],
    peer2: [
      { id: '1', state: 'new', metadata: { name: 'peer1' } },
      { id: '1', state: 'connecting', metadata: { name: 'peer1' } },
      { id: '1', state: 'connected', metadata: { name: 'peer1' } },
      { id: '1', state: 'closed', metadata: { name: 'peer1' } },
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
      return Promise.all([
        // data channel open event
        new Promise((resolve: (value: any) => void) => peer.on('open', (e) => {
          const { remote, channel } = e;
          resolve({ remote: { id: remote.id, metadata: remote.metadata }, channel: { id: channel.id, label: channel.label } });
          channel.send('hello from ' + peer.id);
        })),
        // data channel message event
        new Promise((resolve: (value: any) => void) => peer.on('message', (e) => {
          const { remote, channel, data } = e;
          resolve({ remote: { id: remote.id, metadata: remote.metadata }, channel: { id: channel.id, label: channel.label }, data });
        })),
      ]);
    };

    const peer1Promise = createPeerPromise(peer1);
    const peer2Promise = createPeerPromise(peer2);

    peer1.open({ id: 0, label: 'channel1' });
    peer2.open({ id: 0, label: 'channel1' });

    peer1.join({ room: 'test', metadata: { name: 'peer1' } });
    peer2.join({ room: 'test', metadata: { name: 'peer2' } });

    return [await peer1Promise, await peer2Promise];
  });

  expect({ peer1, peer2 }).toEqual({
    peer1: [
      { remote: { id: '2', metadata: { name: 'peer2' } }, channel: { id: 0, label: 'channel1' } },
      { remote: { id: '2', metadata: { name: 'peer2' } }, channel: { id: 0, label: 'channel1' }, data: 'hello from 2' },
    ],
    peer2: [
      { remote: { id: '1', metadata: { name: 'peer1' } }, channel: { id: 0, label: 'channel1' } },
      { remote: { id: '1', metadata: { name: 'peer1' } }, channel: { id: 0, label: 'channel1' }, data: 'hello from 1' },
    ],
  });
});

test('media streams', async ({ page }) => {
  await page.goto('./tests/sandbox.html');

});
