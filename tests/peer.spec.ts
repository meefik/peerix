import { test, expect } from '@playwright/test';
import type { Peer } from '../src/index.js';

// Enable debug logging from the page console
const DEBUG = 'peerix:*';

test('peer connections', async ({ page }) => {
  await page.goto('./tests/sandbox.html');

  if (DEBUG) {
    page.on('console', (msg) => {
      console.log(`CONSOLE: ${msg.text()}`);
    });
  }

  const [peer1, peer2] = await page.evaluate(async (debug) => {
    const { Peer } = await import('../src/index.js');

    if (debug) localStorage.debug = debug;

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
      peer1.open('default'),
      peer2.open('default'),
    ]);

    await Promise.all([
      peer1.join({ room: 'test', metadata: { name: 'peer1' } }),
      peer2.join({ room: 'test', metadata: { name: 'peer2' } }),
    ]);

    return [await peer1Promise, await peer2Promise];
  }, DEBUG);

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

  const [peer1, peer2] = await page.evaluate(async (debug) => {
    const { Peer } = await import('../src/index.js');

    if (debug) localStorage.debug = debug;

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
            channel: { label: channel.label },
          });

          // wait for all channels to be opened before sending messages
          if (openedChannels.length >= quorum) {
            resolve(openedChannels);

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
            channel: { label: channel.label },
            data,
          });

          // wait for all messages to be received before resolving
          if (messages.length >= 4) {
            resolve(messages);
            Promise.all([
              peer.close({ label: 'channel0' }),
              peer.close({ label: 'channel1' }),
            ]);
          }
        })),
        // data channel close event
        new Promise((resolve: (value: any) => void) => peer.on('close', (e) => {
          const { remote, channel } = e;

          closedChannels.push({
            event: 'close',
            channels: peer.channels.size,
            remote: { id: remote.id, metadata: remote.metadata },
            channel: { label: channel.label },
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

    peer1.open({ label: 'channel0' });
    peer2.open({ label: 'channel0' });
    peer1.open({ label: 'channel1' });
    peer2.open({ label: 'channel1' });

    peer1.join({ room: 'test', metadata: { name: 'peer1' } });
    peer2.join({ room: 'test', metadata: { name: 'peer2' } });

    return [await peer1Promise, await peer2Promise];
  }, DEBUG);

  expect({ peer1, peer2 }).toEqual({
    peer1: [
      [
        {
          event: 'open',
          channels: 2,
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { label: 'channel0' },
        },
        {
          event: 'open',
          channels: 2,
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { label: 'channel1' },
        },
      ],
      [
        {
          event: 'message',
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { label: 'channel0' },
          data: { type: 'by-label', peer: '2', channel: 'channel0' },
        },
        {
          event: 'message',
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { label: 'channel1' },
          data: { type: 'by-label', peer: '2', channel: 'channel1' },
        },
        {
          event: 'message',
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { label: 'channel0' },
          data: { type: 'to-all', peer: '2' },
        },
        {
          event: 'message',
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { label: 'channel1' },
          data: { type: 'to-all', peer: '2' },
        },
      ],
      [
        {
          event: 'close',
          channels: 0,
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { label: 'channel0' },
        },
        {
          event: 'close',
          channels: 0,
          remote: { id: '2', metadata: { name: 'peer2' } },
          channel: { label: 'channel1' },
        },
      ],
    ],
    peer2: [
      [
        {
          event: 'open',
          channels: 2,
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { label: 'channel0' },
        },
        {
          event: 'open',
          channels: 2,
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { label: 'channel1' },
        },
      ],
      [
        {
          event: 'message',
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { label: 'channel0' },
          data: { type: 'by-label', peer: '1', channel: 'channel0' },
        },
        {
          event: 'message',
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { label: 'channel1' },
          data: { type: 'by-label', peer: '1', channel: 'channel1' },
        },
        {
          event: 'message',
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { label: 'channel0' },
          data: { type: 'to-all', peer: '1' },
        },
        {
          event: 'message',
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { label: 'channel1' },
          data: { type: 'to-all', peer: '1' },
        },
      ],
      [
        {
          event: 'close',
          channels: 0,
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { label: 'channel0' },
        },
        {
          event: 'close',
          channels: 0,
          remote: { id: '1', metadata: { name: 'peer1' } },
          channel: { label: 'channel1' },
        },
      ],
    ],
  });
});

test('media streams', async ({ page }) => {
  await page.goto('./tests/sandbox.html');

  // User gesture is required to use AudioContext in some browsers
  await page.click('body');

  if (DEBUG) {
    page.on('console', (msg) => {
      console.log(`CONSOLE: ${msg.text()}`);
    });
  }

  const [peer1, peer2] = await page.evaluate(async (debug) => {
    const { Peer } = await import('../src/index.js');

    if (debug) localStorage.debug = debug;

    const createSyntheticMediaStream = ({ width = 640, height = 360, video = true, audio = true } = {}) => {
      const tracks = [];
      let draw = () => { };

      // 1. Generate the Video Stream (using Canvas)
      if (video) {
        const canvas = document.createElement('canvas');
        canvas.width = width;
        canvas.height = height;
        const ctx = canvas.getContext('2d');
        if (!ctx) {
          throw new Error('Failed to create canvas context');
        }

        // Simple animation loop to make the video "active"
        draw = () => {
          ctx.fillStyle = 'black';
          ctx.fillRect(0, 0, canvas.width, canvas.height);
          ctx.fillStyle = 'lime';
          ctx.font = `${Math.min(canvas.width, canvas.height) * 0.08}px monospace`;
          ctx.textAlign = 'center';
          ctx.fillText(`Synthetic Feed: ${new Date().toLocaleTimeString()}`, canvas.width / 2, canvas.height / 2);
          if (syntheticStream?.active) {
            requestAnimationFrame(draw);
          }
        }

        // Capture the canvas at 15 frames per second
        const videoStream = canvas.captureStream(15);

        tracks.push(...videoStream.getVideoTracks());
      }

      // 2. Generate the Audio Stream (using Web Audio API)
      if (audio) {
        const audioCtx = new window.AudioContext();
        const oscillator = audioCtx.createOscillator();
        const dst = audioCtx.createMediaStreamDestination();

        oscillator.type = 'sine';
        oscillator.frequency.setValueAtTime(440, audioCtx.currentTime); // A4 note
        oscillator.connect(dst);
        oscillator.start();

        tracks.push(...dst.stream.getAudioTracks());
      }

      // 3. Combine into a single MediaStream
      const syntheticStream = new MediaStream(tracks);

      draw();

      return syntheticStream;
    };

    const peer1 = new Peer({ id: '1' });
    const peer2 = new Peer({ id: '2' });

    const createPeerPromise = (peer: Peer) => {
      const stack = [] as any[];

      return Promise.all([
        // track published event
        new Promise((resolve: (value: any) => void) => peer.on('publish', (e) => {
          const { remote, stream, track } = e;

          stack.push({
            event: 'publish',
            remote: { id: remote.id, metadata: remote.metadata },
            stream: {
              active: stream.active,
              videoTracks: stream.getVideoTracks().length,
              audioTracks: stream.getAudioTracks().length,
            },
            track: {
              kind: track.kind,
              enabled: track.enabled,
              readyState: track.readyState
            },
          });

          if (stack.length >= 2) {
            resolve({ peer: peer.id, stack });
          }
        })),
        // track unpublished event
        // new Promise((resolve: (value: any) => void) => peer.on('unpublish', (e) => {
        //   const { remote, stream, track } = e;
        //   resolve({
        //     event: 'unpublish',
        //     remote: { id: remote.id, metadata: remote.metadata },
        //     stream: {
        //       id: stream.id,
        //       active: stream.active,
        //       videoTracks: stream.getVideoTracks().length,
        //       audioTracks: stream.getAudioTracks().length,
        //     },
        //     track: { kind: track.kind, enabled: track.enabled, readyState: track.readyState },
        //   });
        // })),
      ]);
    };

    const peer1Promise = createPeerPromise(peer1);
    const peer2Promise = createPeerPromise(peer2);

    const stream1 = createSyntheticMediaStream({ width: 640, height: 360, video: true, audio: true });
    const stream2 = createSyntheticMediaStream({ width: 640, height: 360, video: true, audio: true });

    await Promise.all([
      peer1.publish({ id: 'stream1', stream: stream1, managed: true }),
      peer2.publish({ id: 'stream2', stream: stream2, managed: true }),
    ]);

    await Promise.all([
      peer1.join({ room: 'test', metadata: { name: 'peer1' } }),
      peer2.join({ room: 'test', metadata: { name: 'peer2' } }),
    ]);

    return [await peer1Promise, await peer2Promise];
  }, DEBUG);

  expect({ peer1, peer2 }).toEqual({
    peer1: [
      {
        peer: '1',
        stack: [
          {
            event: 'publish',
            remote: { id: '2', metadata: { name: 'peer2' } },
            stream: { active: true, videoTracks: 1, audioTracks: 1 },
            track: { kind: 'audio', enabled: true, readyState: 'live' },
          },
          {
            event: 'publish',
            remote: { id: '2', metadata: { name: 'peer2' } },
            stream: { active: true, videoTracks: 1, audioTracks: 1 },
            track: { kind: 'video', enabled: true, readyState: 'live' },
          },
        ],
      },
    ],
    peer2: [
      {
        peer: '2',
        stack: [
          {
            event: 'publish',
            remote: { id: '1', metadata: { name: 'peer1' } },
            stream: { active: true, videoTracks: 1, audioTracks: 1 },
            track: { kind: 'audio', enabled: true, readyState: 'live' },
          },
          {
            event: 'publish',
            remote: { id: '1', metadata: { name: 'peer1' } },
            stream: { active: true, videoTracks: 1, audioTracks: 1 },
            track: { kind: 'video', enabled: true, readyState: 'live' },
          },
        ],
      },
    ],
  });
});
