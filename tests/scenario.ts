export default [
  {
    title: 'Peer Connections',
    defaults: { timeout: 10000 },
    peers: [
      { id: '1' },
      { id: '2' },
    ],
    steps: [
      { peer: '1', call: 'join', args: [{ room: 'test', metadata: { name: 'peer1' } }] },
      { peer: '2', call: 'join', args: [{ room: 'test', metadata: { name: 'peer2' } }] },

      { peer: '1', call: 'open', args: [{}] },

      { peer: '1', wait: 'connection', where: { state: 'new', remote: { id: '2' } } },
      { peer: '2', wait: 'connection', where: { state: 'new', remote: { id: '1' } } },

      { peer: '1', wait: 'connection', where: { state: 'connecting', remote: { id: '2' } } },
      { peer: '2', wait: 'connection', where: { state: 'connecting', remote: { id: '1' } } },

      { peer: '1', wait: 'connection', where: { state: 'connected', remote: { id: '2' } } },
      { peer: '2', wait: 'connection', where: { state: 'connected', remote: { id: '1' } } },

      { peer: '1', call: 'leave' },
      { peer: '2', call: 'leave' },

      { peer: '1', wait: 'connection', where: { state: 'closed', remote: { id: '2' } } },
      { peer: '2', wait: 'connection', where: { state: 'closed', remote: { id: '1' } } },
    ],
  },
  {
    title: 'Data Channels',
    defaults: { timeout: 10000 },
    peers: [
      { id: '1' },
      { id: '2' },
    ],
    steps: [
      { peer: '1', call: 'open', args: [{ label: 'channel1' }] },
      { peer: '2', call: 'open', args: [{ label: 'channel1' }] },

      { peer: '1', call: 'join', args: [{ room: 'test', metadata: { name: 'peer1' } }] },
      { peer: '2', call: 'join', args: [{ room: 'test', metadata: { name: 'peer2' } }] },

      { peer: '1', wait: 'channel:open', where: { remote: { id: '2' }, label: 'channel1' } },
      { peer: '2', wait: 'channel:open', where: { remote: { id: '1' }, label: 'channel1' } },

      { peer: '1', call: 'send', args: ['Hello peer! I am peer1.', { label: 'channel1' }] },
      { peer: '2', call: 'send', args: ['Hello peer! I am peer2.', { label: 'channel1' }] },

      { peer: '1', call: 'send', args: ['Hello all! I am peer1.'] },
      { peer: '2', call: 'send', args: ['Hello all! I am peer2.'] },

      { peer: '1', wait: 'channel:message', where: { remote: { id: '2' }, channel: { label: 'channel1' }, label: 'channel1', data: 'Hello peer! I am peer2.' } },
      { peer: '2', wait: 'channel:message', where: { remote: { id: '1' }, channel: { label: 'channel1' }, label: 'channel1', data: 'Hello peer! I am peer1.' } },

      { peer: '1', wait: 'channel:message', where: { remote: { id: '2' }, channel: { label: 'channel1' }, label: 'channel1', data: 'Hello all! I am peer2.' } },
      { peer: '2', wait: 'channel:message', where: { remote: { id: '1' }, channel: { label: 'channel1' }, label: 'channel1', data: 'Hello all! I am peer1.' } },

      { peer: '1', call: 'close', args: [{ label: 'channel1' }] },
      { peer: '2', call: 'close', args: [{ label: 'channel1' }] },

      { peer: '1', wait: 'channel:close', where: { remote: { id: '2' }, channel: { label: 'channel1' }, label: 'channel1' } },
      { peer: '2', wait: 'channel:close', where: { remote: { id: '1' }, channel: { label: 'channel1' }, label: 'channel1' } },

      { peer: '1', call: 'leave' },
      { peer: '2', call: 'leave' },
    ],
  },
  {
    title: 'Media Streams',
    defaults: { timeout: 10000 },
    peers: [
      { id: '1' },
      { id: '2' },
    ],
    steps: [
      { peer: '1', call: 'join', args: [{ room: 'test', metadata: { name: 'peer1' } }] },
      { peer: '2', call: 'join', args: [{ room: 'test', metadata: { name: 'peer2' } }] },

      { peer: '1', call: 'publish', args: [{ label: 'camera', stream: { video: true, audio: true } }] },
      { peer: '2', wait: 'stream:add', where: { remote: { id: '1' }, stream: { active: true }, label: 'camera' } },
      { peer: '2', wait: 'track:add', where: { remote: { id: '1' }, track: { kind: 'video' }, stream: { active: true }, label: 'camera' } },
      { peer: '2', wait: 'track:add', where: { remote: { id: '1' }, track: { kind: 'audio' }, stream: { active: true }, label: 'camera' } },

      { peer: '2', call: 'publish', args: [{ label: 'camera', stream: { video: true, audio: true } }] },
      { peer: '1', wait: 'stream:add', where: { remote: { id: '2' }, stream: { active: true }, label: 'camera' } },
      { peer: '1', wait: 'track:add', where: { remote: { id: '2' }, track: { kind: 'video' }, stream: { active: true }, label: 'camera' } },
      { peer: '1', wait: 'track:add', where: { remote: { id: '2' }, track: { kind: 'audio' }, stream: { active: true }, label: 'camera' } },

      { peer: '1', call: 'unpublish', args: [{ label: 'camera' }] },
      { peer: '2', wait: 'track:remove', where: { remote: { id: '1' }, track: { kind: 'audio' }, stream: { active: false }, label: 'camera' } },
      { peer: '2', wait: 'track:remove', where: { remote: { id: '1' }, track: { kind: 'video' }, stream: { active: false }, label: 'camera' } },
      { peer: '2', wait: 'stream:remove', where: { remote: { id: '1' }, stream: { active: false }, label: 'camera' } },

      { peer: '2', call: 'unpublish', args: [{ label: 'camera' }] },
      { peer: '1', wait: 'track:remove', where: { remote: { id: '2' }, track: { kind: 'audio' }, stream: { active: false }, label: 'camera' } },
      { peer: '1', wait: 'track:remove', where: { remote: { id: '2' }, track: { kind: 'video' }, stream: { active: false }, label: 'camera' } },
      { peer: '1', wait: 'stream:remove', where: { remote: { id: '2' }, stream: { active: false }, label: 'camera' } },

      { peer: '1', call: 'leave' },
      { peer: '2', call: 'leave' },
    ],
  },
];
