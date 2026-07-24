export default [
  {
    id: "T-001",
    title: "Peer Connections",
    defaults: { timeout: 10000 },
    rooms: {
      "1": { id: "sandbox" },
      "2": { id: "sandbox" },
    },
    steps: [
      {
        room: "1",
        call: "join",
        args: [{ name: "peer1" }],
      },
      {
        room: "2",
        call: "join",
        args: [{ name: "peer2" }],
      },
      {
        room: "1",
        wait: "connection",
        where: { state: "new", peer: { metadata: { name: "peer2" } } },
      },
      {
        room: "2",
        wait: "connection",
        where: { state: "new", peer: { metadata: { name: "peer1" } } },
      },
      {
        room: "1",
        wait: "connection",
        where: { state: "connecting", peer: { metadata: { name: "peer2" } } },
      },
      {
        room: "2",
        wait: "connection",
        where: { state: "connecting", peer: { metadata: { name: "peer1" } } },
      },
      {
        room: "1",
        wait: "connection",
        where: { state: "connected", peer: { metadata: { name: "peer2" } } },
      },
      {
        room: "2",
        wait: "connection",
        where: { state: "connected", peer: { metadata: { name: "peer1" } } },
      },
      { room: "1", call: "leave" },
      { room: "2", call: "leave" },
      {
        room: "1",
        wait: "connection",
        where: { state: "closed", peer: { metadata: { name: "peer2" } } },
      },
      {
        room: "2",
        wait: "connection",
        where: { state: "closed", peer: { metadata: { name: "peer1" } } },
      },
    ],
  },
  {
    id: "T-002",
    title: "Data Channels",
    defaults: { timeout: 10000 },
    rooms: {
      "1": { id: "sandbox" },
      "2": { id: "sandbox" },
    },
    steps: [
      { room: "1", call: "open", args: [{ label: "channel1" }] },
      { room: "2", call: "open", args: [{ label: "channel1" }] },
      {
        room: "1",
        call: "join",
        args: [{ name: "peer1" }],
      },
      {
        room: "2",
        call: "join",
        args: [{ name: "peer2" }],
      },
      {
        room: "1",
        wait: "channel:open",
        where: { peer: { metadata: { name: "peer2" } }, label: "channel1" },
      },
      {
        room: "2",
        wait: "channel:open",
        where: { peer: { metadata: { name: "peer1" } }, label: "channel1" },
      },
      {
        room: "1",
        call: "send",
        args: ["Hello peer! I am peer1.", { label: "channel1" }],
      },
      {
        room: "2",
        call: "send",
        args: ["Hello peer! I am peer2.", { label: "channel1" }],
      },
      {
        room: "1",
        wait: "channel:message",
        where: {
          peer: { metadata: { name: "peer2" } },
          channel: { label: "channel1" },
          label: "channel1",
          data: "Hello peer! I am peer2.",
        },
      },
      {
        room: "2",
        wait: "channel:message",
        where: {
          peer: { metadata: { name: "peer1" } },
          channel: { label: "channel1" },
          label: "channel1",
          data: "Hello peer! I am peer1.",
        },
      },
      { room: "1", call: "close", args: [{ label: "channel1" }] },
      { room: "2", call: "close", args: [{ label: "channel1" }] },
      {
        room: "1",
        wait: "channel:close",
        where: {
          peer: { metadata: { name: "peer2" } },
          channel: { label: "channel1" },
          label: "channel1",
        },
      },
      {
        room: "2",
        wait: "channel:close",
        where: {
          peer: { metadata: { name: "peer1" } },
          channel: { label: "channel1" },
          label: "channel1",
        },
      },
      { room: "1", call: "leave" },
      { room: "2", call: "leave" },
    ],
  },
  {
    id: "T-003",
    title: "Media Streams",
    defaults: { timeout: 10000 },
    rooms: {
      "1": { id: "sandbox" },
      "2": { id: "sandbox" },
    },
    steps: [
      {
        room: "1",
        call: "join",
        args: [{ name: "peer1" }],
      },
      {
        room: "2",
        call: "join",
        args: [{ name: "peer2" }],
      },
      {
        room: "1",
        call: "share",
        args: [{ label: "camera", stream: { video: true, audio: true } }],
      },
      {
        room: "2",
        call: "share",
        args: [{ label: "camera", stream: { video: true, audio: true } }],
      },
      {
        room: "2",
        wait: "stream:add",
        where: {
          peer: { metadata: { name: "peer1" } },
          stream: { active: true },
          label: "camera",
        },
      },
      {
        room: "2",
        wait: "track:add",
        where: {
          peer: { metadata: { name: "peer1" } },
          track: { kind: "video" },
          stream: { active: true },
          label: "camera",
        },
      },
      {
        room: "2",
        wait: "track:add",
        where: {
          peer: { metadata: { name: "peer1" } },
          track: { kind: "audio" },
          stream: { active: true },
          label: "camera",
        },
      },
      {
        room: "1",
        wait: "stream:add",
        where: {
          peer: { metadata: { name: "peer2" } },
          stream: { active: true },
          label: "camera",
        },
      },
      {
        room: "1",
        wait: "track:add",
        where: {
          peer: { metadata: { name: "peer2" } },
          track: { kind: "video" },
          stream: { active: true },
          label: "camera",
        },
      },
      {
        room: "1",
        wait: "track:add",
        where: {
          peer: { metadata: { name: "peer2" } },
          track: { kind: "audio" },
          stream: { active: true },
          label: "camera",
        },
      },
      { room: "1", call: "unshare", args: [{ label: "camera" }] },
      { room: "2", call: "unshare", args: [{ label: "camera" }] },
      {
        room: "2",
        wait: "track:remove",
        where: {
          peer: { metadata: { name: "peer1" } },
          track: { kind: "audio" },
          stream: { active: false },
          label: "camera",
        },
      },
      {
        room: "2",
        wait: "track:remove",
        where: {
          peer: { metadata: { name: "peer1" } },
          track: { kind: "video" },
          stream: { active: false },
          label: "camera",
        },
      },
      {
        room: "2",
        wait: "stream:remove",
        where: {
          peer: { metadata: { name: "peer1" } },
          stream: { active: false },
          label: "camera",
        },
      },
      {
        room: "1",
        wait: "track:remove",
        where: {
          peer: { metadata: { name: "peer2" } },
          track: { kind: "audio" },
          stream: { active: false },
          label: "camera",
        },
      },
      {
        room: "1",
        wait: "track:remove",
        where: {
          peer: { metadata: { name: "peer2" } },
          track: { kind: "video" },
          stream: { active: false },
          label: "camera",
        },
      },
      {
        room: "1",
        wait: "stream:remove",
        where: {
          peer: { metadata: { name: "peer2" } },
          stream: { active: false },
          label: "camera",
        },
      },
      { room: "1", call: "leave" },
      { room: "2", call: "leave" },
    ],
  },
];
