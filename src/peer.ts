import type { Driver } from './drivers/driver.js';
import log from './utils/logger.js';
import { RemotePeer } from './remote.js';
import { Signaler } from './signaler.js';
import { IceCandidateQueue } from './ice.js';
import { PeerixError } from './error.js';
import { UUIDv4 } from './utils/helpers.js';
import { EventEmitter } from './utils/emitter.js';

// Signaling message types
const SIGNAL_JOIN = 1;
const SIGNAL_OFFER = 2;
const SIGNAL_ANSWER = 3;
const SIGNAL_CANDIDATE = 4;
const SIGNAL_LEAVE = 5;

/**
 * Peer class for managing WebRTC peer connections, signaling, media streams, and data channels.
 * 
 * @group Peers
 * @example
 * ```javascript
 * // create a new peer
 * // using default in-memory signaling driver
 * const peer = new Peer();
 * 
 * // listen for connection state changes
 * peer.on('connection', (e) => {
 *   const { remote } = e;
 *   console.log(`Peer ${remote.id} connection state has changed:`, remote.state);
 * });
 *
 * // listen for open channel event
 * peer.on('channel:open', (e) => {
 *   const { remote, channel } = e;
 *   // send a message to the connected peer
 *   channel.send('Hello, peer!');
 * });
 *
 * // listen for incoming messages
 * peer.on('channel:message', (e) => {
 *   const { remote, channel, data } = e;
 *   console.log(`Received message from ${remote.id} on channel ${channel.label}:`, data);
 * });
 *
 * // open a data channel
 * peer.open({ label: 'default' });
 *
 * // join a room
 * peer.join({ room: 'room-id' });
 * ```
 */
export class Peer {
  /** Unique identifier for the local peer. */
  readonly id: string;
  /** Active remote peers indexed by remote peer id. */
  readonly connections: Map<string, RemotePeer>;
  /** Published local streams indexed by application-level stream label. */
  readonly streams: Map<string, StreamOptions>;
  /** Configured local data channels indexed by channel label. */
  readonly channels: Map<string, ChannelOptions>;
  /** Attachable extensions. */
  readonly addons: Set<any>;

  /** Indicates whether the peer is currently active (joined a room). */
  active: boolean;
  /** Current room name. Empty until join() is called. */
  room: string;
  /** Optional metadata announced to other peers in signaling messages. */
  metadata?: any;

  #signaler: Signaler;
  #iceServers: IceServer[];
  #iceTransportPolicy: IceTransportPolicy;
  #connectionTimeout: number;
  #emitter: EventEmitter<PeerEvents>;
  #candidateQueue: IceCandidateQueue;
  #verify?: (options: { id: string; metadata?: any; }) => Promise<boolean> | boolean;

  /**
   * Creates an instance of Peer.
   * 
   * @example
   * ```javascript
   * // create a new peer with default options
   * const peer = new Peer();
   * ```
   *
   * @param options Peer configuration options.
   */
  constructor(options?: PeerOptions) {
    const {
      id = UUIDv4(),
      driver,
      iceServers = [],
      iceTransportPolicy = 'all',
      connectionTimeout = 15,
      compress,
      hash,
      encrypt,
      encryptionKey,
    } = options || {};

    this.active = false;
    this.id = id;
    this.room = '';
    this.connections = new Map();
    this.streams = new Map();
    this.channels = new Map();
    this.addons = new Set();
    this.#signaler = new Signaler({
      driver,
      compress,
      hash,
      encrypt,
      encryptionKey,
      handler: async (message) => {
        try {
          await this.#signalHandler(message);
        }
        catch (err) {
          const error = new PeerixError(err, 'PEER_SIGNALING_ERROR');
          this.emit('error', { id: this.id, name: 'error', error });
        }
      }
    });
    this.#iceServers = iceServers;
    this.#iceTransportPolicy = iceTransportPolicy;
    this.#connectionTimeout = connectionTimeout;
    this.#emitter = new EventEmitter(this);
    this.#candidateQueue = new IceCandidateQueue();
  }

  /**
   * Join a room and start listening for incoming connections.
   * 
   * @example
   * ```javascript
   * // join a room with ID 'room-id' and custom metadata
   * peer.join({ room: 'room-id', metadata: { name: 'Alice' } });
   * ```
   *
   * @param options Room name or join options.
   */
  async join(options?: string | PeerJoinOptions) {
    if (this.active) return;
    this.active = true;

    const { room = 'default', metadata, verify } =
      typeof options === 'object' ? options : { room: options };

    this.room = room;
    this.metadata = metadata;
    this.#verify = verify;

    log('peer:join', { id: this.id, room: this.room, metadata: this.metadata });

    await this.#signaler.subscribe([this.room], [this.room, this.id]);

    await this.#signaler.dispatch(
      [this.room],
      [SIGNAL_JOIN, this.id, this.metadata],
    );
  }

  /**
   * Leave the current room and close all active remote connections.
   * 
   * @example
   * ```javascript
   * // leave the current room
   * peer.leave();
   * ```
   */
  async leave() {
    if (!this.active) return;

    log('peer:leave', { id: this.id, room: this.room, metadata: this.metadata });

    await this.#signaler.unsubscribe([this.room], [this.room, this.id]);

    for (const remote of this.connections.values()) {
      remote.dispose();
    }
    this.connections.clear();

    this.#candidateQueue.clear();

    await this.#signaler.dispatch(
      [this.room],
      [SIGNAL_LEAVE, this.id],
    );

    this.active = false;
  }

  /**
   * Publish new or update an existing media stream to all remote peers 
   * including new ones that join later.
   *
   * If you pass a MediaStream instance directly, it will be published under 
   * a label equal to the stream id. Otherwise, you can specify an explicit 
   * label in the options object. If a stream with the same label already 
   * exists, it will be updated and its tracks will be added/removed as needed
   * to minimize renegotiations.
   * 
   * If the stream is published with the `managed` option, its tracks will be
   * automatically stopped when the stream is unpublished or replaced with 
   * a new stream.
   * 
   * @example
   * ```javascript
   * // get a media stream from the user's camera and microphone
   * const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
   * 
   * // publish a media stream with an explicit label
   * peer.publish({ label: 'camera', stream, managed: true });
   * ```
   *
   * @param options Stream descriptor or MediaStream instance.
   * @returns The published MediaStream instance if successful, or undefined.
   */
  async publish(options: MediaStream | StreamOptions): Promise<MediaStream | void> {
    if (options instanceof MediaStream) {
      options = { label: options.id, stream: options };
    }
    const { label: rawLabel = 'default', stream, ...opts } = options || {};
    const label = String(rawLabel);

    if (stream instanceof MediaStream === false || !stream.getTracks().length) {
      return;
    }

    const {
      stream: newStream = new MediaStream(),
      managed,
    } = this.streams.get(label) || {};

    for (const track of newStream.getTracks()) {
      if (!stream.getTracks().find(t => t.id === track.id)) {
        newStream.removeTrack(track);
        if (managed && track.readyState !== 'ended') {
          track.stop();
        }
      }
    }
    for (const track of stream.getTracks()) {
      if (!newStream.getTracks().find(t => t.id === track.id)) {
        newStream.addTrack(track);
      }
    }

    const newStreamOptions = { label, stream: newStream, ...opts };

    log('peer:publish', { id: this.id, ...newStreamOptions });

    this.streams.set(label, newStreamOptions);

    for (const remote of this.connections.values()) {
      await remote.publish(newStreamOptions);
    }

    return newStream;
  }

  /**
   * Stop publishing a previously published media stream with the given label 
   * and remove it from all remote peers.
   * 
   * If you pass a MediaStream instance directly, it will be unpublished based 
   * on its id as label. Otherwise, you can specify the label in the options 
   * object or pass it directly as a string. 
   * 
   * If the stream was published with the `managed` option, its tracks will be 
   * stopped automatically.
   * 
   * @example
   * ```javascript
   * // unpublish a media stream with an explicit label
   * peer.unpublish({ label: 'camera' });
   * ```
   *
   * @param options Object containing a stream label or MediaStream instance.
   * @returns The unpublished MediaStream instance, or undefined.
   */
  async unpublish(options: MediaStream | { label?: string; }): Promise<MediaStream | void> {
    if (options instanceof MediaStream) {
      options = { label: options.id };
    }
    const { label: rawLabel = 'default' } = options || {};
    const label = String(rawLabel);

    const oldStreamOptions = this.streams.get(label);
    const { stream, managed } = oldStreamOptions || {};

    log('peer:unpublish', { id: this.id, label, stream });

    this.streams.delete(label);

    if (!stream) return;

    if (managed) {
      for (const track of stream.getTracks()) {
        if (track.readyState !== 'ended') {
          track.stop();
        }
      }
    }

    for (const remote of this.connections.values()) {
      await remote.unpublish({ label });
    }

    return stream;
  }

  /**
   * Open a data channel with the given label and options to all remote peers.
   * If a channel with the same label already exists, it will be reused.
   * 
   * You can open a channel with the same label on both local and remote peers
   * or only on one side. In any case, only one channel will be created for 
   * each label. You can send data through the channel in both directions.
   * 
   * @example
   * ```javascript
   * // open a channel with label 'chat'
   * peer.open({ label: 'chat' });
   * ```
   *
   * @param options Channel options or channel label.
   */
  async open(options: string | ChannelOptions) {
    const { label: rawLabel = 'default', ...channelOptions } =
      typeof options === 'object' ? options : { label: options };
    const label = String(rawLabel);

    log('peer:open', { id: this.id, label, ...channelOptions });

    this.channels.set(label, { label, ...channelOptions });

    for (const remote of this.connections.values()) {
      await remote.open({ label, ...channelOptions });
    }
  }

  /**
   * Close a previously opened data channel with the given label 
   * and remove it from all remote peers.
   * 
   * @example
   * ```javascript
   * // close the channel with label 'chat'
   * peer.close({ label: 'chat' });
   * ```
   *
   * @param options Channel label or object containing `label`.
   */
  async close(options: string | { label: string; }) {
    const { label: rawLabel = 'default' } =
      typeof options === 'object' ? options : { label: options };
    const label = String(rawLabel);

    log('peer:close', { id: this.id, label });

    this.channels.delete(label);

    for (const remote of this.connections.values()) {
      await remote.close({ label });
    }
  }

  /**
   * Send a message through data channels.
   *
   * If `options` is omitted, the message is sent to all open channels for every
   * connected remote peer. If `options` is a string, it is treated as channel label.
   * 
   * @example
   * ```javascript
   * // send a message to all channels
   * peer.send('Hello, peers!');
   * // send a message to a specific channel
   * peer.send('Hello, chat channel!', { label: 'chat' });
   * ```
   *
   * @param message Message payload to send. This may be a string, a Blob, an ArrayBuffer, a TypedArray or a DataView object.
   * @param options Optional channel label or object containing `label`.
   */
  send(message: any, options?: string | { label?: string; }) {
    if (!this.active) return;

    const { label: rawLabel } =
      typeof options === 'object' ? options : { label: options };
    const label = typeof rawLabel === 'undefined' ? undefined : String(rawLabel);

    log('peer:send', { id: this.id, label, message });

    for (const remote of this.connections.values()) {
      remote.send(message, { label });
    }
  }

  /**
   * Attach an addon/extension to the peer instance.
   * 
   * @param addon Addon instance to attach.
   */
  async attach(addon: any) {
    await addon.attach(this);
    this.addons.add(addon);
  }

  /**
   * Detach a previously attached addon/extension from the peer instance.
   * 
   * @param addon Addon instance to detach.
   */
  async detach(addon: any) {
    await addon.detach(this);
    this.addons.delete(addon);
  }

  /**
   * Subscribe to one or more peer events.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
  on<K extends keyof PeerEvents>(event: K | K[], handler: (...args: PeerEvents[K]) => void) {
    this.#emitter.on(event, handler);
  }

  /**
   * Subscribe to an event and auto-unsubscribe after first invocation.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
  once<K extends keyof PeerEvents>(event: K | K[], handler: (...args: PeerEvents[K]) => void) {
    this.#emitter.once(event, handler);
  }

  /**
   * Remove a previously registered event listener.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler to remove. If omitted, all handlers for the given event(s) will be removed.
   */
  off<K extends keyof PeerEvents>(event: K | K[], handler?: (...args: PeerEvents[K]) => void) {
    this.#emitter.off(event, handler);
  }

  /**
   * Emit one or more events. 
   * Usually you would not call this method directly.
   *
   * @param event Event name or list of event names.
   * @param args Event payload.
   */
  emit<K extends keyof PeerEvents>(event: K | K[], ...args: PeerEvents[K]) {
    this.#emitter.emit(event, ...args);

    (Array.isArray(event) ? event : [event]).forEach(e => {
      log(`peer:emit:${e}`, ...args);
    });
  }

  /**
   * Create a new RemotePeer instance for an incoming connection or return an existing one.
   * 
   * @param options Options for creating the remote peer connection.
   * @param options.id Remote peer identifier.
   * @param options.metadata Optional metadata announced by the remote peer in signaling messages.
   * @param options.replace If true, an existing connection with the same id will be replaced. Otherwise, it will be reused.
   * @returns The created or existing RemotePeer instance, or void if the connection was rejected.
   */
  async #createRemotePeer(options: { id: string; metadata?: any; replace?: boolean; }) {
    const { id, metadata, replace } = options;

    // verify the incoming request and reject if verification fails
    if (typeof this.#verify === 'function') {
      const verified = await this.#verify({ id, metadata });
      if (!verified) return;
    }

    let remote = this.connections.get(id);
    if (remote && !replace) return remote;
    if (remote && replace) remote.dispose();

    remote = new RemotePeer({
      id,
      metadata,
      room: this.room,
      polite: this.id > id,
      iceServers: this.#iceServers,
      iceTransportPolicy: this.#iceTransportPolicy,
      connectionTimeout: this.#connectionTimeout,
      streams: this.streams,
      channels: this.channels,
    });

    remote.on('offer', async (e) => {
      const { description } = e;
      try {
        await this.#signaler.dispatch(
          [this.room, id],
          [SIGNAL_OFFER, this.id, description, this.metadata],
        );
      }
      catch (err) {
        const error = new PeerixError(err, 'PEER_SIGNALING_ERROR');
        this.emit('error', { id: this.id, name: 'error', error });
      }
    });

    remote.on('answer', async (e) => {
      const { description } = e;
      try {
        await this.#signaler.dispatch(
          [this.room, id],
          [SIGNAL_ANSWER, this.id, description],
        );
      }
      catch (err) {
        const error = new PeerixError(err, 'PEER_SIGNALING_ERROR');
        this.emit('error', { id: this.id, name: 'error', error });
      }
    });

    remote.on('candidate', async (e) => {
      const { candidate } = e;
      try {
        await this.#signaler.dispatch(
          [this.room, id],
          [SIGNAL_CANDIDATE, this.id, candidate],
        );
      }
      catch (err) {
        const error = new PeerixError(err, 'PEER_SIGNALING_ERROR');
        this.emit('error', { id: this.id, name: 'error', error });
      }
    });

    remote.on('connection:closed', (e) => {
      this.connections.delete(id);
      this.#candidateQueue.clear(id);
    });

    remote.on('connection', (e) => {
      this.emit(['connection', e.name], { ...e, id: this.id, remote });
    });

    remote.on('channel', (e) => {
      this.emit(['channel', e.name], { ...e, id: this.id, remote });
    });

    remote.on('stream', (e) => {
      this.emit(['stream', e.name], { ...e, id: this.id, remote });
    });

    remote.on('track', (e) => {
      this.emit(['track', e.name], { ...e, id: this.id, remote });
    });

    remote.on('error', (e) => {
      this.emit('error', { ...e, id: this.id });
    });

    this.connections.set(id, remote);

    this.emit(
      ['connection', 'connection:new'],
      { id: this.id, name: 'connection:new', remote, state: 'new' }
    );

    return remote;
  }

  /**
   * Handle an incoming message dispatched by the driver.
   *
   * Processes signaling message to establish, negotiate, and tear down
   * peer connections.
   *
   * @param message Incoming message from the signaling driver.
   */
  async #signalHandler(message: any) {
    if (!this.active || !message) return;

    const [type, id, ...payload] = message;
    if (!type || !id || this.id === id) return;

    log('peer:signal', { id: this.id, type, remote: id, payload });

    // handle incoming connection
    if (type === SIGNAL_JOIN) {
      const [metadata] = payload;
      const remote = await this.#createRemotePeer({ id, metadata, replace: true });
      if (!remote) return;

      return;
    }

    // set remote description for offer and create answer
    if (type === SIGNAL_OFFER) {
      const [description, metadata] = payload;
      const remote = await this.#createRemotePeer({ id, metadata, replace: false });
      if (!remote) return;

      await remote.applyDescription(description,
        (description) => {
          const candidates: RTCIceCandidateInit[] = [];
          for (const candidate of this.#candidateQueue.pull(id, description)) {
            candidates.push(candidate);
          }
          return candidates;
        });

      return;
    }

    // set remote description for answer
    if (type === SIGNAL_ANSWER) {
      const [description] = payload;
      const remote = this.connections.get(id);
      if (!remote) return;

      await remote.applyDescription(description,
        (description) => {
          const candidates: RTCIceCandidateInit[] = [];
          for (const candidate of this.#candidateQueue.pull(id, description)) {
            candidates.push(candidate);
          }
          return candidates;
        });

      return;
    }

    // add ice candidate
    if (type === SIGNAL_CANDIDATE) {
      const [candidate] = payload;
      const remote = this.connections.get(id);

      const { connection } = remote || {};
      const description = connection?.remoteDescription || undefined;
      const queued = this.#candidateQueue.push(id, candidate, description);
      if (!remote || queued) return;

      await remote.addIceCandidate(candidate);

      return;
    }

    // dispose peer connection
    if (type === SIGNAL_LEAVE) {
      const remote = this.connections.get(id);
      if (!remote) return;

      remote.dispose();

      return;
    }
  }
}

/**
 * ICE server configuration for peer connections.
 * 
 * @group Peers
 */
export type IceServer = {
  /** ICE server URL(s) */
  urls: string | string[];
  /** Optional username for authentication */
  username?: string;
  /** Optional credential for authentication */
  credential?: string;
};

/**
 * ICE transport policy for peer connections.
 * 
 * @group Peers
 */
export type IceTransportPolicy = 'all' | 'relay';

/**
 * Peer connection state.
 * 
 * @group Peers
 */
export type PeerConnectionState = 'new' | 'connecting' | 'connected' | 'disconnected' | 'failed' | 'closed';

/**
 * Local stream publication options.
 * 
 * @group Streams and Channels
 */
export interface StreamOptions {
  /**
   * Stream label.
   * If omitted, the `default` label will be used.
   */
  label?: string;
  /**
   * Media stream to publish.
   */
  stream: MediaStream;
  /**
   * Whether the peer should manage the lifecycle of the stream's tracks.
   * If true, tracks will be stopped when the stream is unpublished or replaced.
   */
  managed?: boolean;
  /**
   * Preferred audio bitrate in bits per second.
   * For example, 16000 for 16 kbps.
   */
  audioBitrate?: number;
  /**
   * Preferred video bitrate in bits per second.
   * For example, 64000 for 64 kbps.
   */
  videoBitrate?: number;
}

/**
 * Options used to create negotiated RTCDataChannel instances.
 * 
 * @group Streams and Channels
 */
export interface ChannelOptions {
  /**
   * Channel label.
   * If omitted, the `default` label will be used.
   */
  label?: string;
  /**
   * Whether ordered delivery is required.
   */
  ordered?: boolean;
  /**
   * Maximum packet lifetime in milliseconds.
   */
  maxPacketLifeTime?: number;
  /**
   * Maximum retransmission attempts.
   */
  maxRetransmits?: number;
  /**
   * Optional subprotocol name.
   */
  protocol?: string;
}

/**
 * Configuration options for creating a {@link Peer} instance.
 * 
 * @group Peers
 */
export interface PeerOptions {
  /**
   * Unique peer identifier. A random UUID is generated when omitted.
   */
  id?: string;
  /**
   * Signaling driver instance for message exchange between peers.
   * If omitted, a default in-memory driver is used, which is suitable 
   * for testing purposes only.
   */
  driver?: Driver;
  /**
   * An array of objects, each describing one server which may be used 
   * by the ICE agent; these are typically STUN and/or TURN servers. 
   * If this isn't specified, the connection attempt will be made 
   * with no STUN or TURN server available, which limits the connection 
   * to local peers.
   * 
   * @example
   * ```javascript
   * iceServers: [{
   *   urls: 'stun:stun.l.google.com:19302'
   * }]
   * ```
   */
  iceServers?: IceServer[];
  /**
   * ICE policy used by created RTCPeerConnection instances. 
   * If set to 'relay', only relay candidates will be used, 
   * otherwise all candidates will be considered.
   */
  iceTransportPolicy?: IceTransportPolicy;
  /**
   * Connection timeout in seconds.
   * By default, it is set to 15 seconds. Use 0 to disable the timeout.
   */
  connectionTimeout?: number;
  /**
   * Compress signaling messages to reduce bandwidth usage by about 30%.
   * Enabled by default.
   */
  compress?: boolean;
  /**
   * Hash namespaces in signaling messages for privacy.
   * Disabled by default.
   */
  hash?: boolean;
  /**
   * Encrypt signaling messages with AES-GCM.
   * Disabled by default.
   */
  encrypt?: boolean;
  /**
   * Encryption key used when `encrypt` is enabled.
   */
  encryptionKey?: string;
}

/**
 * Options for joining a room by calling {@link Peer.join}.
 * 
 * @group Peers
 */
export interface PeerJoinOptions {
  /**
   * Room name to join.
   * If omitted, the peer will join a room with the `default` name.
   */
  room?: string;
  /**
   * Optional metadata to advertise to the remote peer.
   */
  metadata?: any;
  /**
   * Optional callback to accept or reject incoming peer connections.
   * 
   * @param options Options describing the incoming peer connection.
   * @param options.id Remote peer identifier.
   * @param options.metadata Remote peer metadata.
   * @returns A boolean or promise indicating whether the incoming connection should be accepted.
   */
  verify?: (options: { id: string; metadata?: any; }) => Promise<boolean> | boolean;
}

/**
 * Event emitted on peer connection state changes.
 * 
 * @group Peers
 */
export interface PeerConnectionEvent {
  /** Local peer identifier. */
  id: string;
  /** Name of the event. */
  name: 'connection:new' | 'connection:connecting' | 'connection:connected' | 'connection:disconnected' | 'connection:failed' | 'connection:closed';
  /** Remote peer object containing connection details. */
  remote: RemotePeer;
  /** New connection state. */
  state: PeerConnectionState;
}

/**
 * Emitted when a data channel is created or received from a remote peer,
 * when a data channel is opened or closed, when a message is received on a data channel,
 * or when an error occurs.
 * 
 * @group Peers
 */
export interface PeerChannelEvent {
  /** Local peer identifier. */
  id: string;
  /** Name of the event. */
  name: 'channel:new' | 'channel:open' | 'channel:close' | 'channel:message' | 'channel:error';
  /** Remote peer object containing connection details. */
  remote: RemotePeer;
  /** Data channel associated with the event. */
  channel: RTCDataChannel;
  /** Label of the data channel. */
  label: string;
  /** Received message data for message events. */
  data?: any;
  /** Error object containing details about the error for error events. */
  error?: Error;
}

/**
 * Emitted when a remote peer publishes or unpublishes a media stream.
 * 
 * @group Peers
 */
export interface PeerStreamEvent {
  /** Local peer identifier. */
  id: string;
  /** Name of the event. */
  name: 'stream:add' | 'stream:remove';
  /** Remote peer object containing connection details. */
  remote: RemotePeer;
  /** Media stream associated with the event. */
  stream: MediaStream;
  /** Label of the media stream. */
  label: string;
}

/**
 * Emitted when a remote peer adds or removes a media track to a published stream.
 * 
 * @group Peers
 */
export interface PeerTrackEvent {
  /** Local peer identifier. */
  id: string;
  /** Name of the event. */
  name: 'track:add' | 'track:remove';
  /** Remote peer object containing connection details. */
  remote: RemotePeer;
  /** Media stream associated with the event. */
  stream: MediaStream;
  /** Media track associated with the event. */
  track: MediaStreamTrack;
  /** Label of the media stream. */
  label: string;
}

/**
 * Event emitted when an error occurs in any background operations.
 * 
 * @group Peers
 */
export interface PeerErrorEvent {
  /** Local peer identifier. */
  id: string;
  /** Name of the event. */
  name: 'error';
  /** Error object containing details about the error. */
  error: PeerixError;
}

/**
 * Events emitted by {@link Peer} instances.
 * 
 * @group Peers
 */
export interface PeerEvents {
  /** Emitted when a remote peer connection state changes. */
  'connection': [PeerConnectionEvent];
  /** Emitted when a new remote peer connection is established. */
  'connection:new': [PeerConnectionEvent];
  /** Emitted when a remote peer connection is connecting. */
  'connection:connecting': [PeerConnectionEvent];
  /** Emitted when a remote peer connection is successfully connected. */
  'connection:connected': [PeerConnectionEvent];
  /** Emitted when a remote peer connection is disconnected. */
  'connection:disconnected': [PeerConnectionEvent];
  /** Emitted when a remote peer connection fails. */
  'connection:failed': [PeerConnectionEvent];
  /** Emitted when a remote peer connection is closed. */
  'connection:closed': [PeerConnectionEvent];
  /** Emitted when an error occurs in any background operations. */
  'error': [PeerErrorEvent];
  /** Emitted when stream events occur. */
  'stream': [PeerStreamEvent];
  /** Emitted when a remote peer publishes a media stream. */
  'stream:add': [PeerStreamEvent];
  /** Emitted when a remote peer unpublishes a media stream. */
  'stream:remove': [PeerStreamEvent];
  /** Emitted when track events occur. */
  'track': [PeerTrackEvent];
  /** Emitted when a remote peer adds a media track to a published stream. */
  'track:add': [PeerTrackEvent];
  /** Emitted when a remote peer removes a media track from a published stream. */
  'track:remove': [PeerTrackEvent];
  /** Emitted when channel events occur. */
  'channel': [PeerChannelEvent];
  /** Channel created or received from a remote peer. */
  'channel:new': [PeerChannelEvent];
  /** Emitted when a data channel is opened. */
  'channel:open': [PeerChannelEvent];
  /** Emitted when a data channel is closed. */
  'channel:close': [PeerChannelEvent];
  /** Emitted when a message is received on a data channel. */
  'channel:message': [PeerChannelEvent];
  /** Emitted when an error occurs with a remote peer connection or channel. */
  'channel:error': [PeerChannelEvent];
}
