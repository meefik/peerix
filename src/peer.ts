import type { SignalingDriver, SignalingNamespace } from './types/signaling.js';
import type { PeerOptions, JoinOptions, RemotePeer, StreamOptions, ChannelOptions, SendOptions, PeerEvents } from './types/peer.js';
import { MemoryDriver } from './drivers/memory.js';
import EventEmitter from './utils/emitter.js';
import Timeout from './utils/timeout.js';
import { UUIDv4, timeout } from './utils/helpers.js';
import log from './utils/logger.js';
import { PeerixError, ErrorCode } from './error.js';

// All peers without a driver will share the same in-memory signaling bus
const defaultDriver = new MemoryDriver();

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
 *   const { remote, state } = e;
 *   console.log(`Peer ${remote.id} connection state changed:`, state);
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
  /**
   * Unique identifier of the local peer.
   */
  readonly id: string;
  /**
   * Signaling transport used to exchange SDP and ICE messages.
   */
  readonly driver: SignalingDriver;

  /**
   * Active remote peers indexed by remote peer id.
   * 
   * @example
   * ```javascript
   * const peers = Array.from(peer.connections.values()).map((remote) => {
   *   const { id, state, metadata, streams, channels } = remote;
   *   return {
   *     id,
   *     state,
   *     metadata,
   *     streams: Array.from(streams.keys()).join(', '),
   *     channels: Array.from(channels.keys()).join(', '),
   *   };
   * });
   * console.table(peers);
   * ```
   */
  readonly connections: Map<string, RemotePeer>;
  /**
   * Published local streams indexed by application-level stream label.
   */
  readonly streams: Map<string, StreamOptions>;
  /**
   * Configured local data channels indexed by channel label.
   */
  readonly channels: Map<string, ChannelOptions>;
  /**
   * Attachable extensions.
   */
  readonly addons: Set<any>;

  /**
   * Indicates whether the peer is currently active (joined a room).
   */
  active: boolean;
  /**
   * Current room name. Empty until join() is called.
   */
  room: string;
  /**
   * Optional metadata announced to other peers in signaling messages.
   */
  metadata?: any;

  /**
   * STUN/TURN servers passed to every RTCPeerConnection instance.
   */
  #iceServers: { urls: string | string[]; username?: string; credential?: string; }[];
  /**
   * ICE transport policy for created peer connections.
   */
  #iceTransportPolicy: 'all' | 'relay';
  /**
   * Maximum time in seconds to wait for ICE connection establishment.
   */
  #connectionTimeout: number;

  /**
   * Internal event emitter used by on/once/off/emit helpers.
   */
  #emitter: EventEmitter<PeerEvents>;
  /**
   * ICE candidates received before remote description is applied.
   */
  #candidateQueues: Map<string, any[]>;
  /**
   * Set of peer ids for which a local offer is being created to handle glare scenarios.
   */
  #makingOffer: Set<string>;
  /**
   * Set of peer ids for which an answer is being processed to handle glare scenarios.
   */
  #pendingAnswer: Set<string>;
  /**
   * Media stream labels for incoming streams, indexed by remote peer id.
   */
  #streamLabels: Map<string, { [key: string]: string; }>;
  /**
   * Internal references to signaling driver event handlers for proper cleanup on leave.
   */
  #driverHandlers: Map<string, (...args: any[]) => void>;
  /**
   * Optional callback to accept or reject incoming peer connections.
   */
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
      driver = defaultDriver,
      iceServers = [],
      iceTransportPolicy = 'all',
      connectionTimeout = 15,
    } = options || {};

    this.driver = driver;
    this.active = false;
    this.id = id;
    this.room = '';
    this.connections = new Map();
    this.streams = new Map();
    this.channels = new Map();
    this.addons = new Set();
    this.#iceServers = iceServers;
    this.#iceTransportPolicy = iceTransportPolicy;
    this.#connectionTimeout = connectionTimeout;
    this.#emitter = new EventEmitter<PeerEvents>(this);
    this.#candidateQueues = new Map();
    this.#makingOffer = new Set();
    this.#pendingAnswer = new Set();
    this.#streamLabels = new Map();
    this.#driverHandlers = new Map();
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
  async join(options?: string | JoinOptions) {
    if (this.active) return;
    this.active = true;

    const { room = 'default', metadata, verify } =
      typeof options === 'object' ? options : { room: options };

    this.room = room;
    this.metadata = metadata;
    this.#verify = verify;

    log('peer:join', { id: this.id, room: this.room, metadata: this.metadata });

    await this.#registerDriverHandlers([this.room], [this.room, this.id]);

    await this.#emitDriverMessage([this.room], {
      type: 'join',
      id: this.id,
      metadata: this.metadata,
    });
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

    await this.#unregisterDriverHandlers([this.room], [this.room, this.id]);

    for (const remote of this.connections.values()) {
      remote.dispose();
    }
    this.connections.clear();

    this.#candidateQueues.clear();
    this.#makingOffer.clear();
    this.#pendingAnswer.clear();
    this.#streamLabels.clear();

    await this.#emitDriverMessage([this.room], {
      type: 'leave',
      id: this.id,
    });

    this.active = false;
  }

  /**
   * Publish new or update an existing media stream to all remote peers under 
   * a given label.
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
    const { label = 'default', stream, ...opts } = options || {};

    if (typeof label !== 'string' ||
      stream instanceof MediaStream === false || !stream.getTracks().length) {
      return;
    }

    const {
      stream: newStream = new MediaStream(),
      managed,
    } = this.streams.get(label) || {};

    const addedTracks = [];
    const removedTracks = [];

    for (const track of newStream.getTracks()) {
      if (!stream.getTracks().find(t => t.id === track.id)) {
        newStream.removeTrack(track);
        if (managed) track.stop();
        removedTracks.push(track);
      }
    }
    for (const track of stream.getTracks()) {
      if (!newStream.getTracks().find(t => t.id === track.id)) {
        newStream.addTrack(track);
        addedTracks.push(track);
      }
    }

    const newStreamOptions = { label, stream: newStream, ...opts };

    log('peer:publish', { id: this.id, ...newStreamOptions });

    this.streams.set(label, newStreamOptions);

    const bitrateOptions = { audio: opts.audioBitrate, video: opts.videoBitrate };
    for (const remote of this.connections.values()) {
      const { connection } = remote;
      const senders = connection.getSenders();

      for (const track of addedTracks) {
        const removedTrack = removedTracks.find(t => t.kind === track.kind);
        if (removedTrack) {
          const sender = senders.find(s => s.track?.id === removedTrack.id);
          if (sender) {
            await sender.replaceTrack(track);
            await this.#setTrackBitrate(remote, track, bitrateOptions);
            continue;
          }
        }
        connection.addTransceiver(track, { direction: 'sendonly', streams: [newStream] });
        await this.#setTrackBitrate(remote, track, bitrateOptions);
      }

      for (const track of removedTracks) {
        const sender = senders.find(s => s.track?.id === track.id);
        if (sender) {
          await sender.replaceTrack(null);
        }
      }

      for (const transceiver of connection.getTransceivers()) {
        if (transceiver.direction === 'sendonly' && !transceiver.sender.track) {
          transceiver.stop();
        }
      }
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
    const { label = 'default' } = options || {};

    if (typeof label !== 'string' || !this.streams.has(label)) return;

    const oldStreamOptions = this.streams.get(label);
    const { stream, managed } = oldStreamOptions || {};
    const tracks = stream?.getTracks() || [];

    log('peer:unpublish', { id: this.id, label, stream });

    this.streams.delete(label);

    if (managed) {
      for (const track of tracks) {
        track.stop();
      }
    }

    for (const remote of this.connections.values()) {
      const { connection } = remote;
      const senders = connection.getSenders();

      for (const track of tracks) {
        const sender = senders.find(s => s.track?.id === track.id);
        if (sender) {
          await sender.replaceTrack(null);
        }
      }

      for (const transceiver of connection.getTransceivers()) {
        if (transceiver.direction === 'sendonly' && !transceiver.sender.track) {
          transceiver.stop();
        }
      }
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
    const { label = 'default', ...opts } =
      typeof options === 'object' ? options : { label: options };

    if (typeof label !== 'string' || this.channels.has(label)) return;

    log('peer:open', { id: this.id, label, ...opts });

    this.channels.set(label, { label, ...opts });

    for (const remote of this.connections.values()) {
      const isPolite = this.id > remote.id;
      if (!isPolite) {
        this.#createDataChannel(remote, { label, ...opts });
      }
      else {

      }
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
    const { label = 'default' } =
      typeof options === 'object' ? options : { label: options };

    if (typeof label !== 'string' || !this.channels.has(label)) return;

    log('peer:close', { id: this.id, label });

    this.channels.delete(label);

    for (const remote of this.connections.values()) {
      const channel = remote.channels.get(label);
      channel?.close();
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
   * @param options Optional send options or channel label.
   */
  async send(message: any, options?: string | SendOptions) {
    if (!this.active) return;

    const { label, filter } =
      typeof options === 'object' ? options : { label: options };

    if (typeof label !== 'undefined' && typeof label !== 'string') {
      return;
    }

    log('peer:send', { id: this.id, label, message });

    const send = async (remote: RemotePeer, channel: RTCDataChannel) => {
      if (channel.readyState !== 'open') return;

      if (typeof filter === 'function') {
        const allowed = await filter({ remote, channel });
        if (!allowed) return;
      }

      log('peer:channel:send', { id: this.id, remote, channel });

      channel.send(message);
    };

    for (const remote of this.connections.values()) {
      if (typeof label === 'string') {
        const channel = remote.channels.get(label);
        if (!channel || channel.label !== label) continue;
        await send(remote, channel);
      }
      else {
        for (const channel of remote.channels.values()) {
          if (!channel) continue;
          await send(remote, channel);
        }
      }
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
      log(`peer:emit:${e}`, args);
    });
  }

  /**
   * Report an error with an optional error code.
   * 
   * @param err Error object or message.
   * @param code Optional error code.
   */
  #reportError(err: any, code?: ErrorCode) {
    const error = new PeerixError(err, code);
    this.emit('error', { id: this.id, error });
  }

  /**
   * Parse the ICE username fragment (ufrag) from an SDP string.
   * 
   * @param sdp SDP string to parse.
   * @returns The ICE username fragment if found, otherwise undefined.
   */
  #parseUfrag(sdp?: string) {
    return sdp && /a=ice-ufrag:([^\s]+)/m.exec(sdp)?.[1];
  }

  /**
   * Queue an ICE candidate for a remote peer if its remote description is not yet set or the username fragment does not match.
   * 
   * @param id Remote peer id.
   * @param candidate ICE candidate to queue.
   * @param remote Remote peer descriptor, if a connection already exists.
   */
  #queueIceCandidate(id: string, candidate: RTCIceCandidateInit, remote?: RemotePeer): boolean {
    const { sdp } = remote?.connection.remoteDescription || {};
    const ufrag = this.#parseUfrag(sdp);

    if (!remote || ufrag !== candidate.usernameFragment) {
      const queue = this.#candidateQueues.get(id);
      if (!queue) this.#candidateQueues.set(id, [candidate]);
      else queue.push(candidate);
      return true;
    }

    return false;
  }

  /**
   * Helper method to add queued ICE candidates for a remote peer once its remote description is set.
   * 
   * @param remote Remote peer descriptor.
   */
  async #addQueuedIceCandidates(remote: RemotePeer) {
    const { id, connection } = remote;

    if (this.#candidateQueues.has(id)) {
      const { sdp } = connection.remoteDescription || {};
      for (const candidate of this.#candidateQueues.get(id) || []) {
        const ufrag = this.#parseUfrag(sdp);
        if (!sdp || ufrag !== candidate.usernameFragment) {
          continue;
        }
        await this.#addIceCandidate(remote, candidate);
      }
      this.#candidateQueues.delete(id);
    }
  }

  /**
   * Helper method to add an ICE candidate to a remote peer's connection, with error handling.
   * 
   * @param remote Remote peer descriptor.
   * @param candidate ICE candidate to add.
   */
  async #addIceCandidate(remote: RemotePeer, candidate: RTCIceCandidateInit) {
    const { connection } = remote;

    log('peer:addicecandidate', { id: this.id, candidate, remote });

    try {
      await connection.addIceCandidate(candidate);
    } catch (err) {
      this.#reportError(err, 'PEER_ICECANDIDATE_ERROR');
    }
  }

  /**
   * Set up event listeners on a data channel and register it with the remote peer.
   * 
   * @param remote Remote peer descriptor.
   * @param channel Data channel instance to configure.
   */
  #setupDataChannel(remote: RemotePeer, channel: RTCDataChannel) {
    const { label = '' } = channel;
    const { channels } = remote;

    try {
      if (channels.has(label)) {
        channels.get(label)?.close();
      }
      channels.set(label, channel);

      channel.addEventListener('open', () => {
        this.emit('channel:open', { id: this.id, remote, channel, label });
      });
      channel.addEventListener('close', () => {
        channels.delete(label);
        this.emit('channel:close', { id: this.id, remote, channel, label });
      });
      channel.addEventListener('message', (e) => {
        this.emit('channel:message', { id: this.id, remote, channel, label, data: e.data });
      });
      channel.addEventListener('error', (e) => {
        this.emit('channel:error', { id: this.id, remote, channel, label, error: e.error });
      });

      this.emit('channel', { id: this.id, remote, channel, label });
    }
    catch (err) {
      this.#reportError(err, 'PEER_DATACHANNEL_ERROR');
    }
  }

  /**
   * Helper method to add a local media stream to a remote peer and emit appropriate events.
   * 
   * @param remote Remote peer descriptor.
   * @param stream Media stream that contains the track.
   * @param track Media track to add.
   */
  #setupMediaStream(remote: RemotePeer, stream: MediaStream, track: MediaStreamTrack) {
    const { id, streams } = remote;

    try {
      const labels = this.#streamLabels.get(id) || {};
      const label = labels[stream.id] || stream.id;

      const addTrack = () => {
        if (!streams.has(label)) {
          streams.set(label, stream);
          this.emit('stream:add', { id: this.id, remote, stream, label });
        }

        this.emit('track:add', { id: this.id, remote, stream, track, label });
      };

      const removeTrack = () => {
        const hasTrack = stream.getTracks().indexOf(track) !== -1;
        if (hasTrack) {
          stream.removeTrack(track);
          this.emit('track:remove', { id: this.id, remote, stream, track, label });
        }

        if (!stream.active || !stream.getTracks().length) {
          if (streams.has(label)) {
            streams.delete(label);
            this.emit('stream:remove', { id: this.id, remote, stream, label });
          }
        }
      };

      track.addEventListener('ended', removeTrack);

      addTrack();
    }
    catch (err) {
      this.#reportError(err, 'PEER_MEDIASTREAM_ERROR');
    }
  }

  /**
   * Helper method to create RTCDataChannel instances for a remote peer.
   * 
   * @param remote Remote peer descriptor.
   * @param channelOptions Channel options for creating data channels.
   */
  #createDataChannel(remote: RemotePeer, channelOptions: ChannelOptions) {
    const { connection, channels } = remote;

    try {
      const { label = '', ...opts } = channelOptions || {};
      if (channels.has(label)) return;

      log('peer:createdatachannel', { id: this.id, remote, label, ...opts });

      const channel = connection.createDataChannel(label, opts);
      this.#setupDataChannel(remote, channel);
    }
    catch (err) {
      this.#reportError(err, 'PEER_DATACHANNEL_ERROR');
    }
  }

  async #setTrackBitrate(remote: RemotePeer, track: MediaStreamTrack, bitrate: { [key: string]: number | undefined; }) {
    const { connection } = remote;
    const maxBitrate = (bitrate[track.kind] || 0) | 0;

    if (!maxBitrate) return;

    const senders = connection.getSenders();
    const sender = senders.find((sender: RTCRtpSender) => {
      return sender.track && sender.track.id === track.id;
    });

    if (sender) {
      log('peer:track:maxbitrate', { id: this.id, bitrate, track: sender.track, remote });

      const params = sender.getParameters() || {};
      if (!params.encodings) params.encodings = [];
      for (const enc of params.encodings) {
        if (enc) enc.maxBitrate = maxBitrate;
      }
      await sender.setParameters(params);
    }
  }

  /**
   * Helper method to add a media stream to a remote peer's connection.
   * 
   * @param remote Remote peer descriptor.
   * @param streamOptions Stream options for adding the media stream.
   */
  async #addMediaStream(remote: RemotePeer, streamOptions: StreamOptions) {
    try {
      const { stream, audioBitrate, videoBitrate } = streamOptions || {};
      const { connection } = remote;

      const tracks = stream.getTracks();
      const senders = connection.getSenders();

      for (const track of tracks) {
        const hasSender = senders.some(s => s.track && s.track.id === track.id);
        if (hasSender) continue;

        log('peer:addmediastream', { id: this.id, track, stream, remote });

        connection.addTransceiver(track, { direction: 'sendonly', streams: [stream] });
        await this.#setTrackBitrate(remote, track, { audio: audioBitrate, video: videoBitrate });
      }
    }
    catch (err) {
      this.#reportError(err, 'PEER_MEDIASTREAM_ERROR');
    }
  }

  /**
   * Create a new peer connection.
   * 
   * @param id The ID of the remote peer.
   * @param metadata Metadata associated with the remote peer.
   * @returns The created RemotePeer object.
   */
  #createRemote(id: string, metadata: any): RemotePeer {
    log('peer:createremote', { id: this.id, remote: { id, metadata } });

    // close existing connection
    if (this.connections.has(id)) {
      const remote = this.connections.get(id);
      if (remote) remote.dispose();
    }

    const connection = new RTCPeerConnection({
      iceServers: this.#iceServers,
      iceTransportPolicy: this.#iceTransportPolicy,
    });
    const streams = new Map();
    const channels = new Map();
    const connectionTimeout = new Timeout(() => {
      dispose();
      this.#reportError('Connection timeout', 'PEER_CONNECTION_FAILED');
    }, this.#connectionTimeout * 1000);

    const dispose = () => {
      if (!this.connections.has(id)) return;
      this.connections.delete(id);
      connectionTimeout.clear();

      channels.forEach(channel => channel?.close());
      connection?.close();
      remote.state = 'closed';

      this.#candidateQueues.delete(id);
      this.#makingOffer.delete(id);
      this.#pendingAnswer.delete(id);
      this.#streamLabels.delete(id);

      this.emit('connection', { id: this.id, remote, state: 'closed' });
    };

    const remote: RemotePeer = {
      id,
      metadata,
      connection,
      state: 'new',
      streams,
      channels,
      dispose,
    };

    connection.addEventListener('iceconnectionstatechange', (e) => {
      const { iceConnectionState } = e.target as RTCPeerConnection;

      if (iceConnectionState === 'new') {
        const state = 'new';
        remote.state = state;
        this.emit('connection', { id: this.id, remote, state });
      }
      else if (iceConnectionState === 'checking') {
        const state = 'connecting';
        remote.state = state;
        this.emit('connection', { id: this.id, remote, state });
      }
      else if (iceConnectionState === 'connected') {
        connectionTimeout.clear();
        const state = 'connected';
        remote.state = state;
        this.emit('connection', { id: this.id, remote, state });
      }
      else if (iceConnectionState === 'disconnected') {
        const state = 'disconnected';
        remote.state = state;
        this.emit('connection', { id: this.id, remote, state });
        connectionTimeout.start();
      }
      else if (iceConnectionState === 'failed') {
        const state = 'failed';
        remote.state = state;
        this.emit('connection', { id: this.id, remote, state });
        dispose();
      }
      else if (iceConnectionState === 'closed') {
        remote.state = 'closed';
        dispose();
      }
    });

    connection.addEventListener('icecandidate', async (e) => {
      const { candidate } = e;
      if (!candidate) return;

      log('peer:icecandidate', { id: this.id, candidate, remote });

      await this.#emitDriverMessage([this.room, id], {
        type: 'ice',
        id: this.id,
        candidate: typeof candidate.toJSON === 'function'
          ? candidate.toJSON()
          : candidate,
      });
    });

    connection.addEventListener('negotiationneeded', async () => {
      if (connection.signalingState !== 'stable') return;

      try {
        const offer = await this.#createOffer(remote);
        if (offer) {
          await this.#emitDriverMessage([this.room, id], {
            type: 'offer',
            id: this.id,
            metadata: this.metadata,
            description: offer,
            channels: Array.from(this.channels.keys()),
            streams: Array.from(this.streams.keys())
              .reduce((acc, label) => {
                const { stream } = this.streams.get(label) || {};
                if (stream) acc[stream.id] = label;
                return acc;
              }, {} as { [key: string]: string; }),
          });
        }
      }
      catch (err) {
        this.#reportError(err, 'PEER_NEGOTIATION_FAILED');
      }
    });

    connection.addEventListener('datachannel', (e) => {
      const { channel } = e;
      this.#setupDataChannel(remote, channel);
    });

    connection.addEventListener('track', (e) => {
      const { track, streams: [stream] } = e;
      this.#setupMediaStream(remote, stream, track);
    });

    this.connections.set(id, remote);

    this.emit('connection', { id: this.id, remote, state: 'new' });

    // management channel to exchange control messages and handle glare scenarios
    let pingTimer: ReturnType<typeof setInterval>;
    const pingInterval = 5000;
    const manager = connection.createDataChannel('', { negotiated: true, id: 0 });
    const sendMessage = (message: any) => {
      if (manager.readyState === 'open') {
        manager.send(JSON.stringify(message));
      }
    };
    manager.addEventListener('open', async () => {
      pingTimer = setInterval(() => {
        sendMessage(['ping', Date.now().toString(36)]);
      }, pingInterval);

      // negotiation
      const isPolite = this.id > remote.id;
      if (isPolite) {
        sendMessage(['open', Array.from(this.channels.keys())]);
      }
      else {
        for (const channelOptions of this.channels.values()) {
          this.#createDataChannel(remote, channelOptions);
        }
      }

      for (const streamOptions of this.streams.values()) {
        await this.#addMediaStream(remote, streamOptions);
      }
    });
    manager.addEventListener('close', () => {
      clearInterval(pingTimer);
      dispose();
    });
    manager.addEventListener('message', (e) => {
      const [type, payload] = JSON.parse(e.data);
      if (type === 'ping') {
        connectionTimeout.start(pingInterval * 2);
        return;
      }
      if (type === 'open') {
        const isPolite = this.id > remote.id;
        if (isPolite) {
          const remoteChannels = new Set(payload || []);
          for (const channelOptions of this.channels.values()) {
            if (!remoteChannels.has(channelOptions.label)) {
              this.#createDataChannel(remote, channelOptions);
            }
          }
        }
        return;
      }
    });

    return remote;
  }

  /**
   * Create an SDP offer and set it as the local description for a remote peer connection.
   *
   * @param remote Remote peer descriptor.
   * @returns The created RTCSessionDescriptionInit offer.
   */
  async #createOffer(remote: RemotePeer) {
    const { id, connection } = remote;

    log('peer:connection:createoffer', { id: this.id, remote });

    try {
      this.#makingOffer.add(id);
      const offer = await connection.createOffer();
      await connection.setLocalDescription(offer);

      return offer;
    }
    finally {
      this.#makingOffer.delete(id);
    }
  }

  /**
   * Apply a remote SDP description to a peer connection.
   *
   * When the description is an answer, tracks pending-answer state so that
   * glare detection logic in the message handler stays accurate.
   *
   * @param remote Remote peer descriptor.
   * @param description SDP offer or answer received from the remote peer.
   */
  async #setRemoteDescription(remote: RemotePeer, description: RTCSessionDescriptionInit) {
    const { id, connection } = remote;

    log('peer:setremotedescription', { id: this.id, description, remote });

    try {
      if (description.type === 'answer') {
        this.#pendingAnswer.add(id);
      }
      await connection.setRemoteDescription(description);
    }
    finally {
      this.#pendingAnswer.delete(id);
    }
  }

  /**
   * Create an SDP answer and set it as the local description for a remote peer connection.
   *
   * @param remote Remote peer descriptor.
   * @returns The created RTCSessionDescriptionInit answer.
   */
  async #createAnswer(remote: RemotePeer) {
    const { connection } = remote;

    log('peer:createanswer', { id: this.id, remote });

    const answer = await connection.createAnswer();
    await connection.setLocalDescription(answer);

    return answer;
  }

  /**
   * Subscribe to all specified namespaces on the signaling driver.
   *
   * @param namespaces One or more namespace arrays to subscribe to.
   */
  async #registerDriverHandlers(...namespaces: string[][]) {
    log('driver:register', { id: this.id, namespaces });

    const entries: Array<[string, (...args: any[]) => void, boolean]> = [
      ['message', this.#driverMessageHandler, true],
      ['active', this.#driverActiveHandler, false],
      ['inactive', this.#driverInactiveHandler, false],
      ['error', this.#driverErrorHandler, false],
    ];

    try {
      for (const [key, method, useNamespaces] of entries) {
        if (!this.#driverHandlers.has(key)) {
          const handler = method.bind(this);
          const topics = useNamespaces ? namespaces.map(ns => [key, ...ns]) : [[key]];
          for (const topic of topics) {
            await this.driver.on(topic as SignalingNamespace, handler);
          }
          this.#driverHandlers.set(key, handler);
        }
      }
    }
    catch (err) {
      this.#reportError(err, 'PEER_SIGNALING_FAILED');
    }
  }

  /**
   * Unsubscribe from all specified namespaces on the signaling driver.
   *
   * @param namespaces One or more namespace arrays to unsubscribe from.
   */
  async #unregisterDriverHandlers(...namespaces: string[][]) {
    log('driver:unregister', { id: this.id, namespaces });

    const keys: Array<[string, boolean]> = [
      ['message', true],
      ['active', false],
      ['inactive', false],
      ['error', false],
    ];

    try {
      for (const [key, useNamespaces] of keys) {
        const handler = this.#driverHandlers.get(key);
        if (handler) {
          const topics = useNamespaces ? namespaces.map(ns => [key, ...ns]) : [[key]];
          for (const topic of topics) {
            await this.driver.off(topic as SignalingNamespace, handler);
          }
          this.#driverHandlers.delete(key);
        }
      }
    }
    catch (err) {
      this.#reportError(err, 'PEER_SIGNALING_FAILED');
    }
  }

  /**
   * Emit a message to the given namespace via the configured driver.
   *
   * Does nothing when the peer is not active.
   *
   * @param namespace Target namespace for the message.
   * @param message Message payload to send.
   */
  async #emitDriverMessage(namespace: string[], message: any) {
    if (!this.active) return;

    log('driver:emit', { id: this.id, namespace, message });

    try {
      const driverActive = this.driver.active;
      if (driverActive || driverActive === undefined) {
        await this.driver.emit(['message', ...namespace], message);
      }
    }
    catch (err) {
      this.#reportError(err, 'PEER_SIGNALING_FAILED');
    }
  }

  /**
   * Handle the driver becoming active.
   */
  async #driverActiveHandler() {
    if (!this.active) return;

    log('driver:active', { id: this.id });

    // add random jitter to avoid multiple peers reconnecting at the same time
    const jitter = 100 + Math.floor(Math.random() * 900);
    setTimeout(() => {
      if (this.active) this.join();
    }, jitter);
  }

  /**
   * Handle the driver becoming inactive.
   */
  async #driverInactiveHandler() {
    if (!this.active) return;

    log('driver:inactive', { id: this.id });
  }

  /**
   * Handle an error emitted by the driver.
   *
   * @param error Error object or message emitted by the driver.
   */
  async #driverErrorHandler(error: any) {
    if (!this.active) return;

    log('driver:error', { id: this.id, error });

    this.#reportError(error, 'PEER_SIGNALING_FAILED');
  }

  /**
   * Handle an incoming message dispatched by the driver.
   *
   * Processes signaling message to establish, negotiate, and tear down
   * peer connections.
   *
   * @param message Incoming message from the signaling driver.
   */
  async #driverMessageHandler(message: any) {
    if (!this.active || !message) return;

    const { type, id, metadata } = message;
    if (!type || !id || this.id === id) return;

    log('driver:message', { id: this.id, message });

    // verify the incoming connection and reject if verification fails
    if (this.#verify) {
      try {
        const verified = await this.#verify({ id, metadata });
        if (!verified) return;
      }
      catch (err) {
        this.#reportError(err, 'PEER_CONNECTION_FAILED');
        return;
      }
    }

    // handle incoming connection
    if (type === 'join') {
      let remote = this.connections.get(id);
      try {
        if (remote) remote.dispose();
        remote = this.#createRemote(id, metadata);
      }
      catch (err) {
        this.#reportError(err, 'PEER_CONNECTION_FAILED');
        return;
      }

      return;
    }

    // set remote description for offer and create answer
    if (type === 'offer') {
      const { description, channels, streams } = message;

      let remote = this.connections.get(id);
      if (!remote) {
        try {
          remote = this.#createRemote(id, metadata);
        }
        catch (err) {
          this.#reportError(err, 'PEER_CONNECTION_FAILED');
          return;
        }
      }

      const { connection } = remote;

      const readyForOffer = !this.#makingOffer.has(id) &&
        (connection.signalingState === 'stable' || this.#pendingAnswer.has(id));
      const offerCollision = description.type === 'offer' && !readyForOffer;

      const isPolite = this.id > id;
      if (!isPolite && offerCollision) return;

      if (streams) {
        this.#streamLabels.set(id, streams);
      }

      // wait to avoid interrupting previous operations 
      while (this.#makingOffer.has(id) || this.#pendingAnswer.has(id)) {
        await timeout(0);
      }

      try {
        await this.#setRemoteDescription(remote, description);
        await this.#addQueuedIceCandidates(remote);

        const answer = await this.#createAnswer(remote);
        if (answer) {
          await this.#emitDriverMessage([this.room, id], {
            type: 'answer',
            id: this.id,
            metadata: this.metadata,
            description: answer,
          });
        }
      }
      catch (err) {
        this.#reportError(err, 'PEER_NEGOTIATION_FAILED');
      }

      return;
    }

    // set remote description for answer
    if (type === 'answer') {
      const remote = this.connections.get(id);
      if (!remote) return;

      try {
        await this.#setRemoteDescription(remote, message.description);
        await this.#addQueuedIceCandidates(remote);
      }
      catch (err) {
        this.#reportError(err, 'PEER_NEGOTIATION_FAILED');
      }
    }

    // add ice candidate
    if (type === 'ice') {
      const { candidate } = message;
      const remote = this.connections.get(id);

      const queued = this.#queueIceCandidate(id, candidate, remote);
      if (!remote || queued) return;

      this.#addIceCandidate(remote, candidate);

      return;
    }

    // dispose peer connection
    if (type === 'leave') {
      const remote = this.connections.get(id);
      if (!remote) return;

      remote.dispose();

      return;
    }
  }
}
