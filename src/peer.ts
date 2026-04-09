import type { SignalingDriver } from './types/signaling.js';
import type { PeerOptions, JoinOptions, RemotePeer, StreamOptions, ChannelOptions, SendOptions, PeerEvents } from './types/peer.js';
import { MemoryDriver } from './drivers/memory.js';
import EventEmitter from './utils/emitter.js';
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
   * STUN/TURN servers passed to every RTCPeerConnection instance.
   */
  readonly iceServers: { urls: string | string[]; username?: string; credential?: string }[];
  /**
   * ICE transport policy for created peer connections.
   */
  readonly iceTransportPolicy: 'all' | 'relay';
  /**
   * Maximum time in seconds to wait for ICE connection establishment.
   */
  readonly connectionTimeout: number;

  /**
   * Active remote peers indexed by remote peer id.
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
   * Current room name. Empty until join() is called.
   */
  room: string;
  /**
   * Optional metadata announced to other peers in signaling messages.
   */
  metadata?: any;
  /**
   * Indicates whether the peer is currently active (joined a room).
   */
  active: boolean;

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
  #streamLabels: Map<string, { [key: string]: string }>;
  /**
   * Active signaling handler registered on the signaling driver.
   */
  #signaling?: (e: any) => void;
  /**
   * Optional callback to accept or reject incoming peer connections.
   */
  #verify?: (options: { id: string; metadata?: any }) => Promise<boolean> | boolean;

  /**
   * Creates an instance of Peer.
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
    this.id = id;
    this.room = '';
    this.active = false;
    this.iceServers = iceServers;
    this.iceTransportPolicy = iceTransportPolicy;
    this.connectionTimeout = connectionTimeout;
    this.connections = new Map();
    this.streams = new Map();
    this.channels = new Map();
    this.addons = new Set();
    this.#emitter = new EventEmitter<PeerEvents>(this);
    this.#candidateQueues = new Map();
    this.#streamLabels = new Map();
    this.#makingOffer = new Set();
    this.#pendingAnswer = new Set();
  }

  /**
   * Join a room and start listening for incoming connections.
   *
   * @param options Room name or join options.
   */
  async join(options?: string | JoinOptions) {
    if (this.active) return;
    this.active = true;

    const { room = 'default', metadata, verify } = typeof options === 'object'
      ? options : { room: options };

    this.room = room;
    this.metadata = metadata;
    this.#verify = verify;

    log('peer:join', { id: this.id, room: this.room, metadata: this.metadata });

    try {
      this.#signaling = this.#signalHandler.bind(this);
      await this.driver.on([this.room], this.#signaling);
      await this.driver.on([this.room, this.id], this.#signaling);

      await this.driver.emit([this.room], {
        type: 'invoke',
        id: this.id,
        metadata: this.metadata,
      });
    }
    catch (err) {
      this.#reportError(err, 'PEER_SIGNALING_FAILED');
    }
  }

  /**
    * Leave the current room and close all active remote connections.
   */
  async leave() {
    if (!this.active) return;

    log('peer:leave', { id: this.id, room: this.room, metadata: this.metadata });

    if (this.#signaling) {
      try {
        await this.driver.off([this.room], this.#signaling);
        await this.driver.off([this.room, this.id], this.#signaling);
        this.#signaling = undefined;
      }
      catch (err) {
        this.#reportError(err, 'PEER_SIGNALING_FAILED');
      }
    }

    for (const remote of this.connections.values()) {
      remote.dispose();
    }
    this.connections.clear();

    this.#candidateQueues.clear();
    this.#makingOffer.clear();
    this.#pendingAnswer.clear();
    this.#streamLabels.clear();

    this.active = false;
  }

  /**
   * Publish or update a local media stream.
   *
   * When already active, this updates senders on every current connection and
   * triggers negotiation where applicable.
   *
   * @param options Stream descriptor or MediaStream instance.
   */
  async publish(options: StreamOptions | MediaStream) {
    if (options instanceof MediaStream) {
      options = { label: options.id, stream: options };
    }
    const { label = 'default', stream, ...opts } = options;

    const {
      stream: newStream = new MediaStream(),
      managed,
    } = this.streams.get(label) || {};

    for (const track of newStream.getTracks()) {
      if (!stream.getTracks().find(t => t.id === track.id)) {
        newStream.removeTrack(track);
        if (managed) track.stop();
      }
    }
    for (const track of stream.getTracks()) {
      if (!newStream.getTracks().find(t => t.id === track.id)) {
        newStream.addTrack(track);
      }
    }

    log('peer:publish', { id: this.id, label, stream: newStream, ...opts });

    this.streams.set(label, { label, stream: newStream, ...opts });

    if (this.active) {
      try {
        await this.driver.emit([this.room], {
          type: 'invoke',
          id: this.id,
          metadata: this.metadata,
        });
      }
      catch (err) {
        this.#reportError(err, 'PEER_SIGNALING_FAILED');
      }
    }
  }

  /**
   * Stop publishing a previously published local stream.
   *
   * @param options Stream label, MediaStream instance, or object containing `label`.
   */
  async unpublish(options: string | MediaStream | { label: string }) {
    if (options instanceof MediaStream) {
      options = { label: options.id };
    }
    const { label = 'default' } = typeof options === 'object'
      ? options : { label: options };

    const { stream, managed } = this.streams.get(label) || {};
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
        const sender = senders.find((sender: RTCRtpSender) => {
          return sender.track && sender.track.id === track.id;
        });
        if (!sender) continue;

        log('peer:connection:removetrack', { id: this.id, track, stream, remote });

        connection.removeTrack(sender);
      }
    }
  }

  /**
   * Register or create a negotiated data channel with all remote peers.
   *
   * @param options Channel options or channel label.
   */
  async open(options: string | ChannelOptions) {
    const { label = 'default', ...opts } = typeof options === 'object'
      ? options : { label: options };

    log('peer:open', { id: this.id, label, ...opts });

    this.channels.set(label, { label, ...opts });

    if (this.active) {
      try {
        await this.driver.emit([this.room], {
          type: 'invoke',
          id: this.id,
          metadata: this.metadata,
        });
      }
      catch (err) {
        this.#reportError(err, 'PEER_SIGNALING_FAILED');
      }
    }
  }

  /**
   * Close and unregister a negotiated data channel by id.
   *
   * @param options Channel label or object containing `label`.
   */
  async close(options: string | { label: string }) {
    const { label = 'default' } = typeof options === 'object'
      ? options : { label: options };

    log('peer:close', { id: this.id, label });

    this.channels.delete(label);

    for (const remote of this.connections.values()) {
      const channel = remote.channels.get(label);
      if (!channel) continue;
      remote.channels.delete(label);
      channel.close();
    }
  }

  /**
   * Send a message through data channels.
   *
   * If `options` is omitted, the message is sent to all open channels for every
   * connected remote peer. If `options` is a string, it is treated as channel label.
   *
   * @param message Message payload to send. This may be a string, a Blob, an ArrayBuffer, a TypedArray or a DataView object.
   * @param options Optional send options or channel label.
   */
  async send(message: any, options?: string | SendOptions) {
    if (!this.active) return;

    const { label, verify } = typeof options === 'object'
      ? options : { label: options };

    log('peer:send', { id: this.id, label, message });

    for (const remote of this.connections.values()) {
      if (typeof label === 'string') {
        const channel = remote.channels.get(label);
        if (!channel || channel.readyState !== 'open') continue;
        if (channel.label !== label) continue;
        if (typeof verify === 'function') {
          const allowed = verify({ id: remote.id, metadata: remote.metadata, label: channel.label });
          if (!allowed) continue;
        }

        log('peer:channel:send', { id: this.id, remote, channel });

        channel.send(message);
      }
      else {
        for (const channel of remote.channels.values()) {
          if (!channel || channel.readyState !== 'open') continue;
          if (typeof verify === 'function') {
            const allowed = verify({ id: remote.id, metadata: remote.metadata, label: channel.label });
            if (!allowed) continue;
          }

          log('peer:channel:send', { id: this.id, remote, channel });

          channel.send(message);
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
   * @param remote Remote peer descriptor.
   * @param candidate ICE candidate to queue.
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

    log('peer:connection:addicecandidate', { id: this.id, candidate, remote });

    try {
      await connection.addIceCandidate(candidate);
    } catch (err) {
      this.#reportError(err, 'PEER_ICECANDIDATE_ERROR');
    }
  }

  /**
   * Helper method to create a data channel.
   * 
   * @param remote Remote peer descriptor.
   * @param channel Data channel instance.
   */
  #setupDataChannel(remote: RemotePeer, channel: RTCDataChannel) {
    const { label = '' } = channel;
    const { channels, streams } = remote;

    try {
      if (channels.has(label)) {
        const previousChannel = channels.get(label);
        previousChannel?.close();
      }
      channels.set(label, channel);

      channel.addEventListener('open', () => {
        this.emit('channel:open', { id: this.id, remote, channel, label });
      });
      channel.addEventListener('close', () => {
        channels.delete(label);
        this.emit('channel:close', { id: this.id, remote, channel, label });

        // close connection if there are no more active streams or channels
        if (!channels.size && !streams.size) {
          remote.dispose();
        }
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
    const { id, channels, streams } = remote;

    try {
      const labels = this.#streamLabels.get(id) || {};
      const label = labels[stream.id] || stream.id;

      if (!streams.has(label)) {
        streams.set(label, stream);
        stream.addEventListener('removetrack', (e) => {
          const { track } = e;

          this.emit('track:remove', { id: this.id, remote, stream, track, label });

          if (!stream.getTracks().length) {
            streams.delete(label);
            this.emit('stream:remove', { id: this.id, remote, stream, label });
          }

          // close connection if there are no more active streams or channels
          if (!channels.size && !streams.size) {
            remote.dispose();
          }
        });

        this.emit('stream:add', { id: this.id, remote, stream, label });
      }

      this.emit('track:add', { id: this.id, remote, stream, track, label });
    }
    catch (err) {
      this.#reportError(err, 'PEER_MEDIASTREAM_ERROR');
    }
  }

  /**
   * Helper method to create RTCDataChannel instances for a remote peer.
   * 
   * @param remote Remote peer descriptor.
   * @param channels Map of channel options keyed by channel label.
   */
  #createChannels(remote: RemotePeer, channels: Map<string, ChannelOptions>) {
    const { connection } = remote;

    for (const channelOptions of channels.values()) {
      try {
        const { label = '', verify, ...opts } = channelOptions || {};

        log('peer:connection:createdatachannel', { id: this.id, remote, label, ...opts });

        const channel = connection.createDataChannel(label, opts);
        this.#setupDataChannel(remote, channel);
      }
      catch (err) {
        this.#reportError(err, 'PEER_DATACHANNEL_ERROR');
      }
    }
  }

  /**
   * Helper method to add or replace media tracks for a remote peer.
   * 
   * @param remote Remote peer descriptor.
   * @param streams Map of stream options keyed by stream ID.
   */
  #publishStreams(remote: RemotePeer, streams: Map<string, StreamOptions>) {
    const { connection } = remote;

    const setBitrate = (id: string, bitrate: number) => {
      if (!bitrate) return;
      const senders = connection.getSenders();
      const sender = senders.find((sender: RTCRtpSender) => {
        return sender.track && sender.track.id === id;
      });
      if (sender) {
        log('peer:connection:maxbitrate', { id: this.id, bitrate, track: sender.track, remote });

        const params = sender.getParameters() || {};
        if (!params.encodings) params.encodings = [];
        for (let i = 0; i < params.encodings.length; i++) {
          const enc = params.encodings[i];
          if (enc) enc.maxBitrate = bitrate;
        }
        sender.setParameters(params);
      }
    };

    for (const options of streams.values()) {
      try {
        const { stream, audioBitrate, videoBitrate } = options;

        const bitrate: { [key: string]: number } = {
          audio: (audioBitrate || 0) | 0,
          video: (videoBitrate || 0) | 0,
        };

        const tracks = stream.getTracks();
        const senders = connection.getSenders();

        // Replace existing senders or add new ones for each track in the stream
        for (const track of tracks) {
          const senderExists = senders.some((sender) => sender.track && sender.track.id === track.id);
          if (!senderExists) {
            const sender = senders.find((sender) => sender.track
              && sender.track.readyState === 'ended' && sender.track.kind === track.kind);
            if (sender) {
              log('peer:connection:replacetrack', { id: this.id, track, stream, remote });

              sender.replaceTrack(track);
            }
            else {
              log('peer:connection:addtrack', { id: this.id, track, stream, remote });

              connection.addTrack(track, stream);
            }
            // set bitrate for new or existing sender
            setBitrate(track.id, bitrate[track.kind]);
          }
        }

        // Remove senders for tracks that no longer exist in the stream
        for (const sender of senders) {
          if (!sender.track) continue;
          const trackExists = tracks.some(track => sender.track && track.id === sender.track.id);
          if (!trackExists) {
            log('peer:connection:removetrack', { id: this.id, track: sender.track, stream, remote });

            connection.removeTrack(sender);
          }
        }
      }
      catch (err) {
        this.#reportError(err, 'PEER_MEDIASTREAM_ERROR');
      }
    }
  }

  #getFilteredChannels(id: string, metadata: any, labelsToIgnore?: string[]) {
    const filteredChannels = new Map<string, ChannelOptions>();

    // polite-ignoring channels that are already open for the remote peer
    if (!labelsToIgnore) return filteredChannels;

    for (const channelOptions of this.channels.values()) {
      try {
        const { label = '', verify } = channelOptions;

        if (labelsToIgnore.indexOf(label) !== -1) continue;

        if (typeof verify === 'function') {
          const allowed = verify({ id, metadata, label });
          if (!allowed) continue;
        }

        const remote = this.connections.get(id);
        if (remote?.channels.has(label)) continue;

        filteredChannels.set(label, channelOptions);
      }
      catch (err) {
        this.#reportError(err, 'PEER_DATACHANNEL_ERROR');
      }
    }

    return filteredChannels;
  }

  #getFilteredStreams(id: string, metadata: any) {
    const filteredStreams = new Map<string, StreamOptions>();

    for (const streamOptions of this.streams.values()) {
      try {
        const { label = '', verify } = streamOptions;

        if (typeof verify === 'function') {
          const allowed = verify({ id, metadata, label });
          if (!allowed) continue;
        }

        const remote = this.connections.get(id);
        if (remote?.streams.has(label)) continue;

        filteredStreams.set(label, streamOptions);
      }
      catch (err) {
        this.#reportError(err, 'PEER_MEDIASTREAM_ERROR');
      }
    }

    return filteredStreams;
  }

  /**
   * Create a new peer connection.
   * 
   * @param id The ID of the remote peer.
   * @param metadata Metadata associated with the remote peer.
   * @returns The created RemotePeer object.
   */
  #createPeerConnection(id: string, metadata: any): RemotePeer {
    log('peer:connection:create', { id: this.id, remote: { id, metadata } });

    const connection = new RTCPeerConnection({
      iceServers: this.iceServers,
      iceTransportPolicy: this.iceTransportPolicy,
    });
    const streams = new Map();
    const channels = new Map();

    const setConnectionTimeout = () => {
      const timer = this.connectionTimeout > 0 ? setTimeout(
        () => {
          dispose({ silent: true });
          this.#reportError('Connection timeout', 'PEER_CONNECTION_FAILED');
        },
        this.connectionTimeout * 1000,
      ) : undefined;

      return () => clearTimeout(timer);
    };

    const dispose = async ({ silent = false } = {}) => {
      if (!this.connections.has(id)) return;
      this.connections.delete(id);
      stopConnectionTimeout();

      channels.forEach(channel => channel?.close());
      connection?.close();
      remote.state = 'closed';

      this.#candidateQueues.delete(id);
      this.#makingOffer.delete(id);
      this.#pendingAnswer.delete(id);
      this.#streamLabels.delete(id);

      this.emit('connection', { id: this.id, remote, state: 'closed' });

      if (!silent) {
        try {
          await this.driver.emit([this.room, id], {
            type: 'dispose',
            id: this.id,
          });
        } catch (err) {
          this.#reportError(err, 'PEER_SIGNALING_FAILED');
        }
      }
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

    let stopConnectionTimeout = setConnectionTimeout();

    connection.addEventListener('iceconnectionstatechange', (e) => {
      const { iceConnectionState } = e.target as RTCPeerConnection;

      if (iceConnectionState === 'checking') {
        const state = 'connecting';
        remote.state = state;
        this.emit('connection', { id: this.id, remote, state });
      }
      else if (iceConnectionState === 'connected') {
        stopConnectionTimeout();
        const state = 'connected';
        remote.state = state;
        this.emit('connection', { id: this.id, remote, state });
      }
      else if (iceConnectionState === 'disconnected') {
        const state = 'disconnected';
        remote.state = state;
        this.emit('connection', { id: this.id, remote, state });
        stopConnectionTimeout = setConnectionTimeout();
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

      log('peer:connection:icecandidate', { id: this.id, candidate, remote });

      try {
        await this.driver.emit([this.room, id], {
          type: 'ice',
          id: this.id,
          candidate: typeof candidate.toJSON === 'function'
            ? candidate.toJSON()
            : candidate,
        });
      }
      catch (err) {
        this.#reportError(err, 'PEER_SIGNALING_FAILED');
      }
    });

    connection.addEventListener('negotiationneeded', async () => {
      if (connection.signalingState !== 'stable') return;

      try {
        const offer = await this.#createOffer(remote);
        if (offer) {
          try {
            await this.driver.emit([this.room, id], {
              type: 'sdp',
              id: this.id,
              metadata: this.metadata,
              description: offer,
              labels: Array.from(this.streams.keys())
                .reduce((acc, label) => {
                  const { stream } = this.streams.get(label) || {};
                  if (stream) acc[stream.id] = label;
                  return acc;
                }, {} as { [key: string]: string }),
            });
          }
          catch (err) {
            this.#reportError(err, 'PEER_SIGNALING_FAILED');
          }
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

    this.emit('connection', { id: this.id, remote, state: 'new' });

    return remote;
  }

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

  async #setRemoteDescription(remote: RemotePeer, description: RTCSessionDescriptionInit) {
    const { id, connection } = remote;

    log('peer:connection:setremotedescription', { id: this.id, description, remote });

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

  async #createAnswer(remote: RemotePeer) {
    const { connection } = remote;

    log('peer:connection:createanswer', { id: this.id, remote });

    const answer = await connection.createAnswer();
    await connection.setLocalDescription(answer);

    return answer;
  }

  async #signalHandler(signal: any) {
    const { type, id } = signal;
    if (!type || !id || this.id === id) return;

    log('peer:signal', { id: this.id, signal });

    if (type === 'invoke') {
      const { metadata, channels, streams } = signal;
      const isPolite = this.id > id;

      // verify the incoming connection and reject if verification fails
      if (this.#verify) {
        try {
          const verified = this.#verify({ id, metadata });
          if (!verified) return;
        }
        catch (err) {
          this.#reportError(err, 'PEER_CONNECTION_FAILED');
          return;
        }
      }

      // get filtered list of channels and streams
      const filteredChannels = this.#getFilteredChannels(id, metadata, isPolite ? channels : []);
      const filteredStreams = this.#getFilteredStreams(id, metadata);

      // create peer connection, publish streams and create channels
      if (filteredChannels?.size || filteredStreams?.size) {
        let remote = this.connections.get(id);
        if (!remote) {
          try {
            remote = this.#createPeerConnection(id, metadata);
            this.connections.set(id, remote);
          }
          catch (err) {
            this.#reportError(err, 'PEER_CONNECTION_FAILED');
            return;
          }
        }
        this.#createChannels(remote, filteredChannels);
        this.#publishStreams(remote, filteredStreams);
      }

      // inform the initiator about existing channels and streams
      if (!channels && !streams || !isPolite) {
        try {
          const remote = this.connections.get(id);
          await this.driver.emit([this.room, id], {
            type: 'invoke',
            id: this.id,
            metadata: this.metadata,
            channels: Array.from(remote?.channels.keys() || []),
            streams: Array.from(remote?.streams.keys() || []),
          });
        }
        catch (err) {
          this.#reportError(err, 'PEER_SIGNALING_FAILED');
        }
      }

      return;
    }

    // set remote description and create answer
    if (type === 'sdp') {
      const { description, metadata, labels } = signal;

      let remote = this.connections.get(id);
      if (!remote) {
        try {
          remote = this.#createPeerConnection(id, metadata);
          this.connections.set(id, remote);
        }
        catch (err) {
          this.#reportError(err, 'PEER_CONNECTION_FAILED');
          return;
        }
      }

      if (labels) {
        this.#streamLabels.set(id, labels);
      }

      const { connection } = remote;

      const readyForOffer = !this.#makingOffer.has(id) &&
        (connection.signalingState === 'stable' || this.#pendingAnswer.has(id));
      const offerCollision = description.type === 'offer' && !readyForOffer;

      const isPolite = this.id > id;
      if (!isPolite && offerCollision) return;

      // wait to avoid interrupting previous operations 
      while (this.#makingOffer.has(id) || this.#pendingAnswer.has(id)) {
        await timeout(0);
        console.log('timeout', this.id);
      }

      try {
        await this.#setRemoteDescription(remote, description);

        this.#addQueuedIceCandidates(remote);

        if (description.type === 'offer') {
          const answer = await this.#createAnswer(remote);
          if (answer) {
            try {
              await this.driver.emit([this.room, id], {
                type: 'sdp',
                id: this.id,
                metadata: this.metadata,
                description: answer,
              });
            }
            catch (err) {
              this.#reportError(err, 'PEER_SIGNALING_FAILED');
            }
          }
        }
      }
      catch (err) {
        this.#reportError(err, 'PEER_NEGOTIATION_FAILED');
      }

      return;
    }

    // add ice candidate
    if (type === 'ice') {
      const { candidate } = signal;
      const remote = this.connections.get(id);

      const queued = this.#queueIceCandidate(id, candidate, remote);
      if (!remote || queued) return;

      this.#addIceCandidate(remote, candidate);

      return;
    }

    // dispose peer connection
    if (type === 'dispose') {
      const remote = this.connections.get(id);
      if (!remote) return;

      remote.dispose({ silent: true });

      return;
    }
  }
}
