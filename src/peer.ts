import type { SignalingDriver } from './types/signaling.js';
import type { PeerOptions, JoinOptions, RemotePeer, StreamOptions, ChannelOptions, SendOptions, PeerEvents } from './types/peer.js';
import { MemoryDriver } from './drivers/memory.js';
import EventEmitter from './utils/emitter.js';
import { UUIDv4, timeout } from './utils/helpers.js';
import log from './utils/logger.js';

// All peers without a driver will share the same in-memory signaling bus
const defaultDriver = new MemoryDriver();

/**
 * Peer class for managing WebRTC peer connections and signaling.
 * 
 * @group Peers
 * @example
 * ```javascript
 * // create a new peer
 * // using default in-memory signaling driver
 * const peer = new Peer();
 *
 * // listen for open channel event
 * peer.on('open', (e) => {
 *   const { remote, channel } = e;
 *   // send a message to the connected peer
 *   channel.send('Hello, peer!');
 * });
 *
 * // listen for incoming messages
 * peer.on('message', (e) => {
 *   const { remote, channel, data } = e;
 *   console.log('Received message:', data);
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
   * Internal event emitter used by on/once/off/emit helpers.
   */
  private _emitter: EventEmitter<PeerEvents>;
  /**
   * ICE candidates received before remote description is applied.
   */
  private _candidateQueues: Map<string, any[]>;
  /**
   * Media stream labels for incoming streams, indexed by remote peer id.
   */
  private _streamLabels: Map<string, { [key: string]: string }>;
  /**
   * Set of peer ids for which a local offer is being created 
   * to handle glare scenarios.
   */
  private _makingOffer: Set<string>;
  /**
   * Active signaling handler registered on the signaling driver.
   */
  private _signal?: (e: any) => void;
  /**
   * Optional callback to accept or reject incoming peer connections.
   */
  private _verify?: (options: { id: string; metadata?: any }) => Promise<boolean> | boolean;

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
      verify,
    } = options || {};
    this.driver = driver;
    this.id = id;
    this.room = '';
    this.iceServers = iceServers;
    this.iceTransportPolicy = iceTransportPolicy;
    this.connectionTimeout = connectionTimeout;
    this.connections = new Map();
    this.streams = new Map();
    this.channels = new Map();
    this.addons = new Set();
    this._emitter = new EventEmitter<PeerEvents>(this);
    this._candidateQueues = new Map();
    this._streamLabels = new Map();
    this._makingOffer = new Set();
    this._verify = verify;
  }

  /**
   * Indicates whether the peer is currently active.
   *
   * @returns True if the Peer is joined to a room, false otherwise.
   */
  get active(): boolean {
    return !!this._signal;
  }

  /**
   * Join a room and start listening for incoming connections.
   *
   * @param options Room name or join options.
   */
  async join(options?: string | JoinOptions) {
    if (this._signal) return;

    const { room = 'default', metadata } = typeof options === 'object'
      ? options : { room: options };

    this.room = room;
    this.metadata = metadata;

    this._signal = this._signalHandler.bind(this);

    this.driver.on([this.room], this._signal);
    this.driver.on([this.room, this.id], this._signal);

    this.driver.emit([this.room], {
      type: 'invoke',
      id: this.id,
      metadata: this.metadata,
    });

    log('peer:join', { room: this.room, metadata: this.metadata });
  }

  /**
    * Leave the current room and close all active remote connections.
   */
  async leave() {
    if (!this._signal) return;

    this.driver.off([this.room], this._signal);
    this.driver.off([this.room, this.id], this._signal);

    for (const remote of this.connections.values()) {
      remote.dispose();
    }
    this.connections.clear();

    this._candidateQueues.clear();
    this._makingOffer.clear();
    this._streamLabels.clear();

    delete this._signal;

    log('peer:leave', { room: this.room });
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
      managed
    } = this.streams.get(label) || {};
    this.streams.set(label, { ...opts, label, stream: newStream });

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

    if (this._signal) {
      this.driver.emit([this.room], {
        type: 'invoke',
        id: this.id,
        metadata: this.metadata,
      });
    }

    log('peer:publish', { label, options });
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
        if (sender) connection.removeTrack(sender);
      }
    }

    log('peer:unpublish', { label });
  }

  /**
   * Register or create a negotiated data channel with all remote peers.
   *
   * @param options Channel options or channel label.
   */
  async open(options: string | ChannelOptions) {
    const { label = 'default', ...opts } = typeof options === 'object'
      ? options : { label: options };

    this.channels.set(label, { ...opts, label });

    if (this._signal) {
      this.driver.emit([this.room], {
        type: 'invoke',
        id: this.id,
        metadata: this.metadata,
      });
    }

    log('peer:open', { label, options });
  }

  /**
   * Close and unregister a negotiated data channel by id.
   *
   * @param options Channel label or object containing `label`.
   */
  async close(options: string | { label: string }) {
    const { label = 'default' } = typeof options === 'object'
      ? options : { label: options };

    this.channels.delete(label);

    for (const remote of this.connections.values()) {
      const channel = remote.channels.get(label);
      if (channel) channel.close();
      remote.channels.delete(label);
    }

    log('peer:close', { label });
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
    if (!this._signal) return;

    const { label, filter } = typeof options === 'object'
      ? options : { label: options };

    for (const remote of this.connections.values()) {
      if (typeof label === 'string') {
        const channel = remote.channels.get(label);
        if (!channel || channel.readyState !== 'open') continue;
        if (channel.label !== label) continue;
        if (typeof filter === 'function') {
          const allowed = filter({ id: remote.id, metadata: remote.metadata, label: channel.label });
          if (!allowed) continue;
        }
        channel.send(message);
      }
      else {
        for (const channel of remote.channels.values()) {
          if (!channel || channel.readyState !== 'open') continue;
          if (typeof filter === 'function') {
            const allowed = filter({ id: remote.id, metadata: remote.metadata, label: channel.label });
            if (!allowed) continue;
          }
          channel.send(message);
        }
      }
    }

    log('peer:send', { message, options });
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
    this._emitter.on(event, handler);
  }

  /**
   * Subscribe to an event and auto-unsubscribe after first invocation.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
  once<K extends keyof PeerEvents>(event: K | K[], handler: (...args: PeerEvents[K]) => void) {
    this._emitter.once(event, handler);
  }

  /**
   * Remove a previously registered event listener.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler to remove. If omitted, all handlers for the given event(s) will be removed.
   */
  off<K extends keyof PeerEvents>(event: K | K[], handler?: (...args: PeerEvents[K]) => void) {
    this._emitter.off(event, handler);
  }

  /**
   * Emit one or more events.
   *
   * @param event Event name or list of event names.
   * @param args Event payload.
   */
  emit<K extends keyof PeerEvents>(event: K | K[], ...args: PeerEvents[K]) {
    this._emitter.emit(event, ...args);
    log(`peer:emit:${event}`, ...args);
  }

  _parseUfrag(sdp?: string) {
    return sdp && /a=ice-ufrag:([^\s]+)/m.exec(sdp)?.[1];
  }

  async _addQueuedCandidates(remote: RemotePeer) {
    const { id, connection } = remote;

    if (this._candidateQueues.has(id)) {
      const { sdp } = connection.remoteDescription || {};
      for (const candidate of this._candidateQueues.get(id) || []) {
        try {
          const ufrag = this._parseUfrag(sdp);
          if (!sdp || ufrag !== candidate.usernameFragment) {
            console.warn('### ignoring queued candidate', { localId: this.id, remoteId: id, ufrag: candidate.usernameFragment });
            continue;
          }

          console.log('### adding queued candidate', { localId: this.id, remoteId: id, ufrag: candidate.usernameFragment });
          await connection.addIceCandidate(candidate);
        }
        catch (error) {
          this.emit('error', { remote, error, code: 'QUEUED_CANDIDATE_ERROR' });
        }
      }
      this._candidateQueues.delete(id);
    }
  }

  /**
   * Helper method to create a data channel.
   * 
   * @param remote Remote peer descriptor.
   * @param channel Data channel instance.
   */
  private _setupDataChannel(remote: RemotePeer, channel: RTCDataChannel) {
    const { label = '' } = channel;
    remote.channels.set(label, channel);

    channel.addEventListener('open', () => {
      this.emit('open', { remote, channel });
    });
    channel.addEventListener('close', () => {
      remote.channels.delete(label);
      this.emit('close', { remote, channel });

      // close connection if there are no more active streams or channels
      if (!remote.channels.size && !remote.streams.size) {
        remote.dispose();
      }
    });
    channel.addEventListener('message', (e) => {
      this.emit('message', { remote, channel, data: e.data });
    });
    channel.addEventListener('error', (e) => {
      this.emit('error', { remote, channel, error: e.error, code: 'CHANNEL_ERROR' });
    });
  }

  _createChannels(remote: RemotePeer, channels: Map<string, ChannelOptions>) {
    if (!channels.size) return;
    const { connection } = remote;
    for (const channelOptions of channels.values()) {
      const { label = '', filter, ...opts } = channelOptions || {};
      const channel = connection.createDataChannel(label, opts);
      this._setupDataChannel(remote, channel);
    }
  }

  async _publishStreams(remote: RemotePeer, streams: Map<string, StreamOptions>) {
    if (!streams.size) return;

    const { connection } = remote;

    const setBitrate = (id: string, bitrate: number) => {
      if (!bitrate) return;
      const senders = connection.getSenders();
      const sender = senders.find((sender: RTCRtpSender) => {
        return sender.track && sender.track.id === id;
      });
      if (sender) {
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
      const { stream, audioBitrate, videoBitrate } = options;

      const bitrate: { [key: string]: number } = {
        audio: (audioBitrate || 0) | 0,
        video: (videoBitrate || 0) | 0,
      };

      const tracks = stream.getTracks();
      const senders = connection.getSenders();

      // Replace existing senders for tracks that still exist in the stream,
      // add new senders for new tracks
      for (const track of tracks) {
        const sender = senders.find((sender: RTCRtpSender) => {
          return sender.track && sender.track.id === track.id
            && sender.track.readyState !== 'ended';
        });
        if (sender) await sender.replaceTrack(track);
        else connection.addTrack(track, stream);
        // set bitrate for new or existing sender
        setBitrate(track.id, bitrate[track.kind]);
      }

      // Remove senders for tracks that no longer exist in the stream
      for (const sender of senders) {
        if (!sender.track) continue;
        const trackExists = tracks.some(track => track.id === sender.track?.id);
        if (!trackExists) connection.removeTrack(sender);
      }
    }
  }

  _createRemote(id: string, metadata: any): RemotePeer {
    const connection = new RTCPeerConnection({
      iceServers: this.iceServers,
      iceTransportPolicy: this.iceTransportPolicy,
    });
    const streams = new Map();
    const channels = new Map();

    const setConnectionTimeout = () => {
      const timer = this.connectionTimeout > 0 ? setTimeout(
        () => {
          dispose();
          const error = new Error('Connection timeout');
          this.emit('error', { remote, error, code: 'CONNECTION_TIMEOUT' });
        },
        this.connectionTimeout * 1000,
      ) : undefined;

      return () => clearTimeout(timer);
    };

    const dispose = ({ silent = false } = {}) => {
      if (!this.connections.has(id)) return;
      this.connections.delete(id);
      stopConnectionTimeout();

      channels.forEach(channel => channel?.close());
      connection?.close();
      remote.state = 'closed';

      this._candidateQueues.delete(id);
      this._makingOffer.delete(id);
      this._streamLabels.delete(id);

      if (!silent) {
        this.driver.emit([this.room, id], {
          type: 'dispose',
          id: this.id,
        });
      }

      this.emit('state', { remote, state: 'closed' });
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
        this.emit('state', { remote, state });
      }
      else if (iceConnectionState === 'connected') {
        stopConnectionTimeout();
        const state = 'connected';
        remote.state = state;
        this.emit('state', { remote, state });
      }
      else if (iceConnectionState === 'disconnected') {
        const state = 'disconnected';
        remote.state = state;
        this.emit('state', { remote, state });
        stopConnectionTimeout = setConnectionTimeout();
      }
      else if (iceConnectionState === 'failed') {
        const state = 'failed';
        remote.state = state;
        this.emit('state', { remote, state });
        dispose();
      }
      else if (iceConnectionState === 'closed') {
        remote.state = 'closed';
        dispose();
      }
    });

    connection.addEventListener('icecandidate', (e) => {
      const { candidate } = e;
      if (!candidate) return;

      this.driver.emit([this.room, id], {
        type: 'ice',
        id: this.id,
        candidate: typeof candidate.toJSON === 'function'
          ? candidate.toJSON()
          : candidate,
      });
    });

    connection.addEventListener('negotiationneeded', async () => {
      console.warn('### negotiationneeded', { localId: this.id, remoteId: id, signalingState: connection.signalingState, makingOffer: this._makingOffer.has(id) });
      try {
        if (connection.signalingState !== 'stable') return;

        this._makingOffer.add(id);

        const offer = await connection.createOffer();
        await connection.setLocalDescription(offer);

        this._makingOffer.delete(id);

        this.driver.emit([this.room, id], {
          type: 'sdp',
          id: this.id,
          metadata: this.metadata,
          description: offer,
          labels: Array.from(this.streams.keys()).reduce((acc, label) => {
            const { stream } = this.streams.get(label) || {};
            if (stream) acc[stream.id] = label;
            return acc;
          }, {} as { [key: string]: string }),
        });
      } catch (error) {
        this._makingOffer.delete(id);
        this.emit('error', { remote, error, code: 'NEGOTIATION_ERROR' });
      }
    });

    connection.addEventListener('datachannel', (e) => {
      const { channel } = e;
      this._setupDataChannel(remote, channel);
    });

    connection.addEventListener('track', (e) => {
      const { track, streams: [stream] } = e;

      const labels = this._streamLabels.get(id) || {};
      const label = labels[stream.id] || stream.id;

      if (!streams.has(label)) {
        streams.set(label, stream);
        stream.addEventListener('removetrack', (e) => {
          const { track } = e;

          if (!stream.getTracks().length) {
            streams.delete(label);
          }
          this.emit('unpublish', { remote, stream, track, label });

          // close connection if there are no more active streams or channels
          if (!remote.channels.size && !remote.streams.size) {
            remote.dispose();
          }
        });
      }

      this.emit('publish', { remote, stream, track, label });
    });

    this.emit('state', { remote, state: 'new' });

    return remote;
  }

  async _signalHandler(e: any) {
    const { type, id } = e;
    if (!type || !id || this.id === id) return;

    log('peer:signal', e);

    if (type === 'invoke') {
      const { metadata, channels, streams } = e;

      try {
        // verify the incoming connection and reject if verification fails
        if (this._verify) {
          const verified = await this._verify({ id, metadata });
          if (!verified) return;
        }

        let remote = this.connections.get(id);

        // get filtered list of channels
        const remoteChannels = new Set(channels || []);
        const localChannels = new Map<string, ChannelOptions>();
        for (const channelOptions of this.channels.values()) {
          const { label = '', filter } = channelOptions;
          if (remoteChannels.has(label)) continue;
          if (remote?.channels.has(label)) continue;
          if (typeof filter === 'function') {
            const allowed = filter({ id, metadata, label });
            if (!allowed) continue;
          }
          localChannels.set(label, channelOptions);
        }

        // get filtered list of streams
        const localStreams = new Map<string, StreamOptions>();
        for (const streamOptions of this.streams.values()) {
          const { label = '', filter } = streamOptions;
          if (remote?.streams.has(label)) continue;
          if (typeof filter === 'function') {
            const allowed = filter({ id, metadata, label });
            if (!allowed) continue;
          }
          localStreams.set(label, streamOptions);
        }

        if (localChannels.size || localStreams.size) {
          if (!remote) {
            remote = this._createRemote(id, metadata);
            this.connections.set(id, remote);
          }
          this._publishStreams(remote, localStreams);
          this._createChannels(remote, localChannels);
        }

        if (!channels && !streams) {
          this.driver.emit([this.room, id], {
            type: 'invoke',
            id: this.id,
            metadata: this.metadata,
            channels: Array.from(localChannels.keys()),
            streams: Array.from(localStreams.keys()),
          });
        }
      }
      catch (error) {
        this.emit('error', { error, code: 'INVOKE_ERROR' });
      }

      return;
    }

    // set remote description and create answer
    if (type === 'sdp') {
      const { description, metadata, labels } = e;

      let remote = this.connections.get(id);
      if (!remote) {
        remote = this._createRemote(id, metadata);
        this.connections.set(id, remote);
      }

      if (labels) {
        this._streamLabels.set(id, labels);
      }

      try {
        const { connection } = remote;

        const offerCollision = description.type === 'offer'
          && (this._makingOffer.has(id) || connection.signalingState !== 'stable');
        const isPolite = this.id > id;

        console.warn('### description', { localId: this.id, remoteId: id, type: description.type, signalingState: connection.signalingState, makingOffer: this._makingOffer.has(id), offerCollision, isPolite });
        if (offerCollision && isPolite) {
          return console.warn('### ignoring offer', { lid: this.id, rid: id, isPolite, offerCollision });
        }

        // HOTFIX: seems to help with some edge cases of glare and offer collisions
        if (offerCollision) await timeout(100);

        await connection.setRemoteDescription(description);

        await this._addQueuedCandidates(remote);

        if (description.type === 'offer') {
          const answer = await connection.createAnswer();
          await connection.setLocalDescription(answer);

          this.driver.emit([this.room, id], {
            type: 'sdp',
            id: this.id,
            metadata: this.metadata,
            description: answer,
          });
        }
      } catch (error) {
        this.emit('error', { remote, error, code: 'DESCRIPTION_ERROR' });
      }

      return;
    }

    // add ice candidate
    if (type === 'ice') {
      const { candidate } = e;
      const remote = this.connections.get(id);

      const { sdp } = remote?.connection.remoteDescription || {};
      const ufrag = this._parseUfrag(sdp);

      if (!remote || ufrag !== candidate.usernameFragment) {
        console.log('### queueing candidate', { localId: this.id, remoteId: id, ufrag: candidate.usernameFragment });
        const queue = this._candidateQueues.get(id);
        if (!queue) this._candidateQueues.set(id, [candidate]);
        else queue.push(candidate);
        return;
      }

      try {
        const { connection } = remote;
        console.log('### adding candidate', { localId: this.id, remoteId: id, ufrag: candidate.usernameFragment });
        await connection.addIceCandidate(candidate);
      }
      catch (error) {
        this.emit('error', { remote, error, code: 'CANDIDATE_ERROR' });
      }

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
