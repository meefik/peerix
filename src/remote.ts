import { IceServer, IceTransportPolicy, PeerConnectionState, ChannelOptions, StreamOptions } from './peer.js';
import log from './utils/logger.js';
import { PeerixError } from './error.js';
import { timeout } from './utils/helpers.js';
import { EventEmitter } from './utils/emitter.js';
import { ConnectionManager } from './manager.js';

/**
 * Class representing a remote peer connection.
 * You do not need to create RemotePeer instances manually.
 * 
 * @group Remote Peers
 */
export class RemotePeer {
  /** Remote peer identifier. */
  readonly id: string;
  /** Metadata advertised by the remote peer. */
  readonly metadata?: any;
  /** Room name the peer is associated with. */
  readonly room: string;
  /** Native WebRTC peer connection to the remote peer. */
  readonly connection: RTCPeerConnection;
  /** Remote media streams keyed by stream label. */
  readonly streams: Map<string, MediaStream>;
  /** Negotiated data channels keyed by channel label. */
  readonly channels: Map<string, RTCDataChannel>;

  /** Peer connection state, updated on connection state changes. */
  state: PeerConnectionState;

  #emitter: EventEmitter<RemotePeerEvents>;
  #polite: boolean;
  #streamLabels: Map<string, string>;
  #makingOffer: boolean;
  #pendingAnswer: boolean;
  #manager: ConnectionManager;
  #streams: Map<string, StreamOptions>;
  #channels: Map<string, ChannelOptions>;

  /**
   * Create a RemotePeer instance.
   * 
   * @ignore
   * @param options Options for creating the remote peer connection.
   */
  constructor(options: RemotePeerOptions) {
    const {
      id,
      metadata,
      room,
      polite,
      iceServers = [],
      iceTransportPolicy = 'all',
      connectionTimeout = 15,
      streams,
      channels,
    } = options;

    const connection = new RTCPeerConnection({ iceServers, iceTransportPolicy });
    this.connection = connection;
    this.state = 'new';
    this.id = id;
    this.metadata = metadata;
    this.room = room;
    this.streams = new Map();
    this.channels = new Map();

    this.#polite = polite;
    this.#emitter = new EventEmitter(this);
    this.#streamLabels = new Map();
    this.#makingOffer = false;
    this.#pendingAnswer = false;
    this.#streams = new Map(streams);
    this.#channels = new Map(channels);

    connection.addEventListener('iceconnectionstatechange', () => {
      const { iceConnectionState } = connection;

      if (iceConnectionState === 'new') {
        this.#setConnectionState('new');
      }
      else if (iceConnectionState === 'checking') {
        this.#setConnectionState('connecting');
      }
      else if (iceConnectionState === 'connected') {
        this.#setConnectionState('connected');
      }
      else if (iceConnectionState === 'disconnected') {
        this.#setConnectionState('disconnected');
      }
      else if (iceConnectionState === 'failed') {
        this.#setConnectionState('failed');
        this.dispose();
      }
      else if (iceConnectionState === 'closed') {
        this.dispose();
      }
    });

    connection.addEventListener('icecandidate', async (e) => {
      const { candidate } = e;
      if (!candidate) return;
      try {
        const candidateInit = typeof candidate.toJSON === 'function'
          ? candidate.toJSON() : candidate;
        if (this.#manager.active) this.#manager.send('candidate', candidateInit);
        else this.emit('candidate', {
          id: this.id,
          name: 'candidate',
          candidate: candidateInit,
        });
      }
      catch (err) {
        const error = new PeerixError(err, 'PEER_ICECANDIDATE_ERROR');
        this.emit('error', { id: this.id, name: 'error', error });
      }
    });

    connection.addEventListener('negotiationneeded', () => {
      if (connection.signalingState !== 'stable') return;
      try {
        this.#createOffer();
      } catch (err) {
        const error = new PeerixError(err, 'PEER_NEGOTIATION_ERROR');
        this.emit('error', { id: this.id, name: 'error', error });
      }
    });

    connection.addEventListener('datachannel', (e) => {
      const { channel } = e;
      try {
        this.#setupDataChannel(channel);
      }
      catch (err) {
        const error = new PeerixError(err, 'PEER_DATACHANNEL_ERROR');
        this.emit('error', { id: this.id, name: 'error', error });
      }
    });

    connection.addEventListener('track', (e) => {
      const { track, streams: [stream] } = e;
      try {
        this.#setupMediaTrack(track, stream);
      }
      catch (err) {
        const error = new PeerixError(err, 'PEER_MEDIASTREAM_ERROR');
        this.emit('error', { id: this.id, name: 'error', error });
      }
    });

    const manager = new ConnectionManager({ connection, connectionTimeout });
    this.#manager = manager;

    manager.on('open', () => {
      // create channels
      for (const channelOptions of this.#channels.values()) {
        try {
          this.#createChannel(channelOptions);
        } catch (err) {
          const error = new PeerixError(err, 'PEER_DATACHANNEL_ERROR');
          this.emit('error', { id: this.id, name: 'error', error });
        }
      }
      // add streams
      for (const streamOptions of this.#streams.values()) {
        try {
          this.#addStream(streamOptions);
        } catch (err) {
          const error = new PeerixError(err, 'PEER_MEDIASTREAM_ERROR');
          this.emit('error', { id: this.id, name: 'error', error });
        }
      }
    });

    manager.on('close', () => {
      this.dispose();
    });

    manager.on('timeout', () => {
      this.#setConnectionState('failed');
      this.dispose();
    });

    manager.on('channel', (channelOptions) => {
      try {
        this.#createChannel(channelOptions);
      } catch (err) {
        const error = new PeerixError(err, 'PEER_DATACHANNEL_ERROR');
        this.emit('error', { id: this.id, name: 'error', error });
      }
    });

    manager.on('offer', async (description, labels) => {
      try {
        if (labels) {
          this.#setStreamLabels(labels);
        }
        await this.applyDescription(description);
      } catch (err) {
        const error = new PeerixError(err, 'PEER_NEGOTIATION_ERROR');
        this.emit('error', { id: this.id, name: 'error', error });
      }
    });

    manager.on('answer', async (description) => {
      try {
        await this.applyDescription(description);
      } catch (err) {
        const error = new PeerixError(err, 'PEER_NEGOTIATION_ERROR');
        this.emit('error', { id: this.id, name: 'error', error });
      }
    });

    manager.on('candidate', async (candidate) => {
      await this.addIceCandidate(candidate);
    });

    manager.open();
  }

  /**
   * Register an event handler for a specific event type emitted by the remote peer connection.
   * 
   * @param event Event type to listen for.
   * @param handler Callback function to handle the event.
   */
  on<K extends keyof RemotePeerEvents>(event: K | K[], handler: (...args: RemotePeerEvents[K]) => void) {
    this.#emitter.on(event, handler);
  }

  /**
   * Unregister an event handler for a specific event type emitted by the remote peer connection.
   * 
   * @param event Event type to stop listening for.
   * @param handler Callback function to remove from the event listeners.
   */
  off<K extends keyof RemotePeerEvents>(event: K | K[], handler: (...args: RemotePeerEvents[K]) => void) {
    this.#emitter.off(event, handler);
  }

  /**
   * Emit an event with optional payload to the registered event handlers.
   * 
   * @param event Event type to emit.
   * @param args Optional arguments to pass to the event handlers.
   */
  emit<K extends keyof RemotePeerEvents>(event: K | K[], ...args: RemotePeerEvents[K]) {
    this.#emitter.emit(event, ...args);
  }

  /**
   * Cleanup routine that closes and frees all connection resources.
   */
  dispose() {
    log('remote:dispose', { id: this.id, room: this.room, metadata: this.metadata });

    this.#manager.close();
    this.channels.forEach(channel => channel?.close());
    this.connection?.close();

    this.#streamLabels.clear();
    this.#makingOffer = false;
    this.#pendingAnswer = false;

    this.#setConnectionState('closed');
  }

  /**
   * Publish new or update an existing media stream to the current remote peer.
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
    } = this.#streams.get(label) || {};

    const addedTracks = [];
    const removedTracks = [];

    for (const track of newStream.getTracks()) {
      if (!stream.getTracks().find(t => t.id === track.id)) {
        newStream.removeTrack(track);
        if (managed && track.readyState !== 'ended') {
          track.stop();
        }
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

    log('remote:publish', { id: this.id, ...newStreamOptions });

    this.#streams.set(label, newStreamOptions);

    const bitrate = { audio: opts.audioBitrate, video: opts.videoBitrate };
    await this.#updateStream(addedTracks, removedTracks, newStream, bitrate);

    return newStream;
  }

  /**
   * Stop publishing a previously published media stream to the current remote peer.
   * 
   * If you pass a MediaStream instance directly, it will be unpublished based 
   * on its id as label. Otherwise, you can specify the label in the options 
   * object or pass it directly as a string. 
   * 
   * If the stream was published with the `managed` option, its tracks will be 
   * stopped automatically.
   * 
   * @param options A stream label, MediaStream instance, or an object containing a label.
   * @returns The unpublished MediaStream instance, or undefined.
   */
  async unpublish(options: MediaStream | string | { label?: string; }): Promise<MediaStream | void> {
    if (options instanceof MediaStream) {
      options = { label: options.id };
    }
    const { label: rawLabel = 'default' } =
      typeof options === 'object' ? options : { label: options };
    const label = String(rawLabel);

    const oldStreamOptions = this.#streams.get(label);
    const { stream, managed } = oldStreamOptions || {};

    log('remote:unpublish', { id: this.id, label, stream });

    this.#streams.delete(label);

    if (!stream) return;

    if (managed) {
      for (const track of stream.getTracks()) {
        if (track.readyState !== 'ended') {
          track.stop();
        }
      }
    }

    await this.#removeStream(stream);

    return stream;
  }

  /**
   * Open a data channel to the current remote peer.
   * 
   * If a channel with the same label already exists, it will be reused.
   * 
   * You can open a channel with the same label on both local and remote peers
   * or only on one side. In any case, only one channel will be created for 
   * each label. You can send data through the channel in both directions.
   *
   * @param options Channel options or channel label.
   */
  async open(options: string | ChannelOptions) {
    const { label: rawLabel = 'default', ...channelOptions } =
      typeof options === 'object' ? options : { label: options };
    const label = String(rawLabel);

    log('remote:open', { id: this.id, label, ...channelOptions });

    this.#channels.set(label, { label, ...channelOptions });

    this.#createChannel({ label, ...channelOptions });
  }

  /**
   * Close a previously opened data channel to the current remote peer.
   * 
   * @param options Channel label or object containing `label`.
   */
  async close(options: string | { label: string; }) {
    const { label: rawLabel = 'default' } =
      typeof options === 'object' ? options : { label: options };
    const label = String(rawLabel);

    log('remote:close', { id: this.id, label });

    this.#channels.delete(label);

    const channel = this.channels.get(label);
    channel?.close();
  }

  /**
   * Send a message through data channels.
   *
   * If `options` is omitted, the message is sent to all open channels for every
   * connected remote peer. If `options` is a string, it is treated as channel label.
   *
   * @param message Message payload to send. This may be a string, a Blob, an ArrayBuffer, a TypedArray or a DataView object.
   * @param options Optional channel label or object containing `label`.
   */
  send(message: any, options?: string | { label?: string; }) {
    const { label: rawLabel } =
      typeof options === 'object' ? options : { label: options };
    const label = typeof rawLabel === 'undefined' ? undefined : String(rawLabel);

    log('remote:send', { id: this.id, label, message });

    if (typeof label === 'string') {
      const channel = this.channels.get(label);
      if (channel && channel.label === label && channel.readyState === 'open') {
        channel.send(message);
      }
    }
    else {
      for (const channel of this.channels.values()) {
        if (channel && channel.readyState === 'open') {
          channel.send(message);
        }
      }
    }
  }

  /**
   * Apply a remote session description and add the corresponding ICE candidates to the peer connection.
   * 
   * @ignore
   * @param description Remote session description to apply.
   * @param getCandidates Function that returns the ICE candidates corresponding to the given session description.
   * @returns Promise that resolves when the description is applied and ICE candidates are added.
   */
  async applyDescription(description: RTCSessionDescriptionInit, getCandidates?: (description: RTCSessionDescriptionInit) => RTCIceCandidateInit[]) {
    const hasOffer = description.type === 'offer';

    if (hasOffer && this.#hasCollision()) return;

    await this.#setRemoteDescription(description);

    const candidates = getCandidates ? getCandidates(description) : [];
    for (const candidate of candidates) {
      await this.addIceCandidate(candidate);
    }

    if (hasOffer) {
      await this.#createAnswer();
    }
  }

  /**
   * Add an ICE candidate to the peer connection.
   * 
   * @ignore
   * @param candidate ICE candidate to add.
   * @returns Promise that resolves when the candidate is added.
   */
  async addIceCandidate(candidate: RTCIceCandidateInit) {
    const { connection } = this;

    try {
      await connection.addIceCandidate(candidate);
    }
    catch (err) {
      const error = new PeerixError(err, 'PEER_ICECANDIDATE_ERROR');
      this.emit('error', { id: this.id, name: 'error', error });
    }
  }

  /**
   * Helper method to update the connection state.
   * 
   * @param state New connection state to set and emit.
   */
  #setConnectionState(state: PeerConnectionState) {
    if (this.state === state) return;
    this.state = state;
    this.emit(
      ['connection', `connection:${state}`],
      { id: this.id, name: `connection:${state}`, state }
    );
  }

  /**
   * Set custom labels for remote media streams based on their stream ids.
   * 
   * @param labels Object mapping stream ids to custom labels.
   */
  #setStreamLabels(labels: { [key: string]: string; }) {
    this.#streamLabels.clear();
    for (const streamId in labels) {
      this.#streamLabels.set(streamId, labels[streamId]);
    }
  }

  /**
   * Helper method to create a data channel.
   * 
   * @param options Channel options for creating data channels.
   */
  #createChannel(options: ChannelOptions) {
    const { label = 'default', ...channelOptions } = options || {};
    if (this.channels.has(label)) return;

    if (this.#polite) {
      this.#manager.send('channel', { label, ...channelOptions });
    }
    else {
      const channel = this.connection.createDataChannel(label, channelOptions);
      this.#setupDataChannel(channel);
    }
  }

  /**
   * Helper method to add a local media stream to a remote peer.
   * 
   * @param options Object containing the stream and optional bitrate settings.
   */
  async #addStream(options: { stream: MediaStream, audioBitrate?: number, videoBitrate?: number; }) {
    const { stream, audioBitrate, videoBitrate } = options;
    const { connection } = this;

    const tracks = stream.getTracks();
    const senders = connection.getSenders();

    for (const track of tracks) {
      const hasSender = senders.some(s => s.track && s.track.id === track.id);
      if (hasSender) continue;

      connection.addTransceiver(track, { direction: 'sendonly', streams: [stream] });
      await this.#setTrackBitrate(track, { audio: audioBitrate, video: videoBitrate });
    }
  }

  /**
   * Helper method to update the media stream by adding and removing tracks as needed.
   * 
   * @param addedTracks Array of tracks to be added to the stream.
   * @param removedTracks Array of tracks to be removed from the stream.
   * @param stream The media stream to be updated.
   * @param bitrate Optional bitrate settings for the tracks.
   */
  async #updateStream(addedTracks: MediaStreamTrack[], removedTracks: MediaStreamTrack[], stream: MediaStream, bitrate?: { [key: string]: number | undefined; }) {
    const { connection } = this;
    const senders = connection.getSenders();

    for (const track of addedTracks) {
      const removedTrack = removedTracks.find(t => t.kind === track.kind);
      if (removedTrack) {
        const sender = senders.find(s => s.track?.id === removedTrack.id);
        if (sender) {
          await sender.replaceTrack(track);
          await this.#setTrackBitrate(track, bitrate);
          continue;
        }
      }
      connection.addTransceiver(track, { direction: 'sendonly', streams: [stream] });
      await this.#setTrackBitrate(track, bitrate);
    }

    for (const transceiver of connection.getTransceivers()) {
      if (transceiver.direction !== 'sendonly') continue;
      const readyToStop = removedTracks.some(t => (!transceiver.sender.track || t.id === transceiver.sender.track.id));
      if (readyToStop) transceiver.stop();
    }
  }

  /**
   * Helper method to remove a media stream from the peer connection.
   * 
   * @param stream The media stream to be removed.
   */
  async #removeStream(stream: MediaStream) {
    const { connection } = this;
    const existingTracks = stream?.getTracks() || [];

    for (const transceiver of connection.getTransceivers()) {
      if (transceiver.direction !== 'sendonly') continue;
      const readyToStop = existingTracks.some(t => (!transceiver.sender.track || t.id === transceiver.sender.track.id));
      if (readyToStop) transceiver.stop();
    }
  }

  /**
   * Helper method to check if there is a signaling collision with the remote peer.
   * 
   * @returns A boolean indicating whether there is a signaling collision.
   */
  #hasCollision() {
    const readyForOffer = !this.#makingOffer &&
      (this.connection.signalingState === 'stable' || this.#pendingAnswer);
    return (!this.#polite && !readyForOffer);
  }

  /**
   * Helper method to create an offer, set it as the local description 
   * and emit an 'offer' event with the offer and stream labels.
   */
  async #createOffer() {
    const { connection } = this;

    try {
      this.#makingOffer = true;

      const offer = await connection.createOffer();
      await connection.setLocalDescription(offer);

      if (this.#manager.active) {
        const labels = Array.from(this.#streams.keys())
          .reduce((acc, label) => {
            const { stream } = this.#streams.get(label) || {};
            if (stream) acc[stream.id] = label;
            return acc;
          }, {} as { [key: string]: string; });
        this.#manager.send('offer', offer, labels);
      }
      else this.emit('offer', { id: this.id, name: 'offer', description: offer });
    }
    finally {
      this.#makingOffer = false;
    }
  }

  /**
   * Helper method to create an answer, set it as the local description 
   * and emit an 'answer' event with the answer.
   */
  async #createAnswer() {
    const { connection } = this;

    try {
      this.#pendingAnswer = true;

      const answer = await connection.createAnswer();
      await connection.setLocalDescription(answer);

      if (this.#manager.active) this.#manager.send('answer', answer);
      else this.emit('answer', { id: this.id, name: 'answer', description: answer });
    }
    finally {
      this.#pendingAnswer = false;
    }
  }

  /**
   * Helper method to set the remote session description on the peer connection.
   * 
   * @param description The remote session description to be set.
   */
  async #setRemoteDescription(description: RTCSessionDescriptionInit) {
    const { connection } = this;

    // wait to avoid interrupting previous operations
    while (this.#makingOffer || this.#pendingAnswer) {
      await timeout(0);
    }

    await connection.setRemoteDescription(description);
  }

  /**
   * Helper method to set up a data channel and emit appropriate events.
   * 
   * @param channel The RTCDataChannel to be set up.
   */
  #setupDataChannel(channel: RTCDataChannel) {
    const { label = '' } = channel;
    const { channels } = this;

    if (channels.has(label)) {
      channels.get(label)?.close();
    }
    channels.set(label, channel);

    channel.addEventListener('open', () => {
      this.emit(['channel', 'channel:open'], { id: this.id, name: 'channel:open', channel, label });
    });
    channel.addEventListener('close', () => {
      channels.delete(label);
      this.emit(['channel', 'channel:close'], { id: this.id, name: 'channel:close', channel, label });
    });
    channel.addEventListener('message', (e) => {
      this.emit(['channel', 'channel:message'], { id: this.id, name: 'channel:message', channel, label, data: e.data });
    });
    channel.addEventListener('error', (e) => {
      this.emit(['channel', 'channel:error'], { id: this.id, name: 'channel:error', channel, label, error: e.error });
    });

    this.emit(['channel', 'channel:new'], { id: this.id, name: 'channel:new', channel, label });
  }

  /**
   * Helper method to set up a media track by adding it to the corresponding stream 
   * and emitting appropriate events.
   * 
   * @param track Media track to add.
   * @param stream Media stream that contains the track.
   */
  #setupMediaTrack(track: MediaStreamTrack, stream: MediaStream) {
    const label = this.#streamLabels?.get(stream.id) || stream.id;
    const { streams } = this;

    const addTrack = () => {
      if (!streams.has(label)) {
        streams.set(label, stream);
        this.emit(['stream', 'stream:add'], { id: this.id, name: 'stream:add', stream, label });
      }

      this.emit(['track', 'track:add'], { id: this.id, name: 'track:add', track, stream, label });
    };

    const removeTrack = () => {
      stream.removeTrack(track);
      this.emit(['track', 'track:remove'], { id: this.id, name: 'track:remove', track, stream, label });

      if (!stream.active || !stream.getTracks().length) {
        if (streams.has(label)) {
          streams.delete(label);
          this.emit(['stream', 'stream:remove'], { id: this.id, name: 'stream:remove', stream, label });
        }
      }
    };

    track.addEventListener('ended', removeTrack);

    addTrack();
  }

  /**
   * Helper method to set the maximum bitrate for a media track by updating the sender parameters.
   * 
   * @param track Media track for which to set the bitrate.
   * @param bitrate Optional object containing audio and video bitrate settings in bits per second.
   */
  async #setTrackBitrate(track: MediaStreamTrack, bitrate?: { [key: string]: number | undefined; }) {
    const maxBitrate = (bitrate?.[track.kind] || 0) | 0;

    if (!maxBitrate) return;

    const senders = this.connection.getSenders();
    const sender = senders.find((sender: RTCRtpSender) => {
      return sender.track && sender.track.id === track.id;
    });

    if (sender) {
      const params = sender.getParameters() || {};
      if (!params.encodings) params.encodings = [];
      for (const enc of params.encodings) {
        if (enc) enc.maxBitrate = maxBitrate;
      }
      await sender.setParameters(params);
    }
  }
}

/**
 * Options for creating a {@link RemotePeer} instance.
 * 
 * @ignore
 * @group Remote Peers
 */
export interface RemotePeerOptions {
  /** Unique identifier for the remote peer. */
  id: string;
  /** Optional metadata associated with the peer. */
  metadata?: any;
  /** Room name the peer is associated with. */
  room: string;
  /** Indicates if this peer should be polite during negotiation. */
  polite: boolean;
  /** Optional ICE servers for NAT traversal. */
  iceServers?: IceServer[];
  /** Policy for ICE transport. */
  iceTransportPolicy?: IceTransportPolicy;
  /** Timeout in seconds for connection establishment. */
  connectionTimeout?: number;
  /** Map of streams indexed by label. */
  streams: Map<string, StreamOptions>;
  /** Map of data channels indexed by label. */
  channels: Map<string, ChannelOptions>;
}

/**
 * Offer event data.
 * 
 * @ignore
 */
export interface RemotePeerOfferEvent {
  /** Unique identifier for the remote peer. */
  id: string;
  /** Name of the event. */
  name: string;
  /** Session description for the offer. */
  description: RTCSessionDescriptionInit;
  /** Stream labels associated with the offer. */
  labels?: { [key: string]: string; };
}

/**
 * Answer event data.
 * 
 * @ignore
 */
export interface RemotePeerAnswerEvent {
  /** Unique identifier for the remote peer. */
  id: string;
  /** Name of the event. */
  name: string;
  /** Session description for the answer. */
  description: RTCSessionDescriptionInit;
}

/**
 * Candidate event data.
 * 
 * @ignore
 */
export interface RemotePeerCandidateEvent {
  /** Unique identifier for the remote peer. */
  id: string;
  /** Name of the event. */
  name: string;
  /** ICE candidate information. */
  candidate: RTCIceCandidateInit;
}

/**
 * Event emitted on peer connection state changes.
 * 
 * @group Remote Peers
 */
export interface RemotePeerConnectionEvent {
  /** Unique identifier for the remote peer. */
  id: string;
  /** Name of the event. */
  name: 'connection:new' | 'connection:connecting' | 'connection:connected' | 'connection:disconnected' | 'connection:failed' | 'connection:closed';
  /** New connection state. */
  state: PeerConnectionState;
}

/**
 * Channel event data.
 * 
 * @group Remote Peers
 */
export interface RemotePeerChannelEvent {
  /** Unique identifier for the remote peer. */
  id: string;
  /** Name of the event. */
  name: 'channel:new' | 'channel:open' | 'channel:close' | 'channel:message' | 'channel:error';
  /** Data channel associated with the event. */
  channel: RTCDataChannel;
  /** Label of the data channel. */
  label: string;
  /** Data associated with the message event. */
  data?: any;
  /** Error associated with the error event. */
  error?: Error;
}

/**
 * Stream event data.
 * 
 * @group Remote Peers
 */
export interface RemotePeerStreamEvent {
  /** Unique identifier for the remote peer. */
  id: string;
  /** Name of the event. */
  name: 'stream:add' | 'stream:remove';
  /** Media stream associated with the event. */
  stream: MediaStream;
  /** Label of the media stream. */
  label: string;
}

/**
 * Track event data.
 * 
 * @group Remote Peers
 */
export interface RemotePeerTrackEvent {
  /** Unique identifier for the remote peer. */
  id: string;
  /** Name of the event. */
  name: 'track:add' | 'track:remove';
  /** Media track associated with the event. */
  track: MediaStreamTrack;
  /** Media stream associated with the event. */
  stream: MediaStream;
  /** Label of the media stream. */
  label: string;
}

/**
 * Error event data.
 * 
 * @group Remote Peers
 */
export interface RemotePeerErrorEvent {
  /** Unique identifier for the remote peer. */
  id: string;
  /** Name of the event. */
  name: 'error';
  /** Error associated with the event. */
  error: PeerixError;
}

/**
 * Events emitted by {@link RemotePeer} instances.
 * 
 * @group Remote Peers
 */
export interface RemotePeerEvents {
  /** SDP offer received. @ignore */
  'offer': [RemotePeerOfferEvent];
  /** SDP answer received. @ignore */
  'answer': [RemotePeerAnswerEvent];
  /** ICE candidate event. @ignore */
  'candidate': [RemotePeerCandidateEvent];
  /** General connection event. */
  'connection': [RemotePeerConnectionEvent];
  /** New connection established. */
  'connection:new': [RemotePeerConnectionEvent];
  /** Connection is connecting. */
  'connection:connecting': [RemotePeerConnectionEvent];
  /** Connection is fully connected. */
  'connection:connected': [RemotePeerConnectionEvent];
  /** Connection disconnected. */
  'connection:disconnected': [RemotePeerConnectionEvent];
  /** Connection failed to establish. */
  'connection:failed': [RemotePeerConnectionEvent];
  /** Connection closed. */
  'connection:closed': [RemotePeerConnectionEvent];
  /** General data channel event. */
  'channel': [RemotePeerChannelEvent];
  /** New data channel created. */
  'channel:new': [RemotePeerChannelEvent];
  /** Data channel opened. */
  'channel:open': [RemotePeerChannelEvent];
  /** Data channel closed. */
  'channel:close': [RemotePeerChannelEvent];
  /** Data channel message received. */
  'channel:message': [RemotePeerChannelEvent];
  /** Data channel error. */
  'channel:error': [RemotePeerChannelEvent];
  /** General media stream event. */
  'stream': [RemotePeerStreamEvent];
  /** Media stream added. */
  'stream:add': [RemotePeerStreamEvent];
  /** Media stream removed. */
  'stream:remove': [RemotePeerStreamEvent];
  /** General media track event. */
  'track': [RemotePeerTrackEvent];
  /** Media track added. */
  'track:add': [RemotePeerTrackEvent];
  /** Media track removed. */
  'track:remove': [RemotePeerTrackEvent];
  /** Error event. */
  'error': [RemotePeerErrorEvent];
}
