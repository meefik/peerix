import type { Driver } from './drivers/driver.js';
import log from './utils/logger.js';
import { RemotePeer } from './remote.js';
import { MemoryDriver } from './drivers/memory.js';
import { IceCandidateQueue } from './ice.js';
import { PeerixError } from './error.js';
import { base62ToBytes, bytesToBase62, delay } from './utils/helpers.js';
import { EventEmitter } from './utils/emitter.js';
import { compressMessage, decompressMessage } from './utils/compression.js';
import {
  sha256,
  encryptMessage,
  decryptMessage,
  generateKeyPair,
  generateDerivedKey,
  importPublicKey,
  exportPublicKey,
} from './utils/encryption.js';

// All peers without a driver will share the same in-memory signaling bus
const defaultDriver = new MemoryDriver();

// Signal format:
// [type (1 byte), flags (1 byte), publicKey (33 bytes), payload (variable)]
const TYPE_LENGTH = 1;
const FLAGS_LENGTH = 1;
const CRYPTOKEY_LENGTH = 33;
const HEADER_LENGTH = TYPE_LENGTH + FLAGS_LENGTH + CRYPTOKEY_LENGTH;
// Flag bitmasks
const COMPRESSION_ENABLED = 1 << 0;
const ENCRYPTION_ENABLED = 1 << 1;
// Signal types
const SIGNAL_ANNOUNCE = 1;
const SIGNAL_INVOKE = 2;
const SIGNAL_OFFER = 3;
const SIGNAL_ANSWER = 4;
const SIGNAL_CANDIDATE = 5;

// Delay in milliseconds to apply before dispatching ICE candidates
// to batch them and minimize renegotiations
const ICE_CANDIDATE_DEBOUNCE_MS = 50;

/**
 * Manages WebRTC peer connections, signaling, media streams, and data channels.
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
  /** Active remote peers indexed by remote peer id. */
  readonly connections: Map<string, RemotePeer>;
  /** Published local streams indexed by application-level stream label. */
  readonly streams: Map<string, StreamOptions>;
  /** Configured local data channels indexed by channel label. */
  readonly channels: Map<string, ChannelOptions>;
  /** Attachable extensions. */
  readonly addons: Set<any>;

  /** Indicates whether the peer is currently active (joined a room). @readonly */
  active: boolean;
  /** Unique identifier for the local peer. Empty until join() is called. @readonly */
  id: string;
  /** Current room name. Empty until join() is called. @readonly */
  room: string;
  /** Optional metadata announced to other peers in signaling messages. Empty until join() is called. @readonly */
  metadata: any;

  #driver: Driver;
  #iceServers: IceServer[];
  #iceTransportPolicy: IceTransportPolicy;
  #connectionTimeout: number;
  #namespaceHashing: boolean;
  #signalingCompression: boolean;
  #signalingEncryption: boolean;
  #verify?: (options: {
    id: string;
    metadata?: any;
  }) => Promise<boolean> | boolean;
  #emitter: EventEmitter<PeerEvents>;
  #candidateQueue: IceCandidateQueue;
  #keyPair?: CryptoKeyPair;
  #sharedKeys: Map<string, CryptoKey>;
  #signalHandler: (data: number[]) => void;
  #signalActive: () => void;
  #signalError: (err: any) => void;

  /**
   * Creates a new {@link Peer} instance.
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
      driver,
      iceServers = [],
      iceTransportPolicy = 'all',
      connectionTimeout = 15,
      namespaceHashing = true,
      signalingCompression = true,
      signalingEncryption = true,
    } = options || {};

    this.active = false;
    this.id = '';
    this.room = '';
    this.connections = new Map();
    this.streams = new Map();
    this.channels = new Map();
    this.addons = new Set();
    this.#driver = driver || defaultDriver;
    this.#iceServers = iceServers;
    this.#iceTransportPolicy = iceTransportPolicy;
    this.#connectionTimeout = connectionTimeout;
    this.#emitter = new EventEmitter(this);
    this.#candidateQueue = new IceCandidateQueue();
    this.#signalingCompression = signalingCompression;
    this.#namespaceHashing = namespaceHashing;
    this.#signalingEncryption = signalingEncryption;
    this.#sharedKeys = new Map();
    this.#signalHandler = this.#handleSignal.bind(this);
    this.#signalActive = () => {
      if (!this.active) return;
      this.#dispatchSignal({
        type: SIGNAL_ANNOUNCE,
        room: this.room,
      });
    };
    this.#signalError = (err: any) => {
      const error = new PeerixError(err, 'SIGNALING_ERROR');
      this.emit('error', { id: this.id, name: 'error', error });

      log('signal:error', { id: this.id, error });
    };
  }

  /**
   * Joins a room and starts listening for incoming connections.
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

    const {
      room = 'default',
      metadata,
      verify,
    } = typeof options === 'object' ? options : { room: options };

    if (this.#signalingEncryption) {
      this.#keyPair = this.#keyPair || (await generateKeyPair());
      const publicKey = await exportPublicKey(this.#keyPair.publicKey);
      this.id = bytesToBase62(publicKey);
    } else if (!this.id) {
      const randomKey = crypto.getRandomValues(
        new Uint8Array(CRYPTOKEY_LENGTH),
      );
      this.id = bytesToBase62(randomKey);
    }

    this.room = room;
    this.metadata = metadata;
    this.#verify = verify;

    log('peer:join', { id: this.id, room: this.room, metadata: this.metadata });

    await this.#registerSignal();

    await this.#dispatchSignal({
      type: SIGNAL_ANNOUNCE,
      room: this.room,
    });
  }

  /**
   * Leaves the current room and closes all active remote connections.
   *
   * @example
   * ```javascript
   * // leave the current room
   * peer.leave();
   * ```
   */
  async leave() {
    if (!this.active) return;

    log('peer:leave', {
      id: this.id,
      room: this.room,
      metadata: this.metadata,
    });

    await this.#unregisterSignal();

    for (const remote of this.connections.values()) {
      remote.dispose();
    }
    this.connections.clear();

    this.#candidateQueue.clear();
    this.#sharedKeys.clear();

    this.active = false;
  }

  /**
   * Publishes a new media stream or updates an existing one for all remote peers
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
  async publish(
    options: MediaStream | StreamOptions,
  ): Promise<MediaStream | void> {
    if (options instanceof MediaStream) {
      options = { label: options.id, stream: options };
    }
    const { label: rawLabel = 'default', stream, ...opts } = options || {};
    const label = String(rawLabel);

    if (stream instanceof MediaStream === false || !stream.getTracks().length) {
      return;
    }

    const { stream: newStream = new MediaStream(), managed } =
      this.streams.get(label) || {};

    for (const track of newStream.getTracks()) {
      if (!stream.getTracks().find((t) => t.id === track.id)) {
        newStream.removeTrack(track);
        if (managed && track.readyState !== 'ended') {
          track.stop();
        }
      }
    }
    for (const track of stream.getTracks()) {
      if (!newStream.getTracks().find((t) => t.id === track.id)) {
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
   * Stops publishing a previously published media stream with the given label
   * and removes it from all remote peers.
   *
   * If you pass a MediaStream instance directly, it will be unpublished using
   * its id as the label. Otherwise, you can specify the label in the options
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
   * @param options A stream label, MediaStream instance, or an object containing a label.
   * @returns The unpublished MediaStream instance, or undefined.
   */
  async unpublish(
    options: MediaStream | string | { label?: string },
  ): Promise<MediaStream | void> {
    if (options instanceof MediaStream) {
      options = { label: options.id };
    }
    const { label: rawLabel = 'default' } =
      typeof options === 'object' ? options : { label: options };
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
   * Opens a data channel with the given label and options to all remote peers.
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
   * Closes a previously opened data channel with the given label
   * and removes it from all remote peers.
   *
   * @example
   * ```javascript
   * // close the channel with label 'chat'
   * peer.close({ label: 'chat' });
   * ```
   *
   * @param options Channel label or object containing `label`.
   */
  async close(options: string | { label: string }) {
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
   * Sends a message through data channels.
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
  send(message: any, options?: string | { label?: string }) {
    if (!this.active) return;

    const { label: rawLabel } =
      typeof options === 'object' ? options : { label: options };
    const label =
      typeof rawLabel === 'undefined' ? undefined : String(rawLabel);

    log('peer:send', { id: this.id, label, message });

    for (const remote of this.connections.values()) {
      remote.send(message, { label });
    }
  }

  /**
   * Attaches an addon/extension to the peer instance.
   *
   * @param addon Addon instance to attach.
   */
  async attach(addon: any) {
    await addon.attach(this);
    this.addons.add(addon);
  }

  /**
   * Detaches a previously attached addon/extension from the peer instance.
   *
   * @param addon Addon instance to detach.
   */
  async detach(addon: any) {
    await addon.detach(this);
    this.addons.delete(addon);
  }

  /**
   * Subscribes to one or more peer events.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
  on<K extends keyof PeerEvents>(
    event: K | K[],
    handler: (...args: PeerEvents[K]) => void,
  ) {
    this.#emitter.on(event, handler);
  }

  /**
   * Subscribes to an event and auto-unsubscribes after first invocation.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
  once<K extends keyof PeerEvents>(
    event: K | K[],
    handler: (...args: PeerEvents[K]) => void,
  ) {
    this.#emitter.once(event, handler);
  }

  /**
   * Removes a previously registered event listener.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler to remove. If omitted, all handlers for the given event(s) will be removed.
   */
  off<K extends keyof PeerEvents>(
    event: K | K[],
    handler?: (...args: PeerEvents[K]) => void,
  ) {
    this.#emitter.off(event, handler);
  }

  /**
   * Emits one or more events.
   * Usually you would not call this method directly.
   *
   * @param event Event name or list of event names.
   * @param args Event payload.
   */
  emit<K extends keyof PeerEvents>(event: K | K[], ...args: PeerEvents[K]) {
    this.#emitter.emit(event, ...args);
  }

  /**
   * Creates a new RemotePeer instance for an incoming connection or returns an existing one.
   *
   * @param options Options for creating the remote peer connection.
   * @param options.id Remote peer identifier.
   * @param options.metadata Optional metadata announced by the remote peer in signaling messages.
   * @returns The created or existing RemotePeer instance, or void if the connection was rejected.
   */
  async #createRemotePeer(options: { id: string; metadata?: any }) {
    const { id, metadata } = options;

    let remote = this.connections.get(id);
    if (remote && remote.state !== 'closed') return remote;

    // verify the incoming request and reject if verification fails
    if (typeof this.#verify === 'function') {
      const verified = await this.#verify({ id, metadata });
      if (!verified) return;
    }

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

    const iceCandidateQueue: RTCIceCandidateInit[] = [];
    let iceCandidateDebounceTimer: number | undefined;

    remote.on('signal', (e) => {
      const { name, data } = e;
      const types = {
        offer: SIGNAL_OFFER,
        answer: SIGNAL_ANSWER,
        candidate: SIGNAL_CANDIDATE,
      };
      const dispatch = (name: keyof typeof types, message: any[]) =>
        this.#dispatchSignal({
          type: types[name],
          room: this.room,
          to: id,
          message,
        });
      if (name === 'candidate') {
        clearTimeout(iceCandidateDebounceTimer);
        iceCandidateQueue.push(data as RTCIceCandidateInit);
        iceCandidateDebounceTimer = setTimeout(() => {
          dispatch(name, iceCandidateQueue.splice(0, iceCandidateQueue.length));
        }, ICE_CANDIDATE_DEBOUNCE_MS);
      } else if (name === 'offer') {
        dispatch(name, [data, this.metadata]);
      } else if (name === 'answer') {
        dispatch(name, [data]);
      }
    });

    remote.on('connection:failed', () => {
      // try to reconnect
      this.#dispatchSignal({
        type: SIGNAL_INVOKE,
        room: this.room,
        to: id,
        message: [this.metadata],
        encryptionKey: this.#sharedKeys.get(id),
        jitter: 1000,
      });
    });

    remote.on('connection:closed', () => {
      this.connections.delete(id);
      this.#candidateQueue.clear(id);
      this.#sharedKeys.delete(id);
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

    this.emit(['connection', 'connection:new'], {
      id: this.id,
      name: 'connection:new',
      remote,
      state: 'new',
    });

    return remote;
  }

  /**
   * Subscribes to signaling messages for the current room and peer ID.
   */
  async #registerSignal() {
    const namespaces = [[this.room], [this.room, this.id]];
    for (let namespace of namespaces) {
      try {
        if (this.#namespaceHashing) {
          namespace = await Promise.all(namespace.map((part) => sha256(part)));
        }

        log('signal:register', { id: this.id, namespace });

        this.#driver.on('active', this.#signalActive);
        this.#driver.on('error', this.#signalError);
        await this.#driver.subscribe(namespace, this.#signalHandler);
      } catch (err) {
        const error = new PeerixError(err, 'SIGNALING_ERROR');
        this.emit('error', { id: this.id, name: 'error', error });

        log('signal:error', { id: this.id, namespace, error });
      }
    }
  }

  /**
   * Unsubscribes from signaling messages for the current room and peer ID.
   */
  async #unregisterSignal() {
    const namespaces = [[this.room], [this.room, this.id]];
    for (let namespace of namespaces) {
      try {
        if (this.#namespaceHashing) {
          namespace = await Promise.all(namespace.map((part) => sha256(part)));
        }

        log('signal:unregister', { id: this.id, namespace });

        this.#driver.off('active', this.#signalActive);
        this.#driver.off('error', this.#signalError);
        await this.#driver.unsubscribe(namespace, this.#signalHandler);
      } catch (err) {
        const error = new PeerixError(err, 'SIGNALING_ERROR');
        this.emit('error', { id: this.id, name: 'error', error });

        log('signal:error', { id: this.id, namespace, error });
      }
    }
  }

  /**
   * Dispatches a signaling message to the given namespace with optional jitter.
   *
   * @param options Dispatch options for the signaling message.
   * @param options.type The type of the signaling message.
   * @param options.room The room name to dispatch the message to.
   * @param options.to The peer ID to dispatch the message to.
   * @param options.message Signaling message payload.
   * @param options.encryptionKey Optional encryption key used for encrypting the message.
   * @param options.jitter Optional maximum random delay in milliseconds to apply before dispatching the message.
   */
  async #dispatchSignal(options: {
    type: number;
    room: string;
    to?: string;
    message?: any[];
    encryptionKey?: CryptoKey;
    jitter?: number;
  }) {
    if (!this.active || !this.#driver.active) return;

    const { type, room, to, message, jitter = 0, encryptionKey } = options;
    let namespace = to ? [room, to] : [room];

    try {
      if (this.#namespaceHashing) {
        namespace = await Promise.all(namespace.map((part) => sha256(part)));
      }

      const buffer = await this.#encodeSignal(
        type,
        message,
        encryptionKey || to,
      );

      log('signal:dispatch', { id: this.id, type, namespace, message });

      await delay(Math.random() * jitter);

      await this.#driver.dispatch(namespace, Array.from(buffer));
    } catch (err) {
      const error = new PeerixError(err, 'SIGNALING_ERROR');
      this.emit('error', { id: this.id, name: 'error', error });

      log('signal:error', { id: this.id, namespace, error });
    }
  }

  /**
   * Handles an incoming message dispatched by the driver.
   *
   * Processes signaling message to establish, negotiate, and tear down
   * peer connections.
   *
   * @param data The signaling message data.
   */
  async #handleSignal(data: number[]) {
    if (!this.active) return;

    try {
      const [type, id, message] = await this.#decodeSignal(data);
      if (!id) return;

      log('signal:receive', { id: this.id, type, from: id, message });

      // handle new peer announcement
      if (type === SIGNAL_ANNOUNCE) {
        this.#dispatchSignal({
          type: SIGNAL_INVOKE,
          room: this.room,
          to: id,
          message: [this.metadata],
        });

        return;
      }

      // handle incoming connection
      if (type === SIGNAL_INVOKE) {
        const [metadata] = message;
        await this.#createRemotePeer({ id, metadata });

        return;
      }

      // set remote description for offer and create answer
      if (type === SIGNAL_OFFER) {
        const [description, metadata] = message;
        const remote = await this.#createRemotePeer({ id, metadata });
        if (!remote) return;

        await remote.signal(description);

        for (const candidate of this.#candidateQueue.pull(id, description)) {
          await remote.signal(candidate);
        }

        return;
      }

      // set remote description for answer
      if (type === SIGNAL_ANSWER) {
        const [description] = message;
        const remote = this.connections.get(id);
        if (!remote) return;

        await remote.signal(description);

        for (const candidate of this.#candidateQueue.pull(id, description)) {
          await remote.signal(candidate);
        }

        return;
      }

      // add ice candidate
      if (type === SIGNAL_CANDIDATE) {
        const [...candidates] = message;
        const remote = this.connections.get(id);

        const { connection } = remote || {};
        const description = connection?.remoteDescription || undefined;

        for (const candidate of candidates) {
          const queued = this.#candidateQueue.push(id, candidate, description);
          if (!remote || queued) continue;

          await remote.signal(candidate);
        }

        return;
      }
    } catch (err) {
      const error = new PeerixError(err, 'SIGNALING_ERROR');
      this.emit('error', { id: this.id, name: 'error', error });

      log('peer:error', { id: this.id, error });
    }
  }

  /**
   * Encodes a signaling message with the given type and payload, applying optional
   * compression and encryption.
   *
   * @param type The type of the signaling message.
   * @param message The signaling message payload.
   * @param encryptionKey Optional encryption key or peer ID used for encrypting the message.
   * @returns The encoded signaling message.
   */
  async #encodeSignal(
    type: number,
    message: any,
    encryptionKey?: CryptoKey | string,
  ) {
    let flags = 0;
    let payload: Uint8Array = message
      ? new TextEncoder().encode(JSON.stringify(message))
      : new Uint8Array();

    if (payload.byteLength > 0) {
      if (this.#signalingCompression) {
        const compressed = await compressMessage(payload);
        if (compressed.byteLength < payload.byteLength) {
          payload = compressed;
          flags |= COMPRESSION_ENABLED;
        }
      }

      if (this.#signalingEncryption && encryptionKey) {
        if (typeof encryptionKey === 'string') {
          encryptionKey = this.#sharedKeys.get(encryptionKey);
        }
        if (!encryptionKey) throw new Error('Encryption key not found');
        payload = await encryptMessage(payload, encryptionKey);
        flags |= ENCRYPTION_ENABLED;
      }
    }

    const publicKey = base62ToBytes(this.id);

    const buffer = new Uint8Array(HEADER_LENGTH + payload.byteLength);
    let offset = 0;
    buffer.set([type], offset);
    offset += TYPE_LENGTH;
    buffer.set([flags], offset);
    offset += FLAGS_LENGTH;
    buffer.set(publicKey, offset);
    offset += CRYPTOKEY_LENGTH;
    buffer.set(payload, offset);

    return buffer;
  }

  /**
   * Decodes an incoming signaling message and returns its components.
   *
   * @param data The signaling message data as an array of numbers.
   * @returns An array containing the message type, sender ID, and payload.
   */
  async #decodeSignal(data: number[]) {
    const buffer = new Uint8Array(data);
    if (buffer.byteLength < HEADER_LENGTH) return [];

    let offset = 0;
    const type = buffer[offset];
    offset += TYPE_LENGTH;
    const flags = buffer[offset];
    offset += FLAGS_LENGTH;
    const publicKey = buffer.slice(offset, offset + CRYPTOKEY_LENGTH);
    offset += CRYPTOKEY_LENGTH;

    const id = bytesToBase62(publicKey);
    if (!id || this.id === id) return [];

    let encryptionKey = this.#sharedKeys.get(id);
    if (!encryptionKey && this.#signalingEncryption) {
      const rawPublicKey = await importPublicKey(publicKey);
      encryptionKey = await generateDerivedKey(
        this.#keyPair!.privateKey,
        rawPublicKey,
      );
      this.#sharedKeys.set(id, encryptionKey);
    }

    let payload: Uint8Array = buffer.slice(offset);
    if (payload.byteLength > 0) {
      if (encryptionKey) {
        payload = await decryptMessage(payload, encryptionKey);
      }

      if (flags & COMPRESSION_ENABLED) {
        payload = await decompressMessage(payload);
      }
    }

    const message = payload.byteLength
      ? JSON.parse(new TextDecoder().decode(payload))
      : [];

    return [type, id, message];
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
export type PeerConnectionState =
  | 'new'
  | 'connecting'
  | 'connected'
  | 'disconnected'
  | 'failed'
  | 'closed';

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
   * Preferred audio encoding parameters to apply to the stream's audio tracks, such as bitrate or priority.
   */
  audioParameters?: {
    /** Preferred maximum bitrate in bits per second to encode the audio tracks. */
    maxBitrate?: number;
    /** Preferred priority for encoding the audio tracks. */
    priority?: RTCPriorityType;
  };
  /**
   * Preferred video encoding parameters to apply to the stream's video tracks, such as bitrate, frame rate, or priority.
   */
  videoParameters?: {
    /** Preferred maximum bitrate in bits per second to encode the video tracks. */
    maxBitrate?: number;
    /** Preferred maximum frame rate to encode the video tracks. */
    maxFramerate?: number;
    /** Preferred priority for encoding the video tracks. */
    priority?: RTCPriorityType;
    /** Preferred scale factor to downscale the video resolution. */
    scaleResolutionDownBy?: number;
  };
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
   * Enable hashing of namespaces in signaling messages for privacy.
   * This also helps to avoid unsupported characters in room names
   * and peer identifiers.
   * Enabled by default.
   */
  namespaceHashing?: boolean;
  /**
   * Enable compression for signaling messages to reduce bandwidth usage
   * by about 30%.
   * Enabled by default.
   */
  signalingCompression?: boolean;
  /**
   * Encrypt signaling messages with AES-GCM for end-to-end security.
   * Enabled by default.
   */
  signalingEncryption?: boolean;
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
  verify?: (options: {
    id: string;
    metadata?: any;
  }) => Promise<boolean> | boolean;
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
  name:
    | 'connection:new'
    | 'connection:connecting'
    | 'connection:connected'
    | 'connection:disconnected'
    | 'connection:failed'
    | 'connection:closed';
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
  name:
    | 'channel:new'
    | 'channel:open'
    | 'channel:close'
    | 'channel:message'
    | 'channel:error';
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
  connection: [PeerConnectionEvent];
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
  error: [PeerErrorEvent];
  /** Emitted when stream events occur. */
  stream: [PeerStreamEvent];
  /** Emitted when a remote peer publishes a media stream. */
  'stream:add': [PeerStreamEvent];
  /** Emitted when a remote peer unpublishes a media stream. */
  'stream:remove': [PeerStreamEvent];
  /** Emitted when track events occur. */
  track: [PeerTrackEvent];
  /** Emitted when a remote peer adds a media track to a published stream. */
  'track:add': [PeerTrackEvent];
  /** Emitted when a remote peer removes a media track from a published stream. */
  'track:remove': [PeerTrackEvent];
  /** Emitted when channel events occur. */
  channel: [PeerChannelEvent];
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
