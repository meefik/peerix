import type { Driver } from "./drivers/driver.js";
import log from "./utils/logger.js";
import { RemotePeer } from "./remote.js";
import { MemoryDriver } from "./drivers/memory.js";
import { PeerixError } from "./error.js";
import { parseOptions } from "./utils/helpers.js";
import { teeStream } from "./utils/stream.js";
import { EventEmitter } from "./utils/emitter.js";
import { Signaler } from "./signaler.js";
import { Addon } from "./addons/addon.js";

// All peers without a driver will share the same in-memory signaling bus
const defaultDriver = new MemoryDriver();

/**
 * Manages WebRTC peer connections, media streams, and data channels.
 *
 * @group Peers
 * @example
 * ```javascript
 * // create a new peer
 * // using default in-memory signaling driver
 * const peer = new Peer();
 *
 * // listen for connection state changes
 * peer.on("connection", (e) => {
 *   const { remote, state } = e;
 *   console.log(`Peer "${remote.id}" state changed to "${state}"`);
 * });
 *
 * // listen for open channel event
 * peer.on("channel:open", (e) => {
 *   const { remote, label } = e;
 *   console.log(`Channel "${label}" opened with peer "${remote.id}"`);
 *   // send a message to the remote peer
 *   remote.send("Hello, peer!", { label });
 * });
 *
 * // listen for incoming messages
 * peer.on("channel:message", (e) => {
 *   const { remote, data, label } = e;
 *   console.log(`Message from peer "${remote.id}" on channel "${label}":`, data);
 * });
 *
 * // open a data channel
 * peer.open({ label: "default" });
 *
 * // join a room
 * peer.join({ room: "room-id" });
 * ```
 */
export class Peer {
  /** Indicates whether the peer is currently active (joined a room). */
  get active(): boolean {
    return this.#active;
  }
  /** Unique identifier for the local peer. Empty until join() is called. */
  get id(): string {
    return this.#id;
  }
  /** Current room name. Empty until join() is called. */
  get room(): string {
    return this.#room;
  }
  /** Optional metadata announced to other peers in signaling messages. Undefined until join() is called. */
  get metadata(): unknown {
    return this.#metadata;
  }
  /** Active remote peers indexed by remote peer id. */
  get connections(): Map<string, RemotePeer> {
    return this.#connections;
  }
  /** Configured local streams indexed by application-level stream label. */
  get streams(): Map<string, StreamOptions> {
    return this.#streamOptions;
  }
  /** Configured local data channels indexed by channel label. */
  get channels(): Map<string, ChannelOptions> {
    return this.#channelOptions;
  }
  /** Attachable extensions. */
  get addons(): Set<Addon> {
    return this.#addons;
  }

  #active: boolean;
  #id: string;
  #room: string;
  #metadata: unknown;
  #driver: Driver;
  #connections: Map<string, RemotePeer>;
  #streamOptions: Map<string, StreamOptions>;
  #channelOptions: Map<string, ChannelOptions>;
  #addons: Set<Addon>;
  #iceServers: IceServer[];
  #iceTransportPolicy: IceTransportPolicy;
  #iceCandidateDebounce: number;
  #connectionTimeout: number;
  #verify?: (options: {
    id: string;
    metadata?: unknown;
  }) => Promise<boolean> | boolean;
  #emitter: EventEmitter<PeerEvents>;
  #signaler: Signaler;

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
      iceTransportPolicy = "all",
      iceCandidateDebounce = 50,
      connectionTimeout = 15,
      namespaceHashing = true,
      signalingCompression = true,
      signalingEncryption = true,
    } = options || {};

    this.#active = false;
    this.#id = "";
    this.#room = "";
    this.#metadata = undefined;
    this.#driver = driver || defaultDriver;
    this.#connections = new Map();
    this.#streamOptions = new Map();
    this.#channelOptions = new Map();
    this.#addons = new Set();
    this.#iceServers = iceServers;
    this.#iceTransportPolicy = iceTransportPolicy;
    this.#iceCandidateDebounce = iceCandidateDebounce;
    this.#connectionTimeout = connectionTimeout;
    this.#emitter = new EventEmitter(this);
    this.#signaler = new Signaler({
      driver: this.#driver,
      namespaceHashing: namespaceHashing,
      signalingCompression: signalingCompression,
      signalingEncryption: signalingEncryption,
      iceCandidateDebounce: this.#iceCandidateDebounce,
      createRemotePeer: (options) => this.#createRemotePeer(options),
      getRemotePeer: (id) => this.#getRemotePeer(id),
      onError: (error) => {
        this.emit("error", { id: this.#id, name: "error", error });
      },
    });
  }

  /**
   * Joins a room and starts listening for incoming connections.
   *
   * @example
   * ```javascript
   * // join a room with ID "room-id" and custom metadata
   * peer.join({ room: "room-id", metadata: { name: "Alice" } });
   * ```
   *
   * @param options Room name or join options.
   */
  async join(options?: string | PeerJoinOptions): Promise<void> {
    if (this.#active) return;

    const { room, metadata, verify } = parseOptions<PeerJoinOptions>(
      options,
      (value) => {
        return { room: String(value) };
      },
    );

    this.#active = true;

    try {
      this.#room = `${room || "default"}`;
      this.#metadata = metadata;
      this.#verify = verify;

      this.#id = await this.#signaler.register(this.#room, this.#metadata);

      log("peer:join", {
        id: this.#id,
        room: this.#room,
        metadata: this.#metadata,
      });
    } catch (err) {
      await this.leave();
      throw err;
    }
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
  async leave(): Promise<void> {
    if (!this.#active) return;

    log("peer:leave", {
      id: this.#id,
      room: this.#room,
      metadata: this.#metadata,
    });

    await this.#signaler.unregister();

    for (const remote of this.#connections.values()) {
      remote.dispose();
    }
    this.#connections.clear();

    this.#id = "";
    this.#room = "";
    this.#metadata = undefined;
    this.#active = false;
  }

  /**
   * Shares a new media stream or updates an existing one for all remote peers
   * including new ones that join later.
   *
   * If you pass a MediaStream instance directly, it will be shared under
   * a label equal to the stream id. Otherwise, you can specify an explicit
   * label in the options object. If a stream with the same label already
   * exists, it will be updated and its tracks will be added/removed as needed
   * to minimize renegotiations.
   *
   * If the stream is shared with the `managed` option, its tracks will be
   * automatically stopped when the stream is unshared or replaced with
   * a new stream.
   *
   * @example
   * ```javascript
   * // get a media stream from the user's camera and microphone
   * const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
   *
   * // share a media stream with an explicit label
   * peer.share({ label: "camera", stream, managed: true });
   * ```
   *
   * @param options Stream descriptor or MediaStream instance.
   * @returns The shared MediaStream instance if successful, or undefined.
   */
  async share(
    options: MediaStream | StreamOptions,
  ): Promise<MediaStream | void> {
    if (options instanceof MediaStream) {
      options = { label: options.id, stream: options };
    }

    const {
      label = "default",
      stream,
      ...opts
    } = parseOptions<StreamOptions>(options);

    if (!(stream instanceof MediaStream) || !stream.getTracks().length) {
      return;
    }

    const { stream: newStream = new MediaStream(), managed } =
      this.#streamOptions.get(label) || {};

    const incomingTracks = stream.getTracks();
    const currentTracks = newStream.getTracks();
    const incomingTrackIds = new Set(incomingTracks.map((track) => track.id));
    const currentTrackIds = new Set(currentTracks.map((track) => track.id));

    for (const track of currentTracks) {
      if (!incomingTrackIds.has(track.id)) {
        newStream.removeTrack(track);
        if (managed && track.readyState !== "ended") {
          track.stop();
        }
      }
    }
    for (const track of incomingTracks) {
      if (!currentTrackIds.has(track.id)) {
        newStream.addTrack(track);
      }
    }

    const newStreamOptions = { label, stream: newStream, ...opts };

    log("peer:share", { id: this.#id, ...newStreamOptions });

    this.#streamOptions.set(label, newStreamOptions);

    await Promise.allSettled(
      Array.from(this.#connections.values()).map((remote) =>
        remote.share(newStreamOptions),
      ),
    );

    return newStream;
  }

  /**
   * Stops sharing a previously shared media stream with the given label
   * and removes it from all remote peers.
   *
   * If you pass a MediaStream instance directly, it will be unshared using
   * its id as the label. Otherwise, you can specify the label in the options
   * object or pass it directly as a string.
   *
   * If the stream was shared with the `managed` option, its tracks will be
   * stopped automatically.
   *
   * @example
   * ```javascript
   * // unshare a media stream with an explicit label
   * peer.unshare({ label: "camera" });
   * ```
   *
   * @param options A stream label, MediaStream instance, or an object containing a label.
   * @returns The unshared MediaStream instance, or undefined.
   */
  async unshare(
    options: MediaStream | string | { label?: string },
  ): Promise<MediaStream | void> {
    if (options instanceof MediaStream) {
      options = { label: options.id };
    }

    const { label = "default" } = parseOptions(options, (value) => {
      return { label: String(value) };
    });

    const oldStreamOptions = this.#streamOptions.get(label);
    const { stream, managed } = oldStreamOptions || {};

    log("peer:unshare", { id: this.#id, label, stream });

    this.#streamOptions.delete(label);

    if (!stream) return;

    if (managed) {
      for (const track of stream.getTracks()) {
        if (track.readyState !== "ended") {
          track.stop();
        }
      }
    }

    await Promise.allSettled(
      Array.from(this.#connections.values()).map((remote) =>
        remote.unshare({ label }),
      ),
    );

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
   * // open a channel with label "chat"
   * peer.open({ label: "chat" });
   * ```
   *
   * @param options Channel options or channel label.
   */
  async open(options: string | ChannelOptions): Promise<void> {
    const { label = "default", ...channelOptions } =
      parseOptions<ChannelOptions>(options, (value) => {
        return { label: String(value) };
      });

    log("peer:open", { id: this.#id, label, ...channelOptions });

    this.#channelOptions.set(label, { label, ...channelOptions });

    await Promise.allSettled(
      Array.from(this.#connections.values()).map((remote) =>
        remote.open({ label, ...channelOptions }),
      ),
    );
  }

  /**
   * Closes a previously opened data channel with the given label
   * and removes it from all remote peers.
   *
   * @example
   * ```javascript
   * // close the channel with label "chat"
   * peer.close({ label: "chat" });
   * ```
   *
   * @param options Channel label or object containing `label`.
   */
  async close(options: string | { label: string }): Promise<void> {
    const { label = "default" } = parseOptions(options, (value) => {
      return { label: String(value) };
    });

    log("peer:close", { id: this.#id, label });

    this.#channelOptions.delete(label);

    await Promise.allSettled(
      Array.from(this.#connections.values()).map((remote) =>
        remote.close({ label }),
      ),
    );
  }

  /**
   * Sends a message through data channels to all connected remote peers.
   *
   * If `options` is a string, it is treated as the channel label. If a label
   * is not provided, it uses the `default` channel.
   *
   * The `send` method works only with open channels that have no protocol, are
   * ordered (reliable), and match the specified label.
   *
   * @example
   * ```javascript
   * // send a message to default channel
   * peer.send("Hello, all peers!");
   * // send a message to a specific channel
   * peer.send("Hello, chat channel!", { label: "chat" });
   * ```
   *
   * @param message Message payload to send.
   * @param options Send options or channel label.
   * @returns A list of ReadableStream of transfer progress status for each connection.
   */
  send(
    message: unknown,
    options?: string | SendOptions,
  ): ReadableStream<TransferProgress>[] {
    if (!this.#active) return [];

    const { label = "default", info } = parseOptions<SendOptions>(
      options,
      (value) => {
        return { label: String(value) };
      },
    );

    const numConnections = this.#connections.size;
    if (!numConnections) return [];

    log("peer:send", { id: this.#id, label, info, message });

    let streams: ReadableStream[] | undefined;
    if (message instanceof ReadableStream) {
      streams = teeStream(message, numConnections);
    }

    const results: ReadableStream<TransferProgress>[] = [];
    for (const [index, remote] of Array.from(
      this.#connections.values(),
    ).entries()) {
      const data = streams ? streams[index] : message;
      const progress = remote.send(data, { label, info });
      results.push(progress);
    }

    return results;
  }

  /**
   * Attaches an addon/extension to the peer instance.
   *
   * @param addon Addon instance to attach.
   */
  async attach(addon: Addon): Promise<void> {
    await addon.attach(this);
    this.#addons.add(addon);
  }

  /**
   * Detaches a previously attached addon/extension from the peer instance.
   *
   * @param addon Addon instance to detach.
   */
  async detach(addon: Addon): Promise<void> {
    await addon.detach(this);
    this.#addons.delete(addon);
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
  ): void {
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
  ): void {
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
  ): void {
    this.#emitter.off(event, handler);
  }

  /**
   * Emits one or more events.
   * Usually you would not call this method directly.
   *
   * @param event Event name or list of event names.
   * @param args Event payload.
   */
  emit<K extends keyof PeerEvents>(
    event: K | K[],
    ...args: PeerEvents[K]
  ): void {
    this.#emitter.emit(event, ...args);
  }

  /**
   * Serializes the peer to a JSON-compatible object.
   *
   * @returns A serializable representation of the peer.
   */
  toJSON() {
    return {
      id: this.id,
      room: this.room,
      metadata: this.metadata,
      active: this.active,
      connections: Array.from(this.#connections.keys()),
      streams: Array.from(this.#streamOptions.keys()),
      channels: Array.from(this.#channelOptions.keys()),
    };
  }

  /**
   * Creates a new remote peer connection by verifying its existence, validating the peer,
   * binding lifecycle handlers, and registering it in the connections map.
   */
  async #createRemotePeer(options: {
    id: string;
    metadata?: unknown;
  }): Promise<RemotePeer | void> {
    const { id, metadata } = options;

    const existingRemote = this.#getRemotePeer(id);
    if (existingRemote) return;

    const verified = await this.#verifyRemotePeer({ id, metadata });
    if (!verified) return;

    const remote = this.#newRemotePeer({ id, metadata });
    this.#bindRemoteLifecycleHandlers(remote);
    this.#registerRemotePeer(remote);

    return remote;
  }

  /**
   * Retrieves an existing remote peer by ID, excluding peers in a closed state.
   */
  #getRemotePeer(id: string): RemotePeer | void {
    const remote = this.#connections.get(id);
    return remote && remote.state !== "closed" ? remote : undefined;
  }

  /**
   * Verifies a remote peer using the optional verify function provided in the constructor.
   * Returns true if no verify function is set or if the verification passes.
   */
  async #verifyRemotePeer(options: {
    id: string;
    metadata?: unknown;
  }): Promise<boolean> {
    if (typeof this.#verify !== "function") return true;
    return await this.#verify(options);
  }

  /**
   * Instantiates a new RemotePeer with the current peer's configuration settings.
   */
  #newRemotePeer(options: { id: string; metadata?: unknown }): RemotePeer {
    const { id, metadata } = options;

    return new RemotePeer({
      id,
      metadata,
      room: this.#room,
      polite: this.#id > id,
      iceServers: this.#iceServers,
      iceTransportPolicy: this.#iceTransportPolicy,
      connectionTimeout: this.#connectionTimeout,
      streams: this.#streamOptions,
      channels: this.#channelOptions,
    });
  }

  /**
   * Attaches event listeners to a remote peer for propagating connection, channel,
   * stream, track, and error events through the main peer's emitter.
   */
  #bindRemoteLifecycleHandlers(remote: RemotePeer): void {
    remote.on("connection:closed", () => {
      this.#connections.delete(remote.id);
    });

    remote.on("connection", (e) => {
      this.emit(["connection", e.name], { ...e, id: this.#id, remote });
    });

    remote.on("channel", (e) => {
      this.emit(["channel", e.name], { ...e, id: this.#id, remote });
    });

    remote.on("stream", (e) => {
      this.emit(["stream", e.name], { ...e, id: this.#id, remote });
    });

    remote.on("track", (e) => {
      this.emit(["track", e.name], { ...e, id: this.#id, remote });
    });

    remote.on("error", (e) => {
      this.emit("error", { ...e, id: this.#id });
    });
  }

  /**
   * Registers a remote peer in the connections map and emits a connection:new event.
   */
  #registerRemotePeer(remote: RemotePeer): void {
    this.#connections.set(remote.id, remote);

    this.emit(["connection", "connection:new"], {
      id: this.#id,
      name: "connection:new",
      remote,
      state: "new",
    });
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
export type IceTransportPolicy = "all" | "relay";

/**
 * Peer connection state.
 *
 * @group Peers
 */
export type PeerConnectionState =
  | "new"
  | "connecting"
  | "connected"
  | "disconnected"
  | "failed"
  | "closed";

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
   * Media stream to share.
   */
  stream: MediaStream;
  /**
   * Whether the peer should manage the lifecycle of the stream's tracks.
   * If true, tracks will be stopped when the stream is unshared or replaced.
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
   * Optional subprotocol name.
   */
  protocol?: string;
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
}

/**
 * Options for sending a message through a data channel.
 *
 * @group Streams and Channels
 */
export interface SendOptions {
  /** Channel label. If omitted, `default` is used. */
  label?: string;
  /** Optional additional information to send with the message. */
  info?: Record<string, unknown>;
}

/**
 * Progress information for a transfer operation on a channel.
 *
 * @group Streams and Channels
 */
export interface TransferProgress {
  /** Peer ID. */
  id: string;
  /** Channel label. */
  label: string;
  /** Number of bytes transferred so far. */
  current: number;
  /** Total number of bytes to transfer. It could be -1 if it is unknown. */
  total: number;
  /** Whether the transfer is done. */
  done: boolean;
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
   *   urls: "stun:stun.l.google.com:19302"
   * }]
   * ```
   */
  iceServers?: IceServer[];
  /**
   * ICE policy used by created RTCPeerConnection instances.
   * If set to "relay", only relay candidates will be used,
   * otherwise all candidates will be considered.
   */
  iceTransportPolicy?: IceTransportPolicy;
  /**
   * Debounce time in milliseconds for batching ICE candidates before sending
   * them through signaling to minimize the number of messages.
   * By default, it is set to 50 ms.
   */
  iceCandidateDebounce?: number;
  /**
   * Connection timeout in seconds.
   * By default, it is set to 15 seconds. Use 0 to disable the timeout.
   */
  connectionTimeout?: number;
  /**
   * Enable hashing of namespaces in signaling messages for privacy.
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
   * It is recommended to use `a-zA-Z0-9_-` characters only.
   * If omitted, the peer will join a room with the `default` name.
   */
  room?: string;
  /**
   * Optional metadata to advertise to remote peers.
   */
  metadata?: unknown;
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
    metadata?: unknown;
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
    | "connection:new"
    | "connection:connecting"
    | "connection:connected"
    | "connection:disconnected"
    | "connection:failed"
    | "connection:closed";
  /** Remote peer object containing connection details. */
  remote: RemotePeer;
  /** New connection state. */
  state: PeerConnectionState;
}

/**
 * Emitted when a data channel is created or received from a remote peer,
 * when a data channel is opened or closed, when a message is received on a
 * data channel, or when an error occurs.
 *
 * @group Peers
 */
export interface PeerChannelEvent {
  /** Local peer identifier. */
  id: string;
  /** Name of the event. */
  name:
    | "channel:new"
    | "channel:open"
    | "channel:close"
    | "channel:message"
    | "channel:error";
  /** Remote peer object containing connection details. */
  remote: RemotePeer;
  /** Data channel associated with the event. */
  channel: RTCDataChannel;
  /** Label of the data channel. */
  label: string;
  /** Optional additional information associated with the message event. */
  info?: Record<string, unknown>;
  /** Received message data for message events. */
  data?: unknown;
  /** Error object containing details about the error for error events. */
  error?: Error;
}

/**
 * Emitted when a remote peer shares or unshares a media stream.
 *
 * @group Peers
 */
export interface PeerStreamEvent {
  /** Local peer identifier. */
  id: string;
  /** Name of the event. */
  name: "stream:add" | "stream:remove";
  /** Remote peer object containing connection details. */
  remote: RemotePeer;
  /** Media stream associated with the event. */
  stream: MediaStream;
  /** Label of the media stream. */
  label: string;
}

/**
 * Emitted when a remote peer adds a media track to or removes one from a shared stream.
 *
 * @group Peers
 */
export interface PeerTrackEvent {
  /** Local peer identifier. */
  id: string;
  /** Name of the event. */
  name: "track:add" | "track:remove";
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
  name: "error";
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
  "connection:new": [PeerConnectionEvent];
  /** Emitted when a remote peer connection is connecting. */
  "connection:connecting": [PeerConnectionEvent];
  /** Emitted when a remote peer connection is successfully connected. */
  "connection:connected": [PeerConnectionEvent];
  /** Emitted when a remote peer connection is disconnected. */
  "connection:disconnected": [PeerConnectionEvent];
  /** Emitted when a remote peer connection fails. */
  "connection:failed": [PeerConnectionEvent];
  /** Emitted when a remote peer connection is closed. */
  "connection:closed": [PeerConnectionEvent];
  /** Emitted when an error occurs in any background operations. */
  error: [PeerErrorEvent];
  /** Emitted when stream events occur. */
  stream: [PeerStreamEvent];
  /** Emitted when a remote peer shares a media stream. */
  "stream:add": [PeerStreamEvent];
  /** Emitted when a remote peer unshares a media stream. */
  "stream:remove": [PeerStreamEvent];
  /** Emitted when track events occur. */
  track: [PeerTrackEvent];
  /** Emitted when a remote peer adds a media track to a shared stream. */
  "track:add": [PeerTrackEvent];
  /** Emitted when a remote peer removes a media track from a shared stream. */
  "track:remove": [PeerTrackEvent];
  /** Emitted when channel events occur. */
  channel: [PeerChannelEvent];
  /** Emitted when a data channel is created or received from a remote peer. */
  "channel:new": [PeerChannelEvent];
  /** Emitted when a data channel is opened. */
  "channel:open": [PeerChannelEvent];
  /** Emitted when a data channel is closed. */
  "channel:close": [PeerChannelEvent];
  /** Emitted when a message is received on a data channel. */
  "channel:message": [PeerChannelEvent];
  /** Emitted when an error occurs with a remote peer connection or channel. */
  "channel:error": [PeerChannelEvent];
}
