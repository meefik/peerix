import type { Driver } from "./drivers/driver.js";
import { RemotePeer } from "./remote.js";
import { MemoryDriver } from "./drivers/memory.js";
import { PeerixError } from "./error.js";
import { parseOptions } from "./utils/helpers.js";
import {
  teeStream,
  PromiseLikeReadableStream,
  mergeStreams,
} from "./utils/stream.js";
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
 * ```js
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
 * peer.on("channel:message", async (e) => {
 *   const { remote, data, label } = e;
 *   const message = await data;
 *   console.log(`Message from peer "${remote.id}" on channel "${label}":`, message);
 * });
 *
 * // open a data channel
 * await peer.open({ label: "default" });
 *
 * // join a room
 * await peer.join({ room: "room-id" });
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
  get metadata(): Record<string, unknown> | undefined {
    return this.#metadata;
  }
  /** Active remote peers indexed by remote peer id. */
  get connections(): ReadonlyMap<string, RemotePeer> {
    return this.#connections;
  }
  /** Configured local streams indexed by application-level stream label. */
  get streams(): ReadonlyMap<string, StreamOptions> {
    return this.#streamOptions;
  }
  /** Configured local data channels indexed by channel label. */
  get channels(): ReadonlyMap<string, ChannelOptions> {
    return this.#channelOptions;
  }
  /** Attachable extensions. */
  get addons(): ReadonlySet<Addon> {
    return this.#addons;
  }

  #active: boolean;
  #id: string;
  #room: string;
  #metadata?: Record<string, unknown>;
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
    metadata?: Record<string, unknown>;
  }) => Promise<boolean> | boolean;
  #emitter: EventEmitter<PeerEvents>;
  #signaler: Signaler;

  /**
   * Creates a new {@link Peer} instance.
   *
   * @example
   * ```js
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
    } = options ?? {};

    this.#active = false;
    this.#id = "";
    this.#room = "";
    this.#metadata = undefined;
    this.#driver = driver ?? defaultDriver;
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
        this.emit("error", { name: "error", error });
      },
    });
  }

  /**
   * Joins a room and starts listening for incoming connections.
   *
   * If the peer is already active, this method returns immediately without error.
   *
   * @example
   * ```js
   * // join a room with ID "room-id" and custom metadata
   * await peer.join({ room: "room-id", metadata: { name: "Alice" } });
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

      this.emit(["local", "local:join"], {
        name: "local:join",
        room: this.#room,
        metadata: this.#metadata,
      });

      this.#id = await this.#signaler.register(this.#room, this.#metadata);
    } catch (err) {
      await this.leave();
      throw err;
    }
  }

  /**
   * Leaves the current room and closes all active remote connections.
   *
   * If the peer is not currently active, this method returns immediately without error.
   *
   * @example
   * ```js
   * // leave the current room
   * await peer.leave();
   * ```
   */
  async leave(): Promise<void> {
    if (!this.#active) return;

    this.emit(["local", "local:leave"], {
      name: "local:leave",
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
   * @example
   * ```js
   * // get a media stream from the user's camera and microphone
   * const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
   *
   * // share a media stream with an explicit label
   * await peer.share({ label: "camera", stream });
   * ```
   *
   * @param options Stream descriptor or MediaStream instance.
   */
  async share(options: MediaStream | StreamOptions): Promise<void> {
    const {
      label = "default",
      stream,
      ...opts
    } = parseOptions<StreamOptions>(
      options instanceof MediaStream
        ? { label: options.id, stream: options }
        : options,
    );

    if (!(stream instanceof MediaStream) || !stream.getTracks().length) {
      throw new Error("MediaStream is invalid or empty");
    }

    const { stream: newStream = new MediaStream(), managed } =
      this.#streamOptions.get(label) ?? {};

    const incomingTracks = stream.getTracks();
    const currentTracks = newStream.getTracks();
    const incomingTrackIds = new Set(incomingTracks.map((track) => track.id));
    const currentTrackIds = new Set(currentTracks.map((track) => track.id));

    for (const track of currentTracks) {
      if (!incomingTrackIds.has(track.id)) {
        newStream.removeTrack(track);
        if (!managed && track.readyState !== "ended") track.stop();
      }
    }
    for (const track of incomingTracks) {
      if (!currentTrackIds.has(track.id)) {
        newStream.addTrack(track);
      }
    }

    const newStreamOptions = { ...opts, label, stream: newStream };
    this.#streamOptions.set(label, newStreamOptions);

    this.emit(["local", "local:share"], {
      name: "local:share",
      stream: newStream,
      label,
    });

    await Promise.allSettled(
      Array.from(this.#connections.values()).map((remote) =>
        remote.share(newStreamOptions),
      ),
    );
  }

  /**
   * Stops sharing a previously shared media stream with the given label
   * and removes it from all remote peers.
   *
   * If you pass a MediaStream instance directly, it will be unshared using
   * its id as the label. Otherwise, you can specify the label in the options
   * object or pass it directly as a string.
   *
   * @example
   * ```js
   * // unshare a media stream with an explicit label
   * await peer.unshare({ label: "camera" });
   * ```
   *
   * @param options A stream label, MediaStream instance, or an object containing a label.
   */
  async unshare(
    options: MediaStream | string | { label?: string },
  ): Promise<void> {
    const { label = "default" } = parseOptions<{ label: string }>(
      options instanceof MediaStream ? { label: options.id } : options,
      (value) => {
        return { label: String(value) };
      },
    );

    const oldStreamOptions = this.#streamOptions.get(label);
    const { stream, managed } = oldStreamOptions ?? {};

    this.#streamOptions.delete(label);

    if (!stream) return;

    if (!managed) {
      for (const track of stream.getTracks()) {
        if (track.readyState !== "ended") track.stop();
      }
    }

    this.emit(["local", "local:unshare"], {
      name: "local:unshare",
      stream,
      label,
    });

    await Promise.allSettled(
      Array.from(this.#connections.values()).map((remote) =>
        remote.unshare({ label }),
      ),
    );
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
   * ```js
   * // open a channel with label "chat"
   * await peer.open({ label: "chat" });
   * ```
   *
   * @param options Channel options or channel label.
   */
  async open(options: string | ChannelOptions): Promise<void> {
    const { label = "default", ...channelOptions } =
      parseOptions<ChannelOptions>(options, (value) => {
        return { label: String(value) };
      });

    this.#channelOptions.set(label, { ...channelOptions, label });

    this.emit(["local", "local:open"], {
      name: "local:open",
      label,
    });

    await Promise.allSettled(
      Array.from(this.#connections.values()).map((remote) =>
        remote.open({ ...channelOptions, label }),
      ),
    );
  }

  /**
   * Closes a previously opened data channel with the given label
   * and removes it from all remote peers.
   *
   * @example
   * ```js
   * // close the channel with label "chat"
   * await peer.close({ label: "chat" });
   * ```
   *
   * @param options Channel label or object containing `label`.
   */
  async close(options: string | { label?: string }): Promise<void> {
    const { label = "default" } = parseOptions<{ label: string }>(
      options,
      (value) => {
        return { label: String(value) };
      },
    );

    this.#channelOptions.delete(label);

    this.emit(["local", "local:close"], {
      name: "local:close",
      label,
    });

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
   * ```js
   * // send a message to default channel
   * await peer.send("Hello, all peers!");
   * // send large data with a progress handler
   * const file = new File([new Uint8Array(1024 * 1024)], "example.dat");
   * const transfer = peer.send(file, {
   *   label: "chat", // channel label
   *   info: { filename: file.name }, // metadata
   *   signal: AbortSignal.timeout(10000), // abort signal
   * });
   * // optionally handle the progress
   * for await (const progress of transfer) {
   *   const { id, label, current, total } = progress;
   *   const percent = Math.round((current / total) * 100);
   *   console.log(`[${id}:${label}] Sending... ${percent}%`);
   * }
   * ```
   *
   * @param message Message payload to send.
   * @param options Send options or channel label.
   * @returns An iterable aggregated transfer progress across all connections
   *   or a promise which resolves when all transfers complete or error.
   */
  send(
    message: unknown,
    options?: string | SendOptions,
  ): ReadableStream<TransferProgress> & Promise<void> {
    const {
      label = "default",
      info,
      signal,
    } = parseOptions<SendOptions>(options, (value) => {
      return { label: String(value) };
    });

    const numConnections = this.#connections.size;
    if (!this.#active || !numConnections) {
      if (message instanceof ReadableStream) message.cancel();
      return new PromiseLikeReadableStream<TransferProgress>({
        start(controller) {
          controller.close();
        },
      });
    }

    let streams: ReadableStream[] | undefined;
    if (message instanceof ReadableStream) {
      streams = teeStream(message, numConnections);
    }

    const sources: PromiseLikeReadableStream<TransferProgress>[] = [];
    for (const [index, remote] of Array.from(
      this.#connections.values(),
    ).entries()) {
      const data = streams ? streams[index] : message;
      const progress = remote.send(data, { label, info, signal });
      sources.push(progress);
    }

    return mergeStreams<TransferProgress>(sources);
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
   * @example
   * ```js
   * // subscribe to the "connection" event
   * peer.on("connection", (e) => {
   *   console.log("Connection state has changed:", e.state);
   * });
   * ```
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
   * Typically, you would not call this method directly.
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
  toJSON(): {
    id: string;
    room: string;
    metadata?: Record<string, unknown>;
    active: boolean;
    connections: string[];
    streams: string[];
    channels: string[];
  } {
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
    metadata?: Record<string, unknown>;
  }): Promise<RemotePeer | null> {
    const { id, metadata } = options;

    const existingRemote = this.#getRemotePeer(id);
    if (existingRemote) return null;

    const verified = await this.#verifyRemotePeer({ id, metadata });
    if (!verified) return null;

    const remote = this.#newRemotePeer({ id, metadata });
    this.#bindRemoteLifecycleHandlers(remote);
    this.#registerRemotePeer(remote);

    return remote;
  }

  /**
   * Retrieves an existing remote peer by ID, excluding peers in a closed state.
   */
  #getRemotePeer(id: string): RemotePeer | null {
    const remote = this.#connections.get(id);
    return remote && remote.state !== "closed" ? remote : null;
  }

  /**
   * Verifies a remote peer using the optional verify function provided in the constructor.
   * Returns true if no verify function is set or if the verification passes.
   */
  async #verifyRemotePeer(options: {
    id: string;
    metadata?: Record<string, unknown>;
  }): Promise<boolean> {
    if (typeof this.#verify !== "function") return true;
    return await this.#verify(options);
  }

  /**
   * Instantiates a new RemotePeer with the current peer's configuration settings.
   */
  #newRemotePeer(options: {
    id: string;
    metadata?: Record<string, unknown>;
  }): RemotePeer {
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
      this.emit(["connection", e.name], { ...e, remote });
    });

    remote.on("channel", (e) => {
      this.emit(["channel", e.name], { ...e, remote });
    });

    remote.on("stream", (e) => {
      this.emit(["stream", e.name], { ...e, remote });
    });

    remote.on("track", (e) => {
      this.emit(["track", e.name], { ...e, remote });
    });

    remote.on("error", (e) => {
      this.emit("error", e);
    });
  }

  /**
   * Registers a remote peer in the connections map and emits a connection:new event.
   */
  #registerRemotePeer(remote: RemotePeer): void {
    this.#connections.set(remote.id, remote);

    this.emit(["connection", "connection:new"], {
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
  "new" | "connecting" | "connected" | "disconnected" | "failed" | "closed";

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
   * If true, tracks will not be stopped when the stream is unshared or replaced.
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
  /** AbortSignal to cancel the send operation. */
  signal?: AbortSignal;
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
  /** Total number of bytes to transfer if known, otherwise `undefined`. */
  total?: number;
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
   * ```js
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
  metadata?: Record<string, unknown>;
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
    metadata?: Record<string, unknown>;
  }) => Promise<boolean> | boolean;
}

/**
 * Event emitted when a local peer requests to join or leave a room.
 *
 * @group Peers
 */
export interface LocalJoinLeaveEvent {
  /** Name of the event. */
  name: "local:join" | "local:leave";
  /** The room being joined or left. */
  room: string;
  /** The metadata associated with the join/leave operation. */
  metadata?: Record<string, unknown>;
}

/**
 * Event emitted when a request is made to share or unshare a media stream.
 *
 * @group Peers
 */
export interface LocalShareUnshareEvent {
  /** Name of the event. */
  name: "local:share" | "local:unshare";
  /** The shared media stream. */
  stream: MediaStream;
  /** The label associated with the media stream. */
  label: string;
}

/**
 * Event emitted when a request is made to open or close a data channel.
 *
 * @group Peers
 */
export interface LocalOpenCloseEvent {
  /** Name of the event. */
  name: "local:open" | "local:close";
  /** The label associated with the data channel. */
  label: string;
}

/**
 * Event emitted on peer connection state changes.
 *
 * @group Peers
 */
export interface PeerConnectionEvent {
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
  data?: ReadableStream<Uint8Array> & PromiseLike<unknown>;
  /** Error object containing details about the error for error events. */
  error?: PeerixError;
}

/**
 * Emitted when a remote peer shares or unshares a media stream.
 *
 * @group Peers
 */
export interface PeerStreamEvent {
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
  /** Name of the event. */
  name: "error";
  /** Error object containing details about the error. */
  error: PeerixError;
  /** Remote peer associated with the error, if applicable. */
  remote?: RemotePeer;
}

/**
 * Events emitted by {@link Peer} instances.
 *
 * @group Peers
 */
export interface PeerEvents {
  /** Fired on any local method call. */
  local: [LocalJoinLeaveEvent | LocalShareUnshareEvent | LocalOpenCloseEvent];
  /** A peer joins a room. */
  "local:join": [LocalJoinLeaveEvent];
  /** A peer leaves the current room. */
  "local:leave": [LocalJoinLeaveEvent];
  /** A media stream is shared on this peer. */
  "local:share": [LocalShareUnshareEvent];
  /** A media stream is unshared from this peer. */
  "local:unshare": [LocalShareUnshareEvent];
  /** A data channel is opened on this peer. */
  "local:open": [LocalOpenCloseEvent];
  /** A data channel is closed on this peer. */
  "local:close": [LocalOpenCloseEvent];
  /** Fired on any peer connection state change. */
  connection: [PeerConnectionEvent];
  /** A peer connection is created. */
  "connection:new": [PeerConnectionEvent];
  /** A peer connection is connecting. */
  "connection:connecting": [PeerConnectionEvent];
  /** A peer connection is established. */
  "connection:connected": [PeerConnectionEvent];
  /** A peer connection is disconnected. */
  "connection:disconnected": [PeerConnectionEvent];
  /** A peer connection has failed. */
  "connection:failed": [PeerConnectionEvent];
  /** A peer connection is closed. */
  "connection:closed": [PeerConnectionEvent];
  /** Fired on any media stream change from a remote peer. */
  stream: [PeerStreamEvent];
  /** A remote peer shares a media stream. */
  "stream:add": [PeerStreamEvent];
  /** A remote peer unshares a media stream. */
  "stream:remove": [PeerStreamEvent];
  /** Fired on any media track change from a remote peer. */
  track: [PeerTrackEvent];
  /** A remote peer adds a media track to a shared stream. */
  "track:add": [PeerTrackEvent];
  /** A remote peer removes a media track from a shared stream. */
  "track:remove": [PeerTrackEvent];
  /** Fired on any data channel event from a remote peer. */
  channel: [PeerChannelEvent];
  /** A data channel is created by a remote peer. */
  "channel:new": [PeerChannelEvent];
  /** A data channel is opened with a remote peer. */
  "channel:open": [PeerChannelEvent];
  /** A data channel is closed with a remote peer. */
  "channel:close": [PeerChannelEvent];
  /** A message is received on a data channel from a remote peer. */
  "channel:message": [PeerChannelEvent];
  /** An error occurs on a data channel with a remote peer. */
  "channel:error": [PeerChannelEvent];
  /** An error occurs in any background operation. */
  error: [PeerErrorEvent];
}
