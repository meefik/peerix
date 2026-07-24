import type { Driver } from "./drivers/driver.js";
import type {
  IceServer,
  IceTransportPolicy,
  StreamOptions,
  ChannelOptions,
  SendOptions,
  TransferProgress,
  PeerEvents,
} from "./peer.js";
import log from "./utils/logger.js";
import { PeerixError, type ErrorCode } from "./error.js";
import { Peer } from "./peer.js";
import { MemoryDriver } from "./drivers/memory.js";
import { parseOptions } from "./utils/helpers.js";
import {
  teeStream,
  PromiseLikeReadableStream,
  mergeStreams,
} from "./utils/stream.js";
import { EventEmitter } from "./utils/emitter.js";
import { IceCandidateQueue } from "./utils/ice.js";
import { Signaler, SIGNAL_TYPE, type SignalMessage } from "./signaler.js";
import { Addon } from "./addons/addon.js";

// All peers without a driver will share the same in-memory signaling bus
const defaultDriver = new MemoryDriver();

/**
 * Manages WebRTC peer connections, media streams, and data channels.
 *
 * The following diagram provides a high-level overview of the Peerix
 * architecture and its components:
 *
 * ```mermaid
 * graph TD
 *   PX{{Room}} --> SD(Signaling Drivers)
 *   SD --> SS[Signaling Servers]
 *   PX --> ICE[STUN/TURN Servers]
 *   PX --> PC(Peers)
 *   PC --> LCE(Lifecycle Events)
 *   PC --> MS(Media Streams)
 *   PC --> DC(Data Channels)
 *   PX --> ADD(Add-ons)
 * ```
 *
 * @group Room
 * @example
 * ```js
 * // create a room using default in-memory signaling driver
 * const room = new Room({ id: "my-room" });
 *
 * // listen for connection state changes
 * room.on("connection", (e) => {
 *   const { peer, state } = e;
 *   console.log(`Peer "${peer.id}" state changed to "${state}"`);
 * });
 *
 * // listen for open channel event
 * room.on("channel:open", (e) => {
 *   const { peer, label } = e;
 *   console.log(`Channel "${label}" opened with peer "${peer.id}"`);
 *   // send a message to the remote peer
 *   peer.send("Hello, peer!", { label });
 * });
 *
 * // listen for incoming messages
 * room.on("channel:message", async (e) => {
 *   const { peer, data, label } = e;
 *   const message = await data;
 *   console.log(`Message from peer "${peer.id}" on channel "${label}":`, message);
 * });
 *
 * // open a data channel
 * await room.open({ label: "default" });
 *
 * // join a room with optional metadata
 * await room.join({ name: "Alice" });
 * ```
 */
export class Room {
  /** Indicates whether the peer is currently active (joined a room). */
  get active(): boolean {
    return this.#active;
  }
  /** Unique room identifier. */
  get id(): string {
    return this.#id;
  }
  /** Local peer id and metadata. */
  get me(): { id: string; metadata?: Record<string, unknown> } | null {
    return this.#me;
  }
  /** Active peers indexed by peer id. */
  get peers(): ReadonlyMap<string, Peer> {
    return this.#peers;
  }
  /** Attachable extensions. */
  get addons(): ReadonlySet<Addon> {
    return this.#addons;
  }

  #active: boolean;
  #id: string;
  #me: {
    id: string;
    metadata?: Record<string, unknown>;
  } | null;
  #driver: Driver;
  #peers: Map<string, Peer>;
  #streamOptions: Map<string, StreamOptions>;
  #channelOptions: Map<string, ChannelOptions>;
  #addons: Set<Addon>;
  #iceServers: IceServer[];
  #iceTransportPolicy: IceTransportPolicy;
  #iceCandidateDebounce: number;
  #candidateQueue: IceCandidateQueue;
  #connectionTimeout: number;
  #verify?: (options: {
    id: string;
    metadata?: Record<string, unknown>;
  }) => Promise<boolean> | boolean;
  #emitter: EventEmitter<PeerEvents>;
  #signaler: Signaler;
  #driverActiveHandler?: () => void;

  /**
   * Creates a new {@link Peer} instance.
   *
   * @example
   * ```js
   * // create a new peer with default options
   * const peer = new Peer();
   * ```
   *
   * @param options Room configuration options.
   */
  constructor(options?: RoomOptions) {
    const {
      id = "default",
      driver,
      verify,
      iceServers = [],
      iceTransportPolicy = "all",
      iceCandidateDebounce = 50,
      connectionTimeout = 15,
      namespaceHashing = true,
      signalingCompression = true,
      signalingEncryption = true,
    } = options ?? {};

    this.#active = false;
    this.#id = String(id);
    this.#driver = driver ?? defaultDriver;
    this.#verify = verify;
    this.#me = null;
    this.#peers = new Map();
    this.#streamOptions = new Map();
    this.#channelOptions = new Map();
    this.#addons = new Set();
    this.#iceServers = iceServers;
    this.#iceTransportPolicy = iceTransportPolicy;
    this.#iceCandidateDebounce = iceCandidateDebounce;
    this.#candidateQueue = new IceCandidateQueue();
    this.#connectionTimeout = connectionTimeout;
    this.#emitter = new EventEmitter(this);
    this.#signaler = new Signaler({
      driver: this.#driver,
      namespaceHashing,
      signalingCompression,
      signalingEncryption,
      onMessage: this.#onSignalerMessage.bind(this),
      onError: (err) => {
        this.emit("error", {
          name: "error",
          error: new PeerixError(err, "SIGNALING_ERROR"),
        });
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
   * // join a room with optional metadata
   * const me = await peer.join({ name: "Alice" });
   * console.log(`I am ${me.metadata.name} (ID: ${me.id})`);
   * ```
   *
   * @param metadata Metadata to associate with the peer.
   */
  async join(
    metadata?: Record<string, unknown>,
  ): Promise<{ id: string; metadata?: Record<string, unknown> } | null> {
    if (this.#active) return null;
    this.#active = true;

    this.#driverActiveHandler = this.#onDriverActive.bind(this);
    this.#driver.on("active", this.#driverActiveHandler);

    try {
      const id = await this.#signaler.subscribe(this.#id);
      this.#me = { id, metadata };
    } catch (err) {
      await this.leave();
      throw err;
    }

    void this.#signaler.publish({
      type: SIGNAL_TYPE.announce,
      id: this.#id,
    });

    return this.#me;
  }

  /**
   * Leaves the current room and closes all active connections.
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

    if (this.#driverActiveHandler) {
      this.#driver.off("active", this.#driverActiveHandler);
      this.#driverActiveHandler = undefined;
    }

    try {
      await this.#signaler.unsubscribe(this.#id);

      for (const peer of this.#peers.values()) {
        peer.dispose();
      }
    } finally {
      this.#peers.clear();
      this.#candidateQueue.clear();
      this.#me = null;
      this.#active = false;
    }
  }

  /**
   * Shares a new media stream or updates an existing one for all peers
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
   * const stream = await navigator.mediaDevices.getUserMedia(
   *   { video: true, audio: true }
   * );
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

    const hasSharedStream = this.#streamOptions.has(label);
    const { stream: newStream = new MediaStream(), managed } =
      this.#streamOptions.get(label) ?? {};

    const incomingTracks = stream.getTracks();
    const currentTracks = newStream.getTracks();
    const incomingTrackIds = new Set(incomingTracks.map((track) => track.id));
    const currentTrackIds = new Set(currentTracks.map((track) => track.id));
    const endedHandler = async (track: MediaStreamTrack) => {
      try {
        newStream.removeTrack(track);
        this.emit(["track", "track:remove"], {
          name: "track:remove",
          label,
          stream: newStream,
          track,
        });
        if (!newStream.active) {
          await this.unshare({ label });
        }
      } catch (err) {
        this.#emitError(err, "MEDIASTREAM_ERROR");
      }
    };

    for (const track of currentTracks) {
      if (!incomingTrackIds.has(track.id)) {
        newStream.removeTrack(track);
        if (!managed && track.readyState !== "ended") {
          track.stop();
        }
        this.emit(["track", "track:remove"], {
          name: "track:remove",
          label,
          stream: newStream,
          track,
        });
      }
    }
    for (const track of incomingTracks) {
      if (!currentTrackIds.has(track.id)) {
        newStream.addTrack(track);
        if (!managed) {
          track.addEventListener("ended", () => endedHandler(track), {
            once: true,
          });
        }
        this.emit(["track", "track:add"], {
          name: "track:add",
          label,
          stream: newStream,
          track,
        });
      }
    }

    const newStreamOptions = { ...opts, label, stream: newStream };
    this.#streamOptions.set(label, newStreamOptions);

    if (!hasSharedStream) {
      this.emit(["stream", "stream:add"], {
        name: "stream:add",
        label,
        stream: newStream,
      });
    }

    await Promise.allSettled(
      Array.from(this.#peers.values()).map((peer) =>
        peer.share(newStreamOptions),
      ),
    );
  }

  /**
   * Stops sharing a previously shared media stream with the given label
   * and removes it from all peers.
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

    const tracks = stream.getTracks();
    for (const track of tracks) {
      if (!managed && track.readyState !== "ended") {
        track.stop();
      }
      this.emit(["track", "track:remove"], {
        name: "track:remove",
        label,
        stream,
        track,
      });
    }

    this.emit(["stream", "stream:remove"], {
      name: "stream:remove",
      label,
      stream,
    });

    await Promise.allSettled(
      Array.from(this.#peers.values()).map((peer) =>
        peer.unshare({ label }),
      ),
    );
  }

  /**
   * Opens a data channel with the given label and options to all peers.
   * If a channel with the same label already exists, it will be reused.
   *
   * You can open a channel with the same label on both local and remote peers
   * or only on one side. In any case, only one channel will be created for
   * each label. You can send data through the channel in both directions.
   *
   * @example
   * ```js
   * // open a channel with an explicit label
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

    await Promise.allSettled(
      Array.from(this.#peers.values()).map((peer) =>
        peer.open({ ...channelOptions, label }),
      ),
    );
  }

  /**
   * Closes a previously opened data channel with the given label
   * and removes it from all peers.
   *
   * @example
   * ```js
   * // close the channel an explicit label
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

    await Promise.allSettled(
      Array.from(this.#peers.values()).map((peer) =>
        peer.close({ label }),
      ),
    );
  }

  /**
   * Sends a message through data channels to all connected peers.
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
      to,
    } = parseOptions<SendOptions>(options, (value) => {
      return { label: String(value) };
    });

    const numPeers = this.#peers.size;
    if (!this.#active || !numPeers) {
      if (message instanceof ReadableStream) message.cancel();
      return new PromiseLikeReadableStream<TransferProgress>({
        start(controller) {
          controller.close();
        },
      });
    }

    let streams: ReadableStream[] | undefined;
    if (message instanceof ReadableStream) {
      streams = teeStream(message, numPeers);
    }

    const targetPeers = to ? (Array.isArray(to) ? to : [to]) : null;
    const sources: PromiseLikeReadableStream<TransferProgress>[] = [];
    for (const [index, peer] of Array.from(
      this.#peers.values(),
    ).entries()) {
      if (targetPeers && !targetPeers.includes(peer.id)) continue;
      const data = streams ? streams[index] : message;
      const progress = peer.send(data, { label, info, signal });
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
  on<K extends keyof RoomEvents>(
    event: K | K[],
    handler: (...args: RoomEvents[K]) => void,
  ): void {
    this.#emitter.on(event, handler);
  }

  /**
   * Subscribes to an event and auto-unsubscribes after first invocation.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
  once<K extends keyof RoomEvents>(
    event: K | K[],
    handler: (...args: RoomEvents[K]) => void,
  ): void {
    this.#emitter.once(event, handler);
  }

  /**
   * Removes a previously registered event listener.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler to remove. If omitted, all handlers for the given event(s) will be removed.
   */
  off<K extends keyof RoomEvents>(
    event: K | K[],
    handler?: (...args: RoomEvents[K]) => void,
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
  emit<K extends keyof RoomEvents>(
    event: K | K[],
    ...args: RoomEvents[K]
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
    active: boolean;
    me: { id: string; metadata?: Record<string, unknown> } | null;
    peers: string[];
  } {
    return {
      id: this.id,
      active: this.active,
      me: this.me,
      peers: Array.from(this.peers.keys()),
    };
  }

  /**
   * Logs and emits an error event with the given raw error and context code.
   */
  #emitError(err: unknown, code: ErrorCode): void {
    const error = new PeerixError(err, code);
    log("room:error", { error });
    this.emit("error", { name: "error", error });
  }

  /**
   * Creates a new peer connection by verifying its existence, validating the peer,
   * binding lifecycle handlers, and registering it in the connections map.
   */
  async #createPeer(options: {
    id: string;
    metadata?: Record<string, unknown>;
  }): Promise<Peer | null> {
    const { id, metadata } = options;

    if (!this.#me) return null;

    const existingPeer = this.#getPeer(id);
    if (existingPeer) return null;

    const verified = await this.#verifyPeer({ id, metadata });
    if (!verified) return null;

    const peer = new Peer({
      id,
      metadata,
      polite: this.#me.id > id,
      iceServers: this.#iceServers,
      iceTransportPolicy: this.#iceTransportPolicy,
      connectionTimeout: this.#connectionTimeout,
      iceCandidateDebounce: this.#iceCandidateDebounce,
      streams: this.#streamOptions,
      channels: this.#channelOptions,
    });

    peer.on("connection:closed", () => {
      this.#peers.delete(peer.id);
      this.#signaler.reset(id);
      this.#candidateQueue.clear(id);
    });

    peer.on("connection", (e) => {
      this.emit(["connection", e.name], e);
    });

    peer.on("channel", (e) => {
      this.emit(["channel", e.name], e);
    });

    peer.on("stream", (e) => {
      this.emit(["stream", e.name], e);
    });

    peer.on("track", (e) => {
      this.emit(["track", e.name], e);
    });

    peer.on("error", (e) => {
      this.emit("error", e);
    });

    peer.on("signal", (e) => {
      const { name, data } = e;
      if (!name || !this.#me) return;

      let message: unknown[] = [];
      if (name === "offer") {
        message.push(data, this.#me?.metadata);
      } else if (name === "answer") {
        message.push(data);
      } else if (name === "candidate") {
        message.push(...(Array.isArray(data) ? data : [data]));
      }

      void this.#signaler.publish({ type: SIGNAL_TYPE[name], id, message });
    });

    peer.on("connection:failed", () => {
      if (!this.#me) return;

      void this.#signaler.publish(
        {
          type: SIGNAL_TYPE.invoke,
          id,
          message: [this.#me?.metadata],
        },
        {
          jitter: 1000,
        },
      );
    });

    this.#peers.set(peer.id, peer);

    this.emit(["connection", "connection:new"], {
      name: "connection:new",
      state: "new",
      peer,
    });

    return peer;
  }

  /**
   * Retrieves an existing peer by ID, excluding peers in a closed state.
   */
  #getPeer(id: string): Peer | null {
    const peer = this.#peers.get(id);
    return peer && peer.state !== "closed" ? peer : null;
  }

  /**
   * Verifies a peer using the optional verify function provided in the constructor.
   * Returns true if no verify function is set or if the verification passes.
   */
  async #verifyPeer(options: {
    id: string;
    metadata?: Record<string, unknown>;
  }): Promise<boolean> {
    if (typeof this.#verify !== "function") return true;
    return await this.#verify(options);
  }

  /**
   * Handles driver activation events by re-broadcasting an announce signal.
   * This ensures new drivers in the room discover existing peers.
   */
  #onDriverActive() {
    void this.#signaler.publish({
      type: SIGNAL_TYPE.announce,
      id: this.#id,
    });
  }

  /**
   * Handles incoming messages from the signaling bus.
   */
  async #onSignalerMessage(data: SignalMessage): Promise<void> {
    const { type, id, message } = data;

    // Ignore our own messages
    if (!this.#me || id === this.#me.id) return;

    // Another peer joined — respond with an invoke carrying our metadata
    if (type === SIGNAL_TYPE.announce) {
      void this.#signaler.publish({
        type: SIGNAL_TYPE.invoke,
        id,
        message: this.#me.metadata ? [this.#me.metadata] : undefined,
      });
    }

    // Peer discovery — create a connection
    else if (type === SIGNAL_TYPE.invoke) {
      const [metadata] = message as [Record<string, unknown>];
      await this.#createPeer({ id, metadata });
    }

    // Incoming offer from a remote peer
    else if (type === SIGNAL_TYPE.offer) {
      const [description, metadata] = message as [
        RTCSessionDescriptionInit,
        Record<string, unknown>,
      ];
      let peer = this.#getPeer(id);
      if (!peer) peer = await this.#createPeer({ id, metadata });
      if (!peer) return;

      await peer.signal(description);

      // Flush any candidates that arrived before the offer
      for (const candidate of this.#candidateQueue.pull(id, description)) {
        await peer.signal(candidate);
      }
    }

    // Incoming answer from a remote peer
    else if (type === SIGNAL_TYPE.answer) {
      const [description] = message as [RTCSessionDescriptionInit];
      const peer = this.#getPeer(id);
      if (!peer) return;

      await peer.signal(description);

      // Flush any candidates that arrived before the answer
      for (const candidate of this.#candidateQueue.pull(id, description)) {
        await peer.signal(candidate);
      }
    }

    // ICE candidates from a remote peer
    else if (type === SIGNAL_TYPE.candidate) {
      const [...candidates] = message as RTCIceCandidate[];

      const peer = this.#getPeer(id);

      // Queue candidates if the remote description isn't set yet
      const description = peer?.connection.remoteDescription ?? undefined;

      for (const candidate of candidates) {
        const queued = this.#candidateQueue.push(id, candidate, description);
        // Skip if no remote peer or if the candidate was queued
        if (!peer || queued) continue;
        await peer.signal(candidate);
      }
    }
  }
}

/**
 * Configuration options for creating a {@link Peer} instance.
 *
 * @group Room
 */
export interface RoomOptions {
  /**
   * Unique identifier for the room. Uses "default" if omitted.
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
   * to local peers. Empty by default.
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
 * Events emitted by a {@link Room} instance.
 *
 * @group Room
 */
export interface RoomEvents extends PeerEvents {}
