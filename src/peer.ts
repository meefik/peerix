import log from "./utils/logger.js";
import { PeerixError, type ErrorCode, type ErrorEvent } from "./error.js";
import { parseOptions } from "./utils/helpers.js";
import { EventEmitter } from "./utils/emitter.js";
import { PromiseLikeReadableStream } from "./utils/stream.js";
import { Timeout } from "./utils/timeout.js";
import { IceCandidateBatcher } from "./utils/ice.js";
import { ControlChannel } from "./control.js";
import { DataChannel } from "./channel.js";

/** Types of messages exchanged over the control channel. */
const MESSAGE_TYPE = {
  signal: 1, // SDP and ICE messages
  channel: 2, // data channel creation messages
} as const;

/**
 * Represents a peer connection.
 * Do not create Peer instances manually.
 *
 * @group Peers
 */
export class Peer {
  /** Peer identifier. */
  get id(): string {
    return this.#id;
  }
  /** Metadata advertised by the peer. */
  get metadata(): Record<string, unknown> | undefined {
    return this.#metadata;
  }
  /** Peer connection state, updated on connection state changes. */
  get state(): ConnectionState {
    return this.#state;
  }
  /** Native WebRTC peer connection to the peer. */
  get connection(): RTCPeerConnection {
    return this.#connection;
  }
  /** Remote media streams keyed by stream label. */
  get streams(): ReadonlyMap<string, MediaStream> {
    return this.#streams;
  }
  /** Negotiated data channels keyed by channel label. */
  get channels(): ReadonlyMap<string, RTCDataChannel> {
    return this.#channels;
  }

  #id: string;
  #metadata?: Record<string, unknown>;
  #state: ConnectionState;
  #connection: RTCPeerConnection;
  #emitter: EventEmitter<PeerEvents>;
  #streams: Map<string, MediaStream>;
  #channels: Map<string, RTCDataChannel>;
  #polite: boolean;
  #streamLabels: Map<string, string>;
  #ignoreOffer: boolean;
  #signalQueue: Promise<unknown>;
  #streamOptions: Map<string, StreamOptions>;
  #channelOptions: Map<string, ChannelOptions>;
  #controlChannel: ControlChannel;
  #dataChannels: Map<string, DataChannel>;
  #timeout: Timeout;
  #pendingCandidates: RTCIceCandidateInit[];
  #iceCandidateBatcher: IceCandidateBatcher;

  /**
   * Creates a {@link Peer} instance.
   *
   * Do not create Peer instances manually.
   *
   * @internal
   * @param options Options for creating the peer.
   */
  constructor(options: PeerOptions) {
    const {
      id,
      metadata,
      polite = false,
      iceServers = [],
      iceTransportPolicy = "all",
      connectionTimeout = 15,
      iceCandidateDebounce = 50,
      streams,
      channels,
    } = options;

    const connection = new RTCPeerConnection({
      iceServers,
      iceTransportPolicy,
    });
    this.#connection = connection;
    this.#state = "new";
    this.#id = id;
    this.#metadata = metadata;
    this.#streams = new Map();
    this.#channels = new Map();
    this.#polite = polite;
    this.#emitter = new EventEmitter(this);
    this.#streamLabels = new Map();
    this.#ignoreOffer = false;
    this.#streamOptions = new Map(streams);
    this.#channelOptions = new Map(channels);
    this.#dataChannels = new Map();
    this.#timeout = new Timeout(() => {
      this.#setConnectionState("failed");
      this.dispose();
    }, connectionTimeout * 1000);
    this.#pendingCandidates = [];
    this.#signalQueue = Promise.resolve();
    this.#iceCandidateBatcher = new IceCandidateBatcher({
      delay: iceCandidateDebounce,
      onFlush: (candidates) => {
        if (this.state === "closed" || !candidates.length) return;
        this.emit("signal", { name: "candidate", data: candidates });
      },
    });

    connection.addEventListener("iceconnectionstatechange", () => {
      const { iceConnectionState } = connection;

      if (iceConnectionState === "new") {
        this.#setConnectionState("new");
      } else if (iceConnectionState === "checking") {
        this.#setConnectionState("connecting");
      } else if (iceConnectionState === "connected") {
        this.#timeout.stop();
        this.#setConnectionState("connected");
      } else if (iceConnectionState === "disconnected") {
        this.#timeout.start();
        this.#setConnectionState("disconnected");
      } else if (iceConnectionState === "failed") {
        this.#setConnectionState("failed");
        this.dispose();
      } else if (iceConnectionState === "closed") {
        this.dispose();
      }
    });

    connection.addEventListener("icecandidate", async (e) => {
      const { candidate } = e;
      if (!candidate) return;
      try {
        const candidateInit =
          this.#serializeJSON<RTCIceCandidateInit>(candidate);

        log("peer:icecandidate", { id: this.#id, candidate: candidateInit });

        if (this.#controlChannel.active) {
          this.#controlChannel.send(MESSAGE_TYPE.signal, [candidateInit]);
        } else {
          this.#iceCandidateBatcher.push(candidateInit);
        }
      } catch (err) {
        this.#emitError(err, "ICECANDIDATE_ERROR");
      }
    });

    connection.addEventListener("negotiationneeded", () => {
      void this.#enqueueTask(async () => {
        if (connection.signalingState !== "stable") return;
        try {
          await this.#createOffer();
        } catch (err) {
          this.#emitError(err, "NEGOTIATION_ERROR");
        }
      });
    });

    connection.addEventListener("signalingstatechange", () => {
      if (connection.signalingState === "stable") {
        this.#ignoreOffer = false;
      }
    });

    connection.addEventListener("datachannel", (e) => {
      const { channel } = e;
      this.#setupDataChannel(channel);
    });

    connection.addEventListener("track", (e) => {
      const {
        track,
        streams: [stream],
      } = e;
      this.#setupMediaTrack(track, stream);
    });

    this.#controlChannel = new ControlChannel({
      connection,
      callback: {
        open: () => {
          // create channels
          for (const channelOptions of this.#channelOptions.values()) {
            this.#requestDataChannel(channelOptions);
          }
          // add streams
          for (const streamOptions of this.#streamOptions.values()) {
            this.#addStream(streamOptions);
          }
        },
        close: () => {
          this.dispose();
        },
        message: async (event, message) => {
          if (event === MESSAGE_TYPE.signal) {
            const [description, labels] = message as [
              RTCSessionDescriptionInit | RTCIceCandidateInit,
              Record<string, string>?,
            ];
            if (labels) {
              this.#setStreamLabels(labels);
            }
            await this.signal(description);
          } else if (event === MESSAGE_TYPE.channel) {
            this.#requestDataChannel(message as ChannelOptions);
          }
        },
        error: (err: unknown) => {
          this.#emitError(err, "SIGNALING_ERROR");
        },
      },
    });

    this.#timeout.start();

    log("peer:connection", { id: this.#id, state: "new" });
  }

  /**
   * Shares a new media stream to the current peer or updates an existing one.
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
   * const stream = await navigator.mediaDevices.getUserMedia({
   *   video: true, audio: true
   * });
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

    const addedTracks: MediaStreamTrack[] = [];
    const removedTracks: MediaStreamTrack[] = [];

    const incomingTracks = stream.getTracks();
    const currentTracks = newStream.getTracks();
    const incomingTrackIds = new Set(incomingTracks.map((track) => track.id));
    const currentTrackIds = new Set(currentTracks.map((track) => track.id));
    const endedHandler = async (track: MediaStreamTrack) => {
      try {
        newStream.removeTrack(track);
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
        removedTracks.push(track);
        if (!managed && track.readyState !== "ended") track.stop();
      }
    }
    for (const track of incomingTracks) {
      if (!currentTrackIds.has(track.id)) {
        newStream.addTrack(track);
        addedTracks.push(track);
        if (!managed) {
          track.addEventListener("ended", () => endedHandler(track), {
            once: true,
          });
        }
      }
    }

    const newStreamOptions = { ...opts, label, stream: newStream };
    this.#streamOptions.set(label, newStreamOptions);

    await this.#updateStream(newStreamOptions, addedTracks, removedTracks);
  }

  /**
   * Stops sharing a previously shared media stream to the current peer.
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

    await this.#removeStream(stream);
  }

  /**
   * Opens a data channel to the current peer.
   *
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

    this.#requestDataChannel({ ...channelOptions, label });
  }

  /**
   * Closes a previously opened data channel to the current peer.
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

    const dc = this.#dataChannels.get(label);
    dc?.destroy();
  }

  /**
   * Sends a message through a data channel.
   *
   * If `options` is a string, it is treated as the channel label. If no label
   * is provided, it uses the `default` channel.
   *
   * The `send` method works only with open channels that have no protocol specified,
   * are ordered (reliable), and match the specified label.
   *
   * @example
   * ```js
   * // send a message to default channel
   * await peer.send("Hello, peer!");
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
   * @returns A ReadableStream of transfer progress status or a Promise.
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

    const dc = this.#dataChannels.get(label);
    if (!dc) {
      if (message instanceof ReadableStream) message.cancel();
      return new PromiseLikeReadableStream<TransferProgress>({
        start(c) {
          const err = new Error(`No channel found for label: ${label}`);
          c.error(err);
        },
      });
    }

    return dc.send(message, { info, signal });
  }

  /**
   * Applies a remote session description or ICE candidate received from the remote peer.
   *
   * @internal
   * @param data Remote session description or ICE candidate to apply.
   * @returns Promise that resolves when the description is applied and ICE candidates are added.
   */
  signal(data: RTCSessionDescriptionInit | RTCIceCandidateInit): Promise<void> {
    return this.#enqueueTask(async () => {
      if (this.#state === "closed") return;

      if ("sdp" in data && data.type && data.sdp) {
        const hasOffer = data.type === "offer";
        const collision =
          hasOffer && this.#connection.signalingState !== "stable";

        this.#ignoreOffer = !this.#polite && collision;

        if (this.#ignoreOffer) {
          this.#pendingCandidates.length = 0;
          log("peer:collision", { id: this.#id, description: data });
          return;
        }

        try {
          if (hasOffer && collision && this.#polite) {
            await this.#rollbackLocalDescription();
          }

          await this.#setRemoteDescription(data);
          await this.#drainPendingCandidates();

          if (hasOffer) {
            await this.#createAnswer();
          }
        } catch (err) {
          this.#emitError(err, "NEGOTIATION_ERROR");
        }
      } else if ("candidate" in data && data.candidate) {
        if (this.#ignoreOffer) {
          log("peer:ignorecandidate", { id: this.#id, candidate: data });
          return;
        }

        if (!this.#connection.remoteDescription) {
          this.#pendingCandidates.push(data);
          log("peer:queuecandidate", {
            id: this.#id,
            candidate: data,
            count: this.#pendingCandidates.length,
          });
          return;
        }

        await this.#addIceCandidate(data);
      }
    });
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
   * Closes and frees all connection resources.
   */
  dispose(): void {
    if (this.#state === "closed") return;
    this.#setConnectionState("closed");

    log("peer:dispose", { id: this.#id });

    this.#timeout.stop();
    this.#controlChannel.close();
    this.#dataChannels.forEach((dc) => dc.destroy());
    this.#connection?.close();

    this.#channels.clear();
    this.#streams.clear();
    this.#pendingCandidates.length = 0;
    this.#iceCandidateBatcher.clear();
    this.#streamLabels.clear();
    this.#ignoreOffer = false;
  }

  /**
   * Serializes the peer to a JSON-compatible object.
   *
   * @returns A serializable representation of the peer.
   */
  toJSON(): {
    id: string;
    metadata?: Record<string, unknown>;
    state: ConnectionState;
    streams: string[];
    channels: string[];
  } {
    return {
      id: this.id,
      metadata: this.metadata,
      state: this.state,
      streams: Array.from(this.streams.keys()),
      channels: Array.from(this.channels.keys()),
    };
  }

  /**
   * Queues a signaling task so SDP and ICE messages are applied sequentially.
   */
  #enqueueTask<T>(task: () => Promise<T>): Promise<T> {
    const prev = this.#signalQueue;
    const run = prev.then(() => task());
    this.#signalQueue = run.catch(() => {});
    return run;
  }

  /**
   * Generates a map of stream IDs to their corresponding labels from the current stream options.
   */
  #getStreamLabels(): Record<string, string> {
    return Array.from(this.#streamOptions.keys()).reduce(
      (acc, label) => {
        const { stream } = this.#streamOptions.get(label) ?? {};
        if (stream) acc[stream.id] = label;
        return acc;
      },
      {} as Record<string, string>,
    );
  }

  /**
   * Sets custom labels for remote media streams based on their stream ids.
   */
  #setStreamLabels(labels: Record<string, string>): void {
    this.#streamLabels.clear();
    for (const streamId in labels) {
      this.#streamLabels.set(streamId, labels[streamId]);
    }
  }

  /**
   * Serializes a value by calling its `toJSON` method if available; otherwise returns the value unchanged.
   */
  #serializeJSON<T>(data: unknown): T {
    return data && typeof (data as any).toJSON === "function"
      ? (data as any).toJSON()
      : (data as T);
  }

  /**
   * Logs and emits an error event with the given raw error and context code.
   */
  #emitError(err: unknown, code: ErrorCode): void {
    const error = new PeerixError(err, code);
    log("peer:error", { id: this.#id, error });
    this.emit("error", { name: "error", error });
  }

  /**
   * Requests or creates a data channel.
   */
  #requestDataChannel(options: ChannelOptions): void {
    const { label = "default", ...channelOptions } = options ?? {};
    if (this.#dataChannels.has(label)) return;

    log("peer:requestchannel", { id: this.#id, label, ...channelOptions });

    try {
      if (this.#polite) {
        this.#controlChannel.send(MESSAGE_TYPE.channel, {
          ...channelOptions,
          label,
        });
      } else {
        const channel = this.#connection.createDataChannel(
          label,
          channelOptions,
        );
        this.#setupDataChannel(channel);
      }
    } catch (err) {
      this.#emitError(err, "DATACHANNEL_ERROR");
    }
  }

  /**
   * Adds a local media stream to the peer connection.
   */
  async #addStream(streamOptions: StreamOptions): Promise<void> {
    const { stream, audioParameters, videoParameters } = streamOptions;

    log("peer:addstream", {
      id: this.#id,
      stream,
      audioParameters,
      videoParameters,
    });

    try {
      const tracks = stream.getTracks();
      const senders = this.#connection.getSenders();

      for (const track of tracks) {
        const hasSender = senders.some(
          (s) => s.track && s.track.id === track.id,
        );
        if (hasSender) continue;

        await this.#addTrackAsSender(
          stream,
          track,
          audioParameters,
          videoParameters,
        );
      }
    } catch (err) {
      this.#emitError(err, "MEDIASTREAM_ERROR");
    }
  }

  /**
   * Updates the media stream by adding and removing tracks as needed.
   */
  async #updateStream(
    streamOptions: StreamOptions,
    addedTracks: MediaStreamTrack[],
    removedTracks: MediaStreamTrack[],
  ): Promise<void> {
    const { stream, audioParameters, videoParameters } = streamOptions;
    const senders = this.#connection.getSenders();

    log("peer:updatestream", {
      id: this.#id,
      stream,
      addedTracks,
      removedTracks,
    });

    try {
      const usedAddedTracks = new Set<MediaStreamTrack>();

      // 1. Try to replace tracks for senders that are no longer sending their current track
      for (const sender of senders) {
        const currentTrack = sender.track;
        if (!currentTrack || !removedTracks.includes(currentTrack)) {
          continue;
        }

        // Find a replacement track of the same kind that hasn't been used yet
        const replacement = addedTracks.find(
          (t) => t.kind === currentTrack.kind && !usedAddedTracks.has(t),
        );

        if (replacement) {
          await sender.replaceTrack(replacement);
          await this.#setSenderParameters(
            replacement,
            replacement.kind === "audio" ? audioParameters : videoParameters,
          );
          usedAddedTracks.add(replacement);
        }
      }

      // 2. Add any remaining tracks that weren't used as replacements as new senders
      for (const track of addedTracks) {
        if (!usedAddedTracks.has(track)) {
          await this.#addTrackAsSender(
            stream,
            track,
            audioParameters,
            videoParameters,
          );
        }
      }

      // 3. Stop transceivers that were not replaced and are in the removed list
      this.#stopSendonlyTransceivers(removedTracks);
    } catch (err) {
      this.#emitError(err, "MEDIASTREAM_ERROR");
    }
  }

  /**
   * Removes a media stream from the peer connection.
   */
  async #removeStream(stream: MediaStream): Promise<void> {
    log("peer:removestream", { id: this.#id, stream });

    try {
      const existingTracks = stream?.getTracks() ?? [];
      this.#stopSendonlyTransceivers(existingTracks);
    } catch (err) {
      this.#emitError(err, "MEDIASTREAM_ERROR");
    }
  }

  /**
   * Adds a sendonly transceiver for a track and applies optional sender parameters.
   */
  async #addTrackAsSender(
    stream: MediaStream,
    track: MediaStreamTrack,
    audioParameters?: Record<string, unknown>,
    videoParameters?: Record<string, unknown>,
  ): Promise<void> {
    this.#connection.addTransceiver(track, {
      direction: "sendonly",
      streams: [stream],
    });
    await this.#setSenderParameters(
      track,
      track.kind === "audio" ? audioParameters : videoParameters,
    );
  }

  /**
   * Stops sendonly transceivers that have no track or whose track matches one of the provided tracks.
   */
  #stopSendonlyTransceivers(tracks: MediaStreamTrack[]): void {
    for (const transceiver of this.#connection.getTransceivers()) {
      if (transceiver.direction !== "sendonly") continue;
      const readyToStop =
        !transceiver.sender.track ||
        tracks.some((track) => track.id === transceiver.sender.track?.id);
      if (readyToStop) transceiver.stop();
    }
  }

  /**
   * Adds queued ICE candidates after a remote description becomes available.
   */
  async #drainPendingCandidates(): Promise<void> {
    if (
      !this.#connection.remoteDescription ||
      !this.#pendingCandidates.length
    ) {
      return;
    }

    const pendingCandidates = this.#pendingCandidates.splice(0);
    for (const candidate of pendingCandidates) {
      await this.#addIceCandidate(candidate);
    }
  }

  /**
   * Adds an ICE candidate to the peer connection.
   */
  async #addIceCandidate(candidate: RTCIceCandidateInit): Promise<void> {
    if (this.#state === "closed") return;

    log("peer:addcandidate", { id: this.#id, candidate });

    try {
      await this.#connection.addIceCandidate(candidate);
    } catch (err) {
      this.#emitError(err, "ICECANDIDATE_ERROR");
    }
  }

  /**
   * Updates the connection state.
   */
  #setConnectionState(state: ConnectionState): void {
    if (this.#state === state) return;

    log("peer:connection", { id: this.#id, state });

    this.#state = state;
    this.emit(["connection", `connection:${state}`], {
      name: `connection:${state}`,
      peer: this,
      state,
    });
  }

  /**
   * Creates an offer, sets it as the local description,
   * then sends it to the remote peer (including stream labels when available).
   */
  async #createOffer(): Promise<void> {
    const offer = await this.#connection.createOffer();
    await this.#connection.setLocalDescription(offer);
    const description = this.#serializeJSON<RTCSessionDescriptionInit>(
      this.#connection.localDescription,
    );

    if (!description) {
      throw new Error("Failed to set local offer description");
    }

    log("peer:createoffer", { id: this.#id, description });

    this.#transmitSdp("offer", description, this.#getStreamLabels());
  }

  /**
   * Creates an answer, sets it as the local description,
   * then sends it to the remote peer.
   */
  async #createAnswer(): Promise<void> {
    const answer = await this.#connection.createAnswer();
    await this.#connection.setLocalDescription(answer);
    const description = this.#serializeJSON<RTCSessionDescriptionInit>(
      this.#connection.localDescription,
    );

    if (!description) {
      throw new Error("Failed to set local answer description");
    }

    log("peer:createanswer", { id: this.#id, description });

    this.#transmitSdp("answer", description);
  }

  /**
   * Sets the remote session description on the peer connection.
   */
  async #setRemoteDescription(
    description: RTCSessionDescriptionInit,
  ): Promise<void> {
    log("peer:setdescription", { id: this.#id, description });
    await this.#connection.setRemoteDescription(description);
  }

  /**
   * Rolls back the local description to a rollback state if necessary.
   * Suppresses errors that occur during rollback.
   */
  async #rollbackLocalDescription(): Promise<void> {
    try {
      if (this.#connection.signalingState !== "stable") {
        await this.#connection.setLocalDescription({ type: "rollback" });
      }
    } catch {
      /* rollback errors are benign, suppress */
    }
  }

  /**
   * Transmits an SDP offer or answer to the remote peer via the control channel,
   * or emits a signal event if the control channel is not active.
   */
  #transmitSdp(
    type: "offer" | "answer",
    description: RTCSessionDescriptionInit,
    labels?: Record<string, string>,
  ): void {
    if (this.#controlChannel.active) {
      const data: any[] = [description];
      if (labels) data.push(labels);
      this.#controlChannel.send(MESSAGE_TYPE.signal, data);
    } else {
      this.emit("signal", { name: type, data: description });
    }
  }

  /**
   * Sets parameters for a media track by updating the sender parameters.
   */
  async #setSenderParameters(
    track: MediaStreamTrack,
    parameters?: Record<string, unknown>,
  ): Promise<void> {
    if (!parameters) return;

    const senders = this.#connection.getSenders();
    const sender = senders.find((sender: RTCRtpSender) => {
      return sender.track && sender.track.id === track.id;
    });

    if (sender) {
      log("peer:setparameters", { id: this.#id, track, parameters });

      const params = sender.getParameters() ?? {};
      if (!params.encodings) return;
      for (const encoding of params.encodings) {
        if (!encoding) continue;
        Object.assign(encoding, parameters);
      }
      await sender.setParameters(params);
    }
  }

  /**
   * Sets up a data channel and emits appropriate events.
   */
  #setupDataChannel(channel: RTCDataChannel): void {
    const { label = "" } = channel;

    log("peer:setupchannel", { id: this.#id, channel });

    if (this.#dataChannels.has(label)) {
      this.#dataChannels.get(label)?.destroy();
    }

    const emitEvent = (
      name: keyof PeerEvents,
      extra: Record<string, unknown> = {},
    ) => {
      const event = { id: this.#id, channel, label, ...extra };
      this.emit(["channel", name], {
        ...event,
        name,
        peer: this,
      } as ChannelEvent);
    };

    const dc = new DataChannel({
      peerId: this.id,
      channel,
      callback: {
        open: () => emitEvent("channel:open"),
        close: () => {
          emitEvent("channel:close");
          dc.destroy();
        },
        error: (err: unknown) => {
          emitEvent("channel:error", {
            error: new PeerixError(err, "DATACHANNEL_ERROR"),
          });
        },
        message: (data: unknown, info?: Record<string, unknown>) => {
          emitEvent("channel:message", { data, info });
        },
        destroy: () => {
          if (this.#dataChannels.get(label) === dc) {
            this.#dataChannels.delete(label);
            this.#channels.delete(label);
          }
        },
      },
    });

    this.#dataChannels.set(label, dc);
    this.#channels.set(label, channel);

    const event = { id: this.#id, channel, label };
    this.emit(["channel", "channel:new"], {
      ...event,
      name: "channel:new",
      peer: this,
    });
  }

  /**
   * Sets up a media track by adding it to the corresponding stream
   * and emitting appropriate events.
   */
  #setupMediaTrack(track: MediaStreamTrack, stream: MediaStream): void {
    const label = this.#streamLabels.get(stream.id) ?? stream.id;

    log("peer:setuptrack", { id: this.#id, stream, track });

    const addTrack = () => {
      if (!this.#streams.has(label)) {
        this.#streams.set(label, stream);
        const event = { id: this.#id, stream, label };
        this.emit(["stream", "stream:add"], {
          ...event,
          name: "stream:add",
          peer: this,
        });
      }

      const event = { id: this.#id, track, stream, label };
      this.emit(["track", "track:add"], {
        ...event,
        name: "track:add",
        peer: this,
      });
    };

    const removeTrack = () => {
      track.removeEventListener("ended", removeTrack);
      stream.removeTrack(track);

      const event = { id: this.#id, track, stream, label };
      this.emit(["track", "track:remove"], {
        ...event,
        name: "track:remove",
        peer: this,
      });

      if (!stream.active || !stream.getTracks().length) {
        if (this.#streams.has(label)) {
          this.#streams.delete(label);
          const event = { id: this.#id, stream, label };
          this.emit(["stream", "stream:remove"], {
            ...event,
            name: "stream:remove",
            peer: this,
          });
        }
      }
    };

    track.addEventListener("ended", removeTrack);

    addTrack();
  }
}

/**
 * Options for creating a {@link Peer} instance.
 *
 * @internal
 * @group Peers
 */
export interface PeerOptions {
  /** Unique peer identifier. */
  id: string;
  /** Optional metadata associated with the peer. */
  metadata?: Record<string, unknown>;
  /** Indicates if this peer should be polite during negotiation. */
  polite?: boolean;
  /** Optional ICE servers for NAT traversal. */
  iceServers: IceServer[];
  /** Policy for ICE transport. */
  iceTransportPolicy: IceTransportPolicy;
  /** Timeout in seconds for connection establishment. */
  connectionTimeout: number;
  /** Debounce time (ms) for aggregating ICE candidates before sending them. */
  iceCandidateDebounce: number;
  /** Map of streams indexed by label. */
  streams: Map<string, StreamOptions>;
  /** Map of data channels indexed by label. */
  channels: Map<string, ChannelOptions>;
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
export type ConnectionState =
  "new" | "connecting" | "connected" | "disconnected" | "failed" | "closed";

/**
 * Local stream publication options.
 *
 * @group Streams and Channels
 */
export interface StreamOptions {
  /** Stream label. If omitted, the `default` label will be used. */
  label?: string;
  /** Media stream to share. */
  stream: MediaStream;
  /**
   * Whether the peer should manage the lifecycle of the stream's tracks.
   * If true, tracks will not be stopped when the stream is unshared or replaced.
   */
  managed?: boolean;
  /** Preferred audio encoding parameters to apply to the stream's audio tracks, such as bitrate or priority. */
  audioParameters?: {
    /** Preferred maximum bitrate in bits per second to encode the audio tracks. */
    maxBitrate?: number;
    /** Preferred priority for encoding the audio tracks. */
    priority?: RTCPriorityType;
  };
  /** Preferred video encoding parameters to apply to the stream's video tracks, such as bitrate, frame rate, or priority. */
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
  /** Channel label. If omitted, the `default` label will be used. */
  label?: string;
  /** Optional subprotocol name. */
  protocol?: string;
  /** Whether ordered delivery is required. */
  ordered?: boolean;
  /** Maximum packet lifetime in milliseconds. */
  maxPacketLifeTime?: number;
  /** Maximum retransmission attempts. */
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
  /** The target peer or peers to send the message to. */
  to?: string | string[];
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
 * Event emitted when a signal is received from the remote peer,
 * such as an offer, answer, or ICE candidate.
 *
 * @internal
 * @group Peers
 */
export interface SignalEvent {
  /** Name of the event. */
  name: "offer" | "answer" | "candidate";
  /** Signal data, which can be an offer, answer, or ICE candidate. */
  data: RTCSessionDescriptionInit | RTCIceCandidateInit[];
  /** Stream labels associated with the offer. */
  labels?: Record<string, string>;
}

/**
 * Event emitted on peer connection state changes.
 *
 * @group Peers
 */
export interface ConnectionEvent {
  /** Name of the event. */
  name:
    | "connection:new"
    | "connection:connecting"
    | "connection:connected"
    | "connection:disconnected"
    | "connection:failed"
    | "connection:closed";
  /** Peer object containing connection details. */
  peer: Peer;
  /** New connection state. */
  state: ConnectionState;
}

/**
 * Emitted when a data channel is created or received from a peer,
 * when a data channel is opened or closed, when a message is received on a
 * data channel, or when an error occurs.
 *
 * @group Peers
 */
export interface ChannelEvent {
  /** Name of the event. */
  name:
    | "channel:new"
    | "channel:open"
    | "channel:close"
    | "channel:message"
    | "channel:error";
  /** Peer object containing connection details. */
  peer: Peer;
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
 * Emitted when a peer shares or unshares a media stream.
 *
 * @group Peers
 */
export interface StreamEvent {
  /** Name of the event. */
  name: "stream:add" | "stream:remove";
  /** Peer object containing connection details. For local streams, peer is undefined. */
  peer?: Peer;
  /** Media stream associated with the event. */
  stream: MediaStream;
  /** Label of the media stream. */
  label: string;
}

/**
 * Emitted when a peer adds a media track to or removes one from a shared stream.
 *
 * @group Peers
 */
export interface TrackEvent {
  /** Name of the event. */
  name: "track:add" | "track:remove";
  /** Peer object containing connection details. For local tracks, peer is undefined. */
  peer?: Peer;
  /** Media stream associated with the event. */
  stream: MediaStream;
  /** Media track associated with the event. */
  track: MediaStreamTrack;
  /** Label of the media stream. */
  label: string;
}

/**
 * Events emitted by {@link Peer} instances.
 *
 * @group Peers
 */
export interface PeerEvents {
  /** SDP offer, answer, or ICE candidate is exchanged. @internal */
  signal: [SignalEvent];
  /** Fired on any peer connection state change. */
  connection: [ConnectionEvent];
  /** A peer connection is created. */
  "connection:new": [ConnectionEvent];
  /** A peer connection is connecting. */
  "connection:connecting": [ConnectionEvent];
  /** A peer connection is established. */
  "connection:connected": [ConnectionEvent];
  /** A peer connection is disconnected. */
  "connection:disconnected": [ConnectionEvent];
  /** A peer connection has failed. */
  "connection:failed": [ConnectionEvent];
  /** A peer connection is closed. */
  "connection:closed": [ConnectionEvent];
  /** Fired on any media stream change from a remote peer. */
  stream: [StreamEvent];
  /** A media stream is shared by a remote peer. */
  "stream:add": [StreamEvent];
  /** A media stream is unshared by a remote peer. */
  "stream:remove": [StreamEvent];
  /** Fired on any media track change from a remote peer. */
  track: [TrackEvent];
  /** A media track is added to a shared stream by a remote peer. */
  "track:add": [TrackEvent];
  /** A media track is removed from a shared stream by a remote peer. */
  "track:remove": [TrackEvent];
  /** Fired on any data channel event from a remote peer. */
  channel: [ChannelEvent];
  /** A data channel is created with a remote peer. */
  "channel:new": [ChannelEvent];
  /** A data channel is opened with a remote peer. */
  "channel:open": [ChannelEvent];
  /** A data channel is closed with a remote peer. */
  "channel:close": [ChannelEvent];
  /** A message is received on a data channel from a remote peer. */
  "channel:message": [ChannelEvent];
  /** An error occurs on a data channel with a remote peer. */
  "channel:error": [ChannelEvent];
  /** An error occurs in any background operation. */
  error: [ErrorEvent];
}
