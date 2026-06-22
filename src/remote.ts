import type {
  IceServer,
  IceTransportPolicy,
  PeerConnectionState,
  ChannelOptions,
  StreamOptions,
  SendOptions,
  TransferProgress,
} from "./peer.js";
import log from "./utils/logger.js";
import { PeerixError } from "./error.js";
import { parseOptions } from "./utils/helpers.js";
import { EventEmitter } from "./utils/emitter.js";
import { Timeout } from "./utils/timeout.js";
import { ControlChannel } from "./control.js";
import { DataChannel } from "./channel.js";

/** Types of messages exchanged over the control channel. */
const MESSAGE_TYPE = {
  signal: 1, // SDP and ICE messages
  channel: 2, // data channel creation messages
} as const;

/**
 * Represents a remote peer connection.
 * Do not create RemotePeer instances manually.
 *
 * @group Remote Peers
 */
export class RemotePeer {
  /** Remote peer identifier. */
  get id(): string {
    return this.#id;
  }
  /** Room name the peer is associated with. */
  get room(): string {
    return this.#room;
  }
  /** Metadata advertised by the remote peer. */
  get metadata(): unknown {
    return this.#metadata;
  }
  /** Peer connection state, updated on connection state changes. */
  get state(): PeerConnectionState {
    return this.#state;
  }
  /** Native WebRTC peer connection to the remote peer. */
  get connection(): RTCPeerConnection {
    return this.#connection;
  }
  /** Remote media streams keyed by stream label. */
  get streams(): Map<string, MediaStream> {
    return this.#streams;
  }
  /** Negotiated data channels keyed by channel label. */
  get channels(): Map<string, RTCDataChannel> {
    return this.#channels;
  }

  #id: string;
  #room: string;
  #metadata: unknown;
  #state: PeerConnectionState;
  #connection: RTCPeerConnection;
  #emitter: EventEmitter<RemotePeerEvents>;
  #streams: Map<string, MediaStream>;
  #channels: Map<string, RTCDataChannel>;
  #polite: boolean;
  #streamLabels: Map<string, string>;
  #makingOffer: boolean;
  #pendingAnswer: boolean;
  #settingRemoteDescription: boolean;
  #ignoreOffer: boolean;
  #signalQueue: Promise<unknown>;
  #streamOptions: Map<string, StreamOptions>;
  #channelOptions: Map<string, ChannelOptions>;
  #controlChannel: ControlChannel;
  #dataChannels: Map<string, DataChannel>;
  #timeout: Timeout;
  #pendingCandidates: RTCIceCandidateInit[];

  /**
   * Creates a {@link RemotePeer} instance.
   *
   * @internal
   * @param options Options for creating the remote peer connection.
   */
  constructor(options: RemotePeerOptions) {
    const {
      id,
      metadata,
      room,
      polite,
      iceServers = [],
      iceTransportPolicy = "all",
      connectionTimeout = 15,
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
    this.#room = room;
    this.#metadata = metadata;
    this.#streams = new Map();
    this.#channels = new Map();
    this.#polite = polite;
    this.#emitter = new EventEmitter(this);
    this.#streamLabels = new Map();
    this.#makingOffer = false;
    this.#pendingAnswer = false;
    this.#settingRemoteDescription = false;
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

        log("remote:icecandidate", { id: this.#id, candidate: candidateInit });

        if (this.#controlChannel.active) {
          this.#controlChannel.send(MESSAGE_TYPE.signal, [candidateInit]);
        } else {
          this.emit("signal", {
            id: this.#id,
            name: "candidate",
            data: candidateInit,
          });
        }
      } catch (err) {
        this.#emitPeerError(err, "ICECANDIDATE_ERROR");
      }
    });

    connection.addEventListener("negotiationneeded", () => {
      if (connection.signalingState !== "stable") return;
      this.#createOffer();
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
            this.#createChannel(channelOptions);
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
            this.#createChannel(message as ChannelOptions);
          }
        },
        error: (error: unknown) => {
          this.#emitPeerError(error, "SIGNALING_ERROR");
        },
      },
    });

    this.#timeout.start();

    log("remote:connection", { id: this.#id, state: this.#state });
  }

  /**
   * Registers an event handler for a specific event type emitted by the remote peer connection.
   *
   * @param event Event type to listen for.
   * @param handler Callback function to handle the event.
   */
  on<K extends keyof RemotePeerEvents>(
    event: K | K[],
    handler: (...args: RemotePeerEvents[K]) => void,
  ): void {
    this.#emitter.on(event, handler);
  }

  /**
   * Unregisters an event handler for a specific event type emitted by the remote peer connection.
   *
   * @param event Event type to stop listening for.
   * @param handler Callback function to remove from the event listeners.
   */
  off<K extends keyof RemotePeerEvents>(
    event: K | K[],
    handler: (...args: RemotePeerEvents[K]) => void,
  ): void {
    this.#emitter.off(event, handler);
  }

  /**
   * Emits an event with optional payload to the registered event handlers.
   *
   * @param event Event type to emit.
   * @param args Optional arguments to pass to the event handlers.
   */
  emit<K extends keyof RemotePeerEvents>(
    event: K | K[],
    ...args: RemotePeerEvents[K]
  ): void {
    this.#emitter.emit(event, ...args);
  }

  /**
   * Closes and frees all connection resources.
   */
  dispose(): void {
    if (this.#state === "closed") return;
    this.#setConnectionState("closed");

    log("remote:dispose", {
      id: this.#id,
      room: this.#room,
      metadata: this.#metadata,
    });

    this.#timeout.stop();
    this.#controlChannel.close();
    this.#dataChannels.forEach((dc) => dc.destroy());
    this.#connection?.close();

    this.#channels.clear();
    this.#streams.clear();
    this.#pendingCandidates.length = 0;
    this.#streamLabels.clear();
    this.#makingOffer = false;
    this.#pendingAnswer = false;
    this.#settingRemoteDescription = false;
    this.#ignoreOffer = false;
  }

  /**
   * Shares a new media stream to the current remote peer or updates an existing one.
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

    const addedTracks: MediaStreamTrack[] = [];
    const removedTracks: MediaStreamTrack[] = [];

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
        removedTracks.push(track);
      }
    }
    for (const track of incomingTracks) {
      if (!currentTrackIds.has(track.id)) {
        newStream.addTrack(track);
        addedTracks.push(track);
      }
    }

    const newStreamOptions = { label, stream: newStream, ...opts };

    log("remote:share", { id: this.#id, ...newStreamOptions });

    this.#streamOptions.set(label, newStreamOptions);

    await this.#updateStream(newStreamOptions, addedTracks, removedTracks);

    return newStream;
  }

  /**
   * Stops sharing a previously shared media stream to the current remote peer.
   *
   * If you pass a MediaStream instance directly, it will be unshared using
   * its id as the label. Otherwise, you can specify the label in the options
   * object or pass it directly as a string.
   *
   * If the stream was shared with the `managed` option, its tracks will be
   * stopped automatically.
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

    log("remote:unshare", { id: this.#id, label, stream });

    this.#streamOptions.delete(label);

    if (!stream) return;

    if (managed) {
      for (const track of stream.getTracks()) {
        if (track.readyState !== "ended") {
          track.stop();
        }
      }
    }

    await this.#removeStream(stream);

    return stream;
  }

  /**
   * Opens a data channel to the current remote peer.
   *
   * If a channel with the same label already exists, it will be reused.
   *
   * You can open a channel with the same label on both local and remote peers
   * or only on one side. In any case, only one channel will be created for
   * each label. You can send data through the channel in both directions.
   *
   * @param options Channel options or channel label.
   */
  async open(options: string | ChannelOptions): Promise<void> {
    const { label = "default", ...channelOptions } =
      parseOptions<ChannelOptions>(options, (value) => {
        return { label: String(value) };
      });

    log("remote:open", { id: this.#id, label, ...channelOptions });

    this.#channelOptions.set(label, { label, ...channelOptions });

    this.#createChannel({ label, ...channelOptions });
  }

  /**
   * Closes a previously opened data channel to the current remote peer.
   *
   * @param options Channel label or object containing `label`.
   */
  async close(options: string | { label: string }): Promise<void> {
    const { label = "default" } = parseOptions(options, (value) => {
      return { label: String(value) };
    });

    log("remote:close", { id: this.#id, label });

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
   * @param message Message payload to send.
   * @param options Send options or channel label.
   * @returns A ReadableStream of transfer progress status.
   */
  send(
    message: unknown,
    options?: string | SendOptions,
  ): ReadableStream<TransferProgress> {
    const { label = "default", info } = parseOptions<SendOptions>(
      options,
      (value) => {
        return { label: String(value) };
      },
    );

    const dc = this.#dataChannels.get(label);
    if (!dc) {
      if (message instanceof ReadableStream) message.cancel();
      return new ReadableStream({
        start(c) {
          const err = new Error(`No channel found for label: ${label}`);
          c.error(err);
        },
      });
    }

    log("remote:send", { id: this.#id, label, info, message });

    return dc.send(message, info);
  }

  /**
   * Applies a remote session description or ICE candidate received from the remote peer.
   *
   * @internal
   * @param data Remote session description or ICE candidate to apply.
   * @returns Promise that resolves when the description is applied and ICE candidates are added.
   */
  signal(data: RTCSessionDescriptionInit | RTCIceCandidateInit): Promise<void> {
    return this.#enqueueSignal(async () => {
      if (this.#state === "closed") return;

      if ("sdp" in data && data.type && data.sdp) {
        const hasOffer = data.type === "offer";
        const collision = hasOffer && this.#hasCollision();

        this.#ignoreOffer = !this.#polite && collision;

        if (this.#ignoreOffer) {
          this.#pendingCandidates.length = 0;
          log("remote:collision", { id: this.#id, description: data });
          return;
        }

        try {
          if (hasOffer && collision && this.#polite) {
            await this.#waitForNegotiationIdle();
            await this.#rollbackLocalDescription();
          }

          await this.#setRemoteDescription(data);
          await this.#drainPendingCandidates();

          if (hasOffer) {
            await this.#createAnswer();
          }
        } catch (err) {
          this.#emitPeerError(err, "NEGOTIATION_ERROR");
        }
      } else if ("candidate" in data && data.candidate) {
        if (this.#ignoreOffer) {
          log("remote:ignorecandidate", { id: this.#id, candidate: data });
          return;
        }

        if (!this.#connection.remoteDescription) {
          this.#pendingCandidates.push(data);
          log("remote:queuecandidate", {
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
   * Serializes the remote peer to a JSON-compatible object.
   *
   * @returns A serializable representation of the peer.
   */
  toJSON() {
    return {
      id: this.id,
      room: this.room,
      metadata: this.metadata,
      state: this.#state,
      streams: Array.from(this.#streamOptions.keys()),
      channels: Array.from(this.#channelOptions.keys()),
    };
  }

  /**
   * Emits and logs a normalized peer error event.
   */
  #emitPeerError(
    err: unknown,
    code: ConstructorParameters<typeof PeerixError>[1],
  ): void {
    const error = new PeerixError(err, code);
    this.emit("error", { id: this.#id, name: "error", error });
    log("remote:error", { id: this.#id, error });
  }

  /**
   * Waits until negotiation-sensitive flags are clear.
   */
  async #waitForNegotiationIdle(ms = 5000): Promise<void> {
    let t = Date.now();
    while (
      this.#makingOffer ||
      this.#pendingAnswer ||
      this.#settingRemoteDescription
    ) {
      await new Promise((resolve) => setTimeout(resolve, 10));
      if (Date.now() - t > ms) {
        throw new Error("Negotiation idle timeout");
      }
    }
  }

  /**
   * Adds a sendonly transceiver for a track and applies optional sender parameters.
   */
  async #addTrackAsSender(
    stream: MediaStream,
    track: MediaStreamTrack,
    audioParameters?: { [key: string]: unknown },
    videoParameters?: { [key: string]: unknown },
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
   * Stops inactive sendonly transceivers or that are matching the provided tracks.
   */
  #stopSendonlyTransceivers(tracks: MediaStreamTrack[]): void {
    for (const transceiver of this.#connection.getTransceivers()) {
      if (transceiver.direction !== "sendonly") continue;
      const readyToStop = tracks.some(
        (track) =>
          !transceiver.sender.track || track.id === transceiver.sender.track.id,
      );
      if (readyToStop) transceiver.stop();
    }
  }

  /**
   * Queues a signaling task so SDP and ICE messages are applied sequentially.
   */
  #enqueueSignal<T>(task: () => Promise<T>): Promise<T> {
    const prev = this.#signalQueue;
    const run = prev.then(() => task());
    this.#signalQueue = run.catch(() => {});
    return run;
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
      try {
        await this.#addIceCandidate(candidate);
      } catch {
        // Ignore errors during pending candidate drain; the connection may be closing.
      }
    }
  }

  /**
   * Adds an ICE candidate to the peer connection.
   */
  async #addIceCandidate(candidate: RTCIceCandidateInit): Promise<void> {
    log("remote:addcandidate", { id: this.#id, candidate });

    try {
      await this.#connection.addIceCandidate(candidate);
    } catch (err) {
      this.#emitPeerError(err, "ICECANDIDATE_ERROR");
    }
  }

  /**
   * Updates the connection state.
   */
  #setConnectionState(state: PeerConnectionState): void {
    if (this.#state === state) return;
    this.#state = state;
    this.emit(["connection", `connection:${state}`], {
      id: this.#id,
      name: `connection:${state}`,
      state,
    });

    log("remote:connection", { id: this.#id, state });
  }

  /**
   * Sets custom labels for remote media streams based on their stream ids.
   */
  #setStreamLabels(labels: { [key: string]: string }): void {
    this.#streamLabels.clear();
    for (const streamId in labels) {
      this.#streamLabels.set(streamId, labels[streamId]);
    }
  }

  /**
   * Serializes data using its `toJSON` method if available, otherwise returns it as is.
   */
  #serializeJSON<T>(data: any): T {
    return typeof data?.toJSON === "function" ? data.toJSON() : data;
  }

  /**
   * Creates a data channel.
   */
  #createChannel(options: ChannelOptions): void {
    const { label = "default", ...channelOptions } = options || {};
    if (this.#dataChannels.has(label)) return;

    try {
      if (this.#polite) {
        log("remote:requestchannel", {
          id: this.#id,
          label,
          ...channelOptions,
        });

        this.#controlChannel.send(MESSAGE_TYPE.channel, {
          label,
          ...channelOptions,
        });
      } else {
        log("remote:createchannel", { id: this.#id, label, ...channelOptions });

        const channel = this.#connection.createDataChannel(
          label,
          channelOptions,
        );

        this.#setupDataChannel(channel);
      }
    } catch (err) {
      this.#emitPeerError(err, "DATACHANNEL_ERROR");
    }
  }

  /**
   * Adds a local media stream to the peer connection.
   */
  async #addStream(streamOptions: StreamOptions): Promise<void> {
    const { stream, audioParameters, videoParameters } = streamOptions;

    try {
      log("remote:addstream", {
        id: this.#id,
        stream,
        audioParameters,
        videoParameters,
      });

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
      this.#emitPeerError(err, "MEDIASTREAM_ERROR");
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

    try {
      log("remote:updatestream", {
        id: this.#id,
        stream,
        addedTracks,
        removedTracks,
      });

      for (const track of addedTracks) {
        const removedTrack = removedTracks.find((t) => t.kind === track.kind);
        if (removedTrack) {
          const sender = senders.find((s) => s.track?.id === removedTrack.id);
          if (sender) {
            await sender.replaceTrack(track);
            await this.#setSenderParameters(
              track,
              track.kind === "audio" ? audioParameters : videoParameters,
            );
            continue;
          }
        }
        await this.#addTrackAsSender(
          stream,
          track,
          audioParameters,
          videoParameters,
        );
      }

      this.#stopSendonlyTransceivers(removedTracks);
    } catch (err) {
      this.#emitPeerError(err, "MEDIASTREAM_ERROR");
    }
  }

  /**
   * Removes a media stream from the peer connection.
   */
  async #removeStream(stream: MediaStream): Promise<void> {
    const existingTracks = stream?.getTracks() || [];

    try {
      this.#stopSendonlyTransceivers(existingTracks);
    } catch (err) {
      this.#emitPeerError(err, "MEDIASTREAM_ERROR");
    }
  }

  /**
   * Checks for a signaling collision with the remote peer.
   */
  #hasCollision(): boolean {
    const readyForOffer =
      !this.#makingOffer &&
      !this.#settingRemoteDescription &&
      this.#connection.signalingState === "stable";
    return !readyForOffer;
  }

  /**
   * Creates an offer, sets it as the local description,
   * then sends it to the remote peer (including stream labels when available).
   */
  async #createOffer(): Promise<void> {
    try {
      this.#makingOffer = true;

      const offer = await this.#connection.createOffer();
      await this.#connection.setLocalDescription(offer);
      const description = this.#serializeJSON<RTCSessionDescriptionInit>(
        this.#connection.localDescription,
      );

      if (!description) {
        throw new Error("Failed to set local offer description");
      }

      log("remote:createoffer", { id: this.#id, description });

      if (this.#controlChannel.active) {
        const labels = Array.from(this.#streamOptions.keys()).reduce(
          (acc, label) => {
            const { stream } = this.#streamOptions.get(label) || {};
            if (stream) acc[stream.id] = label;
            return acc;
          },
          {} as { [key: string]: string },
        );
        this.#controlChannel.send(MESSAGE_TYPE.signal, [description, labels]);
      } else {
        this.emit("signal", { id: this.#id, name: "offer", data: description });
      }
    } catch (err) {
      await this.#rollbackLocalDescription();
      this.#emitPeerError(err, "NEGOTIATION_ERROR");
    } finally {
      this.#makingOffer = false;
    }
  }

  /**
   * Creates an answer, sets it as the local description,
   * then sends it to the remote peer.
   */
  async #createAnswer(): Promise<void> {
    try {
      this.#pendingAnswer = true;

      const answer = await this.#connection.createAnswer();
      await this.#connection.setLocalDescription(answer);
      const description = this.#serializeJSON<RTCSessionDescriptionInit>(
        this.#connection.localDescription,
      );

      if (!description) {
        throw new Error("Failed to set local answer description");
      }

      log("remote:createanswer", { id: this.#id, description });

      if (this.#controlChannel.active) {
        this.#controlChannel.send(MESSAGE_TYPE.signal, [description]);
      } else {
        this.emit("signal", {
          id: this.#id,
          name: "answer",
          data: description,
        });
      }
    } catch (err) {
      await this.#rollbackLocalDescription();
      this.#emitPeerError(err, "NEGOTIATION_ERROR");
    } finally {
      this.#pendingAnswer = false;
    }
  }

  /**
   * Sets the remote session description on the peer connection.
   */
  async #setRemoteDescription(
    description: RTCSessionDescriptionInit,
  ): Promise<void> {
    log("remote:setdescription", { id: this.#id, description });

    await this.#waitForNegotiationIdle();

    try {
      this.#settingRemoteDescription = true;
      await this.#connection.setRemoteDescription(description);
    } finally {
      this.#settingRemoteDescription = false;
    }
  }

  /**
   * Rolls back the local description to a rollback state if necessary.
   * Suppresses errors that occur during rollback.
   */
  async #rollbackLocalDescription(): Promise<void> {
    const rollbackStates: RTCSignalingState[] = [
      "have-local-offer",
      "have-remote-pranswer",
    ];
    const { signalingState } = this.#connection;

    log("remote:rollback", { id: this.#id, signalingState });

    try {
      if (rollbackStates.includes(signalingState)) {
        await this.#connection.setLocalDescription({ type: "rollback" });
      }
    } catch {}
  }

  /**
   * Sets up a data channel and emits appropriate events.
   */
  #setupDataChannel(channel: RTCDataChannel): void {
    const { label = "" } = channel;

    if (this.#dataChannels.has(label)) {
      this.#dataChannels.get(label)?.destroy();
    }

    const emitEvent = (
      name: keyof RemotePeerEvents,
      extra: Record<string, unknown> = {},
    ) => {
      const event = { id: this.#id, channel, label, ...extra };
      this.emit(["channel", name], {
        ...event,
        name,
      } as RemotePeerChannelEvent);
      log(`remote:${name}`, event);
    };

    const dc = new DataChannel({
      peer: this.id,
      channel,
      callback: {
        open: () => emitEvent("channel:open"),
        close: () => {
          emitEvent("channel:close");
          dc.destroy();
        },
        error: (err: unknown) => {
          const error = err instanceof Error ? err : new Error(String(err));
          emitEvent("channel:error", { error });
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
    });
    log("remote:channel:new", event);
  }

  /**
   * Sets up a media track by adding it to the corresponding stream
   * and emitting appropriate events.
   */
  #setupMediaTrack(track: MediaStreamTrack, stream: MediaStream): void {
    const label = this.#streamLabels.get(stream.id) || stream.id;

    const addTrack = () => {
      if (!this.#streams.has(label)) {
        this.#streams.set(label, stream);
        const event = { id: this.#id, stream, label };
        this.emit(["stream", "stream:add"], { ...event, name: "stream:add" });
        log("remote:stream:add", event);
      }

      const event = { id: this.#id, track, stream, label };
      this.emit(["track", "track:add"], { ...event, name: "track:add" });
      log("remote:track:add", event);
    };

    const removeTrack = () => {
      track.removeEventListener("ended", removeTrack);
      stream.removeTrack(track);

      const event = { id: this.#id, track, stream, label };
      this.emit(["track", "track:remove"], { ...event, name: "track:remove" });
      log("remote:track:remove", event);

      if (!stream.active || !stream.getTracks().length) {
        if (this.#streams.has(label)) {
          this.#streams.delete(label);
          const event = { id: this.#id, stream, label };
          this.emit(["stream", "stream:remove"], {
            ...event,
            name: "stream:remove",
          });
          log("remote:stream:remove", event);
        }
      }
    };

    track.addEventListener("ended", removeTrack);

    addTrack();
  }

  /**
   * Sets parameters for a media track by updating the sender parameters.
   */
  async #setSenderParameters(
    track: MediaStreamTrack,
    parameters?: { [key: string]: unknown },
  ): Promise<void> {
    if (!parameters) return;

    log("remote:setparameters", { id: this.#id, track, parameters });

    const senders = this.#connection.getSenders();
    const sender = senders.find((sender: RTCRtpSender) => {
      return sender.track && sender.track.id === track.id;
    });

    if (sender) {
      const params = sender.getParameters() || {};
      if (!params.encodings) return;
      for (const encoding of params.encodings) {
        if (!encoding) continue;
        Object.assign(encoding, parameters);
      }
      await sender.setParameters(params);
    }
  }
}

/**
 * Options for creating a {@link RemotePeer} instance.
 *
 * @internal
 * @group Remote Peers
 */
export interface RemotePeerOptions {
  /** Unique identifier for the remote peer. */
  id: string;
  /** Optional metadata associated with the peer. */
  metadata?: unknown;
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
 * Event emitted when a signal is received from the remote peer,
 * such as an offer, answer, or ICE candidate.
 *
 * @internal
 * @group Remote Peers
 */
export interface RemoteSignalEvent {
  /** Unique identifier for the remote peer. */
  id: string;
  /** Name of the event. */
  name: "offer" | "answer" | "candidate";
  /** Signal data, which can be an offer, answer, or ICE candidate. */
  data: RTCSessionDescriptionInit | RTCIceCandidateInit;
  /** Stream labels associated with the offer. */
  labels?: Record<string, string>;
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
  name:
    | "connection:new"
    | "connection:connecting"
    | "connection:connected"
    | "connection:disconnected"
    | "connection:failed"
    | "connection:closed";
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
  name:
    | "channel:new"
    | "channel:open"
    | "channel:close"
    | "channel:message"
    | "channel:error";
  /** Data channel associated with the event. */
  channel: RTCDataChannel;
  /** Label of the data channel. */
  label: string;
  /** Optional additional information associated with the message event. */
  info?: Record<string, unknown>;
  /** Data associated with the message event. */
  data?: unknown;
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
  name: "stream:add" | "stream:remove";
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
  name: "track:add" | "track:remove";
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
  name: "error";
  /** Error associated with the event. */
  error: PeerixError;
}

/**
 * Events emitted by {@link RemotePeer} instances.
 *
 * @group Remote Peers
 */
export interface RemotePeerEvents {
  /** Signal event. @internal */
  signal: [RemoteSignalEvent];
  /** General connection event. */
  connection: [RemotePeerConnectionEvent];
  /** New connection established. */
  "connection:new": [RemotePeerConnectionEvent];
  /** Connection is connecting. */
  "connection:connecting": [RemotePeerConnectionEvent];
  /** Connection is fully connected. */
  "connection:connected": [RemotePeerConnectionEvent];
  /** Connection disconnected. */
  "connection:disconnected": [RemotePeerConnectionEvent];
  /** Connection failed to establish. */
  "connection:failed": [RemotePeerConnectionEvent];
  /** Connection closed. */
  "connection:closed": [RemotePeerConnectionEvent];
  /** General data channel event. */
  channel: [RemotePeerChannelEvent];
  /** New data channel created. */
  "channel:new": [RemotePeerChannelEvent];
  /** Data channel opened. */
  "channel:open": [RemotePeerChannelEvent];
  /** Data channel closed. */
  "channel:close": [RemotePeerChannelEvent];
  /** Data channel message received. */
  "channel:message": [RemotePeerChannelEvent];
  /** Data channel error. */
  "channel:error": [RemotePeerChannelEvent];
  /** General media stream event. */
  stream: [RemotePeerStreamEvent];
  /** Media stream added. */
  "stream:add": [RemotePeerStreamEvent];
  /** Media stream removed. */
  "stream:remove": [RemotePeerStreamEvent];
  /** General media track event. */
  track: [RemotePeerTrackEvent];
  /** Media track added. */
  "track:add": [RemotePeerTrackEvent];
  /** Media track removed. */
  "track:remove": [RemotePeerTrackEvent];
  /** Error event. */
  error: [RemotePeerErrorEvent];
}
