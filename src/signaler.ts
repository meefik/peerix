import type { Driver } from "./drivers/driver.js";
import type { RemotePeer } from "./remote.js";
import { PeerixError } from "./error.js";
import log from "./utils/logger.js";
import { IceCandidateBatcher, IceCandidateQueue } from "./utils/ice.js";
import { base62ToBytes, bytesToBase62 } from "./utils/base62.js";
import { encode, decode } from "./utils/protobuf.js";
import { compress, decompress } from "./utils/compression.js";
import {
  encrypt,
  decrypt,
  generateKeyPair,
  generateDerivedKey,
  importPublicKey,
  exportPublicKey,
  sha256,
  PUBLIC_KEY_LENGTH,
} from "./utils/encryption.js";

/** Numeric identifiers for signaling message types. */
const MESSAGE_TYPE = {
  announce: 1,
  invoke: 2,
  offer: 3,
  answer: 4,
  candidate: 5,
} as const;

/** Protobuf field schema for encoding and decoding signaling packets. */
const PACKET_SCHEMA: Record<string, { id: number; type: "uint32" | "bytes" }> =
  {
    type: { id: 1, type: "uint32" },
    flags: { id: 2, type: "uint32" },
    sender: { id: 3, type: "bytes" },
    payload: { id: 4, type: "bytes" },
  };

/**
 * Manages signaling exchanges between peers over a transport driver.
 *
 * Handles subscribing and publishing on namespaces, encoding and decoding
 * signals with optional compression and encryption, and coordinating ICE
 * candidate batching so they are flushed in order.
 */
export class Signaler {
  #active: boolean;
  #driver: Driver;
  #id: string;
  #room: string;
  #metadata?: unknown;
  #namespaceHashing: boolean;
  #signalingCompression: boolean;
  #signalingEncryption: boolean;
  #iceCandidateDebounce: number;
  #createRemotePeer: (options: {
    id: string;
    metadata?: unknown;
  }) => Promise<RemotePeer | void>;
  #getRemotePeer: (id: string) => RemotePeer | void;
  #onError: (error: PeerixError) => void;
  #keyPair?: CryptoKeyPair;
  #sharedKeys: Map<string, CryptoKey>;
  #candidateQueue: IceCandidateQueue;
  #candidateBatchers: Map<string, IceCandidateBatcher>;
  #signalHandler: (data: number[]) => Promise<void>;
  #driverActiveHandler: () => void;
  #driverErrorHandler: (err: unknown) => void;

  /**
   * Creates a new Signaler instance.
   *
   * @param options Configuration for the signaler.
   * @param options.driver Transport driver used to publish and subscribe on signaling namespaces.
   * @param options.namespaceHashing Whether namespace hashing is enabled.
   * @param options.signalingCompression Whether signaling payload compression is enabled.
   * @param options.signalingEncryption Whether signaling payload encryption is enabled.
   * @param options.iceCandidateDebounce Debounce delay in milliseconds before flushing batched ICE candidates.
   * @param options.createRemotePeer Creates a remote peer when an incoming offer or invoke is received.
   * @param options.getRemotePeer Looks up an existing remote peer by ID.
   * @param options.onError Emits a signaling error back to the owning peer.
   */
  constructor(options: {
    driver: Driver;
    namespaceHashing: boolean;
    signalingCompression: boolean;
    signalingEncryption: boolean;
    iceCandidateDebounce: number;
    createRemotePeer: (options: {
      id: string;
      metadata?: unknown;
    }) => Promise<RemotePeer | void>;
    getRemotePeer: (id: string) => RemotePeer | void;
    onError: (error: PeerixError) => void;
  }) {
    const {
      driver,
      namespaceHashing,
      signalingCompression,
      signalingEncryption,
      iceCandidateDebounce,
      createRemotePeer,
      getRemotePeer,
      onError,
    } = options;

    this.#active = false;
    this.#id = "";
    this.#room = "";
    this.#driver = driver;
    this.#namespaceHashing = namespaceHashing;
    this.#signalingCompression = signalingCompression;
    this.#signalingEncryption = signalingEncryption;
    this.#iceCandidateDebounce = iceCandidateDebounce;
    this.#createRemotePeer = createRemotePeer;
    this.#getRemotePeer = getRemotePeer;
    this.#onError = onError;

    this.#sharedKeys = new Map();
    this.#candidateQueue = new IceCandidateQueue();
    this.#candidateBatchers = new Map();
    this.#signalHandler = this.#handleMessage.bind(this);
    this.#driverActiveHandler = () => {
      if (!this.#room) return;

      void this.#publish({
        type: MESSAGE_TYPE.announce,
        namespace: [this.#room],
      });
    };
    this.#driverErrorHandler = (err: unknown) => {
      this.#emitError(err);
    };
  }

  /**
   * Subscribes to signaling namespaces and announces presence.
   *
   * Registers event listeners on the driver for lifecycle changes, subscribes
   * to every prefix of `[room, id]`, and publishes an initial announcement.
   *
   * @param room The room name to join.
   * @param metadata Optional metadata associated with the room.
   * @returns The generated ID for the local peer.
   */
  async register(room: string, metadata?: unknown): Promise<string> {
    if (this.#active) return this.#id;
    this.#active = true;

    this.#id = await this.#generateId();
    this.#room = room;
    this.#metadata = metadata;

    this.#driver.on("active", this.#driverActiveHandler);
    this.#driver.on("error", this.#driverErrorHandler);

    try {
      await this.#subscribe();
    } catch (err) {
      this.#id = "";
      this.#room = "";
      this.#metadata = undefined;

      this.#driver.off("active", this.#driverActiveHandler);
      this.#driver.off("error", this.#driverErrorHandler);

      this.#active = false;

      throw err;
    }

    void this.#publish({
      type: MESSAGE_TYPE.announce,
      namespace: [this.#room],
    });

    return this.#id;
  }

  /**
   * Unsubscribes from all signaling namespaces and removes driver listeners.
   */
  async unregister(): Promise<void> {
    if (!this.#active) return;

    this.#id = "";
    this.#room = "";
    this.#metadata = undefined;
    this.#keyPair = undefined;

    this.#driver.off("active", this.#driverActiveHandler);
    this.#driver.off("error", this.#driverErrorHandler);

    try {
      await this.#unsubscribe();
    } finally {
      this.#candidateBatchers.forEach((batcher) => batcher.clear());
      this.#candidateBatchers.clear();
      this.#candidateQueue.clear();
      this.#sharedKeys.clear();

      this.#active = false;
    }
  }

  /**
   * Binds signal handlers to a remote peer so SDP exchanges and ICE candidates
   * flow through the signaling channel.
   */
  #setupRemotePeer(remote: RemotePeer): void {
    const { id } = remote;

    const batcher = new IceCandidateBatcher({
      delay: this.#iceCandidateDebounce,
      onFlush: (candidates) => {
        if (remote.state === "closed" || !candidates.length) return;

        void this.#publish({
          type: MESSAGE_TYPE.candidate,
          namespace: [this.#room, id],
          message: candidates,
          sender: id,
        });
      },
    });

    this.#candidateBatchers.get(id)?.clear();
    this.#candidateBatchers.set(id, batcher);

    remote.on("signal", (e) => {
      const { name, data } = e;

      if (name === "candidate") {
        batcher.push(data as RTCIceCandidateInit);
        return;
      }

      const type = MESSAGE_TYPE[name];
      if (!type || !this.#room) return;

      void this.#publish({
        type,
        namespace: [this.#room, id],
        message: name === "offer" ? [data, this.#metadata] : [data],
        sender: id,
      });
    });

    remote.on("connection:failed", () => {
      if (!this.#room) return;

      void this.#publish({
        type: MESSAGE_TYPE.invoke,
        namespace: [this.#room, id],
        message: [this.#metadata],
        sender: id,
        jitter: 1000,
      });
    });

    remote.on("connection:closed", () => {
      this.#candidateBatchers.get(id)?.clear();
      this.#candidateBatchers.delete(id);
      this.#candidateQueue.clear(id);
      this.#sharedKeys.delete(id);
    });
  }

  /**
   * Generates a unique peer identifier.
   *
   * When signaling encryption is enabled, returns the exported public key
   * encoded as base62. Otherwise, returns a random byte sequence of the
   * same length.
   */
  async #generateId(): Promise<string> {
    if (this.#signalingEncryption) {
      this.#keyPair = await generateKeyPair();
      const publicKey = await exportPublicKey(this.#keyPair.publicKey);
      return bytesToBase62(publicKey);
    }
    const randomKey = crypto.getRandomValues(new Uint8Array(PUBLIC_KEY_LENGTH));
    return bytesToBase62(randomKey);
  }

  /**
   * Escapes namespace segments for safe use with the transport driver.
   *
   * Applies SHA-256 hashing when namespace hashing is enabled, otherwise
   * replaces non-alphanumeric characters (except `_` and `-`) with underscores.
   */
  async #escapeNamespace(namespace: string[]): Promise<string[]> {
    return this.#namespaceHashing
      ? await Promise.all(namespace.map((value) => sha256(value)))
      : namespace.map((value) => value.replace(/[^a-zA-Z0-9_-]/gu, "_"));
  }

  /**
   * Encodes a signaling message into a binary packet.
   *
   * Serializes the message to JSON, optionally compresses and encrypts the
   * payload, then wraps it in a protobuf-encoded packet with type, flags,
   * and sender fields.
   */
  async #encode(
    type: number,
    message: unknown,
    encryptionKey?: CryptoKey,
  ): Promise<Uint8Array> {
    let payload: Uint8Array = message
      ? new TextEncoder().encode(JSON.stringify(message))
      : new Uint8Array();

    let compressed = false;
    let encrypted = false;

    if (payload.byteLength > 0) {
      if (this.#signalingCompression) {
        const compressedMessage = await compress(payload);
        if (compressedMessage.byteLength < payload.byteLength) {
          payload = compressedMessage;
          compressed = true;
        }
      }

      if (this.#signalingEncryption) {
        if (!encryptionKey) throw new Error("Encryption key not found");
        payload = await encrypt(payload, encryptionKey);
        encrypted = true;
      }
    }

    const sender = base62ToBytes(this.#id);
    const flags = (compressed ? 1 : 0) | (encrypted ? 2 : 0);

    const buffer = encode({ type, flags, sender, payload }, PACKET_SCHEMA);
    if (!buffer) throw new Error("Failed to encode signal");

    return buffer;
  }

  /**
   * Decodes a binary packet back into a signaling message.
   *
   * Parses the protobuf packet, derives or retrieves the shared encryption
   * key when needed, then decrypts and decompresses the payload.
   */
  async #decode(
    data: number[],
  ): Promise<Partial<{ id: string; type: number; message: unknown[] }>> {
    const buffer = new Uint8Array(data);

    const decoded = decode(buffer, PACKET_SCHEMA);
    if (!decoded) return {};

    const { type, flags, sender, payload } = decoded as {
      type: number;
      flags: number;
      sender: Uint8Array;
      payload: Uint8Array;
    };
    const id = bytesToBase62(sender);
    // Do not process the packet if it is from ourselves.
    if (!id || this.#id === id) return {};

    const compressed = (flags & 1) !== 0;
    const encrypted = (flags & 2) !== 0;

    let encryptionKey = this.#sharedKeys.get(id);
    if (!encryptionKey && this.#signalingEncryption) {
      if (!this.#keyPair) {
        throw new Error("Key pair not found for decryption");
      }
      const publicKey = await importPublicKey(sender);
      encryptionKey = await generateDerivedKey(
        this.#keyPair.privateKey,
        publicKey,
      );
      this.#sharedKeys.set(id, encryptionKey);
    }

    let decodedPayload = payload;
    if (payload.byteLength > 0) {
      if (encryptionKey) {
        if (!encrypted) throw new Error("Payload is not encrypted");
        decodedPayload = await decrypt(payload, encryptionKey);
      }
      if (compressed) {
        decodedPayload = await decompress(decodedPayload);
      }
    }

    const message = decodedPayload.byteLength
      ? JSON.parse(new TextDecoder().decode(decodedPayload))
      : [];

    return { id, type, message };
  }

  /**
   * Subscribes to all prefix namespaces for this peer.
   *
   * For a namespace `[room, id]`, subscribes to `[room]` and `[room, id]`
   * so that both room-level and peer-level messages are received.
   */
  async #subscribe(): Promise<void> {
    const namespace = [this.#room, this.#id];
    for (let i = 0; i < namespace.length; i++) {
      const part = namespace.slice(0, i + 1);
      const escaped = await this.#escapeNamespace(part);

      log("signal:subscribe", { id: this.#id, namespace: escaped });

      await this.#driver.subscribe(escaped, this.#signalHandler);
    }
  }

  /**
   * Unsubscribes from all prefix namespaces for this peer.
   */
  async #unsubscribe(): Promise<void> {
    const namespace = [this.#room, this.#id];
    for (let i = 0; i < namespace.length; i++) {
      const part = namespace.slice(0, i + 1);
      const escaped = await this.#escapeNamespace(part);

      log("signal:unsubscribe", { id: this.#id, namespace: escaped });

      await this.#driver.unsubscribe(escaped, this.#signalHandler);
    }
  }

  /**
   * Encodes and publishes a signaling message to the transport driver.
   *
   * Applies optional jitter delay before publishing. Silently aborts if
   * the driver becomes inactive during the delay.
   */
  async #publish(options: SignalPublishOptions): Promise<void> {
    if (!this.#id || !this.#driver.active) return;

    try {
      const { type, namespace, message, jitter = 0, sender } = options;

      const escaped = await this.#escapeNamespace(namespace);
      const encryptionKey = sender ? this.#sharedKeys.get(sender) : undefined;
      const buffer = await this.#encode(type, message, encryptionKey);

      const delay = Math.floor(Math.random() * jitter);
      await new Promise((resolve) => setTimeout(resolve, delay));

      if (!this.#id || !this.#driver.active) return;

      log("signal:publish", {
        id: this.#id,
        type,
        namespace: escaped,
        message,
      });

      await this.#driver.publish(escaped, Array.from(buffer));
    } catch (err) {
      this.#emitError(err);
    }
  }

  /**
   * Processes an incoming signaling message.
   *
   * Decodes the packet and dispatches it based on type: announcements
   * trigger an invoke reply, invokes create a new remote peer, offers
   * and answers signal the remote peer and flush queued candidates,
   * and candidate messages are either queued or signaled directly.
   */
  async #handleMessage(data: number[]): Promise<void> {
    if (!this.#id) return;

    try {
      const { id, type, message = [] } = await this.#decode(data);
      const validType = typeof type === "number";
      const validMessage = Array.isArray(message);
      if (!id || !validType || !validMessage) return;

      log("signal:receive", {
        id: this.#id,
        type,
        from: id,
        message,
      });

      if (type === MESSAGE_TYPE.announce) {
        void this.#publish({
          type: MESSAGE_TYPE.invoke,
          namespace: [this.#room, id],
          message: [this.#metadata],
          sender: id,
        });

        return;
      }

      if (type === MESSAGE_TYPE.invoke) {
        const [metadata] = message as [unknown];
        const remote = await this.#createRemotePeer({ id, metadata });
        if (remote) this.#setupRemotePeer(remote);

        return;
      }

      if (type === MESSAGE_TYPE.offer) {
        const [description, metadata] = message as [
          RTCSessionDescriptionInit,
          unknown,
        ];
        let remote = this.#getRemotePeer(id);
        if (!remote) {
          remote = await this.#createRemotePeer({ id, metadata });
          if (!remote) return;
          this.#setupRemotePeer(remote);
        }

        await remote.signal(description);

        for (const candidate of this.#candidateQueue.pull(id, description)) {
          try {
            await remote.signal(candidate);
          } catch (err) {
            this.#emitError(err);
          }
        }

        return;
      }

      if (type === MESSAGE_TYPE.answer) {
        const [description] = message as [RTCSessionDescriptionInit];
        const remote = this.#getRemotePeer(id);
        if (!remote) return;

        await remote.signal(description);

        for (const candidate of this.#candidateQueue.pull(id, description)) {
          try {
            await remote.signal(candidate);
          } catch (err) {
            this.#emitError(err);
          }
        }

        return;
      }

      if (type === MESSAGE_TYPE.candidate) {
        const [...candidates] = message as RTCIceCandidateInit[];
        const remote = this.#getRemotePeer(id);

        const description = remote?.connection.remoteDescription || undefined;

        for (const candidate of candidates) {
          const queued = this.#candidateQueue.push(id, candidate, description);
          if (!remote || queued) continue;
          try {
            await remote.signal(candidate);
          } catch (err) {
            this.#emitError(err);
          }
        }

        return;
      }
    } catch (err) {
      this.#emitError(err);
    }
  }

  /**
   * Wraps an error in a PeerixError and emits it to the owning peer.
   */
  #emitError(err: unknown): void {
    const error = new PeerixError(err, "SIGNALING_ERROR");
    this.#onError(error);

    log("signal:error", { id: this.#id, error });
  }
}

/** Options for publishing a signaling message. */
export interface SignalPublishOptions {
  /** Message type identifier. */
  type: number;
  /** Target namespace path segments. */
  namespace: string[];
  /** Payload data to send. */
  message?: unknown[];
  /** ID of the intended recipient for encryption key lookup. */
  sender?: string;
  /** Random delay in milliseconds before publishing (for jitter). */
  jitter?: number;
}
