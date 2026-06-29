import type { Driver } from "./drivers/driver.js";
import log from "./utils/logger.js";
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

/** Protobuf field schema for encoding and decoding signaling packets. */
const PACKET_SCHEMA: Record<string, { id: number; type: "uint32" | "bytes" }> =
  {
    type: { id: 1, type: "uint32" },
    flags: { id: 2, type: "uint32" },
    sender: { id: 3, type: "bytes" },
    payload: { id: 4, type: "bytes" },
  };

/** Numeric identifiers for signaling message types. */
export const SIGNAL_TYPE = {
  announce: 1,
  invoke: 2,
  offer: 3,
  answer: 4,
  candidate: 5,
} as const;

/**
 * Manages signaling exchanges between peers over a transport driver.
 *
 * Handles subscribing to and publishing on namespaces, encoding and decoding
 * signals with optional compression and encryption.
 */
export class Signaler {
  #active: boolean;
  #id: string;
  #driver: Driver;
  #namespaceHashing: boolean;
  #signalingCompression: boolean;
  #signalingEncryption: boolean;
  #onMessage: (message: SignalMessage) => Promise<void>;
  #onError: (error: unknown) => void;
  #keyPair: CryptoKeyPair | null;
  #sharedKeys: Map<string, CryptoKey>;
  #signalHandler: (data: number[]) => Promise<void>;

  /**
   * Creates a new {@link Signaler} instance.
   *
   * @param options Configuration for the signaler.
   */
  constructor(options: SignalerOptions) {
    const {
      driver,
      namespaceHashing,
      signalingCompression,
      signalingEncryption,
      onMessage,
      onError,
    } = options;

    this.#active = false;
    this.#id = "";
    this.#driver = driver;
    this.#namespaceHashing = namespaceHashing;
    this.#signalingCompression = signalingCompression;
    this.#signalingEncryption = signalingEncryption;
    this.#onMessage = onMessage;
    this.#onError = onError;
    this.#keyPair = null;
    this.#sharedKeys = new Map();
    this.#signalHandler = this.#handleMessage.bind(this);
  }

  /**
   * Subscribes to signaling namespaces and announces presence.
   *
   * Registers a message handler on the driver for the room namespace and the local
   * peer ID namespace, then publishes an initial announcement.
   *
   * @param room The room name to join.
   * @returns The generated ID for the local peer.
   */
  async subscribe(room: string): Promise<string> {
    const id = await this.#generateId();
    if (this.#active) return this.#id;

    this.#id = id;
    this.#active = true;

    const subscribedNamespaces: string[] = [];
    try {
      const namespaces = [room, id];
      for (const namespace of namespaces) {
        const ns = await this.#escapeNamespace(namespace);
        log("signaler:subscribe", { id, namespace: ns });
        await this.#driver.subscribe(ns, this.#signalHandler);
        subscribedNamespaces.push(ns);
      }
    } catch (err) {
      for (const ns of subscribedNamespaces) {
        try {
          await this.#driver.unsubscribe(ns, this.#signalHandler);
        } catch {}
      }
      this.#id = "";
      this.#active = false;
      throw err;
    }

    return id;
  }

  /**
   * Unsubscribes from signaling namespaces and removes driver listeners.
   *
   * @param room The room namespace to unsubscribe from alongside the local peer ID namespace.
   */
  async unsubscribe(room: string): Promise<void> {
    if (!this.#active) return;

    const id = this.#id;

    try {
      const namespaces = [room, id];
      for (const namespace of namespaces) {
        const ns = await this.#escapeNamespace(namespace);
        log("signaler:unsubscribe", { id, namespace: ns });
        await this.#driver.unsubscribe(ns, this.#signalHandler);
      }
    } finally {
      this.#id = "";
      this.#keyPair = null;
      this.#sharedKeys.clear();
      this.#active = false;
    }
  }

  /**
   * Encodes and publishes a signaling message to the transport driver.
   *
   * Applies optional jitter delay before publishing. Silently aborts if
   * the driver becomes inactive during the delay.
   *
   * @param data The signal message to be published.
   * @param options Optional configuration for the publish operation, including a jitter delay duration in milliseconds.
   */
  async publish(
    data: SignalMessage,
    options?: { jitter: number },
  ): Promise<void> {
    const { jitter = 0 } = options ?? {};
    const { type, id, message } = data;

    const delay = Math.floor(Math.random() * jitter);
    if (delay > 0) {
      await new Promise((resolve) => setTimeout(resolve, delay));
    }

    if (!this.#active || !this.#driver.active) return;

    const namespace = await this.#escapeNamespace(id);
    const encryptionKey = this.#sharedKeys.get(id);
    const buffer = await this.#encode(type, message, encryptionKey);

    log("signaler:publish", {
      id: this.#id,
      type,
      namespace,
      message,
    });

    await this.#driver.publish(namespace, Array.from(buffer));
  }

  /**
   * Removes a derived shared key for the specified peer.
   *
   * @param id The remote peer identifier whose key should be cleared.
   */
  reset(id: string) {
    this.#sharedKeys.delete(id);
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
  async #escapeNamespace(namespace: string): Promise<string> {
    return this.#namespaceHashing
      ? await sha256(namespace)
      : namespace.replace(/[^a-zA-Z0-9_-]/gu, "_");
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

    const sender = base62ToBytes(this.#id, PUBLIC_KEY_LENGTH);
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
  ): Promise<{ id: string; type: number; message: unknown[] } | null> {
    const buffer = new Uint8Array(data);

    const decoded = decode(buffer, PACKET_SCHEMA);
    if (!decoded) throw new Error("Invalid packet");

    const { type, flags, sender, payload } = decoded as {
      type: number;
      flags: number;
      sender: Uint8Array;
      payload: Uint8Array;
    };
    const id = bytesToBase62(sender);
    // Do not process the packet if it is from ourselves.
    if (!id || this.#id === id) return null;

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
   * Processes an incoming signaling message from the driver.
   *
   * Decodes and validates the packet, then forwards it to the registered
   * message callback. Errors are caught and passed to the error handler.
   */
  async #handleMessage(data: number[]): Promise<void> {
    if (!this.#id) return;

    try {
      const decoded = await this.#decode(data);
      if (!decoded) return;

      const { id, type, message = [] } = decoded;
      const validType =
        typeof type === "number" &&
        (Object.values(SIGNAL_TYPE) as number[]).includes(type);
      const validMessage = Array.isArray(message);
      if (!id || !validType || !validMessage) {
        throw new Error("Invalid signaling message");
      }

      const messageType = type as MessageType;

      log("signaler:receive", {
        id: this.#id,
        type: messageType,
        from: id,
        message,
      });

      await this.#onMessage({ type: messageType, id, message });
    } catch (error) {
      log("signaler:error", { id: this.#id, error });
      this.#onError(error);
    }
  }
}

/** Numeric type value for signaling messages, derived from `SIGNAL_TYPE`. */
export type MessageType = (typeof SIGNAL_TYPE)[keyof typeof SIGNAL_TYPE];

/** A signaling message sent or received between peers. */
export interface SignalMessage {
  type: MessageType;
  id: string;
  message?: unknown[];
}

/** Configuration options for constructing a {@link Signaler}. */
export interface SignalerOptions {
  driver: Driver;
  namespaceHashing: boolean;
  signalingCompression: boolean;
  signalingEncryption: boolean;
  onMessage: (message: SignalMessage) => Promise<void>;
  onError: (error: unknown) => void;
}
