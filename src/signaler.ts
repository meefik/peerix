import type { Driver } from './drivers/driver.js';
import log from './utils/logger.js';
import { MemoryDriver } from './drivers/memory.js';

// All peers without a driver will share the same in-memory signaling bus
const defaultDriver = new MemoryDriver();

/**
 * Manages signaling between peers.
 */
export class Signaler {
  #driver: Driver;
  #compression: boolean;
  #hashing: boolean;
  #encryptionKey: string;
  #cryptoKey?: CryptoKey;
  #activeHandler: () => void;
  #inactiveHandler: () => void;
  #errorHandler: (error: any) => void;
  #signalHandler: (message?: any) => void;

  /**
   * Creates a new {@link Signaler} instance.
   *
   * @param options Options for the Signaler.
   */
  constructor(options: SignalerOptions) {
    const {
      driver = defaultDriver,
      compression = true,
      hashing = false,
      encryptionKey = '',
      onActive,
      onInactive,
      onError,
      onSignal,
    } = options;
    this.#driver = driver;
    this.#compression = compression;
    this.#hashing = hashing;
    this.#encryptionKey = encryptionKey;
    this.#signalHandler = async (payload: number[]) => {
      try {
        let buffer = new Uint8Array(payload);
        if (this.#encryptionKey) {
          buffer = await this.#decryptMessage(buffer);
        }
        if (this.#compression) {
          buffer = await this.#decompressMessage(buffer);
        }
        const message = JSON.parse(new TextDecoder().decode(buffer));
        return onSignal(message);
      }
      catch (err) {
        onError(err);
      }
    };
    this.#activeHandler = () => onActive();
    this.#inactiveHandler = () => onInactive();
    this.#errorHandler = (error: any) => onError(error);
  }

  /**
   * Subscribes to the specified namespaces for signaling messages.
   * 
   * @param namespaces Namespaces to subscribe to.
   */
  async subscribe(...namespaces: string[][]) {
    this.#driver.on('active', this.#activeHandler);
    this.#driver.on('inactive', this.#inactiveHandler);
    this.#driver.on('error', this.#errorHandler);

    for (let namespace of namespaces) {
      if (this.#hashing) {
        namespace = await this.#sha256(namespace);
      }
      await this.#driver.subscribe(namespace, this.#signalHandler);
    }
  }

  /**
   * Unsubscribes from the specified namespaces for signaling messages.
   * 
   * @param namespaces Namespaces to unsubscribe from.
   */
  async unsubscribe(...namespaces: string[][]) {
    this.#driver.off('active', this.#activeHandler);
    this.#driver.off('inactive', this.#inactiveHandler);
    this.#driver.off('error', this.#errorHandler);

    for (let namespace of namespaces) {
      if (this.#hashing) {
        namespace = await this.#sha256(namespace);
      }
      await this.#driver.unsubscribe(namespace, this.#signalHandler);
    }
  }

  /**
   * Dispatches a signaling message to the specified namespace.
   * 
   * @param namespace The namespace to dispatch the message to.
   * @param message The message to dispatch.
   */
  async dispatch(namespace: string[], message?: any) {
    if (!this.#driver.active) return;

    if (this.#hashing) {
      namespace = await this.#sha256(namespace);
    }

    let buffer = new TextEncoder().encode(JSON.stringify(message));
    if (this.#compression) {
      buffer = await this.#compressMessage(buffer);
    }
    if (this.#encryptionKey) {
      buffer = await this.#encryptMessage(buffer);
    }

    await this.#driver.dispatch(namespace, Array.from(buffer));
  }

  /**
   * Hashes each part of the namespace using SHA-256 and encodes the result in base62 format.
   *
   * @param namespace The namespace parts to hash.
   * @returns The hashed namespace parts.
   */
  async #sha256(namespace: string[]): Promise<string[]> {
    const alphabet = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
    return await Promise.all(namespace.map(async (part) => {
      const data = new TextEncoder().encode(part);
      const hashBuffer = await crypto.subtle.digest('SHA-256', data);
      const digits = Array.from(new Uint8Array(hashBuffer));
      let result = '';
      while (digits.some((d) => d !== 0)) {
        let remainder = 0;
        for (let i = 0; i < digits.length; i++) {
          const cur = remainder * 256 + digits[i];
          digits[i] = Math.floor(cur / 62);
          remainder = cur % 62;
        }
        result = alphabet[remainder] + result;
      }
      return result || '0';
    }));
  }

  /**
   * Derives a CryptoKey from a secret string using SHA-256 as a key derivation
   * step and AES-GCM as the algorithm.
   *
   * @param secret The secret string to derive the key from.
   * @returns A CryptoKey suitable for AES-GCM encryption and decryption.
   */
  async #createCryptoKey(secret: string): Promise<CryptoKey> {
    const secretHash = await crypto.subtle.digest(
      'SHA-256',
      new TextEncoder().encode(secret),
    );
    return await crypto.subtle.importKey(
      'raw',
      secretHash,
      { name: 'AES-GCM' },
      false,
      ['encrypt', 'decrypt'],
    );
  }

  /**
   * Encrypts a message using AES-GCM with the configured encryption key.
   * Prepends a random 12-byte IV to the ciphertext.
   *
   * @param decrypted The plaintext bytes to encrypt.
   * @returns The encrypted bytes (IV + ciphertext).
   */
  async #encryptMessage(decrypted: Uint8Array): Promise<Uint8Array> {
    if (!this.#cryptoKey) {
      this.#cryptoKey = await this.#createCryptoKey(this.#encryptionKey);
    }
    const iv = crypto.getRandomValues(new Uint8Array(12));
    const ciphertext = new Uint8Array(
      await crypto.subtle.encrypt(
        { name: 'AES-GCM', iv },
        this.#cryptoKey,
        new Uint8Array(decrypted)
      ),
    );
    const data = new Uint8Array(iv.byteLength + ciphertext.byteLength);
    data.set(iv, 0);
    data.set(ciphertext, iv.byteLength);
    return data;
  }

  /**
   * Compresses a message using deflate via the CompressionStream API.
   * Falls back to returning the original if the API is unavailable.
   *
   * @param uncompressed The bytes to compress.
   * @returns The compressed bytes.
   */
  async #compressMessage(uncompressed: Uint8Array): Promise<Uint8Array> {
    if (!('CompressionStream' in window)) return uncompressed;
    const stream = new Blob([uncompressed]).stream();
    const compressedStream = stream.pipeThrough(new CompressionStream('deflate'));
    const response = new Response(compressedStream);
    const arrayBuffer = await response.arrayBuffer();
    return new Uint8Array(arrayBuffer);
  }

  /**
   * Decompresses a deflate-compressed message via the DecompressionStream API.
   * Falls back to returning the original if the API is unavailable.
   *
   * @param compressed The compressed bytes.
   * @returns The decompressed bytes.
   */
  async #decompressMessage(compressed: Uint8Array): Promise<Uint8Array> {
    if (!('DecompressionStream' in window)) return compressed;
    const stream = new Blob([compressed]).stream();
    const decompressedStream = stream.pipeThrough(new DecompressionStream('deflate'));
    const buffer = await new Response(decompressedStream).arrayBuffer();
    return new Uint8Array(buffer);
  }

  /**
   * Decrypts a message encrypted with AES-GCM.
   * Expects the first 12 bytes to be the IV.
   *
   * @param encrypted The encrypted bytes (IV + ciphertext).
   * @returns The decrypted plaintext bytes.
   */
  async #decryptMessage(encrypted: Uint8Array): Promise<Uint8Array> {
    if (!this.#cryptoKey) {
      this.#cryptoKey = await this.#createCryptoKey(this.#encryptionKey);
    }
    const iv = encrypted.slice(0, 12);
    const ct = new Uint8Array(encrypted.slice(12));
    const decrypted = await crypto.subtle.decrypt(
      { name: 'AES-GCM', iv },
      this.#cryptoKey,
      ct
    );
    return new Uint8Array(decrypted);
  }
}

/**
 * Options for configuring a {@link Signaler} instance.
 */
export interface SignalerOptions {
  /** Optional signaling driver. Defaults to an in-memory driver. */
  driver?: Driver;
  /** Compress messages with deflate. Defaults to `true`. */
  compression?: boolean;
  /** Hash namespace parts with SHA-256. Defaults to `false`. */
  hashing?: boolean;
  /** Encrypt messages with AES-GCM. Disabled by default. */
  encryptionKey?: string;
  /** Called when the signaling connection becomes active. */
  onActive: () => void;
  /** Called when the signaling connection becomes inactive. */
  onInactive: () => void;
  /** Called when a signaling error occurs. */
  onError: (error: any) => void;
  /** Called when a signaling message is received. */
  onSignal: (message?: any) => void;
}
