import type { Driver } from './drivers/driver.js';
import { MemoryDriver } from './drivers/memory.js';

// All peers without a driver will share the same in-memory signaling bus
const defaultDriver = new MemoryDriver();

/**
 * Signaler class for managing signaling between peers.
 */
export class Signaler {
  #driver: Driver;
  #handler: (message?: any) => void;
  #compress: boolean;
  #encrypt: boolean;
  #hash: boolean;
  #encryptionKey: string;
  #cryptoKey?: CryptoKey;

  /**
   * Creates a new Signaler instance.
   *
   * @param options Options for the Signaler.
   * @param options.driver Optional driver for signaling. If not provided, a default in-memory driver will be used.
   * @param options.handler Handler function that will be called when a signaling message is received.
   * @param options.compress Whether to compress signaling messages using gzip. Defaults to `true`.
   * @param options.hash Whether to hash namespace parts using SHA-256 before dispatching or subscribing. Defaults to `false`.
   * @param options.encrypt Whether to encrypt signaling messages using AES-GCM. Defaults to `false`.
   * @param options.encryptionKey The secret key used for AES-GCM encryption and decryption. Required when `encrypt` is `true`.
   */
  constructor(options: SignalerOptions) {
    const {
      driver = defaultDriver,
      handler,
      compress = true,
      encrypt = false,
      hash = false,
      encryptionKey = ''
    } = options;
    this.#driver = driver;
    this.#compress = compress;
    this.#encrypt = encrypt;
    this.#hash = hash;
    this.#encryptionKey = encryptionKey;
    this.#handler = async (message: Uint8Array) => {
      if (this.#encrypt) {
        message = await this.#decryptMessage(message);
      }
      if (this.#compress) {
        message = await this.#decompressMessage(message);
      }
      message = JSON.parse(new TextDecoder().decode(message));
      return handler(message);
    };
  }

  /**
   * Subscribes to the specified namespaces for signaling messages.
   * 
   * @param namespaces Namespaces to subscribe to.
   */
  async subscribe(...namespaces: string[][]) {
    for (let namespace of namespaces) {
      if (this.#hash) {
        namespace = await this.#sha256(namespace);
      }
      await this.#driver.subscribe(namespace, this.#handler);
    }
  }

  /**
   * Unsubscribes from the specified namespaces for signaling messages.
   * 
   * @param namespaces Namespaces to unsubscribe from.
   */
  async unsubscribe(...namespaces: string[][]) {
    for (let namespace of namespaces) {
      if (this.#hash) {
        namespace = await this.#sha256(namespace);
      }
      await this.#driver.unsubscribe(namespace, this.#handler);
    }
  }

  /**
   * Dispatches a signaling message to the specified namespace.
   * 
   * @param namespace The namespace to dispatch the message to.
   * @param message The message to dispatch.
   */
  async dispatch(namespace: string[], message?: any) {
    if (this.#hash) {
      namespace = await this.#sha256(namespace);
    }
    const text = JSON.stringify(message);
    message = new TextEncoder().encode(JSON.stringify(message));

    if (this.#compress) {
      message = await this.#compressMessage(message);
    }
    if (this.#encrypt) {
      message = await this.#encryptMessage(message);
    }
    await this.#driver.dispatch(namespace, message);
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
  /** Called when a signaling message is received. */
  handler: (message?: any) => void;
  /** Compress messages with deflate. Defaults to `true`. */
  compress?: boolean;
  /** Hash namespace parts with SHA-256. Defaults to `false`. */
  hash?: boolean;
  /** Encrypt messages with AES-GCM. Defaults to `false`. */
  encrypt?: boolean;
  /** Secret key for AES-GCM encryption. Required when `encrypt` is `true`. */
  encryptionKey?: string;
}
