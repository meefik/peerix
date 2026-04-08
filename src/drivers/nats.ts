import type { SignalingDriver } from '../types/signaling.js';

/**
 * NATS-based signaling driver for inter-process communication.
 *
 * This driver uses [NATS](https://nats.io/) as the underlying messaging system, 
 * allowing for distributed signaling across multiple browsers and devices. 
 * It supports optional encryption of messages using AES-GCM with a secret key 
 * and namespace hashing using SHA-256 for obfuscation.
 * 
 * > This driver requires the `nats.ws` library for WebSocket-based NATS connections 
 * > directly in the browser.
 * 
 * @group Drivers
 * @example
 * ```javascript
 * import { connect } from 'https://esm.sh/nats.ws';
 * 
 * const driver = new NatsDriver({
 *   connect: async () => await connect({ servers: ['wss://demo.nats.io:8443'] }),
 *   secret: 'your-secret-key',
 * });
 * 
 * await driver.open();
 * ```
 */
export class NatsDriver implements SignalingDriver {
  #events: Map<string, Map<(...args: any[]) => void, any>>;
  #connect: (config?: any) => Promise<any>;
  #nc?: any;
  #prefix: string;
  #secret?: string;
  #cryptoKey?: CryptoKey;

  /**
   * Create a new instance of the driver.
   *
   * @param options Configuration options for the driver.
   * @param options.connect A function that returns a promise resolving to a NATS connection instance.
   * @param options.secret An optional secret key for encrypting messages.
   * @param options.prefix An optional prefix for NATS subjects.
   */
  constructor(options: { connect: (config?: any) => Promise<any>; secret?: string; prefix?: string }) {
    const { connect, secret, prefix = '' } = options || {};
    this.#connect = connect;
    this.#secret = secret;
    this.#prefix = prefix;
    this.#events = new Map();
  }

  /**
   * Opens the connection to the NATS server
   * and initializes encryption if a secret is provided.
   * 
   * @param config Optional configuration options.
   */
  async open(config?: any) {
    this.#nc = await this.#connect(config);
    if (this.#secret) {
      this.#cryptoKey = await createEncryptionKey(this.#secret);
    }
  }

  /**
   * Closes the connection to the NATS server.
   */
  async close() {
    if (this.#nc) {
      await this.#nc.close();
      this.#nc = undefined;
    }
    if (this.#cryptoKey) {
      this.#cryptoKey = undefined;
    }
  }

  async on(namespace: string[], handler: (data: any) => void) {
    const ns = await getNS(namespace, this.#prefix, !!this.#cryptoKey);
    const sub = this.#nc.subscribe(ns, {
      callback: async (err: Error, msg: any) => {
        if (err) {
          console.error(err);
          return;
        }
        let data = msg.data;
        if (this.#cryptoKey) {
          data = await decrypt(data, this.#cryptoKey);
        }
        const payload = JSON.parse(new TextDecoder().decode(data));
        handler(payload);
      },
    });
    let handlers = this.#events.get(ns);
    if (!handlers) {
      handlers = new Map();
      this.#events.set(ns, handlers);
    }
    handlers.set(handler, sub);
  }

  async off(namespace: string[], handler: (data: any) => void) {
    const ns = await getNS(namespace, this.#prefix, !!this.#cryptoKey);
    const handlers = this.#events.get(ns);
    const sub = handlers?.get(handler);
    if (sub) {
      sub.unsubscribe();
      handlers?.delete(handler);
    }
    if (!handlers?.size) {
      this.#events.delete(ns);
    }
  }

  async emit(namespace: string[], message: any) {
    const ns = await getNS(namespace, this.#prefix, !!this.#cryptoKey);
    if (this.#nc) {
      let data = new TextEncoder().encode(JSON.stringify(message));
      if (this.#cryptoKey) {
        data = await encrypt(data, this.#cryptoKey);
      }
      this.#nc.publish(ns, data);
    }
  }
}

async function getNS(namespace: string[], prefix: string, hash: boolean) {
  const parts = await Promise.all(
    [prefix, ...namespace]
      .filter(Boolean)
      .map(async part => hash ? await sha256(part) : part)
  );
  return parts.join('.');
}

async function sha256(msg: string) {
  const data = new TextEncoder().encode(msg);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  return Array.from(new Uint8Array(hashBuffer))
    .map(b => ('0' + b.toString(16)).slice(-2))
    .join('');
}

async function createEncryptionKey(secret: string) {
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

async function encrypt(payload: Uint8Array, cryptoKey: CryptoKey) {
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const ciphertext = new Uint8Array(
    await crypto.subtle.encrypt(
      { name: 'AES-GCM', iv },
      cryptoKey,
      new Uint8Array(payload)
    ),
  );
  const data = new Uint8Array(iv.byteLength + ciphertext.byteLength);
  data.set(iv, 0);
  data.set(ciphertext, iv.byteLength);
  return data;
}

async function decrypt(data: Uint8Array, cryptoKey: CryptoKey) {
  const iv = data.slice(0, 12);
  const ct = new Uint8Array(data.slice(12));
  const payload = await crypto.subtle.decrypt(
    { name: 'AES-GCM', iv },
    cryptoKey,
    ct
  );
  return payload;
}
