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
export class NatsDriver extends Map implements SignalingDriver {
  private _connect: (config?: object) => Promise<any>;
  private _secret?: string;
  private _nc?: any;
  private _cryptoKey?: CryptoKey;

  /**
   * Creates a new NatsDriver instance.
   *
   * @param options Configuration options for the driver.
   * @param options.connect A function that returns a promise resolving to a NATS connection instance.
   * @param options.secret An optional secret key for encrypting messages.
   */
  constructor(options: { connect: (config?: object) => Promise<any>; secret?: string }) {
    super();
    const { connect, secret } = options || {};
    this._connect = connect;
    this._secret = secret;
  }

  /**
   * Opens the connection to the NATS server
   * and initializes encryption if a secret is provided.
   * 
   * @param config Optional configuration options.
   */
  async open(config?: object) {
    this._nc = await this._connect(config);
    if (this._secret) {
      this._cryptoKey = await createEncryptionKey(this._secret);
    }
  }

  /**
   * Closes the connection to the NATS server.
   */
  async close() {
    if (this._nc) {
      await this._nc.close();
      delete this._nc;
    }
    if (this._cryptoKey) {
      delete this._cryptoKey;
    }
  }

  async on(namespace: string[], handler: (data: any) => void) {
    const ns = this._cryptoKey
      ? await sha256(namespace.join(':'))
      : namespace.join(':');
    const sub = this._nc.subscribe(ns, {
      callback: async (err: Error, msg: any) => {
        if (err) {
          console.error(err);
          return;
        }
        let data = msg.data;
        if (this._cryptoKey) {
          data = await decrypt(data, this._cryptoKey);
        }
        const payload = JSON.parse(new TextDecoder().decode(data));
        handler(payload);
      },
    });
    if (!this.has(ns)) {
      this.set(ns, new Map());
    }
    this.get(ns).set(handler, sub);
  }

  async off(namespace: string[], handler: (data: any) => void) {
    const ns = this._cryptoKey
      ? await sha256(namespace.join(':'))
      : namespace.join(':');
    const sub = this.get(ns)?.get(handler);
    if (sub) {
      sub.unsubscribe();
      this.get(ns).delete(handler);
    }
    if (!this.get(ns)?.size) {
      this.delete(ns);
    }
  }

  async emit(namespace: string[], message: any) {
    const ns = this._cryptoKey
      ? await sha256(namespace.join(':'))
      : namespace.join(':');
    if (this._nc) {
      let data = new TextEncoder().encode(JSON.stringify(message));
      if (this._cryptoKey) {
        data = await encrypt(data, this._cryptoKey);
      }
      this._nc.publish(ns, data);
    }
  }
}

async function sha256(msg: string) {
  const data = new TextEncoder().encode(msg);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  return Array.from(new Uint8Array(hashBuffer))
    .map(b => b.toString(16).padStart(2, '0'))
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
