import { Driver } from './driver.js';

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
 *   prefix: 'peerix',
 * });
 * 
 * await driver.open();
 * ```
 */
export class NatsDriver extends Driver {
  #handlers: Map<string, Map<(message?: any) => void, any>>;
  #connect: (config?: any) => Promise<any>;
  #nc?: any;
  #prefix: string;
  #secret?: string;
  #cryptoKey?: CryptoKey;

  /**
   * Indicates whether the driver is currently active and connected to the NATS server.
   */
  active: boolean;

  /**
   * Listens for NATS connection status events.
   */
  async #trackConnectionStatus() {
    try {
      for await (const s of this.#nc.status()) {
        if (s.type === 'reconnect') {
          this.emit('active');
        }
        if (s.type === 'disconnect') {
          this.emit('inactive');
        }
      }
    }
    catch (error) {
      this.emit('error', error);
    }
  }

  /**
   * Create a new instance of the driver.
   *
   * @param options Configuration options for the driver.
   * @param options.connect A function that returns a promise resolving to a NATS connection instance.
   * @param options.secret An optional secret key for encrypting messages.
   * @param options.prefix An optional prefix for NATS subjects.
   */
  constructor(options: { connect: (config?: any) => Promise<any>; secret?: string; prefix?: string; }) {
    super();
    const { connect, secret, prefix = '' } = options || {};
    this.#connect = connect;
    this.#secret = secret;
    this.#prefix = prefix;
    this.#handlers = new Map();
    this.active = false;
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
    this.#trackConnectionStatus();

    this.emit('active');
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

    if (this.active) {
      this.emit('inactive');
    }
  }

  async subscribe(namespace: string[], handler: (message?: any) => void) {
    const subject = await getSubject(namespace, this.#prefix, !!this.#cryptoKey);
    const sub = this.#nc.subscribe(subject, {
      callback: async (err: Error, msg: any) => {
        try {
          if (err) throw err;
          let data = msg.data;
          if (this.#cryptoKey) {
            data = await decrypt(data, this.#cryptoKey);
          }
          const payload = JSON.parse(new TextDecoder().decode(data));
          setTimeout(() => handler(payload), 0);
        }
        catch (error) {
          this.emit('error', error);
        }
      },
    });

    const ns = namespace.join(':');
    let handlers = this.#handlers.get(ns);
    if (!handlers) {
      handlers = new Map();
      this.#handlers.set(ns, handlers);
    }
    handlers.set(handler, sub);
  }

  async unsubscribe(namespace: string[], handler: (message?: any) => void) {
    const ns = namespace.join(':');
    const handlers = this.#handlers.get(ns);
    if (handlers) {
      const sub = handlers.get(handler);
      handlers.delete(handler);
      if (!handlers?.size) {
        this.#handlers.delete(ns);
      }
      if (sub) {
        sub.unsubscribe();
      }
    }
  }

  async dispatch(namespace: string[], message?: any) {
    const subject = await getSubject(namespace, this.#prefix, !!this.#cryptoKey);
    let data = new TextEncoder().encode(JSON.stringify(message));
    if (this.#cryptoKey) {
      data = await encrypt(data, this.#cryptoKey);
    }
    this.#nc.publish(subject, data);
  }
}

async function getSubject(namespace: string[], prefix: string, hash: boolean) {
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
