import { bytesToBase62 } from './helpers.js';

// Cached curve parameters
let A: bigint | undefined;
let B: bigint | undefined;
let P: bigint | undefined;

/**
 * Calculates a SHA-256 hash and encodes it as Base62.
 *
 * @param str The input string to hash.
 * @returns The Base62-encoded SHA-256 digest.
 */
export async function sha256(str: string): Promise<string> {
  const data = new TextEncoder().encode(str);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  return bytesToBase62(new Uint8Array(hashBuffer));
}

/**
 * Encrypts plaintext bytes with AES-GCM and prefixes the random IV.
 *
 * @param decrypted The plaintext bytes.
 * @param sharedKey The derived AES-GCM key.
 * @returns The encrypted payload as IV (12 bytes) followed by ciphertext.
 */
export async function encrypt(
  decrypted: Uint8Array,
  sharedKey: CryptoKey,
): Promise<Uint8Array> {
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const ciphertext = new Uint8Array(
    await crypto.subtle.encrypt(
      { name: 'AES-GCM', iv },
      sharedKey,
      new Uint8Array(decrypted),
    ),
  );
  const encrypted = new Uint8Array(iv.byteLength + ciphertext.byteLength);
  encrypted.set(iv, 0);
  encrypted.set(ciphertext, iv.byteLength);
  return encrypted;
}

/**
 * Decrypts an AES-GCM payload where the first 12 bytes are the IV.
 *
 * @param encrypted The encrypted payload as IV + ciphertext.
 * @param sharedKey The derived AES-GCM key.
 * @returns The decrypted plaintext bytes.
 */
export async function decrypt(
  encrypted: Uint8Array,
  sharedKey: CryptoKey,
): Promise<Uint8Array> {
  const iv = encrypted.slice(0, 12);
  const ct = new Uint8Array(encrypted.slice(12));
  const decrypted = await crypto.subtle.decrypt(
    { name: 'AES-GCM', iv },
    sharedKey,
    ct,
  );
  return new Uint8Array(decrypted);
}

/**
 * Generates an ephemeral ECDH key pair on the P-256 curve.
 *
 * @returns The generated key pair.
 */
export async function generateKeyPair(): Promise<CryptoKeyPair> {
  const keyPair = await crypto.subtle.generateKey(
    { name: 'ECDH', namedCurve: 'P-256' },
    false,
    ['deriveKey'],
  );
  return keyPair;
}

/**
 * Derives a shared AES-GCM key from a local private key and a remote public key.
 *
 * @param privateKey The local ECDH private key.
 * @param publicKey The remote ECDH public key.
 * @returns The derived AES-GCM key.
 */
export async function generateDerivedKey(
  privateKey: CryptoKey,
  publicKey: CryptoKey,
): Promise<CryptoKey> {
  const derivedKey = await crypto.subtle.deriveKey(
    { name: 'ECDH', public: publicKey },
    privateKey,
    { name: 'AES-GCM', length: 256 },
    false,
    ['encrypt', 'decrypt'],
  );
  return derivedKey;
}

/**
 * Exports an ECDH public key in compressed SEC1 format.
 *
 * @param key The public key to export.
 * @returns The 33-byte compressed public key.
 */
export async function exportPublicKey(key: CryptoKey): Promise<Uint8Array> {
  const rawKey = await crypto.subtle.exportKey('raw', key);
  return compressPublicKey(rawKey);
}

/**
 * Imports an ECDH public key from compressed SEC1 format.
 *
 * @param key The 33-byte compressed public key.
 * @returns The imported ECDH public key.
 * @throws If the compressed key length or prefix is invalid.
 */
export async function importPublicKey(key: Uint8Array): Promise<CryptoKey> {
  const rawKey = decompressPublicKey(key);
  return await crypto.subtle.importKey(
    'raw',
    new Uint8Array(rawKey),
    { name: 'ECDH', namedCurve: 'P-256' },
    false,
    [],
  );
}

/**
 * Compresses an uncompressed P-256 public key to SEC1 compressed form.
 *
 * @param rawKey The 65-byte uncompressed public key (0x04 || X || Y).
 * @returns The 33-byte compressed public key.
 */
function compressPublicKey(rawKey: ArrayBuffer): Uint8Array {
  const uint8View = new Uint8Array(rawKey);

  const x = uint8View.slice(1, 33);
  const y = uint8View.slice(33);

  // If the last byte of Y is even, prefix is 0x02. If odd, 0x03.
  const prefix = y[31] % 2 === 0 ? 0x02 : 0x03;

  const compressedKey = new Uint8Array(33);
  compressedKey[0] = prefix;
  compressedKey.set(x, 1);

  return compressedKey;
}

/**
 * Decompresses a compressed SEC1 P-256 public key.
 *
 * @param compressedKey The 33-byte compressed key.
 * @returns The 65-byte uncompressed key (0x04 || X || Y).
 * @throws If the key does not have a valid length or prefix byte.
 */
function decompressPublicKey(compressedKey: Uint8Array): Uint8Array {
  if (compressedKey.length !== 33) {
    throw new Error('Compressed public key must be exactly 33 bytes');
  }
  const prefix = compressedKey[0];
  if (prefix !== 0x02 && prefix !== 0x03) {
    throw new Error('Invalid compressed public key prefix');
  }
  const xBytes = compressedKey.slice(1);
  const x = BigInt(
    '0x' +
      Array.from(xBytes)
        .map((b) => b.toString(16).padStart(2, '0'))
        .join(''),
  );

  // P-256 Curve Constants (cached locally)
  const p =
    P ??
    BigInt(
      '0xffffffff00000001000000000000000000000000ffffffffffffffffffffffff',
    );
  const b =
    B ??
    BigInt(
      '0x5ac635d8aa3a93e7b3ebbd55769886bc651d06b0cc53b0f63bce3c3e27d2604b',
    );
  const a = A ?? p - 3n; // In P-256, a is always -3
  // Cache them for subsequent calls
  P = p;
  B = b;
  A = a;

  // 1. Solve for y^2 = x^3 + ax + b
  const x3 = (x * x * x) % p;
  const ax = (a * x) % p;
  const y2 = (x3 + ax + b) % p;

  // 2. Compute square root of y2 modulo P
  // For P-256, sqrt(n) = n^((P+1)/4) mod P
  const exp = (p + 1n) / 4n;
  let y = expMod(y2, exp, p);

  // 3. Check parity: if prefix 0x02, y must be even. If 0x03, y must be odd.
  const isEven = y % 2n === 0n;
  if ((prefix === 0x02 && !isEven) || (prefix === 0x03 && isEven)) {
    y = p - y;
  }

  // 4. Format back to 65-byte uncompressed (0x04 + X + Y)
  const rawKey = new Uint8Array(65);
  rawKey[0] = 0x04;
  rawKey.set(bigIntToUint8Array(x), 1);
  rawKey.set(bigIntToUint8Array(y), 33);
  return rawKey;
}

/**
 * Computes modular exponentiation using square-and-multiply.
 *
 * @param base The exponentiation base.
 * @param exp The exponent.
 * @param mod The modulus.
 * @returns base^exp mod mod.
 */
function expMod(base: bigint, exp: bigint, mod: bigint): bigint {
  let res = 1n;
  base = base % mod;
  while (exp > 0n) {
    if (exp % 2n === 1n) res = (res * base) % mod;
    base = (base * base) % mod;
    exp = exp / 2n;
  }
  return res;
}

/**
 * Converts a bigint to a left-padded 32-byte array.
 *
 * @param bn The bigint value to encode.
 * @returns The 32-byte big-endian representation.
 */
function bigIntToUint8Array(bn: bigint): Uint8Array {
  let hex = bn.toString(16).padStart(64, '0');
  let len = hex.length / 2;
  let u8 = new Uint8Array(len);
  for (let i = 0; i < len; i++) {
    u8[i] = parseInt(hex.substring(i * 2, i * 2 + 2), 16);
  }
  return u8;
}
