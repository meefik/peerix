const CHARSET = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';

/**
 * Returns a promise that resolves after a specified delay.
 *
 * @param ms - The delay in milliseconds (default is 0).
 */
export function timeout(ms: number = 0): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Converts a Uint8Array to a Base62 string.
 * 
 * @param bytes The input byte array.
 * @returns The Base62 encoded string.
 */
export function bytesToBase62(bytes: Uint8Array): string {
  let bigIntVal = 0n;
  for (const byte of bytes) {
    bigIntVal = (bigIntVal << 8n) + BigInt(byte);
  }

  let result = '';
  while (bigIntVal > 0n) {
    result = CHARSET[Number(bigIntVal % 62n)] + result;
    bigIntVal /= 62n;
  }

  return result;
}

/**
 * Converts a Base62 string to a Uint8Array.
 * 
 * @param str The Base62 encoded string.
 * @returns The decoded byte array.
 */
export function base62ToBytes(str: string): Uint8Array {
  let bigIntVal = 0n;
  for (const char of str) {
    const idx = CHARSET.indexOf(char);
    if (idx === -1) throw new Error('Invalid base62 character');
    bigIntVal = bigIntVal * 62n + BigInt(idx);
  }

  const bytes = [];
  while (bigIntVal > 0n) {
    bytes.unshift(Number(bigIntVal & 0xFFn));
    bigIntVal >>= 8n;
  }

  return new Uint8Array(bytes);
}
