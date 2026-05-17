const CHARSET = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';

/**
 * Returns a promise that resolves after a specified delay.
 *
 * @param ms - The delay in milliseconds (default is 0).
 */
export function delay(ms: number = 0): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, Math.floor(ms)));
}

/**
 * Converts a Uint8Array to a Base62 string.
 * 
 * @param bytes The input byte array.
 * @returns The Base62 encoded string.
 */
export function bytesToBase62(bytes: Uint8Array): string {
  if (bytes.length === 0) return '';

  // skip leading zeros to preserve a short representation
  let start = 0;
  while (start < bytes.length && bytes[start] === 0) start++;
  if (start === bytes.length) return CHARSET[0];

  // work with a Uint8Array view and a reusable quotient buffer to minimize allocations
  let value = bytes.subarray(start);
  const digits: number[] = [];
  const quotient = new Uint8Array(value.length);

  while (value.length > 0) {
    let remainder = 0;
    let qlen = 0;
    for (let i = 0; i < value.length; i++) {
      const cur = remainder * 256 + value[i];
      const q = (cur / 62) | 0;
      remainder = cur % 62;
      if (qlen > 0 || q > 0) {
        quotient[qlen++] = q;
      }
    }
    digits.push(remainder);
    if (qlen === 0) break;
    value = quotient.subarray(0, qlen);
  }

  // map digits to chars; digits are little-endian so reverse
  let out = '';
  for (let i = digits.length - 1; i >= 0; i--) out += CHARSET[digits[i]];
  return out;
}

/**
 * Converts a Base62 string to a Uint8Array.
 * 
 * @param str The Base62 encoded string.
 * @returns The decoded byte array.
 */
export function base62ToBytes(str: string): Uint8Array {
  if (str.length === 0) return new Uint8Array();

  // use little-endian representation (LSB at index 0)
  const bytesLE: number[] = [];
  for (let si = 0; si < str.length; si++) {
    const idx = CHARSET.indexOf(str[si]);
    if (idx === -1) throw new Error('Invalid base62 character');

    // multiply current value by 62
    let carry = 0;
    for (let i = 0; i < bytesLE.length; i++) {
      const prod = bytesLE[i] * 62 + carry;
      bytesLE[i] = prod & 0xff;
      carry = prod >>> 8;
    }
    while (carry > 0) {
      bytesLE.push(carry & 0xff);
      carry >>>= 8;
    }

    // add idx
    carry = idx;
    let i = 0;
    while (carry > 0) {
      if (i < bytesLE.length) {
        const sum = bytesLE[i] + carry;
        bytesLE[i] = sum & 0xff;
        carry = sum >>> 8;
      } else {
        bytesLE.push(carry & 0xff);
        carry >>>= 8;
      }
      i++;
    }
  }

  // remove high-order zeros (which are trailing zeros in little-endian)
  while (bytesLE.length > 1 && bytesLE[bytesLE.length - 1] === 0) bytesLE.pop();

  // convert to big-endian Uint8Array
  const out = new Uint8Array(bytesLE.length);
  for (let i = 0; i < bytesLE.length; i++) out[i] = bytesLE[bytesLE.length - 1 - i];
  return out;
}
