/**
 * Generates a RFC4122 v4 (random) UUID.
 *
 * @return {string} UUID
 */
export function UUIDv4(): string {
  return ('10000000100040008000100000000000').replace(/[018]/g, c =>
    (Number(c) ^ (crypto.getRandomValues(new Uint8Array(1))[0] & (15 >> (Number(c) / 4)))).toString(16),
  );
}

/**
 * Hashes a string to a 16-bit unsigned integer using the FNV-1a algorithm.
 *
 * @param {string} str The input string to hash.
 * @return {number} A 16-bit unsigned integer hash of the input string.
 */
export function hashFNV1a(str: string): number {
  let hash = 2166136261; // 32-bit FNV offset basis

  for (let i = 0; i < str.length; i++) {
    // XOR the bottom with the current character
    hash ^= str.charCodeAt(i);
    // Multiply by 32-bit FNV prime
    hash = Math.imul(hash, 16777619);
  }

  // "XOR-folding": Mix the upper 16 bits with the lower 16 bits
  // This squashes the 32-bit number into a 16-bit number
  return ((hash >>> 16) ^ (hash & 0xFFFF)) >>> 0;
}
