/**
 * Generates a RFC4122 v4 (random) UUID.
 */
export function UUIDv4(): string {
  return ('xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx').replace(/[xy]/g, c => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

/**
 * Returns a promise that resolves after a specified delay.
 *
 * @param ms - The delay in milliseconds (default is 0).
 */
export function timeout(ms: number = 0): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
