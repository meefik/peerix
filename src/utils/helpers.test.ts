import { suite, test } from 'node:test';
import assert from 'node:assert/strict';
import { base62ToBytes, bytesToBase62, delay } from './helpers.js';

async function streamToBytes(stream: ReadableStream): Promise<Uint8Array> {
  const buffer = await new Response(stream).arrayBuffer();
  return new Uint8Array(buffer);
}

suite('utils/helpers', async () => {
  test('delay should wait for the specified time', async () => {
    let t = Date.now();
    await delay(50);
    const capturedDelay = Math.floor(Date.now() - t);

    assert.ok(capturedDelay > 0 && capturedDelay < 100);
  });

  test('bytesToBase62 and base62ToBytes should roundtrip non-empty binary data', async () => {
    const input = new Uint8Array([1, 2, 3, 4, 5, 250, 251, 252, 253, 254, 255]);

    const encoded = bytesToBase62(input);
    const decoded = base62ToBytes(encoded);

    assert.equal(encoded.length > 0, true);
    assert.deepEqual(decoded, input);
  });

  test('base62 conversion should handle empty, zero-only, and invalid input', async () => {
    assert.equal(bytesToBase62(new Uint8Array()), '');
    assert.equal(bytesToBase62(new Uint8Array([0, 0, 0])), '0');
    assert.deepEqual(base62ToBytes(''), new Uint8Array());
    assert.deepEqual(base62ToBytes('0'), new Uint8Array([0]));
    assert.throws(() => base62ToBytes('!'), {
      message: 'Invalid base62 character',
    });
  });
});
