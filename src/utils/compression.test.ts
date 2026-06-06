import { suite, test } from 'node:test';
import assert from 'node:assert/strict';
import { compress, decompress } from './compression.js';

suite('utils/compression', async () => {
  test('should compress and decompress data successfully', async () => {
    const text =
      'This is a test of the compression and decompression functions';
    const input = new TextEncoder().encode(text);

    const compressed = await compress(input);
    const decompressed = await decompress(compressed);

    const output = new TextDecoder().decode(decompressed);

    assert.equal(output, text);
  });

  test('should return original data when compression fails', async () => {
    const invalidData = [1n] as any;
    const output = await compress(invalidData);
    assert.equal(output, invalidData);
  });

  test('should return original data when decompression fails', async () => {
    const invalidData = new Uint8Array([255, 0, 1, 2, 3, 4]);
    const output = await decompress(invalidData);
    assert.equal(output, invalidData);
  });
});
