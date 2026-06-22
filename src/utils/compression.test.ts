import { suite, test } from "node:test";
import assert from "node:assert/strict";
import { compress, decompress } from "./compression.js";

suite("utils/compression", async () => {
  test("compress and decompress data successfully", async () => {
    // Arrange
    const text =
      "This is a test of the compression and decompression functions";
    const input = new TextEncoder().encode(text);

    // Act
    const compressed = await compress(input);
    const decompressed = await decompress(compressed);

    // Assert
    const output = new TextDecoder().decode(decompressed);

    assert.equal(output, text);
  });

  test("compress returns original data on failure", async () => {
    // Arrange
    const invalidData = [1n] as any;

    // Act
    const output = await compress(invalidData);

    // Assert
    assert.equal(output, invalidData);
  });

  test("decompress returns original data on failure", async () => {
    // Arrange
    const invalidData = new Uint8Array([255, 0, 1, 2, 3, 4]);

    // Act
    const output = await decompress(invalidData);

    // Assert
    assert.equal(output, invalidData);
  });
});
