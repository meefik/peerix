import { suite, test } from "node:test";
import assert from "node:assert/strict";
import { base62ToBytes, bytesToBase62 } from "./base62.js";

suite("utils/base62", async () => {
  test("bytesToBase62 and base62ToBytes should roundtrip non-empty binary data", async () => {
    // Arrange
    const input = new Uint8Array([1, 2, 3, 4, 5, 250, 251, 252, 253, 254, 255]);

    // Act
    const encoded = bytesToBase62(input);
    const decoded = base62ToBytes(encoded);

    // Assert
    assert.equal(encoded.length > 0, true);
    assert.deepEqual(decoded, input);
  });

  test("base62 conversion should handle empty, zero-only, and invalid input", async () => {
    // Arrange
    const emptyInput = new Uint8Array();
    const zeroInput = new Uint8Array([0, 0, 0]);

    // Act
    const encodedEmpty = bytesToBase62(emptyInput);
    const encodedZeros = bytesToBase62(zeroInput);
    const decodedEmpty = base62ToBytes("");
    const decodedZero = base62ToBytes("0");

    // Assert
    assert.equal(encodedEmpty, "");
    assert.equal(encodedZeros, "0");
    assert.deepEqual(decodedEmpty, new Uint8Array());
    assert.deepEqual(decodedZero, new Uint8Array([0]));
    assert.throws(() => base62ToBytes("!"), {
      message: "Invalid base62 character",
    });
  });
});
