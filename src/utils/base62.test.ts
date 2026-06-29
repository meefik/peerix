import { suite, test } from "node:test";
import assert from "node:assert/strict";
import { base62ToBytes, bytesToBase62 } from "./base62.js";

suite("utils/base62", async () => {
  test("bytesToBase62 roundtrips non-empty binary data", async () => {
    // Arrange
    const input = new Uint8Array([1, 2, 3, 4, 5, 250, 251, 252, 253, 254, 255]);

    // Act
    const encoded = bytesToBase62(input);
    const decoded = base62ToBytes(encoded);

    // Assert
    assert.equal(encoded.length > 0, true);
    assert.deepEqual(decoded, input);
  });

  test("base62 handles empty, zero-only, and invalid input", async () => {
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

  test("base62ToBytes with fixed length pads leading zeros", async () => {
    // Arrange
    const shortStr = bytesToBase62(new Uint8Array([36]));
    const fixedLen = 33;

    // Act
    const result = base62ToBytes(shortStr, fixedLen);

    // Assert
    assert.equal(result.length, fixedLen);
    // Leading bytes must be zero padding
    for (let i = 0; i < fixedLen - 1; i++) {
      assert.equal(result[i], 0);
    }
    assert.equal(result[fixedLen - 1], 36);
  });
});
