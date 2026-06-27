import { suite, test } from "node:test";
import assert from "node:assert/strict";
import { PeerixError } from "./error.js";

suite("PeerixError", async () => {
  test("wraps an Error object preserving name and message", async () => {
    // Arrange
    const original = new SyntaxError("unexpected token");

    // Act
    const error = new PeerixError(original);

    // Assert
    assert.equal(error.message, "unexpected token");
    assert.equal(error.name, "SyntaxError");
    assert.equal(error.code, "UNKNOWN_ERROR");
  });

  test("wraps a string message with custom code", async () => {
    // Act
    const error = new PeerixError("network failure", "SIGNALING_ERROR");

    // Assert
    assert.equal(error.message, "network failure");
    assert.equal(error.name, "Error");
    assert.equal(error.code, "SIGNALING_ERROR");
  });

  test("defaults message to 'Unknown error' for empty string", async () => {
    // Arrange
    const emptyError = new Error();
    emptyError.message = "";

    // Act
    const error = new PeerixError(emptyError);

    // Assert
    assert.equal(error.message, "Unknown error");
  });

  test("maintains correct prototype chain for instanceof checks", async () => {
    // Act
    const error = new PeerixError("test");

    // Assert
    assert.ok(error instanceof PeerixError);
    assert.ok(error instanceof Error);
  });
});
