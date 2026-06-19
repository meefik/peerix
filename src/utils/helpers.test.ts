import { suite, test } from "node:test";
import assert from "node:assert/strict";
import { parseOptions } from "./helpers.js";

suite("utils/helpers.parseOptions", async () => {
  interface CustomOptions {
    key?: string;
  }

  test("should return a shallow copy when options is an object", async () => {
    // Arrange
    const input: CustomOptions = { key: "alpha" };

    // Act
    const result = parseOptions<CustomOptions>(input);

    // Assert
    assert.deepEqual(result, { key: "alpha" });
    assert.notEqual(result, input);
  });

  test("should return an empty object for primitive options when parser is not provided", async () => {
    // Act
    const result = parseOptions<CustomOptions>(1);

    // Assert
    assert.deepEqual(result, {});
  });

  test("should return an empty object for undefined options", async () => {
    // Act
    const result = parseOptions<CustomOptions>(undefined, (options) => ({
      key: String(options),
    }));

    // Assert
    assert.deepEqual(result, {});
  });

  test("should use custom parser when options is a primitive and parser is provided", async () => {
    // Act
    const result = parseOptions<CustomOptions>(1, (options) => ({
      key: String(options),
    }));

    // Assert
    assert.deepEqual(result, { key: "1" });
  });
});
