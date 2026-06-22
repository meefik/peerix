import { suite, test } from "node:test";
import assert from "node:assert/strict";
import { parseOptions } from "./helpers.js";

suite("utils/helpers", async () => {
  interface CustomOptions {
    key?: string;
  }

  test("parseOptions returns a shallow copy when options is an object", async () => {
    // Arrange
    const input: CustomOptions = { key: "alpha" };

    // Act
    const result = parseOptions<CustomOptions>(input);

    // Assert
    assert.deepEqual(result, { key: "alpha" });
    assert.notEqual(result, input);
  });

  test("parseOptions returns an empty object for primitives without a parser", async () => {
    // Act
    const result = parseOptions<CustomOptions>(1);

    // Assert
    assert.deepEqual(result, {});
  });

  test("parseOptions returns an empty object for undefined options", async () => {
    // Act
    const result = parseOptions<CustomOptions>(undefined, (options) => ({
      key: String(options),
    }));

    // Assert
    assert.deepEqual(result, {});
  });

  test("parseOptions uses custom parser when options is a primitive and parser is provided", async () => {
    // Act
    const result = parseOptions<CustomOptions>(1, (options) => ({
      key: String(options),
    }));

    // Assert
    assert.deepEqual(result, { key: "1" });
  });
});
