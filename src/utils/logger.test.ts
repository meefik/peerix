import { suite, test, before } from "node:test";
import assert from "node:assert/strict";
import log from "./logger.js";

/** Mocked localStorage for testing. */
const mockStorage: Storage = {
  _storage: {},
  get length() {
    return Object.keys(this._storage).length;
  },
  getItem(key: string) {
    return this._storage[key] ?? null;
  },
  clear() {
    this._storage = {};
  },
  key(index: number) {
    return Object.keys(this._storage)[index];
  },
  removeItem(key: string) {
    delete this._storage[key];
  },
  setItem(key: string, value: string) {
    this._storage[key] = value;
  },
};

suite("utils/logger", () => {
  before(() => {
    globalThis.localStorage = mockStorage;
    mockStorage.setItem("debug", "peerix:allowed:*,-peerix:allowed:blocked");
  });

  test("log outputs enabled namespaces and stringifies supported values", async (t) => {
    // Arrange
    const calls: unknown[][] = [];
    t.mock.method(console, "log", (...args: unknown[]) => calls.push(args));

    const error = new Error("boom");
    const bytes = new Uint8Array([1, 2, 3]);
    const map = new Map([["k", 1]]);
    const set = new Set([4, 5]);
    const jsonLike = {
      toJSON() {
        return { ok: true };
      },
    };

    // Act
    await log(
      "allowed:topic",
      "text",
      () => "lazy",
      () => ({ error }),
      bytes,
      map,
      set,
      jsonLike,
    );

    // Assert
    assert.equal(calls.length, 1);
    assert.equal(calls[0][0], "[peerix:allowed:topic]");
    assert.equal(calls[0][1], '"text"');
    assert.equal(calls[0][2], '"lazy"');
    assert.equal(calls[0][3], '{"error":{"name":"Error","message":"boom"}}');
    assert.equal(calls[0][4], '{"type":"Uint8Array","byteLength":3}');
    assert.equal(calls[0][5], '[["k",1]]');
    assert.equal(calls[0][6], "[4,5]");
    assert.equal(calls[0][7], '{"ok":true}');
  });

  test("log skips denied namespaces and avoids evaluating lazy arguments", async (t) => {
    // Arrange
    const calls: unknown[][] = [];
    t.mock.method(console, "log", (...args: unknown[]) => calls.push(args));

    let executed = 0;

    // Act
    await log("allowed:blocked", () => {
      executed += 1;
      return "should-not-run";
    });

    // Assert
    assert.equal(executed, 0);
    assert.equal(calls.length, 0);
  });

  test("log skips namespaces not included in allow patterns", async (t) => {
    // Arrange
    const calls: unknown[][] = [];
    t.mock.method(console, "log", (...args: unknown[]) => calls.push(args));

    // Act
    await log("other:topic", "hidden");

    // Assert
    assert.equal(calls.length, 0);
  });
});
