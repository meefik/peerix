import { suite, test } from "node:test";
import assert from "node:assert/strict";
import { setTimeout as wait } from "node:timers/promises";
import { EventEmitter } from "./emitter.js";

suite("utils/emitter", async () => {
  test("EventEmitter subscribes and emits with handler arguments", async () => {
    // Arrange
    type Events = {
      message: [number, string];
    };

    const emitter = new EventEmitter<Events>();
    const calls: Array<[number, string]> = [];

    emitter.on("message", (id, text) => {
      calls.push([id, text]);
    });

    // Act
    emitter.emit("message", 1, "hello");

    // Assert
    assert.equal(calls.length, 0);

    await wait(0);

    assert.deepEqual(calls, [[1, "hello"]]);
  });

  test("EventEmitter.once calls handlers only one time", async () => {
    // Arrange
    type Events = {
      ready: [];
    };

    const emitter = new EventEmitter<Events>();
    let callCount = 0;

    emitter.once("ready", () => {
      callCount += 1;
    });

    // Act
    emitter.emit("ready");
    emitter.emit("ready");

    await wait(0);

    // Assert
    assert.equal(callCount, 1);
    assert.equal(emitter.has("ready"), false);
  });

  test("EventEmitter.off removes only the specified handler with a handler arg", async () => {
    // Arrange
    type Events = {
      update: [number];
    };

    const emitter = new EventEmitter<Events>();
    let leftCalls = 0;
    let rightCalls = 0;

    const leftHandler = () => {
      leftCalls += 1;
    };

    const rightHandler = () => {
      rightCalls += 1;
    };

    emitter.on("update", leftHandler);
    emitter.on("update", rightHandler);

    // Act
    emitter.off("update", leftHandler);
    emitter.emit("update", 42);

    await wait(0);

    // Assert
    assert.equal(leftCalls, 0);
    assert.equal(rightCalls, 1);
    assert.equal(emitter.has("update"), true);
  });

  test("EventEmitter.off removes all handlers when called without a handler", async () => {
    // Arrange
    type Events = {
      sync: [];
    };

    const emitter = new EventEmitter<Events>();
    let callCount = 0;

    emitter.on("sync", () => {
      callCount += 1;
    });
    emitter.on("sync", () => {
      callCount += 1;
    });

    // Act
    emitter.off("sync");
    emitter.emit("sync");

    await wait(0);

    // Assert
    assert.equal(callCount, 0);
    assert.equal(emitter.has("sync"), false);
  });

  test("EventEmitter supports arrays of events and custom context with delay", async () => {
    // Arrange
    type Events = {
      alpha: [string];
      beta: [string];
    };

    const context = { tag: "ctx" };
    const emitter = new EventEmitter<Events>(context, { delay: 10 });
    const seen: string[] = [];

    function handler(this: typeof context, value: string) {
      seen.push(`${this.tag}:${value}`);
    }

    emitter.on(["alpha", "beta"], handler);

    // Act
    emitter.emit(["alpha", "beta"], "ok");

    // Assert
    assert.equal(seen.length, 0);

    await wait(25);

    assert.deepEqual(seen.sort(), ["ctx:ok", "ctx:ok"]);
  });
});
