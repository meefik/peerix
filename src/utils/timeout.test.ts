import { suite, test } from "node:test";
import assert from "node:assert/strict";
import { setTimeout as wait } from "node:timers/promises";
import { Timeout } from "./timeout.js";

suite("utils/timeout", async () => {
  test("should call the callback after the default delay when started", async () => {
    // Arrange
    let calls = 0;
    const timeout = new Timeout(() => {
      calls += 1;
    }, 20);

    // Act
    timeout.start();

    await wait(35);

    // Assert
    assert.equal(calls, 1);
  });

  test("should use the provided delay override when start receives a value", async () => {
    // Arrange
    let calls = 0;
    const timeout = new Timeout(() => {
      calls += 1;
    }, 40);

    // Act
    timeout.start(10);

    await wait(25);

    // Assert
    assert.equal(calls, 1);
  });

  test("should restart the timer when start is called again before expiration", async () => {
    // Arrange
    let calls = 0;
    const timeout = new Timeout(() => {
      calls += 1;
    }, 30);

    // Act
    timeout.start();
    await wait(10);
    timeout.start();

    await wait(25);

    // Assert
    assert.equal(calls, 0);

    await wait(15);
    assert.equal(calls, 1);
  });

  test("should not call the callback after stop is called", async () => {
    // Arrange
    let calls = 0;
    const timeout = new Timeout(() => {
      calls += 1;
    }, 20);

    // Act
    timeout.start();
    timeout.stop();

    await wait(35);

    // Assert
    assert.equal(calls, 0);
  });

  test("stop should be safe when no timer is active", async () => {
    // Arrange
    const timeout = new Timeout(() => {}, 10);

    // Act & Assert
    assert.doesNotThrow(() => {
      timeout.stop();
      timeout.start();
      timeout.stop();
      timeout.stop();
    });
  });
});
