import { suite, test } from "node:test";
import assert from "node:assert/strict";
import { setTimeout as wait } from "node:timers/promises";
import { MemoryDriver } from "./memory.js";

suite("drivers/memory", async () => {
  test("MemoryDriver initializes as active", async () => {
    // Arrange
    const driver = new MemoryDriver();

    // Act & Assert
    assert.equal(driver.active, true);

    driver.destroy();
  });

  test("MemoryDriver delivers messages to all subscribed handlers", async () => {
    // Arrange
    const driver = new MemoryDriver();
    const payloads: number[][] = [];

    await driver.subscribe(["room"], (data) => {
      payloads.push(data);
    });
    await driver.subscribe(["room", "peer"], (data) => {
      payloads.push(data);
    });

    // Act
    await driver.publish(["room"], [1, 2, 3]);
    await wait(0);
    await driver.publish(["room", "peer"], [4, 5, 6]);
    await wait(0);

    // Assert
    assert.deepEqual(payloads, [
      [1, 2, 3],
      [4, 5, 6],
    ]);

    driver.destroy();
  });

  test("MemoryDriver stops delivering after unsubscribe", async () => {
    // Arrange
    const driver = new MemoryDriver();
    const payloads: number[][] = [];
    const handler = (data: number[]) => {
      payloads.push(data);
    };

    await driver.subscribe(["room", "peer"], handler);

    // Act
    await driver.unsubscribe(["room", "peer"], handler);
    await driver.publish(["room", "peer"], [1, 2, 3]);
    await wait(0);

    // Assert
    assert.deepEqual(payloads, []);

    driver.destroy();
  });

  test("MemoryDriver delays message delivery when configured", async () => {
    // Arrange
    const driver = new MemoryDriver({ delay: 20 });
    let callCount = 0;

    await driver.subscribe(["room", "peer"], () => {
      callCount += 1;
    });

    // Act
    await driver.publish(["room", "peer"], [1, 2, 3]);

    // Assert
    await wait(5);
    assert.equal(callCount, 0);

    await wait(30);
    assert.equal(callCount, 1);

    driver.destroy();
  });

  test("MemoryDriver clears subscriptions on destroy and becomes inactive", async () => {
    // Arrange
    const driver = new MemoryDriver();
    const payloads: number[][] = [];

    await driver.subscribe(["room", "peer"], (data) => {
      payloads.push(data);
    });

    // Act
    driver.destroy();
    await driver.publish(["room", "peer"], [1, 2, 3]);
    await wait(0);

    // Assert
    assert.equal(driver.active, false);
    assert.deepEqual(payloads, []);
  });
});
