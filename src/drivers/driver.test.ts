import { suite, test, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { setTimeout as wait } from "node:timers/promises";
import { Driver } from "./driver.js";

suite("drivers/driver", async () => {
  let driver: Driver;

  beforeEach(() => {
    driver = new Driver();
  });

  test("initializes as inactive", () => {
    // Assert
    assert.equal(driver.active, false);
  });

  test("emits active/inactive only on state changes", async () => {
    // Arrange
    let activeCount = 0;
    let inactiveCount = 0;

    driver.on("active", () => activeCount++);
    driver.on("inactive", () => inactiveCount++);

    // Act
    driver.active = true;
    driver.active = true;
    driver.active = false;
    driver.active = false;

    await wait(0);

    // Assert
    assert.equal(activeCount, 1);
    assert.equal(inactiveCount, 1);
  });

  test("emits error with payload", async () => {
    // Arrange
    const errors: unknown[] = [];
    const err = new Error("Something went wrong");

    driver.on("error", (err) => errors.push(err));

    // Act
    driver.emit("error", err);
    await wait(0);

    // Assert
    assert.equal(errors.length, 1);
    assert.deepEqual(errors, [err]);
  });

  test("subscribe/publish/unsubscribe emit correct events", async () => {
    // Arrange
    const subs: [string, (...args: any) => void][] = [];
    const unsubs: [string, (...args: any) => void][] = [];
    const pubs: [string, number[]][] = [];

    driver.on("subscribe", (ns, handler) => subs.push([ns, handler]));
    driver.on("unsubscribe", (ns, handler) => unsubs.push([ns, handler]));
    driver.on("publish", (ns, data) => pubs.push([ns, data]));

    // Act
    const ns = "room";
    const handler = () => {};
    driver.subscribe(ns, handler);
    driver.publish(ns, [42]);
    driver.unsubscribe(ns, handler);

    await wait(0);

    // Assert
    assert.deepEqual(subs, [[ns, handler]]);
    assert.deepEqual(pubs, [[ns, [42]]]);
    assert.deepEqual(unsubs, [[ns, handler]]);
  });

  test("off removes event handler", async () => {
    // Arrange
    let count = 0;
    const handler = () => count++;

    driver.on("active", handler);

    // Act
    driver.active = true;
    driver.off("active", handler);
    driver.active = false;
    driver.active = true;
    await wait(0);

    // Assert
    assert.equal(count, 1);
  });

  test("destroy sets inactive and clears handlers", async () => {
    // Arrange
    let activeCount = 0;
    let inactiveCount = 0;
    let activeAfterDestroy: boolean;

    driver.on("active", () => activeCount++);
    driver.on("inactive", () => inactiveCount++);

    // Act
    driver.active = true;
    driver.destroy();
    await wait(0);
    activeAfterDestroy = driver.active;

    driver.active = true;
    driver.active = false;
    await wait(0);

    // Assert
    assert.equal(activeAfterDestroy, false);
    assert.equal(activeCount, 1);
    assert.equal(inactiveCount, 0);
  });
});
