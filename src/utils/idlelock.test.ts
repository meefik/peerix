import { suite, test } from "node:test";
import assert from "node:assert/strict";
import { setTimeout as wait } from "node:timers/promises";
import { IdleLock } from "./idlelock.js";

suite("utils/idlelock", async () => {
  test("isIdle reflects counter state", () => {
    // Arrange
    const lock = new IdleLock();

    // Assert — idle when count is zero
    assert.equal(lock.isIdle, true);

    lock.acquire();
    assert.equal(lock.isIdle, false);

    lock.release();
    assert.equal(lock.isIdle, true);
  });

  test("counter changes and guards correctly", () => {
    // Arrange
    const lock = new IdleLock();

    // Act & Assert
    assert.equal(lock.count, 0);

    lock.acquire();
    assert.equal(lock.count, 1);

    lock.acquire();
    assert.equal(lock.count, 2);

    lock.release();
    assert.equal(lock.count, 1);

    lock.release();
    assert.equal(lock.count, 0);

    // Guarded — no crash when counter is already zero
    assert.doesNotThrow(() => {
      lock.release();
      lock.release();
    });
    assert.equal(lock.count, 0);
  });

  test("resolves immediately when already idle or disposed", async () => {
    // Arrange
    const idleLock = new IdleLock();
    const disposedLock = new IdleLock();
    disposedLock.dispose();

    // Act & Assert — both resolve without hanging
    await Promise.all([idleLock.waitForIdle(), disposedLock.waitForIdle(10)]);
  });

  test("waits until all operations finish", async () => {
    // Arrange
    const lock = new IdleLock();
    let done = false;

    // Act
    lock.acquire();
    lock.acquire();

    void lock.waitForIdle().then(() => {
      done = true;
    });

    assert.equal(done, false);

    lock.release();
    await wait(0);
    assert.equal(done, false); // still busy

    lock.release();

    // Assert
    await wait(0);
    assert.equal(done, true);
  });

  test("multiple concurrent waiters all resolve on idle", async () => {
    // Arrange
    const lock = new IdleLock();
    const results: boolean[] = [];

    // Act
    lock.acquire();

    void lock.waitForIdle().then(() => results.push(true));
    void lock.waitForIdle().then(() => results.push(true));
    void lock.waitForIdle().then(() => results.push(true));

    lock.release();

    // Assert
    await wait(0);
    assert.equal(results.length, 3);
  });

  test("waitForIdle rejects on timeout without affecting other waiters", async () => {
    // Arrange
    const lock = new IdleLock();
    let resolvedCount = 0;

    // Act
    lock.acquire();

    void lock.waitForIdle(30).catch(() => {});
    void lock.waitForIdle().then(() => {
      resolvedCount += 1;
    });

    await wait(50);
    assert.equal(resolvedCount, 0); // timed-out waiter rejected, other still waiting

    lock.release();

    // Assert
    await wait(0);
    assert.equal(resolvedCount, 1);
  });

  test("methods become no-ops and waiters resolve on dispose", async () => {
    // Arrange
    const lock = new IdleLock();
    lock.acquire();

    let waiterResolved = false;
    void lock.waitForIdle().then(() => {
      waiterResolved = true;
    });

    // Act
    lock.dispose();
    lock.acquire();
    lock.release();

    // Assert — acquire/release ignored, waiter unblocked
    assert.equal(lock.count, 1);
    await wait(0);
    assert.equal(waiterResolved, true);
  });
});
