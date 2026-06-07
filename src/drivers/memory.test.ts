import { suite, test } from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as wait } from 'node:timers/promises';
import { MemoryDriver } from './memory.js';

suite('drivers/memory', async () => {
  test('should initialize as active', async () => {
    const driver = new MemoryDriver();

    assert.equal(driver.active, true);

    driver.destroy();
  });

  test('should deliver messages to all subscribed handlers', async () => {
    const driver = new MemoryDriver();
    const payloads: number[][] = [];

    await driver.subscribe(['room'], (data) => {
      payloads.push(data);
    });

    await driver.subscribe(['room', 'peer'], (data) => {
      payloads.push(data);
    });

    await driver.publish(['room'], [1, 2, 3]);
    await wait(0);
    await driver.publish(['room', 'peer'], [4, 5, 6]);
    await wait(0);

    assert.deepEqual(payloads, [
      [1, 2, 3],
      [4, 5, 6],
    ]);

    driver.destroy();
  });

  test('should stop delivering messages after unsubscribe', async () => {
    const driver = new MemoryDriver();
    const payloads: number[][] = [];
    const handler = (data: number[]) => {
      payloads.push(data);
    };

    await driver.subscribe(['room', 'peer'], handler);
    await driver.unsubscribe(['room', 'peer'], handler);
    await driver.publish(['room', 'peer'], [1, 2, 3]);
    await wait(0);

    assert.deepEqual(payloads, []);

    driver.destroy();
  });

  test('should delay message delivery when configured', async () => {
    const driver = new MemoryDriver({ delay: 20 });
    let callCount = 0;

    await driver.subscribe(['room', 'peer'], () => {
      callCount += 1;
    });

    await driver.publish(['room', 'peer'], [1, 2, 3]);
    await wait(5);

    assert.equal(callCount, 0);

    await wait(30);

    assert.equal(callCount, 1);

    driver.destroy();
  });

  test('should clear subscriptions on destroy and become inactive', async () => {
    const driver = new MemoryDriver();
    const payloads: number[][] = [];

    await driver.subscribe(['room', 'peer'], (data) => {
      payloads.push(data);
    });

    driver.destroy();

    await driver.publish(['room', 'peer'], [1, 2, 3]);
    await wait(0);

    assert.equal(driver.active, false);
    assert.deepEqual(payloads, []);
  });
});
