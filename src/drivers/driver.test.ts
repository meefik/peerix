import { suite, test } from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as wait } from 'node:timers/promises';
import { Driver } from './driver.js';

suite('drivers/driver', async () => {
  test('should initialize as inactive', async () => {
    const driver = new Driver();

    assert.equal(driver.active, false);
  });

  test('should emit active and inactive only on state changes', async () => {
    const driver = new Driver();
    let activeCalls = 0;
    let inactiveCalls = 0;

    driver.on('active', () => {
      activeCalls += 1;
    });
    driver.on('inactive', () => {
      inactiveCalls += 1;
    });

    driver.active = true;
    driver.active = true;
    driver.active = false;
    driver.active = false;

    await wait(0);

    assert.equal(activeCalls, 1);
    assert.equal(inactiveCalls, 1);
    assert.equal(driver.active, false);
  });

  test('should support on, off, and emit with payloads', async () => {
    const driver = new Driver();
    const errors: unknown[] = [];

    const handler = (error: unknown) => {
      errors.push(error);
    };

    driver.on('error', handler);
    driver.emit('error', 'first');
    await wait(0);

    driver.off('error', handler);
    driver.emit('error', 'second');
    await wait(0);

    assert.deepEqual(errors, ['first']);
  });

  test('should expose no-op async subscribe/unsubscribe/publish methods', async () => {
    const driver = new Driver();
    const namespace = ['room', 'peer'];
    const handler = () => {};

    await assert.doesNotReject(driver.subscribe(namespace, handler));
    await assert.doesNotReject(driver.unsubscribe(namespace, handler));
    await assert.doesNotReject(driver.publish(namespace, [1, 2, 3]));
  });

  test('should clear handlers on destroy and set inactive', async () => {
    const driver = new Driver();
    let activeCalls = 0;
    let inactiveCalls = 0;

    driver.on('active', () => {
      activeCalls += 1;
    });
    driver.on('inactive', () => {
      inactiveCalls += 1;
    });

    driver.active = true;
    await wait(0);
    driver.active = false;
    await wait(0);
    driver.active = true;
    await wait(0);

    assert.equal(driver.active, true);
    assert.equal(activeCalls, 2);
    assert.equal(inactiveCalls, 1);

    driver.destroy();
    await wait(0);

    assert.equal(driver.active, false);

    driver.active = true;
    driver.active = false;
    await wait(0);

    assert.equal(activeCalls, 2);
    assert.equal(inactiveCalls, 1);
  });
});
