import { suite, test } from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as wait } from 'node:timers/promises';
import { Timeout } from './timeout.js';

suite('utils/timeout', async () => {
  test('should call the callback after the default delay when started', async () => {
    let calls = 0;
    const timeout = new Timeout(() => {
      calls += 1;
    }, 20);

    timeout.start();

    await wait(35);

    assert.equal(calls, 1);
  });

  test('should use the provided delay override when start receives a value', async () => {
    let calls = 0;
    const timeout = new Timeout(() => {
      calls += 1;
    }, 40);

    timeout.start(10);

    await wait(25);

    assert.equal(calls, 1);
  });

  test('should restart the timer when start is called again before expiration', async () => {
    let calls = 0;
    const timeout = new Timeout(() => {
      calls += 1;
    }, 30);

    timeout.start();
    await wait(10);
    timeout.start();

    await wait(25);
    assert.equal(calls, 0);

    await wait(15);
    assert.equal(calls, 1);
  });

  test('should not call the callback after clear is called', async () => {
    let calls = 0;
    const timeout = new Timeout(() => {
      calls += 1;
    }, 20);

    timeout.start();
    timeout.clear();

    await wait(35);

    assert.equal(calls, 0);
  });

  test('clear should be safe when no timer is active', async () => {
    const timeout = new Timeout(() => {}, 10);

    assert.doesNotThrow(() => {
      timeout.clear();
      timeout.start();
      timeout.clear();
      timeout.clear();
    });
  });
});
