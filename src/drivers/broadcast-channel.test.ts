import { suite, test } from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as wait } from 'node:timers/promises';
import { BroadcastChannelDriver } from './broadcast-channel.js';

suite('drivers/broadcast-channel', async () => {
  test('should initialize as active', async () => {
    const driver = new BroadcastChannelDriver();

    assert.equal(driver.active, true);

    driver.destroy();
  });

  test('should deliver messages to all subscribed handlers', async () => {
    const publisher = new BroadcastChannelDriver();
    const subscriber = new BroadcastChannelDriver();
    const payloads: number[][] = [];

    await subscriber.subscribe(['room'], (data) => {
      payloads.push(data);
    });

    await subscriber.subscribe(['room', 'peer'], (data) => {
      payloads.push(data);
    });

    await publisher.publish(['room'], [1, 2, 3]);
    await publisher.publish(['room', 'peer'], [4, 5, 6]);

    await wait(50);

    assert.deepEqual(payloads, [
      [1, 2, 3],
      [4, 5, 6],
    ]);

    publisher.destroy();
    subscriber.destroy();
  });

  test('should stop delivering messages after unsubscribe', async () => {
    const publisher = new BroadcastChannelDriver();
    const subscriber = new BroadcastChannelDriver();
    const payloads: number[][] = [];
    const handler = (data: number[]) => {
      payloads.push(data);
    };

    await subscriber.subscribe(['room', 'peer'], handler);
    await publisher.publish(['room', 'peer'], [1, 2, 3]);

    await wait(50);

    await subscriber.unsubscribe(['room', 'peer'], handler);
    await publisher.publish(['room', 'peer'], [4, 5, 6]);

    await wait(50);

    assert.deepEqual(payloads, [[1, 2, 3]]);

    publisher.destroy();
    subscriber.destroy();
  });

  test('should clear subscriptions on destroy and become inactive', async () => {
    const publisher = new BroadcastChannelDriver();
    const subscriber = new BroadcastChannelDriver();
    const payloads: number[][] = [];

    await subscriber.subscribe(['room', 'peer'], (data) => {
      payloads.push(data);
    });
    await publisher.publish(['room', 'peer'], [1, 2, 3]);

    await wait(50);

    subscriber.destroy();

    await publisher.publish(['room', 'peer'], [4, 5, 6]);
    await wait(50);

    assert.equal(subscriber.active, false);
    assert.deepEqual(payloads, [[1, 2, 3]]);

    publisher.destroy();
  });
});
