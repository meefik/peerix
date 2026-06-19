import { suite, test } from "node:test";
import assert from "node:assert/strict";
import { setTimeout as wait } from "node:timers/promises";
import { BroadcastChannelDriver } from "./broadcast-channel.js";

const makeChannelName = () =>
  `peerix-${Date.now()}-${Math.random().toString(36).slice(2)}`;

async function waitFor(predicate: () => boolean, timeoutMs: number = 300) {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started >= timeoutMs) {
      throw new Error(
        "Timed out waiting for broadcast-channel message delivery",
      );
    }
    await wait(5);
  }
}

suite("drivers/broadcast-channel", async () => {
  test("should initialize as active", async () => {
    // Arrange
    const channelName = makeChannelName();

    // Act
    const driver = new BroadcastChannelDriver(channelName);

    // Assert
    assert.equal(driver.active, true);

    driver.destroy();
  });

  test("should deliver messages to all subscribed handlers", async () => {
    // Arrange
    const channel = makeChannelName();
    const publisher = new BroadcastChannelDriver(channel);
    const subscriber = new BroadcastChannelDriver(channel);
    const payloads: number[][] = [];

    try {
      await subscriber.subscribe(["room"], (data) => {
        payloads.push(data);
      });
      await subscriber.subscribe(["room", "peer"], (data) => {
        payloads.push(data);
      });

      // Act
      await publisher.publish(["room"], [1, 2, 3]);
      await publisher.publish(["room", "peer"], [4, 5, 6]);

      // Assert
      await waitFor(() => payloads.length === 2);

      assert.deepEqual(payloads, [
        [1, 2, 3],
        [4, 5, 6],
      ]);
    } finally {
      publisher.destroy();
      subscriber.destroy();
    }
  });

  test("should stop delivering messages after unsubscribe", async () => {
    // Arrange
    const channel = makeChannelName();
    const publisher = new BroadcastChannelDriver(channel);
    const subscriber = new BroadcastChannelDriver(channel);
    const payloads: number[][] = [];
    const handler = (data: number[]) => {
      payloads.push(data);
    };

    try {
      await subscriber.subscribe(["room", "peer"], handler);

      // Act
      await publisher.publish(["room", "peer"], [1, 2, 3]);
      await waitFor(() => payloads.length === 1);

      await subscriber.unsubscribe(["room", "peer"], handler);
      await publisher.publish(["room", "peer"], [4, 5, 6]);
      await wait(50);

      // Assert
      assert.deepEqual(payloads, [[1, 2, 3]]);
    } finally {
      publisher.destroy();
      subscriber.destroy();
    }
  });

  test("should clear subscriptions on destroy and become inactive", async () => {
    // Arrange
    const channel = makeChannelName();
    const publisher = new BroadcastChannelDriver(channel);
    const subscriber = new BroadcastChannelDriver(channel);
    const payloads: number[][] = [];

    try {
      await subscriber.subscribe(["room", "peer"], (data) => {
        payloads.push(data);
      });

      // Act
      await publisher.publish(["room", "peer"], [1, 2, 3]);
      await waitFor(() => payloads.length === 1);

      subscriber.destroy();
      await publisher.publish(["room", "peer"], [4, 5, 6]);
      await wait(50);

      // Assert
      assert.equal(subscriber.active, false);
      assert.deepEqual(payloads, [[1, 2, 3]]);
    } finally {
      publisher.destroy();
      if (subscriber.active) {
        subscriber.destroy();
      }
    }
  });
});
