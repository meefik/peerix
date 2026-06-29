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
      throw new Error("Timed out waiting for predicate");
    }
    await wait(5);
  }
}

suite("drivers/broadcast-channel", async () => {
  test("BroadcastChannelDriver initializes as active", async () => {
    // Arrange
    const channelName = makeChannelName();

    // Act
    const driver = new BroadcastChannelDriver(channelName);

    // Assert
    assert.equal(driver.active, true);

    driver.destroy();
  });

  test("BroadcastChannelDriver delivers messages to all subscribed handlers", async () => {
    // Arrange
    const channel = makeChannelName();
    const publisher = new BroadcastChannelDriver(channel);
    const subscriber = new BroadcastChannelDriver(channel);
    const payloads: number[][] = [];

    try {
      await subscriber.subscribe("room", (data) => {
        payloads.push(data);
      });

      // Act
      await publisher.publish("room", [1, 2, 3]);
      await publisher.publish("room", [4, 5, 6]);
      await waitFor(() => payloads.length === 2);

      // Assert
      assert.deepEqual(payloads, [
        [1, 2, 3],
        [4, 5, 6],
      ]);
    } finally {
      publisher.destroy();
      subscriber.destroy();
    }
  });

  test("BroadcastChannelDriver stops delivering after unsubscribe", async () => {
    // Arrange
    const channel = makeChannelName();
    const publisher = new BroadcastChannelDriver(channel);
    const subscriber = new BroadcastChannelDriver(channel);
    const payloads: number[][] = [];
    const handler = (data: number[]) => {
      payloads.push(data);
    };

    try {
      await subscriber.subscribe("room", handler);

      // Act
      await publisher.publish("room", [1, 2, 3]);
      await waitFor(() => payloads.length === 1);

      await subscriber.unsubscribe("room", handler);
      await publisher.publish("room", [4, 5, 6]);
      await wait(50);

      // Assert
      assert.deepEqual(payloads, [[1, 2, 3]]);
    } finally {
      publisher.destroy();
      subscriber.destroy();
    }
  });

  test("BroadcastChannelDriver clears subscriptions on destroy and becomes inactive", async () => {
    // Arrange
    const channel = makeChannelName();
    const publisher = new BroadcastChannelDriver(channel);
    const subscriber = new BroadcastChannelDriver(channel);
    const payloads: number[][] = [];

    try {
      await subscriber.subscribe("room", (data) => {
        payloads.push(data);
      });

      // Act
      await publisher.publish("room", [1, 2, 3]);
      await waitFor(() => payloads.length === 1);

      subscriber.destroy();
      await publisher.publish("room", [4, 5, 6]);
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
