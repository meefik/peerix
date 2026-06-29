import { suite, test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { setTimeout as wait } from "node:timers/promises";
import { SIGNAL_TYPE, Signaler, type SignalMessage } from "./signaler.js";
import { MemoryDriver } from "./drivers/memory.js";
import { PUBLIC_KEY_LENGTH } from "./utils/encryption.js";
import { base62ToBytes } from "./utils/base62.js";

suite("signaler", async () => {
  let driver: MemoryDriver;
  const roomId = "test-room";

  beforeEach(() => {
    driver = new MemoryDriver();
  });

  afterEach(() => {
    driver.destroy();
  });

  test("peers subscribe, exchange announce messages, and unsubscribe from a room", async () => {
    // Arrange
    const subs: string[] = [];
    const unsubs: string[] = [];
    const pubs: string[] = [];
    const msgs: SignalMessage[] = [];
    const errors: unknown[] = [];

    driver.on("subscribe", (namespace: string) => {
      subs.push(namespace);
    });
    driver.on("unsubscribe", (namespace: string) => {
      unsubs.push(namespace);
    });
    driver.on("publish", (namespace: string) => {
      pubs.push(namespace);
    });

    const signaler1 = new Signaler({
      driver,
      namespaceHashing: false,
      signalingCompression: false,
      signalingEncryption: false,
      onMessage: async (data: SignalMessage) => {
        msgs.push(data);
      },
      onError: (err) => {
        errors.push(err);
      },
    });

    const signaler2 = new Signaler({
      driver,
      namespaceHashing: false,
      signalingCompression: false,
      signalingEncryption: false,
      onMessage: async (data: SignalMessage) => {
        msgs.push(data);
      },
      onError: (err) => {
        errors.push(err);
      },
    });

    // Act
    const peerId1 = await signaler1.subscribe(roomId);
    const peerId2 = await signaler2.subscribe(roomId);
    await wait(10);
    await signaler1.publish({ type: SIGNAL_TYPE.announce, id: roomId });
    await signaler2.publish({ type: SIGNAL_TYPE.announce, id: roomId });
    await wait(10);
    await signaler1.unsubscribe(roomId);
    await signaler2.unsubscribe(roomId);
    await wait(10);

    // Assert
    assert.equal(
      base62ToBytes(peerId1, PUBLIC_KEY_LENGTH).length,
      PUBLIC_KEY_LENGTH,
    );
    assert.equal(
      base62ToBytes(peerId2, PUBLIC_KEY_LENGTH).length,
      PUBLIC_KEY_LENGTH,
    );
    assert.deepEqual(subs, [roomId, peerId1, roomId, peerId2]);
    assert.deepEqual(pubs, [roomId, roomId]);
    assert.deepEqual(msgs, [
      { type: SIGNAL_TYPE.announce, id: peerId1, message: [] },
      { type: SIGNAL_TYPE.announce, id: peerId2, message: [] },
    ]);
    assert.deepEqual(unsubs, [roomId, peerId1, roomId, peerId2]);
    assert.equal(errors.length, 0);
  });
});
