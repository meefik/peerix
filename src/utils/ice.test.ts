import { suite, test } from "node:test";
import assert from "node:assert/strict";
import { setTimeout as wait } from "node:timers/promises";
import { IceCandidateBatcher, IceCandidateQueue } from "./ice.js";

suite("utils/ice", async () => {
  test("push passes through when remote description ufrag matches", async () => {
    // Arrange
    const queue = new IceCandidateQueue();
    const description = {
      sdp: "v=0\na=ice-ufrag:match\n",
    } as RTCSessionDescriptionInit;
    const candidate = {
      candidate: "candidate:1 1 udp 2122260223 192.0.2.1 3478 typ host",
      usernameFragment: "match",
    } as RTCIceCandidateInit;

    // Act
    const queued = queue.push("peer-a", candidate, description);

    // Assert
    assert.equal(queued, false);
    assert.deepEqual(queue.pull("peer-a", description), []);
  });

  test("push queues candidates until a matching remote description is available", async () => {
    // Arrange
    const queue = new IceCandidateQueue();
    const queuedCandidate = {
      candidate: "candidate:2 1 udp 2122260223 192.0.2.2 3478 typ host",
      usernameFragment: "later",
    } as RTCIceCandidateInit;

    // Act
    const queued = queue.push("peer-b", queuedCandidate);

    // Assert
    assert.equal(queued, true);

    const matchingDescription = {
      sdp: "v=0\na=ice-ufrag:later\n",
    } as RTCSessionDescriptionInit;
    assert.deepEqual(queue.pull("peer-b", matchingDescription), [
      queuedCandidate,
    ]);
    assert.deepEqual(queue.pull("peer-b", matchingDescription), []);
  });

  test("pull discards non-matching candidates and clears the queue", async () => {
    // Arrange
    const queue = new IceCandidateQueue();
    const description = {
      sdp: "v=0\na=ice-ufrag:keep\n",
    } as RTCSessionDescriptionInit;
    const matchingCandidate = {
      candidate: "candidate:3 1 udp 2122260223 192.0.2.3 3478 typ host",
      usernameFragment: "keep",
    } as RTCIceCandidateInit;
    const discardedCandidate = {
      candidate: "candidate:4 1 udp 2122260223 192.0.2.4 3478 typ host",
      usernameFragment: "drop",
    } as RTCIceCandidateInit;

    queue.push("peer-c", matchingCandidate);
    queue.push("peer-c", discardedCandidate);

    // Act
    const result = queue.pull("peer-c", description);

    // Assert
    assert.deepEqual(result, [matchingCandidate]);
    assert.deepEqual(queue.pull("peer-c", description), []);
  });

  test("pull returns empty when no description is provided", async () => {
    // Arrange
    const queue = new IceCandidateQueue();
    const candidate = {
      candidate: "candidate:11 1 udp 2122260223 192.0.2.11 3478 typ host",
      usernameFragment: "pending",
    } as RTCIceCandidateInit;

    queue.push("peer-g", candidate);

    // Act
    const result = queue.pull("peer-g");

    // Assert
    assert.deepEqual(result, []);
  });

  test("clear removes queued candidates for one peer or all peers", async () => {
    // Arrange
    const queue = new IceCandidateQueue();
    const candidate = {
      candidate: "candidate:5 1 udp 2122260223 192.0.2.5 3478 typ host",
      usernameFragment: "queued",
    } as RTCIceCandidateInit;

    // Act & Assert — clear single peer
    queue.push("peer-d", candidate);
    queue.clear("peer-d");
    assert.deepEqual(queue.pull("peer-d"), []);

    // Act & Assert — clear all peers
    queue.push("peer-e", candidate);
    queue.clear();
    assert.deepEqual(queue.pull("peer-e"), []);
  });

  test("queueSize limits queued candidates per peer and keeps the newest", async () => {
    // Arrange
    const queue = new IceCandidateQueue({ queueSize: 2 });
    const description = {
      sdp: "v=0\na=ice-ufrag:qsize\n",
    } as RTCSessionDescriptionInit;
    const candidate1 = {
      candidate: "candidate:6 1 udp 2122260223 192.0.2.6 3478 typ host",
      usernameFragment: "qsize",
    } as RTCIceCandidateInit;
    const candidate2 = {
      candidate: "candidate:7 1 udp 2122260223 192.0.2.7 3478 typ host",
      usernameFragment: "qsize",
    } as RTCIceCandidateInit;
    const candidate3 = {
      candidate: "candidate:8 1 udp 2122260223 192.0.2.8 3478 typ host",
      usernameFragment: "qsize",
    } as RTCIceCandidateInit;

    queue.push("peer-f", candidate1);
    queue.push("peer-f", candidate2);
    queue.push("peer-f", candidate3);

    // Act
    const result = queue.pull("peer-f", description);

    // Assert
    assert.deepEqual(result, [candidate2, candidate3]);
  });

  test("batcher debounces candidates into a single flush", async () => {
    // Arrange
    const batches: RTCIceCandidateInit[][] = [];
    const batcher = new IceCandidateBatcher({
      delay: 10,
      onFlush: (candidates) => {
        batches.push(candidates);
      },
    });
    const candidate1 = {
      candidate: "candidate:9 1 udp 2122260223 192.0.2.9 3478 typ host",
      usernameFragment: "batch",
    } as RTCIceCandidateInit;
    const candidate2 = {
      candidate: "candidate:10 1 udp 2122260223 192.0.2.10 3478 typ host",
      usernameFragment: "batch",
    } as RTCIceCandidateInit;

    batcher.push(candidate1);
    batcher.push(candidate2);

    // Act
    await wait(25);

    // Assert
    assert.deepEqual(batches, [[candidate1, candidate2]]);
  });

  test("batcher clear cancels pending flush and discards candidates", async () => {
    // Arrange
    const batches: RTCIceCandidateInit[][] = [];
    const batcher = new IceCandidateBatcher({
      delay: 10,
      onFlush: (candidates) => {
        batches.push(candidates);
      },
    });
    const candidate = {
      candidate: "candidate:12 1 udp 2122260223 192.0.2.12 3478 typ host",
      usernameFragment: "cancel",
    } as RTCIceCandidateInit;

    batcher.push(candidate);

    // Act
    batcher.clear();
    await wait(25);

    // Assert
    assert.deepEqual(batches, []);
  });
});
