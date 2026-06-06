import { suite, test } from 'node:test';
import assert from 'node:assert/strict';
import { IceCandidateQueue } from './ice.js';

suite('utils/ice', async () => {
  test('push should pass through candidates when the remote description ufrag matches', async () => {
    const queue = new IceCandidateQueue();
    const description = {
      sdp: 'v=0\na=ice-ufrag:match\n',
    } as RTCSessionDescriptionInit;
    const candidate = {
      candidate: 'candidate:1 1 udp 2122260223 192.0.2.1 3478 typ host',
      usernameFragment: 'match',
    } as RTCIceCandidateInit;

    assert.equal(queue.push('peer-a', candidate, description), false);
    assert.deepEqual(queue.pull('peer-a', description), []);
  });

  test('push should queue candidates until a matching remote description is available', async () => {
    const queue = new IceCandidateQueue();
    const matchingDescription = {
      sdp: 'v=0\na=ice-ufrag:later\n',
    } as RTCSessionDescriptionInit;
    const queuedCandidate = {
      candidate: 'candidate:2 1 udp 2122260223 192.0.2.2 3478 typ host',
      usernameFragment: 'later',
    } as RTCIceCandidateInit;

    assert.equal(queue.push('peer-b', queuedCandidate), true);
    assert.deepEqual(queue.pull('peer-b', matchingDescription), [
      queuedCandidate,
    ]);
    assert.deepEqual(queue.pull('peer-b', matchingDescription), []);
  });

  test('pull should discard non-matching queued candidates when the queue is flushed', async () => {
    const queue = new IceCandidateQueue();
    const description = {
      sdp: 'v=0\na=ice-ufrag:keep\n',
    } as RTCSessionDescriptionInit;
    const matchingCandidate = {
      candidate: 'candidate:3 1 udp 2122260223 192.0.2.3 3478 typ host',
      usernameFragment: 'keep',
    } as RTCIceCandidateInit;
    const discardedCandidate = {
      candidate: 'candidate:4 1 udp 2122260223 192.0.2.4 3478 typ host',
      usernameFragment: 'drop',
    } as RTCIceCandidateInit;

    assert.equal(queue.push('peer-c', matchingCandidate), true);
    assert.equal(queue.push('peer-c', discardedCandidate), true);

    assert.deepEqual(queue.pull('peer-c', description), [matchingCandidate]);
    assert.deepEqual(queue.pull('peer-c', description), []);
  });

  test('clear should remove queued candidates for one peer or all peers', async () => {
    const queue = new IceCandidateQueue();
    const candidate = {
      candidate: 'candidate:5 1 udp 2122260223 192.0.2.5 3478 typ host',
      usernameFragment: 'queued',
    } as RTCIceCandidateInit;

    assert.equal(queue.push('peer-d', candidate), true);
    queue.clear('peer-d');
    assert.deepEqual(queue.pull('peer-d'), []);

    assert.equal(queue.push('peer-e', candidate), true);
    queue.clear();
    assert.deepEqual(queue.pull('peer-e'), []);
  });
});
