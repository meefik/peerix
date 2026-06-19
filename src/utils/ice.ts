/**
 * ICE candidate queue to handle candidates received before remote description is applied.
 */
export class IceCandidateQueue {
  #queues: Map<string, RTCIceCandidateInit[]>;
  #queueSize: number;

  constructor(options: { queueSize?: number } = {}) {
    const { queueSize = 100 } = options;
    this.#queues = new Map();
    this.#queueSize = queueSize;
  }

  /**
   * Parses the ICE username fragment (ufrag) from an SDP string.
   *
   * @param sdp SDP string to parse.
   * @returns The ICE username fragment if found, otherwise undefined.
   */
  #parseUfrag(sdp?: string): string | undefined {
    return sdp && /a=ice-ufrag:([^\s\r]+)/.exec(sdp)?.[1];
  }

  /**
   * Queues an ICE candidate for a remote peer if its remote description is not yet set or the username fragment does not match.
   *
   * @param id Remote peer id.
   * @param candidate ICE candidate to queue.
   * @param description Peer connection remote description.
   * @returns True if the candidate was queued, false if it can be added directly to the peer connection.
   */
  push(
    id: string,
    candidate: RTCIceCandidateInit,
    description?: RTCSessionDescriptionInit,
  ): boolean {
    const { sdp } = description || {};
    const ufrag = this.#parseUfrag(sdp);

    if (!sdp || ufrag !== candidate.usernameFragment) {
      const queue = this.#queues.get(id);
      if (!queue) this.#queues.set(id, [candidate]);
      else {
        queue.push(candidate);
        if (this.#queueSize && queue.length > this.#queueSize) {
          queue.shift();
        }
      }
      return true;
    }

    return false;
  }

  /**
   * Retrieves queued ICE candidates for a remote peer that match the username
   * fragment of the given remote description.
   *
   * This method clears the entire queue for the peer after filtering,
   * so candidates that do not match are also discarded.
   *
   * @param id Remote peer id.
   * @param description Peer connection remote description.
   * @returns An array of ICE candidates whose username fragment matches the remote description.
   */
  pull(
    id: string,
    description?: RTCSessionDescriptionInit,
  ): RTCIceCandidateInit[] {
    const queue = this.#queues.get(id);
    if (!queue || !queue.length) return [];

    const { sdp } = description || {};
    if (!sdp) return [];

    const ufrag = this.#parseUfrag(sdp);
    const matched: RTCIceCandidateInit[] = [];

    for (const candidate of queue) {
      if (ufrag === candidate.usernameFragment) {
        matched.push(candidate);
      }
    }

    const remaining = queue.filter((c) => ufrag !== c.usernameFragment);

    if (remaining.length) {
      this.#queues.set(id, remaining);
    } else {
      this.#queues.delete(id);
    }

    return matched;
  }

  /**
   * Clears all queued candidates.
   *
   * @param id Optional remote peer id to clear candidates for. If not provided, all queues will be cleared.
   */
  clear(id?: string): void {
    if (id) {
      this.#queues.delete(id);
    } else {
      this.#queues.clear();
    }
  }
}

/**
 * Debounces ICE candidates before forwarding them in a single batch.
 */
export class IceCandidateBatcher {
  #delay: number;
  #timer?: ReturnType<typeof setTimeout>;
  #candidates: RTCIceCandidateInit[];
  #onFlush: (candidates: RTCIceCandidateInit[]) => void;

  /**
   * Initializes the ICE candidate batcher with the specified delay and flush callback.
   *
   * @param options Configuration options including delay and onFlush callback.
   */
  constructor(options: {
    delay: number;
    onFlush: (candidates: RTCIceCandidateInit[]) => void;
  }) {
    const { delay, onFlush } = options;
    this.#delay = delay;
    this.#onFlush = onFlush;
    this.#candidates = [];
  }

  /**
   * Adds a candidate to the batch and schedules a flush.
   *
   * @param candidate ICE candidate to add to the batch.
   */
  push(candidate: RTCIceCandidateInit): void {
    clearTimeout(this.#timer);
    this.#candidates.push(candidate);
    this.#timer = setTimeout(() => this.#flush(), this.#delay);
  }

  /**
   * Clears all pending candidates and cancels the scheduled flush.
   */
  clear(): void {
    clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#candidates = [];
  }

  /**
   * Flushes all accumulated candidates by calling the onFlush callback.
   */
  #flush(): void {
    this.#timer = undefined;
    const candidates = this.#candidates.splice(0);
    if (!candidates.length) return;

    this.#onFlush(candidates);
  }
}
