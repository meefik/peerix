/**
 * ICE candidate queue to handle candidates received before remote description is applied.
 */
export class IceCandidateQueue {
  /**
   * ICE candidates received before remote description is applied.
   */
  #queues: Map<string, RTCIceCandidate[]> = new Map();

  /**
   * Parse the ICE username fragment (ufrag) from an SDP string.
   * 
   * @param sdp SDP string to parse.
   * @returns The ICE username fragment if found, otherwise undefined.
   */
  #parseUfrag(sdp?: string) {
    return sdp && /a=ice-ufrag:([^\s]+)/m.exec(sdp)?.[1];
  }

  /**
   * Queue an ICE candidate for a remote peer if its remote description is not yet set or the username fragment does not match.
   * 
   * @param id Remote peer id.
   * @param candidate ICE candidate to queue.
   * @param description Peer connection remote description.
   * @returns True if the candidate was queued, false if it can be added directly to the peer connection.
   */
  push(id: string, candidate: RTCIceCandidate, description?: RTCSessionDescription): boolean {
    const { sdp } = description || {};
    const ufrag = this.#parseUfrag(sdp);

    if (!sdp || ufrag !== candidate.usernameFragment) {
      const queue = this.#queues.get(id);
      if (!queue) this.#queues.set(id, [candidate]);
      else queue.push(candidate);
      return true;
    }

    return false;
  }

  /**
   * Helper method to add queued ICE candidates for a remote peer once its remote description is set.
   * 
   * @param id Remote peer id.
   * @param description Peer connection remote description.
   * @returns An array of candidates that were added to the peer connection. If no candidates were queued or added, an empty array is returned.
   */
  pull(id: string, description?: RTCSessionDescription): RTCIceCandidate[] {
    const candidates = [];
    if (this.#queues.has(id)) {
      const { sdp } = description || {};
      for (const candidate of this.#queues.get(id) || []) {
        const ufrag = this.#parseUfrag(sdp);
        if (!sdp || ufrag !== candidate.usernameFragment) {
          continue;
        }
        candidates.push(candidate);
      }
      this.#queues.delete(id);
    }
    return candidates;
  }

  /**
   * Clear all queued candidates.
   * 
   * @param id Optional remote peer id to clear candidates for. If not provided, all queues will be cleared.
   */
  clear(id?: string) {
    if (id) {
      this.#queues.delete(id);
    } else {
      this.#queues.clear();
    }
  }
}
