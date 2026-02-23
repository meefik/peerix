/**
 * Generates a RFC4122 v4 (random) UUID.
 *
 * @return {string} UUID
 */
export function UUIDv4() {
  return ([1e7] + -1e3 + -4e3 + -8e3 + -1e11).replace(/[018]/g, c =>
    (c ^ (crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4)).toString(16),
  );
}

/**
 * Hashes a string to a 16-bit unsigned integer using the FNV-1a algorithm.
 *
 * @param {string} str The input string to hash.
 * @return {number} A 16-bit unsigned integer hash of the input string.
 */
export function hashFNV1a(str) {
  let hash = 2166136261; // 32-bit FNV offset basis

  for (let i = 0; i < str.length; i++) {
    // XOR the bottom with the current character
    hash ^= str.charCodeAt(i);
    // Multiply by 32-bit FNV prime
    hash = Math.imul(hash, 16777619);
  }

  // "XOR-folding": Mix the upper 16 bits with the lower 16 bits
  // This squashes the 32-bit number into a 16-bit number
  return ((hash >>> 16) ^ (hash & 0xFFFF)) >>> 0;
}

/**
 * Default STUN server for ICE.
 */
export const defaultIceServers = [{ urls: 'stun:stun.l.google.com:19302' }];

/**
 * Sets the bitrate for audio and video tracks in a peer connection.
 * Supported browsers: Chrome 68+, Firefox 64+, Safari 11+
 *
 * @param {RTCPeerConnection} peerConnection - The peer connection.
 * @param {number} [audioBitrate] - Audio bitrate in kbps.
 * @param {number} [videoBitrate] - Video bitrate in kbps.
 */
export function setPeerConnectionBitrate(peerConnection, audioBitrate, videoBitrate) {
  if (
    typeof peerConnection?.getSenders === 'function'
    && 'RTCRtpSender' in window
    && 'getParameters' in window.RTCRtpSender.prototype
    && 'setParameters' in window.RTCRtpSender.prototype
  ) {
    const bitrate = { audio: audioBitrate | 0, video: videoBitrate | 0 };
    peerConnection.getSenders().forEach((sender) => {
      const maxBitrate = bitrate[sender?.track.kind];
      if (!maxBitrate) return;
      const params = sender.getParameters();
      if (!params.encodings) params.encodings = [];
      for (let i = 0; i < params.encodings.length; i++) {
        const enc = params.encodings[i];
        if (enc) enc.maxBitrate = maxBitrate * 1000;
      }
      sender.setParameters(params);
    });
  }
}
