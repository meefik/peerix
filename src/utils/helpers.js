/**
 * Generates a RFC4122 v4 (random) UUID.
 *
 * @return {string} UUID
 */
export function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
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
