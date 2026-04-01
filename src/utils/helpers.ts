/**
 * Generates a RFC4122 v4 (random) UUID.
 *
 * @return {string} UUID
 */
export function UUIDv4(): string {
  return ('xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx').replace(/[xy]/g, c => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

/**
 * Sets the bitrate for audio and video tracks in a peer connection.
 * Supported browsers: Chrome 68+, Firefox 64+, Safari 11+
 *
 * @param pc The peer connection.
 * @param audioBitrate Audio bitrate in bits.
 * @param videoBitrate Video bitrate in bits.
 */
export function setPeerConnectionBitrate(pc: RTCPeerConnection, audioBitrate?: number, videoBitrate?: number) {
  if (
    typeof pc?.getSenders === 'function'
    && 'RTCRtpSender' in window
    && 'getParameters' in window.RTCRtpSender.prototype
    && 'setParameters' in window.RTCRtpSender.prototype
  ) {
    const bitrate: { [key: string]: number } = {
      audio: (audioBitrate || 0) | 0,
      video: (videoBitrate || 0) | 0,
    };
    if (!bitrate.audio && !bitrate.video) return;
    for (const sender of pc.getSenders()) {
      const kind = sender.track?.kind;
      if (!kind) return;
      const maxBitrate = bitrate[kind];
      if (!maxBitrate) return;
      const params = sender.getParameters();
      if (!params.encodings) params.encodings = [];
      for (let i = 0; i < params.encodings.length; i++) {
        const enc = params.encodings[i];
        if (enc) enc.maxBitrate = maxBitrate;
      }
      sender.setParameters(params);
    }
  }
}
