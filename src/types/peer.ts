/**
 * Input passed to `PeerOptions.verify` for validating remote peers.
 */
export interface PeerVerifyOptions {
  /**
   * Remote peer identifier received from signaling.
   */
  id: string;
  /**
   * Remote peer metadata received from signaling.
   */
  metadata?: any;
}

/**
 * Configuration options for creating a {@link Peer} instance.
 */
export interface PeerOptions {
  /**
   * Unique peer identifier. A random UUID is generated when omitted.
   */
  id?: string;
  /**
   * An array of objects, each describing one server which may be used 
   * by the ICE agent; these are typically STUN and/or TURN servers. 
   * If this isn't specified, the connection attempt will be made 
   * with no STUN or TURN server available, which limits the connection 
   * to local peers.
   */
  iceServers?: RTCIceServer[];
  /**
   * ICE policy used by created RTCPeerConnection instances. 
   * If set to 'relay', only relay candidates will be used, 
   * otherwise all candidates will be considered.
   */
  iceTransportPolicy?: 'all' | 'relay';
  /**
   * Connection timeout in seconds. By default, it is set to 30 seconds. Use 0 to disable timeout.
   */
  connectionTimeout?: number;
  /**
   * Optional callback to accept or reject incoming peer connections.
   */
  verify?: (options: PeerVerifyOptions) => Promise<boolean> | boolean;
}

/**
 * Runtime state for one connected remote peer.
 */
export interface RemotePeer {
  /**
   * Remote peer identifier.
   */
  id: string;
  /**
   * Metadata advertised by the remote peer.
   */
  metadata?: any;
  /**
   * WebRTC connection to the remote peer.
   */
  connection: RTCPeerConnection;
  /**
   * Remote media streams keyed by stream id.
   */
  streams: Map<string | number, MediaStream>;
  /**
   * Negotiated data channels keyed by channel id.
   */
  channels: Map<number, RTCDataChannel>;
  /**
   * Cleanup routine that closes channel and connection resources.
   */
  dispose: () => void;
}

/**
 * Context passed to stream-level filters.
 */
export interface StreamFilterOptions {
  /**
   * Target remote peer for stream filtering.
   */
  remote: RemotePeer;
}

/**
 * Local stream publication options.
 */
export interface StreamOptions {
  /**
   * Application-level stream identifier.
   */
  id: string | number;
  /**
   * Media stream to publish.
   */
  stream: MediaStream;
  /**
   * Preferred audio bitrate in kbps (if used by an addon/driver).
   */
  audioBitrate?: number;
  /**
   * Preferred video bitrate in kbps (if used by an addon/driver).
   */
  videoBitrate?: number;
  /**
   * Optional callback to allow or block publishing to a remote peer.
   */
  filter?: (options: StreamFilterOptions) => Promise<boolean> | boolean;
}

/**
 * Context passed to channel-level filters.
 */
export interface ChannelFilterOptions {
  /**
   * Target remote peer for channel filtering.
   */
  remote: RemotePeer;
  /**
   * Target channel for filtering.
   */
  channel: RTCDataChannel;
}

/**
 * Options used to create negotiated RTCDataChannel instances.
 */
export interface ChannelOptions {
  /**
   * Negotiated channel id (0-65535).
   */
  id: number;
  /**
   * Optional channel label.
   */
  label?: string;
  /**
   * Whether ordered delivery is required.
   */
  ordered?: boolean;
  /**
   * Maximum packet lifetime in milliseconds.
   */
  maxPacketLifeTime?: number;
  /**
   * Maximum retransmission attempts.
   */
  maxRetransmits?: number;
  /**
   * Optional subprotocol name.
   */
  protocol?: string;
  /**
   * Optional callback to allow or block this channel for a remote peer.
   */
  filter?: (options: ChannelFilterOptions) => Promise<boolean> | boolean;
}

/**
 * Optional selectors/filters used by `Peer.send`.
 */
export interface SendOptions {
  /**
   * Target channel id.
   */
  id?: number;
  /**
   * Target channel label.
   */
  label?: string;
  /**
   * Optional callback to allow or block sending to a remote channel.
   */
  filter?: (options: ChannelFilterOptions) => Promise<boolean> | boolean;
}

/**
 * Events emitted by {@link Peer} instances.
 */
export interface PeerEvents {
  /**
   * Emitted when a remote peer connects.
   */
  join: [{ remote: RemotePeer }];
  /**
   * Emitted when a remote peer disconnects.
   */
  leave: [{ remote: RemotePeer }];
  /**
   * Emitted when a remote peer publishes a media stream.
   */
  publish: [{ remote: RemotePeer; stream: MediaStream; track: MediaStreamTrack }];
  /**
   * Emitted when a remote peer unpublishes a media stream.
   */
  unpublish: [{ remote: RemotePeer; stream: MediaStream; track?: MediaStreamTrack }];
  /**
   * Emitted when a data channel is opened.
   */
  open: [{ remote: RemotePeer; channel: RTCDataChannel }];
  /**
   * Emitted when a data channel is closed.
   */
  close: [{ remote: RemotePeer; channel: RTCDataChannel }];
  /**
   * Emitted when a message is received on a data channel.
   */
  message: [{ remote: RemotePeer; channel: RTCDataChannel; data: any }];
  /**
   * Emitted when an error occurs with a remote peer connection or channel.
   */
  error: [{ remote?: RemotePeer; channel?: RTCDataChannel; error: any }];
}
