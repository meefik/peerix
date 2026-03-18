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
   * Connection timeout in seconds.
   * By default, it is set to 30 seconds. Use 0 to disable timeout.
   */
  connectionTimeout?: number;
  /**
   * Optional callback to accept or reject incoming peer connections.
   */
  verify?: (options: { id: string; metadata?: any }) => Promise<boolean> | boolean;
}

/**
 * Possible peer connection states.
 */
export type PeerConnectionState = 'new' | 'connecting' | 'connected' | 'disconnected' | 'failed' | 'closed';

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
   * Peer connection state, updated on connection state changes.
   */
  state: PeerConnectionState;
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

export interface JoinOptions {
  /**
   * Room name to join.
   * If omitted, the peer will join a room with the `default` name.
   */
  room?: string;
  /**
   * Optional metadata to advertise to the remote peer.
   */
  metadata?: any;
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
   * Whether the peer should manage the lifecycle of the stream's tracks.
   * If true, tracks will be stopped when the stream is unpublished or replaced.
   */
  managed?: boolean;
  /**
   * Preferred audio bitrate in bits per second.
   * For example, 16000 for 16 kbps.
   */
  audioBitrate?: number;
  /**
   * Preferred video bitrate in bits per second.
   * For example, 64000 for 64 kbps.
   */
  videoBitrate?: number;
  /**
   * Optional callback to allow or block publishing to a remote peer.
   */
  filter?: (options: { remote: RemotePeer }) => boolean;
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
  filter?: (options: { remote: RemotePeer }) => boolean;
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
  filter?: (options: { remote: RemotePeer; channel: RTCDataChannel }) => boolean;
}

/**
 * Events emitted by {@link Peer} instances.
 */
export interface PeerEvents {
  /**
   * Emitted when a remote peer creates a new connection.
   */
  join: [{ remote: RemotePeer }];
  /**
   * Emitted when a remote peer leaves the room and disconnects.
   */
  leave: [{ remote: RemotePeer }];
  /**
   * Emitted when a remote peer connection state changes.
   */
  state: [{ remote: RemotePeer; state: PeerConnectionState }];
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
