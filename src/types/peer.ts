import type { Peer } from '../peer.js';
import type { SignalingDriver } from './signaling.js';

/**
 * Possible peer connection states.
 * 
 * @group Peers
 */
export type PeerConnectionState = 'new' | 'connecting' | 'connected' | 'disconnected' | 'failed' | 'closed';

/**
 * Configuration options for creating a {@link Peer} instance.
 * 
 * @group Peers
 */
export interface PeerOptions {
  /**
   * Unique peer identifier. A random UUID is generated when omitted.
   */
  id?: string;
  /**
   * Signaling driver instance for message exchange between peers.
   * If omitted, a default in-memory driver is used, which is suitable for testing purposes only.
   */
  driver?: SignalingDriver;
  /**
   * An array of objects, each describing one server which may be used 
   * by the ICE agent; these are typically STUN and/or TURN servers. 
   * If this isn't specified, the connection attempt will be made 
   * with no STUN or TURN server available, which limits the connection 
   * to local peers.
   * 
   * @example
   * ```javascript
   * iceServers: [{
   *   urls: 'stun:stun.l.google.com:19302'
   * }]
   * ```
   */
  iceServers?: { urls: string | string[]; username?: string; credential?: string }[];
  /**
   * ICE policy used by created RTCPeerConnection instances. 
   * If set to 'relay', only relay candidates will be used, 
   * otherwise all candidates will be considered.
   */
  iceTransportPolicy?: 'all' | 'relay';
  /**
   * Connection timeout in seconds.
   * By default, it is set to 15 seconds. Use 0 to disable the timeout.
   */
  connectionTimeout?: number;
  /**
   * Reconnection interval in seconds after an unexpected disconnection.
   * By default, it is set to 30 seconds. Use 0 to disable automatic reconnection.
   */
  reconnectionInterval?: number;
  /**
   * Optional callback to accept or reject incoming peer connections.
   * 
   * @param options Options describing the incoming peer connection.
   * @param options.id Remote peer identifier.
   * @param options.metadata Remote peer metadata.
   * @returns A boolean or a promise that resolves to a boolean indicating whether the incoming connection should be accepted.
   */
  verify?: (options: { id: string; metadata?: any }) => Promise<boolean> | boolean;
}

/**
 * Runtime state for one connected remote peer.
 * 
 * @group Peers
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
   * Remote media streams keyed by stream label.
   */
  streams: Map<string, MediaStream>;
  /**
   * Negotiated data channels keyed by channel label.
   */
  channels: Map<string, RTCDataChannel>;
  /**
   * Cleanup routine that closes channel and connection resources.
   * 
   * @param options Optional settings for disposing the remote peer.
   * @param options.silent If true, suppresses emitting a 'leave' message via the signaling driver.
   */
  dispose: (options?: { silent?: boolean }) => void;
}

/**
 * Options for joining a room.
 * 
 * @group Peers
 */
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
 * 
 * @group Peers
 */
export interface StreamOptions {
  /**
   * Stream label.
   * If omitted, the `default` label will be used.
   */
  label?: string;
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
   * 
   * @param options Options describing the target peer and stream.
   * @param options.id Remote peer identifier.
   * @param options.metadata Remote peer metadata.
   * @param options.label Stream label.
   * @returns A boolean indicating whether the stream should be published to the specified peer.
   */
  filter?: (options: { id: string; metadata?: any; label: string }) => boolean;
}

/**
 * Options used to create negotiated RTCDataChannel instances.
 * 
 * @group Peers
 */
export interface ChannelOptions {
  /**
   * Channel label.
   * If omitted, the `default` label will be used.
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
   * 
   * @param options Options describing the target peer and channel.
   * @param options.id Remote peer identifier.
   * @param options.metadata Remote peer metadata.
   * @param options.label Channel label.
   * @returns A boolean indicating whether the channel should be created for the specified peer.
   */
  filter?: (options: { id: string; metadata?: any; label: string }) => boolean;
}

/**
 * Optional selectors/filters used by `Peer.send`.
 * 
 * @group Peers
 */
export interface SendOptions {
  /**
   * Target channel label.
   */
  label?: string;
  /**
   * Optional callback to allow or block sending to a remote channel.
   * 
   * @param options Options describing the target peer and channel.
   * @param options.id Remote peer identifier.
   * @param options.metadata Remote peer metadata.
   * @param options.label Target channel label.
   * @returns A boolean indicating whether the message should be sent to the specified channel.
   */
  filter?: (options: { id: string; metadata?: any; label: string }) => boolean;
}

/**
 * Event emitted on peer connection state changes.
 * 
 * @group Peers
 */
export interface PeerStateEvent {
  /**
   * Local peer identifier.
   */
  id: string;
  /**
   * Remote peer object containing connection details.
   */
  remote: RemotePeer;
  /**
   * New connection state.
   */
  state: PeerConnectionState;
}

/**
 * Emitted when a remote peer publishes a media track.
 * 
 * @group Peers
 */
export interface PeerTrackPublishEvent {
  /**
   * Local peer identifier.
   */
  id: string;
  /**
   * Remote peer object containing connection details.
   */
  remote: RemotePeer;
  /**
   * Media stream associated with the event.
   */
  stream: MediaStream;
  /**
   * Media track associated with the event.
   */
  track: MediaStreamTrack;
  /**
   * Label of the media stream.
   */
  label: string;
}

/**
 * Emitted when a remote peer unpublishes a media track.
 * 
 * @group Peers
 */
export interface PeerTrackUnpublishEvent {
  /**
   * Local peer identifier.
   */
  id: string;
  /**
   * Remote peer object containing connection details.
   */
  remote: RemotePeer;
  /**
   * Media stream associated with the event.
   */
  stream: MediaStream;
  /**
   * Media track associated with the event.
   */
  track: MediaStreamTrack;
  /**
   * Label of the media stream.
   */
  label: string;
}

/**
 * Emitted when a data channel is opened.
 * 
 * @group Peers
 */
export interface PeerChannelOpenEvent {
  /**
   * Local peer identifier.
   */
  id: string;
  /**
   * Remote peer object containing connection details.
   */
  remote: RemotePeer;
  /**
   * Opened data channel.
   */
  channel: RTCDataChannel;
}

/**
 * Emitted when a data channel is closed.
 * 
 * @group Peers
 */
export interface PeerChannelCloseEvent {
  /**
   * Local peer identifier.
   */
  id: string;
  /**
   * Remote peer object containing connection details.
   */
  remote: RemotePeer;
  /**
   * Closed data channel.
   */
  channel: RTCDataChannel;
}

/**
 * Emitted when a message is received on a data channel.
 * 
 * @group Peers
 */
export interface PeerMessageEvent {
  /**
   * Local peer identifier.
   */
  id: string;
  /**
   * Remote peer object containing connection details.
   */
  remote: RemotePeer;
  /**
   * Data channel that received the message.
   */
  channel: RTCDataChannel;
  /**
   * Received message data.
   */
  data: any;
}

/**
 * Emitted when an error occurs with a remote peer connection or channel.
 * 
 * @group Peers
 */
export interface PeerErrorEvent {
  /**
   * Local peer identifier.
   */
  id: string;
  /**
   * Remote peer object containing connection details.
   */
  remote?: RemotePeer;
  /**
   * Data channel associated with the error, if applicable.
   */
  channel?: RTCDataChannel;
  /**
   * Error object or message.
   */
  error: any;
  /**
   * Optional error code.
   */
  code?: string;
}

/**
 * Events emitted by {@link Peer} instances.
 * 
 * @group Peers
 */
export interface PeerEvents {
  /**
   * Emitted when a remote peer connection state changes.
   */
  state: [PeerStateEvent];
  /**
   * Emitted when a remote peer publishes a media track.
   */
  publish: [PeerTrackPublishEvent];
  /**
   * Emitted when a remote peer unpublishes a media track.
   */
  unpublish: [PeerTrackUnpublishEvent];
  /**
   * Emitted when a data channel is opened.
   */
  open: [PeerChannelOpenEvent];
  /**
   * Emitted when a data channel is closed.
   */
  close: [PeerChannelCloseEvent];
  /**
   * Emitted when a message is received on a data channel.
   */
  message: [PeerMessageEvent];
  /**
   * Emitted when an error occurs with a remote peer connection or channel.
   */
  error: [PeerErrorEvent];
}
