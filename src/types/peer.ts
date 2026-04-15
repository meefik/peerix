import type { Peer } from '../peer.js';
import type { PeerixError } from '../error.js';
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
   * If omitted, a default in-memory driver is used, which is suitable 
   * for testing purposes only.
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
  iceServers?: { urls: string | string[]; username?: string; credential?: string; }[];
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
  dispose: () => void;
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
  /**
   * Optional callback to accept or reject incoming peer connections.
   * 
   * @param options Options describing the incoming peer connection.
   * @param options.id Remote peer identifier.
   * @param options.metadata Remote peer metadata.
   * @returns A boolean indicating whether the incoming connection should be accepted.
   */
  verify?: (options: { id: string; metadata?: any; }) => boolean;
}

/**
 * Event emitted on peer connection state changes.
 * 
 * @group Peers
 */
export interface ConnectionStateEvent {
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
 * Event emitted when an error occurs in any background operations.
 * 
 * @group Peers
 */
export interface PeerErrorEvent {
  /**
   * Local peer identifier.
   */
  id: string;
  /**
   * Error object containing details about the error.
   */
  error: PeerixError;
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
  'connection': [ConnectionStateEvent];
  /**
   * Emitted when an error occurs in any background operations.
   */
  'error': [PeerErrorEvent];
  /**
   * Emitted when a remote peer publishes a media stream.
   */
  'stream:add': [StreamAddEvent];
  /**
   * Emitted when a remote peer unpublishes a media stream.
   */
  'stream:remove': [StreamRemoveEvent];
  /**
   * Emitted when a remote peer adds a media track to a published stream.
   */
  'track:add': [TrackAddEvent];
  /**
   * Emitted when a remote peer removes a media track from a published stream.
   */
  'track:remove': [TrackRemoveEvent];
  /**
   * Channel created or received from a remote peer.
   */
  'channel': [ChannelEvent];
  /**
   * Emitted when a data channel is opened.
   */
  'channel:open': [ChannelOpenEvent];
  /**
   * Emitted when a data channel is closed.
   */
  'channel:close': [ChannelCloseEvent];
  /**
   * Emitted when a message is received on a data channel.
   */
  'channel:message': [ChannelMessageEvent];
  /**
   * Emitted when an error occurs with a remote peer connection or channel.
   */
  'channel:error': [ChannelErrorEvent];
}

/**
 * Local stream publication options.
 * 
 * @group Streams
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
}

/**
 * Emitted when a remote peer publishes a media stream.
 * 
 * @group Streams
 */
export interface StreamAddEvent {
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
   * Label of the media stream.
   */
  label: string;
}

/**
 * Emitted when a remote peer unpublishes a media stream.
 * 
 * @group Streams
 */
export interface StreamRemoveEvent {
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
   * Label of the media stream.
   */
  label: string;
}


/**
 * Emitted when a remote peer add a media track to a published stream.
 * 
 * @group Streams
 */
export interface TrackAddEvent {
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
 * Emitted when a remote peer removes a media track from a published stream.
 * 
 * @group Streams
 */
export interface TrackRemoveEvent {
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
 * Options used to create negotiated RTCDataChannel instances.
 * 
 * @group Channels
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
}

/**
 * Optional selectors/filters used by `Peer.send`.
 * 
 * @group Channels
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
   * @param options.remote Remote peer descriptor.
   * @param options.channel Target data channel.
   * @returns A boolean indicating whether the message should be sent to the specified channel.
   */
  filter?: (options: { remote: RemotePeer; channel: RTCDataChannel; }) => boolean | Promise<boolean>;
}

/**
 * Emitted when a data channel created or received from a remote peer.
 * 
 * @group Channels
 */
export interface ChannelEvent {
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
  /**
   * Label of the data channel.
   */
  label: string;
}

/**
 * Emitted when a data channel is opened.
 * 
 * @group Channels
 */
export interface ChannelOpenEvent {
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
  /**
   * Label of the data channel.
   */
  label: string;
}

/**
 * Emitted when a data channel is closed.
 * 
 * @group Channels
 */
export interface ChannelCloseEvent {
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
  /**
   * Label of the data channel.
   */
  label: string;
}

/**
 * Emitted when a message is received on a data channel.
 * 
 * @group Channels
 */
export interface ChannelMessageEvent {
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
   * Label of the data channel.
   */
  label: string;
  /**
   * Received message data.
   */
  data: any;
}

/**
 * Emitted when an error occurs with a remote peer connection or channel.
 * 
 * @group Channels
 */
export interface ChannelErrorEvent {
  /**
   * Local peer identifier.
   */
  id: string;
  /**
   * Remote peer object containing connection details.
   */
  remote: RemotePeer;
  /**
   * Data channel associated with the error, if applicable.
   */
  channel: RTCDataChannel;
  /**
   * Label of the data channel.
   */
  label: string;
  /**
   * Error object containing details about the error.
   */
  error: Error;
}
