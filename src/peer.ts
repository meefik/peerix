import EventEmitter from './utils/emitter.js';
import { SignalingDriver } from './drivers/signaling.js';

import {
  UUIDv4,
  hashFNV1a,
} from './utils/helpers.js';

export interface PeerOptions {
  id?: string;
  iceServers?: RTCIceServer[];
  iceTransportPolicy?: 'all' | 'relay';
  connectionTimeout?: number;
}

export interface RemotePeer {
  id: string;
  metadata?: any;
  connection: RTCPeerConnection;
  streams: Map<string | number, MediaStream>;
  channels: Map<number, RTCDataChannel>;
  dispose: () => void;
}

export interface StreamOptions {
  id: string | number;
  stream: MediaStream;
}

export interface ChannelOptions {
  id: number;
  label?: string;
  ordered?: boolean;
  maxPacketLifeTime?: number;
  maxRetransmits?: number;
  protocol?: string;
}

export interface PublishStreamOptions {
  verify?: (peer: RemotePeer) => boolean;
  // placeholder for future options
}

/**
 * Options for opening a data channel.
 * 
 * @typedef {Object} OpenChannelOptions
 * @property {string} [label=""] - The label for the data channel.
 * @property {boolean} [ordered=true] - Whether the data channel should guarantee ordered delivery.
 * @property {number} [maxPacketLifeTime] - The maximum time in milliseconds that a message can be buffered until it is sent. If both maxPacketLifeTime and maxRetransmits are not set, messages will be retransmitted until they are successfully sent.
 * @property {number} [maxRetransmits] - The maximum number of times a message will be retransmitted if it fails to send. If both maxPacketLifeTime and maxRetransmits are not set, messages will be retransmitted until they are successfully sent.
 * @property {string} [protocol] - The subprotocol name used by the data channel.
 */
export interface OpenChannelOptions {
  label?: string;
  ordered?: boolean;
  maxPacketLifeTime?: number;
  maxRetransmits?: number;
  protocol?: string;
}

/**
 * Peer class for managing WebRTC peer connections and signaling.
 */
export class Peer {
  readonly id: string;
  readonly driver: any;
  readonly iceServers: RTCIceServer[];
  readonly iceTransportPolicy: 'all' | 'relay';
  readonly connectionTimeout: number;

  room: string | undefined;
  metadata: any | undefined;

  readonly connections: Map<string, RemotePeer>;
  readonly streams: Map<string | number, StreamOptions>;
  readonly channels: Map<number, ChannelOptions>;
  readonly addons: Set<any>;

  private _emitter: EventEmitter;
  private _candidateQueues: Map<string, any[]>
  private _handler: undefined | ((e: any) => void);

  /**
   * Creates an instance of Peer.
   *
   * @param {Object} driver Signaling driver (required).
   * @param {Object} [options] Configuration options.
   * @param {string} [options.id=UUIDv4()] Unique identifier for the peer. If not provided, a random UUID will be generated.
   * @param {RTCIceServer[]} [options.iceServers] STUN/TURN servers to use for RTCPeerConnection. If not provided, a default Google STUN server will be used.
   * @param {string} [options.iceTransportPolicy='all'] Optional iceTransportPolicy for RTCPeerConnection.
   * @param {number} [options.connectionTimeout=30] Connection timeout in seconds. If a connection is not established within this time, it will be closed. Set to 0 to disable timeout.
   * @throws {Error} If the driver is not provided.
   */
  constructor(driver: SignalingDriver, options?: PeerOptions) {
    if (!driver) {
      throw new Error('Signaling driver is required');
    }
    const {
      id = UUIDv4(),
      iceServers = [{ urls: 'stun:stun.l.google.com:19302' }],
      iceTransportPolicy = 'all',
      connectionTimeout = 10,
    } = options || {};
    this.driver = driver;
    this.id = id;
    this.iceServers = iceServers;
    this.iceTransportPolicy = iceTransportPolicy;
    this.connectionTimeout = connectionTimeout;
    this.connections = new Map();
    this.streams = new Map();
    this.channels = new Map();
    this.addons = new Set();
    this._emitter = new EventEmitter(this);
    this._candidateQueues = new Map();
  }

  /**
   * Indicates whether the Receiver is currently active.
   *
   * @returns {boolean} True if the Receiver is started, false otherwise.
   */
  get active(): boolean {
    return !!this._handler;
  }

  /**
   * Start listening for incoming connections.
   *
   * @param {string} [room='default'] Room name to join.
   * @param {Object} [metadata] Metadata for the connection.
   */
  connect(room?: string, metadata?: any) {
    if (this.active) return;

    this.room = room || 'default';
    this.metadata = metadata;

    const createRemotePeer = (id: string, metadata: any): RemotePeer => {
      const streams = new Map();
      const channels = new Map();
      const connection = new RTCPeerConnection({
        iceServers: this.iceServers,
        iceTransportPolicy: this.iceTransportPolicy,
      });
      const dispose = () => {
        if (!this.connections.has(id)) return;
        clearTimeout(timeout);

        this.connections.delete(id);

        channels.forEach(channel => channel?.close());
        connection?.close();

        this._candidateQueues.delete(id);

        this.driver.emit([this.room, id], {
          type: 'leave',
          id: this.id,
        });

        this.emit('disconnect', { id });
      };

      const timeout = this.connectionTimeout > 0 ? setTimeout(
        () => {
          dispose();
          const error = new Error('Connection timeout');
          this.emit('error', { id, error });
        },
        this.connectionTimeout * 1000,
      ) : undefined;

      connection.addEventListener('iceconnectionstatechange', (e) => {
        const { iceConnectionState } = e.target as RTCPeerConnection;

        if (iceConnectionState === 'connected') {
          clearTimeout(timeout);
          this.emit('connect', { id });
        }
        else if (iceConnectionState === 'disconnected') {
          dispose();
        }
        else if (iceConnectionState === 'failed') {
          dispose();
          const error = new Error('ICE connection failed');
          this.emit('error', { id, error });
        }
        else if (iceConnectionState === 'closed') {
          dispose();
        }
      });

      connection.addEventListener('icecandidate', (e) => {
        const { candidate } = e;
        if (!candidate) return;

        this.driver.emit([this.room, id], {
          type: 'candidate',
          id: this.id,
          data: typeof candidate.toJSON === 'function'
            ? candidate.toJSON()
            : candidate,
        });
      });

      connection.addEventListener('negotiationneeded', async () => {
        try {
          const offer = await connection.createOffer();
          await connection.setLocalDescription(offer);

          this.driver.emit([this.room, id], {
            type: 'offer',
            id: this.id,
            data: offer,
            // data: typeof offer.toJSON === 'function'
            //   ? offer.toJSON()
            //   : offer,
            metadata,
          });
        }
        catch (error) {
          dispose();
          this.emit('error', { id, error });
        }
      });

      connection.addEventListener('track', (e) => {
        const { track, streams: [stream] } = e;

        if (!streams.has(stream.id)) {
          streams.set(stream.id, stream);
          stream.addEventListener('removetrack', (e) => {
            const { track } = e;
            if (!stream.getTracks().length) {
              streams.delete(stream.id);
            }
            this.emit('unpublish', { id, stream, track });
          });
        }

        this.emit('publish', { id, stream, track });
      });

      if (this.streams.size > 0) {
        for (const { stream } of this.streams.values()) {
          stream.getTracks().forEach(track => connection.addTrack(track, stream));
        }
      }

      if (this.channels.size > 0) {
        for (let [id, options] of this.channels.entries()) {
          const { label = '', ...channelOptions } = options || {};

          const channel = connection.createDataChannel(
            label,
            { ...channelOptions, negotiated: true, id: id || 0 },
          );
          channel.addEventListener('open', () => {
            this.emit('open', { id, channel });
          });
          channel.addEventListener('close', () => {
            this.emit('close', { id, channel });
          });
          channel.addEventListener('message', (e) => {
            this.emit('message', { id, channel, data: e.data });
          });
          channel.addEventListener('error', (e) => {
            this.emit('error', { id, channel, error: e.error });
          });

          channels.set(id, channel);
        }
      }

      return { id, metadata, connection, streams, channels, dispose };
    };

    this._handler = async (e) => {
      const { type, id, data, metadata } = e;
      if (!type || !id || this.id === id) return;

      console.log('Received message', e);

      // join to the room
      if (type === 'join' && data) {
        if (this.connections.has(id)) return;

        const hasLocalData = this.streams.size > 0 || this.channels.size > 0;
        if (!data && !hasLocalData) return;

        try {
          const remotePeer = createRemotePeer(id, metadata);
          this.connections.set(id, remotePeer);

          if (!hasLocalData) {
            const { connection } = remotePeer;
            const offer = await connection.createOffer();
            await connection.setLocalDescription(offer);

            this.driver.emit([this.room, id], {
              type: 'offer',
              id: this.id,
              data: offer,
              // data: typeof offer.toJSON === 'function'
              //   ? offer.toJSON()
              //   : offer,
              metadata,
            });
          }
        }
        catch (error) {
          const remotePeer = this.connections.get(id);
          if (remotePeer) remotePeer.dispose();
          this.emit('error', { id, error });
        }

        return;
      }

      // set remote description and create answer
      if (type === 'offer' && data) {
        try {
          // create new connection if it doesn't exist
          if (!this.connections.has(id)) {
            const remotePeer = createRemotePeer(id, metadata);
            this.connections.set(id, remotePeer);
          }

          const remotePeer = this.connections.get(id);
          if (!remotePeer) {
            throw new Error('Remote peer not found');
          }

          const { connection } = remotePeer;
          await connection.setRemoteDescription(data);

          // add queued candidates
          if (this._candidateQueues.has(id)) {
            for (const candidate of this._candidateQueues.get(id) || []) {
              try {
                await connection.addIceCandidate(candidate);
              }
              catch (error) {
                this.emit('error', { id, error });
              }
            }
            this._candidateQueues.delete(id);
          }

          const answer = await connection.createAnswer();
          await connection.setLocalDescription(answer);

          this.driver.emit([this.room, id], {
            type: 'answer',
            id: this.id,
            data: answer,
            // data: typeof answer.toJSON === 'function'
            //   ? answer.toJSON()
            //   : answer,
          });
        }
        catch (error) {
          const remotePeer = this.connections.get(id);
          if (remotePeer) remotePeer.dispose();
          this.emit('error', { id, error });
        }

        return;
      }

      // set remote description
      if (type === 'answer' && data) {
        const remotePeer = this.connections.get(id);
        if (!remotePeer) return;

        const { connection } = remotePeer;

        try {
          await connection.setRemoteDescription(data);
        }
        catch (error) {
          remotePeer.dispose();
          this.emit('error', { id, error });
          return;
        }

        // add queued candidates
        if (this._candidateQueues.has(id)) {
          for (let candidate of this._candidateQueues.get(id) || []) {
            try {
              await connection.addIceCandidate(candidate);
            }
            catch (error) {
              this.emit('error', { id, error });
            }
          }
          this._candidateQueues.delete(id);
        }

        return;
      }

      // add ice candidate
      if (type === 'candidate' && data) {
        const remotePeer = this.connections.get(id);

        if (!remotePeer) {
          if (!this._candidateQueues.has(id)) this._candidateQueues.set(id, []);
          this._candidateQueues.get(id)?.push(data);
          return;
        }

        const { connection } = remotePeer;

        try {
          await connection.addIceCandidate(data);
        }
        catch (error) {
          this.emit('error', { id, error });
        }

        return;
      }

      // leave the room
      if (type === 'leave') {
        const remotePeer = this.connections.get(id);

        if (remotePeer) {
          remotePeer.dispose();
        }

        return;
      }
    };

    this.driver.on([this.room], this._handler);
    this.driver.on([this.room, this.id], this._handler);

    this.driver.emit([this.room], {
      type: 'join',
      id: this.id,
      data: this.streams.size > 0 || this.channels.size > 0,
      metadata: this.metadata,
    });
  }

  /**
   * Close all connections and clean up resources.
   */
  disconnect() {
    if (!this.active) return;

    this.driver.off([this.room], this._handler);
    this.driver.off([this.room, this.id], this._handler);

    for (const remotePeer of this.connections.values()) {
      remotePeer.dispose();
    }
    this.connections.clear();

    this._candidateQueues.clear();

    delete this._handler;
  }

  publish(stream: MediaStream, id?: string | number, options: PublishStreamOptions = {}) {
    if (!id) id = stream.id;

    const hasLocalData = this.streams.size > 0 || this.channels.size > 0;
    const existingStream = this.streams.get(id)?.stream;
    const newStream = existingStream || new MediaStream();
    this.streams.set(id, { id, stream: newStream, ...options });

    for (const track of newStream.getTracks()) {
      if (!stream.getTracks().find(t => t.id === track.id)) {
        console.log('Removing track', track.id);
        newStream.removeTrack(track);
      }
    }
    for (const track of stream.getTracks()) {
      if (!newStream.getTracks().find(t => t.id === track.id)) {
        console.log('Adding track', track.id);
        newStream.addTrack(track);
      }
    }

    if (!this.active) return;

    for (const remotePeer of this.connections.values()) {
      const { connection } = remotePeer;
      const senders = connection.getSenders();
      for (const track of newStream.getTracks()) {
        const sender = senders.find((sender) => {
          return sender.track && sender.track.id === track.id
            && sender.track.readyState !== 'ended';
        });
        if (sender) sender.replaceTrack(track);
        else connection.addTrack(track, newStream);
      }
      for (const sender of senders) {
        if (sender.track && !newStream.getTracks().find(t => t.id === sender.track?.id)) {
          connection.removeTrack(sender);
        }
      }
    }

    if (!hasLocalData) {
      this.driver.emit([this.room], {
        type: 'join',
        id: this.id,
        data: true,
        metadata: this.metadata,
      });
    }
  }

  unpublish(id: string | number | MediaStream) {
    if (typeof id === 'object') id = id.id;

    const stream = this.streams.get(id)?.stream;
    const tracks = stream?.getTracks() || [];
    this.streams.delete(id);

    if (!this.active) return;

    for (const remotePeer of this.connections.values()) {
      const { connection } = remotePeer;
      const senders = connection.getSenders();
      for (const track of tracks) {
        const sender = senders.find((sender) => {
          return sender.track && sender.track.id === track.id;
        });
        if (sender) connection.removeTrack(sender);
      }
    }
  }

  open(id: number, options: OpenChannelOptions = {}) {
    const hasLocalData = this.streams.size > 0 || this.channels.size > 0;
    this.channels.set(id, { id, ...options });

    if (!this.active) return;

    for (const remotePeer of this.connections.values()) {
      if (remotePeer.channels.has(id)) continue;

      const { label = '', ...channelOptions } = options;
      const { connection } = remotePeer;

      const channel = connection.createDataChannel(
        label,
        { ...channelOptions, negotiated: true, id: id || 0 },
      );
      channel.addEventListener('open', () => {
        this.emit('open', { id, channel });
      });
      channel.addEventListener('close', () => {
        this.emit('close', { id, channel });
      });
      channel.addEventListener('message', (e) => {
        this.emit('message', { id, channel, data: e.data });
      });
      channel.addEventListener('error', (e) => {
        this.emit('error', { id, channel, error: e.error });
      });

      remotePeer.channels.set(id, channel);
    }

    if (!hasLocalData) {
      this.driver.emit([this.room], {
        type: 'join',
        id: this.id,
        data: true,
        metadata: this.metadata,
      });
    }
  }

  close(id: number) {
    this.channels.delete(id);

    if (!this.active) return;

    for (const remotePeer of this.connections.values()) {
      const channel = remotePeer.channels.get(id);
      if (channel) channel.close();
      remotePeer.channels.delete(id);
    }
  }

  send(message: any, id?: number | RTCDataChannel) {
    if (!this.active) return;

    if (id instanceof RTCDataChannel) {
      const channel = id;
      if (channel.readyState === 'open') {
        channel.send(message);
      }
      return;
    }

    for (const remotePeer of this.connections.values()) {
      if (typeof id === 'number') {
        const channel = remotePeer.channels.get(id);
        if (channel && channel.readyState === 'open') {
          channel.send(message);
        }
      }
      else if (!id) {
        for (const channel of remotePeer.channels.values()) {
          if (channel && channel.readyState === 'open') {
            channel.send(message);
          }
        }
      }
    }
  }

  // async attach(addon) {
  //   await addon.attach(this);
  //   this.addons.add(addon);
  // }

  // async detach(addon) {
  //   await addon.detach(this);
  //   this.addons.delete(addon);
  // }

  on(event: string, handler: (...args: any[]) => void) {
    this._emitter.on(event, handler);
  }

  once(event: string, handler: (...args: any[]) => void) {
    this._emitter.once(event, handler);
  }

  off(event: string, handler: (...args: any[]) => void) {
    this._emitter.off(event, handler);
  }

  emit(event: string, ...args: any[]) {
    this._emitter.emit(event, ...args);
  }
}