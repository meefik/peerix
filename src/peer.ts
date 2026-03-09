import EventEmitter from './utils/emitter.js';
import { SignalingDriver } from './types/signaling.js';
import { PeerOptions, RemotePeer, StreamOptions, ChannelOptions, SendOptions } from './types/peer.js';
import { UUIDv4 } from './utils/helpers.js';

/**
 * Peer class for managing WebRTC peer connections and signaling.
 */
export class Peer {
  /**
   * Unique identifier of the local peer.
   */
  readonly id: string;
  /**
   * Signaling transport used to exchange SDP and ICE messages.
   */
  readonly driver: SignalingDriver;
  /**
   * STUN/TURN servers passed to every RTCPeerConnection instance.
   */
  readonly iceServers: RTCIceServer[];
  /**
   * ICE transport policy for created peer connections.
   */
  readonly iceTransportPolicy: 'all' | 'relay';
  /**
   * Maximum time in seconds to wait for ICE connection establishment.
   */
  readonly connectionTimeout: number;

  /**
   * Active remote peers indexed by remote peer id.
   */
  readonly connections: Map<string, RemotePeer>;
  /**
   * Published local streams indexed by application-level stream id.
   */
  readonly streams: Map<string | number, StreamOptions>;
  /**
   * Configured local data channels indexed by channel id.
   */
  readonly channels: Map<number, ChannelOptions>;
  /**
   * Attachable extensions.
   */
  readonly addons: Set<any>;

  /**
   * Current room name. Empty until join() is called.
   */
  room: string;
  /**
   * Optional metadata announced to other peers in signaling messages.
   */
  metadata: any | undefined;

  /**
   * Internal event emitter used by on/once/off/emit helpers.
   */
  private _emitter: EventEmitter;
  /**
   * ICE candidates received before remote description is applied.
   */
  private _candidateQueues: Map<string, any[]>
  /**
   * Active signaling handler registered on the signaling driver.
   */
  private _handler: undefined | ((e: any) => void);

  /**
   * Creates an instance of Peer.
   *
   * @param driver Signaling driver instance for message exchange between peers.
   * @param options Peer configuration options.
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
    this.room = '';
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
   * Indicates whether the peer is currently active.
   *
   * @returns True if the Peer is joined to a room, false otherwise.
   */
  get active(): boolean {
    return !!this._handler;
  }

  /**
   * Join a room and start listening for incoming connections.
   *
   * @param room Room name to join. Defaults to `default` if omitted.
   * @param metadata Optional metadata to announce to other peers in the room via signaling.
   */
  join(room?: string, metadata?: any) {
    if (this._handler) return;

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
    * Leave the current room and close all active remote connections.
   */
  leave() {
    if (!this._handler) return;

    this.driver.off([this.room], this._handler);
    this.driver.off([this.room, this.id], this._handler);

    for (const remote of this.connections.values()) {
      remote.dispose();
    }
    this.connections.clear();

    this._candidateQueues.clear();

    delete this._handler;
  }

  /**
   * Publish or update a local media stream.
   *
   * When already active, this updates senders on every current connection and
   * triggers negotiation where applicable.
   *
   * @param options Stream descriptor or MediaStream instance.
   */
  publish(options: StreamOptions | MediaStream) {
    if (options instanceof MediaStream) {
      options = { id: options.id, stream: options };
    }
    const { id, stream, ...opts } = options;

    const hasLocalData = this.streams.size > 0 || this.channels.size > 0;
    const existingStream = this.streams.get(id)?.stream;
    const newStream = existingStream || new MediaStream();
    this.streams.set(id, { id, stream: newStream, ...opts });

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

  /**
   * Stop publishing a previously published local stream.
   *
   * @param id Stream identifier or object containing `id`.
   */
  unpublish(id: string | number | { id: string | number }) {
    if (typeof id === 'object') id = id.id;

    const stream = this.streams.get(id)?.stream;
    const tracks = stream?.getTracks() || [];
    this.streams.delete(id);

    if (!this.active) return;

    for (const remote of this.connections.values()) {
      const { connection } = remote;
      const senders = connection.getSenders();
      for (const track of tracks) {
        const sender = senders.find((sender) => {
          return sender.track && sender.track.id === track.id;
        });
        if (sender) connection.removeTrack(sender);
      }
    }
  }

  /**
   * Register or create a negotiated data channel with all remote peers.
   *
   * @param options Channel options or channel id.
   */
  open(options: ChannelOptions | number) {
    if (typeof options === 'number') {
      options = { id: options };
    }
    const { id, ...opts } = options;

    const hasLocalData = this.streams.size > 0 || this.channels.size > 0;
    this.channels.set(id, { id, ...opts });

    if (!this.active) return;

    for (const remote of this.connections.values()) {
      if (remote.channels.has(id)) continue;

      const { label = '', ...channelOptions } = options;
      const { connection } = remote;

      const channel = connection.createDataChannel(
        label,
        { ...channelOptions, negotiated: true, id: id || 0 },
      );
      channel.addEventListener('open', () => {
        this.emit('open', { remote, channel });
      });
      channel.addEventListener('close', () => {
        this.emit('close', { remote, channel });
      });
      channel.addEventListener('message', (e) => {
        this.emit('message', { remote, channel, data: e.data });
      });
      channel.addEventListener('error', (e) => {
        this.emit('error', { remote, channel, error: e.error });
      });

      remote.channels.set(id, channel);
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

  /**
   * Close and unregister a negotiated data channel by id.
   *
   * @param id Channel id or object containing `id`.
   */
  close(id: number | { id: number }) {
    if (typeof id === 'object') id = id.id;
    this.channels.delete(id);

    if (!this.active) return;

    for (const remote of this.connections.values()) {
      const channel = remote.channels.get(id);
      if (channel) channel.close();
      remote.channels.delete(id);
    }
  }

  /**
   * Send a message through data channels.
   *
   * If `options` is omitted, the message is sent to all open channels for every
   * connected remote peer. If `options` is a number, it is treated as channel id.
   * If `options` is an RTCDataChannel, the message is sent through that channel.
   *
   * @param message Message payload to send.
   * @param options Optional send options or RTCDataChannel or channel id.
   */
  send(message: any, options?: SendOptions | RTCDataChannel | number) {
    if (!this.active) return;

    if (options instanceof RTCDataChannel) {
      const channel = options;
      if (channel.readyState === 'open') {
        channel.send(message);
      }
      return;
    }

    for (const remote of this.connections.values()) {
      if (typeof options === 'number') {
        const id = options;
        const channel = remote.channels.get(id);
        if (channel && channel.readyState === 'open') {
          channel.send(message);
        }
      }
      else if (!options) {
        for (const channel of remote.channels.values()) {
          if (channel && channel.readyState === 'open') {
            channel.send(message);
          }
        }
      }
    }
  }

  /**
   * Attach an addon/extension to the peer instance.
   * 
   * @param addon Addon instance to attach.
   */
  async attach(addon: any) {
    await addon.attach(this);
    this.addons.add(addon);
  }

  /**
   * Detach a previously attached addon/extension from the peer instance.
   * 
   * @param addon Addon instance to detach.
   */
  async detach(addon: any) {
    await addon.detach(this);
    this.addons.delete(addon);
  }

  /**
   * Subscribe to one or more peer events.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
  on(event: string | string[], handler: (...args: any[]) => void) {
    this._emitter.on(event, handler);
  }

  /**
   * Subscribe to an event and auto-unsubscribe after first invocation.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
  once(event: string | string[], handler: (...args: any[]) => void) {
    this._emitter.once(event, handler);
  }

  /**
   * Remove a previously registered event listener.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler to remove. If omitted, all handlers for the given event(s) will be removed.
   */
  off(event: string | string[], handler: (...args: any[]) => void) {
    this._emitter.off(event, handler);
  }

  /**
   * Emit one or more events.
   *
   * @param event Event name or list of event names.
   * @param args Event payload.
   */
  emit(event: string | string[], ...args: any[]) {
    this._emitter.emit(event, ...args);
  }
}