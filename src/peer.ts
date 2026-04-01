import type { SignalingDriver } from './types/signaling.js';
import type { PeerOptions, JoinOptions, RemotePeer, StreamOptions, ChannelOptions, SendOptions, PeerEvents } from './types/peer.js';
import { MemoryDriver } from './drivers/memory.js';
import EventEmitter from './utils/emitter.js';
import { UUIDv4, setPeerConnectionBitrate } from './utils/helpers.js';
import log from './utils/logger.js';

// All peers without a driver will share the same in-memory signaling bus
const defaultDriver = new MemoryDriver();

/**
 * Peer class for managing WebRTC peer connections and signaling.
 * 
 * @group Peers
 * @example
 * ```javascript
 * // create a new peer
 * // using default in-memory signaling driver
 * const peer = new Peer();
 *
 * // listen for open channel event
 * peer.on('open', (e) => {
 *   const { remote, channel } = e;
 *   // send a message to the connected peer
 *   channel.send('Hello, peer!');
 * });
 *
 * // listen for incoming messages
 * peer.on('message', (e) => {
 *   const { remote, channel, data } = e;
 *   console.log('Received message:', data);
 * });
 *
 * // open a data channel
 * peer.open({ id: 0 });
 *
 * // join a room
 * peer.join({ room: 'room-id' });
 * ```
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
  readonly iceServers: { urls: string | string[]; username?: string; credential?: string }[];
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
  private _emitter: EventEmitter<PeerEvents>;
  /**
   * ICE candidates received before remote description is applied.
   */
  private _candidateQueues: Map<string, any[]>;
  /**
   * Set of peer ids for which a local offer is being created 
   * to handle glare scenarios.
   */
  private _makingOffer: Set<string>;
  /**
   * Active signaling handler registered on the signaling driver.
   */
  private _signal: undefined | ((e: any) => void);
  /**
   * Optional callback to accept or reject incoming peer connections.
   */
  private _verify?: (options: { id: string; metadata?: any }) => Promise<boolean> | boolean;

  /**
   * Creates an instance of Peer.
   *
   * @param options Peer configuration options.
   */
  constructor(options?: PeerOptions) {
    const {
      id = UUIDv4(),
      driver = defaultDriver,
      iceServers = [],
      iceTransportPolicy = 'all',
      connectionTimeout = 30,
      verify,
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
    this._emitter = new EventEmitter<PeerEvents>(this);
    this._candidateQueues = new Map();
    this._makingOffer = new Set();
    this._verify = verify;
  }

  /**
   * Indicates whether the peer is currently active.
   *
   * @returns True if the Peer is joined to a room, false otherwise.
   */
  get active(): boolean {
    return !!this._signal;
  }

  /**
   * Join a room and start listening for incoming connections.
   *
   * @param options Room name or join options.
   */
  join(options?: string | JoinOptions) {
    if (this._signal) return;

    const { room = 'default', metadata } = typeof options === 'object'
      ? options : { room: options };
    this.room = room;
    this.metadata = metadata;

    const createRemote = (id: string, metadata: any): RemotePeer => {
      const streams = new Map();
      const channels = new Map();
      const connection = new RTCPeerConnection({
        iceServers: this.iceServers,
        iceTransportPolicy: this.iceTransportPolicy,
      });
      const dispose = ({ silent = false } = {}) => {
        if (!this.connections.has(id)) return;
        this.connections.delete(id);
        clearTimeout(timeout);

        this._candidateQueues.delete(id);
        this._makingOffer.delete(id);

        channels.forEach(channel => channel?.close());
        connection?.close();
        remote.state = 'closed';

        if (!silent) {
          this.driver.emit([this.room, id], {
            type: 'leave',
            id: this.id,
          });
        }

        this.emit('state', { remote, state: 'closed' });
      };

      const remote: RemotePeer = {
        id,
        metadata,
        connection,
        state: 'new',
        streams,
        channels,
        dispose,
      };

      const timeout = this.connectionTimeout > 0 ? setTimeout(
        () => {
          dispose();
          const error = new Error('Connection timeout');
          this.emit('error', { remote, error, code: 'CONNECTION_TIMEOUT' });
        },
        this.connectionTimeout * 1000,
      ) : undefined;

      connection.addEventListener('iceconnectionstatechange', (e) => {
        const { iceConnectionState } = e.target as RTCPeerConnection;

        if (iceConnectionState === 'checking') {
          const state = 'connecting';
          remote.state = state;
          this.emit('state', { remote, state });
        }
        else if (iceConnectionState === 'connected') {
          clearTimeout(timeout);
          const state = 'connected';
          remote.state = state;
          this.emit('state', { remote, state });
        }
        else if (iceConnectionState === 'disconnected') {
          const state = 'disconnected';
          remote.state = state;
          this.emit('state', { remote, state });
        }
        else if (iceConnectionState === 'failed') {
          const state = 'failed';
          remote.state = state;
          this.emit('state', { remote, state });
          dispose();
        }
        else if (iceConnectionState === 'closed') {
          remote.state = 'closed';
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
          this._makingOffer.add(id);

          const offer = await connection.createOffer();
          await connection.setLocalDescription(offer);

          this.driver.emit([this.room, id], {
            type: 'offer',
            id: this.id,
            data: offer,
            metadata: this.metadata,
          });
        }
        catch (error) {
          dispose();
          this.emit('error', { remote, error, code: 'NEGOTIATION_ERROR' });
        }
        finally {
          this._makingOffer.delete(id);
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
            this.emit('unpublish', { remote, stream, track });
          });
        }

        this.emit('publish', { remote, stream, track });
      });

      if (this.streams.size > 0) {
        for (const options of this.streams.values()) {
          const { stream, audioBitrate, videoBitrate, filter } = options;

          if (typeof filter === 'function') {
            const allowed = filter({ remote });
            if (!allowed) continue;
          }

          stream.getTracks().forEach(track => connection.addTrack(track, stream));

          if (audioBitrate || videoBitrate) {
            setPeerConnectionBitrate(connection, audioBitrate, videoBitrate);
          }
        }
      }

      if (this.channels.size > 0) {
        for (let [channelId, channelOptions] of this.channels.entries()) {
          const { label = '', filter, ...channelRestOptions } = channelOptions || {};

          if (typeof filter === 'function') {
            const allowed = filter({ remote });
            if (!allowed) continue;
          }

          const channel = connection.createDataChannel(
            label,
            { ...channelRestOptions, negotiated: true, id: channelId || 0 },
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
            this.emit('error', { remote, channel, error: e.error, code: 'CHANNEL_ERROR' });
          });

          channels.set(channelId, channel);
        }
      }

      this.emit('state', { remote, state: remote.state });

      return remote;
    };

    this._signal = async (e) => {
      const { type, id, data, metadata } = e;
      if (!type || !id || this.id === id) return;

      log('peer:signal', e);

      // join to the room
      if (type === 'join') {
        if (this.connections.has(id)) return;

        const hasLocalData = this.streams.size > 0 || this.channels.size > 0;
        if (!data && !hasLocalData) return;

        try {
          if (this._verify) {
            const verified = await this._verify({ id, metadata });

            log('peer:verify', { id, metadata, verified });

            if (!verified) return;
          }
        } catch (error) {
          this.emit('error', { error, code: 'VERIFY_ERROR' });
          return;
        }

        try {
          const remote = createRemote(id, metadata);
          this.connections.set(id, remote);

          if (!hasLocalData) {
            const { connection } = remote;

            try {
              this._makingOffer.add(id);

              const offer = await connection.createOffer();
              await connection.setLocalDescription(offer);

              this.driver.emit([this.room, id], {
                type: 'offer',
                id: this.id,
                data: offer,
                metadata: this.metadata,
              });
            }
            finally {
              this._makingOffer.delete(id);
            }
          }
        }
        catch (error) {
          const remote = this.connections.get(id);
          if (remote) remote.dispose();
          this.emit('error', { remote, error, code: 'JOIN_ERROR' });
        }

        return;
      }

      // set remote description and create answer
      if (type === 'offer' && data) {
        try {
          let remote = this.connections.get(id);
          if (!remote) {
            remote = createRemote(id, metadata);
            this.connections.set(id, remote);
          }

          const { connection } = remote;

          // Glare resolution (perfect negotiation): when both peers send offers
          // simultaneously, break the tie by peer ID — the peer with the greater
          // ID is "polite" and rolls back its own offer; the other ignores the
          // incoming offer and waits for the remote to answer.
          // Also check makingOffer: negotiationneeded may be mid-flight (between
          // its stable check and setLocalDescription) so signalingState is still
          // 'stable' even though a local offer is being prepared.
          const isPolite = this.id > id;
          const offerCollision = connection.signalingState !== 'stable' || this._makingOffer.has(id);
          if (offerCollision && !isPolite) return;

          await connection.setRemoteDescription(data);

          // add queued candidates
          if (this._candidateQueues.has(id)) {
            for (const candidate of this._candidateQueues.get(id) || []) {
              try {
                await connection.addIceCandidate(candidate);
              }
              catch (error) {
                this.emit('error', { remote, error, code: 'CANDIDATE_ERROR' });
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
          });
        }
        catch (error) {
          const remote = this.connections.get(id);
          if (remote) remote.dispose();
          this.emit('error', { remote, error, code: 'OFFER_ERROR' });
        }

        return;
      }

      // set remote description
      if (type === 'answer' && data) {
        const remote = this.connections.get(id);
        if (!remote) return;

        const { connection } = remote;

        try {
          await connection.setRemoteDescription(data);
        }
        catch (error) {
          remote.dispose();
          this.emit('error', { remote, error, code: 'ANSWER_ERROR' });
          return;
        }

        // add queued candidates
        if (this._candidateQueues.has(id)) {
          for (let candidate of this._candidateQueues.get(id) || []) {
            try {
              await connection.addIceCandidate(candidate);
            }
            catch (error) {
              this.emit('error', { remote, error, code: 'CANDIDATE_ERROR' });
            }
          }
          this._candidateQueues.delete(id);
        }

        return;
      }

      // add ice candidate
      if (type === 'candidate' && data) {
        const remote = this.connections.get(id);

        if (!remote) {
          if (!this._candidateQueues.has(id)) this._candidateQueues.set(id, []);
          this._candidateQueues.get(id)?.push(data);
          return;
        }

        const { connection } = remote;

        try {
          await connection.addIceCandidate(data);
        }
        catch (error) {
          this.emit('error', { remote, error, code: 'CANDIDATE_ERROR' });
        }

        return;
      }

      // leave the room
      if (type === 'leave') {
        const remote = this.connections.get(id);

        if (remote) {
          remote.dispose({ silent: true });
        }

        return;
      }
    };

    this.driver.on([this.room], this._signal);
    this.driver.on([this.room, this.id], this._signal);

    this.driver.emit([this.room], {
      type: 'join',
      id: this.id,
      data: this.streams.size > 0 || this.channels.size > 0,
      metadata: this.metadata,
    });

    log('peer:join', { room: this.room, metadata: this.metadata });
  }

  /**
    * Leave the current room and close all active remote connections.
   */
  leave() {
    if (!this._signal) return;

    this.driver.off([this.room], this._signal);
    this.driver.off([this.room, this.id], this._signal);

    for (const remote of this.connections.values()) {
      remote.dispose();
    }
    this.connections.clear();

    this._candidateQueues.clear();
    this._makingOffer.clear();

    delete this._signal;

    log('peer:leave', { room: this.room });
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
    const { id = 'default', stream, ...opts } = options;

    const hasLocalData = this.streams.size > 0 || this.channels.size > 0;
    const {
      stream: newStream = new MediaStream(),
      managed
    } = this.streams.get(id) || {};
    this.streams.set(id, { id, stream: newStream, ...opts });

    for (const track of newStream.getTracks()) {
      if (!stream.getTracks().find(t => t.id === track.id)) {
        newStream.removeTrack(track);
        if (managed) track.stop();
      }
    }
    for (const track of stream.getTracks()) {
      if (!newStream.getTracks().find(t => t.id === track.id)) {
        newStream.addTrack(track);
      }
    }

    const { audioBitrate, videoBitrate, filter } = opts;

    for (const remote of this.connections.values()) {
      if (typeof filter === 'function') {
        const allowed = filter({ remote });
        if (!allowed) continue;
      }

      const { connection } = remote;
      const senders = connection.getSenders();
      for (const track of newStream.getTracks()) {
        const sender = senders.find((sender: RTCRtpSender) => {
          return sender.track && sender.track.id === track.id
            && sender.track.readyState !== 'ended';
        });
        if (sender) sender.replaceTrack(track);
        else connection.addTrack(track, newStream);
      }
      for (const sender of senders) {
        const track = newStream.getTracks().find((track) => {
          return track.id === sender.track?.id;
        });
        if (sender.track && !track) {
          connection.removeTrack(sender);
        }
      }

      if (audioBitrate || videoBitrate) {
        setPeerConnectionBitrate(connection, audioBitrate, videoBitrate);
      }
    }

    if (!hasLocalData && this.active) {
      this.driver.emit([this.room], {
        type: 'join',
        id: this.id,
        data: true,
        metadata: this.metadata,
      });
    }

    log('peer:publish', { id, options });
  }

  /**
   * Stop publishing a previously published local stream.
   *
   * @param options Stream identifier or object containing `id`.
   */
  unpublish(options: string | number | { id: string | number }) {
    const { id = 'default' } = typeof options === 'object'
      ? options : { id: options };

    const { stream, managed } = this.streams.get(id) || {};
    const tracks = stream?.getTracks() || [];
    this.streams.delete(id);

    if (managed) {
      for (const track of tracks) {
        track.stop();
      }
    }

    for (const remote of this.connections.values()) {
      const { connection } = remote;
      const senders = connection.getSenders();
      for (const track of tracks) {
        const sender = senders.find((sender: RTCRtpSender) => {
          return sender.track && sender.track.id === track.id;
        });
        if (sender) connection.removeTrack(sender);
      }
    }

    log('peer:unpublish', { id });
  }

  /**
   * Register or create a negotiated data channel with all remote peers.
   *
   * @param options Channel options or channel id.
   */
  open(options: number | ChannelOptions) {
    const { id = 0, ...opts } = typeof options === 'object'
      ? options : { id: options };

    const hasLocalData = this.streams.size > 0 || this.channels.size > 0;
    this.channels.set(id, { id, ...opts });

    const { label = '', filter, ...channelOptions } = (opts as ChannelOptions);

    for (const remote of this.connections.values()) {
      if (remote.channels.has(id)) continue;

      if (typeof filter === 'function') {
        const allowed = filter({ remote });
        if (!allowed) continue;
      }

      const { connection } = remote;

      const channel = connection.createDataChannel(
        label,
        { ...channelOptions, negotiated: true, id },
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
        this.emit('error', { remote, channel, error: e.error, code: 'CHANNEL_ERROR' });
      });

      remote.channels.set(id, channel);
    }

    if (!hasLocalData && this.active) {
      this.driver.emit([this.room], {
        type: 'join',
        id: this.id,
        data: true,
        metadata: this.metadata,
      });
    }

    log('peer:open', { id, options });
  }

  /**
   * Close and unregister a negotiated data channel by id.
   *
   * @param options Channel id or object containing `id`.
   */
  close(options: number | { id: number }) {
    const { id = 0 } = typeof options === 'object'
      ? options : { id: options };

    this.channels.delete(id);

    for (const remote of this.connections.values()) {
      const channel = remote.channels.get(id);
      if (channel) channel.close();
      remote.channels.delete(id);
    }

    log('peer:close', { id });
  }

  /**
   * Send a message through data channels.
   *
   * If `options` is omitted, the message is sent to all open channels for every
   * connected remote peer. If `options` is a number, it is treated as channel id.
   *
   * @param message Message payload to send. This may be a string, a Blob, an ArrayBuffer, a TypedArray or a DataView object.
   * @param options Optional send options or channel id.
   */
  send(message: any, options?: number | SendOptions) {
    if (!this.active) return;

    const { id, label, filter } = typeof options === 'object'
      ? options : { id: options };

    for (const remote of this.connections.values()) {
      if (typeof id === 'number') {
        const channel = remote.channels.get(id);
        if (channel && channel.readyState === 'open') {
          if (label && channel.label !== label) continue;
          if (typeof filter === 'function') {
            const allowed = filter({ remote, channel });
            if (!allowed) continue;
          }
          channel.send(message);
        }
      }
      else {
        for (const channel of remote.channels.values()) {
          if (channel && channel.readyState === 'open') {
            if (label && channel.label !== label) continue;
            if (typeof filter === 'function') {
              const allowed = filter({ remote, channel });
              if (!allowed) continue;
            }
            channel.send(message);
          }
        }
      }
    }

    log('peer:send', { message, options });
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
  on<K extends keyof PeerEvents>(event: K | K[], handler: (...args: PeerEvents[K]) => void) {
    this._emitter.on(event, handler);
  }

  /**
   * Subscribe to an event and auto-unsubscribe after first invocation.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler.
   */
  once<K extends keyof PeerEvents>(event: K | K[], handler: (...args: PeerEvents[K]) => void) {
    this._emitter.once(event, handler);
  }

  /**
   * Remove a previously registered event listener.
   *
   * @param event Event name or list of event names.
   * @param handler Event handler to remove. If omitted, all handlers for the given event(s) will be removed.
   */
  off<K extends keyof PeerEvents>(event: K | K[], handler?: (...args: PeerEvents[K]) => void) {
    this._emitter.off(event, handler);
  }

  /**
   * Emit one or more events.
   *
   * @param event Event name or list of event names.
   * @param args Event payload.
   */
  emit<K extends keyof PeerEvents>(event: K | K[], ...args: PeerEvents[K]) {
    this._emitter.emit(event, ...args);
    log(`peer:emit:${event}`, ...args);
  }
}
