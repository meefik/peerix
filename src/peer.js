import EventEmitter from './utils/emitter.js';

import {
  UUIDv4,
  hashFNV1a,
  defaultIceServers,
} from './utils/helpers.js';

/**
 * Peer class for managing WebRTC peer connections and signaling.
 */
export class Peer extends Map {
  /**
   * Creates an instance of Peer.
   *
   * @param {Object} driver Signaling driver (required).
   * @param {Object} [config] Configuration options.
   * @param {RTCIceServer[]} [config.iceServers] STUN/TURN servers to use for RTCPeerConnection.
   * @param {number} [config.connectionTimeout=30] Connection timeout in seconds.
   * @param {number} [config.pingInterval=30] Ping interval in seconds to re-establish connections.
   * @param {number} [config.pingAttempts=10] Number of ping attempts after all peers are gone.
   * @throws {Error} If the driver is not provided.
   */
  constructor(driver, config) {
    super();
    const {
      id,
      iceServers = defaultIceServers,
      connectionTimeout = 10,
      pingInterval = 30,
      pingAttempts = 10,
    } = config || {};
    this.id = id || UUIDv4();
    this.driver = driver;
    this.iceServers = iceServers;
    this.connectionTimeout = connectionTimeout;
    this.pingInterval = pingInterval;
    this.pingAttempts = pingAttempts;
    this.emitter = new EventEmitter(this);
    this.candidateQueues = new Map();
    this.streams = new Map();
    this.channels = new Map();
    this.addons = new Set();
  }

  /**
   * Indicates whether the Receiver is currently active.
   *
   * @returns {boolean} True if the Receiver is started, false otherwise.
   */
  get active() {
    return !!this._handler;
  }

  /**
   * Start listening for incoming connections.
   *
   * @param {string} [room='default'] Room name to join.
   * @param {Object} [metadata] Metadata for the connection.
   * @returns {void}
   */
  connect(room, metadata) {
    if (this.active) return;

    this.room = room || 'default';
    this.metadata = metadata;

    const createRemotePeer = (id, metadata) => {
      const peer = {
        id,
        metadata,
        pc: new RTCPeerConnection({ iceServers: this.iceServers }),
        channels: new Map(),
        streams: new Map(),
        dispose: (error) => {
          console.trace('Disposing peer', peer, error);
          if (!this.has(id)) return;

          clearTimeout(timeout);

          this.delete(id);
          this.candidateQueues.delete(id);

          if (error) peer.error = error;
          peer.channels?.forEach(channel => channel?.close());
          peer.pc?.close();

          this.driver.emit([this.room, id], {
            type: 'leave',
            id: this.id,
          });

          this.emit('disconnect', { peer });
        },
      };

      const timeout = this.connectionTimeout > 0 && setTimeout(
        () => peer.dispose(new Error('Connection timeout')),
        this.connectionTimeout * 1000,
      );

      peer.pc.addEventListener('iceconnectionstatechange', (e) => {
        const { iceConnectionState } = e.target;
        console.log('iceConnectionState', iceConnectionState, id);
        if (iceConnectionState === 'connected') {
          clearTimeout(timeout);
          this.emit('connect', { peer });
        }
        else if (iceConnectionState === 'disconnected') {
          peer.dispose();
        }
        else if (iceConnectionState === 'failed') {
          // if (peer.pc.restartIce) peer.pc.restartIce();
          // else
          peer.dispose(new Error('ICE connection failed'));
        }
        else if (iceConnectionState === 'closed') {
          peer.dispose();
        }
      });

      peer.pc.addEventListener('icecandidate', (e) => {
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

      peer.pc.addEventListener('negotiationneeded', async () => {
        try {
          const offer = await peer.pc.createOffer();
          await peer.pc.setLocalDescription(offer);

          // send offer
          this.driver.emit([this.room, id], {
            type: 'offer',
            id: this.id,
            data: typeof offer.toJSON === 'function'
              ? offer.toJSON()
              : offer,
            metadata,
          });
        }
        catch (error) {
          peer.dispose(error);
          this.emit('error', { peer, error });
        }
      });

      peer.pc.addEventListener('track', (e) => {
        const { track, streams: [stream] } = e;

        if (!peer.streams.has(stream.id)) {
          peer.streams.set(stream.id, stream);
          stream.addEventListener('removetrack', (e) => {
            const { track } = e;
            if (!stream.getTracks().length) {
              peer.streams.delete(stream.id);
            }
            this.emit('unpublish', { peer, stream, track });
          });
        }

        this.emit('publish', { peer, stream, track });
      });

      if (this.streams.size > 0) {
        for (let stream of this.streams.values()) {
          stream.getTracks().forEach(track => peer.pc.addTrack(track, stream));
          // setPeerConnectionBitrate(peer.connection, this.audioBitrate, this.videoBitrate);
        }
      }

      if (this.channels.size > 0) {
        for (let [id, options] of this.channels.entries()) {
          const defaultLabel = typeof id === 'string' ? id : '';
          const { label = defaultLabel, ...channelOptions } = options || {};
          const generatedId = typeof id === 'number' ? id : hashFNV1a(`${id}`);

          const channel = peer.pc.createDataChannel(
            label,
            { ...channelOptions, negotiated: true, id: generatedId },
          );
          channel.addEventListener('open', () => {
            this.emit('open', { peer, channel });
          });
          channel.addEventListener('close', () => {
            this.emit('close', { peer, channel });
          });
          channel.addEventListener('message', (e) => {
            this.emit('message', { peer, channel, data: e.data });
          });
          channel.addEventListener('error', (e) => {
            this.emit('error', { peer, channel, error: e.error });
          });

          peer.channels.set(id, channel);
        }
      }

      return peer;
    };

    this._handler = async (e) => {
      const { type, id, data, metadata } = e;
      if (!type || !id || this.id === id) return;

      console.log('Received message', e);

      // join to the room
      if (type === 'join' && data) {
        if (this.has(id)) return;

        const hasLocalData = this.streams.size > 0 || this.channels.size > 0;
        if (!data && !hasLocalData) return;

        try {
          const peer = createRemotePeer(id, metadata);
          this.set(id, peer);

          if (!hasLocalData) {
            const offer = await peer.pc.createOffer();
            await peer.pc.setLocalDescription(offer);

            // send offer
            this.driver.emit([this.room, id], {
              type: 'offer',
              id: this.id,
              data: typeof offer.toJSON === 'function'
                ? offer.toJSON()
                : offer,
              metadata,
            });
          }
        }
        catch (error) {
          const peer = this.get(id);
          if (peer) peer.dispose(error);
          this.emit('error', { peer, error });
        }

        return;
      }

      // set remote description and create answer
      if (type === 'offer' && data) {
        try {
          // create new connection if it doesn't exist
          if (!this.has(id)) {
            const peer = createRemotePeer(id, metadata);
            this.set(id, peer);
          }

          const peer = this.get(id);

          await peer.pc.setRemoteDescription(data);

          // add queued candidates
          if (this.candidateQueues.has(id)) {
            for (let candidate of this.candidateQueues.get(id)) {
              try {
                await peer.pc.addIceCandidate(candidate);
              }
              catch (error) {
                this.emit('error', { peer, error });
              }
            }
            this.candidateQueues.delete(id);
          }

          const answer = await peer.pc.createAnswer();
          await peer.pc.setLocalDescription(answer);

          // send answer
          this.driver.emit([this.room, id], {
            type: 'answer',
            id: this.id,
            data: typeof answer.toJSON === 'function'
              ? answer.toJSON()
              : answer,
          });
        }
        catch (error) {
          const peer = this.get(id);
          if (peer) peer.dispose(error);
          this.emit('error', { peer, error });
        }

        return;
      }

      // set remote description
      if (type === 'answer' && data) {
        const peer = this.get(id);
        if (!peer) return;

        try {
          await peer.pc.setRemoteDescription(data);
        }
        catch (error) {
          peer.dispose(error);
          return;
        }

        // add queued candidates
        if (this.candidateQueues.has(id)) {
          for (let candidate of this.candidateQueues.get(id)) {
            try {
              await peer.pc.addIceCandidate(new RTCIceCandidate(candidate));
            }
            catch (error) {
              this.emit('error', { peer, error });
            }
          }
          this.candidateQueues.delete(id);
        }

        return;
      }

      // add ice candidate
      if (type === 'candidate' && data) {
        const peer = this.get(id);

        if (!peer) {
          if (!this.candidateQueues.has(id)) this.candidateQueues.set(id, []);
          this.candidateQueues.get(id).push(data);
          return;
        }

        try {
          await peer.pc.addIceCandidate(new RTCIceCandidate(data));
        }
        catch (error) {
          this.emit('error', { peer, error });
        }

        return;
      }

      // leave the room
      if (type === 'leave') {
        const peer = this.get(id);

        if (peer) {
          peer.dispose();
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

    // this._attempts = 0;
    // this._timer = setInterval(() => {
    //   if (this.peers.size > 0) {
    //     this._attempts = this.pingAttempts;
    //   }
    //   if (this._attempts > 0) {
    //     this._attempts--;
    //     this.invoke();
    //   }
    // }, this.pingInterval * 1000);
  }

  /**
   * Close all connections and clean up resources.
   *
   * @returns {void}
   */
  disconnect() {
    if (!this.active) return;

    // clearInterval(this._timer);
    // delete this._timer;

    this.driver.off([this.room], this._handler);
    this.driver.off([this.room, this.id], this._handler);

    for (const peer of this.values()) {
      peer.dispose();
    }
    this.clear();

    this.candidateQueues.clear();

    delete this._handler;
  }

  publish(stream, id) {
    if (!id) id = stream.id;
    const hasLocalData = this.streams.size > 0 || this.channels.size > 0;
    const newStream = this.streams.get(id) || new MediaStream();
    this.streams.set(id, newStream);

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

    for (const peer of this.values()) {
      const { pc } = peer;
      const senders = pc.getSenders();
      for (const track of newStream.getTracks()) {
        const sender = senders.find((sender) => {
          return sender.track && sender.track.id === track.id
            && sender.track.readyState !== 'ended';
        });
        if (sender) sender.replaceTrack(track);
        else pc.addTrack(track, newStream);
      }
      for (const sender of senders) {
        if (sender.track && !newStream.getTracks().find(t => t.id === sender.track.id)) {
          pc.removeTrack(sender);
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

  unpublish(id) {
    if (typeof id === 'object') {
      id = id.id;
    }
    const stream = this.streams.get(id);
    this.streams.delete(id);

    if (!this.active) return;

    for (const peer of this.values()) {
      const { pc } = peer;
      const senders = pc.getSenders();
      for (const track of stream.getTracks()) {
        const sender = senders.find((sender) => {
          return sender.track && sender.track.id === track.id;
        });
        if (sender) pc.removeTrack(sender);
      }
    }
  }

  open(id, options = {}) {
    const hasLocalData = this.streams.size > 0 || this.channels.size > 0;
    this.channels.set(id, options);

    if (!this.active) return;

    for (const peer of this.values()) {
      if (peer.channels.has(id)) continue;

      const defaultLabel = typeof id === 'string' ? id : '';
      const { label = defaultLabel, ...channelOptions } = options || {};
      const generatedId = typeof id === 'number' ? id : hashFNV1a(`${id}`);

      const { pc } = peer;
      const channel = pc.createDataChannel(
        label,
        { ...channelOptions, negotiated: true, id: generatedId },
      );
      channel.addEventListener('open', () => {
        this.emit('open', { peer, channel });
      });
      channel.addEventListener('close', () => {
        this.emit('close', { peer, channel });
      });
      channel.addEventListener('message', (e) => {
        this.emit('message', { peer, channel, data: e.data });
      });
      channel.addEventListener('error', (e) => {
        this.emit('error', { peer, channel, error: e.error });
      });

      peer.channels.set(id, channel);
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

  close(id) {
    this.channels.delete(id);

    if (!this.active) return;

    for (const peer of this.values()) {
      const channel = peer.channels.get(id);
      if (channel) channel.close();
      peer.channels.delete(id);
    }
  }

  send(message, id) {
    if (!this.active) return;

    for (const peer of this.values()) {
      if (!id) {
        for (const channel of peer.channels.values()) {
          if (channel.readyState === 'open') {
            channel.send(message);
          }
        }
      }
      else {
        const channel = peer.channels.get(id);
        if (channel && channel.readyState === 'open') {
          channel.send(message);
        }
      }
    }
  }

  async attach(addon) {
    await addon.attach(this);
    this.addons.add(addon);
  }

  async detach(addon) {
    await addon.detach(this);
    this.addons.delete(addon);
  }

  on(event, handler) {
    this.emitter.on(event, handler);
  }

  once(event, handler) {
    this.emitter.once(event, handler);
  }

  off(event, handler) {
    this.emitter.off(event, handler);
  }

  emit(event, ...args) {
    this.emitter.emit(event, ...args);
  }
}
