import EventEmitter from './utils/emitter.js';

import {
  uuid,
  defaultIceServers,
} from './utils/helpers.js';

/**
 * Peer listens for signaling messages from senders and establishes WebRTC
 * RTCPeerConnection instances for incoming offers. It manages data channels and
 * remote media streams and emits events to notify callers about messages,
 * streams, disposals, and errors.
 *
 * @extends {EventEmitter}
 *
 * @fires Peer#connect Emitted when a peer connection is established.
 * @fires Peer#stream Emitted when a remote media stream is received.
 * @fires Peer#dispose Emitted when a peer connection is closed.
 * @fires Peer#error Emitted when an error occurs.
 * @fires Peer#channel:open Emitted when a data channel is opened.
 * @fires Peer#channel:close Emitted when a data channel is closed.
 * @fires Peer#channel:error Emitted when a data channel error occurs.
 * @fires Peer#channel:message Emitted when a message is received on a data channel.
 */
export class Peer extends Map {
  /**
   * Creates an instance of Peer.
   *
   * @param {Object} driver Signaling driver (required).
   * @param {Object} config Configuration options.
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
    this.id = id || uuid();
    this.driver = driver;
    this.iceServers = iceServers;
    this.connectionTimeout = connectionTimeout;
    this.pingInterval = pingInterval;
    this.pingAttempts = pingAttempts;
    this.emitter = new EventEmitter(this);
    this.candidateQueues = new Map();
    this.streams = new Set();
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

      // peer.pc.addEventListener('connectionstatechange', (e) => {
      //   const { connectionState } = e.target;
      //   console.log('connectionState', connectionState, id);
      //   this.emit('state', { peer, state: connectionState });
      // });

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
        // else if (iceConnectionState === 'closed') {
        //   peer.dispose();
        // }
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
        console.log('Negotiation needed for peer', id);

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

      peer.pc.addEventListener('datachannel', (e) => {
        const { channel } = e;
        if (peer.channels.has(channel.label)) return;

        peer.channels.set(channel.label, channel);

        this.emit(['channel', `channel:${channel.label}`], { peer, channel });

        channel.addEventListener('open', () => {
          this.emit(['open', `open:${channel.label}`], { peer, channel });
        });

        channel.addEventListener('close', () => {
          this.emit(['close', `close:${channel.label}`], { peer, channel });
        });

        channel.addEventListener('message', (e) => {
          this.emit(['message', `message:${channel.label}`], { peer, channel, data: e.data });
        });

        channel.addEventListener('error', (e) => {
          this.emit(['error', `error:${channel.label}`], { peer, channel, error: e.error });
        });
      });

      peer.pc.addEventListener('track', (e) => {
        const { streams } = e;

        for (let stream of streams) {
          peer.streams.set(stream.id, stream);
        }

        this.emit('track', { peer, streams });
      });

      if (this.streams.size > 0) {
        for (let stream of this.streams) {
          stream.getTracks().forEach(track => peer.pc.addTrack(track, stream));
          // setPeerConnectionBitrate(peer.connection, this.audioBitrate, this.videoBitrate);
        }
      }

      if (this.channels.size > 0) {
        for (let [channelLabel, channelOptions] of this.channels.entries()) {
          if (peer.channels.has(channelLabel)) continue;

          if (typeof channelOptions !== 'object') channelOptions = {};

          console.log('Creating data channel', channelLabel);
          const channel = peer.pc.createDataChannel(channelLabel, channelOptions);
          peer.channels.set(channelLabel, channel);

          channel.addEventListener('open', () => {
            this.emit(['open', `open:${channel.label}`], { peer, channel });
          });

          channel.addEventListener('close', () => {
            this.emit(['close', `close:${channel.label}`], { peer, channel });
          });

          channel.addEventListener('message', (e) => {
            this.emit(['message', `message:${channel.label}`], { peer, channel, data: e.data });
          });

          channel.addEventListener('error', (e) => {
            this.emit(['error', `error:${channel.label}`], { peer, channel, error: e.error });
          });
        }
      }

      return peer;
    };

    this._handler = async (e) => {
      const { type, id, data, metadata } = e;
      if (!type || !id || this.id === id) return;

      console.log('Received message', e);

      // join the room for receiving only
      if (type === 'join') {
        if (this.has(id)) return;
        if (!this.streams.size && !this.channels.size) return;

        try {
          const peer = createRemotePeer(id, metadata);
          this.set(id, peer);
        }
        catch (error) {
          const peer = this.get(id);
          if (peer) peer.dispose(error);
          this.emit('error', { peer, error });
        }

        return;
      }

      // request to share data or media
      if (type === 'invite') {
        if (this.has(id)) return;

        try {
          const peer = createRemotePeer(id, metadata);
          this.set(id, peer);

          if (!this.streams.size && !this.channels.size) {
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

          await peer.pc.setRemoteDescription(new RTCSessionDescription(data));

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
          await peer.pc.setRemoteDescription(new RTCSessionDescription(data));
        }
        catch (error) {
          peer.dispose(error);
          return;
        }

        clearTimeout(peer.timeout);
        delete peer.timeout;

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

      // close connection
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
      type: !this.streams.size && !this.channels.size
        ? 'join'
        : 'invite',
      id: this.id,
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

    this.forEach(peer => peer.dispose());
    this.clear();

    this.candidateQueues.clear();

    delete this._handler;
  }

  publish(stream) {
    const isEmpty = !this.streams.size && !this.channels.size;
    this.streams.add(stream);

    if (!this.active) return;

    if (isEmpty) {
      this.driver.emit([this.room], {
        type: 'invite',
        id: this.id,
        metadata: this.metadata,
      });
    }
    else {
      this.forEach((peer) => {
        const { pc } = peer;
        const senders = pc.getSenders();
        for (const stream of this.streams) {
          stream.getTracks().forEach((track) => {
            const sender = senders.find((sender) => {
              return sender.track && sender.track.kind === track.kind
                && sender.track.readyState === 'ended';
            });
            if (sender) sender.replaceTrack(track);
            else pc.addTrack(track, stream);
          });
        }
      });
    }
  }

  unpublish(stream) {
    this.streams.delete(stream);
    const isEmpty = !this.streams.size && !this.channels.size;

    if (!this.active) return;

    if (isEmpty) {
      this.forEach((peer) => {
        peer.dispose();
      });
    }
    else {
      this.forEach((peer) => {
        const { pc } = peer;
        const senders = pc.getSenders();
        for (const stream of this.streams) {
          stream.getTracks().forEach((track) => {
            const sender = senders.find(sender => sender.track === track);
            if (sender) pc.removeTrack(sender);
          });
        }
      });
    }
  }

  open(label, options) {
    const isEmpty = !this.streams.size && !this.channels.size;
    this.channels.set(label, options);

    if (!this.active) return;

    if (isEmpty) {
      this.driver.emit([this.room], {
        type: 'invite',
        id: this.id,
        metadata: this.metadata,
      });
    }
    else {
      this.forEach((peer) => {
        if (peer.channels.has(label)) return;
        // if (peer.channels.has(label)) {
        //   const channel = peer.channels.get(label);
        //   if (channel) channel.close();
        //   peer.channels.delete(label);
        // }
        const { pc } = peer;
        const channel = pc.createDataChannel(label, options);
        peer.channels.set(label, channel);
      });
    }
  }

  close(label) {
    this.channels.delete(label);
    const isEmpty = !this.streams.size && !this.channels.size;

    if (!this.active) return;

    if (isEmpty) {
      this.forEach((peer) => {
        peer.dispose();
      });
    }
    else {
      this.forEach((peer) => {
        const channel = peer.channels.get(label);
        if (channel) channel.close();
        peer.channels.delete(label);
      });
    }
  }

  send(message, label) {
    if (!this.active) return;

    this.forEach((peer) => {
      if (!label) {
        peer.channels.forEach((channel) => {
          if (channel.readyState === 'open') {
            channel.send(message);
          }
        });
      }
      else {
        const channel = peer.channels.get(label);
        if (channel && channel.readyState === 'open') {
          channel.send(message);
        }
      }
    });
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
