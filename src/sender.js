import EventEmitter from './utils/emitter.js';
import {
  uuid,
  defaultIceServers,
} from './utils/helpers.js';

/**
 * Manages outgoing WebRTC RTCPeerConnections and optional RTCDataChannels to
 * one or more receivers. Responsible for creating offers, attaching local
 * media tracks, exchanging ICE candidates via the signaling driver, and
 * emitting lifecycle and channel events.
 *
 * @extends {EventEmitter}
 *
 * @fires Sender#connect Emitted when a peer connection is established.
 * @fires Sender#dispose Emitted when a peer connection is closed.
 * @fires Sender#error Emitted when an error occurs.
 * @fires Sender#channel:open Emitted when a data channel is opened.
 * @fires Sender#channel:close Emitted when a data channel is closed.
 * @fires Sender#channel:error Emitted when a data channel error occurs.
 * @fires Sender#channel:message Emitted when a message is received on a data channel.
 */
export class Sender extends EventEmitter {
  /**
   * Creates an instance of Sender.
   *
   * @param {Object} config Configuration options.
   * @param {string} [config.id] Optional ID for the Sender instance. If not provided, a UUID will be generated.
   * @param {Object} [config.driver] Signaling driver. In memory by default.
   * @param {RTCIceServer[]} [config.iceServers] STUN/TURN servers to use for RTCPeerConnection.
   * @param {number} [config.connectionTimeout=30] Connection timeout in seconds.
   */
  constructor(config) {
    super();
    const {
      id,
      driver,
      iceServers = defaultIceServers,
      connectionTimeout = 30,
    } = config || {};
    this.id = id || uuid();
    this.driver = driver;
    this.iceServers = iceServers;
    this.connectionTimeout = connectionTimeout;
    this.candidateQueues = new Map();
    this.peers = new Map();
    this.streams = new Map();
    this.channels = new Map();
    this.addons = new Map();
  }

  get type() {
    return 'sender';
  }

  /**
   * Indicates whether the Sender is currently active.
   *
   * @returns {boolean} True if the Sender is started, false otherwise.
   * @readonly
   */
  get active() {
    return !!this._handler;
  }

  /**
   * Start the Sender to listen for incoming connection requests.
   *
   * @param {string} [room='default'] Room name to join.
   * @param {any} [metadata] Metadata to share with receivers.
   * @returns {void}
   */
  connect(room, metadata) {
    if (this.active) return;

    this.room = room || 'default';

    this._handler = async (e) => {
      const { type, id, candidate, answer, credentials } = e;
      if (!type || !id || id === this.id) return;

      // ADDONS: request(self, e)

      // create new peer connection
      if (type === 'invoke') {
        if (this.peers.has(id)) return;

        // if (this.verify) {
        //   const isValid = await this.verify({ id, credentials });
        //   if (!isValid) return;
        // }

        try {
          const peer = {
            id,
            connection: new RTCPeerConnection({ iceServers: this.iceServers }),
            dispose: (error) => {
              clearTimeout(timeout);
              this.peers.delete(id);

              peer.channels?.forEach(channel => channel?.close());
              peer.connection?.close();

              this.driver.emit(['rx', this.room, id], {
                type: 'dispose',
                id: this.id,
              });

              this.emit('dispose', { sender: this, peer, error });
            },
            channels: new Map(),
          };
          this.peers.set(id, peer);

          const timeout = this.connectionTimeout > 0 && setTimeout(
            () => peer.dispose(new Error('Connection timeout')),
            this.connectionTimeout * 1000,
          );

          peer.connection.addEventListener('iceconnectionstatechange', (e) => {
            const { iceConnectionState } = e.target;
            console.log('iceConnectionState', iceConnectionState, id);
            switch (iceConnectionState) {
              case 'connected':
                clearTimeout(timeout);
                this.emit('connected', { sender: this, peer });
                break;
              case 'disconnected':
                peer.dispose();
                break;
              case 'failed':
                peer.dispose(new Error('ICE connection failed'));
                break;
            }
          });

          peer.connection.addEventListener('icecandidate', (e) => {
            const { candidate } = e;
            if (!candidate) return;

            this.driver.emit(['rx', this.room, id], {
              type: 'candidate',
              id: this.id,
              candidate,
            });
          });

          peer.connection.addEventListener('negotiationneeded', async () => {
            const offer = await peer.connection.createOffer();
            await peer.connection.setLocalDescription(offer);

            // send offer
            this.driver.emit(['rx', this.room, id], {
              type: 'offer',
              id: this.id,
              offer,
              metadata,
            });
          });

          this.emit('created', { sender: this, peer });

          if (this.streams.size > 0) {
            for (let stream of this.streams) {
              stream.getTracks().forEach(track => peer.connection.addTrack(track, stream));
              // setPeerConnectionBitrate(peer.connection, this.audioBitrate, this.videoBitrate);
            }
          }

          if (this.channels.size > 0) {
            for (let [channelLabel, channelOptions] of this.channels.entries()) {
              if (peer.channels.has(channelLabel)) continue;

              if (typeof channelOptions !== 'object') channelOptions = {};

              const channel = peer.connection.createDataChannel(channelLabel, channelOptions);
              peer.channels.set(channelLabel, channel);

              this.emit('channel', { sender: this, peer, channel });

              // channel.addEventListener('open', () => {
              //   this.dispatchEvent(new CustomEvent('channel:open', {
              //     detail: { id, peer: conn.peer, channel },
              //   }));
              // }, { once: true });

              // channel.addEventListener('close', () => {
              //   this.dispatchEvent(new CustomEvent('channel:close', {
              //     detail: { id, peer: conn.peer, channel },
              //   }));
              // }, { once: true });

              // channel.addEventListener('error', (e) => {
              //   const { error } = e;
              //   this.dispatchEvent(new CustomEvent('channel:error', {
              //     detail: { id, peer: conn.peer, channel, error },
              //   }));
              // });

              // channel.addEventListener('message', (e) => {
              //   const { data } = e;
              //   this.dispatchEvent(new CustomEvent('channel:message', {
              //     detail: { id, peer: conn.peer, channel, data },
              //   }));
              // });
            }
          }

          const offer = await peer.connection.createOffer();
          await peer.connection.setLocalDescription(offer);

          // send offer
          this.driver.emit(['rx', this.room, id], {
            type: 'offer',
            id: this.id,
            offer,
            metadata,
          });
        }
        catch (error) {
          const peer = this.peers.get(id);
          if (peer) peer.dispose(error);
          else this.emit('error', { sender: this, error });
        }

        return;
      }

      // set remote description
      if (type === 'answer' && answer) {
        const peer = this.peers.get(id);
        if (!peer) return;

        try {
          await peer.connection.setRemoteDescription(new RTCSessionDescription(answer));
        }
        catch (error) {
          peer.dispose(error);
          return;
        }

        // add queued candidates
        if (this.candidateQueues.has(id)) {
          for (let candidate of this.candidateQueues.get(id)) {
            try {
              await peer.connection.addIceCandidate(new RTCIceCandidate(candidate));
            }
            catch (error) {
              this.emit('error', { sender: this, error });
            }
          }
          this.candidateQueues.delete(id);
        }

        return;
      }

      // add ice candidate
      if (type === 'candidate' && candidate) {
        const peer = this.peers.get(id);

        if (!peer) {
          if (!this.candidateQueues.has(id)) this.candidateQueues.set(id, []);
          this.candidateQueues.get(id).push(candidate);
          return;
        }

        try {
          await peer.connection.addIceCandidate(new RTCIceCandidate(candidate));
        }
        catch (error) {
          this.emit('error', { sender: this, error });
        }

        return;
      }
    };

    this.driver.on(['tx', this.room], this._handler);
    this.driver.on(['tx', this.room, this.id], this._handler);

    this.driver.emit(['rx', this.room], {
      type: 'invoke',
      id: this.id,
    });
  }

  /**
   * Stop the Sender and close all connections and data channels.
   *
   * @returns {void}
   */
  disconnect() {
    if (!this.active) return;

    this.driver.off(['tx', this.room], this._handler);
    this.driver.off(['tx', this.room, this.id], this._handler);

    for (let peer of this.peers.values()) {
      peer.dispose();
    }

    this.peers.clear();
    this.candidateQueues.clear();

    delete this._handler;
  }

  async use(label, addon) {
    if (typeof addon === 'undefined') {
      return this.addons.get(label);
    }
    if (addon instanceof MediaStream) {
      addon = new Stream(addon);
    }
    const oldAddon = this.addons.get(label);
    if (oldAddon !== addon && oldAddon) {
      await oldAddon.disable(this);
    }
    if (!addon) {
      this.addons.delete(label);
    }
    else {
      this.addons.set(label, addon);
      await addon.enable(this);
    }
    return oldAddon;
  }

  async setStream(label, stream) {
    const oldStream = this.streams.get(label);
    this.streams.set(label, stream);
    // stop old tracks
    oldStream?.getTracks().forEach(track => track.stop());
    // refresh all existing connections
    this.peers.forEach((conn) => {
      const senders = conn.peer.getSenders();
      stream.getTracks().forEach((track) => {
        const sender = senders.find((sender) => {
          return sender.track && sender.track.kind === track.kind
            && sender.track.readyState === 'ended';
        });
        if (sender) {
          sender.replaceTrack(track);
        }
        else {
          conn.peer.addTrack(track, stream);
        }
      });
    });
  }

  async deleteStream(label) {
    const stream = this.streams.get(label);
    if (!stream) return;

    this.streams.delete(label);

    this.peers.forEach((conn) => {
      const senders = conn.peer.getSenders();
      stream.getTracks().forEach((track) => {
        const sender = senders.find(sender => sender.track === track);
        if (sender) {
          conn.peer.removeTrack(sender);
        }
      });
    });
  }

  async setChannel(label, options) {
    if (typeof options !== 'object') options = {};
    this.channels.set(label, options);

    for (let peer of this.peers.values()) {
      const oldChannel = peer.channels.get(label);
      if (oldChannel) {
        oldChannel.close();
        peer.channels.delete(label);
      }
      const newChannel = peer.connection.createDataChannel(label, options);
      peer.channels.set(label, newChannel);
    }
  }

  async deleteChannel(label) {
    this.channels.delete(label);

    for (let peer of this.peers.values()) {
      const channel = peer.channels.get(label);
      if (channel) {
        channel.close();
        peer.channels.delete(label);
      }
    }
  }
}
