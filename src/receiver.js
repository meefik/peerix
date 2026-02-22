import EventEmitter from './utils/emitter.js';

import {
  uuid,
  defaultIceServers,
} from './utils/helpers.js';

/**
 * Receiver listens for signaling messages from senders and establishes WebRTC
 * RTCPeerConnection instances for incoming offers. It manages data channels and
 * remote media streams and emits events to notify callers about messages,
 * streams, disposals, and errors.
 *
 * @extends {EventEmitter}
 *
 * @fires Receiver#connect Emitted when a peer connection is established.
 * @fires Receiver#stream Emitted when a remote media stream is received.
 * @fires Receiver#dispose Emitted when a peer connection is closed.
 * @fires Receiver#error Emitted when an error occurs.
 * @fires Receiver#channel:open Emitted when a data channel is opened.
 * @fires Receiver#channel:close Emitted when a data channel is closed.
 * @fires Receiver#channel:error Emitted when a data channel error occurs.
 * @fires Receiver#channel:message Emitted when a message is received on a data channel.
 */
export class Receiver extends EventEmitter {
  /**
   * Creates an instance of Receiver.
   *
   * @param {Object} config Configuration options.
   * @param {Object} config.driver Signaling driver (required).
   * @param {RTCIceServer[]} [config.iceServers] STUN/TURN servers to use for RTCPeerConnection.
   * @param {number} [config.connectionTimeout=30] Connection timeout in seconds.
   * @param {number} [config.pingInterval=30] Ping interval in seconds to re-establish connections.
   * @param {number} [config.pingAttempts=10] Number of ping attempts after all peers are gone.
   * @throws {Error} If the driver is not provided.
   */
  constructor(config) {
    super();
    const {
      driver,
      id,
      iceServers = defaultIceServers,
      connectionTimeout = 30,
      pingInterval = 30,
      pingAttempts = 10,
    } = config || {};
    this.id = id || uuid();
    this.driver = driver;
    this.iceServers = iceServers;
    this.connectionTimeout = connectionTimeout;
    this.pingInterval = pingInterval;
    this.pingAttempts = pingAttempts;
    this.candidateQueues = new Map();
    this.peers = new Map();
  }

  /**
   * Gets the type of the Peer, which is 'receiver' for this class.
   *
   * @returns {string} The type of the Peer.
   */
  get type() {
    return 'receiver';
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
   * Start the Receiver and listen for incoming connections.
   *
   * @param {Object} options Options for starting the Receiver.
   * @param {string} [options.room='default'] Room name to join.
   * @param {Object} [options.credentials] Credentials for authentication.
   * @returns {void}
   */
  connect(room, credentials) {
    if (this.active) return;

    this.room = room || 'default';

    // const useAddon = async (method, ...args) => {
    //   try {
    //     for (let addon of this.addons.values()) {
    //       if (typeof addon[method] === 'function') {
    //         await addon[method](this, ...args);
    //       }
    //     }
    //     return true;
    //   }
    //   catch (error) {
    //     this.dispatchEvent(new CustomEvent('error', {
    //       detail: { error },
    //     }));
    //   }
    //   return false;
    // };

    this._handler = async (e) => {
      const { type, id, offer, candidate, metadata } = e;
      if (!type || !id || this.id === id) return;

      // ADDONS: request(this, e)
      // if (!await useAddon('request', e)) return;

      // request connection
      if (type === 'invoke') {
        if (this.peers.has(id)) return;

        this.driver.emit(['tx', this.room, id], {
          type: 'invoke',
          id: this.id,
          credentials,
        });

        return;
      }

      // set remote description
      if (type === 'offer' && offer) {
        try {
          // create new connection if it doesn't exist
          if (!this.peers.has(id)) {
            const peer = {
              id,
              connection: new RTCPeerConnection({ iceServers: this.iceServers }),
              metadata,
              dispose: (error) => {
                clearTimeout(timeout);
                this.peers.delete(id);

                peer.channels?.forEach(channel => channel?.close());
                peer.connection?.close();

                this.emit('disposed', { receiver: this, peer, error });
              },
              streams: new Map(),
              channels: new Map(),
            };
            this.peers.set(id, peer);

            const timeout = this.connectionTimeout > 0 && setTimeout(
              () => peer.dispose(new Error('Connection timeout')),
              this.connectionTimeout * 1000,
            );

            peer.connection.addEventListener('iceconnectionstatechange', (e) => {
              const { iceConnectionState } = e.target;
              switch (iceConnectionState) {
                case 'connected':
                  clearTimeout(timeout);
                  this.emit('connected', { receiver: this, peer });
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

              this.driver.emit(['tx', this.room, id], {
                type: 'candidate',
                id: this.id,
                candidate,
              });
            });

            peer.connection.addEventListener('datachannel', (e) => {
              const { channel } = e;
              if (peer.channels.has(channel.label)) return;

              peer.channels.set(channel.label, channel);

              this.emit('channel', { receiver: this, peer, channel });

              // channel.addEventListener('open', () => {
              //   this.dispatchEvent(new CustomEvent('channel:open', {
              //     detail: { receiver: this, peer: conn.peer, channel },
              //   }));
              // }, { once: true });

              // channel.addEventListener('close', () => {
              //   this.dispatchEvent(new CustomEvent('channel:close', {
              //     detail: { receiver: this, peer: conn.peer, channel },
              //   }));
              // }, { once: true });

              // channel.addEventListener('error', (e) => {
              //   const { error } = e;
              //   this.dispatchEvent(new CustomEvent('channel:error', {
              //     detail: { receiver: this, peer: conn.peer, channel, error },
              //   }));
              // });

              // channel.addEventListener('message', (e) => {
              //   const { data } = e;
              //   this.dispatchEvent(new CustomEvent('channel:message', {
              //     detail: { receiver: this, peer: conn.peer, channel, data },
              //   }));
              // });
            });

            peer.connection.addEventListener('track', (e) => {
              const { streams } = e;

              for (let stream of streams) {
                peer.streams.set(stream.id, stream);
              }

              this.emit('track', { receiver: this, peer, streams });
            });
          }

          const peer = this.peers.get(id);

          this.emit('created', { receiver: this, peer });

          await peer.connection.setRemoteDescription(new RTCSessionDescription(offer));

          // add queued candidates
          if (this.candidateQueues.has(id)) {
            for (let candidate of this.candidateQueues.get(id)) {
              try {
                await peer.connection.addIceCandidate(new RTCIceCandidate(candidate));
              }
              catch (error) {
                this.emit('error', { receiver: this, error });
              }
            }
            this.candidateQueues.delete(id);
          }

          await peer.connection.setLocalDescription(await peer.connection.createAnswer());

          // send answer
          this.driver.emit(['tx', this.room, id], {
            type: 'answer',
            id: this.id,
            answer: peer.connection.localDescription,
          });
        }
        catch (error) {
          const peer = this.peers.get(id);
          if (peer) peer.dispose(error);
          else this.emit('error', { receiver: this, error });
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
          this.emit('error', { receiver: this, error });
        }

        return;
      }

      // end connection
      if (type === 'dispose') {
        const peer = this.peers.get(id);
        if (!peer) return;

        peer.dispose();

        return;
      }
    };

    this.driver.on(['rx', this.room], this._handler);
    this.driver.on(['rx', this.room, this.id], this._handler);

    this.driver.emit(['tx', this.room], {
      type: 'invoke',
      id: this.id,
      credentials,
    });

    this._attempts = 0;
    this._timer = setInterval(() => {
      if (this.peers.size > 0) {
        this._attempts = this.pingAttempts;
      }
      if (this._attempts > 0) {
        this._attempts--;
        this.driver.emit(['tx', this.room], {
          type: 'invoke',
          id: this.id,
          credentials,
        });
      }
    }, this.pingInterval * 1000);
  }

  /**
   * Stop the Receiver and close all connections.
   *
   * @returns {void}
   */
  stop() {
    if (!this.active) return;

    clearInterval(this._timer);
    delete this._timer;

    this.driver.off(['rx', this.room], this._handler);
    this.driver.off(['rx', this.room, this.id], this._handler);

    for (let peer of this.peers.values()) {
      peer.dispose();
    }
    this.peers.clear();

    this.candidateQueues.clear();

    delete this._handler;
  }
}
