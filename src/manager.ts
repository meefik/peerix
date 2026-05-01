import type { ChannelOptions } from './peer.js';
import { EventEmitter } from './utils/emitter.js';
import { Timeout } from './utils/timeout.js';

/**
 * Manages a WebRTC connection, handling timeouts, pings, and message passing.
 * Internally creates a negotiated data channel (id 0) used for keep-alive pings
 * and arbitrary event delivery between peers.
 */
export class ConnectionManager {
  #emitter: EventEmitter<ConnectionManagerEvents>;
  #connection: RTCPeerConnection;
  #connectionTimeout: number;
  #timeout: Timeout;
  #channel?: RTCDataChannel;
  #connectionStateHandler: () => void;
  #pingTimer?: ReturnType<typeof setInterval>;

  /**
   * Indicates whether the internal data channel is open and ready for sending messages.
   */
  get active() {
    return this.#channel?.readyState === 'open';
  }

  /**
   * Creates a new `ConnectionManager` instance.
   *
   * @param options Configuration options for the connection manager.
   */
  constructor(options: ConnectionManagerOptions) {
    const { connection, connectionTimeout } = options;

    this.#emitter = new EventEmitter(this);
    this.#connection = connection;
    this.#connectionTimeout = connectionTimeout || 0;

    this.#timeout = new Timeout(() => {
      this.emit('timeout');
    }, this.#connectionTimeout * 1000);

    this.#connectionStateHandler = () => {
      const { iceConnectionState } = this.#connection;
      if (iceConnectionState === 'connected') {
        this.#timeout.clear();
      }
      if (iceConnectionState === 'disconnected') {
        this.#timeout.start();
      }
    };
  }

  /**
   * Initialises the internal data channel, starts the connection timeout,
   * and begins emitting keep-alive pings once the channel is open.
   */
  open() {
    this.#connection.addEventListener('iceconnectionstatechange', this.#connectionStateHandler);

    const channel = this.#connection.createDataChannel('', { negotiated: true, id: 0 });
    this.#channel = channel;

    const pingInterval = Math.max(1000, Math.ceil(this.#connectionTimeout * 1000 / 2));
    let t = Date.now();

    channel.addEventListener('open', () => {
      clearInterval(this.#pingTimer);
      this.#pingTimer = setInterval(() => this.send('ping'), pingInterval);
      this.#timeout.start();
      this.emit('open');
    });

    channel.addEventListener('close', () => {
      this.close();
      this.emit('close');
    });

    channel.addEventListener('message', (e) => {
      const [event, ...payload] = JSON.parse(e.data);
      if (event === 'ping') {
        const now = Date.now();
        if (now - t > pingInterval * 2) this.#timeout.start();
        else this.#timeout.clear();
        t = now;
      }
      else {
        this.emit(event, ...payload);
      }
    });

    this.#timeout.start();
  }

  /**
   * Cancels the connection timeout and closes the internal data channel.
   */
  close() {
    this.#timeout.clear();
    this.#connection.removeEventListener('iceconnectionstatechange', this.#connectionStateHandler);
    if (this.#pingTimer) {
      clearInterval(this.#pingTimer);
      this.#pingTimer = undefined;
    }
    if (this.#channel) {
      this.#channel.close();
      this.#channel = undefined;
    }
  }

  /**
   * Sends a JSON-encoded event through the internal data channel.
   * The message is silently dropped when the channel is not open.
   *
   * @param event The event name to send.
   * @param payload Optional data to attach to the event.
   */
  send<K extends keyof ConnectionManagerEvents>(event: K, ...payload: ConnectionManagerEvents[K]) {
    this.#channel?.send(JSON.stringify([event, ...payload]));
  }

  /**
   * Registers a listener for one or more connection manager events.
   *
   * @param event The event name or array of event names to listen for.
   * @param handler The callback invoked when the event fires.
   */
  on<K extends keyof ConnectionManagerEvents>(event: K | K[], handler: (...args: ConnectionManagerEvents[K]) => void) {
    this.#emitter.on(event, handler);
  }

  /**
   * Removes a previously registered listener.
   *
   * @param event The event name or array of event names to stop listening for.
   * @param handler The callback to remove.
   */
  off<K extends keyof ConnectionManagerEvents>(event: K | K[], handler: (...args: ConnectionManagerEvents[K]) => void) {
    this.#emitter.off(event, handler);
  }

  /**
   * Emits an event, invoking all registered listeners synchronously.
   *
   * @param event The event name or array of event names to emit.
   * @param args Arguments passed to each listener.
   */
  emit<K extends keyof ConnectionManagerEvents>(event: K | K[], ...args: ConnectionManagerEvents[K]) {
    this.#emitter.emit(event, ...args);
  }
}

/**
 * Configuration options for {@link ConnectionManager}.
 */
export interface ConnectionManagerOptions {
  /** The underlying WebRTC peer connection to manage. */
  connection: RTCPeerConnection;
  /** Timeout in seconds before a stalled connection is considered failed. */
  connectionTimeout: number;
}

/**
 * Events emitted by {@link ConnectionManager}.
 */
export interface ConnectionManagerEvents {
  /** Internal data channel opens successfully. */
  'open': [];
  /** Internal data channel closes. */
  'close': [];
  /** Connection error or timeout occurs. */
  'timeout': [];
  /** Ping is received. */
  'ping': [];
  /** Offer is created and sent. */
  'offer': [RTCSessionDescriptionInit, { [key: string]: string; }];
  /** Answer is created and sent. */
  'answer': [RTCSessionDescriptionInit];
  /** ICE candidate is received. */
  'candidate': [RTCIceCandidateInit];
  /** New data channel is requested. */
  'channel': [ChannelOptions];
}
