import type { ChannelOptions } from './peer.js';
import { EventEmitter } from './utils/emitter.js';

/**
 * Manages internal peer-to-peer control messaging over a WebRTC data channel.
 * Internally creates a negotiated data channel (id 0) used for event delivery
 * between peers.
 */
export class ConnectionManager {
  #emitter: EventEmitter<ConnectionManagerEvents>;
  #channel?: RTCDataChannel;

  /**
   * Indicates whether the internal data channel is open and ready for sending messages.
   */
  get active() {
    return this.#channel?.readyState === 'open';
  }

  /**
   * Creates a new `ConnectionManager` instance.
   */
  constructor() {
    this.#emitter = new EventEmitter(this);
  }

  /**
   * Initialises the internal data channel.
   *
   * @param connection The underlying WebRTC peer connection to manage.
   */
  open(connection: RTCPeerConnection) {
    const channel = connection.createDataChannel('', {
      negotiated: true,
      id: 0,
    });
    this.#channel = channel;

    channel.addEventListener('open', () => {
      this.emit('open');
    });

    channel.addEventListener('close', () => {
      this.emit('close');
    });

    channel.addEventListener('message', (e) => {
      const [event, ...payload] = JSON.parse(e.data);
      this.emit(event, ...payload);
    });
  }

  /**
   * Closes the internal data channel.
   */
  close() {
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
  send<K extends keyof ConnectionManagerEvents>(
    event: K,
    ...payload: ConnectionManagerEvents[K]
  ) {
    if (!this.active) return;
    this.#channel!.send(JSON.stringify([event, ...payload]));
  }

  /**
   * Registers a listener for one or more connection manager events.
   *
   * @param event The event name or array of event names to listen for.
   * @param handler The callback invoked when the event fires.
   */
  on<K extends keyof ConnectionManagerEvents>(
    event: K | K[],
    handler: (...args: ConnectionManagerEvents[K]) => void,
  ) {
    this.#emitter.on(event, handler);
  }

  /**
   * Removes a previously registered listener.
   *
   * @param event The event name or array of event names to stop listening for.
   * @param handler The callback to remove.
   */
  off<K extends keyof ConnectionManagerEvents>(
    event: K | K[],
    handler: (...args: ConnectionManagerEvents[K]) => void,
  ) {
    this.#emitter.off(event, handler);
  }

  /**
   * Emits an event, scheduling all registered listeners for execution.
   *
   * @param event The event name or array of event names to emit.
   * @param args Arguments passed to each listener.
   */
  emit<K extends keyof ConnectionManagerEvents>(
    event: K | K[],
    ...args: ConnectionManagerEvents[K]
  ) {
    this.#emitter.emit(event, ...args);
  }
}

/**
 * Events emitted by {@link ConnectionManager}.
 */
export interface ConnectionManagerEvents {
  /** Internal data channel opens successfully. */
  open: [];
  /** Internal data channel closes. */
  close: [];
  /** New data channel is requested. */
  channel: [ChannelOptions];
  /** Signal event is received. */
  signal: [
    (
      // data
      RTCSessionDescriptionInit | RTCIceCandidateInit
    ),
    // labels
    Record<string, string>?,
  ];
}
