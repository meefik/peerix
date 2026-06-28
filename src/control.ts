import { encode, decode } from "./utils/protobuf.js";

/**
 * Protocol buffer schema for control messages.
 * Maps field names to their wire ID and type for serialization.
 */
const PACKET_SCHEMA: Record<
  string,
  { id: number; type: "uint32" | "bytes" | "bool" }
> = {
  event: { id: 1, type: "uint32" },
  payload: { id: 2, type: "bytes" },
};

/**
 * Manages internal peer-to-peer control messaging over a WebRTC data channel.
 * Internally creates a negotiated data channel (id 0) used for event delivery
 * between peers.
 */
export class ControlChannel {
  #channel: RTCDataChannel;
  #callback: ControlChannelCallback;
  #handlers: Record<string, EventListener>;

  /**
   * Indicates whether the internal data channel is open and ready for sending messages.
   */
  get active(): boolean {
    return this.#channel.readyState === "open";
  }

  /**
   * Creates a new {@link ControlChannel} instance.
   */
  constructor(options: {
    connection: RTCPeerConnection;
    callback: ControlChannelCallback;
  }) {
    const { connection, callback } = options;

    this.#callback = callback;

    this.#channel = connection.createDataChannel("", {
      id: 0,
      negotiated: true,
      ordered: true,
    });
    this.#channel.binaryType = "arraybuffer";

    this.#handlers = {
      open: this.#handleOpen.bind(this),
      close: this.#handleClose.bind(this),
      error: this.#handleError.bind(this),
      message: this.#handleMessage.bind(this),
    };

    for (const [event, handler] of Object.entries(this.#handlers)) {
      this.#channel.addEventListener(event, handler);
    }
  }

  /**
   * Closes the internal data channel.
   */
  close(): void {
    if (this.#channel.readyState !== "closed") {
      this.#channel.close();
    }
  }

  /**
   * Sends a control message through the internal data channel.
   * The event and optional payload are encoded as a protocol buffer.
   *
   * @param event Event code to send.
   * @param message Data to attach to the event.
   * @returns `true` if the message was sent successfully, otherwise `false`.
   */
  send(event: number, message: object): boolean {
    if (this.#channel.readyState !== "open") {
      return false;
    }

    const buffer = this.#encode(event, message);
    if (!buffer) return false;

    this.#channel.send(buffer);

    return true;
  }

  /**
   * Encodes an event and its payload into a protocol buffer byte array.
   */
  #encode(event: number, message: object): Uint8Array<ArrayBuffer> | null {
    const payload = new TextEncoder().encode(JSON.stringify(message));
    return encode<Packet>({ event, payload }, PACKET_SCHEMA);
  }

  /**
   * Decodes a protocol buffer byte array back into an event and message.
   */
  #decode(
    buffer: Uint8Array<ArrayBuffer>,
  ): { event: number; message: object } | null {
    const { event, payload } = decode<Packet>(buffer, PACKET_SCHEMA) ?? {};
    if (typeof event === "number" && payload !== undefined) {
      const message = JSON.parse(new TextDecoder().decode(payload));
      return { event, message };
    }
    return null;
  }

  /**
   * Handles the data channel's open event by notifying the caller.
   */
  #handleOpen(): void {
    this.#callback.open();
  }

  /**
   * Handles the data channel's close event by notifying the caller.
   */
  #handleClose(): void {
    for (const [event, handler] of Object.entries(this.#handlers)) {
      this.#channel.removeEventListener(event, handler);
    }

    this.#callback.close();
  }

  /**
   * Handles the channel's `error` event.
   */
  #handleError(e: Event): void {
    const { error } = e as RTCErrorEvent;
    this.#callback.error(error);
  }

  /**
   * Handles incoming messages on the data channel.
   * Decodes the protocol buffer payload and dispatches to the message callback.
   * Errors during decoding are forwarded to the error callback.
   */
  #handleMessage(e: Event): void {
    try {
      const { data } = e as MessageEvent;
      const decoded = this.#decode(new Uint8Array(data as ArrayBuffer));
      if (decoded) {
        const { event, message } = decoded;
        this.#callback.message(event, message);
      }
    } catch (err) {
      this.#callback.error(err);
    }
  }
}

/** Callback interface for handling control channel events. */
interface ControlChannelCallback {
  open: () => void;
  close: () => void;
  error: (error: unknown) => void;
  message: (event: number, message: object) => void;
}

/** Protobuf-encoded packet structure for data channel communication. */
interface Packet {
  event: number;
  payload: Uint8Array;
}
