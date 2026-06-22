import { encode, decode } from "./utils/protobuf.js";
import { dataToStream, streamToChunks } from "./utils/stream.js";

/** Size of the protobuf header in bytes. */
const HEADER_SIZE = 64;
/** Maximum payload per chunk (16 KB MTU minus header). */
const CHUNK_SIZE = 16 * 1024 - HEADER_SIZE;
/** Threshold that triggers the `bufferedamountlow` event. */
const BUFFERED_AMOUNT_LOW = 32 * 1024;
/** Upper bound on buffered bytes before back-pressure kicks in. */
const BUFFERED_AMOUNT_MAX = 256 * 1024;
/** How long to wait for the channel buffer to drain before giving up. */
const DRAIN_TIMEOUT = 10 * 1000;
/** Maximum 16-bit unsigned integer used for message IDs (wraps around). */
const MAX_MESSAGE_ID = 0xffff;
/** Maximum number of concurrent messages that can be sent. */
const MAX_CONCURRENT_MESSAGES = 1024;

/** Map from human-readable type names to their numeric identifiers. */
const MESSAGE_TYPE = {
  stream: 1, // ReadableStream
  text: 2, // UTF-8 string
  json: 3, // JSON-serializable object
  blob: 4, // Blob or File
  bytes: 5, // ArrayBuffer or Uint8Array
} as const;

/** Protobuf schema for data packets. */
const PACKET_SCHEMA: Record<
  string,
  { id: number; type: "uint32" | "bytes" | "bool" }
> = {
  id: { id: 1, type: "uint32" },
  index: { id: 3, type: "uint32" },
  type: { id: 2, type: "uint32" },
  abort: { id: 4, type: "bool" },
  done: { id: 5, type: "bool" },
  info: { id: 6, type: "bytes" },
  chunk: { id: 7, type: "bytes" },
};

/**
 * Handles bidirectional data transfer over an RTCDataChannel.
 *
 * Messages are split into protobuf-encoded chunks, sent in order, and
 * reassembled on the receiving side. The class provides back-pressure
 * management so that a fast sender doesn't overwhelm the channel buffer.
 */
export class DataChannel {
  #peerId: string;
  #messageId: number;
  #channel: RTCDataChannel;
  #callback: DataChannelCallback;
  #handlers: Record<string, EventListener>;
  #outgoingQueue: Promise<unknown>;
  #incomingQueue: Map<string, MessageController>;
  #textEncoder: TextEncoder;
  #textDecoder: TextDecoder;

  /**
   * Creates a new DataChannel instance.
   *
   * @param channel The WebRTC data channel to transfer data over.
   * @param callback Callback functions to handle propagating events.
   */
  constructor(options: {
    peer: string;
    channel: RTCDataChannel;
    callback: DataChannelCallback;
  }) {
    const { peer, channel, callback } = options;

    this.#peerId = peer;
    this.#channel = channel;
    this.#callback = callback;

    this.#messageId = 0;
    this.#outgoingQueue = Promise.resolve();
    this.#incomingQueue = new Map();
    this.#textEncoder = new TextEncoder();
    this.#textDecoder = new TextDecoder();

    this.#handlers = {
      open: this.#handleOpen.bind(this),
      close: this.#handleClose.bind(this),
      error: this.#handleError.bind(this),
      message: this.#handleMessage.bind(this),
    };

    for (const [event, handler] of Object.entries(this.#handlers)) {
      this.#channel.addEventListener(event, handler);
    }

    // Adjust the channel parameters for correct data transfer.
    channel.binaryType = "arraybuffer";
    channel.bufferedAmountLowThreshold = BUFFERED_AMOUNT_LOW;
  }

  /**
   * Sends a message through the RTCDataChannel.
   *
   * @param message The message to send.
   * @param info Metadata associated with the message.
   * @returns A ReadableStream that resolves when the message is sent.
   */
  send(message: unknown, info?: Record<string, unknown>): ReadableStream {
    let canceled = false;
    let ctrl: ReadableStreamDefaultController | undefined;
    const progress = new ReadableStream({
      start(c) {
        ctrl = c;
      },
      cancel() {
        canceled = true;
      },
    });
    if (!ctrl) {
      throw new Error("Failed to create progress stream");
    }

    // Unreliable channels or channels with custom protocols is not supported.
    const isSupported = !this.#channel.protocol && this.#channel.ordered;
    if (!isSupported) {
      const err = new Error("Channel is not supported");
      ctrl.error(err);
      return progress;
    }

    // Length of the info must be less than chunk size because the info
    // is sent in the first packet, alongside the message data.
    const infoBuffer = this.#encodeJSON(info);
    const infoLength = infoBuffer?.byteLength || 0;
    if (infoLength >= CHUNK_SIZE) {
      const err = new Error("Metadata is too large");
      ctrl.error(err);
      return progress;
    }

    // Enqueue the message send operation to ensure it doesn't interleave
    // with other messages.
    this.#enqueueMessage(async () => {
      let dataStream;
      let failed = false;
      try {
        if (this.#channel.readyState !== "open") {
          throw new Error("Channel is not open");
        }

        this.#messageId = (this.#messageId & MAX_MESSAGE_ID) + 1;
        const { stream, type, size: total } = dataToStream(message);
        dataStream = stream;
        const typeCode = MESSAGE_TYPE[type];
        let current = 0;

        for await (const { index, chunk, done } of streamToChunks(
          stream,
          CHUNK_SIZE,
          infoLength,
        )) {
          if (canceled) {
            throw new Error("Send canceled");
          }

          const isFrstChunk = index === 0;
          const packet: Packet = {
            id: this.#messageId,
            index,
            type: isFrstChunk ? typeCode : undefined,
            info: isFrstChunk && infoBuffer ? infoBuffer : undefined,
            chunk,
            done,
          };

          await this.#sendChunk(packet);

          current += chunk.byteLength;
          if (done) current = total;
          ctrl?.enqueue({
            id: this.#peerId,
            label: this.#channel.label,
            current,
            total,
            done,
          });
        }
      } catch (err) {
        failed = true;
        ctrl?.error(err);
      } finally {
        dataStream?.cancel();
        if (!failed) ctrl?.close();
      }
    });

    return progress;
  }

  /**
   * Closes the data channel and cleans up all resources.
   */
  destroy(): void {
    if (this.#channel.readyState === "closed") return;

    this.#channel.close();

    const err = new Error("Channel is closed");
    this.#incomingQueue.forEach((controller) => controller.error(err));
    this.#incomingQueue.clear();

    this.#callback.destroy();
  }

  /**
   * Appends an async task to the outgoing send queue.
   *
   * Each task waits for the previous one to finish, which guarantees that
   * chunks belonging to different messages are never interleaved.
   */
  #enqueueMessage<T>(task: () => Promise<T>): Promise<T> {
    const prev = this.#outgoingQueue;
    const run = prev.then(() => task());
    this.#outgoingQueue = run.catch(() => {});
    return run;
  }

  /**
   * Generates a unique key for tracking incoming messages by combining the channel label and message ID.
   */
  #getMessageId(id: number): string {
    return `${this.#channel.label}:${id}`;
  }

  /**
   * Sends a chunk of data through a specific channel.
   */
  async #sendChunk(packet: Packet): Promise<void> {
    const buffer = encode(packet, PACKET_SCHEMA);
    if (!buffer) {
      throw new Error("Failed to encode message");
    }

    const maxBufferedAmount = Math.max(
      0,
      BUFFERED_AMOUNT_MAX - buffer.byteLength,
    );

    while (this.#channel.bufferedAmount > maxBufferedAmount) {
      await this.#waitForLowBuffer(maxBufferedAmount);
    }

    if (this.#channel.readyState !== "open") {
      throw new Error("Channel is not open");
    }

    this.#channel.send(new Uint8Array(buffer));
  }

  /**
   * Waits until a data channel can accept more bytes.
   */
  async #waitForLowBuffer(maxBufferedAmount = 0): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      const channel = this.#channel;

      if (channel.readyState !== "open") {
        return reject(new Error("Channel is not open"));
      }

      if (channel.bufferedAmount <= maxBufferedAmount) {
        return resolve();
      }

      let timeoutTimer: ReturnType<typeof setTimeout> | null = null;
      const cleanup = () => {
        channel.removeEventListener("bufferedamountlow", onLow);
        channel.removeEventListener("close", onClose);
        channel.removeEventListener("error", onError);
        if (timeoutTimer) {
          clearTimeout(timeoutTimer);
          timeoutTimer = null;
        }
      };
      const onLow = () => {
        cleanup();
        resolve();
      };
      const onClose = () => {
        cleanup();
        reject(new Error("Channel closed during data send"));
      };
      const onError = () => {
        cleanup();
        reject(new Error("Channel failed during data send"));
      };

      channel.addEventListener("bufferedamountlow", onLow);
      channel.addEventListener("close", onClose);
      channel.addEventListener("error", onError);

      timeoutTimer = setTimeout(() => {
        cleanup();
        reject(new Error("Timed out waiting for channel buffer to drain"));
      }, DRAIN_TIMEOUT);
    });
  }

  /**
   * Sends an abort signal to the channel for a specific message.
   */
  #sendAbort(id: number): void {
    if (this.#channel.readyState !== "open") return;

    const packet: Packet = { id, index: 0, abort: true };
    const payload = encode(packet, PACKET_SCHEMA);
    if (payload) this.#channel.send(new Uint8Array(payload));
  }

  /**
   * Creates a new MessageController for an incoming message.
   *
   * Sets up a ReadableStream and tracks chunk ordering, metadata,
   * and lifecycle (enqueue / close / error) for the message.
   */
  #createMessageController(
    id: number,
    type: number,
    infoBytes?: Uint8Array,
  ): MessageController {
    let streamCtrl: ReadableStreamDefaultController<Uint8Array> | undefined;
    const messageId = this.#getMessageId(id);

    const stream = new ReadableStream({
      start: (ctrl) => {
        streamCtrl = ctrl;
      },
      cancel: () => {
        this.#incomingQueue.delete(messageId);
        this.#sendAbort(id);
      },
    });

    if (!streamCtrl) {
      throw new Error("Failed to initialize data stream");
    }

    // Evict the oldest controller when the incoming queue exceeds its limit.
    if (this.#incomingQueue.size >= MAX_CONCURRENT_MESSAGES) {
      const oldestEntry = this.#incomingQueue.values().next();
      oldestEntry.value?.error(
        new Error("Too many concurrent incoming messages"),
      );
    }

    const controller: MessageController = {
      id,
      index: -1,
      type,
      info: this.#decodeJSON(infoBytes),
      stream,
      enqueue: (index: number, chunk: Uint8Array) => {
        if (streamCtrl) {
          if (index === controller.index + 1) {
            streamCtrl.enqueue(chunk);
            controller.index = index;
          } else {
            const err = new Error("Incorrect message order");
            this.#incomingQueue.delete(messageId);
            streamCtrl.error(err);
          }
        }
      },
      close: () => {
        this.#incomingQueue.delete(messageId);
        streamCtrl?.close();
      },
      error: (err: unknown) => {
        this.#incomingQueue.delete(messageId);
        streamCtrl?.error(err);
      },
    };

    this.#incomingQueue.set(messageId, controller);

    return controller;
  }

  /**
   * Deserializes the content of an incoming message to its final form.
   *
   * Converts the internal ReadableStream into the appropriate type
   * (string, JSON object, ArrayBuffer, Blob, or the stream itself).
   */
  async #deserializeMessage(controller: MessageController): Promise<unknown> {
    let content: unknown = controller.stream;
    if (controller.type === MESSAGE_TYPE.text) {
      content = await new Response(controller.stream).text();
    } else if (controller.type === MESSAGE_TYPE.json) {
      content = await new Response(controller.stream).json();
    } else if (controller.type === MESSAGE_TYPE.bytes) {
      content = await new Response(controller.stream).arrayBuffer();
    } else if (controller.type === MESSAGE_TYPE.blob) {
      content = await new Response(controller.stream).blob();
    }
    return content;
  }

  /**
   * Serializes an object to a JSON string and encodes it as UTF-8 bytes.
   */
  #encodeJSON(
    obj: Record<string, unknown> | undefined,
  ): Uint8Array | undefined {
    try {
      return typeof obj !== "undefined"
        ? this.#textEncoder.encode(JSON.stringify(obj))
        : undefined;
    } catch {
      return;
    }
  }

  /**
   * Decodes UTF-8 bytes into a JSON string and parses it into an object.
   */
  #decodeJSON(
    bytes: Uint8Array | undefined,
  ): Record<string, unknown> | undefined {
    try {
      return typeof bytes !== "undefined"
        ? JSON.parse(this.#textDecoder.decode(bytes))
        : undefined;
    } catch {
      return;
    }
  }

  /**
   * Handles the channel's `open` event.
   */
  #handleOpen(): void {
    this.#callback.open();
  }

  /**
   * Handles the channel's `close` event.
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
   * Processes an incoming raw message from the data channel.
   *
   * Decodes the protobuf payload, routes chunks to the correct in-progress
   * message controller, and fires `onMessage` when a message is complete.
   */
  async #handleMessage(e: Event): Promise<void> {
    try {
      const { data: payload } = e as MessageEvent;

      // For unreliable channels, channels with custom protocols, and
      // non-ArrayBuffer messages, skip protobuf decoding and deliver
      // the message as-is.
      const customTransport =
        this.#channel.protocol ||
        !this.#channel.ordered ||
        !(payload instanceof ArrayBuffer || payload instanceof Uint8Array);

      if (customTransport) {
        this.#callback.message(payload);
        return;
      }

      const packet: Packet | null = decode(
        new Uint8Array(payload),
        PACKET_SCHEMA,
      );
      if (!packet) {
        throw new Error("Failed to decode message");
      }

      const { id, index, type, abort, done, info, chunk } = packet;

      const messageId = this.#getMessageId(id);
      let controller = this.#incomingQueue.get(messageId);

      if (abort) {
        controller?.error(new Error("Message aborted by sender"));
        return;
      }

      // New message — create a controller.
      if (type && index === 0 && !controller) {
        controller = this.#createMessageController(id, type, info);

        // Stream-type messages are delivered immediately as a ReadableStream.
        if (type === MESSAGE_TYPE.stream) {
          this.#callback.message(controller.stream, controller.info);
        }
      }

      // Ignore any inconsistent messages.
      if (!controller) return;

      // Enqueue chunk data if present.
      if (chunk) {
        controller.enqueue(index, chunk);
      }

      // Finalize the message when the done flag is set.
      if (done) {
        controller.close();

        // Non-stream messages are serialized and delivered at the end of the message.
        if (controller.type !== MESSAGE_TYPE.stream) {
          const data = await this.#deserializeMessage(controller);
          this.#callback.message(data, controller.info);
        }
      }
    } catch (err) {
      this.#callback.error(err);
    }
  }
}

/**
 * Internal controller that tracks a single in-progress incoming message.
 *
 * Chunks are enqueued as they arrive, and the stream is closed or errored
 * when the message finishes or errors.
 */
interface MessageController {
  /** Numeric message identifier. */
  id: number;
  /** Current chunk index. Initialized to -1; increments with each enqueued chunk. */
  index: number;
  /** Numeric type identifier (TYPE_STREAM, TYPE_TEXT, etc.). */
  type: number;
  /** ReadableStream that delivers chunks to the consumer. */
  stream: ReadableStream<Uint8Array>;
  /** Optional metadata attached by the sender. */
  info?: Record<string, unknown>;
  /** Push a new chunk into the stream. */
  enqueue: (index: number, chunk: Uint8Array) => void;
  /** Signal that the message is complete and close the stream. */
  close: () => void;
  /** Signal an error and terminate the stream. */
  error: (error: unknown) => void;
}

/** Callback interface for handling propagating events. */
interface DataChannelCallback {
  open: () => void;
  close: () => void;
  error: (error: unknown) => void;
  message: (data: unknown, info?: Record<string, unknown>) => void;
  destroy: () => void;
}

/** Protobuf-encoded packet structure for data channel communication. */
interface Packet {
  id: number;
  index: number;
  type?: number;
  abort?: boolean;
  done?: boolean;
  info?: Uint8Array;
  chunk?: Uint8Array;
}
