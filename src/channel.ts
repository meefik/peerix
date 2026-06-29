import type { TransferProgress } from "./peer.js";
import { encode, decode } from "./utils/protobuf.js";
import {
  dataToStream,
  streamToChunks,
  PromiseLikeReadableStream,
  type DataType,
} from "./utils/stream.js";

/** Size of the protobuf header in bytes. */
const HEADER_SIZE = 64;
/** Maximum payload per chunk (16 KB MTU minus header). */
const CHUNK_SIZE = 16 * 1024 - HEADER_SIZE;
/** Threshold that triggers the `bufferedamountlow` event. */
const BUFFERED_AMOUNT_LOW = 32 * 1024;
/** Upper bound on buffered bytes before back-pressure kicks in. */
const BUFFERED_AMOUNT_MAX = 256 * 1024;
/** How long to wait for the channel buffer to drain or the remote answers before giving up. */
const ASK_TIMEOUT = 10 * 1000;
/** Maximum 16-bit unsigned integer used for transfer IDs (wraps around). */
const MAX_TRANSFER_ID = 0xffff;

/** Map from human-readable type names to their numeric identifiers. */
const MESSAGE_TYPE = {
  text: 1, // UTF-8 string
  json: 2, // JSON-serializable object
  blob: 3, // Blob or File
  bytes: 4, // ArrayBuffer or Uint8Array
} as const;

/** Protobuf schema for data packets. */
const PACKET_SCHEMA: Record<
  string,
  { id: number; type: "uint32" | "bytes" | "bool" }
> = {
  id: { id: 1, type: "uint32" },
  ask: { id: 2, type: "bool" },
  index: { id: 3, type: "uint32" },
  type: { id: 4, type: "uint32" },
  abort: { id: 5, type: "bool" },
  done: { id: 6, type: "bool" },
  info: { id: 7, type: "bytes" },
  chunk: { id: 8, type: "bytes" },
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
  #transferId: number;
  #channel: RTCDataChannel;
  #callback: DataChannelCallback;
  #handlers: Record<string, EventListener>;
  #outgoingTransfers: Map<number, OutgoingTransfer>;
  #incomingTransfers: Map<number, IncomingTransfer>;
  #taskQueue: Promise<unknown>;
  #textEncoder: TextEncoder;
  #textDecoder: TextDecoder;
  #lowBufferResolvers: Set<() => void>;

  /**
   * Creates a new DataChannel instance.
   *
   * @param options.peerId The unique identifier of the remote peer.
   * @param options.channel The WebRTC data channel to transfer data over.
   * @param options.callback Callback functions to handle propagated events.
   */
  constructor({
    peerId,
    channel,
    callback,
  }: {
    peerId: string;
    channel: RTCDataChannel;
    callback: DataChannelCallback;
  }) {
    this.#peerId = peerId;
    this.#channel = channel;
    this.#callback = callback;

    this.#transferId = 0;
    this.#taskQueue = Promise.resolve();
    this.#outgoingTransfers = new Map();
    this.#incomingTransfers = new Map();
    this.#textEncoder = new TextEncoder();
    this.#textDecoder = new TextDecoder();
    this.#lowBufferResolvers = new Set();

    this.#handlers = {
      open: this.#handleOpen.bind(this),
      close: this.#handleClose.bind(this),
      error: this.#handleError.bind(this),
      bufferedamountlow: this.#handleBufferedAmountLow.bind(this),
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
   * @param options Send options including metadata and abort signal.
   * @returns A ReadableStream that yields transfer progress updates.
   */
  send(
    message: unknown,
    options?: { info?: Record<string, unknown>; signal?: AbortSignal },
  ): PromiseLikeReadableStream<TransferProgress> {
    const { info, signal } = options ?? {};

    let aborted = false;
    const onAbort = () => {
      aborted = true;
    };
    const { progress, controller: progressCtrl } =
      this.#createProgress(onAbort);

    const failTransfer = (
      error: Error,
    ): PromiseLikeReadableStream<TransferProgress> => {
      progressCtrl.error(error);
      if (message instanceof ReadableStream) message.cancel();
      return progress;
    };

    // Fail immediately if the signal is already aborted.
    // Use `signal.aborted` instead of the `abort` event.
    if (aborted || signal?.aborted) {
      return failTransfer(
        signal?.reason ?? new DOMException("Transfer aborted", "AbortError"),
      );
    }

    // Unreliable channels or channels with custom protocols are not supported.
    if (this.#channel.protocol || !this.#channel.ordered) {
      return failTransfer(new Error("Channel is not supported"));
    }

    // Length of the info must be less than chunk size because the info
    // is sent in the first packet, alongside the message data.
    const infoBuffer = this.#encodeJSON(info);
    const infoLength = infoBuffer?.byteLength ?? 0;
    if (infoLength >= CHUNK_SIZE) {
      return failTransfer(new Error("Metadata is too large"));
    }

    // Enqueue the message send operation to ensure it doesn't interleave
    // with other messages.
    void this.#enqueueTask(async () => {
      let dataStream, transfer;
      let failed = false;

      try {
        if (this.#channel.readyState !== "open") {
          throw new Error("Channel is not open");
        }

        if (aborted || signal?.aborted) {
          throw (
            signal?.reason ?? new DOMException("Transfer aborted", "AbortError")
          );
        }

        const { stream, type, size: total } = dataToStream(message);
        dataStream = stream;
        this.#transferId = (this.#transferId % MAX_TRANSFER_ID) + 1;
        const id = this.#transferId;
        const label = this.#channel.label;
        const peerId = this.#peerId;
        const typeCode = MESSAGE_TYPE[type];
        let current = 0;

        transfer = this.#createOutgoingTransfer(id);

        for await (const { index, chunk, done } of streamToChunks(
          stream,
          CHUNK_SIZE,
          infoLength,
        )) {
          if (aborted || signal?.aborted) {
            throw (
              signal?.reason ??
              new DOMException("Transfer aborted", "AbortError")
            );
          }

          // Send the data chunk.
          const isFirstChunk = index === 0;
          const packet: Packet = {
            id,
            index,
            type: isFirstChunk ? typeCode : undefined,
            info: isFirstChunk && infoBuffer ? infoBuffer : undefined,
            chunk,
            done,
          };
          await this.#send(packet);

          // Update transfer progress.
          current += chunk.byteLength;
          progressCtrl.enqueue({ id: peerId, label, current, total, done });

          // Wait for delivery confirmation before finishing.
          if (done) {
            await transfer.waitForDone();
          }
        }
      } catch (err) {
        failed = true;
        progressCtrl.error(err);
        if (transfer) {
          transfer.abort(err);
          void this.#send({ id: transfer.id, ask: true, abort: true }).catch(
            () => {},
          );
        }
      } finally {
        if (failed) dataStream?.cancel();
        else progressCtrl.close();
      }
    });

    return progress;
  }

  /**
   * Closes the data channel and cleans up all resources.
   */
  destroy(): void {
    try {
      if (this.#channel.readyState !== "closed") {
        this.#channel.close();
      }
      this.#lowBufferResolvers.clear();

      const err = new Error("Channel is closed");
      this.#outgoingTransfers.forEach((transfer) => transfer.abort(err));
      this.#incomingTransfers.forEach((transfer) => transfer.abort(err));
    } finally {
      this.#callback.destroy();
    }
  }

  /**
   * Appends an async task to the outgoing send queue.
   *
   * Each task waits for the previous one to finish, which guarantees that
   * chunks belonging to different messages are never interleaved.
   */
  #enqueueTask<T>(task: () => Promise<T>): Promise<T> {
    const prev = this.#taskQueue;
    const run = prev.then(() => task());
    // Swallow errors because tasks handle them independently.
    this.#taskQueue = run.catch((err) => {});
    return run;
  }

  /**
   * Creates a progress ReadableStream for reporting transfer status.
   */
  #createProgress(onCancel: () => void): {
    progress: PromiseLikeReadableStream<TransferProgress>;
    controller: ReadableStreamDefaultController;
  } {
    let controller!: ReadableStreamDefaultController;
    const progress = new PromiseLikeReadableStream<TransferProgress>(
      {
        start(ctrl) {
          controller = ctrl;
        },
        cancel() {
          onCancel();
        },
      },
      { highWaterMark: 0, size: () => 1 },
    );
    return {
      progress,
      controller,
    };
  }

  /**
   * Creates a new transfer controller for an outgoing message.
   */
  #createOutgoingTransfer(id: number): OutgoingTransfer {
    let ended = false;
    const listeners = new Set<[() => void, (err: unknown) => void]>();
    const transfer: OutgoingTransfer = {
      id,
      abort: (err?: unknown) => {
        if (ended) return;
        ended = true;
        this.#outgoingTransfers.delete(id);
        const error = err ?? new DOMException("Transfer aborted", "AbortError");
        for (const [, reject] of listeners) reject(error);
      },
      done: () => {
        if (ended) return;
        ended = true;
        this.#outgoingTransfers.delete(id);
        for (const [resolve] of listeners) resolve();
      },
      waitForDone: () => {
        return new Promise<void>((res, rej) => {
          const timer = setTimeout(() => {
            ended = true;
            this.#outgoingTransfers.delete(id);
            rej(new Error("Transfer timed out"));
          }, ASK_TIMEOUT);
          const resolve = () => {
            clearTimeout(timer);
            res();
          };
          const reject = (err: unknown) => {
            clearTimeout(timer);
            rej(err);
          };
          listeners.add([resolve, reject]);
        });
      },
    };

    this.#outgoingTransfers.set(id, transfer);

    return transfer;
  }

  /**
   * Creates a new transfer controller for an incoming message.
   */
  #createIncomingTransfer(
    id: number,
    type: DataType,
    onCancel: (err: unknown) => void,
  ): IncomingTransfer {
    let ended = false;
    let transfer: IncomingTransfer | undefined;
    let streamCtrl!: ReadableStreamDefaultController;

    const data = new PromiseLikeReadableStream<Uint8Array>(
      {
        start: (ctrl) => {
          streamCtrl = ctrl;
        },
        cancel: () => {
          if (ended) return;
          ended = true;
          const err = new Error("Transfer cancelled");
          transfer?.abort(err);
          onCancel(err);
        },
      },
      {},
      type,
    );

    transfer = {
      id,
      index: 0,
      data,
      enqueue: (chunk: Uint8Array) => {
        if (ended) return;
        streamCtrl.enqueue(chunk);
        transfer!.index++;
      },
      close: () => {
        if (ended) return;
        ended = true;
        streamCtrl.close();
        this.#incomingTransfers.delete(id);
      },
      abort: (error: unknown) => {
        if (ended) return;
        ended = true;
        streamCtrl.error(error);
        this.#incomingTransfers.delete(id);
      },
    };

    this.#incomingTransfers.set(id, transfer);

    return transfer;
  }

  /**
   * Sends a packet over the data channel.
   */
  async #send(packet: Packet): Promise<void> {
    if (this.#channel.readyState !== "open") {
      throw new Error("Channel is not open");
    }

    const buffer = encode(packet, PACKET_SCHEMA);
    if (!buffer) {
      throw new Error("Failed to encode message");
    }

    const maxBufferedAmount = Math.max(
      0,
      BUFFERED_AMOUNT_MAX - buffer.byteLength,
    );

    await this.#waitForLowBuffer(maxBufferedAmount);

    this.#channel.send(buffer);
  }

  /**
   * Serializes an object to a JSON string and encodes it as UTF-8 bytes.
   */
  #encodeJSON(
    obj: Record<string, unknown> | undefined,
  ): Uint8Array | undefined {
    try {
      return obj !== undefined
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
      return bytes !== undefined
        ? JSON.parse(this.#textDecoder.decode(bytes))
        : undefined;
    } catch {
      return;
    }
  }

  /**
   * Decodes a numeric type code into its corresponding DataType.
   */
  #decodeType(code: number): DataType {
    let type: DataType = "bytes";
    if (code === MESSAGE_TYPE.text) type = "text";
    else if (code === MESSAGE_TYPE.json) type = "json";
    else if (code === MESSAGE_TYPE.blob) type = "blob";
    return type;
  }

  /**
   * Waits until the channel's buffered amount drops below the given threshold.
   */
  async #waitForLowBuffer(maxBufferedAmount = 0): Promise<void> {
    while (this.#channel.bufferedAmount > maxBufferedAmount) {
      await new Promise<void>((resolve, reject) => {
        const handler = () => {
          clearTimeout(timer);
          this.#lowBufferResolvers.delete(handler);
          resolve();
        };
        this.#lowBufferResolvers.add(handler);
        const timer = setTimeout(() => {
          this.#lowBufferResolvers.delete(handler);
          reject(new Error("Transfer timed out"));
        }, ASK_TIMEOUT);
      });
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
   * Handles the `bufferedamountlow` event by notifying all waiters that the buffer is available for writing.
   */
  #handleBufferedAmountLow(): void {
    for (const resolve of [...this.#lowBufferResolvers]) {
      resolve();
      this.#lowBufferResolvers.delete(resolve);
    }
  }

  /**
   * Processes an incoming raw message from the data channel.
   *
   * Decodes the protobuf payload, routes chunks to the correct in-progress
   * message controller, and fires `onMessage` when a message is complete.
   */
  async #handleMessage(e: Event): Promise<void> {
    try {
      const { data } = e as MessageEvent;

      // For unreliable channels, channels with custom protocols, and
      // non-ArrayBuffer messages, skip protobuf decoding and deliver
      // the message as-is.
      const isSupported =
        !this.#channel.protocol &&
        this.#channel.ordered &&
        data instanceof ArrayBuffer;
      if (!isSupported) {
        this.#callback.message(data);
        return;
      }

      const packet: Packet | null = decode(
        new Uint8Array(data as ArrayBuffer),
        PACKET_SCHEMA,
      );
      if (!packet) {
        throw new Error("Failed to decode message");
      }

      if (packet.ask) {
        this.#handleAskPacket(packet);
      } else {
        this.#handleDataPacket(packet);
      }
    } catch (err) {
      this.#callback.error(err);
    }
  }

  /**
   * Handles an incoming ask packet that acknowledges or aborts an outgoing transfer.
   */
  #handleAskPacket(packet: Packet): void {
    const { id, abort, done } = packet;
    const transfer = this.#outgoingTransfers.get(id);

    if (abort) {
      transfer?.abort(new Error("Aborted by receiver"));
      return;
    }

    if (done && transfer) {
      transfer.done();
    }
  }

  /**
   * Handles an incoming data packet that creates or advances an incoming transfer.
   */
  #handleDataPacket(packet: Packet): void {
    const { id, index, type, abort, done, info, chunk } = packet;
    let transfer = this.#incomingTransfers.get(id);

    if (abort) {
      transfer?.abort(new Error("Aborted by sender"));
      return;
    }

    // New message: create the incoming transfer and fire the callback.
    if (type && index === 0) {
      transfer?.close();
      const decodedType = this.#decodeType(type);
      transfer = this.#createIncomingTransfer(id, decodedType, () => {
        void this.#send({ id, ask: true, abort: true }).catch(() => {});
      });
      const decodedInfo = this.#decodeJSON(info);
      this.#callback.message(transfer.data, decodedInfo);
    }

    // Enqueue data chunk if the order is correct.
    if (chunk && transfer) {
      if (index !== transfer.index) {
        transfer.abort(new Error("Incorrect message order"));
        return;
      } else {
        transfer.enqueue(chunk);
      }
    }

    // Signal completion and acknowledge receipt.
    if (done && transfer) {
      transfer.close();
      void this.#send({ id, ask: true, done: true }).catch(() => {});
    }
  }
}

/** Internal controller that tracks a single in-progress outgoing message. */
interface OutgoingTransfer {
  id: number;
  abort: (error?: unknown) => void;
  done: () => void;
  waitForDone: () => Promise<void>;
}

/** Internal controller that tracks a single in-progress incoming message. */
interface IncomingTransfer {
  id: number;
  index: number;
  data: PromiseLikeReadableStream<Uint8Array>;
  enqueue: (chunk: Uint8Array) => void;
  close: () => void;
  abort: (error: unknown) => void;
}

/** Callback interface for handling propagated events. */
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
  ask?: boolean;
  index?: number;
  type?: number;
  abort?: boolean;
  done?: boolean;
  info?: Uint8Array;
  chunk?: Uint8Array;
}
