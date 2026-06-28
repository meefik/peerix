/** Represents the detected type of data payload. */
export type DataType = "text" | "json" | "blob" | "bytes";

/** Creates a single-chunk ReadableStream from a Uint8Array payload. */
function createOneChunkStream(payload: Uint8Array): ReadableStream {
  return new ReadableStream({
    start(controller) {
      controller.enqueue(payload);
      controller.close();
    },
  });
}

/**
 * Converts various data types into a ReadableStream.
 *
 * @param data The input data to be converted.
 * @returns A ReadableStream representing the input data, its type and size.
 */
export function dataToStream(data: unknown): {
  stream: ReadableStream;
  type: DataType;
  size?: number;
} {
  let stream: ReadableStream;
  let type: DataType;
  let size: number | undefined;

  if (data instanceof ReadableStream) {
    stream = data;
    type = "bytes";
  } else if (data instanceof Blob) {
    stream = data.stream();
    type = "blob";
    size = data.size;
  } else if (data instanceof ArrayBuffer || ArrayBuffer.isView(data)) {
    const payload =
      data instanceof ArrayBuffer
        ? new Uint8Array(data)
        : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    stream = createOneChunkStream(payload);
    type = "bytes";
    size = data.byteLength;
  } else if (typeof data === "string") {
    const payload = new TextEncoder().encode(data);
    stream = createOneChunkStream(payload);
    type = "text";
    size = payload.byteLength;
  } else {
    const jsonString = JSON.stringify(data ?? null);
    const payload = new TextEncoder().encode(jsonString);
    stream = createOneChunkStream(payload);
    type = "json";
    size = payload.byteLength;
  }

  return { stream, type, size };
}

/**
 * Splits a ReadableStream of Uint8Arrays into chunks of a specified size,
 * yielding each chunk as an object containing the chunk and a done flag
 * indicating if it's the last chunk.
 *
 * @param stream The input ReadableStream of Uint8Arrays.
 * @param chunkSize The size of each chunk to be yielded.
 * @param skipBytes Number of bytes to subtract from the first chunk size.
 * @returns An AsyncGenerator yielding objects with index, chunk, and done properties.
 * @throws Error if the resulting chunk size would be zero or negative.
 */
export async function* streamToChunks(
  stream: ReadableStream<Uint8Array>,
  chunkSize: number,
  skipBytes = 0,
): AsyncGenerator<
  { index: number; chunk: Uint8Array; done: boolean },
  void,
  unknown
> {
  if (chunkSize <= 0 || (skipBytes && chunkSize - skipBytes <= 0)) {
    throw new Error("Invalid chunk size");
  }

  const reader = stream.getReader();
  const buffer = new Uint8Array(chunkSize);
  let bufferLen = 0;
  let index = 0;
  let firstChunk = true;
  let pending: Uint8Array | null = null;

  try {
    while (true) {
      const { done, value } = await reader.read();

      if (done) {
        if (pending) {
          yield { index: index++, chunk: pending, done: bufferLen === 0 };
        }
        if (bufferLen > 0) {
          yield {
            index: index++,
            chunk: buffer.slice(0, bufferLen),
            done: true,
          };
        }
        break;
      }

      if (!(value instanceof Uint8Array)) {
        throw new Error("ReadableStream must yield Uint8Array chunks");
      }

      let i = 0;
      while (i < value.length) {
        const target =
          firstChunk && skipBytes
            ? Math.max(chunkSize - skipBytes, 1)
            : chunkSize;
        const space = target - bufferLen;
        if (space <= 0) break;

        const take = Math.min(space, value.length - i);
        buffer.set(value.subarray(i, i + take), bufferLen);
        bufferLen += take;
        i += take;

        if (bufferLen === target) {
          if (pending) {
            yield { index: index++, chunk: pending, done: false };
          }
          pending = buffer.slice(0, target);
          bufferLen = 0;
          firstChunk = false;
        }
      }
    }
  } finally {
    reader.releaseLock();
  }
}

/**
 * Duplicates a stream into N branches.
 *
 * @param source Stream to be duplicated.
 * @param count Number of branches to create.
 * @returns An array of ReadableStream branches.
 */
export function teeStream(
  source: ReadableStream,
  count: number,
): ReadableStream[] {
  if (count <= 0) return [];
  if (count === 1) return [source];

  const branches: ReadableStream[] = [source];
  while (branches.length < count) {
    const stream = branches.shift();
    if (!stream) break;

    const [left, right] = stream.tee();
    branches.push(left, right);
  }

  return branches.slice(0, count);
}

/**
 * Merges multiple ReadableStreams into a single stream.
 *
 * Values from all source streams are forwarded to the merged stream as they
 * become available. The merged stream closes only when every source stream
 * has finished, and errors if any source stream errors.
 *
 * @param sources Streams to merge.
 * @returns A single ReadableStream yielding values from all sources.
 */
export function mergeStreams<R = unknown>(
  sources: ReadableStream<R>[],
): ReadableStream<R> & Promise<void> {
  if (sources.length === 0) {
    return new PromiseLikeReadableStream<R>({
      start(controller) {
        controller.close();
      },
    });
  }

  let doneCount = 0;
  const total = sources.length;
  let errored = false;

  const underlyingSource: UnderlyingSource<R> = {
    start(controller) {
      const defaultController =
        controller as ReadableStreamDefaultController<R>;
      const readers: ReadableStreamDefaultReader<R>[] = sources.map((source) =>
        source.getReader(),
      );
      const readerDone = new Uint8Array(total);

      const pump = async () => {
        try {
          while (doneCount < total) {
            for (let i = 0; i < readers.length; i++) {
              if (readerDone[i]) continue;

              try {
                const { value, done } = await readers[i].read();
                if (done) {
                  readerDone[i] = 1;
                  doneCount++;
                  if (doneCount === total) {
                    defaultController.close();
                  }
                  continue;
                }
                defaultController.enqueue(value);
              } catch (err) {
                errored = true;
                defaultController.error(err);
                return;
              }
            }

            // Yield to microtask queue once per round to prevent fast producers
            // from starving others.
            await Promise.resolve();
          }
        } catch (err) {
          if (!errored) {
            errored = true;
            defaultController.error(err);
          }
        } finally {
          for (const reader of readers) {
            try {
              reader.releaseLock();
            } catch {}
          }
        }
      };

      pump();
    },
  };

  return new PromiseLikeReadableStream<R>(underlyingSource);
}

/**
 * ReadableStream that resolves to typed content as a promise.
 *
 * Note: The underlying stream can only be consumed once. Reading via
 * `.getReader()` / iteration and awaiting the Promise are mutually exclusive
 * — whichever runs first will consume the data, leaving nothing for the other.
 */
export class PromiseLikeReadableStream<R = unknown> extends ReadableStream<R> {
  /**
   * Creates a typed stream that resolves via {@link then}.
   *
   * @param underlyingSource Source producing the stream's data chunks.
   * @param queuingStrategy Strategy for managing backpressure.
   * @param type How to resolve the content as a promise.
   */
  constructor(
    underlyingSource?: UnderlyingSource<R>,
    queuingStrategy?: QueuingStrategy<R>,
    type?: DataType,
  ) {
    super(underlyingSource, queuingStrategy);

    let promise: Promise<unknown | void>;
    const getPromise = (): Promise<unknown | void> => {
      if (promise) return promise;
      if (type === undefined) {
        promise = new Promise<void>(async (resolve, reject) => {
          try {
            for await (const _ of this) {
              // Consume the stream until it is fully read.
            }
            resolve();
          } catch (err) {
            reject(err);
          }
        });
      } else {
        promise = new Promise<unknown>(async (resolve, reject) => {
          const response = new Response(this);
          const converters: Record<DataType, () => Promise<unknown>> = {
            text: () => response.text(),
            json: () => response.json(),
            blob: () => response.blob(),
            bytes: () => response.arrayBuffer(),
          };
          if (converters[type]) {
            converters[type]().then(resolve).catch(reject);
          } else {
            reject(new Error(`Unsupported type: ${type}`));
          }
        });
      }
      return promise;
    };

    this.then = (...args) => getPromise().then(...args);
    this.catch = (...args) => getPromise().catch(...args);
    this.finally = (...args) => getPromise().finally(...args);
  }

  then: Promise<any>["then"];
  catch: Promise<any>["catch"];
  finally: Promise<any>["finally"];

  get [Symbol.toStringTag](): string {
    return this.constructor.name;
  }
}
