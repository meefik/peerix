/** Represents the detected type of data payload. */
export type DataType = "stream" | "blob" | "bytes" | "text" | "json";

/**
 * Converts various data types into a ReadableStream.
 *
 * @param data The input data to be converted.
 * @returns A ReadableStream representing the input data, its type and size.
 */
export function dataToStream(data: unknown): {
  stream: ReadableStream;
  type: DataType;
  size: number;
} {
  let stream: ReadableStream;
  let type: DataType;
  let size: number = -1;

  if (data instanceof ReadableStream) {
    stream = data;
    type = "stream";
  } else if (data instanceof Blob) {
    stream = data.stream();
    type = "blob";
    size = data.size;
  } else if (data instanceof ArrayBuffer || ArrayBuffer.isView(data)) {
    const payload =
      data instanceof ArrayBuffer
        ? new Uint8Array(data)
        : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    stream = new ReadableStream({
      start(controller) {
        controller.enqueue(payload);
        controller.close();
      },
    });
    type = "bytes";
    size = data.byteLength;
  } else if (typeof data === "string") {
    const payload = new TextEncoder().encode(data);
    stream = new ReadableStream({
      start(controller) {
        controller.enqueue(payload);
        controller.close();
      },
    });
    type = "text";
    size = payload.byteLength;
  } else {
    const jsonString = JSON.stringify(data ?? null);
    const payload = new TextEncoder().encode(jsonString);
    stream = new ReadableStream({
      start(controller) {
        controller.enqueue(payload);
        controller.close();
      },
    });
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
 */
export async function* streamToChunks(
  stream: ReadableStream<Uint8Array>,
  chunkSize: number,
  skipBytes?: number,
): AsyncGenerator<
  { index: number; chunk: Uint8Array; done: boolean },
  void,
  unknown
> {
  const reader = stream.getReader();

  let target = skipBytes ? Math.max(chunkSize - skipBytes, 0) : chunkSize;
  if (target === 0) {
    throw new Error("Invalid chunk size");
  }

  const buffer = new Uint8Array(chunkSize);
  let bufferLen = 0;

  let previousChunk: Uint8Array | null = null;
  let index = 0;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      let i = 0;
      while (i < value.length) {
        const space = target - bufferLen;
        const take = Math.min(space, value.length - i);

        buffer.set(value.subarray(i, i + take), bufferLen);
        bufferLen += take;
        i += take;

        if (bufferLen === target) {
          if (previousChunk) {
            yield { index: index++, chunk: previousChunk, done: false };
          }
          previousChunk = buffer.slice(0, target);
          bufferLen = 0;
          target = chunkSize;
        }
      }
    }

    if (bufferLen > 0) {
      if (previousChunk) {
        yield { index: index++, chunk: previousChunk, done: false };
      }
      yield { index: index++, chunk: buffer.slice(0, bufferLen), done: true };
    } else if (previousChunk) {
      yield { index: index++, chunk: previousChunk, done: true };
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
export function mergeStreams<T>(
  sources: ReadableStream<T>[],
): ReadableStream<T> {
  if (sources.length === 0) {
    return new ReadableStream({
      start(controller) {
        controller.close();
      },
    });
  }

  if (sources.length === 1) {
    return sources[0];
  }

  let doneCount = 0;
  const total = sources.length;
  let errored = false;

  return new ReadableStream({
    start(controller) {
      const readers: ReadableStreamDefaultReader<T>[] = sources.map((source) =>
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
                    controller.close();
                  }
                  continue;
                }
                controller.enqueue(value);
              } catch (err) {
                errored = true;
                controller.error(err);
                return;
              }

              await Promise.resolve();
            }
          }
        } catch (err) {
          if (!errored) {
            errored = true;
            controller.error(err);
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
  });
}
