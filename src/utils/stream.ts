/** Represents the detected type of data payload. */
export type DataType = "stream" | "blob" | "binary" | "text" | "json";

/**
 * Converts various data types into a ReadableStream.
 *
 * @param data The input data to be converted.
 * @returns A ReadableStream representing the input data and its type.
 */
export function dataToStream(data: unknown): {
  stream: ReadableStream;
  type: DataType;
} {
  let stream: ReadableStream;
  let type: DataType;

  if (data instanceof ReadableStream) {
    stream = data;
    type = "stream";
  } else if (data instanceof Blob) {
    stream = data.stream();
    type = "blob";
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
    type = "binary";
  } else if (typeof data === "string") {
    const payload = new TextEncoder().encode(data);
    stream = new ReadableStream({
      start(controller) {
        controller.enqueue(payload);
        controller.close();
      },
    });
    type = "text";
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
  }

  return { stream, type };
}

/**
 * Splits a ReadableStream of Uint8Arrays into chunks of a specified size,
 * yielding each chunk as an object containing the chunk and a done flag
 * indicating if it's the last chunk.
 *
 * @param stream The input ReadableStream of Uint8Arrays.
 * @param chunkSize The size of each chunk to be yielded.
 * @param skipBytes Number of bytes to subtract from the first chunk size.
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
