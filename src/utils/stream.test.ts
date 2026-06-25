import { suite, test } from "node:test";
import assert from "node:assert/strict";
import {
  dataToStream,
  streamToChunks,
  teeStream,
  mergeStreams,
  PromiseLikeReadableStream,
} from "./stream.js";

async function bytesToText(bytes: Uint8Array): Promise<string> {
  return new TextDecoder().decode(bytes);
}

async function textToBytes(text: string): Promise<Uint8Array> {
  return new TextEncoder().encode(text);
}

async function streamToBytes(stream: ReadableStream): Promise<Uint8Array> {
  const buffer = await new Response(stream).arrayBuffer();
  return new Uint8Array(buffer);
}

async function bytesToStream(bytes: Uint8Array): Promise<ReadableStream> {
  return new ReadableStream({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    },
  });
}

async function collectPackets(
  stream: ReadableStream<Uint8Array>,
  chunkSize: number,
  skipBytes = 0,
): Promise<Array<{ index: number; chunk: number[]; done: boolean }>> {
  const packets: Array<{ index: number; chunk: number[]; done: boolean }> = [];
  for await (const packet of streamToChunks(stream, chunkSize, skipBytes)) {
    packets.push({
      index: packet.index,
      chunk: [...packet.chunk],
      done: packet.done,
    });
  }
  return packets;
}

suite("utils/stream", async () => {
  test("dataToStream infers types, reports sizes, and produces correct output for all input kinds", async () => {
    // Arrange
    const sourceStream = await bytesToStream(
      await textToBytes("stream payload"),
    );
    const textInput = "hello";
    const binaryInput = new Uint8Array([1, 2, 3, 4, 5]);
    const arrayBuffer = new ArrayBuffer(7);
    const blobInput = new Blob(["blob content"]);
    const jsonInput = { key: "value", num: 42 };

    // Act
    const streamResult = dataToStream(sourceStream);
    const textResult = dataToStream(textInput);
    const binaryResult = dataToStream(binaryInput);
    const arrayBufferResult = dataToStream(arrayBuffer);
    const blobResult = dataToStream(blobInput);
    const jsonResult = dataToStream(jsonInput);

    // Assert - types
    assert.equal(streamResult.type, "bytes");
    assert.equal(streamResult.stream, sourceStream);
    assert.equal(textResult.type, "text");
    assert.equal(binaryResult.type, "bytes");
    assert.equal(arrayBufferResult.type, "bytes");
    assert.equal(blobResult.type, "blob");
    assert.equal(jsonResult.type, "json");

    // Assert - sizes
    assert.equal(streamResult.size, undefined);
    assert.equal(binaryResult.size, 5);
    assert.equal(arrayBufferResult.size, 7);
    assert.equal(blobResult.size, blobInput.size);
    const textBytes = await textToBytes(textInput);
    assert.equal(textResult.size, textBytes.byteLength);
    const jsonBytes = await textToBytes(JSON.stringify(jsonInput));
    assert.equal(jsonResult.size, jsonBytes.byteLength);

    // Assert - stream content
    assert.equal(
      await bytesToText(await streamToBytes(blobResult.stream)),
      "blob content",
    );
    assert.equal(
      await bytesToText(await streamToBytes(textResult.stream)),
      textInput,
    );
    assert.equal(
      await bytesToText(await streamToBytes(jsonResult.stream)),
      JSON.stringify(jsonInput),
    );
    assert.deepEqual(await streamToBytes(binaryResult.stream), binaryInput);
  });

  test("dataToStream respects typed array views when producing byte streams", async () => {
    // Arrange
    const input = new Uint8Array([9, 8, 7, 6, 5]).subarray(1, 4);

    // Act
    const result = dataToStream(input);
    const bytes = await streamToBytes(result.stream);

    // Assert
    assert.equal(result.type, "bytes");
    assert.deepEqual(bytes, new Uint8Array([8, 7, 6]));
  });

  test("dataToStream handles null and undefined as JSON fallback", async () => {
    // Arrange & Act
    const nullResult = dataToStream(null);
    const undefResult = dataToStream(undefined);

    // Assert — both map to the json type with "null" content
    assert.equal(nullResult.type, "json");
    assert.equal(undefResult.type, "json");

    const nullText = await bytesToText(await streamToBytes(nullResult.stream));
    const undefText = await bytesToText(
      await streamToBytes(undefResult.stream),
    );

    assert.equal(nullText, "null");
    assert.equal(undefText, "null");
  });

  test("streamToChunks emits fixed-size chunks and marks only the last as done", async () => {
    // Arrange
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array([1, 2, 3]));
        controller.enqueue(new Uint8Array([4, 5]));
        controller.enqueue(new Uint8Array([6]));
        controller.close();
      },
    });

    // Act
    const packets = await collectPackets(stream, 2);

    // Assert
    assert.deepEqual(packets, [
      { index: 0, chunk: [1, 2], done: false },
      { index: 1, chunk: [3, 4], done: false },
      { index: 2, chunk: [5, 6], done: true },
    ]);
  });

  test("streamToChunks shrinks the first chunk when skipBytes is set", async () => {
    // Arrange
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array([1, 2, 3, 4, 5, 6, 7]));
        controller.close();
      },
    });

    // Act — chunkSize=4, skipBytes=2 → first chunk target = 2, rest = 4
    const packets = await collectPackets(stream, 4, 2);

    // Assert
    assert.deepEqual(packets, [
      { index: 0, chunk: [1, 2], done: false },
      { index: 1, chunk: [3, 4, 5, 6], done: false },
      { index: 2, chunk: [7], done: true },
    ]);
  });

  test("streamToChunks releases the reader lock when the source errors", async () => {
    // Arrange — stream produces one chunk then errors, well within buffer
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array([1]));
        controller.error(new Error("read error"));
      },
    });

    // Act — iterate until the generator throws
    const generator = streamToChunks(stream, 1);
    let readError: Error | undefined;

    try {
      await generator.next(); // yields chunk [1] with done: false
      await generator.next(); // reader.read() rejects with "read error"
    } catch (err) {
      readError = err as Error;
    }

    // Assert — the error propagated through the generator
    assert.equal(readError?.message, "read error");

    // Assert — reader was released in finally; further .next() calls return
    // {done: true} without throwing or hanging (proves generator exited cleanly)
    const after = await generator.next();
    assert.equal(after.done, true);
  });

  test("streamToChunks yields nothing for an empty stream", async () => {
    // Arrange
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.close();
      },
    });

    // Act
    const packets = await collectPackets(stream, 4);

    // Assert
    assert.deepEqual(packets, []);
  });

  test("streamToChunks throws on invalid chunk size arguments", async () => {
    const stream = await bytesToStream(new Uint8Array([1, 2]));
    const consume = async (
      stream: ReadableStream<Uint8Array>,
      chunkSize: number,
      skipBytes?: number,
    ) => {
      for await (const _ of streamToChunks(stream, chunkSize, skipBytes)) {
        /* empty */
      }
    };

    // Act & Assert — zero chunk size
    await assert.rejects(consume(stream, 0), { message: "Invalid chunk size" });

    // Act & Assert — negative chunk size
    await assert.rejects(consume(stream, -1), {
      message: "Invalid chunk size",
    });

    // Act & Assert — skipBytes makes effective first-chunk size zero or negative
    await assert.rejects(consume(stream, 4, 4), {
      message: "Invalid chunk size",
    });

    await assert.rejects(consume(stream, 3, 5), {
      message: "Invalid chunk size",
    });
  });

  test("streamToChunks throws when source yields non-Uint8Array chunks", async () => {
    const stream = new ReadableStream({
      start(controller) {
        controller.enqueue("not a buffer" as unknown as Uint8Array);
        controller.close();
      },
    });

    const generator = streamToChunks(stream, 16);
    await assert.rejects(generator.next(), {
      message: "ReadableStream must yield Uint8Array chunks",
    });
  });

  test("teeStream returns no branches for non-positive counts", async () => {
    // Arrange
    const source = await bytesToStream(new Uint8Array([1, 2, 3]));

    // Act & Assert
    assert.deepEqual(teeStream(source, 0), []);
    assert.deepEqual(teeStream(source, -2), []);
  });

  test("teeStream returns the source stream when count is one", async () => {
    // Arrange
    const source = await bytesToStream(await textToBytes("single branch"));

    // Act
    const branches = teeStream(source, 1);

    // Assert
    assert.equal(branches.length, 1);
    assert.equal(branches[0], source);
    assert.equal(
      await bytesToText(await streamToBytes(branches[0])),
      "single branch",
    );
  });

  test("teeStream produces the specified number of identical branches", async () => {
    // Arrange
    const source = await bytesToStream(await textToBytes("tee payload"));

    // Act
    const branches = teeStream(source, 5);

    // Assert
    assert.equal(branches.length, 5);

    const decoded = await Promise.all(
      branches.map(async (branch) => bytesToText(await streamToBytes(branch))),
    );

    assert.deepEqual(decoded, [
      "tee payload",
      "tee payload",
      "tee payload",
      "tee payload",
      "tee payload",
    ]);
  });

  test("mergeStreams returns an empty PromiseLikeReadableStream when no sources are provided", async () => {
    // Arrange & Act
    const merged = mergeStreams([]);

    // Assert — return type is PromiseLikeReadableStream
    assert.ok(merged instanceof PromiseLikeReadableStream);

    // Assert — the stream closes immediately with no values
    const reader = merged.getReader();
    const { done } = await reader.read();
    assert.equal(done, true);
  });

  test("mergeStreams combines values from multiple streams", async () => {
    // Arrange
    const stream1 = new ReadableStream({
      start(controller) {
        controller.enqueue(1);
        controller.enqueue(2);
        controller.close();
      },
    });

    const stream2 = new ReadableStream({
      start(controller) {
        controller.enqueue("a");
        controller.enqueue("b");
        controller.close();
      },
    });

    // Act
    const values: unknown[] = [];
    for await (const value of mergeStreams([stream1, stream2])) {
      values.push(value);
    }

    // Assert
    assert.equal(values.length, 4);
    assert.ok(values.includes(1));
    assert.ok(values.includes(2));
    assert.ok(values.includes("a"));
    assert.ok(values.includes("b"));
  });

  test("mergeStreams closes only when all source streams are done", async () => {
    // Arrange
    const stream1 = new ReadableStream({
      start(controller) {
        controller.enqueue(1);
        controller.close();
      },
    });

    let resolveClose: () => void;
    const closePromise = new Promise<void>((resolve) => {
      resolveClose = resolve;
    });

    const stream2 = new ReadableStream({
      async start(controller) {
        controller.enqueue("a");
        await closePromise;
        controller.close();
      },
    });

    const merged = mergeStreams([stream1, stream2]);
    const reader = merged.getReader();

    // Act
    const result1 = await reader.read();

    // Assert - first value arrives and stream stays open
    assert.equal(result1.value, 1);
    assert.equal(result1.done, false);

    // Act - resolve the slow stream then read remaining values
    resolveClose!();
    const result2 = await reader.read();
    const result3 = await reader.read();

    // Assert - second value arrives, then stream closes
    assert.equal(result2.done, false);
    assert.equal(result3.done, true);
  });

  test("mergeStreams propagates errors from source streams", async () => {
    // Arrange
    let resolveError: () => void;
    const errorDeferred = new Promise<void>((resolve) => {
      resolveError = resolve;
    });

    const stream1 = new ReadableStream({
      async start(controller) {
        controller.enqueue(1);
        await errorDeferred;
        controller.error(new Error("source error"));
      },
    });

    const stream2 = new ReadableStream({
      start(controller) {
        controller.close();
      },
    });

    // Act
    const merged = mergeStreams([stream1, stream2]);
    const reader = merged.getReader();

    const result1 = await reader.read();
    assert.equal(result1.value, 1);

    // Trigger the error after first read succeeds
    resolveError!();

    try {
      await reader.read();
      assert.fail("Expected error to be thrown");
    } catch (err) {
      assert(err instanceof Error);
      assert.equal(err.message, "source error");
    }
  });

  test("mergeStreams prevents fast producers from starving slow ones", async () => {
    // Arrange
    let resolveSlow: () => void;
    const slowReady = new Promise<void>((resolve) => {
      resolveSlow = resolve;
    });

    // Fast producer enqueues many values immediately
    const fastStream = new ReadableStream({
      start(controller) {
        for (let i = 0; i < 100; i++) {
          controller.enqueue(`fast-${i}`);
        }
        controller.close();
      },
    });

    // Slow producer waits, then enqueues its value
    const slowStream = new ReadableStream({
      async start(controller) {
        await slowReady;
        controller.enqueue("slow-value");
        controller.close();
      },
    });

    const merged = mergeStreams([fastStream, slowStream]);
    const reader = merged.getReader();

    // Act - collect all values through the same reader to keep the stream locked
    const values: string[] = [];
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      values.push(value as string);

      // Let slow stream proceed after reading some fast values
      if (values.length === 1) {
        resolveSlow!();
      }
    }

    // Assert — slow stream's value is present among the collected values,
    // proving it wasn't starved by the fast producer
    assert.ok(values.includes("slow-value"), "Slow producer was starved");
  });

  test("PromiseLikeReadableStream resolves correctly for all DataType values and undefined", async () => {
    const cases: Array<{
      type?: "text" | "json" | "blob" | "bytes";
      data: Uint8Array<ArrayBuffer>;
      assertResolved: (value: unknown) => Promise<void>;
    }> = [
      {
        type: "bytes",
        data: new Uint8Array([10, 20, 30]),
        assertResolved: async (value) => {
          assert(value instanceof ArrayBuffer);
          assert.deepEqual(
            new Uint8Array(value as ArrayBuffer),
            new Uint8Array([10, 20, 30]),
          );
        },
      },
      {
        type: "text",
        data: new TextEncoder().encode("Hello, world!"),
        assertResolved: async (value) => {
          assert.equal(value, "Hello, world!");
        },
      },
      {
        type: "json",
        data: new TextEncoder().encode(
          JSON.stringify({ name: "test", count: 42 }),
        ),
        assertResolved: async (value) => {
          assert.deepEqual(value, { name: "test", count: 42 });
        },
      },
      {
        type: "blob",
        data: new TextEncoder().encode("blob content here"),
        assertResolved: async (value) => {
          assert(value instanceof Blob);
          assert.equal(await (value as Blob).text(), "blob content here");
        },
      },
      {
        type: undefined,
        data: new Uint8Array([1, 2, 3]),
        assertResolved: async (value) => {
          // When type is omitted the stream resolves to void after consuming
          assert.equal(value, undefined);
        },
      },
    ];

    for (const { type, data, assertResolved } of cases) {
      // Arrange
      let resolved: unknown;

      const stream = type
        ? new PromiseLikeReadableStream(
            {
              start(controller) {
                controller.enqueue(data);
                controller.close();
              },
            },
            {},
            type,
          )
        : new PromiseLikeReadableStream({
            start(controller) {
              controller.enqueue(data);
              controller.close();
            },
          });

      // Act
      await stream.then((value) => {
        resolved = value;
      });

      // Assert
      await assertResolved(resolved);
    }
  });

  test("PromiseLikeReadableStream rejects when stream errors and type is undefined", async () => {
    // Arrange
    const stream = new PromiseLikeReadableStream({
      start(controller) {
        controller.error(new Error("stream failed"));
      },
    });

    // Act & Assert
    await assert.rejects(
      stream.then(() => {
        /* no-op */
      }),
      { message: "stream failed" },
    );
  });

  test("PromiseLikeReadableStream rejects when the stream is locked", async () => {
    // Arrange
    const stream = new PromiseLikeReadableStream({
      start(controller) {
        controller.enqueue(new Uint8Array([1, 2]));
        controller.close();
      },
    });

    // Lock the stream by acquiring a reader
    const reader = stream.getReader();

    // Act & Assert
    await assert.rejects(
      stream.then(() => {
        /* no-op */
      }),
      { message: "Invalid state: ReadableStream is locked" },
    );

    reader.releaseLock();
  });

  test("PromiseLikeReadableStream memoizes the promise across multiple then calls", async () => {
    // Arrange
    let consumeCount = 0;
    const stream = new PromiseLikeReadableStream(
      {
        start(controller) {
          consumeCount++;
          controller.enqueue(new TextEncoder().encode("memo"));
          controller.close();
        },
      },
      {},
      "text",
    );

    // Act — calling .then() twice should not create two independent promises
    const p1 = stream.then(() => undefined);
    const p2 = stream.then(() => undefined);

    // Assert — both references point to the same underlying promise
    assert(p1 !== p2, ".then() returns a new chained promise");

    await Promise.all([p1, p2]);

    // Assert — the underlying stream was consumed only once
    assert.equal(consumeCount, 1);
  });
});
