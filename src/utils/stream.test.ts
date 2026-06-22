import { suite, test } from "node:test";
import assert from "node:assert/strict";
import { dataToStream, streamToChunks, teeStream } from "./stream.js";

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

suite("utils/stream", async () => {
  test("dataToStream preserves stream inputs and infers text/json/blob/bytes types", async () => {
    // Arrange
    const sourceStream = await bytesToStream(
      await textToBytes("stream payload"),
    );

    // Act
    const streamResult = dataToStream(sourceStream);
    const bytesResult = dataToStream(new Uint8Array([1, 2, 3]));
    const blobResult = dataToStream(new Blob(["blob payload"]));
    const textResult = dataToStream("text payload");
    const jsonResult = dataToStream({ ok: true, value: 7 });

    // Assert
    assert.equal(streamResult.type, "stream");
    assert.equal(streamResult.stream, sourceStream);
    assert.equal(bytesResult.type, "bytes");
    assert.equal(blobResult.type, "blob");
    assert.equal(textResult.type, "text");
    assert.equal(jsonResult.type, "json");

    const blobBytes = await streamToBytes(blobResult.stream);
    assert.equal(await bytesToText(blobBytes), "blob payload");

    const textBytes = await streamToBytes(textResult.stream);
    assert.equal(await bytesToText(textBytes), "text payload");

    const jsonBytes = await streamToBytes(jsonResult.stream);
    assert.equal(await bytesToText(jsonBytes), '{"ok":true,"value":7}');

    const bytesBytes = await streamToBytes(bytesResult.stream);
    assert.deepEqual(bytesBytes, new Uint8Array([1, 2, 3]));
  });

  test("dataToStream reports correct size for each data type", async () => {
    // Arrange
    const textInput = "hello";
    const binaryInput = new Uint8Array([1, 2, 3, 4, 5]);
    const arrayBuffer = new ArrayBuffer(7);
    const blobInput = new Blob(["some content"]);
    const sourceStream = await bytesToStream(await textToBytes("stream data"));
    const jsonInput = { key: "value", num: 42 };

    // Act
    const streamResult = dataToStream(sourceStream);
    const binaryResult = dataToStream(binaryInput);
    const arrayBufferResult = dataToStream(arrayBuffer);
    const blobResult = dataToStream(blobInput);
    const textResult = dataToStream(textInput);
    const jsonResult = dataToStream(jsonInput);

    // Assert - stream type has unknown size
    assert.equal(streamResult.size, -1);

    // Assert - binary and ArrayBuffer sizes match byteLength
    assert.equal(binaryResult.size, 5);
    assert.equal(arrayBufferResult.size, 7);

    // Assert - blob size matches Blob's size property
    assert.equal(blobResult.size, blobInput.size);

    // Assert - text and json sizes match encoded byte length
    const textBytes = await textToBytes(textInput);
    assert.equal(textResult.size, textBytes.byteLength);

    const jsonBytes = await textToBytes(JSON.stringify(jsonInput));
    assert.equal(jsonResult.size, jsonBytes.byteLength);
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
    const packets: Array<{ index: number; chunk: number[]; done: boolean }> =
      [];
    for await (const packet of streamToChunks(stream, 2)) {
      packets.push({
        index: packet.index,
        chunk: [...packet.chunk],
        done: packet.done,
      });
    }

    // Assert
    assert.deepEqual(packets, [
      { index: 0, chunk: [1, 2], done: false },
      { index: 1, chunk: [3, 4], done: false },
      { index: 2, chunk: [5, 6], done: true },
    ]);
  });

  test("streamToChunks emits a final partial chunk when the stream ends early", async () => {
    // Arrange
    const stream = await bytesToStream(new Uint8Array([10, 11, 12]));

    // Act
    const packets: Array<{ index: number; chunk: number[]; done: boolean }> =
      [];
    for await (const packet of streamToChunks(stream, 2)) {
      packets.push({
        index: packet.index,
        chunk: [...packet.chunk],
        done: packet.done,
      });
    }

    // Assert
    assert.deepEqual(packets, [
      { index: 0, chunk: [10, 11], done: false },
      { index: 1, chunk: [12], done: true },
    ]);
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
});
