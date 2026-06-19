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
  test("dataToStream should preserve stream inputs and infer blob/text/json types", async () => {
    // Arrange
    const sourceStream = await bytesToStream(
      await textToBytes("stream payload"),
    );

    // Act
    const streamResult = dataToStream(sourceStream);
    const blobResult = dataToStream(new Blob(["blob payload"]));
    const textResult = dataToStream("text payload");
    const jsonResult = dataToStream({ ok: true, value: 7 });

    // Assert
    assert.equal(streamResult.type, "stream");
    assert.equal(streamResult.stream, sourceStream);
    assert.equal(blobResult.type, "blob");
    assert.equal(textResult.type, "text");
    assert.equal(jsonResult.type, "json");

    assert.equal(
      await bytesToText(await streamToBytes(blobResult.stream)),
      "blob payload",
    );
    assert.equal(
      await bytesToText(await streamToBytes(textResult.stream)),
      "text payload",
    );
    assert.equal(
      await bytesToText(await streamToBytes(jsonResult.stream)),
      '{"ok":true,"value":7}',
    );
  });

  test("dataToStream should respect typed array views when producing binary streams", async () => {
    // Arrange
    const input = new Uint8Array([9, 8, 7, 6, 5]).subarray(1, 4);

    // Act
    const result = dataToStream(input);
    const bytes = await streamToBytes(result.stream);

    // Assert
    assert.equal(result.type, "binary");
    assert.deepEqual(bytes, new Uint8Array([8, 7, 6]));
  });

  test("streamToChunks should emit fixed-size chunks and mark only the last one as done", async () => {
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
    const packets: Array<{ chunk: number[]; done: boolean }> = [];
    for await (const packet of streamToChunks(stream, 2)) {
      packets.push({ chunk: [...packet.chunk], done: packet.done });
    }

    // Assert
    assert.deepEqual(packets, [
      { chunk: [1, 2], done: false },
      { chunk: [3, 4], done: false },
      { chunk: [5, 6], done: true },
    ]);
  });

  test("streamToChunks should emit a final partial chunk when the stream ends early", async () => {
    // Arrange
    const stream = await bytesToStream(new Uint8Array([10, 11, 12]));

    // Act
    const packets: Array<{ chunk: number[]; done: boolean }> = [];
    for await (const packet of streamToChunks(stream, 2)) {
      packets.push({ chunk: [...packet.chunk], done: packet.done });
    }

    // Assert
    assert.deepEqual(packets, [
      { chunk: [10, 11], done: false },
      { chunk: [12], done: true },
    ]);
  });

  test("teeStream should return no branches for non-positive counts", async () => {
    // Arrange
    const source = await bytesToStream(new Uint8Array([1, 2, 3]));

    // Act & Assert
    assert.deepEqual(teeStream(source, 0), []);
    assert.deepEqual(teeStream(source, -2), []);
  });

  test("teeStream should return the source stream when count is one", async () => {
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

  test("teeStream should produce the specified number of identical branches", async () => {
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
