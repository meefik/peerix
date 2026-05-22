/**
 * Compress a byte sequence using the browser CompressionStream API with
 * the `deflate` algorithm. If the API is unavailable or an error occurs,
 * the original bytes are returned unchanged.
 *
 * @param uncompressed The bytes to compress.
 * @returns The compressed bytes, or the original bytes when compression is
 * not supported or fails.
 */
export async function compressMessage(
  uncompressed: Uint8Array,
): Promise<Uint8Array> {
  if (!('CompressionStream' in window)) return uncompressed;
  try {
    const stream = new Blob([new Uint8Array(uncompressed)]).stream();
    const compressedStream = stream.pipeThrough(
      new CompressionStream('deflate'),
    );
    const arrayBuffer = await new Response(compressedStream).arrayBuffer();
    return new Uint8Array(arrayBuffer);
  } catch {
    return uncompressed;
  }
}

/**
 * Decompress a deflate-compressed byte sequence using the browser
 * DecompressionStream API. If the API is unavailable or decompression
 * fails, the original bytes are returned unchanged.
 *
 * @param compressed The compressed bytes.
 * @returns The decompressed bytes, or the original bytes when decompression
 * is not supported or fails.
 */
export async function decompressMessage(
  compressed: Uint8Array,
): Promise<Uint8Array> {
  if (!('DecompressionStream' in window)) return compressed;
  try {
    const stream = new Blob([new Uint8Array(compressed)]).stream();
    const decompressedStream = stream.pipeThrough(
      new DecompressionStream('deflate'),
    );
    const buffer = await new Response(decompressedStream).arrayBuffer();
    return new Uint8Array(buffer);
  } catch {
    return compressed;
  }
}
