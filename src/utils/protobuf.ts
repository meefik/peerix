/**
 * A minimal Protobuf-like encoder/decoder for:
 * bool, int32, uint32, float, string, and bytes.
 *
 * @example
 * const schema = {
 *   field1: { id: 1, type: 'bool' },
 *   field2: { id: 2, type: 'int32' },
 *   field3: { id: 3, type: 'uint32' },
 *   field4: { id: 4, type: 'float' },
 *   field5: { id: 5, type: 'string' },
 *   field6: { id: 6, type: 'bytes' },
 * };
 *
 * const obj = {
 *   field1: true,
 *   field2: -42,
 *   field3: 123456,
 *   field4: 3.14,
 *   field5: 'Hello, World!',
 *   field6: new Uint8Array([1, 2, 3]),
 * };
 * const encoded = encode(obj, schema);
 * const decoded = decode(encoded, schema);
 *
 * console.log(decoded); // decoded is deeply equal to obj
 */

type FieldType = 'bool' | 'int32' | 'uint32' | 'float' | 'string' | 'bytes';
type SchemaField = { id: number; type: FieldType };
type Schema = Record<string, SchemaField>;
type WireType = 0 | 2 | 5;
type Decoded<T> = { value: T; index: number };
type Value = boolean | number | string | Uint8Array;
type Codec = {
  wire: WireType;
  encode: (value: unknown, textEncoder: TextEncoder) => number[] | null;
  decode: (
    buffer: Uint8Array,
    index: number,
    textDecoder: TextDecoder,
  ) => Decoded<Value> | null;
};

const INT32_MIN = -2147483648;
const INT32_MAX = 2147483647;
const UINT32_MAX = 0xffffffff;
const MAX_FIELD_ID = 536870911;

const isInt32 = (v: unknown): v is number =>
  typeof v === 'number' &&
  Number.isInteger(v) &&
  v >= INT32_MIN &&
  v <= INT32_MAX;

const isUint32 = (v: unknown): v is number =>
  typeof v === 'number' && Number.isInteger(v) && v >= 0 && v <= UINT32_MAX;

const isFiniteNumber = (v: unknown): v is number =>
  typeof v === 'number' && Number.isFinite(v);

const isSafeFiniteFloat32 = (v: unknown): v is number =>
  isFiniteNumber(v) && Number.isFinite(Math.fround(v));

const isValidFieldId = (id: number): boolean =>
  Number.isInteger(id) && id > 0 && id <= MAX_FIELD_ID;

function encodeVarint(value: number): number[] {
  const out: number[] = [];
  let n = value >>> 0;

  while (n > 127) {
    out.push((n & 127) | 128);
    n >>>= 7;
  }

  out.push(n & 127);
  return out;
}

function decodeVarint(
  buffer: Uint8Array,
  index: number,
): Decoded<number> | null {
  let value = 0;
  let shift = 0;

  for (let i = 0; i < 5; i += 1) {
    if (index >= buffer.length) return null;

    const byte = buffer[index++];
    const chunk = byte & 127;
    if (i === 4 && (chunk & 0x70) !== 0) return null;

    value |= chunk << shift;
    if ((byte & 128) === 0) {
      return { value: value >>> 0, index };
    }

    shift += 7;
  }

  return null;
}

const encodeFloat32 = (value: number): number[] => {
  const ab = new ArrayBuffer(4);
  new DataView(ab).setFloat32(0, value, true);
  return Array.from(new Uint8Array(ab));
};

function decodeFloat32(
  buffer: Uint8Array,
  index: number,
): Decoded<number> | null {
  if (index + 4 > buffer.length) return null;
  const view = new DataView(buffer.buffer, buffer.byteOffset + index, 4);
  return { value: view.getFloat32(0, true), index: index + 4 };
}

function decodeLengthDelimited(
  buffer: Uint8Array,
  index: number,
): Decoded<Uint8Array> | null {
  const len = decodeVarint(buffer, index);
  if (!len) return null;

  const end = len.index + len.value;
  if (end > buffer.length) return null;

  return { value: buffer.subarray(len.index, end), index: end };
}

function appendBytes(target: number[], source: number[] | Uint8Array): void {
  for (let i = 0; i < source.length; i += 1) {
    target.push(source[i]);
  }
}

const CODECS: Record<FieldType, Codec> = {
  bool: {
    wire: 0,
    encode: (v) => (typeof v === 'boolean' ? encodeVarint(v ? 1 : 0) : null),
    decode: (buffer, index) => {
      const decoded = decodeVarint(buffer, index);
      if (!decoded || (decoded.value !== 0 && decoded.value !== 1)) return null;
      return { value: decoded.value === 1, index: decoded.index };
    },
  },
  int32: {
    wire: 0,
    encode: (v) => (isInt32(v) ? encodeVarint((v | 0) >>> 0) : null),
    decode: (buffer, index) => {
      const decoded = decodeVarint(buffer, index);
      return decoded
        ? { value: decoded.value | 0, index: decoded.index }
        : null;
    },
  },
  uint32: {
    wire: 0,
    encode: (v) => (isUint32(v) ? encodeVarint(v) : null),
    decode: (buffer, index) => decodeVarint(buffer, index),
  },
  float: {
    wire: 5,
    encode: (v) => (isSafeFiniteFloat32(v) ? encodeFloat32(v) : null),
    decode: (buffer, index) => decodeFloat32(buffer, index),
  },
  string: {
    wire: 2,
    encode: (v, enc) => {
      if (typeof v !== 'string') return null;
      const bytes = enc.encode(v);
      const out = encodeVarint(bytes.length);
      appendBytes(out, bytes);
      return out;
    },
    decode: (buffer, index, dec) => {
      const chunk = decodeLengthDelimited(buffer, index);
      return chunk
        ? { value: dec.decode(chunk.value), index: chunk.index }
        : null;
    },
  },
  bytes: {
    wire: 2,
    encode: (v) => {
      if (!(v instanceof Uint8Array)) return null;
      const out = encodeVarint(v.length);
      appendBytes(out, v);
      return out;
    },
    decode: (buffer, index) => {
      const chunk = decodeLengthDelimited(buffer, index);
      return chunk
        ? { value: new Uint8Array(chunk.value), index: chunk.index }
        : null;
    },
  },
};

function skipUnknownField(
  wireType: number,
  buffer: Uint8Array,
  index: number,
): number | null {
  if (wireType === 0) {
    const decoded = decodeVarint(buffer, index);
    return decoded ? decoded.index : null;
  }

  if (wireType === 1) {
    return index + 8 <= buffer.length ? index + 8 : null;
  }

  if (wireType === 2) {
    const chunk = decodeLengthDelimited(buffer, index);
    return chunk ? chunk.index : null;
  }

  if (wireType === 5) {
    return index + 4 <= buffer.length ? index + 4 : null;
  }

  return null;
}

/** Encodes an object using the provided schema. */
export function encode(
  obj: Record<string, unknown>,
  schema: Schema,
): Uint8Array | null {
  const out: number[] = [];
  const textEncoder = new TextEncoder();
  const fieldIds = new Set<number>();

  for (const [name, field] of Object.entries(schema)) {
    if (!isValidFieldId(field.id) || fieldIds.has(field.id)) return null;
    fieldIds.add(field.id);

    const value = obj[name];
    if (value === undefined || value === null) continue;

    const codec = CODECS[field.type as FieldType];
    if (!codec) return null;
    const payload = codec.encode(value, textEncoder);
    if (!payload) return null;

    const tag = ((field.id << 3) | codec.wire) >>> 0;
    appendBytes(out, encodeVarint(tag));
    appendBytes(out, payload);
  }

  return new Uint8Array(out);
}

/** Decodes a buffer using the provided schema. */
export function decode(
  buffer: Uint8Array,
  schema: Schema,
): Record<string, unknown> | null {
  const result: Record<string, unknown> = {};
  const fieldsById = new Map<number, { name: string; type: FieldType }>();
  const textDecoder = new TextDecoder();

  for (const [name, field] of Object.entries(schema)) {
    if (!isValidFieldId(field.id) || fieldsById.has(field.id)) return null;
    fieldsById.set(field.id, { name, type: field.type });
  }

  let index = 0;
  while (index < buffer.length) {
    const tag = decodeVarint(buffer, index);
    if (!tag) return null;

    index = tag.index;
    const fieldId = tag.value >>> 3;
    const wireType = tag.value & 7;
    if (fieldId === 0) return null;
    const field = fieldsById.get(fieldId);

    if (!field) {
      const next = skipUnknownField(wireType, buffer, index);
      if (next === null) return null;
      index = next;
      continue;
    }

    const codec = CODECS[field.type as FieldType];
    if (!codec) return null;
    if (codec.wire !== wireType) return null;

    const decoded = codec.decode(buffer, index, textDecoder);
    if (!decoded) return null;

    result[field.name] = decoded.value;
    index = decoded.index;
  }

  return result;
}
