import { suite, test } from 'node:test';
import assert from 'node:assert/strict';
import { encode, decode } from './protobuf.js';

suite('utils/protobuf', async () => {
  test('should roundtrip all supported field types', async () => {
    const schema = {
      active: { id: 1, type: 'bool' as const },
      offset: { id: 2, type: 'int32' as const },
      count: { id: 3, type: 'uint32' as const },
      ratio: { id: 4, type: 'float' as const },
      label: { id: 5, type: 'string' as const },
      payload: { id: 6, type: 'bytes' as const },
    };
    const value = {
      active: true,
      offset: -42,
      count: 123456,
      ratio: 3.1415926,
      label: 'peerix',
      payload: new Uint8Array([1, 2, 3, 254]),
    };

    const encoded = encode(value, schema);

    assert.ok(encoded instanceof Uint8Array);
    assert.deepEqual(decode(encoded, schema), {
      ...value,
      ratio: Math.fround(value.ratio),
    });
  });

  test('should omit nullish fields during encoding', async () => {
    const schema = {
      present: { id: 1, type: 'string' as const },
      missing: { id: 2, type: 'uint32' as const },
      nullable: { id: 3, type: 'bytes' as const },
    };

    const encoded = encode(
      {
        present: 'value',
        missing: undefined,
        nullable: null,
      },
      schema,
    );

    assert.ok(encoded instanceof Uint8Array);
    assert.deepEqual(decode(encoded, schema), { present: 'value' });
  });

  test('should skip unknown fields while decoding', async () => {
    const schema = {
      active: { id: 1, type: 'bool' as const },
    };

    const encoded = encode({ active: true }, schema);
    assert.ok(encoded instanceof Uint8Array);

    const withUnknown = new Uint8Array([
      ...encoded,
      0x10,
      0x96,
      0x01,
      0x1a,
      0x03,
      0x09,
      0x08,
      0x07,
    ]);

    assert.deepEqual(decode(withUnknown, schema), { active: true });
  });

  test('should reject invalid schemas', async () => {
    const duplicateIds = {
      first: { id: 1, type: 'bool' as const },
      second: { id: 1, type: 'uint32' as const },
    };
    const invalidFieldId = {
      zero: { id: 0, type: 'bool' as const },
    };

    assert.equal(encode({ first: true, second: 1 }, duplicateIds), null);
    assert.equal(decode(new Uint8Array([0x08, 0x01]), duplicateIds), null);
    assert.equal(encode({ zero: true }, invalidFieldId), null);
    assert.equal(decode(new Uint8Array([0x08, 0x01]), invalidFieldId), null);
  });

  test('should reject malformed or wire-mismatched payloads', async () => {
    const stringSchema = {
      label: { id: 1, type: 'string' as const },
    };
    const boolSchema = {
      active: { id: 1, type: 'bool' as const },
    };

    assert.equal(decode(new Uint8Array([0x08, 0x01]), stringSchema), null);
    assert.equal(
      decode(new Uint8Array([0x0a, 0x02, 0x61]), stringSchema),
      null,
    );
    assert.equal(decode(new Uint8Array([0x08, 0x02]), boolSchema), null);
  });
});
