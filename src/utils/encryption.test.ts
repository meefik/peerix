import { suite, test } from 'node:test';
import assert from 'node:assert/strict';
import {
  decrypt,
  encrypt,
  exportPublicKey,
  generateDerivedKey,
  generateKeyPair,
  importPublicKey,
  sha256,
} from './encryption.js';

suite('utils/encryption', async () => {
  test('sha256 should be deterministic and input-sensitive', async () => {
    const a = await sha256('hello');
    const b = await sha256('hello');
    const c = await sha256('hello!');

    assert.equal(a, b);
    assert.notEqual(a, c);
    assert.ok(a.length > 0);
  });

  test('generateKeyPair should produce ECDH P-256 keys', async () => {
    const keyPair = await generateKeyPair();

    assert.equal(keyPair.privateKey.type, 'private');
    assert.equal(keyPair.publicKey.type, 'public');
    assert.equal(keyPair.privateKey.algorithm.name, 'ECDH');
    assert.equal(
      (keyPair.privateKey.algorithm as EcKeyAlgorithm).namedCurve,
      'P-256',
    );
  });

  test('should derive compatible shared keys and roundtrip encrypt/decrypt', async () => {
    const alice = await generateKeyPair();
    const bob = await generateKeyPair();

    const aliceShared = await generateDerivedKey(
      alice.privateKey,
      bob.publicKey,
    );
    const bobShared = await generateDerivedKey(bob.privateKey, alice.publicKey);

    const plaintext = new TextEncoder().encode('peerix secure payload');
    const encrypted = await encrypt(plaintext, aliceShared);
    const decrypted = await decrypt(encrypted, bobShared);

    assert.equal(new TextDecoder().decode(decrypted), 'peerix secure payload');
    assert.equal(encrypted.slice(0, 12).length, 12);
    assert.ok(encrypted.length > plaintext.length);
  });

  test('exportPublicKey/importPublicKey should preserve key usability', async () => {
    const alice = await generateKeyPair();
    const bob = await generateKeyPair();

    const exported = await exportPublicKey(bob.publicKey);
    assert.equal(exported.length, 33);
    assert.ok(exported[0] === 0x02 || exported[0] === 0x03);

    const imported = await importPublicKey(exported);
    const aliceShared = await generateDerivedKey(alice.privateKey, imported);
    const bobShared = await generateDerivedKey(bob.privateKey, alice.publicKey);

    const plaintext = new TextEncoder().encode('compressed key path works');
    const encrypted = await encrypt(plaintext, aliceShared);
    const decrypted = await decrypt(encrypted, bobShared);

    assert.equal(
      new TextDecoder().decode(decrypted),
      'compressed key path works',
    );
  });

  test('importPublicKey should reject invalid compressed key length', async () => {
    await assert.rejects(importPublicKey(new Uint8Array(32)), {
      message: 'Compressed public key must be exactly 33 bytes',
    });
  });

  test('importPublicKey should reject invalid compressed key prefix', async () => {
    const invalid = new Uint8Array(33);
    invalid[0] = 0x04;

    await assert.rejects(importPublicKey(invalid), {
      message: 'Invalid compressed public key prefix',
    });
  });

  test('decrypt should fail for tampered ciphertext', async () => {
    const alice = await generateKeyPair();
    const bob = await generateKeyPair();

    const aliceShared = await generateDerivedKey(
      alice.privateKey,
      bob.publicKey,
    );
    const bobShared = await generateDerivedKey(bob.privateKey, alice.publicKey);

    const plaintext = new TextEncoder().encode(
      'auth tag should fail if tampered',
    );
    const encrypted = await encrypt(plaintext, aliceShared);
    encrypted[encrypted.length - 1] ^= 0x01;

    await assert.rejects(decrypt(encrypted, bobShared));
  });
});
