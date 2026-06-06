import { suite, test } from 'node:test';
import assert from 'node:assert/strict';
import log from './logger.js';

async function withLoggerEnv(
  run: (calls: unknown[][]) => Promise<void>,
): Promise<void> {
  const originalConsole = globalThis.console;
  const originalLocalStorage = globalThis.localStorage;
  const calls: unknown[][] = [];

  (globalThis as any).console = {
    ...originalConsole,
    log: (...args: unknown[]) => {
      calls.push(args);
    },
  };
  (globalThis as any).localStorage = {
    getItem(key: string) {
      return key === 'debug'
        ? 'peerix:allowed:*,-peerix:allowed:blocked'
        : null;
    },
  };

  try {
    await run(calls);
  } finally {
    (globalThis as any).console = originalConsole;
    (globalThis as any).localStorage = originalLocalStorage;
  }
}

suite('utils/logger', async () => {
  test('should log enabled namespaces and stringify supported values', async () => {
    await withLoggerEnv(async (calls) => {
      const error = new Error('boom');
      const bytes = new Uint8Array([1, 2, 3]);
      const map = new Map([['k', 1]]);
      const set = new Set([4, 5]);
      const jsonLike = {
        toJSON() {
          return { ok: true };
        },
      };

      await log(
        'allowed:topic',
        'text',
        () => 'lazy',
        () => ({ error }),
        bytes,
        map,
        set,
        jsonLike,
      );

      assert.equal(calls.length, 1);
      assert.equal(calls[0][0], '[peerix:allowed:topic]');
      assert.equal(calls[0][1], '"text"');
      assert.equal(calls[0][2], '"lazy"');
      assert.equal(calls[0][3], '{"error":{"name":"Error","message":"boom"}}');
      assert.equal(calls[0][4], '{"type":"Uint8Array","byteLength":3}');
      assert.equal(calls[0][5], '[["k",1]]');
      assert.equal(calls[0][6], '[4,5]');
      assert.equal(calls[0][7], '{"ok":true}');
    });
  });

  test('should skip denied namespaces and avoid evaluating lazy arguments', async () => {
    let executed = 0;

    await withLoggerEnv(async (calls) => {
      await log('allowed:blocked', () => {
        executed += 1;
        return 'should-not-run';
      });

      assert.equal(executed, 0);
      assert.equal(calls.length, 0);
    });
  });

  test('should skip namespaces not included in allow patterns', async () => {
    await withLoggerEnv(async (calls) => {
      await log('other:topic', 'hidden');
      assert.equal(calls.length, 0);
    });
  });
});
