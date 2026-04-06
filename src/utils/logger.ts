const PREFIX = 'peerix';

let allow: RegExp[];
let deny: RegExp[];

/**
 * Logger utility to create namespaced loggers.
 *
 * To enable logs in your browser:
 * `localStorage.debug = 'peerix:*'`
 *
 * Possible patterns:
 * - '*'
 * - 'namespace:*'
 * - 'namespace:subnamespace'
 * - 'namespace:subnamespace*'
 * - '-namespace:excluded'
 * - 'multiple,patterns,-with:exclusions'
 *
 * Possible log levels: debug, info, warn, error.
 *
 * Example usage:
 * ```javascript
 * import log from 'utils/logger.js';
 * log('module:submodule', 'This is a debug message');
 * log('module:submodule', () => 'This is a message inside a function');
 * log('module:submodule', () => ({ error: new Error('Something went wrong') }));
 * ```
 *
 * @param namespace The namespace for the logger.
 * @param args The data to log.
 */
export default async function log(namespace: string, ...args: any) {
  if (!allow || !deny) {
    [allow, deny] = compile(readDebugSetting());
  }
  const ns = `${PREFIX}:${namespace}`;
  if (!isEnabled(ns, allow, deny)) return;
  const ts = new Date().toISOString().slice(11, -1);
  const data = [];
  for (const arg of args) {
    const res = typeof arg === 'function' ? await arg() : arg;
    data.push(stringify(res));
  }
  console.log(`[${ns}]`, ...data);
}

function stringify(value: any): string {
  try {
    return JSON.stringify(value, (k, v) => {
      if (v instanceof Error) {
        return { name: v.name, message: v.message };
      }
      if (v instanceof Map) {
        return Array.from(v);
      }
      if (v instanceof Set) {
        return Array.from(v);
      }
      if (v instanceof Blob) {
        return { type: v.type, size: v.size };
      }
      if (v instanceof MediaStream) {
        return {
          id: v.id,
          active: v.active,
          tracks: v.getTracks().map(t => (
            { id: t.id, kind: t.kind, label: t.label, enabled: t.enabled }),
          ),
        };
      }
      if (v instanceof MediaStreamTrack) {
        return { id: v.id, kind: v.kind, label: v.label, enabled: v.enabled };
      }
      if (v instanceof RTCDataChannel) {
        return { id: v.id, label: v.label, readyState: v.readyState };
      }
      if (typeof v?.toObject === 'function') {
        return v.toObject();
      }
      return v;
    });
  }
  catch (e) {
    return value;
  }
}

function escapeRegexPart(str: string): string {
  return str.replace(/[.+?^${}()|[\]\\]/g, '\\$&');
}

function patternToRegex(pattern: string): RegExp {
  // split on '*' to escape other chars, then join with '.*'
  const parts = pattern.split('*').map(escapeRegexPart);
  return new RegExp(`^${parts.join('.*')}$`);
}

function compile(raw: string): [RegExp[], RegExp[]] {
  const allow: RegExp[] = [];
  const deny: RegExp[] = [];
  raw.split(',')
    .map(s => s.trim())
    .filter(Boolean)
    .forEach((p) => {
      if (p.startsWith('-')) {
        const sub = p.slice(1);
        if (sub) deny.push(patternToRegex(sub));
      }
      else {
        allow.push(patternToRegex(p));
      }
    });
  return [allow, deny];
}

function isEnabled(ns: string, allow: RegExp[], deny: RegExp[]): boolean {
  for (const r of deny) if (r.test(ns)) return false;
  for (const r of allow) if (r.test(ns)) return true;
  return false;
}

function readDebugSetting(): string {
  try {
    const { localStorage } = window || {};
    if (!localStorage) return '';
    return localStorage.getItem('debug') || '';
  }
  catch (e) {
    return '';
  }
}
