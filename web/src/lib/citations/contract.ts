import { sha256 } from '@noble/hashes/sha2.js';
import { bytesToHex } from '@noble/hashes/utils.js';
import type { components } from '../api/generated';

export type AnswerCitationManifest = components['schemas']['AnswerCitationManifestV1'];

function requireValid(ok: unknown): asserts ok {
  if (!ok) throw new TypeError('Invalid answer citations.');
}

function closed(
  value: unknown,
  required: string[],
  optional: string[] = [],
): Record<string, unknown> {
  requireValid(typeof value === 'object' && value !== null && !Array.isArray(value));
  const v = value as Record<string, unknown>;
  requireValid(required.every((key) => Object.hasOwn(v, key)));
  requireValid(Object.keys(v).every((key) => required.includes(key) || optional.includes(key)));
  return v;
}

function text(value: unknown, cap: number): value is string {
  if (typeof value !== 'string' || value.length === 0 || value.length > cap) return false;
  const bytes = new TextEncoder().encode(value);
  return bytes.length <= cap && new TextDecoder('utf-8', { fatal: true }).decode(bytes) === value;
}

function selector(value: unknown): value is string {
  return (
    typeof value === 'string' && value.length <= 160 && /^[A-Za-z0-9][A-Za-z0-9._:-]*$/.test(value)
  );
}

function digest(value: unknown): value is string {
  return typeof value === 'string' && /^[0-9a-f]{64}$/.test(value);
}

function instant(value: unknown): value is string {
  if (typeof value !== 'string') return false;
  const m = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?(Z|[+-]\d{2}:\d{2})$/.exec(
    value,
  );
  if (!m || !m[1] || !m[3] || m[1].startsWith('0000')) return false;
  const calendar = Date.parse(m[1] + 'Z');
  const at = Date.parse(value);
  return (
    Number.isFinite(calendar) &&
    Number.isFinite(at) &&
    new Date(calendar).toISOString().slice(0, 19) === m[1] &&
    new Date(at).getUTCFullYear() >= 1 &&
    new Date(at).getUTCFullYear() <= 9999 &&
    !(
      m[1] === '0001-01-01T00:00:00' &&
      !/[1-9]/.test(m[2] ?? '') &&
      ['Z', '+00:00', '-00:00'].includes(m[3])
    )
  );
}

// Syntax/hostname validation only. Never resolve DNS, fetch a page or claim
// that a presently returned public-looking URL is safe to navigate to.
function publicHttps(value: unknown): value is string {
  if (!text(value, 2048)) return false;
  for (const character of value) {
    const code = character.charCodeAt(0);
    if (code <= 32 || code === 127 || character === '\\') return false;
  }
  const match = /^https:\/\/([^/?#]+)/i.exec(value);
  const authority = match?.[1];
  if (!authority || authority.includes('@')) return false;
  const parts = authority.split(':');
  const hostPart = parts[0],
    port = parts[1];
  if (
    !hostPart ||
    parts.length > 2 ||
    (port !== undefined &&
      port !== '' &&
      (!/^\d+$/.test(port) || Number(port) < 1 || Number(port) > 65535))
  )
    return false;
  const host = hostPart.toLowerCase().replace(/\.$/, '');
  const labels = host.split('.');
  if (
    host.length > 253 ||
    labels.length < 2 ||
    labels.some((label) => !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(label))
  )
    return false;
  if (labels.every((label) => /^(?:\d+|0x[0-9a-f]*)$/.test(label))) return false;
  const blocked = [
    'localhost',
    'local',
    'internal',
    'localdomain',
    'lan',
    'home',
    'test',
    'invalid',
    'onion',
    'metadata.google.internal',
    'metadata.goog',
    'instance-data.ec2.internal',
  ];
  if (blocked.some((suffix) => host === suffix || host.endsWith('.' + suffix))) return false;
  try {
    const url = new URL(value);
    return url.protocol === 'https:' && url.username === '' && url.password === '';
  } catch {
    return false;
  }
}

// Public DATA validation does not establish original durable provenance or
// current permission. The authorized event response supplies that boundary;
// never reconstruct this value from prose or event.content.data.
export function validateAnswerCitations(value: unknown, runId: unknown): AnswerCitationManifest {
  const v = closed(value, ['schema', 'run_id', 'attempt_id', 'sources']);
  requireValid(
    v.schema === 'sessionless.answer-citations.v1' &&
      selector(runId) &&
      v.run_id === runId &&
      selector(v.attempt_id),
  );
  requireValid(Array.isArray(v.sources) && v.sources.length <= 10);
  const seen = new Set<string>();
  const sources: AnswerCitationManifest['sources'] = [];
  for (const source of v.sources) {
    const s = closed(
      source,
      [
        'source_id',
        'rank',
        'url',
        'title',
        'title_truncated',
        'operation_digest',
        'observation_digest',
        'response_digest',
        'retrieved_at',
        'content_kind',
      ],
      ['published_at'],
    );
    requireValid(
      digest(s.operation_digest) && digest(s.observation_digest) && digest(s.response_digest),
    );
    requireValid(
      typeof s.rank === 'number' &&
        Number.isInteger(s.rank) &&
        s.rank >= 1 &&
        s.rank <= 5 &&
        publicHttps(s.url),
    );
    requireValid(
      text(s.title, 512) &&
        typeof s.title_truncated === 'boolean' &&
        instant(s.retrieved_at) &&
        s.content_kind === 'search_excerpt',
    );
    if (Object.hasOwn(s, 'published_at')) requireValid(instant(s.published_at));
    const original = `sessionless.web-search-source.v1\0${s.operation_digest}\0${s.rank}\0${s.url}`;
    const id = 'source-' + bytesToHex(sha256(new TextEncoder().encode(original)));
    requireValid(s.source_id === id && !seen.has(id));
    seen.add(id);
    sources.push({
      source_id: id,
      rank: s.rank,
      url: s.url,
      title: s.title,
      title_truncated: s.title_truncated,
      operation_digest: s.operation_digest,
      observation_digest: s.observation_digest,
      response_digest: s.response_digest,
      retrieved_at: s.retrieved_at,
      content_kind: 'search_excerpt',
      ...(Object.hasOwn(s, 'published_at') ? { published_at: s.published_at as string } : {}),
    });
  }
  // Go's canonical string encoder also quotes these five characters. Count
  // those bytes without admitting an extra public response allowance.
  const canonicalSize = JSON.stringify(v).replace(
    /[<>&\u2028\u2029]/g,
    (c) => '\\u' + c.charCodeAt(0).toString(16).padStart(4, '0'),
  );
  requireValid(new TextEncoder().encode(canonicalSize).length <= 64 * 1024);
  // Svelte state is proxied and is deliberately not structuredClone-able.
  // Copy the validated scalars/array explicitly; never retain reactive inputs.
  return {
    schema: 'sessionless.answer-citations.v1',
    run_id: runId,
    attempt_id: v.attempt_id,
    sources,
  };
}
