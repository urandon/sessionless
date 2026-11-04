import { describe, expect, it } from 'vitest';
import { validateAnswerCitations } from './contract';
import { citationManifest, sourceId } from './test-fixtures';

describe('answer citation public DATA', () => {
  it.each([
    'HTTPS://EXAMPLE.COM/source',
    'https://example.com:/source',
    'https://example.com:00443/source',
    'https://example.com/a?x=1&y=2#fragment',
  ])('preserves original Go-valid HTTPS URL spelling: %s', (url) => {
    const v = citationManifest();
    v.sources[0]!.url = url;
    v.sources[0]!.source_id = sourceId(v.sources[0]!.operation_digest, 1, url);
    expect(validateAnswerCitations(v, v.run_id).sources[0]!.url).toBe(url);
  });
  it('checks the independently pinned source identity and owns ordered copies', () => {
    const v = citationManifest();
    expect(v.sources[0]!.source_id).toBe(
      'source-27de5f55c40f9dabe5546f4666d78771be1b63a1062a280319d6fe21d1bc4c49',
    );
    const got = validateAnswerCitations(v, v.run_id);
    got.sources[0]!.title = 'changed';
    expect(v.sources[0]!.title).not.toBe('changed');
    const proxied = new Proxy(v, {});
    expect(validateAnswerCitations(proxied, v.run_id)).toEqual(v);
    for (const count of [0, 1, 10])
      expect(validateAnswerCitations(citationManifest(count), 'run-cited').sources).toHaveLength(
        count,
      );
    const mixed = citationManifest(10);
    mixed.sources.reverse();
    expect(validateAnswerCitations(mixed, mixed.run_id).sources.map((s) => s.source_id)).toEqual(
      mixed.sources.map((s) => s.source_id),
    );
  });

  it.each([
    [
      'foreign run',
      (v: Record<string, unknown>) => {
        v.run_id = 'foreign';
      },
    ],
    [
      'null sources',
      (v: Record<string, unknown>) => {
        v.sources = null;
      },
    ],
    [
      'eleven sources',
      (v: Record<string, unknown>) => {
        v.sources = citationManifest(11).sources;
      },
    ],
    [
      'private field',
      (v: Record<string, unknown>) => {
        v.receipt = 'private';
      },
    ],
    [
      'unknown schema',
      (v: Record<string, unknown>) => {
        v.schema = 'other';
      },
    ],
  ] as const)('rejects %s', (_, change) => {
    const v = citationManifest() as unknown as Record<string, unknown>;
    change(v);
    expect(() => validateAnswerCitations(v, 'run-cited')).toThrow('Invalid answer citations.');
  });

  it('denies malformed sources, required false absence and null publication', () => {
    const changes = [
      (s: Record<string, unknown>) => {
        delete s.title_truncated;
      },
      (s: Record<string, unknown>) => {
        s.title_truncated = null;
      },
      (s: Record<string, unknown>) => {
        s.published_at = null;
      },
      (s: Record<string, unknown>) => {
        s.retrieved_at = '2026-02-30T12:00:00Z';
      },
      (s: Record<string, unknown>) => {
        s.title = '\ud800';
      },
      (s: Record<string, unknown>) => {
        s.title = 'é'.repeat(257);
      },
      (s: Record<string, unknown>) => {
        s.source_id = 'source-' + 'f'.repeat(64);
      },
      (s: Record<string, unknown>) => {
        s.observation_digest = 'B'.repeat(64);
      },
      (s: Record<string, unknown>) => {
        s.content_kind = 'page_read';
      },
      (s: Record<string, unknown>) => {
        s.excerpt = 'private';
      },
    ];
    for (const change of changes) {
      const v = citationManifest();
      change(v.sources[0]! as unknown as Record<string, unknown>);
      expect(() => validateAnswerCitations(v, v.run_id)).toThrow();
    }
    const v = citationManifest();
    v.sources.push(structuredClone(v.sources[0]!));
    expect(() => validateAnswerCitations(v, v.run_id)).toThrow();
  });

  it.each([
    'javascript:alert(1)',
    'http://example.com/',
    'https://127.0.0.1/',
    'https://0x7f.1/',
    'https://host.local/',
    'https://metadata.google.internal/',
    'https://user:pass@example.com/',
    'https://example.com/\\evil',
  ])('denies unsafe URL even with coherent ID: %s', (url) => {
    const v = citationManifest();
    v.sources[0]!.url = url;
    v.sources[0]!.source_id = sourceId(v.sources[0]!.operation_digest, 1, url);
    expect(() => validateAnswerCitations(v, v.run_id)).toThrow();
  });
});
