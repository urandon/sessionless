import { sha256 } from '@noble/hashes/sha2.js';
import { bytesToHex } from '@noble/hashes/utils.js';
import type { AnswerCitationManifest } from './contract';

// Test-only identities. The first vector is independently pinned from Node's
// standard crypto implementation in the contract test; no new dependency.
export function sourceId(operation: string, rank: number, url: string): string {
  return (
    'source-' +
    bytesToHex(
      sha256(
        new TextEncoder().encode(`sessionless.web-search-source.v1\0${operation}\0${rank}\0${url}`),
      ),
    )
  );
}

export function citationManifest(count = 1): AnswerCitationManifest {
  return {
    schema: 'sessionless.answer-citations.v1',
    run_id: 'run-cited',
    attempt_id: 'attempt-cited',
    sources: Array.from({ length: count }, (_, i) => {
      const operation = (i < 5 ? 'a' : 'd').repeat(64);
      const rank = (i % 5) + 1;
      const url = 'https://example.com/source';
      return {
        source_id: sourceId(operation, rank, url),
        rank,
        url,
        title: '<img src=x onerror=alert(1)> Source title',
        title_truncated: false,
        operation_digest: operation,
        observation_digest: 'b'.repeat(64),
        response_digest: 'c'.repeat(64),
        retrieved_at: '2026-11-04T00:00:00.000123Z',
        published_at: '2026-11-03T12:00:00Z',
        content_kind: 'search_excerpt',
      };
    }),
  };
}
