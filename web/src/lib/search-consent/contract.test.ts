import { describe, expect, it } from 'vitest';
import { validateConsentDiscovery, validateConsentSummary } from './contract';

const expires = '2026-10-20T00:00:00Z';
const pending = { request_id: 'request-1', revision: 1, status: 'pending', expires_at: expires };

describe('private search consent response contract', () => {
  it('accepts authenticated absence or a closed opaque selector, never query material', () => {
    expect(
      validateConsentDiscovery({ session_id: 'ses-1', run_id: 'run-1' }, 'ses-1', 'run-1').pending,
    ).toBeUndefined();
    const value = {
      session_id: 'ses-1',
      run_id: 'run-1',
      pending: { attempt_id: 'attempt-1', request_id: 'request-1', expires_at: expires },
    };
    expect(validateConsentDiscovery(value, 'ses-1', 'run-1')).toEqual(value);
    for (const patch of [
      { query: 'private' },
      { pending: null },
      { run_id: 'foreign' },
      { pending: { ...value.pending, query: 'private' } },
      { pending: { ...value.pending, attempt_id: '../other' } },
    ]) {
      expect(() => validateConsentDiscovery({ ...value, ...patch }, 'ses-1', 'run-1')).toThrow();
    }
  });

  it('preserves exact untrusted query text only in the private view', () => {
    const value = { ...pending, query: ' <script>alert(1)</script>\n https://private.example/ ' };
    expect(validateConsentSummary(value, 'request-1', true)).toEqual(value);
    expect(() => validateConsentSummary(value, 'request-1')).toThrow();
    for (const query of ['', 'x'.repeat(1025), '\ud800']) {
      expect(() => validateConsentSummary({ ...value, query }, 'request-1', true)).toThrow();
    }
  });

  it('rejects mismatched selectors, revisions, malformed instants and private metadata', () => {
    for (const patch of [
      { request_id: 'foreign' },
      { revision: 2 },
      { revision: 1.5 },
      { status: 'success' },
      { expires_at: '2026-02-30T00:00:00Z' },
      { expires_at: '0' },
      { receipt: 'secret' },
    ]) {
      expect(() => validateConsentSummary({ ...pending, ...patch }, 'request-1')).toThrow();
    }
    expect(
      validateConsentSummary({ ...pending, revision: 2, status: 'approved' }, 'request-1'),
    ).toMatchObject({ status: 'approved' });
    expect(
      validateConsentSummary({ ...pending, revision: 3, status: 'revoked' }, 'request-1'),
    ).toMatchObject({ status: 'revoked' });
    expect(() =>
      validateConsentSummary({ ...pending, revision: 3, status: 'approved' }, 'request-1'),
    ).toThrow();
  });
});
