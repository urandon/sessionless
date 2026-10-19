import type { components } from '../api/generated';

export type SearchConsentSummary = components['schemas']['SearchConsentSummaryV1'];
export type SearchConsentView = components['schemas']['SearchConsentViewV1'];
export type SearchConsentChoice = 'approve' | 'deny';

// Private discovery selectors, not an authorizer or a copy of the query.
export type SearchConsentDiscovery = components['schemas']['SearchConsentDiscoveryV1'];

export interface SearchConsentLocator {
  sessionId: string;
  runId: string;
  attemptId: string;
  requestId: string;
}

function requireValid(ok: unknown): asserts ok {
  if (!ok) throw new TypeError('Invalid search consent response.');
}

function closed(
  value: unknown,
  required: string[],
  optional: string[] = [],
): Record<string, unknown> {
  requireValid(typeof value === 'object' && value !== null && !Array.isArray(value));
  const record = value as Record<string, unknown>;
  requireValid(required.every((key) => Object.hasOwn(record, key)));
  requireValid(
    Object.keys(record).every((key) => required.includes(key) || optional.includes(key)),
  );
  return record;
}

export function validConsentSelector(value: unknown): value is string {
  return (
    typeof value === 'string' && value.length <= 160 && /^[A-Za-z0-9][A-Za-z0-9._:-]*$/.test(value)
  );
}

function instant(value: unknown): value is string {
  if (typeof value !== 'string') return false;
  const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.\d{1,9})?Z$/.exec(value);
  if (!match || match[1] === '0001-01-01T00:00:00') return false;
  const at = Date.parse(value);
  return Number.isFinite(at) && new Date(at).toISOString().slice(0, 19) === match[1];
}

export function validateConsentDiscovery(
  value: unknown,
  sessionId: string,
  runId: string,
): SearchConsentDiscovery {
  const v = closed(value, ['session_id', 'run_id'], ['pending']);
  requireValid(
    v.session_id === sessionId &&
      v.run_id === runId &&
      validConsentSelector(sessionId) &&
      validConsentSelector(runId),
  );
  if (Object.hasOwn(v, 'pending')) {
    const pending = closed(v.pending, ['attempt_id', 'request_id', 'expires_at']);
    requireValid(
      validConsentSelector(pending.attempt_id) &&
        validConsentSelector(pending.request_id) &&
        instant(pending.expires_at),
    );
  }
  return structuredClone(value) as SearchConsentDiscovery;
}

export function validateConsentSummary(
  value: unknown,
  requestId: string,
  withQuery = false,
): SearchConsentSummary | SearchConsentView {
  const v = closed(value, [
    'request_id',
    'revision',
    'status',
    'expires_at',
    ...(withQuery ? ['query'] : []),
  ]);
  requireValid(
    validConsentSelector(v.request_id) && v.request_id === requestId && instant(v.expires_at),
  );
  requireValid(
    typeof v.status === 'string' &&
      [
        'pending',
        'approved',
        'denied',
        'expired',
        'cancelled',
        'revoked',
        'reconcile_only',
      ].includes(v.status),
  );
  requireValid(
    v.status === 'pending'
      ? v.revision === 1
      : ['approved', 'denied'].includes(v.status as string)
        ? v.revision === 2
        : v.revision === 2 || v.revision === 3,
  );
  if (withQuery) {
    requireValid(typeof v.query === 'string' && v.query.trim().length > 0);
    const encoded = new TextEncoder().encode(v.query);
    requireValid(
      encoded.length <= 1024 &&
        new TextDecoder('utf-8', { fatal: true }).decode(encoded) === v.query,
    );
  }
  return structuredClone(value) as SearchConsentSummary | SearchConsentView;
}
