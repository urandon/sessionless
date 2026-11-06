import type { components } from '../api/generated';

type Schemas = components['schemas'];
export type AccountLoginMethods = Schemas['AccountLoginMethods'];
export type AccountLoginMethod = Schemas['AccountLoginMethod'];
export type IdentityOperationView = Schemas['IdentityOperationView'];
export type IdentityOperationStart =
  | { kind: 'link'; target_provider: 'yandex' | 'telegram'; expected_revision: string }
  | { kind: 'unlink'; method_handle: string; expected_revision: string };
export type IdentityProvider = AccountLoginMethod['provider'];
export type IdentityOperationContinuation = Schemas['IdentityOperationContinuation'];

export const identityBodyBytes = 8192;
export const identityContinuationBytes = 12 * 1024;
export const providerLabel: Record<IdentityProvider, string> = {
  yandex: 'Yandex',
  telegram: 'Telegram',
};

function valid(ok: unknown): asserts ok {
  if (!ok) throw new TypeError('Invalid login-method response.');
}

export function isIdentityOperationID(value: unknown): value is string {
  return (
    typeof value === 'string' && value.length <= 160 && /^[A-Za-z0-9][A-Za-z0-9._:-]*$/.test(value)
  );
}

export function isIdentityRevision(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    /^[1-9][0-9]{0,19}$/.test(value) &&
    BigInt(value) <= 18446744073709551615n
  );
}

function provider(value: unknown): value is IdentityProvider {
  return value === 'yandex' || value === 'telegram';
}

function closed(
  value: unknown,
  required: string[],
  optional: string[] = [],
): Record<string, unknown> {
  valid(value !== null && typeof value === 'object' && !Array.isArray(value));
  const record = value as Record<string, unknown>;
  valid(
    required.every((key) => Object.hasOwn(record, key)) &&
      Object.keys(record).every((key) => required.includes(key) || optional.includes(key)),
  );
  return record;
}

// These bounded DTOs have no free-text fields. Count every complete property
// token against the decoded tree, so duplicate or escaped-alias keys cannot
// disappear through JSON.parse. No subjects, private metadata or raw errors fit.
function document(text: string, limit = identityBodyBytes): unknown {
  valid(new TextEncoder().encode(text).length <= limit);
  const value: unknown = JSON.parse(text);
  const expected = new Map<string, number>();
  function visit(node: unknown, depth: number): void {
    valid(depth <= 4);
    if (!node || typeof node !== 'object') return;
    if (Array.isArray(node)) {
      for (const item of node) visit(item, depth + 1);
      return;
    }
    for (const [key, item] of Object.entries(node)) {
      expected.set(key, (expected.get(key) ?? 0) + 1);
      visit(item, depth + 1);
    }
  }
  visit(value, 0);
  const observed = new Map<string, number>();
  for (const match of text.matchAll(/"(?:\\.|[^"\\])*"\s*:?/g)) {
    const token = match[0].trimEnd();
    if (!token.endsWith(':')) continue;
    const rawKey = token.slice(0, -1).trimEnd();
    const key: unknown = JSON.parse(rawKey);
    valid(typeof key === 'string' && rawKey === JSON.stringify(key));
    observed.set(key, (observed.get(key) ?? 0) + 1);
  }
  valid(
    expected.size === observed.size &&
      [...expected].every(([key, count]) => observed.get(key) === count),
  );
  return value;
}

function instant(value: unknown): bigint {
  valid(typeof value === 'string');
  const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z$/.exec(value);
  valid(match);
  const seconds = Date.parse(`${match[1]}Z`);
  valid(Number.isFinite(seconds) && new Date(seconds).toISOString().slice(0, 19) === match[1]);
  const nanos = BigInt((match[2] ?? '').padEnd(9, '0'));
  valid(match[1] !== '0001-01-01T00:00:00' || nanos !== 0n);
  return BigInt(seconds) * 1000000n + nanos;
}

export function parseAccountLoginMethods(text: string): AccountLoginMethods {
  const raw = document(text);
  const value = closed(raw, ['revision', 'methods']);
  valid(
    isIdentityRevision(value.revision) &&
      Array.isArray(value.methods) &&
      value.methods.length === 2,
  );
  const methods = value.methods.map((item) => {
    const row = closed(
      item,
      ['provider', 'linked', 'current', 'enabled', 'allowed_actions'],
      ['method_handle', 'remaining_provider'],
    );
    valid(
      provider(row.provider) &&
        typeof row.linked === 'boolean' &&
        typeof row.current === 'boolean' &&
        typeof row.enabled === 'boolean',
    );
    valid(!row.current || row.linked);
    valid(row.linked ? isIdentityOperationID(row.method_handle) : row.method_handle === undefined);
    valid(Array.isArray(row.allowed_actions) && row.allowed_actions.length <= 1);
    const action: unknown = row.allowed_actions[0];
    if (action === 'link')
      valid(!row.linked && row.enabled && row.remaining_provider === undefined);
    else if (action === 'unlink')
      valid(
        row.linked && provider(row.remaining_provider) && row.remaining_provider !== row.provider,
      );
    else valid(action === undefined && row.remaining_provider === undefined);
    return row;
  });
  valid(methods[0]?.provider === 'yandex' && methods[1]?.provider === 'telegram');
  valid(methods.filter((row) => row.current).length === 1);
  for (const row of methods)
    if (row.remaining_provider !== undefined)
      valid(
        methods.some(
          (other) => other.provider === row.remaining_provider && other.linked && other.enabled,
        ),
      );
  return raw as AccountLoginMethods;
}

const outcomes = [
  'linked',
  'unlinked',
  'cancelled',
  'conflict',
  'stale_revision',
  'proof_required',
  'proof_unavailable',
  'last_method',
  'provider_unavailable',
  'expired',
  'access_denied',
];

export function parseIdentityOperation(
  text: string,
  expectedID?: string,
  gone = false,
): IdentityOperationView {
  const raw = document(text);
  const value = closed(
    raw,
    ['operation_id', 'allowed_actions'],
    [
      'kind',
      'phase',
      'target_provider',
      'outcome',
      'expires_at',
      'before_revision',
      'after_revision',
      'completed_at',
    ],
  );
  valid(
    isIdentityOperationID(value.operation_id) &&
      (expectedID === undefined || value.operation_id === expectedID),
  );
  valid(
    Array.isArray(value.allowed_actions) &&
      value.allowed_actions.length <= 2 &&
      new Set(value.allowed_actions).size === value.allowed_actions.length,
  );
  if (value.outcome === 'outcome_unavailable') {
    closed(value, ['operation_id', 'outcome', 'allowed_actions']);
    valid(gone && value.allowed_actions.length === 0);
    return raw as IdentityOperationView;
  }
  valid(
    !gone &&
      (value.kind === 'link' || value.kind === 'unlink') &&
      isIdentityRevision(value.before_revision),
  );
  const expires = instant(value.expires_at);
  if (value.phase === 'terminal') {
    valid(
      value.target_provider === undefined &&
        value.allowed_actions.length === 0 &&
        typeof value.outcome === 'string' &&
        outcomes.includes(value.outcome),
    );
    const completed = instant(value.completed_at);
    if (value.outcome === 'linked' || value.outcome === 'unlinked') {
      valid(
        value.outcome === (value.kind === 'link' ? 'linked' : 'unlinked') &&
          isIdentityRevision(value.after_revision) &&
          BigInt(value.after_revision) === BigInt(value.before_revision) + 1n &&
          completed < expires,
      );
    } else valid(value.after_revision === undefined);
  } else {
    valid(
      ['source_proof', 'target_proof', 'confirm'].includes(value.phase as string) &&
        provider(value.target_provider) &&
        value.completed_at === undefined &&
        value.after_revision === undefined,
    );
    valid(value.kind !== 'unlink' || value.phase !== 'target_proof');
    valid(value.kind !== 'link' || value.phase !== 'source_proof');
    valid(value.outcome === undefined || value.outcome === 'expired');
    for (const action of value.allowed_actions)
      valid(action === 'cancel' || action === (value.phase === 'confirm' ? 'confirm' : 'continue'));
    if (value.outcome === 'expired') valid(value.allowed_actions.length === 0);
  }
  return raw as IdentityOperationView;
}

export function validateIdentityStart(value: IdentityOperationStart): void {
  valid(isIdentityRevision(value.expected_revision));
  if (value.kind === 'link') {
    closed(value, ['kind', 'target_provider', 'expected_revision']);
    valid(provider(value.target_provider));
  } else {
    closed(value, ['kind', 'method_handle', 'expected_revision']);
    valid(value.kind === 'unlink' && isIdentityOperationID(value.method_handle));
  }
}

// Validation is presentation-only, not proof of a configured or healthy
// provider. The server still owns client/state/PKCE/current-scope verification.
export function validateIdentityAuthorizationURL(raw: unknown, origin: string): string {
  valid(typeof raw === 'string' && raw.length <= 8192);
  valid(
    !Array.from(raw).some((character) => {
      const point = character.codePointAt(0)!;
      return point <= 0x20 || point === 0x7f || character === '\\';
    }),
  );
  const url = new URL(raw);
  const selected: IdentityProvider =
    url.origin === 'https://oauth.yandex.ru' && url.pathname === '/authorize'
      ? 'yandex'
      : 'telegram';
  valid(
    url.username === '' &&
      url.password === '' &&
      url.hash === '' &&
      url.port === '' &&
      (selected === 'yandex' ||
        (url.origin === 'https://oauth.telegram.org' && url.pathname === '/auth')),
  );
  valid(
    raw.startsWith(
      selected === 'yandex'
        ? 'https://oauth.yandex.ru/authorize?'
        : 'https://oauth.telegram.org/auth?',
    ),
  );
  const keys = [
    'client_id',
    'redirect_uri',
    'response_type',
    'state',
    'code_challenge',
    'code_challenge_method',
    'scope',
    ...(selected === 'yandex' ? ['force_confirm'] : ['max_age', 'nonce']),
  ];
  valid(
    [...url.searchParams.keys()].length === keys.length &&
      keys.every((key) => url.searchParams.getAll(key).length === 1 && !!url.searchParams.get(key)),
  );
  valid(
    [...url.searchParams.keys()].every((key) => keys.includes(key)) &&
      url.searchParams.get('response_type') === 'code' &&
      url.searchParams.get('code_challenge_method') === 'S256',
  );
  const local = new URL(origin);
  valid(local.origin === origin && ['http:', 'https:'].includes(local.protocol));
  valid(
    url.searchParams.get('redirect_uri') ===
      `${origin}${selected === 'yandex' ? '/auth/login/callback' : '/auth/telegram/callback'}`,
  );
  valid(
    isIdentityOperationID(url.searchParams.get('state')) &&
      /^[A-Za-z0-9_-]{43}$/.test(url.searchParams.get('code_challenge') ?? '') &&
      (url.searchParams.get('client_id')?.length ?? 0) <= 160,
  );
  valid(
    selected === 'yandex'
      ? url.searchParams.get('scope') === 'login:info' &&
          url.searchParams.get('force_confirm') === 'yes'
      : url.searchParams.get('scope') === 'openid profile' &&
          url.searchParams.get('max_age') === '0' &&
          isIdentityOperationID(url.searchParams.get('nonce')),
  );
  return raw;
}

export function parseIdentityContinuation(
  text: string,
  origin: string,
): IdentityOperationContinuation {
  const raw = document(text, identityContinuationBytes);
  const value = closed(raw, ['authorization_url']);
  validateIdentityAuthorizationURL(value.authorization_url, origin);
  return raw as IdentityOperationContinuation;
}
