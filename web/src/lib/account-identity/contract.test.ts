import { describe, expect, it } from 'vitest';
import {
  isIdentityOperationID,
  isIdentityRevision,
  parseAccountLoginMethods,
  parseIdentityContinuation,
  parseIdentityOperation,
  validateIdentityStart,
  type IdentityOperationStart,
} from './contract';

function methods(revision = '9007199254740993') {
  return {
    revision,
    methods: [
      {
        provider: 'yandex',
        linked: true,
        current: true,
        enabled: true,
        method_handle: 'own-yandex',
        allowed_actions: [],
      },
      {
        provider: 'telegram',
        linked: false,
        current: false,
        enabled: true,
        allowed_actions: ['link'],
      },
    ],
  };
}
function pending() {
  return {
    operation_id: 'opaque:operation-1',
    kind: 'link',
    phase: 'source_proof',
    target_provider: 'telegram',
    before_revision: '9007199254740993',
    expires_at: '2026-11-04T12:10:00Z',
    allowed_actions: ['continue', 'cancel'],
  };
}
function authorization(provider = 'telegram', origin = 'https://account.example.com') {
  const url = new URL(
    provider === 'telegram'
      ? 'https://oauth.telegram.org/auth'
      : 'https://oauth.yandex.ru/authorize',
  );
  url.search = new URLSearchParams({
    client_id: '100000',
    redirect_uri: `${origin}${provider === 'telegram' ? '/auth/telegram/callback' : '/auth/login/callback'}`,
    response_type: 'code',
    state: 'bounded-state',
    code_challenge: 'A'.repeat(43),
    code_challenge_method: 'S256',
    scope: provider === 'telegram' ? 'openid profile' : 'login:info',
    ...(provider === 'telegram'
      ? { max_age: '0', nonce: 'bounded-nonce' }
      : { force_confirm: 'yes' }),
  }).toString();
  return url;
}

describe('account identity presentation contract', () => {
  it('rejects every raw ASCII space/control, DEL and backslash in continuation URLs without a regex exception', () => {
    const raw = authorization().href;
    for (const point of [...Array.from({ length: 33 }, (_, index) => index), 0x7f, 0x5c]) {
      expect(() =>
        parseIdentityContinuation(
          JSON.stringify({ authorization_url: raw + String.fromCodePoint(point) }),
          'https://account.example.com',
        ),
      ).toThrow();
    }
  });
  it('keeps all positive uint64 revisions as exact strings and opaque selectors independent of internal prefixes', () => {
    for (const revision of [
      '1',
      '9007199254740991',
      '9007199254740992',
      '9007199254740993',
      '18446744073709551615',
    ]) {
      expect(isIdentityRevision(revision)).toBe(true);
      expect(parseAccountLoginMethods(JSON.stringify(methods(revision))).revision).toBe(revision);
    }
    for (const bad of [
      0,
      1,
      9007199254740992,
      '0',
      '01',
      '+1',
      '1e3',
      '1.0',
      '18446744073709551616',
      '',
      null,
    ])
      expect(isIdentityRevision(bad)).toBe(false);
    for (const id of ['operation-1', 'opaque:receipt_2', 'A'.repeat(160)])
      expect(isIdentityOperationID(id)).toBe(true);
    for (const id of ['', '/foreign', '../other', 'a?token=x', 'a\n', 'a'.repeat(161), 1])
      expect(isIdentityOperationID(id)).toBe(false);
  });
  it('accepts only exactly two known ordered own rows, including linked-but-disabled, and never invents actions', () => {
    const value = methods();
    value.methods[1] = {
      provider: 'telegram',
      linked: true,
      current: false,
      enabled: false,
      method_handle: 'own-telegram',
      allowed_actions: [],
    };
    const parsed = parseAccountLoginMethods(JSON.stringify(value));
    expect(parsed.methods[1]).toMatchObject({ linked: true, enabled: false, allowed_actions: [] });
    const linked = {
      revision: '7',
      methods: [
        {
          provider: 'yandex',
          linked: true,
          current: true,
          enabled: true,
          method_handle: 'own-yandex',
          allowed_actions: [],
        },
        {
          provider: 'telegram',
          linked: true,
          current: false,
          enabled: false,
          method_handle: 'own-telegram',
          remaining_provider: 'yandex',
          allowed_actions: ['unlink'],
        },
      ],
    };
    expect(parseAccountLoginMethods(JSON.stringify(linked)).methods[1]?.remaining_provider).toBe(
      'yandex',
    );
    for (const mutate of [
      (v: ReturnType<typeof methods>) => {
        v.methods.reverse();
      },
      (v: ReturnType<typeof methods>) => {
        v.methods[1]!.provider = 'yandex';
      },
      (v: ReturnType<typeof methods>) => {
        v.methods.pop();
      },
      (v: ReturnType<typeof methods>) => {
        v.methods[0]!.current = false;
      },
      (v: ReturnType<typeof methods>) => {
        v.methods[1]!.enabled = false;
      },
      (v: ReturnType<typeof methods>) => {
        v.methods[0]!.allowed_actions = ['unlink'];
      },
    ]) {
      const bad = methods();
      mutate(bad);
      expect(() => parseAccountLoginMethods(JSON.stringify(bad))).toThrow();
    }
    const raw = JSON.stringify(methods());
    for (const bad of [
      raw.replace('"revision":', '"revision":"7","revision":'),
      raw.replace('"provider":', '"provider":"telegram","provider":'),
      raw.replace('"revision"', '"revis\\u0069on"'),
      raw.replace('"methods":', '"private_subject":"123","methods":'),
      raw + ' true',
      ' '.repeat(8193) + raw,
    ])
      expect(() => parseAccountLoginMethods(bad)).toThrow();
  });
  it('distinguishes pending expiry, pruned terminal receipts and the legitimate minimal 410 view', () => {
    const current = pending();
    expect(
      parseIdentityOperation(JSON.stringify(current), current.operation_id).before_revision,
    ).toBe(current.before_revision);
    const expired = { ...current, outcome: 'expired', allowed_actions: [] };
    expect(parseIdentityOperation(JSON.stringify(expired)).phase).toBe('source_proof');
    const terminal = {
      operation_id: current.operation_id,
      kind: 'link',
      phase: 'terminal',
      outcome: 'linked',
      before_revision: current.before_revision,
      after_revision: '9007199254740994',
      expires_at: current.expires_at,
      completed_at: '2026-11-04T12:01:00.000000001Z',
      allowed_actions: [],
    };
    expect(parseIdentityOperation(JSON.stringify(terminal)).target_provider).toBeUndefined();
    expect(() =>
      parseIdentityOperation(JSON.stringify({ ...terminal, target_provider: 'telegram' })),
    ).toThrow();
    expect(() =>
      parseIdentityOperation(JSON.stringify({ ...terminal, after_revision: '9007199254740995' })),
    ).toThrow();
    expect(() =>
      parseIdentityOperation(JSON.stringify({ ...terminal, completed_at: terminal.expires_at })),
    ).toThrow();
    const missing = {
      operation_id: current.operation_id,
      outcome: 'outcome_unavailable',
      allowed_actions: [],
    };
    expect(parseIdentityOperation(JSON.stringify(missing), current.operation_id, true)).toEqual(
      missing,
    );
    expect(() => parseIdentityOperation(JSON.stringify(missing), current.operation_id)).toThrow();
    expect(() =>
      parseIdentityOperation(
        JSON.stringify({ ...missing, phase: 'terminal' }),
        current.operation_id,
        true,
      ),
    ).toThrow();
    for (const bad of [
      { ...current, operation_id: 'foreign' },
      { ...current, allowed_actions: ['confirm'] },
      { ...current, allowed_actions: ['continue', 'continue'] },
      { ...current, outcome: 'expired' },
      { ...current, kind: 'unlink', phase: 'target_proof' },
      { ...current, completed_at: '2026-11-04T12:01:00Z' },
      { ...current, raw_subject: 'private' },
      { ...current, before_revision: 9007199254740992 },
    ])
      expect(() => parseIdentityOperation(JSON.stringify(bad), current.operation_id)).toThrow();
  });
  it('admits only the fixed HTTPS provider endpoints and exact local callbacks without storing URLs', () => {
    for (const selected of ['yandex', 'telegram']) {
      const url = authorization(selected);
      expect(
        parseIdentityContinuation(
          JSON.stringify({ authorization_url: url.href }),
          'https://account.example.com',
        ).authorization_url,
      ).toBe(url.href);
      for (const mutate of [
        (u: URL) => {
          u.hostname = 'outside.example.com';
        },
        (u: URL) => {
          u.protocol = 'http:';
        },
        (u: URL) => {
          u.username = 'user';
        },
        (u: URL) => {
          u.hash = '#code';
        },
        (u: URL) => {
          u.pathname = '/token';
        },
        (u: URL) => {
          u.searchParams.set('redirect_uri', 'https://other.example.com/auth/login/callback');
        },
        (u: URL) => {
          u.searchParams.set(
            'redirect_uri',
            'https://account.example.com/auth/login/callback?code=secret',
          );
        },
        (u: URL) => {
          u.searchParams.append('client_id', 'other');
        },
        (u: URL) => {
          u.searchParams.set('code', 'secret');
        },
        (u: URL) => {
          u.searchParams.delete(selected === 'telegram' ? 'max_age' : 'force_confirm');
        },
      ]) {
        const bad = new URL(url);
        mutate(bad);
        expect(() =>
          parseIdentityContinuation(
            JSON.stringify({ authorization_url: bad.href }),
            'https://account.example.com',
          ),
        ).toThrow();
      }
    }
    expect(() =>
      parseIdentityContinuation(
        '{"authorization_url":"https://outside.example.com","authorization_url":"https://oauth.telegram.org/auth"}',
        'https://account.example.com',
      ),
    ).toThrow();
  });
  it('rejects forbidden empty selectors rather than inferring a legacy request', () => {
    validateIdentityStart({ kind: 'link', target_provider: 'telegram', expected_revision: '7' });
    validateIdentityStart({ kind: 'unlink', method_handle: 'own-yandex', expected_revision: '7' });
    for (const value of [
      { kind: 'link', target_provider: 'telegram', method_handle: '', expected_revision: '7' },
      { kind: 'unlink', target_provider: '', method_handle: 'own', expected_revision: '7' },
      { kind: 'link', target_provider: 'other', expected_revision: '7' },
      { kind: 'link', target_provider: 'telegram', expected_revision: 7 },
    ])
      expect(() => validateIdentityStart(value as IdentityOperationStart)).toThrow();
  });
});
