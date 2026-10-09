// Bounded stdin-only checks; never reflect a URL/header/token in diagnostics.
import { readSync } from 'node:fs';
const limit = 64 * 1024;
const requireTrue = value => { if (!value) throw new Error('invalid smoke input'); };
const opaque = value => typeof value === 'string' && /^[A-Za-z0-9._~-]{1,160}$/.test(value);
function origin(raw, managed = false) {
  const u = new URL(raw);
  requireTrue(u.protocol === 'https:' && !u.username && !u.password && !u.search && !u.hash && u.pathname === '/' && u.origin === raw && (!managed || /^web\.dev\.(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/.test(u.hostname)));
  return u.origin;
}
function config() {
  const provider = process.env.WEB_LOGIN_PROVIDER;
  requireTrue(provider === 'yandex' || provider === 'telegram');
  const client = process.env[provider === 'yandex' ? 'YANDEX_LOGIN_CLIENT_ID' : 'TELEGRAM_OIDC_CLIENT_ID'];
  requireTrue(opaque(client));
  const web = origin(process.env.CLOUD_WEB_URL, true); origin(process.env.WEB_CONTAINER_URL);
  requireTrue(/^cr\.yandex\/[a-z0-9][a-z0-9_-]*\/web-bff@sha256:[0-9a-f]{64}$/.test(process.env.WEB_IMAGE_REF || ''));
  requireTrue(process.env.WEB_PREPARED_INSTANCES === '0' && /^[1-8]$/.test(process.env.WEB_CONCURRENCY || ''));
  requireTrue(/^(0|[1-9][0-9]{0,2})$/.test(process.env.WEB_COLD_START_WAIT_SECONDS || '0') && Number(process.env.WEB_COLD_START_WAIT_SECONDS || '0') <= 600);
  return { provider, client, web };
}
function stdin() {
  const data = Buffer.alloc(limit + 1); let size = 0;
  while (size <= limit) { const n = readSync(0, data, size, data.length - size, null); if (n === 0) break; size += n; }
  requireTrue(size <= limit); return data.subarray(0, size).toString('utf8');
}
function headers(text) {
  requireTrue(!/[\0\x01-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(text) && text.endsWith('\r\n\r\n'));
  const lines = text.slice(0, -4).split('\r\n');
  requireTrue(/^HTTP\/(?:1\.[01]|2|3) [0-9]{3}(?: [\x20-\x7e]*)?$/.test(lines[0]));
  const status = Number(lines.shift().split(' ')[1]); const fields = new Map();
  for (const line of lines) {
    const match = /^([!#$%&'*+.^_`|~0-9A-Za-z-]+):[ \t]*([^\r\n]*)$/.exec(line); requireTrue(match !== null);
    const key = match[1].toLowerCase(), values = fields.get(key) || []; values.push(match[2]); fields.set(key, values);
  }
  return { status, get(key) { const v = fields.get(key); requireTrue(v?.length === 1); return v[0]; } };
}
function login(text, selected) {
  const h = headers(text); requireTrue(h.status === 303 && h.get('cache-control') === 'no-store');
  const raw = h.get('location'); requireTrue(raw.length <= 8192 && !/\s/.test(raw) && !/%(?![0-9A-Fa-f]{2})/.test(raw)); const u = new URL(raw);
  requireTrue(u.origin === (selected.provider === 'yandex' ? 'https://oauth.yandex.ru' : 'https://oauth.telegram.org') && u.pathname === (selected.provider === 'yandex' ? '/authorize' : '/auth') && !u.username && !u.password && !u.hash && raw === u.href);
  const q = u.searchParams, seen = new Set(), allowed = new Set(['client_id','redirect_uri','response_type','scope','state','code_challenge','code_challenge_method',...(selected.provider === 'telegram' ? ['nonce'] : [])]);
  for (const key of q.keys()) { requireTrue(allowed.has(key) && !seen.has(key)); seen.add(key); }
  requireTrue(q.get('client_id') === selected.client && q.get('response_type') === 'code' && q.get('redirect_uri') === selected.web + (selected.provider === 'yandex' ? '/auth/login/callback' : '/auth/telegram/callback'));
  requireTrue(q.get('scope') === (selected.provider === 'yandex' ? 'login:info' : 'openid profile') && opaque(q.get('state')) && q.get('code_challenge_method') === 'S256');
  const c = q.get('code_challenge'); requireTrue(typeof c === 'string' && /^[A-Za-z0-9_-]{43}$/.test(c)); const b = Buffer.from(c, 'base64url'); requireTrue(b.length === 32 && b.toString('base64url') === c);
  if (selected.provider === 'telegram') requireTrue(opaque(q.get('nonce')));
}
try {
  const selected = config();
  switch (process.argv[2]) {
    case 'config': break;
    case 'login': login(stdin(), selected); break;
    case 'root': { const h = headers(stdin()); requireTrue(h.status === 200 && h.get('strict-transport-security') === 'max-age=31536000; includeSubDomains' && h.get('x-content-type-options') === 'nosniff' && h.get('referrer-policy') === 'strict-origin-when-cross-origin' && h.get('cache-control') === 'no-store'); requireTrue(h.get('content-security-policy').split(';').map(s => s.trim()).includes("frame-ancestors 'none'")); break; }
    case 'me': { const h = headers(stdin()); requireTrue(h.status === 401 && h.get('cache-control') === 'no-store'); break; }
    case 'version': requireTrue(JSON.parse(stdin()).component === 'web-bff'); break;
    case 'iam-config': { const token = stdin().replace(/\n$/, ''); requireTrue(/^[A-Za-z0-9._~-]{1,8192}$/.test(token)); process.stdout.write(`header = "Authorization: Bearer ${token}"\nfail\n`); break; }
    default: requireTrue(false);
  }
} catch { process.stderr.write('cloud Web smoke validation failed\n'); process.exitCode = 1; }
