// SPU3 byte encoding only. Not imported by the existing SPU2 request owner.
// The eventual fixed fetch hook must supply its actual final snapshot and the
// original initialization tuple; this helper is not a send/fit/Source grant.
import { createHash, createHmac } from 'node:crypto';

const names = ['accept', 'content-type', 'user-agent', 'x-stainless-arch', 'x-stainless-lang',
  'x-stainless-os', 'x-stainless-package-version', 'x-stainless-retry-count',
  'x-stainless-runtime', 'x-stainless-runtime-version', 'x-stainless-timeout'];
const deny = () => { throw new Error('pi_request_authentication_invalid'); };
const own = (value, key) => {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  if (!descriptor || !Object.hasOwn(descriptor, 'value')) deny();
  return descriptor.value;
};
const utf8 = (value, maximum) => {
  if (typeof value !== 'string' || value.length === 0 || value.length > maximum) deny();
  for (let i = 0; i < value.length; i++) {
    const c = value.charCodeAt(i);
    if (c >= 0xd800 && c <= 0xdbff) {
      const next = value.charCodeAt(++i); if (!(next >= 0xdc00 && next <= 0xdfff)) deny();
    } else if (c >= 0xdc00 && c <= 0xdfff) deny();
  }
  if (Buffer.byteLength(value) > maximum) deny();
};
const field = (h, value) => {
  const size = Buffer.alloc(4); size.writeUInt32BE(value.byteLength);
  h.update(size); h.update(value);
};

// Caller must fund all native copies before Issue; these bounded helpers do
// not start a fresh ledger or certify the complete SDK/native allocation fit.
export function encodeRequestAuthentication(nonce, binding, profile, endpoint, pairs, body, authorization, maximum) {
  if (![nonce, binding, profile].every((b) => b instanceof Uint8Array && b.byteLength === 32) ||
      typeof authorization !== 'string' || authorization.length < 8 || authorization.length > (64 << 10) + 7 ||
      !authorization.startsWith('Bearer ') || !Number.isSafeInteger(maximum) || maximum < 2 || maximum > 1 << 20 ||
      !Array.isArray(pairs) || pairs.length !== 11) deny();
  utf8(endpoint, 2048); utf8(body, 1 << 20);
  for (let i = 7; i < authorization.length; i++) {
    const c = authorization.charCodeAt(i); if (c < 33 || c > 126) deny();
  }
  let bytes = 2; const values = [];
  for (let i = 0; i < names.length; i++) {
    const pair = own(pairs, String(i));
    if (!Array.isArray(pair) || pair.length !== 2 || own(pair, '0') !== names[i]) deny();
    const value = own(pair, '1');
    if (typeof value !== 'string' || value.length === 0 || value.length > maximum) deny();
    if (value.charCodeAt(0) <= 32 || value.charCodeAt(value.length - 1) <= 32) deny();
    bytes += 6 + names[i].length + value.length;
    if (bytes > maximum) deny();
    for (let j = 0; j < value.length; j++) {
      const c = value.charCodeAt(j); if (c < 32 || c > 126) deny();
    }
    values.push(value); // snapshot once; never invoke getters/re-read aliases
  }
  const headers = Buffer.alloc(bytes); headers.writeUInt16BE(11);
  let at = 2;
  for (let i = 0; i < names.length; i++) {
    const name = names[i]; const value = values[i];
    headers.writeUInt16BE(name.length, at); at += 2;
    at += headers.write(name, at, 'ascii');
    headers.writeUInt32BE(value.length, at); at += 4;
    at += headers.write(value, at, 'ascii');
  }
  // The temporary key buffer is wiped even if hashing throws. The original
  // SDK authorization string's lifetime remains owned by the actual hook.
  const key = Buffer.from(authorization.slice(7), 'ascii');
  try {
    const h = createHmac('sha256', key); h.update('Sessionless/Pi/SPU3/Auth/1\0', 'ascii');
    field(h, nonce); field(h, binding); field(h, profile); field(h, Buffer.from('POST', 'ascii'));
    field(h, Buffer.from(endpoint, 'utf8'));
    field(h, createHash('sha256').update(headers).digest());
    field(h, createHash('sha256').update(body, 'utf8').digest());
    return { headers, mac: h.digest() }; // nonsecret comparison DATA only
  } finally { key.fill(0); }
}
