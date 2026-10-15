import assert from 'node:assert/strict';
import { encodeRequestAuthentication } from './authentication.mjs';

const pairs = [['accept', 'application/json'], ['content-type', 'application/json'], ['user-agent', 'fixture/1'],
  ['x-stainless-arch', 'arm64'], ['x-stainless-lang', 'js'], ['x-stainless-os', 'Linux'],
  ['x-stainless-package-version', '6.40.0'], ['x-stainless-retry-count', '0'],
  ['x-stainless-runtime', 'node'], ['x-stainless-runtime-version', 'v24.19.0'], ['x-stainless-timeout', '30']];
const args = () => [Uint8Array.from({ length: 32 }, (_, i) => i), new Uint8Array(32).fill(0x11),
  new Uint8Array(32).fill(0x22), 'https://api.example.invalid/v1/responses',
  pairs.map((p) => [...p]), '{"fixture":true}', 'Bearer fixture-only-api-key', 331];
const expected = '39dea11e34ea383bc5a3299aeba6577bc397e2737d0792df93ad0f67912a0e04';
const valid = encodeRequestAuthentication(...args());
assert.equal(valid.headers.byteLength, 331);
assert.equal(valid.mac.toString('hex'), expected);
// Every transcript input/key changes the MAC; byte-equivalent JSON semantics
// deliberately do NOT give byte-equivalent commitments.
for (const index of [0, 1, 2, 3, 4, 5, 6]) {
  const a = args();
  if (index < 3) a[index][0] ^= 1;
  else if (index === 4) a[4][2][1] = 'fixture/2';
  else if (index === 5) a[5] = '{ "fixture": true }';
  else a[index] += 'x';
  assert.notEqual(encodeRequestAuthentication(...a).mac.toString('hex'), expected, `field ${index}`);
}
for (const mutate of [
  (a) => { a[7]--; },
  (a) => { a[0] = new Uint8Array(31); },
  (a) => { a[1] = '11'.repeat(32); },
  (a) => { a[3] = 'x'.repeat(2049); },
  (a) => { a[3] = '\ud800'; },
  (a) => { a[5] = '\udc00'; },
  (a) => { a[5] = 'x'.repeat((1 << 20) + 1); },
  (a) => { [a[4][0], a[4][1]] = [a[4][1], a[4][0]]; },
  (a) => { a[4][0][0] = 'authorization'; },
  (a) => { a[4][0][1] += ' '; },
  (a) => { a[4][0][1] += '\r\nsecret'; },
  (a) => { a[4][0][1] = 'é'; },
  (a) => { a[6] = 'bearer fixture-only-api-key'; },
  (a) => { a[6] = 'Bearer '; },
  (a) => { a[6] = 'Bearer fixture key'; },
  (a) => { a[6] = 'Bearer fixture\x7f'; },
  (a) => { a[6] = `Bearer ${'x'.repeat((64 << 10) + 1)}`; },
  (a) => { Object.defineProperty(a[4][0], '1', { get() { throw new Error('getter executed'); } }); },
]) {
  const a = args(); mutate(a); assert.throws(() => encodeRequestAuthentication(...a), /pi_request_authentication_invalid/);
}
for (const mutate of [
  (a) => { a[3] = 'x'.repeat(2048); },
  (a) => { a[5] = 'x'.repeat(1 << 20); },
  (a) => { a[6] = `Bearer ${'x'.repeat(64 << 10)}`; },
]) { const a = args(); mutate(a); assert.equal(encodeRequestAuthentication(...a).mac.byteLength, 32); }
console.log('SPU3 authentication literal vector, seven inputs/key, header and bound negatives: PASS');
