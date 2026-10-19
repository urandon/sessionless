import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { test } from 'node:test';

const script = fileURLToPath(new URL('./wait-ci-ydb.sh', import.meta.url));
const source = readFileSync(script, 'utf8');
const health = (status) => `["CMD-SHELL","sh /health_check"]|running|${status}`;

// All subprocesses are owned fakes; the test never contacts Docker or a DB.
function run(t, states, options = {}) {
  const root = mkdtempSync(join(tmpdir(), 'sessionless-ci-ydb-policy-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const events = join(root, 'events');
  const cursor = join(root, 'cursor');
  const executable = (name, value) => writeFileSync(join(root, name), value, { mode: 0o700 });
  writeFileSync(join(root, 'states'), states.join('\n') + '\n');
  executable('docker', `#!/usr/bin/env node
const fs=require('node:fs'); const a=process.argv.slice(2);
fs.appendFileSync(process.env.EVENTS,JSON.stringify(a)+'\\n');
if(a[0]==='logs'){process.exit(process.env.DIAGNOSTICS_FAIL==='1'?1:0)}
if(a[0]!=='inspect')process.exit(97);
if(a[2]==='{{json .State}}'){console.log('{}');process.exit(process.env.DIAGNOSTICS_FAIL==='1'?1:0)}
if(process.env.INSPECT_FAIL==='1')process.exit(1);
if(process.env.INSPECT_HANG==='1'){setInterval(()=>{},1000)}else{
const n=fs.existsSync(process.env.CURSOR)?Number(fs.readFileSync(process.env.CURSOR)):0;
fs.writeFileSync(process.env.CURSOR,String(n+1));
const s=fs.readFileSync(process.env.STATES,'utf8').trimEnd().split('\\n');
console.log(s[Math.min(n,s.length-1)]);
}
`);
  executable('timeout', `#!/usr/bin/env node
const{spawnSync}=require('node:child_process'); const a=process.argv.slice(2);
if(a.shift()!=='--signal=KILL')process.exit(96);
const d=a.shift(); if(!/^[1-5]s$/.test(d))process.exit(95);
const r=spawnSync(a.shift(),a,{stdio:'inherit',timeout:Number(d.slice(0,-1))*1000,killSignal:'SIGKILL'});
process.exit(r.error?124:(r.status??1));
`);
  executable('sleep', '#!/bin/sh\nexit 0\n');
  let candidate = script;
  if (options.source !== undefined) {
    candidate = join(root, 'candidate.sh');
    writeFileSync(candidate, options.source);
  }
  const result = spawnSync('bash', [candidate, options.name ?? 'sessionless-ydb'], {
    encoding: 'utf8', timeout: 15000,
    env: { ...process.env, PATH: `${root}:${process.env.PATH}`, EVENTS: events, CURSOR: cursor, STATES: join(root, 'states'),
      INSPECT_FAIL: options.inspectFail ? '1' : '0', INSPECT_HANG: options.inspectHang ? '1' : '0', DIAGNOSTICS_FAIL: options.diagnosticsFail ? '1' : '0' },
  });
  assert.equal(result.error, undefined, `bounded policy subprocess: ${result.error}`);
  const calls = (() => { try { return readFileSync(events, 'utf8').trim().split('\n').map(JSON.parse); } catch { return []; } })();
  for (const call of calls) assert.ok(['inspect', 'logs'].includes(call[0]), 'waiter performs no mutation or migration');
  return { ...result, calls };
}

test('starting is not ready; exact SQL/storage health permits completion', (t) => {
  const r = run(t, [health('starting'), health('healthy')]);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.calls.length, 2);
  assert.match(r.stdout, /SQL\/storage health is ready/);
});
test('both exact owned names are supported', (t) => {
  const r = run(t, [health('healthy')], { name: 'sessionless-security-ydb' });
  assert.equal(r.status, 0, r.stderr);
});
for (const value of [health('unhealthy'), health(''), health('unknown'), '["CMD-SHELL","curl monitoring"]|running|healthy', '["CMD-SHELL","sh /health_check"]|exited|healthy', 'null|running|', '']) {
  test(`reject nonready state ${JSON.stringify(value)}`, (t) => assert.notEqual(run(t, [value]).status, 0));
}
for (const name of ['other-project-ydb', '--all', 'sessionless-ydb;echo bad']) {
  test(`reject unowned name ${name} before Docker`, (t) => {
    const r = run(t, [health('healthy')], { name });
    assert.equal(r.status, 2);
    assert.deepEqual(r.calls, []);
  });
}
test('inspect failure and failed diagnostics cannot produce readiness', (t) => {
  const r = run(t, [health('healthy')], { inspectFail: true, diagnosticsFail: true });
  assert.equal(r.status, 1);
  assert.match(r.stderr, /inspect failed or exceeded/);
  assert.equal(r.calls.length, 3);
});
test('a hanging inspect has its own finite deadline', (t) => {
  const r = run(t, [health('healthy')], { inspectHang: true });
  assert.equal(r.status, 1);
  assert.match(r.stderr, /inspect failed or exceeded/);
});
test('expired original deadline rejects even healthy state before readiness inspect', (t) => {
  const before = 'started=$SECONDS';
  assert.ok(source.includes(before));
  const r = run(t, [health('healthy')], { source: source.replace(before, 'started=$((SECONDS - 90))') });
  assert.equal(r.status, 1);
  assert.match(r.stderr, /90-second readiness deadline exhausted/);
  assert.equal(r.calls.filter((a) => a[0] === 'inspect' && a[2] !== '{{json .State}}').length, 0);
  assert.doesNotMatch(r.stdout, /is ready/);
});
test('persistent starting exhausts the finite poll budget, never migrates', (t) => {
  const r = run(t, [health('starting')]);
  assert.equal(r.status, 1);
  assert.match(r.stderr, /readiness budget exhausted/);
  assert.equal(r.calls.filter((a) => a[2] !== '{{json .State}}' && a[0] === 'inspect').length, 90);
});
function verifyBounds(text) {
  assert.match(text, /attempt<=90/);
  assert.match(text, /90 - \(SECONDS - started\)/);
  assert.match(text, /remaining < call_seconds/);
  assert.match(text, /timeout --signal=KILL "\$\{call_seconds\}s" docker inspect/);
  assert.match(text, /timeout --signal=KILL 5s docker logs --timestamps --tail 50/);
  assert.doesNotMatch(text, /\b(?:curl|migrate-local|docker exec|docker start|docker restart)\b/);
}
test('source retains both total and per-call bounds and bounded diagnostics', () => verifyBounds(source));
for (const [name, before, after] of [
  ['poll bound removed', 'attempt<=90', 'attempt>=0'],
  ['original deadline removed', '90 - (SECONDS - started)', '90'],
  ['remaining budget ignored', 'remaining < call_seconds', 'remaining > call_seconds'],
  ['inspect timeout removed', 'timeout --signal=KILL "${call_seconds}s" docker inspect', 'docker inspect'],
  ['diagnostic timeout removed', 'timeout --signal=KILL 5s docker logs', 'docker logs'],
]) {
  test(`causal guard rejects ${name}`, () => {
    assert.ok(source.includes(before));
    assert.throws(() => verifyBounds(source.replace(before, after)));
  });
}
test('zero-poll mutant loses the required positive', (t) => {
  const r = run(t, [health('healthy')], { source: source.replace('attempt<=90', 'attempt<=0') });
  assert.notEqual(r.status, 0);
  assert.doesNotMatch(r.stdout, /is ready/);
});
test('permissive-health mutant is caught by the liveness negative', (t) => {
  const before = `'["CMD-SHELL","sh /health_check"]|running|healthy')`;
  assert.ok(source.includes(before));
  const r = run(t, ['["CMD-SHELL","curl monitoring"]|running|healthy'], { source: source.replace(before, `*'|healthy')`) });
  assert.equal(r.status, 0, 'isolated mutant must reach the deliberately weakened guard');
  // Baseline test above requires this SAME input to fail. Its deliberate
  // acceptance here demonstrates the negative actually detects that guard.
});
