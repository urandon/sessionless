import assert from 'node:assert/strict';
import { Buffer } from 'node:buffer';
import { PassThrough, Writable } from 'node:stream';
import { test } from 'node:test';
import { createPiStdioChannel } from './stdio.mjs';
import { createPhysicalFetchOwner } from './request.mjs';

// No SDK, files, network, credentials or real Source grant. Exact manual BE
// frames are an independent wire oracle, not the production encoder.
const limits = Object.freeze({ initialize_body_bytes: 512, request_body_bytes: 1024,
  final_body_bytes: 512, frame_bytes: 2048 });
const P = 'a'.repeat(64);
const bytes = text => Buffer.from(text, 'utf8');
function frame(kind, body = Buffer.alloc(0), meta = Buffer.alloc(0)) {
  const h = Buffer.alloc(17); h.write('SPU2'); h[4] = kind;
  h.writeUInt32BE(meta.length, 5); h.writeBigUInt64BE(BigInt(body.length), 9);
  return Buffer.concat([h, meta, body]);
}
function use(n = 3600000000000n, status = 1) {
  const meta = Buffer.alloc(9); meta[0] = status; meta.writeBigUInt64BE(n, 1);
  return frame(3, Buffer.alloc(0), meta);
}
function gate() { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; }
function fixture(t, { caps = limits, onWrite, holdDestroyCallbacks = false, start = 100000000000000000000n } = {}) {
  const input = new PassThrough({ highWaterMark: 16384 });
  const chunks = [];
  const callbacks = new Set();
  const output = new Writable({ highWaterMark: 16, write(chunk, _encoding, done) {
    chunks.push(Buffer.from(chunk));
    const settle = error => { callbacks.delete(settle); done(error); };
    callbacks.add(settle);
    if (onWrite) onWrite(chunk, settle); else settle();
  }, destroy(_error, done) {
    if (!holdDestroyCallbacks) for (const finish of [...callbacks]) finish(new Error('synthetic closed writer'));
    done();
  } });
  const original = new AbortController(); let tick = start;
  const timers = new Map(); let timerID = 0;
  const clock = Object.freeze({ now: () => tick,
    schedule(fn, ms) { const id = ++timerID; timers.set(id, { fn, ms }); return id; },
    cancel(id) { timers.delete(id); } });
  const channel = createPiStdioChannel({ input, output, limits: caps, signal: original.signal, clock });
  t.after(async () => { await channel.dispose(); assert.equal(timers.size, 0, 'owned timers remain');
    assert.equal(callbacks.size, 0, 'owned write callback remains');
    for (const stream of [input, output]) for (const event of ['error', 'close', 'readable', 'end'])
      assert.equal(stream.listenerCount(event), 0, `owned ${event} listener remains`); });
  return { channel, input, output, original, chunks, timers,
    setTick(n) { tick = n; }, advance(n) { tick += n; }, get tick() { return tick; } };
}
async function initialized(f, initial = bytes('{"opaque":"original Init DATA"}')) {
  const pending = f.channel.initialize();
  f.input.write(frame(1, initial));
  assert.deepEqual(await pending, initial);
  return f.channel.bindRequest('{"whole":"original 💬 body"}', P);
}
async function granted(f, n) {
  const parentUse = await initialized(f);
  const pending = parentUse.authorizePhysicalSend();
  f.input.write(use(n));
  return { parentUse, capability: await pending };
}

test('legal one-byte IO, exact full request/P, use once, final only, Init survives Use until clear', { timeout: 10000 }, async t => {
  const f = fixture(t); const init = bytes('{"opaque":"original 💬 Init"}');
  const pending = f.channel.initialize();
  for (const b of frame(1, init)) f.input.write(Buffer.from([b]));
  const borrowed = await pending; assert.deepEqual(borrowed, init);
  const request = '{"whole":"original 💬 body"}', parentUse = f.channel.bindRequest(request, P);
  assert.equal(Object.isFrozen(parentUse), true);
  const result = parentUse.authorizePhysicalSend();
  for (const b of use()) f.input.write(Buffer.from([b]));
  const cap = await result; assert.equal(Object.isFrozen(cap), true);
  assert.deepEqual(Object.keys(cap), ['signal', 'assertUsable']);
  assert.equal(cap.assertUsable(), undefined); assert.deepEqual(borrowed, init);
  const meta = bytes(`{"version":1,"phase":"physical_send","backend_profile_digest":"${P}"}`);
  assert.equal(meta.length, 129);
  assert.deepEqual(Buffer.concat(f.chunks), frame(2, bytes(request), meta));
  const final = '{"answer":"provider exact 💬 bytes","artifacts":[]}';
  await f.channel.publishFinal(final);
  assert.deepEqual(Buffer.concat(f.chunks), Buffer.concat([frame(2, bytes(request), meta), frame(4, bytes(final))]));
  await f.channel.dispose(); assert.ok(borrowed.every(b => b === 0)); assert.equal(cap.signal.aborted, true);
});

test('full IPC elapsed including blocked FIRST write, not receipt-now+N or fresh assertion budget', { timeout: 10000 }, async t => {
  const written = gate(), release = gate(); let calls = 0;
  const f = fixture(t, { onWrite(_chunk, done) { if (++calls === 1) { written.resolve(); release.promise.then(() => done()); } else done(); } });
  const parentUse = await initialized(f); const s = f.tick;
  const pending = parentUse.authorizePhysicalSend(); await written.promise;
  f.advance(600000000000n); // ten logical minutes blocked before first write finishes
  release.resolve(); f.input.write(use(3600000000000n));
  const cap = await pending; assert.equal(cap.assertUsable(), undefined);
  f.setTick(s + 3600000000000n - 1n); assert.equal(cap.assertUsable(), undefined);
  f.advance(1n); assert.throws(() => cap.assertUsable(), { message: 'pi_stdio_denied' });
  assert.equal(cap.signal.aborted, true);
});

test('exact BigInt maximum N, unequal epochs, bounded timer milliseconds and no overflow', { timeout: 10000 }, async t => {
  const f = fixture(t); const n = (1n << 63n) - 1n;
  const { capability } = await granted(f, n); assert.equal(capability.assertUsable(), undefined);
  assert.equal([...f.timers.values()][0].ms, 2147483647);
  f.advance(n); assert.throws(() => capability.assertUsable());
});

for (const [name, n, status] of [
  ['denied', 0n, 0], ['invalid-denial', 1n, 0], ['success-zero', 0n, 1],
  ['unknown-status', 1n, 2], ['max-first-over', 1n << 63n, 1], ['uint64-max', (1n << 64n) - 1n, 1],
]) test(`Use ${name} permanently fails`, { timeout: 10000 }, async t => {
  const f = fixture(t), parentUse = await initialized(f); const pending = parentUse.authorizePhysicalSend();
  f.input.write(use(n, status)); await assert.rejects(pending, { message: 'pi_stdio_denied' });
  await assert.rejects(parentUse.authorizePhysicalSend()); assert.equal(f.channel.signal.aborted, true);
});

for (const mode of ['equal', 'exhausted', 'backwards', 'not-bigint']) {
  test(`exchange clock ${mode} denies without grant`, { timeout: 10000 }, async t => {
    const f = fixture(t), parentUse = await initialized(f), s = f.tick;
    const pending = parentUse.authorizePhysicalSend();
    f.setTick(mode === 'not-bigint' ? 1 : mode === 'backwards' ? s - 1n : s + (mode === 'equal' ? 100n : 101n));
    f.input.write(use(100n)); await assert.rejects(pending); assert.equal(f.channel.signal.aborted, true);
  });
}

test('Use magic with high bits never grants a capability', { timeout: 10000 }, async t => {
  const f = fixture(t), owner = await initialized(f); const pending = owner.authorizePhysicalSend();
  const raw = use(); for (let i = 0; i < 4; i++) raw[i] |= 128;
  f.input.write(raw); await assert.rejects(pending); assert.equal(f.channel.signal.aborted, true);
});

test('zero/malformed backend P denied before any wire write, matching canonical Go codec', { timeout: 10000 }, async t => {
  for (const p of ['0'.repeat(64), 'A'.repeat(64), 'a'.repeat(63), 'a'.repeat(65)]) {
    const f = fixture(t); const initial = f.channel.initialize(); f.input.write(frame(1, bytes('x'))); await initial;
    assert.throws(() => f.channel.bindRequest('full body', p)); assert.equal(f.chunks.length, 0);
  }
});

for (const kind of [0, 2, 3, 4, 5, 6]) test(`initial wrong kind ${kind} never reads declared body`, { timeout: 10000 }, async t => {
  const f = fixture(t); const h = frame(kind).subarray(0, 17); h.writeBigUInt64BE((1n << 64n) - 1n, 9);
  const pending = f.channel.initialize(); f.input.write(h);
  await assert.rejects(pending); assert.equal(f.chunks.length, 0);
});

for (const mode of ['empty', 'first-over', 'bad-magic', 'high-bit-magic', 'metadata', 'utf8', 'truncated-header', 'truncated-body']) {
  test(`Init ${mode} denies`, { timeout: 10000 }, async t => {
    const f = fixture(t); let raw = frame(1, bytes('x'));
    if (mode === 'empty') raw = frame(1);
    if (mode === 'first-over') { raw = raw.subarray(0, 17); raw.writeBigUInt64BE(513n, 9); }
    if (mode === 'bad-magic') raw.write('SPU1');
    if (mode === 'high-bit-magic') for (let i = 0; i < 4; i++) raw[i] |= 128;
    if (mode === 'metadata') raw.writeUInt32BE(1, 5);
    if (mode === 'utf8') raw = frame(1, Buffer.from([0xff]));
    if (mode === 'truncated-header') raw = raw.subarray(0, 16);
    if (mode === 'truncated-body') raw = raw.subarray(0, 17);
    const pending = f.channel.initialize(); f.input.end(raw); await assert.rejects(pending);
    assert.equal(f.chunks.length, 0);
  });
}

test('exact maximum Init/request/final, first-over rejected without extra output', { timeout: 10000 }, async t => {
  const caps = Object.freeze({ initialize_body_bytes: 1048559, request_body_bytes: 1048430,
    final_body_bytes: 1048559, frame_bytes: 1 << 20 });
  const f = fixture(t, { caps }); const pending = f.channel.initialize();
  f.input.write(frame(1, Buffer.alloc(caps.initialize_body_bytes, 120))); await pending;
  const body = 'x'.repeat(caps.request_body_bytes), owner = f.channel.bindRequest(body, P);
  const auth = owner.authorizePhysicalSend(); f.input.write(use()); await auth;
  assert.equal(Buffer.concat(f.chunks).length, 1 << 20);
  await assert.rejects(f.channel.publishFinal('x'.repeat(caps.final_body_bytes + 1)));
  assert.equal(Buffer.concat(f.chunks).length, 1 << 20);
  const g = fixture(t, { caps }); const init2 = g.channel.initialize(); g.input.write(frame(1, bytes('x'))); await init2;
  assert.throws(() => g.channel.bindRequest('x'.repeat(caps.request_body_bytes + 1), P));
  assert.equal(g.chunks.length, 0);
  const h = fixture(t, { caps }); const init3 = h.channel.initialize(); h.input.write(frame(1, bytes('x'))); await init3;
  const auth3 = h.channel.bindRequest('x', P).authorizePhysicalSend(); h.input.write(use()); await auth3;
  await h.channel.publishFinal('x'.repeat(caps.final_body_bytes));
  assert.equal(Buffer.concat(h.chunks).subarray(-caps.frame_bytes)[4], 4);
});

for (const mode of ['retire', 'duplicate-use', 'wrong-direction', 'eof']) {
  test(`after Use ${mode} aborts same use signal`, { timeout: 10000 }, async t => {
    const f = fixture(t), { capability } = await granted(f);
    const revoked = gate(); capability.signal.addEventListener('abort', revoked.resolve, { once: true });
    if (mode === 'eof') f.input.end();
    else f.input.write(mode === 'retire' ? frame(5) : mode === 'duplicate-use' ? use() : frame(4, bytes('x')));
    await revoked.promise; assert.equal(capability.signal.aborted, true);
    assert.throws(() => capability.assertUsable()); await assert.rejects(f.channel.publishFinal('x'));
  });
}

test('cancel held reply joins exchange, clears Init and denies late grant', { timeout: 10000 }, async t => {
  const f = fixture(t); const pendingInit = f.channel.initialize(); f.input.write(frame(1, bytes('private-original')));
  const initial = await pendingInit;
  const pending = f.channel.bindRequest('whole body', P).authorizePhysicalSend();
  const denied = assert.rejects(pending); f.original.abort(); await f.channel.dispose(); await denied;
  assert.ok(initial.every(b => b === 0)); assert.equal(f.output.destroyed, true); assert.equal(f.input.destroyed, true);
});

test('cancel held writer unblocks callback and joins without unhandled rejection', { timeout: 10000 }, async t => {
  const entered = gate(); const f = fixture(t, { onWrite() { entered.resolve(); } });
  const parentUse = await initialized(f); const pending = parentUse.authorizePhysicalSend();
  const denied = assert.rejects(pending); await entered.promise;
  f.original.abort(); await f.channel.dispose(); await denied;
});

test('dispose never clears borrowed write bytes or claims join before actual callback', { timeout: 10000 }, async t => {
  const entered = gate(); let settle, borrowed;
  const f = fixture(t, { holdDestroyCallbacks: true, onWrite(chunk, done) { borrowed = chunk; settle = done; entered.resolve(); } });
  const parentUse = await initialized(f); const pending = parentUse.authorizePhysicalSend();
  const denied = assert.rejects(pending); await entered.promise;
  const before = Buffer.from(borrowed); f.original.abort();
  let disposed = false; const disposal = f.channel.dispose().then(() => { disposed = true; });
  // Event-turn barrier, not a duration/expiry oracle: the one write callback is
  // still explicitly held, so no implementation may claim it quiescent.
  try {
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(disposed, false); assert.deepEqual(borrowed, before);
  } finally {
    settle(new Error('synthetic actual callback joined')); await disposal; await denied;
  }
  assert.ok(borrowed.every(b => b === 0));
});

test('dispose held initialize and one-shot failures never allow reentry', { timeout: 10000 }, async t => {
  const f = fixture(t); const first = f.channel.initialize(); const rejected = assert.rejects(first);
  await f.channel.dispose(); await rejected; await assert.rejects(f.channel.initialize());
  const g = fixture(t), parentUse = await initialized(g);
  await assert.rejects(parentUse.authorizePhysicalSend('not zero arguments'));
  await assert.rejects(parentUse.authorizePhysicalSend()); assert.equal(g.chunks.length, 0);
  const h = fixture(t), { parentUse: owner, capability } = await granted(h);
  await assert.rejects(owner.authorizePhysicalSend()); assert.equal(capability.signal.aborted, true);
});

test('long-timer rearm and sub-ms conservative cancellation do not renew J', { timeout: 10000 }, async t => {
  const f = fixture(t), { capability } = await granted(f, (1n << 63n) - 1n);
  const start = f.tick, first = [...f.timers.entries()][0]; f.timers.delete(first[0]);
  f.advance(2147483647n * 1000000n); first[1].fn();
  assert.equal(capability.assertUsable(), undefined); assert.equal(f.timers.size, 1);
  f.setTick(start + (1n << 63n) - 1n - 1n);
  const second = [...f.timers.entries()][0]; f.timers.delete(second[0]); second[1].fn();
  assert.equal(capability.signal.aborted, true); assert.throws(() => capability.assertUsable());
});

const freeze = x => {
  if (x && typeof x === 'object') { for (const v of Object.values(x)) freeze(v); Object.freeze(x); }
  return x;
};
const request = freeze({ model: 'public-model-fixture', stream: true, store: false, max_output_tokens: 64,
  truncation: 'disabled', tool_choice: 'none', text: { format: { type: 'json_schema', name: 'final_fixture',
    strict: true, schema: { type: 'object', properties: { answer: { type: 'string' } }, required: ['answer'], additionalProperties: false } } },
  input: [{ role: 'system', content: 'Preserve all original history and source associations.' },
    { role: 'user', content: [{ type: 'input_text', text: 'history same.json {"n":900719925474099312345}' },
      { type: 'input_image', detail: 'auto', image_url: 'data:image/png;base64,AQIDBA==' }] },
    { type: 'message', id: 'msg-history', role: 'assistant', status: 'completed', content: [{ type: 'output_text', text: 'prior answer', annotations: [] }] },
    { role: 'user', content: [{ type: 'input_text', text: 'newest same.json {"fraction":1e-100}' },
      { type: 'input_image', detail: 'auto', image_url: 'data:image/jpeg;base64,BQYHCA==' }] }] });
const endpoint = 'https://public.invalid/v1/responses';
const headers = freeze([['accept', 'application/json'], ['content-type', 'application/json'], ['authorization', 'Bearer public-NOT-A-KEY'],
  ['user-agent', 'public-fixture'], ['x-stainless-lang', 'js'], ['x-stainless-package-version', '6.40.0'], ['x-stainless-os', 'public-os'],
  ['x-stainless-arch', 'public-arch'], ['x-stainless-runtime', 'node'], ['x-stainless-runtime-version', 'public-runtime'],
  ['x-stainless-retry-count', '0'], ['x-stainless-timeout', '3600']]);
const admitted = freeze({ endpoint, method: 'POST', headers, body: JSON.stringify(request), request, schema: request.text.format.schema });
const physicalLimits = freeze({ native_request_bytes: 16384, pre_send_allocation_bytes: 65536,
  request_header_bytes: 2048, response_header_bytes: 512, response_body_bytes: 8192, response_chunk_bytes: 8192,
  sse_line_bytes: 4096, sse_event_bytes: 4096, sse_events: 64, json_nodes: 20000, json_depth: 32,
  response_copy_work_bytes: 262144, response_allocation_bytes: 131072, process_memory_bytes: 262144 });
const echo = Object.fromEntries(['model', 'max_output_tokens', 'store', 'truncation', 'tool_choice', 'text'].map(k => [k, request[k]]));
function completedResponse() {
  const response = { id: 'resp-public', created_at: 0, error: null, incomplete_details: null,
    instructions: null, metadata: {}, object: 'response', output: [], parallel_tool_calls: true, temperature: 1,
    tools: [], top_p: 1, status: 'completed', ...echo };
  return new Response(`data: ${JSON.stringify({ type: 'response.completed', sequence_number: 1, response })}\n\n`,
    { headers: { 'content-type': 'text/event-stream' } });
}
async function guardedFixture(t, { physicalFetch, mutate } = {}) {
  let f;
  const caps = Object.freeze({ initialize_body_bytes: 512, request_body_bytes: 16384, final_body_bytes: 512, frame_bytes: 32768 });
  let writes = 0, grants = 0, sends = 0, sentSignal;
  f = fixture(t, { caps, onWrite(_part, done) {
    done();
    if (++writes === 3) {
      const raw = Buffer.concat(f.chunks);
      assert.equal(raw.toString('ascii', 0, 4), 'SPU2'); assert.equal(raw[4], 2);
      assert.equal(raw.readUInt32BE(5), 129); assert.equal(raw.readBigUInt64BE(9), BigInt(Buffer.byteLength(admitted.body)));
      assert.equal(raw.toString('utf8', 17, 146), `{"version":1,"phase":"physical_send","backend_profile_digest":"${P}"}`);
      assert.equal(raw.toString('utf8', 146), admitted.body, 'whole actual guarded body crosses IPC, not hash-only');
      grants++; f.input.write(use());
    }
  } });
  const initial = f.channel.initialize(); f.input.write(frame(1, bytes('opaque Init DATA; no schema or real fit claimed'))); await initial;
  const parentUse = f.channel.bindRequest(admitted.body, P);
  const owner = createPhysicalFetchOwner({ limits: physicalLimits, admittedRequest: admitted,
    originalSignal: f.original.signal, parentUse, physicalFetch: async (url, init) => {
      sends++; sentSignal = init.signal; assert.equal(url, endpoint); assert.equal(init.body, admitted.body);
      assert.equal(init.redirect, 'error'); assert.deepEqual([...init.headers], [...new Headers(headers)]);
      assert.notEqual(init.signal, f.original.signal); assert.notEqual(init.signal, f.channel.signal);
      return physicalFetch ? physicalFetch(url, init) : completedResponse();
    } });
  t.after(() => owner.dispose());
  const incoming = { signal: new AbortController().signal, method: 'POST', headers: new Headers(headers), body: admitted.body };
  mutate?.(incoming);
  return { ...f, owner, incoming, get grants() { return grants; }, get sends() { return sends; }, get sentSignal() { return sentSignal; } };
}

test('existing actual fetch guard → ZEROarg child exchange → same physical controller and original full body', { timeout: 10000 }, async t => {
  const f = await guardedFixture(t);
  const response = await f.owner.fetch(endpoint, f.incoming);
  const reader = response.body.getReader(); assert.equal((await reader.read()).done, false);
  assert.equal((await reader.read()).done, true); reader.releaseLock();
  assert.equal(f.grants, 1); assert.equal(f.sends, 1);
  await f.channel.publishFinal('{"answer":"exact provider final"}');
  await assert.rejects(f.owner.fetch(endpoint, f.incoming)); assert.equal(f.sends, 1);
});

test('actual body/header/method/endpoint mismatch gets zero IPC authorization and zero physical sends', { timeout: 10000 }, async t => {
  for (const mode of ['body', 'header', 'method', 'endpoint']) {
    const f = await guardedFixture(t, { mutate(init) {
      if (mode === 'body') init.body = '{"model":"smaller fake"}';
      if (mode === 'header') init.headers.set('authorization', 'Bearer different-public-literal');
      if (mode === 'method') init.method = 'GET';
    } });
    await assert.rejects(f.owner.fetch(mode === 'endpoint' ? endpoint + '/wrong' : endpoint, f.incoming));
    assert.equal(f.chunks.length, 0, mode); assert.equal(f.grants, 0, mode); assert.equal(f.sends, 0, mode);
  }
});

test('Retire aborts SAME actual fetch controller; late raw response cancelled, no replay', { timeout: 10000 }, async t => {
  const entered = gate(), late = gate();
  const f = await guardedFixture(t, { physicalFetch() { entered.resolve(); return late.promise; } });
  const pending = f.owner.fetch(endpoint, f.incoming); const rejected = assert.rejects(pending);
  await entered.promise; const aborted = gate(); f.sentSignal.addEventListener('abort', aborted.resolve, { once: true });
  f.input.write(frame(5)); await aborted.promise; await rejected;
  assert.equal(f.sentSignal.aborted, true); assert.equal(f.channel.signal.aborted, true);
  const cancelled = gate();
  late.resolve(new Response(new ReadableStream({ cancel() { cancelled.resolve(); } }, { highWaterMark: 0 })));
  await cancelled.promise; await f.channel.dispose();
  await assert.rejects(f.owner.fetch(endpoint, f.incoming)); assert.equal(f.sends, 1); assert.equal(f.grants, 1);
});
