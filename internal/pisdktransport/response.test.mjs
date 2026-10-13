import assert from 'node:assert/strict';
import { createGuardedResponse } from './response.mjs';

const encode = (text) => new TextEncoder().encode(text);
const decode = (value) => new TextDecoder().decode(value);
const policy = (overrides = {}) => Object.freeze({
  response_header_bytes: 256, response_body_bytes: 4096, response_chunk_bytes: 4096,
  sse_line_bytes: 512, sse_event_bytes: 1024, sse_events: 64,
  response_copy_work_bytes: 8192, response_allocation_bytes: 2048, ...overrides,
});
const gate = () => { let resolve; const promise = new Promise((done) => { resolve = done; }); return { promise, resolve }; };
const end = encode('data: [DONE]\n\n');
const accept = (frame) => decode(frame) === decode(end) ? 'terminal' : 'continue';
const source = (chunks, options = {}) => {
  const observed = { pulls: 0, cancels: 0 };
  const started = gate();
  const body = new ReadableStream({
    start(controller) { observed.controller = controller; },
    pull(controller) {
      observed.pulls++; started.resolve();
      if (chunks.length) controller.enqueue(chunks.shift());
      else if (!options.stall) controller.close();
    },
    cancel() { observed.cancels++; return options.cancelPromise; },
  }, { highWaterMark: 0 });
  return { body, observed, started };
};
const setup = (chunks = [end], options = {}) => {
  const upstream = source(chunks, options);
  const original = new AbortController();
  const sdk = new AbortController();
  const fetch = new AbortController();
  const response = new Response(upstream.body, {
    status: options.status ?? 200, statusText: options.statusText ?? '',
    headers: options.headers ?? { 'content-type': 'text/event-stream' },
  });
  const create = () => createGuardedResponse({response, limits: options.limits ?? policy(),
    originalSignal: original.signal, sdkSignal: sdk.signal, fetchController: fetch,
    validateFrame: options.validateFrame ?? accept});
  return { ...upstream, original, sdk, fetch, response, create };
};
const safeReject = (promise, code) => assert.rejects(promise, (error) => {
  assert.equal(error.name, 'PiResponseGuardError'); assert.equal(error.message, code);
  assert.equal(error.cause, undefined); return true;
});
const clean = (fixture) => {
  assert.equal(fixture.fetch.signal.aborted, true);
  assert.equal(fixture.observed.cancels, 1);
  assert.equal(fixture.body.locked, false);
};
let cases = 0;
const test = async (_name, fn) => { await fn(); cases++; };

await test('invalid entry arguments have typed safe failure and owned cleanup', async () => {
  for (const overrides of [{validateFrame: undefined}, {sdkSignal: undefined},
    {originalSignal: undefined}, {sdkSignal: {}}, {originalSignal: {}}]) {
    const fixture = setup();
    assert.throws(() => createGuardedResponse({response: fixture.response, limits: policy(),
      originalSignal: fixture.original.signal, sdkSignal: fixture.sdk.signal,
      fetchController: fixture.fetch, validateFrame: accept, ...overrides}),
    /^PiResponseGuardError: pi_response_configuration_invalid$/);
    assert.equal(fixture.observed.pulls, 0); clean(fixture);
  }
  for (const options of [undefined, null]) {
    assert.throws(() => createGuardedResponse(options),
      /^PiResponseGuardError: pi_response_configuration_invalid$/);
  }
});

await test('invalid capabilities and an externally locked reader are not operated on', async () => {
  let foreignCalls = 0;
  const fixture = setup();
  assert.throws(() => createGuardedResponse({response: fixture.response, limits: policy(),
    originalSignal: fixture.original.signal, sdkSignal: fixture.sdk.signal,
    fetchController: {abort() { foreignCalls++; }}, validateFrame: accept}),
  /^PiResponseGuardError: pi_response_configuration_invalid$/);
  assert.equal(foreignCalls, 0); assert.equal(fixture.observed.cancels, 0);
  assert.equal(fixture.fetch.signal.aborted, false);
  assert.throws(() => createGuardedResponse({response: {body: {cancel() { foreignCalls++; }}},
    fetchController: fixture.fetch}), /^PiResponseGuardError: pi_response_configuration_invalid$/);
  assert.equal(foreignCalls, 0); assert.equal(fixture.fetch.signal.aborted, false);
  await fixture.body.cancel();
  const locked = setup(); const ownerReader = locked.body.getReader();
  assert.throws(() => createGuardedResponse({response: locked.response, limits: policy(),
    originalSignal: locked.original.signal, sdkSignal: undefined,
    fetchController: locked.fetch, validateFrame: accept}),
  /^PiResponseGuardError: pi_response_configuration_invalid$/);
  assert.equal(locked.fetch.signal.aborted, true); assert.equal(locked.observed.cancels, 0);
  assert.equal(locked.body.locked, true);
  await ownerReader.cancel(); ownerReader.releaseLock();
});

await test('original frames split at every point, terminal cancels trailing bytes', async () => {
  for (const separator of ['\n\n', '\r\r', '\r\n\r\n']) {
    const one = encode(`data: x${separator}`);
    const terminal = encode(`data: [DONE]${separator}`);
    const wire = encode(`data: x${separator}data: [DONE]${separator}never-forward${separator}`);
    for (let split = 1; split < wire.length; split++) {
      const fixture = setup([wire.subarray(0, split), wire.subarray(split)], {
        validateFrame: (frame) => decode(frame) === decode(terminal) ? 'terminal' : 'continue',
      });
      const reader = fixture.create().body.getReader();
      assert.deepEqual((await reader.read()).value, one);
      assert.deepEqual((await reader.read()).value, terminal);
      assert.equal((await reader.read()).done, true); clean(fixture);
    }
  }
});

await test('no prefetch and one-frame-per-pull with coalesced chunk', async () => {
  const fixture = setup([encode('data: one\n\ndata: two\n\ndata: [DONE]\n\n')]);
  let validations = 0;
  const guard = createGuardedResponse({response: fixture.response, limits: policy(),
    originalSignal: fixture.original.signal, sdkSignal: fixture.sdk.signal,
    fetchController: fixture.fetch, validateFrame(frame) { validations++; return accept(frame); }});
  assert.equal(fixture.observed.pulls, 0); assert.equal(validations, 0);
  const reader = guard.body.getReader();
  assert.equal(decode((await reader.read()).value), 'data: one\n\n');
  assert.equal(validations, 1); assert.equal(fixture.observed.pulls, 1);
  assert.equal(decode((await reader.read()).value), 'data: two\n\n');
  assert.equal(validations, 2); assert.equal(fixture.observed.pulls, 1);
  assert.deepEqual((await reader.read()).value, end); clean(fixture);
});

await test('bytewise max legitimate event and linear copy budget', async () => {
  const fixture = setup([...end].map((byte) => Uint8Array.of(byte)), { limits: policy({
    response_body_bytes: end.length, response_chunk_bytes: 1,
    sse_line_bytes: end.length - 2, sse_event_bytes: end.length, sse_events: 1,
    response_copy_work_bytes: end.length * 2, response_allocation_bytes: end.length * 2,
  }) });
  const reader = fixture.create().body.getReader();
  assert.deepEqual((await reader.read()).value, end); clean(fixture);
});

await test('each original framer overflow cancels before forwarding', async () => {
  for (const limits of [policy({response_chunk_bytes: end.length - 1}),
    policy({sse_line_bytes: end.length - 3}),
    policy({sse_event_bytes: end.length - 1, sse_line_bytes: end.length - 3}),
    policy({response_body_bytes: end.length, response_chunk_bytes: end.length,
      sse_event_bytes: end.length, sse_line_bytes: end.length - 2,
      response_copy_work_bytes: end.length * 2 - 1})]) {
    const fixture = setup([end], { limits });
    await safeReject(fixture.create().body.getReader().read(), 'pi_response_frame_denied'); clean(fixture);
  }
  for (const limits of [policy({sse_events: 1}), policy({response_body_bytes: end.length,
    response_chunk_bytes: end.length, sse_event_bytes: end.length, sse_line_bytes: end.length - 2})]) {
    const fixture = setup([encode('\n\n'), end], { limits });
    const reader = fixture.create().body.getReader(); await reader.read();
    await safeReject(reader.read(), 'pi_response_frame_denied'); clean(fixture);
  }
});

await test('all status, media type, encoding, length and header overflow denials', async () => {
  for (const options of [{status: 400}, {status: 201}, {headers: {}},
    {headers: {'content-type': 'application/json'}},
    {headers: {'content-type': 'text/event-stream', 'content-encoding': 'gzip'}},
    {headers: {'content-type': 'text/event-stream', 'content-encoding': 'identity, identity'}},
    {headers: {'content-type': 'text/event-stream', 'content-length': '4097'}},
    {headers: {'content-type': 'text/event-stream', 'content-length': '01'}},
    {statusText: 'x'.repeat(257)}, {limits: policy({response_header_bytes: 3})},
    {status: 400, headers: {'content-type': 'text/event-stream', 'x-large': 'x'.repeat(257)}}]) {
    const fixture = setup([end], options);
    assert.throws(fixture.create, /^PiResponseGuardError: pi_response_metadata_denied$/);
    assert.equal(fixture.observed.pulls, 0); clean(fixture);
  }
});

await test('exact UTF-8 header budget and identity SSE accepted', async () => {
  const headers = {'content-type': 'text/event-stream; charset=utf-8', 'content-encoding': 'identity', 'x-value': 'é'};
  const size = 3 + [...new Headers(headers)].reduce((sum, [name, value]) =>
    sum + encode(name).length + encode(value).length + 4, 0);
  for (const delta of [0, -1]) {
    const fixture = setup([end], { headers, limits: policy({response_header_bytes: size + delta}) });
    if (delta === 0) assert.deepEqual((await fixture.create().body.getReader().read()).value, end);
    else assert.throws(fixture.create, /^PiResponseGuardError: pi_response_metadata_denied$/);
    clean(fixture);
  }
});

await test('validator denies or throws without exposing raw diagnostics', async () => {
  for (const validateFrame of [() => 'deny', () => 'unknown', () => { throw new Error('secret raw diagnostic'); },
    () => Promise.reject(new Error('private validator error'))]) {
    const fixture = setup([end], {validateFrame});
    await safeReject(fixture.create().body.getReader().read(), 'pi_response_frame_denied'); clean(fixture);
  }
});

await test('validation runs before any frame is observable', async () => {
  const fixture = setup([end], {validateFrame: () => 'deny'});
  let observed = 0;
  try { for await (const _frame of fixture.create().body) observed++; }
  catch (error) { assert.equal(error.message, 'pi_response_frame_denied'); }
  assert.equal(observed, 0); clean(fixture);
});

await test('original, SDK and private controller abort pending read immediately', async () => {
  for (const which of ['original', 'sdk', 'fetch']) {
    const fixture = setup([], {stall: true, cancelPromise: new Promise(() => {})});
    const reader = fixture.create().body.getReader(); const pending = reader.read();
    await fixture.started.promise; fixture[which].abort(new Error('raw abort reason'));
    await safeReject(pending, 'pi_response_aborted'); clean(fixture);
    fixture.original.abort(); fixture.sdk.abort(); assert.equal(fixture.observed.cancels, 1);
  }
});

await test('already aborted original signal rejects before a body read', async () => {
  const fixture = setup(); fixture.original.abort();
  assert.throws(fixture.create, /^PiResponseGuardError: pi_response_aborted$/);
  assert.equal(fixture.observed.pulls, 0); clean(fixture);
});

await test('missing mutable accessor and incoherent canonical limits deny', async () => {
  for (const limits of [undefined, {...policy()}, policy({response_header_bytes: 0}),
    policy({response_header_bytes: Number.MAX_SAFE_INTEGER + 1}), policy({response_allocation_bytes: 1}),
    Object.freeze({...policy(), get response_header_bytes() { throw new Error('must not read getter'); }}),
    Object.freeze({...policy(), get sse_events() { throw new Error('must not read getter'); }})]) {
    const fixture = setup();
    assert.throws(() => createGuardedResponse({response: fixture.response, limits,
      originalSignal: fixture.original.signal, sdkSignal: fixture.sdk.signal,
      fetchController: fixture.fetch, validateFrame: accept}),
    /^PiResponseGuardError: pi_response_configuration_invalid$/);
    assert.equal(fixture.observed.pulls, 0); clean(fixture);
  }
});

await test('consumer cancel and iterator return stop same resources once', async () => {
  const first = setup([], {stall: true, cancelPromise: new Promise(() => {})});
  const reader = first.create().body.getReader(); const pending = reader.read();
  await first.started.promise; await reader.cancel('untrusted raw reason');
  assert.equal((await pending).done, true); clean(first); await reader.cancel(); clean(first);
  const second = setup([encode('data: x\n\ndata: [DONE]\n\n')]);
  for await (const _frame of second.create().body) break;
  clean(second);
});

await test('empty and partial EOF are not terminal completion', async () => {
  for (const chunks of [[], [encode('partial')], [encode('data: x\n\n')]]) {
    const first = chunks[0] && decode(chunks[0]);
    const fixture = setup(chunks); const reader = fixture.create().body.getReader();
    if (first && first.endsWith('\n\n')) await reader.read();
    await safeReject(reader.read(), first === 'partial' ?
      'pi_response_frame_denied' : 'pi_response_incomplete');
    // EOF already closes the source; its cancellation algorithm is not called.
    assert.equal(fixture.fetch.signal.aborted, true); assert.equal(fixture.body.locked, false);
  }
});

await test('upstream errors and zero-byte chunks reject safely', async () => {
  const error = setup([], {stall: true}); const reader = error.create().body.getReader();
  const pending = reader.read(); await error.started.promise;
  error.observed.controller.error(new Error('secret upstream error'));
  await safeReject(pending, 'pi_response_frame_denied');
  assert.equal(error.fetch.signal.aborted, true); assert.equal(error.body.locked, false);
  const empty = setup([new Uint8Array()]);
  await safeReject(empty.create().body.getReader().read(), 'pi_response_frame_denied'); clean(empty);
});

await test('terminal does not read stalled tail or await rejected cancel', async () => {
  const fixture = setup([end], {stall: true, cancelPromise: Promise.reject(new Error('raw cleanup error'))});
  const reader = fixture.create().body.getReader();
  assert.deepEqual((await reader.read()).value, end);
  assert.equal((await reader.read()).done, true); assert.equal(fixture.observed.pulls, 1); clean(fixture);
});

await test('terminal prefix spoof and bounded comments cannot manufacture completion', async () => {
  const fixture = setup([encode(': comment\n\ndata: [DONE]spoof\n\n')]);
  const reader = fixture.create().body.getReader();
  assert.equal(decode((await reader.read()).value), ': comment\n\n');
  assert.equal(decode((await reader.read()).value), 'data: [DONE]spoof\n\n');
  await safeReject(reader.read(), 'pi_response_incomplete');
  assert.equal(fixture.fetch.signal.aborted, true); assert.equal(fixture.body.locked, false);
});

await test('cancel before first pull and pending abort cannot reacquire or forward late bytes', async () => {
  const first = setup(); const body = first.create().body;
  await body.cancel(); assert.equal(first.observed.pulls, 0); clean(first);
  const second = setup([], {stall: true}); const reader = second.create().body.getReader();
  const pending = reader.read(); await second.started.promise; second.sdk.abort();
  await safeReject(pending, 'pi_response_aborted');
  assert.throws(() => second.observed.controller.enqueue(end), TypeError);
  await safeReject(reader.read(), 'pi_response_aborted');
  assert.equal(second.observed.pulls, 1); clean(second);
});

await test('abort during trusted validation never forwards the frame', async () => {
  const fixture = setup([end]);
  const guard = createGuardedResponse({response: fixture.response, limits: policy(),
    originalSignal: fixture.original.signal, sdkSignal: fixture.sdk.signal, fetchController: fixture.fetch,
    validateFrame() { fixture.original.abort(); return 'terminal'; }});
  await safeReject(guard.body.getReader().read(), 'pi_response_aborted'); clean(fixture);
});

console.log(JSON.stringify({passed: true, cases, scope: 'guarded_response_unit_only_no_sdk_or_network'}));
