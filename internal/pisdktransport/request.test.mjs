import assert from 'node:assert/strict';
import { createPhysicalFetchOwner } from './request.mjs';

// Already-admitted PUBLIC SYNTHETIC data/capabilities, never serving evidence.
// No SDK, global fetch, sockets, timer, model, credential or source-phase call.
const freeze = (x) => { if (x && typeof x === 'object') { for (const v of Object.values(x)) freeze(v); Object.freeze(x); } return x; };
const clone = (x) => JSON.parse(JSON.stringify(x));
const gate = () => { let resolve; let reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return {promise, resolve, reject}; };
const schema = freeze({type: 'object', properties: {answer: {type: 'string', maxLength: 32000}, artifacts: {type: 'array', maxItems: 2,
  items: {type: 'object', properties: {name: {type: 'string'}, content: {type: 'string'}}, required: ['name', 'content'], additionalProperties: false}}},
  required: ['answer', 'artifacts'], additionalProperties: false});
const request = freeze({model: 'public-model-fixture', stream: true, store: false, max_output_tokens: 64, truncation: 'disabled', tool_choice: 'none',
  text: {format: {type: 'json_schema', name: 'final_fixture', strict: true, description: 'Synthetic answer and named originals', schema}},
  input: [{role: 'system', content: 'Preserve every original association.'},
    {role: 'user', content: [{type: 'input_text', text: 'original A same.json: {"large":9007199254740993,"fraction":1.234567890123456789}'},
      {type: 'input_text', text: 'original A same.png image/png'}, {type: 'input_image', detail: 'auto', image_url: 'data:image/png;base64,AQIDBA=='}]},
    {type: 'reasoning', id: 'rs-history', summary: [{type: 'summary_text', text: 'Earlier bounded reasoning'}], encrypted_content: 'public-opaque-signature', status: 'completed'},
    {type: 'message', id: 'msg-history', role: 'assistant', status: 'completed', phase: 'final_answer', content: [{type: 'output_text', text: 'Prior ordinary function_call mention', annotations: []}]},
    {role: 'user', content: [{type: 'input_text', text: 'original B same.json: {"fraction":1e-100,"large":18446744073709551615}'},
      {type: 'input_text', text: 'original B same.png image/jpeg'}, {type: 'input_image', detail: 'auto', image_url: 'data:image/jpeg;base64,BQYHCA=='}]}]});
const headers = freeze([['accept', 'application/json'], ['content-type', 'application/json'], ['authorization', 'Bearer public-fixture-NOT-A-KEY'],
  ['user-agent', 'public-sdk-fixture'], ['x-stainless-lang', 'js'], ['x-stainless-package-version', '6.40.0'], ['x-stainless-os', 'public-os-fixture'],
  ['x-stainless-arch', 'public-arch-fixture'], ['x-stainless-runtime', 'node'], ['x-stainless-runtime-version', 'public-runtime-fixture'],
  ['x-stainless-retry-count', '0'], ['x-stainless-timeout', '30']]);
const endpoint = 'https://public.invalid/v1/responses';
const admitted = freeze({endpoint, method: 'POST', headers, body: JSON.stringify(request), request, schema});
const limits = (overrides = {}) => freeze({native_request_bytes: 16384, pre_send_allocation_bytes: 65536,
  request_header_bytes: 2048, response_header_bytes: 512, response_body_bytes: 8192, response_chunk_bytes: 8192,
  sse_line_bytes: 4096, sse_event_bytes: 4096, sse_events: 64, json_nodes: 20000, json_depth: 32,
  response_copy_work_bytes: 262144, response_allocation_bytes: 131072, process_memory_bytes: 262144, ...overrides});
const echoFields = ['model', 'max_output_tokens', 'store', 'truncation', 'tool_choice', 'instructions', 'metadata', 'text', 'reasoning',
  'temperature', 'top_p', 'parallel_tool_calls', 'prompt_cache_key', 'prompt_cache_retention', 'safety_identifier', 'user', 'service_tier'];
const echo = Object.fromEntries(echoFields.filter((key) => Object.hasOwn(request, key)).map((key) => [key, request[key]]));
// Independent logical payload/node oracle; NOT a native heap/interner oracle.
const count = (x) => {
  let nodes = 1; let bytes = typeof x === 'string' ? 2 * x.length : 0; let keyChars = 0; let numberChars = typeof x === 'number' ? String(x).length : 0;
  if (x && typeof x === 'object') for (const [key, value] of Object.entries(x)) {
    const part = count(value); nodes += part.nodes + (Array.isArray(x) ? 0 : 1); bytes += part.bytes + (Array.isArray(x) ? 0 : 2 * key.length);
    keyChars += part.keyChars + (Array.isArray(x) ? 0 : key.length); numberChars += part.numberChars;
  }
  return {nodes, bytes, keyChars, numberChars};
};
const fullCount = count(request); const schemaCount = count(schema); const echoCount = count(echo);
const bodyBytes = new TextEncoder().encode(admitted.body).length;
const R = 6 * endpoint.length + 4 * admitted.body.length + fullCount.bytes + schemaCount.bytes +
  6 * headers.reduce((n, [key, value]) => n + key.length + value.length, 0) + 2 * bodyBytes + 4 * fullCount.keyChars + 2 * fullCount.numberChars;
const outer = (p) => p.response_chunk_bytes + 4 * p.response_header_bytes + 2 * p.response_body_bytes + echoCount.bytes;
const finalEvent = (overrides = {}) => ({type: 'response.completed', sequence_number: 1, response: {id: 'resp-public', created_at: 0,
  error: null, incomplete_details: null, instructions: null, metadata: {}, model: request.model, object: 'response', output: [],
  parallel_tool_calls: true, temperature: 1, tool_choice: 'none', tools: [], top_p: 1, status: 'completed', store: false,
  max_output_tokens: request.max_output_tokens, truncation: 'disabled', text: request.text, ...overrides}});
const wire = (event) => new TextEncoder().encode(`data: ${JSON.stringify(event)}\n\n`);
const upstream = (chunks = [wire(finalEvent())], options = {}) => {
  let pulls = 0; let cancels = 0; let index = 0; const pulled = gate(); const cancelled = gate();
  const stream = new ReadableStream({pull(controller) { pulls++; pulled.resolve(); if (index < chunks.length) controller.enqueue(chunks[index++]); else if (!options.stall) controller.close(); },
    cancel() { cancels++; cancelled.resolve(); if (options.hostileCancel) return new Promise(() => {}); }}, {highWaterMark: 0});
  const response = new Response(stream, {status: options.status ?? 200, headers: {'content-type': 'text/event-stream', ...options.headers}});
  return {response, pulled: pulled.promise, cancelled: cancelled.promise, get pulls() { return pulls; }, get cancels() { return cancels; }};
};
const safeFailure = (code, name = 'PiPhysicalFetchError') => (error) => error instanceof Error && error.name === name && error.message === code;
const fixture = ({p = limits(), expected = admitted, auth, transport, assertion} = {}) => {
  const original = new AbortController(); const sdk = new AbortController(); const use = new AbortController();
  const calls = []; let authorizations = 0; const sent = gate();
  const capability = Object.freeze({signal: use.signal, assertUsable() { return assertion?.({original, sdk, use, owner}); }});
  const parentUse = Object.freeze({authorizePhysicalSend(...args) { authorizations++; assert.equal(args.length, 0, 'parent seam must receive ZERO arguments, not fabricate comparison');
    return auth ? auth.promise : Promise.resolve(capability); }});
  const owner = createPhysicalFetchOwner({limits: p, admittedRequest: expected, originalSignal: original.signal, parentUse,
    physicalFetch(url, init) { calls.push({url, init}); sent.resolve(); return transport ? transport.promise : Promise.resolve(upstream().response); }});
  const incoming = () => ({signal: sdk.signal, method: 'POST', headers: new Headers(expected.headers), body: expected.body});
  return {owner, original, sdk, use, capability, incoming, calls, sent: sent.promise, get authorizations() { return authorizations; }};
};
const finish = async (response) => { const reader = response.body.getReader(); const first = await reader.read(); assert.equal(first.done, false);
  assert.deepEqual(first.value, wire(finalEvent())); assert.equal((await reader.read()).done, true); reader.releaseLock(); };
let cases = 0;
const test = async (name, fn) => { try { await fn(); cases++; } catch (error) { error.message = `${name}: ${error.message}`; throw error; } };
const invalidConfig = (overrides) => { const original = new AbortController(); assert.throws(() => createPhysicalFetchOwner({limits: limits(), admittedRequest: admitted,
  originalSignal: original.signal, parentUse: Object.freeze({authorizePhysicalSend: () => Promise.resolve()}), physicalFetch: () => Promise.resolve(), ...overrides}), safeFailure('pi_physical_configuration_invalid')); };

// FIRST so old-source execution reaches only this narrow R01 regression.
// The fallback observer in finally keeps deliberate old/mutant runs safe;
// it does not count as the production observer oracle or swallow test failure.
await test('malformed native assertion Promise observed without async permission or raw cause', async () => {
  for (const rejected of [true, false]) {
    const returned = rejected ? Promise.reject(new Error('public-opaque-assertion-diagnostic')) : Promise.resolve(undefined);
    const intrinsicThen = Promise.prototype.then; const observers = [];
    Promise.prototype.then = function (yes, no) {
      if (this === returned) observers.push({yes, no});
      return Reflect.apply(intrinsicThen, this, [yes, no]);
    };
    try {
      const f = fixture({assertion: () => returned});
      await assert.rejects(f.owner.fetch(endpoint, f.incoming()), safeFailure('pi_physical_parent_denied'));
      assert.equal(f.calls.length, 0, 'async assertion never grants send');
      assert.equal(observers.length, 1, 'production must immediately observe SAME malformed native Promise');
      assert.equal(typeof observers[0].yes, 'function'); assert.equal(typeof observers[0].no, 'function');
      assert.equal(observers[0].no(new Error('public-opaque-late-diagnostic')), undefined, 'observer ignores raw cause');
      f.owner.dispose();
    } finally {
      Promise.prototype.then = intrinsicThen;
      Reflect.apply(intrinsicThen, returned, [() => {}, () => {}]);
    }
  }
});
await test('held authorization disposal retires owner before late fulfillment/rejection with zero effects', async () => {
  for (const rejection of [false, true]) {
    const auth = gate(); const f = fixture({auth}); const init = f.incoming(); const pending = f.owner.fetch(endpoint, init);
    f.owner.dispose(); await assert.rejects(pending, safeFailure('pi_physical_aborted'));
    init.headers.set('authorization', 'Bearer public-caller-owned-after-dispose'); init.body = '{}';
    if (rejection) auth.reject(new Error('public-opaque-late-auth-diagnostic')); else auth.resolve(f.capability);
    await auth.promise.catch(() => {}); await Promise.resolve();
    assert.equal(f.calls.length, 0); assert.equal(f.authorizations, 1);
    await assert.rejects(f.owner.fetch(endpoint, f.incoming()), safeFailure('pi_physical_send_spent'));
    f.owner.dispose(); assert.equal(f.calls.length, 0);
  }
});
await test('complete full-body/input/schema positive and SAME owned controller throughout terminal', async () => {
  const stream = upstream(); const transport = gate(); const f = fixture({transport}); const init = f.incoming(); const result = f.owner.fetch(endpoint, init);
  await f.sent; assert.equal(f.authorizations, 1); assert.equal(f.calls.length, 1); const sent = f.calls[0];
  assert.equal(sent.url, endpoint); assert.equal(sent.init.body, admitted.body); assert.deepEqual(JSON.parse(sent.init.body), request);
  assert.notStrictEqual(sent.init.headers, init.headers); assert.deepEqual([...sent.init.headers], [...init.headers]);
  assert.notStrictEqual(sent.init.signal, f.sdk.signal); assert.equal(sent.init.redirect, 'error');
  assert.deepEqual(Object.keys(sent.init).sort(), ['body', 'headers', 'method', 'redirect', 'signal']);
  transport.resolve(stream.response); const response = await result; assert.notStrictEqual(response, stream.response);
  assert.equal(stream.pulls, 0, 'no read before consumer pull'); await finish(response);
  assert.equal(sent.init.signal.aborted, true); assert.equal(f.sdk.signal.aborted, false); assert.equal(f.original.signal.aborted, false);
  assert.equal(stream.cancels, 1); assert.equal(stream.response.body.locked, false); f.owner.dispose(); assert.equal(stream.cancels, 1);
});
await test('malformed FIRST entry irreversibly spends before validation/await', async () => {
  const f = fixture(); await assert.rejects(f.owner.fetch(endpoint, {...f.incoming(), duplex: 'half'}), safeFailure('pi_physical_request_denied'));
  await assert.rejects(f.owner.fetch(endpoint, f.incoming()), safeFailure('pi_physical_send_spent')); assert.equal(f.authorizations, 0); assert.equal(f.calls.length, 0);
});
await test('concurrent/reentrant calls cannot obtain a second parent or send', async () => {
  const auth = gate(); const f = fixture({auth}); const first = f.owner.fetch(endpoint, f.incoming());
  const second = f.owner.fetch(endpoint, f.incoming());
  assert.equal(f.authorizations, 1, 'FIRST-entry spend must prevent a second capability BEFORE waiting');
  await assert.rejects(second, safeFailure('pi_physical_send_spent'));
  auth.resolve(f.capability); await finish(await first); assert.equal(f.calls.length, 1);
});
await test('owned Headers snapshot survives mutation while authorization is held', async () => {
  const auth = gate(); const f = fixture({auth}); const init = f.incoming(); const first = f.owner.fetch(endpoint, init);
  init.headers.set('authorization', 'Bearer mutated-public'); init.headers.set('x-extra', 'mutated'); init.body = '{}'; init.method = 'GET';
  auth.resolve(f.capability); await finish(await first);
  assert.equal(f.calls[0].init.headers.get('authorization'), 'Bearer public-fixture-NOT-A-KEY'); assert.equal(f.calls[0].init.headers.has('x-extra'), false);
  assert.equal(f.calls[0].init.body, admitted.body); assert.equal(f.calls[0].init.method, 'POST');
});
for (const kind of ['original', 'sdk', 'dispose']) await test(`held authorization ${kind} abort settles BEFORE gate release`, async () => {
  const auth = gate(); const f = fixture({auth}); const pending = f.owner.fetch(endpoint, f.incoming());
  if (kind === 'dispose') f.owner.dispose(); else f[kind].abort();
  await assert.rejects(pending, safeFailure('pi_physical_aborted')); assert.equal(f.calls.length, 0);
  auth.resolve(f.capability); await auth.promise; await Promise.resolve(); assert.equal(f.calls.length, 0);
});
for (const kind of ['original', 'sdk', 'use', 'dispose']) await test(`held physical fetch ${kind} abort settles and cancels late SAME Response once`, async () => {
  const transport = gate(); const f = fixture({transport}); const pending = f.owner.fetch(endpoint, f.incoming()); await f.sent;
  if (kind === 'dispose') f.owner.dispose(); else f[kind].abort();
  assert.equal(f.calls[0].init.signal.aborted, true, 'held physical fetch cutoff must synchronously abort SAME controller before waiting');
  await assert.rejects(pending, safeFailure('pi_physical_aborted')); assert.equal(f.calls[0].init.signal.aborted, true);
  const late = upstream([], {stall: true, hostileCancel: true}); transport.resolve(late.response); await late.cancelled;
  assert.equal(late.cancels, 1); assert.equal(late.pulls, 0); assert.equal(late.response.body.locked, false); f.owner.dispose(); assert.equal(late.cancels, 1);
});
for (const kind of ['use', 'dispose']) await test(`immediate POST-assert ${kind} recheck prohibits physical send`, async () => {
  const f = fixture({assertion: (x) => kind === 'dispose' ? x.owner.dispose() : x.use.abort()});
  await assert.rejects(f.owner.fetch(endpoint, f.incoming()), safeFailure('pi_physical_aborted')); assert.equal(f.calls.length, 0);
});
await test('false or malformed parent fulfillment and raw rejection are safe terminal failures', async () => {
  for (const value of [true, false, {signal: new AbortController().signal, assertUsable() {}}, Object.freeze({signal: new AbortController().signal, assertUsable() {}, extra: 1})]) {
    const auth = gate(); const f = fixture({auth}); const pending = f.owner.fetch(endpoint, f.incoming()); auth.resolve(value);
    await assert.rejects(pending, safeFailure('pi_physical_parent_denied')); assert.equal(f.calls.length, 0);
  }
  const auth = gate(); const f = fixture({auth}); const pending = f.owner.fetch(endpoint, f.incoming()); auth.reject(new Error('public-secret-diagnostic'));
  await assert.rejects(pending, safeFailure('pi_physical_parent_denied'));
});
await test('five-option contract only; own-data configuration rejects null/accessors/aliases safely', () => {
  for (const value of [null, undefined]) assert.throws(() => createPhysicalFetchOwner(value), safeFailure('pi_physical_configuration_invalid'));
  invalidConfig({validateFrame: () => 'terminal'}); invalidConfig({limits: freeze({...limits(), responseAllocationBytes: 1})});
  invalidConfig({limits: {...limits()}}); invalidConfig({admittedRequest: {...admitted}});
  let touched = 0; const bad = {limits: limits(), admittedRequest: admitted, originalSignal: new AbortController().signal,
    parentUse: Object.freeze({authorizePhysicalSend() {}})}; Object.defineProperty(bad, 'physicalFetch', {enumerable: true, get() { touched++; return () => {}; }});
  assert.throws(() => createPhysicalFetchOwner(bad), safeFailure('pi_physical_configuration_invalid')); assert.equal(touched, 0);
});
await test('exact full-body/schema/header/endpoint/method/input rejection before parent', async () => {
  for (const alter of [(i) => i.body = '{}', (i) => i.method = 'GET', (i) => i.headers.delete('authorization'),
    (i) => i.headers.set('authorization', 'Bearer wrong-public'), (i) => i.headers.set('x-session', 'wrong'), (i) => i.signal = {}, (i) => i.body = new Uint8Array()]) {
    const f = fixture(); const init = f.incoming(); alter(init); await assert.rejects(f.owner.fetch(endpoint, init), safeFailure('pi_physical_request_denied'));
    assert.equal(f.authorizations, 0); assert.equal(f.calls.length, 0);
  }
  for (const url of [new URL(endpoint), endpoint + '?x=1', endpoint + '/extra', 'http://public.invalid/v1/responses']) {
    const f = fixture(); await assert.rejects(f.owner.fetch(url, f.incoming()), safeFailure('pi_physical_request_denied')); assert.equal(f.authorizations, 0);
  }
  const changed = clone(request); changed.input[1].content[2].image_url = 'data:image/png;base64,BQYHCA==';
  const f = fixture({expected: freeze({...admitted, request: freeze(changed)})});
  await assert.rejects(f.owner.fetch(endpoint, f.incoming()), safeFailure('pi_physical_request_denied')); assert.equal(f.authorizations, 0);
  const alteredSchema = clone(schema); alteredSchema.properties.answer.maxLength--;
  invalidConfig({admittedRequest: freeze({...admitted, schema: alteredSchema})});
  for (const mutation of [(r) => r.tools = [], (r) => r.input.push({type: 'function_call', name: 'effect'}), (r) => r.text.format.strict = false,
    (r) => r.store = true, (r) => r.max_output_tokens = 15, (r) => r.truncation = 'auto', (r) => r.input[1].content[2].detail = 'low']) {
    const r = clone(request); mutation(r); invalidConfig({admittedRequest: freeze({...admitted, request: r, body: JSON.stringify(r)})});
  }
});
await test('no caller getter invoked during malformed fetch validation', async () => {
  const f = fixture(); const init = f.incoming(); let touched = 0; Object.defineProperty(init, 'body', {enumerable: true, get() { touched++; return admitted.body; }});
  await assert.rejects(f.owner.fetch(endpoint, init), safeFailure('pi_physical_request_denied')); assert.equal(touched, 0); assert.equal(f.authorizations, 0);
});
await test('strict complete preparse grammar duplicate/escaped-alias/depth/node rejection BEFORE JSON.parse', () => {
  const parse = JSON.parse; let calls = 0; JSON.parse = (...args) => { calls++; return Reflect.apply(parse, JSON, args); };
  try {
    for (const body of [admitted.body.replace('{', '{"model":"duplicate",'), admitted.body.replace('{', '{"\\u006dodel":"duplicate",'),
      admitted.body + 'x', admitted.body.replace('"stream":true', '"stream":tru'), admitted.body.replace('"max_output_tokens":64', '"max_output_tokens":1e999')])
      invalidConfig({admittedRequest: freeze({...admitted, body})});
    invalidConfig({limits: limits({json_depth: 2})}); invalidConfig({limits: limits({json_nodes: fullCount.nodes})});
    assert.equal(calls, 0);
  } finally { JSON.parse = parse; }
});
await test('request R exact monotonic copy fit and first overflow reject BEFORE parse/send', async () => {
  const p = limits({native_request_bytes: bodyBytes, pre_send_allocation_bytes: R}); const f = fixture({p});
  const parse = JSON.parse; let calls = 0; JSON.parse = (...args) => { calls++; return Reflect.apply(parse, JSON, args); };
  let response;
  try { response = await f.owner.fetch(endpoint, f.incoming()); assert.equal(calls, 1); invalidConfig({limits: limits({native_request_bytes: bodyBytes, pre_send_allocation_bytes: R - 1})}); assert.equal(calls, 1); }
  finally { JSON.parse = parse; }
  await finish(response); assert.equal(f.calls.length, 1);
});
await test('UTF8/body/header exact bound and first overflow remain safe before copies', () => {
  const f = fixture({p: limits({native_request_bytes: bodyBytes})}); f.owner.dispose(); invalidConfig({limits: limits({native_request_bytes: bodyBytes - 1})});
  const headerBytes = headers.reduce((n, [key, value]) => n + new TextEncoder().encode(key).length + new TextEncoder().encode(value).length + 4, 0);
  fixture({p: limits({request_header_bytes: headerBytes})}).owner.dispose(); invalidConfig({limits: limits({request_header_bytes: headerBytes - 1})});
  const bad = clone(request); bad.input[0].content += '\ud800'; invalidConfig({admittedRequest: freeze({...admitted, request: bad, body: JSON.stringify(bad)})});
  const unicode = clone(request); unicode.input[0].content += 'ёж😀'; fixture({expected: freeze({...admitted, request: unicode, body: JSON.stringify(unicode)})}).owner.dispose();
});
await test('combined allocation equals total with actual semantic positive; original P unchanged', async () => {
  const base = limits(); const O = outer(base); const A = 65536; const p = limits({response_allocation_bytes: O + A, process_memory_bytes: base.pre_send_allocation_bytes + O + A});
  const before = JSON.stringify(p); const f = fixture({p}); await finish(await f.owner.fetch(endpoint, f.incoming())); assert.equal(JSON.stringify(p), before);
});
await test('constructor Amin joint boundary DISTINCT from semantic frame runtime budget', () => {
  const base = limits(); const O = outer(base); const Amin = Math.max(2 * base.sse_event_bytes + 1, echoCount.bytes);
  for (const field of ['response_allocation_bytes', 'process_memory_bytes']) {
    const exact = field === 'response_allocation_bytes' ? O + Amin : base.pre_send_allocation_bytes + O + Amin;
    fixture({p: limits({[field]: exact})}).owner.dispose(); invalidConfig({limits: limits({[field]: exact - 1})});
  }
  invalidConfig({limits: limits({response_allocation_bytes: 2 * base.sse_event_bytes + 1})});
});
await test('shared response copy and cumulative whole-tree nodes first overflow', () => {
  const p = limits(); const copyMinimum = 4 * p.response_header_bytes + 4 * p.response_body_bytes + p.sse_event_bytes;
  fixture({p: limits({response_copy_work_bytes: copyMinimum + 1})}).owner.dispose(); invalidConfig({limits: limits({response_copy_work_bytes: copyMinimum})});
  const J = 2 * fullCount.nodes + schemaCount.nodes; const Nmin = J + 2 * echoCount.nodes;
  fixture({p: limits({json_nodes: Nmin})}).owner.dispose(); invalidConfig({limits: limits({json_nodes: Nmin - 1})});
});
await test('runtime dynamic reduced allocation rejects frame, not mislabeled constructor failure', async () => {
  const p = limits(); const O = outer(p); const Amin = Math.max(2 * p.sse_event_bytes + 1, echoCount.bytes);
  const f = fixture({p: limits({response_allocation_bytes: O + Amin})}); const response = await f.owner.fetch(endpoint, f.incoming());
  await assert.rejects(response.body.getReader().read(), safeFailure('pi_response_frame_denied', 'PiResponseGuardError')); assert.equal(f.calls[0].init.signal.aborted, true);
});
await test('joint cumulative JSON first response overflow stops BEFORE JSON.parse/forward', async () => {
  const J = 2 * fullCount.nodes + schemaCount.nodes; const p = limits({json_nodes: J + 2 * echoCount.nodes}); const f = fixture({p});
  const response = await f.owner.fetch(endpoint, f.incoming()); const parse = JSON.parse; let calls = 0;
  JSON.parse = (...args) => { calls++; return Reflect.apply(parse, JSON, args); };
  try { await assert.rejects(response.body.getReader().read(), safeFailure('pi_response_frame_denied', 'PiResponseGuardError')); assert.equal(calls, 0); }
  finally { JSON.parse = parse; }
});
for (const options of [{status: 429}, {headers: {'content-type': 'application/json'}}, {headers: {'content-encoding': 'gzip'}},
  {headers: {'x-too-long': 'x'.repeat(600)}}, {headers: {'content-length': '8193'}}]) await test('F09 metadata rejects/cancels SAME source BEFORE reads', async () => {
  const stream = upstream([], {...options, stall: true}); const transport = gate(); const f = fixture({transport}); const pending = f.owner.fetch(endpoint, f.incoming()); await f.sent;
  transport.resolve(stream.response); await assert.rejects(pending, safeFailure('pi_physical_aborted'));
  assert.equal(stream.pulls, 0); assert.equal(stream.cancels, 1); assert.equal(f.calls[0].init.signal.aborted, true); assert.equal(stream.response.body.locked, false);
});
await test('fixed F10 denies effect BEFORE forwarding; no external validator escape', async () => {
  const stream = upstream([wire({type: 'response.function_call_arguments.delta', sequence_number: 1, delta: 'effect'})]);
  const transport = gate(); const f = fixture({transport}); const pending = f.owner.fetch(endpoint, f.incoming()); await f.sent; transport.resolve(stream.response);
  const response = await pending; await assert.rejects(response.body.getReader().read(), safeFailure('pi_response_frame_denied', 'PiResponseGuardError'));
  assert.equal(stream.cancels, 1); assert.equal(f.calls[0].init.signal.aborted, true);
});
for (const kind of ['original', 'sdk', 'use', 'dispose', 'consumer']) await test(`stalled body ${kind} cancellation stops SAME reader/controller once`, async () => {
  const stream = upstream([], {stall: true, hostileCancel: true}); const transport = gate(); const f = fixture({transport}); const pending = f.owner.fetch(endpoint, f.incoming()); await f.sent;
  transport.resolve(stream.response); const response = await pending; const reader = response.body.getReader(); const read = reader.read(); await stream.pulled;
  if (kind === 'consumer') { await reader.cancel(); assert.equal((await read).done, true); }
  else {
    if (kind === 'dispose') f.owner.dispose(); else f[kind].abort();
    assert.equal(f.calls[0].init.signal.aborted, true, 'body cutoff must synchronously abort SAME physical controller before waiting on a read');
    await assert.rejects(read, safeFailure('pi_response_aborted', 'PiResponseGuardError'));
  }
  assert.equal(f.calls[0].init.signal.aborted, true); assert.equal(stream.cancels, 1); assert.equal(stream.response.body.locked, false); f.owner.dispose(); assert.equal(stream.cancels, 1);
});
await test('partial EOF and chunk overflow never masquerade as semantic terminal', async () => {
  for (const chunks of [[new TextEncoder().encode('data: {')], [new Uint8Array(8193)]]) {
    const stream = upstream(chunks); const transport = gate(); const f = fixture({transport}); const pending = f.owner.fetch(endpoint, f.incoming()); await f.sent; transport.resolve(stream.response);
    const response = await pending; await assert.rejects(response.body.getReader().read(), (e) => e.name === 'PiResponseGuardError' && ['pi_response_incomplete', 'pi_response_frame_denied'].includes(e.message));
    assert.equal(f.calls[0].init.signal.aborted, true); assert.equal(stream.response.body.locked, false);
  }
});
await test('transport malformed and raw error causes never leak or retry', async () => {
  for (const value of [{}, null]) { const transport = gate(); const f = fixture({transport}); const pending = f.owner.fetch(endpoint, f.incoming()); await f.sent; transport.resolve(value);
    await assert.rejects(pending, safeFailure('pi_physical_response_denied')); assert.equal(f.calls.length, 1); assert.equal(f.calls[0].init.signal.aborted, true); }
  const transport = gate(); const f = fixture({transport}); const pending = f.owner.fetch(endpoint, f.incoming()); await f.sent; transport.reject(new Error('public-secret-diagnostic'));
  await assert.rejects(pending, safeFailure('pi_physical_transport_denied')); await assert.rejects(f.owner.fetch(endpoint, f.incoming()), safeFailure('pi_physical_send_spent'));
});
console.log(`Pi isolated physical-fetch tests passed (${cases} cases; synthetic NON-authority, no SDK/network/native proof).`);
