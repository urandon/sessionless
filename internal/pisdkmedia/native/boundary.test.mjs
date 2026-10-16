// Deterministic transport-seam tests; these are NOT genuine Pi/SDK composition
// or native-platform conformance evidence.
import assert from 'node:assert/strict';
import test from 'node:test';
import { createOneShotBoundary, validateBounds } from './boundary.mjs';

const endpoint = 'https://api.openai.com/v1/responses';
const limits = Object.freeze({ requestBytes: 4096, headerBytes: 4096, responseBytes: 8192,
  chunkBytes: 4096, eventBytes: 4096, lineBytes: 4096, events: 16,
  nodes: 128, depth: 16, copyBytes: 65536 });
const headers = { authorization: 'Bearer noncredential-test-literal', 'content-type': 'application/json' };
const created = { type: 'response.created', response: { status: 'in_progress', output: [], tools: [], tool_choice: 'none' } };
const completed = { type: 'response.completed', response: { status: 'completed', output: [
  { type: 'message', role: 'assistant', status: 'completed', content: [{ type: 'output_text', text: '{"answer":"ok","artifacts":[]}' }] },
] } };
const frame = value => `event: ${value.type}\ndata: ${JSON.stringify(value)}\n\n`;
const transcript = frame(created) + frame(completed);

function fixture({ content = transcript, fragment = 4096, bounds = limits, status = 200,
  responseHeaders = { 'content-type': 'text/event-stream' }, failFetch = false } = {}) {
  const authority = new AbortController(), sdk = new AbortController();
  const bytes = typeof content === 'string' ? new TextEncoder().encode(content) : content;
  let sends = 0, cancelled = 0, offset = 0, reachedEOF = false, sentOptions;
  const physical = async (_url, init) => {
    sends++; sentOptions = init;
    if (failFetch) throw new Error('test-owned physical failure');
    return new Response(new ReadableStream({
      pull(controller) {
        if (offset === bytes.length) { reachedEOF = true; controller.close(); return; }
        controller.enqueue(bytes.slice(offset, offset + fragment)); offset = Math.min(bytes.length, offset + fragment);
      },
      cancel() { cancelled++; },
    }, { highWaterMark: 0 }), { status, headers: responseHeaders });
  };
  const send = createOneShotBoundary({ endpoint, expectedBody: '{}', expectedHeaders: headers,
    bounds, signal: authority.signal }, physical);
  const init = { method: 'POST', body: '{}', headers, signal: sdk.signal };
  return { send, init, authority, sdk, sends: () => sends, cancelled: () => cancelled,
    sentOptions: () => sentOptions, reachedEOF: () => reachedEOF };
}

test('valid complete source frames survive all upstream fragmentation with one send', async () => {
  for (const fragment of [1, 2, 7, 4096]) {
    const f = fixture({ fragment });
    const response = await f.send(endpoint, f.init);
    assert.equal(await response.text(), transcript, `fragment ${fragment}`);
    assert.equal(f.sends(), 1);
    assert.equal(f.sentOptions().redirect, 'error');
    assert.notEqual(f.sentOptions().signal, f.authority.signal);
    assert.notEqual(f.sentOptions().signal, f.sdk.signal);
    await assert.rejects(f.send(endpoint, f.init), /boundary denied/);
    assert.equal(f.sends(), 1);
  }
});

test('every failure consumes the irreversible send permit', async () => {
  for (const [name, configure] of [
    ['foreign URL', f => ['https://foreign.invalid/v1/responses', f.init]],
    ['body mutation', f => [endpoint, { ...f.init, body: '{"new":true}' }]],
    ['foreign header', f => [endpoint, { ...f.init, headers: { ...headers, extra: 'x' } }]],
    ['missing header', f => [endpoint, { ...f.init, headers: { 'content-type': 'application/json' } }]],
    ['redirect follow', f => [endpoint, { ...f.init, redirect: 'follow' }]],
    ['no SDK signal', f => [endpoint, { ...f.init, signal: undefined }]],
    ['foreign dispatcher', f => [endpoint, { ...f.init, dispatcher: {} }]],
    ['foreign request option', f => [endpoint, { ...f.init, credentials: 'include' }]],
  ]) {
    const f = fixture();
    await assert.rejects(f.send(...configure(f)), /boundary denied/, name);
    assert.equal(f.sends(), 0, name);
    await assert.rejects(f.send(endpoint, f.init), /boundary denied/, name);
    assert.equal(f.sends(), 0, name);
  }
  const f = fixture({ failFetch: true });
  await assert.rejects(f.send(endpoint, f.init), /physical failure/);
  await assert.rejects(f.send(endpoint, f.init), /boundary denied/);
  assert.equal(f.sends(), 1);
});

test('closed SSE effects and incomplete/late terminal data fail before forwarding', async () => {
  const cases = [
    ['missing terminal', frame(created)],
    ['truncated terminal', transcript.slice(0, -1)],
    ['late event', transcript + frame(created)],
    ['DONE sentinel', transcript + 'data: [DONE]\n\n'],
    ['function call', frame({ type: 'response.output_item.added', item: { type: 'function_call', name: 'exec' } })],
    ['custom tool', frame({ type: 'response.completed', response: { output: [{ type: 'custom_tool_call' }] } })],
    ['foreign role', frame({ type: 'response.output_item.added', item: { type: 'message', role: 'user' } })],
    ['failed terminal', frame({ type: 'response.completed', response: { status: 'failed' } })],
    ['missing terminal status', frame({ type: 'response.completed', response: { output: [] } })],
    ['credential in text', frame({ type: 'response.output_text.delta', delta: 'noncredential-test-literal' })],
    ['event name mutation', frame(completed).replace('event: response.completed', 'event: response.created')],
    ['malformed UTF8', new Uint8Array([0xff, 10, 10])],
  ];
  for (const [name, content] of cases) {
    const f = fixture({ content });
    const response = await f.send(endpoint, f.init);
    await assert.rejects(response.text(), /boundary denied/, name);
    assert.equal(f.sends(), 1, name);
    // Once the producer has already closed at EOF there is nothing left to
    // cancel. This is distinct from early rejection of an open producer.
    assert.equal(f.cancelled(), name === 'missing terminal' || name === 'truncated terminal' ? 0 : 1, name);
    assert.equal(f.reachedEOF(), name === 'missing terminal' || name === 'truncated terminal', name);
  }
});

test('non-success and incompatible response headers are never parsed or resent', async () => {
  for (const options of [{ status: 429 }, { status: 500 },
    { responseHeaders: { 'content-type': 'application/json' } },
    { responseHeaders: { 'content-type': 'text/event-stream', 'content-encoding': 'gzip' } },
    { responseHeaders: { 'content-type': 'text/event-stream', extra: 'x'.repeat(4096) } }]) {
    const f = fixture(options);
    await assert.rejects(f.send(endpoint, f.init), /boundary denied/);
    assert.equal(f.cancelled(), 1);
    await assert.rejects(f.send(endpoint, f.init), /boundary denied/);
    assert.equal(f.sends(), 1);
  }
});

test('credential-bearing request or response metadata is denied without secret error text', async () => {
  assert.throws(() => createOneShotBoundary({ endpoint, expectedBody: '{"text":"noncredential-test-literal"}',
    expectedHeaders: headers, bounds: limits, signal: new AbortController().signal }, async () => { throw Error('unreachable'); }),
  error => error.message === 'prepared Pi transport boundary denied');
  const f = fixture({ responseHeaders: { 'content-type': 'text/event-stream', 'x-request-id': 'noncredential-test-literal' } });
  await assert.rejects(f.send(endpoint, f.init), error => error.message === 'prepared Pi transport boundary denied');
  assert.equal(f.sends(), 1);
  assert.equal(f.cancelled(), 1);
});

test('all declared body/SSE allocation caps cause actual denial with a valid restored control', async () => {
  for (const [field, cap] of [['responseBytes', 128], ['chunkBytes', 32], ['eventBytes', 128],
    ['lineBytes', 32], ['events', 1], ['nodes', 2], ['depth', 1], ['copyBytes', 32]]) {
    const bounds = { ...limits, [field]: cap };
    if (field === 'responseBytes' || field === 'eventBytes') {
      bounds.eventBytes = Math.min(bounds.eventBytes, cap);
      bounds.chunkBytes = Math.min(bounds.chunkBytes, cap);
      bounds.lineBytes = Math.min(bounds.lineBytes, cap);
    }
    const f = fixture({ bounds, fragment: field === 'chunkBytes' ? 64 : 1 });
    const response = await f.send(endpoint, f.init);
    await assert.rejects(response.text(), /boundary denied/, field);
    assert.equal(f.sends(), 1, field);
    assert.equal(f.cancelled(), 1, field);
  }
  const control = fixture();
  assert.equal(await (await control.send(endpoint, control.init)).text(), transcript);
});

test('caller cancellation remains effective after SDK headers and SDK signal remains linked', async () => {
  for (const authority of ['authority', 'sdk']) {
    const f = fixture({ fragment: 1 });
    const response = await f.send(endpoint, f.init);
    f[authority].abort();
    await assert.rejects(response.text(), /boundary denied/);
    assert.equal(f.sentOptions().signal.aborted, true);
    assert.equal(f.sends(), 1);
    assert.equal(f.cancelled(), 1);
  }
});

test('unknown, unsafe and nonfinite bound contracts do not construct a boundary', () => {
  for (const bounds of [{ ...limits, extra: 1 }, { ...limits, depth: 33 },
    { ...limits, responseBytes: Infinity }, { ...limits, events: 0 }, { ...limits, copyBytes: 1.5 }])
    assert.throws(() => validateBounds(bounds), /boundary denied/);
});

test('deep/wide raw JSON is rejected before constructing the parsed object graph', async t => {
  const parse = JSON.parse;
  let parses = 0;
  t.mock.method(JSON, 'parse', (...args) => { parses++; return parse(...args); });
  for (const data of ['{"type":"response.created","nested":' + '['.repeat(32) + '0' + ']'.repeat(32) + '}',
    '{"type":"response.created","wide":[' + '0,'.repeat(150) + '0]}']) {
    const f = fixture({ content: `data: ${data}\n\n` });
    const before = parses;
    await assert.rejects((await f.send(endpoint, f.init)).text(), /boundary denied/);
    assert.equal(parses, before, 'unbounded graph reached JSON.parse');
    assert.equal(f.sends(), 1);
    assert.equal(f.cancelled(), 1);
  }
  const control = fixture();
  const before = parses;
  assert.equal(await (await control.send(endpoint, control.init)).text(), transcript);
  assert.equal(parses - before, 2, 'restored genuine control did not reach the parser');
});
