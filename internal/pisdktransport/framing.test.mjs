import assert from 'node:assert/strict';
import { createSSEFramer } from './framing.mjs';

const encode = (value) => new TextEncoder().encode(value);
const limits = (overrides = {}) => Object.freeze({
  response_body_bytes: 4096, response_chunk_bytes: 4096,
  sse_line_bytes: 512, sse_event_bytes: 1024, sse_events: 64,
  response_copy_work_bytes: 8192, response_allocation_bytes: 2048,
  ...overrides,
});
const denied = (fn) => assert.throws(fn, /^Error: pi_sse_frame_denied$/);
let cases = 0;
const test = (_name, fn) => { fn(); cases++; };

test('all split points preserve real SDK separator bytes', () => {
  for (const separator of ['\n\n', '\r\r', '\r\n\r\n']) {
    const first = encode(`event: response.output_text.delta${separator}`);
    const second = encode(`: heartbeat${separator}`);
    const wire = new Uint8Array(first.length + second.length);
    wire.set(first); wire.set(second, first.length);
    for (let split = 0; split <= wire.length; split++) {
      const framer = createSSEFramer(limits());
      const frames = [...framer.feed(wire.subarray(0, split)), ...framer.feed(wire.subarray(split))];
      framer.finish();
      assert.deepEqual(frames, [first, second], `separator/split=${JSON.stringify(separator)}/${split}`);
    }
    const tiny = createSSEFramer(limits());
    const frames = [];
    for (const byte of wire) frames.push(...tiny.feed(Uint8Array.of(byte)));
    tiny.finish(); assert.deepEqual(frames, [first, second]);
  }
});

test('multiline data, comments and blank frames remain original', () => {
  const framer = createSSEFramer(limits());
  const expected = ['data: {\ndata: "text": "x"}\n\n', ': comment\n\n', '\n\n'].map(encode);
  assert.deepEqual([...framer.feed(encode('data: {\ndata: "text": "x"}\n\n: comment\n\n\n\n'))], expected);
  framer.finish();
});

test('exact legitimate limits, including linear copy work', () => {
  const wire = encode('1234\n\n');
  const framer = createSSEFramer(limits({response_body_bytes: 6, response_chunk_bytes: 6,
    sse_line_bytes: 4, sse_event_bytes: 6, sse_events: 1,
    response_copy_work_bytes: 12, response_allocation_bytes: 12}));
  const frames = [];
  for (const byte of wire) frames.push(...framer.feed(Uint8Array.of(byte)));
  framer.finish(); assert.deepEqual(frames, [wire]);
});

test('each +1 overflow closes permanently', () => {
  const cases = [
    [limits({response_chunk_bytes: 5}), '1234\n\n'],
    [limits({response_body_bytes: 5, response_chunk_bytes: 5, sse_line_bytes: 4, sse_event_bytes: 5}), '1234\n\n'],
    [limits({sse_line_bytes: 3}), '1234\n\n'],
    [limits({sse_line_bytes: 4, sse_event_bytes: 5}), '1234\n\n'],
    [limits({sse_events: 1}), '\n\n\n\n'],
    [limits({response_body_bytes: 6, response_chunk_bytes: 6, sse_line_bytes: 4,
      sse_event_bytes: 6, response_copy_work_bytes: 11}), '1234\n\n'],
  ];
  for (const [policy, wire] of cases) {
    const framer = createSSEFramer(policy);
    denied(() => [...framer.feed(encode(wire))]);
    denied(() => [...framer.feed(encode('\n\n'))]);
    denied(() => framer.finish());
  }
});

test('cumulative body accounting survives frame boundaries and empty fragments', () => {
  const framer = createSSEFramer(limits({response_body_bytes: 5, response_chunk_bytes: 3,
    sse_line_bytes: 1, sse_event_bytes: 3}));
  assert.deepEqual([...framer.feed(encode('x\n\n'))], [encode('x\n\n')]);
  assert.deepEqual([...framer.feed(new Uint8Array())], []);
  assert.deepEqual([...framer.feed(encode('x'))], []);
  denied(() => [...framer.feed(encode('\n\n'))]);
  denied(() => framer.finish());
});

test('deterministic partitions, mixed separator tails and reusable owned outputs', () => {
  const expected = ['x\r\n\n', 'y\n\n', 'z\r\n\r\n', ':\r\r', '\n\n'].map(encode);
  const wire = encode('x\r\n\ny\n\nz\r\n\r\n:\r\r\n\n');
  // Reproducible small cohort; no wall clock, sleep or scheduling oracle.
  for (let seed = 1; seed <= 128; seed++) {
    let state = seed;
    const framer = createSSEFramer(limits());
    const actual = [];
    let offset = 0;
    while (offset < wire.length) {
      state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
      const end = Math.min(wire.length, offset + 1 + state % 7);
      actual.push(...framer.feed(wire.subarray(offset, end)));
      offset = end;
    }
    framer.finish(); assert.deepEqual(actual, expected, `seed=${seed}`);
  }
});

test('copy work and event counts remain cumulative across separate feeds', () => {
  const copy = createSSEFramer(limits({response_body_bytes: 5, response_chunk_bytes: 3,
    sse_line_bytes: 1, sse_event_bytes: 3, response_copy_work_bytes: 7}));
  assert.deepEqual([...copy.feed(encode('x\n\n'))], [encode('x\n\n')]);
  denied(() => [...copy.feed(encode('\n\n'))]);
  const events = createSSEFramer(limits({sse_events: 1}));
  assert.deepEqual([...events.feed(encode('\n\n'))], [encode('\n\n')]);
  denied(() => [...events.feed(encode('\n\n'))]);
});

test('unexpected input type and explicit close cannot continue', () => {
  const bad = createSSEFramer(limits());
  denied(() => [...bad.feed('not bytes')]);
  denied(() => [...bad.feed(encode('\n\n'))]);
  const closed = createSSEFramer(limits());
  closed.close(); closed.close();
  denied(() => [...closed.feed(new Uint8Array())]);
});

test('repeated cleanup clears the owned arena exactly once', () => {
  const descriptor = Object.getOwnPropertyDescriptor(Uint8Array.prototype, 'fill');
  const original = Uint8Array.prototype.fill;
  let clears = 0;
  Object.defineProperty(Uint8Array.prototype, 'fill', {
    configurable: true, writable: true,
    value(...args) { clears++; return Reflect.apply(original, this, args); },
  });
  try {
    const framer = createSSEFramer(limits());
    framer.close(); framer.close();
    denied(() => [...framer.feed(new Uint8Array())]);
    denied(() => framer.finish());
    assert.equal(clears, 1, 'cleanup must clear once, not repeat event-sized work');
  } finally {
    if (descriptor) Object.defineProperty(Uint8Array.prototype, 'fill', descriptor);
    else Reflect.deleteProperty(Uint8Array.prototype, 'fill');
  }
});

test('partial EOF is not completion, empty fragments do not reset accounting', () => {
  const framer = createSSEFramer(limits());
  assert.deepEqual([...framer.feed(encode('data: unfinished'))], []);
  assert.deepEqual([...framer.feed(new Uint8Array())], []);
  denied(() => framer.finish());
  denied(() => [...framer.feed(encode('\n\n'))]);
});

test('arena reuse cannot mutate a delivered frame', () => {
  const framer = createSSEFramer(limits());
  const iterator = framer.feed(encode('data: one\n\ndata: two\n\n'));
  const first = iterator.next().value;
  first[0] = 0;
  const second = iterator.next().value;
  assert.deepEqual(second, encode('data: two\n\n'));
  assert.equal(iterator.next().done, true);
  framer.finish(); assert.equal(first[0], 0);
});

test('reentry and early consumer return cannot replay bytes', () => {
  const reentrant = createSSEFramer(limits());
  const iterator = reentrant.feed(encode('\n\n\n\n'));
  assert.deepEqual(iterator.next().value, encode('\n\n'));
  denied(() => [...reentrant.feed(encode('\n\n'))]);
  denied(() => iterator.next());
  const stopped = createSSEFramer(limits());
  const pending = stopped.feed(encode('\n\n\n\n'));
  pending.next(); pending.return();
  denied(() => [...stopped.feed(encode('\n\n'))]);
});

test('invalid absent, mutable, accessor, nonfinite and incoherent limits deny', () => {
  for (const policy of [undefined, {}, {...limits()}, limits({sse_events: 0}),
    limits({sse_event_bytes: NaN}), limits({response_allocation_bytes: 1}),
    Object.freeze({...limits(), get sse_events() { throw new Error('must not invoke getter'); }})]) {
    assert.throws(() => createSSEFramer(policy), /^Error: pi_sse_limits_invalid$/);
  }
});
console.log(JSON.stringify({passed: true, cases, scope: 'byte_framing_unit_only_no_sdk_or_network'}));
