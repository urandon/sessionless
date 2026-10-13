import assert from 'node:assert/strict';
import { createResponsesFramePolicy } from './policy.mjs';
import { createGuardedResponse } from './response.mjs';
import { createSSEFramer } from './framing.mjs';

// Explicit fixture numbers only; no actual SDK/provider/native qualification.
const limits = (overrides = {}) => Object.freeze({
  response_header_bytes: 256, response_body_bytes: 1048576, response_chunk_bytes: 1048576,
  sse_line_bytes: 65536, sse_event_bytes: 131072, sse_events: 1024,
  json_nodes: 50000, json_depth: 32, response_copy_work_bytes: 33554432,
  response_allocation_bytes: 16777216, ...overrides,
});
const freeze = (value) => {
  if (value && typeof value === 'object') { for (const child of Object.values(value)) freeze(child); Object.freeze(value); }
  return value;
};
const clone = (value) => JSON.parse(JSON.stringify(value));
const request = (overrides = {}) => freeze({model: 'public-model-fixture', max_output_tokens: 64,
  store: false, truncation: 'disabled', tool_choice: 'none', tools: [],
  text: {format: {type: 'json_schema', name: 'final_fixture', strict: true,
    schema: {type: 'object', properties: {answer: {type: 'string'}}, required: ['answer'], additionalProperties: false}}},
  ...overrides});
const encode = (text) => new TextEncoder().encode(text);
const wire = (value, separator = '\n\n') => encode(`data: ${JSON.stringify(value)}${separator}`);
const makePolicy = (p = limits(), req = request()) => createResponsesFramePolicy({limits: p, expectedRequest: req});
const message = (overrides = {}) => ({type: 'message', id: 'msg-public', role: 'assistant', status: 'completed',
  content: [{type: 'output_text', text: 'ordinary text mentioning function_call', annotations: []}], ...overrides});
const thinking = (overrides = {}) => ({type: 'reasoning', id: 'rs-public', summary: [{type: 'summary_text', text: 'summary'}],
  content: [{type: 'reasoning_text', text: 'reasoning fixture'}], encrypted_content: 'opaque-signature', status: 'completed', ...overrides});
const response = (status = 'in_progress', overrides = {}) => ({id: 'resp-public', created_at: 0,
  error: null, incomplete_details: status === 'incomplete' ? {reason: 'max_output_tokens'} : null,
  instructions: null, metadata: {}, model: 'public-model-fixture', object: 'response', output: [],
  parallel_tool_calls: true, temperature: 1, tool_choice: 'none', tools: [], top_p: 1, status, ...overrides});
const lifecycle = (type = 'response.completed', overrides = {}) => ({type, sequence_number: 42,
  response: response(type === 'response.created' ? 'in_progress' : type.slice(9), overrides)});
const itemEvent = (type, value = message(), index = 7) => ({type, sequence_number: 42, output_index: index, item: value});
const safeEvents = [
  'response.created', 'response.in_progress', 'response.queued', 'response.completed', 'response.incomplete',
  'response.output_item.added', 'response.output_item.done', 'response.content_part.added', 'response.content_part.done',
  'response.reasoning_summary_part.added', 'response.reasoning_summary_part.done',
  'response.reasoning_summary_text.delta', 'response.reasoning_summary_text.done',
  'response.reasoning_text.delta', 'response.reasoning_text.done', 'response.output_text.delta',
  'response.output_text.done', 'response.refusal.delta', 'response.refusal.done',
];
const deltaTypes = ['response.reasoning_summary_text.delta', 'response.reasoning_text.delta',
  'response.output_text.delta', 'response.refusal.delta'];
const event = (type) => {
  if (['response.created', 'response.in_progress', 'response.queued', 'response.completed', 'response.incomplete'].includes(type)) return lifecycle(type);
  if (type.startsWith('response.output_item.')) return itemEvent(type);
  const value = {type, sequence_number: 42, item_id: type.includes('reasoning') ? 'rs-public' : 'msg-public', output_index: 7};
  if (type.includes('reasoning_summary')) value.summary_index = 5;
  else value.content_index = 5;
  if (type.includes('reasoning_summary_part')) value.part = {type: 'summary_text', text: 'summary'};
  else if (type.includes('content_part')) value.part = {type: 'output_text', text: 'text', annotations: []};
  else if (type.endsWith('.delta')) value.delta = 'text mentions function_call';
  else if (type === 'response.refusal.done') value.refusal = 'refusal';
  else value.text = 'text';
  if (type.startsWith('response.output_text.')) value.logprobs = [];
  return value;
};
const reject = (value, p = limits(), req = request()) => {
  const policy = makePolicy(p, req);
  assert.equal(policy(value instanceof Uint8Array ? value : wire(value)), 'deny');
  assert.equal(policy(wire(lifecycle())), 'deny', 'denial must be irreversible');
};
let cases = 0;
const test = async (_name, fn) => { await fn(); cases++; };

await test('unindexed-to-indexed ID binding reuses the exact canonical identity RECORD', () => {
  const itemID = 'msg-first-canonical-public';
  const value = message({id: itemID});
  const first = wire(lifecycle('response.created', {output: [value]}));
  const later = wire(itemEvent('response.output_item.done', clone(value), 77));
  let canonical; let indexed;
  const set = Map.prototype.set;
  Map.prototype.set = function (key, entry) {
    if (key === itemID && entry && entry.kind === 'message') canonical = entry;
    if (key === 77 && entry && entry.kind === 'message') indexed = entry;
    return Reflect.apply(set, this, [key, entry]);
  };
  try {
    const policy = makePolicy();
    assert.equal(policy(first), 'continue'); assert.equal(policy(later), 'continue');
    assert.ok(canonical); assert.ok(indexed);
    assert.strictEqual(indexed, canonical, 'index must reuse the SAME record, not retain a new parsed equal ID string');
    assert.equal(canonical.id, itemID);
  } finally { Map.prototype.set = set; }
});

await test('unindexed-to-indexed independent explicit retention ledger and first overflow', () => {
  const itemID = 'msg-first-canonical-public';
  const value = message({id: itemID});
  const initial = lifecycle('response.created', {output: [value]});
  const indexed = itemEvent('response.output_item.done', clone(value), 77);
  const retained = 2 * (initial.response.id.length + itemID.length);
  // This ASCII synthetic fixture oracle counts explicit string copies only:
  // decoder/one data-slice/join6*JSON+24; decoded member keys4*length;
  // numeric-token slice2*length; first retained IDs2*length. Not a heap oracle.
  const copies = (input, retainedIDs) => {
    let keys = 0; let numeric = 0;
    const pending = [input];
    while (pending.length) {
      const current = pending.pop();
      if (typeof current === 'number') numeric += String(current).length;
      else if (current && typeof current === 'object') {
        if (Array.isArray(current)) pending.push(...current);
        else for (const [key, child] of Object.entries(current)) { keys += key.length; pending.push(child); }
      }
    }
    return 6 * JSON.stringify(input).length + 24 + 4 * keys + 2 * numeric + retainedIDs;
  };
  const first = copies(initial, retained); const second = copies(indexed, 0);
  const fixture = limits({response_body_bytes: 4096, response_chunk_bytes: 4096,
    sse_event_bytes: 1024, sse_line_bytes: 1024});
  const allocation = 2 * fixture.sse_event_bytes + Math.max(first, retained + second);
  const copyWork = 2 * fixture.response_body_bytes + fixture.sse_event_bytes + first + second;
  for (const field of ['response_allocation_bytes', 'response_copy_work_bytes']) {
    const cap = field === 'response_allocation_bytes' ? allocation : copyWork;
    for (const delta of [0, -1]) {
      const policy = makePolicy(limits({...fixture, [field]: cap + delta}));
      const result = policy(wire(initial));
      if (delta === 0) {
        assert.equal(result, 'continue'); assert.equal(policy(wire(indexed)), 'continue');
      } else if (result === 'continue') assert.equal(policy(wire(indexed)), 'deny');
      else assert.equal(result, 'deny');
    }
  }
});

await test('EXACT depth and cumulative nodes first-overflow deny BEFORE JSON.parse', () => {
  const plain = clone(request()); delete plain.text; const req = freeze(plain);
  // root1/Response2/output3/message4/content5/output_text6/annotations7.
  const nested = wire(lifecycle('response.completed', {output: [message()]}));
  const delta = wire(event('response.output_text.delta'));
  for (const [field, exact, raw, prime] of [['json_depth', 7, nested, undefined],
    // First delta has15 JSON nodes +2 retained identity/index nodes. Second
    // identical delta adds15 JSON nodes, reusing the two existing bindings.
    ['json_nodes', 32, delta, delta]]) {
    for (const offset of [0, -1]) {
      const policy = makePolicy(limits({[field]: exact + offset}), req);
      if (prime) assert.equal(policy(prime), 'continue');
      const parse = JSON.parse; let calls = 0;
      JSON.parse = (...args) => { calls++; return Reflect.apply(parse, JSON, args); };
      try {
        assert.equal(policy(raw), offset === 0 ? (prime ? 'continue' : 'terminal') : 'deny');
        assert.equal(calls, offset === 0 ? 1 : 0, `${field} must stop before materialization`);
      } finally { JSON.parse = parse; }
    }
  }
});

await test('non-toy nested frozen strict final-schema echo is accepted and mismatch denied', () => {
  const req = request({text: {format: {type: 'json_schema', name: 'complete_final', strict: true,
    description: 'Public synthetic answer and named artifacts', schema: {type: 'object',
      properties: {answer: {type: 'string', maxLength: 32000}, artifacts: {type: 'array', maxItems: 2,
        items: {type: 'object', properties: {name: {type: 'string'}, mime_type: {enum: ['text/plain', 'application/json']},
          content: {type: 'string'}}, required: ['name', 'mime_type', 'content'], additionalProperties: false}}},
      required: ['answer', 'artifacts'], additionalProperties: false,
      $defs: {labels: {type: 'array', items: {type: 'object', properties: {value: {type: 'string'}},
        required: ['value'], additionalProperties: false}}}}}}});
  const positive = lifecycle('response.completed', {text: clone(req.text), output: [message()]});
  assert.equal(makePolicy(limits(), req)(wire(positive)), 'terminal');
  const changed = clone(positive);
  changed.response.text.format.schema.properties.artifacts.items.properties.mime_type.enum[1] = 'application/other';
  reject(changed, limits(), req);
  const weakened = clone(positive); weakened.response.text.format.schema.$defs.labels.items.additionalProperties = true;
  reject(weakened, limits(), req);
});

await test('ALL19 genuine safe schema families, raw bytes never changed', () => {
  assert.equal(safeEvents.length, 19);
  for (const type of safeEvents) {
    const raw = wire(event(type)); const before = raw.slice();
    assert.equal(makePolicy()(raw), type === 'response.completed' || type === 'response.incomplete' ? 'terminal' : 'continue', type);
    assert.deepEqual(raw, before);
  }
});

await test('nested message phase/refusal/reasoning signatures and all part shapes', () => {
  for (const phase of [undefined, null, 'commentary', 'final_answer']) {
    const msg = message({content: [{type: 'refusal', refusal: 'safe refusal'}]}); if (phase !== undefined) msg.phase = phase;
    assert.equal(makePolicy()(wire(itemEvent('response.output_item.done', msg))), 'continue');
  }
  for (const encrypted_content of [undefined, null, '', 'opaque-not-base64-required']) {
    const value = thinking(); if (encrypted_content === undefined) delete value.encrypted_content;
    else value.encrypted_content = encrypted_content;
    assert.equal(makePolicy()(wire(itemEvent('response.output_item.done', value))), 'continue');
    assert.equal(makePolicy()(wire(lifecycle('response.completed', {output: [value]}))), 'terminal');
  }
  for (const part of [{type: 'output_text', text: 'x'}, {type: 'refusal', refusal: 'x'}, {type: 'reasoning_text', text: 'x'}]) {
    for (const type of ['response.content_part.added', 'response.content_part.done']) {
      assert.equal(makePolicy()(wire({...event(type), part})), 'continue');
    }
  }
});

await test('default obfuscation on EXACT4 delta envelopes, empty annotations/logprobs subset', () => {
  for (const type of deltaTypes) {
    assert.equal(makePolicy()(wire({...event(type), obfuscation: 'random padding: function_call'})), 'continue');
    for (const obfuscation of [null, 0, {}, []]) reject({...event(type), obfuscation});
  }
  for (const type of safeEvents.filter((type) => !deltaTypes.includes(type))) reject({...event(type), obfuscation: 'x'});
  reject(itemEvent('response.output_item.done', message({content: [{type: 'output_text', text: 'x', annotations: [{type: 'url_citation'}]}]})));
  reject({...event('response.output_text.delta'), logprobs: [{token: 'x'}]});
});

await test('FULL realistic raw Response metadata including store:false and optional output_text', () => {
  const req = request({instructions: 'fixed instructions', metadata: {test: 'public'}, temperature: 0.5, top_p: 0.9,
    parallel_tool_calls: false, prompt_cache_key: 'public-cache', prompt_cache_retention: 'in_memory',
    safety_identifier: 'public-safe', user: 'public-user', service_tier: 'default', reasoning: {effort: 'low', summary: 'auto'}});
  const value = lifecycle('response.completed', {output: [thinking(), message()], store: false,
    instructions: req.instructions, metadata: req.metadata, temperature: req.temperature, top_p: req.top_p,
    parallel_tool_calls: false, background: false, completed_at: 1, conversation: null, previous_response_id: null,
    prompt: null, max_output_tokens: 64, prompt_cache_key: req.prompt_cache_key, prompt_cache_retention: req.prompt_cache_retention,
    safety_identifier: req.safety_identifier, user: req.user, service_tier: 'default', reasoning: req.reasoning,
    text: req.text, top_logprobs: 0, truncation: 'disabled', usage: {input_tokens: 10, output_tokens: 5, total_tokens: 15,
      input_tokens_details: {cached_tokens: 2, cache_write_tokens: 3}, output_tokens_details: {reasoning_tokens: 2}}, output_text: 'actual wire helper optional'});
  assert.equal(makePolicy(limits(), req)(wire(value)), 'terminal');
  delete value.response.output_text; assert.equal(makePolicy(limits(), req)(wire(value)), 'terminal');
  const withNull = lifecycle('response.created', {usage: null, user: null, store: false,
    max_output_tokens: null, reasoning: null, conversation: null, background: null, completed_at: null,
    previous_response_id: null, prompt: null, service_tier: null, top_logprobs: null, truncation: null,
    prompt_cache_retention: null});
  assert.equal(makePolicy()(wire(withNull)), 'continue');
  for (const store of [true, null, 0, 'false', {}]) reject(lifecycle('response.completed', {store}));
});

await test('field-specific absent neutral defaults and explicit selected echo equality', () => {
  for (const instructions of [null, '']) for (const metadata of [null, {}]) for (const user of [null, '']) {
    assert.equal(makePolicy()(wire(lifecycle('response.created', {instructions, metadata, user}))), 'continue');
  }
  for (const parallel_tool_calls of [true, false]) assert.equal(makePolicy()(wire(lifecycle('response.created', {parallel_tool_calls}))), 'continue');
  for (const user of [null, '', 'wrong']) reject(lifecycle('response.completed', {user}), limits(), request({user: 'selected'}));
  reject(lifecycle('response.completed', {user: null}), limits(), request({user: ''}));
  reject(lifecycle('response.completed', {user: 'unexpected'}));
  reject(lifecycle('response.completed', {instructions: 'unexpected'}));
  reject(lifecycle('response.completed', {metadata: {unexpected: 'value'}}));
  for (const [key, selected, wrong] of [['instructions', 'selected', null], ['metadata', {k: 'v'}, {}],
    ['temperature', 0.5, 1], ['top_p', 0.5, 1], ['parallel_tool_calls', false, true],
    ['prompt_cache_key', 'fixed', 'other'], ['prompt_cache_retention', 'in_memory', '24h'],
    ['safety_identifier', 'fixed', 'other'], ['service_tier', 'default', 'priority'],
    ['reasoning', {effort: 'low'}, {effort: 'high'}]]) reject(lifecycle('response.completed', {[key]: wrong}), limits(), request({[key]: selected}));
  const schemaMismatch = clone(request().text); schemaMismatch.format.schema.additionalProperties = true;
  reject(lifecycle('response.completed', {text: schemaMismatch}));
});

await test('ALL forbidden exact event families deny before previous-item assumptions', () => {
  const forbidden = ['response.audio.delta', 'response.audio.done', 'response.audio_transcript.delta', 'response.audio_transcript.done',
    'response.code_interpreter_call.code.delta', 'response.code_interpreter_call.code.done',
    'response.code_interpreter_call.completed', 'response.code_interpreter_call.in_progress', 'response.code_interpreter_call.interpreting',
    'response.file_search_call.completed', 'response.file_search_call.in_progress', 'response.file_search_call.searching',
    'response.function_call_arguments.delta', 'response.function_call_arguments.done',
    'response.web_search_call.completed', 'response.web_search_call.in_progress', 'response.web_search_call.searching',
    'response.image_generation_call.completed', 'response.image_generation_call.generating', 'response.image_generation_call.in_progress', 'response.image_generation_call.partial_image',
    'response.mcp_call_arguments.delta', 'response.mcp_call_arguments.done', 'response.mcp_call.completed', 'response.mcp_call.failed', 'response.mcp_call.in_progress',
    'response.mcp_list_tools.completed', 'response.mcp_list_tools.failed', 'response.mcp_list_tools.in_progress',
    'response.custom_tool_call_input.delta', 'response.custom_tool_call_input.done', 'response.output_text.annotation.added',
    'error', 'response.failed', 'future.approval', 'future.handoff', 'future.compaction'];
  for (const type of forbidden) reject({type, sequence_number: 0, delta: 'x', obfuscation: 'x'});
});

await test('ALL forbidden output item variants at added/done and EVERY lifecycle output', () => {
  const forbidden = ['file_search_call', 'function_call', 'function_call_output', 'web_search_call', 'computer_call', 'computer_call_output',
    'tool_search_call', 'tool_search_output', 'additional_tools', 'compaction', 'image_generation_call', 'code_interpreter_call',
    'local_shell_call', 'local_shell_call_output', 'shell_call', 'shell_call_output', 'apply_patch_call', 'apply_patch_call_output',
    'mcp_call', 'mcp_list_tools', 'mcp_approval_request', 'mcp_approval_response', 'custom_tool_call', 'custom_tool_call_output', 'future_tool'];
  for (const type of forbidden) {
    const value = {type, id: 'tool-public', arguments: '{}', call_id: 'call-public', name: 'tool'};
    for (const family of ['response.output_item.added', 'response.output_item.done']) reject(itemEvent(family, value));
    for (const family of ['response.created', 'response.in_progress', 'response.queued', 'response.completed', 'response.incomplete']) {
      reject(lifecycle(family, {output: [value]}));
    }
  }
});

await test('unknown envelope/Response/nested fields and invalid mandatory shapes', () => {
  for (const type of safeEvents) reject({...event(type), unknown: null});
  for (const key of Object.keys(response())) {
    const value = lifecycle(); delete value.response[key]; reject(value);
  }
  reject(lifecycle('response.completed', {unknown: null}));
  for (const value of [message({role: 'user'}), message({status: 'queued'}), message({phase: 'other'}),
    message({extra: true}), thinking({status: null}), thinking({encrypted_content: {}}),
    thinking({summary: [{type: 'summary_text', text: 'x', extra: true}]}),
    message({content: [{type: 'input_text', text: 'x'}]})]) reject(itemEvent('response.output_item.done', value));
  for (const part of [{type: 'function_call', text: 'x'}, {type: 'reasoning_text', text: 1}, {type: 'refusal', refusal: 'x', extra: null}]) reject({...event('response.content_part.added'), part});
});

await test('terminal statuses, hidden history/capability echoes and usage arithmetic', () => {
  for (const status of ['queued', 'in_progress', 'failed', 'cancelled', undefined, null]) {
    const value = lifecycle(); if (status === undefined) delete value.response.status; else value.response.status = status; reject(value);
  }
  for (const reason of [undefined, null, 'unknown']) reject(lifecycle('response.incomplete', {incomplete_details: reason === undefined ? {} : {reason}}));
  assert.equal(makePolicy()(wire(lifecycle('response.incomplete', {incomplete_details: {reason: 'content_filter'}}))), 'terminal');
  for (const overrides of [{error: {code: 'secret', message: 'secret'}}, {background: true}, {conversation: {id: 'x'}},
    {previous_response_id: 'x'}, {prompt: {id: 'x'}}, {tools: [{type: 'web_search'}]}, {tool_choice: 'auto'},
    {model: 'other'}, {truncation: 'auto'}, {max_output_tokens: 65}, {top_logprobs: 1}, {temperature: 3}, {top_p: -1}]) reject(lifecycle('response.completed', overrides));
  const good = {input_tokens: 0, output_tokens: 0, total_tokens: 0, input_tokens_details: {cached_tokens: 0}, output_tokens_details: {reasoning_tokens: 0}};
  assert.equal(makePolicy()(wire(lifecycle('response.completed', {usage: good}))), 'terminal');
  for (const overrides of [{input_tokens: -1}, {total_tokens: 1}, {output_tokens: 0.5},
    {input_tokens: Number.MAX_SAFE_INTEGER, output_tokens: 1, total_tokens: Number.MAX_SAFE_INTEGER},
    {input_tokens_details: {cached_tokens: 1}}, {input_tokens_details: {cached_tokens: 0, cache_write_tokens: 1}},
    {output_tokens_details: {reasoning_tokens: 1}}, {input_tokens_details: {cached_tokens: 0, unknown: 0}}]) reject(lifecycle('response.completed', {usage: {...good, ...overrides}}));
});

await test('stable response/item/index identities, sparse and done-without-added compatible', () => {
  const policy = makePolicy();
  assert.equal(policy(wire(itemEvent('response.output_item.done', message(), 700))), 'continue');
  assert.equal(policy(wire(lifecycle('response.created'))), 'continue');
  assert.equal(policy(wire(lifecycle('response.completed', {output: [message()]}))), 'terminal');
  assert.equal(policy(wire(lifecycle())), 'deny');
  for (const later of [itemEvent('response.output_item.done', thinking({id: 'msg-public'})),
    itemEvent('response.output_item.done', message({id: 'other'})),
    itemEvent('response.output_item.done', message(), 8)]) {
    const scoped = makePolicy(); scoped(wire(itemEvent('response.output_item.added')));
    assert.equal(scoped(wire(later)), 'deny');
  }
  const scoped = makePolicy(); scoped(wire(lifecycle('response.created')));
  assert.equal(scoped(wire(lifecycle('response.completed', {id: 'other'}))), 'deny');
  for (const sequence_number of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1]) reject({...event('response.output_text.delta'), sequence_number});
});

await test('fatal UTF8/BOM/NUL, exact fields, multiline, comments and named events', () => {
  for (const invalid of [Uint8Array.of(0xc0, 0xaf, 10, 10), Uint8Array.of(0xed, 0xa0, 0x80, 10, 10), Uint8Array.of(0xe2, 0x82, 10, 10)]) reject(invalid);
  const safe = JSON.stringify(event('response.output_text.delta'));
  for (const raw of [`\ufeffdata: ${safe}\n\n`, `: comment\n\ufeffdata: ${safe}\n\n`,
    `data: ${safe}\0\n\n`, `id: x\ndata: ${safe}\n\n`, `retry: 1\ndata: ${safe}\n\n`,
    `Data: ${safe}\n\n`, ` event: x\ndata: ${safe}\n\n`, `event: wrong\ndata: ${safe}\n\n`,
    `event: response.output_text.delta\nevent: response.output_text.delta\ndata: ${safe}\n\n`,
    'event: x\n\n', 'data\n\n', `data: ${safe}\n`, ': no-complete-boundary\n']) reject(encode(raw));
  for (const separator of ['\n\n', '\r\r', '\r\n\r\n']) {
    assert.equal(makePolicy()(encode(`: comment${separator}`)), 'continue');
    assert.equal(makePolicy()(encode(`event: response.output_text.delta${separator.slice(0, separator.length / 2)}data: ${safe}${separator}`)), 'continue');
  }
  const multiline = `data: {\ndata: "type":"response.output_text.delta",\ndata: "sequence_number":42,"item_id":"msg-public","output_index":7,"content_index":5,"delta":"é function_call"}\n\n`;
  assert.equal(makePolicy()(encode(multiline)), 'continue');
  assert.equal(makePolicy()(encode(`event:\ndata: ${safe}\n\n`)), 'continue');
});

await test('every semantic unit inside mixed raw frame, no safe prefix/effect/terminal bypass', () => {
  const safe = JSON.stringify(event('response.output_text.delta'));
  const bad = JSON.stringify({type: 'response.function_call_arguments.delta', sequence_number: 44, delta: '{}'});
  assert.equal(makePolicy()(encode(`data: ${safe}\n\rdata: ${safe}\n\n`)), 'continue');
  reject(encode(`data: ${safe}\n\rdata: ${bad}\n\n`));
  const terminal = JSON.stringify(lifecycle());
  reject(encode(`data: ${terminal}\n\rdata: ${bad}\n\n`));
  reject(encode(`data: ${terminal}\n\rdata: [DONE]\n\n`));
  for (const marker of ['[DONE]', '[DONE]spoof', ' [DONE]', '[DONE] ']) reject(encode(`data: ${marker}\n\n`));
});

await test('complete JSON grammar and DECODED duplicate keys before JSON.parse', () => {
  const safe = JSON.stringify(event('response.output_text.delta'));
  const duplicate = safe.replace('"type":', '"\\u0074ype":"response.output_text.delta","type":');
  reject(encode(`data: ${duplicate}\n\n`));
  const nested = JSON.stringify(lifecycle('response.completed', {output: [message()]}));
  reject(encode(`data: ${nested.replace('"role":"assistant"', '"role":"assistant","r\\u006fle":"assistant"')}\n\n`));
  for (const raw of ['{} garbage', '{"x":01}', '{"x":1.}', '{"x":1e}', '{"x":1e999}', '{"x":-}',
    '{"x":true,}', '{"x":[1,]}', '{"x":"\\q"}', '{"x":"\\uZZZZ"}', '{"x":undefined}',
    '{"x":NaN}', '{"x":Infinity}', '{"x":/* comment */1}', '{"x":"unterminated}', '[{}]']) reject(encode(`data: ${raw}\n\n`));
  const equivalent = safe.replace('"type":', '"\\u0074ype":');
  assert.equal(makePolicy()(encode(`data: ${equivalent}\n\n`)), 'continue');
});

await test('preparse failures never invoke JSON.parse and synchronous reentry closes outer work', () => {
  const raw = wire(event('response.output_text.delta'));
  const bad = encode(`data: ${JSON.stringify(event('response.output_text.delta')).replace('"type":', '"type":"response.output_text.delta","type":')}\n\n`);
  const before = JSON.parse; let calls = 0;
  JSON.parse = (...args) => { calls++; return before(...args); };
  try {
    assert.equal(makePolicy()(bad), 'deny'); assert.equal(calls, 0);
    assert.equal(makePolicy()(raw), 'continue'); assert.equal(calls, 1);
  } finally { JSON.parse = before; }
  const scoped = makePolicy(); const reentrant = raw.slice();
  Object.defineProperty(reentrant, 'byteLength', {get() {
    assert.equal(scoped(raw), 'deny'); return raw.byteLength;
  }});
  assert.equal(scoped(reentrant), 'deny'); assert.equal(scoped(raw), 'deny');
});

await test('immutable canonical limits and bounded own-data snapshot configuration', () => {
  for (const options of [undefined, null, {}, {limits: {...limits()}, expectedRequest: request()},
    {limits: limits({json_nodes: 0}), expectedRequest: request()},
    {limits: limits({response_copy_work_bytes: 1}), expectedRequest: request()},
    {limits: limits({response_allocation_bytes: 1}), expectedRequest: request()},
    {limits: limits(), expectedRequest: {...request()}},
    {limits: limits(), expectedRequest: request({store: true})},
    {limits: limits(), expectedRequest: request({tools: [{type: 'web_search'}]})},
    {limits: limits(), expectedRequest: request({text: {format: {type: 'text'}}})}]) {
    assert.throws(() => createResponsesFramePolicy(options), /^PiResponsePolicyError: pi_response_policy_configuration_invalid$/);
  }
  const accessor = Object.freeze({...limits(), get json_nodes() { throw new Error('not invoked'); }});
  assert.throws(() => makePolicy(accessor), /^PiResponsePolicyError: pi_response_policy_configuration_invalid$/);
  const snapshot = Object.freeze({...request(), get model() { throw new Error('not invoked'); }});
  assert.throws(() => makePolicy(limits(), snapshot), /^PiResponsePolicyError: pi_response_policy_configuration_invalid$/);
  const cycle = {}; cycle.self = cycle; Object.freeze(cycle);
  assert.throws(() => makePolicy(limits({json_depth: 8}), Object.freeze({...request(), metadata: cycle})), /^PiResponsePolicyError: pi_response_policy_configuration_invalid$/);
});

await test('depth/line/frame/body/events/cumulative JSON-node/copy/allocation exact thresholds', () => {
  const raw = wire(event('response.output_text.delta'));
  const plain = clone(request()); delete plain.text; const req = freeze(plain);
  for (const [field, low, high] of [['sse_line_bytes', 5, 65536]]) {
    reject(raw, limits({[field]: low}), req);
    assert.equal(makePolicy(limits({[field]: high}), req)(raw), 'continue');
  }
  reject(raw, limits({sse_event_bytes: raw.length - 1, sse_line_bytes: raw.length - 1}), req);
  const small = limits({sse_events: 1}); const policy = makePolicy(small);
  assert.equal(policy(raw), 'continue'); assert.equal(policy(raw), 'deny');
  const findThreshold = (field, from, to) => {
    while (from < to) {
      const middle = Math.floor((from + to) / 2);
      let ok = false;
      try { ok = makePolicy(limits({[field]: middle}), req)(raw) === 'continue'; } catch { /* configuration cutoff */ }
      if (ok) to = middle; else from = middle + 1;
    }
    return from;
  };
  for (const [field, maximum] of [['json_nodes', 50000], ['response_copy_work_bytes', 33554432], ['response_allocation_bytes', 16777216]]) {
    const threshold = findThreshold(field, 1, maximum);
    // Independent ASCII fixture ledger: decoded frame2*(JSON+8), field8,
    // data slice/join4*JSON, decoded seven keys4*64, numeric tokens8,
    // retained ID20. Parsed objects/GC/native are intentionally NOT measured.
    const scannerCopies = 6 * JSON.stringify(event('response.output_text.delta')).length + 308;
    const expectedThreshold = field === 'json_nodes' ? 17 : field === 'response_copy_work_bytes' ?
      2 * limits().response_body_bytes + limits().sse_event_bytes + scannerCopies :
      2 * limits().sse_event_bytes + scannerCopies;
    assert.equal(threshold, expectedThreshold, `${field} independent explicit component ledger`);
    assert.equal(makePolicy(limits({[field]: threshold}), req)(raw), 'continue', `${field} exact`);
    let bad;
    try { bad = makePolicy(limits({[field]: threshold - 1}), req)(raw); }
    catch (error) { assert.equal(error.message, 'pi_response_policy_configuration_invalid'); bad = 'deny'; }
    assert.equal(bad, 'deny', `${field} first overflow`);
    if (field === 'json_nodes') {
      const cumulative = makePolicy(limits({json_nodes: threshold}), req);
      assert.equal(cumulative(raw), 'continue'); assert.equal(cumulative(raw), 'deny');
    }
  }
  const bodyPolicy = limits({response_body_bytes: raw.length, sse_event_bytes: raw.length, sse_line_bytes: raw.length});
  const bounded = makePolicy(bodyPolicy, req);
  assert.equal(bounded(raw), 'continue'); assert.equal(bounded(raw), 'deny');
});

// Source-derived unit oracle for delayed lone-CR line dispatch. This is NOT an
// SDK import/runtime proof. Keep pending CR between raw chunks, flush at EOF.
const lineOracle = () => {
  let current = ''; let pendingCR = false;
  const lines = [];
  const push = (text) => {
    for (const char of text) {
      if (pendingCR) {
        lines.push(current); current = ''; pendingCR = false;
        if (char === '\n') continue;
      }
      if (char === '\r') pendingCR = true;
      else if (char === '\n') { lines.push(current); current = ''; }
      else current += char;
    }
  };
  return {push, flush() { if (pendingCR || current) push('\n'); }, lines};
};
const semanticTypes = (lines) => {
  const result = []; let data = [];
  for (const line of lines) {
    if (!line) { if (data.length) result.push(JSON.parse(data.join('\n')).type); data = []; }
    else if (line.startsWith('data:')) data.push(line.slice(5).replace(/^ /, ''));
  }
  return result;
};
await test('CRCR across raw frames LF/CR/nonnewline and terminal EOF differential controls', () => {
  const first = `data: ${JSON.stringify(event('response.output_text.delta'))}\r\r`;
  for (const prefix of ['\n', '\r', '']) {
    const second = `${prefix}data: ${JSON.stringify(lifecycle())}\n\n`;
    const oracle = lineOracle(); oracle.push(first);
    assert.deepEqual(semanticTypes(oracle.lines), [], 'SDK-like pending empty CR boundary');
    oracle.push(second); oracle.flush();
    assert.deepEqual(semanticTypes(oracle.lines), ['response.output_text.delta', 'response.completed']);
    const policy = makePolicy(); assert.equal(policy(encode(first)), 'continue'); assert.equal(policy(encode(second)), 'terminal');
    const badPolicy = makePolicy(); assert.equal(badPolicy(encode(first)), 'continue');
    assert.equal(badPolicy(encode(`${prefix}data: ${JSON.stringify({type: 'response.function_call_arguments.delta', sequence_number: 1})}\n\n`)), 'deny');
  }
  const terminal = `data: ${JSON.stringify(lifecycle())}\r\r`;
  const oracle = lineOracle(); oracle.push(terminal); assert.deepEqual(semanticTypes(oracle.lines), []);
  oracle.flush(); assert.deepEqual(semanticTypes(oracle.lines), ['response.completed']);
  assert.equal(makePolicy()(encode(terminal)), 'terminal');
});

const compose = (chunks, validateFrame, stall = false) => {
  const observed = {pulls: 0, cancels: 0};
  let stalled; const pending = new Promise((done) => { stalled = done; });
  const original = new AbortController(); const sdk = new AbortController(); const fetch = new AbortController();
  const source = new ReadableStream({pull(controller) {
    observed.pulls++; if (chunks.length) controller.enqueue(chunks.shift());
    else if (stall) stalled(); else controller.close();
  }, cancel() { observed.cancels++; }}, {highWaterMark: 0});
  const guarded = createGuardedResponse({response: new Response(source, {headers: {'content-type': 'text/event-stream'}}),
    limits: limits(), originalSignal: original.signal, sdkSignal: sdk.signal, fetchController: fetch, validateFrame});
  return {guarded, source, original, sdk, fetch, observed, stalled: pending};
};
await test('real response host no-forward on whole-frame deny and terminal no-drain', async () => {
  const safe = JSON.stringify(event('response.output_text.delta'));
  const effect = JSON.stringify({type: 'response.function_call_arguments.delta', sequence_number: 1});
  const denied = compose([encode(`data: ${safe}\n\rdata: ${effect}\n\n`)], makePolicy());
  await assert.rejects(denied.guarded.body.getReader().read(), /^PiResponseGuardError: pi_response_frame_denied$/);
  assert.equal(denied.observed.cancels, 1); assert.equal(denied.fetch.signal.aborted, true); assert.equal(denied.source.locked, false);
  const terminal = wire(lifecycle()); const good = compose([terminal, encode('never read\n\n')], makePolicy());
  const reader = good.guarded.body.getReader(); assert.deepEqual((await reader.read()).value, terminal);
  assert.equal((await reader.read()).done, true); assert.equal(good.observed.pulls, 1);
  assert.equal(good.observed.cancels, 1); assert.equal(good.fetch.signal.aborted, true);
});

await test('upstream tiny fragments preserve original frames through actual host', async () => {
  const first = wire(event('response.output_text.delta')); const terminal = wire(lifecycle());
  const chunks = [...first, ...terminal].map((byte) => Uint8Array.of(byte));
  const fixture = compose(chunks, makePolicy()); const reader = fixture.guarded.body.getReader();
  assert.deepEqual((await reader.read()).value, first); assert.deepEqual((await reader.read()).value, terminal);
  assert.equal((await reader.read()).done, true); assert.equal(fixture.observed.cancels, 1);
  // Existing raw framer also proves hidden mixed units are not separate frames.
  const framer = createSSEFramer(limits()); const mixed = encode(`data: ${JSON.stringify(event('response.output_text.delta'))}\n\rdata: ${JSON.stringify(lifecycle())}\n\n`);
  assert.equal([...framer.feed(mixed)].length, 1); framer.finish();
});

await test('actual policy/response host original and SDK pending-body cancellation', async () => {
  for (const signal of ['original', 'sdk']) {
    const first = wire(event('response.output_text.delta'));
    const fixture = compose([first], makePolicy(), true); const reader = fixture.guarded.body.getReader();
    assert.deepEqual((await reader.read()).value, first);
    const next = reader.read(); await fixture.stalled;
    fixture[signal].abort(new Error('unexposed private fixture reason'));
    await assert.rejects(next, /^PiResponseGuardError: pi_response_aborted$/);
    assert.equal(fixture.observed.cancels, 1); assert.equal(fixture.fetch.signal.aborted, true);
    assert.equal(fixture.source.locked, false);
  }
});

console.log(JSON.stringify({passed: true, cases, safeEventFamilies: 19,
  scope: 'policy_and_existing_response_host_unit_only_no_sdk_or_network'}));
