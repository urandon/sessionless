import { createGuardedResponse } from './response.mjs';
import { createResponsesFramePolicy } from './policy.mjs';

// PRIVATE isolated sink. No SDK import, source phase, registration, clock or
// global fetch. The zero-argument parent seam in units is NOT owner authority.
// These counters cover declared logical payload/copies/nodes, NOT V8/native
// objects, GC, SDK copies or the original complete pre-Issue allocation ledger.
const fail = (code) => {
  const error = new Error(code);
  error.name = 'PiPhysicalFetchError';
  return error;
};
const deny = () => { throw fail('pi_physical_request_denied'); };
const record = (value) => value !== null && typeof value === 'object' &&
  !Array.isArray(value) && [Object.prototype, null].includes(Object.getPrototypeOf(value));
const safe = (value) => Number.isSafeInteger(value) && value >= 0;
const positive = (value) => safe(value) && value > 0;
const own = (value, name) => Object.hasOwn(value, name);
const data = (value, name) => {
  const d = Object.getOwnPropertyDescriptor(value, name);
  if (!d || !own(d, 'value')) deny();
  return d.value;
};
const closed = (value, required, allowed = required) => record(value) &&
  required.every((key) => own(value, key)) &&
  Reflect.ownKeys(value).every((key) => typeof key === 'string' && allowed.includes(key));
const multiply = (n, k) => {
  if (!safe(n) || !positive(k) || n > Math.floor(Number.MAX_SAFE_INTEGER / k)) deny();
  return n * k;
};
const sum = (...values) => {
  let total = 0;
  for (const n of values) { if (!safe(n) || n > Number.MAX_SAFE_INTEGER - total) deny(); total += n; }
  return total;
};
const subtract = (n, ...charges) => {
  for (const cost of charges) { if (!safe(cost) || cost > n) deny(); n -= cost; }
  return n;
};
function utf8(text, cap) {
  if (typeof text !== 'string') deny();
  let bytes = 0;
  for (let i = 0; i < text.length; i++) {
    const c = text.charCodeAt(i);
    if (c < 128) bytes++;
    else if (c < 2048) bytes += 2;
    else if (c >= 0xd800 && c <= 0xdbff) {
      const next = text.charCodeAt(++i);
      if (!(next >= 0xdc00 && next <= 0xdfff)) deny();
      bytes += 4;
    } else { if (c >= 0xdc00 && c <= 0xdfff) deny(); bytes += 3; }
    if (bytes > cap) deny();
  }
  return bytes;
}
const limitNames = ['canonical_input_bytes', 'native_request_bytes', 'pre_send_allocation_bytes',
  'request_header_bytes', 'response_header_bytes', 'response_body_bytes', 'response_chunk_bytes',
  'sse_line_bytes', 'sse_event_bytes', 'sse_events', 'json_nodes', 'response_copy_work_bytes',
  'response_allocation_bytes', 'process_memory_bytes', 'process_disk_bytes', 'process_frame_bytes',
  'process_stdout_bytes', 'process_events', 'json_depth', 'raw_final_bytes', 'structured_final_bytes',
  'answer_bytes', 'artifact_bytes', 'artifact_total_bytes'];
const needed = ['native_request_bytes', 'pre_send_allocation_bytes', 'request_header_bytes',
  'response_header_bytes', 'response_body_bytes', 'response_chunk_bytes', 'sse_line_bytes',
  'sse_event_bytes', 'sse_events', 'json_nodes', 'response_copy_work_bytes',
  'response_allocation_bytes', 'process_memory_bytes', 'json_depth'];
const echoFields = ['model', 'max_output_tokens', 'store', 'truncation', 'tool_choice',
  'instructions', 'metadata', 'text', 'reasoning', 'temperature', 'top_p', 'parallel_tool_calls',
  'prompt_cache_key', 'prompt_cache_retention', 'safety_identifier', 'user', 'service_tier'];
const requestFields = [...echoFields, 'input', 'stream', 'include', 'prompt_cache_options'];
const headerNames = ['accept', 'content-type', 'authorization', 'user-agent', 'x-stainless-lang',
  'x-stainless-package-version', 'x-stainless-os', 'x-stainless-arch', 'x-stainless-runtime',
  'x-stainless-runtime-version', 'x-stainless-retry-count', 'x-stainless-timeout'];

// Frozen own JSON, with the same container/key/scalar node convention used by
// preparse grammar and F10. Repeated appearances are conservatively charged.
function shape(value, limits, charge) {
  const pending = [{ value, depth: 1 }];
  let nodes = 0; let bytes = 0;
  const text = (s) => { utf8(s, limits.native_request_bytes); const n = multiply(s.length, 2); charge(n); bytes = sum(bytes, n); };
  while (pending.length) {
    if (++nodes > limits.json_nodes) deny();
    const entry = pending.pop(); const v = entry.value;
    if (typeof v === 'string') { text(v); continue; }
    if (v === null || typeof v === 'boolean' || (typeof v === 'number' && Number.isFinite(v))) continue;
    if ((!record(v) && !Array.isArray(v)) || !Object.isFrozen(v) || entry.depth > limits.json_depth) deny();
    const keys = Reflect.ownKeys(v);
    if (keys.length > limits.json_nodes - nodes + 1) deny();
    if (Array.isArray(v)) {
      const length = data(v, 'length');
      if (!safe(length) || keys.length !== length + 1) deny();
      for (let i = 0; i < length; i++) {
        const child = data(v, String(i));
        pending.push({ value: child, depth: entry.depth + (child !== null && typeof child === 'object' ? 1 : 0) });
      }
    } else {
      for (const key of keys) {
        if (typeof key !== 'string' || ++nodes > limits.json_nodes) deny();
        text(key); const child = data(v, key);
        pending.push({ value: child, depth: entry.depth + (child !== null && typeof child === 'object' ? 1 : 0) });
      }
    }
    if (pending.length > limits.json_nodes - nodes) deny();
  }
  return { nodes, bytes };
}
function equal(left, right, cap) {
  const pending = [[left, right]];
  let nodes = 0;
  while (pending.length) {
    if (++nodes > cap) deny();
    const [a, b] = pending.pop();
    if (a === b) continue;
    if (a === null || b === null || typeof a !== 'object' || typeof b !== 'object' ||
        Array.isArray(a) !== Array.isArray(b)) return false;
    const keys = Object.keys(a);
    if (keys.length !== Object.keys(b).length) return false;
    for (const key of keys) {
      if (++nodes > cap || !own(b, key)) return false;
      pending.push([a[key], b[key]]);
    }
    if (pending.length > cap - nodes) deny();
  }
  return true;
}
function input(value) {
  if (!Array.isArray(value) || value.length === 0) deny();
  const strings = (part, kind, field) => closed(part, ['type', field]) && part.type === kind && typeof part[field] === 'string';
  for (const item of value) {
    if (closed(item, ['role', 'content']) && ['system', 'developer'].includes(item.role) && typeof item.content === 'string') continue;
    if (closed(item, ['role', 'content']) && item.role === 'user' && Array.isArray(item.content) && item.content.length) {
      for (const part of item.content) {
        if (strings(part, 'input_text', 'text')) continue;
        if (!closed(part, ['type', 'detail', 'image_url']) || part.type !== 'input_image' || part.detail !== 'auto' ||
            typeof part.image_url !== 'string' || !/^data:image\/(png|jpeg);base64,[A-Za-z0-9+/]+={0,2}$/.test(part.image_url) ||
            (part.image_url.length - part.image_url.indexOf(',') - 1) % 4 !== 0) deny();
      }
      continue;
    }
    if (closed(item, ['type', 'id', 'role', 'status', 'content'], ['type', 'id', 'role', 'status', 'content', 'phase']) &&
        item.type === 'message' && item.role === 'assistant' && item.status === 'completed' &&
        typeof item.id === 'string' && item.id.length > 0 && item.id.length <= 64 && Array.isArray(item.content) &&
        (!own(item, 'phase') || ['commentary', 'final_answer'].includes(item.phase))) {
      for (const part of item.content) if (!closed(part, ['type', 'text', 'annotations']) ||
          part.type !== 'output_text' || typeof part.text !== 'string' || !Array.isArray(part.annotations) || part.annotations.length) deny();
      continue;
    }
    if (closed(item, ['type', 'id', 'summary'], ['type', 'id', 'summary', 'content', 'encrypted_content', 'status']) &&
        item.type === 'reasoning' && typeof item.id === 'string' && item.id.length > 0 && Array.isArray(item.summary) &&
        (!own(item, 'encrypted_content') || item.encrypted_content === null || typeof item.encrypted_content === 'string') &&
        (!own(item, 'status') || ['in_progress', 'completed', 'incomplete'].includes(item.status))) {
      for (const part of item.summary) if (!strings(part, 'summary_text', 'text')) deny();
      if (own(item, 'content')) { if (!Array.isArray(item.content)) deny(); for (const part of item.content) if (!strings(part, 'reasoning_text', 'text')) deny(); }
      continue;
    }
    deny(); // tool/effect/unknown or silently downgraded histories never pass
  }
}
function request(value, schema, cap) {
  if (!closed(value, ['model', 'input', 'stream', 'store', 'max_output_tokens', 'truncation', 'tool_choice', 'text'], requestFields) ||
      typeof value.model !== 'string' || !value.model || value.stream !== true || value.store !== false ||
      !positive(value.max_output_tokens) || value.max_output_tokens < 16 || value.truncation !== 'disabled' || value.tool_choice !== 'none') deny();
  input(value.input);
  if (!closed(value.text, ['format'], ['format', 'verbosity']) ||
      (own(value.text, 'verbosity') && !['low', 'medium', 'high'].includes(value.text.verbosity))) deny();
  const format = value.text.format;
  if (!closed(format, ['type', 'name', 'schema', 'strict'], ['type', 'name', 'schema', 'strict', 'description']) ||
      format.type !== 'json_schema' || typeof format.name !== 'string' || !/^[A-Za-z0-9_-]{1,64}$/.test(format.name) ||
      format.strict !== true || !record(format.schema) || !equal(format.schema, schema, cap) ||
      (own(format, 'description') && typeof format.description !== 'string')) deny();
  if (own(value, 'include') && (!Array.isArray(value.include) || value.include.length !== 1 || value.include[0] !== 'reasoning.encrypted_content')) deny();
  if (own(value, 'prompt_cache_options') && (!closed(value.prompt_cache_options, ['mode']) || value.prompt_cache_options.mode !== 'explicit')) deny();
}

// Whole strict grammar before JSON.parse. Only decoded KEY strings and number
// token slices are materialized; each copy is charged before creation. No new
// expected body is stringified, and no tree budget is restarted at parse.
export function inspectJSONGrammar(text, limits, charge, maxNodes) {
  // Shared private entry, not a public unbounded JSON scanner. Each caller
  // funds charge() on its original ledger and admits the UTF8 source first.
  if (typeof text !== 'string' || !positive(limits?.native_request_bytes) || text.length > limits.native_request_bytes ||
      !positive(limits?.json_depth) || !positive(maxNodes) || typeof charge !== 'function') deny();
  let at = 0; let nodes = 0;
  const stack = [];
  const node = () => { if (++nodes > maxNodes) deny(); };
  const ws = () => { while ([32, 9, 10, 13].includes(text.charCodeAt(at))) at++; };
  const slice = (a, b) => { charge(multiply(b - a, 2)); return text.slice(a, b); };
  const hex = (c) => c >= 48 && c <= 57 ? c - 48 : c >= 65 && c <= 70 ? c - 55 : c >= 97 && c <= 102 ? c - 87 : -1;
  const unicode = () => { let value = 0; for (let i = 0; i < 4; i++) { const n = hex(text.charCodeAt(at++)); if (n < 0) deny(); value = value * 16 + n; } return value; };
  const string = (key) => {
    if (text.charAt(at++) !== '"') deny();
    let from = at; let size = 0; const pieces = [];
    while (at < text.length) {
      const c = text.charCodeAt(at++);
      if (c === 34) {
        if (!key) return;
        if (at - 1 > from) { pieces.push(slice(from, at - 1)); size += at - 1 - from; }
        charge(multiply(size, 2)); return pieces.join('');
      }
      if (c < 32) deny();
      if (c !== 92) continue;
      if (key && at - 1 > from) { pieces.push(slice(from, at - 1)); size += at - 1 - from; }
      const esc = text.charAt(at++); let value;
      if (esc === 'u') {
        const hi = unicode();
        if (hi >= 0xdc00 && hi <= 0xdfff) deny();
        if (hi >= 0xd800 && hi <= 0xdbff) {
          if (text.charAt(at++) !== '\\' || text.charAt(at++) !== 'u') deny();
          const lo = unicode(); if (!(lo >= 0xdc00 && lo <= 0xdfff)) deny();
          if (key) { charge(4); value = String.fromCharCode(hi, lo); }
          size += 2;
        } else { if (key) { charge(2); value = String.fromCharCode(hi); } size++; }
      } else {
        const codes = { '"': 34, '\\': 92, '/': 47, b: 8, f: 12, n: 10, r: 13, t: 9 };
        if (!own(codes, esc)) deny();
        if (key) { charge(2); value = String.fromCharCode(codes[esc]); }
        size++;
      }
      if (key) pieces.push(value);
      from = at;
    }
    deny();
  };
  const digit = (c) => c >= 48 && c <= 57;
  const value = () => {
    ws(); node(); const c = text.charAt(at);
    if (c === '{' || c === '[') {
      if (stack.length + 1 > limits.json_depth) deny();
      stack.push({ object: c === '{', state: c === '{' ? 'firstKey' : 'firstValue', keys: new Set() }); at++; return;
    }
    if (c === '"') { string(false); return; }
    for (const literal of ['true', 'false', 'null']) if (text.startsWith(literal, at)) { at += literal.length; return; }
    const start = at;
    if (text.charAt(at) === '-') at++;
    if (text.charAt(at) === '0') at++;
    else { if (!(text.charCodeAt(at) >= 49 && text.charCodeAt(at) <= 57)) deny(); while (digit(text.charCodeAt(at))) at++; }
    if (text.charAt(at) === '.') { at++; if (!digit(text.charCodeAt(at))) deny(); while (digit(text.charCodeAt(at))) at++; }
    if (['e', 'E'].includes(text.charAt(at))) { at++; if (['+', '-'].includes(text.charAt(at))) at++; if (!digit(text.charCodeAt(at))) deny(); while (digit(text.charCodeAt(at))) at++; }
    if (at === start || !Number.isFinite(Number(slice(start, at)))) deny();
  };
  ws(); if (text.charAt(at) !== '{') deny(); value();
  while (stack.length) {
    const top = stack[stack.length - 1]; ws();
    if (top.state === 'firstKey' || top.state === 'key') {
      if (top.state === 'firstKey' && text.charAt(at) === '}') { at++; stack.pop(); continue; }
      node(); const key = string(true); if (top.keys.has(key)) deny(); top.keys.add(key); top.state = 'colon';
    } else if (top.state === 'colon') { if (text.charAt(at++) !== ':') deny(); top.state = 'value'; }
    else if (top.state === 'firstValue' || top.state === 'value') {
      if (top.state === 'firstValue' && text.charAt(at) === ']') { at++; stack.pop(); continue; }
      top.state = 'end'; value();
    } else {
      if (text.charAt(at) === (top.object ? '}' : ']')) { at++; stack.pop(); }
      else if (text.charAt(at++) === ',') top.state = top.object ? 'key' : 'value'; else deny();
    }
  }
  ws(); if (at !== text.length) deny(); return nodes;
}

export function createPhysicalFetchOwner(options) {
  return createFetchOwner(options, false);
}

// SPU3 never accepts or fabricates SPU2's zero-argument Use result. Its fixed
// stdio HTTP sink sends the final authenticated request; ONLY the original Go
// owner authorizes Source/network use. Both routes retain the same SDK request
// snapshot and bounded response/parser guards, not a second model client.
export function createSupervisedFetchOwner(options) {
  return createFetchOwner(options, true);
}

function createFetchOwner(options, supervised) {
  let expected; let limits; let downstream; let policy; let originalSignal; let parentUse; let physicalFetch;
  let headers; let remainingR; let remainingParseNodes; let bodyNodes;
  try {
    if (!closed(options, supervised ? ['limits', 'admittedRequest', 'originalSignal', 'physicalFetch'] :
      ['limits', 'admittedRequest', 'originalSignal', 'parentUse', 'physicalFetch'])) deny();
    originalSignal = data(options, 'originalSignal');
    parentUse = supervised ? undefined : data(options, 'parentUse'); physicalFetch = data(options, 'physicalFetch');
    if (!(originalSignal instanceof AbortSignal) || originalSignal.aborted ||
        (!supervised && (!closed(parentUse, ['authorizePhysicalSend']) || !Object.isFrozen(parentUse) ||
        typeof data(parentUse, 'authorizePhysicalSend') !== 'function')) || typeof physicalFetch !== 'function') deny();
    const source = data(options, 'limits');
    if (!closed(source, needed, limitNames) || !Object.isFrozen(source)) deny();
    limits = Object.create(null);
    for (const key of Reflect.ownKeys(source)) { const n = data(source, key); if (!positive(n)) deny(); limits[key] = n; }
    if (limits.native_request_bytes > limits.pre_send_allocation_bytes ||
        limits.pre_send_allocation_bytes > limits.process_memory_bytes ||
        limits.response_chunk_bytes > limits.response_body_bytes || limits.sse_line_bytes > limits.sse_event_bytes ||
        limits.sse_event_bytes > limits.response_body_bytes) deny();
    expected = data(options, 'admittedRequest');
    if (!closed(expected, ['endpoint', 'method', 'headers', 'body', 'request', 'schema']) || !Object.isFrozen(expected)) deny();
    for (const key of Reflect.ownKeys(expected)) data(expected, key);
    if (typeof expected.endpoint !== 'string' || expected.method !== 'POST' ||
        !/^https:\/\/[^/?#@]+\/[^?#]*$/.test(expected.endpoint)) deny();
    const n = utf8(expected.body, limits.native_request_bytes);
    remainingR = limits.pre_send_allocation_bytes;
    const charge = (cost) => { remainingR = subtract(remainingR, cost); };
    charge(multiply(expected.endpoint.length, 6)); // expected/captured/normalized logical URL text
    const endpoint = new URL(expected.endpoint);
    if (endpoint.href !== expected.endpoint || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) deny();
    charge(multiply(expected.body.length, 4));
    const full = shape(expected.request, limits, charge);
    const schema = shape(expected.schema, limits, charge);
    if (!record(expected.schema)) deny();
    request(expected.request, expected.schema, limits.json_nodes);
    headers = expected.headers;
    if (!Array.isArray(headers) || !Object.isFrozen(headers) || headers.length !== headerNames.length ||
        Reflect.ownKeys(headers).length !== headers.length + 1) deny();
    const seen = new Set(); let headerBytes = 0;
    for (let i = 0; i < headers.length; i++) {
      const pair = data(headers, String(i));
      if (!Array.isArray(pair) || !Object.isFrozen(pair) || pair.length !== 2 || Reflect.ownKeys(pair).length !== 3) deny();
      const name = data(pair, '0'); const value = data(pair, '1');
      if (!headerNames.includes(name) || seen.has(name) || typeof value !== 'string' || !value) deny();
      seen.add(name);
      headerBytes = sum(headerBytes, utf8(name, limits.request_header_bytes), utf8(value, limits.request_header_bytes), 4);
      if (headerBytes > limits.request_header_bytes || value.charCodeAt(0) <= 32 || value.charCodeAt(value.length - 1) <= 32) deny();
      for (let j = 0; j < value.length; j++) if (value.charCodeAt(j) < 32 || value.charCodeAt(j) === 127) deny();
      charge(multiply(name.length + value.length, 6));
      if ((name === 'accept' || name === 'content-type') && value !== 'application/json') deny();
      if (name === 'authorization' && (!value.startsWith('Bearer ') || value.length === 7)) deny();
      if (name === 'x-stainless-retry-count' && value !== '0') deny();
      if (name === 'x-stainless-lang' && value !== 'js') deny();
      if (name === 'x-stainless-package-version' && value !== '6.40.0') deny();
      if (name === 'x-stainless-runtime' && value !== 'node') deny();
      if (name === 'x-stainless-timeout' && !/^(0|[1-9][0-9]*)$/.test(value)) deny();
    }
    charge(multiply(n, 2)); // parsed REQUEST key/string payload reserved before parse
    // Count the prospective echo BEFORE constructing its retained projection.
    // Values are already frozen; repeated logical appearances still cost X.
    const echoCount = { nodes: 1, bytes: 0 };
    for (const key of echoFields) if (own(expected.request, key)) {
      const part = shape(data(expected.request, key), limits, () => {});
      echoCount.nodes = sum(echoCount.nodes, 1, part.nodes);
      echoCount.bytes = sum(echoCount.bytes, multiply(key.length, 2), part.bytes);
      if (echoCount.nodes > limits.json_nodes) deny();
    }
    const X = echoCount.bytes; const B = limits.response_body_bytes; const C = limits.response_chunk_bytes;
    const E = limits.sse_event_bytes; const H = limits.response_header_bytes;
    const O = sum(C, multiply(H, 4), multiply(B, 2), X);
    const A = Math.min(subtract(limits.response_allocation_bytes, O),
      subtract(limits.process_memory_bytes, limits.pre_send_allocation_bytes, O));
    const D = subtract(limits.response_copy_work_bytes, multiply(H, 4), multiply(B, 2));
    const Jrequest = sum(multiply(full.nodes, 2), schema.nodes);
    const Ndown = subtract(limits.json_nodes, Jrequest, echoCount.nodes);
    if (A <= multiply(E, 2) || X > A || D <= sum(multiply(B, 2), E) || !positive(Ndown) || echoCount.nodes > Ndown) deny();
    remainingParseNodes = subtract(Jrequest, full.nodes, schema.nodes);
    bodyNodes = inspectJSONGrammar(expected.body, limits, charge, remainingParseNodes);
    if (bodyNodes !== full.nodes) deny();
    downstream = Object.freeze({ ...limits, response_allocation_bytes: A, response_copy_work_bytes: D, json_nodes: Ndown });
    Object.freeze(limits);
    const echo = Object.create(null);
    for (const key of echoFields) if (own(expected.request, key)) echo[key] = data(expected.request, key);
    Object.freeze(echo);
    policy = createResponsesFramePolicy({ limits: downstream, expectedRequest: echo });
  } catch { throw fail('pi_physical_configuration_invalid'); }
  finally { options = undefined; } // relinquish constructor's admitted/capability alias

  let spent = false; let closedState = false; let controller; let pending;
  let snapshot; let sdkSignal; let useSignal;
  let rawResponse; let responseOwner = 'none'; let rawCancelled = false;
  let links = [];
  const cancelRaw = (response) => {
    if (!(response instanceof Response) || rawCancelled) return;
    rawCancelled = true;
    try { if (response.body && !response.body.locked) Promise.resolve(response.body.cancel()).catch(() => {}); } catch { /* safe ownership cleanup */ }
  };
  const stop = (error) => {
    if (!closedState) {
      closedState = true;
      for (const [signal, listener] of links) signal.removeEventListener('abort', listener);
      links = [];
      try { if (controller && !controller.signal.aborted) controller.abort(); } catch { /* same owned capability */ }
      if (responseOwner === 'raw') cancelRaw(rawResponse);
      rawResponse = undefined; policy = undefined; expected = undefined; headers = undefined;
      parentUse = undefined; physicalFetch = undefined;
      snapshot = undefined; sdkSignal = undefined; useSignal = undefined;
    }
    if (pending) { const current = pending; pending = undefined; current.reject(error ?? fail('pi_physical_aborted')); }
  };
  const link = (signal) => {
    if (links.some(([old]) => old === signal)) return;
    const listener = () => stop(fail('pi_physical_aborted'));
    links.push([signal, listener]); signal.addEventListener('abort', listener, { once: true });
    if (signal.aborted) listener();
  };
  const alive = (signals) => {
    if (closedState || signals.some((signal) => signal.aborted)) throw fail('pi_physical_aborted');
  };
  // Registered promise callbacks live in OWNER scope, never the per-fetch
  // validation scope. They can retain only these clearable owner slots, not
  // direct init/body/private-Headers/native-transport aliases after stop.
  const onTransportDenied = () => stop(fail('pi_physical_transport_denied'));
  const onParentDenied = () => stop(fail('pi_physical_parent_denied'));
  const onResponse = (response) => {
    if (closedState) { cancelRaw(response); return; }
    try {
      alive([originalSignal, sdkSignal, useSignal, controller.signal]);
      if (!(response instanceof Response)) deny();
      rawResponse = response; responseOwner = 'raw';
      // F09 owns cancel/release starting at entry, including failure.
      responseOwner = 'guarded';
      const guarded = createGuardedResponse({ response, limits: downstream,
        originalSignal, sdkSignal, fetchController: controller, validateFrame: policy });
      rawResponse = undefined;
      alive([originalSignal, sdkSignal, useSignal, controller.signal]);
      const current = pending; pending = undefined; current.resolve(guarded);
    } catch { stop(fail('pi_physical_response_denied')); }
  };
  const ignoreAssertionResult = () => {};
  const startTransport = () => {
    alive([originalSignal, sdkSignal, useSignal, controller.signal]);
    const result = physicalFetch.call(undefined, snapshot.url, {
      method: snapshot.method, headers: snapshot.headers, body: snapshot.body,
      redirect: 'error', signal: controller.signal,
    });
    snapshot = undefined; physicalFetch = undefined;
    if (!(result instanceof Promise)) { if (result instanceof Response) cancelRaw(result); deny(); }
    Promise.prototype.then.call(result, onResponse, onTransportDenied);
  };
  const onAuthorization = (use) => {
    if (closedState) return;
    try {
      if (!closed(use, ['signal', 'assertUsable']) || !Object.isFrozen(use)) deny();
      useSignal = data(use, 'signal'); const assertUsable = data(use, 'assertUsable');
      if (!(useSignal instanceof AbortSignal) || typeof assertUsable !== 'function') deny();
      link(useSignal);
      alive([originalSignal, sdkSignal, useSignal, controller.signal]);
      const assertion = assertUsable.call(use);
      if (assertion instanceof Promise) {
        Promise.prototype.then.call(assertion, ignoreAssertionResult, ignoreAssertionResult);
      }
      if (assertion !== undefined) deny(); // synchronous undefined ONLY, never await permission
      alive([originalSignal, sdkSignal, useSignal, controller.signal]); // immediate POST-assert recheck
      startTransport();
    } catch { stop(fail('pi_physical_parent_denied')); }
  };
  const fetch = (url, init) => {
    if (spent || closedState) return Promise.reject(fail('pi_physical_send_spent'));
    spent = true; // irreversible FIRST entry, including malformed input
    return new Promise((resolve, reject) => {
      pending = { resolve, reject };
      try {
        if (typeof url !== 'string' || url !== expected.endpoint ||
            !closed(init, ['signal', 'method', 'headers', 'body'])) deny();
        sdkSignal = data(init, 'signal');
        const method = data(init, 'method'); const sourceHeaders = data(init, 'headers'); const body = data(init, 'body');
        if (!(sdkSignal instanceof AbortSignal) || method !== 'POST' || !(sourceHeaders instanceof Headers) ||
            typeof body !== 'string' || body !== expected.body) deny();
        const pairs = []; let bytes = 0;
        for (const [name, value] of Headers.prototype.entries.call(sourceHeaders)) {
          bytes = sum(bytes, utf8(name, limits.request_header_bytes), utf8(value, limits.request_header_bytes), 4);
          if (bytes > limits.request_header_bytes || pairs.length === headers.length ||
              !headers.some(([key, wanted]) => name === key && value === wanted)) deny();
          pairs.push(Object.freeze([name, value]));
        }
        if (pairs.length !== headers.length) deny();
        // One reserved matching parsed tree. No helper obtains a fresh node cap.
        if (bodyNodes > remainingParseNodes) deny();
        remainingParseNodes -= bodyNodes;
        const parsed = JSON.parse(body);
        if (!equal(parsed, expected.request, limits.json_nodes)) deny();
        const ownedHeaders = new Headers(pairs);
        for (const [name, value] of Headers.prototype.entries.call(ownedHeaders)) if (!pairs.some(([key, wanted]) => key === name && wanted === value)) deny();
        snapshot = Object.freeze({ url, method, headers: ownedHeaders, body });
        controller = new AbortController();
        link(originalSignal); link(sdkSignal); link(controller.signal);
        alive([originalSignal, sdkSignal, controller.signal]);
        if (supervised) {
          useSignal = controller.signal; // cancellation only; NOT Source permission
          startTransport();
        } else {
          const authorization = parentUse.authorizePhysicalSend.call(parentUse); // ZERO args, NON-authority seam
          parentUse = undefined;
          if (!(authorization instanceof Promise)) deny();
          Promise.prototype.then.call(authorization, onAuthorization, onParentDenied);
        }
      } catch { stop(fail('pi_physical_request_denied')); }
    });
  };
  return Object.freeze({ fetch, dispose() { stop(fail('pi_physical_aborted')); } });
}
