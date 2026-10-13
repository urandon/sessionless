// Private selected Pi6c4f36/OpenAI6.40 tool-free safety policy. This is NOT
// the SDK/Pi semantic parser, a final producer, or a send/profile authority.
// Native objects/GC/SDK copies and the full pre-Issue ledger remain external.
const fail = () => { throw new Error('pi_response_policy_denied'); };
const configuration = () => {
  const error = new Error('pi_response_policy_configuration_invalid');
  error.name = 'PiResponsePolicyError';
  throw error;
};
const record = (value) => value !== null && typeof value === 'object' &&
  !Array.isArray(value) && [Object.prototype, null].includes(Object.getPrototypeOf(value));
const string = (value) => typeof value === 'string';
const id = (value) => string(value) && value.length > 0;
const count = (value) => Number.isSafeInteger(value) && value >= 0;
const positive = (value) => count(value) && value > 0;
const finite = (value) => typeof value === 'number' && Number.isFinite(value);
const empty = (value) => Array.isArray(value) && value.length === 0;
const optional = (value, key, check) => !Object.hasOwn(value, key) || check(value[key]);
const oneOf = (value, values) => values.includes(value);
const closed = (value, required, allowed = required) => record(value) &&
  required.every((key) => Object.hasOwn(value, key)) &&
  Object.keys(value).every((key) => allowed.includes(key));
const checkedDouble = (value) => {
  if (value > Math.floor(Number.MAX_SAFE_INTEGER / 2)) fail();
  return value * 2;
};
const status = (value) => oneOf(value, ['in_progress', 'completed', 'incomplete']);
const effort = (value) => oneOf(value, [null, 'none', 'minimal', 'low', 'medium', 'high', 'xhigh']);
const summary = (value) => oneOf(value, [null, 'auto', 'concise', 'detailed']);
const tiers = [null, 'auto', 'default', 'flex', 'scale', 'priority'];
const requestFields = ['model', 'max_output_tokens', 'store', 'truncation', 'tools', 'tool_choice',
  'instructions', 'metadata', 'text', 'reasoning', 'temperature', 'top_p', 'parallel_tool_calls',
  'prompt_cache_key', 'prompt_cache_retention', 'safety_identifier', 'user', 'service_tier'];
const responseRequired = ['id', 'created_at', 'error', 'incomplete_details', 'instructions',
  'metadata', 'model', 'object', 'output', 'parallel_tool_calls', 'temperature', 'tool_choice',
  'tools', 'top_p', 'status'];
const responseFields = [...responseRequired, 'output_text', 'store', 'background', 'completed_at',
  'conversation', 'max_output_tokens', 'previous_response_id', 'prompt', 'prompt_cache_key',
  'prompt_cache_retention', 'reasoning', 'safety_identifier', 'service_tier', 'text',
  'top_logprobs', 'truncation', 'usage', 'user'];
const limitFields = ['json_nodes', 'json_depth', 'sse_events', 'sse_line_bytes',
  'sse_event_bytes', 'response_body_bytes', 'response_copy_work_bytes', 'response_allocation_bytes'];

function metadata(value) {
  if (value === null) return true;
  if (!record(value)) return false;
  const keys = Object.keys(value);
  return keys.length <= 16 && keys.every((key) => key.length <= 64 &&
    string(value[key]) && value[key].length <= 512);
}
function reasoning(value) {
  return value === null || (closed(value, [], ['effort', 'summary', 'generate_summary']) &&
    optional(value, 'effort', effort) && optional(value, 'summary', summary) &&
    optional(value, 'generate_summary', summary));
}
function textConfig(value) {
  if (!closed(value, [], ['format', 'verbosity']) ||
      !optional(value, 'verbosity', (v) => oneOf(v, [null, 'low', 'medium', 'high']))) return false;
  if (!Object.hasOwn(value, 'format')) return true;
  const format = value.format;
  if (closed(format, ['type']) && oneOf(format.type, ['text', 'json_object'])) return true;
  return closed(format, ['type', 'name', 'schema'], ['type', 'name', 'schema', 'description', 'strict']) &&
    format.type === 'json_schema' && string(format.name) && /^[A-Za-z0-9_-]{1,64}$/.test(format.name) &&
    record(format.schema) && optional(format, 'description', string) &&
    optional(format, 'strict', (v) => v === null || typeof v === 'boolean');
}

// Trusted snapshot only: inspect own DATA before reading, iterative and bounded.
// Parent has already reserved/constructed it; this check does not prove provenance.
function frozenJSON(value, limits) {
  const pending = [{ value, depth: 1 }];
  let nodes = 0;
  let textBytes = 0;
  while (pending.length) {
    if (++nodes > limits.json_nodes) fail();
    const entry = pending.pop();
    const current = entry.value;
    if (current === null || typeof current === 'boolean' || finite(current)) continue;
    if (string(current)) {
      const size = checkedDouble(current.length);
      if (size > limits.response_allocation_bytes - textBytes) fail();
      textBytes += size;
      continue;
    }
    if ((!record(current) && !Array.isArray(current)) || !Object.isFrozen(current) ||
        entry.depth > limits.json_depth) fail();
    const keys = Reflect.ownKeys(current);
    if (keys.length > limits.json_nodes - nodes + 1) fail();
    if (Array.isArray(current)) {
      const length = Object.getOwnPropertyDescriptor(current, 'length');
      if (!length || !Object.hasOwn(length, 'value') || !count(length.value) ||
          keys.length !== length.value + 1) fail();
      for (let index = 0; index < length.value; index++) {
        const descriptor = Object.getOwnPropertyDescriptor(current, String(index));
        if (!descriptor || !Object.hasOwn(descriptor, 'value')) fail();
        pending.push({ value: descriptor.value, depth: entry.depth +
          (descriptor.value !== null && typeof descriptor.value === 'object' ? 1 : 0) });
      }
    } else {
      for (const key of keys) {
        if (!string(key) || ++nodes > limits.json_nodes) fail();
        const descriptor = Object.getOwnPropertyDescriptor(current, key);
        if (!descriptor || !Object.hasOwn(descriptor, 'value')) fail();
        const size = checkedDouble(key.length);
        if (size > limits.response_allocation_bytes - textBytes) fail();
        textBytes += size;
        pending.push({ value: descriptor.value, depth: entry.depth +
          (descriptor.value !== null && typeof descriptor.value === 'object' ? 1 : 0) });
      }
    }
    if (pending.length > limits.json_nodes - nodes) fail();
  }
}

export function createResponsesFramePolicy(options) {
  let limits;
  let expected;
  let copyCredit;
  let allocationBase;
  try {
    if (!record(options)) fail();
    const ld = Object.getOwnPropertyDescriptor(options, 'limits');
    const ed = Object.getOwnPropertyDescriptor(options, 'expectedRequest');
    if (!ld || !ed || !Object.hasOwn(ld, 'value') || !Object.hasOwn(ed, 'value')) fail();
    const source = ld.value;
    if (!record(source) || !Object.isFrozen(source)) fail();
    limits = Object.create(null);
    for (const field of limitFields) {
      const descriptor = Object.getOwnPropertyDescriptor(source, field);
      if (!descriptor || !Object.hasOwn(descriptor, 'value') || !positive(descriptor.value)) fail();
      limits[field] = descriptor.value;
    }
    if (limits.sse_line_bytes > limits.sse_event_bytes ||
        limits.sse_event_bytes > limits.response_body_bytes) fail();
    const framerCopy = checkedDouble(limits.response_body_bytes);
    if (limits.sse_event_bytes > limits.response_copy_work_bytes - framerCopy) fail();
    copyCredit = limits.response_copy_work_bytes - framerCopy - limits.sse_event_bytes;
    allocationBase = checkedDouble(limits.sse_event_bytes);
    if (copyCredit <= 0 || allocationBase >= limits.response_allocation_bytes) fail();
    expected = ed.value;
    frozenJSON(expected, limits);
    if (!closed(expected, ['model', 'max_output_tokens', 'store', 'truncation', 'tool_choice'], requestFields) ||
        !id(expected.model) || !positive(expected.max_output_tokens) || expected.store !== false ||
        expected.truncation !== 'disabled' || expected.tool_choice !== 'none' ||
        !optional(expected, 'tools', empty) ||
        !optional(expected, 'instructions', (v) => v === null || string(v)) ||
        !optional(expected, 'metadata', metadata) || !optional(expected, 'reasoning', reasoning) ||
        !optional(expected, 'text', textConfig) ||
        !optional(expected, 'temperature', (v) => v === null || (finite(v) && v >= 0 && v <= 2)) ||
        !optional(expected, 'top_p', (v) => v === null || (finite(v) && v >= 0 && v <= 1)) ||
        !optional(expected, 'parallel_tool_calls', (v) => typeof v === 'boolean') ||
        !optional(expected, 'prompt_cache_key', string) || !optional(expected, 'safety_identifier', string) ||
        !optional(expected, 'user', string) ||
        !optional(expected, 'prompt_cache_retention', (v) => oneOf(v, [null, 'in_memory', '24h'])) ||
        !optional(expected, 'service_tier', (v) => oneOf(v, tiers))) fail();
    if (Object.hasOwn(expected, 'text') && Object.hasOwn(expected.text, 'format') &&
        (expected.text.format.type !== 'json_schema' || expected.text.format.strict !== true)) fail();
    Object.freeze(limits);
  } catch { configuration(); }

  let closedState = false;
  let active = false;
  let nodes = 0;
  let events = 0;
  let bodyBytes = 0;
  let transientBytes = 0;
  let retainedBytes = 0;
  let responseID;
  const identities = new Map();
  const indices = new Map();
  const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
  const node = () => { if (++nodes > limits.json_nodes) fail(); };
  const allocate = (size, retain = false) => {
    if (!count(size) || size > limits.response_allocation_bytes - allocationBase - retainedBytes - transientBytes) fail();
    if (retain) retainedBytes += size;
    else transientBytes += size;
  };
  const copy = (size, retain = false) => {
    if (!count(size) || size > copyCredit) fail();
    allocate(size, retain);
    copyCredit -= size;
  };
  const slice = (text, start, end) => {
    copy(checkedDouble(end - start));
    return text.slice(start, end);
  };
  const equal = (left, right) => {
    const pending = [[left, right]];
    let seen = 0;
    while (pending.length) {
      if (++seen > limits.json_nodes) fail();
      const [a, b] = pending.pop();
      if (a === b) continue;
      if (a === null || b === null || typeof a !== 'object' || typeof b !== 'object' ||
          Array.isArray(a) !== Array.isArray(b)) return false;
      const keys = Object.keys(a);
      if (keys.length !== Object.keys(b).length) return false;
      for (const key of keys) {
        if (!Object.hasOwn(b, key)) return false;
        pending.push([a[key], b[key]]);
      }
      if (pending.length > limits.json_nodes - seen) fail();
    }
    return true;
  };
  const echo = (value, key, neutral) => {
    if (Object.hasOwn(expected, key)) return equal(value, expected[key]);
    return neutral(value);
  };
  const observe = (itemID, kind, index) => {
    if (!id(itemID) || !oneOf(kind, ['message', 'reasoning'])) fail();
    if (identities.has(itemID)) {
      const old = identities.get(itemID);
      if (old.kind !== kind || (index !== undefined && old.index !== undefined && old.index !== index)) fail();
      if (index !== undefined) old.index = index;
    } else {
      node(); copy(checkedDouble(itemID.length), true);
      identities.set(itemID, {id: itemID, kind, index});
    }
    if (index !== undefined) {
      if (!count(index)) fail();
      if (indices.has(index)) {
        const old = indices.get(index);
        if (old.id !== itemID || old.kind !== kind) fail();
      } else {
        // Reuse the first paid ID record. A later parsed equal string must not
        // become another retained string through the explicit index binding.
        node(); indices.set(index, identities.get(itemID));
      }
    }
  };
  function part(value, kinds) {
    if (!record(value) || !kinds.includes(value.type)) fail();
    if (value.type === 'output_text') {
      if (!closed(value, ['type', 'text'], ['type', 'text', 'annotations', 'logprobs']) ||
          !string(value.text) || !optional(value, 'annotations', empty) || !optional(value, 'logprobs', empty)) fail();
    } else if (value.type === 'refusal') {
      if (!closed(value, ['type', 'refusal']) || !string(value.refusal)) fail();
    } else if (!closed(value, ['type', 'text']) || !string(value.text)) fail();
  }
  function item(value, index) {
    if (!record(value)) fail();
    if (value.type === 'message') {
      if (!closed(value, ['type', 'id', 'content', 'role', 'status'],
        ['type', 'id', 'content', 'role', 'status', 'phase']) || value.role !== 'assistant' ||
        !status(value.status) || !Array.isArray(value.content) ||
        !optional(value, 'phase', (v) => oneOf(v, [null, 'commentary', 'final_answer']))) fail();
      for (const content of value.content) part(content, ['output_text', 'refusal']);
    } else if (value.type === 'reasoning') {
      if (!closed(value, ['type', 'id', 'summary'],
        ['type', 'id', 'summary', 'content', 'encrypted_content', 'status']) ||
        !Array.isArray(value.summary) || !optional(value, 'content', Array.isArray) ||
        !optional(value, 'encrypted_content', (v) => v === null || string(v)) ||
        !optional(value, 'status', status)) fail();
      for (const content of value.summary) part(content, ['summary_text']);
      if (value.content) for (const content of value.content) part(content, ['reasoning_text']);
    } else fail(); // all tool/hosted/approval/future item variants deny
    observe(value.id, value.type, index);
  }
  function usage(value) {
    if (value === null) return;
    if (!closed(value, ['input_tokens', 'output_tokens', 'total_tokens', 'input_tokens_details', 'output_tokens_details']) ||
        !count(value.input_tokens) || !count(value.output_tokens) || !count(value.total_tokens) ||
        value.output_tokens > Number.MAX_SAFE_INTEGER - value.input_tokens ||
        value.input_tokens + value.output_tokens !== value.total_tokens) fail();
    const input = value.input_tokens_details;
    const output = value.output_tokens_details;
    if (!closed(input, ['cached_tokens'], ['cached_tokens', 'cache_write_tokens']) ||
        !closed(output, ['reasoning_tokens']) || !count(input.cached_tokens) ||
        !optional(input, 'cache_write_tokens', count) || !count(output.reasoning_tokens)) fail();
    const write = Object.hasOwn(input, 'cache_write_tokens') ? input.cache_write_tokens : 0;
    if (input.cached_tokens > value.input_tokens || write > value.input_tokens - input.cached_tokens ||
        output.reasoning_tokens > value.output_tokens) fail();
  }
  function response(value, eventType) {
    if (!closed(value, responseRequired, responseFields) || !id(value.id) ||
        value.object !== 'response' || value.model !== expected.model ||
        !finite(value.created_at) || value.created_at < 0 || value.error !== null ||
        !Array.isArray(value.output) || typeof value.parallel_tool_calls !== 'boolean' ||
        !empty(value.tools) || value.tool_choice !== 'none') fail();
    const wanted = eventType === 'response.created' ? 'in_progress' : eventType.slice('response.'.length);
    if (value.status !== wanted) fail();
    if (eventType === 'response.incomplete') {
      if (!closed(value.incomplete_details, ['reason']) ||
          !oneOf(value.incomplete_details.reason, ['max_output_tokens', 'content_filter'])) fail();
    } else if (value.incomplete_details !== null) fail();
    if (responseID === undefined) { node(); copy(checkedDouble(value.id.length), true); responseID = value.id; }
    else if (responseID !== value.id) fail();
    for (const output of value.output) item(output);
    if ((value.instructions !== null && !string(value.instructions)) ||
        !echo(value.instructions, 'instructions', (v) => v === null || v === '') ||
        !metadata(value.metadata) || !echo(value.metadata, 'metadata', (v) => v === null || Object.keys(v).length === 0) ||
        !echo(value.parallel_tool_calls, 'parallel_tool_calls', (v) => typeof v === 'boolean')) fail();
    for (const [key, max] of [['temperature', 2], ['top_p', 1]]) {
      if (value[key] !== null && (!finite(value[key]) || value[key] < 0 || value[key] > max)) fail();
      if (Object.hasOwn(expected, key) && !equal(value[key], expected[key])) fail();
    }
    if (!optional(value, 'output_text', string) ||
        !optional(value, 'store', (v) => v === false && expected.store === false) ||
        !optional(value, 'background', (v) => v === null || v === false) ||
        !optional(value, 'completed_at', (v) => v === null || (finite(v) && v >= 0)) ||
        !optional(value, 'conversation', (v) => v === null) ||
        !optional(value, 'previous_response_id', (v) => v === null) ||
        !optional(value, 'prompt', (v) => v === null) ||
        !optional(value, 'max_output_tokens', (v) => v === null || (positive(v) && v === expected.max_output_tokens)) ||
        !optional(value, 'top_logprobs', (v) => v === null || v === 0) ||
        !optional(value, 'truncation', (v) => v === null || v === 'disabled')) fail();
    for (const key of ['prompt_cache_key', 'safety_identifier']) {
      if (!optional(value, key, (v) => string(v) && echo(v, key, (v) => v === ''))) fail();
    }
    if (!optional(value, 'user', (v) => (v === null || string(v)) && echo(v, 'user', (v) => v === null || v === '')) ||
        !optional(value, 'prompt_cache_retention', (v) => oneOf(v, [null, 'in_memory', '24h']) &&
          (v === null || echo(v, 'prompt_cache_retention', () => false))) ||
        !optional(value, 'reasoning', (v) => reasoning(v) && echo(v, 'reasoning', (v) => v === null || Object.keys(v).length === 0)) ||
        !optional(value, 'text', (v) => textConfig(v) && echo(v, 'text', (v) => Object.keys(v).length === 0)) ||
        !optional(value, 'service_tier', (v) => oneOf(v, tiers) &&
          (!Object.hasOwn(expected, 'service_tier') || equal(v, expected.service_tier)))) fail();
    if (Object.hasOwn(value, 'usage')) usage(value.usage);
  }
  function event(value) {
    if (!record(value) || !string(value.type) || !count(value.sequence_number)) fail();
    const common = ['type', 'sequence_number'];
    const type = value.type;
    if (oneOf(type, ['response.created', 'response.in_progress', 'response.queued', 'response.completed', 'response.incomplete'])) {
      if (!closed(value, [...common, 'response'])) fail();
      response(value.response, type);
      return type === 'response.completed' || type === 'response.incomplete';
    }
    if (oneOf(type, ['response.output_item.added', 'response.output_item.done'])) {
      if (!closed(value, [...common, 'output_index', 'item']) || !count(value.output_index)) fail();
      item(value.item, value.output_index); return false;
    }
    if (oneOf(type, ['response.content_part.added', 'response.content_part.done'])) {
      if (!closed(value, [...common, 'content_index', 'item_id', 'output_index', 'part']) ||
          !count(value.content_index) || !count(value.output_index)) fail();
      part(value.part, ['output_text', 'refusal', 'reasoning_text']);
      observe(value.item_id, value.part.type === 'reasoning_text' ? 'reasoning' : 'message', value.output_index);
      return false;
    }
    if (oneOf(type, ['response.reasoning_summary_part.added', 'response.reasoning_summary_part.done'])) {
      if (!closed(value, [...common, 'summary_index', 'item_id', 'output_index', 'part']) ||
          !count(value.summary_index) || !count(value.output_index)) fail();
      part(value.part, ['summary_text']); observe(value.item_id, 'reasoning', value.output_index); return false;
    }
    const summaryDelta = type === 'response.reasoning_summary_text.delta';
    const summaryDone = type === 'response.reasoning_summary_text.done';
    const reasoningDelta = type === 'response.reasoning_text.delta';
    const reasoningDone = type === 'response.reasoning_text.done';
    const textDelta = type === 'response.output_text.delta';
    const textDone = type === 'response.output_text.done';
    const refusalDelta = type === 'response.refusal.delta';
    const refusalDone = type === 'response.refusal.done';
    if (!(summaryDelta || summaryDone || reasoningDelta || reasoningDone || textDelta || textDone || refusalDelta || refusalDone)) fail();
    const delta = summaryDelta || reasoningDelta || textDelta || refusalDelta;
    const index = summaryDelta || summaryDone ? 'summary_index' : 'content_index';
    const text = delta ? 'delta' : refusalDone ? 'refusal' : 'text';
    const required = [...common, 'item_id', 'output_index', index, text];
    const allowed = [...required];
    if (delta) allowed.push('obfuscation');
    if (textDelta || textDone) allowed.push('logprobs');
    if (!closed(value, required, allowed) || !count(value.output_index) || !count(value[index]) ||
        !string(value[text]) || !optional(value, 'obfuscation', string) || !optional(value, 'logprobs', empty)) fail();
    observe(value.item_id, summaryDelta || summaryDone || reasoningDelta || reasoningDone ? 'reasoning' : 'message', value.output_index);
    return false;
  }

  // Full JSON grammar and decoded duplicate keys before object materialization.
  function grammar(text) {
    let at = 0;
    const stack = [];
    const ws = () => { while (at < text.length && oneOf(text.charCodeAt(at), [32, 9, 10, 13])) at++; };
    const hex = (code) => code >= 48 && code <= 57 ? code - 48 :
      code >= 65 && code <= 70 ? code - 55 : code >= 97 && code <= 102 ? code - 87 : -1;
    const jsonString = (key) => {
      const start = at++;
      let end;
      while (at < text.length) {
        const code = text.charCodeAt(at++);
        if (code === 34) { end = at - 1; break; }
        if (code < 32) fail();
        if (code === 92) {
          const escape = text.charAt(at++);
          if (escape === 'u') {
            for (let index = 0; index < 4; index++) if (hex(text.charCodeAt(at++)) < 0) fail();
          } else if (!oneOf(escape, ['"', '\\', '/', 'b', 'f', 'n', 'r', 't'])) fail();
        }
      }
      if (end === undefined) fail();
      if (!key) return;
      const pieces = [];
      let from = start + 1;
      let length = 0;
      for (let cursor = from; cursor < end; cursor++) {
        if (text.charCodeAt(cursor) !== 92) continue;
        if (cursor > from) { pieces.push(slice(text, from, cursor)); length += cursor - from; }
        const escape = text.charAt(++cursor);
        let value;
        if (escape === 'u') {
          let code = 0;
          for (let index = 0; index < 4; index++) code = code * 16 + hex(text.charCodeAt(++cursor));
          copy(2); value = String.fromCharCode(code);
        } else {
          value = ({ '"': '"', '\\': '\\', '/': '/', b: '\b', f: '\f', n: '\n', r: '\r', t: '\t' })[escape];
        }
        pieces.push(value); length++; from = cursor + 1;
      }
      if (from < end) { pieces.push(slice(text, from, end)); length += end - from; }
      copy(checkedDouble(length));
      return pieces.join('');
    };
    const digit = (code) => code >= 48 && code <= 57;
    const value = () => {
      ws(); node();
      const code = text.charAt(at);
      if (code === '{' || code === '[') {
        if (stack.length + 1 > limits.json_depth) fail();
        stack.push({ object: code === '{', state: code === '{' ? 'firstKey' : 'firstValue', keys: new Set() });
        at++; return;
      }
      if (code === '"') { jsonString(false); return; }
      for (const literal of ['true', 'false', 'null']) {
        if (text.startsWith(literal, at)) { at += literal.length; return; }
      }
      const start = at;
      if (text.charAt(at) === '-') at++;
      if (text.charAt(at) === '0') at++;
      else {
        if (text.charCodeAt(at) < 49 || text.charCodeAt(at) > 57) fail();
        while (digit(text.charCodeAt(at))) at++;
      }
      if (text.charAt(at) === '.') {
        at++; if (!digit(text.charCodeAt(at))) fail();
        while (digit(text.charCodeAt(at))) at++;
      }
      if (text.charAt(at) === 'e' || text.charAt(at) === 'E') {
        at++; if (text.charAt(at) === '+' || text.charAt(at) === '-') at++;
        if (!digit(text.charCodeAt(at))) fail();
        while (digit(text.charCodeAt(at))) at++;
      }
      if (at === start || !Number.isFinite(Number(slice(text, start, at)))) fail();
    };
    ws(); if (text.charAt(at) !== '{') fail(); value();
    while (stack.length) {
      const top = stack[stack.length - 1]; ws();
      if (top.state === 'firstKey' || top.state === 'key') {
        if (top.state === 'firstKey' && text.charAt(at) === '}') { at++; stack.pop(); continue; }
        if (text.charAt(at) !== '"') fail();
        node(); const key = jsonString(true);
        if (top.keys.has(key)) fail();
        top.keys.add(key); top.state = 'colon';
      } else if (top.state === 'colon') {
        if (text.charAt(at++) !== ':') fail(); top.state = 'value';
      } else if (top.state === 'firstValue' || top.state === 'value') {
        if (top.state === 'firstValue' && text.charAt(at) === ']') { at++; stack.pop(); continue; }
        top.state = 'end'; value();
      } else {
        const closer = top.object ? '}' : ']';
        if (text.charAt(at) === closer) { at++; stack.pop(); }
        else if (text.charAt(at++) === ',') top.state = top.object ? 'key' : 'value';
        else fail();
      }
    }
    ws(); if (at !== text.length) fail();
  }

  const validateFrame = (frame) => {
    if (closedState || active) { closedState = true; return 'deny'; }
    active = true;
    transientBytes = 0;
    try {
      if (!(frame instanceof Uint8Array) || frame.byteLength === 0 ||
          frame.byteLength > limits.sse_event_bytes || frame.byteLength > limits.response_body_bytes - bodyBytes) fail();
      const end = frame.byteLength;
      if (!((frame[end - 1] === 10 && frame[end - 2] === 10) ||
          (frame[end - 1] === 13 && frame[end - 2] === 13) ||
          (frame[end - 1] === 10 && frame[end - 2] === 13 && frame[end - 3] === 10 && frame[end - 4] === 13))) fail();
      let lineBytes = 0;
      for (const byte of frame) {
        if (byte === 0) fail();
        if (byte === 10 || byte === 13) lineBytes = 0;
        else if (++lineBytes > limits.sse_line_bytes) fail();
      }
      bodyBytes += frame.byteLength;
      copy(checkedDouble(frame.byteLength));
      const text = decoder.decode(frame);
      const data = [];
      let dataLength = 0;
      let named;
      let sawNamed = false;
      let terminal = false;
      const dispatch = () => {
        if (!data.length && !sawNamed) return;
        if (terminal || !data.length || ++events > limits.sse_events) fail();
        copy(checkedDouble(dataLength + data.length - 1));
        const joined = data.join('\n');
        if (joined === '[DONE]') fail();
        grammar(joined);
        const parsed = JSON.parse(joined);
        if (named && named !== parsed.type) fail();
        terminal = event(parsed);
        data.length = 0; dataLength = 0; named = undefined; sawNamed = false;
      };
      let from = 0;
      for (let cursor = 0; cursor < text.length; cursor++) {
        const code = text.charCodeAt(cursor);
        if (code === 0) fail();
        if (code !== 10 && code !== 13) continue;
        if (text.charCodeAt(from) === 0xfeff) fail();
        if (cursor === from) dispatch();
        else if (text.charAt(from) !== ':') {
          let colon = from;
          while (colon < cursor && text.charAt(colon) !== ':') colon++;
          const field = slice(text, from, colon);
          let start = colon < cursor ? colon + 1 : cursor;
          if (text.charAt(start) === ' ') start++;
          if (field === 'data') {
            const line = slice(text, start, cursor); data.push(line); dataLength += line.length;
          } else if (field === 'event' && !sawNamed) {
            named = slice(text, start, cursor); sawNamed = true;
          } else fail();
        }
        if (code === 13 && text.charCodeAt(cursor + 1) === 10) cursor++;
        from = cursor + 1;
      }
      if (from !== text.length || data.length || sawNamed) fail();
      if (closedState) fail(); // a synchronous reentry cannot reopen outer work
      if (terminal) { closedState = true; identities.clear(); indices.clear(); responseID = undefined; }
      return terminal ? 'terminal' : 'continue';
    } catch {
      closedState = true;
      identities.clear(); indices.clear(); responseID = undefined;
      return 'deny';
    } finally { active = false; transientBytes = 0; }
  };
  return Object.freeze(validateFrame);
}
