// This is a bound inside the genuine Pi/OpenAI SDK path, not a model client.
// A native runtime/transport/package and eligible resource must be independently
// pinned before this implementation can confer registration authority.
import { Buffer } from 'node:buffer';

const eventTypes = new Set([
  'response.created', 'response.in_progress', 'response.output_item.added',
  'response.content_part.added', 'response.output_text.delta',
  'response.output_text.done', 'response.content_part.done',
  'response.output_item.done', 'response.completed',
]);
const itemTypes = new Set(['message', 'output_text']);
const denied = () => new Error('prepared Pi transport boundary denied');

// Bound structural allocation BEFORE JSON.parse constructs an object graph.
// Count key strings too (not only values); this is an explicit conservative
// lexical-node contract, not a claim about V8 allocator bytes. The immutable
// native platform ledger must still reserve the bounded graph/string storage.
function preflightJSON(data, limits) {
  let depth = 0, nodes = 0, quoted = false, escaped = false, primitive = false;
  for (const char of data) {
    if (quoted) {
      if (escaped) escaped = false;
      else if (char === '\\') escaped = true;
      else if (char === '"') quoted = false;
      continue;
    }
    if (char === '"') { quoted = true; primitive = false; nodes++; }
    else if (char === '{' || char === '[') { depth++; nodes++; primitive = false; }
    else if (char === '}' || char === ']') { depth--; primitive = false; }
    else if (char === ',' || char === ':' || /\s/.test(char)) primitive = false;
    else if (!primitive) { nodes++; primitive = true; }
    if (depth < 0 || depth > limits.depth || nodes > limits.nodes) throw denied();
  }
  if (quoted || depth !== 0) throw denied();
}

export function validateBounds(bounds) {
  const names = ['requestBytes', 'headerBytes', 'responseBytes', 'chunkBytes',
    'eventBytes', 'lineBytes', 'events', 'nodes', 'depth', 'copyBytes'];
  if (!bounds || Object.keys(bounds).length !== names.length) throw denied();
  for (const name of names) if (!Number.isSafeInteger(bounds[name]) || bounds[name] < 1) throw denied();
  if (bounds.depth > 32 || bounds.events > 4096 || bounds.eventBytes > 1 << 20 ||
      bounds.lineBytes > bounds.eventBytes || bounds.chunkBytes > bounds.eventBytes ||
      bounds.eventBytes > bounds.responseBytes || bounds.requestBytes > 16 << 20) throw denied();
  return Object.freeze({ ...bounds });
}

function inspectEvent(event, limits, secret) {
  if (!event || !eventTypes.has(event.type)) throw denied();
  if (event.type === 'response.completed' &&
      (event.response?.status !== 'completed' || !Array.isArray(event.response.output))) throw denied();
  let nodes = 0;
  function visit(value, depth) {
    if (++nodes > limits.nodes || depth > limits.depth) throw denied();
    if (typeof value === 'string' && value.includes(secret)) throw denied();
    if (value === null || typeof value !== 'object') return;
    if (Array.isArray(value)) { for (const child of value) visit(child, depth + 1); return; }
    for (const [key, child] of Object.entries(value)) {
      if (key === '__proto__' || key === 'constructor' || key === 'prototype') throw denied();
      if (key === 'type' && !eventTypes.has(child) && !itemTypes.has(child)) throw denied();
      if (key === 'role' && child !== 'assistant') throw denied();
      if (key === 'tools' && (!Array.isArray(child) || child.length !== 0)) throw denied();
      if (key === 'tool_choice' && child !== 'none') throw denied();
      if (key === 'status' && !['in_progress', 'completed'].includes(child)) throw denied();
      visit(child, depth + 1);
    }
  }
  visit(event, 0);
}

// Rechunk complete bounded SSE events before the SDK's incomplete-frame copy
// path. One-byte upstream fragmentation cannot amplify that SDK buffer. Raw
// transport/header/chunk allocations still need the separately pinned runtime
// and physical transport proof; a JS cap cannot bound an already allocated
// lower-layer buffer.
function boundedBody(response, limits, signal, secret) {
  const reader = response.body?.getReader();
  if (!reader) throw denied();
  const pending = new Uint8Array(limits.eventBytes);
  const decoder = new TextDecoder('utf-8', { fatal: true });
  let used = 0, line = 0, total = 0, copied = 0, events = 0, ended = false;
  let terminal = false, chunk, offset = 0;
  let abortListener;
  function stop() { ended = true; pending.fill(0); void reader.cancel().catch(() => {}); }
  if (signal) {
    abortListener = stop;
    signal.addEventListener('abort', abortListener, { once: true });
  }
  return new ReadableStream({
    async pull(controller) {
      try {
        if (signal?.aborted || ended) throw denied();
        while (true) {
          if (!chunk || offset === chunk.length) {
            const next = await reader.read();
            if (next.done) {
              if (used !== 0 || !terminal) throw denied();
              ended = true;
              signal?.removeEventListener('abort', abortListener);
              pending.fill(0); controller.close(); return;
            }
            chunk = next.value; offset = 0;
            if (!(chunk instanceof Uint8Array) || chunk.length > limits.chunkBytes ||
                (total += chunk.length) > limits.responseBytes) throw denied();
          }
          while (offset < chunk.length) {
            if (terminal) throw denied();
            const byte = chunk[offset++];
            if (used === pending.length || ++copied > limits.copyBytes) throw denied();
            pending[used++] = byte;
            if (byte === 13) throw denied(); // This fixed profile selects LF SSE.
            if (byte === 10) line = 0;
            else if (++line > limits.lineBytes) throw denied();
            if (used >= 2 && pending[used - 1] === 10 && pending[used - 2] === 10) {
              if (++events > limits.events || copied + 4 * used > limits.copyBytes) throw denied();
              copied += 4 * used; // Guard decode/forward and downstream parser copy reserve.
              const bytes = pending.slice(0, used);
              const text = decoder.decode(bytes);
              const lines = text.slice(0, -2).split('\n');
              let data = '', name;
              for (const entry of lines) {
                if (entry.startsWith('event: ') && name === undefined) name = entry.slice(7);
                else if (entry.startsWith('data: ') && data === '') data = entry.slice(6);
                else throw denied();
              }
              if (data === '[DONE]') throw denied(); // End only through the closed terminal + EOF.
              preflightJSON(data, limits);
              const event = JSON.parse(data);
              if (name !== undefined && name !== event.type) throw denied();
              inspectEvent(event, limits, secret);
              terminal = event.type === 'response.completed';
              pending.fill(0, 0, used); used = 0;
              controller.enqueue(bytes);
              return; // At most one complete frame is queued per downstream pull.
            }
          }
        }
      } catch {
        stop(); signal?.removeEventListener('abort', abortListener); controller.error(denied());
      }
    },
    cancel() { stop(); signal?.removeEventListener('abort', abortListener); },
  }, { highWaterMark: 1 });
}

export function createOneShotBoundary({ endpoint, expectedBody, expectedHeaders, bounds, signal }, physicalFetch) {
  const limits = validateBounds(bounds);
  if (endpoint !== 'https://api.openai.com/v1/responses' || typeof expectedBody !== 'string' ||
      Buffer.byteLength(expectedBody, 'utf8') > limits.requestBytes || typeof physicalFetch !== 'function') throw denied();
  const headers = new Headers(expectedHeaders);
  const authorization = headers.get('authorization');
  if (!authorization?.startsWith('Bearer ') || authorization.length <= 7 ||
      Buffer.byteLength(authorization) > limits.headerBytes) throw denied();
  const secret = authorization.slice(7);
  if (expectedBody.includes(secret)) throw denied();
  let consumed = false;
  return async function guardedFetch(url, init) {
    if (consumed) throw denied();
    consumed = true; // Never reset after invalid input, failure, timeout or lost response.
    if (signal?.aborted || url !== endpoint || init?.method !== 'POST' ||
        init.body !== expectedBody || ![undefined, 'error'].includes(init.redirect) ||
        !(init.signal instanceof AbortSignal) || Object.keys(init).some(key =>
          !['method', 'body', 'headers', 'redirect', 'signal'].includes(key)) ||
        Object.getOwnPropertySymbols(init).length !== 0) throw denied();
    const actual = new Headers(init.headers);
    let headerBytes = 0;
    for (const [key, value] of actual) {
      headerBytes += Buffer.byteLength(key) + Buffer.byteLength(value) + 4;
      if (headers.get(key) !== value) throw denied();
    }
    for (const [key, value] of headers) if (actual.get(key) !== value) throw denied();
    if (headerBytes > limits.headerBytes) throw denied();
    // SDK 6.40 replaces the original signal with its request controller. Keep
    // both authorities throughout the body; the SDK clears its timer at headers.
    const linkedSignal = signal ? AbortSignal.any([signal, init.signal]) : init.signal;
    const response = await physicalFetch(url, { method: 'POST', body: expectedBody,
      headers: actual, redirect: 'error', signal: linkedSignal });
    if (linkedSignal.aborted || !(response instanceof Response) || response.status !== 200 ||
        response.headers.get('content-type')?.split(';')[0].trim() !== 'text/event-stream' ||
        ![null, 'identity'].includes(response.headers.get('content-encoding'))) {
      await response?.body?.cancel().catch(() => {}); throw denied();
    }
    let responseHeaderBytes = 0;
    for (const [key, value] of response.headers) {
      responseHeaderBytes += Buffer.byteLength(key) + Buffer.byteLength(value) + 4;
      if (value.includes(secret)) { await response.body?.cancel(); throw denied(); }
    }
    if (responseHeaderBytes > limits.headerBytes) { await response.body?.cancel(); throw denied(); }
    return new Response(boundedBody(response, limits, linkedSignal, secret), { status: 200, headers: response.headers });
  };
}
