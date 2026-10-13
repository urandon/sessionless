import { createSSEFramer } from './framing.mjs';

// Private trusted-fetch-owner plumbing, not the semantic/effect allowlist.
// Runtime header/chunk allocation, native buffers and the whole source ledger
// must be qualified before use; these checks cannot undo earlier allocations.
const failureCodes = new WeakMap();
const failure = (code) => {
  const error = new Error(code);
  error.name = 'PiResponseGuardError';
  failureCodes.set(error, code);
  return error;
};
const invalid = () => { throw failure('pi_response_configuration_invalid'); };
// Count before copying: do not allocate an encoded copy of an oversized header.
const bytes = (text, cap) => {
  let count = 0;
  for (let index = 0; index < text.length; index++) {
    const code = text.charCodeAt(index);
    if (code < 0x80) count++;
    else if (code < 0x800) count += 2;
    else if (code >= 0xd800 && code <= 0xdbff && index + 1 < text.length &&
      text.charCodeAt(index + 1) >= 0xdc00 && text.charCodeAt(index + 1) <= 0xdfff) {
      count += 4; index++;
    } else count += 3;
    if (count > cap) throw failure('pi_response_metadata_denied');
  }
  return count;
};

export function createGuardedResponse(options) {
  if (!options || typeof options !== 'object') invalid();
  const { response, limits, originalSignal, sdkSignal, fetchController, validateFrame } = options;
  // All arguments belong to a fixed trusted owner, never provider/user options.
  if (!(response instanceof Response) || !(originalSignal instanceof AbortSignal) ||
      !(sdkSignal instanceof AbortSignal) || !(fetchController instanceof AbortController) ||
      typeof validateFrame !== 'function') {
    // Failed configuration still owns an already-fetched response. Only use
    // genuine, valid owned capabilities; never invoke an invalid object here.
    if (response instanceof Response && fetchController instanceof AbortController) {
      try { if (!fetchController.signal.aborted) fetchController.abort(); } catch { /* safe cleanup */ }
      if (response.body && !response.body.locked) {
        try { Promise.resolve(response.body.cancel()).catch(() => {}); } catch { /* safe cleanup */ }
      }
    }
    invalid();
  }
  // The parent must already arm originalSignal from its original deadline.
  // SDK signal is borrowed read-only; fetchController owns the SAME physical
  // fetch. These are integration prerequisites, not capabilities proved here.
  let framer;
  let reader;
  let iterator;
  let output;
  let stopped = false;
  let terminal = false;
  const signals = [...new Set([originalSignal, sdkSignal, fetchController.signal])];

  // Never await cancellation controlled by the upstream producer. Native
  // reader.cancel closes pending reads; releaseLock is attempted exactly once.
  const stop = () => {
    if (stopped) return;
    stopped = true;
    for (const signal of signals) signal.removeEventListener('abort', onAbort);
    const pending = iterator;
    iterator = undefined;
    try { pending?.return(); } catch { /* retain the primary safe failure */ }
    framer?.close();
    try { if (!fetchController.signal.aborted) fetchController.abort(); } catch { /* trusted cleanup */ }
    if (reader) {
      try { Promise.resolve(reader.cancel()).catch(() => {}); } catch { /* safe cleanup only */ }
      try { reader.releaseLock(); } catch { /* never replace primary failure */ }
    } else if (response.body && !response.body.locked) {
      try { Promise.resolve(response.body.cancel()).catch(() => {}); } catch { /* safe cleanup only */ }
    }
  };
  const deny = (code) => {
    const error = failure(code);
    stop();
    try { output?.error(error); } catch { /* already closed/canceled */ }
    return error;
  };
  function onAbort() { if (!stopped) deny('pi_response_aborted'); }

  const descriptor = limits && Object.getOwnPropertyDescriptor(limits, 'response_header_bytes');
  if (!descriptor || !Object.hasOwn(descriptor, 'value') ||
      !Number.isSafeInteger(descriptor.value) || descriptor.value <= 0) { stop(); invalid(); }
  try { framer = createSSEFramer(limits); } catch { stop(); invalid(); }

  // Header objects are preallocated by the HTTP runtime. Count the complete
  // normalized metadata now, before copying/forwarding it to the genuine SDK.
  try {
    let count = bytes(String(response.status), descriptor.value);
    count += bytes(response.statusText, descriptor.value - count);
    if (count > descriptor.value) throw failure('pi_response_metadata_denied');
    for (const [name, value] of response.headers) {
      let size = bytes(name, descriptor.value - count);
      size += bytes(value, descriptor.value - count - size) + 4;
      if (size > descriptor.value - count) throw failure('pi_response_metadata_denied');
      count += size;
    }
    const type = response.headers.get('content-type');
    const encoding = response.headers.get('content-encoding');
    const length = response.headers.get('content-length');
    if (response.status !== 200 || response.redirected || !response.body || response.body.locked ||
        !type || !/^text\/event-stream(?:\s*;\s*charset=utf-8)?$/i.test(type) ||
        (encoding !== null && encoding.toLowerCase() !== 'identity') ||
        (length !== null && (!/^(0|[1-9][0-9]*)$/.test(length) ||
          !Number.isSafeInteger(Number(length)) || Number(length) > limits.response_body_bytes))) {
      throw failure('pi_response_metadata_denied');
    }
    if (signals.some((signal) => signal.aborted)) throw failure('pi_response_aborted');
    reader = response.body.getReader();
  } catch (error) {
    stop();
    throw failure(failureCodes.get(error) === 'pi_response_aborted' ? 'pi_response_aborted' : 'pi_response_metadata_denied');
  }

  const stream = new ReadableStream({
    start(controller) {
      output = controller;
      for (const signal of signals) signal.addEventListener('abort', onAbort, { once: true });
      // Also covers cancellation between acquiring the reader and subscribing.
      if (signals.some((signal) => signal.aborted)) onAbort();
    },
    async pull(controller) {
      try {
        while (!stopped) {
          if (iterator) {
            const next = iterator.next();
            if (!next.done) {
              // Synchronous fixed read-only trusted policy: no retention or
              // mutation of this frame. Promises/unknown decisions
              // fail closed; this module does not supply or infer that policy.
              const decision = validateFrame(next.value);
              if (stopped) return;
              if (decision instanceof Promise) Promise.prototype.then.call(decision, () => {}, () => {});
              if (decision !== 'continue' && decision !== 'terminal') throw failure('pi_response_frame_denied');
              controller.enqueue(next.value);
              if (decision === 'terminal') {
                terminal = true;
                controller.close();
                stop();
              }
              return; // exactly one original frame per downstream pull
            }
            iterator = undefined;
          }
          const next = await reader.read();
          if (stopped) return;
          if (next.done) {
            framer.finish();
            if (!terminal) throw failure('pi_response_incomplete');
            return;
          }
          if (!(next.value instanceof Uint8Array) || next.value.byteLength === 0) {
            throw failure('pi_response_frame_denied');
          }
          iterator = framer.feed(next.value);
        }
      } catch (error) {
        if (!stopped) deny(failureCodes.get(error) === 'pi_response_incomplete' ?
          'pi_response_incomplete' : 'pi_response_frame_denied');
      }
    },
    cancel() { stop(); },
  }, { highWaterMark: 0 });
  try { return new Response(stream, { status: 200, headers: response.headers }); }
  catch { throw deny('pi_response_metadata_denied'); }
}
