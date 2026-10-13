// Private byte-framing predecessor for Pi 6c4f3602/OpenAI SDK 6.40.
// This does NOT validate JSON/effects, qualify allocations, authorize a send,
// or replace the SDK/Pi SSE parsers. Its owner must validate each complete
// original frame before delivery, under bounded downstream backpressure.

const invalid = () => { throw new Error('pi_sse_limits_invalid'); };
const fields = Object.freeze([
  'response_body_bytes', 'response_chunk_bytes', 'sse_line_bytes',
  'sse_event_bytes', 'sse_events', 'response_copy_work_bytes',
  'response_allocation_bytes',
]);

// Input is the trusted canonical native Limits object, not model/user options.
// These are existing P wire names; no guessed production defaults or aliases.
function snapshotLimits(input) {
  if (!input || !Object.isFrozen(input) ||
      ![Object.prototype, null].includes(Object.getPrototypeOf(input))) invalid();
  const limits = Object.create(null);
  for (const field of fields) {
    const descriptor = Object.getOwnPropertyDescriptor(input, field);
    if (!descriptor || !Object.hasOwn(descriptor, 'value') ||
        !Number.isSafeInteger(descriptor.value) || descriptor.value <= 0) invalid();
    limits[field] = descriptor.value;
  }
  if (limits.response_chunk_bytes > limits.response_body_bytes ||
      limits.sse_line_bytes > limits.sse_event_bytes ||
      limits.sse_event_bytes > limits.response_body_bytes ||
      limits.response_copy_work_bytes < limits.response_body_bytes ||
      limits.sse_event_bytes > Math.floor(limits.response_allocation_bytes / 2)) invalid();
  return Object.freeze(limits);
}

export function createSSEFramer(input) {
  const limits = snapshotLimits(input);
  let arena;
  try { arena = new Uint8Array(limits.sse_event_bytes); } catch { invalid(); }
  let closed = false;
  let active = false;
  let used = 0;
  let lineBytes = 0;
  let bodyBytes = 0;
  let copyWorkBytes = 0;
  let events = 0;
  let previous = -1;
  let beforePrevious = -1;
  let thirdPrevious = -1;
  const close = () => {
    if (closed) return;
    closed = true;
    arena.fill(0);
    used = 0;
  };
  const deny = () => { close(); throw new Error('pi_sse_frame_denied'); };
  const chargeCopy = (bytes) => {
    if (bytes > limits.response_copy_work_bytes - copyWorkBytes) deny();
    copyWorkBytes += bytes;
  };

  function* feed(chunk) {
    if (closed || active) deny();
    // Bytes are owned by the trusted response reader, not a hostile caller.
    if (!(chunk instanceof Uint8Array) ||
        chunk.byteLength > limits.response_chunk_bytes ||
        chunk.byteLength > limits.response_body_bytes - bodyBytes) deny();
    bodyBytes += chunk.byteLength;
    active = true;
    let completed = false;
    try {
      for (let index = 0; index < chunk.byteLength; index++) {
        if (closed || used === limits.sse_event_bytes) deny();
        const byte = chunk[index];
        // SDK LineDecoder uses CR, LF or CRLF. Terminators are not line data;
        // they still count in the complete event and total wire-byte budgets.
        if (byte === 10 || byte === 13) lineBytes = 0;
        else if (++lineBytes > limits.sse_line_bytes) deny();
        chargeCopy(1);
        arena[used++] = byte;
        // Exact SDK findDoubleNewlineIndex patterns; O(1) cross-chunk state.
        const boundary = (byte === 10 && previous === 10) ||
          (byte === 13 && previous === 13) ||
          (byte === 10 && previous === 13 && beforePrevious === 10 && thirdPrevious === 13);
        thirdPrevious = beforePrevious;
        beforePrevious = previous;
        previous = byte;
        if (boundary) {
          if (events === limits.sse_events) deny();
          events++;
          chargeCopy(used);
          // Fresh owned copy: subsequent arena reuse cannot change this frame.
          const frame = arena.slice(0, used);
          used = 0;
          previous = beforePrevious = thirdPrevious = -1;
          yield frame;
        }
      }
      completed = true;
    } finally {
      active = false;
      // A consumer that stops midway cannot resume/replay the remaining bytes.
      if (!completed) close();
    }
  }
  const finish = () => {
    if (closed || active || used !== 0) deny();
    close();
  };
  return Object.freeze({ feed, finish, close });
}
