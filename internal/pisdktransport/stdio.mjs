// Private child half of SPU2. No SDK import, key, fetch, Source client or
// registration. These payload bounds are NOT a complete native fit/clock
// certificate; the original prepared producer must fund this work before Issue.
import { Buffer, isUtf8 } from 'node:buffer';
import { hrtime } from 'node:process';
import { Readable, Writable } from 'node:stream';

const denied = () => new Error('pi_stdio_denied');
const MAX_NS = (1n << 63n) - 1n;
const HEADER = 17;
const META = 129;
const FRAME = 1 << 20;
const clockDefault = Object.freeze({ now: () => hrtime.bigint(),
  schedule: (fn, ms) => setTimeout(fn, ms), cancel: timer => clearTimeout(timer) });
const positive = n => Number.isSafeInteger(n) && n > 0;

// The actual private entrypoint passes its same stdin/stdout, never a network
// proxy or extra descriptor. clock injection is a deterministic unit seam, not
// a platform-duration/provenance grant. Unknown real clock fit stays ineligible.
export function createPiStdioChannel({ input, output, limits, signal, clock = clockDefault }) {
  if (!(input instanceof Readable) || !(output instanceof Writable) || input.destroyed || output.destroyed ||
      !(signal instanceof AbortSignal) || signal.aborted || !Object.isFrozen(limits) ||
      Object.keys(limits).length !== 4 ||
      !['initialize_body_bytes', 'request_body_bytes', 'final_body_bytes', 'frame_bytes'].every(k => positive(limits[k])) ||
      limits.frame_bytes < 147 || limits.frame_bytes > FRAME ||
      limits.initialize_body_bytes > limits.frame_bytes - HEADER ||
      limits.request_body_bytes > limits.frame_bytes - HEADER - META ||
      limits.final_body_bytes > limits.frame_bytes - HEADER ||
      !Object.isFrozen(clock) || typeof clock.now !== 'function' ||
      typeof clock.schedule !== 'function' || typeof clock.cancel !== 'function') throw denied();
  // Fixed payload arena; no Buffer.concat, read-all, payload-sized stream read
  // or allocation based on an uninspected declaration. Node/pipe/native buffers
  // and these Go/JS copies still belong in the original full pre-Issue ledger.
  const arena = Buffer.alloc(limits.frame_bytes);
  const header = Buffer.alloc(HEADER);
  const allowance = Buffer.alloc(9);
  const use = new AbortController();
  let state = 'new', closing = false, last, deadline, timer;
  let body, requestHeader, metadata, initialization, exchange, monitor, publication;
  let inputBytes = 0, outputBytes = 0;
  const inputCap = limits.initialize_body_bytes + 60;
  const outputCap = limits.request_body_bytes + limits.final_body_bytes + 163;
  const waits = new Set();
  // Installed before first destroy/IO, so disposal can join actual close and
  // remove precisely its own listeners without swallowing unrelated errors.
  const inputClosed = new Promise(resolve => input.once('close', resolve));
  const outputClosed = new Promise(resolve => output.once('close', resolve));
  const stop = () => {
    if (closing) return;
    closing = true; state = 'closed';
    if (timer !== undefined) { try { clock.cancel(timer); } catch { /* never grant on timer failure */ } timer = undefined; }
    use.abort();
    for (const cancel of [...waits]) cancel();
    input.destroy(); output.destroy();
  };
  const abort = () => stop();
  signal.addEventListener('abort', abort, { once: true });
  // Never forward raw stream errors to logs or retain task/key-bearing causes.
  const streamError = () => stop();
  input.on('error', streamError); output.on('error', streamError);
  const alive = () => { if (closing || signal.aborted || use.signal.aborted) throw denied(); };
  const now = () => {
    const n = clock.now();
    if (typeof n !== 'bigint' || n < 0n || (last !== undefined && n < last)) throw denied();
    last = n; return n;
  };
  const assertUsable = () => {
    try { alive(); if (deadline === undefined || now() >= deadline) throw denied(); }
    catch { stop(); throw denied(); }
    // Synchronous undefined ONLY, consumed immediately by the actual fetch guard.
  };
  const armTimer = () => {
    alive();
    const left = deadline - now();
    if (left <= 0n) { stop(); return; }
    // Floor to milliseconds so conversion never grants later permission.
    // Long native timeouts are split without Number nanosecond conversion or
    // the setTimeout overflow-to-1ms trap. Synchronous assertion is authoritative.
    const ms = Number(left / 1000000n > 2147483647n ? 2147483647n : left / 1000000n);
    timer = clock.schedule(() => {
      timer = undefined;
      try {
        // For a sub-ms remainder conservatively revoke; never spin zero timers.
        if (deadline - now() < 1000000n) stop(); else armTimer();
      } catch { stop(); }
    }, ms);
  };
  const waitReadable = () => new Promise((resolve, reject) => {
    const cleanup = () => {
      waits.delete(cancel); input.off('readable', ready); input.off('end', cancel); input.off('close', cancel);
    };
    const ready = () => { cleanup(); resolve(); };
    const cancel = () => { cleanup(); reject(denied()); };
    waits.add(cancel); input.once('readable', ready); input.once('end', cancel); input.once('close', cancel);
    if (closing || input.destroyed || input.readableEnded) cancel();
  });
  const readFull = async target => {
    let offset = 0;
    while (offset < target.length) {
      alive();
      // Bounded reads tolerate arbitrary legal fragmentation without asking Node to
      // grow its high-water mark to an attacker-declared frame length.
      const part = input.read(Math.min(target.length - offset, 16384));
      if (part === null) { await waitReadable(); continue; }
      if (!Buffer.isBuffer(part) || part.length === 0 || part.length > target.length - offset ||
          part.length > inputCap - inputBytes) throw denied();
      part.copy(target, offset); inputBytes += part.length; offset += part.length;
    }
  };
  const inspect = expected => {
    if (!exactMagic()) throw denied();
    const kind = header[4], meta = header.readUInt32BE(5), size = header.readBigUInt64BE(9);
    // Direction and expected state checked BEFORE any payload read/allocation.
    if (kind !== expected && kind !== 5) throw denied();
    if (kind === 5) {
      if (meta !== 0 || size !== 0n) throw denied();
      stop(); throw denied();
    }
    if (kind === 1 && (meta !== 0 || size === 0n || size > BigInt(limits.initialize_body_bytes))) throw denied();
    if (kind === 3 && (meta !== 9 || size !== 0n)) throw denied();
    const total = BigInt(HEADER + meta) + size;
    if (total > BigInt(limits.frame_bytes) || BigInt(meta) + size > BigInt(inputCap - inputBytes)) throw denied();
    return Number(size);
  };
  // ASCII decoding masks high bits. A canonical binary magic is byte identity,
  // never a permissive textual decoder (e.g. d3d0d5b2 must NOT mean SPU2).
  const exactMagic = () => header[0] === 0x53 && header[1] === 0x50 && header[2] === 0x55 && header[3] === 0x32;
  const writeFull = async data => {
    if (data.length > outputCap - outputBytes) throw denied();
    for (let offset = 0; offset < data.length;) {
      alive();
      const part = data.subarray(offset, Math.min(data.length, offset + 16384));
      await new Promise((resolve, reject) => {
        // Destroy the SAME owned stream on cancellation, but do not declare
        // the payload released until its actual write callback has settled.
        // A nonconforming injected writer that ignores destroy is not joined
        // by timing out/detaching a promise and clearing its borrowed buffer.
        output.write(part, error => {
          if (error || closing) reject(denied()); else resolve();
        });
      });
      offset += part.length; outputBytes += part.length;
    }
  };
  const makeHeader = (kind, meta, size) => {
    const h = Buffer.alloc(HEADER); h.write('SPU2'); h[4] = kind;
    h.writeUInt32BE(meta, 5); h.writeBigUInt64BE(BigInt(size), 9); return h;
  };
  const utf8 = (text, cap) => {
    if (typeof text !== 'string' || text.length === 0 || text.length > cap || !text.isWellFormed() || Buffer.byteLength(text) > cap) throw denied();
    return Buffer.from(text, 'utf8');
  };
  const initialize = () => {
    if (state !== 'new') { stop(); return Promise.reject(denied()); }
    state = 'initializing';
    initialization = (async () => {
      try {
        await readFull(header); const n = inspect(1);
        const initial = arena.subarray(0, n); await readFull(initial);
        if (!isUtf8(initial)) throw denied();
        alive(); state = 'ready'; return initial; // borrowed until dispose, NOT a parsed Init schema
      } catch { stop(); throw denied(); }
    })();
    return initialization;
  };
  const watchRetire = async () => {
    try {
      await readFull(header);
      // Once Use is accepted the ONLY possible remaining parent frame is Retire.
      if (!exactMagic() || header[4] !== 5 ||
          header.readUInt32BE(5) !== 0 || header.readBigUInt64BE(9) !== 0n) throw denied();
      stop();
    } catch { if (!closing) stop(); }
  };
  const bindRequest = (originalBody, profileDigest) => {
    try {
      alive();
      if (state !== 'ready' || typeof profileDigest !== 'string' || !/^[a-f0-9]{64}$/.test(profileDigest) || profileDigest === '0'.repeat(64)) throw denied();
      body = utf8(originalBody, limits.request_body_bytes);
      metadata = Buffer.from(`{"version":1,"phase":"physical_send","backend_profile_digest":"${profileDigest}"}`, 'utf8');
      if (metadata.length !== META) throw denied();
      requestHeader = makeHeader(2, META, body.length); state = 'bound';
    } catch { stop(); throw denied(); }
    return parentUse;
  };
  // Declared in owner scope, not the bindRequest parameter scope: asynchronous
  // callbacks retain only explicitly clearable slots, not original string args.
  const parentUse = Object.freeze({ authorizePhysicalSend(...args) {
      if (args.length !== 0 || state !== 'bound' || closing) { stop(); return Promise.reject(denied()); }
      state = 'requesting';
      exchange = (async () => {
        try {
          // All full-body/header preparation precedes s; every actual wire write
          // and complete Use read is inside this same measured exchange.
          const s = now();
          await writeFull(requestHeader); await writeFull(metadata); await writeFull(body);
          await readFull(header); inspect(3); await readFull(allowance);
          const r = now(), n = allowance.readBigUInt64BE(1), elapsed = r - s;
          if (allowance[0] !== 1 || n === 0n || n > MAX_NS || elapsed < 0n || elapsed >= n) throw denied();
          deadline = r + (n - elapsed); // s+N, NEVER receipt-now+N
          alive(); state = 'usable'; armTimer(); assertUsable();
          monitor = watchRetire(); // catches EOF/late duplicate/Retire throughout SAME fetch
          return Object.freeze({ signal: use.signal, assertUsable });
        } catch { stop(); throw denied(); }
      })();
      return exchange;
    } });
  const publishFinal = text => {
    if (state !== 'usable' || closing) { stop(); return Promise.reject(denied()); }
    state = 'publishing';
    publication = (async () => {
      let final;
      try {
        assertUsable(); final = utf8(text, limits.final_body_bytes);
        await writeFull(makeHeader(4, 0, final.length)); await writeFull(final);
        alive(); state = 'final';
      } catch { stop(); throw denied(); }
      finally { final?.fill(0); }
    })();
    return publication;
  };
  const dispose = async () => {
    stop();
    await Promise.allSettled([initialization, exchange, monitor, publication]);
    await Promise.all([inputClosed, outputClosed]);
    arena.fill(0); header.fill(0); allowance.fill(0); body?.fill(0); requestHeader?.fill(0); metadata?.fill(0);
    signal.removeEventListener('abort', abort);
    input.off('error', streamError); output.off('error', streamError);
  };
  return Object.freeze({ initialize, bindRequest, publishFinal, dispose, signal: use.signal });
}
