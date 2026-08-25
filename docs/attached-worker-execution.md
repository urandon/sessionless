# Attached worker execution authority (AW-04a)

AW-04a composes owner-scoped attached workers with the canonical Sessionless
scheduler and lease/finalization transactions. It deliberately does not close
all of AW-04: reconnect recovery, drain completion, retry after ambiguous
external effects, cloud wake-up, and daemon integration remain follow-up work.

## Placement and admission

Every dispatch outbox and WorkerJob has a versioned execution placement.
`attached_worker` placement binds the owner, worker, capability digest, policy
digest, and `fallback=deny`. Admission point-loads that exact worker and current
connection. An offline, expired, revoked, generation-stale, capability-mismatched,
or already-busy worker makes the transaction fail; the dispatch is never put on
the managed worker queue.

This is an explicit schema cutover, not a runtime compatibility fallback.
Before the first rollout, stop every dispatch writer and reader, acquire the
deployment lock, and run `make partition-backfill`. Its serializable cutover
transaction requires both legacy `dispatch_outbox` and `worker_jobs` empty and
writes `execution-placement-v1-empty-cutover`. The current no-production-data
environment must run the typed pre-production reset first; this is a fresh
environment cutover, not a rolling-upgrade claim. Web BFF, control API,
reconciler, and worker runtime all refuse startup until the marker exists.
Missing placement remains invalid in every serving reader.

The same admission transaction creates the canonical WorkerJob, quota
reservation, attached attempt head, first LeaseOffer message, and deadline. A
retry-stable lease ID is derived from tenant/run/attempt. The non-renewable
lease lifetime is the admitted maximum runtime plus a 30-second finalization
budget, capped at 24 hours. The quota reservation is held through that exact
lease expiry.

## Single durable authority

The current connection row owns the canonical AW-02 machine snapshot. The
owner-scoped attempt head stores the canonical run, attempt, reservation,
lease generation, opaque derived fence, input/capability/policy digests and
protocol attempt watermarks. A bounded directional message ledger stores exact
frame fingerprints and canonical batches. A 16-bucket composite deadline index
supports bounded recovery without scanning worker or attempt tables.
Cancellation messages also retain their acknowledgement deadline; terminal
evidence and acknowledgements retain authoritative reservation and connection
identities for replay after the singleton attempt head is reused.

Each state change is one serializable YDB transaction:

```text
admission -> durable LeaseOffer
LeaseClaim -> lease claim + WorkerJob start + LeaseAccepted
Progress   -> protocol snapshot + exact progress evidence
Cancel     -> durable Cancel + bounded acknowledgement deadline
Terminal   -> terminal-pending evidence only
commit     -> canonical worker finalization + TerminalAck
```

Caller-supplied revisions, fence strings, outbound frames, context digests and
placement are not authority. The store derives them from the current rows and
canonical protocol reducers. Exact ambiguous-response retries reconcile to the
already-persisted target; divergent same-ID or same-sequence payloads conflict.
The context digest commits the loaded input manifest's canonical artifact
names, media types, blob keys, sizes and SHA-256 values, not merely its mutable
manifest ID.

## Cancellation, expiry, and unknown outcomes

Cancellation before claim writes a replayable Cancel, removes the lease
deadline, and replaces it with a bounded acknowledgement deadline. The attempt
cannot subsequently be claimed. If the worker never acknowledges that known
pre-execution cancellation, expiry atomically advances the worker connection
generation, supersedes the old connection, and retires the attempt; reuse then
requires a fresh attachment under the new generation. Cancellation after claim
also records a bounded acknowledgement deadline. If that deadline expires
without a valid CancelAck, the attempt moves to `fenced_unknown` while retaining
the original signed Cancel frame and protocol snapshot. The platform does not
emit a divergent replacement frame or claim that the worker process stopped.

Lease expiry fences the current attempt under the exact generation and stale
deadline-row revision. Deadline pages advance using the raw composite key even
when a corrupt legacy index row is skipped and counted, so one poison row
cannot block later work. Equal microsecond timestamps cannot skip work.
`fenced_unknown` blocks
automatic retry and terminal finalization until a later explicit resolution
contract exists.

## Terminal materialization boundary

A worker Terminal frame is evidence, not canonical product output. It pins an
exact status and evidence digest in `terminal_pending`. TerminalAck is created
only in the transaction that successfully applies the already-validated
server-side materialization:

- `succeeded` requires a canonical WorkerCompletion;
- `failed` requires a non-cancelled WorkerFailure;
- `cancelled` requires a cancelled WorkerFailure.

That transaction rechecks the current lease ID and numeric fence, updates the
canonical run/attempt/reservation and artifacts/events, advances the protocol
snapshot, and persists the exact TerminalAck. Duplicate completion returns the
same acknowledgement even after the worker ACK retires the head or a later
offer replaces it: retained terminal/ack evidence and canonical run
finalization reconcile the exact request. A different digest, status, attempt,
lease, worker or generation is rejected. Once the worker ACK covers the exact
TerminalAck, one transaction erases the committed protocol attempt, marks the
durable head retired, and makes the connection eligible for a distinct run.

## Remaining AW-04 gates

This slice stays feature-gated and does not by itself complete issue #76. The
remaining gates are:

1. reconnect-before/after-terminal recovery using the durable machine snapshot
   and replay commitments;
2. durable drain admission closure and `drained` only after zero active or
   unknown attempts;
3. explicit retry policy that always allocates a new attempt, lease and fence
   after a resolved retryable outcome and never reuses ambiguous effects;
4. worker daemon/process supervision and Codex execution integration;
5. a production cloud delivery choice: timer polling, paid bounded long poll,
   per-worker broker capability, or an always-on gateway.
