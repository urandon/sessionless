# Canonical frontend ingress

`internal/sessioningress` is the application boundary between an authenticated
frontend adapter and Sessionless-owned conversation state. Telegram, WebUI,
and later adapters provide opaque external identifiers; they do not create
transport-shaped runs or become the conversation source of truth.

## Authentication and binding

An adapter first resolves its external authentication result to an internal
`user_id`. `EnsureSession` then submits only:

- `tenant_id` and authenticated `user_id`;
- an opaque frontend name;
- an opaque external conversation ID;
- deterministic internal binding and initial-session IDs.

YDB requires an active tenant membership with write permission. An existing
binding additionally requires an active writable `SessionParticipant`. A
missing binding can create its initial session only after that membership
check; there is no implicit tenant or membership creation in canonical
ingress.

The clean-context operation creates a new active session and owner participant,
then switches the binding with an expected revision in the same serializable
transaction. It does not archive, truncate, or delete the former session. An
exact retry uses the same new session ID, old binding revision, and timestamp;
a competing stale request fails with `StaleBindingError`.

## Delivery envelope and idempotency

Every adapter supplies a stable external event ID and delivery timestamp. The
application service derives a tenant-scoped opaque idempotency key and stable
event/run/attempt/manifest/outbox IDs with HMAC-SHA-256 and a deployment secret
of at least 32 bytes. The secret is injected from the runtime environment or
Lockbox and is never committed. The persisted JSON envelope is versioned and
contains only:

- `FrontendEventOrigin` with binding ID/revision and opaque frontend,
  conversation, and event IDs;
- text and frontend-neutral string metadata;
- attachment names, media types, and canonical `BlobRef` values.

No Telegram chat, message, or update type appears in `CanonicalIngressStore`.
`internal/syntheticfrontend` exercises the same contract without a transport
SDK.

The frontend deduplication row is keyed by
`(tenant_id, binding_id, idempotency_key)` and stores the original session,
sequence, event, and run. This key is intentionally outside the current
session prefix: a delayed duplicate arriving after `/new` returns the original
result instead of creating an event in the new session. The ordinary
session-scoped idempotency row remains the invariant for direct event appends.

## Object and transaction boundary

Message envelopes and attachments are written before the YDB transaction to:

```text
tenants/<tenant-id>/sessions/<session-id>/events/<event-id>/uploads/<upload-token>/message.json
tenants/<tenant-id>/sessions/<session-id>/events/<event-id>/uploads/<upload-token>/attachments/...
```

Each ingestion attempt receives a cryptographically random upload namespace.
Two concurrent deliveries that both miss the preflight idempotency lookup
therefore cannot overwrite each other's bytes, even when their stable event
and run IDs are identical. The winning YDB transaction makes only its upload
references canonical; a losing attempt leaves separate unreferenced objects.

Every referenced object is revalidated against the tenant/session/event prefix
inside the transaction. A failed object write prevents the YDB operation from
starting. A failed YDB operation rolls back the event, both idempotency facts,
run, attempt, manifest, dispatch outbox, and session-sequence update together.

An object write followed by a rejected or uncertain YDB transaction may leave
an unreferenced immutable object. It is not canonical state and must be removed
by prefix lifecycle/garbage collection after the idempotency uncertainty
window. The ingress service does not eagerly delete it because transaction
outcome may be uncertain, while the per-attempt namespace prevents it from
damaging the committed payload.

## Transitional worker origin

New dispatch outboxes carry `FrontendEventOrigin`. The former Telegram
delivery fields remain an explicitly marked compatibility bridge for the
existing Telegram worker/result path. Issue #36 moves Telegram input to this
contract, while the canonical finalization/projection slice removes the worker
dependency on a Telegram reply target.

Ingress performs an authorized idempotency lookup before writing immutable
objects. A delayed duplicate resolves to the original session even when the
frontend binding has since switched to a newer session, and it does not write
new objects. If concurrent deliveries both miss that preflight lookup, the
transactional deduplication row still makes the first committed payload,
origin, and timestamp canonical; the retry may not rewrite them.

SESSION-04 issue #23 makes origin-only dispatches admissible. Their terminal
worker transaction appends canonical assistant/tool or terminal-notice events
and creates generic projection rows for every binding that still targets the
session. A binding that switched away while the worker was running receives no
projection; the canonical result remains in the original session. Projection
consumers must recheck the recorded binding revision before transport work.

Legacy Telegram-targeted jobs remain admissible and continue to create the
existing Telegram delivery outbox until issues #36 and #37 move that adapter to
the canonical ingress/projection path. That compatibility transaction is
isolated behind `LegacyTelegramWorkerStateStore`; the canonical
`WorkerStateStore` completion/failure contracts and their shared YDB
finalization helpers contain no transport-specific delivery value.

## Stateless worker context

Canonical ingress records the trigger event sequence as the context boundary.
Admission selects the newest compatible immutable snapshot at or before that
sequence and persists its version plus covered sequence on the worker job.
Workers reconstruct `context/history.jsonl` from the verified snapshot and
contiguous event tail. If snapshot metadata or bytes are missing, corrupt, or
incompatible, reconstruction retries an older snapshot and finally replays the
bounded canonical event prefix. The replay output is byte-identical to the
snapshot-plus-tail output.

After a canonical dispatch has been published and its outbox acknowledged, the
reconciler runs best-effort snapshot maintenance for that pinned boundary. It
creates a new immutable version only when at least
`SNAPSHOT_INTERVAL_EVENTS` (128 by default) are covered beyond the latest
snapshot. Catalog traversal is bounded by `SNAPSHOT_MAX_VERSIONS` (32 by
default), and snapshot event/byte work is bounded by the admitted
`LIMIT_CONTEXT_EVENTS` and `LIMIT_CONTEXT_BYTES`. The just-published job keeps
its already-pinned context; later jobs can select the new snapshot. Maintenance
failure is logged but does not fail dispatch: canonical replay remains the
correctness path and a later dispatch can retry the deterministic build.

`LIMIT_CONTEXT_EVENTS` is required for newly admitted jobs alongside the byte
and tool-event budgets. Jobs persisted before this field existed retain a
finite compatibility bound derived from their admitted turn limit. Event,
byte, historical tool-event, payload digest, tenant, session, and trigger
checks all complete before a harness adapter is invoked.

## Canonical terminal finalization

Harness progress boundaries remain operational checkpoints. They are not
canonical transcript events. At terminal success, the worker uploads immutable
payloads below the owning session/event prefix and submits, in order:

1. reconstructable `tool_call` and `tool_result` events returned by the harness;
2. exactly one `assistant_message` event referencing the result artifact
   manifest.

Tool event count and the total encoded tool-event payload bytes are bounded by
the limits admitted with the worker job. The worker validates and prepares the
whole tool-event batch before uploading any canonical tool payload.
Worker jobs persisted before those explicit limits were introduced remain
loadable during rolling upgrades: they use a finite compatibility budget of two
tool events per admitted turn and the admitted context-byte limit. All newly
admitted jobs persist explicit tool-event limits.

Terminal failure and cancellation append one structured `system_notice` with
the stable failure code and cancellation flag. Tool events are retained for
future stateless context reconstruction but do not automatically create
frontend delivery work. Assistant messages and terminal notices create one
`frontend_projection_outbox` row per binding that still targets the session at
commit time.

The fenced terminal YDB transaction atomically appends the ordered events,
updates the session sequence, creates projections, records the artifact
manifest and final run/attempt/quota state, clears scheduling state and writes a
`run_finalizations` digest. An exact callback retry is a no-op. A callback with
different event identities, kinds, idempotency keys, payload references or
validated manifest content fails with a finalization conflict. The YDB boundary
also enforces the status-specific event shape: one assistant event on success,
or exactly one system notice on failure/cancellation. A stale lease fails
before any canonical event or projection is committed.
