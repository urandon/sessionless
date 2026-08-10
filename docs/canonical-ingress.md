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
tenants/<tenant-id>/sessions/<session-id>/events/<event-id>/message.json
tenants/<tenant-id>/sessions/<session-id>/events/<event-id>/attachments/...
```

Every referenced object is revalidated against the tenant/session/event prefix
inside the transaction. A failed object write prevents the YDB operation from
starting. A failed YDB operation rolls back the event, both idempotency facts,
run, attempt, manifest, dispatch outbox, and session-sequence update together.

An object write followed by a rejected or uncertain YDB transaction may leave
an unreferenced immutable object. It is not canonical state and must be removed
by prefix lifecycle/garbage collection after the idempotency uncertainty
window. The ingress service does not eagerly delete it because a concurrent
retry may already have committed the same deterministic object key.

## Transitional worker origin

New dispatch outboxes carry `FrontendEventOrigin`. The former Telegram
delivery fields remain an explicitly marked compatibility bridge for the
existing Telegram worker/result path. Issue #36 moves Telegram input to this
contract, while the canonical finalization/projection slice removes the worker
dependency on a Telegram reply target.
