# Attached-worker identity and enrollment

This document defines the AW-01 identity boundary for a user-owned attached
worker. It covers enrollment, durable identity, optimistic mutation, and
revocation. It does not define the worker transport, capability negotiation,
provider or credential selection, dispatch, or execution protocol.

## Authority and scope

An attached worker is owned by exactly one user inside one tenant. Every
durable lookup and mutation is keyed by `(tenant_id, owner_user_id, ...)`.
The authenticated tenant and owner are authoritative server-side inputs; they
are not accepted from request bodies. A worker or enrollment belonging to a
different tenant or owner is indistinguishable from a missing resource.

`worker_id` and `enrollment_id` are opaque, random, non-sortable identifiers.
Transport connection IDs, process IDs, hostnames, and provider-native session
IDs never become product worker identity.

## Enrollment lifecycle

The control plane creates a short-lived enrollment containing:

- the authoritative tenant and owner;
- a preallocated worker ID and display name;
- an exact audience;
- a SHA-256 digest of a random 32-byte bootstrap secret;
- creation, expiry, retention, and revision fields.

Only the transient grant returned to the owner contains the raw bootstrap
secret. Formatting and JSON serialization redact it, and persistence receives
only the digest. The enrollment row remains available until `retain_until`,
which is later than `expires_at`; YDB TTL is applied to retention rather than
bootstrap validity so a consumed or expired grant cannot immediately lose its
replay evidence.

The worker generates an Ed25519 key pair. Claim requires the exact owner scope,
enrollment ID, audience, bootstrap secret, new public key, and an Ed25519 proof
over the canonical claim transcript. The transcript is a sequence of unsigned
32-bit big-endian length-prefixed byte strings:

```text
sessionless.attached-worker.enrollment-proof.v1
tenant_id
owner_user_id
enrollment_id
worker_id
audience
bootstrap_digest
new_identity_public_key
enrollment_revision (8-byte big-endian)
expires_at Unix nanoseconds (8-byte big-endian)
```

The service verifies the proof before persistence. The store then atomically
rechecks scope, audience, digest, single-use state, expiry, and revision using
its transaction timestamp; marks the enrollment consumed; creates the worker;
and appends its content-free audit event at that same timestamp. Exactly one
concurrent claimant can create the worker. At the exact expiry boundary the
enrollment is expired. A wrong audience or secret is denied before consumed or
expired state is revealed.

The signed expected enrollment revision is immutable replay evidence. After an
ambiguous commit, the exact same request may reconcile as claimed only while
the revision-one worker and its audit event remain the pristine original
target. A different key or any later rename, rotation, connection-generation
advance, or revocation returns consumed instead of weakening the single-use
boundary.

## Worker state and fences

A newly claimed worker starts with:

```text
desired_state          = active
observed_state         = offline
revision               = 1
enrollment_generation  = 1
connection_generation  = 0
```

`revision` is the optimistic concurrency fence for metadata changes.
`enrollment_generation` invalidates older identity/enrollment authority.
`connection_generation` invalidates older live-connection authority. All three
are monotonic and fail closed before unsigned integer overflow.

Rename changes only the display name. Identity rotation advances the revision
and enrollment generation and requires both the current and new Ed25519 keys
to sign the same canonical transcript:

```text
sessionless.attached-worker.identity-rotation-proof.v1
tenant_id
owner_user_id
worker_id
expected_revision (8-byte big-endian)
current_enrollment_generation (8-byte big-endian)
current_connection_generation (8-byte big-endian)
new_identity_public_key
```

Requiring both proofs prevents an actor holding only the current private key
from silently binding a key it does not control, and prevents a claimant
holding only the new key from replacing the existing identity. A replayed,
old-key, missing, or stale-revision proof fails.

Connection generation advancement changes only that fence. AW-02 will define
which protocol event is allowed to request it and how a connection presents
the current generation.

## Deny-first revocation

Revocation is a dedicated compare-and-swap mutation. It atomically:

- sets `desired_state=revoked`;
- advances both enrollment and connection generations;
- advances the worker revision;
- records `revoked_at`;
- appends a V1 audit event.

The stored `observed_state` is deliberately preserved. A control-plane write
cannot claim that an offline worker observed the revocation. Later transport
work may record acknowledgement under a separately defined transition, but no
old identity or connection generation remains eligible after the deny-first
commit.

## Persistence and pagination

Migrations `00077` through `00079` create owner-scoped worker, enrollment, and
audit tables. Serving operations are exact composite-key reads or bounded
ordered owner-prefix reads. Worker lists use an exclusive `after_worker_id`
cursor and a limit from 1 through 100.

The audit table contains no bootstrap material, public keys, signatures,
capabilities, provider data, transport details, prompts, or credentials. Its
V1 actions are enrollment creation and claim, rename, identity rotation,
connection-generation advancement, and revocation. Audit pagination uses an
inclusive `from_worker_revision` cursor so zero returns the enrollment-created
event at revision zero. The next page begins at `last_worker_revision + 1`;
callers must fail before overflow. The limit is from 1 through 100.

## Deliberate non-goals

AW-01 does not authorize a worker to execute anything. It does not include:

- network attachment, heartbeats, leases, or reconnect behavior;
- protocol versions or capability manifests;
- provider, model, billing, entitlement, credential, or harness selection;
- task dispatch, payload egress, tool calls, approvals, or result commit;
- remote observed-state transitions or worker software updates.

Those boundaries remain separate so possession of an enrollment or identity
key cannot silently imply execution, data-egress, spend, credential, or tool
authority. AW-02 builds the versioned connection protocol on this identity
contract.
