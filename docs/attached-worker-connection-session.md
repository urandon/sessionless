# Attached-worker connection session

Issue #109 defines the worker-side owner for one initial AW-03 connection. The
`internal/attachedworkersession` package composes the existing local-state,
bootstrap HTTP, authenticated exchange, transport-proof, and AW-02 conformance
types. It does not make the feature-disabled foreground constructor live and
does not start a poller, daemon, provider, process, or OCI launcher.

## Authority and ownership

One `Connector` may create at most one `Session`. The connector acquires the
kernel-backed local runtime lease before reading any private material and the
session retains that lease until `Close`. This prevents a second process from
logging out, updating generations, or starting another connection owner while
an activation or exchange may still be in flight.

The session binds every frame and response to the exact:

- tenant, owner, and worker;
- enrollment and connection generation;
- selected AW-02 protocol version;
- server-issued connection identity;
- immutable capability digest and authentication expiry.

The AW-02 `ConformanceMachine` validates outgoing worker frames and incoming
platform frames on a cloned snapshot. The clone replaces session state only
after the immediate exchange has a validated result. Exact current platform
replay is idempotent; a divergent replay, cross-scope frame, generation change,
protocol downgrade, or malformed response fails closed.

## Durable versus ephemeral facts

Before the first network operation, the connector advances the existing local
manifest's connection generation and stores a newly generated 32-byte
connection secret in the matching secret record. The existing store commits
those two files as separate atomic renames, secret first. A crash between the
renames is therefore detected as `state_incomplete`; a completed two-file
update is a durable attempt fence and a restart returns
`reconciliation_required` without retrying. Neither outcome is evidence that
the server accepted the connection.

Issue #123 adds a [strict, secret-free local checkpoint](attached-worker-reconnect-checkpoint.md)
for the exact AW-02 machine state, connection binding, protocol offers, and
capability digest. A restarted connector may use that checkpoint only as a
signed claim: #122 compares it with the exact durable server snapshot before
advancing the connection generation. Private keys, connection secrets, bearers,
and request/response bodies remain outside the checkpoint. Issue #124 extends
the claim with the bounded active-attempt summary and optional terminal replay
commitment, then reconciles it against the exact AW-04 ledger.

## Credential custody and evidence

Raw private keys, connection secrets, bearers, nonces, and proofs are copied
only at explicit signing or adapter boundaries and are cleared on return where
Go permits best-effort byte erasure. Existing redacted transport and local
secret types remain the only credential carriers. `ConnectionBindingV1`,
`SnapshotV1`, errors, `String`, and `GoString` contain no bearer, secret,
private key, nonce, proof, endpoint, environment, or protocol payload.

The `ExchangeFactory` receives one temporary bearer copy and must copy any
credential material it retains. A later concrete HTTP composition will own
that retained credential for exactly the `Session` lifetime; an adapter that
supports `Close() error` is closed before the runtime lease is released.

## Cancellation, exact replay, and fencing

Connect and exchange use a caller context plus a configured maximum operation
timeout. If a dependency ignores cancellation, the caller still returns at its
deadline while the original goroutine retains the only operation/runtime
ownership. No replacement activation or exchange may start.

Once the durable generation fence exists, every bootstrap, activation,
persistence, adapter-construction, cancellation, timeout, unavailable,
conflict, or unvalidated-response outcome requires explicit reconciliation
unless the current in-process session can retry one already-built exchange
under the exact same operation owner. That optional recovery is disabled by
default. When enabled, it canonical-encodes the owned AW-02 batch once and
replays only byte-equivalent protocol content after a sanitized transport
`unavailable` result explicitly marked retryable. It never rebuilds an action,
advances a sequence, changes an acknowledgement, follows `Retry-After`, or
crosses a reconnect/process-restart boundary.

Exact replay uses a bounded attempt count and local exponential full jitter
inside the existing total operation timeout. The session pins the pre-effect
acceptance timestamp for the whole operation, so a retry cannot reinterpret an
already-sent lease-bound frame after its lease clock boundary. The first
validated response is applied to the pre-effect conformance snapshot exactly
once. Unauthorized, conflict, protocol, divergent response, exhausted retry,
timeout, and caller cancellation still fail closed. A dependency that ignores
cancellation keeps the single operation owner until it returns, so no
replacement exchange can overtake the ambiguous call. A restart still returns
`reconciliation_required`; the pending batch is deliberately not persisted.
Unauthorized exchange is classified as fenced (covering stale bearer,
generation advance, or server revocation). A protocol `Revoke` permits only
the exact `Revoked` acknowledgement; the session then becomes fenced.

Local validation failures before a network call do not poison a ready session.
They are rejected without advancing the local AW-02 snapshot or invoking the
exchange port.

## Current use and next slices

This package is a reviewed composition contract and deterministic fake surface;
it is not reachable through `attachedworkerforeground.New`. The feature-disabled
[session-to-daemon adapter](attached-worker-daemon-transport.md) owns the
semantic worker action envelope, durable lease/cancel/terminal transitions,
and active-cancel acknowledgement flow. Exact in-process replay is likewise
feature-disabled. The package now composes initial attach and explicit idle or
active reconnect, and exposes the server-selected terminal
replay/discard/committed intent through a read-only recovery result. It does not
auto-send that terminal. A later AW-03 slice must still own real timer cadence,
sleep/wake/offline observations, cost evidence, and reviewed foreground wiring
before bounded polling can become live. This package never infers remote
authority from scalar watermarks or local process state.
