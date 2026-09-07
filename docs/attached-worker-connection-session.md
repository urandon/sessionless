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

The existing local files durably retain only installation identity, the latest
locators and generations, and private/connection-secret material. Challenge
identity, nonces, proof, selected server offer, channel-binding bytes,
connection identity, authentication expiry, AW-02 machine state, and response
watermarks remain process-ephemeral in this slice. None is reconstructed or
declared successful after restart. AW-04 durable attempt and reconnect
reconciliation must land before reconnect can be enabled.

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

## Cancellation, ambiguity, and fencing

Connect and exchange use a caller context plus a configured maximum operation
timeout. If a dependency ignores cancellation, the caller still returns at its
deadline while the original goroutine retains the only operation/runtime
ownership. No replacement activation or exchange may start.

Once the durable generation fence exists, every bootstrap, activation,
persistence, adapter-construction, cancellation, timeout, unavailable,
conflict, or unvalidated-response outcome requires explicit reconciliation.
The session never retries an effect whose server outcome may be unknown.
Unauthorized exchange is classified as fenced (covering stale bearer,
generation advance, or server revocation). A protocol `Revoke` permits only
the exact `Revoked` acknowledgement; the session then becomes fenced.

Local validation failures before a network call do not poison a ready session.
They are rejected without advancing the local AW-02 snapshot or invoking the
exchange port.

## Current use and next slices

This package is a reviewed composition contract and deterministic fake surface;
it is not reachable through `attachedworkerforeground.New`. The feature-disabled
[session-to-daemon adapter](attached-worker-daemon-transport.md) now owns the
semantic worker action envelope plus fake-backed `Source`/`ResultSink` flow.
AW-04 fenced attempt composition must still connect durable
lease/cancel/terminal transitions, an active-cancel watcher must reach the
exact running attempt, and concrete authenticated materialization must be
reviewed before foreground composition can inject the live ports and enable
bounded long polling.
