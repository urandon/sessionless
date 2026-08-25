# Attached worker transport (AW-03)

AW-03 establishes an owner-scoped, authenticated immediate transport for an
attached worker. AW-04a extends that transport with durable, transactionally
fenced attempt frames and heartbeat-driven delivery. Long polling, explicit
cloud wake-up, reconnect reconciliation, and the worker daemon remain later
work.

## Authority and secrets

`tenant_id` and `owner_user_id` on bootstrap routes and inside a connection
bearer are locator hints, not authority. Every lookup is scoped by both values,
then authorized against the stored worker identity and current generations.
The challenge-request signature also binds the exact deployment audience, so
a captured bootstrap request cannot be replayed into another environment.
Missing, cross-owner, stale, expired, consumed, revoked, and invalid-proof
cases collapse to the same public unauthorized result.

The worker generates a 32-byte connection secret. Activation sends and signs
only its SHA-256 digest. The raw secret appears only in the redacted
Authorization bearer used for later exchanges; it is never persisted, logged,
or included in bootstrap request JSON. Challenge and activation expiry decisions
use the store transaction timestamp, not a service-process clock.

## Two-phase initial attach

The handshake follows the AW-02 conformance order and independent directional
sequences:

1. worker `Hello` (`w-...01`, worker sequence 1, ack 0)
2. platform `Challenge` (`p-...01`, platform sequence 1, ack 1)
3. worker signed `Attach` (`w-...02`, worker sequence 2, ack 1)
4. platform `AttachAccepted` (`p-...02`, platform sequence 2, ack 2)
5. worker signed `Manifest` (`w-...03`, worker sequence 3, ack 2)

Activation atomically consumes the challenge, advances the worker connection
generation, and creates an `attaching` connection head. That head has no
presence lease and cannot authorize ordinary exchange. The first authenticated
exchange must contain exactly the Manifest. Its transaction verifies the
bearer/current fences, inserts or reuses immutable capability content, records
the connection-specific signature observation on the current head, changes the
worker/head to online, and starts the bounded presence lease. A requested drain
does not become observed draining until a durable protocol Drain transition is
implemented.

Immutable capability content is keyed by its canonical protocol digest and
does not contain connection generation, signature, or observation time. Those
connection-bound proof fields live on the current connection head, so a later
connection may reuse identical content without a digest-key conflict.

The connection head also owns the canonical, bounded AW-02 machine snapshot.
Activation creates the attached snapshot; Manifest, Heartbeat, and every AW-04
attempt frame atomically replace it. Scalar sequence and acknowledgement
columns are checked projections only. Missing, noncanonical, authority-mismatched,
or divergent same-sequence snapshots fail closed; old online rows without a
snapshot must attach again and are never reconstructed from watermarks.

## Heartbeat-driven delivery

The immediate exchange remains outbound-only from the worker. With the AW-04a
attempt broker enabled, one Heartbeat may report one active attempt only while
`available=false`. After atomically checkpointing that Heartbeat, the service
reauthorizes the current bearer, worker generations, connection and presence,
then point-loads at most one already-durable platform frame:

- LeaseOffer is discovery only; the worker cannot execute until its LeaseClaim
  is atomically committed and the matching LeaseAccepted is returned.
- Cancel carries the durable cancellation revision. A missing acknowledgement
  reaches an explicit `fenced_unknown` state; the platform never fabricates
  process termination.
- TerminalAck is returned only after canonical run, attempt, reservation,
  artifact/event materialization and protocol state commit under the current
  lease fence.

An empty exchange remains HTTP 204. A pending durable platform frame is a
strict, bounded HTTP 200 AW-02 batch. A worker acknowledges it in a later signed
frame or Heartbeat; acknowledged platform frames are no longer returned.

## Fail-closed boundaries

- Reconnect challenge issuance remains disabled until AW-04b composes the
  durable snapshot with reconnect challenge consumption and replay decisions.
  No unusable reconnect challenge is created.
- Without the AW-04a broker, ordinary exchange remains Heartbeat-only. With the
  broker, LeaseClaim, Progress, CancelAck and Terminal are accepted only through
  its single YDB transaction; no handler-local conformance state is authority.
- Platform attempt responses are loaded from the durable message ledger, never
  synthesized by the HTTP adapter. The adapter rejects a response whose scope,
  generations, binding, kind, or canonical payload disagrees with the durable
  attempt head.
- Each accepted Heartbeat advances the durable worker envelope sequence and
  therefore costs one bounded store write. AW-03 enforces one shared minimum
  checkpoint and polling/heartbeat interval of 15 minutes (the
  <=96 writes/worker/day target); AW-03 does not claim write coalescing.
  Expiry is generation/revision fenced and atomically records the worker
  offline transition and a content-free audit.

The connection secret proves possession but does not replace owner scoping,
identity signatures, monotonic generations, revision CAS, or deny-first
revocation.
