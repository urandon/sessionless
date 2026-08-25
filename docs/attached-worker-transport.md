# Attached worker transport (AW-03)

AW-03 establishes an owner-scoped, authenticated immediate transport for an
attached worker. It is deliberately a presence-only slice. Attempt dispatch,
long polling, cloud wake-up, and platform-to-worker frames remain AW-04 work.

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
worker/head to online or draining, and starts the bounded presence lease.

Immutable capability content is keyed by its canonical protocol digest and
does not contain connection generation, signature, or observation time. Those
connection-bound proof fields live on the current connection head, so a later
connection may reuse identical content without a digest-key conflict.

## Fail-closed boundaries

- Reconnect challenge issuance is disabled until AW-04 provides a durable,
  authoritative attempt snapshot for reconciliation. No unusable reconnect
  challenge is created.
- Ordinary AW-03 exchange accepts Heartbeat frames only. Work, cancel,
  progress, terminal, drain, revoke, and other control frames are rejected
  before any sequence checkpoint mutation.
- A non-nil broker is rejected by service construction. Successful immediate
  exchanges return no frame (HTTP 204). AW-04 must add atomic conformance and
  outbound watermark persistence before enabling platform responses.
- Each accepted Heartbeat advances the durable worker envelope sequence and
  therefore costs one bounded store write. AW-03 enforces one shared minimum
  checkpoint and polling/heartbeat interval of 15 minutes (the
  <=96 writes/worker/day target); AW-03 does not claim write coalescing.
  Expiry is generation/revision fenced and atomically records the worker
  offline transition and a content-free audit.

The connection secret proves possession but does not replace owner scoping,
identity signatures, monotonic generations, revision CAS, or deny-first
revocation.
