# Attached worker transport (AW-03)

AW-03 establishes an owner-scoped, authenticated immediate transport for an
attached worker. AW-04a extends that transport with durable, transactionally
fenced attempt frames and heartbeat-driven delivery. AW-03d1 adds the
control-plane half of reconnect for an exact durable head; AW-03d2 adds the
[strict worker-side checkpoint and explicit resume](attached-worker-reconnect-checkpoint.md),
and AW-03d3 reconciles that checkpoint against an active AW-04 attempt without
repeating semantic effects. Long polling, explicit cloud wake-up, and the
worker daemon remain later work. Issue #128 starts the feature-disabled local
cadence/composition slice; it does not enable the product foreground.

AW-04c adds owner-scoped durable drain closure. It closes admission before a
Drain is delivered, serializes the control frame behind any unacknowledged
platform attempt frame, and accepts Drained only after the exact durable
attempt head is absent or retired and the protocol attempt is idle.

The feature-disabled [worker-side connection session](attached-worker-connection-session.md)
owns the corresponding bootstrap-to-immediate-exchange composition, local
generation fence, and optional bounded exact replay of one already-built
authenticated batch. It remains unreachable from the product foreground entry
point until a later cadence/composition slice.

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
atomically changes desired state first. Observed state changes to draining only
when the durable protocol Drain transition can be placed after every earlier
platform frame in the same canonical sequence.

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

## Authoritative reconnect

A reconnect challenge is issued only when the current owner-scoped connection
is online or draining, its authentication has not expired, and its canonical
protocol state is exactly `Ready` or `Draining` (not fully `Drained`). An idle
snapshot must have no non-retired durable attempt; an active snapshot must
match the exact owner-scoped AW-04 attempt head including binding, lease/fence,
capability, sequences, cancellation, and terminal commitment. The
challenge durably pins the exact previous connection ID, revision, capability
digest, and canonical protocol snapshot. Those pins are server-only authority:
they are persisted with the challenge but omitted from the public challenge
JSON.

Activation consumes that single-use challenge in the same transaction that
rechecks every pin against the current worker and connection heads. It accepts
the signed reconnect claim through the AW-02 reducer, advances the connection
generation, rotates the connection ID, bearer digest, and channel binding,
retires the old presence, rebinds any matching active attempt to the new
connection generation without changing its semantic sequences or effects, and
creates a presence-free `attaching` head. The
worker is observed offline until a fresh signed Manifest authorizes the new
head and restores online or draining state from the canonical snapshot.

Pending LeaseOffer, LeaseAccepted, Cancel, and TerminalAck messages retain
their semantic attempt identity and fingerprint. If their old connection
envelope was lost, polling rebuilds only a fresh envelope under the replacement
connection and atomically advances the canonical protocol snapshot. It does
not rerun lease allocation, claim, provider/materialization, cancellation,
terminal commit, or cleanup.

An exact retry after a lost activation response returns the already-committed
connection. A request that changes any predecessor pin or new credential is a
consumed/conflicting replay and cannot mutate state. Reconnect from an expired,
revoked, legacy snapshot-less, or otherwise divergent head fails closed before
challenge creation or activation. The full active-state decision and crash
matrix is maintained in
[the reconnect checkpoint contract](attached-worker-reconnect-checkpoint.md).

## Heartbeat-driven delivery

The immediate exchange remains outbound-only from the worker. With the AW-04a
attempt broker enabled, one Heartbeat may report one active attempt only while
`available=false`. After atomically checkpointing that Heartbeat, the service
reauthorizes the current bearer, worker generations, connection and presence,
then point-loads at most one already-durable platform frame:

- Drain has strict priority over a later attempt frame once earlier platform
  frames are acknowledged. Its semantic revision is stable; reconnect may
  replace only the connection envelope and exact replay returns the same frame.
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
Drained is a worker-to-platform control exchange, not a presence observation.
It is committed atomically with the canonical snapshot and an owner-scoped
audit record. A fully Drained snapshot cannot reconnect; the next attach is
fresh.

## Local wake and request ceiling (#128)

The feature-disabled transport poller makes one immediate cycle on a fresh
instance. After each successful cycle, it waits the shared 15-minute minimum
before considering another exchange. A local wake is a buffered, coalesced
hint that can shorten only the *remainder* of a longer configured interval;
it cannot interrupt the minimum wait, an offline retry backoff, or an in-flight
exchange. A wake left
by a stopped poller is discarded on restart. A repeated `Run` on the same
poller retains its last successful exchange time and waits out any remaining
minimum gap. A new process cannot enforce that gap until composition loads an
authoritative durable checkpoint; the advertised ceiling below therefore
applies only to uninterrupted runs or same-process restarts. This hint is not
a platform message, lease, cancellation, or reconnect authority.

`PollCountUpperBound` estimates the no-wake timer schedule. With local wake,
`PollCountUpperBoundWithWake` uses the minimum interval even if the operator
configured a longer one: one idle worker can make at most 96 scheduled
Heartbeat exchanges per 24 hours on that scoped schedule, excluding the
attach-time exchange, exact in-process retries, active-attempt controls, and
other semantic effects. The bound is a scheduled Heartbeat/write ceiling, not
a total HTTP-request ceiling or a measured YDB RU, bytes-egressed, or
RUB charge. Those values require real cloud-dev evidence before enabling the
foreground path. The daemon's independent idle loop must not be wired on top
of this poller as a second network cadence.

The feature-disabled `CadencedSource` seam calls one `Poller.Step` from each
serial daemon `Source.Next`. Daemon idle backoff may ask again locally, but
Step blocks before the next outbound exchange; while an accepted invocation
is running, no separate poller goroutine continues to send Heartbeats. This
is not yet a production foreground wiring or a cross-process restart proof.

`ReconnectIdleCadence` is a narrower, still feature-disabled restart seam. It
rejects invalid local adapter/poller configuration before network work, then
uses `Connector.Reconnect` to reconcile the local checkpoint against the exact
durable server head and accept a fresh Manifest. Only a conformance
`ConnectionReady` and idle attempt with no terminal replay intent can own a
`CadencedSource`; draining, active, fenced, or unknown recovery stops before
another Heartbeat or process effect.
The first idle Step is conservatively delayed by the configured interval after
that Manifest, including when a local wake is queued. This local cooldown does
not bound repeated reconnect/Manifest traffic across process crashes or supply
an authoritative durable timestamp. Foreground enabling and total request,
YDB RU, egress, and charge ceilings still require separate restart/cost proof.
If the injected clock fails after Manifest or an accepted cycle, the source
stops at `reconciliation_required`; it does not classify that post-effect
failure as retryable local configuration or send a catch-up Heartbeat.

The poller retries a failed cycle with short jitter only when its direct,
trusted error explicitly proves `NoExchangeAttempted`. A merely retryable
HTTP timeout/status is ambiguous after a request was sent, and a joined
reconciliation error must stop the cycle rather than create a new Heartbeat.
After an ambiguous cycle, the same poller instance is latched in
`reconciliation_required`; later `Run` or `Step` calls do not send another
request. Reconstructing a poller in a new process still requires an
authoritative durable checkpoint before a fresh cycle may be attempted.
The future daemon composition must provide this pre-effect proof and an
authoritative restart checkpoint before offline recovery and a cross-process
cost ceiling can be claimed.

The control plane already treats an exact same-sequence request as idempotent
and returns the corresponding durable response. The worker session may pair
that behavior with an explicitly configured, in-process retry: it retains one
canonical batch, retries only sanitized retryable `unavailable` outcomes with
bounded local full jitter, and accepts the first response against the original
pre-effect snapshot and acceptance timestamp. It never creates a new
Heartbeat, LeaseClaim, CancelAck, Terminal, sequence, acknowledgement, or
evidence payload for a retry. Restart,
reconnect, exhaustion, cancellation, unauthorized, conflict, protocol drift,
and divergent replay remain fail-closed reconciliation/fencing boundaries.

## Fail-closed boundaries

- Reconnect accepts only idle or exactly matching active server heads. Any
  unsupported, stale, expired, or divergent head fails before challenge or
  activation. Terminal replay intent is exposed read-only; reconnect does not
  auto-send a terminal or perform a process effect.
- A fully `Drained` worker also remains a fresh-attach boundary. The domain
  projection intentionally groups `Draining` and `Drained` for UX, but the
  reconnect reducer retains only the former as a resumable transport state.
- Without the AW-04a broker, ordinary exchange remains Heartbeat-only. With the
  broker, LeaseClaim, Progress, CancelAck and Terminal are accepted only through
  its single YDB transaction; no handler-local conformance state is authority.
- Platform attempt responses are loaded from the durable message ledger, never
  synthesized by the HTTP adapter. The adapter rejects a response whose scope,
  generations, binding, kind, or canonical payload disagrees with the durable
  attempt head.
- Drain never retires or fabricates an active attempt. The already-claimed
  attempt may complete or cancel while admission remains closed; ambiguous
  termination remains fenced_unknown and blocks Drained.
- Worker exact in-process replay is disabled by default. Explicit reconnect
  is available only through the reviewed checkpoint API; neither path starts a
  polling loop, daemon, process, OCI boundary, provider call, or production route. Pending retry content is process-ephemeral and cleared on
  operation completion where Go permits best-effort byte erasure.
- Each accepted Heartbeat advances the durable worker envelope sequence and
  therefore costs one bounded store write. AW-03 enforces one shared minimum
  checkpoint and polling/heartbeat interval of 15 minutes (the
  <=96 writes/worker/day target); AW-03 does not claim write coalescing.
  Expiry is generation/revision fenced and atomically records the worker
  offline transition and a content-free audit.

The connection secret proves possession but does not replace owner scoping,
identity signatures, monotonic generations, revision CAS, or deny-first
revocation.
