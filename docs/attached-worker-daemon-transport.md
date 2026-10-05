# Attached-worker session-to-daemon adapter

Issue #110 defines the feature-disabled AW-03b bridge between one authenticated
worker connection session and the existing daemon `Source` / `ResultSink`
ports. The package is a fake-backed composition contract. No shipped command
constructs it, starts a daemon, or executes a harness.

## Authority chain

The adapter preserves this one-way authority chain:

1. `attachedworkersession.Session` owns the authenticated connection scope,
   connection sequence and acknowledgement watermarks, and AW-02 conformance
   machine.
2. A platform `LeaseOfferV1` is discovery metadata only. It contains immutable
   attempt binding and digests, but no executable process specification.
3. The adapter sends an exact `LeaseClaimV1` through the session. Only a
   validated, same-binding `LeaseAcceptedV1` permits materialization.
4. The injected materializer receives the exact tenant, owner, worker,
   enrollment generation, connection generation and ID, attempt binding, and
   attempt sequence. It may return bounded stdin, canonical read roots beneath
   the configured materialization root, and an existing invocation-scoped
   credential request.
5. Executable path and digest, argv, and environment come exclusively from the
   locally installed capability-pinned profile. Remote frames and materialized
   content cannot replace them.
6. The resulting deep-owned `attachedworkerdaemon.Invocation` is validated
   before it crosses the `Source` boundary.

The local materialization root and every returned read root must already be a
canonical, existing directory. Symlinked roots, duplicate roots, root escape,
oversized paths, and oversized lists fail closed. The attempt lease must be
live and cannot outlast the authenticated session. Profile environment names
cannot replace `HOME`, `PATH`, XDG roots, locale controls, provider API keys,
access tokens, or secret variables.

## Terminal result boundary

`ResultSink.Complete` maps daemon output to one AW-02 terminal message. Success
requires a zero exit code, no cancellation/deadline/failure code, confirmed
descendant reaping, released isolation boundary, and successful cleanup. A
server cancellation can produce `cancelled`; all other incomplete outcomes
produce `failed`.

Terminal evidence is a SHA-256 digest over versioned typed metadata:

- exact scope, run, attempt, lease, and lease generation;
- immutable context, capability, and policy digests;
- terminal status/result and bounded process counters/flags;
- sanitized failure and isolation-profile codes;
- credential mutation generation and whether the runner failed;
- at most 32 each of nonzero, canonically ordered committed artifact and event
  SHA-256 digests.

Stdout and stderr content, prompts, provider bodies, credentials, local paths,
and raw errors are never placed in frames or evidence. Stdout content is also
excluded from completion idempotency; its bounded byte count remains part of
the typed result. A validated exact `TerminalAckV1` commits completion. The
in-memory attempt slot is retired only after a later worker envelope
acknowledges that platform frame, preserving reconnect authority and connection
replay fingerprints. An absent, ambiguous, divergent, or cross-session
acknowledgement returns reconciliation-required and does not invent success.

## Active cancellation and drain

Cancellation received instead of claim acceptance is acknowledged and
reported terminally without invoking the materializer or daemon. Caller
cancellation during materialization is preserved as a causal error while the
adapter uses a separately bounded cancellation-independent context to attempt
terminal reporting.

Materialization has its own configured timeout. If a materializer violates
cancellation, `Next` still returns at the bound, but the original goroutine
retains adapter operation ownership until the dependency actually returns. No
second claim or materialization can start, partial returned stdin is cleared,
and the accepted attempt remains reconciliation-required rather than being
reported as a fabricated terminal failure.

The active-attempt watcher is used only by the explicit #165 synthetic-denied
activation; the default command remains disabled. It closes the in-process
control gap without reusing `Source.Next`. While the exact invocation remains active it
sends session-owned unavailable/one-active presence heartbeats. An exact
same-binding `CancelV1` is accepted only under the current connection and lease
authority. The watcher then calls the daemon's full-identity `CancelActive`
boundary and emits `CancelAckV1`; the daemon applies the local cancellation
function at most once. Since that exact local effect cancels the invocation
context shared with the watcher, the accepted acknowledgement uses a separate
bounded reporting context. A lost or divergent acknowledgement retains the
local effect as reconciliation-required and never repeats it.

Local cancellation application has its own bounded timeout and ignores caller
cancellation once an exact remote cancel has been accepted. If an injected
canceller violates that bound, the watcher returns reconciliation-required but
retains adapter operation ownership until the dependency actually returns; no
terminal completion, new watcher, acknowledgement, or replacement attempt can
overtake the unresolved local effect.

The adapter operation gate serializes each watcher exchange with terminal
completion. If completion retires the adapter attempt first, a late watcher
cannot reach a replacement invocation. If the runner has already returned and
its cancel function is no longer installed, the cancel remains unknown rather
than being acknowledged as applied. Daemon shutdown records cancel-on-install,
so a shutdown crossing the narrow begin-attempt/install boundary still cancels
the exact process once the function becomes available.

The same active-control exchange now handles an authoritative remote `DrainV1`
without treating it as cancellation. `Daemon.RequestDrain` closes admission
immediately and wakes an idle poll, but does not wait for or cancel the active
invocation. The watcher continues unavailable/one-active heartbeats while that
invocation runs, so an exact later `CancelV1` remains deliverable even though
the connection is draining. The adapter retains the exact drain revision,
commits the terminal transition, sends an unavailable/zero-active heartbeat to
acknowledge `TerminalAckV1` and retire that committed attempt, and only then
emits `DrainedV1`. A divergent or ambiguous transition stops the composed
runtime as reconciliation-required; it never acknowledges a drained worker
while execution is still active or unknown.

## Feature-disabled foreground composition

Issue #130 adds `ForegroundRuntime`, a single-use, library-only composition of
the recovered cadence, adapter, daemon, runner, active-control watcher, result
sink, and bounded session cleanup. AW-05f adds a library-only initial connection
composition, `ConnectPinnedForegroundRuntime`: it builds the session connector
with one lease-held preflight, matches the local harness path/digest/argv and
capability digest, checks every static profile environment name against the
stack's exact allowlist, verifies the #132 pinned stack, and reconciles its
installation-owned OCI residue before connection generation or control-plane
network I/O.
The accepted Manifest enters the same daemon through one cooldown cadence;
there is no extra eager heartbeat. A post-connect construction error, or a
caller cancellation before `Run`, closes the acquired session under an
independent bound. Zero-argument harnesses remain valid. This constructor is
not called by `attachedworkerforeground.New` or `attached-worker run`.

The default-off `ReconnectPinnedForegroundRuntime` uses that same lease-held
profile/capability/stack preflight on reconnect. The Connector must reconcile
the exact local checkpoint with the durable server head before the idle cadence
is created. Claimed, cancellation-pending, draining, terminal, or ambiguous
recovery remains fenced; it is never converted into a fresh poll or process
launch. A reconnect that returns an acquired session with an error closes that
session under an independent bound. The shipped command does not call this
constructor either.

The runtime owns one reconciled session until the daemon stops. Remote cancel
is applied to the exact active identity; remote active drain closes admission
and waits for terminal commit; idle remote drain becomes a graceful daemon
stop. Watcher ambiguity fails closed by cancelling the exact active invocation,
reporting its terminal outcome, and returning reconciliation-required. Session
close always uses an independent bounded cleanup context, including after
caller cancellation.

The composed runtime now publishes content-free daemon state changes through
the authenticated session's existing kernel-backed local lease. A coalescing
update channel never blocks attempt, cancellation, or terminal ownership; the
single observer reads the current daemon status, persists monotonic
accepted/completed/failed counters and local observation time, and retires the
observation before closing the session on normal exit. An observation write
failure cancels the runtime and returns reconciliation-required. A crash may
leave historical `observed_local` evidence, which status/doctor do **not**
upgrade to a live process, server connection, or accepted attempt. No shipped
command constructs this runtime; `attached-worker run` remains default-off.

Here `completed` means a successful process outcome with a confirmed terminal
acknowledgement, while `failed` counts confirmed non-success terminals. A
finished runner whose terminal acknowledgement is ambiguous advances neither
counter and retains a content-free `terminal_unconfirmed` failure code for
reconciliation. The daemon's in-memory `Completed` additionally counts ended
runner attempts; it is not projected as successful completion.

## Current boundaries and follow-up

The AW-05f synthetic path now includes `BoundMaterializer`. Its injected
`SealedInputSource` must authenticate and authorize the exact accepted
tenant/owner/worker, connection generation/ID, and attempt before returning
the immutable `WorkerJob`, input manifest, and blob bytes. The materializer
independently recomputes the server's context digest, matches the selected
capability and policy digests, verifies every blob's size/SHA-256, applies the
admitted input/context/artifact limits, and emits one bounded, deterministic
JSON stdin envelope. It creates no host read root or credential handle and
cannot select executable, argv, or environment. The source transfers ownership
of returned byte buffers so they can be cleared after encoding. A rejected or
expired attempt fails without a source call.

This is a credential-free synthetic boundary, not a production input API or a
Codex prompt format. Context windows, workspace/skill bundles, invocation
credentials, and large-file staging remain fail-closed and require separate
reviewed composition. No shipped command constructs this materializer or starts
the daemon.

The server now has a read-only sealed-input authorization gate for that later
composition. It derives the presented secret digest from the connection bearer
and checks the current worker, connection, and `claimed` attempt in one YDB
transaction at server time, including exact generation, lease/fence/expiry,
three input digests, and optional attempt revision. A content fetch must call
this gate before reading blobs and repeat it with the returned revision after
the read; any changed head discards all fetched bytes. This gate returns no
job, manifest, or blob bytes. The optional `attachedworkersealedinput` route
uses it before tenant-scoped immutable blob reads and again with the observed
attempt revision after those reads. Its HTTPS-only local client source refuses
redirects and caps request, response, and content sizes. The route is
injectable into `controlapi.Options`, but is **not** wired into the shipped
control API; the feature remains default-off.

The default-off `attachedworkersealedinput.ConnectSyntheticPinnedRuntime`
constructor now composes that bounded source with the pinned foreground
runtime and a credential lifecycle that denies every provider operation.
`SessionSourceFactory` receives the exact bearer only from the authenticated
session's `ExchangeFactory.Open`; it cannot fetch sealed input before that
connection is accepted, and its exchange close clears the retained bearer.
`SyntheticRuntime` also owns explicit pre-run and active-run cleanup, including
an interrupted run and failed post-connect construction. This is a library
entrypoint only: no shipped command invokes it, and the control API still does
not mount the optional sealed-input route. The matching default-off
`ReconnectSyntheticPinnedRuntime` goes through the same sealed-input source,
pin verification, and credential-denying stack, but requires the connector's
durable idle reconciliation before it can poll. Non-idle or ambiguous recovery
remains fenced; neither constructor is a production activation command.

The joined local tests cover both an authorization denial before any container
command and one accepted credential-free synthetic attempt through the exported
composition, pinned OCI test double, terminal acknowledgement, local drain,
durable checkpoint, temporary-material removal, and resource release. The
successful path first restarts from a durable idle checkpoint, then restarts
from the exact committed terminal head and runs an idle poll without replaying
the process. A separate joined table generates real durable claimed, draining,
and unknown-claim-response heads through authenticated protocol actions, then proves
that pinned reconnect never polls, materializes input, or starts a process
from those non-idle heads. The unknown response does not assert that the server
accepted the claim. A stale server head advances the attempted local generation
but retires its reusable checkpoint, keeping that generation fenced. The
component tests also cover neighboring offered/cancel/pending variants, exact
cancellation and drain ordering, and cleanup failure. The OCI test double is not a real
container engine or provider turn.

The adapter and feature-disabled runtime are under
`internal/attachedworkerdaemontransport`. The session exposes a semantic
`ExchangeAction` method so callers cannot forge raw frame scope, generations,
connection sequence, or acknowledgement values. Both contracts remain
unreachable from `attachedworkerforeground.New`.

A later slice must complete product activation and local control, packaging,
and production sleep/wake and offline evidence. The two-owner
security and recovery proof in #79 remains a release gate.
