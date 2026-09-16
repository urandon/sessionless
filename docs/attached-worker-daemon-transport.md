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

The feature-disabled active-attempt watcher closes the in-process control gap
without reusing `Source.Next`. While the exact invocation remains active it
sends session-owned unavailable/one-active presence heartbeats. An exact
same-binding `CancelV1` is accepted only under the current connection and lease
authority. The watcher then calls the daemon's full-identity `CancelActive`
boundary and emits `CancelAckV1`; the daemon applies the local cancellation
function at most once. A lost or divergent acknowledgement retains the local
effect as reconciliation-required and never repeats it.

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
sink, and bounded session cleanup. It has no product constructor and remains
unreachable from `attachedworkerforeground.New` and the `attached-worker run`
command.

The runtime owns one reconciled session until the daemon stops. Remote cancel
is applied to the exact active identity; remote active drain closes admission
and waits for terminal commit; idle remote drain becomes a graceful daemon
stop. Watcher ambiguity fails closed by cancelling the exact active invocation,
reporting its terminal outcome, and returning reconciliation-required. Session
close always uses an independent bounded cleanup context, including after
caller cancellation.

## Current boundaries and follow-up

The adapter and feature-disabled runtime are under
`internal/attachedworkerdaemontransport`. The session exposes a semantic
`ExchangeAction` method so callers cannot forge raw frame scope, generations,
connection sequence, or acknowledgement values. Both contracts remain
unreachable from `attachedworkerforeground.New`.

A later slice must provide the concrete authenticated context/artifact
materializer, durable restart/reconnect reconciliation for ambiguous active
cancellation, product activation and packaging, and production sleep/wake and
offline evidence. The two-owner security and recovery proof in #79 remains a
release gate.
