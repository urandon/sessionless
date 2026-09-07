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

## Cancellation and remaining gate

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

Active cancellation remains deliberately unsupported. The current daemon does
not call `Source.Next` while its runner is active, so this adapter cannot poll a
new `CancelV1` and reach the exact active process cancellation authority.
Production foreground composition remains blocked until a reviewed attempt
control watcher provides that behavior, bounded `CancelAckV1` evidence, and
restart/reconnect reconciliation.

## Current boundaries and follow-up

The adapter is under `internal/attachedworkerdaemontransport`. The session
exposes a semantic `ExchangeAction` method so callers cannot forge raw frame
scope, generations, connection sequence, or acknowledgement values. Both
contracts remain unreachable from `attachedworkerforeground.New`.

A later slice must provide the concrete authenticated context/artifact
materializer, durable AW-04 attempt ownership and terminal reconciliation, the
active-cancel watcher, and reviewed foreground wiring. The two-owner security
and recovery proof in #79 remains a release gate.
