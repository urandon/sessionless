# Attached-worker foreground preflight

Issue [#108](https://gitcode.com/urandon/sessionless/issues/108) established
the default-off foreground ownership shell for AW-05. Issue #165 adds an
explicit, private `synthetic-denied` activation profile through the shipped
`run`/`serve` commands. Without that profile, the original disabled behavior
remains. Neither path enables real provider credentials or a provider turn.

## State machine

The bounded local sequence is:

```text
preflight -> runtime-owned -> disabled/unavailable -> draining -> stopped
```

`preflight` first acquires the kernel-backed `runtime.lock`. Only that owner may
then load the secret-free snapshot, validate the active manifest, and read the
separate secret record. A second process fails with `state_busy`; wall-clock
age is never used to steal a lock.

The secret read proves that manifest, owner, worker, generation, identity key,
and connection material are internally consistent. The foreground immediately
zeroes its temporary byte slices. It does not send or format them and cannot
turn them into provider credentials or a live connection.

The disabled activation records monotonic, content-free local observations
with `last_failure_code=feature_disabled`. It carries forward only already
validated counters and never attempts to infer server state. A started shell
retains `runtime.lock` until its explicit, idempotent drain/shutdown lifecycle
persists `draining`, persists `stopped`, durably retires the final observation,
and releases the lock. Without an activation profile, the command entrypoint
performs that sequence immediately.

Shutdown runs exactly once under a separate bounded cleanup context. A caller
may bound how long it waits, but cancellation cannot abort that cleanup; a
later call observes the same terminal result. Concurrent or repeated drain and
shutdown requests use context-aware transition serialization and cannot
duplicate observation retirement or lease release. Starting shutdown cancels
an in-flight drain request before the shutdown bound begins controlling the
remaining transition work. Any cleanup ambiguity overrides the disabled result
and fails closed.
Retirement is revision-guarded, so cleanup cannot remove a different
observation.

If an underlying filesystem operation does not observe cancellation before
the cleanup deadline, bounded callers still return on their own deadline. The
single ownership goroutine remains as a late finalizer: after the operation
unblocks it makes one fresh bounded reconciliation attempt and closes the
runtime lease exactly once. Until that happens the result reports ownership as
`unknown`, rather than falsely claiming release.

## Operator behavior

The foreground entrypoint is explicit:

```text
attached-worker run --state-dir /absolute/symlink-free/path
```

Without an activation profile, the command exits non-zero with one bounded
JSON object whose code is `feature_disabled` after successful preflight and
cleanup. It reports
only constant local facts such as `network_action=not_attempted`,
`process_action=not_attempted`, `credential_action=not_attempted`, and
`server_connection_state=unknown`. It never emits the state path, endpoint,
identity material, connection secret, command line, environment, stdout, or
stderr.

The opt-in profile and service packaging are specified in
[attached-worker packaging](attached-worker-packaging.md). An activated owner
uses the existing authenticated session, daemon, and pinned OCI stack; its
local control remains content-free. A remote Cancel for the exact active
attempt cancels only that workload and sends a bounded `CancelAck` even though
the local invocation context has been cancelled. Lost or divergent ack or
terminal evidence remains reconciliation-required, never a replay grant.

Malformed, partial, logged-out, busy, unsupported, or ambiguous state retains
the stable result code owned by `attachedworkerlocal`. A local observation is
not daemon health, attach acceptance, an AW-04 attempt, or terminal authority.

## Composition boundary

`internal/attachedworkerforeground.ActivationPorts` names the exact existing
bootstrap, source, result-sink, invocation-runner, and daemon authority seams.
The historical #108 constructor remains disabled without an operator profile.
The #165 profile instead connects the shipped command to the accepted
generation-fenced session and pinned stack for synthetic, credential-denied
attempts. It neither selects a provider from ambient state nor changes the
separate #79 two-owner security/recovery release gate. Provider-specific
activation, automatic start/update, and cloud deployment are not implied.
