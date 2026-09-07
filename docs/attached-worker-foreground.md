# Attached-worker foreground preflight

Issue [#108](https://gitcode.com/urandon/sessionless/issues/108) adds the
feature-disabled foreground ownership shell for AW-05. It composes only the
durable local-state lifecycle. It does not enable enrollment, transport,
dispatch, provider execution, or the reviewed OCI launcher.

## State machine

The bounded local sequence is:

```text
preflight -> runtime-owned -> disabled/unavailable -> shutdown -> stopped
```

`preflight` first acquires the kernel-backed `runtime.lock`. Only that owner may
then load the secret-free snapshot, validate the active manifest, and read the
separate secret record. A second process fails with `state_busy`; wall-clock
age is never used to steal a lock.

The secret read proves that manifest, owner, worker, generation, identity key,
and connection material are internally consistent. The foreground immediately
zeroes its temporary byte slices. It does not send or format them and cannot
turn them into provider credentials or a live connection.

The disabled activation records one monotonic, content-free local observation
with `daemon_state=stopped` and `last_failure_code=feature_disabled`. It carries
forward only already validated counters, never attempts to infer server state,
then durably retires the observation before releasing `runtime.lock`. Cleanup
uses a separate bounded context so caller cancellation cannot leave a normal
disabled run looking active. Any cleanup ambiguity overrides the disabled
result and fails closed.

## Operator behavior

The foreground entrypoint is explicit:

```text
attached-worker run --state-dir /absolute/symlink-free/path
```

The current command always exits non-zero with one bounded JSON object whose
code is `feature_disabled` after successful preflight and cleanup. It reports
only constant local facts such as `network_action=not_attempted`,
`process_action=not_attempted`, `credential_action=not_attempted`, and
`server_connection_state=unknown`. It never emits the state path, endpoint,
identity material, connection secret, command line, environment, stdout, or
stderr.

Malformed, partial, logged-out, busy, unsupported, or ambiguous state retains
the stable result code owned by `attachedworkerlocal`. A local observation is
not daemon health, attach acceptance, an AW-04 attempt, or terminal authority.

## Deferred live composition

`internal/attachedworkerforeground.Activation` names the later injection seam,
but the product constructor deliberately accepts no activation implementation.
A later reviewed child must first define the connection-session contract:

- how bootstrap issues an exact connection identity and bearer;
- whether and how that material is persisted or renewed;
- generation fencing, rotation, logout, and crash recovery;
- AW-03 source/result-sink construction and AW-04 conformance authority;
- daemon, invocation runner, provider credential, and OCI launcher ownership;
- bounded drain, replay, ambiguous completion, and remote evidence.

Until that contract lands, no repository binary can construct a live
activation. OS-service/container packaging, authenticated local control,
self-update, destructive uninstall, owner-facing UX, and #79 two-owner E2E are
also separate gates.

