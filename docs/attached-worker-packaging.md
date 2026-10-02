# Attached-worker service package staging

Issue #137 is the default-off packaging and operator-lifecycle child of #77.
This page describes the currently implemented **staging** boundary. Staging is
not service registration, an active daemon, an isolated provider runtime, or a
release approval. Provider turns and cloud writes remain disabled.

## One local owner

`attached-worker serve` with explicit state root, manifest revision, binary
path, and binary digest validates the
enrolled local installation, takes its kernel-backed `runtime.lock`, writes
content-free local observation, and holds that one lease until an operator
stop or process signal. The shipped foreground owner remains
`feature_disabled`: it starts no poll, container, harness, credential use, or
provider call. On signal it closes admission through drain, performs bounded
shutdown, retires its observation, and releases the lease. A failed cleanup is
reported as unknown, not stopped.

The local version-1 control endpoint is a Unix socket at
`<state-dir>.control/control.sock`, outside the strict state-file inventory. Its
directory must be canonical, owned by the process UID, and mode `0700`; the
socket must be owned by that UID and mode `0600`. A second `serve` cannot take
the installation runtime lock. A crash-stale socket is removed only after the
new owner has acquired that lock and verified that no listener responds.

The one-request JSON protocol supports `status`, `doctor`, `drain`, and
`shutdown`. `drain` and `shutdown` require the exact current manifest revision.
Lost mutation responses are ambiguous; clients do not retry them. `status`
and `doctor` report local evidence and preserve remote/server state as
`unknown`. They never return prompts, result payloads, credentials, host paths,
raw provider errors, or stderr.

Build only this binary with the repository target `make attached-worker-build`
(`make build` also builds the WebUI and other components). The local commands
are:

```text
.build/bin/attached-worker serve --state-dir <absolute-state-root> \
  --expected-revision <manifest-revision> \
  --binary <absolute-attached-worker-binary> --binary-sha256 <digest>
.build/bin/attached-worker live-status --state-dir <absolute-state-root>
.build/bin/attached-worker live-doctor --state-dir <absolute-state-root>
.build/bin/attached-worker drain --state-dir <absolute-state-root> --expected-revision <manifest-revision>
.build/bin/attached-worker stop --state-dir <absolute-state-root> --expected-revision <manifest-revision>
```

The long-lived binary clears its inherited process environment before it
starts the service. Existing `status` and `doctor` remain historical local
state reads; use the `live-*` commands to ask the current owner. Neither
surface proves a control-plane connection.

## Reviewed package plan and staged receipt

`package-plan` reads the exact local manifest, verifies the operator-specified
attached-worker binary against its SHA-256 digest, validates a private
canonical staging directory, and checks the prior unit plus receipt. It does
not write a file. `package-apply` recomputes the plan while holding the same
runtime lock, requires its exact reviewed `plan_sha256`, and stages the
mode-specific unit with a versioned receipt. The receipt states
`registration=not_attempted` and records the previous unit digest for an
exact rollback audit. Every staged revision retains its unit and receipt in
private content-addressed archives. `package-rollback-plan` previews the
immediately previous exact unit and `package-rollback-apply` stages it as a
new monotonic install revision. A stale/tampered unit, stale install revision,
live owner, unsupported mode, symlinked/unsafe path, or changed binary fails
closed. The focused local gate is `make attached-worker-package-test`.

```text
.build/bin/attached-worker package-plan --state-dir <absolute-state-root> \
  --package-mode <launchd|systemd-user|rootless-container> \
  --install-dir <private-canonical-staging-dir> \
  --binary <absolute-attached-worker-binary> --binary-sha256 <digest> \
  --expected-install-revision <prior-revision>

.build/bin/attached-worker package-apply --state-dir <absolute-state-root> \
  --package-mode <same-mode> --install-dir <same-dir> \
  --binary <same-binary> --binary-sha256 <same-digest> \
  --expected-install-revision <same-prior-revision> \
  --plan-sha256 <reviewed-plan-digest>

.build/bin/attached-worker package-rollback-plan --state-dir <absolute-state-root> \
  --package-mode <same-mode> --install-dir <same-dir> \
  --binary <previous-absolute-binary> --binary-sha256 <previous-digest> \
  --expected-install-revision <current-revision>

.build/bin/attached-worker package-rollback-apply --state-dir <absolute-state-root> \
  --package-mode <same-mode> --install-dir <same-dir> \
  --binary <same-previous-binary> --binary-sha256 <same-previous-digest> \
  --expected-install-revision <same-current-revision> \
  --plan-sha256 <reviewed-rollback-plan-digest>
```

Rollback requires the previous binary/image, same active manifest revision,
and an intact archived receipt. Pre-archive legacy staged units fail closed;
there is no implicit rollback across a manifest revision change. The operation
affects staged files only, not a registered or running OS service. Lost or
partial writes are reported as ambiguous for operator reconciliation. For
rootless intent, pass the previous immutable `--container-image` on both
rollback commands as well.

Rootless-container staging additionally requires `--container-image` with an
immutable `@sha256:` daemon-service image. That JSON artifact is an exact
intent record, **not** a runnable container definition or a claim that a
container image has been published. The harness/OCI image in the enrolled
manifest is a different artifact and is never reused as the daemon image.

The launchd plist has `RunAtLoad=false` and `KeepAlive=false`; the systemd user
unit has `Restart=no`. These avoid turning an ambiguous crash into automatic
re-admission. The staged artifacts are not loaded or enabled by these
commands. Native units pin the executable digest and manifest revision on
startup; the rootless intent pins an image digest and manifest revision, but
has not been validated as a runnable container. Operator service
registration, verified rootless shared-lock/UID semantics, exact platform
integration evidence, and rollback of a registered
service remain required by #137 before #77 may close. #79 remains the
separate two-owner security/recovery release gate.
