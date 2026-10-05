# Attached-worker service packaging and local operation

Issue #137 is the default-off packaging and operator-lifecycle child of #77.
This page describes the default-off staging and native user-service boundary.
Registration is not a start, and an active OS service is not proof of a live
local owner or server connection. Provider turns and cloud writes remain
disabled.

## One local owner

`attached-worker serve` with explicit state root, manifest revision, binary
path, and binary digest validates the
enrolled local installation, takes its kernel-backed `runtime.lock`, writes
content-free local observation, and holds that one lease until an operator
stop or process signal. Without an explicit activation profile, the foreground
owner remains `feature_disabled`: it starts no poll, container, harness,
credential use, or provider call. On signal it closes admission through drain, performs bounded
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

## Explicit synthetic activation (#165, in progress)

The optional `--activation-profile` is a private, owner-owned `0600` JSON file
in a canonical `0700` directory. It pins the tenant, owner, worker,
enrollment, initial manifest revision, control-plane HTTPS origin, capability,
executable digest, `installation_sha256` of the manifest's OCI and harness
configuration, local materialization/scratch bounds, and an optional
private TLS root bundle. Only `mode=synthetic-denied` is accepted: the existing
session/daemon/transport stack may connect and run a synthetic attempt, but
provider credentials and provider calls remain denied. The long-lived command
clears its inherited environment. A successful reconnect may advance only
the paired manifest revision and connection generation; any other local
manifest change needs a newly reviewed profile.

Launchd/systemd/rootless-container units staged with this opt-in carry both the profile path and
`--activation-sha256`. The digest covers the exact profile and trust-bundle
bytes, and a same-path edit causes a failed start or native readback until a
new reviewed package plan is staged. The local control socket is opened only
after the single runtime reaches its running state. `drain` and `stop` still
require the current live manifest revision, not the unit's installation
baseline. A raw operator `run` may omit the digest; a service `serve` may not.

The rootless-container unit remains offline (`--network none`, no Docker
socket) without an activation profile. An explicit synthetic profile renders a
different, digest-pinned unit with bridge networking for the trusted service
process and narrowly named bind mounts for the current user's rootless Docker
socket, pinned OCI CLI/config, profile/trust, and private materialization and
scratch roots. Writable roots must be the dedicated `materialized` and
`scratch` siblings of the state directory, not an arbitrary owner-private
directory. Package planning rejects a foreign OCI host, non-rootless
boundary, nonempty OCI client config, broad/overlapping writable mount root,
or changed profile bytes. This grants the trusted service process the same
user's rootless engine authority; it does **not** grant the isolated harness
container that socket or network. Docker's bridge is not an egress firewall;
the application transport itself pins the HTTPS origin, and the exact service
binary/image must remain trusted. The opt-in Linux rootless service gate proves
one signed synthetic-denied attempt against a test-owned rootless engine;
exact-head CI and independent security review remain merge gates for changes
to this path.

After an activated connection advances the live manifest revision, an older
archived unit with `--expected-revision` for the prior manifest is not a safe
rollback target. The rollback preview fails closed. To return to the disabled
owner, stop the active service, unregister it, review a new `package-plan`
without `--activation-profile` against the current manifest, apply that exact
plan, then register and explicitly start the new disabled unit. This is a new
package update, not replay of an old unit or an automatic rollback.
The terminal `unregister` receipt authorizes only one replacement stage before
the new unit is registered. A second `package-plan` at that point fails
closed: to correct the staged unit, register it while disabled, unregister it
again, and then review and stage the correction. This keeps every service
change linked to exact OS registration evidence.

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
Package mutations and `native-start` also share a private
`.package-operations.lock` in the private staging directory. It stays held
through the OS start/readback handoff, when the child must be free to acquire
`runtime.lock`.

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
rootless service packaging, pass the previous immutable `--container-image` on both
rollback commands as well.

Rootless-container staging additionally requires `--container-image` with an
immutable `@sha256:` execution-base image. The staged Linux user unit wraps
the exact attached-worker binary in a rootless Docker namespace; the binary is
read-only bind-mounted and verifies its own digest before taking the shared
runtime lease. This is not the harness/OCI image in the enrolled manifest.
The unit does not pull an image, enable itself, restart automatically, use
host networking, inherit Docker context or credentials, or call a provider.
An operator must preload the exact image digest, provide a current-user
rootless Docker socket at `/run/user/<uid>/docker.sock`, and make the Docker
client available at `/usr/bin/docker` before native registration.
`native-start` verifies the socket,
rootless security option, empty private client config, and exact preloaded
image before requesting systemd start. OS readback still requires
`live-status` to prove the single local owner.
Because Docker owns the container after its client exits, the unit also tries
an exact-name stop on abnormal client exit. Inspection checks both systemd and
the exact rootless container: an inactive unit with a remaining container is
`reconciliation_required`, blocking unregister or update. If the rootless
engine is unavailable, inspection cannot assert absence and also fails closed;
the operator must restore the engine and stop only that exact container before
retrying the readback. A failed cleanup is never reported as a clean stop.

The launchd plist has `RunAtLoad=false` and `KeepAlive=false`; the systemd user
unit has `Restart=no`. Native units pin the executable digest and manifest
revision on startup. Neither staging nor registration starts or enables the
service. The rootless unit pins an image digest and manifest revision and
remains default-off until the explicit native start.

## Exact native user-service lifecycle

On Darwin, `launchd` uses only the current `gui/<uid>` domain. On Linux,
`systemd-user` and `rootless-container` use only the current UID's user manager
at `/run/user/<uid>`;
there is no `sudo`, system service, `enable`, or automatic restart. The
operator first stages a unit, then reviews a separate `native-plan` for
`register` and applies that exact plan SHA. Registration has monotonic local
receipts and OS readback. `native-inspect` reports `registered`,
`unregistered`, or `reconciliation_required`, plus OS activity; it does not
claim worker health. `native-start` is an explicit, revision-fenced start
request. Use `live-status`/`live-doctor` for current local-owner evidence,
then `drain` and `stop` with the exact manifest revision. Once the OS service
is inactive, review/apply `native-plan --native-action unregister` before a
package update or staged rollback. Re-register the new staged revision with a
new reviewed plan; there is no implicit in-place reload.

```text
.build/bin/attached-worker native-plan --state-dir <absolute-state-root> \
  --package-mode <launchd|systemd-user|rootless-container> --install-dir <same-private-dir> \
  --binary <same-binary> --binary-sha256 <same-digest> \
  --expected-install-revision <staged-revision> --native-action register

.build/bin/attached-worker native-apply --state-dir <absolute-state-root> \
  --package-mode <same-mode> --install-dir <same-dir> \
  --binary <same-binary> --binary-sha256 <same-digest> \
  --expected-install-revision <same-staged-revision> \
  --native-action register --plan-sha256 <reviewed-native-plan-digest>

.build/bin/attached-worker native-inspect --state-dir <absolute-state-root> \
  --package-mode <same-mode> --install-dir <same-dir> \
  --binary <same-binary> --binary-sha256 <same-digest>

.build/bin/attached-worker native-start --state-dir <absolute-state-root> \
  --package-mode <same-mode> --install-dir <same-dir> \
  --binary <same-binary> --binary-sha256 <same-digest> \
  --expected-install-revision <staged-revision> \
  --expected-registration-revision <registration-revision>
```

For `rootless-container`, include the same `--container-image <exact-digest>`
on every `native-*` command. A different image is a revision conflict, not
an implicit reconfiguration.

For unregister, use the same `native-plan`/`native-apply` pair with
`--native-action unregister` and the currently registered install revision.
An uncertain service-manager result leaves a durable `*.native-pending.json`
marker and blocks staging, rollback, and another registration action.
`native-reconcile` requires that pending plan's exact SHA, inspects the OS
without retrying the mutation, and reports either `completed` or
`not_applied`. Unexpected OS state stays `reconciliation_required` for
manual investigation.

```text
.build/bin/attached-worker native-reconcile --state-dir <absolute-state-root> \
  --package-mode <same-mode> --install-dir <same-dir> \
  --binary <same-binary> --binary-sha256 <same-digest> \
  --plan-sha256 <pending-native-plan-digest>
```

`make attached-worker-native-integration` is opt-in and temporarily registers,
starts, drains, stops, and unregisters a test-owned user service. It requires
a functioning current-user launchd GUI domain or systemd user manager, and
does not use provider credentials, Docker, or cloud resources.

`make attached-worker-rootless-integration` is an opt-in Linux user-service
lifecycle test against an already running, explicitly configured rootless
Docker engine and a preloaded `SESSIONLESS_ROOTLESS_IMAGE`. The pinned Linux
rootless CI gate provisions its own test-owned engine and runs this target;
ordinary tests never start Docker. The exact-binary accepted-attempt proof
waits through the shipped 15-minute post-Manifest heartbeat cooldown; the CI
fixture does not shorten the production cadence. The Mac user-service gate
uses launchd instead. #79 remains the separate two-owner security/recovery
release gate.
