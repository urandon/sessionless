# Attached-worker OCI isolation profile

Issue [#106](https://gitcode.com/urandon/sessionless/issues/106) adds the first
production-direction `IsolationLauncher` for the owner-managed worker. It is a
feature-disabled boundary: no product binary constructs it yet.

## Trust and configuration

`internal/attachedworkeroci.NewLauncher` accepts only explicit operator-owned
configuration:

- the canonical absolute Docker CLI file, an empty private CLI configuration
  directory, and an explicit local `unix://` daemon endpoint;
- the exact engine ID and one matching host boundary: `darwin-vm` or
  `linux-rootless`;
- a repository image reference pinned by SHA-256 digest whose inspected image
  has no declared environment or writable volumes;
- a numeric non-root UID/GID and fixed disk, credential-file, memory, PID, and
  stop-time limits;
- a stable installation ID used only in exact ownership labels for startup
  residue reconciliation.

The empty CLI directory and explicit `--host` prevent ambient Docker contexts,
credential helpers, `HOME`, and `DOCKER_HOST` from selecting authority. The
engine ID prevents an unchanged socket path from silently switching to a
different daemon. Linux requires a rootless or user-namespace security option.
On Darwin, the explicitly selected Linux VM (Docker Desktop, Colima, or an
equivalent reviewed local VM) is part of the trusted computing base; its exact
engine/profile/build tuple must pass the real-engine gate. A generic remote
Docker endpoint is not supported.

## Per-attempt boundary

Preparation creates one randomly named container and then inspects what the
engine actually accepted. The accepted state must exact-match:

- `--pull never` and the pinned image digest;
- `--network none`, read-only rootfs, default seccomp, all capabilities
  dropped, `no-new-privileges`, private PID/cgroup namespaces, no privilege,
  restart, healthcheck, devices, ports, or daemon socket;
- a non-root user, memory and PID cgroup limits, a bounded `/dev/shm`, and one
  bounded `nosuid,nodev,noexec` attempt tmpfs;
- only the exact executable and additional read roots as read-only bind mounts;
- the exact inner executable digest/argv attestation, independently of the
  outer `docker container start --attach` client.

The credential lifecycle gets a special exact-file bridge. `auth.json` is
copied into a `0600` staging file inside the already private host attempt root,
that one file is mounted over the read-only credential root, and `RLIMIT_FSIZE`
bounds it. After verified process and container stop, release validates the
file bound and atomically replaces only the materialized `auth.json` before
removing the container. The surrounding credential service still validates
generation-CAS writeback and release; the launcher cannot commit provider
state.

All synchronous Docker output is memory-bounded. A failed or ambiguous create
is cleaned by its unpredictable preselected name; a successfully created
boundary is controlled only by its full container ID. Inspection, TERM, KILL,
writeback, removal, or malformed output ambiguity fails closed. Release is
idempotent.

`Launcher.Reconcile` is the startup-only residue operation. It lists containers
using the exact profile, installation, and engine labels, validates every full
container ID before any mutation, and removes only that set. Canonical attempt
fencing/recovery remains owned by later #77 composition work.

## Verification

Deterministic fake-client tests cover preflight denial, exact create/inspect
state, unsafe path/mount rejection, cleanup after inspection failure,
credential bounds, lifecycle idempotency, and startup reconciliation without
fixed sleeps. The composed supervisor fixtures also cover bounded preparation,
natural outer-client exit while the isolated workload remains alive, and
outer-client start failure; in each case the isolation boundary must be reaped.

The real-engine matrix is opt-in because it mutates an explicitly selected
local Docker Engine. It builds a static Linux probe, creates a temporary empty
digest-pinned fixture image through an ephemeral local registry, runs the
matrix, and removes every exact test-owned image/container/temp root:

```text
ATTACHED_WORKER_OCI_DOCKER_PATH=/canonical/path/to/docker \
ATTACHED_WORKER_OCI_DOCKER_HOST=unix:///explicit/path/to/docker.sock \
ATTACHED_WORKER_OCI_BOUNDARY=darwin-vm \
make attached-worker-oci-integration
```

The matrix proves allowed scratch writes plus denied host read, denied host
write, denied egress, detached-process teardown, hard scratch exhaustion, and
zero boundary residue. Darwin and Linux need separate recorded passes for the
exact release tuple before a binary may enable the profile.

The canonical Linux proof is `make attached-worker-oci-linux-rootless-ci`, run
by `.github/workflows/attached-worker-oci-rootless.yml` on Ubuntu 24.04. It
downloads the Docker engine and rootless extras at the repository-pinned
version, verifies both archive SHA-256 values, and starts a dedicated transient
non-root systemd user service. Its client configuration, daemon configuration,
data root, exec root, RootlessKit state, socket, and ownership are all explicit;
it neither adopts the hosted runner's rootful daemon nor writes Docker context
state under `HOME`.

Ubuntu 24.04 restricts unprivileged user namespaces with AppArmor by default.
For the statically installed RootlessKit binary, the workflow follows Docker's
[documented rootless setup](https://docs.docker.com/engine/security/rootless/troubleshoot/#distribution-specific-hint):
it loads a `userns` exception attached only to the exact gate-owned RootlessKit
path, refuses to replace a pre-existing profile, and requires successful
profile unload before the job can pass. It does not disable the host-wide
restriction.

Recorded developer evidence (not product enablement or Linux release evidence):

- 2026-09-07, Darwin arm64 host with an explicit Colima Linux VM;
- canonical Docker CLI `29.7.2`, client/server API `1.54`, engine `29.5.2`;
- engine ID `4e6e1cac-3d18-41bc-86f9-ed932787f7d8`, Linux arm64, Ubuntu
  24.04.4 LTS, kernel 6.8.0-117, cgroup v2, built-in seccomp, memory/swap/PID
  limits enabled;
- all six adversarial probes passed and the exact ownership-label residue query
  returned no containers after cleanup.

This tuple establishes the Darwin/Colima development path only; it does not
stand in for the independent Linux rootless evidence recorded below.

Recorded Linux rootless evidence:

- 2026-09-07, exact mirrored commit
  `3ba535869fb1d1c3ecf59e765f9fb2477fcdffc5`, GitHub Actions
  [run 34074475726](https://github.com/urandon/sessionless/actions/runs/34074475726);
- GitHub-hosted `ubuntu24/20260831.293.1`, Ubuntu 24.04.4 LTS, x86_64,
  kernel `6.17.0-1022-azure`, cgroup v2, `overlay2`;
- pinned Docker client/server `28.0.4`, engine ID
  `3464e4fb-3d1d-447d-97b2-b6715dd67c53`, rootless security option verified;
- rootless-extras archive SHA-256
  `0d0c2680d924671df0ac33d53dd71f410dfb253fb4fa5e8a0a231951234781c9`;
- all six adversarial probes passed, and the gate-owned AppArmor profile,
  transient service, containers, images, engine data, runtime state, and
  temporary roots were cleaned successfully.

This is Linux rootless release evidence for the current launcher. Product
enablement remains blocked on the separate #77 packaging and composition work.

## Remaining gates

This profile does not provide foreground/service/container daemon packaging,
durable enrollment/configuration, update/logout/uninstall flows, AW-03/AW-04
exchange composition, canonical crash recovery, or #79's two-owner E2E. Those
items remain required before #77 closes.

Primary behavior and security references:

- [Docker container create](https://docs.docker.com/reference/cli/docker/container/create/)
- [Docker container run](https://docs.docker.com/reference/cli/docker/container/run/)
- [Docker tmpfs mounts](https://docs.docker.com/engine/storage/tmpfs/)
- [Docker rootless mode](https://docs.docker.com/engine/security/rootless/)
- [Docker Desktop macOS permissions and VM boundary](https://docs.docker.com/desktop/setup/install/mac-permission-requirements/)
- [Docker logging drivers](https://docs.docker.com/engine/logging/configure/)
