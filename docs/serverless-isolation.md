# Serverless isolation and lifecycle boundary

Issue #88 adds the feature-disabled execution boundary for one exact
`PreparedInvocation`. It composes the reviewed AW-05 process supervisor with
the serverless authority chain from #87. It does not enable a cloud substrate,
provider credential, or production backend.

## Authority and attestation

`internal/serverlessisolation.SupervisorV1` accepts only a process-local,
issuer-authenticated `PreparedInvocation`. The registry consumes that
capability before calling a substrate driver; the supervisor validates it
again immediately before preparing a process. A capability from a restarted
issuer is invalid and cannot be reconstructed from durable DTOs.

`Preflight` asks the trusted launcher to observe the allocation without
creating a workspace, reading credentials, opening network traffic, or
spawning a process. The returned allocation must exact-match the sealed:

- image and outer harness digests;
- egress proxy artifact and identity digests;
- workload mode;
- inner executable digest and exact argv;
- native protocol and backend-profile digests.

The AW-05 boundary then re-hashes the executable before preparation and again
immediately before process start. Any mismatch fails before the workload can
run.

## Fresh execution boundary

Every run inherits AW-05's mandatory isolation profile. A production launcher
must independently prove filesystem read/write boundaries, denied network,
process containment, and a hard disk bound. A replacement environment and a
private directory alone are not accepted as isolation.

The supervisor creates fresh `0700` home, temporary, work, and XDG directories
below a canonical non-symlink scratch root. It inherits no host environment.
Only names allowlisted at supervisor construction can be supplied, and AW-05
still rejects reserved environment names, duplicate names, path substitution,
and unapproved read roots.

The exact substrate authority supplies wall-time, cleanup, stdout, stderr,
event, artifact, and evidence bounds. Values that cannot be represented by the
reviewed AW-05 implementation fail closed instead of being truncated or
defaulted.

## Stop and finalization order

One run has this fixed order:

1. supervise the local process group and external isolation boundary;
2. reap descendants, tear down and release the boundary, and remove scratch;
3. give the bounded stdout prefix once to the output finalizer;
4. zero the prefix and remove it from every later request and public result;
5. observe credential write-back/release finalization;
6. verify absence of workspace, processes, sockets, and credentials;
7. request instance taint when stop, credential finalization, or residue is
   failed or unknown.

The ordinary output/credential/residue operations share the sealed cleanup
budget. If that budget expires, taint/termination receives a separate bounded
emergency context (capped at 30 seconds); it is never invoked with an already
cancelled context.

Output persistence is deliberately orthogonal to instance hygiene. A failed
or over-budget output proof blocks the invocation result but does not taint an
otherwise verified-clean instance. Failed/unknown stop, credential
finalization, or residue does taint it. Failure to confirm that taint is a
separate `ErrTaintNotConfirmed` condition.

`Cleanup=verified` therefore means all three reuse-safety gates passed. It does
not mean the provider turn succeeded, its outputs were accepted, cancellation
was acknowledged, or the canonical attempt was committed. Those remain
separate facts owned by the surrounding substrate driver and canonical state
machine.

## Verification surface

The package tests cover:

- exact positive attestation and negative substitution of every bound digest,
  argv, protocol, and profile;
- process exit, timeout, active cancellation, a TERM-resistant descendant, and
  output overflow;
- process-local capability invalidation across issuer restart;
- symlink scratch-root rejection;
- bounded output/evidence proof validation and raw output erasure;
- ordered output, credential, and residue finalization;
- independent output, credential, boundary-release, residue, and unconfirmed
  taint failures.

The process-tree, path, environment, output-reader, and cleanup mechanics remain
owned and tested by `internal/attachedworkerdaemon`; #88 does not fork or weaken
that implementation.

## Still disabled

No concrete Yandex launcher, provider proxy, credential materializer, warm-pool
reuse, arbitrary shell/tool profile, or native-session resume is registered by
this change. Those remain the later PR-03c through PR-03f gates in
[serverless-harness.md](serverless-harness.md). The separate feature-disabled
PR-03c composition boundary is specified in
[serverless-egress.md](serverless-egress.md). A production profile cannot be
enabled until its concrete launcher, proxy, secret adapter, and exact image
revision prove the same negative matrix in cloud-dev.
