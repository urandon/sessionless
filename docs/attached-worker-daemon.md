# Attached-worker daemon and supervisor boundary

Issue #77 introduces the local Go boundary that will become the owner-managed
attached-worker daemon. The current AW-05a slice is deliberately a library
contract, not an enabled product daemon or a claim that host isolation is
complete.

## What is implemented

`internal/attachedworkerdaemon` provides three composable boundaries:

- `Supervisor` prepares one mandatory reviewed `IsolationBoundary`, starts its
  exact local client command, verifies the harness SHA-256 digest, supplies an
  exact argv and replacement environment, bounds stdout/stderr and wall time,
  and owns both the local process group and the external boundary lifecycle;
- `InvocationRunner` composes that process with the invocation-scoped
  credential lifecycle. It validates the returned handle and materialization,
  adds only the exact credential root to the launcher read allowlist and the
  exact `auth.json` to its file write allowlist, then
  attempts write-back and release under a separate bounded finalization
  context on every process outcome;
- `Daemon` admits at most one invocation, exposes content-free
  `running`/`draining`/`stopped` status, stops new admission during drain, and
  cancels the active process during bounded shutdown.

Every process invocation receives fresh `0700` `home`, `tmp`, `work`, and XDG
directories below the configured scratch root. The child environment is built
from scratch. Host `HOME`, `PATH`, XDG variables, API keys, tokens, secrets, and
other ambient variables are not inherited. Additional variables and read roots
must be allowlisted when the supervisor is constructed.

The supervisor sends `TERM` to the whole local process group, waits the
configured grace, sends `KILL`, waits/reaps the leader, then verifies that no
local descendant remains. It separately inspects the prepared isolation
boundary, requests graceful stop, force-stops it when necessary, verifies that
the isolated workload is gone, and releases the boundary under bounded cleanup
contexts. This distinction is mandatory for container or VM runtimes whose
workload can survive their local CLI process. Both cleanup paths also run when
the client exits naturally. Output readers and attempt-root deletion are
bounded parts of the same lifecycle; stderr content is never returned or
logged by this package.

## Required isolation launcher

A clean environment and private directory are hygiene, not isolation. A
production `IsolationLauncher` and every boundary returned by `Prepare` must
independently prove all of these
capabilities:

- exact filesystem read and write boundaries;
- denied network access;
- a process boundary that the attempted harness cannot escape;
- a hard attempt disk-byte bound.

The returned lifecycle handle must identify only that invocation, stop the
actual isolated workload rather than merely its CLI client, verify liveness
without trusting child output, honor every cleanup context, and make release
idempotent. A launcher that returns an error from `Prepare` owns cleanup of any
partially created runtime resources. `Prepare` receives a supervisor-bounded
context and must stop and clean partial resources when it expires. The
supervisor does not infer workload death from a successful client exit.

Construction fails with `ErrIsolationUnsupported` if any capability is absent.
The launcher is a trusted port and therefore needs its own platform-specific
implementation, tests, and security review. The test launcher in this package
is fixture-only evidence for process lifecycle behavior; it is not exported and
must never be used by a binary.

Darwin and Linux currently have process-group mechanics. Neither platform has
a committed production filesystem/network launcher yet. Other platforms fail
closed before process creation. In particular, isolated `HOME`, Seatbelt-free
process launch, a container-like directory layout, or a child-reported denial
does not satisfy this contract.

## Credential behavior

Credential handles remain exact to tenant, owner, worker, run, attempt, lease,
and fence. A handle returned by the credential service is checked again before
materialization. The auth file must be a canonical direct `auth.json` child of
a symlink-free credential root. That exact root, never the shared credential
scratch parent, is presented as a read root; only its exact `auth.json` is
presented as a writable file. A caller-supplied process spec cannot grant this
write path.

After the process stops, credential write-back runs before release even when
the process was cancelled or exceeded its deadline. Write-back or release
failure is returned as a stable content-free code and stops the daemon; the
package does not invent successful cleanup or retry an accepted provider turn.

## Drain and shutdown

`Drain` changes the daemon to `draining`, interrupts a blocked poll, refuses a
new invocation, and allows the current invocation to finish. `Shutdown` also
cancels the active invocation and waits only the configured shutdown grace.
The invocation runner and result sink are cancellation-aware ports; a component
that ignores context violates the daemon contract.

Status contains only stable scope identifiers, state, counters, timestamps,
and sanitized failure codes. It cannot represent prompts, results, credentials,
paths, raw stderr, provider errors, or auth material.

## Still required before #77 can close

- production Darwin and Linux isolation launchers with forbidden-read,
  forbidden-write, egress, process escape, and disk-limit tests;
- foreground CLI plus reviewed OS-service and container packaging;
- durable local identity/config storage, update/check, doctor, logout, and
  uninstall-plan commands;
- composition with the AW-03/AW-04 exchange and attempt protocol;
- crash/restart recovery that fences or resumes the exact durable attempt;
- the two-owner security and recovery gate in #79.

Until those items land, no binary enables this package and the attached-worker
daemon remains feature-disabled.
