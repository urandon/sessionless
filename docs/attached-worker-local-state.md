# Attached-worker local state and lifecycle CLI

Issue #107 adds the durable owner-local authority required before a foreground
attached worker can compose the AW-03 transport with the AW-04 attempt machine.
The implementation is intentionally feature-disabled: it does not enroll,
connect, poll, execute a harness, install a service, or revoke server state.

## Authority boundary

The local state directory records configuration and local lifecycle only. It
does not become a second copy of the product state machines:

| Local field or result | Owning authority | Local meaning |
|---|---|---|
| tenant, owner and worker locators; enrollment and connection generations | AW-01 (`docs/attached-worker-identity.md`) | exact scope/fences last installed locally; not proof that the server still accepts them |
| identity-key fingerprint | AW-01 | digest binding for the private key in the separate secret record |
| protocol/server observation | AW-02/AW-03 | always `unknown` in this slice; no cached file upgrades it |
| attempt, cancellation and terminal state | AW-04 | absent; local daemon/process exit cannot commit product terminal state |
| OCI and harness configuration | AW-05/#106 | selected explicit inputs; configuration is not external verification |
| daemon observation | AW-05 | content-free, monotonic evidence written only while holding local process ownership; never server health |
| local logout receipt | #107 | deny-first retirement of local connection material only |

`manifest.json` is public-within-the-installation configuration. `secret.json`
is a separate mode-`0600` record containing the Ed25519 private key and, after
attachment, the 32-byte connection secret. Borrowed provider credentials and
invocation material never enter either file. File permissions protect against
other OS users but do not protect against a compromised process running as the
same user; stronger platform keystore integration remains a later design
choice.

## Explicit root and files

Every command requires `--state-dir` with one absolute canonical path. There is
no `HOME`, current-directory, repository, `PATH`, Docker-context, or provider
configuration fallback. The path and each existing parent must be symlink-free.
The root is owner-controlled mode `0700`; all direct files are owner-controlled
regular files mode `0600`.

The V1 inventory is fixed:

- `manifest.json` — strict, versioned configuration and local lifecycle;
- `secret.json` — separate private material, present only while locally active;
- `logout-intent.json` — crash-recovery barrier for a local logout;
- `runtime-observation.json` — optional content-free AW-05 daemon observation;
- `state.lock` — advisory serialization for state reads and mutations;
- `runtime.lock` — advisory one-process foreground ownership.

The two lock files are created during initialization. Read-only `check`,
`doctor`, `status`, and `uninstall-plan` operations never create them or repair
missing state. Darwin and Linux use a non-blocking kernel `flock`; process death
releases the authority, so timestamps are never used to guess that a lock is
stale. Other platforms return `platform_unsupported`.

Only the holder of `runtime.lock` can persist `runtime-observation.json`.
Observation revisions advance exactly once, their manifest revision must match,
timestamps and counters cannot move backwards, and invalid or stale evidence
fails closed. A configuration update or local logout durably removes the old
observation before committing its new manifest state.

JSON decoding is size bounded, UTF-8 only, case-insensitive duplicate-key
rejecting, unknown-field rejecting, and single-value only. Unsupported schema
versions, permission drift, symlink substitution, orphaned transaction files,
unknown direct inventory entries, and manifest/secret revision or generation
disagreement fail closed.

## Durability and ambiguity

Writes create a same-directory mode-`0600` temporary file, write and `fsync`
it, close it, atomically rename it over the exact target, then `fsync` the state
directory. Initial creation also `fsync`s the state root's parent before any
successful return. A directory-sync failure after a namespace mutation returns `state_ambiguous`;
the caller must reload and reconcile the exact revision rather than repeat an
effect blindly. An orphaned `.sessionless-tmp-*` file is treated as
`state_incomplete`, not silently ignored.

An active update writes the generation-bound secret record before committing
the new manifest. A crash between those commits creates a detectable mismatch
and disables loading. Enrollment and connection generations can stay the same
or advance exactly once. Secret material must remain byte-identical when its
generation stays the same and must change when that generation advances.

Local logout is a small journaled transition:

1. atomically persist `logout-intent.json` for the exact manifest revision and
   caller-supplied idempotency key;
2. durably remove any prior daemon observation;
3. durably remove `secret.json`;
4. atomically commit the `logged_out` manifest and content-free receipt;
5. durably remove the intent.

The presence of an intent denies secret reads immediately. The same
idempotency key can resume after interruption; a different request conflicts.
The receipt says only `secret_state=retired`,
`server_revocation=unknown`, and `remote_erasure=unknown`. It never claims that
an offline control plane or remote host observed anything.

## Operator commands

The feature-disabled binary is built as `attached-worker`:

```text
attached-worker check --state-dir /absolute/symlink-free/path
attached-worker doctor --state-dir /absolute/symlink-free/path
attached-worker status --state-dir /absolute/symlink-free/path
attached-worker logout --state-dir /absolute/symlink-free/path \
  --expected-revision 7 --idempotency-key logout-request-001
attached-worker uninstall-plan --state-dir /absolute/symlink-free/path
attached-worker run --state-dir /absolute/symlink-free/path
```

All commands emit one bounded JSON object and stable result code. They never
echo the state root, endpoint user info/query, private key, connection secret,
provider credential, environment, process arguments, stdout, or stderr.

- `check` proves strict local consistency and exact SHA-256 identity of the
  configured Docker and harness executables; it does not execute either one.
- `doctor` additionally validates the supported host/boundary pairing, pinned
  Docker and harness executable digests, and empty private Docker CLI
  directory. Engine, external-release, daemon, and server observations remain
  `unknown` because this slice makes no live call.
- `status` exposes local lifecycle and generations plus an exact content-free
  daemon state when a valid local observation exists. Server observation stays
  `unknown`; a local `running` value is not server acceptance or health.
- `logout` performs only the journaled local deny-first transition above.
- `uninstall-plan` reports the exact relative inventory and whether the kernel
  runtime lock is free. It deletes nothing and always states that destructive
  action requires separate explicit consent.
- `run` performs the bounded [foreground preflight](attached-worker-foreground.md),
  proves one-process ownership, and returns `feature_disabled` after retiring
  its local observation. It performs no network, process, provider, or OCI
  action.

## Remaining gates

The following remain required before #77 can close:

- enrollment/bootstrap and durable connection setup;
- live composition after the feature-disabled foreground preflight: the AW-03
  HTTP transport/poller, AW-04 attempt protocol, daemon, credential lifecycle,
  and reviewed OCI launcher;
- authenticated local daemon status/control and crash/restart reconciliation;
- reviewed OS-service and container packaging plus update and destructive
  uninstall flows;
- the two-owner security and recovery gate in #79.

Production execution remains disabled until those dependent slices pass
independent review and their own exact-head/runtime gates.
