# Credential lifecycle Phases B0-B1

Phase B0 defines a provider-neutral, local-only credential lifecycle contract.
Phase B1 proves its orchestration inside the harness-neutral worker Manager.
It is deliberately not activated in `cmd/worker-runtime` and does not add a YDB
credential backend, Docker or Terraform changes, or a real subscription login.
Runtime and provider activation remain blocked on the isolated worker boundary
in #18 and the credential lifecycle epic in #13.

## Manager orchestration

Canonical ingress writes the independently membership-authorized user as
`credential_owner_user_id` in the dispatch payload. Scheduler admission copies
that identity unchanged into the WorkerJob payload. Both records already use a
canonical YDB `JsonDocument`, so this field requires no duplicate physical
column or migration. Newly created canonical records always include it. A
legacy zero value remains structurally loadable only so credential-disabled
deterministic workers remain unchanged; required mode rejects it without a
fallback or inferred alias. Existing queued jobs must therefore be drained or
reset before required mode is enabled.

Manager configuration has two modes: disabled (the existing deterministic
behavior) and required. Required mode reloads the current Run, Attempt, and
Lease by exact tenant-scoped keys after the worker start transaction. It
revalidates running states, worker, lease ID, fence, owner, connection, and
tenant before calling the lifecycle. The active lease must cover the full
admitted `MaxRuntime` plus a positive finalization grace bounded to one minute;
otherwise processing fails before `Materialize`.

The required sequence is `Issue → Materialize → Harness Execute → WriteBack →
Release`. Only the invocation handle and exact local materialization enter the
in-memory `ExecutionRequest`; neither is persisted. Write-back runs after
success, harness error, timeout, or cancellation. Release runs even when
write-back fails, using a cancellation-independent bounded finalization
context. Lifecycle errors are converted to a fixed worker orchestration error
and generic durable failure code, so secret bytes, references, handles, and
backend details cannot reach queue, checkpoint, artifact, or error surfaces.

## Authority and scope

An active version-1 binding is authoritative for one tenant, subscription
connection, owner user, provider, authentication mode, entitlement, secret
fingerprint, and positive generation. Its secret reference is opaque: generic
formatting redacts it and JSON omits it. A revoked binding increments the
generation and clears the active reference, fingerprint, and entitlement. The
superseded reference survives only in the binding store's durable cleanup
intent.

`Issue` accepts validated `Run`, `Attempt`, and `Lease` domain objects rather
than free identifiers. The run and attempt must be running; tenant, run,
attempt, worker, lease, and fence must agree; and the lease must be active. A
handle expires at the earlier requested boundary, which must not exceed the
lease expiry. Expiry is exclusive.

The returned handle is invocation-scoped and binds:

- tenant, connection, and owner;
- run, attempt, and worker;
- lease ID and fence token;
- binding generation and expiry.

Every use rechecks the complete immutable handle and the authoritative binding
generation. Wrong-scope, expired, released, revoked, and stale handles fail
closed. Handles must not enter queues, checkpoints, artifacts, session events,
logs, or analytics.

## Local materialization

The service receives an existing trusted scratch directory and creates a fresh
private service root with `0700` permissions. The requested scratch path must
be normalized and its final component cannot be a symlink; symlinked system
ancestors such as macOS `/var` are resolved once and the internal root uses the
canonical path. Each handle can materialize once into a direct child directory
(`0700`) containing one regular `auth.json` file (`0600`). On Linux and macOS,
the service pins the service and invocation directory inodes and performs file
operations with `openat(O_NOFOLLOW)`, descriptor validation, and `unlinkat`.
Unsupported operating systems fail closed. Reads are bounded and reject
traversal, symlinks, mode drift, replacement races, empty files, and oversized
files.

`Release` is idempotent and removes only the exact registered file and direct
child directory. It never performs recursive cleanup. Concurrent release and
revoke callers share one per-handle cleanup owner and wait for the same result,
so duplicate cleanup cannot turn a successful durable revoke into a failure.
Materializations must not be reused across tenants or invocations.

## Write-back and recovery

Unchanged content performs no secret write and preserves the generation.
Changed content follows this order:

1. durably put an uncommitted candidate scoped to tenant, connection, owner,
   and expected generation;
2. compare-and-swap the authoritative binding to the candidate and next
   generation while atomically recording cleanup for the superseded secret;
3. promote the exact candidate to committed state.

An interruption before the CAS leaves an enumerable, scope-bound orphan for a
reconciler. An interruption after the CAS leaves a binding that points at the
uncommitted candidate. The next `Issue` asks the secret backend to atomically
recover that exact binding-to-candidate relation before issuing a handle.
Missing or mismatched candidates fail closed; a broad candidate scan is never
used to guess the active secret.

`RevokeConnection` is deny-first. The binding store atomically increments the
generation and publishes the revoked shape before in-memory handle invalidation
or cleanup. It then removes exact registered materializations nonrecursively
before physical secret deletion. Therefore a concurrent late write-back CAS
loses and cannot resurrect the connection, while already materialized files do
not remain usable until a cooperative caller releases them. Cleanup failure may
be retried from the durable intent without restoring authority.

## Evidence boundary

Errors exposed by the service are fixed sentinels and contain no raw credential
or backend reference. Tests cover generic formatting/JSON redaction, exact
scope, exclusive expiry, one-shot use, secure filesystem modes and path checks,
changed/unchanged CAS behavior, both write-back crash points, exact recovery,
and a deterministic revoke-versus-write-back race under the Go race detector.
