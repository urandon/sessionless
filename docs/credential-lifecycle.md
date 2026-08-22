# Credential lifecycle Phase B0

Phase B0 defines a provider-neutral, local-only credential lifecycle contract.
It is deliberately not wired into `worker-runtime`, the worker manager, YDB,
Docker, Terraform, or a real subscription login. Runtime activation remains
blocked on the isolated worker boundary in #18 and the credential lifecycle
epic in #13.

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
private service root with `0700` permissions. Caller-selected final roots,
non-normalized paths, and roots reached through symlinks are rejected. Each
handle can materialize once into a direct child directory (`0700`) containing
one regular `auth.json` file (`0600`). Reads are bounded and reject traversal,
symlinks, mode drift, replacement races, empty files, and oversized files.

`Release` is idempotent and removes only the exact registered file and direct
child directory. It never performs recursive cleanup. Materializations must not
be reused across tenants or invocations.

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
