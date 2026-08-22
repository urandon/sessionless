# Session lifecycle and destructive retention

Sessionless treats archive, storage retention, destructive deletion, and
legal/audit retention as four independent operations. None is an alias for
another.

## Archive and unarchive

Archive is a reversible visibility state on the canonical `Session`. It keeps
the ordered event stream, snapshots, artifact manifests, bindings, and
immutable Object Storage keys. Repeating archive or unarchive is idempotent: a
retry does not replace the timestamp of the first successful transition.
`/new` remains separate and never archives the previous session.

Canonical session rows have no YDB TTL. Operational TTLs may expire delivery,
lease, checkpoint, quota, and idempotency rows, but must not remove canonical
history.

## Object Storage tiering

Objects under `tenants/` are immutable and start in `STANDARD`. Terraform
moves current versions to `COLD` after `artifact_cold_transition_days` (30 by
default), then to `ICE` after `artifact_ice_transition_days` (365 by default).
The lifecycle changes storage class only; object key, digest, manifest, and
event ordering remain unchanged. Non-current versions expire after the
separate `artifact_retention_days` window.

`ICE` has a minimum billable storage duration of 12 months. Deleting an ice
object earlier charges the remaining minimum duration, so the repository
rejects an ICE threshold below 365 days. Reads and transitions also cost more
in colder classes. Review the access profile before changing these values:

- [Yandex Object Storage classes](https://yandex.cloud/en/docs/storage/concepts/storage-class)
- [lifecycle rule format](https://yandex.cloud/en/docs/storage/s3/api-ref/lifecycles/xml-config)
- [current pricing and minimum duration](https://yandex.cloud/en/docs/storage/pricing)

Storage tiering is ordinary cost retention, not deletion. Legal hold does not
prevent a storage-class transition because the bytes and identity remain
available.

## Legal hold

A legal hold is a durable per-session record with set/release actor, reason,
and timestamps. An active hold blocks deletion request, start, and completion,
while authorized reads and archive transitions continue to work. The released
hold record and durable audit events survive session deletion.

Set or release a hold through the operator-only command:

```sh
export SESSION_DELETE_TENANT_ID='<tenant-id>'
export SESSION_DELETE_SESSION_ID='<session-id>'
export SESSION_DELETE_OPERATOR_ID='<internal-user-id>'
export SESSION_DELETE_REASON='<ticket and retention reason>'
make session-hold
make session-release-hold
```

The operator boundary must authenticate and authorize hold administration.
The command never accepts tenant/session targets as positional text and never
performs a prefix delete.

## Destructive deletion

Deletion is a durable state machine:

```text
requested -> deleting -> completed
```

Only an active session owner can create `requested`. That transaction checks
for an active legal hold and non-terminal runs, records a durable audit event,
and establishes a write fence. Later canonical events, snapshots, bindings,
run state, archive transitions, and reuse of the session ID are rejected.

The dry-run walks tenant/session/run-keyed indexes, with one shared explicit
10,000-row bound across events, snapshots, participants, bindings,
projections, runs, manifests, deliveries, and checkpoint ledgers, plus a
10,000-object bound. New `BlobRef` values must belong to the selected tenant
and remain below `tenants/<tenant-id>/sessions/<session-id>/`. A migrated
legacy object is accepted only below the exact `runs/<run-id>/` prefix of a
run proven to belong to that session. Objects are sorted by exact key; the
confirmation token includes a digest of the full inventory and its proven run
IDs. There is no recursive, root, bucket-wide, glob, or unresolved-variable
delete path.

### Operator procedure

1. Export the YDB and Object Storage environment used by the normal runtime.
2. Export the exact tenant, session, active owner, and reason values.
3. Record the request:

   ```sh
   make session-delete-request
   ```

4. Inspect the bounded JSON plan and copy its `confirmation` value:

   ```sh
   make session-delete-plan
   ```

5. Recheck the ticket, legal-hold status, object count, byte count, and every
   object prefix. Then execute with the exact token:

   ```sh
   export CONFIRM_SESSION_DELETE='delete-session:<tenant>:<session>:<inventory-digest>'
   make session-delete
   ```

The executor transitions to `deleting`, issues idempotent deletes for each
exact object reference, and only then removes canonical rows and records the
durable `completed` tombstone and audit event. An interruption leaves the state
at `deleting`; rerun the plan and the same confirmed command. Already removed
exact objects are safe to delete again. `deleting` is the irreversible phase:
a new legal hold is rejected after it starts, so holds must be established
while the deletion is still `requested`.

The local composed gate deletes one exact object, injects an interruption,
verifies that the durable plan and confirmation are unchanged, and then proves
that retry completes without changing same-tenant or cross-tenant sentinel
sessions. It also removes the target run's TTL-governed operational rows before
planning and proves that canonical history, artifacts, and the non-TTL delivery
and checkpoint ownership ledgers remain sufficient for exact cleanup. This is
a deterministic simulation of TTL effects, not evidence about wall-clock YDB
TTL scheduling.

Before enabling deletion after an upgrade, complete the expand/migrate/cutover
procedure in `migrations/ydb/README.md`. Delivery and checkpoint object
ledgers deliberately have no TTL: operational rows may expire, but exact
object ownership required for later audited deletion must remain available.

Completion removes canonical and content-bearing session state, including the
session display, events, snapshots, participants, bindings, projections, runs,
manifests, and frontend delivery payloads. An exact per-run delivery index
ensures that both inline results and referenced objects are included without
scanning another tenant's state. Other operational rows governed by bounded
TTL may age out under their existing policy. Run lease heads remain as
content-free fencing records so a stale worker fence can never be reused. The
deletion tombstone, released hold, and lifecycle audit events remain so a
deleted ID cannot be silently recreated and an incident can be reconstructed.

The retention/deletion semantics for payload-free API idempotency, mutation,
and Web upload-intent records are tracked separately in issue #57. Until that
decision is implemented, the deletion contract does not claim that every row
which happens to contain a `session_id` is erased.

Tenant/account deletion is an orchestration of these single-session state
machines. It must enumerate authorized session IDs and invoke this exact
procedure per session; it must not introduce a tenant-prefix delete shortcut.

## Recovery and incidents

- `requested`: no object has been intentionally deleted; resolve holds or
  terminal-run blockers, rerun the plan, and require confirmation.
- `deleting`: some exact objects may be absent; rerun the plan and executor.
  Do not edit durable state or manually remove YDB rows.
- `completed`: canonical data is intentionally unavailable. Use the durable
  tombstone and `session.deletion.*` audit events for investigation.
- unexpected cross-boundary reference: stop. Inventory validation fails
  closed; repair the manifest/index inconsistency through a reviewed incident
  procedure before retrying.

Object Storage versioning and non-current retention may provide a limited
infrastructure recovery window, but they are not a product restore promise.
Early deletion from `ICE` can also incur the remaining 12-month minimum charge.
