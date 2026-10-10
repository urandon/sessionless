# YDB migrations

The application owns its YDB tables. Terraform may provision a database and
IAM bindings, but it must not manage the tables in this directory.

The ordered SQL files are embedded into `schema-migrate` and executed through
the Goose library. Every file contains exactly one idempotent schema operation.
YDB does not support transactional DDL, so grouping multiple operations into
one migration would make crash recovery ambiguous.

## Baseline freeze

Migration `00102` adds the bounded, non-authoritative `run_explanation_heads_v1`
read projection (design 0.1.2, #187). Apply its additive schema only under
separate migration authority, then upgrade every canonical writer before
enabling a consumer. No HTTP route is mounted by #187; historical missing
locators remain unknown, with no backfill. Rollback disables consumers and
preserves this table/canonical state. Session deletion removes its exact
tenant/run row even while the route is disabled. No destructive down migration
or production apply is authorized by this change.

Migrations `00103` and `00104` add the finite shared read-rate slots and checked
writer-first deployment receipt for #188 (evidence design 0.1.3). They do not
mount the route or create a receipt. `WEB_RUN_EXPLANATION_ENABLED` is absent/false
by default. Explicit true additionally requires `WEB_RUN_EXPLANATION_WRITER_COMMIT`
to be the exact lower-case 40-hex deployed projection-writer commit, matched by
`run-explanation-rated-v1-writer-first-cutover` in
`run_explanation_cutover_state_v1`. Receipt version, writer, schema and rated
reader versions must all be 1; `old_writer_count` must be zero, completion must
not be in the future, and a 64-hex SHA-256 digest must link the separately retained
old-writer inventory/drain evidence. A flag alone, branch name or missing table
is insufficient. Startup checks both exact-key table schemas without writing.

Under separate deployment authority: apply the additive migrations; upgrade all
canonical writers (scheduler/admission, workers, reconcilers, attached completion
and session deletion); inventory each deployed instance and drain every old
writer; preserve its inventory/commit/drain evidence; then record the exact
typed cutover receipt and enable the BFF with the matching deployment pin.
These repository changes do not perform any of those cloud steps. Rollback
disables the route, retaining both projection and cutover evidence. Session/Run
projection deletion is never gated on read enablement. Rate rows hold only
hashes and bounded 30-second debit receipts, expire logically after ten minutes,
and occupy only slots 0–4095. No independent TTL is enabled: it could delete a
corrupt row based on a mismatched expiry column before validation. Logical reuse
checks the whole row and exact expiry equality, while the finite slot space
bounds physical metadata without a cleanup requirement.

Before the first production deployment, the migration baseline may be rebased
in a reviewed change. Local, CI, and the current pre-production `cloud-dev`
database contain disposable development data and must be recreated from the
revised baseline. A cloud-dev rebase must use the repository-owned guarded
`make cloud-app-reset-plan` / `make cloud-app-reset` procedure; manual table or
bucket deletion is not an accepted migration step.

The first production deployment freezes every migration present in that
deployment. Record the deployed commit and migration head in the deployment
evidence. From that point onward:

- never edit, renumber, or delete an applied migration;
- make every correction through a new forward migration;
- verify checksum history before application rollout.

A baseline rebase must run the complete migration set twice against a clean YDB
Local instance and reset every affected disposable environment before applying
the revised files. The guarded cloud-dev reset drops only the explicit
Sessionless application-table allowlist and deletes only the `tenants/` Object
Storage prefix. It preserves Terraform state, bootstrap/deployment-lock YDB,
IAM, Lockbox, KMS, queues, registry, bucket configuration, and unrelated object
prefixes.

## Commands

```text
make migrate-local
make migration-status
make partition-status
make partition-backfill
make cloud-app-reset-plan
make cloud-app-reset
```

Both commands require `YDB_CONNECTION_STRING`. Authentication is selected by
the official YDB environment credential chain:

- local YDB: `YDB_ANONYMOUS_CREDENTIALS=1`;
- Yandex Cloud serverless runtime: `YDB_METADATA_CREDENTIALS=1`, or its default
  metadata fallback;
- temporary developer access: `YDB_ACCESS_TOKEN_CREDENTIALS`, injected from
  the shell or an OS secret store.

Credentials must not be added to the connection string, command line, image,
or repository.

## Safety protocol

Before a migration is executed, the runner:

1. bootstraps the idempotent migration metadata tables;
2. acquires the fenced `sessionless-schema` lease in YDB;
3. records the file name and SHA-256 checksum as `pending`;
4. applies one Goose migration;
5. records the checksum as `applied`;
6. renews the lease before the next file and releases it at the end.

If the process stops after DDL but before Goose records the version, the next
run replays the same `CREATE ... IF NOT EXISTS` statement. If it stops after
Goose records the version but before the final checksum update, the next run
promotes the already-recorded pending checksum. A changed file fails closed
with checksum drift.

## Expand, migrate, contract

Production changes use three separately deployed phases:

1. **Expand:** add compatible nullable columns/tables/indexes.
2. **Migrate:** deploy dual-read/write code and complete a resumable,
   tenant-keyed backfill.
3. **Contract:** remove obsolete data only in a later release after evidence
   shows that no deployed version depends on it.

For migrations `00062`-`00067`, deploy the expanded schema and dual-write
code first, run `make partition-backfill` after old writers have drained, and
only then treat the `session-lifecycle-indexes-v1` marker as cutover evidence.
Before that marker, serving reads union the new indexes with bounded legacy
fallback queries, so an existing binding or deletion object cannot silently
disappear. The backfill also copies delivery/checkpoint BlobRefs into durable
non-TTL ledgers. If their operational source row has already expired, stop and
handle it as a retention incident; an object reference must never be guessed.

Migrations `00068`-`00070` are additive session-API tables: tenant/user-scoped
create and mutation idempotency ledgers plus a bounded, rebuildable display
materialization. None contains canonical event payloads or attachment bytes.

Migrations `00071`-`00072` add operational projection indexes only. The
per-run index includes `frontend` so one adapter cannot claim another
frontend's work; the ready index uses the stable 16-bucket hash contract for
lost-wake recovery. `make partition-backfill` derives both from existing
projection rows and their canonical event run references. Pre-index orphan
rows whose canonical event is already absent are skipped so one retired tenant
cannot block unrelated backfill; no canonical content or replacement run ID is
invented.

Migrations `00073`-`00074` add the tenant-partitioned Web upload-intent entity
and its user-scoped creation-idempotency ledger. Upload metadata is stored as
one bounded JSON document; object bytes remain in Object Storage. Migration
`00075` adds an optional request-content digest to the canonical frontend
ingress ledger so new Web messages can reject reuse of an idempotency key with
changed text or ordered upload selectors before resolving compute or touching
staged objects. Existing non-Web ingress rows remain compatible.

Migration `00076` adds the owner-keyed `subscription_connections_by_user`
projection. Telegram identity enrollment writes it atomically with the base
connection and repairs an absent projection on an exact retry. The Web resolver
reads at most two rows from one authorized user prefix, then point-reads the
base connection and its actor mapping; stale or mismatched rows fail closed.
Pre-production environments created before this projection must replay their
authoritative identity enrollment or be reset through the guarded application
reset before Web compute selection is enabled.

Migrations `00077`-`00079` add the owner-scoped attached-worker identity
boundary. The enrollment table retains the digest-only single-use grant beyond
its bootstrap expiry and applies TTL to `retain_until`, not `expires_at`. The
worker table stores the durable Ed25519 public identity and monotonic enrollment,
connection, and revision fences. The audit table is content-free and ordered by
worker revision, including enrollment creation at revision zero. All serving
queries use exact `(tenant_id, owner_user_id, ...)` keys or bounded owner-prefix
ranges; transport, provider, credential, capability, and dispatch state are not
stored by these tables.

Migrations `00080`-`00083` add the bounded outbound attached-worker transport
state. Single-use attach challenges retain their consumed marker beyond the
authentication deadline; immutable capability content is keyed by its digest
while the current per-connection signed observation stays on the connection
head; that owner-scoped head coalesces protocol watermarks and presence
checkpoints; and the stable 16-bucket expiry index supports
bounded offline recovery. Raw nonces, bearer credentials, proofs, prompts,
provider credentials, and tool payloads are never persisted by these tables.

Migrations `00084`-`00086` add the fenced attached-worker execution boundary.
One owner-scoped worker head is the concurrency-one contention point and stores
only the current bounded attempt snapshot. Directional attempt messages retain
exact fingerprints and canonical protocol records for ambiguous-response replay.
The stable 16-bucket composite deadline index drives bounded lease and cancel
recovery without scanning worker or attempt payloads. These tables contain no
prompt, result, credential, provider, tool, MCP, path, URL, bearer, nonce,
signature, proof, or channel-binding bytes.

Migration `00087` records the one-time explicit execution-placement cutover.
With every old dispatch writer and reader stopped under the deployment lock,
`make partition-backfill` uses one serializable transaction to require both
legacy `dispatch_outbox` and `worker_jobs` empty and write the marker. The
current pre-production rollout must use the typed reset and is fresh-only;
retained-data migration requires a separately reviewed bounded backfill.
Web BFF, control API, reconciler, and worker runtime refuse startup before the
marker. Serving readers never reinterpret a missing placement as managed.

Migration `00088` records the separate one-time HarnessBindingV1 empty-backlog
cutover. Stop every ingress, scheduler, reconciler, managed worker, and attached
worker admission process; drain `dispatch_outbox` and `worker_jobs`; then run
the serializable cutover. Every serving binary refuses to start without this
second marker. It never permits a legacy zero binding to be interpreted as the
deterministic backend.

Migrations `00089`-`00093` add the owner-scoped provider credential authority
for API and router accounts. The binding row contains only resource revisions,
credential generations, a safe fingerprint, and an opaque secret-backend
locator; plaintext credential bytes never enter YDB or its JSON document. A
rotation or revocation enqueues the superseded locator atomically in the
bounded cleanup table and its stable 16-bucket ready index so an ambiguous
secret-backend deletion is recoverable without another owner mutation.
Each accepted generation or revoke transition also writes one content-free,
deterministic owner/resource audit receipt in the same serializable transaction.
The candidate-fence ledger is a permanent mutation tombstone: candidate cleanup
and binding CAS serialize on the exact owner/resource/mutation tuple, so cleanup
may delete plaintext only after making a paused or retried CAS unable to promote
that candidate. If the CAS won first, cleanup observes the exact authoritative
binding and recovers the candidate instead. The guarded cloud-development reset
therefore requires bindings, cleanup work, and candidate fences to be empty only
after the external secret namespace has been explicitly drained; it never drops
the last deletion/fencing locators on its own.
These tables are separate from subscription connection credentials and do not
authorize provider routing, admission, or execution by themselves.

This slice remains feature-disabled: migrations and contracts do not authorize
credential ingestion or provider use. Enabling it requires a separately
reviewed tenant-scoped encrypted secret backend, bounded candidate recovery,
the cleanup drain, invocation-only materialization/release, and the serving
cutover that proves those components are composed. Until then no binary may
mount an ingestion route or accept a real provider key.

Migrations `00094`-`00095` add the immutable managed provider-effect fence and
the fresh-only ManagedExecutionAuthorityV2 cutover marker. The effect table is
keyed by `(tenant_id, attempt_id, kind)` and is written with `INSERT` only after
the canonical worker job, lease/fence, cancellation state, evidence freshness,
context/input digests and database time have been checked in one serializable
transaction. A different physical claim is reconcile-only. Before enabling any
dispatch reader or writer, the cutover transaction requires `dispatch_outbox`,
`worker_jobs` and `attempt_effect_reservations` all empty; serving code never
coerces an old payload or a missing substrate/cost binding.

Migration `00096` adds one content-free, typed substrate execution evidence
record per managed attempt. The terminal worker transaction validates it
against the persisted effect authority and reservation, stores it atomically
with canonical completion, and includes its digest in finalization
idempotency. The substrate observation remains distinct from canonical run
terminal state and never asserts that commit itself.

Migration `00097` adds append-only reconciliation evidence for a reserved
provider effect. Each row seals the persisted invocation authority,
reservation digest, winning physical claim, typed substrate observation and
observation time. Repeated evidence is idempotent by digest. A `not_found`
observation is inserted in the same transaction that terminally fails the
attempt; other closed observation states are durable retry diagnostics and do
not permit another provider effect.

Migration `00098` adds the owner-scoped attached-worker control-message ledger.
One semantic drain revision is keyed by worker, revision, and direction. The
outbound row may start without a connection envelope while an earlier platform
frame is unacknowledged; delivery or reconnect replaces only that envelope.
The worker's Drained acknowledgement is a separate immutable direction row.
Worker desired/observed state, connection snapshot, control ledger, attempt
retirement checks, and sanitized audit events are reconciled in the same
serializable transaction.

Automatic production down migrations are intentionally disabled. The `Down`
sections are comments so neither Goose nor an operator can accidentally drop
state.

## Crash repair

The [Session retention and replay draft](../../docs/design/session-retention-replay.md)
owns the proposed four-table retention, versioned replay/ownership indexes,
historical coverage and cutover design for #159 / #57. It adds no executable
migration, TTL or current coverage marker. Existing migration behavior below
remains unchanged until that contract and its implementation are accepted.

When a migration fails:

1. stop concurrent deploys and run `schema-migrate status`;
2. compare the pending file with the schema in YDB;
3. restore the original committed file if checksum drift is reported;
4. rerun the idempotent step if it is partially applied;
5. add a new forward migration for corrections—never edit an applied file.

Do not delete or manually advance `sessionless_goose_versions`,
`schema_migration_checksums`, or `schema_migration_lock` without a reviewed
incident procedure.

References:

- https://ydb.tech/docs/en/integrations/migration/goose
- https://ydb.tech/docs/en/reference/ydb-sdk/auth
