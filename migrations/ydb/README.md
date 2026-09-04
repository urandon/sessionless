# YDB migrations

The application owns its YDB tables. Terraform may provision a database and
IAM bindings, but it must not manage the tables in this directory.

The ordered SQL files are embedded into `schema-migrate` and executed through
the Goose library. Every file contains exactly one idempotent schema operation.
YDB does not support transactional DDL, so grouping multiple operations into
one migration would make crash recovery ambiguous.

## Baseline freeze

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

Automatic production down migrations are intentionally disabled. The `Down`
sections are comments so neither Goose nor an operator can accidentally drop
state.

## Crash repair

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
