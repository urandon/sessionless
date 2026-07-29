# YDB migrations

The application owns its YDB tables. Terraform may provision a database and
IAM bindings, but it must not manage the tables in this directory.

The ordered SQL files are embedded into `schema-migrate` and executed through
the Goose library. Every file is immutable after application and contains
exactly one idempotent schema operation. YDB does not support transactional
DDL, so grouping multiple operations into one migration would make crash
recovery ambiguous.

## Commands

```text
make migrate-local
make migration-status
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
