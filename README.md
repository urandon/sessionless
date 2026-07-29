# sessionless

Sessionless is a serverless, cloud-hosted control plane for routing
conversation-backed work to isolated AI-agent workers.

The core is frontend-aware but not frontend-specific. Telegram is the first
frontend and its chat history provides the authoritative conversation context
for the initial product slice. Additional frontends can be added through
transport adapters without redefining runs, scheduling, quota accounting, or
worker isolation. A new clean context is always an explicit frontend action
that advances a context epoch; it does not create a hidden primary session
model or delete the frontend's history.

The control plane is also harness-neutral. Codex, OpenCode, Claude, and
Hermes-style runtimes are candidates for isolated worker adapters, not
dependencies of the domain model. No harness is selected as the permanent
runtime yet.

The target deployment is a scale-to-zero Yandex Cloud serverless environment:
Go control-plane services, YDB operational state, at-least-once queues, and
tenant-partitioned Object Storage. Harness processes execute outside the
control plane in isolated workers with explicitly granted credentials, blobs,
and MCP access.

The repository currently contains the Go component boundaries, harness-neutral
domain/runtime contracts, the authoritative YDB state store and migrations, a
reproducible local development stand, isolated worker packaging, pinned
developer tools, and GitHub Actions CI fed by the GitCode mirror. Production
Telegram ingestion/delivery, cloud queue wiring, subscription-backed harness
execution, and the end-to-end product flow remain later implementation slices.

## Components

- `control-api`: dependency-light HTTP entrypoint with health and build metadata;
- `reconciler`: placeholder for durable frontend-update reconciliation and run scheduling;
- `telegram-sender`: first frontend adapter boundary for ordered Telegram delivery;
- `telegram-fake`: deterministic Telegram Bot API capture/update service for local development;
- `worker-runtime`: separately packaged, harness-neutral worker boundary;
- `internal/domain`: tenant-scoped identities, state machines, quota/usage
  semantics, outboxes, artifacts, and explicit context epochs;
- `internal/ports`: YDB/queue/blob/frontend/credential/harness-neutral runtime
  interfaces;
- `internal/ydbstore`: serializable tenant-scoped YDB state and atomic
  ingress/lease/quota/outbox procedures;
- `internal/ydbmigrate`: embedded Goose migrations with a YDB lease and
  checksum drift protection;
- `internal/idgen`: cryptographically random, non-time-sortable operational IDs;
- `internal/ydbpartition`: versioned bucket derivation, physical table policy,
  backfill, and live partition inspection;
- `internal/s3store`: tenant-enforcing S3-compatible blob adapter for MinIO and
  Yandex Object Storage;
- `internal/sqsqueue`: at-least-once SQS-compatible queue adapter for ElasticMQ
  and YMQ;
- `internal/queuecontract`: versioned queue envelopes containing opaque IDs only.

The remaining component placeholders are explicit and exit after emitting a
readiness event. They do not claim that the product flow or a concrete agent
harness already exists.

The contract invariants and transition tables are documented in
[docs/contracts.md](docs/contracts.md). The architecture source of truth is
[design issue #1](https://gitcode.com/urandon/sessionless/issues/1), and delivery
order is maintained in
[implementation epic #6](https://gitcode.com/urandon/sessionless/issues/6).

## Commands

```sh
make tools
make generate
make test
make build
make dev-up
make dev-seed
make migrate-local
make migration-status
make partition-status
make partition-backfill
make integration
make ydb-integration
make local-integration
make dev-down
```

See [docs/development.md](docs/development.md) for prerequisites, secret
injection, image builds, the guarded reset procedure, and exact local behavior.
YDB keys, TTL, and atomic procedures are documented in
[docs/ydb-state-store.md](docs/ydb-state-store.md).
The primary-key distribution, bucketed ready/expiry layout, migration procedure,
and cloud measurement gate are documented in
[docs/ydb-partitioning.md](docs/ydb-partitioning.md).
The local topology, ports, persistence semantics, adapter configuration, and
reset procedure are documented in
[docs/local-development-stand.md](docs/local-development-stand.md).
Contribution boundaries are in [CONTRIBUTING.md](CONTRIBUTING.md).
