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
domain/runtime contracts, the authoritative YDB state store and migrations,
authenticated Telegram webhook ingestion, durable Telegram delivery, bounded
subscription-aware admission and dispatch, a reproducible local development
stand, isolated worker packaging, pinned developer tools, subscription state
commands, explicit clean-context epochs, and GitHub Actions CI fed by the
GitCode mirror. It also contains the complete isolated worker lifecycle with a
credential-free deterministic harness: durable job materialization, fenced
lease renewal, bounded scratch space, checkpoint/resume, usage events,
content-addressed artifacts, cancellation/timeout handling, and atomic terminal
delivery. A credential-free two-tenant black-box suite now composes the full
local Telegram-to-worker-to-Telegram path and its recovery cases. Provider
authorization and subscription-backed Codex, OpenCode, Claude, or Hermes
adapters remain later implementation slices.

## Components

- `control-api`: HTTP entrypoint with health/build metadata and the authenticated
  Telegram webhook adapter;
- `reconciler`: bounded 16-bucket dispatch publisher and quota-expiry
  reconciler;
- `telegram-sender`: durable, retrying Telegram delivery outbox consumer;
- `telegram-fake`: deterministic Telegram Bot API capture/update service for local development;
- `worker-runtime`: concurrency-one isolated worker; local mode consumes one
  message, while cloud mode handles one trigger-delivered batch per request;
- `deployment-lock`: operator-only fenced YDB lease wrapper for Terraform
  plan/apply serialization;
- `internal/worker`: durable materialize/execute/checkpoint/finalize lifecycle;
- `internal/deterministicharness`: credential-free adapter that proves the
  worker contract before a subscription CLI is selected;
- `internal/domain`: tenant-scoped identities, state machines, quota/usage
  semantics, outboxes, artifacts, and explicit context epochs;
- `internal/ports`: YDB/queue/blob/frontend/credential/harness-neutral runtime
  interfaces;
- `internal/portlog`: process-boundary structured correlation logs without
  payload or credential logging;
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
- `internal/scheduler`: injected-clock admission policy, one-subscription
  reservation enforcement, durable queue publication, and quota expiry;
- `internal/telegramingress`: webhook authentication, opaque deterministic
  identity resolution, normalized input/blob handling, durable subscription
  commands, explicit clean-context transitions, and idempotent run creation;
- `internal/telegramdelivery`: bounded YDB-ready traversal, transactional delivery
  claims, retry policy, and Telegram Bot API sending;
- `internal/queuecontract`: versioned queue envelopes containing opaque IDs only.
- `internal/serverlesshttp` and `internal/yandextriggers`: bounded HTTP
  invocation and normalized Yandex trigger-event adapters.

The worker image deliberately contains only the deterministic adapter today.
This proves orchestration semantics without implying that a permanent harness
or subscription credential protocol has already been selected.

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
make worker-once
make integration
make ydb-integration
make local-integration
make e2e-local
make terraform-ci
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
Telegram webhook, identity, attachment, and delivery procedures are documented
in [docs/telegram.md](docs/telegram.md).
The deterministic two-tenant black-box flow, fault scenarios, timing boundary,
and operator queries are documented in [docs/local-e2e.md](docs/local-e2e.md).
The isolated Yandex Cloud development topology, budget gate, deployment,
canary, rollback, and destroy procedures are documented in
[docs/cloud-development.md](docs/cloud-development.md).
Contribution boundaries are in [CONTRIBUTING.md](CONTRIBUTING.md).
