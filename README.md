# sessionless

Sessionless is a serverless, cloud-hosted control plane for routing
conversation-backed work to isolated AI-agent workers.

Sessionless owns the canonical conversation model: an append-only, strictly
ordered `SessionEvent` stream with immutable snapshots as optional context
materializations. Telegram is the first frontend adapter, and WebUI is the next;
both bind external conversations to canonical sessions without redefining runs,
scheduling, quota accounting, or worker isolation. A `/new` action creates a
new session and switches the frontend binding atomically. Existing sessions and
their history remain intact.

The control plane is also harness-neutral. Codex, OpenCode, Claude, and
Hermes-style runtimes are candidates for isolated worker adapters, not
dependencies of the domain model. No harness is selected as the permanent
runtime yet.

The target deployment is a scale-to-zero Yandex Cloud serverless environment:
Go control-plane services, YDB operational state, at-least-once queues, and
tenant-partitioned Object Storage. A stateless Cloudflare Worker is the narrow
Telegram reachability edge after live tests showed Telegram timing out against
Yandex public endpoints; accepted updates are immediately handed to durable
operator-managed Yandex Workflows. Harness processes execute outside the control plane in
isolated workers with explicitly granted credentials, blobs, and MCP access.

The repository currently contains the Go component boundaries, harness-neutral
domain/runtime contracts, the authoritative YDB state store and migrations,
authenticated Telegram webhook ingestion, durable Telegram delivery, canonical
session domain and port contracts, membership-gated frontend-neutral user-event
ingestion with a deterministic synthetic adapter, bounded
subscription-aware admission and dispatch, a reproducible local development
stand, isolated worker packaging, pinned developer tools, subscription state
commands, and GitHub Actions CI fed by the GitCode mirror. A green mirrored
`main` build can publish immutable deployment-image digests directly to Yandex
Container Registry through short-lived OIDC federation, keeping developer
laptops out of the normal image-build path. It also contains the complete isolated worker lifecycle with a
credential-free deterministic harness: durable job materialization, fenced
lease renewal, bounded scratch space, checkpoint/resume, usage events,
content-addressed artifacts, cancellation/timeout handling, and atomic terminal
delivery. A credential-free two-tenant black-box suite now composes the full
local Telegram-to-worker-to-Telegram path and its recovery cases. Provider
authorization, full multi-frontend projection, and subscription-backed Codex,
OpenCode, Claude, or Hermes adapters remain later implementation slices.
Canonical sessions, ordered events, session participants, snapshots, activity indexes,
and revisioned frontend bindings are persisted directly in YDB. The repository
also implements the Go Web BFF for Telegram OIDC Authorization Code + PKCE,
explicit tenant enrollment, membership authorization, revocable first-party
sessions, CSRF protection, active-tenant session rotation, canonical Web
message/run ingestion, and exact-object upload/download capabilities. The
browser UI remains a separate slice.

## Components

- `control-api`: HTTP entrypoint with health/build metadata and the authenticated
  Telegram webhook adapter;
- `web-bff`: same-origin Go backend-for-frontend for Telegram OIDC, membership-
  authorized tenant selection, revocable opaque browser sessions, canonical
  message/run operations, and exact-object upload/download capabilities;
- `oidc-fake`: deterministic Telegram-shaped OIDC fixture that is hard-disabled
  outside the local environment;
- `reconciler`: queue-driven point scheduler with bounded 16-bucket dispatch
  and quota-expiry recovery;
- `telegram-sender`: queue-driven, durable, retrying Telegram delivery outbox
  consumer with bounded recovery;
- `telegram-fake`: deterministic Telegram Bot API capture/update service for local development;
- `worker-runtime`: concurrency-one isolated worker; local mode consumes one
  message, while cloud mode handles one trigger-delivered batch per request;
- `deployment-lock`: operator-only fenced YDB lease wrapper for Terraform
  plan/apply serialization;
- `internal/worker`: durable materialize/execute/checkpoint/finalize lifecycle;
- `internal/deterministicharness`: credential-free adapter that proves the
  worker contract before a subscription CLI is selected;
- `internal/serverlessharness`, `internal/serverlessisolation`, and
  `internal/serverlessegress`: feature-disabled managed-substrate authority,
  process isolation, and exact attested provider/credential boundaries;
- `internal/domain`: canonical sessions/events/bindings/snapshots, tenant-scoped
  identities, state machines, quota/usage semantics, outboxes, and artifacts;
- `internal/ports`: YDB/queue/blob/frontend/credential/harness-neutral runtime
  interfaces;
- `internal/sessioningress`: frontend-neutral session resolution, clean-context,
  canonical object-envelope, and atomic event/run ingestion application service;
- `internal/sessionapi`: participant-authorized session metadata, fixed-fan-out
  listings, scoped pagination, history/run reads, lifecycle, and rebinding operations;
- `internal/syntheticfrontend`: deterministic non-Telegram adapter proving the
  canonical ingress boundary without a transport SDK;
- `internal/webcontract`: same-origin WebUI request/response, secure-cookie,
  CSRF, and tenant-selector contracts without browser-side tenant authority;
- `internal/webbff`: authorization-code callback, first-party session, CSRF,
  logout, identity, membership, active-tenant, canonical message/run, and
  object-capability HTTP flows;
- `internal/webapi`: server-owned Web binding, compute resolution, canonical
  message ingress, upload intent/commit/promotion, and attachment capability
  application boundary;
- `internal/telegramoidc`: Telegram OIDC Authorization Code + PKCE client with
  pinned RS256 verification and bounded JWKS refresh;
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
  commands, transitional binding compatibility, and idempotent run creation;
- `internal/telegramdelivery`: bounded YDB-ready traversal, transactional delivery
  claims, retry policy, and Telegram Bot API sending;
- `internal/queuecontract`: versioned queue envelopes containing opaque IDs only.
- `internal/serverlesshttp` and `internal/yandextriggers`: bounded HTTP
  invocation and normalized Yandex trigger-event adapters.

The worker image deliberately contains only the deterministic adapter today.
This proves orchestration semantics without implying that a permanent harness
or subscription credential protocol has already been selected.

The contract invariants and transition tables are documented in
[docs/contracts.md](docs/contracts.md). Canonical frontend ingestion and its
object/transaction failure boundary are documented in
[docs/canonical-ingress.md](docs/canonical-ingress.md). Web authentication and its focused
threat model are documented in
[docs/web-auth-contracts.md](docs/web-auth-contracts.md) and
[docs/web-threat-model.md](docs/web-threat-model.md). The architecture source of truth is
[design issue #1](https://gitcode.com/urandon/sessionless/issues/1), and delivery
order is maintained in
[implementation epic #6](https://gitcode.com/urandon/sessionless/issues/6).
Session archive, storage tiering, legal hold, and the separately confirmed
single-session deletion procedure are documented in
[docs/session-lifecycle.md](docs/session-lifecycle.md).
The participant-authorized session listing/history, scoped pagination,
frontend rebinding, bounded display metadata, and admin-safe metadata contracts
are documented in [docs/session-api.md](docs/session-api.md).

## Commands

```sh
make tools
make web-ci
make web-browser-install
make web-browser-test
make generate
make test
make build
make dev-up
make dev-seed
make migrate-local
make migration-status
make partition-status
make partition-backfill
make cloud-app-reset-plan
make cloud-app-reset
make session-delete-request
make session-delete-plan
make session-delete
make session-hold
make session-release-hold
make web-bootstrap
make worker-once
make integration
make ydb-integration
make local-integration
make e2e-local
make terraform-ci
make cloudflare-edge-ci
make dev-down
```

See [docs/development.md](docs/development.md) for prerequisites, secret
injection, image builds, the guarded reset procedure, and exact local behavior.
The Web BFF, OIDC settings, YDB access paths, enrollment boundary, and audited
cloud-dev bootstrap are documented in [docs/web-bff.md](docs/web-bff.md).
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
