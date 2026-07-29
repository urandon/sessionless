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

This repository currently contains the MVP foundation: Go component boundaries,
an executable control API skeleton, harness-neutral domain/runtime contracts,
isolated worker packaging, pinned developer tools, local Compose commands, and
GitHub Actions CI fed by the GitCode mirror. Telegram ingestion, YDB adapters,
cloud queues, subscription-backed harness execution, and end-to-end delivery
are not implemented yet.

## Components

- `control-api`: dependency-light HTTP entrypoint with health and build metadata;
- `reconciler`: placeholder for durable frontend-update reconciliation and run scheduling;
- `telegram-sender`: first frontend adapter boundary for ordered Telegram delivery;
- `worker-codex`: current separately packaged worker-adapter skeleton; its name
  does not commit the core architecture to Codex;
- `internal/domain`: tenant-scoped identities, state machines, quota/usage
  semantics, outboxes, artifacts, and explicit context epochs;
- `internal/ports`: YDB/queue/blob/frontend/credential/harness-neutral runtime
  interfaces;
- `internal/queuecontract`: versioned queue envelopes containing opaque IDs only.

The placeholders are explicit and exit after emitting a readiness event. They
do not claim that the cloud adapters or product flow already exist.

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
make migrate-local
make integration
make dev-down
```

See [docs/development.md](docs/development.md) for prerequisites, secret
injection, image builds, the guarded reset procedure, and exact local behavior.
Contribution boundaries are in [CONTRIBUTING.md](CONTRIBUTING.md).
