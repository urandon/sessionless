# sessionless

Telegram chatbot proxy over isolated OpenCode sessions.

The Telegram chat is the product source of truth: users talk to the bot, send text/images/files, and receive AI processing results back in the same chat without needing to understand sessions. A new clean context should be exposed only as an explicit user command.

This repository currently contains the MVP foundation: Go component boundaries,
an executable control API skeleton, isolated worker packaging, pinned developer
tools, local Compose commands, and GitCode CI.

## Components

- `control-api`: dependency-light HTTP entrypoint with health and build metadata;
- `reconciler`: placeholder for durable Telegram-update reconciliation;
- `telegram-sender`: placeholder for ordered Telegram delivery;
- `worker-codex`: separately packaged AI harness worker boundary.

The placeholders are explicit and exit after emitting a readiness event. They
do not claim that Telegram, YDB, queues, Object Storage, quotas, or subscription
harness integration already exists.

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
