# Development

## Prerequisites

All supported versions are declared in `tools/versions.env`. Run:

```sh
make tools
```

The check is exact and fails with an actionable list when a tool is absent or
has drifted. The current foundation expects Go, Docker Compose, Terraform,
Yandex Cloud CLI (`yc`), YDB CLI, and Goose. Cloud tools are validated here even
though the first local process only needs Go and Docker.

| Tool | Pinned version | Installation source |
| --- | ---: | --- |
| Go | 1.26.4 | [go.dev/dl](https://go.dev/dl/) |
| Docker Compose | 5.3.1 | [Docker Compose install](https://docs.docker.com/compose/install/) |
| Terraform | 1.15.5 | [HashiCorp releases](https://releases.hashicorp.com/terraform/1.15.5/) |
| Yandex Cloud CLI | 1.20.0 | [Yandex Cloud CLI install](https://yandex.cloud/en/docs/cli/operations/install-cli) |
| YDB CLI | 2.31.0 | [YDB CLI downloads](https://ydb.tech/docs/en/downloads/ydb-cli) |
| Goose | 3.27.1 | [Goose releases](https://github.com/pressly/goose/releases/tag/v3.27.1) |

For Goose, the reproducible Go installation command is:

```sh
go install github.com/pressly/goose/v3/cmd/goose@v3.27.1
```

The vendor installers for `yc` and YDB may install a newer release. Run
`make tools` afterward; update `tools/versions.env` in a reviewed change instead
of silently using mixed versions.

Do not use a checked-in file for credentials. `.env.example` contains safe
defaults and an empty token slot only. On macOS, a developer can place a token
in Keychain once:

```sh
security add-generic-password -a "$USER" -s sessionless.telegram-bot-token -w
```

Inject it into the process environment for the current shell:

```sh
export TELEGRAM_BOT_TOKEN="$(security find-generic-password -a "$USER" -s sessionless.telegram-bot-token -w)"
```

On other systems, use the OS credential store or a password manager that can
export into the child process environment. Never write service-account JSON or
subscription credentials into this repository.

## Fast path

```sh
make generate
make test
make build
make integration
```

`make test` checks formatting, runs `go vet`, unit tests, and the race detector.
`make build` writes four binaries to `.build/bin`: `control-api`,
`reconciler`, `telegram-sender`, and `worker-codex`.

## Local stack

Start the control API:

```sh
make dev-up
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/version
```

The initial Compose stack deliberately starts only the foundation control API.
YDB, fake Telegram, Object Storage, and queue emulators belong to their
respective implementation issues and will be added without changing these
top-level commands.

Apply migrations and stop the stack:

```sh
make migrate-local
make dev-down
```

At foundation stage there are no schema files, so `make migrate-local` reports
that fact and exits successfully. Once migrations exist, it fails closed until
the YDB migration ledger and runner are present.

To delete local Compose volumes, use the guarded command:

```sh
CONFIRM_LOCAL_RESET=sessionless-dev make dev-reset
```

The reset target affects only the fixed `sessionless-dev` Compose project. It
does not remove source directories or arbitrary Docker resources.

## Images and CI

```sh
make images
make ci
```

The control plane uses a small distroless runtime. The worker has a separate
Dockerfile so a later harness decision can add OpenCode, Codex, or another CLI
without expanding the webhook/control-plane attack surface.

GitCode CI runs `make ci` and `make images` on pushes and pull requests to
`main`. The workflow is `.gitcode/workflows/ci.yml`.
