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
`make build` writes eight binaries to `.build/bin`: `control-api`,
`reconciler`, `telegram-sender`, `telegram-fake`, `worker-runtime`, and
`schema-migrate`, plus the operator-only `schema-inspect` and
`schema-backfill` tools.

## Local stack

Start and initialize the complete local stand:

```sh
make dev-up
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8081/healthz
```

`make dev-up` starts pinned YDB Local, MinIO, ElasticMQ, the deterministic
Telegram fake, the control API, and the durable Telegram sender. It waits for
every public endpoint, creates the local bucket, applies the embedded YDB
migrations, and idempotently loads the synthetic Telegram fixture. It does not
require cloud credentials or a real Telegram token.

The control API uses the YDB SDK single-connection balancer only inside the
Compose stand. This keeps the client on the Docker-resolvable `ydb-local`
endpoint instead of replacing it with YDB Local's host-facing discovery
address. Cloud deployments retain normal endpoint discovery and balancing.

Run the adapter contracts and stop the stack:

```sh
make migrate-local
make local-integration
make dev-down
```

Normal stop/start preserves the YDB and Object Storage named volumes. ElasticMQ
and the Telegram fake are intentionally ephemeral transport fixtures. The
complete topology, endpoint table, local-only credentials, persistence test,
and Apple Silicon requirements are documented in
[local-development-stand.md](local-development-stand.md).

After the YDB monitoring endpoint is ready, apply or inspect the schema:

```sh
export YDB_CONNECTION_STRING='grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric'
export YDB_ANONYMOUS_CREDENTIALS=1
make migrate-local
make migration-status
make partition-status
make ydb-integration
```

The repository-owned migration binary embeds the SQL set and uses Goose as a
library. It adds a YDB-backed fenced lock, pre-execution checksums, one
idempotent DDL operation per file, and a forward-only production policy. See
`migrations/ydb/README.md` for crash repair and `docs/ydb-state-store.md` for
keys and transaction procedures.

`make partition-status` emits the live primary keys, partition settings, counts,
and contract drift as JSON. The bucketed ready/expiry expand/backfill/cutover
procedure is documented in
[ydb-partitioning.md](ydb-partitioning.md). `make partition-backfill` is a
deployment migration command, not a normal serving operation.

Local defaults use `YDB_ANONYMOUS_CREDENTIALS=1`. Cloud deployments use the
YDB environment credential chain and metadata credentials; do not place access
tokens in the connection string or command line.

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

GitCode is the source of truth for branches and merge requests. Its push mirror
replicates every commit to `github.com/urandon/sessionless`, where GitHub Actions
runs `make ci` and `make images` for every mirrored branch or tag push. The
workflow is `.github/workflows/ci.yml`; a GitHub pull request is not required.

When reviewing a GitCode merge request, match the GitHub Actions run to the
GitCode head commit SHA. Automatic propagation of that status back into the
GitCode merge-request UI is a separate integration; until it exists, this SHA
check is the merge gate.

Publishing GitHub release artifacts back into GitCode is intentionally outside
this CI workflow and tracked separately in
[issue #15](https://gitcode.com/urandon/sessionless/issues/15). Branch CI does
not receive a GitCode publication token.
