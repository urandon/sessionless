# Component and command map

This is the repository inventory behind the product overview. Architecture
invariants belong in [contracts.md](contracts.md); operational procedures belong
in their runbooks; exact command behavior belongs in the `Makefile` and the
scripts it invokes.

## Runtime entrypoints

| Component | Responsibility |
| --- | --- |
| `control-api` | Health/build metadata and authenticated Telegram webhook ingress. |
| `web-bff` | Same-origin Telegram OIDC, first-party browser sessions, tenant selection, canonical session/message/run APIs, and exact-object capabilities. |
| `reconciler` | Queue-driven admission, quota reservation, bounded dispatch, and expiry recovery. |
| `telegram-sender` | Durable, retrying Telegram delivery-outbox consumer. |
| `worker-runtime` | Concurrency-one isolated worker; one local queue item or one cloud trigger batch per invocation. |
| `schema-migrate`, `schema-inspect`, `schema-backfill` | Embedded YDB migration, drift inspection, and bounded partition backfill. |
| `deployment-lock` | Operator-only fenced YDB lease around Terraform operations. |
| `registry-gc`, `release-notes`, `github-release` | Guarded image cleanup and deterministic release tooling. |
| `web-bootstrap`, `session-delete` | Audited membership bootstrap and exact-session lifecycle operations. |
| `telegram-fake`, `oidc-fake` | Deterministic local-only Telegram and OIDC fixtures; both are hard-disabled outside local use. |

## Core packages

| Package | Boundary |
| --- | --- |
| `internal/domain` | Canonical sessions/events/bindings/snapshots, identities, state machines, quota/usage, outboxes, and artifacts. |
| `internal/ports` | YDB, queue, blob, frontend, credential, and harness-neutral interfaces. |
| `internal/sessioningress` | Frontend-neutral resolution, context control, immutable payloads, and atomic event/run ingestion. |
| `internal/sessionapi` | Participant-authorized listings, reads, lifecycle, and rebinding. |
| `internal/webcontract`, `internal/webbff`, `internal/webapi` | Browser-facing contract, authorization boundary, and canonical Web operations. |
| `internal/telegramingress`, `internal/telegramdelivery`, `internal/telegramoidc` | Telegram ingress, projection delivery, and bounded OIDC verification. |
| `internal/worker` | Durable materialize/execute/checkpoint/finalize lifecycle. |
| `internal/deterministicharness` | Credential-free reference adapter for orchestration proofs. |
| `internal/serverlessharness`, `internal/serverlessisolation`, `internal/serverlessegress` | Feature-disabled managed-harness authority, process isolation, and attested provider/credential boundaries. |
| `internal/directopenrouter` | Feature-disabled native [direct OpenRouter](direct-openrouter.md) request/response and observed-route reference backend. |
| `internal/providercomposition` | Closed, feature-disabled [native provider composition](provider-composition.md) for exact Codex, OpenCode, Pi, and direct registrations. |
| `internal/attachedworkersession` | Feature-disabled [worker-side connection owner](attached-worker-connection-session.md) for bootstrap, durable generation fencing, and immediate AW-02 exchange. |
| `internal/ydbstore`, `internal/ydbmigrate`, `internal/ydbpartition` | Tenant-scoped state, migration fencing, and physical partition policy. |
| `internal/s3store`, `internal/sqsqueue`, `internal/queuecontract` | Tenant-enforcing objects and payload-free at-least-once queue contracts. |
| `internal/scheduler` | Injected-clock admission, reservation, publication, and expiry rules. |
| `internal/syntheticfrontend` | Deterministic non-Telegram proof of the canonical ingress boundary. |
| `internal/serverlesshttp`, `internal/yandextriggers` | Bounded HTTP invocation and normalized Yandex trigger adapters. |
| `internal/portlog`, `internal/idgen` | Payload-safe correlation logs and random non-time-sortable operational IDs. |

## Common commands

| Goal | Command | Canonical documentation |
| --- | --- | --- |
| Validate pinned tools | `make tools` | [Development](development.md) |
| Run the repository CI surface | `make ci` | [Development](development.md) |
| Check and build the WebUI | `make web-ci` | [Development](development.md) |
| Run browser E2E and accessibility gates | `make web-browser-install && make web-browser-test` | [Web BFF](web-bff.md) |
| Start or stop the local stand | `make dev-up` / `make dev-down` | [Local stand](local-development-stand.md) |
| Prove the complete local product path | `make e2e-local` | [Local E2E](local-e2e.md) |
| Consume one admitted local worker job | `make worker-once` | [Local stand](local-development-stand.md) |
| Check provider/harness contracts | `make provider-conformance` | [Serverless harness](serverless-harness.md) |
| Validate cloud infrastructure | `make terraform-ci` | [Cloud development](cloud-development.md) |
| Inspect/apply schema state | `make migration-status` / `make migrate-local` | [YDB migration operations](../migrations/ydb/README.md) |
| Preview the deterministic README visual | `make readme-visual-preview` | [README IA](readme-information-architecture.md#visual-refresh-procedure) |

Run `make help` for the complete generated command summary. Destructive and
credential-bearing commands intentionally require additional typed intent; use
their linked runbooks instead of copying an example from another page.
