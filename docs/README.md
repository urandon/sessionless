# Sessionless documentation

This index routes each reader to the canonical document for a goal. The root
[README](../README.md) is the product overview; this page owns navigation. Avoid
copying commands or contracts between pages: update the canonical owner below
and link to it.

## Start here

- [Product overview and delivery status](../README.md)
- [Deterministic local product flow](local-e2e.md)
- [Local development stand](local-development-stand.md)
- [Component and command map](components.md)
- [Domain and runtime contracts](contracts.md)

## Canonical document owners

| Question | Canonical owner |
| --- | --- |
| What is shipped, experimental, or planned? | [Root README](../README.md) and the linked delivery epics |
| Which component or command do I need? | [Component and command map](components.md) |
| How do I install tools and run repository gates? | [Development](development.md) |
| How does the local runtime start, persist, and recover? | [Local development stand](local-development-stand.md) |
| What does the black-box product proof cover? | [Local E2E](local-e2e.md) |
| What is normative architecture? | [Domain and runtime contracts](contracts.md) |
| What is exploratory rather than committed? | [Research and design index](research/README.md) |

## Users and evaluators

- [Deterministic local end-to-end slice](local-e2e.md)
- [Telegram frontend adapter](telegram.md)
- [Frontend-neutral session API](session-api.md)
- [Session lifecycle and destructive retention](session-lifecycle.md)

## Operators

- [Local development stand](local-development-stand.md)
- [Cloud development environment](cloud-development.md)
- [Web BFF and Telegram OIDC](web-bff.md)
- [YDB state store](ydb-state-store.md)
- [YDB physical partitioning](ydb-partitioning.md)
- [Yandex scale-to-zero substrate evidence](yandex-serverless-substrate.md)
- [Container Registry cleanup](registry-gc.md)
- [Releases](releases.md)

## Contributors

- [Contribution workflow](../CONTRIBUTING.md)
- [Development prerequisites and checks](development.md)
- [Go testing best practices](testing-best-practices.md)
- [Optional local RepoWise workflow](repowise-local.md)
- [README and documentation information architecture](readme-information-architecture.md)
- [YDB migration operations](../migrations/ydb/README.md)
- [Terraform infrastructure](../infra/terraform/README.md)

## Architecture

- [Domain and runtime contracts](contracts.md)
- [Canonical frontend ingress](canonical-ingress.md)
- [Frontend-neutral session API](session-api.md)
- [Credential lifecycle](credential-lifecycle.md)
- [Session lifecycle](session-lifecycle.md)
- [YDB state store](ydb-state-store.md)
- [YDB physical partitioning](ydb-partitioning.md)

## Security and trust boundaries

- [Web authentication contracts](web-auth-contracts.md)
- [WebUI authentication threat model](web-threat-model.md)
- [Credential lifecycle](credential-lifecycle.md)
- [Serverless process isolation](serverless-isolation.md)
- [Serverless provider egress](serverless-egress.md)
- [Attached-worker identity and enrollment](attached-worker-identity.md)
- [Attached-worker execution authority](attached-worker-execution.md)

## Integration surfaces

### Frontends

- [Telegram adapter](telegram.md)
- [Web BFF](web-bff.md)
- [Web authentication contracts](web-auth-contracts.md)
- [Canonical ingress](canonical-ingress.md)

### Harnesses and providers

- [Sessionless-owned serverless harness](serverless-harness.md)
- [Serverless isolation](serverless-isolation.md)
- [Serverless egress](serverless-egress.md)
- [Codex integration surface](codex-integration-surface.md)
- [Codex execution-surface measurement](codex-surface-measurement.md)
- [Go-supervised Codex exec backend](codex-exec-adapter.md)
- [Codex subscription worker evidence](codex-subscription-worker.md)

### Attached workers

- [Identity and enrollment](attached-worker-identity.md)
- [Outbound transport](attached-worker-transport.md)
- [Execution authority](attached-worker-execution.md)
- [Daemon and supervisor](attached-worker-daemon.md)
- [OCI isolation profile](attached-worker-oci.md)
- [Local state and lifecycle CLI](attached-worker-local-state.md)
- [Observability and control UX](attached-worker-ux.md)

## Research and design

The [research index](research/README.md) separates evidence and open decisions
from committed runtime contracts. It covers:

- [AI resources, routing, and federation](research/ai-resources-and-federation.md)
- [Attachable workers](research/attachable-workers.md)
- [Harness-neutral evaluation](research/harness-neutral-evaluation.md)
- [Memory and permissions](research/memory-and-permissions.md)
- [Metering and resource attribution](research/metering-and-resource-attribution.md)
- [Native macOS YDB](research/native-macos-ydb.md)
- [OpenRouter provider architecture](research/openrouter-provider-architecture.md)
- [Platform administration](research/platform-admin-console.md)
- [RepoWise evaluation](research/repowise-sessionless-evaluation.md)
- [Skills and automation](research/skills-and-automation.md)
- [Built-in tools, MCP, and permissions](research/tooling-mcp-and-permissions.md)
- [User usage analytics](research/user-usage-analytics.md)
