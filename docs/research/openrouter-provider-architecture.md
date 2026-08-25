# OpenRouter provider architecture and Ox Alpha canary

Research date: 2026-08-25

Tracks: [#51](https://gitcode.com/urandon/sessionless/issues/51), proposed
PR-05 in [AI resources and federation](ai-resources-and-federation.md)

Status: implementation-ready design; no credential accepted and no provider
request made

## Decision

Sessionless owns the **harness boundary, routing and supervision**, not one
mandatory agent implementation. The minimum product routes an admitted attempt
to an exact, versioned harness adapter such as deterministic, Codex, OpenCode or
Pi. Their native sessions, tools, permissions, retries and transcripts never
become canonical Sessionless state.

OpenRouter is an orthogonal model-transport and billing resource. It may be
bound to any harness adapter that passes the required provider-policy
conformance. The same OpenRouter key/model is not automatically interchangeable
across harnesses because each harness has different request, tool, retry,
catalog and evidence behavior.

The priority OpenRouter paths are:

1. **Pi + OpenRouter**: strongest initial canary candidate. Pi has a headless
   JSON RPC mode, a no-session option, built-in OpenRouter support, and an
   `openRouterRouting` configuration that is sent as the request `provider`
   object. It can therefore express exact no-fallback route policy.
2. **OpenCode + OpenRouter**: one-shot JSON events or a bounded local server,
   with explicit per-model OpenRouter `provider` options including
   `allow_fallbacks:false`. It needs stricter suppression of stored credentials,
   config discovery, plugins and persistent sessions.
3. **Codex + OpenRouter**: official custom Responses provider support and the
   already researched `codex exec` lifecycle, but Codex configuration cannot
   carry OpenRouter's per-request `provider` body. This path is eligible only
   for policies satisfied by server-side key/workspace guardrails and available
   evidence.
4. **Sessionless minimal agent + direct OpenRouter API**: a later reference and
   generic provider-backed harness. It gives the strongest request/response
   control but requires Sessionless to own a complete minimal agent/tool loop;
   it is not a prerequisite for the CLI-backed MVP.

These are distinct backend implementations behind one Sessionless-owned
`SessionlessHarnessV1`, which implements the canonical `HarnessDriver`
contract. There is no mid-attempt or implicit fallback among backends. A
scheduler decision seals backend kind/version/artifact, provider resource,
credential generation, model, route policy, placement and capability/policy
digests before execution.

"OpenAI-compatible" describes a wire family only. It does not make an arbitrary
endpoint, capability catalog, error, privacy policy, retry, billing behavior or
harness interchangeable with OpenAI, OpenRouter, Codex, OpenCode or Pi.

## Existing Sessionless contract

The RAG inventory found that this work is already owned by #51 rather than a new
independent provider architecture:

- #1 and the committed contracts make the Go `HarnessDriver` the durable
  harness-neutral product boundary. Concrete harness processes are isolated
  worker adapters; the control plane stays independent of Codex, OpenCode,
  Hermes, Pi or another implementation;
- [AI resources and federation](ai-resources-and-federation.md) already defines
  `router_account`, separates model vendor, transport, billing resource,
  harness, placement, and credential generation, and proposes PR-05;
- #48 owns subscription-backed OpenAI policy. An OpenRouter API key is a
  separate API/router resource and must never be a fallback for a selected
  ChatGPT subscription;
- #81 owns the subscription-backed Go-supervised `codex exec` adapter. A
  Codex/OpenRouter binding is a separate harness-resource profile and must not
  modify or fall back from that subscription route;
- [credential lifecycle](../credential-lifecycle.md) already provides the
  invocation-scoped handle, generation fence, exact `auth.json`
  materialization, write-back, release, and revoke contract, but currently has
  no production YDB/Lockbox credential backend;
- canonical `Session`, `Run`, `Attempt`, `Lease`, fence, terminal commit, and
  usage evidence remain unchanged. Provider output cannot become a second
  attempt state machine.

### Compile-visible routing gap

The current `WorkerJob` and `ExecutionRequest` do not carry a harness/resource
binding, and `worker.Manager` receives one process-wide `HarnessDriver`. That is
correct for the deterministic bootstrap but cannot implement the required
Codex/OpenCode/Pi portfolio.

PR-01 must add one immutable, versioned `HarnessBindingV1` to the canonical
admission/outbox/job path and project it into `ExecutionRequest`. The queue
remains only tenant/run routing identity. A Go routing driver resolves the
already admitted binding to one registered adapter; it never selects from
environment variables, installed binaries, available keys or model catalogs at
execution time. The adapter must exact-match its registered artifact/protocol
digest and provider-resource generation before it can read the credential or
start a process.

Existing jobs need an explicit deployment migration/drain gate. There is no
runtime zero-value alias to the deterministic or Codex harness, because that
would silently change billing and behavior.

## Layering

| Layer | Authority | OpenRouter responsibility |
|---|---|---|
| Session/run/attempt | Sessionless domain and fenced stores | none |
| admission and routing | canonical resource revision, grants, policy and scheduler | observed catalog/health/quota are inputs, never the admission decision |
| harness contract | Go `HarnessDriver`, supervisor, canonical context/events/terminal and adapter registry | none; OpenRouter does not own history or tool execution |
| harness adapter | exact deterministic/Codex/OpenCode/Pi/direct implementation and its private invocation state | adapter translates the sealed route without gaining scheduling authority |
| optional model-provider port | one pinned, bounded model invocation used by a Sessionless-owned agent loop | direct adapter implements exact OpenRouter request, stream, response and observations |
| credential lifecycle | owner-scoped resource binding and secret generation | consume one invocation materialization; never persist or refresh the key itself |
| upstream routing | exact sealed OpenRouter route policy | execute only that policy and report the actual route |

### Harness selection is independent from provider selection

The admitted tuple is conceptually:

```text
HarnessBindingV1 {
  harness_kind, harness_version, executable_or_artifact_digest, protocol_version
  provider_resource_id, provider_resource_revision, credential_generation
  model_id, catalog_digest, route_policy_digest, data_policy_digest
  placement_id, capability_manifest_digest, effective_policy_digest
}
```

The adapter registry admits only reviewed combinations. For example, a Pi
version may support an exact OpenRouter route while the same model through Codex
is ineligible because Codex cannot surface the required route evidence. A model
catalog observation never adds a compatible harness/provider pair.

`HarnessDriver` receives canonical context and emits canonical bounded events;
the adapter may internally translate them to RPC, JSONL, a local OpenAPI server,
or direct model turns. Native session IDs are invocation diagnostics only.
Harness-native tools may run only inside the admitted isolation boundary and
explicit tool/effect policy; ambient native tools, plugins, skills, MCP servers,
config discovery and remembered approvals are disabled unless independently
admitted and mapped to canonical evidence.

The concrete MVP shape is a Sessionless-owned routing harness that itself
implements `HarnessDriver` and delegates to a closed registry of backends:

```text
SessionlessHarnessV1.Execute(canonical request)
  -> validate admitted HarnessBindingV1 and credential generation
  -> resolve exact HarnessBackendV1 by kind/version/artifact/protocol digest
  -> prepare isolated invocation and adapter-specific config
  -> supervise one backend process or direct loop
  -> normalize bounded events/tool evidence/usage/terminal
  -> cleanup boundary and credential materialization
```

This outer harness owns all cross-backend semantics. `CodexBackendV1`,
`OpenCodeBackendV1`, `PiBackendV1` and a future `DirectBackendV1` own only
translation to one native surface. They cannot call each other or make a routing
decision.

### Why Codex CLI is not the generic provider port

The official Codex configuration supports custom providers with `base_url`, an
environment-backed key, static/environment headers, query parameters and the
Responses wire API. It also exposes HTTP and stream retry counts. It does **not**
provide a configuration field for OpenRouter's per-request `provider` body,
including `allow_fallbacks:false`, `require_parameters:true`, exact endpoint
selection, maximum price, or per-request data policy. OpenRouter presets can
carry those values, but API calls always resolve the mutable designated/latest
version; a preset slug is not an immutable policy reference.

OpenRouter key/workspace guardrails can enforce model and provider allowlists,
budgets and some privacy rules. They make a constrained CLI canary defensible,
but they do not expose all request policy or the exact router decision through
Codex's public event contract. Therefore:

- the Codex backend is admitted under its own harness-backend kind and resource
  revision, never as `ModelProviderV1`;
- a dedicated key must allow exactly the admitted model and provider and use
  the strictest applicable privacy/budget settings;
- OpenRouter routing metadata is required for direct calls. If Codex does not
  return equivalent bounded route evidence, the Codex result records route
  evidence as unavailable and is ineligible for policies requiring it;
- an adapter that demonstrably sends the exact request body and reports the
  route is mandatory for per-request no-fallback, exact price, ZDR/data-policy,
  endpoint or route-evidence enforcement. Pi, OpenCode and the direct adapter
  may qualify through conformance; the current Codex surface cannot.

The existing `HarnessDriver` remains the agent-level port. A later
Sessionless-owned minimal agent can depend on a narrower provider port
conceptually shaped as follows; this is not the only MVP harness and the exact
Go package is PR-01/PR-02 scope:

```text
ModelInvocationV1 {
  resource_id, resource_revision, credential_generation
  tenant_id, owner_id, run_id, attempt_id, lease_id, fence
  catalog_digest, provider_policy_digest, data_policy_digest
  model_id, route_policy, required_capabilities
  bounded canonical messages, tools, output contract
}

ModelProviderV1.Invoke(ctx, invocation, credential, sink)
  -> ModelTerminalV1 | sanitized failure

ModelTerminalV1 {
  provider_request_id?, generation_id?, exact model_id
  actual_provider?, finish_class, bounded output/tool calls
  provider usage/cost observations with provenance
  acceptance_class, terminal_digest
}
```

The provider adapter never executes a tool. It validates and returns a tool
request; the Go harness and permission plane decide whether and how to execute
it, then start the next pinned model invocation. Provider-native thread IDs are
diagnostic only.

## Pi/OpenRouter adapter contract

Pi is the preferred first OpenRouter harness spike, not a new product authority.
Run a digest-pinned Pi artifact as one isolated process in RPC mode with session
persistence disabled. The adapter owns stdin/stdout framing, bounded events,
deadlines, cancellation, process-group and external-boundary cleanup, and exact
translation to `HarnessDriver`.

The invocation gets a private agent/config directory and sanitized environment.
Sessionless generates the complete provider/model configuration. The first
canary uses `--no-tools`, `--no-extensions`, `--no-skills`,
`--no-prompt-templates`, `--no-themes`, `--no-context-files` and `--no-session`;
it does not load `~/.pi`, project `.pi`, packages, remembered sessions, ambient
provider keys or automatic model selection. Configuration
pins the OpenRouter base URL, exact model/capability limits and
`openRouterRouting` with `only`, `allow_fallbacks:false`,
`require_parameters:true`, privacy and price constraints supported by the
admitted policy. The API key exists only in the child invocation environment.

Pi's RPC and provider stream are still untrusted adapter protocols. Before MVP
selection, fixtures must prove one in-flight request, cancel/terminal behavior,
output/usage bounds, exact route-body serialization, no session residue and no
extension/config discovery. Enabling any native tool is a later capability
profile with isolated effect tests and canonical tool-call/result evidence; the
adapter rejects unknown RPC events.

## OpenCode/OpenRouter adapter contract

OpenCode is a second minimum harness target. Prefer one-shot `run --format json`
for initial conformance; use its headless server only if measurements show a
material lifecycle benefit and the server is bound to a private invocation
boundary with no cross-attempt state.

Sessionless generates one complete `opencode` configuration and isolated data,
cache and config homes. It pins the exact OpenRouter model and provider request
options, including the no-fallback route. Stored `/connect` credentials,
Models.dev mutation, project/user config, plugins, sharing, imported/resumed
sessions, mDNS/CORS and ambient provider discovery are disabled. The adapter
parses bounded JSON events and owns cancellation and descendant cleanup.

OpenCode conformance must independently prove the same canonical harness and
provider-policy matrix as Pi. Similar-looking JSON or use of the same
OpenRouter key does not permit shared replay, checkpoints or native sessions.

## Codex/OpenRouter backend contract

This adapter reuses the reviewed `codex exec` process supervision and event
surface, but has a distinct identity such as `codex_exec_openrouter_v1`. It must
not modify the subscription-backed Codex resource from #81 or act as its API-key
fallback.

Each invocation receives a private, invocation-scoped `CODEX_HOME` and generated
host-owned configuration. It runs non-interactively with user configuration
ignored and rollout persistence disabled. The configuration pins:

- exact Codex binary digest and harness surface;
- `model_provider = "openrouter"`, fixed
  `https://openrouter.ai/api/v1`, `wire_api = "responses"`, and exact non-alias
  model slug;
- `env_key = "OPENROUTER_API_KEY"`, with the variable present only in the
  sanitized child environment from credential materialization;
- `request_max_retries = 0` and `stream_max_retries = 0`; Sessionless, not
  Codex/OpenRouter defaults, owns ambiguous retry decisions;
- bounded context/compaction values and a generated, digested local model
  catalog. The adapter does not run a shell auth command or trust a live catalog
  refresh to change capability authority;
- no notifications, OTEL exporter, ambient MCP/plugin, user memories, browser,
  provider override, proxy, or inherited `~/.codex` state.

The official OpenRouter guide recommends command-backed auth so Codex refreshes
the OpenRouter catalog. Sessionless deliberately does not execute `sh -c echo
$OPENROUTER_API_KEY`: a shell token command adds avoidable process and stdout
secret exposure, while a live catalog is observation rather than authority.
`env_key` plus a validated local catalog is the bounded automation shape.

The adapter validates Codex JSON events exactly as the subscription adapter
does, plus OpenRouter-specific model/resource observations where available. A
missing actual-provider/route observation is not guessed from the request. The
adapter may run Ox Alpha only in the synthetic policy class until the
capability, route and terminal evidence matrix passes.

## Optional direct OpenRouter backend contract

### Fixed surface

- Base URL is the compiled allowlisted `https://openrouter.ai/api/v1`; V1 has no
  user-supplied endpoint or proxy URL.
- Initial operation is a reviewed OpenRouter Responses or Chat Completions
  request using strict bounded JSON and SSE codecs written in Go. PR-02 must
  select one V1 wire skin from behavior fixtures before implementation; V1 does
  not silently switch skins. Responses aligns with Codex and carries the
  provider policy, while Chat Completions is the most widely exercised generic
  compatibility surface. Both remain OpenRouter-specific DTOs rather than a
  generic arbitrary-base-URL client.
- The credential is an OpenRouter inference API key, not a cookie, management
  key, OpenAI key, BYOK provider key, or CLI login.
- Request headers are an exact allowlist: `Authorization`, `Content-Type`,
  `Accept`, optional constant app attribution, and the reviewed routing-metadata
  opt-in. Raw user, tenant, run, prompt, and credential values never enter
  headers, trace metadata, Broadcast, or app attribution. The synthetic canary
  omits `user`; a later multi-user release may send only a policy-approved,
  resource-scoped HMAC pseudonym for upstream abuse isolation, never a product
  identifier, email or reusable cross-resource hash.
- Redirects, non-HTTPS transport, alternate hosts, response compression without
  an expanded-size bound, and cookies are rejected.

### Pinned route

Every attempt seals the exact model slug and OpenRouter policy. V1 forbids model
aliases such as `~latest`, the random `openrouter/free` router, the multi-model
`models` fallback list, and model mutation after admission.

The request sends an explicit provider policy including:

```json
{
  "allow_fallbacks": false,
  "require_parameters": true,
  "only": ["stealth"]
}
```

where the `only` value must first be confirmed against the current endpoints
catalog and pinned into the route snapshot. Paid/non-stealth routes additionally
pin allowed endpoint/provider slugs, maximum price, data-collection class, and
ZDR requirement when supported. Account-wide settings and key guardrails are
defense in depth; the request still carries the exact restrictions.

The response must match the admitted model and expose an allowed actual route.
Any unknown or changed model/provider/region/data-policy observation makes the
attempt non-committable. The adapter does not retry through another provider.

### Capabilities

`GET /api/v1/models` and the endpoints catalog are observed inventories. Each
snapshot has source, retrieval time, digest, expiry, exact model and endpoint
slugs, context/output limits, parameters, price, modalities, moderation and
data-policy evidence. Catalog claims do not authorize execution and stale
claims cannot add a required capability.

Conformance is behavior-based and versioned. In particular:

- tool support requires exact call/result round-trip fixtures, not merely a
  `tools` catalog flag;
- `response_format` without JSON-schema enforcement does not satisfy a
  schema-required workload;
- context and output limits are the minimum of admitted policy, catalog and
  observed endpoint bounds;
- cancellation, streaming, usage, finish reasons, reasoning fields and error
  mapping are independently tested;
- unknown fields are retained only as bounded diagnostic evidence or rejected;
  they never silently enable a feature.

### Acceptance, retries and cancellation

OpenRouter documents no provider-turn idempotency key. A request can be accepted
upstream before Sessionless receives headers or the first SSE event. Therefore:

- DNS, TLS, credential-load and local encode failures before any request bytes
  are written are `not_accepted` and may be retried under the same attempt only
  by an explicitly bounded transport policy;
- after the HTTP request may have been written, loss before a response is
  `accepted_unknown` and is not automatically retried;
- the first response/generation identifier is acceptance evidence, not proof of
  canonical completion;
- stream loss after acceptance is ambiguous. `/generation` may reconcile usage
  by generation ID but does not reconstruct a missing canonical answer;
- cancellation closes the request context and records local intent. It does not
  invent upstream cancellation acknowledgement;
- retry after any accepted or ambiguous invocation requires a new Attempt and
  the canonical scheduling/fallback decision.

Response caching is beta and is not enabled or used as an idempotency mechanism.
Provider errors and bodies map to fixed content-free codes. Raw upstream errors,
request bodies, outputs, and keys never enter logs, audit, metrics, queue payloads
or support bundles.

## Ox Alpha canary

As observed on 2026-08-25, the official
[Ox Alpha page](https://openrouter.ai/stealth/ox-alpha) and public models API
report:

- exact slug `stealth/ox-alpha` and one `Stealth` endpoint;
- zero prompt and completion price at the observation time;
- 1,048,576-token context and up to 131,072 completion tokens;
- text/image/video input and text output;
- reasoning is mandatory; tools/tool choice and `response_format` are listed;
- free-model availability and rate limits are non-guaranteed.

This is suitable only for a synthetic, non-production conformance canary. It is
not a free production capacity promise. The provider is anonymous, availability
may end without notice, and the route cannot satisfy a policy requiring a known
processor, ZDR, region, or durable price/availability guarantee.

There is also a material policy conflict. The model page says prompts and
completions are retained but not used for training, while the controlling
[Stealth Program EULA](https://openrouter.ai/terms/stealth) says Stealth Models
are free in exchange for collection and use of User Content for model training
and grants a broad perpetual license. Sessionless must apply the more
restrictive interpretation: **training/retention allowed, anonymous processor,
no private or customer data**. A null endpoint `data_policy` is not evidence to
the contrary.

The canary corpus may contain only generated public fixtures. It excludes:

- real Sessionless messages, sessions, issues, repositories or artifacts;
- source code not created solely for the fixture;
- personal, credential, account, tenant, customer or operational data;
- security findings, internal architecture not already public, and secrets;
- tool execution, network retrieval, files, images and video in the first gate.

The first live gate uses the adapter that first passes credential-free
conformance; Pi is the current preference because it can serialize the exact
OpenRouter route policy. It is a single non-streaming text fixture with a tiny
output bound, followed by bounded streaming, cancellation, route metadata,
usage and exact no-fallback checks. OpenCode, Codex and direct-API gates are
separate. The Codex gate is prompt-only, no tools/network, zero Codex retries
and cannot claim route-policy support it cannot observe. Passing one gate proves
only that exact harness/provider tuple; it does not prove another adapter,
model quality, availability, privacy suitability or production readiness.

## Credential custody

Do not provide an API key yet. The current repository has only the local
provider-neutral credential lifecycle; it does not have the production
owner-scoped YDB/Lockbox backend needed to ingest and bind a router credential.
Putting a key in a shell environment, developer `.env`, command line, issue,
Terraform value, generic runtime Lockbox payload or worker image would bypass
the accepted resource/generation contract.

Before a live canary, the following must exist:

1. an owner-scoped `AIResource`/credential binding with immutable provider kind
   `openrouter`, credential generation, revoke fence and policy revision;
2. a write-only ingestion path into a dedicated KMS-backed secret reference,
   with no plaintext in API responses, YDB, Terraform state or logs;
3. invocation-only materialization and release through the existing credential
   lifecycle; the direct Go adapter reads the exact bounded versioned secret
   material directly, while a Pi/OpenCode/Codex adapter may expose
   `OPENROUTER_API_KEY` only to its sanitized child process for that invocation;
4. deny-first revoke, key rotation, expiry and ambiguous cleanup tests;
5. a canary feature flag that cannot select the resource for ordinary runs.

The user-created key for that gate should be a dedicated inference key, not a
management key: short expiry, a very small spend limit, no auto top-up, no BYOK,
an exact `stealth/ox-alpha` model allowlist, a `stealth` provider allowlist, the
explicit synthetic-data privacy posture, and no other application use.
OpenRouter's documented key limits, expiry and guardrails are additional
controls, not the Sessionless authorization source. The key is delivered only
through the accepted secret-ingestion channel and is never pasted into chat or
a tracked file.

## Competitor evidence

| Product | Observed integration | Useful lesson | Rejected inheritance |
|---|---|---|---|
| Codex CLI | Official custom-provider configuration uses OpenRouter's Responses endpoint, an exact model and environment or command auth. Codex exposes retry limits, isolated configuration and ephemeral runs, but no OpenRouter `provider` request-body configuration. | A Codex/OpenRouter backend is viable behind the Sessionless-owned routing harness as a separately governed adapter and synthetic canary. | It cannot stand in for the direct provider port or silently inherit OpenRouter fallbacks, mutable presets/catalog, ambient user config or ChatGPT subscription identity. |
| Pi | Provides headless RPC/no-session operation, a standardized provider stream, built-in OpenRouter credentials/models, and `openRouterRouting` passed as the exact request `provider` object. | Best current first spike for a narrow harness adapter whose OpenRouter route can be pinned and tested. | Pi sessions, native tools, extensions/packages, project/global config and provider catalog do not become product authority. |
| OpenCode | Built-in OpenRouter provider accepts an API key, exposes one-shot JSON events/headless service, loads catalog/models, and supports provider ordering plus `allow_fallbacks:false`. | A second pluggable harness can preserve the same OpenRouter route policy through a different agent implementation. | OpenCode history, tools, plugins, sharing, config and credential store do not become Sessionless authority. |
| Aider | Calls OpenRouter with an API key and an `openrouter/...` model; can choose a model based on available keys/account. | A CLI can consume the API, but the API is the actual provider boundary. | Automatic model selection from ambient keys violates pinned resource/no-fallback policy. |
| OpenRouter Agent SDK/Ori | Adds tool loops and agent state above the same OpenRouter API. | Confirms provider and agent layers are separable. | A second agent runtime and its tool/history semantics are unnecessary for the Sessionless Go harness. |
| OpenAI-compatible SDK path | OpenRouter documents pointing an OpenAI SDK at its base URL. | Shared wire utilities are possible. | A generic OpenAI client cannot validate OpenRouter routing, privacy, cost, metadata, key and error semantics. |

Competitor behavior is implementation evidence, not permission or a security
contract. Sessionless keeps the narrowest official surface and owns its agent
state.

## Implementation backlog

1. **PR-01 resource contracts**: land versioned router resource, route/catalog,
   harness binding, capability, privacy, price and policy-evidence DTOs without
   credentials.
2. **PR-02 harness/provider conformance kit**: one `HarnessDriver` fixture suite
   plus fake OpenRouter HTTPS/SSE, RPC/JSONL fixtures, acceptance ambiguity,
   cancellation, route/usage evidence, process and size/depth/event/time bounds.
3. **PR-05a credential ingestion**: owner-scoped binding plus real secret-store
   adapter, generation/revoke/rotation and invocation-scoped materialization.
4. **PR-05b Pi adapter**: pinned RPC/no-session process, generated exact
   OpenRouter route config, no ambient tools/extensions/config and synthetic
   Ox Alpha canary.
5. **PR-05c OpenCode adapter**: pinned one-shot JSON process first, generated
   exact provider body, isolated homes/no stored auth/plugins/session, and the
   same synthetic canary.
6. **PR-05d Codex/OpenRouter profile**: reuse the supervised Codex adapter with
   isolated generated provider configuration, zero upstream retries and a
   dedicated guarded resource; keep policies requiring unavailable route
   evidence ineligible.
7. **PR-05e direct provider adapter/reference harness**: native Go HTTP
   implementation with exact route/usage evidence and a minimal reviewed agent
   loop when product needs a CLI-independent provider path.
8. **PR-05f portfolio canary**: compare the same immutable Ox Alpha fixtures
   across Pi/OpenCode/Codex/direct supported tuples; publish dated catalog,
   policy, route and rate-limit evidence, never prompt content.
9. **PR-07 E2E/security**: two-owner resource/key isolation, revoke races,
   ambiguous completion, budget exhaustion and adapter rollback.

Rollout is fake conformance -> credential-free catalog observation -> restricted
synthetic Pi canary -> OpenCode and Codex profiles -> optional direct reference
harness -> maintainer-only portfolio -> opt-in tenant. Rollback disables the
exact harness/route/resource revision and allows no new attempts; it never
switches harness, Codex subscription, OpenRouter model, key or billing account.

## Required acceptance tests

- strict request and response DTOs reject duplicate/unknown/security-sensitive
  fields, null ambiguity, invalid UTF-8, noncanonical numbers, deep/wide input,
  oversized bodies/SSE events and trailing data;
- TLS host, redirect, proxy and header allowlists; no cookie, ambient key, trace,
  user ID, raw upstream error or credential leak;
- exact resource owner/revision/key generation, attempt/lease/fence, catalog and
  policy digests plus harness kind/version/artifact/protocol checked before
  request bytes are written;
- each Pi/OpenCode/Codex adapter uses isolated generated configuration, a
  sanitized environment, no ambient credential/model/config/plugin discovery,
  bounded one-in-flight execution and complete process/boundary cleanup;
- the same immutable fixture through different harnesses produces separate
  evidence and retry lineages; native sessions/checkpoints are never shared;
- requested model and actual model/provider match the sealed route; fallback,
  alias and random-router fixtures fail;
- capability mutation matrix for tools, structured output, reasoning, context,
  modalities, stream and finish semantics;
- pre-write failure versus post-write ambiguity, first-event loss, mid-stream
  loss, cancellation, deadline and late-event rejection;
- usage/cost/native-token provenance, unknown fields, free price changes, 429,
  payment-required and catalog removal;
- exact key rotation/revoke during request; no retry with old/new key after
  acceptance;
- two owners cannot select, observe, use, rotate, revoke or infer each other's
  resource or key;
- Ox Alpha fixtures contain no private data and the canary remains impossible
  to select from ordinary production admission.

## Primary sources

- [OpenRouter quickstart and direct API](https://openrouter.ai/docs/quickstart)
- [provider routing and fallback defaults](https://openrouter.ai/docs/guides/routing/provider-selection)
- [chat completion API](https://openrouter.ai/docs/api/api-reference/chat/send-chat-completion-request)
- [usage accounting](https://openrouter.ai/docs/cookbook/administration/usage-accounting)
- [free-model limits](https://openrouter.ai/docs/faq)
- [data collection](https://openrouter.ai/docs/guides/privacy/data-collection)
- [provider logging](https://openrouter.ai/docs/guides/privacy/provider-logging)
- [API key creation](https://openrouter.ai/docs/api/api-reference/api-keys/create-keys)
- [API key observations](https://openrouter.ai/docs/api/api-reference/api-keys/get-current-key)
- [guardrails](https://openrouter.ai/docs/guides/features/guardrails/overview)
- [OpenRouter Codex integration](https://openrouter.ai/docs/cookbook/coding-agents/codex-cli)
- [OpenAI Codex configuration reference](https://developers.openai.com/codex/config-reference)
- [Ox Alpha model](https://openrouter.ai/stealth/ox-alpha)
- [Stealth Program EULA](https://openrouter.ai/terms/stealth)
- [Pi RPC mode](https://github.com/badlogic/pi-mono/blob/main/packages/coding-agent/docs/rpc.md)
- [Pi CLI/tool/resource controls](https://github.com/badlogic/pi-mono/blob/main/packages/coding-agent/docs/usage.md)
- [Pi custom models and OpenRouter routing](https://github.com/badlogic/pi-mono/blob/main/packages/coding-agent/docs/models.md)
- [OpenCode provider documentation](https://opencode.ai/docs/providers)
- [OpenCode CLI](https://opencode.ai/docs/cli)
- [OpenCode OpenRouter adapter source](https://github.com/anomalyco/opencode/blob/dev/packages/core/src/plugin/provider/openrouter.ts)
- [Aider model and key documentation](https://aider.chat/docs/troubleshooting/models-and-keys.html)
