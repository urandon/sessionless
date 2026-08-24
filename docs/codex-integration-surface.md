# Codex integration surface and credential locality

Status date: **2026-08-24**. Decision record for issue
[#62](https://gitcode.com/urandon/sessionless/issues/62), affecting
[#13](https://gitcode.com/urandon/sessionless/issues/13) and
[#61](https://gitcode.com/urandon/sessionless/issues/61).

The credential-free measurements and current decision delta are recorded in
[codex-surface-measurement.md](codex-surface-measurement.md). They currently
leave `codex exec` as the sole candidate for the explicitly consented phase;
they do not approve production use or resume #61.

## Decision

Sessionless provisionally selects the **stable API subset of a pinned Codex App
Server over private stdio**, behind the existing Go `HarnessDriver`, for bounded
adapter research and implementation.

The `codex app-server` command itself is currently documented as experimental
and unsupported for production workloads. Therefore it is an immediate
production-release no-go even though the protocol exposes a stable subset.
Issue #64 must compare it with the stable Python SDK and `codex exec`. A later
official production-support statement or an accepted stable-SDK route is needed
before any personal-subscription release.

The first personal ChatGPT Plus/Pro deployment is **an attached worker owned by
the user**. Its Codex credential stays on that worker. The Sessionless control
plane stores only an opaque AI-resource identity, worker placement, connection
health, lease state, and sanitized provider observations.

A multi-tenant Sessionless cloud worker must not take custody of a consumer
ChatGPT credential by default. That mode remains disabled until the cloud
custody, policy, isolation, refresh/write-back, and consent gates in this
document pass. ChatGPT Enterprise access tokens and workload identity are
separate deployment modes with separate administrator-controlled resources.
OpenAI Platform API keys are separately billed resources and are never a
fallback for a ChatGPT subscription resource.

Codex threads are invocation state, not product history. The first adapter
starts one fresh App Server process, one ephemeral thread, and one turn for one
fenced Sessionless attempt. Sessionless remains authoritative for the canonical
Session, Run, Attempt, context snapshot, checkpoints, artifacts, permissions,
and terminal result.

This accepts App Server only for bounded implementation work. It does **not**
approve any production subscription-backed execution, cloud credential custody,
or subscription federation.

## Evidence vocabulary

- **Documented**: stated by the provider's current primary documentation.
- **Observed**: reproduced locally or read in a pinned source revision.
- **Inference**: a Sessionless design consequence derived from documented or
  observed evidence.
- **Unknown**: requires a provider answer, explicit consent, or a controlled
  acceptance experiment.

Competitor source proves that a mechanism exists. It never proves that a
provider permits Sessionless to use that mechanism.

## Official integration-surface matrix

| Surface | Documented fit | Capabilities relevant to Sessionless | Decision |
| --- | --- | --- | --- |
| Codex App Server | OpenAI's documented integration surface when the agent is part of the product and the client needs direct lifecycle and user-experience control. The command is experimental and unsupported for production; only part of its API is labelled stable. | Language-neutral JSONL protocol; ChatGPT browser/device login; account/workspace state; model discovery; multi-bucket rate limits; account usage; thread/turn/item lifecycle; interruption; streamed progress; approvals; sandbox/configuration state. | **Provisional research/implementation selection.** Pin binary and stable schema, disable experimental capabilities, and block production release. |
| Codex SDK | Official application/automation surface. The TypeScript SDK has a smaller high-level interface; the stable Python SDK controls App Server and bundles a pinned runtime. | Convenient lifecycle ownership and supported runtime packaging. Public high-level docs do not establish account/quota/approval parity or all fencing facts needed by the Go worker. | Mandatory #64 comparator and preferred production candidate if it exposes the required guardrails without an unsafe escape hatch. Runtime/language cost must be measured. |
| `codex exec` | Official non-interactive mode for one-off tasks, pipelines, scheduled jobs, and CI. | Explicit sandbox, JSONL events, output schema, resume, process exit status, and an ephemeral mode. Account connection UX, quota projection, interactive approvals, and exact interrupt semantics are outside its run contract. | Required benchmark and emergency implementation fallback, selected only before an attempt. Never a silent mid-attempt fallback. |
| Codex MCP server | Official way to expose Codex as a specialist tool inside an MCP/Agents SDK workflow. | Portable tool invocation, but loses richer Codex session, diff, account, quota, and product event semantics. | Rejected as the primary personal-agent harness. May become a later tool under #46. |
| Direct ChatGPT/Codex backend emulation | Implemented by OpenCode and Zed, not documented by OpenAI as a third-party integration contract. | Potentially lower process overhead, but requires Sessionless to duplicate OAuth, refresh, model catalog, request headers, quota interpretation, compatibility, and policy assumptions. | Rejected. Do not reuse competitor OAuth client IDs, private endpoint paths, cookies, or hard-coded model entitlement lists. |
| OpenAI API | Official programmatic API with API-key/workload identity billing and policy. | Stable API integration, but it is a separately billed model resource rather than ChatGPT subscription access. | Supported later as a distinct `AIResource`; never an automatic fallback. |

The provisional selection is based on current official OpenAI guidance. App
Server is the only documented surface inspected whose public contract includes
both the full agent lifecycle and the subscription account/quota observations
required by the product, but its command maturity prevents production use. A
bounded one-turn job resembles `codex exec`, while the stable Python SDK is the
official programmatic surface. Both are mandatory comparators: if direct App
Server control does not materially improve account routing, quota projection,
cancellation, or future approval handling, Sessionless must choose the smaller
supported surface rather than defend sunk cost.

## Authentication and billing-resource matrix

| Resource kind | Documented use | Credential owner | Sessionless deployment decision |
| --- | --- | --- | --- |
| Personal ChatGPT Plus/Pro | ChatGPT desktop, Codex CLI, IDE, and App Server login use subscription access. Standard browser login is primary; device-code login is beta and must be enabled by the user or workspace. | User's local Codex client/worker. | Attached worker first after a production-supported surface exists. No federation, credential export, or warm cross-user pool. |
| Consumer credential in Sessionless cloud | Public documentation explains local cached login and trusted runner persistence, but does not establish general multi-tenant SaaS custody or family/federation use. | Sessionless would become refresh-token custodian. | Disabled pending explicit policy, consent, threat model, and controlled evidence. |
| ChatGPT Enterprise Codex access token | Trusted scripts, schedulers, and private CI runners that need workspace-managed Codex access. | Workspace administrator/member under Enterprise controls. | Separate organization resource; require admin configuration, rotation, revocation, and workspace binding. |
| Workload identity | Preferred when the cloud/CI platform can issue short-lived workload tokens. | Organization identity plane. | Preferred cloud enterprise mode when available; separate from consumer login. |
| OpenAI Platform API key | Programmatic use at standard API pricing and API-organization data controls. | API organization/project. | Separate explicit resource and billing route. Never substitute for subscription exhaustion or reauthentication. |

`account.type`, `account/updated.authMode`, and the selected resource kind form
the billing-route guard. A subscription attempt proceeds only while the route
is `chatgpt`. `apiKey`, null account, unauthorized state, or a route change
terminates the attempt as `reauth_required`; it does not trigger another
provider or billing resource.

Provider-policy approval is a versioned decision artifact, not an informal
checkbox. Issue #48 must record one verdict for each tuple of account type,
execution surface, placement, credential custodian, and sharing model. Each
record names the authoritative document or provider response, precise deployment
shape, decision owner, evidence date, go/no-go result, and expiry or re-review
triggers such as terms, auth, runtime, or product changes. Missing, expired, or
ambiguous evidence is no-go. Personal attached worker, Enterprise runner, cloud
consumer custody, and federation are separate decisions.

## Credential locality

| Placement | Benefits | Unresolved risk | Decision |
| --- | --- | --- | --- |
| Attached user worker | Consumer access/refresh credentials never leave the user's environment; provider and local-model locality align with #47. | Worker compromise, offline behavior, update UX, outbound connection leases. | **MVP personal-subscription boundary.** |
| Tenant-scoped encrypted cloud vault | Central scheduling and serverless workers are possible. | High-value multi-tenant secret custody; mutable single-use refresh tokens; crash between refresh and write-back; consent and provider-policy uncertainty. | Gated preview only after all cloud gates pass. |
| Central provider-call broker | Workers do not receive long-lived credentials. | Broker sees all prompt/tool content and still owns refresh tokens; new HA and security-critical service. | Not MVP. Research with #48/#47. |
| Local broker on attached worker | Stable local API while credential remains user-side. | Additional daemon lifecycle and protocol. | Possible evolution of attached worker. |

For personal resources the refresh owner must be singular. Parallel copies of
one mutable Codex login are forbidden. One resource admits at most one active
refresh-capable lease until provider concurrency and refresh semantics are
measured. A quota response such as HTTP 429 changes resource availability; it
does not invalidate the credential. Invalid, reused, or unrecoverable refresh
state becomes `reauth_required` or `credential_reseed_required`.

OpenAI's advanced CI/CD-auth guide documents the exact trusted-runner pattern:
restore the current file, let Codex refresh it, persist the updated file, and
use only one machine or serialized job stream per copy. It also says that API
keys remain the recommended route for ordinary CI/CD and excludes generic OAuth
clients and experimental external-token integrations. Therefore this evidence
proves a supported credential-maintenance mechanism on trusted private
infrastructure; it does not prove that multi-tenant custody, credential sharing,
or subscription federation is an approved product model.

Disconnect is deny-first: stop new leases, drain or cancel active work, clear
local Sessionless authority, and remove Sessionless-owned materializations.
Local `account/logout` or file deletion must not be described as provider-wide
token revocation unless an official revocation contract and acceptance test
prove it. Removing a Sessionless resource must not silently destroy an
unrelated global Codex login on the user's machine.

## Canonical history and retry boundary

```text
canonical Session events + immutable context snapshot
                         |
                         v
fenced Run / Attempt / worker lease
                         |
                         v
fresh App Server -> ephemeral thread -> one turn
                         |
                         v
provisional events + sanitized usage + final candidate
                         |
                         v
lease/fence check -> durable canonical commit -> frontend delivery
```

- Provider thread and turn IDs are diagnostic correlation only.
- Retry starts a new provider process/thread from the same immutable input
  snapshot. It never resumes hidden provider history to repair an ambiguous
  Sessionless attempt.
- Streamed deltas are provisional. Terminal user delivery follows the durable
  canonical commit.
- App Server process loss after `turn/start` but before one terminal
  `turn/completed` is ambiguous. Side-effect-free work may retry under the
  existing Attempt contract. Future tools require their own idempotency and
  effect ledger.
- Provider-native compaction and thread storage are disabled or made ephemeral
  for MVP. Sessionless compaction remains #28/#45 work.

## Data egress and consent

Each AI resource has an explicit egress policy. For the first read-only turn,
only the following may be sent to App Server/OpenAI:

- the selected bounded canonical text history or summary;
- the current user text input;
- fixed Sessionless instructions needed for the one-turn task;
- explicitly selected attachments after type, size, and policy validation;
- tool results only after a later tool is separately authorized.

The following are never provider inputs or metadata:

- access/refresh tokens, credential references, authorization URLs, or device
  codes;
- tenant, frontend transport, queue, lease, fence, storage, or internal user
  identifiers;
- unrelated sessions, memories, attachments, workspace files, environment
  variables, or operational errors;
- MCP/tool schemas not explicitly granted for the run.

The user-facing connection flow must state that selected content is processed
under the chosen ChatGPT workspace's permissions, retention, residency, and
data controls. Consent records the resource, policy revision, allowed content
classes, execution placement, and revocation path. Missing policy, consent, or
an answer to a required data classification denies the run.

Personal attached-worker mode minimizes credential movement, not content
movement: the selected prompt still reaches OpenAI. Local inference resources
under #51 require a separate egress policy and may keep both credentials and
content local.

## Permission and isolation invariants

The provider/harness is never the sole authorization authority.

- Only the App Server stable API subset; `experimentalApi` is false. This does
  not change the experimental maturity of the command itself.
- A fresh managed `HOME`, workspace, process group, and bounded temporary
  directory are created per attempt. An attached worker may reuse only its
  protected connection-scoped credential `CODEX_HOME`; cloud mode uses a fresh
  temporary `CODEX_HOME` materialization with fenced write-back and wipe.
- Ambient host config, API keys, plugins, skills, MCP servers, rules, and prior
  provider threads are not inherited.
- MVP intent is read-only with no tool capability granted by Sessionless.
  App Server still contains native command/file tools, and `approvalPolicy:
  "never"` is not a tool-disable switch.
- Every turn uses an explicit structured `readOnly` sandbox policy with
  restricted readable roots. Omitting `access` is forbidden because its
  documented default is full filesystem read access.
- The process also runs inside an external container/microVM or equivalent
  mount boundary that exposes only the minimal runtime, bounded invocation
  input, and empty work directory. Provider credentials and sibling/user host
  paths must not be readable by model-controlled tools.
- Any unexpected command, file-change, permission, elicitation, dynamic-tool,
  or user-input request is denied and interrupts the turn.
- The OS/container/worker isolation policy remains authoritative even when
  App Server reports a sandbox or permission state.
- Egress defaults to deny and separately permits documented provider traffic.
  Hostname details observed in competitor code are not an allowlist contract.
- Warm containers never reuse auth homes, workspaces, provider processes, quota
  state, or logs across tenants/resources.
- Logs contain bounded sizes, durations, status, version, and sanitized
  observations only; no protocol frame, prompt, model output, path, auth data,
  or raw provider error body.

## Quota and usage semantics

App Server exposes ChatGPT multi-bucket rate-limit observations, optional
credits and reset times, account usage summaries, and per-thread token usage.
They are different facts:

- catalog/model capability does not prove entitlement;
- `usedPercent` is an observation, not a capacity reservation;
- account usage includes activity outside Sessionless;
- turn token counts do not prove subscription billing or remaining quota;
- absent and null fields remain `unknown`, never zero;
- sparse updates merge only present fields or trigger a full refetch;
- every stored observation carries resource generation, provider source, and
  observation time;
- federation cannot schedule fairly from provider `usedPercent` alone.

No rate-limit reset credit is consumed automatically. Such an action spends a
user-owned provider resource and requires a separate explicit product policy.

## Competitor and harness comparison

| Reference | Reusable evidence | Boundary Sessionless must reject |
| --- | --- | --- |
| OpenCode | Typed provider adapters, explicit auth modes, provider-native quota metadata, and a working demonstration that subscription and API resources need separate routing. | Reusing an OAuth client ID, calling private ChatGPT backend paths, rewriting provider requests, decoding unverified JWT metadata as authority, and hard-coding model entitlement are not supported contracts. |
| Zed | Clean separation among a direct ChatGPT provider, Codex as an ACP child process, and a terminal-owned CLI thread. Credentials remain local to the selected execution boundary. | Zed's direct OAuth/backend implementation is competitor evidence, not OpenAI authorization. A terminal thread delegates too much lifecycle and policy to the CLI for Sessionless's canonical-attempt contract. |
| Hermes Agent | App Server lifecycle, fail-closed unattended approvals, usage projection, subscription-aware credential pooling, refresh locking/write-back, and the concrete danger of hidden provider history. | Soft over-cap credential leasing, broad credential environment forwarding, warm provider-thread reuse, and terminal delivery before durable Sessionless commit. |
| DeepSeek Harness | Plugin-seamed harness architecture, append-only model-visible session logging, ACP automation sessions, one-shot permissions, and local-first data documentation. | Preview wire instability, incomplete SDK lifecycle, same-UID-readable credentials, file-only sandbox semantics, and unsafe minimal-example defaults. |

These projects answer architecture and failure-mode questions. None can answer
whether OpenAI permits Sessionless's intended use of a personal ChatGPT
subscription; only provider documentation or an explicit provider decision can
close that gate.

### DeepSeek Harness details

The official DeepSeek Harness source was audited at
`b150a551b8d465e31e418e1b2eaf5e79bbb7d28e` (`0.1.1-rc.2`). It is a useful
harness/runtime comparator, not an OpenAI authorization source.

| Property | DeepSeek Harness evidence | Sessionless consequence |
| --- | --- | --- |
| Maturity | Official developer preview; breaking changes are promised; session format remains version 0. | Do not select it as the MVP production harness. Pin exact source/runtime for any experiment. |
| Architecture | Model, session, tools, sandbox, storage, loop, SDK, and UI are replaceable Cordis plugins. Everything model-visible is recorded in an append-only session log. | Reuse the capability-seam and model-visible-equals-logged design principles, but keep Sessionless canonical authority. |
| SDK JSON-RPC | TypeScript/Python clients launch a runtime; Python ships a pinned executable. Current wire has no version negotiation, prompt-level result/cancel, or session close; it broadcasts runtime-wide events for client-side filtering, has unbounded JSONL/notification buffers, and treats whole-agent idle as prompt completion. | Reject as a production adapter boundary. Client timeout does not cancel server work, and client-side tenant filtering is not isolation. |
| ACP | Automation-only fresh sessions, prompt cancellation, committed-output drain, one-in-flight admission, exact agent identity, connection-owned cleanup, and one-shot permissions. It omits auth, resume/history/configuration, usage, and broader UI semantics. | The only plausible future DeepSeek Harness adapter, after independent protocol bounds and isolation. It is not a replacement for Codex account/quota integration. |
| Credentials | API/provider records from env, YAML, or plugin authorization. Local store uses 0700/0600 and atomic writes, but explicitly does not protect secrets from same-UID tools; remote revocation is deferred. | DeepSeek API/local endpoint and DeepSeek Harness are distinct resource/harness identities. Never mount a raw credential store next to untrusted tools. |
| Sandbox | File-effect modes with fail-closed provider selection; network and process visibility are outside the sandbox vocabulary. | A platform egress/process policy remains mandatory. Do not equate file sandbox with isolation. |
| Data | Local-first by default; external models, web, MCP, or plugins may upload data under their providers' policies. Provider requests also include stable user/session attribution, while optional full telemetry can contain prompts, tool data, file contents and cwd. | Plugin composition, endpoint, attribution and telemetry mode are part of the resource's egress/consent evidence. Keep telemetry disabled until Sessionless owns redaction/export policy. |
| Environment | The TypeScript SDK can receive a fully scrubbed environment, but the Python wrapper starts with `os.environ.copy()` and overlays caller values. | Do not treat a small Python `env` map as sanitization. A future adapter must replace, not augment, the child environment. |
| Runtime closure | Python bundles a Node 24 executable and a large plugin closure; release checksums exist, while public attestations are disabled. Cold start, RSS and descendant cleanup are not published. | Require a minimal ACP composition, SBOM/provenance, exact digest pinning and target-worker benchmarks before adoption. |
| Example safety | The Python minimal example documents `danger-full-access`, bare local filesystem, and persistent shell state. | Examples are not production defaults. Sessionless must supply its own isolated composition and policy tests. |

DeepSeek Harness may later enter as `harness_kind=deepseek-harness` behind the
harness-neutral worker contract, using ACP rather than its current SDK wire.
A DeepSeek API key, local DeepSeek-compatible endpoint, and the harness runtime
are three separate resources/capabilities. The current DeepSeek provider uses
API-key/endpoint configuration; ACP advertises no authentication methods. It
therefore cannot answer or replace the subscription-backed Codex decision.

## Competitor evidence retained

Pinned read-only source revisions:

- OpenCode `3a31c4ea801915c0b050df4b3842997ea62b6e93`;
- Zed `d9ad6aff67e47de43abb270d22de75dd950f1b48`;
- Hermes Agent `c80a0a551c7038517456ee0aeb60203ec92aedb6`;
- DeepSeek Harness `b150a551b8d465e31e418e1b2eaf5e79bbb7d28e`.

Reusable patterns are typed provider/harness boundaries, generation-fenced
refresh, local credential custody, capability handshakes, append-only session
logs, fail-closed approvals, immutable tool provenance, and cheap pre-model
gates. Rejected patterns are OAuth-client reuse, private endpoint emulation,
hard-coded entitlement, ambient environment inheritance, same-UID plaintext
secret exposure, provider-controlled authorization, shared persistent workers,
and token-count-as-billing assumptions.

## Runtime and supply-chain requirements

- Pin an official Codex release and platform artifact digest; generate and
  archive the stable App Server schema checksum from that exact binary.
- Package the runtime reproducibly with SBOM/provenance and no mutable download
  at job start. Upgrade by canary, contract fixtures, and explicit rollback.
- Bound JSONL frame bytes, total output, item count, stderr, process tree, wall
  time, disk, RSS, and shutdown grace.
- Cancellation sends `turn/interrupt`, waits for a bounded terminal event, and
  then terminates and reaps the full process group.
- Measure cold spawn, initialize, account reads, thread start, first event,
  first token, turn completion, teardown, image bytes, RSS, disk, and egress on
  every target worker class. No desktop measurement substitutes for cloud or
  attached-worker evidence.
- Incompatibility, unknown auth mode, unexpected server request, incomplete
  schema support, or an unavailable required isolation feature fails closed.

## Acceptance and go/no-go gates

### Required before resuming or merging the #61 implementation

1. Stable API fixtures cover initialize, ChatGPT-only account guard, model and
   quota reads, sparse updates, one ephemeral thread/turn, usage, interruption,
   terminal completion, malformed/oversized input, and unexpected requests.
2. Dedicated-home and external-isolation tests prove no host credential,
   config, plugin, MCP, rule, history, host-home, credential-store, platform
   process path, or sibling-workspace reads and no cross-tenant warm state.
3. One process/thread/turn per Attempt; no resume, fork, steer, dynamic tools,
   or experimental API.
4. Exact process-group kill/reap and ambiguous-completion behavior are bounded.
5. The runtime binary and stable schema are pinned and upgrade-gated.
6. The same synthetic bounded task is compared with the stable Python SDK and
   `codex exec --json --ephemeral`; account/quota/approval parity, sidecar
   footprint, and direct-App-Server complexity must be explicit.

### Additional gates for personal attached-worker release

1. Outbound authenticated attach, worker ownership, capability advertisement,
   lease/cancel/drain/reconnect, and remote revoke are complete under #47.
2. The worker is the sole refresh owner and never sends token bytes to the
   Sessionless control plane.
3. Explicit content-egress consent and data-policy disclosure are recorded.
4. Read-only no-tool execution passes isolation, redaction, quota, cancellation,
   and resource-revocation tests on supported worker operating systems.
5. Provider policy permits the intended automated personal-subscription use.
6. The selected execution surface is officially supported for production; an
   experimental App Server command is not releasable.

### Additional gates for cloud consumer credential preview

1. Explicit policy and legal approval for third-party hosted credential custody
   and the intended product usage; protocol feasibility is not sufficient.
2. Tenant-scoped encrypted secret backend, exclusive resource lease,
   generation/CAS refresh write-back, crash recovery, reseed flow, and audited
   deletion pass adversarial tests.
3. Separate uid/container, restricted egress, log/core-dump controls, and warm
   reuse isolation pass in the actual cloud runtime.
4. A consented cloud-dev login and turn prove ChatGPT billing-route continuity,
   refresh persistence, quota observation, and no API-key fallback.
5. Cold-start, latency, RSS, disk, duration cost, and shutdown measurements meet
   the declared budget.

### Immediate no-go conditions

- Provider policy is negative or remains materially ambiguous for the proposed
  deployment mode.
- The selected command/runtime is experimental or explicitly unsupported for
  production workloads.
- The run depends on an experimental App Server method.
- Consumer credentials must be copied from a developer's global Codex home.
- Tenant isolation, credential write-back, or full process cleanup cannot be
  proven on a target worker.
- App Server cannot prevent ambient tools/config or cannot provide bounded
  terminal completion.

## Phased backlog consequence

1. Update #61 to target a **local/attached-worker-compatible**, read-only,
   one-turn App Server adapter. Retain the prepared-path and process-lifecycle
   work, but do not wire it into cloud production.
2. Complete #47's attached-worker protocol before personal subscription release.
3. Extend #48/#51 with the typed AI-resource, provider, harness, billing,
   entitlement, placement, and sharing distinctions in this decision.
4. Use #46 for the platform-owned permission/egress plane and #49/#63 for
   quota, cost, and harness-neutral quality gates.
5. Use #64 for the credential-free and explicitly consented attached-worker
   stable-SDK vs App-Server vs exec comparator, including cold-start,
   cancellation, quota, isolation, and refresh/write-back evidence.
6. Cloud consumer credential work remains a later gated task owned by the
   policy and resource decisions in #48 rather than being inferred from #64.

## Primary references

- OpenAI [App Server documentation](https://learn.chatgpt.com/docs/app-server)
  and [integration-layer selection guidance](https://learn.chatgpt.com/blog/codex-as-a-platform).
- OpenAI [Codex authentication](https://learn.chatgpt.com/docs/auth),
  [advanced trusted-runner authentication](https://learn.chatgpt.com/docs/auth/ci-cd-auth),
  [Codex SDK](https://learn.chatgpt.com/docs/codex-sdk), and
  [non-interactive mode](https://learn.chatgpt.com/docs/non-interactive-mode).
- DeepSeek [Harness developer preview](https://deepseek.com/harness/en/),
  [official source](https://github.com/deepseek-ai/deepseek-harness), and
  [data-processing statement](https://www.deepseek.com/harness/en/data-processing/).
