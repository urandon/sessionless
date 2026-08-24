# Codex subscription worker: Phase A evidence and protocol contract

Status date: **2026-08-22**. Scope: issue [#13](https://gitcode.com/urandon/sessionless/issues/13), Phase A only. The later integration-surface and credential-locality decision is recorded in [codex-integration-surface.md](codex-integration-surface.md).

## Verdict

The local machine-protocol slice is **viable for research**: the selected Codex
App Server starts with a fresh isolated `CODEX_HOME`, completes the JSONL
handshake, and exposes the account, quota, usage, thread, turn, cancellation,
approval, and tool-call protocol surfaces needed by an adapter. The command is
currently experimental and unsupported for production even when the client
stays on its stable API subset.

The subscription-backed cloud worker is **not yet approved for production**.
Cloud-dev consent/auth restore, refresh persistence, resource measurements, and
the policy/terms basis for a hosted third-party service using a person's
ChatGPT subscription remain unproved. Current official guidance selects App
Server when the agent is part of the product and direct lifecycle control is
needed. Separately, the advanced CI/CD-auth guidance recommends API keys for
ordinary automation and limits ChatGPT-managed cache persistence to trusted
private infrastructure with one serialized owner. Those deployment constraints
are release gates, not permission to silently change the billing route.

The 2026-08-24 comparator supersedes the provisional App Server selection:
Sessionless keeps its Go harness boundary, uses direct App Server and the stable
Python SDK only as non-selectable research comparators, leaves `codex exec` as
the sole candidate for an explicitly consented local experiment, and makes a
user-owned attached worker the first eligible personal
Plus/Pro placement. No mode can ship until its execution surface is officially
production-supported. The production worker remains Go/serverless and must not
ship a Python SDK/runtime or Python sidecar. Cloud custody of a consumer
credential remains disabled
pending the additional gates in
[the decision record](codex-integration-surface.md).

**There is no API-key fallback.** `account.type != "chatgpt"`,
`account/updated.authMode != "chatgpt"`, an unauthorized response, or lost
credentials make the connection `reauth_required` and stop the run before any
new turn. Sessionless must never inject `OPENAI_API_KEY` to recover a run.

## Evidence vocabulary

- **documented**: stated by official OpenAI documentation;
- **observed**: reproduced locally against the pinned binary/schema without a
  user login;
- **inference**: proposed Sessionless behavior derived from documented or
  observed facts;
- **unknown**: requires a consented account, cloud-dev, or a policy decision.

## Dated evidence matrix

| Claim | Status | 2026-08-22 evidence | Consequence |
| --- | --- | --- | --- |
| App Server is the documented surface for product-embedded lifecycle control and exposes authentication, history, approvals, and streamed events. Its command is experimental and unsupported for production, while a stable API subset is available. | documented | Official [App Server documentation](https://learn.chatgpt.com/docs/app-server) and [integration-surface guidance](https://learn.chatgpt.com/blog/codex-as-a-platform). | App Server is only a provisional implementation boundary; production release remains blocked and the stable SDK is a mandatory comparator. |
| The default transport is newline-delimited JSON over stdio, with `initialize` then `initialized`. | documented + observed | Official protocol docs; isolated local probe returned an initialize result. | A bounded child process can drive one invocation without a listening network port. |
| Selected binary is `codex-cli 0.148.0-alpha.15`. | observed | `/Applications/ChatGPT.app/Contents/Resources/codex --version`. | Pin this exact build in Phase B; fail closed on version/schema drift. |
| Version-specific schema generation works. | documented + observed | `codex app-server generate-json-schema`. Stable v2 SHA-256: `a7cc806f2845736f1176418b97d8eefd239c2e049cb643eee405f1ce07ccb198`; `--experimental` v2 SHA-256: `b4e8157096dd054c008a4f1b538fb6cd8f1f2cb9577a97a4afef59c2296ed608`. | The adapter should use the stable schema only; experimental checksum is provenance, not opt-in. |
| A pristine auth home reports no account and refuses quota/usage reads. | observed | With an empty temporary `HOME` and `CODEX_HOME` and an empty environment: `account/read` returned `account:null, requiresOpenaiAuth:true`; quota and usage returned JSON-RPC `-32600` authentication-required errors. | Missing auth is distinguishable before a turn. No real cache, browser, or device consent was used. |
| ChatGPT browser and device-code login are protocol operations. Device code is beta and must be enabled by the user or workspace. | documented + schema-observed | `account/login/start` accepts `type:"chatgpt"` or `type:"chatgptDeviceCode"`; device-code result carries `loginId`, `verificationUrl`, and `userCode`; completion is `account/login/completed`. | Use standard local browser login when available; device code is an optional headless path. Neither live flow was run in this slice. |
| ChatGPT and API-key billing routes are distinguishable. | documented + schema-observed | `account/read.account.type` is `chatgpt` or `apiKey`; `account/updated.authMode` reports `chatgpt` or `apikey`. Official [authentication documentation](https://learn.chatgpt.com/docs/auth) says API-key auth uses standard API pricing rather than included plan credits. | Require and continuously enforce the ChatGPT route; reject API-key state. |
| Codex persists and refreshes login details. | documented | Login details are cached in `auth.json` under `CODEX_HOME` when file storage is selected, or in a keyring; the file contains tokens and ChatGPT tokens refresh during use. | The credential object is a secret with mutable refresh state, not a YDB payload or immutable build asset. |
| Account and provider observations exist but may be sparse. | documented + schema-observed | `account/read`, `account/rateLimits/read`, `account/rateLimits/updated`, `account/usage/read`, and `thread/tokenUsage/updated`; nullable rate-limit fields and sparse update semantics are in the generated schema. | Missing values stay `unknown`; never synthesize remaining tokens or reset time. |
| A consented subscription can be restored and complete a real cloud-dev turn without switching route. | unknown | Not tested: no browser/device consent, account cache, or cloud credentials were used. | Blocks Phase B acceptance and production release. |
| Hosted use of a personal subscription is permitted and supportable for this product shape. | unknown | Current public docs establish local subscription login but do not establish this multi-tenant hosted use case; managed-workspace permissions and policy may also differ. | Obtain an explicit policy/terms decision before real-user rollout. |
| Image size, cold start, auth restore, first-turn latency, RSS/disk, SIGTERM, and refreshed-cache writeback meet cloud bounds. | unknown | Requires the pinned worker image and consented cloud-dev run. | Measure and publish evidence; do not infer from this macOS probe. |

The local probe used a newly created temporary auth home and a minimal
environment. It did not inspect or copy the operator's Codex/ChatGPT cache and
did not start a login flow.

## Stable one-run protocol

Every connection uses stdio JSONL and a pinned stable schema. The minimum
successful sequence is:

1. `initialize` request, then `initialized` notification.
2. `account/read` with `refreshToken:false`. Continue only when
   `result.account.type == "chatgpt"`.
3. `account/rateLimits/read` and `account/usage/read`. Authentication errors are
   terminal; unavailable or sparse fields become provider observation
   `unknown` rather than zeros.
4. `thread/start` with an invocation workspace and `ephemeral:true`, followed
   by a turn with `approvalPolicy:"never"` and an explicit structured
   `sandboxPolicy` of type `readOnly` whose access is restricted to declared
   roots. Omitting `access` would grant full filesystem read access. App Server
   still contains native tools, so an external container/mount boundary—not the
   approval policy—must prevent host and credential reads. The returned Codex
   thread id is attempt metadata only.
5. `turn/start` with the canonical Sessionless snapshot/event context. The run
   already carries canonical `session_id` and `trigger_event_id`; Codex history
   never becomes product history.
6. Consume `turn/started`, `item/started`, `item/agentMessage/delta`,
   `item/completed`, `thread/tokenUsage/updated`, `error`, and finally
   `turn/completed`.
7. On the local deadline or cancellation, send `turn/interrupt` and require a
   terminal `turn/completed` (`interrupted`, `failed`, or `completed`) before
   the process grace period ends; then kill the whole process group if needed.
8. Reject any `account/updated` route other than `chatgpt`. Before accepting a
   result, repeat `account/read`; an API-key/null/unauthorized state fails the
   attempt without fallback.

Server-initiated `item/commandExecution/requestApproval`,
`item/fileChange/requestApproval`, `item/permissions/requestApproval`,
`mcpServer/elicitation/request`, `item/tool/call`, and
`item/tool/requestUserInput` are security-sensitive. A request not declared in
the invocation allowlist is denied/cancelled and the turn is interrupted. It is
never auto-approved merely because the worker is non-interactive.

## Authentication persistence boundary

The control plane owns only connection state and, where applicable, an opaque
credential/resource reference. It must not receive token bytes in a queue
message, log, artifact, Terraform state, or YDB row.

The first eligible boundary is a user-owned attached worker:

1. The worker creates a connection-scoped, empty `CODEX_HOME`; it never imports
   a developer workstation cache. Standard local browser login is primary.
   Device code is beta and may be used only when explicitly enabled; if neither
   flow is available, connection returns a deterministic unsupported outcome.
2. File credential storage is forced for that isolated local home. Access and
   refresh tokens remain on the worker; the control plane receives only opaque
   resource identity, auth/plan status, non-secret fingerprint, and sanitized
   observations.
3. One resource has one serialized refresh-capable execution stream. Each turn
   uses a fresh execution directory and process, but the worker is the sole
   owner allowed to update the connection credential.
4. Because Codex may refresh during a run, the worker must retain the refreshed
   file atomically. A crash or conflicting generation becomes
   `credential_reseed_required`, never an older-secret retry loop.
5. Disconnect denies new leases first, drains/cancels work, removes
   Sessionless-owned local state, and retains non-secret audit history. It must
   not claim provider-wide token revocation or destroy an unrelated global
   Codex login.

Cloud-vault materialization remains a distinct later mode. If #48 approves it,
the encrypted generation/CAS lifecycle from #59/#60 applies; it is not inferred
from attached-worker feasibility.

On an attached worker, plaintext `auth.json` is allowed in the protected,
persistent, connection-scoped credential home owned by that worker; invocation
workspaces and process state remain fresh. For a later cloud-vault mode, a
materialized `auth.json` is allowed only inside the invocation's protected
temporary filesystem and must be wiped after fenced write-back. It is never a
durable Sessionless database record. OS keyring storage is unsuitable inside
an ephemeral cloud worker unless the runtime provides an explicit
tenant-isolated keyring contract.

## Quota, account, and usage mapping

| Protocol evidence | Sessionless treatment |
| --- | --- |
| `account/read.account.type`, `account/updated.authMode` | Authoritative billing-route guard. Only `chatgpt` is accepted for this adapter. |
| `planType` | Provider-reported entitlement metadata; never sufficient by itself to admit a run. |
| `account/rateLimits/read.rateLimits` and `rateLimitsByLimitId` | Full provider snapshot. Persist the observed fields and timestamp, retaining the provider `limitId`. |
| `account/rateLimits/updated.rateLimits` | Sparse delta. Merge only present values into the last snapshot or refetch; null/absent account metadata does not clear prior evidence. |
| `primary`/`secondary.usedPercent`, `resetsAt`, `windowDurationMins`, `rateLimitReachedType`, `spendControlReached`, `credits` | Map to `available`, `limited`, or `exhausted` only when explicit. Store/reset-block only with a provider-supplied reset. Missing remains `unknown`. |
| `account/usage/read` | Provider account token-activity summary/daily buckets. Store as provider-reported observation with its own confidence/source. |
| `thread/tokenUsage/updated` | Harness-reported per-thread last/total token breakdown. Correlate to the current attempt; do not treat it as remaining provider quota. |
| `error.error.codexErrorInfo` (`unauthorized`, `usageLimitExceeded`) and terminal `turn/completed` | `unauthorized` -> `reauth_required`; explicit usage limit -> provider `exhausted` and scheduler block, with `blocked_until` only if a trustworthy reset was observed. |

Snapshots and deltas can arrive out of order. Phase B must stamp receipt time,
retain raw sanitized provider fields, and fence updates by the active
subscription connection. It must not pool quota across tenants or accounts.

## Synthetic replay fixtures

`test/fixtures/codex-app-server/` contains credential-free JSONL replay recipes
for the stable protocol. Every line is a replay record, not a captured user
session. `kind:"frame"` wraps an exact JSONL wire message and declares its
direction. `kind:"raw"`, `kind:"repeat"`, and `kind:"stall"` describe parser
and deadline inputs without checking in a malformed or megabyte-sized file.

The fixtures cover a successful run, sparse quota merging, reauthentication,
an explicit usage limit, malformed and oversized frames, timeout/interrupt,
unexpected approval/tool requests, and API-key rejection. See the fixture
README for replay semantics.

## Phase B tests and release gates

- Replay every fixture with bounded line size, total output, item count, and
  wall time; unknown methods/fields are either tolerated as data or rejected
  deterministically, never executed.
- Generate the stable schema from the pinned image and compare its SHA-256.
  Experimental API must remain disabled.
- Prove two tenant connections never share `CODEX_HOME`, workspace, process,
  quota state, logs, or refreshed credentials, including warm-runtime reuse.
- Prove model-controlled tools cannot read host home, credential stores,
  sibling workspaces, `/proc` or equivalent platform process paths, or the
  attached worker's persistent credential home. `readOnly` without restricted
  access must fail the policy test.
- Prove API-key state is rejected before `thread/start` and during an active
  turn; inspect the child environment to show no API-key fallback exists.
- Prove timeout and SIGTERM interrupt the turn, terminate descendants, preserve
  the last fenced checkpoint, and do not accept late output.
- In cloud-dev, with explicit consent, measure image bytes, cold start, auth
  restore, first event/first token/turn completion, peak RSS, disk, egress, and
  shutdown. Record the exact account/rate-limit observations without secrets.
- Obtain and record the hosted-use policy/terms decision. A negative or
  ambiguous decision stops the subscription adapter and triggers a deliberate
  alternative-harness design; it does not enable API billing.

## Principal open risks

- App Server is versioned and partly experimental; exact methods and fields can
  drift between Codex releases.
- Public guidance selects App Server for full-harness embedding but recommends
  API keys for ordinary automation. This product deliberately requires a
  different ChatGPT subscription route, so technical success alone remains
  insufficient.
- The App Server command is experimental and unsupported for production; the
  stable Python SDK must be evaluated as a comparator, but is not an eligible
  production implementation because the worker must remain Python-free.
- Device-code authentication is beta and can be disabled by personal/workspace
  settings; standard browser login or an explicit unsupported result is needed.
- Refresh writes make credentials mutable and create crash/fencing problems.
- Rate-limit updates are sparse and may omit resets or whole buckets.
- A compromised worker can read the materialized credential; isolation,
  egress, logging, and secret lifetime are therefore part of the auth boundary.
- Approval and client-side tool requests invert control flow and must be handled
  fail-closed under backpressure, cancellation, and malformed input.
