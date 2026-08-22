# Codex subscription worker: Phase A evidence and protocol contract

Status date: **2026-08-22**. Scope: issue [#13](https://gitcode.com/urandon/sessionless/issues/13), Phase A only.

## Verdict

The local machine-protocol slice is **viable**: the selected Codex App Server
starts with a fresh isolated `CODEX_HOME`, completes the JSONL handshake, and
exposes the account, quota, usage, thread, turn, cancellation, approval, and
tool-call protocol surfaces needed by an adapter.

The subscription-backed cloud worker is **not yet approved for production**.
Cloud-dev consent/auth restore, refresh persistence, resource measurements, and
the policy/terms basis for a hosted third-party service using a person's
ChatGPT subscription remain unproved. Official guidance also says to prefer the
Codex SDK for automated jobs, while the required subscription/account signals
currently live on App Server. That conflict is a release gate, not permission
to silently change the billing route.

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
| App Server is intended for embedded clients and exposes authentication, history, approvals, and streamed events. | documented | Official [App Server documentation](https://learn.chatgpt.com/docs/app-server). It separately recommends the SDK for automated jobs/CI. | App Server is technically relevant, but hosted automation policy/support remains a gate. |
| The default transport is newline-delimited JSON over stdio, with `initialize` then `initialized`. | documented + observed | Official protocol docs; isolated local probe returned an initialize result. | A bounded child process can drive one invocation without a listening network port. |
| Selected binary is `codex-cli 0.148.0-alpha.15`. | observed | `/Applications/ChatGPT.app/Contents/Resources/codex --version`. | Pin this exact build in Phase B; fail closed on version/schema drift. |
| Version-specific schema generation works. | documented + observed | `codex app-server generate-json-schema`. Stable v2 SHA-256: `a7cc806f2845736f1176418b97d8eefd239c2e049cb643eee405f1ce07ccb198`; `--experimental` v2 SHA-256: `b4e8157096dd054c008a4f1b538fb6cd8f1f2cb9577a97a4afef59c2296ed608`. | The adapter should use the stable schema only; experimental checksum is provenance, not opt-in. |
| A pristine auth home reports no account and refuses quota/usage reads. | observed | With an empty temporary `HOME` and `CODEX_HOME` and an empty environment: `account/read` returned `account:null, requiresOpenaiAuth:true`; quota and usage returned JSON-RPC `-32600` authentication-required errors. | Missing auth is distinguishable before a turn. No real cache, browser, or device consent was used. |
| ChatGPT browser and device-code login are protocol operations. | documented + schema-observed | `account/login/start` accepts `type:"chatgpt"` or `type:"chatgptDeviceCode"`; device-code result carries `loginId`, `verificationUrl`, and `userCode`; completion is `account/login/completed`. | `/connect codex` can own a device-code UX, but the live flow was deliberately not run in this slice. |
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
4. `thread/start` with an invocation workspace, `ephemeral:true`,
   `sandbox:"read-only"`, and `approvalPolicy:"never"`. Phase A exposes no
   built-in tools and grants no workspace writes. The returned Codex thread id
   is attempt metadata only.
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

The control plane owns only connection state and an opaque secret reference.
It must not receive token bytes in a queue message, log, artifact, Terraform
state, or YDB row.

The proposed boundary for the feasibility implementation is:

1. `/connect codex` creates a connection-scoped, empty `CODEX_HOME`; it never
   imports a developer workstation cache. The user completes the documented
   `chatgptDeviceCode` flow through a Sessionless-owned consent UI.
2. File credential storage is forced for that isolated home so the resulting
   mutable credential object has an explicit boundary. Immediately ingest it
   as opaque encrypted secret material; YDB stores only tenant/connection,
   secret reference/version, auth mode, plan observation, timestamps, and a
   non-secret fingerprint.
3. One worker invocation creates a new mode-`0700`, connection-scoped
   `CODEX_HOME` and workspace under a dedicated uid, materializes exactly one
   tenant's secret, and removes all API-key environment variables. Warm reuse
   must start from a new directory, not clean selected files in place.
4. Because Codex may refresh tokens during a run, changed credential material
   must be re-encrypted and persisted with connection-version fencing before
   teardown. The exact flush/atomicity contract is **unknown** and requires a
   crash/refresh experiment; until proven, a refresh race produces
   `reauth_required`, never an older-secret retry loop.
5. Disconnect disables future materialization, deletes/revokes the stored
   secret under an audited workflow, calls `account/logout` only on disposable
   materialized copies when useful, and retains non-secret audit history.

OS keyring storage is unsuitable inside an ephemeral worker unless the cloud
runtime provides an explicit tenant-isolated keyring contract. Plaintext
`auth.json` is allowed only inside the invocation's protected temporary
filesystem; it is never a durable Sessionless database record.

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
- Public guidance prefers SDK/API-key automation, while this product requires
  ChatGPT subscription semantics. Technical success alone is insufficient.
- Device-code availability can be disabled by personal/workspace settings.
- Refresh writes make credentials mutable and create crash/fencing problems.
- Rate-limit updates are sparse and may omit resets or whole buckets.
- A compromised worker can read the materialized credential; isolation,
  egress, logging, and secret lifetime are therefore part of the auth boundary.
- Approval and client-side tool requests invert control flow and must be handled
  fail-closed under backpressure, cancellation, and malformed input.
