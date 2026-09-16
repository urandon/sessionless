# OpenAI subscription resource policy

Evidence date: **2026-09-16**

Tracks: [#48](https://gitcode.com/urandon/sessionless/issues/48)

Decision owner: Sessionless maintainers. Re-review by **2026-12-15**, or earlier
when an OpenAI term, plan, authentication surface, Codex runtime, workspace
control, or Sessionless credential-placement assumption changes.

## Decision

Sessionless may offer an opt-in OpenAI subscription resource only through a
reviewed, Python-free `codex exec` backend and only when the identity that owns
the OpenAI entitlement also owns or administers the execution placement.

- **Personal Plus and Pro:** conditional go for a single owner on that owner's
  attached private worker. The ChatGPT credential remains on the worker and
  Sessionless never receives it. No other tenant member may consume the
  entitlement.
- **Personal Free and Go:** no-go for Sessionless execution today. Current
  primary documentation says Codex is included, but does not establish the
  same non-interactive/scriptable entitlement with enough plan-specific
  precision to admit `codex exec` as a product resource.
- **Education workspace member:** conditional go only for owner-local execution
  after a workspace administrator confirms both Codex and the exact scriptable
  surface are enabled. Business/Enterprise access-token availability is not
  generalized to Education.
- **Business and Enterprise member identity:** conditional go on a trusted
  private worker using a workflow-specific Codex access token when the
  workspace permits it. A browser login may be used only for an owner-local
  interactive setup, not as centrally managed automation.
- **Eligible managed workspace service account:** conditional go for
  organization-owned automation using its own access token. Service accounts
  are currently limited to pay-as-you-go plans.
- **Eligible managed workspace workload identity:** preferred conditional go
  for cloud, Kubernetes, or CI placement. OpenAI's WIF support is beta and must
  be enabled for the workspace, so absence of an enabled, exact federation
  rule is no-go.
- **OpenAI Platform API key:** go only as a separately billed API resource. It
  is never a subscription fallback.
- **Personal credential in Sessionless cloud, household/federation sharing,
  shared personal access tokens, copied competitor OAuth clients, private
  endpoint emulation, cookies, and silent API fallback:** no-go.
- **Codex App Server:** research only. OpenAI still states that the command and
  WebSocket transport are experimental and unsupported for production.

`conditional go` authorizes design and the bounded implementation backlog. It
does not enable the disabled runtime backend. Admission remains fail closed
until the attached-worker, isolation, provider-egress, lifecycle, and exact
artifact gates listed below are satisfied.

## Evidence method and limits

The verdict uses current OpenAI primary sources for policy and product claims,
plus pinned competitor source only for mechanism comparison. Competitor code
cannot authorize an OpenAI usage shape.

| Evidence | Current documented fact | Consequence |
| --- | --- | --- |
| OpenAI [authentication](https://learn.chatgpt.com/docs/auth) | ChatGPT sign-in provides subscription access; API-key sign-in is usage-based. Local CLI supports both. Cached ChatGPT credentials refresh automatically and can be stored in a file, keyring, or memory. | The billing authority is explicit and must be pinned; local credential custody is a supported mechanism. |
| OpenAI [non-interactive mode](https://learn.chatgpt.com/docs/non-interactive-mode) and [pricing](https://learn.chatgpt.com/docs/pricing) | `codex exec` is the documented scriptable surface and API-key automation has separate API billing. Pricing says Codex is included across current ChatGPT plans, but the public feature table does not establish equal scriptable entitlement for Free and Go with the precision required here. | `codex exec` is the only current Python-free subscription candidate. Plus/Pro may proceed conditionally; Free/Go remain no-go until plan-specific evidence closes the gap. |
| OpenAI [advanced CI/CD account authentication](https://learn.chatgpt.com/docs/auth/ci-cd-auth) | Account-auth automation is documented for trusted private runners, with one current `auth.json` copy and serialized refresh ownership. API keys remain the recommended default for ordinary CI. | Owner-private account automation is conditionally admissible; multi-tenant credential custody is not established. |
| OpenAI [access tokens](https://learn.chatgpt.com/docs/enterprise/access-tokens) | Business and Enterprise may issue Codex-scoped credentials for trusted non-interactive local workflows. Tokens belong to a member or service account and require trusted runners, narrow scope, rotation, and revocation. | Managed identities have a documented automation path; shared personal tokens do not. |
| OpenAI [service accounts](https://learn.chatgpt.com/docs/enterprise/service-accounts) | A service account is a non-human workspace identity, administered by workspace owners/admins, currently only on pay-as-you-go plans. | Organization automation must use its own identity rather than an employee credential where available. |
| OpenAI [workload identity federation](https://learn.chatgpt.com/docs/enterprise/workload-identity) | Managed workspaces can exchange OIDC or SPIFFE assertions for an in-memory OpenAI token lasting at most one hour. The feature is beta and requires enablement. | WIF is preferred for eligible cloud workers; a missing/invalid rule fails closed without a credential fallback. |
| OpenAI [App Server](https://learn.chatgpt.com/docs/app-server) | The command and WebSocket transport remain experimental and unsupported for production; individual methods also have explicit maturity. | Keep App Server fixtures for research, never select it for this MVP. |
| OpenAI [account sharing policy](https://help.openai.com/en/articles/10471989-openai-account-sharing-policy) and [Terms of Use](https://openai.com/policies/row-terms-of-use/) | An individual account is for its creator; credentials and account access may not be shared. Multiple devices used by that individual are allowed, subject to limits. | A personal subscription cannot become a tenant pool, household resource, or centrally shared credential. |
| OpenAI [Services Agreement](https://openai.com/policies/services-agreement/) | Business customers may integrate the API in applications, but account and individual login credentials may not be shared between users or resold. | Managed workspace membership and distinct identities are mandatory; API integration rights do not convert a ChatGPT login into a shared app credential. |
| OpenAI [supported countries and territories](https://help.openai.com/en/articles/7947663-chatgpt-supported-countries) | Access is geographically constrained and access outside supported locations may lead to suspension. | Enrollment and execution must be in supported locations; Sessionless must not route around a geographic restriction. |
| OpenAI [Terms of Use](https://openai.com/policies/row-terms-of-use/) | A user must be at least 13 or the minimum age of consent in their country; users under 18 need parent or guardian permission. | Provider login alone is not age evidence. Product onboarding must establish eligibility or leave the resource unavailable. |
| OpenAI [enterprise privacy](https://openai.com/enterprise-privacy/) and [authentication](https://learn.chatgpt.com/docs/auth) | Business workspace and API organization data controls are route-specific; a ChatGPT workspace and an API organization are different control planes. | Consent and audit must name the actual route and workspace. Sessionless must not infer API privacy/billing properties for a ChatGPT subscription, or vice versa. |

This review does not claim that OpenAI guarantees a fixed message quota, a
specific model, uninterrupted service, or a stable price. It also does not
interpret a successful experimental request as a policy decision.

### Plan-by-plan verdict

| Plan | Personal/member login on owner-local worker | Managed automation identity | Current verdict |
| --- | --- | --- | --- |
| Free | Public docs say Codex is included, but do not establish exact scriptable entitlement. | None established. | **No-go pending evidence.** |
| Go | Public docs say Codex is included, but do not establish exact scriptable entitlement. | None established. | **No-go pending evidence.** |
| Plus | Documented ChatGPT login plus `codex exec`, with owner-local credential custody. | None established for a personal plan. | **Conditional go, owner only.** |
| Pro | Documented ChatGPT login plus `codex exec`, with owner-local credential custody. | None established for a personal plan. | **Conditional go, owner only.** |
| Business | Member-local login is allowed for that member; documented Codex access tokens support trusted non-interactive workflows when enabled. | Member token; pay-as-you-go service account where eligible; WIF when enabled. | **Conditional go under the matching managed tuple.** |
| Education | Member-local login only after the workspace admin confirms Codex and exact scriptable access. | Access-token/service-account availability is not established by the reviewed docs; WIF only if explicitly enabled for that workspace. | **Conditional local go; managed automation otherwise no-go pending evidence.** |
| Enterprise | Member-local login is allowed for that member; documented Codex access tokens support trusted non-interactive workflows when enabled. | Member token; pay-as-you-go service account where eligible; WIF when enabled. | **Conditional go under the matching managed tuple.** |

## Versioned policy records

Every admitted resource references one immutable record below. `unknown`, an
expired record, a plan/surface mismatch, or a missing condition evaluates to
no-go. A policy refresh creates a new record; it never mutates an in-flight
attempt.

| ID | Plan and identity | Surface | Placement / custodian / sharing | Verdict | Conditions |
| --- | --- | --- | --- | --- | --- |
| `OAI-SUB-2026-09-PLUS-PRO-LOCAL` | Plus or Pro; individual owner | pinned `codex exec --json --ephemeral` | owner-attached private worker / owner-local credential-only home / owner only | **conditional go** | Explicit owner consent; exact ChatGPT route; one refresh owner; supported isolation and egress; no other beneficiary; no API fallback. |
| `OAI-SUB-2026-09-FREE-GO-LOCAL` | Free or Go; individual owner | pinned `codex exec --json --ephemeral` | owner-attached private worker / owner-local credential-only home / owner only | **no-go pending evidence** | Public evidence does not establish plan-specific non-interactive entitlement precisely enough. A new dated record is required; a successful call is insufficient. |
| `OAI-SUB-2026-09-EDU-MEMBER-LOCAL` | Education workspace member | pinned `codex exec --json --ephemeral` | member-attached private worker / member-local credential-only home / that member only | **conditional go** | Workspace admin attests Codex and exact scriptable surface are enabled; member consent; exact ChatGPT route; one refresh owner; no access-token assumption; no fallback. |
| `OAI-SUB-2026-09-PERSONAL-CLOUD` | Any personal plan | any | Sessionless-hosted multi-tenant worker / Sessionless vault or copied `auth.json` / owner or others | **no-go** | Official docs describe trusted private automation, not Sessionless custody of consumer credentials in a shared SaaS plane. |
| `OAI-SUB-2026-09-PERSONAL-SHARED` | Any personal plan | any | any / any / another person, household, team, or federation | **no-go** | Conflicts with individual-account and credential-sharing rules. |
| `OAI-SUB-2026-09-BIZ-MEMBER` | Business or Enterprise member | pinned `codex exec` with Codex access token | trusted private member worker / approved secret store / token creator's workflow only | **conditional go** | Admin enables creation; local Codex permission; least scope; finite expiry; workflow owner; audit and revoke path. |
| `OAI-SUB-2026-09-BIZ-SERVICE` | eligible pay-as-you-go managed workspace service account | pinned `codex exec` with service-account token | trusted organization worker / approved secret store / authorized workspace workload | **conditional go** | Admin-owned non-human identity; dedicated workflow; group/role limits; rotation, audit, revoke, and offboarding tests. |
| `OAI-SUB-2026-09-BIZ-WIF` | eligible managed workspace user or service account | pinned `codex exec` with WIF | rule-bound cloud/CI/cluster worker / no long-lived OpenAI credential / authorized workload only | **conditional go** | Beta enabled by OpenAI; exact issuer/audience/subject rule; protected assertion file; replay protection where available; fail-closed exchange. |
| `OAI-SUB-2026-09-APP-SERVER` | any | `codex app-server` | any | **no-go** | OpenAI explicitly marks the command unsupported for production. |
| `OAI-API-2026-09-EXPLICIT` | API organization/project | Codex CLI/SDK or later reviewed API adapter | declared worker / project key or WIF / project ACL | **go as a different resource** | Separate consent, billing, data controls, budget, and attempt; never automatic fallback. |

Decision records include `evidence_observed_at=2026-09-16`,
`expires_at=2026-12-15`, the exact document URLs above, the maintainer decision
owner, and these early re-review triggers:

- OpenAI changes account sharing, Terms, Services Agreement, Codex auth,
  access-token, WIF, service-account, pricing, or App Server guidance;
- the selected Codex executable, argv, auth storage, route signal, quota output,
  or refresh behavior changes;
- Sessionless changes placement, custodian, beneficiaries, isolation, egress,
  or fallback semantics;
- an incident, suspension, unexplained billing event, or ambiguous credential
  refresh occurs.

## Ownership and authorization model

`SubscriptionResource` is an `AIResource`, not a model or loose credential.
It binds one entitlement owner, one admitted execution shape, and one credential
generation:

```text
SubscriptionResource {
  tenant_id, resource_id, revision
  provider = "openai", account_plan, workspace_id?
  identity_kind: individual | workspace_member | service_account
  owner_principal_id, beneficiary_principal_ids
  execution_placement_id, credential_custodian
  credential_ref, credential_generation
  policy_evidence_id, policy_expires_at
  harness_id, harness_digest, model_policy_revision
  quota_observation_revision, lifecycle_state
}
```

The control plane authorizes membership and resource use, but it never proves
the OpenAI identity. The worker proves possession of the already-enrolled local
resource and accepts only a fenced attempt whose tenant, owner, worker,
connection, resource revision, credential generation, harness, lease, and
policy record all match. For personal resources,
`beneficiary_principal_ids == [owner_principal_id]` is invariant.

### Personal attached-worker flow

```mermaid
sequenceDiagram
    actor Owner
    participant CP as Sessionless control plane
    participant W as Owner-attached worker
    participant C as pinned codex exec
    participant O as OpenAI

    Owner->>W: local ChatGPT login / explicit consent
    W->>CP: advertise opaque resource revision + readiness
    CP->>CP: membership + owner + policy + lease admission
    CP->>W: fenced attempt (no credential bytes)
    W->>W: materialize credential-only private CODEX_HOME
    W->>C: one ephemeral no-tool turn
    C->>O: authenticated ChatGPT route
    O-->>C: events / route and quota observations when exposed
    C-->>W: bounded JSONL terminal result
    W->>W: finalize refresh state, scrub invocation material
    W-->>CP: sanitized evidence + terminal candidate
    CP->>CP: fenced canonical commit
```

### Managed workload-identity flow

```mermaid
sequenceDiagram
    participant IdP as Trusted OIDC/SPIFFE issuer
    participant W as Rule-bound worker
    participant O as OpenAI token exchange
    participant C as pinned codex exec
    participant CP as Sessionless control plane

    IdP-->>W: short-lived assertion in protected file
    W->>O: exchange under exact federation rule
    O-->>W: in-memory OpenAI token (max 1 hour)
    CP->>W: fenced admitted attempt
    W->>C: WIF environment + one ephemeral turn
    C-->>W: bounded result and observations
    W-->>CP: sanitized evidence only
    Note over W,O: exchange failure has no stored-credential fallback
```

### Authorization, refresh, scheduling, quota, use, and revocation

```mermaid
flowchart TD
    A[Owner/admin authorizes exact resource] --> B[Worker reports opaque ready generation]
    B --> C{Scheduler admission}
    C -->|policy/owner/lease/isolation fail| D[Deny before spawn]
    C -->|exact tuple passes| E[Pin resource + credential generation]
    E --> F{Credential current?}
    F -->|no| G[Single refresh owner refreshes]
    G -->|refresh/finalize ambiguous| H[reauth_required; block replay]
    G -->|atomic finalize| I[Observe route + quota with provenance]
    F -->|yes| I
    I -->|known quota required; observation unknown/stale| D
    I -->|eligible| J[One ephemeral codex exec use]
    J --> K[Sanitized terminal + lifecycle observation]
    K --> L{Disconnect/revoke/offboard?}
    L -->|no| M[Release lease; ready]
    L -->|yes| N[Deny new work; drain/cancel; fence generation]
    N --> O[Delete Sessionless materializations]
    O --> P[Owner/admin performs provider logout/revoke]
```

## Credential lifecycle

Personal resources have exactly one refresh owner. A file-backed login can move
only between placements owned by the same individual, through an explicit
user-controlled reseed operation; Sessionless never uploads, downloads, backs
up, logs, or displays it.

Lifecycle states are `pending_login -> ready -> leased -> draining -> ready`,
with terminal branches `reauth_required`, `revoked`, and `disconnected`.

1. Enrollment records the opaque resource and selected policy record.
2. The user logs in locally; the worker reports readiness without token bytes.
3. Admission pins credential generation. Only one refresh-capable lease may be
   active for personal file-backed auth.
4. Codex refreshes during the invocation. The worker atomically finalizes the
   credential-only home before releasing the lease.
5. A refresh/finalization ambiguity blocks replay and moves the resource to
   `reauth_required` or manual reconciliation.
6. Disconnect denies new attempts first, drains/cancels owned work, deletes
   Sessionless-owned materializations, and asks the owner/admin to revoke or
   log out through the provider-supported control.
7. Access-token expiry/revocation and disabled WIF rules block future exchanges;
   deleting a local file is not presented as provider-wide revocation.

No token, refresh token, cookie, authorization header, raw `auth.json`, WIF
assertion, or provider error body may enter YDB, Object Storage, queues, audit
payloads, logs, metrics, Git, CI artifacts, or canonical Session events.

## Scheduling, quota, and fallback

Eligibility is the intersection of tenant membership, exact owner/beneficiary,
unexpired policy record, credential generation, worker/connection health,
isolation and egress attestations, harness digest, consent, and budget. Missing
quota does not become infinity: it is `unknown` and may admit only the bounded
owner-local preview policy that explicitly tolerates unknown preflight quota.

The run pins one resource before the attempt. A 401, 403, 429, exhausted usage
window, revoked WIF rule, token expiry, or route mismatch changes resource
availability and ends that attempt under its retry classification. It never
selects an API key, another member's token, another subscription, a smaller
paid model, or a different provider mid-attempt. A user-approved new resource
can be selected only for a new attempt with a new audit record.

Quota observations preserve provider values, provenance, observation time,
reset time, and coverage. Pricing estimates and plan marketing ranges are not
authoritative remaining capacity. The UI distinguishes `observed`, `estimated`,
`reconciled`, and `unknown` and shows that local and cloud Codex usage may share
the same subscription allowance.

## Geography, age, privacy, and provider relationship boundaries

- **Geography:** enrollment records the declared provider/workspace region and
  execution location. Unknown or unsupported locations are unavailable.
  Sessionless never selects a proxy or alternate placement to evade a provider
  country restriction.
- **Age:** a provider credential is not proof of age or guardian permission.
  Tenant onboarding owns the eligibility assertion; an absent assertion denies
  subscription-resource enrollment. Sessionless does not retain identity
  documents in the resource record.
- **Privacy:** the consent screen names whether data goes to a personal ChatGPT
  account, a managed ChatGPT workspace, or an API organization. Their retention,
  training, residency, and administrator controls are not interchangeable.
  Prompt and attachment content never appears in the policy audit record.
- **Provider relationship:** no special partnership is assumed for an owner
  using OpenAI's documented local CLI/`codex exec` flow on their own attached
  worker. WIF requires provider enablement; managed access tokens require the
  documented workspace controls. Personal cloud custody, shared-beneficiary
  access, or any undocumented backend would require explicit new provider
  authorization and a new reviewed policy record, not an inference from a
  working competitor.

## UX and consent contract

Before connect or first use, show the exact:

- OpenAI plan/workspace and identity kind without exposing credentials;
- owner and permitted beneficiary set;
- attached/private/managed execution placement and credential custodian;
- content and attachment egress destination;
- pinned `codex exec` harness and its no-tool MVP profile;
- no-fallback rule and separate API-billing warning;
- quota freshness and unknown fields;
- disconnect, reauthentication, token expiry, and admin-revocation behavior;
- policy evidence version and expiry.

Consent is versioned and bound to the resource revision. A placement, custodian,
beneficiary, egress, harness, or billing-route change requires new consent and
new admission. Enrollment must never offer household/team sharing for a
personal resource or imply that Sessionless can revoke an OpenAI account.

## Threat and failure model

| Threat / failure | Required control | Fail-closed outcome |
| --- | --- | --- |
| Another tenant or member consumes a personal entitlement | Owner-only beneficiary invariant, membership and resource ACL, fenced worker authority | `owner_mismatch`; no process spawn. |
| Credential exfiltration through prompts, logs, dumps, or ambient home | Credential-only private home/keyring; replacement environment; log allowlist; core-dump denial; no control-plane bytes | Stop, scrub invocation material, revoke/reseed as applicable. |
| Two runners refresh one mutable login | Single refresh owner and exclusive lease; CAS generation finalization | Ambiguous generation blocks reuse and replay. |
| Billing route silently becomes API | Expected `chatgpt` route pinned; no `CODEX_API_KEY`; replacement environment | `billing_route_mismatch`; terminal failure, no fallback. |
| WIF falls back to a long-lived credential | WIF-only environment and OpenAI fail-closed precedence | Exchange failure; no Codex spawn. |
| Stolen access token or assertion | Trusted runner, protected secret/file, narrow scope, short lifetime, issuer/rule constraints, replay protection, rotation | Revoke token/rule and fence its credential generation. |
| Provider accepts a request but terminal outcome is lost | Native acceptance boundary plus fenced ambiguous completion; no internal retry | Reconcile/manual decision; never duplicate blindly. |
| Quota is stale or absent | Typed freshness/provenance and bounded unknown-quota policy | No broad capacity promise; deny policies that require known headroom. |
| Policy or terms drift | Expiring immutable policy record and early re-review triggers | New admissions denied at expiry. |
| Unsupported App Server path leaks into production | Closed harness registry and exact backend ID/digest | Registry rejection before execution. |
| Account/member/service account is offboarded | Admin/user revoke plus Sessionless generation fence and deny-first disconnect | New attempts denied; bounded drain/cancel. |

## Cost model

Personal plan cost is an external subscription paid by the owner. Sessionless
must not convert marketing message ranges into money or promise unused quota.
Its attributable cost is the attached-worker CPU/RAM/disk/network and platform
control-plane work, recorded separately from provider entitlement use.

Managed workspace access may be seat-, credit-, or pay-as-you-go-based. The
resource records the workspace billing authority and reconciles only against
available admin/provider evidence. Service-account and WIF eligibility does not
imply that use is included or free. API-key usage remains usage-based API cost.

Admission budgets therefore remain separate:

1. provider entitlement/quota observation;
2. workspace credits or API monetary budget when authoritative;
3. Sessionless control-plane and network budget;
4. attached/managed worker capacity and operator cost.

## Competitor mechanism audit

### OpenCode pinned-source trace

The OpenCode comparison is source-level and pinned to
[`3a31c4e`](https://github.com/anomalyco/opencode/tree/3a31c4ea801915c0b050df4b3842997ea62b6e93).
It proves implementation mechanics only:

| Concern | Pinned source observation | Sessionless consequence |
| --- | --- | --- |
| Origin and storage | [`auth/index.ts`](https://github.com/anomalyco/opencode/blob/3a31c4ea801915c0b050df4b3842997ea62b6e93/packages/opencode/src/auth/index.ts#L10-L20) stores OAuth refresh/access/expiry/account ID in global `auth.json`; `OPENCODE_AUTH_CONTENT` can replace the file, and writes use mode `0600` ([lines 58–89](https://github.com/anomalyco/opencode/blob/3a31c4ea801915c0b050df4b3842997ea62b6e93/packages/opencode/src/auth/index.ts#L58-L89)). | This is provider-global local state, not a tenant-scoped vault or proof that Sessionless may copy it. |
| Authorization | [`plugin/openai/codex.ts`](https://github.com/anomalyco/opencode/blob/3a31c4ea801915c0b050df4b3842997ea62b6e93/packages/opencode/src/plugin/openai/codex.ts#L10-L16) embeds an OAuth client, private Codex backend URL, and allow/deny model sets. Browser auth uses PKCE/state; headless auth polls device endpoints ([lines 436–553](https://github.com/anomalyco/opencode/blob/3a31c4ea801915c0b050df4b3842997ea62b6e93/packages/opencode/src/plugin/openai/codex.ts#L436-L553)). | Do not copy the client, endpoint, device flow, cookies, headers, or entitlement assumptions. Use the official Codex executable. |
| Persistence boundary | [`provider/auth.ts`](https://github.com/anomalyco/opencode/blob/3a31c4ea801915c0b050df4b3842997ea62b6e93/packages/opencode/src/provider/auth.ts#L163-L220) keeps the pending OAuth callback in process memory, then writes either API-key or OAuth material to the global auth store. | Login completion is not resource enrollment: Sessionless still needs owner, tenant, placement, generation, policy, and consent bindings. |
| Refresh | The Codex plugin coalesces concurrent in-process refresh with one promise, writes rotated tokens back, injects account headers, and rewrites response requests to the private backend ([lines 327–433](https://github.com/anomalyco/opencode/blob/3a31c4ea801915c0b050df4b3842997ea62b6e93/packages/opencode/src/plugin/openai/codex.ts#L327-L433)). | In-process coalescing does not fence multiple hosts or crash ambiguity. Sessionless requires one leased refresh owner and atomic credential-generation finalization. |
| Discovery and entitlement | The plugin filters a catalog with hard-coded model rules and zeroes displayed OAuth cost ([lines 288–323](https://github.com/anomalyco/opencode/blob/3a31c4ea801915c0b050df4b3842997ea62b6e93/packages/opencode/src/plugin/openai/codex.ts#L288-L323)); the provider layer applies plugin filtering to catalog data before loading auth options ([`provider.ts` lines 1415–1454](https://github.com/anomalyco/opencode/blob/3a31c4ea801915c0b050df4b3842997ea62b6e93/packages/opencode/src/provider/provider.ts#L1415-L1454)). | Catalog presence, zero displayed cost, and a successful request are not authoritative model entitlement, remaining quota, or billing evidence. |
| Quota, logout, and revoke | The audited Codex plugin contains no provider-quota read or provider revocation call. The auth store exposes local record removal only ([`auth/index.ts` lines 83–89](https://github.com/anomalyco/opencode/blob/3a31c4ea801915c0b050df4b3842997ea62b6e93/packages/opencode/src/auth/index.ts#L83-L89)). | Treat quota as unknown absent an official observation. Local deletion is disconnect/logout hygiene, never provider-wide revocation evidence. |
| Runtime placement | Auth loading, refresh, request rewriting, and provider calls occur inside the local OpenCode process; the provider layer loads the stored plugin auth into that runtime ([`provider.ts` lines 1583–1602](https://github.com/anomalyco/opencode/blob/3a31c4ea801915c0b050df4b3842997ea62b6e93/packages/opencode/src/provider/provider.ts#L1583-L1602)). | Useful evidence for credential locality, but not for multi-tenant cloud custody or federation. |

The gap is deliberate: OpenCode optimizes a local client, while Sessionless
needs durable tenant authorization, scheduler admission, quota provenance,
crash-safe refresh ownership, and remote offboarding. Its implementation is
therefore competitor evidence, not a dependency or policy authority.

| System / pinned revision | Useful mechanism evidence | Rejected transfer |
| --- | --- | --- |
| OpenCode `3a31c4ea801915c0b050df4b3842997ea62b6e93` | Separates stored auth, provider catalog, plugins, and provider discovery; demonstrates technical Codex subscription access. | OAuth client reuse, private backend calls, provider-global credential records, or treating a successful call as OpenAI authorization. |
| Zed `d9ad6aff67e47de43abb270d22de75dd950f1b48` | Separates direct provider, ACP child process, and terminal-owned CLI; keeps credentials near execution. | Direct OAuth/backend implementation as permission for Sessionless. |
| Hermes Agent `c80a0a551c7038517456ee0aeb60203ec92aedb6` | Shows refresh locking/write-back, provider pools, usage projection, and unattended lifecycle hazards. | Soft over-cap leasing, shared personal credential pools, broad environment forwarding, warm provider history, or Python production closure. |
| DeepSeek Harness `b150a551b8d465e31e418e1b2eaf5e79bbb7d28e` | Shows plugin-seamed harnesses, provider-neutral logs, one-shot permissions, and explicit preview maturity. | Assuming file-only sandboxing, preview wire stability, or a harness identity proves provider entitlement. |

## ADR: selected and rejected options

Accepted:

- `codex exec --json --ephemeral` below Sessionless's Go `HarnessDriver`;
- personal owner-local attached worker with credential-local custody;
- workspace access token/service account for trusted managed workflows;
- WIF as preferred managed cloud credential when enabled;
- immutable policy tuples, exact billing-route pinning, and no fallback;
- one external process and one ephemeral provider turn per fenced Attempt.

Rejected:

- App Server in production while OpenAI marks it unsupported;
- Python SDK/runtime in the production worker;
- consumer `auth.json` in Sessionless cloud;
- shared personal accounts/tokens, household/federation pools, warm cross-user
  workers, and multiple refresh owners;
- copied cookies, private endpoints, competitor OAuth clients, or direct OAuth
  token endpoint calls;
- treating API keys, workspace access tokens, service accounts, WIF, and
  personal subscription login as interchangeable credentials;
- silent model/provider/billing fallback and runtime discovery from ambient
  environment variables.

## Subscription Resource implementation epic

The research decision closes #48 when accepted. Runtime activation remains a
separate, reversible sequence:

| Work item | Estimate | Dependencies | Acceptance evidence |
| --- | ---: | --- | --- |
| SR-01 policy-evidence registry | 5 SP | this decision, #46 | Immutable tuple/version/expiry records; stale or mismatched records deny admission; audit contains ID, never secret; #46 supplies the policy vocabulary and enforcement boundary. |
| SR-02 subscription resource and credential-generation contract | 5 SP | canonical AI resource model, credential lifecycle | Owner/beneficiary invariants, single refresh owner, generation fencing, deny-first disconnect, race and crash tests. |
| SR-03 enrollment and consent UX/API | 5 SP | #46, #47, SR-01/02 | Exact placement, custodian, egress, billing, no-fallback, quota freshness, revoke semantics, and policy expiry are shown and versioned. |
| SR-04 activate pinned Go-supervised Codex exec bridge | 8 SP | #9, #46, #81, attached-worker authority | Closed registry binding from admitted immutable resource; scheduler selects only an eligible exact resource; exact binary/argv/digest; one turn; no ambient auth or API fallback. |
| SR-05 production isolation and provider egress | 8 SP | #46, attached-worker isolation/egress tracks | Credential-only home, replacement environment, reviewed network allowlist/proxy, process-group cleanup, secret/log/dump tests on supported OS. |
| SR-06 account, route, quota, and lifecycle observations | 5 SP | #9, #49, SR-04 | Scheduler receives typed availability; auth route and mismatch are authoritative; quota has provenance/freshness; 401/403/429/revoke/expiry classifications are deterministic. |
| SR-07 adversarial owner and recovery E2E | 8 SP | SR-01–06 | Two owners, stale policy, stale generation, disconnect, refresh ambiguity, cancellation, lost terminal, WIF failure, and no-fallback paths fail closed. |
| SR-08 managed identity profiles | 8 SP | SR-01–07 | Separate member-token, service-account, and WIF profiles; admin enablement, scope, audit, rotation/revoke, and WIF replay/expiry evidence. |

Issue #46 is a release dependency because subscription resources need the same
content, tool, egress, and deny semantics as every other AI resource. Issue #9
is a release dependency because scheduler admission must select the exact
resource revision without fallback. Fake-adapter and schema work may proceed in
parallel, but no provider activation may bypass either dependency.

Rollout is fake process and credential-free conformance -> personal owner-local
canary with no tools/private data -> bounded opt-in attached-worker preview ->
managed token/service-account preview -> beta WIF preview. Each stage has an
independent kill switch that marks the exact resource/harness/policy revision
unavailable. Rollback never chooses another provider or billing resource.

Success evidence is zero cross-owner admissions, zero credential bytes outside
the worker secret boundary, zero silent billing fallbacks, exact cancellation
and ambiguous-completion classification, policy/consent coverage for every
admitted attempt, quota freshness coverage, and provider-versus-platform error
attribution. A live provider experiment is follow-up evidence, not a prerequisite
for accepting this policy design.

## Remaining questions

1. Which Business/Enterprise pricing configurations expose service accounts,
   credits, and usage APIs in each target customer workspace?
2. Will OpenAI graduate Codex WIF and/or App Server to production support with
   stability guarantees that change the selected surface?
3. Which supported attached-worker operating systems can enforce the complete
   credential, filesystem, process, and egress profile without a Linux VM?
4. Which Codex output is sufficiently authoritative to attest ChatGPT billing
   route and quota without using private endpoints?
5. What exact user-facing name should distinguish an included subscription
   allowance from purchased ChatGPT credits and separately billed API usage?

These questions gate their corresponding runtime profiles or UX claims. They
do not reopen the no-sharing/no-cloud-consumer-custody decisions.
