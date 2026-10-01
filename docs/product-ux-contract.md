# Product UX contract

Status: product design baseline with owner-accepted initial scope; runtime capability is not implied.
Owner: [#138](https://gitcode.com/urandon/sessionless/issues/138).
Companions: [design rules](design-rules.md), [admin authority](tenant-federation-admin-contract.md),
[delivery roadmap](product-ux-roadmap.md).

Source provenance: wiki snapshot inspected on 2026-10-01 at
`1a6b208468dc99a53a9eb9f4e1444e9716a8aad4`; documentation delivery base
`a463e9780651667be8a28b0bdce339688c7ad453`. Relevant domain/ports/BFF/Svelte,
blob-delivery and existing UX paths were unchanged between these revisions.
File/line evidence below is pinned to the inspection snapshot, not a promise
about an arbitrary future branch.

Independent source-snapshot resolutions:
[#138 CLEAN](https://gitcode.com/urandon/sessionless/issues/138#note_192407004)
and [#139 CLEAN](https://gitcode.com/urandon/sessionless/issues/139#note_192407016).
[Owner acceptance](https://gitcode.com/urandon/sessionless/issues/139#note_192407231)
selects the **initial** D1–D5 scope only. The extracted document delta still
requires normal independent MR review and exact-head checks/CI; source CLEAN
does not transfer to changed text automatically. No implementation, deployment,
migration, provider/credential access or runtime activation is authorized here.

## 1. Product model and confidence

Sessionless owns the append-only Session stream. External conversations bind to it; transport IDs are not product IDs. A new conversation creates a new Session and switches the relevant binding without truncating its predecessor. A retry/recovery must use canonical admission and attempt semantics, never a UI-created scheduler.

Observed at the inspection base (not an activation claim): membership authorization, per-session participation, canonical sessions/events/runs, upload/artifact capabilities, worker owner projections, backend worker plan/apply/operation receipts, Sessions/Workers navigation.

Existing research inputs, not automatically implemented: AI resource/federation #51, usage #49/#53, platform console #54. Reuse their backlog and contracts.

Proposed here: common information architecture, cohort taxonomy, screen-level truth rules and bounded missing slices. Initial scope is selected in the accepted-scope section below; delegated and cross-tenant authority remains deferred. A proposed role name is not an authorization capability.

## 2. Five perspectives, not five blanket roles

| Perspective and job | Observable | Controllable, subject to server capability | Must not imply |
|---|---|---|---|
| Conversation participant: get work done and recover | Authorized transcript, attachments/results, run evidence, eligible compute summary, personal consumption | Create/send/archive/download under membership + participation; future compute selection/cancel only after contracts exist | Tenant-wide session access; credentials; other people's usage |
| Resource/worker owner: contribute reliable capacity | Owned resource health, freshness, capability, safe operation receipts; revealing consumer aggregates withheld | Owner policy/reserve and grant lifecycle when implemented; worker drain/revoke within reviewed runtime gates | Automatic reading of beneficiary sessions; provider/subscription pooling |
| Tenant administrator: keep a workspace usable | Membership/policy metadata, scoped resource availability, own-authorized usage; revealing aggregates withheld initially | Initial non-owner membership revoke after its missing contract is implemented; wider policy controls deferred | Session content access, ownership of every worker, platform privilege |
| Federation administrator (future/deferred): coordinate contributed capacity | Pool membership and delegated resource-policy summaries, scoped aggregate consumption | Only the intersection of delegated authority, owner grants and pool policy | Owner impersonation, credential access, cross-tenant membership transitivity |
| Platform operator: keep the service safe | Fleet and service metadata, incidents, policy/audit evidence, cost/coverage appropriate to role | Typed approved operations, narrow JIT support grants and break-glass under #54 | A standing unrestricted “admin of everything” |

The same human may occupy several perspectives. The UI switches explicit scope; capabilities are fetched for that scope. A navigation switch grants nothing. Platform administration is a separate security surface, not an extra tab unlocked by a tenant owner flag.

Aggregate disclosure is a separate server-side privacy decision: aggregate permission or tenant/resource ownership alone does not prove safe release of unauthorized individual consumption. Until the applicable #53/#139 disclosure decision is accepted, withhold totals/breakdowns/windows that can reveal another member through own+total subtraction, overlapping queries or join/leave differences. Coarsened release requires an accepted policy and negative evidence, not arbitrary rounding or a guessed cohort threshold. Personal-only views can proceed under their own reviewed contracts.

## 3. Semantic cohorts and entity inventory

| Cohort | Entities | Decision supported | Truth source / boundary |
|---|---|---|---|
| Conversations and results | Session, participant, event, frontend binding, upload, artifact | Where do I continue, and what was actually produced? | Canonical session service + exact authorized blob capability |
| Execution and recovery | Run, Attempt, admission, cancellation request, completion evidence | Is it pending, blocked, active, terminal or uncertain; what safe action exists? | Canonical run/attempt authority; observed worker facts remain separate |
| Resources and placement | AIResource, route/revision, worker, connection, enrollment, capability | What eligible capacity can serve this work? | Admission/resource authority, not client preference inference |
| Access and sharing | Identity, membership, participant, consent, use/admin grant | Who can do what, for how long, in which scope? | Each distinct server authorization predicate |
| Consumption and limits | Observations, ledger, quota, rollup, reconciliation | What did I consume, what remains, how trustworthy is it? | #49/#53 provenance; operational logs are not billing truth |
| Operations and audit | Plan, command, receipt, audit event, support grant | What changed, who requested it, and what was confirmed? | Durable typed operation receipt and audit; not a success toast |

Each read-model field must specify scope, source, observation time, freshness and confidence. Lists use bounded cursor queries. No generic entity page may expose a raw domain object by default.

## 4. Per-entity disclosure/control rules

| Entity family | Public-safe authorized projection | Control and precondition | Forbidden/default withheld |
|---|---|---|---|
| Membership / identity | Own identity; authorized workspace names and roles; admin member summary if permitted | Switch authorized tenant; initial non-owner revoke with expected revision after #146; invitations/role changes deferred | IdP claim as sufficient tenant access; unrelated memberships |
| Session / event / binding | Participant-authorized transcript, lifecycle, safe frontend badge | Create/new/send/archive; exact write authority and idempotency | Tenant admin bulk transcript; raw external tokens/IDs |
| Upload / artifact | Name, size/type, upload/commit evidence, exact authorized download | Prepare/upload/commit; limits and capabilities; retry commit safely | Arbitrary object-store listing or raw bucket keys/URLs |
| Run / Attempt | Canonical status + reason; attempt evidence/freshness; cancellation phase separately | Existing send admission; proposed cancel/retry only with reviewed contract | Inferring terminal completion from transport ACK; blindly resubmitting a billed operation |
| Resource / route / grant | Eligible model/placement/privacy class, policy revision and unavailability reason | Select eligible route only through canonical admission; initial exact owner use-grant revoke after #148; delegation/grant creation deferred | Credential values, private endpoint/host path, unauthorized resources |
| Worker / enrollment | Owner-authorized health, freshness, admission and isolation evidence | Existing server plan/apply; browser activation separately gated | Probe-on-read, online=ready inference, UI hidden fields as host privacy |
| Quota / consumption | Amount+unit+window+coverage+precision; unknown separately | Proposed bounded budgets/alerts with explicit authority | Missing value rendered zero; worker telemetry as exact invoice |
| Operation / audit | Requested target, safe diff, actor/reason, receipt phases and recovery | Exact scope/target, revision, idempotency and applicable confirmation | Request accepted rendered remotely erased/fully stopped |

## 5. Information architecture

```text
Product app — always show workspace, environment and current perspective
├─ Conversations (primary work surface)
│  └─ Conversation → transcript / composer / result cards
│                    └─ execution drawer → reason / evidence / allowed recovery
├─ My capacity → workers / resources / contribution status
├─ Consumption → own usage (revealing workspace aggregates deferred)
└─ Workspace administration → scoped member list / non-owner revoke / safe receipts
   └─ Capacity pool administration → own grants / exact use-grant revoke (after contract)

Platform console — separate auth boundary, scoped operator role
└─ Overview / incidents / tenants / fleet metadata (read-only initially)
   └─ future support content access only through separately accepted limited grant
```

Existing Sessions and Workers stay useful. Do not add empty navigation for backend features that do not exist. Pool administration can be a workspace subview initially; a separate cross-tenant console is not a prerequisite.

## 6. Key wireflows

Conversation: authorized workspace → Session list/new → eligible-compute summary → compose/upload → admission receipt → transcript with run-linked result cards → execution drawer if needed. The composer preserves unsent text through recoverable errors; scope changes must never submit it into the newly selected tenant.

Admission denied: safe structured reason → policy/compute/quota prerequisite → authorized remediation or owner handoff → explicit retry. No automatic provider/host fallback or hidden consent expansion.

Uncertain execution: canonical status + stale/observed evidence → explain what is unknown → show permissible diagnostic/recovery actions → durable receipt → refresh the canonical stream. Never invent a new RunStatus for UI uncertainty.

Owner action: owned worker → current state/evidence → unavailable reason OR allowed plan → target+generation+effect preview → confirm/apply → operation receipt showing requested/acknowledged/fenced/terminal evidence separately. At the inspection base, the browser action catalog is intentionally read-only; do not activate it in a presentation patch.

Membership/grant changes and platform incident flows are specified by #139/#54. Revocation reauthorizes subsequent BFF reads/commands and fresh download-capability issuance. An already issued direct bearer URL may still permit its first retrieval until the stated expiry (current documented download TTL at most two minutes from issuance); authorization-generation/cache invalidation does not revoke that URL. In-progress transfers and already received bytes cannot be recalled. Show these residual-access boundaries in copy/receipts. A future live-authorized analytics export proxy or immediate retrieval-revocation requirement needs its own reviewed delivery contract; neither is implemented by the existing blob presigner.

## 7. Presentation rules

Use [design-rules.md](design-rules.md) as the single owner of semantic, interaction,
accessibility and visual rules. This contract owns product cohorts and scope.

## 8. Inspection evidence and concrete gaps and source mapping

| Finding at pinned revision | Existing source | Bounded handoff |
|---|---|---|
| Membership ≠ transcript authority | `internal/domain/web_auth.go:165-186`; `internal/domain/session.go:143-156`; `internal/ydbstore/session_api.go:481-492` | Explicitly preserve in all admin DTOs and negative tests |
| Chat can describe ambiguous compute, but registered WebUI routes lack a selection command | `SessionDetail.svelte:493-519`; `internal/webbff/handler.go:189-223` | Conversation follow-up: eligible-choice contract + UI, not a client scheduler |
| No registered WebUI run-cancel endpoint | Same handler route set; `web/src/lib/api/openapi.json` | Separate typed cancel/recovery contract before enabling a Stop button |
| Worker controls deliberately unavailable in browser, backend plan/apply exists | `AttachedWorkerActionCatalog.svelte:8-29`; handler routes; #131 | #78/#119 rollout slice consumes runtime authority/evidence gates |
| Generic delegated federation-admin entities not in inspected domain/ports | Scoped source search + #51 research | #139 designs missing grants; do not pretend membership already implements federation |
| Visual/focus and hidden-tab behavior already exist | `styles.css:1-76`; `SessionDetail.svelte:111-123,213-216` | Reuse; browser verification belongs implementation acceptance |
| Usage/platform consoles are research, not registered general app routes | #49/#53/#54 reports; handler routes; `AppShell.svelte:1-24` | Reuse UA/PA decomposition; bounded metadata/rollups first |

Paths for components are under `web/src/lib/components/`. Evidence says nothing about unpublished parallel branch work. Recheck live contracts before opening implementation issues.

## 9. Acceptance/review checklist

Trace every field/control to source or named missing contract. Test two tenants, distinct session participants, owner vs consumer, viewer vs writer, stale versions, duplicate commands, and mint→revoke→redeem before/after capability expiry. Assert fresh capability issuance/BFF reads denied after revocation, pre-issued bearer URL residual validity bounded by expiry, and no recall claim for in-progress/received bytes. Also test two-member own+total inference and overlapping-filter/join/leave disclosure guards. Verify focus/mobile/empty/error/partial/stale states and bounded reads. Conduct independent authority/privacy review of a fixed draft snapshot; mark proposed roles/decisions explicitly. Carry agreed text through versioned docs and repository CI before closing #138. Keep current #34 → #35 release gates unchanged.


## Accepted initial scope and remaining decisions

The [owner receipt](https://gitcode.com/urandon/sessionless/issues/139#note_192407231)
selects these initial product choices, not a complete federation/platform policy:

| Decision | Accepted initial choice | Still deferred |
| --- | --- | --- |
| D1 | Same-tenant capacity-pool administration UI first. | Cross-tenant admin topology and directional trust/data/accounting/recovery rules. Do not narrow or enable #118 execution through a UI decision. |
| D2 | Owner-only administration. Tenant ownership, resource ownership, Session participation and platform role are separate predicates. | Administrative delegation, role editor, delegation amplification and broader membership/policy commands. |
| D3 | Consent binds exact resource, use scope, data class and disclosure version; materially changed conditions require renewed consent. Revoke follows #118 canonical authority and #82 evidence vocabulary. | Exact consent schema/persistence and reviewed in-flight enforcement integration; graceful withdrawal is a distinct future action, not immediate revoke. |
| D4 | Personal-only usage first. Potentially revealing tenant/pool totals, member detail and analytics exports stay withheld. | Final server-side aggregate/anti-differencing policy, export delivery and numeric retention periods remain #49/#53/#139 decisions; no guessed cohort threshold or retention days. |
| D5 | Separate metadata-only read-only platform console. Ordinary removal cannot leave no owner; exceptional recovery/content support access is not enabled initially. | #54 role/auth/security acceptance, JIT/dual-control/break-glass/recovery/support-content contracts and rollout evidence. |

A resource owner is not automatically a tenant owner, and a tenant owner cannot
appropriate another owner's resource. Shared-host disclosure must explicitly
state that the host operator may technically access admitted execution data;
omitting that data from the UI is not host confidentiality.

Initial child handoffs:
[#146](https://gitcode.com/urandon/sessionless/issues/146#note_192407260) is bounded
member listing plus non-owner membership revoke;
[#148](https://gitcode.com/urandon/sessionless/issues/148#note_192407264) is own-grant
projections plus one exact owned use-grant revoke. A revoke of a use grant is
not revoke of the worker/resource/credential or unrelated grants. The full
candidate catalogs below are future design envelopes, not initial permissions.

## Screen, API and evidence handoff

Paths name owners at the pinned inspection base. **Existing** does not imply a
new admin/UI route exists. A **missing** contract is a prerequisite to design
and implement with its canonical owner, never a client-side workaround.

| User decision / screen | Existing or named missing authority | Required acceptance evidence / owner |
| --- | --- | --- |
| Enter/switch workspace | Existing [WebAuthStore](../internal/ports/web.go), membership authorization and Web session rotation; browser/IdP selectors are not authority. | Two-tenant selectors, membership loss and unsent-composer scope isolation; [Web auth](web-auth-contracts.md). |
| Create/continue/new/archive conversation | Existing [SessionAPIStore](../internal/ports/session_api.go), participation and [canonical ingress](canonical-ingress.md). | Participant versus non-participant, append-only history, idempotent binding switch/archive; [Session API](session-api.md). |
| Upload/result/download cards | Existing [UploadIntentStore / WebObjectStore](../internal/ports/web.go), exact-object capabilities and [BFF](web-bff.md). | Limits/commit integrity plus mint-revoke-redeem before/after expiry; no object listing or recall promise. |
| Explain denied/uncertain execution | Existing bounded reads and canonical Run/Attempt truth; missing reason projection, if any, must be split before UI. | #141 empty/stale/partial/denied/unknown fixtures and [design-rule accessibility](design-rules.md#reusable-drawer-and-confirmation-acceptance). |
| Choose eligible compute | Missing WebUI eligible-choice command contract at inspection base; canonical resource/route admission remains #51 owner. | #142 contract before #143 UI; stale route/policy, unauthorized choice, explicit retry and no fallback. |
| Stop/recover work | Missing participant-authorized WebUI cancel/recovery endpoint at inspection base; existing canonical run/attempt/cancel owner defines semantics. | #144 before #145; requested/ACK/stopped/committed and accepted-unknown/replay negatives, not a new RunStatus. |
| Diagnose/control owned worker | Existing [attached-worker UX](attached-worker-ux.md) and backend plan/apply; browser rollout remains #78/#119 authority. | Two-owner visibility, generation conflict, offline ACK unknown and release-sized proof; a read screen never probes. |
| Manage tenant/pool | Existing owner/membership predicate is not a member-management API; initial commands and missing contracts are in [admin contract](tenant-federation-admin-contract.md#first-command-readiness-manifests). | #146 API before #147 UI; #148 grant authority before #149 UI; role/scope/replay/audit/fence/privacy negatives. |
| Inspect consumption | #49 owns metering/rollups; #53 owns bounded authorized analytics. Initial #150 is personal-only. | Relevant UA-01/UA-02/MM-04 before integration; unknown != zero, provenance/coverage, no raw scans; #151 future disclosure policy. |
| Inspect platform incident | Separate #54/#152 identity/session/role boundary before #153 metadata UI. | Tenant/IdP claims denied, revoked roles/JIT, bounded/redacted reads; #154 future command separately accepted, not initial console authority. |

Every planned API/DTO adds a field-level manifest: source and scope, enforcement
point, authority generation/revision, observation time, freshness/coverage,
bounded cursor/window, safe unavailability reason, and forbidden/raw fields.
Every command adds exact actor/target, expected version, idempotency/payload
conflict, atomic audit/durable receipt and failure/recovery behavior. Numeric
query/cardinality/rate budgets need their owning read-model review before
implementation; diagrams and suggested SP do not accept those budgets.

## Source custody and review limits

| Reviewed wiki body | Exact UTF-8 SHA-256 |
| --- | --- |
| [Product-UX-Contract-Draft-2026-10-01.md](https://gitcode.com/urandon/sessionless/wiki/Product-UX-Contract-Draft-2026-10-01.md) | `c1065fb360df108fa1e95bbb71992fe786397fbd5888b4c5060272c16a271514` |
| [Tenant-Federation-Admin-Draft-2026-10-01.md](https://gitcode.com/urandon/sessionless/wiki/Tenant-Federation-Admin-Draft-2026-10-01.md) | `5f11c974aa436b6e8e5b36f23be1d4156c92f0d8633708207e4610392b964b4e` |
| [UX-Delivery-Roadmap-Draft-2026-10-01.md](https://gitcode.com/urandon/sessionless/wiki/UX-Delivery-Roadmap-Draft-2026-10-01.md) | `54de32273868786754c87ad51ffd0388fa3bcff3cd951d352446df523e8a0986` |

These are source-body hashes, not hashes of these extracted files. The source
wiki remains an immutable review reference for this delivery; this extraction
splits design rules, incorporates the additive owner decision receipt, labels
future capabilities, and adds a screen/API/evidence handoff. Review those
changes independently. GitCode issues own live execution status; this document
does not certify production readiness or close the issues.
