# Eligible compute choice contract

Version: **0.1.0 draft**, 2026-10-10.
Owner: [#142](https://gitcode.com/urandon/sessionless/issues/142).
Implementation epic: [#155](https://gitcode.com/urandon/sessionless/issues/155).
UI consumer: [#143](https://gitcode.com/urandon/sessionless/issues/143).
Source baseline: `5b7e215494449894d5b0b162c24f57a4da2875aa`.
Status: proposed detailed engineering contract; not implementation or activation.

A conversation writer chooses an exact resource, model and execution placement
for one message. The browser submits an opaque selector, not a harness binding
or a permission. Canonical ingress rechecks current authority before recording
the message, and canonical admission rechecks it before reserving execution.
Neither path substitutes another resource when the selected choice is stale,
revoked, unavailable or incompatible.

This is the owner-high #155 engineering stream. It remains post-MVP product
scope, not a new release prerequisite. Both agreed MVP execution paths remain
required under the [MVP delivery plan](../mvp-delivery-plan.md). The owner's
agreed-feature delegation permits engineering acceptance after independent
requirements-led review; it does not claim personal source review or authorize
paid calls, credentials, cloud writes, migration apply or runtime activation.

## Inputs and alternatives

The [product UX baseline](../product-ux-contract.md) accepts participant-scoped
compute choice but deliberately leaves its detailed contract open. The
[resource research](../research/ai-resources-and-federation.md) establishes
resource/model/transport/billing/harness/placement separation, eligibility as
an intersection and no implicit fallback. Those decisions are reused; closure
of the entire #51 research program is not required. The accepted
[execution evidence contract](conversation-execution-evidence.md) explains
historical Runs and grants no selection or cancellation authority.

| Alternative | Decision and reason |
| --- | --- |
| Per-message explicit choice | Adopt. Immutable with the message; duplicate sends cannot select a different target. No shared mutable default or separate preference receipt is needed. |
| Shared Session default | Defer. One participant could affect another's later sends; needs separate actor, CAS and change-notification semantics. |
| Per-user durable Session preference | Defer. Useful convenience, but creates another retention and mutation protocol; #143 may retain an unsent selector in scoped memory only. |
| Model-name or subscription-connection-only selection | Reject. Neither pins route, billing, policy, capabilities or placement. |
| List credentials and call the result eligible | Reject. A credential row is not an approved backend, resource-use grant, current route or healthy execution placement. |
| Browser supplies a HarnessBinding or fallback set | Reject. Binding construction and canonical admission remain server-owned. |

## Current source boundaries

These facts are pinned to the baseline, not implementation claims about this
draft or a moving parallel branch.

| Source | Existing capability and required extension |
| --- | --- |
| `internal/domain/provider.go` | ProviderResourceBindingV1 pins kind, resource ID, owner, revision and credential generation. HarnessBindingV1 seals backend, model and catalog/route/privacy/capability/policy/placement digests. These are private authority, not public DTOs. |
| `internal/domain/provider_evidence.go` | ProviderRoutePolicyV1 has a policy revision and exact routes, not a standalone canonical RouteID. Catalog, capability, privacy and price evidence have independent provenance and validity. |
| `internal/sessionlessharness/attached_binder.go` | Reviewed attached pins are bounded to 64 and resolve exact owner resources. Current worker/connection checks happen before canonical commit, not inside it. |
| `internal/ydbstore/web_upload.go` | The existing compute resolver returns at most two owned subscription connections to detect ambiguity; it is not a paginated eligible-choice catalog. |
| `internal/webapi/service.go`, `internal/sessioningress/service.go` | Message mutation identity covers text/uploads; the binder prepares authority before the canonical transaction. A prior preparation cannot authorize a later commit. |
| `internal/ydbstore/canonical_ingress.go`, `internal/ydbstore/scheduler.go` | Canonical writers own event/Run/Attempt/manifest/outbox/idempotency. Scheduler owns admission and reservations. Add exact current-choice checks to those transactions, not a new scheduler. |
| `internal/ydbstore/run_explanation_rated.go` | Current cookie-derived Web identity/membership and resource-time checks occur in one transaction without refreshing activity. Reuse this authorization pattern, not its READ permission or historical evidence as execution permission. |

Main still serves HarnessBindingV1. The unmerged #189 V2/cost work is not an
accepted prerequisite or an alternative decoder. Implement against the accepted
canonical version at integration time; a later accepted writer-first migration
updates this consumer explicitly, with no legacy alias or silent V1 fallback.

No general resource-use-grant/consent registry or persisted current
route/catalog/price registry exists at this baseline. The initial adapter may
enumerate reviewed owner resources and approved placement registrations, never
pretend to implement sharing. Managed real-provider entries stay unavailable
until their owning authority/binder and rollout contracts exist. The contract
represents both attached and managed placements; subscription-only discovery or
a fixture advertised as real managed compute cannot satisfy its integration.

## Actors and authorization

The first-party cookie is the only HTTP identity input. Tenant/user/role, policy
versions, price, resource owner, credential generation and worker identity are
not accepted from request headers or JSON as authority.

Listing requires a current Web session, active-tenant WRITE membership and
WRITE participation in the exact active Session. This preserves the existing
compute route's writer-only boundary. A viewer can still read transcript and
historical Run evidence, but cannot enumerate prospective executable choices or
send. A tenant owner without Session participation is not a conversation writer.

List, fresh send and duplicate recovery reauthorize the exact cookie and current
membership in the transaction that reads or commits the resource. They sample
YDB time, check Web session revoke/expiry/active tenant and membership security
version, and then check Session participation. A BFF precheck is not sufficient.
Reads do not extend Web-session activity. Send checks the same-session CSRF
digest and same-origin Origin in addition to current WRITE authority; changing
the Web session between preliminary HTTP checks and commit cannot succeed.

Missing/deleted/foreign Session and nonparticipant selectors share `not_found`.
Expired/revoked cookies remain `unauthenticated`; denied membership remains
`access_denied` without target information. For an authorized Session, unknown,
foreign, hidden or withdrawn choice selectors share `compute_unavailable`.
Never distinguish a guessed resource's existence, owner, revoked grant or host.

Owner use and beneficiary use are separate predicates. Initially only exact
owner use is backed by existing registration. A future beneficiary adapter must
load a reviewed current use grant and provider terms in the same transaction;
membership, resource ownership, an index row or this design cannot manufacture
that grant. Absence denies rather than enabling sharing.

## Choice registration and current eligibility

ComputeChoiceRegistrationV1 is a private enumeration projection of one
operator-reviewed canonical tuple, not a new resource authority. Its opaque
`choice_id` is generated server-side and reveals no resource or credential ID.
Registration references existing canonical authority and an immutable backend
registration; it never contains plaintext credentials or initiates discovery.

The initial registry has at most 64 entries in the deployed serving manifest,
including both placement kinds. The bound reuses the existing attached-pin
ceiling but applies to the new combined catalog. Overflow rejects configuration;
it never truncates a registry into apparent completeness. A writer-checked
deployment manifest digest and schema version are recorded in a cutover receipt.
Every serving instance must match it. Different or obsolete instance registries
fail closed, rather than giving load-balanced clients contradictory selections.

Only trusted configuration installation may publish this receipt after all
writers/readers and drain checks pass. The browser has no registration/update
endpoint. Registry material is immutable for one approved revision; changing
backend/resource/model/route/policy/placement or disclosure replaces the
revision. Neither that receipt nor a registration supersedes current resource,
membership, consent, price or execution authority.

Each list/send transaction resolves these private facts for a candidate:

| Concern | Current source and pin |
| --- | --- |
| Identity and use | Canonical tenant, initiating writer, exact Session, resource owner; current owner-use predicate or independently reviewed use-grant ID/revision/generation. |
| Resource | Existing kind/ID/revision, credential mode/generation and lifecycle. Never serialize credential ref, fingerprint or account identity. |
| Backend and model | Exact immutable backend descriptor/profile/artifact; vendor/model; catalog observation/digest and conformance evidence. No installed-binary or provider probe. |
| Route and billing | Exact ProviderRouteV1 tuple and ProviderRoutePolicyV1 revision/digest; transport/upstream/endpoint/billing identities stay private. Fallback remains deny. |
| Policy and disclosure | Provider terms verdict/window; effective policy, privacy/data-residency/region and disclosure revision; material changes invalidate the selector. |
| Placement | Exact attached enrollment/connection/capability/policy authority or managed substrate/profile/proxy/isolation/egress revision and expiry. Reconnect may change current connection evidence and invalidate a preview; it never switches the pinned resource. |
| Capability and input | Reviewed required model/tool/modality profile. Actual input class and committed upload metadata are derived on the server at send; a browser hint cannot reclassify private data as public. |
| Capacity and price | Distinct entitlement/quota, monetary price/currency/budget, platform limits and local capacity observations with source and expiry. Missing required facts deny; zero is never invented. Reservations remain canonical admission's job. |

The implementation must use bounded exact current readers for these facts. It
must not reread a process-local snapshot and call it a transactional revocation
check. Where a backend lacks a current reader or approved source, that entry is
unavailable, not eligible. #142 owns integration of existing readers and the
missing commit-time checks; it does not enable a new provider or implement a
general federation/policy registry under another name.

`choice_revision` is SHA-256 over a domain-separated, length-prefixed canonical
encoding of the registration revision, authenticated tenant/user/Session,
membership security version, requested input profile and the exact relevant
authority/evidence revisions and validity windows above. It excludes Run/Attempt
IDs, which do not exist at listing. Stable evidence revisions are pinned; sampled
read time and continuously changing utilization counters are not digest inputs.
The server recomputes the digest at send. A digest is a conflict fence, not a
bearer capability, resource grant or proof that capacity has been reserved.

## Public list protocol

Proposed route: `GET /api/v1/sessions/{session_id}/compute/choices`.
Keep the existing `/compute` status route separate; it is not an alias for this
new contract. Feature-disabled routing returns no synthetic ready choices.

Only `limit`, `cursor` and `input_kind` query keys are accepted. `limit` is 1–4
(default 4); `input_kind` is `text`, `image`, `file` or `mixed` (default text).
This is an advisory capability preview, never a data-class or upload-authority
assertion. No query accepts model names, owner/resource IDs, endpoint URLs,
credential data, sorting or arbitrary filters. GET/HEAD bodies are not consumed;
HEAD is not an alternative route.

ComputeChoicesPageV1 has these closed fields:

| Field | Meaning and disclosure |
| --- | --- |
| `version`, `session_id`, `read_at` | Version 1 and exact authorized Session; read_at is sampled DB time, not remote observation time. |
| `input_kind`, `registry_revision`, `coverage` | Requested preview; opaque manifest revision; coverage `page` or `complete`, not total entitled inventory. |
| `choices` | At most four visible entries. Each has opaque choice_id, choice_revision, curated model/provider labels, placement `attached` or `managed`, state and safe disclosure. No raw binding. |
| Entry `state` and `reason_code` | `eligible`, `unavailable` or `unknown`; closed safe reason only for a choice already disclosable to this writer. Eligible means all required preview predicates held at read, not reservation or a running process. |
| Entry `observed_at`, `valid_until`, `freshness` | Required source window; freshness `current` or `expired`. Missing required evidence yields unknown without fabricated timestamps. valid_until is the earliest required source expiry. |
| Entry `disclosure_revision`, `disclosure` | Exact reviewed disclosure text/class; provider processing and host-operator technical access, admitted input classes and no-fallback rule. Curated fixed copy, not provider error strings. |
| Entry `capacity` | Closed availability/unknown state and source freshness only. No other member's remaining quota, consumption, total pool budget, exact invoice or inferred free price. |
| Optional `next_cursor` | More registered candidate positions may exist; it does not disclose a total or promise another visible result. |

Allowed reason codes for already visible entries: `profile_disabled`,
`evidence_stale`, `capability_unavailable`, `consent_required`,
`capacity_unavailable`, `policy_unavailable`, `unknown`. Membership and use-grant
denials hide the entire entry instead of revealing those predicates. Unknown
quota/price is not displayed as zero, unlimited or free. Labels are reviewed
display strings, never endpoint/host names or credential-derived strings.

CursorV1 is a bounded opaque token with a server MAC. It binds the authenticated
tenant/user/Session/membership security version, registry revision, input_kind,
last examined candidate position and expiry. It carries no public authority IDs
or raw registry material. Maximum encoded length is 512 bytes; validity is at
most 60 seconds and no later than the shortest relevant authority expiry.
Every page reauthorizes and recomputes eligibility; the cursor is not a snapshot
lease or permission. Changed scope/registry or expiry returns `choice_stale`.

Pagination advances by examined registry positions, not returned entries. A
page examines at most four candidates, may be empty with a cursor, and never
loops internally to fill a page or scans hidden candidates past its budget.
No provider/worker RPC, blob issuance, credential refresh, activity update,
reconciliation or product mutation happens on read. Only bounded limiter
metadata may be written, as described below.

## Explicit selection and consent at send

Selection is per-send, not an independently persisted preference command.
CreateMessageRequest adds a required ComputeChoiceSelectorV1 when the checked
choice feature is enabled: `choice_id` (opaque ID, at most 128 bytes),
`choice_revision` (64 lowercase hexadecimal characters) and
`disclosure_revision` (64 lowercase hexadecimal characters). Unknown fields,
mixed versions and absent selectors reject; there is no auto-selection even
when the list contains one candidate. Existing upload/text bounds remain in
force. The new fields add at most 384 decoded bytes to the request envelope.

The writer's explicit send acknowledges the selected, displayed disclosure for
this message only. The server compares disclosure_revision with the current
reviewed disclosure and records the actor, exact resource/use scope, derived
input class, disclosure revision and event in the atomic selection receipt.
This narrow consent is not a resource-use grant, provider-terms approval,
permission to change route, or consent for a later tool/search operation.
Material disclosure/input-policy changes return `consent_required` and require
an explicit refreshed choice/send. Merely opening the list creates no consent.
#143 must show this disclosure before send, including the host operator's
technical access to admitted execution data; UI redaction is not confidentiality.

Fresh canonical commit performs the following in its one Serializable
transaction, with a reset candidate on every retry callback:

1. Resolve current cookie/WRITE membership/Session participation and CSRF.
2. Check the existing exact message idempotency key and mutation identity.
3. Resolve the exact visible registration and every required current source;
   recompute choice/disclosure revision and validate at sampled DB time.
4. Validate actual input/upload class and modality, applicable policy/consent and
   capability. Reject changed material or unavailable authority before dispatch.
5. Build server-owned canonical authority for the generated Run/Attempt from
   that exact tuple; write the event, Run, Attempt, manifest, outbox, idempotency
   fact and selection receipt atomically using existing canonical writers.
6. Return the public receipt only after confirmed commit. Failed or unknown
   commits expose no speculative success; exact replay/reconciliation follows
   the canonical idempotency contract, without another message/provider send.

Preparing immutable upload objects or a binding before this transaction is not
permission. Upload ownership/commit hashes are validated by existing workflows;
failed canonical commit leaves only bounded staged material under existing
cleanup authority, never a dispatch. This draft does not move large object I/O
or provider calls into a database transaction.

The selection becomes immutable with the message. MessageMutationDigestV2 seals
the normalized text, ordered upload IDs and all selector fields, with an explicit
version/domain separator. Changing any selection field under the same key is a
payload conflict, not a new route. An exact duplicate first reauthorizes the
original Session and returns its original committed receipt even if the catalog
changed afterward; it does not reconsider eligibility or enqueue another Run.
Deleted/unauthorized original history cannot be recreated by retry. Existing
retention/tombstone rules bound deduplication; expiry never grants replay of a
previous billed effect.

Canonical admission remains a separate stage. Before reserving quota/lease/job
or offering an attached attempt, its transaction reloads current membership/use,
resource/revocation/policy/profile/capability/price/consent/placement authority and
compares it with the committed immutable tuple. A lost eligibility race records
a canonical safe denial for that Run; it does not rewrite the selector or find a
replacement. Budget/capacity is reserved only there, and the effect boundary
still requires its own current lease/fence checks. No read or selector creates
an Attempt before the normal canonical send.

## Receipts privacy and lifecycle

ComputeChoiceReceiptV1 is an append-only subordinate receipt keyed by canonical
tenant/Run, atomically written with ingress. It binds the Session/event/actor,
message mutation digest, choice/revision/disclosure, derived input class, exact
private authority digest and committed time. Scalar columns and the encoded
record must agree. It is neither another Run lifecycle nor a resource registry.

Public message success adds only version, opaque choice_id/choice_revision,
disclosure_revision, model label, placement, committed_at and canonical Run/event
IDs. It means the selected message/dispatch committed, not that the provider ran
or the scheduler admitted it. No raw credentials, account/worker/owner IDs,
endpoint/path, resource fingerprint, price/budget amounts or detailed private
policy enters that DTO, generic logs, metrics labels or support bundles.

Duplicate lookup and later receipt reads require current access to the original
Session. Retain the private receipt exactly with its canonical Run under the
existing Session lifecycle and applicable legal hold. Session deletion must
remove it and its indexes in the same bounded Run deletion step; no standalone
TTL may delete a receipt while the canonical effect/deduplication record remains.
Do not invent retention days or legal-hold bypass. Discarded browser scopes and
permission loss clear pending selectors and disclosures; in-flight list/send
responses cannot repopulate another user's or tenant's state.

## Resource budgets and cache contract

These are proposed engineering ceilings for this version, requiring independent
design review and implementation counter tests; they are not measured RU/cost.

| Operation | Finite ceiling and enforcement |
| --- | --- |
| Catalog | 64 registered candidates globally per serving manifest; pure in-memory owner-prefix enumeration only after DB authorization. No database/world inventory scan. |
| List work | Four examined candidates; at most twelve bounded current-source point statements per candidate. No candidate whose reader exceeds that bound is enabled. |
| List transaction | At most 64 SQL statements in one attempt and 96 in total across retries, including auth/clock/manifest/rate work; three-second overall resource deadline. A shared counter survives retries; budget exhaustion rolls back and returns temporary unavailability. |
| List payload | Each source record at most 8 KiB, total materialized records at most 512 KiB; serialized public response at most 16 KiB. Validate encoded length before parsing untrusted persisted records. |
| HTTP list | Raw query at most 1 KiB, one instance of each accepted query key, cursor at most 512 bytes; no GET body decoding. No ETag/304 fast path. |
| Fresh send choice work | One registration, at most twelve current-source point statements; at most 24 added statements including receipt/manifest/auth/CSRF, counted across retries inside the existing bounded ingress transaction. Exhaustion denies without a canonical message. Existing ingress/upload/object limits are not enlarged. |
| Receipt | At most 4 KiB private record and 1 KiB public selection receipt. One receipt per Run, no unbounded per-retry history. |
| Browser | Fetch on opening/explicit refresh/page advance, no background catalog polling; one active request per scope, no automatic send or conflict retry. |

The list's shared rate budget is per canonical tenant/user, not per instance,
cookie, Session or cursor. A separate finite table, not the CHAT-1 Run limiter,
uses 4096 validated slots/four probes, GCRA burst two and one refill per five
seconds, with at most sixteen server-request debit receipts per slot. Receipts
expire logically after 30 seconds; idle slots expire logically after ten
minutes. These precedents are from the accepted explanation design, but are
newly proposed here and do not change its table or limits. Validate every
occupant/key/hash before reuse. Saturation denies safely, never allocates an
unbounded new key or advertises zero load. No TTL cleanup is needed for the
finite bound; logical expiry is checked at sampled DB time.

The rate debit and resource read use the same authorized Serializable
transaction. Server-generated request IDs bind the exact selector/query; retry
after a lost commit acknowledgement reuses the receipt without another debit.
Error outcomes reset on callbacks; failed/unknown commit returns no resource
data or refund claim. Only this limiter may write on a list read. HTTP responses,
including rate/errors, use `Cache-Control: no-store` and `Vary: Cookie`; stale
authority is never served from a conditional response cache. No mutation may
reuse cached authorization. Browser data is scoped memory, keyed by identity,
tenant, Session and request generation; relogin/scope change clears it.

## Verification and implementation handoff

Implementation remains in existing #142 under #155; a second epic or a task
created merely for this design would duplicate an existing bounded outcome.
#143 owns the UI only after #142's implemented reviewed contract, not merely this
document. #144/#145 cancellation/recovery remain separate and unchanged.

| Evidence case | Required assertion |
| --- | --- |
| Two tenants, nonparticipant tenant owner, viewer, foreign/hidden choice | Current scope and write authority; guessed selectors are non-oracular; zero binding/dispatch/reservation effects. |
| Owner and beneficiary | Existing owner use works only for its exact approved tuple; no grant on main means beneficiary unavailable, not inferred membership permission. |
| List then revoke, rotate, expire, update route/price/disclosure or change input | Fresh send refuses stale/materially changed tuple; renewed explicit choice cannot bypass capability/privacy classification. |
| Send then revoke before admission; revoke before effect | The corresponding current transactional authority refuses reservation/effect; immutable selection remains explainable, with no alternate route. |
| Same key and exact message; changed selector/disclosure/text/upload/order | One canonical result and original receipt versus typed payload conflict; no second dispatch, reservation or provider send. |
| Commit failure, callback retry, lost commit response, delete during replay | No speculative receipt; callback resets; exact committed recovery only; no resurrection or duplicate effect. |
| Same writer in two tabs or different Session | Per-send exact selectors cannot change another request/default; cursor/revision binds the authenticated Session. |
| Config mixed across serving instances, disabled profile, unknown current reader | Fail closed against checked registry receipt; deterministic fixture never claims real-provider readiness. |
| Page contains hidden candidates, empty page with cursor, malformed/expired cursor | Finite examined positions, safe progress, no total/count leakage, no unbounded fill loop or auth bypass. |
| Statement/byte/rate/deadline saturation and transaction retries | Counters include every source and attempt; no probes, activity refresh, hidden product writes or unbounded limiter keys. |
| Browser scope loss/hide/unmount, stale list, send denied, keyboard/focus | #143 discards late data, retains only same-scope unsent content, explicit retry and truthful receipt; relevant shared accessibility/browser matrix passes. |
| Session deletion/legal hold and schema/cutover | Receipt/index follow canonical bounded lifecycle; real YDB rollback/deletion and absent-reader/writer-first gates, not a fake-only proof. |

Use [Go testing practices](../testing-best-practices.md): isolated fixtures,
one authoritative clock, no correctness sleeps, immediate bounded cleanup,
uncached focused/race/stress/shuffle and relevant full suites. Native integration
must prove list/send/admission transaction races and receipt lifecycle, not just
tagged compilation. Independent review uses original issue/contracts before the
immutable implementation; exact-head mirror CI and guarded merge are required.
UI presentation and browser accessibility tests remain #143's deliverable.

Rollout is default-off. Additive schema/receipt/finite limiter, compatible
canonical writer/readers, actual drained-backlog and registry-manifest checks
precede route enablement. Old writers must not omit choice identity on a new
enabled path. Legacy route behavior may remain disabled from this new surface,
but is not a fallback for missing new fields. Rollback disables new selections
and new sends on this surface; already committed Runs retain their exact
authority and canonical terminal/reconciliation semantics. No stored Run is
retargeted. Migration apply, deployment, credentials or real provider calls need
their separately authorized release steps; a documentation merge grants none.

Design acceptance requires independent review of this exact version and its
source/budget/authority mappings. #142 stays open until the backend contract,
transactional integration, tests/CI/merge and parent synchronization are
delivered. #155 stays open until all mandatory children and its integrated
product acceptance are proven. The goal retains both MVP paths while this
owner-prioritized engineering work advances.
