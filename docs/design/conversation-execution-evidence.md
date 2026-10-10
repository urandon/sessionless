# Conversation execution evidence

Version: **0.1.2 draft**, 2026-10-10. Owner: [#182](https://gitcode.com/urandon/sessionless/issues/182).
Consumers: [#141](https://gitcode.com/urandon/sessionless/issues/141) and
[#155](https://gitcode.com/urandon/sessionless/issues/155).
Status: proposed detailed read contract; independent review and owner acceptance
are required before implementation. This post-MVP high-stream prerequisite is
not an additional gate for either MVP execution track.

The conversation drawer reads one authorized Run and its recorded evidence.
It keeps admission, canonical completion and Attempt observations distinct.
It never infers a failure cause from current compute, event adjacency or a
transport acknowledgement. Missing historical or operational evidence remains
unknown without preventing access to the authorized transcript.

## Inputs and decisions

The accepted [product UX baseline](../product-ux-contract.md) and
[shared interaction rules](../design-rules.md) own presentation and scope.
[Source research 0.1.1](https://gitcode.com/urandon/sessionless/issues/141#note_192999837)
was independently reviewed against main
`d3eb8fdc069938c05be23212e834a7cc3d82a03e`.

At that snapshot, the public event DTO drops internal RunID; terminal notices
have a reusable schema but opaque, not public-taxonomy, codes; multiple
admission denials share `quota_blocked`; and equal scheduler state/retry time
can suppress a new audit despite a changed reason. Run finalization records do
not point to the corresponding notice. These are contract gaps, not proof that
canonical admission/finalization is absent.

| Alternative | Decision in this draft |
| --- | --- |
| Correlate notices by time, proximity or reconstructed IDs | Reject. Managed and attached event-ID recipes differ; IDs stay opaque. |
| Expose canonical event RunID and a safe typed notice | Adopt as an additive event projection; it covers loaded events, not complete per-Run evidence. |
| Page all history or query audit on drawer open | Reject. No unbounded scans, blob fetches or internal audit export. |
| Participant-authorized point explanation read | Adopt. Derive from canonical sources and a transaction-maintained, non-authoritative locator/reason projection. |
| Reuse worker-owner diagnostics | Reject as a participant shortcut. Ownership and participation are separate predicates. |

No new RunStatus, admission policy, cancellation machine, retry decision or
effect ledger is introduced. All new types below are proposals, not currently
implemented contracts. Existing canonical ports remain the execution authority.

## Read boundary

Proposed route: `GET /api/web/v1/runs/{run_id}/explanation`. It accepts no query
parameters or request body. RunID is a selector only. A new narrow
`RunExplanationReadStoreV1` port accepts RunID and a server-resolved authorization
context containing tenant/user IDs, the authenticated membership security version
and the internal Web session lookup digest. None of those authorization fields
are client-supplied or public response fields. It returns only the safe projection
below, never raw domain objects.

The BFF reauthorizes the current first-party Web session and active tenant READ
membership on every request. The resource transaction reads the Run, verifies
its Session still exists, and verifies the requesting user's read participation
in that exact Session before joining evidence. This supplements, not replaces,
the membership boundary. A tenant owner is not automatically a participant.
Missing, deleted, foreign-tenant and nonparticipant targets share public
`not_found`; an expired/revoked Web session remains `unauthenticated` under the
existing BFF contract. Internal errors remain sanitized temporary unavailability.

Run, locator, Attempt, lease and optional operational records are read from one
serializable resource transaction. It rereads the exact Web session and
membership, verifies their identity/tenant bindings and the authenticated
membership security version, and applies `WebSession.Authorize(READ)` at the
sampled transaction time. Missing/revoked/expired sessions use the existing
sanitized `unauthenticated` response. Changed membership security versions and
denied membership use the existing sanitized `access_denied` response without
target information; this route does not change the BFF error taxonomy.
This recheck does not refresh session activity. Revocation, expiry or an active
READ-preserving security-version bump cannot survive an earlier BFF check.
Use that transaction's time for lease validity; response `read_at` comes from
the same sampled time. No provider/worker calls, blob capability issuance,
presence checkpoint, reconciliation, migration or repair occurs on this path.

Reads are `Cache-Control: no-store`; this first version has no HTTP 304/ETag
fast path. Authorization always precedes any response, including a rate-limit
response containing resource-related information. Browser state is memory-only
and keyed by the current Web identity/tenant/Session/Run and request generation.
Scope changes and permission loss discard it. An in-flight response cannot
restore data after its view or authorization scope is discarded.

The existing Session compute endpoint remains separately write-authorized and
owner-scoped. Its not-configured/ambiguous/entitlement/quota observations explain
future sending, not a historical Run. Read-only participants get Run evidence
without compute-write or worker-owner privileges. The new endpoint returns no
compute eligibility decision or enabled mutation/retry capability.

## Public field manifest

`RunExplanationV1` has a fixed version `1`. It has no arbitrary metadata map,
raw error text, host/provider account identity or nested domain object.
Unknown variants omit unavailable fields; they do not use zero, empty IDs or a
timestamp as a fabricated value. Timestamps are UTC instants.

| Field group | Fields and source | Scope and meaning |
| --- | --- | --- |
| Envelope | `version`, `run_id`, `session_id`, `read_at` | Exact authorized Run; sampled transaction time is read time, not last remote observation. |
| Canonical Run | `status`, `created_at`, `updated_at`, optional `finished_at` | Existing RunStatus and timestamps from Run in that transaction. A terminal fact does not expire because it is old. |
| Admission | `availability: recorded\|unknown`, optional `outcome: admitted\|denied`, safe `reason_code`, `observed_at`, `decision_revision`, `coverage: last_recorded_decision` | Latest explicitly recorded canonical scheduler evaluation for the selected Run/Attempt, not a new policy evaluation or live entitlement guarantee. Missing or invalid basis is unknown. |
| Terminal reason | `availability: recorded\|unknown\|not_applicable`, optional safe `reason_code`, `observed_at`, `event_id`, `event_sequence` | Canonical finalization notice metadata for this Run. Nonterminal Run is not_applicable; terminal with missing/unsupported evidence is unknown. A recorded unclassified notice proves that a notice exists, not its detailed cause. |
| Selected Attempt | `availability: recorded\|unknown`, optional `attempt_id`, `number`, existing `status`, `updated_at`, optional `finished_at` | Exact canonical Attempt selected by the ingress/admission locator, validated against the Run. Its state is not a physical-process heartbeat. Number is a count, not a public fence. |
| Attached observation | `availability: recorded\|unknown`, optional `fact`, `observed_at`, `valid_until`, `freshness: within_lease\|expired\|durable` | Sanitized current matching attached receipt; no owner/worker/connection IDs or generations returned. Receipt facts remain separate from canonical Run/Attempt. |
| Coverage | `admission: recorded\|unknown`, `terminal: recorded\|unknown\|not_applicable`, `attempt: recorded\|unknown`, `operational: recorded\|unknown` | Per-source presence and consistency, not a claim of complete transcript, telemetry, billing or all earlier attempts. |

An observation timestamp or revision is emitted only with its recorded source.
`decision_revision` is a monotonic read-projection revision; it is never accepted
as scheduling, lease or command authority. A new request always reauthorizes.

The envelope deliberately omits provider labels because the current Run
provider projection resolves connection information, not a frozen execution
receipt. Existing conversation summaries may retain their current labelled
projection, but cannot use that label to explain this Run's cause.

### Admission reason mapping

Use the existing canonical scheduler code as the input, not RunStatus or current
compute. Public reason codes are a closed enum with fixed copy owned by the
Web contract. Initial supported mappings are:

| Canonical decision code | Public reason code |
| --- | --- |
| `admitted`, `already_admitted` | `admitted` |
| `subscription_reauthentication_required` | `subscription_attention_required` |
| `subscription_draining` | `capacity_draining` |
| `provider_quota_blocked` | `quota_reset_pending` |
| `provider_quota_exhausted_reset_unknown` | `quota_exhausted_reset_unknown` |
| `runtime_limit_exceeded`, `turn_limit_exceeded`, `input_limit_exceeded`, `context_limit_exceeded`, `artifact_limit_exceeded` | Corresponding fixed public limit code, preserving which admitted workload bound failed |
| `subscription_slot_busy` | `capacity_busy` |
| `tenant_queue_depth_exhausted` | `workspace_queue_limit` |
| `tenant_active_run_limit` | `workspace_active_run_limit` |
| Any unsupported decision code | Unknown reason; no raw code or guessed next action |

`dispatch_not_pending` is an internal dispatch observation, not a new admission
decision. It must not overwrite the last recorded decision. The observed
decision does not authorize automatic retry, fallback or route selection.

The historical denial remains historical once readmission supersedes it.
An admitted record may still accompany a failed terminal Run: it explains that
admission happened, not why execution failed. Its fixed copy always says
“Last recorded admission decision” and includes observation time.

### Terminal and attached evidence

Only the exact `sessionless.run-terminal-notice.v1` schema and owning canonical
finalization may supply terminal metadata. Initially map `harness_failed` to
`execution_failed`; `artifact_upload_failed` and
`canonical_result_upload_failed` to `result_persistence_failed`; and
`cancelled`/`cancelled_before_start` to `canonical_cancelled` only when the
committed Run is cancelled and the notice flag agrees. Other syntactically
valid codes map to `unclassified_failure` for failed Run, or unknown reason for
an inconsistent cancelled notice. This does not reflect arbitrary worker text.

Even recognized codes report a recorded phase failure, not an independently
diagnosed root cause. Schema/status/flag inconsistency makes terminal reason
unknown and emits a content-free internal diagnostic. It never changes RunStatus.

For attached operational facts, map canonical attached states `offered`,
`claimed`, `terminal_pending`, `terminal_committed`, `fenced_unknown`, `retired`
to fixed fact codes `offer_recorded`, `claim_recorded`,
`terminal_candidate_recorded`, `terminal_commit_recorded`,
`fenced_outcome_unknown`, `receipt_retired`. Exclude cancellation-phase states
in this first read scope: their request/ACK/process-stop semantics remain
[#144](https://gitcode.com/urandon/sessionless/issues/144)/
[#145](https://gitcode.com/urandon/sessionless/issues/145). Unsupported or
missing states are unknown, not “no work”.

Join the attached head internally by the admitted placement's tenant/owner/
worker selector, then require matching RunID, AttemptID, LeaseID, canonical
numeric lease generation, and the head's own current enrollment/connection
binding. A head replaced by another Attempt must yield unknown for the selected
Run; never disclose the new target. No query to the owner-facing UX port is made.
The adapter must independently enforce participant authorization first.

For nonterminal lease-bound facts, `valid_until` is the canonical lease expiry;
freshness is `within_lease` strictly before it and `expired` at/after it, under
the sampled transaction clock. This says whether that authority window remains
valid, not that the host is alive. Terminal committed/fenced/retired facts are
durable historical receipts; they have no live-health expiry claim. A missing
or mismatched lease is unknown. No client-invented TTL or online inference.

Managed remote process observation is explicitly unknown in V1: canonical
Attempt and terminal notice still apply, but the effect reservation alone
does not prove provider acceptance or physical completion. Adding managed
substrate/reconciliation facts requires a separate reviewed field manifest;
no broad effect ledger or raw attestation is exported here. This is a declared
operational coverage limit, not permission to remove #141's Attempt/unknown
acceptance or either managed MVP proof.

## Canonical locators and atomic update rules

Proposed YDB table `run_explanation_heads_v1` has primary key
`(tenant_id, run_id)` and a bounded typed record. It stores SessionID, an internal
selected AttemptID, the frozen placement locators needed for the optional
attached join, latest safe admission observation, and terminal event ID/sequence
plus its normalized notice metadata. It contains no content, secret, raw audit,
provider body, worker path, capability token or complete WorkerJob.

It is a derived read projection. No scheduler/worker/command may read it to
decide admission, retry, cancellation, fencing or terminal completion. Canonical
transactions write it; GET never creates or refreshes it. The private placement
locators are access-controlled internal keys, not public fields or credentials.

Every projection observation stores its Run/Attempt source identity, phase,
source timestamp and schema version. The selected Attempt identity changes only
with canonical creation/admission of that Attempt. A monotonically increasing
revision prevents stale projection overwrites; revision exhaustion fails the
canonical transaction rather than wrapping. The first implementation must
prove transaction retries and identical replay do not invent new observations.

| Owning transaction | Required projection effect |
| --- | --- |
| Canonical ingress Run+Attempt+outbox creation | Create locator in the same transaction; admission and terminal evidence unknown until recorded. |
| Scheduler evaluation | Record supported admitted/denied outcome, normalized code and selected Attempt atomically, including changed reason with unchanged scheduler state/retry time. Exact same decision replay is unchanged. |
| Readmission or a newly selected Attempt | Supersede the admission observation; update selector; clear any incompatible terminal locator. Historical audit remains separate. |
| Canonical worker/attached terminal finalization | After event sequence allocation, store the exact notice ID/sequence/time and normalized safe metadata in the same terminal transaction; success clears failure metadata. Idempotent matching finalization reuses the same evidence. |
| Other Run lifecycle writes | Invalidate incompatible admission/terminal basis or update it through its owning canonical operation. No stale denial may appear current after a changed admission phase. |
| Session/run deletion | Delete the projection by exact tenant/run alongside existing per-Run cleanup. No new raw history discovery or retention policy is introduced. |

The implementation writer manifest must cover the canonical ingress,
`ydbstore/scheduler.go`, `worker.go`, `operations.go`, central `PutRun` and
attached terminal receipt/finalization paths, plus all source changes since
the design snapshot. This is a release gate for the projection, not a claim
that every writer was exhaustively proven by the initial research.

The reader validates all available joins against current Run and exact selected
Attempt in the same snapshot. A missing locator or incompatible basis gives
unknown for the affected group; canonical Run fields remain readable. A
malformed canonical Run or unbounded/corrupt record fails the request with a
sanitized unavailable response. Diagnostics do not contain the private record.

Adding optional `run_id` to public SessionEvent is an independent additive
transport change. The BFF copies only the already authorized event's canonical
RunID. Generic notices without it stay unlinked. A typed public terminal notice
contains only the safe class above; raw notice data is not used as the drawer's
authority. It still cannot claim absence of a reason outside the loaded page.

## Query cost and rollout limits

These are proposed numeric limits requiring exact-version review and acceptance:

| Limit | Proposed ceiling |
| --- | --- |
| Selected resource | One Run, one selected Attempt and one optional attached head; no historical list |
| Storage | At most 20 point statements including authorization; one resource transaction; no range/audit/history/blob reads |
| Internal projection | 8 KiB encoded record; only bounded selected columns from other sources, no complete WorkerJob/host payload |
| Public body | 16 KiB encoded response; fixed field counts and enum strings; opaque selectors at existing domain ID limits |
| Request | Three-second resource deadline; existing bounded transaction retry policy must fit inside it, no handler retry loop |
| Browser refresh | One in-flight request per drawer; no faster than five seconds, stop on hidden/unmount/permission loss; reuse server backoff, not a new polling scheduler |
| Server rate | Authenticated user+tenant: burst two, sustained one explanation request per five seconds; bounded limiter cardinality and expiry must be specified/tested in its implementation, with no resource details in 429 |

If existing membership/source reads cannot meet the point/body/deadline budget,
the implementation must return to design review rather than silently widen it.
Opening the drawer may trigger this authorized database read, not an external
compute probe. Current compute refresh remains an explicit separate action.

Historical rows are **unknown**, not automatically backfilled. Apply the additive
schema first under separately approved migration authority; upgrade all owning
writers before mounting the feature. Until the writer manifest is verified,
the endpoint and drawer remain disabled. A deployment cutover marker records
which writer/schema version is active; it does not retroactively prove old runs.
Newly observed old Runs may gain only the groups a new canonical operation
actually writes. No scan or invented complete-coverage marker is used.

Rollback disables the route/drawer and stops projection use, preserving canonical
data and the additive table. Destructive down migrations are not required.
Session deletion continues removing projection metadata regardless of route
enablement. This document authorizes no migration apply, cloud deploy, provider
call or cleanup operation.

## Verification and implementation handoff

Use [Go testing practices](../testing-best-practices.md), fake clocks for pure
reducers and one sampled YDB time authority for lease integration boundaries.
Do not use sleeps or an old green CI result as proof of the new contract.

| Required case | Invariant |
| --- | --- |
| Interleaved managed/attached Run notices; generic unlinked event | Exact server correlation, no adjacency or ID reconstruction |
| Notice outside loaded transcript page; old Run with no locator | Per-Run lookup is bounded; loaded-page absence never means no reason |
| Busy slot, workload/tenant pressure, exhausted quota | Distinct safe admission codes; `quota_blocked` is not decoded as exhausted quota |
| Same pressured state/retry time with changed denial code; readmission | Atomic new reason/supersession, historical audit not rendered current |
| Unknown code/schema or cancelled flag/status mismatch | No raw value or fabricated cause; canonical status remains authoritative |
| Read-only participant, nonparticipant tenant owner, cross-tenant guessed IDs, membership loss or active READ-preserving security-version bump between BFF/resource reads; Web session revoke/expiry | Read participation works; inaccessible selectors remain opaque; no stale session or membership success |
| Attempt replaced, generation/lease mismatch, attached head belongs to another Run | Unknown operational group, no disclosure of the replacement target |
| Lease expiry boundary; old terminal receipt; failed/paused browser refresh | Operational expiry differs from durable fact age and client refresh failure |
| Matching terminal replay, concurrent read/finalize/delete, rollback with deletion | One consistent snapshot, no duplicate observation or orphan projection |
| Body/query/rate/deadline limits; readonly opening | Bounded resources and zero probes, capability issuance, repair or runtime side effects |
| Hide/unmount/scope change during request; keyboard/focus/zoom | Discarded response cannot restore data; unsent draft/focus survive; existing drawer matrix is met |

The implementation epic is the existing #155, not a new epic. After owner
acceptance, register linked bounded children for (1) domain/transport projection
contracts and pure reducer fixtures, (2) YDB canonical writer integration,
bounded authorized read/deletion/migration fixtures, and (3) BFF/OpenAPI client
integration and negative/cache/budget proof. Then #141 implements the drawer
against those contracts with browser/accessibility evidence. These are proposed
slices, not linked registered tasks or a completed implementation checklist.
#182 stays open until its accepted version and real handoff are recorded.

Each child must name this design version, #155, predecessors, work/domain/priority
labels, Product UX milestone and exact acceptance. None may absorb #142–#145
selection/cancellation commands or broaden MVP release gates. All production
changes require independent exact-snapshot review, proportional tests, exact-head
CI, merge and parent synchronization under the project protocol.
