# Conversation execution evidence

Version: **0.1.3 accepted engineering amendment**, 2026-10-10.
Baseline: **0.1.2 accepted**. Owner: [#182](https://gitcode.com/urandon/sessionless/issues/182) (closed).
Consumers: [#141](https://gitcode.com/urandon/sessionless/issues/141) and
[#155](https://gitcode.com/urandon/sessionless/issues/155).
Status: accepted detailed read contract, independently reviewed CLEAN and merged
in !161 under explicit owner delegation. Implementation handoff: #186 and #187
under #155, followed by BFF #188 and drawer #141. This post-MVP high-stream prerequisite is
not an additional gate for either MVP execution track.

The 0.1.3 amendment below is owned by implementation [#188](https://gitcode.com/urandon/sessionless/issues/188).
It resolves the shared server-rate mechanism without changing public fields,
product scope or accepted numeric ceilings. The primary accepted the amendment
under the owner's agreed-feature delegation after independent original-goal
review CLEAN at exact draft `6c0f610c401e25e5e89cc8fb03c9c885a13a374a`.
This is design acceptance only; rated adapter, migrations, native/CI and rollout
evidence remain implementation obligations of #188, not completed delivery.

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
effect ledger is introduced. The fields below are the accepted implementation
contract; rollout remains separately gated. Existing canonical ports remain the execution authority.

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

These are accepted numeric limits; implementation must prove them at its exact version:

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

### #187 storage writer and query manifest

The additive storage implementation uses #186's `runexplanation.HeadV1` and
pure reducers. Migration `00102` creates `run_explanation_heads_v1`; no reader,
scheduler or worker consumes that table as execution authority. No route is
mounted here, and there is no backfill or migration activation. Record the
deployed migration head, exact writer commit and drained old-writer inventory
as deployment cutover evidence before a consumer is enabled.

| Owning source | Atomic projection behavior |
| --- | --- |
| `ydbstore/store.go` `PutRun` | All production Run upserts pass here. Invalidate a denial whose exact phase/time changed, admitted evidence on return to created/quota-blocked, and incompatible terminal basis. Never synthesize an admission observation. |
| `PutAttempt` | All production Attempt upserts pass here. An independently written newer Attempt invalidates prior evidence; only canonical creation/admission selects an Attempt. Older Attempt writes do not replace the selector. |
| `PutDispatchOutbox`, `canonical_ingress.go` | Run + Attempt + initial pending outbox creation selects the exact Attempt and immutable placement within the ingress transaction. No whole outbox/WorkerJob is stored in the head. |
| `scheduler.go` `AdmitDispatch` | Record canonical supported/unsupported scheduler decisions after the Run phase is committed in the same transaction; changed reasons survive equal state/retry-time early return. Identical observation replay leaves revision unchanged; `dispatch_not_pending` never overwrites evidence. Readmission supersedes denial. |
| `scheduler.go` `ExpireQuotaReservation` | Central Run hook invalidates evidence when returning to quota-blocked; expiry does not become an invented new admission decision. |
| `worker.go` start/success/failure | Central Run/Attempt hooks preserve or invalidate basis. Canonical completion/failure calls the owning finalizer below; legacy Telegram finalizers never produce canonical notice metadata. |
| `attached_worker_execution.go` terminal materialization; output receipt path | The exact canonical finalizer records terminal evidence, separately from the attached head receipt. Receipt finalization/signature identities are unchanged. |
| `canonical_finalization.go` `appendCanonicalFinalizationTx` | Only after allocating the real canonical event does this transaction capture its ID/sequence. Owning failure schema/code/flag are bound to the immutable blob's exact canonical JSON size/SHA256, with no blob read or reconstructed ID. Unsupported historical payloads remain unknown. The existing finalization digest is unchanged, so durable managed/attached replays retain their identity and evidence. Success clears failure evidence. |
| `operations.go` legacy ingress, succeeded command without Attempt, unused `CompleteRun` | Central hooks apply. Initial pending legacy ingress may select its actual Attempt/placement; commands without Attempt remain unknown. No legacy finalizer fabricates a canonical notice or new Attempt. |
| `session_lifecycle.go` per-Run deletion | Delete the exact tenant/run head alongside canonical cleanup, even while the route is absent/disabled. The schema/reset inventory includes this additive table. |

Source inventory searches production `PutRun`/`PutAttempt` call sites and
direct Run/Attempt DML: the only upserts are central `store.go`; callers are
canonical ingress, scheduler, worker and operations. Attached terminal writers
delegate to the same worker helpers and canonical finalizer.

`RunExplanationReadStoreV1` returns the safe validated Web contract only. One
serializable resource operation samples `CurrentUtcTimestamp()`, rereads the
exact Web session/membership identity and authenticated security version,
then checks the exact canonical Session's current READ participant. It does
not call activity-refreshing `AuthorizeWebSession` or any owner compute API.
Expired/revoked sessions and denied/version-changed membership keep canonical
errors; inaccessible targets use one opaque not-found error. Other failures
become content-free temporary unavailability.

| Statement | Primary-key lookup / selected source |
| --- | --- |
| 1 | Transaction clock; no table |
| 2 | Web session `(shard_bucket, session_digest)` |
| 3 | Membership `(user_bucket, user_id, tenant_id)` |
| 4 | Run `(tenant_id, run_id)` |
| 5 | Session `(tenant_id, session_id)` |
| 6 | Participant `(tenant_id, session_id, user_id)` |
| 7 | Explanation head `(tenant_id, run_id)` |
| 8 | Exact selected Attempt `(tenant_id, attempt_id)` |
| 9 | Optional attached receipt `(tenant_id, owner_user_id, worker_id)` |
| 10 | Its exact canonical lease `(tenant_id, lease_id)` |
| 11 | Current worker binding; bounded generation/state/time columns only |
| 12 | Current connection binding; bounded ID/generation/state/time columns only, no record/secret/signature |

The fullest single snapshot is 12 statements; a historical locator-less read
is 7. A resource-owned budget counts actual statements across **all transaction
retries**, failing before statement 21 rather than resetting per callback.
The same three-second context bounds every retry. Typed JSON sources are capped
in SQL at 8193 transferred bytes and rejected above 8 KiB before decoding; head
encoding is independently validated at 8 KiB. The response validator/marshaller
caps the safe fixed-field body at 16 KiB. Missing/mismatched selected source or
replacement attached target stays unknown, without reading replacement joins.
No range/audit/transcript/history/blob read, activity/presence write, capability,
repair, reconciliation or worker/provider call occurs.

Offline SQL fixtures count the actual full read path, serializable isolation,
read-only behavior and SDK commit retries (14-statement successful short retry;
20-statement rejection on repeated/attached retries). Tagged YDB fixtures cover
all eleven data query plans, authorization changes after BFF preauthorization,
read-only participation/no activity refresh, reason supersession/replay,
historical/mismatched selectors and finalize/read then delete/read snapshots.
Fixture-only native connector barriers hold the serializable reader open after
an exact canonical/auth query while finalization, deletion or membership mutation
commits; successful reads must match an entire committed snapshot, not merely
validate. Exact successful-admission replay preserves the complete head record
and revision, including when the replay request carries a later observation time.
An explicit sentinel-aborted central Run write proves canonical phase and
projection record/revision roll back together, preserving the committed reason.
Test execution receipts and exact-head CI are recorded in the implementation
handoff, not inferred from this manifest. A fake-transaction scripting test
connection proves SQL/schema/plan behavior, not true serializable race behavior;
the latter requires the real transaction adapter in exact-head CI.

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

The implementation epic is the existing #155, not a new epic. Registered bounded
children include #186 (domain/transport projection contracts and pure reducers)
and #187 (YDB canonical writers, authorized bounded reads/deletion/migration fixtures).
BFF #188 owns OpenAPI integration and negative/cache/budget proof separately. Then #141
implements the drawer with browser/accessibility evidence. #182 is closed after
accepted-version review and handoff; registration is not implementation completion.

Each child must name this design version, #155, predecessors, work/domain/priority
labels, Product UX milestone and exact acceptance. None may absorb #142–#145
selection/cancellation commands or broaden MVP release gates. All production
changes require independent exact-snapshot review, proportional tests, exact-head
CI, merge and parent synchronization under the project protocol.

## 0.1.3 amendment: transaction-scoped global read rate

The existing `ReadRunExplanationV1` port remains genuinely read-only and retains
its expected identity/tenant/security-version recheck. Its delivered #187 proof
is not reclassified as a rate-limited HTTP implementation. A distinct internal
`ReadRatedRunExplanationV1` port accepts only the BFF's digest of the first-party
Web-session cookie, its generated request ID and the Run selector. No HTTP
tenant, user, security-version or idempotency field is accepted as authority.

The BFF must not call activity-refreshing `AuthorizeWebSession` on this route.
The rated port samples YDB time and resolves the current canonical Web session,
identity, active tenant and READ membership/security version in one serializable
transaction before limiter or resource access. Missing/revoked/expired sessions
keep existing unauthenticated precedence; denied/version-changed membership
remains access_denied. Shared extracted readers preserve the pure port's exact
expected-context checks. Construct the resource `stateTx` only after resolving
its canonical tenant; do not nest another authorization transaction or add an
optional trusted/bypass flag.

Only the following bounded rate metadata may be written on the rated path;
canonical state, explanation heads, session activity, presence and runtime
authority remain untouched. This is the explicit exception to the baseline's
read-only wording, not a new scheduler, execution budget or effect ledger.

### Finite shared authority

Use an additive YDB rate-slot table with integer primary keys in `[0,4095]`.
Every replica uses the same versioned hash and constants. Derive four distinct
candidate keys from a domain-separated, length-framed hash of the **resolved**
tenant and user. Read all four exact keys. Prefer an existing matching identity;
otherwise choose an empty or logically expired slot in deterministic order.
Live occupants are never evicted. If all candidates are occupied, return the
same content-free 429 with a fixed five-second Retry-After. Capacity/collision
denial is explicit, not permission to bypass the limit or scan for another slot.
The finite key space proves physical cardinality at most 4,096 without a global
counter, range read or cleanup dependency.

Each typed row has a fixed schema/version, the internal identity hash,
theoretical-arrival timestamp (TAT), last debit time, logical expiry, and at most
16 bounded debit-replay receipts. A receipt binds the server request ID, a digest
of its exact resource selector and debit time; it grants no resource access.
No cookie credential/digest, raw resource ID or response body is stored there.
The transferred and encoded row is capped at 8 KiB before decoding/writing.
Invalid, oversized, unsupported or inconsistent rows fail sanitized unavailable;
they never become an empty/free bucket, including when their expiry is old.

At sampled transaction time `now`, allow iff `TAT <= now + 5 seconds`, and on
allowance set `TAT = max(TAT, now) + 5 seconds`. An empty slot starts at `now`.
This admits two initial requests and the third only after five seconds. Denial
changes neither TAT nor expiry; its Retry-After is the positive rounded-up number
of seconds until eligibility. Future/unbounded or backwards-clock inconsistencies
fail closed; no arithmetic wraps or invents replenishment. A charged row's
logical expiry is ten minutes after its last debit, safely after full refill and
receipt retention. Logical expiry alone permits reuse; optional asynchronous
TTL cleanup cannot authorize earlier reuse.

### Commit, retry and resource outcomes

The server-generated request ID is stable only within one bounded HTTP request;
client headers never select it. Retain debit receipts for 30 seconds (at most
16; never evict a still-required receipt to admit another read). The rate ceiling
and three-second request deadline bound admitted receipts in that window. A
matching-ID retry still performs fresh current session/member authorization and
exact resource-selector matching, but does not debit again. A reused ID with a
different selector fails closed. This makes the existing idempotent SDK
transaction retry policy safe for an ambiguous successful debit commit; the
receipt is not a response cache or a substitute for participant authorization.

After rate allowance, run the same exact Run/Session/participant and evidence
queries as the pure port. Success and opaque not-found are **committed outcomes**,
not callback errors: an authorized guessed/missing/inaccessible resource still
consumes its debit. Authorized rate denial is likewise published only after a
successful transaction commit, with no debit. Actual authorization/backend/
validation failures abort. An unresolved commit returns sanitized unavailable;
never claim the debit was refunded. Reset the pending outcome on every callback
attempt so an aborted attempt cannot leave a stale successful response. There is
no handler retry loop or automatic retry of an uncertain HTTP request.

### Budget and rollout proof

One three-second outer context starts before the route's first database access.
One statement budget lives outside the SDK retry callback and counts reads **and
writes** across all attempts, failing before statement 21. The fullest attempt
is at most 17 statements: three clock/session/member statements, four rate-key
reads, one debit write, and the existing nine resource/evidence statements.
Short/historical paths consume less; retries may exhaust the budget and fail
unavailable rather than widen it. The pure port's 12-statement full path remains
unchanged. Neither port reads history, audit ranges, blobs or worker/provider APIs.

The route remains absent by default. Explicit enablement requires the additive
schema and a checked deployment cutover receipt naming the projection writer
version, schema/rated-reader version, exact deployed writer commit and drained
old-writer inventory. A boolean environment flag alone is insufficient. Cutover
checks do not apply migrations, repair/backfill data or prove a cloud deployment.
Existing per-Run projection deletion remains enabled independently of this route.

Required deterministic/native verification adds: two independent store/handler
instances competing for the third debit; committed 404 charging; current auth
before 429; unchanged LastSeenAt/idle/absolute expiry; same-ID replay after a
committed debit and after permission loss; mismatched-selector replay rejection;
known rollback versus ambiguous commit; fixed cardinality/collision/expiry and
corrupt-row denial; aggregate read/write retry exhaustion; and pure-port unchanged
read-only/error behavior. Exact-head CI must cover the actual rated adapter,
not merely the pure arithmetic or fake-transaction scripting plans.
