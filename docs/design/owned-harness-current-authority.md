# Current authority for the owned agent harness

Version: 0.1.6, engineering draft, 2026-10-11. Implementation owner:
[#189](https://gitcode.com/urandon/sessionless/issues/189), under
[#176](https://gitcode.com/urandon/sessionless/issues/176). This extension
defines the current-authority requirements of the accepted
[minimum 0.2.1](owned-agent-harness-minimum.md); it does not change either MVP
track, activate a profile or authorize live calls.

An immutable binding records what was admitted. Before a new effect, the
existing Attempt store must also prove that its installed profile, policy,
price, funding and particular allocation are still authorized. Extend the
existing harness/substrate registration lifecycle with transaction-readable
current projections. Do not add another provider selector, scheduler, quota
ledger or terminal state machine.

## Existing contracts and the missing authority

The source inventory is pinned to `79be1853ae8be4292cbb21651dc804b176547230`.
Later implementation must revalidate these observations.

| Existing contract | What it proves | Required extension |
| --- | --- | --- |
| [Current Session and resource readers](../../internal/ydbstore/serverless_current_authority.go) | Same-transaction active Session, deletion absence, tenant/Session write rights and current API/router credential tuple | Reuse these reads and the existing Job/Run/Attempt/lease/fence join |
| [Provider evidence bundle](../../internal/domain/provider_evidence_bundle.go) | Structural agreement of catalog, route, capability, privacy, price and policy | Authentic installed approval, current generation and freshness; structural validation alone cannot publish approval |
| [Harness registry](../../internal/sessionlessharness/registry.go) and [substrate registry](../../internal/serverlessharness/registry.go) | Exact immutable process registrations and closed profile gates | Trusted installation/revocation writers and current durable heads, not a second registry |
| [Prepared allocation](../../internal/domain/serverless_invocation.go) | Matching image, backend, argv and proxy artifact identities | Actual allocation identity, generation, physical claim, observation window and retirement |
| [Proxy attestation](../../internal/serverlessegress/contract.go) | Trusted preflight's policy, destination, pinning and expiry checks | Current per-claim registered observation from that same trusted proxy |
| [Attempt operation port](../../internal/ports/attempt_operation.go) | Required atomic admission, recovery and before-send contract | Real storage/debits, current projections and process-local one-use minting |
| [Quota reservation](../../internal/domain/quota.go) | Canonical compute-capacity hold, not monetary funding | Explicit trusted per-Attempt spending authorization; a capacity hold or copied cost ceiling cannot authorize payment |

The current startup registers only the deterministic fixture. The owned-agent
denials in both registries and effect admission remain until their separate
implementation and rollout gates pass. No persisted row may turn an
unsupported one-shot driver into a multi-operation driver.

## Authority ownership and trusted publication

An installed profile is a server-owned exact registration, not a queue field
or HTTP request. Its publisher belongs to existing trusted harness/substrate
composition. Publication must verify the actual installed descriptor, supported
execution profile and conformance/calibration receipts; independently accepted
provider/service policy, pricing and terms; and the authorized funding source.
Model output, a structurally valid evidence bundle, an opaque receipt digest
or a generic `approved=true` argument cannot satisfy this boundary.

Use a typed process-local publication grant issued by that trusted composition
and verified by the existing Store, following the existing effect-grant pattern.
The grant authenticates exact bytes, locator, expected generation, transition,
audit request and bounded expiry. It is nonserializable and cannot be decoded
from public input. Authenticating a grant is not a substitute for validating its
source: the issuer must derive it from an explicitly installed reviewed manifest
and the real funding/attestation owner, not sign caller declarations.

The production manifest/source and publisher must be wired explicitly; an
absent source denies publication. Test-owned scripted registrations, explicit
fixture funding and attested in-process allocations are legitimate credential-
free controls, not proof of an approved live provider or cloud allocation.
Installing a projection never changes runtime enablement flags. Paid profile,
terms/funding and deployment authority remain separately required.

## Canonical current projections

The following are proposed initial version-1 storage contracts, not currently
implemented APIs. All keys are derived from canonical server-owned bindings.
No caller chooses another registration, payer, endpoint or allocation.

### Installed profile head

`OwnedAgentProfileCurrentV1` is the current projection of the existing trusted
registration. Its stable key is `(tenant_id, owner_user_id, profile,
backend_profile_digest, resource_kind, resource_id, model_vendor_id, model_id,
input_data_class)`. Each component is validated and bounded before SQL. The
backend/profile/resource/model/class tuple identifies one installed profile;
this lookup does not discover or select another one.

The record contains positive profile revision and monotonically increasing
revocation generation; `active | disabled | revoked`; exact complete backend
descriptor and provider resource revision/generation; installed configuration
and reviewed artifact receipt references; independent catalog/route/capability/
privacy/price/policy and MCP/search grant references; admitted modalities and
calibrated model-window/token-estimator facts; instruction/compaction version;
finite profile limits; approved price/terms, component currencies and explicitly
identified billing owners; source observation, database publication and exclusive
expiry times. Immutable bodies are content-addressed and authorized, with exact
digest/length/type references rather than private material in this head.

The head is one atomic installed approval package. A change to any constituent
profile, policy, grant, evidence, price, terms, payer or resource generation
advances the head generation. Independent resource revocation is additionally
checked through its existing authority; copying a credential tuple into this
head does not replace the credential store. Substrate configuration is matched
against the existing exact substrate profile and current installation generation.
Neither an old immutable configuration nor an expired evidence body grants
current permission.

### Per-Attempt spending authorization

No monetary funding source exists in the current capacity quota. Introduce
`AttemptSpendingAuthorizationCurrentV1` as a scoped authorization attachment
to that same canonical admission/Attempt, not a balance, provider-quota account
or second debit ledger. Its primary key is `(tenant_id, attempt_id,
component_kind, billing_owner_user_id)`; the closed component kinds are
`provider | substrate | mcp_tool | web_search`, at most four rows per Attempt.

Its trusted publisher belongs to the installed admission composition. Before
policy/binding construction, it verifies an actual payer-authorized source
for this exact Attempt and component. The source must independently identify
tenant, Run/Attempt, admitted principal, billing owner, resource/profile,
data class, service grant, approved price/terms revision and currency, a finite
maximum amount and a nonrenewable validity window. An opaque funding digest,
owned credential, held capacity quota, model declaration or finite price
calculation cannot establish that source. A source's finite grant must itself
permit this Attempt; it cannot be reused across Attempts as an implied balance.

The initial implementation supports only installed credential-free fixture
sources: explicit independently evidenced zero or finite paid **synthetic**
amounts. Its publication grant authenticates the exact installed fixture
manifest/source and original canonical admission identity; the Store verifies
that issuer and bytes. Arbitrary fixture labels cannot convert production
registrations into fixture funding. Production paid/platform/foreign-payer
sources remain unsupported and deny publication/admission until their real
payer-authorized source, publisher and revocation path are separately wired
and accepted. Test funding is not authority to charge a live service.

The head contains version; exact Run/Session/principal/payer and component;
resource/profile/price/terms/grant locators and revisions; authenticated source
kind/identity/revision/digest; explicit currency and amount; positive generation;
`active | revoked`; observed/publication/exclusive-expiry times; and the immutable
publication/audit request correlation. The initial fixture source also names
the exact `OwnedAgentProfileCurrentV1` primary key, generation and digest of its
installed fixture manifest: that existing current registration projection is
its source-revocation lookup, not an unspecified additional funding table.
Create authenticates the source once,
with expected generation zero and actual absence. The amount, payer, scope,
price/terms and original window cannot be increased, refreshed or reassigned.
Revocation authenticates the source owner and exact current generation, advances
the generation and retains the tombstone. Exact replay recovers only its prior
result; it never recreates a revoked or absent authorization. A changed grant
requires a fresh canonical Attempt and independently permitted source.

Current readers point-read these exact keys in the same reservation/before-send
SQL transaction and database-time snapshot. They require active exact original
generation/digest, source identity and scope, and the intersection of the original
source, terms and head windows. Revoking the installed fixture source advances
its profile head generation and immediately invalidates all attached spending
heads through this exact join, without scanning Attempts. Revoking a particular
Attempt grant advances that spending head's generation. Copying a still-valid
source digest after either revocation is not permission. No source API call
occurs in SQL. An absent source-revocation projection or unsupported funding
class denies, including when amount is zero.

Admission records these head references and cannot silently recreate them.
Existing Attempt accounting owns all worst-case component and aggregate debits;
this authorization head contains no balance, spend counter, refund or transfer.
Capacity quota remains required independently. Price/terms prove the cost of
an operation; this additional exact source proves who permits its envelope.

### Attempt admission attachment

Ingress and scheduler admission are two distinct phases. The server binder
produces `AttemptExecutionSourceV1` before a capacity reservation exists. It
fixes tenant/Session/principal/Run/Attempt, the original installed-profile
reference, exactly four ordered spending snapshots, completed policy/binding/
placement/substrate/cost digests, exact currency and aggregate amount, and
original `IssuedAt`/exclusive `ExpiresAt`. It contains **no reservation ID**.
The canonical ingress transaction fences this asserted Session against its
actually authorized frontend binding revision and Session before any event,
Run, Attempt or outbox write. A caller/binder Session is not authority.

Carry the source as exact bounded quoted String bytes through authority ports,
canonical commit and dispatch outbox. Early application idempotency lookup
precedes the binder; concurrent-miss transaction recovery compares the original
source bytes as well as execution digests. It never rebuilds a source at the
new delivery time or selects another payer/profile. Owned records require the
source; single-turn/attached records explicitly exclude it. This is a tagged
contract, not a legacy decoder or compatibility fallback. Old owned backlog
must be proven empty before cutover; runtime registrations remain disabled.

`AttemptExecutionAdmissionV1` is an immutable attachment to the existing
canonical admitted WorkerJob, keyed `(tenant_id, attempt_id)`, and created in
the same existing admission/dispatch transaction. Its canonical body contains
version, that exact original `Source`, the actual `ReservationID` and one
database `AdmittedAt`. Admission time must be inside the original source
window. It does not replace `IssuedAt` in policy construction, refresh any
source deadline or reseal the execution package. The source records exact Run,
Session, principal, completed execution digests, current-head references and
approved funding allocations. Missing final attachment is not reconstructed
from the Job or source as a fallback. Structural validation of either phase
does not establish authentic approval, current permission or held capacity.

Each funding component references its exact current spending-authorization
head and names its payer, allocation identity/generation,
resource/profile/grant, currency, approved price/terms revision and evidence,
finite amount and validity. Provider, substrate, MCP and search are accounted
for; service components exact-match `ExternalServiceCostCeilingV1`, including
`BillingOwnerUserID`. Foreign funding needs its actual current grant; neither
resource ownership nor tenant membership silently creates such a grant.
Anonymous access does not prove free pricing. Known-free requires separately
established price/terms plus explicit zero; missing/unknown amounts deny.

The attachment binds the independently authorized aggregate ceiling; it does
not itself approve spending or create an account balance or quota ledger.
Reserve the worst-case preparation,
allocation, active/cleanup, delivery and transport/storage envelope under the
canonical admission/Attempt accounting, requiring both current spending
authorization and the separate capacity quota, before physical preparation. Subsequent
operation and consent charges use the existing Attempt cumulative budget and
must fit the approved component and total allocations. Observed lower usage
does not refund permission to send another request.

The attachment can be created once or recovered exactly; it cannot be replaced
to refresh the policy, source generation, payer, amount or time of an existing
Attempt. Revocation changes current source heads, not this historical body.
An accepted new configuration requires a fresh canonical admission/Attempt.

### Registered allocation and proxy observations

`RegisteredAllocationV1` is a child of the existing outer reservation, keyed
`(tenant_id, attempt_id, physical_invocation_claim_id)`. The trusted supervisor
registers it only after authenticating the confirmed sole owner's outer grant
and observing the real preparation. It binds Run/Session/principal, original
lease/fence/worker and claim, parent reservation and invocation digests,
prepared-invocation/allocation attestation digests, actual allocation identity
and generation, exact substrate/image/backend/argv/proxy association, source
registration generations and observation/publication/expiry. Its states
`active | retired | revoked` describe that observed allocation, not Run status.

Each fixed provider/MCP/search endpoint has a `RegisteredProxyAttestationV1`
under the same claim and allocation, keyed additionally by service kind and
independent scoped transport/grant digest. The actual trusted proxy supplies
policy, artifact/identity, fixed destination, DNS resolution-set/pinning,
redirect/ambient-proxy/TLS enforcement and observation/expiry. These facts may
not be asserted by the model, queue or an arbitrary worker payload. The sealed
operation still binds its own exact request and consent; endpoint attestation
does not approve query arguments or content.

Preparation may resolve/connect only the finite fixed destinations already
authorized by the profile, under its pre-reserved setup/transport envelope.
It sends no model, tool or search payload. No discovery, endpoint override or
RPC occurs inside a SQL callback. Refresh, reconnect or replacement allocation
is not an automatic retry: the first profile permits one physical preparation,
fixed observations and shorter-window stopping, not warm resume. Cleanup and
revocation use the existing supervisor and Attempt lifecycle.

## Preparation order and digest construction

The [current worker](../../internal/worker/manager.go) reserves the outer effect
before `SubstrateRegistry.Prepare` obtains its allocation. Preserve that order:

1. Trusted installation produces independent registration/evidence/grant/price
   facts and authenticated per-Attempt spending heads. Ingress builds the
   original policy, HarnessBindingV2, placement/substrate/cost and immutable
   source package from those facts, fenced to the canonical Session. Scheduler
   admission rereads the original current authorities and adds actual capacity
   reservation/DB admission time in a create-once attachment with WorkerJob.
2. The existing serializable outer reservation validates current Job/Run/Attempt,
   Session/resource/lease, installed approval and funding, then confirms one
   physical claimant. Its grant permits the bounded preparation boundary only;
   it is not authority to dispatch model/tool operations.
3. That sole claimant prepares once outside SQL callbacks, registers the observed
   allocation/proxy facts with the existing Store and receives exact readback.
4. Each subordinate operation atomically rereads current authority, validates the
   sole parent and registered observations, records its immutable reservation
   and worst-case debit, then mints one process-local capability after confirmed
   fresh commit. Immediate before-send validation rereads the same authorities.

Actual allocation cannot be a prerequisite to reserving its own parent claim.
A competing claimant remains reconciliation-only and cannot prepare, register
a replacement allocation or start cleanup cancellation as the original owner.

Keep the existing acyclic order: independent facts/grants → policy → binding →
invocation → outer reservation → registered allocation/proxy observations →
subordinate operation/reservation/observation. Later attachments may reference
completed earlier digests; earlier policy/grants must not contain later current-
row or reservation digests. Profile revisions remain independent facts.

The immutable operation reservation seals the exact registered allocation and
proxy observation digests read at admission; authenticated capabilities and
observations bind the completed reservation. Complete this initial subordinate
contract canonically, including its strict codec/digest fixtures, before its
first production cutover. Do not retain an old-shape decoder or reinterpret an
existing single-turn prepared allocation as current allocation authority.
Domain-separated canonical digests include version, all scope fields, scalar
length boundaries and explicit optional presence. Timestamp and numeric codecs
must preserve exact microseconds and integer values; new persisted authority
records use bounded `String` bytes, not lossy JSON reserialization.

## Publication transitions and ambiguous outcomes

Create requires expected generation zero and actual row absence. Update,
disable or revoke requires the exact current generation and authenticated
installed-source intent. Successful transitions use database time, increase
generation without overflow and preserve an immutable audit correlation.
An identical transition request recovers its prior result; conflicting bytes,
stale generations and unknown rows fail closed. Disabled/revoked heads remain
tombstones. Startup with old configuration cannot resurrect them. A deliberate
new installation requires explicit new reviewed revision/intent; it does not
revalidate an old Attempt's admission attachment.

Allocation registration is create-once for the original claim and observed
allocation. An exact lost acknowledgement recovers that same registration;
conflicting observation, foreign claim or absent recovery cannot register a
replacement. Retirement/revocation is monotonic and cannot revive an allocation.
If physical preparation may have happened but its registration is unconfirmed,
stop and reconcile/clean up that original allocation. Never prepare again to
make the database look complete. All transient tokens/candidates are discarded
after failed or ambiguous commits; minting stays outside retryable callbacks.

Historical reservations and authenticated observations remain readable after
revocation/expiry for reconciliation and canonical terminal evidence. Their
readback never mints a send capability or renews an original deadline.

### Bounded structural registration and process-local preparation fence

[Registered preparation contracts](../../internal/domain/registered_allocation.go)
now define the strict initial allocation/proxy shapes, canonical codecs and
domain-separated complete digests. The allocation preserves the exact original
Source, profile/four spending references, parent claim, lease/fence/worker,
prepared capability digest and absolute expiry, invocation deadline and actual
allocation identity/generation. It seals the complete prepared attestation by
digest rather than embedding or truncating its legitimate 64-by-4096-byte argv.
The record is not permission to reinterpret a single-turn backend as owned.

The proxy record references the exact original active allocation snapshot and
scoped service grant. Its observation cannot precede that allocation's
observation, and its publication/expiry must fit the allocation window. The
provider policy must match the existing sealed substrate policy. Immutable
policy/attestation body references are bounded but do not prove actual body
contents, fixed destination or DNS/TLS enforcement: only the actual trusted
proxy producer may establish those facts. No caller-selected URL, generic
approval boolean or parallel network-policy validator is introduced. The
MCP/search trusted body-verifying producer bridge is still missing.

Structural historical decoding/digests remain available after retirement or
revocation. Correlation to a proxy requires its preserved original allocation
snapshot/digest, not a current tombstone normalized back into active state.
Fresh permission additionally requires unchanged active current heads and the
complete canonical transaction checks. Transitions preserve all original bytes
apart from monotonic lifecycle generation/state/update time; replacement,
renewal, resurrection and generation overflow deny.

The existing [capability issuer](../../internal/serverlessharness/capability.go)
now consumes an authenticated parent's preparation right before physical
preflight. All registries sharing the issuer share that fence; registry rebuild,
authentic grant remint, concurrent requests and unknown partial-preparation
failure cannot prepare the same parent again. Physical registry preflight is
private. The process-local map has a finite 1024-parent capacity and fails
closed before I/O when full. Tombstones survive the original invocation plus
at most thirty seconds of its cleanup window; a monotonic clock high-water
rejects rewind after pruning. This is separate from the original one-use
execution gate and does not weaken it.

These are prerequisites, not durable post-claim registration. A restarted
issuer is not proof that preparation never happened. Current sole-parent
readback, trusted supervisor/proxy publication, create-once atomic registration,
lost-ACK recovery without new preparation, shared retention and real worker/
operation integration remain mandatory. The process-local prepared capability
MAC/digest is not rebuilt after restart as substitute history. Both serving
registries remain closed to owned execution. No live effect, credential,
network, deployment or #189 completion is claimed by these contracts.

## Transaction and resource bounds

Reserve and immediately-before-send use one serializable transaction and its
database timestamp. Reread current Job/Run/Attempt, lease/fence/worker and sole
parent claim, cancellation, Session/deletion/write membership/participation,
current provider resource, installed heads and admission references, every exact
current spending-authorization head and its source-revocation projection,
allocation/proxy observations, original immutable request/predecessor and every
aggregate debit. A valid MAC or historical digest is not a substitute.

The executable deadline is the minimum of original invocation, current lease,
policy/profile/evidence, approved price/terms/funding, allocation/proxy,
operation duration and remaining Attempt windows. A refresh/lease renewal or
progress report cannot expand the original scope. Cleanup has a separate bounded
authority which never permits a new effect.

Initial admission/publication bounds are ten seconds overall, at most three
transaction callback attempts and 32 SQL statements per callback (96 total).
Count retries and bytes across the whole logical call, including commit-loss
recovery; nested helpers do not reset limits. Each current-head record is at
most 32 KiB with a cap-plus-one SQL prefix and strict known fields, duplicate
rejection and exact physical scalar correlation before use. Return at most
4 MiB aggregate authority metadata per logical call. Immutable approval bodies
are at most 256 KiB, verified outside SQL; no truncation or partial JSON acceptance.

Refactor any unbounded legacy authority read used by this path into bounded
canonical projections; loading full conversation material is not an authority
lookup. Prove actual statement/byte maxima with legitimate worst-case native
fixtures before acceptance. No table scan, arbitrary blob fetch, credential
materialization, provider RPC or allocation preflight belongs in a transaction.
Public errors remain fixed content-free denial categories.

The source-only phase implementation does not establish these admission
bounds. Static inventory of the legacy managed happy path at `5b416` found
approximately 32 statements already, before the additional original/current
authority checks; missing rows and helper branches add more. This is an
inventory, not native measurement. Do not count only new head reads, raise the
ceiling implicitly, or accept an unbounded legacy helper as a bounded writer.
Keep owned scheduler admission closed until the complete existing transaction
is refactored and its legitimate worst-case native fixtures meet these bounds.

The executor prerequisite composes existing canonical StateTx helpers into the
same private `ownedAuthorityQuery`, callback and sampled database clock. Its
interface exposes only SQL queries/writes, not a raw transaction or commit.
Known canonical JSON point reads use an explicit full-query whitelist and SQL
cap-plus-one prefixes. Run, Attempt, Session and capacity metadata are capped
at 32 KiB; source-bearing outer Job/outbox and manifest metadata at 128 KiB,
separate from the strict 32-KiB inner Source/current-head bound. Every consumed
scalar is charged before validation; unsupported JSON query forms have no
unbounded fallback. Existing scalar/helper reads still need an explicit bound
and collector before inclusion in the eventual complete admission graph.

Snapshot selection transfers at most sixteen 32-KiB-plus-one metadata records,
never their blob/conversation bodies. The canonical snapshot producer has a
fixed field set, 160-byte opaque identities and one exact derived object key;
32 KiB comfortably covers that legitimate shape. On the owned path, selection
drains, charges and closes the complete bounded result set even after finding
the first valid snapshot. A later oversized row or stream failure denies that
callback. Legacy single-turn/attached selection retains its first-valid behavior
and skips oversized corrupt metadata without downloading the complete cell.

That executor-only prerequisite did not wire a serving owned admission caller, establish
the complete scheduler's worst-case bounds, authenticate installation, create
a final admission attachment or complete retention. Those remaining gates
above are unchanged; a small helper-only transaction is not their proof.

The complete existing scheduler callback is now shared by ordinary serving
admission and a private bounded **managed-only** composition. It is not a second
scheduler or an enabled owned admission port. The attached serving path keeps
its existing transaction and lease/transport writes; it is not silently subjected
to the managed 32-statement limit. All decisions and speculative result fields
are reset on every SDK callback and cleared on an unconfirmed result, including
ordinary serving admission. A rolled-back `admitted` decision cannot survive a
retry that observes a cancelled/nonpending outbox.

The private managed composition samples database time before its outbox read,
uses that time for every existing decision/write and preserves the original
absolute capacity hold deadline (no retry renewal). It includes entitlement,
slot, counters, canonical Run/quota/Job/index writes, explanation, complete
snapshot selection and audit. Scalar prefixes/collectors charge all returned
metadata before closed enum, exact integer, timestamp and canonical identity
validation. Historical reservation ID collisions cannot recreate a hold; exact
slot/reservation/Job replay remains read-only and cannot refresh its expiry.

Its opt-in metadata cache is private to one callback and serializable snapshot.
It only caches explicitly bounded canonical point JSON and explanation-head
reads, including absence. Every SQL write invalidates its table before execution;
unknown/qualified syntax clears the cache conservatively. A retry starts with a
new empty cache; cached values cannot bypass a failed budget or deadline. Only
actual SQL/materialized bytes are charged; bounded cached bytes were already
charged on their original read. No profile/funding/proxy permission is cached
across callbacks or inferred from this optimization.

Explanation selection and admission use the same ordered pure reducers and
retain every revision/fingerprint/time transition, but the bounded composition
persists their final head once rather than writing and reloading an intermediate
selection. Ordinary writers retain their existing persistence path. Canonical
Run invalidation and previous-Attempt selection ordering remain separate and
mandatory; replay never creates missing historical evidence.

Bounded previous-Attempt ordering requires the exact queried identity, canonical
tenant/Run correlation, valid domain shape and no historical timestamp after the
callback's database time; a missing previous row cannot certify ordering. The
existing Job writer validates the input manifest's exact physical identity,
canonical scope/shape and creation time before persisting the Job. Outbox update
time and replay Job/reservation historical times are similarly bounded by that
same clock. Future absolute hold/expiry deadlines remain legitimate. These
checks reuse existing reads, add no SQL, and do not alter ordinary legacy writers.

Instrumented whole-callback controls currently observe 28 SQL for cold managed
slot/counters/explanation and 31 SQL for a cold prior-denied explanation with
previous-Attempt selection. Three complete callbacks observe 84 total statements.
These are historical credential-free single-turn controls, not the owned maximum:
current Session/principal/profile/four spending/final-attachment joins are still
absent and must fit the SAME original limits, not another helper budget. Native
whole-graph evidence is recorded separately in #189. Structurally unbounded
legacy MCP lists and manifest arrays have not acquired a new supported maximum;
the private capped path does not change serving producer/consumer contracts.
Writer-first finite owned configuration bounds and the final atomic writer,
current authority, retention and authentic installation gates remain required
before any serving cutover. Neither these graphs nor their green tests close #189.

### Private owned Final admission composition

The same complete callback now has a private owned composition, still requiring
explicit fixture publication verifiers and a deterministic-fixture backend.
Public `AdmitDispatch` and both serving registries remain closed to owned profiles.
Publication fixtures exercise actual Store writers but are not authentic runtime
installation or permission for live provider/tool/search effects.

One exact-key thirteen-column join checks canonical active Session, deletion
absence, writable membership and participation at the callback's database time.
One exact-key fifty-five-column join checks the original installed-profile head
and all four original spending heads, their physical scalars, exact generations,
digests, payer, allocations and original windows. These are single SELECTs,
not batches of hidden statements or replacement-source discovery. Every returned
scalar is charged before validation; YDB JOIN relations use input-key equality,
with complete original locator predicates in WHERE.

The final immutable `(tenant, Attempt)` record preserves the exact original
Source, actual capacity reservation and sampled database admission time. Final
and its `(tenant, Session, Attempt)` reverse key use INSERT, in the SAME existing
transaction as canonical Run, quota/expiry indices, Job, explanation, slot,
counters and audit. The bounded owned Job writer also uses INSERT: a Job lacking
its Final cannot be overwritten or converted into fresh historical authority.
Exact replay requires the original Final, Job and held reservation and unchanged
current sources; it executes no writes, does not reconstruct absent history and
never renews the original absolute hold deadline.

Source producers reserve 512 bytes inside the unchanged 32-KiB authority codec
for Final's wrapper. Historical structural Source decoding remains distinct from
permission to publish a new producer package. A coherent maximal-field fixture
measures Source29,806 bytes and Final30,053 bytes; the independently calculated
160-byte-reservation/microsecond-timestamp wrapper is 247 bytes. No padded JSON
is accepted as a legitimate producer, and outer MCP/manifest configuration
still needs its writer-first supported bound before serving cutover.

Canonical Run writes and ordered invalidation/selection/admission reducers remain
immediate within the callback. Only the derived explanation's final head is
published once before commit; its pending values and the metadata cache are fresh
on every retry. An absent owned scheduler slot is observed in the same read set
without first writing a redundant provisional Ready slot. Every successful cold
admission or denial still persists the final canonical slot, including the
unchanged quota-blocked denial branch; failure rolls back the complete graph.

Native complete prior-denied owned admission with sixteen snapshots measures
32 SQL, fourteen writes and approximately 50 KiB metadata, within the ORIGINAL
10s/3-callback/32-per-callback/96-total/4-MiB limits. Exact replay measures eleven
SQL and zero writes. Scripted committed lost-ACK recovery and known rollback
reuse the same callback and budget; native contention, public-source revocation,
late failure and real-Job collision controls check canonical atomic outcomes.
These controls do not prove execution effects, every possible supported producer
shape, authenticated installed sources or completion of the whole #189 contract.

Final's reverse key joins the existing bounded Session inventory and shared
CompleteSessionDeletion workflow. Same-transaction legal holds, terminal Runs
and pending attached receipts gate deletion; an orphan index denies completion.
Global profile revocation heads/audits are retained. This covers only the Final
child: per-Attempt funding, outer effect/evidence, subordinate operation and
consent cleanup/unknown-effect disposition remain mandatory before #189 closure.

## Terminal and retention integration

Attempt admission, registered allocation/proxy records, subordinate requests,
reservations, observations, consent and immutable bodies belong to the same
canonical Run/Attempt/Session lifecycle as the outer effect. They do not get
an independent TTL or terminal reducer. Current Session deletion currently
omits explicit joins to outer effect/evidence and proposed subordinate tables;
#189 must fix that cleanup coverage before its completion.

Use the existing terminal/fence authority to prevent new registration or body
writes after cancellation/deletion/terminal. A late authenticated observation
may only fill its original unknown reservation under bounded cleanup authority,
without reopening the Attempt or appending a competing terminal result.
Pending/unknown effects require an explicit residual-cleanup disposition; do not
delete their correlation before the canonical cleanup workflow records it.
Legal holds retain the same attached material. Batch cleanup is indexed,
tenant-scoped and resumable using the existing Session lifecycle.

Installed profile heads are resource/configuration metadata rather than Session
content. Their revocation tombstones follow their existing owning resource and
registration lifecycle; deleting one Session cannot erase a global revocation
generation. Per-Attempt snapshots follow Session retention and retain references
needed to explain historical effects without keeping private request content.

## Verification and implementation handoff

Keep implementation within [#189](https://gitcode.com/urandon/sessionless/issues/189)
and its existing linked row in #176. No new epic is needed: this supplies the
current-registration prerequisite already required by its atomic-operation
acceptance checkbox. The ordered implementation slices are:

- [ ] [#189](https://gitcode.com/urandon/sessionless/issues/189): typed publication/current-head contracts, trusted installed-source
  composition, CAS/revocation/audit writers and bounded readers; gates remain closed.
- [ ] [#189](https://gitcode.com/urandon/sessionless/issues/189): trusted per-Attempt spending heads and immutable admission/funding
  attachment to canonical dispatch, explicit known component/total allocations,
  exact source-generation joins and fail-closed unsupported live funding classes.
- [ ] [#189](https://gitcode.com/urandon/sessionless/issues/189): authenticated post-claim preparation registration, exact lost-ACK
  recovery and bounded original-allocation reconciliation/cleanup.
- [ ] [#189](https://gitcode.com/urandon/sessionless/issues/189): atomic operation/debit persistence, before-send checks and one-use
  capability; typed consent persistence and shared terminal/retention joins.
- [ ] [#189](https://gitcode.com/urandon/sessionless/issues/189): full independent exact-artifact review, proportional native tests,
  exact-head CI/merge and #176 synchronization. Narrow slices do not close #189.

Use [Go testing practices](../testing-best-practices.md) and one authoritative
clock. Required counterexamples include every owner/model/resource/profile/
policy/grant/price/terms/payer/generation axis; absent, malformed, oversized and
scalar-divergent records; coherent revocation during retry and between reserve
and send; at-expiry/future observations; unknown/free/paid pricing, overflow and
each component/total budget; losing/changed claim or allocation/proxy; stale
CAS, exact/conflicting replay and lost commit; hold/deletion/terminal/late writer.
Assert unchanged debit and zero physical sends/preparations on denial.

Positive controls require a real trusted test installation, explicit evidenced
free or finite paid synthetic fixture funding with authenticated spending-head
publication and current source revocation, actual registered scripted supervisor/proxy
observations and the exact original claim. Native serializable writer-versus-
reserver races and causal gate-bypass mutations supplement instrumented unit
tests; maps and compilation alone do not prove atomicity or cleanup coverage.
Check every new canonical field with independent digest/codec goldens and
writer-first empty-backlog cutover fixtures, without compatibility fallbacks.

The later adapter/loop/compaction/MCP/search work remains #190–#197. Actual
calibration, paid/live calls, credentials, deployed substrate/two-owner proof
and activation remain #90/#92/#175 under their separate authority. Both MVP
routes and canonical WebUI product proof still join at release acceptance.
