# Bounded web search for the owned harness

Version: 0.2.5, reusable resource-grant amendment candidate. Predecessor 0.2.4
defines original-owner argument custody; this amendment preserves that contract.
Predecessor 0.2.3
defines the search-wire integration; this amendment preserves that contract.
Predecessor 0.2.2
is the independently reviewed request/result data prefix in !247; its exact
source remains frozen separately. Owners: [#94](https://gitcode.com/urandon/sessionless/issues/94),
[#179](https://gitcode.com/urandon/sessionless/issues/179),
[#180](https://gitcode.com/urandon/sessionless/issues/180); implementation #176.
Version 0.2.1 was accepted with [the minimum harness](owned-agent-harness-minimum.md) at exact
`42139dfb5cefb266adb77f221c0743b53cfb5217` in independently reviewed and CI-verified
[!160](https://gitcode.com/urandon/sessionless/pull/160), under the owner's
agreed-MVP engineering delegation. [#194](https://gitcode.com/urandon/sessionless/issues/194)
and [#195](https://gitcode.com/urandon/sessionless/issues/195) remain uncompleted.
This selects one engineering search profile; it grants no credential, purchase
or live-call authority. [The comparison](../research/web-search-mvp.md) owns the
sources and remaining broader research.

## Required product capability

The owner adds general web search to PR !160's managed MVP minimum. Provide
`web_search` as a model-callable trusted adapter: one public query, bounded source
results, then model continuation and a durable answer with source references in
WebUI. Documentation location alone is insufficient. Constrained Docs MCP
remains separately required; REST search is not MCP conformance. Both use the
same loop, subordinate operation authority and aggregate tool budget.

Propose Tavily Basic as the first adapter, not a measured quality winner.
Search provider/mode is server-admitted and frozen; no model choice, free-tier
rotation, environment credential discovery or fallback. An enabled profile
needs exact API/options/parser artifact, independently checked terms/data/cost
policy and finite useful-query evidence. Exa, Brave, Parallel and Perplexity are
explicit design alternatives, not failover routes. A rejected candidate returns
to reviewed design rather than silently changing provider.

## Grant and input

Proposed server-owned `WebSearchGrantV1` binds tenant/owner, Run/Attempt, resource
revision, independent search/data-policy digest, consent revision, provider and
API profile, fixed origin/path/method, credential reference/audience, price
revision/currency, limits, expiry and revocation generation. No credential bytes
or enclosing agent-policy/binding digest enter the grant. Add its digest beside
MCP grant digest in AgentLoopPolicyV1, then construct binding/invocation authority
in the same acyclic order. Capabilities later bind all completed digests.

This needs an explicit search-resource/credential/metering extension: existing
LLM provider credentials or AllowedMCPServers strings are not search authority.
Reuse owner-scoped custody primitives only after their exact audience, lifetime
and cleanup contracts are shown compatible. Platform-funded resources require
an explicit admission/billing owner; do not infer one from a configured API key.

Model arguments contain only `query` and optional `max_results` (1–5, default 5).
Query is nonempty UTF-8, at most 1,024 bytes; full arguments at most 4 KiB,
additional fields denied. Domains/date range/language/topic/safe-search mode are
sealed profile values, not URLs/headers chosen by the model. Propose an explicitly
named `public_web_basic` profile: broad web discovery, general topic, English
language, safe-search enabled, no publication-date restriction, no provider-side
include-domain restriction and a sealed policy denying local/metadata names,
IP literals and non-HTTPS citation URLs. An absent profile is denied, not broad
access by default. This breadth is an owner acceptance decision, not permission
to transmit private queries. A narrower profile must enumerate domains and
independently filter each result using hostname equality or dot-boundary
subdomain matching, never string suffix/substring matching. Required date filters
use a separately admitted dated profile and observed dates, never prompt claims.

### Reusable resource grant and original invocation scope

The existing `WebSearchGrantV1` is invocation-scoped DATA: its semantic digest
includes Run and Attempt. It cannot also be the constant grant identity in a
reusable installed profile. The existing profile-to-policy producer copies the
profile's digest into each freshly scoped policy, and spending publication
requires that same digest. Replacing a decoder, reconstructing a body reference
or republishing the global profile for every claim does not resolve this
contract mismatch. Preserve retained V1 bytes and identities; do not silently
remove scope fields or treat an old digest as a reusable grant.
These constraints follow the original
[grant](../../internal/domain/web_search.go),
[installed profile](../../internal/domain/owned_agent_current.go),
[profile-to-policy producer](../../internal/domain/attempt_execution_admission.go)
and [spending correlation](../../internal/domain/attempt_spending_authorization.go),
not an assumption that a new generic grant service is required.

For the genuine producer, introduce one explicit `WebSearchResourceGrantV2`.
It is reusable nonsecret authorization DATA for the exact tenant, resource
owner, billing owner, resource/revision/revocation and reviewed search profile.
Retain the V1 service, fixed endpoint/method, credential reference/audience,
data/schema/compiler identities, price/terms/currency, per-call and per-Attempt
ceilings, consent revision and original issuance/expiry. It contains no Session,
Run, Attempt, completed policy or binding digest. Its semantic digest uses a
new `sessionless.web-search-resource-grant.v2` domain, not V1's digest domain.
The distinct version is mandatory; never infer it from a digest or failed parse.

The invocation-bound child already exists: the original scoped
`AgentLoopPolicyV1`, Attempt spending publication, execution Source, parent
reservation and subordinate operation. Bind the V2 resource-grant digest in
their existing grant fields. These original records bind the actual Session,
Run/Attempt, physical claim/lease/fence, completed authority/binding, current
actor, durable MODEL call and exact consent. Do not add another invocation-grant
DTO, approval registry or parallel spending/dispatch owner. A resource grant by
itself never authorizes an invocation, a monetary debit or a send.
For this explicitly tagged V2 path, `ExternalServiceCostCeilingV1.ScopedGrantDigest`
means the grant's tenant/owner/payer/resource/revision scope; the enclosing
original policy and spending/source envelopes supply invocation scope. Update
that contract and its consumers explicitly, not as a blanket reinterpretation
of old V1 records. Consent, operation and normalized result all retain the same
verified semantic resource-grant digest plus their own exact invocation scope.

Use an optional, explicitly versioned `web_search_resource_grant` body-reference
arm in the existing installed profile. Its version is 2 and its body uses the
existing `OwnedAgentAuthorityBodyRefV1` shape: opaque locator, raw SHA256 and
exact byte count. It must identify the original operator-held immutable grant
bytes; it is not an Object Storage path selected by a caller. Authenticate this
reference together with the installed profile, its publication intent and
original source evidence. The strictly decoded body's semantic digest must
equal both the profile's existing `WebSearchGrantDigest` and search cost's
`ScopedGrantDigest`. Retain the distinction between raw body SHA and semantic
grant identity. Do not overload spending's `SourceAuthorization`: it commits
the original invocation-specific source envelope, not this grant body.

The body has a hard 8 KiB ceiling within the existing authority materialization
budget, not an additional Attempt operation-body allowance. Freeze the closed
V2 schema with canonical integers, identifiers, fixed strings, microsecond UTC
times and lowercase digests. Prove its maximum valid encoding fits this cap
before enabling a writer. Reject unknown/duplicate/aliased fields, explicit
null, noncanonical bytes, trailing input and unsupported versions. Reuse the
original bounded immutable-source reader/owner and prospective metadata charges;
add no third operation-body key arm or arbitrary historical blob reader.
Copies and a matching locator cannot reconstruct publisher ownership.

The original operator source must retain and authenticate the exact grant body
and explicit search payer/price/terms/funding evidence before initial spending
publication. Provider/substrate local no-invoice terms cannot authorize paid
Tavily. Each invocation still requires the original publisher's authenticated
finite search spending component, current profile/resource and exact original
source-envelope bytes. Reusing a resource grant across legitimate new Runs is
not replay permission, a shared unlimited balance, a top-up or renewed authority.
No body field or codec invents vendor price verification or actual funding.

The existing generic evidence list and review-receipt presence check are not
such an expenditure authorization. For the first owner-local publisher, require
one explicit `OwnerLocalSearchExpenditureV2` declaration in the authenticated
private installation manifest. It binds version 2, tenant, owner and billing
owner (the same owner for this bounded local path), search resource/revisions,
V2 grant semantic digest and exact grant-body reference, exact accepted price/
terms evidence references, USD currency,
10,000-microunit per-call and 20,000-microunit per-Attempt authorized reservation ceilings,
and original issuance/absolute expiry. The source authenticates and retains all
referenced bytes, validates the closed declaration and acceptance correspondence,
and matches the actual current installed profile before issuing spending.
Presence of arbitrary review bytes or a `known_paid` string is insufficient.
This declaration is the owner's reviewed expenditure acceptance; do not add a
second free-form approval receipt and treat its mere presence as review.
The existing manifest/source commitment owns this declaration; no new approval
map, payer registry or balance is introduced. Unsupported different-payer or
missing/unknown evidence denies this publisher rather than synthesizing consent.

Price/terms evidence uses closed owner-accepted snapshot records: exact provider
and API profile, declared source origin, observation/absolute expiry, and a
bounded immutable payload reference with raw SHA and length. Price also binds
the exact price revision, USD/request unit and a positive accepted tariff no
greater than the 10,000-microunit reservation. Terms binds the grant's data-policy
digest and credential audience. Retain and verify the exact referenced snapshot
bytes within the existing total installation/evidence bounds; no external fetch
or new storage allowance follows. Origin and observation are operator-attested
metadata, not proof of a current HTTP response, vendor signature or legal
interpretation. The original private intake authenticates the operator's
acceptance and exact evidence identities; live qualification separately checks
that the selected service still matches the frozen profile and accepted terms.

This proves an original owner's finite expenditure authorization and the
identity of accepted price/terms evidence, not vendor prepaid balance, account
credit or the final invoice. Keep that evidence confidence explicit. Actual
usage remains measured/reported/unknown under the existing accounting rules;
unknown vendor cost is not zero, and lower reports do not refund the original
reservation. Other managed/platform-funded publishers need their own existing
authenticated billing source; a local declaration does not authorize cloud
funding or consumer-subscription sharing.

New producer, consent creation/read/reconciliation and reserve/before-send
materialization must consume the same original V2 grant owner and strict body
commitment. No synthetic `owned-fixture-grant-*` lookup, caller-authored grant
DTO or fallback to V1 may serve that path. Authenticate current profile/source,
resource/revocation/consent, actor, payer, price and allocation at the original
transactional gates after out-of-SQL body reads. All original absolute windows
remain minima; a resource's longer expiry never renews the 120-second claim.

The V2 compiler binds this resource grant to the original invocation binding
and exact argument bytes. A fresh explicit compiler identity distinguishes it
from the retained invocation-grant V1 DATA compiler. Preserve V1 encodings and
goldens without adapting them into fresh V2 dispatch. The service's fixed Basic
wire/options, argument/request/result caps and original search-wire operation
arm need not acquire a second wire protocol: their compiler/profile commitments
must authenticate the selected V2 implementation. Normalized result scope and
source IDs remain bound to the original operation.

Old profiles omit the new body-reference arm completely and retain their exact
JSON/digest bytes; explicit null is not equivalent to absence. Readers must
recognize and validate V2 before any writer or live profile is enabled. An
absent arm, old fixture grant or unknown version cannot select the genuine
producer. Historical V1 decoding remains DATA-only, not a compatibility send
route. No additional record, migration, table or billing authority is added by this
design amendment; implementation and rollout remain separate.

Verification must prove that two different Attempts use one unchanged installed
resource profile but obtain distinct original scoped policies, source/spending
envelopes, consent and operations. Cross-Attempt substitution must deny at the
original reserve/send gates. Cover raw/semantic digest confusion, forged body
reference, foreign tenant/owner/payer/resource, revoked generation, price drift,
shorter expiry, ambiguous publication, malformed/oversized bodies, exact old
goldens and zero-effect denial without the actual source owner. The existing
compiler/fixture positives cannot substitute for this two-Attempt integration.

Implementation stays in #194 with #195/#191 consumers: strict V2 resource/body
contract; original profile/source publication and custody; genuine spending and
current-authority joins; compiler/consent/reserve/egress integration; then durable
result/continuation and historical accounting/deletion. Keep these as an explicit
execution checklist in the owning issue, not a rewrite of its assignment or a
new generalized grants epic. Both MVP routes and separate live gates remain.

Public task classification and permission to request search consent are frozen
admission facts, not approval of an unknown future query. The first profile uses
an explicitly public task. After a complete model response is durably recorded,
the UI displays the exact generated arguments and receives their approval before
dispatch. A model-generated query does not inherit permission to disclose private
transcript, attachments, identifiers or secrets. Any changed query requires a new
decision. If this UX or public-data proof is unavailable, the tool is disabled
for that task. Regex or model assertions cannot authorize private transmission.

## Query consent within the existing claim

Proposed `WebSearchConsentRequestV1` is a trusted pending decision in the existing
Attempt effect store, not a model instruction or another scheduler. It binds
tenant, Session, initiating user, Run/Attempt, physical invocation claim, lease/
fence, durable model-response digest and call ID, exact argument/query digests,
search grant and completed policy/binding digests, consent/revocation revisions,
and an absolute expiry. Exact arguments use an authorized immutable blob; public
execution evidence contains only the pending status and opaque locator, never
query text. Allow one pending decision at a time and at most two requests per
Attempt; their rows/blobs count within the existing aggregate storage bounds.
Do not reserve a search operation, debit its monetary budget or contact the
search provider merely to obtain consent.

Bounded authenticated BFF reads return the exact query only to its initiating
user with current Session participation and required task/resource consent
authority. The decision endpoint uses the established Web session/CSRF rules;
it accepts only the opaque request identity, expected revision and approve/deny,
not replacement arguments, a new principal or a longer deadline. A resource
transaction rechecks Web session and membership security versions, participation,
consent authority, exact request digests and current Run/Attempt/claim/fence,
cancellation, grant/policy and expiry. A compare-and-set transition writes one
immutable `WebSearchConsentDecisionV1` with the same bindings, authenticated
actor, decision and observed time. Duplicate identical decisions recover that
receipt; conflicting, stale or foreign decisions fail closed. Approval cannot
mint a grant, extend its scope or create a transferable dispatch capability.

The original worker waits within its sole physical claim. Waiting consumes the
same 120-second active wall-time ceiling and every shorter lease, grant, price,
allocation and cost window; it is not a pause or deadline extension. Reads and
UI refreshes have finite per-request and aggregate budgets. Deadline, denial,
cancellation or revocation ends the pending request with an explicit stop and
zero search sends. A late approval cannot reopen it. Worker loss is reconcile-
only: even a durable approval does not authorize a new worker, renewed admission,
resume, fresh Run replay or replacement claim to send this query.

Approval is necessary, never sufficient. The existing reserve operation atomically
rechecks the exact approved receipt and all current authority, then reserves the
search budget and binds its digest to `AttemptOperationV1`. The one-use egress
capability binds that receipt, query and original claim; recheck cancellation,
expiry and revocation at the existing before-send gate. A winning denial before
that gate yields zero sends; revocation after a possible send follows the existing
outcome-unknown and no-replay rules, not a remote-rollback promise. Only a known,
durable result allows model continuation. Arbitrary bounded model-generated
public queries remain supported; there is no static-query substitute.

### Original-owner argument custody

The consent argument `BlobRef` remains in its original Attempt namespace. It
must be produced and read by the same scoped operation-body owner that owns
ordinary request/result bytes; a map fixture or an arbitrary BlobStore reader
is not integration evidence. Add one closed physical-key arm:

```text
<SessionRunObjectPrefix>attempts/<AttemptID>/consent-arguments-v1/<scope-hash>/consent-args-v1.<size>.<sha256>
```

Derive the hash from the exact original operation scope using the existing
`sessionless.operation-body-scope.v1` domain and canonical JSON bytes. The
constructor chooses the complete key; callers supply only nonempty bytes,
at most 4 KiB. The parser accepts only the exact original scope, canonical
positive decimal size and lowercase SHA256. Reject aliases, traversal,
foreign Attempt/hash, malformed metadata and arbitrary historical paths;
never normalize a path or fall back to a broad prefix reader. Existing
ordinary relative references and physical keys remain byte-for-byte unchanged.

Both arms share the original Store state, synchronous I/O gate, caller context,
operation deadline, sticky uncertain-write latch, prospective physical write
and byte charges, and bounded inventory. Charge and remember the actual
predicted `BlobRef` before copying or PUT; failed or ambiguous PUT/readback
does not refund capacity or disappear from the inventory. Consent arguments
consume the existing policy-derived `2 × MaxOperations` write ceiling, at most
24 keys, and the same aggregate body allowance. These are joint ceilings:
12 distinct request/result pairs plus two argument bodies cannot all fit.
The producer must reserve existing capacity or reject before effects; neither
an extra ledger nor a larger limit follows from this amendment.

The existing body reader may distinguish only the ordinary reference and this
exact closed consent key. The existing installed reader slot keeps its original
owner; no additional reader injection or reconstructed capability is needed.
Verify actual bytes, size and raw digest on each read. Consent metadata alone
does not authenticate the MODEL call, grant, initiating actor or current claim.

Physical cleanup prevalidates every inventoried object across both arms before
the first delete, then verifies deletion and absence under the one original
retained cleanup deadline. Preserve the namespace-anchor digest based on the
ordinary prefix: it identifies the original scope owner, not an exhaustive
prefix scan. The existing ordered physical-inventory digest commits every
exact body key and metadata, including unreferenced attempted writes. Returned
inventory copies and the original opaque erasure receipt remain unchanged.

Residual V2 may validate either exact physical arm, while operation request and
response references still resolve only through the ordinary arm. Every consent
argument must match its exact inventoried body. Preserve all ordinary V1/V2
serialization and digest goldens; an additive key arm does not reinterpret old
records. Deploy compatible readers before enabling any writer: old readers
reject new-arm inventories rather than silently accepting them.

This custody repair does not complete the worker loop. The actual producer must
also charge argument storage in the original durable accounting, retain real
consent states in terminal residuals, and join the original finalizer and
historical deletion verification. Until that integration is proved, existing
MODEL/compaction-only terminal and deletion paths must continue to deny
unsupported consent-bearing completion. Do not relax a prepared-writer guard
or manufacture consent/erasure evidence to obtain a success.

Required controlled proofs cover real owner write → private Store read → mixed
inventory → cleanup; unchanged ordinary goldens; cross-scope and cap rejection;
shared copy/concurrency gates; combined count/byte exhaustion; uncertain PUT,
failed/late readback and Close; mixed cleanup failure before any delete; strict
ordinary operation lookup; and unchanged unsupported terminal/deletion denial.
No table, migration, credential, live call or runtime activation is introduced.

## Adapter and network boundary

Trusted Go adapter sends one POST to fixed `https://api.tavily.com/search` through
the same attested proxy as provider/MCP egress, with a distinct sealed search
operation capability and scoped credential. There is no direct worker Internet
path, ambient proxy, browser cookie or shared LLM key. Require verified public
DNS/IP/SNI/TLS, deny redirects/private/metadata addresses and DNS rebinding.
Only the admitted method/path/headers/body/options are allowed. Current one-shot
provider POST policy is not automatically compatible; #90/#92 must prove the
versioned search extension on the exact substrate.

Freeze Basic depth, general topic, max five results, include_answer/raw_content/
images/favicon false, auto_parameters false, and explicit domain/date/language
options supported by the accepted schema. Request usage metadata where supported.
Do not activate extract/crawl/map/research endpoints. Unsupported filters fail
preflight, never disappear from the request. Profile/schema drift stops enablement.

### Distinct argument and physical-request commitments

Model arguments and the search-service POST body are not interchangeable.
Retain the exact original argument bytes, their digest and byte count, and the
digest of the exact decoded UTF-8 query approved through existing consent.
Compile the fixed reviewed profile into a separate immutable JSON body; commit
its actual bytes, digest and size. Fixed options, JSON escaping and encoding
must participate in that physical commitment. A profile/compiler identity also
belongs in the frozen correlation. No credential bytes enter either body.

The model argument limit remains 4 KiB and the decoded query limit remains
1,024 bytes. The fixed body may exceed 4 KiB for otherwise valid escaped input.
Use a separate hard **8 KiB physical search-request ceiling**, verified against
the actual compiler output and the maximum admitted argument shapes. The worst
JSON string expansion is six bytes per query byte; the fixed profile overhead
must separately be proved below 2 KiB, with the actual smaller bound recorded.
Do not truncate, rewrite or narrow legitimate model arguments merely to satisfy
the old argument-sized request assumption. This ceiling is not an increase to
the aggregate Attempt body budget: charge the actual physical request plus
reserved normalized result within the existing 1 MiB aggregate and all shorter
admitted limits. The 24 KiB result, 256 KiB response, one POST, 15-second-or-shorter
window, two searches/four tools and monetary ceilings remain unchanged.

Current AttemptOperationV1 and fixture evidence assume argument-sized tool
requests, including equality between argument and request commitments. They
cannot authorize this production body by relabeling it as a fixture or weakening
those checks. Before dispatch integration, define and independently review an
explicit versioned search-wire arm in the original operation/evidence and
before-send pipeline. It must verify the authentic complete model call and
4 KiB argument bound separately, recompile the frozen profile, compare every
physical byte/digest/length and charge it to the original reservation. Consent
continues to approve the exact original arguments/query, not replacement bytes.
The new search encoding's ArgumentsBytes must match the authentic durable model
call and consent blob. RequestBytes/RequestDigest describe only the encoded
body, not headers or TLS/RPC framing. The policy compares ArgumentsBytes with
MaxToolArgumentBytes and physical bytes with the separate search profile cap.
Preserve existing immutable operation identities and historical reconciliation;
no silent reinterpretation or generic MCP/provider limit change is permitted.
Keep the exact original V1 representation and digest for retained records: an
extra required field would change strict re-marshalling and historical digests.
Unknown encodings fail closed; do not infer ArgumentsBytes from RequestBytes or
reconstruct erased material to grant another send. Retained pair, recovery and
deletion readers must be covered by the versioned integration's tests.

The pure compiler/decoder and typed grant/result are data, not send authority,
current resource or price approval, authenticated observations or a durable
continuation result. Until that original integration and the consent bridge are
proved, registration stays off. Reader byte limits do not prove cancellation of
an arbitrary blocking reader: the actual attested HTTP owner must enforce the
original remaining deadline and cancellation without a detached read goroutine.

### Closed search-wire encoding and original Store integration

Use one optional `search_wire` arm on the existing operation envelope, not a
parallel operation ledger or a replacement for its physical claim. Its own
version is 1 and its API profile is exactly `tavily.search.basic.v1`; it requires
`arguments_bytes` and the frozen compiler artifact digest. The argument count
is positive and at most 4 KiB; the physical request is positive and at most
8 KiB. The original policy compares its tool-argument ceiling to the argument
count, while physical bytes remain charged with the normalized result to the
same aggregate body reservation. This arm is exclusive to `web_search`.

An absent arm remains the original V1 representation, limits and digest. Omit
the nil field completely: preserve all existing field order, operation digest
domain, reservation and observation bytes and historical identities. Unknown
arm versions/profiles, partial arms, explicit null in a canonical stored record,
kind substitutions, duplicate/aliased/extra fields and trailing JSON deny.
Clone the pointer-owned arm deeply through operations, reservations and grants;
a caller must not mutate a sealed capability through a shared pointer. An old
reader cannot accept the new arm by dropping it: exact canonical re-marshalling
must reject the changed representation. Deploy reader support and drain old
readers before activating any new writer; this document authorizes neither.

The Tavily producer must require this arm even for a small request. Neither
absence of the arm nor equality of digests selects a legacy fixture fallback.
Before reserve and send, the original Store must authenticate the complete
durable MODEL call, canonical initiating-task identity and exact consent
argument blob; their actual byte count must match `arguments_bytes`. Recompile
those exact arguments with the authenticated grant/profile/compiler and compare
every physical byte, digest and size. Public Binding/grant/result structs and
retained DATA decoders are not dispatch provenance.

The structured grant's domain-separated semantic digest is distinct from its
serialized body's raw SHA256. Original publisher-owned custody must authenticate
both the scoped locator/body commitment and the semantic digest against the
existing Source/profile/search spending component. Do not reuse the fixture
grant locator or introduce another current-grant table. Recheck the original
parent, resource/revocation/consent, price/terms/payer, allocation/proxy and
approved receipt in the existing reserve and before-send transactions. If the
authentic producer is absent, the new arm denies before generic fixture routing,
debit, capability issuance or send. Historical decoding is not fresh admission.

Per-call charging and result continuation require that same original producer.
Authenticate the 10,000-microunit call reservation against the selected tariff,
then debit the existing search component and aggregate counters, at most 20,000
across two searches. Do not merely relax the old full-component debit, infer
the tariff from a compiler constant, refund lower reports or fabricate money.
A future search-only successor rule requires an authenticated known-price
ceiling, a known durable normalized result and unchanged worst-case debit.
Provider-reported credits and unknown USD remain separate from reservation;
unknown effects, overruns and missing provenance prohibit continuation. Existing
MCP and legacy fixture successor/accounting semantics remain unchanged.

Verify golden original V1 JSON/digests, new closed-arm round trips, deep-copy
isolation, valid arguments whose wire exceeds 4 KiB, consent argument-size
correlation and zero-effect denial without the original producer. Mixed-history
recovery, retained tool pairs/capsules and deletion must use the same strict
reader before a successful search turn is claimed. The current text-only
terminal path cannot be called tool-compatible merely because an operation
decodes. A versioned reader/denial foundation is a prerequisite, not completion
of the required consent → reserve → HTTP → durable result → model answer path.

Search results are data only. Citation URLs grant no network capability and are
not followed, DNS-probed or downloaded by this tool. Fetch/open/find, authentication,
cookies, arbitrary URLs, files/PDFs/images and browser interaction are deferred.
Even HTTPS citations require syntax/host validation: reject credentials, IP
literals, local/metadata names, non-HTTPS schemes and disallowed hosts. Do not
claim a link is safe to browse or its page was read merely because it is returned.
Syntactic hostname checks cannot prove a citation's present DNS addresses. This
does not weaken provider egress validation: the worker connects only to the
fixed search-service origin. A later fetch requires its own DNS/SSRF enforcement.

## Normalized results and citations

Proposed `WebSearchResultV1` is version 1 with exact operation/call identity,
profile revision, query digest, observed retrieval time and an ordered array of
at most five sources. Each source has an operation-scoped opaque `source_id`,
original rank, HTTPS URL, title, bounded untrusted excerpt, optional observed
publication time and explicit excerpt truncation marker. Provider/backend
identity is recorded as provenance, not an index-independence guarantee.

Publication time is not retrieval time. Missing/invalid publication time remains
unknown; it cannot satisfy a required publication-date condition. Do not invent
canonical URL, author, verified fact or full-page coverage. Deduplicate exact
normalized URLs while preserving first rank; stripping tracking parameters or
merging different URLs requires separately proven canonicalization. Source text
cannot change instructions, grants, routes, prices or cancellation behavior.

Proposed per-source byte limits: URL 2,048, title 512, excerpt 2,048; normalized
result 24 KiB, below the loop's 64 KiB tool-result cap. UTF-8 text may be reduced
to an explicitly marked excerpt prefix; required IDs/URLs and JSON structures
are never partially truncated. Invalid required fields reject that source with
a stable warning; malformed/oversized provider JSON rejects the whole operation.
If all candidates are filtered or unusable, return known empty evidence, not
fabricated sources. Provider score/ranking internals, raw errors and bodies stay
out of the public contract. Retention/deletion follows authorized Attempt/Session
operation blobs; no global or cross-owner result cache is introduced.

The model final response may cite only source IDs from durable successful search
observations in that Run/Attempt. Add a reviewed typed citation manifest to
normalized model output, WorkerCompletion/output artifacts and public assistant
projection; existing free text alone cannot prove citation provenance. Trusted
finalization resolves references to safe source metadata and rejects unknown or
cross-owner references. The immutable output binds operation/response digests,
source ID/URL/title and retrieval/observed publication times. Public metadata
omits query text, credentials and private receipts.

WebUI renders these source links accessibly and distinguishes search excerpts
from fetched pages. A returned source link proves neither claim support nor
page-reading; separately evaluate claim-to-source relevance. Compaction preserves
source IDs/operation digests with complete tool pairs, never promoting a summary
or provider-generated answer into source evidence. No raw provider reasoning is
exported. A user may receive an explicitly unsupported answer, not fake grounding.

## Effects budgets and failures

Add operation kind `web_search` to proposed AttemptOperationV1. Reserve before
send, persist the validated result before continuation, retain authentic failed/
unknown observations, and recheck grant/current authority before send/publication.
All searches count within the existing four total tool dispatches; at most two
searches per Attempt, sequential, with no duplicate query/arguments replay under
a new model call ID. Search does not increase the eight-model-call, 120-second
active-time or other aggregate ceilings. No automatic SDK/HTTP retries.

Proposed search limits: one provider request per operation, 15-second deadline
or shorter remaining authority window, 256 KiB aggregate decompressed response
bytes, one response, no streams/pagination/crawl. Byte/time limits apply while
reading, not after unbounded buffering. Unexpected content type/decompression,
malformed JSON or unsupported options yield a sanitized explicit failure.

Seal a fresh price revision in existing microunit monetary admission. Current
published Basic PAYG quote is USD 0.008/request; propose a USD 0.01 per-call and
USD 0.02 aggregate search reservation, within the total Attempt ceiling. This
headroom is not a vendor price guarantee. Any incompatible terms/price changes
deny new admission. Record observed credits/cost or unknown separately from the
reservation; unexpected billed usage halts further work and remains accounted.
Search cost and returned-context model tokens both count; free credits are not
assumed available and unknown cost is not zero.

Distinguish rejected_before_send, known_empty, known_service_error, malformed/
overflow, authority_denied, cancelled_before_send and accepted_outcome_unknown.
HTTP 200 is not proof of valid results. After a possible send, timeout, lost reply
or cancellation remains outcome-unknown unless trusted transport/service evidence
establishes a known outcome. Preserve potential charge and prohibit replay or
model continuation on unknown, as in the minimum loop. Known errors may support
an honest tool-unavailable answer; no corrected physical retry or implicit route
change follows. Cancellation is local stop/cleanup, not remote rollback/refund.

## Verification and implementation handoff

Credential-free fixtures instrument actual sends and reserve/send/persist/publish
barriers. Cover exact options, denied/private queries, consent digest changes,
two owners, grant/lease/price expiry, domain-boundary tricks, unsafe URLs, unknown
dates, deduplication, clipping/overflow, malicious snippets, 200-with-error/429,
late replies, cancellation, persistence failure and crash with zero replay.
Verify model continuation uses only durable results and citation manifests reject
invented/cross-owner sources. Test refreshed/compacted views retain exact source
lineage and unchanged canonical facts. Use documented Go testing practices.
Consent fixtures use fake time and controlled barriers: approval before expiry,
denial/expiry/revocation winning before reserve or send, changed membership/fence/
arguments, duplicate versus conflicting decisions, approval lost-response recovery,
late approval, bounded polling and worker loss after approval. Assert exact
receipt/claim binding, no deadline extension and zero unauthorized or replayed
sends, not merely a UI status or model assertion.

Live acceptance is separately authorized: frozen public queries/options/artifacts,
credential and billing owner, finite request/model budget, data terms and exact
substrate. Execute a genuine model → search → model answer with valid source
links in WebUI plus a separate useful MCP round trip. Measure relevance/grounding,
freshness, latency, error rate and full cost independently under the research
evaluation plan. Scripted tests alone cannot satisfy this gate.

After accepted #180 design, create bounded linked #176 children for grant/resource/
credential/metering contracts, a query-consent WebUI/BFF/worker bridge with its
durable decision/authority and same-claim wait contracts, trusted adapter/proxy
and canonical operation integration, citation manifest/WebUI projection, and
deterministic/live evaluation. The consent bridge is a named prerequisite of
search activation; deterministic proof alone does not authorize a paid/live call.
Reuse common MCP/loop/cost groundwork; do not duplicate ledgers or supervisors.
#94's broader provider matrix, NEEDLE reproduction, private queries and
PageFetchV1/browser research remain open, not hidden extra MVP gates.
