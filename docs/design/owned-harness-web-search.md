# Bounded web search for the owned harness

Version: 0.2.12, explicit local worker credential-backend assembly. The startup
contract below supplies the existing encrypted backend without discovering keys
or changing its wire. The 0.2.11 wait limits below
clarify per-call SQL limits and aggregate waiting costs without extending the
original claim. The 0.2.10 cross-process private consent-reading role
clarifies original-owner argument custody; the 0.2.9 retained formats and
0.2.8 search-credential contracts are unchanged. The credential section
extends 0.2.7's original actor, tool-catalog lineage
and executable-readiness contracts without replacing them.
Predecessor 0.2.6 defines original-grant consumption and
historical stop; this amendment preserves those contracts.
Predecessor 0.2.5 defines reusable resource-grant custody; this amendment
preserves that contract. Predecessor 0.2.4
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

New producer, consent creation/read/current-authority reconciliation and
reserve/before-send materialization must consume the same original V2 grant owner and strict body
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

## Original producer, tool surface and readiness

This amendment closes an implementation-contract gap, not a new product scope
or research program. The existing [configured MODEL preparation](../../internal/ydbstore/local_prepared_model.go)
and [compaction preparation](../../internal/ydbstore/local_compaction_generation.go)
build an MCP-only catalog and compare old requests with that catalog. Existing
[consent authority](../../internal/ydbstore/web_search_consent_authority.go)
uses the fixture's owner-as-initiator convention. Neither convention is sufficient
to introduce genuine model-generated search. Pure `CompileV2` and a valid grant
are not an installed operation owner. The original assignment remains
[#194](https://gitcode.com/urandon/sessionless/issues/194), with the original-worker
consent bridge in [#195](https://gitcode.com/urandon/sessionless/issues/195).

### Canonical initiating actor, then private-owner policy

Derive the initiating actor from the exact canonical user-message event named
by the authenticated `Run.TriggerEventID`; its `SessionEvent.AuthorUserID` is
the actor. [Canonical ingress](../../internal/ydbstore/canonical_ingress.go)
writes the event and Run together. The original prepared snapshot already owns
the admitted canonical event range; reuse that bounded source rather than a
caller-authored user ID, credential owner, model argument or summary assertion.
Validate the Run/worker-job correspondence, tenant, Session, trigger ID, event
kind and Run correlation before using its author. Missing, duplicate, conflicting
or unavailable original provenance denies genuine consent. Compaction cannot
replace this provenance with its summary; preserve the protected triggering
task and its original canonical identity.

The bounded private MVP still requires this independently derived actor to equal
the admitted Source/profile owner. That equality is a policy check, not how the
actor is discovered. Different initiators, delegated resource use and federation
are not added. Existing fixture-only paths retain their explicit grammar; no
fixture convention supplies a missing genuine actor or grants a search send.

The original private holder carries the authenticated trigger/actor commitment
through creation and preparation. Current transactions rejoin that same Run and
canonical provenance plus membership, Session participation, profile, spending,
resource/consent/revocation revisions and the physical claim/fence. Private
discovery, query read, decision, reconciliation, reserve and before-send must
agree with the retained request's initiating user. Authenticated decisions still
require the current Web session and CSRF/security-version checks. An immutable
past author alone is not current membership or decision authority. Reuse existing
bounded reads and projections; no actor registry, new persistent author copy or
additional table is required. Stop-only reconciliation remains negative-only
and never turns missing provenance into disclosure, approval or fresh execution.

### Freeze each MODEL catalog once

Select and freeze the ordinary MODEL tool catalog once in the original invocation
before its first `FreezeRequestV1`. Use a private `localToolSurfaceV1` composition,
not a public plugin registry: the supported catalog representation stays unchanged; the
search-capable composition adds exactly the reviewed `web_search` schema beside
the admitted MCP descriptor. Catalog order, descriptions and schema bytes are
deterministic and participate in the existing frozen request's digest. Reject
name collisions, unknown tools and changed schemas. Never rename MCP as search
or advertise a tool whose original execution path is absent.

The existing [FrozenRequestV1](../../internal/configuredinference/contract.go)
and reserved request-body digest are the per-MODEL commitment. There is no new
durable catalog table or second operation identity. Retained MODEL/COMPACTION
requests validate their own strict canonical bytes, original issuer/reservation/
observation and original profile/grant correspondence. A bounded DATA decoder
may recover their catalog from those authenticated bytes; it does not create
dispatch authority. Validate the supported original catalog's exact descriptor
and compiler identities, not equality with the newest prepared tool array.
An authentic old MCP-only or omitted/empty catalog stays exactly that catalog;
it does not acquire search when a later request can use search. Conversely,
arbitrary old schema bytes are not accepted merely because they decode.

COMPACTION itself remains tools-disabled: its request must have no tool catalog
and cannot dispatch a tool. The compaction generation's fit check and minimum
ordinary probe must include the exact original ordinary MODEL catalog, including
the real search schema when installed; a smaller tool-free fit probe cannot
stand in for the actual next MODEL request. Catalog DATA carried for that future
request is not a tool grant to the compaction model.

Apply the same historical rule at ordinary preparation, compaction source/history loading,
derived-view restoration and response/call correlation. A normalized response
is interpreted using the request that actually caused it. Compaction rebuilds
the derived context while preserving old causal
pairs and their original catalogs. For the current Attempt, its next ordinary
request uses the originally frozen surface, not a newly discovered catalog;
current authority still gates each operation. If original grant/issuer evidence is missing,
use only the already supported authenticated retained DATA/denial path; do not
reconstruct erased bytes, substitute a current grant or mint a serving holder.
Preserve all existing empty/MCP-only request bytes, digests and golden tests.
Current grant expiry/revocation still denies a new call even when the historical
request remains valid DATA.

### Readiness means concrete installed execution

Before the first MODEL, the private invocation composition may expose search
only when the following concrete code paths and their original owners are
installed together. This is installation of the closed producer/consumers, not
construction of a future call's pending request or approved holder:

1. Original authenticated V2 resource-grant/expenditure custody and current
   policy/spending/resource correspondence.
2. The genuine durable MODEL-call consumer, canonical actor and original
   argument-body owner, pending consent creation, and the authenticated #195
   decision/worker bridge.
3. The original claim's bounded wait and installed original-entry method that
   produces a receipt-bound search holder only after the durable call and its
   stored approval, plus reserve/before-send gates and the distinct search
   credential/HTTP owner.
4. Bounded typed result persistence and successor/accounting validation, so an
   approved emitted search can either continue honestly or end through a
   supported durable failure path without orphaning its consent/custody state.

No query, pending record, approved receipt, per-call search holder or monetary
debit is required or created at catalog freeze. Those per-call objects arise
only after the genuine durable MODEL call and decision in their stated order.
An original-entry producer method is installed code, not an injectable callback
or a placeholder holder. Its complete joined controlled proof is required before
this composition is eligible to advertise search.

Readiness is a consequence of constructing these concrete owners in the existing
runtime composition, not a Boolean, grant presence, caller callback, public DTO,
fixture approval, noop transport or request-only placeholder. Authoritative
profile/configuration admission and finite budgets still apply. Do not add a
generic capability service or another lifecycle controller. Installation is not
query approval: an initially consent-free admitted public task may advertise the
tool once that complete bridge exists, then obtain consent for the exact query
only after the MODEL response is durable. Missing/disabled readiness omits search;
a model nevertheless naming it is rejected before pending creation, debit or send.

Source-only actor/catalog foundations may land without advertising search.
During an Attempt the original composition cannot gain search by reconstructing
a new holder after denial or availability drift. Current revocation/closure
always denies later prepare/reserve/send; it neither edits an old request nor
renews a deadline. The real search registration stays off until the concrete
bridge is proved. Controlled genuine MODEL tests may use installed scripted
credential-free HTTP/decision owners, never a production fixture bypass; their
evidence is not real service or activation authority.

### Coherent implementation handoff and verification

Use these bounded execution items under the existing tasks; maintain their
linked checklist in comments, without rewriting original assignments:

- #194 with #191: derive/authenticate the canonical initiating actor and validate
  per-original-request catalogs through ordinary/compaction/history consumers;
  preserve old golden bytes and deny genuine search until execution is ready.
- #194 with #195: the original entry consumes its genuine durable MODEL call,
  writes exact arguments through `WriteConsentArguments`, creates the existing
  pending record and returns only an opaque original-holder handle. It accepts
  no caller-supplied actor, call, arguments, operation, grant or approval facts.
- #195 with #194/#190: join the authenticated decision bridge, same-claim bounded
  waiting and receipt-bound prepared search to distinct credential/HTTP/result
  owners; only then add the search catalog and genuine model-to-tool routing.
- #194/#191/#197: persist and consume the actual typed result, preserve conservative
  accounting and supported failure/history/cleanup semantics, then prove model
  continuation and the required durable cited-answer projection. Citation UI
  and quality measurements remain required for the full outcome, not hidden
  prerequisites for an honest earlier actor/catalog implementation.

Capture and creation share one existing ten-second logical-call budget: three
SQL callbacks total, at most 32
statements per callback/96 total and 4 MiB metadata including out-of-SQL bodies.
Compose private capture/create phases; do not nest the current two-transaction
`CreateWebSearchConsent` inside a capture that has consumed its allowance, reset
the budget or perform body I/O in an SQL callback. Consent preparation does not
reserve a search send or monetary debit. Existing argument/body storage charges,
sticky uncertain writes and shared 24-key inventory still apply. Wait consumes
the original at-most-120-second active window and every shorter authority limit;
search HTTP is at most 15 seconds within those original windows. Authenticate
the accepted 10,000-microunit call reservation/20,000 per Attempt; reported credits
are not measured USD and lower reported usage does not refund the reservation.

Actor/catalog foundation tests first prove their own causal RED/GREEN against
the existing canonical source and genuine MCP-only history; they do not need a
synthetic search dispatch to demonstrate those contracts. Once the concrete
search composition exists, required producer RED reaches a genuinely prepared/durable configured MODEL
search call and fails at the missing original producer, not unrelated setup.
GREEN follows the actual original call, arguments, actor, pending record,
trusted stored decision and same-claim receipt; a coherent HTTP/result slice
also proves one owned send, durable result and successor before claiming that
path. Negative controls cover wrong/absent trigger author, owner impersonation,
membership loss, changed query/catalog, stale fence, revoked grant, denial/late
or conflicting approval, lost ACK, source closure and shared count/byte exhaustion.
Assert zero unauthorized sends and no renewed deadline or blind retry. Genuine
old MCP-only history must still prepare and compact after search is installed;
current revocation must stop execution without rewriting that history.

Consent/actor DATA and Store tests use the existing populated ordinary/connected
CI selections. A genuine fake-HTTP search integration needs its own explicitly
populated selector and additive instrumentation of the original transport, with
reviewed CI invocation and real aggregate timing. Do not disguise search tests
as MCP or add a tagged test that no required job selects. Preserve held fixtures
and existing MCP/legacy semantics. No live model/search/credential/substrate
proof, new table/migration or profile activation follows from this amendment.

## Closed search credentials and immutable object custody

This amendment supplies the missing credential boundary for the existing
MODEL → consent → search HTTPS → durable result → MODEL path. It is not another
catalog-only foundation or proof that search is executable. Preserve all prior
grant, canonical-actor, catalog, original-loop, historical STOP, body-custody,
accounting and readiness contracts, including old canonical bytes and goldens.

### Source-pinned search resource, never an LLM credential

Add one closed `web_search_account` resource kind to the existing provider
credential binding/audit/locator contracts. Its only supported service audience
is `tavily.search`, with direct invocation delivery. Explicitly reject this kind
in every MODEL `HarnessBinding` backend and MODEL profile locator; extending a
generic resource validator alone must not accidentally admit it as an LLM.
Do not model Tavily as an API/router LLM account, subscription or MCP server.

Keep `WebSearchResourceGrantV2` and its semantic/raw commitments unchanged. Add
an optional, strictly decoded nonsecret `search_credential` declaration to the
original authenticated installation manifest: version 1, the exact credential
resource kind/ID/owner/revision/generation/invocation mode, and fixed audience.
Its resource ID equals the original grant's `CredentialRef`; owner and tenant
equal the original private profile/grant owner, and revision/generation equal
the exact current credential binding admitted for that declaration. Search
grant resource revisions and credential-binding revisions remain distinct:
validate each against its own original commitment, never infer their equality.
No `SecretRef`, key, file path or caller-selected backend belongs in this DATA.

The existing whole-manifest `OriginalSource`/`InstalledManifest` commitment
authenticates this declaration along with the original grant and expenditure
evidence. The original Source retains and resolves it; the request cannot supply
another credential locator or audience. Omission preserves old bytes and means
search credentials are disabled, not environment discovery or selection of the
latest generation. Reject null, duplicate/aliased/unknown fields and versions.
Deploy compatible readers before enabling writers; no retained V2 grant is
rewritten and no new grant, source, credential or funding authority is inferred.

### One invocation service, distinct SEARCH issue arm

Extend the same `providercredential.InvocationService` with a closed SEARCH
issue arm, not a fake MODEL binding or general audience/plugin selector. Its
input comes from the original receipt-bound prepared search and Source-owned
declaration. Authenticate the exact original durable MODEL call, query/consent,
canonical initiating actor, private owner/payer, grant/profile/resource and
price/terms revisions, original spending/allocation and current lease/fence.
Issue and materialization must match the declared credential revision and
generation; rotation cannot silently substitute a fresh key into an old claim.

Reuse the existing handle map, synchronous at-most-once materializer, generation
fencing and release/join machinery. A distinct search handle binds the original
Run/Attempt/worker/lease/fence, search operation/receipt and fixed audience. Only
its SEARCH consumer can borrow that key, directly, for the one owned search
wire; never pass the MODEL bearer to search or the search key to MODEL/MCP.
The original reserve and before-send gates still authorize the actual POST;
an issued/materialized credential alone authorizes neither debit nor send.

A no-auth local MODEL must still own this search credential lifecycle. Remove
the existing MODEL-bearer-only composition assumption through an explicit
service-credential arm under the same original cleanup owner, not a pretend
bearer or second controller. Search handles, borrowed bytes, owned header copies
and actual sockets join within the original remaining operation/cleanup windows.
No credential adds time: retain the 120-second-or-shorter claim, 15-second-or-
shorter search limit and existing SQL/body budgets without reset or nesting.
Clear owned mutable secret copies after actual I/O joins, then release the
one-shot handle before the next MODEL. Cancel, Source close or failed cleanup
cannot detach a consumer, reopen materialization or authorize replay.

### Production encrypted backend in the existing Object Storage adapter

Implement `ProviderCredentialSecretStore` inside `internal/s3store`, using its
actual private SDK/IAM object clients and a typed production constructor. Inject
one explicit epoch and dedicated 32-byte encryption key; key acquisition/KMS,
real cloud credentials and live qualification remain separately authorized.
Reject other epochs; do not borrow the Source HMAC key or discover environment
keys. This common backend requires no user-host filesystem. Reuse the existing
YDB binding/CAS/cleanup/candidate-fence records as the sole resource/generation
authority; no table, active-binding ledger, generic `BlobStore.List` or copied
subscription-store state is added.

#### Local worker startup and injected encryption key

For the opt-in local worker command, select the dedicated encryption key through
two explicit nonsecret operator settings: `WORKER_SEARCH_CREDENTIAL_KEY_FILE`
and `WORKER_SEARCH_CREDENTIAL_KEY_EPOCH`. The path is absolute and canonical;
the epoch must satisfy the existing encrypted-backend contract. The environment
carries neither key bytes nor a search-provider secret. Do not search home
directories, infer a key from another setting, generate a key at startup, reuse
the Source HMAC key, or substitute an epoch. Inherited descriptors would require
a new supervisor protocol; they are not this local command's injection path.
Cloud key acquisition/KMS and real provisioning remain separate operations.

Distinguish authenticated declaration omission from Source failure before key
I/O. Add one nonsecret `Source.SearchCredentialConfiguration` accessor returning
an owned declaration copy, or nil only for an authenticated omitted declaration.
It validates the original installation/receipt and lifetime; closure, replacement
or invalid evidence returns an error, never omission. This construction DATA is
not current permission. Serving still uses the existing exact-profile
`ReadSearchCredentialDeclaration` and all binding, grant, consent and send checks.
Omission requires both operator settings absent and performs no key-file I/O.
A present declaration requires both valid settings; otherwise startup fails
before the worker accepts invocations. Nonlocal execution rejects stray local
key settings and constructs no local credential service.

The command-private Unix loader walks directory descriptors without following
symlinks and opens only the selected regular file, owned by the effective user,
mode0600, one hard link, exactly32 bytes and not an all-zero key. Bound the read to33 bytes;
before and after reading, verify that the opened descriptor, no-follow named
entry and canonical parent/path binding still identify the same selected file,
with unchanged size and permissions. A still-valid old descriptor does not
permit a replaced name. Close every descriptor on every outcome and reject
unsupported platforms. Return only a
redacted error. Neither keys nor private paths enter logs, public DTOs, the
installation manifest or process arguments. Clear every caller-owned mutable
key buffer on every exit, including partial intake, failed validation and failed
backend construction. After successful construction, immediately clear the input;
only the encrypted backend retains its own bounded copy.

After the existing YDB and private S3 clients are constructed, build one existing
encrypted credential adapter with that key/epoch and the actual binding Store.
Construct one concrete InvocationService with the existing clock and ID source;
its unsupported MODEL delivery-planner arm is closed and cannot issue a MODEL
delivery template. Pass this exact optional owner through LocalRuntime and the
immutable Store/bridge/keeper tuple. Do not install a fake ready object, a second
binding store, a mutable setter, environment secret discovery or a second SDK.
This constructor wiring alone does not advertise SEARCH.

The command retains the adapter for process lifetime. First drain HTTP callbacks
and join all invocation cleanup; then close the encrypted adapter with a bounded
context before closing Source and YDB. If consumers do not join, retain the
borrowed owners/key state and report failure rather than asserting erasure or
closing dependencies beneath them. Startup-failure paths obey the same ordering.
Controlled tests cover omission without I/O, missing/stray settings, exact key
and epoch validation, symlink/hardlink/owner/mode/size denials, named-entry
replacement while the original descriptor remains valid, clearing on intake
and constructor failure, exact-owner propagation, default-off behavior and drain/close failure.
Use only test-owned synthetic keys and peers; these tests grant no real key-file
read, credential provisioning or live backend access.

Use one private constant namespace, outside ordinary artifact/presign paths:

```text
<private-root>/<hash(exact locator)>/<fixed UTC microsecond time>.<hash(candidate scope)>.<random object ID>
```

Hash canonical unambiguous fields. The opaque bounded reference carries only
the version/epoch/time/scope/object identity needed to derive that exact key;
it is at most the existing 512 bytes and remains redacted on formatting, errors,
public JSON and telemetry. Callers never choose a path. Persist only this V1
wire: eight ASCII magic bytes `SLPCS001`, uint32 big-endian metadata length,
uint32 big-endian ciphertext length, raw 12-byte nonce, canonical metadata JSON,
raw ciphertext including its 16-byte tag, then a raw 32-byte MAC. Metadata is
at most 4 KiB; ciphertext is at most 64 KiB + 16 bytes and also respects the
configured secret limit. Reject inconsistent lengths, trailing bytes and
oversize before allocation; no plaintext JSON/base64 or alternate wire encoding.
Metadata field order is exactly `version`, `epoch`, `tenant_id`, `owner_user_id`,
`resource_kind`, `resource_id`, `resource_revision`, `credential_generation`,
`mutation_id`, `reference`, `fingerprint`, `created_at`. Version is 1; include
the complete scope, persisted raw opaque reference, fingerprint and original
canonical microsecond UTC CreatedAt. Reject unknown/duplicate/null fields and
noncanonical values; exact re-encoding must match the metadata bytes.

Use AES-256-GCM with fresh `crypto/rand` 12-byte nonce and 16-byte tag. Generate
a fresh random 32-byte object ID; HKDF-SHA256 derives separate 32-byte AEAD/MAC
keys from the injected root with that object ID as salt and fixed distinct info
domains `sessionless.provider-credential.object.v1/aead` and
`sessionless.provider-credential.object.v1/mac`. AEAD authenticates the fixed
magic/length header and metadata as AAD. HMAC-SHA256 covers all preceding wire
bytes, including header, nonce, metadata and ciphertext; verify with `hmac.Equal`
before metadata authorizes listing/deletion, without decrypting plaintext.
These sizes and key separation use the official Go [GCM](https://pkg.go.dev/crypto/cipher#NewGCM),
[HKDF](https://pkg.go.dev/crypto/hkdf#Key) and [HMAC](https://pkg.go.dev/crypto/hmac#Equal)
contracts. Prove envelope/page/aggregate bounds; clear owned plaintext and
derived-key buffers on every terminal path.

The constructor owns an independent root-key copy. Close rejects new users and
joins active key users under its supplied finite context and their original
operation deadlines before zeroing root/derived keys. Timeout retains closing
ownership and buffers, not successful closure or zeroing under active users;
a later Close may finish that join but never renew an operation deadline.

Write this one immutable ciphertext object and verify exact bounded readback
BEFORE returning a usable candidate and BEFORE the binding CAS. A unique object
identity has one physical PUT, no hidden SDK/HTTP replay, and no overwrite or
retry under the same key. Exact conditional-PUT behavior must be proved with
scripted SDK and IAM transports; unsupported semantics deny, not unconditional
fallback. Actual Object Storage compatibility is separate qualification.

`CommitProviderCredentialCandidate` is logical promotion: verify the exact
object/current binding, without writing, copying or renaming ciphertext.
`RecoverProviderCredentialCandidate` verifies the exact authoritative object;
missing means false, other failures deny. It never repairs absence by another
PUT. No post-CAS method may recreate secret bytes. Consequently a delayed
Commit/Recover cannot resurrect an old secret after cleanup deletion/ACK.
Conditional COPY and an unproved destination-copy precondition are unnecessary.

Successful pre-CAS PUT/readback establishes the candidate; an unknown PUT fails
before CAS and retains any possibly written object for inventory reconciliation.
All ciphertext objects share the discoverable namespace, including ambiguous
pre-CAS writes. No separate staging key, commit marker or cleanup-only index can
hide one. Exact-reference deletion is idempotent and never reads plaintext;
only its success allows the existing YDB cleanup ACK. Failed/unknown deletion
retains the cleanup obligation. No claim of absolute remote erasure follows.

### Inventory, physical cursors and local borrower fencing

Define uncommitted inventory by current YDB authority, not historical promotion:
an authenticated object not referenced by the exact active binding is eligible
for existing candidate reconciliation. The backend uses a narrow existing
`LoadProviderCredential` reader; it creates no second binding authority. A
superseded committed object can therefore become a candidate as well as retain
its existing cleanup obligation; both deletions remain exact and idempotent.

`ListUncommitted(locator, limit)` uses only the exact locator prefix and bounded
MaxKeys. Validate a found binding and its exact requested locator before using
it for exclusion or generation comparisons; lookup failure is never absence.
Successful not-found grants no active-object exemption, not deletion authority.
Exclude at most the one exact active object and inspect at most `limit+1`
objects; a second bounded physical page may be necessary to advance past that
excluded object. Never report empty
completion while an unseen continuation may contain eligible objects. Existing
Service reconciliation processes/deletes or verifies a batch and relists, up
to its existing four batches; exhaustion fails bounded and a later retry resumes.

Global abandoned listing uses one bounded physical page, authenticated CreatedAt
and the existing typed candidate cursor. Its scope/time/reference reconstruct
the exact key for StartAfter; order is physical, not a required Go tuple sort.
Return the cursor of the last authenticated scanned object even when filtering
produces empty Items with HasMore. Current bindings may be filtered with bounded
reads, but never inferred from LastModified or a marker. Malformed envelopes,
out-of-prefix keys or nonprogressing pages fail bounded instead of fabricating
a cursor. A completed sweep starts again from the beginning on a later pass:
a failed pre-CAS PUT can appear behind an earlier cursor. No single sweep proves
that all remote failed writes are globally quiescent. Retain the same original
remaining context and charge all actual listing/metadata work; add no renewed
deadline, unbounded prefix scan or hidden larger authority allowance.

Noncurrent inventory must not bypass active local credential borrowers. In
BOTH existing `Service.reconcileCandidates` and `DrainAbandonedCandidates`,
after a nonauthoritative transactional candidate fence, reload the current
binding. Validate any found row and exact locator before using its generation;
successful absence still relies on that fence, while a failed/invalid lookup
retains the object. If a validated found binding's generation is greater than
the candidate's, join the existing revoker with
`beforeGeneration=current.CredentialGeneration` BEFORE deleting.
Failed lookup or join retains the object and reports failure. Never use
`candidate.Generation+1`: a future orphan must not invalidate the valid current
generation. Equal-generation different-candidate and future-orphan cases rely
on the exact existing transactional fence, without broadening revocation.
An authoritative candidate instead follows exact read-only recovery. Keep the
binding fence's protection against a later CAS; classification alone is not
deletion authority. This is the existing local borrower join, not fabricated
cross-process/global quiescence. Existing autonomous cleanup's cross-process
join limitation remains explicit, not an exhaustive new MVP rollout gate.

### Causal proof and bounded implementation handoff

Before source acceptance, use actual encryption and the real Service plus
InvocationService against scripted S3/IAM and binding ports, not a body-key map.
Prove a blocked materialization consumer → rotation → noncurrent inventory GC:
both reconciliation entry points must wait for its existing revoker join before
deletion; RED must expose premature deletion on the old implementation. Cover
lookup/join failure retention, authoritative recovery, equal-generation rivals
and a future orphan without invalidating the current consumer.

Also prove encrypted Put/readback → CAS → read-only Commit/Recover → one-shot
materialization; delayed Commit after cleanup ACK must issue zero ciphertext
writes. Cover unknown pre-CAS PUT, CAS ambiguity, readback/delete failures,
late inventory visibility and sweep wrap, exact scoped prefixes, physical cursor
progress/empty filtered pages, bounds, tampering/redaction and concurrent
rotation/revoke/cancel. Metadata listing/deletion must not decrypt plaintext.
Distinct synthetic MODEL/SEARCH keys prove no cross-wire use, including no-auth
MODEL; wrong audience/owner/revision/generation/claim/receipt denies before
secret delivery/send. Preserve old goldens, historical STOP, existing MCP,
unsupported terminal/deletion denial and the documented deterministic/race
testing practices. Scripted transport proof is not actual cloud qualification.

Develop #194/#195 MODEL → pending → decision → same-claim wait in parallel with
concrete controlled credential/HTTP peers; production advertisement/activation
still waits for the complete joined bridge and qualified backend.

Keep the execution checklist in existing issue comments: #194 owns the closed
resource/Source declaration, shared custody/backend and actual original producer;
#195 joins consent and the same-claim worker wait; #190 joins the distinct
credentialed search HTTPS/reserve/before-send owner; #191 consumes the durable
typed result and continuation/history/accounting; #197 supplies cited-answer
projection. Do not rewrite their original assignments or create another broad
hardening epic. The backend alone is not search readiness, a demo, activation or
completion of any full outcome. Complete the existing joined controlled proof
before advertising search; real keys, paid calls and cloud rollout still need
their own authority. No new table, migration or local-host dependency follows.

## Query consent within the existing claim

### Historical stop without another grant archive

Current read, private-query disclosure, approval, reserve and before-send must
materialize the original authenticated grant and rejoin the actual current
profile, spending, actor and invocation facts. A closed/expired Source or failed
body read denies those actions; no fixture or retained-request fallback grants
current permission.

Monotonic reconciliation is a different operation. Its existing immutable
request already commits the grant semantic digest, consent/revocation revisions
and absolute request expiry. Successful creation must have bounded that expiry
by the original grant and all invocation/cost windows. The authenticated original
parent/admission and stored consent projection retain the scope and identities.
Do not add another full grant copy, expiry field, approval registry or table
solely to stop a request after the grant keeper has retired.

Select a stop-only path before any grant-body I/O. Authenticate the original
parent/admission Source and retained request/decision, then read actual canonical
Run/Attempt/lease, membership/participation, profile/spending and allocation
facts within the existing call budget. Original-source authentication still
must succeed; this path does not bypass a failed verifier. An already stopped
exact-revision retry returns the retained evidence without rewriting it. For an
active request, record only a cancellation, claim loss, observed canonical
revocation or expiry supported by those facts. DB time at or beyond the sealed
request expiry is sufficient for expiry; do not invent an unavailable original
grant expiry or extend any retained deadline.

A keeper read error or early Source Close alone is not proof of a canonical
revocation, cancellation or expiry. With healthy canonical heads and an unexpired
request, such an observation cannot manufacture a stop, approval or send. This
is an explicitly selected negative-only transaction, never a fallback after a
current-authority loader has set its sticky denial. Request and decision bytes,
digests and original receipt remain unchanged across a permitted stop/CAS retry.
Controlled stored-parent/MODEL fixtures prove this transaction, not an authentic
production MODEL producer or full worker continuation.

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
allocation and cost window; it is not a pause or deadline extension. Each poll
is one existing logical call, bounded by ten seconds, three SQL callbacks,
32 statements per callback and 96 total. Every poll is also clamped to the
original entry, pending-request and caller-context deadlines. It never nests
another public call or renews admission.

One private counter on the original entry permits at most 24 polls shared by
both search requests in the Attempt. The first poll may begin immediately;
subsequent poll starts are at least five seconds apart, including across
requests. All polls share a four-MiB aggregate metadata-materialization limit,
including actual copies. Before each poll, seed its existing private authority
budget with bytes already spent; charge prospective materialization before
copying and retain charges on failure. Neither a new poll nor a second request
resets this aggregate allowance. SQL callbacks never perform body I/O. The
original holder retains immutable argument and grant evidence; each poll
authenticates the existing current parent, canonical actor, Source, claim,
fence and consent in its transaction, with current Web-session authority
required for a recorded decision. It does not reload argument or grant bodies.

Poll again only after a successful authenticated pending result. An authority,
Source or storage error fails closed and is not retried as healthy pending.
Exhausting the poll or byte limit denies search before send and preserves
unreserved custody through the existing reconcile-only path; exhaustion alone
does not invent a revoked, expired, cancelled or worker-lost terminal fact.
An observed deadline, denial, cancellation or revocation uses its existing
explicit stop with zero search sends. A late approval cannot reopen it. Worker
loss is reconcile-only: even a durable approval does not authorize a new worker,
renewed admission, resume, fresh Run replay or replacement claim to send this
query. WebUI refreshes remain separately bounded authenticated requests and do
not reset the worker's counter, metadata allowance or original deadlines.

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

The consent argument `BlobRef` remains in its original Attempt namespace. The
worker's original scoped operation-body owner produces it and performs serving
reads alongside ordinary request/result bytes. It remains the sole owner of
physical inventory, uncertain writes, cleanup and erasure. A separate BFF DATA
reader does not reconstruct or replace that owner. A map fixture or a matching
key/hash alone is not original producer provenance. Use the closed physical-key arm:

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

Both worker arms share the original Store state, synchronous I/O gate, caller context,
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

#### Private consent reads in the BFF

The BFF may read the exact original consent object through a separate read-only
adapter implementing `AttemptOperationBodyStoreV1`. It accepts only the scope,
reference and cap selected by the authenticated Store intent read, uses the
existing `ParseAttemptOperationConsentArgumentsRefV1`, and rejects ordinary
operation references, grant locators, foreign scopes and arbitrary paths. Check
the positive canonical size, lowercase digest and 4-KiB ceiling before Open.
One synchronous bounded Open/read/Close verifies the actual length and SHA256,
returns fresh caller-owned bytes and clears failed buffers. The adapter retains
only an Open-capable dependency; it exposes no write, delete, list, cache,
inventory, receipt, cleanup or dispatch operation. It cannot prove who wrote an
object or create a serving holder from its bytes.

The existing Store owns disclosure: authenticated intent selection precedes
body I/O, exact argument validation follows it, and a fresh current-authority
transaction rechecks the original actor, Web session, membership, participation,
grant, claim/fence and expiry before releasing query data. The same reopened
Source supplies the BFF's existing verifiers and V2 grant custody. Grant bytes
never come from this consent-body adapter. Construct the BFF's single Store with
those dependencies; do not add a mutable reader setter or a fake worker entry.
Mount consent routes only in the explicitly selected local composition. This
does not make SEARCH ready for the model catalog or enable a live profile.

The read uses the existing ten-second logical-call deadline and prospective
body/copy charges within three SQL callbacks, 32 statements per callback and
4 MiB metadata. The private response remains at most 8 KiB. Missing, deleted,
corrupt or unjoinable data denies without repair, re-PUT, fallback or a
manufactured stop. Revocation, cancellation, permission loss or Source closure
during I/O must deny disclosure at the original checks. Concurrent worker
cleanup may make the read fail; the BFF neither delays cleanup indefinitely nor
infers an erasure receipt or recreates an object. Returned readers must honor
context and synchronous Close; no detached goroutine manufactures cancellation.

Prove the seam with an original `WriteConsentArguments` producer and a separate
BFF Store/reader sharing only authenticated durable facts and BlobStore. Cover
the closed key/cap/integrity rules, no writes/deletes, cleanup/read races and
revocation during body I/O. A synthetic pending row is reader-seam evidence,
not the genuine MODEL producer; the joined bridge still requires original
pending creation, authenticated decision and the same-claim continuation.

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

## 0.2.9 candidate: original-loop retained search formats

Draft for independent review and primary acceptance; not an accepted writer,
runtime registration or activation. This amendment is bounded to #194/#195's
original controlled loop. #197 still owns the complete typed citation manifest,
WebUI citation UX and useful-service evaluation. All 0.2.8 authority, custody,
deadline and accounting requirements above remain prerequisites.

The existing [V3 accounting](../../internal/ports/owned_operation_accounting_v3.go),
[V3 residual](../../internal/ports/owned_operation_residual_v3.go),
[V2 capsule](../../internal/ydbstore/local_terminal_capsule_v2_read.go) and
[V2 context witness](../../internal/compactioncontext/conservative_v2.go)
deliberately support MCP, not consent-bearing search. Do not widen their closed
grammars or reinterpret old rows to make a new search turn appear supported.
The [V2 residual](../../internal/ports/owned_operation_residual_v2.go) already
supports two consent states and both physical body-key arms; its measured
accounting and last-reservation equality do not support this paid-search tail.
Reuse that physical inventory shape, not another store or inventory protocol.
Use the following explicit successor DATA formats under the same original
Source/Store, parent, operation ledger, body owner and terminal finalizer:

- `OwnedOperationAccountingDispositionV4`: existing disposition fields with
  `version:4`, plus `consent_states_digest` and `consent_reserved_bytes`.
  Each operation row retains its sequence/kind/operation/reservation/observation
  digests, reserved debit, usage and disposition. The closed MODEL/COMPACTION
  arm keeps the existing original invoice rule. MCP and SEARCH rows carry
  `tool_spending_digest`, with their actual component identity, never a renamed
  MCP witness. Tool disposition `unknown_reserved` means the authenticated
  full MCP component or the exact SEARCH call reservation, never measured USD.
  SEARCH requires `search_wire`, the original accepted V2 grant,
  exact receipt and a 10,000-microunit reservation. Known typed result and
  unknown vendor USD are distinct; neither reported credits nor a lower report
  refunds that debit. The SEARCH component remains at most 20,000 per Attempt.
- `OwnedOperationResidualInventoryV4`: the existing inventory field order and
  shape with `version:4` and the V4 accounting disposition. `consents` contains
  the actual one or two nonpending states; every original argument reference
  must match its exact inventoried consent-key object. Each SEARCH has the
  same approved request/decision digests as its operation. Unsupported or
  pending consent, missing inventory and manufactured erasure still deny.
- `sessionless.terminal-custody.v3`, `codec_version:3`: the V2 capsule fields
  with V4 `accounting_disposition`, then `consents` and `model_catalogs`.
  `model_catalogs` is ordered by MODEL sequence and contains exactly one entry
  per MODEL: `sequence` and optional `tools`. Omitted tools and an authentic
  empty array retain their own representation; null is rejected. The original
  writer extracts only the exact supported catalog from each authenticated
  frozen request before erasure and seals that correspondence with the capsule.
  COMPACTION has no tools and gets no MODEL catalog entry. Historical DATA
  cannot replace an old catalog with the current ordinary surface.
- `ContextDataV3`/`HistoricalRunV3` retain complete MODEL/tool pairs and use a
  required `version:3` discriminator in each container and a closed correlated
  tool witness keyed by original scope and sequence. Its
  kind selects the unchanged MCP witness or the SEARCH V4 spending/reservation
  row plus exact typed `WebSearchResultV1`. The genuine writer authenticates
  those facts; the DATA decoder cannot authorize dispatch or current funding.
  Preserve source IDs, operation/query/wire/result correspondence and the
  original catalog when fitting, compacting or restoring derived context.
- `sessionless.terminal-failure-custody.v4`, `version:4`, is a distinct retained,
  `joined_retained` failure grammar, not successful accounting or erasure.
  It keeps the existing failure capsule's parent/source, profile/frontier,
  stop/cleanup/capture time, actual operations, canonical event commitments,
  retained bodies and optional derived-context fields; `accounting` uses
  `OwnedOperationFailureAccountingV4`, followed by `consents` and
  `model_catalogs`. Only the five tail cases below are admitted.

### Retained compaction V3, including mixed parents

The [retained codec dispatch](../../internal/compactioncontext/retained_v2.go)
and [core decoder](../../internal/compactioncontext/retained.go) currently admit
only versions 1/2. Add explicit V3 build, decode, sealed-DATA decode and dependency
reader arms before any V3 writer: summary schema
`sessionless.retained-compaction-summary.v3`, manifest schema
`sessionless.compaction-manifest.v3`, and identical summary/manifest `core`
with `version:3`. The model's seven-field `sessionless.compaction-summary.v1`
payload remains unchanged and its exact bytes are retained, not silently
re-encoded. `DerivedContextPlanV1` and existing summary/manifest object keys
remain unchanged; the plan still commits each object's exact size/raw SHA.

Keep every existing core correspondence: original scope/generation/COMPACTION
sequence, reservation/observation and normalized-response digests, source-view
and canonical-frontier identities, profile/policy/bundle/compatibility/calibration
digests, generation/next-request fits, cut/summary digests and summary token
bound. V3's cut commits the closed search/MCP witness and canonical projection
correspondences, original source IDs, ordering and covered/protected/suffix
partition. Complete MODEL/tool pairs remain indivisible; protected originals
and the initiating canonical task are not replaced by summary claims.

A V3 child may use an explicitly decoded V1, V2 or V3 parent only after the
existing exact parent-plan, compatibility, scope, generation and closure checks:
closure at most two; parent generation one; child generation two in the identical
original operation scope, or generation one in a distinct Run of the same
tenant/owner/Session.
Covered sources strictly grow while preserving every parent's exact original
source/projection/witness identity under its own codec, not a retagged V2 search
row. Missing, crossed, cyclic or incompatible parents deny, without trying a
different decoder. Old V1/V2 readers must reject a V3 parent/container explicitly;
their old byte encodings, valid mixed-V1/V2 parent rules and goldens stay intact.

Use the same final V3 serializers for prospective summary/manifest/capsule and
canonical-output bounds, including JSON quoting of worst-case typed results,
grant/consent/accounting metadata and the retained catalogs. The generation
request remains tool-free; minimum successor encoding and the actual rebuilt
next MODEL fit use the invocation's full frozen ordinary catalog and original
calibration, source and parent commitments. A small tools-disabled probe or
successful DATA decode is not next-MODEL fit. Admit these bounded artifacts
before the affected PUT/POST, then validate the actual summary/next request;
keep the existing 32-KiB retained summary, 128-KiB manifest and all shorter
admitted/aggregate/capsule/output limits, without a new allowance.

### Closed V4 failure tails and accounting

All rows before the selected tail are the actual complete, bounded causal
prefix: MODEL/COMPACTION `completed/none` bodies and matched MCP or SEARCH
pairs; successful SEARCH results are exactly `known_results` or `known_empty`.
Unknown effects cannot appear inside that prefix. Each reserved SEARCH binds
the preceding durable MODEL call, exact arguments/query, original actor/source/
grant/claim and the stored approval receipt. A later valid monotonic stop may
retain that immutable approval; it never grants another send. No missing row,
observation, response, human decision or stop evidence is fabricated.

For the table, `R` is the closed reason set `authority_denied`, `cancelled`,
`deadline`, `transport_unknown`, `worker_lost`; `U` is `R` plus `malformed` and
`overflow`. A reason string is not failure authority: the original owned
observer must authenticate the actual zero-send versus possible-send facts
and persist the corresponding observation under the existing observation
grant. A guard denial alone cannot manufacture a receipt or observation.

| Stop | Exact durable tail and response | Consent/call binding and truthful projection |
| --- | --- | --- |
| `search_consent_stopped` | Last actual operation is MODEL `completed/none`, with its exact durable normalized `web_search` call. No SEARCH reservation or observation exists for this call. | Its exact request is `denied`, `expired`, `cancelled`, `revoked` or `reconcile_only`, with the existing valid revision/decision/stop evidence. Denial retains its actual deny decision; other stops may have no decision or retain an earlier approve decision. Project the authentic earlier prefix plus a fixed not-executed failure notice, including an empty prefix; no SEARCH call/result execution pair is invented. |
| `search_rejected_before_send` | Last operation is actual SEARCH `rejected_before_send`, reason in `R`, all response-ref/digest/byte fields absent/zero and normalized body exactly JSON `null`. | Exact preceding MODEL call and original approval receipt match the reserved operation; retain any later stop. Project its redacted call and an authenticated not-sent status bound to actual operation/reservation/observation digests, never a vendor result. No continuation or retry follows. |
| `search_service_error` | Last operation is SEARCH `completed/service_error`; exact durable response bytes decode as `WebSearchResultV1` with `known_service_error`, no sources, and match the reservation/observation. | The same MODEL/request/approve receipt and physical claim bind this result. Project the redacted call and actual bounded typed service-error status plus a fixed failure notice, never raw vendor errors or a final answer. |
| `search_outcome_unknown` | Last operation is SEARCH `accepted_outcome_unknown`, reason in `U`; response fields absent/zero, normalized body exactly JSON `null`. | Preserve the same actual MODEL/consent/approval/operation bindings and possible-send evidence. Project a redacted call and explicitly unknown operation status with authentic digests, not `WebSearchResultV1`, an empty result or rollback. Retain charge/custody; prohibit replay and MODEL continuation. |
| `model_after_search_failure` | Last operation is actual MODEL2: either `rejected_before_send/transport_unknown`, or `accepted_outcome_unknown` with `transport_unknown`, `malformed` or `worker_lost`. Both have absent/zero response fields and normalized body exactly JSON `null`. | At least one prior known durable SEARCH result and its exact approved call/receipt remain in the actual complete prefix; any intervening completed COMPACTION stays tool-free. Preserve MODEL2's own reservation, observation, outcome and reported/unknown usage, not the preceding SEARCH as a substitute tail. Project only the true prefix and an explicit not-sent/unknown MODEL failure notice, never a MODEL2 answer or another call/result. |

`OwnedOperationFailureAccountingV4` has canonical fields `version:4`, `scope`,
`accounting_digest`, `aggregate_debit`, `component_amounts`,
`consent_states_digest`, `consent_reserved_bytes`, `operations`. Each row has
`sequence`, `kind`, `operation_digest`, `reservation_digest`,
`observation_digest`, `outcome`, `reason`, `reserved`, `usage`, and
`tool_spending_digest` only for MCP/SEARCH. These are exact actual reservation/
observation projections, not success-disposition rows or invented observed USD.
The failure digest domain is `sessionless.failure-accounting.v4`, distinct from
`sessionless.operation-accounting-disposition.v4`. SEARCH keeps its authenticated
10,000 reservation and original unknown USD usage, including a no-send rejection;
provider credits are not monetary usage. Preserve actual MODEL usage, including
an overrun/unknown tail, without refund or continuation. Operation count equals
actual rows (one MODEL suffices for the consent-stop case), never a phantom
SEARCH or consent operation. Other missing-observation/unreserved preparation
denials remain explicit existing reconcile-only cases, not new failure authority.

The existing [consent append](../../internal/ydbstore/web_search_consent.go)
already charges the same durable Attempt accounting
`2 * ownedAuthorityHeadBytes` (64 KiB) plus exact `Arguments.Size` in BODY bytes
only, after the MODEL reservation, without an operation/tool/monetary debit.
Derive each charge frontier from the request's unique exact durable MODEL
response/call/argument correspondence in the original ordered states and
authenticate its body through the original inventory. For reservation sequence
`i`, include charges whose MODEL frontier is strictly before `i`, in addition
to setup and actual operation debits through `i`. Final accounting includes
all actual consent charges, even when the last stopped request was appended
after the last MODEL reservation: its body-only tail is not in that reservation's
aggregate. Component amounts and all non-body counters remain unchanged by
consent. Capture/finalization and historical verification must authenticate the
actual existing parent accounting row/digest, including its own `UpdatedAt`,
and consent projection together; do not regenerate that row at the last
reservation time or impose last-reservation equality on the consent-only tail.
`consent_reserved_bytes` is that exact sum and `consent_states_digest` commits
the canonical retained states. Exact lost-ACK recovery and decision/stop updates
do not recharge. Retain all original nonrefundable reservation/count debits.
This reconstructs the existing ledger, not a new debit table or authority.
Prospective terminal/failure fit includes every new field, retained catalog,
consent, source and bounded worst-case result before the first affected PUT or
send. Existing operation, body, metadata and capsule ceilings do not increase.

The new canonical SEARCH call projection contains only call/tool identity and
argument/query digests, never query/argument text. Exact receipt correlation
stays private.
The result projection preserves the actual bounded typed sources and their
operation identity; public metadata omits private receipt and credential facts.
Private capsule/context call correlation uses the original normalized MODEL
bytes, not this redacted public projection. A final text mentioning a source
is not a verified citation manifest or a claim that its page was fetched.

All successor readers select an explicit version/schema before decoding;
unknown, crossed, duplicate/aliased/null/noncanonical fields deny. Old V1/V2/V3
bytes, digests, MCP semantics and historical STOP remain unchanged, including
mixed history. Historical/deletion readers authenticate the original retained
parent/admission/capsule/terminal seal and cannot recreate a serving holder or
reconstruct erased request bytes to grant execution. No new table, migration,
controller, approval registry or signing key follows from these DATA formats.

Required causal proof joins genuine MODEL, original pending/body/canonical
actor, authenticated BFF decision, same-claim wait, exact receipt-bound reserve
and before-send, distinct SEARCH key/owned HTTPS, durable typed result, MODEL2,
then honest terminal/cleanup or supported retained failure. Failure publication
uses the existing original finalizer and exact mixed physical custody; unknown
or tainted writes, failed join, missing ownership or uncertain inventory retain
reconcile-only custody rather than a success erasure receipt. No global
quiescence gate or renewed cleanup/deadline is added. Cover two Attempts
sharing one unchanged installed resource profile, cross-substitution denial,
denial/late approval/cancellation/revocation/unknown effects, lost persistence
ACKs, actual mixed-inventory cleanup and strict history/compaction/deletion.
Keep the old goldens and held fixtures unchanged. If an end-to-end baseline
cannot compile the new vertical, label a temporary candidate mutation restoring
only the old original-producer denial as mutation RED; do not call a missing
API, missing catalog or fixture bypass the producer's causal RED.
