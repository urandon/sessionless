# Bounded web search for the owned harness

Version: 0.2.1 draft, 2026-10-10. Owners: [#94](https://gitcode.com/urandon/sessionless/issues/94),
[#179](https://gitcode.com/urandon/sessionless/issues/179),
[#180](https://gitcode.com/urandon/sessionless/issues/180); implementation #176.
Independent exact-version review and owner acceptance precede implementation.
This proposes one search profile; it grants no credential, purchase or live-call
authority. [The comparison](../research/web-search-mvp.md) owns the sources and
remaining broader research.

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
