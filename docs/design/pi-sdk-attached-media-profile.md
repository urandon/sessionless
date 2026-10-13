# Pi SDK attached media profile

Version: 0.1.3, 2026-10-13.
Owner: [#202](https://gitcode.com/urandon/sessionless/issues/202), under
[#72](https://gitcode.com/urandon/sessionless/issues/72).
Status: engineering contract accepted under agreed-MVP delegation following
independent requirements-led review. Publication and implementation remain
separate; no eligible runtime profile or activation is created by this document.
Version 0.1.2 adds the accepted attached API authority and spending contract.
Version 0.1.3 explicitly amends its previously undefined `CallerDeadline`
representation to distributed server/parent enforcement, without assuming
synchronized wall clocks. This is not an authenticated database-epoch caller
expiry or proof that the server knows the parent's true deadline. Exact source
deadline provenance and enclosing phase protocol remain implementation gates.
The native media requirements accepted in version 0.1.1 remain unchanged.

Use Pi's existing SDK agent loop and standard Responses provider with typed
original images, a fixed trusted bootstrap and bounded transport. This is an
explicit alternative to the Codex media profile, not a replacement model client
disguised as Pi. Preserve the complete attached text/file/image product promise
and the separate managed loop, compaction, MCP and web-search track.

This profile amends only the native representation and integration choice in
[attached media design 0.1.1](attached-harness-media-and-artifacts.md).
`CODEX-EXEC-MEDIA-2026-10-V1` and the closed text profile remain unchanged.
Proposed separate identifier: `PI-SDK-MEDIA-2026-10-V1`. Selection is explicit;
no attachment, model failure or missing credential selects a fallback.

## Research and alternative decision

Source pins are Pi `6c4f360264397c59801f6da2bdac13e3b1fcbe91` and Codex
`f5420174dafba153913a3e697f89002c338dfd7e`, not claims about the latest releases.
The independent original-scope review and source manifests are retained in
the [#199 native-lane receipt](https://gitcode.com/urandon/sessionless/issues/199#note_193248748).
The [#202 acceptance receipt](https://gitcode.com/urandon/sessionless/issues/202)
identifies the exact independent review and primary engineering acceptance.
Exact executable/package provenance remains a release gate.

| Candidate | Source finding | Disposition |
| --- | --- | --- |
| Stock Codex exec | Decoder allocations, image derivation, request/output caps, retries, WebSocket and tool reachability do not satisfy the complete media envelope. | Keep disabled. A reproducible in-artifact derivative is an alternative when preserving the selected Codex resource is necessary. |
| Stock Pi CLI or RPC with an ordinary extension | CLI image ingestion can transform images; extension errors can be caught while the provider request continues. | Not the selected enforcement path. No extension-returned conformance boolean grants permission. |
| Pi SDK typed user images and existing standard Responses provider | The inspected user-image route preserves image blocks through message conversion and constructs data URLs without Photon. Direct provider onPayload executes before responses.create. | Preferred engineering candidate, conditional on exact dependency, transport, context, memory and platform verification. |
| Pi Codex subscription provider | HTTP hooks do not cover the separate WebSocket send path; equivalent output and retry bounds are not established. | Not interchangeable with the standard Responses lane. Retain as a separately reviewable alternative. |
| New Sessionless client replacing Pi's loop and parser | Would remove the ready-made harness rather than integrate it. | Reject for this attached outcome. |

The typed route is supported by pinned Pi source:
[AgentSession.prompt](https://github.com/earendil-works/pi/blob/6c4f360264397c59801f6da2bdac13e3b1fcbe91/packages/coding-agent/src/core/agent-session.ts),
[message conversion](https://github.com/earendil-works/pi/blob/6c4f360264397c59801f6da2bdac13e3b1fcbe91/packages/coding-agent/src/core/messages.ts),
[SDK wiring](https://github.com/earendil-works/pi/blob/6c4f360264397c59801f6da2bdac13e3b1fcbe91/packages/coding-agent/src/core/sdk.ts),
[provider message conversion](https://github.com/earendil-works/pi/blob/6c4f360264397c59801f6da2bdac13e3b1fcbe91/packages/ai/src/api/transform-messages.ts) and
[Responses conversion](https://github.com/earendil-works/pi/blob/6c4f360264397c59801f6da2bdac13e3b1fcbe91/packages/ai/src/api/openai-responses-shared.ts).
Photon remains reachable through other entrypoints. Disabling CLI ingestion,
read tools, extensions and tool-result history is part of the contract, not an
assumption that Pi never processes images.

## Profile and resource authority

Pin the Pi source and build/package digests, complete dependency lock, exact
Node runtime, trusted bootstrap and transport hashes, model and vision
metadata, request/final schemas, image detail, finite context/output/allocation
limits, credential resource, data policy, price revision and platform policy
in one immutable capability/profile binding. Unknown values deny registration;
no runtime catalog lookup supplies missing authority.

Pi's lock pins OpenAI SDK 6.40.0. Its npm integrity is
`sha512-MWtTjd/gQt4jpbji61NTgFWJLoY/PdRJ6wG9/ZDRMYNMlBKrCrSlkLI+KgHP1vR1qT6LKSAyAqIxno6lcK9JiA==`.
This identifies a dependency, not its verified transport behavior. Resolve and
audit that exact package before implementing the transport seam; current SDK
documentation cannot substitute for its source or executable tests.

The selected standard Responses API resource must already be explicitly
eligible for this owner, model, data class and cost policy. Personal Codex
subscription credentials cannot be borrowed for it. This design authorizes
neither a new commercial resource nor live calls. If no eligible resource
exists, keep this candidate disabled and retain the Codex alternative rather
than silently switching products or endpoints.

## Original inputs and sole lifecycle

Reuse authenticated canonical media validation from #199. Preserve every
admitted historical, snapshot and tail attachment with event/sequence,
ordinal, name, MIME and full immutable BlobRef. Verify complete bytes, size,
hash, supported signatures and dimensions before credential Issue. Repeated
filenames remain different source inputs. UTF-8 and JSON files preserve exact
original contents and numeric tokens; names/content are quoted task data.

Go performs bounded PNG/JPEG validation but no extraction, OCR or resize.
Pass the verified original bytes as typed SDK user image blocks in canonical
order. Keep source labels adjacent to their model-visible image positions and
check that association again in the final provider request. Pin blockImages to
false and require a vision model before conversion; Pi's nonvision image
replacement is a denial case, not a supported downgrade.

Native derivation is `NoTransformOriginal`: no local Photon decoder, resize,
re-encoding, image cache or implicit orientation conversion. Data URL base64
must decode to the exact original bytes and declared PNG/JPEG MIME. Pin
`detail:auto`, the inspected standard-provider representation, for this
separate profile. It does not satisfy Codex DetailBased/High geometry; actual
model context and visual usefulness must be calibrated for this exact detail.

Retain both original image cohorts: the 3000 by 1000 PNG and small PNG/JPEG,
including orientation/color-sensitive tasks, historical images and identical
names. No one-image, path-only, OCR or newest-trigger-only substitute closes
the requirement. Requests exceeding an inherited bound fail honestly without
dropping history or increasing that bound.

The same authenticated attached Session, accepted head, Run/Attempt,
lease/fence and original prepared runner own Issue, Materialize, supervised
process, credential finalization, Release and receipt reconciliation.
Subscription credentials retain their original WriteBack. API credentials
require immutable Finalize after the synchronous Materialize callback returns
and unlocks, followed by unconditional, independently bounded Release. A
private Pi prepared driver consumes that authority; it does not attach again,
mint a new claim, resume a native session, create a second runner or extend
credential lifetime. An SDK wrapper is inside this process boundary, not a
network sidecar. The API contract below grants no independent credential or
spending authority to that wrapper.

The same InvocationRunner selects exactly one typed credential arm. Its
subscription CredentialIssueRequest/Handle contracts remain unchanged; the API
arm uses genuine ProviderCredentialIssueRequestV1 and
ProviderInvocationCredentialV1, matched to the original sealed job/resource,
owner, Run/Attempt, lease/fence and expiry. It calls the existing provider
Issue/Materialize once around that same prepared supervisor, then real
immutable Finalize and unconditional Release. Do not bridge an API account
through a fabricated subscription handle or no-op WriteBack. The parent owns
the complete canonical clone and all lifecycle observations; a prepared child
cannot supply credential finalization or release evidence.

## Trusted bootstrap and effects

Create a fresh in-memory Pi SDK session with only the admitted canonical
projection. Preserve Pi's loop, standard Responses construction and SSE parser.
No CLI/RPC command server, HOME discovery, persisted session restore/fork,
settings file, skill/template/extension discovery, custom tool registration or
dynamic provider/catalog/auth plugin is exposed.

Use isolated settings and credential/catalog resources. ModelRuntime can resolve
auth and change baseUrl/headers; therefore an initially fixed model is not
enough. Disable model-network discovery and refresh-on-create, prohibit OAuth
refresh or ambient environment credentials, and validate final endpoint and
auth headers at the actual send boundary. Keep credential bytes out of SDK
events, stdin, model inputs, structured finals and public evidence.

Pin all three retry layers off: SDK request retries, provider retry wrapper and
AgentSession auto-retry. Disable native compaction, steering/follow-up queues
and fallback transports. A private invocation-wide one-shot send permit is
consumed before the physical request and never reset after error, timeout,
lost response or reentry. A second requested model send always denies.

The actual local tool registry is empty. Request tools and provider-hosted tools
are absent, tool choice is none, and effect-bearing response frames reject
before reaching Pi's tool/follow-up flow. Missing-tool errors do not authorize
another turn. Retain OS read-only/no-shell confinement and restricted provider
egress regardless of the empty registry. Owner-host assertions are not
cryptographic attestation against a malicious owner-controlled machine.

## Request response and allocation bounds

Before Issue, verify a conservative fit certificate for the exact complete
context and output reserve. Include formatting, text/file escaping, image
detail and schema overhead; unknown model/image bounds deny. Byte length or a
lower-bound token estimator is not a fit certificate. OpenAI's
[token-counting documentation](https://developers.openai.com/api/docs/guides/token-counting)
distinguishes image/file and formatting costs from plain-text estimates and
counts non-visible output tokens within output limits. This profile adds no
per-turn remote token-count request; calibration needs separate finite authority.

Reserve predictable base64, data URL, JS string, metadata, body and SDK copy
allocations before provider construction. JSON escaping and JS string storage
must be source-accounted, not estimated from compressed-image size. Direct
onPayload rejection alone is too late for already constructed data URLs.
Prohibit samplingParams and mutable request overrides; validate the complete
closed request shape with fixed model, store false, no previous-response or
conversation identifier, truncation disabled and a real finite max_output_tokens
supplied by the trusted stream wrapper. Pin the strict structured-final schema.

At the sole physical fetch boundary, validate exact final method, endpoint,
headers and serialized body after SDK auth/header construction. Use redirect
error and no automatic resend. Enforce response headers, total body/SSE bytes,
line size and event count before the existing SDK parser accumulates them;
cancel/abort the same stream on overflow or forbidden effects. Token limits
alone do not bound error, metadata or streaming bytes.

| Resource | Inherited ceiling or required finite binding |
| --- | --- |
| Text/JSON originals | Two, 128 KiB each and 256 KiB total; exact contents. |
| PNG/JPEG originals | Two, 1 MiB compressed each and 2 MiB total; 4 million pixels each; Go validation at most 32 MiB aggregate decoded RGBA, with scratch/copies separately accounted. |
| Native image representation | Exact originals; base64 at most 4 times ceil(bytes / 3), plus data URL, source labels and actual JSON framing. No native decoded/resampled buffer allowance is consumed. |
| Canonical/transport input | Minimum of original MaxInputBytes/MaxContextBytes, event/count bounds and actual sealed-input wire cap. Preserve the existing 1 MiB prepared transport envelope. |
| Native request and process | Positive immutable full serialized request, pre-send allocation, response/SSE and process memory limits, derived and verified for the exact dependency/runtime/platform before registration; never defaults or unbounded values. |
| Final semantics | 128 KiB structured JSON; answer at most 32 KiB UTF-8 and existing 32000 runes; two text/JSON artifacts at most 32 KiB each and 64 KiB aggregate. |
| Process protocol | Minimum of 1 MiB/frame, 16 MiB stdout, 4096 events, depth32 and 256 KiB raw-final bounds and narrower profile limits. |
| Time and sends | One physical model send; all preparation/stream/cleanup stays inside the original earlier caller/run/lease deadlines and credential lifetime. |
| Upload/export | Reuse #200's two body sends, 64 KiB aggregate and 15 seconds or earlier remaining authority, including reconciliation. |

Native request/response and process values cannot be selected solely to make a
test pass. The implementation must publish an exact source-derived allocation
ledger and measured maximum legitimate cohort within its actual platform
envelope. If stock SDK serialization cannot meet that ledger, add only a small
reviewed native transport/bounds seam. Do not replace the provider/parser/loop,
raise inherited ceilings or manufacture eligibility from a hook callback.

## Final results and reconciliation

Reuse #200's strict answer/artifact schema and private completed-process
producer carrier; only a genuine same-run supervised final may populate it.
Sessionless serializes the exact provider-authored text/JSON contents, not a
directory harvest or synthesized summary file. Preserve final-byte, answer,
artifact, schema/profile and process-observation digests separately.

Subscription WriteBack, or API immutable Finalize after callback unlock, then
unconditional bounded Release and owned staging cleanup precede bounded upload.
Keep output upload capabilities outside the child. Existing attempt-output
custody, exact immutable object reconciliation, READY/canonical finalization
and TerminalAck journal ownership remain unchanged. Cancel/revoke/expired or
ambiguous effects cannot produce a new model send or artifact regeneration.
Current participant checks and durable downloads/refresh/re-login remain #201.

## Attached API resource and spending authorization

The attached Pi standard Responses lane keeps the genuine compute connection,
Run, quota and scheduler unchanged. Its API model account is a separate sealed
resource, not a renamed subscription account or fabricated connection ID.
Admission requires an explicit, default-off mapping for an independently
reviewed Pi profile and an existing owner-authorized API resource. An API enum,
supported price, consistent evidence bundle or this engineering design cannot
supply that permission. The managed branch and subscription routes stay
unchanged.

### Canonical construction and original binding

Construct the native profile, evidence, spend terms, owner receipt and mapping
in that order. Hashes use strict canonical representations and distinct domain
separators; unknown fields and legacy aliases reject. No earlier value refers
to a later hash, so construction needs no cryptographic fixed point.

| Value | Canonical content and construction |
| --- | --- |
| P | Unchanged native base profile: Pi/build/dependencies/Node/transport/model/vision/context/output/allocation/schema/platform bindings. No monetary grant, terms, mapping or receipt reference. |
| E | Existing `ProviderEvidenceBundleV1` against backend P. Its ordinary Policy references the exact Price digest and supplies `EffectivePolicyDigest`; existing evidence scopes and hashing stay unchanged. |
| T | `AttachedPiAPIInvocationPolicyV1`: Version1, `ProviderEvidenceScopeV1` containing backend P, ModelVendorID/ModelID, InputDataClass, EffectivePolicyDigest(E), PriceObservationDigest(E), Currency, MaxContextTokens, MaxOutputTokens, MaxChargeMicrounits and NotBefore/ExpiresAt. Compute TermsDigest externally. No receipt/mapping or final binding/job digest. |
| R | `AttachedPiAPIOwnerAuthorizationReceiptV1`: Version1, IssuerID, DecisionID, TenantID, OwnerUserID, DecisionSourceID, SubjectTermsDigest(T), Decision=`authorize`, IssuedAt/ExpiresAt and MembershipSecurityVersion. Compute ReceiptDigest externally. Its authenticated subject is exactly TermsDigest, not itself, M or a final profile/binding. |
| M | `AttachedPiAPIInvocationPolicyMappingV1`: only Version1, TermsDigest(T) and OwnerAuthorizationReceiptDigest(R). Compute MappingDigest externally after verifying exact canonical T/R and their provenance. M feeds neither P/E/T nor R's subject. |

Add exactly one original invocation comparison carrier:
`HarnessBindingV2.AttachedPiAPIInvocationPolicyDigest`, JSON
`attached_pi_api_invocation_policy_digest,omitempty`, type
`AttachedPiAPIInvocationPolicyMappingDigestV1`. Its nonzero value is the
lowercase SHA256 MappingDigest(M); zero means absent. The reviewed attached
single_turn Pi standard Responses API-account/file-credential arm requires it.
Subscription, managed, fixture, non-Pi, router, environment/direct-delivery and
unreviewed native tuples forbid a nonzero carrier. There is no absent-API
fallback, alternate key or independent job/authority grant.

Original WorkerJob, DispatchOutbox and ExecutionAuthority retain their normal
full HarnessBinding. Its digest preserves all original fields and adds a
length-delimited domain-separated tag plus M only for the valid nonzero arm;
absent existing routes retain their original digest bytes. Original
`AttachedWorkerJobContextDigestV1` already seals the complete binding digest,
therefore the accepted job/context seals M without a second claim or ledger.
Evidence scope continues to contain backend P, not the later full binding.

The server selects the current exact active M. Replacing it with M2 makes an
old job pinned to M ineligible for new Issue, send or success/data publication;
the server never upgrades that job or rebuilds its context implicitly.
Already committed or retired original effects retain their immutable scope
and historical reconciliation. Their ACK does not authorize new model spend
or data publication.

### Authenticated owner authorization

`AttachedPiAPIOwnerPolicyServiceV1` issues R behind the original WebBFF
authenticated mutation boundary. Reuse `Handler.authorizeMutation`, current
Web session and tenant membership/write authorization, and origin/CSRF checks.
The owner explicitly confirms already server-validated exact T. Principal,
tenant, owner, DecisionID, time and membership security version are derived by
the server, not accepted from client receipt fields. The issuer rechecks API
resource ownership/revision/generation, P/E model/data/price/caps and the finite
charge predicate before recording the decision.

Store R as an immutable authorization/audit decision with an exact trusted
DecisionSourceID. `VerifyAttachedPiAPIOwnerAuthorizationReceiptV1` reads that
authoritative issuer decision by exact locator and checks registered issuer
and source provenance, canonical R/subject hashes, active/unrevoked decision,
current owner/tenant membership/security version, resource scope and the
minimum T/R/E expiry. A hash-only file or operator-supplied receipt JSON without
that source denies. If an issuer signs, its explicitly trusted key authenticates
only the exact domain-separated TermsDigest subject; a self-generated config or
test HMAC supplies no permission. Cookie/bearer and private signing material
are absent from R and queued/canonical inputs.

An API-ingestion receipt, synthetic test actor, config flag, supported price,
agent design acceptance or ID-generation HMAC is not an owner spending
decision. This narrow decision/audit record is not a balance, spend/refund,
quota or effect ledger. Issuance authenticates the owner at decision time;
current use verifies the decision and membership, not ambient browser
credentials. No eligible receipt/mapping or live paid authority is created by
this design.

### Current authority at admission and use

The server-only `currentAttachedPiAPIPrerequisitesTx` reuses the exact API-row
predicate from `currentServerlessProviderResourceTx`, not managed admission or
effect machinery. Read the exact tenant/owner/API-resource row through
`readProviderCredential`: bounded 8 KiB canonical metadata, duplicate/field,
scalar and time parity, active ResourceBinding exactly matching selected
resource/revision/generation, UpdatedAt no later than transaction time, and
current evidence. Subscription authority is never substituted by that row.

In the original attached `admitDispatchTx`, load and validate authoritative
outbox/Run/Attempt, then check API prerequisites, current exact P/E/T/R/M and
canonical session deletion/membership/participant permissions before
`ensureSchedulerSlot`, reservations, lease/job/outbox writes or offer creation.
Scope comes from stored Run/outbox, not queue claims. The same serializable
transaction observes API authority and applies original admission. The real
compute connection still supplies its original entitlement/quota; there is no
additional monetary reservation or permission cache.

The read-only `AttachedPiAPIUseAuthorizerV1.AuthorizeAttachedPiAPIUse` operation
uses the original Session-bound HTTPS source/transport seam and its
bearer-derived scope. Its closed request carries original
`AttachedWorkerSealedInputAuthorization`, expected original HarnessBindingDigest,
the expected canonical AttachedPiAPIInvocationPolicyDigest, the versioned
send-relative CallerDeadline defined below and one fixed phase: `pre_issue`,
`physical_send` or `immutable_finalize`. These are comparison fields, not provider selectors,
grants or credential overrides. The server derives model/resource/profile
from the stored original job. The response contains only fixed status, actual
accepted head/revision, actual binding/policy digests, CheckedAt and UseExpiresAt;
no secret/reference/body, bearer, discovery or renewable token.

Reuse the original authenticated claimed-head transaction and predicates,
but add current original job/Run/Attempt/Lease, binding/session/trigger,
worker/connection/accepted revision, active lease/fence, no cancellation or
terminal Run, canonical session/membership/participant/deletion, API and policy
checks in that same transaction. A cached materialization head is not this
later authorization. Before `pre_issue`, the complete canonical clone, source
association, media and allocation checks and original 1 MiB seal still apply.
Original per-root byte/count limits remain unchanged: 8 KiB describes fixed
provider/principal records, not a smaller whole-job or media fixture.

The additional source operation is opened and retired with the same accepted
Session, explicit origin/TLS and no ambient proxy/auth. No new attach/session
is created and the child receives no bearer. The original parent validates
body/profile digest against its owned pre-Issue canonical projection before
answering fixed, bounded, secret-free supervisor IPC; actual SDK method,
endpoint, auth/header and whole-body equality remain independently enforced.

### One send and monotonic authority

The original runner consumes its one irreversible local permit **before**
the one `physical_send` authorization RPC. A denied, lost, late, malformed or
uncertain reply yields zero send and leaves the permit spent. This phase has
no auth-RPC retry, model resend, second child/runner, crash replay or queued
permission. Send only after a matching successful fresh serializable snapshot
and original local cancellation/deadline check. Server authority is bounded by
every original applicable authentic Job/Run/lease, connection auth/presence,
binding evidence and T/R expiry, including any genuinely stored server-side
caller/run deadline. The request can tighten, never enlarge, those bounds.
The genuine parent separately enforces its true retained local deadlines;
no renewal extends either lifetime.

#### CallerDeadline representation and distributed enforcement

The new canonical `AttachedPiAPICallerDeadlineV1` has exactly `Version uint32`
equal to 1 and `RemainingNanoseconds uint64`. Under `CallerDeadline`, its
canonical JSON is `{"version":1,"remaining_nanoseconds":N}`. Both fields are
required and non-null. N is a canonical unsigned decimal integer in
1..9223372036854775807: no sign, fraction, exponent, leading zero, string or
coercion. Reject missing/wrong/zero version, zero/overflow, duplicate or unknown
fields, trailing data and aliases. There is no absolute epoch/time.Time or
legacy arm; this operation is new, not a compatibility read of an existing wire.
Pin exact UTF-8 producer bytes and strict decoder parity. A uint64 cast or
DisallowUnknownFields alone does not prove this grammar.

This leaf describes an **untrusted send-relative tightening allowance**, not
owner authority or a server-authenticated true local caller deadline. Before
implementing the operation, derive its complete fixed request/reply body caps
and framing from the actual authorization/comparison/version/phase fields and
original per-root limits. The leaf grammar does not establish that enclosing
envelope, impose a smaller whole-job/media cap or manufacture runtime eligibility.

At original invocation/source construction, retain the actual governing local
deadline objects and monotonic provenance. Immediately before a phase RPC,
select their earliest applicable deadline L and monotonic send sample s; send
W = L - s. Preparation elapsed before s reduces W. A missing, unknown,
wall-only, saturated or lost-monotonic required bound denies. Never start a
fresh context/process/grace timeout per phase. Prove the actual producer-field
and one-time anchor mapping before implementation; an existing UTC lease check
does not establish cross-clock provenance.

For pre_issue, retain original caller/run/whole-invocation bounds after the
complete canonical/media/fit/allocation checks. For physical_send, retain the
same applicable invocation/model/process/caller/run/use bounds, without reset
since pre_issue. For immutable_finalize, after callback return/unlock and
process quiescence, retain the applicable original credential/use/invocation
and current-authority bounds. Do not intersect unrelated timers merely to
obtain a passing fixture, or turn cleanup grace into model/spend/publication
permission. Owned Release/process/staging cleanup retains its original
independently bounded grace and is attempted even after model-context
cancellation. Its anchors are not restarted; historical control/ACK/failure
paths retain their separate original predicates.

Let a be the CheckedAt timestamp of the final successful fresh serializable
snapshot **inside this same measured RPC**, and D the earliest applicable
authentic server deadline. Validate exact positive B = D - a. Clamp
U = min(B, W) before Add and validate UseExpiresAt = a + U. A larger malicious
W cannot extend B but can misstate the true parent deadline. The server derives
no permission from W and does not certify that true local L remains future.

Record receive sample r from the same original monotonic clock, full elapsed
R = r - s, and positive declared conservative safety slack S. After authentic
status and exact phase/head/revision/binding/M matching, translate to
F = r + min(U - R - S, L - r), only with positive exact arithmetic and retained
monotonic provenance. Thus F <= s + U - S <= s + W - S < L and the server window
is conservatively bounded by s + B - S. Clamp before Add, not after overflowing
or saturating an endpoint. Recheck original cancellation and F immediately at
the actual physical-send/Finalize use boundary; delayed use does not reanchor F.

The final successful snapshot must physically occur within s..r, including all
internal transaction-retry, processing and transport time in R. Return only
that final snapshot's timestamps/current tuple. Cached materialization,
asynchronous or another-RPC timestamps invalidate the argument. No extra clock
RPC, token-count call, application physical_send auth retry or renewable token
is introduced. Never compare independent parent UTC with the database epoch,
reset R/L/F after a reply or remint the consumed permit. Joint genuine-parent
and server enforcement is not attestation against a malicious owner-controlled
host.

Required counterexample: with L = s + 1 second and the server snapshot after
1.5 seconds, B may still be 60 seconds and the server may return a + 1 second.
That reply does not prove the true caller is live. The genuine parent's full
R >= 1.5 seconds yields zero permission and zero send. Claiming independent
server rejection requires another genuinely stored authentic server bound;
do not invent synchronized clocks or infer it from the request.

Deterministic controls retain independent epochs with identical monotonic
elapsed time, work before RPC, caller exhaustion before snapshot, genuine
shorter server bound/max-window clamping, malformed/overflow/lost-monotonic
values, full R/slack/equality exhaustion, delayed use, wrong phase/head/M,
cancelled context, retry delay inside R and a causal cached-snapshot denial.
Restore legitimate controls and preserve complete original two-image/two-owner,
G+1/Fence, failure/cleanup/historical ACK and native-runtime obligations.

Authorization linearizes at the successful serializable current-authority
snapshot's database order, not atomically with network bytes. Revoke/rotate
committed before that snapshot denies a new permit. Revoke after an already
authorized bounded permit is in flight follows original cancellation/ambiguity
rules; it promises neither remote rollback nor immediate cancellation.
Overlapping transactions follow their actual serial order. None of these cases
permits renewal or replay.

### Immutable finalization and publication

Rotation/revoke publishes durable new state before local Fence waits.
Genuine Materialize retains its per-handle state mutex throughout the
synchronous consumer. The fresh remote read-only guard observes durable DB
authority and never reenters/relocks local Materialize, Finalize, Release or
Fence. Real immutable API Finalize runs only after the consumer returns and
unlocks. Verify retained root/path/FD/inode/type/mode/size, complete unchanged
key fingerprint/generation, unexpected entries and fresh `immutable_finalize`
authority. Reject replacement, symlink, truncation or modified API key; never
persist a refreshed key. Cleanup alone cannot report successful Finalize.

Always attempt independently bounded Release and original process/staging
cleanup, including failed authority/finalization. Preserve subscription
WriteBack unchanged and the original earlier caller/process/grace boundaries.
The child owns no lifecycle port or cleanup evidence, and API file delivery
does not inherit credentialWriteFile or environment-key grants.

Recheck the same API/principal/current P/E/T/R/M prerequisite inside original
server READY publication and semantic-success terminal transactions before
canonical writes. Use stored job/binding/receipt digests and original
phase-specific head predicates, not a cached local finalization flag or new
publication ledger. After revoke/head change, new success/artifact/data
publication denies, but unconditional cleanup and original scoped
cancellation/failure/control observations and reconciliation remain allowed
under their existing predicates. Already committed/retired original effects
remain reconcilable under #142; historical ACK is not new API spend or new
data publication and never resends the model. Do not strand Run indefinitely
or synthesize READY after an ambiguous result.

### Conservative API charge

`ValidateAttachedPiAPIInvocationPolicyV1` checks exact T/M/R, original binding,
complete E, immutable profile limits and current time. Require exact scope,
owner/resource/model/data and digests, known matching currency, current trusted
owner decision, minimum expiry, and policy Go or every explicit condition
resolved by the trusted mapping. Existing Policy references Price transitively;
do not duplicate its price evidence or use managed AdmissionCostCeilingV2.

I is the independently verified maximum billed input for the complete
system/history/text/file/vision context; O is the maximum billed output including
reasoning and every billed output category. They must match immutable profile
context/output bounds and all inherited MIN limits. No live token-count call,
arbitrary byte-to-token estimate or omitted historical/image input can supply
these bounds. Pin a conservative tariff proof covering vision, cache,
reasoning and any additional charge at the sealed rates. Unknown bounds or an
unrepresented charge make the lane ineligible, not implicitly free.

Compute the maximum charge in the exact observed currency:

```text
C = ceil(I * InputMicrounitsPerMTokens / 1000000)
  + ceil(O * OutputMicrounitsPerMTokens / 1000000)
```

Round each component upward independently. Check uint64 multiplication and
addition; calculate ceil through quotient plus nonzero remainder, not an
unchecked numerator plus 999999. Deny overflow, inconsistent total context,
expired price/policy/evidence, currency mismatch or C exceeding finite
MaxChargeMicrounits. Zero cap/rate is valid only with explicit genuine
owner/tariff evidence, never a default-free assumption. Recheck the same
original job/binding/profile/policy/price/M tuple at admission, sole send and
final publication. No currency conversion, second provider operation, new
scheduler/quota reservation or shared managed accounting ledger is introduced.

### Source contracts and causal proof

Source references below describe existing primitives at Sessionless
`9be4d2b5aa93d8989f28ef46c2ef2d5a62e8e8f2`; the new API types/operations above
are accepted engineering contracts, not implemented fields or enabled routes.

- [Execution authority](../../internal/ports/canonical_ingress.go),
  `ExecutionAuthorityV2.ValidateForScope`, lines 177–216: attached subscription
  gate; the managed gate at 271–277 remains separate. [Attached binder](../../internal/sessionlessharness/attached_binder.go),
  `AttachedResourcePin.authority`, 129–151, and [registry](../../internal/sessionlessharness/registry.go),
  `resolve`, 168–210, enforce original immutable scope/backend choices.
- [Evidence scope and policy](../../internal/domain/provider_evidence.go),
  `ProviderEvidenceScopeV1`, 47–51, Price/Policy shapes, 185–218, and canonical
  digest, 482–492; [evidence bundle](../../internal/domain/provider_evidence_bundle.go),
  `ValidateForBinding`, 42–111, binds backend P and transitive price/policy.
- [Binding digest](../../internal/domain/provider.go), `HarnessBindingV2.Digest`,
  402–452; [job context digest](../../internal/domain/attached_worker_execution.go),
  `AttachedWorkerJobContextDigestV1`, 233–237, already seals that binding.
- [Dispatch admission](../../internal/ydbstore/scheduler.go), `admitDispatchTx`,
  150–267, retains genuine compute entitlement/quota. [Current prerequisites](../../internal/ydbstore/serverless_current_authority.go),
  19–111, and [provider row reader](../../internal/ydbstore/provider_credential.go),
  `providerCredentialReadSQL`/`readProviderCredential`, 351–412, supply bounded
  canonical/current-resource read semantics, not managed effect authority.
- [Claimed-head authorizer](../../internal/ydbstore/attached_worker_execution.go),
  `AuthorizeAttachedWorkerSealedInput`, 58–108; [head predicate](../../internal/ydbstore/attached_worker_execution_helpers.go),
  98–120; [bearer transport](../../internal/attachedworkertransport/service.go),
  `AuthorizeSealedInputBearer`, 689–719; [Run/Attempt/Lease reads](../../internal/ydbstore/worker.go),
  `LoadWorkerCredentialInvocation`, 193–237; [Session source factory](../../internal/attachedworkersealedinput/session_source.go),
  `SessionSourceFactory.Open`, binds source lifetime to the original Session.
- [Provider mutation](../../internal/providercredential/service.go), 153–164
  and 188–195, publishes state before Fence; [materialization](../../internal/providercredential/invocation.go),
  124–174 and 206–226, consumes once and holds the local mutex. [Pinned API file](../../internal/credentiallifecycle/pinned_secret_file.go),
  `Create`/`Cleanup`, 33–49, needs the real immutable Finalize addition.
- [Original runner](../../internal/attachedworkerdaemon/invocation.go), 39–80
  and 186–265, retains the sole lifecycle; [credential validation](../../internal/ports/validation.go),
  65–140, keeps subscription request/handle semantics; [provider lifecycle](../../internal/ports/provider_credential.go),
  170–200 and 237–249, supplies genuine API requests and synchronous one-use
  Materialize. [Verified credential scope](../../internal/attachedworkerdaemon/verified_attached_input.go),
  350–365, binds original session/trigger/resource/generation/lease authority.
- [Owner mutation authorization](../../internal/webbff/handler.go),
  `authorizeMutation`, 1080–1107, and [Web authorization](../../internal/ports/web.go),
  49–87, provide authenticated session/membership/origin/CSRF provenance;
  [READY publication](../../internal/ydbstore/attached_worker_output_receipt_store.go),
  `finishAttachedWorkerOutputReceipt`, 348–421, uses original current head/job.

The finite implementation proof must include sequential P/E/T/R/M construction
without selfhashes; strict cyclic/unknown/alias negatives; unchanged absent-route
digests; binding/context change when M changes while P/E stay fixed; and real
versus fabricated, foreign, stale or revoked owner receipts. Resource changes
after binder preflight but before admission must yield zero reservation/offer.
Stale/foreign/expired head before Issue must yield zero Issue.

Schedule materialization at G, durable revoke at G+1 while local Fence waits,
then the final send authorization: zero model sends and no deadlock. Distinguish
an already-authorized in-flight effect from a new permission; never retry it.
Cover changed accepted revision/run/lease/cancel/principal between Issue, send,
Finalize and publication, immutable key mutation with unconditional Release,
monotonic expiry/skew/round-trip controls, charge rounding/overflow boundaries,
unknown tariffs and genuine restored controls. Mapping replacement must deny
new use without upgrading old jobs or preventing committed historical ACK.
Retain complete original canonical/media, cancellation/failure/control,
subscription and managed cohorts, not smaller guard-only fixtures.

## Verification and implementation handoff

Design acceptance is not runtime conformance. The implementation must prove:

1. Exact SDK/runtime source and package hashes; startup reachability; auth,
   serialization, retry, redirect, parser and actual send-boundary behavior.
2. Genuine prepared Session/head/lifecycle with zero preflight Issue on corrupt,
   missing, stale, foreign, unsupported or unknown-fit inputs. Same canonical
   historical/snapshot/tail order and exact original bytes reach the request.
3. Maximum legitimate text/file/two-image context, escaping/copy/memory/wire
   accounting and overflow negatives before allocation/send, using the declared
   platform limits. Both original image cohorts remain in the acceptance matrix.
4. Causal denial mutants for all retry/effect/endpoint/auth/override/second-send
   paths, response overflow, cancellation and ambiguous transport; no model or
   tool replay after accepted effects or receipt loss.
5. Structured output/producer/upload/READY/ACK/download proof through the normal
   two-owner path, followed separately by real content-dependent visual tasks,
   actual-platform confinement and bounded browser/profile activation evidence.

Use repository testing guidance, fake transports and deterministic barriers for
implementation, with race/shuffle/stress, independent exact-source review and
all exact-head CI. Scripted evidence does not prove a useful real model, private
data eligibility or cloud containment. Return bounded NO-GO if the exact
dependency/model/platform cannot enforce the profile.

Reuse #72, not another epic. The registered implementation handoff is:

- [ ] [#203 — bounded default-off Pi SDK prepared media adapter](https://gitcode.com/urandon/sessionless/issues/203): bootstrap, exact dependency/transport envelope and same-lifecycle composition after the integrated #199 canonical foundation.
  - [ ] Publish and verify the acyclic P/E/T/R/M canonical types, strict original-binding carrier and unchanged existing-route digests; resolve exact contract implementation through independent original-goal review.
  - [ ] Implement current attached API prerequisites in original admission before reservations/offer, and the original Session-bound phase authorizer with monotonic remaining-duration and one-send proof.
  - [ ] Implement genuine owner-decision provenance/current mapping and conservative tariff/charge checker; absent real owner mapping remains ineligible.
  - [ ] Add real immutable API Finalize after Materialize callback unlock and unconditional Release; preserve subscription WriteBack, failure/control cleanup and historical reconciliation.
  - [ ] Obtain generated catalog/build assets matching the exact Pi source pin and prove genuine Agent/session/ModelRuntime standard Responses/SDK/parser composition, empty local registry and fixed original request; a release label alone is not matching provenance.
  - [ ] Prove exact dependency/Node/platform startup, full context/vision/tariff bounds, allocation ledger and maximum legitimate original cohort with causal negatives; design acceptance is not this numerical conformance.
  - [ ] Join genuine output/custody/publication/download proof through [#200](https://gitcode.com/urandon/sessionless/issues/200), [#201](https://gitcode.com/urandon/sessionless/issues/201) and [#142](https://gitcode.com/urandon/sessionless/issues/142), then independently review the exact candidate and required exact-head CI.
- [ ] [#199 — canonical input and complete native preparation acceptance](https://gitcode.com/urandon/sessionless/issues/199): this design does not deliver its remaining native proof.
- [ ] [#200 — authentic outputs and upload custody](https://gitcode.com/urandon/sessionless/issues/200).
- [ ] [#201 — joined receipts and durable authorized downloads](https://gitcode.com/urandon/sessionless/issues/201).

All implementation rows remain open. Reuse the existing activation owner with
an explicit selected Pi resource mapping to #133/#129/#35 where suitable; do
not automatically create a redundant Pi activation task. Codex #133 remains
open until delivered or explicitly dispositioned within the existing
one-harness product scope. No profile or adapter is silently enabled.

The independent managed #176/#189–197, #175/#90/#92 and two-route #35/#13/#14
checklists do not change. No provider calls, credentials, deployment, shared
migration, destructive cleanup, force-mirror override or network/Shadowrocket
mutation is authorized by this design.
