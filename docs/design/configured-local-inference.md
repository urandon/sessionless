# Configured local inference for development demos

Version: 0.1.5, engineering design. Owner:
[#212](https://gitcode.com/urandon/sessionless/issues/212), under
[#176](https://gitcode.com/urandon/sessionless/issues/176).

The [queued runtime addendum](#queued-runtime-composition) defines implementation
work under #218. Design acceptance does not establish that the implementation
exists, grant operator, credential or live-run authority, or mark an
implementation checkbox complete.

Connect the Sessionless-owned agent to an inference server the operator has
already started. Use one bounded Go HTTP adapter with an explicit endpoint,
model, protocol and authentication policy. Do not require OpenRouter, Pi,
native Ollama management, model downloads or federation to make a local demo.
The adapter supplies model operations; Sessionless still owns the agent loop,
tools, compaction, canonical Session, effects and result publication.

This addendum extends the [managed agent minimum](owned-agent-harness-minimum.md)
for a development-only deployment. It does not change the two required MVP
routes or establish the managed-cloud promise of needing no user host.

## Decision and existing contracts

Use nonstreaming **Chat Completions** as the first wire, with one response choice
and at most one structured tool call. A complete tool response can feed the
existing #191 loop; this is not a text-only substitute for that loop.
Streaming and Responses require their own explicit supported codec/profile;
neither is an alias or an automatic fallback. A requested unsupported mode
fails before send. One qualified server/model is sufficient; shipping three
server integrations is not a prerequisite.

Reuse [local inference research](../research/ollama-local-provider.md) for
protocol differences and disk/test policy. Its stronger native-daemon,
immutable-model and federation contracts remain in #114–#119. An owner-run
endpoint cannot attest those properties and need not acquire them for #212.

Source baseline for the following gap map is main `9bab090`:

| Existing contract | Required bounded extension |
| --- | --- |
| [Provider binding](../../internal/domain/provider.go) | Add an explicit backend/resource contract; credentialless real inference must not be labelled a deterministic fixture. |
| [Route and evidence](../../internal/domain/provider_evidence.go), [bundle validation](../../internal/domain/provider_evidence_bundle.go) | Preserve exact owner/resource/backend/model, independent evidence digests and minimum expiry; distinguish configured facts from observed model behavior. |
| [Current resource reader](../../internal/ydbstore/serverless_current_authority.go) | Recheck an installed local resource without requiring a fictitious credential row; retain current Session/membership/participant and Attempt/fence checks. |
| [Owned profile and funding](owned-harness-current-authority.md) | Admit a real operator-owned local installation, never convert fixture publication/funding into real-provider permission. |
| [Direct OpenRouter](../../internal/directopenrouter/contract.go), [egress](../serverless-egress.md) | Keep their fixed endpoint, Bearer, metadata and public-only transport unchanged. A separate configured adapter does not inherit their attestations. |
| [Substrate binding](../../internal/domain/serverless_harness.go) | Represent the local executable honestly, without a fake container image or cloud attestation. |
| #190 / #191 / #175 | Join provider-session lifetime, durable operations and normal local Web/control/worker composition; an isolated HTTP smoke does not deliver the agent demo. |

## Profile and resource identity

The canonical new names are `backend_kind=direct_openai_compatible`,
`provider_contract_kind=configured_endpoint`, `resource.kind=local_endpoint`,
`transport_kind=direct_api`, and `billing_kind=owner_local`.
The first native protocol is `openai-chat-completions.v1`. These are closed
new variants, not legacy aliases for OpenRouter, a subscription or a fixture.

`ConfiguredInferenceProfileV1` is trusted installation configuration with:

- tenant, owner, resource ID and positive resource/profile revisions;
- exact canonical base URL, configured model/vendor identifiers and wire mode;
- `auth=none` or `auth=bearer`, with no credential bytes in the profile;
- declared server/version/template identity and capability evidence references;
- explicit text/image/file/tool/schema support, context estimator and limits;
- execution-host/substrate identity, allowed data classes, original expiry and
  the local operator authorization/terms reference.

Canonical encoding and the backend profile digest cover all these fields.
Model text, browser requests and generic queue fields cannot supply or change
the configuration. No default model, environment discovery, `/models` request,
provider plugin installation or live capability probe occurs during invocation.
Configuration is read at installation, validated, copied and sealed. A change
requires a new revision and fresh admission, not a reread halfway through work.

For `auth=none`, use `credential_mode=none`, generation zero and delivery
`none`; there is no handle, secret read, Authorization header or release call.
Only the new exact resource/backend/contract tuple may use this combination.
All old fixture-only restrictions remain for other tuples.

For `auth=bearer`, use invocation credentials, positive generation and direct
delivery through the existing owner-scoped credential lifecycle. Extend its
closed local-resource validation where needed; do not invent an API billing
account. The trusted transport alone sees at most 4096 credential bytes;
the loop, tools, DTOs and logs never do. No ambient environment/token file,
OAuth refresh, file/environment conversion or authentication fallback is allowed.
Actual key ingestion/use remains separately authorized.

The existing owned-profile current head can hold the local resource kind in
its `Utf8` key and canonical record. In no-auth mode that installed head owns
resource revision and revocation; do not create a zero-generation credential
row. In Bearer mode the same transaction additionally matches the current
credential binding and generation. The current-resource reader receives the
already verified exact profile reference, never a caller's `approved` flag.
Changing endpoint, auth mode, model, policy or resource revision must retire
the prior profile reference before another effect. A new digest/key alone is
not revocation; owner transfer instead requires a newly authorized resource.

For this first profile, the trusted installer permits one active local profile
per resource. New installation mints a fresh opaque resource ID; generic profile
publication cannot create another local head for a caller-chosen existing ID.
Replacement requires the original installation receipt, exact predecessor
locator/digest/generation and authenticated operator source. In one bounded
existing publication transaction, CAS-retire that predecessor into a retained
tombstone and create the new exact locator, advancing resource/installation
revision. Both audit records commit with the two heads or neither does.
The receipt locates authority; the Store verifies the actual current rows and
source, rather than trusting receipt text as permission.

Serialize replacement through that predecessor CAS: concurrent replacements
cannot both succeed, and a missing/stale receipt is not a fresh-install bypass.
On ambiguous commit, read back the exact old/new locators and mutation intents
before returning an installation receipt. Never resuscitate the old head on
replay. Current spending/source joins reference the original profile generation,
so the revoked predecessor denies subsequent reserve/send even while the new
locator is active. Already accepted effects remain unknown, not undone.
Installation and replacement are closed trusted producer paths, not arbitrary
profile UPSERTs or an unbounded resource scan. Losing the installation receipt
blocks replacement pending explicit reconciliation; it does not authorize
another active head under the same resource ID.

Use existing profile/publication audit, Attempt source/admission, operation and
spending records. No endpoint inventory table, credentialless-secret table,
second lifecycle or additional ledger is introduced. Update the entity registry
for changed record semantics, not by rewriting applied migrations.

## Endpoint policy and execution placement

The first network policy, `local_loopback_http.v1`, accepts only canonical
`http://127.0.0.1:PORT/v1` or `http://[::1]:PORT/v1`, with an explicit nonzero
port. The request path is exactly `/v1/chat/completions`. Reject userinfo,
query/fragment, hostname/DNS, IPv6 zones, other loopback aliases, encoded or
dot-segment paths, backslashes and an implicit port. Do not normalize a
different address/path into an allowed one. Recipes use the numeric address,
not `localhost`.

The connector dials that literal endpoint only. Disable ambient proxies,
redirects, cookies, automatic compression, automatic retries and endpoint/model
fallback. It never accesses metadata, LAN, public hosts, Unix sockets or model
supplied URLs. A separate remote/TLS policy can later share the codec, but is
not enabled by this local profile; it must independently specify TLS roots,
name/address policy and destination pinning without weakening cloud SSRF rules.

The development worker and owner model server must run in the same host network
namespace. Do not rewrite container localhost to the host, inject a tunnel or
expose the service publicly. The Web BFF does not perform inference requests.
The normal local control/worker route uses logical `managed` placement, not a
fabricated attached worker/enrollment record.

Add a closed `substrate.kind=local_development` variant for this deployment:
`workload_mode=in_process_direct`, an attested actual worker executable digest,
explicit host/network-namespace installation identity and finite profile.
`ImageDigest` is empty only for this new kind; the outer artifact digest refers
to the executable. Existing image-backed variants keep their image requirements.
Every consumer that assumes an image/allocation must handle this new variant
explicitly or reject it. Old binaries reject the unknown kind; no decoder alias
or implicit cloud/local conversion is permitted.

This local profile runs trusted Go codecs/loop and constrained tools, never
model-generated shell/code. Its host trust is the developer's own environment,
not certified cross-host/container isolation. It supplies no fake cgroup,
image, proxy or cloud erasure evidence. Existing isolation claims cannot be
manufactured to pass a substrate validator. A bounded local-runtime observation
must attest the facts this profile actually enforces: installed executable,
fixed connector, memory/body/deadline limits and cleanup of owned buffers/I/O.
Yandex profiles still require #90/#92; native CLI isolation still belongs to
#133/#215. Dockerless development does not depend on either container path.

### Registered local connector observation

The existing `OwnedProxyPublicationV1.Bodies` and registered-proxy observation
assume HTTPS/public-unicast. Do not pass a loopback socket through them as if
it had DNS/TLS evidence. Introduce canonical tagged successors
`OwnedProxyPublicationV2` and `RegisteredProxyAttestationV2`, reusing the existing
registration bodies, current observation storage, original producer keeper and
reservation references. The explicit kind is `pinned_https.v1` or
`local_loopback_http.v1`; all serving writers/readers use the successor, without
an untagged V1 decode fallback. Keep existing HTTPS policy semantics unchanged.
Historical rows are not rewritten; the implementation specifies an approved
writer-first/drained cutover and rejects unsupported old serving state.

The local variant seals exact profile/endpoint policy, connector artifact and
installation identity, literal-address/port digest, original source/allocation,
tenant/owner/Run/Attempt/lease/fence/physical claim, model operation digest and
sequence, observation time and exclusive shortest expiry. It records no DNS
resolution set and no verified TLS; those fields are absent, not fabricated.
The HTTPS variant still requires its real DNS/pinned-address/TLS observations.
Policy/body references remain bounded, authorized and content-addressed.

Only the privately installed local connector produces a local observation,
after it has opened the exact literal socket and checked the actual peer.
Preflight sends no HTTP request. Its original keeper retains that same
connection; a recreated socket or copied digest is not its observation.
Publication authenticates that producer and exact original claim/source in the
same existing bounded transaction; it does not grant send authority.
Operation reservation and before-send reread the tagged observation/current
generation and all original authority, then consume the existing one-use
capability before the first HTTP request byte on the retained connection.
Cancellation/expiry/failed registration closes the owned preflight socket.
Each next operation needs its own fresh observation; no socket/observation
reuse across sequence, attempt or resource generation. Local tags cannot
authorize cloud/HTTPS or MCP/search routes, and vice versa.

### Concrete connector ownership and composition

Implement the local connector in `internal/localloopback`. It holds the concrete
`localoperator.Source`, the original
`serverlessharness.BoundedOwnedAllocationPreparationV1`, the existing operation
issuer and the concrete `*ydbstore.Store`. This direction keeps network I/O out
of the persistence adapter and avoids importing Store into the harness/egress
packages. Do not substitute an injected approval callback or a second Store,
issuer, invocation ledger or scheduler. The codec remains a pure data package.

The original allocation supplies a private operation resource lease, not a
caller-created authority DTO. It checks the exact retained Source identity and
entered allocation, fixes the entire model operation before dialing, and shares
one bounded preflight slot across connector wrappers. Seal scope, lease/fence,
physical claim, allocation/source/profile, sequence, request digest/byte count,
result ceiling, kind and absolute deadline. Claiming that slot is irreversible
for the operation, including failed/cancelled preflight; another wrapper cannot
redial the same sequence. A subsequent operation still needs the existing
durable predecessor/outcome and budget checks. No unbounded operation map is
required, and losing process-local state never reconstructs send permission.

The lease inherits the original invocation/allocation cancellation and active
deadline, intersected with the operation and installed Source window. Source
closure also revokes owned connector I/O through a Source-owned signal; it must
not cancel the caller's context or close a shared model/worker. Merely checking
`VerifyEnteredLocalAllocation` once, matching copied digests, or making a fresh
context with the same nominal deadline is insufficient lifetime ownership.

First construct noncopyable bootstrap metadata fixing the concrete Source,
preparation issuer and operation issuer. Construct the connector keeper with
that same bootstrap retained immutably. Its `CreateStore` fixes the actual keeper
pointer and creates one inert Store. The bootstrap itself implements the concrete
preparation burn by calling that same Store's existing durable transaction, never
a forwarding callback or caller-supplied Boolean. Construct the allocation bridge
with this bootstrap as its burn owner. `Complete` verifies the bridge's original
Source/issuer/burn identities, installs its verifier and release-publishes one
immutable Store/bridge/keeper tuple atomically. Until then, all local owned
preparation/publication/reservation/send paths deny. Abandonment leaves it inert.

This initial installation is a dedicated local constructor seam, not a mutable
setter on ordinary Store options or an alternate serving authority. Reject nil,
copied, foreign, repeated or replacement bindings and partially installed owners.
There is no separate mutable `BindStore` or assertion that keeper binding already
happened: the keeper resolves `RuntimeFor(self)` on its original bootstrap before
preflight/send. Only the actual retained keeper can obtain the completed original
Store/bridge tuple. Bootstrap-backed Store readiness also checks that this exact
Store is the tuple's Store; copying either object cannot reuse its ready state.
Identity checks invoke no keeper callback and grant no send permission. They add
no gates to unrelated existing Store profiles. #217 supplies the closed initial
composition seam; #218 owns its normal serving integration. Source still is not
the missing complete effect-source verifier or independent MCP/search custody.

Dial only the admitted numeric loopback TCP address. Check the actual remote
address and retain that same socket; preflight writes no HTTP/TLS/application
bytes. The local policy body is canonical data derived from independent sealed
endpoint/protocol/auth/connector/host/namespace/limit configuration, with an
acyclic digest. The fresh observation additionally seals the full profile and
operation identities. Extend the existing V2 publication body decoder, domain
builder and Store publication/snapshot readers together; neither HTTPS-shaped
bodies nor a newly added transport tag alone are a local observation.

Before writing the fixed `/v1/chat/completions` request, irreversibly consume
the retained socket lease, call the retained Store's
`AuthorizeAttemptOperationBeforeSend` for its current joins and existing one-use
capability consumption, then recheck local cancellation/deadline and exact
payload binding. No earlier publication or identity check is send permission.
First implementation supports `auth=none` without a secret read or auth header;
Bearer fails explicitly until the original #190 session owns its proper custody.
Do not silently ignore a requested Bearer profile.

Every denial, cancellation, expiry and failed/ambiguous registration closes
owned sockets, joins owned I/O and releases bounded buffers. Historical
authenticated observations can remain readable after closure but cannot mint a
fresh socket or capability. Partial writes/lost replies stay reconcile-only;
there is no reconnect/retry or remote-stop claim. Connected tests use actual
owned loopback peers and the existing scripted Store transaction fixture to
prove zero HTTP bytes on denial and exactly one request on success. Keep such
fixtures in external `ydbstore_test` tests with narrow test-only helpers, not
production dial/approval injection. Their result is not real YDB, model or
normal-path demo evidence.

### Local operation invoice custody

The local installation seals `InvoiceTermsV1` bytes and their SHA256 digest.
Those are owner-local invoice terms, not an upstream
`ProviderPriceObservationV1` tariff or its domain-separated digest. Do not
re-encode a local invoice as a provider tariff, change an installed digest, or
interpret a missing/unknown tariff as zero to make operation reservation pass.

Extend the existing private operation-price evidence with exactly two exclusive
variants: standard provider-price evidence and local-invoice evidence. Select
the local variant only for the exact configured/local/owner-local profile and
authenticated spending tuple, backed by the original completed local bootstrap.
Missing or foreign custody denies; never fall back by decoder success. Other
provider routes preserve their existing price format and quote validation.

The concrete original `localoperator.Source.ReadInvoiceTerms(ctx, profileRef,
cap)` supplies an owned clone of the retained canonical invoice body from its
private intake. Require an alive Source, accepted receipt, exact profile
locator/generation/digest and original configuration/installation. Check the
original body reference, digest and byte count before cloning; accept only
positive caps at most 4096 bytes. No file reread, key/manifest export, caller
terms, public approval DTO, new port, issuer, table or price ledger is needed.

Capture custody outside retryable SQL. Evidence retains the original Source
lifetime association, not only decoded terms. In the existing bounded
transaction, rejoin the exact current profile and spending and require the
original lifetime to remain active. Verify tenant/owner, configured endpoint,
vendor/model, billing/resource/install/profile identities, currency/revision,
both owner-supplies declarations, and both invoice amounts equal zero. Match
the original config terms reference with profile provider-price/provider-terms
and provider spending price/terms digests. Require explicit known-free invoice
state, zero admitted provider allowance and zero operation monetary debit;
all original token/count/byte/time and four-component accounting remain.
These terms do not fund MCP/search or assert zero hardware/economic cost.

Keep terms issuance and price observation as separate original timestamps:
they need not be equal. Authoritative transaction time must be contained in
both original windows and the operation deadline cannot exceed either expiry.
Preserve original publication-time restrictions rather than rewriting dates.
Recheck the captured original lifetime on reserve and before-send, and after
the final current transaction before consuming the capability. Actual Source
closure additionally cancels the original operation/connection before writing.
No filesystem/network work occurs in a SQL callback. Historical reads and
standard free/paid provider evidence keep their existing semantics.

Conformance must join genuine Source/accepted receipt/original invoice custody
to actual Store reservation and before-send, including different valid original
timestamps, profile replacement/revocation, foreign/missing/closed custody and
closure between capture and send. Wrong bodies/refs/model/revision/currency,
paid or unknown terms, nonzero debit and external-service funding substitutions
deny. Scripted SQL is labelled separately from native YDB and real-model proof.

## Compatibility and capability qualification

Official interfaces checked on 2026-10-17:

| Capability | Ollama | llama.cpp server | vLLM |
| --- | --- | --- | --- |
| Chat Completions | Nonstream and stream; compatibility is a subset. | Both modes; upstream does not promise complete spec parity. | Chat API requires the appropriate chat template. |
| Authentication | Local HTTP accepts no Authorization; SDK dummy keys are not server credentials. | Optional configured API key; no-auth is possible. | Optional configured API key, not protection for every server endpoint. |
| Structured tools | Documented tools/tool choice; qualify the selected model. | Tool-aware Jinja template/handler required; parallel support varies. | Appropriate parser/template and auto-tool settings required; supported choices vary by parser. |
| JSON schema | Documented response format/schema support. | Supported schema subset and conversion limits. | Structured output backend/reasoning combinations impose limits. |
| Images | Supported with a vision model. | Requires the matching multimodal setup. | Requires compatible multimodal model/processor. |
| Stream usage | Documents include-usage option. | Usage framing is profile-specific. | Include-usage must be explicitly requested/supported. |
| Cancellation | HTTP cancellation is not a documented bounded server-stop receipt. | Same distinction. | A Responses cancel API does not establish a Chat Completions stop guarantee. |

Sources: [Ollama compatibility](https://docs.ollama.com/api/openai-compatibility),
[Ollama structured output](https://docs.ollama.com/capabilities/structured-outputs),
[llama.cpp server](https://github.com/ggml-org/llama.cpp/blob/master/tools/server/README.md),
[llama.cpp function calling](https://github.com/ggml-org/llama.cpp/blob/master/docs/function-calling.md),
[vLLM server](https://docs.vllm.ai/en/latest/serving/online_serving/openai_compatible_server/),
[vLLM tools](https://docs.vllm.ai/en/latest/features/tool_calling/), and
[vLLM structured output](https://docs.vllm.ai/en/latest/features/structured_outputs/).

A capability is usable only for the exact installed server/model/template/config
tuple after closed wire fixtures and the selected live qualification. A
documentation claim, model name or `/models` entry does not enable it. The first
codec declares nonstream text and complete function calls; it rejects stream,
Responses, unsupported response formats and unqualified media before send.
The eventual file/image demo must qualify appropriate preprocessing or a vision
profile through #175/#191; transport delivery does not remove that requirement.

## Bounded model operation

The request is one JSON Chat Completions body with the exact model, `stream=false`,
`n=1`, explicit output-token limit and already authorized messages/tool schema.
It contains no arbitrary extra parameters, router metadata, response URL,
provider search, native MCP or remote image URL. Admitted images, when supported,
are bounded materialized data, not fetch instructions.

The adapter receives one process-local authorized model operation from #190.
The request digest covers the complete encoded payload and profile. That
operation is consumed before request effects; the adapter does not mint its
own permission or reserve a second budget. It returns typed complete text or
one function call, usage/provenance, finish classification and transport facts.
It executes no tool and appends no canonical message itself.

The initial hard ceilings are the smaller of profile, admission, remaining
operation authority and these caps:

| Bound | Maximum |
| --- | --- |
| One request and response body | 1 MiB each |
| Response headers | 16 KiB |
| JSON nesting and members per object | 32 and 256 |
| Choices and tool calls per response | 1 and 1 |
| Tool name and call ID | 128 UTF-8 bytes each |
| Tool arguments and final text | 4 KiB and 256 KiB |
| Messages and tool descriptors | 256 and 16; also the aggregate byte cap |
| Output tokens | 2048; profile may require fewer |
| Operation and cleanup | 30 seconds each, never beyond their respective authority |

Validate and bound input before encoding/allocating large copies. Limit the
actual wire body while reading even when Content-Length is missing or false.
Reject duplicate JSON keys, invalid UTF-8, malformed/truncated/trailing JSON,
nonfinite or overflowing numbers, ambiguous choice/call identity and oversized
values. Bounded unknown metadata may be ignored only after the envelope limits;
unknown effect-bearing fields or finish reasons cannot authorize a tool.
Tool argument JSON must be complete and validated against the frozen tool
schema before #191 admits it. Reject multiple/duplicate calls without dispatch.

Only a complete valid final stop or tool-call finish is usable. Length/content
filter/error/unknown finish produces a stable stop reason, not successful text.
Keep configured model, provider-reported model and model provenance separate;
an alias is accepted only as an explicitly declared exact response-model value.
Missing/mismatched model metadata cannot become verified upstream identity.
Never publish reasoning/hidden internal fields or raw transport/provider errors.

Usage is provider-reported when structurally valid, otherwise unknown; it is
not independent token measurement. Reserved conservative input/output limits
still apply, including schema/media overhead and exact context fit. Unknown
token fit denies before send; missing usage does not refund authority or grant
another operation. Preserve known, estimated, reserved and unknown categories.

## Lifetime cancellation and durable continuation

#190 owns the original provider session, exact resource/profile/fence and
credential lifetime. No-auth skips credential work, not admission, preparation,
current authority or durable operation reservation. Bearer mode uses a reviewed
session extension that owns custody; it must not retain a callback from the
old one-shot materializer or repeatedly invoke that boundary.

Recheck current owner/resource/registration, cancellation, lease/fence, profile
and remaining budget before every send. Use the shortest original deadline;
progress and HTTP bytes cannot extend it. Per-operation client connections are
not shared across attempts or auth generations. Cancellation closes the owned
request/connection/body and clears buffers under bounded cleanup. Do not stop,
unload, restart or kill the owner's inference server.

Closing HTTP does not prove server compute stopped or an accepted request was
undone. A send/write/read/timeout/crash ambiguity remains accepted-outcome-unknown
and reconcile-only. There is no automatic HTTP retry, protocol/model fallback,
repeat on reconnect or resumed provider send after process loss. Tests count
actual requests at owned peers, including partial writes and lost replies.

Persist the validated complete normalized response before reserving any tool;
persist the tool result before model continuation. Reuse the original canonical
Attempt operation/effect and response-blob custody. On persistence failure,
stop without a tool/continuation. Terminal output/artifacts and safe errors use
the existing canonical finalizer; the transport's response is not a second
transcript or terminal authority.

## Operator authority and cost

The operator installs the exact local profile and authorizes its owner, data,
model, endpoint, original finite window and resource envelope. Installation is
not a generic public `approved=true` write. Extend the existing authenticated
profile publication/source grant with `owner_local_installation`; verify its
actual trusted local operator source and current revocation head. Do not admit
a real local profile through `fixture_installation` or skip four-component
Attempt spending checks merely because no API key exists.

For the explicitly owner-supplied no-invoice model/host, the installed terms may
authorize zero Sessionless provider/substrate invoice allocations in the existing
monetary ceiling and spending attachments. That fact requires the operator's
explicit bounded source/terms, not inference from localhost, auth=none or a
configured zero. The invoice revision and original source digest/expiry remain
current-checked with the profile. Preserve existing accounting sums and one-use
operation debits. No reusable balance, new payer authority or extra table follows.

Zero invoice is not zero economic cost: memory, wall time, tokens, disk and
energy/opportunity-cost estimates remain separately labelled. Unknown economic
cost is not presented as measured zero. Existing price evidence must state that
its zero rate is an operator-authorized local invoice term, not an upstream
published/observed tariff. If a proposed source cannot represent that provenance
without false fixture/price facts, its implementation is not accepted.
Paid/unknown external MCP/search services retain their independently authorized
component prices and budgets; no local endpoint opens those capabilities.

## Local qualification and implementation handoff

The real run manifest names one server/version/model/template, explicit auth,
literal endpoint, actual worker host, data class, supported capabilities,
no-paid-upstream commitment, finite request/token/time envelope and stop rule.
The owner supplies/starts the endpoint. Do not probe private config or claim
immutable weights, no-cloud mode or owned daemon from an HTTP reply. In
particular, Ollama can route cloud models through its local server.

Default CI uses owned fake HTTP peers only. The opt-in real smoke follows
separate owner authorization, starts with synthetic/public input and records
actual transport/capability limitations without exact generated-text assertions.
No install/model pull/daemon mutation, paid fallback, tunnel, cloud deployment
or shared migration is authorized by this design.

Implementation owner is the primary agent, with disjoint bounded implementation
agents where useful and independent requirements-led review of each exact head.
The necessary delivery boundaries are registered as real children coordinating
#212 under #176; no additional epic is needed:

1. [#216 — canonical local resource and runtime admission](https://gitcode.com/urandon/sessionless/issues/216): new closed variants,
   current profile/resource/credential and owner-local funding source, honest
   executable substrate, exact evidence/registry gates and entity-registry
   updates. Reuse existing records and preserve old OpenRouter/fixture behavior.
2. [#217 — endpoint contracts and bounded HTTP codec](https://gitcode.com/urandon/sessionless/issues/217): consume #216's canonical contracts;
   profile/URL validation, explicit optional/no auth, nonstream complete text/tool
   normalization, actual connector observations and byte/deadline/cancellation/
   no-retry fixtures. Pure codec work can proceed independently; driver remains
   disabled until canonical admission/composition is connected.
3. [#218 — provider session and local Web demo composition](https://gitcode.com/urandon/sessionless/issues/218): connect #216/#217 through #190/#191 and local #175/#35 without bypassing their
   scope. Supply operator configuration/recipe, deterministic normal-path
   text/tool/result/answer and unauthorized/revoke/fence tests; later bounded
   owner-approved real proof. #212 remains open until all its criteria pass.

Children name this design/version, predecessor evidence and concrete exit
checks. The first must deliver the current-authority contract and tests, not
only extend enums; the first two need no cloud/sidecar proof. The third cannot claim
a full agent from a disconnected transport smoke. Existing #189–#197 still own
their respective loop/compaction/MCP/search capabilities and integration.

Follow [Go testing practices](../testing-best-practices.md): barrier-driven
cancellation/partial-send peers, isolated tests, bounded cleanup, uncached race
and shuffle/stress coverage, malformed/oversized/schema fixtures, auth absence
and redaction, denied URL/proxy/redirect/placement and exact effect counters.
Obtain independent exact-head review, repository docs checks and required CI
before merging each implementation. Validate source changes against the real
task, not just tests; a design merge does not activate inference.

Causal authority fixtures must prove that an endpoint/model change creating a
new locator revokes old reserve/send and spending references, that two competing
replacements yield one active successor, and that lost-commit recovery cannot
revive either head. Connector fixtures must deny forged producer, copied
connection, wrong kind/peer/sequence/source/allocation and expired or revoked
observations before HTTP bytes; a real owned loopback peer counts zero sends
for each denial. Neither test family may pass by accepting fake TLS/DNS or
skipping the existing registered-observation/current-authority joins.

## Queued runtime composition

This addendum closes the serving composition gap, not by adding another harness
or weakening a guard. Reuse the [#189 current-authority contract](owned-harness-current-authority.md),
#190 original provider-session lifetime, [#193 MCP contract](owned-harness-tools-mcp.md)
and [#194 search contract](owned-harness-web-search.md). The managed minimum
continues to require MCP, general web search, durable tool continuation and
compaction; a disabled-tool/text-only profile cannot substitute for it.
Local development remains one qualification environment, not fulfillment of the
separate managed-cloud or attached-worker release proof.

### Existing seams and missing producers

The source audit for this addendum uses main `f31ff93`. Names in the right column
are required deltas, not claims that a runtime already supplies them.

| Existing canonical seam | Required composition delta |
| --- | --- |
| [Source intake and receipt](../../internal/localoperator/source.go), [runtime observation](../../internal/localoperator/execution_runtime.go) | Retain the genuine private-file Source, accepted original profile receipt, invoice and bounded evidence bodies; add complete component-specific source authentication below. |
| [Spending producer](../../internal/localoperator/funding.go), [publisher](../../internal/ydbstore/attempt_spending_publication.go) | Main authenticates provider/substrate only. Add real MCP and search provenance, exact original-head recovery and no top-up, using the existing four spending records/audits. |
| [OwnedAttemptEffectSourceVerifierV1](../../internal/ports/owned_attempt_effect_source.go) | Supply its concrete installation-backed implementation; existing scripted implementations are not production provenance. |
| [HarnessBinder / authority validation](../../internal/ports/canonical_ingress.go), [canonical ingress](../../internal/sessioningress/service.go) | Add the trusted configured-local owned binder and an exact configured tuple in managed authority validation; the current non-subscription branch permits only a deterministic fixture. |
| [Execution package builder](../../internal/domain/attempt_execution_admission.go) | Use its acyclic derivation from the authenticated published profile/four snapshots. The builder is pure data validation, not approval. |
| [Existing scheduler](../../internal/ydbstore/scheduler.go) | Join the configured arm to the same bounded owned admission callback; it currently requires the deterministic backend and has no public owned serving route. |
| [Run](../../internal/domain/run.go), [quota](../../internal/domain/quota.go), [slot](../../internal/domain/scheduling.go), [scheduler policy](../../internal/scheduler/policy.go) | Add the closed no-subscription association and complete admission/release/index contract below. Existing validators, physical slot and expiry readers require actual subscription identity; changing a binder alone cannot supply local capacity. |
| [LocalV1 factory](../../internal/providerloopsession/local.go), [pre-lease facade](../../internal/providerloopsession/local_worker_entry.go), [preflight](../../internal/ydbstore/local_worker_preflight.go) | Create one original inert session/Store/bootstrap/keeper tuple per Manager invocation, before its read-only preflight; do not reuse a consumed session. |
| [Worker command](../../cmd/worker-runtime/main.go) | Install this opt-in composition, retaining deterministic/default-off behavior. The existing HTTP batch reuses one Manager across up to ten messages; the local arm must construct/close its invocation owners per message. |

Non-rejected #193 work contains a real private-intake MCP funding candidate
(`internal/localoperator/mcp_funding.go`, `service_evidence.go` in
`internal/mcpadmission`, and the MCP branch of `funding.go`). It retains an
independent grant, price/terms reports and their actual source-document bytes.
It is unmerged work, not an available main API or a complete four-component
verifier. Reuse only after its own review/integration. The rejected #193
`local_prepared_model.go` / `local_model_effect.go` changes are not approved by
this design and must not be reimplemented under another name to evade that
boundary.

### Complete Source authentication, not a permission callback

Keep one concrete `localoperator.Source` per authenticated installation. Its
existing `ManifestV1.Evidence` / `EvidenceBodyV1`, private source key and accepted
`OwnedProfilePublicationReceiptV1` remain the custody format. Do not add a
secret file format, per-Run approval map or exported signing/approval API.
Extend its private component readers, publication verification and the existing
`VerifyOwnedAttemptEffectSource(AttemptExecutionSourceV1)` interface together:

1. Authenticate the original Source identity, exact retained manifest/config,
   receipt profile locator/generation/digest and installation/owner/payer.
   Correlate the original configured endpoint/backend/substrate, worker artifact,
   capability/review evidence and complete canonical profile, not just a MAC.
2. Authenticate every component's `SourceCanonicalBytes`, body length/hash and
   domain-separated source authenticator against those retained originals.
   Revalidate its exact scope, profile, price/terms, known amount, currency,
   resource/revisions and original windows. DB-assigned publication/update
   stamps and monotonic revocation fields are not source-signed declaration
   fields; current physical scalars/digests must still match in the Store.
3. Verify exactly four ordered snapshots and the complete package digests using
   the canonical source/package validation. No omitted component, fixture source,
   arbitrary current head, caller quote or copied body reference is accepted.

| Component | Authentic retained basis and required separation |
| --- | --- |
| `provider` | Original `InvoiceTermsV1`, exact configured model/endpoint and explicit owner-supplied model; zero local invoice only. Preserve the existing invoice source-authenticator domain. |
| `substrate` | The same explicitly owner-supplied host invoice, exact original executable/installation/substrate; independently scoped substrate envelope. This says nothing about economic cost or external services. |
| `mcp_tool` | Actual bounded `MCPToolGrantV1`, independent `MCPPriceEvidenceV1` / `MCPTermsEvidenceV1` reports and their retained original service documents, as developed under #193. The MCP-specific source-authenticator domain cannot accept invoice or search bytes. |
| `web_search` | Actual typed search grant required by #194 plus separately retained price and terms interpretations of the exact search service/profile and original independent documents. Bind the existing `ExternalServiceCostCeilingV1` search kind, grant/resource/revisions, payer/currency, known finite price state/amount and original windows. A distinct search source-authenticator domain must reject MCP/invoice substitutions. |

Search-specific grant/report decoding and trusted producer are **missing**, not
implied by the generic [external cost shape](../../internal/domain/external_service_cost.go)
or consent codecs. Implement them under #194 and feed their bounded nonsecret
bodies through the existing Source evidence custody. The reports must bind
the exact grant, original source URL/body hash/size, observation and exclusive
expiry; price additionally binds revision/currency/state/finite amount, terms
the public-data/use/consent boundary. URLs are provenance data, never something
Source or a SQL retry fetches. A renamed grant/report is not its independent
source document. One actual document may support both reports, but local
invoice bytes cannot support either external-service claim. Preserve existing
manifest/body caps; an unsupported oversized declaration denies rather than
raising them implicitly.

Private file intake proves which declaration and bytes the operator installed;
it does not prove that a URL is official, a tariff is accurate, terms permit the
use, a service is free, a model is qualified or a live schema is unchanged.
Independent research/review must substantiate those external facts before
enablement; separately authorized qualification supplies actual service/model
observations. Unknown is not zero. This design neither selects a new search
provider nor turns the proposed Tavily quote into current funding authority.

All verifier work is bounded and I/O-free on every Store retry. Historical
source authentication may use original issuance/publication times after their
windows have elapsed; it never treats that as current permission or revives a
closed Source's execution lifetime. Executable permission still requires the
actual current DB-time joins. `ExecutionLifetime` cancellation remains attached
to every invocation and owned I/O; matching old bytes cannot recreate it.

### Trusted binder, original issuance and queued admission

The binder is installed trusted composition, not a new HTTP configuration API.
It receives only server-derived `HarnessBindingRequest` actor/Session and stable
planned Run/Attempt IDs from `sessioningress.Service`. It borrows the one exact
installed Source/receipt and canonical Store; browser/model/queue fields cannot
select a different endpoint, payer, source, grant or profile.

Before a first package is emitted, use existing `PublishOwnedProfile` /
`Source.AcceptReceipt` installation results and `Source.SpendingPublication` /
`PublishAttemptSpending` for its four actual components. Add the minimum bounded
typed point-read/recovery seam over the existing profile and four spending
heads/audits: original locator, full physical-scalar/record/digest correlation,
source authentication, original scope and DB timestamp, never a scan or a
public "approve package" method. Neither `Configuration()` nor a receipt alone
is current-state proof.

An existing component is read back exactly, never reissued with another mutation
or extended window. If initial publication is partial, missing components may
be published only within the already fixed original envelope, capped by the
earliest retained component/source deadline; an ambiguous result first requires
exact readback. Conflicting, revoked, expired, foreign or unverifiable history
denies. Retain an incomplete publication for the existing lifecycle; do not
replace it, top up amounts or create another Attempt merely to recover.

After all four heads are confirmed in one bounded current snapshot, the maximum
original profile/component DB publication time is the package's deterministic
logical issuance; expiry is the minimum original window. Neither calculation
proves a producer event or authenticates an opaque grant/receipt. Call
`BuildOwnedAgentExecutionPackageV1` with that logical issuance only after the
concrete Source has authenticated every preserved original component body,
source/installation association and receipt. It performs pure derivation, not
fresh signing or spending issuance. Retry wall time cannot renew the package.
Before canonical commit this is only a candidate, not a recovered committed
Source. After commit, recover the exact original Source String from canonical
outbox/Job/Final history; never rebuild missing committed history from current
heads or digests. Lost original source/key/receipt association denies.
Recheck current validity against the snapshot's DB time before returning
`ExecutionAuthorityV2`; no capacity or operation capability exists yet.
On an already committed frontend delivery, existing `LookupBound` wins before
binder/publication and returns the original result.

Ingress `ReceivedAt` / `CommittedAt` describe the canonical user event, not a
funding clock; they can precede these publication timestamps. The exact owned
configured arm of `ManagedExecutionAuthorityV2.ValidateForScope` must validate
the original package's structural time correlations at its sealed issuance,
not backdate funding or rewrite event timestamps to satisfy `At`. Fresh validity
comes from explicit source/current joins at authoritative DB time, not a browser
time or an out-of-transaction approval. Add those bounded original profile/four
spending and concrete source checks to the SAME existing canonical commit
transaction for this arm, alongside the actual writable frontend
binding/revision/Session/source-scope join. Its DB observation is distinct from
the preserved event timestamp; both must retain their real meaning. Store the
exact authenticated source String on the existing outbox. A switched binding,
foreign Session, revoked source or elapsed window denies atomically. Preserve
all legacy validation branches; no observation field or receipt becomes a
standalone permission carrier.

Select the configured owned scheduler route only from trusted startup plus the
actual stored closed tuple: owned profile, `direct_openai_compatible`,
`configured_endpoint`, `local_endpoint`, `owner_local`, `local_development` and
explicit supported auth. Queue bytes alone cannot enable it. The same existing
bounded callback must authenticate Source and rejoin
`readOwnedAdmissionPrincipal`, `readOwnedAdmissionSources` and current provider
resource before its original slot/quota/Run/Job/Final/reverse-index/outbox writes.
There is no second admission or manual per-Run SQL. Final attaches the actual
reservation and DB admission time without rebuilding Source. Exact replay
preserves that original hold; unknown/failed commits return no candidate.
Measure the complete graph within the unchanged 10s / three callbacks /
32 statements per callback / 96 total / 4 MiB bounds, not a new helper budget.

### Local resource capacity without a fabricated subscription

The local arm needs a closed tagged capacity association, not an empty or
invented `SubscriptionConnectionID` accepted by a weakened validator. Its local
variant contains version, `kind=local_endpoint`, tenant, owner and the actual
opaque resource ID minted by the original installation Source. Derive it only
from the authenticated profile/source, never frontend input. This is a stable
capacity identity across profile replacement; original profile/resource
revisions and spending generations remain separately bound by Source/Final.
The subscription variant still names an actual subscription connection. A
local association and a subscription identity are mutually exclusive; mixed,
unknown, foreign or missing association denies.

The local association has exactly these canonical JSON fields, in this fixed
producer order: `version`, `kind`, `tenant_id`, `owner_id`, `resource_id`.
`version` is integer `1`; `kind` is exactly `local_endpoint`. Validate tenant,
owner and resource with the existing [opaque identity validators](../../internal/domain/identity.go):
nonempty, at most 160 bytes, ASCII `[A-Za-z0-9][A-Za-z0-9._:-]*`.
Use the [existing canonical domain codec convention](../../internal/domain/owned_agent_current_codec.go):
closed typed encoding, existing record cap, unknown/trailing/duplicate/missing
field rejection and exact byte-for-byte encode/decode roundtrip. No alternate
field order, whitespace, numeric form, null or case alias is accepted.
The association digest is lowercase SHA256 of those exact canonical bytes;
the physical local capacity key is exactly `local-v1/` followed by that
64-character digest. Persist and compare the original bytes and digest, not
only the derived key. Its tag/hash is a bounded lookup address, never authority.

Extend the existing Run and QuotaReservation canonical shapes/immutability and
physical-scalar checks to carry that local association while their subscription
field is absent. Match Run, reservation, stored outbox/Job source and Final on
the same tenant/owner/resource; do not infer an association from an empty field
or substitute the subscription field with a resource ID. Update ingress request
validation, worker loaded-state/authority checks, bounded Run projections,
usage observations and expiry candidates to preserve the same explicit arm.
Local usage remains local/provider-reported or unknown, not a fabricated
subscription entitlement/quota observation. Genuine subscription records retain
their existing identity and validation semantics.

There is a real storage delta, not a configuration-only solution:
[Run](../../migrations/ydb/00007_create_runs.sql) and
[reservation](../../migrations/ydb/00013_create_quota_reservations.sql) payloads
can retain the new association, but the
[slot table](../../migrations/ydb/00039_create_subscription_scheduler_slots.sql)
has only a subscription-labelled primary key and no local identity record;
the [expiry index](../../migrations/ydb/00026_create_quota_expiry_v2.sql) also
projects only subscription identity. Add a nullable canonical local-association
`local_capacity_association String` and `local_capacity_digest Utf8` to these
two existing tables through a new reviewed migration. Local rows require both
non-NULL, the complete strict canonical record, matching digest/derived key and
exact tenant/owner/resource correlation to the actual Run/Quota/source.
Both columns NULL select the genuine legacy subscription arm, which must still
validate its real subscription identity and owning records. One NULL, unknown
kind, malformed/noncanonical bytes, key/digest mismatch or conflicting owner
denies; there is no decoder fallback to another arm.

For the LOCAL arm only, the SQL adapter uses the historically named slot PK
and expiry `subscription_connection_id` column as the physical capacity lookup
address above. It is never assigned to a domain `SubscriptionConnectionID`;
Run/Quota subscription fields remain absent. These physical columns are `Utf8`,
so the 73-byte ASCII key fits their schema and existing bounded scalar reads.
Its slash is deliberately outside the domain subscription identity grammar.
Actual legacy keys/rows remain byte-for-byte unchanged, without retyping or
backfilling them into local records. Any collision with an existing real legacy
row, or with different full local bytes/ownership, denies the local transition;
never overwrite, repair or reinterpret the conflicting row. Every slot/expiry
read/write/release must project both local columns and validate the complete
selected arm against Run/Quota, not route solely from the key prefix.
Update the entity registry, slot/scalar readers/writers, quota-expiry
producer/list/consumer, retention inventory and every relevant projection
together; no new table, index traversal, ledger or scheduler is introduced.
Before enabling local writers, drain old control/worker/reconciler executors
that do not implement this exact arm. Applying the additive migration alone
does not establish runtime compatibility; an old expiry reader must never be
allowed to cast the new address into subscription authority. Updated executors
reject unsupported local records rather than treating them as subscriptions.

Inside the SAME bounded admission callback, rejoin the actual current local
installation/profile, four funding heads, writable principal and original
resource identity. These establish local readiness, not `EntitlementActive`
or a made-up `ProviderQuotaAvailable`. Add the exact local alternative to the
existing `scheduler.Snapshot` / `Evaluate` path: one held reservation per local
resource, unchanged tenant queue/active limits, workload limits and counted
capacity unit. Busy, revoked, expired, unknown or unsupported sources deny;
no subscription row/read is required. Source loss cannot silently grant a slot.
Create the same reservation, expiry projection, slot ownership, counters,
Run/Job/Final and outbox state atomically. Exact lost-ACK recovery requires the
same association, active Run/reservation, original Job/Final/hold and source;
it writes no replacement and never renews the deadline or increments counters.

Extend existing `finishWorkerScheduling` and `ExpireQuotaReservation`, not a
local cleanup loop. In the canonical terminal transaction, success transitions
held capacity to committed; failure/cancellation transitions it to released.
Clear only the slot owned by the exact original Run/reservation and association,
remove its exact expiry key and update queue/active counters once. Neither
release nor replay requires an active subscription or renewed local funding;
they authenticate retained original ownership and the existing terminal/fence
authority. Recompute an empty slot's readiness from current local authority,
or leave it non-ready if unavailable/revoked; clearing capacity never enables
new work by itself. A foreign/mismatched slot denies atomically without touching
its owner or counters. Finalization replay after lost ACK returns the original
terminal result with no second release/decrement/refund or effect capability.

Queued hold expiry uses the existing indexed candidate and same transaction:
match its explicit association to the original reservation/Run/slot, transition
only an expired held reservation, remove its exact expiry projection and undo
only its managed queued count. Pre-lease cancellation uses the same owned
release, including an explicit no-hold no-op before admission. Running or
possibly active/unknown effects are not released by this queued TTL path;
retain existing reconciliation/cleanup/terminal authority before scheduling
capacity is reusable. Revocation cannot strand a known terminal release or
reopen an unknown invocation. Fresh Store/process recovery reads these exact
records; it never recreates a missing hold, funds a replacement or repeats I/O.

### Installation borrow versus invocation ownership

| Lifetime | Ownership boundary |
| --- | --- |
| Installation | Trusted startup opens the original Source and accepted receipt, fixed executable/host/namespace and four service/funding declarations. Control/worker processes use `OpenExisting` for that same original installation where necessary, never `OpenFresh` per message. DB/blob clients are borrowed; replacement retains the existing predecessor CAS. |
| One queue delivery / `Manager.RunOnce` | Construct fresh preparation/operation issuers and inert `NewLocalV1` with that Source and concrete verifier. Its factory owns the original Store/bootstrap/bridge/keeper tuple and `WorkerState()` facade; no allocation, SQL or socket is created by construction. Supply that same session and facade to one Manager. |
| Claimed invocation | Manager alone receives/loads, calls read-only `PreflightWorkerJob`, claims/starts the lease, reserves the sole original effect and enters it. Only then may the original session prepare/register and run the model/tool/result loop. No second claim, preparer, registry driver or permission callback is introduced. |
| Completion | Use the original durable response/tool/compaction bodies, output finalizer, cleanup/reconciliation, delivery wake and ACK. Close/join the invocation owners on every exit before advancing the batch; do not close the borrowed Source/DB or stop the operator's model server. |

Every HTTP-trigger message, including the second message in one batch, gets a
distinct session/keeper/issuers and Manager. Cancellation links invocation,
original Source, lease/fence and shorter component windows; retry/refresh cannot
extend any of them. Duplicates and worker loss use the existing durable
reconciliation/no-replay path, never a reconstructed process capability or new
socket to repeat an uncertain MODEL/tool operation. Default deterministic and
attached paths keep their own exact registration/ownership behavior.

### #218 implementation and evidence checklist

Keep these as linked implementation checks under existing
[#218](https://gitcode.com/urandon/sessionless/issues/218), coordinating its
existing predecessors; no additional enabling epic or manual routine owner
design gate is introduced:

- [ ] #218 with [#193](https://gitcode.com/urandon/sessionless/issues/193) and [#194](https://gitcode.com/urandon/sessionless/issues/194): reviewed real MCP/search custody and four-component Source verifier/publication/recovery. Prove cross-component substitutions, unknown/forged prices, foreign/expired sources, closure and malformed bodies deny without effects.
- [ ] #218 with [#189](https://gitcode.com/urandon/sessionless/issues/189): trusted binder, exact authority-validation and existing bounded scheduler route. Prove original four snapshots/issuance, complete atomic writes, retry/partial publication/lost ACK, current revocation and binding-switch denials; no renewed funding/hold.
- [ ] #218: genuine canonical local Run with **no subscription row** reaches admission, uses one local resource slot and the original finite hold, then releases through success/failure/cancel/queued expiry and fresh-Store/lost-ACK recovery. Assert zero subscription queries, exact-once counters/expiry removal, unchanged original hold, and collision/foreign/mixed/revoked/unknown-source denials. Cover canonical association/key goldens and byte mutations, malformed/missing/wrong tags, a colliding legacy row with unchanged bytes, genuine legacy release untouched, two Runs racing for one resource, replacement while an old hold exists, a foreign slot, repeated completion/expiry and running-unknown TTL non-release; prove the additive physical contract and old-runtime drain before enabling local writers.
- [ ] #218 with [#190](https://gitcode.com/urandon/sessionless/issues/190) and [#191](https://gitcode.com/urandon/sessionless/issues/191): fresh per-message runtime ownership and actual normal queued Manager entry, then complete durable tool/result/model/finalizer integration. Do not substitute an already-running lease fixture or repeated one-shot invocations.
- [ ] #218 with [#175](https://gitcode.com/urandon/sessionless/issues/175): credential-free joined fixture starts at authenticated canonical Web ingress **before** admission/lease, observes the queued state and actual claim, then model → authorized MCP/search result → continuation → canonical durable answer. Refresh/login reads must show the same answer/citations. Include two-message batch isolation, two owners, revoke/fence/expiry/cancel/unknown-outcome and zero-replay controls; qualify compaction/media through their existing tasks.
- [ ] #218 / #212: independent exact-artifact review, Node docs checks, proportionate race/repeat checks and exact-head CI, then an honest operator recipe. Scripted SQL, owned fake HTTP and browser fixtures remain labelled separately from native YDB, real model/service and cloud evidence. Separately authorized operator qualification/real WebUI proof is still required, not implied by merging this design.

No real source/key files, endpoints, service catalog or tariff have been read or
qualified by this addendum. Concrete external funding/terms and exact service
qualification remain evidence to obtain, not values to invent. If an accepted
candidate cannot meet the existing no-paid-upstream envelope or service policy,
report that specific choice for resolution; do not disable required tools, spend
money, change provider or broaden data disclosure silently.
