# Configured local inference for development demos

Version: 0.1.4, engineering design. Owner:
[#212](https://gitcode.com/urandon/sessionless/issues/212), under
[#176](https://gitcode.com/urandon/sessionless/issues/176).

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
