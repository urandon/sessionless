# Sessionless owned agent harness minimum

Version: 0.2.5, engineering clarifications. Design owner: [#180](https://gitcode.com/urandon/sessionless/issues/180); implementation parent: [#176](https://gitcode.com/urandon/sessionless/issues/176). The accepted 0.2.1 package at `42139dfb5cefb266adb77f221c0743b53cfb5217` received independent original-goal review, exact-head CI and guarded merge in [!160](https://gitcode.com/urandon/sessionless/pull/160), under the owner's agreed-MVP engineering delegation. Version 0.2.2 clarifies provider-usage continuation; 0.2.3 specifies bounded operation-body storage; 0.2.4 defines normalized model bodies and Store correlation; 0.2.5 specifies local invocation construction and first-model composition. [#190](https://gitcode.com/urandon/sessionless/issues/190) records independent engineering review and implementation evidence. These increments do not complete the provider session or its normal product composition. The linked implementation checklist is registered in #176. No additional manual human PR sign-off is required; unresolved findings and exact-head verification remain gates. Engineering acceptance does not enable a provider, MCP connection or cloud profile.

## Decision and scope

The managed MVP must execute an actual bounded agent loop: authorized input, a model-generated structured tool call, a useful MCP result, model continuation and a durable answer visible in WebUI. It must also compact a derived context view without rewriting canonical history. A single tool-free request is foundation evidence, not this outcome. The independently required attached-worker path remains [#72](https://gitcode.com/urandon/sessionless/issues/72) with [#129](https://gitcode.com/urandon/sessionless/issues/129) and the selected activation candidate [#133](https://gitcode.com/urandon/sessionless/issues/133).

The owner's PR !160 request also requires bounded general web search, not only official-document location. Add a trusted search adapter with durable source-linked results and WebUI citations. Full-page fetch/find/browser and private queries remain outside this slice; constrained MCP remains independently required.

Choose a Go-owned sequential loop inside the existing isolated managed runtime. Sessionless owns Session, Run, Attempt, lease/fence, admission, effects, quota and terminal commit. Native harness threads, local transcripts, checkpoints and model call IDs never replace these authorities. The exact compaction and tool contracts are [derived compaction](owned-harness-compaction.md), [constrained MCP](owned-harness-tools-mcp.md) and [bounded web search](owned-harness-web-search.md), package version 0.2.1.

## Research and current implementation

Accepted inputs are [#177 loop and exports](https://gitcode.com/urandon/sessionless/issues/177#note_192990035), its [input and restore supplement](https://gitcode.com/urandon/sessionless/issues/177#note_192990146), [#178 compaction v0.3.1](https://gitcode.com/urandon/sessionless/issues/178#note_192990497) and [#179 MCP v0.2](https://gitcode.com/urandon/sessionless/issues/179#note_192991193). Their independent research acceptance does not accept this design or prove executable fixtures. The [two-track owner decision](https://gitcode.com/urandon/sessionless/wiki/MVP-Execution-Tracks-2026-10-10.md) governs release scope.

The following observations use main `d3eb8fdc069938c05be23212e834a7cc3d82a03e`; implementation must revalidate them rather than treating this inventory as permanent.

| Existing surface | Reuse | Required delta |
| --- | --- | --- |
| [Execution ports](../../internal/ports/ports.go), HarnessDriver and closed registry | Exact preflight/execute/cancel, context/artifact input, result and operational event sink | Sealed agent policy, typed structured model response, subordinate durable operations and authenticated error observations |
| [Outer runtime](../serverless-harness.md), substrate and prepared invocation | One physical claimant, lease/fence, exact allocation and cleanup evidence | Explicit multi-operation loop profile; the one-provider-turn profile does not authorize repeated calls |
| [Egress boundary](../serverless-egress.md) | Single attested proxy, fixed destinations, scoped credential lifecycle, denied ambient network | New bounded loop, MCP and search policies on that proxy; current POST-only provider boundary neither implements MCP transport nor authorizes a search endpoint by configuration |
| [Canonical context](../../internal/domain/serverless_context.go), ContextWindow and immutable snapshots | Exact admitted history and artifact references | Separate derived summary manifest, complete tool units and validation on fresh restore |
| ExecutionToolEvent and WorkerCompletion/WorkerFailure | Canonical tool call/result vocabulary and fenced terminal transactions | Mid-loop durable observations before continuation, not only terminal arrays |
| [Backend registry](../../internal/sessionlessharness/registry.go) | Sanitized errors and bound provider evidence | Error branch drops ToolEvents/Outputs/Summary; never rely on these to preserve a sent tool operation |

## Authority and policy binding

Proposed `AgentLoopPolicyV1` is immutable server-owned configuration with a digest, exact backend/provider/model revisions, input modalities/data class, instruction version, compaction policy digest, MCP and web-search grant digests, finite limits and expiry. The model and queue message cannot select or modify it. It is admitted with the existing resource/placement/substrate authority, not discovered from the environment.

Construct digests in one acyclic order: independent tool/data policy and resource/profile identifiers → scoped MCPToolGrantV1 and WebSearchGrantV1 → AgentLoopPolicyV1 → HarnessBindingV2 → invocation authority. Neither grant may contain the enclosing agent-policy, harness-binding or invocation-authority digest. Execution capabilities and observations bind those completed digests later; they do not enter the earlier policy/grant construction.

Introduce one canonical successor `HarnessBindingV2` sealing this digest. Its explicit owned-agent profile requires a nonempty agent policy; other reviewed backend kinds explicitly exclude it. No V1 decoder fallback, zero-value owned profile or silent migration is allowed. Update all affected constructors, persisted dispatch/jobs, digest validation and conformance fixtures in a bounded writer-first, empty-backlog cutover. Existing delivered behavior is retained through the canonical successor, not a legacy alias. Do not invent a second owner/resource selector in ExecutionRequest.

Operation admission must reload current Run/Attempt/lease/fence, cancellation, resource generation/revocation, profile, policy, price and proxy/allocation freshness. The deadline is the minimum of all authority windows and the remaining attempt/call budget. A lease renewal, where needed, uses the existing lease port; progress never renews time or authority by itself. Failed freshness checks stop before the next effect.

The [current authority extension](owned-harness-current-authority.md) specifies
the #189 engineering draft for trusted current registration, explicit funding
and post-claim allocation/proxy observation. It supplies this existing
requirement, not a new MVP feature or runtime activation. Its independent review
and concrete persistence/integration evidence remain required.

The prepared invocation is consumed once at entry to the reviewed loop boundary. A new `ProviderLoopSessionV1` inside that trusted boundary materializes the invocation credential once, owns its bounded lifetime and releases it under independent cleanup. Each model operation is authorized separately within that session. This is a required versioned extension: do not call the existing one-shot BoundaryV1 repeatedly, retain its callback, or weaken its at-most-once materializer. Neither the model nor the MCP client receives provider credential bytes. The loop session has no endpoint/model override and cannot outlive the original invocation authority.

## Sequential execution

1. Exact-resolve the enabled backend and substrate, validate admitted context/artifacts/policy, claim the existing physical invocation and attest its allocation/proxy. Reject unsupported modalities before any provider call.
2. Build the current view from trusted instructions, the current task, an admitted derived summary where applicable and the original complete suffix. Perform compaction only at a safe boundary and within the same aggregate limits.
3. Atomically reserve one model operation and its worst-case call/token/cost budget, then send once through the trusted loop session. Persist the validated normalized response before any requested tool can be admitted.
4. A final answer proceeds to fenced output finalization. A complete structured call is validated against the frozen catalog/grant. Partial argument chunks cannot dispatch. The first profile permits one tool call per response; a multi-call response fails before any of its calls are sent.
5. For web search, obtain the exact durable query-consent decision within the same claim and remaining wall-time/authority windows; missing or denied consent stops without a search send. Atomically reserve the tool operation with its required consent receipt, recheck authorization immediately before send, dispatch once and persist the validated bounded outcome. Only a durable known outcome permits another model operation. Protocol/domain errors remain distinct; no physical retry or route fallback follows from an error.
6. Repeat within the finite budget. Finalize immutable output/artifact manifests, credential release and workspace/process cleanup. The existing canonical transaction alone commits terminal outcome. Missing cleanup evidence cannot become success.

Text streams are optional operational progress, not canonical partial assistant messages. A model length-stop, malformed call, duplicate call ID, missing result, unknown accepted operation or exhausted budget produces a bounded stop reason rather than a fabricated successful answer.

## Durable subordinate operations

Extend the existing Attempt effect store, rather than creating another session store or scheduler. Proposed `AttemptOperationV1` has one identity `(tenant_id, run_id, attempt_id, physical_invocation_claim_id, operation_sequence)` and kind `model | compaction | mcp_tool | web_search`. It seals the parent effect reservation, lease/fence, policy/profile digests, request digest, predecessor observation digest, scoped tool call ID where applicable, reserved limits and deadline. Request content lives in an authorized immutable blob, not public evidence.

The existing outer reservation still decides the sole physical claimant. Before network effects, an atomic store operation validates current authority, appends the reservation and debits the aggregate budget. It returns a process-local one-use operation capability, not a serializable permission. Reservation and observation are immutable; an observation records `completed | rejected_before_send | accepted_outcome_unknown`, bounded sanitized reason, response reference/digest and usage provenance. The effect reservation and subordinate rows share Attempt authority and retention/deletion rules. They do not create terminal, retry or quota authority.

A lost store response may be reconciled by exact reservation identity/digest; this can recover the append result but never repeats a network dispatch. A crash after reservation is conservatively unknown unless trusted transport evidence proves otherwise. A fresh physical delivery is reconcile-only under the existing rule. MVP does not resume model/tool execution after a worker restart, including when its native checkpoint claims completion. A later explicit Run is not a mechanism for replaying the unresolved operation.

Persist a validated model response before reserving its tool, and persist a validated tool result before model continuation. On persistence failure, stop; do not feed an undurable result back to the model. Stored content is revalidated and owner-authorized before use. Bounds include rows, bodies and result manifests, so durability is not an unbounded logging channel.

### Scoped operation-body storage

Use the existing `ports.BlobStore` through one concrete `operationbody.StoreV1`
adapter fixed to the complete original operation scope and admitted loop
limits. Constructor validation is DATA validation, not admission or a send
capability. The eventual trusted composer installs the same adapter for the
provider session and `AttemptOperationBodyStoreV1`; the model and tools receive
neither handle. Setting an unrelated residual custody must not silently replace
this reader. Canonical state and quota remain with the existing Attempt Store.

Bodies use the existing session/run object prefix followed by
`attempt-operations-v1/`, a domain-separated SHA256 of the full canonical scope,
and one opaque `opbody-v1.<bytes>.<sha256>` reference. Size is positive canonical
decimal; hash is lowercase raw SHA256, not the configured-codec request digest.
No URL, path, caller prefix, padded decimal or alternate reference is accepted.
Reject non-normalized scope-derived keys rather than cleaning them into another
identity. The scope digest binds owner, session, run, attempt, lease/fence,
physical claim, reservation, invocation, binding and policy, not just tenant.

Reserve physical write capacity before copying/writing: at most two bodies per
admitted maximum operation and the admitted aggregate body-byte ceiling.
Attempts consume this local bound without refunds, including failures; it is
not another durable budget or proof that a fresh adapter can resume execution.
One fail-fast gate covers synchronous I/O and is shared by wrapper copies.
Caller cancellation and the shorter of ten seconds and the policy network
duration bound each call. BlobStore/readers must cooperate with context and
synchronous Close; do not abandon goroutines to claim bounded completion.

Write one nonempty private copy with no adapter retry, verify the exact returned
tenant/key/size/hash, then independently read back bounded bytes and require
exact size/hash and successful Close before publishing the reference. Clear the
private write/readback copies. Any uncertainty in that write disables further
writes on the instance, without deleting a possibly accepted object. Reads may
still reconcile DATA: require exact original scope and canonical reference,
cap before Open, and recheck actual size/hash, context and Close on every read.
Return fresh caller-owned bytes only; no retained body/object map is needed.

The underlying BlobStore is not create-only/WORM. Content addressing plus
verification detects replacement; it does not prove physical immutability,
absence or erasure. This adapter adds no table, transcript, automatic Delete,
credential custody or residual-disposal receipt. The session owns active
lifetime/current-authority checks and joins its synchronous calls. Owning
retention/finalization and separately authenticated cleanup remain unchanged.
Requests are stored before reservation and normalized responses before complete
observation; an ambiguous PUT or failed verification stops, never replays the
provider. Real session/lineage and normal Web composition remain #190/#191/#218
work, not outcomes established by this adapter alone.

Failed backend exports are untrusted. The worker queries authenticated subordinate observations tied to the exact parent effect/claim; the error registry must not blindly forward arbitrary ToolEvents. Failed terminal projection can record a tool_call plus a tool_result whose explicit outcome is `accepted_outcome_unknown`, with no fabricated service response or success payload. This is a Sessionless observation of the effect, not a claim that the remote tool replied. Unresolved effects still prohibit continuation and compaction. The terminal reducer verifies exact operation coverage and suppresses duplicate canonical events.

## Input and exported facts

Reuse ExecutionRequest identity, immutable ContextSnapshot/ContextWindow, InputArtifacts and the sealed bindings. Runtime materialization and output upload are supervisor capabilities, not model-callable shell/file tools. Images/files are admitted only by the exact model/profile with bounded artifact references, digest, media type and size; otherwise return unsupported input before effect. Paths, arbitrary URLs and caller-supplied native resume IDs grant no access.

Normalized operational observations use a version, stable operation sequence, tool call correlation, policy/source digests and timestamps. ExecutionEvent checkpoints are locators for this evidence, not evidence of completion themselves. Canonical user/assistant/tool facts are projected once by the existing store; compaction is a derived context artifact, not a new human message or a rewritten transcript. No hidden chain-of-thought, credentials, private transport errors or workspace paths enter canonical/public progress.

Outputs retain the existing artifact-manifest pipeline. Usage separates observed from estimated/unknown per physical operation; aggregate successful responses do not erase failed or compaction spend. Unknown is not zero. Provider outcome, tool outcome, cancellation, cleanup and canonical terminal commit remain independent facts. Existing ProviderExecutionEvidenceV1 is not silently reinterpreted as a multi-call proof: add a reviewed loop observation linked to each operation and use the existing typed substrate evidence only for its established assertions.

### Provider-usage provenance and conservative continuation

A complete known model response and a measured bill are different facts. The
configured-inference codec reports provider-supplied token counts; it does not
independently measure them or the billed amount. Preserve these counts as
`provider_reported` in the existing `AttemptOperationUsageV1` observation, never
as `observed`. The reported arm requires at least one token metric and excludes
an amount; `observed` retains its existing partial measured metrics, and
`unknown` retains no numbers. No new table, transcript or accounting ledger is
introduced. The observation digest authenticates the provenance as well as the
numbers. Existing observation shapes and digests are unchanged.

Decode token components independently: a missing or malformed total/other
component must not erase a valid reported input/output count and hide a known
overrun. Preserve every valid nonnegative integer component with reported
provenance; omit invalid/absent components rather than infer zero. Retain a
reported total only when both components exist and their checked sum matches.
No valid component means unknown usage. A manually supplied projection claiming
an inconsistent total is invalid, not a source of authority.

For model and compaction operations only, usage may be unknown, partially
measured or provider-reported without making a complete response's outcome
unknown. Continuation still requires the exact authenticated, durable completed
predecessor with no error reason, validated bounded response/lineage and all
original current-authority, policy, price, context-fit and remaining-budget
checks. A missing response, accepted-outcome-unknown effect, service/protocol
error, absent persistence or observed/reported metric above its reservation
stops continuation. An overrun remains recordable evidence, not a reason to
discard the observation. Provider reports cannot supply missing admission,
token-fit or price evidence.

This continuation rule relies on the existing Store reserving the full proven
worst-case token/cost debit before each send. The complete debit remains in the
aggregate even when later usage is absent or smaller: no refund, top-up,
additional call or budget recalculation follows from a report. In particular,
unknown money does not become zero money. A route unable to prove a conservative
per-call bound remains ineligible before its first send. A provider report may
stop work conservatively but never grants authority or reduces a reservation.

Eligibility branches on the completed predecessor's kind, not the proposed
next operation: a known model response can admit a tool under its own exact
call/grant checks, while a completed tool/search predecessor retains its
existing measured-amount requirement before any continuation. This clarification
does not change external-service billing or grant rules.
Cleanup, retention/disposal and final success keep their separate evidence
requirements; permission to continue is not proof of settled billing or safe
physical deletion. In particular, the residual-disposal inventory's measured
billing requirement remains unchanged; newly representable provider reports do
not qualify that route or claim final export/cleanup readiness. Its integration
with conservative reservations remains explicit work for #191. The existing
fixture-only body/lineage parser remains
fixture-only until #190/#191 deliver their real normalized path. This change
alone does not complete either task or enable a profile.

Verification must cover absent/partial/reported metrics, each token/money
overrun, forged provenance/unknown numbers, known versus unknown/error outcomes,
immutable observation correlation, unchanged aggregate debits, unchanged tool
gates and independent cleanup/disposal semantics. Preserve existing one-shot
and fixture behavior without manufacturing measurements.

### Normalized successful model bodies

For the configured inference route, construct a versioned normalized body only
through the original `FrozenRequestV1` response decoder. Keep successful final
text or one complete validated call, the allowed reported model and truthful
unknown/provider-reported usage. Refusal, length, service/transport errors,
unsupported finishes and unverified models cannot become successful normalized
results. Compaction accepts only final text up to 16 KiB; this is not the derived
summary manifest or restore authority owned by #192.

The closed body binds the full configured-profile digest, **raw original request
SHA256**, operation kind and explicit effective codec limits. Those limits may
be narrower than the profile and are not recoverable from wire bytes alone.
Do not substitute the codec's domain-separated request digest for the raw hash.
Stored DATA is validated against the exact original request, preserving original
schema bytes/whitespace, message roles and complete history units, schema/name/
argument constraints and already used call IDs. Reject duplicate or unknown
fields, invalid Unicode/numbers, trailing data, unsupported depth/member counts
and noncanonical normalized encodings. A tool name is DATA, never a route or
grant. Do not retain raw provider metadata, hidden reasoning or headers here.

The normalized wrapper, including limits and escaping overhead, must fit the
operation's reserved `ResultBytes` and the 64 KiB normalized-body cap. Its slot
is distinct from the HTTP response ceiling. The same verified body adapter
writes it before authenticated complete observation. No unknown/report metric
becomes a measurement or zero-priced bill, and a known overrun remains recordable
while independently denying continuation.

For the actual local bootstrap, retain a private clone of the original Source
configuration as immutable decoding DATA. This is not a current-authority
projection, cannot open a connection and remains usable for historical decoding
after Source retirement. Successful local model observations revalidate original
request/body/profile/kind and exact usage both before record and authenticated
read; Blob I/O remains outside retryable SQL callbacks. Existing grant,
reservation/history joins and immutable observation CAS remain authoritative.
`completed + service_error` and unknown/error paths retain their existing honest
shape; they are not promoted to successful results.

This representation neither proves a real provider replied nor authorizes a
successor. The original trusted transport/session and authenticated observation
own that evidence. The existing fixture continuation/tool grammar stays
fixture-only until its real typed successor is implemented; normalized calls
alone cannot dispatch MCP/search. ProviderLoopSession, continuation, compaction,
final export and normal Web composition remain separate #190/#191/#192/#218
work. No real profile or new external operation is enabled by this contract.

### Local invocation construction and first-model composition

The scoped body adapter requires the genuine parent reservation digest. Do not
preconstruct a supposed parent, create a second Store or add a public body setter
to break constructor ordering. The local bootstrap installs one fixed, initially
closed body reader when no body/custody owner is supplied. Its sole start claims
ownership before effects, reserves the real parent on that same Store, joins
its original grant and persisted Final/source, and derives scope/limits there.
Only then create the adapter, perform the existing burn/preparation/publication
and consume the original prepared invocation once. Arm the fixed reader exactly
once with that same adapter. Preserve preinstalled body/residual-custody profiles;
the new start rejects them rather than silently replacing their owner.

Replay, ambiguous commit or partial failure does not permit a new start. A
losing start cannot close the winner. Keep canonical receipts on failure;
closing a local bridge is not canonical retirement or disposal evidence. The
returned private original entry exposes historical cloned DATA and its actual
body adapter, not a caller-constructible approval or fresh send permission.

The concrete local session factory owns its bootstrap, Store, bridge and keeper;
it borrows the original Source and DB without closing them. A shared fail-fast
gate serializes copied wrappers. Entry retains an invocation context that is
not canceled when the start method returns. Intersect Source, original caller,
allocation and operation lifetimes without renewing them. Close cancels before
joining I/O, under one original cleanup window, and reports uncertain joins.

The first-model composition freezes the configured request, persists it through
the original adapter, then performs real preflight/publication/reservation and
Store before-send authorization. Normalize and persist the reply, authenticate
with the original observation grant, and record/read back through the same
Store. Unknown/undurable outcomes do not become usable durable successes or
another send. Noauth accepts no credential lifecycle and performs zero credential
calls. The genuine one-materialization credential branch remains required.

Until the real typed continuation contract is implemented, reject sequence >1
before acquiring another socket, publication or reservation. This explicit
first-model increment is not the completed multi-operation ProviderLoopSession,
agent loop, compaction, tool grant, canonical finalizer or normal Web demo.
Full execution-source authenticity, all applicable real funding and executed
calibrated token fit remain normal-composer prerequisites; scripted verifiers
and installation declarations do not supply those proofs. Existing real serving
profiles stay disabled. No daemon, model, key or cloud action follows from
constructing or merging this component.

## Initial finite ceilings

These are proposed upper bounds for the first public-data profile, not measured capacity, defaults for all harnesses or permission for live calls. Admission may choose smaller values. Every limit is sealed and required; zero/missing is invalid. A concrete model/profile and substrate must fit these bounds and prove cost before enablement.

| Dimension | Proposed maximum per Attempt |
| --- | --- |
| Concurrent operations | 1; no tool batch or parallel model requests |
| Physical model calls | 8 total, including at most 2 compaction generations; no automatic retry |
| Physical tool dispatches | 4 across MCP and search, including at most 2 web searches; identical regenerated calls are denied, not retried |
| Model input tokens | 32,768 per call, 131,072 aggregate reserved input tokens |
| Model output tokens | 2,048 per call, 16,384 aggregate; summary consumes this budget too |
| Active wall time | 120 seconds or the shorter authority/cost window |
| Per network operation | 30 seconds or the remaining shorter window; no progress extension |
| Cleanup and observation | At most 30 seconds under separate bounded cleanup authority; cannot start work |
| Context / summary bytes | 1 MiB admitted view; 16 KiB summary; token fit also required |
| Tool arguments / result | 4 KiB / 64 KiB per call; whole structured value or explicit overflow stop |
| MCP protocol | 12 outbound messages including setup/cancel/teardown, one catalog page, 128 inbound frames and 256 KiB aggregate inbound bytes |
| Durable operations / normalized bodies | At most 12 operations, 1 MiB aggregate bodies; artifact manifests have existing tighter bounds |

Check token fit against the exact model window after instructions, schemas, media overhead and output reserve, using a calibrated tokenizer or a proven conservative estimator. Unknown fit fails before send. Reserving upper bounds before operations prevents a late usage report from overspending the admitted ceiling. Reconcile observed use without granting an automatic extra call.

Reuse AdmissionCostCeilingV1 currency, fresh price revision and substrate/provider/total microunit ceilings. Admission must include all model and compaction calls, active and cleanup time, delivery overhead and network/storage bounds. MCP and search service charges require separately reviewed price/terms evidence and allocation within the total ceiling; anonymous access is not proof of zero price. If current cost authority cannot represent this, require a versioned canonical extension before live enablement. Unknown external cost is not silently excluded. The above numeric caps do not substitute for monetary admission.

## Alternatives and MVP exclusions

| Choice | Disposition and reason |
| --- | --- |
| Go-owned sequential loop with derived summary | Adopt for owner decision; smallest explicit canonical operation boundary |
| Hosted/native agent transcript as authority | Reject; it would replace canonical Sessionless history/effects |
| Provider-native opaque compaction | Defer to a separately compatible exact profile, never a portable fallback |
| OpenAI Docs search via constrained MCP | Candidate from #179; useful public-document locator, subject to actual descriptor and live proof gates |
| Provider-neutral built-in web search | Required owner addition; Tavily Basic proposal from #94 comparison, sealed grant/price/egress and source-linked WebUI output; no native opaque search loop |
| Shell/browser, memory, subagents, skill marketplace | Defer; not required to demonstrate this minimum |
| Generic MCP discovery, OAuth/private sessions, resources/prompts/sampling | Defer; would expand capabilities and credential/data-flow scope |
| Automatic retries, model fallback and warm native resume | Reject in the first profile; unknown effects remain reconcile-only |

## Verification and release gates

Use [Go testing practices](../testing-best-practices.md), fake clocks/barriers and instrumented scripted providers/transports. Separate protocol, authority, lifecycle, task outcome and quality as in [#63 evaluation](../research/harness-neutral-evaluation.md). No fixed sleeps, model-shaped assertion alone or transcript claim proves a physical dispatch.

| Required fixture | Authoritative assertions |
| --- | --- |
| Structured loop | Observed model → tool → model counters, durable request/result order, final canonical tool pair and answer exactly once |
| Stream and schema faults | Partial/duplicate/malformed/multiple calls and denied args produce zero unauthorized tool sends |
| Persistence and crash | Fault before/after every reserve/send/observe/terminal barrier; no continuation on undurable results and zero replay on fresh delivery |
| Revocation and owner isolation | Cross-owner context/grant, changed generation/fence/policy and expired attestation denied before next effect |
| Compaction and restore | Two compactions, exact lineage/suffix, unchanged canonical bytes, protected constraints and no unknown-effect cut |
| Cancellation and bounds | Before-send denial, after-send unknown, late reply, endless progress, every budget dimension, independent cleanup |
| Result injection and export | Tool text cannot grant effects; safe public errors; no secrets/hidden reasoning; truthful usage and failed-backend observations |
| Web search and citations | Explicit query consent, exact source references, zero private-query/forbidden-domain sends, bounded billing/errors and durable source-linked WebUI output |
| Product outcome | Separately authorized real model/search and model/MCP continuations, durable canonical WebUI answer, two-owner isolation and exact profile/cleanup/cost evidence |

Deterministic tests establish contract behavior, not summary fidelity or useful real-model behavior. Live trials require explicit public inputs, exact artifacts/model/descriptor/policy, finite budgets and owner authority. Retain separate omission/hallucination, task success, citation usefulness, token reduction, latency and cost results. Safety failures cannot be averaged away by quality scores.

Keep the owned-agent profile disabled until accepted design, deterministic conformance, independent exact-head review/CI, measured substrate/egress proof #90, two-owner #92 and product activation #175 are proven. Disablement denies new work while exact cancellation/reconciliation remains available. Rollback never changes endpoint/model implicitly or replays an effect. #35/#13/#14 join both MVP paths; neither can disappear because the other is easier.

## Accepted engineering choices and implementation handoff

The independently reviewed engineering package accepts the sequential/no-replay minimum; public Docs MCP plus bounded web-search design/data boundary and initial search-provider candidate; derived-summary policy with explicit no-fit/unsupported outcomes; finite ceilings subject to exact-profile calibration; and required canonical authority/egress extensions. This is the agreed-MVP engineering handoff recorded in #176, not another human PR/design approval gate. Exact-profile calibration, credentials, terms, paid calls and cloud enablement retain their separate authority and proof requirements; no specific paid model or cloud profile is activated.

The exact-version review, CI and merge have completed. #176 now owns these real linked implementation children in dependency order:

1. [#189](https://gitcode.com/urandon/sessionless/issues/189): canonical policy/binding and subordinate operation persistence, budgets, typed consent/storage/receipt contracts and cutover.
2. [#190](https://gitcode.com/urandon/sessionless/issues/190): trusted multi-operation provider session, credential custody and aggregate evidence.
3. [#191](https://gitcode.com/urandon/sessionless/issues/191): sequential loop and normalized error/terminal projection.
4. [#192](https://gitcode.com/urandon/sessionless/issues/192): derived compaction storage/publication/restore and fidelity fixtures.
5. [#193](https://gitcode.com/urandon/sessionless/issues/193): frozen MCP client/grant and attested transport; [#194](https://gitcode.com/urandon/sessionless/issues/194): bounded search adapter/grant/credential/cost contracts and proxy transport; [#195](https://gitcode.com/urandon/sessionless/issues/195): exact-query consent WebUI/BFF/original-worker bridge; [#196](https://gitcode.com/urandon/sessionless/issues/196): source citations and WebUI projection. Consent is a search-activation prerequisite, not resumed execution authority. #194 tests #189 receipts with the isolated fixture store and remains disabled until #195.
6. [#197](https://gitcode.com/urandon/sessionless/issues/197): joined deterministic conformance and quality experiment preparation.
7. Exact-profile live/substrate/two-owner acceptance and WebUI activation through existing #90/#92/#175.

Registration is not completion: each actual child retains its acceptance checklist, this accepted version, #176, predecessor evidence, owning milestone, work-type/domain/priority labels and bounded tests/non-goals. Synchronize authoritative implementation receipts with #176 and release parents. Historical pending-decomposition/manual-signoff wording is superseded by !160 and the #176 handoff; both required MVP execution paths remain unchanged.
