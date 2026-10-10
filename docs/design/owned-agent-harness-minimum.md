# Sessionless owned agent harness minimum

Version: 0.2.0 draft, 2026-10-10. Design owner: [#180](https://gitcode.com/urandon/sessionless/issues/180); implementation parent: [#176](https://gitcode.com/urandon/sessionless/issues/176). Independent review and explicit product-owner acceptance are required before implementation decomposition. This draft does not enable a provider, MCP connection or cloud profile.

## Decision and scope

The managed MVP must execute an actual bounded agent loop: authorized input, a model-generated structured tool call, a useful MCP result, model continuation and a durable answer visible in WebUI. It must also compact a derived context view without rewriting canonical history. A single tool-free request is foundation evidence, not this outcome. The independently required attached-worker path remains [#72](https://gitcode.com/urandon/sessionless/issues/72) with [#129](https://gitcode.com/urandon/sessionless/issues/129) and the selected activation candidate [#133](https://gitcode.com/urandon/sessionless/issues/133).

The owner's PR !160 request also requires bounded general web search, not only official-document location. Add a trusted search adapter with durable source-linked results and WebUI citations. Full-page fetch/find/browser and private queries remain outside this slice; constrained MCP remains independently required.

Choose a Go-owned sequential loop inside the existing isolated managed runtime. Sessionless owns Session, Run, Attempt, lease/fence, admission, effects, quota and terminal commit. Native harness threads, local transcripts, checkpoints and model call IDs never replace these authorities. The exact compaction and tool contracts are [derived compaction](owned-harness-compaction.md), [constrained MCP](owned-harness-tools-mcp.md) and [bounded web search](owned-harness-web-search.md), package version 0.2.0.

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

The prepared invocation is consumed once at entry to the reviewed loop boundary. A new `ProviderLoopSessionV1` inside that trusted boundary materializes the invocation credential once, owns its bounded lifetime and releases it under independent cleanup. Each model operation is authorized separately within that session. This is a required versioned extension: do not call the existing one-shot BoundaryV1 repeatedly, retain its callback, or weaken its at-most-once materializer. Neither the model nor the MCP client receives provider credential bytes. The loop session has no endpoint/model override and cannot outlive the original invocation authority.

## Sequential execution

1. Exact-resolve the enabled backend and substrate, validate admitted context/artifacts/policy, claim the existing physical invocation and attest its allocation/proxy. Reject unsupported modalities before any provider call.
2. Build the current view from trusted instructions, the current task, an admitted derived summary where applicable and the original complete suffix. Perform compaction only at a safe boundary and within the same aggregate limits.
3. Atomically reserve one model operation and its worst-case call/token/cost budget, then send once through the trusted loop session. Persist the validated normalized response before any requested tool can be admitted.
4. A final answer proceeds to fenced output finalization. A complete structured call is validated against the frozen catalog/grant. Partial argument chunks cannot dispatch. The first profile permits one tool call per response; a multi-call response fails before any of its calls are sent.
5. Atomically reserve the tool operation, recheck authorization immediately before send, dispatch once and persist the validated bounded outcome. Only a durable known outcome permits another model operation. Protocol/domain errors remain distinct; no physical retry or route fallback follows from an error.
6. Repeat within the finite budget. Finalize immutable output/artifact manifests, credential release and workspace/process cleanup. The existing canonical transaction alone commits terminal outcome. Missing cleanup evidence cannot become success.

Text streams are optional operational progress, not canonical partial assistant messages. A model length-stop, malformed call, duplicate call ID, missing result, unknown accepted operation or exhausted budget produces a bounded stop reason rather than a fabricated successful answer.

## Durable subordinate operations

Extend the existing Attempt effect store, rather than creating another session store or scheduler. Proposed `AttemptOperationV1` has one identity `(tenant_id, run_id, attempt_id, physical_invocation_claim_id, operation_sequence)` and kind `model | compaction | mcp_tool | web_search`. It seals the parent effect reservation, lease/fence, policy/profile digests, request digest, predecessor observation digest, scoped tool call ID where applicable, reserved limits and deadline. Request content lives in an authorized immutable blob, not public evidence.

The existing outer reservation still decides the sole physical claimant. Before network effects, an atomic store operation validates current authority, appends the reservation and debits the aggregate budget. It returns a process-local one-use operation capability, not a serializable permission. Reservation and observation are immutable; an observation records `completed | rejected_before_send | accepted_outcome_unknown`, bounded sanitized reason, response reference/digest and usage provenance. The effect reservation and subordinate rows share Attempt authority and retention/deletion rules. They do not create terminal, retry or quota authority.

A lost store response may be reconciled by exact reservation identity/digest; this can recover the append result but never repeats a network dispatch. A crash after reservation is conservatively unknown unless trusted transport evidence proves otherwise. A fresh physical delivery is reconcile-only under the existing rule. MVP does not resume model/tool execution after a worker restart, including when its native checkpoint claims completion. A later explicit Run is not a mechanism for replaying the unresolved operation.

Persist a validated model response before reserving its tool, and persist a validated tool result before model continuation. On persistence failure, stop; do not feed an undurable result back to the model. Stored content is revalidated and owner-authorized before use. Bounds include rows, bodies and result manifests, so durability is not an unbounded logging channel.

Failed backend exports are untrusted. The worker queries authenticated subordinate observations tied to the exact parent effect/claim; the error registry must not blindly forward arbitrary ToolEvents. Failed terminal projection can record a tool_call plus a tool_result whose explicit outcome is `accepted_outcome_unknown`, with no fabricated service response or success payload. This is a Sessionless observation of the effect, not a claim that the remote tool replied. Unresolved effects still prohibit continuation and compaction. The terminal reducer verifies exact operation coverage and suppresses duplicate canonical events.

## Input and exported facts

Reuse ExecutionRequest identity, immutable ContextSnapshot/ContextWindow, InputArtifacts and the sealed bindings. Runtime materialization and output upload are supervisor capabilities, not model-callable shell/file tools. Images/files are admitted only by the exact model/profile with bounded artifact references, digest, media type and size; otherwise return unsupported input before effect. Paths, arbitrary URLs and caller-supplied native resume IDs grant no access.

Normalized operational observations use a version, stable operation sequence, tool call correlation, policy/source digests and timestamps. ExecutionEvent checkpoints are locators for this evidence, not evidence of completion themselves. Canonical user/assistant/tool facts are projected once by the existing store; compaction is a derived context artifact, not a new human message or a rewritten transcript. No hidden chain-of-thought, credentials, private transport errors or workspace paths enter canonical/public progress.

Outputs retain the existing artifact-manifest pipeline. Usage separates observed from estimated/unknown per physical operation; aggregate successful responses do not erase failed or compaction spend. Unknown is not zero. Provider outcome, tool outcome, cancellation, cleanup and canonical terminal commit remain independent facts. Existing ProviderExecutionEvidenceV1 is not silently reinterpreted as a multi-call proof: add a reviewed loop observation linked to each operation and use the existing typed substrate evidence only for its established assertions.

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

## Design acceptance and implementation handoff

Owner decisions requested: accept the sequential/no-replay minimum; public Docs MCP plus bounded web-search design/data boundary and proposed initial search provider; derived-summary policy with explicit no-fit/unsupported outcomes; proposed finite ceilings subject to exact-profile calibration; and required canonical authority/egress extensions. The owner's search scope request is not acceptance of this exact design or paid/credentialed enablement. No specific paid model or cloud profile is selected by this draft.

After independent review and explicit acceptance of an exact version, reuse #176 and create real linked implementation children in this order:

1. Canonical policy/binding and subordinate operation persistence, budgets and cutover.
2. Trusted multi-operation provider session, credential custody and aggregate evidence.
3. Sequential loop and normalized error/terminal projection.
4. Derived compaction storage/publication/restore and fidelity fixtures.
5. Frozen MCP client/grant, bounded search adapter/grant/credential/cost contracts and extended attested proxy transport; source citation manifest and WebUI projection.
6. Joined deterministic conformance and quality experiment preparation.
7. Exact-profile live/substrate/two-owner acceptance and WebUI activation through existing #90/#92/#175.

These are decomposition outcomes, not created issue IDs or completed checklist rows. Each child must name this accepted version, #176, predecessor evidence, owning milestone, work-type/domain/priority labels and its bounded tests/non-goals. Sync #178/#179/#180 and release parents; correct and re-review superseded optional-track wording in PR !159 before merge. Until acceptance, #176 remains pending implementation decomposition.
