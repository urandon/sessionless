# Derived context compaction for the owned harness

Version: 0.2.1 draft, 2026-10-10. Design scope: [#178](https://gitcode.com/urandon/sessionless/issues/178) and [#180](https://gitcode.com/urandon/sessionless/issues/180). Requires independent review and explicit design acceptance under the project protocol with [the minimum harness](owned-agent-harness-minimum.md). Compaction semantics remain unchanged from 0.1.1; package version includes the web-search consent correction. A shorter context is not permission to rewrite Session events or replay tools.

## Selected approach

Use one bounded summary generation over older complete units, then retain an exact original suffix. Trusted instructions/current policy and the latest user task remain outside lossy text. A summary is untrusted derived data with provenance, never canonical history, credentials or execution authority. On overflow that cannot be safely compacted, stop explicitly rather than silently delete a protected unit.

This consumes [accepted #178 research v0.3.1](https://gitcode.com/urandon/sessionless/issues/178#note_192990497). Pinned comparators: Codex `f5420174dafba153913a3e697f89002c338dfd7e`, OpenCode `3a31c4ea801915c0b050df4b3842997ea62b6e93`, Pi `6c4f360264397c59801f6da2bdac13e3b1fcbe91`. The receipt owns their exact source references and limitations. Adopt portable summary plus suffix; reject canonical mutation, arbitrary oldest-input deletion and hidden auxiliary retries. Defer split-turn multi-summary optimization and provider-native opaque compaction to separately accepted profiles.

## Source and continuity model

The existing [canonical context types](../../internal/domain/serverless_context.go) and [SessionContextWindow](../../internal/domain/session.go) define the immutable admitted source through the triggering event. They remain lossless. Extend the owned loop with a derived view; do not repurpose ContextWindow's canonical digest as a summary digest.

A source view contains that admitted canonical range plus validated durable completed operations in the current Attempt, in their causal order. These operational records are not additional canonical Session events. Their terminal projection must not appear twice in a later view. Each source reference has its kind, stable event/operation ID, ordering position and digest; no arithmetic equates an operation sequence with a Session sequence.

Build semantic units by correlation, not character or message count. One model response and all its declared tool calls/results remain together. The first profile admits at most one call per response; imported historical multi-call units must still validate every pair. Compute a cut only between complete units: if a candidate spans overlapping/interleaved correlations, move the cut before the entire connected unit. Orphan/duplicate/mismatched call IDs and malformed histories fail closed. A tool-free completed message may be its own unit.

Keep the latest user task, its attachments and all current task constraints protected. Trusted system policy and current grants are always rebuilt from authoritative configuration, never summarized. Any unresolved or accepted-outcome-unknown effect prohibits compaction and further model/tool dispatch in that Attempt, even if an observational terminal tool_result makes the displayed history syntactically paired. A summary cannot turn unknown into completed.

## Derived artifact contract

Proposed `DerivedContextCompactionV1` has the following required fields. Immutable manifests and payloads use tenant-scoped Object Storage; the existing canonical state adapter owns publication of their references. No worker-local file or native thread is a durable authority.

| Field group | Required content and validation |
| --- | --- |
| Identity | Version 1; tenant, owner, Session; producing Run/Attempt/physical claim and summary operation ID |
| Canonical coverage | Admitted context snapshot digest, trigger/through sequence and ordered summarized canonical event IDs with body digests |
| Operational coverage | Ordered summarized current-Attempt operation IDs/observation digests, separate from canonical IDs |
| Suffix boundary | First retained source ID/kind and exact ordered suffix reference digest; canonical through bound; no suffix text duplicated in the summary manifest |
| Summary | Immutable blob reference, content digest, UTF-8 byte count and token count/estimator provenance; complete structured summary schema version |
| Lineage | Optional parent artifact reference/digest and its covered source digest; no cycle or duplicate coverage; at most two generations per Attempt |
| Compatibility | Exact harness, provider/model, tokenizer, trusted instruction, summarizer prompt and compaction policy digests |
| Artifacts | Original event/artifact IDs, immutable content digest, media type and size; explicit omitted-content reason where admitted policy allows omission |
| Usage | Summary operation observation, reserved input/output/cost, observed or unknown use with units and provenance; failed generations remain accounted separately |

Summary schema separates task/constraints, established facts with source references, unresolved work, artifact references and omissions. Validate complete JSON with bounded arrays/text and known references. It excludes grants, credentials, hidden reasoning and claimed tool completion unsupported by source. Structural validation does not prove factual fidelity; the focused quality gate below is also required.

For a second compaction, load the validated parent and its source coverage. Combine its summary with newly summarized complete units, record the exact new coverage and parent digest, and retain the new original suffix once. Never treat the parent summary as a canonical source event or recursively add its bytes to event coverage. A parent chain is bounded by the policy; if its verified source/parent closure is unavailable, stop rather than silently produce a new lineage.

## Admission and generation

At each safe pre-model boundary, estimate the entire provider input including trusted instructions, current task, schemas, retained text, images/files and provider formatting, plus the required output reserve. Compare against the exact model window and the policy ceiling. Tokenizer/estimator compatibility is sealed; a heuristic without a conservative calibrated upper bound cannot authorize a call.

When this view does not fit, choose an older complete prefix while retaining the protected units. The summarizer input includes the previous validated summary where applicable, that prefix, the structured summary schema and the summarizer instructions. It must fit independently before sending; the design cannot rely on asking an already-overflowing model to shorten its input. If either the summarizer input or a protected unit cannot fit, return `context_no_fit`.

Use one generation operation, tools disabled, no native split-turn second pass, no automatic provider retry. Count it among the maximum eight model calls/two compactions, with the same token/byte/cost/deadline reservations as [the minimum policy](owned-agent-harness-minimum.md#initial-finite-ceilings). Proposed summary ceiling is 16 KiB and 2,048 output tokens, both required. Provider errors, incomplete/length-stopped output, missing source references or empty substantive summary fail; no partial summary is installed.

The first profile does not excerpt the latest user task or arbitrary tool JSON. Oversized units stop explicitly. Older file/image bodies may be represented by authorized immutable references only if the exact profile's accepted modality/omission policy allows that task; an image filename or media label never counts as seen image content. Unsupported required content returns an explicit stop. No general artifact-read tool or arbitrary URL fetch is added to solve overflow.

## Publication and recovery

1. Reserve the compaction operation under the current Attempt, generate once, validate the structured response and persist its complete operation observation.
2. Write immutable summary/manifest blobs, verify their digests and source closure, then atomically publish the derived reference under the same lease/fence and expected source-view digest. Unreferenced uploads cannot become the active context.
3. Re-read current authority/cancellation before publication. Reject changed source, model/policy, owner grant or fence. A cancelled, expired, failed or unknown operation cannot publish a new view.
4. Rebuild the resulting view from authoritative instructions/task, the published summary and original suffix. Verify complete-unit continuity, no duplicated coverage, exact digests and actual fit before another model operation.

Atomic publication concerns the derived pointer only; it never rewrites, truncates or retroactively adds human facts to Session history. Artifact upload failure leaves the previous valid pointer unchanged. Retention/deletion follows tenant/session data rules, including parent summaries and unreferenced upload cleanup; no immortal compaction cache is introduced.

A fresh process can reconstruct a view from canonical events, immutable artifacts and validated published manifests without a native transcript directory. It rechecks current authorization and compatible digests. This proves portability of context construction, not permission to resume an already claimed Attempt after a crash. That delivery remains reconcile-only. A newly admitted Run may reuse a valid derived summary only after verifying canonical coverage, compatible policy/model and permissions; current-Attempt operational references must already have exact canonical terminal projection, with an explicit verified mapping, otherwise reject reuse. No cross-Attempt checkpoint adoption is implicit.

An older view can remain active after a failed summary only if it is still valid and fits; otherwise stop. No fallback to a corrupt artifact, foreign tenant, unavailable parent or unverified native summary is allowed. In the first profile a failed generation ends the current Attempt; a future separately admitted attempt does not erase the earlier incurred/unknown usage.

## Verification and fidelity

Credential-free fixtures use fake clocks, scripted summaries and reserve/send/persist/publish barriers. Assert original canonical count, order, IDs and body bytes exactly unchanged in every case.

| Case | Required proof |
| --- | --- |
| Two compactions and fresh reconstruction | Parent/hash closure, each source covered once, original suffix once, deterministic rebuilt view |
| Pair cuts and malformed histories | Move cuts to complete connected units or deny; orphan/duplicate/interleaved IDs never split silently |
| Accepted-unknown effect | Zero summary/model/tool sends after ambiguity; observational result is not success |
| Oversized task/tool/media and overhead | Pre-send no-fit; no split JSON, identifier or protected task; modality omission is explicit |
| Same filename with different artifacts | Stable event/artifact IDs and digests remain distinct; no local-path identity |
| Summary error/empty/length-stop/cancel/budget | No publication, no hidden generation retry, all physical attempts accounted or unknown |
| Wrong owner/source/hash/parent/model/policy | No view installation or grant restoration; published pointer remains unchanged |
| Blob/store failure and fresh Run reuse | Atomic publication, verified terminal mapping and no operational/canonical duplication |
| Constraint/fact/task fidelity | Scripted expected facts and IDs preserved; a malicious summary cannot change tool authorization |

Separately authorized live evaluation under [#63](../research/harness-neutral-evaluation.md) must measure useful task completion after repeated compaction, constraint preservation, factual omissions/hallucinations and artifact/citation continuity. Also record token reduction, auxiliary spend and latency independently. Passing scripted fixtures or a smaller prompt does not establish live quality. A profile that loses mandatory task constraints is not enabled merely because its safety checks pass.

## Acceptance and non-goals

Owner acceptance must cover the portable schema/lineage, protected-task/no-excerpt rule, explicit no-fit and modality policy, finite summary budget and fresh-Run mapping. Concrete model/tokenizer calibration and live fidelity remain exact-profile gates, not facts supplied by this draft. General memory/search, provider-native opaque state and automated summarization retries are outside this minimum. #178 remains open through its design and implementation-test acceptance; research acceptance alone is insufficient.
