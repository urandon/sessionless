# Derived context compaction for the owned harness

Version: 0.2.2. Design scope: [#178](https://gitcode.com/urandon/sessionless/issues/178) and [#180](https://gitcode.com/urandon/sessionless/issues/180); implementation [#192](https://gitcode.com/urandon/sessionless/issues/192) remains incomplete. The accepted 0.2.1 baseline accompanied [the minimum harness](owned-agent-harness-minimum.md) at exact `42139dfb5cefb266adb77f221c0743b53cfb5217` in [!160](https://gitcode.com/urandon/sessionless/pull/160). This revision specifies protected source units, durable ownership and the runtime integration boundary. A shorter context never permits canonical-history mutation or physical-effect replay.

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

### Protected source units

The first implementation preserves every original system and user unit,
including older user messages and their admitted attachments. It does not ask
the summarizer to decide which user constraints remain important. Current
instructions, policy and grants still come from their original authoritative
sources, not a historical system message. Historical system text is retained
as historical material, never promoted into the current trusted instruction.

Cut the ordered source only between complete connected units. If any member of
a correlation component is protected, protect the entire component. Within the
older prefix, keep protected originals separate from the assistant/tool units
eligible for summarization. The manifest records both ordered reference sets
and the exact original suffix; the rebuilt view retains each protected unit's
role, identity, bytes and relative order once. A protected unit already in the
suffix is not also inserted from the prefix. Parent-summary bytes are never
counted as original coverage.

This profile must compact assistant/tool-heavy histories successfully twice.
If the protected originals, schemas, formatting and output reserve alone cannot
fit, return `context_no_fit`; do not truncate a user task or report that denying
all long histories implements compaction. Model-native tool transport remains
subject to complete call/result correlation after reconstruction.

### Durable head and terminal correspondence

Use one optional typed `DerivedContextHeadV1` in the existing Run JSON payload.
It binds the original producing Attempt/claim, generation, immutable manifest
reference/digest, source-view digest and completed compaction observation.
Generation is bounded to two per Attempt. No second current head is stored in
Attempt, a new table, a checkpoint or a worker cache.

The first profile also bounds the entire retained manifest closure to two
generations, including any reused predecessor from another Run. A fresh Run
does not reset this traversal bound. If another generation would exceed it,
rebuild from independently fitting canonical material or stop explicitly;
never discard an unverified parent or relabel its summary as a canonical event.

This choice avoids relying on the operational TTL of
[`attempts`](../../migrations/ydb/00009_create_attempts.sql) and
[`checkpoints`](../../migrations/ydb/00012_create_checkpoints.sql).
[`runs`](../../migrations/ydb/00007_create_runs.sql) already retains its JSON
payload under Session retention. An absent head means no derived view, not an
alternate decoder or permission to use a native checkpoint. A different Attempt
cannot adopt an unprojected head or resume its original execution.

Only the original Store's dedicated compaction publisher may replace this
field. Ordinary Run creation requires an absent head; every generic Run update
must preserve the exact retained head, including cancellation and terminal
writers. The publisher reads the current Run in its owned transaction, compares
the expected previous head and source digest, validates the new typed value and
serializes the complete preserved Run. It cannot trust a caller-populated Run
field, a Boolean approval or an unchecked `PutRun` UPSERT. The full Run payload
must still fit the existing 32 KiB owned-authority read bound.

Canonical finalization adds a bounded terminal correspondence to this same
Run-owned metadata, atomically with the canonical append. For every operational
source covered by the derived view, it binds the exact original reservation and
observation digests to the emitted canonical event IDs and content digests.
It also retains the inputs needed to recompute the terminal replay digest.
The existing [`run_finalizations`](../../migrations/ydb/00059_create_run_finalizations.sql)
content digest must seal this metadata and the derived manifest, not only the
visible event drafts. Do not infer correspondence from a call ID, ordinal,
operation sequence or matching text. Compaction-generation metadata itself
does not become another canonical assistant message.

A fresh Run selects a predecessor only through terminal events already inside
its newly admitted canonical window. It verifies the retained successful Run,
terminal replay digest, exact correspondence and immutable manifest closure,
then rechecks current permissions/model/policy and reconstructs from canonical
material. It does not require an expired Attempt/lease to become active again.
An incomplete or unprojected correspondence denies reuse; the original claim
and any accepted-unknown effects remain reconcile-only.

### Publication authority and object budgets

Persist the generation's normalized response and its authenticated completed
operation observation before publishing a derived head. Copy the validated
structured summary into a typed retained artifact, then write its immutable
manifest. Read back both objects and verify their digests before entering the
publication transaction. Their fixed `derived-context-v1/` namespace is under
the producing Session/Run prefix, distinct from erasable attempt-operation
bodies. The summary artifact seals its validated schema, source references,
original reservation/observation and normalized-response digests. Neither a
raw blob reference nor the copy alone proves successful transfer of custody.
The transaction independently
rechecks current source/profile/grants, cancellation, lease/fence, original
allocation, successful operation, expected head and source/parent closure.
Recording an operation outcome alone is not this publication check.

Extend the original scoped body adapter with a dedicated retained-artifact
write, sharing its original I/O gate and aggregate byte budget. It admits only
one summary artifact of at most 32 KiB including metadata and one manifest of
at most 128 KiB per original compaction operation: at most four attempted
retained-object writes per Attempt. These allowances are one-use and
nonrefundable across adapter/session copies. Reserve their encoded upper
bounds before generation and charge every attempted physical write, including
the summary copy, to the same aggregate allowance. Ordinary request/response
writes keep their existing `2 * MaxOperations` cap; retained writes are not a
generic extra-write pool or a reason to recreate the adapter. Summary content
still has the independent 16 KiB/2,048-token ceiling. Metadata or aggregate
overflow stops explicitly, without another generation or a smaller unreviewed
manifest.

Retain the terminal correspondence inline only while the complete Run remains
within 32 KiB; overflow fails finalization rather than dropping mappings.
Before retiring original operation bodies, the existing output-custody path
must verify the complete retained dependency closure and its terminal mapping.
Extend that typed custody evidence where necessary; do not claim the existing
format already covers compaction. A missing or uncertain transfer cannot
authorize retirement or fresh-Run reuse. The original operation residual
inventory still includes every original request/response; namespace-close
success still means actual deletion and verified absence. A live summary must
never depend on a response object this close will erase.

Fresh-Run reuse verifies the retained structured summary, its sealed generation
digests, canonical correspondence and terminal replay digest; it does not
require an already erased original request/response. Any source dependency not
reconstructable from those retained facts prevents transfer and reuse. Keep
each producing Run's at most two summary/manifest pairs until Session deletion;
replacing its head or borrowing it from a later Run does not release them.
This avoids a new reference-count table or cross-Run garbage collector.
Session deletion enumerates owning Runs and their bounded retained pairs and
parent references through the existing paged deletion workflow. Unpublished
uploads remain owned cleanup residues. Do not increase Attempt TTL or create
an immortal cache to avoid implementing cleanup.

### Runtime integration

The pre-model consumer derives source from the same Store's admitted WorkerJob,
canonical context window, verified material and authenticated durable operation
history. A caller-created `RequestV1`, restored checkpoint or detached source-ID
list cannot authorize compaction. Keep the lossless
[`sessioncontext`](../../internal/sessioncontext/projection.go) codec unchanged.

The original local session, operation reservation/issuer, body adapter and
allocation lease own the single compaction generation. Replace sequence-one
guards only when this complete source/publication/next-request chain is joined;
do not merely admit a `compaction` kind through the existing provider path.
The actual next frozen model request must consume the published head and its
verified original suffix. No parallel runner or caller approval callback is
introduced.

Both generation-input and rebuilt-input admission must execute a concrete
conservative fitter bound to the installed model/template/tokenizer calibration.
An estimator name or calibration digest by itself is not a fit result. The
configured local-inference qualification owns that concrete profile evidence;
synthetic calibration fixtures prove software wiring, not real-model fit or
quality. Missing fit capability produces a pre-send denial.

1. Reserve the compaction operation under the current Attempt, generate once, validate the structured response and persist its complete operation observation.
2. Copy the validated structured summary to its retained artifact and write its manifest; read back both and verify their digests and source closure. Atomically publish the derived reference under the same lease/fence and expected source-view digest. Unreferenced uploads cannot become the active context; retiring original operation bodies additionally requires verified terminal custody of the retained closure.
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
| Retained summary custody and cleanup | Verified copy before original-body disposal; original namespace fully absent after close; fresh Run works without erased bodies; bounded retained closure removed with Session |
| Constraint/fact/task fidelity | Scripted expected facts and IDs preserved; a malicious summary cannot change tool authorization |

Separately authorized live evaluation under [#63](../research/harness-neutral-evaluation.md) must measure useful task completion after repeated compaction, constraint preservation, factual omissions/hallucinations and artifact/citation continuity. Also record token reduction, auxiliary spend and latency independently. Passing scripted fixtures or a smaller prompt does not establish live quality. A profile that loses mandatory task constraints is not enabled merely because its safety checks pass.

## Acceptance and non-goals

Engineering acceptance under the agreed MVP delegation covers the portable schema/lineage, protected-task/no-excerpt rule, explicit no-fit and modality policy, finite summary budget and fresh-Run mapping, supported by research and independent review. Concrete model/tokenizer calibration and live fidelity remain exact-profile gates. General memory/search, provider-native opaque state and automated summarization retries are outside this minimum. #178 remains open through its design and implementation-test acceptance; research acceptance alone is insufficient.
