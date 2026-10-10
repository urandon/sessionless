# Compute choice context evidence

Version: **0.1.3**, 2026-10-11.
Owner: [#142](https://gitcode.com/urandon/sessionless/issues/142).
Implementation epic: [#155](https://gitcode.com/urandon/sessionless/issues/155).
Source baseline: `f589fece3811b7a04eeacf5cbaff4ca4d5c1c978`.
Status: engineering contract with shared rendering, prepared source material and
final-record measurement implemented; canonical writer integration remains
partial, not a deployed source of eligibility.

Compute selection must account for the entire canonical context that an
invocation will consume. Persist bounded, writer-derived facts at every event
boundary and a current Session aggregate, then verify the actual material again
at consumption. A sequence number, snapshot manifest or newest-message hint
cannot establish compatible history. These facts prove material properties, not
resource-use permission, model token capacity, price or reserved execution.

This amends the context/source and query sections of the
[eligible compute choice contract 0.1.1](eligible-compute-choice.md), without
rewriting that frozen design. Its authorization, selector, idempotency,
receipt, current admission/effect checks, privacy, rate and default-off rollout
requirements remain mandatory. #142 and #155 remain open. Both attached and
managed MVP routes, including managed semantic compaction, constrained MCP and
web search, remain required by the [MVP delivery plan](../mvp-delivery-plan.md).
The owner-prioritized #155 product stream does not become a hidden MVP gate.

## Source findings and alternatives

| Current source | Consequence |
| --- | --- |
| `internal/domain/session.go` | LastEventSequence identifies a boundary, not payload provenance, modalities or compiled prompt size. |
| `internal/sessioncontext/codec.go` | EncodeRecord checks actual JSON bytes against the immutable payload hash/size. Snapshots are lossless zstd JSONL of every event, not semantic compaction. Raw payload size is not encoded-record size. |
| `internal/sessioncontext/builder.go`, `internal/ydbstore/sessions.go` | The snapshot manifest is persisted before the object. Metadata can exist while the object is missing; current snapshot selection cannot prove object availability. |
| `internal/sessioncontext/projection.go`, `internal/worker/context.go` | Existing canonical projection verifies scoped contiguous snapshot-plus-tail material and attachment bytes. Reuse it at consumption; its blob I/O/history enumeration cannot run in a bounded list transaction. |
| `internal/codexexec/driver.go` | The actual initial compiler accepts exact user envelope v1 with no attachments and assistant schema v1, and rejects tools/notices. Both history JSONL and rendered prompt have independent 1 MiB limits. A failed-run notice can make later reuse incompatible. |
| `internal/ydbstore/canonical_ingress.go`, `internal/ydbstore/canonical_finalization.go`, `internal/ydbstore/sessions.go` | All production event inserts converge on insertSessionEventTx, but that helper and the general append port currently receive metadata, not verified payload bytes. |

| Alternative | Decision |
| --- | --- |
| Scan blobs or invoke a harness during list | Reject: unbounded history/I/O, no same-transaction source proof, potential effects. |
| Classify only new text, DisplayText or upload hints | Reject: earlier private or incompatible material remains in the admitted prefix. |
| Asynchronously refreshed readiness boolean | Reject: cannot fence the exact prefix, renderer or writer generation. |
| Bounded writer facts plus immutable boundary aggregates | Adopt for initial lossless canonical history: exact append transactions own publication; old boundaries remain point-readable. |
| Verified compilation receipt for each prefix and profile | Defer: needs separately bounded profile cardinality, source CAS publication, maintenance authority and lifecycle. Not an implicit repair path. |

## Canonical facts and finite representation

Use one canonical SessionMaterialPrefixV1 shape for both a current row and an
immutable boundary row. Its encoded private record is at most 8 KiB, including
all identifiers and strings. It contains no transcript text, credentials,
endpoint, browser-visible authority or arbitrary profile map.

| Field group | Exact meaning and bound |
| --- | --- |
| Identity | Version 1, canonical tenant_id/session_id, through_sequence, last_event_id or absent only at genesis; scalar keys agree with the record. IDs use canonical domain validation and the total record ceiling. |
| Contract | Exact RecordSchemaV1 and the fixed `codex-text-v1` renderer contract identity, each at most 128 UTF-8 bytes. This is compiler identity, not approval of a backend artifact. |
| Coverage | Closed `complete` or `unknown`, with fixed reason bits for unverified legacy prefix, missing/invalid contribution, unsupported evidence version, non-round-tripping codec material or arithmetic saturation. Coverage cannot improve merely by appending valid text. |
| Boundary | A 64-lowercase-hex prefix digest and, for a verified last event, exact canonical encoded-record SHA-256. The chain is domain-separated and length-prefixed; timestamps are final canonical UTC values, not preparation timestamps. |
| Source measures | Five named event-kind counts; verified payload bytes, encoded JSONL bytes, maximum encoded record bytes, tool-envelope bytes, historical attachment count and referenced attachment bytes. Counts/bytes are distinct uint64 quantities capped at MaxInt64; no bytes-as-tokens field. |
| Renderer measures | Fixed codex compatible-record and incompatible-record counts, rendered-frame bytes and unknown-record count. No arbitrary caller-supplied renderer or extensible list in V1. |

Every addition is checked before arithmetic. Saturation keeps a capped scalar
and makes coverage unknown; it never wraps or manufactures a smaller context.
This does not relax existing canonical append/product limits or prevent ordinary
canonical history from recording a failure. Complete coverage requires named
kind counts to sum to ThroughSequence, every source contribution to be verified,
and every measure to be exact. Unknown legacy histories need not have known kind
counts; their partial totals cannot be used for a positive capability verdict.

New private tables have these exact keys:

| Table | Key and record |
| --- | --- |
| `session_material_current_v1` | `(tenant_id, session_id)`; latest SessionMaterialPrefixV1. |
| `session_material_boundaries_v1` | `(tenant_id, session_id, sequence)`; immutable SessionMaterialPrefixV1 plus the final event's bounded contribution, together at most 8 KiB. No per-profile fan-out. |
| `session_snapshot_material_v1` | `(tenant_id, session_id, snapshot_version)`; at most 8 KiB binding exact snapshot ref/hash/size, codec, ThroughSequence, last_event_id, boundary digest and verified decoded JSONL digest/size. |

Boundary records carry the complete aggregate, not just its digest: admission
of an earlier trigger must not substitute today's aggregate or scan later
events. Storage is one current row per Session, one boundary per canonical
event and one fact per canonical snapshot; the two histories are bounded by
their existing retention/creation limits. The worst-case additional record
storage is 8 KiB per event/snapshot plus 8 KiB per Session, excluding database
overhead. No cache refresh creates additional versions.

Genesis is SHA-256 of a fixed V1 genesis domain and canonical tenant/Session
identity, at sequence zero, with exact zero counts and renderer header accounted
separately. Only createSessionTx for a genuinely new empty Session initializes
it. An existing Session conflict/retry does not overwrite it. Missing facts for
older history are unknown, never manufactured genesis.

For each verified event, hash the actual EncodeRecord output after final
sequence, Run/origin and canonical timestamp assignment. Fold the prior prefix
digest, event sequence/identity, encoded-record digest and the canonical
contribution encoding under a V1 fold domain. EncodeRecord followed by decode
must preserve payload hash/size and exact semantic record; merely json.Valid is
insufficient because RawMessage encoding can change payload representation.
Non-round-tripping payloads contribute unknown, not normalized replacement
bytes under the old immutable hash.

For an unverified append, chain its exact final event metadata and a fixed
unknown-contribution marker instead. If the previous row is missing, use an
explicit unverified-legacy seed binding scope and prior sequence; mark coverage
unknown permanently for that prefix lineage. That digest is a conflict fence,
not proof of unread legacy bytes. General append cannot guess their kinds,
payloads, modalities or prompt metrics.

## Verified contribution and shared renderer

`sessionmaterial.PreparedV1` is a server-internal prepared material type
with private fields, no JSON serializer and a bounded constructor over actual
payload bytes and exact immutable reference. It clones inputs, verifies scope,
hash/size and the canonical payload schema, and has no caller-supplied
classification/compatible/approved flag. Byte-bearing proof is transient;
database records retain only bounded measures/digests. Release the private byte
copy after commit/failure with the existing sensitive-buffer discipline.

`PrepareV1` accepts the stable event identity before sequence/time allocation.
`SessionEvent.ValidateMaterialIdentity` reuses the canonical domain predicates
without manufacturing a provisional sequence or timestamp; ordinary final
`Validate` retains its existing error precedence and optional author/Run rules.
Preparation owns cloned payload bytes and optional identity pointers. Ordinary
value copies share one release lifetime; explicit `Clone` owns independent
bytes. `Release` is idempotent, clears owned storage and invalidates aliases.
Concurrent copying/release is synchronized; neither JSON nor formatting exposes
the private material.

V1 inspects at most 1 MiB of one payload, sufficient for the initial compiler's
independent full-history limit. An oversize or unrecognized contribution becomes
unknown for this projection, not a new canonical payload limit or a reason to
truncate input. New evidence versions must explicitly extend this ceiling for
other profiles. Validate attachment descriptors against the canonical authorized
artifact references; media/name hints alone cannot certify an image or safe
content. Any historical attachment rejects initial codex-text compatibility,
even where its bytes are separately verified. All material stays PRIVATE.

Identity strings total at most 8 KiB. At most 64 ordered attachment references
and 64 KiB of aggregate attachment metadata are inspected; names, media types
and keys each have a 4096-byte ceiling. The descriptor's name is at most 4072
bytes and must be a safe nonempty basename. Its canonical manifest name must
equal `attachment-%02d-<descriptor name>` at the same one-based position, with
identical media type and BlobRef. The constructor excludes the leading
`message.json` payload artifact, validates each reference's event namespace and
checks the referenced-size sum against MaxInt64. It does not read attachment
bodies or prove media type, content safety or authorization.

Recognized source envelopes are user version 1, assistant-message schema v1,
tool-event schema v1 and run-terminal-notice schema v1. Unknown fields, trailing
data, unsupported schemas, hash/size/scope mismatch or exceeded inspection bounds
produce an unverified result. A recognized tool, notice or attachment-bearing
source can have known source measures while remaining incompatible with the
initial text renderer; these cases are not successful empty text contributions.

The current compiler's exact event parsing/role-frame rendering now resides in
pure `internal/canonicalprompt`, importing domain but not ports, worker or
sessioncontext. Both codexexec and the prepared constructor use that actual
helper. The helper preserves current
strict unknown-field/trailing-data behavior, user
version 1/no attachments, assistant schema v1, nonempty valid UTF-8/no NUL,
and rejection of all other event kinds. It must not silently tighten accepted
payload semantics or skip an incompatible record. The driver still verifies
full decoded history and exact trigger before rendering.

The shared renderer alone supplies neither persisted prefix facts nor positive
eligibility. Its 2 MiB helper-input ceiling
allows frame-limit testing independently of the driver's 1 MiB full-JSONL
ceiling; it does not enlarge the driver limit or the constructor's 1 MiB
payload-inspection ceiling. Individual frames are bounded by 1 MiB including
their role framing, and the driver separately bounds the header plus all frames.
Literal golden tests preserve Go JSON's case-folded fields, duplicate/null
behavior and invalid-string replacement, as well as Unicode and whitespace.
Canonical hash/size and record validation remain separate consumer predicates.
Driver tests reach the exact JSONL ceiling using legal record whitespace while
the resulting prompt stays small; one more source byte fails before execution.
The pure frame test reaches its own ceiling, where adding the header exceeds the
full-prompt ceiling. This is not evidence that an oversize rendered prompt is
reachable through the stricter current full-JSONL input limit.

`sessioncontext.MeasureRecordV1` now measures one final record through the
actual `EncodeRecord` and the same private per-record decoder used by
`DecodeJSONL`. It preserves sequence N greater than one; full-history decoding
still separately requires contiguous sequence 1 through N. The constructor
inspects at most 1,048,576 payload bytes and 8,192 total raw bytes across the
event's identity, kind, idempotency, payload-reference and optional author/Run
strings before hashing, parsing or encoding. Actual encoded JSONL, including
the newline, is independently capped at 1,056,768 bytes. These are projection
inspection ceilings, not new canonical-history write limits or full-context
capacity. Unknown returns a content-free error and an unverified zero value,
never a successful zero measure.

The opaque result retains only cloned final metadata, exact encoded byte count
and SHA-256. It retains no transcript bytes, rejects JSON serialization and
formats without private identities. Matching requires semantic metadata and
the codec's exact timestamp representation: a location name alone may differ,
but changing the encoded offset cannot reuse the old record digest. Neither
measurement nor decoding silently rewrites timestamps to UTC. Payload JSON
whose whitespace, HTML escaping or other representation changes under the
codec fails hash-preserving round-trip and remains unknown. Measurable tool,
notice or attachment-bearing source is not automatically renderer-compatible.

`sessioncontext.MeasurePreparedRecordV1` binds a live preparation to the actual
validated final event, comparing every stable identity field while leaving the
allocated sequence/time unchanged. It copies bytes atomically, measures through
`MeasureRecordV1` and clears that private copy on every return. Its opaque result
retains only cloned final metadata and bounded measures/digests, not the payload
or preparation handle. Released, zero, mismatched or non-round-tripping material
cannot supply verified final measurements. No metadata-only caller can forge a
verified result from public scalar measures.

This implements preparation and final-record measurement only. Writer
integration, atomic folds, prefix/boundary persistence and native verification
remain required; no current positive eligibility uses these helpers.

`sessionmaterial` may depend on that pure helper and domain; ports may reference
the opaque prepared type, but must not import sessioncontext, which already
uses ports. YDB's fold uses sessioncontext.EncodeRecord with final metadata and
the constructor's verified bounded bytes. Snapshot verification uses the same
fold. No alternate JSONL format, approximate compiler or package cycle is
introduced.

For a complete compatible prefix, codex rendered bytes are exactly
`len("Sessionless canonical transcript v1\n") + sum(actual role-frame bytes)`.
Check both this total and encoded whole-history JSONL against 1 MiB, separately.
Tool calls/results, notices, unknown payloads or attachments make the initial
profile incompatible/unknown rather than disappearing from the prompt.
Assistant ArtifactManifestID remains part of verified source identity even
though this compiler renders only Summary; artifact content is not implicitly
injected or declared harmless if another profile actually consumes it.

Token capacity is a separate necessary predicate. Enablement requires reviewed
measurement or a demonstrably conservative bound tied to the exact model,
tokenizer/version, backend artifact and all actual provider/CLI framing,
instructions and reserved output. Aggregate UTF-8 bytes, MaxContextTokens and
observed usage are not such proof. Until that evidence exists, token readiness
is unknown and positive eligibility denies. A non-additive tokenizer cannot be
approximated by summing per-event token counts without a verified bound.

## Atomic canonical writers

Prepared material remains server-owned and absent from public HTTP schemas.
No object I/O or provider execution occurs inside the database transaction.
Preparing a contribution is not permission for a later commit.

| Writer | Required integration |
| --- | --- |
| `sessioningress/service.go` user envelope producers | Prepare from the actual marshalled envelope and authorized ordered artifact refs. Validate the returned BlobRef against those same bytes/key/scope before passing the sealed preparation through CanonicalUserEventCommit. |
| `worker/manager.go` completion and failure | Prepare every actual tool/assistant/notice envelope before finalization. Current putCanonicalEventDraft must verify returned blob metadata; display text is not evidence. |
| `attachedworkeroutput/materialize.go` | Reuse its verified put/read boundary; prepare the exact final canonical envelope, not candidate output or an artifact name. |
| `Store.AppendSessionEvent` | Add an explicit server-internal verified contribution path. Metadata-only calls atomically append unknown facts; they cannot assert a text-compatible prefix. |
| `insertSessionEventTx` and its callers | Fold against exact final event metadata in the existing transaction, insert the immutable boundary, and update current facts with the Session sequence. |

User event, prefix/boundary, Run/Attempt, artifact manifest, selection receipt,
outbox and idempotency fact commit together. Finalization folds every event in
its batch in order; terminal Run/finalization/projection and all facts are atomic.
An exact canonical replay returns the original result without another fold;
conflicting payload/contribution identity fails. Callback retry resets temporary
output, lost acknowledgement uses canonical exact-key recovery, and rollback
publishes neither speculative aggregate nor partial batch. Existing deletion
fences apply to every writer. No second event sequencer is introduced.

## List send admission and consumption

List reads the current aggregate once after current WRITE authorization and
rate checks, comparing its scope/sequence with the same-transaction Session.
For a nonzero prefix it also reads the exact immutable boundary at that sequence
and compares the complete common prefix shape, including last_event_id,
digest, coverage, contract identities and every measure. A matching sequence
alone is insufficient: divergent current/anchor facts return source failure.
For sequence zero, independently derive the exact genesis digest and zero
measures from canonical scope; no event-boundary row is expected. Missing
legacy/unknown coverage cannot pass as genesis or positive compatibility.
It evaluates the actual prior prefix and fixed manifest's renderer/token/private
policy. input_kind remains an advisory prospective capability hint: it cannot
claim an exact size, provenance or renderer result for an uncommitted message.
An eligible preview is still not authorization of arbitrary new bytes.

choice_revision additionally binds prefix digest, coverage/contract identities
and the exact renderer/model measurement evidence revision. It does not bind
read_at or incidental utilization. Fresh send reauthorizes and resolves exact
idempotency before mutable facts as required by 0.1.1. It matches the preview's
prior sequence/digest and the same independent boundary/genesis comparison,
folds actual new input, and checks resulting whole-prefix
modality/provenance/history/prompt and token limits before any dispatch commit.
The private receipt pins both prior and triggering boundary digests/sequences,
not only the latest-message classification. Changed prefix requires refresh;
an exact authorized duplicate returns the original committed receipt.

Admission reads the immutable trigger boundary, not the latest Session
aggregate: other messages may already have appended. It validates exact
Run/receipt/trigger identity and current compute/policy authority before normal
reservation/offer. Selecting a snapshot requires its exact verified material
fact and matching boundary at the snapshot's own ThroughSequence; missing facts
cannot certify that representation. Existing same-prefix replay fallback may
use canonical events instead, under unchanged hard bounds and exact trigger.
If neither representation can be proven, deny safely; never switch target,
broaden prefix, omit records or truncate to fit.

Before consuming material or starting a provider effect, existing
ProjectCanonical/attachment verification must establish actual snapshot-plus-
tail bytes, source scope, event chain, trigger and expected aggregate/digest.
The real renderer recomputes output with its pinned contract. A persisted
snapshot fact proves prior verification, not present physical availability:
missing/replaced/corrupt objects still reject or use the permitted same-prefix
representation fallback. Current lease/fence and effect-policy checks remain
separate and necessary.

Inventory every additional input in the checked profile: workspace/input
artifacts, resume checkpoint, skills/instructions/system framing, overlays,
allowed MCP server/tool configuration and tool/search results. Fixed immutable
instructions bind ref/hash and measured contribution in the manifest; absent
input uses an explicit none variant, not omitted authority. Dynamic or unknown
material denies until its separately reviewed source, scope, measurement and
consent contract exists. This projection does not authorize MCP/search effects,
host reads or egress; future operation policies and participant rights remain
independent. Compaction needs its accepted derived-source contract, not a
lossless snapshot renamed as compaction.

## Exact query budgets and failure precedence

The added tables use exact primary keys derived from authorized canonical
identities, never browser-supplied resource identities. Each current/boundary
reader selects the first 8193 bytes of record and this explicit scalar
projection: version and through_sequence as Uint64 (16 bytes), tenant_id,
session_id and last_event_id with 161-byte prefixes (483 bytes maximum), and
prefix_digest with a 65-byte prefix. Domain IDs allow 160 bytes; digests allow
64, so each extra byte detects oversize. Charge the actual record/string lengths
and fixed 16 bytes before parse. The maximum charged data payload of one read
is **8193 + 16 + 483 + 65 = 8757 bytes**, not 8193. This excludes transport/RU
overhead and does not claim a network or billing measurement.

Snapshot-material reads use the same scalar projection (boundary_digest in
place of prefix_digest), plus Uint64 snapshot_version (8 bytes): **8765 bytes**.
Verify every scalar against both the exact requested canonical key/boundary and
decoded record. Reject overlong/unknown/mismatched material before eligibility;
do not decode an oversized record or expose its contents. All variable bytes,
including the oversize detection prefix, count even when decoding will fail.
Every statement and attempt shares the existing counter and DB-clock semantics.

| Operation | Additional source work and resulting bound |
| --- | --- |
| List | One `session_material_current_v1` read and, for a nonzero prefix, one exact `session_material_boundaries_v1` read per Session, shared across candidates. Initial maximum becomes thirteen shared statements plus six per candidate: **37** for four, not 35. Genesis uses 36. No snapshot reads on list. |
| Fresh send | One current-fact read plus the same prior-boundary read for nonzero prefixes, and one boundary INSERT/current UPSERT pair inside the existing append transaction. No nested transaction or repeated per-candidate anchor read. Count the two writer statements with canonical ingress, not pretend they are list reads. |
| Admission | One trigger-boundary read. If using a snapshot, one snapshot-material read and one snapshot-boundary read, at most three extra point reads; no latest-fact read is needed for an older trigger. Count them with ALL inherited admission statements. |
| Snapshot fact publication | One exact boundary read and one immutable fact INSERT with same-record retry checks, plus existing Session/deletion fencing. Separate maintenance transaction, not a list side effect. |

List ceilings remain 64 statements per attempt/96 across retries, ordinary
8 KiB records, 768 KiB materialized bytes per attempt/1 MiB across retries,
three-second overall deadline and 16 KiB public response. The two new list reads
add at most **17,514 charged data bytes** per nonzero-prefix attempt, not counting
any inherited source material. A second attempt at the nominal 37 statements
is 74 total; a third cannot exceed 96. Genesis adds one 8757-byte read.
Send's 24 added-choice-statement retry-total ceiling is unchanged; its current
fact and prior-boundary reads are included. Admission's three extra reads add
at most **26,279 charged data bytes**. Canonical append's new writes and these
reads require explicit total-budget/query-plan tests before enablement, not an
assertion that their inherited transaction budgets automatically fit.

Missing legacy facts or valid unknown/unsupported coverage produces unknown
readiness; compatible new text does not repair it. Known incompatible history
uses the existing safe capability/policy outcome. Cross-scope/key disagreement,
malformed facts, current-row/Session sequence disagreement after cutover,
arithmetic/statement/byte/deadline failure return generic source failure and
503 with no partial choices. An intentionally saturated valid unknown record
is not numeric wrap or a malformed success. Existing disclosability,
consent/staleness/policy precedence, no-store/Vary:Cookie, authorization and
non-oracular public reasons remain unchanged. Private fact/digest details do not
appear in public labels, errors, cursors, metrics or support bundles.

## Snapshot publication retention and rollout

SnapshotBuilder verifies actual source blobs and derives the same boundary
fold while encoding. Positive snapshot facts require a complete source boundary
and parity with its recorded contribution versions. Unknown legacy/append
coverage may still produce an ordinary canonical snapshot under existing
bounds, but cannot acquire complete coverage merely through snapshot creation.
Retain existing manifest-before-object crash recovery;
only after the exact written object has been successfully verified may the
bounded immutable snapshot-material fact be published by CAS against its
canonical manifest/boundary in a deletion-fenced transaction. Failed publication
leaves a manifest/object without positive material proof. Exact bounded retry
may repair it, never list-time blob scans or a new automatic authority service.

All three tables belong to canonical Session retention and legal hold, not a
separate permission registry. Extend the inventory in
`internal/ydbstore/session_lifecycle.go`: delete current, boundary and snapshot
facts with the existing canonical Session history deletion transaction; count
their statements/work under the same hard bound and legal-hold check. Any
individual future snapshot pruning must delete its material fact atomically
with its manifest. No independent TTL erases facts while effects/source history
are retained. Staged objects retain existing bounded cleanup authority.

Deploy additive schema, then every canonical writer and reader, then a trusted
checked serving manifest/cutover identifying schema, contribution/codec/renderer
versions, writer generation and actual drained incompatible writer/backlog
proof. Old writers must be fenced before positive selection enablement; a
missing fact never enables fallback. Existing Sessions/snapshots remain unknown
unless separately authorized verified maintenance reads exact source bytes and
publishes against an unchanged prefix CAS. No backfill or migration apply is
authorized by this design. Rollback disables new selections without retargeting
already committed Runs or erasing their facts/receipts.

## Verification and remaining delivery

Follow [Go testing practices](../testing-best-practices.md) and test exact
source-derived invariants, not a process-local approved flag.

| Proof | Required cases |
| --- | --- |
| Constructor and renderer | Golden parity with the actual driver; hash/size/scope mismatch, strict field/trailing-data behavior, JSON representation round-trip, invalid text/NUL, role/header overhead, 1 MiB boundaries and checked arithmetic. |
| Whole history | Old attachments, tools, notices or unknown legacy material cannot become compatible after new plain text; PRIVATE remains PRIVATE; model tokens cannot be inferred from bytes/usage. |
| Native writers | Concurrent gap-free append, exact retry, lost ACK, conflicting material, rollback and multi-event terminal batch; all writer seams update facts atomically, including metadata-only append. |
| Selection and admission | Prefix change invalidates preview; actual new input exceeds hint/limits; earlier trigger after later append uses exact boundary; auth/revoke races and zero unauthorized reservation/effect. |
| Snapshot and consumer | Snapshot/tail and full replay yield identical facts; wrong boundary, swapped/missing object, manifest-before-object crash, exact bounded repair, incompatible snapshot history and actual renderer mismatch. |
| Added material | Checkpoint/workspace/instruction/tool/search refs cannot inject unmeasured or unconsented input; no late overlay or target fallback. |
| Native queries and lifecycle | Exact PK/query plans and all statement/byte/retry ceilings; scalar/record mismatch, hold/deletion races, complete fact inventory cleanup and old-writer cutover race. |

Documentation tests and independent review accept only this engineering
amendment. #142 still requires implemented checked serving evidence, all token/
cost/private-policy predicates, positive list producer, HTTP mounting,
selector/send/receipt/admission/effect integration, both real route proofs and
native lifecycle/cutover evidence, followed by exact-head CI and guarded merge.
#143 owns UI delivery; #144/#145 remain separate. No fixture, lossless snapshot,
source-fact row or documentation merge completes those outcomes or activates a
provider.
