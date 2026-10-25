# Single-answer inline receipt persistence

Version: 0.1.0. Status: engineering design accepted under agreed-MVP delegation
after independent original-requirements-first review. Implementation, aggregate
memory proof and activation remain separate and incomplete.
Implementation owner: [#200](https://gitcode.com/urandon/sessionless/issues/200).
Parent contract: [attached media and artifacts](attached-harness-media-and-artifacts.md),
inline V2 under #198/#72. Activation remains under #133/#220; this document
does not change MVP scope or authorize live operations.

## Problem and decision

The private SQL receipt currently serializes both the original candidate
Summary and its derived canonical event DisplayText. With maximum escaping,
each is about 192,000 bytes. The actual YDB Table SDK lends the JSON parameter
to the protobuf request; gRPC then copies it into its encoded request while
the original JSON and canonical blob plan remain live. Removing Marshal's
returned copy alone does not eliminate this overlapping pair.

For the admitted two-artifact cohort, a conservative server-only lower bound
is 2 x 390,120 JSON bytes + 192,000 planned answer bytes + 65,536 artifact
bytes + 32,768 Summary bytes = 1,070,544 bytes. This exceeds the unchanged
1 MiB owned-payload bound before allocator capacity and protocol overhead.
This is source-derived simultaneous ownership, not a heap measurement.

Use one private, discriminated SQL encoding that stores the original untrimmed
answer once. Derive only the redundant DisplayText, whose exact relation
`DisplayText == strings.TrimSpace(Candidate.Summary)` is already required by
`validateInlineCanonicalPlanV2`. Do not truncate answers, reduce semantic
maxima, enlarge the memory limit or introduce another table/custody lifecycle.

## One closed stored shape

The existing version-2 receipt fields and stage remain. Add the required
root string `persistence_encoding: "single-answer-v1"` to the private SQL
representation only. The in-memory/domain receipt and public wire do not gain
this field.

Store Candidate.Summary unmodified, including leading/trailing whitespace.
For canonical_copying/ready, the single canonical event's `display_text` key
is **absent**, not an empty-string/null placeholder. This follows the existing
domain `omitempty` representation after projection. Other event fields and
all Completion/Usage/Substrate data retain their original meaning and shape.
For prepared/body_claimed/retired_without_writes, Materialization.Completion
and Materialization.Failure remain explicit null, with an empty EvidenceDigest,
as required by the original typed canonical shape. No fictional event/manifest
is introduced.

Accept exactly this discriminator and representation. Reject missing, null,
wrong or unknown encoding, any stored display_text key, redundant old V2 shape,
aliases/duplicates/unknown fields and unsupported versions. Do not accept both
absence and an empty placeholder. V1 stays on its unchanged existing reader;
malformed V2 never falls back to V1.

### Private framing, not a smaller metadata budget

The discriminator member including its comma adds exactly 42 JSON bytes.
For a short answer `"x"`, omission saves only 19 bytes, so the new frame can
be 23 bytes larger. Do not assume that every valid record becomes smaller.
Keep the old writer's 512 KiB limit on the full-domain canonical JSON,
checked by an exact streaming byte count rather than a retained full copy.
Allow exactly 42 additional bytes for the private stored frame: its encoded
and raw lexical ceiling is `512 KiB + 42`, not a reduced metadata/Usage cap.
Bound the raw frame before projection/copy. The projection cannot exceed the
old full-domain writer size plus that framing allowance.

Readers retain their existing domain/identity/digest checks and get no new
Usage count, decoded-metadata or canonical-reencoding ceiling: existing raw
JSON can use harmless whitespace/escaping different from Marshal's spelling.
This private framing allowance changes neither the 1 MiB aggregate bound nor
any public/body/semantic maximum. An identity-valid short-answer/large-Usage
fixture at the old boundary must round-trip; frame overflow must deny before
the codec allocates its metadata copy. Full phase capacity remains separately
required, including this fixed framing overhead.

## Encode and decode invariants

Before encoding, validate the complete original typed receipt, including
stage, identity, fingerprints, canonical materialization digest and exact
derived-display relation. Projection must not hide a mismatch. Clone only
the event container needed for omission; do not mutate the caller's receipt
or duplicate its answer/artifact contents.

Decode bounded original bytes using the existing strict lexical limits and
closed metadata guard. Extract the single answer without building a
payload-sized generic value tree. Reject the forbidden display_text member
before it can create another large decoded string. Restore the original
answer and, only for a valid canonical-plan stage, derive the single event's
DisplayText. Then perform the original complete identity/digest validation;
outer readers still verify actual tenant/owner/attempt and denormalized columns.
Encoding/discrimination is data, never authorization.

Keep original untrimmed answer fingerprints and canonical event/blob digests.
Hash the same existing full-domain/public JSON bytes, not the compact SQL
representation. If fingerprint allocation is optimized, stream those exact
old bytes after the unchanged hash prefix; retain golden/differential vectors.

## Consumer and compatibility boundary

Use the codec for every V2 SQL write and shared envelope read. Audit normal
load, reconciliation, pending/custody inventory, finalizer, deletion/capture
and retirement consumers. They continue receiving the original reconstructed
typed receipt and apply their existing fences. No decoder or projection can
create READY, clear copying custody or authorize another send.
Deletion capture also validates scalar cells against the original payload;
its V2 branch must consume the closed codec result instead of applying the
old full-domain JSON guard to compact raw bytes. Preserve all original
cells/body, run/identity and custody-inventory comparisons; V1 is unchanged.

Public Prepare/body/control formats, capabilities, deadlines, physical sends,
V1 rows, canonical product payloads, private native bytes, journal and exact
TerminalAck are unchanged. Reuse the existing receipt table/key; no migration,
row rewrite or compatibility alias is introduced. Local main lacks these V2
codec files, establishing unmerged source provenance, not proof that no external
rows exist. The disabled rollout must refuse an incompatible row; any actual
data disposition is outside this source task's authority.

## Verification and remaining memory obligation

Implementation checklist belongs in #200's execution comment:

- All five stages round-trip to the exact original typed identity and digests.
- Max escaped 32 KiB/32,000-rune answer, two distinct 32 KiB artifact bodies,
  255-byte names, maximum scope and accepted Usage/Substrate survive.
- Whitespace remains in Summary; only DisplayText is trimmed. A mismatch,
  missing/null/alias/duplicate/unknown encoding or display member denies.
- The caller's original receipt is unchanged; insert/update/error/cancel and
  one-shot SQL semantics remain. V1 and public fingerprint vectors are stable.
- Differential parser controls, bounded fuzz/race/shuffle, native JsonDocument
  persistence/custody and cumulative original-issue independent review qualify
  the actual changed artifact before merge.

This projection removes a concrete duplicate; it is **not** whole-memory proof.
Before closing the aggregate finding, account separately for encode/decode,
validation/fingerprint, SQL/protobuf/gRPC buffers and canonical planning/reread
lifetimes, including their capacities and borrowed ownership. A cap at Encoder's
Write occurs after allocation and is insufficient. Shared JSON pool history
is not a per-attempt capacity or zeroization guarantee. Preserve all admitted
metadata instead of adding a smaller hidden limit. Do not restore the old
payload-sized generic canonical trees when simplifying the decoder.

Research/design/source/fake tests only. No actual provider/credential/service,
native VM/YDB launch, downloads, deployment, migration or network changes.
