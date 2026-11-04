# Canonical input and retained output correspondence

Version: 0.1. Implementation/integration owners:
[#194](https://gitcode.com/urandon/sessionless/issues/194),
[#191](https://gitcode.com/urandon/sessionless/issues/191) and
[#197](https://gitcode.com/urandon/sessionless/issues/197).
This clarifies history consumption for the existing managed harness. It does not
change ingress, scheduling, output formats, funding, or execution authority.

## Problem and existing authorities

Canonical [ingress](../../internal/ydbstore/canonical_ingress.go) appends an
authenticated user message and creates its Run/outbox without requiring another
Run to finish first. Canonical
[finalization](../../internal/ydbstore/canonical_finalization.go) appends one
contiguous output batch at the Session's latest sequence in the original
transaction. An input event has its producing RunID too; that does not make it
an output of the Run's terminal capsule.

These are therefore legitimate sequence shapes:

```text
user-A, outputs-A, user-B, outputs-B
user-A, user-B, outputs-A, outputs-B
```

In the second shape, a later reader of Run B encounters both another input and
Run A's output before B's output batch. Allowing only foreign user events before
an output batch would still reject that valid history. Conversely, ignoring all
foreign events would not prove that their output, failure, or legacy provenance
was checked.

The original [history owner](../../internal/ydbstore/local_terminal_history.go)
and its existing retained readers remain authoritative. A shape predicate,
RunID, queried Run row, nonempty cache or capsule schema alone is not a proof of
complete history or permission to execute.

## Required correspondence

1. Keep the complete original bounded canonical view, its source/frontier
   identity and every payload's existing integrity checks. Do not filter the
   view down to a convenient Run, discard intervening records, reorder events,
   fabricate missing events, or reconstruct erased execution requests.
2. Separate canonical inputs from producing events. When present in the view,
   the original trigger is identified by the Run's exact TriggerEventID, not by
   being any user event with the same RunID. Check original tenant/Session,
   author correspondence, payload identity, uniqueness and ordering before the
   Run's output batch. Do not invent a requirement that trigger and output are
   adjacent. Apply this distinction consistently to V2 and V3 consumers.
3. Discover every other Run contributing a tool call/result, assistant message
   or Run-associated notice through the existing bounded query. Partition those
   producers exhaustively under their actual supported contracts: sealed local
   success, supported retained failure, original legacy success, or a permitted
   ordinary legacy notice. Querying a Run is not classification. A missing or
   ambiguous classification denies; it must not become a legacy fallback.
4. Preserve each existing legacy path's own validation and semantics. A permitted
   ordinary legacy notice is not a completed tool pair, retained failure,
   successful model result, source citation or execution receipt. Closed failure
   notices must still have their exact retained failure correspondence. An
   unanchored tool-producing Run is not an ordinary notice.
5. For each supported sealed success or failure, authenticate exactly the facts
   required by its original codec: parent/admission, accounting, custody,
   marker/seal and actual canonical payloads, with manifest/evidence only where
   that original arm requires them. Do not require or invent a public artifact
   manifest for a retained failure or add a query for one. Match every producing
   event of that Run exactly once; extra or missing same-Run outputs deny.
   Existing singleton failure and legacy arms retain their own contracts.
6. A Run's own emitted batch remains contiguous and ordered, with the original
   output identities, timestamps, complete call/result pairs and terminal
   anchor. Apply the existing output-count ceiling to that batch, not the total
   distance from input to terminal anchor. Other authenticated inputs or fully
   classified producers may precede the batch, but cannot occur inside it.
7. An intervening producer is accepted only as part of the complete closure
   above, not because its RunID differs from the selected Run. Keep every
   sequence and existing scope/time/payload constraint. Foreign tenant/Session,
   gaps, duplicates, unknown producer kinds and crossed capsule identities deny.
8. Publish the materialized history collections only after the complete selected
   closure succeeds. An error after one capsule must not leave a populated
   prefix that a subsequent call mistakes for complete validation. Use local
   owned candidate collections with error cleanup, or prove an existing sticky
   denial through every caller. A new unchecked ready/completed Boolean does
   not establish coverage.

All these are DATA correspondence checks. They cannot reconstruct a serving
holder, current approval, funding witness or provider capability. The existing
current Source/member/claim checks still decide a new invocation. Existing
unresolved-effect/reconcile-only rules are not converted into successful
history or silently omitted.

## Bounds and compatibility

Reuse the original bounded view, producing-ID query and retained-body reads.
Do not add a history query, recursive ancestor discovery, grant archive, table,
migration, receipt registry or new execution state machine. Classifying records
in memory does not increase the existing SQL callback, metadata, body, context,
capsule or active-time allowances. If the complete legitimate history exceeds
an existing admission limit, return its actual bounded failure; do not drop
records to make it fit.

No output schema or sealed byte changes follow from this correction. Preserve
old valid V1/V2/V3 encodings, supported mixed generations, legacy notices,
MCP-only and zero-tool text behavior, finalization identities and exact source
lineage. Do not make ordinary historical decoding depend on live credentials,
renew old grants or send provider traffic.

## Implementation and verification checklist

- [ ] Enumerate the current producer-classification and materialization arms,
  including the actual existing ordinary legacy-notice grammar; make their
  coverage explicit before relaxing the segment guard. A classification label
  is not validation, and legacy failures do not all require local capsules.
- [ ] Reproduce the original trigger/output confusion through each affected
  existing V2/V3 API, then pass the same positive after the bounded correction.
- [ ] Exercise normal ingress A, ingress B while A is active, original atomic
  completion A/B and a later preparation C. C must consume both original inputs
  and both validated output batches; a user-only prefix fixture is insufficient.
- [ ] Check zero/four output pairs, odd/even input-prefix lengths, supported
  mixed generations and existing ordinary legacy notices without changing their
  interpretation. Test malformed trigger/author/body, cross-scope records,
  output interleaving, gaps/duplicates, too many pairs, unanchored tool producers,
  crossed seals and missing failure correspondence.
- [ ] Force a later retained read/validation to fail after an earlier one passes;
  repeating the same consumer must not accept the partial prefix or leak owned
  bodies. Check bounded cleanup and original uncertainty semantics.
- [ ] Preserve old text/MCP/failure suites and prospective-fit limits; obtain
  independent exact-artifact review, populated race/repeat/shuffle tests and
  the existing exact-head CI gates before merge.

Pure shape/DATA fixtures are useful regression evidence, not genuine queued
ingress/finalization, native YDB, fresh login, real-model usefulness, or full MVP
proof. Keep those evidence scopes explicit in the owning execution comments.
