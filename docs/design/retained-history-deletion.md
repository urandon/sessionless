# Writer-aware deletion of retained loop history

Version: 0.1.0, independently reviewed engineering design. Primary acceptance
covers this contract/decomposition only, not implementation or rollout.
Owner: [#221](https://gitcode.com/urandon/sessionless/issues/221), a narrow
[#192](https://gitcode.com/urandon/sessionless/issues/192) obligation under
[#176](https://gitcode.com/urandon/sessionless/issues/176). Reuse the existing
Session deletion lifecycle; this is not the whole upload-management program.
Input: [terminal custody 0.1.2](owned-loop-terminal-custody.md).

## Decision and existing gaps

Retain `requested -> deleting -> completed`, the existing Session fence,
typed inventory confirmation, exact-object deleter and final SQL transaction.
Add one small versioned verification commitment to `SessionDeletion.record`.
The commitment records what the original Store verified before destruction;
it does not reconstruct an uploader, cleanup receipt or execution capability.
No new table, scheduler, reference counter, upload service or GC is needed.

The [current lifecycle](../../internal/ydbstore/session_lifecycle.go) rejects
every custody-bearing Run at Request, Start and Complete. Its inventory misses
the private Run plans. Independently,
[admission retention](../../internal/ydbstore/owned_admission_retention.go)
rejects every prepared parent. Simply removing either guard is unsafe.
[Execute](../../internal/sessionlifecycle/service.go) deletes objects before
Complete, so verification requiring those bodies cannot be repeated after a
crash. All four changes must land together before this path is eligible.

## Eligible writer and trust boundary

Only a genuine successful local V2 completion covers its exact original scope.
The trusted verifier must join and validate:

- Canonical Run and its Session index, complete custody/derived metadata,
  successful terminal status, cancellation absence and exact tagged V2 marker.
- Original admission/parent, source identity, preparation burn, original and
  retired allocation transition, persisted V2 evidence and accounting. Scope,
  digests, times, claim and complete correspondence must agree.
- Actual bounded capsule bytes, original canonical event payloads and manifest,
  retained summary/manifest closure and the unchanged V2 finalization seal.
  Reuse the historical codecs/digest functions; do not invent another seal or
  trust status, marker syntax, copied evidence or an explanation projection.

The [original cleanup](../../internal/ydbstore/local_terminal_cleanup.go) joins
keeper/bridge, retires allocation and closes the physical write namespace;
the [original finalizer](../../internal/ydbstore/local_terminal_finalization.go)
alone can commit the success correspondence/evidence/marker. That successful
seal attests its earlier private receipt verification. The deletion verifier
does not recreate or MAC-verify an erased receipt from its digest. Hashes are
integrity evidence under trusted writers, not protection against coherent
privileged database rewriting.

Use historical producer validation, not active execution authorization:
deletion must work after Source/profile/lease expiry without reopening them.
Factor the historical validation from current consumer fit/profile checks;
retain all structural and original-scope/accounting invariants. Unsupported
historical codecs deny explicitly; this does not waive #191 tool integration.

Inspect every admission key in the Session reverse index, not only the selected
Run Attempt. Exactly proven successful scopes may retire their own prepared
parents/children. Existing genuinely unprepared-parent handling remains.
Foreign, orphan, extra prepared, failed/cancelled/planned, ambiguous PUT or
unknown-writer scopes remain blockers. An absent object, expired lease, stopped
outer process, cancelled HTTP request or retired allocation alone is not proof.
This version does not design reconciliation for those blockers.

## Exact inventory and recoverability

Keep SQL ownership rows intact until the final completion transaction. Extend
the existing paged inventory, retaining its one aggregate 10,000 row/object
ceilings, exact BlobRef validation, dedup/conflicting-ref rejection, checked
byte sum and tenant/Session/producing-Run namespace checks.

| Owning SQL data | Objects to enumerate |
| --- | --- |
| Existing events/snapshots/manifests/receipts/delivery/checkpoint rows | Existing exact references, unchanged |
| Every Run's terminal plan | Its exact private capsule, even if not public output |
| Every Run's complete derived inventory | Both summary/manifest plans, including unselected/unpublished plans; never only Head |
| Original same-Session dependency owners | Their already registered Run pairs/capsules or canonical references, deduplicated through their own ownership rows |
| A local custody-bearing Run | Deterministic prepaid cancellation notice using original `EncodeFailureV1(run.ID,true,"cancelled")` and `PlanEventDraftV1` codecs, even after success |

A retained manifest's references are not independent deletion authority.
Before Start, resolve every dependency to an exact original owning SQL record
within this same Session inventory. A missing owner, mismatched BlobRef or
cross-Session dependency denies; do not recursively harvest arbitrary blob
paths. Shared profile/calibration/instruction/provider objects are not Session
property and must not be deleted. Original operation bodies already erased by
genuine success are not resurrected or re-PUT. Failed/unknown writers are not
made safe by adding their guessed paths to the inventory.

This ownership rule makes the complete sorted exact-object list reconstructable
from retained SQL and the original deterministic notice codec, even after
every object has been deleted. A dependency available only inside a blob and
not represented by an owning SQL record is an explicit unsupported inventory,
not an excuse to retain only a digest of an unrecoverable list. Do not delete
SQL ownership pages incrementally. A future producer needing additional refs
must register them durably before PUT in its existing owner, not add discovery
to the resume path.

## Trusted commitment and digest order

Add optional `retained_verification` to the existing deletion JSON, with a
closed `sessionless.retained-deletion.v1` shape, at most 2 KiB:
`schema`, `tenant_id`, `session_id`, `request_digest`, `metadata_digest`,
`inventory_digest`, `rows`, `objects`, `bytes`, `verified_runs`, `verified_at`.
All digests are lowercase SHA256; counts/bytes are checked unsigned values,
time is UTC microseconds. Unknown members/versions, duplicates, oversize or
inconsistent state deny. No plaintext content, key, per-object journal or
caller-provided approval is stored. The existing outer request identifies its
owner/reason/request time; `request_digest` binds those immutable fields.

`metadata_digest` hashes a domain-separated, length-delimited stream ordered
by table identity and complete primary key: exact canonical typed postimages
of every ownership/authority/retention row used by this deletion, including
Run/index/marker, canonical material identities, manifests, receipts, all
admissions/parents/allocation/accounting/evidence and scoped operation children.
Include absent optional rows and end-of-range/count facts so insertion,
deletion, reordering or extra prepared Attempts cannot be hidden. Exclude only
the deletion commitment itself, its changing lifecycle fields and audit rows;
there is no hash cycle. Mutable current principal/legal-hold authorization is
checked separately at every destructive admission/completion, not substituted
by this historical digest.

`inventory_digest` hashes a versioned canonical encoding of the full sorted
existing inventory and counts, including complete BlobRefs/Run IDs. Proof
order is original trusted writer facts -> actual body verification -> full SQL
rejoin and inventory equality -> commitment. The existing Store transaction
alone installs it together with `deleting`; Request rejects supplied
`retained_verification`, and public lifecycle methods accept no approval DTO or
hash override. A format-valid caller-created commitment never authorizes Start.

## Transitions, races and restart

1. **Request:** retain terminal Run, owner/membership, legal-hold and pending
   attached-receipt gates, but separate terminal/index validation from custody
   physical-erasure eligibility. Install the existing requested tombstone to
   fence registration, execution and canonical appends. This transition grants
   no Delete. Planned/unknown terminal writers may remain requested/blocking;
   they cannot become eligible merely because the fence exists.
2. **Plan:** enumerate bounded SQL-only exact refs for typed confirmation.
   No physical deletion or proof claim. Do not modify the request or capture.
3. **Start from requested:** constructor-fixed read-only blob access captures
   bounded SQL metadata, verifies actual immutable bodies outside SQL, then
   transactionally re-reads the complete captured postimages/ranges/inventory.
   Recheck durable request, current owner/membership, legal hold, all terminal
   Runs, attached receipts and every scoped writer. Only an exact match commits
   the trusted commitment and `deleting` atomically. Any changed source, extra
   registration, lost ownership or ambiguous proof leaves no Delete authority.
4. **Before first or resumed Delete:** Service must re-plan AFTER successful
   Start and compare both the persisted commitment and original typed
   confirmation against this exact inventory. Do not use its older pre-Start
   Plan. A changed inventory needs a new explicit confirmation; no Delete has
   occurred. Stores lacking the trusted reader deny retained Start, while their
   old V1 path remains available.
5. **Deleting retry:** reconstruct inventory and metadata solely from SQL;
   compare every digest/count/request field with the originally stored capture
   and recheck current authorization/hold. Never re-read deleted capsules,
   refresh verified_at, replace the capture or renew writer permission. Same
   exact Delete of an already absent object is idempotent under BlobStore's
   existing contract; errors remain incomplete, with no PUT/list-prefix.
6. **Complete:** rebuild and compare the same SQL-owned inventory before the
   transaction; inside it rejoin the committed metadata/capture/ranges and
   existing gates, then delete only proven scoped children plus the existing
   canonical rows and mark the surviving tombstone completed atomically.
   Caller counts must equal captured totals; counts alone are not proof.
   The trusted Service has already received success for every exact Delete.
   `CompleteSessionDeletion` remains an internal trusted application port, not
   an externally callable counter/receipt endpoint. Store checks prove SQL
   admission and inventory identity; caller counters do not independently prove
   object absence. CLI/HTTP composition must route destruction through this
   Service, never expose direct Complete or claim an arbitrary direct call as
   physical-erasure evidence. This retains the existing trust boundary instead
   of adding a serializable delete capability or another receipt ledger.
7. **Unknown ACK:** read back the original request/state/capture. Exact committed
   `deleting` resumes the original inventory; requested must repeat verification
   before any Delete. Complete lost-ACK replay succeeds only on identical
   tombstone/counts. Never infer success from an observation timeout or create
   another deletion attempt. Repeated physical deletes do not repeat uploads.

Preserve current legal-hold ordering: a hold can be established while requested
and blocks Start; `PutSessionLegalHold` rejects new holds once deleting begins.
Revocation or metadata corruption during deletion stops further work/completion
for explicit reconciliation; it cannot undo prior physical deletes or justify
inventing an override. Registration and terminal finalization must continue to
read the SAME Session tombstone in their original transactions. Paused/unknown
preexisting I/O never becomes quiescent merely through this database fence.

## Bounds, storage and composition

Stream and hash SQL pages; do not buffer every 32-KiB Run. Retain existing
row/object limits across all new parents/children, not per Run independently.
Use bounded point reads, overflow sentinels, exact EOF and Close checks. Reuse
the per-Run authority verifier's 3 callbacks/32 statements per callback/96 total,
4-MiB materialization and 10-second deadline; never reset it inside a Run.
The deletion call retains the existing finite outer deadline (CLI: 20 minutes).
Account cumulative bytes with checked arithmetic against the sum of admitted
per-Run envelopes and bounded SQL/BlobRef inventory; process one Run at a time.
No unbounded I/O, parallel buffer fan-out, SQL-held blob I/O or automatic limit
increase. Oversized Sessions stop explicitly before physical destruction.

Trusted application composition supplies the Store's read-only blob port;
never accept a per-request verifier callback. Factor pure historical validation
without creating a Source or reactivating an allocation. Wire the existing
session-delete CLI and SessionLifecycle Service to the same reader/deleter
installation; defaults must not quietly enable retained deletion. Preserve
canonical V1 codecs/markers and existing V1 deletion. No backfill or reinterpretation
of old deleting tombstones: retained state without a genuine capture denies.

Update the entity registry's lifecycle/retention explanation and source mappings
when implemented. `session_deletions.record` gains the small typed field;
physical tables, applied migration files and TTLs remain unchanged. The capture
survives completion as content-free audit evidence. This document changes none
of those runtime semantics by itself.

## Handoff and decisive evidence

[One implementation child, #222](https://gitcode.com/urandon/sessionless/issues/222),
jointly delivers trusted capture,
complete reconstructable inventory, exact prepared-scope retirement, Service
post-Start/resume ordering, constructor wiring and entity-registry updates.
Do not merge a guard relaxation before the complete path is reviewed/tested.
The owning #192/#176 checklist tracks this child; source publication and its
predecessor integration remain required before implementation acceptance.

Tests must start from genuine zero- and two-compaction producers/Complete,
not handwritten status/evidence. Assert capsule/all-pairs/dependency/notice
coverage and dedup; exact original scope only; paused/copied/unknown/failed
writers and extra prepared Attempts denied; marker/evidence/accounting/
correspondence/capture/SQL-index tampering; registration vs Request/Start races;
legal hold/revocation; bounded row/object/body/Close failures; lost ACK at each
transition; partial-delete restart with a fresh Store/Service and deleted bodies
unreadable; no PUT, no new capsule read, no resurrection; exact final replay and
unchanged V1 regressions. Required CI and independent exact-head review precede
merge. Native SQL/actual storage evidence remains distinct from owned fakes.

No actual deletion, local native-YDB rerun, cloud/migration/provider/key calls,
downloads, daemon/VM or network changes follow from design acceptance. Neither
#192 nor the MVP is completed by this document or by controlled fixtures.
