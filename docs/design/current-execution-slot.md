# Current lease storage within Run

Version: 0.1.1. Status: accepted engineering design; implementation pending.
Owner: [#205](https://gitcode.com/urandon/sessionless/issues/205).
Predecessor: the accepted four-index reduction in #206 and #207.

Store the current lease tuple on the existing tenant-scoped Run row and retain
the bounded historical Lease records. This can remove `lease_heads` without a
replacement table, scheduler or permission issuer. It does not consolidate Run,
Attempt, effect reservations and output custody into a generic execution ledger.

The first index-retirement contract remains unchanged. This draft concerns one
additional table and preserves both MVP execution routes. It adds no release
gate for an unrelated working demo.

## Alternatives and selected proposal

| Layout | Physical consequence | Decision |
| --- | --- | --- |
| Complete current tuple in lease_heads and retain leases | Both tables remain | Reject as a table-reduction slice; useful read simplification alone does not meet this outcome |
| Current tuple in runs and retained leases | Retire lease_heads after compatible transition | Proposed next bounded implementation |
| Current tuple only and no retained leases | Remove stop and terminal observation input | Defer until equivalent existing evidence and retention have been designed |
| Merge all execution and accounting bodies | Couples unrelated ownership and lifecycle | Outside this slice |

After #207, the proposed application-table change is 101 to 100 at the inspected
source baseline. Additional unrelated entities change that total. Historical
CREATE lineage and effective schema remain separate registry concepts.

## One current tuple

Use the existing `runs` primary key `(tenant_id, run_id)`. Add seven nullable,
typed columns, outside the canonical `domain.Run` JSON payload:

| Column | Type | Meaning |
| --- | --- | --- |
| current_lease_id | Utf8 | Current physical claim |
| current_lease_attempt_id | Utf8 | Owning Attempt |
| current_lease_worker_id | Utf8 | Exact worker |
| current_lease_fence_token | Uint64 | Positive current fence |
| current_lease_acquired_at | Timestamp | Original acquisition and deadline anchor |
| current_lease_expires_at | Timestamp | Current expiry |
| current_lease_updated_at | Timestamp | Last current-tuple mutation |

All seven NULL means no current lease. All seven populated and validated means a
complete tuple. Any partial, malformed, foreign or inconsistent tuple is
unavailable/denied, never treated as an empty slot. A missing Run is distinct
from a present Run with an empty slot.

Do not encode a second tuple inside Run JSON, add legacy key aliases, fabricate
a Run using a blind slot UPSERT, or add a lease-ID lookup table. Keep the existing
public Lease representation and retained `leases` ownership initially. Retained
Lease bodies can continue to resolve the existing renewal API's LeaseID to RunID;
this slice requires no new lookup table or public locator API. Any internal
locator change must still validate the same tenant/Run/Lease/fence tuple.

## Atomic operations

Use the existing serializable state transaction and its time authority. A Run
write that does not own the slot must preserve these columns. The official
[YDB UPSERT contract](https://ydb.tech/docs/en/yql/reference/syntax/upsert_into)
preserves unspecified columns of an existing row; verify that property through
the project's pinned backend and SDK before accepting the replacement.

| Operation | Required mapping |
| --- | --- |
| New claim | Read the existing Run and complete current tuple, validate the original owning Attempt and admission gates, then atomically write retained Lease, expiry-v2 tuple and new slot. Preserve overflow rejection and fence advancement for an existing expired claim |
| Exact active replay | Correlate the same tenant/Run/Lease/Attempt/worker/fence/acquired_at/expiry and retained body; return the original Lease without extending it or issuing another mutation |
| Competing active claim | Return the existing held/conflict outcome; no overwrite |
| Renewal | Require exact current Lease/fence and unexpired ownership, preserve original acquired_at, then replace old expiry-v2 tuple and update retained body and slot atomically |
| Worker mutation | Current slot remains a transactional prerequisite for the same caller-owned publication/effect checks; a retained Lease alone grants nothing |
| Terminal release | Commit the existing terminal/output/counter transitions and clear all seven slot columns with the exact expected Lease/fence; delete only the matching expiry-v2 tuple. Never delete the Run to release a lease |

Clearing the slot preserves the existing terminal semantics; it does not create
a new permanent fence counter. Existing terminal/effect barriers must still
reject reopening, stale publication and unknown-effect replay after release.
Read-only stop or explanation consumers may inspect retained historical
evidence without turning it into current execution authority.

## Consumer replacement

Replace the complete set of existing production head consumers, not just the
ordinary claim path:

| Consumer | Invariant retained |
| --- | --- |
| operations claim, renew and requireLeaseOwnership | Per-Run contention, exact claim/fence/expiry, read-only stable replay |
| attached worker allocation and canonical head read | Same transaction as the offer; original database time and complete tuple |
| owned Attempt effect parent | Physical tuple/body correlations, original acquired_at and bounded deadline; existing private admission limits |
| worker terminal scheduling | Exact slot clear and retained Lease body; existing canonical terminal/counter updates |
| web-search consent stop/reconciliation | Retained Lease is read before lost-current-claim disposition; malformed/foreign evidence denies |
| bounded ready/expiry repair | Correlate Run, slot, retained body and Attempt; copy only the exact derived tuple without effect replay |
| Session deletion and retention owners | Run deletion revokes current ownership; preserve existing retained custody and blob/effect cleanup constraints |

Project named bounded slot columns rather than fetch an arbitrary full Run row
for the old head read. Preserve the existing per-field source-admission limits,
metadata byte/callback accounting and physical/body correlation at each consumer;
row co-location does not waive them. Existing protected Source/SQL/accounting
STOP restrictions are not lifted by this design.

No consumer may use old head data as an automatic fallback once the Run-slot
contract is canonical. Missing discovery remains repair/unavailable. Missing
lease, effect or custody evidence is not proof an external request was unsent.

## Writes and contention

A one-for-one head-write to slot-write mapping preserves the scoped successful
statement counts: 3 for an absent-head/absent-LeaseID first claim, 4 for expired
head/new-LeaseID reclaim, 4 for ordinary renewal, 0 in exact active allocation
replay and 2 in the terminal lease-release portion. An already-present selected
LeaseID adds its previous-index DELETE to the absent-ID claim/reclaim cases.
These exclude reads, retries, counters, output and other surrounding work.

Therefore the proposed saving is one physical table, not a demonstrated reduction
in writes or RU. Renewal already reads Run through PutLease ownership validation;
co-location additionally writes the same row. Official
[YDB transaction semantics](https://ydb.tech/docs/en/concepts/transactions)
describe optimistic row-read conflicts, so compare the existing and proposed
claim/renew/cancel/finalize interleavings rather than assume less contention.
Use the pinned backend for acceptance; current public documentation is not its
execution receipt.

## Maintenance cutover and recovery

Choose a maintenance cutover, not mixed-authority rolling operation. Original
head-authoritative code and Run-slot-authoritative code must never concurrently
make currentness decisions or mutate the affected execution scope. Serializable
row conflicts are not a bridge between two different authoritative records.

Use this finite sequence:

1. Add nullable slot columns with one idempotent schema operation per migration,
   preserving all applied SQL/checksums. Original code remains head-authoritative
   and does not write shadow slots. There is no temporary dual-write runtime.
2. Stop new execution admission for the affected scope. Stop and join **every**
   head-authoritative writer and currentness consumer: ordinary/attached workers,
   control endpoints, renewal/reclaim/release callbacks, effect preparation,
   deletion/backfill/repair and pending transaction retries. The operation manifest
   names the actual binaries, processes/endpoints, affected tenants/Runs and
   existing deployment controls that prevent late invocations. Require observed
   completion of outstanding transactions and prevention of later old-authority
   work; process termination, an elapsed TTL or a guessed sleep alone is not
   proof. If existing controls cannot establish this boundary, stay on the
   original authority and do not perform the cutover.
3. With that boundary maintained, copy the exact scoped current tuples from
   heads plus correlated retained Lease/Attempt/Run sources. Bound explicit
   locators, pages, cumulative rows/bytes, callbacks, retries and total duration.
   Verify both directions, including a populated slot with no head, not only
   heads missing slots. An empty original head must not make a historical Lease
   current. Clear stale state only from authoritative release/terminal or
   never-claimed correspondence; unexplained active mismatch remains unavailable.
   Preserve original timestamps, fence, expiry-v2 tuple and retained-body TTL.
4. Verify complete stable correspondence and record it with the exact source and
   quiescence evidence in the existing deployment handoff. Keep original actors
   stopped, then start the **sole Run-slot authority** and reopen admission. No
   head-authoritative currentness reader/writer may survive this step; the
   retained physical table is a rollback aid, never a runtime fallback.
5. Retire `lease_heads` only through a separately reviewed exact forward
   definition and effective-schema registry update. The owner's grant for four
   discovery-index DROPs does not authorize this additional table operation.

An operator crash or partially completed copy keeps admission and old actors
stopped. Resume the bounded copy/verification from authoritative source tuples,
or restore the verified original writer family before reopening service; partial
coverage, schema presence or a receipt alone never elects the Run-slot authority.
Maintain the stop across the crash using the selected existing deployment
controls, not a new persistent mode, migration ledger or permission issuer.
Read-only history may remain available only if it cannot authorize current work.

Rollback has the same exclusive boundary in reverse: close admission, stop/join
all Run-slot writers and currentness consumers, then reconstruct and verify old
heads from the complete current slot plus retained sources. Verify populated
**and empty** mappings; remove a stale old head only with exact released or
never-claimed correspondence. An unexplained active mismatch is not permission
to select either copy. Check expiry-v2 correspondence and original fence/time
identity, then start head-authoritative code only after Run-slot actors cannot
resume. Do not shorten retained Lease lifetime or synthesize a new claim.

After physical retirement, keep the forward binary unless the separately
authorized rollback first reconstructs the old schema and exact metadata under
this same stop. Neither direction resends ambiguous provider/upload work. No
automatic destructive Down or zero-downtime rolling upgrade is promised.

## Verification and implementation handoff

The bounded implementation checklist under #205 should contain real linked
children, registered only after independent acceptance of this design:

- Additive typed columns and complete source/consumer mapping, bounded bidirectional
  transfer/recovery and deterministic fixtures for the maintenance boundary.
- Canonical Run-slot consumer cutover with retained evidence and the existing
  renewal API, plus caller-level claim/cancel/finalize/replay tests.
- Exact head retirement/registry/docs and separately authorized disposable
  integration proof, followed by separately authorized retained rollout.

A small implementation may combine the first two when its immutable review and
coverage remain coherent. Do not create another epic, scheduler or permission
framework for these steps.

Acceptance requires pinned-backend slot preservation across normal PutRun,
all-null/full/partial and absent/foreign Run cases, original acquired_at,
overflow/stale-worker/replay/cancel/finalize controls, historical consent and
terminal evidence, bounded operator repair and two-owner isolation. Compare
before/after complete schema and scoped mutation counts. Prove that old-head
expiry versus renewed-slot expiry, terminal slot clear, pending callback after
stop and both rollback directions cannot produce mixed-authority operation.
Failure or restart during transfer must keep admission closed. Measure conflict/retry
behavior under the same finite workload if claiming a performance improvement.
Run focused race/repeated/shuffled checks, relevant existing suites,
entity-registry drift checks, independent exact-source review and all required
CI before merge. No new local/native run, credentials or provider call is
authorized by this design.

## Source contracts

- [Current Lease and Attempt domain](../../internal/domain/work.go)
- [Run persistence and Lease ownership](../../internal/ydbstore/store.go)
- [Current claim and renewal](../../internal/ydbstore/operations.go)
- [Attached allocation](../../internal/ydbstore/attached_worker_execution_helpers.go)
- [Effect parent](../../internal/ydbstore/owned_attempt_effect_parent.go)
- [Terminal scheduling](../../internal/ydbstore/worker.go)
- [Consent stop](../../internal/ydbstore/web_search_consent_reconcile.go)
- [Bounded repair](../../internal/ydbstore/ready_expiry_repair.go)
- [Accepted first reduction](state-storage-simplification.md)
- [Entity registry](../architecture/entity-registry.md)
