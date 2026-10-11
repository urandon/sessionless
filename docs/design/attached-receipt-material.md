# Attached receipt canonical material verification

Version: 0.1.1, 2026-10-11.
Owner: [#142](https://gitcode.com/urandon/sessionless/issues/142), under
[#155](https://gitcode.com/urandon/sessionless/issues/155).
Source: `6c503bfbc0023f94d9b5dabbd0f6143e47888c87` and the accepted
[compute choice context evidence contract 0.1.5](compute-choice-context-evidence.md).
Status: independently reviewed engineering contract accepted under the agreed-feature
engineering delegation; implementation, maximum-case evidence and enablement
remain separate gates.

Receipt-capable attached finalization must prepare canonical material from the
actual immutable envelopes referenced by its durable receipt. A private
preparation created while accepting candidate output cannot survive receipt
serialization or authorize a later terminal commit. Keep object reads outside
SQL, recheck authority at commit, and return completed replay without reading
objects again. This changes neither the attached execution protocol nor the
agreed two-route MVP scope.

## Current source and decision

`internal/controlapi/attached_worker.go` passes the Store as both transport
broker and finalizer. `Store.CommitAttachedWorkerTerminal` partitions pending,
committed and retired reconciliation. The receipt resolver in
`internal/ydbstore/attached_worker_output_receipt_store.go` intentionally rejects
caller-supplied completion or failure for a receipt-capable manifest and loads
the durable READY receipt instead. Its reader currently selects an unbounded
legacy JsonDocument. The receipt HTTP response ceiling of 512 KiB does not bound
that persisted read or establish integer fidelity.

Use a narrowly scoped finalizer wrapper that holds Store and BlobStore while
forwarding the existing poll, exchange and drain broker behavior. Introduce two
Store-owned internal seams: a read-only terminal preflight and a commit that
accepts a private prepared receipt proof. Do not add prepared material to public
HTTP/protocol schemas or bypass the resolver through supplied materialization.
The ordinary metadata-only append remains UNKNOWN; no backfill repairs old
prefixes merely because current output can be verified.

## Preflight and replay

The preflight first validates the same request and owner membership as the
existing terminal commit. It reconstructs the existing committed or retired
terminal chain using retained Terminal and TerminalAck ledger records, exact
materialization reservation and execution connection provenance, canonical
Run finalization and receipt bindings. An exact authorized replay returns the
original attempt and outbound result, with zero object opens, no event or prefix
fold, and no speculative mutating commit probe. Conflicting evidence does not
become a new pending request.

For a genuinely pending terminal, use one transaction-time snapshot to verify
the current owner, worker, connection, protocol manifest, attempt revision,
Run, canonical attempt, reservation, lease, fence, job harness and deletion
state. The receipt must be READY, not copying, and match its exact primary key,
terminal status, canonical digest, process observation, credential-required
flag, harness binding and ordered materialization. Its canonical envelope refs
come only from this authorized receipt.

Preserve legitimate reconnects: immutable execution provenance may refer to the
connection that performed the effect, while current-head authority refers to
the rebound connection. Do not require those two connections to be equal.
Historical replay retains the existing owner and ledger proof; imposing a new
live lease or unrevoked-worker condition would incorrectly destroy lost-ACK
recovery after completion.

The preflight returns either replay, an existing typed denial/conflict, or a
private read plan. The plan seals the entire validated durable receipt,
manifest, job/harness and relevant pending authority identities, not only
CanonicalDigest. The digest excludes some receipt metadata and cannot fence
its replacement alone. The seal is a server-internal ownership object, not a
caller-controlled boolean, reusable permission token or serialized credential.

## Actual envelope reads

Read only the receipt's canonical envelope refs, outside the transaction.
Before Open, check exact tenant, Session and event namespace, declared size and
hash and the operation's remaining budget. Read at most the declared size plus
one byte, verify exact size and SHA-256, close the reader on every path, and
clear temporary bytes after owned preparation has copied them. Reader close
errors must not leave a successful proof.

Reuse PrepareV1 with the actual stable event identity and ordered canonical
artifact associations. Reuse MeasurePreparedRecordV1 after final sequence/time
assignment inside the canonical transaction. Do not rescan candidate staging
artifacts or attachment bodies merely to measure an assistant Summary, infer
media safety, normalize immutable payload bytes, or use DisplayText as proof.

Valid unsupported envelope versions and projection-oversize envelopes retain
UNKNOWN material coverage, under the existing projection policy. They do not
truncate output or reject otherwise valid canonical history merely for lacking
an eligible text projection. For a planned bounded read, missing objects,
short/extra data, mismatched hash or scope, and corrupt recognized envelopes
produce a content-free source error and no canonical commit. Temporary object
errors remain retryable; they never downgrade corrupted bytes to a complete
fact or cause another provider effect.

Legacy JsonDocument records cannot be described as exact original bytes.
Re-marshalling a decoded value cannot detect an earlier rounded integer.
The bounded proof path therefore checks numeric tokens before typed decode:
integer-valued fields produced by the existing trusted integer serializers must
be within the exact safe interval [-9007199254740991, 9007199254740991] and the
field's own narrower domain range. Tokens at or beyond either unsafe boundary,
fractions, invalid integer forms and unsupported numeric fields fail the source
check. The decoded safe interval cannot contain an originally unsafe integer
rounded from outside it; this relies on the existing producer's integer-only
field schema, not on a claim about arbitrary stored JSON. Independent scalar
keys and all existing receipt, observation, manifest and Run commitments still
have to match. No canonical commit follows failed authority proof.

| Field class | Required proof and deterministic outcome |
| --- | --- |
| Receipt identity and binding | Version, attempt sequence, enrollment/connection/lease generations, expected attempt revision and expiry microseconds use the safe integer policy and independent keys/ledger/current authority. Any mismatch or ambiguity is conflict/source failure, never UNKNOWN. |
| Receipt materialization and canonical authority | All integer fields in event refs, manifest, completion/failure, usage, process observation and relevant job/Run/reservation/harness/ledger records use the safe policy and their existing typed validation/commitments. Failed canonical or observation digest is conflict with no commit, regardless of whether measurement could otherwise succeed. |
| New material measurements | Counts, bytes and final prefix integers derive from actual bounded envelope bytes, final events and exact String fact rows, not legacy receipt hints. Valid authority with unsupported or oversized projection may append UNKNOWN; measurement hints cannot rescue failed authority. |

No schema conversion, guessed original value or arbitrary legacy normalization
is part of this slice. Authority outside the safe numeric profile cannot use
the bounded proof path. Wider exact numeric receipt storage would need a
separate explicit schema/writer/read contract and cutover, not an UNKNOWN
fallthrough that finalizes unproved authority.

## Commit and races

After reading, the prepared-commit transaction repeats the existing terminal
partitions before consuming preparations. A concurrent winner returns its exact
committed result rather than refolding or rereading objects. Otherwise recheck
the same complete receipt/read-plan identity, manifest, job/harness, current
pending authority and deletion fence. A changed receipt, revoked or replaced
authority, expired pending lease or conflicting canonical state publishes
nothing. Prepared bytes are evidence about source only, never permission.

Fold each final event, immutable boundary and current prefix in the existing
Serializable transaction with canonical Run/Attempt/reservation finalization,
TerminalAck and existing protocol updates. Preserve original output, manifest,
usage, cancellation and terminal time semantics. Each SDK callback resets its
temporary result and fold cache; operation counters survive retries. Retain
owned preparations only for this operation and release them on every return.
An ambiguous database response may recover through exact canonical replay only
if the same operation still has a permitted callback and statement/byte/time
budget. Recovery consumes the shared allowance; it does not reset the deadline
or create a third commit/recovery callback. If exhausted, return unavailable
without a speculative TerminalAck. A later authenticated operation performs
the normal read-only committed replay with zero object Opens and no new fold.

## Proposed operation ceilings

These are conservative initial engineering ceilings, not measured latency,
memory, request-unit or billing claims. Native maximum-case and failure tests
must establish that supported receipt paths fit before acceptance. If a valid
supported maximum does not fit, amend the contract with its complete accounting
before release; do not silently skip checks or call the operation bounded.

| Resource | Proposed ceiling and accounting |
| --- | --- |
| Overall lifetime | 30 seconds or the caller's earlier deadline, spanning preflight, reads, commit and recovery. |
| DB callbacks | At most two preflight and two commit-or-recovery callbacks total. A recovery is charged to the latter pool; it is permitted only while that pool and all whole-operation budgets remain. No hidden outer restart loop. |
| Statements | 1024 per callback, 4096 across the operation, including inherited authority, ledger, Run, lifecycle, event and prefix work. Count every Exec/Query/QueryRow before dispatch. |
| One private persisted record | 512 KiB plus one detection byte, selected by bounded SQL projection before JSON decode; projected scalars are charged too. |
| DB returned material | 8 MiB per callback, 32 MiB across the operation; actual projected lengths plus fixed scalar widths count before decode, including failed/oversize reads. |
| Envelope reads | At most 64 Opens for the entire operation; no per-SDK-retry reread. |
| Verified envelope bytes | At most 1 MiB per inspected envelope, 4 MiB actual bytes total plus at most 64 oversize-detection bytes. Oversize projection remains UNKNOWN without opening that object. |
| Owned prepared material | Existing 64 preparations, 4 MiB total and identity/association ceilings from context contract 0.1.5; these do not replace DB or object budgets. |

All variable rows need explicit bounded projections or a bounded closed ledger
shape. A LIMIT on row count or HTTP payload does not cap bytes. Reuse current
MaxAttemptMessages = 64 ledger cardinality, including an excess detection row,
and reject excess without decoding that row. Missing budget instrumentation on
an inherited helper prevents positive operation readiness; do not change the
global Store.Transact defaults to hide that gap. The operation-specific wrapper
owns callback/deadline and byte/statement charging.

The supported bounded profile must enumerate READY success with and without
artifacts, failure and cancelled output, all 64 planned envelope slots,
maximum permitted manifest/ref metadata, a legitimate connection rebind,
committed/retired/historical replay and maximum retained 64-message ledgers.
Existing ProductLimits.MaxArtifacts, BlobRef.Key and Artifact.MediaType do not
establish a universal metadata ceiling. The profile therefore requires each
authorized persisted record and all returned material to fit the declared
ceilings; HTTP limits alone do not establish this condition. An oversized
legacy record is a source-budget failure before decode, with zero object reads
and no commit/ACK. There is no unbounded legacy fallback inside this operation
and no claim that every historically valid output fits. This is distinct from
an oversized envelope projection whose validated authority remains usable
with UNKNOWN material facts. Ordinary nonreceipt behavior is unchanged.

Before implementation acceptance, inventory and charge every helper used for
owner membership and transaction time; worker, connection and attempt heads;
manifest/protocol authority; Terminal/TerminalAck ledger and provenance;
receipt; job/harness; Run/Attempt/reservation and existing finalization;
Session/deletion and current/anchor material; and every canonical event, fact,
finalization, message, presence/deadline and protocol write. Maximum native tests
must cover the supported partitions, retry and recovery exhaustion. An omitted
inherited record, scalar or statement is not assumed free.

## Implementation and evidence checklist

- [ ] Add bounded read-only preflight and a private sealed prepared-commit seam;
      cover pending, committed, retired and legitimate rebind partitions.
- [ ] Add the outside-SQL coordinator and actual control API broker composition,
      retaining polling, exchange, drain and legacy nonreceipt behavior.
- [ ] Account for every inherited DB statement, projected record/scalar byte,
      callback, envelope Open/read and deadline; prove supported maximum cases.
- [ ] Test foreign scope/key, mutated full receipt, forged hint/preparation,
      receipt replacement, deletion/revocation/expiry after read, unsupported and
      oversize UNKNOWN, missing/corrupt objects, and reader close/clear failures.
- [ ] Test exact duplicate/retired replay with zero object Opens, concurrent
      winner, callback retry, rollback and lost ACK with unchanged canonical
      event/prefix/TerminalAck counts and original output.
- [ ] Run isolated native real transactions and proportionate race/shuffle
      regressions, independent exact-artifact review, current exact-head CI and
      guarded integration. Preserve the full #142/#155 remaining checklist.

This slice does not implement snapshots, selector/send/admission/UI, token or
cost policy, provider activation, managed compaction or the joined MVP proof.
It authorizes no deployment, shared migration, live credentials or paid call.
