# Owned-loop terminal output and retained custody

Version: 0.1.1, independently reviewed engineering contract. Implementation
and exact-profile activation remain incomplete and separately verified.
Owners: [#191](https://gitcode.com/urandon/sessionless/issues/191) canonical
exports, [#192](https://gitcode.com/urandon/sessionless/issues/192) compaction
custody/reuse and [#212](https://gitcode.com/urandon/sessionless/issues/212)
local composition. Parent [#176](https://gitcode.com/urandon/sessionless/issues/176).
This clarifies the producer and lifecycle assumptions in
[compaction](owned-harness-compaction.md) and
[configured local inference](configured-local-inference.md); it does not
replace either MVP route or enable a profile.

## Existing gap and selected boundary

The [original local session](../../internal/providerloopsession/local.go)
returns authenticated operation observations, not canonical output identities.
[Manager](../../internal/worker/manager.go) creates canonical drafts from
`ExecutionResult` after losing their operation provenance. The existing
[terminal transaction](../../internal/ydbstore/worker.go) and
[replay digest](../../internal/ydbstore/canonical_finalization.go) remain the
sole canonical finalizer; the digest does not yet seal derived metadata.

`PreparedExecutionV1`, Registry and Manager require image/process
`SubstrateExecutionEvidenceV1`. Its
[local-development rejection](../../internal/domain/serverless_execution_evidence.go)
is correct and must remain. `LocalV1.Close` does not durably retire an
allocation or erase its namespace. The existing
[residual V1 contract](../../internal/ports/owned_operation_residual.go)
requires measured usage and is not an implemented local erasure producer.

Factor one original output composer and terminal sequence from these existing
owners. No second worker, invocation ledger, scheduler, success finalizer,
caller approval callback or new table is introduced. Keep the managed V1
interfaces unchanged; select an explicit V2 composition only for registrations
that implement the complete successor below. No nil arm or V1 coercion.

## One reservation and execution-result successor

Manager reserves the physical effect before preparation. Direct local Start
currently reserves it again inside `StartLocalInvocation`. Factor the original
bootstrap's post-reservation entry: direct Start first obtains the original
confirmed-fresh result, while the V2 worker preparer hands that same Store's
already obtained result to the same private consumer. The consumer verifies
the original issuer-authenticated fresh grant, exact Store/bootstrap identity,
one-use preparation burn, Source, body adapter, allocation and original
deadlines. Recovered, replayed, ambiguous or foreign results cannot enter.
A copied DTO or caller Boolean cannot mint the private consumer ownership.

The V2 preparer/Registry/execution/Manager/completion boundary has a closed
tagged `execution_evidence.v2` contract: `version` is 2, `kind` is `managed_v1`
or `local_v1`, and exactly the corresponding `managed_v1` or `local_v1` member
is present. The managed arm contains the unchanged V1 evidence and follows its current
validation and provider-evidence checks. The local arm is admitted only for
`local_development` plus the authenticated owner-local installation; it cannot
claim image/process isolation, cloud metering or stopping the owner's model.

Execution first returns a private prepared-output handle, not completed local
evidence. It is created only by the original entered owner after freezing its
frontier. Manager's factored composer/uploader uses that handle to prepare
output before the owner finishes cleanup. The handle exposes no dispatch,
re-open, reserve or caller-selected digest/signing method. Copies share its
one-use state and original absolute cleanup deadline. It cannot be restored
from persisted JSON. Existing V1 execution remains on its existing path.

The local arm's persisted fields are exactly: `version` (1), original
`scope`, `parent_digest`, `source_digest`, `profile_digest`,
`original_allocation_digest`, `retired_allocation_digest`, `frontier_digest`,
`capsule` (BlobRef), `residual_inventory_digest`, `residual_receipt_digest`,
`accounting_disposition_digest`, `stopped_at`, `retired_at`, `erased_at` and
`evidence_digest`. Times are UTC microseconds in observed phase order. This is
content-free evidence, not permission. `evidence_digest` hashes the other
fields with a domain-separated versioned encoding. The actual private owner
must authenticate the local arm against its joined keeper, durable retirement
and physical-erasure receipt before its first terminal commit; structural
validation or a caller-supplied matching hash is insufficient.

The existing evidence record stores the explicitly tagged successor JSON under
the same tenant/Attempt/claim and digest keys. Do not reinterpret an old V1 row
as V2 or create an alternative decoder. Reuse the existing transaction and
lease/fence checks; isolate the current managed V1 writer from the V2 writer.
The local arm never invents `ProviderExecutionEvidenceV1` to satisfy V1.

## Private terminal capsule

Choose one immutable private Run-owned object, not an ordinary
`ArtifactManifest.Artifacts` entry: that manifest has no private visibility.
Putting operational material there would publish it with the assistant answer.
Do not append late output to an already published immutable compaction
manifest. That has the wrong lifecycle and invalidates its selected digest.

The capsule is a closed JSON `sessionless.terminal-custody.v1` object. Its exact
top-level fields are `schema`, `scope`, `profile_digest`, `codec_version`,
`frontier_digest`, `operations`, `derived_context`, `events` and
`public_manifest`. No raw request, native transcript, auth header, credential,
hidden reasoning, private transport error or workspace pathname is retained.
The object has at most 12 operation entries and 25 canonical event identities;
the public manifest obeys its existing admitted artifact count. The complete
capsule is at most 512 KiB, including metadata and JSON escaping. An operation's
normalized body is at most 64 KiB; all normalized bodies together are at most
384 KiB. Tighter existing profile/materialization limits still apply.

Each operation entry has exactly `reservation`, `observation`,
`normalized_body`, `projection` and `event_ids`. Reservation and observation
are their strict existing domain values, not reduced identity guesses.
`normalized_body` is the exact bounded normalized JSON previously authenticated
by that observation. Its bytes/digest must match the observation; entries are
the complete original causal frontier, not an arbitrary selected subset.

`projection` is a closed disposition:

- `assistant`: only the actual terminal MODEL's visible answer; exactly one
  canonical assistant event is produced by the original composer.
- `tool_pair`: the original typed composer binds the MODEL's declared call and
  the actual authenticated tool result to their exact canonical events. Call
  ID, ordinal or matching text alone never supplies the join.
- `retained_only`: completed intermediate MODEL material that contributed to
  the derivation but emits no separate assistant event; `event_ids` is empty.
- `compaction`: generation provenance/lineage only; `event_ids` is empty.

The capsule's `events` are the exact finalizer event identities (ID, kind,
idempotency key, payload BlobRef and display text). All assistant/tool bodies
are the exact encoded canonical payloads prepared by the same composer; its
strict payload codec and immutable readback establish their digest. Retained
operation IDs and canonical IDs remain disjoint. Compact Run correspondence
binds the capsule, original frontier and terminal assistant anchor; that anchor
attests derivation, NOT byte equality of every intermediate answer.

Before capture, the producer uses each original request and trusted profile to
execute the existing normalized decoder and validate tool schemas/correlation.
After erasure a new retained-material codec validates the closed normalized
representation, exact reservation/observation/body association, dispositions
and terminal seal. It does not call the old request-dependent decoder without
its request or claim request-free structural parsing establishes authenticity.
The successful original finalization seal is what preserves the verified
derivation. A tool codec without a concrete original typed producer is not
eligible for transfer; this does not waive #191's required tool integration.

## Run ownership and budgets

Add one optional `terminal_custody` member containing typed V1 metadata to the
existing Run payload, adjacent to derived context. It works also for a Run with no
compaction. Its exact fields are `version`, `plan`, `correspondence` (optional)
and `local_evidence` (optional, required only for the local completion arm).
`plan` has exactly `scope`, `capsule` (BlobRef) and `frontier_digest`.
`correspondence` has exactly `version`, `capsule_digest`,
`terminal_assistant_event_id`, `canonical_events_digest`,
`derived_context_digest` and `accounting_disposition_digest`. The event digest
binds ordered canonical event identities. The derived-context digest hashes
`sessionless.terminal-derived-context.v1\x00` followed by the canonical JSON
encoding of the complete validated derived-context value; no compaction is
canonical JSON `null`, not an omitted or empty substitute.
Correspondence contains neither its own nor the final
finalization digest. No response text lives inline in Run metadata.

One dedicated original Store publisher registers the exact plan in its
transaction BEFORE PUT. Its call-wide irreversible first-write-attempt guard
is claimed before the first physical mutation and survives SDK transaction
callback retries. No second mutation attempt is allowed through that owner.
Only unambiguously confirmed commit plus exact readback permits the original
one-use uploader to proceed. Unknown registration means no PUT, even if a later
callback/read observes an identical plan or the first write never persisted.
Later reconciliation can discover cleanup identity, not upload authority.
One immutable slot per Run includes failed/unpublished
plans; a replacement Attempt cannot reset it or spend another upload. A
conflicting replacement stops explicitly rather than deleting old residue or
retrying an uncertain PUT. Generic Run writers preserve this complete value
and all derived metadata; only the dedicated original finalizer installs
correspondence. The common physical Run-write boundary enforces the existing
complete 32 KiB ceiling before mutation, including later status writes.

The fixed key is `SessionRunObjectPrefix + terminal-custody-v1/ +
scope_hash + /sha256/ + BlobRef.SHA256 + .json`. `scope_hash` is SHA256 of
`sessionless.terminal-custody-scope.v1\x00` followed by canonical JSON of the
complete operation scope. The content segment is the raw SHA256 of the exact
capsule bytes, identical to `BlobRef.SHA256`, following the existing retained
object-key convention; it is not a second hash of that hash. The fixed versioned
prefix supplies the object-kind namespace. Registration
stores that exact reference/size/digest, never an arbitrary caller path.
The object is not another derived summary/manifest pair and cannot consume or
reset their two-pair/four-write allocation.

Use the original owner's I/O gate and shared allowance. Reserve one capsule
PUT attempt and one bounded verification readback; there is no automatic PUT
retry, replacement key or detached uploader. Count the capsule and all planned
summary/manifest objects together with visible output against
`ProductLimits.MaxArtifacts` and `SubstrateBindingV1.Limits.ArtifactBytes`.
Charge actual encoded upper bounds before their writes, including failed or
uncertain attempts. Reserve the capsule's maximum admitted envelope before
the first model call; the selected policy may choose a smaller envelope, but
must seal it at admission and reject later overflow without truncation.

The capsule copy additionally uses the original operation adapter's
`MaxAggregateBodyBytes`; it cannot bypass that pool as a public output. Reserve
its admitted encoded allowance before any operation body writes; retained
pairs retain their existing prepaid allowances. Existing per-read byte/time,
SQL scan and materialization bounds also apply to capsule verification. The
new shared accounting owner is a factoring of original adapter/uploader
ownership, not another quota or refund ledger. Missing count/byte capacity
denies before effects; do not increase profile limits automatically. Canonical
event payloads retain their existing independent event-count/byte limits.

## Irreversible terminal phases

1. Under the original session gate, latch terminal entry irreversibly. All new
   preparation, reserve, tool/model send and compaction publication reject
   through every session/handle copy. Capture the complete known-effect
   frontier. An unobserved/unknown operation prevents successful finalization.
2. The original composer validates that frontier and selected retained closure,
   prepares exact canonical payloads/public outputs and the private capsule,
   charges/registers its plan, then PUTs and verifies retained readbacks.
   Missing/uncertain transfer causes ZERO original-body deletions.
3. Stop and join owned keeper connections, gates and in-process allocation
   using one original absolute cleanup deadline independent of the cancelled
   active Source. Do not stop the borrowed model server, Source/DB owner or
   worker host. A join timeout is not a successful stop receipt.
4. CAS the existing registered allocation from its exact active digest to
   retired. Preserve historical snapshots and authenticate them; neither the
   close method nor a retired DTO alone proves this durable transition.
5. The original concrete namespace owner verifies the retained dependency
   closure, immutable plan, prepared drafts and complete original body
   inventory. Fence that namespace, delete only its exact request/response
   objects and verify absence. Retained capsule and derived pairs are outside
   that erasable namespace. Partial deletion/read/close/absence uncertainty
   taints the owner and permits only bounded exact-inventory reconciliation,
   never a provider resend or a fresh erasure plan.
6. Produce authenticated local terminal evidence from actual stop, retirement,
   custody and erasure receipts. The unchanged managed arm retains its own
   independently proven execution/cleanup evidence.
7. The SAME canonical terminal transaction requires the current scheduler
   lease/fence and original persisted Run/Attempt/reservation, verifies the
   private prepared package against historical retired allocation and exact
   retained facts, and atomically commits events, output manifest, terminal
   states, complete metadata and `run_finalizations` digest. No active-send
   allocation predicate is reused after retirement. No legacy completion path
   or `requireEvidence=false` bypass is permitted.

The private completion owner may finish cleanup after caller/Source cancellation
but cannot renew the deadline, restore execution or claim canonical success.
Cancellation visible at the terminal transaction uses the existing cancelled
outcome. The capsule above is a SUCCESS candidate only. Late cancellation or
failure never rewrites it, commits its assistant drafts or installs success
correspondence. Its registered plan and derived inventory remain cleanup
residues; `correspondence` and `local_evidence` stay absent. The existing
failure/cancellation composer prepares exactly one bounded SystemNotice and
uses the original failure transaction, sealing the preserved planned metadata
but asserting neither successful transfer/reuse nor unverified cleanup.
Reserve this separate bounded notice capacity under existing canonical-output
limits before execution; it is not another capsule or an extra assistant.
The original cleanup owner still joins/retires/reconciles its actual work;
failure to finish cleanup is reported honestly and cannot authorize reuse.
Lease expiry/loss prevents a first success commit; retained planned objects
remain owned reconciliation/deletion residues.

Session deletion coordinates registration, physical writes and writer join:
the existing Session deletion-admission transaction fences new registrations,
execution and terminal appends, then pages every owning Run/registered plan.
It cancels and joins the ORIGINAL admitted writers before deleting their exact
objects. Registration and deletion admission serialize on that same Session
fence. A pre-PUT status check alone is insufficient. Original PUT owners must
recheck the fence at I/O admission, remain registered until actual join and
never retry after ambiguous registration or upload. Lost/unknown writer state
or an ambiguous in-flight PUT keeps deletion incomplete/reconcile-only until
its exact physical commit or non-effect is established. Expiry, a cancelled
HTTP request or an absent object alone cannot prove no later write. No success
receipt may be inferred from a caller's writer-stopped flag. This narrow
writer/plan join belongs to the existing paged deletion workflow, not a new
upload service, table or general upload-management release gate.

## Accounting, digest order and recovery

Preserve residual inventory V1 unchanged. Its explicitly selected V2 successor
has the same full scope/frontier/body/transport/retired-allocation inventory,
plus `accounting_disposition`: original immutable aggregate debit, four
component allocations and exact usage per operation. Closed local provider
dispositions are `measured`, `provider_reported_reserved` and
`unknown_reserved`; only the first claims measurement. The latter two retain
their entire original worst-case reservation, without refund or reusable
credit. A provider invoice of zero requires the authenticated original
owner-supplied no-invoice terms, revision/digest and expiry. It is not inferred
from no-auth, localhost or missing measurement. Other tool/substrate components
keep their own original provenance and amounts; host/economic cost is not zero.

Digest order is acyclic: exact normalized material/canonical payload identities
and public manifest -> capsule -> compact correspondence and actual residual
receipt -> local execution evidence -> finalization seal. Hash local evidence
without its `evidence_digest`; seal the complete Run metadata, both derived
inventory/head and terminal custody, in the existing replay digest. The capsule
contains neither a future cleanup receipt nor its own/finalization digest.

Historical verification is explicitly seal-attested digest-only for the
cleanup receipt and accounting disposition. Their original bounded material
is verified by the private original producer BEFORE the first commit; their
digests, not their complete material/signatures, are retained in local evidence
and correspondence. Original reservations/observations in the capsule retain
the usage/debit facts needed for context/provenance. A later reader does not
recreate an erased receipt, independently re-run its MAC verification or claim
that a digest itself proves physical deletion or measured cost. The successful
canonical terminal seal attests that original verification occurred.

Exact lost-ACK replay recomputes that seal from retained capsule, persisted
terminal metadata and existing marker, without reading erased bodies or
requiring an expired Source/Attempt to become active. A changed/missing capsule,
head, event, manifest or recorded receipt/disposition DIGEST conflicts; absence
of the original private receipt after commit is expected, not permission to
reconstruct it. Before a first
commit, missing original private ownership cannot be reconstructed from those
records; leave the effect reconcile-only, not a fabricated success.

A fresh Run selects a successful predecessor only through terminal events in
its newly admitted canonical window. Verify the producing Run/terminal seal,
capsule, canonical identities, original source correspondence and complete
retained closure; recheck current tenant/owner grants, model/profile/policy and
concrete fit. Rebuild from retained normalized material without duplicating
its canonical projection or reviving old sends. Resolve dependencies directly
to original producing Runs, not recursive borrowed-capsule chains. The entire
derived summary closure still has at most two generations across Runs; local
generation numbering never resets that traversal bound.

Session deletion pages owning Runs and enumerates each registered capsule and
both planned summary/manifest pairs, including failed/unpublished plans, plus
their exact original dependencies. Reuse/head replacement does not release
ownership. Private capsule lookup is internal tenant/Session-authorized data;
it is absent from public artifact APIs, transcript payloads and progress logs.
No cross-Run reference counter or general garbage collector is added.

## Implementation handoff and decisive checks

These are existing implementation owners, not new parallel epics:

- [ ] #212: same-Store fresh post-reservation entry and explicitly registered
  V2 local execution/preparer/result/evidence composition; V1 rejection tests
  remain unchanged. This precedes local terminal integration.
- [ ] #191: one authentic original output composer and factored existing
  uploader/finalizer, private prepared terminal handle, shared output bounds
  and original lease/fence atomic success/replay transaction.
- [ ] #192: strict capsule/Run metadata/registration, concrete namespace
  custody and V2 accounting disposition, terminal correspondence/replay seal,
  post-erasure fresh-Run reconstruction and complete deletion inventory.

Original-requirements-first independent review must check every phase and the
actual producer, not only codecs. Joined tests execute original session -> two
compactions -> composer -> stop/retire/custody/erase -> one canonical assistant
and exact replay -> a NEW Run reconstructing after body absence with zero old
effects. Include fitting/no-compaction runs, tool-pair continuity, foreign/stale
authority, copied/replayed reservation, late cancellation/fence/lease loss,
corrupt capsule/parent/events, incomplete frontier, failed PUT/readback,
partial erasure, shared count/byte/32-KiB overflow with rollback and all
failed/unpublished deletion residues. Use deterministic owned peers and the
repository testing contract; fixture SQL is not native-YDB evidence, real
model qualification or cloud E2E. Production registration, live models,
credentials, YDB runs, migrations and deployment need their separate authority.
