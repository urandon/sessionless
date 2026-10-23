# Entity registry and storage decisions

The [entity registry](entity-registry.json) maps physical application tables to
their creation migrations, observed keys, schema retention and source consumers.
The v2 registry keeps **105 historical CREATE records** and derives **101
effective active tables** after four forward retirement definitions. These are
source-layout counts, not an assertion about any retained deployment.
Logical state and durability decisions are specified in the
[state storage simplification design](../design/state-storage-simplification.md).
The owning [issue #205](https://gitcode.com/urandon/sessionless/issues/205) holds
the report, problem register, review findings and implementation decisions.

An authenticated exact-query consent view is transient private data derived
from the existing consent state and its immutable scoped argument body. It has
no new row, table, TTL, cache, approval registry or custody owner. The original
consent transaction owns decisions; the existing Session deletion inventory
continues to own their argument objects. A browser view grants no search send
or replay authority and is never part of public execution evidence.
Private pending discovery point-reads the existing canonical Session, Run and
Job/Attempt mapping plus the same bounded consent projection. It stores no
discovery index or Run lifecycle field and exposes no query/body reference.

Consent argument objects use one closed, versioned key arm within the original
operation-body owner's Attempt scope. They share its I/O gate, prospective
write/byte charges and physical inventory; they are not a second store, ledger
or erasure authority. Ordinary keys and namespace/inventory digest formulas
remain unchanged. The joint policy-derived ceiling stays at most 24 keys, so
the producer must reject an unrepresentable sequence before effects. Physical
custody compatibility alone does not establish consent-aware terminal accounting
or retained-history deletion: those original consumers stay fail-closed until
their genuine integration is proved. See the
[argument-custody contract](../design/owned-harness-web-search.md#original-owner-argument-custody).

The genuine search resource-grant reference belongs in the existing
installed profile as one optional explicitly versioned body-reference arm. Its
original operator source owns immutable bytes and authentic publication; raw
body SHA is distinct from the semantic resource-grant digest. Invocation scope
already belongs to the existing policy, spending/source, parent and operation
records, not another grant table or per-Attempt profile rewrite. Old nil-arm
profile bytes remain unchanged. Source publication alone supplies neither
current database authority nor runtime dispatch permission. See the
[resource-grant contract](../design/owned-harness-web-search.md#reusable-resource-grant-and-original-invocation-scope).

`OwnerLocalSearchExpenditureV2` and closed owner-accepted price/terms snapshots
are nonsecret declarations within the same private installation manifest and
bounded original Source holder. They are not new YDB records, a payer registry,
an approval map or a balance. The original source files/key and exact publication
receipt authenticate reconstruction; a copied body reference cannot reconstruct
the keeper. Accepted evidence identities do not prove vendor credit, current
tariffs or legal interpretation. Source/config/profile/grant/expenditure/snapshot
and invocation limits bound use; existing per-Attempt spending/current/audit
records retain the durable allocation facts and no-top-up fence.

An entity is not a table. A canonical Session may have several rebuildable
lookup projections; a current execution record may contain several bounded
operational fields. Separate physical storage requires a concrete access,
lifecycle or transaction reason, not one table for every state or proof.

The [derived-compaction design](../design/owned-harness-compaction.md#durable-head-and-terminal-correspondence)
selects one bounded typed head and terminal correspondence in existing Run JSON,
with immutable summary/manifest bodies in scoped object storage. The typed
head/inventory codec and generic Run writer preserve this storage envelope;
the dedicated publisher, terminal correspondence and retained-object lifecycle
still require their owning implementation. There is no new table or applied
schema change. Attempt/checkpoint TTL cannot own fresh-Run context retention. The dedicated
publisher must preserve current-authority/CAS checks; generic Run writers must
preserve the head exactly, and canonical finalization must seal its terminal
mapping into the existing replay digest. Session deletion owns the bounded
object closure. Retained summary/manifest pairs use a distinct Session/Run
namespace, with verified custody before the original operation namespace is
erased. The same Run metadata inventories at most two exact planned pairs
before upload, including failed/unpublished objects; registration grants no
send, retry or reconstructed write authority. Generic writers preserve the
complete metadata. Each producing Run retains its pairs until Session deletion;
head replacement does not require a reference-count table or unsafe early
garbage collection. Common Run writes enforce the complete 32 KiB payload
ceiling, and common point reads reject larger payloads before decoding. The
WebUI explanation projects only its base Run fields within the existing
8 KiB read budget; that read-only projection must never feed a writer.
Physical table counts and existing TTLs remain unchanged.

The same Run payload owns optional typed `terminal_custody` metadata: one
immutable private capsule plan, including failed/unpublished residue, and
success-only correspondence/local evidence. Generic writers preserve this
complete value together with derived inventory/head. The capsule is not a
public `ArtifactManifest` entry or another derived pair; Session deletion owns
its exact registered object. Structural validation does not prove original
upload, stop, retirement or erasure. Their dedicated producer and terminal
replay seal remain required; no table, TTL or complete 32 KiB Run bound changes.

The local completion successor uses that same Run and the existing
`substrate_execution_evidence` row, with explicitly tagged V2 evidence; it does
not reinterpret managed V1 rows or add an execution table. Its full original
debit/components and reported-or-unknown usage remain reserved, not refunded
or presented as measured cost. The canonical finalization transaction owns
the Run/Attempt/quota, event, manifest, evidence and replay marker together.

Before fencing the ordinary body namespace, the original cleanup owner also
uploads one deterministic prepaid cancellation SystemNotice body. This object
is separate from the success capsule's event list. The owning registered Run
must retain its exact cancellation reference for deletion/reconciliation even
when success commits or no terminal event commits. Late cancellation selects
only that notice, never assistant drafts or success correspondence/evidence.
The fixed cancelled notice cannot represent another failure. The conservative
deletion inventory must include these residues, not just visible output or the
selected retained Head. Their registration does not prove stopped writers.

The [writer-aware deletion contract](../design/retained-history-deletion.md)
reuses `session_deletions.record` for one content-free, at most 2 KiB
`retained_verification` commitment. It binds the immutable original request,
full SQL postimages/ranges and exact reconstructable object inventory; it is
not a new table, upload capability, scheduler or per-object delete ledger.
The tombstone and commitment survive completion for bounded audit evidence.
Current principal/membership and legal hold remain separate live checks.

The bounded table/postimage catalog is in
[`session_deletion_capture_sql.go`](../../internal/ydbstore/session_deletion_capture_sql.go);
[`session_deletion_capture.go`](../../internal/ydbstore/session_deletion_capture.go)
joins existing owners and rejects absent/foreign/mismatched scalar and JSON
facts. Original per-Run verification work and historical writer/body checks
remain transient, not additional persistent entities. Trusted lifecycle
composition in
[`session_deletion_verified.go`](../../internal/ydbstore/session_deletion_verified.go)
may install a commitment only after successful body verification and final
SQL rejoin; retries reconstruct it from SQL without resurrecting deleted
bodies or granting fresh effects. Exactly proven admission/parent/operation
scopes retire in the same canonical completion transaction, not at Plan or
Request. Uncertain writers remain blocked. Neither status nor Object Storage
absence proves that an earlier PUT cannot complete late. See the
[explicit operator opt-in](../session-lifecycle.md#retained-successful-local-loop-history).

The original operation-body adapter reserves 160 KiB per retained pair before
generation: at most 32 KiB for the encoded summary artifact and 128 KiB for its
manifest. At most two pairs share the original aggregate byte allowance and
I/O gate; copying a handle cannot restore capacity. These process-local slots
are nonrefundable and are not durable entities or publication authority.
Uncertain writes disable further writes while preserving reconciliation reads.
Run inventory registration must commit before either retained PUT; a new
process cannot reconstruct write ownership from that inventory. Semantic
summary validation, head publication, terminal custody and Session deletion
remain separate parts of the retained-object lifecycle.

## Logical entity groups

The following groups map the first reviewed questions to physical storage.
They overlap where a table contains both current and historical data; a group
is not a request for another table. The complete physical list remains in JSON.

| Logical entity | Physical mapping | Required or proposed durability |
| --- | --- | --- |
| Canonical conversation and useful output | `sessions`, `session_events`, `artifact_manifests`, `run_finalizations` | Durable acknowledged content and exact finalization; large immutable bytes have separate storage ownership. |
| Accepted Run identity and cancellation | `runs`, `run_idempotency`, `runs_by_session` | Durable accepted identity/cancellation; the lookup projection has a separate reconstruction/lifecycle decision. |
| Current execution | `attempts`, `lease_heads`, `leases`, `worker_jobs` | Current owner/fence/expiry and admitted input remain transactional; a merged layout needs replacement of existing joins. |
| Historical execution detail | Historical `attempts`/`leases`, `checkpoints`, diagnostics | Evaluate bounded retention, derived views or ephemeral progress separately from current execution. No blanket permanent-history requirement. |
| Irreversible effect and output custody | `attempt_effect_reservations`, reconciliation evidence, `attached_worker_output_receipts` | Retain exact accepted/ambiguous effect identity and custody until owning reconciliation/retention permits retirement. |
| Ready and expiry discovery | Four `_v2` tables; historical lineage for `lease_expiry`, `dispatch_ready`, `quota_expiry`, `telegram_delivery_ready` | Single v2 serving projection per family. Legacy entries remain as historical CREATE and optional reset-cleanup ownership, not active tables. |

The [consumer and durability study](../research/state-storage-and-durability.md)
records actual joins, default retention, old-index callers and source write
counts. The [first reduction contract](../design/state-storage-simplification.md#first-reduction-contract)
does not classify every remaining physical table. The separately accepted
[current lease within Run design](../design/current-execution-slot.md) selects
seven typed Run columns instead of `lease_heads`, retaining bounded `leases`
evidence. That replacement is not implemented: the active source inventory
remains 101 tables, including `lease_heads`. Other Run/Attempt/history and
owner/retention decisions stay open in #205.

## Configured local resource records

The disabled #216 local-inference contracts reuse
`owned_agent_profile_current_v1` and its publication audit for the real
`local_endpoint` resource. No-auth has generation zero and no credential row;
it still requires the exact original active profile/generation/digest at the
same transactional time. Bearer additionally uses the existing
`provider_credential_bindings` and credential audit with a positive generation.
The profile's sealed executable and host/network-namespace installation facts
are configuration identity, not measured cloud/container isolation. Host
identity includes the actual kernel boot UUID (Darwin `kern.bootsessionuuid`,
Linux `/proc/sys/kernel/random/boot_id`), not just hostname/UID. Same-boot process
restart can resume; reboot or host transfer requires explicit reconciliation
and requalification, never automatic fresh installation or old-receipt reuse.

Canonical local-profile data alone cannot install or authorize the resource.
The local publication intent binds the exact configured profile and, for a
replacement, the original predecessor receipt plus a distinct retirement
mutation. The existing serializable publication writes both locator heads
(retained predecessor tombstone and absent successor) with both immutable audit
rows under one complete intent digest. Replay reads both exact intents and
retained heads without rewriting either; a partial pair or unknown commit
returns no usable receipt. These records add no execution permission by
themselves: fresh IDs and source/terms authentication remain producer-owned.
The owner-local source keeper reads an explicitly selected bounded private
manifest and separate signing key through owner/mode-checked retained file
descriptors. It mints fresh resource/installation/mutation IDs internally;
resume requires the original source/key/configuration and exact receipt, and
cannot create a new head. Replacement additionally requires the predecessor
keeper and the existing paired CAS. The key, retained declaration bytes and
pending grants are process-local, not a new database entity or credential row.
The manifest fingerprint and source reference are nonsecret canonical identity;
the signing key is distinct from any inference Bearer credential and is never
serialized into profiles, grants or audits.

`owner_local_installation` spending references this exact installed profile and
its original finite terms. The keeper can authenticate only the explicitly
owner-supplied provider/substrate zero invoice; that is not measured zero
economic cost or a free external-service grant. MCP/search retain independent
source/price authority and all four current joins remain mandatory. Actual
declaration bodies do not establish model qualification, independent review or
execution permission. The complete effect-source producer, tagged connector
and normal composition remain disabled until their owning contracts pass.
Existing head/audit/source/operation tables hold the necessary
facts; no endpoint inventory, zero-generation secret row or operational ledger
is introduced. Applied migration history and physical table counts do not change.

Owned connector observations use the canonical tagged
`RegisteredProxyAttestationV2` record. The first observation still lives in the
existing `attempt_effect_reservations` original/current/body columns; later
sequences still use `attempt_operation_proxies_v1`. The table suffix is not a
record-codec fallback. V2 seals the original operation digest and sequence, with
exclusive pinned-HTTPS, local MODEL-loopback, or local public-MCP HTTPS facts,
and is reread by reservation,
before-send and history/custody consumers. No new endpoint or observation table
is needed. Retained V1 rows are not rewritten or promoted into V2 authority;
their presence blocks unsupported serving/recovery and needs the separately
authorized [drained format cutover](../serverless-egress.md#owned-observation-format-and-cutover).
The explicit `local_public_mcp_https.v1` arm binds the original local runtime
installation/profile/host/namespace to the independent retained MCP grant,
descriptor, fixed endpoint, public-address resolution and verified TLS peer.
It does not reinterpret MODEL loopback as a tool proxy. Its stateless POST-only
protocol counters remain bounded process-local execution state; durable
operation reservation/observation and exact proxy bodies reuse the records
above. Missing local state cannot authorize protocol or tool replay. This
component enables no real profile; unknown service money remains unknown and
cannot authorize MODEL continuation. No table, index, migration or operational
progress ledger is added.
Local observation data cannot be relabelled as an image-backed allocation or
replace the actual retained local connector.

Canonical `PreparedAllocationV2` and `RegisteredAllocationV2` preserve the same
allocation original/current/intent columns in `attempt_effect_reservations`.
The explicit local runtime body seals the actual executable/installation/host/
namespace correlations and exact configured owned-IO limits, not an image,
cgroup or whole-host memory claim. The original short preparation/first-entry
expiry remains a separate immutable field. A local allocation's active window
is derived once from original invocation/active/source/funding limits, not
extended from publication time; image-backed allocations keep their original
prepared-expiry ceiling. Current joins reproduce the exact original authority.
The concrete retained producer, entered-capability bit, context, nonce and
buffer/connection ownership stay process-local. Losing them cannot reconstruct
execution permission from these rows. No new table, token store or ledger is
introduced; old V1 allocation records require the same separately authorized
drained cutover, not reinterpretation or migration-history rewriting.

Provider token reports use the existing `attempt_operations_v1` immutable
observation JSON and digest, with explicit `provider_reported` provenance rather
than claiming measured usage. Existing `observed`/`unknown` record encodings are
unchanged. No reported money, refund, new column, ledger or migration is added.
Known model/compaction outcome may continue with unmeasured usage only under the
existing full worst-case reservation and all original authority/lineage checks;
reported or measured overruns stop work without discarding their evidence.
Completed tool/search predecessors retain their measured-money gate. Residual
disposal/retention and finalization remain separately owned; this continuation
rule does not qualify their measured-usage requirements or retire any facts.
See the [provider-usage clarification](../design/owned-agent-harness-minimum.md#provider-usage-provenance-and-conservative-continuation).

Operation bodies reuse BlobStore objects under the existing session/run prefix,
with `attempt-operations-v1/<full-scope-digest>/opbody-v1.<bytes>.<sha256>` keys.
The canonical operation rows retain their existing reference/digest/size fields;
there is no body index table, transcript or durable adapter accounting. The
complete claim scope separates object namespaces and every read rechecks exact
size/hash. Local adapter write counters bound physical I/O only and cannot
restore Store quota after reconstruction. Ambiguous writes retain possible
objects and stop new writes. The original adapter additionally keeps at most
24 distinct attempted ordinary references in process memory, before each PUT,
including bodies not referenced by a durable operation. Its one-use cleanup
fences the same shared I/O gate, verifies all known bodies and deletes only
those exact references under one original deadline. An ambiguous PUT blocks
erasure even if the object currently appears absent. Exact absence requires
an explicit storage capability; S3 uses one bounded, non-redirecting HEAD and
accepts only an authenticated not-found response, not arbitrary read errors.
This inventory and its opaque original-owner receipt are not another table,
quota ledger or durable proof after process loss.

The bounded search compiler introduces scoped `WebSearchGrantV1` and
`WebSearchResultV1` data, not new tables or a search-owned transcript. Frozen
argument/wire buffers and decoder state are process-local and reconstruct no
send permission. Once the original publisher/operation integration is delivered,
grant custody is correctness-critical authority data, while exact arguments,
physical request and normalized result use the existing authorized operation
body/observation references and retention/deletion closure. A retained hash or
serialized result is not authenticated observation or continuation authority;
lost or erased source cannot be recovered by searching again. The current
data-only codec publishes no records and changes no table, TTL or migration.
See the [search contract](../design/owned-harness-web-search.md#distinct-argument-and-physical-request-commitments).

The optional `AttemptOperationSearchWireV1` encoding belongs inside the existing
`attempt_operations_v1` reservation record and its original digest/call index;
argument size and compiler/profile commitments are not a new grant or ledger.
It adds no row, object reference, TTL or deletion owner. Nil-arm historical bytes
stay unchanged; strict readers do not turn retained data into send permission.
Until the authentic search producer is installed, the original Store denies
fresh new-arm reserve/send before generic evidence materialization or effects.

The original local terminal owner joins its actual keeper/lease and allocation,
uses the existing allocation retirement CAS, authenticates the historical
Run/frontier and retained capsule/canonical/derived objects, then invokes that
physical erasure. Its private cleanup record does not claim measured usage,
canonical terminal success or Session-deletion readiness. The canonical
finalizer, V2 accounting disposition and post-erasure restore remain separate
integration requirements; the conservative Session-deletion guard above stays
in force. No new durable entity or applied migration is added. See the
[scoped body contract](../design/owned-agent-harness-minimum.md#scoped-operation-body-storage).

Configured model successes use a bounded normalized body in that same namespace
and the existing observation reference/digest/size, not another response table.
Its immutable profile/request/effective-limits fields are DATA; the local
bootstrap's private original configuration clone is ephemeral decoding state,
not another current-authority registry. Record and authenticated read verify
the body and its exact usage correlation. No new retention/deletion semantics
or table are introduced; see the
[normalized body contract](../design/owned-agent-harness-minimum.md#normalized-successful-model-bodies).

The local invocation's constructor-ordering bridge is one process-local closed
reader attached to the original bootstrap, armed once with a body adapter derived
from the confirmed existing parent reservation. It adds no persisted entity,
approval registry, scheduler or quota ledger. Entry/source/adapter object identity
is not current permission or canonical retirement/disposal evidence; see the
[construction contract](../design/owned-agent-harness-minimum.md#local-invocation-construction-and-first-model-composition).

## Source and rollout boundary

The [compatible code step](https://gitcode.com/urandon/sessionless/issues/206)
removes unversioned ready/expiry writes, ordinary cleanup and the unused tenant
lease-expiry reader. Normal bootstrap backfills lifecycle indexes only; it no
longer copies the four legacy ready/expiry tables. The four legacy CREATE
records and optional historical reset ownership remain. The ordered retirement
definitions now remove those four tables from the effective source schema;
the registry still covers every one of the 105 historical CREATE tables.

Before deploying v2-only code with retained work, complete ready/expiry v2
backfill and exact tuple verification using the compatible dual-writing
predecessor, then drain its actual old readers/writers over the configured
lease/retry window. The lifecycle cutover marker is not that verification.
The predecessor handoff and rollback evidence belong in #206 execution
comments; no deployment or backfill execution is granted by a source check.

Legacy `knownRefs` entries with no serving SQL readers/writers do not mean the
physical table is absent. Reset allowlists, immutable migrations, v2 logical
partition names and test-owned cleanup are retained metadata/ownership, not
legacy scheduling consumers. Retained v2 entries distinguish SELECT readers
from UPSERT/DELETE writers. These remain bounded static samples, not a complete
runtime graph.

The [bounded operator repair](../ready-expiry-repair.md) reconstructs one selected
v2 tuple from retained, correlated sources in a serializable transaction. It
adds no table and changes no canonical parent or external effect. Its source
and output refusal limits do not establish environment rollout; deployment
handoff and physical retirement still require their own evidence. Physical
retirement and effective-schema tracking
belong to [#207](https://gitcode.com/urandon/sessionless/issues/207), under the
separate rollout authority described in the design.

## What the inventory establishes

Each record identifies a table, its CREATE migration and primary key. The
observed key scope describes those columns, not authenticated tenant or owner
permissions. Reader and writer paths are bounded static-reference samples;
their counts do not prove a complete runtime call graph or that a table is safe
to retire. A schema TTL clause is an observed storage setting. An absent TTL is
not a permanent-retention requirement; runtime deletion and retention owners
must be reviewed separately.

`durabilityReview: pending` and `proposedDisposition: review` mean a storage
decision is still open. They are not approval to drop, merge or weaken a table.
Migration-runner bookkeeping and other infrastructure databases are outside
this application-table list. ALTER-only files are migration operations, not
additional entities.

## Required logical entity decision

For each reviewed entity, record these decisions in the linked design or owning
issue, and reference them from its registry disposition:

| Field | Question |
| --- | --- |
| Authority and scope | Which actor owns it, and which tenant, user, Session or resource does it authorize? |
| Loss consequence | Is loss merely missing progress, or can it lose accepted content, repeat an effect or publish a stale result? |
| Durability | Canonical durable, correctness-critical operational, rebuildable projection or process-local ephemeral? |
| Reconstruction | Which authoritative inputs recreate it, and may reconstruction repeat an external operation? |
| Retention | What keeps it live, who retires it, and which pending custody or revocation facts forbid deletion? |
| Access and atomicity | Which bounded query or transaction needs a separate record, table or index? |
| Disposition | Keep, merge, rebuild, ephemeral or retire; proposed versus independently accepted must remain distinct. |
| Verification | Which loss, restart, stale-worker, ambiguous-effect and retention cases establish the decision? |

## Maintenance

Update the registry with any application-table addition, rename, removal or
primary-key change. Update reviewed decisions when consumers, lifecycle or
retention change. New persistence must explain reuse of existing records,
fields and indexes before introducing another table. Do not silently overwrite
the original issue assignment with execution history.

Run from the repository root:

```sh
make entity-registry-check
make entity-registry-test
```

The checker derives historical CREATE names/keys and the effective active set
in numeric migration order. The canonical
[retirement declaration](../../migrations/ydb/retirements.json) binds exactly four
owned old tables, their original/successor CREATEs and DROP files; the migration
loader consumes the same declaration before opening YDB. Missing, duplicate,
foreign, early, no-IF/extra-statement DROP, subsequent reactivation/ALTER,
registry active/history drift and invalid source references fail closed.
The four original CREATE mappings are not deleted. A retired record's
`retirement` field must match the ordered source exactly. It is not a general SQL parser, deployed-schema census,
authorization verifier or proof of retention/call-graph correctness. A static
pass cannot authorize a database reset or table deletion.

Actual schema reductions follow accepted design, bounded implementation,
independent review and applicable checks. A separately authorized rollout
resolves deployed readers/writers and retained data before retiring old state.
Baseline compaction belongs to [#174](https://gitcode.com/urandon/sessionless/issues/174);
it does not by itself reduce the entity or table count.
