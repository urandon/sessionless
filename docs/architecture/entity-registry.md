# Entity registry and storage decisions

The [entity registry](entity-registry.json) maps physical application tables to
their creation migrations, observed keys, schema retention and source consumers.
The v2 registry keeps **106 historical CREATE records** and derives **102
effective active tables** after four forward retirement definitions. These are
source-layout counts, not an assertion about any retained deployment.
Logical state and durability decisions are specified in the
[state storage simplification design](../design/state-storage-simplification.md).
The owning [issue #205](https://gitcode.com/urandon/sessionless/issues/205) holds
the report, problem register, review findings and implementation decisions.

An entity is not a table. A canonical Session may have several rebuildable
lookup projections; a current execution record may contain several bounded
operational fields. Separate physical storage requires a concrete access,
lifecycle or transaction reason, not one table for every state or proof.

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
remains 102 tables, including `lease_heads`. Other Run/Attempt/history and
owner/retention decisions stay open in #205.

The [#200 output-custody implementation](https://gitcode.com/urandon/sessionless/issues/200)
reuses the existing per-attempt `attached_worker_output_receipts` row/key for a
closed upload-plan subrecord rather than introducing a new upload table.
Its active, receipt-bound and settled-abandoned variants are distinct from
canonical READY. An issued, ambiguous source-write grant remains spent and
blocks deletion; a planned key is not an observed object. Noncanonical
settlement preserves exact verified source refs without publishing output or
reopening send rights. The private admission method checks current membership,
worker/connection/head, pinned job/profile and Session deletion in the same
transaction as pending-row insertion. It has no signer or body sink and does
not reset an existing plan's cutoff or spent ordinals. The claim method reuses
that ordinary read set and serializes a one-way unissued-to-write-pending update
in the same row. A duplicate claim cannot reissue a grant; transaction retries
retain the first selected deadline or tighten it, and an outer transaction
error discards any apparent callback success. Read-only reconciliation rechecks
the same ordinary scope, nonce and original producer/final digests, preserves
spent states and never turns expiry into settlement or deletion permission.
None of these methods has a signer or body sink. Native serialization is not
yet proven. Structural validation is
not original-writer or full Source/provider/profile authority: authenticated
control registration, effect-authorized signing, the private exporter, actual
request join and native deletion races remain incomplete.

## Attached Pi API owner decisions

`attached_pi_api_policy_v1` is one owner/resource/model/data-class namespace,
not a table per Run, attempt, permission check or operational observation.
It stores immutable authenticated owner decisions and a monotone current
mapping/revocation head atomically. Historical decision replay never rewrites
that head. The locator deliberately excludes resource generation and native P:
a replacement for the same lane must invalidate the old mapping rather than
leave it independently current. Current use also rechecks membership security
version, exact active API resource revision/generation, and the authoritative
server-selected native/evidence/tariff catalog.

Existing storage cannot safely substitute this narrow authority: the managed
`owned_agent_profile_current_v1` contract is restricted to its managed/public
profile lifecycle; web-search consent belongs to an individual attempt parent;
provider credential metadata authorizes credential ownership, not an owner's
confirmed model spending terms; generic Web audit has TTL and no current CAS
head. The new namespace therefore retains two record kinds (decision and head),
no extra proposal/index/table, and no default TTL that could revive permission.
It is not a balance, monetary reservation, quota or effect ledger.

The source migration is not an applied migration or live permission. No
genuinely qualified installed Pi native/evidence/full-context tariff catalog is
supplied by this implementation. Its absence denies new offers/decisions/use;
structural P/E/T consistency, config flags and receipt hashes do not replace it.

## Source and rollout boundary

The [compatible code step](https://gitcode.com/urandon/sessionless/issues/206)
removes unversioned ready/expiry writes, ordinary cleanup and the unused tenant
lease-expiry reader. Normal bootstrap backfills lifecycle indexes only; it no
longer copies the four legacy ready/expiry tables. The four legacy CREATE
records and optional historical reset ownership remain. The ordered retirement
definitions now remove those four tables from the effective source schema;
the registry still covers every one of the 106 historical CREATE tables.

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
