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
does not accept the exploratory Run/Attempt/Lease layout or classify every
remaining physical table. Unknown owner/retention decisions stay open in #205.

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
