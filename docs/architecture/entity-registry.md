# Entity registry and storage decisions

The [entity registry](entity-registry.json) maps physical application tables to
their creation migrations, observed keys, schema retention and source consumers.
Logical state and durability decisions are specified in the
[state storage simplification design](../design/state-storage-simplification.md).
The owning [issue #205](https://gitcode.com/urandon/sessionless/issues/205) holds
the report, problem register, review findings and implementation decisions.

An entity is not a table. A canonical Session may have several rebuildable
lookup projections; a current execution record may contain several bounded
operational fields. Separate physical storage requires a concrete access,
lifecycle or transaction reason, not one table for every state or proof.

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

The checker derives CREATE names and keys from the repository's migration
format and rejects missing, extra, duplicate or mismatched entries and invalid
source paths. It is not a general SQL parser, deployed-schema census,
authorization verifier or proof of retention/call-graph correctness. A static
pass cannot authorize a database reset or table deletion.

Actual schema reductions follow accepted design, bounded implementation,
independent review and applicable checks. A separately authorized rollout
resolves deployed readers/writers and retained data before retiring old state.
Baseline compaction belongs to [#174](https://gitcode.com/urandon/sessionless/issues/174);
it does not by itself reduce the entity or table count.
