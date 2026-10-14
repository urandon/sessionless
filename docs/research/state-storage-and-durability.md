# State storage and durability

The first supported reduction is retirement of four duplicate ready/expiry
access paths. Changing the Run, Attempt or Lease layout is a separate change:
current authorization and result finalization still depend on those records.
The [design](../design/state-storage-simplification.md) defines the first
reduction; [#205](https://gitcode.com/urandon/sessionless/issues/205) holds
measurements, unresolved decisions and implementation progress.

## Harness and checkpoint comparison

The local code observations below refer to the linked immutable revisions.
Counts exclude secondary indexes and are scoped to the indicated schema,
not the entire product or a deployed database.

| System | Observed storage boundary | Adopt or reject for Sessionless |
| --- | --- | --- |
| [Pi SessionManager](https://github.com/earendil-works/pi/blob/6c4f360264397c59801f6da2bdac13e3b1fcbe91/packages/coding-agent/src/core/session-manager.ts) | `_appendEntry` updates the in-memory tree and `_persist` writes JSONL. Initial disk writing is deferred until an assistant message; an in-memory mode is available. The inspected append path has no explicit fsync. | Adopt compact event representation and derived context. Do not copy the initial write boundary for acknowledged canonical user input or infer power-loss durability from synchronous file calls. |
| [OpenCode schema](https://github.com/anomalyco/opencode/blob/3a31c4ea801915c0b050df4b3842997ea62b6e93/packages/core/src/database/schema.gen.ts) | 19 SQLite CREATE tables in the generated baseline. Messages, parts, sessions, context epochs and input admission have distinct query/lifecycle roles; ordinary SQL indexes provide many access paths. | Prefer indexes over duplicate materialized tables when query and transaction semantics permit. Neither the count nor local credential storage is a cloud-control-plane design. |
| [Hermes schema](https://github.com/NousResearch/hermes-agent/blob/c80a0a551c7038517456ee0aeb60203ec92aedb6/hermes_state_common.py) | 11 base tables in `SCHEMA_SQL`, with additional FTS/feature storage. Session/message history is distinct from current turn leases and compression locks; expiry lookup uses SQL indexes. | Separate current concurrency facts from useful history. A local lease holder and expiry are not a substitute for Sessionless owner scope, current fences or external-effect custody. |
| [Codex migration families](https://github.com/openai/codex/blob/f5420174dafba153913a3e697f89002c338dfd7e/codex-rs/state/src/migrations.rs) | State, logs, goals, memory, queue and thread-history migrators are separate. [Thread history](https://github.com/openai/codex/blob/f5420174dafba153913a3e697f89002c338dfd7e/codex-rs/state/thread_history_migrations/0001_thread_history.sql) stores turns/items plus projection progress. [Official configuration](https://learn.chatgpt.com/docs/config-file/config-reference) distinguishes SQLite runtime state and configurable JSONL history. | Different lifecycles justify some separation. Count effective schemas rather than every historical CREATE, including objects later renamed or dropped. No whole-Codex table count is asserted. |
| [Temporal workflows](https://docs.temporal.io/workflows) and [activities](https://docs.temporal.io/activities) | Workflow state is reconstructed from recorded events; completed activity results are reused during replay. Failed activity attempts can retry, optionally using heartbeat details. | Keep authoritative results separate from transient execution memory. External effects still need idempotency or reconciliation; adopting automatic retries for an unknown model/upload acceptance would be unsafe. No Temporal runtime dependency is proposed. |
| [LangGraph checkpointers](https://docs.langchain.com/oss/python/langgraph/checkpointers) | Checkpoints belong to threads; successful task writes can survive another task's failure. `sync`, `async` and `exit` choose different persistence boundaries. `exit` does not recover intermediate state after a process crash. [Stores](https://docs.langchain.com/oss/python/langgraph/persistence) separately hold cross-thread data. | Choose durability by loss consequences, not one uniform guarantee for all observations. A lighter progress/checkpoint mode cannot remove the durable decision that prevents duplicate external effects. |

Sessionless combines a harness with a multi-principal control plane, canonical
conversation ownership, worker concurrency and external-effect fencing. Local
session files and managed checkpoints inform representation and recovery; they
do not establish equivalent authorization or a target table count.

## Logical state and existing consumers

| Entity | Current storage and source | Loss and simplification conclusion |
| --- | --- | --- |
| Accepted Run identity and result | `runs`, `run_idempotency`, canonical Session events and `run_finalizations`; [PutRun](../../internal/ydbstore/store.go) and [canonical finalization](../../internal/ydbstore/canonical_finalization.go) | Preserve accepted input, cancellation and exact terminal correlation. Display/status projections may be reconstructed, but idempotency and acknowledged output cannot be replaced by missing progress. |
| Current Attempt and worker input | `attempts`, `worker_jobs`, reservation and input manifest; [LoadWorkerJob](../../internal/ydbstore/worker.go) | Loader point-joins the selected Attempt and its admitted inputs. Memory-only Attempt replacement is not currently supported. Historical detail is a different retention decision from the active attempt. |
| Current Lease | `lease_heads` plus `leases`; [requireLeaseOwnership](../../internal/ydbstore/operations.go) checks exact lease ID, fence and expiry. [Owned effect authorization](../../internal/ydbstore/owned_attempt_effect_parent.go) also joins the body and head. | Preserve the exact current tuple and its transactional checks. Folding the body into a head is a candidate only after every join, acquisition/release path and retained-effect dependency is replaced. An expired discovery row is not the publication authority. |
| Irreversible effect | `attempt_effect_reservations` and reconciliation/output evidence; [same parent claim](../../internal/ydbstore/owned_attempt_effect_parent.go) | Same-claim reuse requires fresh matching authority; another physical claim is reconcile-only. Loss of progress, lease or a checkpoint cannot imply the effect was unsent. |
| Historical operational detail | Attempt/lease/checkpoint rows and diagnostics; [Store defaults](../../internal/ydbstore/store.go) | Default operational retention is already 90 days, not an explicit forever-history requirement. The value is not a validated minimum. Shorter retention or ephemeral detail needs a defined replay/custody horizon and actual cleanup consumers. |
| Ready/expiry discovery | Four old/new pairs maintained by [Store](../../internal/ydbstore/store.go), [scheduler](../../internal/ydbstore/scheduler.go), [operations](../../internal/ydbstore/operations.go) and [backfill](../../internal/ydbpartition/backfill.go) | Derived from current authoritative work; old-generation writes are concrete duplication. Index loss needs repair or a visible unavailable/reconcile state, not another model call. |

These are logical groups, not one new table per row. Current and historical
facts can share a physical table while having different retention needs.

## Four legacy index pairs

| Legacy / retained table | Current read dependency | Required source change |
| --- | --- | --- |
| `lease_expiry` / `lease_expiry_v2` | The bucketed API is defined and exercised by an integration test; no production Go caller was found. The separate tenant/time `ListExpiredLeases` method is declared without a Go caller in this snapshot. | Remove the obsolete internal API after checking callers; remove legacy writes/deletes from normal and attached lease transitions. This does not establish an integrated expiry loop. |
| `dispatch_ready` / `dispatch_ready_v2` | Dispatch selection reads v2; legacy reads remain in deployment backfill. | Stop legacy writes/deletes and retire its backfill operation. |
| `quota_expiry` / `quota_expiry_v2` | Quota expiry selection reads v2; native tests still assert both versions. | Stop legacy writes/deletes, retire backfill and update tests to the single retained path. |
| `telegram_delivery_ready` / `telegram_delivery_ready_v2` | Delivery selection reads v2; legacy reads remain in backfill. Session deletion removes both. | Stop legacy writes/deletes and update backfill, deletion and tests together. Telegram enablement remains outside WebUI MVP. |

The `session-lifecycle-indexes-v1` marker governs other lifecycle projections;
it does not turn off these four pairs' dual writes. Partition policy entries
describe layout, not a runtime retirement switch. The existing
[partition retirement procedure](../ydb-partitioning.md#expand-backfill-cut-over-contract)
requires completed backfill, compatible readers and an old-writer drain.
No deployed inventory or completed drain is inferred from source.

## Static write amplification

Numbers below count direct base, inventory and discovery SQL writes in the
named `Put*` branch, assuming successful execution. Writes inside called
scheduler/explanation/finalization helpers are unchanged and excluded. These
are not total transaction cost, latency, bytes, RU or deployed-load measurements.

| Path | Dual generation | V2 only | Reduction |
| --- | --- | --- | --- |
| New lease | 3 | 2 | 1 legacy UPSERT |
| Replacement lease | 5 | 3 | 1 legacy DELETE and 1 UPSERT |
| Pending dispatch save | 5 | 3 | 1 legacy DELETE and 1 UPSERT |
| Nonpending dispatch save | 3 | 2 | 1 legacy DELETE |
| New held quota reservation | 3 | 2 | 1 legacy UPSERT |
| Replacement held reservation | 5 | 3 | 1 legacy DELETE and 1 UPSERT |
| New active Telegram delivery | 4 | 3 | 1 legacy UPSERT; reverse inventory unchanged |
| Replacement active Telegram delivery | 6 | 4 | 1 legacy DELETE and 1 UPSERT; reverse inventory unchanged |

The proposed active application schema falls from 105 to 101 tables only when
the four legacy tables are actually retired. Stopping writes alone leaves 105
physical tables. A forward migration history may still contain 105 historical
CREATE definitions; an effective-schema inventory must account for retirement
instead of passing off a shorter migration baseline as this reduction.

## Decisions and remaining questions

Adopt a single v2 access path per queue/expiry family, existing exact authority
checks and explicit retention ownership. Reject a new retirement state table,
parallel permission issuer, generic catchall ledger, blind external retries
and competitor-derived table targets.

Defer the physical Run/Attempt/Lease merge until its complete transaction and
effect graph supports a replacement layout. Do not use that larger study to
block the independently bounded index reduction. For later changes, measure
actual transaction work, storage and cleanup cost; source statement counts
alone do not establish those costs.
