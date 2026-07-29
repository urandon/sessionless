# YDB state store

MVP-03 makes YDB the authoritative operational store. Telegram remains the
first frontend and the initial authoritative conversation context; the tables
below persist ingestion idempotency, scheduling, quota observations, artifact
references, and delivery state rather than inventing a competing chat history.

## Partitioning and access paths

The complete physical-key inventory, high-entropy ID contract, explicit table
settings, bucketed ready/expiry layout, and cloud measurement gate are in
[ydb-partitioning.md](ydb-partitioning.md).

Tenant entity tables start their primary key with `tenant_id`. The four global
ready/expiry tables start with a bounded physical bucket and time, but retain
`tenant_id` in both the key and every authorization/mutation path. The adapter
requires an internal tenant ID for tenant-scoped transactions and rejects
domain objects whose tenant does not match it.

| Table | Primary key | Hot-path operation |
| --- | --- | --- |
| `tenants` | `(tenant_id)` | point-read tenant status |
| `actors` | `(tenant_id, actor_id)` | point-read internal actor |
| `conversations` | `(tenant_id, conversation_id)` | point-read current context epoch |
| `context_epochs` | `(tenant_id, conversation_id, context_epoch)` | prefix-read a conversation's epochs |
| `telegram_updates` | `(tenant_id, source_id, update_id)` | point insert/read for Bot API deduplication |
| `subscription_connections` | `(tenant_id, subscription_connection_id)` | point-read credential reference and observed entitlement |
| `subscription_scheduler_slots` | `(tenant_id, subscription_connection_id)` | serializable one-subscription admission contention point |
| `tenant_scheduler_counters` | `(tenant_id)` | point-read/update bounded queue and active-run counters |
| `runs` | `(tenant_id, run_id)` | point-read/update one run |
| `run_idempotency` | `(tenant_id, idempotency_key)` | point-resolve an ingress command to a run |
| `attempts` | `(tenant_id, attempt_id)` | point-read/update one attempt |
| `lease_heads` | `(tenant_id, run_id)` | serializable contention point and fence allocation |
| `leases` | `(tenant_id, lease_id)` | point-read a lease and its immutable fence |
| `lease_expiry_v2` | `(shard_bucket, expires_at, tenant_id, run_id)` | bounded global recovery range by bucket/time |
| `checkpoints` | `(tenant_id, attempt_id, sequence)` | prefix-read ordered checkpoints |
| `quota_reservations` | `(tenant_id, quota_reservation_id)` | point transition held/committed/released/expired |
| `quota_expiry_v2` | `(shard_bucket, expires_at, tenant_id, quota_reservation_id)` | bounded global expiry range by bucket/time |
| `usage_observations` | `(tenant_id, subscription_connection_id, observed_at, usage_observation_id)` | bounded time-prefix read per subscription |
| `artifact_manifests` | `(tenant_id, artifact_manifest_id)` | point-read immutable result references |
| `dispatch_outbox` | `(tenant_id, dispatch_outbox_id)` | point publish/ack |
| `dispatch_ready_v2` | `(shard_bucket, available_at, tenant_id, dispatch_outbox_id)` | bounded global pending dispatch range |
| `telegram_delivery_outbox` | `(tenant_id, telegram_delivery_id)` | point delivery transition |
| `telegram_delivery_ready_v2` | `(shard_bucket, available_at, tenant_id, telegram_delivery_id)` | bounded global pending/retry delivery range |
| `audit_events` | `(tenant_id, occurred_at, audit_event_id)` | bounded tenant/time audit reads |

There is no global sequence or shared per-user counter. Production opaque IDs
come from 128 random bits; lease fences are monotonic only within a single
tenant/run head. Ready/expiry rows use a stable 16-way object-ID hash so a
global reconciler has bounded fan-out without concentrating an elephant tenant.

Operational search fields are first-class columns. `JsonDocument` payloads
preserve exact domain round trips, but schedulers, reconcilers, quota monitors,
and admin tooling must not scan or filter arbitrary JSON.

## Atomic procedures

### Telegram ingestion

```text
serializable transaction
  point-read telegram_updates(tenant, source, update)
  if present: return its run_id
  put run + run_idempotency
  put initial attempt
  put dispatch outbox
  insert telegram update marker
commit
```

At-least-once delivery and an uncertain commit response are safe: every row has
a stable key, and the YDB SDK retries the entire idempotent transaction. A
concurrent duplicate conflicts on the update marker, re-reads the winner, and
does not leave its speculative run or outbox rows behind.

### Lease claim and renewal

```text
claim:
  point-read lease_heads(tenant, run)
  reject another unexpired lease
  fence = previous fence + 1
  put immutable lease row + replace head

renew:
  point-read lease row + head
  require lease_id, fence and unexpired ownership
  extend lease row + head in one transaction
```

YDB serializable conflict retry guarantees one winner for distinct concurrent
claims. Every worker-side state/result write must carry the acquired fence;
once the head changes, the old worker receives `ErrLeaseLost`.

### Quota, checkpoints, results, and outboxes

- dispatch admission point-reads the connection, scheduler slot, tenant
  counters, run, attempt, and outbox in one serializable transaction;
- one subscription slot can hold only one active run/reservation pair;
- quota reservations use deterministic row IDs and one-way domain transitions;
- a successful queue publish acknowledges the durable dispatch outbox; an
  uncertain acknowledgement may duplicate the payload-free queue envelope but
  reuses its deterministic message ID;
- expiry enumerates exactly 16 bucket/time ranges, expires only held
  reservations, clears the matching slot, and decrements the tenant queue
  counter idempotently;
- checkpoint `(attempt_id, sequence)` keys prevent duplicate sequence writes;
- a completed run, usage observations, artifact manifest, and Telegram
  delivery outbox commit together;
- dispatch publication acknowledgement transitions the existing outbox row in
  the same tenant transaction.

Only YDB errors classified as retryable by the official SDK are retried. Domain
validation, tenant mismatch, lease ownership, and idempotency conflicts return
directly without retry.

Provider quota and internal product limits are deliberately separate. An
unknown provider remaining balance remains unknown; it does not fabricate a
token number. Admission enforces queue depth, active runs, entitlement, known
provider exhaustion, a configured reservation workload shape, and the MVP rule
of one reserved run per user-owned subscription. Replacing the configured
shape with measured input/context/artifact dimensions at the context-assembly
boundary remains part of the open MVP-06 issue.

## TTL and logical expiry

YDB TTL deletion is asynchronous. Reads whose correctness depends on expiry
must still compare the timestamp:

- Telegram update and run-idempotency markers: 30 days by default;
- attempts, historical leases, checkpoints, reservations, usage, and outboxes:
  90 days after their last relevant timestamp by default;
- audit retention is supplied explicitly by the audit writer.

`lease_heads` has no TTL. An expired head retains the last fence so recovery
cannot reuse a stale fence token.

## Authentication and deployment ownership

The store uses `ydb-go-sdk-auth-environ`. Local tests set anonymous credentials;
Yandex Cloud serverless containers use metadata credentials and a service
account. Static tokens are allowed only as short-lived, environment-injected
developer credentials.

Terraform owns the serverless YDB database, service accounts, IAM, and
endpoints. The application migration binary owns tables and deploy-time schema
compatibility.
