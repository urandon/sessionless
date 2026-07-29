# YDB state store

MVP-03 makes YDB the authoritative operational store. Telegram remains the
first frontend and the initial authoritative conversation context; the tables
below persist ingestion idempotency, scheduling, quota observations, artifact
references, and delivery state rather than inventing a competing chat history.

## Partitioning and access paths

Every application table starts its primary key with `tenant_id`. The adapter
requires an internal tenant ID for every transaction and rejects domain objects
whose tenant does not match it.

| Table | Primary key | Hot-path operation |
| --- | --- | --- |
| `tenants` | `(tenant_id)` | point-read tenant status |
| `actors` | `(tenant_id, actor_id)` | point-read internal actor |
| `conversations` | `(tenant_id, conversation_id)` | point-read current context epoch |
| `context_epochs` | `(tenant_id, conversation_id, context_epoch)` | prefix-read a conversation's epochs |
| `telegram_updates` | `(tenant_id, source_id, update_id)` | point insert/read for Bot API deduplication |
| `subscription_connections` | `(tenant_id, subscription_connection_id)` | point-read credential reference and observed entitlement |
| `runs` | `(tenant_id, run_id)` | point-read/update one run |
| `run_idempotency` | `(tenant_id, idempotency_key)` | point-resolve an ingress command to a run |
| `attempts` | `(tenant_id, attempt_id)` | point-read/update one attempt |
| `lease_heads` | `(tenant_id, run_id)` | serializable contention point and fence allocation |
| `leases` | `(tenant_id, lease_id)` | point-read a lease and its immutable fence |
| `lease_expiry` | `(tenant_id, expires_at, run_id)` | bounded recovery range by tenant/time |
| `checkpoints` | `(tenant_id, attempt_id, sequence)` | prefix-read ordered checkpoints |
| `quota_reservations` | `(tenant_id, quota_reservation_id)` | point transition held/committed/released/expired |
| `quota_expiry` | `(tenant_id, expires_at, quota_reservation_id)` | bounded expiry range by tenant/time |
| `usage_observations` | `(tenant_id, subscription_connection_id, observed_at, usage_observation_id)` | bounded time-prefix read per subscription |
| `artifact_manifests` | `(tenant_id, artifact_manifest_id)` | point-read immutable result references |
| `dispatch_outbox` | `(tenant_id, dispatch_outbox_id)` | point publish/ack |
| `dispatch_ready` | `(tenant_id, available_at, dispatch_outbox_id)` | bounded pending dispatch range |
| `telegram_delivery_outbox` | `(tenant_id, telegram_delivery_id)` | point delivery transition |
| `telegram_delivery_ready` | `(tenant_id, available_at, telegram_delivery_id)` | bounded pending/retry delivery range |
| `audit_events` | `(tenant_id, occurred_at, audit_event_id)` | bounded tenant/time audit reads |

There is no global sequence or shared per-user counter. Opaque IDs are generated
outside YDB; lease fences are monotonic only within a single tenant/run head.
This avoids a cross-tenant hot partition.

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

- quota reservations use idempotent row IDs and one-way domain transitions;
- checkpoint `(attempt_id, sequence)` keys prevent duplicate sequence writes;
- a completed run, usage observations, artifact manifest, and Telegram
  delivery outbox commit together;
- dispatch publication acknowledgement transitions the existing outbox row in
  the same tenant transaction.

Only YDB errors classified as retryable by the official SDK are retried. Domain
validation, tenant mismatch, lease ownership, and idempotency conflicts return
directly without retry.

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
