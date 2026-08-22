# YDB state store

YDB is the authoritative store for both the canonical Sessionless conversation
stream and operational control-plane state. Telegram is only the first frontend
adapter. Its external conversations resolve through revisioned bindings to the
same sessions that WebUI and later frontends will read and append.

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
| `sessions` | `(tenant_id, session_id)` | point-read canonical session metadata and allocate its next event sequence |
| `session_events` | `(tenant_id, session_id, sequence)` | bounded prefix-read immutable, ordered canonical history |
| `session_event_idempotency` | `(tenant_id, session_id, idempotency_key)` | point-resolve an append retry to its existing event |
| `frontend_bindings` | `(tenant_id, binding_id)` | point-read/switch a revisioned frontend binding |
| `frontend_bindings_by_session` | `(tenant_id, session_id, binding_id)` | bounded binding inventory and projection fan-out for one session |
| `frontend_binding_keys` | `(tenant_id, frontend, external_conversation_id)` | point-resolve an external conversation without a scan |
| `frontend_ingress_idempotency` | `(tenant_id, binding_id, idempotency_key)` | point-resolve a frontend duplicate to its original session event and run, including after a binding switch |
| `frontend_projection_outbox` | `(tenant_id, frontend_projection_id)` | point-read frontend-neutral work referencing a canonical assistant/system event and binding revision |
| `frontend_projections_by_session` | `(tenant_id, session_id, frontend_projection_id)` | bounded destructive-retention inventory for one session |
| `frontend_projections_by_run` | `(tenant_id, run_id, frontend, frontend_projection_id)` | bounded frontend-specific projection lookup for one terminal run wake |
| `frontend_projection_ready_v1` | `(frontend, shard_bucket, created_at, tenant_id, frontend_projection_id)` | fixed 16-bucket recovery traversal for lost projection wakes |
| `session_participants` | `(tenant_id, session_id, user_id)` | point-authorize tenant membership and session role |
| `session_snapshots` | `(tenant_id, session_id, version)` | bounded prefix-read immutable context materializations |
| `session_activity` | `(tenant_id, user_id, status, activity_bucket, updated_at, session_id)` | fixed 16-query recent-session fan-out per member |
| `session_api_idempotency` | `(tenant_id, user_id, idempotency_key)` | point idempotency for frontend-neutral session creation |
| `session_api_mutations` | `(tenant_id, user_id, idempotency_key, kind)` | point retry facts for idempotent frontend-neutral mutations |
| `session_displays` | `(tenant_id, session_id)` | bounded rebuildable title/preview and current-run metadata |
| `session_legal_holds` | `(tenant_id, session_id)` | point-check durable legal/audit retention before deletion |
| `session_deletions` | `(tenant_id, session_id)` | point-read write fence and durable requested/deleting/completed tombstone |
| `external_identities` | `(shard_bucket, provider, subject)` | point-resolve a verified frontend identity to one internal user |
| `external_identities_by_user` | `(user_bucket, user_id, provider, subject)` | bounded reverse identity lookup |
| `tenant_memberships` | `(user_bucket, user_id, tenant_id)` | bounded membership list and point authorization |
| `tenant_invitations` | `(tenant_id, invitation_id)` | point-consume a one-time enrollment grant |
| `oidc_login_challenges` | `(shard_bucket, state_digest)` | point-consume one browser-bound OIDC transaction |
| `web_sessions` | `(shard_bucket, session_digest)` | point-authorize, rotate, or revoke a first-party browser session |
| `development_bootstrap_grants` | `(tenant_id, user_id)` | exact cloud-dev bootstrap idempotency and audit ledger |
| `telegram_updates` | `(tenant_id, source_id, update_id)` | point insert/read for Telegram control-command deduplication; ordinary messages use `frontend_ingress_idempotency` |
| `subscription_connections` | `(tenant_id, subscription_connection_id)` | point-read credential reference and observed entitlement |
| `subscription_connections_by_user` | `(tenant_id, user_id, subscription_connection_id)` | bounded owner-scoped compute selection; each selected base row is revalidated through its actor mapping |
| `subscription_scheduler_slots` | `(tenant_id, subscription_connection_id)` | serializable one-subscription admission contention point |
| `tenant_scheduler_counters` | `(tenant_id)` | point-read/update bounded queue and active-run counters |
| `worker_jobs` | `(tenant_id, run_id)` | point-load immutable worker references and admitted limits |
| `runs` | `(tenant_id, run_id)` | point-read/update one run with explicit session/event correlation |
| `run_finalizations` | `(tenant_id, run_id)` | point-fence an exact terminal callback digest and reject conflicting retries |
| `runs_by_session` | `(tenant_id, session_id, created_at, run_id)` | bounded recent-run read for one session |
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
| `artifact_manifests_by_run` | `(tenant_id, run_id, artifact_manifest_id)` | bounded exact-manifest inventory for one session run |
| `dispatch_outbox` | `(tenant_id, dispatch_outbox_id)` | point publish/ack |
| `dispatch_ready_v2` | `(shard_bucket, available_at, tenant_id, dispatch_outbox_id)` | bounded global pending dispatch range |
| `telegram_delivery_outbox` | `(tenant_id, telegram_delivery_id)` | point delivery transition |
| `telegram_deliveries_by_run` | `(tenant_id, run_id, telegram_delivery_id)` | bounded delivery inventory and destructive cleanup for one run |
| `checkpoint_objects_by_run` | `(tenant_id, run_id, checkpoint_id)` | durable exact checkpoint-object inventory after operational checkpoint TTL |
| `session_lifecycle_backfill_state` | `(backfill_id)` | expand/migrate/cutover completion marker for lifecycle indexes |
| `telegram_delivery_ready_v2` | `(shard_bucket, available_at, tenant_id, telegram_delivery_id)` | bounded global pending/retry delivery range |
| `audit_events` | `(tenant_id, occurred_at, audit_event_id)` | bounded tenant/time audit reads |
| `web_security_audit_events` | `(shard_bucket, occurred_at, request_id)` | bounded 16-way time reads for pre-auth and CSRF security events |

There is no global sequence or shared per-user counter. Production opaque IDs
come from 128 random bits; lease fences are monotonic only within a single
tenant/run head. Ready/expiry rows use a stable 16-way object-ID hash so a
global reconciler has bounded fan-out without concentrating an elephant tenant.

Operational search fields are first-class columns. `JsonDocument` payloads
preserve exact domain round trips, but schedulers, reconcilers, quota monitors,
and admin tooling must not scan or filter arbitrary JSON.

Canonical payloads use a second tenant/session authorization boundary in Object
Storage:

```text
tenants/<tenant-id>/sessions/<session-id>/events/<event-id>/...
tenants/<tenant-id>/sessions/<session-id>/snapshots/<version>.jsonl.zst
```

An event or snapshot is rejected if its `BlobRef` points to another tenant,
session, event prefix, or the snapshot key for its exact immutable version.

## Atomic procedures

### Canonical session append and frontend switch

```text
append event (serializable transaction)
  point-read sessions(tenant, session)
  point-read session_event_idempotency(tenant, session, key)
  if exact retry: return the existing event without a write
  sequence = session.last_event_sequence + 1
  insert immutable session event + idempotency row
  advance session sequence and fixed-fan-out activity rows
commit

/new or equivalent frontend action (serializable transaction)
  require active tenant write membership and current session participation
  point-read frontend binding and require expected revision
  create new session + active owner membership + activity row
  switch binding to the new session and increment revision
commit

frontend-neutral user input (serializable transaction)
  point-read frontend_ingress_idempotency(tenant, binding, key)
  if present: return its original session event and run, even after /new
  require active tenant write membership and current session participation
  point-read binding and require its expected revision
  allocate the next sequence inside the bound active session
  insert canonical user event + both idempotency rows
  put run + initial attempt + input manifest + dispatch outbox
  advance session sequence/activity rows
commit
```

Concurrent appends and canonical ingress serialize on one session row and
produce a gap-free sequence.
There is deliberately no global event sequence. An uncertain
create-and-switch response is safe to retry with the original binding revision,
new session ID, and timestamp; a different request receives a stale-binding or
content-conflict error.

### Telegram ingestion

```text
serializable transaction
  authorize active tenant membership and writable session participant
  point-read frontend_ingress_idempotency(tenant, binding, normalized delivery)
  if present: return its original session event and run
  append canonical user event and advance the session sequence
  put run + initial attempt + input manifest + dispatch outbox
  insert frontend ingress idempotency fact
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
  counters, run, attempt, and outbox, then writes the reservation and immutable
  worker job in one serializable transaction; canonical jobs also pin the
  newest compatible snapshot at/before their trigger and the bounded tail
  through that trigger;
- one subscription slot can hold only one active run/reservation pair;
- quota reservations use deterministic row IDs and one-way domain transitions;
- a successful queue publish acknowledges the durable dispatch outbox; an
  uncertain acknowledgement may duplicate the payload-free queue envelope but
  reuses its deterministic message ID;
- after canonical publish and acknowledgement, reconciler best-effort snapshot
  maintenance creates a new immutable version only at the configured event
  interval; bounded failures leave replay authoritative and are retried later;
- expiry enumerates exactly 16 bucket/time ranges, expires only held
  reservations, clears the matching slot, and decrements the tenant queue
  counter idempotently;
- checkpoint `(attempt_id, sequence)` keys prevent duplicate sequence writes;
- each worker boundary renews ownership when needed and commits its checkpoint
  plus usage under the current fence;
- worker context reads are tenant/session scoped and bounded by the admitted
  event count; they verify contiguous sequence coverage through the pinned
  trigger before any harness starts;
- a completed run, attempt, reservation, artifact manifest, Telegram delivery,
  scheduler counters, and lease-index cleanup commit together;
- failed, cancelled, or timed-out work releases its reservation and commits a
  same-chat terminal delivery in the same transaction;
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

Canonical `sessions`, `session_events`, `session_event_idempotency`,
`frontend_bindings`, `session_participants`, `session_snapshots`, lifecycle
tombstones, and lifecycle audit events have no TTL. Archiving changes
visibility/status; it does not delete or truncate history. Destructive removal
uses the separately authorized and audited state machine in
[session-lifecycle.md](session-lifecycle.md), never YDB TTL.

YDB TTL deletion is asynchronous. Reads whose correctness depends on expiry
must still compare the timestamp:

- OIDC login challenges: ten minutes or less;
- Web sessions: seven-day absolute lifetime or less, with a server-enforced
  12-hour sliding idle lifetime;
- tenant invitations: their explicit grant expiry;
- frontend ingress, Telegram update, and run-idempotency markers: 30 days by default;
- attempts, worker job descriptors, historical leases, checkpoints,
  reservations, usage, and outboxes: 90 days after their last relevant
  timestamp by default;
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
