# Deterministic local end-to-end slice

`make e2e-local` is the black-box proof gate between the individual adapter
contracts and cloud-dev deployment. It requires Docker Compose but no cloud,
Telegram, provider, or subscription credential.

## Executed topology

```mermaid
sequenceDiagram
    participant Fixture as E2E fixture
    participant API as control-api
    participant DB as YDB Local
    participant Scheduler as reconciler
    participant Queue as ElasticMQ
    participant Worker as one-shot worker container
    participant Blob as MinIO
    participant Sender as telegram-sender
    participant Telegram as Telegram fake

    Fixture->>API: signed Telegram update
    API->>Blob: normalized message and attachments
    API->>DB: update + run + attempt + manifest + dispatch outbox
    Scheduler->>DB: tenant-scoped admission + quota reservation + worker job
    Scheduler->>Queue: tenant_id + run_id
    Worker->>Queue: receive one dispatch
    Worker->>DB: fenced lease + checkpoints + usage
    Worker->>Blob: content-addressed outputs
    Worker->>DB: terminal state + manifest + delivery outbox
    Sender->>Telegram: same-chat text and documents
    Sender->>DB: sent or retry-wait transition
```

The worker is built and invoked through the Compose `worker` profile. Each
invocation is read-only, nonroot, concurrency-one, receives one queue message,
uses a private tmpfs scratch directory, and is removed on exit.

## Command

```sh
make e2e-local
```

The command:

1. starts and migrates the pinned local stand;
2. builds the worker image;
3. resets only ephemeral Telegram captures and visible queue messages;
4. runs the tagged black-box suite;
5. prints service logs automatically when the suite fails.

The stand is intentionally left running for inspection. Use `make dev-down` for
a non-destructive stop or the guarded `dev-reset` command when local volumes
must be deleted.

## Automated scenarios

The suite proves:

- two tenants submit interleaved and out-of-order Telegram updates;
- a duplicate Bot API update maps to one logical run and one dispatch;
- disconnected subscriptions are blocked, then admitted with provider quota
  explicitly `unknown` without any API-billing fallback;
- provider quota `exhausted` and `blocked_until_reset` recover only after an
  explicit observation/state change;
- an admitted dispatch whose first queue publication fails is republished by
  the reconciler after the queue recovers;
- retryable worker failures before the first checkpoint and after checkpoint
  one resume without duplicate terminal state;
- durable cancellation releases the reservation and creates a same-chat reply;
- a deterministic Telegram 429 produces a durable delivery retry and one
  logical captured reply;
- replaying a terminal queue message produces no re-execution, charge, or
  delivery;
- output artifact keys and reads remain tenant-scoped;
- `/new` changes the next workload to a new canonical `session_id` while the
  previous session remains intact.

The lower-level worker, YDB, S3, queue, ingress, and delivery suites retain
their focused concurrency, fencing, path traversal, size-limit, and
cross-tenant negative cases. The E2E suite composes those same production
adapters rather than replacing them with an in-memory product path.

## Correlation and timing evidence

The test emits one correlation line per input:

```text
correlation update_id=<id> tenant_id=<opaque> run_id=<opaque> chat_id=<synthetic>
```

The ingress logger emits `update_id` and `run_id`; the scheduler and worker
queue decorators emit `message_id`, `tenant_id`, and `run_id`; the Telegram
delivery decorator emits `tenant_id`, `run_id`, `delivery_id`, and `chat_id`.
This preserves the same opaque correlation chain across process boundaries
without logging message bodies, credentials, or artifact contents.

Worker invocation JSON and Go subtest durations are printed with `go test -v`.
These durations form the named local baseline for control-plane orchestration;
they intentionally exclude provider inference. Compare only runs using the same
commit, pinned images, host/runner class, cache state, and Docker architecture.
They are not a cloud cold-start or product latency qualification.

## Operator inspection

Use the opaque `tenant_id` and `run_id` printed by the test:

```sql
SELECT status, context_epoch, created_at, updated_at
FROM runs
WHERE tenant_id = $tenant_id AND run_id = $run_id;

SELECT attempt_number, status, worker_id, updated_at
FROM attempts
WHERE tenant_id = $tenant_id AND run_id = $run_id;

SELECT sequence, created_at
FROM checkpoints
WHERE tenant_id = $tenant_id AND run_id = $run_id
ORDER BY sequence;

SELECT source, input_tokens, output_tokens, observed_at
FROM usage_events
WHERE tenant_id = $tenant_id AND run_id = $run_id;

SELECT status, next_attempt_at, updated_at, payload
FROM telegram_delivery_outbox
WHERE tenant_id = $tenant_id AND run_id = $run_id;
```

`attempt_count` is contained in the typed JSON payload; the indexed columns
above remain optimized for delivery scheduling rather than ad hoc reporting.

Queue state is visible at `http://localhost:9325`; MinIO is visible at
`http://localhost:9001`. `docker compose --project-name sessionless-dev logs
control-api reconciler worker-runtime telegram-sender` provides component
logs. Never use these local credentials or endpoints in cloud environments.
