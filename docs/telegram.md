# Telegram frontend adapter

Telegram is the first frontend, not the control plane's primary session model.
The Bot API update and chat history remain authoritative transport facts, while
Sessionless owns canonical session history. The adapter persists normalized
tenant-scoped inputs plus the operational run and delivery state required to
execute work safely.

## Webhook contract

The endpoint is:

```text
POST /telegram/webhook
```

In cloud-dev, Telegram posts to a minimal Cloudflare Worker at
`dev-api-sessionless.triborg.dev`. The Worker accepts only the webhook path,
verifies Telegram's secret binding, validates JSON and a one-MiB ceiling, and
forwards the unchanged body to a Yandex Workflows capability URL held in a
second secret binding. It returns `204` only after Workflows returns a valid
execution ID; a handoff failure becomes `502` so Telegram retries.

The workflow and Worker are one operator-managed edge unit. Terraform grants
the required service-account permissions but does not manage the workflow:
otherwise its computed public execution URL would be persisted in Terraform
state. The deployment script obtains the URL directly from `yc`, keeps it only
in process memory and a mode-0600 temporary secrets file, and passes only that
file path to Wrangler.

Workflows durably records the update, then forwards it to
`POST /telegram/webhook`, injects `X-Telegram-Bot-Api-Secret-Token` from
Lockbox, and retries selected transient HTTP, timeout, and quota failures.
Direct Telegram delivery to either API Gateway or the native public Workflows
URL is not used because live tests timed out before reaching both otherwise
healthy Yandex public endpoints.

The internal endpoint compares `X-Telegram-Bot-Api-Secret-Token` in constant
time before processing the JSON body. The forwarded update returns `200` only
after:

1. its opaque identity mapping has been materialized in YDB;
2. message content and downloaded attachments have been written below the
   tenant object prefix;
3. the Telegram update, run, initial attempt, input artifact manifest, and
   dispatch outbox have committed in one serializable YDB transaction.

Duplicate `(tenant_id, source_id, update_id)` deliveries return the canonical
run and do not create another run, attempt, manifest, or dispatch outbox.
Unsupported update kinds and group chats are acknowledged and ignored in the
current private-chat MVP. Processing failures return `503` so Telegram retries.

Logs contain update/run IDs and state only. Message text, captions, file
contents, bot tokens, webhook secrets, and identity keys are never logged.
The Cloudflare Worker also emits no application logs and never forwards the
Telegram secret header or the Workflows capability URL.

## Commands

The private-chat command surface is:

```text
/connect codex
/compute status
/compute disconnect codex
/new
```

Any other slash-prefixed message receives the supported-command list and is
never dispatched as an AI workload. A command is represented by a terminal
control-plane run with no attempt or dispatch outbox. In one serializable YDB
transaction the adapter deduplicates the Telegram update, applies the
subscription or binding change, records the command run, and enqueues an inline
Telegram delivery. This avoids a state-committed/reply-missing crash window.

`/connect codex` currently moves the deterministic connection to
`reauthentication_required`. It stores no credential and tells the user that
the isolated Codex authorization adapter is still required. `/compute status`
reports only provider, connection, provider-quota, and scheduler states; it
never exposes tokens or credential references. `/compute disconnect codex`
clears the credential reference, marks the connection `disconnected`, resets
observed quota to `unknown`, moves admission to `reauth_required`, and does not
enable an API-billing fallback.

`/new` creates a new canonical session and switches this Telegram conversation
to it. Telegram history, stored artifacts, and the previous session are not
deleted. Session creation, active owner membership, and the expected-revision
binding switch commit in one YDB transaction. An old webhook or retry cannot
overwrite a newer binding. The command itself is not sent to an AI worker.

## Opaque identity resolution

Every replica derives the same IDs with HMAC-SHA-256 and a deployment secret:

- tenant and frontend binding from the private Telegram chat ID;
- actor from the tenant chat plus Telegram user ID;
- subscription connection from the tenant chat, user ID, and provider name.

Only a truncated keyed digest is encoded into internal IDs. Telegram numeric
IDs cannot be recovered from these values, and no global lookup-table scan is
required. Rotating `TELEGRAM_IDENTITY_HMAC_KEY` changes the mapping and therefore
requires an explicit migration; it must not be rotated like an ordinary
short-lived credential.

The raw frontend IDs remain only in tenant-scoped actor and frontend-binding
records needed to address Telegram and reconcile frontend facts.

## Object layout

Normalized input and attachments are stored as:

```text
tenants/<tenant-id>/inputs/<run-id>/message.json
tenants/<tenant-id>/inputs/<run-id>/attachments/<ordinal>-<safe-name>
```

The input artifact manifest is committed with the run. A BlobStore adapter
enforces the tenant prefix again on every put/open/delete operation.

The Telegram file flow uses `getFile`, checks the declared and streamed size,
then downloads through `/file/bot<token>/<file_path>`. The local Telegram fake
implements both endpoints; test files can be registered with:

```text
POST /test/files/<file-id>?name=<file-name>
```

## Durable delivery

After a Telegram delivery outbox commits, its producer publishes a payload-free
`wake.telegram` envelope. The YMQ-triggered `telegram-sender` point-reads the
row by `(tenant_id, telegram_delivery_id)`. A serializable transaction then
re-reads the delivery and moves it to `sending`; concurrent or duplicate wake
deliveries therefore produce one claim winner and terminal duplicates are
no-ops. The fixed 16-bucket `telegram_delivery_ready_v2` traversal is reserved
for startup and six-hour recovery, not the normal delivery path. A `sending`
row remains indexed behind a two-minute visibility timeout, so a process crash
cannot strand it permanently. The sender reads only tenant-authorized
payload/artifact blobs. Control commands use bounded inline text in the same
outbox model, so they require no object-store write between the state
transition and durable reply creation.

Successful sends move to `sent`. Failures use bounded exponential backoff and
move through `retry_wait`; the configured attempt limit ends in `failed`.
Telegram Bot API does not expose an idempotency key for `sendMessage`, so a
process crash after Telegram accepts a message but before YDB records `sent`
can cause a physical duplicate. The durable outbox guarantees one logical
delivery record, but eliminating that external ambiguity requires a future
reconciliation strategy rather than a false exactly-once claim.

## Configuration

Non-secret settings:

- `TELEGRAM_API_BASE_URL`;
- `TELEGRAM_SOURCE_ID`;
- `DEFAULT_COMPUTE_PROVIDER`;
- `DELIVERY_QUEUE_URL`.

Secrets are injected through the process environment or workload secret store:

- `TELEGRAM_BOT_TOKEN`;
- `TELEGRAM_WEBHOOK_SECRET`;
- `TELEGRAM_IDENTITY_HMAC_KEY` (at least 32 bytes).

The checked-in Compose defaults are synthetic local-only values pointed at
`telegram-fake`. Production values must never be committed.
