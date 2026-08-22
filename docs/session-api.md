# Frontend-neutral session API

## Boundary

`internal/sessionapi` is the authenticated application boundary used by the
Web BFF and future frontend adapters. A request obtains its tenant and user
from a current server-side authorization; `session_id`, frontend coordinates,
and cursors are selectors, never authority.

The service exposes session create, point metadata read, active/archived list,
ordered event history, run history, archive/unarchive, and internal frontend
bind/switch operations. The Web application layer adds canonical message
ingress, exact-object upload intents, point run reads, and attachment download
capabilities. Telegram, Web, and synthetic adapters keep using the same
canonical session/binding/event ports. A binding switch changes only the
frontend's selected session; it does not mutate the previous session.

## Web routes

| Method | Route | Contract |
| --- | --- | --- |
| `GET` / `POST` | `/api/web/v1/sessions` | Page active/archived sessions, or idempotently create one |
| `GET` | `/api/web/v1/sessions/{session_id}` | Open participant-authorized bounded metadata |
| `GET` | `/api/web/v1/sessions/{session_id}/events` | Page ordered canonical events and authorized payloads |
| `GET` | `/api/web/v1/sessions/{session_id}/runs` | Page execution observations for the session |
| `POST` | `/api/web/v1/sessions/{session_id}/archive` | Archive or unarchive without deleting history |
| `POST` | `/api/web/v1/sessions/{session_id}/messages` | Idempotently append a Web user event and create its run |
| `POST` | `/api/web/v1/uploads` | Create an exact, short-lived direct-upload capability |
| `POST` | `/api/web/v1/uploads/{upload_id}/commit` | Verify and commit the exact staged object |
| `GET` | `/api/web/v1/runs/{run_id}` | Read one participant-authorized run for polling |
| `GET` | `/api/web/v1/sessions/{session_id}/events/{sequence}/attachments/{index}` | Create a short-lived download capability for one canonical attachment |
| `GET` | `/api/web/v1/sessions/{session_id}/runs/{run_id}/artifact-manifests/{manifest_id}/artifacts/{index}` | Create a short-lived capability for one exact worker artifact |

All mutations require the Web BFF's exact-origin and double-submit CSRF
checks. Session, upload, and message creation use caller-supplied idempotency
keys and deterministic, user-scoped identities. Archive/unarchive is naturally
idempotent.

The browser cannot create or switch arbitrary frontend bindings. Message
ingress creates or validates the server-owned Web binding with frontend `web`
and external conversation ID equal to the authorized canonical `session_id`.
The internal revision-fenced binding port remains available to trusted
frontend adapters; it is not a browser route.

## Authorization and errors

Every ordinary read checks an active `session_participants` row for the
authorized tenant/user/session before returning metadata or opening an Object
Storage reference. Writes additionally require a non-viewer participant and a
session not fenced by destructive deletion. Binding an existing frontend
authorizes both its current session and the target session in the same YDB
transaction; an archived target cannot become current.

A missing session and a session unavailable to the caller both produce the
same `404 not_found` response. Invalid selectors/cursors produce
`400 invalid_request`; stale binding revisions and conflicting idempotency
facts produce `409 conflict`; unavailable dependencies produce
`503 temporarily_unavailable`. Responses never include storage keys, tenant
authority supplied by the browser, or raw authorization errors.

Message submission fails closed unless the requesting user has exactly one
configured compute connection whose actor record maps to that user. Its
response identifies the canonical event and run and includes the selected
provider's safe entitlement/quota observation; it never returns a provider
credential. Retrying the same message idempotency key returns the same
canonical event/run instead of appending another event.

Administrative metadata is a separate `SessionAdminMetadataStore` port. It
returns only the session row, bounded display materialization, current run,
and provider observation. It cannot list or materialize event payloads and is
not part of ordinary worker/frontend dependencies. The admin surface must add
its own explicit platform authorization before calling this port.

## Pagination and consistency

Session lists merge exactly 16 `session_activity` buckets, each read with a
bounded owner/status key prefix and page limit. The global order is
`(updated_at DESC, session_id DESC)`. Run pages use
`(created_at DESC, run_id DESC)`; event pages use strictly increasing canonical
sequence. Every continuation token is HMAC-authenticated and scoped to its
kind, tenant, user, status where applicable, and session where applicable. A
token cannot be replayed against another authorized scope and exposes no
storage continuation object.

Event history also accepts `after_sequence=<unsigned integer>` for incremental
projection. It returns events strictly after that canonical sequence and is
mutually exclusive with `cursor`; `limit` remains bounded to 100. This form is
intended for foreground polling after a message is accepted, while opaque
`cursor` remains the stable keyset-pagination contract.

Pages have read-committed/keyset semantics: activity committed after page one
may appear before its continuation boundary and therefore is not injected into
later pages. Retrying the same cursor against unchanged state is deterministic.
No hot route scans a tenant table or event payload collection.

`session_displays` is a rebuildable, payload-free list materialization. Title
and preview are whitespace-normalized and bounded to 120 and 280 Unicode code
points. Origin describes the last observed frontend input. Provider/harness
and run state describe execution only; they never define session identity.

Event bytes are opened only after participant authorization, are size-bounded,
and must match the canonical BlobRef byte count and SHA-256 digest before JSON
projection. Attachments remain tenant-scoped immutable references; capability
download URLs are issued by the separate upload/download boundary.

Session lists, session point reads, event pages, run pages, and point run reads
return a representation-derived `ETag`. A matching `If-None-Match` returns
`304 Not Modified` without a response body. Responses that contain a
non-terminal run additionally return integer-seconds `Retry-After` and the
more precise `X-Sessionless-Poll-After-Ms`; clients should use these hints
instead of a fixed tight polling loop. Message creation also returns a
`Retry-After` hint for its run.

## Exact-object upload and download capabilities

An upload starts with `POST /api/web/v1/uploads` containing `session_id`,
`idempotency_key`, `name`, `media_type`, positive `size`, and a lowercase
hexadecimal `sha256` plus `content_md5`, the canonical padded standard-base64
encoding of the 16-byte MD5 digest. The server validates participant write
access, creates a tenant/user/session-bound intent, and returns a short-lived
`PUT` URL plus the exact required headers. The client must send the declared
content length and every returned header, including `Content-MD5`, exactly.
The MD5 protects the direct Object Storage upload; commit independently checks
the declared SHA-256 against server-read object bytes.

Commit takes only a body `upload_id` equal to the path selector. The server
reauthorizes the caller and obtains authoritative Object Storage metadata for
the server-generated staging key; client-supplied metadata is never trusted.
Submitting a message may claim up to eight committed upload IDs. It rechecks
the staging ETag and metadata, conditionally promotes each object into the
immutable canonical event namespace, and only then commits the event/run.
Missing, expired, cross-session, cross-user, overwritten, or already-claimed
intents fail closed. Staging objects alone are never conversation history.

Attachment reads address a canonical event sequence and zero-based attachment
index. The BFF first authorizes and verifies that exact canonical reference,
then returns a short-lived `GET` capability. Capability URLs are bearer
secrets: clients must keep them out of logs, analytics, referrers, and
persistent browser storage.

Assistant event projections expose only the owning `run_id` and
`manifest_id`. A worker-artifact read additionally binds both selectors to the
requested session, checks active participant access, and addresses one bounded
zero-based manifest index. The response contains safe display metadata and a
short-lived exact-object capability; it never exposes a BlobRef or storage key.

## Verification

```sh
make test
make build
make ydb-integration
make local-integration
make e2e-local
```

The YDB suite proves fixed-fan-out keyset pagination, active/archived
transitions, participant denial, paged event/run reads, revision-fenced
frontend attachment, admin-safe point metadata, and Telegram/synthetic access
to the same canonical session.
