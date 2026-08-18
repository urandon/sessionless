# Frontend-neutral session API

## Boundary

`internal/sessionapi` is the authenticated application boundary used by the
Web BFF and future frontend adapters. A request obtains its tenant and user
from a current server-side authorization; `session_id`, frontend coordinates,
and cursors are selectors, never authority.

The service exposes session create, point metadata read, active/archived list,
ordered event history, run history, archive/unarchive, and frontend
bind/switch operations. Telegram and synthetic adapters keep using the same
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
| `POST` | `/api/web/v1/frontend-bindings` | Bind a new frontend or revision-fenced switch to an active session |

All mutations require the Web BFF's exact-origin and double-submit CSRF
checks. Create uses a caller-supplied idempotency key and a deterministic,
user-scoped session identity. Archive/unarchive is naturally idempotent.
Frontend creation uses `expected_revision=0`; subsequent switches require the
last observed positive revision. An exact retry returns the already-switched
binding.

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
