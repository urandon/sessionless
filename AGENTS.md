# AGENTS.md

## Product direction
- Build a multi-frontend product whose canonical conversation state is an
  append-only `Session` event stream owned by Sessionless.
- Telegram is the first frontend adapter, not the source of truth. WebUI and
  later frontends bind external conversations to the same canonical sessions.
- `/new` creates a new session and atomically switches the current frontend
  binding. It never mutates or truncates an existing session.
- Support user messages with images/files and project AI results back to every
  bound frontend without making transport identifiers product identities.

## Execution model
- AI work runs in isolated, harness-pluggable workers with access only to
  authorized session material and explicitly allowed MCP servers.
- The serverless control plane routes work to workers rather than processing AI
  work inline.
- Assume hosting targets Yandex Cloud serverless primitives unless requirements change.
- Persist canonical and operational state in YDB and large immutable payloads
  in Object Storage. Partition and authorize both by tenant.

## Implementation preferences
- Backend language priority: Go first, then TypeScript, then Python.
- Keep infrastructure choices friendly to Yandex Cloud serverless and YDB support.
- Include WebUI/admin surfaces for sessions, worker health, and consumed-token
  monitoring. Membership, not an identity-provider claim alone, grants tenant access.

## Current repository state
- The repository contains a Go control plane, YDB migrations/state adapters,
  queue and blob adapters, a local/cloud development stand, a deterministic
  worker harness, Telegram adapters, tests, and CI.
- Canonical session domain and port contracts are present. Persistence migration
  and frontend adaptation are tracked separately; do not describe legacy
  conversation/context tables as the canonical model.
- Use only commands backed by the Makefile and documented in `README.md` and
  `docs/development.md`.
