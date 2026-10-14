# AGENTS.md

## Product direction
- Build a multi-frontend product whose canonical conversation state is an
  append-only `Session` event stream owned by Sessionless.
- Telegram is an implemented frontend adapter, not the source of truth.
  WebUI is the first MVP launch frontend; Telegram messaging/Cloudflare rollout
  is deferred until after MVP. All frontends bind to the same canonical sessions.
- Follow `docs/mvp-delivery-plan.md` for current release scope. Do not promote
  post-MVP sharing, administration or exhaustive recovery work into hidden gates.
- MVP requires both one supported ready-made harness on an attached worker and
  Sessionless-managed execution without a user host. The managed harness also
  needs a bounded model/tool/result loop, derived compaction, useful constrained
  MCP and bounded web search. Follow research, accepted design, then linked
  implementation tasks; neither route is a silent fallback. Real profiles stay
  disabled until their own bounded rollout gates pass.
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
- Persist canonical and correctness-critical durable state in YDB and large
  immutable payloads in Object Storage. Partition and authorize both by tenant.
  Classify operational durability, reconstruction and retention through the
  [entity registry](docs/architecture/entity-registry.md); do not default every
  progress observation or intermediate execution state to permanent storage.

## Implementation preferences
- Backend language priority: Go first, then TypeScript, then Python.
- Keep infrastructure choices friendly to Yandex Cloud serverless and YDB support.
- Include WebUI/admin surfaces for sessions, worker health, and consumed-token
  monitoring. Membership, not an identity-provider claim alone, grants tenant access.
- All Go tests and test reviews must follow
  [`docs/testing-best-practices.md`](docs/testing-best-practices.md), including
  its deterministic-time, isolation, cleanup, race, and exact-CI requirements.
- Entity/table changes must update the [entity registry](docs/architecture/entity-registry.md)
  and justify separate storage against reuse of existing records, fields or
  indexes. Preserve the smallest necessary durable fence/effect/custody facts;
  missing transient state must not authorize blind external replay.
  Run `make entity-registry-check` and `make entity-registry-test` with schema
  changes. Applied migrations and runtime guarantees change only through their
  reviewed implementation and separately authorized rollout.

## Engineering acceptance and orchestration

- Follow the [autonomous development protocol](https://gitcode.com/urandon/sessionless/wiki/Autonomous-Development-and-Agent-Orchestration.md).
  Routine research, engineering design and implementation within agreed
  MVP/features are delegated to agents; do not require manual human PR review
  or repeat owner approval of those engineering choices.
- An independent reviewer starts from the original goal, issue and authoritative
  contracts, derives its own requirements and evidence matrix before inspecting
  the candidate, and reports unresolved findings and limitations. Primary design
  acceptance cites that review, research and applicable checks; it is not a claim
  that the owner personally reviewed the PR.
- Merge still requires independent review of the exact stabilized artifact,
  proportionate tests, all required CI on the current exact head and the primary
  merge audit. Green CI alone does not accept a design or complete a feature.
- Return to the owner for changes to product/features or MVP scope, material
  unresolved product/policy/UX choices outside delegation, or new external
  authority. Engineering delegation does not itself authorize deployment,
  paid/live calls, credentials, migration application or destructive operations.
- Research feeds accepted versioned design, then linked implementation children.
  Keep epic checklists, priority disposition and current evidence synchronized
  before handoff/compaction; preserve both MVP tracks and owner-assigned priorities.

## Current repository state
- The repository contains a Go control plane, YDB migrations/state adapters,
  queue and blob adapters, a local/cloud development stand, a deterministic
  worker harness, Telegram adapters, tests, and CI.
- Canonical session domain and port contracts are present. Persistence migration
  and frontend adaptation are tracked separately; do not describe legacy
  conversation/context tables as the canonical model.
- Use only commands backed by the Makefile and documented in `README.md` and
  `docs/development.md`.

## Worktree lifecycle
- Treat every worktree as task-scoped. Keep Go build and module caches in the
  repository-shared cache, while temporary files, binaries, and runtime state
  remain local to the worktree.
- When a task is closed, merged, abandoned, or otherwise no longer needs its
  worktree, stop its owned processes and verify that the tree is clean and has
  no unpreserved commits. Then remove the worktree and its merged local branch;
  remove the remote feature branch when that external mutation is authorized.
- Never keep a completed worktree merely as a build-cache archive. Never remove
  a dirty or unmerged worktree; report the exact state that must be preserved.
