# State storage simplification

Version: 0.1. Status: draft design for
[#205](https://gitcode.com/urandon/sessionless/issues/205).

Keep accepted conversation content and the smallest state that prevents
incorrect external effects durable. Keep intermediate execution detail only as
long as it is useful or necessary for recovery. Rebuild projections from their
authoritative inputs, and keep process-local observations out of the database
unless another process genuinely needs them. The complete physical inventory
is the [entity registry](../architecture/entity-registry.md).

This separates durability from physical layout: several durable facts can
share an existing scoped record, while some operational records still need
durability. Reducing tables is not permission to lose accepted answers, publish
an old worker's result or resend an ambiguously accepted provider request.

## Storage classes

| Class | State and consequence of loss | Proposed storage treatment |
| --- | --- | --- |
| Canonical durable | Accepted Session events, useful answers, artifact ownership, identity and current security decisions | Authoritative tenant-scoped YDB records; immutable large bytes in Object Storage. Acknowledgement must follow the required durable commit. |
| Correctness critical operational | Current execution owner/fence, cancellation authority, irreversible-effect identity and unresolved output or credential custody | The smallest bounded durable record until exact reconciliation or retirement. Prefer existing authoritative records and atomic updates. |
| Rebuildable projection | Lists, display summaries, ready/expiry access paths and other derived views | Rebuild from authoritative inputs. Use an index where its query and lifecycle semantics suffice; rebuilding must not replay model or upload effects. |
| Process local ephemeral | Uncommitted progress, local timers, callback state, parser buffers, transient diagnostics | Memory or bounded local temporary state. Loss stops that execution; it does not grant a replacement external send. |

These are proposed review classes, not claims that existing consumers already
use the smaller model. Every replacement specifies what loss the product may
tolerate and how it fails visibly. TTL is a cleanup mechanism, not fencing,
revocation or proof that an effect never happened.

## Run Attempt and Lease

The existing [Run](../../internal/domain/run.go) carries canonical linkage,
idempotency, status and cancellation. The existing
[Attempt and Lease](../../internal/domain/work.go) carry attempt identity,
worker, fence and expiry. Their complete persisted representation must not be
confused with the minimum facts required for correctness.

| Logical entity | Minimum durable facts to assess | Detail that may be bounded or derived | Failure rule |
| --- | --- | --- | --- |
| Run | Accepted input/Session identity, idempotency association, accepted terminal result and current cancellation/authorization reference | Display status, timing breakdown and historical execution detail where reconstructible | Losing progress may produce a visible interrupted/unknown state, not lose an acknowledged answer or create a new send. |
| Attempt | Current winning attempt/worker and the exact accepted or ambiguous effect identity until reconciliation | Historical attempts, diagnostic observations and progress with an explicit retention limit | A replacement cannot infer that an unknown previous call was unsent. Keep only the facts needed to reconcile or deny replay. |
| Lease | Current owner, monotonic fence and applicable expiry while execution can mutate authoritative state | Superseded lease bodies and expiry indexes after their recovery dependencies retire | An expired or lost lease denies further publication. Removing history must not let a delayed old worker regain authority. |
| Worker job | Exact admitted input/resource/profile/context references required to execute or reconcile the current attempt | Duplicate copies of already immutable data and retired job material | Reconstruction uses admitted immutable inputs; it cannot invent a new resource or reset consumed permission. |
| Effect or output custody | Exact operation identity, disposition and ownership until accepted/ambiguous effects are reconciled | Redundant explanatory copies that cannot change the decision | A timeout is not proof of nonacceptance, quiescence or deletion. |

The first implementation study should test whether current attempt and lease
facts can live in the existing Run/execution-owner record rather than require
independent permanent bodies and separate heads. This is conditional on the
real contention keys, resource-concurrency rules, deletion and pending-effect
joins. It is not a new execution state machine or a second accounting ledger.

A deliberately lossy local development profile may stop and require explicit
operator intervention after a crash. That tradeoff can simplify progress and
diagnostic retention, but cannot turn missing state into permission to repeat
an external request. Production/multi-owner guarantees and the local profile
must be named separately rather than silently weakened together.

## Physical reduction candidates

The [problem register in #205](https://gitcode.com/urandon/sessionless/issues/205)
tracks evidence and disposition. The initial candidates are:

1. Complete retirement analysis for the old/new `lease_expiry`,
   `dispatch_ready`, `quota_expiry` and `telegram_delivery_ready` generations.
   Existing writers still maintain both versions; removal needs actual consumer
   and rollout evidence, not an unused-name assumption.
2. Co-locate compatible current execution fields in existing authority records,
   retaining the necessary fence and pending effect/custody identity. Bound or
   retire intermediate Attempt/Lease history after its actual consumers retire.
3. Consolidate one-time cutover/backfill metadata only when keyed scope,
   retention, verification and deployment authority are compatible. A typed
   namespace cannot become a generic approval or renewal API.
4. Review separately stored current-policy and audit records for reuse of the
   existing authenticated resource/owner decision. Keep a real decision and its
   revocation semantics; do not add a parallel permission issuer per adapter.
5. Replace manual lookup projections with suitable native indexes or explicit
   rebuilds where bounded reads, deletion ownership and atomicity are preserved.
   Queue wakeups remain hints; losing one must not lose accepted work or make
   it safe to repeat an unknown effect.

No target table count is chosen from competitor counts. Each accepted change
must reduce a named table, durable field, duplicate write or retained history
and state how that reduction is measured.

## Harness comparison

The source comparison uses the repository's recorded competitor snapshots,
not a claim about their latest releases:

| Harness | Persistence observation | Relevant lesson |
| --- | --- | --- |
| [Pi](https://github.com/earendil-works/pi/blob/6c4f360264397c59801f6da2bdac13e3b1fcbe91/packages/coding-agent/src/core/session-manager.ts) | SessionManager persists append-only JSONL trees | Conversation history and branching do not require a separate relational table per event subtype. |
| [OpenCode](https://github.com/anomalyco/opencode/blob/3a31c4ea801915c0b050df4b3842997ea62b6e93/packages/core/src/database/schema.gen.ts) | Generated SQLite baseline has 19 tables including sessions, messages, parts, events, accounts and permissions | Group records around actual entities and query paths; migration history and legacy storage are not the live entity count. |
| [Hermes](https://github.com/NousResearch/hermes-agent/blob/c80a0a551c7038517456ee0aeb60203ec92aedb6/hermes_state_common.py) | Main SCHEMA_SQL has 11 base tables, with additional FTS/feature storage | Bounded current leases and diagnostics can be distinguished from useful message history. |
| Codex | [Official documentation](https://learn.chatgpt.com/docs/config-file/config-reference) describes SQLite runtime state and JSONL history; the inspected source separates state/history/goals/queue/memory/log migration sets | Multiple storage lifecycles are legitimate, but do not imply one table per intermediate observation. |

Sessionless also coordinates multiple principals, workers, queues, credentials
and external effects. Those require a larger correctness kernel than a local
single-user harness. They do not justify indefinite retention of every derived
status. A managed-checkpoint comparison and the actual write-path measurements
remain required before selecting the runtime reduction design.

## Implementation and verification order

1. Keep the physical registry complete and map each proposed reduction to its
   logical entity, authority, readers, writers and retirement owner.
2. Review the current Run/Attempt/Lease transaction and effect graph. Specify
   accepted loss semantics and the durable kernel; do not infer it from names
   or primary keys.
3. Independently review one coherent current-execution layout and a separate
   obsolete-access-path retirement plan. Link bounded implementation children
   in #205 with before/after storage and write counts.
4. Implement disabled or compatible changes with old/new record mapping and
   focused deterministic failure/recovery tests. Source changes require review
   and exact-head CI; they do not apply a shared migration.
5. Under separately approved rollout authority, reconcile deployed readers,
   writers and retained data before removing physical tables. Measure the
   resulting schema, transaction work and bootstrap behavior.

Verification covers crash before/after claim, expired/lost lease, stale worker,
cancel/revoke, ambiguous provider/upload acceptance, duplicate ingress and
receipt/ACK loss. Restored legitimate controls must still deliver the exact
durable answer/artifacts with no effect replay. Each removed projection has a
bounded reconstruction or an explicit unsupported/reconcile-only state.

This draft changes neither applied migrations nor existing runtime consumers.
[#174](https://gitcode.com/urandon/sessionless/issues/174) separately compacts
the migration baseline. Reports and execution history belong in #205 comments,
not in this design or the original issue description.
