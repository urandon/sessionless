# Go-supervised Codex exec backend

Status date: **2026-10-11**. This is the feature-disabled implementation of
[#81](https://gitcode.com/urandon/sessionless/issues/81). It is a disabled
backend contract, canonical `HarnessDriver` bridge, concrete accepted-authority
resolver, prepared Go-supervisor boundary, and fake-process evidence,
not production provider enablement.

## Architectural position

Sessionless owns the harness. Canonical sessions, runs, attempts, leases,
fences, events, terminal commit, tools, and retries never become Codex state.
`internal/codexexec` is one backend below the closed `SessionlessHarnessV1`
registry. The exact executable, protocol, model, limits, and file-credential
delivery profile now have a disabled registry descriptor, but the legacy
invocation adapter remains a lower-level compatibility fixture. The production
composition accepts the separate `codexexec.Driver`, which implements
`ports.HarnessDriver`. `NewPreparedAttachedWorkerDriverV1` composes its concrete
resolver over one immutable accepted-attempt snapshot and its exact pinned
prepared-process boundary. It is not selected from the environment, an
installed binary, an available credential, or a live model catalog.

The same outer harness will eventually route an already-admitted immutable
binding to distinct backends such as:

- `codex_exec_subscription_v1`;
- `codex_exec_openrouter_v1`;
- `opencode_openrouter_v1`;
- `pi_openrouter_v1`;
- a future Sessionless minimal-agent `direct_openrouter_v1`.

There is no mid-attempt fallback among them. A Codex subscription and an
OpenRouter API resource are different billing, credential, policy, and data
egress authorities even if the selected model family overlaps.

Codex, OpenCode, and Pi integrate at the process/RPC backend layer. A direct
OpenRouter call belongs at a narrower provider port used by a Sessionless-owned
agent loop. “OpenAI-compatible” describes a wire family only; it does not make
route policy, privacy, quota, cost, errors, or actual provider evidence
interchangeable. OpenRouter Ox Alpha is eligible only for the accepted
`externally_shareable` cohort: public research, public data, generated
fixtures, and reviewed open-source work without private inputs.

## Implemented bounded contract

The concrete resolver consumes one immutable, owner-scoped attempt snapshot only
after the attached-worker protocol has accepted it. It performs no central-store
read from the user-owned worker. Execution accepts only a live `claimed`
snapshot; cancellation may also use the exact `cancel_requested` or
`cancel_acknowledged` snapshot without reviving execution. The driver verifies
the resulting exact owner/worker/connection/run/attempt/lease/fence
authority, attached-worker capability and policy digests, and the complete
immutable subscription resource binding. Its resolver must project the fields
which are intentionally absent from provider-neutral `ExecutionRequest`
(connection/enrollment generations, reservation, context digest, and lease
expiry) from the already accepted attached-worker session. A missing, stale,
expired, or cross-scope projection fails before process execution; the driver
never manufactures those values.

Canonical preflight happens before the worker claims its execution lease, so it
validates only the immutable binding and attached-worker placement. It neither
resolves nor fabricates post-claim lease authority. Execute resolves the full
live authority after canonical orchestration has claimed the lease and rejects
an expired invocation credential before reaching the process boundary. The
prepared boundary independently rechecks the minimum lease, credential, and
harness-evidence lifetime immediately before supervisor preparation, carries
that remaining lifetime as the supervisor context deadline, and the supervisor
checks it again after isolation preparation immediately before `Start`.

One prepared boundary is a one-shot capability for exactly one accepted
attempt/reservation. Its state advances `not_started -> running -> terminal`;
the capability is consumed before credential binding or process preparation,
including when preparation fails. A sequential or concurrent replay therefore
cannot produce a second provider effect. This stateful latch is what backs the
reported provider-effect fence; the request's numeric limit alone is not
evidence of fencing.

The executable path, version evidence, SHA-256 digest, and model are local
configuration. The fixed command is:

```text
codex exec --json --ephemeral --ignore-user-config --ignore-rules \
  --strict-config --sandbox read-only --skip-git-repo-check \
  --color never --model <sealed-model> -
```

The bridge compiles bounded UTF-8 stdin from the descriptor-pinned canonical
`context/history.jsonl`, never from argv, environment, or an unverified
caller string. The
subscription profile uses a private credential-only `CODEX_HOME`; the
Codex/OpenRouter profile instead loads a complete, generated and digested
`CODEX_HOME/config.toml` plus local model catalog. Neither profile can reach an
ambient user home. The supervisor supplies a replacement environment, and the
existing canonical credential lifecycle adds only invocation-scoped authority,
then performs bounded CAS finalization and release after the driver returns.
The bridge consumes that
already prepared handle and exact direct-child `auth.json`; it cannot perform
a second Issue/Materialize cycle. Its process boundary reports that refreshed
local credential state is quiesced and safe for the outer lifecycle to write
back; it does not claim that the later CAS has already succeeded. An
admission-pinned credential generation is checked before process spawn. The
reviewed isolation launcher must attest this exact inner Codex artifact and
argv independently of its outer container/bwrap/VM client command.

The strict JSONL reducer accepts only one ordered turn:

1. one `thread.started`;
2. one `turn.started` (provider acceptance boundary);
3. bounded reasoning/agent-message items and exactly one final agent message;
4. exactly one `turn.completed` and no later event.

Unknown/effect-bearing items, duplicate case-folded JSON keys, malformed or
unterminated frames, duplicate terminals, post-terminal output, and byte/event/
depth limits fail closed as non-retryable `protocol_drift` (or
`terminal_protocol_drift` after a terminal). Only a clean exit before
`turn.started` is `pre_acceptance`; malformed or effect-bearing output cannot
claim that replay-safe class. Native IDs, reasoning, provider error bodies, raw
frames, prompts, credentials, and stderr are not representable in public
evidence.

The result keeps these facts independent:

- provider lifecycle classification;
- provider acceptance and native terminal observation;
- process-group stop and isolation cleanup;
- credential write-back/release finalization;
- bounded private final candidate;
- deterministic content-free evidence digest;
- billing route and quota, both `unknown` until an authorized surface proves
  them.

`completed_with_teardown_failure` may retain the private final candidate and
evidence digest to prevent an unsafe replay, but is not canonical success.
The adapter never emits a cancellation acknowledgement or terminal commit;
AW-04 owns those durable facts. There is no internal provider retry.
Cancellation and finish arbitrate under the same lock: a successful boundary
cancel means cancellation won while the attempt was active and is reflected in
the returned process observation; once finish wins, later cancellation reports
not-active instead of a false acknowledgement.

## Shared prepared execution owner — 2026-10-09

The local preparation for [#133](https://gitcode.com/urandon/sessionless/issues/133)
adds `attachedworkerdaemon.NewPreparedInvocationRunner`. It configures a
`PreparedProcessRunner` inside the existing `InvocationRunner`, not another
credential owner. Both the ordinary and prepared modes use the same
Issue → Materialize → execute → WriteBack → Release implementation and bounded
finalization contexts. The returned handle must match the complete admitted
resource binding, including revision, as well as owner/run/attempt/worker/lease
and credential generation. A mismatch retires acquired authority before any
executor call.

The executor receives `PreparedInvocationV1`: identity, cloned credential-bound
process metadata, and already-issued provider-neutral credential/materialization
projections. It gets no lifecycle port and returns only actual `AttemptResult`
and error; credential release/generation/finalization evidence is assigned by
the outer runner. Borrowed stdin is cleared when the prepared call returns.
This private input is excluded from JSON and formatted without scope, paths,
prompt or credential details. Prepared mode requires credentials explicitly and
denies credentialless input; it has no fallback to the ordinary process runner.

The offline joined test feeds those once-prepared projections into the existing
Codex `Driver.Execute` using a fake process boundary. It verifies one lifecycle
sequence and one process call, semantic summary/checkpoint reduction, ambiguous
protocol failure without retry, and finalization failure overriding driver
success. The fake supplies process cleanup observations separately: successful
driver reduction alone is never proof that a supervisor/isolation boundary was
actually cleaned up. This joined test does not exercise the concrete
`NewPreparedAttachedWorkerDriverV1` constructor or a real Codex process; existing
runtime tests above own that constructor's fake-executable proof.

Run `make attached-worker-prepared-invocation-test` for both complete package
race suites and 20 shuffled focused repetitions. These tests need no network,
provider, YDB, model, credential bytes or new downloaded dependency.

The authenticated sealed-input authorization transaction now projects its
actual claimed `AttachedWorkerAttemptV1` along with the revision. Both input
authorization gates require the same exact head; the bounded HTTPS client and
materializer independently match it to the accepted request. The serialized
input budget includes that head even for credential-free snapshot input.

`VerifiedAttachedInputV1` preserves the typed job, manifest, projected history,
original canonical proof and artifact bytes after verification. It passes from
materialization through `Invocation` to `PreparedInvocationV1`, never by
decoding synthetic stdin. It is a private-runtime, redacted, non-wire carrier,
not an issuer or a new authorization decision. Each ownership boundary clones
finite input, and borrowed buffers are cleared after prepared execution; the
daemon clears its source-owned carrier on discard and before terminal reporting.
Prepared mode rejects absent or mismatched typed input before credential Issue.
Ordinary credential-free executable behavior and normal provider denial remain
unchanged. The strict sealed-input HTTPS response now includes `AcceptedAttempt`:
source and client must upgrade together. Old or missing-head responses are
rejected, with no legacy decoder or compatibility fallback. Profiles remain
disabled during this coordinated contract roll-forward.

## Explicit prepared Stack composition — 2026-10-11

The normal library `attachedworkerstack.New` can now select the concrete
text-only composition with an explicit `Config.PreparedCodex`. Nil keeps the
ordinary stack; shipped activation remains nil and denies provider input.
The selected artifact, fixed argv, model, original accepted head and immutable
subscription binding are rejoined before credential Issue. Missing typed input,
attachments, extra artifacts, workspace, skills, MCP and unsupported canonical history fail closed,
without silently discarding material or decoding synthetic stdin for authority.
The sole `message.json` / `application/json` ingress envelope is supported only
when its complete immutable reference and bytes equal the final projected
user-message trigger, including the original tenant, Session, author, Run and
admitted sequence boundary. The original verified projection can contain that
trigger in a snapshot; a tail-only proof is not required. This envelope is
consumed through canonical history, never staged as an arbitrary attachment.

Prepared Stack execution additionally requires one bind to the genuine Session
returned by the authenticated Connector. Preflight still pins/reconciles the
original manifest before any generation or network effect. Initial construction
binds only after readiness; reconnect additionally requires the existing idle,
no-terminal reconciliation. The bound generation is exactly manifest +1, never
a repaired snapshot. Each Run rechecks the original Session's current readiness,
authentication window and exact accepted-head connection/capability identity.
Absent, expired, closed, fenced, foreign or second bindings deny before Issue;
post-network construction failures close that same Session without reattaching.

`PreparedAttachedWorkerRunnerV1` privately stages the verified canonical
`context/history.jsonl` in fresh 0700/0600 task-owned storage, uses the original
constructor and real Go supervisor, and leaves the supervisor's attempt cwd
unchanged. Held directory descriptors bound cleanup to the original staging
objects. The sole outer `InvocationRunner` still owns Issue, Materialize,
WriteBack and Release; no second credential lifecycle is introduced.

Only the concrete prepared boundary can mint the private same-run observation,
after the supervisor returns and final cancel/finish arbitration settles.
The driver keeps its bounded semantic result separately from that actual
observation. Neither raw stdout nor a public observer callback supplies receipt
authority. A single private finalized slot joins the exact returned outer result
only after WriteBack/Release and successful staging cleanup. Unknown outcomes
and incomplete cleanup remain reconciliation-only, never resend authority.

Pinned foreground construction installs the concrete Stack candidate only when
the original output-receipt feature and publisher are present, rejecting a
caller-supplied builder. Candidate retries remain immutable; the normal adapter
retires the slot only after matching TerminalAck and receipt-journal retirement
(or the owning runtime closes). This is not a new durable replay owner: original
accepted-attempt authority and the one-shot boundary retain that responsibility.

Local fake-executable tests exercise normal Stack selection, genuine supervisor
observation, both cancel/finish orders, held/failing credential finalization,
owner/pin/staging denials and lost receipt/ACK responses without another spawn.
`TestWebAttachedPreparedCodexCanonicalReceipt` is the separately run,
`ydbintegration`-tagged two-owner fixture through actual Web admission, YDB
accepted head, TLS sealed input, normal Stack and canonical receipt finalizer.
Its engine/provider remain fixed test executables; compilation alone is not
native runtime evidence. The test-only producer/materializer select `CODEX_HOME`
only for the exact admitted Codex backend, and remain unavailable in shipped
constructors.

File/image output, real artifact/platform/egress evidence and live activation
remain separate unfinished gates. This text-only library delivery is neither
real-provider rollout nor completion of #133 or the MVP.

## Why production remains disabled

The package is not wired into any binary. Its local `Enabled` field is only a
reversible component gate and is not provider authorization. Production still
requires all of the following:

- the merged #48 `OAI-SUB-2026-09-PLUS-PRO-LOCAL` conditional authorization
  tuple for
  eligible personal subscription, owner-managed attached worker, local credential
  custody, and exact Codex exec surface, including its expiry/re-review gate;
- explicit product foreground activation of the concrete library composition.
  The closed registry and shipped command/profile keep provider selection disabled;
- reviewed production egress isolation. The current AW-05 isolation profile
  correctly requires `NetworkDenied=true`, so it can run fake contract tests
  but cannot perform a real provider turn;
- AW-04/AW-05/AW-07 recovery, revocation, cancellation, and two-owner security
  gates, including #79;
- exact official Codex artifact packaging, SBOM/provenance, conformance canary,
  and rollback;
- an owner-scoped production credential backend. No API key, provider login,
  or operator Codex home is used by this slice.

Do not weaken `NetworkDenied`, overload the single AW-02 harness manifest with
a Codex identity, or add a runtime default that treats a missing binding as
Codex/deterministic. Those changes would create silent egress/billing fallback
and a second provider state machine.

## Verification

The concrete runtime tests consume an accepted attempt snapshot, reuse an already
materialized `auth.json`, run the exact pinned argv through the real Go
supervisor, preserve the credential for the outer write-back owner, and route
exact cancellation to the active process. They also prove one-shot sequential
replay rejection, final expiry checks before isolation preparation and process
start, and the cancel/finish arbitration invariant under the race detector.

Pure reducer tests cover clean completion, pre/post-acceptance loss, duplicate
and post-terminal events, unexpected effects, malformed/unterminated/duplicate
JSON, and bounds. The real supervisor fake-executable composition proves exact
argv, stdin privacy, replacement environment, pinned digest, invocation-scoped
credential materialization, write-back/release, and cleanup. Existing AW-05
tests continue to own cancellation, deadline, TERM-resistant descendants,
natural leader loss, no-late-output, and isolation-boundary teardown.

Driver contract tests additionally cover canonical transcript compilation,
prepared-credential reuse, exact attached-worker authority, cross-owner and
stale-generation rejection, policy expiry, content-free terminal evidence,
accepted-outcome ambiguity, credential-quiescence/teardown failures, and
exact cancellation routing while the registration is disabled.

No live provider call or secret is required or allowed in this phase.
