# Go-supervised Codex exec backend

Status date: **2026-09-17**. This is the feature-disabled implementation of
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

No shipped stack or foreground constructor selects the prepared executor.
It is not an invocation replay latch: the existing accepted-attempt owner and
one-shot prepared Codex process boundary retain that authority. Remaining #133
work must preserve typed accepted job/binding/context after proof verification,
privately stage `context/history.jsonl`, and join bounded semantic output with
actual process observations into a receipt candidate only after credential
finalization. Do not recover authority from the synthetic stdin envelope,
expose `Stack`'s raw supervisor, or wrap another lifecycle around this runner.
Normal provider materialization, runtime activation and OCI egress remain
disabled; this library seam is not real-provider rollout or completion of #133.

## Why production remains disabled

The package is not wired into any binary. Its local `Enabled` field is only a
reversible component gate and is not provider authorization. Production still
requires all of the following:

- the merged #48 `OAI-SUB-2026-09-PLUS-PRO-LOCAL` conditional authorization
  tuple for
  eligible personal subscription, owner-managed attached worker, local credential
  custody, and exact Codex exec surface, including its expiry/re-review gate;
- product foreground activation of the already concrete authority resolver and
  prepared process/cancellation composition. The closed registry carries the
  production-shaped driver but keeps registration and the command path disabled;
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
