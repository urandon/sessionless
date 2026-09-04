# Go-supervised Codex exec backend

Status date: **2026-08-26**. This is the bounded first implementation slice of
[#81](https://gitcode.com/urandon/sessionless/issues/81). It is a disabled
backend contract and credential-free/fake-process evidence, not production
provider enablement.

## Architectural position

Sessionless owns the harness. Canonical sessions, runs, attempts, leases,
fences, events, terminal commit, tools, and retries never become Codex state.
`internal/codexexec` is one backend below the future closed
`SessionlessHarnessV1` registry; it is deliberately not a `HarnessDriver` and
is not selected from the environment, an installed binary, an available
credential, or a live model catalog.

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

The adapter verifies an exact owner/worker/connection/run/attempt/lease/fence
authority, attached-worker capability and policy digests, and the complete
immutable subscription resource binding. The outer registry
must later derive that authority from the admitted canonical binding; remote
inputs never choose its fields.

The executable path, version evidence, SHA-256 digest, and model are local
configuration. The fixed command is:

```text
codex exec --json --ephemeral --ignore-user-config --ignore-rules \
  --strict-config --sandbox read-only --skip-git-repo-check \
  --color never --model <sealed-model> -
```

The instruction is bounded UTF-8 stdin, never argv or environment. The
supervisor supplies a replacement environment, and the existing credential
lifecycle adds only invocation-scoped `CODEX_HOME`, permits writes only to the
exact `auth.json`, then performs bounded write-back and release. An
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

## Why production remains disabled

The package is not wired into any binary. Its local `Enabled` field is only a
reversible component gate and is not provider authorization. Production still
requires all of the following:

- the accepted #48 authorization tuple for personal subscription, owner-managed
  attached worker, local credential custody, and exact Codex exec surface;
- a canonical immutable `HarnessBindingV1` in admission/job/execution and a
  closed registry that keeps the signed worker harness identity Sessionless-
  owned while separately advertising backend descriptors;
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

Pure reducer tests cover clean completion, pre/post-acceptance loss, duplicate
and post-terminal events, unexpected effects, malformed/unterminated/duplicate
JSON, and bounds. The real supervisor fake-executable composition proves exact
argv, stdin privacy, replacement environment, pinned digest, invocation-scoped
credential materialization, write-back/release, and cleanup. Existing AW-05
tests continue to own cancellation, deadline, TERM-resistant descendants,
natural leader loss, no-late-output, and isolation-boundary teardown.

No live provider call or secret is required or allowed in this phase.
