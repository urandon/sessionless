# Codex execution-surface measurement

Status date: **2026-08-25**. Scope: credential-free comparison and the first
explicitly consented authenticated measurement from issue
[#64](https://gitcode.com/urandon/sessionless/issues/64). The task remains open:
the measurement identifies a viable attached-worker candidate, but does not
establish a provider-policy, quota, cancellation, or cloud-custody production
go.

## Preliminary decision

The credential-free evidence changes the provisional #62 decision:

- the Sessionless production harness must remain compatible with the existing
  Go/serverless deployment model. It must not add Python, a Python SDK, or a
  Python runtime to the worker image or mandatory build/test path. A production
  adapter may invoke an explicitly pinned external Codex process, but the
  Sessionless-owned runtime and orchestration remain Go binaries;

- direct App Server remains useful as the richest research protocol, but is a
  production `no-go` while the command is documented as experimental and
  unsupported;
- stable Python SDK `0.147.0` is a research comparator only and is also a
  current `no-go` for the unattended
  Sessionless boundary, despite being the officially recommended automation
  surface: its measured public high-level defaults and abstractions do not
  preserve required environment, approval, restricted-read-root, and typed
  quota guards;
- `codex exec --json --ephemeral` is the only remaining Python-free candidate.
  A consented 30-sample run establishes the happy path on a user-owned attached
  macOS worker, but it is not approved for production: authoritative account
  route, quota visibility, cancellation, refresh behavior, full isolation,
  resource cost, and ambiguous completion remain release gates.

This is not a recommendation to resume the paused App Server implementation in
#61. The credential-free phase made no provider call or auth-cache access. The
authenticated phase used a dedicated isolated login after explicit operator
consent. The Sessionless measurement orchestration did not read or copy the
operator's global cache; the Codex child necessarily read the task-owned
`CODEX_HOME/auth.json` created by that isolated login. Neither credential bytes
nor cache contents were printed or committed.

## Current official contract

The [App Server guide](https://learn.chatgpt.com/docs/app-server) says that the
App Server command and WebSocket transport are experimental and unsupported for
production workloads. It also defines a stable protocol subset selected by
`experimentalApi:false`. Its `readOnly` policy defaults to full filesystem read
when `access` is omitted, so a mode-only request is not a restricted-root
boundary.

The [Codex SDK guide](https://learn.chatgpt.com/docs/codex-sdk) recommends the
SDK for application/automation integration. The stable Python package owns a
local App Server runtime and pins a matching CLI binary. The
[non-interactive guide](https://learn.chatgpt.com/docs/non-interactive-mode)
documents `codex exec`, JSONL output, ephemeral operation, and sandbox choices.
Official support of a surface and suitability of its defaults are separate
questions; this spike tests both.

## Exact measured artifacts

Host: macOS arm64. Python: CPython 3.12. SDK closure is recorded in
`test/fixtures/codex-surface/python-sdk-darwin-arm64.requirements.txt` and is an
opt-in research dependency, not a Sessionless runtime or CI dependency. Its
measurements may falsify assumptions about the other surfaces, but cannot make
the Python SDK eligible for production selection.

| Artifact | Version / size | SHA-256 |
| --- | --- | --- |
| Direct Codex binary from the installed ChatGPT app | `0.148.0-alpha.15`; 212,613,840 bytes | `7645c3caf5607e4528eb3a15b12496c284c2a918939aed34e863c760c1b421e7` |
| Direct stable v2 App Server schema | 526,247 bytes | `a7cc806f2845736f1176418b97d8eefd239c2e049cb643eee405f1ce07ccb198` |
| `openai-codex` wheel | `0.147.0` | `ab2e0b3a41dba5a62be8561397cf3e7913afb53b5372ad881002a6f0b77e6a0a` |
| SDK-bundled Codex runtime | `0.147.0`; 219,997,536 bytes | `19c4f144c5226a9f17c58e6f0fa854843b0f77a6eb420f40e2745a12f10f5d37` |
| SDK runtime stable v2 schema | 513,375 bytes | `f3dec1e031d99a420b137b903f02196d4325eece57620c925bb7130b25f168d2` |
| Isolated SDK virtual environment | 291,280 KiB | closure hashes in the pinned fixture |

The live probe additionally fail-closes on the runtime binary digest and the
exact installed SDK files that define process launch, the high-level API, and
sandbox mapping: `client.py`
`76bdb1e63c62987c3530ea763e9655a06b308cbc4e18cb51958e85b6c23aec3b`,
`api.py` `673defd0ccf1348a86c2bb589cb3a1a69cb315b0a3ecb29525c52f0515a82476`,
and `_sandbox.py`
`01ab6cabc1642941ba958b287c34a5475066c934b67e4fd194d78b4bb2eb27b2`.

The direct and SDK ClientRequest method sets are equal in this sample, but the
stable schemas are not byte-identical (288 versus 285 generated files). An SDK
upgrade is therefore a protocol/runtime upgrade and must regenerate schema,
rerun fixtures, and repeat the comparator.

## Credential-free results

Each probe used a fresh `0700` root containing new `HOME`, `CODEX_HOME`, temp,
and workspace directories. The child environment was an allowlist and did not
contain `OPENAI_API_KEY`, provider credentials, host config, or the operator's
Codex home. Reports contain booleans, version/platform, durations, and stable
finding codes only.

| Surface | Three cold credential-free samples | Result | Evidence |
| --- | ---: | --- | --- |
| Stable Python SDK + pinned runtime | about 0.36–0.54 s | **no-go** | Initialization and unauthenticated account route work. Published `CodexConfig.experimental_api` defaults true; `CodexClient.start` begins with `os.environ.copy()`; the default server-request handler accepts command and file approval requests; high-level `Codex` has no approval-handler injection; public read-only preset emits no restricted `access`; no typed account rate-limit read method exists. A low-level client can override some behavior, but that ceases to be the smallest safe high-level surface and still lacks the root/quota contracts. |
| Direct stable App Server subset | about 42–52 ms | **no-go** | `experimentalApi:false`, clean account route, private stdio and isolated environment work. The command is not production-supported, and the current Phase-A thread request uses `read-only` without explicit restricted roots. |
| `codex exec` contract | about 8–9 ms for help/contract startup | **candidate** | Exact binary exposes `--json`, `--ephemeral`, `--ignore-user-config`, `--ignore-rules`, and `read-only`. These timings are contract-probe startup, not provider-turn latency. Account/quota parity and cancellation are not established. |

The SDK statements above are observations of the exact pinned wheel, not claims
about every future SDK. The probe supplies an explicit rejecting low-level
approval handler so the credential-free initialization itself cannot accept a
server request; it separately records that the published default is unsafe for
this unattended use case. Even if a later Python SDK fixes these behavioral
gaps, it remains comparator-only unless the Go/serverless deployment
requirement is explicitly superseded by a separate architecture decision.

## Consented authenticated result

The operator explicitly authorized use of their existing ChatGPT/Codex
subscription login and the disclosed fixed, non-secret benchmark input. On
2026-08-24 the pinned `codex-cli 0.148.0-alpha.15` binary
(`sha256:7645c3caf5607e4528eb3a15b12496c284c2a918939aed34e863c760c1b421e7`)
ran 30 sequential cold `codex exec --json --ephemeral` processes on Darwin
arm64 with model `gpt-5.4` and low reasoning. Every sample used a fresh `0700`
workspace and temporary directory, read-only sandbox, ignored user config and
rules, no API-key environment fallback, and no tools or persistent provider
thread.

| Metric | min | p50 | p95 | p99 | max |
| --- | ---: | ---: | ---: | ---: | ---: |
| Completion | 6,119 ms | 9,864 ms | 23,000 ms | 26,471 ms | 26,471 ms |
| Spawn to first JSONL event | 96 ms | 101 ms | 223 ms | 996 ms | 996 ms |
| Stdout | 357 B | 359 B | 359 B | 359 B | 359 B |
| Stderr | 39 B | 39 B | 182 B | 182 B | 182 B |
| Invocation temporary disk after exit | 0 B | 0 B | 0 B | 0 B | 0 B |

All 30 processes exited zero, emitted bounded valid JSONL, returned exactly the
required marker, and emitted no command, file-change, MCP, dynamic-tool,
web-search, approval, or user-input event. Public evidence contains aggregates
and stable finding codes only. Raw prompt/output, identity, account, auth,
token, URL, provider-error, and protocol-frame data were neither published nor
placed in the repository.

This is **positive feasibility evidence**, not a production pass. `exec` does
not expose an authoritative ChatGPT billing-route assertion or the App Server's
account/rate-limit observations. Its process contract has no in-band typed
interrupt acknowledgement: the Go supervisor must cancel by terminating and
reaping the process group, and loss after provider acceptance but before one
validated terminal event remains an ambiguous attempt. Sessionless can safely
retry the current read-only, side-effect-free turn from the same immutable
context; future effects require idempotency keys and an effect ledger. The
credential-lifecycle service already supplies fenced materialization,
generation-CAS write-back, crash recovery, and deny-first revocation, but these
contracts have not yet been composed with the real `exec` process.

Accordingly the evidence changes the decision to:

- retain `codex exec` as the sole candidate for a minimal Go-supervised
  attached-worker adapter;
- do not use App Server or the Python SDK as a production dependency;
- keep the App Server-only #61 spike paused and replace its intended production
  activation with a dedicated Go-supervised `codex exec` adapter task; that task
  must not wire `worker-runtime` until OS-level cancellation, terminal-event,
  credential write-back, external isolation, and #18 reachability tests pass;
- treat account route and quota as separately refreshed `AIResource`
  observations; if no supported authoritative source exists, expose them as
  unknown and deny policy that requires them rather than inventing values;
- keep cloud consumer-credential custody and subscription federation disabled
  until #48 records explicit provider authorization for those deployment
  tuples.

## Reproducible runner

The Go command is intentionally not wired into `make ci` because the real
binary and optional SDK wheel are local research inputs:

```sh
/opt/homebrew/bin/python3.12 -m venv .build/codex-sdk-venv
.build/codex-sdk-venv/bin/python -m pip install \
  --only-binary=:all: --require-hashes \
  -r test/fixtures/codex-surface/python-sdk-darwin-arm64.requirements.txt

go run ./cmd/codex-surface-probe \
  --surface app-server \
  --codex-bin /absolute/path/to/pinned/codex \
  --scratch /absolute/private/scratch \
  --iterations 3 \
  --output .build/codex-surface-contract/app-server.json

go run ./cmd/codex-surface-probe \
  --surface exec \
  --codex-bin /absolute/path/to/pinned/codex \
  --scratch /absolute/private/scratch \
  --iterations 3 \
  --output .build/codex-surface-contract/exec.json

go run ./cmd/codex-surface-probe \
  --surface python-sdk \
  --python-bin .build/codex-sdk-venv/bin/python \
  --scratch /absolute/private/scratch \
  --iterations 3 \
  --output .build/codex-surface-contract/python-sdk.json
```

`internal/codexsurface` makes executable and scratch paths absolute before the
child changes directory. This prevents a relative-child-cwd failure from being
misclassified as protocol drift. It also avoids the macOS `/var` to
`/private/var` canonical-path mismatch seen when an App Server reports its
resolved Codex home. Every helper command has a wall-clock deadline, bounded
stdout/stderr, wait/reap, and Darwin/Linux process-group kill so a stalled SDK
wrapper cannot leave its bundled App Server descendant alive.

## Authenticated measurement contract

`PublicMeasurementReport` is aggregate-only and requires at least 30 samples
per surface. Every included metric must cover all samples and reports only
`min/p50/p95/p99/max`, counts, booleans, versions, platform, and stable failure
or finding codes. Planned metrics are spawn, initialization, account read,
thread start, first event, first token, completion, teardown, peak RSS,
temporary disk, descendant count, and event bytes. A surface that cannot expose
a lifecycle metric records a capability finding; it does not invent a zero.

Private raw timing/resource observations may live only under a task-owned
`0700` ignored `.build` directory for the duration of the experiment. Even the
private record must not contain prompts, model output, token strings, account
identifiers/email, auth/device URLs or codes, raw provider errors, or protocol
frames. Public evidence is generated from validated aggregates, then raw local
observations are deleted after review. Nothing is uploaded to CI artifacts.

The same fixed bounded text input, immutable Sessionless context bytes, model,
reasoning settings, external mount/network policy, and timeout must be used for
all eligible surfaces. There is no mid-attempt fallback and no API-key fallback.

## Remaining work and stop conditions

The consented happy-path phase is complete. Further provider calls still need
their own bounded experiment definition and must reuse a user-owned attached
worker; they must not copy the login into the repository or a cloud worker.
The next implementation/evidence slice must test OS-level cancellation,
deadline escalation, child-process loss before/after terminal JSONL, exact
credential refresh/write-back and restart recovery, external filesystem-read
isolation, peak RSS/descendants, and account/quota observation freshness.

Issue #64 stays open until those failure-path results and the policy verdict in
#48 are attached. Stop rather than proceed when any of these conditions holds:

- the only way to obtain a required signal is an unsupported/private API;
- an API-key route appears where a ChatGPT subscription resource was selected;
- cancellation leaves descendants or cannot bound additional output/work;
- an ambiguous attempt can produce an unledgered external effect;
- credential mutation cannot be serialized and fenced across restart;
- the external isolation boundary exposes host credentials, sibling workspaces,
  or undeclared mounts;
- provider authorization for the exact placement/custodian/sharing tuple is
  missing, expired, or ambiguous.

There is no fallback to the OpenAI API, Python SDK, or experimental App Server.
If `exec` cannot pass these gates, the honest result is that no current
subscription-backed production surface is eligible.
