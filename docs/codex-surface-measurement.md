# Codex execution-surface measurement

Status date: **2026-08-24**. Scope: credential-free half of issue
[#64](https://gitcode.com/urandon/sessionless/issues/64). The authenticated
phase is intentionally paused until the operator explicitly authorizes use of
their ChatGPT subscription and the exact fixed, non-secret input.

## Preliminary decision

The credential-free evidence changes the provisional #62 decision:

- direct App Server remains useful as the richest research protocol, but is a
  production `no-go` while the command is documented as experimental and
  unsupported;
- stable Python SDK `0.147.0` is also a current `no-go` for the unattended
  Sessionless boundary, despite being the officially recommended automation
  surface: its measured public high-level defaults and abstractions do not
  preserve required environment, approval, restricted-read-root, and typed
  quota guards;
- `codex exec --json --ephemeral` is the only remaining candidate for the
  explicitly consented phase. It is not approved yet: external isolation,
  account route, cancellation, refresh behavior, quota visibility, resource
  cost, and ambiguous completion still require real measurements.

This is not a recommendation to resume #61. No provider call, login, auth-cache
read/copy, or prompt submission occurred in this phase.

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
opt-in research dependency, not a Sessionless runtime or CI dependency.

| Artifact | Version / size | SHA-256 |
| --- | --- | --- |
| Direct Codex binary from the installed ChatGPT app | `0.148.0-alpha.15`; 212,613,840 bytes | `7645c3caf5607e4528eb3a15b12496c284c2a918939aed34e863c760c1b421e7` |
| Direct stable v2 App Server schema | 526,247 bytes | `a7cc806f2845736f1176418b97d8eefd239c2e049cb643eee405f1ce07ccb198` |
| `openai-codex` wheel | `0.147.0` | `ab2e0b3a41dba5a62be8561397cf3e7913afb53b5372ad881002a6f0b77e6a0a` |
| SDK-bundled Codex runtime | `0.147.0`; 219,997,536 bytes | `19c4f144c5226a9f17c58e6f0fa854843b0f77a6eb420f40e2745a12f10f5d37` |
| SDK runtime stable v2 schema | 513,375 bytes | `f3dec1e031d99a420b137b903f02196d4325eece57620c925bb7130b25f168d2` |
| Isolated SDK virtual environment | 291,280 KiB | closure hashes in the pinned fixture |

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
this unattended use case.

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

## Consent gate and remaining work

Before the authenticated phase the operator must explicitly authorize both:

1. using their existing local ChatGPT/Codex subscription login in this
   attached-worker experiment without copying it into the repository or a
   cloud worker;
2. sending the exact disclosed fixed, non-secret benchmark text to OpenAI for
   at least 30 cold invocations per eligible surface.

Without that authorization, stop here. After authorization, run only the
surfaces still eligible under credential-free gates, measure completion,
interrupt/deadline/process-loss/ambiguous outcomes and refresh/write-back, and
update #62/#61/#47/#48/#63 with a final decision. If `exec` cannot prove the
ChatGPT billing route or sufficient cancellation/isolation semantics, the
correct result is that no current surface is eligible—not a fallback to the
API or experimental App Server.
