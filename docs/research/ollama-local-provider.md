# Ollama-backed local development and federated provider

Research date: **2026-09-07**

Tracks: [#105](https://gitcode.com/urandon/sessionless/issues/105),
[#51](https://gitcode.com/urandon/sessionless/issues/51), and proposed PR-04 in
[AI resources and federation](ai-resources-and-federation.md)

Status: decision-ready design and experiment plan; no model was downloaded or
executed for this report

## Decision

Sessionless should add a first-class, feature-disabled **native Ollama** backend
for owner-scoped local endpoint resources. The backend should use Ollama's
native `/api/chat` stream and native inventory/lifecycle endpoints. It may share
bounded HTTP and event-decoding utilities with other adapters, but it must not
be implemented by relabelling the direct OpenRouter adapter or by assuming that
the OpenAI-compatible surface has identical semantics.

The first product shape is an attached-worker-local endpoint at a sealed
loopback address. Sessionless sends an admitted attempt to the owner's worker;
the worker talks to Ollama locally and returns fenced canonical evidence.
Federation beneficiaries receive a grant to that resource, never the endpoint,
an Ollama identity, or host credentials. The initial backend does not expose
Ollama on a LAN, tunnel it to the control plane, or enable Ollama Cloud.

Three test tiers remain separate:

1. deterministic fake HTTP/stream fixtures are the correctness and CI oracle;
2. an opt-in tiny real model proves local transport and lifecycle behavior;
3. an opt-in Qwen3.8-27B experiment measures the useful ceiling of the current
   24 GB Apple Silicon host.

Real model text is never an exact assertion. A real-model run may prove bounded
streaming, cancellation, tool-call shape, usage fields, model identity,
residency, and cleanup, but not deterministic product correctness.

## Evidence checkpoint

### Ollama contract

- The native API defaults to `http://localhost:11434/api`; the API is expected
  to remain backward compatible but is not strictly versioned:
  [API introduction](https://docs.ollama.com/api/introduction).
- Local access requires no authentication. Cloud models, private models, and
  direct `ollama.com` access are separate authenticated surfaces:
  [authentication](https://docs.ollama.com/api/authentication).
- Ollama exposes only parts of the OpenAI API. Its Responses support is
  non-stateful, and the compatibility surface cannot set context size directly
  without a separately created model:
  [OpenAI compatibility](https://docs.ollama.com/api/openai-compatibility).
- Native tool calling supports single, parallel, multi-turn, and streamed tool
  requests. The agent loop still belongs to Sessionless, not the model server:
  [tool calling](https://docs.ollama.com/capabilities/tool-calling).
- Native inventory returns the model digest, format, parameter size, and
  quantization level through `/api/tags`:
  [list models](https://docs.ollama.com/api/tags).
- macOS Apple M-series machines are supported, model storage can consume tens
  or hundreds of gigabytes, and the default location is under `~/.ollama`:
  [macOS documentation](https://docs.ollama.com/macos).
- `OLLAMA_MODELS` moves model storage. `OLLAMA_CONTEXT_LENGTH`,
  `OLLAMA_MAX_LOADED_MODELS`, `OLLAMA_NUM_PARALLEL`, `OLLAMA_MAX_QUEUE`,
  `OLLAMA_FLASH_ATTENTION`, and `OLLAMA_KV_CACHE_TYPE` control the relevant
  memory and admission envelope. `OLLAMA_NO_CLOUD=1` disables Ollama cloud
  features. `keep_alive: 0` or `ollama stop` unloads a model:
  [FAQ](https://docs.ollama.com/faq).

### Current model facts

The official Ollama catalog currently publishes:

| Role | Candidate | Published size | Initial decision |
| --- | --- | ---: | --- |
| Tiny real smoke | `qwen3.5:0.8b` | 1.0 GB | First transport/lifecycle probe. Compare usefulness with 2B and 4B, but keep only one tiny pinned digest in the normal local recipe. |
| Small local demo | `qwen3.5:4b` | 3.4 GB | Initial practical default if the tiny model cannot produce valid tool calls reliably. |
| Older coding control | `qwen2.5-coder:0.5b`, `1.5b`, or `3b` | model-dependent | Comparison only; do not choose it by inertia over current small Qwen3.5 models. |
| 24 GB ceiling | `qwen3.8:27b-mlx` or `qwen3.8:27b-q4_K_M` | 18 GB | Explicit bounded experiment, never default setup or CI. |

Sources: [Qwen3.5 Ollama tags](https://ollama.com/library/qwen3.5/tags),
[Qwen2.5-Coder](https://ollama.com/library/qwen2.5-coder), and
[Qwen3.8 Ollama tags](https://ollama.com/library/qwen3.8/tags). The upstream
[Qwen3.8-27B model card](https://huggingface.co/Qwen/Qwen3.8-27B) identifies a
28B-parameter Apache-2.0 BF16 model; the official
[Qwen3.8 repository](https://github.com/QwenLM/Qwen3.8) is the release and
deployment reference.

The 18 GB artifact leaves less than 6 GB of a 24 GB unified-memory machine for
macOS, Ollama, model state, the KV cache, the worker, and all other processes.
Therefore catalog size makes loading plausible, not safe or useful. Success is
defined by measured headroom and a bounded workload, not by reaching the first
token or by the advertised 256K context window.

### Local host baseline

Read-only inspection on 2026-09-07 found:

- Mac mini `Mac16,11`, Apple M4 Pro, 12 CPU cores, 24 GB RAM, arm64;
- Ollama CLI 0.31.1 at `/opt/homebrew/bin/ollama`;
- no running endpoint on `127.0.0.1:11434` and no Ollama application bundle;
- no configured `OLLAMA_*` variables;
- 610 MB under `~/.ollama`, all attributable to model/blob storage;
- no Ollama model store on the hubdisk-backed workspace.

This is readiness evidence only. The service was not started, model inventory
was not queried, and no model was pulled. Before any experiment, move or
re-pull only the known Ollama model store into a dedicated hubdisk directory
and set `OLLAMA_MODELS` explicitly. Never replace `~/.ollama` wholesale because
it may also contain configuration and identity material.

## Competitor evidence

Pinned competitors illustrate useful mechanisms and failure modes; they do not
define Sessionless authority.

| Project | Pinned evidence | Lesson |
| --- | --- | --- |
| OpenAI Codex | [`333c41e`](https://github.com/openai/codex/tree/333c41eef6b9ba3697fe913973fd58afe32d1ef5) uses its configured Responses wire for model traffic, while a separate native Ollama client probes `/api/tags` and `/api/version` and manages explicit pulls. | Separate provider invocation from native discovery/lifecycle. Version-gate the selected wire surface. Sessionless should not auto-pull a default model. |
| Zed | [`d9ad6af`](https://github.com/zed-industries/zed/tree/d9ad6aff67e47de43abb270d22de75dd950f1b48) has a first-class native Ollama crate using `/api/chat`, `/api/tags`, and `/api/show`; it sends `num_ctx` explicitly and exposes auto-discovery policy. | Native capability and exact context observations are worth preserving instead of flattening everything into OpenAI compatibility. |
| Pi | [`6c4f360`](https://github.com/earendil-works/pi/tree/6c4f360264397c59801f6da2bdac13e3b1fcbe91) models Ollama as an OpenAI-compatible custom provider and documents compatibility flags; its agent configuration can use a dummy API key and some opt-in tests auto-pull a model. | Compatibility flags are evidence that the wire is not semantically uniform. Reject fake credentials and surprise downloads. |
| OpenCode | [`3a31c4e`](https://github.com/anomalyco/opencode/tree/3a31c4ea801915c0b050df4b3842997ea62b6e93) configures local Ollama through an OpenAI-compatible provider and warns that tool use may need a 16K–32K context. | Useful demo path, but a configured model/context claim is not measured capacity or capability evidence. |
| Hermes Agent | [`c80a0a5`](https://github.com/NousResearch/hermes-agent/tree/c80a0a551c7038517456ee0aeb60203ec92aedb6) contains Ollama-specific context, thinking, vision, truncation, and finish-reason probes. | Provider-specific drift accumulates quickly behind a generic compatibility label; conformance must make each deviation explicit. |

The local competitor clones live under
`/Volumes/hubdisk/workspace/research/sessionless-competitors`. They are research
inputs only and are not build, runtime, or CI dependencies.

## Sessionless contract changes

The current V1 domain admits only `subscription`, `api_account`,
`router_account`, and a deterministic fixture. It also restricts
`credential_mode=none` and the credentialless provider contract to that
fixture. Ollama must not be squeezed into `api_account`, assigned a dummy API
key, or treated as the deterministic fixture.

Add explicit compile-visible concepts before implementing the adapter:

```text
backend_kind = direct_ollama
provider_contract_kind = local_endpoint
resource.kind = local_endpoint
resource.credential_mode = none        # local loopback only
credential_delivery_kind = none
transport_kind = direct_api
transport_provider = ollama
endpoint_id = worker-local-ollama
```

The `none` combination is valid only when all of these conditions hold:

- the selected execution placement is an owner-scoped attached worker;
- the endpoint root comes from a digest-pinned local launch profile, not a
  request, model response, public DTO, ambient environment, or central database
  URL;
- the worker launches and owns the Ollama daemon with an allowlisted
  environment, `OLLAMA_NO_CLOUD=1`, a dedicated model directory, and an
  isolated config/identity home; it records the executable digest, process
  identity and start time, runtime version, and launch-profile digest;
- the endpoint is a worker-selected loopback address that is never published
  to a beneficiary; an already running user Ollama service is inventory-only
  evidence and is refused for real-model or federated execution;
- resource revision, worker/enrollment/connection generations, model digest,
  model-instance generation, capability/policy evidence, and grant generation
  are sealed before dispatch;
- the worker validates the exact full attempt authority and fence before making
  any request.

`/api/version`, `/api/tags`, `/api/show`, and `/api/ps` do not attest the
daemon's launch environment. Therefore a probe of an ambient loopback service
cannot prove no-cloud mode. The supervisor must derive that property from its
own sealed launch receipt and live process identity. It must reject cloud-model
inventory and prove the no-cloud denial path before accepting any beneficiary
prompt.

An authenticated reverse proxy or remote Ollama deployment is a later managed
endpoint resource with explicit TLS and invocation credentials. It must not be
silently accepted by the local-endpoint profile.

## Native adapter boundary

The adapter should support a deliberately small endpoint set:

| Endpoint | Purpose | Authority rule |
| --- | --- | --- |
| `GET /api/version` | Runtime compatibility evidence | Exact parsed version or unknown; unknown cannot authorize a feature that needs a minimum version. |
| `GET /api/tags` | Installed model inventory and immutable digest | Catalog observation only; no implicit model selection. |
| `POST /api/show` | Model template, parameters, and advertised capabilities | Advisory evidence that must be confirmed by conformance. |
| `GET /api/ps` | Loaded size, processor split, context, and expiry | Capacity/diagnostic observation, never billing truth. |
| `POST /api/chat` | One bounded model turn, streamed or non-streamed | Exact admitted model/digest and context; redirect, proxy, cookies, compression surprises, and oversized frames fail closed. |

Pull, create, copy, delete, push, and arbitrary blob operations are not model
invocation. They belong to an explicit owner-authorized local lifecycle tool.
The invocation adapter never downloads a missing model and never falls back to
another tag.

Ollama's documented chat request accepts a model name/tag, while `/api/tags`
reports its digest separately and the chat response does not attest the served
manifest digest. Consequently, resolving a tag before admission is not an
atomic immutable-model guarantee. PR-04b is blocked from enabling real or
federated execution until it proves one of these strategies against the pinned
Ollama version:

1. invoke an immutable digest-qualified reference and verify the server honors
   it atomically; or
2. use a workflow-owned daemon and model store, serialize all lifecycle changes
   against active attempts under one supervisor-held model-instance generation,
   make the store immutable to the daemon during the invocation cohort, and
   prove with a real-server adversarial retag/replacement test that a changed
   model cannot start or commit under the old generation.

A pre/post `/api/tags` or `/api/ps` comparison alone is detection after an
unattested invocation and is insufficient. If neither strategy is supportable,
the honest outcome is a local diagnostic/demo profile without the exact-digest
or federation claim; the production resource remains disabled.

The outer Sessionless harness owns the agent/tool loop. The Ollama adapter
returns bounded content, thinking observations where policy permits, and typed
tool requests. It never executes a tool. Every follow-up model call remains in
the same admitted attempt and exact model/resource/profile tuple.

Provider evidence should record:

- runtime version and model name plus full digest;
- actual context setting and advertised/observed modalities/tool support;
- load, prompt-evaluation, generation, and total durations;
- prompt and generated token counts with provider-reported provenance;
- finish/cancel/failure classification and response framing revision;
- loaded size, processor split, and expiry as capacity diagnostics;
- endpoint/profile/resource/grant revisions without exposing a raw endpoint to
  beneficiaries.

Local inference has zero provider invoice cost, not zero cost. Meter hardware
time and energy only as separately labelled estimates; never synthesize a
currency value from tokens.

## Federation and security

```text
federation member
  -> canonical run and explicit resource grant
  -> admission pins owner, worker, resource/model/profile/grant revisions
  -> outbound attached-worker lease
  -> worker-local loopback Ollama request
  -> fenced canonical events and terminal evidence
```

The owner controls beneficiaries, allowed models/data classes, concurrency,
queue depth, schedule, owner reserve, and revocation behavior. Removing a grant
or changing its generation blocks new attempts immediately. In-flight revoke
behavior is explicit policy; stale worker or resource generations cannot
commit. The resource owner must be shown that beneficiary prompts and tool
results are processed on their machine.

Required denials include cross-owner resource use, arbitrary endpoint/host,
non-loopback local profile, ambient or replaced daemon process, stale model
digest or model-instance generation, stale grant or worker generation,
unexpected cloud model, absent sealed no-cloud launch evidence, implicit proxy,
redirect, and model fallback. `localhost` is placement-relative and is never a
control-plane endpoint.

## Deterministic and real-model verification

Default unit/integration CI uses a fake native Ollama server with strict request
and NDJSON response fixtures. It covers model digest substitution, malformed or
oversized frames, unsupported tool/vision/thinking claims, cancellation before
and after response acceptance, timeout, 503 overload, truncated stream,
ambiguous completion, and late output after fence/revoke.

Real-model tests require an explicit opt-in flag, an already installed exact
model digest in the dedicated store, a supervisor-launched no-cloud daemon, and
a bounded context/output/time budget. They never invoke `ollama pull`, never
change global configuration, and skip with a diagnostic reason when
prerequisites are absent. They include a cloud-model denial probe before prompt
submission and an adversarial retag/replacement case before the exact-digest
profile can pass. Assertions concern schema, authority, lifecycle, bounds, and
cleanup—not generated wording.

## Model experiment protocol

### Tiny smoke and demo

1. Relocate model blobs to the dedicated hubdisk store and verify the system
   disk does not grow by the model size.
2. Pin the exact Ollama version, model tag, full digest, quantization, context,
   and environment settings in the receipt.
3. Start with `qwen3.5:0.8b`; test health, cold/warm load, one streamed turn, one
   typed tool request, cancellation, unload, and offline replay.
4. If it cannot satisfy the bounded tool schema, compare 2B and 4B and select
   the smallest model that does. The 4B candidate is the initial demo ceiling,
   not an automatic promotion.

### Qwen3.8-27B on M4 Pro / 24 GB

Use only the official 18 GB MLX NVFP4 or Q4_K_M tag, then record the full local
digest. Run one model and one request at a time, begin at 4K context, then 8K,
and increase only while headroom remains. Enable supported Flash Attention and
compare the default f16 KV cache with q8_0; do not begin with q4_0. Bound output
to 512 tokens and unload after each measurement cohort.

Predeclare an abort boundary: stop the cohort on red memory pressure,
meaningful sustained swap growth, host unresponsiveness, repeated OOM/reload,
or failure to retain enough capacity for the worker and OS. Record cold/warm
latency, time to first token, prompt/decode throughput, resident/peak memory,
swap delta, loaded processor split, context, thermal behavior, cancellation,
and unload recovery. A model that merely loads but causes swap storms is a
no-go for the 24 GB profile.

## Dependency-ordered delivery plan

1. [#114](https://gitcode.com/urandon/sessionless/issues/114) extends the
   provider binding and conformance taxonomy for placement-bound, keyless local
   endpoint resources.
2. [#115](https://gitcode.com/urandon/sessionless/issues/115) adds a
   feature-disabled native Ollama adapter with closed fake fixtures and proves
   or rejects an atomic immutable-model strategy.
3. [#116](https://gitcode.com/urandon/sessionless/issues/116) adds an opt-in,
   hubdisk-safe, workflow-owned no-cloud daemon lifecycle and tiny-model smoke
   recipe. Ambient user services remain untouched but cannot execute a real or
   federated attempt.
4. [#117](https://gitcode.com/urandon/sessionless/issues/117) executes the
   M4/24 GB small-model and Qwen3.8 benchmark protocol.
5. [#118](https://gitcode.com/urandon/sessionless/issues/118) binds the resource
   to attached-worker ownership, grants, admission, fencing, capacity, and
   metering.
6. [#119](https://gitcode.com/urandon/sessionless/issues/119) adds
   enrollment/status/diagnostic UX only after the contracts above settle.

Each item is a separate review and rollback boundary. The adapter remains
disabled until the exact resource contract, fake conformance, attached-worker
security, and operator kill switch pass. A real-model failure disables only the
affected model/profile; deterministic fixtures and other provider routes remain
available. No route silently switches to OpenRouter, OpenAI, another Ollama
model, or Ollama Cloud.

## Success criteria

- no model blob is placed on the system disk by a Sessionless workflow;
- default CI is credential-free, model-free, deterministic, and bounded;
- the native adapter proves exact digest/model-instance/context/stream/tool/
  cancel semantics, or remains disabled with the unsupported guarantee named;
- a tiny local model supports the diagnostic demo, or a precise no-go and the
  smallest passing fallback are recorded;
- Qwen3.8-27B receives a measured go/no-go for the 24 GB host rather than an
  inference from artifact size;
- federation grants resource use without disclosing endpoint or credentials;
- zero stale owner, worker, resource, model, model-instance, process, profile,
  grant, or attempt generations can start or commit supported real/federated
  work;
- local cost, capacity, availability, and privacy are visible and never
  presented as free or globally trusted.
