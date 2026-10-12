# Pi SDK attached media profile

Version: 0.1.1, 2026-10-12.
Owner: [#202](https://gitcode.com/urandon/sessionless/issues/202), under
[#72](https://gitcode.com/urandon/sessionless/issues/72).
Status: engineering contract accepted under agreed-MVP delegation following
independent requirements-led review. Publication and implementation remain
separate; no eligible runtime profile or activation is created by this document.
Version 0.1.1 records acceptance and the registered handoff; the v0.1.0 contract
is unchanged.

Use Pi's existing SDK agent loop and standard Responses provider with typed
original images, a fixed trusted bootstrap and bounded transport. This is an
explicit alternative to the Codex media profile, not a replacement model client
disguised as Pi. Preserve the complete attached text/file/image product promise
and the separate managed loop, compaction, MCP and web-search track.

This profile amends only the native representation and integration choice in
[attached media design 0.1.1](attached-harness-media-and-artifacts.md).
`CODEX-EXEC-MEDIA-2026-10-V1` and the closed text profile remain unchanged.
Proposed separate identifier: `PI-SDK-MEDIA-2026-10-V1`. Selection is explicit;
no attachment, model failure or missing credential selects a fallback.

## Research and alternative decision

Source pins are Pi `6c4f360264397c59801f6da2bdac13e3b1fcbe91` and Codex
`f5420174dafba153913a3e697f89002c338dfd7e`, not claims about the latest releases.
The independent original-scope review and source manifests are retained in
the [#199 native-lane receipt](https://gitcode.com/urandon/sessionless/issues/199#note_193248748).
The [#202 acceptance receipt](https://gitcode.com/urandon/sessionless/issues/202)
identifies the exact independent review and primary engineering acceptance.
Exact executable/package provenance remains a release gate.

| Candidate | Source finding | Disposition |
| --- | --- | --- |
| Stock Codex exec | Decoder allocations, image derivation, request/output caps, retries, WebSocket and tool reachability do not satisfy the complete media envelope. | Keep disabled. A reproducible in-artifact derivative is an alternative when preserving the selected Codex resource is necessary. |
| Stock Pi CLI or RPC with an ordinary extension | CLI image ingestion can transform images; extension errors can be caught while the provider request continues. | Not the selected enforcement path. No extension-returned conformance boolean grants permission. |
| Pi SDK typed user images and existing standard Responses provider | The inspected user-image route preserves image blocks through message conversion and constructs data URLs without Photon. Direct provider onPayload executes before responses.create. | Preferred engineering candidate, conditional on exact dependency, transport, context, memory and platform verification. |
| Pi Codex subscription provider | HTTP hooks do not cover the separate WebSocket send path; equivalent output and retry bounds are not established. | Not interchangeable with the standard Responses lane. Retain as a separately reviewable alternative. |
| New Sessionless client replacing Pi's loop and parser | Would remove the ready-made harness rather than integrate it. | Reject for this attached outcome. |

The typed route is supported by pinned Pi source:
[AgentSession.prompt](https://github.com/earendil-works/pi/blob/6c4f360264397c59801f6da2bdac13e3b1fcbe91/packages/coding-agent/src/core/agent-session.ts),
[message conversion](https://github.com/earendil-works/pi/blob/6c4f360264397c59801f6da2bdac13e3b1fcbe91/packages/coding-agent/src/core/messages.ts),
[SDK wiring](https://github.com/earendil-works/pi/blob/6c4f360264397c59801f6da2bdac13e3b1fcbe91/packages/coding-agent/src/core/sdk.ts),
[provider message conversion](https://github.com/earendil-works/pi/blob/6c4f360264397c59801f6da2bdac13e3b1fcbe91/packages/ai/src/api/transform-messages.ts) and
[Responses conversion](https://github.com/earendil-works/pi/blob/6c4f360264397c59801f6da2bdac13e3b1fcbe91/packages/ai/src/api/openai-responses-shared.ts).
Photon remains reachable through other entrypoints. Disabling CLI ingestion,
read tools, extensions and tool-result history is part of the contract, not an
assumption that Pi never processes images.

## Profile and resource authority

Pin the Pi source and build/package digests, complete dependency lock, exact
Node runtime, trusted bootstrap and transport hashes, model and vision
metadata, request/final schemas, image detail, finite context/output/allocation
limits, credential resource, data policy, price revision and platform policy
in one immutable capability/profile binding. Unknown values deny registration;
no runtime catalog lookup supplies missing authority.

Pi's lock pins OpenAI SDK 6.40.0. Its npm integrity is
`sha512-MWtTjd/gQt4jpbji61NTgFWJLoY/PdRJ6wG9/ZDRMYNMlBKrCrSlkLI+KgHP1vR1qT6LKSAyAqIxno6lcK9JiA==`.
This identifies a dependency, not its verified transport behavior. Resolve and
audit that exact package before implementing the transport seam; current SDK
documentation cannot substitute for its source or executable tests.

The selected standard Responses API resource must already be explicitly
eligible for this owner, model, data class and cost policy. Personal Codex
subscription credentials cannot be borrowed for it. This design authorizes
neither a new commercial resource nor live calls. If no eligible resource
exists, keep this candidate disabled and retain the Codex alternative rather
than silently switching products or endpoints.

## Original inputs and sole lifecycle

Reuse authenticated canonical media validation from #199. Preserve every
admitted historical, snapshot and tail attachment with event/sequence,
ordinal, name, MIME and full immutable BlobRef. Verify complete bytes, size,
hash, supported signatures and dimensions before credential Issue. Repeated
filenames remain different source inputs. UTF-8 and JSON files preserve exact
original contents and numeric tokens; names/content are quoted task data.

Go performs bounded PNG/JPEG validation but no extraction, OCR or resize.
Pass the verified original bytes as typed SDK user image blocks in canonical
order. Keep source labels adjacent to their model-visible image positions and
check that association again in the final provider request. Pin blockImages to
false and require a vision model before conversion; Pi's nonvision image
replacement is a denial case, not a supported downgrade.

Native derivation is `NoTransformOriginal`: no local Photon decoder, resize,
re-encoding, image cache or implicit orientation conversion. Data URL base64
must decode to the exact original bytes and declared PNG/JPEG MIME. Pin
`detail:auto`, the inspected standard-provider representation, for this
separate profile. It does not satisfy Codex DetailBased/High geometry; actual
model context and visual usefulness must be calibrated for this exact detail.

Retain both original image cohorts: the 3000 by 1000 PNG and small PNG/JPEG,
including orientation/color-sensitive tasks, historical images and identical
names. No one-image, path-only, OCR or newest-trigger-only substitute closes
the requirement. Requests exceeding an inherited bound fail honestly without
dropping history or increasing that bound.

The same authenticated attached Session, accepted head, Run/Attempt,
lease/fence and original prepared runner own Issue, Materialize, supervised
process, WriteBack, Release and receipt reconciliation. A private Pi prepared
driver consumes that authority; it does not attach again, mint a new claim,
resume a native session, create a second runner or extend credential lifetime.
An SDK wrapper is inside this process boundary, not a network sidecar.

## Trusted bootstrap and effects

Create a fresh in-memory Pi SDK session with only the admitted canonical
projection. Preserve Pi's loop, standard Responses construction and SSE parser.
No CLI/RPC command server, HOME discovery, persisted session restore/fork,
settings file, skill/template/extension discovery, custom tool registration or
dynamic provider/catalog/auth plugin is exposed.

Use isolated settings and credential/catalog resources. ModelRuntime can resolve
auth and change baseUrl/headers; therefore an initially fixed model is not
enough. Disable model-network discovery and refresh-on-create, prohibit OAuth
refresh or ambient environment credentials, and validate final endpoint and
auth headers at the actual send boundary. Keep credential bytes out of SDK
events, stdin, model inputs, structured finals and public evidence.

Pin all three retry layers off: SDK request retries, provider retry wrapper and
AgentSession auto-retry. Disable native compaction, steering/follow-up queues
and fallback transports. A private invocation-wide one-shot send permit is
consumed before the physical request and never reset after error, timeout,
lost response or reentry. A second requested model send always denies.

The actual local tool registry is empty. Request tools and provider-hosted tools
are absent, tool choice is none, and effect-bearing response frames reject
before reaching Pi's tool/follow-up flow. Missing-tool errors do not authorize
another turn. Retain OS read-only/no-shell confinement and restricted provider
egress regardless of the empty registry. Owner-host assertions are not
cryptographic attestation against a malicious owner-controlled machine.

## Request response and allocation bounds

Before Issue, verify a conservative fit certificate for the exact complete
context and output reserve. Include formatting, text/file escaping, image
detail and schema overhead; unknown model/image bounds deny. Byte length or a
lower-bound token estimator is not a fit certificate. OpenAI's
[token-counting documentation](https://developers.openai.com/api/docs/guides/token-counting)
distinguishes image/file and formatting costs from plain-text estimates and
counts non-visible output tokens within output limits. This profile adds no
per-turn remote token-count request; calibration needs separate finite authority.

Reserve predictable base64, data URL, JS string, metadata, body and SDK copy
allocations before provider construction. JSON escaping and JS string storage
must be source-accounted, not estimated from compressed-image size. Direct
onPayload rejection alone is too late for already constructed data URLs.
Prohibit samplingParams and mutable request overrides; validate the complete
closed request shape with fixed model, store false, no previous-response or
conversation identifier, truncation disabled and a real finite max_output_tokens
supplied by the trusted stream wrapper. Pin the strict structured-final schema.

At the sole physical fetch boundary, validate exact final method, endpoint,
headers and serialized body after SDK auth/header construction. Use redirect
error and no automatic resend. Enforce response headers, total body/SSE bytes,
line size and event count before the existing SDK parser accumulates them;
cancel/abort the same stream on overflow or forbidden effects. Token limits
alone do not bound error, metadata or streaming bytes.

| Resource | Inherited ceiling or required finite binding |
| --- | --- |
| Text/JSON originals | Two, 128 KiB each and 256 KiB total; exact contents. |
| PNG/JPEG originals | Two, 1 MiB compressed each and 2 MiB total; 4 million pixels each; Go validation at most 32 MiB aggregate decoded RGBA, with scratch/copies separately accounted. |
| Native image representation | Exact originals; base64 at most 4 times ceil(bytes / 3), plus data URL, source labels and actual JSON framing. No native decoded/resampled buffer allowance is consumed. |
| Canonical/transport input | Minimum of original MaxInputBytes/MaxContextBytes, event/count bounds and actual sealed-input wire cap. Preserve the existing 1 MiB prepared transport envelope. |
| Native request and process | Positive immutable full serialized request, pre-send allocation, response/SSE and process memory limits, derived and verified for the exact dependency/runtime/platform before registration; never defaults or unbounded values. |
| Final semantics | 128 KiB structured JSON; answer at most 32 KiB UTF-8 and existing 32000 runes; two text/JSON artifacts at most 32 KiB each and 64 KiB aggregate. |
| Process protocol | Minimum of 1 MiB/frame, 16 MiB stdout, 4096 events, depth32 and 256 KiB raw-final bounds and narrower profile limits. |
| Time and sends | One physical model send; all preparation/stream/cleanup stays inside the original earlier caller/run/lease deadlines and credential lifetime. |
| Upload/export | Reuse #200's two body sends, 64 KiB aggregate and 15 seconds or earlier remaining authority, including reconciliation. |

Native request/response and process values cannot be selected solely to make a
test pass. The implementation must publish an exact source-derived allocation
ledger and measured maximum legitimate cohort within its actual platform
envelope. If stock SDK serialization cannot meet that ledger, add only a small
reviewed native transport/bounds seam. Do not replace the provider/parser/loop,
raise inherited ceilings or manufacture eligibility from a hook callback.

## Final results and reconciliation

Reuse #200's strict answer/artifact schema and private completed-process
producer carrier; only a genuine same-run supervised final may populate it.
Sessionless serializes the exact provider-authored text/JSON contents, not a
directory harvest or synthesized summary file. Preserve final-byte, answer,
artifact, schema/profile and process-observation digests separately.

Credential WriteBack/Release and owned staging cleanup precede bounded upload.
Keep output upload capabilities outside the child. Existing attempt-output
custody, exact immutable object reconciliation, READY/canonical finalization
and TerminalAck journal ownership remain unchanged. Cancel/revoke/expired or
ambiguous effects cannot produce a new model send or artifact regeneration.
Current participant checks and durable downloads/refresh/re-login remain #201.

## Verification and implementation handoff

Design acceptance is not runtime conformance. The implementation must prove:

1. Exact SDK/runtime source and package hashes; startup reachability; auth,
   serialization, retry, redirect, parser and actual send-boundary behavior.
2. Genuine prepared Session/head/lifecycle with zero preflight Issue on corrupt,
   missing, stale, foreign, unsupported or unknown-fit inputs. Same canonical
   historical/snapshot/tail order and exact original bytes reach the request.
3. Maximum legitimate text/file/two-image context, escaping/copy/memory/wire
   accounting and overflow negatives before allocation/send, using the declared
   platform limits. Both original image cohorts remain in the acceptance matrix.
4. Causal denial mutants for all retry/effect/endpoint/auth/override/second-send
   paths, response overflow, cancellation and ambiguous transport; no model or
   tool replay after accepted effects or receipt loss.
5. Structured output/producer/upload/READY/ACK/download proof through the normal
   two-owner path, followed separately by real content-dependent visual tasks,
   actual-platform confinement and bounded browser/profile activation evidence.

Use repository testing guidance, fake transports and deterministic barriers for
implementation, with race/shuffle/stress, independent exact-source review and
all exact-head CI. Scripted evidence does not prove a useful real model, private
data eligibility or cloud containment. Return bounded NO-GO if the exact
dependency/model/platform cannot enforce the profile.

Reuse #72, not another epic. The registered implementation handoff is:

- [ ] [#203 — bounded default-off Pi SDK prepared media adapter](https://gitcode.com/urandon/sessionless/issues/203): bootstrap, exact dependency/transport envelope and same-lifecycle composition after the integrated #199 canonical foundation.
- [ ] [#199 — canonical input and complete native preparation acceptance](https://gitcode.com/urandon/sessionless/issues/199): this design does not deliver its remaining native proof.
- [ ] [#200 — authentic outputs and upload custody](https://gitcode.com/urandon/sessionless/issues/200).
- [ ] [#201 — joined receipts and durable authorized downloads](https://gitcode.com/urandon/sessionless/issues/201).

All implementation rows remain open. Reuse the existing activation owner with
an explicit selected Pi resource mapping to #133/#129/#35 where suitable; do
not automatically create a redundant Pi activation task. Codex #133 remains
open until delivered or explicitly dispositioned within the existing
one-harness product scope. No profile or adapter is silently enabled.

The independent managed #176/#189–197, #175/#90/#92 and two-route #35/#13/#14
checklists do not change. No provider calls, credentials, deployment, shared
migration, destructive cleanup, force-mirror override or network/Shadowrocket
mutation is authorized by this design.
