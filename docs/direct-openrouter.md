# Native direct OpenRouter reference backend

Issue: [#103](https://gitcode.com/urandon/sessionless/issues/103)

Status: feature-disabled V1 reference contract; no real credential, provider
call, production registration, or cloud resource is enabled.

## Decision

The native backend uses one compiled non-streaming OpenRouter Chat Completions
wire skin. It is not a generic OpenAI-compatible client and accepts no runtime
base URL, model alias, provider list, proxy, retry policy, user identifier,
plugin, tool, or session authority.

The immutable request is:

- `POST https://openrouter.ai/api/v1/chat/completions` over certificate-validated
  TLS;
- `Accept: application/json`, `Content-Type: application/json`, and
  `X-OpenRouter-Metadata: enabled`; the sealed HTTP boundary injects the
  `Authorization` value synchronously and the driver never receives key bytes;
- model `stealth/ox-alpha`, one bounded canonical transcript message,
  `stream:false`, and bounded `max_completion_tokens`;
- `provider.allow_fallbacks:false`, `provider.require_parameters:true`, and
  `provider.only:["stealth"]`;
- no redirects, cookies, content encoding, ambient proxy, native retry, or
  second provider effect. The boundary contract additionally requires the
  exact `Authorization`/`Bearer` injection shape and denies ambient credentials.

This shape follows the current official
[Chat Completions API](https://openrouter.ai/docs/api/api-reference/chat/create-a-chat-completion)
and [provider routing controls](https://openrouter.ai/docs/guides/routing/provider-selection).
The metadata opt-in is mandatory because a successful terminal must prove the
requested model, direct strategy, single attempt, and one selected Stealth
endpoint. Admitted route intent and observed route evidence remain separate.

## Boundary ownership

`internal/directopenrouter.Driver` owns canonical transcript compilation,
exact request encoding, strict response decoding, and provider-neutral terminal
evidence. `HTTPBoundaryV1` owns direct invocation credential materialization,
the Authorization header, DNS/TLS transport enforcement, byte ceilings,
cancellation, effect fencing, and cleanup/finalization receipts.

The boundary contract carries an invocation-scoped credential handle and a
`direct` materialization descriptor, never secret bytes. Its printable and JSON
forms redact the request and response bodies and credential authority. A
production HTTP boundary is deliberately absent; wiring it belongs to the
serverless harness composition after the credential and egress authorities are
sealed together.

The feature gate denies preflight and new execution. Cancellation remains
routable through an exact identity after the gate is disabled so an already
started attempt can still be torn down; this matches the other native adapter
contracts and cannot create a new provider send.

## Fail-closed response contract

V1 accepts exactly one bounded non-streaming `chat.completion` with:

- exact response model `stealth/ox-alpha`;
- one choice at index zero, assistant text, and `finish_reason:"stop"`;
- internally consistent provider-reported prompt, completion, and total tokens;
- `openrouter_metadata.attempt:1`, `requested:"stealth/ox-alpha"`,
  `strategy:"direct"`, and exactly one selected `Stealth` endpoint for the
  exact model.

Unknown or duplicate fields, case-folded duplicate keys, invalid UTF-8,
excessive depth/width/bytes, extra choices, tool/refusal/reasoning payloads,
trailing data, truncation, redirects, cookies, and encoded bodies fail closed.
Malformed provider bodies and provider errors never appear in public errors.

Before a completed terminal is committed, the adapter also requires exact
request-byte accounting, exactly one fenced provider effect, a 200 JSON
response, cleanup success, and credential finalization. Cancellation and
post-write transport ambiguity remain explicit non-success lifecycle evidence;
the adapter never retries them.

## Verification

`make provider-conformance` runs credential-free, race-enabled native and
registry checks using local fake boundaries. `make provider-conformance-fuzz`
exercises the response parser. Neither target performs DNS, network, secret
lookup, or provider execution.
