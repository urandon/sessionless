# Constrained tools and MCP for the owned harness

Version: 0.1.1 draft, 2026-10-10. Design owner: [#179](https://gitcode.com/urandon/sessionless/issues/179), composed by [#180](https://gitcode.com/urandon/sessionless/issues/180). Requires independent review and explicit owner acceptance. This is a proposed public-data profile, not an enabled MCP connection or a live service guarantee.

## Useful minimum

Choose the documented OpenAI Docs MCP endpoint `https://developers.openai.com/mcp` and tool `search_openai_docs` as the first candidate. Its task is to locate relevant official public documentation and give the model bounded results/citations for continuation. It is not arbitrary web search, full-page browsing, API execution or private-session retrieval. [Accepted #179 research v0.2](https://gitcode.com/urandon/sessionless/issues/179#note_192991193) compares Codex/OpenCode/Pi, Claude tool search and service/placement alternatives; it records the official contract versus illustrative schema distinction.

The endpoint/tool are documented by [Docs MCP](https://developers.openai.com/learn/docs-mcp). The actual initialized protocol version, server descriptor, tool input/output schemas and result variants must be captured under separately authorized public-data experiments before exact-profile enablement. Example JSON in documentation is not an observed live catalog and cannot freeze a production schema. This draft does not assume the example's fields, free pricing, availability, retention or SLA.

Keep the model-visible built-in surface empty beyond this admitted MCP tool for the first profile. Immutable context/input materialization and output finalization remain trusted runtime operations. The model needs no shell, package installation or arbitrary filesystem access to answer a public documentation question with citations. A future artifact-ID/range reader requires a distinct bounded policy; do not add it implicitly to make a fixture pass.

## Placement and network authority

Run a trusted MCP client in the isolated worker, outside model control, through the **same attested proxy boundary** as provider egress. There is no direct worker Internet path or alternate proxy. Reuse the [outer isolation](../serverless-isolation.md), [egress authority](../serverless-egress.md) and [owned loop operations](owned-agent-harness-minimum.md#durable-subordinate-operations).

The current provider PolicyV1 is fixed POST-only and one effect; it does not support MCP by configuration. A versioned sealed proxy policy must explicitly distinguish provider and MCP destinations/operations and bind the descriptor/grant. MCP routing admits only the fixed HTTPS origin, port 443 and canonical `/mcp` path, with explicitly accepted Streamable HTTP methods and headers. Deny redirects, ambiguous paths, ambient proxies, metadata/private/loopback/link-local addresses and DNS rebinding; pin connections to validated public addresses and verify certificates/SNI. Attest proxy artifact, workload identity, policy enforcement and expiry before effects. Configured allowlists alone are not proof of substrate enforcement.

Transport GET for an admitted stream and DELETE for owned session teardown may be enabled only in that exact MCP policy; they do not open generic browsing. Response citation links are data, not fetch destinations. A local/client SDK cannot bypass the proxy on reconnect, discovery, auth or cancellation. #90/#92 must prove this extension on the exact substrate before activation. If it cannot enforce the policy, stop and obtain an explicit substrate decision, not a runtime fallback.

## Frozen descriptor and grant

Proposed server-owned `MCPToolGrantV1` seals tenant/owner, admitted Run/Attempt scope, an independent tool/data-policy digest, existing resource revision and profile identifiers, data class, consent revision, fixed endpoint/transport/protocol version, tool name and full normalized descriptor digest, supported schema dialect/validator version, input/result limits, expiry and revoke generation. The independent policy/identifiers must not themselves include the enclosing agent-policy/binding. The grant excludes AgentLoopPolicyV1, HarnessBindingV2 and invocation-authority digests. Construction order is independent policy/identifiers → scoped grant → agent policy → binding → invocation authority; later operation capabilities bind all completed digests. Its grant digest belongs to AgentLoopPolicyV1 and the canonical binding, not mutable ExecutionRequest.AllowedMCPServers strings. That existing string list is neither a schema nor sufficient authority.

A trusted registration owns normalized model-visible names and routing. Descriptor prose/annotations never grant permission. Use only the exact captured tool schema; reject unknown, changed or unsupported descriptors before a call. Accept a bounded single catalog page only; pagination, list_changed or drift invalidates the profile rather than expanding its catalog. Handshake negotiates an explicitly admitted protocol version; no nearest-version or legacy HTTP+SSE fallback.

Validate the complete JSON argument value, including nested required fields, types, enums, additional properties and bounded sizes. The schema validator must support the actual admitted dialect/keywords. Reject unresolved external references, cyclic/oversized schemas or ignored constraints. Arguments are complete only after the model response is validated and durably recorded; streamed fragments never dispatch. Name, server, schema and grant all exact-match the scoped call. One response may request only one tool in the first profile.

## Public data and consent

The first live profile admits only reviewed public question/query material. Public classification is an admission decision with provenance and consent, not a regex guess made by the model. The initial useful demonstration is a public documentation task; unrestricted private conversations remain disabled for this MCP profile. Do not forward canonical transcripts, attachments, internal URLs, user identifiers, secret names or provider credentials to form a query. Consent UX exposes the exact tool arguments and data boundary before dispatch.

Anonymous MCP means no MCP credential, cookie, ambient OAuth token or provider key is attached. A 401/auth challenge fails unavailable; do not discover/login automatically. Private MCP requires a separate audience/issuer/custody/consent design. Current owner/resource/grant revocation is rechecked before send and publication; a public endpoint cannot authorize cross-owner data.

Treat tool text, search snippets, descriptor descriptions and links as untrusted data. They cannot change instructions, request callbacks, add tools or alter limits. Isolation and authorization must deny those effects even if the model follows hostile content. Separately test answer/citation contamination: labeling content untrusted is not a proof that the model cannot be influenced.

## Protocol and normalized results

Implement the admitted Streamable HTTP contract with both JSON and bounded SSE responses, exact request IDs and session ownership. The [MCP 2025-11-25 specification](https://modelcontextprotocol.io/specification/2025-11-25) is the research baseline, not permission to negotiate arbitrary future versions. Record the actual version in profile evidence.

Bound setup, catalog, notifications, result frames, tool calls and teardown under the [minimum ceilings](owned-agent-harness-minimum.md#initial-finite-ceilings): 12 outbound messages total, one catalog page, 128 inbound frames, 256 KiB aggregate inbound, 4 KiB arguments and 64 KiB per tool result. Progress cannot reset the hard deadline. Disable automatic SDK retries/reconnect dispatch. Unexpected request IDs, duplicate terminal messages, server-originated requests or unsupported content types fail closed.

Normalize only the captured, accepted result variants. Validate structured output against the frozen output schema when present; text is bounded untrusted content, not proof of arbitrary structured fields. Reject unsupported image/audio/resource/link variants unless the exact profile explicitly admits them. A required oversized structured value returns overflow, not invalid truncated JSON. Safe citation URLs use the approved official documentation origin/path policy; unsafe links are not surfaced as actionable citations and never fetched automatically.

Keep `protocol_error`, tool `isError`/domain error, authority denial, initialization/catalog unavailability, malformed/oversized result, cancellation and `accepted_outcome_unknown` separate. A known safe domain error may be included in model continuation, but the first profile forbids corrected/repeated physical dispatch of that failed operation. The model may conclude that the tool is unavailable. A genuinely different bounded query is separately admitted under the remaining budget; equality uses canonical normalized name/arguments plus original operation identity, not only provider-generated call ID.

## Effects and cancellation

Reserve each physical tools/call in the canonical subordinate operation store before send. Persist its validated result/observation before model continuation. Even a read-only search can disclose query data and incur cost, so it is an effect with no automatic retry. A regenerated model call ID cannot bypass the one-way reservation or duplicate-call check.

Setup/catalog transport has its own counted message/byte/time budget and owned session evidence; it is not a hidden model tool call. Any request capable of dispatching a tool must pass the operation gate. Session reconnect/resumption is not physical-call retry; the first profile disables automatic resumption rather than relying on an unproven SDK behavior.

Cancel local work under the shorter admitted deadline, send protocol cancellation only where permitted, and close/join owned connections under the independent cleanup budget. Never cancel initialization using a tool-request cancellation shape. Cancellation does not imply remote rollback or a refund. A late/lost reply after dispatch remains unknown and cannot publish a success or cause another tools/call. Cancel-before-send with verified zero dispatch is distinct. Final remote/session cleanup and local stop are independently observed, not inferred from an ACK.

Authenticated durable operation observations remain available when the backend errors; terminal ToolEvents alone are insufficient. Unknown tool outcome halts the Attempt and prevents compaction/replay even after restart. Use the exact failed-outcome projection described in the minimum contract, not a fabricated service response.

## Alternatives and admission gates

| Alternative | Disposition |
| --- | --- |
| Worker client through the existing attested proxy | Propose; preserves isolation and canonical operations without a second execution authority |
| New central MCP gateway | Defer unless exact-substrate evidence requires it; would add session/credential/error custody obligations |
| Microsoft Learn MCP | Alternative public-doc candidate; not automatic fallback or frozen schema |
| Own documentation corpus | Defer; useful but not an implemented service and adds corpus/index ownership |
| Generic web search/page retrieval | Separate #94 research; no hidden release gate for this bounded locator task |
| Private OAuth, generic discovery, resources/prompts/sampling/elicitation/tasks | Exclude from first profile |

Before enablement, capture the actual handshake/catalog/result and evidence of useful public queries, then freeze the exact artifact/descriptor/validator/profile. Review terms, data handling, availability and possible charges; unknown cost is not zero. Provider model/tool calling support, monetary admission and exact proxy enforcement are separate gates. A documented endpoint is not enough to enable it.

## Verification and useful live proof

Credential-free tests use scripted model responses, fake HTTP/SSE transport and fake time/barriers. Prove real observed dispatch counts, not merely echoed transcripts.

| Fixture | Required proof |
| --- | --- |
| Successful loop | Model emits structured search call, fake transport observes one dispatch, bounded result is durable, second model answer and canonical pair commit once |
| Forged/drifted call | Wrong server/name/schema, nested/extra/oversized args, paginated/drifted catalog denied before tool network activity |
| JSON/SSE faults | Mismatched/duplicate IDs, unsupported dialect/content, domain vs protocol error, unsafe citation and overflow distinctions |
| Revocation/data-flow | Cross-owner/stale grant/private query denied; no provider credential or transcript in captured MCP requests |
| Egress | Redirect/private address, DNS rebinding, certificate/proxy/identity drift; no direct reconnect path |
| Cancel/restart | Before-send zero dispatch vs after-send unknown, late reply ignored, fresh delivery reconcile-only, no automatic retry |
| Cost and resource bounds | Setup/cancel/teardown included, endless progress stops, aggregate messages/bytes/model/tool budgets cannot reset |
| Result injection | No capability expansion, callback, filesystem or credential access; truthful answer/citation failure classification |

Separately authorized live proof requires a real model-generated public query, actual tools/call/result, model continuation grounded in a useful official documentation result, durable canonical answer visible in WebUI, exact profile/schema/artifact and usage/cost/cleanup receipts. Require relevant official citations; do not present a locator-only service as full document reading or general search. Deterministic tests do not prove endpoint availability, actual schemas or model quality. #179 stays open until its design/fake/live gates are satisfied; this draft alone completes none of them.
