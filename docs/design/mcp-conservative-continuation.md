# Original MCP continuation under conservative reservation

Version: 0.1.0, accepted engineering clarification for #190/#191/#193 under [#176](https://gitcode.com/urandon/sessionless/issues/176).
Base: reviewed !278 source `e49ba020e2b9d7535628c8ca35e64e379f904857`,
merged as `f061ef611f53f61078ea06a99d2c2c85051c50a5`.
Independent original-goal review resolved the historical projection and physical
publication findings, then reported no unresolved P1/P2 on draft SHA256
`83334ef6915f7af2c5aea4246d8bb401ddfcb47652dbfe600eeb532fc30913e4`.
Primary engineering acceptance adopts that unchanged contract; this header
records acceptance metadata only. Implementation evidence remains separate.
No live authority follows. The exact implementation PR must receive its own
independent stabilized-artifact review and all required CI.

## Required outcome

The existing original entry must complete MODEL -> MCP -> durable result ->
MODEL -> canonical tool call/result and assistant answer, within one synchronous
Issue/Materialize/Release and the same current Store, source and finite budgets.
An offline call counter without an eligible canonical finalizer is insufficient.
The first-model and compaction components are not the complete managed MVP.

The minimum v0.2.5 deliberately requires measured money for tool/search
predecessors. !278 therefore retains the actual MCP result with unknown money
and stops. The new route needs an explicit engineering clarification rather
than deleting that guard, relabelling a tariff as a bill or adding a callback
that supplies a claimed amount. The existing search and generic fixture gates
are not changed by this proposal.

## Evidence and alternatives

The pinned [MCP tools contract](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)
defines correlated tool results, not an authenticated billing receipt. The
[transport contract](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)
does not turn an HTTP reply into charge, cancellation or rollback evidence.
These are protocol facts, not a current service price or compatibility claim.
No live MCP experiment or new financial assumption is needed for this design.

Existing original Source custody validates a scoped grant, independent retained
price and terms reports plus their source-document bodies. Those reports
describe the entire admitted service envelope; they are not a measured tariff
or bill. Existing Store reservation debits the complete MCP component ceiling
for every tools/call, as well as aggregate call/byte/time limits, without refunds.
Unknown actual money cannot reduce that debit or permit another charged call.

Adopt conservative continuation with these existing authorities. Reject:
invented observed zero, a new billing callback, unbounded credit, a replacement
scheduler/accounting store, a generic measured-gate bypass, or manufacturing a
free MCP service from the local MODEL provider's invoice. A real cost owner or
new financial envelope remains a separate owner decision when the original
retained evidence cannot conservatively bound the complete admitted operation.

## Continuation authority

1. The original entry owns the prepared MCP holder and original observation
   issuer. A completed nonerror result is durably stored, decoded and correlated
   with the preceding MODEL request/response, exact call/name/arguments/grant,
   full operation scope, reservation and authenticated observation.
2. Usage remains `unknown` when no measured amount exists. Neither installed
   known-free terms, an upper bound nor a successful HTTP reply sets an observed
   amount. Every valid measured/reported overrun remains recordable and stops.
3. A private conservative eligibility witness is derived from the SAME live
   Source and Store. Authenticate actual retained grant/price/terms/documents,
   payer/resource/profile/generation/currency/revision/expiry, exact original
   spending component and its complete pre-send debit. Digest-only DATA, a
   caller-supplied witness/amount/boolean or an MCP annotation cannot create it.
4. Capture/preparation, transactional reserve and before-send rejoin current
   membership, canonical Run/Attempt/lease/fence, cancellation, source/profile,
   allocation/proxy and funding. Keep the original statement/body/time ceilings;
   no fresh budget or retryable I/O inside SQL callbacks.
5. All prior debits remain charged. MODEL2 reserves its own worst-case MODEL
   debit; the MCP component is never refunded, repriced or reused. A second
   positive-priced tool cannot exceed the unchanged already exhausted component.
   Zero-priced tools still consume the sealed aggregate dispatch/message/byte
   and time limits. Each setup still reserves one catalog page against the
   existing aggregate single-page ceiling; do not widen it to fit another call.
   No price state or payer changes to make a scenario fit.
6. Unknown accepted outcome, missing/undurable body or observation, malformed
   result, service/protocol error, absent current evidence, revoked authority or
   failed cleanup stops without MODEL2 or physical retry. Existing service-error
   continuation is not added implicitly. Fresh deliveries remain reconcile-only.

The common generic tool/search measured-money predicate stays strict. Original
MCP continuation must supply an equally strong, explicit authenticated route at
every join, not infer that route from `kind=mcp_tool` or `usage=unknown` alone.

## Context, finalization and retention are one delivery

The ordinary context composer must receive the exact normalized MODEL call and
its correlated complete tool result as one atomic unit. Validate installed tool
schemas on MODEL/COMPACTION requests; an MCP JSON-RPC request has a different
grammar and must not be mistaken for a model request with a missing catalog.
Preserve original request schema bytes, call-ID and semantic-repeat rules,
derived compaction lineage, recent suffix and final token-fit calibration.

Before tools/call and MODEL2 effects, prove terminal representability under the
original capsule/body/event/artifact limits. Count the whole call/result pair,
following MODEL answer, normalized bodies, accounting and escaping overhead.
The actual writer uses the same encoder and limits as this feasibility proof.
Do not send MODEL2 knowing that an assistant-only finalizer will reject it.

Use the existing canonical output/Session finalization transaction to append
the exact tool-call event, tool-result event and final assistant event once.
Every projection binds the original operation/body digest, call and grant;
tool content is untrusted DATA, never fresh effects or hidden reasoning.
The existing terminal owner, output custody, idempotency marker and cleanup
join remain authoritative. No new event store, transcript or SQL table.

The operation-body keeper's physical assistant-only uploader must become an
explicit versioned, bounded per-event publication closure for the sealed plan.
It owns each exact ordered event ID, kind, payload size/hash and destination,
with one-use writes and exact readback before correspondence publication. It
does not expose an arbitrary caller-selected blob/event writer. Lost ACK or
partial publication preserves owned residue and never repeats a possible write.

Version the new retained terminal capsule and conservative accounting/residual
representation explicitly: terminal capsule V2 and accounting/residual V3.
Existing V1 capsules and V2 accounting/residual
semantics keep their strict validation. The new variant retains each exact
reservation, authenticated observation and full reserved amount with an
explicit unknown-reserved MCP disposition; it does not assert measured billing,
refund eligibility, remote rollback or settled debt. Namespace erasure requires
the original output keeper's actual bounded cleanup evidence and retention of
the sealed canonical accounting facts, not a service tariff substituted for a
disposal receipt. No new indefinite billing/recovery retention gate is added.

Historical read/refresh/restore and deletion must recognize the exact new writer
version, verify all events and markers, normalized typed operations, complete
pair order, manifest, accounting and cleanup references. Never reinterpret an
old assistant-only capsule as the new format or relax its original decoder.
Map each typed operation to its exact canonical event IDs and payload digests.
Historical context currently reconstructs retained tool pairs at the assistant
anchor; the new format must not also emit those same canonical pair records.
Verify complete canonical-event/operation correspondence, emit each pair exactly
once, and reject duplicates, reordered/missing events or inconsistent mappings.
Preserve full retained evidence and source/compaction lineage even where an
already projected frame is omitted from the served context.
No migration or application-data rewrite is required: reuse immutable capsule
payloads and the existing Session/Run/Attempt/operation rows. Any newly discovered
schema requirement needs entity-registry justification and separate rollout.

## Implementation order and acceptance

Keep this scope in existing #190/#191/#193; do not close their full outcomes
with a helper-only increment. Implement in dependency order:

1. Versioned truthful accounting/retained-capsule contracts and pure validators.
2. Original Source/Store conservative MCP eligibility and typed context joins.
3. Matched prospective capacity and canonical pair/answer writer, cleanup,
   historical reader/restore/retirement integration.
4. Same finite runtime engine continuation, after all preceding gates exist.
   Credentialed and noauth entry points share this engine, with one credential
   lifecycle only in the credentialed arm and zero credential calls in noauth.
5. Joined offline proof, independent exact-head review, all required CI and
   primary integration audit. Normal serving profiles remain disabled.

Required evidence includes zero- and positive-finite-price controls with actual
money still unknown; unchanged aggregate debits; one MODEL1/tools-call/MODEL2
and credential lifecycle; exact MODEL2 tool input and canonical pair/answer;
finalization replay and fresh retained-history restore after source retirement;
next-turn refresh and compaction with each historical canonical pair once;
pre-send terminal overflow denial; evidence/scope/payer/revision/expiry mutation
denials; unknown outcome/persistence/lost reply/duplicate/revocation no-replay;
known overrun stops; positive-price second-tool denial; malformed/tampered
capsule/accounting/cleanup negatives. Preserve generic tool/search measured
tests unchanged. Follow race/count/shuffle, isolation, fake-time/barrier,
bounded cleanup and exact-CI rules in [testing practices](../testing-best-practices.md).

Offline evidence cannot establish real endpoint/schema/terms qualification,
actual charge settlement, useful model quality, WebUI/cloud E2E, substrate or
the independently required attached route. #175/#90/#92/#35/#13/#14 retain those
existing gates. No credential, model/MCP/search call, deploy, migration apply,
new notification or data export is authorized by coding or design acceptance.
