# Original owned-loop failure frontier and canonical projection

Version: 0.1.0, engineering DRAFT for [#191](https://gitcode.com/urandon/sessionless/issues/191)
under [#176](https://gitcode.com/urandon/sessionless/issues/176).
Source baseline: PR !280 `8367b67310616955e6efb91f356fec3fc752f4bb`,
tree `ed9c760e4be97c0af17491c7ac8bfd9535f38374`; that implementation's
integration gates remain separate. This draft is not accepted implementation,
permission to enable a profile, or a claim of real service/cleanup evidence.

## Requirement, evidence and boundary

The [minimum](owned-agent-harness-minimum.md#durable-subordinate-operations)
requires authenticated subordinate observations to survive backend failure and
produce truthful canonical call/result-or-unknown/stop facts exactly once.
The original #191 also requires the same finite engine in the isolated worker,
not a second scheduler. Reuse the accepted research/design; no new service
experiment or product-scope decision is needed for this internal engineering.

The baseline genuinely completes MODEL -> MCP -> durable result -> MODEL ->
canonical pair and answer. Its remaining source gaps are concrete:

- Registry correctly discards untrusted backend ToolEvents on error.
- Manager's generic failure composer permits one SystemNotice only.
- Local failed execution returns before success-terminal capture; that capture
  requires a completed nonerror frontier ending in MODEL.
- The worker post-reservation route still calls RunModelStep, not the finite
  engine. LocalV1.Run cannot be called there because it would start again.

The narrower [terminal custody 0.1.2](owned-loop-terminal-custody.md) describes
success candidates and notice-only late cancellation/failure. This explicit
successor adds an authenticated failure arm; it does NOT reinterpret its
capsule V1/V2, success correspondence, local evidence or local_v2 replay seal.
Existing V1 managed/attached behavior and registry sanitization stay unchanged.

## Original owner and terminal state

Factor the current original entered session's engine behind a closed
post-reservation method. Manager consumes its original same-Store fresh
reservation once with EnterWorkerEffect, then invokes that method. Direct Run
and this route share the same claim, engine, limits, keeper and terminal owner;
they cannot be mixed, copied into two winners, or reserve a second parent.
Credentialed execution still owns one synchronous Issue/Materialize/Release.
The engine remains fixed; no caller callback, endpoint override or tool sender.

On any stop, irreversibly latch effect admission BEFORE teardown. Every
MODEL/MCP/compaction preparation, reservation, proxy and publication route must
observe that same latch. Cancel and join owned active I/O; never detach it or
stop the borrowed inference server. Preserve the original absolute independent
cleanup deadline, including after cancellation/expiry; do not start a fresh
30-second window on error or after credential release. Release stays outside
the one materialization, after borrowed bytes and owned transport are cleared.

A separately typed private failure-capture owner is not PreparedLocalTerminalV1
and cannot become a successful completion owner. Claim it before Close clears
the required original handles. It may read bounded historical facts and publish
the terminal package, but cannot reopen effects, issue credentials, renew
authority or expose a generic signer. The same original mutation/I/O gates,
caps and one-use publication rules apply. Missing physical join or uncertain
custody remains explicit reconciliation, never successful erasure evidence.

## Authenticated frontier, not backend output

The original Store captures the COMPLETE contiguous reserved frontier, at most
12 operations, plus the existing parent/accounting/proxy identities. Authenticate
each exact reservation and persisted observation through the original issuer
and current durable rows. Revalidate complete normalized successful bodies
against their original request/profile/call/grant before use. Blob reads remain
outside retryable SQL callbacks, followed by exact bounded SQL rejoin.

This is a historical terminal-read capability, not active operation admission.
It is bounded by the ORIGINAL cleanup deadline and private entry ownership;
expiry/revocation of send authority cannot erase already recorded facts or
grant another send. Session deletion/finalization fencing, exact tenant/Run/
Attempt/parent and current scheduler lease/fence still control the first commit.
The ordinary grant-expiring observation reader is not silently weakened.

Retain the exact complete observation when durably readable. A lost record ACK
permits bounded exact readback, not a second effect or replacing a different
observation. An authenticated completed result with service_error remains a
known service error; it cannot become success or measured money. Unknown has
no invented response bytes. Valid reported/measured overrun facts remain
recordable, even though they deny continuation.

A reservation with no persisted observation is NOT an authenticated remote
outcome. Preserve its original uncertain-effect residue and permit bounded
exact reconciliation only; do not invent an observation to finish a capsule.
If that causal/authentic complete frontier cannot be established, no partial
pair package is committed and no physical effect is repeated. Do not silently
drop a reserved tail to make an earlier prefix look complete. This contract
adds no new unobserved-operation lifecycle or synthesized cleanup evidence.

The worker obtains this original Store-owned package independently of the
backend's error DTO. Arbitrary ToolEvents, Summary, Outputs, public hashes and
matching call text never become failure authority; Registry keeps dropping them.

## Truthful canonical facts and bounded stop

For each reserved tool, match the exact durable preceding MODEL call, scoped
grant/name/normalized arguments, reservation, observation and normalized result.
Emit one tool_call and one tool_result in original order, followed by exactly
one bounded SystemNotice and NO assistant event/public output manifest.
MODEL/COMPACTION intermediates are retained provenance, not assistant answers.
An unadmitted model request is not a physically reserved tool and emits no
purported tool effect. No tool reservations means a notice-only failure.

Keep the existing canonical tool-event envelope. Its result content is an
explicit closed Sessionless outcome object: completed with the verified result,
service_error with its verified bounded domain error, rejected_before_send
only with authenticated zero-send evidence, or accepted_outcome_unknown with
NO fabricated service response. Missing/conflicting observations are a
reconciliation blocker, never synthesized accepted_outcome_unknown evidence.
The exact closed codec binds the original operation/reservation/observation
identities and cannot accept raw transport errors, secrets or hidden reasoning.

Use a finite sanitized stop-code enum for malformed/partial/multiple/duplicate
model call, unsupported input/finish, authority denial, service/protocol error,
unknown/persistence failure, budget/expiry/cancellation and cleanup uncertainty.
Do not derive a code from arbitrary error text. Unknown tool outcome forbids
same-Attempt continuation/compaction and all physical redelivery retries.
Regenerated IDs, wrapper copies and an explicit later Run cannot replay an
unresolved reserved operation. Search/consent remain #194/#195 contracts.

## One capsule slot, retained metadata and strict versions

Reuse the existing immutable Run terminal-custody Plan and fixed object key.
If no plan has been registered, the original owner may register exactly one
closed sessionless.terminal-failure-custody.v1 capsule in that same slot.
It retains the complete reservation/authenticated-observation/normalized-body
frontier, exact canonical-event mapping, original full accounting and selected
stop. Unknown/rejected bodies have explicit absent-body discriminants, never
guessed zero values. No raw request/header/credential/path or private error.

If a success candidate plan already exists, DO NOT replace it or upload another
capsule. Verify the existing V1/V2 capsule and use its exact authenticated facts
as the failure package's retained source. A successfully uploaded candidate is
not a committed assistant answer. The failure arm projects its tool prefix and
notice only; original success drafts remain owned deletion residues. Missing
or uncertain candidate readback means reconciliation, not another upload.

Keep RunTerminalCustodyV1 and its strict success-only fields unchanged. Add one
optional compact Run.terminal_failure version1 commitment containing exact
scope/parent, plan/capsule format+digest, complete frontier/accounting digest,
sanitized stop, ordered canonical-event digest and explicit cleanup disposition.
It MUST reference that SAME Plan; success Correspondence and LocalEvidence
remain absent. No inline bodies or second capsule/grant/operation ledger.
Generic Run writers may neither create nor alter this commitment and must
preserve it byte-exactly, along with all existing metadata. Clone/strict decode,
complete 32-KiB read/write caps and the entity registry must include it.

Select a separate local_f1: finalization marker and domain-separated
seal binding failed/cancelled status, original parent, full retained metadata,
capsule/accounting, stop, ordered events and actual cleanup disposition. Existing
untagged V1 and local_v2 bytes/validation remain unchanged. The nine-character
prefix preserves the existing 73-byte marker and 74-byte overflow read bound.
Tagged decoder and
bounded marker readers reject missing/unknown/stripped/malformed versions;
hash equality is DATA integrity, not original first-commit authority.

Failure accounting is a distinct versioned retained representation, not a
relaxation of successful accounting/residual V2/V3. Preserve the original setup
baseline, all aggregate/component debits and exact usage provenance, including
unknown/overrun. No refund, repricing, top-up or residual-erasure
receipt follows from error. No unknown charge becomes zero or settled debt.

The same original one-use per-event uploader writes/readbacks exact ordered
payloads before the existing fenced failure transaction. Burn before I/O;
lost ACK/partial PUT never retries. Reserve the worst of successful and failed
capsule/event encodings before effects, using matched counters and actual
writers under ORIGINAL artifact/body/event/SQL/time ceilings. Reuse the same
prepaid capsule slot; no extra artifact, deferred cap increase or truncation.

The dedicated failure arm factors the EXISTING failure transaction and
Run/Attempt/quota/event/finalization updates; it is not a second finalizer.
Generic FailWorkerJob still accepts exactly one SystemNotice. First commit
requires the private original package and exact durable correspondence, not
caller drafts. Commit/replay never installs success evidence or grants effects.
Exact lost-ACK replay uses sealed retained facts, not erased bodies/expired TTL
or a reconstructed private owner. Changed events/capsule/status/metadata conflict.

Fresh history positively classifies failure SystemNotice anchors and tagged
seals, not assistant-only discovery or a V1 fallback. Verify all actual canonical
payloads/mappings and emit each pair once. Unknown facts remain
explicitly unresolved, never usable completed-result continuation or a safe
compaction cut. A new Run must not implicitly dispatch them. Retained historical
DATA is not refreshed send/cleanup authority.

Session deletion inventories that one plan, all canonical payloads and original
failed/unpublished success drafts plus derived residues. Preserve the existing
writer registration/fence/join and uncertainty rules. No original-body erasure
is authorized without complete retained dependency proof and actual independent
physical cleanup; failure commitment alone supplies neither. Do not require a
new reconciliation service, table, TTL, background retry or deployment.

## Implementation and decisive evidence

Stay in existing #191, not a new epic. Order: strict failure codecs/commitment
and guards -> original historical capture/projection and matched fit -> one-use
publication/fenced failure/replay/history/deletion -> worker entered-engine join
-> joined negative/limit fixtures. No writer is eligible before its consumers.

Use the genuine original Source/Store/issuer/keeper and owned MODEL/TLS peers:
durable known MCP then failed MODEL; MCP service error/lost reply and missing
observation reconciliation blocker; partial/multiple/malformed/duplicate calls
with zero
unauthorized sends; persistence and publication faults at every barrier;
revoke/fence/cancel/expiry and original aggregate exhaustion; unsupported media
and hostile tool escalation; missing cleanup and late callbacks; exact failure
commit/lost-ACK replay/fresh history/deletion after TTL expiry. Assert canonical
payload bytes/order, full debit, authentic evidence, actual dispatch counts,
no double reservation/materialization, no hidden reasoning and no extra writes.
Keep success, old notice-only and registry-sanitization regressions unchanged.

Follow [testing practices](../testing-best-practices.md): causal barriers/one
clock, immediate bounded cleanup, race, fresh uncached and repeated shuffled
focused tests, package/normal integration, exact stabilized independent review,
all nine required exact-head CI jobs and primary audit. Deterministic local
SQL/HTTP/TLS fixtures do not qualify native YDB/cloud, model quality, billing,
real profiles, WebUI rollout or either MVP track. Live credentials, provider/
MCP/search, deploy/migration/Monium operations remain unauthorized.
