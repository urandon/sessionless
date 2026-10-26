# Attached resource generation publication

Version: 0.1.0. Status: accepted engineering design; implementation and activation pending.
Owner: [#133](https://gitcode.com/urandon/sessionless/issues/133).
Prerequisites: [owner-local state](owner-local-subscription-state.md),
[native boundary](external-harness-boundary.md) and
[credential lifecycle](../credential-lifecycle.md).

## Decision and boundary

Add authenticated, metadata-only publication of the owner-local credential
generation into the existing server subscription resource. A server-approved
baseline explicitly delegates deterministic generation-only evidence
projection. New attempts receive immutable bindings at the published generation;
completed attempts retain their original binding. Defaults remain off.

This is not credential custody, entitlement discovery, an OAuth-account parser,
provider introspection or a new scheduler/lifecycle. Initial registration is
trusted operator input. The existing lifecycle preserves its binding's provider,
owner and entitlement across exact CAS; it compares bounded credential bytes,
not provider principals. See [service.go](../../internal/credentiallifecycle/service.go)
lines 207–266 and [securefs_unix.go](../../internal/credentiallifecycle/securefs_unix.go)
lines 330–364. Neither publication nor a successful native turn proves account
continuity, provider permission, quota or subscription entitlement.

## Existing seams and missing composition

The operator-configured [AttachedResourcePin](../../internal/sessionlessharness/attached_binder.go)
lines 15–28 carries a binding, not all six evidence records. Its binder validates
current worker/connection identity, then copies that fixed template (lines
86–148). [Onboarding](../../internal/ydbstore/attached_worker_onboarding.go)
lines 104–138 creates a subscription with empty credential reference and unknown
entitlement. [Its existing row](../../migrations/ydb/00006_create_subscription_connections.sql)
has no credential-generation head. None supplies this publication protocol.

The native consumer also needs work. [NativePreparedConfigV2](../../internal/codexexec/native_policy_v2.go)
lines 27–52 freezes one bundle. [Construction and Session binding](../../internal/codexexec/native_prepared_v2.go)
lines 16–57 retain it; lines 99–106 pass it into the boundary.
[Prepared Run](../../internal/codexexec/prepared_composition.go) lines 138–140
validates that same bundle against each job. Merely publishing generation N+1
cannot make this fixed-N consumer run a second-generation job.

Additionally, [normal sealed input](../../internal/attachedworkersealedinput/service.go)
lines 83–110 rejects nonfixture providers; its credential request builder is
explicitly test-only (lines 164–205). This design requires a reviewed native
subscription source path, not promotion of `allowTestProvider`.

## Trusted baseline and deterministic projection

Define one canonical `AttachedResourceGenerationBaselineV1`: exact tenant,
owner, subscription, worker, enrollment, positive baseline revision, approved
binding template, all six original evidence records and explicit delegation
for credential-generation projection. Configure that complete trusted baseline
on both server and native worker. A pin's digests alone cannot reconstruct it;
a worker-supplied bundle, matching hash or auth file cannot install it.

The delegation covers only the same approved resource/backend/model/profile,
data classes and policy semantics during the baseline's original validity
window. It treats credentials as the trusted owner's opaque local resource;
it makes no same-account claim. Entitlement and quota remain independently
server-authorized. Changed scope, enrollment, artifact/profile, policy or model
requires a replacement approved baseline, not a generation report.

One pure, closed `ProjectAttachedResourceGenerationV1` operation copies the
baseline and changes only `Resource.CredentialGeneration`, including that field
in each evidence scope. `Resource.Revision` remains the baseline revision;
credential rotation is not baseline replacement. Recompute component digests,
policy digest references and binding digests in dependency order. Preserve
every observation timestamp, state, support/route claim, allowed class, named
unknown and expiry exactly. Never stamp refresh time as a provider observation.
The resulting binding expiry is still the original minimum evidence expiry.

This is necessary because [bundle validation](../../internal/domain/provider_evidence_bundle.go)
lines 42–51 binds all six scopes to the full resource, while lines 82–110 bind
digests, policy links and minimum expiry. Projection is deterministic data,
not authority by itself: both consumers require the independently installed
baseline, current authorization and exact generation head.

### Separate dynamic and installation policy identities

There is another concrete consumer mismatch:
[canonical authority](../../internal/ports/canonical_ingress.go) lines 213–214
requires placement `PolicyDigest` to equal the binding's dynamic
`EffectivePolicyDigest`. However, [native construction](../../internal/codexexec/native_conduit_v3.go)
lines 93–95 compares the accepted attempt's policy digest to the static
installation digest. These cannot be aliases: generation projection changes
the evidence-policy digest, not the installation.

Use an explicit `NativeProfileV3` semantics successor with policy identity
`OAI-SUB-NATIVE-ENVELOPE-V3`, revision 1. Preserve exact canonical dynamic
equality; derive the placement digest from that projected policy. The native
constructor must compare the accepted dynamic policy with the original sealed
binding and qualified projected bundle. Independently recompute and verify
static `launch.PolicyDigest` and its pinned backend descriptor:
[native_profile_v2.go](../../internal/codexexec/native_profile_v2.go) lines
161–173 already commit static-profile plus installation-policy identity into
`BackendProfileDigest`. Retain the original attempt/resource scope digest and
socket/installation checks. No new wire authority is needed. This is a versioned
native adaptation, not relaxed comparisons, global equality changes or silent
reinterpretation of admitted V2 profiles/jobs; no legacy alias is accepted.

## Metadata shape, authentication and CAS

Add one bounded `attached_resource_generation_v1` JsonDocument column to the
existing `subscription_connections` row; update its entity-registry entry.
There is no new table, secret store, historical snapshot collection or operation
journal. Its closed `AttachedResourceGenerationHeadV1` fields are `version`,
`owner_user_id`, `worker_id`, `enrollment_generation`, `baseline_revision`,
`credential_generation` and `revoked`. Row keys provide tenant/subscription scope.
Credential references remain empty. Do not store secret bytes, fingerprints or
hashes of auth bodies. Keep existing entitlement/quota/observed-at semantics;
publication may update the row's ordinary modification timestamp only.

One strict canonical request carries that scope, current connection ID and
connection generation, expected credential generation and target head. One
response returns the authoritative head. Reject duplicate members, unknown fields,
aliases and unsupported versions; there is no legacy spelling or inference.
Request, response and persisted metadata are each at most 4 KiB, with existing
ID validation, positive bounded scalars and checked uint64 arithmetic. One
operation and at most one exact point-read reconciliation share a caller-bounded
window of at most 15 seconds; no scan or automatic retry loop is introduced.

Extend the existing authenticated connection service/handler composition.
[Bearer binding](../../internal/attachedworkertransport/service.go) lines
699–745 derives the secret digest rather than accepting it remotely; parsing
alone does not authorize a write. In the mutation transaction, recheck current
tenant membership, resource-owner projection, approved baseline, worker,
enrollment, current connection/secret, capability and authentication expiry.
Only the exact authorized worker may publish its resource's metadata.

Resolve ownership through `subscription_connections.actor_id` to
`actors.user_id` and the exact `subscription_connections_by_user` point row,
as onboarding lines 72–78 and 105–118 do. Do not assume an existing owner column
or equate actor ID with user ID. This server row's provider is `codex`; the local
OAuth binding's `openai`/`chatgpt` tuple is a different layer. Reported active
state never flips server entitlement or quota to active.

Initial absent expected-0 to active generation-1 requires the installed
baseline and explicit owner-local initialization; it does not set entitlement
active. Normal active publication requires current expected-N and target N+1,
same scope/baseline and unrevoked head. Reserve the final generation for revoke,
matching local overflow rules. Exact duplicate target tuples return the same
head without another increment. Conflicting/stale requests fail closed.

Revoke publishes denial, never eligibility. An authenticated higher revoked
generation may supersede a stale active head even if an intermediate refresh
ACK was lost; no active generation jump is permitted. Exact revoked duplicates
are idempotent. A revoked head cannot be reactivated by this endpoint. Local
revoke denies immediately; an offline server is not claimed to know it before
publication or existing connection expiry fences take effect.
Revocation still uses current identity authorization, but does not require
positive admission eligibility or an unexpired evidence window first.

## ACK, reconnect and new admission

The original connected Session owns the metadata client and its bearer lifetime,
using the [existing source-factory seam](../../internal/attachedworkersealedinput/session_source.go)
lines 84–108. It reports metadata from the same owner-local store used by the
lifecycle/currentness reader, not arbitrary caller DTOs. Lost reply allows only
an authenticated exact point read of the current head. An exact target tuple,
including its revoked boolean, is an ACK; another generation, unexpected revoke
or baseline mismatch is not. Never redo
WriteBack, create another candidate or call the provider to recover an ACK.

Reconnect authenticates the new current connection and reads the same resource
head. It does not advance credentials, clear revoke or reconstruct execution
authority. New enrollment needs the existing explicit baseline replacement.
Missing/mismatched ACK disables new local execution; no second durable ACK
ledger is needed. Existing attempt recovery and terminal ownership remain intact.

The binder reads the published head and projects a fresh immutable per-attempt
binding. New admission rechecks the exact head inside
[admitDispatchTx](../../internal/ydbstore/scheduler.go) before new reservation,
job and offer writes (lines 150–269, 433–498). The direct
[offerAttachedWorkerAttemptTx](../../internal/ydbstore/attached_worker_execution.go)
path must perform the same resource check before allocation (lines 197–252).
No stale outbox/job is rewritten from N to N+1; it is denied. Compute-choice
observations are not a substitute for this transactional admission check.

## Private native handoff and terminal separation

Replace the generation-enabled consumer's fixed-bundle assumption with the
same explicitly configured, immutable baseline. For each actual authenticated
Session/accepted sealed job, privately project at that job's generation after
checking the acknowledged head and baseline. Validate all projected digests,
scope and expiry against the original sealed binding before Issue, then retain
that immutable projected bundle with its original prepared entry/boundary.
Never mutate global profile/config evidence, a previous slot or sealed authority.
Do not expose an arbitrary projector callback or accept worker-authored grants.

Provide an explicit normal native-subscription sealed-input composition. Derive
one credential IssueRequest from authoritative Run/Attempt/Lease, owner and
original job resource; preserve the actual Session, accepted-attempt and input
rechecks. The sole outer runner uses the owner-local store/lifecycle. No cloud
secret read, second Issue, ambient login adoption or test-provider flag follows.

After its own successful N-to-N+1 WriteBack/Release, an original completed N
result keeps its original producer, evidence, receipt/finalizer and exact ACK
identity. The receipt-only correction remains a prerequisite; it grants no new
launch, CONNECT, body replay or provider invocation. Publication/ACK for N+1
gates later execution, not completion of already-owned N output. Existing
one-shot export, READY reconciliation, expiry and retirement rules remain.

## Alternatives and implementation handoff

Manual operator approval per refresh preserves the boundary but makes ordinary
token rotation operationally impractical. Removing generation from evidence
scopes is a broader versioned redesign. Delegated deterministic projection is
the smallest explicit extension without SDK interception or new account gates.

Keep these unchecked implementation rows under #133, not a duplicate epic:

1. [#233](https://gitcode.com/urandon/sessionless/issues/233): server
   metadata/auth/CAS/direct-read adapter and existing-row migration;
   trusted baseline/projector plus transactional binder/admission integration.
2. [#234](https://gitcode.com/urandon/sessionless/issues/234): original Session
   publication/ACK client and native per-attempt baseline
   consumption; versioned dynamic/install policy-role separation and explicit
   normal subscription sealed-input source composition.
3. [#235](https://gitcode.com/urandon/sessionless/issues/235): synthetic tests
   for initial/refresh/revoke, foreign/stale/duplicate reports,
   lost ACK/reconnect, expiry/overflow, exact digest projection and unchanged
   observations, cross-role digest substitution denial, two-generation native
   consumption and completed-N terminal/ACK.

#232 retains local persistence; #219/#220 and applicable #199/#201 retain their
existing native/platform/media owners. Exact review/CI, actual qualification,
separately authorized useful WebUI proof under #35 and reversible resource
activation remain #133's existing checklist. This design supplies no activation,
provider permission or demo-completion claim and creates no new research gate.
