# Attached-worker canonical output receipt (#166 implementation contract)

Status: implemented and independently reviewed under closed
[#166](https://gitcode.com/urandon/sessionless/issues/166) and
[#79](https://gitcode.com/urandon/sessionless/issues/79), merged in MR !147.
The bounded two-owner positive receipt/TerminalAck gate passed; the real
provider remains disabled. #76/#133 own normal product composition and rollout
under the [WebUI-first MVP plan](mvp-delivery-plan.md).

## Why a receipt is necessary

The daemon's current Terminal digest commits process, isolation, credential
lifecycle, and cleanup observations. `CommitAttachedWorkerTerminal` requires a
different commitment: `runFinalizationDigest` of the canonical run status,
artifact manifest, and ordered session-event identities. The digests cannot be
substituted. The old synthetic-denied joined profile correctly leaves an
attempt in `terminal_pending`; it is not positive receipt proof. The separate
credential-bearing test-provider gate now proves canonical success and
TerminalAck for both owners. The normal Web/control bridge now supplies the
server receipt/finalizer; the worker's prepared Codex driver and credential
composition remain disabled under #133. Test success does not authorize
provider activation.

The positive path needs one server-owned, immutable output receipt. It is not
authority delegated to a worker and is never constructed from process stdout,
exit status, a provider thread ID, or caller-selected event IDs.

## Default-off receipt-session library (#133 preparation)

`attachedworkersealedinput.NewReceiptSessionSourceFactory` explicitly joins the
existing sealed-input source, accepted exchange and receipt publisher. It
accepts only input/receipt HTTPS endpoints on the exact same origin. Neither
HTTP channel is available before the session accepts `Open`; both retain
copies of that connection's bearer, never a later ambient credential. One
open/close state machine arbitrates acquisition and retirement, including a
delegate that returns after close. Session close revokes both clients and
cancels in-flight reads/publications without waiting for delegate close; outer
factory close releases the acquired exchange. Transport delegates still own
their exchange cancellation/close contract. Server-side transactional owner,
attempt, lease and policy authorization remains mandatory.

The receipt client owns the existing bounded exact response-loss replay. This
seam never retries an invocation or constructs a replacement candidate. The
YDB-tagged provider fixture reuses it instead of its own bearer/lifecycle
wrapper. Verify the seam without a real HTTP server, provider or YDB process:

```sh
make attached-worker-receipt-session-test
```

These fake-transport tests prove pre-open/post-close denial, same accepted
bearer, exact receipt replay, acquisition failure/race cleanup, and cancellation
on session/factory close. They are lifecycle proof, not cloud/provider E2E.
The ordinary activation path still calls `NewSessionSourceFactory`, whose
receipt channel is unavailable. `synthetic-denied`, normal service/materializer
provider denial, `DeniedCredentialLifecycle` and OCI network denial are
unchanged. Local credential custody, typed canonical input/result composition,
one credential/process owner, prepared driver, artifact/policy pins and reviewed
egress remain #133 work; #129 still gates cloud transport rollout.

## Authority and data flow

1. After the invocation and bounded teardown, the current owner-scoped
   connection submits one output candidate **and one typed process/cleanup
   observation** for its exact attempt. The connection bearer authenticates
   transport but does not by itself authorize output. A new mutating broker
   check must compare tenant, owner, worker, enrollment/connection generations,
   connection ID, run, attempt, reservation, lease ID, lease generation,
   numeric fence, context/capability/policy digests, expiry, and revocation
   state in one YDB transaction. Sealed-input's read-only check is not reused.
   `claimed` admits success or non-cancelled failure only while no cancellation
   revision exists. `cancel_requested` admits only a cancelled failure before
   its CancelAck deadline. After a valid CancelAck, `cancel_acknowledged` admits
   a cancelled failure while the lease, fence and protocol state remain
   current; the already-satisfied acknowledgement deadline does not bound
   teardown. Fenced,
   pre-claim-cancelled, retired and expired attempts admit no new receipt.
   These rules must match the existing protocol reducer, not bypass it.
2. Before submission, the daemon seals the bounded candidate, process
   observation and an opaque attempt-scoped idempotency nonce through its
   exclusive runtime lease in a private, durable local checkpoint. Exact
   resubmission is allowed; divergent overwrite is denied. The current
   reconnect constructor fences a sealed but unfinished submission before
   network access or fresh dispatch. Automatic post-crash replay is **not yet
   wired**; a lost response after the bounded same-process retry therefore
   leaves an explicit unknown outcome, never an invented ACK. The server
   validates the canonical manifest and ordered events against the run,
   limits, tenant-scoped immutable blob references, event-kind rules, and
   terminal status. It allocates canonical event IDs, checks the referenced
   object contents/digests and retains them through finalization. Large
   payloads remain in Object Storage; only bounded canonical metadata and
   verified references enter YDB. No provider credential or raw prompt/result
   enters transport logs or audit summaries. Bounded canonical display text,
   where the Session event contract requires it, remains tenant-scoped.
3. The server computes the same `runFinalizationDigest` used by YDB. A
   serializable preparation transaction stores a pending immutable receipt
   before copying any canonical objects; a second transaction marks it ready
   after exact object verification. Finalization accepts only ready receipts.
   The receipt stores the candidate fingerprint, generated event
   IDs, canonical digest, full execution binding (including the admitted
   provider resource and credential generation via the harness-binding digest),
   idempotency nonce, candidate
   fingerprint, and a distinct digest of the typed process observation.
   The key is tenant/owner/worker/attempt/lease generation; one attempt has
   at most one receipt. An exact retry with the same nonce and fingerprint
   returns those *persisted* event IDs and digest only when ready. A changed nonce, candidate,
   status, observation or binding conflicts. After a lost response, the daemon
   queries/replays this exact key and never submits a fresh outcome. The
   current server implementation supports exact retry, and the HTTPS client
   retries the identical request once after an ambiguous transport/server
   failure. Local crash replay is still missing. Retention covers the
   Terminal/ACK and historical replay windows. A pending receipt also acts
   as a deletion barrier: session deletion cannot be requested, inventoried,
   started, or completed while an Object Storage copy may still be in flight.
   Receipt preparation and ready transitions check deletion state in their
   YDB transactions. Only one durable copy writer may hold a pending receipt.
   A duplicate request cannot copy concurrently; after a synchronous copy
   failure proven to have happened before any remote `Put` was dispatched,
   the first writer releases that right for an exact retry. A timeout or lost
   Object Storage response is ambiguous: its write may still complete after
   the local call returns. That case retains the copy barrier and blocks both
   retry and session deletion until a separate remote-quiescence proof exists.
   A crash is treated the same way; age alone is not a quiescence proof.
   The S3 adapter uses one SDK attempt for `PutObject` and disables Go HTTP
   replay of body-bearing PUTs; otherwise a hidden first attempt could finish
   after a later successful retry and invalidate the ready/deletion decision.
   This does not solve an ambiguous single request: it remains pending until
   remote quiescence is proven. An upstream proxy that retries PUTs without
   exposing that fact is outside this adapter contract and must not be used
   for the receipt copy path. Zero-byte candidate objects are rejected before
   any receipt copy: HTTP/2 can retry a bodyless PUT even without `GetBody`.
4. The observation records bounded, content-free exit/cancel/deadline,
   descendant reap, isolation teardown/release, attempt-root cleanup and
   credential lifecycle outcomes. It is authenticated and bound to the same
   execution attempt, not supplied later by a terminal-commit caller. The
   server rejects a committable receipt unless teardown, descendant reap,
   root cleanup and required credential release are successful. Local daemon
   attestation is not proof against a compromised host; the rootless canary
   checks the platform boundary separately. A failed cleanup is audit-only
   blocked evidence, never a canonical success or ACK.
5. The daemon may emit Terminal only with the receipt's canonical digest.
   Its process-observation digest remains distinct and cannot stand in for
   output. If receipt creation or verification fails, the daemon cannot
   invent a canonical failure event or acknowledge itself. The attempt
   remains pending or is fenced by existing deadlines.
6. Terminal reception persists pending evidence only. In the finalization
   transaction, the server loads the immutable receipt and its process
   observation, rechecks their full binding, cleanup predicate, current
   worker/connection authority, lease and fence, checks Terminal status and
   digest against the receipt, and revalidates the canonical materialization.
   Only then does it write Session events, run/attempt/quota changes and
   TerminalAck atomically. Revoke, expiry, stale terminal or changed receipt
   yields no product mutation or ACK.

An authorized reconnect may rebind the current attempt to a new connection
ID/generation via the existing server-side reconnect transaction. The receipt
retains its immutable **execution** connection; a new bearer may only recover
it after the current attempt head and protocol snapshot prove that exact
rebind, same owner/attempt/lease/fence and pending Terminal digest. It cannot
resubmit divergent output or replay an old connection envelope. Revocation,
unauthorized replacement, or stale generation does not transfer authority.
The finalization transaction verifies this chain, not an impossible equality
between the original execution connection and the new transport connection.

The positive path needs an explicitly negotiated, capability-pinned receipt
feature. The existing `synthetic-denied` profile still emits process digests
and must never be reinterpreted as receipt-capable. The single V1 Terminal
digest field carries the canonical digest only for the negotiated path;
process observation is stored separately before Terminal. Missing feature,
ready receipt or observation fails closed, without a V1 downgrade fallback.

For attempts whose pinned capability manifest negotiates `FeatureOutputReceipt`,
`AttachedWorkerTerminalCommit` loads the immutable receipt inside its transaction
and rejects caller-supplied materialization. The legacy non-receipt profile
still has a supplied-materialization branch; it is not this positive-result
path. Activation must require the receipt feature, never downgrade to that
legacy authority. Migration and production rollout must remain explicit:
no dual-write fallback from receipt to the old caller-supplied path.

## Proof order and dependency boundary

The #79 gate can use a credential-bearing **test provider** with distinct
owner-scoped resource IDs and generation tags; it must not use a real provider
token or claim entitlement. That lets #79 test the credential-selection and
provider-resource isolation boundary before #81 enables `codex exec`. #81 then
reuses the reviewed receipt contract for its real adapter; it does not have to
be production-enabled to run the #79 security gate. Unsupported provider
policy, egress, filesystem isolation, or credential mode still denies before
invocation.

The closed #79 gate uses a joined daemon/test-provider path and one authoritative
YDB control plane. Its bounded acceptance includes:

| Case | Required observation |
| --- | --- |
| A and B with colliding worker locator/identity | Distinct connection, job, sealed input, resource, credential generation, output receipt, and terminal scope. |
| Cross-owner output or receipt replay | No receipt, canonical event, or ACK in the foreign scope. |
| Same-key divergent output; lost response | Divergence conflicts; exact retry returns one immutable receipt. |
| Revoke, stale generation, authorized reconnect | Old bearer/envelope cannot finalize; only an exact server-rebound current attempt can recover one receipt. |
| Applicable cancel, stale fence and ambiguous outcome negatives | No invented success or automatic repeated provider effect. Existing crash/protocol safety gates remain, without an exhaustive interleaving requirement. |
| Process success without receipt, receipt without cleanup, lost submission response | No premature ACK; exact nonce/fingerprint recovery or a fenced unknown outcome. |

The mandatory race-clean joined-provider/YDB gate, exact-head CI and independent
review passed for #79/#166. Actual filesystem/network/process isolation remains
a separate rollout-platform gate in #133; mock OCI or local Mac dockerless
evidence does not prove a different rootless platform. A new joined two-owner
rootless canary and exhaustive cancel/crash/network/split-brain simulation are
not conditions for reopening #79. Automatic post-crash replay and remote
quiescence after ambiguous PUT remain later hardening; the current path blocks
instead of repeating provider effects or claiming success. This contract alone
grants no provider-call, deployment, credential-write or enablement authority.
