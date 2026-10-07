# Attached-worker canonical output receipt (proposed #166 contract)

Status: proposed for owner review under
[#166](https://gitcode.com/urandon/sessionless/issues/166), a bounded child of
#79. This document is not an enablement decision. The current activated daemon
remains `synthetic-denied`, and #79 is not complete.

## Why a receipt is necessary

The daemon's current Terminal digest commits process, isolation, credential
lifecycle, and cleanup observations. `CommitAttachedWorkerTerminal` requires a
different commitment: `runFinalizationDigest` of the canonical run status,
artifact manifest, and ordered session-event identities. The digests cannot be
substituted. The current joined-daemon gate correctly leaves the attempt in
`terminal_pending`; it cannot prove a successful provider turn or TerminalAck.
There is no production caller that supplies canonical materialization to
`CommitAttachedWorkerTerminal` today.

The positive path needs one server-owned, immutable output receipt. It is not
authority delegated to a worker and is never constructed from process stdout,
exit status, a provider thread ID, or caller-selected event IDs.

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
2. The daemon seals the bounded candidate, process observation and an opaque
   attempt-scoped idempotency nonce in its local durable recovery state before
   submission; losing that state cannot trigger re-execution. The server
   validates the canonical manifest and ordered events against the run,
   limits, tenant-scoped immutable blob references, event-kind rules, and
   terminal status. It allocates canonical event IDs, checks the referenced
   object contents/digests and retains them through finalization. Large
   payloads remain in Object Storage; only bounded canonical metadata and
   verified references enter YDB. No provider credential or raw prompt/result
   enters transport logs or audit summaries. Bounded canonical display text,
   where the Session event contract requires it, remains tenant-scoped.
3. The server computes the same `runFinalizationDigest` used by YDB. One
   serializable insert-or-read stores the immutable candidate, generated event
   IDs, canonical digest, full execution binding, idempotency nonce, candidate
   fingerprint, and a distinct digest of the typed process observation.
   The key is tenant/owner/worker/attempt/lease generation; one attempt has
   at most one receipt. An exact retry with the same nonce and fingerprint
   returns those *persisted* event IDs and digest. A changed nonce, candidate,
   status, observation or binding conflicts. After a lost response, the daemon
   queries/replays this exact key and never submits a fresh outcome. Retention
   covers the Terminal/ACK and historical replay windows.
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
receipt or observation fails closed, without a V1 downgrade fallback.

The server must not continue accepting `AttachedWorkerTerminalCommit` with
caller-supplied materialization once the receipt path is active. That method
should load the immutable receipt inside its transaction; otherwise a second
materialization authority remains possible. Migration/rollout must be explicit:
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

Required deterministic cases, using two activated daemon processes and one
authoritative YDB control plane:

| Case | Required observation |
| --- | --- |
| A and B with colliding worker locator/identity | Distinct connection, job, sealed input, resource, credential generation, output receipt, and terminal scope. |
| Cross-owner output or receipt replay | No receipt, canonical event, or ACK in the foreign scope. |
| Same-key divergent output; lost response | Divergence conflicts; exact retry returns one immutable receipt. |
| Revoke, stale generation, authorized reconnect | Old bearer/envelope cannot finalize; only an exact server-rebound current attempt can recover one receipt. |
| Cancel, crash, network loss, split brain | One fenced outcome; no invented success or duplicate provider effect. |
| Process success without receipt, receipt without cleanup, lost submission response | No premature ACK; exact nonce/fingerprint recovery or a fenced unknown outcome. |
| Rootless two-owner canary | Actual filesystem/network/process isolation, no ambient credential path, clean attempt roots, untouched sentinels. |

Run the joined cases in the mandatory race-clean release gate. Keep the real
rootless canary separately opt-in until the CI runner and exact pinned image
are reviewed; do not present mock OCI or a local Mac dockerless run as that
platform proof. #79 closes only after the receipt implementation, credential
test path, joined/rootless evidence, exact-head CI, and independent review all
pass. This proposal itself grants no permission for provider calls, cloud
deployment, credential writes, or production enablement.
