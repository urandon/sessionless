# Attached-worker durable reconnect checkpoint

Issue #123 implements the worker-side AW-03d2 half of durable idle reconnect. It
composes the exact server-authoritative reconnect endpoint from #122 with a
strict owner-local checkpoint. It does not enable foreground or daemon wiring,
timer cadence, provider execution, OCI launch, or active-attempt recovery.

## Authority boundary

`reconnect-checkpoint.json` is continuation evidence, never product
authority. The worker signs a claim derived from the checkpoint; the control
plane compares that claim with its exact durable connection and AW-02 machine
snapshot before it creates a new connection generation. A local file,
watermark, PID, timestamp, or worker assertion cannot create, revive, or
advance server state.

The checkpoint is accepted only when all of these values match the current
local manifest exactly:

- manifest revision, tenant, owner, and worker;
- enrollment and connection generations;
- AW-02 protocol version, immutable offers, and capability digest;
- prior connection identity, authentication expiry, and channel-binding
  digest;
- the canonical, digest-sealed `MachineSnapshotV1`.

The embedded machine must be `ready` or `draining`, have an idle attempt,
contain no pending terminal replay, and contain no in-progress reconnect.
Restoration reconstructs `MachineConfig` with the public identity key derived
from the separately loaded private key. Its configuration digest therefore
detects substituted channel binding, offers, scope, generations, or identity.

The file contains no private key, connection secret, bearer, provider
credential, request/response body, artifact, or harness payload. The
channel-binding value is a one-way digest already bound into the AW-02
configuration; it is not sufficient to authenticate a request.

## Persistence and invalidation

Only the holder of `runtime.lock` may load, write, or retire the checkpoint.
Every write:

1. reloads the exact manifest and current checkpoint under `state.lock`;
2. checks the expected checkpoint revision and requires exactly revision +1;
3. encodes one strict, bounded JSON value;
4. writes a same-directory mode-`0600` temporary file;
5. fsyncs the file, atomically renames it, and fsyncs the directory.

A post-rename durability failure is `state_ambiguous`. The session stops and
requires an exact reload; it does not repeat the network effect. Invalid JSON,
unknown or case-colliding fields, future versions, permission drift,
cross-owner/worker substitution, generation drift, and snapshot-digest drift
fail closed before network access.

Initial attach writes checkpoint revision 1 after the Manifest exchange has
been validated. Each later idle exchange advances it with CAS. Entering an
active attempt, revocation, drain completion, a manifest/generation update, or
logout retires it. A crash between a remote active-attempt effect and local
retirement may leave an older idle checkpoint, but #122 rejects that claim
against the non-idle server snapshot without mutation. Recovering that active
effect belongs to AW-03d3.

## Reconnect sequence

On restart, `Connector.Reconnect` acquires `runtime.lock`, loads the exact
manifest, secret record, and checkpoint, and restores the prior conformance
machine. It then:

1. generates a fresh connection secret and worker nonce;
2. atomically advances the local manifest/secret connection generation,
   retiring the old checkpoint before any network call;
3. requests a reconnect challenge signed with the current enrollment identity
   and prior connection generation;
4. derives a fresh channel binding and builds the signed reconnect claim from
   the restored machine;
5. accepts only the server's bounded authoritative reconnect decision;
6. opens the new bearer-bound exchange, sends the exact capability Manifest,
   and persists checkpoint revision 1 for the new generation.

Any ambiguity after the local generation fence returns
`reconciliation_required`. Reconnect never falls back to a new initial
attach and never infers success from local evidence.

## Remaining boundary

AW-03d3 must reconcile active attempt/lease/cancellation/terminal commitments
with the durable AW-04 ledger. Only after that reviewed slice may a separate
cadence/foreground slice enable a timer poller or process activation.
