# Attached-worker durable reconnect checkpoint

Issue #123 implements the worker-side AW-03d2 durable reconnect checkpoint and
issue #124 extends it with AW-03d3 active-attempt reconciliation. Together they
compose the exact server-authoritative reconnect endpoint from #122 with a
strict owner-local checkpoint. They do not enable foreground or daemon wiring,
timer cadence, provider execution, or OCI launch.

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

The embedded machine must be `ready` or `draining`, contain no in-progress
reconnect, and carry either idle authority or one bounded active-attempt
summary plus its optional terminal replay commitment. Restoration reconstructs
`MachineConfig` with the public identity key derived from the separately loaded
private key. Its configuration digest therefore detects substituted channel
binding, offers, scope, generations, or identity.

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
been validated. Each later validated exchange advances it with CAS, including
active-attempt states. Revocation, drain completion, a manifest/generation
update, or logout retires it. A crash after a remote attempt transition but
before local checkpoint replacement leaves an older claim; the server compares
that claim with the exact AW-02 snapshot and AW-04 attempt ledger and permits
only their deterministic reconciliation plan. Local evidence never advances
the server head.

## Active-state decision table

The durable server snapshot and attempt ledger are the authority. A matching
local claim determines only which already-committed message may need a fresh
connection envelope.

| Durable server head | Reconnect result | Forbidden repeat |
|---|---|---|
| `offered` | retransmit the existing LeaseOffer semantic message in a fresh envelope | lease allocation or process launch |
| `claimed` with no progress, or running progress | retransmit a lost LeaseAccepted when it is still pending; otherwise continue observing | lease claim, launcher, provider, or credential materialization |
| `cancel_requested` or cancelled before claim | retransmit the existing Cancel when pending | local cancellation effect or cancel-generation allocation |
| `cancel_acknowledged` | continue observing the existing cancelled attempt | CancelAck or local cancellation effect |
| `fenced_unknown` | preserve the fence and retransmit only the committed fenced Cancel when pending | execution, success commit, or retry under a new lease |
| `terminal_pending` | obey the protocol terminal decision: replay the exact committed terminal, discard it, or continue observing | new evidence or terminal sequence |
| `terminal_committed` | retransmit a lost TerminalAck until acknowledged, then retire through the normal acknowledgement proof | terminal materialization, canonical commit, or cleanup effect |
| idle | resume without attempt replay | creation of an attempt from local state |

A mismatched owner, worker, connection, enrollment/connection generation,
capability, binding, lease/fence, cancellation revision, terminal commitment,
or attempt sequence fails before activation. Expired authentication, and
expired lease/cancel authority where the active state still depends on it, fail
the same way. A committed terminal or explicit fenced-unknown head remains a
durable historical decision and is not revived as executable authority.

## Crash and lost-response matrix

| Boundary | Durable fact after restart | Recovery |
|---|---|---|
| before local generation fence | old manifest/checkpoint remain current | retry reconnect from the same exact predecessor |
| after local generation fence, before challenge/activation result | local generation is newer but remote outcome is unknown | stop with `reconciliation_required`; never initial-attach fallback |
| activation committed, response lost | consumed challenge stores one exact result | exact request replay returns that result; divergent replay conflicts |
| active attempt rebound, fresh platform envelope response lost | semantic ledger key/fingerprint are unchanged | the same pending semantic message is rebuilt under the current envelope and redelivered |
| worker action accepted remotely, checkpoint replacement lost | server snapshot/ledger are ahead of the local claim | AW-02 plan selects acknowledgement/replay/discard/fence; the local claim cannot invent progress |
| checkpoint rename completed but directory fsync failed | local durability is ambiguous | stop and reload exact state; no network or process effect is repeated |
| concurrent reconnect or stale checkpoint CAS loses | one server generation and one local revision remain current | deterministic conflict; loser cannot overwrite or retire newer evidence |

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
5. accepts only the server's bounded authoritative reconnect decision and
   exposes any terminal replay/discard/committed decision as read-only recovery
   intent;
6. opens the new bearer-bound exchange, sends the exact capability Manifest,
   and persists checkpoint revision 1 for the new generation.

Any ambiguity after the local generation fence returns
`reconciliation_required`. Reconnect never falls back to a new initial
attach and never infers success from local evidence.

## Remaining boundary

The reconnect API remains feature-disabled and side-effect free beyond the
existing protocol/store transitions. A separate reviewed cadence/foreground
slice must consume the read-only recovery intent, enable bounded polling, and
own process activation. In particular, reconnect itself never auto-sends a
terminal or starts/cancels a process.
