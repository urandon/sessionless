# Owner-local subscription state

Version: 0.1.0. Status: accepted engineering design after independent review.
The `internal/ownerlocalcredential` backend implements the local storage and
existing-lifecycle wrapper under [#232](https://gitcode.com/urandon/sessionless/issues/232).
Serving composition, operator key/auth provisioning and rollout remain separate.
Owner: [#133](https://gitcode.com/urandon/sessionless/issues/133); prerequisite
contracts: [credential lifecycle](../credential-lifecycle.md) and
[native boundary](external-harness-boundary.md). This is design only, not an
implemented backend, provider entitlement, serving profile or rollout permission.

## Scope and source findings

Implement the existing `ports.CredentialBindingStore` and
`ports.CredentialSecretStore` for one explicitly configured owner-local
subscription scope. Its metadata `LoadCredentialBinding` also implements
`codexexec.NativeCredentialBindingReaderV2`; it never reads secret bodies.
Keep `Issue -> Materialize -> Execute -> WriteBack -> Release`, the original
generation/revocation fence and exact native currentness checks unchanged.

`internal/credentiallifecycle/service.go` implements that lifecycle. Its
subscription binding/secret ports are implemented by `internal/ownerlocalcredential`;
the same store supplies metadata-only native reads, while `NewLifecycle` wraps
the original service with the exact-handle admission guard. The provider-resource
YDB adapter is not interchangeable: #85 explicitly separates subscription
bindings. `attachedworkeractivation.ProfileV1` remains synthetic-denied.

`internal/ydbstore/attached_worker_onboarding.go` authenticates owner membership
and worker identity, then registers a Codex resource with empty credential
reference and unknown entitlement. It does not publish credential generation.
`compute_choice_evidence.go` constructs structural immutable evidence, expressly
not an eligible serving registration. No authenticated owner-local OAuth
generation-publication path is established by these sources. That missing
composition remains a mandatory #133 item, not an inferred existing API.

Non-goals: cloud credential custody, YDB/schema changes, another scheduler or
credential state machine, ambient login discovery, TLS/SDK interception,
provider calls, Pi, automatic activation, live key provisioning or rotation.

## One authority and local ownership

One explicit canonical root belongs to exactly
`(tenant_id, owner_user_id, subscription_connection_id)` and the current OS
owner. Never infer it from HOME, environment, an auth file or installed Codex.
The operator installs exactly one such root per scope; copying/restoring it
does not register a second refresh owner or authorize rollback of a revocation.
Host administrators and malicious same-UID store rewriting are outside this
trusted-host boundary; protection against their rollback is not claimed.

One backend instance holds a nonblocking cross-process owner flock for its
lifetime. Busy means deny, not wait/retry or open another root. The same live
instance supplies lifecycle storage and native metadata reads; short metadata
mutations serialize internally. An exact-scope wrapper in the existing
foreground/lifecycle composition acquires a one-window admission guard before
Issue. Pre-handle Issue failure releases it; success binds it to the exact
returned handle through Materialize/Execute/WriteBack/Release. Failed Release
retains denial and original cleanup ownership until that exact cleanup is
definitive; no next window opens. Revoke bypasses this admission guard and can
run during execution, but cannot release it before original cleanup is definite.
Do not hold a mutation mutex across process execution or create another
lifecycle/lease/durable entity. Process death drops the flock, not durable facts.

## Durable shape and encryption

Use one versioned bounded metadata snapshot containing exact scope, revision,
per-key cumulative encryption-use counter and key epoch, canonical binding,
candidate inventory and superseded-reference cleanup intents. Binding CAS and
its old-reference cleanup intent are one
atomic image, never two independent writes. No secret plaintext enters it.
Strict decoding rejects unknown/duplicate/alias fields, unsupported versions,
invalid transitions or overflow; there is one canonical spelling, no fallback.
An adapter-private persistence codec preserves `SecretRef.StorageValue()` in
that private image and reconstructs it through its validated constructor; generic
Marshal of `CredentialBinding` would omit it. Public domain JSON/formatting
continues to omit/redact the reference; no public DTO gains a raw locator.

Secret candidates are immutable encrypted regular files. Use standard-library
AES-256-GCM with an explicitly injected operator 32-byte key; missing/wrong key
fails closed. Dedicate each key to this exact scope, canonical root and key
epoch: never reuse it across roots/epochs. The key is not stored beside this root, in environment/argv,
generic DTOs/logs, source files or an ambient keychain. Key acquisition,
replacement, backup and rotation require a separately authorized procedure;
there is no automatic key generation or recovery fallback here. Clear owned
temporary plaintext/key buffers when their bounded lifetime ends; this is not
a guarantee of erasing every runtime copy or historical filesystem blocks.

Allocate a fresh 256-bit random opaque reference and standard random GCM nonce
using `crypto/rand`; collision/entropy failure denies without overwrite. Define
one closed binary envelope with bounded canonical header, nonce and ciphertext.
Authenticate the exact version, scope/root/key-epoch identity, reference, original expected generation,
plaintext length and fingerprint as AAD. The header cannot supply authority.
On read, join those fields to the current binding or exact candidate/cleanup
record before decryption; authenticate before exposing plaintext and verify its
length/fingerprint afterward. A candidate created at expected N can be current
only through the matching N-to-N+1 binding publication. Keep that creation
generation in its immutable envelope after commit; never rewrite its AAD.
Before encryption, atomically reserve its candidate slot and increment the
durable use counter; a missing body grants no binding authority. Bound one key
to fewer than 2^32 encryptions, including failed/abandoned reservations, not only
surviving files. Exhaust means deny until separately authorized key rotation;
never reset the counter, reuse a nonce or silently replace the key. A pre-initial
snapshot may have no binding; only explicit registration can publish one.
Ordinary crash reopen preserves the original store's durable counter. Backup,
copy or restore is not an automatic reopen and cannot reset usage accounting.
Same-key restore is unsupported when all prior uses cannot be proved; require
separately authorized fresh-key provisioning and monotonic binding/revocation
reconciliation, otherwise stay disabled. No global counter/KMS or detection of
malicious same-UID rollback is claimed.

Plaintext exists only in bounded trusted lifecycle memory and its existing
ephemeral `auth.json` materialization, never a durable candidate/temp file.
The native child receives only that exact file, not this root or encryption key.

## Filesystem, capacity and publication

Linux/Darwin adapters use pinned directory FDs, no-follow component traversal,
relative operations and stable inode/owner checks: root 0700; regular files
0600, same OS owner, Nlink=1. Reject replacement, hardlinks, symlinks, mode drift,
unexpected entries and unsupported platforms before authority or allocation.
Pin and recheck lock/root identity; replacing a pathname cannot mint ownership.

Reuse the principles in `localoperator/secure_unix.go`,
`credentiallifecycle/securefs_unix.go`, `attachedworkerlocal/lock_unix.go` and
`attachedworkerlocal/store.go`'s `writeAtomic`: they are private implementations,
not already exported interchangeable storage adapters. New adapter code must
preserve descriptor-relative safety across creation, rename, read and unlink.

Caps are explicit constructor policy: auth plaintext at most 1 MiB (existing
lifecycle default); envelope header at most 4 KiB plus GCM nonce/tag; metadata
at most 64 KiB; at most four uncommitted candidates, four cleanup intents and
one committed active secret. Reserve the fourth cleanup slot and its complete
metadata bytes exclusively for revoking the last active reference. Ordinary
rotation may occupy at most three cleanup slots. A full candidate/cleanup
inventory denies new rotation/Issue as applicable, never evicts active/pending
custody and never blocks publication of that reserved revoked shape.
Allow at most one ciphertext temp and one metadata temp; conservatively cap
owned store files at 12 MiB before writes. Counts/sizes and existing filesystem
free space are checked prospectively, with actual bounded reads/writes too.
Disk exhaustion may still defeat fsync; report failure/unknown, not durable revoke.
No unbounded directory walk, historical versions, background daemon or GC scan.

Persist and fsync candidate ciphertext before making it the binding target.
Write a fresh same-directory metadata temp, fsync it, atomically rename it and
fsync the directory. After successful rename but failed directory fsync,
publication is UNKNOWN: reload the exact image/relation under the same owner,
do not roll back, publish another candidate or repeat the provider invocation.
Do not report crash durability merely because a read now sees the new image.
An unlink is complete only after directory sync; uncertainty retains cleanup
responsibility. Successful file removal does not claim secure physical erasure.

## Registration, refresh and revoke

Initial registration is a trusted explicit operator input with independently
authorized exact scope/provider/auth mode/policy. Persist its encrypted
candidate, then atomically publish absent expected-0 to generation-1 plus its
candidate relation. This is an adapter initialization operation, not an added
public port or an arbitrary caller-supplied active DTO. A supplied `auth.json`
alone proves neither entitlement nor resource eligibility; never overwrite an
existing/revoked root through initialization.

Unchanged WriteBack performs no candidate write or generation change. Changed
WriteBack retains the existing candidate -> CAS -> commit order. CAS checks
owner/scope/provider/auth mode, exact positive current generation, unrevoked
state, next=N+1 and candidate reference/fingerprint/expected-N relation.
Reject wrapped/zero or active MaxUint64 generations: reserve the final uint64
generation for the revoked shape so an active binding cannot become unrevokable.
Normal refresh at that boundary denies; exact revoke can still advance once.

Revoke atomically publishes the next revoked binding, clearing active reference,
fingerprint and entitlement, with the reserved superseded cleanup intent before
materialization cleanup. Late WriteBack loses CAS. Repeated exact revoke returns
the existing revoked result without incrementing or restoring eligibility.
The existing lifecycle must finish exact registered-file cleanup before secret
deletion; Release keeps its independent bounded budget after failed WriteBack.
Neither cleanup lag nor key/IO failure restores active authority.

Recover only the current binding's exact uncommitted candidate relation.
Promotion is metadata-only; no secret rewrite or provider call. Candidate lists
are scope-bound and capped, not guesses about which secret should be active.
Never delete a candidate/secret referenced by the active binding, including a
current candidate whose commit reply was lost. Orphan cleanup uses an exact
noncurrent relation and durable intent, not age or filename alone.

## Sealed generation and terminal ownership

Every new attempt's immutable resource CredentialGeneration must equal the
local binding generation. Never upgrade a sealed job after refresh, reinterpret
the generation as a lower bound or silently reuse its previous binding.
New native launch/egress remains subject to existing currentness checks.
The necessary authenticated metadata-only publication/acknowledgement into
the existing resource/admission owner must be designed in #133 composition;
on a missing/stale publication, new admission stays disabled. No secret/ref
leaves the owner host and no second authority table is introduced by this plan.

Successful WriteBack by the original completed invocation must not invalidate
its already-authenticated exact output receipt, finalizer or ACK reconciliation
merely because generation advanced. Keep those terminal owners/fences intact;
they grant no new native connection, process, body send or provider replay.
The composition review must trace the ordering explicitly, not weaken the
native currentness reader to let a stale execution proceed.

## Failures and verification

| Boundary | Required result |
| --- | --- |
| Busy/wrong owner/key/scope, malformed or missing active body | Deny before Issue/native access; content-free error |
| Candidate durable, CAS not published | Exact bounded orphan relation; no active authority |
| CAS published, commit interrupted | Recover only that binding-to-candidate relation |
| Rename/fsync or cleanup result ambiguous | Preserve exact state/custody; no rollback/replay/success claim |
| Revoke races refresh or inventory is near cap | Revoke shape uses reserved headroom; stale CAS cannot resurrect |
| Generation/nonce/key-use/disk bound exhausted | Fail closed; no wrap, eviction, auto-rotation or provider retry |

Implementation needs deterministic two-owner/cross-scope, read-after-reopen,
initialization collision, max-boundary/overflow, revoke-vs-CAS, unchanged/changed
writeback, both candidate/CAS crash points, commit/revoke reply loss, active
candidate deletion denial, authenticated-envelope tamper and filesystem
replacement/hardlink/mode controls. Verify actual ciphertext-only durable files,
bounded ownership/cleanup and marker redaction with synthetic keys/bodies only.
Use focused/package race/shuffle/repeat and exact-head CI; fake/source checks
are not OAuth refresh, native isolation or cloud/browser proof.

## Implementation handoff

One backend implementation child under #133 owns the adapters, bounded local
format/encryption/locking, existing-lifecycle composition and synthetic tests.
Do not create another epic or reopen delivered #59/#81. Remaining #133 checklist
items own authenticated resource-generation publication, native serving-profile
composition, exact artifact/policy/platform/OAuth qualification and reversible
activation. Link real child IDs in #133's execution checklist after registration;
unchecked items remain open. This design adds no implementation or rollout.

Alternatives: ambient auth adoption lacks canonical authority; memory-only state
loses revoke/CAS/cleanup on restart; plaintext candidates violate encrypted
custody; provider YDB reuse overloads another resource and risks cloud custody;
a new database/broker introduces an unnecessary owner and operational surface.
