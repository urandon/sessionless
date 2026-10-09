# Private owner-worker onboarding

AW-06a [#170](https://gitcode.com/urandon/sessionless/issues/170) implements the
minimum operator-assisted enrollment path of
[#78](https://gitcode.com/urandon/sessionless/issues/78). It creates a fresh
owner-scoped identity and one exact compute-resource projection through
production APIs, without editing database rows or importing fixture identities.
It does not install/start a service, contact an OCI engine, supply provider
credentials, prove entitlement, or enable a real provider.

## Authority and prerequisites

The pilot owner must already have an authenticated user ID and active tenant
write membership. The operator obtains these IDs through the existing
membership/bootstrap process, not from an unverified identity-provider claim.
The operator uses the existing explicit `YDB_CONNECTION_STRING` and YDB
credentials against an already migrated database. The owner never receives
those credentials. `ONBOARD` confirmation is intent, not authorization:
membership is checked again inside the database mutation transaction.

The privileged operator handoff is the enrollment capability boundary specified
in [attached-worker-ux.md](attached-worker-ux.md#enrollment). Transfer grant,
claim, head and receipt files only over an authenticated private channel between
that operator and the exact owner. They are not public Web DTOs, tickets, chat
attachments, diagnostic bundles or logs. In particular, the grant and claim
contain bootstrap material. Private keys stay in the owner's pending/local
state and are never sent to the operator.

Every handoff path must be absolute and canonical (no symlinked parents), in an
existing owner-only `0700` directory. Files must be regular, owned by the current
user, `0600`, and at most 128 KiB. On macOS use the real `/private/...` path when
`/tmp` or `/var` is a symlink. The commands refuse unsafe paths, duplicate/unknown
JSON fields, trailing input and conflicting existing files; they never chmod or
overwrite unrelated caller data. Keep a separate private directory on each host.

Prepare `setup.json` on the owner host with `version: 1`, the exact canonical
HTTPS `control_plane_origin`, and the reviewed `oci`/`harness` inputs from the
existing [local manifest contract](attached-worker-packaging.md):

- `oci`: absolute `docker_path` and `cli_config_dir`, pinned `docker_sha256`,
  explicit `host`, `engine_id`, `installation_id`, supported `boundary`,
  digest-pinned `image`, non-root `user_id`/`group_id`, and explicit
  `disk_bytes`, `credential_file_bytes`, `memory_bytes`, `pids_limit`,
  `stop_seconds` limits;
- `harness`: absolute `executable`, pinned `sha256`, and `arguments`.

No identity, server generation, connection secret, provider credential or
runtime observation is accepted in this declarative setup. Validation is not
artifact/engine verification. Enrollment itself needs neither Docker nor a
running daemon; activation verifies those reviewed inputs separately.

## Fresh enrollment: four bounded steps

The following command templates use `/absolute/private/operator` and
`/absolute/private/owner` as placeholders for the two real private directories.
Replace `tenant-id`, `user-id`, origin and display name with the approved pilot
scope. No bootstrap/key bytes are command-line arguments.

1. **Operator prepares and persists one short-lived grant.** Provide exactly
   `ONBOARD enroll-create user-id INTO tenant-id` on standard input, then run:

   ```sh
   make attached-worker-admin ARGS='enroll-create --tenant tenant-id --owner user-id --display-name laptop --audience worker-v1 --origin https://control.example --ttl 5m --grant-file /absolute/private/operator/grant.json'
   ```

   The IDs and secret are saved before the first database write. A lost database
   response is retried with this same file/command, not a new enrollment. The
   grant expires; it does not grant access to other tenants or owners.

2. **Owner durably generates a key before delivering a signed claim.** Privately
   transfer `grant.json`, retain `setup.json`, then run:

   ```sh
   make attached-worker-setup ARGS='enroll-prepare --state-dir /absolute/private/owner/installation --pending-file /absolute/private/owner/enrollment-pending.json --grant-file /absolute/private/owner/grant.json --setup-file /absolute/private/owner/setup.json --request-file /absolute/private/owner/claim.json'
   ```

   Preserve the pending file. Repeating the same command returns the same key
   and claim. A changed grant/setup or partial conflicting installation is
   refused rather than silently rekeyed. Do not delete pending state to resolve
   an ambiguous remote result.

3. **Operator claims and registers the exact own resource.** Privately receive
   `claim.json`; provide `ONBOARD enroll-claim user-id INTO tenant-id`, then run:

   ```sh
   make attached-worker-admin ARGS='enroll-claim --tenant tenant-id --owner user-id --claim-file /absolute/private/operator/claim.json --receipt-file /absolute/private/operator/enrollment-receipt.json'
   ```

   The signed claim is checked against the immutable persisted grant. Resource,
   actor ownership and the owner projection are created atomically; an existing
   different resource is a conflict, never an implicit picker/rebind. A receipt
   is returned only after registration succeeds. If a response is lost, retry
   the same claim and receipt path before any activation/rotation changes the
   worker. Interrupted claim-to-registration is recoverable this way.

4. **Owner completes local setup using the exact receipt.** Transfer that receipt
   privately, then run:

   ```sh
   make attached-worker-setup ARGS='enroll-complete --state-dir /absolute/private/owner/installation --pending-file /absolute/private/owner/enrollment-pending.json --receipt-file /absolute/private/owner/enrollment-receipt.json'
   ```

   The receipt must match owner, worker, key and generation. Exact repeated
   completion is idempotent. Incomplete/corrupt/conflicting local state is
   fail-closed and must be inspected, not overwritten. This initializes private
   local state but does not stage or start a launchd/systemd/container service.

The Web compute-resource resolver now sees one exact own `codex` resource with
an empty credential reference and **entitlement unknown / quota unknown**.
Unknown is not active, exhausted, or a successful provider login. Scheduler
admission remains denied until independent eligibility and activation gates
are met. Another owner cannot discover or use this resource. Current
status/diagnostics and plan/apply drain/revoke remain the existing #82/#104/#131
surfaces; enrollment does not add another lifecycle machine.

The operator's content-free result identifies `worker_id` and `resource_id`;
the private receipt retains the exact tenant/owner/enrollment generation.
Those are the identity inputs to the existing
[`WEB_ATTACHED_RESOURCE_PINS` composition](development.md#attached-execution-composition-default-off).
Capability/policy digests and its expiring harness-binding template must still
come from independent #133 evidence. Do not synthesize them from enrollment or
turn either default-off execution/control flag on as part of this runbook.
After rotation, an old resource pin is stale and must be deliberately replaced.

## Identity rotation and lost responses

Stop the owned runtime before preparation and completion; the local runtime
lease prevents rotation while it is busy. Use new private paths for each
distinct rotation, while retaining the same paths across retries:

```sh
make attached-worker-admin ARGS='worker-head --tenant tenant-id --owner user-id --worker worker-id --receipt-file /absolute/private/operator/head.json'
make attached-worker-setup ARGS='rotate-prepare --state-dir /absolute/private/owner/installation --pending-file /absolute/private/owner/rotation-pending.json --head-file /absolute/private/owner/head.json --request-file /absolute/private/owner/rotation.json'
make attached-worker-admin ARGS='identity-rotate --tenant tenant-id --owner user-id --rotation-file /absolute/private/operator/rotation.json --receipt-file /absolute/private/operator/rotation-receipt.json'
make attached-worker-setup ARGS='rotate-complete --state-dir /absolute/private/owner/installation --pending-file /absolute/private/owner/rotation-pending.json --receipt-file /absolute/private/owner/rotation-receipt.json'
```

Transfer the head/request/receipt privately at the corresponding host boundary.
The operator commands require `ONBOARD worker-head user-id INTO tenant-id` or
`ONBOARD identity-rotate user-id INTO tenant-id` on standard input. Both old and
new keys sign the exact rotation. Lost server responses reconcile only an exact
next worker head plus its committed rotation audit; a later/revoked head is not
success. Completion retires old runtime checkpoints; the next transport attach
must pass the existing generation fences. This setup path refuses to rotate an
already activated provider resource: activation-aware rotation belongs to the
existing rollout/control boundary, not an implicit eligibility reset.

Keep private recovery material until exact completion is confirmed, then remove
it according to the operator's secret-retention policy. Revocation fences server
authority; neither enrollment nor local uninstall promises remote erasure.

## Verification

`make test` covers both commands, private-file boundaries, durable key reuse,
strict scope/key/generation checks, dual-proof rotation and lock behavior under
the race detector. `make attached-worker-joined-provider-ydb-gate` against an
already migrated test YDB also includes the three `TestAttachedWorkerOnboarding*`
scenarios: fresh production-API handoff/resource projection, membership and
resource conflicts, and rotation replay/revocation. Those scenarios create no
worker/resource rows through fixture SQL and do not start providers/services.

[#133](https://gitcode.com/urandon/sessionless/issues/133) owns real-provider
eligibility and activation; [#35](https://gitcode.com/urandon/sessionless/issues/35)
owns the cloud/browser pilot proof. Broader worker catalog/interactive controls
remain in #78; the MVP does not require a second administration framework.
