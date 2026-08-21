# Deployment-aware Container Registry cleanup

Registry cleanup is a GitHub-hosted operational job. It does not create a
Yandex timer, function, workflow, container, or other permanent compute
resource. The weekly schedule is always a dry-run. A live run is possible only
through `workflow_dispatch`, with mode `delete` and the exact confirmation
`cloud-dev:<registry-id>`.

## Safety model

The cleanup computes and validates the complete decision set for all five
managed repositories before its first mutation:

- `control-api`;
- `web-bff`;
- `reconciler`;
- `telegram-sender`;
- `worker-runtime`.

It retains active stable and candidate revisions, the other managed active
revisions, the three newest distinct successful deployment manifests, images
inside the 48-hour safety window, and optional explicitly protected rollback
digests. Live revision digests are authoritative and are cross-checked against
the reviewed Terraform inventory. Tags and OCI labels are diagnostic evidence,
not an authority for deletion.

Evidence collection and deletion run inside the same fenced YDB deployment
lock used by `scripts/cloud-terraform.sh`. GitHub concurrency only prevents two
copies of this workflow from overlapping; it is not the deployment race
control. An incomplete repository page, missing manifest, live/Terraform
disagreement, invalid field, or unavailable lock blocks the whole plan before
deletion.

The Yandex API deletes an immutable image resource ID. Immediately before a
live mutation, the cleanup rechecks that the ID still names the planned
repository and digest. A repeated run treats an already absent planned image as
idempotent. A later deletion failure is reported as a partial failure; it is
never rewritten as a zero-deletion run.

## Terraform inventory bridge

The GitHub runner deliberately does not receive a Terraform backend key. An
operator with already-approved backend access exports a canonical, non-secret
projection from the selected remote state:

```sh
export CLOUD_DEV_BACKEND_CONFIG=/secure/path/cloud-dev.backend.hcl
terraform -chdir=infra/terraform/cloud-dev init \
  -backend-config="$CLOUD_DEV_BACKEND_CONFIG" -input=false
./scripts/cloud-registry-gc-inventory.sh \
  >/secure/path/cloud-dev-registry-gc-inventory.json
```

Review the output and store its complete JSON value in the protected GitHub
repository variable `YANDEX_REGISTRY_GC_INVENTORY_JSON`. The envelope contains
the state lineage and serial plus a SHA-256 digest of the canonical
`registry_gc_inventory` Terraform output. `scripts/registry-gc.sh` recomputes
that digest and fails closed if the variable is malformed or changed. Refresh
the variable after every applied change to managed container revisions,
repositories, slots, or lifecycle policies. A stale bridge does not fall back
to discovery by names: its disagreement with live revisions blocks deletion.

Optional durable rollback protection is a reviewed JSON value in
`YANDEX_REGISTRY_GC_PROTECTED_DIGESTS_JSON`:

```json
{
  "schema_version": 1,
  "environment": "cloud-dev",
  "registry_id": "replace-registry-id",
  "digests": {
    "control-api": ["sha256:replace-with-64-lowercase-hex"]
  }
}
```

The map may omit repositories and may only add protection. Unknown repository
names, malformed digests, or a registry mismatch block the run. For a local
operator run, `REGISTRY_GC_PROTECTED_DIGESTS_FILE` accepts the same schema;
setting both file and JSON sources is rejected.

## Workload identity and permissions

The workflow exchanges its GitHub OIDC token for a short-lived Yandex IAM token
with `scripts/github-yandex-token.mjs`. It verifies issuer, audience, exact
repository, exact `main` ref, and the Terraform-bound subject before writing
the masked access token to `GITHUB_ENV`. No Docker login or static cloud key is
created.

Configure these protected GitHub variables/secrets after the infrastructure is
applied:

- `YANDEX_OIDC_AUDIENCE`;
- `YANDEX_REGISTRY_GC_GITHUB_OIDC_SUBJECT`;
- `YANDEX_REGISTRY_GC_SERVICE_ACCOUNT_ID`;
- `YANDEX_REGISTRY_GC_INVENTORY_JSON`;
- optional `YANDEX_REGISTRY_GC_PROTECTED_DIGESTS_JSON`;
- secret `TERRAFORM_LOCK_YDB_CONNECTION_STRING`.

The lock database belongs to the separate bootstrap Terraform root. Provision
it first without a cleaner ID, apply the cloud-dev foundation to create the
cleaner account, then review and apply a bootstrap plan with
`registry_cleaner_service_account_id` set to the cloud-dev output
`registry_cleaner_service_account_id`. This second bootstrap apply adds only a
database-scoped `ydb.editor` binding on the deployment-lock database. Do not
replace it with a folder-scoped YDB role.

The cleanup identity is separate from the publisher and its registry role is
bound independently to each managed repository, never to the registry or
folder. Yandex Container Registry currently requires the same
`container-registry.images.pusher` role for image deletion and image upload;
Yandex IAM does not support user-defined roles. Consequently IAM cannot express
a delete-without-push identity. The workflow contains no push or tag operation,
and repository-scoped bindings are the narrowest available platform boundary.

The identity also has `serverless-containers.auditor` on each exact managed
container, which exposes revision metadata without revision environment
variables. Until the cross-root YDB binding and all protected GitHub values are
configured, the job is expected to fail closed and upload a blocked report.

## Deployment-manifest evidence

The workflow uses its repository-scoped GitHub token to enumerate successful
`main` CI runs and every artifact page needed to locate exactly one
`deployment-images-<sha>` artifact per selected run. It keeps the newest three
distinct source SHAs. Every downloaded manifest must be schema version 2,
contain exactly the five repositories, and use references from the inventory's
exact registry. Fewer than three valid, unexpired manifests block deletion;
there is no age-only fallback.

## Reports and operation

Every workflow first creates a sanitized blocked JSON/Markdown report. The GC
command replaces it only after producing a validated result. Both files are
uploaded with `if: always()` and retained for 90 days. The JSON report is the
machine authority; the Markdown summary is derived from it. Upstream response
bodies, OIDC tokens, IAM tokens, and Terraform backend material must never be
included.

Run the weekly-equivalent mode manually:

```sh
gh workflow run registry-gc.yml --repo urandon/sessionless \
  --ref main -f mode=dry-run
```

After reviewing a complete dry-run report, start a live run with the registry
ID from the same reviewed inventory:

```sh
gh workflow run registry-gc.yml --repo urandon/sessionless --ref main \
  -f mode=delete -f confirmation='cloud-dev:replace-registry-id'
```

Never retry by bypassing the inventory, manifest, lock, or confirmation gates.
Correct the missing evidence and create a new auditable run.

## Native lifecycle policy

The Terraform-managed native policy remains disabled. Its retained-count and
age rule may be inspected through a Yandex lifecycle dry-run, but it must not be
activated: native Container Registry lifecycle processing has no knowledge of
live revisions, rollback protection, deployment manifests, or the shared YDB
lock. An active age/tag rule could independently delete a protected old digest
and invalidate the deployment-aware safety proof.
