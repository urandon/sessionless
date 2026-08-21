# Terraform cloud environments

This tree is the infrastructure source for Sessionless cloud environments.
GitCode remains the source repository; reviewed Terraform plans are artifacts
of the deployment workflow and are never committed.

## State boundary

`bootstrap/` is a separately applied root module. It creates:

- a private, versioned Object Storage bucket for Terraform state;
- a deletion-protected, throttled YDB Serverless database for deployment
  leases;
- a `terraform_locks` table with TTL-based abandoned-lock cleanup.

The bootstrap root starts with local state. After its first successful apply,
migrate that state into the bucket it created. Never include `bootstrap/` in an
application-environment destroy plan.

Terraform's S3 backend lockfile remains enabled as the authoritative state-file
mutex. The YDB lease is a second, environment-aware deployment gate used by the
repository deployment wrapper; it prevents two operators from promoting
different plans for the same environment even when they use different state
keys. The `deployment-lock` wrapper is used by `scripts/cloud-terraform.sh`.

## Credentials

Use a short-lived IAM token through the standard Yandex provider environment
chain. For the Object Storage backend, issue an ephemeral access key with
`yc iam access-key issue-ephemeral` into a restricted external AWS credentials
file and select it with `AWS_SHARED_CREDENTIALS_FILE` and `AWS_PROFILE`. Do not
put credentials in `.tfvars`, backend HCL, Terraform resources, command lines,
or this repository.

## Bootstrap

```sh
cd infra/terraform/bootstrap
cp bootstrap.tfvars.example bootstrap.auto.tfvars
# Fill only non-secret resource identifiers and globally unique names.
terraform init
terraform plan -out=.terraform/bootstrap.tfplan
terraform apply .terraform/bootstrap.tfplan
```

After apply, copy `backend.hcl.example` outside the repository, replace the
bucket/key placeholders, enable the S3 backend declaration locally, and
migrate bootstrap state explicitly:

```sh
cp backend.s3.tf.example backend.s3.tf
terraform init -migrate-state -backend-config=/secure/path/bootstrap.backend.hcl
```

`backend.s3.tf` is intentionally ignored. It must not exist during the first
local-state apply because the S3 bucket does not exist yet. After migration,
keep it in the operator checkout so subsequent bootstrap plans use the remote
backend.

The example backend file contains no credentials. Keep the resulting backend
file outside the repository because bucket names and state keys are deployment
metadata.

## Safety

- `prevent_destroy` protects the state bucket and lock database/table in
  addition to YDB deletion protection.
- Bucket versioning and noncurrent-version retention make state recovery
  possible after an operator error.
- Public bucket access is disabled.
- The application artifact bucket permits pre-signed URLs only because Yandex
  disables those URLs together with static-key authentication. No persistent
  Object Storage static key is provisioned; exact-origin CORS and private IAM
  grants preserve the capability boundary. See the documented tradeoff in
  [`docs/cloud-development.md`](../../docs/cloud-development.md).
- YDB is capped at zero provisioned RU, a bounded on-demand RU/s ceiling, and a
  small storage ceiling.
- The lock row TTL is recovery hygiene, not permission to steal a live lock;
  the deployment wrapper uses owner and fence tokens.

## Cloud development root

`cloud-dev/` composes the reusable `foundation`, `runtime`, `edge`, and `web`
modules.
It creates a dedicated folder, least-privilege runtime identities, YDB
Serverless, bounded queues and DLQs, tenant-safe Object Storage, Container
Registry, KMS/Lockbox metadata, private HTTP containers, timer/YMQ triggers,
a delegated public DNS zone, managed DNS/certificate records, and blue/green
API Gateway routing. The Web module adds a digest-only, scale-to-zero BFF and a
dedicated allowlisted gateway/certificate at `web.dev.<base_domain>`; its
gateway identity cannot invoke the control containers. Only the parent-zone NS
delegation remains an external operator action.

Billing budgets and Monitoring alerts are required external guardrails because
the pinned Yandex provider does not expose those resources. The exact
folder-first bootstrap, budget verification, secret loading, saved-plan apply,
canary, rollback, and protected destroy procedures are in
[`docs/cloud-development.md`](../../docs/cloud-development.md).

`make terraform-ci` formats and validates both roots, runs a credential-free
mocked plan for the Web deployment, and enforces the digest, scale-to-zero,
gateway-isolation, secret-boundary, route-allowlist, and 100 RUB monthly budget
policies before a branch can merge.
