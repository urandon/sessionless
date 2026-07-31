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
keys. The wrapper is added with the cloud-dev environment root rather than to
the bootstrap root.

## Credentials

Use short-lived user/federation credentials or workload identity through the
standard Yandex provider environment chain. Object Storage backend credentials
must be injected through `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` by the
operator's credential store. Do not put credentials in `.tfvars`, backend HCL,
Terraform resources, command lines, or this repository.

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
bucket/key placeholders, and migrate bootstrap state explicitly:

```sh
terraform init -migrate-state -backend-config=/secure/path/bootstrap.backend.hcl
```

The example backend file contains no credentials. Keep the resulting backend
file outside the repository because bucket names and state keys are deployment
metadata.

## Safety

- `prevent_destroy` protects the state bucket and lock database/table in
  addition to YDB deletion protection.
- Bucket versioning and noncurrent-version retention make state recovery
  possible after an operator error.
- Public bucket access is disabled.
- YDB is capped at zero provisioned RU, a bounded on-demand RU/s ceiling, and a
  small storage ceiling.
- The lock row TTL is recovery hygiene, not permission to steal a live lock;
  the deployment wrapper uses owner and fence tokens.
