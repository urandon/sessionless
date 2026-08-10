# Infrastructure

Cloud infrastructure lives under `infra/terraform`. The bootstrap stack is
deliberately separate from destroyable application environments: it owns only
the versioned Object Storage state bucket and the dedicated YDB deployment-lock
database/table. Application environments consume those outputs but cannot
destroy them.

See [terraform/README.md](terraform/README.md) for the bootstrap, plan, apply,
and recovery procedures. Local emulators remain under `infra/local`.

This directory contains the Yandex Cloud Terraform modules and the minimal
Cloudflare Telegram reachability edge. Infrastructure changes must preserve the
control-plane contracts, least-privilege IAM boundaries, and scale-to-zero
deployment model documented by the implementation issues.

Rules for future changes:

- use remote state and locking appropriate for Yandex Cloud;
- keep `dev`, `stage`, and `prod` inputs separate;
- inject credentials from CI workload identity or the operator environment;
- never commit service-account keys, Terraform state, or generated plans;
- preserve YDB as the operational database to avoid a cross-cloud dependency.

`infra/cloudflare/telegram-edge` contains no operational state or business
logic. Pinned Wrangler deploys it with two secret bindings supplied from the
operator environment; neither binding is managed by Terraform.

The pinned Terraform and `yc` versions live in `tools/versions.env`.
