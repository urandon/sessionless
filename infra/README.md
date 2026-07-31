# Infrastructure

Cloud infrastructure lives under `infra/terraform`. The bootstrap stack is
deliberately separate from destroyable application environments: it owns only
the versioned Object Storage state bucket and the dedicated YDB deployment-lock
database/table. Application environments consume those outputs but cannot
destroy them.

See [terraform/README.md](terraform/README.md) for the bootstrap, plan, apply,
and recovery procedures. Local emulators remain under `infra/local`.

This directory is the boundary for Yandex Cloud Terraform modules and
environment roots. Infrastructure code is intentionally not invented in the
repository-foundation issue: the deployment issues will add modules only after
the control-plane contracts and IAM boundaries exist.

Rules for future changes:

- use remote state and locking appropriate for Yandex Cloud;
- keep `dev`, `stage`, and `prod` inputs separate;
- inject credentials from CI workload identity or the operator environment;
- never commit service-account keys, Terraform state, or generated plans;
- preserve YDB as the operational database to avoid a cross-cloud dependency.

The pinned Terraform and `yc` versions live in `tools/versions.env`.
