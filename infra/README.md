# Infrastructure

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
