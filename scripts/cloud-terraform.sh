#!/bin/sh
set -eu

: "${CLOUD_DEV_BACKEND_CONFIG:?set CLOUD_DEV_BACKEND_CONFIG}"
: "${CLOUD_DEV_TFVARS:?set CLOUD_DEV_TFVARS}"

action="${1:-plan}"
shift || true
if test "$action" = output; then
  exec terraform -chdir=infra/terraform/cloud-dev output "$@"
fi
: "${TERRAFORM_LOCK_YDB_CONNECTION_STRING:?set the bootstrap lock database connection string}"
case "$action" in
  folder-bootstrap)
    expected="sessionless-cloud-dev:folder"
    if test "${CONFIRM_FOLDER_BOOTSTRAP:-}" != "$expected"; then
      printf 'refusing folder bootstrap; set CONFIRM_FOLDER_BOOTSTRAP=%s\n' "$expected" >&2
      exit 1
    fi
    terraform -chdir=infra/terraform/cloud-dev init \
      -backend-config="$CLOUD_DEV_BACKEND_CONFIG" -input=false
    exec go run ./cmd/deployment-lock with -- \
      terraform -chdir=infra/terraform/cloud-dev apply \
      -var-file="$CLOUD_DEV_TFVARS" \
      -target=module.foundation.yandex_resourcemanager_folder.environment
    ;;
  plan)
    ./scripts/cloud-preflight.sh
    exec go run ./cmd/deployment-lock with -- \
      terraform -chdir=infra/terraform/cloud-dev plan \
      -var-file="$CLOUD_DEV_TFVARS" "$@"
    ;;
  apply)
    test "$#" -eq 1 || {
      printf 'apply requires exactly one reviewed saved-plan path\n' >&2
      exit 2
    }
    ./scripts/cloud-preflight.sh
    exec go run ./cmd/deployment-lock with -- \
      terraform -chdir=infra/terraform/cloud-dev apply "$1"
    ;;
  destroy-plan)
    expected="sessionless-cloud-dev:${CLOUD_DEV_FOLDER_ID:-unset}"
    if test "${CONFIRM_CLOUD_DESTROY:-}" != "$expected"; then
      printf 'refusing destroy; set CONFIRM_CLOUD_DESTROY=%s after reviewing the saved plan\n' "$expected" >&2
      exit 1
    fi
    ./scripts/cloud-preflight.sh
    exec go run ./cmd/deployment-lock with -- \
      terraform -chdir=infra/terraform/cloud-dev plan -destroy \
      -var-file="$CLOUD_DEV_TFVARS" "$@"
    ;;
  destroy-apply)
    expected="sessionless-cloud-dev:${CLOUD_DEV_FOLDER_ID:-unset}"
    if test "${CONFIRM_CLOUD_DESTROY:-}" != "$expected" || test "$#" -ne 1; then
      printf 'destroy-apply requires exact confirmation and one reviewed saved-plan path\n' >&2
      exit 1
    fi
    ./scripts/cloud-preflight.sh
    exec go run ./cmd/deployment-lock with -- \
      terraform -chdir=infra/terraform/cloud-dev apply "$1"
    ;;
  *)
    printf 'unsupported cloud Terraform action: %s\n' "$action" >&2
    exit 2
    ;;
esac
