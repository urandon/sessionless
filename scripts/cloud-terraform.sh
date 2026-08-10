#!/bin/sh
set -eu

: "${CLOUD_DEV_BACKEND_CONFIG:?set CLOUD_DEV_BACKEND_CONFIG}"
: "${CLOUD_DEV_TFVARS:?set CLOUD_DEV_TFVARS}"

action="${1:-plan}"
shift || true

load_ymq_provider_credentials() {
  if test -n "${YC_MESSAGE_QUEUE_ACCESS_KEY:-}" || test -n "${YC_MESSAGE_QUEUE_SECRET_KEY:-}"; then
    if test -z "${YC_MESSAGE_QUEUE_ACCESS_KEY:-}" || test -z "${YC_MESSAGE_QUEUE_SECRET_KEY:-}"; then
      printf 'both YC_MESSAGE_QUEUE_ACCESS_KEY and YC_MESSAGE_QUEUE_SECRET_KEY must be set together\n' >&2
      exit 1
    fi
    return
  fi

  secret_id="$(terraform -chdir=infra/terraform/cloud-dev output -raw queue_provisioner_secret_id)"
  version_id="$(terraform -chdir=infra/terraform/cloud-dev output -raw queue_provisioner_secret_version_id)"
  if test -z "$secret_id" || test -z "$version_id"; then
    printf 'YMQ provisioning credentials are not bootstrapped; run queue-auth-bootstrap first\n' >&2
    exit 1
  fi

  payload="$(yc lockbox payload get --id "$secret_id" --version-id "$version_id" --format json)"
  YC_MESSAGE_QUEUE_ACCESS_KEY="$(printf '%s' "$payload" | jq -er '.entries[] | select(.key == "access-key") | .text_value')"
  YC_MESSAGE_QUEUE_SECRET_KEY="$(printf '%s' "$payload" | jq -er '.entries[] | select(.key == "secret-key") | .text_value')"
  export YC_MESSAGE_QUEUE_ACCESS_KEY YC_MESSAGE_QUEUE_SECRET_KEY
  unset payload secret_id version_id
}

if test "$action" = output; then
  exec terraform -chdir=infra/terraform/cloud-dev output "$@"
fi
: "${TERRAFORM_LOCK_YDB_CONNECTION_STRING:?set the bootstrap lock database connection string}"
if test -z "${YDB_ACCESS_TOKEN_CREDENTIALS:-}" && test -n "${YC_TOKEN:-}"; then
  YDB_ACCESS_TOKEN_CREDENTIALS="$YC_TOKEN"
  export YDB_ACCESS_TOKEN_CREDENTIALS
fi
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
      -input=false -auto-approve \
      -var-file="$CLOUD_DEV_TFVARS" \
      -target=module.foundation.yandex_resourcemanager_folder.environment
    ;;
  queue-auth-bootstrap)
    expected="sessionless-cloud-dev:queue-auth"
    if test "${CONFIRM_QUEUE_AUTH_BOOTSTRAP:-}" != "$expected"; then
      printf 'refusing YMQ credential bootstrap; set CONFIRM_QUEUE_AUTH_BOOTSTRAP=%s\n' "$expected" >&2
      exit 1
    fi
    ./scripts/cloud-preflight.sh
    exec go run ./cmd/deployment-lock with -- \
      terraform -chdir=infra/terraform/cloud-dev apply \
      -input=false -auto-approve \
      -var-file="$CLOUD_DEV_TFVARS" \
      -target=module.foundation.yandex_iam_service_account_static_access_key.queue_provisioner \
      -target=module.foundation.yandex_iam_service_account_static_access_key.scheduler_ymq
    ;;
  workflow-state-release)
    expected="sessionless-dev:telegram-ingress"
    if test "${CONFIRM_WORKFLOW_STATE_RELEASE:-}" != "$expected"; then
      printf 'refusing workflow state release; set CONFIRM_WORKFLOW_STATE_RELEASE=%s\n' "$expected" >&2
      exit 1
    fi
    ./scripts/cloud-preflight.sh
    exec go run ./cmd/deployment-lock with -- \
      terraform -chdir=infra/terraform/cloud-dev state rm \
      'module.edge.yandex_serverless_workflow.telegram_ingress'
    ;;
  plan)
    ./scripts/cloud-preflight.sh
    load_ymq_provider_credentials
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
    load_ymq_provider_credentials
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
    load_ymq_provider_credentials
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
    load_ymq_provider_credentials
    exec go run ./cmd/deployment-lock with -- \
      terraform -chdir=infra/terraform/cloud-dev apply "$1"
    ;;
  *)
    printf 'unsupported cloud Terraform action: %s\n' "$action" >&2
    exit 2
    ;;
esac
