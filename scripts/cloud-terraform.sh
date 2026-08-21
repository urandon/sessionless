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
    : "${LEGACY_TELEGRAM_WORKFLOW_ID:?set the exact legacy workflow ID after resolving it from state and Yandex Cloud}"
    case "$LEGACY_TELEGRAM_WORKFLOW_ID" in
      *[!a-z0-9]*|'') printf '%s\n' 'invalid LEGACY_TELEGRAM_WORKFLOW_ID' >&2; exit 2 ;;
    esac
    expected="sessionless-dev:telegram-ingress:${LEGACY_TELEGRAM_WORKFLOW_ID}"
    if test "${CONFIRM_WORKFLOW_STATE_RELEASE:-}" != "$expected"; then
      printf 'refusing workflow state release; set CONFIRM_WORKFLOW_STATE_RELEASE=%s\n' "$expected" >&2
      exit 1
    fi
    ./scripts/cloud-preflight.sh
    state_identity=$(terraform -chdir=infra/terraform/cloud-dev show -json |
      jq -cer '.. | objects | select(.address? == "module.edge.yandex_serverless_workflow.telegram_ingress") | .values | {id, name}')
    state_id=$(printf '%s' "$state_identity" | jq -er '.id')
    state_name=$(printf '%s' "$state_identity" | jq -er '.name')
    if test "$state_id" != "$LEGACY_TELEGRAM_WORKFLOW_ID" || test "$state_name" != 'sessionless-dev-telegram-ingress'; then
      printf '%s\n' 'refusing workflow state release: remote state identity does not match the expected legacy workflow' >&2
      exit 1
    fi
    live_identity=$(yc serverless workflow get --folder-id "$CLOUD_DEV_FOLDER_ID" --id "$LEGACY_TELEGRAM_WORKFLOW_ID" --format json |
      jq -cer '.workflow | {id, name}')
    live_id=$(printf '%s' "$live_identity" | jq -er '.id')
    live_name=$(printf '%s' "$live_identity" | jq -er '.name')
    if test "$live_id" != "$state_id" || test "$live_name" != "$state_name"; then
      printf '%s\n' 'refusing workflow state release: live Yandex identity does not match remote state' >&2
      exit 1
    fi
    exec go run ./cmd/deployment-lock with -- \
      terraform -chdir=infra/terraform/cloud-dev state rm \
      'module.edge.yandex_serverless_workflow.telegram_ingress'
    ;;
  plan)
    ./scripts/cloud-preflight.sh
    load_ymq_provider_credentials
    if test -n "${CLOUD_DEV_IMAGE_TFVARS:-}"; then
      test -f "$CLOUD_DEV_IMAGE_TFVARS" || {
        printf 'CLOUD_DEV_IMAGE_TFVARS does not exist: %s\n' "$CLOUD_DEV_IMAGE_TFVARS" >&2
        exit 1
      }
      set -- "-var-file=$CLOUD_DEV_IMAGE_TFVARS" "$@"
    else
      foundation_only=0
      for argument in "$@"; do
        if test "$argument" = "-target=module.foundation"; then
          foundation_only=1
        fi
      done
      if test "$foundation_only" -ne 1; then
        printf '%s\n' 'CLOUD_DEV_IMAGE_TFVARS is required for every non-foundation plan' >&2
        exit 1
      fi
    fi
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
