#!/bin/sh
set -eu

root_dir=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
edge_dir="$root_dir/infra/cloudflare/telegram-edge"
workflow_template="$root_dir/infra/yandex/workflows/telegram-ingress.yaml.tmpl"

: "${CLOUDFLARE_ACCOUNT_ID:?set the Cloudflare account ID}"
: "${CLOUDFLARE_API_TOKEN:?load a scoped Cloudflare API token from the operator credential store}"
: "${TELEGRAM_WEBHOOK_SECRET:?load the Telegram webhook secret from the operator credential store}"
: "${CLOUD_DEV_FOLDER_ID:?set the cloud-dev folder ID}"
: "${YANDEX_API_FQDN:?set the non-secret API Gateway FQDN}"
: "${YANDEX_GATEWAY_SERVICE_ACCOUNT_ID:?set the non-secret gateway service-account ID}"
: "${YANDEX_LOG_GROUP_ID:?set the non-secret runtime log-group ID}"
: "${YANDEX_TELEGRAM_SECRET_ID:?set the non-secret Telegram Lockbox secret ID}"

workflow_name=sessionless-dev-telegram-ingress-external
case "$YANDEX_API_FQDN" in
  *[!a-z0-9.-]*|'') printf '%s\n' 'invalid YANDEX_API_FQDN' >&2; exit 2 ;;
esac
for identifier in "$CLOUD_DEV_FOLDER_ID" "$YANDEX_GATEWAY_SERVICE_ACCOUNT_ID" "$YANDEX_LOG_GROUP_ID" "$YANDEX_TELEGRAM_SECRET_ID"; do
  case "$identifier" in
    *[!a-z0-9]*|'') printf '%s\n' 'invalid Yandex resource identifier' >&2; exit 2 ;;
  esac
done
if test -n "${YANDEX_WORKFLOW_ID:-}"; then
  case "$YANDEX_WORKFLOW_ID" in
    *[!a-z0-9]*|'') printf '%s\n' 'invalid YANDEX_WORKFLOW_ID' >&2; exit 2 ;;
  esac
fi

export CLOUDFLARE_ACCOUNT_ID CLOUDFLARE_API_TOKEN
export WRANGLER_SEND_METRICS=false
mkdir -p "$root_dir/.build/wrangler-config"
export XDG_CONFIG_HOME="$root_dir/.build/wrangler-config"
export WRANGLER_LOG_PATH="$root_dir/.build/wrangler.log"

npm --prefix "$edge_dir" ci
npm --prefix "$edge_dir" run check

umask 077
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/sessionless-telegram-edge.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM
workflow_spec="$work_dir/workflow.yaml"
secret_file="$work_dir/cloudflare-secrets.json"

sed \
  -e "s/__API_FQDN__/$YANDEX_API_FQDN/g" \
  -e "s/__TELEGRAM_SECRET_ID__/$YANDEX_TELEGRAM_SECRET_ID/g" \
  "$workflow_template" >"$workflow_spec"

if test -n "${YANDEX_WORKFLOW_ID:-}"; then
  expected="sessionless:telegram-ingress:${YANDEX_WORKFLOW_ID}"
  if test "${CONFIRM_YANDEX_WORKFLOW_UPDATE:-}" != "$expected"; then
    printf 'refusing workflow update; set CONFIRM_YANDEX_WORKFLOW_UPDATE=%s\n' "$expected" >&2
    exit 1
  fi
  workflow_identity=$(yc serverless workflow get --folder-id "$CLOUD_DEV_FOLDER_ID" --id "$YANDEX_WORKFLOW_ID" --format json |
    jq -cer '.workflow | {id, name, labels}')
  actual_id=$(printf '%s' "$workflow_identity" | jq -er '.id')
  actual_name=$(printf '%s' "$workflow_identity" | jq -er '.name')
  labels_match=$(printf '%s' "$workflow_identity" |
    jq -r '.labels."managed-by" == "sessionless" and .labels.component == "telegram-ingress" and .labels.environment == "dev"')
  if test "$actual_id" != "$YANDEX_WORKFLOW_ID" || test "$actual_name" != "$workflow_name" || test "$labels_match" != true; then
    printf '%s\n' 'refusing workflow update: immutable ID, fixed name, or ownership labels do not match' >&2
    exit 1
  fi
  yc serverless workflow update --id "$YANDEX_WORKFLOW_ID" \
    --folder-id "$CLOUD_DEV_FOLDER_ID" \
    --description 'Externally managed Telegram acknowledgement and API Gateway forwarding bridge' \
    --labels 'managed-by=sessionless,component=telegram-ingress,environment=dev' \
    --yaml-spec "$workflow_spec" \
    --service-account-id "$YANDEX_GATEWAY_SERVICE_ACCOUNT_ID" \
    --set-is-public \
    --log-group-id "$YANDEX_LOG_GROUP_ID" \
    --min-log-level info >/dev/null
  workflow_id=$YANDEX_WORKFLOW_ID
else
  workflow_list=$(yc serverless workflow list --folder-id "$CLOUD_DEV_FOLDER_ID" --format json)
  existing_workflows=$(printf '%s' "$workflow_list" |
    jq -c --arg name "$workflow_name" '[.workflows[]? | select(.name == $name) | {id, name}]')
  existing_count=$(printf '%s' "$existing_workflows" | jq -er 'length')
  if test "$existing_count" -gt 1; then
    printf '%s\n' 'refusing workflow create: fixed name resolves to multiple workflow IDs' >&2
    exit 1
  fi
  if test "$existing_count" -eq 1; then
    existing_identity=$(printf '%s' "$existing_workflows" | jq -cer '.[0]')
    existing_id=$(printf '%s' "$existing_identity" | jq -er '.id')
    printf 'refusing name-based update of existing workflow %s; verify ownership, set YANDEX_WORKFLOW_ID=%s, and provide the ID-bound confirmation\n' "$workflow_name" "$existing_id" >&2
    exit 1
  fi
  yc serverless workflow create --name "$workflow_name" \
    --folder-id "$CLOUD_DEV_FOLDER_ID" \
    --description 'Externally managed Telegram acknowledgement and API Gateway forwarding bridge' \
    --labels 'managed-by=sessionless,component=telegram-ingress,environment=dev' \
    --yaml-spec "$workflow_spec" \
    --service-account-id "$YANDEX_GATEWAY_SERVICE_ACCOUNT_ID" \
    --set-is-public \
    --log-group-id "$YANDEX_LOG_GROUP_ID" \
    --min-log-level info >/dev/null
  workflow_id=$(yc serverless workflow get --folder-id "$CLOUD_DEV_FOLDER_ID" --name "$workflow_name" --format json |
    jq -er '.workflow | select(.name == "sessionless-dev-telegram-ingress-external" and .labels."managed-by" == "sessionless" and .labels.component == "telegram-ingress" and .labels.environment == "dev") | .id')
fi

export workflow_id
YANDEX_WORKFLOW_URL=$(yc serverless workflow get --folder-id "$CLOUD_DEV_FOLDER_ID" --id "$workflow_id" --format json |
  jq -er '.workflow | select(.id == $ENV.workflow_id and .name == "sessionless-dev-telegram-ingress-external" and .labels."managed-by" == "sessionless" and .labels.component == "telegram-ingress" and .labels.environment == "dev") | .execution_url | select(type == "string" and length > 0)')
export TELEGRAM_WEBHOOK_SECRET YANDEX_WORKFLOW_URL
jq -n \
  '{TELEGRAM_WEBHOOK_SECRET: env.TELEGRAM_WEBHOOK_SECRET, YANDEX_WORKFLOW_URL: env.YANDEX_WORKFLOW_URL}' \
  >"$secret_file"
npm --prefix "$edge_dir" exec -- wrangler deploy --secrets-file "$secret_file"
unset TELEGRAM_WEBHOOK_SECRET YANDEX_WORKFLOW_URL workflow_id

printf '%s\n' 'Yandex Workflows ingress and Cloudflare Telegram edge deployed with secret bindings.'
printf '%s\n' 'Register https://dev-api-sessionless.triborg.dev/telegram/webhook with Telegram.'
