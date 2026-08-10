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

YANDEX_WORKFLOW_NAME=${YANDEX_WORKFLOW_NAME:-sessionless-dev-telegram-ingress-external}
case "$YANDEX_API_FQDN" in
  *[!a-z0-9.-]*|'') printf '%s\n' 'invalid YANDEX_API_FQDN' >&2; exit 2 ;;
esac
for identifier in "$CLOUD_DEV_FOLDER_ID" "$YANDEX_GATEWAY_SERVICE_ACCOUNT_ID" "$YANDEX_LOG_GROUP_ID" "$YANDEX_TELEGRAM_SECRET_ID"; do
  case "$identifier" in
    *[!a-z0-9]*|'') printf '%s\n' 'invalid Yandex resource identifier' >&2; exit 2 ;;
  esac
done
case "$YANDEX_WORKFLOW_NAME" in
  *[!a-z0-9-]*|'') printf '%s\n' 'invalid YANDEX_WORKFLOW_NAME' >&2; exit 2 ;;
esac

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

if yc serverless workflow get --folder-id "$CLOUD_DEV_FOLDER_ID" --name "$YANDEX_WORKFLOW_NAME" >/dev/null 2>&1; then
  yc serverless workflow update --name "$YANDEX_WORKFLOW_NAME" \
    --folder-id "$CLOUD_DEV_FOLDER_ID" \
    --description 'Externally managed Telegram acknowledgement and API Gateway forwarding bridge' \
    --yaml-spec "$workflow_spec" \
    --service-account-id "$YANDEX_GATEWAY_SERVICE_ACCOUNT_ID" \
    --set-is-public \
    --log-group-id "$YANDEX_LOG_GROUP_ID" \
    --min-log-level info >/dev/null
else
  yc serverless workflow create --name "$YANDEX_WORKFLOW_NAME" \
    --folder-id "$CLOUD_DEV_FOLDER_ID" \
    --description 'Externally managed Telegram acknowledgement and API Gateway forwarding bridge' \
    --yaml-spec "$workflow_spec" \
    --service-account-id "$YANDEX_GATEWAY_SERVICE_ACCOUNT_ID" \
    --set-is-public \
    --log-group-id "$YANDEX_LOG_GROUP_ID" \
    --min-log-level info >/dev/null
fi

YANDEX_WORKFLOW_URL=$(yc serverless workflow get --folder-id "$CLOUD_DEV_FOLDER_ID" --name "$YANDEX_WORKFLOW_NAME" --format json |
  jq -er '.workflow.execution_url | select(type == "string" and length > 0)')
export TELEGRAM_WEBHOOK_SECRET YANDEX_WORKFLOW_URL
jq -n \
  '{TELEGRAM_WEBHOOK_SECRET: env.TELEGRAM_WEBHOOK_SECRET, YANDEX_WORKFLOW_URL: env.YANDEX_WORKFLOW_URL}' \
  >"$secret_file"
npm --prefix "$edge_dir" exec -- wrangler deploy --secrets-file "$secret_file"
unset TELEGRAM_WEBHOOK_SECRET YANDEX_WORKFLOW_URL

printf '%s\n' 'Yandex Workflows ingress and Cloudflare Telegram edge deployed with secret bindings.'
printf '%s\n' 'Register https://dev-api-sessionless.triborg.dev/telegram/webhook with Telegram.'
