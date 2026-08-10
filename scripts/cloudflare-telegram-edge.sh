#!/bin/sh
set -eu

root_dir=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
edge_dir="$root_dir/infra/cloudflare/telegram-edge"

: "${CLOUDFLARE_ACCOUNT_ID:?set the Cloudflare account ID}"
: "${CLOUDFLARE_API_TOKEN:?load a scoped Cloudflare API token from the operator credential store}"
: "${TELEGRAM_WEBHOOK_SECRET:?load the Telegram webhook secret from the operator credential store}"
: "${YANDEX_WORKFLOW_URL:?load the sensitive telegram_workflow_execution_url Terraform output}"

export CLOUDFLARE_ACCOUNT_ID CLOUDFLARE_API_TOKEN
export WRANGLER_SEND_METRICS=false
mkdir -p "$root_dir/.build/wrangler-config"
export XDG_CONFIG_HOME="$root_dir/.build/wrangler-config"
export WRANGLER_LOG_PATH="$root_dir/.build/wrangler.log"

npm --prefix "$edge_dir" ci
npm --prefix "$edge_dir" run check

umask 077
secret_file=$(mktemp "${TMPDIR:-/tmp}/sessionless-cloudflare-secrets.XXXXXX")
trap 'rm -f "$secret_file"' EXIT HUP INT TERM
jq -n \
  --arg telegram "$TELEGRAM_WEBHOOK_SECRET" \
  --arg workflow "$YANDEX_WORKFLOW_URL" \
  '{TELEGRAM_WEBHOOK_SECRET: $telegram, YANDEX_WORKFLOW_URL: $workflow}' >"$secret_file"
npm --prefix "$edge_dir" exec -- wrangler deploy --secrets-file "$secret_file"

printf '%s\n' 'Cloudflare Telegram edge deployed with secret bindings.'
printf '%s\n' 'Register https://dev-api-sessionless.triborg.dev/telegram/webhook with Telegram.'
