#!/bin/sh
set -eu

: "${CLOUD_API_URL:?set CLOUD_API_URL from Terraform output}"
: "${TELEGRAM_BOT_TOKEN:?load TELEGRAM_BOT_TOKEN from the credential store}"
: "${TELEGRAM_WEBHOOK_SECRET:?load TELEGRAM_WEBHOOK_SECRET from the credential store}"

webhook_tmp="$(mktemp -d "${TMPDIR:-/tmp}/sessionless-webhook.XXXXXX")"
trap 'rm -rf "$webhook_tmp"' EXIT HUP INT TERM
chmod 700 "$webhook_tmp"
printf 'url = "https://api.telegram.org/bot%s/setWebhook"\n' "$TELEGRAM_BOT_TOKEN" >"$webhook_tmp/curl.conf"
printf 'request = "POST"\n' >>"$webhook_tmp/curl.conf"
printf 'data-urlencode = "url=%s/telegram/webhook"\n' "$CLOUD_API_URL" >>"$webhook_tmp/curl.conf"
printf 'data-urlencode = "secret_token=%s"\n' "$TELEGRAM_WEBHOOK_SECRET" >>"$webhook_tmp/curl.conf"
printf 'fail\nsilent\nshow-error\n' >>"$webhook_tmp/curl.conf"
curl --config "$webhook_tmp/curl.conf" | jq -e '.ok == true' >/dev/null
printf 'Telegram webhook points at %s/telegram/webhook\n' "$CLOUD_API_URL"
