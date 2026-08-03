#!/bin/sh
set -eu

: "${TELEGRAM_EDGE_URL:?set TELEGRAM_EDGE_URL to the verified external edge from issue #17; never use the Yandex Terraform api_url}"
: "${TELEGRAM_BOT_TOKEN:?load TELEGRAM_BOT_TOKEN from the credential store}"
: "${TELEGRAM_WEBHOOK_SECRET:?load TELEGRAM_WEBHOOK_SECRET from the credential store}"

if [ "${CONFIRM_EXTERNAL_TELEGRAM_EDGE:-}" != "sessionless-external-edge" ]; then
	printf '%s\n' 'refusing webhook registration without CONFIRM_EXTERNAL_TELEGRAM_EDGE=sessionless-external-edge' >&2
	exit 2
fi

case "$TELEGRAM_EDGE_URL" in
https://*) ;;
*)
	printf '%s\n' 'TELEGRAM_EDGE_URL must be an https URL' >&2
	exit 2
	;;
esac

telegram_edge_url=${TELEGRAM_EDGE_URL%/}

webhook_tmp="$(mktemp -d "${TMPDIR:-/tmp}/sessionless-webhook.XXXXXX")"
trap 'rm -rf "$webhook_tmp"' EXIT HUP INT TERM
chmod 700 "$webhook_tmp"
printf 'url = "https://api.telegram.org/bot%s/setWebhook"\n' "$TELEGRAM_BOT_TOKEN" >"$webhook_tmp/curl.conf"
printf 'request = "POST"\n' >>"$webhook_tmp/curl.conf"
printf 'data-urlencode = "url=%s/telegram/webhook"\n' "$telegram_edge_url" >>"$webhook_tmp/curl.conf"
printf 'data-urlencode = "secret_token=%s"\n' "$TELEGRAM_WEBHOOK_SECRET" >>"$webhook_tmp/curl.conf"
printf 'fail\nsilent\nshow-error\n' >>"$webhook_tmp/curl.conf"
curl --config "$webhook_tmp/curl.conf" | jq -e '.ok == true' >/dev/null
printf 'Telegram webhook points at %s/telegram/webhook\n' "$telegram_edge_url"
