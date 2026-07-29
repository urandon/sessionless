#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
telegram_url=${TELEGRAM_API_BASE_URL:-http://127.0.0.1:8081}
fixture="$repo_root/test/fixtures/telegram/text-message.json"

if [ ! -f "$fixture" ]; then
	printf 'Telegram fixture not found: %s\n' "$fixture" >&2
	exit 1
fi

curl --fail --silent --show-error \
	--request POST \
	--header 'Content-Type: application/json' \
	--data-binary "@$fixture" \
	"$telegram_url/test/updates" >/dev/null

printf 'Synthetic Telegram fixture loaded from %s\n' "$fixture"
