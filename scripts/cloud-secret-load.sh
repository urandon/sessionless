#!/bin/sh
set -eu

command -v yc >/dev/null 2>&1 || { printf 'yc is required\n' >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { printf 'jq is required\n' >&2; exit 1; }
: "${TELEGRAM_SECRET_ID:?set TELEGRAM_SECRET_ID from the foundation Terraform output}"
: "${TELEGRAM_BOT_TOKEN:?load TELEGRAM_BOT_TOKEN from the operator credential store}"
: "${TELEGRAM_WEBHOOK_SECRET:?load TELEGRAM_WEBHOOK_SECRET from the operator credential store}"
: "${TELEGRAM_IDENTITY_HMAC_KEY:?load TELEGRAM_IDENTITY_HMAC_KEY from the operator credential store}"

# The payload is streamed through stdin. Secret values never enter argv,
# Terraform state, a plan artifact, the repository, or the command output.
jq -cn \
  --arg bot "$TELEGRAM_BOT_TOKEN" \
  --arg webhook "$TELEGRAM_WEBHOOK_SECRET" \
  --arg identity "$TELEGRAM_IDENTITY_HMAC_KEY" \
  '[
    {key:"bot-token", text_value:$bot},
    {key:"webhook-secret", text_value:$webhook},
    {key:"identity-hmac-key", text_value:$identity}
  ]' | yc lockbox secret add-version --id "$TELEGRAM_SECRET_ID" --payload - --format json
