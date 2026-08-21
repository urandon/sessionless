#!/bin/sh
set -eu

command -v yc >/dev/null 2>&1 || { printf 'yc is required\n' >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { printf 'jq is required\n' >&2; exit 1; }
: "${WEB_BFF_SECRET_ID:?set WEB_BFF_SECRET_ID from the foundation Terraform output}"
: "${TELEGRAM_OIDC_CLIENT_SECRET:?load TELEGRAM_OIDC_CLIENT_SECRET from the operator credential store}"
: "${SESSION_API_CURSOR_HMAC_KEY:?load SESSION_API_CURSOR_HMAC_KEY from the operator credential store}"
: "${SESSION_API_ID_HMAC_KEY:?load SESSION_API_ID_HMAC_KEY from the operator credential store}"

if test "${#SESSION_API_CURSOR_HMAC_KEY}" -lt 32 || test "${#SESSION_API_ID_HMAC_KEY}" -lt 32; then
  printf '%s\n' 'Session API HMAC keys must each contain at least 32 bytes' >&2
  exit 1
fi

# The payload is streamed through stdin. Secret values never enter argv,
# Terraform state, a plan artifact, the repository, or command output.
jq -cn \
  --arg oidc "$TELEGRAM_OIDC_CLIENT_SECRET" \
  --arg cursor "$SESSION_API_CURSOR_HMAC_KEY" \
  --arg identity "$SESSION_API_ID_HMAC_KEY" \
  '[
    {key:"oidc-client-secret", text_value:$oidc},
    {key:"session-cursor-hmac-key", text_value:$cursor},
    {key:"session-id-hmac-key", text_value:$identity}
  ]' | yc lockbox secret add-version --id "$WEB_BFF_SECRET_ID" --payload - --format json
