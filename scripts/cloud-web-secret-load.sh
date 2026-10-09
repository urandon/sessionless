#!/bin/sh
set -eu

command -v yc >/dev/null 2>&1 || { printf 'yc is required\n' >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { printf 'jq is required\n' >&2; exit 1; }
: "${WEB_BFF_SECRET_ID:?set WEB_BFF_SECRET_ID from the foundation Terraform output}"
WEB_LOGIN_PROVIDER=${WEB_LOGIN_PROVIDER:-telegram}
export WEB_LOGIN_PROVIDER
case "$WEB_LOGIN_PROVIDER" in
  telegram) : "${TELEGRAM_OIDC_CLIENT_SECRET:?load TELEGRAM_OIDC_CLIENT_SECRET from the operator credential store}" ;;
  yandex) : "${YANDEX_LOGIN_CLIENT_SECRET:?load YANDEX_LOGIN_CLIENT_SECRET from the operator credential store}" ;;
  *) printf '%s\n' 'WEB_LOGIN_PROVIDER must be telegram or yandex' >&2; exit 1 ;;
esac
: "${SESSION_API_CURSOR_HMAC_KEY:?load SESSION_API_CURSOR_HMAC_KEY from the operator credential store}"
: "${SESSION_API_ID_HMAC_KEY:?load SESSION_API_ID_HMAC_KEY from the operator credential store}"

if test "${#SESSION_API_CURSOR_HMAC_KEY}" -lt 32 || test "${#SESSION_API_ID_HMAC_KEY}" -lt 32; then
  printf '%s\n' 'Session API HMAC keys must each contain at least 32 bytes' >&2
  exit 1
fi

# The payload is streamed through stdin. Secret values never enter argv,
# Terraform state, a plan artifact, the repository, or command output. The
# legacy oidc-client-secret Lockbox key stores only the selected login secret;
# Yandex uses OAuth, not OIDC. Provider selection must match the deployment.
jq -cn \
  '(if env.WEB_LOGIN_PROVIDER == "yandex" then env.YANDEX_LOGIN_CLIENT_SECRET else env.TELEGRAM_OIDC_CLIENT_SECRET end) as $login_secret | [
    {key:"oidc-client-secret", text_value:$login_secret},
    {key:"session-cursor-hmac-key", text_value:env.SESSION_API_CURSOR_HMAC_KEY},
    {key:"session-id-hmac-key", text_value:env.SESSION_API_ID_HMAC_KEY}
  ]' | yc lockbox secret add-version --id "$WEB_BFF_SECRET_ID" --payload - --format json
