#!/bin/sh
set -eu

# Source migration is explicit, including an old variable set to an empty value.
if test "${WEB_LOGIN_PROVIDER+x}" = x; then
  printf '%s\n' 'remove WEB_LOGIN_PROVIDER; configure WEB_LOGIN_PROVIDERS explicitly' >&2
  exit 1
fi
: "${WEB_LOGIN_PROVIDERS:?set WEB_LOGIN_PROVIDERS explicitly}"
export WEB_LOGIN_PROVIDERS
case "$WEB_LOGIN_PROVIDERS" in
  yandex) : "${YANDEX_LOGIN_CLIENT_SECRET:?load the selected Yandex client secret}" ;;
  telegram) : "${TELEGRAM_OIDC_CLIENT_SECRET:?load the selected Telegram client secret}" ;;
  yandex,telegram)
    : "${YANDEX_LOGIN_CLIENT_SECRET:?load the selected Yandex client secret}"
    : "${TELEGRAM_OIDC_CLIENT_SECRET:?load the selected Telegram client secret}" ;;
  *) printf '%s\n' 'WEB_LOGIN_PROVIDERS must be yandex, telegram, or yandex,telegram' >&2; exit 1 ;;
esac
command -v yc >/dev/null 2>&1 || { printf 'yc is required\n' >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { printf 'jq is required\n' >&2; exit 1; }
: "${WEB_BFF_SECRET_ID:?set WEB_BFF_SECRET_ID from the foundation Terraform output}"
: "${SESSION_API_CURSOR_HMAC_KEY:?load SESSION_API_CURSOR_HMAC_KEY from the operator credential store}"
: "${SESSION_API_ID_HMAC_KEY:?load SESSION_API_ID_HMAC_KEY from the operator credential store}"
if test "${#SESSION_API_CURSOR_HMAC_KEY}" -lt 32 || test "${#SESSION_API_ID_HMAC_KEY}" -lt 32; then
  printf '%s\n' 'Session API HMAC keys must each contain at least 32 bytes' >&2
  exit 1
fi

# No old-key fallback: create a separately approved payload version containing
# the selected provider-specific entries. Secret bytes travel only over stdin.
jq -cn '[
  (if env.WEB_LOGIN_PROVIDERS == "yandex" or env.WEB_LOGIN_PROVIDERS == "yandex,telegram" then
    {key:"yandex-login-client-secret",text_value:env.YANDEX_LOGIN_CLIENT_SECRET} else empty end),
  (if env.WEB_LOGIN_PROVIDERS == "telegram" or env.WEB_LOGIN_PROVIDERS == "yandex,telegram" then
    {key:"telegram-oidc-client-secret",text_value:env.TELEGRAM_OIDC_CLIENT_SECRET} else empty end),
  {key:"session-cursor-hmac-key",text_value:env.SESSION_API_CURSOR_HMAC_KEY},
  {key:"session-id-hmac-key",text_value:env.SESSION_API_ID_HMAC_KEY}
]' | yc lockbox secret add-version --id "$WEB_BFF_SECRET_ID" --payload - --format json
