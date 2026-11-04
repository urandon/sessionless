#!/bin/sh
set -eu
repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
test_root=$(mktemp -d "${TMPDIR:-/tmp}/sessionless-web-secret-load.XXXXXX")
trap 'rm -rf "$test_root"' EXIT HUP INT TERM
fake_bin="$test_root/bin"
mkdir -p "$fake_bin"
cat >"$fake_bin/jq" <<'EOF'
#!/bin/sh
set -eu
for argument in "$@"; do
 case "$argument" in
 *"$EXPECTED_YANDEX_SECRET"*|*"$EXPECTED_TELEGRAM_SECRET"*|*"$EXPECTED_CURSOR_KEY"*|*"$EXPECTED_ID_KEY"*) printf '%s\n' 'secret exposed in jq argv' >&2; exit 2 ;;
 esac
done
test "$1" = '-cn'
case "$2" in *'key:"yandex-login-client-secret"'*) ;; *) exit 2 ;; esac
case "$2" in *'key:"telegram-oidc-client-secret"'*) ;; *) exit 2 ;; esac
case "$WEB_LOGIN_PROVIDERS" in
 yandex) test "$YANDEX_LOGIN_CLIENT_SECRET" = "$EXPECTED_YANDEX_SECRET" ;;
 telegram) test "$TELEGRAM_OIDC_CLIENT_SECRET" = "$EXPECTED_TELEGRAM_SECRET" ;;
 yandex,telegram) test "$YANDEX_LOGIN_CLIENT_SECRET" = "$EXPECTED_YANDEX_SECRET"; test "$TELEGRAM_OIDC_CLIENT_SECRET" = "$EXPECTED_TELEGRAM_SECRET" ;;
 *) exit 2 ;;
esac
test "$SESSION_API_CURSOR_HMAC_KEY" = "$EXPECTED_CURSOR_KEY"
test "$SESSION_API_ID_HMAC_KEY" = "$EXPECTED_ID_KEY"
if test -n "${REAL_JQ:-}"; then exec "$REAL_JQ" "$@"; fi
printf '%s\n' payload-from-environment
EOF
cat >"$fake_bin/yc" <<'EOF'
#!/bin/sh
set -eu
test "$*" = 'lockbox secret add-version --id web-secret-id --payload - --format json'
printf '%s\n' called >>"$TEST_YC_CALLS"
if test -n "${REAL_JQ:-}"; then
 "$REAL_JQ" -e '
 . == [
  (if env.WEB_LOGIN_PROVIDERS == "yandex" or env.WEB_LOGIN_PROVIDERS == "yandex,telegram" then {key:"yandex-login-client-secret",text_value:env.EXPECTED_YANDEX_SECRET} else empty end),
  (if env.WEB_LOGIN_PROVIDERS == "telegram" or env.WEB_LOGIN_PROVIDERS == "yandex,telegram" then {key:"telegram-oidc-client-secret",text_value:env.EXPECTED_TELEGRAM_SECRET} else empty end),
  {key:"session-cursor-hmac-key",text_value:env.EXPECTED_CURSOR_KEY},
  {key:"session-id-hmac-key",text_value:env.EXPECTED_ID_KEY}
 ]' >/dev/null
else
 test "$(cat)" = payload-from-environment
fi
printf '%s\n' '{}'
EOF
chmod 755 "$fake_bin/jq" "$fake_bin/yc"
export REAL_JQ="$(command -v jq || true)"
export TEST_YC_CALLS="$test_root/yc-calls"
export PATH="$fake_bin:$PATH"
export WEB_BFF_SECRET_ID=web-secret-id
export EXPECTED_YANDEX_SECRET=review-yandex-secret-marker
export EXPECTED_TELEGRAM_SECRET=review-telegram-secret-marker
export EXPECTED_CURSOR_KEY=review-cursor-key-marker-1234567890
export EXPECTED_ID_KEY=review-identity-key-marker-123456789
export SESSION_API_CURSOR_HMAC_KEY="$EXPECTED_CURSOR_KEY" SESSION_API_ID_HMAC_KEY="$EXPECTED_ID_KEY"
unset WEB_LOGIN_PROVIDER
for mode in yandex telegram yandex,telegram; do
 export WEB_LOGIN_PROVIDERS="$mode"
 export YANDEX_LOGIN_CLIENT_SECRET="$EXPECTED_YANDEX_SECRET" TELEGRAM_OIDC_CLIENT_SECRET="$EXPECTED_TELEGRAM_SECRET"
 "$repo_root/scripts/cloud-web-secret-load.sh" >"$test_root/out" 2>"$test_root/err"
 if grep -F -e "$EXPECTED_YANDEX_SECRET" -e "$EXPECTED_TELEGRAM_SECRET" -e "$EXPECTED_CURSOR_KEY" -e "$EXPECTED_ID_KEY" "$test_root/out" "$test_root/err" >/dev/null; then
  printf '%s\n' 'loader exposed secret bytes' >&2; exit 1
 fi
done
test "$(wc -l <"$TEST_YC_CALLS" | tr -d ' ')" = 3
deny_without_write() {
 if "$repo_root/scripts/cloud-web-secret-load.sh" >"$test_root/out" 2>"$test_root/err"; then
  printf '%s\n' 'invalid login configuration accepted' >&2; exit 1
 fi
 test "$(wc -l <"$TEST_YC_CALLS" | tr -d ' ')" = 3
 if grep -F -e "$EXPECTED_YANDEX_SECRET" -e "$EXPECTED_TELEGRAM_SECRET" -e "$EXPECTED_CURSOR_KEY" -e "$EXPECTED_ID_KEY" "$test_root/out" "$test_root/err" >/dev/null; then
  printf '%s\n' 'loader exposed secret bytes' >&2; exit 1
 fi
}
export WEB_LOGIN_PROVIDERS=yandex
unset YANDEX_LOGIN_CLIENT_SECRET
deny_without_write
export WEB_LOGIN_PROVIDERS=telegram
unset TELEGRAM_OIDC_CLIENT_SECRET
deny_without_write
export WEB_LOGIN_PROVIDERS=yandex,telegram YANDEX_LOGIN_CLIENT_SECRET="$EXPECTED_YANDEX_SECRET"
deny_without_write
export TELEGRAM_OIDC_CLIENT_SECRET="$EXPECTED_TELEGRAM_SECRET"
unset YANDEX_LOGIN_CLIENT_SECRET
deny_without_write
export YANDEX_LOGIN_CLIENT_SECRET="$EXPECTED_YANDEX_SECRET"
for mode in unknown telegram,yandex yandex,yandex '' ' telegram'; do
 export WEB_LOGIN_PROVIDERS="$mode"; deny_without_write
done
export WEB_LOGIN_PROVIDERS=yandex,telegram
export WEB_LOGIN_PROVIDER=''
deny_without_write
unset WEB_LOGIN_PROVIDER WEB_LOGIN_PROVIDERS
deny_without_write
printf '%s\n' 'three provider modes, separate payload keys, no fallback and secret redaction passed'
