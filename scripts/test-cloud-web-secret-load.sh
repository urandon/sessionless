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
    *"$EXPECTED_OIDC_SECRET"*|*"$EXPECTED_CURSOR_KEY"*|*"$EXPECTED_ID_KEY"*)
      printf '%s\n' 'secret value was exposed in jq argv' >&2
      exit 2
      ;;
  esac
done

test "$1" = '-cn'
case "$WEB_LOGIN_PROVIDER" in
  telegram) test "$TELEGRAM_OIDC_CLIENT_SECRET" = "$EXPECTED_OIDC_SECRET" ;;
  yandex) test "$YANDEX_LOGIN_CLIENT_SECRET" = "$EXPECTED_OIDC_SECRET" ;;
  *) exit 2 ;;
esac
case "$2" in
  *'if env.WEB_LOGIN_PROVIDER == "yandex" then env.YANDEX_LOGIN_CLIENT_SECRET else env.TELEGRAM_OIDC_CLIENT_SECRET end'*) ;;
  *) printf '%s\n' 'jq must select only the configured login secret' >&2; exit 2 ;;
esac
test "$SESSION_API_CURSOR_HMAC_KEY" = "$EXPECTED_CURSOR_KEY"
test "$SESSION_API_ID_HMAC_KEY" = "$EXPECTED_ID_KEY"
if test -n "${REAL_JQ:-}"; then
  exec "$REAL_JQ" "$@"
fi
printf '%s\n' payload-from-environment
EOF

cat >"$fake_bin/yc" <<'EOF'
#!/bin/sh
set -eu

test "$*" = 'lockbox secret add-version --id web-secret-id --payload - --format json'
printf '%s\n' called >>"$TEST_YC_CALLS"
if test -n "${REAL_JQ:-}"; then
  "$REAL_JQ" -e '
    length == 3 and
    .[0] == {key:"oidc-client-secret",text_value:env.EXPECTED_OIDC_SECRET} and
    .[1] == {key:"session-cursor-hmac-key",text_value:env.EXPECTED_CURSOR_KEY} and
    .[2] == {key:"session-id-hmac-key",text_value:env.EXPECTED_ID_KEY}
  ' >/dev/null
else
  test "$(cat)" = payload-from-environment
fi
printf '%s\n' '{}'
EOF

chmod 755 "$fake_bin/jq" "$fake_bin/yc"

oidc_secret='review-oidc-secret-marker'
cursor_key='review-cursor-key-marker-1234567890'
id_key='review-identity-key-marker-123456789'

real_jq=$(command -v jq || true)
export REAL_JQ="$real_jq"
export TEST_YC_CALLS="$test_root/yc-calls"
export PATH="$fake_bin:$PATH"
export WEB_BFF_SECRET_ID=web-secret-id
export SESSION_API_CURSOR_HMAC_KEY="$cursor_key"
export SESSION_API_ID_HMAC_KEY="$id_key"
export EXPECTED_OIDC_SECRET="$oidc_secret"
export EXPECTED_CURSOR_KEY="$cursor_key"
export EXPECTED_ID_KEY="$id_key"

# Default compatibility and both explicit selectors work even when the other
# provider's credential differs. Only fake yc is ever invoked by this test.
unset WEB_LOGIN_PROVIDER YANDEX_LOGIN_CLIENT_SECRET
export TELEGRAM_OIDC_CLIENT_SECRET="$oidc_secret"
"$repo_root/scripts/cloud-web-secret-load.sh" >/dev/null
export WEB_LOGIN_PROVIDER=telegram
export YANDEX_LOGIN_CLIENT_SECRET=other-provider-marker
"$repo_root/scripts/cloud-web-secret-load.sh" >/dev/null
export WEB_LOGIN_PROVIDER=yandex
export YANDEX_LOGIN_CLIENT_SECRET="$oidc_secret"
export TELEGRAM_OIDC_CLIENT_SECRET=other-provider-marker
"$repo_root/scripts/cloud-web-secret-load.sh" >/dev/null
test "$(wc -l <"$TEST_YC_CALLS" | tr -d ' ')" = 3

deny_without_write() {
  if "$repo_root/scripts/cloud-web-secret-load.sh" >"$test_root/out" 2>"$test_root/err"; then
    printf '%s\n' 'invalid selected login configuration was accepted' >&2
    exit 1
  fi
  test "$(wc -l <"$TEST_YC_CALLS" | tr -d ' ')" = 3
  if grep -F -e "$oidc_secret" -e "$cursor_key" -e "$id_key" "$test_root/out" "$test_root/err" >/dev/null; then
    printf '%s\n' 'secret value was exposed in diagnostics' >&2
    exit 1
  fi
}
unset YANDEX_LOGIN_CLIENT_SECRET
deny_without_write
export WEB_LOGIN_PROVIDER=telegram
unset TELEGRAM_OIDC_CLIENT_SECRET
deny_without_write
export WEB_LOGIN_PROVIDER=unknown
deny_without_write

printf '%s\n' 'Web secret loader selects the provider, denies missing/unknown configuration, and keeps payload values out of argv'
