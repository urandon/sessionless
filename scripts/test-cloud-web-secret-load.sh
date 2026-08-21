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

test "$TELEGRAM_OIDC_CLIENT_SECRET" = "$EXPECTED_OIDC_SECRET"
test "$SESSION_API_CURSOR_HMAC_KEY" = "$EXPECTED_CURSOR_KEY"
test "$SESSION_API_ID_HMAC_KEY" = "$EXPECTED_ID_KEY"
printf '%s\n' payload-from-environment
EOF

cat >"$fake_bin/yc" <<'EOF'
#!/bin/sh
set -eu

test "$*" = 'lockbox secret add-version --id web-secret-id --payload - --format json'
test "$(cat)" = payload-from-environment
printf '%s\n' '{}'
EOF

chmod 755 "$fake_bin/jq" "$fake_bin/yc"

oidc_secret='review-oidc-secret-marker'
cursor_key='review-cursor-key-marker-1234567890'
id_key='review-identity-key-marker-123456789'

PATH="$fake_bin:$PATH" \
  WEB_BFF_SECRET_ID=web-secret-id \
  TELEGRAM_OIDC_CLIENT_SECRET="$oidc_secret" \
  SESSION_API_CURSOR_HMAC_KEY="$cursor_key" \
  SESSION_API_ID_HMAC_KEY="$id_key" \
  EXPECTED_OIDC_SECRET="$oidc_secret" \
  EXPECTED_CURSOR_KEY="$cursor_key" \
  EXPECTED_ID_KEY="$id_key" \
  "$repo_root/scripts/cloud-web-secret-load.sh" >/dev/null

printf '%s\n' 'Web secret loader keeps payload values out of argv'
