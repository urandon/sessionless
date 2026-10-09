#!/bin/sh
set -eu
umask 077

smoke_script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
smoke_check="$smoke_script_dir/cloud-web-login-check.mjs"
# Public preflight must complete before any network or cloud client.
node "$smoke_check" config || exit 2
smoke_tmp=$(mktemp -d "${TMPDIR:-/tmp}/sessionless-web-smoke.XXXXXX")
trap 'rm -rf "$smoke_tmp"' EXIT
trap 'exit 1' HUP INT TERM
chmod 700 "$smoke_tmp"
fail() { printf '%s\n' 'cloud Web smoke check failed' >&2; exit 1; }
curl_smoke() {
  curl --disable --connect-timeout 5 --max-time 20 --max-filesize 65536 --proto '=https' \
    --suppress-connect-headers --silent --show-error "$@" 2>/dev/null
}
anonymous_status=$(curl_smoke --output /dev/null --write-out '%{http_code}' "$WEB_CONTAINER_URL/healthz") || fail
case "$anonymous_status" in 401|403) ;; *) fail ;; esac
yc iam create-token >"$smoke_tmp/token" 2>/dev/null || fail
node "$smoke_check" iam-config <"$smoke_tmp/token" >"$smoke_tmp/private.conf" || fail
rm -f "$smoke_tmp/token"
curl_smoke --config "$smoke_tmp/private.conf" "$WEB_CONTAINER_URL/healthz" >/dev/null || fail
curl_smoke --fail "$CLOUD_WEB_URL/healthz" >/dev/null || fail
curl_smoke --fail "$CLOUD_WEB_URL/readyz" >/dev/null || fail
curl_smoke --fail --output "$smoke_tmp/version.json" "$CLOUD_WEB_URL/version" || fail
node "$smoke_check" version <"$smoke_tmp/version.json" || fail
curl_smoke --fail --dump-header "$smoke_tmp/root.headers" --output "$smoke_tmp/root.html" \
  --header 'Accept: text/html' "$CLOUD_WEB_URL/" || fail
node "$smoke_check" root <"$smoke_tmp/root.headers" || fail
curl_smoke --dump-header "$smoke_tmp/me.headers" --output /dev/null "$CLOUD_WEB_URL/api/web/v1/me" || fail
node "$smoke_check" me <"$smoke_tmp/me.headers" || fail
# Capture the authorization URL privately; never follow or print it.
curl_smoke --dump-header "$smoke_tmp/login.headers" --output /dev/null \
  "$CLOUD_WEB_URL/auth/login/start?return_to=%2F" || fail
node "$smoke_check" login <"$smoke_tmp/login.headers" || fail
for forbidden in /telegram/webhook /api/not-allowed; do
  status=$(curl_smoke --output /dev/null --write-out '%{http_code}' --request POST "$CLOUD_WEB_URL$forbidden") || fail
  case "$status" in 404|405) ;; *) fail ;; esac
done
if test "${WEB_COLD_START_WAIT_SECONDS:-0}" -gt 0; then
  sleep "$WEB_COLD_START_WAIT_SECONDS"
  first_byte=$(curl_smoke --fail --output /dev/null --write-out '%{time_starttransfer}' "$CLOUD_WEB_URL/healthz") || fail
  case "$first_byte" in ''|*[!0-9.]*) fail ;; esac
  printf 'cold-start first-byte latency: %ss\n' "$first_byte"
fi
printf '%s\n' 'private isolation, managed HTTPS, headers, selected login, and Web route checks passed'
