#!/bin/sh
set -eu

: "${CLOUD_WEB_URL:?set CLOUD_WEB_URL from Terraform output web_url}"
: "${WEB_CONTAINER_URL:?set WEB_CONTAINER_URL from Terraform output web_container_url}"
: "${WEB_IMAGE_REF:?set WEB_IMAGE_REF from Terraform output web_image_ref}"
: "${WEB_PREPARED_INSTANCES:?set WEB_PREPARED_INSTANCES from Terraform output web_prepared_instances}"
: "${WEB_CONCURRENCY:?set WEB_CONCURRENCY from Terraform output web_concurrency}"

case "$CLOUD_WEB_URL" in https://web.dev.*) ;; *) printf '%s\n' 'CLOUD_WEB_URL must be the managed HTTPS Web hostname' >&2; exit 2 ;; esac
case "$WEB_IMAGE_REF" in cr.yandex/*/web-bff@sha256:????????????????????????????????????????????????????????????????) ;; *) printf '%s\n' 'WEB_IMAGE_REF is not an immutable web-bff digest' >&2; exit 2 ;; esac
test "$WEB_PREPARED_INSTANCES" = 0 || { printf '%s\n' 'Web BFF must have zero prepared instances' >&2; exit 1; }
test "$WEB_CONCURRENCY" -ge 1 && test "$WEB_CONCURRENCY" -le 8 || { printf '%s\n' 'Web BFF concurrency is outside the cost boundary' >&2; exit 1; }

smoke_tmp=$(mktemp -d "${TMPDIR:-/tmp}/sessionless-web-smoke.XXXXXX")
trap 'rm -rf "$smoke_tmp"' EXIT HUP INT TERM
chmod 700 "$smoke_tmp"

anonymous_status=$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' "$WEB_CONTAINER_URL/healthz")
case "$anonymous_status" in 401|403) ;; *) printf 'anonymous direct Web container status = %s, want 401 or 403\n' "$anonymous_status" >&2; exit 1 ;; esac

iam_token=$(yc iam create-token)
printf 'header = "Authorization: Bearer %s"\nfail\nsilent\nshow-error\n' "$iam_token" >"$smoke_tmp/private.conf"
unset iam_token
curl --config "$smoke_tmp/private.conf" "$WEB_CONTAINER_URL/healthz" >/dev/null

curl --fail --silent --show-error "$CLOUD_WEB_URL/healthz" >/dev/null
curl --fail --silent --show-error "$CLOUD_WEB_URL/readyz" >/dev/null
curl --fail --silent --show-error "$CLOUD_WEB_URL/version" | jq -e '.component == "web-bff"' >/dev/null

curl --fail --silent --show-error --dump-header "$smoke_tmp/root.headers" --output "$smoke_tmp/root.html" \
  --header 'Accept: text/html' "$CLOUD_WEB_URL/"
for header in \
  'strict-transport-security: max-age=31536000; includeSubDomains' \
  'x-content-type-options: nosniff' \
  'referrer-policy: strict-origin-when-cross-origin' \
  'cache-control: no-store'; do
  tr -d '\r' <"$smoke_tmp/root.headers" | grep -Fiq "$header" || { printf 'missing Web root header: %s\n' "$header" >&2; exit 1; }
done
tr -d '\r' <"$smoke_tmp/root.headers" | grep -Fi 'content-security-policy:' | grep -Fq "frame-ancestors 'none'" || {
  printf '%s\n' 'Web root CSP does not deny framing' >&2
  exit 1
}

me_status=$(curl --silent --show-error --dump-header "$smoke_tmp/me.headers" --output /dev/null --write-out '%{http_code}' "$CLOUD_WEB_URL/api/web/v1/me")
test "$me_status" = 401 || { printf 'unauthenticated /me status = %s, want 401\n' "$me_status" >&2; exit 1; }
tr -d '\r' <"$smoke_tmp/me.headers" | grep -Fiq 'cache-control: no-store' || { printf '%s\n' 'auth response is not no-store' >&2; exit 1; }

curl --silent --show-error --dump-header "$smoke_tmp/oidc.headers" --output /dev/null \
  "$CLOUD_WEB_URL/auth/telegram/start?return_to=%2F"
location=$(tr -d '\r' <"$smoke_tmp/oidc.headers" | sed -n 's/^[Ll]ocation: //p' | head -n 1)
case "$location" in https://oauth.telegram.org/*) ;; *) printf '%s\n' 'OIDC start did not redirect to Telegram' >&2; exit 1 ;; esac

for forbidden in /telegram/webhook /api/not-allowed; do
  status=$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' --request POST "$CLOUD_WEB_URL$forbidden")
  case "$status" in 404|405) ;; *) printf 'forbidden Web route %s returned %s\n' "$forbidden" "$status" >&2; exit 1 ;; esac
done

if test "${WEB_COLD_START_WAIT_SECONDS:-0}" -gt 0; then
  sleep "$WEB_COLD_START_WAIT_SECONDS"
  first_byte=$(curl --fail --silent --show-error --output /dev/null --write-out '%{time_starttransfer}' "$CLOUD_WEB_URL/healthz")
  printf 'cold-start first-byte latency: %ss\n' "$first_byte"
fi

printf '%s\n' 'private isolation, managed HTTPS, headers, OIDC, and Web route checks passed'
