#!/bin/sh
set -eu

: "${CLOUD_API_URL:?set CLOUD_API_URL from Terraform output}"
: "${CONTROL_CONTAINER_URL:?set a private control slot URL from Terraform output}"

smoke_tmp="$(mktemp -d "${TMPDIR:-/tmp}/sessionless-smoke.XXXXXX")"
trap 'rm -rf "$smoke_tmp"' EXIT HUP INT TERM
chmod 700 "$smoke_tmp"
iam_token="$(yc iam create-token)"
printf 'header = "Authorization: Bearer %s"\nfail\nsilent\nshow-error\n' "$iam_token" >"$smoke_tmp/private.conf"
unset iam_token

curl --config "$smoke_tmp/private.conf" "${CONTROL_CONTAINER_URL}/healthz" >/dev/null
curl --fail --silent --show-error "${CLOUD_API_URL}/healthz" >/dev/null
curl --fail --silent --show-error "${CLOUD_API_URL}/readyz" >/dev/null
curl --fail --silent --show-error "${CLOUD_API_URL}/version" | jq -e '.component == "control-api"' >/dev/null
printf 'private IAM invocation and public API Gateway smoke checks passed\n'
