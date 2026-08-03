#!/bin/sh
set -eu

for tool in terraform yc curl jq docker go; do
  command -v "$tool" >/dev/null 2>&1 || {
    printf 'required tool is missing: %s\n' "$tool" >&2
    exit 1
  }
done

: "${CLOUD_DEV_BACKEND_CONFIG:?set CLOUD_DEV_BACKEND_CONFIG to an external backend HCL file}"
: "${CLOUD_DEV_TFVARS:?set CLOUD_DEV_TFVARS to an external non-secret tfvars file}"
: "${BILLING_ACCOUNT_ID:?set BILLING_ACCOUNT_ID}"
: "${BUDGET_ID:?set BUDGET_ID}"
: "${CLOUD_DEV_FOLDER_ID:?set CLOUD_DEV_FOLDER_ID after the folder-only bootstrap apply}"

test -r "$CLOUD_DEV_BACKEND_CONFIG" || { printf 'backend config is not readable\n' >&2; exit 1; }
test -r "$CLOUD_DEV_TFVARS" || { printf 'tfvars file is not readable\n' >&2; exit 1; }

case "$CLOUD_DEV_BACKEND_CONFIG:$CLOUD_DEV_TFVARS" in
  *"/infra/terraform/"*)
    printf 'backend and tfvars files must remain outside the repository\n' >&2
    exit 1
    ;;
esac

preflight_tmp="$(mktemp -d "${TMPDIR:-/tmp}/sessionless-preflight.XXXXXX")"
trap 'rm -rf "$preflight_tmp"' EXIT HUP INT TERM
chmod 700 "$preflight_tmp"

iam_token="$(yc iam create-token)"
chmod 700 "$preflight_tmp"
printf 'url = "https://billing.api.cloud.yandex.net/billing/v1/budgets/%s"\n' "$BUDGET_ID" >"$preflight_tmp/curl.conf"
printf 'header = "Authorization: Bearer %s"\n' "$iam_token" >>"$preflight_tmp/curl.conf"
printf 'fail\nsilent\nshow-error\n' >>"$preflight_tmp/curl.conf"
curl --config "$preflight_tmp/curl.conf" >"$preflight_tmp/budget.json"
unset iam_token

jq -e --arg id "$BUDGET_ID" --arg account "$BILLING_ACCOUNT_ID" --arg folder "$CLOUD_DEV_FOLDER_ID" \
  '(.id == $id and .billingAccountId == $account and .status == "ACTIVE") and
   any(.. | strings; . == $folder)' \
  "$preflight_tmp/budget.json" >/dev/null || {
    printf 'the budget is absent, inactive, belongs to another account, or does not filter the dev folder\n' >&2
    exit 1
  }

terraform -chdir=infra/terraform/cloud-dev init \
  -backend-config="$CLOUD_DEV_BACKEND_CONFIG" -input=false
terraform -chdir=infra/terraform/cloud-dev validate
printf 'cloud-dev preflight passed; budget %s covers folder %s\n' "$BUDGET_ID" "$CLOUD_DEV_FOLDER_ID"
