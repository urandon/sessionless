#!/bin/sh
set -eu

action="${1:-plan}"
case "$action" in
  plan|execute) ;;
  *)
    printf 'usage: %s [plan|execute]\n' "$0" >&2
    exit 2
    ;;
esac

resolved_folder="$(./scripts/cloud-terraform.sh output -raw folder_id)"
resolved_ydb="$(./scripts/cloud-terraform.sh output -raw ydb_connection_string)"
resolved_bucket="$(./scripts/cloud-terraform.sh output -raw artifact_bucket_name)"

check_resolved_target() {
  name="$1"
  supplied="$2"
  resolved="$3"
  if test -n "$supplied" && test "$supplied" != "$resolved"; then
    printf '%s does not match the selected cloud-dev Terraform state\n' "$name" >&2
    exit 1
  fi
}

check_resolved_target CLOUD_DEV_FOLDER_ID "${CLOUD_DEV_FOLDER_ID:-}" "$resolved_folder"
check_resolved_target YDB_CONNECTION_STRING "${YDB_CONNECTION_STRING:-}" "$resolved_ydb"
check_resolved_target S3_BUCKET "${S3_BUCKET:-}" "$resolved_bucket"

APP_ENV=cloud-dev
CLOUD_DEV_FOLDER_ID="$resolved_folder"
YDB_CONNECTION_STRING="$resolved_ydb"
S3_BUCKET="$resolved_bucket"
S3_ENDPOINT="${S3_ENDPOINT:-https://storage.yandexcloud.net}"
S3_REGION="${S3_REGION:-ru-central1}"
SESSIONLESS_RESET_OBJECT_PREFIX=tenants/
export APP_ENV CLOUD_DEV_FOLDER_ID YDB_CONNECTION_STRING
export S3_BUCKET S3_ENDPOINT S3_REGION SESSIONLESS_RESET_OBJECT_PREFIX

if test -z "${YDB_ACCESS_TOKEN_CREDENTIALS:-}" && test -n "${YC_TOKEN:-}"; then
  YDB_ACCESS_TOKEN_CREDENTIALS="$YC_TOKEN"
  export YDB_ACCESS_TOKEN_CREDENTIALS
fi

if test "$action" = plan; then
  exec go run ./cmd/preprod-reset
fi

if test -z "${YC_TOKEN:-}" && test "${S3_IAM_METADATA_CREDENTIALS:-0}" != 1; then
  printf 'execute requires YC_TOKEN or S3_IAM_METADATA_CREDENTIALS=1\n' >&2
  exit 1
fi
exec go run ./cmd/preprod-reset --execute
