#!/bin/sh
# Credential-free command-boundary regression: no real yc/curl/Terraform calls.
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
test_tmp=$(mktemp -d "${TMPDIR:-/tmp}/sessionless-preflight-test.XXXXXX")
trap 'rm -rf "$test_tmp"' EXIT HUP INT TERM
mkdir "$test_tmp/bin"

# Use a closed PATH, so a Docker installation on the test host cannot hide
# an accidental reintroduction of Docker as a cloud-only prerequisite.
for tool in jq mktemp chmod rm; do
  ln -s "$(command -v "$tool")" "$test_tmp/bin/$tool"
done
printf '%s\n' '#!/bin/sh' 'printf "%s\n" synthetic-test-token' >"$test_tmp/bin/yc"
printf '%s\n' '#!/bin/sh' 'printf "%s\n" "$*" >>"$PREFLIGHT_TEST_CALLS"' >"$test_tmp/bin/terraform"
printf '%s\n' '#!/bin/sh' 'exit 0' >"$test_tmp/bin/go"
printf '%s\n' '#!/bin/sh' \
  'case "$2" in' \
  '  */account-curl.conf) printf "%s\n" "{\"id\":\"billing-test\",\"active\":true,\"currency\":\"RUB\"}" ;;' \
  '  */curl.conf) printf "%s\n" "{\"id\":\"budget-test\",\"billingAccountId\":\"billing-test\",\"status\":\"ACTIVE\",\"costBudget\":{\"amount\":\"100\",\"resetPeriod\":\"MONTHLY\",\"filter\":{\"cloudFoldersFilters\":[{\"folderIds\":[\"folder-test\"]}]}}}" ;;' \
  '  *) exit 90 ;;' \
  'esac' >"$test_tmp/bin/curl"
chmod +x "$test_tmp/bin/yc" "$test_tmp/bin/terraform" "$test_tmp/bin/go" "$test_tmp/bin/curl"
touch "$test_tmp/backend.hcl" "$test_tmp/cloud.tfvars"

cd "$repo_root"
PATH="$test_tmp/bin" \
  PREFLIGHT_TEST_CALLS="$test_tmp/calls" \
  CLOUD_DEV_BACKEND_CONFIG="$test_tmp/backend.hcl" \
  CLOUD_DEV_TFVARS="$test_tmp/cloud.tfvars" \
  BILLING_ACCOUNT_ID=billing-test BUDGET_ID=budget-test CLOUD_DEV_FOLDER_ID=folder-test \
  /bin/sh "$repo_root/scripts/cloud-preflight.sh" >"$test_tmp/result"
grep -Fq 'cloud-dev preflight passed' "$test_tmp/result"
grep -Fq -- '-chdir=infra/terraform/cloud-dev init' "$test_tmp/calls"
grep -Fq -- '-chdir=infra/terraform/cloud-dev validate' "$test_tmp/calls"

# Removing a still-required tool must fail before any network/TF work.
mv "$test_tmp/bin/go" "$test_tmp/go"
if PATH="$test_tmp/bin" /bin/sh "$repo_root/scripts/cloud-preflight.sh" >"$test_tmp/missing" 2>&1; then
  printf '%s\n' 'preflight must reject a missing required tool' >&2
  exit 1
fi
grep -Fq 'required tool is missing: go' "$test_tmp/missing"
printf '%s\n' 'Cloud preflight works without Docker and retains required-tool gates'
