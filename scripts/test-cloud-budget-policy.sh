#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
policy="$repo_root/scripts/validate-cloud-budget.jq"

valid='{"id":"budget-1","billingAccountId":"account-1","status":"ACTIVE","expenseBudget":{"resetPeriod":"MONTHLY","amount":"100","filter":{"serviceIds":[],"cloudFoldersFilters":[{"cloudId":"cloud-1","folderIds":["folder-1"]}]}}}'

printf '%s' "$valid" | jq -e --arg id budget-1 --arg account account-1 --arg folder folder-1 -f "$policy" >/dev/null

for mutation in \
  '.expenseBudget.amount = "101"' \
  '.expenseBudget.resetPeriod = "QUARTER"' \
  '.expenseBudget.filter.serviceIds = ["serverless"]' \
  '.expenseBudget.filter.cloudFoldersFilters[0].folderIds += ["folder-2"]' \
  '.status = "FINISHED"'; do
  if printf '%s' "$valid" | jq "$mutation" | \
    jq -e --arg id budget-1 --arg account account-1 --arg folder folder-1 -f "$policy" >/dev/null; then
    printf 'budget policy accepted forbidden mutation: %s\n' "$mutation" >&2
    exit 1
  fi
done

printf '%s\n' '100 RUB monthly folder budget policy invariants passed'
