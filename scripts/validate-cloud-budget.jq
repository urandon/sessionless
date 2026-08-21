def usage_budget: (.costBudget // .expenseBudget // null);

(.id == $id and .billingAccountId == $account and .status == "ACTIVE") and
(usage_budget as $budget |
  $budget != null and
  ($budget.amount | tonumber) > 0 and
  ($budget.amount | tonumber) <= 100 and
  $budget.resetPeriod == "MONTHLY" and
  (($budget.filter.serviceIds // []) | length) == 0 and
  (($budget.filter.cloudFoldersFilters // []) | length) == 1 and
  ($budget.filter.cloudFoldersFilters[0].folderIds == [$folder]))
