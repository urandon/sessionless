#!/bin/sh
set -eu

: "${REGISTRY_GC_REPORT_JSON:?set REGISTRY_GC_REPORT_JSON}"
: "${REGISTRY_GC_REPORT_MARKDOWN:?set REGISTRY_GC_REPORT_MARKDOWN}"

mkdir -p "$(dirname "$REGISTRY_GC_REPORT_JSON")" "$(dirname "$REGISTRY_GC_REPORT_MARKDOWN")"
mode=${REGISTRY_GC_MODE:-dry-run}
case "$mode" in
  dry-run|delete) ;;
  *) mode=invalid ;;
esac

jq -S -n \
  --arg mode "$mode" \
  --arg repository "${GITHUB_REPOSITORY:-unknown}" \
  --arg commit "${GITHUB_SHA:-unknown}" \
  --arg run_id "${GITHUB_RUN_ID:-}" \
  --arg run_attempt "${GITHUB_RUN_ATTEMPT:-}" \
  --arg run_url "${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:-unknown}/actions/runs/${GITHUB_RUN_ID:-unknown}" \
  '{
    schema_version: 1,
    environment: "cloud-dev",
    mode: $mode,
    status: "blocked",
    source: {
      repository: $repository,
      commit: $commit,
      workflow_run_id: $run_id,
      workflow_run_attempt: $run_attempt,
      workflow_run_url: $run_url
    },
    decisions: [],
    totals: {
      evaluated: 0,
      retained: 0,
      delete_candidates: 0,
      deleted: 0,
      estimated_reclaimed_bytes: 0
    },
    blocking_errors: [{code: "run_incomplete"}]
  }' >"$REGISTRY_GC_REPORT_JSON"

cat >"$REGISTRY_GC_REPORT_MARKDOWN" <<EOF
## Container Registry cleanup

- Environment: \`cloud-dev\`
- Mode: \`$mode\`
- Status: **blocked**
- Deletions: **0**

The cleanup did not reach a complete, validated decision. The machine report
contains the typed \`run_incomplete\` blocker until the cleanup command replaces
this bootstrap report.
EOF
