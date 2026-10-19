#!/usr/bin/env bash
set -euo pipefail

# Only observe the two disposable containers already created by CI. The pinned
# image's /health_check proves SQL, scheme access and a storage-backed DDL pair;
# monitoring/cluster alone is process liveness, not migration readiness.
if [[ $# != 1 ]]; then
  echo 'usage: wait-ci-ydb.sh sessionless-ydb|sessionless-security-ydb' >&2
  exit 2
fi
container=$1
case "$container" in
  sessionless-ydb|sessionless-security-ydb) ;;
  *) echo 'refusing an unowned CI container name' >&2; exit 2 ;;
esac

diagnose() {
  echo "YDB readiness failed for $container: $1" >&2
  timeout --signal=KILL 5s docker inspect --format '{{json .State}}' "$container" >&2 || true
  timeout --signal=KILL 5s docker logs --timestamps --tail 50 "$container" >&2 || true
  return 1
}

# Both wall-clock budget and finite poll count apply. Each inspect is bounded
# by the smaller of its own 3s allowance and the original remaining budget.
started=$SECONDS
for ((attempt=1; attempt<=90; attempt++)); do
  remaining=$((90 - (SECONDS - started)))
  if ((remaining <= 0)); then
    diagnose '90-second readiness deadline exhausted'
    exit 1
  fi
  call_seconds=3
  if ((remaining < call_seconds)); then call_seconds=$remaining; fi
  if ! state=$(timeout --signal=KILL "${call_seconds}s" docker inspect --format '{{json .Config.Healthcheck.Test}}|{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$container"); then
    diagnose 'inspect failed or exceeded its deadline'
    exit 1
  fi
  case "$state" in
    '["CMD-SHELL","sh /health_check"]|running|healthy')
      echo "YDB SQL/storage health is ready: $container"
      exit 0
      ;;
    '["CMD-SHELL","sh /health_check"]|running|starting') ;;
    *) diagnose "unexpected health or container state: $state"; exit 1 ;;
  esac
  sleep 1
done
diagnose '90-poll readiness budget exhausted'
