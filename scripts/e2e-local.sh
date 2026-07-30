#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
project_name=sessionless-dev

cd "$repo_root"

failure_logs() {
	status=$?
	if [ "$status" -ne 0 ]; then
		docker compose --project-name "$project_name" ps >&2 || true
		docker compose --project-name "$project_name" logs \
			--no-color --tail 150 \
			control-api reconciler telegram-sender telegram-fake queue-local >&2 || true
	fi
	exit "$status"
}
trap failure_logs EXIT HUP INT TERM

make dev-up
docker compose --project-name "$project_name" --profile worker build worker-runtime

YDB_CONNECTION_STRING=${YDB_CONNECTION_STRING:-grpc://localhost:${YDB_GRPC_PORT:-2136}/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric}
YDB_ANONYMOUS_CREDENTIALS=${YDB_ANONYMOUS_CREDENTIALS:-1}
SESSIONLESS_E2E=1
export YDB_CONNECTION_STRING YDB_ANONYMOUS_CREDENTIALS SESSIONLESS_E2E

go test -v -count=1 -tags=e2elocal ./test/e2e/...
