#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
project_name=sessionless-dev
orchestrator=${SESSIONLESS_LOCAL_ORCHESTRATOR:-}

if [ -z "$orchestrator" ]; then
	if [ -n "${YDBD_PATH:-}" ]; then
		orchestrator=dockerless
	else
		orchestrator=compose
	fi
fi

case "$orchestrator" in
	compose|dockerless) ;;
	*) printf 'SESSIONLESS_LOCAL_ORCHESTRATOR must be compose or dockerless\n' >&2; exit 1 ;;
esac

cd "$repo_root"

failure_logs() {
	status=$?
	if [ "$status" -ne 0 ]; then
		if [ "$orchestrator" = dockerless ]; then
			DOCKERLESS_LOG_TAIL=150 "$repo_root/scripts/dockerless-local.sh" logs >&2 || true
		else
			docker compose --project-name "$project_name" ps >&2 || true
			docker compose --project-name "$project_name" logs \
				--no-color --tail 150 \
				control-api reconciler telegram-sender telegram-fake queue-local >&2 || true
		fi
	fi
	exit "$status"
}
trap failure_logs EXIT HUP INT TERM

if [ "$orchestrator" = dockerless ]; then
	make dockerless-up
else
	make dev-up
	docker compose --project-name "$project_name" --profile worker build worker-runtime
fi

if [ "$orchestrator" = dockerless ]; then
	# Host-process E2E is deliberately loopback-only. Numeric addresses avoid
	# sending local service discovery through a machine-level DNS/proxy client.
	YDB_CONNECTION_STRING="grpc://127.0.0.1:${YDB_GRPC_PORT:-2136}/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric"
	S3_ENDPOINT="http://127.0.0.1:${S3_API_PORT:-9000}"
	QUEUE_ENDPOINT="http://127.0.0.1:${QUEUE_API_PORT:-9324}"
	DISPATCH_QUEUE_URL="$QUEUE_ENDPOINT/000000000000/sessionless-dispatch"
	DEAD_LETTER_QUEUE_URL="$QUEUE_ENDPOINT/000000000000/sessionless-dlq"
	TELEGRAM_API_BASE_URL="http://127.0.0.1:${TELEGRAM_FAKE_PORT:-8081}"
	SESSIONLESS_BASE_URL="http://127.0.0.1:${SESSIONLESS_HTTP_PORT:-8080}"
else
	YDB_CONNECTION_STRING=${YDB_CONNECTION_STRING:-grpc://localhost:${YDB_GRPC_PORT:-2136}/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric}
fi
YDB_ANONYMOUS_CREDENTIALS=${YDB_ANONYMOUS_CREDENTIALS:-1}
SESSIONLESS_E2E=1
SESSIONLESS_E2E_ORCHESTRATOR=$orchestrator
export YDB_CONNECTION_STRING YDB_ANONYMOUS_CREDENTIALS SESSIONLESS_E2E SESSIONLESS_E2E_ORCHESTRATOR
if [ "$orchestrator" = dockerless ]; then
	export S3_ENDPOINT QUEUE_ENDPOINT DISPATCH_QUEUE_URL DEAD_LETTER_QUEUE_URL
	export TELEGRAM_API_BASE_URL SESSIONLESS_BASE_URL
fi

go test -v -count=1 -tags=e2elocal ./test/e2e/...
