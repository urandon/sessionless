#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
project_name=sessionless-dev

cd "$repo_root"
set -a
. "$repo_root/tools/versions.env"
set +a

compose() {
	docker compose --project-name "$project_name" "$@"
}

wait_http() {
	service_name=$1
	url=$2
	attempt=1
	while [ "$attempt" -le 120 ]; do
		if curl --fail --silent --show-error "$url" >/dev/null 2>&1; then
			printf '%s is ready: %s\n' "$service_name" "$url"
			return 0
		fi
		attempt=$((attempt + 1))
		sleep 1
	done

	printf '%s did not become ready: %s\n' "$service_name" "$url" >&2
	compose ps >&2 || true
	compose logs --no-color --tail 100 "$service_name" >&2 || true
	return 1
}

compose up --build --detach \
	ydb-local \
	object-storage-local \
	queue-local \
	telegram-fake \
	control-api

wait_http ydb-local "http://127.0.0.1:${YDB_MONITORING_PORT:-8765}/monitoring/cluster"
wait_http object-storage-local "http://127.0.0.1:${S3_API_PORT:-9000}/minio/health/ready"
wait_http queue-local "http://127.0.0.1:${QUEUE_API_PORT:-9324}/?Action=ListQueues&Version=2012-11-05"
wait_http telegram-fake "http://127.0.0.1:${TELEGRAM_FAKE_PORT:-8081}/healthz"
wait_http control-api "http://127.0.0.1:${SESSIONLESS_HTTP_PORT:-8080}/readyz"

compose run --rm object-storage-init

YDB_CONNECTION_STRING=${YDB_CONNECTION_STRING:-grpc://localhost:${YDB_GRPC_PORT:-2136}/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric}
YDB_ANONYMOUS_CREDENTIALS=${YDB_ANONYMOUS_CREDENTIALS:-1}
export YDB_CONNECTION_STRING YDB_ANONYMOUS_CREDENTIALS
make migrate-local
TELEGRAM_API_BASE_URL=${TELEGRAM_API_BASE_URL:-http://127.0.0.1:${TELEGRAM_FAKE_PORT:-8081}}
export TELEGRAM_API_BASE_URL
make dev-seed

printf 'Sessionless local stand is ready.\n'
