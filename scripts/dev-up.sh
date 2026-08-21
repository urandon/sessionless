#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
project_name=sessionless-dev
. "$repo_root/scripts/local-ydb-readiness.sh"

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

run_migrations() {
	migration_log=$(mktemp "${TMPDIR:-/tmp}/sessionless-ydb-migration.XXXXXX")
	ydb_log=$(mktemp "${TMPDIR:-/tmp}/sessionless-ydb-service.XXXXXX")
	trap 'rm -f "$migration_log" "$ydb_log"' EXIT
	max_attempts=${YDB_MIGRATION_MAX_ATTEMPTS:-60}
	retry_delay=${YDB_MIGRATION_RETRY_DELAY_SECONDS:-1}
	case "$max_attempts" in
		''|*[!0-9]*|0) printf 'YDB_MIGRATION_MAX_ATTEMPTS must be a positive integer\n' >&2; return 1 ;;
	esac
	case "$retry_delay" in
		''|*[!0-9]*) printf 'YDB_MIGRATION_RETRY_DELAY_SECONDS must be a non-negative integer\n' >&2; return 1 ;;
	esac
	attempt=1
	while [ "$attempt" -le "$max_attempts" ]; do
		if make migrate-local >"$migration_log" 2>&1; then
			cat "$migration_log"
			rm -f "$migration_log" "$ydb_log"
			trap - EXIT
			return 0
		fi
		compose logs --no-color --tail 100 ydb-local >"$ydb_log" 2>&1 || true
		classification=$(classify_ydb_startup_failure "$migration_log" "$ydb_log")
		case "$classification" in
			boot-storage-failure)
				cat "$migration_log" >&2
				printf '%s\n' 'ydb-local reported a boot-storage failure (ReasonBootBSError or NumUnconnectedDisks).' >&2
				printf '%s\n' 'No volumes were deleted. Run `make dev-down` to stop containers while preserving data.' >&2
				printf '%s\n' 'Inspect the bounded YDB log tail and named volumes as documented in docs/local-development-stand.md.' >&2
				printf '%s\n' 'Only for disposable local data: CONFIRM_LOCAL_RESET=sessionless-dev make dev-reset' >&2
				cat "$ydb_log" >&2
				return 1
				;;
			retry) ;;
			fatal)
				cat "$migration_log" >&2
				return 1
				;;
			*)
				printf 'unexpected YDB startup classification: %s\n' "$classification" >&2
				return 1
				;;
		esac
		printf 'ydb-local database is initializing storage pools (attempt %d/%d)\n' "$attempt" "$max_attempts"
		attempt=$((attempt + 1))
		sleep "$retry_delay"
	done

	cat "$migration_log" >&2
	printf 'ydb-local did not become migration-ready after %d attempts\n' "$max_attempts" >&2
	return 1
}

main() {
	cd "$repo_root"
	set -a
	. "$repo_root/tools/versions.env"
	set +a

	printf 'Starting local infrastructure services.\n'
	compose up --build --detach \
		ydb-local \
		object-storage-local \
		queue-local \
		telegram-fake

	# The monitoring endpoint proves process liveness only. The migration below
	# is the query-backed database readiness gate.
	wait_http ydb-local "http://127.0.0.1:${YDB_MONITORING_PORT:-8765}/monitoring/cluster"
	wait_http object-storage-local "http://127.0.0.1:${S3_API_PORT:-9000}/minio/health/ready"
	wait_http queue-local "http://127.0.0.1:${QUEUE_API_PORT:-9324}/?Action=ListQueues&Version=2012-11-05"
	wait_http telegram-fake "http://127.0.0.1:${TELEGRAM_FAKE_PORT:-8081}/healthz"

	compose run --rm object-storage-init

	YDB_CONNECTION_STRING=${YDB_CONNECTION_STRING:-grpc://localhost:${YDB_GRPC_PORT:-2136}/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric}
	YDB_ANONYMOUS_CREDENTIALS=${YDB_ANONYMOUS_CREDENTIALS:-1}
	export YDB_CONNECTION_STRING YDB_ANONYMOUS_CREDENTIALS
	printf 'Applying the YDB schema before application startup.\n'
	run_migrations

	printf 'Starting schema-dependent application services.\n'
	compose up --build --detach \
		control-api \
		telegram-sender \
		reconciler
	wait_http control-api "http://127.0.0.1:${SESSIONLESS_HTTP_PORT:-8080}/readyz"

	TELEGRAM_API_BASE_URL=${TELEGRAM_API_BASE_URL:-http://127.0.0.1:${TELEGRAM_FAKE_PORT:-8081}}
	export TELEGRAM_API_BASE_URL
	make dev-seed

	printf 'Sessionless local stand is ready.\n'
}

if [ "${SESSIONLESS_DEV_UP_LIBRARY:-0}" != "1" ]; then
	main "$@"
fi
