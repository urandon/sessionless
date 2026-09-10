#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
runtime_root=${SESSIONLESS_DOCKERLESS_ROOT:-$repo_root/.build/dockerless}
native_deps_dir=${SESSIONLESS_NATIVE_DEPS_DIR:-$(dirname "$repo_root")/sessionless-native-deps}
state_dir=$runtime_root/pids
log_dir=$runtime_root/logs
data_dir=$runtime_root/data
scratch_dir=$runtime_root/tmp

fail() {
	printf 'dockerless local stand: %s\n' "$*" >&2
	exit 1
}

require_absolute_path() {
	name=$1
	value=$2
	case "$value" in
		/*) ;;
		*) fail "$name must be an absolute path: $value" ;;
	esac
}

require_executable() {
	name=$1
	value=$2
	require_absolute_path "$name" "$value"
	test -x "$value" || fail "$name is not executable: $value"
}

require_file() {
	name=$1
	value=$2
	require_absolute_path "$name" "$value"
	test -f "$value" || fail "$name does not exist: $value"
}

prepare_runtime() {
	require_absolute_path SESSIONLESS_DOCKERLESS_ROOT "$runtime_root"
	case "$runtime_root" in
		/|/Users|/Volumes|/Volumes/hubdisk|/Volumes/hubdisk/workspace)
			fail "SESSIONLESS_DOCKERLESS_ROOT is too broad: $runtime_root"
			;;
	esac
	mkdir -p "$state_dir" "$log_dir" "$data_dir" "$scratch_dir"
	runtime_root=$(CDPATH= cd -- "$runtime_root" && pwd -P)
	state_dir=$runtime_root/pids
	log_dir=$runtime_root/logs
	data_dir=$runtime_root/data
	scratch_dir=$runtime_root/tmp
	case "$runtime_root" in
		/|/Users|/Volumes|/Volumes/hubdisk|/Volumes/hubdisk/workspace)
			fail "resolved SESSIONLESS_DOCKERLESS_ROOT is too broad: $runtime_root"
			;;
	esac
}

load_versions() {
	set -a
	# shellcheck source=/dev/null
	. "$repo_root/tools/versions.env"
	set +a
}

configure_paths() {
	YDBD_PATH=${YDBD_PATH:-}
	YDB_LOCAL_LAUNCHER_PATH=${YDB_LOCAL_LAUNCHER_PATH:-}
	YDB_RUNTIME_DIR=${YDB_RUNTIME_DIR:-$data_dir/ydb}
	YDB_CONFIG_PATH=${YDB_CONFIG_PATH:-$YDB_RUNTIME_DIR/cluster/kikimr_configs/config.yaml}
	MINIO_PATH=${MINIO_PATH:-$native_deps_dir/bin/minio.$MINIO_IMAGE_VERSION}
	MC_PATH=${MC_PATH:-$native_deps_dir/bin/mc.$MINIO_MC_IMAGE_VERSION}
	ELASTICMQ_JAR=${ELASTICMQ_JAR:-$native_deps_dir/elasticmq/elasticmq-server-all-$ELASTICMQ_IMAGE_VERSION.jar}
	JAVA_PATH=${JAVA_PATH:-$native_deps_dir/jre-21/Contents/Home/bin/java}
}

configure_environment() {
	APP_ENV=${APP_ENV:-local}
	SESSIONLESS_ENVIRONMENT=${SESSIONLESS_ENVIRONMENT:-local}
	LOG_LEVEL=${LOG_LEVEL:-debug}
	YDB_GRPC_PORT=${YDB_GRPC_PORT:-2136}
	YDB_MONITORING_PORT=${YDB_MONITORING_PORT:-8765}
	YDB_INTERCONNECT_PORT=${YDB_INTERCONNECT_PORT:-19001}
	YDB_CONNECTION_STRING=${YDB_CONNECTION_STRING:-grpc://127.0.0.1:$YDB_GRPC_PORT/local?go_query_mode=scripting\&go_fake_tx=scripting\&go_query_bind=declare,numeric}
	YDB_ANONYMOUS_CREDENTIALS=${YDB_ANONYMOUS_CREDENTIALS:-1}

	S3_API_PORT=${S3_API_PORT:-9000}
	S3_CONSOLE_PORT=${S3_CONSOLE_PORT:-9001}
	S3_ENDPOINT=${S3_ENDPOINT:-http://127.0.0.1:$S3_API_PORT}
	S3_REGION=${S3_REGION:-us-east-1}
	S3_BUCKET=${S3_BUCKET:-sessionless-local}
	S3_ACCESS_KEY_ID=${S3_ACCESS_KEY_ID:-sessionless-local}
	S3_SECRET_ACCESS_KEY=${S3_SECRET_ACCESS_KEY:-sessionless-local-secret}
	S3_FORCE_PATH_STYLE=${S3_FORCE_PATH_STYLE:-true}

	QUEUE_API_PORT=${QUEUE_API_PORT:-9324}
	QUEUE_UI_PORT=${QUEUE_UI_PORT:-9325}
	QUEUE_ENDPOINT=${QUEUE_ENDPOINT:-http://127.0.0.1:$QUEUE_API_PORT}
	QUEUE_REGION=${QUEUE_REGION:-us-east-1}
	QUEUE_ACCESS_KEY_ID=${QUEUE_ACCESS_KEY_ID:-sessionless-local}
	QUEUE_SECRET_ACCESS_KEY=${QUEUE_SECRET_ACCESS_KEY:-sessionless-local-secret}
	DISPATCH_QUEUE_URL=${DISPATCH_QUEUE_URL:-$QUEUE_ENDPOINT/000000000000/sessionless-dispatch}
	SCHEDULER_WAKE_QUEUE_URL=${SCHEDULER_WAKE_QUEUE_URL:-$QUEUE_ENDPOINT/000000000000/sessionless-scheduler-wake}
	DELIVERY_QUEUE_URL=${DELIVERY_QUEUE_URL:-$QUEUE_ENDPOINT/000000000000/sessionless-delivery}
	DEAD_LETTER_QUEUE_URL=${DEAD_LETTER_QUEUE_URL:-$QUEUE_ENDPOINT/000000000000/sessionless-dlq}
	SCHEDULER_WAKE_DEAD_LETTER_QUEUE_URL=${SCHEDULER_WAKE_DEAD_LETTER_QUEUE_URL:-$DEAD_LETTER_QUEUE_URL}
	DELIVERY_DEAD_LETTER_QUEUE_URL=${DELIVERY_DEAD_LETTER_QUEUE_URL:-$DEAD_LETTER_QUEUE_URL}
	OUTBOX_QUEUE_ACCESS_KEY_ID=${OUTBOX_QUEUE_ACCESS_KEY_ID:-$QUEUE_ACCESS_KEY_ID}
	OUTBOX_QUEUE_SECRET_ACCESS_KEY=${OUTBOX_QUEUE_SECRET_ACCESS_KEY:-$QUEUE_SECRET_ACCESS_KEY}

	TELEGRAM_FAKE_PORT=${TELEGRAM_FAKE_PORT:-8081}
	TELEGRAM_API_BASE_URL=${TELEGRAM_API_BASE_URL:-http://127.0.0.1:$TELEGRAM_FAKE_PORT}
	TELEGRAM_FAKE_TOKEN=${TELEGRAM_FAKE_TOKEN:-local-test-token}
	TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN:-$TELEGRAM_FAKE_TOKEN}
	TELEGRAM_WEBHOOK_SECRET=${TELEGRAM_WEBHOOK_SECRET:-local-webhook-secret}
	TELEGRAM_IDENTITY_HMAC_KEY=${TELEGRAM_IDENTITY_HMAC_KEY:-sessionless-local-identity-key-0001}
	TELEGRAM_SOURCE_ID=${TELEGRAM_SOURCE_ID:-bot-primary}
	DEFAULT_COMPUTE_PROVIDER=${DEFAULT_COMPUTE_PROVIDER:-codex}

	SESSIONLESS_HTTP_PORT=${SESSIONLESS_HTTP_PORT:-8080}
	SESSIONLESS_BASE_URL=${SESSIONLESS_BASE_URL:-http://127.0.0.1:$SESSIONLESS_HTTP_PORT}
	SCHEDULER_WAKE_RETRY_DELAY=${SCHEDULER_WAKE_RETRY_DELAY:-250ms}
	SCHEDULER_WAKE_MAX_DELIVERY_COUNT=${SCHEDULER_WAKE_MAX_DELIVERY_COUNT:-8}
	SCHEDULER_RESERVATION_TTL=${SCHEDULER_RESERVATION_TTL:-5m}
	LIMIT_TENANT_QUEUE_DEPTH=${LIMIT_TENANT_QUEUE_DEPTH:-8}
	LIMIT_ACTIVE_RUNS=${LIMIT_ACTIVE_RUNS:-1}
	LIMIT_CONTEXT_BYTES=${LIMIT_CONTEXT_BYTES:-67108864}
	LIMIT_CONTEXT_EVENTS=${LIMIT_CONTEXT_EVENTS:-512}
	SNAPSHOT_INTERVAL_EVENTS=${SNAPSHOT_INTERVAL_EVENTS:-128}
	SNAPSHOT_MAX_VERSIONS=${SNAPSHOT_MAX_VERSIONS:-32}
	TELEGRAM_SENDER_BASE_BACKOFF=${TELEGRAM_SENDER_BASE_BACKOFF:-250ms}
	TELEGRAM_SENDER_MAX_BACKOFF=${TELEGRAM_SENDER_MAX_BACKOFF:-2s}

	WORKER_ID=${WORKER_ID:-worker-runtime-local}
	WORKER_SCRATCH_ROOT=${WORKER_SCRATCH_ROOT:-$scratch_dir/worker}
	WORKER_LEASE_TTL=${WORKER_LEASE_TTL:-2m}
	WORKER_RETRY_DELAY=${WORKER_RETRY_DELAY:-1s}
	WORKER_QUEUE_WAIT=${WORKER_QUEUE_WAIT:-2s}
	WORKER_MAX_DELIVERY_COUNT=${WORKER_MAX_DELIVERY_COUNT:-5}
	WORKER_MAX_BLOB_BYTES=${WORKER_MAX_BLOB_BYTES:-67108864}
	WORKER_MAX_SNAPSHOT_FALLBACKS=${WORKER_MAX_SNAPSHOT_FALLBACKS:-4}
	DETERMINISTIC_HARNESS_TURNS=${DETERMINISTIC_HARNESS_TURNS:-2}
	DETERMINISTIC_HARNESS_ARTIFACTS=${DETERMINISTIC_HARNESS_ARTIFACTS:-1}
	DETERMINISTIC_HARNESS_FAIL_BEFORE_FIRST_TURN=${DETERMINISTIC_HARNESS_FAIL_BEFORE_FIRST_TURN:-false}
	DETERMINISTIC_HARNESS_FAIL_AT_TURN=${DETERMINISTIC_HARNESS_FAIL_AT_TURN:-0}
	DETERMINISTIC_HARNESS_RETRYABLE_FAIL=${DETERMINISTIC_HARNESS_RETRYABLE_FAIL:-false}

	DOCKERLESS_LOG_MAX_BYTES=${DOCKERLESS_LOG_MAX_BYTES:-10485760}
	case "$DOCKERLESS_LOG_MAX_BYTES" in
		''|*[!0-9]*|0) fail 'DOCKERLESS_LOG_MAX_BYTES must be a positive integer' ;;
	esac
	export APP_ENV SESSIONLESS_ENVIRONMENT LOG_LEVEL
	export YDB_CONNECTION_STRING YDB_ANONYMOUS_CREDENTIALS
	export S3_ENDPOINT S3_REGION S3_BUCKET S3_ACCESS_KEY_ID S3_SECRET_ACCESS_KEY S3_FORCE_PATH_STYLE
	export QUEUE_ENDPOINT QUEUE_REGION QUEUE_ACCESS_KEY_ID QUEUE_SECRET_ACCESS_KEY
	export DISPATCH_QUEUE_URL SCHEDULER_WAKE_QUEUE_URL DELIVERY_QUEUE_URL DEAD_LETTER_QUEUE_URL
	export SCHEDULER_WAKE_DEAD_LETTER_QUEUE_URL DELIVERY_DEAD_LETTER_QUEUE_URL
	export OUTBOX_QUEUE_ACCESS_KEY_ID OUTBOX_QUEUE_SECRET_ACCESS_KEY
	export TELEGRAM_API_BASE_URL TELEGRAM_FAKE_TOKEN TELEGRAM_BOT_TOKEN TELEGRAM_WEBHOOK_SECRET
	export TELEGRAM_IDENTITY_HMAC_KEY TELEGRAM_SOURCE_ID DEFAULT_COMPUTE_PROVIDER
	export SESSIONLESS_BASE_URL
	export SCHEDULER_WAKE_RETRY_DELAY SCHEDULER_WAKE_MAX_DELIVERY_COUNT SCHEDULER_RESERVATION_TTL
	export LIMIT_TENANT_QUEUE_DEPTH LIMIT_ACTIVE_RUNS LIMIT_CONTEXT_BYTES LIMIT_CONTEXT_EVENTS
	export SNAPSHOT_INTERVAL_EVENTS SNAPSHOT_MAX_VERSIONS
	export TELEGRAM_SENDER_BASE_BACKOFF TELEGRAM_SENDER_MAX_BACKOFF
	export WORKER_ID WORKER_SCRATCH_ROOT WORKER_LEASE_TTL WORKER_RETRY_DELAY WORKER_QUEUE_WAIT
	export WORKER_MAX_DELIVERY_COUNT WORKER_MAX_BLOB_BYTES WORKER_MAX_SNAPSHOT_FALLBACKS
	export DETERMINISTIC_HARNESS_TURNS DETERMINISTIC_HARNESS_ARTIFACTS
	export DETERMINISTIC_HARNESS_FAIL_BEFORE_FIRST_TURN DETERMINISTIC_HARNESS_FAIL_AT_TURN
	export DETERMINISTIC_HARNESS_RETRYABLE_FAIL
}

pid_path() {
	printf '%s/%s.pid\n' "$state_dir" "$1"
}

marker_path() {
	printf '%s/%s.marker\n' "$state_dir" "$1"
}

process_command() {
	ps -p "$1" -o command= 2>/dev/null || true
}

bound_log_file() {
	log_file=$1
	test -f "$log_file" || return 0
	log_bytes=$(wc -c <"$log_file" | tr -d ' ')
	case "$log_bytes" in
		''|*[!0-9]*) fail "could not measure log file: $log_file" ;;
	esac
	test "$log_bytes" -le "$DOCKERLESS_LOG_MAX_BYTES" && return 0
	bounded_log=$log_file.tail.$$
	if ! tail -c "$DOCKERLESS_LOG_MAX_BYTES" "$log_file" >"$bounded_log"; then
		rm -f "$bounded_log"
		fail "could not retain bounded log tail: $log_file"
	fi
	mv "$bounded_log" "$log_file"
}

owned_pid() {
	service=$1
	pid_file=$(pid_path "$service")
	marker_file=$(marker_path "$service")
	test -f "$pid_file" && test -f "$marker_file" || return 1
	pid=$(cat "$pid_file")
	case "$pid" in
		''|*[!0-9]*) return 1 ;;
	esac
	marker=$(cat "$marker_file")
	test -n "$marker" || return 1
	command=$(process_command "$pid")
	test -n "$command" || return 1
	case "$command" in
		*"$marker"*) printf '%s\n' "$pid"; return 0 ;;
		*) return 1 ;;
	esac
}

record_process() {
	service=$1
	pid=$2
	marker=$3
	tmp_pid=$(pid_path "$service").tmp.$$
	tmp_marker=$(marker_path "$service").tmp.$$
	printf '%s\n' "$pid" >"$tmp_pid"
	printf '%s\n' "$marker" >"$tmp_marker"
	mv "$tmp_pid" "$(pid_path "$service")"
	mv "$tmp_marker" "$(marker_path "$service")"
}

start_process() {
	service=$1
	marker=$2
	shift 2
	if pid=$(owned_pid "$service"); then
		printf '%s is already running (pid %s)\n' "$service" "$pid"
		return 0
	fi
	rm -f "$(pid_path "$service")" "$(marker_path "$service")"
	log_file=$log_dir/$service.log
	bound_log_file "$log_file"
	nohup "$@" >>"$log_file" 2>&1 </dev/null &
	pid=$!
	record_process "$service" "$pid" "$marker"
	attempt=1
	while test "$attempt" -le 20; do
		if owned_pid "$service" >/dev/null; then
			printf 'started %s (pid %s)\n' "$service" "$pid"
			return 0
		fi
		attempt=$((attempt + 1))
		sleep 0.1
	done
	tail -n 60 "$log_file" >&2 || true
	fail "$service exited during startup"
}

stop_process() {
	service=$1
	pid_file=$(pid_path "$service")
	marker_file=$(marker_path "$service")
	if test ! -e "$pid_file" && test ! -e "$marker_file"; then
		printf '%s is not managed by this stand\n' "$service"
		return 0
	fi
	if test ! -f "$pid_file" || test ! -f "$marker_file"; then
		printf 'refusing incomplete %s pid metadata\n' "$service" >&2
		return 1
	fi
	pid=$(cat "$pid_file")
	case "$pid" in
		''|*[!0-9]*) printf 'refusing invalid %s pid metadata\n' "$service" >&2; return 1 ;;
	esac
	if test -z "$(process_command "$pid")"; then
		rm -f "$pid_file" "$marker_file"
		printf 'removed exited %s pid metadata\n' "$service"
		return 0
	fi
	if ! owned_pid "$service" >/dev/null; then
		printf 'refusing to signal stale or mismatched %s pid metadata\n' "$service" >&2
		return 1
	fi
	kill -TERM "$pid"
	attempt=1
	while test "$attempt" -le 150; do
		if ! kill -0 "$pid" 2>/dev/null; then
			rm -f "$pid_file" "$marker_file"
			printf 'stopped %s\n' "$service"
			return 0
		fi
		attempt=$((attempt + 1))
		sleep 0.1
	done
	printf '%s did not stop after 15 seconds; pid %s was not force-killed\n' "$service" "$pid" >&2
	return 1
}

wait_http() {
	service=$1
	url=$2
	attempt=1
	while test "$attempt" -le 120; do
		if curl --connect-timeout 2 --max-time 5 --fail --silent --show-error "$url" >/dev/null 2>&1; then
			printf '%s is ready: %s\n' "$service" "$url"
			return 0
		fi
		attempt=$((attempt + 1))
		sleep 1
	done
	tail -n 100 "$log_dir/$service.log" >&2 || true
	fail "$service did not become ready: $url"
}

build_apps() {
	make -C "$repo_root" dockerless-build
}

start_ydb_direct() {
	require_file YDB_CONFIG_PATH "$YDB_CONFIG_PATH"
	cluster_dir=$(CDPATH= cd -- "$(dirname "$YDB_CONFIG_PATH")/.." && pwd -P)
	node_dir=$cluster_dir/node_1
	mkdir -p "$node_dir"
	start_process ydb-local "$YDBD_PATH" \
		sh -c 'cd "$1"; shift; exec "$@"' sh "$node_dir" \
		"$YDBD_PATH" server \
		"--yaml-config=$YDB_CONFIG_PATH" \
		--node=1 \
		"--log-file-name=$log_dir/ydbd.log" \
		"--grpc-port=$YDB_GRPC_PORT" \
		"--mon-port=$YDB_MONITORING_PORT" \
		"--ic-port=$YDB_INTERCONNECT_PORT" \
		--tiny-mode
}

start_ydb() {
	require_executable YDBD_PATH "$YDBD_PATH"
	if pid=$(owned_pid ydb-local); then
		printf 'ydb-local is already running (pid %s)\n' "$pid"
		return 0
	fi
	if curl --connect-timeout 1 --max-time 2 --fail --silent \
		"http://127.0.0.1:$YDB_MONITORING_PORT/monitoring/cluster" >/dev/null 2>&1; then
		fail "YDB monitoring port $YDB_MONITORING_PORT belongs to an unmanaged process"
	fi

	if test ! -f "$YDB_CONFIG_PATH"; then
		test -n "$YDB_LOCAL_LAUNCHER_PATH" ||
			fail 'YDB_LOCAL_LAUNCHER_PATH is required for the first deployment (or provide an existing YDB_CONFIG_PATH)'
		require_executable YDB_LOCAL_LAUNCHER_PATH "$YDB_LOCAL_LAUNCHER_PATH"
		mkdir -p "$YDB_RUNTIME_DIR"
		deploy_log=$log_dir/ydb-deploy.log
		if ! (
			cd "$YDB_RUNTIME_DIR"
			GRPC_PORT=$YDB_GRPC_PORT \
				MON_PORT=$YDB_MONITORING_PORT \
				IC_PORT=$YDB_INTERCONNECT_PORT \
				"$YDB_LOCAL_LAUNCHER_PATH" deploy \
				--fixed-ports \
				--suppress-version-check \
				--ydb-working-dir "$YDB_RUNTIME_DIR" \
				--ydb-binary-path "$YDBD_PATH"
		) >"$deploy_log" 2>&1; then
			tail -n 100 "$deploy_log" >&2 || true
			fail 'upstream local_ydb deployment failed'
		fi
	fi

	recipe=$YDB_RUNTIME_DIR/ydb_recipe.json
	if test -f "$recipe" && command -v jq >/dev/null 2>&1; then
		launcher_pid=$(jq -r '.nodes["1"].pid // empty' "$recipe")
		recipe_grpc=$(jq -r '.nodes["1"].grpc_port // empty' "$recipe")
		recipe_mon=$(jq -r '.nodes["1"].mon_port // empty' "$recipe")
		case "$launcher_pid" in
			''|*[!0-9]*) ;;
			*)
				launcher_command=$(process_command "$launcher_pid")
				case "$launcher_command" in
					*"$YDBD_PATH"*"$YDB_RUNTIME_DIR"*)
						if test "$recipe_grpc" = "$YDB_GRPC_PORT" && \
							test "$recipe_mon" = "$YDB_MONITORING_PORT"; then
							record_process ydb-local "$launcher_pid" "$YDBD_PATH"
							printf 'adopted upstream local_ydb process (pid %s)\n' "$launcher_pid"
							return 0
						fi
						printf 'upstream local_ydb selected grpc=%s mon=%s; restarting on grpc=%s mon=%s\n' \
							"$recipe_grpc" "$recipe_mon" "$YDB_GRPC_PORT" "$YDB_MONITORING_PORT"
						kill -TERM "$launcher_pid"
						attempt=1
						while test "$attempt" -le 150; do
							if ! kill -0 "$launcher_pid" 2>/dev/null; then
								break
							fi
							attempt=$((attempt + 1))
							sleep 0.1
						done
						kill -0 "$launcher_pid" 2>/dev/null &&
							fail "upstream local_ydb process $launcher_pid did not stop; it was not force-killed"
						;;
				esac
				;;
		esac
	fi
	start_ydb_direct
}

start_minio() {
	require_executable MINIO_PATH "$MINIO_PATH"
	mkdir -p "$data_dir/minio"
	start_process object-storage-local "$MINIO_PATH" env \
		MINIO_ROOT_USER="$S3_ACCESS_KEY_ID" \
		MINIO_ROOT_PASSWORD="$S3_SECRET_ACCESS_KEY" \
		"$MINIO_PATH" server "$data_dir/minio" \
		"--address=:$S3_API_PORT" "--console-address=:$S3_CONSOLE_PORT"
}

initialize_minio() {
	require_executable MC_PATH "$MC_PATH"
	mc_config=$runtime_root/mc
	mkdir -p "$mc_config"
	MC_CONFIG_DIR="$mc_config" "$MC_PATH" alias set sessionless-native \
		"$S3_ENDPOINT" "$S3_ACCESS_KEY_ID" "$S3_SECRET_ACCESS_KEY" >/dev/null
	MC_CONFIG_DIR="$mc_config" "$MC_PATH" mb --ignore-existing \
		"sessionless-native/$S3_BUCKET" >/dev/null
	printf 'object-storage-local bucket is ready: %s\n' "$S3_BUCKET"
}

start_queue() {
	require_executable JAVA_PATH "$JAVA_PATH"
	require_file ELASTICMQ_JAR "$ELASTICMQ_JAR"
	start_process queue-local "$ELASTICMQ_JAR" \
		"$JAVA_PATH" "-Dconfig.file=$repo_root/infra/local/elasticmq.conf" \
		-jar "$ELASTICMQ_JAR"
}

start_telegram_fake() {
	start_process telegram-fake "$repo_root/.build/bin/telegram-fake" env \
		PORT="$TELEGRAM_FAKE_PORT" TELEGRAM_FAKE_TOKEN="$TELEGRAM_FAKE_TOKEN" \
		"$repo_root/.build/bin/telegram-fake"
}

start_control_api() {
	start_process control-api "$repo_root/.build/bin/control-api" env \
		PORT="$SESSIONLESS_HTTP_PORT" "$repo_root/.build/bin/control-api"
}

start_telegram_sender() {
	start_process telegram-sender "$repo_root/.build/bin/telegram-sender" \
		"$repo_root/.build/bin/telegram-sender"
}

start_reconciler() {
	start_process reconciler "$repo_root/.build/bin/reconciler" \
		"$repo_root/.build/bin/reconciler"
}

run_migrations() {
	# shellcheck source=/dev/null
	. "$repo_root/scripts/local-ydb-readiness.sh"
	migration_log=$log_dir/ydb-migration.log
	attempt=1
	max_attempts=${YDB_MIGRATION_MAX_ATTEMPTS:-60}
	while test "$attempt" -le "$max_attempts"; do
		if make -C "$repo_root" migrate-local >"$migration_log" 2>&1; then
			cat "$migration_log"
			return 0
		fi
		classification=$(classify_ydb_startup_failure "$migration_log" "$log_dir/ydb-local.log")
		case "$classification" in
			retry-storage-pools|retry-local-dial)
				printf 'YDB query readiness pending (%s, attempt %d/%d)\n' \
					"$classification" "$attempt" "$max_attempts"
				;;
			*)
				cat "$migration_log" >&2
				fail "YDB migration failed ($classification)"
				;;
		esac
		attempt=$((attempt + 1))
		sleep "${YDB_MIGRATION_RETRY_DELAY_SECONDS:-1}"
	done
	cat "$migration_log" >&2
	fail "YDB did not become query-ready after $max_attempts attempts"
}

up() {
	test -n "$YDBD_PATH" || fail 'YDBD_PATH is required for Dockerless startup'
	build_apps
	start_ydb
	wait_http ydb-local "http://127.0.0.1:$YDB_MONITORING_PORT/monitoring/cluster"
	start_minio
	wait_http object-storage-local "$S3_ENDPOINT/minio/health/ready"
	initialize_minio
	start_queue
	wait_http queue-local "$QUEUE_ENDPOINT/?Action=ListQueues&Version=2012-11-05"
	start_telegram_fake
	wait_http telegram-fake "$TELEGRAM_API_BASE_URL/healthz"
	run_migrations
	printf 'Committing the explicit execution-placement cutover before application startup.\n'
	if ! make -C "$repo_root" partition-backfill; then
		printf '%s\n' 'Execution-placement cutover requires empty dispatch_outbox and worker_jobs.' >&2
		printf '%s\n' 'No data was deleted; inspect the existing local rows before retrying.' >&2
		fail 'execution-placement cutover is incomplete'
	fi
	start_control_api
	start_telegram_sender
	start_reconciler
	wait_http control-api "$SESSIONLESS_BASE_URL/readyz"
	make -C "$repo_root" dev-seed
	printf 'Sessionless Dockerless stand is ready. Runtime: %s\n' "$runtime_root"
}

down() {
	status=0
	for service in reconciler telegram-sender control-api telegram-fake queue-local object-storage-local ydb-local; do
		stop_process "$service" || status=1
	done
	for log_name in reconciler telegram-sender control-api telegram-fake queue-local object-storage-local ydb-local ydbd ydb-deploy ydb-migration worker-runtime; do
		bound_log_file "$log_dir/$log_name.log" || status=1
	done
	return "$status"
}

status() {
	result=0
	for service in ydb-local object-storage-local queue-local telegram-fake control-api telegram-sender reconciler; do
		if pid=$(owned_pid "$service"); then
			printf '%-22s running pid=%s\n' "$service" "$pid"
		else
			printf '%-22s stopped or unmanaged\n' "$service"
			result=1
		fi
	done
	return "$result"
}

logs() {
	for service in ydb-local object-storage-local queue-local telegram-fake control-api telegram-sender reconciler worker-runtime; do
		log_file=$log_dir/$service.log
		if test -f "$log_file"; then
			printf '\n==> %s <==\n' "$service"
			tail -n "${DOCKERLESS_LOG_TAIL:-150}" "$log_file"
		fi
	done
}

worker_once() {
	test -x "$repo_root/.build/bin/worker-runtime" || build_apps
	worker_scratch=$(mktemp -d "$scratch_dir/worker.XXXXXX")
	invocation_log=$(mktemp "$log_dir/worker-invocation.XXXXXX")
	trap 'rm -rf "$worker_scratch"; rm -f "$invocation_log"' EXIT HUP INT TERM
	WORKER_SCRATCH_ROOT=$worker_scratch
	export WORKER_SCRATCH_ROOT
	status=0
	"$repo_root/.build/bin/worker-runtime" >"$invocation_log" 2>&1 || status=$?
	cat "$invocation_log"
	cat "$invocation_log" >>"$log_dir/worker-runtime.log"
	bound_log_file "$log_dir/worker-runtime.log"
	rm -rf "$worker_scratch"
	rm -f "$invocation_log"
	trap - EXIT HUP INT TERM
	return "$status"
}

restart_service() {
	service=$1
	case "$service" in
		queue-local)
			stop_process queue-local
			start_queue
			wait_http queue-local "$QUEUE_ENDPOINT/?Action=ListQueues&Version=2012-11-05"
			;;
		reconciler)
			stop_process reconciler
			start_reconciler
			;;
		*) fail "restart supports only queue-local and reconciler, got: $service" ;;
	esac
}

main() {
	prepare_runtime
	load_versions
	configure_paths
	configure_environment
	command=${1:-}
	case "$command" in
		up) up ;;
		down) down ;;
		status) status ;;
		logs) logs ;;
		worker-once) worker_once ;;
		start)
			case "${2:-}" in
				queue-local) start_queue; wait_http queue-local "$QUEUE_ENDPOINT/?Action=ListQueues&Version=2012-11-05" ;;
				reconciler) start_reconciler ;;
				*) fail 'start supports only queue-local and reconciler' ;;
			esac
			;;
		stop)
			case "${2:-}" in
				queue-local|reconciler) stop_process "$2" ;;
				*) fail 'stop supports only queue-local and reconciler' ;;
			esac
			;;
		restart) restart_service "${2:-}" ;;
		*) fail 'usage: scripts/dockerless-local.sh {up|down|status|logs|worker-once|start SERVICE|stop SERVICE|restart SERVICE}' ;;
	esac
}

main "$@"
