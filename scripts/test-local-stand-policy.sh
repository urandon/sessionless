#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
test_root=$(mktemp -d "${TMPDIR:-/tmp}/sessionless-local-stand-policy.XXXXXX")
trap 'rm -rf "$test_root"' EXIT HUP INT TERM

fail() {
	printf 'local stand policy: %s\n' "$*" >&2
	exit 1
}

if ! command -v jq >/dev/null 2>&1; then
	fail 'jq is required to inspect the resolved Compose configuration'
fi
if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
	fail 'Docker Compose is required; install/start the project Docker runtime, then re-run make local-stand-policy-test'
fi

compose_config="$test_root/compose-config.json"
if ! docker compose --project-name sessionless-dev --profile '*' \
	--file "$repo_root/compose.yaml" config --format json >"$compose_config"; then
	fail 'Docker Compose could not render compose.yaml; run make compose-config for the underlying error'
fi

logging_violations=$(jq -r '
  .services
  | to_entries[]
  | select(
      .value.logging.driver != "json-file"
      or (.value.logging.options["max-size"] | tostring) != "10m"
      or (.value.logging.options["max-file"] | tostring) != "3"
    )
  | "\(.key): expected json-file/max-size=10m/max-file=3, got \(.value.logging // {})"
' "$compose_config")
if test -n "$logging_violations"; then
	printf '%s\n' "$logging_violations" >&2
	fail 'every resolved Compose service must use bounded json-file logging'
fi

helper="$repo_root/scripts/local-ydb-readiness.sh"
dev_up="$repo_root/scripts/dev-up.sh"
test -f "$helper" || fail 'scripts/local-ydb-readiness.sh is missing'

# shellcheck source=/dev/null
. "$helper"

storage_pool_log="$test_root/storage-pool.log"
raw_localhost_dial_log="$test_root/raw-localhost-dial.log"
escaped_localhost_dial_log="$test_root/escaped-localhost-dial.log"
ipv4_dial_log="$test_root/ipv4-dial.log"
ipv6_dial_log="$test_root/ipv6-dial.log"
remote_dial_log="$test_root/remote-dial.log"
generic_deadline_log="$test_root/generic-deadline.log"
boot_reason_log="$test_root/boot-reason.log"
boot_disks_log="$test_root/boot-disks.log"
unrelated_log="$test_root/unrelated.log"
empty_log="$test_root/empty.log"
printf '%s\n' "database doesn't have storage pools" >"$storage_pool_log"
printf '%s\n' 'failed to dial "localhost:2136": context deadline exceeded' >"$raw_localhost_dial_log"
printf '%s\n' 'failed to dial \"localhost:2136\": context deadline exceeded' >"$escaped_localhost_dial_log"
printf '%s\n' 'failed to dial "127.0.0.1:2136": context deadline exceeded' >"$ipv4_dial_log"
printf '%s\n' 'failed to dial "[::1]:2136": context deadline exceeded' >"$ipv6_dial_log"
printf '%s\n' 'failed to dial "ydb.example.test:2136": context deadline exceeded' >"$remote_dial_log"
printf '%s\n' 'context deadline exceeded' >"$generic_deadline_log"
printf '%s\n' 'tablet failed: ReasonBootBSError while opening local database' >"$boot_reason_log"
printf '%s\n' 'storage startup failed: NumUnconnectedDisks=1' >"$boot_disks_log"
printf '%s\n' 'authentication denied while applying schema' >"$unrelated_log"
: >"$empty_log"

test "$(classify_ydb_startup_failure "$storage_pool_log" "$empty_log")" = retry-storage-pools ||
	fail 'storage-pool initialization must be classified as retryable'
test "$(classify_ydb_startup_failure "$raw_localhost_dial_log" "$empty_log")" = retry-local-dial ||
	fail 'raw localhost SDK dial timeout must be classified as retry-local-dial'
test "$(classify_ydb_startup_failure "$escaped_localhost_dial_log" "$empty_log")" = retry-local-dial ||
	fail 'slog-escaped localhost SDK dial timeout must be classified as retry-local-dial'
test "$(classify_ydb_startup_failure "$ipv4_dial_log" "$empty_log")" = retry-local-dial ||
	fail '127.0.0.1 SDK dial timeout must be classified as retry-local-dial'
test "$(classify_ydb_startup_failure "$ipv6_dial_log" "$empty_log")" = retry-local-dial ||
	fail '[::1] SDK dial timeout must be classified as retry-local-dial'
test "$(classify_ydb_startup_failure "$remote_dial_log" "$empty_log")" = fatal ||
	fail 'a remote SDK dial timeout must remain fatal'
test "$(classify_ydb_startup_failure "$generic_deadline_log" "$empty_log")" = fatal ||
	fail 'a generic context deadline must remain fatal'
test "$(classify_ydb_startup_failure "$raw_localhost_dial_log" "$boot_reason_log")" = boot-storage-failure ||
	fail 'ReasonBootBSError must override a transient migration symptom'
test "$(classify_ydb_startup_failure "$storage_pool_log" "$boot_disks_log")" = boot-storage-failure ||
	fail 'NumUnconnectedDisks must override a transient migration symptom'
test "$(classify_ydb_startup_failure "$unrelated_log" "$empty_log")" = fatal ||
	fail 'unrelated migration errors must be fatal'

run_migration_fixture() {
	mode=$1
	state_file=$2
	service_log=$3
	output_file=$4
	migration_max_attempts=${5:-3}
	local_dial_max_attempts=${6-__production_default__}
	: >"$state_file"
	if (
		export SESSIONLESS_DEV_UP_LIBRARY=1
		export YDB_MIGRATION_MAX_ATTEMPTS="$migration_max_attempts"
		if test "$local_dial_max_attempts" = __production_default__; then
			unset YDB_LOCAL_DIAL_MAX_ATTEMPTS
		else
			export YDB_LOCAL_DIAL_MAX_ATTEMPTS="$local_dial_max_attempts"
		fi
		export YDB_MIGRATION_RETRY_DELAY_SECONDS=0
		export FIXTURE_MODE="$mode"
		export FIXTURE_STATE_FILE="$state_file"
		export FIXTURE_SERVICE_LOG="$service_log"
		# shellcheck source=/dev/null
		. "$dev_up"
		make() {
			attempts=$(wc -l <"$FIXTURE_STATE_FILE" | tr -d ' ')
			attempts=$((attempts + 1))
			printf '%s\n' "$attempts" >>"$FIXTURE_STATE_FILE"
			case "$FIXTURE_MODE" in
				retry-storage-then-success)
					if test "$attempts" -eq 1; then
						printf '%s\n' "database doesn't have storage pools" >&2
						return 1
					fi
					printf '%s\n' 'migration completed'
					return 0
					;;
				retry-dial-then-success)
					if test "$attempts" -eq 1; then
						printf '%s\n' 'failed to dial \"localhost:2136\": context deadline exceeded' >&2
						return 1
					fi
					printf '%s\n' 'migration completed'
					return 0
					;;
				persistent-local-dial)
					printf '%s\n' 'failed to dial \"localhost:2136\": context deadline exceeded' >&2
					return 1
					;;
				persistent-storage-pools)
					printf '%s\n' "database doesn't have storage pools" >&2
					return 1
					;;
				boot-storage)
					printf '%s\n' "database doesn't have storage pools" >&2
					return 1
					;;
				generic-deadline)
					printf '%s\n' 'context deadline exceeded' >&2
					return 1
					;;
				remote-dial)
					printf '%s\n' 'failed to dial "ydb.example.test:2136": context deadline exceeded' >&2
					return 1
					;;
				unrelated)
					printf '%s\n' 'authentication denied while applying schema' >&2
					return 1
					;;
				*) return 2 ;;
			esac
		}
		compose() {
			case " $* " in
				*' logs '*' ydb-local '*) cat "$FIXTURE_SERVICE_LOG" ;;
				*) return 0 ;;
			esac
		}
		sleep() { :; }
		run_migrations
	) >"$output_file" 2>&1; then
		return 0
	fi
	return 1
}

retry_state="$test_root/retry.state"
retry_output="$test_root/retry.out"
run_migration_fixture retry-storage-then-success "$retry_state" "$empty_log" "$retry_output" ||
	fail 'a transient storage-pool initialization failure did not recover'
test "$(wc -l <"$retry_state" | tr -d ' ')" -eq 2 ||
	fail 'transient storage-pool initialization must retry exactly once in the fixture'
grep -F 'ydb-local database is initializing storage pools' "$retry_output" >/dev/null ||
	fail 'transient retry did not emit an actionable readiness message'

dial_retry_state="$test_root/dial-retry.state"
dial_retry_output="$test_root/dial-retry.out"
run_migration_fixture retry-dial-then-success "$dial_retry_state" "$empty_log" "$dial_retry_output" 5 3 ||
	fail 'a transient local SDK dial timeout did not recover'
test "$(wc -l <"$dial_retry_state" | tr -d ' ')" -eq 2 ||
	fail 'transient local SDK dial timeout must retry exactly once in the fixture'
grep -F 'ydb-local loopback SDK endpoint is not ready (dial attempt 1/3)' "$dial_retry_output" >/dev/null ||
	fail 'local SDK dial retry did not emit a reason-specific bounded message'

dial_bound_state="$test_root/dial-bound.state"
dial_bound_output="$test_root/dial-bound.out"
if run_migration_fixture persistent-local-dial "$dial_bound_state" "$empty_log" "$dial_bound_output" 5 2; then
	fail 'persistent local SDK dial timeout was incorrectly accepted'
fi
test "$(wc -l <"$dial_bound_state" | tr -d ' ')" -eq 2 ||
	fail 'persistent local SDK dial timeout must stop at its dedicated bound'
grep -F 'after 2 dial attempts' "$dial_bound_output" >/dev/null ||
	fail 'persistent local SDK dial timeout omitted its dedicated bound'

default_dial_bound_state="$test_root/default-dial-bound.state"
default_dial_bound_output="$test_root/default-dial-bound.out"
if run_migration_fixture persistent-local-dial "$default_dial_bound_state" "$empty_log" "$default_dial_bound_output" 60; then
	fail 'persistent local SDK dial timeout was incorrectly accepted with the production defaults'
fi
test "$(wc -l <"$default_dial_bound_state" | tr -d ' ')" -eq 3 ||
	fail 'the production local SDK dial bound must stop after exactly three attempts'
grep -F 'after 3 dial attempts' "$default_dial_bound_output" >/dev/null ||
	fail 'the production local SDK dial bound must remain three independently of the global bound'

storage_bound_state="$test_root/storage-bound.state"
storage_bound_output="$test_root/storage-bound.out"
if run_migration_fixture persistent-storage-pools "$storage_bound_state" "$empty_log" "$storage_bound_output" 3 2; then
	fail 'persistent storage-pool initialization was incorrectly accepted'
fi
test "$(wc -l <"$storage_bound_state" | tr -d ' ')" -eq 3 ||
	fail 'persistent storage-pool initialization must stop at the migration bound'
grep -F 'after 3 attempts' "$storage_bound_output" >/dev/null ||
	fail 'persistent storage-pool initialization omitted its migration bound'

boot_state="$test_root/boot.state"
boot_output="$test_root/boot.out"
if run_migration_fixture boot-storage "$boot_state" "$boot_reason_log" "$boot_output"; then
	fail 'boot-storage failure was incorrectly retried or accepted'
fi
test "$(wc -l <"$boot_state" | tr -d ' ')" -eq 1 ||
	fail 'boot-storage failure must stop after the first migration attempt'

fatal_state="$test_root/fatal.state"
fatal_output="$test_root/fatal.out"
if run_migration_fixture unrelated "$fatal_state" "$empty_log" "$fatal_output"; then
	fail 'unrelated migration failure was incorrectly retried or accepted'
fi
test "$(wc -l <"$fatal_state" | tr -d ' ')" -eq 1 ||
	fail 'unrelated migration failure must stop after the first attempt'

generic_state="$test_root/generic.state"
generic_output="$test_root/generic.out"
if run_migration_fixture generic-deadline "$generic_state" "$empty_log" "$generic_output"; then
	fail 'generic context deadline was incorrectly retried or accepted'
fi
test "$(wc -l <"$generic_state" | tr -d ' ')" -eq 1 ||
	fail 'generic context deadline must stop after the first attempt'

remote_state="$test_root/remote.state"
remote_output="$test_root/remote.out"
if run_migration_fixture remote-dial "$remote_state" "$empty_log" "$remote_output"; then
	fail 'remote SDK dial timeout was incorrectly retried or accepted'
fi
test "$(wc -l <"$remote_state" | tr -d ' ')" -eq 1 ||
	fail 'remote SDK dial timeout must stop after the first attempt'

invalid_dial_bound_state="$test_root/invalid-dial-bound.state"
invalid_dial_bound_output="$test_root/invalid-dial-bound.out"
if run_migration_fixture persistent-local-dial "$invalid_dial_bound_state" "$empty_log" "$invalid_dial_bound_output" 5 0; then
	fail 'zero YDB_LOCAL_DIAL_MAX_ATTEMPTS was incorrectly accepted'
fi
test ! -s "$invalid_dial_bound_state" ||
	fail 'invalid local SDK dial bound must fail before a migration attempt'
grep -F 'YDB_LOCAL_DIAL_MAX_ATTEMPTS must be a positive integer' "$invalid_dial_bound_output" >/dev/null ||
	fail 'invalid local SDK dial bound omitted its validation error'

fake_bin="$test_root/bin"
mkdir -p "$fake_bin"
cat >"$fake_bin/docker" <<'EOF'
#!/bin/sh
set -eu
printf '%s\n' "$*" >>"$FAKE_DOCKER_LOG"
case " $* " in
	*' compose '*' logs '*' ydb-local '*)
		printf '%s\n' 'tablet failed: ReasonBootBSError while opening local database'
		;;
esac
exit 0
EOF
cat >"$fake_bin/curl" <<'EOF'
#!/bin/sh
printf '%s\n' "$*" >>"$FAKE_CURL_LOG"
exit 0
EOF
cat >"$fake_bin/sleep" <<'EOF'
#!/bin/sh
exit 0
EOF
cat >"$fake_bin/make" <<'EOF'
#!/bin/sh
printf '%s\n' "$*" >>"$FAKE_MAKE_LOG"
if test "$*" = migrate-local; then
	printf '%s\n' "database doesn't have storage pools" >&2
	exit 1
fi
exit 0
EOF
chmod 755 "$fake_bin/docker" "$fake_bin/curl" "$fake_bin/sleep" "$fake_bin/make"

export FAKE_DOCKER_LOG="$test_root/dev-up-docker.log"
export FAKE_MAKE_LOG="$test_root/dev-up-make.log"
export FAKE_CURL_LOG="$test_root/dev-up-curl.log"
: >"$FAKE_DOCKER_LOG"
: >"$FAKE_MAKE_LOG"
: >"$FAKE_CURL_LOG"
if PATH="$fake_bin:$PATH" sh "$dev_up" >"$test_root/dev-up.out" 2>&1; then
	fail 'dev-up unexpectedly continued after YDB boot-storage failure'
fi
expected_stop='compose --project-name sessionless-dev stop control-api telegram-sender reconciler'
grep -Fx "$expected_stop" "$FAKE_DOCKER_LOG" >/dev/null ||
	fail 'dev-up did not quiesce existing schema-dependent services before readiness checks'
stop_line=$(grep -n -Fx "$expected_stop" "$FAKE_DOCKER_LOG" | cut -d: -f1)
infra_line=$(grep -n ' up --build --detach ydb-local object-storage-local queue-local telegram-fake$' "$FAKE_DOCKER_LOG" | cut -d: -f1)
test -n "$stop_line" && test -n "$infra_line" && test "$stop_line" -lt "$infra_line" ||
	fail 'schema-dependent services must be stopped before infrastructure starts'
if grep -E ' up .*control-api| up .*telegram-sender| up .*reconciler' "$FAKE_DOCKER_LOG" >/dev/null; then
	fail 'dev-up started schema-dependent services after YDB boot-storage failure'
fi
test "$(cat "$FAKE_MAKE_LOG")" = migrate-local ||
	fail 'dev-up must stop after the query-backed YDB readiness probe fails'
grep -F 'No volumes were deleted.' "$test_root/dev-up.out" >/dev/null ||
	fail 'boot-storage failure did not emit volume-preserving recovery guidance'
test -s "$FAKE_CURL_LOG" || fail 'dev-up did not perform bounded liveness probes'
while IFS= read -r curl_args; do
	printf '%s\n' "$curl_args" | grep -F -- '--connect-timeout 2' >/dev/null ||
		fail "liveness probe omitted connect timeout: $curl_args"
	printf '%s\n' "$curl_args" | grep -F -- '--max-time 5' >/dev/null ||
		fail "liveness probe omitted total request timeout: $curl_args"
done <"$FAKE_CURL_LOG"

reset_docker_log="$test_root/dev-reset-docker.log"
export FAKE_DOCKER_LOG="$reset_docker_log"
: >"$reset_docker_log"
if PATH="$fake_bin:$PATH" sh "$repo_root/scripts/dev-reset.sh" >"$test_root/reset-unconfirmed.out" 2>&1; then
	fail 'dev-reset accepted a missing confirmation'
fi
test ! -s "$reset_docker_log" || fail 'dev-reset invoked Docker without exact confirmation'
if CONFIRM_LOCAL_RESET=wrong PATH="$fake_bin:$PATH" \
	sh "$repo_root/scripts/dev-reset.sh" >"$test_root/reset-wrong.out" 2>&1; then
	fail 'dev-reset accepted an incorrect confirmation'
fi
test ! -s "$reset_docker_log" || fail 'dev-reset invoked Docker with an incorrect confirmation'
CONFIRM_LOCAL_RESET=sessionless-dev PATH="$fake_bin:$PATH" \
	sh "$repo_root/scripts/dev-reset.sh" >"$test_root/reset-confirmed.out" 2>&1
test "$(cat "$reset_docker_log")" = \
	'compose --project-name sessionless-dev down --volumes --remove-orphans' ||
	fail 'dev-reset changed its exact-confirmation deletion boundary'

printf '%s\n' 'local stand operational policy invariants passed'
