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

transient_log="$test_root/transient.log"
boot_reason_log="$test_root/boot-reason.log"
boot_disks_log="$test_root/boot-disks.log"
unrelated_log="$test_root/unrelated.log"
empty_log="$test_root/empty.log"
printf '%s\n' "database doesn't have storage pools" >"$transient_log"
printf '%s\n' 'tablet failed: ReasonBootBSError while opening local database' >"$boot_reason_log"
printf '%s\n' 'storage startup failed: NumUnconnectedDisks=1' >"$boot_disks_log"
printf '%s\n' 'authentication denied while applying schema' >"$unrelated_log"
: >"$empty_log"

test "$(classify_ydb_startup_failure "$transient_log" "$empty_log")" = retry ||
	fail 'storage-pool initialization must be classified as retryable'
test "$(classify_ydb_startup_failure "$transient_log" "$boot_reason_log")" = boot-storage-failure ||
	fail 'ReasonBootBSError must override a transient migration symptom'
test "$(classify_ydb_startup_failure "$transient_log" "$boot_disks_log")" = boot-storage-failure ||
	fail 'NumUnconnectedDisks must override a transient migration symptom'
test "$(classify_ydb_startup_failure "$unrelated_log" "$empty_log")" = fatal ||
	fail 'unrelated migration errors must be fatal'

run_migration_fixture() {
	mode=$1
	state_file=$2
	service_log=$3
	output_file=$4
	: >"$state_file"
	if (
		export SESSIONLESS_DEV_UP_LIBRARY=1
		export YDB_MIGRATION_MAX_ATTEMPTS=3
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
				retry-then-success)
					if test "$attempts" -eq 1; then
						printf '%s\n' "database doesn't have storage pools" >&2
						return 1
					fi
					printf '%s\n' 'migration completed'
					return 0
					;;
				boot-storage)
					printf '%s\n' "database doesn't have storage pools" >&2
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
run_migration_fixture retry-then-success "$retry_state" "$empty_log" "$retry_output" ||
	fail 'a transient storage-pool initialization failure did not recover'
test "$(wc -l <"$retry_state" | tr -d ' ')" -eq 2 ||
	fail 'transient storage-pool initialization must retry exactly once in the fixture'
grep -F 'ydb-local database is initializing storage pools' "$retry_output" >/dev/null ||
	fail 'transient retry did not emit an actionable readiness message'

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
