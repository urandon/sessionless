#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
test_root=$(mktemp -d "${TMPDIR:-/tmp}/sessionless-dockerless-policy.XXXXXX")
trap 'rm -rf "$test_root"' EXIT HUP INT TERM

fail() {
	printf 'dockerless stand policy: %s\n' "$*" >&2
	exit 1
}

runner=$repo_root/scripts/dockerless-local.sh
e2e_runner=$repo_root/scripts/e2e-local.sh
test -x "$runner" || fail 'scripts/dockerless-local.sh must be executable'
sh -n "$runner" || fail 'scripts/dockerless-local.sh is not valid POSIX shell'
sh -n "$e2e_runner" || fail 'scripts/e2e-local.sh is not valid POSIX shell'

if grep -F 'docker compose' "$runner" >/dev/null; then
	fail 'the Dockerless lifecycle script must never invoke Docker Compose'
fi
grep -F 'SESSIONLESS_E2E_ORCHESTRATOR' "$repo_root/test/e2e/slice_test.go" >/dev/null ||
	fail 'the E2E suite does not expose an orchestrator boundary'
grep -F 'YDBD_PATH is required for Dockerless startup' "$runner" >/dev/null ||
	fail 'Dockerless startup must require explicit YDBD_PATH intent'
grep -F 'dockerless-up:' "$repo_root/Makefile" >/dev/null ||
	fail 'Makefile does not expose dockerless-up'
grep -F 'e2e-local-dockerless:' "$repo_root/Makefile" >/dev/null ||
	fail 'Makefile does not expose e2e-local-dockerless'
grep -F 'GRPC_PORT=$YDB_GRPC_PORT' "$runner" >/dev/null ||
	fail 'upstream local_ydb deploy does not receive the requested gRPC port'
grep -F 'cd "$YDB_RUNTIME_DIR"' "$runner" >/dev/null ||
	fail 'upstream local_ydb deploy can pollute the repository root'
grep -F 'make -C "$repo_root" partition-backfill' "$runner" >/dev/null ||
	fail 'Dockerless startup omits the mandatory execution-placement cutover'
grep -F 'grpc://127.0.0.1:' "$e2e_runner" >/dev/null ||
	fail 'Dockerless E2E does not pin YDB to numeric loopback'
grep -F 'grpc://127.0.0.1:2136/local' "$repo_root/Makefile" >/dev/null ||
	fail 'local integration does not default YDB to numeric loopback'
if grep -F 'grpc://localhost:2136/local' "$repo_root/test/localintegration/stand_test.go" >/dev/null; then
	fail 'local integration tests retain a proxy-sensitive localhost YDB default'
fi
grep -F 'QUEUE_ENDPOINT="http://127.0.0.1:' "$e2e_runner" >/dev/null ||
	fail 'Dockerless E2E does not pin its queue to numeric loopback'

fake_bin=$test_root/bin
mkdir -p "$fake_bin"
docker_calls=$test_root/docker-calls
printf '#!/bin/sh\nprintf "unexpected docker call\\n" >>"%s"\nexit 97\n' "$docker_calls" >"$fake_bin/docker"
chmod +x "$fake_bin/docker"

if PATH="$fake_bin:$PATH" SESSIONLESS_DOCKERLESS_ROOT="$test_root/runtime" \
	"$runner" up >"$test_root/missing-ydbd.out" 2>&1; then
	fail 'Dockerless startup accepted a missing YDBD_PATH'
fi
grep -F 'YDBD_PATH is required' "$test_root/missing-ydbd.out" >/dev/null ||
	fail 'missing YDBD_PATH did not produce an actionable error'
test ! -e "$docker_calls" || fail 'Docker was invoked while rejecting missing YDBD_PATH'

mkdir -p "$test_root/stale/pids"
printf '%s\n' "$$" >"$test_root/stale/pids/control-api.pid"
printf '%s\n' '/definitely/not/the/current/shell' >"$test_root/stale/pids/control-api.marker"
if PATH="$fake_bin:$PATH" SESSIONLESS_DOCKERLESS_ROOT="$test_root/stale" \
	"$runner" down >"$test_root/stale.out" 2>&1; then
	fail 'Dockerless down accepted mismatched PID ownership metadata'
fi
kill -0 "$$" 2>/dev/null || fail 'mismatched PID metadata signalled an unrelated process'
grep -F 'refusing to signal stale or mismatched control-api pid metadata' "$test_root/stale.out" >/dev/null ||
	fail 'mismatched PID ownership did not fail closed'
test ! -e "$docker_calls" || fail 'Docker was invoked by Dockerless down'

bounded_root=$test_root/bounded
mkdir -p "$bounded_root/logs"
dd if=/dev/zero of="$bounded_root/logs/control-api.log" bs=256 count=2 2>/dev/null
PATH="$fake_bin:$PATH" SESSIONLESS_DOCKERLESS_ROOT="$bounded_root" \
	DOCKERLESS_LOG_MAX_BYTES=128 "$runner" down >"$test_root/bounded.out" 2>&1 ||
	fail 'Dockerless down failed while bounding an owned log file'
bounded_bytes=$(wc -c <"$bounded_root/logs/control-api.log" | tr -d ' ')
test "$bounded_bytes" -eq 128 ||
	fail "bounded log size = $bounded_bytes, want 128"
test ! -e "$docker_calls" || fail 'Docker was invoked while bounding Dockerless logs'

printf 'Dockerless stand policy checks passed.\n'
