#!/bin/sh
set -eu
unset WEB_LOGIN_PROVIDER WEB_LOGIN_PROVIDERS

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
test_root=$(mktemp -d "${TMPDIR:-/tmp}/sessionless-dockerless-policy.XXXXXX")
trap 'rm -rf "$test_root"' EXIT HUP INT TERM
# The copied runner canonicalizes its repo root. Keep fake process markers in
# the same physical namespace (macOS TMPDIR may have a trailing slash/symlink).
test_root=$(CDPATH= cd -- "$test_root" && pwd -P)

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

# Exercise the actual CLI's pre-effect denials. No BFF/model/DB is started and
# no existing binary or host port is used by these cases.
for mode in missing-origin missing-provider duplicate-provider reversed-provider unknown-provider cloud zero-port leading-zero-port oversized-port nondigit-port; do
	web_environment=local
	web_origin=https://web.localhost
	web_provider=yandex
	web_port=8083
	case "$mode" in
		missing-origin) web_origin= ;;
		missing-provider) web_provider= ;;
		duplicate-provider) web_provider=yandex,yandex ;;
		reversed-provider) web_provider=telegram,yandex ;;
		unknown-provider) web_provider=unknown ;;
		cloud) web_environment=cloud-dev ;;
		zero-port) web_port=0 ;;
		leading-zero-port) web_port=08083 ;;
		oversized-port) web_port=65536 ;;
		nondigit-port) web_port=not-a-port ;;
	esac
	if env -u WEB_LOGIN_PROVIDER PATH="$fake_bin:$PATH" SESSIONLESS_DOCKERLESS_ROOT="$test_root/web-$mode" \
		SESSIONLESS_ENVIRONMENT="$web_environment" WEB_BASE_URL="$web_origin" \
		WEB_LOGIN_PROVIDERS="$web_provider" WEB_PORT="$web_port" \
		"$runner" start web-bff >"$test_root/web-$mode.out" 2>&1; then
		fail "native Web accepted invalid explicit configuration: $mode"
	fi
	case "$mode" in
		missing-*) expected='requires explicit WEB_BASE_URL and WEB_LOGIN_PROVIDERS' ;;
		duplicate-provider|reversed-provider|unknown-provider) expected='WEB_LOGIN_PROVIDERS must be yandex, telegram, or yandex,telegram' ;;
		cloud) expected='requires SESSIONLESS_ENVIRONMENT=local' ;;
		*) expected='WEB_PORT must be a decimal port between 1 and 65535' ;;
	esac
	grep -F "$expected" "$test_root/web-$mode.out" >/dev/null ||
		fail "native Web denial did not identify $mode"
	test ! -e "$test_root/web-$mode/pids/web-bff.pid" ||
		fail "native Web denial acquired a process: $mode"
done
for retired in '' yandex telegram; do
	if PATH="$fake_bin:$PATH" SESSIONLESS_DOCKERLESS_ROOT="$test_root/web-retired" \
		SESSIONLESS_ENVIRONMENT=local WEB_BASE_URL=https://web.localhost \
		WEB_LOGIN_PROVIDER="$retired" WEB_LOGIN_PROVIDERS=yandex \
		"$runner" start web-bff >"$test_root/web-retired.out" 2>&1; then
		fail 'native Web accepted retired singular variable'
	fi
	grep -F 'remove WEB_LOGIN_PROVIDER; configure WEB_LOGIN_PROVIDERS explicitly' "$test_root/web-retired.out" >/dev/null ||
		fail 'native Web retired-variable denial missing'
	test ! -e "$test_root/web-retired/pids/web-bff.pid" || fail 'retired variable acquired a Web process'
done
test ! -e "$docker_calls" || fail 'Docker was invoked by native Web validation'

# A copied real runner exercises successful dispatch against explicitly fake
# commands. The peers neither bind sockets nor execute an application. The ps
# peer acknowledges only after the launch-argument trace is complete, so the
# check does not race the bounded background recorder.
web_fixture=$test_root/web-fixture
mkdir -p "$web_fixture/scripts" "$web_fixture/tools" "$web_fixture/.build/bin" "$web_fixture/fake-bin"
cp "$runner" "$web_fixture/scripts/dockerless-local.sh"
cp "$repo_root/tools/versions.env" "$web_fixture/tools/versions.env"
printf '#!/bin/sh\nexit 97\n' >"$web_fixture/.build/bin/web-bff"
cat >"$web_fixture/fake-bin/nohup" <<'STUB'
#!/bin/sh
printf '%s\n' "$@" >"$DOCKERLESS_POLICY_TRACE.pending"
mv "$DOCKERLESS_POLICY_TRACE.pending" "$DOCKERLESS_POLICY_TRACE"
STUB
cat >"$web_fixture/fake-bin/ps" <<'STUB'
#!/bin/sh
test -e "$DOCKERLESS_POLICY_TRACE" || exit 1
if test "${DOCKERLESS_POLICY_MODE:-normal}" = child-exit; then
	if test -e "$DOCKERLESS_POLICY_TRACE.observed"; then exit 1; fi
	: >"$DOCKERLESS_POLICY_TRACE.observed"
fi
printf '%s\n' "$DOCKERLESS_POLICY_MARKER"
STUB
cat >"$web_fixture/fake-bin/lsof" <<'STUB'
#!/bin/sh
if test "${DOCKERLESS_POLICY_MODE:-normal}" = foreign-ready; then printf '999999\n'; exit 0; fi
test -e "$DOCKERLESS_POLICY_TRACE" || exit 1
if test "${DOCKERLESS_POLICY_MODE:-normal}" = changed-listener && test -e "$DOCKERLESS_POLICY_READY"; then
	printf '999999\n'
else
	cat "$DOCKERLESS_POLICY_RUNTIME/pids/web-bff.pid"
fi
STUB
cat >"$web_fixture/fake-bin/curl" <<'STUB'
#!/bin/sh
printf '%s\n' "$@" >"$DOCKERLESS_POLICY_READY"
STUB
chmod +x "$web_fixture/.build/bin/web-bff" "$web_fixture/fake-bin/nohup" \
	"$web_fixture/fake-bin/ps" "$web_fixture/fake-bin/lsof" "$web_fixture/fake-bin/curl"
PATH="$web_fixture/fake-bin:$fake_bin:$PATH" SESSIONLESS_DOCKERLESS_ROOT="$web_fixture/runtime" \
	SESSIONLESS_ENVIRONMENT=local WEB_BASE_URL=https://web.localhost WEB_LOGIN_PROVIDERS=yandex \
	PORT=18080 WEB_PORT=18083 DOCKERLESS_POLICY_MARKER="$web_fixture/.build/bin/web-bff" \
	DOCKERLESS_POLICY_RUNTIME="$web_fixture/runtime" \
	DOCKERLESS_POLICY_TRACE="$web_fixture/launch" DOCKERLESS_POLICY_READY="$web_fixture/ready" \
	"$web_fixture/scripts/dockerless-local.sh" start web-bff >"$web_fixture/out" 2>&1 ||
	{ sed -n '1,12p' "$web_fixture/out" >&2; tail -n 8 "$web_fixture/runtime/logs/web-bff.log" >&2; fail 'native Web CLI did not reach its fake launch/readiness peers'; }
printf '%s\n' env PORT=18083 "$web_fixture/.build/bin/web-bff" >"$web_fixture/expected-launch"
cmp -s "$web_fixture/expected-launch" "$web_fixture/launch" ||
	fail 'native Web did not launch only the exact child with its dedicated port'
grep -F 'http://127.0.0.1:18083/readyz' "$web_fixture/ready" >/dev/null ||
	fail 'native Web readiness did not observe its own port'
test -f "$web_fixture/runtime/pids/web-bff.pid" && test -f "$web_fixture/runtime/pids/web-bff.marker" ||
	fail 'native Web launch omitted owned process metadata'
test ! -e "$docker_calls" || fail 'Docker was invoked by native Web launch'

for mode in foreign-ready child-exit changed-listener; do
	if PATH="$web_fixture/fake-bin:$fake_bin:$PATH" SESSIONLESS_DOCKERLESS_ROOT="$web_fixture/$mode" \
		SESSIONLESS_ENVIRONMENT=local WEB_BASE_URL=https://web.localhost WEB_LOGIN_PROVIDERS=yandex \
		WEB_PORT=18083 DOCKERLESS_POLICY_MODE="$mode" DOCKERLESS_POLICY_MARKER="$web_fixture/.build/bin/web-bff" \
		DOCKERLESS_POLICY_RUNTIME="$web_fixture/$mode" DOCKERLESS_POLICY_TRACE="$web_fixture/$mode-launch" \
		DOCKERLESS_POLICY_READY="$web_fixture/$mode-ready" \
		"$web_fixture/scripts/dockerless-local.sh" start web-bff >"$web_fixture/$mode.out" 2>&1; then
		fail "native Web accepted false readiness: $mode"
	fi
	case "$mode" in
		foreign-ready) expected='port belongs to another listener' ;;
		child-exit) expected='child exited or lost ownership before readiness' ;;
		changed-listener) expected='listener ownership changed during readiness' ;;
	esac
	grep -F "$expected" "$web_fixture/$mode.out" >/dev/null ||
		fail "native Web did not identify false readiness: $mode"
	if test "$mode" != changed-listener; then
		test ! -e "$web_fixture/$mode-ready" || fail "native Web probed HTTP without an owned listener: $mode"
	fi
done
test ! -e "$docker_calls" || fail 'Docker was invoked by false readiness checks'

# Optional leaf wiring must not turn legacy stand startup into Web activation.
grep -F 'dockerless-web-build: prepare web-stage' "$repo_root/Makefile" >/dev/null ||
	fail 'native Web build omits the normal embedded frontend build'
grep -F './scripts/dockerless-local.sh start web-bff' "$repo_root/Makefile" >/dev/null ||
	fail 'native Web Make entry does not use the owned lifecycle'
grep -F 'PORT="$web_port" "$repo_root/.build/bin/web-bff"' "$runner" >/dev/null ||
	fail 'native Web must override shared control-api PORT for its own child'
if sed -n '/^up() {/,/^}/p' "$runner" | grep -F 'start_web_bff' >/dev/null; then
	fail 'ordinary stand startup must not silently enable Web'
fi

# A stale optional-Web marker must receive the SAME no-signal ownership check.
mkdir -p "$test_root/stale-web/pids"
printf '%s\n' "$$" >"$test_root/stale-web/pids/web-bff.pid"
printf '%s\n' '/definitely/not/the/current/web-bff' >"$test_root/stale-web/pids/web-bff.marker"
if PATH="$fake_bin:$PATH" SESSIONLESS_DOCKERLESS_ROOT="$test_root/stale-web" \
	"$runner" stop web-bff >"$test_root/stale-web.out" 2>&1; then
	fail 'native Web stop accepted mismatched ownership metadata'
fi
kill -0 "$$" 2>/dev/null || fail 'native Web stop signalled an unrelated process'
grep -F 'refusing to signal stale or mismatched web-bff pid metadata' "$test_root/stale-web.out" >/dev/null ||
	fail 'native Web ownership denial was not actionable'
test ! -e "$docker_calls" || fail 'Docker was invoked by native Web stop'

printf 'Dockerless stand policy checks passed.\n'
