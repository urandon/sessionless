#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$repo_root"
. "$repo_root/tools/versions.env"
. "$repo_root/build/images.env"

test "$(uname -s)" = Linux || {
	printf '%s\n' 'the Linux rootless gate requires Linux' >&2
	exit 2
}
test "$(uname -m)" = x86_64 || {
	printf '%s\n' 'the pinned Linux rootless gate currently supports x86_64 only' >&2
	exit 2
}
test "$(id -u)" != 0 || {
	printf '%s\n' 'the Linux rootless gate must run as a non-root user' >&2
	exit 2
}

for command_name in awk curl go id journalctl sha256sum systemctl systemd-run tar; do
	command -v "$command_name" >/dev/null 2>&1 || {
		printf '%s is required\n' "$command_name" >&2
		exit 1
	}
done
: "${RUNNER_TEMP:?set an absolute private CI temporary directory}"
case "$RUNNER_TEMP" in
	/*) ;;
	*) printf '%s\n' 'RUNNER_TEMP must be absolute' >&2; exit 2 ;;
esac
test -d "$RUNNER_TEMP" && test -w "$RUNNER_TEMP" || {
	printf '%s\n' 'RUNNER_TEMP must be an existing writable directory' >&2
	exit 2
}

runner_uid=$(id -u)
expected_runtime_dir="/run/user/$runner_uid"
test "${XDG_RUNTIME_DIR:-}" = "$expected_runtime_dir" || {
	printf 'XDG_RUNTIME_DIR must be %s\n' "$expected_runtime_dir" >&2
	exit 2
}
test -d "$XDG_RUNTIME_DIR" && test -w "$XDG_RUNTIME_DIR" || {
	printf '%s\n' 'the systemd user runtime directory is unavailable' >&2
	exit 2
}
test "${DBUS_SESSION_BUS_ADDRESS:-}" = "unix:path=$XDG_RUNTIME_DIR/bus" || {
	printf '%s\n' 'DBUS_SESSION_BUS_ADDRESS does not select the user manager bus' >&2
	exit 2
}
systemctl --user show-environment >/dev/null

tool_root="$RUNNER_TEMP/sessionless-rootless-toolchain"
data_root="$RUNNER_TEMP/sessionless-rootless-data"
exec_root="$XDG_RUNTIME_DIR/sessionless-rootless-exec"
state_root="$XDG_RUNTIME_DIR/sessionless-rootless-state"
client_config="$tool_root/client-config"
daemon_config="$tool_root/daemon.json"
docker_archive="$tool_root/docker.tgz"
extras_archive="$tool_root/docker-rootless-extras.tgz"
docker_url="https://download.docker.com/linux/static/stable/x86_64/docker-${ATTACHED_WORKER_ROOTLESS_DOCKER_VERSION}.tgz"
extras_url="https://download.docker.com/linux/static/stable/x86_64/docker-rootless-extras-${ATTACHED_WORKER_ROOTLESS_DOCKER_VERSION}.tgz"
unit_name=sessionless-rootless-docker.service
service_started=false

test ! -e "$tool_root" && test ! -e "$data_root" && test ! -e "$exec_root" && test ! -e "$state_root" || {
	printf '%s\n' 'a rootless gate-owned path already exists' >&2
	exit 2
}
unit_load_state=$(systemctl --user show "$unit_name" --property=LoadState --value 2>/dev/null || true)
if test -e "$XDG_RUNTIME_DIR/docker.sock" || \
	{ test -n "$unit_load_state" && test "$unit_load_state" != not-found; }; then
	printf '%s\n' 'refusing to replace an existing rootless Docker socket or gate service' >&2
	exit 2
fi

cleanup_path() {
	case "$1" in
		"$RUNNER_TEMP"/sessionless-rootless-*) rm -rf -- "$1" ;;
		"$XDG_RUNTIME_DIR"/sessionless-rootless-*) rm -rf -- "$1" ;;
		*) printf 'refusing unexpected rootless cleanup target: %s\n' "$1" >&2; return 1 ;;
	esac
}

cleanup() {
	status=$?
	trap - EXIT HUP INT TERM
	if test "$service_started" = true; then
		systemctl --user stop "$unit_name" >/dev/null 2>&1 || true
		systemctl --user reset-failed "$unit_name" >/dev/null 2>&1 || true
	fi
	cleanup_path "$exec_root" || status=1
	cleanup_path "$state_root" || status=1
	cleanup_path "$data_root" || status=1
	cleanup_path "$tool_root" || status=1
	exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM

mkdir -m 700 "$tool_root" "$data_root" "$client_config"
printf '{}\n' >"$daemon_config"
chmod 600 "$daemon_config"
curl --fail --location --silent --show-error --retry 3 --output "$docker_archive" "$docker_url"
curl --fail --location --silent --show-error --retry 3 --output "$extras_archive" "$extras_url"
printf '%s  %s\n' "$ATTACHED_WORKER_ROOTLESS_DOCKER_SHA256" "$docker_archive" | sha256sum --check --strict
printf '%s  %s\n' "$ATTACHED_WORKER_ROOTLESS_EXTRAS_SHA256" "$extras_archive" | sha256sum --check --strict
tar -xzf "$docker_archive" -C "$tool_root"
tar -xzf "$extras_archive" -C "$tool_root"

docker_bin="$tool_root/docker/docker"
rootless_bin="$tool_root/docker-rootless-extras/dockerd-rootless.sh"
rootlesskit_bin="$tool_root/docker-rootless-extras/rootlesskit"
for executable in "$docker_bin" "$tool_root/docker/dockerd" "$rootless_bin" "$rootlesskit_bin"; do
	test -x "$executable" || {
		printf 'missing pinned rootless executable: %s\n' "$executable" >&2
		exit 1
	}
done

PATH="$tool_root/docker:$tool_root/docker-rootless-extras:$PATH"
export PATH
DOCKER_CONFIG="$client_config"
export DOCKER_CONFIG
test "$(dockerd --version | awk '{sub(/,$/, "", $3); print $3}')" = "$ATTACHED_WORKER_ROOTLESS_DOCKER_VERSION" || {
	printf '%s\n' 'pinned dockerd version mismatch' >&2
	exit 1
}
command -v newuidmap >/dev/null 2>&1 && command -v newgidmap >/dev/null 2>&1 || {
	printf '%s\n' 'newuidmap/newgidmap are required for rootless Docker' >&2
	exit 1
}
awk -F: -v user="$(id -un)" '$1 == user && $3 >= 65536 { found = 1 } END { exit !found }' /etc/subuid || {
	printf '%s\n' 'a subordinate UID range of at least 65536 must be configured' >&2
	exit 1
}
awk -F: -v user="$(id -un)" '$1 == user && $3 >= 65536 { found = 1 } END { exit !found }' /etc/subgid || {
	printf '%s\n' 'a subordinate GID range of at least 65536 must be configured' >&2
	exit 1
}

"$rootlesskit_bin" true
service_started=true
systemd-run --user \
	--unit="$unit_name" \
	--collect \
	--property=Delegate=yes \
	--property=KillMode=mixed \
	--property=TasksMax=infinity \
	--property=Type=exec \
	--setenv="PATH=$PATH" \
	--setenv="XDG_RUNTIME_DIR=$XDG_RUNTIME_DIR" \
	--setenv="DOCKERD_ROOTLESS_ROOTLESSKIT_STATE_DIR=$state_root" \
	"$rootless_bin" \
	"--config-file=$daemon_config" \
	"--data-root=$data_root" \
	"--exec-root=$exec_root" \
	"--host=unix://$XDG_RUNTIME_DIR/docker.sock"

docker_host="unix://$XDG_RUNTIME_DIR/docker.sock"
ready=false
attempt=0
while test "$attempt" -lt 60; do
	if "$docker_bin" --host "$docker_host" info >/dev/null 2>&1; then
		ready=true
		break
	fi
	attempt=$((attempt + 1))
	sleep 1
done
if test "$ready" != true; then
	printf '%s\n' 'pinned rootless Docker did not become ready' >&2
	journalctl --user-unit "$unit_name" --no-pager -n 80 >&2 || true
	exit 1
fi

client_version=$("$docker_bin" --host "$docker_host" version --format '{{.Client.Version}}')
server_version=$("$docker_bin" --host "$docker_host" version --format '{{.Server.Version}}')
test "$client_version" = "$ATTACHED_WORKER_ROOTLESS_DOCKER_VERSION" && \
	test "$server_version" = "$ATTACHED_WORKER_ROOTLESS_DOCKER_VERSION" || {
	printf 'unexpected Docker tuple: client=%s server=%s\n' "$client_version" "$server_version" >&2
	exit 1
}
security_options=$("$docker_bin" --host "$docker_host" info --format '{{json .SecurityOptions}}')
case "$security_options" in
	*'"name=rootless"'*) ;;
	*) printf 'rootless security option absent: %s\n' "$security_options" >&2; exit 1 ;;
esac
cgroup_version=$("$docker_bin" --host "$docker_host" info --format '{{.CgroupVersion}}')
test "$cgroup_version" = 2 || {
	printf '%s\n' 'rootless Docker must expose cgroup v2' >&2
	exit 1
}

"$docker_bin" --host "$docker_host" pull "$LOCAL_REGISTRY_IMAGE" >/dev/null
ATTACHED_WORKER_OCI_DOCKER_PATH="$docker_bin" \
ATTACHED_WORKER_OCI_DOCKER_HOST="$docker_host" \
ATTACHED_WORKER_OCI_BOUNDARY=linux-rootless \
	"$repo_root/scripts/test-attached-worker-oci.sh"

engine_id=$("$docker_bin" --host "$docker_host" info --format '{{.ID}}')
engine_arch=$("$docker_bin" --host "$docker_host" info --format '{{.Architecture}}')
engine_os=$("$docker_bin" --host "$docker_host" info --format '{{.OperatingSystem}}')
engine_kernel=$("$docker_bin" --host "$docker_host" info --format '{{.KernelVersion}}')
engine_storage=$("$docker_bin" --host "$docker_host" info --format '{{.Driver}}')
printf 'Linux rootless OCI gate passed: docker=%s rootless_extras_sha256=%s engine_id=%s architecture=%s os="%s" kernel="%s" cgroup=%s storage=%s runner_image=%s/%s\n' \
	"$server_version" "$ATTACHED_WORKER_ROOTLESS_EXTRAS_SHA256" "$engine_id" "$engine_arch" \
	"$engine_os" "$engine_kernel" "$cgroup_version" "$engine_storage" \
	"${ImageOS:-local}" "${ImageVersion:-local}"
