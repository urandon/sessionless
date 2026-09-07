#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$repo_root"
. "$repo_root/build/images.env"

for command_name in curl go perl tar; do
  command -v "$command_name" >/dev/null 2>&1 || {
    printf '%s is required\n' "$command_name" >&2
    exit 1
  }
done

: "${ATTACHED_WORKER_OCI_DOCKER_PATH:?set the canonical absolute Docker CLI path}"
: "${ATTACHED_WORKER_OCI_DOCKER_HOST:?set the explicit unix:// Docker endpoint}"
: "${ATTACHED_WORKER_OCI_BOUNDARY:?set darwin-vm or linux-rootless}"

case "$ATTACHED_WORKER_OCI_DOCKER_PATH" in
  /*) ;;
  *) printf '%s\n' 'ATTACHED_WORKER_OCI_DOCKER_PATH must be absolute' >&2; exit 2 ;;
esac
case "$ATTACHED_WORKER_OCI_DOCKER_HOST" in
  unix:///*) ;;
  *) printf '%s\n' 'ATTACHED_WORKER_OCI_DOCKER_HOST must be an explicit unix:// endpoint' >&2; exit 2 ;;
esac
case "$ATTACHED_WORKER_OCI_BOUNDARY" in
  darwin-vm|linux-rootless) ;;
  *) printf '%s\n' 'ATTACHED_WORKER_OCI_BOUNDARY must be darwin-vm or linux-rootless' >&2; exit 2 ;;
esac

docker_cli() {
  perl -e '$seconds = shift; alarm($seconds); exec @ARGV or die "exec failed\n"' 30 \
    "$ATTACHED_WORKER_OCI_DOCKER_PATH" --host "$ATTACHED_WORKER_OCI_DOCKER_HOST" "$@"
}

mkdir -p "$repo_root/.build/tmp"
test_root=$(mktemp -d "$repo_root/.build/tmp/attached-worker-oci.XXXXXX")
suffix=$$
registry_name="sessionless-aw05b-registry-$suffix"
installation_id="real-engine-gate-$suffix"
fixture_tag=''
fixture_digest=''

cleanup() {
  status=$?
  trap - EXIT HUP INT TERM
  if ! owned=$(docker_cli container ls --all --quiet \
    --filter "label=dev.sessionless.attached-worker.profile=sessionless.oci.docker.v1" \
    --filter "label=dev.sessionless.attached-worker.installation=$installation_id" 2>/dev/null); then
    printf '%s\n' 'could not enumerate test-owned containers during cleanup' >&2
    status=1
    owned=''
  fi
  for id in $owned; do
    case "$id" in
      *[!0-9a-f]*|'')
        printf '%s\n' 'refusing malformed owned-container cleanup output' >&2
        status=1
        ;;
      ????????????????????????????????????????????????????????????????)
        docker_cli container rm --force --volumes -- "$id" >/dev/null 2>&1 || true ;;
      *)
        printf '%s\n' 'refusing malformed owned-container cleanup output' >&2
        status=1
        ;;
    esac
  done
  docker_cli container rm --force --volumes "$registry_name" >/dev/null 2>&1 || true
  if test -n "$fixture_tag"; then
    docker_cli image rm --force "$fixture_tag" >/dev/null 2>&1 || true
  fi
  if test -n "$fixture_digest"; then
    docker_cli image rm --force "$fixture_digest" >/dev/null 2>&1 || true
  fi

  if docker_cli info >/dev/null 2>&1; then
    if ! remaining=$(docker_cli container ls --all --quiet \
      --filter "label=dev.sessionless.attached-worker.profile=sessionless.oci.docker.v1" \
      --filter "label=dev.sessionless.attached-worker.installation=$installation_id" 2>/dev/null); then
      printf '%s\n' 'could not verify test-owned container cleanup' >&2
      status=1
    elif test -n "$remaining"; then
      printf '%s\n' 'test-owned containers remain after cleanup' >&2
      status=1
    fi
    if docker_cli container inspect "$registry_name" >/dev/null 2>&1; then
      printf '%s\n' 'ephemeral registry container remains after cleanup' >&2
      status=1
    fi
    if test -n "$fixture_tag" && docker_cli image inspect "$fixture_tag" >/dev/null 2>&1; then
      printf '%s\n' 'fixture image tag remains after cleanup' >&2
      status=1
    fi
    if test -n "$fixture_digest" && docker_cli image inspect "$fixture_digest" >/dev/null 2>&1; then
      printf '%s\n' 'fixture image digest remains after cleanup' >&2
      status=1
    fi
  else
    printf '%s\n' 'Docker Engine unavailable for cleanup verification' >&2
    status=1
  fi
  case "$test_root" in
    "$repo_root/.build/tmp/attached-worker-oci."*)
      rm -rf "$test_root" || status=1
      test ! -e "$test_root" || status=1
      ;;
    *) printf '%s\n' 'refusing unexpected OCI test-root cleanup target' >&2; status=1 ;;
  esac
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM

engine_id=$(docker_cli info --format '{{.ID}}')
engine_arch=$(docker_cli info --format '{{.Architecture}}')
case "$engine_arch" in
  arm64|aarch64) go_arch=arm64 ;;
  amd64|x86_64) go_arch=amd64 ;;
  *) printf 'unsupported engine architecture: %s\n' "$engine_arch" >&2; exit 1 ;;
esac

docker_cli image inspect "$LOCAL_REGISTRY_IMAGE" >/dev/null
GOOS=linux GOARCH="$go_arch" CGO_ENABLED=0 go build -trimpath \
  -o "$test_root/probe" ./test/fixtures/attached-worker-oci-probe
tar -cf "$test_root/rootfs.tar" --files-from /dev/null

docker_cli run --detach --rm --name "$registry_name" \
  --log-driver none \
  --publish 127.0.0.1::5000 \
  --tmpfs /var/lib/registry:rw,nosuid,nodev,noexec,size=64m \
  "$LOCAL_REGISTRY_IMAGE" >/dev/null
registry_port=$(docker_cli port "$registry_name" 5000/tcp | sed -n 's/.*://p' | tail -1)
case "$registry_port" in
  ''|*[!0-9]*) printf '%s\n' 'could not resolve local registry port' >&2; exit 1 ;;
esac
registry_ready=false
attempt=0
while test "$attempt" -lt 50; do
  if curl --connect-timeout 1 --max-time 1 --fail --silent --show-error \
    "http://127.0.0.1:$registry_port/v2/" >/dev/null 2>&1; then
    registry_ready=true
    break
  fi
  attempt=$((attempt + 1))
  sleep 0.1
done
if test "$registry_ready" != true; then
  printf '%s\n' 'local OCI fixture registry did not become ready' >&2
  exit 1
fi

fixture_tag="127.0.0.1:$registry_port/sessionless-aw05b-fixture:local"
docker_cli image import --platform "linux/$go_arch" "$test_root/rootfs.tar" "$fixture_tag" >/dev/null
docker_cli push "$fixture_tag" >/dev/null
fixture_digest=$(docker_cli image inspect --format '{{index .RepoDigests 0}}' "$fixture_tag")
case "$fixture_digest" in
  *@sha256:????????????????????????????????????????????????????????????????) ;;
  *) printf '%s\n' 'fixture image did not resolve to an immutable digest' >&2; exit 1 ;;
esac

mkdir "$test_root/docker-config"
chmod 700 "$test_root/docker-config"

SESSIONLESS_OCI_REAL_ENGINE=1 \
SESSIONLESS_OCI_DOCKER_PATH="$ATTACHED_WORKER_OCI_DOCKER_PATH" \
SESSIONLESS_OCI_CLI_CONFIG_DIR="$test_root/docker-config" \
SESSIONLESS_OCI_HOST="$ATTACHED_WORKER_OCI_DOCKER_HOST" \
SESSIONLESS_OCI_ENGINE_ID="$engine_id" \
SESSIONLESS_OCI_INSTALLATION_ID="$installation_id" \
SESSIONLESS_OCI_BOUNDARY="$ATTACHED_WORKER_OCI_BOUNDARY" \
SESSIONLESS_OCI_IMAGE="$fixture_digest" \
SESSIONLESS_OCI_PROBE="$test_root/probe" \
go test -race -count=1 -run '^TestRealEngineIsolationMatrix$' -timeout=2m ./internal/attachedworkeroci
