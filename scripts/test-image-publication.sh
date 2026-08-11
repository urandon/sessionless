#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
test_root=$(mktemp -d "${TMPDIR:-/tmp}/sessionless-image-publication.XXXXXX")
trap 'rm -rf "$test_root"' EXIT HUP INT TERM

source_sha=$(git -C "$repo_root" rev-parse HEAD)
other_sha=0000000000000000000000000000000000000000
build_digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
conflicting_config_digest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
candidate_config_digest=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
remote_manifest_digest=sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
metadata_dir="$test_root/metadata"
fake_bin="$test_root/bin"
fake_state="$test_root/state"
fake_log="$test_root/docker.log"
manifest_path="$test_root/deployment-images.json"
mkdir -p "$metadata_dir" "$fake_bin" "$fake_state"

for name in control-api reconciler telegram-sender worker-runtime; do
  jq -n --arg digest "$build_digest" \
    '{"containerimage.digest": $digest}' >"$metadata_dir/$name.json"
  printf '%s\n' "$source_sha" >"$metadata_dir/$name.source-sha"
done

cat >"$fake_bin/docker" <<'EOF'
#!/bin/sh
set -eu
printf '%s\n' "$*" >>"$FAKE_DOCKER_LOG"

if test "$1" = image && test "$2" = inspect; then
  case "$5" in
    '{{.Os}}/{{.Architecture}}') printf '%s\n' linux/amd64 ;;
    '{{.Id}}') printf '%s\n' "$FAKE_CANDIDATE_CONFIG_DIGEST" ;;
    *) printf 'unsupported image inspect format: %s\n' "$5" >&2; exit 2 ;;
  esac
  exit 0
fi
if test "$1" = tag; then
  exit 0
fi
if test "$1" = push; then
  key=$(printf '%s' "$2" | tr '/:' '__')
  : >"$FAKE_DOCKER_STATE/$key"
  exit 0
fi
if test "$1" = buildx && test "$2" = imagetools && test "$3" = inspect; then
  reference=$4
  key=$(printf '%s' "$reference" | tr '/:' '__')
  case "$FAKE_REMOTE_MODE" in
    same)
      config_digest=$FAKE_CANDIDATE_CONFIG_DIGEST
      ;;
    mismatch)
      config_digest=$FAKE_CONFLICTING_CONFIG_DIGEST
      ;;
    absent)
      if test -f "$FAKE_DOCKER_STATE/$key"; then
        config_digest=$FAKE_CANDIDATE_CONFIG_DIGEST
      else
        printf '%s\n' 'manifest unknown' >&2
        exit 1
      fi
      ;;
    error)
      printf '%s\n' 'registry authentication unavailable' >&2
      exit 1
      ;;
    *)
      printf 'unsupported FAKE_REMOTE_MODE: %s\n' "$FAKE_REMOTE_MODE" >&2
      exit 2
      ;;
  esac
  case "${5:-}" in
    --format)
      jq -n --arg digest "$FAKE_REMOTE_MANIFEST_DIGEST" '{digest: $digest}'
      ;;
    --raw)
      jq -n --arg config "$config_digest" '{config: {digest: $config}}'
      ;;
    *)
      printf 'unsupported imagetools inspect arguments: %s\n' "$*" >&2
      exit 2
      ;;
  esac
  exit 0
fi

printf 'unexpected docker invocation: %s\n' "$*" >&2
exit 2
EOF
chmod 755 "$fake_bin/docker"

run_publish() {
  mode=$1
  PATH="$fake_bin:$PATH" \
    FAKE_DOCKER_LOG="$fake_log" \
    FAKE_DOCKER_STATE="$fake_state" \
    FAKE_REMOTE_MODE="$mode" \
    FAKE_CANDIDATE_CONFIG_DIGEST="$candidate_config_digest" \
    FAKE_CONFLICTING_CONFIG_DIGEST="$conflicting_config_digest" \
    FAKE_REMOTE_MANIFEST_DIGEST="$remote_manifest_digest" \
    YANDEX_CONTAINER_REGISTRY_ID=crptestregistry \
    CLOUD_IMAGE_TAG="$source_sha" \
    CLOUD_IMAGE_METADATA_DIR="$metadata_dir" \
    CLOUD_IMAGE_MANIFEST_PATH="$manifest_path" \
    SOURCE_BUILT_AT=2026-08-10T00:00:00Z \
    "$repo_root/scripts/publish-cloud-images.sh"
}

if CLOUD_DEV_BACKEND_CONFIG=unused \
  CLOUD_DEV_TFVARS=unused \
  CLOUD_IMAGE_TAG="$other_sha" \
  "$repo_root/scripts/cloud-images.sh" >"$test_root/tag-mismatch.out" 2>&1; then
  printf '%s\n' 'cloud-images accepted a tag that does not match HEAD' >&2
  exit 1
fi
if ! grep -q 'must match the checked-out commit' "$test_root/tag-mismatch.out"; then
  printf '%s\n' 'cloud-images did not explain the checkout/tag mismatch' >&2
  exit 1
fi

printf '%s\n' "$other_sha" >"$metadata_dir/control-api.source-sha"
: >"$fake_log"
if run_publish same >"$test_root/source-mismatch.out" 2>&1; then
  printf '%s\n' 'publisher accepted build provenance for another commit' >&2
  exit 1
fi
if test -s "$fake_log"; then
  printf '%s\n' 'publisher contacted Docker before rejecting source provenance' >&2
  exit 1
fi
printf '%s\n' "$source_sha" >"$metadata_dir/control-api.source-sha"

: >"$fake_log"
if run_publish mismatch >"$test_root/digest-mismatch.out" 2>&1; then
  printf '%s\n' 'publisher overwrote a conflicting commit tag' >&2
  exit 1
fi
if grep -q '^push ' "$fake_log"; then
  printf '%s\n' 'publisher pushed before rejecting a conflicting commit tag' >&2
  exit 1
fi

: >"$fake_log"
run_publish same >/dev/null
if grep -q '^push ' "$fake_log"; then
  printf '%s\n' 'publisher pushed an already matching commit tag' >&2
  exit 1
fi
if ! grep -q -- ' --raw$' "$fake_log"; then
  printf '%s\n' 'publisher did not read the raw registry manifest for config identity' >&2
  exit 1
fi

rm -f "$fake_state"/*
: >"$fake_log"
run_publish absent >/dev/null
push_count=$(grep -c '^push ' "$fake_log")
if test "$push_count" -ne 4; then
  printf 'expected four first-publication pushes, got %s\n' "$push_count" >&2
  exit 1
fi
jq -e \
  --arg sha "$source_sha" \
  --arg digest "$remote_manifest_digest" \
  '.source_sha == $sha and (.images | length) == 4 and all(.images[]; .digest == $digest)' \
  "$manifest_path" >/dev/null

rm -f "$fake_state"/*
: >"$fake_log"
if run_publish error >"$test_root/inspect-error.out" 2>&1; then
  printf '%s\n' 'publisher treated an unknown registry error as an absent tag' >&2
  exit 1
fi
if grep -q '^push ' "$fake_log"; then
  printf '%s\n' 'publisher pushed after an unsafe registry inspection failure' >&2
  exit 1
fi

printf '%s\n' 'image publication invariants passed'
