#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
test_root=$(mktemp -d "${TMPDIR:-/tmp}/sessionless-image-publication.XXXXXX")
trap 'rm -rf "$test_root"' EXIT HUP INT TERM

source_sha=$(git -C "$repo_root" rev-parse HEAD)
other_sha=0000000000000000000000000000000000000000
conflicting_config_digest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
other_manifest_digest=sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
metadata_dir="$test_root/metadata"
fake_bin="$test_root/bin"
fake_state="$test_root/state"
fake_log="$test_root/docker.log"
manifest_path="$test_root/deployment-images.json"
receipt_path="$test_root/deployment-images.receipt.json"
candidate_config_file="$test_root/candidate-config.json"
candidate_manifest_file="$test_root/candidate-manifest.json"
candidate_registry_file="$test_root/candidate-registry.json"
mkdir -p "$metadata_dir" "$fake_bin" "$fake_state"

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

jq -cS -n '{
  architecture: "amd64",
  os: "linux",
  rootfs: {type: "layers", diff_ids: []}
}' >"$candidate_config_file"
candidate_config_digest="sha256:$(sha256_file "$candidate_config_file")"
candidate_config_size=$(wc -c <"$candidate_config_file" | tr -d ' ')
jq -cS -n \
  --arg config_digest "$candidate_config_digest" \
  --argjson config_size "$candidate_config_size" \
  '{
    schemaVersion: 2,
    mediaType: "application/vnd.docker.distribution.manifest.v2+json",
    config: {
      mediaType: "application/vnd.docker.container.image.v1+json",
      size: $config_size,
      digest: $config_digest
    },
    layers: []
  }' >"$candidate_manifest_file"
build_digest="sha256:$(sha256_file "$candidate_manifest_file")"
remote_manifest_digest=$build_digest
jq -S -n \
  --arg source_sha "$source_sha" \
  '{
    schema_version: 1,
    host: "127.0.0.1:5000",
    container: "",
    network: "",
    namespace: "transport",
    source_sha: $source_sha
  }' >"$candidate_registry_file"

for name in control-api web-bff reconciler telegram-sender worker-runtime; do
  jq -n --arg digest "$build_digest" \
    '{"containerimage.digest": $digest}' >"$metadata_dir/$name.json"
  printf '%s\n' "$source_sha" >"$metadata_dir/$name.source-sha"
  printf '%s\n' "$build_digest" >"$metadata_dir/$name.registry-manifest-digest"
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
if test "$1" = buildx && test "$2" = build; then
  if test "${SOURCE_DATE_EPOCH:-}" != "$FAKE_EXPECTED_SOURCE_DATE_EPOCH"; then
    printf 'docker did not inherit SOURCE_DATE_EPOCH: expected %s, got %s\n' \
      "$FAKE_EXPECTED_SOURCE_DATE_EPOCH" "${SOURCE_DATE_EPOCH:-<unset>}" >&2
    exit 2
  fi
  metadata_file=
  while test "$#" -gt 0; do
    if test "$1" = --metadata-file; then
      metadata_file=$2
      shift 2
    else
      shift
    fi
  done
  if test -z "$metadata_file"; then
    printf '%s\n' 'buildx invocation did not provide --metadata-file' >&2
    exit 2
  fi
  jq -n --arg digest "$FAKE_BUILD_DIGEST" \
    '{"containerimage.digest": $digest}' >"$metadata_file"
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
if test "$1" = buildx && test "$2" = imagetools && test "$3" = create; then
  metadata_file=
  tagged_reference=
  source_reference=
  shift 3
  while test "$#" -gt 0; do
    case "$1" in
      --prefer-index=false|--progress=plain) shift ;;
      --metadata-file) metadata_file=$2; shift 2 ;;
      --tag) tagged_reference=$2; shift 2 ;;
      *) source_reference=$1; shift ;;
    esac
  done
  case "$source_reference" in
    127.0.0.1:5000/transport/*@"$FAKE_BUILD_DIGEST") ;;
    *) printf 'unexpected registry-native source: %s\n' "$source_reference" >&2; exit 2 ;;
  esac
  if test -z "$metadata_file" || test -z "$tagged_reference"; then
    printf '%s\n' 'imagetools create omitted metadata or target tag' >&2
    exit 2
  fi
  jq -n --arg digest "$FAKE_BUILD_DIGEST" \
    '{"containerimage.descriptor": {digest: $digest}}' >"$metadata_file"
  key=$(printf '%s' "$tagged_reference" | tr '/:' '__')
  : >"$FAKE_DOCKER_STATE/$key"
  exit 0
fi
if test "$1" = buildx && test "$2" = imagetools && test "$3" = inspect; then
  reference=$4
  key=$(printf '%s' "$reference" | tr '/:' '__')
  case "$reference" in
    *@sha256:*) immutable_reference=1 ;;
    *) immutable_reference=0 ;;
  esac
  case "$FAKE_REMOTE_MODE" in
    same)
      config_digest=$FAKE_CANDIDATE_CONFIG_DIGEST
      ;;
    mismatch)
      config_digest=$FAKE_CONFLICTING_CONFIG_DIGEST
      ;;
    absent)
      if test "$immutable_reference" -eq 1 || test -f "$FAKE_DOCKER_STATE/$key"; then
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

cat >"$fake_bin/curl" <<'EOF'
#!/bin/sh
set -eu

headers_file=
url=
while test "$#" -gt 0; do
  case "$1" in
    --dump-header) headers_file=$2; shift 2 ;;
    --header) shift 2 ;;
    --fail|--silent|--show-error) shift ;;
    *) url=$1; shift ;;
  esac
done
case "$url" in
  */manifests/*)
    if test -n "$headers_file"; then
      printf 'HTTP/1.1 200 OK\r\nDocker-Content-Digest: %s\r\n\r\n' \
        "$FAKE_BUILD_DIGEST" >"$headers_file"
    fi
    cat "$FAKE_CANDIDATE_MANIFEST"
    ;;
  */blobs/*)
    cat "$FAKE_CANDIDATE_CONFIG"
    ;;
  *) printf 'unexpected curl URL: %s\n' "$url" >&2; exit 2 ;;
esac
EOF
chmod 755 "$fake_bin/curl"

expected_source_date_epoch=$(git -C "$repo_root" show -s --format=%ct HEAD)
: >"$fake_log"
(
  unset SOURCE_DATE_EPOCH
  PATH="$fake_bin:$PATH" \
    FAKE_DOCKER_LOG="$fake_log" \
    FAKE_EXPECTED_SOURCE_DATE_EPOCH="$expected_source_date_epoch" \
    FAKE_BUILD_DIGEST="$build_digest" \
    FAKE_CANDIDATE_MANIFEST="$candidate_manifest_file" \
    FAKE_CANDIDATE_CONFIG="$candidate_config_file" \
    IMAGE_METADATA_DIR="$metadata_dir" \
    IMAGE_PLATFORM=linux/amd64 \
    IMAGE_EXPORTER_MODE=registry \
    DOCKER_BUILD_CACHE=none \
    "$repo_root/scripts/build-runtime-images.sh"
)
build_count=$(grep -c '^buildx build ' "$fake_log")
if test "$build_count" -ne 5; then
  printf 'expected five reproducible image builds, got %s\n' "$build_count" >&2
  exit 1
fi

run_publish() {
  mode=$1
  attempt=${2:-1}
  PATH="$fake_bin:$PATH" \
    FAKE_DOCKER_LOG="$fake_log" \
    FAKE_DOCKER_STATE="$fake_state" \
    FAKE_BUILD_DIGEST="$build_digest" \
    FAKE_CANDIDATE_MANIFEST="$candidate_manifest_file" \
    FAKE_CANDIDATE_CONFIG="$candidate_config_file" \
    FAKE_REMOTE_MODE="$mode" \
    FAKE_CANDIDATE_CONFIG_DIGEST="$candidate_config_digest" \
    FAKE_CONFLICTING_CONFIG_DIGEST="$conflicting_config_digest" \
    FAKE_REMOTE_MANIFEST_DIGEST="${FAKE_REMOTE_MANIFEST_DIGEST_OVERRIDE:-$remote_manifest_digest}" \
    YANDEX_CONTAINER_REGISTRY_ID=crptestregistry \
    CLOUD_IMAGE_TAG="$source_sha" \
    CLOUD_IMAGE_METADATA_DIR="$metadata_dir" \
    CLOUD_IMAGE_CANDIDATE_REGISTRY_PATH="$candidate_registry_file" \
    CLOUD_IMAGE_MANIFEST_PATH="$manifest_path" \
    CLOUD_IMAGE_RECEIPT_PATH="$receipt_path" \
    SOURCE_BUILT_AT=2026-08-10T00:00:00Z \
    SOURCE_RUN_ID=publication-test \
    SOURCE_RUN_ATTEMPT="$attempt" \
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

mv "$metadata_dir/control-api.registry-manifest-digest" \
  "$metadata_dir/control-api.registry-manifest-digest.missing"
: >"$fake_log"
if run_publish same >"$test_root/missing-canonical-digest.out" 2>&1; then
  printf '%s\n' 'publisher accepted missing canonical registry manifest evidence' >&2
  exit 1
fi
if test -s "$fake_log"; then
  printf '%s\n' 'publisher contacted Docker before rejecting missing manifest evidence' >&2
  exit 1
fi
mv "$metadata_dir/control-api.registry-manifest-digest.missing" \
  "$metadata_dir/control-api.registry-manifest-digest"

printf '%s\n' "$other_manifest_digest" \
  >"$metadata_dir/control-api.registry-manifest-digest"
: >"$fake_log"
if run_publish same >"$test_root/canonical-digest-mismatch.out" 2>&1; then
  printf '%s\n' 'publisher accepted mismatched canonical registry manifest evidence' >&2
  exit 1
fi
if test -s "$fake_log"; then
  printf '%s\n' 'publisher contacted Docker before rejecting manifest evidence mismatch' >&2
  exit 1
fi
printf '%s\n' "$build_digest" >"$metadata_dir/control-api.registry-manifest-digest"

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
if grep -q '^buildx imagetools create ' "$fake_log"; then
  printf '%s\n' 'publisher copied before rejecting a conflicting commit tag' >&2
  exit 1
fi

: >"$fake_log"
run_publish same >/dev/null
if grep -q '^buildx imagetools create ' "$fake_log"; then
  printf '%s\n' 'publisher copied an already matching commit tag' >&2
  exit 1
fi
if ! grep -q -- "@${remote_manifest_digest} --raw$" "$fake_log"; then
  printf '%s\n' 'publisher did not bind raw inspection to the immutable manifest digest' >&2
  exit 1
fi
if grep -q -- ":${source_sha} --raw$" "$fake_log"; then
  printf '%s\n' 'publisher inspected a raw manifest through the mutable commit tag' >&2
  exit 1
fi

: >"$fake_log"
if FAKE_REMOTE_MANIFEST_DIGEST_OVERRIDE="$other_manifest_digest" \
  run_publish same >"$test_root/manifest-mismatch.out" 2>&1; then
  printf '%s\n' 'publisher accepted a different existing manifest with the same config' >&2
  exit 1
fi
if grep -q '^buildx imagetools create ' "$fake_log"; then
  printf '%s\n' 'publisher copied before rejecting an existing manifest mismatch' >&2
  exit 1
fi
unset FAKE_REMOTE_MANIFEST_DIGEST_OVERRIDE
rm -f "$fake_state"/*
: >"$fake_log"
run_publish absent >/dev/null
copy_count=$(grep -c '^buildx imagetools create ' "$fake_log")
if test "$copy_count" -ne 5; then
  printf 'expected five first-publication registry copies, got %s\n' "$copy_count" >&2
  exit 1
fi
jq -e \
  --arg sha "$source_sha" \
  --arg digest "$remote_manifest_digest" \
  '.schema_version == 2 and .source.sha == $sha and .build.platform == "linux/amd64" and
   (.images | length) == 5 and
   .images["web-bff"].reference == "cr.yandex/crptestregistry/web-bff@" + $digest and
   all(.images[]; .manifest_digest == $digest)' \
  "$manifest_path" >/dev/null

cp "$manifest_path" "$test_root/first-deployment-images.json"
cp "$receipt_path" "$test_root/first-publication-receipt.json"
run_publish same 2 >/dev/null
if ! cmp -s "$test_root/first-deployment-images.json" "$manifest_path"; then
  printf '%s\n' 'same-SHA publication did not reproduce the deployment manifest byte-for-byte' >&2
  exit 1
fi
if cmp -s "$test_root/first-publication-receipt.json" "$receipt_path"; then
  printf '%s\n' 'publication receipts did not retain run-specific attempt metadata' >&2
  exit 1
fi

EXPECTED_SOURCE_SHA="$source_sha" EXPECTED_REGISTRY_ID=crptestregistry \
  "$repo_root/scripts/cloud-image-tfvars.sh" "$manifest_path" >"$test_root/images.tfvars.json"
jq -e --arg digest "$remote_manifest_digest" \
  '.web_image_ref == "cr.yandex/crptestregistry/web-bff@" + $digest' \
  "$test_root/images.tfvars.json" >/dev/null

rm -f "$fake_state"/*
: >"$fake_log"
if run_publish error >"$test_root/inspect-error.out" 2>&1; then
  printf '%s\n' 'publisher treated an unknown registry error as an absent tag' >&2
  exit 1
fi
if grep -q '^buildx imagetools create ' "$fake_log"; then
  printf '%s\n' 'publisher copied after an unsafe registry inspection failure' >&2
  exit 1
fi

printf '%s\n' 'image publication invariants passed'
