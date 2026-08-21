#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$repo_root"
. "$repo_root/build/images.env"

for command_name in curl docker git jq tar; do
  command -v "$command_name" >/dev/null 2>&1 || {
    printf '%s is required\n' "$command_name" >&2
    exit 1
  }
done

test_root=$(mktemp -d "${TMPDIR:-/tmp}/sessionless-image-reproducibility.XXXXXX")
suffix=$$
registry_name="sessionless-repro-registry-$suffix"
builder_a="sessionless-repro-a-$suffix"
builder_b="sessionless-repro-b-$suffix"

cleanup() {
  docker buildx rm "$builder_a" >/dev/null 2>&1 || true
  docker buildx rm "$builder_b" >/dev/null 2>&1 || true
  docker rm -f "$registry_name" >/dev/null 2>&1 || true
  rm -rf "$test_root"
}
trap cleanup EXIT HUP INT TERM

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

source_sha=$(git rev-parse HEAD)
source_tree=$(git rev-parse HEAD^{tree})
built_at=$(git show -s --format=%cI HEAD)
source_date_epoch=$(git show -s --format=%ct HEAD)
context_dir="$test_root/context"
mkdir -p "$context_dir"
git archive --format=tar HEAD >"$test_root/context.tar"
tar -xf "$test_root/context.tar" -C "$context_dir"

docker run --detach --rm \
  --name "$registry_name" \
  --publish 127.0.0.1::5000 \
  "$LOCAL_REGISTRY_IMAGE" >/dev/null
registry_port=$(docker port "$registry_name" 5000/tcp | sed -n 's/.*://p' | tail -1)
case "$registry_port" in
  ''|*[!0-9]*) printf '%s\n' 'could not resolve local registry port' >&2; exit 1 ;;
esac
registry="127.0.0.1:$registry_port"

build_room() {
  room=$1
  builder=$2
  metadata_dir="$test_root/$room/metadata"
  mkdir -p "$metadata_dir"

  docker buildx create \
    --name "$builder" \
    --driver docker-container \
    --driver-opt "image=$BUILDKIT_IMAGE" >/dev/null
  docker buildx inspect "$builder" --bootstrap >/dev/null

  VERSION="$source_sha" \
    COMMIT="$source_sha" \
    BUILT_AT="$built_at" \
    SOURCE_DATE_EPOCH="$source_date_epoch" \
    IMAGE_PLATFORM=linux/amd64 \
    DOCKER_BUILD_CACHE=none \
    IMAGE_METADATA_DIR="$metadata_dir" \
    IMAGE_LOCAL_NAMESPACE="$registry/$room" \
    IMAGE_LOCAL_TAG="$source_sha" \
    IMAGE_BUILDER="$builder" \
    IMAGE_BUILD_CONTEXT="$context_dir" \
    IMAGE_REQUIRE_CLEAN_CHECKOUT="${IMAGE_REQUIRE_CLEAN_CHECKOUT:-0}" \
    IMAGE_VERIFY_TOOLCHAIN=1 \
    "$repo_root/scripts/build-runtime-images.sh"

  for name in control-api web-bff reconciler telegram-sender worker-runtime; do
    docker push "$registry/$room/$name:$source_sha" >/dev/null
  done
}

build_room a "$builder_a"
build_room b "$builder_b"

capture_evidence() {
  room=$1
  name=$2
  repository="$room/$name"
  metadata_dir="$test_root/$room/metadata"
  manifest_file="$test_root/$room/$name.manifest.json"
  config_file="$test_root/$room/$name.config.json"
  evidence_file="$test_root/$room/$name.evidence.json"

  curl --fail --silent --show-error \
    --header 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
    "http://$registry/v2/$repository/manifests/$source_sha" >"$manifest_file"
  config_digest=$(jq -er '.config.digest' "$manifest_file")
  curl --fail --silent --show-error \
    "http://$registry/v2/$repository/blobs/$config_digest" >"$config_file"
  manifest_size=$(wc -c <"$manifest_file" | tr -d ' ')
  manifest_digest="sha256:$(sha256_file "$manifest_file")"
  input_digest=$(sed -n '1p' "$metadata_dir/$name.inputs.sha256")
  buildx_manifest_digest=$(jq -er '."containerimage.digest"' "$metadata_dir/$name.json")

  jq -S -n \
    --arg image "$name" \
    --arg source_sha "$source_sha" \
    --arg source_tree "$source_tree" \
    --arg platform linux/amd64 \
    --arg input_digest "$input_digest" \
    --arg buildx_manifest_digest "$buildx_manifest_digest" \
    --arg manifest_digest "$manifest_digest" \
    --argjson manifest_size "$manifest_size" \
    --slurpfile manifest "$manifest_file" \
    --slurpfile config "$config_file" \
    '{
      schema_version: 1,
      image: $image,
      source_sha: $source_sha,
      source_tree: $source_tree,
      platform: $platform,
      input_digest: $input_digest,
      buildx_manifest_digest: $buildx_manifest_digest,
      manifest: {
        media_type: $manifest[0].mediaType,
        digest: $manifest_digest,
        size: $manifest_size
      },
      config_digest: $manifest[0].config.digest,
      diff_ids: $config[0].rootfs.diff_ids,
      layers: ($manifest[0].layers | map({media_type: .mediaType, digest: .digest, size: .size}))
    }' >"$evidence_file"
}

evidence_path=${IMAGE_REPRODUCIBILITY_EVIDENCE_PATH:-.build/image-reproducibility.json}
candidate_metadata_dir=${IMAGE_METADATA_DIR:-.build/image-metadata}
mkdir -p "$(dirname "$evidence_path")" "$candidate_metadata_dir"
jq -S -n \
  --arg source_sha "$source_sha" \
  --arg source_tree "$source_tree" \
  '{schema_version: 1, source_sha: $source_sha, source_tree: $source_tree, platform: "linux/amd64", images: {}}' \
  >"$evidence_path"

for name in control-api web-bff reconciler telegram-sender worker-runtime; do
  capture_evidence a "$name"
  capture_evidence b "$name"
  if ! cmp -s "$test_root/a/$name.evidence.json" "$test_root/b/$name.evidence.json"; then
    printf 'reproducibility mismatch for %s:\n' "$name" >&2
    diff -u "$test_root/a/$name.evidence.json" "$test_root/b/$name.evidence.json" >&2 || true
    exit 1
  fi

  jq --arg name "$name" \
    --slurpfile evidence "$test_root/b/$name.evidence.json" \
    '.images[$name] = $evidence[0]' "$evidence_path" >"$test_root/evidence.next"
  mv "$test_root/evidence.next" "$evidence_path"

  docker tag "$registry/b/$name:$source_sha" "sessionless/$name:dev"
  cp "$test_root/b/metadata/$name.json" "$candidate_metadata_dir/$name.json"
  cp "$test_root/b/metadata/$name.source-sha" "$candidate_metadata_dir/$name.source-sha"
  cp "$test_root/b/metadata/$name.inputs.json" "$candidate_metadata_dir/$name.inputs.json"
  cp "$test_root/b/metadata/$name.inputs.sha256" "$candidate_metadata_dir/$name.inputs.sha256"
done

jq -S . "$evidence_path" >"$test_root/evidence.canonical"
mv "$test_root/evidence.canonical" "$evidence_path"
jq -e '
  .schema_version == 1 and
  .platform == "linux/amd64" and
  (.images | keys | sort) == ["control-api", "reconciler", "telegram-sender", "web-bff", "worker-runtime"] and
  all(.images[]; (.manifest.digest | test("^sha256:[0-9a-f]{64}$")))
' "$evidence_path" >/dev/null

printf 'five-image clean-room reproducibility evidence written to %s\n' "$evidence_path"
