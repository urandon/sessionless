#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$repo_root"
. "$repo_root/build/images.env"

version=${VERSION:-dev}
commit=${COMMIT:-$(git rev-parse HEAD)}
built_at=${BUILT_AT:-$(git show -s --format=%cI HEAD)}
source_date_epoch=${SOURCE_DATE_EPOCH:-$(git show -s --format=%ct HEAD)}
export SOURCE_DATE_EPOCH="$source_date_epoch"
platform=${IMAGE_PLATFORM:-}
cache_mode=${DOCKER_BUILD_CACHE:-none}
cache_suffix=$(printf '%s' "${DOCKER_CACHE_SCOPE_SUFFIX:-local}" | tr -c 'A-Za-z0-9_.-' '-')
metadata_dir=${IMAGE_METADATA_DIR:-.build/image-metadata}
local_namespace=${IMAGE_LOCAL_NAMESPACE:-sessionless}
local_tag=${IMAGE_LOCAL_TAG:-dev}
builder=${IMAGE_BUILDER:-}
build_context=${IMAGE_BUILD_CONTEXT:-.}
require_clean=${IMAGE_REQUIRE_CLEAN_CHECKOUT:-0}
verify_toolchain=${IMAGE_VERIFY_TOOLCHAIN:-0}
checkout_sha=$(git rev-parse HEAD)
source_tree=$(git rev-parse HEAD^{tree})
checkout_clean=true
if ! git diff --quiet || ! git diff --cached --quiet; then
  checkout_clean=false
fi

if test "$commit" != "$checkout_sha"; then
  printf 'COMMIT must match the checked-out commit: expected %s, got %s\n' \
    "$checkout_sha" "$commit" >&2
  exit 2
fi
if test "$require_clean" = 1 && test "$checkout_clean" != true; then
  printf '%s\n' 'IMAGE_REQUIRE_CLEAN_CHECKOUT=1 but tracked checkout changes are present' >&2
  exit 2
fi
if test "$verify_toolchain" = 1; then
  actual_buildx_version=$(docker buildx version | sed -n 's/.* v\{0,1\}\([0-9][0-9.]*\) .*/\1/p')
  if test "$actual_buildx_version" != "$BUILDX_VERSION"; then
    printf 'Docker Buildx %s is required, got %s\n' "$BUILDX_VERSION" "${actual_buildx_version:-unknown}" >&2
    exit 2
  fi
fi
mkdir -p "$metadata_dir"

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

images_file_digest=$(sha256_file "$repo_root/build/images.env")

build_image() {
  name=$1
  dockerfile=$2
  component=${3:-}
  docker_stage=${4:-}
  image_family=${5:-control}
  metadata_file="$metadata_dir/$name.json"
  source_file="$metadata_dir/$name.source-sha"
  inputs_file="$metadata_dir/$name.inputs.json"
  inputs_digest_file="$metadata_dir/$name.inputs.sha256"
  dockerfile_for_build="$repo_root/$dockerfile"
  if test "$build_context" != . && test -f "$build_context/$dockerfile"; then
    dockerfile_for_build="$build_context/$dockerfile"
  fi
  dockerfile_digest=$(sha256_file "$dockerfile_for_build")

  jq -S -n \
    --arg name "$name" \
    --arg source_sha "$commit" \
    --arg source_tree "$source_tree" \
    --arg version "$version" \
    --arg built_at "$built_at" \
    --arg source_date_epoch "$source_date_epoch" \
    --arg platform "${platform:-native}" \
    --arg dockerfile "$dockerfile" \
    --arg dockerfile_digest "sha256:$dockerfile_digest" \
    --arg images_file_digest "sha256:$images_file_digest" \
    --arg component "$component" \
    --arg target "$docker_stage" \
    --arg cache_mode "$cache_mode" \
    --arg frontend "$DOCKERFILE_FRONTEND_IMAGE" \
    --arg go_builder "$GO_BUILDER_IMAGE" \
    --arg node_builder "$NODE_BUILDER_IMAGE" \
    --arg static_runtime "$DISTROLESS_STATIC_IMAGE" \
    --arg base_runtime "$DISTROLESS_BASE_IMAGE" \
    --arg buildkit "$BUILDKIT_IMAGE" \
    --arg buildx_version "$BUILDX_VERSION" \
    --argjson clean_checkout "$checkout_clean" \
    '{
      schema_version: 1,
      image: $name,
      source_sha: $source_sha,
      source_tree: $source_tree,
      version: $version,
      built_at: $built_at,
      source_date_epoch: $source_date_epoch,
      platform: $platform,
      dockerfile: $dockerfile,
      dockerfile_digest: $dockerfile_digest,
      images_file_digest: $images_file_digest,
      component: $component,
      target: $target,
      cache_mode: $cache_mode,
      exporter: "docker-load-then-registry-push",
      provenance: false,
      sbom: false,
      clean_checkout: $clean_checkout,
      toolchain: {
        dockerfile_frontend: $frontend,
        go_builder: $go_builder,
        node_builder: $node_builder,
        distroless_static: $static_runtime,
        distroless_base: $base_runtime,
        buildkit: $buildkit,
        buildx_version: $buildx_version
      }
    }' >"$inputs_file"
  printf 'sha256:%s\n' "$(sha256_file "$inputs_file")" >"$inputs_digest_file"

  set -- docker buildx build --provenance=false --sbom=false \
    --output type=docker,rewrite-timestamp=true,compression=gzip,compression-level=9,force-compression=true,oci-mediatypes=false \
    --build-arg "VERSION=$version" \
    --build-arg "COMMIT=$commit" \
    --build-arg "BUILT_AT=$built_at" \
    --build-arg "SOURCE_DATE_EPOCH=$source_date_epoch" \
    --file "$dockerfile_for_build" \
    --metadata-file "$metadata_file" \
    --tag "${local_namespace}/${name}:${local_tag}"
  if test -n "$builder"; then
    set -- "$@" --builder "$builder"
  fi
  case "$image_family" in
    control)
      set -- "$@" \
        --build-arg "GO_BUILDER_IMAGE=$GO_BUILDER_IMAGE" \
        --build-arg "NODE_BUILDER_IMAGE=$NODE_BUILDER_IMAGE" \
        --build-arg "DISTROLESS_STATIC_IMAGE=$DISTROLESS_STATIC_IMAGE"
      ;;
    worker)
      set -- "$@" \
        --build-arg "GO_BUILDER_IMAGE=$GO_BUILDER_IMAGE" \
        --build-arg "DISTROLESS_BASE_IMAGE=$DISTROLESS_BASE_IMAGE"
      ;;
    *) printf 'unsupported image family: %s\n' "$image_family" >&2; exit 2 ;;
  esac
  if test -n "$platform"; then
    set -- "$@" --platform "$platform"
  fi
  if test -n "$component"; then
    set -- "$@" --build-arg "TARGET=$component"
  fi
  if test -n "$docker_stage"; then
    set -- "$@" --target "$docker_stage"
  fi
  case "$cache_mode" in
    gha)
      set -- "$@" \
        --cache-from "type=gha,scope=${name}-${cache_suffix}" \
        --cache-to "type=gha,mode=max,scope=${name}-${cache_suffix}"
      ;;
    none) ;;
    *) printf 'unsupported DOCKER_BUILD_CACHE: %s\n' "$cache_mode" >&2; exit 2 ;;
  esac
  "$@" "$build_context"

  digest=$(jq -er '."containerimage.digest"' "$metadata_file")
  if ! printf '%s' "$digest" | jq -Re 'test("^sha256:[0-9a-f]{64}$")' >/dev/null; then
    printf 'Buildx returned a non-SHA-256 digest for %s\n' "$name" >&2
    exit 1
  fi
  printf '%s\n' "$commit" >"$source_file"
}

build_image control-api build/control.Dockerfile control-api runtime
build_image web-bff build/control.Dockerfile web-bff web-bff-runtime
build_image reconciler build/control.Dockerfile reconciler runtime
build_image telegram-sender build/control.Dockerfile telegram-sender runtime
build_image worker-runtime build/worker-runtime.Dockerfile '' '' worker
