#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
: "${CLOUD_DEV_BACKEND_CONFIG:?set CLOUD_DEV_BACKEND_CONFIG}"
: "${CLOUD_DEV_TFVARS:?set CLOUD_DEV_TFVARS}"
image_tag="${CLOUD_IMAGE_TAG:-$(git rev-parse HEAD)}"
case "$image_tag" in *[!0-9a-f]*) printf 'invalid image tag\n' >&2; exit 1;; esac
if test "${#image_tag}" -ne 40; then
  printf '%s\n' 'CLOUD_IMAGE_TAG must be a full commit SHA' >&2
  exit 1
fi
checkout_sha=$(git rev-parse HEAD)
if test "$image_tag" != "$checkout_sha"; then
  printf 'CLOUD_IMAGE_TAG must match the checked-out commit: expected %s, got %s\n' \
    "$checkout_sha" "$image_tag" >&2
  exit 1
fi

registry_id="$(terraform -chdir=infra/terraform/cloud-dev output -raw registry_id)"
manifest_path="${CLOUD_IMAGE_MANIFEST_PATH:-.build/deployment-images-${image_tag}.json}"
built_at="$(git show -s --format=%cI "$image_tag")"
metadata_dir="${CLOUD_IMAGE_METADATA_DIR:-.build/image-metadata}"
candidate_registry_path="${CLOUD_IMAGE_CANDIDATE_REGISTRY_PATH:-.build/image-candidate-registry.json}"

cleanup_candidate_registry() {
  CLOUD_IMAGE_CANDIDATE_REGISTRY_PATH="$candidate_registry_path" \
    sh "$repo_root/scripts/cleanup-image-candidate-registry.sh"
}
trap cleanup_candidate_registry EXIT HUP INT TERM

IMAGE_REQUIRE_CLEAN_CHECKOUT=1 \
  IMAGE_METADATA_DIR="$metadata_dir" \
  IMAGE_REPRODUCIBILITY_RETAIN_REGISTRY=1 \
  IMAGE_CANDIDATE_REGISTRY_PATH="$candidate_registry_path" \
  make image-reproducibility-test
YANDEX_CONTAINER_REGISTRY_ID="$registry_id" \
  CLOUD_IMAGE_TAG="$image_tag" \
  CLOUD_IMAGE_METADATA_DIR="$metadata_dir" \
  CLOUD_IMAGE_CANDIDATE_REGISTRY_PATH="$candidate_registry_path" \
  CLOUD_IMAGE_MANIFEST_PATH="$manifest_path" \
  CLOUD_IMAGE_REQUIRE_CLEAN_INPUTS=1 \
  SOURCE_BUILT_AT="$built_at" \
  ./scripts/publish-cloud-images.sh
cleanup_candidate_registry
trap - EXIT HUP INT TERM
