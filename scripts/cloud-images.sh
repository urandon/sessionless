#!/bin/sh
set -eu

: "${CLOUD_DEV_BACKEND_CONFIG:?set CLOUD_DEV_BACKEND_CONFIG}"
: "${CLOUD_DEV_TFVARS:?set CLOUD_DEV_TFVARS}"
image_tag="${CLOUD_IMAGE_TAG:-$(git rev-parse HEAD)}"
case "$image_tag" in *[!0-9a-f]*) printf 'invalid image tag\n' >&2; exit 1;; esac
if test "${#image_tag}" -ne 40; then
  printf '%s\n' 'CLOUD_IMAGE_TAG must be a full commit SHA' >&2
  exit 1
fi

registry_id="$(terraform -chdir=infra/terraform/cloud-dev output -raw registry_id)"
manifest_path="${CLOUD_IMAGE_MANIFEST_PATH:-.build/deployment-images-${image_tag}.json}"
built_at="$(git show -s --format=%cI "$image_tag")"

VERSION="$image_tag" COMMIT="$image_tag" BUILT_AT="$built_at" IMAGE_PLATFORM=linux/amd64 make images
YANDEX_CONTAINER_REGISTRY_ID="$registry_id" \
  CLOUD_IMAGE_TAG="$image_tag" \
  CLOUD_IMAGE_MANIFEST_PATH="$manifest_path" \
  SOURCE_BUILT_AT="$built_at" \
  ./scripts/publish-cloud-images.sh
