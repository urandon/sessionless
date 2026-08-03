#!/bin/sh
set -eu

: "${CLOUD_DEV_BACKEND_CONFIG:?set CLOUD_DEV_BACKEND_CONFIG}"
: "${CLOUD_DEV_TFVARS:?set CLOUD_DEV_TFVARS}"
image_tag="${CLOUD_IMAGE_TAG:-$(git rev-parse HEAD)}"
case "$image_tag" in *[!a-zA-Z0-9_.-]*) printf 'invalid image tag\n' >&2; exit 1;; esac
target_platform="linux/amd64"

repositories="$(terraform -chdir=infra/terraform/cloud-dev output -json repository_names)"
control_repository="$(printf '%s' "$repositories" | jq -r '."control-api"')"
reconciler_repository="$(printf '%s' "$repositories" | jq -r '.reconciler')"
sender_repository="$(printf '%s' "$repositories" | jq -r '."telegram-sender"')"
worker_repository="$(printf '%s' "$repositories" | jq -r '."worker-runtime"')"

docker build --platform "$target_platform" --provenance=false --load \
  --build-arg TARGET=control-api -f build/control.Dockerfile \
  -t "cr.yandex/${control_repository}:${image_tag}" .
docker build --platform "$target_platform" --provenance=false --load \
  --build-arg TARGET=reconciler -f build/control.Dockerfile \
  -t "cr.yandex/${reconciler_repository}:${image_tag}" .
docker build --platform "$target_platform" --provenance=false --load \
  --build-arg TARGET=telegram-sender -f build/control.Dockerfile \
  -t "cr.yandex/${sender_repository}:${image_tag}" .
docker build --platform "$target_platform" --provenance=false --load \
  -f build/worker-runtime.Dockerfile \
  -t "cr.yandex/${worker_repository}:${image_tag}" .

for image in \
  "cr.yandex/${control_repository}:${image_tag}" \
  "cr.yandex/${reconciler_repository}:${image_tag}" \
  "cr.yandex/${sender_repository}:${image_tag}" \
  "cr.yandex/${worker_repository}:${image_tag}"; do
  actual_platform="$(docker image inspect "$image" --format '{{.Os}}/{{.Architecture}}')"
  if test "$actual_platform" != "$target_platform"; then
    printf 'refusing to push %s image %s; Yandex Serverless Containers requires %s\n' \
      "$actual_platform" "$image" "$target_platform" >&2
    exit 1
  fi
  docker push "$image"
done
printf 'pushed immutable deployment tag %s for %s\n' "$image_tag" "$target_platform"
