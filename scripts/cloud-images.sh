#!/bin/sh
set -eu

: "${CLOUD_DEV_BACKEND_CONFIG:?set CLOUD_DEV_BACKEND_CONFIG}"
: "${CLOUD_DEV_TFVARS:?set CLOUD_DEV_TFVARS}"
image_tag="${CLOUD_IMAGE_TAG:-$(git rev-parse HEAD)}"
case "$image_tag" in *[!a-zA-Z0-9_.-]*) printf 'invalid image tag\n' >&2; exit 1;; esac

repositories="$(terraform -chdir=infra/terraform/cloud-dev output -json repository_names)"
control_repository="$(printf '%s' "$repositories" | jq -r '."control-api"')"
reconciler_repository="$(printf '%s' "$repositories" | jq -r '.reconciler')"
sender_repository="$(printf '%s' "$repositories" | jq -r '."telegram-sender"')"
worker_repository="$(printf '%s' "$repositories" | jq -r '."worker-runtime"')"

docker build --build-arg TARGET=control-api -f build/control.Dockerfile -t "cr.yandex/${control_repository}:${image_tag}" .
docker build --build-arg TARGET=reconciler -f build/control.Dockerfile -t "cr.yandex/${reconciler_repository}:${image_tag}" .
docker build --build-arg TARGET=telegram-sender -f build/control.Dockerfile -t "cr.yandex/${sender_repository}:${image_tag}" .
docker build -f build/worker-runtime.Dockerfile -t "cr.yandex/${worker_repository}:${image_tag}" .

for image in \
  "cr.yandex/${control_repository}:${image_tag}" \
  "cr.yandex/${reconciler_repository}:${image_tag}" \
  "cr.yandex/${sender_repository}:${image_tag}" \
  "cr.yandex/${worker_repository}:${image_tag}"; do
  docker push "$image"
done
printf 'pushed immutable deployment tag %s\n' "$image_tag"
