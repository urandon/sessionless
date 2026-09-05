#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
. "$repo_root/build/images.env"
. "$repo_root/tools/versions.env"

for command_name in docker jq; do
  command -v "$command_name" >/dev/null 2>&1 || {
    printf '%s is required\n' "$command_name" >&2
    exit 1
  }
done

go_builder_descriptor=$(
  docker buildx imagetools inspect "$GO_BUILDER_IMAGE" --raw |
    jq -cer '
      [.manifests[] |
        select(
          .platform.os == "linux" and
          .platform.architecture == "amd64" and
          ((.platform.variant // "") == "")
        )] |
      if length == 1 then .[0]
      else error("pinned Go builder index must contain exactly one linux/amd64 manifest")
      end
    '
)
actual_go_builder_child=$(printf '%s\n' "$go_builder_descriptor" | jq -er '.digest')
actual_go_builder_revision=$(
  printf '%s\n' "$go_builder_descriptor" |
    jq -er '.annotations["org.opencontainers.image.revision"]'
)
actual_go_builder_source=$(
  printf '%s\n' "$go_builder_descriptor" |
    jq -er '.annotations["org.opencontainers.image.source"]'
)
actual_go_builder_version=$(
  printf '%s\n' "$go_builder_descriptor" |
    jq -er '.annotations["org.opencontainers.image.version"]'
)
expected_go_builder_source="https://github.com/docker-library/golang.git#$GO_BUILDER_SOURCE_REVISION:1.26/alpine3.24"
if test "$actual_go_builder_child" != "$GO_BUILDER_LINUX_AMD64_MANIFEST_DIGEST" ||
  test "$actual_go_builder_revision" != "$GO_BUILDER_SOURCE_REVISION" ||
  test "$actual_go_builder_source" != "$expected_go_builder_source" ||
  test "$actual_go_builder_version" != "$GO_VERSION-alpine3.24"; then
  printf '%s\n' 'pinned Go builder linux/amd64 provenance does not match the reviewed contract' >&2
  printf 'child: expected %s, got %s\n' \
    "$GO_BUILDER_LINUX_AMD64_MANIFEST_DIGEST" "$actual_go_builder_child" >&2
  printf 'revision: expected %s, got %s\n' \
    "$GO_BUILDER_SOURCE_REVISION" "$actual_go_builder_revision" >&2
  printf 'source: expected %s, got %s\n' \
    "$expected_go_builder_source" "$actual_go_builder_source" >&2
  printf 'version: expected %s, got %s\n' \
    "$GO_VERSION-alpine3.24" "$actual_go_builder_version" >&2
  exit 1
fi

printf '%s\n' 'pinned Go builder linux/amd64 provenance verified'
