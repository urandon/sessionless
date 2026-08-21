#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
workflow="$repo_root/.github/workflows/release.yml"
foundation="$repo_root/infra/terraform/modules/foundation/main.tf"
foundation_vars="$repo_root/infra/terraform/modules/foundation/variables.tf"

test -f "$workflow"

if grep -Eq '^[[:space:]]*(pull_request|pull_request_target):' "$workflow"; then
  printf '%s\n' 'release workflow must not run for pull requests' >&2
  exit 1
fi
grep -F 'tags:' "$workflow" >/dev/null
grep -F 'workflow_dispatch:' "$workflow" >/dev/null
test "$(grep -c 'environment: release' "$workflow")" -eq 2
grep -F 'contents: read' "$workflow" >/dev/null
grep -F 'id-token: write' "$workflow" >/dev/null
grep -F 'contents: write' "$workflow" >/dev/null

validate_job=$(sed -n '/^  validate:/,/^  verify:/p' "$workflow")
verify_job=$(sed -n '/^  verify:/,/^  images:/p' "$workflow")
images_job=$(sed -n '/^  images:/,/^  release:/p' "$workflow")
release_job=$(sed -n '/^  release:/,$p' "$workflow")
for read_only_job in "$validate_job" "$verify_job"; do
  printf '%s\n' "$read_only_job" | grep -F 'contents: read' >/dev/null
  if printf '%s\n' "$read_only_job" | grep -Eq 'contents: write|id-token: write'; then
    printf '%s\n' 'read-only release job has a privileged permission' >&2
    exit 1
  fi
done
printf '%s\n' "$images_job" | grep -F 'contents: read' >/dev/null
printf '%s\n' "$images_job" | grep -F 'id-token: write' >/dev/null
if printf '%s\n' "$images_job" | grep -F 'contents: write' >/dev/null; then
  printf '%s\n' 'image release job must not receive GitHub contents write' >&2
  exit 1
fi
printf '%s\n' "$release_job" | grep -F 'contents: write' >/dev/null
if printf '%s\n' "$release_job" | grep -F 'id-token: write' >/dev/null; then
  printf '%s\n' 'GitHub release job must not receive a cloud identity token' >&2
  exit 1
fi

uses_lines=$(sed -n 's/^[[:space:]]*uses:[[:space:]]*//p' "$workflow")
if printf '%s\n' "$uses_lines" | grep -Ev '^[^@]+@[0-9a-f]{40}([[:space:]]+#.*)?$' >/dev/null; then
  printf '%s\n' 'release workflow contains a non-immutable action reference' >&2
  printf '%s\n' "$uses_lines" | grep -Ev '^[^@]+@[0-9a-f]{40}([[:space:]]+#.*)?$' >&2
  exit 1
fi
for pin in \
  'actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803' \
  'actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16' \
  'actions/setup-node@249970729cb0ef3589644e2896645e5dc5ba9c38' \
  'docker/setup-buildx-action@37fe631027851001ddb9b187196cc803df7f5f0e' \
  'actions/upload-artifact@b7c566a772e6b6bfb58ed0dc250532a479d7789f' \
  'actions/download-artifact@018cc2cf5baa6db3ef3c5f8a56943fffe632ef53'; do
  grep -F "$pin" "$workflow" >/dev/null || {
    printf 'release workflow is missing immutable action %s\n' "$pin" >&2
    exit 1
  }
done

grep -F 'persist-credentials: false' "$workflow" >/dev/null
grep -F 'sh scripts/verify-release-tag.sh' "$workflow" >/dev/null
test "$(grep -c 'verify-release-tag.sh' "$workflow")" -ge 4
grep -F 'IMAGE_REPRODUCIBILITY_RETAIN_REGISTRY: "1"' "$workflow" >/dev/null
grep -F 'CLOUD_IMAGE_REQUIRE_CLEAN_INPUTS: "1"' "$workflow" >/dev/null
grep -F 'SOURCE_REPOSITORY: gitcode.com/urandon/sessionless' "$workflow" >/dev/null
grep -F -- '--source-sha "$RELEASE_SOURCE_SHA"' "$workflow" >/dev/null
grep -F '*-rc.*) prerelease_arg=--prerelease' "$workflow" >/dev/null
test "$(grep -c '\.build/release/assets' "$workflow")" -ge 8
if grep -Eq '(^|[^[:alnum:]_-])latest([^[:alnum:]_-]|$)|--clobber' "$workflow"; then
  printf '%s\n' 'release workflow contains a mutable tag or clobber path' >&2
  exit 1
fi

grep -F '"release-publisher"' "$foundation" >/dev/null
grep -F 'resource "yandex_iam_workload_identity_oidc_federation" "github_release"' \
  "$foundation" >/dev/null
grep -F 'resource "yandex_iam_workload_identity_federated_credential" "github_release"' \
  "$foundation" >/dev/null
grep -F 'serviceAccount:${yandex_iam_service_account.runtime["release-publisher"].id}' \
  "$foundation" >/dev/null
if sed -n '/account_roles = {/,/^  }/p' "$foundation" | grep -F 'release-publisher' >/dev/null; then
  printf '%s\n' 'release publisher unexpectedly received a folder role' >&2
  exit 1
fi
grep -F ':environment:release' "$foundation_vars" >/dev/null

for name in control-api web-bff reconciler telegram-sender worker-runtime; do
  grep -F "$name" "$workflow" >/dev/null || {
    printf 'release workflow is missing image %s\n' "$name" >&2
    exit 1
  }
done

node --check "$repo_root/scripts/github-yandex-release-login.mjs"
sh -n "$repo_root/scripts/verify-release-tag.sh"
sh -n "$repo_root/scripts/test-release-tag-provenance.sh"
sh "$repo_root/scripts/test-release-tag-provenance.sh"
sh -n "$repo_root/scripts/test-release-oidc.sh"
sh "$repo_root/scripts/test-release-oidc.sh"

printf '%s\n' 'release workflow and identity policy invariants passed'
