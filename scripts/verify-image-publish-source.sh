#!/bin/sh
set -eu

source_sha=${IMAGE_PUBLISH_SOURCE_SHA:?set IMAGE_PUBLISH_SOURCE_SHA}
confirmation=${IMAGE_PUBLISH_CONFIRMATION:?set IMAGE_PUBLISH_CONFIRMATION}
event_name=${GITHUB_EVENT_NAME:?set GITHUB_EVENT_NAME}
ref=${GITHUB_REF:?set GITHUB_REF}
workflow_sha=${GITHUB_SHA:?set GITHUB_SHA}
repository=${GITHUB_REPOSITORY:?set GITHUB_REPOSITORY}

case "$source_sha" in
  *[!0-9a-f]*|'') printf '%s\n' 'image publish source SHA must be lowercase hexadecimal' >&2; exit 2 ;;
esac
if test "${#source_sha}" -ne 40; then
  printf '%s\n' 'image publish source SHA must contain exactly 40 characters' >&2
  exit 2
fi
case "$workflow_sha" in
  *[!0-9a-f]*|'') printf '%s\n' 'GitHub workflow SHA must be lowercase hexadecimal' >&2; exit 2 ;;
esac
if test "${#workflow_sha}" -ne 40; then
  printf '%s\n' 'GitHub workflow SHA must contain exactly 40 characters' >&2
  exit 2
fi
if test "$event_name" != workflow_dispatch; then
  printf '%s\n' 'image publication requires an explicit workflow_dispatch event' >&2
  exit 2
fi
if test "$ref" != refs/heads/main; then
  printf '%s\n' 'image publication is restricted to refs/heads/main' >&2
  exit 2
fi
if test "$repository" != urandon/sessionless; then
  printf '%s\n' 'image publication is restricted to urandon/sessionless' >&2
  exit 2
fi
if test "$confirmation" != "publish-images:$source_sha"; then
  printf '%s\n' 'image publication confirmation does not match the selected source SHA' >&2
  exit 2
fi
if test "$source_sha" != "$workflow_sha"; then
  printf '%s\n' 'selected source SHA must equal the trusted workflow_dispatch main SHA' >&2
  exit 2
fi
if test "$(git rev-parse HEAD)" != "$source_sha"; then
  printf '%s\n' 'checked-out commit does not match the selected image publication SHA' >&2
  exit 2
fi

github_remote=https://github.com/urandon/sessionless.git
gitcode_remote=https://gitcode.com/urandon/sessionless.git
if test "${GITHUB_ACTIONS:-false}" != true; then
  github_remote=${IMAGE_PUBLISH_GITHUB_REMOTE:-$github_remote}
  gitcode_remote=${IMAGE_PUBLISH_GITCODE_REMOTE:-$gitcode_remote}
fi

resolve_main() {
  remote=$1
  result=$(git ls-remote --refs "$remote" refs/heads/main)
  count=$(printf '%s\n' "$result" | awk 'NF { count += 1 } END { print count + 0 }')
  if test "$count" -ne 1; then
    printf '%s\n' 'image publication remote must expose exactly one main ref' >&2
    exit 2
  fi
  printf '%s\n' "$result" | awk '{ print $1 }'
}

github_main=$(resolve_main "$github_remote")
gitcode_main=$(resolve_main "$gitcode_remote")
if test "$github_main" != "$gitcode_main"; then
  printf '%s\n' 'GitCode and GitHub main refs differ; wait for mirror convergence' >&2
  exit 2
fi
if test "$source_sha" != "$github_main"; then
  printf '%s\n' 'selected source SHA is no longer the exact current mirrored main ref' >&2
  exit 2
fi

if test -n "${IMAGE_PUBLISH_PROVENANCE_OUTPUT:-}"; then
  printf 'valid=true\nsource_sha=%s\n' "$source_sha" >>"$IMAGE_PUBLISH_PROVENANCE_OUTPUT"
fi
printf 'verified explicit image publication intent for %s\n' "$source_sha"
