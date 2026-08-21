#!/bin/sh
set -eu

export LC_ALL=C

die() {
  printf 'release provenance: %s\n' "$*" >&2
  exit 1
}

required() {
  eval "value=\${$1:-}"
  test -n "$value" || die "$1 is required"
}

for name in RELEASE_TAG GITHUB_EVENT_NAME GITHUB_REF GITHUB_REF_NAME \
  GITHUB_REF_TYPE GITHUB_REPOSITORY GITHUB_SERVER_URL GITHUB_SHA; do
  required "$name"
done

test "$GITHUB_REPOSITORY" = urandon/sessionless ||
  die "unexpected GitHub repository"
test "$GITHUB_SERVER_URL" = https://github.com ||
  die "unexpected GitHub server"
case "$GITHUB_EVENT_NAME" in
  push|workflow_dispatch) ;;
  *) die "unsupported GitHub event $GITHUB_EVENT_NAME" ;;
esac
test "$GITHUB_REF_TYPE" = tag || die "release runs require a tag ref"
test "$GITHUB_REF" = "refs/tags/$RELEASE_TAG" ||
  die "GitHub ref does not match RELEASE_TAG"
test "$GITHUB_REF_NAME" = "$RELEASE_TAG" ||
  die "GitHub ref name does not match RELEASE_TAG"

if ! printf '%s\n' "$RELEASE_TAG" | grep -Eq \
  '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-rc\.(0|[1-9][0-9]*))?$'; then
  die "tag must be vMAJOR.MINOR.PATCH or vMAJOR.MINOR.PATCH-rc.N"
fi

gitcode_remote=https://gitcode.com/urandon/sessionless.git
github_remote=https://github.com/urandon/sessionless.git
if test "${RELEASE_PROVENANCE_TEST_MODE:-0}" = 1; then
  test "${GITHUB_ACTIONS:-false}" != true ||
    die "test remotes are forbidden in GitHub Actions"
  gitcode_remote=${RELEASE_TEST_GITCODE_REMOTE:?set RELEASE_TEST_GITCODE_REMOTE}
  github_remote=${RELEASE_TEST_GITHUB_REMOTE:?set RELEASE_TEST_GITHUB_REMOTE}
fi

local_raw=$(git rev-parse --verify "refs/tags/$RELEASE_TAG") ||
  die "local tag is missing"
local_type=$(git cat-file -t "$local_raw") || die "local tag object is unreadable"
case "$local_type" in
  commit|tag) ;;
  *) die "release tag must resolve from a commit or annotated tag object" ;;
esac
local_peeled=$(git rev-parse --verify "$RELEASE_TAG^{commit}") ||
  die "local tag does not peel to a commit"
head_sha=$(git rev-parse --verify HEAD^{commit}) || die "HEAD is not a commit"
test "$head_sha" = "$local_peeled" || die "checked-out HEAD differs from the peeled tag"
test "$GITHUB_SHA" = "$local_peeled" || die "GITHUB_SHA differs from the peeled tag"

remote_tag_identity() {
  remote=$1
  label=$2
  lines=$(git ls-remote "$remote" "refs/tags/$RELEASE_TAG" "refs/tags/$RELEASE_TAG^{}") ||
    die "$label tag lookup failed"
  raw=$(printf '%s\n' "$lines" | awk -v ref="refs/tags/$RELEASE_TAG" '$2 == ref {print $1}')
  peeled=$(printf '%s\n' "$lines" | awk -v ref="refs/tags/$RELEASE_TAG^{}" '$2 == ref {print $1}')
  test "$(printf '%s\n' "$raw" | sed '/^$/d' | wc -l | tr -d ' ')" = 1 ||
    die "$label does not contain exactly one raw tag ref"
  case "$local_type" in
    tag)
      test "$(printf '%s\n' "$peeled" | sed '/^$/d' | wc -l | tr -d ' ')" = 1 ||
        die "$label annotated tag has no unique peeled commit"
      ;;
    commit)
      test -z "$peeled" || die "$label lightweight tag unexpectedly has a peeled object"
      peeled=$raw
      ;;
  esac
  printf '%s %s\n' "$raw" "$peeled"
}

gitcode_identity=$(remote_tag_identity "$gitcode_remote" GitCode)
set -- $gitcode_identity
gitcode_raw=$1
gitcode_peeled=$2
github_identity=$(remote_tag_identity "$github_remote" GitHub)
set -- $github_identity
github_raw=$1
github_peeled=$2

test "$gitcode_raw" = "$local_raw" || die "GitCode raw tag object differs from local"
test "$github_raw" = "$local_raw" || die "GitHub raw tag object differs from local"
test "$gitcode_peeled" = "$local_peeled" || die "GitCode peeled commit differs from local"
test "$github_peeled" = "$local_peeled" || die "GitHub peeled commit differs from local"

gitcode_main=$(git ls-remote "$gitcode_remote" refs/heads/main | awk '$2 == "refs/heads/main" {print $1}') ||
  die "GitCode main lookup failed"
github_main=$(git ls-remote "$github_remote" refs/heads/main | awk '$2 == "refs/heads/main" {print $1}') ||
  die "GitHub main lookup failed"
test "$(printf '%s\n' "$gitcode_main" | sed '/^$/d' | wc -l | tr -d ' ')" = 1 ||
  die "GitCode main is missing or ambiguous"
test "$github_main" = "$gitcode_main" || die "mirrored GitHub main differs from GitCode main"
git cat-file -e "$gitcode_main^{commit}" 2>/dev/null ||
  die "mirrored main commit is absent from the clean checkout"
git rev-list --first-parent "$gitcode_main" | grep -Fx "$local_peeled" >/dev/null ||
  die "release tag is not on the first-parent history of GitCode main"

if test -n "${RELEASE_PROVENANCE_OUTPUT:-}"; then
  {
    printf 'tag=%s\n' "$RELEASE_TAG"
    printf 'raw_tag_sha=%s\n' "$local_raw"
    printf 'source_sha=%s\n' "$local_peeled"
    printf 'main_sha=%s\n' "$gitcode_main"
  } >>"$RELEASE_PROVENANCE_OUTPUT"
fi

printf 'verified GitCode and GitHub tag %s: raw=%s peeled=%s main=%s\n' \
  "$RELEASE_TAG" "$local_raw" "$local_peeled" "$gitcode_main"
