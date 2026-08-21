#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
test_root=$(mktemp -d "${TMPDIR:-/tmp}/sessionless-release-tag.XXXXXX")
trap 'rm -rf "$test_root"' EXIT HUP INT TERM

gitcode="$test_root/gitcode.git"
github="$test_root/github.git"
source_repo="$test_root/source"
checkout="$test_root/checkout"
git init --bare -q "$gitcode"
git init -q -b main "$source_repo"
git -C "$source_repo" config user.name 'Release Test'
git -C "$source_repo" config user.email release-test@example.invalid
printf '%s\n' initial >"$source_repo/file.txt"
git -C "$source_repo" add file.txt
git -C "$source_repo" commit -q -m 'Initial release'
git -C "$source_repo" tag -a v1.0.0 -m v1.0.0
git -C "$source_repo" remote add origin "$gitcode"
git -C "$source_repo" push -q origin main v1.0.0
git clone -q --mirror "$gitcode" "$github"
git clone -q "$github" "$checkout"
git -C "$checkout" checkout -q v1.0.0

source_sha=$(git -C "$checkout" rev-parse v1.0.0^{commit})
run_verify() {
  (
    cd "$checkout"
    RELEASE_PROVENANCE_TEST_MODE=1 \
      RELEASE_TEST_GITCODE_REMOTE="$gitcode" \
      RELEASE_TEST_GITHUB_REMOTE="$github" \
      RELEASE_TAG=v1.0.0 \
      GITHUB_EVENT_NAME=push \
      GITHUB_REF=refs/tags/v1.0.0 \
      GITHUB_REF_NAME=v1.0.0 \
      GITHUB_REF_TYPE=tag \
      GITHUB_REPOSITORY=urandon/sessionless \
      GITHUB_SERVER_URL=https://github.com \
      GITHUB_SHA="$source_sha" \
      sh "$repo_root/scripts/verify-release-tag.sh"
  )
}

run_verify >/dev/null

git -C "$source_repo" tag v1.1.0
git -C "$source_repo" push -q origin v1.1.0
git --git-dir="$github" fetch -q "$gitcode" '+refs/tags/*:refs/tags/*'
git -C "$checkout" fetch -q --tags
git -C "$checkout" checkout -q v1.1.0
source_sha=$(git -C "$checkout" rev-parse HEAD)
RELEASE_TAG=v1.1.0
(
  cd "$checkout"
  RELEASE_PROVENANCE_TEST_MODE=1 RELEASE_TEST_GITCODE_REMOTE="$gitcode" \
    RELEASE_TEST_GITHUB_REMOTE="$github" RELEASE_TAG="$RELEASE_TAG" \
    GITHUB_EVENT_NAME=workflow_dispatch GITHUB_REF="refs/tags/$RELEASE_TAG" \
    GITHUB_REF_NAME="$RELEASE_TAG" GITHUB_REF_TYPE=tag \
    GITHUB_REPOSITORY=urandon/sessionless GITHUB_SERVER_URL=https://github.com \
    GITHUB_SHA="$source_sha" sh "$repo_root/scripts/verify-release-tag.sh"
) >/dev/null

# A GitHub-only tag must fail before any publication step can run.
git --git-dir="$github" tag v1.2.0 "$source_sha"
git -C "$checkout" fetch -q --tags
git -C "$checkout" checkout -q v1.2.0
if (
  cd "$checkout"
  RELEASE_PROVENANCE_TEST_MODE=1 RELEASE_TEST_GITCODE_REMOTE="$gitcode" \
    RELEASE_TEST_GITHUB_REMOTE="$github" RELEASE_TAG=v1.2.0 \
    GITHUB_EVENT_NAME=push GITHUB_REF=refs/tags/v1.2.0 GITHUB_REF_NAME=v1.2.0 \
    GITHUB_REF_TYPE=tag GITHUB_REPOSITORY=urandon/sessionless \
    GITHUB_SERVER_URL=https://github.com GITHUB_SHA="$source_sha" \
    sh "$repo_root/scripts/verify-release-tag.sh" >/dev/null 2>&1
); then
  printf '%s\n' 'GitHub-only tag passed provenance validation' >&2
  exit 1
fi

# Independently annotated objects for the same commit are not mirror evidence.
git -C "$source_repo" tag -a v1.3.0 -m gitcode-object "$source_sha"
git -C "$source_repo" push -q origin v1.3.0
GIT_COMMITTER_DATE='2030-01-01T00:00:00Z' \
  git --git-dir="$github" tag -a v1.3.0 -m github-object "$source_sha"
git -C "$checkout" fetch -q --force --tags
git -C "$checkout" checkout -q v1.3.0
if (
  cd "$checkout"
  RELEASE_PROVENANCE_TEST_MODE=1 RELEASE_TEST_GITCODE_REMOTE="$gitcode" \
    RELEASE_TEST_GITHUB_REMOTE="$github" RELEASE_TAG=v1.3.0 \
    GITHUB_EVENT_NAME=push GITHUB_REF=refs/tags/v1.3.0 GITHUB_REF_NAME=v1.3.0 \
    GITHUB_REF_TYPE=tag GITHUB_REPOSITORY=urandon/sessionless \
    GITHUB_SERVER_URL=https://github.com GITHUB_SHA="$source_sha" \
    sh "$repo_root/scripts/verify-release-tag.sh" >/dev/null 2>&1
); then
  printf '%s\n' 'different annotated tag objects passed provenance validation' >&2
  exit 1
fi

if (
  cd "$checkout"
  RELEASE_PROVENANCE_TEST_MODE=1 RELEASE_TEST_GITCODE_REMOTE="$gitcode" \
    RELEASE_TEST_GITHUB_REMOTE="$github" RELEASE_TAG=v1.3.0 \
    GITHUB_EVENT_NAME=push GITHUB_REF=refs/heads/main GITHUB_REF_NAME=main \
    GITHUB_REF_TYPE=branch GITHUB_REPOSITORY=urandon/sessionless \
    GITHUB_SERVER_URL=https://github.com GITHUB_SHA="$source_sha" \
    sh "$repo_root/scripts/verify-release-tag.sh" >/dev/null 2>&1
); then
  printf '%s\n' 'branch ref passed release provenance validation' >&2
  exit 1
fi

printf '%s\n' 'release tag provenance invariants passed'
