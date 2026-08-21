#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
source_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
tag=v1.2.3
audience=https://github.com/urandon

make_token() {
  ref=$1
  sha=$2
  repository=$3
  TOKEN_REF="$ref" TOKEN_SHA="$sha" TOKEN_REPOSITORY="$repository" \
    TOKEN_AUDIENCE="$audience" node -e '
      const encode = (value) => Buffer.from(JSON.stringify(value)).toString("base64url");
      const claims = {
        iss: "https://token.actions.githubusercontent.com",
        aud: process.env.TOKEN_AUDIENCE,
        sub: "repo:urandon/sessionless:environment:release",
        repository: process.env.TOKEN_REPOSITORY,
        repository_id: "123",
        repository_owner_id: "456",
        ref: process.env.TOKEN_REF,
        sha: process.env.TOKEN_SHA,
        event_name: "push",
      };
      process.stdout.write(`${encode({alg: "RS256", typ: "JWT"})}.${encode(claims)}.fixture`);
    '
}

run_claim_check() {
  token=$1
  RELEASE_OIDC_TEST_TOKEN="$token" \
    RELEASE_TAG="$tag" \
    RELEASE_SOURCE_SHA="$source_sha" \
    YANDEX_OIDC_AUDIENCE="$audience" \
    GITHUB_REPOSITORY=urandon/sessionless \
    GITHUB_REF="refs/tags/$tag" \
    GITHUB_SHA="$source_sha" \
    GITHUB_EVENT_NAME=push \
    OIDC_CLAIM_ONLY=1 \
    node "$repo_root/scripts/github-yandex-release-login.mjs"
}

valid=$(make_token "refs/tags/$tag" "$source_sha" urandon/sessionless)
run_claim_check "$valid" >/dev/null

bad_ref=$(make_token refs/heads/main "$source_sha" urandon/sessionless)
if run_claim_check "$bad_ref" >/dev/null 2>&1; then
  printf '%s\n' 'release OIDC accepted a branch claim' >&2
  exit 1
fi

bad_sha=$(make_token "refs/tags/$tag" bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb urandon/sessionless)
if run_claim_check "$bad_sha" >/dev/null 2>&1; then
  printf '%s\n' 'release OIDC accepted a different source commit' >&2
  exit 1
fi

bad_repository=$(make_token "refs/tags/$tag" "$source_sha" attacker/sessionless)
if run_claim_check "$bad_repository" >/dev/null 2>&1; then
  printf '%s\n' 'release OIDC accepted a different repository' >&2
  exit 1
fi

if GITHUB_ACTIONS=true RELEASE_OIDC_TEST_TOKEN="$valid" \
  RELEASE_TAG="$tag" RELEASE_SOURCE_SHA="$source_sha" \
  YANDEX_OIDC_AUDIENCE="$audience" GITHUB_REPOSITORY=urandon/sessionless \
  GITHUB_REF="refs/tags/$tag" GITHUB_SHA="$source_sha" GITHUB_EVENT_NAME=push \
  OIDC_CLAIM_ONLY=1 node "$repo_root/scripts/github-yandex-release-login.mjs" \
  >/dev/null 2>&1; then
  printf '%s\n' 'release OIDC test-token bypass was available in GitHub Actions' >&2
  exit 1
fi

printf '%s\n' 'release OIDC claim invariants passed'
