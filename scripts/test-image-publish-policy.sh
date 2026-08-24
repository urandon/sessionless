#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
ci_workflow="$repo_root/.github/workflows/ci.yml"
publish_workflow="$repo_root/.github/workflows/publish-images.yml"

test -f "$publish_workflow"
grep -F 'workflow_dispatch:' "$publish_workflow" >/dev/null
if grep -Eq '^[[:space:]]*(push|pull_request|pull_request_target|schedule):' "$publish_workflow"; then
  printf '%s\n' 'image publication workflow has an automatic trigger' >&2
  exit 1
fi
if grep -F 'id-token: write' "$ci_workflow" >/dev/null || \
   grep -F 'github-yandex-registry-login.mjs' "$ci_workflow" >/dev/null || \
   grep -F 'publish-cloud-images.sh' "$ci_workflow" >/dev/null; then
  printf '%s\n' 'ordinary CI still contains a privileged image publication path' >&2
  exit 1
fi
grep -F 'make image-reproducibility-test' "$ci_workflow" >/dev/null

validate_job=$(sed -n '/^  validate:/,/^  publish:/p' "$publish_workflow")
publish_job=$(sed -n '/^  publish:/,$p' "$publish_workflow")
printf '%s\n' "$validate_job" | grep -F 'contents: read' >/dev/null
if printf '%s\n' "$validate_job" | grep -F 'id-token: write' >/dev/null; then
  printf '%s\n' 'publication validation job unexpectedly receives an OIDC token' >&2
  exit 1
fi
printf '%s\n' "$publish_job" | grep -F 'id-token: write' >/dev/null
printf '%s\n' "$publish_job" | grep -F "needs.validate.outputs.valid == 'true'" >/dev/null
test "$(grep -c 'verify-image-publish-source.sh' "$publish_workflow")" -eq 3
grep -F 'IMAGE_REPRODUCIBILITY_RETAIN_REGISTRY: "1"' "$publish_workflow" >/dev/null
grep -F 'CLOUD_IMAGE_REQUIRE_CLEAN_INPUTS: "1"' "$publish_workflow" >/dev/null
grep -F 'github-yandex-registry-login.mjs' "$publish_workflow" >/dev/null
grep -F 'publish-cloud-images.sh' "$publish_workflow" >/dev/null
grep -F 'if: always()' "$publish_workflow" >/dev/null
grep -F 'cleanup-image-candidate-registry.sh' "$publish_workflow" >/dev/null
if grep -F 'IMAGE_PUBLISH_GITHUB_REMOTE:' "$publish_workflow" >/dev/null || \
   grep -F 'IMAGE_PUBLISH_GITCODE_REMOTE:' "$publish_workflow" >/dev/null; then
  printf '%s\n' 'workflow must not override fixed publication provenance remotes' >&2
  exit 1
fi

uses_lines=$(sed -n 's/^[[:space:]]*uses:[[:space:]]*//p' "$publish_workflow")
if printf '%s\n' "$uses_lines" | grep -Ev '^[^@]+@[0-9a-f]{40}([[:space:]]+#.*)?$' >/dev/null; then
  printf '%s\n' 'image publication workflow contains a non-immutable action reference' >&2
  exit 1
fi

fixture=$(mktemp -d "${TMPDIR:-/tmp}/sessionless-image-publish-policy.XXXXXX")
trap 'rm -rf "$fixture"' EXIT HUP INT TERM
source_repo="$fixture/source"
github_bare="$fixture/github.git"
gitcode_bare="$fixture/gitcode.git"
git init -q "$source_repo"
git -C "$source_repo" config user.name fixture
git -C "$source_repo" config user.email fixture@example.invalid
printf '%s\n' fixture >"$source_repo/README"
git -C "$source_repo" add README
git -C "$source_repo" commit -q -m fixture
git -C "$source_repo" branch -M main
git clone -q --bare "$source_repo" "$github_bare"
git clone -q --bare "$source_repo" "$gitcode_bare"
source_sha=$(git -C "$source_repo" rev-parse HEAD)

run_verify() {
  (
    cd "$source_repo"
    GITHUB_ACTIONS=false \
    GITHUB_EVENT_NAME="${TEST_EVENT_NAME:-workflow_dispatch}" \
    GITHUB_REF="${TEST_REF:-refs/heads/main}" \
    GITHUB_SHA="${TEST_WORKFLOW_SHA:-$source_sha}" \
    GITHUB_REPOSITORY="${TEST_REPOSITORY:-urandon/sessionless}" \
    IMAGE_PUBLISH_SOURCE_SHA="${TEST_SOURCE_SHA:-$source_sha}" \
    IMAGE_PUBLISH_CONFIRMATION="${TEST_CONFIRMATION:-publish-images:$source_sha}" \
    IMAGE_PUBLISH_GITHUB_REMOTE="$github_bare" \
    IMAGE_PUBLISH_GITCODE_REMOTE="$gitcode_bare" \
      sh "$repo_root/scripts/verify-image-publish-source.sh"
  )
}

run_verify >/dev/null
if TEST_EVENT_NAME=push run_verify >/dev/null 2>&1; then
  printf '%s\n' 'push event unexpectedly passed image publication provenance' >&2
  exit 1
fi
if TEST_REF=refs/heads/feature run_verify >/dev/null 2>&1; then
  printf '%s\n' 'feature branch unexpectedly passed image publication provenance' >&2
  exit 1
fi
if TEST_CONFIRMATION=wrong run_verify >/dev/null 2>&1; then
  printf '%s\n' 'wrong typed confirmation unexpectedly passed' >&2
  exit 1
fi
other_sha=0000000000000000000000000000000000000000
if TEST_SOURCE_SHA="$other_sha" TEST_WORKFLOW_SHA="$other_sha" \
   TEST_CONFIRMATION="publish-images:$other_sha" run_verify >/dev/null 2>&1; then
  printf '%s\n' 'non-HEAD source unexpectedly passed image publication provenance' >&2
  exit 1
fi

printf '%s\n' divergent >>"$source_repo/README"
git -C "$source_repo" add README
git -C "$source_repo" commit -q -m divergent
git -C "$source_repo" push -q "$gitcode_bare" HEAD:main
git -C "$source_repo" checkout -q --detach "$source_sha"
if run_verify >/dev/null 2>&1; then
  printf '%s\n' 'divergent GitCode/GitHub main refs unexpectedly passed' >&2
  exit 1
fi

node --check "$repo_root/scripts/github-yandex-registry-login.mjs"
audience=https://github.com/urandon
make_oidc_token() {
  token_event=$1
  token_sha=$2
  TOKEN_EVENT="$token_event" TOKEN_SHA="$token_sha" TOKEN_AUDIENCE="$audience" node -e '
    const encode = (value) => Buffer.from(JSON.stringify(value)).toString("base64url");
    const claims = {
      iss: "https://token.actions.githubusercontent.com",
      aud: process.env.TOKEN_AUDIENCE,
      sub: "repo:urandon/sessionless:ref:refs/heads/main",
      repository: "urandon/sessionless",
      repository_id: "123",
      repository_owner_id: "456",
      ref: "refs/heads/main",
      sha: process.env.TOKEN_SHA,
      event_name: process.env.TOKEN_EVENT,
    };
    process.stdout.write(`${encode({alg: "RS256", typ: "JWT"})}.${encode(claims)}.fixture`);
  '
}
run_oidc_claim_check() {
  token=$1
  REGISTRY_OIDC_TEST_TOKEN="$token" \
    IMAGE_PUBLISH_SOURCE_SHA="$source_sha" \
    YANDEX_OIDC_AUDIENCE="$audience" \
    GITHUB_REPOSITORY=urandon/sessionless \
    GITHUB_REF=refs/heads/main \
    GITHUB_SHA="$source_sha" \
    GITHUB_EVENT_NAME=workflow_dispatch \
    OIDC_CLAIM_ONLY=1 \
    node "$repo_root/scripts/github-yandex-registry-login.mjs"
}
valid_token=$(make_oidc_token workflow_dispatch "$source_sha")
run_oidc_claim_check "$valid_token" >/dev/null
push_token=$(make_oidc_token push "$source_sha")
if run_oidc_claim_check "$push_token" >/dev/null 2>&1; then
  printf '%s\n' 'registry OIDC accepted a push event claim' >&2
  exit 1
fi
wrong_sha_token=$(make_oidc_token workflow_dispatch 0000000000000000000000000000000000000000)
if run_oidc_claim_check "$wrong_sha_token" >/dev/null 2>&1; then
  printf '%s\n' 'registry OIDC accepted a different source SHA' >&2
  exit 1
fi
if GITHUB_ACTIONS=true REGISTRY_OIDC_TEST_TOKEN="$valid_token" \
  IMAGE_PUBLISH_SOURCE_SHA="$source_sha" YANDEX_OIDC_AUDIENCE="$audience" \
  GITHUB_REPOSITORY=urandon/sessionless GITHUB_REF=refs/heads/main \
  GITHUB_SHA="$source_sha" GITHUB_EVENT_NAME=workflow_dispatch OIDC_CLAIM_ONLY=1 \
  node "$repo_root/scripts/github-yandex-registry-login.mjs" >/dev/null 2>&1; then
  printf '%s\n' 'registry OIDC test-token bypass was available in GitHub Actions' >&2
  exit 1
fi
sh -n "$repo_root/scripts/verify-image-publish-source.sh"
printf '%s\n' 'explicit image publication workflow invariants passed'
