#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
test_root=$(mktemp -d "${TMPDIR:-/tmp}/sessionless-registry-gc-policy.XXXXXX")
trap 'rm -rf "$test_root"' EXIT HUP INT TERM
fake_bin="$test_root/bin"
archive_dir="$test_root/archives"
mkdir -p "$fake_bin" "$archive_dir" "$test_root/reports"

sha_a=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
sha_b=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
sha_c=cccccccccccccccccccccccccccccccccccccccc
digest=sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
registry_id=crptestregistry

make_manifest_archive() {
  sha=$1
  artifact_id=$2
  fixture_dir="$test_root/fixture-$artifact_id"
  mkdir "$fixture_dir"
  jq -S -n --arg sha "$sha" --arg digest "$digest" --arg registry "$registry_id" '
    {
      schema_version: 2,
      source: {sha: $sha},
      images: (reduce
        ["control-api", "web-bff", "reconciler", "telegram-sender", "worker-runtime"][] as $name
        ({}; .[$name] = {
          manifest_digest: $digest,
          reference: ("cr.yandex/" + $registry + "/" + $name + "@" + $digest)
        }))
    }
  ' >"$fixture_dir/deployment-images.json"
  (cd "$fixture_dir" && zip -q "$archive_dir/$artifact_id.zip" deployment-images.json)
}
make_manifest_archive "$sha_a" 203
make_manifest_archive "$sha_b" 202
make_manifest_archive "$sha_c" 201

cat >"$fake_bin/curl" <<'EOF'
#!/bin/sh
set -eu
destination=
url=
while test "$#" -gt 0; do
  case "$1" in
    --output) destination=$2; shift 2 ;;
    --header) shift 2 ;;
    --fail|--silent|--show-error|--retry-all-errors) shift ;;
    --retry) shift 2 ;;
    *) url=$1; shift ;;
  esac
done
test -n "$destination" && test -n "$url"
printf '%s\n' "$url" >>"$FAKE_CURL_LOG"
case "$url" in
  */actions/workflows/ci.yml/runs?*)
    if test "${FAKE_RUNS_MODE:-complete}" = incomplete; then
      printf '%s\n' '{"total_count":1,"workflow_runs":[{"id":103,"head_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","head_branch":"main","conclusion":"success"}]}' >"$destination"
    else
      printf '%s\n' '{"total_count":3,"workflow_runs":[{"id":103,"head_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","head_branch":"main","conclusion":"success"},{"id":102,"head_sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","head_branch":"main","conclusion":"success"},{"id":101,"head_sha":"cccccccccccccccccccccccccccccccccccccccc","head_branch":"main","conclusion":"success"}]}' >"$destination"
    fi
    ;;
  */actions/runs/103/artifacts?*)
    if test "${FAKE_RUNS_MODE:-complete}" = incomplete; then
      printf '%s\n' '{"total_count":0,"artifacts":[]}' >"$destination"
    else
      printf '%s\n' '{"total_count":1,"artifacts":[{"id":203,"name":"deployment-images-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","expired":false}]}' >"$destination"
    fi
    ;;
  */actions/runs/102/artifacts?*)
    printf '%s\n' '{"total_count":1,"artifacts":[{"id":202,"name":"deployment-images-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","expired":false}]}' >"$destination"
    ;;
  */actions/runs/101/artifacts?*)
    printf '%s\n' '{"total_count":1,"artifacts":[{"id":201,"name":"deployment-images-cccccccccccccccccccccccccccccccccccccccc","expired":false}]}' >"$destination"
    ;;
  */actions/artifacts/203/zip) cp "$FAKE_ARCHIVE_DIR/203.zip" "$destination" ;;
  */actions/artifacts/202/zip) cp "$FAKE_ARCHIVE_DIR/202.zip" "$destination" ;;
  */actions/artifacts/201/zip) cp "$FAKE_ARCHIVE_DIR/201.zip" "$destination" ;;
  *) printf 'unexpected fake GitHub URL: %s\n' "$url" >&2; exit 2 ;;
esac
EOF
chmod 755 "$fake_bin/curl"

cat >"$fake_bin/go" <<'EOF'
#!/bin/sh
set -eu
printf '%s\n' "$*" >>"$FAKE_GO_LOG"
EOF
chmod 755 "$fake_bin/go"

inventory_payload="$test_root/inventory-payload.json"
jq -cS -n --arg registry "$registry_id" --arg digest "$digest" --arg sha "$sha_a" '
  def repository($name): {id: ("repo" + ($name | gsub("-"; ""))), name: ($registry + "/" + $name)};
  def container($component; $slot): {
    component: $component,
    container_id: ("container" + ($component | gsub("-"; "")) + $slot),
    image_ref: ("cr.yandex/" + $registry + "/" + $component + "@" + $digest),
    repository: $component,
    revision_id: ("revision" + ($component | gsub("-"; "")) + $slot),
    slot: $slot,
    source_sha: $sha
  };
  {
    schema_version: 1,
    environment: "cloud-dev",
    lock_environment: "cloud-dev",
    folder_id: "folderdev",
    registry_id: $registry,
    stable_slot: "blue",
    candidate_slot: "green",
    repositories: {
      "control-api": repository("control-api"),
      "web-bff": repository("web-bff"),
      reconciler: repository("reconciler"),
      "telegram-sender": repository("telegram-sender"),
      "worker-runtime": repository("worker-runtime")
    },
    containers: {
      "control-blue": container("control-api"; "blue"),
      "control-green": container("control-api"; "green"),
      "web-bff": container("web-bff"; "singleton"),
      reconciler: container("reconciler"; "singleton"),
      "telegram-sender": container("telegram-sender"; "singleton"),
      "worker-runtime": container("worker-runtime"; "singleton")
    },
    lifecycle_policy_status: {
      "control-api": "disabled",
      "web-bff": "disabled",
      reconciler: "disabled",
      "telegram-sender": "disabled",
      "worker-runtime": "disabled"
    }
  }
' >"$inventory_payload"
if command -v sha256sum >/dev/null 2>&1; then
  inventory_digest="sha256:$(sha256sum "$inventory_payload" | awk '{print $1}')"
else
  inventory_digest="sha256:$(shasum -a 256 "$inventory_payload" | awk '{print $1}')"
fi
inventory=$(jq -cS --arg digest "$inventory_digest" \
  '. + {terraform: {state_lineage: "lineage-test", state_serial: 7, outputs_digest: $digest}}' \
  "$inventory_payload")
protected=$(jq -cS -n --arg registry "$registry_id" --arg digest "$digest" \
  '{schema_version: 1, environment: "cloud-dev", registry_id: $registry, digests: {"control-api": [$digest]}}')

export PATH="$fake_bin:$PATH"
export FAKE_ARCHIVE_DIR="$archive_dir"
export FAKE_CURL_LOG="$test_root/curl.log"
export FAKE_GO_LOG="$test_root/go.log"
export GITHUB_TOKEN=test-github-token
export GITHUB_REPOSITORY=urandon/sessionless
export TERRAFORM_LOCK_YDB_CONNECTION_STRING=grpc://lock.test/database
export YC_TOKEN=test-yandex-token
export REGISTRY_GC_INVENTORY_JSON="$inventory"
export REGISTRY_GC_REPORT_JSON="$test_root/reports/report.json"
export REGISTRY_GC_REPORT_MARKDOWN="$test_root/reports/report.md"

REGISTRY_GC_MODE=dry-run GITHUB_SHA="$sha_a" \
  sh "$repo_root/scripts/init-registry-gc-report.sh"
jq -e '.status == "blocked" and .mode == "dry-run" and .totals.deleted == 0' \
  "$REGISTRY_GC_REPORT_JSON" >/dev/null
grep -F 'Deletions: **0**' "$REGISTRY_GC_REPORT_MARKDOWN" >/dev/null

: >"$FAKE_GO_LOG"
: >"$FAKE_CURL_LOG"
REGISTRY_GC_MODE=delete REGISTRY_GC_CONFIRM=wrong GITHUB_EVENT_NAME=workflow_dispatch \
  sh "$repo_root/scripts/registry-gc.sh" >"$test_root/bad-confirmation.out" 2>&1 && {
  printf '%s\n' 'registry cleanup accepted an invalid delete confirmation' >&2
  exit 1
}
test ! -s "$FAKE_GO_LOG"
test ! -s "$FAKE_CURL_LOG"

: >"$FAKE_GO_LOG"
: >"$FAKE_CURL_LOG"
REGISTRY_GC_MODE=delete REGISTRY_GC_CONFIRM=wrong GITHUB_EVENT_NAME=schedule \
  REGISTRY_GC_PROTECTED_DIGESTS_JSON="$protected" \
  sh "$repo_root/scripts/registry-gc.sh"
grep -F 'run ./cmd/deployment-lock with -- go run ./cmd/registry-gc' "$FAKE_GO_LOG" >/dev/null
grep -F -- '--mode dry-run' "$FAKE_GO_LOG" >/dev/null
grep -F -- '--protected-digests ' "$FAKE_GO_LOG" >/dev/null

: >"$FAKE_GO_LOG"
REGISTRY_GC_MODE=delete REGISTRY_GC_CONFIRM="cloud-dev:$registry_id" \
  GITHUB_EVENT_NAME=workflow_dispatch sh "$repo_root/scripts/registry-gc.sh"
grep -F -- '--mode delete' "$FAKE_GO_LOG" >/dev/null

: >"$FAKE_GO_LOG"
if FAKE_RUNS_MODE=incomplete REGISTRY_GC_MODE=dry-run GITHUB_EVENT_NAME=workflow_dispatch \
  sh "$repo_root/scripts/registry-gc.sh" >"$test_root/incomplete.out" 2>&1; then
  printf '%s\n' 'registry cleanup accepted fewer than three deployment manifests' >&2
  exit 1
fi
test ! -s "$FAKE_GO_LOG"

bad_inventory=$(printf '%s' "$inventory" | jq -c '.terraform.outputs_digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"')
: >"$FAKE_GO_LOG"
: >"$FAKE_CURL_LOG"
if REGISTRY_GC_INVENTORY_JSON="$bad_inventory" REGISTRY_GC_MODE=dry-run \
  GITHUB_EVENT_NAME=workflow_dispatch sh "$repo_root/scripts/registry-gc.sh" \
  >"$test_root/bad-inventory.out" 2>&1; then
  printf '%s\n' 'registry cleanup accepted a mismatched inventory digest' >&2
  exit 1
fi
test ! -s "$FAKE_GO_LOG"
test ! -s "$FAKE_CURL_LOG"

grep -F 'cron: "17 3 * * 0"' "$repo_root/.github/workflows/registry-gc.yml" >/dev/null
grep -F 'id-token: write' "$repo_root/.github/workflows/registry-gc.yml" >/dev/null
grep -F 'if: always()' "$repo_root/.github/workflows/registry-gc.yml" >/dev/null
grep -F 'cloud-dev:<registry-id>' "$repo_root/.github/workflows/registry-gc.yml" >/dev/null
grep -F "grep -q '[[:cntrl:]]'" "$repo_root/.github/workflows/registry-gc.yml" >/dev/null
if grep -E 'uses:[[:space:]]+actions/[^@]+@v[0-9]+' \
  "$repo_root/.github/workflows/registry-gc.yml" >/dev/null; then
  printf '%s\n' 'privileged registry GC workflow actions must use immutable commit pins' >&2
  exit 1
fi
test "$(grep -Ec 'uses:[[:space:]]+actions/[^@]+@[0-9a-f]{40}[[:space:]]+# v[0-9]+' \
  "$repo_root/.github/workflows/registry-gc.yml")" -eq 4
grep -F 'go run ./cmd/deployment-lock with --' "$repo_root/scripts/registry-gc.sh" >/dev/null
grep -F '"registry-cleaner"' "$repo_root/infra/terraform/modules/foundation/main.tf" >/dev/null
grep -F 'resource "yandex_container_repository_iam_binding" "image_publisher"' \
  "$repo_root/infra/terraform/modules/foundation/main.tf" >/dev/null
grep -F 'serviceAccount:${yandex_iam_service_account.runtime["registry-cleaner"].id}' \
  "$repo_root/infra/terraform/modules/foundation/main.tf" >/dev/null
grep -E 'role[[:space:]]*=[[:space:]]*"container-registry.images.pusher"' \
  "$repo_root/infra/terraform/modules/foundation/main.tf" >/dev/null
grep -F 'status = "disabled"' "$repo_root/infra/terraform/modules/foundation/main.tf" >/dev/null
grep -E 'role[[:space:]]*=[[:space:]]*"serverless-containers.auditor"' \
  "$repo_root/infra/terraform/modules/runtime/main.tf" >/dev/null
grep -E 'role[[:space:]]*=[[:space:]]*"serverless-containers.auditor"' \
  "$repo_root/infra/terraform/modules/web/main.tf" >/dev/null
grep -F 'resource "yandex_ydb_database_iam_binding" "registry_cleaner"' \
  "$repo_root/infra/terraform/bootstrap/main.tf" >/dev/null
grep -E 'role[[:space:]]*=[[:space:]]*"ydb.editor"' \
  "$repo_root/infra/terraform/bootstrap/main.tf" >/dev/null

if grep -R -E 'registry_cleaner.*folder_iam|folder_iam.*registry_cleaner' \
  "$repo_root/infra/terraform" >/dev/null; then
  printf '%s\n' 'registry cleaner must not receive folder-scoped IAM roles' >&2
  exit 1
fi

printf '%s\n' 'registry GC operational policy invariants passed'
