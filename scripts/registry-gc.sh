#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
: "${REGISTRY_GC_INVENTORY_JSON:?set REGISTRY_GC_INVENTORY_JSON to the reviewed Terraform-output bridge JSON}"
: "${REGISTRY_GC_REPORT_JSON:?set REGISTRY_GC_REPORT_JSON}"
: "${REGISTRY_GC_REPORT_MARKDOWN:?set REGISTRY_GC_REPORT_MARKDOWN}"
: "${TERRAFORM_LOCK_YDB_CONNECTION_STRING:?set the bootstrap deployment-lock database connection string}"
: "${YC_TOKEN:?set YC_TOKEN through the OIDC exchange}"

mode=${REGISTRY_GC_MODE:-dry-run}
event_name=${GITHUB_EVENT_NAME:-local}
if test "$event_name" = schedule; then
  mode=dry-run
fi
case "$mode" in
  dry-run|delete) ;;
  *) printf 'unsupported REGISTRY_GC_MODE: %s\n' "$mode" >&2; exit 2 ;;
esac

umask 077
mkdir -p "$repo_root/.build"
run_dir=$(mktemp -d "$repo_root/.build/registry-gc-run.XXXXXX")
trap 'rm -rf "$run_dir"' EXIT HUP INT TERM
inventory="$run_dir/inventory.json"
inventory_payload="$run_dir/inventory-payload.json"
manifests_dir="$run_dir/manifests"
mkdir "$manifests_dir"
printf '%s\n' "$REGISTRY_GC_INVENTORY_JSON" >"$inventory"

jq -e '
  .registry_id as $registry |
  .schema_version == 1 and
  .environment == "cloud-dev" and
  .lock_environment == "cloud-dev" and
  (.registry_id | test("^[a-z0-9]+$")) and
  (.terraform.state_lineage | type == "string" and length > 0) and
  (.terraform.state_serial | type == "number" and . >= 0 and floor == .) and
  (.terraform.outputs_digest | test("^sha256:[0-9a-f]{64}$")) and
  (.repositories | keys | sort) ==
    ["control-api", "reconciler", "telegram-sender", "web-bff", "worker-runtime"] and
  all(.repositories | to_entries[];
    (.value | keys | sort) == ["id", "name"] and
    (.value.id | test("^[a-z0-9]+$")) and
    .value.name == ($registry + "/" + .key)) and
  (.containers | keys | sort) ==
    ["control-blue", "control-green", "reconciler", "telegram-sender", "web-bff", "worker-runtime"] and
  all(.containers | to_entries[];
    (.value | keys | sort) ==
      ["component", "container_id", "image_ref", "repository", "revision_id", "slot", "source_sha"] and
    (.value.container_id | test("^[a-z0-9]+$")) and
    (.value.revision_id | test("^[a-z0-9]+$")) and
    (.value.source_sha | test("^[0-9a-f]{40}$")) and
    (.value.image_ref | test("^cr\\.yandex/[a-z0-9]+/[a-z0-9-]+@sha256:[0-9a-f]{64}$")) and
    .value.repository == .value.component) and
  (.lifecycle_policy_status | keys | sort) ==
    ["control-api", "reconciler", "telegram-sender", "web-bff", "worker-runtime"] and
  all(.lifecycle_policy_status[]; . == "disabled")
' "$inventory" >/dev/null || {
  printf '%s\n' 'REGISTRY_GC_INVENTORY_JSON failed strict Terraform-output bridge validation' >&2
  exit 1
}
jq -cS 'del(.terraform)' "$inventory" >"$inventory_payload"
if command -v sha256sum >/dev/null 2>&1; then
  actual_outputs_digest="sha256:$(sha256sum "$inventory_payload" | awk '{print $1}')"
else
  actual_outputs_digest="sha256:$(shasum -a 256 "$inventory_payload" | awk '{print $1}')"
fi
recorded_outputs_digest=$(jq -er '.terraform.outputs_digest' "$inventory")
if test "$actual_outputs_digest" != "$recorded_outputs_digest"; then
  printf '%s\n' 'Terraform-output bridge digest does not match its inventory payload' >&2
  exit 1
fi
registry_id=$(jq -er '.registry_id' "$inventory")

if test "$mode" = delete; then
  if test "$event_name" != workflow_dispatch; then
    printf '%s\n' 'live registry deletion is restricted to a manual workflow_dispatch run' >&2
    exit 1
  fi
  expected_confirmation="cloud-dev:$registry_id"
  if test "${REGISTRY_GC_CONFIRM:-}" != "$expected_confirmation"; then
    printf 'refusing live registry deletion; confirmation must equal %s\n' \
      "$expected_confirmation" >&2
    exit 1
  fi
fi

protected_file=
if test -n "${REGISTRY_GC_PROTECTED_DIGESTS_FILE:-}" &&
  test -n "${REGISTRY_GC_PROTECTED_DIGESTS_JSON:-}"; then
  printf '%s\n' 'set only one protected-digests source' >&2
  exit 2
fi
if test -n "${REGISTRY_GC_PROTECTED_DIGESTS_JSON:-}"; then
  protected_file="$run_dir/protected-digests.json"
  printf '%s\n' "$REGISTRY_GC_PROTECTED_DIGESTS_JSON" >"$protected_file"
elif test -n "${REGISTRY_GC_PROTECTED_DIGESTS_FILE:-}"; then
  protected_file=$REGISTRY_GC_PROTECTED_DIGESTS_FILE
  test -f "$protected_file" || {
    printf 'protected-digests file does not exist: %s\n' "$protected_file" >&2
    exit 1
  }
fi
if test -n "$protected_file"; then
  jq -e \
    --arg registry "$registry_id" \
    '.schema_version == 1 and .environment == "cloud-dev" and
     .registry_id == $registry and
     (.digests | keys - ["control-api", "reconciler", "telegram-sender", "web-bff", "worker-runtime"] | length) == 0 and
     all(.digests[]; type == "array" and all(.[]; test("^sha256:[0-9a-f]{64}$")))' \
    "$protected_file" >/dev/null || {
    printf '%s\n' 'protected-digests evidence failed strict validation' >&2
    exit 1
  }
fi

GITHUB_TOKEN=${GITHUB_TOKEN:?set GITHUB_TOKEN for deployment-manifest evidence} \
GITHUB_REPOSITORY=${GITHUB_REPOSITORY:?set GITHUB_REPOSITORY} \
  "$repo_root/scripts/download-registry-gc-manifests.sh" \
  "$inventory" "$manifests_dir" "${REGISTRY_GC_MANIFEST_KEEP_COUNT:-3}"

set -- go run ./cmd/registry-gc \
  --inventory "$inventory" \
  --manifests-dir "$manifests_dir" \
  --report-json "$REGISTRY_GC_REPORT_JSON" \
  --report-markdown "$REGISTRY_GC_REPORT_MARKDOWN" \
  --mode "$mode"
if test -n "$protected_file"; then
  set -- "$@" --protected-digests "$protected_file"
fi

export YDB_ACCESS_TOKEN_CREDENTIALS=${YDB_ACCESS_TOKEN_CREDENTIALS:-$YC_TOKEN}
export DEPLOYMENT_ENVIRONMENT=cloud-dev
go run ./cmd/deployment-lock with -- "$@"
