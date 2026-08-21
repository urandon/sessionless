#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
state_file=$(mktemp "${TMPDIR:-/tmp}/sessionless-registry-gc-state.XXXXXX")
inventory_file=$(mktemp "${TMPDIR:-/tmp}/sessionless-registry-gc-inventory.XXXXXX")
trap 'rm -f "$state_file" "$inventory_file"' EXIT HUP INT TERM

terraform -chdir="$repo_root/infra/terraform/cloud-dev" state pull >"$state_file"
jq -e '
  (.lineage | type == "string" and length > 0) and
  (.serial | type == "number" and . >= 0 and floor == .) and
  (.outputs.registry_gc_inventory.value.schema_version == 1)
' "$state_file" >/dev/null || {
  printf '%s\n' 'remote Terraform state lacks complete registry_gc_inventory evidence' >&2
  exit 1
}

jq -cS '.outputs.registry_gc_inventory.value' "$state_file" >"$inventory_file"
if command -v sha256sum >/dev/null 2>&1; then
  outputs_digest="sha256:$(sha256sum "$inventory_file" | awk '{print $1}')"
else
  outputs_digest="sha256:$(shasum -a 256 "$inventory_file" | awk '{print $1}')"
fi

jq -S \
  --arg lineage "$(jq -er '.lineage' "$state_file")" \
  --argjson serial "$(jq -er '.serial' "$state_file")" \
  --arg outputs_digest "$outputs_digest" \
  '. + {
    terraform: {
      state_lineage: $lineage,
      state_serial: $serial,
      outputs_digest: $outputs_digest
    }
  }' "$inventory_file"
