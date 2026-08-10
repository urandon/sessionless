#!/bin/sh
set -eu

test "$#" -eq 1 || {
  printf 'usage: %s deployment-images.json\n' "$0" >&2
  exit 2
}
manifest=$1

jq -e '.schema_version == 1 and .platform == "linux/amd64"' "$manifest" >/dev/null
source_sha=$(jq -er '.source_sha' "$manifest")
case "$source_sha" in
  *[!0-9a-f]*|'') printf '%s\n' 'manifest source_sha is invalid' >&2; exit 1 ;;
esac
if test "${#source_sha}" -ne 40; then
  printf '%s\n' 'manifest source_sha is not a full commit SHA' >&2
  exit 1
fi
if test -n "${EXPECTED_SOURCE_SHA:-}" && test "$source_sha" != "$EXPECTED_SOURCE_SHA"; then
  printf '%s\n' 'manifest source_sha does not match EXPECTED_SOURCE_SHA' >&2
  exit 1
fi

for name in control-api reconciler telegram-sender worker-runtime; do
  reference=$(jq -er --arg name "$name" '.images[$name].reference' "$manifest")
  digest=$(jq -er --arg name "$name" '.images[$name].digest' "$manifest")
  if ! printf '%s' "$digest" | jq -Re 'test("^sha256:[0-9a-f]{64}$")' >/dev/null; then
    printf 'manifest contains an invalid SHA-256 digest for %s\n' "$name" >&2
    exit 1
  fi
  expected_reference_suffix="/${name}@${digest}"
  case "$reference" in
    cr.yandex/*"$expected_reference_suffix") ;;
    *) printf 'manifest digest/reference mismatch for %s\n' "$name" >&2; exit 1 ;;
  esac
  if test -n "${EXPECTED_REGISTRY_ID:-}"; then
    case "$reference" in
      "cr.yandex/${EXPECTED_REGISTRY_ID}/"*) ;;
      *) printf 'manifest registry does not match EXPECTED_REGISTRY_ID for %s\n' "$name" >&2; exit 1 ;;
    esac
  fi
done

jq -S '{
  control_blue_image_ref: .images["control-api"].reference,
  control_green_image_ref: .images["control-api"].reference,
  runtime_image_refs: {
    reconciler: .images.reconciler.reference,
    "telegram-sender": .images["telegram-sender"].reference,
    "worker-runtime": .images["worker-runtime"].reference
  }
}' "$manifest"
