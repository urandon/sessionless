#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
. "$repo_root/build/images.env"

for variable_name in \
  DOCKERFILE_FRONTEND_IMAGE \
  GO_BUILDER_IMAGE \
  NODE_BUILDER_IMAGE \
  DISTROLESS_STATIC_IMAGE \
  DISTROLESS_BASE_IMAGE \
  BUILDKIT_IMAGE \
  LOCAL_REGISTRY_IMAGE; do
  eval "reference=\${$variable_name}"
  if ! printf '%s' "$reference" | jq -Re 'test("^[^[:space:]]+@sha256:[0-9a-f]{64}$")' >/dev/null; then
    printf '%s must be an immutable SHA-256 image reference\n' "$variable_name" >&2
    exit 1
  fi
done

for dockerfile in build/control.Dockerfile build/worker-runtime.Dockerfile; do
  first_line=$(sed -n '1p' "$repo_root/$dockerfile")
  if test "$first_line" != "# syntax=$DOCKERFILE_FRONTEND_IMAGE"; then
    printf '%s does not use the canonical pinned Dockerfile frontend\n' "$dockerfile" >&2
    exit 1
  fi
  if grep -E '^FROM [^$].*[^@]sha256:|^FROM [^$]*:[^@[:space:]]+([[:space:]]|$)' "$repo_root/$dockerfile" >/dev/null; then
    printf '%s contains a tag-only or non-canonical FROM input\n' "$dockerfile" >&2
    exit 1
  fi
done

test "$(grep -c -- '-buildvcs=false' "$repo_root/build/control.Dockerfile")" -eq 2
test "$(grep -c -- '-buildvcs=false' "$repo_root/build/worker-runtime.Dockerfile")" -eq 1

for required in \
  '--provenance=false' \
  '--sbom=false' \
  'rewrite-timestamp=true' \
  'compression=gzip' \
  'compression-level=9' \
  'force-compression=true' \
  'oci-mediatypes=false'; do
  grep -F -- "$required" "$repo_root/scripts/build-runtime-images.sh" >/dev/null || {
    printf 'build pipeline is missing deterministic exporter setting %s\n' "$required" >&2
    exit 1
  }
done

printf '%s\n' 'immutable image input and exporter invariants passed'
