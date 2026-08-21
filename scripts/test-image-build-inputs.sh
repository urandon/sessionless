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

for assignment in \
  "GO_BUILDER_IMAGE=$GO_BUILDER_IMAGE" \
  "NODE_BUILDER_IMAGE=$NODE_BUILDER_IMAGE" \
  "DISTROLESS_STATIC_IMAGE=$DISTROLESS_STATIC_IMAGE"; do
  grep -F "ARG $assignment" "$repo_root/build/control.Dockerfile" >/dev/null || {
    printf 'control Dockerfile default diverges from build/images.env: %s\n' "$assignment" >&2
    exit 1
  }
done
for assignment in \
  "GO_BUILDER_IMAGE=$GO_BUILDER_IMAGE" \
  "DISTROLESS_BASE_IMAGE=$DISTROLESS_BASE_IMAGE"; do
  grep -F "ARG $assignment" "$repo_root/build/worker-runtime.Dockerfile" >/dev/null || {
    printf 'worker Dockerfile default diverges from build/images.env: %s\n' "$assignment" >&2
    exit 1
  }
done
if grep -F 'GO_VERSION:' "$repo_root/compose.yaml" >/dev/null; then
  printf '%s\n' 'Compose still passes the removed GO_VERSION Docker build argument' >&2
  exit 1
fi
grep -F 'make image-reproducibility-test' "$repo_root/scripts/cloud-images.sh" >/dev/null
grep -F 'IMAGE_REQUIRE_CLEAN_CHECKOUT=1' "$repo_root/scripts/cloud-images.sh" >/dev/null
grep -F 'CLOUD_IMAGE_REQUIRE_CLEAN_INPUTS=1' "$repo_root/scripts/cloud-images.sh" >/dev/null
grep -F 'IMAGE_EXPORTER_MODE=registry' "$repo_root/scripts/test-image-reproducibility.sh" >/dev/null
grep -F 'http = true' "$repo_root/scripts/test-image-reproducibility.sh" >/dev/null
grep -F 'transport_manifest_digest == .manifest.digest' \
  "$repo_root/scripts/test-image-reproducibility.sh" >/dev/null
grep -F 'docker buildx imagetools create --prefer-index=false' \
  "$repo_root/scripts/test-image-reproducibility.sh" >/dev/null
grep -F 'registry-manifest-digest' "$repo_root/scripts/publish-cloud-images.sh" >/dev/null
grep -F '.exporter == "registry"' "$repo_root/scripts/publish-cloud-images.sh" >/dev/null
grep -F 'imagetools create --prefer-index=false --progress=plain' \
  "$repo_root/scripts/publish-cloud-images.sh" >/dev/null
grep -F '."containerimage.descriptor".digest' \
  "$repo_root/scripts/publish-cloud-images.sh" >/dev/null
grep -F 'IMAGE_REPRODUCIBILITY_RETAIN_REGISTRY=1' "$repo_root/scripts/cloud-images.sh" >/dev/null
grep -F 'if: always()' "$repo_root/.github/workflows/ci.yml" >/dev/null
grep -F 'cleanup-image-candidate-registry.sh' "$repo_root/.github/workflows/ci.yml" >/dev/null

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
grep -F 'SESSIONLESS_WEB_VERSION="${COMMIT}" npm run build' "$repo_root/build/control.Dockerfile" >/dev/null
grep -F "name: process.env.SESSIONLESS_WEB_VERSION || 'dev'" "$repo_root/web/svelte.config.js" >/dev/null
expected_web_version=0123456789abcdef0123456789abcdef01234567
actual_web_version=$(
  cd "$repo_root/web"
  SESSIONLESS_WEB_VERSION="$expected_web_version" node --input-type=module -e \
    "const {default: config} = await import('./svelte.config.js'); process.stdout.write(config.kit.version.name)"
)
test "$actual_web_version" = "$expected_web_version"
fallback_web_version=$(
  cd "$repo_root/web"
  SESSIONLESS_WEB_VERSION= node --input-type=module -e \
    "const {default: config} = await import('./svelte.config.js'); process.stdout.write(config.kit.version.name)"
)
test "$fallback_web_version" = dev

for required in \
  '--provenance=false' \
  '--sbom=false' \
  'rewrite-timestamp=true' \
  'compatibility-version=30' \
  'compression=gzip' \
  'compression-level=9' \
  'force-compression=true' \
  'oci-mediatypes=false'; do
  grep -F -- "$required" "$repo_root/scripts/build-runtime-images.sh" >/dev/null || {
    printf 'build pipeline is missing deterministic exporter setting %s\n' "$required" >&2
    exit 1
  }
done
if grep -F 'registry.insecure=true' "$repo_root/scripts/build-runtime-images.sh" >/dev/null; then
  printf '%s\n' 'plain-HTTP registry mode incorrectly requests insecure HTTPS' >&2
  exit 1
fi

printf '%s\n' 'immutable image input and exporter invariants passed'
