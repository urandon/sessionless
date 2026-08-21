#!/bin/sh
set -eu

: "${YANDEX_CONTAINER_REGISTRY_ID:?set the target Yandex Container Registry ID}"
: "${CLOUD_IMAGE_TAG:?set the full source commit SHA}"
: "${CLOUD_IMAGE_MANIFEST_PATH:?set the output manifest path}"
CLOUD_IMAGE_RECEIPT_PATH=${CLOUD_IMAGE_RECEIPT_PATH:-${CLOUD_IMAGE_MANIFEST_PATH%.json}.receipt.json}

case "$YANDEX_CONTAINER_REGISTRY_ID" in
  *[!a-z0-9]*|'') printf '%s\n' 'invalid YANDEX_CONTAINER_REGISTRY_ID' >&2; exit 2 ;;
esac
case "$CLOUD_IMAGE_TAG" in
  *[!0-9a-f]*|'') printf '%s\n' 'CLOUD_IMAGE_TAG must be a lowercase hexadecimal commit SHA' >&2; exit 2 ;;
esac
if test "${#CLOUD_IMAGE_TAG}" -ne 40; then
  printf '%s\n' 'CLOUD_IMAGE_TAG must be the full 40-character commit SHA' >&2
  exit 2
fi

target_platform=linux/amd64
manifest_dir=$(dirname "$CLOUD_IMAGE_MANIFEST_PATH")
receipt_dir=$(dirname "$CLOUD_IMAGE_RECEIPT_PATH")
mkdir -p "$manifest_dir" "$receipt_dir"
manifest_tmp=$(mktemp "$manifest_dir/deployment-images.XXXXXX")
entry_tmp=$(mktemp "$manifest_dir/deployment-image-entry.XXXXXX")
inspect_error=$(mktemp "$manifest_dir/registry-inspect.XXXXXX")
actions_tmp=$(mktemp "$manifest_dir/deployment-image-actions.XXXXXX")
receipt_tmp=$(mktemp "$receipt_dir/deployment-image-receipt.XXXXXX")
contract_tmp=$(mktemp "$manifest_dir/deployment-image-contract.XXXXXX")
trap 'rm -f "$manifest_tmp" "$entry_tmp" "$inspect_error" "$actions_tmp" "$receipt_tmp" "$contract_tmp"' EXIT HUP INT TERM

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

source_repository=${SOURCE_REPOSITORY:-gitcode.com/urandon/sessionless}
source_run_url=${SOURCE_RUN_URL:-}
source_committed_at=${SOURCE_BUILT_AT:-$(git show -s --format=%cI "$CLOUD_IMAGE_TAG")}
source_date_epoch=$(git show -s --format=%ct "$CLOUD_IMAGE_TAG")
source_tree=$(git show -s --format=%T "$CLOUD_IMAGE_TAG")
metadata_dir=${CLOUD_IMAGE_METADATA_DIR:-${IMAGE_METADATA_DIR:-.build/image-metadata}}

jq -S -n \
  --arg source_sha "$CLOUD_IMAGE_TAG" \
  --arg source_tree "$source_tree" \
  --arg source_repository "$source_repository" \
  --arg source_committed_at "$source_committed_at" \
  --arg source_date_epoch "$source_date_epoch" \
  --arg platform "$target_platform" \
  '{
    schema_version: 2,
    source: {
      repository: $source_repository,
      sha: $source_sha,
      tree: $source_tree,
      committed_at: $source_committed_at,
      source_date_epoch: $source_date_epoch
    },
    build: {
      platform: $platform,
      contract_digest: null,
      input_digests: {}
    },
    images: {}
  }' >"$manifest_tmp"
jq -S -n '{}' >"$actions_tmp"

for name in control-api web-bff reconciler telegram-sender worker-runtime; do
  local_image="sessionless/${name}:dev"
  registry_repository="cr.yandex/${YANDEX_CONTAINER_REGISTRY_ID}/${name}"
  tagged_reference="${registry_repository}:${CLOUD_IMAGE_TAG}"
  metadata_file="$metadata_dir/$name.json"
  source_file="$metadata_dir/$name.source-sha"
  inputs_file="$metadata_dir/$name.inputs.json"
  inputs_digest_file="$metadata_dir/$name.inputs.sha256"

  if test ! -f "$metadata_file" || test ! -f "$source_file" ||
    test ! -f "$inputs_file" || test ! -f "$inputs_digest_file"; then
    printf 'missing build provenance for %s in %s\n' "$name" "$metadata_dir" >&2
    exit 1
  fi
  built_source_sha=$(sed -n '1p' "$source_file")
  if test "$built_source_sha" != "$CLOUD_IMAGE_TAG"; then
    printf 'refusing to publish %s: built source %s does not match tag %s\n' \
      "$name" "$built_source_sha" "$CLOUD_IMAGE_TAG" >&2
    exit 1
  fi
  build_digest=$(jq -er '."containerimage.digest"' "$metadata_file")
  if ! printf '%s' "$build_digest" | jq -Re 'test("^sha256:[0-9a-f]{64}$")' >/dev/null; then
    printf 'build metadata contains a non-SHA-256 digest for %s\n' "$name" >&2
    exit 1
  fi
  input_digest=$(sed -n '1p' "$inputs_digest_file")
  actual_input_digest="sha256:$(sha256_file "$inputs_file")"
  if test "$input_digest" != "$actual_input_digest"; then
    printf 'build input digest mismatch for %s\n' "$name" >&2
    exit 1
  fi
  jq -e \
    --arg name "$name" \
    --arg sha "$CLOUD_IMAGE_TAG" \
    --arg platform "$target_platform" \
    '.schema_version == 1 and .image == $name and .source_sha == $sha and
     .platform == $platform and .provenance == false and .sbom == false' \
    "$inputs_file" >/dev/null
  if test "${CLOUD_IMAGE_REQUIRE_CLEAN_INPUTS:-0}" = 1 &&
    ! jq -e '.clean_checkout == true and .cache_mode == "none"' "$inputs_file" >/dev/null; then
    printf 'trusted publication requires clean, cache-free build inputs for %s\n' "$name" >&2
    exit 1
  fi

  actual_platform=$(docker image inspect "$local_image" --format '{{.Os}}/{{.Architecture}}')
  if test "$actual_platform" != "$target_platform"; then
    printf 'refusing to publish %s image %s; expected %s\n' \
      "$actual_platform" "$local_image" "$target_platform" >&2
    exit 1
  fi
  candidate_config_digest=$(docker image inspect "$local_image" --format '{{.Id}}')
  if ! printf '%s' "$candidate_config_digest" | jq -Re 'test("^sha256:[0-9a-f]{64}$")' >/dev/null; then
    printf 'local image has a non-SHA-256 config digest for %s\n' "$name" >&2
    exit 1
  fi

  : >"$inspect_error"
  if remote_descriptor=$(docker buildx imagetools inspect "$tagged_reference" --format '{{json .Manifest}}' 2>"$inspect_error"); then
    existing_digest=$(printf '%s' "$remote_descriptor" | jq -er '.digest // .Digest')
    if ! printf '%s' "$existing_digest" | jq -Re 'test("^sha256:[0-9a-f]{64}$")' >/dev/null; then
      printf 'registry returned invalid manifest identity for %s\n' "$name" >&2
      exit 1
    fi
    if test "$existing_digest" != "$build_digest"; then
      printf 'refusing to overwrite %s: registry manifest is %s, build manifest is %s\n' \
        "$tagged_reference" "$existing_digest" "$build_digest" >&2
      rm -f "$inspect_error"
      exit 1
    fi
    existing_reference="${registry_repository}@${existing_digest}"
    if ! remote_manifest=$(docker buildx imagetools inspect "$existing_reference" --raw 2>>"$inspect_error"); then
      printf 'could not inspect the raw registry manifest for %s:\n' "$existing_reference" >&2
      cat "$inspect_error" >&2
      rm -f "$inspect_error"
      exit 1
    fi
    existing_config_digest=$(printf '%s' "$remote_manifest" | jq -er '.config.digest // .Config.digest')
    if ! printf '%s' "$existing_config_digest" | jq -Re 'test("^sha256:[0-9a-f]{64}$")' >/dev/null; then
      printf 'registry returned invalid manifest identity for %s\n' "$name" >&2
      exit 1
    fi
    if test "$existing_config_digest" != "$candidate_config_digest"; then
      printf 'refusing to overwrite %s: registry config is %s, build config is %s\n' \
        "$tagged_reference" "$existing_config_digest" "$candidate_config_digest" >&2
      rm -f "$inspect_error"
      exit 1
    fi
    printf 'registry tag already matches the complete build manifest; skipping push: %s\n' "$tagged_reference"
    publication_action=verified_existing
  else
    inspect_message=$(tr '[:upper:]' '[:lower:]' <"$inspect_error")
    case "$inspect_message" in
      *'manifest unknown'*|*'not found'*|*'no such manifest'*) ;;
      *)
        printf 'could not safely inspect existing registry tag %s:\n' "$tagged_reference" >&2
        cat "$inspect_error" >&2
        rm -f "$inspect_error"
        exit 1
        ;;
    esac
    docker tag "$local_image" "$tagged_reference"
    docker push "$tagged_reference"
    publication_action=pushed
  fi
  rm -f "$inspect_error"

  remote_descriptor=$(docker buildx imagetools inspect "$tagged_reference" --format '{{json .Manifest}}')
  digest=$(printf '%s' "$remote_descriptor" | jq -er '.digest // .Digest')
  if ! printf '%s' "$digest" | jq -Re 'test("^sha256:[0-9a-f]{64}$")' >/dev/null; then
    printf 'registry returned a non-SHA-256 digest for %s\n' "$name" >&2
    exit 1
  fi
  if test "$digest" != "$build_digest"; then
    printf 'registry manifest mismatch for %s: expected %s, got %s\n' \
      "$name" "$build_digest" "$digest" >&2
    exit 1
  fi
  immutable_reference="${registry_repository}@${digest}"
  remote_manifest=$(docker buildx imagetools inspect "$immutable_reference" --raw)
  remote_config_digest=$(printf '%s' "$remote_manifest" | jq -er '.config.digest // .Config.digest')
  if ! printf '%s' "$remote_config_digest" | jq -Re 'test("^sha256:[0-9a-f]{64}$")' >/dev/null; then
    printf 'registry returned a non-SHA-256 config digest for %s\n' "$name" >&2
    exit 1
  fi
  if test "$remote_config_digest" != "$candidate_config_digest"; then
    printf 'registry config mismatch for %s: expected %s, got %s\n' \
      "$name" "$candidate_config_digest" "$remote_config_digest" >&2
    exit 1
  fi
  jq -S \
    --arg name "$name" \
    --arg tagged_reference "$tagged_reference" \
    --arg digest "$digest" \
    --arg immutable_reference "$immutable_reference" \
    --arg config_digest "$remote_config_digest" \
    --arg input_digest "$input_digest" \
    '.images[$name] = {
      tagged_reference: $tagged_reference,
      reference: $immutable_reference,
      manifest_digest: $digest,
      config_digest: $config_digest,
      input_digest: $input_digest
    } |
    .build.input_digests[$name] = $input_digest' "$manifest_tmp" >"$entry_tmp"
  mv "$entry_tmp" "$manifest_tmp"

  jq -S \
    --arg name "$name" \
    --arg action "$publication_action" \
    --arg tagged_reference "$tagged_reference" \
    --arg manifest_digest "$digest" \
    '.[$name] = {
      action: $action,
      tagged_reference: $tagged_reference,
      manifest_digest: $manifest_digest
    }' "$actions_tmp" >"$entry_tmp"
  mv "$entry_tmp" "$actions_tmp"
done

jq -S '.build.input_digests' "$manifest_tmp" >"$contract_tmp"
contract_digest="sha256:$(sha256_file "$contract_tmp")"
jq -S --arg contract_digest "$contract_digest" \
  '.build.contract_digest = $contract_digest' "$manifest_tmp" >"$entry_tmp"
mv "$entry_tmp" "$manifest_tmp"

jq -e '
  .schema_version == 2 and
  (.images | keys | sort) == ["control-api", "reconciler", "telegram-sender", "web-bff", "worker-runtime"]
' "$manifest_tmp" >/dev/null
jq -S . "$manifest_tmp" >"$entry_tmp"
mv "$entry_tmp" "$manifest_tmp"
mv "$manifest_tmp" "$CLOUD_IMAGE_MANIFEST_PATH"
manifest_digest="sha256:$(sha256_file "$CLOUD_IMAGE_MANIFEST_PATH")"

jq -S -n \
  --arg source_sha "$CLOUD_IMAGE_TAG" \
  --arg manifest_digest "$manifest_digest" \
  --arg published_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg source_run_url "$source_run_url" \
  --arg source_run_id "${SOURCE_RUN_ID:-}" \
  --arg source_run_attempt "${SOURCE_RUN_ATTEMPT:-}" \
  --slurpfile images "$actions_tmp" \
  '{
    schema_version: 1,
    source_sha: $source_sha,
    deployment_manifest_digest: $manifest_digest,
    published_at: $published_at,
    run: {
      provider: "github-actions",
      id: $source_run_id,
      attempt: $source_run_attempt,
      url: $source_run_url
    },
    images: $images[0]
  }' >"$receipt_tmp"
mv "$receipt_tmp" "$CLOUD_IMAGE_RECEIPT_PATH"
trap - EXIT HUP INT TERM
rm -f "$entry_tmp" "$inspect_error" "$actions_tmp" "$contract_tmp"
printf 'verified five immutable deployment images and wrote %s plus %s\n' \
  "$CLOUD_IMAGE_MANIFEST_PATH" "$CLOUD_IMAGE_RECEIPT_PATH"
