#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
: "${RELEASE_TAG:?set RELEASE_TAG}"
: "${RELEASE_SOURCE_SHA:?set RELEASE_SOURCE_SHA}"

manifest_path=${RELEASE_MANIFEST_PATH:-.build/deployment-images.json}
notes_path=${RELEASE_NOTES_PATH:-.build/release/release-notes.md}
asset_dir=${RELEASE_ASSET_DIR:-.build/release/assets}

case "$RELEASE_TAG" in
  v0.0.0|v0.0.0-rc.0) ;;
  v[0-9]*.[0-9]*.[0-9]*|v[0-9]*.[0-9]*.[0-9]*-rc.[0-9]*) ;;
  *) printf '%s\n' 'RELEASE_TAG must be a supported SemVer tag' >&2; exit 2 ;;
esac
if ! printf '%s' "$RELEASE_TAG" | jq -Re '
  test("^v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-rc\\.(0|[1-9][0-9]*))?$")
' >/dev/null; then
  printf '%s\n' 'RELEASE_TAG must be vMAJOR.MINOR.PATCH or vMAJOR.MINOR.PATCH-rc.N' >&2
  exit 2
fi
if ! printf '%s' "$RELEASE_SOURCE_SHA" | jq -Re 'test("^[0-9a-f]{40}$")' >/dev/null; then
  printf '%s\n' 'RELEASE_SOURCE_SHA must be a full lowercase commit SHA' >&2
  exit 2
fi

tag_sha=$(git -C "$repo_root" rev-parse "refs/tags/$RELEASE_TAG^{commit}" 2>/dev/null) || {
  printf 'release tag is not available in the checkout: %s\n' "$RELEASE_TAG" >&2
  exit 1
}
head_sha=$(git -C "$repo_root" rev-parse HEAD)
if test "$tag_sha" != "$RELEASE_SOURCE_SHA" || test "$head_sha" != "$RELEASE_SOURCE_SHA"; then
  printf '%s\n' 'release tag, source SHA, and checked-out HEAD must match' >&2
  exit 1
fi

for source_path in "$manifest_path" "$notes_path"; do
  if test ! -f "$source_path" || test -L "$source_path"; then
    printf 'release input must be a regular non-symlink file: %s\n' "$source_path" >&2
    exit 1
  fi
done
if test ! -s "$notes_path"; then
  printf '%s\n' 'release notes must not be empty' >&2
  exit 1
fi

jq -e --arg sha "$RELEASE_SOURCE_SHA" '
  def digest: type == "string" and test("^sha256:[0-9a-f]{64}$");
  def sha: type == "string" and test("^[0-9a-f]{40}$");
  def exact_components:
    (keys | sort) == ["control-api", "reconciler", "telegram-sender", "web-bff", "worker-runtime"];
  . as $manifest |
  (keys | sort) == ["build", "images", "schema_version", "source"] and
  (.source | keys | sort) == ["committed_at", "repository", "sha", "source_date_epoch", "tree"] and
  (.build | keys | sort) == ["contract_digest", "input_digests", "platform"] and
  .schema_version == 2 and
  .source.repository == "gitcode.com/urandon/sessionless" and
  .source.sha == $sha and (.source.sha | sha) and
  (.source.tree | sha) and
  (.source.committed_at | type == "string" and length > 0) and
  (.source.source_date_epoch | type == "string" and test("^[0-9]+$")) and
  .build.platform == "linux/amd64" and
  (.build.contract_digest | digest) and
  (.build.input_digests | exact_components) and
  (all(.build.input_digests[]; digest)) and
  (.images | exact_components) and
  ([.images[].tagged_reference | capture("^cr\\.yandex/(?<registry>[a-z0-9]+)/[^:]+:[0-9a-f]{40}$").registry] | unique | length == 1) and
  (all(.images | to_entries[];
    (.value | keys | sort) == ["config_digest", "input_digest", "manifest_digest", "reference", "tagged_reference"] and
    (.value.manifest_digest | digest) and
    (.value.config_digest | digest) and
    (.value.input_digest | digest) and
    .value.input_digest == $manifest.build.input_digests[.key] and
    (.value.tagged_reference == ("cr.yandex/" + (.value.tagged_reference | capture("^cr\\.yandex/(?<registry>[a-z0-9]+)/").registry) + "/" + .key + ":" + $sha)) and
    (.value.reference == ((.value.tagged_reference | sub(":[0-9a-f]{40}$"; "")) + "@" + .value.manifest_digest))
  ))
' "$manifest_path" >/dev/null || {
  printf '%s\n' 'deployment image manifest does not satisfy the release contract' >&2
  exit 1
}

if test -e "$asset_dir"; then
  printf 'release asset directory already exists: %s\n' "$asset_dir" >&2
  exit 1
fi
asset_parent=$(dirname "$asset_dir")
mkdir -p "$asset_parent"
stage_dir=$(mktemp -d "$asset_parent/.release-assets.XXXXXX")
cleanup() {
  rm -rf "$stage_dir"
}
trap cleanup EXIT HUP INT TERM

cp "$manifest_path" "$stage_dir/deployment-images.json"
cp "$notes_path" "$stage_dir/release-notes.md"
if command -v sha256sum >/dev/null 2>&1; then
  manifest_sha=$(sha256sum "$stage_dir/deployment-images.json" | awk '{print $1}')
else
  manifest_sha=$(shasum -a 256 "$stage_dir/deployment-images.json" | awk '{print $1}')
fi
printf '%s  deployment-images.json\n' "$manifest_sha" >"$stage_dir/deployment-images.sha256"

mv "$stage_dir" "$asset_dir"
trap - EXIT HUP INT TERM
printf 'staged deterministic release assets in %s\n' "$asset_dir"
