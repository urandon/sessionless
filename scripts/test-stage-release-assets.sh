#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
fixture=$(mktemp -d)
cleanup() {
  rm -rf "$fixture"
}
trap cleanup EXIT HUP INT TERM

mkdir -p "$fixture/repo/scripts" "$fixture/repo/.build/release"
cp "$repo_root/scripts/stage-release-assets.sh" "$fixture/repo/scripts/"
git -C "$fixture/repo" init -q
git -C "$fixture/repo" config user.name 'Release Fixture'
git -C "$fixture/repo" config user.email 'release-fixture@example.invalid'
printf '%s\n' fixture >"$fixture/repo/source.txt"
git -C "$fixture/repo" add source.txt scripts/stage-release-assets.sh
git -C "$fixture/repo" commit -qm 'fixture'
git -C "$fixture/repo" tag v1.2.3
source_sha=$(git -C "$fixture/repo" rev-parse HEAD)
source_tree=$(git -C "$fixture/repo" rev-parse 'HEAD^{tree}')

printf '%s\n' '# Release v1.2.3' >"$fixture/repo/.build/release/release-notes.md"
jq -S -n --arg sha "$source_sha" --arg tree "$source_tree" '
  def components: ["control-api", "web-bff", "reconciler", "telegram-sender", "worker-runtime"];
  def manifest_digest: "sha256:" + ("a" * 64);
  def config_digest: "sha256:" + ("b" * 64);
  {
    schema_version: 2,
    source: {
      repository: "gitcode.com/urandon/sessionless",
      sha: $sha,
      tree: $tree,
      committed_at: "2026-08-21T00:00:00Z",
      source_date_epoch: "1787270400"
    },
    build: {
      platform: "linux/amd64",
      contract_digest: ("sha256:" + ("c" * 64)),
      input_digests: (reduce components[] as $name ({}; .[$name] = ("sha256:" + ("d" * 64))))
    },
    images: (reduce components[] as $name ({};
      .[$name] = {
        tagged_reference: ("cr.yandex/crpfixture/" + $name + ":" + $sha),
        reference: ("cr.yandex/crpfixture/" + $name + "@" + manifest_digest),
        manifest_digest: manifest_digest,
        config_digest: config_digest,
        input_digest: ("sha256:" + ("d" * 64))
      }
    ))
  }
' >"$fixture/repo/.build/deployment-images.json"

(
  cd "$fixture/repo"
  RELEASE_TAG=v1.2.3 \
    RELEASE_SOURCE_SHA="$source_sha" \
    RELEASE_MANIFEST_PATH=.build/deployment-images.json \
    RELEASE_NOTES_PATH=.build/release/release-notes.md \
    RELEASE_ASSET_DIR=.build/release/assets \
    sh scripts/stage-release-assets.sh
)

test -f "$fixture/repo/.build/release/assets/deployment-images.json"
test -f "$fixture/repo/.build/release/assets/deployment-images.sha256"
test -f "$fixture/repo/.build/release/assets/release-notes.md"
(
  cd "$fixture/repo/.build/release/assets"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -c deployment-images.sha256 >/dev/null
  else
    expected=$(awk '{print $1}' deployment-images.sha256)
    actual=$(shasum -a 256 deployment-images.json | awk '{print $1}')
    test "$actual" = "$expected"
  fi
)

jq '.images.unexpected = .images["control-api"]' \
  "$fixture/repo/.build/deployment-images.json" >"$fixture/repo/.build/deployment-images-invalid.json"
if (
  cd "$fixture/repo"
  RELEASE_TAG=v1.2.3 \
    RELEASE_SOURCE_SHA="$source_sha" \
    RELEASE_MANIFEST_PATH=.build/deployment-images-invalid.json \
    RELEASE_NOTES_PATH=.build/release/release-notes.md \
    RELEASE_ASSET_DIR=.build/release/invalid-assets \
    sh scripts/stage-release-assets.sh
) >/dev/null 2>&1; then
  printf '%s\n' 'staging accepted a manifest with an unexpected component' >&2
  exit 1
fi

printf '%s\n' 'release asset staging tests passed'
