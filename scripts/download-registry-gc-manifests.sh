#!/bin/sh
set -eu

test "$#" -eq 3 || {
  printf 'usage: %s inventory.json output-directory keep-count\n' "$0" >&2
  exit 2
}
inventory=$1
output_dir=$2
keep_count=$3

: "${GITHUB_TOKEN:?set GITHUB_TOKEN}"
: "${GITHUB_REPOSITORY:?set GITHUB_REPOSITORY}"
case "$GITHUB_REPOSITORY" in
  */*) ;;
  *) printf '%s\n' 'GITHUB_REPOSITORY must be owner/name' >&2; exit 2 ;;
esac
if test "$keep_count" != 3; then
  printf '%s\n' 'registry cleanup requires exactly the last three deployment manifests' >&2
  exit 2
fi
test -d "$output_dir" || {
  printf 'manifest output directory does not exist: %s\n' "$output_dir" >&2
  exit 2
}
if find "$output_dir" -mindepth 1 -print -quit | grep . >/dev/null; then
  printf 'manifest output directory must be empty: %s\n' "$output_dir" >&2
  exit 2
fi

registry_id=$(jq -er '.registry_id' "$inventory")
case "$registry_id" in
  ''|*[!a-z0-9]*) printf '%s\n' 'inventory registry_id is invalid' >&2; exit 2 ;;
esac

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/sessionless-registry-gc-manifests.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM
api=https://api.github.com
page=1
found=0
seen_shas=

github_get() {
  url=$1
  destination=$2
  curl --fail --silent --show-error --retry 3 --retry-all-errors \
    --header "Authorization: Bearer $GITHUB_TOKEN" \
    --header 'Accept: application/vnd.github+json' \
    --header 'X-GitHub-Api-Version: 2022-11-28' \
    --output "$destination" "$url"
}

while test "$found" -lt "$keep_count"; do
  runs_page="$tmp_dir/runs-$page.json"
  github_get "$api/repos/$GITHUB_REPOSITORY/actions/workflows/ci.yml/runs?branch=main&status=success&event=push&per_page=100&page=$page" "$runs_page"
  jq -e '.total_count >= 0 and (.workflow_runs | type == "array")' "$runs_page" >/dev/null || {
    printf 'invalid GitHub workflow-runs page %s\n' "$page" >&2
    exit 1
  }
  run_count=$(jq '.workflow_runs | length' "$runs_page")
  if test "$run_count" -eq 0; then
    break
  fi

  runs_list="$tmp_dir/runs-$page.jsonl"
  jq -c '.workflow_runs[] | {id, head_sha, head_branch, conclusion}' "$runs_page" >"$runs_list"
  while IFS= read -r run; do
    test "$found" -lt "$keep_count" || break
    run_id=$(printf '%s' "$run" | jq -er '.id | tostring')
    head_sha=$(printf '%s' "$run" | jq -er '.head_sha')
    branch=$(printf '%s' "$run" | jq -er '.head_branch')
    conclusion=$(printf '%s' "$run" | jq -er '.conclusion')
    if ! printf '%s' "$run_id" | jq -Re 'test("^[0-9]+$")' >/dev/null ||
      ! printf '%s' "$head_sha" | jq -Re 'test("^[0-9a-f]{40}$")' >/dev/null ||
      test "$branch" != main || test "$conclusion" != success; then
      printf 'invalid successful main workflow run evidence on page %s\n' "$page" >&2
      exit 1
    fi
    case " $seen_shas " in
      *" $head_sha "*) continue ;;
    esac

    artifact_page=1
    artifact_id=
    artifact_matches=0
    while :; do
      artifacts_page="$tmp_dir/artifacts-$run_id-$artifact_page.json"
      github_get "$api/repos/$GITHUB_REPOSITORY/actions/runs/$run_id/artifacts?per_page=100&page=$artifact_page" "$artifacts_page"
      jq -e '.total_count >= 0 and (.artifacts | type == "array")' "$artifacts_page" >/dev/null || {
        printf 'invalid artifact page for workflow run %s\n' "$run_id" >&2
        exit 1
      }
      page_matches=$(jq --arg name "deployment-images-$head_sha" \
        '[.artifacts[] | select(.name == $name and .expired == false)] | length' "$artifacts_page")
      artifact_matches=$((artifact_matches + page_matches))
      if test "$page_matches" -gt 0; then
        artifact_id=$(jq -er --arg name "deployment-images-$head_sha" \
          '.artifacts[] | select(.name == $name and .expired == false) | .id' "$artifacts_page")
      fi
      artifact_total=$(jq '.total_count' "$artifacts_page")
      if test $((artifact_page * 100)) -ge "$artifact_total"; then
        break
      fi
      artifact_page=$((artifact_page + 1))
      if test "$artifact_page" -gt 100; then
        printf 'artifact pagination exceeded the safety limit for run %s\n' "$run_id" >&2
        exit 1
      fi
    done
    if test "$artifact_matches" -eq 0; then
      continue
    fi
    if test "$artifact_matches" -ne 1 ||
      ! printf '%s' "$artifact_id" | jq -Re 'test("^[0-9]+$")' >/dev/null; then
      printf 'expected exactly one deployment manifest artifact for run %s\n' "$run_id" >&2
      exit 1
    fi

    archive="$tmp_dir/artifact-$artifact_id.zip"
    github_get "$api/repos/$GITHUB_REPOSITORY/actions/artifacts/$artifact_id/zip" "$archive"
    entries=$(unzip -Z1 "$archive" | awk '/(^|\/)deployment-images\.json$/ && $0 !~ /\.\./ {print}')
    if test "$(printf '%s\n' "$entries" | sed '/^$/d' | wc -l | tr -d ' ')" -ne 1; then
      printf 'artifact %s does not contain exactly one safe deployment-images.json\n' "$artifact_id" >&2
      exit 1
    fi
    entry=$(printf '%s\n' "$entries" | sed -n '1p')
    manifest="$output_dir/deployment-images-$(printf '%02d' $((found + 1)))-$head_sha.json"
    unzip -p "$archive" "$entry" >"$manifest"
    jq -e \
      --arg sha "$head_sha" \
      --arg registry "$registry_id" \
      '.schema_version == 2 and .source.sha == $sha and
       (.images | keys | sort) == ["control-api", "reconciler", "telegram-sender", "web-bff", "worker-runtime"] and
       all(.images | to_entries[];
         (.value.manifest_digest | test("^sha256:[0-9a-f]{64}$")) and
         .value.reference == ("cr.yandex/" + $registry + "/" + .key + "@" + .value.manifest_digest))' \
      "$manifest" >/dev/null || {
      printf 'deployment manifest from run %s failed strict validation\n' "$run_id" >&2
      exit 1
    }
    seen_shas="$seen_shas $head_sha"
    found=$((found + 1))
  done <"$runs_list"

  test "$found" -lt "$keep_count" || break
  total_count=$(jq '.total_count' "$runs_page")
  if test $((page * 100)) -ge "$total_count"; then
    break
  fi
  page=$((page + 1))
  if test "$page" -gt 100; then
    printf '%s\n' 'workflow-run pagination exceeded the safety limit' >&2
    exit 1
  fi
done

if test "$found" -ne "$keep_count"; then
  printf 'found %s valid distinct deployment manifests; exactly %s are required\n' \
    "$found" "$keep_count" >&2
  exit 1
fi
