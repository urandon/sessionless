#!/bin/sh
set -eu

candidate_registry_path=${CLOUD_IMAGE_CANDIDATE_REGISTRY_PATH:-${IMAGE_CANDIDATE_REGISTRY_PATH:-.build/image-candidate-registry.json}}
if test ! -f "$candidate_registry_path"; then
  exit 0
fi

jq -e '
  .schema_version == 1 and
  (.source_sha | test("^[0-9a-f]{40}$")) and
  .namespace == "transport"
' "$candidate_registry_path" >/dev/null
container=$(jq -er '.container // ""' "$candidate_registry_path")
network=$(jq -er '.network // ""' "$candidate_registry_path")
source_sha=$(jq -er '.source_sha' "$candidate_registry_path")
case "$container" in
  sessionless-repro-registry-*) ;;
  '') ;;
  *) printf 'refusing to remove unexpected candidate registry container: %s\n' "$container" >&2; exit 2 ;;
esac
case "$network" in
  sessionless-repro-network-*) ;;
  '') ;;
  *) printf 'refusing to remove unexpected candidate registry network: %s\n' "$network" >&2; exit 2 ;;
esac

if test -n "$container"; then
  if docker inspect "$container" >/dev/null 2>&1; then
    candidate_label=$(docker inspect --format '{{index .Config.Labels "dev.sessionless.candidate-registry"}}' "$container")
    source_label=$(docker inspect --format '{{index .Config.Labels "dev.sessionless.source-sha"}}' "$container")
    if test "$candidate_label" != true || test "$source_label" != "$source_sha"; then
      printf 'refusing to remove unverified candidate registry container: %s\n' "$container" >&2
      exit 2
    fi
    docker rm -f "$container" >/dev/null
  fi
fi
if test -n "$network"; then
  if docker network inspect "$network" >/dev/null 2>&1; then
    candidate_label=$(docker network inspect --format \
      '{{index .Labels "dev.sessionless.candidate-registry"}}' "$network")
    source_label=$(docker network inspect --format \
      '{{index .Labels "dev.sessionless.source-sha"}}' "$network")
    if test "$candidate_label" != true || test "$source_label" != "$source_sha"; then
      printf 'refusing to remove unverified candidate registry network: %s\n' "$network" >&2
      exit 2
    fi
    docker network rm "$network" >/dev/null
  fi
fi
if test -n "$container" || test -n "$network"; then
  rm -f "$candidate_registry_path"
fi
