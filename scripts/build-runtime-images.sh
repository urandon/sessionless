#!/bin/sh
set -eu

version=${VERSION:-dev}
commit=${COMMIT:-$(git rev-parse HEAD)}
built_at=${BUILT_AT:-$(git show -s --format=%cI HEAD)}
source_date_epoch=${SOURCE_DATE_EPOCH:-$(git show -s --format=%ct HEAD)}
platform=${IMAGE_PLATFORM:-}
cache_mode=${DOCKER_BUILD_CACHE:-none}
cache_suffix=$(printf '%s' "${DOCKER_CACHE_SCOPE_SUFFIX:-local}" | tr -c 'A-Za-z0-9_.-' '-')

build_image() {
  name=$1
  dockerfile=$2
  target=${3:-}

  set -- docker buildx build --provenance=false --load \
    --build-arg "VERSION=$version" \
    --build-arg "COMMIT=$commit" \
    --build-arg "BUILT_AT=$built_at" \
    --build-arg "SOURCE_DATE_EPOCH=$source_date_epoch" \
    --file "$dockerfile" \
    --tag "sessionless/${name}:dev"
  if test -n "$platform"; then
    set -- "$@" --platform "$platform"
  fi
  if test -n "$target"; then
    set -- "$@" --build-arg "TARGET=$target"
  fi
  case "$cache_mode" in
    gha)
      set -- "$@" \
        --cache-from "type=gha,scope=${name}-${cache_suffix}" \
        --cache-to "type=gha,mode=max,scope=${name}-${cache_suffix}"
      ;;
    none) ;;
    *) printf 'unsupported DOCKER_BUILD_CACHE: %s\n' "$cache_mode" >&2; exit 2 ;;
  esac
  "$@" .
}

build_image control-api build/control.Dockerfile control-api
build_image web-bff build/control.Dockerfile web-bff
build_image reconciler build/control.Dockerfile reconciler
build_image telegram-sender build/control.Dockerfile telegram-sender
build_image worker-runtime build/worker-runtime.Dockerfile
