#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
files=$(find "$repo_root" \
	\( -path '*/.git' -o -path '*/.build' -o -path '*/.gitcode' -o -path '*/node_modules' \) -prune -o \
	-name '*.go' -type f -print)

if [ -z "$files" ]; then
	exit 0
fi

unformatted=$(gofmt -l $files)
if [ -n "$unformatted" ]; then
	printf 'Go files need formatting:\n%s\n' "$unformatted"
	exit 1
fi
