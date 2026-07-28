#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
files=$(find "$repo_root" -name '*.go' -type f -not -path '*/.git/*' -not -path '*/.build/*')

if [ -z "$files" ]; then
	exit 0
fi

unformatted=$(gofmt -l $files)
if [ -n "$unformatted" ]; then
	printf 'Go files need formatting:\n%s\n' "$unformatted"
	exit 1
fi
