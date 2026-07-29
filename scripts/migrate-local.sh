#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
migration_dir="$repo_root/migrations/ydb"
migrations=$(find "$migration_dir" -maxdepth 1 -name '*.sql' -type f | sort)

if [ -z "$migrations" ]; then
	printf 'No embedded YDB migrations found.\n'
	exit 0
fi

if [ -z "${YDB_CONNECTION_STRING:-}" ]; then
	printf 'YDB_CONNECTION_STRING is required. See .env.example for the local safe default.\n'
	exit 1
fi

cd "$repo_root"
exec go run ./cmd/schema-migrate
