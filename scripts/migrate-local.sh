#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
migration_dir="$repo_root/migrations/ydb"
migrations=$(find "$migration_dir" -maxdepth 1 -name '*.sql' -type f | sort)

if [ -z "$migrations" ]; then
	printf 'No YDB migrations exist yet; schema work starts in implementation issue #5.\n'
	exit 0
fi

if ! command -v goose >/dev/null 2>&1; then
	printf 'Goose is required to apply YDB migrations. Run make tools for the pinned version.\n'
	exit 1
fi

if [ -z "${YDB_CONNECTION_STRING:-}" ]; then
	printf 'YDB_CONNECTION_STRING is required. See .env.example for the local safe default.\n'
	exit 1
fi

exec goose -env=none -dir "$migration_dir" ydb "$YDB_CONNECTION_STRING" up
