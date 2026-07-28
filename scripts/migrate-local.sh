#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
migration_dir="$repo_root/migrations/ydb"
migrations=$(find "$migration_dir" -maxdepth 1 -name '*.sql' -type f | sort)

if [ -z "$migrations" ]; then
	printf 'No YDB migrations exist yet; schema work starts in implementation issue #5.\n'
	exit 0
fi

printf 'YDB migrations are present, but the migration runner is intentionally assigned to issue #5.\n'
printf 'Refusing to apply files without a durable migration ledger.\n'
exit 1
