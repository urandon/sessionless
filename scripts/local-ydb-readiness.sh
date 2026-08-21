#!/bin/sh

# This file is sourced by dev-up.sh and by deterministic policy tests. Keep it
# side-effect free: it must only declare helpers.

classify_ydb_startup_failure() {
	migration_log=$1
	service_log=$2

	if grep -Eq 'ReasonBootBSError|NumUnconnectedDisks' "$migration_log" "$service_log"; then
		printf '%s\n' 'boot-storage-failure'
		return 0
	fi

	if grep -Fq "database doesn't have storage pools" "$migration_log"; then
		printf '%s\n' 'retry'
		return 0
	fi

	printf '%s\n' 'fatal'
}
