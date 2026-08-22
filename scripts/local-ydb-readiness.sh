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
		printf '%s\n' 'retry-storage-pools'
		return 0
	fi

	# The monitoring endpoint can become live just before the local SDK endpoint.
	# Match only the SDK's exact loopback dial-timeout shape. In particular, a
	# generic deadline or a dial failure for a configured remote host stays fatal.
	# Zero or more backslashes cover nested escaping by slog and the SDK's
	# structured error wrappers while the endpoint and suffix stay exact.
	if grep -Eq 'failed to dial (\\)*"(localhost|127\.0\.0\.1|\[::1\]):[0-9]+(\\)*": context deadline exceeded' "$migration_log"; then
		printf '%s\n' 'retry-local-dial'
		return 0
	fi

	printf '%s\n' 'fatal'
}
