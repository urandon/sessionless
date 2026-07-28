#!/bin/sh
set -eu

project_name=sessionless-dev

if [ "${CONFIRM_LOCAL_RESET:-}" != "$project_name" ]; then
	printf 'Refusing to delete local Compose volumes.\n'
	printf 'Re-run with CONFIRM_LOCAL_RESET=%s make dev-reset\n' "$project_name"
	exit 1
fi

docker compose --project-name "$project_name" down --volumes --remove-orphans
