#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
# shellcheck disable=SC1091
. "$repo_root/tools/versions.env"

failures=0

report_missing() {
	printf '%-18s MISSING (expected %s)\n' "$1" "$2"
	failures=$((failures + 1))
}

report_version() {
	name=$1
	expected=$2
	actual=$3
	if [ "$actual" = "$expected" ]; then
		printf '%-18s OK %s\n' "$name" "$actual"
	else
		printf '%-18s MISMATCH got %s, expected %s\n' "$name" "$actual" "$expected"
		failures=$((failures + 1))
	fi
}

if command -v go >/dev/null 2>&1; then
	actual=$(go version | awk '{print $3}' | sed 's/^go//')
	report_version "Go" "$GO_VERSION" "$actual"
else
	report_missing "Go" "$GO_VERSION"
fi

if command -v docker >/dev/null 2>&1; then
	actual=$(docker compose version --short 2>/dev/null | sed 's/^v//')
	if [ -n "$actual" ]; then
		report_version "Docker Compose" "$DOCKER_COMPOSE_VERSION" "$actual"
	else
		report_missing "Docker Compose" "$DOCKER_COMPOSE_VERSION"
	fi
else
	report_missing "Docker Compose" "$DOCKER_COMPOSE_VERSION"
fi

if command -v terraform >/dev/null 2>&1; then
	actual=$(terraform version | awk 'NR == 1 {gsub(/^v/, "", $2); print $2}')
	report_version "Terraform" "$TERRAFORM_VERSION" "$actual"
else
	report_missing "Terraform" "$TERRAFORM_VERSION"
fi

if command -v yc >/dev/null 2>&1; then
	actual=$(yc version --semantic)
	report_version "Yandex Cloud CLI" "$YC_CLI_VERSION" "$actual"
else
	report_missing "Yandex Cloud CLI" "$YC_CLI_VERSION"
fi

if command -v ydb >/dev/null 2>&1; then
	actual=$(ydb version --semantic)
	report_version "YDB CLI" "$YDB_CLI_VERSION" "$actual"
else
	report_missing "YDB CLI" "$YDB_CLI_VERSION"
fi

if command -v goose >/dev/null 2>&1; then
	actual=$(goose -version 2>&1 | sed -E 's/.*v?([0-9]+\.[0-9]+\.[0-9]+).*/\1/')
	report_version "Goose" "$GOOSE_VERSION" "$actual"
else
	report_missing "Goose" "$GOOSE_VERSION"
fi

if [ "$failures" -ne 0 ]; then
	printf '\n%d tool check(s) failed. See docs/development.md for installation guidance.\n' "$failures"
	exit 1
fi

printf '\nAll pinned tools are available.\n'
