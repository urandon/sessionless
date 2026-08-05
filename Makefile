SHELL := /bin/sh
TERRAFORM ?= terraform

BIN_DIR := .build/bin
GO_CACHE_DIR := $(CURDIR)/.build/cache/go-build
GO_MOD_CACHE_DIR := $(CURDIR)/.build/cache/go-mod
GO_TMP_DIR := $(CURDIR)/.build/tmp
COMPONENTS := control-api reconciler telegram-sender telegram-fake worker-runtime schema-migrate schema-inspect schema-backfill preprod-reset deployment-lock
VERSION ?= dev
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || printf unknown)
BUILT_AT ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
export GOCACHE := $(GO_CACHE_DIR)
export GOMODCACHE := $(GO_MOD_CACHE_DIR)
export GOTMPDIR := $(GO_TMP_DIR)
export GOTELEMETRY := off
LDFLAGS := -s -w \
	-X gitcode.com/urandon/sessionless/internal/buildinfo.Version=$(VERSION) \
	-X gitcode.com/urandon/sessionless/internal/buildinfo.Commit=$(COMMIT) \
	-X gitcode.com/urandon/sessionless/internal/buildinfo.BuiltAt=$(BUILT_AT)

.PHONY: help prepare tools generate fmt fmt-check lint test build integration ydb-integration local-integration e2e-local ci terraform-ci \
	compose-config images dev-up dev-seed migrate-local migration-status partition-status partition-backfill cloud-app-reset-plan cloud-app-reset \
	worker-once dev-down dev-reset clean

help:
	@printf '%s\n' \
		'make tools          validate every pinned developer tool' \
		'make generate       run Go generators' \
		'make test           formatting, static analysis, unit tests, race detector' \
		'make build          build all component binaries' \
		'make integration    run foundation integration tests' \
		'make ydb-integration run YDB Local schema and concurrency tests' \
		'make local-integration run YDB/S3/SQS/Telegram adapter tests against the local stand' \
		'make e2e-local      run the deterministic two-tenant black-box slice' \
		'make terraform-ci   format-check and validate Terraform roots' \
		'make images         build control-plane and worker images' \
		'make dev-up         start, initialize, migrate, seed, and verify the local stand' \
		'make dev-seed       idempotently load synthetic local fixtures' \
		'make migrate-local  apply embedded YDB migrations' \
		'make migration-status inspect Goose and checksum state' \
		'make partition-status inspect physical keys and partition settings as JSON' \
		'make partition-backfill copy legacy ready/expiry rows into the v2 bucketed layout' \
		'make cloud-app-reset-plan inspect the exact guarded cloud-dev reset target' \
		'make cloud-app-reset execute the typed-confirmed cloud-dev application reset' \
		'make worker-once    consume at most one admitted run with the deterministic harness' \
		'make dev-down       stop the local stack' \
		'make dev-reset      guarded deletion of local Compose volumes'

tools:
	@./scripts/check-tools.sh

prepare:
	@mkdir -p "$(GO_CACHE_DIR)" "$(GO_MOD_CACHE_DIR)" "$(GO_TMP_DIR)"

generate: prepare
	go generate ./...

fmt:
	gofmt -w $$(find . -name '*.go' -type f -not -path './.git/*' -not -path './.build/*')

fmt-check:
	@./scripts/check-format.sh

lint: prepare
	go vet ./...

test: prepare fmt-check lint
	go test -race ./...

build: prepare
	@mkdir -p $(BIN_DIR)
	@set -e; for component in $(COMPONENTS); do \
		printf 'building %s\n' "$$component"; \
		CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" \
			-o "$(BIN_DIR)/$$component" "./cmd/$$component"; \
	done

integration: prepare
	go test -race -tags=integration ./test/integration/...

ydb-integration: prepare
	go test -race -tags=ydbintegration ./test/ydbintegration/...

local-integration: prepare
	YDB_CONNECTION_STRING="$${YDB_CONNECTION_STRING:-grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric}" \
	YDB_ANONYMOUS_CREDENTIALS="$${YDB_ANONYMOUS_CREDENTIALS:-1}" \
	go test -race -tags=localintegration ./test/localintegration/...

e2e-local: prepare
	@./scripts/e2e-local.sh

ci: generate test build integration

terraform-ci:
	$(TERRAFORM) fmt -recursive -check -diff infra/terraform
	$(TERRAFORM) -chdir=infra/terraform/bootstrap init -backend=false -input=false
	$(TERRAFORM) -chdir=infra/terraform/bootstrap validate
	$(TERRAFORM) -chdir=infra/terraform/cloud-dev init -backend=false -input=false
	$(TERRAFORM) -chdir=infra/terraform/cloud-dev validate

compose-config:
	docker compose --project-name sessionless-dev config --quiet

images:
	docker build --build-arg TARGET=control-api -f build/control.Dockerfile -t sessionless/control-api:dev .
	docker build --build-arg TARGET=reconciler -f build/control.Dockerfile -t sessionless/reconciler:dev .
	docker build --build-arg TARGET=telegram-sender -f build/control.Dockerfile -t sessionless/telegram-sender:dev .
	docker build -f build/worker-runtime.Dockerfile -t sessionless/worker-runtime:dev .

dev-up:
	@./scripts/dev-up.sh

dev-seed:
	@./scripts/seed-local.sh

migrate-local: prepare
	@./scripts/migrate-local.sh

migration-status: prepare
	go run ./cmd/schema-migrate status

partition-status: prepare
	go run ./cmd/schema-inspect

partition-backfill: prepare
	go run ./cmd/schema-backfill

cloud-app-reset-plan: prepare
	@./scripts/cloud-app-reset.sh plan

cloud-app-reset: prepare
	@./scripts/cloud-app-reset.sh execute

worker-once:
	docker compose --project-name sessionless-dev --profile worker run --rm worker-runtime

dev-down:
	docker compose --project-name sessionless-dev down --remove-orphans

dev-reset:
	@./scripts/dev-reset.sh

clean:
	rm -rf ".build"
