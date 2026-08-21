SHELL := /bin/sh
TERRAFORM ?= terraform

BIN_DIR := .build/bin
GO_CACHE_DIR := $(CURDIR)/.build/cache/go-build
GO_MOD_CACHE_DIR := $(CURDIR)/.build/cache/go-mod
GO_TMP_DIR := $(CURDIR)/.build/tmp
COMPONENTS := control-api web-bff reconciler telegram-sender telegram-fake oidc-fake worker-runtime schema-migrate schema-inspect schema-backfill preprod-reset deployment-lock web-bootstrap session-delete
GO_PACKAGE_PATTERNS := ./cmd/... ./internal/... ./migrations/...
WEB_DIR := web
WEB_BUILD_DIR := $(WEB_DIR)/build
WEB_EMBED_DIR := internal/webstatic/dist
PLAYWRIGHT_INSTALL_ARGS ?= chromium
VERSION ?= dev
COMMIT ?= $(shell git rev-parse HEAD 2>/dev/null || printf unknown)
BUILT_AT ?= $(shell git show -s --format=%cI HEAD 2>/dev/null || printf unknown)
export GOCACHE := $(GO_CACHE_DIR)
export GOMODCACHE := $(GO_MOD_CACHE_DIR)
export GOTMPDIR := $(GO_TMP_DIR)
export GOTELEMETRY := off
LDFLAGS := -s -w \
	-X gitcode.com/urandon/sessionless/internal/buildinfo.Version=$(VERSION) \
	-X gitcode.com/urandon/sessionless/internal/buildinfo.Commit=$(COMMIT) \
	-X gitcode.com/urandon/sessionless/internal/buildinfo.BuiltAt=$(BUILT_AT)

.PHONY: help prepare tools web-tools go-package-layout generate fmt fmt-check lint test build web-install web-openapi-check web-check web-build web-stage web-ci web-browser-install web-browser-test integration ydb-integration local-integration e2e-local ci image-publication-test budget-policy-test web-deployment-policy-test terraform-ci cloudflare-edge-ci \
	compose-config images dev-up dev-seed migrate-local migration-status partition-status partition-backfill cloud-app-reset-plan cloud-app-reset session-delete-request session-delete-plan session-delete session-hold session-release-hold \
	worker-once web-bootstrap dev-down dev-reset clean

help:
	@printf '%s\n' \
		'make tools          validate every pinned developer tool' \
		'make web-ci         install locked WebUI dependencies and run deterministic checks/build' \
		'make web-browser-install install the pinned Chromium browser runtime' \
		'make web-browser-test run the separately gated Playwright and axe suites' \
		'make generate       run Go generators' \
		'make test           formatting, static analysis, unit tests, race detector' \
		'make build          build all component binaries' \
		'make integration    run foundation integration tests' \
		'make ydb-integration run YDB Local schema and concurrency tests' \
		'make local-integration run YDB/S3/SQS/Telegram adapter tests against the local stand' \
		'make e2e-local      run the deterministic two-tenant black-box slice' \
		'make image-publication-test validate immutable image publication guards' \
		'make terraform-ci   validate Terraform, run a mocked Web plan, and enforce policies' \
		'make cloudflare-edge-ci test and dry-run bundle the Telegram edge Worker' \
		'make images         build control-plane and worker images' \
		'make dev-up         start, initialize, migrate, seed, and verify the local stand' \
		'make dev-seed       idempotently load synthetic local fixtures' \
		'make migrate-local  apply embedded YDB migrations' \
		'make migration-status inspect Goose and checksum state' \
		'make partition-status inspect physical keys and partition settings as JSON' \
		'make partition-backfill copy legacy ready/expiry and lifecycle-index rows, then mark cutover' \
		'make cloud-app-reset-plan inspect the exact guarded cloud-dev reset target' \
		'make cloud-app-reset execute the typed-confirmed cloud-dev application reset' \
		'make session-delete-request record an owner-authorized deletion request' \
		'make session-delete-plan print the bounded exact-object deletion plan' \
		'make session-delete execute the exact typed-confirmed session deletion' \
		'make session-hold set a durable legal hold on one session' \
		'make session-release-hold release a durable legal hold on one session' \
		'make worker-once    consume at most one admitted run with the deterministic harness' \
		'make web-bootstrap  create an audited, confirmed cloud-dev Web membership' \
		'make dev-down       stop the local stack' \
		'make dev-reset      guarded deletion of local Compose volumes'

tools:
	@./scripts/check-tools.sh

web-tools:
	@./scripts/check-tools.sh web

go-package-layout:
	@unexpected=$$(find . \
		\( -path './.git' -o -path './.build' -o -path './.gitcode' -o -path '*/node_modules' \) -prune -o \
		-type f -name '*.go' -print | \
		grep -Ev '^\./(cmd|internal|migrations|test)/' || true); \
	if [ -n "$$unexpected" ]; then \
		printf 'Go files outside the explicit package roots:\n%s\n' "$$unexpected"; \
		printf 'Update GO_PACKAGE_PATTERNS so project packages cannot be skipped.\n'; \
		exit 1; \
	fi

prepare:
	@mkdir -p "$(GO_CACHE_DIR)" "$(GO_MOD_CACHE_DIR)" "$(GO_TMP_DIR)"

generate: prepare go-package-layout
	go generate $(GO_PACKAGE_PATTERNS)

fmt:
	gofmt -w $$(find . \
		\( -path './.git' -o -path './.build' -o -path './.gitcode' -o -path '*/node_modules' \) -prune -o \
		-name '*.go' -type f -print)

fmt-check:
	@./scripts/check-format.sh

lint: prepare go-package-layout
	go vet $(GO_PACKAGE_PATTERNS)

test: prepare fmt-check lint
	go test -race $(GO_PACKAGE_PATTERNS)

build: prepare web-stage
	@mkdir -p $(BIN_DIR)
	@set -e; for component in $(COMPONENTS); do \
		printf 'building %s\n' "$$component"; \
		CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" \
			-o "$(BIN_DIR)/$$component" "./cmd/$$component"; \
	done

web-install: web-tools
	npm --prefix $(WEB_DIR) ci

web-openapi-check: web-install
	npm --prefix $(WEB_DIR) run api:check

web-check: web-openapi-check
	npm --prefix $(WEB_DIR) run format:check
	npm --prefix $(WEB_DIR) run lint
	npm --prefix $(WEB_DIR) run check
	npm --prefix $(WEB_DIR) run test

web-build: web-install
	npm --prefix $(WEB_DIR) run build

web-stage: web-check web-build
	@test -f "$(WEB_BUILD_DIR)/200.html"
	@mkdir -p "$(WEB_EMBED_DIR)"
	@find "$(WEB_EMBED_DIR)" -mindepth 1 ! -name '.keep' ! -name '.gitignore' -delete
	@cp -R "$(WEB_BUILD_DIR)/." "$(WEB_EMBED_DIR)/"

web-ci: web-stage

web-browser-install: web-install
	npm --prefix $(WEB_DIR) exec playwright -- install $(PLAYWRIGHT_INSTALL_ARGS)

web-browser-test: web-install
	npm --prefix $(WEB_DIR) run test:e2e
	npm --prefix $(WEB_DIR) run test:a11y

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

ci: web-ci generate test build integration image-publication-test

image-publication-test:
	@./scripts/test-image-publication.sh

budget-policy-test:
	@./scripts/test-cloud-budget-policy.sh

web-deployment-policy-test:
	@./scripts/test-web-deployment-policy.sh

terraform-ci:
	$(TERRAFORM) fmt -recursive -check -diff infra/terraform
	$(TERRAFORM) -chdir=infra/terraform/bootstrap init -backend=false -input=false
	$(TERRAFORM) -chdir=infra/terraform/bootstrap validate
	$(TERRAFORM) -chdir=infra/terraform/cloud-dev init -backend=false -input=false
	$(TERRAFORM) -chdir=infra/terraform/cloud-dev validate
	$(TERRAFORM) -chdir=infra/terraform/cloud-dev test -filter=web_deployment.tftest.hcl
	$(MAKE) budget-policy-test
	$(MAKE) web-deployment-policy-test

cloudflare-edge-ci:
	npm --prefix infra/cloudflare/telegram-edge ci
	npm --prefix infra/cloudflare/telegram-edge run check

compose-config:
	docker compose --project-name sessionless-dev config --quiet

images:
	@VERSION="$(VERSION)" COMMIT="$(COMMIT)" BUILT_AT="$(BUILT_AT)" ./scripts/build-runtime-images.sh

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

session-delete-request: prepare
	go run ./cmd/session-delete request

session-delete-plan: prepare
	go run ./cmd/session-delete plan

session-delete: prepare
	go run ./cmd/session-delete execute

session-hold: prepare
	go run ./cmd/session-delete hold

session-release-hold: prepare
	go run ./cmd/session-delete release-hold

worker-once:
	docker compose --project-name sessionless-dev --profile worker run --rm worker-runtime

web-bootstrap: prepare
	go run ./cmd/web-bootstrap

dev-down:
	docker compose --project-name sessionless-dev down --remove-orphans

dev-reset:
	@./scripts/dev-reset.sh

clean:
	rm -rf ".build"
