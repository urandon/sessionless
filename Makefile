SHELL := /bin/sh

BIN_DIR := .build/bin
GO_CACHE_DIR := $(CURDIR)/.build/cache/go-build
GO_MOD_CACHE_DIR := $(CURDIR)/.build/cache/go-mod
GO_TMP_DIR := $(CURDIR)/.build/tmp
COMPONENTS := control-api reconciler telegram-sender worker-codex
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

.PHONY: help prepare tools generate fmt fmt-check lint test build integration ci \
	compose-config images dev-up migrate-local dev-down dev-reset clean

help:
	@printf '%s\n' \
		'make tools          validate every pinned developer tool' \
		'make generate       run Go generators' \
		'make test           formatting, static analysis, unit tests, race detector' \
		'make build          build all component binaries' \
		'make integration    run foundation integration tests' \
		'make images         build control-plane and worker images' \
		'make dev-up         build and start the local control API' \
		'make migrate-local  apply local YDB migrations when the runner exists' \
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

ci: generate test build integration

compose-config:
	docker compose --project-name sessionless-dev config --quiet

images:
	docker build --build-arg TARGET=control-api -f build/control.Dockerfile -t sessionless/control-api:dev .
	docker build -f build/worker-codex.Dockerfile -t sessionless/worker-codex:dev .

dev-up:
	docker compose --project-name sessionless-dev up --build --detach control-api

migrate-local:
	@./scripts/migrate-local.sh

dev-down:
	docker compose --project-name sessionless-dev down --remove-orphans

dev-reset:
	@./scripts/dev-reset.sh

clean:
	rm -rf ".build"
