SHELL := /bin/bash

GO            ?= go
GOLANGCI_LINT ?= $(shell go env GOPATH)/bin/golangci-lint
PROTOC        ?= protoc
PROTOC_GEN_GO ?= $(shell go env GOPATH)/bin/protoc-gen-go

PKG           := github.com/gantry/gantry
BIN_DIR       := bin
COVER_FILE    := coverage.txt

PROTO_DIR     := proto
PROTO_FILES   := $(shell find $(PROTO_DIR) -name '*.proto' 2>/dev/null)

.PHONY: all
all: build

## ---- toolchain ---------------------------------------------------------

.PHONY: tools
tools:
	@$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest

## ---- build / test ------------------------------------------------------

.PHONY: build
build:
	@mkdir -p $(BIN_DIR)
	@$(GO) build -o $(BIN_DIR)/gantry ./cmd/gantry

.PHONY: test
test:
	@$(GO) test ./...

.PHONY: cover
cover:
	@$(GO) test -coverprofile=$(COVER_FILE) -covermode=atomic ./...
	@$(GO) tool cover -func=$(COVER_FILE) | tail -1

.PHONY: vet
vet:
	@$(GO) vet ./...

.PHONY: lint
lint:
	@$(GOLANGCI_LINT) run

.PHONY: tidy
tidy:
	@$(GO) mod tidy

## ---- protobuf ----------------------------------------------------------

.PHONY: proto
proto:
	@if [ -z "$(PROTO_FILES)" ]; then echo "no .proto files under $(PROTO_DIR)"; exit 0; fi
	@$(PROTOC) \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(PROTO_DIR) --go_opt=paths=source_relative \
		$(PROTO_FILES)

# Verifies that committed *.pb.go matches the current .proto sources.
# Run after `make proto`; CI uses this to catch un-regenerated bindings.
.PHONY: proto-check
proto-check: proto
	@if ! git diff --quiet -- '$(PROTO_DIR)/**/*.pb.go'; then \
		echo "Generated protobuf bindings are out of date. Run 'make proto' and commit."; \
		git --no-pager diff -- '$(PROTO_DIR)/**/*.pb.go' | head -100; \
		exit 1; \
	fi

## ---- aggregate ---------------------------------------------------------

.PHONY: check
check: vet test

## ---- e2e ---------------------------------------------------------------

# tools-e2e: verify the kind-based suite's CLI prereqs are on PATH.
# Run once after cloning; not chained into `make check` because it
# requires docker to be running.
.PHONY: tools-e2e
tools-e2e:
	@missing=""; \
	for bin in docker kind kubectl; do \
		command -v $$bin >/dev/null 2>&1 || missing="$$missing $$bin"; \
	done; \
	if [ -n "$$missing" ]; then \
		echo "missing e2e prereqs:$$missing"; \
		echo "install instructions: e2e/README.md#prereqs"; \
		exit 1; \
	fi; \
	docker info >/dev/null 2>&1 || { echo "docker engine not running"; exit 1; }; \
	echo "e2e prereqs OK"

# e2e: full kind-based integration run. Sets the e2e build tag so
# the suite (gated by //go:build e2e) is compiled in. Timeout is
# generous because kind cluster boot dominates.
#
# Operator knobs:
#   E2E_KEEP=1 make e2e   leaves the cluster alive after the test
.PHONY: e2e
e2e:
	@$(GO) test -tags=e2e -count=1 -timeout=20m -v ./e2e/...

.PHONY: clean
clean:
	@rm -rf $(BIN_DIR) $(COVER_FILE) e2e/.artifacts
