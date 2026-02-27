# Development Makefile for vgi-rpc-go

# Configurable paths — override with env vars or on the command line.
VGI_RPC_PYTHON_PATH ?= /Users/rusty/Development/vgi-rpc
GO_CONFORMANCE_WORKER ?= $(CURDIR)/conformance-worker
PYTHON ?= $(VGI_RPC_PYTHON_PATH)/.venv/bin/python
export VGI_RPC_PYTHON_PATH GO_CONFORMANCE_WORKER

GOBIN := $(shell go env GOPATH)/bin
COVDIR := $(CURDIR)/_covdata

.PHONY: build lint test coverage docs clean

# --- Build -----------------------------------------------------------------

build:
	go build ./...
	cd vgirpc/otel && go build ./...

conformance-worker:
	go build -o conformance-worker ./conformance/cmd/vgi-rpc-conformance-go

conformance-worker-cover:
	go build -cover -covermode=atomic -o conformance-worker ./conformance/cmd/vgi-rpc-conformance-go

benchmark-worker:
	go build -o benchmark-worker ./benchmark/cmd/vgi-rpc-benchmark-go

# --- Lint ------------------------------------------------------------------

lint:
	go build ./...
	go vet ./...
	$(GOBIN)/staticcheck ./...
	cd vgirpc/otel && go vet ./...

# --- Test ------------------------------------------------------------------

test: conformance-worker
	$(PYTHON) -m pytest test_go_conformance.py -v

# --- Coverage --------------------------------------------------------------

coverage: conformance-worker-cover
	rm -rf $(COVDIR) && mkdir -p $(COVDIR)
	GOCOVERDIR=$(COVDIR) $(PYTHON) -m pytest test_go_conformance.py -v
	go tool covdata textfmt -i=$(COVDIR) -o=coverage-go.txt
	@echo "Coverage written to coverage-go.txt"

# --- Docs ------------------------------------------------------------------

docs:
	mkdocs serve

# --- Clean -----------------------------------------------------------------

clean:
	rm -f conformance-worker benchmark-worker
	rm -rf $(COVDIR) coverage-go.txt
