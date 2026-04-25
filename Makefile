# Development Makefile for vgi-rpc-go

# Configurable paths — override with env vars or on the command line.
GO_CONFORMANCE_WORKER ?= $(CURDIR)/conformance-worker
PYTHON ?= /Users/rusty/Development/vgi-rpc/.venv/bin/python
export GO_CONFORMANCE_WORKER

GOBIN := $(shell go env GOPATH)/bin
COVDIR := $(CURDIR)/_covdata

.PHONY: build lint test coverage leakcheck docs clean

# --- Build -----------------------------------------------------------------

build:
	go build ./...
	cd vgirpc/otel && go build ./...
	cd vgirpc/jwtauth && go build ./...
	cd vgirpc/s3 && go build ./...
	cd vgirpc/gcs && go build ./...

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
	cd vgirpc/jwtauth && go vet ./...

# --- Test ------------------------------------------------------------------

test: conformance-worker
	$(PYTHON) -m pytest test_go_conformance.py -v

# --- Coverage --------------------------------------------------------------

coverage: conformance-worker-cover
	rm -rf $(COVDIR) && mkdir -p $(COVDIR)
	GOCOVERDIR=$(COVDIR) $(PYTHON) -m pytest test_go_conformance.py -v
	go tool covdata textfmt -i=$(COVDIR) -o=coverage-go.txt
	@echo "Coverage written to coverage-go.txt"

# --- Leak check ------------------------------------------------------------
# Builds the conformance worker with -tags leakcheck so every internal
# Arrow allocation routes through a single shared CheckedAllocator. The
# worker prints LeakCheckSummary to stderr on exit; pytest captures it.

leakcheck:
	go build -tags leakcheck -o conformance-worker ./conformance/cmd/vgi-rpc-conformance-go
	$(PYTHON) -m pytest test_go_conformance.py -v -s 2>&1 | grep -E "vgirpc leakcheck|passed|failed" | tail -20

# --- Docs ------------------------------------------------------------------

docs:
	mkdocs serve

# --- Clean -----------------------------------------------------------------

clean:
	rm -f conformance-worker benchmark-worker
	rm -rf $(COVDIR) coverage-go.txt
