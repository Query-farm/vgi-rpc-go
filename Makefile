# Development Makefile for vgi-rpc-go

# Configurable paths — override with env vars or on the command line.
VGI_RPC_PYTHON_PATH ?= /Users/rusty/Development/vgi-rpc
GO_CONFORMANCE_WORKER ?= $(CURDIR)/conformance-worker
export VGI_RPC_PYTHON_PATH GO_CONFORMANCE_WORKER

COVDIR := $(CURDIR)/_covdata

.PHONY: lint test coverage clean

# --- Lint ------------------------------------------------------------------

lint:
	go build ./...
	go vet ./...
	staticcheck ./...

# --- Build -----------------------------------------------------------------

conformance-worker:
	go build -o conformance-worker ./conformance/cmd/vgi-rpc-conformance-go

conformance-worker-cover:
	go build -cover -covermode=atomic -o conformance-worker ./conformance/cmd/vgi-rpc-conformance-go

# --- Test ------------------------------------------------------------------

test: conformance-worker
	python -m pytest test_go_conformance.py -v

# --- Coverage --------------------------------------------------------------

coverage: conformance-worker-cover
	rm -rf $(COVDIR) && mkdir -p $(COVDIR)
	GOCOVERDIR=$(COVDIR) python -m pytest test_go_conformance.py -v
	go tool covdata textfmt -i=$(COVDIR) -o=coverage-go.txt
	@echo "Coverage written to coverage-go.txt"

# --- Clean -----------------------------------------------------------------

clean:
	rm -f conformance-worker
	rm -rf $(COVDIR) coverage-go.txt
