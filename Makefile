# Development Makefile for vgi-rpc-go

# Configurable paths — override with env vars or on the command line.
GO_CONFORMANCE_WORKER ?= $(CURDIR)/conformance-worker
export GO_CONFORMANCE_WORKER

GOBIN := $(shell go env GOPATH)/bin
COVDIR := $(CURDIR)/_covdata

# --- Python environment ----------------------------------------------------
# The conformance suite is driven by the released vgi-rpc package from PyPI.
# By default the test targets bootstrap a repo-local .venv and install it, so
# a fresh clone can run `make test` with no manual setup.
#
# To use an interpreter you manage yourself (e.g. a checkout of
# vgi-rpc-python installed with -e), override PYTHON and the bootstrap is
# skipped entirely:
#
#	PYTHON=/path/to/python make test
VENV := $(CURDIR)/.venv
PYTHON ?= $(VENV)/bin/python
VGI_RPC_SPEC ?= vgi-rpc[http,cli,external]>=0.20.0

# Bootstrap only when PYTHON came from this file — never when the caller
# supplied their own interpreter.
ifeq ($(filter command line environment,$(origin PYTHON)),)
PYTHON_BOOTSTRAP := $(VENV)/bin/python
else
PYTHON_BOOTSTRAP :=
endif

.PHONY: build lint go-test test coverage leakcheck race docs docs-verify venv clean

# --- Build -----------------------------------------------------------------

build:
	go build ./...
	cd vgirpc/otel && go build ./...
	cd vgirpc/sentry && go build ./...
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
	go run ./tools/docverify
	cd vgirpc/otel && go vet ./...
	cd vgirpc/sentry && go vet ./...
	cd vgirpc/jwtauth && go vet ./...

# --- Python venv -----------------------------------------------------------

venv: $(VENV)/bin/python

$(VENV)/bin/python:
	python3 -m venv "$(VENV)"
	"$(VENV)/bin/python" -m pip install --quiet --upgrade pip
	"$(VENV)/bin/python" -m pip install --quiet "$(VGI_RPC_SPEC)" pytest pytest-timeout
	@echo "created $(VENV) with $(VGI_RPC_SPEC)"

# --- Test ------------------------------------------------------------------
# Two suites, deliberately split (see CLAUDE.md § Testing Policy):
#
#   go-test — language-local Go logic the cross-language harness structurally
#             cannot reach (intermediary helpers, client-side code, unexported
#             internals, pure functions). Seconds; no Python needed.
#   test    — the canonical cross-language conformance suite, which is what
#             actually keeps this port aligned with the Python/Java/TS/Rust
#             ones. Runs go-test first, since it is nearly free.
#
# The submodules (otel, sentry, jwtauth, s3, gcs) carry no tests; they are
# covered by `make lint`.

go-test:
	go test ./...

test: go-test conformance-worker $(PYTHON_BOOTSTRAP)
	$(PYTHON) -m pytest test_go_conformance.py -v

# --- Coverage --------------------------------------------------------------

coverage: conformance-worker-cover $(PYTHON_BOOTSTRAP)
	rm -rf $(COVDIR) && mkdir -p $(COVDIR)
	GOCOVERDIR=$(COVDIR) $(PYTHON) -m pytest test_go_conformance.py -v
	go tool covdata textfmt -i=$(COVDIR) -o=coverage-go.txt
	@echo "Coverage written to coverage-go.txt"

# --- Leak check ------------------------------------------------------------
# Builds the conformance worker with -tags leakcheck so every internal
# Arrow allocation routes through a single shared CheckedAllocator. The
# worker prints LeakCheckSummary to stderr on exit; pytest captures it.

leakcheck: $(PYTHON_BOOTSTRAP)
	go build -tags leakcheck -o conformance-worker ./conformance/cmd/vgi-rpc-conformance-go
	$(PYTHON) -m pytest test_go_conformance.py -v -s 2>&1 | grep -E "vgirpc leakcheck|passed|failed" | tail -20

# --- Race detector --------------------------------------------------------
# Builds the conformance worker with -race and runs the full conformance
# suite. The Go race detector instruments every shared-memory access; the
# worker exits non-zero if any data race is observed. ~3-5x slower than
# the regular build, but proves the lockstep streaming, transport-kind
# binding, OutputCollector budget snapshots, and external-fetch hedging
# are race-free under the conformance workload.
#
# GORACE=halt_on_error=1 causes the test run to fail on the first race
# rather than logging-and-continuing, so CI sees the failure clearly.

race: $(PYTHON_BOOTSTRAP)
	go build -race -o conformance-worker ./conformance/cmd/vgi-rpc-conformance-go
	GORACE=halt_on_error=1 VGI_GO_WORKER_TEARDOWN_TIMEOUT=30 $(PYTHON) -m pytest test_go_conformance.py -v -p no:timeout

# --- Documentation verification -------------------------------------------
# Checks README.md, CLAUDE.md and docs/** against the code: module paths
# resolve, complete examples compile against the working tree, symbol
# references exist, and relative links resolve. See tools/docverify.

docs-verify:
	go run ./tools/docverify

# --- Docs ------------------------------------------------------------------

docs:
	mkdocs serve

# --- Clean -----------------------------------------------------------------

clean:
	rm -f conformance-worker benchmark-worker
	rm -rf $(COVDIR) coverage-go.txt
