# Development Makefile for vgi-rpc-go

# Configurable paths — override with env vars or on the command line.
GO_CONFORMANCE_WORKER ?= $(CURDIR)/conformance-worker
export GO_CONFORMANCE_WORKER

GOBIN := $(shell go env GOPATH)/bin
COVDIR := $(CURDIR)/_covdata

# --- Python environment ----------------------------------------------------
# The conformance suite ships inside vgi-rpc itself, so *which* vgi-rpc the
# venv holds decides what this port is measured against. That choice is not a
# detail: a released wheel is the wrong reference whenever the wire is moving,
# and running this suite against PyPI's v0.25.0 — which predates the
# multiservice work entirely — reports hundreds of failures that say nothing
# about this port.
#
# So the bootstrap prefers a checkout of vgi-rpc-python and installs it
# editable, which is what CI does (it clones the reference at HEAD). Point
# VGI_RPC_PYTHON_REPO somewhere else, or at nothing, to change that:
#
#	VGI_RPC_PYTHON_REPO=/path/to/vgi-rpc-python make test   # another checkout
#	VGI_RPC_PYTHON_REPO= make test                          # released wheel
#
# With no checkout the bootstrap falls back to VGI_RPC_SPEC from PyPI and says
# so, so a fresh clone still runs — it just says what it is testing against.
#
# To use an interpreter you manage yourself, override PYTHON and the bootstrap
# is skipped entirely:
#
#	PYTHON=/path/to/python make test
VENV := $(CURDIR)/.venv
PYTHON ?= $(VENV)/bin/python
VGI_RPC_SPEC ?= vgi-rpc[http,cli,external]>=0.20.0

# The reference checkout, if one is present. `wildcard` rather than a bare
# path so that an absent tree is empty rather than a pip error — a default
# naming one machine's layout must degrade, not break, on every other.
VGI_RPC_PYTHON_REPO ?= $(wildcard $(HOME)/Development/vgi-rpc-python)

# Extras and pins mirror .github/workflows/ci.yml: the conformance extra
# carries jsonschema for access-record validation, and httpx2 is pinned to the
# last release whose zstd decoder actually decodes.
VGI_RPC_REF_EXTRAS := [http,cli,external,conformance]
VGI_RPC_TEST_DEPS := pytest pytest-timeout httpx2==2.9.1

# Bootstrap only when PYTHON came from this file — never when the caller
# supplied their own interpreter.
ifeq ($(filter command line environment,$(origin PYTHON)),)
PYTHON_BOOTSTRAP := $(VENV)/bin/python
else
PYTHON_BOOTSTRAP :=
endif

.PHONY: build lint go-test test coverage leakcheck race docs docs-verify venv clean \
	conformance-worker conformance-worker-cover benchmark-worker \
	ci staticcheck-install conformance-runner conformance-access-log

# --- Build -----------------------------------------------------------------

build:
	go build ./...
	cd vgirpc/otel && go build ./...
	cd vgirpc/sentry && go build ./...
	cd vgirpc/jwtauth && go build ./...
	cd vgirpc/s3 && go build ./...
	cd vgirpc/gcs && go build ./...

# PHONY on purpose: the rule names a file but lists no prerequisites, so
# without this make treats an existing binary as up to date forever and
# silently runs the conformance suite against a stale worker. Let `go build`
# decide what needs recompiling — it is already incremental.
conformance-worker:
	go build -o conformance-worker ./conformance/cmd/vgi-rpc-conformance-go

conformance-worker-cover:
	go build -cover -covermode=atomic -o conformance-worker ./conformance/cmd/vgi-rpc-conformance-go

benchmark-worker:
	go build -o benchmark-worker ./benchmark/cmd/vgi-rpc-benchmark-go

# --- Lint ------------------------------------------------------------------
# STATICCHECK_VERSION mirrors the pin in .github/workflows/ci.yml. It is not
# cosmetic: releases add and retire checks, so a locally installed older
# staticcheck reports a different finding set and can call a tree clean that
# CI then rejects. `make staticcheck-install` puts the pinned one in $(GOBIN).

STATICCHECK_VERSION ?= v0.8.0

staticcheck-install:
	GOTOOLCHAIN=local go install honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION)

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
ifeq ($(strip $(VGI_RPC_PYTHON_REPO)),)
	"$(VENV)/bin/python" -m pip install --quiet "$(VGI_RPC_SPEC)" $(VGI_RPC_TEST_DEPS)
	@echo "created $(VENV) with $(VGI_RPC_SPEC) (released wheel)"
	@echo "  note: no vgi-rpc-python checkout found. A released wheel lags the wire;"
	@echo "  set VGI_RPC_PYTHON_REPO=/path/to/vgi-rpc-python to test against the reference."
else
	"$(VENV)/bin/python" -m pip install --quiet -e "$(VGI_RPC_PYTHON_REPO)$(VGI_RPC_REF_EXTRAS)" $(VGI_RPC_TEST_DEPS)
	@echo "created $(VENV) from $(VGI_RPC_PYTHON_REPO) (editable)"
endif

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

# --- CI parity -------------------------------------------------------------
# The complete gate list from .github/workflows/ci.yml in one command, so the
# answer to "will CI pass" does not require pushing to find out.
#
# Three of these gates are reachable through no other make target, which is
# exactly how an ST1005 finding reached main:
#
#   staticcheck            `make lint` runs it, but against whatever version
#                          happens to be installed -- see STATICCHECK_VERSION.
#   conformance-runner     the runner-driven suite. `make test` runs pytest,
#                          which carries no large_payload cases at all, so
#                          large_payload.echo_binary_over_int32_max (>2 GiB on
#                          each side) runs here and nowhere else.
#   conformance-access-log the access-record spec check. Verified by hand for
#                          months and drifted anyway.
#
# And one gate passes by *skipping* everywhere else: the native Go client test
# no-ops unless VGI_RPC_PYTHON names an interpreter, so `make go-test` has
# never run it.
#
# PYTHON_BIN is where the venv's console scripts live, derived from PYTHON so
# that an interpreter supplied by the caller still resolves its own
# vgi-rpc-test rather than the repo venv's.

PYTHON_BIN := $(dir $(PYTHON))
ACCESS_LOG := $(CURDIR)/_access-log.jsonl

conformance-runner: conformance-worker $(PYTHON_BOOTSTRAP)
	$(PYTHON_BIN)vgi-rpc-test --cmd "$(GO_CONFORMANCE_WORKER)"

conformance-access-log: conformance-worker $(PYTHON_BOOTSTRAP)
	rm -f $(ACCESS_LOG)
	$(PYTHON_BIN)vgi-rpc-test \
		--cmd "$(GO_CONFORMANCE_WORKER) --access-log $(ACCESS_LOG) --access-log-debug" \
		--access-log "$(ACCESS_LOG)" \
		--require-request-data \
		--filter '!large_payload.echo_binary_over_int32_max'
	rm -f $(ACCESS_LOG)

ci: build staticcheck-install $(PYTHON_BOOTSTRAP)
	go vet ./...
	$(GOBIN)/staticcheck ./...
	go test ./...
	go run ./tools/docverify
	VGI_RPC_PYTHON=$(PYTHON) go test ./vgirpc -run '^TestPythonNativeClientTypedExchange$$' -count=1
	$(MAKE) test
	$(MAKE) conformance-runner
	$(MAKE) conformance-access-log

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
	rm -f conformance-worker benchmark-worker $(ACCESS_LOG)
	rm -rf $(COVDIR) coverage-go.txt
