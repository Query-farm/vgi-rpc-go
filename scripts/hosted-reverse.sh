#!/usr/bin/env bash
# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0
#
# Run the reference's hosted-protocols group against a conformance worker whose
# application protocols are registered in an order no sort produces:
# zeta.Primary.v1, conformance.Secondary.v1, alpha.Extra.v1.
#
# The main conformance worker cannot show this: ConformanceService sorts before
# conformance.Secondary.v1 in ASCII, so a server that sorted its listing by name
# instead of keeping registration order would pass every port-suite test. The
# reference requires every port to run this (MULTI_PROTOCOL_HOSTING.md §4), on
# stdio, a unix socket and HTTP. The worker takes its transport flag first.
#
# Environment: PYTHON (an interpreter with vgi-rpc[http,conformance], pytest and
# pytest-timeout), GO_CONFORMANCE_WORKER (default ./conformance-worker).
set -euo pipefail

PYTHON="${PYTHON:-python3}"
WORKER="${GO_CONFORMANCE_WORKER:-$(pwd)/conformance-worker}"
EXPECT="zeta.Primary.v1,conformance.Secondary.v1,alpha.Extra.v1"
hosted() { "$PYTHON" -m vgi_rpc.conformance.hosted_protocols "$@" --expect "$EXPECT" -- -q; }

pids=()
sock="$(mktemp -u /tmp/vgo-rev-XXXXXX).sock"   # short: AF_UNIX paths are bounded
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; rm -f "$sock"; }
trap cleanup EXIT

echo "== stdio"
hosted --cmd "$WORKER --hosted-reverse"

echo "== unix"
"$WORKER" --unix "$sock" --hosted-reverse >/dev/null & pids+=($!)
for _ in $(seq 50); do [ -S "$sock" ] && break; sleep 0.1; done
hosted --unix "$sock"

echo "== http"
out="$(mktemp)"
"$WORKER" --http --hosted-reverse >"$out" & pids+=($!)
for _ in $(seq 50); do grep -q '^PORT:' "$out" && break; sleep 0.1; done
port="$(sed -n 's/^PORT://p' "$out")"
[ -n "$port" ] || { echo "worker printed no PORT:" >&2; exit 1; }
hosted --url "http://127.0.0.1:$port"
