#!/usr/bin/env bash
# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0
#
# Run the reference's two cross-port drift checks against this port.
#
# Neither check can be performed from inside this repository alone: both
# compare *this implementation to the Python reference*, which is why they ran
# only on a maintainer's laptop until now — and so the drift they catch was the
# drift nothing enforced. In the last day they found four ports describing
# vgi_rpc.Reflection.v1 as having zero methods, and six ports logging six
# different wrong protocol_hash values. None of that is visible from inside any
# single port, including this one.
#
#   describe_diff.py         spawns ./conformance-worker, asks it over
#                            vgi_rpc.Reflection.v1 for every protocol it hosts,
#                            describes each, and compares protocol_hash and
#                            full structure against Python's. Exits non-zero on
#                            disagreement.
#   identity_consistency.py  greps this port's source for the constants and
#                            strings vgi_rpc.Identity.v1 requires and prints a
#                            matrix. A source grep is a weak instrument; it is
#                            here because the alternative is nothing.
#
# Both tools resolve ports as <repos>/vgi-rpc-<name>, so they need this repo and
# the reference checkout as siblings. Rather than require the caller to arrange
# that — CI clones the reference into RUNNER_TEMP, a laptop keeps checkouts
# wherever it keeps them — this builds a throwaway directory of two symlinks
# and points VGI_RPC_REPOS at it. Nothing is copied and nothing moves.
#
# Environment:
#   VGI_RPC_PYTHON_REPO  the vgi-rpc-python checkout to compare against
#                        (required — see the note on absence below)
#   PYTHON               interpreter with that checkout installed
#                        (default: python3)
#
# There is deliberately no "skip when the reference is missing" path. The whole
# value of these checks is telling "checked and agrees" apart from "not
# checked", and a gate that quietly passes when its subject is absent is the
# second one wearing the first one's colours.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
PYTHON="${PYTHON:-python3}"
REF="${VGI_RPC_PYTHON_REPO:-}"

if [ -z "$REF" ]; then
	cat >&2 <<'MSG'
cross-port: no vgi-rpc-python checkout.

These checks compare this port to the reference, so the reference has to be
present; there is no meaningful single-repo form of them. Point at a checkout:

    VGI_RPC_PYTHON_REPO=/path/to/vgi-rpc-python make cross-port

CI clones Query-farm/vgi-rpc-python at HEAD — unpinned on purpose, so that a
reference change which breaks this port turns this port red the same day.
MSG
	exit 1
fi

if [ ! -d "$REF/tools/cross-port" ]; then
	echo "cross-port: $REF is not a vgi-rpc-python checkout (no tools/cross-port)" >&2
	exit 1
fi
REF="$(cd "$REF" && pwd -P)"

if [ ! -x "$REPO_ROOT/conformance-worker" ]; then
	echo "cross-port: $REPO_ROOT/conformance-worker missing — run 'make conformance-worker'" >&2
	exit 1
fi

SHIM="$(mktemp -d)"
trap 'rm -rf "$SHIM"' EXIT
ln -s "$REF" "$SHIM/vgi-rpc-python"
ln -s "$REPO_ROOT" "$SHIM/vgi-rpc-go"
export VGI_RPC_REPOS="$SHIM"

echo "cross-port: this port  $REPO_ROOT"
echo "cross-port: reference  $REF"
echo

echo "=== describe_diff --only go"
"$PYTHON" "$REF/tools/cross-port/describe_diff.py" --only go

echo
echo "=== identity_consistency --only go"
# --verbose so the run names the files it actually read. The tool audits by
# grepping source, and a port whose repository it cannot see is reported as
# ABSENT and *returns zero* — an empty matrix is indistinguishable from a
# clean one unless something checks that real rows were produced. So capture
# the output, let the exit status stand, and then insist the go column came
# from files that exist.
IDENTITY_LOG="$SHIM/identity.log"
set +e
"$PYTHON" "$REF/tools/cross-port/identity_consistency.py" --only go --verbose 2>&1 | tee "$IDENTITY_LOG"
IDENTITY_STATUS=${PIPESTATUS[0]}
set -e
if [ "$IDENTITY_STATUS" -ne 0 ]; then
	exit "$IDENTITY_STATUS"
fi
if grep -qE '^(ABSENT|NOT IMPLEMENTED): go( |$)' "$IDENTITY_LOG"; then
	echo >&2
	echo "cross-port: identity_consistency exited 0 without auditing go — that is a" >&2
	echo "cross-port: pass over nothing, not a pass. See the line above." >&2
	exit 1
fi
if ! grep -qE '^=== go: [1-9][0-9]* identity file\(s\)' "$IDENTITY_LOG"; then
	echo >&2
	echo "cross-port: identity_consistency read no go source files. The matrix above" >&2
	echo "cross-port: is empty, so its 'agree' means nothing was compared." >&2
	exit 1
fi

echo
echo "cross-port: this port agrees with the reference"
