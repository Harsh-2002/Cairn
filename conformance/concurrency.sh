#!/usr/bin/env bash
# Concurrency / contention regression: many simultaneous clients hammer ONE key (create race,
# compare-and-swap race, last-writer-wins) and assert the single-writer path stays atomic and
# uncorrupted under contention. Stdlib-only Python.
#
# Usage: BIN=target/debug/cairn PY=python3 conformance/concurrency.sh
set -euo pipefail

BIN="${BIN:-target/debug/cairn}"
PY="${PY:-python3}"
DATA="$(mktemp -d)"
cleanup() { rm -rf "$DATA"; }
trap cleanup EXIT

BIN="$BIN" DATA="$DATA" PORT="${PORT:-9087}" N="${N:-32}" "$PY" "$(dirname "$0")/concurrency.py"
