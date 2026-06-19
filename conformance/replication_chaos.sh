#!/usr/bin/env bash
# Replication chaos / limit regression (ARCH 20): deliberately break replication and assert safe
# degradation — no data loss, source stays healthy, eventual convergence. Two real cairn nodes; the
# Python driver owns both processes so it can inject faults (target down, source SIGKILL, rapid
# overwrite). This is the FAULT-INJECTION counterpart to soak.sh (happy-path). Requires boto3.
#
# Usage: BIN=target/debug/cairn PY=python3 conformance/replication_chaos.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-python3}"
DATA="$(mktemp -d)"
cleanup() { rm -rf "$DATA"; }
trap cleanup EXIT

[ -x "$BIN" ] || { echo "FAIL: binary not found: $BIN (build it: cargo build --bin cairn)"; exit 1; }
"$PY" -c "import boto3" 2>/dev/null || { echo "FAIL: boto3 not importable by $PY"; exit 1; }

BIN="$BIN" DATA="$DATA" PORT1="${PORT1:-9085}" PORT2="${PORT2:-9086}" REPL_INTERVAL="${REPL_INTERVAL:-1}" \
  "$PY" "$(dirname "$0")/replication_chaos.py"
