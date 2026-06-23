#!/usr/bin/env bash
# 5-node MESH bucket-replication harness (ARCH 20): stands up five real cairn nodes, wires a full
# mesh (every node replicates a bucket to the other four), and drives the data-resiliency /
# corruption / throughput / bottleneck scenarios. The Python driver (stdlib only — no boto3) owns
# all five node processes so it can inject faults (crash + restart). Running a server needs the dev
# sandbox disabled.
#
# Usage: BIN=target/debug/cairn PY=python3 conformance/mesh.sh [scenario-ids...]
#   e.g. conformance/mesh.sh            # all scenarios
#        conformance/mesh.sh 1 3        # only convergence + version-id identity
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-python3}"
DATA="$(mktemp -d)"
cleanup() { rm -rf "$DATA"; }
trap cleanup EXIT

[ -x "$BIN" ] || { echo "FAIL: binary not found: $BIN (build it: cargo build --bin cairn)"; exit 1; }

BIN="$BIN" DATA="$DATA" BASE_PORT="${BASE_PORT:-7500}" REPL_INTERVAL="${REPL_INTERVAL:-2}" \
  WORKERS="${WORKERS:-4}" CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}" \
  "$PY" "$(dirname "$0")/mesh.py" "$@"
