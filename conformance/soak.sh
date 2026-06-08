#!/usr/bin/env bash
# Multi-host replication soak (ARCH §20, §29). Two Cairn nodes, a sustained PUT workload against
# the source, periodic byte-for-byte verification that objects replicated to the target, and a
# leak check on the source's RSS.
#
# Topology (mirrors the single-target node->node replication shape in docs/operations.md §2):
#   node-1  = replication TARGET (the mirror; a plain Cairn).
#   node-2  = SOURCE; configured with CAIRN_REPLICATION_ENDPOINT pointed at node-1, so its
#             replication worker ships the source bucket's versions to node-1.
#
# What it asserts, over a configurable DURATION (default 120s, the CI value):
#   * a boto3 PUT workload runs continuously against the source;
#   * every few seconds a random sample of already-PUT objects is read back from the TARGET and
#     compared byte-for-byte against what was written  ->  replication mismatches MUST be 0;
#   * the source process RSS is sampled throughout and must stay roughly flat (no monotonic
#     growth)  ->  no leak.
#
# Usage:
#   conformance/soak.sh
#   DURATION=60 conformance/soak.sh                              # shorter run
#   BIN=target/debug/cairn PY=/tmp/cairnvenv/bin/python conformance/soak.sh
#
# Exit status: 0 only if mismatches == 0 AND the source RSS did not grow past the leak threshold;
# non-zero on any infrastructure failure or assertion failure.
set -euo pipefail
export CAIRN_UI_ADDR=off  # multi-node harness: no UI listener (would collide on the default UI port)

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-/tmp/cairnvenv/bin/python}"
PORT1="${PORT1:-9083}"   # target (mirror)
PORT2="${PORT2:-9084}"   # source
DURATION="${DURATION:-120}"
BUCKET="${BUCKET:-soak}"
REPL_INTERVAL="${REPL_INTERVAL:-2}"

DATA1="$(mktemp -d)"
DATA2="$(mktemp -d)"

# Each node's bootstrap and serve MUST share a master key, or the sealed SigV4 secret cannot be
# unsealed at serve time (the secret is envelope-encrypted under the master key at bootstrap).
KEY1="$(openssl rand -hex 32)"
KEY2="$(openssl rand -hex 32)"

SRV1=""
SRV2=""
cleanup() {
  [ -n "$SRV1" ] && kill "$SRV1" 2>/dev/null || true
  [ -n "$SRV2" ] && kill "$SRV2" 2>/dev/null || true
  [ -n "$SRV1" ] && wait "$SRV1" 2>/dev/null || true
  [ -n "$SRV2" ] && wait "$SRV2" 2>/dev/null || true
  rm -rf "$DATA1" "$DATA2"
}
trap cleanup EXIT

note() { printf '  %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

[ -x "$BIN" ] || fail "binary not found or not executable: $BIN (build it: cargo build --bin cairn)"
command -v "$PY" >/dev/null 2>&1 || fail "python interpreter not found: $PY (expected boto3-enabled venv)"
"$PY" -c "import boto3" 2>/dev/null || fail "boto3 not importable by $PY"

wait_healthy() {
  local port="$1" pid="$2" log="$3"
  for _ in $(seq 1 100); do
    curl -fsS -o /dev/null "http://127.0.0.1:$port/healthz" 2>/dev/null && return 0
    kill -0 "$pid" 2>/dev/null || { printf 'server on %s exited during startup; log:\n%s\n' \
      "$port" "$(cat "$log")" >&2; return 1; }
    sleep 0.1
  done
  printf 'server on %s did not become healthy in time; log:\n%s\n' "$port" "$(cat "$log")" >&2
  return 1
}

# --- node-1: replication TARGET ----------------------------------------------------------------
note "starting node-1 (replication target) on 127.0.0.1:$PORT1"
T_BOOT="$(
  env CAIRN_DATA_DIR="$DATA1/data" CAIRN_DB_PATH="$DATA1/data/cairn.db" \
      CAIRN_LISTEN_ADDR="127.0.0.1:$PORT1" CAIRN_MASTER_KEY="$KEY1" \
      CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-error}" \
      "$BIN" bootstrap
)" || fail "node-1 bootstrap failed"
T_AKID="$(echo "$T_BOOT" | awk '/Access Key Id/ {print $NF}')"
T_SECRET="$(echo "$T_BOOT" | awk '/Secret Access Key/ {print $NF}')"
[ -n "$T_AKID" ] && [ -n "$T_SECRET" ] || fail "could not parse node-1 credentials"

env CAIRN_DATA_DIR="$DATA1/data" CAIRN_DB_PATH="$DATA1/data/cairn.db" \
    CAIRN_LISTEN_ADDR="127.0.0.1:$PORT1" CAIRN_MASTER_KEY="$KEY1" \
    CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-error}" \
    "$BIN" serve >"$DATA1/server.log" 2>&1 &
SRV1=$!

# --- node-2: SOURCE, replicating to node-1 -----------------------------------------------------
note "starting node-2 (source, replicates to node-1) on 127.0.0.1:$PORT2"
S_BOOT="$(
  env CAIRN_DATA_DIR="$DATA2/data" CAIRN_DB_PATH="$DATA2/data/cairn.db" \
      CAIRN_LISTEN_ADDR="127.0.0.1:$PORT2" CAIRN_MASTER_KEY="$KEY2" \
      CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-error}" \
      "$BIN" bootstrap
)" || fail "node-2 bootstrap failed"
S_AKID="$(echo "$S_BOOT" | awk '/Access Key Id/ {print $NF}')"
S_SECRET="$(echo "$S_BOOT" | awk '/Secret Access Key/ {print $NF}')"
[ -n "$S_AKID" ] && [ -n "$S_SECRET" ] || fail "could not parse node-2 credentials"

env CAIRN_DATA_DIR="$DATA2/data" CAIRN_DB_PATH="$DATA2/data/cairn.db" \
    CAIRN_LISTEN_ADDR="127.0.0.1:$PORT2" CAIRN_MASTER_KEY="$KEY2" \
    CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-error}" \
    CAIRN_REPLICATION_ENDPOINT="http://127.0.0.1:$PORT1" \
    CAIRN_REPLICATION_ACCESS_KEY="$T_AKID" \
    CAIRN_REPLICATION_SECRET="$T_SECRET" \
    CAIRN_REPLICATION_REGION="us-east-1" \
    CAIRN_REPLICATION_INTERVAL_SECS="$REPL_INTERVAL" \
    "$BIN" serve >"$DATA2/server.log" 2>&1 &
SRV2=$!

wait_healthy "$PORT1" "$SRV1" "$DATA1/server.log" || fail "node-1 unhealthy"
wait_healthy "$PORT2" "$SRV2" "$DATA2/server.log" || fail "node-2 unhealthy"
note "both nodes healthy"

# --- drive the soak --------------------------------------------------------------------------
# The source PID is the process whose RSS we leak-check.
"$PY" "$(dirname "$0")/soak.py" \
  --source-endpoint "http://127.0.0.1:$PORT2" \
  --source-access-key "$S_AKID" --source-secret "$S_SECRET" \
  --target-endpoint "http://127.0.0.1:$PORT1" \
  --target-access-key "$T_AKID" --target-secret "$T_SECRET" \
  --bucket "$BUCKET" --duration "$DURATION" --source-pid "$SRV2" \
  || fail "soak harness reported a failure (mismatch or leak); see output above"

note "soak passed: 0 replication mismatches, source RSS flat"
