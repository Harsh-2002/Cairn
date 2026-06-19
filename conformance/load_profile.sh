#!/usr/bin/env bash
# Bootstrap a fresh temp Cairn, run the macro load profiles against it, print the report, tear
# down. Equivalent to MinIO's `warp` (unavailable here) but built on boto3 — the same client path
# the conformance suite drives. See docs/benchmarks.md for interpretation. (ARCH 30.2)
#
# Usage:
#   conformance/load_profile.sh
#   BIN=target/debug/cairn PY=/tmp/cairnvenv/bin/python conformance/load_profile.sh
#   QUICK=1 conformance/load_profile.sh        # smaller sizes/counts for a smoke run
#
# Exits non-zero on any error (binary missing, server failed to start, harness raised).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-/tmp/cairnvenv/bin/python}"
PORT="${PORT:-9081}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_UI_ADDR=off  # the harness tests the S3 API; no UI listener
export CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"

SRV=""
cleanup() {
  [ -n "$SRV" ] && kill "$SRV" 2>/dev/null || true
  [ -n "$SRV" ] && wait "$SRV" 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT

fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

[ -x "$BIN" ] || fail "binary not found or not executable: $BIN (build it: cargo build --bin cairn)"
[ -x "$PY" ] || fail "python interpreter not found: $PY (expected boto3-enabled venv)"
"$PY" -c "import boto3" 2>/dev/null || fail "boto3 not importable by $PY"

BOOT="$("$BIN" bootstrap)" || fail "bootstrap failed"
AKID="$(echo "$BOOT" | awk '/Access Key Id/ {print $NF}')"
SECRET="$(echo "$BOOT" | awk '/Secret Access Key/ {print $NF}')"
[ -n "$AKID" ] && [ -n "$SECRET" ] || fail "could not parse bootstrap credentials"

"$BIN" serve >"$DATA/server.log" 2>&1 &
SRV=$!

started=0
for _ in $(seq 1 100); do
  curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && { started=1; break; }
  kill -0 "$SRV" 2>/dev/null || fail "server exited during startup; log:
$(cat "$DATA/server.log")"
  sleep 0.1
done
[ "$started" -eq 1 ] || fail "server did not become healthy in time; log:
$(cat "$DATA/server.log")"

QUICK_FLAG=""
[ "${QUICK:-0}" = "1" ] && QUICK_FLAG="--quick"

"$PY" "$(dirname "$0")/load_profile.py" "$AKID" "$SECRET" "http://127.0.0.1:$PORT" $QUICK_FLAG \
  || fail "load profile harness exited non-zero"
