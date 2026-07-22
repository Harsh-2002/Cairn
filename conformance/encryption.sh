#!/usr/bin/env bash
# Server-side-encryption (SSE) object-body conformance (ARCH 27) through a REAL AWS SDK (boto3):
# genuine SigV4 + the hyper adapter + SDK-side header decode — the wire path the in-process SSE tests
# (hand-built request structs) never exercise. Because the node runs locally, the driver also reads
# and TAMPERS committed blobs under the data dir to prove on-disk ciphertext and fail-closed reads.
#
# Two boot legs (two server configs); the bash launcher owns the process, the .py drives each phase:
#   leg 1  CAIRN_KMS_KEY_IDS allow-list set, at-rest OFF, UI listener ON  -> cases a, c, d, e, f, g
#   leg 2  CAIRN_ENCRYPT_AT_REST=true, UI listener OFF                    -> case b
#
# Usage: BIN=target/debug/cairn PY=python3 conformance/encryption.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-/tmp/cairnvenv/bin/python}"
PORT="${PORT:-9094}"
UIPORT=$((PORT + 1))
DATA="$(mktemp -d)"

# A fixed hex master key WITH letters — an all-digit hex string is parsed as an int by Figment. Held
# constant across both legs (each leg still gets its own fresh data dir, so they never interfere).
export CAIRN_MASTER_KEY="00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"
KEY_ID="alias/cairn-conformance"

SRV=""
cleanup() { [ -n "$SRV" ] && kill "$SRV" 2>/dev/null || true; [ -n "$SRV" ] && wait "$SRV" 2>/dev/null || true; rm -rf "$DATA"; }
trap cleanup EXIT
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

[ -x "$BIN" ] || fail "binary not found or not executable: $BIN"
command -v "$PY" >/dev/null 2>&1 || fail "python interpreter not found: $PY (needs boto3)"
"$PY" -c "import boto3" 2>/dev/null || fail "boto3 not importable by $PY"

wait_healthz() {
  for _ in $(seq 1 100); do
    curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && return 0
    kill -0 "$SRV" 2>/dev/null || fail "server exited at startup; log: $(cat "$1")"
    sleep 0.1
  done
  fail "server did not become healthy; log: $(cat "$1")"
}
stop_srv() { [ -n "$SRV" ] && kill "$SRV" 2>/dev/null || true; [ -n "$SRV" ] && wait "$SRV" 2>/dev/null || true; SRV=""; }

# ---- leg 1: KMS allow-list, at-rest OFF, UI ON (cases a, c, d, e, f, g) ----
D1="$DATA/d1"
export CAIRN_DATA_DIR="$D1/data"
export CAIRN_DB_PATH="$D1/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_UI_ADDR="127.0.0.1:$UIPORT"
export CAIRN_KMS_KEY_IDS="$KEY_ID"
unset CAIRN_ENCRYPT_AT_REST || true

BOOT="$("$BIN" bootstrap)" || fail "bootstrap failed (leg 1)"
AK="$(echo "$BOOT" | awk '/Access Key Id/ {print $NF}')"
SK="$(echo "$BOOT" | awk '/Secret Access Key/ {print $NF}')"
[ -n "$AK" ] && [ -n "$SK" ] || fail "could not parse bootstrap credentials (leg 1)"

"$BIN" serve >"$D1/server.log" 2>&1 &
SRV=$!
wait_healthz "$D1/server.log"

if "$PY" "$ROOT/conformance/encryption.py" main "$AK" "$SK" \
    "http://127.0.0.1:$PORT" "$CAIRN_DATA_DIR" "$KEY_ID" "http://127.0.0.1:$UIPORT"; then
  echo "PASS: SSE wire contract + on-disk ciphertext + fail-closed (leg 1)"
else
  fail "leg 1 SSE assertions failed; server log: $(tail -20 "$D1/server.log")"
fi
stop_srv

# ---- leg 2: transparent at-rest, UI OFF (case b) ----
D2="$DATA/d2"
export CAIRN_DATA_DIR="$D2/data"
export CAIRN_DB_PATH="$D2/data/cairn.db"
export CAIRN_UI_ADDR=off
export CAIRN_ENCRYPT_AT_REST=true
unset CAIRN_KMS_KEY_IDS || true

BOOT2="$("$BIN" bootstrap)" || fail "bootstrap failed (leg 2)"
AK2="$(echo "$BOOT2" | awk '/Access Key Id/ {print $NF}')"
SK2="$(echo "$BOOT2" | awk '/Secret Access Key/ {print $NF}')"
[ -n "$AK2" ] && [ -n "$SK2" ] || fail "could not parse bootstrap credentials (leg 2)"

"$BIN" serve >"$D2/server.log" 2>&1 &
SRV=$!
wait_healthz "$D2/server.log"

if "$PY" "$ROOT/conformance/encryption.py" atrest "$AK2" "$SK2" \
    "http://127.0.0.1:$PORT" "$CAIRN_DATA_DIR" "" ""; then
  echo "PASS: transparent at-rest stores ciphertext while advertising nothing (leg 2)"
else
  fail "leg 2 at-rest assertions failed; server log: $(tail -20 "$D2/server.log")"
fi
stop_srv

echo "PASS: SSE object-body conformance (all legs)"
