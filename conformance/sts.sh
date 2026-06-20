#!/usr/bin/env bash
# STS temporary-credential regression (ARCH 14): boot a real cairn binary, mint a scoped, expiring
# session credential through the management API, and prove a standard S3 SDK consumes it
# (X-Amz-Security-Token) with exactly the granted access — scoped GET allowed, ungranted PUT denied,
# cross-bucket denied, tampered/absent token denied. The UI/management listener must be ON (minting
# is via /api/v1).
#
# Usage: BIN=target/debug/cairn PY=/path/to/python-with-boto3 conformance/sts.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-/tmp/cairnvenv/bin/python}"
PORT="${PORT:-9090}"
UIPORT="${UIPORT:-9091}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_UI_ADDR="127.0.0.1:$UIPORT"
export CAIRN_MASTER_KEY; CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"

SRV=""
cleanup() { [ -n "$SRV" ] && kill "$SRV" 2>/dev/null || true; [ -n "$SRV" ] && wait "$SRV" 2>/dev/null || true; rm -rf "$DATA"; }
trap cleanup EXIT
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

[ -x "$BIN" ] || fail "binary not found or not executable: $BIN"
[ -x "$PY" ] || fail "python interpreter not found: $PY (needs boto3)"
"$PY" -c "import boto3" 2>/dev/null || fail "boto3 not importable by $PY"

BOOT="$("$BIN" bootstrap)" || fail "bootstrap failed"
AK="$(echo "$BOOT" | awk '/Access Key Id/ {print $NF}')"
SK="$(echo "$BOOT" | awk '/Secret Access Key/ {print $NF}')"
# The management API is Bearer-authenticated with a dedicated `cairn_<id>.<secret>` token (distinct
# from the SigV4 pair used for S3), printed on the bootstrap "Authorization: Bearer …" line.
BEARER="$(echo "$BOOT" | awk '/Authorization: Bearer/ {print $3}')"
[ -n "$AK" ] && [ -n "$SK" ] && [ -n "$BEARER" ] || fail "could not parse bootstrap credentials"

"$BIN" serve >"$DATA/server.log" 2>&1 &
SRV=$!
for _ in $(seq 1 100); do
  curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && break
  kill -0 "$SRV" 2>/dev/null || fail "server exited at startup; log: $(cat "$DATA/server.log")"
  sleep 0.1
done

"$PY" "$(dirname "$0")/sts.py" "$AK" "$SK" \
  "http://127.0.0.1:$PORT" "http://127.0.0.1:$UIPORT" "$BEARER"

echo "PASS: STS scoped temporary credentials minted + enforced end-to-end"
