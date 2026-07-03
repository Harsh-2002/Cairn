#!/usr/bin/env bash
# Webhook event-notification regression (ARCH 20-style): boot a real cairn binary, configure a
# bucket webhook endpoint through the management API, drive S3 PUT/DELETE, and assert a local sink
# receives the correctly-shaped, HMAC-signed S3 event records. The UI/management listener must be ON
# (the notification config is set via /api/v1, not the S3 ?notification subresource).
#
# Usage: BIN=target/debug/cairn PY=/path/to/python-with-boto3 conformance/notifications.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-/tmp/cairnvenv/bin/python}"
PORT="${PORT:-9088}"
UIPORT="${UIPORT:-9089}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_UI_ADDR="127.0.0.1:$UIPORT"
export CAIRN_MASTER_KEY; CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"
# Drain the outbox every 2s so the test sees deliveries promptly.
export CAIRN_WEBHOOK_INTERVAL_SECS=2
# This harness points a webhook at a loopback sink (127.0.0.1). The SSRF guard blocks internal
# endpoints by default at both registration (management API) and delivery (connect time), so opt into
# the escape hatch — the same knob an operator sets for an on-prem/internal event collector (ARCH 27).
export CAIRN_ALLOW_INTERNAL_ENDPOINTS=true

SRV=""
cleanup() { [ -n "$SRV" ] && kill "$SRV" 2>/dev/null || true; [ -n "$SRV" ] && wait "$SRV" 2>/dev/null || true; rm -rf "$DATA"; }
trap cleanup EXIT
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

[ -x "$BIN" ] || fail "binary not found or not executable: $BIN"
command -v "$PY" >/dev/null 2>&1 || fail "python interpreter not found: $PY (needs boto3)"
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

"$PY" "$(dirname "$0")/notifications.py" "$AK" "$SK" \
  "http://127.0.0.1:$PORT" "http://127.0.0.1:$UIPORT" "$BEARER"

echo "PASS: webhook event notifications delivered end-to-end"
