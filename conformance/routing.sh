#!/usr/bin/env bash
# Routing fall-through regression harness (audit 2026-07): prove a PRESENT-BUT-INVALID bucket/key
# segment is rejected instead of collapsed to `None` and re-routed to the bucket/root handler
# (which destroyed an empty bucket on `DELETE /b/<1025-byte key>`). See conformance/routing.py.
# Usage: BIN=target/debug/cairn PY=python3 conformance/routing.sh
set -euo pipefail

BIN="${BIN:-target/debug/cairn}"
PY="${PY:-python3}"
PORT="${PORT:-9093}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_UI_ADDR=off  # the harness tests S3-port routing; no UI listener
export CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"

BOOT="$("$BIN" bootstrap)"
AKID="$(echo "$BOOT" | awk '/Access Key Id/ {print $NF}')"
SECRET="$(echo "$BOOT" | awk '/Secret Access Key/ {print $NF}')"

"$BIN" serve &
SRV=$!
cleanup() { kill "$SRV" 2>/dev/null || true; wait "$SRV" 2>/dev/null || true; rm -rf "$DATA"; }
trap cleanup EXIT

for _ in $(seq 1 100); do
  curl -s -o /dev/null "http://127.0.0.1:$PORT/healthz" && break
  sleep 0.1
done

"$PY" "$(dirname "$0")/routing.py" "$AKID" "$SECRET" "http://127.0.0.1:$PORT"
