#!/usr/bin/env bash
# Integrity-scrub regression (ARCH 8.6/26.4): corrupt a stored, uncompressed/unencrypted blob on
# disk and assert the background scrub re-reads it, finds the ETag mismatch, and reports it as
# `cairn_scrub_corruption_total` — turning silent bit-rot into an observable event rather than a
# corrupted byte served to a client.
#
# Usage: BIN=target/debug/cairn PY=/path/to/python-with-boto3 conformance/scrub.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-/tmp/cairnvenv/bin/python}"
PORT="${PORT:-9086}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_UI_ADDR=off
export CAIRN_MASTER_KEY; CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"
# Scrub every 2s so a pass runs within the test window.
export CAIRN_SCRUB_INTERVAL_SECS=2

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
[ -n "$AK" ] && [ -n "$SK" ] || fail "could not parse bootstrap credentials"

"$BIN" serve >"$DATA/server.log" 2>&1 &
SRV=$!
for _ in $(seq 1 100); do
  curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && break
  kill -0 "$SRV" 2>/dev/null || fail "server exited at startup; log: $(cat "$DATA/server.log")"
  sleep 0.1
done

# PUT an incompressible 100000-byte object (stored uncompressed, unencrypted — exactly the path the
# scrub is the only safety net for).
"$PY" - "$AK" "$SK" "http://127.0.0.1:$PORT" <<'PY'
import sys, os, boto3
from botocore.config import Config
ak, sk, ep = sys.argv[1], sys.argv[2], sys.argv[3]
s3 = boto3.client("s3", endpoint_url=ep, aws_access_key_id=ak, aws_secret_access_key=sk,
                  region_name="us-east-1", config=Config(s3={"addressing_style": "path"}))
s3.create_bucket(Bucket="scrub")
s3.put_object(Bucket="scrub", Key="obj", Body=os.urandom(100000))
print("put 100000-byte object")
PY

# Locate the stored blob (the only 100000-byte file under the data tree) and flip a byte in place.
blob="$(find "$DATA/data" -type f -size 100000c | head -1)"
[ -n "$blob" ] || fail "could not locate the stored blob to corrupt"
printf '\xff' | dd of="$blob" bs=1 seek=50000 count=1 conv=notrunc 2>/dev/null
echo "  corrupted one byte of $blob"

# Wait for at least one scrub pass after the corruption (interval is 2s).
sleep 5
corrupt="$(curl -fsS "http://127.0.0.1:$PORT/metrics" | awk '/^cairn_scrub_corruption_total/ {s+=$2} END {print s+0}')"
echo "  cairn_scrub_corruption_total = $corrupt"
if [ "${corrupt%.*}" -ge 1 ] 2>/dev/null; then
  echo "PASS: the scrub detected the corrupted blob"
else
  fail "the scrub did not detect the corruption (counter=$corrupt); log: $(tail -5 "$DATA/server.log")"
fi
