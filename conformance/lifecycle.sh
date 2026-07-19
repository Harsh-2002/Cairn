#!/usr/bin/env bash
# Lifecycle background-loop ENFORCEMENT (ARCH 19.2/19.3). `conformance.py` only asserts that a
# lifecycle configuration is *accepted* (`check("lifecycle expiration accepted", True)` — a literal
# `True`); nothing there proves a rule is ever applied. This harness proves the scanner actually
# acts: it expires the matching object, leaves the non-matching one, stays inside its own bucket,
# ignores a Disabled rule, and on a versioned bucket hides the key behind a delete marker instead of
# destroying the data.
#
# The scanner interval is driven down to 1s (`CAIRN_LIFECYCLE_INTERVAL_SECS`; the config validator's
# only floor is "must be positive", so 1 is legal) and the driver polls for observable state — it
# never sleeps a fixed amount to "wait for" a scan.
#
# NOTE: one assertion is currently RED on purpose — a "# KNOWN GAP" in lifecycle.py: a 501
# NotImplemented is delivered to the client as "We encountered an internal error. Please try again."
# (and logged at ERROR as an internal fault) because `error_map::error_response` gates the
# descriptive message on `status.is_server_error()`. Fix the message branch, not the assertion.
#
# Usage: BIN=target/debug/cairn PY=/path/to/python-with-boto3 conformance/lifecycle.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-python3}"
PORT="${PORT:-9086}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_UI_ADDR=off  # the harness only drives the S3 port
export CAIRN_MASTER_KEY; CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"
# Sweep once a second so a scan lands inside the test window (default is 3600s).
export CAIRN_LIFECYCLE_INTERVAL_SECS=1

SRV=""
cleanup() {
  [ -n "$SRV" ] && kill "$SRV" 2>/dev/null || true
  [ -n "$SRV" ] && wait "$SRV" 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

[ -x "$BIN" ] || fail "binary not found or not executable: $BIN"
command -v "$PY" >/dev/null 2>&1 || fail "python interpreter not found: $PY (needs boto3)"
"$PY" -c "import boto3" 2>/dev/null || fail "boto3 not importable by $PY"

# `export VAR="$(cmd)"` masks the command's exit status under `set -e` — keep them split.
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
kill -0 "$SRV" 2>/dev/null || fail "server is not running; log: $(cat "$DATA/server.log")"

"$PY" "$(dirname "$0")/lifecycle.py" "$AK" "$SK" "http://127.0.0.1:$PORT" \
  || fail "lifecycle driver failed; server log tail: $(tail -20 "$DATA/server.log")"
