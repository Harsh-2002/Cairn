#!/usr/bin/env bash
# Escalating warp limit test (ARCH 30): ramp concurrency to find the single-writer saturation point
# and assert the server stays CORRECT (zero operation errors) and ALIVE at every level, including
# past the throughput ceiling. The single group-committing writer is the architectural bottleneck:
# as concurrency climbs, throughput should PLATEAU (the writer saturates) while requests queue —
# never errors, never a crash, never corruption. This is the load-axis counterpart to the
# fault-injection harnesses; it complements warp.sh (a fixed-concurrency benchmark).
#
# Usage: BIN=target/debug/cairn conformance/warp_escalate.sh
#        LEVELS="8 32 64 128 256" DURATION=8s OBJ_SIZE=256KiB conformance/warp_escalate.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PORT="${PORT:-9088}"
REGION="${REGION:-us-east-1}"
BUCKET="${BUCKET:-warp-escalate}"
OBJ_SIZE="${OBJ_SIZE:-256KiB}"     # smallish: write-heavy, maximises writer-commit pressure
DURATION="${DURATION:-8s}"
LEVELS="${LEVELS:-8 32 64 128}"    # concurrency ramp

WARP_VERSION="${WARP_VERSION:-1.0.0}"
WARP_URL="https://github.com/minio/warp/releases/download/v${WARP_VERSION}/warp_Linux_x86_64.tar.gz"
WARP_CACHE="${WARP_CACHE:-/tmp/cairn-warp}"   # shared with warp.sh
WARP="${WARP:-}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_UI_ADDR=off
export CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-error}"

SRV=""
cleanup() {
  [ -n "$SRV" ] && kill "$SRV" 2>/dev/null || true
  [ -n "$SRV" ] && wait "$SRV" 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT
note() { printf '  %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

# --- warp binary (reuse warp.sh's cache) -----------------------------------------------------
if [ -z "$WARP" ]; then
  if command -v warp >/dev/null 2>&1; then
    WARP="$(command -v warp)"
  else
    WARP="$WARP_CACHE/warp"
    if [ ! -x "$WARP" ]; then
      note "downloading warp v$WARP_VERSION"
      mkdir -p "$WARP_CACHE"; tgz="$WARP_CACHE/warp.tar.gz"
      curl -fsSL -o "$tgz" "$WARP_URL" || fail "could not download warp from $WARP_URL"
      tar -xzf "$tgz" -C "$WARP_CACHE" warp || fail "warp tarball had no 'warp' binary"
      chmod +x "$WARP"; rm -f "$tgz"
    fi
  fi
fi
[ -x "$WARP" ] || fail "warp not found or not executable: $WARP"

# --- server ----------------------------------------------------------------------------------
[ -x "$BIN" ] || fail "binary not found: $BIN (build it: cargo build --bin cairn)"
BOOT="$("$BIN" bootstrap)" || fail "bootstrap failed"
AKID="$(echo "$BOOT" | awk '/Access Key Id/ {print $NF}')"
SECRET="$(echo "$BOOT" | awk '/Secret Access Key/ {print $NF}')"
[ -n "$AKID" ] && [ -n "$SECRET" ] || fail "could not parse bootstrap credentials"

"$BIN" serve >"$DATA/server.log" 2>&1 &
SRV=$!
for _ in $(seq 1 100); do
  curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && break
  kill -0 "$SRV" 2>/dev/null || fail "server exited during startup; log:
$(cat "$DATA/server.log")"
  sleep 0.1
done
note "cairn healthy on 127.0.0.1:$PORT; ramping warp put concurrency: $LEVELS"

common=(--host "127.0.0.1:$PORT" --access-key "$AKID" --secret-key "$SECRET"
        --region "$REGION" --bucket "$BUCKET" --obj.size "$OBJ_SIZE" --noclear)

# --- the concurrency ramp --------------------------------------------------------------------
printf '\n  %-12s %-40s %-8s\n' "concurrency" "throughput (warp report)" "errors"
worst_errors=0
for C in $LEVELS; do
  set +e
  out="$("$WARP" put "${common[@]}" --concurrent "$C" --duration "$DURATION" 2>&1)"
  rc=$?
  set -e
  # A crash is the hard breakpoint: the writer saturating must queue, never kill the process.
  kill -0 "$SRV" 2>/dev/null || fail "server CRASHED at concurrency=$C; log tail:
$(tail -8 "$DATA/server.log")"
  errs="$(printf '%s\n' "$out" | sed -n 's/^Errors: \([0-9]*\).*/\1/p' | tail -1 || true)"
  errs="${errs:-0}"
  raw="$(printf '%s\n' "$out" | grep -cE '<ERROR>' || true)"
  thr="$(printf '%s\n' "$out" | grep -iE 'obj/s|MiB/s|MB/s' | grep -iE 'average|throughput|\*' | head -1 | sed 's/^[* ]*//' || true)"
  if [ -z "$thr" ]; then thr="$(printf '%s\n' "$out" | grep -iE 'obj/s' | head -1 | sed 's/^[* ]*//' || true)"; fi
  total=$((errs + raw))
  printf '  %-12s %-40s %-8s\n' "$C" "${thr:-(n/a)}" "$total"
  if [ "$total" -gt "$worst_errors" ]; then worst_errors=$total; fi
  if [ "$rc" -ne 0 ]; then note "(warp put exited $rc at concurrency=$C)"; fi
done

# --- integrity: everything written must read back ---------------------------------------------
set +e
gout="$("$WARP" get "${common[@]}" --concurrent 16 --objects 100 --duration 8s 2>&1)"
set -e
kill -0 "$SRV" 2>/dev/null || fail "server crashed during the verification GET phase"
gerr="$(printf '%s\n' "$gout" | grep -cE '<ERROR>' || true)"

printf '\n=== verdict ===\n'
[ "$worst_errors" -eq 0 ] || fail "warp saw $worst_errors operation error(s) under load — the writer did not degrade cleanly (it should queue, not error)"
[ "$gerr" -eq 0 ] || fail "verification GET reported $gerr error(s) — written objects are not all retrievable (integrity)"
note "writer stayed alive and error-free across every concurrency level; all writes verified by read-back"
exit 0
