#!/usr/bin/env bash
# Real MinIO `warp` macro benchmark against a freshly-bootstrapped Cairn (ARCH §30.2).
#
# `warp` is the canonical S3 macro-benchmark. This harness downloads the upstream binary (once),
# bootstraps a throwaway Cairn the same env-only way conformance/run.sh does, creates a bucket, and
# drives `warp put`, `warp get`, and `warp mixed` against it with path-style addressing, then tears
# everything down. It finishes in ~2-3 minutes at the default (modest) size/duration.
#
# Usage:
#   conformance/warp.sh
#   BIN=target/debug/cairn conformance/warp.sh                  # reuse a prebuilt binary
#   WARP=/usr/local/bin/warp conformance/warp.sh                # reuse an installed warp
#   DURATION=30s OBJ_SIZE=512KiB CONCURRENT=4 conformance/warp.sh
#
# Exit status: 0 only if every warp phase completes with zero operation errors; non-zero on any
# infrastructure failure (binary missing, server failed to start, warp download failed) OR if warp
# reported operation errors. See the "Known blocker" note below and docs/benchmarks.md.
#
# `warp`'s object-name generator deliberately puts '(' and ')' in keys to stress URL-encoding. The
# SigV4 canonical-URI double-encoding bug this once surfaced (keys with '(' ')' space failing
# SignatureDoesNotMatch) is FIXED (cairn-auth sigv4: uri_encode(percent_decode(path))), so the script
# runs get/put/mixed strict and any operation error fails the run.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PORT="${PORT:-9082}"
REGION="${REGION:-us-east-1}"
BUCKET="${BUCKET:-warp-benchmark}"

# Modest defaults: the whole run lands in ~2-3 min.
OBJ_SIZE="${OBJ_SIZE:-1MiB}"
CONCURRENT="${CONCURRENT:-4}"
DURATION="${DURATION:-20s}"
OBJECTS="${OBJECTS:-50}"

# Pinned upstream warp release. The GitHub releases assets use un-versioned names
# (warp_Linux_x86_64.tar.gz); v1.5.0+ ship only on dl.min.io, so v1.0.0 is the newest tarball on
# github.com/minio/warp/releases and is what we pin for a stable, scriptable URL.
WARP_VERSION="${WARP_VERSION:-1.0.0}"
WARP_URL="https://github.com/minio/warp/releases/download/v${WARP_VERSION}/warp_Linux_x86_64.tar.gz"
WARP_CACHE="${WARP_CACHE:-/tmp/cairn-warp}"
WARP="${WARP:-}"

DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
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

# --- 0. obtain the warp binary -----------------------------------------------------------------
if [ -z "$WARP" ]; then
  if command -v warp >/dev/null 2>&1; then
    WARP="$(command -v warp)"
  else
    WARP="$WARP_CACHE/warp"
    if [ ! -x "$WARP" ]; then
      note "downloading warp v$WARP_VERSION from github.com/minio/warp/releases"
      mkdir -p "$WARP_CACHE"
      tgz="$WARP_CACHE/warp.tar.gz"
      curl -fsSL -o "$tgz" "$WARP_URL" || fail "could not download warp from $WARP_URL"
      tar -xzf "$tgz" -C "$WARP_CACHE" warp || fail "warp tarball did not contain a 'warp' binary"
      chmod +x "$WARP"
      rm -f "$tgz"
    fi
  fi
fi
[ -x "$WARP" ] || fail "warp binary not found or not executable: $WARP"
note "warp: $("$WARP" --version 2>&1 | head -1)"

# --- 1. server -------------------------------------------------------------------------------
[ -x "$BIN" ] || fail "binary not found or not executable: $BIN (build it: cargo build --bin cairn)"

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
note "cairn healthy on 127.0.0.1:$PORT"

# --- 2. the benchmark bucket -----------------------------------------------------------------
# warp creates (and, without --noclear, clears) the --bucket itself during its prepare step, so
# there is nothing to pre-create here; --noclear keeps the bucket and its objects between phases.

# --- 3. warp phases --------------------------------------------------------------------------
# Common flags. v1.0.0 auto-detects path-style for an IP:port --host, so no --path-style/--lookup
# flag is needed (and v1.0.0 does not accept one); newer warp would take `--lookup path`.
common=(--host "127.0.0.1:$PORT" --access-key "$AKID" --secret-key "$SECRET"
        --region "$REGION" --bucket "$BUCKET" --obj.size "$OBJ_SIZE"
        --concurrent "$CONCURRENT" --noclear)

total_errors=0
run_phase() {
  local name="$1"; shift
  printf '\n=== warp %s (obj.size=%s concurrent=%s duration=%s) ===\n' \
    "$name" "$OBJ_SIZE" "$CONCURRENT" "$DURATION"
  local out rc
  # Capture so we can both print the report and parse the error count. warp exits non-zero when
  # prepare aborts (get/mixed); `put` exits zero even with per-op errors.
  set +e
  out="$("$WARP" "$name" "${common[@]}" "$@" 2>&1)"
  rc=$?
  set -e
  # Drop the per-object error spam (the known key-encoding blocker) and the blank lines warp
  # emits between them; keep the throughput report and the prepare-abort line. We still count the
  # errors below.
  printf '%s\n' "$out" \
    | grep -vE 'warp: <ERROR>.*(signature does not match|upload error|download error)' \
    | grep -vE '^[[:space:]]*$' || true
  # Pull warp's own "Errors: N" line if present.
  local errs
  errs="$(printf '%s\n' "$out" | sed -n 's/^Errors: \([0-9]*\).*/\1/p' | tail -1)"
  [ -z "$errs" ] && errs=0
  # Count raw per-op error lines too (covers the prepare-abort case where no summary prints).
  local raw
  raw="$(printf '%s\n' "$out" | grep -cE '<ERROR>.*(signature does not match|error)' || true)"
  total_errors=$((total_errors + errs + (rc != 0 ? (raw > 0 ? raw : 1) : 0)))
  if [ "$rc" -ne 0 ]; then
    printf '  (warp %s exited %s; see the known-blocker note in this script / docs/benchmarks.md)\n' \
      "$name" "$rc"
  fi
}

run_phase put   --duration "$DURATION"
run_phase get   --objects "$OBJECTS" --duration "$DURATION"
run_phase mixed --objects "$OBJECTS" --duration "$DURATION"

# --- 4. verdict ------------------------------------------------------------------------------
printf '\n=== summary ===\n'
if [ "$total_errors" -eq 0 ]; then
  note "all warp phases completed with zero operation errors"
  exit 0
fi
printf 'WARP REPORTED %s OPERATION ERROR(S).\n' "$total_errors" >&2
exit 1
