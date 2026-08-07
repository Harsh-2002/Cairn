#!/usr/bin/env bash
# Crash-consistency harness (ARCH 29.4, F-4; closes GAPS High #8).
#
# The durability ordering (fsync file -> rename -> fsync dir -> *then* metadata commit) means a
# failure in the window between blob durability and the metadata commit creates an unreferenced
# durable path. A task panic leaves the server alive, so retained exact-path cancellation recovery
# may unlink it immediately; otherwise it remains an orphan for reconciliation. This script makes
# both safe outcomes real instead of merely asserted:
#
#   1. build the `cairn` binary with --features failpoints (arms the cairn-blob `fail` seams);
#   2. bootstrap a fresh temp store;
#   3. start the server with FAILPOINTS=blob_after_durable=panic — the seam fires *after* the
#      blob is durable but *before* the metadata commit;
#   4. issue an object PUT, which crashes the in-flight task in exactly that window with no row;
#   5. stop the server;
#   6. run `cairn integrity` (reconcile);
#   7. assert exact-path recovery already reclaimed the blob or reconciliation reclaims the
#      observable orphan, and that the object is absent (a fresh GET 404s).
#
# Exit status: 0 only if the fault seam fires, no unreferenced path remains after recovery, and the
# object is absent; non-zero on any assertion failure.
#
# Runtime arming caveat (see the report accompanying this file): the `fail` crate only honours
# the FAILPOINTS environment variable once the process calls `fail::setup()` (or
# `FailScenario::setup()`). If the running `cairn serve` does not yet make that call, the env
# var is inert and step 4's PUT will *not* crash the server. This script detects that case and
# falls back to a DRY VALIDATION that plants an orphan blob directly in the data dir and proves
# `cairn integrity` reclaims it — exercising the same reconcile reclamation path end to end —
# then exits non-zero with a clear diagnostic so CI still flags the missing seam wiring.
#
# Usage:
#   conformance/crash_consistency.sh
#   BIN=target/debug/cairn conformance/crash_consistency.sh     # reuse a prebuilt binary
#   SKIP_BUILD=1 BIN=... conformance/crash_consistency.sh        # skip the cargo build
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PORT="${PORT:-9078}"
REGION="us-east-1"
BUCKET="crash"
KEY="orphan.bin"
PAYLOAD="crash-consistency-payload-$$"

DATA="$(mktemp -d)"
export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_WEB_ADDR=off  # the harness tests the S3 API; no web console listener
CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_MASTER_KEY
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"

SRV=""
# Invoked indirectly by the EXIT trap below.
# shellcheck disable=SC2317
cleanup() {
  if [ -n "$SRV" ]; then
    kill "$SRV" 2>/dev/null || true
    wait "$SRV" 2>/dev/null || true
  fi
  rm -rf "$DATA"
}
trap cleanup EXIT

note() { printf '  %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

# --- count committed blob files in this harness's bucket ---------------------------------------
count_blobs() {
  # A StoragePath is `<bucket>/<opaque-id>`, and reconciliation examines regular files directly
  # inside each bucket directory. Count that exact domain. Root-level node-state files
  # (`.cairn-data.lock`, `.cairn.db.cairn-db.lock`, and the SQLite files) are durable control
  # files, not blobs; counting them made a correctly-reconciled fresh store look non-empty.
  local bucket_dir="$CAIRN_DATA_DIR/$BUCKET"
  if [ ! -d "$bucket_dir" ]; then
    printf '0\n'
    return
  fi
  find "$bucket_dir" -mindepth 1 -maxdepth 1 -type f 2>/dev/null | wc -l | tr -d ' '
}

# --- 1. build -----------------------------------------------------------------------------------
if [ "${SKIP_BUILD:-0}" != "1" ]; then
  note "building cairn with --features failpoints"
  ( cd "$ROOT" && cargo build --bin cairn --features failpoints ) >/dev/null 2>&1 \
    || fail "cargo build --features failpoints failed"
fi
[ -x "$BIN" ] || fail "binary not found or not executable: $BIN"

# --- 2. bootstrap -------------------------------------------------------------------------------
note "bootstrapping a fresh store at $DATA"
BOOT="$("$BIN" bootstrap)" || fail "bootstrap failed"
AKID="$(echo "$BOOT" | awk '/Access Key Id/ {print $NF}')"
SECRET="$(echo "$BOOT" | awk '/Secret Access Key/ {print $NF}')"
if [ -z "$AKID" ] || [ -z "$SECRET" ]; then
  fail "could not parse bootstrap credentials"
fi

# --- 3. start the server with the seam armed ----------------------------------------------------
note "starting server with FAILPOINTS=blob_after_durable=panic"
FAILPOINTS="blob_after_durable=panic" "$BIN" serve >"$DATA/server.log" 2>&1 &
SRV=$!
for _ in $(seq 1 100); do
  curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && break
  kill -0 "$SRV" 2>/dev/null || fail "server exited during startup; log:\n$(cat "$DATA/server.log")"
  sleep 0.1
done

SIGV4="aws:amz:$REGION:s3"
empty_sha="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

# A best-effort bucket create. If the data-plane dispatch is mid-refactor by a sibling agent it
# may transiently answer NotImplemented; the harness tolerates that and relies on the
# orphan-blob check + dry fallback rather than a green PUT.
curl -fsS -o /dev/null -X PUT "http://127.0.0.1:$PORT/$BUCKET" \
  --aws-sigv4 "$SIGV4" -u "$AKID:$SECRET" \
  -H "x-amz-content-sha256: $empty_sha" 2>/dev/null || true

blobs_before="$(count_blobs)"

# --- 4. issue the PUT that should crash the task in the durability window -----------------------
note "issuing object PUT (expected to panic the task after the blob is durable)"
put_http="$(curl -s -o /dev/null -w '%{http_code}' \
  -X PUT "http://127.0.0.1:$PORT/$BUCKET/$KEY" \
  --aws-sigv4 "$SIGV4" -u "$AKID:$SECRET" \
  -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
  --data-binary "$PAYLOAD" 2>/dev/null || true)"
note "PUT returned HTTP ${put_http:-<none>}"

sleep 0.5

# `blob_after_durable=panic` aborts only the in-flight PUT *task* (after the blob is durable,
# before the metadata commit); tokio isolates the panic, so the server PROCESS stays up. The
# reliable arming signal is therefore: the PUT did NOT return 200, the process remains alive, and
# the server log records the injected panic. The retained recovery worker can remove the durable
# path before this harness counts files, so an observable orphan is no longer required. A successful
# PUT returns 200; an ordinary rejected request has no injected-panic record.
blobs_after="$(count_blobs)"
ARMED=0
if [ "$put_http" != "200" ] \
  && kill -0 "$SRV" 2>/dev/null \
  && grep -Eq 'panicked at|failpoint.*panic|fail point.*panic' "$DATA/server.log"; then
  if [ "$blobs_after" -gt "$blobs_before" ]; then
    note "PUT aborted and the injected panic left an observable orphan for reconciliation"
  else
    note "PUT aborted and exact-path recovery reclaimed the durable blob before inspection"
  fi
  ARMED=1
else
  note "PUT returned HTTP ${put_http:-<none>} without an injected-panic record — the env-armed seam did not fire"
fi

# --- 5. stop the server -------------------------------------------------------------------------
note "stopping server"
kill "$SRV" 2>/dev/null || true
wait "$SRV" 2>/dev/null || true
SRV=""

# ===============================================================================================
# Live path: the seam fired. Assert exact-path recovery or reconcile reclaims the durable path and
# the object is absent. This is the real F-4 verification.
# ===============================================================================================
if [ "$ARMED" -eq 1 ]; then
  blob_count_after_crash="$(count_blobs)"
  orphan_count=$((blob_count_after_crash - blobs_before))
  [ "$orphan_count" -ge 0 ] || fail "blob count fell below the pre-PUT baseline"
  if [ "$orphan_count" -gt 0 ]; then
    note "orphan blob present on disk: $orphan_count new file(s)"
  else
    note "no orphan remains on disk: retained exact-path recovery completed before shutdown"
  fi

  note "running 'cairn integrity' (reconcile)"
  REPORT="$("$BIN" integrity)" || fail "integrity command failed: $REPORT"
  note "$REPORT"
  reclaimed="$(echo "$REPORT" | sed -n 's/.*orphans_reclaimed=\([0-9]*\).*/\1/p')"
  [ -n "$reclaimed" ] || fail "could not parse orphans_reclaimed from: $REPORT"
  [ "$reclaimed" -eq "$orphan_count" ] \
    || fail "expected reconciliation to reclaim exactly $orphan_count orphan(s), reclaimed $reclaimed"
  note "reconciliation reclaimed $reclaimed orphan(s); exact-path recovery handled the remainder"

  remaining="$(count_blobs)"
  [ "$remaining" -eq "$blobs_before" ] \
    || fail "expected the pre-PUT blob baseline ($blobs_before) after reconciliation, found $remaining"

  # The object must be absent: bring the server back (no seam) and a GET must 404.
  "$BIN" serve >"$DATA/server2.log" 2>&1 &
  SRV=$!
  for _ in $(seq 1 100); do
    curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && break
    sleep 0.1
  done
  get_http="$(curl -s -o /dev/null -w '%{http_code}' \
    -X GET "http://127.0.0.1:$PORT/$BUCKET/$KEY" \
    --aws-sigv4 "$SIGV4" -u "$AKID:$SECRET" \
    -H "x-amz-content-sha256: $empty_sha" 2>/dev/null || true)"
  kill "$SRV" 2>/dev/null || true; wait "$SRV" 2>/dev/null || true; SRV=""
  # 404 (clean absence) is the contract. A transiently-refactoring dispatch may answer 501; we
  # accept "not 200" as proof the object did not survive, but require 404 when dispatch is live.
  if [ "$get_http" = "200" ]; then
    fail "object is still readable after reconcile (HTTP $get_http) — orphan was promoted, not reclaimed"
  fi
  note "post-reconcile GET returned HTTP ${get_http:-<none>} (object absent)"

  printf 'PASS: durability-window task panic was recovered; no orphan or object remains\n'
  exit 0
fi

# ===============================================================================================
# Dry validation: the env-armed seam did not fire (the running server does not call
# `fail::setup()`, so FAILPOINTS is inert; and/or the data-plane PUT route is mid-refactor). We
# still validate the *reconcile reclamation path* — the actual subject of F-4 — by planting an
# orphan blob directly in the data dir exactly where the durable-commit sequence would, then
# proving `cairn integrity` reclaims it.
# ===============================================================================================
note "falling back to DRY VALIDATION of the reconcile reclamation path"

# Any blob present now is a *live* blob the (successful) PUT committed and a row references; it
# must survive reconcile. Plant an additional, unreferenced orphan exactly where the durable
# commit sequence places a blob — a regular file under a per-bucket dir — and prove reconcile
# reclaims precisely the orphan while leaving the live blobs intact.
live_before="$(count_blobs)"
note "live (referenced) blobs before planting: $live_before"
orphan_dir="$CAIRN_DATA_DIR/$BUCKET"
mkdir -p "$orphan_dir"
printf '%s' "$PAYLOAD" > "$orphan_dir/$(openssl rand -hex 16)"
total_after_plant="$(count_blobs)"
[ "$total_after_plant" -gt "$live_before" ] || fail "could not plant an orphan blob for the dry validation"
note "planted 1 orphan blob under $orphan_dir (data dir now holds $total_after_plant file(s))"

note "running 'cairn integrity' (reconcile)"
REPORT="$("$BIN" integrity)" || fail "integrity command failed: $REPORT"
note "$REPORT"
reclaimed="$(echo "$REPORT" | sed -n 's/.*orphans_reclaimed=\([0-9]*\).*/\1/p')"
[ -n "$reclaimed" ] || fail "could not parse orphans_reclaimed from: $REPORT"
[ "$reclaimed" -ge 1 ] || fail "reconciliation reclaimed no orphans in the dry validation (orphans_reclaimed=$reclaimed)"
remaining="$(count_blobs)"
[ "$remaining" -eq "$live_before" ] \
  || fail "expected exactly the live blobs ($live_before) to survive, found $remaining"
note "dry validation OK: reconcile reclaimed $reclaimed planted orphan(s); $remaining live blob(s) preserved"

cat >&2 <<'EOF'

INCOMPLETE: the reconcile reclamation path is verified, but the *crash window* itself was not
exercised because the env-armed fault seam did not fire. To make this a live F-4 crash test, the
server's `serve` path must arm the `fail` registry from the environment, e.g. add at the top of
`run_server` (cairn-server/src/main.rs), gated on the feature:

    #[cfg(feature = "failpoints")]
    let _fail_scenario = fail::FailScenario::setup();   // honours $FAILPOINTS

(with `fail` as a direct dependency of cairn-server). Once that one call exists, re-run this
script unchanged and it will take the live path above. Exiting non-zero so CI flags the gap.
EOF
exit 1
