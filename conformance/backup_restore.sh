#!/usr/bin/env bash
# Backup / restore / integrity end-to-end (ARCH 8, operations.md §6, backup-restore.md). Boot a real
# cairn binary, populate objects of varied size AND compressibility, then exercise the full disaster
# path with plain curl (Bearer auth — no SDK):
#   1. populate + verify a byte-identical round-trip on the primary;
#   2. stop, run `cairn integrity` (clean baseline: errors=0), then `cairn backup`;
#   3. corrupt the primary's largest blob (simulate bit-rot AFTER the snapshot);
#   4. `cairn restore` the snapshot into a FRESH data dir and prove every object GETs back
#      byte-identical — the snapshot is a point-in-time copy unaffected by the later primary damage;
#   5. delete a blob on the restored node and prove `cairn integrity --repair` drops exactly the one
#      dangling row (that object 404s; the others still round-trip).
# Each CLI step is synchronous; its stdout counts are parsed and asserted (never a sleep).
#
# (scrub.sh covers on-READ corruption detection; this harness covers snapshot fidelity + repair.)
#
# Usage: BIN=target/debug/cairn bash conformance/backup_restore.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PORT="${PORT:-9098}"
WORK="$(mktemp -d)"
D1="$WORK/primary"        # the live node's data dir
D2="$WORK/restored"       # the disaster-recovery target (fresh)
BK="$WORK/backup"         # the snapshot
PAY="$WORK/payloads"      # local copies of every object body, for byte-identical verification

export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_UI_ADDR=off
export CAIRN_MASTER_KEY; CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"

SRV=""
cleanup() { [ -n "$SRV" ] && kill "$SRV" 2>/dev/null || true; [ -n "$SRV" ] && wait "$SRV" 2>/dev/null || true; rm -rf "$WORK"; }
trap cleanup EXIT
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
ok() { printf '  ok: %s\n' "$*"; }

ENDPOINT="http://127.0.0.1:$PORT"
BUCKET="bkp"

# Point the binary's env at a given data dir (each subcommand and the server read it at exec time).
use_data_dir() {
  export CAIRN_DATA_DIR="$1/data"
  export CAIRN_DB_PATH="$1/data/cairn.db"
}

start_node() {
  "$BIN" serve >"$WORK/server.log" 2>&1 &
  SRV=$!
  for _ in $(seq 1 100); do
    curl -fsS -o /dev/null "$ENDPOINT/healthz" 2>/dev/null && return 0
    kill -0 "$SRV" 2>/dev/null || fail "server exited at startup; log: $(cat "$WORK/server.log")"
    sleep 0.1
  done
  fail "server did not become healthy in time; log: $(cat "$WORK/server.log")"
}
stop_node() {
  [ -n "$SRV" ] && kill "$SRV" 2>/dev/null || true
  [ -n "$SRV" ] && wait "$SRV" 2>/dev/null || true
  SRV=""
}

# S3 helpers (Bearer auth; the data plane accepts the management Bearer token).
put_obj() { curl -fsS -X PUT -H "Authorization: Bearer $BEARER" --data-binary @"$3" "$ENDPOINT/$1/$2" >/dev/null; }
get_code() { curl -s -o "$1" -w '%{http_code}' -H "Authorization: Bearer $BEARER" "$ENDPOINT/$2/$3"; }

[ -x "$BIN" ] || fail "binary not found or not executable: $BIN"

# --- credentials + payloads -------------------------------------------------------------------
use_data_dir "$D1"
BOOT="$("$BIN" bootstrap)" || fail "bootstrap failed"
BEARER="$(echo "$BOOT" | awk '/Authorization: Bearer/ {print $3}')"
[ -n "$BEARER" ] || fail "could not parse bootstrap bearer token"

mkdir -p "$PAY"
head -c 2097152 /dev/urandom > "$PAY/big.bin"   # 2 MiB incompressible → the largest on-disk blob
head -c 262144 /dev/zero    > "$PAY/zeros.bin"  # 256 KiB highly compressible (exercises compression)
# ~8 KiB of text via a loop (no `yes | head`: that races SIGPIPE under `set -o pipefail`).
: > "$PAY/text.txt"
for _ in $(seq 1 100); do
  printf '%s\n' "cairn backup/restore conformance line — repeated to about eight kilobytes of text" >> "$PAY/text.txt"
done
printf 'tiny' > "$PAY/tiny.txt"                 # a handful of bytes
KEYS="big.bin zeros.bin text.txt tiny.txt"

# --- 1) populate + verify on the primary ------------------------------------------------------
start_node
curl -fsS -X PUT -H "Authorization: Bearer $BEARER" "$ENDPOINT/$BUCKET" >/dev/null || fail "create bucket failed"
for k in $KEYS; do put_obj "$BUCKET" "$k" "$PAY/$k" || fail "PUT $k failed"; done
for k in $KEYS; do
  st="$(get_code "$WORK/got" "$BUCKET" "$k")"
  [ "$st" = "200" ] || fail "primary GET $k expected 200, got $st"
  cmp -s "$WORK/got" "$PAY/$k" || fail "primary GET $k is not byte-identical"
done
ok "populated 4 objects (2MiB random, 256KiB zeros, 8KiB text, tiny) and verified byte-identical"
stop_node

# --- 2) clean integrity baseline + backup -----------------------------------------------------
OUT="$("$BIN" integrity)" || fail "integrity (baseline) failed: $OUT"
echo "$OUT" | grep -q "reconciliation complete:" || fail "integrity printed no 'reconciliation complete' line: $OUT"
ERRS="$(echo "$OUT" | sed -n 's/.*errors=\([0-9]*\).*/\1/p')"
[ "$ERRS" = "0" ] || fail "integrity baseline reported errors=$ERRS (expected 0): $OUT"
ok "integrity baseline is clean (errors=0)"

OUT="$("$BIN" backup "$BK")" || fail "backup failed: $OUT"
echo "$OUT" | grep -q "backup complete:" || fail "backup printed no 'backup complete' line: $OUT"
ENTRIES="$(echo "$OUT" | sed -n 's/.*(\([0-9]*\) blob entries).*/\1/p')"
[ -n "$ENTRIES" ] && [ "$ENTRIES" -ge 1 ] || fail "backup reported a non-positive blob-entry count: $OUT"
[ -f "$BK/cairn.db" ] || fail "snapshot is missing the database file"
[ -d "$BK/blobs" ] || fail "snapshot is missing the blobs/ tree"
ok "backup wrote cairn.db + blobs/ ($ENTRIES blob entries)"

# --- 3) corrupt the primary's largest blob AFTER the snapshot ---------------------------------
victim="$(find "$D1/data" -type f ! -path '*/.staging/*' ! -name '*.db' ! -name '*.db-wal' ! -name '*.db-shm' \
  -printf '%s\t%p\n' 2>/dev/null | sort -rn | sed -n '1p' | cut -f2-)"
[ -n "$victim" ] || fail "could not locate the largest primary blob to corrupt"
printf '\xff\xff\xff\xff' | dd of="$victim" bs=1 seek=1024 count=4 conv=notrunc 2>/dev/null
ok "corrupted the primary's largest blob (post-snapshot bit-rot): $(basename "$victim")"

# --- 4) restore the snapshot into a FRESH data dir; everything round-trips ---------------------
use_data_dir "$D2"
OUT="$("$BIN" restore "$BK")" || fail "restore failed: $OUT"
echo "$OUT" | grep -q "restore complete: reconciled scanned=" || fail "restore printed no 'restore complete' line: $OUT"
SCANNED="$(echo "$OUT" | sed -n 's/.*scanned=\([0-9]*\).*/\1/p')"
[ -n "$SCANNED" ] && [ "$SCANNED" -ge 1 ] || fail "restore reconcile scanned a non-positive count: $OUT"
ok "restore into a fresh data dir reconciled (scanned=$SCANNED)"

start_node
for k in $KEYS; do
  st="$(get_code "$WORK/got" "$BUCKET" "$k")"
  [ "$st" = "200" ] || fail "restored GET $k expected 200, got $st"
  cmp -s "$WORK/got" "$PAY/$k" || fail "restored GET $k is not byte-identical (snapshot fidelity broken)"
done
ok "every object GETs back byte-identical from the restored node (snapshot unaffected by primary damage)"
stop_node

# --- 5) integrity --repair drops exactly the one dangling row ---------------------------------
victim2="$(find "$D2/data" -type f ! -path '*/.staging/*' ! -name '*.db' ! -name '*.db-wal' ! -name '*.db-shm' \
  -printf '%s\t%p\n' 2>/dev/null | sort -rn | sed -n '1p' | cut -f2-)"
[ -n "$victim2" ] || fail "could not locate the largest restored blob to delete"
rm -f "$victim2"
OUT="$("$BIN" integrity --repair)" || fail "integrity --repair failed: $OUT"
echo "$OUT" | grep -q "repair complete: dangling_rows_dropped=" || fail "repair printed no 'repair complete' line: $OUT"
DROPPED="$(echo "$OUT" | sed -n 's/.*dangling_rows_dropped=\([0-9]*\).*/\1/p')"
[ -n "$DROPPED" ] && [ "$DROPPED" -ge 1 ] || fail "repair dropped no dangling rows after deleting a blob: $OUT"
ok "integrity --repair dropped the dangling row(s) (dangling_rows_dropped=$DROPPED)"

start_node
# The deleted object (the largest = big.bin) is now gone; the others must still round-trip.
st="$(get_code "$WORK/got" "$BUCKET" "big.bin")"
[ "$st" = "404" ] || fail "the repaired-away object should 404, got $st"
for k in zeros.bin text.txt tiny.txt; do
  st="$(get_code "$WORK/got" "$BUCKET" "$k")"
  [ "$st" = "200" ] || fail "surviving GET $k expected 200, got $st"
  cmp -s "$WORK/got" "$PAY/$k" || fail "surviving GET $k is not byte-identical (repair was not surgical)"
done
ok "the repaired-away object 404s; the other three still round-trip (repair was surgical)"
stop_node

echo "BACKUP/RESTORE OK — snapshot fidelity + reconcile baseline + surgical integrity --repair"
echo "PASS: backup/restore/integrity holds end-to-end"
