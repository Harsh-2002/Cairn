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
# The same snapshot also carries a completed THREE-PART multipart object (two >=5 MiB parts and a
# small tail). Its v2 `<uuid>.meta` sidecar is the object's durability authority: the harness proves
# the snapshot pairs it with its data file, decodes it (format_version=2 plus the exact part layout),
# and re-reads `?partNumber=1/2/3` after restore into a fresh tree, concatenating byte-exactly. A
# final arm downgrades that restored sidecar to a layout-less v1 record and deletes the index row
# (a simulated lost WAL tail), so startup rebuild must re-create the row WITHOUT parts and fail
# part-number reads closed (`400 InvalidPart`) instead of serving the wrong byte range.
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
D3="$WORK/legacy-meta"    # a second fresh target for the layout-less v1 sidecar arm
BK="$WORK/backup"         # the snapshot
PAY="$WORK/payloads"      # local copies of every object body, for byte-identical verification

export CAIRN_API_ADDR="127.0.0.1:$PORT"
export CAIRN_CONSOLE_ADDR=off
export CAIRN_MASTER_KEY; CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"

SRV=""
cleanup() {
  if [ -n "$SRV" ]; then
    kill "$SRV" 2>/dev/null || true
    wait "$SRV" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
ok() { printf '  ok: %s\n' "$*"; }

ENDPOINT="http://127.0.0.1:$PORT"
BUCKET="bkp"
MP_BUCKET="bkmp"
MP_KEY="multi.bin"
MP_S1=5242880               # S3's non-final part minimum, exactly 5 MiB
MP_S2=5242880
MP_S3=1048576               # final part may be smaller
MP_TOTAL=$((MP_S1 + MP_S2 + MP_S3))
PYBIN="${PY:-python3}"

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
  if [ -n "$SRV" ]; then
    kill "$SRV" 2>/dev/null || true
    wait "$SRV" 2>/dev/null || true
  fi
  SRV=""
}

# S3 helpers (Bearer auth; the data plane accepts the management Bearer token).
put_obj() { curl -fsS -X PUT -H "Authorization: Bearer $BEARER" --data-binary @"$3" "$ENDPOINT/$1/$2" >/dev/null; }
get_code() { curl -s -o "$1" -w '%{http_code}' -H "Authorization: Bearer $BEARER" "$ENDPOINT/$2/$3"; }

# Read one response header case-insensitively; fail rather than echo an empty value.
header_value() { # headers-file name
  awk -v want="$2" '
    BEGIN { want = tolower(want) ":" }
    tolower($1) == want {
      value = $2
      for (field = 3; field <= NF; field++) value = value " " $field
      sub(/\r$/, "", value)
    }
    END { if (value == "") exit 1; print value }
  ' "$1"
}

# S3 `?partNumber` reads. A completed multipart object answers 206 with the part's content range
# and `x-amz-mp-parts-count`; the returned bytes must be the exact uploaded part.
check_mp_parts() { # label
  local label="$1" number size offset last want status headers body
  local -a part_files=()
  offset=0
  for number in 1 2 3; do
    eval "size=\$MP_S$number"
    last=$((offset + size - 1))
    body="$WORK/mp-part-$number.bin"
    headers="$WORK/mp-part-$number.hdr"
    status="$(curl -s -o "$body" -D "$headers" -w '%{http_code}' \
      -H "Authorization: Bearer $BEARER" \
      "$ENDPOINT/$MP_BUCKET/$MP_KEY?partNumber=$number")"
    [ "$status" = "206" ] || fail "$label: GET ?partNumber=$number expected 206, got $status"
    [ "$(header_value "$headers" content-length)" = "$size" ] \
      || fail "$label: part $number is not $size bytes"
    want="bytes $offset-$last/$MP_TOTAL"
    [ "$(header_value "$headers" content-range)" = "$want" ] \
      || fail "$label: part $number content-range is not '$want'"
    [ "$(header_value "$headers" x-amz-mp-parts-count)" = "3" ] \
      || fail "$label: part $number does not report the 3-part layout"
    cmp -s "$body" "$PAY/part$number.bin" || fail "$label: part $number bytes differ"
    part_files+=("$body")
    offset=$((offset + size))
  done
  cat "${part_files[@]}" > "$WORK/mp-parts-concat.bin"
  cmp -s "$WORK/mp-parts-concat.bin" "$PAY/multi.bin" \
    || fail "$label: concatenated part reads differ from the original object"
  [ "$(wc -c <"$WORK/mp-parts-concat.bin")" -eq "$MP_TOTAL" ] \
    || fail "$label: concatenated part reads are not $MP_TOTAL bytes"
}

# Decode the v2 sidecar and assert the exact durability record: one `<uuid>.meta` paired with its
# data file, format_version=2, the recorded composite ETag and the whole part layout. Prints the
# sidecar filename so the caller can compare snapshot and restored copies.
assert_mp_sidecar() { # bucket-dir total etag-without-quotes
  "$PYBIN" - "$1" "$2" "$3" "$MP_S1" "$MP_S2" "$MP_S3" <<'PY'
import json
import pathlib
import sys

directory = pathlib.Path(sys.argv[1])
total = int(sys.argv[2])
etag = sys.argv[3]
sizes = [int(value) for value in sys.argv[4:7]]
assert total == sum(sizes), "fixture sizes disagree with the total"
sidecars = sorted(directory.glob("*.meta"))
assert len(sidecars) == 1, f"expected exactly one sidecar, found {[p.name for p in sidecars]}"
sidecar = sidecars[0]
data = [path for path in directory.iterdir()
        if path.is_file() and not path.name.endswith((".meta", ".tmp"))]
assert [path.name for path in data] == [sidecar.name[:-5]], f"sidecar/data pairing: {data}"
record = json.loads(sidecar.read_text())
assert record["format_version"] == 2, record.get("format_version")
assert record["bucket"] == "bkmp" and record["key"] == "multi.bin", record
assert record["storage_path"] == f"bkmp/{sidecar.name[:-5]}", record.get("storage_path")
assert record["size_logical"] == total, record.get("size_logical")
assert record["size_physical"] == total, record.get("size_physical")
assert record["etag"] == etag, (record.get("etag"), etag)
layout = record.get("multipart_parts")
assert layout is not None, "a v2 sidecar must carry its part layout"
assert [(part["part_number"], part["size"]) for part in layout] == \
    [(number, size) for number, size in enumerate(sizes, start=1)], layout
payload = data[0].read_bytes()
assert len(payload) == total, len(payload)
# An assembled object keeps its metadata in the sidecar, never an inline trailer: a plaintext
# object's `CRNI` suffix would make the same bytes rebuild a different (ordinary) record.
assert payload[-4:] != b"CRNI", "completed multipart data must not carry an inline trailer"
print(sidecar.name)
PY
}

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
# Three multipart parts: S3 requires every non-final part to be at least 5 MiB.
head -c "$MP_S1" /dev/urandom > "$PAY/part1.bin"
head -c "$MP_S2" /dev/urandom > "$PAY/part2.bin"
head -c "$MP_S3" /dev/urandom > "$PAY/part3.bin"
cat "$PAY/part1.bin" "$PAY/part2.bin" "$PAY/part3.bin" > "$PAY/multi.bin"

# Initiate, upload three parts, complete. The completion ETag is the composite `<md5>-<count>`,
# which is what makes a lost/absent layout fail closed instead of silently serving one part.
create_multipart_object() {
  local init upload xml number status etag
  local -a etags=()
  init="$(curl -fsS -X POST --data-binary '' -H "Authorization: Bearer $BEARER" \
    "$ENDPOINT/$MP_BUCKET/$MP_KEY?uploads")" || fail "initiate multipart upload failed"
  upload="$(printf '%s' "$init" | sed -n 's:.*<UploadId>\([^<]*\)</UploadId>.*:\1:p')"
  [ -n "$upload" ] || fail "initiate response carried no UploadId: $init"
  for number in 1 2 3; do
    status="$(curl -s -o /dev/null -D "$WORK/mp-upload-$number.hdr" -w '%{http_code}' \
      -X PUT -H "Authorization: Bearer $BEARER" --data-binary @"$PAY/part$number.bin" \
      "$ENDPOINT/$MP_BUCKET/$MP_KEY?uploadId=$upload&partNumber=$number")"
    [ "$status" = "200" ] || fail "UploadPart $number expected 200, got $status"
    etag="$(header_value "$WORK/mp-upload-$number.hdr" etag)" || fail "UploadPart $number returned no ETag"
    etags+=("$etag")
  done
  xml="<CompleteMultipartUpload>"
  for number in 1 2 3; do
    xml+="<Part><PartNumber>$number</PartNumber><ETag>${etags[$((number - 1))]}</ETag></Part>"
  done
  xml+="</CompleteMultipartUpload>"
  status="$(curl -s -o "$WORK/mp-complete.xml" -D "$WORK/mp-complete.hdr" -w '%{http_code}' \
    -X POST -H "Authorization: Bearer $BEARER" -H 'Content-Type: application/xml' \
    --data-binary "$xml" "$ENDPOINT/$MP_BUCKET/$MP_KEY?uploadId=$upload")"
  [ "$status" = "200" ] || fail "CompleteMultipartUpload expected 200, got $status: $(cat "$WORK/mp-complete.xml")"
}

# --- 1) populate + verify on the primary ------------------------------------------------------
start_node
curl -fsS -X PUT -H "Authorization: Bearer $BEARER" "$ENDPOINT/$BUCKET" >/dev/null || fail "create bucket failed"
curl -fsS -X PUT -H "Authorization: Bearer $BEARER" "$ENDPOINT/$MP_BUCKET" >/dev/null \
  || fail "create multipart bucket failed"
for k in $KEYS; do put_obj "$BUCKET" "$k" "$PAY/$k" || fail "PUT $k failed"; done
for k in $KEYS; do
  st="$(get_code "$WORK/got" "$BUCKET" "$k")"
  [ "$st" = "200" ] || fail "primary GET $k expected 200, got $st"
  cmp -s "$WORK/got" "$PAY/$k" || fail "primary GET $k is not byte-identical"
done
ok "populated 4 objects (2MiB random, 256KiB zeros, 8KiB text, tiny) and verified byte-identical"

create_multipart_object
st="$(curl -s -o "$WORK/mp-full.bin" -D "$WORK/mp-full.hdr" -w '%{http_code}' \
  -H "Authorization: Bearer $BEARER" "$ENDPOINT/$MP_BUCKET/$MP_KEY")"
[ "$st" = "200" ] || fail "primary GET $MP_KEY expected 200, got $st"
cmp -s "$WORK/mp-full.bin" "$PAY/multi.bin" || fail "primary GET $MP_KEY is not byte-identical"
check_mp_parts "primary"
MP_ETAG="$(header_value "$WORK/mp-full.hdr" etag)"
MP_ETAG="${MP_ETAG#\"}"; MP_ETAG="${MP_ETAG%\"}"
printf '%s' "$MP_ETAG" | grep -Eq '^[0-9a-f]{32}-3$' \
  || fail "completed multipart object ETag is not a 3-part composite: $MP_ETAG"
ok "created a 3-part multipart object ($MP_S1+$MP_S2+$MP_S3 bytes, ETag $MP_ETAG) and read every ?partNumber back"
stop_node

# --- 2) clean integrity baseline + backup -----------------------------------------------------
OUT="$("$BIN" integrity)" || fail "integrity (baseline) failed: $OUT"
echo "$OUT" | grep -q "reconciliation complete:" || fail "integrity printed no 'reconciliation complete' line: $OUT"
ERRS="$(echo "$OUT" | sed -n 's/.*errors=\([0-9]*\).*/\1/p')"
[ "$ERRS" = "0" ] || fail "integrity baseline reported errors=$ERRS (expected 0): $OUT"
ok "integrity baseline is clean (errors=0)"

OUT="$("$BIN" backup "$BK")" || fail "backup failed: $OUT"
echo "$OUT" | grep -q "backup complete:" || fail "backup printed no 'backup complete' line: $OUT"
ENTRIES="$(echo "$OUT" | sed -n 's/.*(\([0-9][0-9]*\) blob entries.*/\1/p')"
if ! { [ -n "$ENTRIES" ] && [ "$ENTRIES" -ge 1 ]; }; then
  fail "backup reported a non-positive blob-entry count: $OUT"
fi
[ -f "$BK/metadata.sqlite3" ] || fail "snapshot is missing fixed metadata.sqlite3"
[ -f "$BK/manifest.json" ] || fail "snapshot is missing the completion manifest"
grep -q '"complete": true' "$BK/manifest.json" || fail "snapshot manifest is not marked complete"
grep -q '"database_file": "metadata.sqlite3"' "$BK/manifest.json" \
  || fail "snapshot manifest does not bind the fixed database filename"
[ -d "$BK/blobs" ] || fail "snapshot is missing the blobs/ tree"
ok "backup wrote manifest.json last + metadata.sqlite3 + blobs/ ($ENTRIES blob entries)"

# The snapshot pairs the v2 multipart sidecar with its data file under blobs/, and the record it
# carries is the only durable description of the part layout. Assert the exact bytes of that
# contract here, while the primary is stopped and the copy is a point-in-time image.
MP_SIDECAR="$(assert_mp_sidecar "$BK/blobs/$MP_BUCKET" "$MP_TOTAL" "$MP_ETAG")" \
  || fail "the snapshot's multipart sidecar is not a valid v2 part-layout record"
[ -f "$BK/blobs/$MP_BUCKET/${MP_SIDECAR%.meta}" ] \
  || fail "the snapshot's multipart sidecar has no data file"
ok "snapshot carries $MP_BUCKET/$MP_SIDECAR (format_version=2, parts $MP_S1/$MP_S2/$MP_S3, ETag $MP_ETAG) with its data file"

# --- 3) corrupt the primary's largest blob AFTER the snapshot ---------------------------------
# Scoped to the ordinary bucket: the multipart object lives in its own bucket and must stay intact
# for the post-restore part-number arm (and it is larger than every ordinary blob).
victim="$(find "$D1/data/$BUCKET" -type f ! -path '*/.staging/*' ! -name '*.db' ! -name '*.db-wal' ! -name '*.db-shm' \
  -printf '%s\t%p\n' 2>/dev/null | sort -rn | sed -n '1p' | cut -f2-)"
[ -n "$victim" ] || fail "could not locate the largest primary blob to corrupt"
# PROVE the corruption, do not assume it. This used to `printf '\xff\xff\xff\xff' | dd … 2>/dev/null`
# and then print "corrupted" unconditionally: dd's status was discarded, and writing 0xff over bytes
# that are ALREADY 0xff is a no-op — so a silently-skipped write left this step asserting a
# precondition it had not established, and everything downstream would pass without ever testing
# post-snapshot bit rot. (This is the scrub.sh failure mode; see corrupt_blob there.) Flip the
# existing bytes instead, so the write always changes the file, and verify that it did.
vsize="$(wc -c <"$victim")"
[ "${vsize:-0}" -ge 1025 ] || fail "the largest primary blob is only ${vsize}B — offset 1024 is past EOF, so the bit-rot step cannot land"
vbefore="$(md5sum "$victim" | cut -d' ' -f1)"
# Read the byte that is actually there, XOR it, write it back: always a real change, whatever it was.
vbyte="$(dd if="$victim" bs=1 skip=1024 count=1 status=none | od -An -tu1 | tr -d ' \n')"
[ -n "$vbyte" ] || fail "could not read the byte at offset 1024 of $victim"
printf '%b' "$(printf '\\x%02x' "$(( vbyte ^ 0x5A ))")" \
  | dd of="$victim" bs=1 seek=1024 count=1 conv=notrunc status=none \
  || fail "the corrupting write itself failed on $victim"
sync
vafter="$(md5sum "$victim" | cut -d' ' -f1)"
[ "$vbefore" != "$vafter" ] || fail "the corrupting write did not change $victim — the post-snapshot bit-rot step never happened"
ok "corrupted the primary's largest blob (post-snapshot bit-rot): $(basename "$victim") [md5 ${vbefore:0:8} -> ${vafter:0:8}]"

# --- 4) restore the snapshot into a FRESH data dir; everything round-trips ---------------------
use_data_dir "$D2"
OUT="$("$BIN" restore "$BK")" || fail "restore failed: $OUT"
echo "$OUT" | grep -q "restore complete: reconciled scanned=" || fail "restore printed no 'restore complete' line: $OUT"
SCANNED="$(echo "$OUT" | sed -n 's/.*scanned=\([0-9]*\).*/\1/p')"
if ! { [ -n "$SCANNED" ] && [ "$SCANNED" -ge 1 ]; }; then
  fail "restore reconcile scanned a non-positive count: $OUT"
fi
ok "restore into a fresh data dir reconciled (scanned=$SCANNED)"

# The restored sidecar must be the snapshot's own record, byte for byte, and must still describe a
# v2 part-layout object paired with its restored data file.
RESTORED_SIDECAR="$(assert_mp_sidecar "$D2/data/$MP_BUCKET" "$MP_TOTAL" "$MP_ETAG")" \
  || fail "the restored multipart sidecar is not a valid v2 part-layout record"
[ "$RESTORED_SIDECAR" = "$MP_SIDECAR" ] \
  || fail "restore changed the multipart sidecar name ($MP_SIDECAR -> $RESTORED_SIDECAR)"
cmp -s "$BK/blobs/$MP_BUCKET/$MP_SIDECAR" "$D2/data/$MP_BUCKET/$RESTORED_SIDECAR" \
  || fail "restore changed the multipart sidecar bytes"
ok "restored tree carries the snapshot's v2 sidecar byte-for-byte with its data file"

start_node
for k in $KEYS; do
  st="$(get_code "$WORK/got" "$BUCKET" "$k")"
  [ "$st" = "200" ] || fail "restored GET $k expected 200, got $st"
  cmp -s "$WORK/got" "$PAY/$k" || fail "restored GET $k is not byte-identical (snapshot fidelity broken)"
done
st="$(curl -s -o "$WORK/mp-full.bin" -D "$WORK/mp-full.hdr" -w '%{http_code}' \
  -H "Authorization: Bearer $BEARER" "$ENDPOINT/$MP_BUCKET/$MP_KEY")"
[ "$st" = "200" ] || fail "restored GET $MP_KEY expected 200, got $st"
cmp -s "$WORK/mp-full.bin" "$PAY/multi.bin" || fail "restored GET $MP_KEY is not byte-identical"
[ "$(header_value "$WORK/mp-full.hdr" etag)" = "\"$MP_ETAG\"" ] \
  || fail "restored GET $MP_KEY did not echo the recorded composite ETag"
check_mp_parts "restored"
ok "every object GETs back byte-identical from the restored node (snapshot unaffected by primary damage)"
ok "restored ?partNumber=1/2/3 returns the three parts and concatenates to the original object"
stop_node

# --- 5) integrity --repair drops exactly the one dangling row ---------------------------------
victim2="$(find "$D2/data/$BUCKET" -type f ! -path '*/.staging/*' ! -name '*.db' ! -name '*.db-wal' ! -name '*.db-shm' \
  -printf '%s\t%p\n' 2>/dev/null | sort -rn | sed -n '1p' | cut -f2-)"
[ -n "$victim2" ] || fail "could not locate the largest restored blob to delete"
rm -f "$victim2"
OUT="$("$BIN" integrity --repair)" || fail "integrity --repair failed: $OUT"
echo "$OUT" | grep -q "repair complete: dangling_rows_dropped=" || fail "repair printed no 'repair complete' line: $OUT"
DROPPED="$(echo "$OUT" | sed -n 's/.*dangling_rows_dropped=\([0-9]*\).*/\1/p')"
if ! { [ -n "$DROPPED" ] && [ "$DROPPED" -ge 1 ]; }; then
  fail "repair dropped no dangling rows after deleting a blob: $OUT"
fi
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

# --- 6) a layout-less v1 sidecar must fail ?partNumber closed after a row-loss rebuild ----------
# A pre-v2 binary wrote a sidecar without the part layout. Simulate that durable record together
# with a lost index row (the WAL tail that published it is gone), so startup rebuild re-creates the
# row from the sidecar alone. The composite ETag still marks a multipart object, so with no layout
# the part-number read must fail closed rather than serve part 1 (or the whole object) as the answer.
use_data_dir "$D3"
OUT="$("$BIN" restore "$BK")" || fail "restore into the legacy-sidecar target failed: $OUT"
LEGACY_SIDECAR="$D3/data/$MP_BUCKET/$MP_SIDECAR"
[ -f "$LEGACY_SIDECAR" ] || fail "the legacy-sidecar target is missing $MP_SIDECAR"
"$PYBIN" - "$LEGACY_SIDECAR" "$D3/data/cairn.db" <<'PY' || fail "could not build the layout-less v1 fixture"
import json
import sqlite3
import sys

sidecar, database = sys.argv[1], sys.argv[2]
record = json.loads(open(sidecar).read())
# Exactly what an older writer's v1 record can express: the object's identity and bytes, no layout.
record["format_version"] = 1
record.pop("multipart_parts", None)
with open(sidecar, "w") as file:
    json.dump(record, file, separators=(",", ":"))

with sqlite3.connect(database) as connection:
    connection.execute("PRAGMA foreign_keys=ON")
    row = connection.execute(
        "SELECT size_logical, size_physical, owner_id FROM object_versions "
        "WHERE bucket_name = 'bkmp' AND key = 'multi.bin'").fetchone()
    assert row is not None, "fixture object row missing"
    logical, physical, owner = row
    deleted = connection.execute(
        "DELETE FROM object_versions WHERE bucket_name = 'bkmp' AND key = 'multi.bin'").rowcount
    assert deleted == 1, deleted
    # A lost WAL tail also loses the writer-maintained roll-ups, so remove them too: starting from
    # accounting no real power loss could produce would make the fixture prove the wrong thing.
    connection.execute(
        "UPDATE bucket_stats SET objects = objects - 1, versions = versions - 1, "
        "logical_bytes = logical_bytes - ?, physical_bytes = physical_bytes - ? "
        "WHERE bucket_name = 'bkmp'", (logical, physical))
    connection.execute(
        "UPDATE user_stats SET logical_bytes = logical_bytes - ? WHERE owner_id = ?", (logical, owner))
PY

start_node
if ! READ_BACK="$("$PYBIN" - "$D3/data/cairn.db" <<'PY'
import sqlite3
import sys

connection = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
row = connection.execute(
    "SELECT etag, multipart_parts, size_logical FROM object_versions "
    "WHERE bucket_name = 'bkmp' AND key = 'multi.bin'").fetchall()
assert len(row) == 1, row
etag, layout, size = row[0]
assert layout is None, layout
assert etag.endswith("-3"), etag
print(f"{etag} {size}")
PY
)"; then
  fail "startup rebuild did not restore exactly one layout-less object row"
fi
[ "$READ_BACK" = "$MP_ETAG $MP_TOTAL" ] \
  || fail "the rebuilt row is not the layout-less v1 record: $READ_BACK"
st="$(curl -s -o "$WORK/legacy-part.bin" -w '%{http_code}' -H "Authorization: Bearer $BEARER" \
  "$ENDPOINT/$MP_BUCKET/$MP_KEY?partNumber=1")"
[ "$st" = "400" ] || fail "layout-less partNumber=1 expected 400, got $st"
grep -q '<Code>InvalidPart</Code>' "$WORK/legacy-part.bin" \
  || fail "layout-less partNumber=1 is not InvalidPart: $(cat "$WORK/legacy-part.bin")"
st="$(get_code "$WORK/legacy-full.bin" "$MP_BUCKET" "$MP_KEY")"
[ "$st" = "200" ] || fail "layout-less plain GET expected 200, got $st"
cmp -s "$WORK/legacy-full.bin" "$PAY/multi.bin" || fail "layout-less plain GET is not byte-identical"
ok "a layout-less v1 sidecar rebuilds the row without parts and ?partNumber fails closed (400 InvalidPart)"
stop_node

echo "BACKUP/RESTORE OK — snapshot fidelity + reconcile baseline + surgical integrity --repair"
echo "PASS: backup/restore/integrity holds end-to-end, including v2 multipart sidecars and ?partNumber"

# The complementary stdlib driver populates durable state beyond current plaintext objects.
BIN="$BIN" "${PY:-python3}" "$ROOT/conformance/recovery_state.py"
