#!/usr/bin/env bash
# Integrity-scrub regression (ARCH 8.6/26.4): corrupt a stored blob on disk and assert the
# background scrub re-reads it, fails its integrity check, and reports it as
# `cairn_scrub_corruption_total` — turning silent bit-rot into an observable event rather than a
# corrupted byte served to a client.
#
# FOUR ARMS, one server each, because the scrub used to cover exactly one of them:
#   1. plaintext    — an incompressible object on a plain node (the original arm).
#   2. at-rest      — CAIRN_ENCRYPT_AT_REST=true. Every version on such a node carries an
#                     `sse_descriptor`; the pre-fix scrub skipped all of them, so it verified 0% of
#                     the store while logging `scanned=0 corrupt=0 "scrub pass complete"`.
#   3. sse-s3       — a CLIENT-encrypted object (`x-amz-server-side-encryption: AES256`) on an
#                     otherwise plaintext node: same skip, on any node, for any SSE object.
#   4. compressed   — a zstd-compressed object (the CRNB container must fail its own integrity
#                     check), plus a multipart object asserting the composite-ETag SKIP is COUNTED.
#
# Every arm also asserts the accounting is coherent — a non-zero `cairn_scrub_objects_total`
# (scanned) so no arm can pass vacuously by verifying nothing, and a zero
# `cairn_scrub_skipped_total{reason="key_unavailable"}` on a healthy ring.
#
# Usage: BIN=target/debug/cairn PY=/path/to/python-with-boto3 conformance/scrub.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-/tmp/cairnvenv/bin/python}"
PORT="${PORT:-9086}"
UIPORT="${UIPORT:-9186}"
TMPROOT="$(mktemp -d)"

# Every server this harness starts is registered here and killed by the single EXIT trap: a leaked
# throwaway node keeps a port and a few hundred MB of page cache and has OOM-killed a runner before.
PIDS=()
cleanup() {
  for p in ${PIDS[@]+"${PIDS[@]}"}; do
    kill "$p" 2>/dev/null || true
    wait "$p" 2>/dev/null || true
  done
  rm -rf "$TMPROOT"
}
trap cleanup EXIT
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

[ -x "$BIN" ] || fail "binary not found or not executable: $BIN"
command -v "$PY" >/dev/null 2>&1 || fail "python interpreter not found: $PY (needs boto3)"
"$PY" -c "import boto3" 2>/dev/null || fail "boto3 not importable by $PY"

# Start one throwaway node for an arm. Extra `VAR=value` arguments are added to its environment —
# NOTE for CAIRN_ENCRYPT_AT_REST: strict Figment wants the boolean `true`, not `1`.
# Sets: DATADIR, AK, SK, BEARER, SRV, ARMPORT, ARMUIPORT.
start_node() {
  local arm="$1" port="$2" uiport="$3"
  shift 3
  DATADIR="$TMPROOT/$arm/data"
  ARMPORT="$port"
  ARMUIPORT="$uiport"
  mkdir -p "$DATADIR"
  local mk
  mk="$(openssl rand -hex 32)"
  ENVS=(
    "CAIRN_DATA_DIR=$DATADIR"
    "CAIRN_DB_PATH=$DATADIR/cairn.db"
    "CAIRN_LISTEN_ADDR=127.0.0.1:$port"
    "CAIRN_UI_ADDR=127.0.0.1:$uiport"
    "CAIRN_MASTER_KEY=$mk"
    "CAIRN_LOG_LEVEL=${CAIRN_LOG_LEVEL:-warn}"
    # Scrub every 2s so a pass runs within the test window. NOTE: the scrub is OFF BY DEFAULT
    # (CAIRN_SCRUB_INTERVAL_SECS=0) — this harness has to turn it on, exactly like an operator does.
    "CAIRN_SCRUB_INTERVAL_SECS=2"
    "$@"
  )
  local boot
  boot="$(env "${ENVS[@]}" "$BIN" bootstrap)" || fail "[$arm] bootstrap failed"
  AK="$(echo "$boot" | awk '/Access Key Id/ {print $NF}')"
  SK="$(echo "$boot" | awk '/Secret Access Key/ {print $NF}')"
  BEARER="$(echo "$boot" | awk '/Authorization: Bearer/ {print $3}')"
  [ -n "$AK" ] && [ -n "$SK" ] && [ -n "$BEARER" ] || fail "[$arm] could not parse bootstrap credentials"

  env "${ENVS[@]}" "$BIN" serve >"$TMPROOT/$arm/server.log" 2>&1 &
  SRV=$!
  PIDS+=("$SRV")
  local i
  for i in $(seq 1 100); do
    curl -fsS -o /dev/null "http://127.0.0.1:$port/healthz" 2>/dev/null && break
    kill -0 "$SRV" 2>/dev/null || fail "[$arm] server exited at startup; log: $(cat "$TMPROOT/$arm/server.log")"
    sleep 0.1
  done
}

stop_node() {
  kill "$SRV" 2>/dev/null || true
  wait "$SRV" 2>/dev/null || true
}

# Sum a Prometheus counter family (all label sets) from the arm's /metrics.
metric() {
  curl -fsS "http://127.0.0.1:$ARMPORT/metrics" | awk -v pat="$1" '$1 ~ pat {s+=$2} END {print s+0}'
}
ge1() { [ "${1%.*}" -ge 1 ] 2>/dev/null; }

# Flip one byte of the OBJECT blob stored under bucket $1 (blobs live at
# $CAIRN_DATA_DIR/<bucket>/<id>), simulating bit rot. Deterministically target the LARGEST regular
# file under the bucket dir — the object blob — rather than `find | head -1`, whose traversal order
# is filesystem-dependent: a wrong pick (a sidecar, or another bucket's smaller blob) would leave the
# object's real blob intact, so the scrub reads it clean and the arm "detects nothing" nondeterministically.
corrupt_blob() {
  local bucket="$1" blob size
  blob="$(find "$DATADIR/$bucket" -type f -printf '%s\t%p\n' 2>/dev/null | sort -rn | head -1 | cut -f2-)"
  [ -n "$blob" ] || fail "could not locate a stored blob under $DATADIR/$bucket"
  size="$(wc -c <"$blob")"
  # Guard the selection: the object blobs these arms create are ~1 KiB+; a tiny file means we picked
  # the wrong one and the corruption would land somewhere the scrub never reads.
  [ "${size:-0}" -ge 100 ] 2>/dev/null || fail "picked an implausibly small blob ($size bytes) under $DATADIR/$bucket — refusing to corrupt the wrong file"
  printf '\xff' | dd of="$blob" bs=1 seek="$((size / 2))" count=1 conv=notrunc 2>/dev/null
  echo "  corrupted one byte of $blob ($size bytes)"
}

# The shared assertions every arm makes. POLL for detection rather than a fixed sleep: the scrub runs
# every 2s, but a post-corruption pass now DECRYPTS every object, so on a contended CI runner it can
# take longer than any single fixed wait — a genuinely corrupt blob is detected within a few passes,
# so wait for the counter deterministically (the memory rule: poll, never guess a sleep).
assert_pass() {
  local arm="$1"
  local corrupt scanned key_unavail deadline
  corrupt=0
  deadline=$(( SECONDS + ${SCRUB_POLL_SECS:-60} ))
  while [ "$SECONDS" -lt "$deadline" ]; do
    corrupt="$(metric '^cairn_scrub_corruption_total')"
    ge1 "$corrupt" && break
    sleep 1
  done
  scanned="$(metric '^cairn_scrub_objects_total')"
  key_unavail="$(metric '^cairn_scrub_skipped_total.reason="key_unavailable"')"
  echo "  [$arm] corruption_total=$corrupt objects_total(scanned)=$scanned skipped{key_unavailable}=$key_unavail"
  ge1 "$corrupt" || fail "[$arm] the scrub did not detect the corruption within ${SCRUB_POLL_SECS:-60}s (counter=$corrupt); log: $(tail -5 "$TMPROOT/$arm/server.log")"
  # The zero-count companion: a pass that verified NOTHING must not be able to satisfy this arm.
  ge1 "$scanned" || fail "[$arm] the scrub verified nothing (cairn_scrub_objects_total=$scanned) — an all-skipped pass, not coverage"
  [ "${key_unavail%.*}" -eq 0 ] 2>/dev/null || fail "[$arm] versions skipped for an unavailable key on a healthy ring ($key_unavail)"
}

put_object() { # $1=bucket $2=key $3=size $4=sse ("" or "AES256")
  "$PY" - "$AK" "$SK" "http://127.0.0.1:$ARMPORT" "$1" "$2" "$3" "${4:-}" <<'PY'
import sys, os, boto3
from botocore.config import Config
ak, sk, ep, bucket, key, size, sse = sys.argv[1:8]
s3 = boto3.client("s3", endpoint_url=ep, aws_access_key_id=ak, aws_secret_access_key=sk,
                  region_name="us-east-1", config=Config(s3={"addressing_style": "path"}))
s3.create_bucket(Bucket=bucket)
extra = {"ServerSideEncryption": sse} if sse else {}
s3.put_object(Bucket=bucket, Key=key, Body=os.urandom(int(size)), **extra)
print(f"  put {size}-byte object s3://{bucket}/{key}" + (f" (SSE={sse})" if sse else ""))
PY
}

echo "=== arm 1/4: plaintext (an uncompressed, unencrypted blob) ==="
start_node plaintext "$PORT" "$UIPORT"
put_object scrub obj 100000 ""
corrupt_blob scrub
assert_pass plaintext
stop_node
echo "PASS: the scrub detected corruption of a plaintext blob"

echo "=== arm 2/4: transparent at-rest encryption (CAIRN_ENCRYPT_AT_REST=true) ==="
# Pre-fix this arm FAILS: every version on this node carries an sse_descriptor, so the scrub skipped
# the whole store and reported scanned=0 corrupt=0.
start_node atrest "$((PORT + 1))" "$((UIPORT + 1))" CAIRN_ENCRYPT_AT_REST=true
put_object scrub obj 100000 ""
# A SECOND encrypted object in a DIFFERENT bucket, left INTACT. Its blob is never corrupted, so it
# must be counted verified — which makes `scanned > corrupt` a real signal that an intact encrypted
# object is verified, not just that a corrupt one is caught (with one object, scanned>=1 is implied
# by corrupt>=1 and proves nothing about the healthy path).
put_object intact healthy 100000 ""
corrupt_blob scrub
assert_pass atrest
scanned="$(metric '^cairn_scrub_objects_total')"
corrupt="$(metric '^cairn_scrub_corruption_total')"
[ "${scanned%.*}" -gt "${corrupt%.*}" ] 2>/dev/null \
  || fail "[atrest] no intact encrypted object was counted verified (scanned=$scanned corrupt=$corrupt) — the scrub only proved it can catch a corrupt one"
stop_node
echo "PASS: the scrub detected corruption of a transparently-encrypted blob and verified an intact one"

echo "=== arm 3/4: client SSE-S3 (x-amz-server-side-encryption: AES256) ==="
start_node ssesd "$((PORT + 2))" "$((UIPORT + 2))"
# A plaintext object too, so `scanned` cannot come only from the encrypted one and the corruption
# assertion is unambiguously about the SSE object (only its blob is corrupted).
put_object plain other 1000 ""
put_object scrub obj 100000 AES256
corrupt_blob scrub
assert_pass ssesd
stop_node
echo "PASS: the scrub detected corruption of a client-SSE-S3 blob"

echo "=== arm 4/4: compressed blob + the counted composite-ETag skip ==="
start_node compressed "$((PORT + 3))" "$((UIPORT + 3))"
"$PY" - "$AK" "$SK" "http://127.0.0.1:$ARMPORT" <<'PY'
import sys, boto3
from botocore.config import Config
ak, sk, ep = sys.argv[1], sys.argv[2], sys.argv[3]
s3 = boto3.client("s3", endpoint_url=ep, aws_access_key_id=ak, aws_secret_access_key=sk,
                  region_name="us-east-1", config=Config(s3={"addressing_style": "path"}))
s3.create_bucket(Bucket="scrub")
s3.create_bucket(Bucket="mpart")
# A single-part multipart upload: its ETag is the composite "{md5}-1" form, which is a hash OF
# HASHES and therefore NOT re-hashable from the assembled bytes. The scrub must COUNT that skip.
up = s3.create_multipart_upload(Bucket="mpart", Key="assembled")["UploadId"]
part = s3.upload_part(Bucket="mpart", Key="assembled", UploadId=up, PartNumber=1, Body=b"x" * 4096)
s3.complete_multipart_upload(Bucket="mpart", Key="assembled", UploadId=up,
                             MultipartUpload={"Parts": [{"ETag": part["ETag"], "PartNumber": 1}]})
print("  completed a 1-part multipart upload (composite ETag)")
PY
# Turn on zstd compression for `scrub` (management API, Bearer-authenticated) and PUT a highly
# compressible body so the blob is stored as a CRNB container.
curl -fsS -X PUT -H "Authorization: Bearer $BEARER" -H 'content-type: application/json' \
  -d '{"algorithm":"zstd","block_size":65536}' \
  "http://127.0.0.1:$ARMUIPORT/api/v1/buckets/scrub/compression" >/dev/null \
  || fail "[compressed] could not enable bucket compression"
"$PY" - "$AK" "$SK" "http://127.0.0.1:$ARMPORT" <<'PY'
import sys, boto3
from botocore.config import Config
ak, sk, ep = sys.argv[1], sys.argv[2], sys.argv[3]
s3 = boto3.client("s3", endpoint_url=ep, aws_access_key_id=ak, aws_secret_access_key=sk,
                  region_name="us-east-1", config=Config(s3={"addressing_style": "path"}))
s3.put_object(Bucket="scrub", Key="obj", Body=b"compress me " * 20000, ContentType="text/plain")
head = s3.head_object(Bucket="scrub", Key="obj")
print(f"  put a compressible object ({head['ContentLength']} logical bytes)")
PY
corrupt_blob scrub
assert_pass compressed
composite="$(metric '^cairn_scrub_skipped_total.reason="composite_etag"')"
echo "  [compressed] skipped{composite_etag}=$composite"
ge1 "$composite" || fail "[compressed] the multipart object's un-hashable ETag was not counted as a skip (=$composite) — a silent skip is the defect this harness guards"
stop_node
echo "PASS: the scrub detected corruption of a compressed blob and counted the composite-ETag skip"

echo "PASS: all four scrub arms"
