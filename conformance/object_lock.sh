#!/usr/bin/env bash
# Object Lock / WORM regression (ARCH 16.5): drive a real cairn binary with the boto3 AWS SDK and
# assert the immutability contract — COMPLIANCE retention cannot be deleted or weakened (even with
# the bypass header), GOVERNANCE yields only to `s3:BypassGovernanceRetention` + the bypass header,
# a legal hold blocks regardless and releases on demand, and a bucket default retention is stamped
# onto new versions and surfaced on HEAD.
#
# Usage: BIN=target/debug/cairn PY=/path/to/python-with-boto3 conformance/object_lock.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-/tmp/cairnvenv/bin/python}"
PORT="${PORT:-9087}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_UI_ADDR=off
export CAIRN_MASTER_KEY; CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"

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
[ -n "$AK" ] && [ -n "$SK" ] || fail "could not parse bootstrap credentials"

"$BIN" serve >"$DATA/server.log" 2>&1 &
SRV=$!
for _ in $(seq 1 100); do
  curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && break
  kill -0 "$SRV" 2>/dev/null || fail "server exited at startup; log: $(cat "$DATA/server.log")"
  sleep 0.1
done

"$PY" - "$AK" "$SK" "http://127.0.0.1:$PORT" <<'PY'
import sys, datetime, boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ak, sk, ep = sys.argv[1], sys.argv[2], sys.argv[3]
s3 = boto3.client("s3", endpoint_url=ep, aws_access_key_id=ak, aws_secret_access_key=sk,
                  region_name="us-east-1", config=Config(s3={"addressing_style": "path"},
                                                         retries={"max_attempts": 1}))

def check(label, cond):
    if not cond:
        print(f"FAIL: {label}"); sys.exit(1)
    print(f"  ok: {label}")

far = datetime.datetime(2099, 1, 1, tzinfo=datetime.timezone.utc)

# Create with object lock -> forced versioning + a default GOVERNANCE retention of 1 day.
s3.create_bucket(Bucket="wormb", ObjectLockEnabledForBucket=True)
check("forced versioning", s3.get_bucket_versioning(Bucket="wormb").get("Status") == "Enabled")
s3.put_object_lock_configuration(Bucket="wormb", ObjectLockConfiguration={
    "ObjectLockEnabled": "Enabled",
    "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 1}}})
cfg = s3.get_object_lock_configuration(Bucket="wormb")["ObjectLockConfiguration"]
check("default retention persisted",
      cfg["Rule"]["DefaultRetention"]["Mode"] == "GOVERNANCE")

# A plain PUT inherits the default retention; HEAD echoes it.
s3.put_object(Bucket="wormb", Key="def", Body=b"data")
hd = s3.head_object(Bucket="wormb", Key="def")
check("default retention stamped + echoed", hd.get("ObjectLockMode") == "GOVERNANCE")

# COMPLIANCE is immutable: no delete, no bypass, no shortening.
s3.put_object(Bucket="wormb", Key="c", Body=b"x")
cv = s3.head_object(Bucket="wormb", Key="c")["VersionId"]
s3.put_object_retention(Bucket="wormb", Key="c", VersionId=cv,
                        Retention={"Mode": "COMPLIANCE", "RetainUntilDate": far})
try:
    s3.delete_object(Bucket="wormb", Key="c", VersionId=cv, BypassGovernanceRetention=True)
    check("compliance immutable even with bypass", False)
except ClientError:
    check("compliance immutable even with bypass", True)
try:
    s3.put_object_retention(Bucket="wormb", Key="c", VersionId=cv,
        Retention={"Mode": "COMPLIANCE",
                   "RetainUntilDate": datetime.datetime(2030, 1, 1, tzinfo=datetime.timezone.utc)})
    check("compliance cannot be shortened", False)
except ClientError:
    check("compliance cannot be shortened", True)

# Legal hold blocks then releases.
s3.put_object(Bucket="wormb", Key="h", Body=b"x")
hv = s3.head_object(Bucket="wormb", Key="h")["VersionId"]
s3.put_object_legal_hold(Bucket="wormb", Key="h", VersionId=hv, LegalHold={"Status": "ON"})
try:
    s3.delete_object(Bucket="wormb", Key="h", VersionId=hv)
    check("legal hold blocks delete", False)
except ClientError:
    check("legal hold blocks delete", True)
s3.put_object_legal_hold(Bucket="wormb", Key="h", VersionId=hv, LegalHold={"Status": "OFF"})
# "h" also inherited the bucket's default GOVERNANCE retention (1 day), so releasing the legal hold
# alone is not enough to delete it — governance must be bypassed too (matches S3 semantics).
s3.delete_object(Bucket="wormb", Key="h", VersionId=hv, BypassGovernanceRetention=True)
check("delete after legal-hold release", True)

# GOVERNANCE yields to the bypass.
s3.put_object(Bucket="wormb", Key="g", Body=b"x")
gv = s3.head_object(Bucket="wormb", Key="g")["VersionId"]
s3.put_object_retention(Bucket="wormb", Key="g", VersionId=gv,
                        Retention={"Mode": "GOVERNANCE", "RetainUntilDate": far})
try:
    s3.delete_object(Bucket="wormb", Key="g", VersionId=gv)
    check("governance blocks without bypass", False)
except ClientError:
    check("governance blocks without bypass", True)
s3.delete_object(Bucket="wormb", Key="g", VersionId=gv, BypassGovernanceRetention=True)
check("governance delete with bypass", True)

print("OBJECT LOCK OK — WORM retention + legal hold enforced via the AWS SDK")
PY

echo "PASS: object-lock WORM contract holds end-to-end"
