#!/usr/bin/env bash
# Object Lock / WORM regression (ARCH 16.5): drive a real cairn binary with the boto3 AWS SDK and
# assert the immutability contract across S3 and the management plane: atomic explicit/default
# retention for PUT, Copy, and multipart; source lock state never copied; COMPLIANCE and legal hold
# block every permanent-delete surface (including administrator force/prefix deletion);
# GOVERNANCE yields only to `s3:BypassGovernanceRetention` + the bypass header; enablement and
# required versioning cannot be disabled.
#
# Usage: BIN=target/debug/cairn PY=/path/to/python-with-boto3 conformance/object_lock.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-/tmp/cairnvenv/bin/python}"
PORT="${PORT:-9087}"
WEBPORT="${WEBPORT:-9088}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_WEB_ADDR="127.0.0.1:$WEBPORT"
export CAIRN_MASTER_KEY; CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"

SRV=""
cleanup() {
  if [ -n "$SRV" ]; then
    kill "$SRV" 2>/dev/null || true
    wait "$SRV" 2>/dev/null || true
  fi
  rm -rf "$DATA"
}
trap cleanup EXIT
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

[ -x "$BIN" ] || fail "binary not found or not executable: $BIN"
command -v "$PY" >/dev/null 2>&1 || fail "python interpreter not found: $PY (needs boto3)"
"$PY" -c "import boto3" 2>/dev/null || fail "boto3 not importable by $PY"

BOOT="$("$BIN" bootstrap)" || fail "bootstrap failed"
AK="$(echo "$BOOT" | awk '/Access Key Id/ {print $NF}')"
SK="$(echo "$BOOT" | awk '/Secret Access Key/ {print $NF}')"
BEARER="$(echo "$BOOT" | awk '/Authorization: Bearer/ {print $NF}')"
if [ -z "$AK" ] || [ -z "$SK" ] || [ -z "$BEARER" ]; then
  fail "could not parse bootstrap credentials"
fi

"$BIN" serve >"$DATA/server.log" 2>&1 &
SRV=$!
S3_READY=""
for _ in $(seq 1 100); do
  if curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null; then
    S3_READY=1
    break
  fi
  kill -0 "$SRV" 2>/dev/null || fail "server exited at startup; log: $(cat "$DATA/server.log")"
  sleep 0.1
done
[ -n "$S3_READY" ] || fail "S3 listener did not become ready; log: $(cat "$DATA/server.log")"
CONTROL_READY=""
for _ in $(seq 1 100); do
  if curl -fsS -o /dev/null "http://127.0.0.1:$WEBPORT/api/v1/health" 2>/dev/null; then
    CONTROL_READY=1
    break
  fi
  kill -0 "$SRV" 2>/dev/null || fail "server exited while control listener started; log: $(cat "$DATA/server.log")"
  sleep 0.1
done
[ -n "$CONTROL_READY" ] || fail "control listener did not become ready; log: $(cat "$DATA/server.log")"

"$PY" - "$AK" "$SK" "$BEARER" "http://127.0.0.1:$PORT" "http://127.0.0.1:$WEBPORT" <<'PY'
import datetime
import hashlib
import http.client
import json
import sys
import urllib.parse

import boto3
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import ClientError

ak, sk, bearer, ep, control = sys.argv[1:6]
s3 = boto3.client("s3", endpoint_url=ep, aws_access_key_id=ak, aws_secret_access_key=sk,
                  region_name="us-east-1", config=Config(s3={"addressing_style": "path"},
                                                         retries={"max_attempts": 1}))

def check(label, cond):
    if not cond:
        print(f"FAIL: {label}"); sys.exit(1)
    print(f"  ok: {label}")

def code(exc):
    return exc.response["Error"]["Code"]

creds = Credentials(ak, sk)
data_url = urllib.parse.urlsplit(ep)
control_url = urllib.parse.urlsplit(control)

def raw_s3(method, path, query="", body=b"", headers=None):
    target = path + (f"?{query}" if query else "")
    request = AWSRequest(method=method, url=f"{ep}{target}", data=body)
    request.headers["host"] = data_url.netloc
    request.headers["x-amz-content-sha256"] = hashlib.sha256(body).hexdigest()
    for name, value in (headers or {}).items():
        request.headers[name] = value
    S3SigV4Auth(creds, "s3", "us-east-1").add_auth(request)
    conn = http.client.HTTPConnection(data_url.hostname, data_url.port, timeout=30)
    conn.request(method, target, body=body, headers=dict(request.headers))
    response = conn.getresponse()
    payload = response.read()
    status = response.status
    conn.close()
    return status, payload

def xml_error_code(payload):
    start = payload.find(b"<Code>")
    end = payload.find(b"</Code>", start)
    return "" if start < 0 or end < 0 else payload[start + 6:end].decode()

def control_delete(target):
    conn = http.client.HTTPConnection(control_url.hostname, control_url.port, timeout=30)
    conn.request("DELETE", target, headers={"Authorization": f"Bearer {bearer}"})
    response = conn.getresponse()
    payload = response.read()
    status = response.status
    conn.close()
    return status, payload

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

# Enablement is creation-only and immutable, and a locked bucket can never leave Enabled
# versioning. Raw signed calls cover malformed documents that botocore rejects client-side.
status, payload = raw_s3(
    "PUT", "/false-lock",
    headers={"x-amz-bucket-object-lock-enabled": "false"})
check("create header cannot encode disabled Object Lock",
      status == 400 and xml_error_code(payload) == "InvalidArgument")

for label, body in (
    ("empty", b"<ObjectLockConfiguration/>"),
    ("disabled", b"<ObjectLockConfiguration><ObjectLockEnabled>Disabled</ObjectLockEnabled>"
                 b"</ObjectLockConfiguration>"),
):
    status, payload = raw_s3(
        "PUT", "/wormb", "object-lock", body,
        {"content-type": "application/xml"})
    check(f"{label} Object Lock configuration rejected",
          status == 400 and xml_error_code(payload) == "MalformedXML")

try:
    s3.put_bucket_versioning(
        Bucket="wormb", VersioningConfiguration={"Status": "Suspended"})
    check("locked bucket refuses versioning suspension", False)
except ClientError as exc:
    check("locked bucket refuses versioning suspension as InvalidBucketState",
          code(exc) == "InvalidBucketState")
check("failed suspension leaves versioning Enabled",
      s3.get_bucket_versioning(Bucket="wormb").get("Status") == "Enabled")
check("failed disable leaves Object Lock Enabled",
      s3.get_object_lock_configuration(Bucket="wormb")
        ["ObjectLockConfiguration"]["ObjectLockEnabled"] == "Enabled")

s3.create_bucket(Bucket="late-lock")
s3.put_bucket_versioning(
    Bucket="late-lock", VersioningConfiguration={"Status": "Enabled"})
status, payload = raw_s3(
    "PUT", "/late-lock", "object-lock",
    b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>"
    b"</ObjectLockConfiguration>",
    {"content-type": "application/xml"})
check("Object Lock cannot be enabled after bucket creation",
      status == 409 and xml_error_code(payload) == "InvalidBucketState")
s3.delete_bucket(Bucket="late-lock")

# Default-retention bounds are inclusive and replacement is atomic on validation failure.
def put_default(field, value):
    s3.put_object_lock_configuration(Bucket="wormb", ObjectLockConfiguration={
        "ObjectLockEnabled": "Enabled",
        "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", field: value}}})

for field, maximum in (("Days", 36500), ("Years", 100)):
    put_default(field, maximum)
    cfg = s3.get_object_lock_configuration(Bucket="wormb")["ObjectLockConfiguration"]
    check(f"default retention accepts maximum {field}",
          cfg["Rule"]["DefaultRetention"][field] == maximum)
    for invalid in (0, maximum + 1):
        try:
            put_default(field, invalid)
            check(f"default retention rejects {field}={invalid}", False)
        except ClientError as exc:
            check(f"default retention rejects {field}={invalid} as InvalidArgument",
                  exc.response["Error"]["Code"] == "InvalidArgument")
    cfg = s3.get_object_lock_configuration(Bucket="wormb")["ObjectLockConfiguration"]
    check(f"invalid {field} replacement leaves configuration unchanged",
          cfg["Rule"]["DefaultRetention"][field] == maximum)

# Restore the short default used by the deletion checks below.
put_default("Days", 1)

# Explicit PUT installs the version, tags, retention, and legal hold together. Incomplete headers
# are rejected before an object becomes visible.
s3.create_bucket(Bucket="copyb", ObjectLockEnabledForBucket=True)
status, payload = raw_s3(
    "PUT", "/copyb/malformed", body=b"must-not-commit",
    headers={"x-amz-object-lock-mode": "GOVERNANCE"})
check("incomplete explicit retention is InvalidRequest",
      status == 400 and xml_error_code(payload) == "InvalidRequest")
try:
    s3.get_object(Bucket="copyb", Key="malformed")
    check("malformed explicit retention commits no object", False)
except ClientError as exc:
    check("malformed explicit retention commits no object", code(exc) == "NoSuchKey")

put = s3.put_object(
    Bucket="copyb", Key="source", Body=b"source-bytes",
    Tagging="class=source",
    ObjectLockMode="COMPLIANCE",
    ObjectLockRetainUntilDate=far,
    ObjectLockLegalHoldStatus="ON")
source_version = put["VersionId"]
source_head = s3.head_object(Bucket="copyb", Key="source", VersionId=source_version)
check("explicit PUT retention committed", source_head.get("ObjectLockMode") == "COMPLIANCE")
check("explicit PUT legal hold committed",
      source_head.get("ObjectLockLegalHoldStatus") == "ON")
check("explicit PUT tags committed",
      s3.get_object_tagging(Bucket="copyb", Key="source", VersionId=source_version)["TagSet"]
      == [{"Key": "class", "Value": "source"}])

# Copy carries tags under the COPY directive, but the source's WORM capability is never inherited.
copy = s3.copy_object(
    Bucket="copyb", Key="copied",
    CopySource={"Bucket": "copyb", "Key": "source", "VersionId": source_version},
    TaggingDirective="COPY")
copied_version = copy["VersionId"]
copied_head = s3.head_object(Bucket="copyb", Key="copied", VersionId=copied_version)
check("copy never inherits source retention", copied_head.get("ObjectLockMode") is None)
check("copy never inherits source legal hold",
      copied_head.get("ObjectLockLegalHoldStatus") == "OFF")
check("copy still carries source tags",
      s3.get_object_tagging(Bucket="copyb", Key="copied", VersionId=copied_version)["TagSet"]
      == [{"Key": "class", "Value": "source"}])

explicit_copy = s3.copy_object(
    Bucket="copyb", Key="explicit-copy",
    CopySource={"Bucket": "copyb", "Key": "source", "VersionId": source_version},
    ObjectLockMode="GOVERNANCE",
    ObjectLockRetainUntilDate=far,
    ObjectLockLegalHoldStatus="ON")
explicit_copy_head = s3.head_object(
    Bucket="copyb", Key="explicit-copy", VersionId=explicit_copy["VersionId"])
check("copy applies destination explicit retention",
      explicit_copy_head.get("ObjectLockMode") == "GOVERNANCE")
check("copy applies destination explicit legal hold",
      explicit_copy_head.get("ObjectLockLegalHoldStatus") == "ON")

# Multipart pins initiation tags/legal-hold intent, but resolves the current bucket default only at
# Complete. Change GOVERNANCE -> COMPLIANCE after initiation to make the timing observable.
mpu = s3.create_multipart_upload(
    Bucket="wormb", Key="multipart-default",
    Tagging="class=multipart",
    ObjectLockLegalHoldStatus="ON")
s3.put_object_lock_configuration(Bucket="wormb", ObjectLockConfiguration={
    "ObjectLockEnabled": "Enabled",
    "Rule": {"DefaultRetention": {"Mode": "COMPLIANCE", "Days": 2}}})
part = s3.upload_part(
    Bucket="wormb", Key="multipart-default", UploadId=mpu["UploadId"],
    PartNumber=1, Body=b"one-part")
completed = s3.complete_multipart_upload(
    Bucket="wormb", Key="multipart-default", UploadId=mpu["UploadId"],
    MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part["ETag"]}]})
mpu_head = s3.head_object(
    Bucket="wormb", Key="multipart-default", VersionId=completed["VersionId"])
check("multipart resolves default at completion",
      mpu_head.get("ObjectLockMode") == "COMPLIANCE")
check("multipart pins initiation legal hold",
      mpu_head.get("ObjectLockLegalHoldStatus") == "ON")
check("multipart pins initiation tags",
      s3.get_object_tagging(
          Bucket="wormb", Key="multipart-default", VersionId=completed["VersionId"])["TagSet"]
      == [{"Key": "class", "Value": "multipart"}])

# Restore the short default used by the deletion checks below.
put_default("Days", 1)

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

# Management administrators are deliberately not a WORM bypass. Recursive deletion reports the
# exact protected version while continuing to a later unlocked sibling; force-delete then returns
# 409 and leaves the protected row, lock state, and bytes intact.
s3.create_bucket(Bucket="manageb", ObjectLockEnabledForBucket=True)
protected = s3.put_object(
    Bucket="manageb", Key="folder/a-protected", Body=b"protected-bytes",
    ObjectLockMode="COMPLIANCE", ObjectLockRetainUntilDate=far)
unlocked = s3.put_object(
    Bucket="manageb", Key="folder/z-unlocked", Body=b"unlocked-bytes")

status, payload = control_delete(
    "/api/v1/buckets/manageb/objects?prefix=folder%2F")
check("management prefix delete succeeds partially", status == 200)
result = json.loads(payload)
check("management prefix delete removes unlocked sibling", result.get("deleted") == 1)
check("management prefix delete signals protected remainder", result.get("more") is True)
errors = result.get("errors", [])
check("management prefix delete reports exact protected key/version",
      len(errors) == 1
      and errors[0].get("key") == "folder/a-protected"
      and errors[0].get("version_id") == protected["VersionId"])
try:
    s3.get_object(
        Bucket="manageb", Key="folder/z-unlocked", VersionId=unlocked["VersionId"])
    check("management prefix delete removed unlocked version", False)
except ClientError as exc:
    check("management prefix delete removed unlocked version",
          code(exc) == "NoSuchVersion")
check("management prefix delete preserves protected bytes",
      s3.get_object(
          Bucket="manageb", Key="folder/a-protected",
          VersionId=protected["VersionId"])["Body"].read() == b"protected-bytes")
check("management prefix delete preserves protected lock",
      s3.head_object(
          Bucket="manageb", Key="folder/a-protected",
          VersionId=protected["VersionId"]).get("ObjectLockMode") == "COMPLIANCE")

status, _ = control_delete("/api/v1/buckets/manageb")
check("administrator force-delete cannot bypass Object Lock", status == 409)
check("force-delete preserves protected bytes after conflict",
      s3.get_object(
          Bucket="manageb", Key="folder/a-protected",
          VersionId=protected["VersionId"])["Body"].read() == b"protected-bytes")
check("force-delete preserves protected lock after conflict",
      s3.head_object(
          Bucket="manageb", Key="folder/a-protected",
          VersionId=protected["VersionId"]).get("ObjectLockMode") == "COMPLIANCE")

print("OBJECT LOCK OK — S3 and management WORM contracts hold end-to-end")
PY

echo "PASS: object-lock WORM contract holds end-to-end across S3 + management"
