#!/usr/bin/env python3
"""Driver for conformance/encryption.sh — server-side-encryption (SSE) object-body conformance
(ARCH 27), driven through a REAL AWS SDK (boto3): genuine SigV4 + the hyper adapter + SDK-side header
decode. The rest of the SSE test surface is in-process against hand-built request structs; this is
the only harness that proves the wire contract end to end AND inspects the committed ciphertext on
disk (the node is local, so the harness can read + tamper blobs under the data dir).

Two phases, one per server-config leg (the launcher restarts the server between them):

  main   (leg 1: CAIRN_KMS_KEY_IDS allow-list set, at-rest OFF) — cases a, c, d, e, f, g
  atrest (leg 2: CAIRN_ENCRYPT_AT_REST=1)                       — case b

  (a) SSE-KMS + SSE-S3 wire contract (PUT/GET/HEAD echo, byte-exact GET)
  (b) transparent at-rest: advertises nothing, stored ciphertext on disk
  (c) mandatory-SSE: a header-less client PUT is refused 400
  (d) bucket-default silent encryption (AES256 and aws:kms defaults)
  (e) multipart + SSE incl. UploadPartCopy (staged parts are ciphertext on disk)
  (f) a tampered committed blob fails closed (GET errors, never plaintext)
  (g) cross-policy CopyObject (upgrade to kms default; downgrade to plaintext default)

Usage: encryption.py <phase> <ak> <sk> <s3-endpoint> <data-dir> <key-id> <web-endpoint>
"""
import http.client
import json
import os
import sys
import urllib.parse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

PHASE = sys.argv[1]
AK, SK, EP, DATA_DIR = sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
KEY_ID = sys.argv[6] if len(sys.argv) > 6 else ""
WEB_EP = sys.argv[7] if len(sys.argv) > 7 else ""

s3 = boto3.client(
    "s3", endpoint_url=EP, aws_access_key_id=AK, aws_secret_access_key=SK,
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}, retries={"total_max_attempts": 1, "mode": "standard"}),
)

fails = []
def check(label, cond):
    print(("  ok: " if cond else "FAIL: ") + label)
    if not cond:
        fails.append(label)
    return cond

# The trailer of an encrypted CRNB block container: 34 bytes, magic `CRNB` then a version byte;
# VERSION_ENCRYPTED == 2 (crates/cairn-blob/src/compress.rs). We assert the on-disk bytes carry this
# trailer AND do not contain the known plaintext run — encryption is real, not advertised-only.
VERSION_ENCRYPTED = 2
MARKER = b"PLAINTEXT-MARKER-DO-NOT-FIND-ON-DISK-"

def hh(resp):
    return resp.get("ResponseMetadata", {}).get("HTTPHeaders", {})

def committed_blobs(bucket):
    """Opaque-id blob files committed under $DATA/<bucket>/ (never named by key)."""
    d = os.path.join(DATA_DIR, bucket)
    return [os.path.join(d, f) for f in sorted(os.listdir(d))] if os.path.isdir(d) else []

def staged_parts(upload_id):
    d = os.path.join(DATA_DIR, ".staging", "multipart", upload_id)
    return [os.path.join(d, f) for f in sorted(os.listdir(d))] if os.path.isdir(d) else []

def trailer_encrypted(blob):
    return len(blob) >= 34 and blob[-34:-30] == b"CRNB" and blob[-30] == VERSION_ENCRYPTED

def assert_encrypted_on_disk(path, label):
    with open(path, "rb") as fh:
        blob = fh.read()
    check(f"{label}: on-disk blob is a VERSION_ENCRYPTED CRNB container", trailer_encrypted(blob))
    check(f"{label}: known plaintext marker is ABSENT from the stored bytes", MARKER not in blob)

def mgmt(method, path, body=None):
    """A management-API call on the web-console listener, authenticated with the bootstrap Bearer token."""
    u = urllib.parse.urlparse(WEB_EP)
    c = http.client.HTTPConnection(u.hostname, u.port, timeout=30)
    payload = json.dumps(body).encode() if body is not None else b""
    c.request(method, path, body=payload,
              headers={"authorization": f"Bearer {AK}.{SK}", "content-type": "application/json"})
    r = c.getresponse(); d = r.read(); c.close()
    return r.status, d

PART = b"m" * (5 * 1024 * 1024)   # a non-final multipart part must be >= 5 MiB
TAIL = b"the-multipart-tail-bytes"


# ============================================================ (a) SSE-KMS + SSE-S3 wire contract
def case_a():
    print("\n--- (a) SSE-KMS + SSE-S3 wire contract (real SigV4, SDK header decode) ---")
    s3.create_bucket(Bucket="kmswire")
    body = MARKER * 300
    put = s3.put_object(Bucket="kmswire", Key="obj", Body=body,
                        ServerSideEncryption="aws:kms", SSEKMSKeyId=KEY_ID, BucketKeyEnabled=True)
    check("PUT echoes ServerSideEncryption=aws:kms", put.get("ServerSideEncryption") == "aws:kms")
    check("PUT echoes SSEKMSKeyId exactly", put.get("SSEKMSKeyId") == KEY_ID)
    check("PUT echoes BucketKeyEnabled=True", put.get("BucketKeyEnabled") is True)

    get = s3.get_object(Bucket="kmswire", Key="obj")
    check("GET body is byte-exact", get["Body"].read() == body)
    check("GET echoes ServerSideEncryption=aws:kms", get.get("ServerSideEncryption") == "aws:kms")
    check("GET echoes SSEKMSKeyId exactly", get.get("SSEKMSKeyId") == KEY_ID)
    check("GET echoes BucketKeyEnabled=True", get.get("BucketKeyEnabled") is True)

    head = s3.head_object(Bucket="kmswire", Key="obj")
    check("HEAD echoes ServerSideEncryption=aws:kms", head.get("ServerSideEncryption") == "aws:kms")
    check("HEAD echoes SSEKMSKeyId exactly", head.get("SSEKMSKeyId") == KEY_ID)
    check("HEAD echoes BucketKeyEnabled=True", head.get("BucketKeyEnabled") is True)

    # plain SSE-S3 (AES256) round-trip + echo
    aes_body = MARKER * 120
    ap = s3.put_object(Bucket="kmswire", Key="aes", Body=aes_body, ServerSideEncryption="AES256")
    check("AES256 PUT echoes ServerSideEncryption=AES256", ap.get("ServerSideEncryption") == "AES256")
    ag = s3.get_object(Bucket="kmswire", Key="aes")
    check("AES256 GET body is byte-exact", ag["Body"].read() == aes_body)
    check("AES256 GET echoes ServerSideEncryption=AES256", ag.get("ServerSideEncryption") == "AES256")


# ============================================================ (b) transparent at-rest (leg 2)
def case_b():
    print("\n--- (b) transparent at-rest: advertises nothing, ciphertext on disk (leg 2) ---")
    s3.create_bucket(Bucket="atrest")
    body = MARKER * 200
    s3.put_object(Bucket="atrest", Key="silent", Body=body)   # NO SSE header
    get = s3.get_object(Bucket="atrest", Key="silent")
    check("at-rest GET body is byte-exact", get["Body"].read() == body)
    check("at-rest GET advertises NO ServerSideEncryption", get.get("ServerSideEncryption") is None)
    head = s3.head_object(Bucket="atrest", Key="silent")
    check("at-rest HEAD advertises NO ServerSideEncryption", head.get("ServerSideEncryption") is None)
    blobs = committed_blobs("atrest")
    check("at-rest: exactly one committed blob on disk", len(blobs) == 1)
    if blobs:
        assert_encrypted_on_disk(blobs[0], "at-rest")


# ============================================================ (c) mandatory-SSE 403/400
def case_c():
    print("\n--- (c) mandatory-SSE: a header-less client PUT is refused ---")
    s3.create_bucket(Bucket="mandatory")
    # The `required` flag is a management-plane control (the S3 ?encryption surface has no such field);
    # set it via PUT /api/v1/buckets/<name>/encryption {"algorithm":"none","required":true}.
    st, d = mgmt("PUT", "/api/v1/buckets/mandatory/encryption",
                 {"algorithm": "none", "required": True})
    check("management API set required:true (204)", st == 204)
    refused = False
    try:
        s3.put_object(Bucket="mandatory", Key="plain", Body=b"no sse header")
    except ClientError as e:
        status = e.response["ResponseMetadata"]["HTTPStatusCode"]
        code = e.response["Error"]["Code"]
        refused = status == 400 and code == "InvalidRequest"
        print(f"    (rejected with HTTP {status} {code})")
    check("header-less PUT to a required-SSE bucket is refused 400 InvalidRequest", refused)
    # An explicit-SSE PUT to the same bucket still succeeds.
    ok_put = s3.put_object(Bucket="mandatory", Key="ok", Body=MARKER * 50,
                           ServerSideEncryption="AES256")
    check("an explicit SSE PUT to the required-SSE bucket succeeds",
          ok_put.get("ServerSideEncryption") == "AES256")

    # SECURITY (Phase-2 audit): the mandatory-SSE control must hold on EVERY create path, not only a
    # plain PUT. A header-less MULTIPART upload and a header-less COPY must also be refused — before
    # the fix they silently stored PLAINTEXT in the required bucket.
    def absent(bucket, key):
        try:
            s3.head_object(Bucket=bucket, Key=key)
            return False
        except ClientError:
            return True
    mp_refused = False
    try:
        up = s3.create_multipart_upload(Bucket="mandatory", Key="mpk")["UploadId"]
        pp = s3.upload_part(Bucket="mandatory", Key="mpk", UploadId=up, PartNumber=1, Body=b"x" * 16)
        s3.complete_multipart_upload(Bucket="mandatory", Key="mpk", UploadId=up,
                                     MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": pp["ETag"]}]})
    except ClientError as e:
        mp_refused = e.response["Error"]["Code"] == "InvalidRequest"
    check("header-less MULTIPART complete to a required-SSE bucket is refused", mp_refused)
    check("no plaintext multipart object landed in the required-SSE bucket", absent("mandatory", "mpk"))
    cp_refused = False
    try:
        s3.copy_object(Bucket="mandatory", Key="cpk",
                       CopySource={"Bucket": "mandatory", "Key": "ok"})
    except ClientError as e:
        cp_refused = e.response["Error"]["Code"] == "InvalidRequest"
    check("header-less COPY into a required-SSE bucket is refused", cp_refused)
    check("no plaintext copied object landed in the required-SSE bucket", absent("mandatory", "cpk"))
    # WITH an SSE header, multipart still completes — the control blocks only non-compliant writes.
    up2 = s3.create_multipart_upload(Bucket="mandatory", Key="mpok",
                                     ServerSideEncryption="AES256")["UploadId"]
    pp2 = s3.upload_part(Bucket="mandatory", Key="mpok", UploadId=up2, PartNumber=1, Body=b"y" * 16)
    s3.complete_multipart_upload(Bucket="mandatory", Key="mpok", UploadId=up2,
                                 MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": pp2["ETag"]}]})
    check("an explicit-SSE multipart complete to the required-SSE bucket succeeds",
          not absent("mandatory", "mpok"))


# ============================================================ (d) bucket-default silent encryption
def case_d():
    print("\n--- (d) bucket-default silent encryption (AES256 and aws:kms) ---")
    # AES256 default via the S3 ?encryption surface.
    s3.create_bucket(Bucket="defaes")
    s3.put_bucket_encryption(
        Bucket="defaes",
        ServerSideEncryptionConfiguration={
            "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]})
    body = MARKER * 200
    s3.put_object(Bucket="defaes", Key="obj", Body=body)   # NO SSE header
    g = s3.get_object(Bucket="defaes", Key="obj")
    check("AES256-default GET body is byte-exact", g["Body"].read() == body)
    check("AES256-default GET advertises AES256", g.get("ServerSideEncryption") == "AES256")
    blobs = committed_blobs("defaes")
    check("AES256-default: one committed blob on disk", len(blobs) == 1)
    if blobs:
        assert_encrypted_on_disk(blobs[0], "AES256-default")

    # aws:kms default (with a named key id from the allow-list).
    s3.create_bucket(Bucket="defkms")
    s3.put_bucket_encryption(
        Bucket="defkms",
        ServerSideEncryptionConfiguration={
            "Rules": [{"ApplyServerSideEncryptionByDefault": {
                "SSEAlgorithm": "aws:kms", "KMSMasterKeyID": KEY_ID}}]})
    kbody = MARKER * 210
    s3.put_object(Bucket="defkms", Key="obj", Body=kbody)   # NO SSE header
    gk = s3.get_object(Bucket="defkms", Key="obj")
    check("kms-default GET body is byte-exact", gk["Body"].read() == kbody)
    check("kms-default GET advertises aws:kms", gk.get("ServerSideEncryption") == "aws:kms")
    check("kms-default GET echoes the default key id", gk.get("SSEKMSKeyId") == KEY_ID)
    kblobs = committed_blobs("defkms")
    check("kms-default: one committed blob on disk", len(kblobs) == 1)
    assert_encrypted_on_disk(kblobs[0], "kms-default")


# ============================================================ (e) multipart + SSE incl. copy
def multipart_sse(bucket, key, sse, key_id=None):
    kw = {"ServerSideEncryption": sse}
    if key_id:
        kw["SSEKMSKeyId"] = key_id
    up = s3.create_multipart_upload(Bucket=bucket, Key=key, **kw)
    uid = up["UploadId"]
    body1 = MARKER * (len(PART) // len(MARKER) + 1)   # >= 5 MiB, marker-rich
    p1 = s3.upload_part(Bucket=bucket, Key=key, UploadId=uid, PartNumber=1, Body=body1)
    # a mixed UploadPartCopy for the final part, from an encrypted source object
    s3.put_object(Bucket=bucket, Key=key + "-src", Body=MARKER * 40, ServerSideEncryption="AES256")
    cp = s3.upload_part_copy(Bucket=bucket, Key=key, UploadId=uid, PartNumber=2,
                             CopySource={"Bucket": bucket, "Key": key + "-src"})
    parts = staged_parts(uid)
    check(f"{sse} multipart: two staged part files exist pre-complete", len(parts) == 2)
    for i, pth in enumerate(parts, 1):
        with open(pth, "rb") as fh:
            blob = fh.read()
        check(f"{sse} multipart: staged part {i} is a VERSION_ENCRYPTED CRNB container",
              trailer_encrypted(blob))
        check(f"{sse} multipart: staged part {i} has no plaintext marker on disk", MARKER not in blob)
    comp = s3.complete_multipart_upload(
        Bucket=bucket, Key=key, UploadId=uid,
        MultipartUpload={"Parts": [
            {"PartNumber": 1, "ETag": p1["ETag"]},
            {"PartNumber": 2, "ETag": cp["CopyPartResult"]["ETag"]}]})
    check(f"{sse} multipart: complete advertises {sse}", comp.get("ServerSideEncryption") == sse)
    if key_id:
        check(f"{sse} multipart: complete echoes the key id", comp.get("SSEKMSKeyId") == key_id)
    head = s3.head_object(Bucket=bucket, Key=key)
    check(f"{sse} multipart: HEAD advertises {sse}", head.get("ServerSideEncryption") == sse)
    check(f"{sse} multipart: ETag is multipart-shaped (-2)", head["ETag"].rstrip('"').endswith("-2"))
    got = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    check(f"{sse} multipart: GET assembled body is byte-exact", got == body1 + (MARKER * 40))

def case_e():
    print("\n--- (e) multipart + SSE incl. UploadPartCopy (AES256 and aws:kms) ---")
    s3.create_bucket(Bucket="mpsse")
    multipart_sse("mpsse", "aes-big", "AES256")
    multipart_sse("mpsse", "kms-big", "aws:kms", KEY_ID)


# ============================================================ (f) tampered blob fails closed
def case_f():
    print("\n--- (f) a tampered committed blob fails closed (GET errors, never plaintext) ---")
    s3.create_bucket(Bucket="tamper")
    body = MARKER * 200
    s3.put_object(Bucket="tamper", Key="obj", Body=body, ServerSideEncryption="AES256")
    blobs = committed_blobs("tamper")
    if not check("tamper: located exactly one committed blob", len(blobs) == 1):
        return
    path = blobs[0]
    with open(path, "rb") as fh:
        raw = bytearray(fh.read())
    # flip one byte in the ciphertext body (before the 34-byte trailer) so GCM authentication fails
    off = max(0, (len(raw) - 34) // 2)
    raw[off] ^= 0xFF
    with open(path, "wb") as fh:
        fh.write(raw)
    print(f"    flipped one ciphertext byte at offset {off} of {os.path.basename(path)}")
    errored = False
    returned = b""
    try:
        r = s3.get_object(Bucket="tamper", Key="obj")
        returned = r["Body"].read()   # GCM tag mismatch surfaces here for a streamed body
    except Exception as e:            # noqa: BLE001 — any failure is a pass; never a served body
        errored = True
        print(f"    (GET raised {type(e).__name__})")
    check("tamper: GET of the corrupted SSE blob ERRORS (fails closed)", errored)
    check("tamper: no plaintext / marker ever returned", MARKER not in returned and returned != body)


# ============================================================ (g) cross-policy copy
def case_g():
    print("\n--- (g) cross-policy CopyObject (upgrade to kms default; downgrade to plaintext) ---")
    s3.create_bucket(Bucket="srcbkt")
    src_body = MARKER * 220
    s3.put_object(Bucket="srcbkt", Key="src", Body=src_body, ServerSideEncryption="AES256")

    # upgrade: SSE-S3 source -> aws:kms-default destination advertises aws:kms
    s3.create_bucket(Bucket="upkms")
    s3.put_bucket_encryption(
        Bucket="upkms",
        ServerSideEncryptionConfiguration={
            "Rules": [{"ApplyServerSideEncryptionByDefault": {
                "SSEAlgorithm": "aws:kms", "KMSMasterKeyID": KEY_ID}}]})
    s3.copy_object(Bucket="upkms", Key="dst", CopySource={"Bucket": "srcbkt", "Key": "src"})
    gu = s3.get_object(Bucket="upkms", Key="dst")
    check("copy upgrade: dest advertises aws:kms", gu.get("ServerSideEncryption") == "aws:kms")
    check("copy upgrade: dest echoes the default key id", gu.get("SSEKMSKeyId") == KEY_ID)
    check("copy upgrade: dest body is byte-exact (source bytes preserved)",
          gu["Body"].read() == src_body)

    # downgrade: encrypted source -> plaintext (no-default) destination round-trips byte-exact
    s3.create_bucket(Bucket="plaindst")
    s3.copy_object(Bucket="plaindst", Key="dst", CopySource={"Bucket": "srcbkt", "Key": "src"})
    gd = s3.get_object(Bucket="plaindst", Key="dst")
    check("copy downgrade: dest advertises NO ServerSideEncryption",
          gd.get("ServerSideEncryption") is None)
    check("copy downgrade: dest body is byte-exact (source bytes preserved)",
          gd["Body"].read() == src_body)


def main():
    if PHASE == "main":
        case_a(); case_c(); case_d(); case_e(); case_f(); case_g()
    elif PHASE == "atrest":
        case_b()
    else:
        print(f"unknown phase {PHASE!r}"); sys.exit(2)
    print("\n==== RESULT:", "OK" if not fails else f"{len(fails)} FAILURE(S): {fails}", "====")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
