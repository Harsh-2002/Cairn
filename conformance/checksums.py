#!/usr/bin/env python3
"""Driver for conformance/checksums.sh — modern-SDK flexible-checksum round-trip (ARCH 21.1).

Asserts Cairn echoes the stored `x-amz-checksum-{algo}` (and `x-amz-checksum-type`) so an SDK with
default data-integrity protection (boto3 >=1.36, aws-cli v2, JS/Go/Java v2) can verify the transfer:
  * PUT echoes the computed checksum;
  * GET / HEAD echo it iff `x-amz-checksum-mode: ENABLED` (never unprompted);
  * a Range GET never echoes the whole-object digest;
  * CRC32 + SHA256 always; CRC32C + CRC64NVME additionally when the CRT extra is installed.

Usage: checksums.py <access-key> <secret-key> <endpoint-url>
"""
import sys
import boto3
from botocore.config import Config

ak, sk, ep = sys.argv[1], sys.argv[2], sys.argv[3]

s3 = boto3.client(
    "s3", endpoint_url=ep, aws_access_key_id=ak, aws_secret_access_key=sk,
    region_name="us-east-1", config=Config(s3={"addressing_style": "path"}),
)

# A second client with response validation OFF: boto3's default `when_supported` silently adds
# `x-amz-checksum-mode: ENABLED` to every GET, so a genuinely mode-less GET needs `when_required`.
s3_plain = boto3.client(
    "s3", endpoint_url=ep, aws_access_key_id=ak, aws_secret_access_key=sk,
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}, response_checksum_validation="when_required"),
)

# Which algorithms this boto3 install can actually emit (CRC32C/CRC64NVME need botocore[crt]).
try:
    import awscrt  # noqa: F401
    HAVE_CRT = True
except ImportError:
    HAVE_CRT = False

ALGOS = ["CRC32", "SHA256"] + (["CRC32C", "CRC64NVME"] if HAVE_CRT else [])
HDR = {"CRC32": "x-amz-checksum-crc32", "CRC32C": "x-amz-checksum-crc32c",
       "CRC64NVME": "x-amz-checksum-crc64nvme", "SHA1": "x-amz-checksum-sha1",
       "SHA256": "x-amz-checksum-sha256"}

failures = []

def check(cond, msg):
    if cond:
        print(f"  ok: {msg}")
    else:
        print(f"  FAIL: {msg}")
        failures.append(msg)

def echoed(resp):
    h = resp.get("ResponseMetadata", {}).get("HTTPHeaders", {})
    return {k: v for k, v in h.items() if k.startswith("x-amz-checksum")}

BODY = b"the quick brown fox jumps over the lazy dog" * 23
s3.create_bucket(Bucket="checks")
print(f"CRT extra installed: {HAVE_CRT}; testing algorithms: {ALGOS}")

for algo in ALGOS:
    print(f"== {algo} ==")
    key = f"obj-{algo.lower()}"
    put = s3.put_object(Bucket="checks", Key=key, Body=BODY, ChecksumAlgorithm=algo)
    pe = echoed(put)
    check(HDR[algo] in pe, f"PUT echoes {HDR[algo]}")
    check(pe.get("x-amz-checksum-type") == "FULL_OBJECT", "PUT echoes x-amz-checksum-type FULL_OBJECT")
    put_val = pe.get(HDR[algo])

    get = s3.get_object(Bucket="checks", Key=key, ChecksumMode="ENABLED")
    body = get["Body"].read()
    check(body == BODY, f"{algo} body round-trips byte-identical")
    ge = echoed(get)
    check(ge.get(HDR[algo]) == put_val, f"GET (mode=ENABLED) echoes the same {HDR[algo]}")

    # The SDK validated the download against the echoed checksum: boto3 surfaces the parsed value.
    parsed = {k: v for k, v in get.items() if k.startswith("Checksum")}
    check(any(k.upper().endswith(algo) for k in parsed), f"boto3 parsed a Checksum{algo} on GET")

    plain = s3_plain.get_object(Bucket="checks", Key=key)
    check(HDR[algo] not in echoed(plain), "GET without mode does NOT leak the checksum")

    head = s3.head_object(Bucket="checks", Key=key, ChecksumMode="ENABLED")
    check(echoed(head).get(HDR[algo]) == put_val, f"HEAD (mode=ENABLED) echoes the {HDR[algo]}")

# Default PUT (no explicit algorithm): the SDK adds one automatically and Cairn must echo it.
print("== default (SDK-chosen) ==")
s3.put_object(Bucket="checks", Key="default", Body=BODY)
dg = s3.get_object(Bucket="checks", Key="default", ChecksumMode="ENABLED")
check(bool(echoed(dg)), "default-integrity object echoes a checksum on GET (mode=ENABLED)")

# A Range GET must not echo the whole-object checksum.
print("== range ==")
rg = s3.get_object(Bucket="checks", Key="default", Range="bytes=0-9", ChecksumMode="ENABLED")
check(rg["Body"].read() == BODY[:10], "range GET returns the requested slice")
check(not echoed(rg), "range GET does NOT echo a whole-object checksum")

# Composite multipart checksums (Phase 1). A CRC32 multipart upload composes a COMPOSITE
# checksum-of-checksums (a `-N`-suffixed object value); a CRC64NVME upload composes a whole-object
# FULL_OBJECT value (no suffix). boto3/botocore[crt] itself validates the composite on the GET.
PART = b"m" * (5 * 1024 * 1024)   # the non-final part must be >= 5 MiB
TAIL = b"the-multipart-tail"
FULL = PART + TAIL

print("== multipart CRC32 (COMPOSITE) ==")
mp = s3.create_multipart_upload(
    Bucket="checks", Key="mpu-crc32", ChecksumAlgorithm="CRC32", ChecksumType="COMPOSITE")
uid = mp["UploadId"]
p1 = s3.upload_part(Bucket="checks", Key="mpu-crc32", UploadId=uid, PartNumber=1,
                    Body=PART, ChecksumAlgorithm="CRC32")
p2 = s3.upload_part(Bucket="checks", Key="mpu-crc32", UploadId=uid, PartNumber=2,
                    Body=TAIL, ChecksumAlgorithm="CRC32")
check("ChecksumCRC32" in p1, "upload_part echoes the per-part CRC32")
parts = [
    {"PartNumber": 1, "ETag": p1["ETag"], "ChecksumCRC32": p1["ChecksumCRC32"]},
    {"PartNumber": 2, "ETag": p2["ETag"], "ChecksumCRC32": p2["ChecksumCRC32"]},
]
comp = s3.complete_multipart_upload(
    Bucket="checks", Key="mpu-crc32", UploadId=uid,
    MultipartUpload={"Parts": parts}, ChecksumType="COMPOSITE")
check(comp.get("ChecksumCRC32", "").endswith("-2"), "complete returns a COMPOSITE CRC32 (-2 suffix)")
check(comp.get("ChecksumType") == "COMPOSITE", "complete reports ChecksumType COMPOSITE")
# The SDK validates the download against the echoed composite checksum.
gm = s3.get_object(Bucket="checks", Key="mpu-crc32", ChecksumMode="ENABLED")
check(gm["Body"].read() == FULL, "multipart CRC32 body round-trips byte-identical")
check(gm.get("ChecksumType") == "COMPOSITE", "GET (mode=ENABLED) reports COMPOSITE type")
attrs = s3.get_object_attributes(
    Bucket="checks", Key="mpu-crc32", ObjectAttributes=["Checksum", "ObjectSize"])
check(attrs.get("Checksum", {}).get("ChecksumType") == "COMPOSITE",
      "GetObjectAttributes reports ChecksumType COMPOSITE")

if HAVE_CRT:
    print("== multipart CRC64NVME (FULL_OBJECT) ==")
    mp = s3.create_multipart_upload(
        Bucket="checks", Key="mpu-crc64", ChecksumAlgorithm="CRC64NVME", ChecksumType="FULL_OBJECT")
    uid = mp["UploadId"]
    q1 = s3.upload_part(Bucket="checks", Key="mpu-crc64", UploadId=uid, PartNumber=1,
                        Body=PART, ChecksumAlgorithm="CRC64NVME")
    q2 = s3.upload_part(Bucket="checks", Key="mpu-crc64", UploadId=uid, PartNumber=2,
                        Body=TAIL, ChecksumAlgorithm="CRC64NVME")
    qparts = [
        {"PartNumber": 1, "ETag": q1["ETag"], "ChecksumCRC64NVME": q1["ChecksumCRC64NVME"]},
        {"PartNumber": 2, "ETag": q2["ETag"], "ChecksumCRC64NVME": q2["ChecksumCRC64NVME"]},
    ]
    fc = s3.complete_multipart_upload(
        Bucket="checks", Key="mpu-crc64", UploadId=uid,
        MultipartUpload={"Parts": qparts}, ChecksumType="FULL_OBJECT")
    check(fc.get("ChecksumType") == "FULL_OBJECT", "complete reports ChecksumType FULL_OBJECT")
    check("-" not in fc.get("ChecksumCRC64NVME", ""),
          "a FULL_OBJECT CRC64NVME carries no -N suffix")
    # The whole-object value must equal the same bytes uploaded as a single-part PUT.
    s3.put_object(Bucket="checks", Key="single-crc64", Body=FULL, ChecksumAlgorithm="CRC64NVME")
    sp = s3.get_object(Bucket="checks", Key="single-crc64", ChecksumMode="ENABLED")
    check(fc.get("ChecksumCRC64NVME") == sp.get("ChecksumCRC64NVME"),
          "multipart FULL_OBJECT CRC64NVME equals the single-part whole-object value")

if failures:
    print(f"\n{len(failures)} assertion(s) failed")
    sys.exit(1)
print("\nall checksum round-trip assertions passed")
