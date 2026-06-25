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

if failures:
    print(f"\n{len(failures)} assertion(s) failed")
    sys.exit(1)
print("\nall checksum round-trip assertions passed")
