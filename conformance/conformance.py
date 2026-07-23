#!/usr/bin/env python3
"""Conformance test: drive a running Cairn server with the boto3 AWS SDK (real SigV4 signing,
and — on modern boto3 — default flexible checksums that use the aws-chunked streaming body,
directly exercising Cairn's streaming chunked decoder, the F-5 fix)."""

import datetime
import io
import sys

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

akid, secret, endpoint = sys.argv[1], sys.argv[2], sys.argv[3]

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=akid,
    aws_secret_access_key=secret,
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}, retries={"total_max_attempts": 1, "mode": "standard"}),
)

def check(label, cond):
    if not cond:
        print(f"FAIL: {label}")
        sys.exit(1)
    print(f"  ok: {label}")

# --- bucket + simple object ---
s3.create_bucket(Bucket="conf")
check("create_bucket", "conf" in [b["Name"] for b in s3.list_buckets()["Buckets"]])

s3.put_object(Bucket="conf", Key="hello.txt", Body=b"hello from boto3", ContentType="text/plain")
body = s3.get_object(Bucket="conf", Key="hello.txt")["Body"].read()
check("put/get roundtrip", body == b"hello from boto3")

h = s3.head_object(Bucket="conf", Key="hello.txt")
check("head content-length", h["ContentLength"] == 16)

r = s3.get_object(Bucket="conf", Key="hello.txt", Range="bytes=0-4")["Body"].read()
check("ranged get", r == b"hello")

lst = s3.list_objects_v2(Bucket="conf", Prefix="hel")
check("list_objects_v2 prefix", any(o["Key"] == "hello.txt" for o in lst.get("Contents", [])))

# --- conditional + copy ---
s3.copy_object(Bucket="conf", Key="copy.txt", CopySource={"Bucket": "conf", "Key": "hello.txt"})
check("copy_object", s3.get_object(Bucket="conf", Key="copy.txt")["Body"].read() == b"hello from boto3")

# --- multipart via the high-level transfer manager (forces a low threshold) ---
big = bytes((i * 2654435761 >> 24) & 0xFF for i in range(6 * 1024 * 1024))
cfg = TransferConfig(multipart_threshold=5 * 1024 * 1024, multipart_chunksize=5 * 1024 * 1024)
s3.upload_fileobj(io.BytesIO(big), "conf", "big.bin", Config=cfg)
got = s3.get_object(Bucket="conf", Key="big.bin")["Body"].read()
check("multipart upload roundtrip (6 MiB)", got == big)

# --- multipart + SSE-S3: the assembled object must be encrypted at rest, not silently plaintext.
# The SSE header is set at initiate; a round-trip through the real blob store only succeeds if the
# assembled blob was actually sealed under the same DEK the descriptor records (a plaintext blob
# could not decrypt), so a correct round-trip + a reported AES256 proves end-to-end encryption.
s3.upload_fileobj(io.BytesIO(big), "conf", "big-sse.bin", Config=cfg,
                  ExtraArgs={"ServerSideEncryption": "AES256"})
sse_head = s3.head_object(Bucket="conf", Key="big-sse.bin")
check("multipart SSE round-trips byte-identical",
      s3.get_object(Bucket="conf", Key="big-sse.bin")["Body"].read() == big)
check("multipart SSE object reports AES256", sse_head.get("ServerSideEncryption") == "AES256")

# --- lifecycle: expiration is accepted; storage-class transition is rejected (not silently no-op'd) ---
s3.put_bucket_lifecycle_configuration(Bucket="conf", LifecycleConfiguration={
    "Rules": [{"ID": "exp", "Status": "Enabled", "Filter": {"Prefix": "tmp/"},
               "Expiration": {"Days": 30}}]})
check("lifecycle expiration accepted", True)
try:
    s3.put_bucket_lifecycle_configuration(Bucket="conf", LifecycleConfiguration={
        "Rules": [{"ID": "tier", "Status": "Enabled", "Filter": {"Prefix": "cold/"},
                   "Transitions": [{"Days": 30, "StorageClass": "GLACIER"}]}]})
    check("lifecycle transition rejected", False)
except ClientError:
    check("lifecycle transition rejected", True)

# --- versioning ---
s3.create_bucket(Bucket="vers")
s3.put_bucket_versioning(Bucket="vers", VersioningConfiguration={"Status": "Enabled"})
s3.put_object(Bucket="vers", Key="v", Body=b"one")
s3.put_object(Bucket="vers", Key="v", Body=b"two")
versions = s3.list_object_versions(Bucket="vers").get("Versions", [])
check("versioning keeps 2 versions", len(versions) == 2)
check("latest is newest", s3.get_object(Bucket="vers", Key="v")["Body"].read() == b"two")

# --- object tagging ---
s3.put_object_tagging(
    Bucket="conf", Key="hello.txt",
    Tagging={"TagSet": [{"Key": "env", "Value": "prod"}]},
)
tags = {t["Key"]: t["Value"] for t in s3.get_object_tagging(Bucket="conf", Key="hello.txt")["TagSet"]}
check("object tagging", tags.get("env") == "prod")

# --- Object Lock / WORM / retention / legal hold ---
# A bucket created with object lock is forced to versioning Enabled and reports its config.
s3.create_bucket(Bucket="lockb", ObjectLockEnabledForBucket=True)
check("object-lock bucket is versioned",
      s3.get_bucket_versioning(Bucket="lockb").get("Status") == "Enabled")
olc = s3.get_object_lock_configuration(Bucket="lockb")["ObjectLockConfiguration"]
check("object-lock enabled", olc.get("ObjectLockEnabled") == "Enabled")

far_future = datetime.datetime(2099, 1, 1, tzinfo=datetime.timezone.utc)

# COMPLIANCE retention is immutable until it expires — not even the bypass header lifts it.
s3.put_object(Bucket="lockb", Key="locked", Body=b"immutable")
lvid = s3.head_object(Bucket="lockb", Key="locked")["VersionId"]
s3.put_object_retention(Bucket="lockb", Key="locked", VersionId=lvid,
                        Retention={"Mode": "COMPLIANCE", "RetainUntilDate": far_future})
r = s3.get_object_retention(Bucket="lockb", Key="locked", VersionId=lvid)["Retention"]
check("compliance retention set", r["Mode"] == "COMPLIANCE")
# HEAD echoes the lock headers.
hl = s3.head_object(Bucket="lockb", Key="locked")
check("HEAD echoes lock mode", hl.get("ObjectLockMode") == "COMPLIANCE")
try:
    s3.delete_object(Bucket="lockb", Key="locked", VersionId=lvid)
    check("compliance blocks version delete", False)
except ClientError:
    check("compliance blocks version delete", True)
try:
    s3.delete_object(Bucket="lockb", Key="locked", VersionId=lvid,
                     BypassGovernanceRetention=True)
    check("compliance ignores bypass header", False)
except ClientError:
    check("compliance ignores bypass header", True)

# Legal hold blocks a permanent delete regardless of retention, and releasing it re-enables it.
s3.put_object(Bucket="lockb", Key="held", Body=b"x")
hvid = s3.head_object(Bucket="lockb", Key="held")["VersionId"]
s3.put_object_legal_hold(Bucket="lockb", Key="held", VersionId=hvid,
                         LegalHold={"Status": "ON"})
check("legal hold on",
      s3.get_object_legal_hold(Bucket="lockb", Key="held",
                               VersionId=hvid)["LegalHold"]["Status"] == "ON")
try:
    s3.delete_object(Bucket="lockb", Key="held", VersionId=hvid)
    check("legal hold blocks delete", False)
except ClientError:
    check("legal hold blocks delete", True)
s3.put_object_legal_hold(Bucket="lockb", Key="held", VersionId=hvid,
                         LegalHold={"Status": "OFF"})
s3.delete_object(Bucket="lockb", Key="held", VersionId=hvid)
check("delete after legal-hold release", True)

# GOVERNANCE retention blocks a delete, but the bypass header (with permission) lifts it.
s3.put_object(Bucket="lockb", Key="gov", Body=b"g")
gvid = s3.head_object(Bucket="lockb", Key="gov")["VersionId"]
s3.put_object_retention(Bucket="lockb", Key="gov", VersionId=gvid,
                        Retention={"Mode": "GOVERNANCE", "RetainUntilDate": far_future})
try:
    s3.delete_object(Bucket="lockb", Key="gov", VersionId=gvid)
    check("governance blocks without bypass", False)
except ClientError:
    check("governance blocks without bypass", True)
s3.delete_object(Bucket="lockb", Key="gov", VersionId=gvid, BypassGovernanceRetention=True)
check("governance delete with bypass", True)

# --- bulk + single delete ---
s3.delete_objects(Bucket="conf", Delete={"Objects": [
    {"Key": "hello.txt"}, {"Key": "copy.txt"}, {"Key": "big-sse.bin"}]})
s3.delete_object(Bucket="conf", Key="big.bin")
remaining = s3.list_objects_v2(Bucket="conf").get("KeyCount", 0)
check("bulk + single delete cleared bucket", remaining == 0)

print("CONFORMANCE OK — boto3 drove Cairn through the full object lifecycle")
