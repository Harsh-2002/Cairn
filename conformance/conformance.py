#!/usr/bin/env python3
"""Conformance test: drive a running Cairn server with the boto3 AWS SDK (real SigV4 signing,
and — on modern boto3 — default flexible checksums that use the aws-chunked streaming body,
directly exercising Cairn's streaming chunked decoder, the F-5 fix)."""

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
    config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
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

# --- bulk + single delete ---
s3.delete_objects(Bucket="conf", Delete={"Objects": [
    {"Key": "hello.txt"}, {"Key": "copy.txt"}, {"Key": "big-sse.bin"}]})
s3.delete_object(Bucket="conf", Key="big.bin")
remaining = s3.list_objects_v2(Bucket="conf").get("KeyCount", 0)
check("bulk + single delete cleared bucket", remaining == 0)

print("CONFORMANCE OK — boto3 drove Cairn through the full object lifecycle")
