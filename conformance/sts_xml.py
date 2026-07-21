#!/usr/bin/env python3
"""AWS-STS wire-surface conformance (ARCH 14): point a standard boto3 **STS** client at the S3 data
plane and prove it mints temporary credentials via the XML surface (GetSessionToken + AssumeRole),
then use those credentials through a normal S3 client. This is the SDK/Terraform interop the wire
surface exists for — distinct from `sts.py`, which mints via the management JSON API.

Args: <sigv4_access_key> <sigv4_secret> <s3_endpoint>
"""

import sys

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ak, sk, s3_endpoint = sys.argv[1], sys.argv[2], sys.argv[3]


def check(label, cond):
    if not cond:
        print(f"FAIL: {label}")
        sys.exit(1)
    print(f"  ok: {label}")


def s3_client(akid, secret, token=None):
    return boto3.client(
        "s3", endpoint_url=s3_endpoint, aws_access_key_id=akid, aws_secret_access_key=secret,
        aws_session_token=token, region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )


# The STS client is pointed at the SAME endpoint as S3 (Cairn serves STS on the S3 port), exactly as
# an operator sets `sts_endpoint == s3_endpoint` for the SDK / Terraform's assume_role{}.
sts = boto3.client(
    "sts", endpoint_url=s3_endpoint, aws_access_key_id=ak, aws_secret_access_key=sk,
    region_name="us-east-1", config=Config(retries={"max_attempts": 1}),
)

# ---- GetSessionToken: mint via the XML surface, then use the creds through S3. ----
gst = sts.get_session_token(DurationSeconds=900)["Credentials"]
check("GetSessionToken minted a CAIRNTMP key", gst["AccessKeyId"].startswith("CAIRNTMP"))
check("GetSessionToken returned a secret", bool(gst["SecretAccessKey"]))
check("GetSessionToken returned a session token", bool(gst["SessionToken"]))
check("GetSessionToken returned an expiration", bool(gst["Expiration"]))

# The root key is an administrator, so the session carries full-S3 (but never the admin
# short-circuit); exercise a create/put/get round-trip with the minted creds.
sess = s3_client(gst["AccessKeyId"], gst["SecretAccessKey"], gst["SessionToken"])
sess.create_bucket(Bucket="stsx")
sess.put_object(Bucket="stsx", Key="hello.txt", Body=b"via STS XML")
body = sess.get_object(Bucket="stsx", Key="hello.txt")["Body"].read()
check("GetSessionToken creds work through the S3 client", body == b"via STS XML")

# ---- AssumeRole: the Terraform assume_role{} path. RoleArn/RoleSessionName are audit-only. ----
ar = sts.assume_role(
    RoleArn="arn:aws:iam::000000000000:role/deployer", RoleSessionName="conf-session",
)
creds = ar["Credentials"]
check("AssumeRole minted a CAIRNTMP key", creds["AccessKeyId"].startswith("CAIRNTMP"))
check("AssumeRole echoed AssumedRoleUser.Arn", "assumed-role/deployer" in ar["AssumedRoleUser"]["Arn"])
sess2 = s3_client(creds["AccessKeyId"], creds["SecretAccessKey"], creds["SessionToken"])
body2 = sess2.get_object(Bucket="stsx", Key="hello.txt")["Body"].read()
check("AssumeRole creds work through the S3 client", body2 == b"via STS XML")

# ---- Negative: an out-of-range duration is rejected as InvalidParameterValue (400). ----
try:
    sts.get_session_token(DurationSeconds=100)
    check("out-of-range duration rejected", False)
except ClientError as e:
    check(
        "out-of-range duration rejected",
        e.response["Error"]["Code"] in ("InvalidParameterValue", "ValidationError"),
    )

print("STS-XML OK — SDK STS client minted + consumed temporary credentials via the wire surface")
