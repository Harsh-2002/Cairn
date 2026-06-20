#!/usr/bin/env python3
"""STS temporary-credential conformance: mint a scoped, expiring session credential via the
management API and prove a standard S3 SDK consumes it (X-Amz-Security-Token) with exactly the
granted access — scoped GET allowed, ungranted PUT denied, cross-bucket denied, bad token denied.

Args: <sigv4_access_key> <sigv4_secret> <s3_endpoint> <mgmt_endpoint> <bearer_token>
"""

import json
import sys
import urllib.request

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ak, sk, s3_endpoint, mgmt_endpoint, bearer = (
    sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5],
)


def check(label, cond):
    if not cond:
        print(f"FAIL: {label}")
        sys.exit(1)
    print(f"  ok: {label}")


def client(akid, secret, token=None):
    return boto3.client(
        "s3", endpoint_url=s3_endpoint, aws_access_key_id=akid, aws_secret_access_key=secret,
        aws_session_token=token, region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )


# Admin (root SigV4) sets up two buckets and an object.
admin = client(ak, sk)
admin.create_bucket(Bucket="stsb")
admin.put_object(Bucket="stsb", Key="readme.txt", Body=b"hello sts")
admin.create_bucket(Bucket="otherb")

# Mint a session scoped to read-only on stsb/* via the management API (Bearer-authenticated).
policy = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": "s3:GetObject",
        "Resource": "arn:aws:s3:::stsb/*",
    }],
}
req = urllib.request.Request(
    f"{mgmt_endpoint}/api/v1/credentials/temporary",
    data=json.dumps({"duration_secs": 900, "policy": policy}).encode(),
    method="POST",
    headers={"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"},
)
with urllib.request.urlopen(req) as resp:
    minted = json.loads(resp.read())
check("minted temporary credential", minted["access_key_id"].startswith("CAIRNTMP"))
check("session token returned", bool(minted["session_token"]))

sess = client(minted["access_key_id"], minted["secret_access_key"], minted["session_token"])

# Granted: read the scoped object.
body = sess.get_object(Bucket="stsb", Key="readme.txt")["Body"].read()
check("scoped GET allowed", body == b"hello sts")

# Denied: a write the policy does not grant.
try:
    sess.put_object(Bucket="stsb", Key="nope.txt", Body=b"x")
    check("ungranted PUT denied", False)
except ClientError:
    check("ungranted PUT denied", True)

# Denied: a different bucket entirely.
try:
    sess.get_object(Bucket="otherb", Key="z")
    check("cross-bucket GET denied", False)
except ClientError:
    check("cross-bucket GET denied", True)

# Denied: the right access key + secret but a tampered session token (fail-closed token check).
bad = client(minted["access_key_id"], minted["secret_access_key"], "not-the-real-token")
try:
    bad.get_object(Bucket="stsb", Key="readme.txt")
    check("tampered session token denied", False)
except ClientError:
    check("tampered session token denied", True)

# Denied: the temporary access key WITHOUT any session token (a session key needs its token).
notoken = client(minted["access_key_id"], minted["secret_access_key"])
try:
    notoken.get_object(Bucket="stsb", Key="readme.txt")
    check("missing session token denied", False)
except ClientError:
    check("missing session token denied", True)

print("STS OK — scoped temporary credentials minted + enforced via the AWS SDK")
