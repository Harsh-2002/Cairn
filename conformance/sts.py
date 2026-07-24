#!/usr/bin/env python3
"""STS temporary-credential conformance: mint a scoped, expiring session credential via the
management API and prove a standard S3 SDK consumes it (X-Amz-Security-Token) with exactly the
granted access — scoped GET allowed, ungranted PUT denied, cross-bucket denied, bad token denied.

Also the NON-ADMIN least-privilege BOUNDARY (the security property that matters most for STS): a
member who mints a session over the STS WIRE SURFACE, signed with their OWN long-term key, gets a
session bounded by their own rights — it can touch what they can and NOTHING they cannot, never
escalating to the parent's authorization. Every other STS test mints as the root ADMIN; this is the
only leg that proves a member's session cannot exceed the member. Four independent code guards
enforce it (get_session_token uses effective_access_policy for non-admins; authorize classifies any
session AuthenticatedMember ABOVE the owner/admin arm; the authz engine grants a session only from
its own inline policy; attach_policy skips the parent-policy load) — a regression in any ONE would
silently escalate a member's session, and only this leg would catch it.

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
        config=Config(s3={"addressing_style": "path"}, retries={"total_max_attempts": 1, "mode": "standard"}),
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

# ---- Non-admin least-privilege boundary: a MEMBER-minted session must never exceed the member. ----
# Create a member with their own SigV4 creds via the management API (the authz.py pattern).
req = urllib.request.Request(
    f"{mgmt_endpoint}/api/v1/users",
    data=json.dumps({"display_name": "sts-member", "role": "member"}).encode(),
    method="POST",
    headers={"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"},
)
with urllib.request.urlopen(req) as resp:
    member = json.loads(resp.read())
m_ak, m_sk = member["s3_access_key_id"], member["s3_secret_key"]

# The member creates and OWNS a bucket — effective_access_policy synthesizes s3:* over owned buckets —
# and has NO rights to the admin's stsb/otherb.
member_s3 = client(m_ak, m_sk)
member_s3.create_bucket(Bucket="stsmemberbkt")
member_s3.put_object(Bucket="stsmemberbkt", Key="mine.txt", Body=b"member-owned")

# The member mints a session over the STS WIRE SURFACE, signed with the member's OWN long-term key
# (not the admin management API) — exactly how a member SDK/Terraform mints.
member_sts = boto3.client(
    "sts", endpoint_url=s3_endpoint, aws_access_key_id=m_ak, aws_secret_access_key=m_sk,
    region_name="us-east-1", config=Config(retries={"total_max_attempts": 1, "mode": "standard"}),
)
mcreds = member_sts.get_session_token(DurationSeconds=900)["Credentials"]
check("member GetSessionToken minted a CAIRNTMP key", mcreds["AccessKeyId"].startswith("CAIRNTMP"))
msess = client(mcreds["AccessKeyId"], mcreds["SecretAccessKey"], mcreds["SessionToken"])

# CAN do what the member can: read the member's own object (the session inherits the member's rights).
body = msess.get_object(Bucket="stsmemberbkt", Key="mine.txt")["Body"].read()
check("member session reads the member's OWN bucket", body == b"member-owned")

# CANNOT exceed the member: the admin's bucket is off-limits — the member has no rights there, so the
# session has none either. If the session escalated to the parent admin's full-S3, this would pass.
try:
    msess.get_object(Bucket="stsb", Key="readme.txt")
    check("member session CANNOT read the admin's bucket (no escalation to parent)", False)
except ClientError as e:
    check("member session CANNOT read the admin's bucket (no escalation to parent)",
          e.response["Error"]["Code"] == "AccessDenied")

# ...and is NOT full-S3: it cannot write the admin's bucket either.
try:
    msess.put_object(Bucket="otherb", Key="escalate.txt", Body=b"x")
    check("member session is NOT full-S3 (cannot write the admin's bucket)", False)
except ClientError as e:
    check("member session is NOT full-S3 (cannot write the admin's bucket)",
          e.response["Error"]["Code"] == "AccessDenied")

# A member AssumeRole carrying an inline Policy is fail-closed DENIED: there is no subset-proof
# engine, so a non-admin may not hand-supply a session policy (only an admin may).
try:
    member_sts.assume_role(
        RoleArn="arn:aws:iam::000000000000:role/x", RoleSessionName="member-sess",
        Policy=json.dumps({"Version": "2012-10-17",
                           "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}),
    )
    check("member AssumeRole with an inline Policy is DENIED (no non-admin subset proof)", False)
except ClientError as e:
    check("member AssumeRole with an inline Policy is DENIED (no non-admin subset proof)",
          e.response["Error"]["Code"] == "AccessDenied")

print("STS OK — scoped temporary credentials minted + enforced via the AWS SDK; a member-minted "
      "session never exceeds the member")
