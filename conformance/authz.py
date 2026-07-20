#!/usr/bin/env python3
"""Auth, tenancy, and credential conformance — every assertion pins the EXACT S3 error code AND
HTTP status, never merely "a ClientError was raised".

Five boundaries, in order:

  1. TENANT ISOLATION. A second, non-admin identity is minted through the management API. Its
     ListBuckets must return exactly its own buckets — the owner filter in `list_buckets` is a
     single expression (`(role != Administrator).then(|| user_id)`, service.rs), so a regression
     there silently enumerates the whole node to every tenant. Cross-tenant GET/PUT/DELETE/LIST
     are AccessDenied 403.
  2. ACCESSDENIED BEFORE NOSUCHKEY. Asking for a NON-EXISTENT key in someone else's bucket must be
     403, not 404: a 404 answers "that key is not there", which is an existence oracle. The control
     case (the owner asking for the same missing key) must still be 404 NoSuchKey, so the 403 above
     is a real authorization decision and not a blanket status.
  3. SIGV4 FAILURE MODES. Bad signature / unknown key / skewed date, each with its own code. Built
     as hand-signed raw requests: boto3 will not emit a deliberately wrong signature or a two-hour
     stale X-Amz-Date.
  4. PRESIGNED URLS. Redeemed with NO credentials at all, so the URL itself is the bearer token —
     which makes expiry the one and only revocation mechanism. Minted with ExpiresIn=1 and POLLED
     until it stops working (never slept past), then pinned to its code.
  5. ANONYMOUS ACCESS. Denied by default; allowed exactly as far as a public bucket policy grants
     (GetObject yes, ListBucket/PutObject no); shut off again by Block Public Access.

Args: <root_sigv4_access_key> <root_sigv4_secret> <s3_endpoint> <mgmt_endpoint> <root_bearer>
Env:  STRICT_GAPS=1 turns a documented S3-spec deviation into a hard failure.
"""

import datetime
import hashlib
import hmac
import http.client
import json
import os
import sys
import time
import urllib.parse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

root_ak, root_sk, endpoint, mgmt, root_bearer = sys.argv[1:6]

REGION = "us-east-1"
STRICT_GAPS = os.environ.get("STRICT_GAPS") == "1"

_ep = urllib.parse.urlsplit(endpoint)
S3_HOST, S3_PORT = _ep.hostname, _ep.port
_mg = urllib.parse.urlsplit(mgmt)

GAPS = []


def check(label, cond):
    if not cond:
        print(f"FAIL: {label}")
        sys.exit(1)
    print(f"  ok: {label}")


def gap(label, cond, spec, observed, evidence):
    """A check whose expectation is S3-CORRECT but which Cairn does not currently satisfy.

    The assertion is NOT weakened — `cond` is the correct one. A deviation is reported loudly and
    collected for the final summary; STRICT_GAPS=1 makes it exit non-zero. This exists so the
    harness stays usable as a gate for the other ~50 checks while still stating, in code, what S3
    actually requires.
    """
    if cond:
        print(f"  ok: {label}")
        return
    GAPS.append((label, spec, observed, evidence))
    print(f"  KNOWN GAP: {label}\n      S3 requires: {spec}\n      Cairn does:  {observed}"
          f"\n      evidence:    {evidence}")


def code_of(e):
    return e.response["Error"]["Code"]


def status_of(e):
    return e.response["ResponseMetadata"]["HTTPStatusCode"]


def client(ak, sk):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        region_name=REGION,
        # `signature_version="s3v4"` is REQUIRED for the presigned section: without it botocore
        # mints a legacy SigV2 `AWSAccessKeyId=…&Signature=…` URL, which Cairn (SigV4-only, ARCH
        # 14.1) correctly treats as an unsigned/anonymous request — so the presigned checks would
        # be testing anonymous denial rather than presigned redemption.
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                      retries={"max_attempts": 1}),
    )


def denied(label, op, code="AccessDenied", status=403):
    """Run `op`, which MUST raise a ClientError carrying exactly `code`/`status`."""
    try:
        op()
        check(f"{label} -> {code} {status}", False)
    except ClientError as e:
        ok = code_of(e) == code and status_of(e) == status
        if not ok:
            print(f"      (got {code_of(e)} {status_of(e)})")
        check(f"{label} -> {code} {status}", ok)


# --- raw, hand-signed SigV4 -----------------------------------------------------------------------
# Full control over the date and the signature bytes: botocore will not sign with a stale
# X-Amz-Date, and it will not hand back a deliberately corrupted signature.
def _sha(b):
    return hashlib.sha256(b).hexdigest()


def _hmac(k, m):
    return hmac.new(k, m.encode(), hashlib.sha256).digest()


def sign(method, path, ak, sk, body=b"", when=None, query=None, corrupt_sig=False):
    """Return the signed header dict for a request. `when` overrides the signing timestamp."""
    query = query or {}
    now = when or datetime.datetime.now(datetime.timezone.utc)
    amz = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    ph = _sha(body)
    h = {"host": f"{S3_HOST}:{S3_PORT}", "x-amz-date": amz, "x-amz-content-sha256": ph}
    cq = "&".join(
        f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}"
        for k, v in sorted(query.items())
    )
    signed = sorted(h)
    ch = "".join(f"{k}:{h[k].strip()}\n" for k in signed)
    sh = ";".join(signed)
    cr = "\n".join([method, urllib.parse.quote(path, safe="/"), cq, ch, sh, ph])
    scope = f"{day}/{REGION}/s3/aws4_request"
    sts = "\n".join(["AWS4-HMAC-SHA256", amz, scope, _sha(cr.encode())])
    kd = _hmac(("AWS4" + sk).encode(), day)
    kr = hmac.new(kd, REGION.encode(), hashlib.sha256).digest()
    ks = hmac.new(kr, b"s3", hashlib.sha256).digest()
    ksig = hmac.new(ks, b"aws4_request", hashlib.sha256).digest()
    sig = hmac.new(ksig, sts.encode(), hashlib.sha256).hexdigest()
    if corrupt_sig:
        # Flip exactly one hex nibble: same length, same shape, wrong signature — so the request
        # fails signature VERIFICATION rather than being rejected as malformed.
        sig = sig[:-1] + ("0" if sig[-1] != "0" else "1")
    h["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, SignedHeaders={sh}, Signature={sig}"
    )
    return h


def raw(method, path, headers=None, body=b"", query=None, host=None, port=None):
    """Send a request exactly as given (no implicit auth). Returns (status, body_bytes)."""
    qs = "&".join(
        f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}"
        for k, v in sorted((query or {}).items())
    )
    conn = http.client.HTTPConnection(host or S3_HOST, port or S3_PORT, timeout=30)
    conn.request(method, path + (f"?{qs}" if qs else ""), body=body, headers=headers or {})
    r = conn.getresponse()
    data = r.read()
    conn.close()
    return r.status, data


def get_url(url):
    """GET an absolute URL with NO credentials attached. Returns (status, body_bytes)."""
    u = urllib.parse.urlsplit(url)
    conn = http.client.HTTPConnection(u.hostname, u.port, timeout=30)
    conn.request("GET", u.path + (f"?{u.query}" if u.query else ""), headers={"host": u.netloc})
    r = conn.getresponse()
    data = r.read()
    conn.close()
    return r.status, data


def put_url(url, payload):
    u = urllib.parse.urlsplit(url)
    conn = http.client.HTTPConnection(u.hostname, u.port, timeout=30)
    conn.request("PUT", u.path + (f"?{u.query}" if u.query else ""), body=payload,
                 headers={"host": u.netloc, "content-length": str(len(payload))})
    r = conn.getresponse()
    data = r.read()
    conn.close()
    return r.status, data


def err_code(body):
    """Pull <Code> out of an S3 XML error document."""
    if b"<Code>" not in body:
        return None
    return body.split(b"<Code>", 1)[1].split(b"</Code>", 1)[0].decode()


# --- fixtures: two tenants ------------------------------------------------------------------------
root = client(root_ak, root_sk)

conn = http.client.HTTPConnection(_mg.hostname, _mg.port, timeout=30)
conn.request(
    "POST",
    "/api/v1/users",
    body=json.dumps({"display_name": "authz-alice", "role": "member"}).encode(),
    headers={"authorization": f"Bearer {root_bearer}", "content-type": "application/json"},
)
r = conn.getresponse()
created = json.loads(r.read())
check("POST /api/v1/users minted a second identity", r.status == 201)
conn.close()
alice_ak, alice_sk = created["s3_access_key_id"], created["s3_secret_key"]
alice_bearer = f"{created['bearer_access_key_id']}.{created['bearer_secret']}"
alice = client(alice_ak, alice_sk)

root.create_bucket(Bucket="authz-root")
root.put_object(Bucket="authz-root", Key="secret.txt", Body=b"root-only", ContentType="text/plain")
alice.create_bucket(Bucket="authz-alice")
alice.put_object(Bucket="authz-alice", Key="mine.txt", Body=b"alice-only")
check("both tenants have a bucket + object",
      root.get_object(Bucket="authz-root", Key="secret.txt")["Body"].read() == b"root-only"
      and alice.get_object(Bucket="authz-alice", Key="mine.txt")["Body"].read() == b"alice-only")

# =================================================================================================
# 1. TENANT ISOLATION
# =================================================================================================
print("\n-- 1. tenant isolation --")

# The regression the audit flagged: `list_buckets` filters by owner ONLY for a non-administrator.
# Assert set EQUALITY, not "does not contain" — a partial leak is still a leak.
alice_names = {b["Name"] for b in alice.list_buckets()["Buckets"]}
check("member ListBuckets returns EXACTLY its own buckets", alice_names == {"authz-alice"})
check("member ListBuckets does not enumerate the other tenant", "authz-root" not in alice_names)
root_names = {b["Name"] for b in root.list_buckets()["Buckets"]}
check("administrator ListBuckets still sees every bucket",
      {"authz-root", "authz-alice"} <= root_names)

# The ListBuckets response also carries an <Owner>; it must be the CALLER, not the node's root.
raw_list = raw("GET", "/", headers=sign("GET", "/", alice_ak, alice_sk))
check("member ListBuckets is 200", raw_list[0] == 200)
check("member ListBuckets body names only its own bucket",
      b"authz-alice" in raw_list[1] and b"authz-root" not in raw_list[1])

denied("member GET of another tenant's object",
       lambda: alice.get_object(Bucket="authz-root", Key="secret.txt"))
denied("member PUT into another tenant's bucket",
       lambda: alice.put_object(Bucket="authz-root", Key="evil.txt", Body=b"pwn"))
denied("member DELETE of another tenant's object",
       lambda: alice.delete_object(Bucket="authz-root", Key="secret.txt"))
denied("member LIST of another tenant's bucket",
       lambda: alice.list_objects_v2(Bucket="authz-root"))
denied("member DeleteBucket of another tenant's bucket",
       lambda: alice.delete_bucket(Bucket="authz-root"))
denied("member GetBucketPolicy on another tenant's bucket",
       lambda: alice.get_bucket_policy(Bucket="authz-root"))
denied("member PutBucketPolicy on another tenant's bucket",
       lambda: alice.put_bucket_policy(
           Bucket="authz-root",
           Policy=json.dumps({"Version": "2012-10-17", "Statement": [{
               "Effect": "Allow", "Principal": "*", "Action": "s3:GetObject",
               "Resource": "arn:aws:s3:::authz-root/*"}]})))
denied("member PutObjectAcl on another tenant's object",
       lambda: alice.put_object_acl(Bucket="authz-root", Key="secret.txt", ACL="public-read"))
# HEAD carries no body, so botocore surfaces the bare status as the code; assert the status.
try:
    alice.head_object(Bucket="authz-root", Key="secret.txt")
    check("member HEAD of another tenant's object -> 403", False)
except ClientError as e:
    check("member HEAD of another tenant's object -> 403", status_of(e) == 403)

# Nothing the member attempted may have landed.
after = root.get_object(Bucket="authz-root", Key="secret.txt")
check("victim object still intact after every cross-tenant attempt",
      after["Body"].read() == b"root-only")
root_keys = [o["Key"] for o in root.list_objects_v2(Bucket="authz-root").get("Contents", [])]
check("victim bucket holds exactly its own key", root_keys == ["secret.txt"])
check("victim bucket still exists", root.head_bucket(Bucket="authz-root") is not None)

# The management API is the other tenancy surface: a member must not be able to mint users.
st, _ = raw("POST", "/api/v1/users", host=_mg.hostname, port=_mg.port,
            body=json.dumps({"display_name": "escalate", "role": "administrator"}).encode(),
            headers={"authorization": f"Bearer {alice_bearer}", "content-type": "application/json"})
check("member cannot create users through the management API -> 403", st == 403)

# =================================================================================================
# 2. ACCESSDENIED TAKES PRECEDENCE OVER NOSUCHKEY
# =================================================================================================
print("\n-- 2. AccessDenied before NoSuchKey (no existence oracle) --")

denied("member GET of a NON-EXISTENT key in another tenant's bucket",
       lambda: alice.get_object(Bucket="authz-root", Key="does-not-exist-ffff"))
denied("member DELETE of a NON-EXISTENT key in another tenant's bucket",
       lambda: alice.delete_object(Bucket="authz-root", Key="does-not-exist-ffff"))
try:
    alice.head_object(Bucket="authz-root", Key="does-not-exist-ffff")
    check("member HEAD of a NON-EXISTENT key in another tenant's bucket -> 403", False)
except ClientError as e:
    check("member HEAD of a NON-EXISTENT key in another tenant's bucket -> 403",
          status_of(e) == 403)

# The two responses must be INDISTINGUISHABLE: if the missing-key 403 differed in any way from the
# present-key 403, the difference is itself the oracle. Compare the raw wire bytes minus the
# per-request id.
def scrub(body):
    out = body
    for tag in (b"RequestId", b"HostId"):
        while b"<" + tag + b">" in out:
            head, rest = out.split(b"<" + tag + b">", 1)
            out = head + rest.split(b"</" + tag + b">", 1)[1]
    return out


st_present, b_present = raw("GET", "/authz-root/secret.txt",
                            headers=sign("GET", "/authz-root/secret.txt", alice_ak, alice_sk))
st_absent, b_absent = raw("GET", "/authz-root/does-not-exist-ffff",
                          headers=sign("GET", "/authz-root/does-not-exist-ffff",
                                       alice_ak, alice_sk))
check("cross-tenant GET is 403 for both a present and an absent key",
      st_present == 403 and st_absent == 403)
check("both denials carry the AccessDenied code",
      err_code(b_present) == "AccessDenied" and err_code(b_absent) == "AccessDenied")
# The <Resource> legitimately differs (it echoes the requested path); everything else must match.
check("the two denials are otherwise byte-identical (no existence oracle)",
      scrub(b_present).replace(b"secret.txt", b"X").replace(b"does-not-exist-ffff", b"X")
      == scrub(b_absent).replace(b"secret.txt", b"X").replace(b"does-not-exist-ffff", b"X"))

# Control: the OWNER asking for the same missing key must get a real 404 — proving the 403 above is
# an authorization decision, not a blanket status for everything.
denied("owner GET of a missing key in its OWN bucket",
       lambda: root.get_object(Bucket="authz-root", Key="does-not-exist-ffff"),
       code="NoSuchKey", status=404)
denied("GET of a bucket that exists nowhere",
       lambda: alice.get_object(Bucket="authz-nonexistent-bucket", Key="k"),
       code="NoSuchBucket", status=404)

# =================================================================================================
# 3. SIGV4 FAILURE MODES
# =================================================================================================
print("\n-- 3. SigV4 failure modes --")

# Control: the same hand-rolled signer must produce a request Cairn ACCEPTS, otherwise every
# negative below could be passing for the wrong reason.
st, body = raw("GET", "/authz-root/secret.txt",
               headers=sign("GET", "/authz-root/secret.txt", root_ak, root_sk))
check("hand-signed control request is accepted (200)", st == 200 and body == b"root-only")

st, body = raw("GET", "/authz-root/secret.txt",
               headers=sign("GET", "/authz-root/secret.txt", root_ak, root_sk, corrupt_sig=True))
check("corrupted signature -> SignatureDoesNotMatch 403",
      st == 403 and err_code(body) == "SignatureDoesNotMatch")

# A valid signature computed with the WRONG SECRET is the realistic form of the same failure.
st, body = raw("GET", "/authz-root/secret.txt",
               headers=sign("GET", "/authz-root/secret.txt", root_ak, "not-the-real-secret"))
check("signature computed with the wrong secret -> SignatureDoesNotMatch 403",
      st == 403 and err_code(body) == "SignatureDoesNotMatch")

# An unknown access key must NOT be reported as a signature mismatch (and vice versa) — the two
# codes tell a client which of the pair to fix.
st, body = raw("GET", "/authz-root/secret.txt",
               headers=sign("GET", "/authz-root/secret.txt",
                            "CAIRNAAAAAAAAAAAAAAAAAAAAAAAAAAAA", root_sk))
check("unknown access key id -> InvalidAccessKeyId 403",
      st == 403 and err_code(body) == "InvalidAccessKeyId")

# A DEACTIVATED key is the revocation path for a long-lived credential: it must stop working.
conn = http.client.HTTPConnection(_mg.hostname, _mg.port, timeout=30)
conn.request("PATCH", f"/api/v1/users/{created['id']}", body=json.dumps({"is_active": False}).encode(),
             headers={"authorization": f"Bearer {root_bearer}", "content-type": "application/json"})
r = conn.getresponse()
deact_ok = r.status == 200
r.read()
conn.close()
check("member deactivated via the management API", deact_ok)
st, body = raw("GET", "/authz-alice/mine.txt",
               headers=sign("GET", "/authz-alice/mine.txt", alice_ak, alice_sk))
check("a deactivated user's own key stops working -> InvalidAccessKeyId 403",
      st == 403 and err_code(body) == "InvalidAccessKeyId")
conn = http.client.HTTPConnection(_mg.hostname, _mg.port, timeout=30)
conn.request("PATCH", f"/api/v1/users/{created['id']}", body=json.dumps({"is_active": True}).encode(),
             headers={"authorization": f"Bearer {root_bearer}", "content-type": "application/json"})
r = conn.getresponse()
r.read()
conn.close()
# Poll rather than sleep: the auth cache is invalidated by an epoch bump, so the key comes back
# as soon as the reactivation commits.
for _ in range(100):
    st, _ = raw("GET", "/authz-alice/mine.txt",
                headers=sign("GET", "/authz-alice/mine.txt", alice_ak, alice_sk))
    if st == 200:
        break
    time.sleep(0.05)
check("reactivating the user restores its key", st == 200)

# Clock skew. AWS S3 answers RequestTimeTooSkewed / 403 (documented in the S3 error reference);
# a client library keys its clock-resync retry off exactly that code.
skewed = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=2)
st, body = raw("GET", "/authz-root/secret.txt",
               headers=sign("GET", "/authz-root/secret.txt", root_ak, root_sk, when=skewed))
check("a two-hour-stale X-Amz-Date is REJECTED (not accepted)", st != 200)
check("skewed X-Amz-Date -> RequestTimeTooSkewed 403",
      st == 403 and err_code(body) == "RequestTimeTooSkewed")

# A skew in the FUTURE is the same class and must be rejected identically.
future = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)
st_f, body_f = raw("GET", "/authz-root/secret.txt",
                   headers=sign("GET", "/authz-root/secret.txt", root_ak, root_sk, when=future))
check("a two-hour-future X-Amz-Date is rejected the same way as a stale one",
      st_f != 200 and (st_f, err_code(body_f)) == (st, err_code(body)))

# No credentials at all on a private object is anonymous, not malformed (covered in §5), but a
# GARBLED Authorization header must be a clean 4xx and never a 5xx or a success.
st, body = raw("GET", "/authz-root/secret.txt",
               headers={"host": f"{S3_HOST}:{S3_PORT}", "authorization": "AWS4-HMAC-SHA256 junk"})
check("malformed Authorization header -> 4xx, never 200/5xx", 400 <= st < 500)

# =================================================================================================
# 4. PRESIGNED URLS
# =================================================================================================
print("\n-- 4. presigned URLs --")

url = root.generate_presigned_url("get_object",
                                  Params={"Bucket": "authz-root", "Key": "secret.txt"},
                                  ExpiresIn=300)
st, body = get_url(url)
check("presigned GET redeems with NO credentials attached", st == 200 and body == b"root-only")

put = root.generate_presigned_url("put_object",
                                  Params={"Bucket": "authz-root", "Key": "presigned.txt"},
                                  ExpiresIn=300)
st, body = put_url(put, b"via-presigned-put")
check("presigned PUT redeems with NO credentials attached", st == 200)
check("presigned PUT actually stored the bytes",
      root.get_object(Bucket="authz-root", Key="presigned.txt")["Body"].read()
      == b"via-presigned-put")

# Tampering with a presigned URL must fail signature verification, not fall through to anonymous.
bad = url.replace("X-Amz-Signature=", "X-Amz-Signature=0", 1)[:-1]
st, body = get_url(bad)
check("presigned GET with a tampered signature -> SignatureDoesNotMatch 403",
      st == 403 and err_code(body) == "SignatureDoesNotMatch")

# A presigned URL is a credential, so it carries the SIGNER's authority and no more: the member's
# presigned URL for the other tenant's object must still be denied by authorization.
alice_url = alice.generate_presigned_url("get_object",
                                         Params={"Bucket": "authz-root", "Key": "secret.txt"},
                                         ExpiresIn=300)
st, body = get_url(alice_url)
check("a member's presigned URL does NOT bypass authorization -> AccessDenied 403",
      st == 403 and err_code(body) == "AccessDenied")

# Expiry — the ONLY revocation mechanism a presigned URL has. Mint a 1-second URL, prove it works
# at least once, then POLL until it stops working (a bare sleep past the deadline would not prove
# the URL was live to begin with, and would be timing-flaky).
short = root.generate_presigned_url("get_object",
                                    Params={"Bucket": "authz-root", "Key": "secret.txt"},
                                    ExpiresIn=1)
st, body = get_url(short)
check("a 1-second presigned URL is live when minted", st == 200 and body == b"root-only")
deadline = time.monotonic() + 20
final = None
while time.monotonic() < deadline:
    st, body = get_url(short)
    if st != 200:
        final = (st, err_code(body))
        break
    time.sleep(0.1)
check("the 1-second presigned URL stopped working within the poll window", final is not None)
# AWS answers an expired presigned URL with 403 AccessDenied ("Request has expired").
check("expired presigned URL -> AccessDenied 403", final == (403, "AccessDenied"))
# And it must stay dead — not flap back to 200 on a later attempt.
st, _ = get_url(short)
check("expired presigned URL stays rejected on a retry", st == 403)

# X-Amz-Expires past the 7-day maximum must not mint an eternal credential.
long_url = root.generate_presigned_url("get_object",
                                       Params={"Bucket": "authz-root", "Key": "secret.txt"},
                                       ExpiresIn=300)
over = long_url.replace("X-Amz-Expires=300", "X-Amz-Expires=99999999")
st, body = get_url(over)
check("rewriting X-Amz-Expires invalidates the signature -> SignatureDoesNotMatch 403",
      st == 403 and err_code(body) == "SignatureDoesNotMatch")

# =================================================================================================
# 5. ANONYMOUS ACCESS AND PUBLIC BUCKET POLICY
# =================================================================================================
print("\n-- 5. anonymous access --")

st, body = raw("GET", "/authz-root/secret.txt", headers={"host": f"{S3_HOST}:{S3_PORT}"})
check("anonymous GET of a private object -> AccessDenied 403",
      st == 403 and err_code(body) == "AccessDenied")
st, body = raw("PUT", "/authz-root/anon.txt", headers={"host": f"{S3_HOST}:{S3_PORT}"}, body=b"x")
check("anonymous PUT -> AccessDenied 403", st == 403 and err_code(body) == "AccessDenied")
st, body = raw("DELETE", "/authz-root/secret.txt", headers={"host": f"{S3_HOST}:{S3_PORT}"})
check("anonymous DELETE -> AccessDenied 403", st == 403 and err_code(body) == "AccessDenied")
st, body = raw("GET", "/authz-root", headers={"host": f"{S3_HOST}:{S3_PORT}"})
check("anonymous LIST of a private bucket -> AccessDenied 403",
      st == 403 and err_code(body) == "AccessDenied")
# ListBuckets has no bucket to carry a policy, so anonymous can never reach it.
st, body = raw("GET", "/", headers={"host": f"{S3_HOST}:{S3_PORT}"})
check("anonymous ListBuckets -> AccessDenied 403", st == 403 and err_code(body) == "AccessDenied")
check("anonymous ListBuckets leaked no bucket names",
      b"authz-root" not in body and b"authz-alice" not in body)

# The public path. Cairn implements bucket policies with `Principal: "*"` (cairn-authz
# `principal_matches` / `PrincipalSpec::Any`) gated by Block Public Access, both of which default
# to all-false — so a public policy is honoured until BPA is turned on.
root.create_bucket(Bucket="authz-public")
root.put_object(Bucket="authz-public", Key="open.txt", Body=b"public-bytes")
root.put_object(Bucket="authz-public", Key="hidden.txt", Body=b"still-listed-only-by-owner")
root.put_bucket_policy(Bucket="authz-public", Policy=json.dumps({
    "Version": "2012-10-17",
    "Statement": [{"Sid": "public-read", "Effect": "Allow", "Principal": "*",
                   "Action": "s3:GetObject", "Resource": "arn:aws:s3:::authz-public/*"}],
}))
st, body = raw("GET", "/authz-public/open.txt", headers={"host": f"{S3_HOST}:{S3_PORT}"})
check("anonymous GET succeeds under a public bucket policy", st == 200 and body == b"public-bytes")

# The grant must be exactly as wide as it is written: GetObject only, this bucket only.
st, body = raw("GET", "/authz-public", headers={"host": f"{S3_HOST}:{S3_PORT}"})
check("public GetObject policy does NOT grant anonymous ListBucket -> AccessDenied 403",
      st == 403 and err_code(body) == "AccessDenied")
st, body = raw("PUT", "/authz-public/anon.txt", headers={"host": f"{S3_HOST}:{S3_PORT}"}, body=b"x")
check("public GetObject policy does NOT grant anonymous PutObject -> AccessDenied 403",
      st == 403 and err_code(body) == "AccessDenied")
st, body = raw("DELETE", "/authz-public/open.txt", headers={"host": f"{S3_HOST}:{S3_PORT}"})
check("public GetObject policy does NOT grant anonymous DeleteObject -> AccessDenied 403",
      st == 403 and err_code(body) == "AccessDenied")
st, body = raw("GET", "/authz-root/secret.txt", headers={"host": f"{S3_HOST}:{S3_PORT}"})
check("the public policy did not leak to the OTHER bucket",
      st == 403 and err_code(body) == "AccessDenied")
check("nothing anonymous wrote landed",
      [o["Key"] for o in root.list_objects_v2(Bucket="authz-public").get("Contents", [])]
      == ["hidden.txt", "open.txt"])

# Block Public Access is the kill switch: RestrictPublicBuckets must shut the public policy off
# without the policy itself being touched.
root.put_public_access_block(Bucket="authz-public", PublicAccessBlockConfiguration={
    "BlockPublicAcls": True, "IgnorePublicAcls": True,
    "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
st, body = raw("GET", "/authz-public/open.txt", headers={"host": f"{S3_HOST}:{S3_PORT}"})
check("Block Public Access shuts off the public policy -> AccessDenied 403",
      st == 403 and err_code(body) == "AccessDenied")
check("the bucket policy itself is untouched by BPA",
      "public-read" in root.get_bucket_policy(Bucket="authz-public")["Policy"])
check("the owner can still read through BPA",
      root.get_object(Bucket="authz-public", Key="open.txt")["Body"].read() == b"public-bytes")
root.delete_public_access_block(Bucket="authz-public")
st, body = raw("GET", "/authz-public/open.txt", headers={"host": f"{S3_HOST}:{S3_PORT}"})
check("removing BPA restores the public policy", st == 200 and body == b"public-bytes")

# Finally, deleting the policy must revoke anonymous access.
root.delete_bucket_policy(Bucket="authz-public")
st, body = raw("GET", "/authz-public/open.txt", headers={"host": f"{S3_HOST}:{S3_PORT}"})
check("deleting the bucket policy revokes anonymous GET -> AccessDenied 403",
      st == 403 and err_code(body) == "AccessDenied")

# --- verdict --------------------------------------------------------------------------------------
print()
if GAPS:
    print(f"{len(GAPS)} KNOWN GAP(S) vs the S3 specification:")
    for label, spec, observed, evidence in GAPS:
        print(f"  * {label}\n      required: {spec}\n      observed: {observed}\n"
              f"      evidence: {evidence}")
    if STRICT_GAPS:
        print("STRICT_GAPS=1 — failing on the gap(s) above")
        sys.exit(1)
print("AUTHZ OK — tenancy, SigV4 failure modes, presigned expiry, and anonymous access all pinned")
