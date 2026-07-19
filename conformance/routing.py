#!/usr/bin/env python3
"""Routing fall-through conformance (audit 2026-07): a request segment that is PRESENT BUT INVALID
must be REJECTED, never collapsed into "absent".

`route_path` used to parse both segments with `.ok()`, which folded "invalid" into `None` — and
`None` is exactly how `dispatch` decides an operation is bucket- or root-level. So a malformed
*object* request silently re-routed to the *bucket* handler, verified live against a running
server before the fix:

  * `DELETE /b/<1025-byte key>` reached DeleteBucket and **DESTROYED an empty bucket**.
  * `GET /b/<1025-byte key>` returned 200 + a `ListBucketResult` body instead of an error.
  * `PUT /new/<1025-byte key>` reached CreateBucket and conjured a bucket that was never asked for.
  * `GET /UPPERCASE/k` had *both* segments collapsed and reached ListBuckets.

The same class covers an unhandled `?subresource`: `DELETE /b?ownershipControls` must not fall
through to the bare verb and delete the bucket (ARCH 13 — answer 501 NotImplemented).

Every assertion here FAILS before the fix and passes after, and each pins the **exact HTTP status**
rather than merely "a ClientError was raised" — the whole bug was a wrong-but-successful response,
which a bare `except ClientError` would happily accept.

Args: <sigv4_access_key> <sigv4_secret> <s3_endpoint>
"""

import hashlib
import http.client
import sys
import urllib.parse

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import ClientError

akid, secret, endpoint = sys.argv[1], sys.argv[2], sys.argv[3]

REGION = "us-east-1"
# MAX_KEY_LEN is 1024 (cairn-types `id.rs`); one byte over is the shortest input that trips it.
LONG_KEY = "k" * 1025
# Uppercase is rejected by `BucketName::parse` (DNS-compatible names are lowercase) but ACCEPTED by
# botocore's client-side bucket-name check, so it reaches the server as a real request.
BAD_BUCKET = "RoutingBadName"

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=akid,
    aws_secret_access_key=secret,
    region_name=REGION,
    config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
)


def check(label, cond):
    if not cond:
        print(f"FAIL: {label}")
        sys.exit(1)
    print(f"  ok: {label}")


# --- raw signed request -------------------------------------------------------------------------
# boto3 has no operation for most of what this harness must name (an unhandled `?subresource`, a
# deliberately invalid bucket name, a raw over-long key), and the SDK hides the response body on an
# error — but the body is the evidence for "did this leak a bucket listing?". So build the request
# by hand and sign it with botocore's real SigV4 signer: same wire format, full control.
_creds = Credentials(akid, secret)
_url = urllib.parse.urlsplit(endpoint)


def raw(method, path, query="", body=b""):
    """Send a hand-built, SigV4-signed request. Returns (status, body_bytes)."""
    url = f"{endpoint}{path}" + (f"?{query}" if query else "")
    req = AWSRequest(method=method, url=url, data=body)
    # Set `host` and the payload hash BEFORE signing so both are covered by SignedHeaders and the
    # bytes we put on the wire are exactly the bytes that were signed.
    req.headers["host"] = _url.netloc
    req.headers["x-amz-content-sha256"] = hashlib.sha256(body).hexdigest()
    SigV4Auth(_creds, "s3", REGION).add_auth(req)
    conn = http.client.HTTPConnection(_url.hostname, _url.port, timeout=30)
    conn.request(method, path + (f"?{query}" if query else ""), body=body,
                 headers=dict(req.headers))
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


# A listing that escaped to a caller who asked for an object is the bug's fingerprint. Check the
# root element of EVERY flavour of listing, plus `<Contents>` — the canary bucket is non-empty on
# purpose, so a leaked ListBucket really would carry object entries.
LISTING_MARKERS = (b"ListBucketResult", b"ListAllMyBucketsResult", b"<Contents>", b"<Buckets>")


def no_listing(body):
    return not any(m in body for m in LISTING_MARKERS)


def status_of(err):
    return err.response["ResponseMetadata"]["HTTPStatusCode"]


def code_of(err):
    return err.response["Error"]["Code"]


# --- fixtures -----------------------------------------------------------------------------------
# `rt-empty` is EMPTY on purpose: an empty bucket is the case where a fall-through to DeleteBucket
# SUCCEEDS and destroys it. On a non-empty bucket the same bug only surfaces as a 409 BucketNotEmpty.
s3.create_bucket(Bucket="rt-empty")
s3.create_bucket(Bucket="rt-canary")
s3.create_bucket(Bucket="rt-subres")
s3.put_object(Bucket="rt-canary", Key="canary.txt", Body=b"canary", ContentType="text/plain")
check("fixtures created", s3.get_object(Bucket="rt-canary", Key="canary.txt")["Body"].read()
      == b"canary")

# --- 1. the critical regression: an over-long key must not delete the bucket ---------------------
try:
    s3.delete_object(Bucket="rt-empty", Key=LONG_KEY)
    check("DELETE with a 1025-byte key is rejected (it returned success)", False)
except ClientError as e:
    check("DELETE with a 1025-byte key -> 400 InvalidArgument",
          status_of(e) == 400 and code_of(e) == "InvalidArgument")

# The assertion that actually pins the bug: the bucket is still there.
s3.head_bucket(Bucket="rt-empty")
check("empty bucket SURVIVED the malformed DELETE", True)
check("empty bucket still listed", "rt-empty" in [b["Name"] for b in s3.list_buckets()["Buckets"]])

# The mirror image: a malformed PUT must not reach CreateBucket and conjure a bucket.
st, _ = raw("PUT", f"/rt-ghost/{LONG_KEY}", body=b"x")
check("PUT with a 1025-byte key -> 400", st == 400)
check("no ghost bucket was created",
      "rt-ghost" not in [b["Name"] for b in s3.list_buckets()["Buckets"]])

# --- 2. an over-long key must not return a bucket listing ----------------------------------------
try:
    s3.get_object(Bucket="rt-canary", Key=LONG_KEY)
    check("GET with a 1025-byte key is rejected (it returned success)", False)
except ClientError as e:
    check("GET with a 1025-byte key -> 400 InvalidArgument",
          status_of(e) == 400 and code_of(e) == "InvalidArgument")

# Inspect the raw body: the SDK would surface a leaked ListBucketResult as a successful streaming
# read, so "no exception" is not the same as "no listing leaked".
st, body = raw("GET", f"/rt-canary/{LONG_KEY}")
check("raw GET with a 1025-byte key -> 400", st == 400)
check("raw GET body leaked no bucket listing", no_listing(body))

st, body = raw("HEAD", f"/rt-canary/{LONG_KEY}")
check("raw HEAD with a 1025-byte key -> 400", st == 400)

# --- 3. an invalid bucket name must not fall through to a root-level listing ---------------------
# Both segments collapsed to None before the fix, which is the ListBuckets route — so the response
# enumerated every bucket on the node to a caller who named a bucket that cannot exist.
st, body = raw("GET", f"/{BAD_BUCKET}")
check("GET on an invalid bucket name -> 400", st == 400)
check("invalid bucket name leaked no listing", no_listing(body))
check("invalid bucket name leaked no bucket names", b"rt-canary" not in body)

st, body = raw("GET", f"/{BAD_BUCKET}/some-key")
check("GET on an invalid bucket name + key -> 400", st == 400)
check("invalid bucket name + key leaked no listing", no_listing(body))

st, _ = raw("DELETE", f"/{BAD_BUCKET}")
check("DELETE on an invalid bucket name -> 400", st == 400)

# --- 4. unhandled subresources answer 501 and the target survives --------------------------------
# `DELETE /b?<subresource>` with no handler must NOT fall through to the bare DELETE verb. Run it
# against the EMPTY bucket, the case where the bare verb would actually succeed and destroy it.
for sub in ("ownershipControls", "accelerate", "versioning", "requestPayment", "logging",
            "location", "website", "notification"):
    st, body = raw("DELETE", "/rt-subres", query=sub)
    check(f"DELETE /rt-subres?{sub} -> 501 NotImplemented",
          st == 501 and b"NotImplemented" in body)
s3.head_bucket(Bucket="rt-subres")
check("bucket SURVIVED every unhandled subresource DELETE", True)

# The object-level twin: `PUT key?attributes` must not be treated as a body-bearing PUT and
# overwrite the object, and `DELETE key?uploads` must not delete it.
st, body = raw("PUT", "/rt-canary/canary.txt", query="attributes", body=b"CLOBBERED")
check("PUT canary.txt?attributes -> 501 NotImplemented", st == 501 and b"NotImplemented" in body)
st, body = raw("DELETE", "/rt-canary/canary.txt", query="uploads")
check("DELETE canary.txt?uploads -> 501 NotImplemented", st == 501 and b"NotImplemented" in body)

# --- 5. the canary is untouched by every malformed request above ---------------------------------
got = s3.get_object(Bucket="rt-canary", Key="canary.txt")
check("canary object still readable", got["Body"].read() == b"canary")
check("canary object was not clobbered", got["ContentLength"] == 6)
keys = [o["Key"] for o in s3.list_objects_v2(Bucket="rt-canary").get("Contents", [])]
check("canary bucket holds exactly the canary", keys == ["canary.txt"])
names = [b["Name"] for b in s3.list_buckets()["Buckets"]]
check("all three buckets survived",
      {"rt-empty", "rt-canary", "rt-subres"} <= set(names))

print("ROUTING OK — invalid segments are rejected, never re-routed to the bucket/root handler")
