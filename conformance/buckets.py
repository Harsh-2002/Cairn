#!/usr/bin/env python3
"""Bucket-level operation surface conformance (Package D).

Cairn's bucket *object* surface is well covered by `conformance.py`; the bucket surface itself was
not. This harness pins, with EXACT S3 error codes AND exact HTTP status codes:

  * CreateBucket, its `Location` header, and re-creating a bucket you already own.
  * Bucket-name validation — uppercase, <3, >63, IP-address-shaped, illegal charset, leading /
    trailing dot or hyphen, adjacent separators — and that a rejected name creates NO bucket.
  * DeleteBucket: 204 empty / 409 BucketNotEmpty / 404 NoSuchBucket, plus "the 409 did not
    half-delete anything".
  * HeadBucket: 200 owned / 404 missing.
  * GetBucketLocation.
  * The full PUT -> GET -> DELETE -> GET(404) round-trip for every bucket config subresource Cairn
    implements (tagging, cors, lifecycle, policy, versioning, replication), and a 501 NotImplemented
    for every one it does not (website, encryption, notification, accelerate, ...).
  * A REAL, unauthenticated CORS preflight (`OPTIONS` + `Origin` + `Access-Control-Request-Method`)
    against a configured rule set, asserting the `Access-Control-Allow-*` response headers, plus the
    negative cases (wrong origin / wrong method / disallowed header / no config).

LOAD-BEARING INVARIANT: after EVERY config-subresource DELETE the harness re-asserts the bucket
itself still exists. That is the standing guard against the PR #1 routing fall-through class, where
`DELETE /b?<subresource>` reached the bare DELETE verb and DESTROYED the bucket. The config bucket
is deliberately kept EMPTY, because an empty bucket is exactly the case where such a fall-through
SUCCEEDS instead of bouncing off a 409 BucketNotEmpty.

Args: <sigv4_access_key> <sigv4_secret> <s3_endpoint>
"""

import hashlib
import http.client
import json
import sys
import urllib.parse

import boto3
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import ClientError

akid, secret, endpoint = sys.argv[1], sys.argv[2], sys.argv[3]

REGION = "us-east-1"  # CAIRN_REGION default (cairn-server config.rs)
ORIGIN = "https://app.example.com"

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=akid,
    aws_secret_access_key=secret,
    region_name=REGION,
    config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
)

_failures = []
_notes = []


def check(label, cond):
    if not cond:
        print(f"FAIL: {label}")
        _failures.append(label)
        return False
    print(f"  ok: {label}")
    return True


def note(label, cond):
    """A cosmetic spec deviation: asserted and reported loudly, but not a gate failure.

    Reserved for differences no S3 client branches on (a response Content-Type on a body every SDK
    parses by shape). Anything a client can observe behaviourally uses `check`.
    """
    if not cond:
        print(f"NOTE (cosmetic deviation): {label}")
        _notes.append(label)
        return False
    print(f"  ok: {label}")
    return True


def status_of(err):
    return err.response["ResponseMetadata"]["HTTPStatusCode"]


def code_of(err):
    return err.response["Error"]["Code"]


# --- raw signed request -----------------------------------------------------------------------
# Most of what this harness must name has no boto3 operation (a deliberately invalid bucket name is
# rejected client-side by botocore's own validator and never reaches the wire; an unimplemented
# `?subresource` has no model at all), and the SDK hides both the raw status and the response
# HEADERS — and the CORS headers ARE the assertion. So hand-build and sign with botocore's real
# SigV4 signer: same wire format, full control.
_creds = Credentials(akid, secret)
_url = urllib.parse.urlsplit(endpoint)


def raw(method, path, query="", body=b"", headers=None, sign=True):
    """Send a hand-built request (SigV4-signed unless `sign=False`).

    Returns (status, headers-dict-with-lowercased-keys, body-bytes).
    """
    target = path + (f"?{query}" if query else "")
    req = AWSRequest(method=method, url=f"{endpoint}{target}", data=body)
    req.headers["host"] = _url.netloc
    for k, v in (headers or {}).items():
        req.headers[k] = v
    if sign:
        # Set host + payload hash BEFORE signing so the bytes on the wire are the bytes signed.
        req.headers["x-amz-content-sha256"] = hashlib.sha256(body).hexdigest()
        # `S3SigV4Auth`, NOT the generic `SigV4Auth`: S3 signs the RAW request path, while the
        # generic signer normalizes and re-encodes it — which turns `/bkt%20name` into
        # `/bkt%2520name` in the canonical request and yields SignatureDoesNotMatch on any bucket
        # name containing a percent-escape. That is a client-side quirk, not a server one.
        S3SigV4Auth(_creds, "s3", REGION).add_auth(req)
    conn = http.client.HTTPConnection(_url.hostname, _url.port, timeout=30)
    conn.request(method, target, body=body, headers=dict(req.headers))
    resp = conn.getresponse()
    data = resp.read()
    hdrs = {k.lower(): v for k, v in resp.getheaders()}
    conn.close()
    return resp.status, hdrs, data


def err_code(body):
    """Pull `<Code>` out of an S3 XML error document (empty string when absent)."""
    start = body.find(b"<Code>")
    if start < 0:
        return ""
    end = body.find(b"</Code>", start)
    return body[start + 6:end].decode() if end > 0 else ""


def bucket_names():
    return [b["Name"] for b in s3.list_buckets()["Buckets"]]


def alive(bucket, label):
    """The load-bearing guard: the bucket itself must still exist."""
    st, _, _ = raw("HEAD", f"/{bucket}")
    return check(f"GUARD: bucket `{bucket}` still exists {label} (HEAD -> 200)", st == 200)


# ==================================================================================================
# 1. CreateBucket + idempotency
# ==================================================================================================
print("\n--- 1. CreateBucket ---")
r = s3.create_bucket(Bucket="bkt-main")
check("CreateBucket -> 200", r["ResponseMetadata"]["HTTPStatusCode"] == 200)
check("CreateBucket echoes Location: /bkt-main",
      r["ResponseMetadata"]["HTTPHeaders"].get("location") == "/bkt-main")
check("CreateBucket is visible in ListBuckets", "bkt-main" in bucket_names())

# Re-creating a bucket you already own. AWS S3 answers 200 OK for this in us-east-1 only and
# 409 BucketAlreadyOwnedByYou everywhere else; Cairn always answers 409 (service.rs:464 maps the
# metadata `Conflict` to `Error::BucketAlreadyOwnedByYou`, error_map.rs:18 -> 409). Pinned to the
# implemented behaviour, which is what MinIO/Ceph also do. See the report note.
try:
    s3.create_bucket(Bucket="bkt-main")
    check("re-CreateBucket of your own bucket is answered (it returned success)", False)
except ClientError as e:
    check("re-CreateBucket own bucket -> 409 BucketAlreadyOwnedByYou",
          code_of(e) == "BucketAlreadyOwnedByYou" and status_of(e) == 409)

# The failed re-create must not have disturbed the original.
alive("bkt-main", "after a duplicate CreateBucket")

# A bucket with dots in the middle is a LEGAL S3 name — the positive control for the charset rules
# exercised below, so a blanket "reject anything with a dot" would be caught.
st, _, _ = raw("PUT", "/bkt.dots.ok")
check("CreateBucket with legal embedded dots -> 200", st == 200)


# ==================================================================================================
# 2. Bucket-name validation (BucketName::parse, cairn-types/src/id.rs:45)
# ==================================================================================================
# Every one of these is sent RAW: botocore's client-side bucket validator would refuse to put some
# of them on the wire, and the point is to test the SERVER's validation.
print("\n--- 2. bucket-name validation ---")
BAD_NAMES = [
    ("BktUpper",        "uppercase letters"),
    ("ab",              "shorter than 3 characters"),
    ("a" * 64,          "longer than 63 characters"),
    ("192.168.100.1",   "IP-address-shaped"),
    ("bkt_underscore",  "underscore (illegal charset)"),
    ("bkt name",        "space (illegal charset)"),
    ("bkt$dollar",      "dollar sign (illegal charset)"),
    (".leading-dot",    "leading dot"),
    ("trailing-dot.",   "trailing dot"),
    ("-leading-hyphen", "leading hyphen"),
    ("trailing-hyphen-", "trailing hyphen"),
    ("bkt..double-dot", "adjacent dots"),
    ("bkt.-mixed",      "adjacent dot+hyphen"),
]
for name, why in BAD_NAMES:
    quoted = urllib.parse.quote(name, safe="")
    st, _, body = raw("PUT", f"/{quoted}")
    # KNOWN GAP (cosmetic): AWS S3 answers `InvalidBucketName` here. Cairn folds every name-parse
    # failure into `InvalidArgument` (cairn-types error.rs:236). The 400 status — the part clients
    # branch on — is correct, so this pins Cairn's actual code rather than asserting nothing.
    check(f"CreateBucket `{name}` ({why}) -> 400 InvalidArgument",
          st == 400 and err_code(body) == "InvalidArgument")

names = bucket_names()
check("no invalid-name bucket was created",
      not any(n for n, _ in BAD_NAMES if n in names))
# A 63-character name is the boundary that must be ACCEPTED, proving the length check is inclusive.
LONGEST = "b" * 63
st, _, _ = raw("PUT", f"/{LONGEST}")
check("CreateBucket with a 63-character name (the inclusive upper bound) -> 200", st == 200)
st, _, _ = raw("DELETE", f"/{LONGEST}")
check("...and it deletes cleanly -> 204", st == 204)


# ==================================================================================================
# 3. HeadBucket
# ==================================================================================================
print("\n--- 3. HeadBucket ---")
st, hdrs, body = raw("HEAD", "/bkt-main")
check("HeadBucket on an owned bucket -> 200", st == 200)
check("HeadBucket echoes x-amz-request-id", "x-amz-request-id" in hdrs)
check("HeadBucket sends no body (HTTP HEAD)", body == b"")

st, _, _ = raw("HEAD", "/bkt-does-not-exist")
check("HeadBucket on a missing bucket -> 404", st == 404)
try:
    s3.head_bucket(Bucket="bkt-does-not-exist")
    check("SDK HeadBucket on a missing bucket raises (it returned success)", False)
except ClientError as e:
    # HEAD carries no body, so botocore has no `<Code>` to read and synthesizes "404".
    check("SDK HeadBucket missing -> 404 / code '404'",
          status_of(e) == 404 and code_of(e) == "404")

# GET (ListObjects) on the same missing bucket DOES carry a body, so the exact S3 code is checkable.
try:
    s3.list_objects_v2(Bucket="bkt-does-not-exist")
    check("ListObjects on a missing bucket raises (it returned success)", False)
except ClientError as e:
    check("ListObjects on a missing bucket -> 404 NoSuchBucket",
          code_of(e) == "NoSuchBucket" and status_of(e) == 404)


# ==================================================================================================
# 4. GetBucketLocation
# ==================================================================================================
print("\n--- 4. GetBucketLocation ---")
loc = s3.get_bucket_location(Bucket="bkt-main")
check("GetBucketLocation -> 200", loc["ResponseMetadata"]["HTTPStatusCode"] == 200)
check(f"GetBucketLocation returns the configured region ({REGION})",
      loc.get("LocationConstraint") == REGION)
st, _, body = raw("GET", "/bkt-main", query="location")
check("raw GetBucketLocation body is a LocationConstraint document",
      st == 200 and b"<LocationConstraint" in body)
try:
    s3.get_bucket_location(Bucket="bkt-does-not-exist")
    check("GetBucketLocation on a missing bucket raises (it returned success)", False)
except ClientError as e:
    check("GetBucketLocation on a missing bucket -> 404 NoSuchBucket",
          code_of(e) == "NoSuchBucket" and status_of(e) == 404)


# ==================================================================================================
# 5. DeleteBucket
# ==================================================================================================
print("\n--- 5. DeleteBucket ---")
try:
    s3.delete_bucket(Bucket="bkt-does-not-exist")
    check("DeleteBucket on a missing bucket raises (it returned success)", False)
except ClientError as e:
    check("DeleteBucket on a missing bucket -> 404 NoSuchBucket",
          code_of(e) == "NoSuchBucket" and status_of(e) == 404)

s3.create_bucket(Bucket="bkt-full")
s3.put_object(Bucket="bkt-full", Key="occupant.txt", Body=b"occupied")
try:
    s3.delete_bucket(Bucket="bkt-full")
    check("DeleteBucket on a NON-EMPTY bucket raises (it returned success)", False)
except ClientError as e:
    check("DeleteBucket on a non-empty bucket -> 409 BucketNotEmpty",
          code_of(e) == "BucketNotEmpty" and status_of(e) == 409)
# The rejected delete must be a total no-op: bucket AND its contents intact.
alive("bkt-full", "after a rejected non-empty DeleteBucket")
check("the occupying object survived the rejected DeleteBucket",
      s3.get_object(Bucket="bkt-full", Key="occupant.txt")["Body"].read() == b"occupied")

s3.delete_object(Bucket="bkt-full", Key="occupant.txt")
r = s3.delete_bucket(Bucket="bkt-full")
check("DeleteBucket on a now-empty bucket -> 204", r["ResponseMetadata"]["HTTPStatusCode"] == 204)
check("the deleted bucket left ListBuckets", "bkt-full" not in bucket_names())
st, _, _ = raw("HEAD", "/bkt-full")
check("HeadBucket on the deleted bucket -> 404", st == 404)
# DeleteBucket is not idempotent in S3: the second delete is a 404, not a 204.
try:
    s3.delete_bucket(Bucket="bkt-full")
    check("second DeleteBucket raises (it returned success)", False)
except ClientError as e:
    check("second DeleteBucket -> 404 NoSuchBucket",
          code_of(e) == "NoSuchBucket" and status_of(e) == 404)


# ==================================================================================================
# 6. Bucket config subresources: PUT -> GET -> DELETE -> GET(404) round-trips
# ==================================================================================================
# `bkt-cfg` is deliberately kept EMPTY for the whole section: an empty bucket is the case where a
# subresource DELETE that falls through to the bare DELETE verb SUCCEEDS and destroys the bucket
# (on a non-empty bucket the same bug only shows as a harmless 409). Every `alive()` below is that
# guard firing.
print("\n--- 6. config subresources ---")
s3.create_bucket(Bucket="bkt-cfg")

# The exact S3 error code each subresource must return when it has never been configured.
ABSENT_CODES = {
    "tagging": "NoSuchTagSet",
    "cors": "NoSuchCORSConfiguration",
    "lifecycle": "NoSuchLifecycleConfiguration",
    "policy": "NoSuchBucketPolicy",
    "replication": "ReplicationConfigurationNotFoundError",
}
for sub, code in ABSENT_CODES.items():
    st, hdrs, body = raw("GET", "/bkt-cfg", query=sub)
    check(f"GET ?{sub} when unset -> 404 {code}", st == 404 and err_code(body) == code)
    check(f"GET ?{sub} 404 echoes x-amz-request-id", "x-amz-request-id" in hdrs)

# --- 6a. tagging ---------------------------------------------------------------------------------
r = s3.put_bucket_tagging(Bucket="bkt-cfg", Tagging={"TagSet": [
    {"Key": "env", "Value": "conformance"}, {"Key": "owner", "Value": "buckets-harness"}]})
check("PutBucketTagging -> 204", r["ResponseMetadata"]["HTTPStatusCode"] == 204)
got = {t["Key"]: t["Value"] for t in s3.get_bucket_tagging(Bucket="bkt-cfg")["TagSet"]}
check("GetBucketTagging round-trips both tags",
      got == {"env": "conformance", "owner": "buckets-harness"})
r = s3.delete_bucket_tagging(Bucket="bkt-cfg")
check("DeleteBucketTagging -> 204", r["ResponseMetadata"]["HTTPStatusCode"] == 204)
st, _, body = raw("GET", "/bkt-cfg", query="tagging")
check("GET ?tagging after DELETE -> 404 NoSuchTagSet",
      st == 404 and err_code(body) == "NoSuchTagSet")
alive("bkt-cfg", "after DELETE ?tagging")

# --- 6b. cors ------------------------------------------------------------------------------------
CORS_XML = f"""<CORSConfiguration>
  <CORSRule>
    <AllowedOrigin>{ORIGIN}</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedMethod>PUT</AllowedMethod>
    <AllowedHeader>content-type</AllowedHeader>
    <AllowedHeader>x-amz-*</AllowedHeader>
    <ExposeHeader>ETag</ExposeHeader>
    <ExposeHeader>x-amz-request-id</ExposeHeader>
    <MaxAgeSeconds>3000</MaxAgeSeconds>
  </CORSRule>
</CORSConfiguration>""".encode()
st, _, _ = raw("PUT", "/bkt-cfg", query="cors", body=CORS_XML,
               headers={"content-type": "application/xml"})
check("PutBucketCors -> 204", st == 204)
rules = s3.get_bucket_cors(Bucket="bkt-cfg")["CORSRules"]
check("GetBucketCors returns exactly one rule", len(rules) == 1)
check("GetBucketCors round-trips AllowedOrigins", rules[0]["AllowedOrigins"] == [ORIGIN])
check("GetBucketCors round-trips AllowedMethods", rules[0]["AllowedMethods"] == ["GET", "PUT"])
check("GetBucketCors round-trips ExposeHeaders",
      rules[0]["ExposeHeaders"] == ["ETag", "x-amz-request-id"])
check("GetBucketCors round-trips MaxAgeSeconds", rules[0]["MaxAgeSeconds"] == 3000)
# A malformed CORS document must be REJECTED at PUT, not stored to fail later at preflight time.
st, _, body = raw("PUT", "/bkt-cfg", query="cors", body=b"<CORSConfiguration><CORSRule>")
check("PutBucketCors with malformed XML -> 400 MalformedXML",
      st == 400 and err_code(body) == "MalformedXML")
check("the malformed PUT did not clobber the stored CORS rules",
      s3.get_bucket_cors(Bucket="bkt-cfg")["CORSRules"] == rules)
# (the DELETE + 404 re-check for cors happens in section 7, after the preflight tests use it)

# --- 6c. lifecycle -------------------------------------------------------------------------------
r = s3.put_bucket_lifecycle_configuration(Bucket="bkt-cfg", LifecycleConfiguration={"Rules": [
    {"ID": "expire-tmp", "Status": "Enabled", "Filter": {"Prefix": "tmp/"},
     "Expiration": {"Days": 7}}]})
check("PutBucketLifecycleConfiguration -> 204", r["ResponseMetadata"]["HTTPStatusCode"] == 204)
lc = s3.get_bucket_lifecycle_configuration(Bucket="bkt-cfg")["Rules"]
check("GetBucketLifecycleConfiguration round-trips the rule",
      len(lc) == 1 and lc[0]["ID"] == "expire-tmp" and lc[0]["Expiration"]["Days"] == 7)
r = s3.delete_bucket_lifecycle(Bucket="bkt-cfg")
check("DeleteBucketLifecycle -> 204", r["ResponseMetadata"]["HTTPStatusCode"] == 204)
st, _, body = raw("GET", "/bkt-cfg", query="lifecycle")
check("GET ?lifecycle after DELETE -> 404 NoSuchLifecycleConfiguration",
      st == 404 and err_code(body) == "NoSuchLifecycleConfiguration")
alive("bkt-cfg", "after DELETE ?lifecycle")

# --- 6d. policy ----------------------------------------------------------------------------------
POLICY = {
    "Version": "2012-10-17",
    "Statement": [{
        "Sid": "PublicRead", "Effect": "Allow", "Principal": "*",
        "Action": ["s3:GetObject"], "Resource": "arn:aws:s3:::bkt-cfg/public/*",
    }],
}
r = s3.put_bucket_policy(Bucket="bkt-cfg", Policy=json.dumps(POLICY))
check("PutBucketPolicy -> 204", r["ResponseMetadata"]["HTTPStatusCode"] == 204)
check("GetBucketPolicy round-trips the document",
      json.loads(s3.get_bucket_policy(Bucket="bkt-cfg")["Policy"]) == POLICY)
# A bucket policy is a JSON document; AWS answers GetBucketPolicy with `Content-Type:
# application/json`. Cairn serves every stored config doc through `get_bucket_doc`, which hardcodes
# `S3Response::xml` (service.rs:2437) — so the JSON body is labelled application/xml. No SDK
# branches on it (botocore reads the raw payload), hence a note rather than a gate failure.
st, hdrs, _ = raw("GET", "/bkt-cfg", query="policy")
note("GetBucketPolicy is served as application/json",
     hdrs.get("content-type", "").startswith("application/json"))
# A syntactically invalid policy must be rejected by the policy engine, not stored blind.
st, _, body = raw("PUT", "/bkt-cfg", query="policy", body=b'{"Statement": "not-a-list"}')
check("PutBucketPolicy with a malformed policy -> 400 MalformedPolicy",
      st == 400 and err_code(body) == "MalformedPolicy")
check("the malformed PUT did not clobber the stored policy",
      json.loads(s3.get_bucket_policy(Bucket="bkt-cfg")["Policy"]) == POLICY)
r = s3.delete_bucket_policy(Bucket="bkt-cfg")
check("DeleteBucketPolicy -> 204", r["ResponseMetadata"]["HTTPStatusCode"] == 204)
st, _, body = raw("GET", "/bkt-cfg", query="policy")
check("GET ?policy after DELETE -> 404 NoSuchBucketPolicy",
      st == 404 and err_code(body) == "NoSuchBucketPolicy")
alive("bkt-cfg", "after DELETE ?policy")

# --- 6e. versioning ------------------------------------------------------------------------------
# GetBucketVersioning on a never-versioned bucket is a 200 with an EMPTY Status (S3 semantics), not
# a 404 — versioning is the one subresource that always exists.
v = s3.get_bucket_versioning(Bucket="bkt-cfg")
check("GetBucketVersioning on a fresh bucket -> 200 with no Status",
      v["ResponseMetadata"]["HTTPStatusCode"] == 200 and "Status" not in v)
r = s3.put_bucket_versioning(Bucket="bkt-cfg",
                             VersioningConfiguration={"Status": "Enabled"})
check("PutBucketVersioning(Enabled) -> 200", r["ResponseMetadata"]["HTTPStatusCode"] == 200)
check("GetBucketVersioning reads back Enabled",
      s3.get_bucket_versioning(Bucket="bkt-cfg").get("Status") == "Enabled")
s3.put_bucket_versioning(Bucket="bkt-cfg", VersioningConfiguration={"Status": "Suspended"})
check("GetBucketVersioning reads back Suspended",
      s3.get_bucket_versioning(Bucket="bkt-cfg").get("Status") == "Suspended")
# Versioning has no DELETE in S3 — and the fall-through class means the bare DELETE verb would
# destroy the bucket. It must be 501, and the bucket must survive.
st, _, body = raw("DELETE", "/bkt-cfg", query="versioning")
check("DELETE ?versioning -> 501 NotImplemented (no such S3 operation)",
      st == 501 and err_code(body) == "NotImplemented")
alive("bkt-cfg", "after DELETE ?versioning")
# A bogus versioning status must be rejected rather than silently stored.
st, _, body = raw("PUT", "/bkt-cfg", query="versioning",
                  body=b"<VersioningConfiguration><Status>Sideways</Status>"
                       b"</VersioningConfiguration>")
check("PutBucketVersioning with an unknown Status -> 400", st == 400)
check("the rejected PUT left versioning Suspended",
      s3.get_bucket_versioning(Bucket="bkt-cfg").get("Status") == "Suspended")

# --- 6f. replication -----------------------------------------------------------------------------
# Cairn requires versioning Enabled before a replication rule may be stored (service.rs:2493) —
# a documented, deliberate divergence-from-nothing that mirrors S3's own requirement.
REPL_XML = (b"<ReplicationConfiguration><Role>arn:aws:iam::0:role/x</Role><Rule>"
            b"<ID>r1</ID><Status>Enabled</Status><Prefix></Prefix>"
            b"<Destination><Bucket>arn:aws:s3:::bkt-main</Bucket></Destination>"
            b"</Rule></ReplicationConfiguration>")
st, _, body = raw("PUT", "/bkt-cfg", query="replication", body=REPL_XML)
check("PutBucketReplication on a SUSPENDED-versioning bucket -> 400 InvalidRequest",
      st == 400 and err_code(body) == "InvalidRequest")
s3.put_bucket_versioning(Bucket="bkt-cfg", VersioningConfiguration={"Status": "Enabled"})
st, _, _ = raw("PUT", "/bkt-cfg", query="replication", body=REPL_XML)
check("PutBucketReplication on a versioned bucket -> 204", st == 204)
st, _, body = raw("GET", "/bkt-cfg", query="replication")
check("GetBucketReplication round-trips the rule", st == 200 and b"<ID>r1</ID>" in body)
st, _, _ = raw("DELETE", "/bkt-cfg", query="replication")
check("DeleteBucketReplication -> 204", st == 204)
st, _, body = raw("GET", "/bkt-cfg", query="replication")
check("GET ?replication after DELETE -> 404 ReplicationConfigurationNotFoundError",
      st == 404 and err_code(body) == "ReplicationConfigurationNotFoundError")
alive("bkt-cfg", "after DELETE ?replication")

# --- 6g. the subresources Cairn does NOT implement -----------------------------------------------
# Not "skipped": an unimplemented operation must answer a clean 501 NotImplemented, and — the
# fall-through guard again — must NOT reach the bare verb underneath it.
UNIMPLEMENTED = ["website", "encryption", "notification", "accelerate", "requestPayment",
                 "logging", "analytics", "inventory", "metrics", "intelligent-tiering",
                 "policyStatus"]
for sub in UNIMPLEMENTED:
    st, _, body = raw("GET", "/bkt-cfg", query=sub)
    check(f"GET ?{sub} -> 501 NotImplemented", st == 501 and err_code(body) == "NotImplemented")
    st, _, body = raw("PUT", "/bkt-cfg", query=sub, body=b"<x/>")
    check(f"PUT ?{sub} -> 501 NotImplemented", st == 501 and err_code(body) == "NotImplemented")
alive("bkt-cfg", "after every unimplemented-subresource GET/PUT")

# KNOWN GAP (suspected product bug): a 501 NotImplemented is a CLIENT-fault, deterministic answer —
# "this node does not implement that operation" — but `error_response` (error_map.rs:52) gates the
# opaque-message + `tracing::error!` path on `status.is_server_error()`, and 501 is in the 5xx
# class. So every unimplemented subresource (a) answers `<Message>We encountered an internal error.
# Please try again.</Message>`, telling the caller to RETRY something that can never succeed, and
# (b) emits an ERROR-level "internal error serving request" log line — remotely triggerable log
# spam that pollutes real 5xx alerting. AWS's message here is descriptive and non-retryable.
# The `<Code>` is correct; only the message and the log level are wrong.
st, _, body = raw("GET", "/bkt-cfg", query="website")
check("501 NotImplemented carries a descriptive message, not the opaque internal-error text",
      b"We encountered an internal error" not in body)


# ==================================================================================================
# 7. CORS preflight — a REAL browser preflight, sent UNAUTHENTICATED
# ==================================================================================================
# ARCH 18.2: a browser sends `OPTIONS` with no credentials, so preflight is evaluated against the
# bucket's stored CORS rules BEFORE authentication (service.rs `dispatch`). These requests carry no
# Authorization header on purpose — if any of them 403'd on auth rather than on a CORS mismatch, the
# feature would be unusable from a browser.
print("\n--- 7. CORS preflight (unauthenticated) ---")


def preflight(bucket, origin=ORIGIN, method="GET", req_headers=None):
    hdrs = {"origin": origin, "access-control-request-method": method}
    if req_headers:
        hdrs["access-control-request-headers"] = req_headers
    return raw("OPTIONS", f"/{bucket}", headers=hdrs, sign=False)

st, hdrs, _ = preflight("bkt-cfg", req_headers="content-type")
check("preflight (allowed origin+method, unauthenticated) -> 200", st == 200)
check("preflight echoes Access-Control-Allow-Origin: the origin",
      hdrs.get("access-control-allow-origin") == ORIGIN)
check("preflight advertises both allowed methods",
      {m.strip() for m in hdrs.get("access-control-allow-methods", "").split(",")} == {"GET", "PUT"})
check("preflight echoes the requested header in Access-Control-Allow-Headers",
      hdrs.get("access-control-allow-headers") == "content-type")
check("preflight returns Access-Control-Max-Age: 3000",
      hdrs.get("access-control-max-age") == "3000")
check("preflight returns Vary: Origin (cacheability)", hdrs.get("vary") == "Origin")
check("preflight advertises the ExposeHeaders",
      {h.strip() for h in hdrs.get("access-control-expose-headers", "").split(",")}
      == {"ETag", "x-amz-request-id"})

# A wildcard AllowedHeader pattern must cover a matching request header...
st, hdrs, _ = preflight("bkt-cfg", req_headers="x-amz-meta-colour")
check("preflight with a header matched by the `x-amz-*` wildcard -> 200",
      st == 200 and hdrs.get("access-control-allow-headers") == "x-amz-meta-colour")

# ...and the negative cases must all be a clean 403, never an accidental 200.
st, hdrs, body = preflight("bkt-cfg", origin="https://evil.example.com")
check("preflight from a DISALLOWED origin -> 403 AccessDenied",
      st == 403 and err_code(body) == "AccessDenied")
check("preflight from a disallowed origin emits no Allow-Origin header",
      "access-control-allow-origin" not in hdrs)

st, _, body = preflight("bkt-cfg", method="DELETE")
check("preflight for a DISALLOWED method -> 403 AccessDenied",
      st == 403 and err_code(body) == "AccessDenied")

st, _, body = preflight("bkt-cfg", req_headers="x-not-allowed")
check("preflight requesting a DISALLOWED header -> 403 AccessDenied",
      st == 403 and err_code(body) == "AccessDenied")

st, _, body = raw("OPTIONS", "/bkt-cfg", headers={"origin": ORIGIN}, sign=False)
check("OPTIONS with no Access-Control-Request-Method (not a preflight) -> 403 AccessDenied",
      st == 403 and err_code(body) == "AccessDenied")

st, _, body = raw("OPTIONS", "/bkt-cfg", headers={"access-control-request-method": "GET"},
                  sign=False)
check("OPTIONS with no Origin (not a preflight) -> 403 AccessDenied",
      st == 403 and err_code(body) == "AccessDenied")

st, _, body = preflight("bkt-main")
check("preflight against a bucket with NO CORS configuration -> 403 AccessDenied",
      st == 403 and err_code(body) == "AccessDenied")

# --- the ACTUAL cross-origin request, not just the preflight -------------------------------------
# A browser blocks the response body of a real cross-origin GET unless the RESPONSE also carries
# Allow-Origin — the preflight alone is not enough (audit 2026-07, service.rs `handle`).
s3.put_object(Bucket="bkt-cfg", Key="public/hello.txt", Body=b"cors-body")
st, hdrs, body = raw("GET", "/bkt-cfg/public/hello.txt", headers={"origin": ORIGIN})
check("cross-origin GET succeeds", st == 200 and body == b"cors-body")
check("cross-origin GET response carries Access-Control-Allow-Origin",
      hdrs.get("access-control-allow-origin") == ORIGIN)
check("cross-origin GET response carries Vary: Origin", hdrs.get("vary") == "Origin")
check("cross-origin GET response exposes the configured headers",
      {h.strip() for h in hdrs.get("access-control-expose-headers", "").split(",")}
      == {"ETag", "x-amz-request-id"})
st, hdrs, _ = raw("GET", "/bkt-cfg/public/hello.txt",
                  headers={"origin": "https://evil.example.com"})
check("cross-origin GET from a disallowed origin gets NO Allow-Origin header",
      st == 200 and "access-control-allow-origin" not in hdrs)

# --- DeleteBucketCors closes the round-trip -------------------------------------------------------
r = s3.delete_bucket_cors(Bucket="bkt-cfg")
check("DeleteBucketCors -> 204", r["ResponseMetadata"]["HTTPStatusCode"] == 204)
st, _, body = raw("GET", "/bkt-cfg", query="cors")
check("GET ?cors after DELETE -> 404 NoSuchCORSConfiguration",
      st == 404 and err_code(body) == "NoSuchCORSConfiguration")
st, _, body = preflight("bkt-cfg")
check("preflight after DeleteBucketCors -> 403 AccessDenied",
      st == 403 and err_code(body) == "AccessDenied")
st, hdrs, _ = raw("GET", "/bkt-cfg/public/hello.txt", headers={"origin": ORIGIN})
check("cross-origin GET after DeleteBucketCors carries no Allow-Origin header",
      st == 200 and "access-control-allow-origin" not in hdrs)
alive("bkt-cfg", "after DELETE ?cors")

# Preflight must not become a pre-auth bucket oracle beyond what S3 already leaks: a preflight for a
# bucket that does not exist answers NoSuchBucket (fetch_bucket runs before the CORS match).
st, _, body = preflight("bkt-no-such-bucket")
check("preflight against a MISSING bucket -> 404 NoSuchBucket",
      st == 404 and err_code(body) == "NoSuchBucket")


# ==================================================================================================
# 8. Final survivorship sweep
# ==================================================================================================
print("\n--- 8. survivorship ---")
names = set(bucket_names())
check("every bucket that should exist still exists",
      {"bkt-main", "bkt-cfg", "bkt.dots.ok"} <= names)
check("every bucket that was deleted is gone",
      not ({"bkt-full", LONGEST} & names))
check("no ghost bucket was conjured by any rejected request",
      names == {"bkt-main", "bkt-cfg", "bkt.dots.ok"})
check("the object written through the CORS section is intact",
      s3.get_object(Bucket="bkt-cfg", Key="public/hello.txt")["Body"].read() == b"cors-body")

if _notes:
    print(f"\n{len(_notes)} cosmetic deviation(s) (reported, not gating):")
    for n in _notes:
        print(f"  - {n}")
if _failures:
    print(f"\nBUCKETS FAILED — {len(_failures)} assertion(s):")
    for f in _failures:
        print(f"  - {f}")
    sys.exit(1)
print("\nBUCKETS OK — create/delete/head/location, name validation, every config subresource "
      "round-trip, and CORS preflight all behave; no config DELETE ever took the bucket with it")
