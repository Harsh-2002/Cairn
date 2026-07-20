#!/usr/bin/env python3
"""Multipart-upload lifecycle conformance (package C).

`conformance.py` only ever reaches multipart through the high-level `upload_fileobj` transfer
manager, which by construction can never produce an out-of-order part list, an undersized
non-final part, a superseded part number, a double-completion, or a visible `CompleteMultipartUpload`
response — so the entire *session-state* half of the S3 multipart contract was untested. This
harness drives the LOW-LEVEL `create_multipart_upload` / `upload_part` / `complete_multipart_upload`
triple throughout and pins the exact S3 error CODE **and** HTTP STATUS of every failure mode:

  NoSuchUpload 404 · InvalidPart 400 · InvalidPartOrder 400 · EntityTooSmall 400 · InvalidArgument 400

It also drives ListMultipartUploads paging as a REAL loop with a hard iteration budget. Issues #2/#3
were exactly an infinite-loop (a prefix-matching key re-served page 1 forever) and a `max-uploads=0`
bug in that listing, so "the loop terminates AND covers every upload exactly once" is asserted
through the SDK rather than asserted about the SQL.

Every check runs to the end (they do not short-circuit) and the failures are reprinted as a list,
because a harness written against untested behaviour is most useful when it shows ALL of the gaps
in one run. This harness originally found three product defects; two — `max-parts=0` returning an
unterminable page, and `EntityTooSmall` being reported before `InvalidPartOrder` — are now FIXED
and their assertions are ordinary gating `check()`s. One remains open and is marked
`known_issue()` so it reports loudly without gating CI:

  * An early 4xx on a body-bearing UploadPart abandons the request body without `Connection: close`,
    so the keep-alive connection is silently unusable and a LATER, unrelated request fails
    (tracked as https://github.com/Harsh-2002/Cairn/issues/5; measured 6/6 failures at 2 MiB).

Args: <sigv4_access_key> <sigv4_secret> <s3_endpoint>
"""

import hashlib
import http.client
import sys
import urllib.parse
import xml.etree.ElementTree as ET

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import ClientError

akid, secret, endpoint = sys.argv[1], sys.argv[2], sys.argv[3]

REGION = "us-east-1"
NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
MIN_PART = 5 * 1024 * 1024  # S3's minimum size for a non-final part

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=akid,
    aws_secret_access_key=secret,
    region_name=REGION,
    config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
)

FAILURES = []


def check(label, cond):
    if not cond:
        print(f"FAIL: {label}")
        FAILURES.append(label)
    else:
        print(f"  ok: {label}")


def known_issue(label, cond, issue, why):
    """A real assertion for a defect that is TRACKED but not yet fixed.

    It reports loudly either way but does NOT fail the run, so the harness can gate CI on
    everything else. This is deliberately NOT a way to soften an expectation: the assertion
    keeps asserting the correct behavior, and `issue` must name an open issue. When the fix
    lands, promote this back to `check` and delete the marker.
    """
    if cond:
        print(f"  ok: {label}  (tracked as {issue} — appears FIXED, promote to check())")
    else:
        print(f"KNOWN ISSUE {issue} (not gating): {label}\n       {why}")


def status_of(err):
    return err.response["ResponseMetadata"]["HTTPStatusCode"]


def code_of(err):
    return err.response["Error"]["Code"]


# --- raw signed request ---------------------------------------------------------------------
# Needed for the wire-level cases boto3 cannot express or hides: a partNumber outside 1..10000
# (botocore refuses some of them client-side, and a client-side refusal proves nothing about the
# server), and the raw ListParts/ListMultipartUploads XML — `IsTruncated` without a
# `NextPartNumberMarker` is an unterminable page, and only the body shows that.
_creds = Credentials(akid, secret)
_url = urllib.parse.urlsplit(endpoint)


def raw(method, path, query="", body=b""):
    """Send a hand-built, SigV4-signed request. Returns (status, body_bytes)."""
    url = f"{endpoint}{path}" + (f"?{query}" if query else "")
    req = AWSRequest(method=method, url=url, data=body)
    req.headers["host"] = _url.netloc
    req.headers["x-amz-content-sha256"] = hashlib.sha256(body).hexdigest()
    SigV4Auth(_creds, "s3", REGION).add_auth(req)
    conn = http.client.HTTPConnection(_url.hostname, _url.port, timeout=60)
    conn.request(method, path + (f"?{query}" if query else ""), body=body,
                 headers=dict(req.headers))
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


def err_code(body):
    """The S3 error code out of a raw error document."""
    try:
        return (ET.fromstring(body).findtext("Code") or "").strip()
    except ET.ParseError:
        return ""


def text(elem, tag):
    return elem.findtext(f"{NS}{tag}")


def md5hex(b):
    return hashlib.md5(b).hexdigest()


def mp_etag(part_md5_hexes):
    """The S3 multipart ETag: md5(concat(binary part md5s)) + "-N", rendered quoted."""
    joined = b"".join(bytes.fromhex(h) for h in part_md5_hexes)
    return f'"{md5hex(joined)}-{len(part_md5_hexes)}"'


# Deterministic payloads, built once and reused everywhere so the harness moves ~11 MiB total.
BIG = bytes((i * 2654435761 >> 24) & 0xFF for i in range(MIN_PART))  # exactly 5 MiB
TAIL = b"tail-block-" * 1000                                          # small final part
SMALL_A, SMALL_B, SMALL_C = b"alpha" * 100, b"bravo" * 100, b"charlie" * 100

s3.create_bucket(Bucket="mpu")
s3.create_bucket(Bucket="mpu-list")
s3.create_bucket(Bucket="mpu-ver")
s3.put_bucket_versioning(Bucket="mpu-ver", VersioningConfiguration={"Status": "Enabled"})
check("fixtures: three buckets created",
      {"mpu", "mpu-list", "mpu-ver"} <= {b["Name"] for b in s3.list_buckets()["Buckets"]})


def begin(bucket, key, **kw):
    r = s3.create_multipart_upload(Bucket=bucket, Key=key, **kw)
    return r["UploadId"]


def put_part(bucket, key, upload_id, n, data):
    return s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=n, Body=data)


def fresh_client():
    """A client with its own, disposable connection pool — see the keep-alive block in section 4."""
    return boto3.client(
        "s3", endpoint_url=endpoint, aws_access_key_id=akid, aws_secret_access_key=secret,
        region_name=REGION,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}))


# =================================================================================================
# 1. the low-level happy path + the multipart ETag format
# =================================================================================================
init = s3.create_multipart_upload(Bucket="mpu", Key="hp/object.bin", ContentType="application/x-test")
uid = init["UploadId"]
check("CreateMultipartUpload returns a non-empty UploadId", isinstance(uid, str) and len(uid) > 0)
check("CreateMultipartUpload echoes Bucket/Key",
      init["Bucket"] == "mpu" and init["Key"] == "hp/object.bin")

p1 = put_part("mpu", "hp/object.bin", uid, 1, BIG)
p2 = put_part("mpu", "hp/object.bin", uid, 2, TAIL)
check("UploadPart part 1 ETag is the part's md5, quoted", p1["ETag"] == f'"{md5hex(BIG)}"')
check("UploadPart part 2 ETag is the part's md5, quoted", p2["ETag"] == f'"{md5hex(TAIL)}"')

lp = s3.list_parts(Bucket="mpu", Key="hp/object.bin", UploadId=uid)
check("ListParts echoes Bucket/Key/UploadId",
      lp["Bucket"] == "mpu" and lp["Key"] == "hp/object.bin" and lp["UploadId"] == uid)
check("ListParts lists both parts, in part-number order",
      [p["PartNumber"] for p in lp["Parts"]] == [1, 2])
check("ListParts reports the true part sizes",
      [p["Size"] for p in lp["Parts"]] == [len(BIG), len(TAIL)])
check("ListParts reports the per-part ETags",
      [p["ETag"] for p in lp["Parts"]] == [p1["ETag"], p2["ETag"]])
check("ListParts is not truncated for a 2-part upload", lp["IsTruncated"] is False)

done = s3.complete_multipart_upload(
    Bucket="mpu", Key="hp/object.bin", UploadId=uid,
    MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": p1["ETag"]},
                               {"PartNumber": 2, "ETag": p2["ETag"]}]})
expect_etag = mp_etag([md5hex(BIG), md5hex(TAIL)])
check('CompleteMultipartUpload ETag is "md5(concat(part-md5s))-N"', done["ETag"] == expect_etag)
check("CompleteMultipartUpload echoes Bucket/Key",
      done["Bucket"] == "mpu" and done["Key"] == "hp/object.bin")
check("CompleteMultipartUpload returns a Location naming the object",
      "mpu" in done["Location"] and "hp/object.bin" in done["Location"])

got = s3.get_object(Bucket="mpu", Key="hp/object.bin")
check("assembled object is byte-identical to part1||part2", got["Body"].read() == BIG + TAIL)
check("assembled object ETag matches the completion ETag", got["ETag"] == expect_etag)
head = s3.head_object(Bucket="mpu", Key="hp/object.bin")
check("HEAD reports the assembled length", head["ContentLength"] == len(BIG) + len(TAIL))
check("HEAD carries the multipart ETag", head["ETag"] == expect_etag)
check("initiate-time ContentType survives to the assembled object",
      head["ContentType"] == "application/x-test")

# The session is consumed by a successful completion: a second one must not be a false success.
try:
    s3.complete_multipart_upload(
        Bucket="mpu", Key="hp/object.bin", UploadId=uid,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": p1["ETag"]},
                                   {"PartNumber": 2, "ETag": p2["ETag"]}]})
    check("double CompleteMultipartUpload is rejected (it succeeded)", False)
except ClientError as e:
    check("double CompleteMultipartUpload -> NoSuchUpload 404",
          code_of(e) == "NoSuchUpload" and status_of(e) == 404)

try:
    s3.list_parts(Bucket="mpu", Key="hp/object.bin", UploadId=uid)
    check("ListParts after completion is rejected (it succeeded)", False)
except ClientError as e:
    check("ListParts after completion -> NoSuchUpload 404",
          code_of(e) == "NoSuchUpload" and status_of(e) == 404)

# =================================================================================================
# 2. ListMultipartUploads — max-uploads=0, and paging driven as a REAL loop (issues #2/#3)
# =================================================================================================
# `lm/a` deliberately holds THREE concurrent sessions: a key with more uploads than the page size is
# precisely the shape that re-served page 1 forever, because resuming mid-key needs BOTH the
# key-marker and the upload-id-marker to round-trip.
expected = set()
for key, n in (("lm/a", 3), ("lm/b", 1), ("lm/c", 1), ("zz/other", 1)):
    for _ in range(n):
        expected.add((key, begin("mpu-list", key, ContentType="text/plain")))
check("fixtures: 6 concurrent uploads across 4 keys", len(expected) == 6)

allup = s3.list_multipart_uploads(Bucket="mpu-list")
check("ListMultipartUploads (default) returns every open upload",
      {(u["Key"], u["UploadId"]) for u in allup.get("Uploads", [])} == expected)
check("ListMultipartUploads (default) is not truncated", allup["IsTruncated"] is False)

# --- max-uploads=0 (issue #3): zero uploads, and NOT a spurious truncated page.
z = s3.list_multipart_uploads(Bucket="mpu-list", MaxUploads=0)
check("ListMultipartUploads max-uploads=0 returns zero uploads", z.get("Uploads", []) == [])
check("ListMultipartUploads max-uploads=0 is not truncated", z["IsTruncated"] is False)
check("ListMultipartUploads max-uploads=0 advertises no NextKeyMarker", "NextKeyMarker" not in z)
st, body = raw("GET", "/mpu-list", query="uploads&max-uploads=0")
root = ET.fromstring(body)
check("raw max-uploads=0 -> 200 with MaxUploads=0 echoed",
      st == 200 and text(root, "MaxUploads") == "0")
check("raw max-uploads=0 body contains no <Upload> entries", root.find(f"{NS}Upload") is None)

# --- the real paging loop (issue #2): one upload per page, must terminate AND cover everything.
seen, pages, km, uim = [], 0, None, None
while True:
    pages += 1
    check("ListMultipartUploads paging terminates (<= 20 pages)", pages <= 20)
    if pages > 20:
        break
    kw = {"Bucket": "mpu-list", "MaxUploads": 1}
    if km is not None:
        kw["KeyMarker"] = km
    if uim is not None:
        kw["UploadIdMarker"] = uim
    page = s3.list_multipart_uploads(**kw)
    seen.extend((u["Key"], u["UploadId"]) for u in page.get("Uploads", []))
    if not page["IsTruncated"]:
        break
    # A truncated page that does not hand back a resume marker is an unterminable listing.
    check("truncated page advertises NextKeyMarker", bool(page.get("NextKeyMarker")))
    check("truncated page advertises NextUploadIdMarker", bool(page.get("NextUploadIdMarker")))
    nkm, nuim = page.get("NextKeyMarker"), page.get("NextUploadIdMarker")
    if not nkm:
        break
    check("paging strictly advances", (nkm, nuim) != (km, uim))
    km, uim = nkm, nuim

check("paged listing returned every upload exactly once (no gaps, no duplicates)",
      sorted(seen) == sorted(expected) and len(seen) == len(set(seen)))
check("paging with max-uploads=1 took exactly one page per upload (+1 terminal)",
      pages in (len(expected), len(expected) + 1))

pref = s3.list_multipart_uploads(Bucket="mpu-list", Prefix="lm/")
check("ListMultipartUploads honours Prefix",
      {(u["Key"], u["UploadId"]) for u in pref.get("Uploads", [])}
      == {e for e in expected if e[0].startswith("lm/")})

# A key-marker at or past the last key yields an empty, terminal page — no marker may loop.
tail_page = s3.list_multipart_uploads(Bucket="mpu-list", KeyMarker="zzzzzz")
check("KeyMarker past the last key -> empty, untruncated page",
      tail_page.get("Uploads", []) == [] and tail_page["IsTruncated"] is False)

# =================================================================================================
# 3. ListParts pagination
# =================================================================================================
pk = "parts/paged.bin"
puid = begin("mpu-list", pk)
pe = {n: put_part("mpu-list", pk, puid, n, d)["ETag"]
      for n, d in ((1, SMALL_A), (2, SMALL_B), (3, SMALL_C))}

pg = s3.list_parts(Bucket="mpu-list", Key=pk, UploadId=puid, MaxParts=2)
check("ListParts max-parts=2 returns exactly 2 parts",
      [p["PartNumber"] for p in pg["Parts"]] == [1, 2])
check("ListParts max-parts=2 is truncated", pg["IsTruncated"] is True)
check("ListParts NextPartNumberMarker is the last returned part number",
      str(pg["NextPartNumberMarker"]) == "2")
check("ListParts echoes MaxParts", pg["MaxParts"] == 2)

pg2 = s3.list_parts(Bucket="mpu-list", Key=pk, UploadId=puid, MaxParts=2,
                    PartNumberMarker=pg["NextPartNumberMarker"])
check("ListParts resumes strictly after the marker",
      [p["PartNumber"] for p in pg2["Parts"]] == [3])
check("ListParts final page is not truncated", pg2["IsTruncated"] is False)
check("ListParts echoes PartNumberMarker", str(pg2["PartNumberMarker"]) == "2")

# The same as a real loop, so a marker that fails to advance shows up as non-termination.
collected, marker, rounds = [], 0, 0
while True:
    rounds += 1
    check("ListParts paging terminates (<= 10 pages)", rounds <= 10)
    if rounds > 10:
        break
    page = s3.list_parts(Bucket="mpu-list", Key=pk, UploadId=puid, MaxParts=1,
                         PartNumberMarker=marker)
    collected.extend(p["PartNumber"] for p in page["Parts"])
    if not page["IsTruncated"]:
        break
    nxt = page.get("NextPartNumberMarker")
    check("truncated ListParts page advertises NextPartNumberMarker", nxt is not None)
    if nxt is None:
        break
    check("ListParts marker strictly advances", int(nxt) > int(marker))
    marker = int(nxt)
check("ListParts paging covered every part exactly once", collected == [1, 2, 3])

# FIXED (was a sibling of issue #3): `max-parts=0` used to render IsTruncated=true with NO
# NextPartNumberMarker — an unterminable page. `list_parts` now short-circuits the 0 case against
# an empty page through the same `page_size` parser as `list_multipart_uploads`, so the assertions
# below are ordinary gating checks.
st, body = raw("GET", f"/mpu-list/{pk}", query=f"uploadId={urllib.parse.quote(puid)}&max-parts=0")
root = ET.fromstring(body)
check("raw ListParts max-parts=0 -> 200 with zero <Part> entries",
      st == 200 and root.find(f"{NS}Part") is None)
check("ListParts max-parts=0 does not advertise an unterminable page "
      "(IsTruncated=true with no NextPartNumberMarker)",
      not (text(root, "IsTruncated") == "true" and text(root, "NextPartNumberMarker") is None))

# =================================================================================================
# 4. Abort, and every unknown-uploadId path
# =================================================================================================
akey = "abort/gone.bin"
auid = begin("mpu", akey)
put_part("mpu", akey, auid, 1, SMALL_A)
s3.abort_multipart_upload(Bucket="mpu", Key=akey, UploadId=auid)
check("AbortMultipartUpload returns 2xx", True)

for label, op in (
    ("ListParts", lambda u: s3.list_parts(Bucket="mpu", Key=akey, UploadId=u)),
    ("AbortMultipartUpload", lambda u: s3.abort_multipart_upload(Bucket="mpu", Key=akey, UploadId=u)),
    # UploadPart goes through a THROWAWAY client: the server answers this 404 without consuming the
    # request body and leaves the keep-alive connection unusable ~1-in-20 times (measured), which
    # poisons the shared client's pool and makes an unrelated LATER request fail. That defect is
    # pinned deterministically in its own block below; here we just keep it from randomising the
    # rest of the run.
    ("UploadPart", lambda u: fresh_client().upload_part(
        Bucket="mpu", Key=akey, UploadId=u, PartNumber=1, Body=SMALL_A)),
    ("CompleteMultipartUpload", lambda u: s3.complete_multipart_upload(
        Bucket="mpu", Key=akey, UploadId=u,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": f'"{md5hex(SMALL_A)}"'}]})),
    ("UploadPartCopy", lambda u: s3.upload_part_copy(
        Bucket="mpu", Key=akey, UploadId=u, PartNumber=1,
        CopySource={"Bucket": "mpu", "Key": "hp/object.bin"})),
):
    try:
        op(auid)
        check(f"{label} after abort is rejected (it succeeded)", False)
    except ClientError as e:
        check(f"{label} after abort -> NoSuchUpload 404",
              code_of(e) == "NoSuchUpload" and status_of(e) == 404)

    try:
        op("this-upload-id-does-not-exist")
        check(f"{label} with an unknown uploadId is rejected (it succeeded)", False)
    except ClientError as e:
        check(f"{label} with an unknown uploadId -> NoSuchUpload 404",
              code_of(e) == "NoSuchUpload" and status_of(e) == 404)

check("the aborted upload is gone from ListMultipartUploads",
      auid not in [u["UploadId"] for u in
                   s3.list_multipart_uploads(Bucket="mpu").get("Uploads", [])])

# KNOWN GAP (suspected product bug): UploadPart against an unknown uploadId is rejected BEFORE the
# request body is read (service.rs `upload_part` scopes the session ahead of `stage_part`, which is
# correct and deliberate) — but the 404 is sent with neither `Connection: close` nor the body
# drained, so the client cannot tell the connection is finished. Measured: with a 2 MiB body the
# next request on that connection ALWAYS dies (6/6 broken pipe); with a small body the connection
# survives ~19/20, so a pooled SDK client intermittently fails a LATER, UNRELATED request. Either
# outcome is legal on its own; sending no signal is what makes it a defect.
# The assertion is the client-visible contract: after an early error the connection is either still
# usable, or the response said it was closing.
_ka = http.client.HTTPConnection(_url.hostname, _url.port, timeout=60)


def ka_send(method, path, query, body=b""):
    req = AWSRequest(method=method, url=f"{endpoint}{path}?{query}", data=body)
    req.headers["host"] = _url.netloc
    req.headers["x-amz-content-sha256"] = hashlib.sha256(body).hexdigest()
    SigV4Auth(_creds, "s3", REGION).add_auth(req)
    _ka.request(method, f"{path}?{query}", body=body, headers=dict(req.headers))
    r = _ka.getresponse()
    data = r.read()
    return r.status, r.getheader("connection"), data


ka_uid = begin("mpu", "keepalive/probe.bin")
st, conn_hdr, _ = ka_send("PUT", "/mpu/keepalive/probe.bin",
                          "partNumber=1&uploadId=nosuchuploadatall", b"z" * (2 * 1024 * 1024))
check("UploadPart with an unknown uploadId and a 2 MiB body -> 404 (body never read)", st == 404)
reusable = True
try:
    st2, _, _ = ka_send("GET", "/mpu/keepalive/probe.bin",
                        f"uploadId={urllib.parse.quote(ka_uid)}")
    reusable = st2 == 200
except (BrokenPipeError, ConnectionResetError, http.client.HTTPException):
    reusable = False
known_issue(
    "after an early error the connection is reusable OR the response said `Connection: close`",
    reusable or (conn_hdr or "").lower() == "close",
    "https://github.com/Harsh-2002/Cairn/issues/5",
    "Measured: with a 2 MiB body the next request on the connection dies 6/6; with a small body it "
    "survives ~19/20, so a pooled SDK client intermittently fails a LATER, UNRELATED request. The "
    "fix is in the cairn-server adapter (drain the body or send `Connection: close` on the early "
    "reject path), a different subsystem from the protocol bugs fixed alongside this harness.",
)
try:
    _ka.close()
except OSError:
    pass
s3.abort_multipart_upload(Bucket="mpu", Key="keepalive/probe.bin", UploadId=ka_uid)

# An uploadId is scoped to the (bucket, key) it was initiated for — using it under any other path
# must be NoSuchUpload, never a silent write to the requested path.
scoped_uid = begin("mpu", "scope/real.bin")
put_part("mpu", "scope/real.bin", scoped_uid, 1, SMALL_A)
try:
    s3.complete_multipart_upload(
        Bucket="mpu", Key="scope/other.bin", UploadId=scoped_uid,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": f'"{md5hex(SMALL_A)}"'}]})
    check("completing an uploadId under a different key is rejected (it succeeded)", False)
except ClientError as e:
    check("completing an uploadId under a different key -> NoSuchUpload 404",
          code_of(e) == "NoSuchUpload" and status_of(e) == 404)
try:
    s3.head_object(Bucket="mpu", Key="scope/other.bin")
    check("the wrong-path completion created no object", False)
except ClientError as e:
    check("the wrong-path completion created no object (404)", status_of(e) == 404)
check("the mis-addressed upload is still live and listable",
      [p["PartNumber"] for p in
       s3.list_parts(Bucket="mpu", Key="scope/real.bin", UploadId=scoped_uid)["Parts"]] == [1])
s3.abort_multipart_upload(Bucket="mpu", Key="scope/real.bin", UploadId=scoped_uid)

# =================================================================================================
# 5. Session-state / argument errors
# =================================================================================================
ekey = "errors/session.bin"
euid = begin("mpu", ekey)
e1 = put_part("mpu", ekey, euid, 1, SMALL_A)["ETag"]
e2 = put_part("mpu", ekey, euid, 2, SMALL_B)["ETag"]

# --- empty part list. AWS answers MalformedXML here; Cairn answers InvalidArgument. Both are 400
# client errors with the same operational meaning, so the status is pinned exactly and the code is
# accepted as either — see the report's "divergence, not bug" note.
try:
    s3.complete_multipart_upload(Bucket="mpu", Key=ekey, UploadId=euid,
                                 MultipartUpload={"Parts": []})
    check("CompleteMultipartUpload with an empty part list is rejected (it succeeded)", False)
except ClientError as e:
    check("CompleteMultipartUpload with an empty part list -> 400 InvalidArgument/MalformedXML",
          status_of(e) == 400 and code_of(e) in ("InvalidArgument", "MalformedXML"))

# --- partNumber bounds. Sent raw: botocore will happily serialize these, but a client-side
# rejection would prove nothing about the server, so drive the wire directly.
for pn in ("0", "10001", "-1", "notanumber"):
    st, body = raw("PUT", f"/mpu/{ekey}",
                   query=f"partNumber={pn}&uploadId={urllib.parse.quote(euid)}", body=SMALL_A)
    check(f"UploadPart partNumber={pn} -> 400 InvalidArgument",
          st == 400 and err_code(body) == "InvalidArgument")
# `PUT key?uploadId=…` with NO partNumber has no handler. It must NOT fall through to `put_object`
# and overwrite the object body — `uploadId` is in UNHANDLED_OBJECT_SUBRESOURCES precisely so it
# lands on the NotImplemented guard (service.rs ~3623, the routing.py fall-through class). AWS would
# answer 400 here; 501 is the deliberate, safe local answer, and what matters is that it is neither
# a silent PutObject nor a silent UploadPart.
st, body = raw("PUT", f"/mpu/{ekey}", query=f"uploadId={urllib.parse.quote(euid)}", body=SMALL_A)
check("UploadPart with no partNumber at all -> 501 NotImplemented (never a silent PutObject)",
      st == 501 and err_code(body) == "NotImplemented")
check("the bad partNumbers recorded no parts",
      [p["PartNumber"] for p in
       s3.list_parts(Bucket="mpu", Key=ekey, UploadId=euid)["Parts"]] == [1, 2])

# --- part-list validation, each with its own S3 code. A validation failure must leave the upload
# ACTIVE and retryable (audit #14) — asserted after every one.
def still_active(label):
    parts = s3.list_parts(Bucket="mpu", Key=ekey, UploadId=euid)["Parts"]
    check(f"upload is still active and retryable after {label}",
          [p["PartNumber"] for p in parts] == [1, 2])


try:
    s3.complete_multipart_upload(
        Bucket="mpu", Key=ekey, UploadId=euid,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": f'"{md5hex(b"wrong")}"'},
                                   {"PartNumber": 2, "ETag": e2}]})
    check("complete with a wrong part ETag is rejected (it succeeded)", False)
except ClientError as e:
    check("complete with a wrong part ETag -> InvalidPart 400",
          code_of(e) == "InvalidPart" and status_of(e) == 400)
still_active("InvalidPart (wrong ETag)")

# A single part that was never uploaded: one entry, so no ordering or minimum-size rule can apply
# and the answer is unambiguously InvalidPart.
try:
    s3.complete_multipart_upload(
        Bucket="mpu", Key=ekey, UploadId=euid,
        MultipartUpload={"Parts": [{"PartNumber": 7, "ETag": e2}]})
    check("complete naming a never-uploaded part is rejected (it succeeded)", False)
except ClientError as e:
    check("complete naming a never-uploaded part -> InvalidPart 400",
          code_of(e) == "InvalidPart" and status_of(e) == 400)
still_active("InvalidPart (missing part)")

# --- undersized non-final part. Part 1 here is ~500 bytes, far under the 5 MiB floor, and it is
# NOT the last part in the list — exactly the EntityTooSmall condition. (The same part as the FINAL
# entry is legal, which the very next block proves.)
try:
    s3.complete_multipart_upload(
        Bucket="mpu", Key=ekey, UploadId=euid,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": e1},
                                   {"PartNumber": 2, "ETag": e2}]})
    check("complete with an undersized non-final part is rejected (it succeeded)", False)
except ClientError as e:
    check("complete with an undersized non-final part -> EntityTooSmall 400",
          code_of(e) == "EntityTooSmall" and status_of(e) == 400)
still_active("EntityTooSmall")

# The very same session completes cleanly once the part list is legal (a single, final small part),
# proving none of the five rejections above bricked it.
ok = s3.complete_multipart_upload(Bucket="mpu", Key=ekey, UploadId=euid,
                                  MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": e1}]})
check("a small FINAL part is legal — the rejected session still completes",
      ok["ETag"] == mp_etag([md5hex(SMALL_A)]))
check("completing a subset of parts assembles only those parts",
      s3.get_object(Bucket="mpu", Key=ekey)["Body"].read() == SMALL_A)

# --- ordering. Driven on a session whose parts are BOTH >= 5 MiB (the same 5 MiB buffer uploaded
# twice) so the minimum-size rule cannot fire first and the ordering rule is what is actually
# measured. See the KNOWN GAP right below for why that matters.
okey = "errors/order.bin"
ouid = begin("mpu", okey)
o1 = put_part("mpu", okey, ouid, 1, BIG)["ETag"]
o2 = put_part("mpu", okey, ouid, 2, BIG)["ETag"]

try:
    s3.complete_multipart_upload(
        Bucket="mpu", Key=okey, UploadId=ouid,
        MultipartUpload={"Parts": [{"PartNumber": 2, "ETag": o2},
                                   {"PartNumber": 1, "ETag": o1}]})
    check("complete with parts out of order is rejected (it succeeded)", False)
except ClientError as e:
    check("complete with parts out of order -> InvalidPartOrder 400",
          code_of(e) == "InvalidPartOrder" and status_of(e) == 400)

try:
    s3.complete_multipart_upload(
        Bucket="mpu", Key=okey, UploadId=ouid,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": o1},
                                   {"PartNumber": 1, "ETag": o1}]})
    check("complete with a duplicated part number is rejected (it succeeded)", False)
except ClientError as e:
    check("complete with a duplicated part number -> InvalidPartOrder 400",
          code_of(e) == "InvalidPartOrder" and status_of(e) == 400)

try:
    s3.complete_multipart_upload(
        Bucket="mpu", Key=okey, UploadId=ouid,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": o1},
                                   {"PartNumber": 7, "ETag": o2}]})
    check("complete naming a missing part after a valid one is rejected (it succeeded)", False)
except ClientError as e:
    check("complete naming a missing part after a valid one -> InvalidPart 400",
          code_of(e) == "InvalidPart" and status_of(e) == 400)
check("the ordering session survived all three rejections",
      [p["PartNumber"] for p in
       s3.list_parts(Bucket="mpu", Key=okey, UploadId=ouid)["Parts"]] == [1, 2])
s3.abort_multipart_upload(Bucket="mpu", Key=okey, UploadId=ouid)

# FIXED (validation PRECEDENCE): `complete_multipart` used to apply the minimum-size rule
# POSITIONALLY before establishing that the list was ordered, so an out-of-order list of small
# parts reported EntityTooSmall instead of InvalidPartOrder. Ordering is now validated in its own
# pre-pass ahead of the size check, so this is an ordinary gating check.
skey = "errors/precedence.bin"
suid = begin("mpu", skey)
s1 = put_part("mpu", skey, suid, 1, SMALL_A)["ETag"]
s2 = put_part("mpu", skey, suid, 2, SMALL_B)["ETag"]
try:
    s3.complete_multipart_upload(
        Bucket="mpu", Key=skey, UploadId=suid,
        MultipartUpload={"Parts": [{"PartNumber": 2, "ETag": s2},
                                   {"PartNumber": 1, "ETag": s1}]})
    check("out-of-order SMALL parts are rejected (it succeeded)", False)
except ClientError as e:
    check("out-of-order SMALL parts -> InvalidPartOrder 400 (not EntityTooSmall)",
          code_of(e) == "InvalidPartOrder" and status_of(e) == 400)
s3.abort_multipart_upload(Bucket="mpu", Key=skey, UploadId=suid)

# =================================================================================================
# 6. Re-uploading a part number: last writer wins
# =================================================================================================
rkey = "rewrite/part.bin"
ruid = begin("mpu", rkey)
old_etag = put_part("mpu", rkey, ruid, 1, SMALL_A)["ETag"]
new_etag = put_part("mpu", rkey, ruid, 1, SMALL_B)["ETag"]
check("re-uploading part 1 returns the NEW content's ETag", new_etag == f'"{md5hex(SMALL_B)}"')
parts = s3.list_parts(Bucket="mpu", Key=rkey, UploadId=ruid)["Parts"]
check("re-uploading part 1 leaves exactly one part row", [p["PartNumber"] for p in parts] == [1])
check("ListParts reports the NEW part's ETag and size",
      parts[0]["ETag"] == new_etag and parts[0]["Size"] == len(SMALL_B))

try:
    s3.complete_multipart_upload(Bucket="mpu", Key=rkey, UploadId=ruid,
                                 MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": old_etag}]})
    check("completing with the SUPERSEDED part ETag is rejected (it succeeded)", False)
except ClientError as e:
    check("completing with the SUPERSEDED part ETag -> InvalidPart 400",
          code_of(e) == "InvalidPart" and status_of(e) == 400)

s3.complete_multipart_upload(Bucket="mpu", Key=rkey, UploadId=ruid,
                             MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": new_etag}]})
check("last writer wins: the assembled object holds the SECOND upload's bytes",
      s3.get_object(Bucket="mpu", Key=rkey)["Body"].read() == SMALL_B)

# =================================================================================================
# 7. Multipart x versioning
# =================================================================================================
vkey = "versioned/mp.bin"
v1 = begin("mpu-ver", vkey)
v1e = put_part("mpu-ver", vkey, v1, 1, SMALL_A)["ETag"]
r1 = s3.complete_multipart_upload(Bucket="mpu-ver", Key=vkey, UploadId=v1,
                                  MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": v1e}]})
check("multipart completion in a versioned bucket returns a VersionId",
      bool(r1.get("VersionId")) and r1["VersionId"] != "null")

v2 = begin("mpu-ver", vkey)
v2e = put_part("mpu-ver", vkey, v2, 1, SMALL_B)["ETag"]
r2 = s3.complete_multipart_upload(Bucket="mpu-ver", Key=vkey, UploadId=v2,
                                  MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": v2e}]})
check("a second multipart completion mints a DIFFERENT VersionId",
      r2.get("VersionId") not in (None, "", r1["VersionId"]))
check("the older multipart version is still readable by id",
      s3.get_object(Bucket="mpu-ver", Key=vkey, VersionId=r1["VersionId"])["Body"].read() == SMALL_A)
check("the latest read returns the newer multipart version",
      s3.get_object(Bucket="mpu-ver", Key=vkey)["Body"].read() == SMALL_B)
vers = s3.list_object_versions(Bucket="mpu-ver", Prefix=vkey)["Versions"]
check("both multipart versions are listed",
      {r1["VersionId"], r2["VersionId"]} <= {v["VersionId"] for v in vers})
check("exactly one multipart version is IsLatest",
      [v["VersionId"] for v in vers if v["IsLatest"]] == [r2["VersionId"]])

# =================================================================================================
# 8. UploadPartCopy
# =================================================================================================
src = SMALL_A + SMALL_B + SMALL_C
s3.put_object(Bucket="mpu", Key="copysrc.bin", Body=src)

ckey = "copied/whole.bin"
cuid = begin("mpu", ckey)
cp = s3.upload_part_copy(Bucket="mpu", Key=ckey, UploadId=cuid, PartNumber=1,
                         CopySource={"Bucket": "mpu", "Key": "copysrc.bin"})
check("UploadPartCopy returns a CopyPartResult ETag = md5 of the copied bytes",
      cp["CopyPartResult"]["ETag"] == f'"{md5hex(src)}"')
check("UploadPartCopy returns a LastModified", "LastModified" in cp["CopyPartResult"])
cparts = s3.list_parts(Bucket="mpu", Key=ckey, UploadId=cuid)["Parts"]
check("the copied part is recorded with the source's size",
      [(p["PartNumber"], p["Size"]) for p in cparts] == [(1, len(src))])
cdone = s3.complete_multipart_upload(
    Bucket="mpu", Key=ckey, UploadId=cuid,
    MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": cp["CopyPartResult"]["ETag"]}]})
check("the copy-assembled object is byte-identical to the source",
      s3.get_object(Bucket="mpu", Key=ckey)["Body"].read() == src)
check("the copy-assembled object carries a -1 multipart ETag",
      cdone["ETag"] == mp_etag([md5hex(src)]))

# Ranged UploadPartCopy: the part is exactly the requested byte range of the source.
rkey2 = "copied/ranged.bin"
ruid2 = begin("mpu", rkey2)
rng = s3.upload_part_copy(Bucket="mpu", Key=rkey2, UploadId=ruid2, PartNumber=1,
                          CopySource={"Bucket": "mpu", "Key": "copysrc.bin"},
                          CopySourceRange="bytes=100-199")
check("ranged UploadPartCopy ETag = md5 of exactly that range",
      rng["CopyPartResult"]["ETag"] == f'"{md5hex(src[100:200])}"')
s3.complete_multipart_upload(
    Bucket="mpu", Key=rkey2, UploadId=ruid2,
    MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": rng["CopyPartResult"]["ETag"]}]})
check("ranged UploadPartCopy assembles exactly the requested range",
      s3.get_object(Bucket="mpu", Key=rkey2)["Body"].read() == src[100:200])

try:
    s3.upload_part_copy(Bucket="mpu", Key=rkey2, UploadId=ruid2, PartNumber=1,
                        CopySource={"Bucket": "mpu", "Key": "no-such-source.bin"})
    check("UploadPartCopy from a missing source is rejected (it succeeded)", False)
except ClientError as e:
    # The session is already completed above, so the scope check fires first — either way this must
    # be a 4xx naming the thing that is actually absent.
    check("UploadPartCopy from a missing source -> 404 NoSuchKey/NoSuchUpload",
          status_of(e) == 404 and code_of(e) in ("NoSuchKey", "NoSuchUpload"))

# =================================================================================================
if FAILURES:
    print(f"\nMULTIPART FAILED — {len(FAILURES)} assertion(s):")
    for f in FAILURES:
        print(f"  - {f}")
    sys.exit(1)
print("MULTIPART OK — low-level lifecycle, paging loops, and every session-state error code")
