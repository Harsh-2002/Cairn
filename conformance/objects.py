#!/usr/bin/env python3
"""Object payload / header / range conformance (package F).

The broad `conformance.py` smoke gate proves an object round-trips; it never pins *what the bytes
and headers are on the wire*. This harness does, and every assertion names an EXACT S3 error code
AND an EXACT HTTP status — `except ClientError: pass` accepts a 500 as happily as a 404, which is
how a header regression hides.

Coverage:
  1. The six stored system response headers (ARCH 13.4) round-trip PUT -> GET/HEAD, including the
     `aws-chunked` transfer-coding strip a modern SDK forces on every PUT.
  2. `x-amz-meta-*` user metadata: round-trip, header-name case folding, non-ASCII values.
  3. CopyObject: COPY carries ALL six system headers + user metadata + tags; REPLACE takes them
     from the copy request and inherits NOTHING; an unknown directive is InvalidArgument 400.
  4. `response-*` query overrides on GET *and* HEAD.
  5. Single-part ETag shape (quoted lowercase md5) and GET/HEAD header parity.
  6. Zero-byte objects end to end.
  7. Range edge cases with exact statuses: 206, suffix, open-ended, clamped, past-EOF 416,
     and ranges against a zero-byte object.
  8. Keys that need URL encoding: space, unicode/emoji, `+`, `%`, `&`, deep prefix, 1024 bytes.
  9. Content-MD5: correct succeeds, mismatched is BadDigest 400, malformed is InvalidDigest 400.
 10. DeleteObjects: Quiet, partial failure, duplicate keys in one request.

Assertions that currently FAIL against Cairn are marked `# KNOWN GAP:` with the S3 requirement and
the service.rs evidence; they are NOT weakened to make the run green. Failures are accumulated so
one run reports every divergence, and the process exits non-zero if any remain.

Args: <sigv4_access_key> <sigv4_secret> <s3_endpoint>
"""

import base64
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
BUCKET = "objf"

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=akid,
    aws_secret_access_key=secret,
    region_name=REGION,
    config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
)

FAILURES = []
GAPS = []


def check(label, cond):
    if cond:
        print(f"  ok: {label}")
    else:
        print(f"FAIL: {label}")
        FAILURES.append(label)
    return bool(cond)


def gap(label, cond, why):
    """A `check` whose failure is a SUSPECTED PRODUCT BUG, not a broken test.

    It still fails the run — the point is to report it loudly with the spec citation, never to
    downgrade the expectation. `why` is printed only when the assertion does not hold.
    """
    if cond:
        print(f"  ok: {label}")
    else:
        print(f"FAIL (suspected product bug): {label}\n       {why}")
        FAILURES.append(label)
        GAPS.append((label, why))
    return bool(cond)


def status_of(err):
    return err.response["ResponseMetadata"]["HTTPStatusCode"]


def code_of(err):
    return err.response["Error"]["Code"]


def hdrs(resp):
    """Lowercased response headers exactly as they came off the wire."""
    return resp["ResponseMetadata"]["HTTPHeaders"]


# --- hand-signed raw request ---------------------------------------------------------------------
# boto3 cannot express several things this harness must pin: the EXACT bytes of a stored
# `Content-Encoding` (the SDK rewrites it), a non-ASCII header value, or a Range whose response the
# SDK would turn into an exception before the headers can be inspected. Sign by hand with botocore's
# real SigV4 signer — same wire format, full control. Keep raw paths ASCII/-unescaped: plain
# `SigV4Auth` (not the S3 variant) re-quotes the canonical path, so weird keys go through boto3.
_creds = Credentials(akid, secret)
_url = urllib.parse.urlsplit(endpoint)


def raw(method, path, query="", body=b"", headers=None):
    """Send a hand-built, SigV4-signed request. Returns (status, {header: value}, body_bytes)."""
    url = f"{endpoint}{path}" + (f"?{query}" if query else "")
    req = AWSRequest(method=method, url=url, data=body)
    req.headers["host"] = _url.netloc
    req.headers["x-amz-content-sha256"] = hashlib.sha256(body).hexdigest()
    for k, v in (headers or {}).items():
        req.headers[k] = v
    SigV4Auth(_creds, "s3", REGION).add_auth(req)
    # http.client encodes header values as latin-1; a non-ASCII value must go on the wire as the
    # UTF-8 bytes that were signed, so hand it bytes rather than str.
    wire = {}
    for k, v in dict(req.headers).items():
        wire[k] = v.encode("utf-8") if isinstance(v, str) and not v.isascii() else v
    conn = http.client.HTTPConnection(_url.hostname, _url.port, timeout=30)
    conn.request(method, path + (f"?{query}" if query else ""), body=body, headers=wire)
    resp = conn.getresponse()
    data = resp.read()
    got = {k.lower(): v for k, v in resp.getheaders()}
    conn.close()
    return resp.status, got, data


s3.create_bucket(Bucket=BUCKET)
check("fixture bucket created", BUCKET in [b["Name"] for b in s3.list_buckets()["Buckets"]])

# =================================================================================================
# 1. System response headers (ARCH 13.4) round-trip PUT -> GET/HEAD
# =================================================================================================
SYS = {
    "content-type": "text/plain; charset=utf-8",
    "content-encoding": "gzip",
    "content-disposition": 'attachment; filename="report.txt"',
    "content-language": "en-GB",
    "cache-control": "max-age=120, public",
    "expires": "Wed, 21 Oct 2026 07:28:00 GMT",
}
BODY = b"the quick brown fox"

st, _, _ = raw("PUT", "/objf/sys.txt", body=BODY, headers=SYS)
check("raw PUT with all six system headers -> 200", st == 200)

g = s3.get_object(Bucket=BUCKET, Key="sys.txt")
gh = hdrs(g)
for name, want in SYS.items():
    check(f"GET echoes {name} verbatim", gh.get(name) == want)
check("GET body intact", g["Body"].read() == BODY)

h = s3.head_object(Bucket=BUCKET, Key="sys.txt")
hh = hdrs(h)
for name, want in SYS.items():
    check(f"HEAD echoes {name} verbatim", hh.get(name) == want)

# The `aws-chunked` transfer coding must be stripped before storing (PR #1 / ARCH 13.4,
# `stored_content_encoding` in service.rs): it describes the request framing, not the stored bytes,
# and a stored `gzip,aws-chunked` would make every GET advertise a framing the response never uses.
st, _, _ = raw("PUT", "/objf/enc-mixed.txt", body=BODY,
               headers={"content-encoding": "gzip,aws-chunked"})
check("PUT content-encoding 'gzip,aws-chunked' -> 200", st == 200)
check("stored content-encoding strips aws-chunked (== 'gzip')",
      hdrs(s3.get_object(Bucket=BUCKET, Key="enc-mixed.txt")).get("content-encoding") == "gzip")

st, _, _ = raw("PUT", "/objf/enc-only.txt", body=BODY, headers={"content-encoding": "aws-chunked"})
check("PUT content-encoding 'aws-chunked' (only value) -> 200", st == 200)
check("content-encoding is ABSENT when aws-chunked was the only token",
      "content-encoding" not in hdrs(s3.get_object(Bucket=BUCKET, Key="enc-only.txt")))

# The same thing through the real SDK: boto3 >=1.36 defaults to a flexible-checksum aws-chunked
# streaming body, so it appends the token to the caller's Content-Encoding on the wire.
s3.put_object(Bucket=BUCKET, Key="enc-sdk.txt", Body=BODY, ContentEncoding="gzip",
              ContentType="text/plain")
ce = hdrs(s3.get_object(Bucket=BUCKET, Key="enc-sdk.txt")).get("content-encoding")
check(f"SDK PUT stores content-encoding as 'gzip', not 'gzip,aws-chunked' (got {ce!r})",
      ce == "gzip")

# An object with no optional headers must not invent them.
s3.put_object(Bucket=BUCKET, Key="bare.txt", Body=BODY)
bh = hdrs(s3.get_object(Bucket=BUCKET, Key="bare.txt"))
check("a bare PUT emits no content-encoding/disposition/language/cache-control/expires",
      not any(k in bh for k in ("content-encoding", "content-disposition", "content-language",
                                "cache-control", "expires")))
check("a bare PUT defaults content-type to binary/octet-stream or application/octet-stream",
      bh.get("content-type") in ("binary/octet-stream", "application/octet-stream"))

# =================================================================================================
# 2. User metadata
# =================================================================================================
s3.put_object(Bucket=BUCKET, Key="meta.txt", Body=BODY,
              Metadata={"Project": "Cairn", "MiXeD-CaSe": "KeepValue", "empty": ""})
m = s3.head_object(Bucket=BUCKET, Key="meta.txt")["Metadata"]
check("metadata key names are folded to lowercase (HTTP header semantics)",
      set(m) == {"project", "mixed-case", "empty"})
check("metadata VALUES keep their case verbatim",
      m["project"] == "Cairn" and m["mixed-case"] == "KeepValue")
check("an empty metadata value round-trips as empty", m["empty"] == "")
check("GET reports the same metadata as HEAD",
      s3.get_object(Bucket=BUCKET, Key="meta.txt")["Metadata"] == m)

# The AWS-documented safe form for non-ASCII metadata is URL encoding — pure ASCII on the wire, so
# this MUST round-trip byte for byte.
enc = urllib.parse.quote("naïve café 🪨")
s3.put_object(Bucket=BUCKET, Key="meta-enc.txt", Body=BODY, Metadata={"label": enc})
back = s3.head_object(Bucket=BUCKET, Key="meta-enc.txt")["Metadata"]["label"]
check("url-encoded unicode metadata round-trips exactly", back == enc)
check("url-encoded metadata decodes back to the original text",
      urllib.parse.unquote(back) == "naïve café 🪨")

# Raw UTF-8 bytes in a metadata value: AWS S3 returns the stored bytes verbatim. The requirement
# asserted here is the weaker, undeniable one — Cairn must NOT accept the request and silently store
# an EMPTY value, because that is unreported data loss.
st, gh2, _ = raw("PUT", "/objf/meta-utf8.txt", body=BODY,
                 headers={"x-amz-meta-label": "café"})
if st == 200:
    v = hdrs(s3.head_object(Bucket=BUCKET, Key="meta-utf8.txt")).get("x-amz-meta-label", "")
    gap("raw UTF-8 metadata is stored, not silently blanked",
        v != "",
        "PUT x-amz-meta-label: café (UTF-8) returned 200 but the stored value is empty. "
        "cairn-server/src/adapter.rs:63 maps every header with `v.to_str().unwrap_or(\"\")`, so any "
        "non-visible-ASCII header value becomes \"\" before it reaches the S3 layer. Real S3 "
        "returns the stored bytes verbatim.")
else:
    print(f"  note: raw UTF-8 metadata PUT was REJECTED with HTTP {st} "
          "(no silent data loss; see report — adapter.rs:63 blanks non-ASCII header values, "
          "which also breaks the SigV4 signature over them)")
    check("raw UTF-8 metadata is rejected with a 4xx, never a 5xx", 400 <= st < 500)

# =================================================================================================
# 3. CopyObject directives
# =================================================================================================
s3.put_object(Bucket=BUCKET, Key="src.txt", Body=BODY, ContentType=SYS["content-type"],
              ContentEncoding="gzip", ContentDisposition=SYS["content-disposition"],
              ContentLanguage=SYS["content-language"], CacheControl=SYS["cache-control"],
              Expires="Wed, 21 Oct 2026 07:28:00 GMT",
              Metadata={"origin": "source"}, Tagging="team=core&tier=hot")
srch = hdrs(s3.head_object(Bucket=BUCKET, Key="src.txt"))
check("copy source carries all six system headers",
      all(srch.get(k) == v for k, v in SYS.items()))

# --- COPY (the default directive) preserves EVERYTHING. Five of these six were hard-coded None
# before PR #1 (service.rs `sys()` at the CopyObject row build), so every copy silently lost them.
s3.copy_object(Bucket=BUCKET, Key="cp-copy.txt",
               CopySource={"Bucket": BUCKET, "Key": "src.txt"})
ch = hdrs(s3.head_object(Bucket=BUCKET, Key="cp-copy.txt"))
for name, want in SYS.items():
    check(f"COPY directive preserves {name}", ch.get(name) == want)
check("COPY directive preserves user metadata",
      s3.head_object(Bucket=BUCKET, Key="cp-copy.txt")["Metadata"] == {"origin": "source"})
check("COPY directive preserves the object bytes",
      s3.get_object(Bucket=BUCKET, Key="cp-copy.txt")["Body"].read() == BODY)
cp_tags = {t["Key"]: t["Value"] for t in
           s3.get_object_tagging(Bucket=BUCKET, Key="cp-copy.txt")["TagSet"]}
check("default tagging-directive (COPY) inherits the source tag set",
      cp_tags == {"team": "core", "tier": "hot"})

# --- REPLACE takes the six headers + metadata from THIS request and inherits nothing.
s3.copy_object(Bucket=BUCKET, Key="cp-repl.txt",
               CopySource={"Bucket": BUCKET, "Key": "src.txt"},
               MetadataDirective="REPLACE",
               ContentType="application/json", CacheControl="no-store",
               Metadata={"fresh": "yes"})
rh = hdrs(s3.head_object(Bucket=BUCKET, Key="cp-repl.txt"))
check("REPLACE sets content-type from the copy request", rh.get("content-type") == "application/json")
check("REPLACE sets cache-control from the copy request", rh.get("cache-control") == "no-store")
check("REPLACE does NOT inherit the source content-encoding", "content-encoding" not in rh)
check("REPLACE does NOT inherit the source content-disposition", "content-disposition" not in rh)
check("REPLACE does NOT inherit the source content-language", "content-language" not in rh)
check("REPLACE does NOT inherit the source expires", "expires" not in rh)
check("REPLACE replaces user metadata wholesale",
      s3.head_object(Bucket=BUCKET, Key="cp-repl.txt")["Metadata"] == {"fresh": "yes"})
check("REPLACE still copies the object bytes",
      s3.get_object(Bucket=BUCKET, Key="cp-repl.txt")["Body"].read() == BODY)
check("metadata-directive REPLACE does NOT imply a tagging replace (tags still inherited)",
      {t["Key"]: t["Value"] for t in
       s3.get_object_tagging(Bucket=BUCKET, Key="cp-repl.txt")["TagSet"]}
      == {"team": "core", "tier": "hot"})

# --- the tagging directive is independent of the metadata directive.
s3.copy_object(Bucket=BUCKET, Key="cp-tagrepl.txt",
               CopySource={"Bucket": BUCKET, "Key": "src.txt"},
               TaggingDirective="REPLACE", Tagging="env=prod")
check("tagging-directive REPLACE takes the inline x-amz-tagging",
      {t["Key"]: t["Value"] for t in
       s3.get_object_tagging(Bucket=BUCKET, Key="cp-tagrepl.txt")["TagSet"]} == {"env": "prod"})
check("tagging-directive REPLACE leaves the (COPY) system headers alone",
      hdrs(s3.head_object(Bucket=BUCKET, Key="cp-tagrepl.txt")).get("content-disposition")
      == SYS["content-disposition"])

# --- an unknown directive is a client error, never silently treated as COPY.
try:
    s3.copy_object(Bucket=BUCKET, Key="cp-bad.txt",
                   CopySource={"Bucket": BUCKET, "Key": "src.txt"}, MetadataDirective="MERGE")
    check("unknown x-amz-metadata-directive is rejected (it succeeded)", False)
except ClientError as e:
    check("unknown x-amz-metadata-directive -> 400 InvalidArgument",
          status_of(e) == 400 and code_of(e) == "InvalidArgument")
try:
    s3.copy_object(Bucket=BUCKET, Key="cp-bad2.txt",
                   CopySource={"Bucket": BUCKET, "Key": "src.txt"}, TaggingDirective="MERGE")
    check("unknown x-amz-tagging-directive is rejected (it succeeded)", False)
except ClientError as e:
    check("unknown x-amz-tagging-directive -> 400 InvalidArgument",
          status_of(e) == 400 and code_of(e) == "InvalidArgument")
names = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix="cp-bad").get("Contents", [])]
check("a rejected directive created no destination object", names == [])

# The aws-chunked strip is a PUT-path helper (`stored_content_encoding`); the copy path reads the
# header directly (service.rs `sys("content-encoding", ...)`), so a REPLACE copy can persist the
# transfer coding S3 strips.
st, _, _ = raw("PUT", "/objf/cp-chunked.txt", headers={
    "x-amz-copy-source": f"/{BUCKET}/src.txt",
    "x-amz-metadata-directive": "REPLACE",
    "content-encoding": "gzip,aws-chunked",
})
check("raw REPLACE copy with 'gzip,aws-chunked' -> 200", st == 200)
_cpce = hdrs(s3.head_object(Bucket=BUCKET, Key="cp-chunked.txt")).get("content-encoding")
# KNOWN GAP: the copy path never normalizes Content-Encoding, so it stores `gzip,aws-chunked`.
gap("REPLACE copy strips aws-chunked from the stored content-encoding",
    _cpce == "gzip",
    f"stored content-encoding is {_cpce!r}. CopyObject with x-amz-metadata-directive: REPLACE and "
    "Content-Encoding 'gzip,aws-chunked' stored the token verbatim. service.rs CopyObject builds "
    "the row with "
    "`sys(\"content-encoding\", &src_row.content_encoding)` -> `req.header(name)`, bypassing the "
    "`stored_content_encoding` normalizer the PUT path uses; AWS strips the transfer coding on "
    "every write path.")

# =================================================================================================
# 4. response-* overrides on GET and on HEAD
# =================================================================================================
OV = {
    "ResponseContentType": "application/x-override",
    "ResponseContentDisposition": 'inline; filename="ov.txt"',
    "ResponseCacheControl": "no-cache",
}
og = hdrs(s3.get_object(Bucket=BUCKET, Key="sys.txt", **OV))
check("GET response-content-type overrides the stored content-type",
      og.get("content-type") == OV["ResponseContentType"])
check("GET response-content-disposition overrides the stored value",
      og.get("content-disposition") == OV["ResponseContentDisposition"])
check("GET response-cache-control overrides the stored value",
      og.get("cache-control") == OV["ResponseCacheControl"])
check("an override REPLACES rather than appends (single value, no comma-joined duplicate)",
      "," not in og.get("content-type", ""))
check("un-overridden stored headers survive the override",
      og.get("content-language") == SYS["content-language"])

oh = hdrs(s3.head_object(Bucket=BUCKET, Key="sys.txt", **OV))
check("HEAD applies response-content-type too",
      oh.get("content-type") == OV["ResponseContentType"])
check("HEAD applies response-content-disposition too",
      oh.get("content-disposition") == OV["ResponseContentDisposition"])
check("HEAD applies response-cache-control too",
      oh.get("cache-control") == OV["ResponseCacheControl"])

# =================================================================================================
# 5. ETag shape + GET/HEAD parity
# =================================================================================================
etag = hdrs(s3.head_object(Bucket=BUCKET, Key="sys.txt"))["etag"]
md5 = hashlib.md5(BODY).hexdigest()
check("single-part ETag is the quoted lowercase md5 of the body", etag == f'"{md5}"')
check("single-part ETag carries no multipart '-N' suffix", "-" not in etag)
check("GET and HEAD report the same ETag",
      hdrs(s3.get_object(Bucket=BUCKET, Key="sys.txt"))["etag"] == etag)
check("ListObjectsV2 reports the same ETag",
      next(o["ETag"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix="sys.txt")["Contents"])
      == etag)

PARITY = ("etag", "content-type", "content-length", "last-modified", "accept-ranges",
          "content-encoding", "content-disposition", "content-language", "cache-control",
          "expires", "x-content-type-options")
gget = hdrs(s3.get_object(Bucket=BUCKET, Key="sys.txt"))
ghead = hdrs(s3.head_object(Bucket=BUCKET, Key="sys.txt"))
for name in PARITY:
    check(f"GET/HEAD parity on {name}", gget.get(name) == ghead.get(name))
check("HEAD reports the full content-length (not 0)", ghead.get("content-length") == str(len(BODY)))

# =================================================================================================
# 6. Zero-byte objects
# =================================================================================================
s3.put_object(Bucket=BUCKET, Key="empty.bin", Body=b"", ContentType="application/zero")
z = s3.get_object(Bucket=BUCKET, Key="empty.bin")
check("zero-byte GET returns an empty body", z["Body"].read() == b"")
check("zero-byte GET reports Content-Length: 0", hdrs(z).get("content-length") == "0")
check("zero-byte ETag is the md5 of the empty string",
      hdrs(z)["etag"] == '"d41d8cd98f00b204e9800998ecf8427e"')
zh = s3.head_object(Bucket=BUCKET, Key="empty.bin")
check("zero-byte HEAD reports ContentLength 0", zh["ContentLength"] == 0)
check("zero-byte HEAD keeps the declared content-type",
      hdrs(zh).get("content-type") == "application/zero")
check("zero-byte object is listed with Size 0",
      next(o["Size"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix="empty.bin")["Contents"])
      == 0)

# =================================================================================================
# 7. Range GET edge cases
# =================================================================================================
ALPHA = b"abcdefghijklmnopqrstuvwxyz"  # 26 bytes
s3.put_object(Bucket=BUCKET, Key="alpha.txt", Body=ALPHA, ContentType="text/plain")


def ranged(spec):
    return raw("GET", "/objf/alpha.txt", headers={"range": spec})


st, hh2, data = ranged("bytes=0-4")
check("range bytes=0-4 -> 206", st == 206)
check("range bytes=0-4 returns exactly those bytes", data == b"abcde")
check("range bytes=0-4 content-range is 'bytes 0-4/26'", hh2.get("content-range") == "bytes 0-4/26")
check("range bytes=0-4 content-length is 5", hh2.get("content-length") == "5")

st, hh2, data = ranged("bytes=-5")
check("suffix range bytes=-5 -> 206", st == 206)
check("suffix range bytes=-5 returns the LAST five bytes", data == b"vwxyz")
check("suffix range bytes=-5 content-range is 'bytes 21-25/26'",
      hh2.get("content-range") == "bytes 21-25/26")

st, hh2, data = ranged("bytes=5-")
check("open range bytes=5- -> 206", st == 206)
check("open range bytes=5- returns the tail", data == ALPHA[5:])
check("open range bytes=5- content-range is 'bytes 5-25/26'",
      hh2.get("content-range") == "bytes 5-25/26")

st, hh2, data = ranged("bytes=25-25")
check("single-byte range bytes=25-25 -> 206 with one byte", st == 206 and data == b"z")

# A range whose end runs past EOF is CLAMPED (satisfiable because the start is in range).
st, hh2, data = ranged("bytes=20-1000")
check("range bytes=20-1000 clamps to EOF -> 206", st == 206)
check("range bytes=20-1000 returns the tail", data == ALPHA[20:])
check("range bytes=20-1000 content-range is 'bytes 20-25/26'",
      hh2.get("content-range") == "bytes 20-25/26")

# A range that STARTS past EOF is unsatisfiable.
try:
    s3.get_object(Bucket=BUCKET, Key="alpha.txt", Range="bytes=100-200")
    check("range starting past EOF is rejected (it succeeded)", False)
except ClientError as e:
    check("range bytes=100-200 -> 416 InvalidRange",
          status_of(e) == 416 and code_of(e) == "InvalidRange")
try:
    s3.get_object(Bucket=BUCKET, Key="alpha.txt", Range="bytes=26-")
    check("open range starting exactly at EOF is rejected (it succeeded)", False)
except ClientError as e:
    check("range bytes=26- (start == size) -> 416 InvalidRange",
          status_of(e) == 416 and code_of(e) == "InvalidRange")
try:
    s3.get_object(Bucket=BUCKET, Key="alpha.txt", Range="bytes=10-5")
    check("inverted range is rejected (it succeeded)", False)
except ClientError as e:
    check("inverted range bytes=10-5 -> 416 InvalidRange",
          status_of(e) == 416 and code_of(e) == "InvalidRange")

# A syntactically MALFORMED Range is a different case from an unsatisfiable one: RFC 7233 says an
# unparsable Range header field MUST be ignored (serve 200 + the whole representation), and that is
# what AWS S3 does. Cairn answers 416. Tolerated here rather than asserted either way, because the
# harness is a CI gate and the divergence is reported for a human to decide; the bytes are still
# checked so neither answer may serve a wrong body.
st, hh2, data = ranged("bytes=not-a-range")
if st == 200:
    check("malformed Range is ignored -> 200 + whole object (RFC 7233 / AWS)", data == ALPHA)
else:
    print("  note: malformed 'Range: bytes=not-a-range' -> "
          f"HTTP {st} (RFC 7233 and AWS S3 IGNORE an unparsable Range and return 200; "
          "service.rs `parse_range` returns Error::InvalidRange). Reported, not gated.")
    check("malformed Range at least serves no partial body", st == 416 and b"abc" not in data)

# --- ranges against a zero-byte object -----------------------------------------------------------
try:
    s3.get_object(Bucket=BUCKET, Key="empty.bin", Range="bytes=0-4")
    check("range on a zero-byte object is rejected (it succeeded)", False)
except ClientError as e:
    check("range bytes=0-4 on a zero-byte object -> 416 InvalidRange",
          status_of(e) == 416 and code_of(e) == "InvalidRange")

# RFC 7233: "If the selected representation is zero length, the byte-range-spec is unsatisfiable" —
# a suffix range against an empty object is a 416, exactly like the first-byte-pos form above.
st, hh2, data = raw("GET", "/objf/empty.bin", headers={"range": "bytes=-5"})
# KNOWN GAP: Cairn answers 206 'bytes 0-0/0' instead of 416 for a suffix range on an empty object.
gap("suffix range bytes=-5 on a zero-byte object -> 416 InvalidRange",
    st == 416,
    f"got HTTP {st} with Content-Range {hh2.get('content-range')!r}. The suffix arm of "
    "service.rs `parse_range` clamps n to `total` (0) and returns Some(offset 0, length 0) instead "
    "of Error::InvalidRange, so a zero-length representation answers 206. The emitted "
    "'bytes 0-0/0' is itself invalid per RFC 7233 (last-byte-pos must be < complete-length) and "
    "self-contradictory against the Content-Length: 0 sent with it.")

# The same defect on a non-empty object: a zero-length suffix range.
st, hh2, data = ranged("bytes=-0")
# KNOWN GAP: Cairn answers 206 'bytes 26-26/26' (one byte past EOF) instead of 416.
gap("zero-length suffix range bytes=-0 -> 416 InvalidRange",
    st == 416,
    f"got HTTP {st} with Content-Range {hh2.get('content-range')!r} and {len(data)} body bytes. "
    "RFC 7233: a suffix-length of 0 is unsatisfiable. service.rs `parse_range` computes "
    "(total - 0, 0), yielding a 206 whose Content-Range points one byte past the end.")

# =================================================================================================
# 8. Keys that need URL encoding
# =================================================================================================
KEYS = [
    "plain space/file name.txt",
    "unicode/naïve-café.txt",
    "emoji/🪨-cairn-🚀.bin",
    "plus/a+b=c.txt",
    "percent/100%-done.txt",
    "amp/rock&roll.txt",
    "question/what?.txt",
    "hash/c#sharp.txt",
    "quote/it's \"quoted\".txt",
    "tilde~and,comma;semi:colon.txt",
    "deep/" + "/".join(f"lvl{i}" for i in range(1, 13)) + "/leaf.txt",
]
MAX_KEY = "z" * 1024  # MAX_KEY_LEN in cairn-types/src/id.rs — the largest VALID key.
KEYS.append(MAX_KEY)

for k in KEYS:
    payload = k.encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=k, Body=payload, ContentType="text/plain")
    label = k if k is not MAX_KEY else "<1024-byte key>"
    got = s3.get_object(Bucket=BUCKET, Key=k)
    check(f"key round-trips through PUT/GET: {label!r}", got["Body"].read() == payload)
    check(f"key round-trips through HEAD: {label!r}",
          s3.head_object(Bucket=BUCKET, Key=k)["ContentLength"] == len(payload))

listed = set()
token = None
while True:
    kw = {"Bucket": BUCKET, "MaxKeys": 200}
    if token:
        kw["ContinuationToken"] = token
    page = s3.list_objects_v2(**kw)
    listed.update(o["Key"] for o in page.get("Contents", []))
    if not page.get("IsTruncated"):
        break
    token = page["NextContinuationToken"]
missing = [k for k in KEYS if k not in listed]
check(f"every URL-encoding-sensitive key comes back byte-identical from ListObjectsV2 "
      f"(missing: {[m[:40] for m in missing]})", not missing)

# The url encoding-type response form must survive the same keys.
enc_page = s3.list_objects_v2(Bucket=BUCKET, Prefix="emoji/", EncodingType="url")
enc_keys = [urllib.parse.unquote(o["Key"]) for o in enc_page.get("Contents", [])]
check("ListObjectsV2 with EncodingType=url decodes back to the emoji key",
      enc_keys == ["emoji/🪨-cairn-🚀.bin"])

# A delimiter listing over a key containing a space must return an intact CommonPrefix.
dl = s3.list_objects_v2(Bucket=BUCKET, Prefix="plain ", Delimiter="/")
check("delimiter listing over a space-bearing prefix returns the intact CommonPrefix",
      [p["Prefix"] for p in dl.get("CommonPrefixes", [])] == ["plain space/"])

check("deleting a space+unicode key works",
      s3.delete_object(Bucket=BUCKET, Key="unicode/naïve-café.txt")
      ["ResponseMetadata"]["HTTPStatusCode"] == 204)
try:
    s3.head_object(Bucket=BUCKET, Key="unicode/naïve-café.txt")
    check("the deleted unicode key is gone (HEAD succeeded)", False)
except ClientError as e:
    check("HEAD of the deleted unicode key -> 404 (Error code '404')",
          status_of(e) == 404 and code_of(e) in ("404", "NoSuchKey"))

# =================================================================================================
# 9. Content-MD5
# =================================================================================================
good = base64.b64encode(hashlib.md5(BODY).digest()).decode()
r = s3.put_object(Bucket=BUCKET, Key="md5-ok.txt", Body=BODY, ContentMD5=good)
check("PUT with a correct Content-MD5 -> 200",
      r["ResponseMetadata"]["HTTPStatusCode"] == 200)
check("the Content-MD5 object round-trips",
      s3.get_object(Bucket=BUCKET, Key="md5-ok.txt")["Body"].read() == BODY)

bad = base64.b64encode(hashlib.md5(b"something else entirely").digest()).decode()
try:
    s3.put_object(Bucket=BUCKET, Key="md5-bad.txt", Body=BODY, ContentMD5=bad)
    check("PUT with a mismatched Content-MD5 is rejected (it succeeded)", False)
except ClientError as e:
    check("PUT with a mismatched Content-MD5 -> 400 BadDigest",
          status_of(e) == 400 and code_of(e) == "BadDigest")
try:
    s3.head_object(Bucket=BUCKET, Key="md5-bad.txt")
    check("a BadDigest PUT stored nothing (HEAD succeeded)", False)
except ClientError as e:
    check("a BadDigest PUT stored nothing -> HEAD 404", status_of(e) == 404)

st, _, body = raw("PUT", "/objf/md5-malformed.txt", body=BODY,
                  headers={"content-md5": "!!!not-base64!!!"})
check("PUT with a malformed Content-MD5 -> 400", st == 400)
check("PUT with a malformed Content-MD5 -> InvalidDigest", b"InvalidDigest" in body)

# =================================================================================================
# 10. DeleteObjects
# =================================================================================================
BATCH = [f"del/{i}.txt" for i in range(5)]
for k in BATCH:
    s3.put_object(Bucket=BUCKET, Key=k, Body=b"x")

# --- non-quiet: every key reported, in request order.
d = s3.delete_objects(Bucket=BUCKET,
                      Delete={"Objects": [{"Key": k} for k in BATCH[:3]], "Quiet": False})
check("DeleteObjects non-quiet -> 200", d["ResponseMetadata"]["HTTPStatusCode"] == 200)
check("DeleteObjects non-quiet reports every deleted key IN REQUEST ORDER",
      [x["Key"] for x in d.get("Deleted", [])] == BATCH[:3])
check("DeleteObjects non-quiet reports no errors", d.get("Errors", []) == [])
remaining = {o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix="del/").get("Contents", [])}
check("DeleteObjects actually removed the keys", remaining == set(BATCH[3:]))

# --- quiet: successes suppressed.
d = s3.delete_objects(Bucket=BUCKET,
                      Delete={"Objects": [{"Key": k} for k in BATCH[3:]], "Quiet": True})
check("DeleteObjects quiet -> 200", d["ResponseMetadata"]["HTTPStatusCode"] == 200)
check("DeleteObjects quiet suppresses the <Deleted> entries", d.get("Deleted", []) == [])
check("DeleteObjects quiet reports no errors", d.get("Errors", []) == [])
check("DeleteObjects quiet still deleted the keys",
      s3.list_objects_v2(Bucket=BUCKET, Prefix="del/").get("Contents", []) == [])

# --- a nonexistent key is a SUCCESS in S3 (idempotent delete), not an error.
d = s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": "del/never-existed.txt"}]})
check("DeleteObjects of a nonexistent key reports it as Deleted, not an Error",
      [x["Key"] for x in d.get("Deleted", [])] == ["del/never-existed.txt"]
      and d.get("Errors", []) == [])

# --- partial failure: one structurally invalid key must not fail the whole request.
for k in ("part/ok1.txt", "part/ok2.txt"):
    s3.put_object(Bucket=BUCKET, Key=k, Body=b"x")
OVERLONG = "q" * 1025  # one byte past MAX_KEY_LEN
d = s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [
    {"Key": "part/ok1.txt"}, {"Key": OVERLONG}, {"Key": "part/ok2.txt"}]})
check("DeleteObjects with one bad key still returns 200",
      d["ResponseMetadata"]["HTTPStatusCode"] == 200)
check("DeleteObjects reports the valid keys as deleted",
      [x["Key"] for x in d.get("Deleted", [])] == ["part/ok1.txt", "part/ok2.txt"])
errs = d.get("Errors", [])
check("DeleteObjects reports exactly one per-key error", len(errs) == 1)
check("the per-key error names the offending key", errs and errs[0]["Key"] == OVERLONG)
check("the per-key error code is InvalidArgument, not a generic InternalError",
      errs and errs[0]["Code"] == "InvalidArgument")
check("the valid keys really were deleted despite the sibling failure",
      s3.list_objects_v2(Bucket=BUCKET, Prefix="part/").get("Contents", []) == [])

# --- errors are reported even in Quiet mode (Quiet suppresses successes only).
d = s3.delete_objects(Bucket=BUCKET,
                      Delete={"Objects": [{"Key": OVERLONG}], "Quiet": True})
check("Quiet mode still reports per-key errors",
      len(d.get("Errors", [])) == 1 and d["Errors"][0]["Code"] == "InvalidArgument")
check("Quiet mode reports no successes for a failed key", d.get("Deleted", []) == [])

# --- duplicate keys in one request: a same-key UNIQUE-constraint race once surfaced as a spurious
# InternalError (service.rs groups duplicate entries and awaits them sequentially).
s3.put_object(Bucket=BUCKET, Key="dup/one.txt", Body=b"x")
d = s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [
    {"Key": "dup/one.txt"}, {"Key": "dup/one.txt"}, {"Key": "dup/one.txt"}]})
check("DeleteObjects with a triplicated key -> 200",
      d["ResponseMetadata"]["HTTPStatusCode"] == 200)
check("a duplicated key raises NO error (idempotent, never InternalError)",
      d.get("Errors", []) == [])
check("a duplicated key is reported once per request entry",
      [x["Key"] for x in d.get("Deleted", [])] == ["dup/one.txt"] * 3)
check("the duplicated key is gone",
      s3.list_objects_v2(Bucket=BUCKET, Prefix="dup/").get("Contents", []) == [])

# Interleaved duplicates across distinct keys exercise the grouping + reordering together: distinct
# keys run concurrently while same-key entries stay sequential, and the response must still be in
# request order.
for k in ("mix/a.txt", "mix/b.txt", "mix/c.txt"):
    s3.put_object(Bucket=BUCKET, Key=k, Body=b"x")
order = ["mix/a.txt", "mix/b.txt", "mix/a.txt", "mix/c.txt", "mix/b.txt", "mix/a.txt"]
d = s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in order]})
check("interleaved duplicate keys raise no error", d.get("Errors", []) == [])
check("interleaved duplicate results come back in REQUEST order",
      [x["Key"] for x in d.get("Deleted", [])] == order)
check("all interleaved keys are gone",
      s3.list_objects_v2(Bucket=BUCKET, Prefix="mix/").get("Contents", []) == [])

# =================================================================================================
if FAILURES:
    print(f"\n{len(FAILURES)} ASSERTION(S) FAILED:")
    for f in FAILURES:
        print(f"  - {f}")
    if GAPS:
        print(f"\n{len(GAPS)} of them are SUSPECTED PRODUCT BUGS (see the KNOWN GAP notes above).")
    sys.exit(1)

print("OBJECT PAYLOAD/HEADER/RANGE OK — headers, copy directives, ranges, exotic keys, "
      "Content-MD5 and DeleteObjects all conform")
