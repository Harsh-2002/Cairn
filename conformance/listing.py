#!/usr/bin/env python3
"""Listing / pagination / versioning conformance (package B+E).

Two families of behaviour that Cairn implements but that nothing black-box pinned:

  * **Listing & pagination** — `delimiter` + `CommonPrefixes` (including a NON-"/" and a
    MULTI-CHARACTER delimiter), the `delimiter=` *empty-but-present* case (a real bug once lived
    exactly there: `"".find()` returns `Some(0)`, which collapses every key into one CommonPrefix
    and returns zero Contents — see `list_objects` in service.rs and `list_impl` in
    cairn-meta/store.rs), `max-keys` truncation driven as a REAL token-fed pagination loop,
    `StartAfter`, the distinct ListObjects **v1** `Marker`/`NextMarker` code path, `EncodingType`,
    the empty-bucket shape, and UTF-8 byte ordering.
  * **Versioning** — Suspended-bucket null-version overwrite-in-place, delete markers (creation,
    the 404 + `x-amz-delete-marker` read, the 405 for a marker's own version id, and the canonical
    **undelete**: DELETE the delete marker by version id and the object comes back), version-scoped
    GET/HEAD/DELETE, `NoSuchVersion`, the `ListObjectVersions` response shape, its
    `KeyMarker`/`VersionIdMarker` pagination round-trip (PR #1 fixed a base64 KeyMarker bug here —
    a marker that is *itself valid base64* is the adversarial case, so one fixture key is `aaaa`),
    copy-from-an-explicit-source-VersionId, and `GetBucketVersioning` before any config exists.

Every failure assertion pins the **exact S3 error code AND HTTP status** — `except ClientError:
pass` accepts a 500 as happily as a 404 and is not an assertion.

Checks are COLLECTED rather than exit-on-first-failure: a listing bug usually implies several
downstream ones, and the whole set is the diagnosis. Non-zero exit if anything failed.

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

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=akid,
    aws_secret_access_key=secret,
    region_name=REGION,
    config=Config(s3={"addressing_style": "path"}, retries={"total_max_attempts": 1, "mode": "standard"}),
)

FAILURES = []


def check(label, cond):
    """Record an assertion. Collected, not fail-fast — see the module docstring."""
    if cond:
        print(f"  ok: {label}")
    else:
        print(f"  FAIL: {label}")
        FAILURES.append(label)
    return bool(cond)


def fatal(label):
    """A fixture/setup failure: the rest of the run would be meaningless."""
    print(f"FATAL: {label}")
    sys.exit(2)


def status_of(err):
    return err.response["ResponseMetadata"]["HTTPStatusCode"]


def code_of(err):
    return err.response["Error"]["Code"]


def hdr(resp_meta, name):
    return resp_meta["ResponseMetadata"]["HTTPHeaders"].get(name)


def err_hdr(err, name):
    return err.response["ResponseMetadata"]["HTTPHeaders"].get(name)


# --- raw signed request ---------------------------------------------------------------------
# boto3 cannot express several inputs this harness must put on the wire: a PRESENT-BUT-EMPTY
# `delimiter=` (the SDK drops an empty string from the query), a non-integer `max-keys`, or an
# over-1000 `max-keys`. Hand-build and sign with botocore's real SigV4 signer, then parse the XML
# directly — which also lets us assert on elements boto3's model hides.
_creds = Credentials(akid, secret)
_url = urllib.parse.urlsplit(endpoint)


def q(*pairs):
    """Build a percent-encoded query string from (key, value) pairs.

    botocore's `_canonical_query_string_url` signs the query bytes VERBATIM, while the server
    canonicalises by decoding and re-encoding per SigV4 — so an unencoded reserved character (a
    `/` in a prefix, say) signs one way and verifies another and the request 403s. Encoding here
    makes both sides agree. A `None` value is a valueless subresource (`?versions`).
    """
    out = []
    for k, v in pairs:
        if v is None:
            out.append(urllib.parse.quote(k, safe="-_.~"))
        else:
            out.append(
                f"{urllib.parse.quote(k, safe='-_.~')}={urllib.parse.quote(v, safe='-_.~')}"
            )
    return "&".join(out)


def raw(method, path, query="", body=b""):
    """Send a hand-built, SigV4-signed request. Returns (status, body_bytes)."""
    target = path + (f"?{query}" if query else "")
    req = AWSRequest(method=method, url=f"{endpoint}{target}", data=body)
    req.headers["host"] = _url.netloc
    req.headers["x-amz-content-sha256"] = hashlib.sha256(body).hexdigest()
    SigV4Auth(_creds, "s3", REGION).add_auth(req)
    conn = http.client.HTTPConnection(_url.hostname, _url.port, timeout=30)
    conn.request(method, target, body=body, headers=dict(req.headers))
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


def local(tag):
    return tag.rsplit("}", 1)[-1]


def kids(root, name):
    return [e for e in root if local(e.tag) == name]


def txt(root, name, default=None):
    for e in root:
        if local(e.tag) == name:
            return e.text if e.text is not None else ""
    return default


def raw_list(query, path="/lst-delim"):
    """GET a listing via a hand-signed request; returns the parsed XML root."""
    st, body = raw("GET", path, query=query)
    if st != 200:
        fatal(f"raw listing {path}?{query} returned {st}: {body[:300]!r}")
    return ET.fromstring(body)


def raw_keys(root):
    return [txt(c, "Key") for c in kids(root, "Contents")]


def raw_prefixes(root):
    return [txt(c, "Prefix") for c in kids(root, "CommonPrefixes")]


# =================================================================================================
# PART 1 — LISTING & PAGINATION
# =================================================================================================
print("== listing: delimiter / CommonPrefixes ==")

# `lst-delim` mixes three separator alphabets on purpose so ONE fixture can be re-listed under a
# "/" delimiter, a single-character non-"/" delimiter, and a MULTI-character delimiter.
DELIM_KEYS = [
    "a/b/c1.txt",
    "a/b/c2.txt",
    "a/d.txt",
    "b/e.txt",
    "m::n",
    "m::o",
    "mq",
    "plain.txt",
    "top.txt",
    "x|w",
    "x|y|z",
]
s3.create_bucket(Bucket="lst-delim")
for k in DELIM_KEYS:
    s3.put_object(Bucket="lst-delim", Key=k, Body=k.encode())
if len(s3.list_objects_v2(Bucket="lst-delim", MaxKeys=1000).get("Contents", [])) != len(DELIM_KEYS):
    fatal("lst-delim fixture did not land")

r = s3.list_objects_v2(Bucket="lst-delim", Delimiter="/")
check("delimiter=/ groups top-level folders",
      [p["Prefix"] for p in r.get("CommonPrefixes", [])] == ["a/", "b/"])
check("delimiter=/ leaves un-nested keys in Contents",
      [o["Key"] for o in r.get("Contents", [])]
      == ["m::n", "m::o", "mq", "plain.txt", "top.txt", "x|w", "x|y|z"])
check("delimiter=/ KeyCount counts Contents + CommonPrefixes", r["KeyCount"] == 7 + 2)
check("delimiter=/ echoes Delimiter", r.get("Delimiter") == "/")
check("delimiter=/ is not truncated", r["IsTruncated"] is False)

# prefix + delimiter: descend one level. The CommonPrefix must carry the FULL path, not the
# prefix-relative remainder.
r = s3.list_objects_v2(Bucket="lst-delim", Prefix="a/", Delimiter="/")
check("prefix=a/ delimiter=/ -> CommonPrefixes are full paths",
      [p["Prefix"] for p in r.get("CommonPrefixes", [])] == ["a/b/"])
check("prefix=a/ delimiter=/ -> Contents is the leaf only",
      [o["Key"] for o in r.get("Contents", [])] == ["a/d.txt"])
check("prefix=a/ delimiter=/ echoes Prefix", r.get("Prefix") == "a/")
check("prefix=a/ delimiter=/ KeyCount", r["KeyCount"] == 2)

# Deepest level: nothing left to group.
r = s3.list_objects_v2(Bucket="lst-delim", Prefix="a/b/", Delimiter="/")
check("prefix=a/b/ delimiter=/ -> no CommonPrefixes", r.get("CommonPrefixes", []) == [])
check("prefix=a/b/ delimiter=/ -> both leaves",
      [o["Key"] for o in r.get("Contents", [])] == ["a/b/c1.txt", "a/b/c2.txt"])

# A prefix that is not itself a folder boundary still groups on the next delimiter.
r = s3.list_objects_v2(Bucket="lst-delim", Prefix="a", Delimiter="/")
check("prefix=a (no slash) delimiter=/ -> CommonPrefixes a/",
      [p["Prefix"] for p in r.get("CommonPrefixes", [])] == ["a/"])
check("prefix=a (no slash) delimiter=/ -> no Contents", r.get("Contents", []) == [])

# A prefix matching nothing: an empty, non-truncated page (NOT an error).
r = s3.list_objects_v2(Bucket="lst-delim", Prefix="zzz-nothing/", Delimiter="/")
check("non-matching prefix -> KeyCount 0, not an error",
      r["KeyCount"] == 0 and r["IsTruncated"] is False and "Contents" not in r)

# --- non-"/" delimiter -------------------------------------------------------------------------
r = s3.list_objects_v2(Bucket="lst-delim", Delimiter="|")
check("non-'/' delimiter '|' groups on '|'",
      [p["Prefix"] for p in r.get("CommonPrefixes", [])] == ["x|"])
check("non-'/' delimiter '|' leaves '/'-keys ungrouped in Contents",
      [o["Key"] for o in r.get("Contents", [])]
      == ["a/b/c1.txt", "a/b/c2.txt", "a/d.txt", "b/e.txt",
          "m::n", "m::o", "mq", "plain.txt", "top.txt"])
r = s3.list_objects_v2(Bucket="lst-delim", Prefix="x|", Delimiter="|")
check("prefix=x| delimiter=| descends one '|' level",
      [p["Prefix"] for p in r.get("CommonPrefixes", [])] == ["x|y|"]
      and [o["Key"] for o in r.get("Contents", [])] == ["x|w"])

# --- multi-character delimiter -------------------------------------------------------------------
r = s3.list_objects_v2(Bucket="lst-delim", Delimiter="::")
check("multi-char delimiter '::' groups on the whole string",
      [p["Prefix"] for p in r.get("CommonPrefixes", [])] == ["m::"])
check("multi-char delimiter '::' does not group 'mq'",
      "mq" in [o["Key"] for o in r.get("Contents", [])]
      and "m::n" not in [o["Key"] for o in r.get("Contents", [])])

# --- the empty-but-present delimiter -------------------------------------------------------------
# `delimiter=` must behave EXACTLY as if absent. `str::find("")` is `Some(0)`, so a naive
# implementation groups every key into the single CommonPrefix "" and returns zero Contents.
# minio-go (hence warp's recursive list) sends `delimiter=` unconditionally, so this is a live
# interop path, not a curiosity. boto3 drops an empty string from the query, so probe it RAW —
# and probe all three listing surfaces, since each parses `delimiter` separately.
root = raw_list(q(("list-type", "2"), ("delimiter", "")))
check("v2 delimiter= (empty) returns every key, as if absent", raw_keys(root) == DELIM_KEYS)
check("v2 delimiter= (empty) returns NO CommonPrefixes", raw_prefixes(root) == [])
check("v2 delimiter= (empty) KeyCount is the full set",
      txt(root, "KeyCount") == str(len(DELIM_KEYS)))
check("v2 delimiter= (empty) omits the Delimiter element", txt(root, "Delimiter") is None)

root = raw_list(q(("list-type", "2"), ("delimiter", ""), ("prefix", "a/")))
check("v2 delimiter= (empty) + prefix still filters by prefix",
      raw_keys(root) == ["a/b/c1.txt", "a/b/c2.txt", "a/d.txt"] and raw_prefixes(root) == [])

root = raw_list(q(("delimiter", ""), ("max-keys", "1000"), ("marker", "")))  # v1 code path
check("v1 delimiter= (empty) behaves as absent",
      raw_keys(root) == DELIM_KEYS and raw_prefixes(root) == [])
check("v1 delimiter= (empty) omits the Delimiter element", txt(root, "Delimiter") is None)

root = raw_list(q(("versions", None), ("delimiter", "")))
check("ListObjectVersions delimiter= (empty) behaves as absent",
      [txt(v, "Key") for v in kids(root, "Version")] == DELIM_KEYS
      and raw_prefixes(root) == [])

# The sanity twin: a genuinely absent delimiter must give the same answer as the empty one.
root = raw_list(q(("list-type", "2")))
check("absent delimiter == empty delimiter", raw_keys(root) == DELIM_KEYS)


# --- lexicographic UTF-8 ordering ----------------------------------------------------------------
print("== listing: UTF-8 byte ordering ==")
ORD_KEYS = ["B", "_", "a", "z", "~", "ä", "日"]  # already in UTF-8 byte order
s3.create_bucket(Bucket="lst-order")
for k in reversed(ORD_KEYS):  # insert in the WRONG order so insertion order can't fake it
    s3.put_object(Bucket="lst-order", Key=k, Body=b"o")
got = [o["Key"] for o in s3.list_objects_v2(Bucket="lst-order")["Contents"]]
check("keys are returned in UTF-8 byte (not locale/ASCII-case) order", got == ORD_KEYS)
check("byte ordering matches python's own utf-8 sort",
      got == sorted(ORD_KEYS, key=lambda s: s.encode()))
check("uppercase sorts before lowercase (byte order, not case-insensitive)",
      got.index("B") < got.index("a"))
check("multi-byte keys sort after all ASCII", got.index("ä") > got.index("~"))


# --- pagination: v2 continuation-token loop ------------------------------------------------------
print("== listing: v2 pagination loop ==")
PAGE_KEYS = [f"k{i:02d}" for i in range(10)]
s3.create_bucket(Bucket="lst-page")
for k in PAGE_KEYS:
    s3.put_object(Bucket="lst-page", Key=k, Body=b"p")

seen, pages, token, guard = [], [], None, 0
while True:
    guard += 1
    if guard > 25:
        fatal("v2 pagination did not terminate in 25 pages (loop)")
    kw = {"Bucket": "lst-page", "MaxKeys": 3}
    if token is not None:
        kw["ContinuationToken"] = token
    r = s3.list_objects_v2(**kw)
    page = [o["Key"] for o in r.get("Contents", [])]
    pages.append((page, r["IsTruncated"], r["KeyCount"], r.get("NextContinuationToken")))
    check(f"page {guard}: KeyCount matches len(Contents)", r["KeyCount"] == len(page))
    check(f"page {guard}: MaxKeys echoed as 3", r["MaxKeys"] == 3)
    if token is not None:
        check(f"page {guard}: ContinuationToken echoed", r.get("ContinuationToken") == token)
    seen += page
    if not r["IsTruncated"]:
        check("final page carries NO NextContinuationToken", "NextContinuationToken" not in r)
        break
    if not check(f"page {guard}: truncated page carries NextContinuationToken",
                 r.get("NextContinuationToken")):
        fatal("cannot continue pagination without a token")
    token = r["NextContinuationToken"]

check("v2 pagination terminated", True)
check("v2 pagination produced 4 pages for 10 keys @ max-keys=3", len(pages) == 4)
check("v2 page sizes are 3,3,3,1", [len(p[0]) for p in pages] == [3, 3, 3, 1])
check("v2 IsTruncated is true,true,true,false", [p[1] for p in pages] == [True, True, True, False])
check("v2 pagination returned every key exactly once, in order", seen == PAGE_KEYS)
check("v2 pagination emitted no duplicates", len(seen) == len(set(seen)))
check("v2 pagination tokens never repeat",
      len({p[3] for p in pages if p[3]}) == len([p for p in pages if p[3]]))

# max-keys edge cases (raw: boto3's model forbids a non-integer and clamps nothing).
root = raw_list(q(("list-type", "2"), ("max-keys", "0")), path="/lst-page")
check("max-keys=0 -> KeyCount 0, IsTruncated false, no Contents",
      txt(root, "KeyCount") == "0" and txt(root, "IsTruncated") == "false"
      and raw_keys(root) == [])
check("max-keys=0 -> no NextContinuationToken", txt(root, "NextContinuationToken") is None)
root = raw_list(q(("list-type", "2"), ("max-keys", "5000")), path="/lst-page")
check("max-keys=5000 is capped at 1000 (AWS caps, it does not error)",
      txt(root, "MaxKeys") == "1000" and len(raw_keys(root)) == 10)
st, body = raw("GET", "/lst-page", query=q(("max-keys", "abc")))
check("max-keys=abc -> 400 InvalidArgument",
      st == 400 and b"<Code>InvalidArgument</Code>" in body)
st, body = raw("GET", "/lst-page", query=q(("max-keys", "-1")))
check("max-keys=-1 -> 400 InvalidArgument",
      st == 400 and b"<Code>InvalidArgument</Code>" in body)

# KNOWN GAP (see report, finding C): an UNDECODABLE continuation token must be REJECTED, not
# folded into "no token". `decode_token` in service.rs is `base64::decode(..).ok()`, and
# `list_objects` then passes that `None` straight through as "start from the beginning" — the same
# invalid-collapses-to-absent class that conformance/routing.sh exists to prevent. The failure mode
# is not a wrong page, it is a SILENT RESTART: a paginating consumer whose token got truncated
# re-processes the whole bucket forever instead of erroring. S3 answers 400 InvalidArgument.
try:
    r = s3.list_objects_v2(Bucket="lst-page", ContinuationToken="!!!not-base64!!!")
    got = [o["Key"] for o in r.get("Contents", [])]
    check("garbage ContinuationToken is rejected, not silently restarted from page 1 "
          f"(it returned {len(got)} keys starting at {got[0] if got else None!r})", False)
except ClientError as e:
    check("garbage ContinuationToken -> 400 InvalidArgument",
          status_of(e) == 400 and code_of(e) == "InvalidArgument")


# --- StartAfter ---------------------------------------------------------------------------------
print("== listing: StartAfter ==")
r = s3.list_objects_v2(Bucket="lst-page", StartAfter="k04")
check("StartAfter=k04 is EXCLUSIVE (k05..k09)",
      [o["Key"] for o in r.get("Contents", [])] == PAGE_KEYS[5:])
check("StartAfter=k04 KeyCount", r["KeyCount"] == 5)
# KNOWN GAP (see report, finding E): S3 echoes StartAfter in the ListObjectsV2 response;
# `cairn_xml::list_objects_v2` never receives or emits it.
check("StartAfter echoed in the response", r.get("StartAfter") == "k04")

r = s3.list_objects_v2(Bucket="lst-page", StartAfter="k04zzz")
check("StartAfter on a key that does not exist resumes after it",
      [o["Key"] for o in r.get("Contents", [])] == PAGE_KEYS[5:])
r = s3.list_objects_v2(Bucket="lst-page", StartAfter="k99")
check("StartAfter past the end -> empty, not truncated",
      r["KeyCount"] == 0 and r["IsTruncated"] is False)
r = s3.list_objects_v2(Bucket="lst-delim", Prefix="a/", StartAfter="a/b/c1.txt")
check("StartAfter combines with Prefix",
      [o["Key"] for o in r.get("Contents", [])] == ["a/b/c2.txt", "a/d.txt"])
r = s3.list_objects_v2(Bucket="lst-page", StartAfter="k04", MaxKeys=2)
check("StartAfter + MaxKeys truncates from the StartAfter point",
      [o["Key"] for o in r.get("Contents", [])] == ["k05", "k06"] and r["IsTruncated"] is True)


# --- ListObjects v1: Marker / NextMarker (a DISTINCT code path from v2) --------------------------
print("== listing: v1 Marker/NextMarker loop ==")
seen1, marker, guard = [], None, 0
while True:
    guard += 1
    if guard > 25:
        fatal("v1 pagination did not terminate in 25 pages (loop)")
    kw = {"Bucket": "lst-page", "MaxKeys": 3}
    if marker is not None:
        kw["Marker"] = marker
    r = s3.list_objects(**kw)
    page = [o["Key"] for o in r.get("Contents", [])]
    check(f"v1 page {guard}: Marker echoed", r.get("Marker", "") == (marker or ""))
    seen1 += page
    if not r["IsTruncated"]:
        break
    if not check(f"v1 page {guard}: truncated page carries NextMarker", r.get("NextMarker")):
        fatal("cannot continue v1 pagination without NextMarker")
    # The whole point: NextMarker must be a PLAIN object key echoed back verbatim as `marker`
    # (v2's opaque base64 token is a different animal). A base64-encoded NextMarker here would
    # seek to the base64 string and loop or skip keys (audit 2026-07).
    check(f"v1 page {guard}: NextMarker is a plain key, not an opaque token",
          r["NextMarker"] in PAGE_KEYS)
    marker = r["NextMarker"]

check("v1 pagination returned every key exactly once, in order", seen1 == PAGE_KEYS)
check("v1 pagination emitted no duplicates", len(seen1) == len(set(seen1)))
check("v1 pagination took 4 pages", guard == 4)

# KNOWN GAP (see report, finding A): S3's `marker` is EXCLUSIVE — "Amazon S3 starts listing AFTER
# this specified key" — and its `NextMarker` is therefore the LAST key of the page. Cairn treats
# `marker` as an inclusive resume cursor and compensates by emitting the FIRST UNRETURNED key as
# NextMarker (`list_impl`: `page.next_cursor = Some(summary.key)`; `list_objects` consumes
# `req.query("marker")` straight into `ListQuery::cursor`, and `list_impl` seeks `key >= cursor`).
# The pair is self-consistent, so Cairn's OWN loop above is correct — but the two halves are each
# non-conformant, and any client following the AWS contract (resume from the last key it saw, which
# is what AWS tells you to do and what botocore's own paginator falls back to when NextMarker is
# absent) re-reads one key per page boundary. Both assertions must move together in a fix.
r = s3.list_objects(Bucket="lst-page", Marker="k04")
check("v1 Marker is EXCLUSIVE (marker=k04 starts at k05)",
      [o["Key"] for o in r.get("Contents", [])] == PAGE_KEYS[5:])
r = s3.list_objects(Bucket="lst-page", MaxKeys=3)
check("v1 NextMarker is the LAST key of the page",
      r.get("NextMarker") == r["Contents"][-1]["Key"])
# StartAfter (v2) IS exclusive — `list_impl` runs it through `successor()` — so the two sibling
# parameters disagree with each other, which is the sharpest evidence that `marker` is the outlier.
check("v2 StartAfter and v1 Marker agree on exclusivity",
      [o["Key"] for o in s3.list_objects(Bucket="lst-page", Marker="k04").get("Contents", [])]
      == [o["Key"] for o in
          s3.list_objects_v2(Bucket="lst-page", StartAfter="k04").get("Contents", [])])
r = s3.list_objects(Bucket="lst-page", MaxKeys=3)
check("v1 response has no KeyCount element (that is a v2-only field)", "KeyCount" not in r)
check("v1 Marker is echoed as empty on page 1", r.get("Marker", "") == "")
# v1 with a delimiter: NextMarker must still advance past the grouped prefix.
r = s3.list_objects(Bucket="lst-delim", Delimiter="/", MaxKeys=2)
check("v1 delimiter + max-keys truncates across CommonPrefixes",
      r["IsTruncated"] is True
      and [p["Prefix"] for p in r.get("CommonPrefixes", [])] == ["a/", "b/"])
r2 = s3.list_objects(Bucket="lst-delim", Delimiter="/", MaxKeys=2, Marker=r["NextMarker"])
check("v1 delimiter pagination advances past the CommonPrefixes",
      [o["Key"] for o in r2.get("Contents", [])] == ["m::n", "m::o"])


# --- empty bucket --------------------------------------------------------------------------------
print("== listing: empty bucket ==")
s3.create_bucket(Bucket="lst-empty")
r = s3.list_objects_v2(Bucket="lst-empty")
check("empty bucket v2: KeyCount 0", r["KeyCount"] == 0)
check("empty bucket v2: no Contents element", "Contents" not in r)
check("empty bucket v2: IsTruncated false", r["IsTruncated"] is False)
check("empty bucket v2: no NextContinuationToken", "NextContinuationToken" not in r)
check("empty bucket v2: Name echoed", r["Name"] == "lst-empty")
r = s3.list_objects(Bucket="lst-empty")
check("empty bucket v1: no Contents, not truncated",
      "Contents" not in r and r["IsTruncated"] is False)
r = s3.list_object_versions(Bucket="lst-empty")
check("empty bucket versions: no Versions and no DeleteMarkers",
      "Versions" not in r and "DeleteMarkers" not in r and r["IsTruncated"] is False)
r = s3.list_objects_v2(Bucket="lst-empty", Delimiter="/")
check("empty bucket + delimiter: no CommonPrefixes",
      r["KeyCount"] == 0 and "CommonPrefixes" not in r)

try:
    s3.list_objects_v2(Bucket="lst-no-such-bucket")
    check("listing a nonexistent bucket is rejected (it returned success)", False)
except ClientError as e:
    check("listing a nonexistent bucket -> 404 NoSuchBucket",
          status_of(e) == 404 and code_of(e) == "NoSuchBucket")


# --- EncodingType=url ----------------------------------------------------------------------------
print("== listing: EncodingType=url ==")
ENC_KEYS = [
    "enc/plus+key",
    "enc/pct%41key",
    "enc/sp ace.txt",
    "enc/tilde~and&amp.txt",
    "enc/日本語.txt",
    "enc/été/café.txt",
]
s3.create_bucket(Bucket="lst-enc")
for k in ENC_KEYS:
    s3.put_object(Bucket="lst-enc", Key=k, Body=k.encode())

# (a) The functional requirement: whatever the encoding, keys must round-trip byte-for-byte.
# botocore auto-sets EncodingType=url on ListObjects*, so this is also the default-SDK path.
got = [o["Key"] for o in s3.list_objects_v2(Bucket="lst-enc")["Contents"]]
check("keys with space/+/%/&/unicode round-trip through the default SDK listing",
      sorted(got) == sorted(ENC_KEYS))
for k in ENC_KEYS:
    if not check(f"GET round-trips the listed key {k!r}",
                 s3.get_object(Bucket="lst-enc", Key=k)["Body"].read() == k.encode()):
        break
check("'&' in a key is XML-escaped, not emitted raw",
      b"&amp;amp" in raw("GET", "/lst-enc", query=q(("prefix", "enc/tilde")))[1])

# (b) The spec requirement. S3: "If you specify the encoding-type request parameter, Amazon S3
# includes this element [EncodingType] in the response and returns encoded key name values."
# botocore only URL-decodes when it auto-set the parameter, so an EXPLICIT EncodingType='url'
# hands us the raw wire values — exactly what we want to inspect.
r = s3.list_objects_v2(Bucket="lst-enc", EncodingType="url")
# KNOWN GAP (see report, finding F): Cairn ignores `encoding-type` entirely — `list_objects` /
# `list_object_versions` in service.rs never read the parameter and cairn-xml never emits an
# <EncodingType> element or percent-encodes anything. It is partly compensated for: `ObjectKey`
# REJECTS the keys that XML cannot carry (C0 controls, U+FFFE/U+FFFF), with the comment "no
# encoding-type=url fallback exists". The default-SDK path above still round-trips, because
# botocore only URL-decodes when the response echoes EncodingType — but the parameter is
# silently ignored rather than honoured or refused.
check("v2 EncodingType=url is echoed in the response", r.get("EncodingType") == "url")
check("v2 EncodingType=url percent-encodes the space in a key",
      any("%20" in o["Key"] for o in r.get("Contents", [])))
r = s3.list_objects(Bucket="lst-enc", EncodingType="url")
check("v1 EncodingType=url is echoed in the response", r.get("EncodingType") == "url")
r = s3.list_object_versions(Bucket="lst-enc", EncodingType="url")
check("ListObjectVersions EncodingType=url is echoed in the response",
      r.get("EncodingType") == "url")
# Delimiter + prefix must be encoded too when encoding-type=url is honoured.
r = s3.list_objects_v2(Bucket="lst-enc", EncodingType="url", Prefix="enc/été/")
check("v2 EncodingType=url percent-encodes the echoed Prefix",
      r.get("Prefix") == "enc/%C3%A9t%C3%A9/")


# =================================================================================================
# PART 2 — VERSIONING
# =================================================================================================
print("== versioning: GetBucketVersioning before any config ==")
s3.create_bucket(Bucket="ver-fresh")
r = s3.get_bucket_versioning(Bucket="ver-fresh")
check("GetBucketVersioning on a never-configured bucket -> 200",
      r["ResponseMetadata"]["HTTPStatusCode"] == 200)
check("GetBucketVersioning on a never-configured bucket has NO Status", "Status" not in r)
check("GetBucketVersioning on a never-configured bucket has no MFADelete", "MFADelete" not in r)
st, body = raw("GET", "/ver-fresh", query=q(("versioning", None)))
check("the raw unconfigured VersioningConfiguration is an empty document",
      st == 200 and b"<Status>" not in body and b"VersioningConfiguration" in body)
try:
    s3.get_bucket_versioning(Bucket="ver-no-such")
    check("GetBucketVersioning on a nonexistent bucket is rejected", False)
except ClientError as e:
    check("GetBucketVersioning on a nonexistent bucket -> 404 NoSuchBucket",
          status_of(e) == 404 and code_of(e) == "NoSuchBucket")


print("== versioning: enabled, version-scoped GET/HEAD/DELETE ==")
s3.create_bucket(Bucket="ver-b")
s3.put_bucket_versioning(Bucket="ver-b", VersioningConfiguration={"Status": "Enabled"})
check("GetBucketVersioning reflects Enabled",
      s3.get_bucket_versioning(Bucket="ver-b").get("Status") == "Enabled")

p1 = s3.put_object(Bucket="ver-b", Key="obj", Body=b"v1-body")
p2 = s3.put_object(Bucket="ver-b", Key="obj", Body=b"v2-body")
v1, v2 = p1.get("VersionId"), p2.get("VersionId")
if not (v1 and v2):
    fatal("PUT into an Enabled bucket returned no x-amz-version-id")
check("each PUT into an Enabled bucket mints a distinct version id", v1 != v2)
check("an Enabled-bucket version id is not the null sentinel",
      v1 != "null" and v2 != "null")

check("plain GET returns the latest version",
      s3.get_object(Bucket="ver-b", Key="obj")["Body"].read() == b"v2-body")
check("plain GET echoes the latest VersionId",
      s3.get_object(Bucket="ver-b", Key="obj").get("VersionId") == v2)
check("version-scoped GET returns the older version",
      s3.get_object(Bucket="ver-b", Key="obj", VersionId=v1)["Body"].read() == b"v1-body")
h = s3.head_object(Bucket="ver-b", Key="obj", VersionId=v1)
check("version-scoped HEAD echoes the requested VersionId", h.get("VersionId") == v1)
check("version-scoped HEAD reports that version's size", h["ContentLength"] == 7)

BOGUS = "bogus-version-id-0000"
try:
    s3.get_object(Bucket="ver-b", Key="obj", VersionId=BOGUS)
    check("GET with a bogus VersionId is rejected (it returned success)", False)
except ClientError as e:
    check("GET with a bogus VersionId -> 404 NoSuchVersion",
          status_of(e) == 404 and code_of(e) == "NoSuchVersion")
try:
    s3.head_object(Bucket="ver-b", Key="obj", VersionId=BOGUS)
    check("HEAD with a bogus VersionId is rejected (it returned success)", False)
except ClientError as e:
    # HEAD has no body, so botocore cannot surface the S3 error code — only the status.
    check("HEAD with a bogus VersionId -> 404", status_of(e) == 404)
# DELETE is deliberately NOT symmetric with GET/HEAD here: S3's DeleteObject is idempotent and
# documents no NoSuchVersion, and AWS answers 204 for a well-formed version id that does not
# exist. (An earlier draft of this harness asserted 404 NoSuchVersion — that was the TEST being
# wrong about S3, not Cairn.) What matters is that it neither errors nor destroys anything else.
d = s3.delete_object(Bucket="ver-b", Key="obj", VersionId=BOGUS)
check("DELETE with a bogus VersionId is an idempotent 204, not an error",
      d["ResponseMetadata"]["HTTPStatusCode"] == 204)
check("DELETE with a bogus VersionId destroyed nothing",
      sorted(v["VersionId"] for v in
             s3.list_object_versions(Bucket="ver-b", Prefix="obj")["Versions"])
      == sorted([v1, v2]))


print("== versioning: delete markers and the canonical undelete ==")
d = s3.delete_object(Bucket="ver-b", Key="obj")
dm = d.get("VersionId")
check("DELETE on a versioned bucket reports DeleteMarker=true", d.get("DeleteMarker") is True)
check("DELETE on a versioned bucket returns the new marker's VersionId",
      bool(dm) and dm not in (v1, v2, "null"))

try:
    s3.get_object(Bucket="ver-b", Key="obj")
    check("GET of a delete-marked key is rejected (it returned success)", False)
except ClientError as e:
    check("GET of a delete-marked key -> 404 NoSuchKey",
          status_of(e) == 404 and code_of(e) == "NoSuchKey")
    check("GET of a delete-marked key carries x-amz-delete-marker: true",
          err_hdr(e, "x-amz-delete-marker") == "true")
    check("GET of a delete-marked key carries the marker's x-amz-version-id",
          err_hdr(e, "x-amz-version-id") == dm)
try:
    s3.head_object(Bucket="ver-b", Key="obj")
    check("HEAD of a delete-marked key is rejected (it returned success)", False)
except ClientError as e:
    check("HEAD of a delete-marked key -> 404", status_of(e) == 404)
    check("HEAD of a delete-marked key carries x-amz-delete-marker: true",
          err_hdr(e, "x-amz-delete-marker") == "true")

# Naming the delete marker's OWN version id is a 405, not a 404: a marker has no content.
try:
    s3.get_object(Bucket="ver-b", Key="obj", VersionId=dm)
    check("GET of a delete marker's own VersionId is rejected (it returned success)", False)
except ClientError as e:
    check("GET of a delete marker's own VersionId -> 405 MethodNotAllowed",
          status_of(e) == 405 and code_of(e) == "MethodNotAllowed")
    check("the 405 carries x-amz-delete-marker: true",
          err_hdr(e, "x-amz-delete-marker") == "true")

# The response shape while the marker is latest.
lv = s3.list_object_versions(Bucket="ver-b", Prefix="obj")
versions = lv.get("Versions", [])
markers = lv.get("DeleteMarkers", [])
check("ListObjectVersions splits Versions from DeleteMarkers",
      len(versions) == 2 and len(markers) == 1)
check("the delete marker is IsLatest", markers and markers[0]["IsLatest"] is True)
check("the delete marker's VersionId matches the DELETE response",
      markers and markers[0]["VersionId"] == dm)
check("no Version entry is IsLatest while a marker is latest",
      all(v["IsLatest"] is False for v in versions))
check("DeleteMarker entries carry Key, LastModified and Owner",
      markers and {"Key", "LastModified", "Owner"} <= set(markers[0])
      and markers[0]["Owner"].get("ID"))
check("DeleteMarker entries carry NO ETag/Size (a marker has no content)",
      markers and "ETag" not in markers[0] and "Size" not in markers[0])
check("Version entries carry ETag, Size, StorageClass and Owner",
      versions and {"ETag", "Size", "StorageClass", "Owner"} <= set(versions[0])
      and versions[0]["Owner"].get("ID"))
check("a delete-marked key disappears from ListObjectsV2",
      "obj" not in [o["Key"] for o in s3.list_objects_v2(Bucket="ver-b").get("Contents", [])])

# THE UNDELETE: removing the delete marker by version id restores the object.
und = s3.delete_object(Bucket="ver-b", Key="obj", VersionId=dm)
check("deleting a delete marker returns 204",
      und["ResponseMetadata"]["HTTPStatusCode"] == 204)
# KNOWN GAP (see report, finding D): AWS documents both headers on the DeleteObject response —
# `x-amz-delete-marker` ("indicates whether the specified object version that was permanently
# deleted was a delete marker") and `x-amz-version-id` ("if you delete a specific object version,
# the value returned is the version ID of the object version deleted"). The `?versionId` branch of
# `delete_object` in service.rs returns a bare `S3Response::status(NO_CONTENT)` with neither. The
# marker-CREATING branch right below it sets both correctly, so this is that one branch. Without
# it a client cannot confirm an undelete actually removed a marker rather than a data version.
check("deleting a delete marker reports DeleteMarker=true", und.get("DeleteMarker") is True)
check("deleting a delete marker echoes the removed marker's VersionId",
      und.get("VersionId") == dm)
check("UNDELETE: the object is readable again",
      s3.get_object(Bucket="ver-b", Key="obj")["Body"].read() == b"v2-body")
check("UNDELETE: the restored latest is the pre-delete latest version",
      s3.head_object(Bucket="ver-b", Key="obj").get("VersionId") == v2)
lv = s3.list_object_versions(Bucket="ver-b", Prefix="obj")
check("UNDELETE: the DeleteMarkers array is gone", "DeleteMarkers" not in lv)
check("UNDELETE: exactly one Version is IsLatest and it is v2",
      [v["VersionId"] for v in lv["Versions"] if v["IsLatest"]] == [v2])
check("UNDELETE: the key is back in ListObjectsV2",
      "obj" in [o["Key"] for o in s3.list_objects_v2(Bucket="ver-b").get("Contents", [])])

# A permanent version delete removes only that version.
s3.delete_object(Bucket="ver-b", Key="obj", VersionId=v1)
try:
    s3.get_object(Bucket="ver-b", Key="obj", VersionId=v1)
    check("a permanently deleted version is gone (it returned success)", False)
except ClientError as e:
    check("GET of a permanently deleted version -> 404 NoSuchVersion",
          status_of(e) == 404 and code_of(e) == "NoSuchVersion")
check("the surviving version is untouched",
      s3.get_object(Bucket="ver-b", Key="obj", VersionId=v2)["Body"].read() == b"v2-body")
lv = s3.list_object_versions(Bucket="ver-b", Prefix="obj")
check("ListObjectVersions no longer lists the purged version",
      [v["VersionId"] for v in lv["Versions"]] == [v2])
check("a plain DELETE of a NONEXISTENT version-scoped version is a no-op 204",
      s3.delete_object(Bucket="ver-b", Key="never-existed")
      ["ResponseMetadata"]["HTTPStatusCode"] == 204)


print("== versioning: copy from an explicit source VersionId ==")
p3 = s3.put_object(Bucket="ver-b", Key="obj", Body=b"v3-body")
v3 = p3["VersionId"]
s3.copy_object(Bucket="ver-b", Key="copied",
               CopySource={"Bucket": "ver-b", "Key": "obj", "VersionId": v2})
check("copy from an explicit source VersionId copies THAT version's bytes",
      s3.get_object(Bucket="ver-b", Key="copied")["Body"].read() == b"v2-body")
check("copy without a source VersionId copies the latest",
      (s3.copy_object(Bucket="ver-b", Key="copied-latest",
                      CopySource={"Bucket": "ver-b", "Key": "obj"}),
       s3.get_object(Bucket="ver-b", Key="copied-latest")["Body"].read())[1] == b"v3-body")
try:
    s3.copy_object(Bucket="ver-b", Key="copied-bogus",
                   CopySource={"Bucket": "ver-b", "Key": "obj", "VersionId": BOGUS})
    check("copy from a bogus source VersionId is rejected (it returned success)", False)
except ClientError as e:
    check("copy from a bogus source VersionId -> 404 NoSuchVersion",
          status_of(e) == 404 and code_of(e) == "NoSuchVersion")
# Copying a delete marker by version id has no content to copy.
dm2 = s3.delete_object(Bucket="ver-b", Key="obj")["VersionId"]
try:
    s3.copy_object(Bucket="ver-b", Key="copied-marker",
                   CopySource={"Bucket": "ver-b", "Key": "obj", "VersionId": dm2})
    check("copy from a delete marker's VersionId is rejected (it returned success)", False)
except ClientError as e:
    check("copy from a delete marker's VersionId -> 404 NoSuchKey",
          status_of(e) == 404 and code_of(e) == "NoSuchKey")
s3.delete_object(Bucket="ver-b", Key="obj", VersionId=dm2)  # undelete for later checks


print("== versioning: Suspended (the null version, overwrite in place) ==")
s3.create_bucket(Bucket="ver-sus")
s3.put_bucket_versioning(Bucket="ver-sus", VersioningConfiguration={"Status": "Enabled"})
sv1 = s3.put_object(Bucket="ver-sus", Key="s", Body=b"enabled-1")["VersionId"]
s3.put_bucket_versioning(Bucket="ver-sus", VersioningConfiguration={"Status": "Suspended"})
check("GetBucketVersioning reflects Suspended",
      s3.get_bucket_versioning(Bucket="ver-sus").get("Status") == "Suspended")

s3.put_object(Bucket="ver-sus", Key="s", Body=b"suspended-1")
lv = s3.list_object_versions(Bucket="ver-sus", Prefix="s")
check("a Suspended-bucket PUT creates the 'null' version",
      "null" in [v["VersionId"] for v in lv["Versions"]])
check("a Suspended-bucket PUT preserves the pre-existing identified version",
      sv1 in [v["VersionId"] for v in lv["Versions"]])
check("the null version is IsLatest after a Suspended PUT",
      [v["VersionId"] for v in lv["Versions"] if v["IsLatest"]] == ["null"])

s3.put_object(Bucket="ver-sus", Key="s", Body=b"suspended-2")
lv = s3.list_object_versions(Bucket="ver-sus", Prefix="s")
check("a second Suspended PUT OVERWRITES the null version in place (no new version)",
      sorted(v["VersionId"] for v in lv["Versions"]) == sorted(["null", sv1]))
check("exactly ONE null version exists",
      [v["VersionId"] for v in lv["Versions"]].count("null") == 1)
check("the null version holds the latest bytes",
      s3.get_object(Bucket="ver-sus", Key="s")["Body"].read() == b"suspended-2")
check("the pre-suspension version is still individually readable",
      s3.get_object(Bucket="ver-sus", Key="s", VersionId=sv1)["Body"].read() == b"enabled-1")
check("VersionId='null' addresses the null version explicitly",
      s3.get_object(Bucket="ver-sus", Key="s", VersionId="null")["Body"].read() == b"suspended-2")

d = s3.delete_object(Bucket="ver-sus", Key="s")
check("a Suspended DELETE reports DeleteMarker=true", d.get("DeleteMarker") is True)
lv = s3.list_object_versions(Bucket="ver-sus", Prefix="s")
check("a Suspended DELETE replaces the null VERSION with a null DELETE MARKER",
      [v["VersionId"] for v in lv.get("Versions", [])] == [sv1]
      and [m["VersionId"] for m in lv.get("DeleteMarkers", [])] == ["null"])
try:
    s3.get_object(Bucket="ver-sus", Key="s")
    check("GET after a Suspended DELETE is rejected (it returned success)", False)
except ClientError as e:
    check("GET after a Suspended DELETE -> 404 NoSuchKey",
          status_of(e) == 404 and code_of(e) == "NoSuchKey")
    check("GET after a Suspended DELETE carries x-amz-delete-marker: true",
          err_hdr(e, "x-amz-delete-marker") == "true")
s3.delete_object(Bucket="ver-sus", Key="s", VersionId="null")
check("UNDELETE in a Suspended bucket: the older identified version is restored",
      s3.get_object(Bucket="ver-sus", Key="s")["Body"].read() == b"enabled-1")


print("== versioning: ListObjectVersions pagination (KeyMarker/VersionIdMarker) ==")
# `aaaa` is deliberately VALID BASE64: PR #1 fixed a bug where the KeyMarker was base64
# round-tripped, so a key that decodes cleanly seeks to garbage instead of failing loudly.
VKEYS = ["aaaa", "bbbb/cc", "zzz"]
s3.create_bucket(Bucket="ver-page")
s3.put_bucket_versioning(Bucket="ver-page", VersioningConfiguration={"Status": "Enabled"})
expected = set()
for k in VKEYS:
    for i in range(3):
        expected.add((k, s3.put_object(Bucket="ver-page", Key=k, Body=f"{k}-{i}".encode())
                      ["VersionId"]))
if len(expected) != 9:
    fatal("ver-page fixture did not produce 9 distinct versions")

seen_v, kmark, vmark, guard = [], None, None, 0
while True:
    guard += 1
    if guard > 25:
        fatal("ListObjectVersions pagination did not terminate in 25 pages (loop)")
    kw = {"Bucket": "ver-page", "MaxKeys": 2}
    if kmark is not None:
        kw["KeyMarker"] = kmark
    if vmark is not None:
        kw["VersionIdMarker"] = vmark
    r = s3.list_object_versions(**kw)
    page = [(v["Key"], v["VersionId"]) for v in r.get("Versions", [])]
    check(f"versions page {guard}: at most MaxKeys entries", len(page) <= 2)
    check(f"versions page {guard}: KeyMarker echoed", r.get("KeyMarker", "") == (kmark or ""))
    seen_v += page
    if not r["IsTruncated"]:
        check("final versions page carries no NextKeyMarker", "NextKeyMarker" not in r)
        break
    if not check(f"versions page {guard}: truncated page carries NextKeyMarker",
                 r.get("NextKeyMarker")):
        fatal("cannot continue ListObjectVersions pagination")
    # The PR #1 regression: the marker is a PLAIN key + a PLAIN version id, never base64.
    check(f"versions page {guard}: NextKeyMarker is a plain key, not base64",
          r["NextKeyMarker"] in VKEYS)
    check(f"versions page {guard}: NextVersionIdMarker is a real version id",
          r.get("NextVersionIdMarker") in {v for _, v in expected})
    kmark, vmark = r["NextKeyMarker"], r.get("NextVersionIdMarker")

check("ListObjectVersions pagination terminated", True)
check("ListObjectVersions pagination emitted no duplicates",
      len(seen_v) == len(set(seen_v)))
check("ListObjectVersions pagination returned every (key, version) exactly once",
      set(seen_v) == expected and len(seen_v) == 9)
check("ListObjectVersions pagination took 5 pages for 9 versions @ max-keys=2", guard == 5)
check("ListObjectVersions returns keys in ascending order",
      [k for k, _ in seen_v] == sorted(k for k, _ in seen_v))
check("exactly one version per key IsLatest",
      sorted(v["Key"] for v in s3.list_object_versions(Bucket="ver-page")["Versions"]
             if v["IsLatest"]) == VKEYS)

# The PR #1 property, isolated: a KeyMarker that is ITSELF VALID BASE64 must be consumed as a
# literal key. If it were base64-decoded, `aaaa` -> b'i\xa6\x9a' would seek past every ASCII key
# and the listing would come back EMPTY (or, for the old bug, silently restart at page 1).
r = s3.list_object_versions(Bucket="ver-page", KeyMarker="aaaa")
got_keys = {v["Key"] for v in r.get("Versions", [])}
check("a KeyMarker that is valid base64 is consumed as a literal key, not decoded",
      {"bbbb/cc", "zzz"} <= got_keys)
check("a base64-shaped KeyMarker is echoed back verbatim", r.get("KeyMarker") == "aaaa")

# KNOWN GAP (see report, finding B): with `key-marker` given and NO `version-id-marker`, S3 begins
# the listing IMMEDIATELY AFTER that key. `list_impl` seeks `key >= cursor` and only skips within
# the marker key when a `version_id_marker` is zipped alongside it (`vid_marker`), so a bare
# key-marker re-returns every version of the marker key. Cairn's own page loop above always sends
# the PAIR, which is why it is duplicate-free — but a client that saved only the key (or synthesised
# one) gets a whole key's versions twice. Same root cause and same class as finding A.
r = s3.list_object_versions(Bucket="ver-page", KeyMarker="bbbb/cc")
check("a bare KeyMarker resumes strictly AFTER that key",
      {v["Key"] for v in r.get("Versions", [])} == {"zzz"})
r = s3.list_object_versions(Bucket="ver-page", KeyMarker="aaaa")
check("a bare KeyMarker does not re-return the marker key's own versions",
      "aaaa" not in {v["Key"] for v in r.get("Versions", [])})

# Versions listing shares the delimiter machinery with the object listing.
r = s3.list_object_versions(Bucket="ver-page", Delimiter="/")
check("ListObjectVersions groups on a delimiter",
      [p["Prefix"] for p in r.get("CommonPrefixes", [])] == ["bbbb/"]
      and sorted({v["Key"] for v in r["Versions"]}) == ["aaaa", "zzz"])
r = s3.list_object_versions(Bucket="ver-page", Prefix="bbbb/", Delimiter="/")
check("ListObjectVersions honours prefix + delimiter together",
      {v["Key"] for v in r["Versions"]} == {"bbbb/cc"} and "CommonPrefixes" not in r)
try:
    s3.list_object_versions(Bucket="ver-no-such-bucket")
    check("ListObjectVersions on a nonexistent bucket is rejected", False)
except ClientError as e:
    check("ListObjectVersions on a nonexistent bucket -> 404 NoSuchBucket",
          status_of(e) == 404 and code_of(e) == "NoSuchBucket")

# An unversioned bucket still answers ListObjectVersions, with the 'null' sentinel.
r = s3.list_object_versions(Bucket="lst-page", Prefix="k00")
check("an unversioned bucket lists its object as the 'null' version",
      [v["VersionId"] for v in r["Versions"]] == ["null"]
      and r["Versions"][0]["IsLatest"] is True)


# =================================================================================================
print()
if FAILURES:
    print(f"LISTING/VERSIONING: {len(FAILURES)} FAILED CHECK(S)")
    for f in FAILURES:
        print(f"  - {f}")
    sys.exit(1)
print("LISTING/VERSIONING OK — pagination round-trips, delimiters group, versions undelete")
