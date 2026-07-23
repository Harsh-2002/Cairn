#!/usr/bin/env python3
"""Lifecycle background-loop ENFORCEMENT conformance (ARCH 19.1-19.3).

`conformance.py` checks lifecycle with the literal `check("lifecycle expiration accepted", True)` —
it proves a configuration is *accepted* and nothing whatsoever about a rule ever being *applied*.
This driver proves the scanner acts, and acts only where it is allowed to:

  * an Enabled expiration rule with a prefix filter deletes the matching object and leaves the
    non-matching one alone (an unversioned bucket: the data really goes away);
  * a rule on bucket A never touches an identically-keyed object in bucket B;
  * a Disabled rule does nothing;
  * on a VERSIONED bucket, expiration inserts a delete marker instead of destroying data — the
    noncurrent versions stay retrievable by version id, and the hidden key answers GET with
    404 NoSuchKey + `x-amz-delete-marker: true` (ARCH 16.1);
  * PUT -> GET -> DELETE of the configuration document itself round-trips, a missing configuration
    answers 404 `NoSuchLifecycleConfiguration`, and a storage-class Transition rule is REJECTED with
    501 NotImplemented rather than stored as a silent no-op (and rejecting stores nothing).

No fixed sleep waits for a scan. The launcher sets `CAIRN_LIFECYCLE_INTERVAL_SECS=1` and every wait
here polls for observable state with a bounded retry budget and a loud timeout message.

Making an object expire *now* uses `Expiration.Date` in the past, which the scanner evaluates as
`now >= date` independent of object age (`matching_expiration_rule` in cairn-lifecycle/scanner.rs) —
the same semantics as S3, and the only way to get a sub-day test. `Days` would need a day to elapse.

Args: <sigv4_access_key> <sigv4_secret> <s3_endpoint>
"""

import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

akid, secret, endpoint = sys.argv[1], sys.argv[2], sys.argv[3]

REGION = "us-east-1"
# A midnight-UTC date well in the past: due immediately, and midnight-aligned so the configuration
# is one real S3 would also accept (AWS rejects a non-midnight `Date`).
PAST = datetime(2020, 1, 1, tzinfo=timezone.utc)
# Bounded poll budget for "the scanner did something" (interval is 1s).
POLL_TIMEOUT_S = 30.0
POLL_INTERVAL_S = 0.25

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=akid,
    aws_secret_access_key=secret,
    region_name=REGION,
    config=Config(s3={"addressing_style": "path"}, retries={"total_max_attempts": 1, "mode": "standard"}),
)

_failed = False


def check(label, cond):
    global _failed
    if not cond:
        print(f"FAIL: {label}")
        _failed = True
    else:
        print(f"  ok: {label}")


def err(e):
    """(code, http_status) of a ClientError — the two things every assertion here pins."""
    return (
        e.response["Error"]["Code"],
        e.response["ResponseMetadata"]["HTTPStatusCode"],
    )


def get_body(bucket, key, **kw):
    return s3.get_object(Bucket=bucket, Key=key, **kw)["Body"].read()


def gone_reason(bucket, key):
    """None while the key still reads; otherwise the (code, status, headers) of its GET failure."""
    try:
        s3.get_object(Bucket=bucket, Key=key)
        return None
    except ClientError as e:
        code, status = err(e)
        return (code, status, e.response["ResponseMetadata"]["HTTPHeaders"])


def wait_expired(bucket, key, what):
    """Poll GET until the lifecycle scanner has removed/hidden `key`. Returns the failure tuple.

    Fails the run (rather than hanging or silently passing) if the budget runs out — the timeout
    message names the exact object so a red CI run is diagnosable without a rerun.
    """
    deadline = time.monotonic() + POLL_TIMEOUT_S
    while time.monotonic() < deadline:
        reason = gone_reason(bucket, key)
        if reason is not None:
            return reason
        time.sleep(POLL_INTERVAL_S)
    check(f"{what} (timed out after {POLL_TIMEOUT_S}s waiting for s3://{bucket}/{key})", False)
    return None


def expiration_rule(rule_id, prefix, status="Enabled"):
    return {
        "ID": rule_id,
        "Status": status,
        "Filter": {"Prefix": prefix},
        "Expiration": {"Date": PAST},
    }


# =================================================================================================
# Fixtures. Everything is created up front so a single scan pass sees the whole world; the object
# count is deliberately tiny (9 objects, 5 buckets) to keep the harness well under a minute.
# =================================================================================================

# `lc-expire`: the bucket the rule acts on. Prefix filter `tmp/`.
s3.create_bucket(Bucket="lc-expire")
s3.put_object(Bucket="lc-expire", Key="tmp/doomed", Body=b"doomed")
s3.put_object(Bucket="lc-expire", Key="tmp-not-a-folder", Body=b"prefix-match")
s3.put_object(Bucket="lc-expire", Key="keep/safe", Body=b"safe")

# `lc-other`: an identically-keyed object in a bucket with NO configuration of its own.
s3.create_bucket(Bucket="lc-other")
s3.put_object(Bucket="lc-other", Key="tmp/doomed", Body=b"other-bucket")

# `lc-disabled`: the same rule, Status=Disabled.
s3.create_bucket(Bucket="lc-disabled")
s3.put_object(Bucket="lc-disabled", Key="tmp/doomed", Body=b"disabled-rule")

# `lc-vers`: versioning Enabled, two generations of the matching key plus a non-matching key.
s3.create_bucket(Bucket="lc-vers")
s3.put_bucket_versioning(Bucket="lc-vers", VersioningConfiguration={"Status": "Enabled"})
v1 = s3.put_object(Bucket="lc-vers", Key="tmp/hist", Body=b"gen1")["VersionId"]
v2 = s3.put_object(Bucket="lc-vers", Key="tmp/hist", Body=b"gen2")["VersionId"]
s3.put_object(Bucket="lc-vers", Key="keep/hist", Body=b"keep")
check("versioned bucket issued two distinct version ids", v1 != v2 and v1 and v2)

s3.put_bucket_lifecycle_configuration(
    Bucket="lc-expire", LifecycleConfiguration={"Rules": [expiration_rule("exp", "tmp/")]}
)
s3.put_bucket_lifecycle_configuration(
    Bucket="lc-disabled",
    LifecycleConfiguration={"Rules": [expiration_rule("off", "tmp/", status="Disabled")]},
)
s3.put_bucket_lifecycle_configuration(
    Bucket="lc-vers", LifecycleConfiguration={"Rules": [expiration_rule("exp", "tmp/")]}
)

# =================================================================================================
# 1. The scanner expires a matching current object in an UNVERSIONED bucket.
# =================================================================================================
reason = wait_expired("lc-expire", "tmp/doomed", "scanner expired the prefix-matching object")
if reason:
    code, status, headers = reason
    check(
        "expired object is GONE: GET -> 404 NoSuchKey",
        code == "NoSuchKey" and status == 404,
    )
    check(
        "unversioned expiration DESTROYS data (no x-amz-delete-marker on the 404)",
        headers.get("x-amz-delete-marker") is None,
    )

keys = {o["Key"] for o in s3.list_objects_v2(Bucket="lc-expire").get("Contents", [])}
check("expired key is absent from ListObjectsV2", "tmp/doomed" not in keys)

# =================================================================================================
# 2. Two full scan passes, proven by sentinels rather than by sleeping.
#
# A sentinel is a fresh `tmp/` object; observing it expire proves a scan pass reached this bucket
# AFTER the sentinel was written. Two rounds guarantee that every OTHER bucket has also been fully
# scanned at least once since all fixtures existed, whatever order `list_buckets` yields — so the
# "survivor" assertions below are real negatives, not just "the scanner has not gotten there yet".
# =================================================================================================
for round_no in (1, 2):
    sentinel = f"tmp/sentinel-{round_no}"
    s3.put_object(Bucket="lc-expire", Key=sentinel, Body=b"sentinel")
    reason = wait_expired("lc-expire", sentinel, f"scan pass {round_no} observed (sentinel expired)")
    if reason:
        check(f"scan pass {round_no} expired {sentinel}", reason[0] == "NoSuchKey" and reason[1] == 404)

# =================================================================================================
# 3. Survivors: the filter, the bucket boundary, and Status=Disabled are all honoured.
# =================================================================================================
check(
    "non-matching prefix survives (keep/safe untouched)",
    get_body("lc-expire", "keep/safe") == b"safe",
)
check(
    "prefix is a byte prefix, not a folder: 'tmp-not-a-folder' does NOT match 'tmp/'",
    get_body("lc-expire", "tmp-not-a-folder") == b"prefix-match",
)
check(
    "rules are per-bucket: lc-other's identically-keyed object survives",
    get_body("lc-other", "tmp/doomed") == b"other-bucket",
)
check(
    "a Disabled rule does nothing",
    get_body("lc-disabled", "tmp/doomed") == b"disabled-rule",
)

# =================================================================================================
# 4. VERSIONED bucket: expiration inserts a delete marker; the data survives behind it.
# =================================================================================================
reason = wait_expired("lc-vers", "tmp/hist", "versioned expiration hid the current version")
if reason:
    code, status, headers = reason
    check(
        "versioned expiration: GET of the hidden key -> 404 NoSuchKey",
        code == "NoSuchKey" and status == 404,
    )
    check(
        "versioned expiration: the 404 carries x-amz-delete-marker: true",
        headers.get("x-amz-delete-marker") == "true",
    )

listing = s3.list_object_versions(Bucket="lc-vers", Prefix="tmp/hist")
versions = listing.get("Versions", [])
markers = listing.get("DeleteMarkers", [])
check("versioned expiration created exactly one delete marker", len(markers) == 1)
check("versioned expiration deleted NO data (both versions still listed)", len(versions) == 2)
check("the delete marker is the latest version", bool(markers) and markers[0]["IsLatest"] is True)
check(
    "no object version claims IsLatest once a marker hides the key",
    all(v["IsLatest"] is False for v in versions),
)
check("noncurrent version v1 is still retrievable by version id",
      get_body("lc-vers", "tmp/hist", VersionId=v1) == b"gen1")
check("noncurrent version v2 is still retrievable by version id",
      get_body("lc-vers", "tmp/hist", VersionId=v2) == b"gen2")

if markers:
    try:
        s3.get_object(Bucket="lc-vers", Key="tmp/hist", VersionId=markers[0]["VersionId"])
        check("GET of the delete marker's own version id -> 405 MethodNotAllowed", False)
    except ClientError as e:
        check(
            "GET of the delete marker's own version id -> 405 MethodNotAllowed",
            err(e) == ("MethodNotAllowed", 405),
        )

check(
    "versioned bucket: the non-matching key keeps no marker",
    get_body("lc-vers", "keep/hist") == b"keep",
)
check(
    "versioned bucket: the non-matching key has no delete marker at all",
    len(s3.list_object_versions(Bucket="lc-vers", Prefix="keep/").get("DeleteMarkers", [])) == 0,
)

# The scanner is idempotent: `list_current` excludes delete markers, so a later pass must not stack a
# second marker on the already-hidden key (ARCH 19.2). Two more sentinel-proven passes, then recount.
for round_no in (3, 4):
    sentinel = f"tmp/sentinel-{round_no}"
    s3.put_object(Bucket="lc-expire", Key=sentinel, Body=b"sentinel")
    wait_expired("lc-expire", sentinel, f"scan pass {round_no} observed (sentinel expired)")
check(
    "idempotent: repeated scans do not stack a second delete marker",
    len(s3.list_object_versions(Bucket="lc-vers", Prefix="tmp/hist").get("DeleteMarkers", [])) == 1,
)

# =================================================================================================
# 5. The configuration document itself: PUT -> GET -> DELETE, and Transition is rejected.
# =================================================================================================
s3.create_bucket(Bucket="lc-cfg")

try:
    s3.get_bucket_lifecycle_configuration(Bucket="lc-cfg")
    check("GET lifecycle with no configuration -> 404 NoSuchLifecycleConfiguration", False)
except ClientError as e:
    check(
        "GET lifecycle with no configuration -> 404 NoSuchLifecycleConfiguration",
        err(e) == ("NoSuchLifecycleConfiguration", 404),
    )

put = s3.put_bucket_lifecycle_configuration(
    Bucket="lc-cfg",
    LifecycleConfiguration={
        "Rules": [
            {
                "ID": "cfg-rule",
                "Status": "Enabled",
                "Filter": {"Prefix": "logs/"},
                "Expiration": {"Days": 30},
            }
        ]
    },
)
check(
    "PUT lifecycle -> 204 No Content",
    put["ResponseMetadata"]["HTTPStatusCode"] == 204,
)

got = s3.get_bucket_lifecycle_configuration(Bucket="lc-cfg")
check("GET lifecycle -> 200", got["ResponseMetadata"]["HTTPStatusCode"] == 200)
rules = got.get("Rules", [])
check("GET lifecycle returns the one stored rule", len(rules) == 1)
if rules:
    r = rules[0]
    check("round-trip preserves ID", r.get("ID") == "cfg-rule")
    check("round-trip preserves Status", r.get("Status") == "Enabled")
    check("round-trip preserves the prefix filter",
          r.get("Filter", {}).get("Prefix") == "logs/" or r.get("Prefix") == "logs/")
    check("round-trip preserves Expiration.Days", r.get("Expiration", {}).get("Days") == 30)

# A Transition rule must FAIL LOUDLY (501) rather than be stored as a silent no-op: Cairn does not
# tier data (service.rs `put_bucket_config`, ConfigAspect::Lifecycle). This agrees with — and is
# stricter than — conformance.py's bare `except ClientError`.
try:
    s3.put_bucket_lifecycle_configuration(
        Bucket="lc-cfg",
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "tier",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "cold/"},
                    "Transitions": [{"Days": 30, "StorageClass": "GLACIER"}],
                }
            ]
        },
    )
    check("PUT lifecycle with a Transition -> 501 NotImplemented", False)
except ClientError as e:
    check(
        "PUT lifecycle with a Transition -> 501 NotImplemented",
        err(e) == ("NotImplemented", 501),
    )
    # KNOWN GAP (suspected product bug): `error_map::error_response` gates the descriptive
    # `<Message>` on `status.is_server_error()`, which is TRUE for 501 — so a deliberate, permanent
    # rejection is delivered as "We encountered an internal error. Please try again." and logged at
    # ERROR as "internal error serving request". Retrying can never succeed, and the whole point of
    # the rejection (service.rs: "Fail loudly ... instead of storing a no-op") is that the operator
    # learns Cairn does not tier data. NotImplemented is a permanent capability answer, not a
    # transient server fault; it belongs on the descriptive-message side of that branch.
    check(
        "the 501 message does not misreport a permanent rejection as a retryable internal error",
        "internal error" not in e.response["Error"].get("Message", "").lower(),
    )

rules_after = s3.get_bucket_lifecycle_configuration(Bucket="lc-cfg").get("Rules", [])
check(
    "the rejected Transition PUT stored nothing (the previous rule is intact)",
    len(rules_after) == 1 and rules_after[0].get("ID") == "cfg-rule",
)

delete = s3.delete_bucket_lifecycle(Bucket="lc-cfg")
check(
    "DELETE lifecycle -> 204 No Content",
    delete["ResponseMetadata"]["HTTPStatusCode"] == 204,
)
try:
    s3.get_bucket_lifecycle_configuration(Bucket="lc-cfg")
    check("GET after DELETE -> 404 NoSuchLifecycleConfiguration", False)
except ClientError as e:
    check(
        "GET after DELETE -> 404 NoSuchLifecycleConfiguration",
        err(e) == ("NoSuchLifecycleConfiguration", 404),
    )

# =================================================================================================
# 6. Deleting the configuration DISARMS the scanner: an object matching the deleted rule survives.
# =================================================================================================
s3.create_bucket(Bucket="lc-disarm")
s3.put_object(Bucket="lc-disarm", Key="tmp/survivor", Body=b"survivor")
s3.put_bucket_lifecycle_configuration(
    Bucket="lc-disarm", LifecycleConfiguration={"Rules": [expiration_rule("exp", "tmp/")]}
)
s3.delete_bucket_lifecycle(Bucket="lc-disarm")
for round_no in (5, 6):
    sentinel = f"tmp/sentinel-{round_no}"
    s3.put_object(Bucket="lc-expire", Key=sentinel, Body=b"sentinel")
    wait_expired("lc-expire", sentinel, f"scan pass {round_no} observed (sentinel expired)")
check(
    "deleting the configuration disarms the scanner for that bucket",
    get_body("lc-disarm", "tmp/survivor") == b"survivor",
)

if _failed:
    print("LIFECYCLE CONFORMANCE FAILED")
    sys.exit(1)
print("LIFECYCLE OK — the background scanner enforces rules, and only where it is allowed to")
