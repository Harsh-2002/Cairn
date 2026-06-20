#!/usr/bin/env python3
"""Webhook event-notification conformance: stand up a local HTTP sink, configure a bucket's webhook
endpoint through the management API, drive S3 PUT/DELETE with boto3, and assert the sink receives the
correctly-shaped, HMAC-signed S3 event records.

Args: <sigv4_access_key> <sigv4_secret> <s3_endpoint> <mgmt_endpoint> <bearer_token>

The SigV4 pair signs S3 requests; the Bearer token (a distinct `cairn_<id>.<secret>` credential)
authenticates the admin-gated management API.
"""

import hashlib
import hmac
import json
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

import boto3
from botocore.config import Config

ak, sk, s3_endpoint, mgmt_endpoint, bearer = (
    sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5],
)
SECRET = "topsecret-hmac-key"
RECEIVED = []
_lock = threading.Lock()


class Sink(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        length = int(self.headers.get("content-length", 0))
        body = self.rfile.read(length)
        with _lock:
            RECEIVED.append((dict(self.headers), body))
        self.send_response(200)
        self.end_headers()


def check(label, cond):
    if not cond:
        print(f"FAIL: {label}")
        sys.exit(1)
    print(f"  ok: {label}")


# Start the local sink on an ephemeral port.
server = HTTPServer(("127.0.0.1", 0), Sink)
sink_port = server.server_address[1]
threading.Thread(target=server.serve_forever, daemon=True).start()
sink_url = f"http://127.0.0.1:{sink_port}/hook"
print(f"sink listening at {sink_url}")

s3 = boto3.client(
    "s3", endpoint_url=s3_endpoint, aws_access_key_id=ak, aws_secret_access_key=sk,
    region_name="us-east-1", config=Config(s3={"addressing_style": "path"}),
)

# Create the bucket (notifications can only be set on an existing bucket).
s3.create_bucket(Bucket="notif")

# Configure the webhook endpoint via the management API (Bearer = "<access>.<secret>").
config = {
    "endpoints": [{
        "id": "e1",
        "url": sink_url,
        "events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
        "secret": SECRET,
    }]
}
req = urllib.request.Request(
    f"{mgmt_endpoint}/api/v1/buckets/notif/notifications",
    data=json.dumps(config).encode(),
    method="PUT",
    headers={"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"},
)
with urllib.request.urlopen(req) as resp:
    check("set notifications via management API", resp.status in (200, 204))

# Read it back: the secret must NOT be echoed, only a presence flag.
req = urllib.request.Request(
    f"{mgmt_endpoint}/api/v1/buckets/notif/notifications",
    headers={"Authorization": f"Bearer {bearer}"},
)
with urllib.request.urlopen(req) as resp:
    got = json.loads(resp.read())
check("endpoint listed", got["endpoints"][0]["id"] == "e1")
check("secret never echoed", "secret" not in got["endpoints"][0] and got["endpoints"][0]["has_secret"])


def wait_for(predicate, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with _lock:
            for headers, body in RECEIVED:
                if predicate(headers, body):
                    return headers, body
        time.sleep(0.3)
    return None


# PUT an object → expect an ObjectCreated:Put delivery, HMAC-signed.
s3.put_object(Bucket="notif", Key="hello.txt", Body=b"hi there")
hit = wait_for(lambda h, b: json.loads(b)["Records"][0]["eventName"] == "s3:ObjectCreated:Put")
check("ObjectCreated:Put delivered", hit is not None)
headers, body = hit
rec = json.loads(body)["Records"][0]
check("event carries the key", rec["s3"]["object"]["key"] == "hello.txt")
check("event carries the bucket", rec["s3"]["bucket"]["name"] == "notif")
# Verify the HMAC signature.
expected = "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
sig = headers.get("X-Cairn-Signature") or headers.get("x-cairn-signature")
check("HMAC signature valid", sig == expected)

# DELETE the object → expect an ObjectRemoved delivery (unversioned bucket → :Delete).
s3.delete_object(Bucket="notif", Key="hello.txt")
hit = wait_for(lambda h, b: json.loads(b)["Records"][0]["eventName"].startswith("s3:ObjectRemoved"))
check("ObjectRemoved delivered", hit is not None)

server.shutdown()
print("NOTIFICATIONS OK — webhook events delivered and HMAC-verified via the AWS SDK + mgmt API")
