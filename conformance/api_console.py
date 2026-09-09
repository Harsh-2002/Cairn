#!/usr/bin/env python3
"""API/console boundary, public download, endpoint migration and headless CLI regressions."""
import contextlib
import hashlib
import http.client
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
from urllib.parse import urlsplit

import boto3
from botocore.config import Config as S3Config
from botocore.auth import SigV4Auth, SigV4QueryAuth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

ROOT = Path(__file__).resolve().parent.parent
BIN = str(Path(os.environ.get("BIN", ROOT / "target/debug/cairn")).resolve())


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request(base, method, path, body=None, headers=None):
    headers = dict(headers or {})
    if isinstance(body, dict):
        body = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    url = urlsplit(base)
    conn = http.client.HTTPConnection(url.hostname, url.port, timeout=10)
    try:
        conn.request(method, path, body, headers)
        res = conn.getresponse()
        return res.status, dict(res.getheaders()), res.read()
    finally:
        conn.close()


def expect(result, status):
    assert result[0] == status, (result[0], status, result[2][:200])
    return result


@contextlib.contextmanager
def node(headless=False, **config):
    with tempfile.TemporaryDirectory(prefix="cairn-api-console-") as tmp:
        api_port, console_port = free_port(), free_port()
        while api_port == console_port:
            console_port = free_port()
        env = {k: v for k, v in os.environ.items() if not k.startswith("CAIRN_")}
        env.update(CAIRN_API_ADDR=f"127.0.0.1:{api_port}",
                   CAIRN_CONSOLE_ADDR="off" if headless else f"127.0.0.1:{console_port}",
                   CAIRN_DATA_DIR=f"{tmp}/data", CAIRN_DB_PATH=f"{tmp}/data/meta.db",
                   CAIRN_MASTER_KEY=secrets.token_hex(32), CAIRN_ROOT_ACCESS_KEY="api-console",
                   CAIRN_ROOT_SECRET_KEY=secrets.token_hex(24), CAIRN_UPDATE_CHECK_ENABLED="false")
        env.update(config)
        subprocess.run([BIN, "bootstrap"], env=env, check=True, capture_output=True)
        with open(f"{tmp}/server.log", "w+") as log:
            proc = subprocess.Popen([BIN, "serve"], env=env, stdout=log, stderr=log)
            api, console = f"http://127.0.0.1:{api_port}", f"http://127.0.0.1:{console_port}"
            try:
                deadline = time.monotonic() + 30
                while True:
                    if proc.poll() is not None:
                        raise AssertionError("server exited before readiness")
                    try:
                        if request(api, "GET", "/readyz")[0] == 200:
                            break
                    except OSError:
                        pass
                    assert time.monotonic() < deadline, "readiness timeout"
                    time.sleep(.05)
                yield api, console, env
            finally:
                proc.terminate()
                try:
                    proc.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                    raise AssertionError("server did not drain")


def check_node():
    with node() as (api, console, env):
        auth = {"Authorization": f"Bearer {env['CAIRN_ROOT_ACCESS_KEY']}.{env['CAIRN_ROOT_SECRET_KEY']}"}
        login = expect(request(console, "POST", "/api/v1/session", {
            "access_key": env["CAIRN_ROOT_ACCESS_KEY"], "secret_key": env["CAIRN_ROOT_SECRET_KEY"],
        }, {"Origin": console}), 200)
        cookie = {"Cookie": login[1]["set-cookie"].split(";")[0]}
        for method in ["GET", "POST", "DELETE", "OPTIONS"]:
            for path in ["/api/v1/session", "/api/v1/events/ticket", "/api/v1/events/stream"]:
                expect(request(api, method, path, headers=auth), 404)
        expect(request(api, "GET", "/api/v1/overview", headers=cookie), 403)
        expect(request(console, "GET", "/api/v1/overview", headers=cookie), 200)
        for path in ["/overview", "/system", "/system/endpoints", "/system/crypto-status", "/users", "/buckets", "/tags", "/activity", "/credentials/temporary", "/imports"]:
            expect(request(api, "GET", "/api/v1" + path, headers=auth), 200)
            expect(request(console, "GET", "/api/v1" + path, headers=auth), 200)
            expect(request(api, "GET", "/api/v1" + path), 403)
        expect(request(api, "POST", "/api/v1/buckets", {"name": "boundary"}, auth), 201)
        member = json.loads(expect(request(api, "POST", "/api/v1/users", {"display_name": "Member", "role": "member"}, auth), 201)[2])
        member_auth = {"Authorization": f"Bearer {member['bearer_access_key_id']}.{member['bearer_secret']}"}
        expect(request(api, "POST", "/api/v1/buckets", {"name": "forbidden"}, member_auth), 403)

        # A real SDK signer verifies that the management body is bound to its signature.
        body = b'{"name":"signed"}'
        signed = AWSRequest(method="POST", url=api + "/api/v1/buckets", data=body,
                            headers={"x-amz-content-sha256": hashlib.sha256(body).hexdigest(), "Content-Type": "application/json"})
        SigV4Auth(Credentials(env["CAIRN_ROOT_ACCESS_KEY"], env["CAIRN_ROOT_SECRET_KEY"]), "s3", "us-east-1").add_auth(signed)
        expect(request(api, "POST", "/api/v1/buckets", b'{"name":"tampered"}', dict(signed.headers)), 400)
        expect(request(api, "POST", "/api/v1/buckets", body, dict(signed.headers)), 201)
        expect(request(api, "GET", "/api/v1/buckets/tampered", headers=auth), 404)
        presigned = AWSRequest(method="GET", url=api + "/api/v1/overview")
        SigV4QueryAuth(Credentials(env["CAIRN_ROOT_ACCESS_KEY"], env["CAIRN_ROOT_SECRET_KEY"]), "s3", "us-east-1").add_auth(presigned)
        parsed = urlsplit(presigned.url)
        expect(request(api, "GET", parsed.path + "?" + parsed.query), 403)

        expect(request(api, "PUT", "/api/v1/buckets/boundary/encryption", {"algorithm": "AES256"}, auth), 204)
        payload = b'<script>window.shareExecuted=true;document.cookie="injected=1"</script>'
        expect(request(api, "PUT", "/boundary/active.html", payload, {**auth, "Content-Type": "text/html"}), 200)
        for base in [api, console]:
            share = json.loads(expect(request(base, "POST", "/api/v1/buckets/boundary/objects/shares", {
                "key": "active.html", "delivery": "console_download", "expires_in_secs": 3600,
            }, auth), 200)[2])
            assert share["url"].startswith(console + "/share/")
            path = urlsplit(share["url"]).path
            downloaded = expect(request(console, "GET", path, headers={"Cookie": "cairn_session=invalid", "Authorization": "Bearer invalid"}), 200)
            assert downloaded[2] == payload
            assert downloaded[1]["content-type"] == "application/octet-stream"
            assert downloaded[1]["content-disposition"].startswith("attachment")
            for key, value in [("x-content-type-options", "nosniff"), ("cache-control", "no-store"), ("referrer-policy", "no-referrer")]:
                assert downloaded[1][key] == value
            assert "set-cookie" not in downloaded[1]
            assert expect(request(console, "HEAD", path), 200)[2] == b""
            assert expect(request(console, "GET", path, headers={"Range": "bytes=0-6"}), 206)[2] == payload[:7]
            expect(request(console, "GET", path, headers={"If-None-Match": downloaded[1]["etag"]}), 304)
            expect(request(console, "GET", path, headers={"Service-Worker": "script"}), 403)
            expect(request(console, "PUT", path, b"replace", auth), 404)
            expect(request(api, "DELETE", "/api/v1/buckets/boundary/objects/shares/" + share["id"], headers=auth), 204)
            expect(request(console, "GET", path), 410)
        expect(request(console, "GET", "/boundary/active.html", headers=cookie), 404)
        # The loopback convenience never guesses a public API hostname from a console hostname.
        diagnostic = json.loads(expect(request(console, "GET", "/api/v1/system/endpoints", headers={**auth, "Host": "console.example.test"}), 200)[2])
        assert diagnostic["issues"][0]["code"] == "ApiPublicUrlRequired"
        shares_path = "/api/v1/buckets/boundary/objects/shares"
        before = expect(request(console, "GET", shares_path, headers=auth), 200)[2]
        expect(request(console, "POST", shares_path, {"key": "active.html", "delivery": "api"},
                       {**auth, "Host": "console.example.test"}), 400)
        assert expect(request(console, "GET", shares_path, headers=auth), 200)[2] == before
        if os.environ.get("BROWSER") == "1":
            browser_env = {k: v for k, v in os.environ.items() if not k.startswith("CAIRN_")}
            browser_env.update(CAIRN_E2E_CONSOLE_URL=console, CAIRN_E2E_ACCESS_KEY=env["CAIRN_ROOT_ACCESS_KEY"], CAIRN_E2E_SECRET_KEY=env["CAIRN_ROOT_SECRET_KEY"])
            subprocess.run(["node", "web/scripts/console-e2e.mjs"], cwd=ROOT, env=browser_env, check=True, timeout=180)
    print("API/console authentication, signed bodies and encrypted public downloads passed")


def check_headless():
    with node(headless=True) as (api, console, env):
        for old, new in [("CAIRN_LISTEN_ADDR", "CAIRN_API_ADDR"), ("CAIRN_WEB_ADDR", "CAIRN_CONSOLE_ADDR"), ("CAIRN_PUBLIC_BASE_URL", "CAIRN_API_PUBLIC_URL")]:
            result = subprocess.run([BIN, "validate-config"], env={**env, old: "do-not-print-this"}, capture_output=True)
            assert result.returncode != 0 and new.encode() in result.stderr
            assert b"do-not-print-this" not in result.stderr
        sdk = boto3.client("s3", endpoint_url=api, region_name="us-east-1",
                           aws_access_key_id=env["CAIRN_ROOT_ACCESS_KEY"],
                           aws_secret_access_key=env["CAIRN_ROOT_SECRET_KEY"],
                           config=S3Config(signature_version="s3v4", s3={"addressing_style": "path"}))
        sdk.create_bucket(Bucket="headless-sdk")
        sdk.put_object(Bucket="headless-sdk", Key="sdk", Body=b"sdk headless")
        assert sdk.get_object(Bucket="headless-sdk", Key="sdk")["Body"].read() == b"sdk headless"
        client_env = {k: v for k, v in os.environ.items() if not k.startswith("CAIRN_")}
        client_env.update(CAIRN_API_ENDPOINT=api, CAIRN_ACCESS_KEY=env["CAIRN_ROOT_ACCESS_KEY"], CAIRN_SECRET_KEY=env["CAIRN_ROOT_SECRET_KEY"])
        result = subprocess.run([BIN, "overview", "--json"], env=client_env, check=True, capture_output=True)
        assert "buckets" in json.loads(result.stdout)
        for command in ["overview", "object", "share"]:
            result = subprocess.run([BIN, command, "--help"], env=client_env, check=True, capture_output=True)
            assert b"CAIRN_SECRET_KEY" in result.stdout
            assert client_env["CAIRN_SECRET_KEY"].encode() not in result.stdout + result.stderr
        for old in ["CAIRN_ENDPOINT", "CAIRN_S3_ENDPOINT"]:
            result = subprocess.run([BIN, "overview"], env={**client_env, old: "do-not-print-this"}, capture_output=True)
            assert result.returncode != 0 and b"CAIRN_API_ENDPOINT" in result.stderr
            assert b"do-not-print-this" not in result.stderr
        auth = {"Authorization": f"Bearer {env['CAIRN_ROOT_ACCESS_KEY']}.{env['CAIRN_ROOT_SECRET_KEY']}"}
        expect(request(api, "PUT", "/headless", headers=auth), 200)
        expect(request(api, "PUT", "/headless/test", b"headless", auth), 200)
        for delivery, status in [("api", 200), ("console_download", 400)]:
            result = expect(request(api, "POST", "/api/v1/buckets/headless/objects/shares", {"key": "test", "delivery": delivery}, auth), status)
            if status == 200:
                path = urlsplit(json.loads(result[2])["url"]).path
                assert expect(request(api, "GET", path), 200)[2] == b"headless"
        try:
            request(console, "GET", "/")
        except OSError:
            pass
        else:
            raise AssertionError("headless node bound a console")
    print("Headless native administration, S3, shares and CLI migration passed")


def check_configured_origins():
    with node(CAIRN_API_PUBLIC_URL="https://api.example.test:443/",
              CAIRN_CONSOLE_PUBLIC_URL="https://console.example.test/") as (api, console, env):
        auth = {"Authorization": f"Bearer {env['CAIRN_ROOT_ACCESS_KEY']}.{env['CAIRN_ROOT_SECRET_KEY']}"}
        status = json.loads(expect(request(console, "GET", "/api/v1/system/endpoints", headers=auth), 200)[2])
        assert status["api_url"] == "https://api.example.test"
        assert status["console_url"] == "https://console.example.test"
        assert status["issues"] == []
        expect(request(api, "PUT", "/configured", headers=auth), 200)
        expect(request(api, "PUT", "/configured/test", b"configured", auth), 200)
        for delivery, origin in [("api", status["api_url"]), ("console_download", status["console_url"])]:
            share = json.loads(expect(request(console, "POST", "/api/v1/buckets/configured/objects/shares",
                                  {"key": "test", "delivery": delivery}, auth), 200)[2])
            assert share["url"].startswith(origin + "/share/")
        for setting, value in [("CAIRN_API_PUBLIC_URL", "https://console.example.test:443"),
                               ("CAIRN_CONSOLE_PUBLIC_URL", "https://bad.example/path")]:
            result = subprocess.run([BIN, "validate-config"], env={**env, setting: value}, capture_output=True)
            assert result.returncode != 0
    print("Explicit public origins and invalid configuration refusal passed")


if __name__ == "__main__":
    check_node()
    check_headless()
    check_configured_origins()
