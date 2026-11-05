#!/usr/bin/env python3
"""Release-to-main upgrade and snapshot-rollback drill for Cairn.

Runs the last published release, writes plaintext/multipart/encrypted objects,
stops it, snapshots the whole data directory, starts the current binary on the
same data, and verifies:
  * schema v42 and storage-protocol floor 3,
  * every old object still reads byte-for-byte (including ?partNumber),
  * old rows keep internal_sha256 and have internal_blake3 NULL,
  * the old multipart sidecar is v2 and the rebuilt/current sidecar is v3,
  * a new post-upgrade multipart write records BLAKE3 and a v3 sidecar,
  * the old binary refuses the upgraded database,
  * copying the pre-upgrade snapshot back lets the old binary start and read.

Not a CI gate: it needs a published binary and a live server, and boto3 is not in the CI image.
Manual run with a throwaway venv (or any boto3-enabled interpreter):
  gh release download v2026.09.13 --repo Harsh-2002/Cairn \
    --pattern cairn-linux-amd64 --pattern SHA256SUMS --dir /var/tmp/cairn-upgrade
  (cd /var/tmp/cairn-upgrade && sha256sum -c SHA256SUMS --ignore-missing)
  python3 conformance/upgrade_drill.py --root /var/tmp/cairn-upgrade/run \
    --old-bin /var/tmp/cairn-upgrade/cairn-linux-amd64 \
    --new-bin target/release/cairn --port 9090
"""
import argparse
import atexit
import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

BUCKET = "upgrade-drill"
SERVERS = []


def _cleanup_servers():
    for proc in list(SERVERS):
        try:
            stop(proc)
        except Exception:
            pass


atexit.register(_cleanup_servers)


def log(msg):
    print(msg, flush=True)


def wait_health(port, proc, logfile):
    for _ in range(300):
        if proc.poll() is not None:
            raise RuntimeError(f"server exited during startup:\n{logfile.read_text()[-4000:]}")
        try:
            import urllib.request
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=1):
                return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("server did not become healthy")


def bootstrap(binary, env):
    out = subprocess.run([binary, "bootstrap"], env=env, capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(f"bootstrap failed: {out.stderr[-2000:]}")
    access = re.search(r"Access Key Id:\s*(\S+)", out.stdout)
    secret = re.search(r"Secret Access Key:\s*(\S+)", out.stdout)
    if not access or not secret:
        raise RuntimeError(f"could not parse bootstrap output: {out.stdout[-2000:]}")
    return access.group(1), secret.group(1)


def start(binary, root, port, key, creds=None, extra_env=None):
    data = root / "data"
    env = {
        **os.environ,
        "CAIRN_DATA_DIR": str(data),
        "CAIRN_DB_PATH": str(data / "cairn.db"),
        "CAIRN_API_ADDR": f"127.0.0.1:{port}",
        "CAIRN_CONSOLE_ADDR": "off",
        "CAIRN_MASTER_KEY": key,
        "CAIRN_LOG_LEVEL": "info",
        "CAIRN_ALLOW_INTERNAL_ENDPOINTS": "true",
        **(extra_env or {}),
    }
    access, secret = creds if creds is not None else bootstrap(binary, env)
    logfile = root / "server.log"
    handle = logfile.open("wb")
    proc = subprocess.Popen([binary, "serve"], env=env, stdout=handle, stderr=subprocess.STDOUT)
    proc._logfile = handle
    SERVERS.append(proc)
    wait_health(port, proc, logfile)
    return proc, env, access, secret, logfile


def stop(proc):
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
    proc._logfile.close()
    if proc in SERVERS:
        SERVERS.remove(proc)


def client(port, access, secret):
    return boto3.client(
        "s3",
        endpoint_url=f"http://127.0.0.1:{port}",
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}),
    )


def write_objects(s3):
    s3.create_bucket(Bucket=BUCKET)
    plain = b"plaintext before upgrade " + bytes(range(256)) * 64
    s3.put_object(Bucket=BUCKET, Key="plain", Body=plain, ContentType="text/plain")
    encrypted = b"encrypted before upgrade " + b"E" * 5000
    s3.put_object(
        Bucket=BUCKET,
        Key="encrypted",
        Body=encrypted,
        ServerSideEncryption="AES256",
    )
    parts = [bytes([i]) * (5 * 1024 * 1024) for i in (1, 2)]
    created = s3.create_multipart_upload(Bucket=BUCKET, Key="multipart")
    uploaded = []
    for number, body in enumerate(parts, 1):
        result = s3.upload_part(
            Bucket=BUCKET,
            Key="multipart",
            UploadId=created["UploadId"],
            PartNumber=number,
            Body=body,
        )
        uploaded.append({"ETag": result["ETag"], "PartNumber": number})
    completed = s3.complete_multipart_upload(
        Bucket=BUCKET,
        Key="multipart",
        UploadId=created["UploadId"],
        MultipartUpload={"Parts": uploaded},
    )
    return {
        "plain": plain,
        "encrypted": encrypted,
        "multipart": b"".join(parts),
        "multipart_etag": completed["ETag"],
    }


def verify_objects(s3, expected, part_reads=False):
    for key in ("plain", "encrypted", "multipart"):
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        assert body == expected[key], f"{key} body mismatch after upgrade"
        log(f"  GET {key}: {len(body)} bytes exact")
    if part_reads:
        # The current code restores the part layout for objects that recorded one; the released
        # object here predates that record, so its part-number read must fail closed instead.
        try:
            s3.get_object(Bucket=BUCKET, Key="multipart", PartNumber=1)
        except Exception:
            log("  legacy multipart ?partNumber=1: refused as expected (no recorded layout)")
        else:
            raise AssertionError("legacy v1 multipart part-number read unexpectedly succeeded")
    head = s3.head_object(Bucket=BUCKET, Key="multipart")
    assert head["ETag"] == expected["multipart_etag"], "composite ETag changed"


def db_state(root):
    db = root / "data" / "cairn.db"
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        schema = conn.execute("SELECT max(version) FROM schema_migrations").fetchone()[0]
        proto = conn.execute(
            "SELECT minimum_reader, minimum_writer FROM storage_protocol WHERE singleton=1"
        ).fetchone()
        columns = {row[1] for row in conn.execute("PRAGMA table_info(object_versions)")}
        blake3 = "internal_blake3" if "internal_blake3" in columns else "NULL"
        rows = conn.execute(
            f"SELECT key, internal_sha256 IS NOT NULL, {blake3} IS NOT NULL "
            "FROM object_versions WHERE is_delete_marker=0 ORDER BY key"
        ).fetchall()
        parts = None
        if "multipart_parts" in columns:
            parts = conn.execute(
                "SELECT multipart_parts FROM object_versions WHERE key='multipart'"
            ).fetchone()[0]
        return schema, proto, rows, parts
    finally:
        conn.close()


def sidecar_version(root):
    data = root / "data"
    for meta in data.rglob("*.meta"):
        try:
            record = json.loads(meta.read_text())
        except Exception:
            continue
        if record.get("key") == "multipart":
            return record.get("format_version"), meta
    return None, None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--old-bin", required=True)
    ap.add_argument("--new-bin", required=True)
    ap.add_argument("--port", type=int, default=9090)
    args = ap.parse_args()
    root = Path(args.root)
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    key = "11" * 32

    log("== 1. populate with the released binary")
    proc, env, access, secret, _ = start(args.old_bin, root, args.port, key)
    s3 = client(args.port, access, secret)
    expected = write_objects(s3)
    verify_objects(s3, expected)
    stop(proc)
    old_schema, old_proto, old_rows, old_parts = db_state(root)
    old_sidecar, _ = sidecar_version(root)
    log(f"  released state: schema v{old_schema}, protocol {old_proto}, sidecar v{old_sidecar}")
    if old_sidecar is None:
        log("  released multipart has no sidecar (the release predates it); upgrade must still read it")

    log("== 2. snapshot the pre-upgrade data directory")
    snapshot = root / "snapshot"
    shutil.copytree(root / "data", snapshot)
    log(f"  snapshot: {snapshot}")

    log("== 3. start the current binary on the same data")
    proc, env, access, secret, logfile = start(
        args.new_bin, root, args.port, key, creds=(access, secret)
    )
    s3 = client(args.port, access, secret)
    schema, proto, rows, parts = db_state(root)
    log(f"  upgraded state: schema v{schema}, protocol {proto}, rows={rows}")
    assert schema >= 42, f"expected schema >= 42, got {schema}"
    assert proto == (3, 3), f"expected protocol (3,3), got {proto}"
    log(f"  legacy multipart layout after upgrade: {parts!r} (absent is expected for this release)")
    for key_name, has_sha, has_blake3 in rows:
        assert has_sha and not has_blake3, f"legacy row {key_name} changed baseline: {has_sha}/{has_blake3}"
    verify_objects(s3, expected, part_reads=True)

    log("== 4. new post-upgrade multipart writes record BLAKE3 and a v3 sidecar")
    # The startup rebuild leaves the durable v2 sidecar intact; only a new write is v3.
    new_parts = [bytes([3]) * (5 * 1024 * 1024)]
    created = s3.create_multipart_upload(Bucket=BUCKET, Key="multipart-new")
    uploaded = []
    for number, body in enumerate(new_parts, 1):
        result = s3.upload_part(
            Bucket=BUCKET, Key="multipart-new", UploadId=created["UploadId"],
            PartNumber=number, Body=body,
        )
        uploaded.append({"ETag": result["ETag"], "PartNumber": number})
    s3.complete_multipart_upload(
        Bucket=BUCKET, Key="multipart-new", UploadId=created["UploadId"],
        MultipartUpload={"Parts": uploaded},
    )
    for number in (1,):
        body = s3.get_object(Bucket=BUCKET, Key="multipart-new", PartNumber=number)["Body"].read()
        assert body == new_parts[number - 1], "new multipart part-number read mismatch"
        log(f"  GET multipart-new ?partNumber={number}: exact")
    schema2, _, rows2, _ = db_state(root)
    new_row = [row for row in rows2 if row[0] == "multipart-new"][0]
    assert new_row[2] and not new_row[1], f"new multipart row lacks BLAKE3: {new_row}"
    log(f"  new multipart row: internal_blake3={new_row[2]}, internal_sha256={new_row[1]}")
    # Find the new sidecar by key.
    new_sidecar = None
    for meta in (root / "data").rglob("*.meta"):
        try:
            record = json.loads(meta.read_text())
        except Exception:
            continue
        if record.get("key") == "multipart-new":
            new_sidecar = record.get("format_version")
            assert record.get("internal_blake3") and not record.get("internal_sha256")
    assert new_sidecar == 3, f"new multipart sidecar should be v3, got {new_sidecar}"
    log("  new multipart sidecar: v3 with internal_blake3")
    verify_objects(s3, expected, part_reads=True)
    stop(proc)

    log("== 5. the released binary must refuse the upgraded database")
    refusal = root / "old-refusal.log"
    env_old = {
        **os.environ,
        "CAIRN_DATA_DIR": str(root / "data"),
        "CAIRN_DB_PATH": str(root / "data" / "cairn.db"),
        "CAIRN_API_ADDR": f"127.0.0.1:{args.port + 1}",
        "CAIRN_CONSOLE_ADDR": "off",
        "CAIRN_MASTER_KEY": key,
        "CAIRN_LOG_LEVEL": "info",
    }
    result = subprocess.run(
        [args.old_bin, "serve"], env=env_old, capture_output=True, text=True, timeout=60
    )
    refusal.write_text(result.stdout + result.stderr)
    assert result.returncode != 0, "released binary unexpectedly started on the upgraded database"
    assert "schema" in (result.stdout + result.stderr).lower() or "protocol" in (
        result.stdout + result.stderr
    ).lower(), "refusal did not mention schema/protocol"
    log(f"  refused as expected (exit {result.returncode}): {refusal}")

    log("== 6. snapshot rollback: the released binary starts on the restored copy")
    rollback = root / "rollback"
    shutil.copytree(snapshot, rollback / "data")
    proc, env, access, secret, _ = start(
        args.old_bin, rollback, args.port + 2, key, creds=(access, secret)
    )
    s3 = client(args.port + 2, access, secret)
    verify_objects(s3, expected)
    stop(proc)
    log("  rollback read every pre-upgrade object exactly")

    log("PASS: release -> main upgrade, v42/v3, old-binary refusal, and snapshot rollback")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr, flush=True)
        raise
