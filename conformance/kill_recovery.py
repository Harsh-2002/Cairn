#!/usr/bin/env python3
"""Abrupt-process-kill recovery drill.

The crash harnesses arm task-level failpoints; this one sends SIGKILL while a
live PUT workload is in flight, then restarts the same data directory and
checks the actual recovery contract:

  * every PUT that was acknowledged before the kill reads back byte-for-byte;
  * a PUT whose response was lost is either fully present and correct or absent
    (never a 5xx, short body, or wrong bytes);
  * a multipart object completed before the kill keeps its full body and every
    ?partNumber after restart;
  * `cairn integrity` on the stopped store reports zero errors.

This is a process crash, not a power loss: the kernel page cache survives, so it
cannot prove disk-cache loss. It does exercise SQLite WAL rollback, the blob
commit sequence, startup rebuild/reconcile, and reflink-completed sidecars.

Requires boto3. Manual run:
  python3 conformance/kill_recovery.py --bin target/release/cairn --root /var/tmp/kill-test
"""
import argparse
import os
import re
import shutil
import signal
import subprocess
import threading
import time
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config

ROUNDS = 3


def log(msg):
    print(msg, flush=True)


def wait_health(port, proc, logfile):
    for _ in range(300):
        if proc.poll() is not None:
            raise RuntimeError(f"server exited:\n{logfile.read_text()[-3000:]}")
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=1)
            return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("server did not become healthy")


def env_for(root, port):
    return {
        **os.environ,
        "CAIRN_DATA_DIR": str(root / "data"),
        "CAIRN_DB_PATH": str(root / "data" / "cairn.db"),
        "CAIRN_API_ADDR": f"127.0.0.1:{port}",
        "CAIRN_CONSOLE_ADDR": "off",
        "CAIRN_MASTER_KEY": "33" * 32,
        "CAIRN_LOG_LEVEL": "error",
        "CAIRN_ALLOW_INTERNAL_ENDPOINTS": "true",
    }


def bootstrap(binary, env):
    out = subprocess.run([binary, "bootstrap"], env=env, capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(out.stderr[-2000:])
    return (
        re.search(r"Access Key Id:\s*(\S+)", out.stdout).group(1),
        re.search(r"Secret Access Key:\s*(\S+)", out.stdout).group(1),
    )


def start(binary, root, port):
    env = env_for(root, port)
    logfile = root / "server.log"
    handle = logfile.open("wb")
    proc = subprocess.Popen([binary, "serve"], env=env, stdout=handle, stderr=subprocess.STDOUT)
    wait_health(port, proc, logfile)
    return proc, env


def stop(proc):
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)


def client(port, access, secret):
    return boto3.client(
        "s3",
        endpoint_url=f"http://127.0.0.1:{port}",
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )


def put_multipart(s3, bucket, key, parts):
    created = s3.create_multipart_upload(Bucket=bucket, Key=key)
    uploaded = []
    for number, body in enumerate(parts, 1):
        result = s3.upload_part(
            Bucket=bucket,
            Key=key,
            UploadId=created["UploadId"],
            PartNumber=number,
            Body=body,
        )
        uploaded.append({"ETag": result["ETag"], "PartNumber": number})
    s3.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=created["UploadId"],
        MultipartUpload={"Parts": uploaded},
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--root", required=True)
    ap.add_argument("--port", type=int, default=9097)
    ap.add_argument("--rounds", type=int, default=ROUNDS)
    args = ap.parse_args()
    root = Path(args.root)
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)

    for round_no in range(1, args.rounds + 1):
        log(f"== round {round_no}: populate, SIGKILL under load, recover")
        rdir = root / f"round{round_no}"
        rdir.mkdir()
        access, secret = bootstrap(args.bin, env_for(rdir, args.port))
        proc, env = start(args.bin, rdir, args.port)
        s3 = client(args.port, access, secret)
        bucket = "kill"
        s3.create_bucket(Bucket=bucket)

        # A completed multipart object that must survive the kill with its part layout.
        parts = [bytes([7]) * (5 * 1024 * 1024), bytes([8]) * (5 * 1024 * 1024)]
        put_multipart(s3, bucket, "completed-multipart", parts)
        expected_mp = b"".join(parts)

        acked = {}
        attempted = {}
        lock = threading.Lock()
        stop_writers = threading.Event()

        def writer(worker):
            local = client(args.port, access, secret)
            counter = 0
            while not stop_writers.is_set():
                key = f"w{worker}/obj-{counter:06d}"
                body = bytes([worker]) * (64 * 1024 + counter % 4096)
                with lock:
                    attempted[key] = body
                try:
                    local.put_object(Bucket=bucket, Key=key, Body=body)
                except Exception:
                    continue
                with lock:
                    acked[key] = body
                counter += 1

        threads = [threading.Thread(target=writer, args=(i,), daemon=True) for i in range(6)]
        for thread in threads:
            thread.start()
        time.sleep(2.0)
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=20)
        stop_writers.set()
        for thread in threads:
            thread.join(timeout=10)

        # Offline integrity recovery on the stopped store.
        integrity = subprocess.run(
            [args.bin, "integrity"], env=env, capture_output=True, text=True
        )
        if integrity.returncode != 0:
            raise RuntimeError(f"integrity failed:\n{integrity.stdout}\n{integrity.stderr}")
        if "errors=0" not in (integrity.stdout + integrity.stderr):
            raise RuntimeError(f"integrity reported errors:\n{integrity.stdout}")
        log(f"  integrity after kill: {integrity.stdout.strip().splitlines()[-1]}")

        # Restart the same store and verify.
        proc, env = start(args.bin, rdir, args.port)
        s3 = client(args.port, access, secret)

        body = s3.get_object(Bucket=bucket, Key="completed-multipart")["Body"].read()
        assert body == expected_mp, "completed multipart body changed after kill"
        for number in (1, 2):
            part = s3.get_object(
                Bucket=bucket, Key="completed-multipart", PartNumber=number
            )["Body"].read()
            assert part == parts[number - 1], f"multipart part {number} changed after kill"
        log("  completed multipart: full body and ?partNumber=1/2 exact")

        for key, expected in acked.items():
            got = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            assert got == expected, f"acknowledged object {key} is wrong after kill"
        log(f"  acknowledged PUTs: {len(acked)} read back exactly")

        lost_response = 0
        for key, expected in attempted.items():
            if key in acked:
                continue
            try:
                got = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            except s3.exceptions.NoSuchKey:
                continue
            except Exception as error:  # noqa: BLE001
                code = getattr(error, "response", {}).get("Error", {}).get("Code")
                if code in ("404", "NoSuchKey", "NotFound"):
                    continue
                raise
            assert got == expected, f"unacknowledged object {key} is torn"
            lost_response += 1
        log(f"  in-flight PUTs: {lost_response} committed with a lost response, 0 torn")
        stop(proc)
        log(f"  round {round_no}: PASS")

    log("PASS: SIGKILL recovery preserved every acknowledged object and the multipart layout")


if __name__ == "__main__":
    main()
