"""Bounded stdlib SigV4 client. One persistent connection per worker; no retries."""
import concurrent.futures
import datetime
import hashlib
import hmac
import http.client
import json
import os
import threading
import time


def request(connection, method, path, body=b""):
    now = datetime.datetime.now(datetime.timezone.utc)
    stamp, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    digest = hashlib.sha256(body).hexdigest()
    headers = {"host": f"127.0.0.1:{connection.port}", "x-amz-content-sha256": digest, "x-amz-date": stamp}
    names = ";".join(sorted(headers))
    canonical = "\n".join([method, path, "", "".join(f"{key}:{headers[key]}\n" for key in sorted(headers)), names, digest])
    scope = f"{day}/us-east-1/s3/aws4_request"
    key = ("AWS4" + os.environ["LAB_SECRET"]).encode()
    for part in (day, "us-east-1", "s3", "aws4_request"):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    signed = "\n".join(["AWS4-HMAC-SHA256", stamp, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    signature = hmac.new(key, signed.encode(), hashlib.sha256).hexdigest()
    headers["Authorization"] = f"AWS4-HMAC-SHA256 Credential=lab-key/{scope}, SignedHeaders={names}, Signature={signature}"
    connection.request(method, path, body, headers)
    response = connection.getresponse()
    data = response.read(2 * 1024 * 1024)
    if response.status not in (200, 204):
        raise RuntimeError(f"unexpected {method} status {response.status}")
    return data


def payload(size, seed):
    state = seed
    data = bytearray(size)
    for i in range(size):
        state = (state * 6364136223846793005 + 1442695040888963407) & ((1 << 64) - 1)
        data[i] = (state >> 32) & 255
    return bytes(data)


def distribution(samples):
    samples.sort()
    count = len(samples)
    if not count:
        return {"count": 0}
    return {"count": count, "sum_seconds": sum(samples), "p50_seconds": samples[(count - 1) // 2],
            "p99_seconds": samples[(count * 99 + 99) // 100 - 1] if count >= 10_000 else None, "max_seconds": samples[-1]}


def emit(value):
    print(json.dumps(value), flush=True)


def run(config):
    port = int(os.environ["LAB_PORT"])
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    for bucket in range(config["buckets"]):
        request(connection, "PUT", f"/lab-{bucket:04}")
    connection.close()
    data = payload(config["size"], config["seed"])
    cap = config["max_ops"] // config["cycles"]
    for cycle in range(config["cycles"]):
        admitted = 0
        lock = threading.Lock()
        start = time.monotonic()
        deadline = start + config["seconds"]
        emit({"kind": "phase", "cycle": cycle, "phase": "load"})

        def worker(index):
            nonlocal admitted
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            samples = [[], [], []]
            try:
                while time.monotonic() < deadline:
                    with lock:
                        if admitted >= cap:
                            break
                        sequence = admitted
                        admitted += 1
                    path = f"/lab-{index % config['buckets']:04}/worker-{index:04}-{sequence % 16:02}"
                    for column, (method, body) in enumerate((("PUT", data), ("GET", b""), ("DELETE", b""))):
                        before = time.monotonic()
                        result = request(connection, method, path, body)
                        if method == "GET" and result != data:
                            raise RuntimeError("S3 checksum/length mismatch")
                        samples[column].append(time.monotonic() - before)
                return samples
            finally:
                connection.close()

        combined = [[], [], []]
        with concurrent.futures.ThreadPoolExecutor(config["concurrency"]) as pool:
            for samples in pool.map(worker, range(config["concurrency"])):
                for column, values in zip(combined, samples):
                    column.extend(values)
        elapsed = time.monotonic() - start
        emit({"kind": "cycle", "cycle": cycle, "elapsed": elapsed,
              "operation_names": ["put", "get", "delete"],
              "successful_transactions": len(combined[0]), "transactions_per_second": len(combined[0]) / elapsed,
              "operation_cap_reached": admitted >= cap, "operations": [distribution(values) for values in combined]})
        del combined, samples
        emit({"kind": "phase", "cycle": cycle, "phase": "idle"})
        time.sleep(config["idle"])
    emit({"kind": "complete", "status": "PASS"})


if __name__ == "__main__":
    import sys
    run(json.loads(open(sys.argv[1], encoding="utf-8").read()))
