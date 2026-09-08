"""Fixed live-object preparation/readback for the charged offline recovery screen."""
import concurrent.futures
import http.client
import json
import os
from pathlib import Path
import sys

from s3_driver import payload, request


def run(config):
    if (set(config) != {"objects", "size", "seed", "mode"}
            or config["objects"] not in (8, 100, 1000)
            or config["size"] != 4096 or config["mode"] not in ("prepare", "verify")
            or not isinstance(config["seed"], int) or not 0 <= config["seed"] < 2**64):
        raise ValueError("invalid bounded recovery fixture")
    data = payload(config["size"], config["seed"])
    port = int(os.environ["LAB_PORT"])
    if config["mode"] == "prepare":
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        try:
            request(connection, "PUT", "/lab-recovery")
        finally:
            connection.close()

    def worker(index):
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        count = 0
        try:
            for number in range(index, config["objects"], 4):
                path = f"/lab-recovery/key-{number:06}"
                if config["mode"] == "prepare":
                    request(connection, "PUT", path, data)
                elif request(connection, "GET", path) != data:
                    raise RuntimeError("restored payload mismatch")
                count += 1
            return count
        finally:
            connection.close()

    with concurrent.futures.ThreadPoolExecutor(4) as pool:
        count = sum(pool.map(worker, range(4)))
    if count != config["objects"]:
        raise RuntimeError("incomplete preparation/readback")
    print(json.dumps({"status": "PASS", "mode": config["mode"], "objects": count}), flush=True)


if __name__ == "__main__":
    run(json.loads(Path(sys.argv[1]).read_text()))
