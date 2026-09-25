"""Read the bounded mmap counters written by diagnostic sync_probe.c."""

import struct
from pathlib import Path


MAGIC = b"CSYNCv2\0"
FIELDS = ("fsync_root", "fsync_staging", "fsync_other_dir", "fsync_other",
          "fdatasync")
FORMAT = "<8s" + "QQQ" * len(FIELDS)
SIZE = struct.calcsize(FORMAT)


def initialize(path):
    path = Path(path)
    with path.open("xb") as output:
        output.write(struct.pack(FORMAT, MAGIC, *([0] * (3 * len(FIELDS)))))


def read(path):
    data = Path(path).read_bytes()
    if len(data) != SIZE:
        raise ValueError("sync probe capture has wrong size")
    values = struct.unpack(FORMAT, data)
    if values[0] != MAGIC:
        raise ValueError("sync probe capture has wrong magic")
    result = {}
    for index, name in enumerate(FIELDS):
        calls, total_ns, max_ns = values[1 + index * 3:1 + (index + 1) * 3]
        result[name] = {"calls": calls, "total_ms": total_ns / 1_000_000,
                        "max_ms": max_ns / 1_000_000}
    return result


def delta(before, after):
    """Subtract cumulative calls/time; max latency cannot be interval-subtracted."""
    return {name: {field: after[name][field] - before[name][field]
                   for field in ("calls", "total_ms")}
            for name in FIELDS}
