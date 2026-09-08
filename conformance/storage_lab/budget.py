"""Persistent, exclusive campaign accounting. No credentials belong in this file."""

import fcntl
import json
import os
from pathlib import Path
import stat
import math
import time
import uuid

PHASES = {"baseline": 600, "fanout": 480, "recovery": 840, "packing": 1080, "metadata": 600}
SPACE_LIMIT = 100_000_000_000
CLEANUP_HEADROOM = 1_000_000_000
RECOVERY_SECONDS = 30


class Unavailable(RuntimeError):
    pass


def atomic_json(path, value):
    temporary = path.with_suffix(".new")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def read_json(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor) as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise ValueError("campaign state must be a regular file")
        content = source.read(4 * 1024 * 1024 + 1)
        if len(content) > 4 * 1024 * 1024:
            raise ValueError("campaign state exceeds its bounded size")
        return json.loads(content)


def valid_token(token):
    return isinstance(token, str) and len(token) == 32 and all(character in "0123456789abcdef" for character in token)


def validate_ledger(ledger):
    if not isinstance(ledger, dict) or ledger.get("protocol") != 1:
        raise ValueError("unsupported ledger protocol")
    spent = ledger.get("spent_seconds")
    if not isinstance(spent, (int, float)) or not math.isfinite(spent) or spent < 0:
        raise ValueError("invalid recorded runtime")
    if ledger.get("last_phase") not in range(len(PHASES)) or not isinstance(ledger.get("runs"), list):
        raise ValueError("invalid campaign phase/history")
    for run in [*ledger["runs"], *([ledger["active"]] if ledger.get("active") is not None else [])]:
        if not isinstance(run, dict) or not valid_token(run.get("id")):
            raise ValueError("invalid owned run identity")
        if run.get("phase") not in PHASES or not isinstance(run.get("children"), list):
            raise ValueError("invalid run phase/process ownership")
        for child in run["children"]:
            if not isinstance(child, dict) or not isinstance(child.get("pid"), int) or child["pid"] <= 1 or child.get("pgrp") != child["pid"]:
                raise ValueError("invalid owned group leader")


def owned_path(raw, *, require_ssd=True):
    path = Path(raw).absolute()
    if path != path.resolve() or (require_ssd and (not path.is_relative_to("/SSD") or path == Path("/SSD"))):
        raise ValueError("campaign path must be a canonical, nonsymlink directory below /SSD")
    return path


def footprint(root):
    """Count allocated and apparent bytes conservatively; never follow links or mounts."""
    device = root.stat().st_dev
    total = 0
    stack = [root]
    while stack:
        directory = stack.pop()
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    try:
                        info = entry.stat(follow_symlinks=False)
                    except FileNotFoundError:
                        # A live workload may finish/unlink a staging file after readdir.
                        # Admission reserves outstanding work; removed files consume no space.
                        continue
                    if info.st_dev != device or stat.S_ISLNK(info.st_mode):
                        raise Unavailable("unexpected mount or symbolic link in owned campaign")
                    total += max(info.st_size, info.st_blocks * 512)
                    if stat.S_ISDIR(info.st_mode):
                        stack.append(Path(entry.path))
        except FileNotFoundError:
            if directory == root:
                raise
    return total


def remove_tree(root, deadline):
    """Bounded, depth-first deletion, without following a symlink or crossing a device."""
    device = root.lstat().st_dev

    def visit(path):
        if time.monotonic() >= deadline:
            raise Unavailable("cleanup deadline reached; retained directory blocks admissions")
        info = path.lstat()
        if info.st_dev != device:
            raise Unavailable("refusing cleanup across a mount")
        if stat.S_ISDIR(info.st_mode):
            with os.scandir(path) as entries:
                for entry in entries:
                    visit(Path(entry.path))
            path.rmdir()
        else:
            path.unlink()

    visit(root)


class Campaign:
    def __init__(self, root, *, create=False, require_ssd=True):
        self.root = owned_path(root, require_ssd=require_ssd)
        if create:
            self.root.mkdir(mode=0o700)
            atomic_json(self.root / "owner.json", {"protocol": 1, "id": uuid.uuid4().hex})
            atomic_json(self.root / "ledger.json", {
                "protocol": 1, "spent_seconds": 0, "last_phase": 0,
                "peak_bytes": 0, "active": None, "runs": [],
            })
        if self.root.stat().st_uid != os.getuid() or self.root.stat().st_mode & 0o077:
            raise ValueError("campaign must be private and owned by the current user")
        self.owner = read_json(self.root / "owner.json")
        if self.owner.get("protocol") != 1 or not valid_token(self.owner.get("id")):
            raise ValueError("unsupported campaign ownership protocol")
        self.lock = os.fdopen(os.open(self.root / "campaign.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600), "w")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            self.lock.close()
            raise Unavailable("another coordinator owns this campaign") from error
        try:
            self.ledger = read_json(self.root / "ledger.json")
            validate_ledger(self.ledger)
        except BaseException:
            self.close()
            raise

    def close(self):
        self.lock.close()

    def save(self):
        atomic_json(self.root / "ledger.json", self.ledger)

    def admit(self, phase, seconds, reserve_bytes):
        if self.ledger["active"] is not None:
            raise Unavailable("unfinished run requires explicit recovery; no budget reset")
        position = list(PHASES).index(phase)
        available = sum(list(PHASES.values())[:position + 1]) - self.ledger["spent_seconds"]
        if position < self.ledger["last_phase"] or seconds <= 0 or seconds > available - RECOVERY_SECONDS:
            raise Unavailable("phase/campaign runtime allowance exhausted or phase order reversed")
        used = footprint(self.root)
        free = os.statvfs(self.root)
        if reserve_bytes <= 0 or used + reserve_bytes + CLEANUP_HEADROOM > SPACE_LIMIT or reserve_bytes + CLEANUP_HEADROOM > free.f_bavail * free.f_frsize:
            raise Unavailable("insufficient reserved space for data, WAL, profiles, logs and cleanup")
        token = uuid.uuid4().hex
        self.ledger["spent_seconds"] += seconds
        self.ledger["last_phase"] = position
        # Charge the complete reservation before preparation. A killed coordinator cannot
        # refund time or accidentally declare its still-running filesystem work complete.
        self.ledger["active"] = {
            "id": token, "phase": phase, "reserved_seconds": seconds,
            "reserve_bytes": reserve_bytes, "initial_bytes": used, "children": [],
        }
        self.save()
        return token

    def finish(self, elapsed, status, *, clean):
        active = self.ledger["active"]
        if not clean:
            self.save()
            return
        charged = max(0, elapsed)
        self.ledger["spent_seconds"] -= active["reserved_seconds"] - charged
        self.ledger["runs"].append({**active, "elapsed_seconds": elapsed, "status": status})
        self.ledger["peak_bytes"] = max(self.ledger["peak_bytes"], footprint(self.root))
        self.ledger["active"] = None
        self.save()
