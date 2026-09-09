"""Owned process groups, bounded output and Linux attribution samples."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

from budget import Unavailable

OUTPUT_LIMIT = 16 * 1024 * 1024


def identity(pid):
    text = Path(f"/proc/{pid}/stat").read_text()
    fields = text[text.rindex(")") + 2:].split()
    return {"pid": pid, "pgrp": int(fields[2]), "start": fields[19],
            "boot": Path("/proc/sys/kernel/random/boot_id").read_text().strip()}


def still_same(record):
    try:
        return identity(record["pid"]) == {key: record[key] for key in ("pid", "pgrp", "start", "boot")}
    except (FileNotFoundError, ProcessLookupError):
        return False


def group_members(group):
    result = []
    for path in Path("/proc").iterdir():
        if not path.name.isdecimal():
            continue
        try:
            record = identity(int(path.name))
            if record["pgrp"] == group:
                result.append(record)
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            pass
    return result


class Child:
    def __init__(self, command, env, directory, label, campaign, *, discard=False):
        self.label = label
        self.reaped = False
        self.output_overflow = False
        self.output_error = False
        self.paths = [directory / f"{label}.jsonl", directory / f"{label}.stderr"]
        self.process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "--gate", *command],
            env=env, cwd=directory, stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL if discard else subprocess.PIPE,
            stderr=subprocess.PIPE, start_new_session=True,
        )
        self.threads = []
        try:
            self.record = {**identity(self.process.pid), "label": label}
            campaign.ledger["active"]["children"].append(self.record)
            campaign.save()
            for source, path in zip((self.process.stdout, self.process.stderr), self.paths):
                if source is None:
                    continue
                thread = threading.Thread(target=self._drain, args=(source, path), daemon=True)
                thread.start()
                self.threads.append(thread)
            # The gate exits on EOF if the coordinator dies before recording ownership.
            self.process.stdin.write(b"G")
            self.process.stdin.close()
        except BaseException:
            # In particular, failed ledger fsync must not leave an unrecorded gate alive.
            self.process.stdin.close()
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.process.wait(timeout=3)
            self.reaped = True
            for thread in self.threads:
                thread.join(timeout=1)
            for source in (self.process.stdout, self.process.stderr):
                if source is not None and not source.closed:
                    source.close()
            raise

    def exited(self):
        if self.reaped:
            return self.process.returncode
        # poll()/wait() would reap the leader and permit PID reuse while we still own
        # descendants. WNOWAIT keeps the leader identity pinned until group teardown.
        result = os.waitid(os.P_PID, self.process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
        if result is None:
            return None
        return result.si_status if result.si_code == os.CLD_EXITED else -result.si_status

    def _drain(self, source, path):
        written = 0
        try:
            with source, path.open("wb") as target:
                while chunk := source.read(64 * 1024):
                    remaining = max(0, OUTPUT_LIMIT - written)
                    target.write(chunk[:remaining])
                    written += len(chunk)
                    if written > OUTPUT_LIMIT:
                        self.output_overflow = True
        except OSError:
            self.output_error = True

    def stop(self, deadline):
        # This object owns the unreaped child; its PID cannot have been recycled. Kill
        # the whole group even when the wrapper exited before one of its descendants.
        if self.reaped:
            return
        if not still_same(self.record):
            raise Unavailable("process identity changed before teardown")
        for sig, grace in ((signal.SIGTERM, 2), (signal.SIGKILL, 2)):
            try:
                os.killpg(self.process.pid, sig)
            except ProcessLookupError:
                pass
            until = min(deadline, time.monotonic() + grace)
            while time.monotonic() < until and live_group_members(self.process.pid):
                time.sleep(0.02)
            if not live_group_members(self.process.pid):
                break
        if live_group_members(self.process.pid):
            raise Unavailable("owned process group did not quiesce; cleanup remains blocked")
        self.process.wait(timeout=max(0.01, deadline - time.monotonic()))
        self.reaped = True
        for thread in self.threads:
            thread.join(timeout=max(0, deadline - time.monotonic()))
        if any(thread.is_alive() for thread in self.threads):
            raise Unavailable("owned process group did not quiesce; cleanup remains blocked")

    def finish_profiled_target(self, executable, deadline):
        """Let the target exit and the profiler flush before bounded group teardown."""
        matches = []
        for member in live_group_members(self.process.pid):
            try:
                if Path(f"/proc/{member['pid']}/exe").resolve(strict=True) == executable:
                    matches.append(member)
            except (FileNotFoundError, ProcessLookupError, PermissionError):
                continue
        if len(matches) != 1:
            raise Unavailable("cannot identify one owned profiling target for graceful shutdown")
        member = matches[0]
        descriptor = os.pidfd_open(member["pid"])
        try:
            if identity(member["pid"]) != member:
                raise Unavailable("profiling target changed before graceful shutdown")
            signal.pidfd_send_signal(descriptor, signal.SIGTERM)
        finally:
            os.close(descriptor)
        while self.exited() is None:
            if time.monotonic() >= deadline:
                raise Unavailable("profiler did not finish after target shutdown; trace may be incomplete")
            time.sleep(0.02)
        if self.exited() != 0:
            raise Unavailable("profiling wrapper failed after target shutdown")


def live_group_members(group):
    members = []
    for record in group_members(group):
        try:
            text = Path(f"/proc/{record['pid']}/stat").read_text()
            if text[text.rindex(")") + 2] not in ("Z", "X"):
                members.append(record)
        except (FileNotFoundError, ProcessLookupError):
            pass
    return members


def sample(group):
    records = []
    for member in group_members(group):
        root = Path(f"/proc/{member['pid']}")
        try:
            fields = {}
            try:
                fields["executable"] = str((root / "exe").resolve(strict=True))
            except (FileNotFoundError, PermissionError):
                fields["executable"] = None
            for name in ("status", "smaps_rollup", "io"):
                try:
                    lines = (root / name).read_text().splitlines()
                except PermissionError:
                    fields[name] = {"unavailable": "permission"}
                    continue
                fields[name] = {key: value.strip() for key, _, value in (line.partition(":") for line in lines)
                                if key in {"VmRSS", "RssAnon", "RssFile", "RssShmem", "VmSwap", "Threads", "Pss", "Pss_Anon", "Pss_File", "Private_Dirty", "Shared_Clean", "Anonymous", "read_bytes", "write_bytes", "syscr", "syscw"}}
            try:
                fields["fds"] = len(list((root / "fd").iterdir()))
            except PermissionError:
                # Procfs access can disappear during child exit. Preserve the
                # remaining sample without inventing a zero descriptor count.
                fields["fds"] = None
                fields["fds_unavailable"] = "permission"
            fields["stat"] = (root / "stat").read_text().split(")", 1)[1].strip()
            records.append({"identity": member, **fields})
        except (FileNotFoundError, ProcessLookupError):
            continue
    host = {}
    for name in ("pressure/cpu", "pressure/io", "pressure/memory", "diskstats", "loadavg", "meminfo"):
        try:
            host[name] = Path("/proc", name).read_text()
        except OSError:
            host[name] = None
    return {"monotonic": time.monotonic(), "unix_seconds": time.time(), "processes": records, "host": host}


if __name__ == "__main__":
    import resource
    if sys.argv[1] != "--gate" or sys.stdin.buffer.read(1) != b"G":
        sys.exit(2)
    # Bound each output/database/profile file independently in addition to whole-case
    # admission. A SIGXFSZ or profiler failure is never a successful measurement.
    resource.setrlimit(resource.RLIMIT_FSIZE, (2 * 1024**3, 2 * 1024**3))
    os.execvpe(sys.argv[2], sys.argv[2:], os.environ)
