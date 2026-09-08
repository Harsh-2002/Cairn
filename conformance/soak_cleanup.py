"""Bounded terminal multipart cleanup verification for the constant-load soak."""
import collections
from contextlib import closing
import os
from pathlib import Path
import sqlite3
import threading
import time


CLEANUP_TIMEOUT = 30.0  # Same bound as the focused multipart cleanup conformance gate.
PENDING_LIMIT = 1024
PROBE_PAGE = 64
PROBE_INTERVAL = 0.1


class CleanupProbe:
    def __init__(self, database, data_dir, bucket):
        self.database = Path(database).resolve().as_uri() + "?mode=ro"
        self.data_dir = Path(data_dir)
        self.bucket = bucket

    @staticmethod
    def accounting(conn, bucket, principal):
        # Both roll-ups and their independent reconstruction come from ONE WAL snapshot. Never
        # fetch whole metadata tables: only aggregate the bounded multipart working set.
        for table, column, identity, predicate in (
            ("multipart_bucket_stats", "bucket_name", bucket, "u.bucket_name = ?"),
            ("multipart_principal_stats", "principal_id", principal,
             "COALESCE(u.initiated_by, u.owner_id) = ?"),
        ):
            active = conn.execute(
                f"SELECT COUNT(*) FROM multipart_uploads u WHERE {predicate}", (identity,)
            ).fetchone()[0]
            amount = conn.execute(f"""
                SELECT COALESCE(SUM(size), 0) FROM multipart_parts p
                JOIN multipart_uploads u ON u.id=p.upload_id WHERE {predicate}
                """, (identity,)).fetchone()[0]
            amount += conn.execute(f"""
                SELECT COALESCE(SUM(reserved_bytes), 0) FROM multipart_part_reservations p
                JOIN multipart_uploads u ON u.id=p.upload_id WHERE {predicate}
                """, (identity,)).fetchone()[0]
            amount += conn.execute(f"""
                SELECT COALESCE(SUM(bytes), 0) FROM multipart_staging_cleanups WHERE {column}=?
                """, (identity,)).fetchone()[0]
            actual = conn.execute(
                f"SELECT active_uploads, staged_bytes FROM {table} WHERE {column}=?", (identity,)
            ).fetchone()
            if actual is None and (active, amount) == (0, 0):
                continue
            if actual != (active, amount):
                raise AssertionError(f"{table}: {actual} != independently charged {(active, amount)}")

    def __call__(self, upload_ids):
        # A slow/corrupt query must not pin the observer or the final drain indefinitely.
        query_deadline = time.monotonic() + 1.0
        with closing(sqlite3.connect(self.database, uri=True, timeout=0.1)) as conn:
            conn.set_progress_handler(lambda: time.monotonic() >= query_deadline, 1000)
            conn.execute("PRAGMA query_only=ON")
            conn.execute("BEGIN")
            principal = conn.execute("SELECT owner_id FROM buckets WHERE name=?",
                                     (self.bucket,)).fetchone()
            if principal is None:
                raise AssertionError("multipart soak bucket disappeared")
            self.accounting(conn, self.bucket, principal[0])
            result = {}
            for uid in upload_ids:
                pending = []
                for table, column in (
                    ("multipart_uploads", "id"), ("multipart_parts", "upload_id"),
                    ("multipart_part_reservations", "upload_id"),
                    ("multipart_staging_cleanups", "upload_id"),
                    ("storage_write_intents", "upload_id"),
                ):
                    if conn.execute(f"SELECT 1 FROM {table} WHERE {column}=? LIMIT 1",
                                    (uid,)).fetchone():
                        pending.append(table)
                # '/' and '0' are adjacent ASCII bounds, so these use the two path indexes and
                # match this exact session, including index spools attributed through owner paths.
                prefix = f".staging/multipart/{uid}/"
                upper = prefix[:-1] + "0"
                for column in ("storage_path", "quota_owner_path"):
                    if conn.execute(f"""SELECT 1 FROM storage_cleanups
                            WHERE {column}>=? AND {column}<? LIMIT 1""",
                                    (prefix, upper)).fetchone():
                        pending.append(f"storage_cleanups.{column}")
                if os.path.lexists(self.data_dir / ".staging" / "multipart" / uid):
                    if "multipart_staging_cleanups" not in pending:
                        raise AssertionError(f"{uid}: staging directory outlived its quota debt")
                    pending.append("staging directory")
                result[uid] = ", ".join(pending)
            return result


class TerminalCleanup:
    """Workers only enqueue; the observer owns I/O. Retired IDs are immediately forgotten."""
    def __init__(self, probe, clock=time.monotonic, limit=PENDING_LIMIT, timeout=CLEANUP_TIMEOUT):
        self.probe, self.clock, self.limit, self.timeout = probe, clock, limit, timeout
        self.pending = collections.OrderedDict()
        self.lock = threading.Lock()
        self.wake = threading.Event()
        self.closed = False
        self.registered = self.verified = self.failed = self.peak = 0
        self.first_failure = ""
        self.thread = None

    def fail(self, message):
        self.failed += 1
        if not self.first_failure:
            self.first_failure = message

    def register(self, uid):
        with self.lock:
            self.registered += 1
            if self.closed or self.failed or uid in self.pending or len(self.pending) >= self.limit:
                self.fail(f"{uid}: terminal cleanup queue closed, duplicate, or full ({self.limit})")
                return
            self.pending[uid] = self.clock() + self.timeout
            self.peak = max(self.peak, len(self.pending))

    def poll(self):
        with self.lock:
            page = list(self.pending.items())[:PROBE_PAGE]
        if not page:
            return
        try:
            state = self.probe([uid for uid, _ in page])
            if set(state) != {uid for uid, _ in page}:
                raise AssertionError("cleanup probe did not account for every requested session")
        except Exception as exc:  # Schema/read/accounting failures must fail the gate, never skip it.
            with self.lock:
                self.fail(f"cleanup probe: {type(exc).__name__}: {exc}")
            return
        with self.lock:
            now = self.clock()
            for uid, deadline in page:
                if now >= deadline:
                    self.fail(f"{uid}: cleanup exceeded {self.timeout:g}s ({state[uid] or 'late absence'})")
                    return
                elif not state[uid]:
                    self.pending.pop(uid)
                    self.verified += 1
                else:
                    self.pending.move_to_end(uid)  # Round-robin; no busy session hides peers.

    def start(self):
        def observe():
            while True:
                self.poll()
                with self.lock:
                    if self.failed or (self.closed and not self.pending):
                        return
                self.wake.wait(PROBE_INTERVAL)
                self.wake.clear()
        self.thread = threading.Thread(target=observe, name="terminal-cleanup", daemon=True)
        self.thread.start()

    def finish(self):
        # Original per-session deadlines survive the final drain; stopping load grants no extension.
        with self.lock:
            self.closed = True
        self.wake.set()
        self.thread.join(self.timeout + 2)
        with self.lock:
            if self.thread.is_alive():
                self.fail("terminal cleanup observer failed to finish within the bounded drain")
            return {"registered": self.registered, "verified": self.verified,
                    "failed": self.failed, "pending": len(self.pending),
                    "peak": self.peak, "first_failure": self.first_failure}
