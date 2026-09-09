#!/usr/bin/env python3
"""Charged descriptive backup/baseline/full-scan/restore costs; never activates journal boot."""
import argparse
from contextlib import closing
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import secrets
import shutil
import signal
import sqlite3
import sys
import time

from budget import Campaign, SPACE_LIMIT, Unavailable, atomic_json, footprint, remove_tree
from lab import (HERE, RESULTS, available_port, clean_environment, manifest,
                 observe_storage_cleanup, tool_output)
from processes import Child, sample
from s3_driver import payload

COUNTS = (100, 1000)
PRIMARY = "offline storage-baseline wall time including its mandatory safety snapshot"
PROTECTED = "exact live payloads and metadata, backup/restore/full-scan startup wall time and process memory"


def reserve_bytes(counts):
    # Source, ordinary/safety snapshots and restored tree; DB/index/WAL amplification, staging,
    # metadata-copy temporaries, bounded command logs and cleanup headroom all count.
    return 4 * (max(counts) * (4096 + 256 * 1024) + 32 * 1024**2) + 512 * 1024**2


def read_state(database, objects, expected_hash):
    with closing(sqlite3.connect(f"file:{database}?mode=ro", uri=True)) as conn:
        columns = [row[1] for row in conn.execute("PRAGMA table_info(object_versions)")]
        records = conn.execute("SELECT * FROM object_versions ORDER BY id").fetchall()
        if len(records) != objects:
            raise RuntimeError("unexpected authoritative object count")
        for record in records:
            row = dict(zip(columns, record))
            if (row["size_logical"] != 4096 or row["size_physical"] != 4096
                    or row["sse_descriptor"] is not None or row["internal_sha256"] != expected_hash
                    or row["is_delete_marker"] or not row["storage_path"]):
                raise RuntimeError("unexpected authoritative object metadata")
        recovery = conn.execute("SELECT coverage_state,legacy_accounting_hold,legacy_release_authorized "
                                "FROM storage_recovery_state WHERE singleton=1").fetchone()
        pending = {table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                   for table in ("storage_write_intents", "storage_intent_paths", "storage_cleanups",
                                 "multipart_part_reservations", "multipart_staging_cleanups")}
    if any(pending.values()):
        raise RuntimeError(f"offline phase left unresolved journal/quota work: {pending}")
    return {"object_rows_sha256": hashlib.sha256(repr(records).encode()).hexdigest(),
            "objects": objects, "coverage": recovery, "pending": pending}


def command_resources(path):
    try:
        fields = path.read_text().strip().split("\t")
    except OSError as error:
        raise Unavailable("GNU time output missing") from error
    if len(fields) != 5:
        raise Unavailable("GNU time output missing or invalid")
    try:
        elapsed, user, system, rss, status = map(float, fields)
    except ValueError as error:
        raise Unavailable("GNU time output is not numeric") from error
    if (not all(math.isfinite(value) and value >= 0 for value in (elapsed, user, system, rss))
            or rss == 0 or status != 0):
        raise Unavailable("command resource measurement incomplete")
    return {"gnu_elapsed_seconds_0_01_resolution": elapsed, "user_cpu_seconds": user,
            "system_cpu_seconds": system, "peak_rss_kib": rss}


def finish_server(child, binary, work_deadline, deadline):
    """A failed durability shutdown is an operation failure, not missing profiler evidence."""
    if child.exited() is not None:
        raise RuntimeError(f"{child.label}: server exited before requested shutdown")
    try:
        child.finish_profiled_target(binary, min(work_deadline, time.monotonic() + 10))
    except Unavailable:
        outcome = child.exited()
        if outcome is not None and outcome != 0:
            raise RuntimeError(f"{child.label}: graceful durability shutdown failed (exit {outcome})") from None
        raise
    child.stop(min(deadline - 2, time.monotonic() + 4))


def run_screen(args, campaign, counts=COUNTS):
    if not counts or any(count not in (8, *COUNTS) for count in counts):
        raise ValueError("bounded object counts required")
    reserved = reserve_bytes(counts)
    token = campaign.admit("recovery", args.allow_seconds, reserved)
    start = time.monotonic()
    deadline, work_deadline = start + args.allow_seconds, start + args.allow_seconds - 15
    directory = campaign.root / token
    children, cases, reasons = [], [], []
    clean, status = False, "INCONCLUSIVE"
    report = {"id": token, "kind": "recovery-cost", "primary_metric": PRIMARY,
              "protected_workloads": PROTECTED, "seed": args.seed, "counts": counts,
              "size": 4096, "concurrency": 4, "orphan_aliases": 64, "cases": cases,
              "decision": "KEEP mandatory full startup scans",
              "measurement_scope": "descriptive end-to-end command costs; no throughput or p99 claim",
              "cache_state": "uncontrolled; repeated processes share filesystem cache",
              "remaining_defaults": f"unset CAIRN_* defaults from config.rs at declared {args.commit}; no inherited CAIRN_* values",
              "activation": "INCONCLUSIVE: no journal-boot candidate; Phase 3C gate did not qualify"}
    try:
        directory.mkdir(mode=0o700)
        artifacts, data = directory / "artifacts", directory / "data"
        artifacts.mkdir(mode=0o700)
        data.mkdir(mode=0o700)
        (artifacts / "scratch").mkdir(mode=0o700)
        binary = Path(args.binary).resolve(strict=True)
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise ValueError("explicit executable binary required")
        time_binary = shutil.which("time")
        if not time_binary:
            raise Unavailable("GNU time unavailable")
        base_env = {**clean_environment(), "TMPDIR": str(artifacts / "scratch")}

        def check_budget():
            if time.monotonic() >= work_deadline:
                raise Unavailable("runtime allowance reached; preserving cleanup time")
            used = footprint(campaign.root)
            campaign.ledger["peak_bytes"] = max(campaign.ledger["peak_bytes"], used)
            if used > campaign.ledger["active"]["initial_bytes"] + reserved - 256 * 1024**2 or used >= SPACE_LIMIT - 1_000_000_000:
                raise Unavailable("reserved disk headroom reached")
            if any(child.output_error or child.output_overflow for child in children):
                raise Unavailable("bounded child output was incomplete")

        def check_output():
            if any(child.output_error or child.output_overflow for child in children):
                raise Unavailable("bounded child output was incomplete after drain")

        def launch(command, label, environment=base_env):
            check_budget()
            child = Child(command, environment, artifacts, label, campaign)
            children.append(child)
            return child

        report["manifest"] = manifest(args, binary, launch)
        report["time_tool"] = tool_output(["time", "--version"], launch)
        report["time_tool"]["executable"] = time_binary
        if "GNU" not in report["time_tool"].get("output", ""):
            raise Unavailable("GNU time is required for command resource measurements")

        def wait(child, label):
            with (artifacts / f"{label}.samples.jsonl").open("w") as stream:
                while child.exited() is None:
                    check_budget()
                    stream.write(json.dumps(sample(child.process.pid)) + "\n")
                    stream.flush()
                    if stream.tell() > 16 * 1024**2:
                        raise Unavailable("bounded process observations exceeded")
                    time.sleep(0.02)
            outcome = child.exited()
            child.stop(min(deadline - 2, time.monotonic() + 4))
            if outcome != 0:
                raise RuntimeError(f"{label}: command failed (exit {outcome}); bounded logs retained")
            check_output()

        def command(environment, arguments, label):
            resources = artifacts / f"{label}.time.tsv"
            before = time.monotonic()
            child = launch([time_binary, "-f", "%e\t%U\t%S\t%M\t%x", "-o", str(resources),
                            str(binary), *arguments], label, environment)
            wait(child, label)
            return {"coordinator_wall_seconds": time.monotonic() - before,
                    **command_resources(resources)}

        def server(environment, label):
            before = time.monotonic()
            child = launch([str(binary), "serve"], label, environment)
            while True:
                check_budget()
                if child.exited() is not None:
                    raise RuntimeError(f"{label}: startup failed before readiness")
                connection = http.client.HTTPConnection("127.0.0.1", int(environment["LAB_PORT"]), timeout=0.5)
                try:
                    connection.request("GET", "/readyz")
                    response = connection.getresponse()
                    response.read(4096)
                    if response.status == 200:
                        break
                except (OSError, http.client.HTTPException):
                    pass
                finally:
                    connection.close()
                time.sleep(0.02)
            ready_seconds = time.monotonic() - before
            observation = sample(child.process.pid)
            targets = [process for process in observation["processes"] if process.get("executable") == str(binary)]
            if (len(targets) != 1 or not targets[0].get("status", {}).get("RssAnon")
                    or not targets[0].get("smaps_rollup", {}).get("Pss")):
                raise Unavailable("ready process anonymous/PSS observation unavailable")
            return child, {"start_to_ready_seconds": ready_seconds, "ready_observation": observation}

        def stop_server(child):
            finish_server(child, binary, work_deadline, deadline)
            check_output()

        expected_hash = hashlib.sha256(payload(4096, args.seed)).hexdigest()
        for objects in counts:
            check_budget()
            case_root = data / str(objects)
            case_root.mkdir(mode=0o700)
            source, target = case_root / "source", case_root / "restored"
            secret, master, port = secrets.token_hex(32), secrets.token_hex(32), available_port()
            environment = {**base_env, "CAIRN_DATA_DIR": str(source), "CAIRN_DB_PATH": str(source / "cairn.db"),
                           "CAIRN_MASTER_KEY": master, "CAIRN_ROOT_ACCESS_KEY": "lab-key", "CAIRN_ROOT_SECRET_KEY": secret,
                           "CAIRN_API_ADDR": f"127.0.0.1:{port}", "CAIRN_CONSOLE_ADDR": "off",
                           "CAIRN_META_BACKEND": "sqlite", "CAIRN_META_SHARDS": "1", "CAIRN_META_SYNCHRONOUS": "full",
                           "CAIRN_META_READ_POOL_SIZE": "8", "CAIRN_META_CACHE_BYTES_PER_CONN": "8388608",
                           "CAIRN_META_MMAP_BYTES": "0", "CAIRN_META_GROUP_COMMIT_LINGER_MICROS": "0",
                           "CAIRN_RUNTIME_WORKER_THREADS": "8", "CAIRN_ENCRYPT_AT_REST": "false",
                           "CAIRN_UPDATE_CHECK_ENABLED": "false",
                           "CAIRN_LOG_LEVEL": "error", "LAB_SECRET": secret, "LAB_PORT": str(port)}
            case = {"objects": objects, "phases": {}, "status": "INCONCLUSIVE"}
            cases.append(case)

            def drive(mode, environment, phase=None):
                label = f"{objects}-{phase or mode}"
                config = {"objects": objects, "size": 4096, "seed": args.seed, "mode": mode}
                path = artifacts / f"{label}.config.json"
                atomic_json(path, config)
                child = launch([sys.executable, str(HERE / "recovery_driver.py"), str(path)], label, environment)
                wait(child, label)
                records = [json.loads(line) for line in child.paths[0].read_text().splitlines()]
                if records != [{"status": "PASS", "mode": mode, "objects": objects}]:
                    raise RuntimeError("incomplete live preparation/readback result")

            running, _ = server(environment, f"{objects}-prepare-server")
            prepare_start = time.monotonic()
            drive("prepare", environment)
            case["preparation_cleanup"] = observe_storage_cleanup(
                source / "cairn.db", work_deadline, lambda: running.exited() is None)
            if case["preparation_cleanup"]["status"] != "drained":
                raise Unavailable("preparation cleanup was not observed complete before shutdown")
            case["preparation_seconds"] = time.monotonic() - prepare_start
            stop_server(running)
            original = read_state(source / "cairn.db", objects, expected_hash)
            case["effective_config"] = {key: value for key, value in environment.items()
                                        if key.startswith("CAIRN_") and key not in
                                        {"CAIRN_MASTER_KEY", "CAIRN_ROOT_ACCESS_KEY", "CAIRN_ROOT_SECRET_KEY"}}
            case["phases"]["backup"] = command(environment, ["backup", str(case_root / "backup")], f"{objects}-backup")
            orphan_paths = [source / ".staging" / (hashlib.sha256(f"{args.seed}:{number}".encode()).hexdigest()[:32] + ".index.tmp")
                            for number in range(64)]
            for path in orphan_paths:
                check_budget()
                with path.open("xb") as stream:
                    stream.write(b"legacy namespace cost fixture")
                    stream.flush()
                    os.fsync(stream.fileno())
            stage_fd = os.open(source / ".staging", os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(stage_fd)
            finally:
                os.close(stage_fd)
            case["phases"]["baseline"] = command(environment, ["storage-baseline", str(case_root / "safety")], f"{objects}-baseline")
            completed = read_state(source / "cairn.db", objects, expected_hash)
            if completed["coverage"] != ("complete", 0, 0) or any(path.exists() for path in orphan_paths):
                raise RuntimeError("baseline did not complete exact orphan cleanup")
            if completed["object_rows_sha256"] != original["object_rows_sha256"]:
                raise RuntimeError("baseline changed authoritative object rows")
            running, startup = server(environment, f"{objects}-full-scan-server")
            case["phases"]["full_scan_startup"] = startup
            drive("verify", environment, "baseline-verify")
            stop_server(running)
            restored_env = {**environment, "CAIRN_DATA_DIR": str(target), "CAIRN_DB_PATH": str(target / "cairn.db")}
            case["phases"]["restore"] = command(restored_env, ["restore", str(case_root / "backup")], f"{objects}-restore")
            restored = read_state(target / "cairn.db", objects, expected_hash)
            if restored["object_rows_sha256"] != original["object_rows_sha256"] or restored["coverage"] != ("incomplete", 0, 0):
                raise RuntimeError("restore changed authoritative rows or reused coverage")
            running, _ = server(restored_env, f"{objects}-verify-server")
            drive("verify", restored_env, "restore-verify")
            stop_server(running)
            case.update(status="PASS", verified_objects=objects, baseline_verified_objects=objects,
                        original=original, completed=completed, restored=restored)
            remove_tree(case_root, deadline - 2)
            print(json.dumps({"objects": objects, "diagnostic_status": "PASS", "activation": "INCONCLUSIVE"}), flush=True)
        check_output()
        status = "PASS"
    except KeyboardInterrupt:
        status, reasons = "CANCELLED", ["operator interruption"]
    except Unavailable as error:
        status, reasons = "INCONCLUSIVE", [str(error)]
    except Exception as error:
        status, reasons = "FAIL", [f"{type(error).__name__}: {error}"]
    finally:
        try:
            for child in reversed(children):
                child.stop(deadline - 1)
            if (directory / "data").exists():
                remove_tree(directory / "data", deadline - 1)
            clean = True
        except Exception as error:
            if status not in ("FAIL", "CANCELLED"):
                status = "INCONCLUSIVE"
            reasons.append(f"cleanup incomplete: {error}")
        elapsed = time.monotonic() - start
        charged = elapsed + 1
        if charged > args.allow_seconds:
            if status not in ("FAIL", "CANCELLED"):
                status = "INCONCLUSIVE"
            reasons.append("wall time exceeded reservation; actual time plus finalization charged")
        report.update(status=status, reasons=reasons, elapsed_seconds=elapsed, charged_seconds=charged,
                      cleaned_data_and_processes=clean, finalization_allowance_seconds=1)
        atomic_json(campaign.root / f"{token}.result.json", report)
        campaign.finish(max(charged, time.monotonic() - start), status, clean=clean)
    print(json.dumps({"status": status, "decision": report["decision"], "result": str(campaign.root / f"{token}.result.json"),
                      "reasons": reasons, "spent_seconds": campaign.ledger["spent_seconds"],
                      "peak_bytes": campaign.ledger["peak_bytes"]}), flush=True)
    return RESULTS[status]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("root", "binary", "commit", "build-settings"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--seed", type=int, default=0x5eed)
    parser.add_argument("--allow-seconds", type=int, default=180)
    args = parser.parse_args()
    if not 30 <= args.allow_seconds <= 300 or not 0 <= args.seed < 2**64:
        parser.error("bounded runtime and u64 seed required")
    campaign = None
    try:
        campaign = Campaign(args.root)
        return run_screen(args, campaign)
    except Unavailable as error:
        print(json.dumps({"status": "INCONCLUSIVE", "decision": "KEEP mandatory full startup scans", "reason": str(error)}))
        return 2
    finally:
        if campaign:
            campaign.close()


if __name__ == "__main__":
    def interrupted(*_):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
