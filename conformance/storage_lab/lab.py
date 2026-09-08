#!/usr/bin/env python3
"""Bounded storage experiments. All case preparation, collection and cleanup is charged."""
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import platform
import secrets
import shutil
import signal
import socket
import sys
import time

from budget import Campaign, PHASES, RECOVERY_SECONDS, SPACE_LIMIT, Unavailable, atomic_json, footprint, remove_tree
from processes import Child, identity, live_group_members, sample, still_same

HERE = Path(__file__).resolve().parent
RESULTS = {"PASS": 0, "FAIL": 1, "INCONCLUSIVE": 2, "CANCELLED": 130}


def tool_output(command, launch):
    try:
        if shutil.which(command[0]) is None:
            return {"unavailable": True}
        child = launch(command, f"tool-{command[0]}")
        deadline = time.monotonic() + 3
        while child.exited() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        result = child.exited()
        child.stop(time.monotonic() + 3)
        if result is None:
            return {"unavailable": "timeout"}
        return {"exit": result, "output": "".join(path.read_text(errors="replace") for path in child.paths)[:16_384]}
    except OSError:
        return {"unavailable": True}


def manifest(args, binary, launch):
    with binary.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    return {"declared_commit": args.commit, "binary": str(binary), "sha256": digest,
            "build_settings": args.build_settings, "coordinator_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "harness_sources_sha256": {str(path.relative_to(HERE)): hashlib.sha256(path.read_bytes()).hexdigest()
                                       for path in sorted([*HERE.glob("*.py"), HERE / "src/main.rs", HERE / "Cargo.lock"])},
            "platform": platform.platform(), "python": sys.version, "cpu": tool_output(["lscpu", "--json"], launch),
            "filesystem": tool_output(["findmnt", "-J", "-T", args.root], launch),
            "devices": tool_output(["lsblk", "-J", "-o", "NAME,TYPE,SIZE,ROTA,MOUNTPOINTS"], launch),
            "tools": {name: tool_output(command, launch) for name, command in {
                "perf": ["perf", "--version"], "heaptrack": ["heaptrack", "--version"],
                "rustc": ["rustc", "--version"], "readelf": ["readelf", "--version"]}.items()}}


def reservation(config, profile):
    # Fixed workload: <= max_ops publications, bounded keys/rows, three operations per
    # transaction, <= 1-MiB payloads. Include both a replacement and the outstanding old
    # file, conservative SQLite page/index/WAL amplification, artifacts and file caps.
    # Comparison arms run sequentially and retained artifacts remain in footprint().
    return (config["max_ops"] * (256 * 1024 + 2 * config["size"])
            + config["concurrency"] * (4 * config["size"] + 512 * 1024)
            + (8 if profile != "none" else 1) * 1024**3)


def clean_environment():
    # Never inherit a production CAIRN_* variable or a profiler injection setting.
    return {**{key: os.environ[key] for key in ("PATH", "LANG", "LC_ALL", "TZ") if key in os.environ}, "RUSTUP_TOOLCHAIN": "stable"}


def available_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def metrics(port):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=0.5)
    try:
        connection.request("GET", "/metrics")
        response = connection.getresponse()
        content = response.read(2 * 1024 * 1024)
        if response.status != 200 or len(content) == 2 * 1024 * 1024:
            raise Unavailable("metrics missing or exceeded bounded response")
        return content.decode()
    finally:
        connection.close()


def profile_command(profile, directory, command):
    if profile == "none":
        return command
    if profile == "cpu":
        if shutil.which("perf") is None:
            raise Unavailable("perf is unavailable")
        return ["perf", "record", "-F", "99", "--call-graph", "dwarf", "-o", str(directory / "cpu.perf"), "--", *command]
    if shutil.which("heaptrack") is None:
        raise Unavailable("heaptrack is unavailable; live allocation owners unresolved")
    return ["heaptrack", "--record-only", "-o", str(directory / "heap"), *command]


def metrics_sample(port):
    try:
        return {"metrics": metrics(port)}
    except (OSError, http.client.HTTPException, Unavailable) as error:
        # Profiling can delay this auxiliary endpoint. Preserve the telemetry gap,
        # but allow the workload and profiler to finish so their evidence survives.
        return {"metrics_unavailable": f"{type(error).__name__}: {error}"}


def recover(campaign):
    active = campaign.ledger["active"]
    if active is None:
        return {"status": "PASS", "recovered": False}
    available = sum(list(PHASES.values())[:campaign.ledger["last_phase"] + 1]) - campaign.ledger["spent_seconds"]
    allowance = min(30, available)
    if allowance < 5:
        raise Unavailable("no phase time remains for bounded recovery; ownership remains fenced")
    # Previous interrupted work keeps its full charge. Recovery is additional work.
    campaign.ledger["spent_seconds"] += allowance
    campaign.save()
    start = time.monotonic()
    deadline = start + allowance
    for record in active["children"]:
        members = live_group_members(record["pgrp"])
        if not members:
            continue
        if not still_same(record):
            raise Unavailable("leader identity is gone but its group is occupied; refusing to signal unknown processes")
        pinned = []
        try:
            # pidfds prevent signal-to-recycled-PID races during interrupted-run recovery.
            for member in members:
                descriptor = os.pidfd_open(member["pid"])
                if identity(member["pid"]) != member:
                    os.close(descriptor)
                    raise Unavailable("process changed during recovery")
                pinned.append(descriptor)
            for descriptor in pinned:
                try:
                    signal.pidfd_send_signal(descriptor, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        finally:
            for descriptor in pinned:
                os.close(descriptor)
        while live_group_members(record["pgrp"]):
            if time.monotonic() >= deadline - 1:
                raise Unavailable("recovery group did not quiesce")
            time.sleep(0.02)
    data = campaign.root / active["id"] / "data"
    if data.exists():
        remove_tree(data, deadline - 1)
    elapsed = time.monotonic() - start
    campaign.ledger["spent_seconds"] -= allowance - elapsed
    campaign.ledger["runs"].append({**active, "status": "CANCELLED", "elapsed_seconds": active["reserved_seconds"], "recovery_seconds": elapsed})
    campaign.ledger["active"] = None
    campaign.save()
    return {"status": "CANCELLED", "recovered": True, "spent_seconds": campaign.ledger["spent_seconds"]}


def purge_artifacts(campaign):
    if campaign.ledger["active"] is not None:
        raise Unavailable("recover unfinished work before purging artifacts")
    available = sum(list(PHASES.values())[:campaign.ledger["last_phase"] + 1]) - campaign.ledger["spent_seconds"]
    allowance = min(RECOVERY_SECONDS, available)
    if allowance < 1:
        raise Unavailable("no runtime remains for artifact cleanup")
    campaign.ledger["spent_seconds"] += allowance
    campaign.save()
    start = time.monotonic()
    for run in campaign.ledger["runs"]:
        directory = campaign.root / run["id"]
        if directory.exists():
            remove_tree(directory, start + allowance - 0.1)
    elapsed = time.monotonic() - start
    campaign.ledger["spent_seconds"] -= allowance - elapsed
    campaign.ledger.setdefault("maintenance", []).append({"action": "purge", "seconds": elapsed})
    campaign.save()
    return {"status": "PASS", "purged_artifacts": True, "preserved": "ledger and compact result JSON"}


def run_case(args, campaign):
    config = {key: getattr(args, key) for key in ("layer", "concurrency", "buckets", "size", "seed", "seconds", "idle", "cycles", "max_ops")}
    if args.allow_seconds < args.cycles * (args.seconds + args.idle) + 30:
        raise ValueError("allow-seconds must include all cycles plus 30 seconds for preparation/cleanup")
    reserved = reservation(config, args.profile)
    token = campaign.admit(args.phase, args.allow_seconds, reserved)
    start = time.monotonic()
    deadline = start + args.allow_seconds
    work_deadline = deadline - 15
    directory = campaign.root / token
    children = []
    status, reasons, clean = "INCONCLUSIVE", [], False
    metrics_gaps = 0
    report = {"id": token, "phase": args.phase, "profile": args.profile, "workload": config.copy(),
              "primary_metric": args.primary_metric, "protected_workloads": args.protected,
              "adoption": "not evaluated by a single diagnostic run", "cache_state": "uncontrolled; restart does not imply cold cache"}
    try:
        directory.mkdir(mode=0o700)
        artifacts = directory / "artifacts"
        artifacts.mkdir(mode=0o700)
        scratch = artifacts / "scratch"
        scratch.mkdir(mode=0o700)
        config["root"] = str(directory / "data")
        atomic_json(artifacts / "config.json", config)
        binary = Path(args.binary).resolve(strict=True)
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise ValueError("explicit binary must be an executable regular file")
        command = [str(binary), str(artifacts / "config.json")]
        env = {**clean_environment(), "TMPDIR": str(scratch)}
        port = None
        server = None

        def launch(command, label, environment=env, discard=False):
            if time.monotonic() >= work_deadline:
                raise Unavailable("runtime budget exhausted before child admission")
            child = Child(command, environment, artifacts, label, campaign, discard=discard)
            children.append(child)
            return child

        report["manifest"] = manifest(args, binary, launch)
        if args.profile != "none":
            symbols = tool_output(["readelf", "--sections", "--wide", str(binary)], launch)
            report["manifest"]["debug_sections"] = symbols
            if symbols.get("exit") != 0 or ".debug_info" not in symbols.get("output", ""):
                raise Unavailable("profiling requires a verified debug-info section in the explicit binary")

        if args.layer == "s3":
            port = available_port()
            secret = secrets.token_hex(24)
            server_env = {**env, "CAIRN_DATA_DIR": config["root"], "CAIRN_DB_PATH": str(directory / "data" / "metadata.db"),
                          "CAIRN_MASTER_KEY": secrets.token_hex(32), "CAIRN_ROOT_ACCESS_KEY": "lab-key", "CAIRN_ROOT_SECRET_KEY": secret,
                          "CAIRN_LISTEN_ADDR": f"127.0.0.1:{port}", "CAIRN_WEB_ADDR": "off", "CAIRN_META_SYNCHRONOUS": "full",
                          "CAIRN_META_READ_POOL_SIZE": "8", "CAIRN_META_CACHE_BYTES_PER_CONN": "8388608", "CAIRN_META_MMAP_BYTES": "0",
                          "CAIRN_META_BACKEND": "sqlite", "CAIRN_META_SHARDS": "1", "CAIRN_META_GROUP_COMMIT_LINGER_MICROS": "0",
                          "CAIRN_META_CACHE_TOTAL_BUDGET_BYTES": "2147483648", "CAIRN_META_CACHE_BYTES": "67108864",
                          "CAIRN_BLOB_IO_POOL_SIZE": "64", "CAIRN_BLOB_IO_READ_POOL_SIZE": "64",
                          "CAIRN_RUNTIME_WORKER_THREADS": "8", "CAIRN_RUNTIME_MAX_BLOCKING_THREADS": "512",
                          "CAIRN_CONCURRENCY_LIMIT": "1024", "CAIRN_MAX_CONNECTIONS": "8192",
                          "CAIRN_WAL_CHECKPOINT_INTERVAL_SECS": "300", "CAIRN_WAL_CHECKPOINT_SIZE_BYTES": "67108864",
                          "CAIRN_AUTH_CACHE_TTL_SECS": "30", "CAIRN_ENCRYPT_AT_REST": "false", "CAIRN_UPDATE_CHECK_ENABLED": "false"}
            report["server_configuration"] = {key: value for key, value in server_env.items()
                                              if key.startswith("CAIRN_") and key not in {"CAIRN_MASTER_KEY", "CAIRN_ROOT_ACCESS_KEY", "CAIRN_ROOT_SECRET_KEY"}}
            report["remaining_defaults"] = f"unset CAIRN_* defaults from crates/cairn-server/src/config.rs at declared {args.commit}; no inherited CAIRN_* values"
            bootstrap = launch([str(binary), "bootstrap"], "bootstrap", server_env, discard=True)
            while bootstrap.exited() is None:
                if time.monotonic() >= min(work_deadline, start + 15):
                    raise Unavailable("bootstrap timed out")
                time.sleep(0.05)
            if bootstrap.exited() != 0:
                raise RuntimeError("bootstrap failed")
            server = launch(profile_command(args.profile, artifacts, [str(binary), "serve"]), "server", server_env)
            while True:
                if server.exited() is not None:
                    raise Unavailable("server/profiler exited before readiness")
                if time.monotonic() >= min(work_deadline, start + 25):
                    raise Unavailable("server readiness timeout")
                try:
                    metrics(port)
                    break
                except (OSError, http.client.HTTPException):
                    time.sleep(0.05)
            command = [sys.executable, str(HERE / "s3_driver.py"), str(artifacts / "config.json")]
            env = {**env, "LAB_PORT": str(port), "LAB_SECRET": secret}
        else:
            command = profile_command(args.profile, artifacts, command)
        driver = launch(command, "driver", env)
        with (artifacts / "samples.jsonl").open("w") as samples:
            while driver.exited() is None:
                now = time.monotonic()
                if now >= work_deadline:
                    raise Unavailable("runtime allowance reached; admissions stopped for cleanup")
                used = footprint(campaign.root)
                campaign.ledger["peak_bytes"] = max(campaign.ledger["peak_bytes"], used)
                if used > campaign.ledger["active"]["initial_bytes"] + reserved - 256 * 1024**2 or used >= SPACE_LIMIT - 1_000_000_000:
                    raise Unavailable("reserved disk headroom reached; admissions stopped")
                if any(child.output_overflow or child.output_error for child in children):
                    raise Unavailable("bounded output cap reached; measurement incomplete")
                if server and server.exited() is not None:
                    raise RuntimeError("server exited during workload")
                measurement = {"elapsed": now - start, "footprint_bytes": used, "driver": sample(driver.process.pid)}
                if server:
                    measurement["server"] = sample(server.process.pid)
                    measurement.update(metrics_sample(port))
                    metrics_gaps += int("metrics_unavailable" in measurement)
                samples.write(json.dumps(measurement) + "\n")
                samples.flush()
                time.sleep(0.25)
        if driver.exited() != 0:
            if args.profile != "none":
                raise Unavailable("profiled driver failed; inspect bounded stderr")
            raise RuntimeError("driver operation failed; inspect bounded stderr")
        driver.stop(min(deadline, time.monotonic() + 4))
        if driver.output_error or driver.output_overflow:
            raise Unavailable("driver output incomplete")
        records = [json.loads(line) for line in driver.paths[0].read_text().splitlines() if line.startswith("{")]
        cycles = [record for record in records if record.get("kind") == "cycle"]
        report["cycles"] = cycles
        if len(cycles) != 3 or not any(record.get("kind") == "complete" for record in records):
            raise Unavailable("insufficient completed load/idle cycles")
        if any(cycle["operation_cap_reached"] or cycle["successful_transactions"] == 0 for cycle in cycles):
            raise Unavailable("operation cap or insufficient samples prevented equal load cycles")
        if any(len(cycle.get("bucket_transactions", [])) != args.buckets or not all(cycle["bucket_transactions"]) for cycle in cycles):
            raise Unavailable("not every configured bucket received successful transactions")
        if args.profile != "none":
            # Collection is evidence availability, not an attribution conclusion. Decoding and
            # correlating stacks is a separate charged invocation in the results phase.
            if server:
                server.finish_profiled_target(binary, min(deadline - 8, time.monotonic() + 8))
            profile_files = list(artifacts.glob("cpu.perf" if args.profile == "cpu" else "heap*"))
            if not profile_files or not any(path.stat().st_size for path in profile_files):
                raise Unavailable("profiler produced no usable artifact")
            report["profile_analysis"] = "pending; no bottleneck or leak claim established"
            raise Unavailable("profile collected; sample sufficiency and stack attribution require analysis")
        if metrics_gaps:
            raise Unavailable("metrics sampling was incomplete")
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
            if any(child.output_error or child.output_overflow for child in children):
                if status == "PASS":
                    status = "INCONCLUSIVE"
                reasons.append("one or more child output streams were incomplete")
            if (directory / "data").exists():
                remove_tree(directory / "data", deadline - 1)
            clean = True
        except Exception as error:
            status = "INCONCLUSIVE"
            reasons.append(f"cleanup incomplete: {error}")
        elapsed = time.monotonic() - start
        if elapsed > args.allow_seconds:
            status = "INCONCLUSIVE"
            reasons.append("wall time exceeded the reservation; actual time charged")
        report.update(status=status, reasons=reasons, metrics_sampling_gaps=metrics_gaps,
                      elapsed_seconds=elapsed, cleaned_data_and_processes=clean)
        atomic_json(campaign.root / f"{token}.result.json", report)
        campaign.finish(time.monotonic() - start, status, clean=clean)
    print(json.dumps({"status": status, "reasons": reasons, "result": str(campaign.root / f"{token}.result.json"),
                      "spent_seconds": campaign.ledger["spent_seconds"], "peak_bytes": campaign.ledger["peak_bytes"]}))
    return RESULTS[status]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("init", "status", "run", "recover", "purge"))
    parser.add_argument("--root", required=True)
    parser.add_argument("--phase", choices=PHASES, default="baseline")
    parser.add_argument("--binary")
    parser.add_argument("--commit")
    parser.add_argument("--build-settings")
    parser.add_argument("--layer", choices=("meta", "blob", "s3"), default="meta")
    parser.add_argument("--profile", choices=("none", "cpu", "heap"), default="none")
    parser.add_argument("--concurrency", type=int, choices=(4, 32, 128), default=4)
    parser.add_argument("--buckets", type=int, choices=(1, 16, 128), default=1)
    parser.add_argument("--size", type=int, choices=(1024, 4096, 16384, 65536, 262144, 1048576), default=4096)
    parser.add_argument("--seed", type=int, default=0x5EED)
    parser.add_argument("--seconds", type=int, default=5)
    parser.add_argument("--idle", type=int, default=2)
    parser.add_argument("--cycles", type=int, choices=(3,), default=3)
    parser.add_argument("--max-ops", type=int, default=30_000)
    parser.add_argument("--allow-seconds", type=int, default=60)
    parser.add_argument("--primary-metric", default="successful transactions per second")
    parser.add_argument("--protected", default="operation errors, returned bytes, latency, memory")
    args = parser.parse_args()
    if args.action == "run" and (not all((args.binary, args.commit, args.build_settings))
            or not 1 <= args.seconds <= 60 or not 0 <= args.idle <= 30 or not 3 <= args.max_ops <= 300_000
            or not 0 <= args.seed < 2**64):
        parser.error("run requires binary, commit, build-settings and bounded workload parameters")
    campaign = None
    try:
        campaign = Campaign(args.root, create=args.action == "init")
        if args.action == "run":
            return run_case(args, campaign)
        if args.action == "recover":
            result = recover(campaign)
            print(json.dumps(result))
            return 0
        if args.action == "purge":
            print(json.dumps(purge_artifacts(campaign)))
            return 0
        print(json.dumps(campaign.ledger, indent=2))
        return 0
    except Unavailable as error:
        print(json.dumps({"status": "INCONCLUSIVE", "reason": str(error)}))
        return 2
    finally:
        if campaign:
            campaign.close()


if __name__ == "__main__":
    def interrupted(*_):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
