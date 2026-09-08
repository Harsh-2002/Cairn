#!/usr/bin/env python3
"""One bounded hybrid-publication diagnostic arm; no packing adoption decision."""
import argparse
import json
import math
import os
from pathlib import Path
import signal
import time

from budget import Campaign, CLEANUP_HEADROOM, SPACE_LIMIT, Unavailable, atomic_json, footprint, remove_tree
from lab import RESULTS, clean_environment, manifest
from processes import Child, OUTPUT_LIMIT, sample

MIB = 1024**2
ADMISSION_BYTES = 32 * MIB
PACK_THRESHOLD = MIB
SAMPLE_LIMIT = 16 * MIB
RUN_HEADROOM = 256 * MIB


def validate_args(args):
    if (args.mode not in ("files", "packed") or type(args.objects) is not int
            or not 1 <= args.objects <= 16_384 or type(args.size) is not int
            or not 1 <= args.size <= 4 * MIB or type(args.concurrency) is not int
            or args.concurrency not in (1, 4, 32, 128) or type(args.seed) is not int
            or not 0 <= args.seed < 2**64 or type(args.known_length) is not bool
            or type(args.allow_seconds) is not int or not 30 <= args.allow_seconds <= 150):
        raise ValueError("bounded size/count/concurrency, u64 seed and 30..150 seconds required")


def reserve_bytes(args):
    # Completed bytes move from temporary to final names, without a second dataset.
    # Count record/inode allocation, generous SQLite/index/WAL amplification, the
    # bounded builder, one final segment, eight children with two capped logs each,
    # process observations and finalization headroom. Campaign adds cleanup headroom.
    return (args.objects * (args.size + 16_384 + 256 * 1024)
            + ADMISSION_BYTES + 4 * MIB + 16 * OUTPUT_LIMIT + SAMPLE_LIMIT + RUN_HEADROOM)


def validate_result(record, args):
    if not isinstance(record, dict) or record.get("status") != "PASS":
        raise Unavailable("driver did not provide a final PASS record")
    for name in ("mode", "size", "objects"):
        if type(record.get(name)) is not type(getattr(args, name)) or record[name] != getattr(args, name):
            raise RuntimeError(f"driver result disagrees with requested {name}")
    counts = ("published", "verified", "artifact_count", "packed_records", "dedicated_records",
              "peak_admitted_bytes", "peak_pending_records")
    if any(type(record.get(name)) is not int or record[name] < 0 for name in counts):
        raise Unavailable("driver count/admission measurements missing or invalid")
    if record["published"] != args.objects or record["verified"] != args.objects:
        raise RuntimeError("not every requested object was published and verified")
    if record["packed_records"] + record["dedicated_records"] != args.objects:
        raise RuntimeError("packing/dedicated record counts do not cover every object")
    if not 1 <= record["artifact_count"] <= args.objects:
        raise RuntimeError("invalid durable artifact count")
    dedicated = args.mode == "files" or not args.known_length or args.size > PACK_THRESHOLD
    if record["dedicated_records"] != (args.objects if dedicated else 0):
        raise RuntimeError("driver violated encoded-size/known-length placement policy")
    if dedicated and record["artifact_count"] != args.objects:
        raise RuntimeError("dedicated records must each have their own artifact")
    if not 0 < record["peak_admitted_bytes"] <= ADMISSION_BYTES:
        raise RuntimeError("driver exceeded or did not measure the 32-MiB admission budget")
    if not 0 < record["peak_pending_records"] <= min(args.objects, args.concurrency):
        raise RuntimeError("driver pending-record measurement is impossible")
    latency = record.get("publication_latency")
    if not isinstance(latency, dict) or type(latency.get("count")) is not int or latency["count"] != args.objects:
        raise Unavailable("publication latency count is missing or incomplete")
    values = [record.get("publication_seconds"), latency.get("sum_seconds"), latency.get("max_seconds")]
    if any(type(value) not in (int, float) or not math.isfinite(value) or value < 0 for value in values):
        raise Unavailable("publication timing measurements missing or invalid")
    if latency["max_seconds"] > latency["sum_seconds"]:
        raise RuntimeError("publication maximum exceeds summed latency")
    return record


def read_driver_record(path):
    try:
        lines = path.read_text().splitlines()
        records = [json.loads(line) for line in lines if line.strip()]
    except (OSError, ValueError) as error:
        raise Unavailable("driver completion output missing or malformed") from error
    if len(records) != 1:
        raise Unavailable("expected exactly one final driver record")
    if not isinstance(records[0], dict):
        raise Unavailable("driver completion record must be an object")
    return records[0]


def read_result(path, args):
    return validate_result(read_driver_record(path), args)


def run_arm(args, campaign):
    validate_args(args)
    reserved = reserve_bytes(args)
    # Nothing hashes/discovers a binary or creates workload data before admission.
    token = campaign.admit("packing", args.allow_seconds, reserved)
    start = time.monotonic()
    deadline, work_deadline = start + args.allow_seconds, start + args.allow_seconds - 15
    directory = campaign.root / token
    children, reasons = [], []
    status, clean = "INCONCLUSIVE", False
    report = {"id": token, "kind": "packing", "mode": args.mode,
              "configuration": {name: getattr(args, name) for name in
                                ("size", "objects", "concurrency", "seed", "known_length")},
              "measurement_scope": "raw fixture generation, completed-byte publication and readback; encoding and S3 excluded",
              "decision": "NO adoption decision; one diagnostic arm",
              "cache_state": "uncontrolled; process restart does not imply cold cache",
              "pack_threshold_encoded_bytes": PACK_THRESHOLD,
              "timing_scope": "driver publication wall/latency include deterministic fixture generation and admission; readback/cleanup excluded; count/sum/maximum, no p99 claim"}
    try:
        directory.mkdir(mode=0o700)
        artifacts, data = directory / "artifacts", directory / "data"
        artifacts.mkdir(mode=0o700)
        data.mkdir(mode=0o700)
        (artifacts / "scratch").mkdir(mode=0o700)
        binary = Path(args.binary).resolve(strict=True)
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise ValueError("explicit executable regular binary required")
        environment = {**clean_environment(), "TMPDIR": str(artifacts / "scratch")}

        def check_output():
            if any(child.output_error or child.output_overflow for child in children):
                raise Unavailable("bounded child output was incomplete")

        def check_budget():
            if time.monotonic() >= work_deadline:
                raise Unavailable("runtime allowance reached; preserving cleanup time")
            used = footprint(campaign.root)
            campaign.ledger["peak_bytes"] = max(campaign.ledger["peak_bytes"], used)
            if (used > campaign.ledger["active"]["initial_bytes"] + reserved - RUN_HEADROOM
                    or used >= SPACE_LIMIT - CLEANUP_HEADROOM):
                raise Unavailable("reserved disk headroom reached")
            check_output()

        def launch(command, label):
            check_budget()
            child = Child(command, environment, artifacts, label, campaign)
            children.append(child)
            return child

        report["manifest"] = manifest(args, binary, launch)
        check_budget()
        seconds = min(120, math.floor(work_deadline - time.monotonic()))
        if seconds < 1:
            raise Unavailable("no complete driver second remains after preparation")
        config = {**report["configuration"], "root": str(data / "store"), "mode": args.mode,
                  "deadline_seconds": seconds}
        config_path = artifacts / "packing.config.json"
        atomic_json(config_path, config)
        report["driver_configuration"] = config
        command = [str(binary), str(config_path)]
        report["command"] = command
        before = time.monotonic()
        driver = launch(command, "packing")
        sample_count = 0
        with (artifacts / "packing.samples.jsonl").open("w") as stream:
            while driver.exited() is None:
                check_budget()
                stream.write(json.dumps(sample(driver.process.pid)) + "\n")
                stream.flush()
                if stream.tell() > SAMPLE_LIMIT:
                    raise Unavailable("bounded process observation output exceeded")
                sample_count += 1
                time.sleep(0.02)
        outcome = driver.exited()
        driver.stop(min(deadline - 2, time.monotonic() + 4))
        check_budget()  # Includes output flags set while final drains joined.
        if outcome == RESULTS["INCONCLUSIVE"]:
            terminal = read_driver_record(driver.paths[0])
            if terminal.get("status") == "INCONCLUSIVE" and isinstance(terminal.get("reason"), str) and terminal["reason"]:
                raise Unavailable(terminal["reason"])
        if outcome != 0:
            raise RuntimeError(f"packing driver failed (exit {outcome}); bounded logs retained")
        report["measurements"] = read_result(driver.paths[0], args)
        report["coordinator_driver_wall_seconds"] = time.monotonic() - before
        report["process_sample_count"] = sample_count
        report["process_sampling_scope"] = "diagnostic observations; no minimum sample or RSS comparison claim"
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
            # Check again after every tool/driver drain, including exception paths.
            if any(child.output_error or child.output_overflow for child in children):
                if status == "PASS":
                    status = "INCONCLUSIVE"
                reasons.append("bounded child output was incomplete after final drain")
            if (directory / "data").exists():
                remove_tree(directory / "data", deadline - 1)
            if (directory / "artifacts" / "scratch").exists():
                remove_tree(directory / "artifacts" / "scratch", deadline - 1)
            clean = True
        except Exception as error:
            status = "INCONCLUSIVE"
            reasons.append(f"cleanup incomplete: {error}")
        elapsed = time.monotonic() - start
        charged = elapsed + 1
        if charged > args.allow_seconds:
            status = "INCONCLUSIVE"
            reasons.append("wall time exceeded reservation; actual time plus finalization charged")
        report.update(status=status, reasons=reasons, elapsed_seconds=elapsed, charged_seconds=charged,
                      cleaned_data_and_processes=clean, finalization_allowance_seconds=1)
        atomic_json(campaign.root / f"{token}.result.json", report)
        campaign.finish(max(charged, time.monotonic() - start), status, clean=clean)
    print(json.dumps({"status": status, "result": str(campaign.root / f"{token}.result.json"),
                      "reasons": reasons, "spent_seconds": campaign.ledger["spent_seconds"],
                      "peak_bytes": campaign.ledger["peak_bytes"]}), flush=True)
    return RESULTS[status]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("root", "binary", "commit", "build-settings"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--mode", choices=("files", "packed"), required=True)
    parser.add_argument("--size", type=int, required=True)
    parser.add_argument("--objects", type=int, required=True)
    parser.add_argument("--concurrency", type=int, choices=(1, 4, 32, 128), default=4)
    parser.add_argument("--seed", type=int, default=0x5eed)
    parser.add_argument("--unknown-length", dest="known_length", action="store_false")
    parser.add_argument("--allow-seconds", type=int, default=120)
    args = parser.parse_args()
    try:
        validate_args(args)
    except ValueError as error:
        parser.error(str(error))
    campaign = None
    try:
        campaign = Campaign(args.root)
        return run_arm(args, campaign)
    except KeyboardInterrupt:
        print(json.dumps({"status": "CANCELLED", "reason": "operator interruption"}))
        return RESULTS["CANCELLED"]
    except Unavailable as error:
        print(json.dumps({"status": "INCONCLUSIVE", "reason": str(error)}))
        return RESULTS["INCONCLUSIVE"]
    except (OSError, ValueError) as error:
        print(json.dumps({"status": "FAIL", "reason": str(error)}))
        return RESULTS["FAIL"]
    finally:
        if campaign:
            campaign.close()


if __name__ == "__main__":
    def interrupted(*_):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
