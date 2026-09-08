#!/usr/bin/env python3
"""Predeclared, charged paired packing screen; no production adoption authority."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import resource
import signal
import statistics
import time

from budget import Campaign, CLEANUP_HEADROOM, SPACE_LIMIT, Unavailable, atomic_json, footprint, remove_tree
from fanout import summarize_observations
from lab import RESULTS, clean_environment, manifest
from packing import ADMISSION_BYTES, MIB, RUN_HEADROOM, SAMPLE_LIMIT, read_driver_record
from processes import Child, OUTPUT_LIMIT, sample

OBJECTS = 1024
SIZES = (1024, 4096, 16384, 65536, 262144, MIB)
CASES = {f"known-{size}": {"size": size, "objects": OBJECTS, "concurrency": 32, "known_length": True}
         for size in SIZES}
CASES.update({f"unknown-{size}": {"size": size, "objects": OBJECTS, "concurrency": 32, "known_length": False}
              for size in (4096, MIB)})
CASES[f"known-{2 * MIB}"] = {"size": 2 * MIB, "objects": OBJECTS, "concurrency": 32, "known_length": True}
GLOBAL_PROTECTED_CASES = ("unknown-4096", f"unknown-{MIB}", f"known-{2 * MIB}")
TIMINGS = ("workload_seconds", "publication_seconds", "overwrite_seconds", "delete_seconds",
           "readback_seconds", "range_read_seconds", "collection_seconds", "cleanup_seconds",
           "snapshot_seconds", "restore_seconds", "reopen_seconds")
DIRECT_PROTECTED = tuple(name for name in TIMINGS if name != "collection_seconds") + ("peak_rss_kib",)
PROTECTED = DIRECT_PROTECTED + ("publication_mean_seconds", "publication_max_seconds")
DRIFT_METRICS = ("workload_seconds", "publication_seconds", "readback_seconds", "range_read_seconds")
MIN_SAMPLES = 2
MIN_OBSERVED_SECONDS = .1
DRIFT_LIMIT = .20


def validate_args(args):
    if (type(args.pairs) is not int or args.pairs not in (1, 5)
            or type(args.seed) is not int or not 0 <= args.seed < 2**64
            or type(args.allow_seconds) is not int or not 30 <= args.allow_seconds <= 1080):
        raise ValueError("one/five pairs, u64 seed and 30..1080 seconds required")


def reserve_bytes(cases, pairs):
    # Original, replacement, snapshot and restored corpus plus metadata/WAL amplification.
    # Completed arms retain bounded stdout, stderr and process samples until final cleanup.
    corpus = max(config["objects"] * (config["size"] + 16_384 + 256 * 1024)
                 for config in cases.values())
    arms = len(cases) * pairs * 2
    return 4 * corpus + ADMISSION_BYTES + 4 * MIB + (arms + 8) * (2 * OUTPUT_LIMIT + SAMPLE_LIMIT) + RUN_HEADROOM


def number(record, name, *, positive=True):
    value = record.get(name)
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0 or (positive and value == 0):
        raise Unavailable(f"missing/invalid measurement: {name}")
    return value


def count(record, name):
    value = record.get(name)
    if type(value) is not int or value < 0:
        raise Unavailable(f"missing/invalid count: {name}")
    return value


def validate_result(record, config):
    if not isinstance(record, dict) or record.get("status") != "PASS" or type(record.get("measurement_protocol")) is not int or record["measurement_protocol"] != 1:
        raise Unavailable("complete measurement protocol 1 result required")
    for name in ("mode", "size", "objects"):
        if type(record.get(name)) is not type(config[name]) or record[name] != config[name]:
            raise RuntimeError(f"driver disagrees with requested {name}")
    objects = config["objects"]
    expected = {"published": objects, "verified": objects, "overwritten": objects // 4,
                "deleted": objects // 2, "final_records": objects // 2,
                "survivor_verified": objects // 2, "restored_verified": objects // 2,
                "reopened_verified": objects // 2, "range_read_count": objects // 2,
                "pending_writes": 0, "cleanup_debts": 0}
    for name, value in expected.items():
        if count(record, name) != value:
            raise RuntimeError(f"incorrect complete workload count: {name}")
    packed = config["mode"] == "packed" and config["known_length"] and config["size"] <= MIB
    if count(record, "packed_records") != (objects if packed else 0) or count(record, "dedicated_records") != (0 if packed else objects):
        raise RuntimeError("append placement counts violate requested policy")
    artifacts = count(record, "artifact_count")
    if not 1 <= artifacts <= objects or (not packed and artifacts != objects):
        raise RuntimeError("invalid initial append artifact count")
    if not 0 < count(record, "peak_admitted_bytes") <= ADMISSION_BYTES:
        raise RuntimeError("32-MiB admission ceiling violated")
    # One metadata page and one actual collector job each consume a record permit.
    if not 0 < count(record, "peak_pending_records") <= max(2, min(objects, config["concurrency"])):
        raise RuntimeError("pending admission ceiling violated")
    for name in TIMINGS:
        number(record, name, positive=name != "collection_seconds")
    if record["workload_seconds"] + 1e-6 < sum(record[name] for name in
            ("publication_seconds", "overwrite_seconds", "delete_seconds", "collection_seconds", "cleanup_seconds")):
        raise RuntimeError("primary timer omits measured mutation/collection/cleanup work")
    for name in ("peak_rss_kib", "live_physical_bytes", "range_read_bytes"):
        if count(record, name) == 0:
            raise Unavailable(f"nonzero measured {name} required")
    if record["range_read_bytes"] > objects // 2 * config["size"]:
        raise RuntimeError("range-read bytes exceed full survivor corpus")
    latency = record.get("publication_latency")
    if not isinstance(latency, dict) or count(latency, "count") != objects:
        raise Unavailable("initial publication latency count missing")
    if number(latency, "max_seconds") > number(latency, "sum_seconds"):
        raise RuntimeError("publication maximum exceeds summed latency")
    if latency.get("p99_seconds") is not None and objects < 10_000:
        raise Unavailable("p99 requires 10,000 successful requests in each arm")
    collection = record.get("collection_check")
    if not isinstance(collection, dict) or collection.get("completed") is not True:
        raise Unavailable("complete collection observation missing")
    if packed and (count(collection, "copied_records") == 0 or count(collection, "retired_sources") == 0):
        raise Unavailable("packed arm did not exercise live-record collection and retirement")
    return record


def measurements(arm, config):
    values = validate_result(arm["measurements"], {**config, "mode": arm["mode"]})
    result = {name: number(values, name) for name in DIRECT_PROTECTED}
    result["publication_mean_seconds"] = values["publication_latency"]["sum_seconds"] / values["publication_latency"]["count"]
    result["publication_max_seconds"] = values["publication_latency"]["max_seconds"]
    return result


def assess(arms, cases, pairs):
    result = {"status": "INCONCLUSIVE", "decision": "KEEP files", "threshold_bytes": None,
              "reasons": [], "comparisons": {}}
    try:
        if cases != CASES or pairs != 5 or len(arms) != len(CASES) * 10:
            raise Unavailable("five complete pairs across the full predeclared matrix required")
        indexed = {(arm["case"], arm["pair"], arm["mode"]): arm for arm in arms}
        if len(indexed) != len(arms):
            raise Unavailable("duplicate comparison arms")
        for arm in arms:
            if (arm["status"] != "PASS" or arm.get("sample_count", 0) < MIN_SAMPLES
                    or number(arm, "sampled_seconds") < MIN_OBSERVED_SECONDS
                    or number(arm, "observer_average_cpu_cores", positive=False) >= .5):
                raise Unavailable("insufficient target observations or saturated observer")
        for case, config in cases.items():
            controls, ratios = [], []
            for pair in range(pairs):
                control = measurements(indexed[case, pair, "files"], config)
                candidate = measurements(indexed[case, pair, "packed"], config)
                controls.append(control)
                ratios.append({metric: candidate[metric] / control[metric] for metric in PROTECTED})
            medians = {metric: statistics.median(row[metric] for row in ratios) for metric in PROTECTED}
            drift = {metric: max(row[metric] for row in controls) / min(row[metric] for row in controls) - 1
                     for metric in DRIFT_METRICS}
            result["comparisons"][case] = {"median_packed_to_files": medians, "paired_ratios": ratios,
                                             "files_control_relative_span": drift}
            if any(value > DRIFT_LIMIT for value in drift.values()):
                result["reasons"].append(f"{case}: files control drift exceeds 20% span")
        if result["reasons"]:
            return result
        failures = [(case, metric, ratio) for case in GLOBAL_PROTECTED_CASES for metric, ratio in
                    result["comparisons"][case]["median_packed_to_files"].items() if ratio > 1.10]
        threshold = None
        for size in SIZES:
            ratios = result["comparisons"][f"known-{size}"]["median_packed_to_files"]
            qualifies = ratios["workload_seconds"] <= .80 and all(value <= 1.10 for value in ratios.values())
            result["comparisons"][f"known-{size}"]["qualifies"] = qualifies
            if qualifies and (threshold is None and size == SIZES[0] or threshold is not None):
                threshold = size
            else:
                break
        if failures or threshold is None:
            result.update(status="FAIL", reasons=["no qualifying contiguous range with protected workloads"],
                          protected_regressions=failures)
        else:
            result.update(status="PASS", decision="PROPOSE separate format/migration/contract review",
                          threshold_bytes=threshold)
    except (KeyError, TypeError, ValueError, RuntimeError) as error:
        result["reasons"].append(f"missing/invalid evidence: {error}")
    return result


def run_comparison(args, campaign, cases=CASES):
    validate_args(args)
    reserved = reserve_bytes(cases, args.pairs)
    token = campaign.admit("packing", args.allow_seconds, reserved)
    start = time.monotonic()
    deadline, work_deadline = start + args.allow_seconds, start + args.allow_seconds - 15
    directory = campaign.root / token
    children, arms, reasons = [], [], []
    status, clean = "INCONCLUSIVE", False
    report = {"id": token, "kind": "packing-measurement", "cases": cases, "pairs": args.pairs,
              "seed": args.seed, "arms": arms,
              "primary": "total append/overwrite/delete/collection/exact-cleanup wall seconds; 20% median reduction",
              "protected_metrics": PROTECTED, "global_protected_cases": GLOBAL_PROTECTED_CASES,
              "control_drift_limit": DRIFT_LIMIT,
              "cache_state": "uncontrolled; process restart does not imply cold cache",
              "scope": "raw fixture generation, durable publication and collection; S3 and encoding excluded",
              "tail_scope": "count/sum/maximum only; fewer than 10,000 requests per phase, no p99 claim",
              "statistical_scope": "five paired descriptive medians; no significance or confidence claim"}
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

        def check_budget():
            if time.monotonic() >= work_deadline:
                raise Unavailable("runtime allowance reached; preserving cleanup time")
            used = footprint(campaign.root)
            campaign.ledger["peak_bytes"] = max(campaign.ledger["peak_bytes"], used)
            if used > campaign.ledger["active"]["initial_bytes"] + reserved - RUN_HEADROOM or used >= SPACE_LIMIT - CLEANUP_HEADROOM:
                raise Unavailable("reserved disk headroom reached")
            if any(child.output_error or child.output_overflow for child in children):
                raise Unavailable("bounded child output incomplete")
            return used

        def launch(command, label):
            check_budget()
            child = Child(command, environment, artifacts, label, campaign)
            children.append(child)
            return child

        report["manifest"] = manifest(args, binary, launch)
        source = Path(__file__).parent
        report["packing_sources_sha256"] = {str(path.relative_to(source)): hashlib.sha256(path.read_bytes()).hexdigest()
                                             for path in sorted((source / "src/packing").glob("*.rs"))}
        report["preregistration_sha256"] = hashlib.sha256((source / "../../docs/storage-packing-measurement-2026-09.md").read_bytes()).hexdigest()
        for pair in range(args.pairs):
            for case, parameters in cases.items():
                for mode in (("files", "packed") if pair % 2 == 0 else ("packed", "files")):
                    check_budget()
                    seconds = min(120, math.floor(work_deadline - time.monotonic()))
                    if seconds < 1:
                        raise Unavailable("no complete driver second remains")
                    label = f"{case}-{pair}-{mode}"
                    config = {**parameters, "root": str(data / label), "mode": mode, "seed": args.seed,
                              "measurement": True, "deadline_seconds": seconds}
                    config_path = artifacts / f"{label}.config.json"
                    atomic_json(config_path, config)
                    driver = launch([str(binary), str(config_path)], label)
                    observations = []
                    cpu_start = resource.getrusage(resource.RUSAGE_SELF)
                    observed_start = time.monotonic()
                    with (artifacts / f"{label}.samples.jsonl").open("w") as stream:
                        while driver.exited() is None:
                            used = check_budget()
                            observation = sample(driver.process.pid)
                            observation["footprint_bytes"] = used
                            stream.write(json.dumps(observation) + "\n")
                            stream.flush()
                            if stream.tell() > SAMPLE_LIMIT:
                                raise Unavailable("bounded process observations exceeded")
                            observations.append(observation)
                            time.sleep(.02)
                    outcome = driver.exited()
                    driver.stop(min(deadline - 2, time.monotonic() + 4))
                    check_budget()
                    if outcome == RESULTS["INCONCLUSIVE"]:
                        terminal = read_driver_record(driver.paths[0])
                        if terminal.get("status") == "INCONCLUSIVE":
                            raise Unavailable(str(terminal.get("reason", "driver observation incomplete")))
                    if outcome != 0:
                        raise RuntimeError(f"{label}: driver failed with exit {outcome}; logs retained")
                    terminal = read_driver_record(driver.paths[0])
                    wall = time.monotonic() - observed_start
                    cpu_end = resource.getrusage(resource.RUSAGE_SELF)
                    observer_cpu = cpu_end.ru_utime + cpu_end.ru_stime - cpu_start.ru_utime - cpu_start.ru_stime
                    arm = {"case": case, "pair": pair, "mode": mode, "status": "PASS",
                           "configuration": config, "measurements": validate_result(terminal, config),
                           "observer_average_cpu_cores": observer_cpu / wall, "driver_wall_seconds": wall}
                    try:
                        arm.update(summarize_observations(observations, binary, args.device))
                    except Unavailable as error:
                        arm.update(status="INCONCLUSIVE", sample_count=0, measurement_gap=str(error))
                    arms.append(arm)
                    atomic_json(artifacts / f"{label}.result.json", arm)
                    remove_tree(Path(config["root"]), deadline - 2)
                    print(json.dumps({"arm": label, "status": arm["status"], "completed_arms": len(arms)}), flush=True)
        report["assessment"] = assess(arms, cases, args.pairs)
        status = report["assessment"]["status"]
        reasons.extend(report["assessment"]["reasons"])
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
                status = "INCONCLUSIVE"
                reasons.append("bounded child output incomplete after final drain")
            for owned in (directory / "data", directory / "artifacts/scratch"):
                if owned.exists():
                    remove_tree(owned, deadline - 1)
            clean = True
        except Exception as error:
            status = "INCONCLUSIVE"
            reasons.append(f"cleanup incomplete: {error}")
        elapsed = time.monotonic() - start
        charged = elapsed + 1
        if charged > args.allow_seconds:
            status = "INCONCLUSIVE"
            reasons.append("wall time exceeded reservation; actual time plus finalization charged")
        decision = report.get("assessment", {}).get("decision", "KEEP files") if status == "PASS" else "KEEP files"
        report.update(status=status, decision=decision, reasons=reasons, elapsed_seconds=elapsed,
                      charged_seconds=charged, cleaned_data_and_processes=clean, finalization_allowance_seconds=1)
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
    parser.add_argument("--pairs", type=int, choices=(1, 5), default=5)
    parser.add_argument("--case", choices=("all", *CASES), default="all")
    parser.add_argument("--seed", type=int, default=0x5eed)
    parser.add_argument("--allow-seconds", type=int, default=1080)
    parser.add_argument("--device", default="sdb1")
    args = parser.parse_args()
    try:
        validate_args(args)
    except ValueError as error:
        parser.error(str(error))
    campaign = None
    try:
        campaign = Campaign(args.root)
        cases = CASES if args.case == "all" else {args.case: CASES[args.case]}
        return run_comparison(args, campaign, cases)
    except KeyboardInterrupt:
        print(json.dumps({"status": "CANCELLED", "decision": "KEEP files", "reason": "operator interruption"}))
        return RESULTS["CANCELLED"]
    except Unavailable as error:
        print(json.dumps({"status": "INCONCLUSIVE", "decision": "KEEP files", "reason": str(error)}))
        return RESULTS["INCONCLUSIVE"]
    except (OSError, ValueError) as error:
        print(json.dumps({"status": "FAIL", "decision": "KEEP files", "reason": str(error)}))
        return RESULTS["FAIL"]
    finally:
        if campaign:
            campaign.close()


if __name__ == "__main__":
    def interrupted(*_):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
