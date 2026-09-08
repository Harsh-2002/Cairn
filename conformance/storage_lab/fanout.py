#!/usr/bin/env python3
"""Charged, paired namespace comparisons; incomplete evidence always retains flat placement."""
import argparse
import json
import math
import os
from pathlib import Path
import resource
import signal
import statistics
import time

from budget import Campaign, SPACE_LIMIT, Unavailable, atomic_json, footprint, remove_tree
from lab import RESULTS, clean_environment, manifest
from processes import Child, sample

PRIMARY = "all-live reconciliation wall seconds, 20,480 raw 4-KiB files in one hot bucket"
PROTECTED = "publication, whole/range GET, delete, cleanup scan and peak RSS; 16 buckets and 1-MiB control"
CASES = {
    "hot-4k": {"objects": 20_480, "buckets": 1, "size": 4096, "concurrency": 32},
    "distributed-4k": {"objects": 20_480, "buckets": 16, "size": 4096, "concurrency": 32},
    "large-1m": {"objects": 512, "buckets": 1, "size": 1_048_576, "concurrency": 4},
}


def reserve_bytes(cases):
    # Reserve BOTH stores despite serialized teardown, inode/directory allocation, pending
    # temporary+final files, read frames and bounded artifacts. There is no database/WAL here.
    per_store = max(config["objects"] * (config["size"] + 16_384)
                    + config["buckets"] * 257 * 65_536
                    + config["concurrency"] * (2 * config["size"] + 512 * 1024)
                    for config in cases.values())
    return 2 * per_store + 512 * 1024**2


def _reduce_records(records, config):
    complete = [record for record in records if record.get("kind") == "complete"]
    if len(complete) != 1 or complete[0].get("status") != "PASS":
        raise Unavailable("driver did not complete exact survivor verification")
    phases = [record for record in records if record.get("kind") in ("operations", "reconcile")]
    result = {record["phase"]: record for record in phases}
    if len(phases) != 6 or set(result) != {"publish", "read", "delete", "verify", "live_scan", "cleanup_scan"}:
        raise Unavailable("driver phase measurements are incomplete")
    durability = [record for record in records if record.get("kind") == "durability"]
    if len(durability) != 1:
        raise Unavailable("directory durability measurements are missing")
    count = config["objects"]
    if any(result[phase]["count"] != expected for phase, expected in
           (("publish", count), ("read", count), ("delete", count // 2), ("verify", count))):
        raise Unavailable("unexpected successful operation counts")
    if result["live_scan"]["scanned"] != count or result["live_scan"]["reclaimed"] != 0 or result["cleanup_scan"]["scanned"] != count // 2 or result["cleanup_scan"]["reclaimed"] != count // 4:
        raise Unavailable("unexpected scan/survivor counts")
    for phase in ("publish", "read", "delete"):
        if sum(latency["count"] for latency in result[phase]["latencies"]) != result[phase]["count"]:
            raise Unavailable("latency sample count differs from successful operation count")
    result["durability"] = durability[0]
    result["peak_rss_kib"] = complete[0].get("peak_rss_kib")
    if not result["peak_rss_kib"]:
        raise Unavailable("process peak RSS measurement is missing")
    return result


def reduce_records(records, config):
    try:
        return _reduce_records(records, config)
    except (KeyError, TypeError, ValueError) as error:
        raise Unavailable(f"missing/invalid driver measurements: {error}") from error


def measurements(arm):
    data = arm["measurements"]
    values = {"live_scan_seconds": data["live_scan"]["wall_seconds"],
              "cleanup_scan_seconds": data["cleanup_scan"]["wall_seconds"], "peak_rss_kib": data["peak_rss_kib"]}
    for phase in ("publish", "read", "delete"):
        values[f"{phase}_wall_seconds"] = data[phase]["wall_seconds"]
        for index, latency in enumerate(data[phase]["latencies"]):
            if not latency.get("count"):
                continue
            values[f"{phase}_{index}_p50_seconds"] = latency["p50_seconds"]
            if latency.get("p99_seconds") is not None:
                if latency["count"] < 10_000:
                    raise ValueError("p99 claimed without 10,000 successful samples")
                values[f"{phase}_{index}_p99_seconds"] = latency["p99_seconds"]
    if not all(isinstance(value, (int, float)) and math.isfinite(value) and value > 0 for value in values.values()):
        raise Unavailable("invalid/missing comparison metric")
    return values


def _assess(arms, cases, pairs):
    result = {"status": "INCONCLUSIVE", "decision": "KEEP flat", "reasons": [], "comparisons": {}}
    if cases != CASES:
        result["reasons"].append("the complete predeclared workload matrix is required for adoption")
        return result
    if pairs < 5 or len(arms) != pairs * len(cases) * 2:
        result["reasons"].append("fewer than five complete paired runs for every declared workload")
        return result
    if any(arm["status"] != "PASS" or arm.get("sample_count", 0) < 2 for arm in arms):
        result["reasons"].append("operation failure or missing process measurements")
        return result
    indexed = {(arm["case"], arm["pair"], arm["layout"]): arm for arm in arms}
    if len(indexed) != len(arms):
        result["reasons"].append("duplicate comparison arms")
        return result
    for case in cases:
        controls, candidates, ratios = [], [], []
        for pair in range(pairs):
            try:
                flat = measurements(indexed[case, pair, "flat"])
                fanout = measurements(indexed[case, pair, "fanout"])
            except KeyError:
                result["reasons"].append("missing paired workload")
                return result
            if flat.keys() != fanout.keys():
                result["reasons"].append("unequal measurement/sample coverage")
                return result
            controls.append(flat)
            candidates.append(fanout)
            ratios.append({metric: fanout[metric] / flat[metric] for metric in flat})
        medians = {metric: statistics.median(row[metric] for row in ratios) for metric in ratios[0]}
        drift = {metric: max(row[metric] for row in controls) / min(row[metric] for row in controls) - 1
                 for metric in ("live_scan_seconds", "publish_wall_seconds", "read_wall_seconds", "delete_wall_seconds")}
        result["comparisons"][case] = {"median_candidate_to_flat": medians, "paired_ratios": ratios,
                                        "flat_control_relative_span": drift, "flat": controls, "fanout": candidates}
        if any(value > 0.20 for value in drift.values()):
            result["reasons"].append(f"{case}: flat control drift exceeds predeclared 20% span")
    if result["reasons"]:
        return result
    if "hot-4k" not in result["comparisons"]:
        result["reasons"].append("nominated primary workload is absent")
        return result
    primary = result["comparisons"]["hot-4k"]["median_candidate_to_flat"]["live_scan_seconds"]
    regressions = [(case, metric, value) for case, comparison in result["comparisons"].items()
                   for metric, value in comparison["median_candidate_to_flat"].items() if value > 1.10]
    if primary > 0.80 or regressions:
        result.update(status="FAIL", reasons=["candidate did not meet adoption gates"], protected_regressions=regressions)
    else:
        result.update(status="PASS", decision="PROPOSE separate promotion review", reasons=[])
    return result


def assess(arms, cases, pairs):
    try:
        return _assess(arms, cases, pairs)
    except (KeyError, TypeError, ValueError, Unavailable) as error:
        return {"status": "INCONCLUSIVE", "decision": "KEEP flat", "reasons": [f"missing/invalid measurements: {error}"], "comparisons": {}}


def summarize_observations(observations, binary, device):
    selected = [(observation, process) for observation in observations for process in observation["processes"]
                if process.get("executable") == str(binary)]
    if len(selected) < 2:
        raise Unavailable("fewer than two target-executable process samples")
    first, last = selected[0], selected[-1]
    def number(process, section, key):
        value = process.get(section, {}).get(key)
        return int(value.split()[0]) if value else None
    peaks = {}
    for section, keys in {"status": ("VmRSS", "RssAnon", "RssFile", "Threads"), "smaps_rollup": ("Pss", "Anonymous")}.items():
        for key in keys:
            values = [number(process, section, key) for _, process in selected]
            if any(value is None for value in values):
                raise Unavailable(f"missing process memory measurement: {key}")
            peaks[key] = max(values)
    def ticks(process):
        fields = process["stat"].split()
        return int(fields[11]) + int(fields[12])
    elapsed = last[0]["monotonic"] - first[0]["monotonic"]
    def device_counters(observation):
        for line in (observation["host"].get("diskstats") or "").splitlines():
            fields = line.split()
            if fields[2] == device:
                return [int(value) for value in fields[3:]]
        raise Unavailable(f"device counters unavailable: {device}")
    return {"sample_count": len(selected), "sampled_seconds": elapsed, "peak_memory_kib_or_threads": peaks,
            "peak_fds": max(process["fds"] for _, process in selected),
            "driver_average_cpu_cores": (ticks(last[1]) - ticks(first[1])) / os.sysconf("SC_CLK_TCK") / elapsed,
            "device": device, "device_first": device_counters(first[0]), "device_last": device_counters(last[0]),
            "host_pressure_last": {key: last[0]["host"][key] for key in ("pressure/cpu", "pressure/io", "pressure/memory")}}


def run_comparison(args, campaign, cases=CASES):
    reserved = reserve_bytes(cases)
    token = campaign.admit("fanout", args.allow_seconds, reserved)
    start = time.monotonic()
    deadline, work_deadline = start + args.allow_seconds, start + args.allow_seconds - 15
    directory = campaign.root / token
    children, arms, clean, reasons = [], [], False, []
    report = {"id": token, "kind": "fanout", "primary_metric": PRIMARY, "protected_workloads": PROTECTED,
              "cases": cases, "pairs": args.pairs, "seed": args.seed,
              "cache_state": "uncontrolled; process restart does not imply cold cache",
              "units": "raw namespace publication and real BlobStore operations; not S3 throughput",
              "oracle": "exact generated liveness, no SQL lookup cost", "arms": arms}
    status = "INCONCLUSIVE"
    try:
        directory.mkdir(mode=0o700)
        artifacts = directory / "artifacts"
        artifacts.mkdir(mode=0o700)
        (artifacts / "scratch").mkdir(mode=0o700)
        data_root = directory / "data"
        data_root.mkdir(mode=0o700)
        binary = Path(args.binary).resolve(strict=True)
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise ValueError("explicit binary must be an executable regular file")
        env = {**clean_environment(), "TMPDIR": str(artifacts / "scratch")}

        def launch(command, label, environment=env):
            if time.monotonic() >= work_deadline:
                raise Unavailable("runtime budget exhausted before child admission")
            child = Child(command, environment, artifacts, label, campaign)
            children.append(child)
            return child

        report["manifest"] = manifest(args, binary, launch)
        for pair in range(args.pairs):
            # Alternate AB/BA within each workload. Each arm has its own process and dataset.
            for case, parameters in cases.items():
                for layout in (("flat", "fanout") if pair % 2 == 0 else ("fanout", "flat")):
                    label = f"{case}-{pair}-{layout}"
                    config = {**parameters, "root": str(data_root / label), "layout": layout, "seed": args.seed}
                    config_path = artifacts / f"{label}.config.json"
                    atomic_json(config_path, config)
                    driver = launch([str(binary), str(config_path)], label)
                    observations = []
                    cpu_start = resource.getrusage(resource.RUSAGE_SELF)
                    sample_start = time.monotonic()
                    with (artifacts / f"{label}.samples.jsonl").open("w") as stream:
                        while driver.exited() is None:
                            if time.monotonic() >= work_deadline:
                                raise Unavailable("runtime allowance reached; preserving cleanup time")
                            used = footprint(campaign.root)
                            campaign.ledger["peak_bytes"] = max(campaign.ledger["peak_bytes"], used)
                            if used > campaign.ledger["active"]["initial_bytes"] + reserved - 256 * 1024**2 or used >= SPACE_LIMIT - 1_000_000_000:
                                raise Unavailable("reserved disk headroom reached")
                            if any(child.output_error or child.output_overflow for child in children):
                                raise Unavailable("bounded child output was incomplete")
                            observation = sample(driver.process.pid)
                            observation["footprint_bytes"] = used
                            stream.write(json.dumps(observation) + "\n")
                            stream.flush()
                            if stream.tell() > 32 * 1024**2:
                                raise Unavailable("bounded process observation output exceeded")
                            observations.append(observation)
                            time.sleep(0.25)
                    sample_wall = time.monotonic() - sample_start
                    cpu_end = resource.getrusage(resource.RUSAGE_SELF)
                    observer_cpu = cpu_end.ru_utime + cpu_end.ru_stime - cpu_start.ru_utime - cpu_start.ru_stime
                    if driver.exited() != 0:
                        driver.stop(min(deadline - 2, time.monotonic() + 4))
                        diagnostic = driver.paths[1].read_text(errors="replace")[-2048:]
                        raise RuntimeError(f"{label}: driver operation failed: {diagnostic}")
                    driver.stop(min(deadline - 2, time.monotonic() + 4))
                    records = [json.loads(line) for line in driver.paths[0].read_text().splitlines() if line.startswith("{")]
                    arm = {"case": case, "pair": pair, "layout": layout, "status": "PASS", "measurements": reduce_records(records, parameters),
                           "observer_cpu_seconds": observer_cpu, "observer_average_cpu_cores": observer_cpu / sample_wall}
                    try:
                        arm.update(summarize_observations(observations, binary, args.device))
                    except Unavailable as error:
                        arm.update(status="INCONCLUSIVE", measurement_gap=str(error), sample_count=0)
                    arms.append(arm)
                    if arm["observer_average_cpu_cores"] >= 0.5:
                        arm["status"] = "INCONCLUSIVE"
                        reasons.append(f"{label}: observer consumed at least half a CPU core during sampling")
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
            if (directory / "data").exists():
                remove_tree(directory / "data", deadline - 1)
            clean = True
        except Exception as error:
            status = "INCONCLUSIVE"
            reasons.append(f"cleanup incomplete: {error}")
        elapsed = time.monotonic() - start
        # Conservatively include final serialization and the ledger fsync in the charged time.
        charged = elapsed + 1
        if charged > args.allow_seconds:
            status = "INCONCLUSIVE"
            reasons.append("wall time exceeded reservation; actual time plus finalization charged")
        report.update(status=status, decision=report.get("assessment", {}).get("decision", "KEEP flat"),
                      reasons=reasons, elapsed_seconds=elapsed, charged_seconds=charged,
                      cleaned_data_and_processes=clean, finalization_allowance_seconds=1)
        atomic_json(campaign.root / f"{token}.result.json", report)
        campaign.finish(max(charged, time.monotonic() - start), status, clean=clean)
    print(json.dumps({"status": status, "decision": report["decision"], "result": str(campaign.root / f"{token}.result.json"),
                      "reasons": reasons,
                      "spent_seconds": campaign.ledger["spent_seconds"], "peak_bytes": campaign.ledger["peak_bytes"]}), flush=True)
    return RESULTS[status]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--build-settings", required=True)
    parser.add_argument("--pairs", type=int, choices=(1, 5), default=5)
    parser.add_argument("--case", choices=("all", *CASES), default="all")
    parser.add_argument("--seed", type=int, default=0x5eed)
    parser.add_argument("--allow-seconds", type=int, default=720)
    parser.add_argument("--device", default="sdb1")
    args = parser.parse_args()
    if not 30 <= args.allow_seconds <= 720 or not 0 <= args.seed < 2**64:
        parser.error("bounded runtime and u64 seed required")
    campaign = None
    try:
        campaign = Campaign(args.root)
        cases = CASES if args.case == "all" else {args.case: CASES[args.case]}
        return run_comparison(args, campaign, cases)
    except Unavailable as error:
        print(json.dumps({"status": "INCONCLUSIVE", "decision": "KEEP flat", "reason": str(error)}))
        return 2
    finally:
        if campaign:
            campaign.close()


if __name__ == "__main__":
    def interrupted(*_):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
