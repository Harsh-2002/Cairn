#!/usr/bin/env python3
"""Reduce completed diagnostic records under their existing campaign allowance."""
import argparse
import json
import math
import os
from pathlib import Path
import statistics
import time

from budget import Campaign, Unavailable, atomic_json


def records(path):
    if not path.exists():
        return []
    if path.stat().st_size > 32 * 1024**2:
        raise Unavailable("record reduction input exceeds 32 MiB; inspect it with a streaming tool")
    with path.open() as source:
        return [json.loads(line) for line in source if line.startswith("{")]


def kib(value):
    return int(value.split()[0]) if value else None


def target_samples(samples, group, executable):
    result = []
    for sample in samples:
        measured = sample.get(group)
        if not measured:
            continue
        for process in measured["processes"]:
            if process.get("executable") == executable:
                stat = process["stat"].split()
                result.append({"unix_seconds": measured["unix_seconds"],
                               "identity": process["identity"],
                               "cpu_ticks": int(stat[11]) + int(stat[12]),
                               "rss_kib": kib(process["status"].get("VmRSS")),
                               "anon_kib": kib(process["status"].get("RssAnon")),
                               "file_kib": kib(process["status"].get("RssFile")),
                               "pss_kib": kib(process["smaps_rollup"].get("Pss")),
                               "threads": int(process["status"]["Threads"]), "fds": process["fds"]})
    return result


def cpu_cores(samples):
    if len(samples) < 2 or samples[0]["identity"] != samples[-1]["identity"]:
        return None
    seconds = samples[-1]["unix_seconds"] - samples[0]["unix_seconds"]
    return ((samples[-1]["cpu_ticks"] - samples[0]["cpu_ticks"])
            / os.sysconf("SC_CLK_TCK") / seconds) if seconds > 0 else None


def heap_phase(points, phase, process_epoch, workload):
    begin = phase["unix_seconds"] - process_epoch
    duration = workload["idle" if phase["phase"] == "idle" else "seconds"]
    # /proc process start is tick-granular and profiler time starts shortly afterwards.
    # Exclude both boundaries so the next load cannot masquerade as idle retention.
    inside = [point for point in points if begin + .25 <= point["seconds"] <= begin + duration - .25]
    return {"cycle": phase["cycle"], "phase": phase["phase"], "approximate_start_seconds": begin,
            "samples": len(inside), "last": inside[-1] if inside else None}


def summarize_heap(root, report):
    source = root / report["source_run"] / "artifacts"
    result = json.loads((root / f"{report['source_run']}.result.json").read_text())
    group = "server" if result["workload"]["layer"] == "s3" else "driver"
    measured = next((sample[group] for sample in records(source / "samples.jsonl")
                     if any(process.get("executable") == result["manifest"]["binary"]
                            for process in sample[group]["processes"])), None)
    if measured is None:
        return {"run_id": report["source_run"], "unavailable": "no target clock alignment sample"}
    target = next(process for process in measured["processes"] if process.get("executable") == result["manifest"]["binary"])
    epoch = measured["unix_seconds"] - measured["monotonic"] + int(target["identity"]["start"]) / os.sysconf("SC_CLK_TCK")
    points = []
    path = root / report["id"] / "artifacts/allocations.massif"
    if path.stat().st_size > 16 * 1024**2:
        raise Unavailable("heap timeline input exceeds the bounded reduction limit")
    with path.open() as stream:
        for line in stream:
            if line.startswith("time="):
                stamp = float(line[5:])
            elif line.startswith("mem_heap_B="):
                points.append({"seconds": stamp, "bytes": int(line[11:])})
    return {"layer": result["workload"]["layer"], "run_id": report["source_run"], "decode_id": report["id"],
            "source_sha256": report["source_sha256"], "metrics_gaps": result.get("metrics_sampling_gaps", 0),
            "alignment": "Approximate process-start / proc monotonic alignment; exclude 250 ms at both edges. Timeline peaks are sampled, not exact heaptrack peak.",
            "phase_heap": [heap_phase(points, phase, epoch, result["workload"])
                           for phase in records(source / "driver.jsonl") if phase.get("kind") == "phase"],
            "timeline": points}


def device_summary(samples, group, device):
    available = [item[group] for item in samples if group in item]
    if len(available) < 2:
        return None

    def counters(item):
        for line in item["host"]["diskstats"].splitlines():
            fields = line.split()
            if fields[2] == device:
                return list(map(int, fields[3:]))
        return None

    first, last = counters(available[0]), counters(available[-1])
    if first is None or last is None:
        return None
    delta = [end - start for start, end in zip(first, last)]
    seconds = available[-1]["monotonic"] - available[0]["monotonic"]
    completed = delta[0] + delta[4]
    return {"device": device, "seconds": seconds,
            "first_counters": first, "last_counters": last,
            "read_mib_per_second": delta[2] / 2048 / seconds,
            "write_mib_per_second": delta[6] / 2048 / seconds,
            "read_write_await_ms": (delta[3] + delta[7]) / completed if completed else None,
            "busy_fraction": delta[9] / 1000 / seconds,
            "average_queue": delta[10] / 1000 / seconds,
            "scope": "whole sampled interval including idle; host-wide device includes unrelated work"}


def summarize_run(root, report, device):
    directory = root / report["id"] / "artifacts"
    samples = records(directory / "samples.jsonl")
    events = records(directory / "driver.jsonl")
    group = "server" if report["workload"]["layer"] == "s3" else "driver"
    measured = target_samples(samples, group, report["manifest"]["binary"])
    phases = [event for event in events if event.get("kind") == "phase"]
    cycles = report.get("cycles", [])
    result = {"id": report["id"], "status": report["status"], "reasons": report["reasons"],
              "profile": report["profile"], "workload": report["workload"],
              "manifest": report["manifest"], "server_configuration": report.get("server_configuration"),
              "cycles": cycles, "phase_observations": [],
              "device": device_summary(samples, group, device),
              "checkpoints": [event for event in events if event.get("kind") == "checkpoint"]}
    series, nonfinite = {}, 0
    for sample in samples:
        for line in sample.get("metrics", "").splitlines():
            if not line.startswith("cairn_"):
                continue
            name, value = line.rsplit(" ", 1)
            value = float(value)
            if not math.isfinite(value):
                nonfinite += 1
                continue
            entry = series.setdefault(name, {"first": value, "last": value, "max": value, "samples": 0})
            entry.update(last=value, max=max(entry["max"], value), samples=entry["samples"] + 1)
    result["server_metrics"] = series
    result["nonfinite_server_metric_samples"] = nonfinite
    result["metrics_sampling_gaps"] = report.get("metrics_sampling_gaps", 0)
    if cycles:
        result["median_transactions_per_second"] = statistics.median(cycle["transactions_per_second"] for cycle in cycles)
        result["successful_transactions"] = sum(cycle["successful_transactions"] for cycle in cycles)
    for phase in phases:
        begin = phase["unix_seconds"]
        duration = report["workload"]["seconds" if phase["phase"] == "load" else "idle"]
        points = [point for point in measured if begin <= point["unix_seconds"] < begin + duration]
        observation = {**phase, "sample_count": len(points), "cpu_cores": cpu_cores(points)}
        if points:
            observation["last_sample"] = points[-1]
            observation["peak_rss_kib"] = max(point["rss_kib"] for point in points)
        if group == "server":
            clients = target_samples(samples, "driver", os.path.realpath(os.sys.executable))
            observation["client_cpu_cores"] = cpu_cores([point for point in clients if begin <= point["unix_seconds"] < begin + duration])
        result["phase_observations"].append(observation)
    stages = {}
    writer_events = [event for event in events if event.get("kind") == "writer"]
    for event in writer_events:
        for name, values in event["stages_count_sum_max_errors"].items():
            aggregate = stages.setdefault(name, [0, 0, 0, 0])
            aggregate[0] += values[0]
            aggregate[1] += values[1]
            aggregate[2] = max(aggregate[2], values[2])
            aggregate[3] += values[3]
    result["writer_stage_count_sum_max_errors"] = stages
    result["writer_max_queue"] = max((event["queue_depth"] for event in writer_events), default=None)
    result["writer_dropped_samples"] = max((event["dropped_samples"] for event in writer_events), default=None)
    if samples:
        result["last_host_pressure"] = {key: value for key, value in samples[-1][group]["host"].items() if key.startswith("pressure/") or key == "loadavg"}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--device", required=True)
    args = parser.parse_args()
    campaign = Campaign(args.root)
    try:
        prior = list(campaign.ledger["runs"])
        token = campaign.admit("baseline", 30, 1024**3)
        start = time.monotonic()
        status = "FAIL"
        try:
            reduced, profiles = [], []
            for run in prior:
                if time.monotonic() - start > 25:
                    raise Unavailable("summary decoding allowance exhausted")
                source = campaign.root / f"{run['id']}.result.json"
                if not source.exists():
                    continue
                report = json.loads(source.read_text())
                if "workload" in report:
                    reduced.append(summarize_run(campaign.root, report, args.device))
                elif report.get("profile") == "heap" and report.get("decoding_complete"):
                    profiles.append(summarize_heap(campaign.root, report))
            output = campaign.root / f"{token}.summary.json"
            atomic_json(output, {"runs": reduced, "heap_profiles": profiles, "ledger_before_summary": campaign.ledger})
            status = "PASS"
            print(output)
        finally:
            campaign.finish(time.monotonic() - start, status, clean=True)
    finally:
        campaign.close()


if __name__ == "__main__":
    main()
