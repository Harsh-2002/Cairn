#!/usr/bin/env python3
"""Conditional paired Fjall/SQLite evidence under the existing metadata allowance."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import statistics
import time

from budget import Campaign, CLEANUP_HEADROOM, SPACE_LIMIT, Unavailable, atomic_json, footprint, remove_tree
from lab import RESULTS, clean_environment, manifest
from metadata_capacity import (FAMILIES, assess as assess_capacity, count, expected_state, finite,
                               final_process_cpu_ticks, process_sample, read_records, reserve_bytes,
                               validate_histogram, validate_writer)
from processes import Child, OUTPUT_LIMIT

SEED_ROWS = 100_000
PHASE_SECONDS = 5
PAIRS = (("sqlite", "fjall"), ("fjall", "sqlite"), ("sqlite", "fjall"),
         ("fjall", "sqlite"), ("sqlite", "fjall"))
CRITERIA = {"minimum_median_paired_throughput_gain": .20, "minimum_each_pair_throughput_gain": -.10,
            "maximum_control_drift": .10, "maximum_median_protected_ratio": 1.10,
            "p99_minimum_family_observations": 10_000, "minimum_family_outcomes_per_arm": 100}
SAMPLE_LIMIT = 16 * 1024**2
RUN_HEADROOM = 256 * 1024**2
FAMILY_COUNTS_PER_CYCLE = {name: 1 for name in FAMILIES}
FAMILY_COUNTS_PER_CYCLE.update(version_append=2, permanent_delete=4, multipart_reserve=2, journal_settle=11)


def validate_args(args):
    if (type(args.seed) is not int or not 0 <= args.seed < 2**64
            or type(args.allow_seconds) is not int or not 90 <= args.allow_seconds <= 600
            or not isinstance(args.commit, str) or not args.commit.strip()
            or not isinstance(args.build_settings, str) or not args.build_settings.strip()):
        raise ValueError("explicit build identity, u64 seed and 90..600 seconds required")


def prerequisite(path):
    content = Path(path).read_bytes()
    if len(content) > 8 * 1024**2:
        raise Unavailable("capacity prerequisite exceeds bound")
    source = json.loads(content)
    source = source.get("measurement", source)
    assessment = assess_capacity(source.get("arms", []))
    if (source.get("status") != "PASS" or source.get("cleaned_data_and_processes") is not True
            or assessment.get("status") != "PASS" or 1 not in assessment.get("qualifying_populations_buckets", [])):
        raise Unavailable("hot-bucket canonical Writer prerequisite does not qualify")
    return {"sha256": hashlib.sha256(content).hexdigest(), "measurement_id": source.get("id"),
            "revalidated_assessment": assessment}


def validate_observation(value, engine, *, load=False):
    if not isinstance(value, dict) or value.get("stages_complete") is not True:
        raise Unavailable("incomplete Writer observations")
    writer = value.get("writer")
    if engine == "sqlite":
        validate_writer(writer, expected_apply_rejections=load)
    else:
        if not isinstance(writer, dict):
            raise Unavailable("candidate Writer counters missing")
        for key in ("admission_ns", "queue_ns", "begin_ns", "apply_ns", "commit_ns", "mutations", "batches", "rejected", "writer_tid"):
            count(writer.get(key), key)
        if not writer["writer_tid"] or writer["rejected"] > writer["mutations"] or (not load and writer["rejected"]):
            raise Unavailable("invalid candidate Writer identity/outcomes")
        for key in ("engine_maxima", "engine_final"):
            counters = value.get(key)
            if not isinstance(counters, dict):
                raise Unavailable("candidate resource observations missing")
            for counter in ("cache_capacity_bytes", "cache_resident_bytes", "write_buffer_bytes", "sealed_memtables", "journal_bytes", "journal_count",
                            "live_tree_bytes", "live_tables", "level_zero_tables", "outstanding_flushes", "active_compactions", "completed_compactions", "compaction_seconds"):
                # Very short verification may complete before the first interval tick.
                if key == "engine_maxima" and not value.get("engine_samples"):
                    continue
                finite(counters.get(counter), counter)


def validate_records(records, config):
    if not isinstance(records, list) or not 3 <= len(records) <= 7:
        raise Unavailable("missing or excess comparison records")
    wanted = ["opened"] + (["prepared"] if config["action"] == "prepare" else []) + ["verified_before"]
    if config["action"] == "run":
        wanted += ["phase", "verified_after"]
    wanted += ["result"]
    if [row.get("event") for row in records] != wanted or records[0].get("config") != config:
        raise Unavailable("comparison event order/config changed")
    expected = expected_state(1)
    # Tiny correctness fixtures use the same exact distribution, never an actual capacity result.
    if config["seed_rows"] != 100_000:
        expected.update(seed_rows=config["seed_rows"], current_data_rows=config["seed_rows"] * 8 // 10,
                        historical_data_rows=config["seed_rows"] // 10, current_delete_markers=config["seed_rows"] // 10,
                        seed_logical_bytes=config["seed_rows"] * 9 // 10 * 128)
    for record in records:
        event = record["event"]
        if event in ("prepared", "verified_before", "verified_after", "result") and record.get("expected") != expected:
            raise Unavailable("comparison seed distribution changed")
        if event in ("prepared", "verified_before", "verified_after"):
            validate_observation(record.get("observation"), config["engine"])
        if event.startswith("verified") and record.get("quota_verified") is not True:
            raise Unavailable("independent comparison quota/generation verification missing")
    result = records[-1]
    if result.get("status") != "complete" or result.get("close_complete") is not True:
        raise Unavailable("comparison close incomplete")
    for field in ("open_seconds", "checkpoint_seconds", "close_seconds", "total_seconds"):
        finite(result.get(field), field, positive=True)
    for field in ("read_bytes", "write_bytes", "cancelled_write_bytes"):
        count(result.get("process_io_after_close", {}).get(field), "closed process I/O " + field)
    disk = result.get("disk_after_close", {})
    for field in ("files", "logical_bytes", "allocated_bytes", "physical_table_bytes"):
        count(disk.get(field), field)
    if not disk["files"] or not disk["logical_bytes"] or not disk["allocated_bytes"]:
        raise Unavailable("closed database footprint absent")
    if config["action"] != "run":
        if result.get("phase") is not None:
            raise Unavailable("preparation/verification was mislabeled as load")
        return result
    phase = result.get("phase", {})
    if phase != records[2].get("report") or phase.get("concurrency") != 128:
        raise Unavailable("load phase report inconsistent")
    seconds = finite(phase.get("seconds"), "phase wall", positive=True)
    bundles = count(phase.get("completed_operation_bundles"), "completed bundles")
    rate = finite(phase.get("bundles_per_second"), "bundle rate", positive=True)
    if seconds < config["phase_seconds"] or bundles < 640 or bundles % 5 or not math.isclose(rate, bundles / seconds, rel_tol=1e-6):
        raise Unavailable("comparison did not complete full admitted cycles")
    if finite(phase.get("writer_cpu_seconds"), "Writer CPU") > seconds * 1.1:
        raise Unavailable("impossible Writer CPU")
    validate_observation(phase.get("observation"), config["engine"], load=True)
    if config["engine"] == "fjall":
        stages = phase["observation"]["writer"]
        if (sum(stages[name] for name in ("begin_ns", "apply_ns", "commit_ns")) / 1e9 > seconds * 1.05
                or not stages["batches"] or stages["mutations"] < bundles or stages["rejected"] != bundles // 5):
            raise Unavailable("candidate service counters disagree with the admitted workload")
    families = phase.get("families", {})
    if set(families) != FAMILIES:
        raise Unavailable("comparison family coverage incomplete")
    for family, values in families.items():
        successful = validate_histogram(values.get("successful"), family)
        rejected = validate_histogram(values.get("expected_rejected"), family + " rejection")
        if successful + rejected != bundles // 5 * FAMILY_COUNTS_PER_CYCLE[family]:
            raise Unavailable("comparison changed the exact five-bundle outcome distribution")
        if family == "conditional_reject":
            if successful or rejected < CRITERIA["minimum_family_outcomes_per_arm"]:
                raise Unavailable("missing/mislabeled conditional rejection")
        elif rejected or successful < CRITERIA["minimum_family_outcomes_per_arm"]:
            raise Unavailable("missing/mislabeled successful family")
    return result


def histogram(arm, family):
    kind = "expected_rejected" if family == "conditional_reject" else "successful"
    return arm["result"]["phase"]["families"][family][kind]


def assess(arms):
    loads = [arm for arm in arms if arm["config"]["action"] == "run"]
    expected_order = [engine for pair in PAIRS for engine in pair]
    if len(loads) != 10 or [arm["config"]["engine"] for arm in loads] != expected_order:
        raise Unavailable("five ordered A/B/B/A pairs are required")
    if [(arm["config"]["action"], arm["config"]["engine"]) for arm in arms] != [
        ("prepare", "sqlite"), ("prepare", "fjall"), *[("run", engine) for engine in expected_order], ("verify", "sqlite"), ("verify", "fjall")]:
        raise Unavailable("preparation or final fresh-process verification incomplete")
    for arm in arms:
        result = validate_records(arm["records"], arm["config"])
        if result != arm.get("result"):
            raise Unavailable("arm result disagrees with retained records")
        if count(arm.get("process_sample_count"), "process samples") < 2:
            raise Unavailable("missing process observations")
        for field in ("peak_rss_kib", "peak_fds", "process_cpu_seconds"):
            finite(arm.get(field), field, positive=True)
    reasons, pairs = [], []
    for index in range(5):
        pair = {arm["config"]["engine"]: arm for arm in loads[index * 2:index * 2 + 2]}
        baseline, candidate = pair["sqlite"], pair["fjall"]
        gain = candidate["result"]["phase"]["bundles_per_second"] / baseline["result"]["phase"]["bundles_per_second"] - 1
        protected = {"peak_rss_kib": candidate["peak_rss_kib"] / baseline["peak_rss_kib"]}
        resources = {field: candidate[field] / baseline[field] for field in ("peak_fds", "process_cpu_seconds")}
        resources["process_cpu_per_bundle"] = (candidate["process_cpu_seconds"] / candidate["result"]["phase"]["completed_operation_bundles"]) / (baseline["process_cpu_seconds"] / baseline["result"]["phase"]["completed_operation_bundles"])
        for field in ("open_seconds", "checkpoint_seconds", "close_seconds"):
            protected[field] = candidate["result"][field] / baseline["result"][field]
        resources["allocated_database_bytes"] = candidate["result"]["disk_after_close"]["allocated_bytes"] / baseline["result"]["disk_after_close"]["allocated_bytes"]
        for family in sorted(FAMILIES):
            old, new = histogram(baseline, family), histogram(candidate, family)
            protected[f"{family}.mean"] = (new["sum_seconds"] / new["count"]) / (old["sum_seconds"] / old["count"])
            protected[f"{family}.observed_max"] = new["max_seconds"] / old["max_seconds"]
        if gain < CRITERIA["minimum_each_pair_throughput_gain"]:
            reasons.append(f"pair {index + 1}: candidate throughput regressed more than 10%")
        pairs.append({"pair": index + 1, "order": list(PAIRS[index]), "throughput_gain": gain,
                      "protected_ratios": protected, "resource_ratios": resources})
    gain = statistics.median(pair["throughput_gain"] for pair in pairs)
    if gain < CRITERIA["minimum_median_paired_throughput_gain"]:
        reasons.append("median paired throughput gain is below 20%")
    drift = {}
    for engine in ("sqlite", "fjall"):
        rates = [arm["result"]["phase"]["bundles_per_second"] for arm in loads if arm["config"]["engine"] == engine]
        drift[engine] = max(rates) / min(rates) - 1
        if drift[engine] > CRITERIA["maximum_control_drift"]:
            reasons.append(f"{engine}: repeated C128 controls drift more than 10%")
    medians = {key: statistics.median(pair["protected_ratios"][key] for pair in pairs) for key in pairs[0]["protected_ratios"]}
    regressions = {key: ratio for key, ratio in medians.items() if ratio > CRITERIA["maximum_median_protected_ratio"]}
    if regressions:
        reasons.append("protected paired medians exceed the predeclared 10% limit")
    qualified = not reasons
    status = "INCONCLUSIVE" if any(value > CRITERIA["maximum_control_drift"] for value in drift.values()) else "PASS"
    return {"status": status, "protected_median_ratios": medians, "protected_regressions": regressions, "candidate_qualified": qualified, "pairs": pairs,
            "median_throughput_gain": gain, "control_drift": drift, "qualification_reasons": reasons,
            "decision": "KEEP SQLite; isolated candidate qualifies for a separate migration/restore proposal only" if qualified else
                        "KEEP SQLite; conditional candidate does not meet the predeclared gain and protection gates"}


def process_io(pid):
    result = {}
    try:
        content = Path(f"/proc/{pid}/io").read_text()
    except (PermissionError, FileNotFoundError, ProcessLookupError):
        # Linux may revoke access across exec or after exit. The driver reports its own
        # complete I/O after joining engine workers; an unavailable sample is not zero I/O.
        return None
    for line in content.splitlines():
        key, value = line.split(":", 1)
        result[key] = int(value)
    return result


def run_comparison(args, campaign):
    validate_args(args)
    prior = sum(run.get("elapsed_seconds", run["reserved_seconds"]) for run in campaign.ledger["runs"] if run["phase"] == "metadata")
    if prior + args.allow_seconds > 600:
        raise Unavailable("conditional comparison cannot exceed the cumulative 600-second metadata allowance")
    trigger = prerequisite(args.capacity_result)
    reserved = reserve_bytes()
    token = campaign.admit("metadata", args.allow_seconds, reserved)
    start, observer_start = time.monotonic(), time.process_time()
    deadline, work_deadline = start + args.allow_seconds, start + args.allow_seconds - 15
    directory = campaign.root / token
    children, arms, reasons = [], [], []
    status, clean = "INCONCLUSIVE", False
    report = {"id": token, "kind": "metadata_alternative", "prerequisite": trigger, "criteria": CRITERIA,
              "pairs_order": [list(pair) for pair in PAIRS], "seed_rows": SEED_ROWS, "phase_seconds": PHASE_SECONDS,
              "arms": arms, "decision": "KEEP SQLite; comparison incomplete", "candidate_qualified": False,
              "scope": "one hot bucket, C128, identical metadata-only trace; all actor/engine workers counted; no physical objects",
              "cache_state": "each arm uses a fresh process and full seed verification; filesystem cache is uncontrolled",
              "resource_scope": "50ms complete-process samples and Linux I/O counters; includes generator and engine workers; all database files counted",
              "unproven": ["production migration", "backup/restore interchange", "physical recovery", "power-loss behavior", "full S3 semantics outside the fixed trace"]}
    try:
        directory.mkdir(mode=0o700)
        artifacts, data = directory / "artifacts", directory / "data"
        artifacts.mkdir(mode=0o700)
        data.mkdir(mode=0o700)
        (artifacts / "scratch").mkdir(mode=0o700)
        binary = Path(args.binary).resolve(strict=True)
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise ValueError("explicit prebuilt executable required")
        environment = {**clean_environment(), "TMPDIR": str(artifacts / "scratch")}

        def check_budget():
            if time.monotonic() >= work_deadline:
                raise Unavailable("metadata comparison allowance reached; preserving cleanup time")
            used = footprint(campaign.root)
            campaign.ledger["peak_bytes"] = max(campaign.ledger["peak_bytes"], used)
            if used > campaign.ledger["active"]["initial_bytes"] + reserved - RUN_HEADROOM or used >= SPACE_LIMIT - CLEANUP_HEADROOM:
                raise Unavailable("metadata comparison disk headroom reached")
            if any(child.output_error or child.output_overflow for child in children):
                raise Unavailable("bounded comparison output incomplete")

        def launch(command, label):
            check_budget()
            child = Child(command, environment, artifacts, label, campaign)
            children.append(child)
            return child

        report["manifest"] = manifest(args, binary, launch)
        source = Path(__file__).resolve().parent
        paths = [source / "src/metadata_comparison.rs", source / "src/metadata_run.rs", source / "src/lib.rs",
                 *sorted((source / "src/metadata_capacity").glob("*.rs")), *sorted((source / "src/metadata_fjall").glob("*.rs"))]
        report["manifest"]["comparison_sources_sha256"] = {str(path.relative_to(source)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
        ticks = os.sysconf("SC_CLK_TCK")
        report["ticks_per_second"] = ticks
        jobs = [("prepare", "sqlite"), ("prepare", "fjall"), *[("run", engine) for pair in PAIRS for engine in pair], ("verify", "sqlite"), ("verify", "fjall")]
        for index, (action, engine) in enumerate(jobs):
            check_budget()
            remaining = work_deadline - time.monotonic()
            if action == "prepare":
                seconds = math.floor((remaining - (10 * (PHASE_SECONDS + 2) + 12)) / (2 - index))
            else:
                seconds = math.floor(remaining - (len(jobs) - index - 1) * 2)
            if seconds < (PHASE_SECONDS + 4 if action == "run" else 5):
                raise Unavailable("insufficient reserved time for remaining populated comparison arms")
            config = {"root": str(data / engine), "engine": engine, "action": action, "seed_rows": SEED_ROWS,
                      "seed": args.seed, "phase_seconds": PHASE_SECONDS, "deadline_seconds": min(seconds, 600), "ticks_per_second": ticks}
            label = f"{index:02}-{action}-{engine}"
            config_path = artifacts / f"{label}.config.json"
            atomic_json(config_path, config)
            began = time.monotonic()
            driver = launch([str(binary), str(config_path)], label)
            samples, peak_rss, peak_fds, missing_fds, missing_io, final_ticks = 0, 0, 0, 0, 0, 0
            io_maxima = {}
            with (artifacts / f"{label}.samples.jsonl").open("w") as stream:
                while True:
                    check_budget()
                    sample = process_sample(driver.process.pid)
                    sample["io"] = process_io(driver.process.pid)
                    if sample["io"] is None:
                        missing_io += 1
                    else:
                        for key, value in sample["io"].items():
                            io_maxima[key] = max(io_maxima.get(key, 0), value)
                    stream.write(json.dumps(sample) + "\n")
                    stream.flush()
                    if stream.tell() > SAMPLE_LIMIT // len(jobs):
                        raise Unavailable("comparison process sample bound exceeded")
                    samples += 1
                    peak_rss = max(peak_rss, sample["rss_kib"])
                    if sample["fds"] is None:
                        missing_fds += 1
                    else:
                        peak_fds = max(peak_fds, sample["fds"])
                    final_ticks = sample["process_cpu_ticks"]
                    if driver.exited() is not None:
                        final_ticks = final_process_cpu_ticks(driver.process.pid)
                        terminal_io = process_io(driver.process.pid)
                        if terminal_io is not None:
                            for key, value in terminal_io.items():
                                io_maxima[key] = max(io_maxima.get(key, 0), value)
                        stream.write(json.dumps({"event": "process_exit", "process_cpu_ticks": final_ticks, "io": terminal_io}) + "\n")
                        break
                    time.sleep(.05)
            outcome = driver.exited()
            driver.stop(min(deadline - 2, time.monotonic() + 4))
            records = read_records(driver.paths[0])
            arm = {"config": config, "records": records, "coordinator_wall_seconds": time.monotonic() - began,
                   "process_sample_count": samples, "peak_rss_kib": peak_rss, "peak_fds": peak_fds,
                   "process_cpu_seconds": final_ticks / ticks, "process_io_maxima": io_maxima, "fd_samples_unavailable": missing_fds, "io_samples_unavailable": missing_io}
            arms.append(arm)
            if outcome != 0:
                raise Unavailable(f"comparison arm {label} failed: {records[-1] if records else 'no complete output'}")
            arm["result"] = validate_records(records, config)
        assessment = assess(arms)
        report.update(assessment)
        status = assessment["status"]
        if status == "INCONCLUSIVE":
            reasons.extend(assessment["qualification_reasons"])
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
                reasons.append("comparison output incomplete after final drain")
            for path in (directory / "data", directory / "artifacts/scratch"):
                if path.exists():
                    remove_tree(path, deadline - 1)
            clean = True
        except Exception as error:
            status = "INCONCLUSIVE"
            reasons.append(f"comparison cleanup incomplete: {error}")
        elapsed = time.monotonic() - start
        charged = elapsed + 1
        if charged > args.allow_seconds:
            status = "INCONCLUSIVE"
            reasons.append("comparison exceeded reservation; actual elapsed time is charged")
        if status != "PASS":
            report.update(candidate_qualified=False, decision="KEEP SQLite; conditional comparison did not qualify")
        report.update(status=status, reasons=reasons, elapsed_seconds=elapsed, charged_seconds=charged,
                      cleaned_data_and_processes=clean, observer_process_cpu_seconds=time.process_time() - observer_start)
        atomic_json(campaign.root / f"{token}.result.json", report)
        campaign.finish(max(charged, time.monotonic() - start), status, clean=clean)
    print(json.dumps({"status": status, "result": str(campaign.root / f"{token}.result.json"), "reasons": reasons,
                      "candidate_qualified": report["candidate_qualified"], "spent_seconds": campaign.ledger["spent_seconds"],
                      "peak_bytes": campaign.ledger["peak_bytes"]}), flush=True)
    return RESULTS[status]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("root", "binary", "commit", "build-settings", "capacity-result"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--seed", type=int, default=0x5eed)
    parser.add_argument("--allow-seconds", type=int, required=True)
    args = parser.parse_args()
    campaign = None
    try:
        validate_args(args)
        campaign = Campaign(args.root)
        return run_comparison(args, campaign)
    except KeyboardInterrupt:
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
    signal.signal(signal.SIGINT, interrupted)
    raise SystemExit(main())
