#!/usr/bin/env python3
"""Bounded populated canonical-Writer comparison; never a production replacement decision."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import time

from budget import Campaign, CLEANUP_HEADROOM, SPACE_LIMIT, Unavailable, atomic_json, footprint, remove_tree
from lab import RESULTS, clean_environment, manifest
from processes import Child, OUTPUT_LIMIT

MIB = 1024**2
SEED_ROWS = 100_000
PHASE_SECONDS = 10
MATRIX = ((1, (4, 32, 128, 32, 4)), (16, (32, 128, 32)))
SAMPLE_LIMIT = 16 * MIB
RUN_HEADROOM = 256 * MIB
FAMILIES = frozenset(("version_append", "conditional_accept", "conditional_reject", "permanent_delete",
                     "marker_insert", "marker_remove", "multipart_reserve", "multipart_publish",
                     "multipart_replace", "multipart_abort", "current_read", "version_read", "prefix_list",
                     "delimiter_list", "version_list", "journal_settle", "outbox_publish", "outbox_claim_settle"))
CRITERIA = {"queue_nonempty_fraction_at_least": .8, "serialized_occupancy_fraction_at_least": .8,
            "c128_throughput_gain_over_c32_at_most": .2, "repeated_control_drift_at_most": .1,
            "minimum_queue_samples": 20, "p99_minimum_successful_family_observations": 10_000}


def validate_args(args):
    if (type(args.seed) is not int or not 0 <= args.seed < 2**64
            or type(args.allow_seconds) is not int or not 90 <= args.allow_seconds <= 600
            or not isinstance(args.commit, str) or not args.commit.strip()
            or not isinstance(args.build_settings, str) or not args.build_settings.strip()):
        raise ValueError("u64 seed, explicit commit/build settings and 90..600 seconds required")


def reserve_bytes():
    # Both full populations, all indexes/rollups and transient worker state are reserved even
    # though the driver runs sequentially. 32 KiB/seed row deliberately exceeds actual metadata.
    return 2 * SEED_ROWS * 32_768 + 2 * 256 * MIB + 20 * OUTPUT_LIMIT + SAMPLE_LIMIT + RUN_HEADROOM


def finite(value, name, *, positive=False):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0 or (positive and value == 0):
        raise Unavailable(f"missing or invalid {name}")
    return value


def count(value, name):
    if type(value) is not int or value < 0:
        raise Unavailable(f"missing or invalid {name}")
    return value


def expected_state(buckets):
    return {"seed_rows": SEED_ROWS, "current_data_rows": 80_000, "historical_data_rows": 10_000,
            "current_delete_markers": 10_000, "seed_logical_bytes": 90_000 * 128,
            "auxiliary_sessions": buckets, "auxiliary_parts": buckets, "auxiliary_part_bytes": buckets * 64}


def validate_writer(writer, *, expected_apply_rejections=False):
    if not isinstance(writer, dict):
        raise Unavailable("missing Writer observation")
    for key in ("dropped_at_start", "dropped_at_end"):
        if count(writer.get(key), key) != 0:
            raise Unavailable("dropped Writer stage observation prevents service-demand attribution")
    samples = count(writer.get("queue_samples"), "queue samples")
    nonempty = count(writer.get("queue_nonempty_samples"), "nonempty queue samples")
    if nonempty > samples:
        raise Unavailable("impossible Writer queue counts")
    count(writer.get("queue_max"), "queue maximum")
    count(writer.get("peak_wal_bytes"), "peak WAL bytes")
    stages = writer.get("stages")
    if not isinstance(stages, dict):
        raise Unavailable("missing Writer stage map")
    for name, stage in stages.items():
        if not isinstance(stage, dict):
            raise Unavailable("invalid Writer stage observation")
        count(stage.get("count"), f"{name} count")
        if count(stage.get("failed"), f"{name} failed") != 0:
            # Expected conditional rejection is visible in caller outcomes; some stage collectors
            # label its apply span unsuccessful. That must be disclosed, not confused with SQL I/O.
            if name != "apply" or not expected_apply_rejections:
                raise Unavailable(f"unsuccessful Writer {name} stage")
        total = finite(stage.get("sum_seconds"), f"{name} sum")
        maximum = finite(stage.get("max_seconds"), f"{name} maximum")
        if maximum > total + 1e-9:
            raise Unavailable("Writer maximum exceeds stage total")
    return writer


def validate_histogram(histogram, name):
    if not isinstance(histogram, dict):
        raise Unavailable(f"missing {name} histogram")
    observations = count(histogram.get("count"), f"{name} count")
    total = finite(histogram.get("sum_seconds"), f"{name} latency sum")
    maximum = finite(histogram.get("max_seconds"), f"{name} latency maximum")
    width = finite(histogram.get("bucket_relative_width"), f"{name} histogram width", positive=True)
    overflow = count(histogram.get("overflow"), f"{name} overflow")
    if maximum > total + 1e-9 or width > .020001 or overflow:
        raise Unavailable(f"invalid or overflowing {name} latency distribution")
    interval = histogram.get("p99_seconds_interval")
    if observations < 10_000:
        if interval is not None:
            raise Unavailable("p99 claimed with fewer than 10000 family observations")
    elif (not isinstance(interval, list) or len(interval) != 2
          or finite(interval[0], "p99 lower") > finite(interval[1], "p99 upper")):
        raise Unavailable("missing bounded p99 interval for sufficiently sampled family")
    return observations


def validate_quota(quota, buckets):
    if not isinstance(quota, dict):
        raise Unavailable("missing independent multipart quota verification")
    if quota.get("user_logical_bytes") != 90_000 * 128:
        raise Unavailable("user logical quota counter differs from exact seed")
    for key in ("multipart_bucket_stats", "multipart_principal_stats"):
        if (quota.get(key) != {"active_uploads": buckets, "staged_bytes": buckets * 64}
                or any(type(value) is not int for value in quota.get(key, {}).values())):
            raise Unavailable("multipart session/part quota counters differ from exact fixture")


def validate_phase(phase, index, concurrency):
    if not isinstance(phase, dict) or phase.get("phase") != index or phase.get("concurrency") != concurrency:
        raise Unavailable("missing, reordered or wrong-concurrency phase")
    elapsed = finite(phase.get("seconds"), "phase duration", positive=True)
    if elapsed < PHASE_SECONDS:
        raise Unavailable("phase did not reach its predeclared duration")
    bundles = count(phase.get("completed_operation_bundles"), "completed operation bundles")
    if not bundles or bundles % 5:
        raise Unavailable("phase did not finish each admitted five-bundle workload cycle")
    count(phase.get("cleanup_claims_lost"), "exact cleanup claims invalidated by concurrent quota linkage")
    throughput = finite(phase.get("bundles_per_second"), "request-bundle throughput", positive=True)
    if not math.isclose(throughput, bundles / elapsed, rel_tol=1e-6):
        raise Unavailable("throughput does not match acknowledged operation count")
    cpu = finite(phase.get("writer_cpu_seconds"), "actual Writer thread CPU")
    occupancy = finite(phase.get("serialized_observed_seconds"), "serialized observed occupancy")
    if cpu > elapsed * 1.1 or occupancy > elapsed * 1.05:
        raise Unavailable("impossible single-Writer CPU or serialized occupancy")
    if phase.get("stage_samples_complete") is not True:
        raise Unavailable("Writer stage observations incomplete")
    writer = validate_writer(phase.get("writer"), expected_apply_rejections=True)
    if writer["queue_samples"] < CRITERIA["minimum_queue_samples"]:
        raise Unavailable("insufficient queue observations")
    serialized = 0
    for name in ("begin", "apply", "commit"):
        stage = writer["stages"].get(name)
        if not stage or not stage["count"]:
            raise Unavailable("missing serialized Writer stage")
        serialized += stage["sum_seconds"]
    if not math.isclose(serialized, occupancy, abs_tol=1e-9, rel_tol=1e-6):
        raise Unavailable("queue/admission time was mixed into serialized Writer occupancy")
    families = phase.get("families")
    if not isinstance(families, dict) or set(families) != FAMILIES:
        raise Unavailable("operation-family outcomes do not cover the declared mix")
    for family, values in families.items():
        if not isinstance(values, dict):
            raise Unavailable("invalid operation-family outcome histogram")
        successful = validate_histogram(values.get("successful"), family)
        rejected = validate_histogram(values.get("expected_rejected"), family + " expected rejection")
        if family == "conditional_reject":
            if successful or not rejected:
                raise Unavailable("conditional rejection outcome mislabeled")
        elif not successful or rejected:
            raise Unavailable("missing successful family or unexpected rejection")
    if writer["stages"]["apply"]["failed"] > families["conditional_reject"]["expected_rejected"]["count"]:
        raise Unavailable("unsuccessful Writer apply batches exceed expected conditional rejections")
    return phase


def validate_records(records, config):
    if (not isinstance(config, dict) or type(config.get("seed_rows")) is not int
            or config["seed_rows"] != SEED_ROWS or config.get("phase_seconds") != PHASE_SECONDS
            or config.get("buckets") not in (1, 16)
            or type(config.get("ticks_per_second")) is not int or not 1 <= config["ticks_per_second"] <= 100_000):
        raise Unavailable("configuration does not declare the fixed populated metadata experiment")
    if not isinstance(records, list) or len(records) != 2 * len(config["concurrency"]) + 4:
        raise Unavailable("expected exact start/preparation/verification/phase/completion stream")
    events = ["start", "prepared", "seed_verified"] + [event for _ in config["concurrency"] for event in ("phase", "phase_verified")] + ["result"]
    if any(not isinstance(record, dict) or record.get("event") != event for record, event in zip(records, events)):
        raise Unavailable("driver events incomplete, duplicated or reordered")
    start, prepared, seed_verified = records[:3]
    if (start.get("schema") != 1 or start.get("variant") != "canonical_writer_full_populated_v1"
            or start.get("config") != config
            or start.get("cache") != {"read_connections": 8, "kib_per_connection": 8192,
                                      "mmap_bytes": 0, "application_cache": "absent"}):
        raise Unavailable("driver configuration/cache posture differs from declared canonical workload")
    expected = expected_state(config["buckets"])
    if (prepared.get("expected") != expected
            or any(type(value) is not int for value in prepared.get("expected", {}).values())):
        raise Unavailable("100000-row current/history/marker seed was not prepared exactly")
    finite(prepared.get("seconds"), "seed preparation duration", positive=True)
    validate_writer(prepared.get("writer"))
    validate_writer(seed_verified.get("writer"))
    validate_quota(seed_verified.get("quota"), config["buckets"])
    phases = []
    for index, concurrency in enumerate(config["concurrency"]):
        phases.append(validate_phase(records[3 + index * 2].get("report"), index, concurrency))
        verified = records[4 + index * 2]
        if verified.get("phase") != index:
            raise Unavailable("phase verification identity mismatch")
        validate_writer(verified.get("writer"))
        validate_quota(verified.get("quota"), config["buckets"])
    result = records[-1]
    if (result.get("status") != "complete" or result.get("expected") != expected
            or result.get("phases") != phases or result.get("physical_objects") != "not_created_metadata_only"):
        raise Unavailable("driver completion does not match verified populated phases")
    for key in ("checkpoint_seconds", "reopen_seconds", "total_seconds"):
        finite(result.get(key), key)
    count(result.get("post_checkpoint_wal_bytes"), "post-checkpoint WAL")
    validate_writer(result.get("reopen_writer"))
    validate_quota(result.get("reopen_quota"), config["buckets"])
    for name in ("before_checkpoint", "final_database"):
        sizes = result.get(name)
        if not isinstance(sizes, dict):
            raise Unavailable("missing database size/checkpoint/reopen evidence")
        for field in ("sqlite_version", "sqlite_source_id"):
            if not isinstance(sizes.get(field), str) or not sizes[field].strip():
                raise Unavailable("runtime SQLite provenance missing")
        for field in ("database_file_bytes", "page_size", "page_count", "freelist_pages"):
            count(sizes.get(field), field)
        if not sizes["database_file_bytes"] or not sizes["page_count"] or sizes["freelist_pages"] > sizes["page_count"]:
            raise Unavailable("invalid database page/size evidence")
        dbstat = sizes.get("dbstat")
        if not isinstance(dbstat, dict) or type(dbstat.get("available")) is not bool:
            raise Unavailable("dbstat availability was not recorded")
        if dbstat["available"]:
            if not isinstance(dbstat.get("bytes_by_kind"), dict):
                raise Unavailable("dbstat byte evidence absent")
            for value in dbstat["bytes_by_kind"].values():
                count(value, "dbstat bytes")
        elif not isinstance(dbstat.get("reason"), str) or not dbstat["reason"]:
            raise Unavailable("dbstat unavailable without reason")
    return result


def assess(arms):
    decision = {"status": "INCONCLUSIVE", "writer_limit": "NO demonstrated Writer limit",
                "decision": "KEEP SQLite; alternative experiment remains conditional",
                "criteria": CRITERIA, "comparisons": [], "reasons": []}
    try:
        if len(arms) != 2:
            raise Unavailable("both one-bucket and sixteen-bucket populations are required")
        for arm, (buckets, concurrency) in zip(arms, MATRIX):
            config = arm.get("config", {})
            if config.get("buckets") != buckets or config.get("concurrency") != list(concurrency):
                raise Unavailable("controlled population/concurrency matrix differs")
            result = validate_records(arm.get("records"), config)
            if count(arm.get("process_sample_count"), "process sample count") < 2:
                raise Unavailable("insufficient process attribution observations")
            for field in ("process_cpu_seconds", "peak_rss_kib", "peak_fds"):
                finite(arm.get(field), field, positive=True)
            phases = result["phases"]
            controls = {}
            for concurrency_level in (4, 32):
                repeated = [p["bundles_per_second"] for p in phases if p["concurrency"] == concurrency_level]
                if len(repeated) > 1:
                    controls[str(concurrency_level)] = max(repeated) / min(repeated) - 1
            if "32" not in controls or (buckets == 1 and "4" not in controls):
                raise Unavailable("required repeated concurrency controls absent")
            c32 = sum(p["bundles_per_second"] for p in phases if p["concurrency"] == 32) / sum(p["concurrency"] == 32 for p in phases)
            c128 = sum(p["bundles_per_second"] for p in phases if p["concurrency"] == 128) / sum(p["concurrency"] == 128 for p in phases)
            high = [p for p in phases if p["concurrency"] == 128]
            queue = min(p["writer"]["queue_nonempty_samples"] / p["writer"]["queue_samples"] for p in high)
            occupancy = min(p["serialized_observed_seconds"] / p["seconds"] for p in high)
            comparison = {"buckets": buckets, "control_drift_fraction_by_concurrency": controls,
                          "c128_throughput_gain_over_c32": c128 / c32 - 1,
                          "minimum_c128_queue_nonempty_fraction": queue,
                          "minimum_c128_serialized_occupancy_fraction": occupancy,
                          "c128_writer_cpu_seconds": [p["writer_cpu_seconds"] for p in high]}
            decision["comparisons"].append(comparison)
            if any(drift > CRITERIA["repeated_control_drift_at_most"] for drift in controls.values()):
                raise Unavailable("repeated concurrency control drift exceeds ten percent")
        decision["status"] = "PASS"
        qualifying = [c["buckets"] for c in decision["comparisons"]
                      if c["minimum_c128_queue_nonempty_fraction"] >= .8
                      and c["minimum_c128_serialized_occupancy_fraction"] >= .8
                      and c["c128_throughput_gain_over_c32"] <= .2]
        decision["qualifying_populations_buckets"] = qualifying
        if qualifying:
            decision["writer_limit"] = f"Observed Writer service meets the predeclared limiting criterion for bucket populations {qualifying}"
            decision["decision"] = f"KEEP SQLite; transactional Fjall experiment may be evaluated only for qualifying bucket populations {qualifying} under full semantic parity"
        else:
            decision["reasons"].append("complete measurements do not meet every predeclared Writer-limiting condition")
    except (Unavailable, KeyError, TypeError, ZeroDivisionError) as error:
        decision["reasons"].append(str(error))
    return decision


def read_records(path):
    if path.stat().st_size > OUTPUT_LIMIT:
        raise Unavailable("driver output exceeds cap")
    try:
        records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    except (OSError, ValueError) as error:
        raise Unavailable("malformed driver output") from error
    if len(records) > 16:
        raise Unavailable("driver emitted excess records")
    return records


def process_sample(pid):
    # Driver has one owned process. Read group CPU from its leader stat and RSS/FDs without the
    # large host diskstats/smaps snapshots used by other phases; observer CPU is separate.
    root = Path(f"/proc/{pid}")
    stat = (root / "stat").read_text()
    fields = stat[stat.rindex(")") + 2:].split()
    status = dict(line.split(":", 1) for line in (root / "status").read_text().splitlines() if ":" in line)
    rss = status.get("VmRSS", "0 kB").split()
    try:
        fds = len(list((root / "fd").iterdir()))
    except (PermissionError, FileNotFoundError):
        # An unreaped zombie retains final CPU ticks but no live descriptor namespace.
        current = (root / "stat").read_text()
        fields = current[current.rindex(")") + 2:].split()
        fds = 0 if fields[0] == "Z" else None
    return {"monotonic": time.monotonic(), "process_cpu_ticks": int(fields[11]) + int(fields[12]),
            "rss_kib": int(rss[0]), "fds": fds, "state": fields[0],
            "observer_cpu_seconds": time.process_time()}


def run_comparison(args, campaign):
    validate_args(args)
    prior = sum(run.get("elapsed_seconds", run["reserved_seconds"]) for run in campaign.ledger["runs"] if run["phase"] == "metadata")
    if prior + args.allow_seconds > 600:
        raise Unavailable("600-second metadata allowance includes prior metadata work")
    reserved = reserve_bytes()
    token = campaign.admit("metadata", args.allow_seconds, reserved)
    start, observer_start = time.monotonic(), time.process_time()
    deadline, work_deadline = start + args.allow_seconds, start + args.allow_seconds - 15
    directory = campaign.root / token
    children, reasons, arms = [], [], []
    status, clean = "INCONCLUSIVE", False
    report = {"id": token, "kind": "metadata", "matrix": [{"buckets": b, "concurrency": list(c)} for b, c in MATRIX],
              "seed_rows_per_population": SEED_ROWS, "phase_seconds": PHASE_SECONDS, "criteria": CRITERIA,
              "measurement_scope": "canonical SQLite Writer and WAL pool; metadata-only payload fixtures; no auth cache, object files or physical recovery",
              "cache_state": "uncontrolled filesystem cache; process restart is not a cold-cache trial",
              "arms": arms, "decision": "KEEP SQLite; alternative experiment remains conditional"}
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
                raise Unavailable("metadata allowance reached; preserving process/data cleanup time")
            used = footprint(campaign.root)
            campaign.ledger["peak_bytes"] = max(campaign.ledger["peak_bytes"], used)
            if (used > campaign.ledger["active"]["initial_bytes"] + reserved - RUN_HEADROOM
                    or used >= SPACE_LIMIT - CLEANUP_HEADROOM):
                raise Unavailable("metadata reserved disk headroom reached")
            if any(child.output_error or child.output_overflow for child in children):
                raise Unavailable("bounded child output incomplete")

        def launch(command, label):
            check_budget()
            child = Child(command, environment, artifacts, label, campaign)
            children.append(child)
            return child

        report["manifest"] = manifest(args, binary, launch)
        source_root = Path(__file__).resolve().parent
        report["manifest"]["metadata_workload_sources_sha256"] = {
            str(path.relative_to(source_root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted((source_root / "src/metadata_capacity").glob("*.rs"))}
        ticks = os.sysconf("SC_CLK_TCK")
        if type(ticks) is not int or not 1 <= ticks <= 100_000:
            raise Unavailable("Linux process CPU tick frequency unavailable")
        report["ticks_per_second"] = ticks
        total_samples = 0
        for index, (buckets, concurrency) in enumerate(MATRIX):
            check_budget()
            # Explicitly reserve a share for each remaining population. No first-seed overrun
            # silently consumes the second population's preparation and validation allowance.
            seconds = math.floor((work_deadline - time.monotonic() - 2) / (len(MATRIX) - index))
            if seconds < len(concurrency) * PHASE_SECONDS + 10:
                raise Unavailable("insufficient admitted preparation/phase/reopen time for remaining populations")
            config = {"root": str(data / f"population-{buckets}"), "buckets": buckets,
                      "seed_rows": SEED_ROWS, "seed": args.seed, "concurrency": list(concurrency),
                      "phase_seconds": PHASE_SECONDS, "deadline_seconds": seconds, "ticks_per_second": ticks}
            config_path = artifacts / f"population-{buckets}.config.json"
            atomic_json(config_path, config)
            before = time.monotonic()
            driver = launch([str(binary), str(config_path)], f"population-{buckets}")
            # Aggregate counters are constant space; bounded raw observations remain auditable.
            sample_count, peak_rss, peak_fds, final_ticks, fd_unavailable = 0, 0, 0, 0, 0
            with (artifacts / f"population-{buckets}.samples.jsonl").open("w") as stream:
                while True:
                    check_budget()
                    observation = process_sample(driver.process.pid)
                    stream.write(json.dumps(observation) + "\n")
                    stream.flush()
                    if stream.tell() > SAMPLE_LIMIT // len(MATRIX):
                        raise Unavailable("bounded metadata process observations exceeded")
                    sample_count += 1
                    peak_rss = max(peak_rss, observation["rss_kib"])
                    if observation["fds"] is None:
                        fd_unavailable += 1
                    else:
                        peak_fds = max(peak_fds, observation["fds"])
                    final_ticks = observation["process_cpu_ticks"]
                    if driver.exited() is not None:
                        break
                    time.sleep(.05)
            outcome = driver.exited()
            driver.stop(min(deadline - 2, time.monotonic() + 4))
            check_budget()
            records = read_records(driver.paths[0])
            arm = {"config": config, "records": records, "coordinator_wall_seconds": time.monotonic() - before,
                   "process_sample_count": sample_count, "peak_rss_kib": peak_rss, "peak_fds": peak_fds,
                   "process_cpu_seconds": final_ticks / ticks, "fd_samples_unavailable": fd_unavailable,
                   "process_sampling_scope": "maxima of available 50ms samples; transient unavailable FD samples counted explicitly",
                   "process_cpu_scope": "complete driver process including launch gate; separate from actual Writer thread CPU"}
            arms.append(arm)
            total_samples += sample_count
            if outcome == RESULTS["INCONCLUSIVE"]:
                terminal = records[-1] if records else {}
                raise Unavailable(terminal.get("reason", "driver did not complete its admitted metadata work"))
            if outcome != 0:
                raise RuntimeError(f"metadata driver failed (exit {outcome}); bounded logs retained")
            validate_records(records, config)
        assessment = assess(arms)
        report.update(assessment)
        reasons.extend(assessment["reasons"])
        report["process_sample_count"] = total_samples
        status = assessment["status"]
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
                reasons.append("bounded child output incomplete after final drain")
            for path in (directory / "data", directory / "artifacts/scratch"):
                if path.exists():
                    remove_tree(path, deadline - 1)
            clean = True
        except Exception as error:
            status = "INCONCLUSIVE"
            reasons.append(f"cleanup incomplete: {error}")
        elapsed = time.monotonic() - start
        charged = elapsed + 1
        if charged > args.allow_seconds:
            status = "INCONCLUSIVE"
            reasons.append("actual elapsed time plus finalization exceeded reservation and was charged")
        report.update(status=status, reasons=reasons, elapsed_seconds=elapsed, charged_seconds=charged,
                      cleaned_data_and_processes=clean, finalization_allowance_seconds=1,
                      observer_process_cpu_seconds=time.process_time() - observer_start)
        if status != "PASS":
            report["writer_limit"] = "NO demonstrated Writer limit"
            report["decision"] = "KEEP SQLite; alternative experiment remains conditional"
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
    parser.add_argument("--seed", type=int, default=0x5eed)
    parser.add_argument("--allow-seconds", type=int, default=600)
    args = parser.parse_args()
    try:
        validate_args(args)
    except ValueError as error:
        parser.error(str(error))
    campaign = None
    try:
        campaign = Campaign(args.root)
        return run_comparison(args, campaign)
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
