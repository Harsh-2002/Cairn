#!/usr/bin/env python3
"""Run the preregistered root-barrier comparison under one client-time ledger.

Each child runner's wall cap is at most the remaining *client-process* allowance.
Consequently even a child that fails before writing its last arm cannot overspend the
campaign. An interrupted campaign is deliberately not resumable without inspection.
"""

import argparse
import json
import math
import os
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path

from rustfs_compare import (BUCKET, CASES, STRICT_SETTINGS, capacity_compatible,
                            host_capacity, sha256, tree_usage)


PHASES = (
    ("put_ab", "ab", "put_1m", 5, 420),
    ("put_rustfs", "rustfs", "put_1m", 2, 240),
    ("mixed_ab", "ab", "mixed_1m", 1, 180),
    ("get_ab", "ab", "get_1m", 1, 180),
    ("list_ab", "ab", "list_4k", 1, 180),
)

DIAGNOSTIC_PHASES = (
    ("put_ab", "ab", "put_1m", 2, 220),
    ("put_rustfs", "rustfs", "put_1m", 2, 220),
)

PUT_TIMING_PHASES = (
    ("timing_ab", "ab", "put_1m", 3, 270),
    ("timing_rustfs", "rustfs", "put_1m", 1, 130),
)

PUT_TIMING_STAGES = ("total", "preflight", "storage_admission", "blob_stage",
                     "publication_prep", "publication", "notification", "audit")


def atomic_json(path, value):
    temporary = path.with_name(path.name + ".new")
    with temporary.open("x") as output:
        json.dump(value, output, indent=2)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)


def phase_client_seconds(report):
    arms = report.get("arms")
    if not isinstance(arms, list) or not arms:
        raise ValueError("phase has no measured arms")
    total = 0.0
    for arm in arms:
        elapsed = arm.get("load_elapsed_seconds")
        if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed <= 0:
            raise ValueError("phase has invalid client-process time")
        total += elapsed
    return total


def put_timing_metrics_valid(arm):
    """Require loss-free, same-population PUT stages in the instrumented arm."""
    lines = arm.get("metrics_after_drain")
    if not isinstance(lines, list) or not all(isinstance(line, str) for line in lines):
        return False
    try:
        dropped_lines = [line for line in lines
                         if line.startswith("cairn_put_timing_dropped_total ")]
        if len(dropped_lines) != 1:
            return False
        dropped = float(dropped_lines[0].split()[-1])
        counts = []
        for stage in PUT_TIMING_STAGES:
            prefix = f'cairn_put_stage_seconds_count{{stage="{stage}",result="ok"}} '
            stage_lines = [line for line in lines if line.startswith(prefix)]
            if len(stage_lines) != 1:
                return False
            counts.append(float(stage_lines[0][len(prefix):]))
        interrupted = [float(line.split()[-1]) for line in lines
                       if line.startswith("cairn_put_stage_seconds_count{")
                       and 'result="interrupted"' in line]
    except (ValueError, TypeError):
        return False
    return (dropped == 0 and all(math.isfinite(count) and count.is_integer()
                                 and count >= 5 for count in counts)
            and len(set(counts)) == 1
            and all(math.isfinite(count) and count == 0 for count in interrupted))


def phase_passes(report, phase, identities, duration, metrics_drain_seconds,
                 require_put_timing=False, expected_host_capacity=None,
                 allow_ballooning=False):
    """Refuse to promote a runner PASS from the wrong binary, mode, or workload."""
    _, kind, case, pairs, _ = phase
    case_info = next(item for item in CASES if item[0] == case)
    if ("stopped_reason" in report or len(report.get("arms", [])) != 2 * pairs
            or report.get("pairs") != pairs or report.get("duration") != duration
            or report.get("metrics_drain_seconds") != metrics_drain_seconds
            or report.get("allow_ballooning", False) != allow_ballooning):
        return False
    capacity = report.get("host_capacity")
    if (not isinstance(capacity, dict)
            or (expected_host_capacity is not None and not capacity_compatible(
                expected_host_capacity, capacity, allow_ballooning))):
        return False
    if kind == "ab":
        expected = {name: identities[name] for name in ("candidate", "control", "warp")}
        if (report.get("case") != case or report.get("binaries") != expected
                or report.get("single_candidate_diagnostic") is not False):
            return False
    else:
        expected = {"cairn": identities["candidate"]["sha256"],
                    "rustfs": identities["rustfs"]["sha256"],
                    "warp": identities["warp"]["sha256"]}
        expected_case = [*case_info[:4], list(case_info[4])]
        if (report.get("cases") != [expected_case]
                or report.get("binary_sha256") != expected
                or report.get("engine_selection") != "both"
                or report.get("strict_settings") != STRICT_SETTINGS):
            return False
    for position, arm in enumerate(report["arms"]):
        pair, offset = divmod(position, 2)
        order = (("control", "candidate") if pair % 2 == 0 else
                 ("candidate", "control")) if kind == "ab" else (
                 ("cairn", "rustfs") if pair % 2 == 0 else ("rustfs", "cairn"))
        variant = order[offset]
        engine = "cairn" if kind == "ab" else variant
        score = arm.get("objects_per_second")
        if (arm.get("status") != "PASS" or arm.get("reported_errors") != 0
                or arm.get("error_line_count") != 0 or arm.get("warp_exit") != 0
                or arm.get("limit_hit") is not False or arm.get("case") != case
                or arm.get("concurrency") != case_info[3]
                or arm.get("engine") != engine
                or (kind == "ab" and arm.get("variant") != variant)
                or arm.get("strict_settings") != STRICT_SETTINGS[engine]
                or type(score) not in (int, float) or not math.isfinite(score) or score <= 0):
            return False
        if (not capacity_compatible(capacity, arm.get("host_capacity_before"),
                                    allow_ballooning)
                or not capacity_compatible(capacity, arm.get("host_capacity_after"),
                                           allow_ballooning)
                or not capacity_compatible(arm.get("host_capacity_before"),
                                           arm.get("host_capacity_after"),
                                           allow_ballooning)):
            return False
        if case_info[1] in ("put", "get", "mixed"):
            throughput = arm.get("mib_per_second")
            if (type(throughput) not in (int, float) or not math.isfinite(throughput)
                    or throughput <= 0):
                return False
        mode = "full-metadata" if engine == "cairn" else "strict"
        if arm.get("durability") != {"bucket": BUCKET, "mode": mode}:
            return False
        if engine == "rustfs" and arm.get("durability_after") != arm["durability"]:
            return False
        if (case_info[1] == "put" and not arm.get("warp_operation_timeline", {})
                .get("status", "").startswith("captured source-derived")):
            return False
        if (require_put_timing and engine == "cairn" and
                (kind == "rustfs" or variant == "candidate") and
                not put_timing_metrics_valid(arm)):
            return False
    return True


def phase_command(args, phase, wall_cap):
    _, kind, case, pairs, _ = phase
    common = ["--root", str(args.root), "--warp", str(args.warp),
              "--case", case, "--pairs", str(pairs),
              "--duration", str(args.duration),
              "--metrics-drain-seconds", str(args.metrics_drain_seconds),
              "--max-seconds", str(wall_cap), "--max-bytes", str(args.max_bytes)]
    if args.allow_ballooning:
        common.append("--allow-ballooning")
    if kind == "ab":
        return [sys.executable, str(Path(__file__).with_name("cairn_put_ab.py")),
                "--control", str(args.control), "--candidate", str(args.candidate),
                *common]
    return [sys.executable, str(Path(__file__).with_name("rustfs_compare.py")),
            "--cairn", str(args.candidate), "--rustfs", str(args.rustfs), *common]


def phase_output(root, phase):
    _, kind, case, _, _ = phase
    return (root / f"cairn-{case}-ab-results.json" if kind == "ab"
            else root / "comparison-results.json")


def admissible_wall_cap(phase_wall_cap, remaining_client_seconds):
    # Retain 30 seconds for the child runner's deadline/teardown jitter. Its own
    # deadline is shorter still, but do not turn that implementation detail into
    # the only protection for the campaign's externally authorized allowance.
    return min(phase_wall_cap, math.floor(remaining_client_seconds) - 30)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    for name in ("candidate", "control", "rustfs", "warp"):
        parser.add_argument(f"--{name}", type=Path, required=True)
        parser.add_argument(f"--{name}-sha256", required=True)
    parser.add_argument("--max-client-seconds", type=int, default=600)
    parser.add_argument("--profile", choices=("adoption", "diagnostic", "put_timing"),
                        default="adoption")
    parser.add_argument("--max-bytes", type=int, default=10_000_000_000)
    parser.add_argument("--duration", type=int, default=12)
    parser.add_argument("--metrics-drain-seconds", type=int, default=16)
    parser.add_argument("--allow-ballooning", action="store_true",
                        help="PUT timing diagnostic only: permit bounded VM MemTotal drift")
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    if (args.root.is_symlink() or not root.is_dir()
            or not str(root).startswith("/var/tmp/cairn-performance-")
            or stat.S_IMODE(root.stat().st_mode) != 0o700):
        parser.error("root must be a private, nonsymlink /var/tmp/cairn-performance-* directory")
    args.root = root
    if not (120 <= args.max_client_seconds <= 3600
            and 1_000_000_000 <= args.max_bytes <= 100_000_000_000
            and 5 <= args.duration <= 60
            and 0 <= args.metrics_drain_seconds <= 20):
        parser.error("campaign bounds are outside safe ranges")
    if args.profile == "put_timing" and (args.max_client_seconds != 300
                                         or args.max_bytes != 10_000_000_000
                                         or args.duration != 12
                                         or args.metrics_drain_seconds != 16):
        parser.error("PUT timing profile requires the preregistered 300s/10GB/12s/16s bounds")
    if args.allow_ballooning and args.profile != "put_timing":
        parser.error("ballooning tolerance is for the PUT timing diagnostic only")
    identities = {}
    for name in ("candidate", "control", "rustfs", "warp"):
        path = getattr(args, name).resolve(strict=True)
        if not path.is_file() or not os.access(path, os.X_OK):
            parser.error(f"{name} is not executable")
        digest = sha256(path)
        if digest != getattr(args, f"{name}_sha256"):
            parser.error(f"{name} SHA-256 does not match preregistration")
        setattr(args, name, path)
        identities[name] = {"path": str(path), "sha256": digest}
    ledger_path = root / "root-barrier-campaign.json"
    if ledger_path.exists() or ledger_path.is_symlink():
        parser.error("existing campaign ledger; inspect it instead of resetting the allowance")
    if tree_usage(root)[0] > args.max_bytes - 1_000_000_000:
        parser.error("insufficient task-owned disk headroom")
    ledger = {"schema": 1, "max_client_seconds": args.max_client_seconds,
              "max_bytes": args.max_bytes, "duration": args.duration,
              "host_capacity": host_capacity(),
              "allow_ballooning": args.allow_ballooning,
              "profile": args.profile,
              "metrics_drain_seconds": args.metrics_drain_seconds,
              "binaries": identities, "phases": [], "client_seconds": 0.0}
    atomic_json(ledger_path, ledger)
    phases = (DIAGNOSTIC_PHASES if args.profile == "diagnostic" else
              PUT_TIMING_PHASES if args.profile == "put_timing" else PHASES)
    for phase in phases:
        name, _, _, pairs, phase_wall_cap = phase
        remaining = args.max_client_seconds - ledger["client_seconds"]
        wall_cap = admissible_wall_cap(phase_wall_cap, remaining)
        if wall_cap < 60 or tree_usage(root)[0] > args.max_bytes - 1_000_000_000:
            ledger["stopped_reason"] = "remaining time or scratch cannot admit another phase"
            atomic_json(ledger_path, ledger)
            break
        command = phase_command(args, phase, wall_cap)
        output = phase_output(root, phase)
        retained = root / f"root-barrier-{name}-raw.json"
        if output.exists() or retained.exists():
            ledger["stopped_reason"] = f"pre-existing output for phase {name}"
            atomic_json(ledger_path, ledger)
            break
        entry = {"name": name, "status": "running", "wall_cap_seconds": wall_cap,
                 "command": command, "started_unix_seconds": time.time()}
        ledger["phases"].append(entry)
        atomic_json(ledger_path, ledger)
        print(f"[{len(ledger['phases'])}/{len(phases)}] {name}, "
              f"{remaining:.2f}s client budget remaining", flush=True)
        completed = subprocess.run(command, check=False)
        try:
            capacity_after_phase = host_capacity()
            entry["host_capacity_after"] = capacity_after_phase
            report = json.loads(output.read_text())
            charged = phase_client_seconds(report)
            shutil.copyfile(output, retained)
            entry.update(status="PASS" if completed.returncode == 0
                         and capacity_compatible(ledger["host_capacity"],
                                                 capacity_after_phase,
                                                 args.allow_ballooning)
                         and phase_passes(report, phase, identities, args.duration,
                                          args.metrics_drain_seconds,
                                          require_put_timing=args.profile == "put_timing",
                                          expected_host_capacity=ledger["host_capacity"],
                                          allow_ballooning=args.allow_ballooning)
                         else "INCONCLUSIVE",
                         client_seconds=charged, raw_report=str(retained),
                         raw_sha256=sha256(retained), exit_code=completed.returncode)
            ledger["client_seconds"] += charged
            if charged > wall_cap or ledger["client_seconds"] > args.max_client_seconds:
                entry["status"] = "INCONCLUSIVE"
                ledger["stopped_reason"] = "client-process allowance exceeded"
        except (OSError, ValueError, KeyError, TypeError) as error:
            entry.update(status="INCONCLUSIVE", error=str(error),
                         exit_code=completed.returncode)
        entry["finished_unix_seconds"] = time.time()
        atomic_json(ledger_path, ledger)
        if entry["status"] != "PASS":
            ledger.setdefault("stopped_reason", f"phase {name} did not pass")
            atomic_json(ledger_path, ledger)
            break
    else:
        ledger["status"] = "PASS"
        atomic_json(ledger_path, ledger)
    if ledger.get("status") != "PASS":
        raise SystemExit(ledger.get("stopped_reason", "campaign incomplete"))
    print(f"Completed {len(phases)} phases; {ledger['client_seconds']:.2f}s client-process "
          f"time; {ledger_path}")


if __name__ == "__main__":
    main()
