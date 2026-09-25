#!/usr/bin/env python3
"""Bounded, alternating same-host Cairn A/B using fresh stores per arm."""

import argparse
import json
import os
import time
from pathlib import Path

from rustfs_compare import CASES, host_capacity, run_arm, sha256, tree_usage


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--control", type=Path)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--warp", type=Path, required=True)
    parser.add_argument("--case", choices=("put_1m", "mixed_1m", "get_1m", "list_4k"),
                        default="put_1m")
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--single-candidate", action="store_true",
                        help="one diagnostic candidate arm, never an adoption comparison")
    parser.add_argument("--duration", type=int, default=12)
    parser.add_argument("--max-load-seconds", type=float,
                        help="hard client-load timeout for a metered diagnostic arm")
    parser.add_argument("--metrics-drain-seconds", type=int, default=17)
    parser.add_argument("--allow-ballooning", action="store_true",
                        help="diagnostic only: permit up to 10%% MemTotal drift; retain samples")
    parser.add_argument("--control-linger-us", type=int, default=0)
    parser.add_argument("--candidate-linger-us", type=int, default=0)
    parser.add_argument("--sync-probe-lib", type=Path,
                        help="optional diagnostic glibc sync interposer")
    parser.add_argument("--max-seconds", type=int, default=600)
    parser.add_argument("--max-bytes", type=int, default=10_000_000_000)
    args = parser.parse_args()
    if not (1 <= args.pairs <= 5 and 5 <= args.duration <= 60
            and 0 <= args.metrics_drain_seconds <= 20 and 60 <= args.max_seconds <= 1800
            and 0 <= args.control_linger_us <= 1000 and 0 <= args.candidate_linger_us <= 1000):
        parser.error("an experiment bound is outside its safe range")
    if args.single_candidate:
        if args.pairs != 1 or args.control is not None or args.max_load_seconds is None:
            parser.error("single-candidate mode needs --pairs 1, no control, and a load cap")
    elif args.control is None:
        parser.error("paired mode needs --control")
    if args.max_load_seconds is not None and not 1 <= args.max_load_seconds <= 60:
        parser.error("measured client-load cap outside diagnostic bounds")
    root = args.root.resolve(strict=True)
    if not root.is_dir() or root.is_symlink() or not str(root).startswith("/var/tmp/cairn-performance-"):
        parser.error("root must be a private /var/tmp/cairn-performance-* directory")
    for binary in (args.candidate, args.warp) + (() if args.single_candidate else (args.control,)):
        if not binary.is_file() or not os.access(binary, os.X_OK):
            parser.error(f"not executable: {binary}")
    if args.sync_probe_lib is not None:
        args.sync_probe_lib = args.sync_probe_lib.resolve(strict=True)
        if not args.sync_probe_lib.is_file():
            parser.error("sync probe is not a file")
    args.warp = str(args.warp)
    report = {
        "schema": 1, "case": args.case, "pairs": args.pairs, "duration": args.duration,
        "host_capacity": host_capacity(),
        "allow_ballooning": args.allow_ballooning,
        "single_candidate_diagnostic": args.single_candidate,
        "max_load_seconds": args.max_load_seconds,
        "metrics_drain_seconds": args.metrics_drain_seconds,
        "linger_us": {"control": args.control_linger_us,
                      "candidate": args.candidate_linger_us},
        "binaries": {name: {"path": str(path), "sha256": sha256(path)} for name, path in
                     (("candidate", args.candidate), ("warp", args.warp)) +
                     (() if args.single_candidate else (("control", args.control),))},
        "sync_probe_sha256": sha256(args.sync_probe_lib) if args.sync_probe_lib else None,
        "arms": [],
    }
    output = root / f"cairn-{args.case}-{'single' if args.single_candidate else 'ab'}-results.json"
    start = time.monotonic()
    deadline = start + args.max_seconds - 30
    case = next(case for case in CASES if case[0] == args.case)
    try:
        for pair in range(args.pairs):
            order = (("candidate",) if args.single_candidate else
                     (("control", "candidate") if pair % 2 == 0 else ("candidate", "control")))
            for variant in order:
                if time.monotonic() + args.duration + args.metrics_drain_seconds + 30 >= deadline:
                    raise RuntimeError("remaining time cannot admit another arm")
                if tree_usage(root)[0] > args.max_bytes - 1_000_000_000:
                    raise RuntimeError("insufficient owned disk headroom")
                args.cairn = str(getattr(args, variant))
                args.cairn_linger_us = getattr(args, f"{variant}_linger_us")
                index = len(report["arms"]) + 1
                total = args.pairs if args.single_candidate else 2 * args.pairs
                print(f"[{index}/{total}] pair {pair + 1} {variant}", flush=True)
                result, _ = run_arm(args, root, case, "cairn", index, deadline)
                result["variant"] = variant
                report["arms"].append(result)
                report["elapsed_seconds"] = time.monotonic() - start
                output.write_text(json.dumps(report, indent=2) + "\n")
                print(f"  {result['status']} {result['summary']} errors={result['reported_errors']}", flush=True)
                if result["status"] != "PASS":
                    raise RuntimeError(f"arm {index} did not pass")
    except (KeyboardInterrupt, OSError, RuntimeError) as error:
        report["stopped_reason"] = str(error)
        report["elapsed_seconds"] = time.monotonic() - start
        output.write_text(json.dumps(report, indent=2) + "\n")
        raise SystemExit(str(error))
    print(f"Completed {len(report['arms'])} arms in {report['elapsed_seconds']:.1f}s; {output}")


if __name__ == "__main__":
    main()
