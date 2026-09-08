#!/usr/bin/env python3
"""Decode an owned profile under the same persistent campaign allowance."""
import argparse
import hashlib
import json
from pathlib import Path
import signal
import time

from budget import Campaign, PHASES, SPACE_LIMIT, Unavailable, atomic_json, footprint, valid_token
from lab import RESULTS, clean_environment
from processes import Child


def analyze(args, campaign):
    if not valid_token(args.run_id) or not any(run["id"] == args.run_id for run in campaign.ledger["runs"]):
        raise ValueError("source must be a completed run in this campaign")
    token = campaign.admit(args.phase, args.allow_seconds, 6 * 1024**3)
    start = time.monotonic()
    deadline = start + args.allow_seconds
    directory = campaign.root / token
    child = None
    status, reasons, clean = "INCONCLUSIVE", [], False
    report = {"id": token, "source_run": args.run_id, "kind": "profile_analysis", "profile": args.profile}
    try:
        directory.mkdir(mode=0o700)
        artifacts = directory / "artifacts"
        artifacts.mkdir(mode=0o700)
        scratch = artifacts / "scratch"
        scratch.mkdir(mode=0o700)
        source = campaign.root / args.run_id / "artifacts"
        if args.profile == "cpu":
            inputs = [source / "cpu.perf"]
        else:
            inputs = [path for path in source.glob("heap*") if path.is_file() and path.suffix in {".gz", ".zst"}]
        if len(inputs) != 1 or not inputs[0].is_file() or inputs[0].stat().st_size < 256:
            raise Unavailable("no unique nonempty completed profile artifact")
        profile = inputs[0]
        with profile.open("rb") as stream:
            report["source_sha256"] = hashlib.file_digest(stream, "sha256").hexdigest()
        command = (["perf", "report", "--stdio", "--percent-limit", "1", "--sort", "symbol", "-i", str(profile)]
                   if args.profile == "cpu" else
                   ["heaptrack_print", "--file", str(profile), "--print-leaks=1", "--peak-limit=10", "--sub-peak-limit=3",
                    "--disable-builtin-suppressions", "--disable-embedded-suppressions",
                    "--print-massif", str(artifacts / "allocations.massif"), "--massif-detailed-freq=1"])
        report["command"] = command
        child = Child(command, {**clean_environment(), "TMPDIR": str(scratch)}, artifacts, "decoded", campaign)
        while child.exited() is None:
            if time.monotonic() >= deadline - 8:
                raise Unavailable("profile decoding runtime allowance reached")
            used = footprint(campaign.root)
            campaign.ledger["peak_bytes"] = max(campaign.ledger["peak_bytes"], used)
            if used >= SPACE_LIMIT - 1_000_000_000 or child.output_overflow or child.output_error:
                raise Unavailable("profile decoding space/output allowance reached")
            time.sleep(0.1)
        code = child.exited()
        child.stop(deadline - 1)
        if code != 0 or child.output_error or child.output_overflow:
            raise Unavailable("profile decoder failed or output was incomplete")
        report["decoded_stdout"] = str(child.paths[0].relative_to(campaign.root))
        report["decoded_stderr"] = child.paths[1].read_text(errors="replace")[:8192]
        status = "INCONCLUSIVE"
        reasons.append("profile decoded; sample sufficiency and ownership interpretation require review")
        report["decoding_complete"] = True
        report["interpretation"] = "decoding completed; attribution and sample sufficiency require review"
    except KeyboardInterrupt:
        status, reasons = "CANCELLED", ["operator interruption"]
    except Unavailable as error:
        status, reasons = "INCONCLUSIVE", [str(error)]
    except Exception as error:
        status, reasons = "FAIL", [f"{type(error).__name__}: {error}"]
    finally:
        try:
            if child:
                child.stop(deadline - 1)
            clean = True
        except Exception as error:
            status = "INCONCLUSIVE"
            reasons.append(f"profile decoder did not quiesce: {error}")
        elapsed = time.monotonic() - start
        if elapsed > args.allow_seconds:
            status = "INCONCLUSIVE"
            reasons.append("profile decoding exceeded its reservation; actual time charged")
        report.update(status=status, reasons=reasons, elapsed_seconds=elapsed, cleaned_data_and_processes=clean)
        atomic_json(campaign.root / f"{token}.result.json", report)
        campaign.finish(time.monotonic() - start, status, clean=clean)
    print(json.dumps({"status": status, "reasons": reasons, "result": str(campaign.root / f"{token}.result.json")}))
    return RESULTS[status]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--phase", choices=PHASES, default="baseline")
    parser.add_argument("--profile", choices=("cpu", "heap"), required=True)
    parser.add_argument("--allow-seconds", type=int, default=45)
    args = parser.parse_args()
    if args.allow_seconds < 10:
        parser.error("allow-seconds must reserve decoder teardown time")
    campaign = Campaign(args.root)
    try:
        return analyze(args, campaign)
    finally:
        campaign.close()


if __name__ == "__main__":
    def interrupted(*_):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
