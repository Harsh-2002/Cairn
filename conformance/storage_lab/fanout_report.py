#!/usr/bin/env python3
"""Export the fanout decision and measured medians under the shared campaign allowance."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import time

from budget import Campaign, Unavailable, atomic_json, read_json, valid_token


def render(report):
    if report.get("kind") != "fanout":
        raise ValueError("expected a fanout comparison")
    lines = ["## Measurement result", "", f"**{report['status']} — {report['decision']}.**", "",
             *[f"- {reason}" for reason in report.get("reasons", [])], "",
             "All ratios are medians of paired candidate/flat measurements; lower is better.", "",
             "| Workload | Metric | Flat median | Fanout median | Paired ratio |",
             "|---|---|---:|---:|---:|"]
    comparisons = report.get("assessment", {}).get("comparisons", {})
    for case, comparison in comparisons.items():
        for metric, ratio in comparison["median_candidate_to_flat"].items():
            medians = [statistics.median(arm[metric] for arm in comparison[layout])
                       for layout in ("flat", "fanout")]
            lines.append(f"| {case} | {metric} | {medians[0]:.6g} | {medians[1]:.6g} | {ratio:.4f} |")
    for case, comparison in comparisons.items():
        drift = comparison["flat_control_relative_span"]
        lines.extend(["", f"{case} flat-control spans (max/min − 1): "
                      + ", ".join(f"{metric}={value:.1%}" for metric, value in drift.items()) + ".", ""])
    lines.extend(["", "| Workload | Layout | Publications/s | Reads/s | Deletes/s | Directory sync calls | Created directories | Parent creation/sync seconds | Sync call seconds | Driver CPU cores | Observer CPU cores |",
                  "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"])
    for case in report["cases"]:
        for layout in ("flat", "fanout"):
            arms = [arm for arm in report["arms"] if arm["case"] == case and arm["layout"] == layout]
            if not arms:
                continue
            def median(get):
                values = [get(arm) for arm in arms]
                return f"{statistics.median(values):.6g}" if all(value is not None for value in values) else "unavailable"
            values = [median(lambda arm, phase=phase: arm["measurements"][phase]["successful_per_second"])
                      for phase in ("publish", "read", "delete")]
            values += [median(lambda arm, key=key: arm["measurements"]["durability"][key]) for key in
                       ("coalesced_directory_sync_calls", "created_directories", "directory_creation_parent_sync_cumulative_seconds", "coalesced_directory_sync_cumulative_seconds")]
            values += [median(lambda arm, key=key: arm.get(key)) for key in ("driver_average_cpu_cores", "observer_average_cpu_cores")]
            lines.append(f"| {case} | {layout} | " + " | ".join(values) + " |")
    lines.extend(["", "Sync times are cumulative across concurrent calls, not disjoint service time. Read rates combine equal whole/range counts; eligible p99 values above are medians of per-arm p99, not a pooled p99.", "",
                  f"Comparison `{report['id']}` charged {report['charged_seconds']:.6f} seconds. "
                  f"Data/process cleanup completed: {report['cleaned_data_and_processes']}. "
                  "The JSON companion preserves every arm, memory/descriptor/thread samples, device counters, pressure and exact provenance.", ""])
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--run", required=True)
    args = parser.parse_args()
    if not valid_token(args.run):
        parser.error("expected an owned comparison token")
    campaign = Campaign(args.root)
    try:
        token = campaign.admit("fanout", 15, 128 * 1024**2)
        start, status = time.monotonic(), "FAIL"
        try:
            source = campaign.root / f"{args.run}.result.json"
            report = read_json(source)
            markdown = render(report)
            if time.monotonic() - start > 10:
                raise Unavailable("report reduction allowance exhausted")
            exported = {"comparison": report, "ledger_before_export": campaign.ledger,
                        "exporter_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
            atomic_json(campaign.root / f"{token}.export.json", exported)
            (campaign.root / f"{token}.export.md").write_text(markdown)
            status = "PASS"
            print(json.dumps({"export_id": token, "markdown": markdown}))
        finally:
            campaign.finish(time.monotonic() - start + 1, status, clean=True)
    finally:
        campaign.close()


if __name__ == "__main__":
    main()
