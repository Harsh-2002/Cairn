"""Decision and real fresh-process resume fixtures; these are not capacity measurements."""
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from budget import Campaign, Unavailable
import metadata_alternative as alternative
from test_metadata_capacity import writer


def observation(engine, load=False):
    if engine == "sqlite":
        return {"stages_complete": True, "writer": writer(), "serialized_observed_seconds": 1}
    counters = dict.fromkeys(("cache_capacity_bytes", "cache_resident_bytes", "write_buffer_bytes", "sealed_memtables", "journal_bytes", "journal_count",
                             "live_tree_bytes", "live_tables", "level_zero_tables", "outstanding_flushes", "active_compactions", "completed_compactions", "compaction_seconds"), 1)
    stages = dict.fromkeys(("admission_ns", "queue_ns", "begin_ns", "apply_ns", "commit_ns", "mutations", "batches", "writer_tid"), 1000)
    stages["rejected"] = 100 if load else 0
    return {"stages_complete": True, "writer": stages, "engine_samples": 2, "engine_maxima": counters, "engine_final": counters}


def distribution(count, seconds):
    return {"count": count, "sum_seconds": count * seconds, "max_seconds": seconds if count else 0,
            "p99_seconds_interval": None, "bucket_relative_width": .02, "overflow": 0}


def arm(action, engine):
    config = {"root": "/fixture/" + engine, "engine": engine, "action": action, "seed_rows": 100_000,
              "seed": 42, "phase_seconds": 5, "deadline_seconds": 120, "ticks_per_second": 100}
    expected = alternative.expected_state(1)
    records = [{"event": "opened", "config": config, "open_seconds": 1}]
    if action == "prepare":
        records.append({"event": "prepared", "seconds": 1, "observation": observation(engine), "expected": expected})
    records.append({"event": "verified_before", "expected": expected, "observation": observation(engine), "quota_verified": True})
    phase = None
    if action == "run":
        bundles = 1500 if engine == "fjall" else 1000
        families = {}
        for family, multiplier in alternative.FAMILY_COUNTS_PER_CYCLE.items():
            count = bundles // 5 * multiplier
            rejected = family == "conditional_reject"
            families[family] = {"successful": distribution(0 if rejected else count, .001), "expected_rejected": distribution(count if rejected else 0, .001)}
        phase = {"concurrency": 128, "seconds": 5., "completed_operation_bundles": bundles, "bundles_per_second": bundles / 5,
                 "writer_cpu_seconds": 1, "cleanup_claims_lost": 0, "families": families, "observation": observation(engine, True)}
        if engine == "fjall":
            phase["observation"]["writer"].update(mutations=bundles * 4, rejected=bundles // 5)
        records.append({"event": "phase", "report": phase})
        records.append({"event": "verified_after", "expected": expected, "observation": observation(engine), "quota_verified": True})
    result = {"event": "result", "status": "complete", "expected": expected, "phase": phase, "close_complete": True,
              "open_seconds": 1., "checkpoint_seconds": 1., "close_seconds": 1., "total_seconds": 8.,
              "process_io_after_close": {"read_bytes": 0, "write_bytes": 4096, "cancelled_write_bytes": 0},
              "disk_after_close": {"files": 3, "logical_bytes": 1000, "allocated_bytes": 4096, "physical_table_bytes": 0}}
    records.append(result)
    return {"config": config, "records": records, "result": result, "process_sample_count": 50,
            "peak_rss_kib": 100, "peak_fds": 20, "process_cpu_seconds": 2.}


def arms():
    return [arm("prepare", "sqlite"), arm("prepare", "fjall"), *[arm("run", engine) for pair in alternative.PAIRS for engine in pair],
            arm("verify", "sqlite"), arm("verify", "fjall")]


class AlternativeTests(unittest.TestCase):
    def test_complete_controlled_pairs_qualify_only_the_isolated_candidate(self):
        result = alternative.assess(arms())
        self.assertEqual(result["status"], "PASS")
        self.assertTrue(result["candidate_qualified"])
        self.assertEqual(result["median_throughput_gain"], .5)
        self.assertTrue(result["decision"].startswith("KEEP SQLite"))

    def test_protected_median_failure_and_control_drift_cannot_qualify(self):
        values = arms()
        for value in values:
            if value["config"]["action"] == "run" and value["config"]["engine"] == "fjall":
                value["peak_rss_kib"] *= 1.11
        result = alternative.assess(values)
        self.assertFalse(result["candidate_qualified"])
        self.assertIn("peak_rss_kib", result["protected_regressions"])
        values = arms()
        phase = values[2]["result"]["phase"]
        phase["seconds"] = 6.
        phase["bundles_per_second"] = phase["completed_operation_bundles"] / 6.
        result = alternative.assess(values)
        self.assertEqual(result["status"], "INCONCLUSIVE")
        self.assertFalse(result["candidate_qualified"])

    def test_missing_arm_wrong_family_distribution_and_false_p99_fail_closed(self):
        with self.assertRaises(Unavailable):
            alternative.assess(arms()[:-1])
        for change in ("distribution", "p99", "close", "observation", "final_io"):
            values = arms()
            target = values[2]
            if change == "distribution":
                target["result"]["phase"]["families"]["permanent_delete"]["successful"]["count"] -= 1
            elif change == "p99":
                target["result"]["phase"]["families"]["current_read"]["successful"]["p99_seconds_interval"] = [.1, .2]
            elif change == "close":
                target["result"]["close_complete"] = False
            elif change == "observation":
                target["result"]["phase"]["observation"]["stages_complete"] = False
            else:
                del target["result"]["process_io_after_close"]
            with self.subTest(change=change), self.assertRaises(Unavailable):
                alternative.assess(values)

    def test_cumulative_metadata_allowance_rejects_before_process_or_prerequisite(self):
        with tempfile.TemporaryDirectory() as root:
            campaign = Campaign(Path(root) / "campaign", create=True, require_ssd=False)
            try:
                campaign.ledger["runs"].append({"phase": "metadata", "elapsed_seconds": 301, "reserved_seconds": 301})
                args = SimpleNamespace(seed=42, allow_seconds=300, commit="fixture", build_settings="fixture", capacity_result="absent")
                with patch.object(alternative, "prerequisite") as prerequisite, patch.object(alternative, "Child") as child:
                    with self.assertRaises(Unavailable):
                        alternative.run_comparison(args, campaign)
                    prerequisite.assert_not_called()
                    child.assert_not_called()
            finally:
                campaign.close()

    def test_missing_capacity_predicate_does_not_launch_candidate(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "capacity.json"
            path.write_text(json.dumps({"status": "PASS", "cleaned_data_and_processes": True, "arms": []}))
            with self.assertRaises(Unavailable):
                alternative.prerequisite(path)

    def test_failed_owned_process_is_charged_and_cleaned_without_retry(self):
        with tempfile.TemporaryDirectory() as root:
            campaign = Campaign(Path(root) / "campaign", create=True, require_ssd=False)
            args = SimpleNamespace(seed=42, allow_seconds=180, commit="fixture", build_settings="fixed correctness fixture",
                                   capacity_result="unused", binary="/bin/false")
            try:
                with redirect_stdout(io.StringIO()), patch.object(alternative, "prerequisite", return_value={"fixture": True}), \
                     patch.object(alternative, "manifest", return_value={"fixture": True}), patch.object(alternative, "reserve_bytes", return_value=16 * 1024**2), \
                     patch.object(alternative, "RUN_HEADROOM", 1024):
                    result = alternative.run_comparison(args, campaign)
                self.assertEqual(result, alternative.RESULTS["INCONCLUSIVE"])
                self.assertIsNone(campaign.ledger["active"])
                self.assertEqual(len(campaign.ledger["runs"]), 1)
                self.assertGreater(campaign.ledger["spent_seconds"], 0)
                run = campaign.ledger["runs"][0]
                self.assertEqual(len(run["children"]), 1, "failed arm must not be retried")
                self.assertFalse((campaign.root / run["id"] / "data").exists())
                report = json.loads((campaign.root / f"{run['id']}.result.json").read_text())
                self.assertTrue(report["cleaned_data_and_processes"])
                self.assertFalse(report["candidate_qualified"])
            finally:
                campaign.close()

    @unittest.skipUnless(os.environ.get("LAB_TEST_METADATA_COMPARISON_DRIVER"), "explicit correctness-test comparison binary not supplied")
    def test_actual_engines_resume_same_seed_across_fresh_processes(self):
        binary = str(Path(os.environ["LAB_TEST_METADATA_COMPARISON_DRIVER"]).resolve(strict=True))
        with tempfile.TemporaryDirectory(prefix="metadata-alternative-correctness-") as temporary:
            root = Path(temporary).resolve()
            for engine in ("sqlite", "fjall"):
                identity = None
                for action in ("prepare", "run", "verify"):
                    config = {"root": str(root / engine), "engine": engine, "action": action, "seed_rows": 100,
                              "seed": 42, "phase_seconds": 1, "deadline_seconds": 45, "ticks_per_second": os.sysconf("SC_CLK_TCK")}
                    path = root / "config.json"
                    path.write_text(json.dumps(config))
                    result = subprocess.run([binary, str(path)], capture_output=True, text=True, timeout=50, check=False)
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    records = [json.loads(line) for line in result.stdout.splitlines()]
                    alternative.validate_records(records, config)
                    current = (root / engine / "fixture.json").read_bytes()
                    if identity is not None:
                        self.assertEqual(current, identity, "resume must not replace seed identity")
                    identity = current
                damaged = json.loads(identity)
                damaged["seed"] += 1
                (root / engine / "fixture.json").write_text(json.dumps(damaged))
                result = subprocess.run([binary, str(path)], capture_output=True, text=True, timeout=30, check=False)
                self.assertEqual(result.returncode, 2, "mismatched resume descriptor must fail closed")
                (root / engine / "fixture.json").write_bytes(identity)
                marker = root / engine / ("metadata.sqlite3" if engine == "sqlite" else "fjall/version")
                held = marker.with_name(marker.name + ".held")
                marker.rename(held)
                result = subprocess.run([binary, str(path)], capture_output=True, text=True, timeout=30, check=False)
                self.assertEqual(result.returncode, 2, "missing seed database/format must fail before creation")
                self.assertFalse(marker.exists(), "resume must not create replacement database state")
                held.rename(marker)


if __name__ == "__main__":
    unittest.main()
