"""Recovery screen ownership/measurement regressions; fixed fixtures are not benchmarks."""
import argparse
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from budget import Campaign, Unavailable
from recovery import COUNTS, command_resources, finish_server, reserve_bytes, run_screen
from recovery_driver import run as drive


class RecoveryCostTests(unittest.TestCase):
    def test_failed_durability_shutdown_is_not_downgraded_to_missing_measurement(self):
        child = Mock(label="source")
        child.exited.side_effect = [None, 1]
        child.finish_profiled_target.side_effect = Unavailable("wrapper failure")
        with self.assertRaisesRegex(RuntimeError, "durability shutdown failed"):
            finish_server(child, Path("/fixture/cairn"), 100, 110)
        child.stop.assert_not_called()
        child.exited.side_effect = [None, None]
        with self.assertRaises(Unavailable):
            finish_server(child, Path("/fixture/cairn"), 100, 110)

    def test_resource_gaps_and_failed_commands_cannot_be_reported_as_measured_success(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "resources.tsv"
            path.write_text("0.25\t0.10\t0.05\t4096\t0\n")
            self.assertEqual(command_resources(path)["peak_rss_kib"], 4096)
            for bad in ("", "0.25\t0.1\t0.1\t0\t0", "0.25\t0.1\t0.1\t4096\t1",
                        "nan\t0.1\t0.1\t4096\t0", "0.25\t-1\t0.1\t4096\t0",
                        "unknown\t0.1\t0.1\t4096\t0"):
                path.write_text(bad)
                with self.assertRaises(Unavailable):
                    command_resources(path)

    def test_reservation_covers_simultaneous_source_snapshots_restore_and_metadata(self):
        self.assertGreater(reserve_bytes(COUNTS), 4 * 1000 * (4096 + 256 * 1024) + 512 * 1024**2)

    def test_unbounded_or_unknown_client_configuration_is_rejected_before_network(self):
        for config in ({}, {"objects": 1001, "size": 4096, "seed": 1, "mode": "prepare"},
                       {"objects": 100, "size": 4096, "seed": 1, "mode": "unknown"}):
            with self.assertRaises(ValueError):
                drive(config)

    def test_missing_binary_failure_is_charged_and_owned_data_is_removed(self):
        with tempfile.TemporaryDirectory() as root:
            campaign = Campaign(Path(root) / "campaign", create=True, require_ssd=False)
            args = argparse.Namespace(root=str(campaign.root), binary=str(Path(root) / "missing"),
                                      commit="fixture", build_settings="fixed failure fixture",
                                      seed=1, allow_seconds=30)
            try:
                self.assertEqual(run_screen(args, campaign, (8,)), 1)
                result = json.loads(next(campaign.root.glob("*.result.json")).read_text())
                self.assertEqual(result["status"], "FAIL")
                self.assertEqual(result["decision"], "KEEP mandatory full startup scans")
                self.assertTrue(result["cleaned_data_and_processes"])
                self.assertFalse((campaign.root / result["id"] / "data").exists())
                self.assertIsNone(campaign.ledger["active"])
                self.assertGreaterEqual(campaign.ledger["spent_seconds"], 1)
            finally:
                campaign.close()

    @unittest.skipUnless(os.environ.get("LAB_TEST_RECOVERY_SERVER"), "explicit correctness-test server not supplied")
    def test_tiny_live_screen_verifies_restore_and_cleans_all_owned_work(self):
        with tempfile.TemporaryDirectory() as root:
            campaign = Campaign(Path(root) / "campaign", create=True, require_ssd=False)
            args = argparse.Namespace(root=str(campaign.root), binary=os.environ["LAB_TEST_RECOVERY_SERVER"],
                                      commit="fixture", build_settings="debug fixed correctness fixture",
                                      seed=1, allow_seconds=90)
            try:
                code = run_screen(args, campaign, (8,))
                result = json.loads(next(campaign.root.glob("*.result.json")).read_text())
                self.assertEqual(code, 0, result["reasons"])
                self.assertEqual(result["cases"][0]["verified_objects"], 8)
                self.assertEqual(result["cases"][0]["baseline_verified_objects"], 8)
                self.assertEqual(result["cases"][0]["preparation_cleanup"]["status"], "drained")
                self.assertIn("INCONCLUSIVE", result["activation"])
                self.assertTrue(result["cleaned_data_and_processes"])
                self.assertFalse((campaign.root / result["id"] / "data").exists())
                self.assertIsNone(campaign.ledger["active"])
            finally:
                campaign.close()


if __name__ == "__main__":
    unittest.main()
