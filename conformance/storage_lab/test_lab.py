"""Small deterministic correctness/failure fixtures; these are not performance experiments."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from budget import Campaign, SPACE_LIMIT, Unavailable, footprint, remove_tree, validate_ledger
from lab import profile_command, purge_artifacts, recover, reservation, run_case
from processes import Child, live_group_members
from s3_driver import distribution, payload


class BudgetTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "campaign"
        self.campaign = Campaign(self.path, create=True, require_ssd=False)

    def tearDown(self):
        self.campaign.close()
        self.temporary.cleanup()

    def test_exclusive_ledger_and_oversized_admission(self):
        with self.assertRaises(Unavailable):
            Campaign(self.path, require_ssd=False)
        with self.assertRaises(Unavailable):
            self.campaign.admit("baseline", 601, 1024)
        with self.assertRaises(Unavailable):
            self.campaign.admit("baseline", 30, SPACE_LIMIT)
        self.assertEqual(self.campaign.ledger["spent_seconds"], 0)

    def test_crash_reservation_persists_and_cleanup_blocks_refund(self):
        self.campaign.admit("baseline", 60, 1024 * 1024)
        self.campaign.finish(3, "CANCELLED", clean=False)
        self.campaign.close()
        self.campaign = Campaign(self.path, require_ssd=False)
        self.assertEqual(self.campaign.ledger["spent_seconds"], 60)
        with self.assertRaises(Unavailable):
            self.campaign.admit("baseline", 30, 1024)
        self.campaign.finish(4, "CANCELLED", clean=True)
        self.assertEqual(self.campaign.ledger["spent_seconds"], 4)

    def test_corrupt_ledger_cannot_authorize_cleanup_outside_root(self):
        self.campaign.admit("baseline", 30, 1024)
        self.campaign.ledger["active"]["id"] = "../../unrelated"
        with self.assertRaises(ValueError):
            validate_ledger(self.campaign.ledger)

    def test_unused_time_moves_forward_and_overruns_are_charged(self):
        self.campaign.admit("baseline", 60, 1024)
        self.campaign.finish(61, "INCONCLUSIVE", clean=True)
        self.assertEqual(self.campaign.ledger["spent_seconds"], 61)
        self.campaign.admit("fanout", 700, 1024)
        self.campaign.finish(2, "PASS", clean=True)
        with self.assertRaises(Unavailable):
            self.campaign.admit("baseline", 10, 1024)
        self.assertEqual(self.campaign.ledger["spent_seconds"], 63)

    def test_symlink_and_cleanup_deadline(self):
        external = Path(self.temporary.name) / "external"
        external.write_text("keep")
        data = self.path / "data"
        data.mkdir()
        (data / "link").symlink_to(external)
        with self.assertRaises(Unavailable):
            footprint(self.path)
        with self.assertRaises(Unavailable):
            remove_tree(data, time.monotonic() - 1)
        remove_tree(data, time.monotonic() + 2)
        self.assertEqual(external.read_text(), "keep")

    def test_case_reservation_covers_outstanding_data_and_profiles(self):
        config = {"max_ops": 30_000, "concurrency": 128, "size": 1024 * 1024}
        self.assertGreater(reservation(config, "cpu"), reservation(config, "none"))
        self.assertGreater(reservation(config, "none"), config["max_ops"] * config["size"] * 2)

    def test_recovery_preserves_full_interrupted_charge(self):
        token = self.campaign.admit("baseline", 60, 1024 * 1024)
        data = self.path / token / "data"
        data.mkdir(parents=True)
        (data / "fixture").write_bytes(b"abc")
        result = recover(self.campaign)
        self.assertTrue(result["recovered"])
        self.assertFalse(data.exists())
        self.assertIsNone(self.campaign.ledger["active"])
        self.assertGreaterEqual(self.campaign.ledger["spent_seconds"], 60)

    def test_gate_eof_never_executes_child(self):
        sentinel = self.path / "must-not-exist"
        gate = Path(__file__).with_name("processes.py")
        result = subprocess.run([sys.executable, str(gate), "--gate", sys.executable, "-c", f"open({str(sentinel)!r}, 'w').close()"],
                                input=b"", capture_output=True, timeout=5, check=False)
        self.assertEqual(result.returncode, 2)
        self.assertFalse(sentinel.exists())

    def test_process_group_termination_and_bounded_logs(self):
        self.campaign.admit("baseline", 30, 1024 * 1024)
        child = None
        try:
            with patch("processes.OUTPUT_LIMIT", 1024):
                child = Child([sys.executable, "-c", "import os,time; os.write(1,b'x'*65536); time.sleep(10)"],
                              {"PATH": os.environ["PATH"]}, self.path, "fixture", self.campaign)
                deadline = time.monotonic() + 5
                while not child.output_overflow and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(child.output_overflow)
                child.stop(time.monotonic() + 3)
                self.assertFalse(live_group_members(child.process.pid))
                self.assertLessEqual(child.paths[0].stat().st_size, 1024)
                child.stop(time.monotonic() + 1)  # idempotent; no signal to a reused PID
        finally:
            if child:
                child.stop(time.monotonic() + 4)

    def test_exited_leader_keeps_identity_until_reaped(self):
        self.campaign.admit("baseline", 30, 1024 * 1024)
        child = Child([sys.executable, "-c", "print('done')"], {"PATH": os.environ["PATH"]}, self.path, "fixture", self.campaign)
        try:
            deadline = time.monotonic() + 5
            while child.exited() is None and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(child.exited(), 0)
            self.assertTrue(Path(f"/proc/{child.process.pid}/stat").exists())
        finally:
            child.stop(time.monotonic() + 3)

    def test_failed_ownership_persistence_never_leaves_an_executable_child(self):
        self.campaign.admit("baseline", 30, 1024 * 1024)
        sentinel = self.path / "must-not-exist"
        with patch.object(self.campaign, "save", side_effect=OSError("injected ENOSPC")):
            with self.assertRaises(OSError):
                Child([sys.executable, "-c", f"open({str(sentinel)!r}, 'w').close()"],
                      {"PATH": os.environ["PATH"]}, self.path, "fixture", self.campaign)
        self.assertFalse(sentinel.exists())
        self.assertFalse(live_group_members(self.campaign.ledger["active"]["children"][-1]["pgrp"]))

    def test_purge_preserves_ledger_and_compact_results(self):
        token = self.campaign.admit("baseline", 30, 1024 * 1024)
        directory = self.path / token / "artifacts"
        directory.mkdir(parents=True)
        (directory / "fixture.perf").write_bytes(b"fixture")
        result = self.path / f"{token}.result.json"
        result.write_text("{}")
        self.campaign.finish(1, "PASS", clean=True)
        purge_artifacts(self.campaign)
        self.assertFalse(directory.exists())
        self.assertTrue(result.exists())
        self.assertTrue((self.path / "ledger.json").exists())
        self.assertGreater(self.campaign.ledger["spent_seconds"], 1)


class MeasurementTests(unittest.TestCase):
    def test_missing_profiler_is_unavailable(self):
        with patch("lab.shutil.which", return_value=None):
            for profile in ("cpu", "heap"):
                with self.assertRaises(Unavailable):
                    profile_command(profile, Path("."), ["fixture"])

    def test_p99_sample_gate_and_seed(self):
        self.assertIsNone(distribution([0.1] * 9999)["p99_seconds"])
        self.assertEqual(distribution([0.1] * 10_000)["p99_seconds"], 0.1)
        self.assertEqual(payload(1024, 0x5EED), payload(1024, 0x5EED))
        self.assertNotEqual(payload(1024, 1), payload(1024, 2))

    @unittest.skipUnless(os.environ.get("LAB_TEST_SERVER"), "explicit correctness-test binary not supplied")
    def test_real_s3_fixture_and_coordinator_cleanup(self):
        import argparse
        with tempfile.TemporaryDirectory() as temporary:
            campaign = Campaign(Path(temporary) / "campaign", create=True, require_ssd=False)
            args = argparse.Namespace(root=str(campaign.root), binary=os.environ["LAB_TEST_SERVER"],
                commit="correctness-fixture", build_settings="debug correctness fixture; no performance claim",
                layer="s3", concurrency=4, buckets=1, size=1024, seed=0x5eed,
                seconds=1, idle=0, cycles=3, max_ops=6, profile="none", allow_seconds=60,
                phase="baseline", primary_metric="byte-exact fixture", protected="cleanup")
            try:
                # The cap deliberately prevents a performance PASS while the three cycles
                # exercise real signed requests, bootstrap and bounded server teardown.
                self.assertEqual(run_case(args, campaign), 2)
                result = json.loads(next(campaign.root.glob("*.result.json")).read_text())
                self.assertEqual(len(result["cycles"]), 3)
                self.assertTrue(result["cleaned_data_and_processes"])
                self.assertIsNone(campaign.ledger["active"])
            finally:
                campaign.close()


if __name__ == "__main__":
    unittest.main()
