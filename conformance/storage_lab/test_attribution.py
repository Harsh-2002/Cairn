"""Attribution failures preserve campaign accounting and never imply available stacks."""
import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import tempfile
import sys
import time
import unittest
from unittest.mock import patch

from analyze import analyze
from budget import Campaign, footprint
from processes import Child
from lab import metrics_sample
from summarize import cpu_cores, device_summary, heap_phase


class AttributionTests(unittest.TestCase):
    def test_heap_idle_alignment_excludes_adjacent_load_and_teardown(self):
        points = [{"seconds": t, "bytes": b} for t, b in [(3.01, 1000), (3.5, 100), (4.7, 110), (4.99, 2000)]]
        result = heap_phase(points, {"unix_seconds": 103, "phase": "idle", "cycle": 0}, 100, {"idle": 2})
        self.assertEqual(result["last"]["bytes"], 110)
        self.assertEqual(result["samples"], 2)

    def test_auxiliary_metrics_timeout_is_recorded_without_discarding_trace(self):
        with patch("lab.metrics", side_effect=TimeoutError("timed out")) as metrics:
            self.assertEqual(metrics_sample(1), {"metrics_unavailable": "TimeoutError: timed out"})
            metrics.assert_called_once_with(1)

    def test_cpu_samples_require_same_process_and_device_units_are_explicit(self):
        first = {"identity": {"pid": 1}, "cpu_ticks": 100, "unix_seconds": 10}
        last = {"identity": {"pid": 1}, "cpu_ticks": 200, "unix_seconds": 12}
        with patch("summarize.os.sysconf", return_value=100):
            self.assertEqual(cpu_cores([first, last]), .5)
            self.assertIsNone(cpu_cores([first, {**last, "identity": {"pid": 2}}]))
        samples = [{"driver": {"monotonic": 10, "host": {"diskstats": "8 1 sdb1 0 0 0 0 0 0 0 0 0 0 0"}}},
                   {"driver": {"monotonic": 12, "host": {"diskstats": "8 1 sdb1 10 0 2048 20 20 0 4096 40 0 100 200"}}}]
        device = device_summary(samples, "driver", "sdb1")
        self.assertEqual(device["read_mib_per_second"], .5)
        self.assertEqual(device["write_mib_per_second"], 1)
        self.assertEqual(device["read_write_await_ms"], 2)
        self.assertEqual(device["busy_fraction"], .05)
        self.assertEqual(device["average_queue"], .1)

    def test_footprint_tolerates_staging_unlink_after_directory_read(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            staging = root / "finished.tmp"
            staging.write_bytes(b"payload")
            original = os.scandir

            @contextmanager
            def raced_scan(directory):
                with original(directory) as entries:
                    entries = list(entries)
                staging.unlink()
                yield iter(entries)

            with patch("budget.os.scandir", raced_scan):
                self.assertEqual(footprint(root), 0)

    def test_target_shutdown_allows_wrapper_to_flush_before_group_teardown(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = Campaign(Path(temporary) / "campaign", create=True, require_ssd=False)
            token = campaign.admit("baseline", 30, 1024)
            directory = campaign.root / token
            directory.mkdir()
            target = directory / "target.py"
            target.write_text("import signal, time\nfrom pathlib import Path\n"
                              "signal.signal(signal.SIGTERM, lambda *_: exit(0))\n"
                              "Path('ready').touch()\nwhile True: time.sleep(.01)\n")
            wrapper = directory / "wrapper.sh"
            wrapper.write_text('"$1" target.py\nresult=$?\nsleep .1\necho flushed > complete\nexit "$result"\n')
            child = Child(["/bin/sh", str(wrapper), sys.executable], {}, directory, "profile", campaign)
            deadline = time.monotonic() + 5
            try:
                while not (directory / "ready").exists():
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(.01)
                child.finish_profiled_target(Path(sys.executable).resolve(), deadline)
                self.assertEqual((directory / "complete").read_text(), "flushed\n")
                self.assertEqual(child.exited(), 0)
            finally:
                child.stop(time.monotonic() + 4)
                campaign.finish(1, "PASS", clean=True)
                campaign.close()

    def test_missing_profile_is_inconclusive_and_analysis_time_is_charged(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = Campaign(Path(temporary) / "campaign", create=True, require_ssd=False)
            source = campaign.admit("baseline", 30, 1024)
            (campaign.root / source / "artifacts").mkdir(parents=True)
            campaign.finish(1, "INCONCLUSIVE", clean=True)
            args = argparse.Namespace(run_id=source, phase="baseline", profile="heap", allow_seconds=10)
            try:
                with patch("analyze.Child") as child:
                    self.assertEqual(analyze(args, campaign), 2)
                    child.assert_not_called()
                self.assertGreater(campaign.ledger["spent_seconds"], 1)
                self.assertIsNone(campaign.ledger["active"])
                report = json.loads(next(campaign.root.glob("*.result.json")).read_text())
                self.assertEqual(report["status"], "INCONCLUSIVE")
                self.assertTrue((campaign.root / source).exists())
            finally:
                campaign.close()


if __name__ == "__main__":
    unittest.main()
