"""Attribution failures preserve campaign accounting and never imply available stacks."""
import argparse
import json
from pathlib import Path
import tempfile
import sys
import time
import unittest
from unittest.mock import patch

from analyze import analyze
from budget import Campaign
from processes import Child


class AttributionTests(unittest.TestCase):
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
