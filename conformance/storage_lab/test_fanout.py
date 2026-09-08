"""Synthetic decision regressions and tiny correctness fixtures, never performance evidence."""
import argparse
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest

from budget import Campaign, Unavailable
from fanout import CASES, assess, reduce_records, reserve_bytes, run_comparison


def arm(case, pair, layout, ratio=1.0):
    scale = ratio if layout == "fanout" else 1.0
    latency = {"count": 20_480, "p50_seconds": scale, "p99_seconds": scale}
    operation = {"wall_seconds": scale, "latencies": [latency, {"count": 0}]}
    return {"case": case, "pair": pair, "layout": layout, "status": "PASS", "sample_count": 3,
            "measurements": {"publish": copy.deepcopy(operation), "read": copy.deepcopy(operation),
                             "delete": copy.deepcopy(operation), "live_scan": {"wall_seconds": scale},
                             "cleanup_scan": {"wall_seconds": scale}, "peak_rss_kib": 100 * scale}}


def paired(ratio=0.75):
    return [arm(case, pair, layout, ratio) for case in CASES for pair in range(5) for layout in ("flat", "fanout")]


class FanoutTests(unittest.TestCase):
    def test_reservation_includes_both_stores_and_inflight_headroom(self):
        largest = max(case["objects"] * case["size"] for case in CASES.values())
        self.assertGreater(reserve_bytes(CASES), 2 * largest + 256 * 1024**2)

    def test_only_complete_repeated_gain_can_propose_promotion(self):
        result = assess(paired(), CASES, 5)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["decision"], "PROPOSE separate promotion review")
        self.assertEqual(assess(paired(0.85), CASES, 5)["decision"], "KEEP flat")
        self.assertEqual(assess(paired(0.85), CASES, 5)["status"], "FAIL")

    def test_protected_regression_and_control_drift_cannot_pass(self):
        arms = paired()
        for value in arms:
            if value["case"] == "large-1m" and value["layout"] == "fanout":
                value["measurements"]["peak_rss_kib"] = 111
        self.assertEqual(assess(arms, CASES, 5)["status"], "FAIL")
        arms = paired()
        arms[0]["measurements"]["live_scan"]["wall_seconds"] *= 1.5
        result = assess(arms, CASES, 5)
        self.assertEqual(result["status"], "INCONCLUSIVE")
        self.assertEqual(result["decision"], "KEEP flat")

    def test_missing_duplicate_or_under_sampled_measurements_are_inconclusive(self):
        for change in ("missing", "duplicate", "samples", "metric", "p99"):
            arms = paired()
            if change == "missing":
                arms.pop()
            elif change == "duplicate":
                arms[-1] = arms[0]
            elif change == "samples":
                arms[0]["sample_count"] = 1
            elif change == "metric":
                del arms[0]["measurements"]["peak_rss_kib"]
            else:
                arms[0]["measurements"]["read"]["latencies"][0]["count"] = 9999
            result = assess(arms, CASES, 5)
            self.assertEqual(result["status"], "INCONCLUSIVE", change)
            self.assertEqual(result["decision"], "KEEP flat", change)
        self.assertEqual(assess(paired()[:2], CASES, 1)["status"], "INCONCLUSIVE")
        hot = [value for value in paired() if value["case"] == "hot-4k"]
        self.assertEqual(assess(hot, {"hot-4k": CASES["hot-4k"]}, 5)["status"], "INCONCLUSIVE")
        altered = copy.deepcopy(CASES)
        altered["large-1m"]["objects"] = 16
        self.assertEqual(assess(paired(), altered, 5)["status"], "INCONCLUSIVE")

    def test_missing_completion_is_not_a_successful_measurement(self):
        with self.assertRaises(Unavailable):
            reduce_records([], {"objects": 16})

    @unittest.skipUnless(os.environ.get("LAB_TEST_FANOUT_DRIVER"), "explicit correctness-test binary not supplied")
    def test_real_tiny_paired_fixture_cleans_processes_data_and_accounts_time(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = Campaign(Path(temporary) / "campaign", create=True, require_ssd=False)
            args = argparse.Namespace(root=str(campaign.root), binary=os.environ["LAB_TEST_FANOUT_DRIVER"],
                                      commit="correctness-fixture", build_settings="debug fixed fixture; no performance claim",
                                      pairs=1, seed=0x5eed, allow_seconds=60, device="fixture-unavailable-device")
            cases = {"tiny": {"objects": 16, "buckets": 2, "size": 1024, "concurrency": 4}}
            try:
                self.assertEqual(run_comparison(args, campaign, cases), 2)
                result = json.loads(next(campaign.root.glob("*.result.json")).read_text())
                self.assertEqual(result["status"], "INCONCLUSIVE")
                self.assertEqual(result["decision"], "KEEP flat")
                self.assertEqual(len(result["arms"]), 2)
                self.assertTrue(result["cleaned_data_and_processes"])
                self.assertFalse((campaign.root / result["id"] / "data").exists())
                self.assertIsNone(campaign.ledger["active"])
                self.assertGreater(campaign.ledger["spent_seconds"], 1)
                for value in result["arms"]:
                    self.assertEqual(value["measurements"]["verify"]["count"], 16)
                    self.assertEqual(value["measurements"]["cleanup_scan"]["reclaimed"], 4)
            finally:
                campaign.close()


if __name__ == "__main__":
    unittest.main()
