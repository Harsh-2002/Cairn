"""Offline regressions for the exact single-key DELETE comparison."""

import unittest

from delete_single_key import key_name, latency_summary, pair_summary


class LatencySummaryTests(unittest.TestCase):
    def test_percentiles_follow_the_repository_scan_convention(self):
        summary = latency_summary([0.004, 0.001, 0.003, 0.002])
        self.assertEqual(summary["count"], 4)
        self.assertAlmostEqual(summary["mean_ms"], 2.5)
        self.assertAlmostEqual(summary["p50_ms"], 2.0)
        self.assertAlmostEqual(summary["p90_ms"], 3.0)
        self.assertAlmostEqual(summary["p99_ms"], 3.0)
        self.assertAlmostEqual(summary["max_ms"], 4.0)

    def test_empty_sample_is_explicit_not_a_zero(self):
        self.assertEqual(latency_summary([]), {"count": 0})


class PairSummaryTests(unittest.TestCase):
    @staticmethod
    def arm(pair, engine, rate, status="PASS"):
        return {"pair": pair, "engine": engine, "objects_per_second": rate,
                "status": status}

    def test_pairs_join_by_declared_pair_not_arm_order(self):
        arms = [
            self.arm(1, "rustfs", 50.0),
            self.arm(1, "cairn", 100.0),
            self.arm(2, "cairn", 80.0),
            self.arm(2, "rustfs", 40.0),
        ]
        pairs = pair_summary(arms)
        self.assertEqual([item["pair"] for item in pairs], [1, 2])
        self.assertAlmostEqual(pairs[0]["cairn_over_rustfs"], 2.0)
        self.assertAlmostEqual(pairs[1]["cairn_over_rustfs"], 2.0)

    def test_failed_or_incomplete_pairs_are_not_scored(self):
        arms = [
            self.arm(1, "cairn", 100.0),
            self.arm(1, "rustfs", 10.0, "INCONCLUSIVE"),
            self.arm(2, "cairn", 100.0),
        ]
        self.assertEqual(pair_summary(arms), [])

    def test_zero_or_missing_rate_is_not_a_ratio(self):
        arms = [
            self.arm(1, "cairn", 100.0),
            self.arm(1, "rustfs", 0.0),
            self.arm(2, "cairn", None),
            self.arm(2, "rustfs", 50.0),
        ]
        self.assertEqual(pair_summary(arms), [])


class KeyNameTests(unittest.TestCase):
    def test_keys_are_distinct_and_fixed_width(self):
        names = [key_name(index) for index in (0, 1, 9_999)]
        self.assertEqual(len(set(names)), len(names))
        self.assertTrue(all(len(name) == len("single-000000000") for name in names))
        self.assertEqual(names[0], "single-000000000")
        self.assertEqual(names[1], "single-000000001")


if __name__ == "__main__":
    unittest.main()
