#!/usr/bin/env python3
"""Pure-helper regressions for the replication throughput benchmark; no server or load."""

import unittest

from replication_perf import (
    load_window,
    parse_bootstrap,
    parse_prometheus,
    parse_warp,
    percentile,
    plain_metrics,
    rate,
    redact_command,
    summarize_arms,
    summarize_samples,
)


PROMETHEUS = """\
# HELP cairn_replication_bytes_total Total logical bytes
# TYPE cairn_replication_bytes_total counter
cairn_replication_bytes_total 1048576
cairn_replication_completed_total 12
cairn_replication_lag_seconds 0.75
cairn_replication_pending{target="arn:aws:s3:::replbench"} 4
cairn_replication_pending{target="env-default"} 0
cairn_overlay_pending 0
cairn_unrelated_total 99
"""


class PrometheusTests(unittest.TestCase):
    def test_unlabeled_and_labeled_series_are_separated(self):
        series = parse_prometheus(PROMETHEUS, (
            "cairn_replication_bytes_total",
            "cairn_replication_completed_total",
            "cairn_replication_lag_seconds",
            "cairn_replication_pending",
            "cairn_overlay_pending",
        ))
        self.assertEqual(series["cairn_replication_bytes_total"], 1048576)
        self.assertEqual(series["cairn_replication_completed_total"], 12)
        self.assertEqual(series["cairn_replication_lag_seconds"], 0.75)
        self.assertEqual(
            series['cairn_replication_pending{target="arn:aws:s3:::replbench"}'], 4)
        self.assertEqual(series['cairn_replication_pending{target="env-default"}'], 0)
        self.assertEqual(series["cairn_overlay_pending"], 0)
        self.assertNotIn("cairn_unrelated_total", series)

    def test_plain_collapse_keeps_last_labeled_value_per_family(self):
        plain = plain_metrics(parse_prometheus(PROMETHEUS, ("cairn_replication_pending",)))
        self.assertEqual(plain["cairn_replication_pending"], 0)

    def test_malformed_and_non_finite_lines_are_ignored(self):
        text = "cairn_replication_bytes_total not-a-number\n" \
               "cairn_replication_completed_total NaN\n" \
               "garbage-without-a-space\n" \
               "cairn_replication_failed_total 3\n"
        series = parse_prometheus(text, ("cairn_replication_bytes_total",
                                        "cairn_replication_completed_total",
                                        "cairn_replication_failed_total"))
        self.assertEqual(series, {"cairn_replication_failed_total": 3.0})


class MathTests(unittest.TestCase):
    def test_nearest_rank_percentile(self):
        values = [5.0, 1.0, 4.0, 2.0, 3.0]
        self.assertEqual(percentile(values, 0.95), 5.0)
        self.assertEqual(percentile(values, 0.5), 3.0)
        self.assertIsNone(percentile([], 0.95))

    def test_rate_rejects_zero_denominator(self):
        self.assertEqual(rate(10, 2), 5)
        self.assertIsNone(rate(10, 0))

    def test_load_window_undoes_warp_boundary_trim_and_clamps_to_wall(self):
        self.assertEqual(load_window(11.24, 5.0), 9.0)
        self.assertEqual(load_window(5.0, 5.0), 5.0)
        self.assertEqual(load_window(7.5, None), 7.5)


class WarpReportTests(unittest.TestCase):
    def test_command_redaction_never_keeps_a_secret(self):
        command = ["warp", "put", "--access-key", "perftarget1", "--secret-key",
                   "deadbeef", "--bucket", "replbench"]
        redacted = redact_command(command)
        self.assertNotIn("perftarget1", redacted)
        self.assertNotIn("deadbeef", redacted)
        self.assertEqual(redacted[-2:], ["--bucket", "replbench"])

    def test_put_report_is_parsed_from_the_put_block(self):
        output = """
Report: PUT. Concurrency: 16. Ran: 29s
 * Average: 236.70 MiB/s, 236.70 obj/s
 * Reqs: Avg: 67.1ms, 50%: 60.2ms, 90%: 92.4ms, 99%: 141.7ms
 * Errors: 0

Throughput, split into 29 x 1s:
 * Fastest: 310.1MiB/s, 310.10 obj/s
"""
        parsed = parse_warp(output)
        self.assertEqual(parsed["objects_per_second"], 236.70)
        self.assertEqual(parsed["mib_per_second"], 236.70)
        self.assertEqual(parsed["ran_seconds"], 29.0)
        self.assertEqual(parsed["reported_errors"], 0)
        self.assertEqual(parsed["request_latency_ms"]["p50"], 60.2)
        self.assertEqual(parsed["request_latency_ms"]["p99"], 141.7)

    def test_errors_and_missing_report_are_visible(self):
        parsed = parse_warp("some prepare failure\n* Errors: 3\n")
        self.assertIsNone(parsed["objects_per_second"])
        self.assertEqual(parsed["reported_errors"], 3)


class BootstrapTests(unittest.TestCase):
    def test_credentials_parse_without_echoing_them(self):
        akid, secret = parse_bootstrap(
            "  Access Key Id: perftarget0123456789abcdef\n"
            "  Secret Access Key: 0123456789abcdef0123456789abcdef0123456789abcdef\n")
        self.assertTrue(akid.startswith("perftarget"))
        self.assertEqual(len(secret), 48)

    def test_missing_credentials_fail_closed(self):
        with self.assertRaises(ValueError):
            parse_bootstrap("Access Key Id: only-one-half\n")


class SampleSummaryTests(unittest.TestCase):
    def test_lag_and_unreplicated_use_live_api_not_the_15s_gauge(self):
        samples = [
            {"phase": "load", "lag_s": 1, "pending": 2, "claimed": 1, "failed": 0,
             "metrics": {"cairn_replication_lag_seconds": 0,
                         "cairn_replication_queue_depth": 99,
                         "cairn_replication_unreplicated": 0}},
            {"phase": "load", "lag_s": 5, "pending": 0, "claimed": 0, "failed": 0,
             "metrics": {"cairn_replication_lag_seconds": 0,
                         "cairn_replication_queue_depth": 99,
                         "cairn_replication_unreplicated": 0}},
            {"phase": "drain", "lag_s": 0, "pending": 0, "claimed": 0, "failed": 0,
             "metrics": {}},
        ]
        summary = summarize_samples(samples, "load")
        self.assertEqual(summary["samples"], 2)
        self.assertEqual(summary["lag_max_s"], 5)
        self.assertEqual(summary["lag_p95_s"], 5)
        self.assertEqual(summary["unreplicated_max"], 3)
        self.assertEqual(summary["lag_metric_max_s"], 0)
        self.assertEqual(summary["queue_depth_metric_max"], 99)


class ArmSummaryTests(unittest.TestCase):
    @staticmethod
    def arm(index, pair, kind, put_rate, shipped_rate):
        return {
            "index": index, "pair": pair, "arm": kind, "shape": "1m",
            "source_put": {"obj_s": put_rate, "mib_s": put_rate},
            "warp": {"request_latency_ms": {"p50": 1.0, "p99": 2.0}},
            "ship": {
                "shipped_obj_s": shipped_rate, "shipped_mib_s": shipped_rate,
                "drain_seconds": 1.0,
                "drain_shipped_obj_s": shipped_rate,
                "drain_shipped_mib_s": shipped_rate,
                "live": {"lag_max_s": 2, "lag_p95_s": 1, "unreplicated_max": 10},
            },
            "cpu": {"source_cpu_s_per_shipped_mib": 0.1,
                    "source_total_cpu_s_per_shipped_mib": 0.1,
                    "target_cpu_s_per_shipped_mib": 0.05,
                    "target_total_cpu_s_per_shipped_mib": 0.05,
                    "source_cpu_s_per_put": 0.01,
                    "source_total_cpu_s_per_put": 0.01},
        }

    def test_shapes_and_pairs_are_not_pooled(self):
        arms = [
            self.arm(1, 1, "control", 200, 0),
            self.arm(2, 1, "replicated", 180, 180),
            self.arm(3, 2, "replicated", 190, 190),
            self.arm(4, 2, "control", 210, 0),
        ]
        arms.append({**self.arm(5, 1, "control", 50, 0), "shape": "64m"})
        summary = summarize_arms(arms)
        self.assertEqual(summary["1m"]["arms"], 4)
        self.assertEqual(summary["1m"]["pairs"][0]["control_first"], True)
        self.assertEqual(summary["1m"]["pairs"][1]["control_first"], False)
        self.assertAlmostEqual(
            summary["1m"]["replicated_over_control_put_ratio_mean"], (0.9 + 190 / 210) / 2)
        self.assertAlmostEqual(summary["1m"]["control"]["source_put_obj_s_mean"], 205.0)
        self.assertAlmostEqual(
            summary["1m"]["replicated_minus_control_total_cpu_s_per_put_mean"], 0.0)
        self.assertEqual(summary["1m"]["replicated"]["unreplicated_max"], 10)
        self.assertEqual(summary["64m"]["arms"], 1)


if __name__ == "__main__":
    unittest.main()
