"""Offline regressions for the Cairn-only single-key DELETE writer attribution."""

import unittest

from delete_single_key_cairn import counter_delta, stage_report


def exposition(**values):
    """Build the parsed `name -> value` map a scrape would produce."""
    return dict(values)


class CounterDeltaTests(unittest.TestCase):
    def test_delta_is_the_cumulative_difference(self):
        self.assertEqual(counter_delta(exposition(a=3.0), exposition(a=10.0), "a"), 7.0)

    def test_a_series_that_first_appears_in_the_window_starts_at_zero(self):
        # The DELETE-labelled request histogram is created by the first scored request, so its
        # absence before the window is a zero start rather than missing evidence.
        self.assertEqual(counter_delta(exposition(), exposition(a=10.0), "a"), 10.0)
        self.assertEqual(counter_delta(None, exposition(a=10.0), "a"), 10.0)

    def test_a_series_missing_after_the_window_is_not_a_delta(self):
        self.assertIsNone(counter_delta(exposition(a=3.0), exposition(), "a"))
        self.assertIsNone(counter_delta(exposition(a=3.0), exposition(a=10.0), "c"))


class StageReportTests(unittest.TestCase):
    def test_stage_means_divide_by_the_sampled_count(self):
        before = exposition(
            **{
                'cairn_writer_stage_seconds_sum{stage="apply",result="ok"}': 1.0,
                'cairn_writer_stage_seconds_count{stage="apply",result="ok"}': 10.0,
                'cairn_writer_stage_seconds_sum{stage="commit",result="ok"}': 2.0,
                'cairn_writer_stage_seconds_count{stage="commit",result="ok"}': 4.0,
                "cairn_writer_commit_seconds_sum": 0.4,
                "cairn_writer_commit_seconds_count": 4.0,
                "cairn_writer_batch_size_sum": 28.0,
                "cairn_writer_batch_size_count": 4.0,
            }
        )
        after = exposition(
            **{
                'cairn_writer_stage_seconds_sum{stage="apply",result="ok"}': 1.5,
                'cairn_writer_stage_seconds_count{stage="apply",result="ok"}': 15.0,
                "cairn_writer_commit_seconds_sum": 1.4,
                "cairn_writer_commit_seconds_count": 8.0,
                "cairn_writer_batch_size_sum": 60.0,
                "cairn_writer_batch_size_count": 8.0,
            }
        )
        report = stage_report(before, after)
        self.assertEqual(report["apply"]["samples"], 5.0)
        self.assertAlmostEqual(report["apply"]["mean_ms"], 100.0)
        self.assertEqual(report["commit"]["commits"], 4.0)
        self.assertAlmostEqual(report["commit"]["mean_ms"], 250.0)
        self.assertAlmostEqual(report["batch"]["mean_size"], 8.0)
        # `commit` also had no stage-tagged series in this fixture, so it is derived only from the
        # commit histogram and no separate stage entry appears.
        self.assertEqual(set(report), {"apply", "commit", "batch"})

    def test_error_samples_are_not_counted_as_successful_stage_work(self):
        before = exposition(
            **{
                'cairn_writer_stage_seconds_sum{stage="apply",result="ok"}': 1.0,
                'cairn_writer_stage_seconds_count{stage="apply",result="ok"}': 2.0,
                'cairn_writer_stage_seconds_sum{stage="apply",result="error"}': 9.0,
                'cairn_writer_stage_seconds_count{stage="apply",result="error"}': 3.0,
            }
        )
        after = exposition(
            **{
                'cairn_writer_stage_seconds_sum{stage="apply",result="ok"}': 2.0,
                'cairn_writer_stage_seconds_count{stage="apply",result="ok"}': 4.0,
                'cairn_writer_stage_seconds_sum{stage="apply",result="error"}': 90.0,
                'cairn_writer_stage_seconds_count{stage="apply",result="error"}': 30.0,
            }
        )
        report = stage_report(before, after)
        self.assertEqual(report["apply"]["samples"], 2.0)
        self.assertAlmostEqual(report["apply"]["mean_ms"], 500.0)

    def test_absent_summary_series_produce_no_stage_entry(self):
        self.assertEqual(stage_report(exposition(), exposition()), {})


if __name__ == "__main__":
    unittest.main()
