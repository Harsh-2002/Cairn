"""Synthetic reducer and tiny owned-process correctness tests; no measured comparison."""
import argparse
import copy
from contextlib import nullcontext, redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from budget import Campaign, Unavailable, footprint
from packing_measure import (CASES, MIB, OBJECTS, TIMINGS, assess, reserve_bytes,
                             run_comparison, validate_args, validate_result)
from processes import live_group_members


def arguments(**changes):
    args = argparse.Namespace(root="unused", binary="/missing", commit="correctness-fixture",
                              build_settings="synthetic correctness only", pairs=5, seed=24301,
                              allow_seconds=30, device="unused")
    vars(args).update(changes)
    return args


def completion(config):
    count = config["objects"]
    packed = config["mode"] == "packed" and config["known_length"] and config["size"] <= MIB
    result = {"status": "PASS", "measurement_protocol": 1, "mode": config["mode"],
              "size": config["size"], "objects": count, "published": count, "verified": count,
              "overwritten": count // 4, "deleted": count // 2, "final_records": count // 2,
              "survivor_verified": count // 2, "restored_verified": count // 2,
              "reopened_verified": count // 2, "range_read_count": count // 2,
              "range_read_bytes": count // 2, "pending_writes": 0, "cleanup_debts": 0,
              "packed_records": count if packed else 0, "dedicated_records": 0 if packed else count,
              "artifact_count": 2 if packed else count, "peak_admitted_bytes": MIB,
              "peak_pending_records": min(count, config["concurrency"]), "peak_rss_kib": 8192,
              "live_physical_bytes": count * config["size"],
              "publication_latency": {"count": count, "sum_seconds": 2., "max_seconds": .1},
              "collection_check": {"completed": True, "copied_records": 4 if packed else 0,
                                   "retired_sources": 2 if packed else 0}}
    result.update({name: .1 for name in TIMINGS})
    result["workload_seconds"] = 1.
    return result


def arms():
    result = []
    for pair in range(5):
        for case, config in CASES.items():
            for mode in ("files", "packed"):
                record = completion({**config, "mode": mode})
                if mode == "packed":
                    for metric in TIMINGS:
                        record[metric] *= .7
                result.append({"case": case, "pair": pair, "mode": mode, "status": "PASS",
                               "measurements": record, "sample_count": 8, "sampled_seconds": .2,
                               "observer_average_cpu_cores": .1})
    return result


class MeasurementTests(unittest.TestCase):
    def test_argument_and_complete_reservation_bounds(self):
        validate_args(arguments())
        for changes in ({"pairs": True}, {"pairs": 4}, {"seed": -1}, {"allow_seconds": 1081}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                validate_args(arguments(**changes))
        size = reserve_bytes(CASES, 5)
        self.assertGreater(size, 4 * OBJECTS * max(case["size"] for case in CASES.values()) + len(CASES) * 10 * 48 * MIB)
        self.assertLess(size, 100_000_000_000 - 1_000_000_000)

    def test_no_comparison_before_campaign_admission(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = Campaign(Path(temporary) / "campaign", create=True, require_ssd=False)
            try:
                before = footprint(campaign.root)
                with patch.object(campaign, "admit", side_effect=Unavailable("exhausted")), \
                        patch("packing_measure.manifest") as discover, patch("packing_measure.Child") as child:
                    with self.assertRaises(Unavailable):
                        run_comparison(arguments(), campaign)
                    discover.assert_not_called()
                    child.assert_not_called()
                self.assertEqual(footprint(campaign.root), before)
            finally:
                campaign.close()

    def test_driver_counts_placement_gc_and_primary_scope_fail_closed(self):
        config = {**CASES["known-1024"], "mode": "packed"}
        for changes in ({"survivor_verified": 1}, {"pending_writes": 1}, {"cleanup_debts": 1},
                        {"packed_records": 0}, {"workload_seconds": .1},
                        {"collection_check": {"completed": True, "copied_records": 0, "retired_sources": 0}},
                        {"peak_admitted_bytes": 33 * MIB}, {"peak_pending_records": 33}):
            record = completion(config)
            record.update(changes)
            with self.subTest(changes=changes), self.assertRaises(RuntimeError):
                validate_result(record, config)
        self.assertIsNotNone(validate_result(completion(config), config))

    def test_missing_nonfinite_and_false_p99_cannot_qualify(self):
        config = {**CASES["known-1024"], "mode": "files"}
        for field in TIMINGS + ("peak_rss_kib", "restored_verified"):
            record = completion(config)
            del record[field]
            with self.subTest(field=field), self.assertRaises(Unavailable):
                validate_result(record, config)
        for value in (float("nan"), float("inf"), True, -1):
            record = completion(config)
            record["range_read_seconds"] = value
            with self.assertRaises(Unavailable):
                validate_result(record, config)
        record = completion(config)
        record["publication_latency"]["p99_seconds"] = .1
        with self.assertRaises(Unavailable):
            validate_result(record, config)

    def test_complete_paired_matrix_selects_full_contiguous_range(self):
        result = assess(arms(), CASES, 5)
        self.assertEqual(result["status"], "PASS", result)
        self.assertEqual(result["threshold_bytes"], MIB)
        self.assertEqual(len(result["comparisons"]), len(CASES))

    def test_gap_stops_range_and_smallest_failure_cannot_be_bypassed(self):
        for size, threshold, status in ((16384, 4096, "PASS"), (1024, None, "FAIL")):
            data = arms()
            for arm in data:
                if arm["case"] == f"known-{size}" and arm["mode"] == "packed":
                    arm["measurements"]["workload_seconds"] = .9
            result = assess(data, CASES, 5)
            self.assertEqual(result["status"], status, result)
            self.assertEqual(result["threshold_bytes"], threshold)

    def test_unknown_streaming_large_range_and_memory_are_protected(self):
        for case, metric in (("unknown-4096", "publication_seconds"), (f"unknown-{MIB}", "range_read_seconds"),
                             (f"known-{2 * MIB}", "peak_rss_kib"), ("known-1024", "restore_seconds")):
            data = arms()
            for arm in data:
                if arm["case"] == case and arm["mode"] == "packed":
                    arm["measurements"][metric] = 10000 if metric == "peak_rss_kib" else .111
            result = assess(data, CASES, 5)
            self.assertEqual(result["status"], "FAIL", (case, metric, result))
            self.assertIsNone(result["threshold_bytes"])

    def test_unselected_large_packing_policy_does_not_reject_a_smaller_threshold(self):
        data = arms()
        for arm in data:
            if arm["mode"] == "packed" and arm["case"] in ("known-262144", f"known-{MIB}"):
                arm["measurements"]["workload_seconds"] = 2.0
                arm["measurements"]["peak_rss_kib"] = 20000
        result = assess(data, CASES, 5)
        self.assertEqual(result["status"], "PASS", result)
        self.assertEqual(result["threshold_bytes"], 65536)
        # The global large control follows the dedicated path in both modes regardless of
        # which contiguous packing threshold qualifies.
        for mode in ("files", "packed"):
            config = {**CASES[f"known-{2 * MIB}"], "mode": mode}
            record = completion(config)
            self.assertEqual(record["dedicated_records"], OBJECTS)
            self.assertEqual(record["packed_records"], 0)
            self.assertIs(validate_result(record, config), record)

    def test_publication_per_request_mean_and_maximum_are_protected(self):
        for field, value in (("sum_seconds", 2.22), ("max_seconds", .111)):
            data = arms()
            for arm in data:
                if arm["case"] == "known-1024" and arm["mode"] == "packed":
                    arm["measurements"]["publication_latency"][field] = value
            result = assess(data, CASES, 5)
            self.assertEqual(result["status"], "FAIL", result)
            self.assertIsNone(result["threshold_bytes"])

    def test_incomplete_duplicate_drifting_or_saturated_evidence_is_inconclusive(self):
        scenarios = []
        data = arms(); data.pop(); scenarios.append(data)
        data = arms(); data[-1] = copy.deepcopy(data[0]); scenarios.append(data)
        data = arms(); data[0]["measurements"]["workload_seconds"] = 1.21; scenarios.append(data)
        data = arms(); data[0]["sample_count"] = 1; scenarios.append(data)
        data = arms(); data[0]["sampled_seconds"] = .09; scenarios.append(data)
        data = arms(); data[0]["observer_average_cpu_cores"] = .5; scenarios.append(data)
        data = arms(); data[0]["measurements"]["collection_check"] = {}; scenarios.append(data)
        for data in scenarios:
            result = assess(data, CASES, 5)
            self.assertEqual(result["status"], "INCONCLUSIVE", result)
            self.assertEqual(result["decision"], "KEEP files")
        self.assertEqual(assess(arms(), CASES, 1)["status"], "INCONCLUSIVE")
        self.assertEqual(assess(arms(), {"known-1024": CASES["known-1024"]}, 5)["status"], "INCONCLUSIVE")

    def test_tiny_owned_driver_cleanup_and_incomplete_protocol(self):
        for body, status in (("print('{}')", "INCONCLUSIVE"), ("raise SystemExit(1)", "FAIL"),
                             ("print('{\"status\":\"FAIL\"}'); raise SystemExit(1)", "FAIL")):
            with self.subTest(body=body), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary)
                binary = path / "fixture.py"
                binary.write_text("#!/usr/bin/env python3\n" + body + "\n")
                binary.chmod(0o700)
                campaign = Campaign(path / "campaign", create=True, require_ssd=False)
                try:
                    with patch("packing_measure.manifest", return_value={}), redirect_stdout(io.StringIO()):
                        run_comparison(arguments(binary=str(binary)), campaign, {"tiny": {"objects": 8,
                                       "size": 1024, "concurrency": 4, "known_length": True}})
                    run = campaign.ledger["runs"][-1]
                    report = json.loads((campaign.root / f"{run['id']}.result.json").read_text())
                    self.assertEqual(report["status"], status, report)
                    self.assertTrue(report["cleaned_data_and_processes"])
                    self.assertIsNone(campaign.ledger["active"])
                    self.assertFalse((campaign.root / run["id"] / "data").exists())
                    for child in run["children"]:
                        self.assertFalse(live_group_members(child["pgrp"]))
                finally:
                    campaign.close()


    def test_completed_tiny_pair_retains_records_and_cleans_actual_data(self):
        parameters = {"objects": 8, "size": 1024, "concurrency": 4, "known_length": True}
        records = {mode: completion({**parameters, "mode": mode}) for mode in ("files", "packed")}
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)
            binary = path / "fixture.py"
            binary.write_text("#!/usr/bin/env python3\nimport json, pathlib, sys, time\n"
                              "config = json.load(open(sys.argv[1]))\n"
                              "root = pathlib.Path(config['root']); root.mkdir()\n"
                              "(root / 'owned-data').write_bytes(b'fixture')\n"
                              f"records = {records!r}\n"
                              "time.sleep(.12)\nprint(json.dumps(records[config['mode']]))\n")
            binary.chmod(0o700)
            campaign = Campaign(path / "campaign", create=True, require_ssd=False)
            try:
                with patch("packing_measure.manifest", return_value={}), redirect_stdout(io.StringIO()):
                    run_comparison(arguments(binary=str(binary), pairs=1), campaign, {"tiny": parameters})
                run = campaign.ledger["runs"][-1]
                report = json.loads((campaign.root / f"{run['id']}.result.json").read_text())
                self.assertEqual(report["status"], "INCONCLUSIVE", report)
                self.assertEqual(len(report["arms"]), 2)
                self.assertTrue(report["cleaned_data_and_processes"])
                self.assertFalse((campaign.root / run["id"] / "data").exists())
                self.assertEqual(len(list((campaign.root / run["id"] / "artifacts").glob('*.result.json'))), 2)
            finally:
                campaign.close()

    @unittest.skipUnless(os.environ.get("LAB_TEST_PACKING_DRIVER"), "explicit correctness-test binary not supplied")
    def test_real_tiny_measurement_pipeline_is_never_a_qualification(self):
        cases = {f"tiny-{known}": {"objects": 16, "size": 1024, "concurrency": 4, "known_length": known}
                 for known in (True, False)}
        with tempfile.TemporaryDirectory() as temporary:
            campaign = Campaign(Path(temporary) / "campaign", create=True, require_ssd=False)
            try:
                with patch("packing_measure.manifest", return_value={}), redirect_stdout(io.StringIO()):
                    run_comparison(arguments(binary=os.environ["LAB_TEST_PACKING_DRIVER"], pairs=1), campaign, cases)
                run = campaign.ledger["runs"][-1]
                report = json.loads((campaign.root / f"{run['id']}.result.json").read_text())
                self.assertEqual(report["status"], "INCONCLUSIVE", report)
                self.assertEqual(report["decision"], "KEEP files")
                self.assertEqual(len(report["arms"]), 4, report)
                for arm in report["arms"]:
                    metrics = arm["measurements"]
                    self.assertEqual(metrics["published"], 16)
                    self.assertEqual(metrics["restored_verified"], 8)
                    self.assertEqual(metrics["reopened_verified"], 8)
                    self.assertEqual(metrics["pending_writes"], 0)
                    self.assertEqual(metrics["cleanup_debts"], 0)
                self.assertTrue(report["cleaned_data_and_processes"])
                self.assertIsNone(campaign.ledger["active"])
                self.assertFalse((campaign.root / run["id"] / "data").exists())
                for child in run["children"]:
                    self.assertFalse(live_group_members(child["pgrp"]))
            finally:
                campaign.close()

    def test_cancelled_owned_process_is_stopped_and_cleanup_failure_blocks_admission(self):
        for fail_cleanup in (False, True):
            with self.subTest(fail_cleanup=fail_cleanup), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary)
                binary = path / "fixture.py"
                binary.write_text("#!/usr/bin/env python3\nimport time\ntime.sleep(60)\n")
                binary.chmod(0o700)
                campaign = Campaign(path / "campaign", create=True, require_ssd=False)
                try:
                    with patch("packing_measure.manifest", return_value={}), \
                            patch("packing_measure.sample", side_effect=KeyboardInterrupt), \
                            (patch("packing_measure.remove_tree", side_effect=Unavailable("fixture cleanup failure"))
                             if fail_cleanup else nullcontext()), redirect_stdout(io.StringIO()):
                        run_comparison(arguments(binary=str(binary), pairs=1), campaign,
                                       {"tiny": {"objects": 8, "size": 1024, "concurrency": 4, "known_length": True}})
                    run = campaign.ledger["active"] if fail_cleanup else campaign.ledger["runs"][-1]
                    report = json.loads((campaign.root / f"{run['id']}.result.json").read_text())
                    self.assertEqual(report["status"], "INCONCLUSIVE" if fail_cleanup else "CANCELLED", report)
                    self.assertEqual(report["cleaned_data_and_processes"], not fail_cleanup)
                    self.assertEqual(campaign.ledger["active"] is not None, fail_cleanup)
                    for child in run["children"]:
                        self.assertFalse(live_group_members(child["pgrp"]))
                    if fail_cleanup:
                        with self.assertRaises(Unavailable):
                            campaign.admit("packing", 30, 1024)
                finally:
                    campaign.close()


if __name__ == "__main__":
    unittest.main()
