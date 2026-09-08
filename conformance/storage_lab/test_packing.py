"""Fixed correctness fixtures for the packing coordinator, never performance evidence."""
import argparse
import copy
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from budget import Campaign, Unavailable, footprint
from packing import (ADMISSION_BYTES, MIB, PACK_THRESHOLD, reserve_bytes, run_arm,
                     validate_args, validate_result)
from processes import live_group_members


def arguments(**changes):
    args = argparse.Namespace(root="unused", binary="/missing-driver", commit="correctness-fixture",
                              build_settings="fixed correctness test; no performance claim", mode="packed",
                              size=1024, objects=16, concurrency=4, seed=0x5eed, known_length=True,
                              allow_seconds=60)
    vars(args).update(changes)
    return args


def completion(args=None):
    args = args or arguments()
    dedicated = args.mode == "files" or not args.known_length or args.size > PACK_THRESHOLD
    return {"status": "PASS", "mode": args.mode, "size": args.size, "objects": args.objects,
            "published": args.objects, "verified": args.objects,
            "artifact_count": args.objects if dedicated else 1,
            "packed_records": 0 if dedicated else args.objects,
            "dedicated_records": args.objects if dedicated else 0,
            "peak_admitted_bytes": min(ADMISSION_BYTES, args.size * min(args.concurrency, args.objects)),
            "peak_pending_records": min(args.concurrency, args.objects), "publication_seconds": .1,
            "publication_latency": {"count": args.objects, "sum_seconds": .2, "max_seconds": .02}}


class PackingTests(unittest.TestCase):
    def test_configuration_boundaries_are_strict_and_unknown_length_is_explicit(self):
        for args in (arguments(size=1, objects=1, concurrency=1, seed=0, allow_seconds=30),
                     arguments(size=4 * MIB, objects=16_384, concurrency=128, seed=2**64 - 1,
                               known_length=False, allow_seconds=150)):
            validate_args(args)
        for change in ({"size": 0}, {"size": 4 * MIB + 1}, {"objects": 0}, {"objects": 16_385},
                       {"objects": True}, {"concurrency": 2}, {"seed": -1}, {"seed": 2**64},
                       {"known_length": 1}, {"allow_seconds": 29}, {"allow_seconds": 151}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                validate_args(arguments(**change))

    def test_reservation_includes_database_wal_memory_logs_and_headroom(self):
        args = arguments(size=4 * MIB, objects=16_384, concurrency=128)
        self.assertGreater(reserve_bytes(args), args.objects * (args.size + 256 * 1024) + ADMISSION_BYTES)
        self.assertLess(reserve_bytes(args), 100_000_000_000 - 1_000_000_000)

    def test_admission_failure_precedes_manifest_binary_discovery_and_workload_data(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = Campaign(Path(temporary) / "campaign", create=True, require_ssd=False)
            try:
                before = footprint(campaign.root)
                with patch.object(campaign, "admit", side_effect=Unavailable("budget exhausted")), \
                        patch("packing.manifest") as discover, patch("packing.Child") as child:
                    with self.assertRaises(Unavailable):
                        run_arm(arguments(), campaign)
                    discover.assert_not_called()
                    child.assert_not_called()
                self.assertEqual(footprint(campaign.root), before)
                self.assertIsNone(campaign.ledger["active"])
                self.assertFalse(list(campaign.root.glob("*.result.json")))
            finally:
                campaign.close()

    def test_additive_fields_and_all_dedicated_fallbacks_are_valid(self):
        for args in (arguments(), arguments(mode="files"), arguments(known_length=False),
                     arguments(size=PACK_THRESHOLD), arguments(size=PACK_THRESHOLD + 1)):
            record = completion(args)
            record["future_measurement"] = {"value": 1}
            self.assertIs(validate_result(record, args), record)

    def test_incomplete_counts_wrong_placement_and_excess_memory_cannot_pass(self):
        for change in ({"verified": 15}, {"published": 15}, {"mode": "files"},
                       {"packed_records": 15}, {"artifact_count": 0}, {"artifact_count": 17},
                       {"packed_records": 0, "dedicated_records": 16},
                       {"peak_admitted_bytes": ADMISSION_BYTES + 1},
                       {"peak_admitted_bytes": 0}, {"peak_pending_records": 0},
                       {"peak_pending_records": 5}):
            record = completion()
            record.update(change)
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                validate_result(record, arguments())
        args = arguments(known_length=False)
        record = completion(args)
        record["artifact_count"] = 1
        with self.assertRaises(RuntimeError):
            validate_result(record, args)

    def test_missing_or_nonfinite_measurements_remain_inconclusive(self):
        changes = ({"peak_pending_records": True}, {"publication_seconds": float("nan")},
                   {"publication_seconds": float("inf")}, {"publication_seconds": -1},
                   {"publication_latency": {"count": 15}}, {"status": "INCONCLUSIVE"})
        for change in changes:
            record = completion()
            record.update(change)
            with self.subTest(change=change), self.assertRaises(Unavailable):
                validate_result(record, arguments())
        for field in completion():
            record = completion()
            del record[field]
            with self.subTest(missing=field), self.assertRaises((RuntimeError, Unavailable)):
                validate_result(record, arguments())

    def run_fixture(self, script, expected, *, late_overflow=False, cleanup_failure=False):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            driver = base / "driver"
            driver.write_text("#!/usr/bin/env python3\n" + script)
            driver.chmod(0o700)
            campaign = Campaign(base / "campaign", create=True, require_ssd=False)
            args = arguments(root=str(campaign.root), binary=str(driver))
            import packing
            original_child = packing.Child

            class LateOverflowChild(original_child):
                def stop(self, deadline):
                    super().stop(deadline)
                    self.output_overflow = True

            try:
                with patch("packing.manifest", return_value={"fixture": True}), redirect_stdout(io.StringIO()), \
                        patch("packing.Child", LateOverflowChild if late_overflow else original_child):
                    if cleanup_failure:
                        with patch("packing.remove_tree", side_effect=Unavailable("fixture cleanup failure")):
                            code = run_arm(args, campaign)
                    else:
                        code = run_arm(args, campaign)
                report = json.loads(next(campaign.root.glob("*.result.json")).read_text())
                self.assertEqual(code, {"PASS": 0, "FAIL": 1, "INCONCLUSIVE": 2, "CANCELLED": 130}[expected], report)
                self.assertEqual(report["status"], expected, report)
                self.assertIn("NO adoption decision", report["decision"])
                self.assertGreater(campaign.ledger["spent_seconds"], 0)
                run = campaign.ledger["active"] if cleanup_failure else campaign.ledger["runs"][-1]
                for child in run["children"]:
                    self.assertFalse(live_group_members(child["pgrp"]))
                if cleanup_failure:
                    self.assertFalse(report["cleaned_data_and_processes"])
                    self.assertIsNotNone(campaign.ledger["active"])
                    with self.assertRaises(Unavailable):
                        campaign.admit("packing", 30, 1024)
                else:
                    self.assertTrue(report["cleaned_data_and_processes"])
                    self.assertIsNone(campaign.ledger["active"])
                    self.assertFalse((campaign.root / report["id"] / "data").exists())
                    self.assertFalse((campaign.root / report["id"] / "artifacts" / "scratch").exists())
                return report
            finally:
                campaign.close()

    def test_owned_driver_pass_and_failure_paths_cleanup_and_charge(self):
        record = completion()
        preparation = "import json, pathlib, sys\nc = json.loads(pathlib.Path(sys.argv[1]).read_text())\np = pathlib.Path(c['root'])\np.mkdir()\n(p / 'fixture').write_bytes(b'fixture')\n"
        self.run_fixture(preparation + f"print({json.dumps(json.dumps(record))})\n", "PASS")
        self.run_fixture(preparation + "raise SystemExit(7)\n", "FAIL")
        self.run_fixture(preparation + "print('not json')\n", "INCONCLUSIVE")
        wrong = copy.deepcopy(record)
        wrong["verified"] -= 1
        self.run_fixture(preparation + f"print({json.dumps(json.dumps(wrong))})\n", "FAIL")
        self.run_fixture(preparation + f"print({json.dumps(json.dumps(record))})\n" * 2, "INCONCLUSIVE")

    def test_explicit_driver_deadline_is_inconclusive_and_cleanup_is_complete(self):
        self.run_fixture('import json\nprint(json.dumps({"status": "INCONCLUSIVE", "reason": "packing diagnostic deadline reached"}))\nraise SystemExit(2)\n', "INCONCLUSIVE")
        self.run_fixture('print("{}")\nraise SystemExit(2)\n', "FAIL")

    def test_final_drain_overflow_cannot_report_pass(self):
        self.run_fixture(f"print({json.dumps(json.dumps(completion()))})\n", "INCONCLUSIVE", late_overflow=True)

    def test_incomplete_cleanup_retains_admission_fence(self):
        self.run_fixture(f"print({json.dumps(json.dumps(completion()))})\n", "INCONCLUSIVE", cleanup_failure=True)

    def test_deadline_stops_owned_child_and_removes_data(self):
        import packing
        original_footprint = packing.footprint
        observations = 0

        def exhausted(root):
            nonlocal observations
            observations += 1
            if observations >= 3:
                raise Unavailable("runtime allowance reached; preserving cleanup time")
            return original_footprint(root)

        with patch("packing.footprint", side_effect=exhausted):
            self.run_fixture("import time\ntime.sleep(60)\n", "INCONCLUSIVE")

    @unittest.skipUnless(os.environ.get("LAB_TEST_PACKING_DRIVER"), "explicit correctness-test binary not supplied")
    def test_real_tiny_files_packed_and_unknown_length_fallback(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = Campaign(Path(temporary) / "campaign", create=True, require_ssd=False)
            try:
                for mode, known_length in (("files", True), ("packed", True), ("packed", False)):
                    args = arguments(root=str(campaign.root), binary=os.environ["LAB_TEST_PACKING_DRIVER"],
                                     mode=mode, known_length=known_length)
                    with redirect_stdout(io.StringIO()):
                        code = run_arm(args, campaign)
                    run = campaign.ledger["runs"][-1]
                    report = json.loads((campaign.root / f"{run['id']}.result.json").read_text())
                    self.assertEqual(code, 0, report)
                    self.assertEqual(report["measurements"]["verified"], 16)
                    self.assertEqual(report["measurements"]["published"], 16)
                    self.assertTrue(report["cleaned_data_and_processes"])
                    self.assertIsNone(campaign.ledger["active"])
                    self.assertFalse((campaign.root / run["id"] / "data").exists())
                    for child in run["children"]:
                        self.assertFalse(live_group_members(child["pgrp"]))
            finally:
                campaign.close()


if __name__ == "__main__":
    unittest.main()
