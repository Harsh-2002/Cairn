"""Synthetic decisions and owned-process correctness fixtures; never performance evidence."""
import argparse
import copy
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from budget import Campaign, Unavailable, atomic_json, footprint, remove_tree
from metadata_capacity import (FAMILIES, MATRIX, PHASE_SECONDS, SEED_ROWS, assess, expected_state,
                               reserve_bytes, run_comparison, validate_args)
from lab import clean_environment
from processes import Child, live_group_members


def arguments(**changes):
    args = argparse.Namespace(root="unused", binary="/missing-driver", commit="synthetic-correctness",
                              build_settings="debug fixture; not performance", seed=42, allow_seconds=180)
    vars(args).update(changes)
    return args


def histogram(count):
    return {"count": count, "sum_seconds": count * .001, "max_seconds": .001 if count else 0,
            "p99_seconds_interval": [.00099, .00101] if count >= 10000 else None,
            "bucket_relative_width": .02, "overflow": 0}


def writer(occupancy=9., queue=.9):
    return {"dropped_at_start": 0, "dropped_at_end": 0, "queue_samples": 100,
            "queue_nonempty_samples": round(queue * 100), "queue_max": 32, "peak_wal_bytes": 1000,
            "stages": {name: {"count": 200, "sum_seconds": occupancy / 3,
                               "max_seconds": .02, "failed": 0} for name in ("begin", "apply", "commit")}}


def phase(index, concurrency, throughput=100., occupancy=9., queue=.9):
    return {"phase": index, "concurrency": concurrency, "seconds": 10.,
            "completed_operation_bundles": round(throughput * 10), "bundles_per_second": throughput, "cleanup_claims_lost": 0,
            "families": {family: {"successful": histogram(0 if family == "conditional_reject" else 1000),
                                  "expected_rejected": histogram(1000 if family == "conditional_reject" else 0)}
                         for family in FAMILIES},
            "writer_cpu_seconds": 2., "serialized_observed_seconds": occupancy,
            "stage_samples_complete": True, "writer": writer(occupancy, queue)}


def records(config, *, high_throughput=110., occupancy=9., queue=.9):
    buckets = config["buckets"]
    quota = {name: {"active_uploads": buckets, "staged_bytes": buckets * 64}
             for name in ("multipart_bucket_stats", "multipart_principal_stats")}
    quota["user_logical_bytes"] = 90_000 * 128
    values = [{"event": "start", "schema": 1, "variant": "canonical_writer_full_populated_v1",
               "config": config, "cache": {"read_connections": 8, "kib_per_connection": 8192,
                                             "mmap_bytes": 0, "application_cache": "absent"}},
              {"event": "prepared", "seconds": 1., "expected": expected_state(buckets), "writer": writer()},
              {"event": "seed_verified", "writer": writer(), "quota": quota}]
    phases = []
    for index, concurrency in enumerate(config["concurrency"]):
        report = phase(index, concurrency, high_throughput if concurrency == 128 else 100., occupancy, queue)
        phases.append(report)
        values.extend([{"event": "phase", "report": report},
                       {"event": "phase_verified", "phase": index, "writer": writer(), "quota": quota}])
    sizes = {"sqlite_version": "synthetic", "sqlite_source_id": "synthetic-correctness", "database_file_bytes": 16384, "page_size": 4096, "page_count": 4, "freelist_pages": 0,
             "dbstat": {"available": False, "reason": "synthetic unavailable fixture"}}
    values.append({"event": "result", "status": "complete", "expected": expected_state(buckets),
                   "phases": phases, "before_checkpoint": sizes, "checkpoint_seconds": .1,
                   "post_checkpoint_wal_bytes": 0, "reopen_seconds": .01, "reopen_writer": writer(),
                   "reopen_quota": quota, "final_database": sizes, "total_seconds": 32.,
                   "physical_objects": "not_created_metadata_only"})
    return values


def arms(**changes):
    result = []
    for buckets, concurrency in MATRIX:
        config = {"root": f"/tmp/population-{buckets}", "buckets": buckets, "seed_rows": SEED_ROWS,
                  "seed": 42, "concurrency": list(concurrency), "phase_seconds": PHASE_SECONDS,
                  "deadline_seconds": 200, "ticks_per_second": 100}
        result.append({"config": config, "records": records(config, **changes), "process_sample_count": 100,
                       "process_cpu_seconds": 5., "peak_rss_kib": 1000, "peak_fds": 15})
    return result


class MetadataCapacityTests(unittest.TestCase):
    def test_matrix_reservation_and_strict_bounds(self):
        self.assertGreater(reserve_bytes(), 2 * SEED_ROWS * 32768)
        self.assertLess(reserve_bytes(), 99_000_000_000)
        for change in ({"seed": -1}, {"seed": True}, {"seed": 2**64}, {"allow_seconds": 89},
                       {"allow_seconds": 601}, {"commit": ""}, {"build_settings": ""}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                validate_args(arguments(**change))

    def test_complete_evidence_only_enables_conditional_experiment(self):
        result = assess(arms())
        self.assertEqual(result["status"], "PASS", result)
        self.assertIn("meets the predeclared", result["writer_limit"])
        self.assertIn("KEEP SQLite", result["decision"])
        for changes in ({"high_throughput": 150.}, {"occupancy": 7.}, {"queue": .7}):
            result = assess(arms(**changes))
            self.assertEqual(result["status"], "PASS", result)
            self.assertEqual(result["writer_limit"], "NO demonstrated Writer limit")

    def test_qualifying_single_population_is_scoped_and_hot_c4_control_is_required(self):
        values = arms()
        values[1]["records"] = records(values[1]["config"], occupancy=7.)
        result = assess(values)
        self.assertEqual(result["status"], "PASS", result)
        self.assertEqual(result["qualifying_populations_buckets"], [1])
        self.assertIn("only for qualifying bucket populations [1]", result["decision"])
        values = arms()
        values[0]["records"][11]["report"].update(bundles_per_second=115., completed_operation_bundles=1150)
        self.assertEqual(assess(values)["status"], "INCONCLUSIVE")

    def test_missing_outcome_or_attribution_prevents_conclusion(self):
        changes = [lambda a: a.pop(), lambda a: a[0]["records"].pop(2),
                   lambda a: a[0]["records"][1]["expected"].update(historical_data_rows=0),
                   lambda a: a[0]["records"][2]["quota"]["multipart_bucket_stats"].update(staged_bytes=0),
                   lambda a: a[0]["records"][3]["report"]["families"].pop("multipart_replace"),
                   lambda a: a[0]["records"][3]["report"]["writer"].update(dropped_at_end=1),
                   lambda a: a[0]["records"][3]["report"].update(serialized_observed_seconds=10.),
                   lambda a: a[0]["records"][3]["report"].update(writer_cpu_seconds=float("nan")),
                   lambda a: a[0]["records"][-1].pop("reopen_quota")]
        for change in changes:
            values = copy.deepcopy(arms()); change(values)
            result = assess(values)
            self.assertEqual(result["status"], "INCONCLUSIVE", result)
            self.assertEqual(result["writer_limit"], "NO demonstrated Writer limit")

    def test_drift_and_undersampled_p99_are_inconclusive(self):
        values = arms()
        values[0]["records"][9]["report"].update(bundles_per_second=115., completed_operation_bundles=1150)
        self.assertEqual(assess(values)["status"], "INCONCLUSIVE")
        values = arms()
        values[0]["records"][3]["report"]["families"]["current_read"]["successful"]["p99_seconds_interval"] = [.001, .002]
        self.assertEqual(assess(values)["status"], "INCONCLUSIVE")
        self.assertEqual(assess(arms())["status"], "PASS")

    def test_admission_precedes_discovery(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = Campaign(Path(temporary) / "campaign", create=True, require_ssd=False)
            try:
                before = footprint(campaign.root)
                with patch.object(campaign, "admit", side_effect=Unavailable("exhausted")), \
                        patch("metadata_capacity.manifest") as discover, patch("metadata_capacity.Child") as child:
                    with self.assertRaises(Unavailable): run_comparison(arguments(), campaign)
                    discover.assert_not_called(); child.assert_not_called()
                self.assertEqual(footprint(campaign.root), before)
            finally: campaign.close()

    def test_metadata_cap_cannot_borrow_other_phase_time(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = Campaign(Path(temporary) / "campaign", create=True, require_ssd=False)
            try:
                campaign.ledger["runs"].append({"phase": "metadata", "elapsed_seconds": 421, "reserved_seconds": 421})
                with patch.object(campaign, "admit") as admit, self.assertRaises(Unavailable):
                    run_comparison(arguments(), campaign)
                admit.assert_not_called()
            finally: campaign.close()

    def run_fake(self, output, expected, *, cleanup_failure=False, late_overflow=False):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary); driver = base / "driver"
            driver.write_text("#!/usr/bin/env python3\nimport json,pathlib,sys\n"
                              "c=json.loads(pathlib.Path(sys.argv[1]).read_text())\n"
                              "p=pathlib.Path(c['root']); p.mkdir(); (p/'data').write_text('fixture')\n" + output)
            driver.chmod(0o700)
            campaign = Campaign(base / "campaign", create=True, require_ssd=False)
            import metadata_capacity
            real_child = metadata_capacity.Child
            class LateOverflow(real_child):
                def stop(self, deadline):
                    super().stop(deadline); self.output_overflow = True
            try:
                with redirect_stdout(io.StringIO()), patch("metadata_capacity.manifest", return_value={"fixture": True}), \
                        patch("metadata_capacity.Child", LateOverflow if late_overflow else real_child):
                    args = arguments(root=str(campaign.root), binary=str(driver))
                    if cleanup_failure:
                        with patch("metadata_capacity.remove_tree", side_effect=Unavailable("fixture cleanup failed")):
                            code = run_comparison(args, campaign)
                    else: code = run_comparison(args, campaign)
                report = json.loads(next(campaign.root.glob("*.result.json")).read_text())
                self.assertEqual(code, {"PASS": 0, "FAIL": 1, "INCONCLUSIVE": 2}[expected], report)
                self.assertEqual(report["status"], expected, report)
                self.assertIn("KEEP SQLite", report["decision"])
                self.assertGreater(campaign.ledger["spent_seconds"], 0)
                run = campaign.ledger["active"] if cleanup_failure else campaign.ledger["runs"][-1]
                for child in run["children"]: self.assertFalse(live_group_members(child["pgrp"]))
                self.assertEqual(report["cleaned_data_and_processes"], not cleanup_failure)
                if cleanup_failure: self.assertIsNotNone(campaign.ledger["active"])
                else:
                    self.assertIsNone(campaign.ledger["active"])
                    self.assertFalse((campaign.root / report["id"] / "data").exists())
            finally: campaign.close()

    @unittest.skipUnless(os.environ.get("LAB_TEST_METADATA_DRIVER"),
                         "explicit correctness-test metadata binary not supplied")
    def test_real_tiny_cli_populated_state_and_fresh_reopen(self):
        # This disposable correctness ledger never reads or charges the measurement campaign.
        # The ordinary Child gate still records process ownership before executing the binary.
        with tempfile.TemporaryDirectory(prefix="metadata-cli-correctness-") as temporary:
            base = Path(temporary).resolve()
            campaign = Campaign(base / "campaign", create=True, require_ssd=False)
            child = None
            start = time.monotonic()
            deadline = start + 30
            clean = False
            status = "FAIL"
            try:
                token = campaign.admit("metadata", 30, 32 * 1024**2)
                directory = campaign.root / token
                directory.mkdir(mode=0o700)
                data, artifacts = directory / "data", directory / "artifacts"
                data.mkdir(mode=0o700)
                artifacts.mkdir(mode=0o700)
                scratch = artifacts / "scratch"
                scratch.mkdir(mode=0o700)
                binary = Path(os.environ["LAB_TEST_METADATA_DRIVER"]).resolve(strict=True)
                self.assertTrue(binary.is_file() and os.access(binary, os.X_OK))
                config = {"root": str(data / "store"), "buckets": 1, "seed_rows": 100,
                          "seed": 42, "concurrency": [4], "phase_seconds": 1,
                          "deadline_seconds": 20, "ticks_per_second": os.sysconf("SC_CLK_TCK")}
                config_path = artifacts / "correctness.config.json"
                atomic_json(config_path, config)
                child = Child([str(binary), str(config_path)],
                              {**clean_environment(), "TMPDIR": str(scratch)},
                              artifacts, "metadata-correctness", campaign)
                while child.exited() is None and time.monotonic() < deadline - 5:
                    time.sleep(.02)
                self.assertIsNotNone(child.exited(), "bounded correctness driver did not finish")
                outcome = child.exited()
                child.stop(deadline - 1)
                self.assertEqual(outcome, 0, child.paths[1].read_text())
                self.assertFalse(child.output_error or child.output_overflow)
                self.assertFalse(live_group_members(child.process.pid))
                stream = [json.loads(line) for line in child.paths[0].read_text().splitlines() if line.strip()]
                self.assertEqual([record["event"] for record in stream],
                                 ["start", "prepared", "seed_verified", "phase", "phase_verified", "result"])
                initial, prepared, seed_verified, phase_event, phase_verified, final = stream
                self.assertEqual(initial["schema"], 1)
                self.assertEqual(initial["variant"], "canonical_writer_full_populated_v1")
                self.assertEqual(initial["config"], config)
                self.assertEqual(initial["cache"], {"read_connections": 8, "kib_per_connection": 8192,
                                                    "mmap_bytes": 0, "application_cache": "absent"})
                expected = {"seed_rows": 100, "current_data_rows": 80, "historical_data_rows": 10,
                            "current_delete_markers": 10, "seed_logical_bytes": 90 * 128,
                            "auxiliary_sessions": 1, "auxiliary_parts": 1, "auxiliary_part_bytes": 64}
                self.assertEqual(prepared["expected"], expected)
                self.assertEqual(final["expected"], expected)
                self.assertEqual(final["status"], "complete")
                self.assertEqual(final["physical_objects"], "not_created_metadata_only")
                phase = phase_event["report"]
                self.assertEqual(phase["concurrency"], 4)
                self.assertGreater(phase["completed_operation_bundles"], 0)
                self.assertEqual(phase["completed_operation_bundles"] % 5, 0)
                self.assertEqual(set(phase["families"]), FAMILIES)
                for family, values in phase["families"].items():
                    successful = values["successful"]["count"]
                    rejected = values["expected_rejected"]["count"]
                    if family == "conditional_reject":
                        self.assertEqual(successful, 0)
                        self.assertGreater(rejected, 0)
                    else:
                        self.assertGreater(successful, 0, family)
                        self.assertEqual(rejected, 0, family)
                self.assertEqual(final["phases"], [phase])
                for quota in (seed_verified["quota"], phase_verified["quota"], final["reopen_quota"]):
                    self.assertEqual(quota["user_logical_bytes"], expected["seed_logical_bytes"])
                    for name in ("multipart_bucket_stats", "multipart_principal_stats"):
                        self.assertEqual(quota[name], {"active_uploads": 1, "staged_bytes": 64})
                for sizes in (final["before_checkpoint"], final["final_database"]):
                    self.assertTrue(sizes["sqlite_version"])
                    self.assertTrue(sizes["sqlite_source_id"])
                self.assertEqual(final["post_checkpoint_wal_bytes"], 0)
                self.assertGreaterEqual(final["reopen_seconds"], 0)
                database = data / "store" / "metadata.sqlite3"
                self.assertTrue(database.is_file())
                self.assertFalse(Path(str(database) + "-wal").exists())
                self.assertFalse(Path(str(database) + "-shm").exists())
                status = "PASS"
            finally:
                try:
                    if child is not None:
                        child.stop(deadline)
                    if campaign.ledger["active"] is not None:
                        for name in ("data", "artifacts/scratch"):
                            path = campaign.root / campaign.ledger["active"]["id"] / name
                            if path.exists():
                                remove_tree(path, deadline)
                    clean = True
                finally:
                    if campaign.ledger["active"] is not None:
                        campaign.finish(time.monotonic() - start, status, clean=clean)
                    campaign.close()

    def test_complete_synthetic_owned_stream_and_late_overflow(self):
        harness = str(Path(__file__).resolve().parent)
        script = (f"sys.path.insert(0, {harness!r})\nfrom test_metadata_capacity import records\n"
                  "for record in records(c): print(json.dumps(record))\n")
        self.run_fake(script, "PASS")
        self.run_fake(script, "INCONCLUSIVE", late_overflow=True)

    def test_owned_invalid_failure_and_deadline_outputs_clean_and_charge(self):
        self.run_fake("print('{}')\n", "INCONCLUSIVE")
        self.run_fake("print('{}'); raise SystemExit(7)\n", "FAIL")
        self.run_fake("print(json.dumps({'event':'error','reason':'seed deadline'})); raise SystemExit(2)\n", "INCONCLUSIVE")

    def test_final_drain_overflow_and_cleanup_failure_remain_inconclusive(self):
        self.run_fake("print('{}')\n", "INCONCLUSIVE", late_overflow=True)
        self.run_fake("print('{}')\n", "INCONCLUSIVE", cleanup_failure=True)


if __name__ == "__main__": unittest.main()
