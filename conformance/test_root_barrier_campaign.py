"""Offline tests for one-ledger metering; no server or Warp process is started."""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from root_barrier_campaign import (DIAGNOSTIC_PHASES, PHASES, PUT_TIMING_PHASES,
                                   PUT_TIMING_STAGES, admissible_wall_cap, main,
                                   phase_client_seconds, phase_passes,
                                   put_timing_metrics_valid)
from rustfs_compare import BUCKET, CASES, STRICT_SETTINGS, host_capacity, sha256


def fixture_report(phase, identities, duration=12, drain=16, capacity=None):
    _, kind, case, pairs, _ = phase
    case_info = next(item for item in CASES if item[0] == case)
    capacity = capacity or host_capacity()
    report = {"pairs": pairs, "duration": duration, "metrics_drain_seconds": drain,
              "host_capacity": capacity,
              "arms": []}
    if kind == "ab":
        report.update(case=case, single_candidate_diagnostic=False,
                      binaries={name: identities[name] for name in
                                ("candidate", "control", "warp")})
    else:
        report.update(cases=[[*case_info[:4], list(case_info[4])]],
                      engine_selection="both", strict_settings=STRICT_SETTINGS,
                      binary_sha256={"cairn": identities["candidate"]["sha256"],
                                     "rustfs": identities["rustfs"]["sha256"],
                                     "warp": identities["warp"]["sha256"]})
    for pair in range(pairs):
        order = (("control", "candidate") if pair % 2 == 0 else
                 ("candidate", "control")) if kind == "ab" else (
                 ("cairn", "rustfs") if pair % 2 == 0 else ("rustfs", "cairn"))
        for variant in order:
            engine = "cairn" if kind == "ab" else variant
            arm = {"load_elapsed_seconds": 10.0, "status": "PASS",
                   "reported_errors": 0, "error_line_count": 0, "warp_exit": 0,
                   "limit_hit": False, "case": case, "concurrency": case_info[3],
                   "engine": engine, "strict_settings": STRICT_SETTINGS[engine],
                   "durability": {"bucket": BUCKET,
                                  "mode": "full-metadata" if engine == "cairn" else "strict"},
                   "objects_per_second": 80.0, "mib_per_second": 80.0,
                   "host_capacity_before": capacity.copy(),
                   "host_capacity_after": capacity.copy(),
                   "warp_operation_timeline": {"status": "captured source-derived test"}}
            if kind == "ab":
                arm["variant"] = variant
            if engine == "rustfs":
                arm["durability_after"] = arm["durability"]
            if phase[0].startswith("timing_") and engine == "cairn" and (
                    kind == "rustfs" or variant == "candidate"):
                arm["metrics_after_drain"] = [
                    *[f'cairn_put_stage_seconds_count{{stage="{stage}",result="ok"}} 12'
                      for stage in PUT_TIMING_STAGES],
                    "cairn_put_timing_dropped_total 0",
                ]
            report["arms"].append(arm)
    return report


class CampaignLedgerTests(unittest.TestCase):
    def test_put_timing_opt_in_accepts_only_bounded_ballooning(self):
        identities = {name: {"path": "/bin/true", "sha256": "test"}
                      for name in ("candidate", "control", "rustfs", "warp")}
        phase = PUT_TIMING_PHASES[0]
        report = fixture_report(phase, identities)
        report["allow_ballooning"] = True
        baseline = report["host_capacity"]
        report["arms"][0]["host_capacity_after"]["memtotal_kib"] += baseline["memtotal_kib"] // 20
        self.assertTrue(phase_passes(report, phase, identities, 12, 16,
                                     require_put_timing=True,
                                     expected_host_capacity=baseline,
                                     allow_ballooning=True))
        self.assertFalse(phase_passes(report, phase, identities, 12, 16,
                                      require_put_timing=True,
                                      expected_host_capacity=baseline))
        report["arms"][0]["host_capacity_after"]["memtotal_kib"] += baseline["memtotal_kib"] // 10
        self.assertFalse(phase_passes(report, phase, identities, 12, 16,
                                      require_put_timing=True,
                                      expected_host_capacity=baseline,
                                      allow_ballooning=True))

    def test_time_and_arm_validation_reject_vacuous_or_bad_reports(self):
        identities = {name: {"path": "/bin/true", "sha256": "test"}
                      for name in ("candidate", "control", "rustfs", "warp")}
        phase = DIAGNOSTIC_PHASES[0]
        report = fixture_report(phase, identities)
        self.assertEqual(phase_client_seconds(report), 40.0)
        self.assertTrue(phase_passes(report, phase, identities, 12, 16))
        report["arms"].pop()
        self.assertFalse(phase_passes(report, phase, identities, 12, 16))
        for bad in (0, -1, True, float("nan"), float("inf"), "17"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                phase_client_seconds({"arms": [{"load_elapsed_seconds": bad}]})
        report = fixture_report(phase, identities)
        report["arms"][0]["reported_errors"] = 1
        self.assertFalse(phase_passes(report, phase, identities, 12, 16))

    def test_phase_rejects_wrong_identity_mode_order_and_missing_live_timeline(self):
        identities = {name: {"path": "/bin/true", "sha256": "test"}
                      for name in ("candidate", "control", "rustfs", "warp")}
        for phase in DIAGNOSTIC_PHASES:
            with self.subTest(phase=phase[0]):
                report = fixture_report(phase, identities)
                self.assertTrue(phase_passes(report, phase, identities, 12, 16))
                changed_capacity = report["host_capacity"].copy()
                changed_capacity["memtotal_kib"] += 1
                self.assertFalse(phase_passes(
                    report, phase, identities, 12, 16,
                    expected_host_capacity=changed_capacity))
                report["arms"][0]["host_capacity_after"]["memtotal_kib"] += 1
                self.assertFalse(phase_passes(report, phase, identities, 12, 16))
                report = fixture_report(phase, identities)
                report["arms"][0]["warp_operation_timeline"] = {"status": "unavailable"}
                self.assertFalse(phase_passes(report, phase, identities, 12, 16))
                report = fixture_report(phase, identities)
                report["arms"][0]["durability"] = {"bucket": BUCKET, "mode": "relaxed"}
                self.assertFalse(phase_passes(report, phase, identities, 12, 16))
                report = fixture_report(phase, identities)
                report["arms"][0]["engine"] = "rustfs"
                self.assertFalse(phase_passes(report, phase, identities, 12, 16))
                report = fixture_report(phase, identities)
                report["arms"][0]["mib_per_second"] = None
                self.assertFalse(phase_passes(report, phase, identities, 12, 16))
                report = fixture_report(phase, identities)
                if phase[1] == "ab":
                    report["binaries"]["candidate"] = {"path": "/bin/true",
                                                        "sha256": "wrong"}
                else:
                    report["binary_sha256"]["cairn"] = "wrong"
                self.assertFalse(phase_passes(report, phase, identities, 12, 16))

    def test_wall_reservation_is_stricter_than_remaining_client_allowance(self):
        self.assertEqual(admissible_wall_cap(360, 600), 360)
        self.assertEqual(admissible_wall_cap(360, 120.9), 90)
        self.assertLess(admissible_wall_cap(180, 60), 60)

    def test_complete_campaign_accounts_all_five_phases_without_live_load(self):
        with tempfile.TemporaryDirectory(prefix="cairn-performance-ledger-",
                                         dir="/var/tmp") as directory:
            root = Path(directory)
            binary = Path("/bin/true")
            digest = sha256(binary)
            argv = ["root_barrier_campaign.py", "--root", str(root)]
            for name in ("candidate", "control", "rustfs", "warp"):
                argv.extend((f"--{name}", str(binary), f"--{name}-sha256", digest))
            calls = []
            capacity = {"memtotal_kib": 10_000_000, "swaptotal_kib": 0,
                        "logical_cpus": 4}

            def fake_run(command, **kwargs):
                self.assertEqual(kwargs, {"check": False})
                phase = PHASES[len(calls)]
                calls.append(command)
                self.assertLessEqual(int(command[command.index("--max-seconds") + 1]), 600)
                identities = {name: {"path": str(binary.resolve()), "sha256": digest}
                              for name in ("candidate", "control", "rustfs", "warp")}
                report = fixture_report(phase, identities, capacity=capacity)
                filename = (f"cairn-{phase[2]}-ab-results.json" if phase[1] == "ab"
                            else "comparison-results.json")
                (root / filename).write_text(json.dumps(report))
                return SimpleNamespace(returncode=0)

            with patch.object(sys, "argv", argv), patch(
                    "root_barrier_campaign.host_capacity", return_value=capacity), patch(
                    "root_barrier_campaign.subprocess.run", side_effect=fake_run):
                main()
            ledger = json.loads((root / "root-barrier-campaign.json").read_text())
            self.assertEqual(ledger["status"], "PASS")
            self.assertEqual(ledger["client_seconds"], 200.0)
            self.assertEqual([entry["name"] for entry in ledger["phases"]],
                             [phase[0] for phase in PHASES])
            self.assertEqual(len(calls), 5)
            self.assertTrue(all(entry["host_capacity_after"] == ledger["host_capacity"]
                                for entry in ledger["phases"]))
            for phase in PHASES:
                self.assertTrue((root / f"root-barrier-{phase[0]}-raw.json").is_file())
            with patch.object(sys, "argv", argv), self.assertRaises(SystemExit):
                main()

    def test_diagnostic_profile_has_only_paired_put_phases(self):
        self.assertEqual([(kind, case, pairs) for _, kind, case, pairs, _ in
                          DIAGNOSTIC_PHASES],
                         [("ab", "put_1m", 2), ("rustfs", "put_1m", 2)])
        self.assertLess(8 * 20, 300)

    def test_put_timing_profile_requires_loss_free_candidate_and_reference_samples(self):
        identities = {name: {"path": "/bin/true", "sha256": "test"}
                      for name in ("candidate", "control", "rustfs", "warp")}
        self.assertEqual([(kind, case, pairs) for _, kind, case, pairs, _ in
                          PUT_TIMING_PHASES],
                         [("ab", "put_1m", 3), ("rustfs", "put_1m", 1)])
        for phase in PUT_TIMING_PHASES:
            with self.subTest(phase=phase[0]):
                report = fixture_report(phase, identities)
                self.assertTrue(phase_passes(report, phase, identities, 12, 16,
                                             require_put_timing=True))
                measured = next(arm for arm in report["arms"]
                                if arm["engine"] == "cairn"
                                and (phase[1] == "rustfs" or arm["variant"] == "candidate"))
                self.assertTrue(put_timing_metrics_valid(measured))
                measured["metrics_after_drain"].append(
                    'cairn_put_stage_seconds_count{stage="publication",result="interrupted"} 1')
                self.assertFalse(phase_passes(report, phase, identities, 12, 16,
                                              require_put_timing=True))
                measured["metrics_after_drain"].pop()
                measured["metrics_after_drain"].append(
                    'cairn_put_stage_seconds_count{stage="total",result="ok"} 12')
                self.assertFalse(phase_passes(report, phase, identities, 12, 16,
                                              require_put_timing=True))
                measured["metrics_after_drain"].pop()
                measured["metrics_after_drain"][-1] = "cairn_put_timing_dropped_total 1"
                self.assertFalse(phase_passes(report, phase, identities, 12, 16,
                                              require_put_timing=True))
                measured["metrics_after_drain"][-1] = "cairn_put_timing_dropped_total 0"
                measured["metrics_after_drain"][0] = (
                    'cairn_put_stage_seconds_count{stage="total",result="ok"} 11')
                self.assertFalse(phase_passes(report, phase, identities, 12, 16,
                                              require_put_timing=True))
                measured["metrics_after_drain"] = []
                self.assertFalse(phase_passes(report, phase, identities, 12, 16,
                                              require_put_timing=True))

    def test_put_timing_profile_uses_one_bounded_ledger_without_live_load(self):
        with tempfile.TemporaryDirectory(prefix="cairn-performance-ledger-",
                                         dir="/var/tmp") as directory:
            root = Path(directory)
            binary = Path("/bin/true")
            digest = sha256(binary)
            identities = {name: {"path": str(binary.resolve()), "sha256": digest}
                          for name in ("candidate", "control", "rustfs", "warp")}
            argv = ["root_barrier_campaign.py", "--root", str(root), "--profile",
                    "put_timing", "--max-client-seconds", "300"]
            for name in identities:
                argv.extend((f"--{name}", str(binary), f"--{name}-sha256", digest))
            calls = []
            capacity = {"memtotal_kib": 10_000_000, "swaptotal_kib": 0,
                        "logical_cpus": 4}

            def fake_run(command, **kwargs):
                self.assertEqual(kwargs, {"check": False})
                phase = PUT_TIMING_PHASES[len(calls)]
                calls.append(command)
                report = fixture_report(phase, identities, capacity=capacity)
                filename = ("cairn-put_1m-ab-results.json" if phase[1] == "ab"
                            else "comparison-results.json")
                (root / filename).write_text(json.dumps(report))
                return SimpleNamespace(returncode=0)

            with patch.object(sys, "argv", argv), patch(
                    "root_barrier_campaign.host_capacity", return_value=capacity), patch(
                    "root_barrier_campaign.subprocess.run", side_effect=fake_run):
                main()
            ledger = json.loads((root / "root-barrier-campaign.json").read_text())
            self.assertEqual(ledger["status"], "PASS")
            self.assertEqual(ledger["profile"], "put_timing")
            self.assertEqual(ledger["max_client_seconds"], 300)
            self.assertEqual(ledger["client_seconds"], 80.0)
            self.assertEqual([entry["name"] for entry in ledger["phases"]],
                             ["timing_ab", "timing_rustfs"])
            self.assertEqual(len(calls), 2)

    def test_inconclusive_first_phase_stops_without_spending_next_phase(self):
        with tempfile.TemporaryDirectory(prefix="cairn-performance-ledger-",
                                         dir="/var/tmp") as directory:
            root = Path(directory)
            binary = Path("/bin/true")
            digest = sha256(binary)
            identities = {name: {"path": str(binary.resolve()), "sha256": digest}
                          for name in ("candidate", "control", "rustfs", "warp")}
            argv = ["root_barrier_campaign.py", "--profile", "diagnostic",
                    "--max-client-seconds", "300", "--root", str(root)]
            for name in identities:
                argv.extend((f"--{name}", str(binary), f"--{name}-sha256", digest))
            calls = []

            def fake_run(_command, **_kwargs):
                calls.append(True)
                report = fixture_report(DIAGNOSTIC_PHASES[0], identities)
                report["arms"][0]["warp_operation_timeline"]["status"] = "unavailable"
                (root / "cairn-put_1m-ab-results.json").write_text(json.dumps(report))
                return SimpleNamespace(returncode=0)

            with patch.object(sys, "argv", argv), patch(
                    "root_barrier_campaign.subprocess.run", side_effect=fake_run), self.assertRaises(
                    SystemExit):
                main()
            ledger = json.loads((root / "root-barrier-campaign.json").read_text())
            self.assertEqual(len(calls), 1)
            self.assertEqual(len(ledger["phases"]), 1)
            self.assertEqual(ledger["phases"][0]["status"], "INCONCLUSIVE")
            self.assertEqual(ledger["client_seconds"], 40.0)

    def test_capacity_change_after_child_refuses_phase(self):
        with tempfile.TemporaryDirectory(prefix="cairn-performance-ledger-",
                                         dir="/var/tmp") as directory:
            root = Path(directory)
            binary = Path("/bin/true")
            digest = sha256(binary)
            identities = {name: {"path": str(binary.resolve()), "sha256": digest}
                          for name in ("candidate", "control", "rustfs", "warp")}
            argv = ["root_barrier_campaign.py", "--profile", "put_timing",
                    "--max-client-seconds", "300", "--root", str(root)]
            for name in identities:
                argv.extend((f"--{name}", str(binary), f"--{name}-sha256", digest))

            def fake_run(_command, **_kwargs):
                report = fixture_report(PUT_TIMING_PHASES[0], identities)
                (root / "cairn-put_1m-ab-results.json").write_text(json.dumps(report))
                return SimpleNamespace(returncode=0)

            changed = host_capacity()
            changed["memtotal_kib"] += 1
            with patch.object(sys, "argv", argv), patch(
                    "root_barrier_campaign.subprocess.run", side_effect=fake_run), patch(
                    "root_barrier_campaign.host_capacity",
                    side_effect=[host_capacity(), changed]), self.assertRaises(SystemExit):
                main()
            ledger = json.loads((root / "root-barrier-campaign.json").read_text())
            self.assertEqual(ledger["phases"][0]["status"], "INCONCLUSIVE")
            self.assertEqual(ledger["phases"][0]["host_capacity_after"], changed)
            self.assertEqual(ledger["client_seconds"], 60.0)


if __name__ == "__main__":
    unittest.main()
