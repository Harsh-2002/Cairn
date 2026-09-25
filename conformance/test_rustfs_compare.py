"""Offline regressions for the bounded Cairn/RustFS comparison parser."""

import datetime
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from cairn_put_ab import main as ab_main
from rustfs_compare import (CASES, analyze_warp_put_timeline, arm_usage, cairn_metrics,
                            capacity_compatible,
                            diskstats_delta, io_pressure_totals, parse_diskstats,
                            parse_host_capacity,
                            parse_process_stat, parse_warp, parse_warp_aggregate,
                            prepare_verified_bucket, process_cpu_snapshot, process_io_delta,
                            process_io_snapshot, put_timeline_valid,
                            scored_host_window,
                            server_environment, run_arm, tree_usage, unrelated_cpu_delta,
                            verify_rustfs_durability, warp_benchdata_paths)


class WarpParserTests(unittest.TestCase):
    def test_host_capacity_is_exact_and_rejects_missing_fields(self):
        self.assertEqual(parse_host_capacity("MemTotal: 12255072 kB\nSwapTotal: 0 kB\n", 4),
                         {"memtotal_kib": 12255072, "swaptotal_kib": 0,
                          "logical_cpus": 4})
        with self.assertRaisesRegex(ValueError, "missing SwapTotal"):
            parse_host_capacity("MemTotal: 12255072 kB\n", 4)
        with self.assertRaisesRegex(ValueError, "invalid host capacity"):
            parse_host_capacity("MemTotal: 12255072 kB\nSwapTotal: 0 kB\n", 0)

    def test_opt_in_ballooning_keeps_fixed_capacity_and_ten_percent_bound(self):
        before = {"memtotal_kib": 10_000_000, "swaptotal_kib": 0, "logical_cpus": 4}
        small = dict(before, memtotal_kib=10_500_000)
        self.assertFalse(capacity_compatible(before, small))
        self.assertTrue(capacity_compatible(before, small, True))
        self.assertTrue(capacity_compatible(before, dict(before, memtotal_kib=9_000_000), True))
        self.assertFalse(capacity_compatible(before, dict(before, memtotal_kib=8_999_999), True))
        self.assertFalse(capacity_compatible(before, dict(before, logical_cpus=8), True))
        self.assertFalse(capacity_compatible(before, dict(before, swaptotal_kib=1), True))
        self.assertFalse(capacity_compatible(before, dict(before, memtotal_kib=True), True))

    def test_synthetic_v18_aggregate_shape_decodes_to_expected_score(self):
        fixture = Path(__file__).parent / "fixtures" / "warp_v18_put_aggregate.json"
        result = parse_warp_aggregate(fixture.read_bytes())
        self.assertEqual(result["analyzed_seconds_exact"], 8)
        self.assertEqual(result["aggregate_mib_per_second"], 1)

    def test_v18_benchdata_argument_matches_aggregate_artifact(self):
        arm = Path("/var/tmp/example-arm")
        self.assertEqual(warp_benchdata_paths(arm),
                         (arm / "warp", arm / "warp.json.zst"))

    def test_put_requires_proven_analyzer_interval_before_scoring(self):
        missing = {"status": "unavailable: missing Warp aggregate"}
        captured = {"status": "captured source-derived Warp scored interval with report-score parity"}
        self.assertFalse(put_timeline_valid("put", missing))
        self.assertTrue(put_timeline_valid("put", captured))
        self.assertTrue(put_timeline_valid("get", missing))

    def test_analyzer_json_uses_exact_scored_interval_and_rejects_bad_data(self):
        fixture = Path(__file__).parent / "fixtures" / "warp_v18_put_aggregate.json"
        realtime = json.loads(fixture.read_bytes())
        payload = json.dumps(realtime).encode()
        timeline = parse_warp_aggregate(payload)
        self.assertEqual(timeline["analyzed_seconds_exact"], 8)
        self.assertEqual(timeline["aggregate_mib_per_second"], 1)
        self.assertEqual(timeline["raw_end_unix_seconds"] - timeline["end_unix_seconds"], 4)
        with self.assertRaisesRegex(ValueError, "successful scored operation"):
            parse_warp_aggregate(payload, "GET")
        realtime["by_op_type"]["PUT"]["throughput"]["measure_duration_millis"] = 7000
        with self.assertRaisesRegex(ValueError, "interval"):
            parse_warp_aggregate(json.dumps(realtime).encode())
        with self.assertRaisesRegex(ValueError, "JSON"):
            parse_warp_aggregate(b"not-json")

    def test_scored_host_census_excludes_partial_boundary_intervals(self):
        samples = [{"unix_seconds": second,
                    "unrelated_process_cpu": {"sampled_cpu_seconds": second},
                    "host_io_pressure_totals_us": {"some": second * 10,
                                                  "full": second * 2},
                    "server_process_io": {key: second * 100 for key in
                                          ("rchar", "wchar", "syscr", "syscw",
                                           "read_bytes", "write_bytes")}}
                   for second in (2, 4, 6, 8, 10)]
        result = scored_host_window(samples, 0,
                                    {"start_unix_seconds": 3, "end_unix_seconds": 9})
        self.assertEqual(result["complete_intervals"], 2)
        self.assertEqual(result["coverage_seconds"], 4)
        self.assertEqual(result["unrelated_cpu_seconds"], 14)
        self.assertEqual(result["host_io_pressure_delta_us"], {"some": 40, "full": 8})
        self.assertEqual(result["server_process_io_valid_intervals"], 2)
        self.assertEqual(result["server_process_io_delta"]["write_bytes"], 400)

    def test_warp_timeline_uses_analyzer_without_promoting_missing_data(self):
        with tempfile.TemporaryDirectory() as directory:
            arm = Path(directory)
            self.assertIn("unavailable", analyze_warp_put_timeline(
                "warp", arm, 1_700_000_000, 1_700_000_010, [],
                time.monotonic() + 60, 1)["status"])
            (arm / "warp.json.zst").write_bytes(b"fake fixture")
            def fake_analyzer(command, **_kwargs):
                self.assertNotIn("--full", command)
                self.assertIn("--json", command)
                self.assertEqual(command[-1], str(arm / "warp.json.zst"))
                first = datetime.datetime.fromtimestamp(1_700_000_001, datetime.timezone.utc)
                last = datetime.datetime.fromtimestamp(1_700_000_009, datetime.timezone.utc)
                fixture = Path(__file__).parent / "fixtures" / "warp_v18_put_aggregate.json"
                payload = json.loads(fixture.read_bytes())
                throughput = payload["by_op_type"]["PUT"]["throughput"]
                throughput["start_time"] = first.isoformat()
                throughput["end_time"] = (last + datetime.timedelta(seconds=4)).isoformat()
                return SimpleNamespace(returncode=0, stdout=json.dumps(payload).encode())
            with patch("rustfs_compare.subprocess.run", side_effect=fake_analyzer):
                result = analyze_warp_put_timeline(
                    "warp", arm, 1_700_000_000, 1_700_000_010, [], time.monotonic() + 60, 1)
            self.assertEqual(result["analyzed_seconds_exact"], 8)
            self.assertEqual(result["host_window"]["complete_intervals"], 0)
            self.assertIn("report-score parity", result["status"])
            with patch("rustfs_compare.subprocess.run", side_effect=fake_analyzer):
                stale = analyze_warp_put_timeline(
                    "warp", arm, 1_700_000_020, 1_700_000_030, [], time.monotonic() + 60, 1)
            self.assertIn("outside client-load", stale["status"])
            with patch("rustfs_compare.subprocess.run", side_effect=fake_analyzer):
                mismatch = analyze_warp_put_timeline(
                    "warp", arm, 1_700_000_000, 1_700_000_010, [],
                    time.monotonic() + 60, 2)
            self.assertIn("differs", mismatch["status"])

    def test_mixed_weights_are_explicit_and_stable(self):
        case = next(case for case in CASES if case[0] == "mixed_1m")
        self.assertEqual(case[4], ("--objects", "100", "--get-distrib", "45",
                                   "--stat-distrib", "30", "--put-distrib", "15",
                                   "--delete-distrib", "10"))

    def test_v18_put_summary(self):
        result = parse_warp(
            "Report: PUT. Concurrency: 32. Ran: 9s\n"
            " * Average: 1.40 MiB/s, 357.84 obj/s\n"
            " * Reqs: Avg: 99ms, 50%: 80ms, 90%: 0.2s, 99%: 900us\n",
            "put",
        )
        self.assertEqual(result["objects_per_second"], 357.84)
        self.assertEqual(result["analyzed_seconds"], 9)
        self.assertEqual(result["reported_errors"], 0)
        self.assertEqual(result["reports"]["PUT"]["request_latency_ms"],
                         {"avg": 99, "p50": 80, "p90": 200, "p99": 0.9})

    def test_preparation_put_does_not_replace_measured_get(self):
        result = parse_warp(
            "Report: PUT. Concurrency: 16. Ran: 5s\n"
            " * Average: 5 MiB/s, 5 obj/s\n"
            "Report: GET. Concurrency: 16. Ran: 9s\n"
            " * Average: 700 MiB/s, 700 obj/s\n",
            "get",
        )
        self.assertEqual(result["mib_per_second"], 700)

    def test_mixed_total_and_components(self):
        result = parse_warp(
            "Report: DELETE. Concurrency: 16. Ran: 10s\n"
            " * Average: 30 obj/s\n"
            "Report: Total. Concurrency: 16. Ran: 10s\n"
            " * Average: 200 MiB/s, 300 obj/s\n",
            "mixed",
        )
        self.assertEqual(result["objects_per_second"], 300)
        self.assertEqual(result["reports"]["DELETE"]["summary"], "* Average: 30 obj/s")

    def test_multipart_size_errors_are_not_success(self):
        result = parse_warp(
            "warp: <ERROR> unexpected download size\n"
            "Report: GET. Concurrency: 16. Ran: 10s\n"
            " * Average: 30 MiB/s, 6 obj/s, 64 errors\n"
            " * Errors: 82\n",
            "multipart",
        )
        self.assertIsNone(result["summary"])
        self.assertGreater(result["reported_errors"], 0)


class OwnedFootprintTests(unittest.TestCase):
    def test_process_stat_parser_handles_parentheses_and_rejects_truncation(self):
        fields = ["0"] * 20
        fields[0] = "S"
        fields[11], fields[12], fields[19] = "100", "25", "987"
        self.assertEqual(parse_process_stat("123 (go) test) " + " ".join(fields)),
                         ((123, 987), "go) test", 125))
        self.assertIsNone(parse_process_stat("123 (go test) S 0 0"))
        self.assertIsNone(parse_process_stat("123 go test S " + " ".join(fields)))

    def test_process_cpu_delta_keeps_pid_reuse_separate_and_bounds_names(self):
        before = {(12, 100): ("old", 100), (13, 100): ("vm", 30)}
        after = {(12, 200): ("new", 15), (13, 100): ("vm", 40)}
        result = unrelated_cpu_delta(before, after)
        self.assertEqual(result["processes_seen"], 2)
        self.assertEqual(result["exited_since_previous"], 1)
        self.assertEqual([entry["name"] for entry in result["top"]], ["new", "vm"])
        self.assertGreater(result["sampled_cpu_seconds"], 0)
        self.assertIsNone(unrelated_cpu_delta(None, after))

    def test_process_census_unavailable_is_not_a_false_zero(self):
        with patch("rustfs_compare.os.scandir", side_effect=PermissionError):
            self.assertIsNone(process_cpu_snapshot())

    def test_io_pressure_parser_keeps_only_cumulative_stall_microseconds(self):
        with patch("rustfs_compare.Path.read_text",
                   return_value="some avg10=3.0 avg60=2.0 avg300=1.0 total=123\n"
                                "full avg10=1.0 avg60=1.0 avg300=0.5 total=45\n"):
            self.assertEqual(io_pressure_totals(), {"some": 123, "full": 45})

    def test_process_io_counters_reject_missing_or_reset_samples(self):
        fixture = ("rchar: 10\nwchar: 20\nsyscr: 1\nsyscw: 2\n"
                   "read_bytes: 4096\nwrite_bytes: 8192\n")
        with patch("rustfs_compare.Path.read_text", return_value=fixture):
            before = process_io_snapshot(123)
        self.assertEqual(before["write_bytes"], 8192)
        after = {key: value + 1 for key, value in before.items()}
        self.assertEqual(process_io_delta(before, after)["write_bytes"], 1)
        self.assertIsNone(process_io_delta(before, None))
        self.assertIsNone(process_io_delta(before, {"write_bytes": 8193}))
        after["write_bytes"] = 0
        self.assertIsNone(process_io_delta(before, after))
        with patch("rustfs_compare.Path.read_text", return_value="write_bytes: 2\n"):
            self.assertIsNone(process_io_snapshot(123))
        with patch("rustfs_compare.Path.read_text", side_effect=PermissionError):
            self.assertIsNone(process_io_snapshot(123))

    def test_diskstats_matches_exact_volume_device_and_fixed_counters(self):
        diskstats = ("8 0 whole 999 0 999 0 999 0 999 0 0 999 999\n"
                     "8 1 volume 7 2 1024 31 11 3 2048 47 1 59 61 0 0\n")
        self.assertEqual(parse_diskstats(diskstats, os.makedev(8, 1)),
                         {"name": "volume", "reads": 7, "read_sectors": 1024,
                          "read_ms": 31, "writes": 11, "write_sectors": 2048,
                          "write_ms": 47, "io_ms": 59, "weighted_io_ms": 61})
        self.assertIsNone(parse_diskstats(diskstats, os.makedev(8, 2)))

    def test_malformed_diskstats_is_unavailable_not_a_false_zero(self):
        self.assertIsNone(parse_diskstats("8 1 short 1 2\n8 1 bad x 0 0 0 0 0 0 0 0 0 0",
                                          os.makedev(8, 1)))

    def test_device_delta_rejects_replacement_reset_and_missing_samples(self):
        before = {"name": "sda1", "reads": 10, "writes": 20}
        self.assertEqual(diskstats_delta(before, {"name": "sda1", "reads": 12,
                                                  "writes": 25}),
                         {"reads": 2, "writes": 5})
        self.assertIsNone(diskstats_delta(before, {"name": "sdb1", "reads": 12,
                                                    "writes": 25}))
        self.assertIsNone(diskstats_delta(before, {"name": "sda1", "reads": 9,
                                                    "writes": 25}))
        self.assertIsNone(diskstats_delta(before, None))

    def test_disappearing_entries_do_not_fail_sampling(self):
        with tempfile.TemporaryDirectory() as directory:
            child = Path(directory, "sample")
            child.write_bytes(b"data")
            used, inodes = tree_usage(directory)
            self.assertGreaterEqual(used, 0)
            self.assertEqual(inodes, 1)

    def test_live_sampler_accounts_for_base_and_owned_arm(self):
        with tempfile.TemporaryDirectory() as directory:
            arm = Path(directory, "arm")
            arm.mkdir()
            (arm / "object").write_bytes(b"x" * 4096)
            used, inodes = arm_usage(100, 2, arm)
            self.assertGreaterEqual(used, 4196)
            self.assertEqual(inodes, 3)

    def test_base_before_arm_growth_does_not_double_count(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "tool").write_bytes(b"static")
            arm = root / "arm"
            arm.mkdir()
            base_bytes, base_inodes = tree_usage(root)
            (arm / "object").write_bytes(b"live")
            self.assertEqual(arm_usage(base_bytes, base_inodes, arm), tree_usage(root))


class StrictDurabilityTests(unittest.TestCase):
    def test_single_candidate_cli_runs_only_one_metered_diagnostic_arm(self):
        with tempfile.TemporaryDirectory(prefix="cairn-performance-test-", dir="/var/tmp") as directory:
            root = Path(directory)
            candidate = root / "candidate"
            warp = root / "warp"
            for binary in (candidate, warp):
                binary.write_bytes(b"test binary")
                binary.chmod(0o755)
            argv = ["cairn_put_ab.py", "--root", directory,
                    "--candidate", str(candidate), "--warp", str(warp),
                    "--single-candidate", "--pairs", "1", "--duration", "5",
                    "--max-load-seconds", "11.5", "--metrics-drain-seconds", "0",
                    "--max-seconds", "90"]
            result = ({"status": "PASS", "summary": "diagnostic",
                       "reported_errors": 0, "load_elapsed_seconds": 10.0}, None)
            with patch.object(sys, "argv", argv), patch("cairn_put_ab.run_arm", return_value=result) as arm:
                with patch("builtins.print"):
                    ab_main()
            self.assertEqual(arm.call_count, 1)
            report = json.loads((root / "cairn-put_1m-single-results.json").read_text())
            self.assertTrue(report["single_candidate_diagnostic"])
            self.assertEqual(report["max_load_seconds"], 11.5)
            self.assertEqual(report["arms"][0]["variant"], "candidate")
            self.assertNotIn("control", report["binaries"])

    def test_invalid_measured_load_cap_rejects_before_creating_an_arm(self):
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(max_load_seconds=0)
            case = next(case for case in CASES if case[0] == "put_1m")
            with self.assertRaises(ValueError):
                run_arm(args, Path(directory), case, "cairn", 1, time.monotonic() + 60)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_linger_screen_rejects_unbounded_override_without_leaving_an_arm(self):
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(cairn_linger_us=1001)
            case = next(case for case in CASES if case[0] == "put_1m")
            with self.assertRaises(ValueError):
                run_arm(args, Path(directory), case, "cairn", 1, time.monotonic() + 60)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_inherited_storage_overrides_cannot_change_an_arm(self):
        inherited = {"CAIRN_META_SYNCHRONOUS": "normal",
                     "RUSTFS_NEW_BUCKET_DURABILITY_MODE": "relaxed",
                     "RUSTFS_DRIVE_SYNC_ENABLE": "false", "AWS_PROFILE": "foreign",
                     "WARP_ACCESS_KEY": "foreign", "LD_PRELOAD": "foreign.so",
                     "SYNC_PROBE_FILE": "/foreign/probe", "PATH": "/usr/bin"}
        with patch.dict("os.environ", inherited, clear=True):
            for engine in ("cairn", "rustfs"):
                env = server_environment(engine, Path("/private/volume"), "127.0.0.1:18001",
                                         "secret", "master")
                self.assertEqual(env["PATH"], "/usr/bin")
                self.assertNotIn("AWS_PROFILE", env)
                self.assertNotIn("WARP_ACCESS_KEY", env)
                self.assertNotIn("LD_PRELOAD", env)
                self.assertNotIn("SYNC_PROBE_FILE", env)
                if engine == "cairn":
                    self.assertEqual(env["CAIRN_META_SYNCHRONOUS"], "full")
                    self.assertNotIn("RUSTFS_NEW_BUCKET_DURABILITY_MODE", env)
                else:
                    self.assertEqual(env["RUSTFS_DURABILITY_MODE"], "strict")
                    self.assertEqual(env["RUSTFS_NEW_BUCKET_DURABILITY_MODE"], "strict")
                    self.assertEqual(env["RUSTFS_DRIVE_SYNC_ENABLE"], "true")
                    self.assertNotIn("CAIRN_META_SYNCHRONOUS", env)

    def test_missing_or_relaxed_bucket_mode_rejects_scoring(self):
        for state in (b'{"bucket":"cairn-benchmark","mode":null}',
                      b'{"bucket":"cairn-benchmark","mode":"relaxed"}',
                      b'{"bucket":"other","mode":"strict"}', b'<html>bad response</html>'):
            with self.subTest(state=state), patch("rustfs_compare.signed_request", return_value=state):
                with self.assertRaises(RuntimeError):
                    verify_rustfs_durability(18001, "secret")

    def test_verified_bucket_requires_strict_readback(self):
        with patch("rustfs_compare.signed_request",
                   side_effect=[b"", b'{"bucket":"cairn-benchmark","mode":"strict"}']) as request:
            self.assertEqual(prepare_verified_bucket(18001, "secret", "rustfs")["mode"], "strict")
        self.assertEqual([call.args[2] for call in request.call_args_list], ["PUT", "GET"])

    def test_metric_capture_keeps_only_fixed_diagnostic_families(self):
        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

            def read(self, _):
                return (b'# TYPE cairn_blob_object_write_stage_seconds histogram\n'
                        b'cairn_blob_object_write_stage_seconds_sum{stage="body"} 1.5\n'
                        b'cairn_put_stage_seconds_sum{stage="storage_admission",result="ok"} 0.2\n'
                        b'cairn_put_timing_dropped_total 0\n'
                        b'cairn_writer_batch_size_count 2\n'
                        b'cairn_objects 10\n')

        with patch("rustfs_compare.urllib.request.urlopen", return_value=Response()):
            self.assertEqual(cairn_metrics("127.0.0.1:18001"),
                             ['cairn_blob_object_write_stage_seconds_sum{stage="body"} 1.5',
                              'cairn_put_stage_seconds_sum{stage="storage_admission",result="ok"} 0.2',
                              'cairn_put_timing_dropped_total 0',
                              'cairn_writer_batch_size_count 2'])


if __name__ == "__main__":
    unittest.main()
