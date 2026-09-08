"""Prevent insufficient or incomparable latency evidence from becoming qualification."""
import copy
from pathlib import Path
import tempfile
import unittest

from journal_report import cleanup_drained, compare, metrics, validate_reports
from summarize import summarize_run


def drained():
    return {'status': 'drained', 'protocol': {'minimum_reader': 2, 'minimum_writer': 2},
            'boundary': 'after_final_idle_before_server_stop', 'access': 'sqlite_mode_ro',
            'wait_budget_seconds': 5, 'elapsed_seconds': .25,
            'samples': [{'elapsed_seconds': .2, 'write_intents': 0, 'cleanups': 0}]}


def run(count=500):
    return {'status': 'PASS', 'workload': {'seconds': 3, 'buckets': 1, 'max_ops': 30000}, 'cycles': [
        {'cycle': cycle, 'operation_names': ['put', 'get', 'delete'],
         'transactions_per_second': count / 3, 'elapsed': 3., 'operation_cap_reached': False,
         'bucket_transactions': [count], 'successful_transactions': count,
         'operations': [{'count': count, 'p50_seconds': .001, 'p99_seconds': None}
                        for _ in range(3)]} for cycle in range(3)],
        'phase_observations': [{'cycle': cycle, 'phase': 'load', 'cpu_cores': .5, 'client_cpu_cores': .3,
                               'last_sample': {'rss_kib': 100, 'anon_kib': 90, 'pss_kib': 95}}
                               for cycle in range(3)]}


def reports():
    arms, ledger = [[], []], []
    for pair in range(5):
        for side in range(2):
            token = f'{pair * 2 + side:032x}'
            arms[side].append({
                'id': token, 'status': 'PASS', 'phase': 'recovery', 'profile': 'none',
                'cleaned_data_and_processes': True,
                'workload': {'layer': 's3', 'concurrency': 4, 'buckets': 1, 'size': 4096,
                             'seed': 0x5eed, 'seconds': 3, 'idle': 1, 'cycles': 3, 'max_ops': 30000},
                'manifest': {'sha256': str(side + 1) * 64, 'declared_commit': str(side + 1) * 40,
                             'build_settings': 'release default features, symbols',
                             'coordinator_sha256': 'a' * 64,
                             'harness_sources_sha256': {name: 'b' * 64 for name in
                                                        ('s3_driver.py', 'lab.py', 'budget.py', 'processes.py')}},
                'server_configuration': {'CAIRN_META_SYNCHRONOUS': 'full', 'CAIRN_META_BACKEND': 'sqlite',
                                         'CAIRN_META_SHARDS': '1', 'CAIRN_DATA_DIR': f'/fixture/{token}',
                                         'CAIRN_DB_PATH': f'/fixture/{token}/meta.db',
                                         'CAIRN_LISTEN_ADDR': f'127.0.0.1:{10000 + pair * 2 + side}',
                                         'CAIRN_BLOB_IO_POOL_SIZE': '64'}})
            ledger.append({'id': token, 'phase': 'recovery', 'status': 'PASS'})
    combined = arms[0] + arms[1]
    return combined, [report['id'] for report in combined], ledger


class JournalEvidence(unittest.TestCase):
    def test_reduction_retains_cleanup_observation_and_missing_evidence(self):
        report = {**run(), 'id': 'fixture', 'reasons': [], 'profile': 'none',
                  'manifest': {'binary': '/fixture/cairn'}, 'post_load_storage_cleanup': drained()}
        report['workload']['layer'] = 's3'
        with tempfile.TemporaryDirectory() as directory:
            reduced = summarize_run(Path(directory), report, 'fixture-device')
            self.assertEqual(reduced['post_load_storage_cleanup'], report['post_load_storage_cleanup'])
            del report['post_load_storage_cleanup']
            self.assertIsNone(summarize_run(Path(directory), report, 'fixture-device')['post_load_storage_cleanup'])

    def test_baseline_unsupported_does_not_disqualify_observed_candidate_cleanup(self):
        baseline, candidate = run(), run()
        baseline['post_load_storage_cleanup'] = {'status': 'unsupported'}
        candidate['post_load_storage_cleanup'] = drained()
        comparison = compare([baseline], [candidate])
        self.assertEqual(comparison['cleanup_qualification']['status'], 'PASS')

    def test_cleanup_gaps_preserve_descriptive_latency_ratios(self):
        for observation in (None, {'status': 'residual'}, {'status': 'unavailable'}, {'status': 'unsupported'}):
            candidate = run()
            candidate['post_load_storage_cleanup'] = observation
            comparison = compare([run()], [candidate])
            self.assertEqual(comparison['cleanup_qualification']['status'], 'INCONCLUSIVE')
            self.assertEqual(comparison['median_paired_candidate_to_baseline']['put_cycle_p50_seconds'], 1)
            self.assertTrue(any('post-load journal cleanup' in reason for reason in comparison['reasons']))

    def test_claimed_drain_requires_bounded_live_zero_count_evidence(self):
        self.assertTrue(cleanup_drained(drained()))
        changes = [lambda value: value.update(elapsed_seconds=6),
                   lambda value: value.update(wait_budget_seconds=6),
                   lambda value: value.update(boundary='after_stop'),
                   lambda value: value.update(samples=[]),
                   lambda value: value['samples'][0].update(cleanups=1),
                   lambda value: value['samples'][0].update(write_intents=False),
                   lambda value: value['samples'][0].update(elapsed_seconds=float('nan'))]
        for change in changes:
            value = drained()
            change(value)
            self.assertFalse(cleanup_drained(value))

    def test_many_small_cycles_do_not_manufacture_p99(self):
        comparison = compare([run(4000) for _ in range(5)], [run(4000) for _ in range(5)])
        self.assertEqual(comparison['status'], 'INCONCLUSIVE')
        self.assertIsNone(comparison['median_paired_candidate_to_baseline']['put_cycle_p99_seconds'])

    def test_failed_cycle_cannot_be_compared(self):
        value = run()
        value['status'] = 'FAIL'
        with self.assertRaises(ValueError):
            metrics(value)

    def test_control_drift_and_client_saturation_remain_explicit(self):
        baseline = [run() for _ in range(5)]
        candidate = copy.deepcopy(baseline)
        for cycle in baseline[0]['cycles']:
            cycle['operations'][0]['p50_seconds'] *= 2
        for phase in candidate[0]['phase_observations']:
            phase['client_cpu_cores'] = .95
        reasons = compare(baseline, candidate)['reasons']
        self.assertTrue(any('control span' in reason for reason in reasons))
        self.assertTrue(any('saturation' in reason for reason in reasons))

    def test_nonfinite_and_negative_samples_are_rejected(self):
        for invalid in (float('nan'), float('inf'), -.1):
            with self.subTest(invalid=invalid):
                value = run()
                value['cycles'][0]['operations'][0]['p50_seconds'] = invalid
                with self.assertRaises(ValueError):
                    metrics(value)

    def test_capped_short_duplicate_or_inconsistent_cycles_are_rejected(self):
        changes = [lambda value: value['cycles'][0].update(operation_cap_reached=True),
                   lambda value: value['cycles'][0].update(elapsed=2.),
                   lambda value: value['cycles'][1].update(cycle=0),
                   lambda value: value['cycles'][0].update(transactions_per_second=1.),
                   lambda value: value['cycles'][0]['operations'][1].update(count=1),
                   lambda value: value['cycles'][0].update(bucket_transactions=[1])]
        for change in changes:
            value = run()
            change(value)
            with self.assertRaises(ValueError):
                metrics(value)
        # Exactly the declared per-cycle cap cannot be a PASS even if its flag is stale.
        with self.assertRaises(ValueError):
            metrics(run(10_000))

    def test_one_saturated_cycle_is_not_hidden_by_median_client_cpu(self):
        baseline = [run() for _ in range(5)]
        candidate = copy.deepcopy(baseline)
        candidate[0]['phase_observations'][0]['client_cpu_cores'] = .95
        self.assertEqual(metrics(candidate[0])['client_cpu_cores'], .3)
        self.assertTrue(any('saturation' in reason for reason in compare(baseline, candidate)['reasons']))

    def test_memory_is_phase_end_only_and_partial_observations_remain_unavailable(self):
        value = run()
        value['phase_observations'][0]['peak_rss_kib'] = 1000
        self.assertEqual(metrics(value)['max_phase_end_rss_kib'], 100)
        del value['phase_observations'][0]['last_sample']['pss_kib']
        self.assertIsNone(metrics(value)['max_phase_end_pss_kib'])
        self.assertTrue(any('pss' in reason for reason in compare([value], [run()])['reasons']))

    def test_all_operation_p99_limits_remain_visible(self):
        reasons = compare([run()], [run()])['reasons']
        for operation in ('PUT', 'GET', 'DELETE'):
            self.assertTrue(any(f'{operation} p99' in reason for reason in reasons))

    def test_matching_builds_allow_only_ephemeral_configuration_differences(self):
        values, tokens, ledger = reports()
        validate_reports(values, 5, tokens, ledger)
        values[-1]['server_configuration']['CAIRN_BLOB_IO_POOL_SIZE'] = '1'
        with self.assertRaisesRegex(ValueError, 'configuration'):
            validate_reports(values, 5, tokens, ledger)

    def test_mixed_provenance_is_rejected_within_and_across_arms(self):
        for field, replacement in (('sha256', 'c' * 64), ('declared_commit', 'c' * 40),
                                   ('build_settings', 'debug'), ('coordinator_sha256', 'c' * 64),
                                   ('harness_sources_sha256', {'s3_driver.py': 'c' * 64})):
            with self.subTest(field=field):
                values, tokens, ledger = reports()
                values[-1]['manifest'][field] = replacement
                with self.assertRaises(ValueError):
                    validate_reports(values, 5, tokens, ledger)

    def test_report_identity_and_alternating_pair_order_are_verified(self):
        values, tokens, ledger = reports()
        values[0]['id'] = 'f' * 32
        with self.assertRaisesRegex(ValueError, 'identity'):
            validate_reports(values, 5, tokens, ledger)
        values, tokens, ledger = reports()
        ledger[0], ledger[1] = ledger[1], ledger[0]
        with self.assertRaisesRegex(ValueError, 'alternating'):
            validate_reports(values, 5, tokens, ledger)

    def test_screen_and_confirmation_workloads_cannot_be_mixed(self):
        values, tokens, ledger = reports()
        with self.assertRaises(ValueError):
            validate_reports([values[0], values[5]], 1, [tokens[0], tokens[5]], ledger)
        for value in values:
            value['workload']['size'] = 1024 * 1024
        validate_reports([values[0], values[5]], 1, [tokens[0], tokens[5]], ledger)
        with self.assertRaises(ValueError):
            validate_reports(values, 5, tokens, ledger)


if __name__ == '__main__':
    unittest.main()
