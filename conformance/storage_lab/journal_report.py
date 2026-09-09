#!/usr/bin/env python3
"""Reduce paired protocol-2 S3 lifecycle costs under the persistent campaign allowance."""
import argparse
import hashlib
import json
import math
import re
from pathlib import Path
import statistics
import time

from budget import Campaign, Unavailable, atomic_json, read_json, valid_token
from summarize import summarize_run


def number(value, name, *, positive=False):
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not math.isfinite(value) or value < 0 or (positive and value == 0)):
        raise ValueError(f'{name}: expected a finite {"positive" if positive else "nonnegative"} number')
    return value


def metrics(run):
    if run['status'] != 'PASS' or len(run['cycles']) != 3:
        raise ValueError('all three equal-duration cycles must pass before comparison')
    cycles = run['cycles']
    if [cycle['cycle'] for cycle in cycles] != [0, 1, 2]:
        raise ValueError('expected three distinct ordered cycles')
    seconds = number(run['workload']['seconds'], 'load duration', positive=True)
    cap = run['workload']['max_ops'] // 3
    for cycle in cycles:
        if cycle['operation_names'] != ['put', 'get', 'delete'] or len(cycle['operations']) != 3:
            raise ValueError('expected complete signed S3 transactions')
        count = cycle['successful_transactions']
        if (type(count) is not int or count <= 0 or count >= cap
                or cycle['operation_cap_reached'] is not False):
            raise ValueError('empty or capped cycle cannot be compared')
        elapsed = number(cycle['elapsed'], 'cycle elapsed', positive=True)
        rate = number(cycle['transactions_per_second'], 'transaction rate', positive=True)
        if elapsed < seconds or not math.isclose(rate, count / elapsed, rel_tol=1e-9):
            raise ValueError('shortened cycle or inconsistent transaction rate')
        buckets = cycle['bucket_transactions']
        if (len(buckets) != run['workload']['buckets']
                or any(type(value) is not int or value <= 0 for value in buckets)
                or sum(buckets) != count):
            raise ValueError('bucket transaction counts do not match the completed cycle')
        for row in cycle['operations']:
            if type(row['count']) is not int or row['count'] != count:
                raise ValueError('operation sample count differs from successful transactions')
            number(row['p50_seconds'], 'operation p50', positive=True)
            if row['p99_seconds'] is not None:
                number(row['p99_seconds'], 'operation p99', positive=True)
                if row['p99_seconds'] < row['p50_seconds']:
                    raise ValueError('operation p99 precedes its p50')
    values = {'transactions_per_second': statistics.median(c['transactions_per_second'] for c in cycles)}
    for index, operation in enumerate(('put', 'get', 'delete')):
        observations = [cycle['operations'][index] for cycle in cycles]
        values[f'{operation}_cycle_p50_seconds'] = statistics.median(row['p50_seconds'] for row in observations)
        # Preserve the driver's stronger per-cycle sufficiency gate. Summing small cycles does
        # not reconstruct a pooled quantile or make their absent p99 measurements available.
        values[f'{operation}_cycle_p99_seconds'] = (
            statistics.median(row['p99_seconds'] for row in observations)
            if all(row['count'] >= 10_000 and row['p99_seconds'] is not None for row in observations)
            else None)
    phases = [phase for phase in run['phase_observations'] if phase['phase'] == 'load']
    if [phase['cycle'] for phase in phases] != [0, 1, 2]:
        raise ValueError('expected one load observation per cycle')
    for output, source in (('server_cpu_cores', 'cpu_cores'), ('client_cpu_cores', 'client_cpu_cores')):
        points = [number(phase[source], source) for phase in phases if phase.get(source) is not None]
        values[output] = statistics.median(points) if len(points) == 3 else None
    for name in ('rss_kib', 'anon_kib', 'pss_kib'):
        points = [number(phase['last_sample'][name], name) for phase in run['phase_observations']
                  if phase.get('last_sample', {}).get(name) is not None]
        values[f'max_phase_end_{name}'] = max(points) if len(points) == len(run['phase_observations']) else None
    values['successful_transactions'] = sum(cycle['successful_transactions'] for cycle in cycles)
    return values


def validate_reports(reports, baseline_count, tokens, ledger_runs):
    if baseline_count not in (1, 5) or len(reports) != 2 * baseline_count:
        raise ValueError('expected five confirmation pairs or one screening pair')
    if len(tokens) != len(reports) or len(set(tokens)) != len(tokens):
        raise ValueError('expected distinct owned report identities')
    ledger = {run['id']: (index, run) for index, run in enumerate(ledger_runs)}
    reference = reports[0]
    expected = {'layer': 's3', 'concurrency': 4, 'buckets': 1,
                'size': 4096 if baseline_count == 5 else 1024 * 1024,
                'seed': 0x5eed, 'seconds': 3, 'idle': 1, 'cycles': 3, 'max_ops': 30000}
    ephemeral = {'CAIRN_DATA_DIR', 'CAIRN_DB_PATH', 'CAIRN_API_ADDR'}
    configuration = {key: value for key, value in reference['server_configuration'].items() if key not in ephemeral}
    if (configuration.get('CAIRN_META_SYNCHRONOUS') != 'full'
            or configuration.get('CAIRN_META_BACKEND') != 'sqlite'
            or configuration.get('CAIRN_META_SHARDS') != '1'):
        raise ValueError('comparison requires FULL-durability single-shard SQLite')
    provenance_fields = ('build_settings', 'coordinator_sha256', 'harness_sources_sha256')
    for token, report in zip(tokens, reports):
        if (report['id'] != token or token not in ledger or ledger[token][1]['status'] != 'PASS'
                or ledger[token][1]['phase'] != 'recovery' or report['phase'] != 'recovery'):
            raise ValueError('report identity is not a completed owned recovery-phase run')
        if ({key: report['workload'][key] for key in expected} != expected
                or report['profile'] != 'none' or report['status'] != 'PASS'
                or report['cleaned_data_and_processes'] is not True):
            raise ValueError('comparison arms differ from the predeclared workload or did not finish')
        if {key: value for key, value in report['server_configuration'].items() if key not in ephemeral} != configuration:
            raise ValueError('effective server configuration differs between arms')
        manifest = report['manifest']
        sources = manifest['harness_sources_sha256']
        hashes = [manifest['sha256'], manifest['coordinator_sha256']]
        if isinstance(sources, dict):
            hashes.extend(sources.values())
        if (any(not isinstance(value, str) or not re.fullmatch('[0-9a-f]{64}', value) for value in hashes)
                or not isinstance(manifest['declared_commit'], str) or not manifest['declared_commit']
                or not isinstance(manifest['build_settings'], str) or not manifest['build_settings']
                or not isinstance(sources, dict)
                or not {'s3_driver.py', 'lab.py', 'budget.py', 'processes.py'}.issubset(sources)):
            raise ValueError('missing executable/build/harness provenance')
        if any(manifest[key] != reference['manifest'][key] for key in provenance_fields):
            raise ValueError('build settings or harness provenance differs between arms')
    for arm in (reports[:baseline_count], reports[baseline_count:]):
        identity = (arm[0]['manifest']['sha256'], arm[0]['manifest']['declared_commit'])
        if any((report['manifest']['sha256'], report['manifest']['declared_commit']) != identity for report in arm):
            raise ValueError('one arm mixes executable hashes or declared revisions')
    if (reports[0]['manifest']['sha256'] == reports[baseline_count]['manifest']['sha256']
            or reports[0]['manifest']['declared_commit'] == reports[baseline_count]['manifest']['declared_commit']):
        raise ValueError('baseline and candidate must identify distinct builds and revisions')
    paired = [token for pair in zip(tokens[:baseline_count], tokens[baseline_count:]) for token in pair]
    positions = [ledger[token][0] for token in paired]
    if positions != sorted(positions):
        raise ValueError('runs do not follow the declared alternating baseline/candidate pair order')


def compare(baselines, candidates):
    if len(baselines) != len(candidates) or len(baselines) not in (1, 5):
        raise ValueError('expected five paired confirmation arms or one screening pair')
    reasons = []
    if len(baselines) != 5:
        reasons.append('single screening pair cannot establish a stable comparison')
    cleanup_reasons = []
    for index, run in enumerate(candidates):
        observation = run.get('post_load_storage_cleanup')
        if not cleanup_drained(observation):
            state = observation.get('status', 'unavailable') if isinstance(observation, dict) else 'unavailable'
            cleanup_reasons.append(f'candidate pair {index + 1}: post-load journal cleanup {state}')
    reasons.extend(cleanup_reasons)
    values = [list(map(metrics, arm)) for arm in (baselines, candidates)]
    ratios, spans = {}, {}
    for metric in values[0][0]:
        if metric == 'successful_transactions':
            continue
        left, right = ([row[metric] for row in arm] for arm in values)
        if any(value is None or value <= 0 for value in (*left, *right)):
            ratios[metric] = None
            if 'p99' not in metric:
                reasons.append(f'{metric}: paired ratio unavailable from missing or zero observations')
            continue
        ratios[metric] = statistics.median(b / a for a, b in zip(left, right))
        spans[metric] = max(left) / min(left) - 1
    for metric in ('put_cycle_p50_seconds', 'get_cycle_p50_seconds', 'delete_cycle_p50_seconds', 'transactions_per_second'):
        if spans.get(metric, float('inf')) > .20:
            reasons.append(f'{metric}: baseline control span exceeds 20%')
    clients = [phase.get('client_cpu_cores') for run in baselines + candidates
               for phase in run['phase_observations'] if phase['phase'] == 'load']
    if any(value is None for value in clients):
        reasons.append('client CPU observations unavailable; saturation cannot be excluded')
    elif any(value >= .85 for value in clients):
        reasons.append('Python client approaches one CPU core; client saturation cannot be excluded')
    for operation in ('put', 'get', 'delete'):
        if any(row[f'{operation}_cycle_p99_seconds'] is None for arm in values for row in arm):
            reasons.append(f'per-cycle samples do not support a {operation.upper()} p99 claim')
    return {'status': 'INCONCLUSIVE' if reasons else 'PASS', 'reasons': reasons,
            'cleanup_qualification': {'status': 'INCONCLUSIVE' if cleanup_reasons else 'PASS',
                                      'reasons': cleanup_reasons,
                                      'scope': 'Committed protocol-2 intent/debt counts after final idle; no steady-state GC claim'},
            'baseline': values[0], 'candidate': values[1],
            'median_paired_candidate_to_baseline': ratios, 'baseline_relative_spans': spans,
            'scope': 'Observed lifecycle cost only; no throughput adoption or journal-startup decision'}


def cleanup_drained(observation):
    if (not isinstance(observation, dict) or observation.get('status') != 'drained'
            or observation.get('protocol') != {'minimum_reader': 2, 'minimum_writer': 2}
            or observation.get('boundary') != 'after_final_idle_before_server_stop'
            or observation.get('access') != 'sqlite_mode_ro'):
        return False
    try:
        elapsed = number(observation['elapsed_seconds'], 'cleanup elapsed')
        budget = number(observation['wait_budget_seconds'], 'cleanup budget', positive=True)
        samples = observation['samples']
        if not 0 < elapsed <= budget <= 5 or not samples:
            return False
        previous = -1
        for sample in samples:
            when = number(sample['elapsed_seconds'], 'cleanup snapshot elapsed')
            if not previous <= when <= elapsed:
                return False
            previous = when
            if any(type(sample[name]) is not int or sample[name] < 0 for name in ('write_intents', 'cleanups')):
                return False
        return samples[-1]['write_intents'] == samples[-1]['cleanups'] == 0
    except (KeyError, TypeError, ValueError):
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    parser.add_argument('--baseline', nargs='+', required=True)
    parser.add_argument('--candidate', nargs='+', required=True)
    parser.add_argument('--device', required=True)
    args = parser.parse_args()
    tokens = args.baseline + args.candidate
    if len(tokens) > 10 or len(set(tokens)) != len(tokens) or not all(map(valid_token, tokens)):
        parser.error('provide distinct owned tokens for at most five pairs')
    campaign = Campaign(args.root)
    try:
        token = campaign.admit('recovery', 30, 128 * 1024**2)
        start, status = time.monotonic(), 'FAIL'
        try:
            reports = [read_json(campaign.root / f'{run}.result.json') for run in tokens]
            validate_reports(reports, len(args.baseline), tokens, campaign.ledger['runs'])
            reduced = []
            for report in reports:
                if time.monotonic() - start > 25:
                    raise Unavailable('comparison reduction allowance exhausted')
                reduced.append(summarize_run(campaign.root, report, args.device))
            if time.monotonic() - start > 25:
                raise Unavailable('comparison reduction allowance exhausted')
            split = len(args.baseline)
            comparison = compare(reduced[:split], reduced[split:])
            atomic_json(campaign.root / f'{token}.export.json', {
                'kind': 'journal-cost', 'comparison': comparison, 'runs': reduced,
                'ledger_before_export': campaign.ledger,
                'reducer_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
            status = comparison['status']
            print(json.dumps({'export_id': token, 'comparison': comparison}))
        finally:
            campaign.finish(time.monotonic() - start, status, clean=True)
    finally:
        campaign.close()


if __name__ == '__main__':
    main()
