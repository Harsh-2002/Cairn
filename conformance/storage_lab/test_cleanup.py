"""Bounded live-WAL observation never supplies cleanup or a missing measurement."""
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from lab import observe_storage_cleanup


class CleanupObservation(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / 'metadata.db'
        self.writer = sqlite3.connect(self.database, isolation_level=None)
        self.addCleanup(self.writer.close)
        self.writer.execute('PRAGMA journal_mode=WAL')
        self.writer.executescript('''
            CREATE TABLE storage_protocol(singleton,minimum_reader,minimum_writer);
            INSERT INTO storage_protocol VALUES(1,2,2);
            CREATE TABLE storage_write_intents(id);
            CREATE TABLE storage_cleanups(id);
        ''')
        self.now = 100.

    def observe(self, *, duration=10, alive=lambda: True, on_sleep=lambda: None):
        def sleep(seconds):
            self.now += seconds
            on_sleep()
        with patch('lab.time', SimpleNamespace(monotonic=lambda: self.now, sleep=sleep)):
            return observe_storage_cleanup(self.database, self.now + duration, alive)

    def test_reads_live_wal_then_observes_owned_worker_drain(self):
        self.writer.execute('INSERT INTO storage_write_intents VALUES(1)')
        self.writer.execute('INSERT INTO storage_cleanups VALUES(2)')
        def worker():
            self.writer.executescript('DELETE FROM storage_write_intents; DELETE FROM storage_cleanups;')
        result = self.observe(on_sleep=worker)
        self.assertEqual(result['status'], 'drained')
        self.assertEqual([(sample['write_intents'], sample['cleanups']) for sample in result['samples']],
                         [(1, 1), (0, 0)])
        self.assertAlmostEqual(result['elapsed_seconds'], .2)

    def test_residual_debt_is_bounded_by_work_deadline_without_writes(self):
        self.writer.execute('INSERT INTO storage_cleanups VALUES(2)')
        statements, connections = [], []
        connect = sqlite3.connect
        def read_connection(uri, **kwargs):
            self.assertTrue(uri.endswith('?mode=ro'))
            connection = connect(uri, **kwargs)
            connection.set_trace_callback(statements.append)
            connections.append(connection)
            return connection
        with patch('lab.sqlite3.connect', side_effect=read_connection):
            result = self.observe(duration=.45)
        self.assertEqual(result['status'], 'residual')
        self.assertAlmostEqual(result['elapsed_seconds'], .45)
        self.assertEqual(result['samples'][-1]['cleanups'], 1)
        self.assertTrue(all(statement.startswith(('SELECT ', 'PRAGMA query_only=ON')) for statement in statements))
        self.assertEqual(self.writer.execute('SELECT COUNT(*) FROM storage_cleanups').fetchone()[0], 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            connections[0].execute('SELECT 1')

    def test_wait_never_extends_past_five_seconds(self):
        self.writer.execute('INSERT INTO storage_cleanups VALUES(2)')
        result = self.observe()
        self.assertEqual(result['status'], 'residual')
        self.assertEqual(result['wait_budget_seconds'], 5)
        self.assertEqual(result['elapsed_seconds'], 5)
        self.assertLessEqual(len(result['samples']), 26)

    def test_baseline_without_journal_is_explicitly_unsupported(self):
        self.writer.execute('UPDATE storage_protocol SET minimum_reader=1,minimum_writer=1')
        self.assertEqual(self.observe()['status'], 'unsupported')
        self.writer.execute('DROP TABLE storage_protocol')
        self.assertEqual(self.observe()['status'], 'unsupported')

    def test_missing_candidate_table_or_server_is_not_a_drain(self):
        self.writer.execute('DROP TABLE storage_cleanups')
        result = self.observe()
        self.assertEqual(result['status'], 'unavailable')
        self.assertIn('no such table', result['reason'])
        self.assertEqual(self.observe(alive=lambda: False)['status'], 'unavailable')

    def test_missing_database_is_never_created(self):
        self.database = self.database.with_name('missing.db')
        self.assertEqual(self.observe()['status'], 'unavailable')
        self.assertFalse(self.database.exists())

    def test_no_time_left_does_not_open_database(self):
        with patch('lab.sqlite3.connect') as connect:
            result = self.observe(duration=0)
        connect.assert_not_called()
        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(result['samples'], [])

    def test_server_exit_after_snapshot_invalidates_observation(self):
        answers = iter((True, True, False))
        result = self.observe(alive=lambda: next(answers))
        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(result['samples'], [])


if __name__ == '__main__':
    unittest.main()
