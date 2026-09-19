"""Restart reuse and invalidation without real provider calls."""
import json
from contextlib import closing
from pathlib import Path
import sqlite3
import subprocess
import sys
import unittest
from unittest.mock import patch

from pipeline import cloud_transcription_summary as worker
from pipeline import cloud_transcription_summary_cache as cache
from pipeline.tests import test_cloud_transcription_summary as fixtures


@unittest.skipUnless(hasattr(cache, '_Disk'), 'requires persistent runtime')
class PersistentTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.WorkerTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.case.prepare()

    def warm(self):
        with cache.scope(self.case.ref, persistent=True):
            self.case.cycle()
            self.case.client.complete = True
            self.case.cycle()
            result = self.case.cycle()
            self.assertEqual(result['transcript_summaries_complete'], 1)

    def test_cold_scope_reuses_record_and_export_without_paid_replay(self):
        self.warm()
        with cache.scope(self.case.ref, persistent=True) as state, \
                patch.object(worker, '_record_snapshot', side_effect=AssertionError('record rebuilt')), \
                patch.object(worker.r, 'export_plan', side_effect=AssertionError('export rebuilt')):
            result = self.case.cycle()
            self.assertEqual(result['new_paid_requests'], 0)
            self.assertEqual(result['transcript_summaries_complete'], 1)
            self.assertGreaterEqual(state.statistics()['disk_hits'], 2)
        self.assertEqual(len(self.case.client.created), 2)

    def test_changed_record_forces_original_validator(self):
        self.warm()
        record = next((self.case.worker_root / 'records').iterdir())
        worker.r.put(record / 'new-proof.json', {'changed': True})
        with cache.scope(self.case.ref, persistent=True), patch.object(worker, '_record_snapshot', wraps=worker._record_snapshot) as validator:
            self.case.snapshot()
            self.assertEqual(validator.call_count, 1)

    def test_missing_root_reservation_still_fails_with_persistent_hit(self):
        self.warm()
        reservation = next((self.case.worker_root / 'reservations').glob('*.json'))
        reservation.rename(reservation.with_suffix('.saved'))
        with cache.scope(self.case.ref, persistent=True):
            with self.assertRaises(RuntimeError):
                self.case.snapshot()

    def disk(self, namespace='test-namespace'):
        return cache._Disk({'state_root': str(self.case.worker_root)}, namespace)

    def test_payload_corruption_and_code_namespace_mismatch_are_misses(self):
        disk = self.disk()
        disk.put('record', b'key', b'witness', b'{"safe":true}')
        self.assertIsNotNone(disk.get('record', b'key', b'witness'))
        self.assertIsNone(self.disk('other-code').get('record', b'key', b'witness'))
        with closing(sqlite3.connect(disk.root / 'cache.sqlite3')) as connection, connection:
            connection.execute('UPDATE snapshots SET payload=?', (b'{"safe":false}',))
        self.assertIsNone(disk.get('record', b'key', b'witness'))

    def test_corrupt_database_is_disposable_miss(self):
        disk = self.disk()
        worker.r.mkdir(disk.root)
        worker.r.put_bytes(disk.root / 'cache.sqlite3', b'not sqlite')
        self.assertIsNone(disk.get('record', b'key', b'witness'))
        disk.put('record', b'key', b'witness', b'{}')
        self.assertGreater(disk.counts['disk_errors'], 0)

    def test_symlink_is_rejected_without_touching_target(self):
        disk = self.disk()
        worker.r.mkdir(disk.root)
        target = self.case.worker_root / 'manifest.json'
        before = target.read_bytes()
        (disk.root / 'cache.sqlite3').symlink_to(target)
        self.assertIsNone(disk.get('record', b'key', b'witness'))
        disk.put('record', b'key', b'witness', b'{}')
        self.assertEqual(target.read_bytes(), before)

    def test_real_new_process_can_read_snapshot(self):
        disk = self.disk()
        disk.put('record', b'key', b'witness', b'{"safe":true}')
        code = 'from pipeline.cloud_transcription_summary_cache import _Disk; import sys; d=_Disk({"state_root":sys.argv[1]},"test-namespace"); assert d.get("record",b"key",b"witness")[1]==b\'{"safe":true}\'; print("restart-hit")'
        result = subprocess.run([sys.executable, '-B', '-c', code, str(self.case.worker_root)],
                                text=True, capture_output=True, timeout=30, check=True)
        self.assertEqual(result.stdout.strip(), 'restart-hit')

    def test_disk_bounds_evict_old_entries_and_skip_oversize(self):
        disk = self.disk()
        with patch.object(cache, 'MAX_DISK_BYTES', 6):
            disk.put('record', b'one', b'w', b'{}')
            disk.put('record', b'two', b'w', b'{"a":1}')
        self.assertIsNone(disk.get('record', b'one', b'w'))
        self.assertIsNone(disk.get('record', b'two', b'w'))
        with patch.object(cache, 'MAX_DISK_VALUE_BYTES', 1):
            disk.put('record', b'large', b'w', b'{}')
        self.assertIsNone(disk.get('record', b'large', b'w'))

    def test_interrupted_transaction_does_not_publish_partial_snapshot(self):
        disk = self.disk()
        disk.put('record', b'key', b'witness', b'{"safe":true}')
        with self.assertRaisesRegex(RuntimeError, 'interrupted'):
            with disk.connection() as connection, connection:
                connection.execute('UPDATE snapshots SET payload=?', (b'{"partial":true}',))
                raise RuntimeError('interrupted')
        self.assertEqual(disk.get('record', b'key', b'witness')[1], b'{"safe":true}')

    def test_changed_external_source_rejects_persistent_reuse(self):
        self.warm()
        source = Path(self.case.available[0]['transcript']['path'])
        source.chmod(0o600)
        source.write_bytes(source.read_bytes() + b' ')
        with cache.scope(self.case.ref, persistent=True):
            with self.assertRaises(RuntimeError):
                self.case.snapshot()


if __name__ == '__main__':
    unittest.main()
