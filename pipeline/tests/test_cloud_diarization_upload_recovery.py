import json
from pathlib import Path
import tempfile
import unittest

from pipeline import cloud_diarization_upload_recovery as recovery
from pipeline import transcript_summary as io


class RecoveryTests(unittest.TestCase):
    def test_any_paid_evidence_prevents_another_blob_retry(self):
        with tempfile.TemporaryDirectory() as temp:
            folder=Path(temp)/'job';folder.mkdir(mode=0o700)
            reservations=Path(temp)/'reservations';reservations.mkdir(mode=0o700)
            self.assertTrue(recovery.eligible_for_blob_retry(folder,reservations,'job'))
            for name in ('intent.json','submission.json','reconciled.json','completion.json',
                         'terminal-job.json','provider-transcript.json','submission-untrusted-response.json'):
                path=folder/name;path.touch()
                self.assertFalse(recovery.eligible_for_blob_retry(folder,reservations,'job'),name)
                path.unlink()
            (reservations/'job.json').touch()
            self.assertFalse(recovery.eligible_for_blob_retry(folder,reservations,'job'))

    def test_only_exact_original_hold_is_released_and_retained(self):
        with tempfile.TemporaryDirectory() as temp:
            folder=Path(temp);original=dict(reason='cloud request transport failed',job_id='job')
            io.put(folder/'hold.json',original);raw=(folder/'hold.json').read_bytes()
            recovery.release_upload_hold(folder,original,io)
            self.assertFalse((folder/'hold.json').exists())
            self.assertEqual((folder/'hold-resolved-upload-recovery-20260915.json').read_bytes(),raw)
            newer=dict(reason='invalid word timestamp',job_id='job');io.put(folder/'hold.json',newer)
            recovery.release_upload_hold(folder,original,io)
            self.assertEqual(io.read(io.binding(folder/'hold.json')),newer)

    def test_unrelated_hold_cannot_be_cleared(self):
        with tempfile.TemporaryDirectory() as temp:
            folder=Path(temp);io.put(folder/'hold.json',dict(reason='different'))
            with self.assertRaises(RuntimeError):
                recovery.release_upload_hold(folder,dict(reason='cloud request transport failed'),io)
            self.assertTrue((folder/'hold.json').exists())


if __name__=='__main__':unittest.main()
