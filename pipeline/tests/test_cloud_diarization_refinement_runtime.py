from contextlib import contextmanager
from pathlib import Path
import unittest
from unittest.mock import patch

from pipeline import cloud_transcription as cloud
from pipeline.tests.test_cloud_transcription_runtime import CloudRuntimeTests


@unittest.skipUnless(hasattr(cloud, 'narrow'), 'requires refined cloud runtime')
class RefinedCloudTests(unittest.TestCase):
    def setUp(self):
        self.case = CloudRuntimeTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        row = self.case.add_recording()
        row['title'] = 'Interview with my wife'
        self.case.prepare()
        root = self.case.root / 'followup'
        cloud.io.mkdir(root)
        cloud.io.mkdir(root / 'jobs')
        self.policy_ref = cloud.io.put(root / 'policy.json', {
            'kind': cloud.narrow.KIND + '_policy', 'schema_version': 1,
            'state_root': str(root), 'plan': self.case.ref,
            'configuration': self.case.plan['screen_config'], 'text_recordings': [],
            'rules': cloud.narrow.RULES, 'implementation': cloud.narrow.implementation()})

    @contextmanager
    def enabled(self):
        with patch.object(cloud.release, 'active_diarization_policy', return_value=self.policy_ref):
            yield

    def test_high_risk_title_enables_and_preserves_original_screen(self):
        original = cloud.io.read_bytes(cloud.io.binding(self.case.folder() / 'screen.json'))
        with self.enabled():
            result = self.case.cycle()
        self.assertEqual(result['new_paid_requests'], 1)
        self.assertTrue(self.case.submits()[0][-1])
        self.assertEqual(cloud.io.read_bytes(cloud.io.binding(self.case.folder() / 'screen.json')), original)
        self.assertTrue((self.case.folder() / 'policy-screen.json').exists())

    def test_missing_followup_does_not_upload_or_reserve(self):
        with self.enabled(), patch.object(cloud.narrow, 'risk', return_value={
                'route': 'followup_required', 'reasons': []}):
            result = self.case.cycle()
        self.assertEqual(result['new_paid_requests'], 0)
        self.assertEqual(self.case.audio_calls, [])
        self.assertEqual(self.case.submits(), [])
        self.assertFalse((self.case.folder() / 'intent.json').exists())

    def test_existing_paid_request_keeps_original_setting(self):
        self.case.cycle()
        intent = cloud.io.read_bytes(cloud.io.binding(self.case.folder() / 'intent.json'))
        with self.enabled():
            self.case.clients['assemblyai'].poll_status = 'completed'
            result = self.case.cycle(allow_paid_api=False, budget_microusd=None)
        self.assertEqual(result['new_paid_requests'], 0)
        self.assertEqual(len(self.case.submits()), 1)
        self.assertEqual(cloud.io.read_bytes(cloud.io.binding(self.case.folder() / 'intent.json')), intent)
        self.assertFalse(cloud.io.read(cloud.io.binding(self.case.folder() / 'transcript.json'))['diarization_requested'])

    def test_new_policy_proof_is_checked_during_collection(self):
        with self.enabled():
            self.case.cycle()
            self.case.clients['assemblyai'].poll_status = 'completed'
            self.case.cycle(allow_paid_api=False, budget_microusd=None)
            selected = cloud.export(self.case.ref)['records']
        self.assertEqual(len(selected), 1)
        doc = cloud.io.read(selected[0]['transcript'])
        self.assertTrue(doc['diarization_requested'])
        self.assertIn('policy-screen.json', doc['screen_decision']['path'])


if __name__ == '__main__':
    unittest.main()
