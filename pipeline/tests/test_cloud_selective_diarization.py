from contextlib import contextmanager
from copy import deepcopy
import unittest
from unittest.mock import patch

from pipeline import cloud_transcription as cloud
from pipeline import cloud_transcription_selective_diarization as selective
from pipeline.tests.test_cloud_transcription_runtime import CloudRuntimeTests


class SelectiveRouteTests(unittest.TestCase):
    def route(self, *, title='Solo gaming stream', state='screen_uncertain', faces=0, leads=()):
        return selective.route({'recording_id': 'one', 'title': title, 'aliases': []},
            {'state': state, 'evidence_summary': {'multiple_face_samples': faces}}, leads)

    def test_acoustic_positive_alone_is_off(self):
        self.assertFalse(self.route(state='screen_positive')['diarization'])

    def test_uncertain_alone_is_off(self):
        self.assertFalse(self.route()['diarization'])

    def test_negative_alone_is_off(self):
        self.assertFalse(self.route(state='screen_negative')['diarization'])

    def test_multiple_faces_keep_diarization(self):
        self.assertTrue(self.route(state='screen_negative', faces=1)['diarization'])

    def test_exact_transcript_keyword_lead_keeps_diarization(self):
        self.assertTrue(self.route(state='screen_negative', leads=['one'])['diarization'])
        self.assertFalse(self.route(leads=['another'])['diarization'])

    def test_conversation_title_keeps_diarization(self):
        self.assertTrue(self.route(title='FIRST Stream with my Girlfriend!')['diarization'])
        self.assertTrue(self.route(title='Interview with my sister')['diarization'])

    def test_context_title_does_not_enable(self):
        self.assertFalse(self.route(title='Talking about my girlfriend')['diarization'])

    def test_no_identity_or_solo_claim(self):
        result = self.route(faces=3)
        self.assertFalse(result['whole_recording_solo_proven'])
        self.assertFalse(result['speaker_identity_inferred'])


@unittest.skipUnless(hasattr(cloud, 'selective'), 'requires selective versioned runtime')
class SelectiveRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.case = CloudRuntimeTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def prepare(self, *, positive=False, uncertain=False, title='Solo stream', lead=False):
        row = self.case.add_recording()
        row['title'] = title
        self.case.screen_positive = positive
        self.case.screen_uncertain = uncertain
        self.case.prepare()
        root = self.case.root / 'legacy-policy'
        cloud.io.mkdir(root)
        legacy_ref = cloud.io.put(root/'policy.json', {
            'kind': cloud.narrow.KIND + '_policy', 'schema_version': 1,
            'state_root': str(root), 'plan': self.case.ref,
            'configuration': self.case.plan['screen_config'],
            'text_recordings': [row['recording_id']] if lead else [],
            'rules': cloud.narrow.RULES, 'implementation': cloud.narrow.implementation()})
        self.ref = selective.prepare(self.case.ref, legacy_ref, self.case.root/'selective'/'policy.json')

    @contextmanager
    def enabled(self):
        with patch.object(cloud.release, 'active_selective_policy', return_value=self.ref):
            yield

    def test_positive_only_disabled_before_paid_request_and_collected(self):
        self.prepare(positive=True)
        before = cloud.io.binding(self.case.folder()/'screen.json')
        with self.enabled():
            self.assertEqual(self.case.cycle()['new_paid_requests'], 1)
            self.assertFalse(self.case.submits()[0][-1])
            intent = cloud.io.read(cloud.io.binding(self.case.folder()/'intent.json'))
            self.assertEqual(intent['maximum_cost_microusd'], cloud.cost_bound('assemblyai', 60000, False))
            self.case.clients['assemblyai'].poll_status = 'completed'
            self.case.cycle(allow_paid_api=False, budget_microusd=None)
            self.assertEqual(len(cloud.export(self.case.ref)['records']), 1)
        self.assertEqual(cloud.io.binding(self.case.folder()/'screen.json'), before)
        doc = cloud.io.read(cloud.io.binding(self.case.folder()/'transcript.json'))
        self.assertFalse(doc['diarization_requested'])
        self.assertTrue(doc['screen_decision']['path'].endswith('/selective-screen.json'))

    def test_uncertain_multiple_faces_kept_without_new_followups(self):
        self.prepare(uncertain=True)
        with self.enabled(), patch.object(cloud.narrow, 'effective', side_effect=AssertionError('must not run followup')):
            self.assertEqual(self.case.cycle()['new_paid_requests'], 1)
        self.assertTrue(self.case.submits()[0][-1])

    def test_transcript_lead_still_enables(self):
        self.prepare(lead=True)
        with self.enabled(): self.case.cycle()
        self.assertTrue(self.case.submits()[0][-1])

    def test_title_still_enables(self):
        self.prepare(title='Interview with my wife')
        with self.enabled(): self.case.cycle()
        self.assertTrue(self.case.submits()[0][-1])

    def test_paid_positive_remains_frozen(self):
        self.prepare(positive=True)
        self.case.cycle()
        before = cloud.io.binding(self.case.folder()/'intent.json')
        with self.enabled():
            self.case.clients['assemblyai'].poll_status = 'completed'
            self.case.cycle(allow_paid_api=False, budget_microusd=None)
        self.assertEqual(cloud.io.binding(self.case.folder()/'intent.json'), before)
        self.assertEqual(len(self.case.submits()), 1)
        doc = cloud.io.read(cloud.io.binding(self.case.folder()/'transcript.json'))
        self.assertTrue(doc['diarization_requested'])

    def test_orphan_reservation_frozen(self):
        self.prepare(positive=True)
        self.case.cycle()
        (self.case.folder()/'intent.json').rename(self.case.folder()/'intent.saved')
        (self.case.folder()/'submission.json').rename(self.case.folder()/'submission.saved')
        with self.enabled():
            self.assertEqual(self.case.cycle()['state'], 'reconciliation_required')
        self.assertEqual(len(self.case.submits()), 1)

    def test_corrupt_selective_proof_cannot_repost(self):
        self.prepare(positive=True)
        with self.enabled():
            self.case.cycle()
            path = self.case.folder()/'selective-screen.json'
            value = cloud.io.read(cloud.io.binding(path))
            value['diarization'] = True
            self.case.rewrite(path, value)
            with self.assertRaises(RuntimeError): self.case.cycle()
        self.assertEqual(len(self.case.submits()), 1)

    def test_preview_does_not_persist(self):
        self.prepare(uncertain=True)
        with self.enabled(): cloud.status(self.case.ref)
        self.assertFalse((self.case.folder()/'selective-screen.json').exists())


if __name__ == '__main__':
    unittest.main()
