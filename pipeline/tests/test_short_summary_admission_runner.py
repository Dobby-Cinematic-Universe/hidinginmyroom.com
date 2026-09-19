import unittest
from types import SimpleNamespace
from pipeline.short_summary_admission_runner import install, REASON


class AdmissionTest(unittest.TestCase):
    def test_boundary_and_identity_preserved(self):
        docs = {str(n): {'segments': [{'text': 'word ' * n}], 'title': 'ignored ' * 100} for n in (0, 49, 50, 90)}
        worker = SimpleNamespace(r=SimpleNamespace(read=lambda ref: docs[ref]),
            _identity_holds=lambda available: {'90': {'recording_id': '90', 'reason': 'speaker_identity_pending'}},
            _public=lambda manifest, snapshot: {'speaker_identity_holds': list(snapshot['identity_holds'].values())})
        install(worker)
        available = {key: {'transcript': key} for key in docs}
        held = worker._identity_holds(available)
        self.assertEqual(set(held), {'0', '49', '90'})
        self.assertEqual(held['49']['reason'], REASON)
        status = worker._public({}, {'identity_holds': held})
        self.assertEqual(status['short_transcripts_withheld'], 2)
        self.assertEqual(status['speaker_identity_pending_recording_ids'], ['90'])

    def test_original_validation_is_not_bypassed(self):
        def reject(_):
            raise ValueError('invalid source')
        worker = SimpleNamespace(_identity_holds=reject, _public=lambda *_: {})
        install(worker)
        with self.assertRaisesRegex(ValueError, 'invalid source'):
            worker._identity_holds({})
