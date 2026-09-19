"""Focused routing tests; run from the staged title runtime, never real APIs."""
from contextlib import contextmanager
from pathlib import Path
import unittest
from unittest.mock import patch

from pipeline import cloud_transcription as cloud
from pipeline import cloud_transcription_title_policy as titles
from pipeline import transcript_summary as io
from pipeline.tests.test_cloud_transcription_runtime import CloudRuntimeTests


@unittest.skipUnless(hasattr(cloud, 'release'), 'requires staged title-routing runtime')
class TitleRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.case = CloudRuntimeTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.policy = io.put(self.case.root / 'title-policy.json', titles.prepare_policy())

    def prepare(self, title='Interview with my wife'):
        row = self.case.add_recording()
        row['title'] = title
        self.case.prepare()
        self.folder = self.case.folder()

    @contextmanager
    def enabled(self):
        # Only policy selection is injected here. Source/screen/result/budget
        # validators, durable intents, normalizer, and paid-call ordering are real.
        # The execution-release module separately tests old/new code admission.
        with patch.object(cloud.release, 'active_title_policy', return_value=self.policy):
            yield

    def test_title_negative_enables_diarization_without_changing_screen(self):
        self.prepare()
        before = io.read_bytes(io.binding(self.folder / 'screen.json'))
        with self.enabled():
            result = self.case.cycle()
        self.assertEqual(result['new_paid_requests'], 1)
        self.assertEqual(self.case.submits()[0][-1], True)
        self.assertEqual(io.read_bytes(io.binding(self.folder / 'screen.json')), before)
        effective = io.read(io.binding(self.folder / 'effective-screen.json'))
        self.assertEqual(effective['state'], 'screen_negative')
        self.assertTrue(effective['method']['title_override']['applied'])

    def test_mentions_stay_off(self):
        self.prepare('Thoughts about my girlfriend')
        with self.enabled():
            self.case.cycle()
        self.assertFalse(self.case.submits()[0][-1])
        self.assertFalse((self.folder / 'effective-screen.json').exists())

    def test_preview_does_not_write_and_budget_checks_higher_price_first(self):
        self.prepare()
        with self.enabled():
            status = cloud.status(self.case.ref)
            self.assertEqual(status['title_diarization_overrides'], 1)
            self.assertFalse((self.folder / 'effective-screen.json').exists())
            lower = cloud.cost_bound('assemblyai', 60000, False)
            result = self.case.cycle(budget_microusd=lower)
        self.assertEqual(result['state'], 'budget_paused')
        self.assertEqual(self.case.audio_calls, [])
        self.assertEqual(self.case.submits(), [])
        self.assertFalse((self.folder / 'effective-screen.json').exists())

    def test_existing_paid_false_does_not_change_on_policy_activation(self):
        self.prepare()
        self.case.cycle()
        original = io.read_bytes(io.binding(self.folder / 'intent.json'))
        with self.enabled():
            status = cloud.status(self.case.ref)
            self.assertEqual(status['title_diarization_overrides'], 0)
            self.case.clients['assemblyai'].poll_status = 'completed'
            self.case.cycle(allow_paid_api=False, budget_microusd=None)
        self.assertEqual(len(self.case.submits()), 1)
        self.assertEqual(io.read_bytes(io.binding(self.folder / 'intent.json')), original)
        self.assertFalse(io.read(io.binding(self.folder / 'transcript.json'))['diarization_requested'])

    def test_orphan_reservation_keeps_original_false_decision(self):
        self.prepare()
        self.case.cycle()
        (self.folder / 'intent.json').rename(self.folder / 'intent.saved')
        (self.folder / 'submission.json').rename(self.folder / 'submission.saved')
        with self.enabled():
            result = self.case.cycle()
        self.assertEqual(result['state'], 'reconciliation_required')
        self.assertEqual(result['reserved_microusd'], cloud.cost_bound('assemblyai', 60000, False))
        self.assertEqual(len(self.case.submits()), 1)
        self.assertFalse((self.folder / 'effective-screen.json').exists())

    def test_effective_completion_normalizes_and_anonymous_gate_still_holds(self):
        from pipeline import cloud_transcription_summary as summaries
        self.prepare()
        with self.enabled():
            self.case.cycle()
            self.case.clients['assemblyai'].poll_status = 'completed'
            self.case.cycle(allow_paid_api=False, budget_microusd=None)
            source = cloud.export(self.case.ref)['records'][0]
        spec = {key: source[key] for key in ('recording_id', 'format', 'transcript', 'completion')}
        spec.update(title=None, date=None)
        normalized = io.sources_module.normalize_source(spec)
        self.assertEqual(normalized['segments'][0]['speaker'], 'SPEAKER_0000')
        self.assertIn(source['recording_id'], summaries._identity_holds({source['recording_id']: source}))

    def test_changed_effective_proof_stops_paid_work(self):
        self.prepare()
        with self.enabled():
            self.case.cycle()
            proof_path = self.folder / 'effective-screen.json'
            value = io.read(io.binding(proof_path))
            value['diarization'] = False
            self.case.rewrite(proof_path, value)
            with self.assertRaises(RuntimeError):
                self.case.cycle()
        self.assertEqual(len(self.case.submits()), 1)


if __name__ == '__main__':
    unittest.main()
