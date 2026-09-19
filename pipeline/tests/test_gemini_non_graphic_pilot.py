from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from pipeline import gemini_non_graphic_pilot as pilot
from pipeline.tests.test_transcript_summary_core import source


class SanitizedPilotTests(unittest.TestCase):
    def setUp(self):
        self.job = pilot.r.core.initial_jobs([source(['First remark.', 'Second remark.', 'Unchanged ending.'])])[0]
        self.edits = [dict(first=1, last=2, text='A neutral description of the first two remarks.',
                           classification='uncertainty')]

    def test_original_unchanged_and_original_citations_retained(self):
        before = deepcopy(self.job)
        new = pilot.sanitize(self.job, self.edits)
        self.assertEqual(before, self.job)
        self.assertNotEqual(new['job_id'], before['job_id'])
        self.assertEqual(new['evidence'][-1], before['evidence'][-1])
        expected = pilot.r.core._unique_citations([c for e in before['evidence'][:2] for c in e['citations']])
        self.assertEqual(new['evidence'][0]['citations'], expected)
        self.assertTrue(new['evidence'][0]['text'].startswith(pilot.PREFIX))
        pilot.r.core.validate_job(new)

    def test_original_text_and_metadata_not_in_sanitized_wire(self):
        new = pilot.sanitize(self.job, self.edits)
        body = pilot.r.canonical(new['request']['body']).decode()
        for value in ('First remark.', 'Second remark.', 'start_ms', 'transcript_sha256', '/private/'):
            self.assertNotIn(value, body)
        self.assertIn('Unchanged ending.', body)

    def test_overlapping_out_of_range_empty_edits_rejected(self):
        for edits in ([], self.edits * 2, [{**self.edits[0], 'last': 100}],
                      [{**self.edits[0], 'text': ''}]):
            with self.subTest(edits=edits), self.assertRaises((ValueError, RuntimeError)):
                pilot.sanitize(self.job, edits)

    def test_unsafe_classification_not_invented(self):
        with self.assertRaises((ValueError, RuntimeError)):
            pilot.sanitize(self.job, [{**self.edits[0], 'classification':'verified_fact'}])

    def test_submission_is_idempotent_and_preserves_ambiguous_intent(self):
        for fails in (False, True):
            with self.subTest(fails=fails), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                job = pilot.sanitize(self.job, self.edits)
                wave = dict(wave_id='summarywave_'+'a'*32, provider='gemini',
                    model=job['model'], jobs=[job])
                manifest = dict(state_root=str(root), wave=wave, maximum_cost_microusd=100)
                ref = dict(path=str(root/'manifest.json'),sha256='b'*64)
                client = Mock()
                client.batch_bytes.return_value = b'{}'
                client.create_batch.return_value = dict(name='batches/test', done=False,
                    metadata=dict(state='BATCH_STATE_PENDING', displayName=wave['wave_id'],
                                  model='models/'+wave['model']))
                with patch.object(pilot, 'load', return_value=manifest):
                    if fails:
                        client.create_batch.side_effect = pilot.r.client_module.BatchClientError('transport', ambiguous=True)
                        with self.assertRaises(pilot.r.client_module.BatchClientError):
                            pilot.submit(ref, client=client)
                    else:
                        self.assertEqual(pilot.submit(ref, client=client)['state'], 'submitted')
                    result = pilot.submit(ref, client=client)
                    self.assertEqual(result['state'], 'needs_reconciliation' if fails else 'already_submitted')
                    client.create_batch.assert_called_once()
                    self.assertTrue((root/'submit-intent.json').exists())


if __name__ == '__main__':
    unittest.main()
