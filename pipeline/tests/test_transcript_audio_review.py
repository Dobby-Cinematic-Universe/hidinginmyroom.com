import copy
import tempfile
from pathlib import Path
import unittest

from pipeline.transcript_audio_review import analyze, binding, reviewed_projection, select_clips


class ReviewTests(unittest.TestCase):
    def fixture(self):
        texts = ['Hello there', 'The donation text to speech is loud', 'quiet words']
        segments = [dict(start_ms=i*1000, end_ms=i*1000+900, text=t, speaker=f'S{i}') for i,t in enumerate(texts)]
        raw = dict(utterances=[dict(start=s['start_ms'], end=s['end_ms'], text=s['text'], speaker=str(i), confidence=.5 if i==2 else .9) for i,s in enumerate(segments)])
        return dict(segments=segments), raw

    def test_no_automatic_identity_or_exclusion(self):
        doc, raw = self.fixture()
        before = copy.deepcopy(doc)
        report = analyze(doc, raw)
        preview = reviewed_projection(report, [])
        self.assertEqual(doc, before)
        self.assertEqual(len(preview['segments']), 3)
        self.assertEqual(preview['summary_preview'], '')
        self.assertFalse(preview['production_eligible'])
        self.assertIsNone(report['verified_human_participant_count'])
        self.assertTrue(all(r['audio_source']=='unknown' for r in report['segments']))
        self.assertIn('donation_or_tts_context_not_source_proof', report['segments'][1]['review_flags'])

    def test_alignment_fail_closed(self):
        doc, raw = self.fixture()
        raw['utterances'][0]['text'] = 'wrong'
        with self.assertRaises(ValueError): analyze(doc, raw)

    def test_label_mapping_fail_closed(self):
        doc, raw = self.fixture()
        raw['utterances'][1]['speaker'] = '0'
        with self.assertRaises(ValueError): analyze(doc, raw)

    def test_invalid_confidence(self):
        doc, raw = self.fixture()
        raw['utterances'][0]['confidence'] = float('nan')
        with self.assertRaises(ValueError): analyze(doc, raw)

    def test_clip_cap(self):
        self.assertEqual(len(select_clips(analyze(*self.fixture())['segments'], 2)), 2)

    def test_reviewed_preview_preserves_evidence_and_strips_times(self):
        report = analyze(*self.fixture())
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)/'evidence.txt'
            p.write_text('review notes')
            decision = dict(segment_index=0, reviewer='test', evidence=[binding(p)], audio_source='participant', intelligibility='clear', identity='Daniel')
            preview = reviewed_projection(report, [decision])
            self.assertEqual(preview['summary_preview'], 'Daniel: Hello there')
            self.assertEqual(preview['segments'][0]['start_ms'], 0)
            self.assertEqual(preview['segments'][0]['speaker'], 'S0')
            with self.assertRaises(ValueError): reviewed_projection(report, [decision, decision])
            p.write_text('modified')
            with self.assertRaises(ValueError): reviewed_projection(report, [decision])

    def test_missing_evidence_rejected(self):
        with self.assertRaises(ValueError):
            reviewed_projection(analyze(*self.fixture()), [dict(segment_index=0, reviewer='x', evidence=[], audio_source='tts', intelligibility='clear')])


if __name__ == '__main__':
    unittest.main()
