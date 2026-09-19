from copy import deepcopy
import unittest

from pipeline import cloud_transcription_diarization_policy as p


class RiskPolicyTests(unittest.TestCase):
    def recording(self, title='An ordinary update'):
        return {'recording_id': 'recording-1', 'title': title, 'aliases': []}

    def base(self, state='screen_uncertain', faces=0):
        return {'state': state, 'evidence_summary': {'multiple_face_samples': faces}}

    def test_risk_signals_and_low_risk_uncertainty(self):
        policy = {'text_recordings': []}
        self.assertEqual(p.risk(self.recording(), self.base(), policy)['route'], 'followup_required')
        for record, base in ((self.recording(), self.base('screen_positive')),
                             (self.recording('Interview with my wife'), self.base()),
                             (self.recording(), self.base(faces=2))):
            self.assertEqual(p.risk(record, base, policy)['route'], 'diarize')
        self.assertEqual(p.risk(self.recording(), self.base('screen_negative'), policy)['route'], 'no_diarization')
        self.assertEqual(p.risk(self.recording(), self.base(), {'text_recordings': ['recording-1']})['route'], 'diarize')

    def test_windows_are_bounded_and_disjoint_across_whole_span(self):
        for duration in (5000, 40000, 80000, 10 * 3600000):
            windows = p.windows(100, duration)
            self.assertLessEqual(len(windows), 8)
            for i, row in enumerate(windows):
                self.assertTrue(100 <= row['start_ms'] < row['end_ms'] <= duration)
                self.assertLessEqual(row['end_ms'] - row['start_ms'], 10000)
                if i:
                    self.assertGreaterEqual(row['start_ms'], windows[i-1]['end_ms'])

    def samples(self, speakers=None, omit=(), failure=()):
        job = {'windows': p.windows(0, 80000), 'base_excerpts': [], 'visual_uncertain': False}
        rows = []
        for window in job['windows']:
            i = window['index'];start, end = window['start_ms'], window['end_ms']
            vector = [0.0] * 192;vector[(speakers or {}).get(i, 0)] = 1.0
            excerpts = [] if i in omit or i in failure else [{
                'id': f'fresh-{i}-0', 'probe_id': f'fresh-{i}', 'start_ms': start,
                'end_ms': start + 2500, 'speech_ms': 2500, 'embedding': vector}]
            rows.append({'state': 'needs_review' if i in failure else 'analyzed', 'window': window,
                'job_sha256': p.io.digest(job), 'excerpts': excerpts, 'legacy_eligible_excerpts': 0,
                'vad_positive_ms': 2500 if excerpts else 0,
                'receipt': {'requested_start_ms': start, 'requested_end_ms': end,
                    'source_pts_verified': True, 'silence_padding': False, 'pcm_sha256': 'a' * 64,
                    'start_ms': start, 'end_ms': end, 'short_sample': False, 'decoded_samples': (end-start)*16,
                    'discarded_samples': 0, 'timestamp_discontinuities': 0, 'waveform_retimed': False}})
        return job, rows

    def test_adequate_single_group_followup_can_disable(self):
        job, rows = self.samples()
        result = p.classify(job, rows)
        self.assertEqual(result['state'], 'screen_negative')
        self.assertFalse(result['diarization'])
        self.assertFalse(result['whole_recording_solo_proven'])

    def test_supported_diversity_keeps_enabled(self):
        job, rows = self.samples(speakers={4: 1, 5: 1, 6: 1, 7: 1})
        self.assertEqual(p.classify(job, rows)['state'], 'screen_positive')

    def test_insufficient_or_failed_followup_stays_enabled(self):
        for options in ({'omit': (6, 7)}, {'failure': (0,)}, {'omit': tuple(range(8))}):
            job, rows = self.samples(**options)
            result = p.classify(job, rows)
            self.assertEqual(result['state'], 'screen_uncertain')
            self.assertTrue(result['diarization'])

    def test_missing_or_foreign_checkpoint_rejected(self):
        job, rows = self.samples()
        with self.assertRaises(RuntimeError):p.classify(job, rows[:-1])
        rows[0]['job_sha256'] = 'f' * 64
        with self.assertRaises(RuntimeError):p.classify(job, rows)


if __name__ == '__main__':
    unittest.main()
