import unittest
from pipeline import corpus_summary_preview as p

class PreviewTests(unittest.TestCase):
    def fixture(self):
        meta=dict(recording_id='media_sha256_test',title='Example',duration_ms=2000,
            aliases=[dict(platform='internet_archive',canonical_url='https://archive.org/download/example/video.mp4',source_native_id='example/video.mp4')])
        doc=dict(kind='himr_cloud_recording_transcript',status='completed',recording_id=meta['recording_id'],
            segments=[dict(start_ms=100,end_ms=1500,text='Original words.',speaker='Daniel')])
        return meta,doc,dict(sha256='a'*64),dict(value=None,basis='unknown')

    def test_exact_words_timing_and_no_claim_of_full_human_review(self):
        row=p.project_record(*self.fixture());rev=row['transcript_revisions'][0]
        self.assertEqual(rev['segments'][0]['start_ms'],100)
        self.assertEqual(rev['segments'][0]['text'],'Original words.')
        self.assertTrue(rev['machine_generated']);self.assertTrue(rev['unreviewed'])
        self.assertFalse(rev['verified_quotation'])

    def test_rejects_local_asr_and_invalid_timing(self):
        values=self.fixture();values[1]['kind']='local_asr'
        with self.assertRaises(ValueError):p.project_record(*values)
        values=self.fixture();values[1]['segments'][0]['end_ms']=100
        with self.assertRaises(ValueError):p.project_record(*values)

    def test_rejects_private_and_signed_urls(self):
        for url in ['file:///private/video','https://example.test/video?token=secret','https://archive.org/download/example/video' + '?signature=secret']:
            values=self.fixture();values[0]['aliases'][0]['canonical_url']=url
            with self.assertRaises(ValueError):p.project_record(*values)

    def test_speaker_uncertainty_and_noise_are_not_identity_claims(self):
        self.assertEqual(p.speaker(dict(speaker_name='Daniel',attribution_uncertainty=True)),'Daniel (uncertain)')
        self.assertEqual(p.speaker(dict(speaker_name='Daniel',audio_source='background_noise')),'Background noise')

    def test_summary_is_prepared_not_publication_approved(self):
        doc=dict(date={'value':None},title='Example',sections={'summary':[dict(text='A summary',classification='uncertainty')]})
        row=p.summary_row(doc,'rec_test')
        self.assertEqual(row['publication'],'prepared');self.assertIsNone(row['period'])
        self.assertEqual(row['sections']['summary'][0]['source_recording_ids'],['rec_test'])

    def test_metadata_only_entry_does_not_invent_text(self):
        meta,_,_,date=self.fixture();row=p.metadata_record(meta,date)
        self.assertEqual(row['review_state'],'metadata_only')
        self.assertEqual(row['transcript_revisions'],[])

if __name__=='__main__':unittest.main()
