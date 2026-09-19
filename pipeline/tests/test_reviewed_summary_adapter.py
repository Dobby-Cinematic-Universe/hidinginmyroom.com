from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from pipeline import reviewed_transcript_feed as feed
from pipeline import reviewed_summary_adapter as adapter
from pipeline import transcript_summary as runner
from pipeline import transcript_summary_sources as sources
from pipeline import cloud_transcription_summary as worker


class ReviewedAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.original = dict(kind='himr_cloud_recording_transcript', schema_version=1,
            status='completed', recording_id='recording-test', job_id='job1', segments=[
                dict(text='A substantive word. ' * 20, speaker=label,
                     start_ms=i * 5000, end_ms=(i + 1) * 5000)
                for i, label in enumerate(['A', 'B', 'C'])])
        self.original_ref = feed.atomic(self.root / 'original.json', self.original)
        self.confirmed = feed.atomic(self.root / 'confirmed.json', dict(records=[]))
        self.reviews = [dict(transcript=self.original_ref, decision=dict(job_id='job1',
            label=label, scope='label', segment_index=0, source=source, name=name))
            for label, source, name in [('A', 'participant', 'Daniel'),
                                       ('B', 'uncertain', ''), ('C', 'tts', '')]]
        self.review_refs = [feed.atomic(self.root / ('review-' + str(i) + '.json'), value)
                            for i, value in enumerate(self.reviews)]
        self.copy = self.project()
        self.doc = adapter.render(self.copy, 'recording-test', sources)
        self.spec = dict(recording_id='recording-test', format='cloud', title=None, date=None,
            transcript=feed.atomic(self.root / 'rendered.json', self.doc), completion=self.copy)
        self.addCleanup(adapter.install_sources(sources))

    def project(self):
        doc, _, _ = feed.project(self.original, self.original_ref, self.reviews)
        doc['projection'].update(review_decisions=self.review_refs, confirmations=self.confirmed,
                                 implementation=feed.bound(feed.__file__))
        return feed.immutable(self.root, 'reviewed', doc)

    def test_reviewed_evidence_has_honest_rendered_provenance_and_original_turn_links(self):
        normalized = sources.normalize_source(self.spec)
        self.assertEqual(normalized['provenance']['source_kind'], adapter.KIND)
        self.assertEqual(normalized['provenance']['completion_evidence'], self.copy)
        self.assertFalse(normalized['provenance']['verified_quotation'])
        self.assertEqual(len(normalized['segments']), 2)
        self.assertTrue(normalized['segments'][0]['text'].startswith('Daniel: '))
        self.assertTrue(normalized['segments'][1]['text'].startswith('Uncertain speaker: '))
        self.assertEqual(normalized['segments'][1]['start_ms'], 5000)
        self.assertEqual(sources.validate_source(normalized), normalized)
        self.assertEqual(feed.read(self.original_ref), self.original)

    def test_model_prompt_retains_manual_names_but_not_timestamps_or_metadata(self):
        source = sources.normalize_source(self.spec)
        config = runner.core.normalize_config({**runner.core.DEFAULT_CONFIG,
            'transcript_profile':'gemini_flash_batch',
            'transcript_input_policy':'text_and_speaker_evidence_v1',
            'gemini_schema_policy':'local_array_bounds_v2'})
        jobs = runner.core.initial_jobs([source], config)
        self.assertTrue(jobs)
        body = runner.canonical(jobs[0]['request']['body']).decode()
        self.assertIn('Daniel:', body)
        self.assertIn('Uncertain speaker:', body)
        for omitted in ('start_ms', 'end_ms', 'original_segment_index', str(self.root), 'job1'):
            self.assertNotIn(omitted, body)

    def test_rendered_text_or_saved_review_tampering_is_rejected(self):
        tampered = deepcopy(self.doc)
        tampered['segments'][1]['text'] = tampered['segments'][1]['text'].replace('Uncertain speaker', 'Daniel')
        spec = {**self.spec, 'transcript': feed.atomic(self.root / 'tampered.json', tampered)}
        with self.assertRaises(sources.SourceError):
            sources.normalize_source(spec)
        Path(self.review_refs[0]['path']).write_text('{}')
        with self.assertRaises(sources.SourceError):
            sources.normalize_source(self.spec)

    def test_normalized_snapshot_cannot_change_the_manual_name(self):
        source = sources.normalize_source(self.spec)
        source['segments'][1]['text'] = 'Daniel: invented'
        source['segments'][1]['evidence_id'] = sources._evidence_id(source['transcript'], source['segments'][1])
        source['source_id'] = 'summarysrc_' + sources._hash({k:v for k,v in source.items() if k!='source_id'})[:32]
        with self.assertRaises(sources.SourceError):
            sources.validate_source(source)

    def test_short_participant_content_does_not_count_speaker_prefixes(self):
        self.original['segments'][0]['text'] = 'word ' * 24
        self.original['segments'][1]['text'] = 'word ' * 24
        self.original_ref = feed.atomic(self.root / 'short.json', self.original)
        for review in self.reviews:
            review['transcript'] = self.original_ref
        self.review_refs = [feed.atomic(self.root / ('short-review-' + str(i) + '.json'), value)
                            for i,value in enumerate(self.reviews)]
        ref = self.project()
        doc = adapter.render(ref, 'recording-test', sources)
        self.assertEqual(doc['word_count'],48)
        self.assertFalse(doc['summary_eligible'])
        spec = {**self.spec, 'completion':ref,
                'transcript':feed.atomic(self.root / 'short-rendered.json', doc)}
        with self.assertRaises(sources.SourceError):
            sources.normalize_source(spec)

    def test_sealed_sources_stay_exact_and_third_party_gets_only_one_reviewed_revision(self):
        item = dict(recording_id='recording-test', title='Private title', review_complete=True,
                    transcript=self.copy, original_transcript=self.original_ref)
        index = self.root / 'index.json'
        feed.atomic(index, dict(kind='himr_reviewed_transcript_feed', records=[item]))
        manifest = dict(state_root=str(self.root))
        fake = SimpleNamespace(r=runner, MAX_RECORDS=100, _record_key=worker._record_key)
        initial = adapter.select_sources(fake, manifest, {}, index)
        self.assertEqual(len(initial),1)
        sealed = initial['recording-test']
        entry_path = self.root / 'entries' / (worker._record_key('recording-test') + '.json')
        feed.atomic(entry_path, dict(source=sealed))
        feed.atomic(index, dict(kind='himr_reviewed_transcript_feed', records=[]))
        self.assertEqual(adapter.select_sources(fake,manifest,{},index),initial)
        old_doc = feed.atomic(self.root / 'third-party.json', dict(kind='himr_third_party_transcript_import'))
        old = dict(recording_id='recording-test', format='third_party', transcript=old_doc)
        feed.atomic(entry_path, dict(source=old))
        feed.atomic(index, dict(kind='himr_reviewed_transcript_feed', records=[item]))
        selected = adapter.select_sources(fake,manifest,{'recording-test':old},index)
        revision = adapter.revision_id('recording-test',self.original_ref)
        self.assertEqual(set(selected),{'recording-test',revision})
        self.assertEqual(selected['recording-test'],old)
        spec = {**self.spec, **{k:selected[revision][k] for k in ('recording_id','transcript','completion')}}
        self.assertEqual(sources.normalize_source(spec)['recording_id'],revision)

    def test_standard_cloud_validation_is_not_bypassed(self):
        with self.assertRaises(sources.SourceError):
            sources.normalize_source({**self.spec, 'transcript':self.original_ref})

    def test_collector_checks_extensions_and_installs_source_adapter(self):
        from contextlib import ExitStack
        from pipeline import cloud_transcription_summary_parallel as parallel
        with ExitStack() as stack, patch.object(parallel, '_initialize') as original, \
                patch.object(parallel, '_CHILD', {'stack':stack}):
            adapter.initialize_collector(feed.bound(adapter.__file__),feed.bound(feed.__file__), 'one','two')
            original.assert_called_once_with('one','two')
            self.assertEqual(sources.normalize_source(self.spec)['provenance']['source_kind'],adapter.KIND)

    def test_worker_end_to_end_uses_existing_ledger_and_does_not_repeat_paid_work(self):
        from contextlib import ExitStack
        from pipeline.tests.test_cloud_transcription_summary import WorkerTests
        case = WorkerTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.available = []
        case.prepare()
        index = self.root / 'worker-feed.json'
        item = dict(recording_id='recording-test', title='Reviewed recording',
            review_complete=True, transcript=self.copy, original_transcript=self.original_ref)
        feed.atomic(index,dict(kind='himr_reviewed_transcript_feed',records=[item]))
        with ExitStack() as stack:
            from pipeline import cloud_transcription_summary_parallel as parallel
            stack.enter_context(patch.object(worker,'parallel_collection',parallel,create=True))
            for module, attribute in ((worker,'_available'),(worker,'_identity_holds'),
                    (worker.parallel_collection,'_initialize'),(sources,'_cloud'),(sources,'validate_source')):
                stack.enter_context(patch.object(module,attribute,getattr(module,attribute)))
            adapter.install(worker,index)
            self.assertEqual(case.cycle()['new_paid_requests'],1)
            case.client.complete=True
            self.assertEqual(case.cycle()['new_paid_requests'],1)
            final=case.cycle()
            self.assertEqual(final['transcript_summaries_complete'],1)
            self.assertEqual(worker.export(case.ref)['transcript_summaries'],1)
            self.assertEqual(case.cycle()['new_paid_requests'],0)
            self.assertEqual(len(case.client.created),2)
            self.assertLessEqual(final['accounted_microusd'],5_000_000)
            self.assertTrue(list((case.worker_root/'reservations').glob('*.json')))
