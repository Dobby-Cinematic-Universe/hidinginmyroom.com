from copy import deepcopy
import unittest
from unittest.mock import patch

from pipeline import transcript_summary as r
from pipeline import transcript_summary_single_speaker as singleton
from pipeline import cloud_transcription_summary as worker
from pipeline.tests import test_cloud_transcription_summary as fixtures


class SingleLabelTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.WorkerTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.case.available[:] = [self.case.source(speaker='SPEAKER_0000')]
        self.case.prepare()

    def spec(self):
        return worker._request(worker.load_manifest(self.case.ref), self.case.available[0])['sources'][0]

    def test_project_preserves_text_times_raw_references_and_provenance(self):
        original = r.sources_module.normalize_source(self.spec())
        projected = singleton.project(original)
        self.assertNotEqual(projected['source_id'], original['source_id'])
        self.assertEqual(projected['provenance'], original['provenance'])
        self.assertEqual(projected['transcript'], original['transcript'])
        for before, after in zip(original['segments'], projected['segments']):
            self.assertEqual([before[k] for k in ('text', 'start_ms', 'end_ms')],
                             [after[k] for k in ('text', 'start_ms', 'end_ms')])
            self.assertIsNone(after['speaker'])
            self.assertIsNone(after['source_ref']['speaker_scope'])
        self.assertEqual(original['segments'][0]['speaker'], 'SPEAKER_0000')
        self.assertEqual(singleton.project(projected), projected)

    def test_multiple_labels_are_not_collapsed(self):
        source = self.case.available[0]
        doc = r.read(source['transcript'])
        doc['segments'].append({**doc['segments'][0], 'start_ms': 10000, 'end_ms': 20000,
                                'speaker': 'SPEAKER_0001', 'text': 'A different voice.'})
        source['transcript'] = self.case.file(doc)
        normalized = r.sources_module.normalize_source(self.spec())
        self.assertEqual(singleton.project(normalized), normalized)

    def test_scope_restores_original_and_replays_projected_plan(self):
        previous = r.sources_module.normalize_source
        raw = r.read_bytes(self.case.available[0]['transcript'])
        with singleton.scope(self.case.ref):
            entry = worker._ensure_record(worker.load_manifest(self.case.ref), self.case.available[0])
            _, sources = r.load_plan(entry['plan']['path'], entry['plan']['sha256'])
            self.assertIsNone(sources[0]['segments'][0]['speaker'])
            with self.assertRaises(RuntimeError):
                with singleton.scope(self.case.ref):
                    pass
        self.assertIs(r.sources_module.normalize_source, previous)
        with singleton.scope(self.case.ref):
            _, sources = r.load_plan(entry['plan']['path'], entry['plan']['sha256'])
            self.assertIsNone(sources[0]['segments'][0]['speaker'])
        self.assertEqual(r.read_bytes(self.case.available[0]['transcript']), raw)

    def test_existing_legacy_plan_is_not_rewritten(self):
        entry = worker._ensure_record(worker.load_manifest(self.case.ref), self.case.available[0])
        before = r.read_bytes(entry['plan'])
        with singleton.scope(self.case.ref):
            _, sources = r.load_plan(entry['plan']['path'], entry['plan']['sha256'])
            self.assertEqual(sources[0]['segments'][0]['speaker'], 'SPEAKER_0000')
            self.assertEqual(singleton.statistics()['legacy_summary_plans_preserved'], 1)
        self.assertEqual(r.read_bytes(entry['plan']), before)

    @unittest.skipUnless(hasattr(worker, 'singleton'), 'requires refined worker')
    def test_single_label_admitted_and_sent_without_speaker_or_timestamps(self):
        case = self.case
        with singleton.scope(case.ref):
            result = case.cycle()
        self.assertEqual(result['speaker_identity_pending'], 0)
        self.assertEqual(result['new_paid_requests'], 1)
        import json
        _, requests, _ = case.client.created[0]
        payload = json.loads(requests[0]['request']['contents'][0]['parts'][0]['text'])
        self.assertEqual(set(payload), {'stage', 'evidence'})
        for row in payload['evidence']:
            self.assertNotIn('speaker', row)
            self.assertNotIn('start_ms', row)
            self.assertNotIn('end_ms', row)

    @unittest.skipUnless(hasattr(worker, 'singleton'), 'requires refined worker')
    def test_two_labels_still_hold_the_summary(self):
        source = self.case.available[0]
        doc = r.read(source['transcript'])
        doc['segments'].append({**doc['segments'][0], 'start_ms': 10000, 'end_ms': 20000,
                                'speaker': 'SPEAKER_0001', 'text': 'Another speaker.'})
        source['transcript'] = self.case.file(doc)
        with singleton.scope(self.case.ref):
            result = self.case.cycle()
        self.assertEqual(result['speaker_identity_pending'], 1)
        self.assertEqual(self.case.client.created, [])

    @unittest.skipUnless(hasattr(worker, 'singleton'), 'requires refined worker')
    def test_projected_source_completes_and_restarts_without_rebuying(self):
        case = self.case
        with singleton.scope(case.ref), worker.job_cache.scope(case.ref):
            self.assertEqual(case.cycle()['new_paid_requests'], 1)
            case.client.complete = True
            self.assertEqual(case.cycle()['new_paid_requests'], 1)
            self.assertEqual(case.cycle()['transcript_summaries_complete'], 1)
        with singleton.scope(case.ref), worker.job_cache.scope(case.ref):
            result = case.cycle()
            self.assertEqual(result['new_paid_requests'], 0)
            self.assertEqual(result['speaker_identity_pending'], 0)
            self.assertEqual(worker.export(case.ref)['transcript_summaries'], 1)
        self.assertEqual(len(case.client.created), 2)


if __name__ == '__main__':
    unittest.main()
