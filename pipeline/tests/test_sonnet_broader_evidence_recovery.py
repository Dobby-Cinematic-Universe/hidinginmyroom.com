from copy import deepcopy
import json
from unittest import TestCase
from pipeline import sonnet_broader_evidence_recovery as recovery


def fixture(refs):
    job = dict(job_id='test', evidence=[dict(item_id='source', classification='reported_statement', transcript_ids=['t1'])],
               transcript_ids=['t1'], stage='archive', period='selected-archive', final=True)
    payload = dict(summary=[dict(text='Original prose.', classification='reported_statement', evidence_ids=refs)],
                   topics=[], events=[], uncertainties=[])
    response = dict(model=recovery.c.MODEL, type='message', role='assistant', stop_reason='end_turn', stop_sequence=None,
                    content=[dict(type='text', text=json.dumps(payload))])
    return job, response


class EvidenceRecoveryTests(TestCase):
    def test_preserves_source_and_evidence_set(self):
        job, response = fixture(['e1','e1'])
        before = deepcopy(response)
        result, corrected, changes = recovery.repair(job, response)
        self.assertEqual(response, before)
        self.assertEqual(result['sections']['summary'][0]['text'], 'Original prose.')
        self.assertEqual(result['sections']['summary'][0]['evidence_ids'], ['source'])
        self.assertEqual(changes[0]['before'], ['e1','e1'])


    def test_unknown_evidence_still_rejected(self):
        job, response = fixture(['e2','e2'])
        with self.assertRaises(recovery.r.Error): recovery.repair(job, response)


    def test_does_not_repair_unrelated_failures(self):
        job, response = fixture(['e1'])
        with self.assertRaisesRegex(recovery.r.Error, 'no duplicate'): recovery.repair(job, response)
