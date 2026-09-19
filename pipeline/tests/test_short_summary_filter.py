import json
from pathlib import Path
import tempfile
import unittest
from pipeline.short_summary_filter import Filter,summary_eligible,transcript_words
from pipeline.transcript_audio_review import binding


class ShortSummaryTests(unittest.TestCase):
    def test_boundary_and_metadata(self):
        d={'title':'title '*100,'segments':[{'text':'word '*49,'start_ms':999}]}
        self.assertEqual(transcript_words(d),49);self.assertFalse(summary_eligible(d))
        d['segments'].append({'text':'last'});self.assertTrue(summary_eligible(d))

    def test_retroactive_and_new_exports_without_original_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);records=root/'records';folder=records/'one';(folder/'exports').mkdir(parents=True)
            source=root/'transcript.json';source.write_text(json.dumps({'segments':[{'text':'Hi.'}]}));ref=binding(source)
            (folder/'plan.json').write_text(json.dumps(dict(plan_id='plan',request_value={'sources':[dict(recording_id='r',transcript=ref)]})))
            worker=Filter(records,root/'filtered');first=worker.scan()
            self.assertEqual(first['short_sources_withheld'],1);self.assertEqual(first['short_exported_records_withheld'],0)
            ex=folder/'exports/summaries-test.json';ex.write_text(json.dumps(dict(phase='transcripts',plan_id='plan',phase_complete=True,results=[])));original=binding(ex)
            second=worker.scan();self.assertEqual(second['short_exported_records_withheld'],1)
            index=json.loads((root/'filtered/index.json').read_text());self.assertEqual(index['eligible_summaries'],[])
            self.assertEqual(index['withheld_sources'][0]['transcript'],ref)
            self.assertEqual(binding(source),ref);self.assertEqual(binding(ex),original)
