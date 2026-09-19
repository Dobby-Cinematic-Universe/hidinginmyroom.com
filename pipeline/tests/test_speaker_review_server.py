import json
from pathlib import Path
import tempfile
import threading
import unittest
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from pipeline.speaker_review_server import ReviewServer, byte_range, ranked_names, review_progress
from pipeline.transcript_audio_review import binding


class ReviewTests(unittest.TestCase):
    def test_review_completion_includes_unnamed_participants(self):
        r=dict(transcript={'sha256':'a'},confirmed={},doc={'segments':[{'speaker':'A'},{'speaker':'A'},{'speaker':'B'}]})
        def decision(label,source,scope='label',index=0,ref='a'):
            return dict(transcript={'sha256':ref},decision=dict(label=label,source=source,scope=scope,segment_index=index,name=''))
        self.assertEqual(review_progress(r,[])['unresolved_turns'],3)
        saved=[decision('A','participant'),decision('B','playback')]
        self.assertTrue(review_progress(r,saved)['labeling_complete'])
        saved.append(decision('A','uncertain','segment',0))
        self.assertEqual(review_progress(r,saved)['unresolved_turns'],0)
        self.assertEqual(review_progress(r,saved)['reviewed_uncertain_turns'],1)
        saved.append(decision('A','tts'))
        self.assertEqual(review_progress(r,saved)['unresolved_turns'],0)
        saved.append(decision('A','participant','segment',0))
        self.assertTrue(review_progress(r,saved)['labeling_complete'])
        saved.append(decision('A','uncertain',ref='stale'))
        self.assertTrue(review_progress(r,saved)['labeling_complete'])

    def test_confirmations_count_but_saved_uncertainty_overrides(self):
        r=dict(transcript={},confirmed={'confirmed_mappings':[dict(label='A',name='Daniel')]},doc={'segments':[{'speaker':'A'}]})
        self.assertTrue(review_progress(r,[])['labeling_complete'])
        d=dict(transcript={},decision=dict(label='A',scope='label',source='uncertain'))
        self.assertTrue(review_progress(r,[d])['labeling_complete'])
        self.assertEqual(review_progress(r,[d])['reviewed_uncertain_turns'],1)

    def test_names_rank_current_assignments_not_revisions(self):
        records={'j':dict(transcript={'sha256':'a'},confirmed={'confirmed_mappings':[dict(label='A',name='Daniel')],
            'additional_confirmed_participants_without_label':[dict(name='Ice Poseidon')]})}
        def review(label,name,source='participant',ref='a'):
            return dict(transcript={'sha256':ref},decision=dict(job_id='j',scope='label',label=label,segment_index=0,name=name,source=source))
        reviews=[review('A','Daniel'),review('A','Daniel'),review('B','  daniel '),review('C','Mila'),review('D','Wrong',ref='old')]
        self.assertEqual(ranked_names(records,reviews),[dict(name='Daniel',uses=2),dict(name='Mila',uses=1),dict(name='Ice Poseidon',uses=0)])
        reviews.append(review('B','','uncertain'))
        self.assertEqual(ranked_names(records,reviews)[0],dict(name='Daniel',uses=1))

    def test_ranges(self):
        self.assertEqual(byte_range('bytes=2-5',10),(2,5,True))
        self.assertEqual(byte_range('bytes=-3',10),(7,9,True))
        self.assertEqual(byte_range('bytes=7-',10),(7,9,True))
        for r in ['bytes=10-','bytes=5-2','bytes=0-1,4-5','bytes=-0']:
            with self.assertRaises(ValueError):byte_range(r,10)

    def test_http_media_and_append_only_reviews(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);media=root/'payload';media.write_bytes(b'0000ftypabcdefgh')
            doc=dict(segments=[dict(speaker='A',start_ms=0,end_ms=1000,text='Hello')],
                     duration_seconds=1,source_media=dict(path=str(media),byte_count=16))
            source=root/'transcript.json';source.write_text(json.dumps(doc));ref=binding(source)
            report=root/'report.json';report.write_text(json.dumps(dict(reports=[dict(job_id='job1',title='Test',transcript=ref,candidates=[])])))
            conf=root/'confirmed.json';conf.write_text(json.dumps(dict(records=[])))
            server=ReviewServer(('127.0.0.1',0),report,conf,root/'decisions')
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            base=f'http://127.0.0.1:{server.server_port}'
            try:
                with urlopen(Request(base+'/media/job1',headers={'Range':'bytes=4-7'})) as response:
                    self.assertEqual(response.status,206);self.assertEqual(response.read(),b'ftyp')
                with self.assertRaises(HTTPError) as error:
                    urlopen(Request(base+'/api/records',headers={'Host':'evil.example'}))
                self.assertEqual(error.exception.code,403)
                data=dict(job_id='job1',scope='segment',segment_index=0,label='A',name='Daniel',source='participant',notes='Test fixture')
                with self.assertRaises(HTTPError) as error:
                    urlopen(Request(base+'/api/decision',data=json.dumps(data).encode()))
                self.assertEqual(error.exception.code,403)
                for _ in range(2):
                    with urlopen(Request(base+'/api/decision',data=json.dumps(data).encode(),headers={'X-Review-Token':server.token})) as response:
                        self.assertTrue(json.load(response)['saved'])
                self.assertEqual(len(list((root/'decisions').glob('*.json'))),2)
                self.assertEqual(binding(source),ref)
                data['segment_index']=4
                with self.assertRaises(HTTPError) as error:
                    urlopen(Request(base+'/api/decision',data=json.dumps(data).encode(),headers={'X-Review-Token':server.token}))
                self.assertEqual(error.exception.code,400)
                # New completed recordings become visible without a server restart.
                report.write_text(json.dumps(dict(reports=[
                    dict(job_id='job1',title='Test',transcript=ref,candidates=[]),
                    dict(job_id='job2',title='New recording',transcript=ref,candidates=[])])))
                with urlopen(base+'/api/records') as response:
                    result=json.load(response)
                self.assertEqual(len(result['records']),2)
                self.assertEqual(set(server.records),{'job1','job2'})
            finally:server.shutdown();server.server_close();thread.join()
