import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock
from pipeline.cloud_media_isolation_runner import isolate, classify, RecordHeld, MediaCircuitOpen


class MediaError(RuntimeError):pass


class IsolationTests(unittest.TestCase):
    def test_fatal_errors_not_classified(self):
        for e in [OSError('Input/output error'),MediaError('FFmpeg implementation changed'),
                  MediaError('prepared upload audio differs from its receipt'),
                  MediaError('insufficient private workspace space for one whole-recording upload')]:
            self.assertFalse(classify(e,MediaError))

    def fixture(self,root):
        rows=[dict(job_id=f'j{i}',recording={'id':i}) for i in range(4)]
        plan={'state_root':str(root),'recordings':rows,'ffmpeg':{}}
        def put(p,d):Path(p).write_text(json.dumps(d))
        io=SimpleNamespace(mkdir=lambda p:Path(p).mkdir(exist_ok=True),put=put,
            safe=SimpleNamespace(exists=lambda p:Path(p).exists()),binding=lambda p:p,
            read=lambda p:json.loads(Path(p).read_text()))
        prepare=Mock(side_effect=MediaError('prepared WAV is truncated'))
        inspect=Mock(return_value={'state':'ready','reservation':0})
        cloud=SimpleNamespace(io=io,CloudError=RuntimeError,media=SimpleNamespace(prepare=prepare,MediaError=MediaError),
            inspect_job=inspect,cycle=Mock(),load_plan=lambda ref:plan,
            _folder=lambda p,r:root/r['job_id'],status=lambda ref:{'counts':{}})
        return cloud,rows,plan,prepare,inspect

    def test_hold_persists_skips_on_restart_and_checks_original_inspection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);cloud,rows,plan,prepare,inspect=self.fixture(root)
            for _ in range(2):
                with isolate(cloud,{},root/'holds',{},lambda e:None):
                    if not (root/'holds/j0.json').exists():
                        with self.assertRaises(RecordHeld):cloud.media.prepare(rows[0]['recording'],root/'j0',{})
                    self.assertEqual(cloud.inspect_job(plan,{},rows[0])['state'],'needs_review')
                    self.assertEqual(cloud.inspect_job(plan,{},rows[1])['state'],'ready')
            self.assertEqual(prepare.call_count,1)
            self.assertEqual(inspect.call_count,4)
            self.assertIs(cloud.media.prepare,prepare)

    def test_paid_evidence_cannot_be_hidden(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);cloud,rows,plan,_,_=self.fixture(root)
            (root/'j0').mkdir();(root/'j0/intent.json').write_text('{}')
            with isolate(cloud,{},root/'holds',{},lambda e:None):
                with self.assertRaisesRegex(RuntimeError,'paid evidence'):
                    cloud.media.prepare(rows[0]['recording'],root/'j0',{})
            self.assertFalse((root/'holds/j0.json').exists())

    def test_three_failure_circuit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);cloud,rows,plan,_,_=self.fixture(root)
            with isolate(cloud,{},root/'holds',{},lambda e:None):
                for i in range(3):
                    with self.assertRaises(MediaCircuitOpen if i==2 else RecordHeld):
                        cloud.media.prepare(rows[i]['recording'],root/f'j{i}',{})

    def test_cycle_continues_only_for_record_hold(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);cloud,_,_,_,_=self.fixture(root)
            original=cloud.cycle;original.side_effect=RecordHeld('j0')
            with isolate(cloud,{},root/'holds',{},lambda e:None):
                self.assertEqual(cloud.cycle({})['state'],'running')
                original.side_effect=OSError('I/O failure')
                with self.assertRaises(OSError):cloud.cycle({})

    def test_success_resets_failure_streak(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);cloud,rows,_,prepare,_=self.fixture(root)
            error=MediaError('prepared WAV is truncated')
            prepare.side_effect=[error,{'audio':'ok'},error,error]
            with isolate(cloud,{},root/'holds',{},lambda e:None):
                for i in range(4):
                    if i==1:self.assertEqual(cloud.media.prepare(rows[i]['recording'],root/f'j{i}',{}),{'audio':'ok'})
                    else:
                        with self.assertRaises(RecordHeld):cloud.media.prepare(rows[i]['recording'],root/f'j{i}',{})

    def test_original_inspection_error_remains_fatal_with_hold(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);cloud,rows,plan,_,inspect=self.fixture(root)
            with isolate(cloud,{},root/'holds',{},lambda e:None):
                with self.assertRaises(RecordHeld):cloud.media.prepare(rows[0]['recording'],root/'j0',{})
                inspect.side_effect=RuntimeError('damaged evidence')
                with self.assertRaisesRegex(RuntimeError,'damaged evidence'):cloud.inspect_job(plan,{},rows[0])
