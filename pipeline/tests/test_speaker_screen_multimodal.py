"""Model-free policy/recovery tests plus bounded native FFmpeg timing checks."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from pipeline import speaker_screen_multimodal as runner
from pipeline import speaker_screen_multimodal_core as core
from pipeline import speaker_screen_multimodal_engine as engine


def voice(index, axis, *, probe=None, start=None):
    vector = [0.] * 192; vector[axis] = 1.
    begin = index * 10000 if start is None else start
    return {"id": f"e-{index}", "probe_id": f"p-{index}" if probe is None else probe,
            "start_ms": begin, "end_ms": begin + 2500, "speech_ms": 2500, "embedding": vector}


def frame(t, count=2, digest=None):
    return {"state": "decoded", "target_ms": t, "actual_ms": t,
            "frame_sha256": hashlib.sha256(str(t).encode()).hexdigest() if digest is None else digest,
            "face_count": count, "width": 640, "height": 360,
            "faces": [{"box": [i * 70., 20., 50., 50.], "score": .95, "clipped": False} for i in range(count)]}


class PolicyTests(unittest.TestCase):
    def test_frames_cover_whole_long_recording(self):
        times = core.frame_times(2000, 12 * 3600000)
        self.assertEqual(len(times), 32)
        self.assertGreater(times[-1], 11 * 3600000)
        self.assertGreater(times[0], 2000)
        self.assertLess(times[-1], 12 * 3600000)

    def test_short_and_offset_frame_plans(self):
        self.assertEqual(core.frame_times(9000, 9001), [9000])
        self.assertEqual(len(core.frame_times(9000, 31000)), 3)
        for bounds in ((True, 100), (-1, 100), (4, 4), (0, float('inf'))):
            with self.subTest(bounds=bounds), self.assertRaises(core.TriageError):
                core.frame_times(*bounds)

    def test_audio_baseline_survives_visual_cues(self):
        base = core.audio_windows(2000, 200000)
        targeted = core.audio_windows(2000, 200000, [50000, 150000])
        self.assertEqual(targeted[:len(base)], base)
        self.assertLessEqual(len(targeted), 12)
        for i, row in enumerate(targeted):
            self.assertGreaterEqual(row['start_ms'], 2000)
            self.assertLessEqual(row['end_ms'], 200000)
            for other in targeted[i+1:]:
                self.assertFalse(row['start_ms'] < other['end_ms'] and other['start_ms'] < row['end_ms'])

    def test_short_audio_and_out_of_range_cues(self):
        self.assertEqual(core.audio_windows(100, 900, [0, 1000]), [
            {'index': 0, 'start_ms': 100, 'end_ms': 900, 'reason': 'uniform_audio_baseline'}])

    def test_vad_tolerates_short_real_gaps(self):
        probs = [.9] * 94
        probs[40] = .1
        spans = core.speech_excerpts(probs, 48000)
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0]['end_sample'], 48000)
        self.assertEqual(spans[0]['speech_samples'], 48000-512)

    def test_vad_keeps_multiple_utterances_and_original_silence(self):
        probs = [.9] * 80 + [.1] * 12 + [.9] * 80
        spans = core.speech_excerpts(probs, len(probs) * 512)
        self.assertEqual(len(spans), 2)
        self.assertLess(spans[0]['end_sample'], spans[1]['start_sample'])
        self.assertGreaterEqual(spans[1]['start_sample'], 92*512)

    def test_vad_allows_natural_pause_but_not_low_speech_density(self):
        probabilities=[.9]*94
        probabilities[35:41]=[.1]*6
        spans=core.speech_excerpts(probabilities,48000)
        self.assertEqual(len(spans),1)
        self.assertEqual(spans[0]['end_sample']-spans[0]['start_sample'],48000)
        self.assertEqual(spans[0]['speech_samples'],48000-6*512)
        self.assertEqual(core.speech_excerpts(([.9]+[.1]*6)*13,91*512),[])

    def test_vad_cap_spreads_excerpts(self):
        spans = core.speech_excerpts([.9] * 313, 160000)
        self.assertEqual(len(spans), 3)
        self.assertEqual(spans[0]['start_sample'], 0)
        self.assertEqual(spans[-1]['end_sample'], 160000)
        for row in spans:
            self.assertGreaterEqual(row['end_sample']-row['start_sample'], 32000)
            self.assertLessEqual(row['end_sample']-row['start_sample'], 48000)

    def test_vad_short_silence_and_invalid_inputs(self):
        self.assertEqual(core.speech_excerpts([0.] * 100, 51200), [])
        self.assertEqual(core.speech_excerpts([.9] * 30, 15360), [])
        for p, n in (([math.nan], 512), ([True],512), ([.9],513), ([.9],0)):
            with self.subTest(p=p,n=n), self.assertRaises(core.TriageError):
                core.speech_excerpts(p,n)

    def test_two_supported_acoustic_cores(self):
        summary = core.audio_diversity([voice(0,0),voice(1,0),voice(2,1),voice(3,1)])
        self.assertEqual(summary['state'], 'supported_audio_diversity')
        self.assertEqual(summary['support']['maximum_cross_cosine'], 0.)
        self.assertIsNone(summary['speaker_count'])

    def test_outlier_cannot_make_second_speaker(self):
        self.assertIsNone(core.audio_diversity([voice(0,0),voice(1,0),voice(2,0),voice(3,1)])['support'])

    def test_same_probe_cannot_supply_repeated_support(self):
        rows = [voice(0,0,probe='a'),voice(1,0,probe='a'),voice(2,1),voice(3,1)]
        self.assertIsNone(core.audio_diversity(rows)['support'])

    def test_reprocessing_same_audio_cannot_supply_repeated_support(self):
        rows = [voice(0,0,start=0),voice(1,0,start=1000),voice(2,1),voice(3,1)]
        self.assertIsNone(core.audio_diversity(rows)['support'])

    def test_opposing_cores_cannot_be_same_time(self):
        rows = [voice(0,0,start=0),voice(1,0,start=5000),voice(2,1,start=0),voice(3,1,start=5000)]
        self.assertIsNone(core.audio_diversity(rows)['support'])

    def test_no_forced_two_cluster_answer(self):
        self.assertEqual(core.audio_diversity([voice(i,0) for i in range(5)])['state'], 'no_supported_diversity_in_samples')
        self.assertEqual(core.audio_diversity([])['state'], 'insufficient_audio')

    def test_bad_vectors_and_duplicate_ids_rejected(self):
        for row in (dict(voice(0,0),embedding=[0.]*192), dict(voice(0,0),embedding=[math.nan]*192),
                    dict(voice(0,0),embedding=[1.]),dict(voice(0,0),speech_ms=5000)):
            with self.subTest(row=row['speech_ms']), self.assertRaises(core.TriageError):
                core.audio_diversity([row])
        with self.assertRaises(core.TriageError):
            core.audio_diversity([voice(0,0),voice(0,0)])

    def test_faces_never_establish_speakers(self):
        visual = core.visual_summary([frame(1000),frame(2000)],2)
        self.assertTrue(visual['repeated_visual_cue'])
        route = core.route(core.audio_diversity([]), visual)
        self.assertEqual(route['label'],'visual_cue_needs_audio_review')
        self.assertFalse(route['semantics']['faces_are_speakers'])
        self.assertFalse(route['automatic_diarization_authorized'])

    def test_static_poster_does_not_get_repeated_corroboration(self):
        visual = core.visual_summary([frame(1000,digest='a'*64),frame(2000,digest='a'*64)],2)
        self.assertFalse(visual['repeated_visual_cue'])
        self.assertEqual(visual['multiple_face_samples'],2)

    def test_offscreen_audio_can_be_positive(self):
        audio=core.audio_diversity([voice(0,0),voice(1,0),voice(2,1),voice(3,1)])
        self.assertEqual(core.route(audio,core.visual_summary([],0,video_state='no_video'))['label'],'audio_diversity_candidate')

    def test_unavailable_visuals_are_not_negative(self):
        visual=core.visual_summary([{'state':'needs_review','target_ms':2000}],1)
        self.assertEqual(visual['state'],'unusable_visual_samples')
        self.assertFalse(visual['repeated_visual_cue'])

    def test_visual_invalid_counts_hashes_times_and_boxes_fail_closed(self):
        bad = [dict(frame(1000),face_count=3),dict(frame(1000),frame_sha256='wrong'),
               dict(frame(1000),actual_ms=0),dict(frame(1000),width=0)]
        f=frame(1000); f['faces'][0]['box'][0]=630.; bad.append(f)
        for row in bad:
            with self.subTest(row=row.get('frame_sha256')), self.assertRaises(core.TriageError):
                core.visual_summary([row],1)
        with self.assertRaises(core.TriageError):
            core.visual_summary([frame(1000),frame(1000)],2)


class DecoderTests(unittest.TestCase):
    def test_frame_pts_preserved(self):
        stderr='[showinfo@triage @ 0x123] config in time_base: 1/1000, frame_rate: 25/1\n[showinfo@triage @ 0x123] n: 0 pts: 2120 pts_time:2.12\n'
        self.assertEqual(engine.frame_timestamp(stderr,2100),2120)
        with self.assertRaises(engine.DecodeReview): engine.frame_timestamp(stderr,2200)
        with self.assertRaises(engine.DecodeReview): engine.frame_timestamp('',2000)

    def test_short_audio_preserved_not_padded(self):
        stderr='[ashowinfo@triage @ 0x123] n:0 pts:32000 pts_time:2 fmt:s16 channels:1 chlayout:mono rate:16000 nb_samples:16000\n'
        pcm,receipt=engine.audio_receipt(b'\0'*32000,stderr,2000,4000)
        self.assertEqual(len(pcm),32000); self.assertEqual(receipt['end_ms'],3000)
        self.assertTrue(receipt['short_sample']); self.assertFalse(receipt['silence_padding'])

    def test_audio_gap_byte_mismatch_and_empty_rejected(self):
        good='[ashowinfo@triage @ 0x123] n:0 pts:32000 pts_time:2 fmt:s16 channels:1 chlayout:mono rate:16000 nb_samples:16000\n'
        gap=good+'[ashowinfo@triage @ 0x123] n:1 pts:50000 pts_time:3.125 fmt:s16 channels:1 chlayout:mono rate:16000 nb_samples:16000\n'
        for body,stderr in ((b'\0'*64000,gap),(b'\0'*100,good),(b'',good)):
            with self.assertRaises(engine.DecodeReview): engine.audio_receipt(body,stderr,2000,4000)

    def test_long_contiguous_part_recovered_without_retiming_or_joining(self):
        def line(i,pts,n):
            return f'[ashowinfo@triage @ 0x123] n:{i} pts:{pts} pts_time:0 fmt:s16 channels:1 chlayout:mono rate:16000 nb_samples:{n}\n'
        # Real discontinuity; keep the four-second later interval, not a joined
        # waveform falsely labeled as continuous. Bytes prove which part survived.
        stderr=line(0,16000,16000)+line(1,40000,32000)+line(2,72000,32000)
        body=b'\1\0'*16000+b'\2\0'*64000
        pcm,r=engine.audio_receipt(body,stderr,1000,7000)
        self.assertEqual(pcm,b'\2\0'*64000)
        self.assertEqual((r['start_ms'],r['end_ms']),(2500,6500))
        self.assertEqual(r['discarded_samples'],16000)
        self.assertEqual(r['decoded_samples'],80000)
        self.assertEqual(r['timestamp_discontinuities'],1)
        self.assertTrue(r['short_sample']); self.assertFalse(r['waveform_retimed'])
        self.assertFalse(r['silence_padding'])

    def test_audio_receipt_accounting_and_excerpt_escape_rejected(self):
        w={'index':0,'start_ms':0,'end_ms':5000,'reason':'uniform_audio_baseline'}
        v={'window':w,'state':'analyzed','excerpts':[],'legacy_eligible_excerpts':0,'vad_positive_ms':0,
           'receipt':{'requested_start_ms':0,'requested_end_ms':5000,'start_ms':1000,'end_ms':4000,
                      'source_pts_verified':True,'silence_padding':False,'short_sample':True,'pcm_sha256':'a'*64,
                      'decoded_samples':80000,'discarded_samples':32000,'timestamp_discontinuities':1,'waveform_retimed':False}}
        runner.validate_audio_checkpoint(v,w)
        bad=deepcopy(v); bad['receipt']['discarded_samples']=1
        with self.assertRaises(core.TriageError): runner.validate_audio_checkpoint(bad,w)
        bad=deepcopy(v); bad['excerpts']=[dict(voice(0,0,start=0),id='fresh-0-0',probe_id='fresh-0')]
        with self.assertRaises(core.TriageError): runner.validate_audio_checkpoint(bad,w)

    def test_command_is_bounded_and_never_uses_async_padding(self):
        with mock.patch.object(engine,'bounded_process',return_value=(b'', '')) as execute:
            with self.assertRaises(engine.DecodeReview): engine.decode_audio(7,8,1,{'start_ms':5000,'end_ms':15000})
        argv=execute.call_args.args[0]
        self.assertIn('-copyts',argv); self.assertNotIn('-seek_timestamp',argv)
        self.assertIn('file,pipe',argv); self.assertNotIn('-y',argv)
        self.assertIn('async=0',' '.join(argv)); self.assertNotIn('apad',' '.join(argv))

    def test_voice_backend_reused_and_multiple_excerpts(self):
        model=engine.AudioEngine({'kind':'himr_speaker_screen_models','schema_version':1,
            'silero_vad':{'path':'/local/vad.onnx','sha256':'a'*64},
            'ecapa_embedding':{'path':'/local/voice.ckpt','sha256':'b'*64}})
        backend=mock.Mock()
        backend.probabilities.return_value=[.9]*313
        backend.encode.return_value=[1.]+[0.]*191
        with mock.patch.object(model.engine,'_load',return_value=backend):
            result=model.analyze(b'\1\0'*160000,{'start_ms':1000},'probe1')
        self.assertEqual(len(result['excerpts']),3); self.assertEqual(backend.encode.call_count,3)
        self.assertEqual(result['excerpts'][-1]['end_ms'],11000)
        self.assertEqual(result['legacy_eligible_excerpts'],1)


@unittest.skipUnless(Path('/usr/bin/ffmpeg').exists() and Path('/usr/bin/ffprobe').exists(),'native FFmpeg unavailable')
class NativeDecodeTests(unittest.TestCase):
    def test_audio_only_uses_audio_without_visual_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'audio.wav'
            subprocess.run(['/usr/bin/ffmpeg','-v','error','-nostdin','-threads','1',
                '-f','lavfi','-i','anullsrc=r=16000:cl=mono','-t','2','-c:a','pcm_s16le',str(path)],check=True,timeout=15)
            with runner.safe.opened(path) as src,runner.safe.opened('/usr/bin/ffmpeg') as ff,runner.safe.opened('/usr/bin/ffprobe') as fp:
                media=engine.probe(src,fp,2000)
                self.assertEqual(media['video']['state'],'absent')
                self.assertEqual(media['audio']['state'],'available')
                pcm,receipt=engine.decode_audio(src,ff,media['audio']['stream_index'],{'start_ms':100,'end_ms':1900})
                self.assertEqual(len(pcm),1800*32)
                self.assertEqual(receipt['timestamp_discontinuities'],0)
                self.assertFalse(receipt['waveform_retimed'])

    def test_coarse_video_ticks_do_not_round_seek_backwards(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'coarse.mp4'
            subprocess.run(['/usr/bin/ffmpeg','-v','error','-nostdin','-threads','1','-filter_threads','1',
                '-f','lavfi','-i','testsrc2=size=128x96:rate=30','-t','2','-c:v','mpeg4','-threads:v','1',
                '-video_track_timescale','30',str(path)],check=True,timeout=15)
            with runner.safe.opened(path) as src,runner.safe.opened('/usr/bin/ffmpeg') as ff:
                body,actual=engine.decode_frame(src,ff,0,1009)
            self.assertEqual(len(body),640*360*3)
            self.assertEqual(actual,1033)

    def test_real_nonzero_pts_and_short_eof(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'offset.mkv'
            subprocess.run(['/usr/bin/ffmpeg','-v','error','-nostdin','-threads','1','-filter_threads','1',
                '-f','lavfi','-i','testsrc2=size=128x96:rate=25','-f','lavfi','-i','anullsrc=r=16000:cl=mono',
                '-t','3','-c:v','mpeg4','-threads:v','1','-c:a','pcm_s16le','-output_ts_offset','2',str(path)],check=True,timeout=15)
            with runner.safe.opened(path) as source, runner.safe.opened('/usr/bin/ffmpeg') as ffmpeg, runner.safe.opened('/usr/bin/ffprobe') as ffprobe:
                metadata=engine.probe(source,ffprobe,3000)
                self.assertEqual(metadata['video']['span']['start_ms'],2000)
                self.assertEqual(metadata['source_start_ms'],2000)
                body,actual=engine.decode_frame(source,ffmpeg,metadata['video']['stream_index'],2150,source_start_ms=2000)
                self.assertEqual(len(body),640*360*3); self.assertGreaterEqual(actual,2150)
                pcm,receipt=engine.decode_audio(source,ffmpeg,metadata['audio']['stream_index'],{'start_ms':4500,'end_ms':5500},source_start_ms=2000)
                self.assertTrue(receipt['short_sample']); self.assertEqual(receipt['start_ms'],4500)
                self.assertEqual(receipt['end_ms'],5000); self.assertEqual(len(pcm),16000)


@unittest.skipUnless(Path('/usr/bin/ffmpeg').exists() and Path('/usr/bin/ffprobe').exists(),'native tools unavailable')
class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.base=Path(self.tmp.name)
        for name in ('assets','models','audio_env','old','raw'):
            (self.base/name).mkdir(mode=0o700)
        (self.base/'audio_env/bin').mkdir(mode=0o700)
        self.python=self.base/'audio_env/bin/python'; self.python.symlink_to(Path(sys.executable).resolve())
        (self.base/'audio_env/pyvenv.cfg').write_text('home = /usr/bin\n')
        self.source=self.base/'raw/media'; self.source.write_bytes(b'fake media')
        self.record={'path':str(self.source),'byte_count':10,'sha256':hashlib.sha256(b'fake media').hexdigest(),'duration_hint_ms':40000}
        self.record['media_id']='media_sha256_'+self.record['sha256']
        acq=self.save('raw/acquisition.json',{'status':'completed','errors':[], 'admission':{k:self.record[k] for k in ('path','byte_count','sha256','media_id')}})
        self.inventory=self.save('inventory.json',{'kind':'himr_private_speaker_screen_archive_inventory','schema_version':1,
            'records':[{'recording':self.record,'aliases':[{'title':'a title is not evidence'}],'acquisition_result':acq}]})
        self.assets=self.save('assets/manifest.json',{'runtime':{'python':{'path':str(Path(sys.executable).resolve())}}})
        self.models=self.save('models/models.json',{'kind':'himr_speaker_screen_models','schema_version':1,
            'silero_vad':{'path':str(self.base/'models/vad.onnx'),'sha256':'a'*64},
            'ecapa_embedding':{'path':str(self.base/'models/voice.ckpt'),'sha256':'b'*64}})
        self.root=self.base/'new'

    def save(self,name,value):
        path=self.base/name; path.write_text(json.dumps(value)); return runner.bind(path)

    def prepare(self, **kw):
        result=runner.prepare(self.inventory['path'],self.inventory['sha256'],self.base/'old',self.assets['path'],
            self.models['path'],self.python,self.root,**kw)
        return result,runner.read(result['manifest'])

    def test_prepare_is_model_and_network_free(self):
        with mock.patch.object(engine,'face_runtime',side_effect=AssertionError('model import')),mock.patch.object(engine,'probe',side_effect=AssertionError('decode')):
            result,manifest=self.prepare()
        self.assertEqual(result['media_decodes'],0); self.assertEqual(result['paid_requests'],0)
        self.assertEqual(len(manifest['jobs']),1)
        self.assertEqual(self.source.read_bytes(),b'fake media')

    def test_selection_duplicates_and_workspace_overlap_rejected(self):
        with self.assertRaises(core.TriageError): self.prepare(media_ids=[self.record['media_id']]*2)
        with self.assertRaises(core.TriageError): self.prepare(media_ids=['unknown'])
        self.root=self.base/'old/child'
        with self.assertRaises(core.TriageError): self.prepare()

    def test_acquisition_mismatch_rejected(self):
        v=runner.read(self.inventory); v['records'][0]['recording']['byte_count']=9
        self.inventory=self.save('bad-inventory.json',v)
        with self.assertRaises(core.TriageError): self.prepare()

    def test_implementation_drift_blocks_resume(self):
        result,manifest=self.prepare()
        with mock.patch.object(runner,'implementation',return_value={}):
            with self.assertRaises(core.TriageError): runner.load_manifest(result['manifest']['path'],result['manifest']['sha256'])

    def test_changed_source_blocks_run_before_workers(self):
        result,manifest=self.prepare(); self.source.write_bytes(b'changedxxx')
        with mock.patch.object(runner.subprocess,'Popen',side_effect=AssertionError('must not launch')):
            with self.assertRaises(core.TriageError): runner.run(result['manifest']['path'],result['manifest']['sha256'])

    def test_partial_visual_deadline_resumes_without_redecoding_committed_frame(self):
        _,manifest=self.prepare(); row=manifest['jobs'][0]
        folder=self.root/'jobs'/row['job_id']; runner.mkdir(folder)
        metadata={name:{'state':'available','stream_index':i,'span':{'start_ms':0,'end_ms':40000}}
                  for i,name in enumerate(('video','audio'))}
        clock=[0.]; calls=[]
        def decode(_src,_ff,_stream,target,**kw):
            calls.append(target); clock[0]=25.
            return b'pixels',target
        def detect(_body,_runtime):
            f=frame(0,0)
            return {k:f[k] for k in ('frame_sha256','faces','face_count','width','height')}
        with mock.patch.object(runner.time,'monotonic',side_effect=lambda:clock[0]), \
             mock.patch.object(engine,'probe',return_value=metadata) as probe, \
             mock.patch.object(engine,'decode_frame',side_effect=decode), \
             mock.patch.object(engine,'detect_faces',side_effect=detect):
            self.assertFalse(runner.visual_job(manifest,row,lambda:None,20.,1,2,3))
            self.assertEqual(len(calls),1)
            self.assertFalse((folder/'visual.json').exists())
            self.assertEqual(len(list(folder.glob('frame-*.json'))),1)
            self.assertTrue(runner.visual_job(manifest,row,lambda:None,100.,1,2,3))
            self.assertEqual(len(calls),3); self.assertEqual(len(set(calls)),3)
            self.assertEqual(probe.call_count,1)

    def test_one_bad_frame_continues_and_resume_skips_committed_samples(self):
        result,manifest=self.prepare(audio_refresh='all'); row=manifest['jobs'][0]
        folder=self.root/'jobs'/row['job_id']; runner.mkdir(folder)
        metadata={name:{'state':'available','stream_index':i,'span':{'start_ms':0,'end_ms':40000}} for i,name in enumerate(('video','audio'))}
        calls=[]
        def decode(_source,_ffmpeg,_stream,target,**kwargs):
            calls.append(target)
            if len(calls)==1: raise engine.DecodeReview('bad_frame')
            return b'pixels',target
        def detector(body,runtime):
            f=frame(0,0); return {k:f[k] for k in ('frame_sha256','faces','face_count','width','height')}
        with mock.patch.object(engine,'probe',return_value=metadata),mock.patch.object(engine,'decode_frame',side_effect=decode),mock.patch.object(engine,'detect_faces',side_effect=detector):
            self.assertTrue(runner.visual_job(manifest,row,lambda:None,time.monotonic()+30,1,2,3))
            self.assertTrue(runner.visual_job(manifest,row,lambda:None,time.monotonic()+30,1,2,3))
        self.assertEqual(len(calls),3)
        visual=runner.outcome(folder/'visual.json',manifest,row,'visual')
        self.assertEqual(visual['summary']['frames_needing_review'],1)
        model=mock.Mock(); model.analyze.return_value={'excerpts':[],'legacy_eligible_excerpts':0,'vad_positive_ms':0}
        def audio(_source,_ffmpeg,_stream,window,**kwargs):
            return b'\0'*32000,{'start_ms':window['start_ms'],'end_ms':window['start_ms']+1000,
                'requested_start_ms':window['start_ms'],'requested_end_ms':window['end_ms'],
                'source_pts_verified':True,'silence_padding':False,'short_sample':True,'pcm_sha256':'a'*64,
                'decoded_samples':16000,'discarded_samples':0,'timestamp_discontinuities':0,'waveform_retimed':False}
        with mock.patch.object(engine,'decode_audio',side_effect=audio):
            self.assertTrue(runner.audio_job(manifest,row,lambda:model,time.monotonic()+30,1,2))
        summary=runner.status(manifest)
        self.assertEqual(summary['counts']['complete'],1)
        self.assertEqual(summary['state'],'completed_with_sample_reviews')
        with mock.patch.object(runner.subprocess,'Popen',side_effect=AssertionError('completed replay must not launch')):
            replay=runner.run(result['manifest']['path'],result['manifest']['sha256'])
        self.assertEqual(replay,summary)
        frame_path=next(folder.glob('frame-*.json')); raw=json.loads(frame_path.read_bytes()); raw['state']='changed'
        frame_path.write_text(json.dumps(raw))
        with self.assertRaises(RuntimeError): runner.status(manifest)

    def test_nonzero_offset_hint_is_explicit_not_verified_eof(self):
        span=engine._span({'start_time':'2.123'},40000)
        self.assertEqual(span['start_ms'],2123); self.assertEqual(span['end_ms'],42123)
        self.assertFalse(span['exact_eof_verified'])
        self.assertEqual(span['basis'],'inventory_duration_hint')


if __name__ == '__main__':
    unittest.main()
