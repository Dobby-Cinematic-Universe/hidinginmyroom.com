"""Isolated acquisition, selective cloud ASR and Gemini summary for one new item."""
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import time
import urllib.request
from urllib.parse import quote
from pipeline import targeted_retranscription as t
from pipeline import cloud_transcription_screen as screen
from pipeline import cloud_transcription_selective_diarization as selective
from pipeline import transcript_summary_campaign as campaign
r=t.io
ROOT=Path('research/private-transcriptions/cloud-archive-20260913/new-archive-20260918-Sr9Vv5pVG1g').resolve()
BASE=Path('research/private-transcriptions/cloud-archive-20260913').resolve()
NAME='20260917-Hiding in my room is live!-Sr9Vv5pVG1g.mp4'
MAX_MEDIA_BYTES=2_000_000_000


def media_binding(path, expected_size):
    """Hash media with its own bound, not the summary JSON artifact limit."""
    path=r.safe.path_value(path)
    if not 0 < expected_size <= MAX_MEDIA_BYTES:
        raise ValueError('Unexpected media size')
    with r.safe.opened(path) as fd:
        if os.fstat(fd).st_size != expected_size:
            raise ValueError('Download size differs')
        digest=r.safe.hash_fd(fd, expected_size, time.monotonic()+300)
    return dict(path=str(path),sha256=digest,byte_count=expected_size)


def status(state,**extra):
    t.feed.atomic(ROOT/'status.json',dict(state=state,**extra))
    print(json.dumps(dict(state=state,**extra)),flush=True)


def main():
    os.umask(0o077);r.mkdir(ROOT)
    with r.locked(ROOT):
        if not (ROOT/'plan.json').exists():
            status('acquiring')
            if not (ROOT/'archive-metadata.json').exists():
                with urllib.request.urlopen('https://archive.org/metadata/699994',timeout=60) as response:metadata=json.load(response)
                r.put(ROOT/'archive-metadata.json',metadata)
            metadata=t.read(ROOT/'archive-metadata.json')
            entry=next(x for x in metadata['files'] if x['name']==NAME)
            if not 0 < int(entry['size']) <= MAX_MEDIA_BYTES:raise ValueError('Unexpected media size')
            url='https://archive.org/download/699994/'+quote(NAME)
            target=ROOT/'source.mp4'
            if not target.exists():
                partial=ROOT/'source.mp4.partial'
                subprocess.run(['curl','--fail','--location','--retry','3','--continue-at','-', '--max-time','3600','--output',str(partial),url],check=True)
                if partial.stat().st_size!=int(entry['size']):raise ValueError('Download size differs')
                with partial.open('rb') as f:digest=hashlib.file_digest(f,'sha1').hexdigest()
                if digest!=entry['sha1']:raise ValueError('Download checksum differs')
                partial.rename(target)
            binding=media_binding(target,int(entry['size']))
            probe=json.loads(subprocess.check_output(['/usr/bin/ffprobe','-v','error','-show_format','-show_streams','-of','json',str(target)]))
            r.put(ROOT/'media-probe.json',probe)
            if not any(x['codec_type']=='audio' for x in probe['streams']):raise ValueError('No audio stream')
            duration=round(float(probe['format']['duration'])*1000)
            recording=dict(recording_id='media_sha256_'+binding['sha256'],media=binding,duration_ms=duration,
                title='Hiding in my room is live!',date=dict(value='2026-09-17',basis='archive_filename'),
                source_ids=dict(youtube=['Sr9Vv5pVG1g'],archive_native=['699994/'+NAME]),
                aliases=[dict(title='Hiding in my room is live!',canonical_url=url,platform='internet_archive',source_native_id='699994/'+NAME,youtube_id='Sr9Vv5pVG1g',date=dict(value='2026-09-17',basis='archive_filename'))],
                state='ready',reasons=[],audio_state='audio_present')
            acquisition=r.put(ROOT/'acquisition-result.json',dict(
                kind='himr_new_archive_acquisition',schema_version=1,status='completed',errors=[],
                admission=screen._raw_recording(recording),source_url=url,
                archive_metadata=r.binding(ROOT/'archive-metadata.json'),archive_sha1=entry['sha1']))
            recording['aliases'][0]['acquisition_result']=acquisition
            prior=t.prior_evidence(BASE,recording)
            if prior:raise ValueError('Prior cloud work exists; inspect before submitting')
            r.put(ROOT/'recording.json',recording)
            status('screening_speakers');r.mkdir(ROOT/'screen')
            config=t.read(BASE/'transcription-v5/plan.json')['screen_config']
            decision=screen.screen_one(recording,ROOT/'screen',config)
            r.put(ROOT/'speaker-screen.json',decision)
            route=selective.route(recording,decision,[])
            r.put(ROOT/'selective-routing.json',route)
            old=t.read(BASE/'targeted-retranscription-20260917-v3/plan.json')
            maximum=math.ceil((duration+t.media.tolerance_ms(duration))/1000)*230000//3600+1
            if duration>t.client.ASSEMBLYAI_MAX_SECONDS*1000 or maximum>1_000_000:raise ValueError('New item exceeds one-dollar/AssemblyAI admission')
            job='newarchive_'+binding['sha256'][:32]
            row=dict(job_id=job,recording=recording,reason='explicit_new_archive_item',probe_start_ms=0,alternate_recordings=[],prior_paid=[],diarization=route['diarization'],provider='assemblyai',language='auto',maximum_cost_microusd=maximum)
            r.mkdir(ROOT/'jobs');r.mkdir(ROOT/'jobs'/job);r.mkdir(ROOT/'reservations')
            plan=dict(kind=t.KIND,state_root=str(ROOT),base=str(BASE),implementation=t.implementations(),ffmpeg=r.binding('/usr/bin/ffmpeg'),vad=old['vad'],recordings=[row],allocation_microusd=1_000_000,maximum_cost_microusd=maximum,approval='User requested processing this new archive item; no existing recordings replaced.',new_item_implementation=r.binding(__file__))
            r.put(ROOT/'plan.json',plan)
        ref=r.binding(ROOT/'plan.json');plan=r.read(ref)
        if plan['new_item_implementation']!=r.binding(__file__):raise ValueError('New-item implementation changed')
        row=plan['recordings'][0];job=row['job_id'];folder=ROOT/'jobs'/job
        status('cloud_transcription',diarization=row['diarization'],maximum_asr_usd=row['maximum_cost_microusd']/1e6)
        result=t.process(ref,job,str(Path('.env').resolve()))
        if result!='completed':status('needs_attention',transcription_state=result);return
        state=t.read(folder/'status.json')
        if state['requires_speaker_review']:status('awaiting_speaker_review');return
        doc=t.read(folder/'transcript.json')
        if len(doc['text'].split())<50:status('transcribed_summary_withheld_short');return
        summary=ROOT/'summary';r.mkdir(summary)
        screen_ref=r.put(summary/'screen.json',dict(kind='himr_cloud_speaker_screen_decision',schema_version=1,recording_id=doc['recording_id'],media=doc['source_media'],diarization=doc['diarization_requested'],source=r.binding(ROOT/'speaker-screen.json')))
        canonical={k:v for k,v in doc.items() if k!='recovery_plan'}
        canonical.update(screen_decision=screen_ref,verified_quotation=False,normalizer_implementation_sha256=r.binding(t.client.__file__)['sha256'])
        transcript=r.put(summary/'transcript.json',canonical)
        completion=r.put(summary/'completion.json',dict(kind='himr_cloud_transcription_completion',schema_version=1,job_id=job,audio=doc['audio'],raw_result=doc['raw_result'],provider_job=doc['provider_job'],screen_decision=screen_ref,transcript=transcript))
        date_ref=r.put(summary/'date.json',dict(kind='himr_summary_date_evidence',schema_version=1,recording_id=doc['recording_id'],value='2026-09-17',date_kind='recorded',basis='direct_catalogue_metadata'))
        spec=dict(transcript=transcript,completion=completion,format='cloud',recording_id=doc['recording_id'],title=row['recording']['title'],date=dict(value='2026-09-17',kind='recorded',evidence=date_ref))
        r.sources_module.normalize_source(spec)
        config={**r.core.DEFAULT_CONFIG,'timeline_profile':'gemini_flash_batch','max_chunk_input_bytes':24000,'max_evidence_refs_per_item':256,'gemini_schema_policy':'local_array_bounds_v2','transcript_input_policy':'text_and_speaker_evidence_v1'}
        manifest=campaign.validate_manifest(dict(kind=campaign.KIND,schema_version=1,state_root=str(summary/'run'),shards=[[spec]],config=config,budget_microusd=2_000_000,max_active_shards=1,poll_seconds=30,max_runtime_seconds=86400,cloud=dict(processing_approved=True,paid_tier_confirmed=True),classification_policy='conservative_evidence_inheritance_v1',implementation=campaign.implementation()))
        mref=r.put(summary/'manifest.json',manifest);status('summarizing')
        outcome=campaign.run(mref['path'],mref['sha256'],allow_paid_api=True)
        status('processed',summary_status=outcome,publication_performed=False,corpus_integration_pending=True)


if __name__=='__main__':
    try:main()
    except Exception as error:
        if ROOT.exists():status('needs_attention',error_type=type(error).__name__,reason=str(error)[:300])
        raise
