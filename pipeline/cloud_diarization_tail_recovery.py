"""Recover valid turns while quarantining an exact invalid repetitive tail.

This is an explicitly partial derivative, not a claim that the provider response
passed validation. No timestamps are fabricated, no speech text is rewritten,
and the anomalous final turn remains recoverable in its own bound artifact.
"""
import argparse
from collections import Counter
from copy import deepcopy
import os
from pathlib import Path
import time

from pipeline import cloud_transcription_client as client
from pipeline import reviewed_transcript_feed as feed
from pipeline import transcript_summary as io

JOB = 'cloudjob_47ecb56b9c0e65b638da8f4d17fd0b4a'


def isolate_tail(raw, duration_ms):
    turns=raw.get('utterances')
    if (type(duration_ms) is not int or duration_ms<=0 or not isinstance(turns,list)
            or len(turns)<2):raise ValueError('not a bounded multi-turn recording')
    tail=turns[-1];words=tail.get('words')
    if not isinstance(words,list) or not 64<=len(words)<=512:
        raise ValueError('tail does not match the bounded repetition defect')
    if any(not isinstance(w.get('text'),str) for w in words):
        raise ValueError('invalid repetition text')
    tokens=Counter(w['text'].casefold().strip('.,!?') for w in words)
    if len(tokens)!=1 or not next(iter(tokens)):
        raise ValueError('cannot quarantine ordinary mixed-word speech')
    if (type(tail.get('start')) is not int or type(tail.get('end')) is not int
            or not 0<=tail['start']<duration_ms<tail['end']<=duration_ms+5000
            or not 0<duration_ms-tail['start']<=30000
            or turns[-2]['end']>tail['start']):
        raise ValueError('tail is not a short bounded end-of-file overrun')
    if (not raw['text'].endswith(tail['text'])
            or raw.get('words',[])[-len(words):]!=words):
        raise ValueError('tail differs between provider representations')
    adjusted=deepcopy(raw)
    adjusted['utterances']=adjusted['utterances'][:-1]
    adjusted['words']=adjusted['words'][:-len(words)]
    adjusted['text']=adjusted['text'][:-len(tail['text'])].rstrip()
    # Complete unchanged validation must pass for every retained turn. Any
    # additional defect elsewhere is not silently repaired by this operation.
    normalized=client.normalize_result('assemblyai',adjusted,
        expected_duration_seconds=duration_ms/1000,job=raw,diarization=True,language='auto')
    if not normalized['segments'] or normalized['text']!=adjusted['text']:
        raise ValueError('retained prefix failed text validation')
    anomaly=dict(kind='himr_quarantined_provider_tail',utterance_index=len(turns)-1,
        original_utterance=deepcopy(tail),audio_start_ms=tail['start'],audio_end_ms=duration_ms,
        provider_end_ms=tail['end'],overrun_ms=tail['end']-duration_ms,
        repetition_count=len(words),word_timestamps_reliable=False,
        reason='single_token_repetition_and_timestamps_beyond_end_of_audio',
        audio_content_confirmed=False,original_audio_unchanged=True,
        excluded_from_recovered_transcript=True,human_review_required=True)
    return adjusted,normalized,anomaly


def recover(plan_ref, expected_raw_sha256, report_ref, output):
    plan=io.read(plan_ref);row=next(r for r in plan['recordings'] if r['job_id']==JOB)
    folder=Path(plan['state_root'])/'jobs'/JOB;out=Path(output).resolve();io.mkdir(out)
    with io.locked(folder):
        if (folder/'completion.json').exists():raise ValueError('completed job cannot be replaced')
        hold_ref=io.binding(folder/'hold.json');hold=io.read(hold_ref)
        if hold['plan']!=plan_ref or hold['job_id']!=JOB or hold['reason']!='invalid word timestamp':
            raise ValueError('unexpected collection hold')
        intent=io.read(io.binding(folder/'intent.json'))
        if (intent!=io.read(io.binding(Path(plan['state_root'])/'reservations'/(JOB+'.json')))
                or intent['plan']!=plan_ref or intent['provider']!='assemblyai'
                or intent['diarization'] is not True or intent['language']!='auto'):
            raise ValueError('paid intent binding differs')
        raw_ref=dict(path=str(folder/'terminal-job.json'),sha256=expected_raw_sha256)
        raw=io.read(raw_ref);receipt=io.read(io.binding(folder/'submission.json'))
        remote=client.validate_job('assemblyai',receipt)
        client.validate_job('assemblyai',raw,expected_job_id=remote['job_id'])
        saved=io.read(io.binding(folder/'audio.json'));audio=intent['audio']
        if (saved['audio']!=audio or saved['source']!=row['recording']['media']
                or row['recording']['recording_id']!=intent['recording_id']):
            raise ValueError('original media/audio binding differs')
        screen_ref=io.binding(folder/'screen.json');screen=io.read(screen_ref)
        if screen['plan']!=plan_ref or screen['diarization'] is not True:
            raise ValueError('diarization authority differs')
        adjusted,normalized,anomaly=isolate_tail(raw,audio['duration_ms'])
        report=io.read(report_ref)
        if any(r['job_id']==JOB for r in report['reports']):raise ValueError('review report already contains this job')
        saved_hold_ref=io.put(out/'original-hold.json',hold)
        anomaly.update(original_raw_result=raw_ref,original_hold=saved_hold_ref,
            source_media=row['recording']['media'],job_id=JOB,
            approval='User: Please continue with the targeted recovery.')
        anomaly_ref=io.put(out/'quarantined-tail.json',anomaly)
        adjusted_ref=io.put(out/'retained-prefix-result.json',adjusted)
        recovery=dict(kind='partial_prefix_recovery_with_quarantined_repetitive_tail',
            original_raw_result=raw_ref,original_hold=saved_hold_ref,adjusted_result=adjusted_ref,
            quarantine=anomaly_ref,implementation=io.binding(__file__),
            retained_turn_count=len(normalized['segments']),omitted_turn_count=1,
            omitted_audio_start_ms=anomaly['audio_start_ms'],omitted_audio_end_ms=anomaly['audio_end_ms'],
            retained_speech_text_changed=False,retained_segment_timestamps_changed=False,
            partial_recovery=True,paid_retry=False,human_review_required=True)
        doc=dict(kind='himr_cloud_recording_transcript',schema_version=1,job_id=JOB,
            recording_id=row['recording']['recording_id'],source_media=row['recording']['media'],
            status='completed',provider_job_id=remote['job_id'],raw_result=raw_ref,provider_job=raw_ref,
            screen_decision=screen_ref,audio=audio,whole_recording_submitted=True,
            normalizer_implementation_sha256=io.binding(client.__file__)['sha256'],
            machine_generated=True,full_media_coverage_verified=False,human_reviewed=False,
            verified_quotation=False,speaker_identity_inferred=False,publication_authority=False,
            language_policy=row['language'],detected_language=raw['language_code'],
            source_third_party_preserved=row['retained_third_party']['transcript'],
            partial_recovery=True,recovery=recovery,**normalized)
        transcript_ref=io.put(folder/'transcript.json',doc)
        report['reports'].append(dict(job_id=JOB,title=row['recording']['title'],transcript=transcript_ref,
            recovered_copy=True,candidates=[dict(segment_index=len(doc['segments'])-1,flags=[
                'Partial recovery: final 22 seconds retained separately for review; invalid repetitive provider tail excluded.'])]))
        report.update(recovery_base_report=report_ref,targeted_recovery_plan=plan_ref,
            original_provider_results_preserved=True,new_paid_requests=0)
        report_output=io.put(out/'review.json',report)
        completion=dict(kind='himr_cloud_transcription_completion',schema_version=1,job_id=JOB,
            audio=audio,raw_result=raw_ref,provider_job=raw_ref,transcript=transcript_ref,
            screen_decision=screen_ref,batch_plan=plan_ref,partial_recovery=True,recovery=io.binding(out/'quarantined-tail.json'))
        io.put(folder/'completion.json',completion)
        retained=folder/'hold-resolved-partial-tail-recovery-20260915.json'
        if retained.exists():raise ValueError('prior recovery hold already exists')
        # Every evidence binding references the durable copy above; retain the
        # original marker too under an explicitly resolved name.
        os.rename(folder/'hold.json',retained)
        with io.paths.retained_directory(folder) as fd:os.fsync(fd)
        feed.atomic(folder/'status.json',dict(job_id=JOB,state='completed',partial_recovery=True,
            provider='assemblyai',provider_job_id=remote['job_id'],updated_unix=time.time(),
            requires_speaker_review=True,quarantined_tail=True))
        return dict(transcript=transcript_ref,report=report_output,quarantine=anomaly_ref,
            retained_turns=len(doc['segments']),paid_requests=0,partial_recovery=True)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for flag in ('plan','expected-plan-sha256','expected-raw-sha256','report','output'):
        p.add_argument('--'+flag,required=True)
    a=p.parse_args();os.umask(0o077)
    print(feed.encode(recover(dict(path=a.plan,sha256=a.expected_plan_sha256),
        a.expected_raw_sha256,io.binding(a.report),a.output)).decode(),end='')


if __name__=='__main__':main()
