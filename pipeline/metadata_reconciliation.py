"""Offline reconciliation into separate copies; no paid submissions or identity guesses."""
import argparse
from collections import Counter
from copy import deepcopy
import re
import hashlib
import stat
from pathlib import Path
from pipeline import transcript_summary as io
from pipeline import cloud_transcription_import as third


def third_party_copy(recording,match,inventory):
    if len(match['candidates'])!=1:raise ValueError('multiple different transcript candidates')
    allowed_basis={'exact_youtube_id','literal_archive_source_filename','exact_media_stem'}
    if any(re.search(r'\[[A-Za-z0-9_-]{11}\]$',key) for key in match['matched_keys']):allowed_basis.add('literal_export_basename')
    if not match['matching_basis'] or any(x not in allowed_basis for x in match['matching_basis']):
        raise ValueError('source identity not exact')
    allowed={'identity_key_matches_multiple_recordings','possible_missing_tail','nonconsecutive_cue_numbers','nonmonotonic_cue_start'}
    if set(match['issues'])-allowed:raise ValueError('transcript structure or timeline requires review')
    duration=recording['duration_ms']
    if not duration:raise ValueError('missing media duration')
    candidate=match['candidates'][0]
    duration_selected=False
    if match['status']=='ambiguous':
        durations=[inventory[rid]['duration_ms'] for rid in match['related_physical_recordings']]
        if any(not d or abs(d-duration)>max(10000,duration*.01) for d in durations):
            end=candidate.get('last_end_ms')
            others=[d for d in durations if d and abs(d-duration)>max(10000,duration*.01)]
            duration_selected=bool(end and abs(duration-end)<=max(120000,duration*.05)
                and others and all(abs(duration-end)<abs(d-end) for d in others))
            if not duration_selected:raise ValueError('different-duration variants need timeline alignment')
    original=dict(path=candidate['path'],sha256=candidate['sha256'])
    source=Path(original['path'])
    if not stat.S_ISREG(source.lstat().st_mode) or source.stat().st_size>third.MAX_FILE_BYTES:
        raise ValueError('third-party source must be a bounded regular file')
    body=source.read_bytes()
    if hashlib.sha256(body).hexdigest()!=original['sha256']:raise ValueError('third-party source changed')
    parsed=third.parse_transcript(body)
    if not parsed['segments'] or parsed['format']!='srt' or set(parsed['issues'])-{'nonconsecutive_cue_numbers','nonmonotonic_cue_start'}:
        raise ValueError('empty or structurally unsupported transcript')
    reordered='nonmonotonic_cue_start' in parsed['issues']
    if reordered:parsed['segments']=sorted(parsed['segments'],key=lambda s:(s['start_ms'],s['end_ms']))
    end=max(s['end_ms'] for s in parsed['segments'])
    if end>duration+max(10000,duration*.01):raise ValueError('transcript exceeds media duration')
    partial=duration-end>max(120000,duration*.1)
    return dict(kind='himr_third_party_transcript_import',schema_version=1,status='completed',
        recording_id=recording['recording_id'],segments=parsed['segments'],
        provenance=dict(attribution='u/MelatoninHighs',label='HIMR-Transcripts; Universal-3.5 Pro (author confirmation reported by operator)',
            source_url='https://old.reddit.com/r/HIMRFAM2/comments/1weonll/himr_transcripts_2015september_2026_excluding/',
            original=original,rights_note='Local preparation only; no publication rights inferred.'),
        reconciliation=dict(method='exact_source_identity_and_compatible_variant_duration',
            partial=partial,media_alignment_verified=False,original_text_changed=False,
            cues_reordered_by_retained_timestamps=reordered,duration_disambiguated=duration_selected,
            coverage_note=('Partial transcript: the ending may be missing. ' if partial else '')+
                ('Cues reordered by their existing timestamps; no words or timestamps changed. ' if reordered else '')+
                ('This duration is the closest compatible match among different-length copies. ' if duration_selected else '')+
                'Mapped by exact source identity and retained duration; audio alignment and full coverage are unverified.'))


def cloud_copy(row,folder):
    raw_ref=io.binding(folder/'terminal-job.json');raw=io.read(raw_ref)
    audio=io.read(io.binding(folder/'audio.json'))['audio']
    if raw.get('status')!='completed':raise ValueError('provider result not completed')
    if not raw.get('text') or not raw.get('utterances'):raise ValueError('provider recognized no usable speech')
    duration=audio['duration_ms'];segments=[];labels={}
    for u in raw['utterances']:
        start,end=u.get('start'),u.get('end');label=u.get('speaker')
        if type(start)!=int or type(end)!=int or not 0<=start<end<=duration+2000:
            raise ValueError('invalid utterance timestamp')
        if not isinstance(label,str) or not re.fullmatch('[A-Z]{1,4}',label):raise ValueError('invalid provider speaker')
        if not isinstance(u.get('text'),str) or not u['text'].strip():raise ValueError('empty utterance')
        labels.setdefault(label,'SPEAKER_'+str(len(labels)).zfill(4))
        segments.append(dict(start_ms=start,end_ms=end,text=u['text'],speaker=labels[label]))
    if any(a['start_ms']>b['start_ms'] for a,b in zip(segments,segments[1:])):raise ValueError('utterance order mismatch')
    clean=lambda s:' '.join(s.split())
    if clean(raw['text'])!=clean(' '.join(s['text'] for s in segments)):raise ValueError('provider text differs from utterances')
    return dict(kind='himr_cloud_recording_transcript',schema_version=1,status='completed',
        job_id=row['job_id'],recording_id=row['recording']['recording_id'],provider='assemblyai',model=raw.get('speech_model') or 'AssemblyAI (retained provider result)',
        source_media=row['recording']['media'],audio=audio,duration_seconds=duration/1000,
        raw_result=raw_ref,diarization_requested=True,segments=segments,
        full_media_coverage_verified=False,human_reviewed=False,machine_generated=True,
        verified_quotation=False,publication_authority=False,speaker_labels_are_identities=False,
        reconciliation=dict(method='retain_provider_utterances_without_word_timing',partial=False,
            coverage_note='Recovered existing provider utterances. Word timing mismatch is not used; utterance timestamps and text are unchanged.'))


def reconcile(base,preview,root):
    io.mkdir(root)
    io.mkdir(root/'transcripts')
    plan=io.read(io.binding(base/'transcription-v5/plan.json'))
    rows={r['recording']['recording_id']:r for r in plan['recordings']};inventory={k:r['recording'] for k,r in rows.items()}
    matches_doc=io.read(io.binding(base/'transcription-v5/third-party-matches.json'))
    matches=dict(zip(matches_doc['recording_ids'],matches_doc['matches']))
    source_report=io.read(io.binding(preview/'preparation.json'))
    review=deepcopy(io.read(io.binding(base/'reviewed-transcript-feed-v1/review-index.json')))
    known={r['job_id'] for r in review['reports']};records=[];held=[];counts=Counter()
    for exclusion in source_report['excluded']:
        pid=exclusion['recording_id'];row=rows[pid];folder=base/'transcription-v5/jobs'/row['job_id']
        try:
            if exclusion['reason']=='anonymous multi-speaker transcript needs review':
                ref=io.binding(folder/'transcript.json');doc=io.read(ref)
            elif matches[pid]['status'] in {'ambiguous','review_required'}:
                doc=third_party_copy(inventory[pid],matches[pid],inventory)
                ref=io.put(root/'transcripts'/(row['job_id']+'.json'),doc)
            elif (folder/'collection-review.json').exists():
                doc=cloud_copy(row,folder);ref=io.put(root/'transcripts'/(row['job_id']+'.json'),doc)
            else:raise ValueError(exclusion['reason'])
            labels={s.get('speaker') for s in doc['segments'] if s.get('speaker')}
            needs_review=len(labels)>1
            records.append(dict(recording_id=pid,transcript=ref,needs_speaker_review=needs_review,
                reconciliation=doc.get('reconciliation',{})))
            counts['needs_speaker_review' if needs_review else 'ready']+=1
            counts['partial']+=bool(doc.get('reconciliation',{}).get('partial'))
            if needs_review and row['job_id'] not in known:
                review['reports'].append(dict(job_id=row['job_id'],title=inventory[pid]['title'],
                    transcript=ref,candidates=[],recovered_copy=True))
                known.add(row['job_id']);counts['added_to_speaker_review']+=1
        except (ValueError,RuntimeError,KeyError) as error:
            held.append(dict(recording_id=pid,title=inventory[pid]['title'],reason=str(error)))
    index=dict(kind='himr_metadata_reconciliation',records=records,held=held,counts=dict(counts),
        source_preview=io.binding(preview/'preparation.json'),new_paid_requests=0,original_transcripts_modified=False)
    io.put(root/'index.json',index);io.put(root/'review-index.json',review)
    return dict(counts=dict(counts),held=len(held),held_reasons=dict(Counter(r['reason'] for r in held)),index=io.binding(root/'index.json'))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('base','preview','root'):p.add_argument('--'+name,required=True,type=Path)
    a=p.parse_args();print(io.canonical(reconcile(a.base,a.preview,a.root)).decode())

if __name__=='__main__':main()
