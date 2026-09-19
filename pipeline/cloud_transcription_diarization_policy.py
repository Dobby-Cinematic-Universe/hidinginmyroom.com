"""Risk-based future diarization with bounded, private audio follow-up proofs.

No paid calls, transcript replacement or speaker identification occur here.
Original screen decisions remain immutable. Low-risk uncertain recordings wait
for at most eight new short probes; an unresolved follow-up keeps diarization.
"""
from __future__ import annotations

import argparse
from collections import Counter
from copy import deepcopy
import hashlib
from pathlib import Path
import signal
import time

from pipeline import transcript_summary as io
from pipeline import cloud_transcription_screen as screen
from pipeline import cloud_transcription_title_policy as titles

mm, core, engine = screen.mm, screen.core, screen.mm.engine
KIND = 'himr_risk_based_cloud_diarization'
RULES = {'version': 1, 'followup_windows': 8, 'window_ms': 10000,
         'negative_min_usable_quarters': 4, 'positive_audio_keeps_diarization': True,
         'conversation_titles_keep_diarization': True, 'strong_text_leads_keep_diarization': True,
         'multiple_face_cues_keep_diarization': True, 'inconclusive_followup_keeps_diarization': True,
         'existing_paid_decisions_frozen': True, 'automatic_retranscription': False,
         'whole_recording_solo_proven': False, 'speaker_identity_inferred': False}


class PolicyError(RuntimeError):
    pass


def implementation():
    return {'policy': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'screen': screen.implementation(),
            'titles': hashlib.sha256(Path(titles.__file__).read_bytes()).hexdigest()}


def prepare(plan_ref, root, text_report_ref=None):
    plan = io.read(plan_ref)
    if plan.get('kind') != 'himr_third_party_first_cloud_transcription_plan':
        raise PolicyError('follow-up requires the bound cloud campaign')
    config = screen.load_config(plan['screen_config'])
    template = mm.load_manifest(config['template_manifest']['path'], config['template_manifest']['sha256'])
    ids = set()
    if text_report_ref is not None:
        report = io.read(text_report_ref)
        if report.get('kind') != 'himr_transcript_conversation_review_leads' or report['inventory'] != plan['inventory']:
            raise PolicyError('text leads do not match the campaign inventory')
        for lead in report['leads']:
            if lead['tier'] in {'strong_text_lead', 'moderate_text_lead'}:
                ids.update(lead['exact_selected_recording_ids'])
    root = io.safe.path_value(root)
    io.protect(root, {'plan': plan_ref, 'recordings': plan['recordings'],
                      'screen': plan['screen_config'], 'template': template,
                      'report': text_report_ref, 'cloud_root': plan['state_root']})
    io.mkdir(root)
    io.mkdir(root / 'jobs')
    value = {'kind': KIND + '_policy', 'schema_version': 1, 'plan': plan_ref,
             'configuration': plan['screen_config'], 'state_root': str(root),
             'text_report': text_report_ref, 'text_recordings': sorted(ids),
             'audio_models': template['audio_models'], 'audio_python': template['audio_python'],
             'ffmpeg': template['ffmpeg'], 'rules': RULES, 'implementation': implementation()}
    return io.put(root / 'policy.json', value)


def load(ref):
    value = io.read(ref)
    if (value.get('kind') != KIND + '_policy' or value.get('schema_version') != 1
            or value.get('rules') != RULES or value.get('implementation') != implementation()
            or ref['path'] != str(Path(value['state_root']) / 'policy.json')):
        raise PolicyError('risk policy or implementation changed')
    io.safe.file_binding(value['plan'])
    io.safe.file_binding(value['configuration'])
    io.safe.path_value(value['state_root'])
    if not isinstance(value.get('text_recordings'), list) or len(value['text_recordings']) > 10000:
        raise PolicyError('text lead selection exceeds its bound')
    return value


def risk(recording, base, policy):
    reasons = []
    if base['state'] == 'screen_positive':
        reasons.append('supported_acoustic_diversity')
    if titles.match_titles(recording)['matched']:
        reasons.append('explicit_conversation_title')
    if recording['recording_id'] in policy['text_recordings']:
        reasons.append('strong_or_moderate_exact_transcript_lead')
    if base['evidence_summary'].get('multiple_face_samples', 0):
        reasons.append('visual_people_cue')
    return {'route': 'diarize' if reasons else 'no_diarization' if base['state'] == 'screen_negative'
            else 'followup_required', 'reasons': reasons}


def windows(start_ms, end_ms):
    duration = core.interval(start_ms, end_ms)
    width = min(RULES['window_ms'], max(1, duration // RULES['followup_windows']))
    result = []
    for i in range(RULES['followup_windows']):
        center = start_ms + (2 * i + 1) * duration // (2 * RULES['followup_windows'])
        start = max(start_ms, min(end_ms - width, center - width // 2))
        if any(start < row['end_ms'] and row['start_ms'] < start + width for row in result):
            continue
        result.append({'index': len(result), 'start_ms': start, 'end_ms': start + width,
                       'reason': 'risk_followup_quarter_' + str(i // 2)})
    return result


def _context(recording, base):
    result = mm.read(base['result'])
    visual = mm.read(result['visual_proof'])
    metadata = mm.read(visual['probe'])['media']
    manifest = mm.load_manifest(base['manifest']['path'], base['manifest']['sha256'])
    matches = [row for row in manifest['jobs'] if str(Path(manifest['state_root']) / 'jobs' /
               row['job_id'] / 'result.json') == base['result']['path']]
    if len(matches) != 1:
        raise PolicyError('follow-up base result is not a unique admitted recording')
    job = screen._validate_job(recording, manifest, matches[0])
    old = list(job['cached_audio']['excerpts'])
    for reference in result['audio_proofs']:
        old.extend(mm.read(reference)['excerpts'])
    old = [{**row, 'id': 'old-' + row['id'], 'probe_id': 'old-' + row['probe_id']} for row in old]
    return result, metadata, old


def job_plan(recording, base_ref, policy_ref):
    policy = load(policy_ref)
    base = io.read(base_ref)
    screen.validate_decision(base, recording, policy['configuration'])
    result, metadata, old = _context(recording, base)
    audio = metadata['audio']
    span = audio['span'] if audio['state'] == 'available' else None
    planned = [] if span is None else windows(span['start_ms'] + min(100,
        (span['end_ms'] - span['start_ms']) // 4), span['end_ms'])
    return {'kind': KIND + '_job', 'schema_version': 1, 'policy': policy_ref,
            'recording': recording, 'base_decision': base_ref, 'source_witness': base['source_witness'],
            'audio': audio, 'source_start_ms': metadata.get('source_start_ms', 0),
            'windows': planned, 'base_excerpts': old,
            'visual_uncertain': bool(result['visual']['frames_needing_review']) or
                result['visual']['state'] not in {'no_multiple_faces_observed', 'no_video'}}


def classify(job, checkpoints):
    if len(checkpoints) != len(job['windows']):
        raise PolicyError('follow-up is incomplete')
    fresh, usable_quarters = [], set()
    failed = 0
    for value, window in zip(checkpoints, job['windows'], strict=True):
        mm.validate_audio_checkpoint(value, window)
        if value.get('job_sha256') != io.digest(job):
            raise PolicyError('follow-up checkpoint belongs to a different job')
        fresh.extend(value['excerpts'])
        if value['state'] != 'analyzed':
            failed += 1
        else:
            if value['receipt'].get('timestamp_discontinuities'):
                failed += 1
            if value['excerpts']:
                usable_quarters.add(int(window['reason'].rsplit('_', 1)[1]))
    diversity = core.audio_diversity(job['base_excerpts'] + fresh)
    if diversity['state'] == 'supported_audio_diversity':
        state, reason = 'screen_positive', 'followup_supported_acoustic_diversity'
    elif (len(usable_quarters) == RULES['negative_min_usable_quarters'] and not failed
          and not job['visual_uncertain'] and not diversity['anchor_search_capped']):
        state, reason = 'screen_negative', 'usable_followup_speech_across_all_four_quarters_without_supported_diversity'
    else:
        state, reason = 'screen_uncertain', 'bounded_followup_remained_inconclusive'
    return {'state': state, 'diarization': state != 'screen_negative', 'reason': reason,
            'usable_quarters': sorted(usable_quarters), 'sample_failures': failed,
            'fresh_excerpts': len(fresh), 'audio': diversity,
            'whole_recording_solo_proven': False, 'speaker_identity_inferred': False}


def result_path(policy, job_id):
    return Path(policy['state_root']) / 'jobs' / job_id / 'result.json'


def validate_result(reference, recording, base_ref, policy_ref):
    value = io.read(reference)
    job = io.read(value['job'])
    expected = job_plan(recording, base_ref, policy_ref)
    if job != expected:
        raise PolicyError('follow-up plan no longer matches its source and policy')
    folder = Path(value['job']['path']).parent
    refs = value['checkpoints']
    if len(refs) != len(job['windows']) or any(ref['path'] != str(folder / f'probe-{i:02d}.json')
                                               for i, ref in enumerate(refs)):
        raise PolicyError('follow-up checkpoint selection differs')
    expected_result = {'kind': KIND + '_result', 'schema_version': 1, 'job': value['job'],
                       'checkpoints': refs, 'decision': classify(job, [io.read(ref) for ref in refs])}
    if value != expected_result:
        raise PolicyError('follow-up decision does not replay')
    return value['decision']


def effective(recording, base_ref, policy_ref, followup_ref=None):
    policy = load(policy_ref)
    base = io.read(base_ref)
    routing = risk(recording, base, policy)
    if routing['route'] == 'followup_required':
        if followup_ref is None:
            return None
        followup = validate_result(followup_ref, recording, base_ref, policy_ref)
        state, diarization = followup['state'], followup['diarization']
    else:
        state, diarization = base['state'], routing['route'] == 'diarize'
    output = deepcopy(base)
    output.update(state=state, diarization=diarization)
    output['method']['diarization_review'] = {'policy': policy_ref, 'base_decision': base_ref,
        'followup': followup_ref, 'routing': routing, 'paid_requests_reinterpreted': False}
    return output


def validate_effective(value, recording, config_ref):
    proof = value.get('method', {}).get('diarization_review')
    if not isinstance(proof, dict):
        raise PolicyError('missing diarization follow-up policy proof')
    policy = load(proof['policy'])
    if policy['configuration'] != config_ref:
        raise PolicyError('follow-up base configuration differs')
    base = io.read(proof['base_decision'])
    screen.validate_decision(base, recording, config_ref)
    expected = effective(recording, proof['base_decision'], proof['policy'], proof['followup'])
    if value != expected:
        raise PolicyError('effective diarization policy does not replay')
    return value


def preview(recording, base, policy, job_id):
    routing = risk(recording, base, policy)
    return routing['route'] != 'followup_required' or io.safe.exists(result_path(policy, job_id))


def run(policy_ref, *, max_jobs=10000, max_seconds=86400, stopping=lambda: False):
    policy = load(policy_ref)
    plan = io.read(policy['plan'])
    root = Path(policy['state_root'])
    deadline = time.monotonic() + max_seconds
    counts = Counter()
    model = None
    with io.locked(root):
        for row in plan['recordings']:
            if stopping() or time.monotonic() >= deadline or sum(counts.values()) >= max_jobs:
                break
            if row['disposition'] != 'cloud':
                continue
            original = Path(plan['state_root']) / 'jobs' / row['job_id']
            if (io.safe.exists(original / 'intent.json') or io.safe.exists(Path(plan['state_root']) /
                    'reservations' / (row['job_id'] + '.json'))):
                continue
            base_ref = io.binding(original / 'screen.json')
            base = io.read(base_ref)
            if risk(row['recording'], base, policy)['route'] != 'followup_required':
                continue
            target = result_path(policy, row['job_id'])
            if io.safe.exists(target):
                continue
            folder = target.parent
            io.mkdir(folder)
            job = job_plan(row['recording'], base_ref, policy_ref)
            job_ref = io.put(folder / 'job.json', job)
            refs = []
            with io.safe.opened(row['recording']['media']['path']) as source_fd, \
                    io.safe.opened(policy['ffmpeg']['path']) as ffmpeg_fd:
                for window in job['windows']:
                    if stopping() or time.monotonic() >= deadline:
                        return {'state': 'paused', 'completed': dict(counts), 'paid_requests': 0}
                    path = folder / f"probe-{window['index']:02d}.json"
                    if not io.safe.exists(path):
                        if io.safe.witness(source_fd) != job['source_witness']:
                            raise PolicyError('source changed before follow-up')
                        try:
                            pcm, receipt = engine.decode_audio(source_fd, ffmpeg_fd, job['audio']['stream_index'],
                                window, source_start_ms=job['source_start_ms'])
                            if model is None:
                                model = engine.AudioEngine(io.read(policy['audio_models']))
                            observation = model.analyze(pcm, receipt, f"fresh-{window['index']}")
                            value = {'state': 'analyzed', 'window': window, 'receipt': receipt, **observation}
                        except engine.DecodeReview:
                            value = {'state': 'needs_review', 'window': window, 'excerpts': []}
                        if io.safe.witness(source_fd) != job['source_witness']:
                            raise PolicyError('source changed during follow-up')
                        io.put(path, {'job_sha256': io.digest(job), **value})
                    refs.append(io.binding(path))
            value = {'kind': KIND + '_result', 'schema_version': 1, 'job': job_ref,
                     'checkpoints': refs, 'decision': classify(job, [io.read(ref) for ref in refs])}
            io.put(target, value)
            counts[value['decision']['state']] += 1
            print(io.canonical({'event': 'followup_completed', 'job_id': row['job_id'],
                'decision': {k: value['decision'][k] for k in ('state', 'diarization', 'usable_quarters')},
                'completed_this_run': sum(counts.values()), 'paid_requests': 0}).decode().strip(), flush=True)
    return {'state': 'paused' if stopping() else 'followup_run_finished', 'completed': dict(counts), 'paid_requests': 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--policy', required=True)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--max-jobs', type=int, default=10000)
    parser.add_argument('--max-seconds', type=int, default=86400)
    args = parser.parse_args()
    io.safe.integer(args.max_jobs, 1, 10000, 'follow-up job limit')
    io.safe.integer(args.max_seconds, 1, 86400, 'follow-up runtime')
    paused = [False]
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: paused.__setitem__(0, True))
    # Same network-denied, resource-bounded CPU environment as existing screens.
    io.safe.child_limits(4 * 1024**3)
    result = run({'path': args.policy, 'sha256': args.expected_sha256}, max_jobs=args.max_jobs,
                 max_seconds=args.max_seconds, stopping=lambda: paused[0])
    print(io.canonical(result).decode().strip(), flush=True)


if __name__ == '__main__':
    main()
