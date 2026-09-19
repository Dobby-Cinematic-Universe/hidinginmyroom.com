"""Separate third-party-first, whole-recording AssemblyAI/Rev AI campaign.

Prepare/import/status/export are offline. Paid runs require explicit authorization
and an immutable workspace spending limit. A durable intent without a receipt
requires GET-only reconciliation, never automatic resubmission or provider retry.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation
import hashlib
import math
import os
from pathlib import Path
import signal
import stat
import sys
import time

from pipeline import transcript_summary as io
from pipeline import cloud_transcription_env as env
from pipeline import cloud_transcription_import as imports
from pipeline import cloud_transcription_client as clients
from pipeline import cloud_transcription_media as media
from pipeline import cloud_transcription_screen as screen

KIND = 'himr_third_party_first_cloud_transcription_plan'
MAX_RECORDINGS = 10000
RATES = {'assemblyai': 230000, 'revai': 200000}  # microUSD/hour, including diarization
POLICY = {'whole_recordings': True, 'automatic_chunking': False, 'automatic_paid_retries': False,
          'fallback_on_ambiguous_submission': False, 'global_glossary_prompt': False,
          'speaker_labels_are_identities': False, 'publication_authority': False,
          'source_mutation': False, 'existing_campaign_mutation': False,
          'third_party_model_attestation': 'user_confirmed_author_report_not_independently_verified'}
MODULES = ('cloud_transcription.py', 'cloud_transcription_env.py', 'cloud_transcription_import.py',
           'cloud_transcription_client.py', 'cloud_transcription_media.py', 'cloud_transcription_archive.py',
           'cloud_transcription_screen.py', 'cloud_transcription_recovery.py')


class CloudError(RuntimeError):
    pass


def implementation():
    return {**io.implementation(), **{name: hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest()
                                     for name in MODULES}}


def private_root(path):
    path = io.safe.path_value(path)
    current = Path('/')
    for part in path.parts[1:]:
        current /= part
        try:
            current.mkdir(mode=0o700)
        except FileExistsError:
            pass
        if not stat.S_ISDIR(current.lstat().st_mode):
            raise CloudError('workspace path contains a symlink or non-directory')
    with io.paths.retained_directory(path):
        pass
    return path


def route(recording):
    duration = recording['duration_ms']
    if type(duration) is not int or duration <= 0:
        return None, 'invalid_or_unknown_duration'
    if duration <= clients.ASSEMBLYAI_MAX_SECONDS * 1000:
        return 'assemblyai', 'within_assemblyai_whole_recording_limit'
    if duration <= clients.REVAI_MAX_SECONDS * 1000:
        return 'revai', 'exceeds_assemblyai_duration_within_revai_limit'
    return None, 'exceeds_both_providers_whole_recording_limit'


def cost_bound(provider, duration_ms, diarization=True):
    # Never release this reservation automatically, even after a provider error.
    seconds = max(15, math.ceil((duration_ms + media.tolerance_ms(duration_ms)) / 1000))
    rate = RATES[provider] - (20000 if provider == 'assemblyai' and not diarization else 0)
    return (seconds * rate + 3599) // 3600


def job_id(recording):
    return 'cloudjob_' + io.digest({'media': recording['media'], 'recording_id': recording['recording_id'],
                                   'duration_ms': recording['duration_ms']})[:32]


def _optional(path):
    return io.read(io.binding(path)) if io.safe.exists(path) else None


def _admissions(reference):
    if reference is None:
        return {}, 0
    from pipeline import cloud_transcription_recovery as recovery
    catalog = recovery.load_catalog(reference)
    rows = catalog['admissions']
    indexed = {row['recording_id']: row['admission'] for row in rows}
    if len(indexed) != len(rows):
        raise CloudError('cloud recovery repeats a recording')
    return indexed, catalog['prior_reserved_microusd']


def _admitted(reference, recording):
    from pipeline import cloud_transcription_recovery as recovery
    return recovery.load_admission(reference, recording)


def prepare(inventory_ref, third_party_root, state_root, *, ffmpeg_path='/usr/bin/ffmpeg', screen_config_ref=None,
            cloud_admissions_ref=None):
    archive = io.read(inventory_ref)
    if archive.get('kind') != 'himr_cloud_transcription_archive_inventory' or archive.get('schema_version') != 1:
        raise CloudError('expected an explicitly hash-bound cloud archive inventory')
    recordings = archive.get('recordings')
    if not isinstance(recordings, list) or not 1 <= len(recordings) <= MAX_RECORDINGS:
        raise CloudError('archive inventory exceeds recording bound')
    ids = [row['recording_id'] for row in recordings]
    if len(set(ids)) != len(ids):
        raise CloudError('duplicate recording identities')
    root = io.safe.path_value(state_root)
    third_party_root = io.safe.path_value(third_party_root)
    ffmpeg = io.binding(ffmpeg_path)
    if screen_config_ref is not None:
        screen.validate_config(io.read(screen_config_ref))
    admissions, prior_reserved = _admissions(cloud_admissions_ref)
    if set(admissions) - set(ids):
        raise CloudError('recovered cloud sources are outside the selected archive')
    io.protect(root, {'inventory': inventory_ref, 'third_party_root': str(third_party_root),
                      'recordings': recordings, 'ffmpeg': ffmpeg, 'screen_config': screen_config_ref,
                      'cloud_admissions': cloud_admissions_ref})
    private_root(root)
    marker = {'kind': KIND + '_workspace', 'schema_version': 1, 'inventory': inventory_ref,
              'third_party_root': str(third_party_root), 'screen_config': screen_config_ref,
              'cloud_admissions': cloud_admissions_ref,
              'implementation': implementation()}
    with io.locked(root):
        if not io.safe.exists(root / 'workspace.json') and any(p.name != 'execution.lock' for p in root.iterdir()):
            raise CloudError('refusing nonempty unmarked cloud workspace')
        io.put(root / 'workspace.json', marker)
        if io.safe.exists(root / 'plan.json'):
            ref = io.binding(root / 'plan.json')
            load_plan(ref)
            return {'state': 'already_prepared', 'plan': ref, **status(ref)}
        entries = imports.inventory(third_party_root)
        catalogue = io.put(root / 'third-party-inventory.json', {'kind': 'himr_cloud_third_party_inventory',
                            'schema_version': 1, 'entries': entries})
        matched = imports.match_recordings(recordings, entries)
        match_ref = io.put(root / 'third-party-matches.json', {'kind': 'himr_cloud_third_party_matches',
                          'schema_version': 1, 'recording_ids': ids, 'matches': matched})
        io.mkdir(root / 'imports')
        io.mkdir(root / 'jobs')
        io.mkdir(root / 'reservations')
        io.mkdir(root / 'screen-worker')
        rows = []
        for recording, match in zip(recordings, matched, strict=True):
            conflicts = [conflict for conflict in archive.get('identity_conflicts', [])
                         if recording['recording_id'] in conflict.get('recording_ids', [])]
            only_shared_youtube = (recording.get('reasons') == ['source_id_maps_to_multiple_physical_recordings']
                                   and bool(conflicts) and all(c.get('platform') == 'youtube' for c in conflicts))
            row = {'job_id': job_id(recording), 'recording': recording, 'provider': None,
                   'disposition': 'review', 'reason': None, 'import': None,
                   'maximum_cost_microusd': 0, 'match_status': match['status']}
            if recording['state'] == 'no_audio':
                row.update(disposition='no_audio', reason='source_has_no_audio')
            elif match['status'] == 'selected' and (recording['state'] == 'ready' or
                 only_shared_youtube):
                # Batch matcher independently binds the strongest exact source key;
                # a shared lower-specificity ID cannot authorize a wrong import.
                selected = match['selected']
                normalized = imports.normalize(selected, recording['recording_id'], duration_ms=recording['duration_ms'])
                folder = root / 'imports' / row['job_id']
                io.mkdir(folder)
                original = io.put_bytes(folder / 'original.txt', imports.read_raw(selected))
                provenance = io.put(folder / 'provenance.json', {**normalized['provenance'],
                                    'retained_original': original, 'match_evidence': match['matched_keys']})
                transcript = io.put(folder / 'transcript.json', normalized['transcript'])
                row.update(disposition='third_party', reason='exact_identity_and_structure_checked',
                           import_={'original': original, 'provenance': provenance, 'transcript': transcript})
                row['import'] = row.pop('import_')
            elif recording['recording_id'] in admissions:
                recovered = _admitted(admissions[recording['recording_id']], recording)
                row.update(disposition='cloud_import', reason='reused_verified_paid_cloud_result',
                           import_={key: recovered[key] for key in ('transcript', 'completion', 'provenance')})
                row['import'] = row.pop('import_')
            elif match['status'] in {'ambiguous', 'review_required'}:
                row['reason'] = 'third_party_' + match['status']
            elif recording['state'] != 'ready':
                row['reason'] = 'archive_' + recording['state']
            else:
                provider, reason = route(recording)
                row.update(provider=provider, reason=reason)
                if provider:
                    row.update(disposition='cloud', maximum_cost_microusd=cost_bound(provider, recording['duration_ms']))
            rows.append(row)
        plan = {'kind': KIND, 'schema_version': 1, 'state_root': str(root), 'inventory': inventory_ref,
                'third_party_inventory': catalogue, 'matching': match_ref, 'implementation': implementation(),
                'ffmpeg': ffmpeg, 'screen_config': screen_config_ref,
                'cloud_admissions': cloud_admissions_ref, 'prior_reserved_microusd': prior_reserved,
                'rates_microusd_hour': RATES, 'policy': POLICY, 'recordings': rows}
        ref = io.put(root / 'plan.json', plan)
        return {'state': 'prepared_offline', 'plan': ref, **status(ref)}


def load_plan(ref):
    io.safe.file_binding(ref)
    plan = io.read(ref)
    required = {'kind', 'schema_version', 'state_root', 'inventory', 'third_party_inventory', 'matching',
                'implementation', 'ffmpeg', 'screen_config', 'cloud_admissions', 'prior_reserved_microusd',
                'rates_microusd_hour', 'policy', 'recordings'}
    io.safe.exact(plan, required, 'cloud plan')
    if (plan['kind'] != KIND or type(plan['schema_version']) is not int or plan['schema_version'] != 1 or
            plan['policy'] != POLICY or plan['rates_microusd_hour'] != RATES or plan['implementation'] != implementation()):
        raise CloudError('cloud plan implementation or policy changed; retain old paid evidence')
    root = io.safe.path_value(plan['state_root'])
    if plan['screen_config'] is not None:
        screen.validate_config(io.read(plan['screen_config']))
    admissions, prior_reserved = _admissions(plan['cloud_admissions'])
    if plan['prior_reserved_microusd'] != prior_reserved:
        raise CloudError('imported cloud accounting differs from its original paid evidence')
    if Path(ref['path']) != root / 'plan.json':
        raise CloudError('plan must be bound to its original private workspace')
    archive = io.read(plan['inventory'])
    recordings = archive['recordings']
    if not 1 <= len(recordings) <= MAX_RECORDINGS or len(plan['recordings']) != len(recordings):
        raise CloudError('cloud source partition differs')
    io.protect(root, {'inventory': plan['inventory'], 'recordings': recordings,
                      'ffmpeg': plan['ffmpeg'], 'screen_config': plan['screen_config'],
                      'cloud_admissions': plan['cloud_admissions']})
    for row, source in zip(plan['recordings'], recordings, strict=True):
        io.safe.exact(row, {'job_id', 'recording', 'provider', 'disposition', 'reason', 'import',
                            'maximum_cost_microusd', 'match_status'}, 'cloud recording plan')
        if row['recording'] != source or row['job_id'] != job_id(source):
            raise CloudError('cloud recording differs from source inventory')
        if row['disposition'] == 'cloud':
            if source['recording_id'] in admissions:
                raise CloudError('recovered paid cloud recording cannot enter a new submission queue')
            provider, reason = route(source)
            if (source['state'] != 'ready' or row['match_status'] != 'missing' or row['import'] is not None or
                    row['provider'] != provider or row['reason'] != reason or provider is None or
                    row['maximum_cost_microusd'] != cost_bound(provider, source['duration_ms'])):
                raise CloudError('cloud provider route or reservation differs')
        elif row['disposition'] not in {'third_party', 'cloud_import', 'review', 'no_audio'} or row['provider'] is not None or row['maximum_cost_microusd'] != 0:
            raise CloudError('invalid offline disposition')
        if row['disposition'] == 'third_party':
            if row['match_status'] != 'selected' or not isinstance(row['import'], dict):
                raise CloudError('third-party selection lacks verified import')
            for artifact in row['import'].values():
                io.safe.file_binding(artifact)
                if Path(artifact['path']).parent != root / 'imports' / row['job_id']:
                    raise CloudError('import artifact escaped its private recording folder')
        if row['disposition'] == 'cloud_import':
            admission = admissions.get(source['recording_id'])
            if admission is None:
                raise CloudError('cloud import is not admitted by the source catalog')
            recovered = _admitted(admission, source)
            if row['import'] != {key: recovered[key] for key in ('transcript', 'completion', 'provenance')}:
                raise CloudError('cloud import artifact or original paid provenance differs')
    marker = _optional(root / 'workspace.json')
    if (not marker or marker.get('inventory') != plan['inventory'] or marker.get('implementation') != plan['implementation']
            or marker.get('screen_config') != plan['screen_config']):
        raise CloudError('cloud workspace binding differs')
    if marker.get('cloud_admissions') != plan['cloud_admissions']:
        raise CloudError('workspace cloud recovery binding differs')
    return plan


def _folder(plan, row):
    return Path(plan['state_root']) / 'jobs' / row['job_id']


def _screen_decision(plan, row):
    path = _folder(plan, row) / 'screen.json'
    if not io.safe.exists(path):
        return None, None
    ref = io.binding(path)
    value = io.read(ref)
    if plan['screen_config'] is None:
        raise CloudError('screen decision has no configured screening contract')
    screen.validate_decision(value, row['recording'], plan['screen_config'])
    return ref, value


def _intent(ref, row, audio, screen_ref, decision):
    return {'kind': 'himr_cloud_paid_intent', 'schema_version': 1, 'plan': ref,
            'job_id': row['job_id'], 'recording_id': row['recording']['recording_id'],
            'provider': row['provider'], 'audio': audio,
            'screen_decision': screen_ref, 'diarization': decision['diarization'],
            'request_metadata': row['job_id'] + '_' + io.digest({'audio': audio, 'screen': screen_ref})[:24],
            'maximum_cost_microusd': cost_bound(row['provider'], row['recording']['duration_ms'], decision['diarization'])}


def _transcript_document(row, audio, raw_ref, terminal_ref, terminal, raw, screen_ref, decision):
    normalized = clients.normalize_result(row['provider'], raw,
                   expected_duration_seconds=audio['duration_ms'] / 1000, job=terminal,
                   diarization=decision['diarization'])
    return {'kind': 'himr_cloud_recording_transcript', 'schema_version': 1,
           'job_id': row['job_id'], 'recording_id': row['recording']['recording_id'],
           'source_media': row['recording']['media'], 'status': 'completed',
           'provider_job_id': terminal['id'], 'raw_result': raw_ref, 'provider_job': terminal_ref,
           'screen_decision': screen_ref, 'audio': audio, 'whole_recording_submitted': True,
           'normalizer_implementation_sha256': hashlib.sha256(Path(clients.__file__).read_bytes()).hexdigest(),
           'machine_generated': True, 'full_media_coverage_verified': False, 'human_reviewed': False,
           'verified_quotation': False, 'speaker_identity_inferred': False, 'publication_authority': False,
           **normalized}


def _completion(row, audio, raw_ref, terminal_ref, transcript_ref, screen_ref):
    return {'kind': 'himr_cloud_transcription_completion', 'schema_version': 1,
            'job_id': row['job_id'], 'audio': audio, 'raw_result': raw_ref,
            'provider_job': terminal_ref, 'transcript': transcript_ref, 'screen_decision': screen_ref}


def _collection_review(ref, row, folder, raw_ref, terminal_ref, screen_ref):
    return {'kind': 'himr_cloud_collection_review', 'schema_version': 1,
            'plan': ref, 'job_id': row['job_id'], 'recording_id': row['recording']['recording_id'],
            'provider': row['provider'], 'source_media': row['recording']['media'],
            'intent': io.binding(folder / 'intent.json'), 'audio_receipt': io.binding(folder / 'audio.json'),
            'raw_result': raw_ref, 'provider_job': terminal_ref, 'screen_decision': screen_ref,
            'normalizer_implementation_sha256': hashlib.sha256(Path(clients.__file__).read_bytes()).hexdigest(),
            'reason': 'provider_result_normalization_rejected', 'automatic_paid_retry': False}


def inspect_job(plan, ref, row):
    folder = _folder(plan, row)
    reservation = _optional(Path(plan['state_root']) / 'reservations' / (row['job_id'] + '.json'))
    if not io.safe.exists(folder):
        if reservation:
            raise CloudError('reserved cloud job workspace is missing; never repeat its paid submission')
        return {'state': 'waiting_screen', 'reservation': 0, 'receipt': None, 'screen_state': None, 'diarization': None}
    with io.paths.retained_directory(folder):
        job = _optional(folder / 'job.json')
        if job is None and not reservation:
            # Another stage may be between atomic mkdir and its immutable marker.
            return {'state': 'waiting_screen', 'reservation': 0, 'receipt': None, 'screen_state': None, 'diarization': None}
        if job != {'plan': ref, 'recording': row}:
            raise CloudError('cloud job binding differs')
        intent = _optional(folder / 'intent.json')
        receipt = _optional(folder / 'submission.json') or _optional(folder / 'reconciled.json')
        complete = _optional(folder / 'completion.json')
        terminal = _optional(folder / 'terminal-job.json')
        review = _optional(folder / 'collection-review.json')
        screen_ref, decision = _screen_decision(plan, row)
        screen_info = {'screen_state': decision['state'] if decision else None,
                       'diarization': decision['diarization'] if decision else None}
        if not intent:
            if receipt or complete or terminal or review:
                raise CloudError('paid evidence exists without a durable intent')
            if reservation:
                if decision is None:
                    raise CloudError('reserved paid request lost its screen proof')
                prepared = _optional(folder / 'audio.json')
                if not prepared or reservation != _intent(ref, row, prepared['audio'], screen_ref, decision):
                    raise CloudError('orphan paid reservation differs; review required')
                return {'state': 'reconciliation_required', 'reservation': reservation['maximum_cost_microusd'], 'receipt': None, **screen_info}
            return {'state': 'ready' if decision else 'waiting_screen', 'reservation': 0, 'receipt': None, **screen_info}
        prepared = _optional(folder / 'audio.json')
        if not prepared or prepared['source'] != row['recording']['media'] or prepared['ffmpeg'] != plan['ffmpeg']:
            raise CloudError('paid job lacks bound audio preparation')
        if decision is None or intent != reservation or intent != _intent(ref, row, prepared['audio'], screen_ref, decision):
            raise CloudError('paid intent differs from its sealed route or reservation')
        state = 'reconciliation_required' if receipt is None else 'pending'
        if receipt:
            checked = clients.validate_job(row['provider'], receipt)
            if checked['status'] in {'error', 'failed'}:
                state = 'failed'
        if terminal:
            remote = clients.validate_job(row['provider'], terminal, expected_job_id=receipt['id'] if receipt else None)
            state = 'failed' if remote['status'] in {'error', 'failed'} else 'collecting'
        if review:
            if complete or not receipt or not terminal or terminal['status'] not in {'completed', 'transcribed'}:
                raise CloudError('collection review lacks exclusive successful provider terminal evidence')
            terminal_ref = io.binding(folder / 'terminal-job.json')
            raw_ref = io.binding(folder / ('provider-transcript.json' if row['provider'] == 'revai' else 'terminal-job.json'))
            if review != _collection_review(ref, row, folder, raw_ref, terminal_ref, screen_ref):
                raise CloudError('collection review differs from its retained paid evidence')
            raw = io.read(raw_ref)
            try:
                clients.normalize_result(row['provider'], raw, expected_duration_seconds=prepared['audio']['duration_ms'] / 1000,
                                         job=terminal, diarization=decision['diarization'])
            except clients.CloudClientError:
                state = 'needs_review'
            else:
                raise CloudError('collection review rejection no longer replays')
        if complete:
            if not receipt or not terminal or terminal['status'] not in {'completed', 'transcribed'}:
                raise CloudError('cloud completion lacks provider terminal evidence')
            terminal_ref = io.binding(folder / 'terminal-job.json')
            raw_ref = io.binding(folder / ('provider-transcript.json' if row['provider'] == 'revai' else 'terminal-job.json'))
            transcript_ref = io.binding(folder / 'transcript.json')
            if complete != _completion(row, prepared['audio'], raw_ref, terminal_ref, transcript_ref, screen_ref):
                raise CloudError('cloud completion artifact bindings differ')
            expected = _transcript_document(row, prepared['audio'], raw_ref, terminal_ref,
                                           terminal, io.read(raw_ref), screen_ref, decision)
            if io.read(transcript_ref) != expected:
                raise CloudError('normalized transcript differs from retained provider result')
            state = 'completed'
        return {'state': state, 'reservation': intent['maximum_cost_microusd'], 'receipt': receipt, **screen_info}


def _states(plan, ref):
    allowed = {row['job_id'] + '.json' for row in plan['recordings'] if row['disposition'] == 'cloud'}
    with io.paths.retained_directory(Path(plan['state_root']) / 'reservations') as directory:
        with os.scandir(directory) as entries:
            if any(entry.name not in allowed and not entry.name.startswith('.summary-') for entry in entries):
                raise CloudError('unexpected paid reservation evidence')
    values = {row['job_id']: inspect_job(plan, ref, row) for row in plan['recordings'] if row['disposition'] == 'cloud'}
    total = sum(value['reservation'] for value in values.values())
    limit = _optional(Path(plan['state_root']) / 'spending-limit.json')
    if total and limit is None:
        raise CloudError('paid reservations lost their spending-limit proof; review required')
    if limit is not None:
        io.safe.exact(limit, {'kind', 'plan', 'maximum_microusd', 'scope', 'automatic_hold_release'}, 'spending limit')
        io.safe.integer(limit['maximum_microusd'], 1, 10**12, 'spending limit')
        if (limit['kind'] != 'himr_cloud_spending_limit' or limit['plan'] != ref or
                limit['scope'] != 'this_workspace_not_provider_account' or limit['automatic_hold_release'] is not False or
                total + plan['prior_reserved_microusd'] > limit['maximum_microusd']):
            raise CloudError('paid reservation accounting differs from its spending limit')
    return values


def run_screen(ref, *, max_jobs=1, max_runtime_seconds=3600, stopping=lambda: False):
    """Independent local stage; deliberately does not take the cloud HTTP lock."""
    io.safe.integer(max_jobs, 1, MAX_RECORDINGS, 'screen job limit')
    io.safe.integer(max_runtime_seconds, 1, 86400, 'screen runtime')
    plan = load_plan(ref)
    if plan['screen_config'] is None:
        raise CloudError('prepare a plan with a hash-bound screening configuration first')
    root = Path(plan['state_root'])
    deadline, completed, decisions = time.monotonic() + max_runtime_seconds, 0, Counter()
    with io.locked(root / 'screen-worker'):
        for row in plan['recordings']:
            if stopping() or time.monotonic() >= deadline or completed >= max_jobs:
                break
            if row['disposition'] != 'cloud':
                continue
            folder = _folder(plan, row)
            if io.safe.exists(folder / 'screen.json'):
                _screen_decision(plan, row)
                continue
            io.mkdir(folder)
            io.put(folder / 'job.json', {'plan': ref, 'recording': row})
            io.mkdir(folder / 'screen-work')
            decision = screen.screen_one(row['recording'], folder / 'screen-work', plan['screen_config'])
            screen.validate_decision(decision, row['recording'], plan['screen_config'])
            io.put(folder / 'screen.json', decision)
            decisions[decision['state']] += 1
            completed += 1
    return {'state': 'screen_paused' if stopping() else 'screen_cycle_completed',
            'new_screens': completed, 'decisions': dict(decisions), 'new_paid_requests': 0}


def status(ref):
    plan = load_plan(ref)
    states = _states(plan, ref)
    counts = dict(Counter(row['disposition'] for row in plan['recordings']))
    counts.update({'cloud_' + key: value for key, value in Counter(row['state'] for row in states.values()).items()})
    limit = _optional(Path(plan['state_root']) / 'spending-limit.json')
    return {'kind': 'himr_cloud_transcription_status', 'counts': counts,
            'selected_recordings': len(plan['recordings']),
            'planned_cloud_cost_bound_microusd': sum(row['maximum_cost_microusd'] for row in plan['recordings']),
            'reserved_microusd': plan['prior_reserved_microusd'] + sum(value['reservation'] for value in states.values()),
            'prior_cloud_reservation_microusd': plan['prior_reserved_microusd'],
            'spending_limit_microusd': None if limit is None else limit['maximum_microusd'],
            'provider_routes': dict(Counter(row['provider'] for row in plan['recordings'] if row['provider'])),
            'screen_decisions': dict(Counter(value['screen_state'] for value in states.values() if value['screen_state'])),
            'diarization_enabled': sum(value['diarization'] is True for value in states.values()),
            'diarization_disabled': sum(value['diarization'] is False for value in states.values()),
            'automatic_paid_retries': False, 'source_mutation': False, 'new_paid_requests': 0}


def client_for(provider, env_file=None):
    key = env.api_key(provider, env_file=env_file)
    if key is None:
        raise CloudError('missing ' + ('ASSEMBLYAI_API_KEY' if provider == 'assemblyai' else 'REVAI_ACCESS_TOKEN') + ' in environment or private .env')
    return (clients.AssemblyAIClient if provider == 'assemblyai' else clients.RevAIClient)(key)


def _collect(plan, ref, row, client):
    folder = _folder(plan, row)
    current = inspect_job(plan, ref, row)
    if current['state'] in {'ready', 'waiting_screen', 'reconciliation_required', 'failed', 'needs_review', 'completed'}:
        return 0
    receipt = current['receipt']
    terminal = _optional(folder / 'terminal-job.json')
    if terminal is None:
        remote = client.poll(receipt['id'])
        checked = clients.validate_job(row['provider'], remote, expected_job_id=receipt['id'])
        if checked['status'] not in {'completed', 'transcribed', 'error', 'failed'}:
            return 0
        io.put(folder / 'terminal-job.json', remote)
        terminal = remote
    if terminal['status'] in {'failed', 'error'}:
        return 0
    raw = terminal
    if row['provider'] == 'revai':
        raw = _optional(folder / 'provider-transcript.json')
        if raw is None:
            raw = client.transcript(receipt['id'])
            io.put(folder / 'provider-transcript.json', raw)
    raw_ref = io.binding(folder / ('provider-transcript.json' if row['provider'] == 'revai' else 'terminal-job.json'))
    audio = io.read(io.binding(folder / 'audio.json'))['audio']
    terminal_ref = io.binding(folder / 'terminal-job.json')
    screen_ref, decision = _screen_decision(plan, row)
    try:
        doc = _transcript_document(row, audio, raw_ref, terminal_ref, terminal, raw, screen_ref, decision)
    except clients.CloudClientError:
        # Only a fully captured provider result's normalization rejection is a
        # per-record hold. Source/proof/storage errors are not swallowed here.
        io.put(folder / 'collection-review.json', _collection_review(ref, row, folder, raw_ref, terminal_ref, screen_ref))
        return 0
    output = io.put(folder / 'transcript.json', doc)
    completion = _completion(row, audio, raw_ref, terminal_ref, output, screen_ref)
    io.put(folder / 'completion.json', completion)
    return media.prune_completed(folder, completion)


def cycle(ref, *, allow_paid_api=False, budget_microusd=None, max_new_jobs=1, max_active=2,
          env_file=None, client_factory=None, prepare_audio=None, stopping=lambda: False):
    io.safe.integer(max_new_jobs, 0, 32, 'new-job limit')
    io.safe.integer(max_active, 1, 8, 'active-job limit')
    if budget_microusd is not None:
        io.safe.integer(budget_microusd, 1, 10**12, 'spending limit')
    plan = load_plan(ref)
    root = Path(plan['state_root'])
    factory = client_factory or (lambda provider: client_for(provider, env_file))
    audio_builder = prepare_audio or media.prepare
    connected = {}
    def client(provider):
        if provider not in connected:
            connected[provider] = factory(provider)
        return connected[provider]
    submitted, pruned = 0, 0
    with io.locked(root):
        states = _states(plan, ref)  # Never recreate lost budget proof over paid history.
        if allow_paid_api:
            if budget_microusd is None:
                raise CloudError('new transcription submissions require an explicit --budget-usd')
            if budget_microusd < plan['prior_reserved_microusd']:
                raise CloudError('spending limit is below previously reserved cloud work')
            io.put(root / 'spending-limit.json', {'kind': 'himr_cloud_spending_limit',
                     'plan': ref, 'maximum_microusd': budget_microusd,
                     'scope': 'this_workspace_not_provider_account', 'automatic_hold_release': False})
        for row in plan['recordings']:
            if row['disposition'] != 'cloud' or stopping():
                continue
            if states[row['job_id']]['state'] in {'pending', 'collecting'}:
                pruned += _collect(plan, ref, row, client(row['provider']))
        states = _states(plan, ref)
        if any(state['state'] == 'reconciliation_required' for state in states.values()):
            return {'state': 'reconciliation_required', **status(ref), 'new_paid_requests': submitted,
                    'pruned_upload_audio_bytes': pruned}
        budget_paused = False
        for row in plan['recordings']:
            if stopping() or not allow_paid_api or submitted >= max_new_jobs:
                break
            if row['disposition'] != 'cloud' or states[row['job_id']]['state'] != 'ready':
                continue
            active = sum(value['state'] in {'pending', 'collecting'} for value in states.values())
            if active >= max_active:
                break
            reserved = plan['prior_reserved_microusd'] + sum(value['reservation'] for value in states.values())
            screen_ref, decision = _screen_decision(plan, row)
            amount = cost_bound(row['provider'], row['recording']['duration_ms'], decision['diarization'])
            if reserved + amount > budget_microusd:
                budget_paused = True
                continue
            selected_client = client(row['provider'])  # Fail missing-key checks before preprocessing.
            folder = _folder(plan, row)
            io.mkdir(folder)
            io.put(folder / 'job.json', {'plan': ref, 'recording': row})
            audio = audio_builder(row['recording'], folder, plan['ffmpeg'])
            clients.validate_duration(row['provider'], audio['duration_ms'] / 1000)
            if audio['byte_count'] > (clients.ASSEMBLYAI_MAX_UPLOAD_BYTES if row['provider'] == 'assemblyai' else clients.REVAI_MAX_UPLOAD_BYTES):
                raise CloudError('prepared whole recording exceeds its sealed upload route')
            if stopping():
                break
            if row['provider'] == 'assemblyai':
                upload = _optional(folder / 'upload.json')
                if upload is None:
                    try:
                        upload = selected_client.upload(audio['path'], expected_sha256=audio['sha256'])
                    except clients.CloudClientError as error:
                        if getattr(error, 'response', None) is not None:
                            io.put(folder / 'upload-untrusted-response.json', error.response)
                        raise
                    io.put(folder / 'upload.json', upload)
                upload_url = clients.validate_upload_url(upload.get('upload_url'))
            if stopping():
                break
            intent = _intent(ref, row, audio, screen_ref, decision)
            io.put(root / 'reservations' / (row['job_id'] + '.json'), intent)
            io.put(folder / 'intent.json', intent)  # Must be durable before the paid POST.
            try:
                receipt = (selected_client.submit(upload_url, diarization=decision['diarization']) if row['provider'] == 'assemblyai' else
                           selected_client.submit_file(audio['path'], expected_sha256=audio['sha256'],
                                                       metadata=intent['request_metadata'], diarization=decision['diarization']))
            except clients.CloudClientError as error:
                if getattr(error, 'response', None) is not None:
                    io.put(folder / 'submission-untrusted-response.json', error.response)
                raise
            io.put(folder / 'submission.json', receipt)  # Retain even if subsequent validation fails.
            clients.validate_job(row['provider'], receipt)
            submitted += 1
            states[row['job_id']] = inspect_job(plan, ref, row)
        if stopping():
            state = 'paused'
        elif budget_paused:
            state = 'budget_paused'
        elif all(value['state'] in {'completed', 'failed', 'needs_review'} for value in states.values()):
            held = (any(row['disposition'] == 'review' for row in plan['recordings'])
                    or any(value['state'] in {'failed', 'needs_review'} for value in states.values()))
            state = 'cloud_complete_with_review_holds' if held else 'completed'
        else:
            state = 'running' if allow_paid_api else 'collected_existing_only'
        return {'state': state, **status(ref), 'new_paid_requests': submitted, 'pruned_upload_audio_bytes': pruned}


def reconcile(ref, job, remote_id, *, env_file=None, client=None):
    plan = load_plan(ref)
    row = next((r for r in plan['recordings'] if r['job_id'] == job and r['disposition'] == 'cloud'), None)
    if row is None:
        raise CloudError('unknown cloud recording job')
    with io.locked(Path(plan['state_root'])):
        current = inspect_job(plan, ref, row)
        if current['state'] != 'reconciliation_required':
            raise CloudError('only a durable ambiguous paid intent may be reconciled')
        folder = _folder(plan, row)
        intent = io.read(io.binding(folder / 'intent.json'))
        client = client or client_for(row['provider'], env_file)
        remote = client.poll(remote_id)
        clients.validate_job(row['provider'], remote, expected_job_id=remote_id)
        if row['provider'] == 'assemblyai':
            upload = io.read(io.binding(folder / 'upload.json'))
            if remote.get('audio_url') != upload['upload_url'] or remote.get('speaker_labels') is not intent['diarization'] or remote.get('language_code') != 'en':
                raise CloudError('remote AssemblyAI job does not bind the exact uploaded audio and options')
        elif remote.get('metadata') != intent['request_metadata']:
            raise CloudError('remote Rev AI job does not bind the exact intent fingerprint')
        io.put(folder / 'reconciled.json', remote)
        return {'state': 'reconciled_existing_job', 'job_id': job, 'new_paid_requests': 0}


def export(ref):
    plan = load_plan(ref)
    records = []
    for row in plan['recordings']:
        source = row['recording']
        artifact = row['import']['transcript'] if row['disposition'] == 'third_party' else None
        format_, completion_ref = ('third_party' if artifact else None), None
        if row['disposition'] == 'cloud_import':
            artifact, completion_ref, format_ = row['import']['transcript'], row['import']['completion'], 'cloud'
        if row['disposition'] == 'cloud' and inspect_job(plan, ref, row)['state'] == 'completed':
            artifact = io.read(io.binding(_folder(plan, row) / 'completion.json'))['transcript']
            format_, completion_ref = 'cloud', io.binding(_folder(plan, row) / 'completion.json')
        if artifact is not None:
            io.read(artifact)
            records.append({'recording_id': source['recording_id'], 'title': source['title'],
                            'date_metadata': source['date'], 'format': format_, 'transcript': artifact,
                            'completion': completion_ref})
    return {'kind': 'himr_preferred_transcript_selection', 'schema_version': 1, 'plan': ref,
            'records': records, 'source_preference': ['verified_third_party', 'assemblyai_universal_3_5_pro', 'revai'],
            'automatic_summary_restart': False, 'publication_authority': False}


def usd(value):
    try:
        amount = Decimal(value) * 1000000
        if not amount.is_finite() or amount != amount.to_integral_value() or not 0 < amount <= 10**12:
            raise ValueError
        return int(amount)
    except (InvalidOperation, ValueError):
        raise argparse.ArgumentTypeError('budget must be a positive USD value with at most six decimals') from None


@contextmanager
def pause_signal():
    stopped = [False]
    previous = signal.signal(signal.SIGTERM, lambda *_: stopped.__setitem__(0, True))
    try:
        yield lambda: stopped[0]
    finally:
        signal.signal(signal.SIGTERM, previous)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest='command', required=True)
    prep = modes.add_parser('prepare')
    prep.add_argument('--inventory', required=True)
    prep.add_argument('--expected-sha256', required=True)
    prep.add_argument('--third-party-root', required=True)
    prep.add_argument('--state-root', required=True)
    prep.add_argument('--ffmpeg', default='/usr/bin/ffmpeg')
    prep.add_argument('--screen-config')
    prep.add_argument('--screen-config-sha256')
    prep.add_argument('--cloud-admissions')
    prep.add_argument('--cloud-admissions-sha256')
    for name in ('status', 'screen', 'cycle', 'run', 'reconcile', 'export'):
        sub = modes.add_parser(name)
        sub.add_argument('--plan', required=True)
        sub.add_argument('--expected-sha256', required=True)
        if name in {'cycle', 'run', 'reconcile'}:
            sub.add_argument('--env-file')
        if name in {'cycle', 'run'}:
            sub.add_argument('--allow-paid-api', action='store_true')
            sub.add_argument('--budget-usd', type=usd)
            sub.add_argument('--max-new-jobs', type=int, default=1)
            sub.add_argument('--max-active', type=int, default=2)
        if name == 'run':
            sub.add_argument('--max-runtime-seconds', type=int, default=3600)
            sub.add_argument('--poll-seconds', type=int, default=60)
        if name == 'screen':
            sub.add_argument('--max-jobs', type=int, default=1)
            sub.add_argument('--max-runtime-seconds', type=int, default=3600)
        if name == 'reconcile':
            sub.add_argument('--job-id', required=True)
            sub.add_argument('--remote-id', required=True)
        if name == 'export':
            sub.add_argument('--output', required=True)
    args = parser.parse_args(argv)
    if args.command == 'prepare':
        if bool(args.screen_config) != bool(args.screen_config_sha256):
            raise CloudError('screen configuration requires both path and exact SHA-256')
        if bool(args.cloud_admissions) != bool(args.cloud_admissions_sha256):
            raise CloudError('cloud admissions require both path and exact SHA-256')
        result = prepare({'path': args.inventory, 'sha256': args.expected_sha256},
                         args.third_party_root, args.state_root, ffmpeg_path=args.ffmpeg,
                         screen_config_ref=None if not args.screen_config else
                         {'path': args.screen_config, 'sha256': args.screen_config_sha256},
                         cloud_admissions_ref=None if not args.cloud_admissions else
                         {'path': args.cloud_admissions, 'sha256': args.cloud_admissions_sha256})
    else:
        ref = {'path': args.plan, 'sha256': args.expected_sha256}
        if args.command == 'status':
            result = status(ref)
        elif args.command == 'screen':
            with pause_signal() as stopping:
                result = run_screen(ref, max_jobs=args.max_jobs, max_runtime_seconds=args.max_runtime_seconds,
                                    stopping=stopping)
        elif args.command == 'export':
            value = export(ref)
            target = io.safe.path_value(args.output)
            plan = load_plan(ref)
            if target.parent != Path(plan['state_root']):
                raise CloudError('selection export must be a fresh file in this private workspace')
            with io.locked(target.parent):
                result = {'state': 'exported_offline', 'output': io.put(target, value), 'records': len(value['records'])}
        elif args.command == 'reconcile':
            result = reconcile(ref, args.job_id, args.remote_id, env_file=args.env_file)
        else:
            if args.command == 'run':
                io.safe.integer(args.max_runtime_seconds, 1, 14 * 86400, 'runtime bound')
                io.safe.integer(args.poll_seconds, 15, 3600, 'poll interval')
            with pause_signal() as stopping:
                deadline = time.monotonic() + (args.max_runtime_seconds if args.command == 'run' else 0)
                while True:
                    result = cycle(ref, allow_paid_api=args.allow_paid_api, budget_microusd=args.budget_usd,
                                   max_new_jobs=args.max_new_jobs, max_active=args.max_active,
                                   env_file=args.env_file, stopping=stopping)
                    print(io.canonical(result).decode().strip(), flush=True)
                    if args.command != 'run' or result['state'] != 'running' or stopping() or time.monotonic() >= deadline:
                        break
                    end = min(deadline, time.monotonic() + args.poll_seconds)
                    while not stopping() and time.monotonic() < end:
                        time.sleep(max(0, min(1, end - time.monotonic())))
            return 0 if result['state'] not in {'needs_review', 'reconciliation_required'} else 2
    print(io.canonical(result).decode().strip(), flush=True)
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (RuntimeError, OSError, ValueError, KeyError, TypeError) as error:
        print('Cloud transcription stopped: ' + str(error), file=sys.stderr)
        raise SystemExit(2)
