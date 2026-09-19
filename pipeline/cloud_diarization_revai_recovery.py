"""One explicitly approved Rev AI replacement for a never-submitted upload.

The original plan/runtime stay sealed. A bound sidecar authorizes only this
provider change; the original job lock and budget ledger prevent duplicates.
Rev multipart upload IS the paid request and must never be automatically retried.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import sys
import time

KIND = 'himr_single_recording_revai_recovery'
JOB = 'cloudjob_002dcd70e78a489b1d3bdd0fc126ffff'
PLAN_SHA = '265fb7ecbf9e7d53d257faeb3f2a67e1bc4d465da5bf575bc6923ea782b82b0a'
MEDIA_SHA = 'dd042182cf993c80f1760527894ff77877bf1d57ea470be57c35344d6e560b48'
FLAC_SHA = '1e43877b6b87498134ae1c6b1a38cc6cc6a95c83c918582c2a97f5a12a20c428'
STOP = False


class RecoveryError(RuntimeError):
    pass


def read_bound(ref):
    raw = Path(ref['path']).read_bytes()
    if hashlib.sha256(raw).hexdigest() != ref['sha256']:
        raise RecoveryError('recovery binding changed')
    return json.loads(raw)


def load_runtime(runtime, plan_ref):
    if plan_ref['sha256'] != PLAN_SHA:
        raise RecoveryError('recovery is restricted to the approved original batch')
    runtime = Path(runtime).resolve()
    sys.path.insert(0, str(runtime))
    from pipeline import cloud_diarization_batch as batch
    if Path(batch.__file__).resolve().parent.parent != runtime:
        raise RecoveryError('wrong batch runtime imported')
    return batch, batch.load_plan(plan_ref)


def selection(plan, batch):
    row = next(r for r in plan['recordings'] if r['job_id'] == JOB)
    if (row['provider'] != 'assemblyai' or row['language'] != 'auto'
            or row['diarization'] is not True
            or row['recording']['media']['sha256'] != MEDIA_SHA):
        raise RecoveryError('provider replacement differs from the approved recording')
    duration = row['recording']['duration_ms']
    batch.clients.validate_duration('revai', duration / 1000)
    seconds = max(15, math.ceil((duration + batch.media.tolerance_ms(duration)) / 1000))
    cost = (seconds * 200_000 + 3599) // 3600
    if cost > row['maximum_cost_microusd']:
        raise RecoveryError('replacement must fit the original job allowance')
    if plan['budget']['combined_ceiling_microusd'] + 10_000 > batch.TOTAL_CAP:
        raise RecoveryError('combined allocation including retained pilot exceeds cap')
    return row, cost


def submission_state(folder, reservations, expected, batch):
    """A reservation alone is enough to prohibit another paid POST."""
    io = batch.io
    receipt_path = folder / 'submission.json'
    intent_path = folder / 'intent.json'
    reservation_path = reservations / (JOB + '.json')
    if receipt_path.exists():
        if (not intent_path.exists() or not reservation_path.exists()
                or io.read(io.binding(intent_path)) != expected
                or io.read(io.binding(reservation_path)) != expected):
            raise RecoveryError('paid receipt does not match this approved replacement')
        return 'collect'
    if (reservation_path.exists()
            or any((folder / name).exists() for name in batch.PAID_FILES)):
        raise RecoveryError('existing paid evidence requires reconciliation; no repeat POST')
    return 'submit'


def check_hold(folder, original, io):
    if (not (folder / 'hold.json').exists()
            or io.read(io.binding(folder / 'hold.json')) != original):
        raise RecoveryError('original AssemblyAI upload hold changed')


def prepare(runtime, plan_ref, output):
    batch, plan = load_runtime(runtime, plan_ref)
    io = batch.io
    row, cost = selection(plan, batch)
    folder = Path(plan['state_root']) / 'jobs' / JOB
    root = Path(output).resolve()
    io.protect(root, {'plan': plan_ref, 'job': str(folder), 'runtime': runtime})
    io.mkdir(root)
    with io.locked(root), io.locked(folder):
        if (root / 'manifest.json').exists():
            raise RecoveryError('recovery already prepared; resume its pinned runner')
        submission_state(folder, Path(plan['state_root']) / 'reservations', None, batch)
        batch.assert_no_prior_paid([row], plan['prior_plans'])
        original = io.read(io.binding(folder / 'hold.json'))
        if (original['plan'] != plan_ref or original['job_id'] != JOB
                or original['reason'] != 'cloud request transport failed'
                or (folder / 'upload.json').exists()):
            raise RecoveryError('expected an upload failure before provider acceptance')
        hold_ref = io.put(root / 'assemblyai-upload-hold.json', original)
        runner_ref = io.put_bytes(root / 'runner.py', Path(__file__).read_bytes())
        manifest = dict(kind=KIND, schema_version=1, state_root=str(root), job_id=JOB,
            plan=plan_ref, runtime=str(Path(runtime).resolve()), implementation=runner_ref,
            runtime_release=io.binding(Path(runtime) / 'runtime-release.json'),
            job=io.binding(folder / 'job.json'), screen=io.binding(folder / 'screen.json'),
            audio_receipt=io.binding(folder / 'audio.json'),
            flac_receipt=io.binding(folder / 'flac-transport.json'), original_hold=hold_ref,
            approved_provider='revai', approved_model='machine', language='en', diarization=True,
            maximum_cost_microusd=cost, budget=plan['budget'], retained_pilot_allowance_microusd=10_000,
            approval='User: Yes, do that. In response to switching only dinner and owl cafe with bloodbucket to Rev AI.',
            whole_recording=True, segment_timestamps=True, canonical_word_timestamps=False,
            paid_retries_authorized=False, other_recordings_changed=False,
            new_gemini_requests=False, original_plan_modified=False)
        return io.put(root / 'manifest.json', manifest)


def bootstrap(ref):
    manifest = read_bound(ref)
    if (manifest.get('kind') != KIND or manifest.get('job_id') != JOB
            or Path(ref['path']) != Path(manifest['state_root']) / 'manifest.json'
            or manifest.get('approved_provider') != 'revai'
            or manifest.get('approved_model') != 'machine' or manifest.get('language') != 'en'
            or manifest.get('diarization') is not True
            or manifest.get('paid_retries_authorized') is not False
            or manifest.get('whole_recording') is not True):
        raise RecoveryError('recovery scope or options changed')
    implementation = manifest['implementation']
    if (Path(implementation['path']).resolve() != Path(__file__).resolve()
            or hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != implementation['sha256']):
        raise RecoveryError('pinned recovery implementation changed')
    read_bound(manifest['runtime_release'])
    batch, plan = load_runtime(manifest['runtime'], manifest['plan'])
    row, cost = selection(plan, batch)
    if manifest['maximum_cost_microusd'] != cost or manifest['budget'] != plan['budget']:
        raise RecoveryError('recovery budget changed')
    return manifest, plan, row, batch


def retained_audio(manifest, plan, row, batch):
    io = batch.io
    folder = Path(plan['state_root']) / 'jobs' / JOB
    if io.read(manifest['job']) != dict(plan=manifest['plan'], recording=row):
        raise RecoveryError('original job differs')
    screen = io.read(manifest['screen'])
    saved, flac = io.read(manifest['audio_receipt']), io.read(manifest['flac_receipt'])
    audio, transport = saved['audio'], flac['transport']
    if (screen['plan'] != manifest['plan'] or screen['diarization'] is not True
            or screen['recording_id'] != row['recording']['recording_id']
            or saved['source'] != row['recording']['media'] or flac['audio'] != audio
            or saved['ffmpeg'] != plan['ffmpeg'] or flac['ffmpeg'] != plan['ffmpeg']
            or flac['lossless_verified'] is not True
            or Path(transport['path']) != folder / 'audio.flac' or transport['sha256'] != FLAC_SHA
            or not 0 < transport['byte_count'] <= batch.clients.REVAI_MAX_UPLOAD_BYTES
            or abs(audio['duration_ms'] - row['recording']['duration_ms']) > batch.media.tolerance_ms(row['recording']['duration_ms'])):
        raise RecoveryError('retained whole-file lossless audio differs')
    batch.clients.validate_duration('revai', audio['duration_ms'] / 1000)
    return audio, transport


def paid_intent(ref, manifest, row, audio, transport, batch):
    metadata = JOB + '_revai_' + batch.io.digest(dict(recovery=ref, audio=audio))[:16]
    return dict(kind=KIND + '_paid_intent', plan=manifest['plan'], provider_fallback=ref,
        job_id=JOB, provider='revai', recording_id=row['recording']['recording_id'],
        audio=audio, transport=transport, screen_decision=manifest['screen'],
        diarization=True, language='en', request_metadata=metadata,
        maximum_cost_microusd=manifest['maximum_cost_microusd'])


def observed_client(clients, key, progress):
    class Body(clients._StreamingBody):
        def __iter__(self):
            started = time.monotonic()
            last = 0.0
            sent = 0
            for block in super().__iter__():
                if STOP:
                    raise clients.CloudClientError('paid upload interrupted; reconcile before any retry', ambiguous=True)
                yield block
                sent += len(block)
                now = time.monotonic()
                if now - last >= 20 or sent == self.length:
                    last = now
                    progress('submitting', bytes_sent=sent, total_bytes=self.length,
                        elapsed_seconds=round(now - started, 1), provider_acceptance_confirmed=False)

    class Client(clients.RevAIClient):
        def _request(self, method, path, **kwargs):
            data = kwargs.get('data')
            if isinstance(data, clients._StreamingBody):
                kwargs['data'] = Body(data.descriptor, data.before, data.expected_sha256,
                    data.prefix, data.suffix, path=data.path)
            return super()._request(method, path, **kwargs)
    return Client(key, timeout_seconds=3600)


def save_completion(ref, manifest, row, audio, receipt, terminal, raw, batch):
    io, clients = batch.io, batch.clients
    folder = Path(io.read(manifest['plan'])['state_root']) / 'jobs' / JOB
    remote = clients.validate_job('revai', receipt)
    clients.validate_job('revai', terminal, expected_job_id=remote['job_id'])
    normalized = clients.normalize_result('revai', raw, job=terminal, diarization=True,
        language='en', expected_duration_seconds=audio['duration_ms'] / 1000)
    labels = {s['speaker'] for s in normalized['segments'] if s.get('speaker')}
    if len(labels) == 1:
        normalized['single_speaker_normalization'] = dict(original_provider_speaker_labels=normalized['provider_speaker_labels'])
        normalized['provider_speaker_labels'] = {}
        for segment in normalized['segments']:
            segment['speaker'] = None
    terminal_ref = io.binding(folder / 'terminal-job.json')
    raw_ref = io.binding(folder / 'provider-transcript.json')
    doc = dict(kind='himr_cloud_recording_transcript', schema_version=1, job_id=JOB,
        recording_id=row['recording']['recording_id'], source_media=row['recording']['media'],
        status='completed', provider_job_id=remote['job_id'], raw_result=raw_ref, provider_job=terminal_ref,
        screen_decision=manifest['screen'], audio=audio, whole_recording_submitted=True,
        normalizer_implementation_sha256=io.binding(clients.__file__)['sha256'],
        machine_generated=True, full_media_coverage_verified=False, human_reviewed=False,
        verified_quotation=False, speaker_identity_inferred=False, publication_authority=False,
        language_policy='en', detected_language=terminal.get('language'),
        source_third_party_preserved=row['retained_third_party']['transcript'],
        provider_fallback=ref, originally_planned_provider='assemblyai', **normalized)
    transcript_ref = io.put(folder / 'transcript.json', doc)
    completion = dict(kind='himr_cloud_transcription_completion', schema_version=1, job_id=JOB,
        audio=audio, raw_result=raw_ref, provider_job=terminal_ref, transcript=transcript_ref,
        screen_decision=manifest['screen'], batch_plan=manifest['plan'], provider_fallback=ref)
    io.put(folder / 'completion.json', completion)
    return dict(state='completed', provider_job_id=remote['job_id'], transcript=transcript_ref,
        speaker_labels=len(labels), requires_speaker_review=len(labels) > 1,
        maximum_cost_microusd=manifest['maximum_cost_microusd'])


def run(ref, env_file, allow_paid):
    manifest, plan, row, batch = bootstrap(ref)
    io = batch.io
    root = Path(manifest['state_root'])
    folder = Path(plan['state_root']) / 'jobs' / JOB
    previous_state = [None]

    def progress(state, **extra):
        value = dict(job_id=JOB, state=state, provider='revai', planned_provider='assemblyai',
            provider_fallback=ref, updated_unix=time.time(), automatic_paid_retry=False, **extra)
        batch.feed.atomic(root / 'status.json', value)
        batch.feed.atomic(folder / 'status.json', value)
        if state != previous_state[0]:
            aggregate = batch.status(manifest['plan'])
            for record in aggregate['records']:
                if record['job_id'] == JOB:
                    record.update(state=state, provider='revai', planned_provider='assemblyai', provider_fallback=ref)
            aggregate['states'] = dict(Counter(r['state'] for r in aggregate['records']))
            aggregate['provider_overrides'] = [ref]
            batch.feed.atomic(Path(plan['state_root']) / 'status.json', aggregate)
            previous_state[0] = state
        print(json.dumps(value), flush=True)

    with io.locked(root), io.locked(folder):
        try:
            if (folder / 'completion.json').exists():
                completion = io.read(io.binding(folder / 'completion.json'))
                doc = io.read(completion['transcript'])
                if completion.get('provider_fallback') != ref or doc['provider'] != 'revai':
                    raise RecoveryError('existing completion is not this provider replacement')
                progress('completed', provider_job_id=doc['provider_job_id'])
                return 'completed'
            if (root / 'hold.json').exists():
                progress(io.read(io.binding(root / 'hold.json'))['state'])
                return 'held'
            audio, transport = retained_audio(manifest, plan, row, batch)
            check_hold(folder, io.read(manifest['original_hold']), io)
            intent = paid_intent(ref, manifest, row, audio, transport, batch)
            state = submission_state(folder, Path(plan['state_root']) / 'reservations', intent, batch)
            api = observed_client(batch.clients, batch.env.api_key('revai', env_file=env_file), progress)
            if state == 'submit':
                if not allow_paid:
                    raise RecoveryError('first submission requires explicit paid approval')
                batch.assert_no_prior_paid([row], plan['prior_plans'])
                # Verify only this retained FLAC, not the archive. The client
                # separately rechecks and pins the descriptor for the paid POST.
                with batch.clients._upload_file(transport['path'], transport['sha256'],
                        batch.clients.REVAI_MAX_UPLOAD_BYTES) as (_, before):
                    if before.st_size != transport['byte_count']:
                        raise RecoveryError('retained FLAC byte count changed')
                if STOP:
                    progress('paused')
                    return 'paused'
                batch.reserve(plan, manifest['plan'], row, intent)
                progress('submitting', total_bytes=transport['byte_count'])
                try:
                    receipt = api.submit_file(transport['path'], expected_sha256=transport['sha256'],
                        metadata=intent['request_metadata'], diarization=True)
                except batch.clients.CloudClientError as error:
                    if error.response is not None:
                        io.put(folder / 'submission-untrusted-response.json', error.response)
                    raise
                # Durable receipt precedes all validation, including metadata.
                io.put(folder / 'submission.json', receipt)
            else:
                receipt = io.read(io.binding(folder / 'submission.json'))
            remote = batch.clients.validate_job('revai', receipt)
            if receipt.get('metadata') != intent['request_metadata']:
                raise RecoveryError('provider receipt metadata differs')
            api.timeout_seconds = 120
            progress('pending', provider_job_id=remote['job_id'], provider_acceptance_confirmed=True)
            deadline = time.monotonic() + 6 * 3600
            while not STOP and time.monotonic() < deadline:
                terminal = batch._optional(folder / 'terminal-job.json')
                try:
                    if terminal is None:
                        terminal = api.poll(remote['job_id'])
                        checked = batch.clients.validate_job('revai', terminal, expected_job_id=remote['job_id'])
                        if checked['status'] not in {'transcribed', 'failed'}:
                            sleep(20)
                            continue
                        io.put(folder / 'terminal-job.json', terminal)
                    batch.clients.validate_job('revai', terminal, expected_job_id=remote['job_id'])
                    if terminal.get('metadata') != intent['request_metadata']:
                        raise RecoveryError('provider terminal metadata differs')
                    if terminal['status'] != 'transcribed':
                        raise RecoveryError('provider failed; no automatic paid retry')
                    progress('collecting', provider_job_id=remote['job_id'])
                    raw = batch._optional(folder / 'provider-transcript.json')
                    if raw is None:
                        raw = api.transcript(remote['job_id'])
                        io.put(folder / 'provider-transcript.json', raw)
                except batch.clients.CloudClientError as error:
                    if batch.upload.transient(error):
                        progress('pending', provider_job_id=remote['job_id'], transient_get_error=True)
                        sleep(60)
                        continue
                    raise
                result = save_completion(ref, manifest, row, audio, receipt, terminal, raw, batch)
                io.put(root / 'result.json', result)
                # Leave the original hold as retained evidence. Completion takes
                # precedence, while a missing completion can never cause AAI POST.
                progress(**result)
                return 'completed'
            progress('paused', provider_job_id=remote['job_id'])
            return 'paused'
        except Exception as error:
            uncertain = ((folder / 'intent.json').exists()
                or (Path(plan['state_root']) / 'reservations' / (JOB + '.json')).exists()) and not (folder / 'submission.json').exists()
            state = 'reconciliation_required' if uncertain else 'needs_review'
            hold = dict(state=state, job_id=JOB, provider_fallback=ref, error_type=type(error).__name__,
                reason=str(error) if isinstance(error, (RecoveryError, batch.clients.CloudClientError, batch.BatchError))
                    else 'local recovery error; retained evidence requires inspection',
                status_code=getattr(error, 'status_code', None), automatic_paid_retry=False)
            io.put(root / 'hold.json', hold)
            progress(state, error_type=hold['error_type'], reason=hold['reason'], status_code=hold['status_code'])
            return state


def stop(*_):
    global STOP
    STOP = True


def sleep(seconds):
    deadline = time.monotonic() + seconds
    while not STOP and time.monotonic() < deadline:
        time.sleep(min(1, deadline - time.monotonic()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('prepare', 'run'))
    for name in ('runtime', 'plan', 'output', 'manifest', 'expected-sha256', 'env-file'):
        parser.add_argument('--' + name)
    parser.add_argument('--allow-paid-api', action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    if args.command == 'prepare':
        print(json.dumps(prepare(args.runtime, dict(path=args.plan, sha256=args.expected_sha256), args.output)))
    else:
        state = run(dict(path=args.manifest, sha256=args.expected_sha256), args.env_file, args.allow_paid_api)
        if state not in {'completed', 'paused'}:
            raise SystemExit(2)


if __name__ == '__main__':
    main()
