"""Isolated, single-purchase Rev AI pilot; never admits an excerpt to the archive."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time
import wave

from pipeline import cloud_transcription_client as client_module
from pipeline import cloud_transcription_env as env
from pipeline import transcript_summary as io

REPOSITORY = Path(__file__).resolve().parents[1]
BASE = REPOSITORY / 'research/private-transcriptions/cloud-archive-20260913'
ROOT = BASE / 'revai-pilot-v1'
PLAN = {'path': str(BASE / 'transcription-v5/plan.json'),
        'sha256': '8f270964eee982c230418123c9f1992a44a0b602be0e6b1ee695902f922e07f5'}
SOURCE_SHA = '879bad49a11602859383342c5ba69499b6e6023f3506e51eadb4466767114b46'
PILOT_ALLOWANCE = 10_000


def digest_file(path):
    with open(path, 'rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def prepare():
    plan = io.read(PLAN)
    total = plan['prior_reserved_microusd'] + sum(row['maximum_cost_microusd'] for row in plan['recordings'])
    assert total + PILOT_ALLOWANCE <= 150_000_000
    row = next(row for row in plan['recordings'] if row['recording']['media']['sha256'] == SOURCE_SHA)
    source = row['recording']['media']
    assert digest_file(source['path']) == SOURCE_SHA
    if ROOT.exists():
        raise RuntimeError('Pilot root already exists; use submit or collect, never prepare another purchase')
    io.mkdir(ROOT)
    audio = ROOT / 'audio.wav'
    subprocess.run(['/usr/bin/ffmpeg', '-hide_banner', '-loglevel', 'error', '-nostdin', '-n',
                    '-threads', '1', '-protocol_whitelist', 'file,pipe', '-ss', '300', '-i', source['path'],
                    '-t', '120', '-map', '0:a:0', '-vn', '-ac', '1', '-ar', '16000',
                    '-c:a', 'pcm_s16le', '-threads', '1', '-map_metadata', '-1', str(audio)],
                   check=True, timeout=180)
    audio.chmod(0o600)
    with wave.open(str(audio)) as stream:
        duration = stream.getnframes() / stream.getframerate()
        assert stream.getnchannels() == 1 and stream.getsampwidth() == 2 and stream.getframerate() == 16000
    assert 119 <= duration <= 120
    value = {'kind': 'himr_isolated_revai_pilot', 'source': source, 'source_title': row['recording']['title'],
             'source_offset_seconds': 300, 'duration_seconds': duration, 'audio': io.binding(audio),
             'cloud_plan': PLAN, 'client_sha256': digest_file(client_module.__file__),
             'metadata': 'himr_revai_pilot_v1_' + SOURCE_SHA[:16],
             'options': client_module.revai_options(diarization=True),
             'maximum_pilot_microusd': PILOT_ALLOWANCE,
             'full_existing_plan_plus_pilot_maximum_microusd': total + PILOT_ALLOWANCE,
             'aggregate_authorized_microusd': 150_000_000,
             'archive_admission': False, 'automatic_paid_retries': False}
    io.put(ROOT / 'manifest.json', value)
    return {'state': 'prepared', 'duration_seconds': duration, 'audio_bytes': audio.stat().st_size,
            'maximum_pilot_microusd': PILOT_ALLOWANCE}


def run(command, env_file, allow_paid):
    if command == 'prepare':
        return prepare()
    with io.locked(ROOT):
        manifest = io.read(io.binding(ROOT / 'manifest.json'))
        assert manifest['client_sha256'] == digest_file(client_module.__file__)
        assert manifest['cloud_plan'] == PLAN and manifest['maximum_pilot_microusd'] == PILOT_ALLOWANCE
        io.read(PLAN)
        client = client_module.RevAIClient(env.api_key('revai', env_file=env_file))
        receipt_path = ROOT / 'submission.json'
        if command == 'submit':
            if not allow_paid:
                raise RuntimeError('Paid pilot requires --allow-paid-api')
            if receipt_path.exists():
                return {'state': 'already_submitted', 'new_paid_requests': 0}
            if (ROOT / 'intent.json').exists():
                raise RuntimeError('Retained intent without receipt: reconcile, never automatically repeat POST')
            # Reserve the pilot separately; the entire sealed campaign plus this
            # allowance fits the original aggregate cap even if every job runs.
            io.put(ROOT / 'reservation.json', {'manifest': io.binding(ROOT / 'manifest.json'),
                                              'maximum_microusd': PILOT_ALLOWANCE})
            io.put(ROOT / 'intent.json', {'manifest': io.binding(ROOT / 'manifest.json'),
                                         'metadata': manifest['metadata'], 'diarization': True})
            try:
                receipt = client.submit_file(manifest['audio']['path'], expected_sha256=manifest['audio']['sha256'],
                                             metadata=manifest['metadata'], diarization=True)
            except client_module.CloudClientError as error:
                if error.response is not None:
                    io.put(ROOT / 'submission-untrusted-response.json', error.response)
                io.put(ROOT / 'submission-error.json', {'message': str(error), 'status_code': error.status_code,
                                                        'ambiguous': error.ambiguous, 'automatic_retry': False})
                raise
            io.put(receipt_path, receipt)
            checked = client_module.validate_job('revai', receipt)
            return {'state': 'submitted', 'job_id': checked['job_id'], 'new_paid_requests': 1}
        receipt = io.read(io.binding(receipt_path))
        identifier = client_module.validate_job('revai', receipt)['job_id']
        terminal_path = ROOT / 'terminal-job.json'
        if terminal_path.exists():
            job = io.read(io.binding(terminal_path))
        else:
            job = client.poll(identifier)
            io.put(ROOT / ('poll-' + str(time.time_ns()) + '.json'), job)
            checked = client_module.validate_job('revai', job, expected_job_id=identifier)
            if checked['status'] == 'in_progress':
                return {'state': 'pending', 'job_id': identifier, 'new_paid_requests': 0}
            io.put(terminal_path, job)
        if job['status'] != 'transcribed':
            return {'state': 'provider_failed', 'job_id': identifier, 'new_paid_requests': 0}
        raw_path = ROOT / 'provider-transcript.json'
        if not raw_path.exists():
            io.put(raw_path, client.transcript(identifier))
        raw = io.read(io.binding(raw_path))
        normalized = client_module.normalize_result('revai', raw, expected_duration_seconds=manifest['duration_seconds'],
                                                     job=job, diarization=True)
        assert normalized['segments'] and all('words' not in segment for segment in normalized['segments'])
        output = io.put(ROOT / 'normalized-transcript.json', normalized)
        report = {'state': 'completed', 'job_id': identifier, 'duration_seconds': normalized['duration_seconds'],
                  'segments': len(normalized['segments']), 'speaker_labels': len(normalized['provider_speaker_labels']),
                  'segment_timestamps_retained': True, 'normalized_word_timestamps': False,
                  'estimated_cost_microusd': int((manifest['duration_seconds'] * 200_000 + 3599) // 3600),
                  'maximum_reserved_microusd': PILOT_ALLOWANCE, 'normalized_transcript': output,
                  'archive_admission': False, 'new_paid_requests': 0}
        io.put(ROOT / 'completion.json', report)
        return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['prepare', 'submit', 'collect'])
    parser.add_argument('--env-file', default=str(REPOSITORY / '.env'))
    parser.add_argument('--allow-paid-api', action='store_true')
    args = parser.parse_args()
    print(json.dumps(run(args.command, args.env_file, args.allow_paid_api)))
