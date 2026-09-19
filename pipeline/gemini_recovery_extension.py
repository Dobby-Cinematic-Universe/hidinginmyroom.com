"""Receipt-based reuse of completed exports, with conservative cache accounting.

Completed exports are local retained authority, not a new semantic verification.
Unfinished records still use the original paid-work validators and scheduler.
"""
from collections import OrderedDict
from copy import deepcopy
import hashlib
from pathlib import Path
import time


def cached_usage(original):
    def calculate(job, response):
        if not isinstance(response, dict) or not isinstance(response.get('usageMetadata'), dict):
            return original(job, response)
        usage = response['usageMetadata']
        cached = usage.get('cachedContentTokenCount', 0)
        prompt = usage.get('promptTokenCount')
        if type(cached) is not int or cached < 0 or type(prompt) is not int or cached > prompt:
            return job['budget']['maximum_cost_microusd'], False
        # Cached tokens are part of promptTokenCount, not an additional charge.
        # Retain the normal input rate as an upper estimate (no cache discount).
        adjusted = {**response, 'usageMetadata': {**usage, 'cachedContentTokenCount': 0}}
        return original(job, adjusted)
    return calculate


def memoized_validator(original, canonical, parse, limit=32 * 1024**2):
    cache = OrderedDict()
    used = 0
    def validate(value):
        nonlocal used
        key = hashlib.sha256(canonical(value)).digest()
        if key in cache:
            cache.move_to_end(key)
            return parse(cache[key])
        result = original(value)
        encoded = canonical(result)
        if len(encoded) <= limit:
            cache[key] = encoded
            used += len(encoded)
            while used > limit or len(cache) > 256:
                _, old = cache.popitem(last=False)
                used -= len(old)
        return result
    return validate


def retained_terminal(worker, entry, expected):
    """Reuse only a fully collected, successful, content-addressed final export.

Read and bind receipts/captures/requests; do not regenerate completed job graphs.
Any unfinished/failed record falls back to normal inspection. Bad bindings fail.
"""
    r = worker.r
    root = Path(entry['plan']['path']).parent
    exports = sorted((root / 'exports').glob('summaries-*.json'))
    if len(exports) != 1:
        return None
    folders = sorted((root / 'waves').glob('summarywave_*'))
    if not folders or any(not (p / 'collection.json').exists() for p in folders):
        return None
    plan = worker.job_cache._plan(entry['plan'])
    if plan['request_value'] != expected or plan['request'] != entry['request']:
        raise r.Error('retained terminal request differs')
    export_ref = r.binding(exports[0])
    export = r.read(export_ref)
    if (export.get('kind') != 'himr_private_summary_export' or export.get('schema_version') != 1
            or export.get('phase') != 'transcripts' or export.get('phase_complete') is not True
            or export.get('plan_id') != plan['plan_id'] or export.get('selected_source_ids') != plan['source_ids']
            or export.get('semantics') != r.SEMANTICS
            or exports[0].name != 'summaries-' + r.digest(export)[:32] + '.json'):
        raise r.Error('retained terminal export binding differs')
    if len(folders) > expected['limits']['max_waves']:
        raise r.Error('retained terminal wave bound exceeded')
    waves, collections, finals, job_ids = [], {}, [], set()
    for folder in folders:
        wave = r.read(r.binding(folder / 'wave.json'))
        body = {k: v for k, v in wave.items() if k != 'wave_id'}
        if (wave.get('wave_id') != folder.name or folder.name != 'summarywave_' + r.digest(body)[:32]
                or wave.get('plan_id') != plan['plan_id'] or wave.get('provider') != 'gemini'
                or wave.get('retry_of') is not None or wave.get('kind') != 'himr_transcript_summary_wave'
                or wave.get('schema_version') != 1 or not isinstance(wave.get('jobs'), list)
                or not 1 <= len(wave['jobs']) <= expected['limits']['max_jobs_per_wave']):
            raise r.Error('retained terminal wave identity differs')
        jobs = wave['jobs']
        if (any(j['stage'] not in {'chunk', 'transcript'} or j['provider'] != 'gemini' for j in jobs)
                or wave['maximum_cost_microusd'] != sum(j['budget']['maximum_cost_microusd'] for j in jobs)):
            raise r.Error('retained terminal wave cost/stage differs')
        data = r.read_bytes({'path': str(folder / 'requests.bin'), 'sha256': wave['input_sha256']})
        if len(data) != wave['input_bytes']:
            raise r.Error('retained terminal input size differs')
        intent = r.read(r.binding(folder / 'submit-intent.json'))
        if intent != {k: wave[k] for k in ('wave_id', 'maximum_cost_microusd', 'input_sha256')}:
            raise r.Error('retained terminal intent differs')
        submitted = r.read(r.binding(folder / 'submitted.json'))
        collection = r.read(r.binding(folder / 'collection.json'))
        if (submitted['wave_id'] != wave['wave_id'] or collection['wave_id'] != wave['wave_id']
                or submitted['remote_id'] != collection['remote_id']
                or collection.get('kind') != 'himr_transcript_summary_collection'
                or collection.get('schema_version') != 1
                or Path(collection['capture']['path']) != folder / 'capture.json'):
            raise r.Error('retained terminal collection binding differs')
        response_name = 'reconciliation-response.json' if submitted['reconciled'] else 'submission-response.json'
        if Path(submitted['response']['path']) != folder / response_name:
            raise r.Error('retained submission response path differs')
        r.read(submitted['response'])
        capture = r.read(collection['capture'])
        ids = [j['job_id'] for j in jobs]
        if len(set(ids)) != len(ids) or job_ids.intersection(ids):
            raise r.Error('retained terminal duplicate job')
        job_ids.update(ids)
        outcomes = collection['outcomes']
        if (len(outcomes) != len(ids) or {row['job_id'] for row in outcomes} != set(ids)
                or len(capture['items']) != len(ids) or {row['custom_id'] for row in capture['items']} != set(ids)):
            raise r.Error('retained terminal capture/outcome coverage differs')
        if any(row['state'] != 'completed' for row in outcomes):
            return None
        for row in outcomes:
            result = row['result']
            if result['job_id'] != row['job_id']:
                raise r.Error('retained result job differs')
            if result['scope']['final'] and result['stage'] == 'transcript':
                finals.append(result)
        waves.append(wave)
        collections[wave['wave_id']] = collection
    waves.sort(key=lambda row: row['ordinal'])
    if [w['ordinal'] for w in waves] != list(range(len(waves))) or not finals or finals != export['results']:
        raise r.Error('retained terminal final/order differs')
    accounted = worker.accounting.accounted_state(plan, {'waves': waves, 'collections': collections})
    status = dict(state='completed', transcript_phase_complete=True, pending_waves=[],
        ambiguous_waves=[], prepared_waves=[], failed_jobs=0, ready_jobs=0)
    compact = [{**{k: w[k] for k in ('wave_id', 'input_sha256', 'maximum_cost_microusd')},
        'input_tokens': worker.admission.wave_input_tokens(w['jobs']),
        'folder': str(root / 'waves' / w['wave_id'])} for w in waves]
    artifact = dict(state='exported_private', artifact=export_ref, final_summaries=len(finals),
        phase='transcripts', phase_complete=True, complete=export['complete'])
    return dict(status=status, accounted=accounted, waves=compact), artifact


def install(worker):
    worker.accounting.usage_cost = cached_usage(worker.accounting.usage_cost)
    r = worker.r
    r.core.validate_job = memoized_validator(r.core.validate_job, r.canonical, r.parse)
    original_snapshot, original_export = worker._record_snapshot, worker.job_cache.cached_export
    def available(manifest):
        # Source refresh consumes completed transcript receipts, not the acoustic
        # screen's entire historical model manifests for every completed job.
        # Newly admitted sources still undergo normal source/provider validation.
        cloud = worker.cloud
        plan = cloud.load_plan(manifest['cloud_plan'])
        selected = {}
        for row in plan['recordings']:
            source = row['recording']
            disposition = row['disposition']
            artifact = completion_ref = None
            if disposition == 'third_party':
                artifact, format_ = row['import']['transcript'], 'third_party'
            elif disposition == 'cloud_import':
                artifact, completion_ref, format_ = row['import']['transcript'], row['import']['completion'], 'cloud'
            elif disposition == 'cloud':
                folder = cloud._folder(plan, row)
                path = folder / 'completion.json'
                if not r.safe.exists(path):
                    continue
                completion_ref = r.binding(path)
                receipt = r.read(completion_ref)
                if (receipt.get('kind') != 'himr_cloud_transcription_completion'
                        or receipt.get('schema_version') != 1 or receipt.get('job_id') != row['job_id']
                        or Path(receipt['transcript']['path']) != folder / 'transcript.json'):
                    raise r.Error('preferred completion receipt differs')
                artifact, format_ = receipt['transcript'], 'cloud'
            if artifact is None:
                continue
            doc = r.read(artifact)
            if doc.get('recording_id') != source['recording_id'] or doc.get('status') != 'completed':
                raise r.Error('preferred transcript identity/status differs')
            if completion_ref is not None and r.read(completion_ref)['transcript'] != artifact:
                raise r.Error('preferred completion transcript differs')
            recording = source['recording_id']
            if recording in selected or len(selected) >= worker.MAX_RECORDS:
                raise r.Error('preferred receipt selection duplicates/exceeds bound')
            selected[recording] = dict(recording_id=recording, title=source['title'],
                date_metadata=source['date'], format=format_, transcript=artifact, completion=completion_ref)
        return selected
    worker._available = available
    retained = {}
    counts = dict(terminal_records_reused=0, unfinished_records_validated=0)
    started = time.monotonic()
    def snapshot(entry, expected):
        value = retained_terminal(worker, entry, expected)
        if value is not None:
            compact, artifact = value
            retained[entry['plan']['path']] = (worker.job_cache._record_witness(Path(entry['plan']['path']).parent), artifact)
            counts['terminal_records_reused'] += 1
        else:
            compact = original_snapshot(entry, expected)
            counts['unfinished_records_validated'] += 1
        if sum(counts.values()) % 25 == 0:
            print(r.canonical(dict(event='recovery_inspection_progress', **counts,
                elapsed_seconds=round(time.monotonic() - started, 1))).decode().strip(), flush=True)
        return compact
    def export(entry, validator):
        existing = retained.get(entry['plan']['path'])
        if existing and existing[0] == worker.job_cache._record_witness(Path(entry['plan']['path']).parent):
            r.read_bytes(existing[1]['artifact'])
            return deepcopy(existing[1])
        return original_export(entry, validator)
    worker._record_snapshot, worker.job_cache.cached_export = snapshot, export
