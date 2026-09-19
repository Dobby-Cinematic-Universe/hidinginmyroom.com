"""Offline finite recovery selection from the stopped worker's held records.

Only inspect failed records and the previous authority; do not rescan media,
rebuild completed transcripts, call providers, or change original artifacts.
"""
from collections import Counter
from copy import deepcopy
import datetime as dt
from pathlib import Path

from pipeline import gemini_targeted_retry as retry


def prepare(worker, worker_ref, previous_ref, status_ref, output, *, approval):
    r = worker.r
    manifest, status = r.read(worker_ref), r.read(status_ref)
    root = Path(manifest['state_root'])
    previous = retry.Authority(worker, previous_ref)
    if previous.value['worker'] != worker_ref or not approval or status.get('state') != 'paused':
        raise r.Error('recovery selection requires explicit approval and a paused exact worker')
    grants = {}
    for original in previous.value['grants']:
        grant = deepcopy(original)
        for job in grant['jobs']:
            job.update(max_attempts=previous.limits[job['job_id']],
                previous_failure=job.get('previous_failure'))
        grants[grant['wave']['path']] = grant
    repairs = {g['wave']['path']: deepcopy(g) for g in previous.value.get('repairs', [])}
    withheld, fresh = Counter(), set()
    with r.locked(root):
        for hold in status['holds']:
            if not hold.get('failed_jobs'):
                continue
            plan_ref = hold['plan']
            plan_root = Path(plan_ref['path']).parent
            if plan_root.parent != root / 'records':
                raise r.Error('recovery hold escaped worker')
            entry_ref = r.binding(root / 'entries' / (plan_root.name + '.json'))
            entry = r.read(entry_ref)
            if entry['plan'] != plan_ref:
                raise r.Error('recovery held entry differs')
            waves = []
            for path in (plan_root / 'waves').glob('summarywave_*/wave.json'):
                wave_ref = r.binding(path)
                wave = r.read(wave_ref)
                collection_ref = r.binding(path.parent / 'collection.json') if (path.parent / 'collection.json').exists() else None
                collection = r.read(collection_ref) if collection_ref else None
                waves.append((wave, wave_ref, collection, collection_ref))
            waves.sort(key=lambda row: row[0]['ordinal'])
            successful = {o['job_id'] for _, _, c, _ in waves if c for o in c['outcomes'] if o['state'] == 'completed'}
            active = {j['job_id'] for w, _, c, _ in waves if c is None for j in w['jobs']}
            attempts = Counter(j['job_id'] for w, _, _, _ in waves for j in w['jobs'])
            for wave, wave_ref, collection, collection_ref in waves:
                if collection is None:
                    continue
                common = dict(recording_id=entry['source']['recording_id'], entry=entry_ref,
                    plan=plan_ref, wave=wave_ref, collection=collection_ref)
                jobs = {j['job_id']: j for j in wave['jobs']}
                local = []
                for outcome in collection['outcomes']:
                    key = outcome['job_id']
                    if outcome['state'] == 'completed' or key in successful or key in active:
                        continue
                    job = jobs[key]
                    if wave_ref['path'] in repairs and any(j['job_id'] == key for j in repairs[wave_ref['path']]['jobs']):
                        continue
                    if outcome.get('validation_error') == retry.REPAIR_ERROR and (
                            attempts[key] == 1 or (wave['retry_of'] is not None and previous.permits(entry, wave))):
                        local.append(dict(job_id=key, job_sha256=r.digest(job)))
                        continue
                    if wave['retry_of'] is not None:
                        continue
                    if not retry.Authority.retryable(outcome, True,
                            batch=r.read(collection['capture'])['batch']) or attempts[key] not in {1, 2}:
                        withheld[outcome.get('validation_error') or outcome['failure']] += 1
                        continue
                    proof = None
                    if attempts[key] == 2:
                        prior = [(w, c, ref) for w, _, c, ref in waves
                            if c and w['retry_of'] == wave['wave_id'] and job in w['jobs']]
                        if len(prior) != 1:
                            raise r.Error('no exact failed second-attempt proof')
                        latest, = [o for o in prior[0][1]['outcomes'] if o['job_id'] == key]
                        if not retry.Authority.retryable(latest, True,
                                batch=r.read(prior[0][1]['capture'])['batch']):
                            # An earlier INTERNAL failure never overrides a
                            # subsequent safety block or unapproved failure.
                            if not (latest.get('validation_error') == retry.REPAIR_ERROR
                                    and previous.permits(entry, prior[0][0])):
                                withheld[latest.get('validation_error') or latest['failure']] += 1
                            continue
                        proof = prior[0][2]
                    grant = grants.setdefault(wave_ref['path'], dict(common, jobs=[]))
                    selected = dict(job_id=key, job_sha256=r.digest(job),
                        maximum_cost_microusd=job['budget']['maximum_cost_microusd'],
                        max_attempts=attempts[key] + 1, previous_failure=proof)
                    grant['jobs'] = [v for v in grant['jobs'] if v['job_id'] != key] + [selected]
                    fresh.add(key)
                if local:
                    grant = repairs.setdefault(wave_ref['path'], dict(common, jobs=[]))
                    grant['jobs'].extend(local)
        ordered = sorted(grants.values(), key=lambda g: g['wave']['path'])
        for grant in ordered:
            grant['jobs'].sort(key=lambda j: j['job_id'])
        value = dict(kind=retry.KIND, schema_version=2, worker=worker_ref, approval=approval,
            selection_cutoff_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
            max_attempts_per_selected_job=3, provider_error_code=13, grants=ordered,
            selected_job_count=sum(len(g['jobs']) for g in ordered),
            maximum_additional_cost_microusd=sum(j['maximum_cost_microusd'] for g in ordered for j in g['jobs']),
            repairs=sorted(repairs.values(), key=lambda g: g['wave']['path']),
            repaired_job_count=sum(len(g['jobs']) for g in repairs.values()))
        reference = r.put(Path(output), value)
        authority = retry.Authority(worker, reference)
        return dict(authority=reference, selected_jobs=value['selected_job_count'],
            preserved_previous_jobs=previous.value['selected_job_count'], newly_recoverable_failed_jobs=len(fresh),
            repair_jobs=value['repaired_job_count'], records=len(authority.records),
            withheld=dict(withheld), new_paid_requests=0)
