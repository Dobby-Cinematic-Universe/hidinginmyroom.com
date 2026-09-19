"""Two explicitly approved non-graphic Gemini copies, one Claude batch attempt.

Isolated process-local input projection; no live worker or original mutation.
Durable intent before POST, no automatic retry or promotion, exact evidence reuse.
"""
import argparse
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
import time
from pipeline import gemini_non_graphic_pilot as pilot

r = pilot.r
KIND = 'himr_claude_non_graphic_recovery'
ALLOWED = {'summaryjob_165979c1427bb37bb92f2cfc19ae9e90',
           'summaryjob_b93de14bf2532c0de749645748eec213'}


@contextmanager
def projection():
    original = r.core.compact_input
    def compact(stage, scope, evidence):
        value = original(stage, scope, evidence)
        if stage != 'chunk':
            raise r.Error('recovery accepts chunks only')
        return dict(stage=stage, evidence=[{k:v for k,v in row.items()
            if k in {'evidence_id', 'text', 'classification', 'speaker'}} for row in value['evidence']])
    r.core.compact_input = compact
    try:
        yield
    finally:
        r.core.compact_input = original


def implementation():
    return dict(base=pilot.implementation(), recovery=r.binding(__file__))


def build(inputs):
    jobs, selected = [], []
    for item in inputs:
        m = pilot.load(item['manifest'])
        collection = r.read(item['collection'])
        capture = r.read(collection['capture'])
        if collection != {**r.collect_result(m['wave'], capture), 'capture':collection['capture']}:
            raise r.Error('Gemini refusal collection does not replay')
        for job in m['wave']['jobs']:
            if job['job_id'] not in ALLOWED:
                continue
            row, = [x for x in capture['items'] if x['custom_id'] == job['job_id']]
            if (row.get('response') or {}).get('promptFeedback', {}).get('blockReason') != 'PROHIBITED_CONTENT':
                raise r.Error('expected exact renewed Gemini refusal')
            config = deepcopy(job['config'])
            config['transcript_profile'] = 'anthropic_sonnet_batch'
            config.pop('transcript_input_policy', None)
            config.pop('gemini_schema_policy', None)
            with projection():
                derived = r.core.make_job('chunk', job['scope'], job['evidence'], [], config)
            if derived['prompt']['input'] != job['prompt']['input']:
                raise r.Error('Claude must receive exactly the same non-graphic evidence')
            jobs.append(derived)
            selected.append(job['job_id'])
    if len(jobs) != 2 or set(selected) != ALLOWED:
        raise r.Error('recovery requires exactly the two approved refused copies')
    body = dict(provider='anthropic', model=jobs[0]['model'], jobs=jobs)
    return dict(wave_id='summarywave_' + r.digest(body)[:32], **body)


def prepare(base, root):
    inputs = [dict(manifest=r.binding(base / name / 'run/manifest.json'),
                   collection=r.binding(base / name / 'run/collection.json'))
              for name in ('non-graphic-pilot-20260916', 'non-graphic-remaining-20260916')]
    wave = build(inputs)
    allowance = sum(j['budget']['maximum_cost_microusd'] for j in wave['jobs'])
    if allowance > 1_000_000:
        raise r.Error('two-request recovery exceeds one dollar conservative allowance')
    r.protect(root, inputs)
    r.mkdir(root)
    value = dict(kind=KIND, inputs=inputs, wave=wave, implementation=implementation(),
        approval='User: Try Claude on the non-graphic copies. Exactly two renewed refusals; no automatic retries.',
        maximum_cost_microusd=allowance, state_root=str(root.resolve()),
        full_recording_summary=False, automatic_promotion=False, automatic_retries=False)
    return r.put(root / 'manifest.json', value)


def load(ref):
    m = r.read(ref)
    if (m['kind'] != KIND or m['implementation'] != implementation()
            or m['wave'] != build(m['inputs'])
            or Path(ref['path']) != Path(m['state_root']) / 'manifest.json'
            or m['maximum_cost_microusd'] != sum(j['budget']['maximum_cost_microusd'] for j in m['wave']['jobs'])):
        raise r.Error('recovery manifest replay differs')
    return m


def submit(ref, env_file=None, client=None):
    m = load(ref)
    root, wave = Path(m['state_root']), m['wave']
    with r.locked(root):
        if (root / 'submitted.json').exists():
            return dict(state='already_submitted', **r.read(r.binding(root / 'submitted.json')))
        if (root / 'submit-intent.json').exists():
            return dict(state='needs_reconciliation', automatic_resubmission=False)
        api = client or r.api_client('anthropic', env_file=env_file)
        requests = r.anthropic_requests(wave)
        wire = r.anthropic_module.anthropic_batch_bytes(requests)
        payload = r.put_bytes(root / 'requests.bin', wire)
        r.put(root / 'submit-intent.json', dict(manifest=ref, input=payload,
            maximum_cost_microusd=m['maximum_cost_microusd']))
        remote = api.create_batch(requests)
        response = r.put(root / 'submission-response.json', remote)
        view = r.check_remote(wave, remote)
        receipt = dict(remote_id=view['id'], wave_id=wave['wave_id'], response=response)
        r.put(root / 'submitted.json', receipt)
        return dict(state='submitted', **receipt)


def poll(ref, env_file=None, client=None):
    m = load(ref)
    root, wave = Path(m['state_root']), m['wave']
    with r.locked(root):
        if (root / 'collection.json').exists():
            collection = r.read(r.binding(root / 'collection.json'))
        else:
            receipt = r.read(r.binding(root / 'submitted.json'))
            api = client or r.api_client('anthropic', env_file=env_file)
            if (root / 'capture.json').exists():
                capture = r.read(r.binding(root / 'capture.json'))
            else:
                remote = api.get_batch(receipt['remote_id'])
                view = r.check_remote(wave, remote, receipt['remote_id'])
                if view['status'] not in r.TERMINAL:
                    return dict(state='waiting_remote', remote_id=receipt['remote_id'])
                capture = dict(batch=remote, items=r.remote_items('anthropic', remote, api))
                r.put(root / 'capture.json', capture)
            with projection():
                collection = {**r.collect_result(wave, capture), 'capture':r.binding(root / 'capture.json')}
            r.put(root / 'collection.json', collection)
        completed = sum(o['state'] == 'completed' for o in collection['outcomes'])
        return dict(state='completed' if completed == 2 else 'needs_review', completed=completed,
            held=2-completed, full_recording_summaries=0, automatic_promotion=False)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('command', choices=('prepare', 'submit', 'poll', 'watch'))
    p.add_argument('--path', required=True)
    p.add_argument('--sha256')
    p.add_argument('--base')
    p.add_argument('--env-file')
    p.add_argument('--allow-paid-api', action='store_true')
    a = p.parse_args()
    if a.command == 'prepare':
        result = prepare(Path(a.base), Path(a.path))
    else:
        ref = dict(path=str(Path(a.path).resolve()), sha256=a.sha256)
        if a.command == 'submit':
            if not a.allow_paid_api:
                p.error('explicit --allow-paid-api required')
            result = submit(ref, a.env_file)
        else:
            deadline = time.monotonic() + (86400 if a.command == 'watch' else 0)
            while True:
                result = poll(ref, a.env_file)
                print(r.canonical(result).decode(), flush=True)
                if result['state'] != 'waiting_remote' or time.monotonic() >= deadline:
                    return
                time.sleep(30)
    print(r.canonical(result).decode(), flush=True)


if __name__ == '__main__':
    main()
