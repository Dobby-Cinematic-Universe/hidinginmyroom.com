"""Explicit one-time replacements of five unresolved Gemini POSTs.

Original unknown attempts stay intact. Replacement captures are separately
identified candidates, not fabricated reconciliation receipts or automatic retries.
"""
import argparse
from pathlib import Path
import time
from pipeline import transcript_summary as r

KIND = 'himr_explicit_unknown_gemini_replacement'
APPROVAL = 'User: Allow one replacement attempt despite duplicate-charge risk.'


def implementation():
    return dict(runtime=r.implementation(), replacement=r.binding(__file__))


def build(listing_ref):
    listing = r.read(listing_ref)
    if not listing['complete'] or listing['matches'] or len(listing['targets']) != 5:
        raise r.Error('expected five unmatched submission intents')
    rows = []
    for item in listing['targets']:
        original = Path(item['plan']['path']).parent / 'waves' / item['wave_id']
        wave_ref = r.binding(original / 'wave.json')
        wave = r.read(wave_ref)
        intent = r.read(item['intent'])
        if intent['wave_id'] != wave['wave_id'] or wave['wave_id'] != item['wave_id']:
            raise r.Error('unknown attempt identity differs')
        if intent['input_sha256'] != wave['input_sha256']:
            raise r.Error('unknown attempt payload differs')
        if wave['provider'] != 'gemini':
            raise r.Error('replacement is Gemini-only')
        for job in wave['jobs']:
            r.core.validate_job(job)
        body = dict(provider='gemini', model=wave['model'], jobs=wave['jobs'],
            explicit_unknown_replacement=dict(original_wave=wave_ref,
                original_intent=item['intent'], listing=listing_ref, approval=APPROVAL))
        new = dict(wave_id='summarywave_' + r.digest(body)[:32], **body)
        rows.append(dict(title=item['title'], original_plan=item['plan'], wave=new))
    if len({x['wave']['wave_id'] for x in rows}) != 5:
        raise r.Error('duplicate replacement wave')
    return rows


def prepare(listing_ref, root):
    rows = build(listing_ref)
    allowance = sum(j['budget']['maximum_cost_microusd'] for x in rows for j in x['wave']['jobs'])
    if allowance > 2_000_000:
        raise r.Error('replacement allowance exceeds two dollars')
    r.protect(root, dict(listing=listing_ref, rows=rows))
    r.mkdir(root)
    for row in rows:
        r.mkdir(root / row['wave']['wave_id'])
    return r.put(root / 'manifest.json', dict(kind=KIND, listing=listing_ref,
        approval=APPROVAL, rows=rows, implementation=implementation(), state_root=str(root.resolve()),
        maximum_cost_microusd=allowance, duplicate_charge_risk_accepted=True,
        automatic_retries=False, automatic_promotion=False, originals_modified=False))


def load(ref):
    m = r.read(ref)
    if (m['kind'] != KIND or m['implementation'] != implementation()
            or m['rows'] != build(m['listing'])
            or Path(ref['path']) != Path(m['state_root']) / 'manifest.json'):
        raise r.Error('replacement manifest replay differs')
    return m


def submit(ref, env_file=None, client=None):
    m = load(ref)
    api = client or r.api_client('gemini', env_file=env_file)
    outcomes = []
    for row in m['rows']:
        wave = row['wave']
        folder = Path(m['state_root']) / wave['wave_id']
        with r.locked(folder):
            if (folder / 'submitted.json').exists():
                outcomes.append(dict(state='already_submitted', **r.read(r.binding(folder / 'submitted.json'))))
                continue
            if (folder / 'submit-intent.json').exists():
                outcomes.append(dict(wave_id=wave['wave_id'], state='needs_reconciliation'))
                continue
            original = Path(wave['explicit_unknown_replacement']['original_wave']['path']).parent
            if any((original / name).exists() for name in ('submitted.json', 'submission-response.json', 'capture.json')):
                raise r.Error('original now has a receipt; review before buying replacement')
            requests = [dict(key=j['job_id'], request=j['request']['body']) for j in wave['jobs']]
            payload = r.put_bytes(folder / 'requests.bin', api.batch_bytes(wave['model'], requests, wave['wave_id']))
            r.put(folder / 'submit-intent.json', dict(manifest=ref, input=payload, wave_id=wave['wave_id']))
            remote = api.create_batch(wave['model'], requests, wave['wave_id'])
            response = r.put(folder / 'submission-response.json', remote)
            view = r.check_remote(wave, remote)
            receipt = dict(wave_id=wave['wave_id'], remote_id=view['id'], response=response)
            r.put(folder / 'submitted.json', receipt)
            outcomes.append(dict(state='submitted', **receipt))
    return dict(outcomes=outcomes, automatic_retries=False)


def poll(ref, env_file=None, client=None):
    m = load(ref)
    api = client or r.api_client('gemini', env_file=env_file)
    outcomes = []
    for row in m['rows']:
        wave = row['wave']
        folder = Path(m['state_root']) / wave['wave_id']
        with r.locked(folder):
            if (folder / 'collection.json').exists():
                collection = r.read(r.binding(folder / 'collection.json'))
            elif not (folder / 'submitted.json').exists():
                outcomes.append(dict(wave_id=wave['wave_id'], state='needs_reconciliation'))
                continue
            else:
                receipt = r.read(r.binding(folder / 'submitted.json'))
                if (folder / 'capture.json').exists():
                    capture = r.read(r.binding(folder / 'capture.json'))
                else:
                    remote = api.get_batch(receipt['remote_id'])
                    view = r.check_remote(wave, remote, receipt['remote_id'])
                    if view['status'] not in r.TERMINAL:
                        outcomes.append(dict(wave_id=wave['wave_id'], state='waiting_remote'))
                        continue
                    capture = dict(batch=remote, items=r.remote_items('gemini', remote, api))
                    r.put(folder / 'capture.json', capture)
                collection = {**r.collect_result(wave, capture), 'capture':r.binding(folder / 'capture.json')}
                r.put(folder / 'collection.json', collection)
            completed = sum(o['state'] == 'completed' for o in collection['outcomes'])
            outcomes.append(dict(wave_id=wave['wave_id'], completed=completed,
                held=len(wave['jobs'])-completed, state='completed' if completed == len(wave['jobs']) else 'needs_review'))
    return dict(outcomes=outcomes, automatic_promotion=False)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('command', choices=('prepare','submit','poll','watch'))
    p.add_argument('--path', required=True)
    p.add_argument('--sha256', required=True)
    p.add_argument('--root')
    p.add_argument('--env-file')
    p.add_argument('--allow-paid-api', action='store_true')
    a = p.parse_args()
    ref = dict(path=str(Path(a.path).resolve()), sha256=a.sha256)
    if a.command == 'prepare':
        result = prepare(ref, Path(a.root))
    elif a.command == 'submit':
        if not a.allow_paid_api:
            p.error('explicit --allow-paid-api required')
        result = submit(ref, a.env_file)
    else:
        deadline = time.monotonic() + (86400 if a.command == 'watch' else 0)
        while True:
            result = poll(ref, a.env_file)
            print(r.canonical(result).decode(), flush=True)
            if not any(o['state']=='waiting_remote' for o in result['outcomes']) or time.monotonic() >= deadline:
                return
            time.sleep(30)
    print(r.canonical(result).decode(), flush=True)


if __name__ == '__main__':
    main()
