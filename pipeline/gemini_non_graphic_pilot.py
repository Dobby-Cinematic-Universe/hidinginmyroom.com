"""Finite, explicitly reviewed non-graphic copies of blocked Gemini chunks.

No discovery, automatic rewriting/retry, weakened safety settings, original
mutation or promotion to full-recording summaries. Source ranges remain local;
provider inputs contain only compact evidence and clearly marked paraphrases.
"""
import argparse
from copy import deepcopy
from pathlib import Path
import time

from pipeline import transcript_summary as r

KIND = 'himr_non_graphic_summary_pilot'
PREFIX = '[Editorial non-graphic paraphrase, not a verbatim quotation] '


def sanitize(job, edits):
    r.core.validate_job(job)
    if job['provider'] != 'gemini' or job['stage'] != 'chunk' or job['dependencies']:
        raise r.Error('pilot accepts original Gemini chunks only')
    if not isinstance(edits, list) or not edits:
        raise r.Error('explicit human-reviewed edits required')
    evidence, cursor = [], 0
    for edit in edits:
        r.safe.exact(edit, {'first', 'last', 'text', 'classification'}, 'non-graphic edit')
        first, last = edit['first'], edit['last']
        r.safe.integer(first, cursor + 1, len(job['evidence']), 'first evidence ordinal')
        r.safe.integer(last, first, len(job['evidence']), 'last evidence ordinal')
        if (not isinstance(edit['text'], str) or not edit['text'].strip()
                or len(edit['text']) > 4000 or edit['classification'] not in r.core.CLASSIFICATIONS):
            raise r.Error('invalid non-graphic editorial replacement')
        evidence.extend(deepcopy(job['evidence'][cursor:first-1]))
        rows = job['evidence'][first-1:last]
        # Never resolve conflicting speaker labels by choosing one.
        speakers = {row['speaker'] for row in rows}
        body = dict(text=PREFIX + edit['text'], classification=edit['classification'],
            speaker=next(iter(speakers)) if len(speakers) == 1 else None,
            citations=r.core._unique_citations([c for row in rows for c in row['citations']]))
        evidence.append(dict(evidence_id=r.core._id('summaryevidence_', body), **body))
        cursor = last
    evidence.extend(deepcopy(job['evidence'][cursor:]))
    config = deepcopy(job['config'])
    config['transcript_input_policy'] = 'text_and_speaker_evidence_v1'
    result = r.core.make_job('chunk', job['scope'], evidence, [], config)
    if result['job_id'] == job['job_id']:
        raise r.Error('editorial copy must have a distinct job identity')
    return result


def original(selection):
    r.safe.exact(selection, {'wave', 'collection', 'job_id', 'edits'}, 'pilot selection')
    wave = r.read(selection['wave'])
    collection = r.read(selection['collection'])
    capture = r.read(collection['capture'])
    if collection != {**r.collect_result(wave, capture), 'capture': collection['capture']}:
        raise r.Error('original provider collection does not replay')
    key = selection['job_id']
    job, = [j for j in wave['jobs'] if j['job_id'] == key]
    outcome, = [o for o in collection['outcomes'] if o['job_id'] == key]
    item, = [i for i in capture['items'] if i['custom_id'] == key]
    if (outcome['state'] == 'completed'
            or (item.get('response') or {}).get('promptFeedback', {}).get('blockReason') != 'PROHIBITED_CONTENT'):
        raise r.Error('pilot must bind an actual blocked request')
    return job


def build(selection):
    r.safe.exact(selection, {'kind', 'approval', 'records'}, 'pilot selection document')
    if selection['kind'] != KIND + '_selection' or not selection['approval']:
        raise r.Error('explicit non-graphic-copy approval required')
    if not 1 <= len(selection['records']) <= 14:
        raise r.Error('pilot requires a finite selection of at most fourteen blocked chunks')
    jobs = [sanitize(original(row), row['edits']) for row in selection['records']]
    if len({j['job_id'] for j in jobs}) != len(jobs):
        raise r.Error('duplicate sanitized jobs')
    body = dict(provider='gemini', model=jobs[0]['model'], jobs=jobs,
        classification_policy='conservative_evidence_inheritance_v1')
    return dict(wave_id='summarywave_' + r.digest(body)[:32], **body)


def implementation():
    return dict(runner=r.implementation(), pilot=r.binding(__file__))


def prepare(selection_ref, root):
    selection = r.read(selection_ref)
    root = Path(root).resolve()
    r.protect(root, dict(selection=selection_ref, records=selection['records']))
    wave = build(selection)
    r.mkdir(root)
    manifest = dict(kind=KIND, schema_version=1, selection=selection_ref,
        state_root=str(root), implementation=implementation(), wave=wave,
        maximum_cost_microusd=sum(j['budget']['maximum_cost_microusd'] for j in wave['jobs']),
        original_artifacts_unchanged=True, full_recording_summary=False,
        automatic_retries=False, automatic_promotion=False)
    return r.put(root / 'manifest.json', manifest)


def load(ref):
    m = r.read(ref)
    if m['kind'] != KIND or m['implementation'] != implementation():
        raise r.Error('pilot implementation changed')
    root = Path(m['state_root'])
    if Path(ref['path']) != root / 'manifest.json' or m['wave'] != build(r.read(m['selection'])):
        raise r.Error('pilot input differs from exact reviewed edits')
    if m['maximum_cost_microusd'] != sum(j['budget']['maximum_cost_microusd'] for j in m['wave']['jobs']):
        raise r.Error('pilot price allowance differs')
    return m


def submit(ref, *, env_file=None, client=None):
    m = load(ref)
    root, wave = Path(m['state_root']), m['wave']
    with r.locked(root):
        if (root / 'submitted.json').exists():
            return dict(state='already_submitted', **r.read(r.binding(root / 'submitted.json')))
        if (root / 'submit-intent.json').exists():
            return dict(state='needs_reconciliation', automatic_resubmission=False)
        api = client or r.api_client('gemini', env_file=env_file)
        requests = [dict(key=j['job_id'], request=j['request']['body']) for j in wave['jobs']]
        wire = api.batch_bytes(wave['model'], requests, wave['wave_id'])
        # Persist exact sanitized wire bytes and intent before the only POST.
        payload = r.put_bytes(root / 'requests.bin', wire)
        r.put(root / 'submit-intent.json', dict(manifest=ref, input=payload,
            maximum_cost_microusd=m['maximum_cost_microusd']))
        remote = api.create_batch(wave['model'], requests, wave['wave_id'])
        view = r.check_remote(wave, remote)
        response = r.put(root / 'submission-response.json', remote)
        receipt = dict(remote_id=view['id'], wave_id=wave['wave_id'], response=response)
        r.put(root / 'submitted.json', receipt)
        return dict(state='submitted', **receipt)


def poll(ref, *, env_file=None, client=None):
    m = load(ref)
    root, wave = Path(m['state_root']), m['wave']
    with r.locked(root):
        if (root / 'collection.json').exists():
            collection = r.read(r.binding(root / 'collection.json'))
        else:
            receipt = r.read(r.binding(root / 'submitted.json'))
            api = client or r.api_client('gemini', env_file=env_file)
            if (root / 'capture.json').exists():
                capture = r.read(r.binding(root / 'capture.json'))
            else:
                remote = api.get_batch(receipt['remote_id'])
                view = r.check_remote(wave, remote, receipt['remote_id'])
                if view['status'] not in r.TERMINAL:
                    return dict(state='waiting_remote', remote_id=receipt['remote_id'])
                capture = dict(batch=remote, items=r.remote_items('gemini', remote, api))
                r.put(root / 'capture.json', capture)
            collection = {**r.collect_result(wave, capture), 'capture':r.binding(root / 'capture.json')}
            r.put(root / 'collection.json', collection)
        completed = sum(o['state'] == 'completed' for o in collection['outcomes'])
        return dict(state='completed' if completed == len(wave['jobs']) else 'needs_review',
            completed=completed, held=len(wave['jobs'])-completed,
            full_recording_summaries=0, automatic_promotion=False,
            collection=r.binding(root / 'collection.json'))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('command', choices=('prepare', 'submit', 'poll', 'watch'))
    p.add_argument('--path', required=True)
    p.add_argument('--sha256', required=True)
    p.add_argument('--state-root')
    p.add_argument('--env-file')
    p.add_argument('--allow-paid-api', action='store_true')
    args = p.parse_args()
    ref = dict(path=str(Path(args.path).resolve()), sha256=args.sha256)
    if args.command == 'prepare':
        if not args.state_root:
            p.error('prepare requires --state-root')
        result = prepare(ref, args.state_root)
    elif args.command == 'submit':
        if not args.allow_paid_api:
            p.error('submit requires explicit --allow-paid-api')
        result = submit(ref, env_file=args.env_file)
    else:
        deadline = time.monotonic() + (86400 if args.command == 'watch' else 0)
        while True:
            result = poll(ref, env_file=args.env_file)
            print(r.canonical(result).decode().strip(), flush=True)
            if result['state'] != 'waiting_remote' or time.monotonic() >= deadline:
                return
            time.sleep(30)
    print(r.canonical(result).decode().strip(), flush=True)


if __name__ == '__main__':
    main()
