"""Bounded recovery overlay for the immutable broader campaign.

Preserves original receipts and outcomes. One corrected attempt per held job;
uncertain POSTs are never repeated. Shares the original campaign spending cap.
"""
import argparse
from copy import deepcopy
from pathlib import Path
import time
from pipeline import sonnet_broader_campaign as c

r = c.r
ORIGINAL_STATE = c.state


def retry_params(job):
    params = deepcopy(job['params'])
    params['max_tokens'] = 16384
    params['system'] += (
        '\nCORRECTION CONTRACT: This is a replacement for an invalid output. '
        'Use ONLY the request-local evidence_id strings in the supplied evidence table, '
        f'e1 through e{len(job["evidence"])}. Never use hexadecimal IDs, source_ids, '
        'parent IDs or IDs quoted inside evidence text as evidence_ids. '
        'Produce at most 12 items per section, each under 900 characters and with '
        '1 to 32 distinct supporting evidence IDs. Combine related points concisely. '
        'Do not invent support or remove uncertainty. All evidence must support the claim. '
        'Return complete JSON, with a nonempty summary and all four sections.')
    return params


def retry_cost(params):
    rates = r.core.PROFILES['anthropic_sonnet_batch']
    return (((len(r.canonical(params)) + 4096) * rates['input_rate_eighths_microusd'] + 7) // 8
            + (params['max_tokens'] * rates['output_rate_eighths_microusd'] + 7) // 8)


def overlay(root, original):
    state = ORIGINAL_STATE(original)
    attempted, pending, recovered, failed = set(), [], {}, []
    for path in sorted((root / 'batches').glob('*/batch.json')):
        batch = r.read(r.binding(path)); folder = path.parent
        if batch['batch_id'] != 'recovery_' + r.digest(batch['jobs'])[:32]:
            raise r.Error('recovery batch identity differs')
        for job in batch['jobs']:
            key = job['job_id']
            if key in attempted or job != state['jobs'][key]:
                raise r.Error('duplicate or altered recovery job')
            attempted.add(key)
        requests = [dict(custom_id=j['job_id'], params=retry_params(j)) for j in batch['jobs']]
        cost = sum(retry_cost(x['params']) for x in requests)
        intent = folder / 'submit-intent.json'
        if intent.exists():
            sealed = r.read(r.binding(intent))
            if sealed != dict(requests=requests, maximum_cost_microusd=cost):
                raise r.Error('recovery intent differs')
            state['reserved'] += cost
        if (folder / 'capture.json').exists():
            receipt = r.read(r.binding(folder / 'submitted.json'))
            capture = r.read(r.binding(folder / 'capture.json'))
            r.anthropic_module.validate_batch(capture['batch'], expected_id=receipt['remote_id'],
                                              expected_count=len(batch['jobs']))
            if capture['batch']['processing_status'] != 'ended':
                raise r.Error('nonterminal recovery capture')
            rows = c.outcomes(dict(job_ids=[j['job_id'] for j in batch['jobs']]),
                              state['jobs'], capture['items'])
            collection = dict(capture=r.binding(folder / 'capture.json'), outcomes=rows)
            r.put(folder / 'collection.json', collection)
            for row in rows:
                if row['state'] == 'completed': recovered[row['job_id']] = row['result']
                else: failed.append(row)
        else: pending.append((folder, batch, requests, cost))
    state['results'].update(recovered)
    state['held'] = [x for x in state['held'] if x['job_id'] not in recovered]
    return state, attempted, pending, failed, len(recovered)


def tick(root, original, api, budget):
    state, attempted, pending, failed, recovered = overlay(root, original)
    for folder, batch, requests, cost in pending:
        receipt = folder / 'submitted.json'
        if not receipt.exists():
            if (folder / 'submit-intent.json').exists(): continue
            if state['reserved'] + cost > budget: continue
            r.put(folder / 'submit-intent.json', dict(requests=requests, maximum_cost_microusd=cost))
            state['reserved'] += cost
            remote = api.create_batch(requests)
            response = r.put(folder / 'submission-response.json', remote)
            r.anthropic_module.validate_batch(remote, expected_count=len(requests))
            r.put(receipt, dict(remote_id=remote['id'], response=response))
        remote = api.get_batch(r.read(r.binding(receipt))['remote_id'])
        r.anthropic_module.validate_batch(remote, expected_id=r.read(r.binding(receipt))['remote_id'],
                                          expected_count=len(requests))
        if remote['processing_status'] == 'ended':
            r.put(folder / 'capture.json', dict(batch=remote, items=r.remote_items('anthropic', remote, api)))
    state, attempted, pending, failed, recovered = overlay(root, original)
    slots = max(0, 2 - len(pending) - len(state['pending']))
    candidates = [state['jobs'][x['job_id']] for x in state['held'] if x['job_id'] not in attempted]
    for offset in range(0, min(len(candidates), slots * 16), 16):
        jobs = candidates[offset:offset + 16]
        cost = sum(retry_cost(retry_params(j)) for j in jobs)
        if state['reserved'] + cost > budget: break
        batch = dict(batch_id='recovery_' + r.digest(jobs)[:32], jobs=jobs)
        folder = root / 'batches' / batch['batch_id']; r.mkdir(folder)
        r.put(folder / 'batch.json', batch)
        state['reserved'] += cost
    return dict(recovered_jobs=recovered, failed_replacements=failed,
                pending_replacements=len(overlay(root, original)[2]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--campaign', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--env-file', required=True)
    parser.add_argument('--allow-paid-api', action='store_true')
    args = parser.parse_args()
    if not args.allow_paid_api: parser.error('requires --allow-paid-api')
    ref = r.binding(args.campaign.resolve()); m, selection, leaves, metadata = c.load(ref)
    root = args.root.resolve(); original = Path(m['state_root'])
    r.protect(root, ref); r.mkdir(root); r.mkdir(root / 'batches')
    r.put(root / 'authority.json', dict(campaign=ref, implementation=r.binding(__file__),
        approval='Recover held broader summaries, one corrected attempt per held job; retain original cap.',
        max_attempts_per_held_job=1, budget_microusd=m['budget_microusd']))
    api = r.api_client('anthropic', env_file=args.env_file)
    c.state = lambda path: overlay(root, path)[0]
    deadline = time.monotonic() + 86400
    with r.locked(root):
        while time.monotonic() < deadline:
            try:
                with r.locked(original): recovery = tick(root, original, api, m['budget_microusd'])
                pending = recovery['pending_replacements']
                limits = {**m, 'max_active_batches': max(0, 2 - pending)}
                status = c.cycle(ref, limits, selection, leaves, metadata, api, allow_paid=True)
                compact = {k:v for k,v in status.items() if k not in ('scope_progress','source_exclusions')}
                compact['recovery'] = recovery
                c.files.atomic(root / 'status.json', compact)
                print(r.canonical(compact).decode(), flush=True)
                if status['complete']: break
                remaining = overlay(root, original)
                unattempted = any(x['job_id'] not in remaining[1] for x in remaining[0]['held'])
                if not pending and not status['active_batches'] and not status['ready_jobs'] and not unattempted: break
            except r.client_module.BatchClientError as exc:
                print(r.canonical(dict(state='transport_hold', error=type(exc).__name__)).decode(), flush=True)
            time.sleep(30)


if __name__ == '__main__': main()
