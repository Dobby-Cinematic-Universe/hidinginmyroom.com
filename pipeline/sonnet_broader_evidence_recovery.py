"""Audited duplicate-reference repair; replay sealed recoveries and resume frontier.

Never changes source captures, prose, classifications or the set of evidence.
Unknown references and all other schema errors still fail the original validator.
"""
import argparse
from contextlib import ExitStack
from copy import deepcopy
import importlib.util
from pathlib import Path
import time
from pipeline import sonnet_broader_recovery as prior
from pipeline.sonnet_broader_strict_recovery import strict_params

c, r = prior.c, prior.r


def repair(job, response):
    payload = r.response_payload('anthropic', response)
    changes = []
    for section, items in payload.items():
        for index, item in enumerate(items):
            ids = item['evidence_ids']
            if not isinstance(ids, list) or any(not isinstance(x, str) for x in ids):
                raise r.Error('invalid evidence list')
            unique = list(dict.fromkeys(ids))
            if ids != unique:
                changes.append(dict(section=section, index=index, before=ids, after=unique))
                item['evidence_ids'] = unique
    if not changes:
        raise r.Error('no duplicate references to repair')
    corrected = deepcopy(response)
    parts = [p for p in corrected['content'] if p['type'] == 'text']
    if len(parts) != 1:
        raise r.Error('expected one structured text block')
    parts[0]['text'] = r.canonical(payload).decode()
    result = c.normalize(job, corrected)
    return result, corrected, changes


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--prior-root', type=Path, required=True)
    p.add_argument('--strict-root', type=Path, required=True)
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--env-file', required=True)
    p.add_argument('--allow-paid-api', action='store_true')
    a = p.parse_args()
    previous, strict, output = a.prior_root.resolve(), a.strict_root.resolve(), a.root.resolve()
    authority = r.read(r.binding(strict/'authority.json'))
    for directory in (previous, strict):
        if r.read(r.binding(directory/'authority.json'))['implementation'] != r.binding(prior.__file__):
            raise r.Error('sealed recovery implementation changed')
    policy = r.read(r.binding(strict/'strict-policy.json'))
    from pipeline import sonnet_broader_strict_recovery as strict_module
    if policy['implementation'] != r.binding(strict_module.__file__):
        raise r.Error('sealed strict policy changed')
    spec = importlib.util.spec_from_file_location('evidence_recovery_replay', prior.__file__)
    worker = importlib.util.module_from_spec(spec); spec.loader.exec_module(worker)
    worker.ORIGINAL_STATE = lambda original: prior.overlay(previous, original)[0]
    worker.retry_params = strict_params
    ref = authority['campaign']; m, selection, leaves, metadata = c.load(ref)
    original = Path(m['state_root'])
    r.mkdir(output)
    r.put(output/'authority.json', dict(campaign=ref, implementation=r.binding(__file__),
        strict_authority=r.binding(strict/'authority.json'),
        policy='Deduplicate evidence references only; preserve captured provider bytes and original cap.'))

    def state(root):
        s, _, pending, failed, _ = worker.overlay(strict, root)
        if pending:
            raise r.Error('strict recovery still has pending work')
        failed_ids = {row['job_id'] for row in failed}
        for capture in sorted((strict/'batches').glob('*/capture.json')):
            for row in r.read(r.binding(capture))['items']:
                key = row['custom_id']
                if key not in failed_ids or row['error'] is not None: continue
                result, corrected, changes = repair(s['jobs'][key], row['response'])
                r.put(output/(key+'.json'), dict(capture=r.binding(capture),
                    changes=changes, corrected_response=corrected, result=result))
                s['results'][key] = result
                s['held'] = [x for x in s['held'] if x['job_id'] != key]
        return s

    c.state = state
    api = r.api_client('anthropic', env_file=a.env_file) if a.allow_paid_api else None
    with ExitStack() as stack:
        for directory in (previous, strict, output): stack.enter_context(r.locked(directory))
        deadline = time.monotonic()+86400
        while time.monotonic() < deadline:
            status = c.cycle(ref, m, selection, leaves, metadata, api, allow_paid=a.allow_paid_api)
            compact = {k:v for k,v in status.items() if k not in ('scope_progress','source_exclusions')}
            c.files.atomic(output/'status.json', compact)
            print(r.canonical(compact).decode(), flush=True)
            if status['complete'] or not a.allow_paid_api or not status['active_batches']: break
            time.sleep(30)


if __name__ == '__main__': main()
