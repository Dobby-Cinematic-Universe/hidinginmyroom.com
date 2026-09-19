"""One schema-constrained follow-up after the initial recovery drains.

Loads the prior recovery in an independent module namespace so its sealed
requests continue to replay with their original parameters. All spending and
successful outcomes are inherited; only still-held jobs are eligible.
"""
import argparse
from contextlib import ExitStack
import importlib.util
from pathlib import Path
import sys
import time
from pipeline import sonnet_broader_recovery as prior


def strict_params(job):
    params = prior.retry_params(job)
    schema = params['output_config']['format']['schema']
    schema['definitions'] = {'evidence_reference': dict(type='string',
        enum=['e'+str(n) for n in range(1, len(job['evidence'])+1)])}
    for section in prior.c.SECTIONS:
        schema['properties'][section]['items']['properties']['evidence_ids']['items'] = {
            '$ref':'#/definitions/evidence_reference'}
    return params


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--prior-root', type=Path, required=True)
    args, rest = p.parse_known_args()
    root = args.prior_root.resolve()
    authority = prior.r.read(prior.r.binding(root / 'authority.json'))
    if authority['implementation'] != prior.r.binding(prior.__file__):
        raise prior.r.Error('prior recovery implementation changed')
    spec = importlib.util.spec_from_file_location('himr_strict_recovery_runtime', prior.__file__)
    worker = importlib.util.module_from_spec(spec); spec.loader.exec_module(worker)
    worker.ORIGINAL_STATE = lambda original: prior.overlay(root, original)[0]
    worker.retry_params = strict_params
    output = Path(rest[rest.index('--root')+1]).resolve()
    prior.r.mkdir(output)
    prior.r.put(output / 'strict-policy.json', dict(implementation=prior.r.binding(__file__),
        previous=prior.r.binding(root / 'authority.json'), max_additional_attempts_per_held_job=1,
        approval='Complete the requested recovery with request-local evidence enum constraints; same shared cap.'))
    deadline = time.monotonic() + 86400
    # Do not interrupt an in-flight POST or overlap the original recovery owner.
    with ExitStack() as stack:
        while True:
            try:
                stack.enter_context(prior.r.locked(root)); break
            except prior.r.Error as error:
                if str(error) != 'another summary command owns this workspace': raise
                if time.monotonic() >= deadline: raise
                time.sleep(30)
        pending = prior.overlay(root, Path(authority['campaign']['path']).parent)[2]
        if pending:
            raise prior.r.Error('prior recovery has unsettled requests; resume it before strict follow-up')
        sys.argv = [sys.argv[0], *rest]
        worker.main()


if __name__ == '__main__': main()
