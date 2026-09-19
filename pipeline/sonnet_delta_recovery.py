"""Bounded strict recovery of the incremental synthesis; no indexing actions."""
import importlib.util
import sys
from pathlib import Path
from pipeline import sonnet_summary_delta as delta
from pipeline import sonnet_broader_recovery as prior
from pipeline.sonnet_broader_strict_recovery import strict_params


def main():
    args=sys.argv[1:]
    campaign=Path(args[args.index('--campaign')+1]).resolve()
    output=Path(args[args.index('--root')+1]).resolve()
    m=prior.r.read(prior.r.binding(campaign))
    if m['delta_implementation']!=prior.r.binding(delta.__file__):
        raise prior.r.Error('Incremental implementation changed')
    prior.r.mkdir(output)
    prior.r.put(output/'strict-policy.json',dict(implementation=prior.r.binding(__file__),
        strict_parameters=prior.r.binding(sys.modules[strict_params.__module__].__file__),
        approval='Recover failed incremental summaries with one strict replacement per held job, preserving evidence checks and shared cap.'))
    spec=importlib.util.spec_from_file_location('delta_strict_recovery',prior.__file__)
    worker=importlib.util.module_from_spec(spec);spec.loader.exec_module(worker)
    worker.retry_params=strict_params
    worker.c.frontier=delta.frontier
    worker.main()


if __name__=='__main__':main()
