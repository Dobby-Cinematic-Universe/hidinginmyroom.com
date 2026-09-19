"""Explicit provider-managed spending for one immutable Gemini worker.

Cost estimates and reservations remain evidence, not spending authority. This
opt-in adapter disables dollar admission only; queue, intent, source, request,
attempt and result validation are unchanged. Original manifests stay sealed.

Small exact-source adaptations avoid copying the retained state validator or
changing its on-disk implementation identity. A changed guard fails activation
closed. All other statements execute with the original module globals.
"""
from functools import partial
import inspect
from pathlib import Path
import textwrap

POLICY = 'google_dashboard_spend_cap_v1'


def variant(function, replacements):
    source = getattr(function, '__himr_variant_source__', None)
    if source is None:
        source = textwrap.dedent(inspect.getsource(function))
    for before, after in replacements:
        if source.count(before) != 1:
            raise RuntimeError('retained spend guard changed: ' + function.__name__)
        source = source.replace(before, after, 1)
    namespace = {}
    exec(compile(source, str(Path(__file__)) + ':' + function.__name__, 'exec'),
         function.__globals__, namespace)
    result = namespace[function.__name__]
    result.__himr_variant_source__ = source
    return result


def install(worker, reference):
    """Install before retry/cache adapters; scope to this worker's Gemini plans."""
    r = worker.r
    manifest = r.read(reference)
    root = Path(manifest['state_root'])
    if (manifest.get('kind') != worker.KIND or Path(reference['path']) != root / 'manifest.json'
            or any(r.core.PROFILES[manifest['config'][key]]['provider'] != 'gemini'
                   for key in ('transcript_profile', 'timeline_profile'))):
        raise r.Error('dashboard spend policy requires an exact Gemini worker')

    def plan_scope(plan):
        request = plan['request_value']
        return (Path(request['state_root']).parent == root / 'records'
                and all(r.core.PROFILES[request['config'][key]]['provider'] == 'gemini'
                        for key in ('transcript_profile', 'timeline_profile')))

    globals_saved = []
    for module, name, value in ((r, '_dashboard_spend_plan', plan_scope),
            (worker, '_dashboard_spend_manifest', lambda value: value == manifest)):
        if hasattr(module, name):
            raise r.Error('dashboard spend policy already installed')
        globals_saved.append((module, name))
        setattr(module, name, value)
    patches = []

    def replace(module, name, edits):
        original = getattr(module, name)
        replacement = variant(original, edits)
        patches.append((module, name, original))
        setattr(module, name, replacement)

    def restore():
        for module, name, original in reversed(patches):
            setattr(module, name, original)
        for module, name in globals_saved:
            delattr(module, name)

    try:
        replace(r, 'load_state', [(
            'or reserved > plan["request_value"]["budget"]["max_reserved_microusd"]:',
            'or (reserved > plan["request_value"]["budget"]["max_reserved_microusd"] and not _dashboard_spend_plan(plan)):')])
        replace(r, 'submit_wave', [(
            'if state["reserved_microusd"] + wave["maximum_cost_microusd"] > plan["request_value"]["budget"]["max_reserved_microusd"]:',
            'if not _dashboard_spend_plan(plan) and state["reserved_microusd"] + wave["maximum_cost_microusd"] > plan["request_value"]["budget"]["max_reserved_microusd"]:')])
        replace(worker, '_snapshot', [(
            'if settled + held > manifest["max_total_budget_microusd"]:',
            'if not _dashboard_spend_manifest(manifest) and settled + held > manifest["max_total_budget_microusd"]:')])
        replace(worker, '_permanent_candidate_holds', [(
            'if wave["maximum_cost_microusd"] > remaining_without_pending:',
            'if not _dashboard_spend_manifest(manifest) and wave["maximum_cost_microusd"] > remaining_without_pending:')])
        # The dedicated worker's pure admission still validates every monetary
        # value, but no amount of dollars alone blocks an otherwise safe wave.
        replace(worker.admission, 'assess_admission', [
            ('if accounted > budget_limit_microusd:', 'if False:  # Provider-managed spend, accounting retained.'),
            ('candidate_exceeds_budget_limit = candidate_cost_microusd > budget_limit_microusd',
             'candidate_exceeds_budget_limit = False'),
            ('budget_state = "available"', 'budget_state = "provider_managed"'),
            ('if accounted + candidate_cost_microusd > budget_limit_microusd:', 'if False:  # No local dollar admission.'),
            ('"budget_remaining_microusd": budget_limit_microusd - accounted,',
             '"budget_remaining_microusd": None, "local_spend_limit_enforced": False,')])
        original_public = worker._public

        def public(current, snapshot):
            result = original_public(current, snapshot)
            if current == manifest:
                result.update(spend_policy=POLICY, local_spend_limit_enforced=False,
                    historical_worker_budget_microusd=result['max_total_budget_microusd'],
                    max_total_budget_microusd=None, provider_spend_cap_verified=False,
                    provider_cap_batch_overshoot_possible=True)
            return result

        patches.append((worker, '_public', original_public))
        worker._public = public
    except BaseException:
        restore()
        raise
    return restore


def initialize_collector(extension_ref, reference, previous_initializer, *args):
    from pipeline import cloud_transcription_summary as worker
    if worker.r.binding(__file__) != extension_ref or args[0] != reference:
        raise worker.r.Error('dashboard-spend collector binding differs')
    restore = install(worker, reference)
    try:
        previous_initializer(*args)
    except BaseException:
        restore()
        raise
    # Keep the spend adapter alive until all later-installed adapters unwind.
    stack = worker.parallel_collection._CHILD['stack']
    from contextlib import ExitStack
    outer = ExitStack()
    outer.callback(restore)
    outer.callback(stack.close)
    worker.parallel_collection._CHILD['stack'] = outer
    import atexit
    atexit.register(outer.close)


def install_collectors(worker, reference):
    from pipeline.gemini_targeted_retry import initialize_unextended_collector
    previous = worker.parallel_collection._initialize
    if getattr(previous, '__name__', None) == '_initialize':
        previous = initialize_unextended_collector
    worker.parallel_collection._initialize = partial(initialize_collector,
        worker.r.binding(__file__), reference, previous)
