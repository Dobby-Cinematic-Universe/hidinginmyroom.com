"""Explicit six-recording replacement lane; originals and old paid work stay put."""
import argparse
from contextlib import contextmanager
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys

SELECTED = (
    'cloudjob_41d88f588e3e65a7594d88c7906e4f8a',  # Meeting Sora
    'cloudjob_a6f151bfea7077efaefda7327475a75e',  # Livestream with wife
    'cloudjob_b472c41e25245f407a24fa19e6c23cee',  # Mila Q&A
    'cloudjob_f4954b15da06da48fd9a9be33131d1b3',  # Sunny stream
    'cloudjob_1ea3e138c183016b15dceb5a911c3b17',  # Mega Q&A
    'cloudjob_268a181b7daf6e7d1909de5fd49c9fa7',  # First live stream with girlfriend
)
BUDGET = 5_000_000
TOTAL_CAP = 150_000_000


def select(base):
    rows = {r['job_id']: r for r in base['recordings']}
    selected = []
    for job in SELECTED:
        row = rows[job]
        source = row['recording']
        if row['disposition'] != 'third_party' or not row.get('import'):
            raise ValueError('replacement requires the explicitly selected third-party source')
        if source['state'] != 'ready' and not (
                source['state'] == 'review' and source['reasons'] == ['source_id_maps_to_multiple_physical_recordings']):
            raise ValueError('replacement cannot bypass a media/admission failure')
        # This selects the exact existing preferred physical file, not every
        # encoding sharing its YouTube ID. The original decision is kept below.
        derived = deepcopy(source)
        derived.update(state='ready', reasons=[])
        selected.append((row, derived))
    maximum = base['prior_reserved_microusd'] + sum(r['maximum_cost_microusd'] for r in base['recordings'])
    if maximum + BUDGET > TOTAL_CAP:
        raise ValueError('replacement allocation exceeds the combined cloud cap')
    return selected, maximum


@contextmanager
def title_scope(cloud, plan_ref, policy_ref):
    cloud.titles.load_policy(policy_ref)
    previous = cloud.release.active_title_policy
    cloud.release.active_title_policy = lambda ref: policy_ref if ref == plan_ref else previous(ref)
    try:
        yield
    finally:
        cloud.release.active_title_policy = previous


def prepare(cloud, base_ref, root):
    io = cloud.io
    base = io.read(base_ref)
    selected, old_maximum = select(base)
    root = Path(root).resolve()
    for row, _ in selected:
        io.read(row['import']['transcript'])
        folder = Path(base['state_root']) / 'jobs' / row['job_id']
        if any((folder / name).exists() for name in ('intent.json', 'submission.json', 'completion.json', 'collection-review.json')):
            raise ValueError('selected source already has cloud paid evidence; no automatic repeat')
    cloud.private_root(root)
    io.mkdir(root / 'inputs')
    io.mkdir(root / 'explicit-replacement-no-imports')
    auth = dict(kind='himr_explicit_third_party_diarization_replacement', schema_version=1,
        original_plan=base_ref, original_maximum_microusd=old_maximum,
        allocation_microusd=BUDGET, combined_cap_microusd=TOTAL_CAP,
        approval='User requested diarised retranscription of the six shortlisted full recordings.',
        physical_selection='Exact physical recording already selected for its current third-party transcript.',
        evidence_limits='Screen/title/text candidates; no verified speaker count or identity inferred.',
        recordings=[dict(job_id=r['job_id'], original_recording=r['recording'],
            retained_third_party=r['import'], selected_recording=s) for r, s in selected],
        original_files_modified=False, blind_paid_retries=False)
    authorization = io.put(root / 'authorization.json', auth)
    inventory = io.put(root / 'inputs/inventory.json', dict(kind='himr_cloud_transcription_archive_inventory',
        schema_version=1, recordings=[s for _, s in selected], identity_conflicts=[], authorization=authorization))
    result = cloud.prepare(inventory, root / 'explicit-replacement-no-imports', root / 'transcription',
        screen_config_ref=base['screen_config'])
    ref = result['plan']
    plan = cloud.load_plan(ref)
    if any(r['disposition'] != 'cloud' for r in plan['recordings']):
        raise ValueError('selected replacement source was not admitted')
    policy = io.put(root / 'title-policy.json', cloud.titles.prepare_policy())
    with title_scope(cloud, ref, policy):
        for row in plan['recordings']:
            folder = cloud._folder(plan, row)
            io.mkdir(folder)
            io.put(folder / 'job.json', {'plan': ref, 'recording': row})
            if not (folder / 'screen.json').exists():
                decision = cloud.screen.lookup_completed(row['recording'], plan['screen_config'])
                if decision is None:
                    raise ValueError('selected replacement lacks retained screening evidence')
                io.put(folder / 'screen.json', decision)
            _, effective = cloud._screen_decision(plan, row, plan_ref=ref, persist=True)
            if not effective or effective['diarization'] is not True:
                raise ValueError('replacement must request diarization')
    maximum = sum(r['maximum_cost_microusd'] for r in plan['recordings'])
    if maximum > BUDGET:
        raise ValueError('six-recording reservation exceeds allocation')
    return io.put(root / 'runner-v2.json', dict(kind='himr_diarization_replacement_runner',
        authorization=authorization, plan=ref, title_policy=policy,
        maximum_cost_microusd=maximum, allocation_microusd=BUDGET,
        implementation=io.binding(__file__)))


def run(base, reference, env_file):
    cloud, io = base.cloud, base.cloud.io
    config = io.read(reference)
    io.read_bytes(config['implementation'])
    auth = io.read(config['authorization'])
    original = io.read(auth['original_plan'])
    selected, maximum = select(original)
    if (maximum != auth['original_maximum_microusd'] or config['allocation_microusd'] != BUDGET
            or auth['allocation_microusd'] != BUDGET or auth['combined_cap_microusd'] != TOTAL_CAP):
        raise ValueError('replacement budget authority changed')
    plan = cloud.load_plan(config['plan'])
    if [r['recording'] for r in plan['recordings']] != [s for _, s in selected]:
        raise ValueError('replacement selection changed')
    from pipeline import cloud_media_isolation_runner as isolation
    root = Path(reference['path']).parent
    with title_scope(cloud, config['plan'], config['title_policy']), cloud.pause_signal() as stopping:
        for row in plan['recordings']:
            _, screen = cloud._screen_decision(plan, row, plan_ref=config['plan'])
            if screen is None or screen['diarization'] is not True:
                raise ValueError('replacement diarization changed')
        with isolation.isolate(cloud, config['plan'], root / 'media-holds', io.binding(isolation.__file__), base._emit):
            result = base.run(config['plan'], allow_paid_api=True, budget_microusd=BUDGET,
                max_active=4, env_file=env_file, max_runtime_seconds=86400,
                poll_seconds=60, cooldown_seconds=300, stopping=stopping, on_event=base._emit)
        base._emit(result)
        return 2 if result['state'] in {'needs_review', 'reconciliation_required'} else 0


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('command', choices=['prepare', 'run'])
    p.add_argument('--runtime-path', required=True)
    p.add_argument('--extension-sha256', required=True)
    p.add_argument('--base-plan'); p.add_argument('--base-sha256'); p.add_argument('--output')
    p.add_argument('--manifest'); p.add_argument('--expected-sha256'); p.add_argument('--env-file')
    p.add_argument('--allow-paid-api', action='store_true')
    a = p.parse_args()
    if hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != a.extension_sha256:
        raise ValueError('replacement extension code changed')
    runtime = Path(a.runtime_path).resolve()
    sys.path.insert(0, str(runtime))
    from pipeline import cloud_transcription_resilient as base
    if Path(base.cloud.__file__).resolve().parent != runtime / 'pipeline':
        raise ValueError('unexpected cloud runtime')
    if a.command == 'prepare':
        print(json.dumps(prepare(base.cloud, dict(path=a.base_plan, sha256=a.base_sha256), a.output)))
        return 0
    if not a.allow_paid_api or not a.env_file:
        raise ValueError('paid run needs explicit approval and dotenv path')
    return run(base, dict(path=a.manifest, sha256=a.expected_sha256), a.env_file)


if __name__ == '__main__':
    raise SystemExit(main())
