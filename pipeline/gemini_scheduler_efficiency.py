"""Opt-in scheduler repair over the unchanged, hash-pinned Gemini runtime.

Reuse a cycle's source/identity selection, skip pending dependency graphs, and
read already-sealed candidate waves directly. The original submitter still
validates every paid request; reservations and unresolved holds are unchanged.
"""
from copy import deepcopy
import math
from pathlib import Path
import time

from pipeline import gemini_queue_tokens as queue
from pipeline import reviewed_transcript_feed as feed


def candidate_wave(worker, entry, wave_id, expected=None):
    r = worker.r
    path = Path(entry['plan']['path']).parent / 'waves' / wave_id / 'wave.json'
    wave = r.read(r.binding(path))
    authority = getattr(worker, '_targeted_retry', None)
    approved_retry = authority is not None and authority.permits(entry, wave)
    if (wave.get('wave_id') != wave_id
            or wave_id != 'summarywave_' + r.digest({k: v for k, v in wave.items() if k != 'wave_id'})[:32]
            or wave.get('provider') != 'gemini' or (wave.get('retry_of') is not None and not approved_retry)
            or any(j['provider'] != 'gemini' or j['stage'] not in {'chunk', 'transcript'} for j in wave['jobs'])
            or wave['maximum_cost_microusd'] != sum(j['budget']['maximum_cost_microusd'] for j in wave['jobs'])):
        raise worker.SummaryWorkerError('sealed candidate identity/provider/cost differs')
    if expected is not None and any(wave[k] != expected[k] for k in ('wave_id', 'input_sha256', 'maximum_cost_microusd')):
        raise worker.SummaryWorkerError('candidate differs from validated snapshot')
    return wave


def prepare_candidate(worker, row, snapshot):
    progress, entry = row['status'], row['entry']
    if progress['prepared_waves']:
        wave_id = progress['prepared_waves'][0]
        return candidate_wave(worker, entry, wave_id, snapshot['waves'][wave_id][2]), False
    if not progress['ready_jobs']:
        return None, False  # Pending/ambiguous dependencies cannot make progress locally.
    ref = entry['plan']
    prepared = worker.r.prepare_plan(ref['path'], ref['sha256'], phase='transcripts')
    if not prepared.get('wave_id'):
        return None, False
    return candidate_wave(worker, entry, prepared['wave_id']), True


def install(worker, cache_root, *, env_file=None, allow_count_api=False, queue_policy=queue.POLICY,
            queue_tier='tier1'):
    r, admission = worker.r, worker.admission
    provider_token_limit, counted_ceiling = queue.tier_limits(queue_tier)
    if queue_tier != 'tier1' and queue_policy != queue.COUNTED_POLICY:
        raise ValueError('higher queue tiers require counted admission')
    # Explicit launcher opt-in; retained manifests and financial bounds do not
    # change. The pure admission validator reads this process-local ceiling.
    admission.DEFAULT_TOKEN_LIMIT = provider_token_limit
    root = Path(cache_root)
    r.mkdir(root)
    api = None
    if allow_count_api:
        key = r.env_module.api_key('GEMINI_API_KEY', default_path=r.ROOT / '.env', env_file=env_file)
        api = queue.CountClient(key, timeout_seconds=30)
    counter = queue.Counter(root / 'queue-tokens-v1', api, policy=queue_policy)
    queue_limit = (min(admission.DEFAULT_TOKEN_LIMIT, counted_ceiling)
        if counter.policy == queue.COUNTED_POLICY else admission.DEFAULT_TOKEN_LIMIT)
    original_available, original_gate, original_snapshot = worker._available, worker._identity_holds, worker._snapshot
    cycle_cache = [None]
    metrics = dict(source_selection_calls=0, identity_gate_calls=0, snapshot_calls=0,
        pending_preparations_skipped=0, sealed_candidates_reused=0)

    def available(manifest):
        cache = cycle_cache[0]
        if cache is None:
            return original_available(manifest)
        if 'available' not in cache:
            metrics['source_selection_calls'] += 1
            cache['available'] = original_available(manifest)
        return deepcopy(cache['available'])

    def gate(selected):
        cache = cycle_cache[0]
        if cache is None:
            return original_gate(selected)
        identity = r.digest(selected)
        if cache.get('identity_key') != identity:
            metrics['identity_gate_calls'] += 1
            cache['identity_key'], cache['identity_holds'] = identity, original_gate(selected)
        return deepcopy(cache['identity_holds'])

    def snapshot(manifest, ref):
        metrics['snapshot_calls'] += 1
        value = original_snapshot(manifest, ref)
        wanted = {row['wave_id'] for row in worker._active_waves(value)}
        wanted.update(wave for row in value['records'].values() for wave in row['status']['prepared_waves'])
        waves = {wave_id: candidate_wave(worker, value['waves'][wave_id][0], wave_id,
            value['waves'][wave_id][2]) for wave_id in wanted}
        # Only active/unknown/prepared waves are counted. Do not recount the
        # archive's completed requests or send their prompts to the API again.
        counter.ensure([job for wave in waves.values() for job in wave['jobs']])
        for wave_id, wave in waves.items():
            value['waves'][wave_id][2].update(counter.estimate(wave['jobs']))
        return value

    worker._available, worker._identity_holds, worker._snapshot = available, gate, snapshot

    def cycle(worker_ref, *, allow_paid_api=False, max_active=4, max_new_waves=4,
              env_file=None, client=None, stopping=lambda: False,
              max_enqueued_tokens=queue_limit, adaptive_state=None):
        if not allow_paid_api:
            raise worker.SummaryWorkerError('summary run requires explicit --allow-paid-api')
        for value, maximum, name in ((max_active, admission.MAX_ACTIVE, 'active waves'),
                (max_new_waves, admission.MAX_ACTIVE, 'new waves'),
                (max_enqueued_tokens, queue_limit, 'queue token ceiling')):
            r.safe.integer(value, 0 if name == 'new waves' else 1, maximum, name)
        if adaptive_state is not None:
            admission.advance(adaptive_state, now_seconds=int(time.monotonic()))
            if adaptive_state['max_active'] != max_active:
                raise worker.SummaryWorkerError('adaptive concurrency ceiling differs')
        started = time.monotonic()
        before = dict(metrics)
        manifest = worker.load_manifest(worker_ref)
        state_root = Path(manifest['state_root'])
        target = max_active if adaptive_state is None else adaptive_state['target']
        cooldown = 0 if adaptive_state is None else adaptive_state['cooldown_until_seconds']
        submitted = attempted = polled = 0
        errors, blocked = [], []
        throttled = modified = False
        collection_report = dict(groups_completed=0, workers=1, events=[])

        def throttle():
            nonlocal target, cooldown, throttled
            limited = [event for event in errors if event.get('status_code') == 429]
            if not limited or throttled:
                return
            throttled = True
            delay = max((event.get('retry_after_seconds') or 0 for event in limited), default=0)
            now = int(time.monotonic())
            if adaptive_state is not None:
                updated = admission.advance(adaptive_state, now_seconds=now, rate_limited=True,
                    retry_after_seconds=math.ceil(delay))
                adaptive_state.clear()
                adaptive_state.update(updated)
                target, cooldown = updated['target'], updated['cooldown_until_seconds']
            else:
                cooldown = now + max(60, math.ceil(delay))

        cycle_cache[0] = {}
        try:
            with r.locked(state_root):
                view = snapshot(manifest, worker_ref)
                authority = getattr(worker, '_targeted_retry', None)
                if authority is not None and authority.prepare_pending(view, worker_ref, stopping=stopping):
                    modified = True
                    view = snapshot(manifest, worker_ref)
                groups = [dict(plan=row['entry']['plan'], waves=list(row['status']['pending_waves']))
                    for row in view['records'].values() if row['status']['pending_waves']]
                collection_start = time.monotonic()
                if client is None and worker.parallel_collection.active():
                    collection_report = worker.parallel_collection.poll_groups(groups, stopping=stopping)
                    errors.extend(collection_report['transport_events'])
                    polled = sum(e.get('state') in {'remote_pending', 'collected'} for e in collection_report['events'])
                else:
                    for group in groups:
                        for wave_id in group['waves']:
                            if stopping():
                                break
                            ref = group['plan']
                            try:
                                result = r.poll_wave(ref['path'], ref['sha256'], wave_id, client=client, env_file=env_file)
                                polled += result.get('state') in {'remote_pending', 'collected'}
                            except r.client_module.BatchClientError as error:
                                errors.append(worker._transport_event(error, wave_id, 'poll'))
                collection_seconds = time.monotonic() - collection_start
                throttle()
                # Polling can publish captures before returning an error, so
                # always refresh after any collection work has been attempted.
                if groups:
                    view = snapshot(manifest, worker_ref)
                deferred = worker._permanent_candidate_holds(manifest, view, max_enqueued_tokens)
                blocked.extend(deferred.values())
                active_records = sum(not row['status']['transcript_phase_complete']
                    and recording not in deferred and row['status']['state'] != 'needs_review'
                    and (recording not in view['identity_holds'] or bool(row['status']['pending_waves'])
                         or bool(row['status']['ambiguous_waves'])) for recording, row in view['records'].items())
                added = False
                if max_new_waves and not stopping() and int(time.monotonic()) >= cooldown:
                    for recording, source in view['available'].items():
                        if active_records >= target or stopping():
                            break
                        if recording not in view['records'] and recording not in view['identity_holds']:
                            worker._ensure_record(manifest, source)
                            active_records += 1
                            added = True
                if added:
                    view = snapshot(manifest, worker_ref)
                active = worker._active_waves(view)
                accounted, settled = view['accounted_microusd'], view['usage_estimate_microusd']
                ordered = view['records'].items() if authority is None else authority.ordered(view['records'])
                for recording, row in ordered:
                    if stopping():
                        break
                    progress, entry = row['status'], row['entry']
                    ref = entry['plan']
                    if recording in view['identity_holds']:
                        continue
                    if progress['transcript_phase_complete']:
                        worker.job_cache.cached_export(entry, lambda: r.export_plan(ref['path'], ref['sha256'], phase='transcripts'))
                        continue
                    if (recording in deferred or attempted >= max_new_waves or len(active) >= target
                            or int(time.monotonic()) < cooldown
                            or progress['state'] in {'needs_review', 'needs_reconciliation'}):
                        continue
                    wave, created = prepare_candidate(worker, row, view)
                    modified |= created
                    if wave is None:
                        metrics['pending_preparations_skipped'] += 1
                        continue
                    metrics['sealed_candidates_reused'] += not created
                    wave_id = wave['wave_id']
                    if any(value['wave_id'] == wave_id for value in active):
                        continue
                    reservation = worker._reservation(worker_ref, entry, wave)
                    ledger = state_root / 'reservations' / (wave_id + '.json')
                    if r.safe.exists(ledger):
                        if r.read(r.binding(ledger)) != reservation:
                            raise worker.SummaryWorkerError('existing global wave reservation differs')
                        blocked.append(dict(wave_id=wave_id, reasons=['existing_reservation_requires_review']))
                        continue
                    counter.ensure(wave['jobs'])
                    input_tokens = counter.estimate(wave['jobs'])['input_tokens']
                    decision = admission.assess_admission(active_waves=active,
                        candidate_input_tokens=input_tokens, candidate_cost_microusd=wave['maximum_cost_microusd'],
                        settled_microusd=settled, held_microusd=accounted - settled,
                        budget_limit_microusd=manifest['max_total_budget_microusd'], target=target,
                        token_limit=max_enqueued_tokens, now_seconds=int(time.monotonic()), cooldown_until_seconds=cooldown)
                    if not decision['allowed']:
                        blocked.append(dict(wave_id=wave_id, recording_id=recording, **decision))
                        continue
                    if stopping():
                        break  # Do not create an orphan reservation on graceful stop.
                    # Original cost and durable ledger precede the unchanged
                    # full-validating paid submitter. Never retry an intent.
                    r.put(ledger, reservation)
                    modified = True
                    accounted += wave['maximum_cost_microusd']
                    # Once reserved, finish the intent/submission boundary even
                    # if shutdown is requested. Existing intents never replay.
                    try:
                        result = r.submit_wave(ref['path'], ref['sha256'], wave_id, allow_paid_api=True,
                            client=client, env_file=env_file, phase='transcripts')
                        if result['state'] == 'submitted':
                            submitted += 1
                            attempted += 1
                        active.append(dict(wave_id=wave_id, input_tokens=input_tokens,
                            state='pending' if result['state'] in {'submitted', 'already_submitted'} else 'ambiguous'))
                    except r.client_module.BatchClientError as error:
                        attempted += 1
                        active.append(dict(wave_id=wave_id, input_tokens=input_tokens, state='ambiguous'))
                        errors.append(worker._transport_event(error, wave_id, 'submit'))
                        throttle()
                final = snapshot(manifest, worker_ref) if modified else view
                public = worker._public(manifest, final)
                final_deferred = worker._permanent_candidate_holds(manifest, final, max_enqueued_tokens)
                budget_blocked = any(any(reason in value['reasons'] for reason in
                    ('budget_limit', 'candidate_settled_budget_ceiling')) for value in blocked)
                unknown_pressure = any(value.get('reconciliation_required') for value in blocked)
                state = ('paused' if stopping() else 'waiting_rate_limit' if int(time.monotonic()) < cooldown
                    else 'waiting_remote' if public['pending_waves'] else 'needs_review' if unknown_pressure
                    else 'needs_review' if public['holds'] and public['potential_active_waves'] >= target
                    else 'ready' if final_deferred and public['waiting_for_summary_admission']
                    else 'budget_paused' if budget_blocked else 'needs_review' if blocked
                    else 'ready' if public['waiting_for_summary_admission'] else 'needs_review' if public['holds']
                    else 'waiting_for_speaker_identity' if public['speaker_identity_pending']
                    else 'waiting_for_preferred_transcripts')
                if adaptive_state is not None and not throttled:
                    updated = admission.advance(adaptive_state, now_seconds=int(time.monotonic()),
                        successful_cycle=not errors and (polled > 0 or submitted > 0))
                    adaptive_state.clear()
                    adaptive_state.update(updated)
                current = worker._active_waves(final)
                active_metadata = [final['waves'][v['wave_id']][2] for v in current]
                result = dict(public, state=state, new_paid_requests=attempted,
                    confirmed_new_submissions=submitted, transport_events=errors, admission_holds=blocked,
                    adaptive_concurrency=deepcopy(adaptive_state), active_target_used=target,
                    operator_enqueued_token_ceiling=max_enqueued_tokens, account_quota_verified=False,
                    collection_seconds=round(collection_seconds, 3), cycle_seconds=round(time.monotonic() - started, 3),
                    enqueued_input_token_estimate=public['enqueued_input_token_allowance'],
                    queue_token_accounting=dict(policy=counter.policy,
                        counted_jobs=sum(w.get('counted_jobs', 0) for w in active_metadata),
                        fallback_jobs=sum(w.get('fallback_jobs', 0) for w in active_metadata),
                        counted_input_tokens=sum(w.get('counted_input_tokens', 0) for w in active_metadata),
                        fallback_input_token_allowance=sum(w.get('fallback_input_token_allowance', 0) for w in active_metadata),
                        per_request_headroom_tokens=sum(w.get('per_request_headroom_tokens', 0) for w in active_metadata),
                        operator_declared_tier=queue_tier,
                        provider_batch_token_limit=provider_token_limit,
                        tier1_batch_token_limit=queue.TIER1_TOKEN_LIMIT,
                        operator_reserve_tokens=provider_token_limit - max_enqueued_tokens,
                        provider_reported_occupancy=False,
                        confirmed_pending_input_token_estimate=sum(final['waves'][v['wave_id']][2]['input_tokens']
                            for v in current if v['state'] == 'pending'),
                        unresolved_submission_token_estimate=sum(final['waves'][v['wave_id']][2]['input_tokens']
                            for v in current if v['state'] != 'pending'),
                        financial_input_token_allowance=sum(w.get('financial_input_token_allowance', w['input_tokens']) for w in active_metadata),
                        cache=deepcopy(counter.metrics), financial_reservations_changed=False),
                    local_processing={k: metrics[k] - before[k] for k in metrics},
                    initial_job_reuse=worker.job_cache.statistics(), parallel_collection=worker.parallel_collection.statistics())
                if authority is not None:
                    result['targeted_recovery'] = authority.report(final)
                feed.atomic(root / 'status.json', result)
                # Preserve the worker's existing return contract. The atomic
                # status file avoids relying on split journal lines for metrics.
                return {**result,
                    'hold_count': len(result['holds']), 'admission_hold_count': len(blocked),
                    'detail_status_path': str(root / 'status.json')}
        finally:
            cycle_cache[0] = None

    worker.cycle = cycle
    return counter
