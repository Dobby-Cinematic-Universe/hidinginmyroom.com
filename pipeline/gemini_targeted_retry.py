"""Finite, operator-approved retries over unchanged one-attempt summary plans.

An immutable authority binds the original worker, entries, plans, failed waves,
collections and exact jobs. Version one allows one extra INTERNAL-error attempt.
Version two adds explicit per-job attempt ceilings and narrowly proven output
repairs. Successful jobs and unknown POSTs never gain submission authority.
"""
from collections import Counter
from copy import deepcopy
from functools import partial
from pathlib import Path

KIND = 'himr_gemini_targeted_retry_authority'
REPAIR_ERROR = 'uncertainties require uncertainty classification'
RETRY_VALIDATION_ERRORS = frozenset({'invalid evidence reference count',
    'summary item cites missing or foreign evidence', 'invalid summary item text'})


class Authority:
    def __init__(self, worker, reference):
        self.worker, self.r, self.reference = worker, worker.r, deepcopy(reference)
        r = self.r
        value = r.read(reference)
        extended = value.get('schema_version') == 2
        fields = {'kind', 'schema_version', 'worker', 'approval',
            'selection_cutoff_utc', 'max_attempts_per_selected_job', 'provider_error_code',
            'grants', 'selected_job_count', 'maximum_additional_cost_microusd'}
        if extended:
            fields |= {'repairs', 'repaired_job_count'}
        r.safe.exact(value, fields, 'targeted retry authority')
        if (value['kind'] != KIND or type(value['schema_version']) is not int
                or value['schema_version'] not in {1, 2}
                or value['max_attempts_per_selected_job'] != (3 if extended else 2)
                or value['provider_error_code'] != 13 or not value['approval']
                or not isinstance(value['grants'], list) or not 1 <= len(value['grants']) <= (2048 if extended else 100)):
            raise r.Error('invalid finite retry authority')
        manifest = r.read(value['worker'])
        root = Path(manifest['state_root'])
        self.value, self.records, self.by_path, self.prepared = value, {}, {}, set()
        self.by_recording, self.limits, self.repairs, self.prior_failures = {}, {}, {}, {}
        seen, total = set(), 0
        for grant in value['grants']:
            r.safe.exact(grant, {'recording_id', 'entry', 'plan', 'wave', 'collection', 'jobs'}, 'retry grant')
            entry, plan = r.read(grant['entry']), r.read(grant['plan'])
            plan_root = Path(grant['plan']['path']).parent
            if (plan_root.parent != root / 'records' or Path(grant['plan']['path']).name != 'plan.json'
                    or Path(grant['entry']['path']) != root / 'entries' / (plan_root.name + '.json')
                    or entry['plan'] != grant['plan'] or entry['source']['recording_id'] != grant['recording_id']
                    or entry['request'] != plan['request']
                    or Path(plan['request_value']['state_root']) != plan_root
                    or plan['request_value']['budget']['max_attempts_per_job'] != 1):
                raise r.Error('retry grant escaped its original one-attempt worker plan')
            wave, collection = r.read(grant['wave']), r.read(grant['collection'])
            folder = plan_root / 'waves' / wave['wave_id']
            if (Path(grant['wave']['path']) != folder / 'wave.json'
                    or Path(grant['collection']['path']) != folder / 'collection.json'
                    or wave['plan_id'] != plan['plan_id'] or wave['retry_of'] is not None
                    or wave['provider'] != 'gemini' or collection['wave_id'] != wave['wave_id']
                    or Path(collection['capture']['path']) != folder / 'capture.json'):
                raise r.Error('retry must refer to an original collected Gemini wave')
            # Full receipt, capture, request and graph replay still happens in
            # load_state before any retry is prepared, submitted or collected.
            capture = r.read(collection['capture'])
            original = {j['job_id']: j for j in wave['jobs']}
            outcomes = {o['job_id']: o for o in collection['outcomes']}
            if not isinstance(grant['jobs'], list) or not 1 <= len(grant['jobs']) <= 64:
                raise r.Error('retry grant requires a bounded nonempty job selection')
            approved = {}
            for item in grant['jobs']:
                job_fields = {'job_id', 'job_sha256', 'maximum_cost_microusd'}
                if extended:
                    job_fields |= {'max_attempts', 'previous_failure'}
                r.safe.exact(item, job_fields, 'retry job')
                key = item['job_id']
                job, outcome = original.get(key), outcomes.get(key)
                if (key in seen or job is None or outcome is None
                        or outcome.get('state') == 'completed'
                        or not self.retryable(outcome, extended, batch=capture['batch'])
                        or job['provider'] != 'gemini' or job['stage'] not in {'chunk', 'transcript'}
                        or r.digest(job) != item['job_sha256']
                        or job['budget']['maximum_cost_microusd'] != item['maximum_cost_microusd']):
                    raise r.Error('retry selection is not an exact confirmed INTERNAL failure or approved validation failure')
                limit = item.get('max_attempts', 2)
                if type(limit) is not int or limit not in ({2, 3} if extended else {2}):
                    raise r.Error('invalid individual retry attempt ceiling')
                proof = item.get('previous_failure')
                if (limit == 3) != (proof is not None):
                    raise r.Error('third attempt requires a bound second-attempt failure')
                if proof is not None:
                    previous = r.read(proof)
                    prior_folder = plan_root / 'waves' / previous['wave_id']
                    prior_wave = r.read(r.binding(prior_folder / 'wave.json'))
                    previous_outcomes = {o['job_id']: o for o in previous['outcomes']}
                    if (Path(proof['path']) != prior_folder / 'collection.json'
                            or prior_wave['retry_of'] != wave['wave_id']
                            or job not in prior_wave['jobs']
                            or not self.retryable(previous_outcomes.get(key, {}), extended,
                                batch=r.read(previous['capture'])['batch'])):
                        raise r.Error('third-attempt proof is not this exact failed retry')
                    r.read(previous['capture'])
                    self.prior_failures[previous['wave_id']] = previous
                self.limits[key] = limit
                seen.add(key)
                approved[key] = deepcopy(job)
                total += item['maximum_cost_microusd']
            record = self.records.setdefault(plan['plan_id'], dict(plan=plan, entry=entry, grants={}))
            if (record['plan'] != plan or record['entry'] != entry or wave['wave_id'] in record['grants']):
                raise r.Error('duplicate or conflicting retry grant')
            record['grants'][wave['wave_id']] = dict(jobs=approved, collection=collection)
            self.by_path[grant['plan']['path']] = record
            self.by_recording[grant['recording_id']] = record
        if len(seen) != value['selected_job_count'] or total != value['maximum_additional_cost_microusd']:
            raise r.Error('retry authority job/cost totals differ')
        if extended:
            self.load_repairs(root, seen)

    @staticmethod
    def retryable(outcome, extended=False, *, batch=None):
        return (outcome.get('state') != 'completed' and (
            (outcome.get('failure') == 'provider_request_failed'
                and type(outcome.get('provider_error_code')) is int and outcome['provider_error_code'] == 13)
            or (extended and outcome.get('failure') == 'output_needs_review'
                and outcome.get('validation_error') in RETRY_VALIDATION_ERRORS)
            or (extended and outcome.get('failure') == 'missing_terminal_result'
                and isinstance(batch, dict) and batch.get('done') is True
                and type(batch.get('error', {}).get('code')) is int
                and batch['error']['code'] == 13
                and batch.get('metadata', {}).get('state') == 'BATCH_STATE_FAILED')))

    def load_repairs(self, root, retry_jobs):
        """Only promote the uncertainty section to the more cautious tag.

        Original model text, citations, captures and failed collections remain
        immutable. The same strict result validator must accept the correction.
        """
        from pipeline import transcript_summary_classification as classification
        r, seen = self.r, set()
        if not isinstance(self.value['repairs'], list) or len(self.value['repairs']) > 2048:
            raise r.Error('invalid bounded local repair selection')
        for grant in self.value['repairs']:
            r.safe.exact(grant, {'recording_id', 'entry', 'plan', 'wave', 'collection', 'jobs'}, 'local repair grant')
            entry, plan = r.read(grant['entry']), r.read(grant['plan'])
            wave, collection = r.read(grant['wave']), r.read(grant['collection'])
            plan_root = Path(grant['plan']['path']).parent
            folder = plan_root / 'waves' / wave['wave_id']
            if (plan_root.parent != root / 'records' or Path(grant['plan']['path']).name != 'plan.json'
                    or Path(grant['entry']['path']) != root / 'entries' / (plan_root.name + '.json')
                    or entry['plan'] != grant['plan'] or entry['request'] != plan['request']
                    or entry['source']['recording_id'] != grant['recording_id']
                    or Path(plan['request_value']['state_root']) != plan_root
                    or Path(grant['wave']['path']) != folder / 'wave.json'
                    or Path(grant['collection']['path']) != folder / 'collection.json'
                    or Path(collection['capture']['path']) != folder / 'capture.json'
                    or wave['plan_id'] != plan['plan_id']
                    or (wave['retry_of'] is not None and not self.permits(entry, wave))
                    or wave['provider'] != 'gemini' or collection['wave_id'] != wave['wave_id']
                    or wave.get('classification_policy') != classification.POLICY):
                raise r.Error('local repair escaped its original collected Gemini wave')
            capture = r.read(collection['capture'])
            if collection != {**r.collect_result(wave, capture), 'capture': collection['capture']}:
                raise r.Error('local repair failure differs from provider replay')
            jobs = {j['job_id']: j for j in wave['jobs']}
            outcomes = {o['job_id']: o for o in collection['outcomes']}
            items = {i['custom_id']: i for i in capture['items']}
            repaired = {}
            if not isinstance(grant['jobs'], list) or not 1 <= len(grant['jobs']) <= 64:
                raise r.Error('invalid bounded local repair jobs')
            for item in grant['jobs']:
                r.safe.exact(item, {'job_id', 'job_sha256'}, 'local repair job')
                key = item['job_id']
                job, outcome = jobs.get(key), outcomes.get(key)
                if (key in seen or (key in retry_jobs and wave['retry_of'] is None)
                        or job is None or outcome is None
                        or r.digest(job) != item['job_sha256'] or outcome['state'] == 'completed'
                        or outcome.get('validation_error') != REPAIR_ERROR):
                    raise r.Error('local repair is not an exact uncertainty classification failure')
                payload = r.response_payload('gemini', items[key]['response'])
                corrected = deepcopy(payload)
                changes = []
                for index, row in enumerate(corrected['uncertainties']):
                    if row['classification'] not in r.core.CLASSIFICATIONS:
                        raise r.Error('local repair cannot invent an unknown classification')
                    if row['classification'] != 'uncertainty':
                        changes.append(dict(section='uncertainties', item_index=index,
                            model_classification=row['classification'], inherited_classification='uncertainty'))
                        row['classification'] = 'uncertainty'
                if not changes:
                    raise r.Error('local repair must make a strictly more cautious correction')
                result, inherited = classification.normalize(job, corrected)
                repaired[key] = {**outcome, 'state': 'completed', 'result': result,
                    'failure': None, 'validation_error': None,
                    'classification_adjustments': changes + inherited}
                seen.add(key)
            record = self.records.setdefault(plan['plan_id'], dict(plan=plan, entry=entry, grants={}))
            if record['plan'] != plan or record['entry'] != entry or wave['wave_id'] in self.repairs:
                raise r.Error('duplicate or conflicting local repair grant')
            self.repairs[wave['wave_id']] = dict(plan_id=plan['plan_id'], collection=collection, outcomes=repaired)
            self.by_path[grant['plan']['path']] = record
            self.by_recording[grant['recording_id']] = record
        if len(seen) != self.value['repaired_job_count']:
            raise r.Error('local repair job count differs')

    def repaired_outcomes(self, plan, wave, collection):
        repair = self.repairs.get(wave['wave_id'])
        if repair is None:
            return collection['outcomes']
        if repair['plan_id'] != plan['plan_id'] or repair['collection'] != collection:
            raise self.r.Error('local output repair proof changed')
        return [deepcopy(repair['outcomes'].get(row['job_id'], row)) for row in collection['outcomes']]

    def record(self, plan):
        record = self.records.get(plan['plan_id'])
        if record is not None and record['plan'] != plan:
            raise self.r.Error('retry plan differs from immutable original')
        return record

    def entry_record(self, entry):
        record = self.by_path.get(entry['plan']['path'])
        if record is not None and record['entry'] != entry:
            raise self.r.Error('retry entry differs from immutable original')
        return record

    def permits(self, entry, wave):
        record = self.entry_record(entry)
        grant = None if record is None else record['grants'].get(wave['retry_of'])
        return bool(grant and wave['jobs'] and wave['plan_id'] == record['plan']['plan_id']
            and all(job == grant['jobs'].get(job['job_id']) for job in wave['jobs']))

    def check_state(self, record, state):
        for original, grant in record['grants'].items():
            if state['collections'].get(original) != grant['collection']:
                raise self.r.Error('retry failure proof changed')
        for wave in state['waves']:
            if (wave['wave_id'] in self.prior_failures
                    and state['collections'].get(wave['wave_id']) != self.prior_failures[wave['wave_id']]):
                raise self.r.Error('second-attempt failure proof changed')
            if wave['retry_of'] is not None and not self.permits(record['entry'], wave):
                raise self.r.Error('retry wave contains an unapproved original/job')
        if any(count > self.limits.get(key, 1) for key, count in state['attempts'].items()):
            raise self.r.Error('job exceeded its individual operator-approved attempt limit')

    def retained_initial(self, manifest, sources, config, original):
        """Seed only original chunks; retries must not duplicate cache coverage.

        This is limited to the explicitly approved records. The normal source
        coverage and subsequent full state replay still validate every chunk.
        """
        if len(sources) != 1 or sources[0]['recording_id'] not in self.by_recording:
            return original(manifest, sources, config)
        r = self.r
        record = self.by_recording[sources[0]['recording_id']]
        plan = record['plan']
        if (manifest != r.read(self.value['worker']) or config != manifest['config']
                or plan != self.worker.job_cache._plan(record['entry']['plan'])
                or plan['source_ids'] != [s['source_id'] for s in sources]
                or plan['source_bytes'] != sum(len(r.canonical(s)) for s in sources)):
            raise r.Error('retry initial cache source/configuration differs')
        root = Path(plan['request_value']['state_root'])
        if any(r.read(r.binding(root / 'sources' / (s['source_id'] + '.json'))) != s for s in sources):
            raise r.Error('retry initial cache normalized source differs')
        jobs = {}
        for path in (root / 'waves').glob('summarywave_*/wave.json'):
            wave = r.read(r.binding(path))
            if wave.get('retry_of') is not None:
                continue  # Full replay admits only exact approved retries.
            for job in wave['jobs']:
                if job['stage'] == 'chunk':
                    key = job['job_id']
                    if key in jobs or key not in plan['initial_job_ids']:
                        raise r.Error('original retry-cache chunk coverage repeats or differs')
                    jobs[key] = job
        if set(jobs) != set(plan['initial_job_ids']):
            return None  # The normal finite builder handles an incomplete frontier.
        initial = [jobs[key] for key in plan['initial_job_ids']]
        r.core.source_coverage(sources, initial)
        return initial

    def progress(self, record, state):
        counts = Counter()
        successful = {v['job_id'] for v in state['results']}
        for repair in self.repairs.values():
            if repair['plan_id'] == record['plan']['plan_id']:
                counts['locally_repaired_jobs'] += len(successful.intersection(repair['outcomes']))
        retried = {j['job_id']: wave for wave in state['waves'] if wave['retry_of'] is not None for j in wave['jobs']}
        for grant in record['grants'].values():
            for key in grant['jobs']:
                if key in successful:
                    counts['completed_jobs'] += 1
                    continue
                wave = retried.get(key)
                if wave is None:
                    counts['unprepared_jobs'] += 1
                    continue
                folder = self.r.wave_folder(record['plan'], wave['wave_id'])
                if wave['wave_id'] in state['collections']:
                    counts['failed_again_jobs'] += 1
                elif (folder / 'submitted.json').exists():
                    counts['pending_jobs'] += 1
                elif ((folder / 'submit-intent.json').exists()
                        or (Path(self.value['worker']['path']).parent / 'reservations' / (wave['wave_id'] + '.json')).exists()):
                    counts['ambiguous_jobs'] += 1
                else:
                    counts['prepared_jobs'] += 1
        return dict(counts)

    def prepare_pending(self, view, worker_ref, *, stopping=lambda: False):
        if worker_ref != self.value['worker']:
            raise self.r.Error('retry authority belongs to a different worker')
        changed = False
        for recording, row in view['records'].items():
            record = self.entry_record(row['entry'])
            if record is None or recording in view['identity_holds'] or stopping():
                continue
            ref = row['entry']['plan']
            for original in record['grants']:
                group = (ref['path'], original)
                if group in self.prepared:
                    continue
                result = self.r.prepare_plan(ref['path'], ref['sha256'], retry_wave=original, phase='transcripts')
                changed |= result['state'] == 'prepared'
                self.prepared.add(group)
        return changed

    def ordered(self, records):
        return sorted(records.items(), key=lambda item: self.entry_record(item[1]['entry']) is None)

    def report(self, view):
        counts = Counter()
        for row in view['records'].values():
            counts.update(row['status'].get('targeted_retry', {}))
        return dict(authority=self.reference, selected_jobs=self.value['selected_job_count'],
            selected_recordings=len(self.records), maximum_additional_cost_microusd=self.value['maximum_additional_cost_microusd'],
            automatic_retry_discovery=False, max_attempts_per_selected_job=self.value['max_attempts_per_selected_job'], **counts)


def install_core(worker, authority):
    """The effective attempt override exists only inside validated retry replay."""
    r = worker.r
    old_state, old_create = r.load_state, r.create_wave
    old_retained = worker.job_cache._retained
    replay_state = old_state
    if authority.repairs:
        from pipeline.gemini_dashboard_spend import variant
        if hasattr(r, '_approved_gemini_repair_outcomes'):
            raise r.Error('local output repairs already installed')
        replay_state = variant(old_state, [('for row in collection["outcomes"]:',
            'for row in _approved_gemini_repair_outcomes(plan, wave, collection):')])
        r._approved_gemini_repair_outcomes = authority.repaired_outcomes

    def effective(plan):
        value = deepcopy(plan)
        value['request_value']['budget']['max_attempts_per_job'] = authority.value['max_attempts_per_selected_job']
        return value

    def load_state(plan, sources):
        record = authority.record(plan)
        if record is None:
            return old_state(plan, sources)
        state = replay_state(effective(plan), sources)
        authority.check_state(record, state)
        return state

    def create_wave(plan, state, candidates, retry_of=None):
        if retry_of is None:
            return old_create(plan, state, candidates)
        record = authority.record(plan)
        grant = None if record is None else record['grants'].get(retry_of)
        if grant is None:
            raise r.Error('retry requires an exact operator-approved original wave')
        authority.check_state(record, state)
        selected = [job for job in candidates if job == grant['jobs'].get(job['job_id'])
                    and 1 <= state['attempts'].get(job['job_id'], 0) < authority.limits[job['job_id']]]
        return old_create(effective(plan), state, selected, retry_of=retry_of)

    r.load_state, r.create_wave = load_state, create_wave
    worker.job_cache._retained = lambda manifest, sources, config: authority.retained_initial(
        manifest, sources, config, old_retained)

    def restore():
        r.load_state, r.create_wave = old_state, old_create
        worker.job_cache._retained = old_retained
        if authority.repairs:
            del r._approved_gemini_repair_outcomes
    return restore


def initialize_unextended_collector(*args):
    """Resolve the original initializer in a fresh spawned interpreter."""
    from pipeline import cloud_transcription_summary_parallel as parallel
    parallel._initialize(*args)


def initialize_collector(extension_ref, authority_ref, previous_initializer, *args):
    """Spawn-safe extension of the reviewed-source initializer; no keys passed."""
    from pipeline import cloud_transcription_summary as worker
    r = worker.r
    if r.binding(__file__) != extension_ref:
        raise r.Error('targeted retry collector implementation changed')
    previous_initializer(*args)
    parallel = worker.parallel_collection
    try:
        authority = Authority(worker, authority_ref)
        if parallel._CHILD['worker_ref'] != authority.value['worker']:
            raise r.Error('retry collector worker differs')
        parallel._CHILD['stack'].callback(install_core(worker, authority))
    except BaseException:
        parallel._CHILD['stack'].close()
        raise


def install(worker, reference):
    authority = Authority(worker, reference)
    r = worker.r
    restore_core = install_core(worker, authority)
    old_record, old_initializer = worker._record_snapshot, worker.parallel_collection._initialize

    def record_snapshot(entry, expected):
        record = authority.entry_record(entry)
        if record is None:
            return old_record(entry, expected)
        plan, sources = r.load_plan(entry['plan']['path'], entry['plan']['sha256'])
        if plan['request'] != entry['request'] or plan['request_value'] != expected:
            raise worker.SummaryWorkerError('targeted retry record summary plan differs')
        state = r.load_state(plan, sources)
        progress = r.status_from_state(plan, sources, state, phase='transcripts')
        progress['targeted_retry'] = authority.progress(record, state)
        waves = []
        for wave in state['waves']:
            if (wave['provider'] != 'gemini' or any(j['stage'] not in {'chunk', 'transcript'} for j in wave['jobs'])
                    or (wave['retry_of'] is not None and not authority.permits(entry, wave))):
                raise worker.SummaryWorkerError('unapproved retry/provider/summary stage')
            waves.append({**{k: wave[k] for k in ('wave_id', 'input_sha256', 'maximum_cost_microusd')},
                'input_tokens': worker.admission.wave_input_tokens(wave['jobs']),
                'folder': str(r.wave_folder(plan, wave['wave_id']))})
        return dict(status=progress, accounted=worker.accounting.accounted_state(plan, state), waves=waves)

    worker._record_snapshot, worker._targeted_retry = record_snapshot, authority
    previous_initializer = (initialize_unextended_collector
        if getattr(old_initializer, '__name__', None) == '_initialize' else old_initializer)
    worker.parallel_collection._initialize = partial(initialize_collector,
        r.binding(__file__), authority.reference, previous_initializer)

    def restore():
        restore_core()
        worker._record_snapshot, worker.parallel_collection._initialize = old_record, old_initializer
        del worker._targeted_retry
    return authority, restore
