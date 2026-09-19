"""Disposable queue counts, separate from immutable financial reservations.

Only Google's countTokens endpoint is reachable here. No generation, cancellation,
paid retry, or ledger mutation is possible. Unavailable counts fall back to the
original byte-based allowance, including for ambiguous/orphan submissions.
"""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
import re
import threading
import time
from pathlib import Path

from pipeline import transcript_summary_client as client
from pipeline import transcript_summary as io

KIND = 'himr_gemini_queue_token_count_v1'
POLICY = 'google_count_tokens_plus_10_percent_and_128_tokens_v1'
COUNTED_POLICY = 'google_count_tokens_with_five_percent_queue_reserve_v2'
TIER1_TOKEN_LIMIT = 3_000_000
COUNTED_TOKEN_CEILING = 2_850_000
TIER2_TOKEN_LIMIT = 400_000_000
TIER2_TOKEN_CEILING = 380_000_000
MAX_ENTRIES = 50000


def tier_limits(tier):
    if tier == 'tier1':
        return TIER1_TOKEN_LIMIT, COUNTED_TOKEN_CEILING
    if tier == 'tier2':
        return TIER2_TOKEN_LIMIT, TIER2_TOKEN_CEILING
    raise ValueError('unsupported operator-declared Gemini tier')


def request_value(job):
    if job.get('provider') != 'gemini' or not re.fullmatch(r'gemini-[a-z0-9][a-z0-9.-]{0,126}', job['model']):
        raise ValueError('queue counting requires a Gemini job')
    body = deepcopy(job['request']['body'])
    # Storage is a transport control, not model input. Count the complete
    # GenerateContentRequest, including system instructions and output schema.
    if body.pop('store', False) is not False:
        raise ValueError('queue counting cannot enable storage')
    body['model'] = 'models/' + job['model']
    return {'generateContentRequest': body}


class CountClient(client.GeminiBatchClient):
    def _destination(self, method, path):
        if method != 'POST' or not re.fullmatch(r'/v1beta/models/gemini-[a-z0-9][a-z0-9.-]{0,126}:countTokens', path):
            raise client.BatchClientError('only Gemini countTokens is permitted')
        return 'https://generativelanguage.googleapis.com' + path

    def _response_limit(self, raw):
        if raw:
            raise client.BatchClientError('raw token-count downloads are forbidden')
        return 65536

    def count(self, job):
        return self._request('POST', '/v1beta/models/' + job['model'] + ':countTokens',
            data=io.canonical(request_value(job)), content_type='application/json')


class Counter:
    def __init__(self, root, api=None, *, workers=8, policy=POLICY):
        if policy not in {POLICY, COUNTED_POLICY}:
            raise ValueError('unsupported queue token policy')
        self.root, self.api = Path(root), api
        if not 1 <= workers <= 8:
            raise ValueError('bounded token counter concurrency required')
        io.mkdir(self.root)
        self.workers = workers
        self.policy = policy
        self.memory = {}
        self.missing_until = {}
        self.cooldown_until = 0
        self.lock = threading.Lock()
        self.metrics = dict(provider_counts=0, disk_hits=0, failures=0, cache_hits=0)

    def identity(self, job):
        return dict(kind=KIND, model=job['model'], request_sha256=hashlib.sha256(io.canonical(request_value(job))).hexdigest())

    def key(self, identity):
        return io.digest(identity)

    def lookup(self, job):
        identity = self.identity(job)
        key = self.key(identity)
        with self.lock:
            if key in self.memory:
                self.metrics['cache_hits'] += 1
                return self.memory[key]
        path = self.root / (key + '.json')
        if not path.exists():
            return None
        try:
            value = io.read(io.binding(path))
            total = value['response']['totalTokens']
            if value['identity'] != identity or type(total) is not int or not 0 < total <= 2_000_000:
                raise ValueError('invalid retained token count')
        except (OSError, ValueError, KeyError, io.Error):
            # A corrupt optional cache never releases a reservation or becomes
            # zero tokens. Leave it intact for inspection and use the old bound.
            return None
        with self.lock:
            if len(self.memory) < MAX_ENTRIES:
                self.memory[key] = total
            self.metrics['disk_hits'] += 1
        return total

    def _fetch(self, job):
        if self.lookup(job) is not None or self.api is None:
            return
        identity = self.identity(job)
        key = self.key(identity)
        with self.lock:
            now = time.monotonic()
            if now < self.cooldown_until or now < self.missing_until.get(key, 0):
                return
            if len(self.memory) >= MAX_ENTRIES:
                return
        try:
            response = self.api.count(job)
            total = response.get('totalTokens')
            if type(total) is not int or not 0 < total <= 2_000_000:
                raise ValueError('invalid provider token count')
            io.put(self.root / (key + '.json'), dict(identity=identity, response=response))
            with self.lock:
                self.memory[key] = total
                self.metrics['provider_counts'] += 1
        except (client.BatchClientError, OSError, ValueError, io.Error) as error:
            with self.lock:
                self.metrics['failures'] += 1
                self.missing_until[key] = time.monotonic() + 300
                if getattr(error, 'status_code', None) == 429:
                    self.cooldown_until = time.monotonic() + max(60, getattr(error, 'retry_after_seconds', None) or 0)

    def ensure(self, jobs):
        unique = {self.key(self.identity(job)): job for job in jobs}
        missing = [job for job in unique.values() if self.lookup(job) is None]
        if missing and self.api is not None:
            with ThreadPoolExecutor(max_workers=self.workers) as pool:
                list(pool.map(self._fetch, missing))

    def estimate(self, jobs):
        if not jobs:
            raise ValueError('cannot count an empty wave')
        total = counted = fallback = allowance = tokenized = fallback_tokens = headroom = 0
        for job in jobs:
            bound = job['budget']['input_token_allowance']
            if type(bound) is not int or bound <= 0:
                raise ValueError('invalid original financial token allowance')
            allowance += bound
            value = self.lookup(job)
            if value is None:
                total += bound
                fallback += 1
                fallback_tokens += bound
            else:
                # The opt-in policy reserves headroom once at the queue ceiling,
                # not again on every input. Cache keys and financial bounds stay
                # identical across policies, so switching never recounts/rebills.
                padding = (value * 110 + 99) // 100 + 128 - value if self.policy == POLICY else 0
                total += value + padding
                tokenized += value
                headroom += padding
                counted += 1
        return dict(input_tokens=total, counted_jobs=counted, fallback_jobs=fallback,
            counted_input_tokens=tokenized, fallback_input_token_allowance=fallback_tokens,
            per_request_headroom_tokens=headroom,
            financial_input_token_allowance=allowance, policy=self.policy, exact=False)
