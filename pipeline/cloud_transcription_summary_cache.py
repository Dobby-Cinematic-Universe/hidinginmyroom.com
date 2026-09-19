"""Scoped, bounded reuse of exact initial jobs for the cloud summary worker.

Retained jobs are authenticated by an explicitly bound existing record plan,
the current normalized source, exact configuration, source character coverage,
and reconstructed job/request identities. Greedy chunk boundaries do not need
rediscovery merely to inspect an already sealed plan. The original plan, wave,
receipt, result and spending validators still run unchanged.

This module never modifies existing artifacts, submits API work, retries a job,
or changes source text. Only the calling context uses the temporary initial_jobs
dispatch; other contexts retain the original behavior. Cached values are bounded
serialized copies, not mutable references supplied to callers.
"""
from __future__ import annotations

from collections import OrderedDict
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
import hashlib
import os
from pathlib import Path
import stat
import threading

from pipeline import transcript_summary as r

core = r.core
_ORIGINAL_INITIAL_JOBS = core.initial_jobs
_ACTIVE = ContextVar("himr_cloud_summary_initial_job_cache", default=None)
_SCOPE_LOCK = threading.Lock()
MAX_CACHE_BYTES = 256 * 1024**2
MAX_CACHE_ENTRIES = 4096
MAX_RECORD_CACHE_BYTES = 32 * 1024**2
MAX_EXPORT_CACHE_BYTES = 32 * 1024**2
MAX_RECORD_FILES = 8192
MAX_RECORD_DIRECTORIES = 1024
MAX_RECORD_DEPTH = 8
WORKER_KIND = "himr_cloud_transcript_summary_worker"


class CacheError(RuntimeError):
    pass


def _record_witness(root):
    """Bounded metadata-only inventory; never follow links or read bodies."""
    result = []
    files = directories = 0
    fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns", "st_mode", "st_uid", "st_nlink")

    def visit(path, depth):
        nonlocal files, directories
        if depth > MAX_RECORD_DEPTH:
            raise CacheError("record snapshot directory depth exceeds its bound")
        with r.paths.retained_directory(path) as directory:
            info = os.fstat(directory)
            if info.st_uid not in {0, os.getuid()} or info.st_mode & 0o022:
                raise CacheError("record snapshot directory is not safely owned")
            directories += 1
            if directories > MAX_RECORD_DIRECTORIES:
                raise CacheError("record snapshot directory count exceeds its bound")
            result.append([str(path), *(getattr(info, field) for field in fields)])
            with os.scandir(directory) as entries:
                children = []
                for entry in entries:
                    children.append((entry.name, entry.stat(follow_symlinks=False)))
                    if len(children) > MAX_RECORD_FILES + MAX_RECORD_DIRECTORIES:
                        raise CacheError("record snapshot directory listing exceeds its bound")
                children.sort()
            for name, info in children:
                child = path / name
                if stat.S_ISDIR(info.st_mode):
                    visit(child, depth + 1)
                elif (stat.S_ISREG(info.st_mode) and info.st_nlink == 1
                      and info.st_uid in {0, os.getuid()} and not info.st_mode & 0o022):
                    files += 1
                    if files > MAX_RECORD_FILES:
                        raise CacheError("record snapshot file count exceeds its bound")
                    result.append([str(child), *(getattr(info, field) for field in fields)])
                else:
                    raise CacheError("record snapshot contains an unsafe link or nonregular entry")
    visit(root, 0)
    return result


def _core_sha256():
    return hashlib.sha256(Path(core.__file__).read_bytes()).hexdigest()


def _select_builder():
    # Import BEFORE installing our dispatch. The fast module retains an original
    # unsupported-contract fallback, which must not capture this wrapper.
    try:
        from pipeline import transcript_summary_fast_initial as fast
    except ModuleNotFoundError as error:
        if error.name != "pipeline.transcript_summary_fast_initial":
            raise
        return _ORIGINAL_INITIAL_JOBS
    return fast.initial_jobs


def _key(sources, config, implementation_sha256):
    contract = {"config": config, "instructions": core.INSTRUCTIONS,
                "gemini_output_contract": core.GEMINI_OUTPUT_CONTRACT, "profiles": core.PROFILES,
                "schema": core.response_schema(config),
                "gemini_response_schema": core._gemini_schema(core.response_schema(config)),
                "pricing_valid_until": core.PRICING_VALID_UNTIL,
                "core_sha256": implementation_sha256}
    return hashlib.sha256(core.canonical(sources) + b"\0" + core.canonical(contract)).digest()


def _worker(reference):
    value = r.read(reference)
    from pipeline import cloud_transcription_summary as worker
    r.safe.exact(value, {"kind", "schema_version", "cloud_plan", "state_root", "config",
                         "max_total_budget_microusd", "implementation", "policy"}, "cached summary worker")
    if (value["kind"] != WORKER_KIND or type(value["schema_version"]) is not int or value["schema_version"] != 1
            or value["policy"] != worker.POLICY or core.normalize_config(value["config"]) != value["config"]
            or value["config"]["transcript_profile"] != "gemini_flash_batch"
            or "broader_synthesis" in value["config"]):
        raise CacheError("initial-job cache requires the exact cloud transcript worker contract")
    root = r.safe.path_value(value["state_root"])
    if reference["path"] != str(root / "manifest.json") or r.read(r.binding(root / "workspace.json")) != value:
        raise CacheError("cached worker manifest or original workspace marker differs")
    r.safe.integer(value["max_total_budget_microusd"], 1, 10_000_000_000, "summary budget")
    return value


def _plan(reference):
    """Authenticate the normal plan without calling its chunk rediscovery step."""
    value = r.read(reference)
    r.safe.exact(value, {"kind", "schema_version", "request", "request_value", "source_ids", "source_bytes",
                         "initial_job_ids", "implementation", "semantics", "plan_id"}, "retained summary plan")
    body = {key: item for key, item in value.items() if key != "plan_id"}
    if (value["kind"] != "himr_transcript_summary_plan" or type(value["schema_version"]) is not int
            or value["schema_version"] != 1 or value["plan_id"] != "summaryplan_" + r.digest(body)[:32]
            or value["implementation"] != r.implementation() or value["semantics"] != r.SEMANTICS):
        raise CacheError("retained initial jobs require an unchanged exact summary plan")
    request = r.validate_request(value["request_value"])
    if request != r.read(value["request"]) or reference["path"] != str(Path(request["state_root"]) / "plan.json"):
        raise CacheError("retained summary request or plan location changed")
    root = Path(request["state_root"])
    if r.read(r.binding(root / "workspace.json")) != r.MARKER:
        raise CacheError("retained summary record marker differs")
    ids = value["initial_job_ids"]
    if (not isinstance(ids, list) or len(ids) > request["limits"]["max_jobs"]
            or any(not isinstance(item, str) for item in ids) or len(set(ids)) != len(ids)):
        raise CacheError("retained plan initial job identities are malformed or duplicate")
    return value


def _partial_evidence(sources, jobs):
    """Even an incomplete retained frontier must not hide corrupt evidence."""
    index = {(source["source_id"], segment["evidence_id"]): (source, segment)
             for source in sources for segment in source["segments"]}
    for job in jobs:
        core.validate_job(job)
        for evidence in job["evidence"]:
            if len(evidence["citations"]) != 1:
                raise CacheError("retained chunk has non-source evidence")
            citation = evidence["citations"][0]
            found = index.get((citation["source_id"], citation["evidence_id"]))
            if found is None:
                raise CacheError("retained chunk cites an unselected source")
            source, segment = found
            start, end = citation["char_start"], citation["char_end"]
            if not 0 <= start < end <= len(segment["text"]) or core._raw_evidence(source, segment, start, end) != evidence:
                raise CacheError("retained chunk source text, timing or evidence identity differs")


def _retained(manifest, sources, config):
    if len(sources) != 1 or config != manifest["config"] or sources[0]["format"] not in {"third_party", "cloud"}:
        return None
    from pipeline import cloud_transcription_summary as worker
    source = sources[0]
    name = worker._record_key(source["recording_id"])
    root = Path(manifest["state_root"])
    entry_path = root / "entries" / (name + ".json")
    if not r.safe.exists(entry_path):
        return None  # New recording: no retained job frontier exists yet.
    entry = r.read(r.binding(entry_path))
    r.safe.exact(entry, {"kind", "schema_version", "source", "request", "plan"}, "retained worker entry")
    if (entry["kind"] != WORKER_KIND + "_record" or type(entry["schema_version"]) is not int
            or entry["schema_version"] != 1 or entry["source"].get("recording_id") != source["recording_id"]
            or entry["source"].get("format") != source["format"]
            or entry["source"].get("transcript") != source["transcript"]
            or entry["plan"]["path"] != str(root / "records" / name / "plan.json")):
        raise CacheError("retained worker entry changed its source or record workspace")
    plan = _plan(entry["plan"])
    expected = worker._request(manifest, entry["source"])
    if (plan["request"] != entry["request"] or plan["request_value"] != expected
            or expected["config"] != config or plan["source_ids"] != [source["source_id"]]
            or plan["source_bytes"] != sum(len(r.canonical(item)) for item in sources)):
        raise CacheError("retained plan differs from this exact source/configuration")
    record_root = Path(expected["state_root"])
    stored = r.read(r.binding(record_root / "sources" / (source["source_id"] + ".json")))
    if stored != source:
        raise CacheError("retained normalized source differs from current bound transcript")
    initial = plan["initial_job_ids"]
    wanted = set(initial)
    positions = {job_id: index for index, job_id in enumerate(initial)}
    collected = {}
    with r.paths.retained_directory(record_root / "waves") as directory:
        with os.scandir(directory) as entries:
            names = sorted(entry.name for entry in entries if entry.name.startswith("summarywave_"))
    if len(names) > expected["limits"]["max_waves"]:
        raise CacheError("retained wave selection exceeds original bound")
    for name in names:
        folder = r.wave_folder(plan, name)
        path = folder / "wave.json"
        if not r.safe.exists(path):
            continue  # Inert interrupted folder, never evidence of a paid job.
        wave = r.read(r.binding(path))
        body = {key: item for key, item in wave.items() if key != "wave_id"}
        if (wave.get("kind") != "himr_transcript_summary_wave" or type(wave.get("schema_version")) is not int
                or wave["schema_version"] != 1 or wave.get("plan_id") != plan["plan_id"]
                or wave.get("wave_id") != name or name != "summarywave_" + r.digest(body)[:32]
                or not isinstance(wave.get("jobs"), list)
                or not 1 <= len(wave["jobs"]) <= expected["limits"]["max_jobs_per_wave"]):
            raise CacheError("retained wave identity or job selection differs")
        chunk_jobs = []
        for job in wave["jobs"]:
            if not isinstance(job, dict):
                raise CacheError("retained wave job is malformed")
            if job.get("stage") != "chunk":
                if job.get("job_id") in wanted:
                    raise CacheError("retained initial job changed its stage")
                continue
            identity = job.get("job_id")
            if (identity not in wanted or identity in collected or job.get("config") != config
                    or job.get("scope") != core._scope([source["source_id"]], None, 0, positions[identity], False)):
                raise CacheError("retained chunk identity/order/configuration differs or repeats")
            collected[identity] = job
            chunk_jobs.append(job)
        if chunk_jobs:
            wire = r.wire(wave["jobs"])
            if (wave.get("input_sha256") != hashlib.sha256(wire).hexdigest() or wave.get("input_bytes") != len(wire)
                    or r.read_bytes({"path": str(folder / "requests.bin"), "sha256": wave["input_sha256"]}) != wire):
                raise CacheError("retained chunk request bytes differ from their sealed wave")
    if set(collected) != wanted:
        _partial_evidence(sources, list(collected.values()))
        return None  # Some initial jobs have not yet been materialized in a wave.
    jobs = [collected[identity] for identity in initial]
    # This validates each exact job/request and checks every source character,
    # citation, local timestamp and evidence ID without searching new boundaries.
    core.source_coverage(sources, jobs)
    return jobs


class _State:
    def __init__(self, worker_reference, manifest, builder, max_cache_bytes, max_entries):
        self.worker_reference = deepcopy(worker_reference)
        self.manifest, self.builder = manifest, builder
        self.max_cache_bytes, self.max_entries = max_cache_bytes, max_entries
        self.core_sha256 = _core_sha256()
        self.cache, self.bytes = OrderedDict(), 0
        self.record_cache, self.record_bytes = OrderedDict(), 0
        self.export_cache, self.export_bytes = OrderedDict(), 0
        self.counts = {"hits": 0, "retained_seed_hits": 0, "fresh_builds": 0,
                       "evictions": 0, "uncached_oversize": 0,
                       "record_hits": 0, "record_validations": 0, "record_evictions": 0,
                       "record_uncached_oversize": 0, "export_hits": 0, "export_validations": 0,
                       "export_evictions": 0, "export_uncached_oversize": 0}

    def statistics(self):
        return {**self.counts, "cache_entries": len(self.cache), "cache_bytes": self.bytes,
                "max_cache_entries": self.max_entries, "max_cache_bytes": self.max_cache_bytes,
                "record_cache_entries": len(self.record_cache), "record_cache_bytes": self.record_bytes,
                "max_record_cache_bytes": MAX_RECORD_CACHE_BYTES,
                "export_cache_entries": len(self.export_cache), "export_cache_bytes": self.export_bytes,
                "max_export_cache_bytes": MAX_EXPORT_CACHE_BYTES}

    def _record_identity(self, entry):
        if _core_sha256() != self.core_sha256:
            raise CacheError("summary core source changed during the cache scope")
        from pipeline import cloud_transcription_summary as worker
        r.safe.exact(entry, {"kind", "schema_version", "source", "request", "plan"}, "record snapshot entry")
        r.safe.file_binding(entry["request"])
        r.safe.file_binding(entry["plan"])
        key_name = worker._record_key(entry["source"]["recording_id"])
        root = Path(self.manifest["state_root"]) / "records" / key_name
        if entry["plan"]["path"] != str(root / "plan.json"):
            raise CacheError("record snapshot escaped its exact worker record workspace")
        key = hashlib.sha256(r.canonical({"entry": entry, "worker": self.worker_reference,
                                         "config": self.manifest["config"],
                                         "core_sha256": self.core_sha256})).digest()
        return root, key

    def cached_record(self, entry, validator):
        """Cache compact status only; caller rechecks source/request and ledger."""
        root, key = self._record_identity(entry)
        before = _record_witness(root)
        signature = hashlib.sha256(r.canonical(before)).digest()
        existing = self.record_cache.get(key)
        if existing is not None and existing[0] == signature:
            self.record_cache.move_to_end(key)
            self.counts["record_hits"] += 1
            return r.parse(existing[1])
        self.counts["record_validations"] += 1
        value = validator()
        # Full validator is read-only; its own worker holds the mutation lock.
        # Do not turn a concurrent edit into an attested mixed-state snapshot.
        if before != _record_witness(root):
            raise CacheError("record changed during snapshot validation")
        encoded = r.canonical(value)
        cost = len(key) + len(signature) + len(encoded)
        previous = self.record_cache.pop(key, None)
        if previous is not None:
            self.record_bytes -= len(key) + len(previous[0]) + len(previous[1])
        if cost <= MAX_RECORD_CACHE_BYTES:
            self.record_cache[key] = (signature, encoded)
            self.record_bytes += cost
            while len(self.record_cache) > MAX_CACHE_ENTRIES or self.record_bytes > MAX_RECORD_CACHE_BYTES:
                removed_key, (removed_signature, removed_value) = self.record_cache.popitem(last=False)
                self.record_bytes -= len(removed_key) + len(removed_signature) + len(removed_value)
                self.counts["record_evictions"] += 1
        else:
            self.counts["record_uncached_oversize"] += 1
        return r.parse(encoded)

    def cached_export(self, entry, validator):
        """Reuse a validated export only while all record proof metadata agrees."""
        root, key = self._record_identity(entry)
        exports = root / "exports"
        # Create only the original export namespace before taking a witness;
        # otherwise first mkdir legitimately changes the record root metadata.
        r.mkdir(exports)

        def artifact(value):
            if not isinstance(value, dict) or value.get("state") != "exported_private" or value.get("phase") != "transcripts":
                raise CacheError("cached export callback returned an unsupported artifact")
            reference = value.get("artifact")
            r.safe.file_binding(reference)
            if Path(reference["path"]).parent != exports:
                raise CacheError("cached export artifact escaped its record export directory")
            r.read_bytes(reference)

        def protected(witness):
            return [row for row in witness if Path(row[0]) != exports and exports not in Path(row[0]).parents]

        before = _record_witness(root)
        signature = hashlib.sha256(r.canonical(before)).digest()
        existing = self.export_cache.get(key)
        if existing is not None and existing[0] == signature:
            value = r.parse(existing[1])
            artifact(value)  # Exact export bytes remain mandatory, even on hits.
            if before != _record_witness(root):
                raise CacheError("record changed during cached export inspection")
            self.export_cache.move_to_end(key)
            self.counts["export_hits"] += 1
            return value
        self.counts["export_validations"] += 1
        value = validator()
        after = _record_witness(root)
        if protected(before) != protected(after):
            raise CacheError("export callback changed proof metadata outside its export directory")
        artifact(value)
        if after != _record_witness(root):
            raise CacheError("record changed after export validation")
        signature = hashlib.sha256(r.canonical(after)).digest()
        encoded = r.canonical(value)
        cost = len(key) + len(signature) + len(encoded)
        previous = self.export_cache.pop(key, None)
        if previous is not None:
            self.export_bytes -= len(key) + len(previous[0]) + len(previous[1])
        if cost <= MAX_EXPORT_CACHE_BYTES:
            self.export_cache[key] = (signature, encoded)
            self.export_bytes += cost
            while len(self.export_cache) > MAX_CACHE_ENTRIES or self.export_bytes > MAX_EXPORT_CACHE_BYTES:
                removed_key, (removed_signature, removed_value) = self.export_cache.popitem(last=False)
                self.export_bytes -= len(removed_key) + len(removed_signature) + len(removed_value)
                self.counts["export_evictions"] += 1
        else:
            self.counts["export_uncached_oversize"] += 1
        return r.parse(encoded)

    def initial_jobs(self, values, config=None):
        config = core.normalize_config(config)
        sources = core._source_list(values)
        core._topic_selections(sources, config)
        if _core_sha256() != self.core_sha256:
            raise CacheError("summary core source changed during the cache scope")
        key = _key(sources, config, self.core_sha256)
        encoded = self.cache.get(key)
        if encoded is not None:
            self.cache.move_to_end(key)
            self.counts["hits"] += 1
            return r.parse(encoded)
        jobs = _retained(self.manifest, sources, config)
        if jobs is None:
            self.counts["fresh_builds"] += 1
            jobs = self.builder(sources, config)
            core.source_coverage(sources, jobs)
        else:
            self.counts["retained_seed_hits"] += 1
        encoded = r.canonical(jobs)
        cost = len(key) + len(encoded)
        if cost <= self.max_cache_bytes:
            self.cache[key] = encoded
            self.bytes += cost
            while len(self.cache) > self.max_entries or self.bytes > self.max_cache_bytes:
                old_key, old_encoded = self.cache.popitem(last=False)
                self.bytes -= len(old_key) + len(old_encoded)
                self.counts["evictions"] += 1
        else:
            self.counts["uncached_oversize"] += 1
        return r.parse(encoded)


def _dispatch(sources, config=None):
    state = _ACTIVE.get()
    return _ORIGINAL_INITIAL_JOBS(sources, config) if state is None else state.initial_jobs(sources, config)


def statistics():
    state = _ACTIVE.get()
    return None if state is None else state.statistics()


def cached_record(entry, validator):
    """Outside an explicit scope, retain the original full validation behavior."""
    state = _ACTIVE.get()
    return validator() if state is None else state.cached_record(entry, validator)


def cached_export(entry, validator):
    """Outside an explicit scope, invoke the original export each time."""
    state = _ACTIVE.get()
    return validator() if state is None else state.cached_export(entry, validator)


@contextmanager
def scope(worker_manifest_ref, *, max_cache_bytes=MAX_CACHE_BYTES, max_entries=MAX_CACHE_ENTRIES):
    """An explicit, reversible optimization; no implicit service/global enable."""
    r.safe.integer(max_cache_bytes, 1, MAX_CACHE_BYTES, "initial-job cache byte bound")
    r.safe.integer(max_entries, 1, MAX_CACHE_ENTRIES, "initial-job cache entry bound")
    if _ACTIVE.get() is not None or not _SCOPE_LOCK.acquire(blocking=False):
        raise CacheError("another initial-job cache scope is active")
    token = None
    previous = core.initial_jobs
    installed = False
    try:
        if previous is not _ORIGINAL_INITIAL_JOBS:
            raise CacheError("initial-job constructor has an unexpected existing override")
        manifest = _worker(worker_manifest_ref)
        builder = _select_builder()
        state = _State(worker_manifest_ref, manifest, builder, max_cache_bytes, max_entries)
        token = _ACTIVE.set(state)
        core.initial_jobs = _dispatch
        installed = True
        yield state
    finally:
        if installed:
            core.initial_jobs = previous
        if token is not None:
            _ACTIVE.reset(token)
        _SCOPE_LOCK.release()
