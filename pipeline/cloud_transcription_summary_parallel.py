"""Bounded process-parallel GET/collection of existing paid Gemini waves.

The parent retains the global summary-worker lock. Each task owns one distinct
record plan; its waves are polled sequentially using the original record locks
and validators. No paid submission, reconciliation, retry, model change, or new
reservation is performed here. Workers enter the explicitly bound runtime
release and job-cache scope independently, reading their own explicit .env file.
"""
from __future__ import annotations

import atexit
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from copy import deepcopy
import math
import multiprocessing
from pathlib import Path
import threading

from pipeline import transcript_summary as r
from pipeline import cloud_transcription_summary as worker
from pipeline import cloud_transcription_release as release
from pipeline import cloud_transcription_summary_cache as job_cache

MAX_WORKERS = 4
MAX_GROUPS = 64
MAX_WAVES_PER_RECORD = 256
_ACTIVE = ContextVar("himr_cloud_summary_parallel_collection", default=None)
_CHILD = None


class ParallelPollError(RuntimeError):
    def __init__(self, message, *, report=None):
        super().__init__(message)
        self.report = report


def _runtime(path):
    root = r.safe.path_value(path)
    with r.paths.retained_directory(root):
        if (Path.cwd() != root or Path(worker.__file__).parent != root / "pipeline"
                or Path(__file__).parent != root / "pipeline"):
            raise ParallelPollError("parallel collection requires the exact versioned runtime working directory")
    return root


def _groups(values):
    if not isinstance(values, list) or len(values) > MAX_GROUPS:
        raise ParallelPollError("parallel collection group count exceeds its bound")
    plans, waves, result = set(), set(), []
    for value in values:
        r.safe.exact(value, {"plan", "waves"}, "parallel collection group")
        r.safe.file_binding(value["plan"])
        path = value["plan"]["path"]
        if path in plans:
            raise ParallelPollError("one record plan cannot be assigned to multiple collection workers")
        plans.add(path)
        selected = value["waves"]
        if not isinstance(selected, list) or not 1 <= len(selected) <= MAX_WAVES_PER_RECORD:
            raise ParallelPollError("collection requires a bounded nonempty wave selection")
        for name in selected:
            if (not isinstance(name, str) or not name.startswith("summarywave_")
                    or len(name) != 44 or any(char not in "0123456789abcdef" for char in name[12:])
                    or name in waves):
                raise ParallelPollError("collection wave identity is invalid or duplicated")
            waves.add(name)
        result.append(deepcopy(value))
    return result


def _initialize(worker_ref, execution_release_ref, env_file, runtime_path, stop_event):
    """Spawn initializer receives only refs/paths/synchronization, never keys."""
    global _CHILD
    _CHILD = None
    _runtime(runtime_path)
    r.safe.path_value(env_file)
    stack = ExitStack()
    try:
        stack.enter_context(release.activate(execution_release_ref))
        manifest = worker.load_manifest(worker_ref)
        stack.enter_context(job_cache.scope(worker_ref))
    except BaseException:
        stack.close()
        raise
    _CHILD = {"worker_ref": deepcopy(worker_ref), "manifest": manifest,
              "env_file": str(env_file), "stop_event": stop_event, "stack": stack}
    atexit.register(stack.close)


def _admit_group(group):
    if _CHILD is None:
        raise ParallelPollError("collection worker was not explicitly initialized")
    _groups([group])
    reference = group["plan"]
    manifest = _CHILD["manifest"]
    root = Path(manifest["state_root"])
    plan_root = Path(reference["path"]).parent
    if plan_root.parent != root / "records" or Path(reference["path"]).name != "plan.json":
        raise ParallelPollError("collection plan escaped its worker record workspace")
    entry = r.read(r.binding(root / "entries" / (plan_root.name + ".json")))
    r.safe.exact(entry, {"kind", "schema_version", "source", "request", "plan"}, "collection worker entry")
    if (entry.get("kind") != worker.KIND + "_record" or type(entry.get("schema_version")) is not int
            or entry["schema_version"] != 1 or entry["plan"] != reference
            or entry["source"].get("format") not in {"third_party", "cloud"}
            or plan_root.name != worker._record_key(entry["source"]["recording_id"])):
        raise ParallelPollError("collection entry differs from its exact record plan")
    plan = r.read(reference)
    expected = worker._request(manifest, entry["source"])
    if (plan.get("request") != entry["request"] or plan.get("request_value") != expected
            or r.read(entry["request"]) != expected):
        raise ParallelPollError("collection request differs from the approved Gemini worker")
    for wave_id in group["waves"]:
        folder = r.wave_folder(plan, wave_id)
        wave = r.read(r.binding(folder / "wave.json"))
        if wave.get("provider") != "gemini" or wave.get("wave_id") != wave_id:
            raise ParallelPollError("collection accepts existing Gemini waves only")
        if not r.safe.exists(folder / "submit-intent.json") or not r.safe.exists(folder / "submitted.json"):
            raise ParallelPollError("collection requires an existing durable paid intent and receipt")
        reservation = r.read(r.binding(root / "reservations" / (wave_id + ".json")))
        expected_reservation = {"kind": worker.KIND + "_reservation", "schema_version": 1,
            "worker": _CHILD["worker_ref"], "record_plan": reference, "wave_id": wave_id,
            "input_sha256": wave["input_sha256"], "maximum_cost_microusd": wave["maximum_cost_microusd"]}
        if reservation != expected_reservation:
            raise ParallelPollError("collection lacks its original global reservation")
    return reference


def _client_error(error, wave_id):
    code = getattr(error, "status_code", None)
    delay = getattr(error, "retry_after_seconds", None)
    return {"operation": "poll", "wave_id": wave_id, "state": "transport_error_retained",
            "status_code": code if type(code) is int and 100 <= code <= 599 else None,
            "retry_after_seconds": delay if type(delay) in {int, float} and 0 <= delay <= 2**53
                and math.isfinite(delay) else None, "automatic_retry": False}


def _poll_group(group):
    events = []
    current = None
    try:
        reference = _admit_group(group)
        for current in group["waves"]:
            if _CHILD["stop_event"].is_set():
                break
            try:
                result = r.poll_wave(reference["path"], reference["sha256"], current,
                                     env_file=_CHILD["env_file"])
            except r.client_module.BatchClientError as error:
                events.append(_client_error(error, current))
                continue  # Next selected wave, not a retry of this wave.
            if (not isinstance(result, dict) or result.get("wave_id") != current
                    or result.get("state") not in {"already_collected", "remote_pending", "collected"}):
                raise ParallelPollError("original collection returned an unsupported result")
            events.append({"operation": "poll", "wave_id": current, **{
                key: result[key] for key in ("state", "completed", "needs_review", "remote_state") if key in result}})
    except Exception as error:
        # Never pickle a raw exception/provider body/credential back to parent.
        fatal = {"operation": "poll", "wave_id": current, "state": "collection_validation_failed",
                 "error_type": type(error).__name__}
        return {"events": events, "fatal_errors": [fatal], "groups_completed": 0, "new_paid_requests": 0}
    return {"events": events, "fatal_errors": [], "groups_completed": int(len(events) == len(group["waves"])),
            "new_paid_requests": 0, "cache": job_cache.statistics()}


class _Pool:
    def __init__(self, worker_ref, execution_release_ref, env_file, runtime_path, max_workers):
        _runtime(runtime_path)
        for reference in (worker_ref, execution_release_ref):
            r.safe.file_binding(reference)
        r.safe.path_value(env_file)
        r.safe.integer(max_workers, 1, MAX_WORKERS, "parallel collection workers")
        context = multiprocessing.get_context("spawn")
        self.stop_event = context.Event()
        self.max_workers = max_workers
        self.executor = ProcessPoolExecutor(max_workers=max_workers, mp_context=context,
            initializer=_initialize, initargs=(deepcopy(worker_ref), deepcopy(execution_release_ref),
                                              str(env_file), str(runtime_path), self.stop_event))
        self.lock = threading.Lock()
        self.closed = False
        self._shutdown_complete = False
        self.counts = {"groups_completed": 0, "waves_polled": 0, "transport_errors": 0, "fatal_errors": 0}

    def statistics(self):
        return {**self.counts, "workers": self.max_workers, "new_paid_requests": 0}

    def close(self):
        if self._shutdown_complete:
            return
        # A failed drain must never make this executor reusable. Successful
        # shutdown is separately tracked so a later cleanup can finish a drain
        # if the first shutdown call itself was interrupted.
        self.closed = True
        self.stop_event.set()
        self.executor.shutdown(wait=True, cancel_futures=True)
        self._shutdown_complete = True

    def poll_groups(self, groups, *, stopping=lambda: False, on_event=lambda _event: None):
        groups = _groups(groups)  # Validate whole partition before any dispatch.
        if self.closed or not self.lock.acquire(blocking=False):
            raise ParallelPollError("collection pool is closed or already polling")
        futures = {}
        events, fatal_errors, completed = [], [], 0
        pending = iter(groups)
        exhausted = False
        self.stop_event.clear()
        try:
            while futures or not exhausted:
                if stopping():
                    self.stop_event.set()
                    exhausted = True
                while not exhausted and not fatal_errors and len(futures) < self.max_workers:
                    try:
                        group = next(pending)
                    except StopIteration:
                        exhausted = True
                        break
                    futures[self.executor.submit(_poll_group, group)] = group
                if not futures:
                    break
                done, _ = wait(futures, timeout=0.5, return_when=FIRST_COMPLETED)
                for future in done:
                    group = futures.pop(future)
                    try:
                        report = future.result()
                        if not isinstance(report, dict) or not isinstance(report.get("events"), list) or not isinstance(report.get("fatal_errors"), list):
                            raise ParallelPollError("collection worker report is malformed")
                    except Exception as error:
                        report = {"events": [], "groups_completed": 0,
                                  "fatal_errors": [{"operation": "poll", "wave_id": None,
                                      "state": "collection_worker_failed", "error_type": type(error).__name__}]}
                    events.extend(report["events"])
                    fatal_errors.extend(report["fatal_errors"])
                    completed += report.get("groups_completed", 0)
                    for event in report["events"]:
                        on_event(event)
                    if fatal_errors:
                        self.stop_event.set()
                        exhausted = True
            report = {"events": events, "transport_events": [event for event in events
                       if event.get("state") == "transport_error_retained"], "fatal_errors": fatal_errors,
                      "groups_completed": completed, "workers": self.max_workers, "new_paid_requests": 0}
            self.counts["groups_completed"] += completed
            self.counts["waves_polled"] += len(events)
            self.counts["transport_errors"] += len(report["transport_events"])
            self.counts["fatal_errors"] += len(fatal_errors)
            if fatal_errors:
                raise ParallelPollError("parallel collection failed validation; paid state retained", report=report)
            return report
        except BaseException:
            # BrokenProcessPool can mark futures failed before its manager has
            # physically reaped workers. Shut down synchronously HERE, while the
            # caller still owns the global mutation lock, not only at outer
            # scope exit. This also covers partial dispatch and event failures.
            self.close()
            raise
        finally:
            # Never let parent enter admission/budget mutation while children
            # still write captures. No terminate/kill or automatic pool restart.
            try:
                if futures and not self._shutdown_complete:
                    self.stop_event.set()
                    try:
                        wait(futures)
                    except BaseException:
                        self.close()
                        raise
            finally:
                self.lock.release()


def active():
    return _ACTIVE.get() is not None


def statistics():
    value = _ACTIVE.get()
    return None if value is None else value.statistics()


def poll_groups(groups, *, stopping=lambda: False, on_event=lambda _event: None):
    value = _ACTIVE.get()
    if value is None:
        raise ParallelPollError("parallel collection requires an explicit worker scope")
    return value.poll_groups(groups, stopping=stopping, on_event=on_event)


@contextmanager
def scope(worker_ref, execution_release_ref, *, env_file, runtime_path, max_workers=4):
    if _ACTIVE.get() is not None:
        raise ParallelPollError("nested parallel collection scope is not permitted")
    pool = _Pool(worker_ref, execution_release_ref, env_file, runtime_path, max_workers)
    token = _ACTIVE.set(pool)
    try:
        yield pool
    finally:
        try:
            pool.close()
        finally:
            _ACTIVE.reset(token)
