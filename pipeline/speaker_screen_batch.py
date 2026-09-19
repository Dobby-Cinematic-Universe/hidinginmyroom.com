"""Finite, private batches of explicitly selected standalone speaker screens.

No discovery, campaign integration, publication, model download, or ASR is done
here. Each unfinished recording gets at most one bounded worker invocation per
explicit batch run. Existing per-window checkpoints provide resumability.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import stat
import subprocess
import sys
import tempfile
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from pipeline import speaker_screen as screen
from pipeline import speaker_screen_paths as paths

BatchError = screen.ScreenError
MAX_JOBS = 128
MAX_ATTEMPTS = 4096
DEFAULT_RESOURCES = {"concurrency": 2, "max_run_seconds": 3600}
MARKER = {"kind": "himr_private_speaker_screen_batch_workspace", "schema_version": 1}
KINDS = {"multiple_speaker_candidate", "uncertain", "no_second_voice_detected_in_sampled_audio"}


def _implementation():
    return {**screen.implementation_hashes(), Path(__file__).name: hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}


def _python_binding():
    invocation = screen.path_value(sys.executable)
    target = invocation.resolve(strict=True)
    with screen.opened(target, executable=True) as descriptor:
        sha = screen.hash_fd(descriptor, 256 * 1024**2, time.monotonic() + 30)
    configuration = invocation.parent.parent / "pyvenv.cfg"
    config = None
    if configuration.exists():
        with screen.opened(configuration) as descriptor:
            config = {"path": str(configuration), "sha256": screen.hash_fd(descriptor, 65536, time.monotonic() + 10)}
    return {"path": str(invocation), "resolved_path": str(target), "sha256": sha,
            "python_version": sys.version, "venv_configuration": config}


def _overlap(left: Path, right: Path) -> bool:
    return left == right or left.is_relative_to(right) or right.is_relative_to(left)


def _load_orders(references):
    if not isinstance(references, list) or not 1 <= len(references) <= MAX_JOBS:
        raise BatchError(f"batch requires 1..{MAX_JOBS} explicit work orders")
    plans, seen_refs, seen_media = [], set(), set()
    for reference in references:
        screen.file_binding(reference)
        if reference["path"] in seen_refs:
            raise BatchError("batch repeats a work order path")
        seen_refs.add(reference["path"])
        value = screen.read_json(screen.path_value(reference["path"]), reference["sha256"])
        plan = screen.build_plan(value)
        identity = plan["order"]["recording"]["media_id"]
        if identity in seen_media:
            raise BatchError("batch repeats a recording identity")
        seen_media.add(identity)
        plans.append(plan)
    return plans


def _check_paths(state_root, references, plans, *, request_path=None, python=None):
    roots = [screen.path_value(state_root)] + [screen.path_value(plan["order"]["output_root"]) for plan in plans]
    inputs = [screen.path_value(row["path"]) for row in references]
    source_paths = set()
    for plan in plans:
        order = plan["order"]
        source = order["recording"]["path"]
        if source in source_paths:
            raise BatchError("batch repeats a recording source path")
        source_paths.add(source)
        inputs += [screen.path_value(value["path"]) for value in (
            order["recording"], order["ffmpeg"], order["models"]["silero_vad"], order["models"]["ecapa_embedding"])]
    if request_path is not None:
        inputs.append(screen.path_value(request_path))
    if python is not None:
        inputs += [screen.path_value(python[key]) for key in ("path", "resolved_path")]
        if python["venv_configuration"] is not None:
            inputs.append(screen.path_value(python["venv_configuration"]["path"]))
    inputs += [Path(__file__).resolve(), Path(screen.__file__).resolve()]
    for index, root in enumerate(roots):
        if any(_overlap(root, other) for other in roots[index + 1:]):
            raise BatchError("batch and recording output roots must be disjoint")
        if any(_overlap(root, source) for source in inputs):
            raise BatchError("an output workspace overlaps an input, model, or executable")


def build_manifest(request, *, request_binding=None):
    screen.exact(request, {"kind", "schema_version", "work_orders", "state_root", "resources"}, "batch request")
    if request["kind"] != "himr_speaker_screen_batch_request" or type(request["schema_version"]) is not int or request["schema_version"] != 1:
        raise BatchError("unsupported batch request")
    if not isinstance(request["resources"], dict) or set(request["resources"]) - set(DEFAULT_RESOURCES):
        raise BatchError("unknown batch resource limits")
    resources = {**DEFAULT_RESOURCES, **request["resources"]}
    screen.integer(resources["concurrency"], 1, 4, "batch concurrency")
    screen.integer(resources["max_run_seconds"], 10, 86400, "batch wall-clock bound")
    root = screen.path_value(request["state_root"])
    if request_binding is not None:
        screen.file_binding(request_binding)
    plans = _load_orders(request["work_orders"])
    python = _python_binding()
    _check_paths(root, request["work_orders"], plans,
                 request_path=None if request_binding is None else request_binding["path"], python=python)
    jobs = [{"index": index, "work_order": dict(reference), "plan_id": plan["plan_id"],
             "plan_sha256": screen.digest(plan), "media_id": plan["order"]["recording"]["media_id"],
             "output_root": plan["order"]["output_root"], "planned_windows": len(plan["windows"]),
             "resources": plan["order"]["resources"]}
            for index, (reference, plan) in enumerate(zip(request["work_orders"], plans))]
    value = {"kind": "himr_speaker_screen_batch_manifest", "schema_version": 1,
             "request": request_binding, "state_root": str(root), "resources": resources,
             "python": python, "implementation": _implementation(), "jobs": jobs,
             "semantics": {"visibility": "private", "cpu_only": True, "discovery": False,
                           "controller_mutation": False, "publication_authority": False,
                           "invocations_per_unfinished_recording_per_run": 1,
                           "whole_recording_solo_claimed": False, "person_identity_claimed": False}}
    return {**value, "batch_id": "screenbatch_" + screen.digest(value)[:32]}


def validate_manifest(value):
    fields = {"kind", "schema_version", "request", "state_root", "resources", "python",
              "implementation", "jobs", "semantics", "batch_id"}
    screen.exact(value, fields, "batch manifest")
    if not isinstance(value["jobs"], list) or not 1 <= len(value["jobs"]) <= MAX_JOBS:
        raise BatchError("invalid batch job count")
    references = []
    for job in value["jobs"]:
        screen.exact(job, {"index", "work_order", "plan_id", "plan_sha256", "media_id", "output_root", "planned_windows", "resources"}, "batch job")
        references.append(job["work_order"])
    request = {"kind": "himr_speaker_screen_batch_request", "schema_version": 1,
               "work_orders": references, "state_root": value["state_root"], "resources": value["resources"]}
    expected = build_manifest(request, request_binding=value["request"])
    if value != expected:
        raise BatchError("batch manifest, work order, interpreter, or implementation binding differs")
    if value["request"] is not None:
        original = screen.read_json(Path(value["request"]["path"]), value["request"]["sha256"])
        if build_manifest(original, request_binding=value["request"]) != value:
            raise BatchError("batch request differs from its sealed manifest")
    return value


def _workspace(root, *, create=False):
    root = screen.path_value(root)
    if create:
        with paths.retained_directory(root.parent) as parent:
            try:
                os.mkdir(root.name, 0o700, dir_fd=parent)
                os.fsync(parent)
            except FileExistsError:
                pass
    with paths.retained_directory(root) as directory:
        marker = root / "workspace.json"
        if not screen.exists(marker) and create:
            with os.scandir(directory) as entries:
                if next(entries, None) is not None:
                    raise BatchError("refusing a nonempty unmarked batch workspace")
            screen.write_immutable(marker, MARKER)
        if screen.read_json(marker) != MARKER:
            raise BatchError("batch workspace marker differs")
    return root


def seal_manifest(request_path, expected_sha256, output):
    reference = {"path": str(screen.path_value(request_path)), "sha256": expected_sha256}
    screen.file_binding(reference)
    request = screen.read_json(Path(reference["path"]), expected_sha256)
    manifest = build_manifest(request, request_binding=reference)
    root = Path(manifest["state_root"])
    if screen.path_value(output) != root / "manifest.json":
        raise BatchError("manifest output must be exactly state_root/manifest.json")
    _workspace(root, create=True)
    screen.write_immutable(root / "manifest.json", manifest)
    return manifest


@contextmanager
def _locked(root):
    with paths.retained_directory(root) as directory:
        if screen.read_json(root / "workspace.json") != MARKER:
            raise BatchError("batch workspace marker differs")
        descriptor = os.open("batch.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, 0o600, dir_fd=directory)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1 or info.st_size != 0:
                raise BatchError("unsafe batch execution lock")
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise BatchError("another batch invocation or surviving worker owns this workspace") from None
            yield descriptor
        finally:
            os.close(descriptor)


def _projection(job, result):
    if not isinstance(result, dict) or result.get("plan_id") != job["plan_id"]:
        raise BatchError("worker status has a different recording plan")
    state = result.get("state")
    if state not in {"not_started", "paused", "completed"}:
        raise BatchError("worker status is not a recognized bounded outcome")
    planned = result.get("planned_windows")
    screen.integer(planned, 1, 512, "planned windows")
    if planned != job["planned_windows"]:
        raise BatchError("worker planned window count differs")
    completed = result.get("completed_windows", 0)
    screen.integer(completed, 0, planned, "completed windows")
    remaining = result.get("remaining_windows", planned)
    if type(remaining) is not int or remaining != planned - completed or (state == "completed") != (completed == planned):
        raise BatchError("partial sampling cannot be labeled completed")
    summary = result.get("summary")
    classification, coverage, reasons = None, None, []
    if state != "not_started":
        if not isinstance(summary, dict) or summary.get("status") not in KINDS:
            raise BatchError("worker classification is invalid")
        classification = summary["status"]
        coverage = summary.get("coverage")
        reasons = summary.get("reason_flags")
        if (not isinstance(coverage, dict) or coverage.get("planned_windows") != planned
                or coverage.get("inspected_windows") != completed
                or coverage.get("uninspected_windows") != remaining
                or not isinstance(reasons, list) or any(not isinstance(reason, str) for reason in reasons)):
            raise BatchError("worker coverage does not match checkpoint counts")
    decision = result.get("screening_decision_complete", state == "completed")
    if type(decision) is not bool or (state == "completed" and not decision):
        raise BatchError("worker screening decision is invalid")
    if decision and state != "completed" and not (
            state == "paused" and classification == "multiple_speaker_candidate"
            and result.get("stop_reason") == "supported_multiple_speakers"
            and job["resources"].get("early_stop_on_positive", False)):
        raise BatchError("an unfinished sampling plan cannot claim a finished screen")
    return {"index": job["index"], "media_id": job["media_id"], "plan_id": job["plan_id"],
            "output_root": job["output_root"], "sampling_state": state,
            "screening_decision_complete": decision, "classification": classification,
            "completed_windows": completed, "planned_windows": planned, "remaining_windows": remaining,
            "coverage": coverage, "reason_flags": reasons,
            "stop_reason": result.get("stop_reason"), "source_currently_rechecked": False}


def _read_job(job):
    plan = screen.build_plan(screen.read_json(Path(job["work_order"]["path"]), job["work_order"]["sha256"]))
    if plan["plan_id"] != job["plan_id"] or screen.digest(plan) != job["plan_sha256"]:
        raise BatchError("recording work order or implementation changed")
    return _projection(job, screen.read_status(plan))


def _summary(manifest, rows, *, invocation_state=None, errors=None):
    counts = {name: sum(row["classification"] == name for row in rows) for name in sorted(KINDS)}
    counts.update(recordings=len(rows), screening_decisions_complete=sum(row["screening_decision_complete"] for row in rows),
                  sampling_plans_completed=sum(row["sampling_state"] == "completed" for row in rows),
                  sampling_plans_paused=sum(row["sampling_state"] == "paused" for row in rows),
                  not_started=sum(row["sampling_state"] == "not_started" for row in rows))
    complete = counts["screening_decisions_complete"] == len(rows)
    return {"kind": "himr_speaker_screen_batch_status", "schema_version": 1,
            "batch_id": manifest["batch_id"], "state": "screening_complete" if complete else "paused",
            "invocation_state": invocation_state, "counts": counts, "recordings": rows,
            "errors": errors or [], "semantics": {**manifest["semantics"],
                "candidate_counts_are_not_person_counts": True,
                "negative_means_only_no_second_voice_detected_in_sampled_audio": True,
                "partial_positive_may_finish_screen_without_finishing_sampling": True,
                "source_currently_rechecked": False}}


def _load_manifest(path, expected):
    screen.file_binding({"path": str(screen.path_value(path)), "sha256": expected})
    value = validate_manifest(screen.read_json(Path(path), expected))
    if Path(path) != Path(value["state_root"]) / "manifest.json":
        raise BatchError("batch manifest is outside its admitted workspace")
    _workspace(Path(value["state_root"]))
    return value


def status_batch(path, expected):
    manifest = _load_manifest(path, expected)
    return _summary(manifest, [_read_job(job) for job in manifest["jobs"]])


def _attempt_number(root, manifest):
    with paths.retained_directory(root) as directory:
        with os.scandir(directory) as entries:
            names = []
            for entry in entries:
                names.append(entry.name)
                if len(names) > 3 * MAX_ATTEMPTS + 16:
                    raise BatchError("batch workspace exceeds its bounded journal size")
    attempts = []
    for name in names:
        if name.startswith("attempt-") and name.endswith(".start.json"):
            raw = name[len("attempt-"):-len(".start.json")]
            if len(raw) != 6 or not raw.isascii() or not raw.isdigit() or int(raw) < 1:
                raise BatchError("malformed batch attempt filename")
            attempts.append(int(raw))
    if sorted(attempts) != list(range(1, len(attempts) + 1)) or len(attempts) >= MAX_ATTEMPTS:
        raise BatchError("batch attempt sequence is incomplete or exhausted")
    for number in sorted(attempts):
        start = screen.read_json(root / f"attempt-{number:06d}.start.json")
        screen.exact(start, {"kind", "schema_version", "batch_id", "attempt", "job_index", "plan_id", "work_order"}, "batch attempt")
        screen.integer(start["job_index"], 0, len(manifest["jobs"]) - 1, "attempt job index")
        job = manifest["jobs"][start["job_index"]]
        expected = {"kind": "himr_speaker_screen_batch_attempt", "schema_version": 1,
                    "batch_id": manifest["batch_id"], "attempt": number,
                    "job_index": job["index"], "plan_id": job["plan_id"], "work_order": job["work_order"]}
        if start != expected:
            raise BatchError("batch attempt differs from its manifest")
        finish_path = root / f"attempt-{number:06d}.finish.json"
        if screen.exists(finish_path):
            finish = screen.read_json(finish_path)
            if (finish.get("kind") != "himr_speaker_screen_batch_attempt_result"
                    or finish.get("batch_id") != manifest["batch_id"] or finish.get("attempt") != number
                    or finish.get("start_sha256") != screen.digest(start)):
                raise BatchError("batch attempt result differs from its start")
    for name in names:
        if name.startswith("attempt-") and name.endswith(".finish.json"):
            if name.replace(".finish.json", ".start.json") not in names:
                raise BatchError("batch attempt result lacks its original start")
    return len(attempts) + 1


def _child_environment(threads):
    return {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
            "PYTHONNOUSERSITE": "1", "PYTHONDONTWRITEBYTECODE": "1", "CUDA_VISIBLE_DEVICES": "",
            "HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1", "HF_HUB_DISABLE_TELEMETRY": "1",
            "OMP_NUM_THREADS": str(threads), "MKL_NUM_THREADS": str(threads),
            "OPENBLAS_NUM_THREADS": str(threads), "NUMEXPR_NUM_THREADS": str(threads)}


def _launch(manifest, job, lock_fd, deadline, attempt, *, child_signal_mask=None):
    root = Path(manifest["state_root"])
    if _python_binding() != manifest["python"]:
        raise BatchError("selected Python executable or virtual environment changed")
    start = {"kind": "himr_speaker_screen_batch_attempt", "schema_version": 1,
             "batch_id": manifest["batch_id"], "attempt": attempt, "job_index": job["index"],
             "plan_id": job["plan_id"], "work_order": job["work_order"]}
    screen.write_immutable(root / f"attempt-{attempt:06d}.start.json", start)
    parent = os.getpid()
    # The caller defers cancellation until it registers this child. Do not pass
    # that temporary blocked mask through exec into the standalone worker.
    inherited_mask = (signal.pthread_sigmask(signal.SIG_BLOCK, set())
                      if child_signal_mask is None else child_signal_mask)
    stdout = stderr = process = None

    def limits():
        screen.die_with_parent(parent)
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        resource.setrlimit(resource.RLIMIT_FSIZE, (screen.MAX_JSON, screen.MAX_JSON))
        screen.deny_internet()
        signal.pthread_sigmask(signal.SIG_SETMASK, inherited_mask)

    try:
        stdout = tempfile.TemporaryFile(mode="w+b", dir=root)
        stderr = tempfile.TemporaryFile(mode="w+b", dir=root)
        process = subprocess.Popen(
            [manifest["python"]["path"], "-B", str(Path(screen.__file__).resolve()), "run",
             "--work-order", job["work_order"]["path"], "--expected-sha256", job["work_order"]["sha256"]],
            stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr, close_fds=True,
            pass_fds=(lock_fd,), start_new_session=True, preexec_fn=limits, cwd=str(ROOT),
            env=_child_environment(job["resources"]["threads"]),
        )
        return {"process": process, "stdout": stdout, "stderr": stderr, "job": job, "start": start,
                "deadline": min(deadline, time.monotonic() + job["resources"]["max_run_seconds"] + 15)}
    except BaseException:
        # Own the process from Popen's return until the caller registers it.
        # This also handles non-signal failures while building the return value.
        try:
            if process is not None:
                _terminate({"process": process})
        finally:
            if stdout is not None:
                stdout.close()
            if stderr is not None:
                stderr.close()
        raise


@contextmanager
def _deferred_launch_signals():
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM})
    try:
        yield previous
    finally:
        # A pending cancellation is delivered only after workers.append, when
        # the enclosing finally can terminate and reap every admitted child.
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


def _terminate(worker):
    process = worker["process"]
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    process.wait(timeout=5)


def _finish(manifest, worker, *, outcome):
    job = worker["job"]
    row = _read_job(job)  # Per-window proofs are authoritative, never stdout counts.
    if outcome == "returned":
        worker["stdout"].seek(0)
        body = worker["stdout"].read(screen.MAX_JSON + 1)
        if len(body) > screen.MAX_JSON:
            raise BatchError("worker output exceeds its bound")
        def pairs(items):
            value = {}
            for key, item in items:
                if key in value:
                    raise BatchError("duplicate worker JSON key")
                value[key] = item
            return value
        try:
            raw = json.loads(body, object_pairs_hook=pairs,
                             parse_constant=lambda _: (_ for _ in ()).throw(BatchError("nonfinite worker JSON")))
            if _projection(job, raw) != row:
                raise BatchError("worker output differs from saved checkpoint replay")
        except (ValueError, UnicodeError, RecursionError) as error:
            raise BatchError("worker did not return valid bounded JSON") from error
    receipt = {"kind": "himr_speaker_screen_batch_attempt_result", "schema_version": 1,
               "batch_id": manifest["batch_id"], "attempt": worker["start"]["attempt"],
               "start_sha256": screen.digest(worker["start"]), "outcome": outcome,
               "returncode": worker["process"].returncode, "recording": row}
    screen.write_immutable(Path(manifest["state_root"]) / f"attempt-{worker['start']['attempt']:06d}.finish.json", receipt)
    return row


@contextmanager
def _cancellable():
    def stop(_signum, _frame):
        raise KeyboardInterrupt
    previous = {number: signal.signal(number, stop) for number in (signal.SIGINT, signal.SIGTERM)}
    try:
        yield
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def run_batch(path, expected):
    manifest = _load_manifest(path, expected)
    root = Path(manifest["state_root"])
    deadline = time.monotonic() + manifest["resources"]["max_run_seconds"]
    workers, errors = [], []
    invocation_state = "finished"
    with _cancellable(), _locked(root) as lock_fd:
        rows = [_read_job(job) for job in manifest["jobs"]]
        pending = [job for job, row in zip(manifest["jobs"], rows) if not row["screening_decision_complete"]]
        if not pending:
            return _summary(manifest, rows, invocation_state="finished")
        attempt = _attempt_number(root, manifest)
        try:
            while pending or workers:
                if time.monotonic() >= deadline:
                    invocation_state = "time_limit"
                    break
                while pending and len(workers) < manifest["resources"]["concurrency"]:
                    if time.monotonic() >= deadline:
                        break
                    if _implementation() != manifest["implementation"]:
                        raise BatchError("speaker screen implementation changed during batch")
                    if attempt > MAX_ATTEMPTS:
                        raise BatchError("batch attempt bound reached")
                    job = pending.pop(0)
                    with _deferred_launch_signals() as child_signal_mask:
                        workers.append(_launch(manifest, job, lock_fd, deadline, attempt,
                                               child_signal_mask=child_signal_mask))
                    attempt += 1
                for worker in list(workers):
                    code = worker["process"].poll()
                    if code is None and time.monotonic() < worker["deadline"]:
                        continue
                    outcome = "returned" if code == 0 else "worker_failed"
                    if code is None:
                        _terminate(worker)
                        outcome = "time_limit"
                    try:
                        rows[worker["job"]["index"]] = _finish(manifest, worker, outcome=outcome)
                        if outcome != "returned":
                            errors.append({"job_index": worker["job"]["index"], "reason": outcome})
                    finally:
                        worker["stdout"].close()
                        worker["stderr"].close()
                        workers.remove(worker)
                if workers:
                    time.sleep(min(0.2, max(0, deadline - time.monotonic())))
        except KeyboardInterrupt:
            invocation_state = "cancelled"
        finally:
            # Kill every worker before replaying any output: a malformed first
            # job must never leave another recording running after cancellation.
            for worker in workers:
                try:
                    _terminate(worker)
                except (OSError, subprocess.SubprocessError):
                    errors.append({"job_index": worker["job"]["index"], "reason": "cleanup_failed"})
            for worker in workers:
                try:
                    rows[worker["job"]["index"]] = _finish(manifest, worker, outcome="interrupted")
                except (BatchError, OSError, ValueError, RuntimeError):
                    errors.append({"job_index": worker["job"]["index"], "reason": "interrupted_checkpoint_replay_failed"})
                finally:
                    worker["stdout"].close()
                    worker["stderr"].close()
        return _summary(manifest, rows, invocation_state=invocation_state, errors=errors)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    planner = subparsers.add_parser("plan")
    planner.add_argument("--request", required=True)
    planner.add_argument("--expected-sha256", required=True)
    planner.add_argument("--output", required=True)
    for command in ("run", "status"):
        selected = subparsers.add_parser(command)
        selected.add_argument("--manifest", required=True)
        selected.add_argument("--expected-sha256", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            value = seal_manifest(args.request, args.expected_sha256, args.output)
        elif args.command == "status":
            value = status_batch(args.manifest, args.expected_sha256)
        else:
            value = run_batch(args.manifest, args.expected_sha256)
        print(json.dumps(value, indent=2, sort_keys=True, allow_nan=False))
        if value.get("invocation_state") == "cancelled":
            return 130
        return 2 if value.get("errors") else 0
    except (BatchError, OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"SpeakerScreenBatchError: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
