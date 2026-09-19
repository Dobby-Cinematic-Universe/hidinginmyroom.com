"""Finite private supervisor for explicitly sealed guided screening batches.

This module never admits media, changes ASR, or invents screening orders. Each
owned child runs one existing hash-bound guided manifest. Normal bounded pauses
can repeat a finite number of times; failed batches are isolated and storage I/O
errors stop the campaign. Existing screening implementations remain untouched.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import errno
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import selectors
import signal
import stat
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from pipeline import speaker_screen as screen
from pipeline import speaker_screen_batch as old_batch
from pipeline import speaker_screen_archive_guided as guided
from pipeline import speaker_screen_paths as paths

ScreenError = screen.ScreenError


class ChildCleanupError(ScreenError):
    """Owned process cleanup is uncertain; no other batch may be launched."""


MARKER = {"kind": "himr_private_guided_speaker_screen_campaign_workspace", "schema_version": 1}
MAX_BATCHES = 128
MAX_PASSES = 8
MAX_SECONDS = 7 * 24 * 3600
MAX_STDOUT = 16 * 1024**2
MAX_STDERR = 2 * 1024**2
SNAPSHOT_SECONDS = 10
TERMINAL_BATCH_STATES = ("completed", "failed", "storage_error", "supervisor_error")
STOP_STATES = ("storage_error", "supervisor_error")
IMPLEMENTATION_NAMES = guided.IMPLEMENTATION_NAMES + ("speaker_screen_campaign.py",)
COUNT_KEYS = ("recordings", "screening_decisions_complete", "sampling_plans_completed", "completed_windows",
    "planned_windows", "baseline_planned_windows", "baseline_inspected_windows", "baseline_remaining_windows",
    "targeted_planned_windows", "targeted_inspected_windows", "targeted_remaining_windows",
    "multiple_speaker_candidate", "uncertain", "no_second_voice_detected_in_sampled_audio")


def _implementation():
    return {name: hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest()
            for name in IMPLEMENTATION_NAMES}


def _verify(manifest):
    if manifest["implementation"] != _implementation():
        raise ScreenError("campaign implementation changed")
    if manifest["python"] != old_batch._python_binding():
        raise ScreenError("campaign Python environment changed")


def _batch_document(binding):
    screen.file_binding(binding)
    value = screen.read_json(screen.path_value(binding["path"]), binding["sha256"])
    if (value.get("kind") != "himr_archive_guided_speaker_screen_manifest" or value.get("schema_version") != 1
            or type(value.get("schema_version")) is not int or not isinstance(value.get("plans"), list)
            or not 1 <= len(value["plans"]) <= old_batch.MAX_JOBS):
        raise ScreenError("campaign accepts only sealed guided batch manifests")
    if value.get("implementation") != guided._implementation():
        raise ScreenError("guided batch implementation differs from current reviewed sources")
    if screen.path_value(binding["path"]) != screen.path_value(value["state_root"]) / "manifest.json":
        raise ScreenError("guided batch manifest is outside its workspace")
    return value


def build_manifest(request, *, request_binding=None):
    screen.exact(request, {"kind", "schema_version", "campaign_root", "python", "batches",
                           "max_passes_per_batch", "max_run_seconds"}, "campaign request")
    if (request["kind"] != "himr_guided_speaker_screen_campaign_request"
            or type(request["schema_version"]) is not int or request["schema_version"] != 1):
        raise ScreenError("unsupported guided campaign request")
    screen.integer(request["max_passes_per_batch"], 1, MAX_PASSES, "maximum passes per batch")
    screen.integer(request["max_run_seconds"], 10, MAX_SECONDS, "campaign wall-clock limit")
    if request["python"] != old_batch._python_binding():
        raise ScreenError("plan the campaign with its exact isolated batch interpreter")
    references = request["batches"]
    if not isinstance(references, list) or not 1 <= len(references) <= MAX_BATCHES:
        raise ScreenError("campaign requires 1..128 explicit guided batches")
    root = screen.path_value(request["campaign_root"])
    protected = [Path(__file__).parent / name for name in IMPLEMENTATION_NAMES]
    protected += [Path(request["python"][key]) for key in ("path", "resolved_path")]
    if request["python"]["venv_configuration"] is not None:
        protected.append(Path(request["python"]["venv_configuration"]["path"]))
    if request_binding is not None:
        screen.file_binding(request_binding)
        protected.append(Path(request_binding["path"]))
    batches, seen_paths, seen_media, device = [], set(), set(), None
    for index, reference in enumerate(references):
        value = _batch_document(reference)
        if reference["path"] in seen_paths:
            raise ScreenError("campaign repeats a batch manifest")
        seen_paths.add(reference["path"])
        execution = guided.validate_execution(value["execution"])
        selected = (execution["device"], execution["gpu_uuid"])
        if device is not None and selected != device:
            raise ScreenError("campaign cannot mix execution devices or GPU identities")
        device = selected
        if value["runtime_binding"]["python"] != request["python"]:
            raise ScreenError("batch Python differs from campaign interpreter")
        protected.append(screen.path_value(value["state_root"]))
        protected += [screen.path_value(ref["path"]) for ref in value["guidance_sources"]]
        count = {key: 0 for key in COUNT_KEYS}
        for plan in value["plans"]:
            order = plan["order"]
            recording = order["recording"]
            if recording["media_id"] in seen_media:
                raise ScreenError("campaign repeats a recording identity")
            seen_media.add(recording["media_id"])
            protected += [screen.path_value(ref["path"]) for ref in (
                recording, order["ffmpeg"], order["models"]["silero_vad"], order["models"]["ecapa_embedding"], plan["work_order"])]
            protected.append(screen.path_value(plan["original_output_root"]))
            count["recordings"] += 1
            count["planned_windows"] += len(plan["windows"])
            count["baseline_planned_windows"] += len(plan["sampling"]["baseline_indices"])
            count["targeted_planned_windows"] += len(plan["sampling"]["target_indices"])
        count["baseline_remaining_windows"] = count["baseline_planned_windows"]
        count["targeted_remaining_windows"] = count["targeted_planned_windows"]
        batches.append({"index": index, "manifest": dict(reference), "batch_id": value["batch_id"],
                        "planned_counts": count, "batch_max_run_seconds": execution["max_run_seconds"]})
    if any(old_batch._overlap(root, target) for target in protected):
        raise ScreenError("campaign workspace overlaps an input or existing workspace")
    value = {"kind": "himr_guided_speaker_screen_campaign", "schema_version": 1,
        "request": request_binding, "campaign_root": str(root), "python": request["python"],
        "implementation": _implementation(), "batches": batches,
        "max_passes_per_batch": request["max_passes_per_batch"], "max_run_seconds": request["max_run_seconds"],
        "device": device[0], "gpu_uuid": device[1],
        "semantics": {"visibility": "private", "discovery": False, "source_mutation": False,
            "controller_mutation": False, "publication_authority": False, "identity_inferred": False,
            "batch_order_is_explicit": True, "failed_batches_are_not_completed": True,
            "storage_io_error_stops_campaign": True, "finite_pass_budget_persists_across_restarts": True}}
    result = {**value, "campaign_id": "guidedscreencampaign_" + screen.digest(value)[:32]}
    if len(screen.canonical(result)) > screen.MAX_JSON:
        raise ScreenError("campaign manifest exceeds JSON size limit")
    return result


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
        if create and not screen.exists(root / "workspace.json"):
            with os.scandir(directory) as entries:
                if next(entries, None) is not None:
                    raise ScreenError("refusing an unmarked nonempty campaign workspace")
            screen.write_immutable(root / "workspace.json", MARKER)
        if screen.read_json(root / "workspace.json") != MARKER:
            raise ScreenError("campaign workspace marker differs")
    return root


def create_campaign(request_path, expected, output):
    binding = {"path": str(screen.path_value(request_path)), "sha256": expected}
    screen.file_binding(binding)
    manifest = build_manifest(screen.read_json(Path(binding["path"]), expected), request_binding=binding)
    root = Path(manifest["campaign_root"])
    if screen.path_value(output) != root / "manifest.json":
        raise ScreenError("campaign manifest must be campaign_root/manifest.json")
    _workspace(root, create=True)
    _verify(manifest)
    screen.write_immutable(root / "manifest.json", manifest)
    return manifest


def _load_manifest(path, expected):
    value = screen.read_json(screen.path_value(path), expected)
    screen.exact(value, {"kind", "schema_version", "request", "campaign_root", "python", "implementation",
        "batches", "max_passes_per_batch", "max_run_seconds", "device", "gpu_uuid", "semantics", "campaign_id"},
        "campaign manifest")
    try:
        request = {"kind": "himr_guided_speaker_screen_campaign_request", "schema_version": 1,
            "campaign_root": value["campaign_root"], "python": value["python"],
            "batches": [row["manifest"] for row in value["batches"]],
            "max_passes_per_batch": value["max_passes_per_batch"], "max_run_seconds": value["max_run_seconds"]}
    except (KeyError, TypeError) as error:
        raise ScreenError("malformed campaign batch bindings") from error
    if build_manifest(request, request_binding=value["request"]) != value:
        raise ScreenError("campaign manifest or sealed inputs differ")
    if value["request"] is not None:
        original = screen.read_json(Path(value["request"]["path"]), value["request"]["sha256"])
        if build_manifest(original, request_binding=value["request"]) != value:
            raise ScreenError("campaign request changed")
    root = _workspace(value["campaign_root"])
    if screen.path_value(path) != root / "manifest.json":
        raise ScreenError("campaign manifest is outside its workspace")
    return value


def _check_lock(fd):
    value = os.fstat(fd)
    if (not stat.S_ISREG(value.st_mode) or value.st_uid != os.getuid() or value.st_nlink != 1
            or stat.S_IMODE(value.st_mode) != 0o600 or value.st_size):
        raise ScreenError("unsafe campaign execution lock")


@contextmanager
def _locked(root):
    with paths.retained_directory(root) as directory:
        descriptor = os.open("campaign.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory)
        try:
            _check_lock(descriptor)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ScreenError("another campaign supervisor or surviving child holds the lease") from None
            yield descriptor
        finally:
            os.close(descriptor)


def _atomic_snapshot(root, value):
    body = screen.canonical(value)
    if len(body) > screen.MAX_JSON:
        raise ScreenError("campaign status exceeds bounded JSON size")
    with paths.retained_directory(root) as directory:
        target = "status.json"
        if screen.exists(root / target):
            info = os.stat(target, dir_fd=directory, follow_symlinks=False)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1
                    or stat.S_IMODE(info.st_mode) != 0o600):
                raise ScreenError("unsafe campaign status snapshot")
        temporary = ".campaign-status-" + uuid.uuid4().hex
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target, src_dir_fd=directory, dst_dir_fd=directory)
            os.fsync(directory)
        finally:
            try:
                os.unlink(temporary, dir_fd=directory)
            except FileNotFoundError:
                pass


def _receipt_path(root, index, number, suffix):
    return root / f"batch-{index:04d}-pass-{number:02d}.{suffix}.json"


def _empty_state(batch):
    return {"index": batch["index"], "batch_id": batch["batch_id"], "state": "not_started",
            "passes_started": 0, "last_verified_counts": dict(batch["planned_counts"]),
            "last_verified_recordings": [], "errors": []}


def _validate_counts(batch, value):
    if not isinstance(value, dict) or any(key not in value for key in COUNT_KEYS):
        raise ScreenError("guided child omitted required screening counts")
    result = {key: value[key] for key in COUNT_KEYS}
    for key, count in result.items():
        screen.integer(count, 0, old_batch.MAX_JOBS * 576, "guided count " + key)
    expected = batch["planned_counts"]
    for key in ("recordings", "planned_windows", "baseline_planned_windows", "targeted_planned_windows"):
        if result[key] != expected[key]:
            raise ScreenError("guided child planned counts differ from its sealed batch")
    if (result["completed_windows"] > result["planned_windows"]
            or result["screening_decisions_complete"] > result["recordings"]
            or result["sampling_plans_completed"] > result["recordings"]):
        raise ScreenError("guided child completed counts exceed plan")
    for prefix in ("baseline", "targeted"):
        if result[prefix + "_inspected_windows"] + result[prefix + "_remaining_windows"] != result[prefix + "_planned_windows"]:
            raise ScreenError("guided child coverage counters differ")
    if result["completed_windows"] != result["baseline_inspected_windows"] + result["targeted_inspected_windows"]:
        raise ScreenError("guided child total coverage differs from baseline plus targeted")
    return result


def _project_child(batch, value):
    if (not isinstance(value, dict) or value.get("kind") != "himr_archive_guided_speaker_screen_status"
            or value.get("batch_id") != batch["batch_id"] or value.get("schema_version") != 1):
        raise ScreenError("child output belongs to a different guided batch")
    counts = _validate_counts(batch, value.get("counts"))
    rows = value.get("recordings")
    if not isinstance(rows, list) or len(rows) != counts["recordings"]:
        raise ScreenError("child recording report does not cover exact batch")
    projected = []
    for row in rows:
        if not isinstance(row, dict):
            raise ScreenError("invalid guided recording status")
        # Explicitly exclude private embeddings or arbitrary nested worker data.
        projected.append({key: row.get(key) for key in ("index", "original_index", "plan_id", "media_id",
            "title", "date", "priority", "sampling_state", "screening_decision_complete", "classification",
            "completed_windows", "planned_windows", "remaining_windows", "baseline_planned_windows",
            "baseline_inspected_windows", "baseline_remaining_windows", "targeted_planned_windows",
            "targeted_inspected_windows", "targeted_remaining_windows", "stop_reason")})
    state, invocation = value.get("state"), value.get("invocation_state")
    if state not in ("screening_complete", "paused") or invocation not in ("finished", "failed", "time_limit", "cancelled"):
        raise ScreenError("unrecognized guided child outcome")
    if (state == "screening_complete") != (counts["screening_decisions_complete"] == counts["recordings"]):
        raise ScreenError("guided child completion state differs from coverage")
    errors = value.get("errors")
    if not isinstance(errors, list) or len(errors) > 256:
        raise ScreenError("guided child error report exceeds bound")
    return {"counts": counts, "recordings": projected, "state": state, "invocation_state": invocation,
            "errors": [{"reason": str(row.get("reason", row))[:2000]} if isinstance(row, dict)
                       else {"reason": str(row)[:2000]} for row in errors]}


def _read_states(manifest, root):
    states = []
    for batch in manifest["batches"]:
        state = _empty_state(batch)
        missing = False
        for number in range(1, manifest["max_passes_per_batch"] + 1):
            start_path = _receipt_path(root, batch["index"], number, "start")
            finish_path = _receipt_path(root, batch["index"], number, "finish")
            if not screen.exists(start_path):
                missing = True
                if screen.exists(finish_path):
                    raise ScreenError("campaign pass finish has no start")
                continue
            if missing:
                raise ScreenError("campaign pass sequence has a gap")
            if state["state"] in TERMINAL_BATCH_STATES:
                raise ScreenError("campaign has another pass after a terminal batch result")
            start = screen.read_json(start_path)
            screen.exact(start, {"kind", "schema_version", "campaign_id", "batch_id", "index", "pass", "started_unix"}, "campaign pass start")
            if (start["kind"] != "himr_guided_speaker_screen_campaign_pass"
                    or start["schema_version"] != 1 or start["campaign_id"] != manifest["campaign_id"]
                    or start["batch_id"] != batch["batch_id"] or start["index"] != batch["index"] or start["pass"] != number):
                raise ScreenError("campaign pass start differs from manifest")
            state["passes_started"] = number
            state["state"] = "interrupted"
            if not screen.exists(finish_path):
                continue
            finish = screen.read_json(finish_path)
            screen.exact(finish, {"kind", "schema_version", "campaign_id", "batch_id", "index", "pass", "start_sha256",
                                 "outcome", "finished_unix"}, "campaign pass finish")
            if (finish["kind"] != "himr_guided_speaker_screen_campaign_pass_result"
                    or finish["schema_version"] != 1 or finish["campaign_id"] != manifest["campaign_id"]
                    or finish["batch_id"] != batch["batch_id"] or finish["index"] != batch["index"]
                    or finish["pass"] != number or finish["start_sha256"] != screen.digest(start)):
                raise ScreenError("campaign pass finish differs from start")
            outcome = finish["outcome"]
            if not isinstance(outcome, dict) or outcome.get("state") not in (*TERMINAL_BATCH_STATES, "paused", "cancelled"):
                raise ScreenError("invalid campaign pass outcome")
            state["state"] = outcome["state"]
            state["errors"] = outcome.get("errors", [])
            if outcome.get("screening") is not None:
                report = outcome["screening"]
                state["last_verified_counts"] = _validate_counts(batch, report["counts"])
                state["last_verified_recordings"] = report["recordings"]
            if state["state"] == "completed" and state["last_verified_counts"]["screening_decisions_complete"] != batch["planned_counts"]["recordings"]:
                raise ScreenError("campaign completed receipt lacks complete screening evidence")
        if state["state"] not in TERMINAL_BATCH_STATES and state["passes_started"] >= manifest["max_passes_per_batch"]:
            state["state"] = "failed"
            state["errors"] = [{"reason": "finite pass budget exhausted; remaining recordings are not completed"}]
        states.append(state)
    return states


def _status(manifest, states, *, state=None, active=None, started_unix=None):
    counts = {key: sum(row["last_verified_counts"][key] for row in states) for key in COUNT_KEYS}
    batches = {"total": len(states), "completed": sum(row["state"] == "completed" for row in states),
        "failed": sum(row["state"] in ("failed", *STOP_STATES) for row in states),
        "remaining": sum(row["state"] not in TERMINAL_BATCH_STATES for row in states)}
    if state is None:
        state = ("completed" if batches["completed"] == batches["total"] else
                 "finished_with_failures" if not batches["remaining"] else "paused")
    return {"kind": "himr_guided_speaker_screen_campaign_status", "schema_version": 1,
        "campaign_id": manifest["campaign_id"], "state": state, "updated_unix": time.time(),
        "started_unix": started_unix, "active": active, "batches": batches,
        "counts": counts, "batch_statuses": states,
        "semantics": {**manifest["semantics"], "counts_are_last_verified_pass_results": True,
            "active_batch_may_have_newer_committed_checkpoints": active is not None,
            "full_sampling_is_not_full_diarization": True}}


def _completed_receipts(document):
    """Best-effort live lower bounds, never authoritative checkpoint replay.

    Read only each sealed plan's exact immutable final-result path. No directory
    scan, partial checkpoint sequence, embedding access, or source-media reads.
    A concurrent initialization or unreadable receipt merely reduces telemetry;
    it cannot mask or replace the guided child's own validated final outcome.
    """
    count = windows = unreadable = 0
    for plan in document["plans"]:
        try:
            plan_id = plan["plan_id"]
            if not isinstance(plan_id, str) or not screen.IDENTIFIER.fullmatch(plan_id):
                raise ScreenError("invalid planned receipt identity")
            path = Path(document["state_root"]) / plan_id / "result.json"
            if not screen.exists(path):
                continue
            value = screen.read_json(path)
            planned = len(plan["windows"])
            if (value.get("kind") != "himr_archive_guided_speaker_screen_result"
                    or type(value.get("schema_version")) is not int or value["schema_version"] != 1
                    or value.get("plan_id") != plan_id or value.get("state") != "completed"
                    or value.get("screening_decision_complete") is not True
                    or value.get("recording") != plan["order"]["recording"]
                    or type(value.get("planned_windows")) is not int or value["planned_windows"] != planned
                    or type(value.get("completed_windows")) is not int or value["completed_windows"] != planned
                    or type(value.get("remaining_windows")) is not int or value["remaining_windows"] != 0):
                raise ScreenError("receipt does not establish a completed sampling plan")
            count += 1
            windows += value["completed_windows"]
        except (ScreenError, OSError, ValueError, KeyError, TypeError):
            unreadable += 1
    return {"active_batch_completed_receipts": count,
            "active_batch_completed_windows_lower_bound": windows,
            "active_batch_receipt_read_errors": unreadable,
            "active_batch_receipts_are_unverified_telemetry": True}


def _origin(manifest, root, *, create=False):
    path = root / "campaign-start.json"
    if not screen.exists(path):
        if not create:
            return None
        screen.write_immutable(path, {"kind": "himr_guided_speaker_screen_campaign_start", "schema_version": 1,
            "campaign_id": manifest["campaign_id"], "started_unix": time.time()})
    value = screen.read_json(path)
    screen.exact(value, {"kind", "schema_version", "campaign_id", "started_unix"}, "campaign origin")
    if (value["kind"] != "himr_guided_speaker_screen_campaign_start" or value["schema_version"] != 1
            or value["campaign_id"] != manifest["campaign_id"] or type(value["started_unix"]) not in (int, float)
            or not math.isfinite(value["started_unix"]) or value["started_unix"] < 0):
        raise ScreenError("campaign origin differs")
    return value["started_unix"]


def _io_error(value):
    if isinstance(value, OSError) and value.errno == errno.EIO:
        return True
    text = str(value).casefold()
    return "[errno 5]" in text or "input/output error" in text


def _decode_json(body):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ScreenError("duplicate child JSON key")
            result[key] = value
        return result
    try:
        return json.loads(body, object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ScreenError("nonfinite child JSON")))
    except (ValueError, UnicodeError, RecursionError) as error:
        raise ScreenError("guided child did not emit bounded valid JSON") from error


def _terminate(process):
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired as error:
            raise ChildCleanupError("owned guided child could not be reaped after SIGKILL") from error


def _command(manifest, batch):
    return [manifest["python"]["path"], "-B", str(Path(guided.__file__).resolve()), "run",
            "--manifest", batch["manifest"]["path"], "--expected-sha256", batch["manifest"]["sha256"]]


def _execute_batch(manifest, batch, lock_fd, deadline, progress):
    _verify(manifest)
    # Recheck this exact immutable input immediately before spawning. The guided
    # child independently rebuilds/validates its complete plan and all guidance.
    _batch_document(batch["manifest"])
    command = _command(manifest, batch)
    environment = dict(os.environ)
    environment.update(PYTHONDONTWRITEBYTECODE="1", PYTHONNOUSERSITE="1")
    process = None
    # Guided wall-clock bounds start after metadata validation. Allow bounded
    # startup/cleanup overhead, then enforce an independent parent watchdog.
    deadline = min(deadline, time.monotonic() + batch["batch_max_run_seconds"] + 120)
    streams = {"stdout": bytearray(), "stderr": bytearray()}
    last_update = 0.0
    limited = False
    drain_deadline = None
    try:
        with old_batch._deferred_launch_signals() as inherited_mask:
            parent_pid = os.getpid()

            def child_setup():
                # Do not leak the parent's temporary spawn-critical-section
                # signal mask through exec; the guided child must receive
                # SIGTERM so its own model/decoder cleanup can run promptly.
                screen.die_with_parent(parent_pid)
                signal.pthread_sigmask(signal.SIG_SETMASK, inherited_mask)

            process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                close_fds=True, pass_fds=(lock_fd,), start_new_session=True, preexec_fn=child_setup, env=environment)
        with selectors.DefaultSelector() as selector:
            for name in streams:
                stream = getattr(process, name)
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, name)
            while selector.get_map():
                now = time.monotonic()
                if process.poll() is not None:
                    if drain_deadline is None:
                        drain_deadline = now + 2
                    elif now >= drain_deadline:
                        raise ChildCleanupError("guided child exited but owned output pipes did not close")
                if now >= deadline:
                    limited = True
                    _terminate(process)
                if now - last_update >= SNAPSHOT_SECONDS:
                    progress(process.pid)
                    last_update = now
                for key, _ in selector.select(timeout=min(1.0, max(0.01, deadline - now))):
                    body = os.read(key.fileobj.fileno(), 65536)
                    if not body:
                        selector.unregister(key.fileobj)
                        continue
                    selected = streams[key.data]
                    if len(selected) + len(body) > (MAX_STDOUT if key.data == "stdout" else MAX_STDERR):
                        raise ScreenError("guided child output exceeded its bounded capture limit")
                    selected.extend(body)
                if limited and process.poll() is not None and not selector.get_map():
                    break
        returncode = process.wait(timeout=5)
        stderr = bytes(streams["stderr"]).decode("utf-8", errors="replace")[-4096:]
        report = None
        errors = []
        if streams["stdout"]:
            try:
                report = _project_child(batch, _decode_json(bytes(streams["stdout"])))
            except ScreenError as error:
                errors.append({"reason": str(error)})
        else:
            errors.append({"reason": "guided child emitted no screening status"})
        if report is not None:
            errors += report["errors"]
        if returncode != 0:
            errors.append({"reason": f"guided child exited {returncode}: {stderr}"})
        if any(_io_error(row["reason"]) for row in errors) or _io_error(stderr):
            state = "storage_error"
        elif limited:
            state = "cancelled"
            errors.append({"reason": "finite campaign or guided-child watchdog limit reached"})
        elif report is not None and report["invocation_state"] == "cancelled":
            state = "cancelled"
        elif errors or report is None or report["invocation_state"] == "failed":
            state = "failed"
        else:
            state = "completed" if report["state"] == "screening_complete" else "paused"
        return {"state": state, "exit_code": returncode, "screening": report, "errors": errors}
    finally:
        if process is not None:
            with old_batch._deferred_launch_signals():
                try:
                    _terminate(process)
                finally:
                    for name in streams:
                        stream = getattr(process, name)
                        if stream is not None:
                            stream.close()


def run_campaign(path, expected):
    manifest = _load_manifest(path, expected)
    root = Path(manifest["campaign_root"])
    with old_batch._cancellable(), _locked(root) as lock_fd:
        states = _read_states(manifest, root)
        origin = _origin(manifest, root, create=True)
        remaining = max(0.0, manifest["max_run_seconds"] - (time.time() - origin))
        deadline = time.monotonic() + min(remaining, manifest["max_run_seconds"])
        state = None
        stopped = next((row["state"] for row in states if row["state"] in STOP_STATES), None)
        if stopped is not None:
            result = _status(manifest, states, state=stopped, started_unix=origin)
            _atomic_snapshot(root, result)
            return result
        try:
            for batch in manifest["batches"]:
                index = batch["index"]
                while states[index]["state"] not in TERMINAL_BATCH_STATES:
                    if time.monotonic() >= deadline:
                        state = "time_limit"
                        break
                    number = states[index]["passes_started"] + 1
                    if number > manifest["max_passes_per_batch"]:
                        break
                    _verify(manifest)
                    started = time.time()
                    start = {"kind": "himr_guided_speaker_screen_campaign_pass", "schema_version": 1,
                        "campaign_id": manifest["campaign_id"], "batch_id": batch["batch_id"],
                        "index": index, "pass": number, "started_unix": started}
                    screen.write_immutable(_receipt_path(root, index, number, "start"), start)
                    states[index]["passes_started"] = number
                    states[index]["state"] = "running"
                    active_document = None

                    def progress(pid):
                        nonlocal active_document
                        try:
                            if active_document is None:
                                active_document = _batch_document(batch["manifest"])
                            telemetry = _completed_receipts(active_document)
                        except (ScreenError, OSError, ValueError, KeyError, TypeError):
                            telemetry = {"active_batch_completed_receipts": 0,
                                "active_batch_completed_windows_lower_bound": 0,
                                "active_batch_receipt_read_errors": 1,
                                "active_batch_receipts_are_unverified_telemetry": True}
                        _atomic_snapshot(root, _status(manifest, states, state="running", started_unix=origin,
                            active={"batch_index": index, "pass": number, "pid": pid,
                                    "started_unix": started, "elapsed_seconds": max(0.0, time.time() - started),
                                    **telemetry}))

                    progress(None)
                    try:
                        outcome = _execute_batch(manifest, batch, lock_fd, deadline, progress)
                    except KeyboardInterrupt:
                        outcome = {"state": "cancelled", "exit_code": None, "screening": None,
                                   "errors": [{"reason": "campaign interrupted; committed guided checkpoints remain resumable"}]}
                    except (ScreenError, OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
                        outcome = {"state": "storage_error" if _io_error(error) else "supervisor_error" if isinstance(error, ChildCleanupError) else "failed", "exit_code": None,
                                   "screening": None, "errors": [{"reason": f"{type(error).__name__}: {str(error)[:2000]}"}]}
                    _verify(manifest)
                    screen.write_immutable(_receipt_path(root, index, number, "finish"), {
                        "kind": "himr_guided_speaker_screen_campaign_pass_result", "schema_version": 1,
                        "campaign_id": manifest["campaign_id"], "batch_id": batch["batch_id"], "index": index,
                        "pass": number, "start_sha256": screen.digest(start), "outcome": outcome, "finished_unix": time.time()})
                    states = _read_states(manifest, root)
                    _atomic_snapshot(root, _status(manifest, states, started_unix=origin))
                    if outcome["state"] in ("cancelled", *STOP_STATES):
                        state = "time_limit" if time.monotonic() >= deadline else outcome["state"]
                        break
                if state is not None:
                    break
        except KeyboardInterrupt:
            state = "cancelled"
        states = _read_states(manifest, root)
        _verify(manifest)
        result = _status(manifest, states, state=state, started_unix=origin)
        _atomic_snapshot(root, result)
        return result


def status_campaign(path, expected):
    manifest = _load_manifest(path, expected)
    root = Path(manifest["campaign_root"])
    with paths.retained_directory(root) as directory:
        descriptor = None
        try:
            try:
                descriptor = os.open("campaign.lock", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
            except FileNotFoundError:
                pass
            if descriptor is not None:
                _check_lock(descriptor)
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
                except BlockingIOError:
                    if screen.exists(root / "status.json"):
                        value = screen.read_json(root / "status.json")
                        if value.get("campaign_id") != manifest["campaign_id"]:
                            raise ScreenError("campaign snapshot belongs to another manifest")
                        return {**value, "supervisor_lease_active": True, "snapshot_is_atomic": True,
                                "snapshot_age_seconds": max(0.0, time.time() - value["updated_unix"])}
                    return {"kind": "himr_guided_speaker_screen_campaign_status", "schema_version": 1,
                            "campaign_id": manifest["campaign_id"], "state": "running", "counts": None,
                            "supervisor_lease_active": True, "snapshot_deferred": True}
            states = _read_states(manifest, root)
            origin = _origin(manifest, root)
            state = next((row["state"] for row in states if row["state"] in STOP_STATES), None)
            if state is None and origin is not None and time.time() - origin >= manifest["max_run_seconds"]:
                state = "time_limit"
            return {**_status(manifest, states, state=state, started_unix=origin), "supervisor_lease_active": False}
        finally:
            if descriptor is not None:
                os.close(descriptor)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    planner = commands.add_parser("plan")
    planner.add_argument("--request", required=True)
    planner.add_argument("--expected-sha256", required=True)
    planner.add_argument("--output", required=True)
    for command in ("run", "status"):
        selected = commands.add_parser(command)
        selected.add_argument("--manifest", required=True)
        selected.add_argument("--expected-sha256", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            value = create_campaign(args.request, args.expected_sha256, args.output)
        elif args.command == "run":
            value = run_campaign(args.manifest, args.expected_sha256)
        else:
            value = status_campaign(args.manifest, args.expected_sha256)
        print(json.dumps(value, sort_keys=True, indent=2, allow_nan=False))
        return 130 if value.get("state") == "cancelled" else 2 if value.get("state") in (
            "finished_with_failures", "storage_error", "supervisor_error", "time_limit") else 0
    except KeyboardInterrupt:
        print("Guided campaign interrupted; committed checkpoints remain intact.", file=sys.stderr)
        return 130
    except (ScreenError, OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"GuidedSpeakerScreenCampaignError: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
