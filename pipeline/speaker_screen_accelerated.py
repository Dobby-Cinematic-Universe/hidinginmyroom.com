"""Explicit, finite resident CPU/CUDA speaker screens, isolated from CPU v1/ASR.

One model pair survives across recordings. Two bounded decoder processes can
prepare the next fixed probe batch while inference runs. Evidence is atomic per
fixed batch, recording-local, private, and never an identity index.
"""
from __future__ import annotations

import argparse
import base64
from contextlib import contextmanager
import copy
import fcntl
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import re
import resource
import signal
import socket
import stat
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from pipeline import speaker_screen as screen
from pipeline import speaker_screen_batch as old_batch
from pipeline import speaker_screen_core as core
from pipeline import speaker_screen_paths as paths
from pipeline import speaker_screen_accelerated_engine as engine
from pipeline import speaker_screen_accelerated_worker as worker_api

ScreenError = screen.ScreenError


class InvocationLimit(ScreenError):
    """A normal bounded pause; only fully committed batches survive."""


MARKER = {"kind": "himr_private_resident_speaker_screen_workspace", "schema_version": 1}
DEFAULT_EXECUTION = {"device": "cpu", "threads": 1, "batch_size": 8, "decode_prefetch": 2,
                     "max_run_seconds": 3600, "cuda_memory_fraction": 0.5,
                     "gpu_uuid": None, "host_memory_max_bytes": 4 * 1024**3}
MAX_RUNS = 4096
IMPLEMENTATION_NAMES = (
    "speaker_screen.py", "speaker_screen_core.py", "speaker_screen_engine.py",
    "speaker_screen_paths.py", "speaker_screen_batch.py", "speaker_screen_accelerated.py",
    "speaker_screen_accelerated_engine.py", "speaker_screen_accelerated_worker.py")


def _implementation():
    return {name: hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest()
            for name in IMPLEMENTATION_NAMES}


def validate_execution(value):
    if not isinstance(value, dict) or set(value) - set(DEFAULT_EXECUTION):
        raise ScreenError("unknown resident execution fields")
    value = {**DEFAULT_EXECUTION, **value}
    if value["device"] not in ("cpu", "cuda"):
        raise ScreenError("resident device must be cpu or cuda")
    for key, low, high in (("threads", 1, 2), ("batch_size", 1, 16),
                           ("decode_prefetch", 1, 2), ("max_run_seconds", 10, 86400)):
        screen.integer(value[key], low, high, key)
    fraction = value["cuda_memory_fraction"]
    if (isinstance(fraction, bool) or not isinstance(fraction, (int, float))
            or not math.isfinite(fraction) or not 0.1 <= fraction <= 0.75):
        raise ScreenError("CUDA allocator fraction must be finite in 0.1..0.75")
    value["cuda_memory_fraction"] = float(fraction)
    if type(value["host_memory_max_bytes"]) is not int or value["host_memory_max_bytes"] != 4 * 1024**3:
        raise ScreenError("resident host memory ceiling must be exactly 4 GiB")
    if value["device"] == "cuda":
        if not isinstance(value["gpu_uuid"], str) or not re.fullmatch(
                r"GPU-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", value["gpu_uuid"]):
            raise ScreenError("CUDA requires an explicit NVIDIA GPU UUID")
    elif value["gpu_uuid"] is not None:
        raise ScreenError("CPU execution cannot select a GPU")
    return value


def _runtime_binding(execution):
    versions = engine.cpu.runtime_versions()
    expected = engine.CPU_RUNTIME_PINS if execution["device"] == "cpu" else engine.CUDA_RUNTIME_PINS
    if versions != expected:
        raise ScreenError("select the reviewed isolated CPU or CUDA Python environment")
    driver = None
    if execution["device"] == "cuda":
        raw = worker_api._small_text("/proc/driver/nvidia/version")
        match = re.search(r"^NVRM version:.*?\b([0-9]{3,4}\.[0-9]{1,3}(?:\.[0-9]{1,3})?)\b", raw, re.MULTILINE)
        if not match:
            raise ScreenError("cannot bind the NVIDIA driver version")
        driver = match.group(1)
    return {"python": old_batch._python_binding(), "versions": versions,
            "recipe": engine.model_recipe(execution["device"]), "nvidia_driver_version": driver}


def build_manifest(request, *, request_binding=None):
    screen.exact(request, {"kind", "schema_version", "work_orders", "state_root", "execution"}, "resident request")
    if (request["kind"] != "himr_resident_speaker_screen_request"
            or type(request["schema_version"]) is not int or request["schema_version"] != 1):
        raise ScreenError("unsupported resident request")
    execution = validate_execution(request["execution"])
    runtime = _runtime_binding(execution)
    original_plans = old_batch._load_orders(request["work_orders"])
    root = screen.path_value(request["state_root"])
    if request_binding is not None:
        screen.file_binding(request_binding)
    old_batch._check_paths(root, request["work_orders"], original_plans,
                          request_path=None if request_binding is None else request_binding["path"],
                          python=runtime["python"])
    for name in IMPLEMENTATION_NAMES:
        if old_batch._overlap(root, Path(__file__).parent / name):
            raise ScreenError("resident workspace overlaps an implementation file")
    models = original_plans[0]["order"]["models"]
    if any(plan["order"]["models"] != models for plan in original_plans):
        raise ScreenError("resident batch requires one identical hash-bound model pair")
    implementation = _implementation()
    plans = []
    for index, (reference, original) in enumerate(zip(request["work_orders"], original_plans)):
        order = copy.deepcopy(original["order"])
        # Original orders are read-only templates. Their output roots are never used.
        original_output = order["output_root"]
        order["output_root"] = str(root)
        windows = core.plan_windows(order["recording"]["duration_ms"], order["policy"])
        width = min(execution["batch_size"], order["resources"]["max_windows_per_run"])
        value = {"kind": "himr_resident_speaker_screen_plan", "schema_version": 1,
                 "index": index, "work_order": dict(reference), "original_output_root": original_output,
                 "order": order, "execution": execution, "runtime_binding": runtime,
                 "implementation": implementation, "windows": windows,
                 "batches": [windows[start:start + width] for start in range(0, len(windows), width)]}
        plans.append({**value, "plan_id": "residentscreen_" + screen.digest(value)[:32]})
    value = {"kind": "himr_resident_speaker_screen_manifest", "schema_version": 1,
             "request": request_binding, "state_root": str(root), "execution": execution,
             "runtime_binding": runtime, "implementation": implementation, "models": models,
             "plans": plans, "semantics": {"visibility": "private", "discovery": False,
                 "controller_mutation": False, "source_mutation": False, "publication_authority": False,
                 "person_identity_claimed": False, "cross_recording_evidence_cache": False,
                 "models_resident_across_recordings": True, "fixed_atomic_probe_batches": True,
                 "one_bounded_invocation_per_recording_per_run": True}}
    return {**value, "batch_id": "residentbatch_" + screen.digest(value)[:32]}


def validate_manifest(value):
    screen.exact(value, {"kind", "schema_version", "request", "state_root", "execution", "runtime_binding",
                         "implementation", "models", "plans", "semantics", "batch_id"}, "resident manifest")
    if not isinstance(value["plans"], list) or not 1 <= len(value["plans"]) <= old_batch.MAX_JOBS:
        raise ScreenError("resident manifest requires 1..128 exact plans")
    try:
        request = {"kind": "himr_resident_speaker_screen_request", "schema_version": 1,
                   "work_orders": [plan["work_order"] for plan in value["plans"]],
                   "state_root": value["state_root"], "execution": value["execution"]}
    except (TypeError, KeyError) as error:
        raise ScreenError("malformed resident plans") from error
    if build_manifest(request, request_binding=value["request"]) != value:
        raise ScreenError("resident manifest, input, implementation, or runtime differs")
    if value["request"] is not None:
        original = screen.read_json(Path(value["request"]["path"]), value["request"]["sha256"])
        if build_manifest(original, request_binding=value["request"]) != value:
            raise ScreenError("resident request differs from sealed manifest")
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
        if not screen.exists(root / "workspace.json") and create:
            with os.scandir(directory) as entries:
                if next(entries, None) is not None:
                    raise ScreenError("refusing an unmarked nonempty resident workspace")
            screen.write_immutable(root / "workspace.json", MARKER)
        if screen.read_json(root / "workspace.json") != MARKER:
            raise ScreenError("resident workspace marker differs")
    return root


def seal_manifest(request_path, expected_sha256, output):
    binding = {"path": str(screen.path_value(request_path)), "sha256": expected_sha256}
    screen.file_binding(binding)
    value = build_manifest(screen.read_json(Path(binding["path"]), expected_sha256), request_binding=binding)
    root = Path(value["state_root"])
    if screen.path_value(output) != root / "manifest.json":
        raise ScreenError("manifest must be state_root/manifest.json")
    _workspace(root, create=True)
    screen.write_immutable(root / "manifest.json", value)
    return value


def _load_manifest(path, expected):
    binding = {"path": str(screen.path_value(path)), "sha256": expected}
    screen.file_binding(binding)
    value = validate_manifest(screen.read_json(Path(path), expected))
    if Path(path) != Path(value["state_root"]) / "manifest.json":
        raise ScreenError("resident manifest is outside its workspace")
    _workspace(Path(value["state_root"]))
    return value


@contextmanager
def _locked(root):
    with paths.retained_directory(root) as directory:
        if screen.read_json(root / "workspace.json") != MARKER:
            raise ScreenError("resident workspace marker differs")
        fd = os.open("resident.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size):
                raise ScreenError("unsafe resident execution lock")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ScreenError("another resident screen or surviving child holds this workspace") from None
            yield fd
        finally:
            os.close(fd)


def _checkpoint_path(job, index):
    return job / f"batch-{index:04d}.json"


def _validate_runtime(plan, runtime):
    screen.exact(runtime, {"models", "recipe", "threads", "device", "batch_size", "cuda_memory_fraction",
                           "runtime_versions", "cuda", "requested_gpu_uuid", "nvidia_driver_version"}, "resident runtime")
    execution = plan["execution"]
    expected = {"models": plan["order"]["models"], "recipe": plan["runtime_binding"]["recipe"],
                "threads": execution["threads"], "device": execution["device"], "batch_size": execution["batch_size"],
                "runtime_versions": plan["runtime_binding"]["versions"], "requested_gpu_uuid": execution["gpu_uuid"],
                "nvidia_driver_version": plan["runtime_binding"]["nvidia_driver_version"],
                "cuda_memory_fraction": execution["cuda_memory_fraction"] if execution["device"] == "cuda" else None}
    if any(screen.canonical({"value": runtime[key]}) != screen.canonical({"value": value}) for key, value in expected.items()):
        raise ScreenError("resident runtime provenance differs from sealed plan")
    if execution["device"] == "cpu":
        if runtime["cuda"] is not None:
            raise ScreenError("CPU checkpoints cannot contain a CUDA runtime")
        return
    cuda = runtime["cuda"]
    screen.exact(cuda, {"logical_device", "name", "total_memory_bytes", "capability", "cuda_runtime",
                       "memory_fraction", "torch_allocator_limit_bytes"}, "CUDA provenance")
    if type(cuda["logical_device"]) is not int or cuda["logical_device"] != 0 or cuda["cuda_runtime"] != "12.8":
        raise ScreenError("CUDA checkpoint device/runtime differs")
    screen.integer(cuda["total_memory_bytes"], 1024**3, 1024**4, "GPU memory")
    if not isinstance(cuda["name"], str) or not 1 <= len(cuda["name"]) <= 256:
        raise ScreenError("CUDA checkpoint has no bounded GPU name")
    if not isinstance(cuda["capability"], list) or len(cuda["capability"]) != 2:
        raise ScreenError("invalid CUDA capability")
    for value in cuda["capability"]:
        screen.integer(value, 0, 100, "CUDA capability")
    if (type(cuda["memory_fraction"]) is not float or cuda["memory_fraction"] != execution["cuda_memory_fraction"]
            or type(cuda["torch_allocator_limit_bytes"]) is not int
            or cuda["torch_allocator_limit_bytes"] != int(cuda["total_memory_bytes"] * execution["cuda_memory_fraction"])):
        raise ScreenError("CUDA checkpoint allocator bound differs")


def _validate_observation(observation):
    if not isinstance(observation, dict):
        raise ScreenError("resident observation must be an object")
    vector = observation.get("embedding")
    if vector is not None and (not isinstance(vector, list) or len(vector) != 192
            or any(type(value) not in (int, float) or not math.isfinite(value) for value in vector)
            or not math.isclose(math.hypot(*vector), 1.0, rel_tol=1e-6, abs_tol=1e-6)):
        raise ScreenError("resident checkpoint embedding must be normalized and 192-dimensional")


def _observations_for(plan, job, binding, *, cache=None):
    observations, proofs, runtime, completed_batches = [], [], None, set()
    missing_seen = False
    for index, windows in enumerate(plan["batches"]):
        path = _checkpoint_path(job, index)
        if not screen.exists(path):
            missing_seen = True
            continue
        if missing_seen:
            raise ScreenError("resident checkpoint sequence has a gap")
        value = screen.read_json(path)
        screen.exact(value, {"kind", "schema_version", "plan_id", "binding_sha256", "batch_index",
                             "window_results", "runtime"}, "resident batch checkpoint")
        if (value["kind"] != "himr_resident_speaker_screen_checkpoint"
                or type(value["schema_version"]) is not int or value["schema_version"] != 1
                or type(value["batch_index"]) is not int or value["batch_index"] != index
                or value["plan_id"] != plan["plan_id"] or value["binding_sha256"] != screen.digest(binding)
                or not isinstance(value["window_results"], list) or len(value["window_results"]) != len(windows)
                or not isinstance(value["runtime"], dict) or not value["runtime"]):
            raise ScreenError("resident checkpoint differs from plan/source/batch")
        _validate_runtime(plan, value["runtime"])
        if runtime is not None and runtime != value["runtime"]:
            raise ScreenError("resident checkpoints mix incompatible runtimes")
        runtime = value["runtime"]
        for item, window in zip(value["window_results"], windows):
            screen.exact(item, {"window", "pcm_sha256", "observation"}, "resident window evidence")
            if (screen.canonical(item["window"]) != screen.canonical(window)
                    or not isinstance(item["pcm_sha256"], str) or not screen.SHA.fullmatch(item["pcm_sha256"])
                    or not isinstance(item["observation"], dict)
                    or item["observation"].get("index") != window["index"]):
                raise ScreenError("resident evidence window differs")
            _validate_observation(item["observation"])
            observations.append(item["observation"])
        completed_batches.add(index)
        proofs.append({"batch_index": index, "sha256": screen.digest(value)})
    summary = core.summarize(plan["order"]["recording"]["duration_ms"], plan["windows"],
                             observations, plan["order"]["policy"], cache=cache)
    return observations, proofs, runtime, summary, completed_batches


def _result_document(plan, binding, observations, proofs, runtime, summary):
    complete = len(observations) == len(plan["windows"])
    positive = bool(plan["order"]["resources"].get("early_stop_on_positive", False)
                    and summary["status"] == "multiple_speaker_candidate")
    return {"kind": "himr_resident_speaker_screen_result", "schema_version": 1,
            "plan_id": plan["plan_id"], "recording": plan["order"]["recording"],
            "state": "completed" if complete else "paused", "completed_windows": len(observations),
            "planned_windows": len(plan["windows"]), "remaining_windows": len(plan["windows"]) - len(observations),
            "screening_decision_complete": complete or positive,
            "stop_reason": "sampling_plan_completed" if complete else "supported_multiple_speakers" if positive else "invocation_limit",
            "source_binding": binding, "runtime": runtime, "checkpoint_hashes": proofs, "summary": summary,
            "policy": {"visibility": "private", "cpu_only": plan["execution"]["device"] == "cpu",
                       "device": plan["execution"]["device"], "scores_calibrated": False,
                       "human_review_required": True, "person_identity_claimed": False,
                       "full_diarization": False, "whole_recording_solo_claim": False,
                       "catalogue_mutation": False, "source_mutation": False, "publication_authority": "none"}}


def _read_job(plan, root):
    job = root / plan["plan_id"]
    initial = {"plan_id": plan["plan_id"], "state": "not_started", "planned_windows": len(plan["windows"])}
    if not screen.exists(job):
        return initial
    with paths.retained_directory(job) as directory:
        if not screen.exists(job / "source-binding.json"):
            with os.scandir(directory) as entries:
                names = {entry.name for entry in entries}
            if not names <= {"plan.json"}:
                raise ScreenError("incomplete resident initialization contains unexpected evidence")
            if "plan.json" in names and screen.read_json(job / "plan.json") != plan:
                raise ScreenError("saved resident plan differs")
            return initial
        if screen.read_json(job / "plan.json") != plan:
            raise ScreenError("saved resident plan differs")
        binding = screen.read_json(job / "source-binding.json")
        screen.validate_binding(binding, plan)
        observation, proofs, runtime, summary, _ = _observations_for(plan, job, binding)
        result = _result_document(plan, binding, observation, proofs, runtime, summary)
        if screen.exists(job / "result.json") and screen.read_json(job / "result.json") != result:
            raise ScreenError("sealed resident result differs from checkpoint replay")
        return result


def _projection(plan, value):
    return {"index": plan["index"], "plan_id": plan["plan_id"],
            "media_id": plan["order"]["recording"]["media_id"],
            "sampling_state": value["state"],
            "screening_decision_complete": value.get("screening_decision_complete", False),
            "classification": value.get("summary", {}).get("status"),
            "completed_windows": value.get("completed_windows", 0), "planned_windows": len(plan["windows"]),
            "remaining_windows": value.get("remaining_windows", len(plan["windows"])),
            "coverage": value.get("summary", {}).get("coverage"),
            "reason_flags": value.get("summary", {}).get("reason_flags", []),
            "stop_reason": value.get("stop_reason"), "source_currently_rechecked": False}


def _summary(manifest, rows, *, invocation_state=None, errors=None, metrics=None):
    counts = {"recordings": len(rows), "screening_decisions_complete": sum(row["screening_decision_complete"] for row in rows),
              "sampling_plans_completed": sum(row["sampling_state"] == "completed" for row in rows),
              "completed_windows": sum(row["completed_windows"] for row in rows),
              "planned_windows": sum(row["planned_windows"] for row in rows)}
    counts.update({name: sum(row["classification"] == name for row in rows) for name in sorted(old_batch.KINDS)})
    return {"kind": "himr_resident_speaker_screen_status", "schema_version": 1,
            "batch_id": manifest["batch_id"], "device": manifest["execution"]["device"],
            "state": "screening_complete" if counts["screening_decisions_complete"] == len(rows) else "paused",
            "invocation_state": invocation_state, "counts": counts, "recordings": rows, "errors": errors or [],
            "metrics": metrics, "semantics": {**manifest["semantics"], "source_currently_rechecked": False,
                "completed_sampling_is_not_complete_diarization": True, "whole_recording_solo_claimed": False}}


def status_batch(path, expected):
    manifest = _load_manifest(path, expected)
    root = Path(manifest["state_root"])
    with paths.retained_directory(root) as directory:
        descriptor = None
        try:
            try:
                descriptor = os.open("resident.lock", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
            except FileNotFoundError:
                pass
            if descriptor is not None:
                info = os.fstat(descriptor)
                if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                        or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size):
                    raise ScreenError("unsafe resident status lock")
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
                except BlockingIOError:
                    return {"kind": "himr_resident_speaker_screen_status", "schema_version": 1,
                            "batch_id": manifest["batch_id"], "device": manifest["execution"]["device"],
                            "state": "running", "counts": None, "snapshot_deferred": True,
                            "reason": "an active writer owns the checkpoint sequence", "errors": []}
            rows = [_projection(plan, _read_job(plan, root)) for plan in manifest["plans"]]
        finally:
            if descriptor is not None:
                os.close(descriptor)
    return _summary(manifest, rows)


def _close_process(process, connection):
    try:
        connection.close()
    finally:
        if process.pid is not None and process.is_alive():
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                process.kill()
        if process.pid is not None:
            process.join(timeout=2)
            if process.is_alive():
                process.kill()
                process.join(timeout=2)
            if process.is_alive():
                raise ScreenError("resident decoder could not be reaped")


def _decode_child(connection, parent_pid, implementation, cpu_seconds, inherited_mask, lease):
    lease_fd = None
    try:
        os.setsid()
        screen.die_with_parent(parent_pid)
        signal.pthread_sigmask(signal.SIG_SETMASK, inherited_mask)
        if lease is not None:
            lease_fd = lease.detach()
        os.environ.clear()
        os.environ.update(old_batch._child_environment(1))
        resource.setrlimit(resource.RLIMIT_CPU, (cpu_seconds, cpu_seconds))
        screen.child_limits(screen.DECODE_ADDRESS_SPACE_BYTES)
        worker_api.verify_implementation(implementation)
        deadline = time.monotonic() + cpu_seconds
        while True:
            request = worker_api.receive_packet(connection, deadline)
            screen.exact(request, {"recording", "ffmpeg", "source_witness", "ffmpeg_witness", "windows", "timeout"}, "decode request")
            if not isinstance(request["windows"], list) or not 1 <= len(request["windows"]) <= 16:
                raise ScreenError("decode request exceeds bounded windows")
            timeout = request["timeout"]
            if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or not 0 < timeout <= 600:
                raise ScreenError("invalid decode timeout")
            worker_api.verify_implementation(implementation)
            decoded = []
            with screen.opened(request["recording"]["path"]) as source, screen.opened(request["ffmpeg"]["path"], executable=True) as ffmpeg:
                if screen.witness(source) != request["source_witness"] or screen.witness(ffmpeg) != request["ffmpeg_witness"]:
                    raise ScreenError("decoder source or executable changed")
                for window in request["windows"]:
                    started = time.monotonic()
                    pcm = screen.decode_window(source, ffmpeg, window, min(timeout, screen.remaining(deadline)))
                    if screen.witness(source) != request["source_witness"] or screen.witness(ffmpeg) != request["ffmpeg_witness"]:
                        raise ScreenError("decoder input changed during probe")
                    decoded.append({"window": window, "pcm_base64": base64.b64encode(pcm).decode("ascii"),
                                    "decode_seconds": time.monotonic() - started})
            worker_api.verify_implementation(implementation)
            worker_api.send_packet(connection, {"decoded": decoded}, deadline)
    except EOFError:
        pass
    except BaseException as error:
        try:
            worker_api.send_packet(connection, {"error": f"{type(error).__name__}: {str(error)[:1200]}"}, time.monotonic() + 1)
        except BaseException:
            pass
    finally:
        connection.close()
        if lease_fd is not None:
            os.close(lease_fd)


class DecodePool:
    """Bounded spawned decoders; at most one future fixed batch is outstanding."""
    def __init__(self, count, implementation, *, max_run_seconds, lock_fd=None):
        self.workers = []
        self.active = None
        self.decoded_seconds = 0.0
        self.worker_starts = 0
        self.config = (count, implementation, max_run_seconds, lock_fd)
        self._start()

    def _start(self):
        count, implementation, max_run_seconds, lock_fd = self.config
        context = multiprocessing.get_context("spawn")
        try:
            for _ in range(count):
                parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
                process = None
                try:
                    with old_batch._deferred_launch_signals() as mask:
                        lease = None if lock_fd is None else worker_api.InheritedLease(lock_fd)
                        process = context.Process(target=_decode_child, args=(child, os.getpid(), implementation,
                            max_run_seconds + 10, mask, lease), daemon=True)
                        process.start()
                        self.workers.append((process, parent))
                        self.worker_starts += 1
                except BaseException:
                    if process is not None and (process, parent) not in self.workers:
                        _close_process(process, parent)
                    else:
                        parent.close() if process is None else None
                    raise
                finally:
                    child.close()
        except BaseException:
            self.close()
            raise

    def submit(self, order, source_witness, ffmpeg_witness, windows, timeout, deadline):
        if self.active is not None:
            raise ScreenError("only one decode prefetch batch may be outstanding")
        if not self.workers:
            self._start()
        self.active = (windows, [])
        for index, (process, connection) in enumerate(self.workers):
            selected = windows[index::len(self.workers)]
            if not selected:
                continue
            worker_api.send_packet(connection, {"recording": order["recording"], "ffmpeg": order["ffmpeg"],
                "source_witness": source_witness, "ffmpeg_witness": ffmpeg_witness, "windows": selected,
                "timeout": min(timeout, screen.remaining(deadline))}, deadline)
            self.active[1].append(connection)

    def collect(self, deadline):
        if self.active is None:
            raise ScreenError("no decode batch is outstanding")
        windows, connections = self.active
        decoded = []
        for connection in connections:
            value = worker_api.receive_packet(connection, deadline)
            if "error" in value:
                raise ScreenError("resident decode failed: " + str(value["error"])[:1200])
            screen.exact(value, {"decoded"}, "decode response")
            if not isinstance(value["decoded"], list) or len(value["decoded"]) > 16:
                raise ScreenError("decode response exceeds window bound")
            for item in value["decoded"]:
                screen.exact(item, {"window", "pcm_base64", "decode_seconds"}, "decoded probe")
                seconds = item["decode_seconds"]
                if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or seconds < 0:
                    raise ScreenError("invalid decoder timing")
                pcm = base64.b64decode(item["pcm_base64"], validate=True)
                window = item["window"]
                if window not in windows or len(pcm) != (window["end_ms"] - window["start_ms"]) * 32:
                    raise ScreenError("decoded PCM differs from exact planned probe")
                self.decoded_seconds += seconds
                decoded.append({"window": window, "pcm": pcm})
        decoded.sort(key=lambda value: value["window"]["index"])
        if [item["window"] for item in decoded] != windows:
            raise ScreenError("decoded batch reordered, repeated, or omitted probes")
        self.active = None
        return decoded

    def close(self):
        workers, self.workers = self.workers, []
        self.active = None
        failures = []
        with old_batch._deferred_launch_signals():
            for process, connection in workers:
                try:
                    _close_process(process, connection)
                except (OSError, RuntimeError) as error:
                    failures.append(error)
        if failures:
            raise ScreenError("one or more decoder processes could not be cleaned up") from failures[0]


def _run_number(root, manifest):
    with os.scandir(paths.anchor(root)) as entries:
        names = [entry.name for entry in entries]
    if len(names) > 2 * MAX_RUNS + 140:
        raise ScreenError("resident journal exceeds bounded size")
    starts = sorted(name for name in names if name.startswith("run-") and name.endswith(".start.json"))
    if len(starts) >= MAX_RUNS or starts != [f"run-{number:06d}.start.json" for number in range(1, len(starts) + 1)]:
        raise ScreenError("resident run sequence differs or is exhausted")
    for number, name in enumerate(starts, 1):
        start = screen.read_json(root / name)
        if start != {"kind": "himr_resident_speaker_screen_run", "schema_version": 1,
                     "batch_id": manifest["batch_id"], "run": number}:
            raise ScreenError("resident run journal differs from manifest")
        finish = root / f"run-{number:06d}.finish.json"
        if screen.exists(finish):
            value = screen.read_json(finish)
            if value.get("run") != number or value.get("start_sha256") != screen.digest(start) or value.get("batch_id") != manifest["batch_id"]:
                raise ScreenError("resident run result differs from its start")
    for name in names:
        if name.startswith("run-") and name.endswith(".finish.json") and name.replace(".finish.json", ".start.json") not in starts:
            raise ScreenError("resident run result lacks its start")
    return len(starts) + 1


def _run_recording(plan, root, model, decoders, deadline, metrics):
    order = plan["order"]
    deadline = min(deadline, time.monotonic() + order["resources"]["max_run_seconds"])
    with screen.opened(order["recording"]["path"]) as source, screen.opened(order["ffmpeg"]["path"], executable=True) as ffmpeg:
        source_witness, ffmpeg_witness = screen.witness(source), screen.witness(ffmpeg)
        if source_witness["st_size"] != order["recording"]["byte_count"]:
            raise ScreenError("resident source byte count differs")
        if screen.hash_fd(ffmpeg, 256 * 1024**2, deadline) != order["ffmpeg"]["sha256"]:
            raise ScreenError("resident FFmpeg SHA-256 differs")
        if order["source_verification"] == "sha256" and screen.hash_fd(source, 64 * 1024**3, deadline) != order["recording"]["sha256"]:
            raise ScreenError("resident source SHA-256 differs")
        binding = {"kind": "himr_speaker_screen_source_binding", "schema_version": 1,
                   "plan_id": plan["plan_id"], "source_witness": source_witness,
                   "source_sha256_reverified": order["source_verification"] == "sha256"}
        job = root / plan["plan_id"]
        try:
            os.mkdir(job.name, 0o700, dir_fd=paths.anchor(root))
            screen.sync_directory(root)
        except FileExistsError:
            pass
        with paths.retained_directory(job):
            screen.write_immutable(job / "plan.json", plan)
            screen.write_immutable(job / "source-binding.json", binding)
            cache = core.SummaryCache()
            observations, proofs, runtime, summary, completed = _observations_for(plan, job, binding, cache=cache)
            initial = _result_document(plan, binding, observations, proofs, runtime, summary)
            if screen.exists(job / "result.json"):
                if screen.read_json(job / "result.json") != initial:
                    raise ScreenError("resident final result differs from checkpoint replay")
                return initial
            if initial["screening_decision_complete"]:
                screen.write_immutable(job / "result.json", initial)
                return initial
            pending = []
            added = 0
            for index, windows in enumerate(plan["batches"]):
                if index in completed:
                    continue
                if added + len(windows) > order["resources"]["max_windows_per_run"]:
                    break
                pending.append((index, windows))
                added += len(windows)
            timeout = order["resources"]["window_timeout_seconds"]
            if pending and time.monotonic() < deadline:
                decoders.submit(order, source_witness, ffmpeg_witness, pending[0][1], timeout, deadline)
            for ordinal, (index, windows) in enumerate(pending):
                if time.monotonic() >= deadline:
                    break
                worker_api.verify_implementation(plan["implementation"])
                decoded = decoders.collect(deadline)
                # One future fixed batch overlaps this batch's VAD/embedding work.
                if ordinal + 1 < len(pending) and time.monotonic() < deadline:
                    decoders.submit(order, source_witness, ffmpeg_witness, pending[ordinal + 1][1], timeout, deadline)
                started = time.monotonic()
                answer = model.analyze_batch(decoded, min(timeout * len(windows), screen.remaining(deadline)))
                metrics["model_roundtrip_seconds"] += time.monotonic() - started
                if not isinstance(answer, dict) or not isinstance(answer.get("runtime"), dict) or not answer["runtime"]:
                    raise ScreenError("resident model response lacks runtime provenance")
                _validate_runtime(plan, answer["runtime"])
                if runtime is not None and answer["runtime"] != runtime:
                    raise ScreenError("resident runtime changed; do not mix embeddings")
                incoming = answer.get("observations")
                if not isinstance(incoming, list) or len(incoming) != len(windows):
                    raise ScreenError("resident observation batch count differs")
                results = []
                for item, observation, window in zip(decoded, incoming, windows):
                    if not isinstance(observation, dict) or observation.get("index") != window["index"]:
                        raise ScreenError("resident observation belongs to another probe")
                    _validate_observation(observation)
                    if (observation.get("embedding") is not None and type(observation.get("speech_ms")) is int
                            and observation["speech_ms"] < order["policy"]["min_speech_ms"]):
                        observation = {**observation, "embedding": None}
                    results.append({"window": window, "pcm_sha256": hashlib.sha256(item["pcm"]).hexdigest(), "observation": observation})
                candidate = observations + [item["observation"] for item in results]
                candidate_summary = core.summarize(order["recording"]["duration_ms"], plan["windows"], candidate, order["policy"], cache=cache)
                with screen.opened(order["recording"]["path"]) as current:
                    if (screen.witness(current) != source_witness or screen.witness(source) != source_witness
                            or screen.witness(ffmpeg) != ffmpeg_witness):
                        raise ScreenError("resident source changed or was replaced before checkpoint")
                worker_api.verify_implementation(plan["implementation"])
                checkpoint = {"kind": "himr_resident_speaker_screen_checkpoint", "schema_version": 1,
                              "plan_id": plan["plan_id"], "binding_sha256": screen.digest(binding),
                              "batch_index": index, "window_results": results, "runtime": answer["runtime"]}
                started = time.monotonic()
                screen.write_immutable(_checkpoint_path(job, index), checkpoint)
                metrics["checkpoint_write_seconds"] += time.monotonic() - started
                metrics["new_windows"] += len(windows)
                metrics["new_embedding_windows"] += sum(item["observation"]["embedding"] is not None for item in results)
                metrics["model_statistics"] = answer.get("statistics", {})
                observations, summary, runtime = candidate, candidate_summary, answer["runtime"]
                if order["resources"].get("early_stop_on_positive", False) and summary["status"] == "multiple_speaker_candidate":
                    break
            # An unused future probe cannot invalidate a supported decision.
            # Kill/discard lookahead; fresh decoders start lazily if another
            # recording needs them. Never mix unread responses into its evidence.
            if decoders.active is not None:
                decoders.close()
            observations, proofs, runtime, summary, _ = _observations_for(plan, job, binding, cache=cache)
            result = _result_document(plan, binding, observations, proofs, runtime, summary)
            if result["screening_decision_complete"]:
                screen.write_immutable(job / "result.json", result)
            return result


def run_batch(path, expected):
    manifest = _load_manifest(path, expected)
    execution, root = manifest["execution"], Path(manifest["state_root"])
    started = time.monotonic()
    deadline = started + execution["max_run_seconds"]
    model = decoders = None
    metrics = {"elapsed_seconds": 0.0, "model_workers_started": 0, "decoder_workers_started": 0,
               "model_roundtrip_seconds": 0.0, "decode_cpu_wall_seconds_sum": 0.0,
               "checkpoint_write_seconds": 0.0, "new_windows": 0, "new_embedding_windows": 0,
               "model_statistics": {}}
    errors, invocation = [], "finished"
    with old_batch._cancellable(), _locked(root) as lock_fd:
        rows = [_projection(plan, _read_job(plan, root)) for plan in manifest["plans"]]
        pending = [plan for plan, row in zip(manifest["plans"], rows) if not row["screening_decision_complete"]]
        if not pending:
            metrics["elapsed_seconds"] = time.monotonic() - started
            return _summary(manifest, rows, invocation_state="finished", metrics=metrics)
        number = _run_number(root, manifest)
        start = {"kind": "himr_resident_speaker_screen_run", "schema_version": 1,
                 "batch_id": manifest["batch_id"], "run": number}
        screen.write_immutable(root / f"run-{number:06d}.start.json", start)
        current = None
        try:
            worker_api.verify_implementation(manifest["implementation"])
            model = worker_api.ResidentModelProcess(manifest["models"], execution, manifest["implementation"],
                max_run_seconds=execution["max_run_seconds"], lock_fd=lock_fd)
            metrics["model_workers_started"] = 1
            decoders = DecodePool(execution["decode_prefetch"], manifest["implementation"],
                                 max_run_seconds=execution["max_run_seconds"], lock_fd=lock_fd)
            metrics["decoder_workers_started"] = execution["decode_prefetch"]
            for plan in pending:
                current = plan
                if time.monotonic() >= deadline:
                    invocation = "time_limit"
                    break
                recording_deadline = min(deadline, time.monotonic() + plan["order"]["resources"]["max_run_seconds"])
                try:
                    result = _run_recording(plan, root, model, decoders, recording_deadline, metrics)
                except (ScreenError, OSError, RuntimeError, EOFError, TimeoutError):
                    if time.monotonic() >= recording_deadline:
                        raise InvocationLimit("recording reached its bounded invocation limit") from None
                    raise
                rows[plan["index"]] = _projection(plan, result)
        except InvocationLimit:
            invocation = "time_limit"
        except KeyboardInterrupt:
            invocation = "cancelled"
        except (ScreenError, OSError, ValueError, RuntimeError, EOFError, TimeoutError) as error:
            invocation = "time_limit" if time.monotonic() >= deadline else "failed"
            errors.append({"index": None if current is None else current["index"],
                           "reason": f"{type(error).__name__}: {str(error)[:1200]}"})
        finally:
            try:
                with old_batch._deferred_launch_signals():
                    if decoders is not None:
                        metrics["decode_cpu_wall_seconds_sum"] = decoders.decoded_seconds
                        metrics["decoder_workers_started"] = decoders.worker_starts
                        try:
                            decoders.close()
                        except (OSError, RuntimeError) as error:
                            errors.append({"index": None, "reason": "decoder cleanup failed: " + str(error)[:1200]})
                            invocation = "failed"
                    if model is not None:
                        try:
                            model.close()
                        except (OSError, RuntimeError) as error:
                            errors.append({"index": None, "reason": "model cleanup failed: " + str(error)[:1200]})
                            invocation = "failed"
            except KeyboardInterrupt:
                invocation = "cancelled"
        # Rebuild all reporting from durable evidence, including the interrupted job.
        for plan in manifest["plans"]:
            try:
                rows[plan["index"]] = _projection(plan, _read_job(plan, root))
            except (ScreenError, OSError, ValueError, RuntimeError) as error:
                errors.append({"index": plan["index"], "reason": "checkpoint replay failed: " + str(error)[:1200]})
                invocation = "failed"
        metrics["elapsed_seconds"] = time.monotonic() - started
        result = _summary(manifest, rows, invocation_state=invocation, errors=errors, metrics=metrics)
        screen.write_immutable(root / f"run-{number:06d}.finish.json", {
            "kind": "himr_resident_speaker_screen_run_result", "schema_version": 1,
            "batch_id": manifest["batch_id"], "run": number, "start_sha256": screen.digest(start), "result": result})
        return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    planner = commands.add_parser("plan")
    planner.add_argument("--request", required=True)
    planner.add_argument("--expected-sha256", required=True)
    planner.add_argument("--output", required=True)
    for name in ("run", "status"):
        selected = commands.add_parser(name)
        selected.add_argument("--manifest", required=True)
        selected.add_argument("--expected-sha256", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            result = seal_manifest(args.request, args.expected_sha256, args.output)
        elif args.command == "status":
            result = status_batch(args.manifest, args.expected_sha256)
        else:
            result = run_batch(args.manifest, args.expected_sha256)
        print(json.dumps(result, sort_keys=True, indent=2, allow_nan=False))
        return 130 if result.get("invocation_state") == "cancelled" else 2 if result.get("errors") else 0
    except KeyboardInterrupt:
        print("Resident screen interrupted; committed probe batches are preserved.", file=sys.stderr)
        return 130
    except (ScreenError, OSError, ValueError, RuntimeError, EOFError, TimeoutError) as error:
        print(f"ResidentSpeakerScreenError: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
