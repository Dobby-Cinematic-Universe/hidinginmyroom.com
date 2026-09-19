"""Finite, offline resident inference process for the separate accelerated screen.

Only standard-library modules are imported here. Model libraries are loaded in
the spawned child after environment, network, resource and source checks.
The framing helpers are also used by the separate decoder processes.
"""
from __future__ import annotations

import base64
from contextlib import contextmanager
import copy
import json
import math
import multiprocessing
from multiprocessing.reduction import DupFd
import os
from pathlib import Path
import re
import resource
import signal
import socket
import sys
import time

from pipeline import speaker_screen as screen

ScreenError = screen.ScreenError
MAX_PACKET = 16 * 1024**2
HOST_MEMORY_MAX = 4 * 1024**3
IMPLEMENTATION_FILES = frozenset({
    "speaker_screen.py", "speaker_screen_core.py", "speaker_screen_engine.py",
    "speaker_screen_paths.py", "speaker_screen_batch.py", "speaker_screen_accelerated.py",
    "speaker_screen_accelerated_worker.py", "speaker_screen_accelerated_engine.py",
})
PIPELINE_DIRECTORY = Path(__file__).resolve().parent
GPU_UUID = re.compile(r"^GPU-[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}$")


def _finite_json(value):
    """Reject non-JSON Python values, non-string keys and non-finite numbers."""
    def check(node, depth=0):
        if depth > 64:
            raise ScreenError("worker JSON nesting exceeds its bound")
        if node is None or type(node) in (str, bool, int):
            return
        if type(node) is float and math.isfinite(node):
            return
        if type(node) is list:
            for item in node:
                check(item, depth + 1)
            return
        if type(node) is dict and all(type(key) is str for key in node):
            for item in node.values():
                check(item, depth + 1)
            return
        raise ScreenError("worker packet must contain strict finite JSON")
    check(value)
    try:
        body = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (ValueError, UnicodeError, RecursionError) as error:
        raise ScreenError("invalid worker JSON") from error
    if not isinstance(value, dict) or not 0 < len(body) <= MAX_PACKET:
        raise ScreenError("worker packet exceeds its size or object bound")
    return body


def send_packet(connection, value, deadline):
    body = _finite_json(value)
    connection.settimeout(screen.remaining(deadline))
    connection.sendall(len(body).to_bytes(4, "big") + body)


def receive_packet(connection, deadline):
    def receive(count):
        body = bytearray()
        while len(body) < count:
            connection.settimeout(screen.remaining(deadline))
            part = connection.recv(count - len(body))
            if not part:
                raise EOFError("resident worker connection closed")
            body.extend(part)
        return bytes(body)

    length = int.from_bytes(receive(4), "big")
    if not 0 < length <= MAX_PACKET:
        raise ScreenError("worker packet exceeds its size bound")

    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ScreenError("duplicate worker JSON key")
            value[key] = item
        return value

    def constant(_):
        raise ScreenError("nonfinite worker JSON")

    try:
        value = json.loads(receive(length), object_pairs_hook=pairs, parse_constant=constant)
        _finite_json(value)
    except (ValueError, UnicodeError, RecursionError) as error:
        raise ScreenError("invalid worker JSON") from error
    return value


def verify_implementation(expected):
    """Hash only the fixed implementation files, never caller-selected paths."""
    if (type(expected) is not dict or set(expected) != IMPLEMENTATION_FILES
            or any(type(value) is not str or not screen.SHA.fullmatch(value)
                   for value in expected.values())):
        raise ScreenError("accelerated implementation binding differs")
    deadline = time.monotonic() + 10
    for name in sorted(IMPLEMENTATION_FILES):
        with screen.opened(PIPELINE_DIRECTORY / name) as fd:
            if screen.hash_fd(fd, 2 * 1024**2, deadline) != expected[name]:
                raise ScreenError("accelerated implementation changed")


def validate_execution(value):
    screen.exact(value, {"device", "threads", "batch_size", "decode_prefetch", "max_run_seconds",
                         "cuda_memory_fraction", "gpu_uuid", "host_memory_max_bytes"}, "execution")
    if type(value["device"]) is not str or value["device"] not in {"cpu", "cuda"}:
        raise ScreenError("resident device must be cpu or cuda")
    for key, low, high in (("threads", 1, 2), ("batch_size", 1, 16), ("decode_prefetch", 1, 2),
                           ("max_run_seconds", 10, 86400)):
        screen.integer(value[key], low, high, key)
    fraction = value["cuda_memory_fraction"]
    if (type(fraction) not in (int, float) or not math.isfinite(fraction) or not 0.1 <= fraction <= 0.75):
        raise ScreenError("CUDA fraction must be finite and bounded")
    if type(value["host_memory_max_bytes"]) is not int or value["host_memory_max_bytes"] != HOST_MEMORY_MAX:
        raise ScreenError("resident host memory limit must be exactly 4 GiB")
    if value["device"] == "cpu":
        if value["gpu_uuid"] is not None:
            raise ScreenError("CPU resident work must not select a GPU")
    elif type(value["gpu_uuid"]) is not str or not GPU_UUID.fullmatch(value["gpu_uuid"]):
        raise ScreenError("CUDA resident work requires an explicit GPU UUID")
    return copy.deepcopy(value)


def _small_text(path):
    with open(path, "rb") as handle:
        body = handle.read(65537)
    if len(body) > 65536:
        raise ScreenError("cgroup metadata exceeds its bound")
    try:
        return body.decode("ascii").strip()
    except UnicodeError as error:
        raise ScreenError("invalid cgroup metadata") from error


def _memory_hierarchy(leaf, root):
    """Minimum ancestor limits are the aggregate cgroup-v2 hard ceilings."""
    memory, swap = [], []
    if leaf != root and root not in leaf.parents:
        raise ScreenError("cgroup is outside its unified mount")
    current = leaf
    for _ in range(128):
        # The cgroup2 filesystem root itself need not expose these files.
        for filename, target in (("memory.max", memory), ("memory.swap.max", swap)):
            try:
                raw = _small_text(current / filename)
            except FileNotFoundError:
                if current == root:
                    continue
                raise ScreenError("CUDA cgroup lacks aggregate memory controls") from None
            if raw == "max":
                continue
            if not re.fullmatch(r"0|[1-9][0-9]{0,19}", raw):
                raise ScreenError("invalid CUDA cgroup memory limit")
            target.append(int(raw))
        if current == root:
            break
        current = current.parent
    else:
        raise ScreenError("CUDA cgroup hierarchy exceeds its bound")
    if not memory or not 0 < min(memory) <= HOST_MEMORY_MAX or not swap or min(swap) != 0:
        raise ScreenError("CUDA requires inherited aggregate memory.max <= 4 GiB and memory.swap.max = 0")
    return {"memory_max_bytes": min(memory), "memory_swap_max_bytes": min(swap)}


def verify_cuda_memory_limit():
    lines = _small_text("/proc/self/cgroup").splitlines()
    unified = [line[3:] for line in lines if line.startswith("0::")]
    if len(unified) != 1:
        raise ScreenError("CUDA requires a unified cgroup-v2 memory limit")
    relative = unified[0]
    if (not relative.startswith("/") or "//" in relative
            or any(part in {".", ".."} for part in relative.split("/"))):
        raise ScreenError("invalid unified cgroup path")
    mounts = []
    for line in _small_text("/proc/self/mountinfo").splitlines():
        fields = line.split()
        if "-" not in fields:
            continue
        separator = fields.index("-")
        if separator + 1 < len(fields) and fields[separator + 1] == "cgroup2":
            # A namespace-relative cgroup mount conceals ancestors: fail closed.
            if len(fields) < 6 or fields[3] != "/" or "\\" in fields[4]:
                raise ScreenError("CUDA cgroup ancestors cannot be inspected")
            mounts.append(Path(fields[4]))
    if len(mounts) != 1 or not mounts[0].is_absolute():
        raise ScreenError("CUDA requires one inspectable unified cgroup mount")
    before = unified[0]
    result = _memory_hierarchy(mounts[0] / relative.lstrip("/"), mounts[0])
    if _small_text("/proc/self/cgroup").splitlines() != lines:
        raise ScreenError("CUDA cgroup changed while checking resource bounds")
    return {**result, "cgroup": before}


def _validate_items(items, batch_size, *, encoded=False):
    if type(items) is not list or not 1 <= len(items) <= batch_size:
        raise ScreenError("resident batch must contain one through the admitted batch size")
    result = []
    for item in items:
        screen.exact(item, {"pcm_base64" if encoded else "pcm", "window"}, "resident batch item")
        window = item["window"]
        screen.exact(window, {"index", "start_ms", "end_ms"}, "resident window")
        screen.integer(window["index"], 0, 511, "window index")
        screen.integer(window["start_ms"], 0, 86400000, "window start")
        screen.integer(window["end_ms"], 1, 86400000, "window end")
        duration = window["end_ms"] - window["start_ms"]
        if not 1 <= duration <= 10000:
            raise ScreenError("resident window exceeds its duration bound")
        pcm = item.get("pcm")
        if encoded:
            text = item["pcm_base64"]
            if type(text) is not str or len(text) > 426668:
                raise ScreenError("resident PCM exceeds its encoding bound")
            try:
                pcm = base64.b64decode(text, validate=True)
            except (ValueError, UnicodeError) as error:
                raise ScreenError("invalid resident PCM encoding") from error
        if type(pcm) is not bytes or len(pcm) != duration * 32:
            raise ScreenError("resident PCM does not match the exact probe duration")
        result.append({"pcm": pcm, "window": dict(window)})
    return result


def _observations(value, items):
    if type(value) is not list or len(value) != len(items):
        raise ScreenError("resident observations do not match their batch")
    for row, item in zip(value, items):
        screen.exact(row, {"index", "start_ms", "end_ms", "speech_ms", "embedding"}, "resident observation")
        window = item["window"]
        for field in ("index", "start_ms", "end_ms", "speech_ms"):
            if type(row[field]) is not int:
                raise ScreenError("resident observation has invalid integer metadata")
        if (row["index"] != window["index"] or not window["start_ms"] <= row["start_ms"] < row["end_ms"] <= window["end_ms"]
                or not 0 <= row["speech_ms"] <= row["end_ms"] - row["start_ms"]):
            raise ScreenError("resident observation lies outside its probe")
        vector = row["embedding"]
        if vector is None:
            if row["start_ms"] != window["start_ms"] or row["end_ms"] != window["end_ms"]:
                raise ScreenError("unembedded resident observation must retain its probe bounds")
        elif (type(vector) is not list or len(vector) != 192
              or any(type(number) not in (int, float) or not math.isfinite(number) for number in vector)
              or not 2000 <= row["speech_ms"] == row["end_ms"] - row["start_ms"] <= 5000
              or not math.isclose(math.hypot(*vector), 1.0, rel_tol=1e-6, abs_tol=1e-6)):
            raise ScreenError("resident observation has an invalid normalized embedding")


@contextmanager
def deferred_signals():
    """Register child ownership before a pending interactive stop is delivered."""
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM})
    try:
        yield previous
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


def _restore_lease(lease):
    return InheritedLease(lease.detach())


class InheritedLease:
    def __init__(self, fd):
        self.fd = fd

    def __reduce__(self):
        # During spawn this uses its pass-fd transport, not a long-lived
        # resource-sharer registration that could survive failed startup.
        return _restore_lease, (DupFd(self.fd),)

    def detach(self):
        if self.fd is None:
            raise ScreenError("execution lease was already detached")
        result, self.fd = self.fd, None
        return result


def _runtime_provenance(engine, execution):
    value = copy.deepcopy(engine.provenance())
    if type(value) is not dict or not value:
        raise ScreenError("resident engine has no stable provenance")
    value["requested_gpu_uuid"] = execution["gpu_uuid"]
    value["nvidia_driver_version"] = None
    if execution["device"] == "cuda":
        raw = _small_text("/proc/driver/nvidia/version")
        match = re.search(r"^NVRM version:.*?\b([0-9]{3,4}\.[0-9]{1,3}(?:\.[0-9]{1,3})?)\b", raw, re.MULTILINE)
        if not match:
            raise ScreenError("CUDA driver version cannot be bound to the runtime")
        value["nvidia_driver_version"] = match.group(1)
    return value


def _child_environment(execution):
    os.environ.clear()
    os.environ.update({"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "PYTHONDONTWRITEBYTECODE": "1",
                       "CUDA_VISIBLE_DEVICES": execution["gpu_uuid"] if execution["device"] == "cuda" else "",
                       "HF_HUB_OFFLINE": "1", "HF_DATASETS_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
                       "HF_HUB_DISABLE_TELEMETRY": "1", "DO_NOT_TRACK": "1", "PYANNOTE_METRICS_ENABLED": "0",
                       "OMP_NUM_THREADS": str(execution["threads"]), "MKL_NUM_THREADS": str(execution["threads"]),
                       "OPENBLAS_NUM_THREADS": str(execution["threads"]), "NUMEXPR_NUM_THREADS": str(execution["threads"]),
                       "NVIDIA_TF32_OVERRIDE": "0", "CUBLAS_WORKSPACE_CONFIG": ":4096:8"})
    sys.dont_write_bytecode = True


def _child_resource_limits(execution, max_run_seconds):
    cpu_seconds = math.ceil(max_run_seconds) * execution["threads"] + 10
    resource.setrlimit(resource.RLIMIT_CPU, (cpu_seconds, cpu_seconds))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    os.nice(10)
    screen.deny_internet()
    if execution["device"] == "cpu":
        resource.setrlimit(resource.RLIMIT_AS, (HOST_MEMORY_MAX, HOST_MEMORY_MAX))
    else:
        # CUDA reserves large virtual address ranges: aggregate RSS+swap must
        # instead be constrained by the inherited cgroup, never RLIMIT_AS.
        verify_cuda_memory_limit()


def _model_child(connection, models, execution, implementation, parent_pid, deadline, signal_mask, lease_fd):
    lease = lease_fd
    lease_fd = None
    try:
        if lease is not None:
            lease_fd = lease.detach()
        # Suppress arbitrary library prints: private evidence travels only over
        # this AF_UNIX connection, never inherited stdout/stderr or model logs.
        with open(os.devnull, "r+b", buffering=0) as sink:
            for fd in (0, 1, 2):
                os.dup2(sink.fileno(), fd)
        os.setsid()
        screen.die_with_parent(parent_pid)
        signal.pthread_sigmask(signal.SIG_SETMASK, signal_mask)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)
        signal.setitimer(signal.ITIMER_REAL, screen.remaining(deadline))
        _child_environment(execution)
        _child_resource_limits(execution, screen.remaining(deadline))
        verify_implementation(implementation)
        from pipeline.speaker_screen_accelerated_engine import ResidentScreenEngine
        engine = ResidentScreenEngine(models, device=execution["device"], threads=execution["threads"],
                                      batch_size=execution["batch_size"],
                                      cuda_memory_fraction=execution["cuda_memory_fraction"])
        engine.initialize()
        runtime = _runtime_provenance(engine, execution)
        runtime_body = _finite_json(runtime)
        verify_implementation(implementation)
        send_packet(connection, {"type": "ready", "runtime": runtime}, deadline)
        batches = windows = 0
        while True:
            request = receive_packet(connection, deadline)
            screen.exact(request, {"type", "request_id", "items"}, "resident request")
            if request["type"] != "batch" or type(request["request_id"]) is not int or request["request_id"] != batches + 1:
                raise ScreenError("resident request sequence differs")
            items = _validate_items(request["items"], execution["batch_size"], encoded=True)
            verify_implementation(implementation)
            if execution["device"] == "cuda":
                verify_cuda_memory_limit()
            observations = engine.analyze_batch(items)
            _observations(observations, items)
            if _finite_json(_runtime_provenance(engine, execution)) != runtime_body:
                raise ScreenError("resident engine provenance changed")
            verify_implementation(implementation)
            batches += 1
            windows += len(items)
            send_packet(connection, {"type": "batch", "request_id": batches, "observations": observations,
                                     "runtime": runtime, "statistics": {"model_initializations": 1,
                                     "batches": batches, "windows": windows}}, deadline)
    except EOFError:
        pass
    except BaseException as error:
        try:
            send_packet(connection, {"type": "error", "code": "resident_worker_failed",
                                     "error_type": type(error).__name__[:80]}, time.monotonic() + 1)
        except BaseException:
            pass
    finally:
        if lease_fd is not None:
            os.close(lease_fd)
        connection.close()


class ResidentModelProcess:
    """Asynchronously load once, then serve bounded batches serially.

    Any transport, validation, timeout or cancellation failure closes the worker;
    a possibly completed but unacknowledged batch is never silently reused.
    """
    def __init__(self, models, execution, implementation, *, max_run_seconds, lock_fd=None):
        self.connection = self.process = None
        self.closed = False
        self.runtime = None
        self.requests = self.windows = 0
        self.execution = validate_execution(execution)
        screen.integer(max_run_seconds, 10, 86400, "max_run_seconds")
        if max_run_seconds > execution["max_run_seconds"]:
            raise ScreenError("resident time budget exceeds its sealed execution limit")
        screen.engine.validate_model_config(models)
        verify_implementation(implementation)
        self.implementation = copy.deepcopy(implementation)
        self.deadline = time.monotonic() + max_run_seconds
        if lock_fd is not None:
            if type(lock_fd) is not int or lock_fd < 0:
                raise ScreenError("invalid inherited execution lease")
            os.fstat(lock_fd)
        child = None
        try:
            with deferred_signals() as previous:
                context = multiprocessing.get_context("spawn")
                self.connection, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
                self.process = context.Process(target=_model_child, args=(child, copy.deepcopy(models),
                    self.execution, self.implementation, os.getpid(), self.deadline, previous,
                    InheritedLease(lock_fd) if lock_fd is not None else None), daemon=True)
                self.process.start()
                child.close()
                child = None
        except BaseException:
            self.close()
            raise
        finally:
            if child is not None:
                child.close()

    def analyze_batch(self, items, timeout):
        if self.closed:
            raise ScreenError("resident worker is closed")
        try:
            if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
                raise ScreenError("resident batch timeout must be positive and finite")
            deadline = min(self.deadline, time.monotonic() + timeout)
            items = _validate_items(items, self.execution["batch_size"])
            verify_implementation(self.implementation)
            if self.runtime is None:
                ready = self._receive(deadline)
                screen.exact(ready, {"type", "runtime"}, "resident readiness")
                if ready["type"] != "ready" or type(ready["runtime"]) is not dict or not ready["runtime"]:
                    raise ScreenError("resident readiness lacks stable provenance")
                self.runtime = copy.deepcopy(ready["runtime"])
            request_id = self.requests + 1
            send_packet(self.connection, {"type": "batch", "request_id": request_id, "items": [
                {"window": item["window"], "pcm_base64": base64.b64encode(item["pcm"]).decode("ascii")}
                for item in items]}, deadline)
            value = self._receive(deadline)
            screen.exact(value, {"type", "request_id", "observations", "runtime", "statistics"}, "resident response")
            if value["type"] != "batch" or type(value["request_id"]) is not int or value["request_id"] != request_id:
                raise ScreenError("resident response sequence differs")
            if _finite_json(value["runtime"]) != _finite_json(self.runtime):
                raise ScreenError("resident response runtime changed")
            _observations(value["observations"], items)
            statistics = {"model_initializations": 1, "batches": request_id, "windows": self.windows + len(items)}
            if _finite_json(value["statistics"]) != _finite_json(statistics):
                raise ScreenError("resident response counters differ")
            verify_implementation(self.implementation)
            screen.remaining(deadline)
            self.requests, self.windows = request_id, statistics["windows"]
            return {key: value[key] for key in ("observations", "runtime", "statistics")}
        except BaseException as error:
            self.close()
            if isinstance(error, TimeoutError):
                raise ScreenError("resident model batch timed out; existing checkpoints are preserved") from None
            if isinstance(error, (EOFError, BrokenPipeError, ConnectionResetError)):
                raise ScreenError("resident model exited without a complete response") from None
            raise

    def _receive(self, deadline):
        value = receive_packet(self.connection, deadline)
        if value.get("type") == "error":
            # Do not echo arbitrary native-library messages or private vectors.
            raise ScreenError("resident model worker failed; no batch observations were admitted")
        return value

    def close(self):
        if self.closed:
            return
        self.closed = True
        with deferred_signals():
            if self.connection is not None:
                self.connection.close()
            process = self.process
            if process is not None and process.pid is not None:
                if process.is_alive():
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        process.kill()  # Child may not have reached setsid yet.
                process.join(timeout=2)
                if process.is_alive():
                    process.kill()
                    process.join(timeout=2)
                if process.is_alive():
                    raise ScreenError("resident worker could not be reaped")
