"""Standalone, opt-in CPU speaker-diversity screening. Never a campaign stage.

Only explicitly named local files are opened. Original media, ASR, databases,
campaigns and services are never modified. Model/PCM data stays private. The
default source check is an honest metadata witness, not a new full-media hash.
"""
from __future__ import annotations

import argparse
import base64
from copy import deepcopy
from contextlib import contextmanager
import ctypes
import ctypes.util
import errno
import fcntl
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import re
import resource
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from pipeline import speaker_screen_core as core
from pipeline import speaker_screen_engine as engine
from pipeline import speaker_screen_paths as paths

ScreenError = core.ScreenError
MAX_JSON = 16 * 1024**2
MAX_PCM = 16000 * 2 * 10
MAX_MESSAGE = 1024**2
MODEL_ADDRESS_SPACE_BYTES = 4 * 1024**3
DECODE_ADDRESS_SPACE_BYTES = 1024**3
SHA = re.compile(r"^[0-9a-f]{64}$")
IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
MARKER = {"kind": "himr_standalone_speaker_screen_workspace", "schema_version": 1}
FORMATS = "mov,matroska,avi,mp3,wav,flac,aac,ogg,mpegts,mpeg,asf,aiff"


def canonical(value):
    try:
        return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                           allow_nan=False) + "\n").encode()
    except (ValueError, TypeError, RecursionError) as error:
        raise ScreenError("invalid finite JSON value") from error


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def exact(value, fields, label):
    if not isinstance(value, dict) or set(value) != set(fields):
        raise ScreenError(f"{label} fields differ")


def integer(value, low, high, label):
    if type(value) is not int or not low <= value <= high:
        raise ScreenError(f"{label} must be an integer in {low}..{high}")


def path_value(value):
    if not isinstance(value, (str, Path)):
        raise ScreenError("a normalized absolute path is required")
    raw = str(value)
    path = Path(raw)
    if (not path.is_absolute() or path == Path("/") or str(path) != raw or "//" in raw or "\\" in raw
            or any(part in {".", ".."} for part in raw.split("/"))
            or any(ord(char) < 32 for char in raw) or len(raw.encode()) > 4096):
        raise ScreenError("a normalized absolute non-root path is required")
    return path


def file_binding(value):
    exact(value, {"path", "sha256"}, "file binding")
    path_value(value["path"])
    if not isinstance(value["sha256"], str) or not SHA.fullmatch(value["sha256"]):
        raise ScreenError("invalid SHA-256")


@contextmanager
def opened(path, *, executable=False):
    """Retain a no-symlink descriptor and reject peer-writable/nonregular input."""
    path = path_value(path)
    anchored = paths.anchor(path.parent)
    directory = os.dup(anchored) if anchored is not None else os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    fd = None
    try:
        for part in (() if anchored is not None else path.parts[1:-1]):
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                            dir_fd=directory)
            os.close(directory)
            directory = child
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                     dir_fd=directory)
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_mode & 0o022
                or info.st_uid not in {os.getuid(), 0}
                or (executable and not info.st_mode & 0o111)):
            raise ScreenError("input must be a safe owned regular file")
        yield fd
    finally:
        if fd is not None:
            os.close(fd)
        os.close(directory)


def witness(fd):
    value = os.fstat(fd)
    return {key: getattr(value, key) for key in
            ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns", "st_mode", "st_uid", "st_nlink")}


def read_json(path, expected=None):
    with opened(path) as fd:
        before = witness(fd)
        if not 0 < before["st_size"] <= MAX_JSON:
            raise ScreenError("JSON exceeds its size bound")
        with os.fdopen(os.dup(fd), "rb") as stream:
            body = stream.read(MAX_JSON + 1)
        if len(body) != before["st_size"] or witness(fd) != before:
            raise ScreenError("JSON changed while being read")
    if expected is not None and hashlib.sha256(body).hexdigest() != expected:
        raise ScreenError("JSON SHA-256 mismatch")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ScreenError("duplicate JSON key")
            result[key] = value
        return result

    try:
        value = json.loads(body, object_pairs_hook=pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(ScreenError("nonfinite JSON")))
        canonical(value)
    except (ValueError, UnicodeError, RecursionError) as error:
        raise ScreenError("invalid JSON") from error
    if not isinstance(value, dict):
        raise ScreenError("JSON must be an object")
    return value


def hash_fd(fd, maximum, deadline):
    before = witness(fd)
    if not 0 < before["st_size"] <= maximum:
        raise ScreenError("file exceeds its hash size bound")
    offset, value = 0, hashlib.sha256()
    while offset < before["st_size"]:
        remaining(deadline)
        body = os.pread(fd, min(1024**2, before["st_size"] - offset), offset)
        if not body:
            raise ScreenError("file truncated during verification")
        offset += len(body)
        value.update(body)
    if witness(fd) != before:
        raise ScreenError("file changed during verification")
    return value.hexdigest()


def validate_order(value):
    exact(value, {"kind", "schema_version", "recording", "ffmpeg", "models", "policy",
                  "resources", "source_verification", "output_root"}, "work order")
    if value["kind"] != "himr_cpu_speaker_screen_work_order" or type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ScreenError("unsupported screen work order")
    recording = value["recording"]
    exact(recording, {"media_id", "path", "sha256", "byte_count", "duration_ms"}, "recording")
    if not isinstance(recording["media_id"], str) or not IDENTIFIER.fullmatch(recording["media_id"]):
        raise ScreenError("invalid recording media_id")
    file_binding({key: recording[key] for key in ("path", "sha256")})
    integer(recording["byte_count"], 1, 64 * 1024**3, "source byte_count")
    integer(recording["duration_ms"], 1, 86400000, "recording duration_ms")
    file_binding(value["ffmpeg"])
    engine.validate_model_config(value["models"])
    if not isinstance(value["policy"], dict) or set(value["policy"]) - set(core.DEFAULT_POLICY):
        raise ScreenError("unknown screen policy fields")
    policy = core.validate_policy({**core.DEFAULT_POLICY, **value["policy"]})
    resources = value["resources"]
    required_resources = {"threads", "window_timeout_seconds", "max_run_seconds", "max_windows_per_run"}
    if (not isinstance(resources, dict) or not required_resources <= set(resources)
            or set(resources) - required_resources - {"early_stop_on_positive"}):
        raise ScreenError("resources fields differ")
    if type(resources.get("early_stop_on_positive", False)) is not bool:
        raise ScreenError("early_stop_on_positive must be a boolean")
    for key, low, high in (("threads", 1, 2), ("window_timeout_seconds", 10, 600),
                           ("max_run_seconds", 10, 3600), ("max_windows_per_run", 1, 512)):
        integer(resources[key], low, high, key)
    if value["source_verification"] not in ("metadata_witness", "sha256"):
        raise ScreenError("unknown source verification mode")
    output = path_value(value["output_root"])
    for binding in (recording, value["ffmpeg"], value["models"]["silero_vad"], value["models"]["ecapa_embedding"]):
        if path_value(binding["path"]).is_relative_to(output):
            raise ScreenError("source/tools/models cannot be inside the output workspace")
    return {**value, "policy": policy}


def prepare_order(order, preset, output_root, *, threads=1):
    """Derive explicit new metadata; never alter the input order or start work."""
    order = deepcopy(validate_order(order))
    integer(threads, 1, 2, "threads")
    if preset not in {"throughput", "fast-triage"}:
        raise ScreenError("unknown screening preset")
    order["output_root"] = str(path_value(output_root))
    order["resources"].update(threads=threads, max_run_seconds=3600,
                              max_windows_per_run=512,
                              early_stop_on_positive=preset == "fast-triage")
    if preset == "fast-triage":
        order["policy"].update(probe_ms=10000, stride_ms=300000, max_windows=64)
    return validate_order(order)


def implementation_hashes():
    implementation = {}
    for module in (Path(__file__), Path(core.__file__), Path(engine.__file__), Path(paths.__file__)):
        implementation[module.name] = hashlib.sha256(module.read_bytes()).hexdigest()
    return implementation


def build_plan(order):
    order = validate_order(order)
    value = {"kind": "himr_cpu_speaker_screen_plan", "schema_version": 1,
             "order": order, "implementation": implementation_hashes(),
             "python_version": sys.version.split()[0],
             "execution_limits": {"model_address_space_bytes": MODEL_ADDRESS_SPACE_BYTES,
                                  "decode_address_space_bytes": DECODE_ADDRESS_SPACE_BYTES,
                                  "runtime_benchmarked": False},
             "windows": core.plan_windows(order["recording"]["duration_ms"], order["policy"])}
    return {**value, "plan_id": "screen_" + digest(value)[:32]}


def sync_directory(path):
    anchored = paths.anchor(path)
    fd = os.dup(anchored) if anchored is not None else os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_immutable(path, value):
    with paths.retained_directory(path.parent) as directory:
        _write_immutable_at(directory, path, value)


def _write_immutable_at(directory, path, value):
    body = canonical(value)
    if len(body) > MAX_JSON:
        raise ScreenError("output JSON exceeds its size bound")
    temporary = ".speaker-screen-" + uuid.uuid4().hex
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        # Atomic, non-overwriting publication without a crash-time hardlink gap.
        library = ctypes.CDLL(None, use_errno=True)
        rename = getattr(library, "renameat2", None)
        if rename is None:
            raise ScreenError("Linux renameat2 is required for restart-safe publication")
        rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        result = rename(directory, os.fsencode(temporary), directory, os.fsencode(path.name), 1)
        if result != 0:
            error = ctypes.get_errno()
            if error != errno.EEXIST:
                raise OSError(error, os.strerror(error))
            if canonical(read_json(path)) != body:
                raise ScreenError("existing immutable screen artifact differs") from None
        os.fsync(directory)
    finally:
        try:
            os.unlink(temporary, dir_fd=directory)
        except FileNotFoundError:
            pass


def exists(path):
    if paths.anchor(path) is not None:
        return True
    anchored = paths.anchor(path.parent)
    if anchored is None:
        return path.exists() or path.is_symlink()
    try:
        os.stat(path.name, dir_fd=anchored, follow_symlinks=False)
        return True
    except FileNotFoundError:
        return False


def workspace(path, create=False):
    path = path_value(path)
    # Only the explicit leaf may be created. Never mkdir through campaign trees.
    if create:
        parent = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for part in path.parts[1:-1]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
                os.close(parent)
                parent = child
            try:
                os.mkdir(path.name, 0o700, dir_fd=parent)
                os.fsync(parent)
            except FileExistsError:
                pass
        finally:
            os.close(parent)
    with paths.retained_directory(path) as directory:
        marker = path / "workspace.json"
        if not exists(marker) and create:
            with os.scandir(directory) as entries:
                if next(entries, None) is not None:
                    raise ScreenError("refusing a nonempty unmarked workspace")
            write_immutable(marker, MARKER)
        if read_json(marker) != MARKER:
            raise ScreenError("output is not a standalone screen workspace")
    return path


@contextmanager
def locked(root):
    with paths.retained_directory(root) as directory:
        if read_json(root / "workspace.json") != MARKER:
            raise ScreenError("screen workspace marker differs")
        with _lock_at(directory):
            yield directory


@contextmanager
def _lock_at(directory):
    fd = os.open("screen.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=directory)
    try:
        value = os.fstat(fd)
        if not stat.S_ISREG(value.st_mode) or value.st_uid != os.getuid() or value.st_nlink != 1 or value.st_mode & 0o077:
            raise ScreenError("unsafe screen lock")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ScreenError("another screen invocation owns this workspace") from None
        yield
    finally:
        os.close(fd)


def remaining(deadline):
    value = deadline - time.monotonic()
    if value <= 0:
        raise ScreenError("screen invocation reached its time budget; checkpoints remain reusable")
    return value


def deny_internet():
    """Kernel-deny new IPv4/IPv6 sockets, including in native model libraries."""
    class Comparison(ctypes.Structure):
        _fields_ = [("arg", ctypes.c_uint), ("op", ctypes.c_uint),
                    ("datum_a", ctypes.c_uint64), ("datum_b", ctypes.c_uint64)]
    library = ctypes.util.find_library("seccomp")
    if not library:
        raise ScreenError("libseccomp is required for the offline CPU worker")
    seccomp = ctypes.CDLL(library, use_errno=True)
    seccomp.seccomp_init.argtypes = [ctypes.c_uint32]
    seccomp.seccomp_init.restype = ctypes.c_void_p
    seccomp.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
    seccomp.seccomp_syscall_resolve_name.restype = ctypes.c_int
    seccomp.seccomp_rule_add_array.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_int,
                                              ctypes.c_uint, ctypes.POINTER(Comparison)]
    seccomp.seccomp_load.argtypes = [ctypes.c_void_p]
    seccomp.seccomp_release.argtypes = [ctypes.c_void_p]
    context = seccomp.seccomp_init(0x7FFF0000)
    if not context:
        raise ScreenError("cannot initialize offline sandbox")
    try:
        for name in (b"socket", b"socketpair"):
            syscall = seccomp.seccomp_syscall_resolve_name(name)
            if syscall < 0:
                raise ScreenError("unsupported offline sandbox architecture")
            for family in (2, 10):  # AF_INET, AF_INET6; local IPC remains available.
                comparison = Comparison(0, 4, family, 0)  # SCMP_CMP_EQ
                if seccomp.seccomp_rule_add_array(context, 0x50000 | errno.EPERM, syscall, 1,
                                                  ctypes.byref(comparison)) != 0:
                    raise ScreenError("cannot configure offline sandbox")
        if seccomp.seccomp_load(context) != 0:
            raise ScreenError("cannot enter offline sandbox")
    finally:
        seccomp.seccomp_release(context)


def child_limits(address_space_bytes=MODEL_ADDRESS_SPACE_BYTES):
    os.nice(10)
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    deny_internet()
    # Last: a forked decoder may temporarily inherit a larger parent address map.
    resource.setrlimit(resource.RLIMIT_AS, (address_space_bytes, address_space_bytes))


def die_with_parent(expected_parent):
    library = ctypes.CDLL(None, use_errno=True)
    library.prctl.argtypes = [ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong]
    if library.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:  # PR_SET_PDEATHSIG
        raise ScreenError("cannot bind CPU worker lifetime to its parent")
    if os.getppid() != expected_parent:
        os.kill(os.getpid(), signal.SIGKILL)


def decode_window(source_fd, ffmpeg_fd, window, timeout):
    milliseconds = window["end_ms"] - window["start_ms"]
    maximum = milliseconds * 32
    command = [f"/proc/self/fd/{ffmpeg_fd}", "-v", "error", "-nostdin", "-threads", "1",
               "-filter_threads", "1", "-hwaccel", "none", "-protocol_whitelist", "file,pipe",
               "-format_whitelist", FORMATS, "-ss", f'{window["start_ms"] / 1000:.3f}',
               "-i", f"/proc/self/fd/{source_fd}", "-t", f"{milliseconds / 1000:.3f}",
               "-map", "0:a:0", "-vn", "-sn", "-dn", "-threads", "1", "-ac", "1",
               "-ar", "16000", "-c:a", "pcm_s16le", "-f", "s16le", "pipe:1"]
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
        parent_pid = os.getpid()
        def limits():
            die_with_parent(parent_pid)
            resource.setrlimit(resource.RLIMIT_FSIZE, (2 * MAX_PCM, 2 * MAX_PCM))
            cpu_seconds = max(1, int(timeout) + 2)
            resource.setrlimit(resource.RLIMIT_CPU, (cpu_seconds, cpu_seconds))
            child_limits(DECODE_ADDRESS_SPACE_BYTES)
        process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=output, stderr=errors,
                                   pass_fds=(source_fd, ffmpeg_fd), start_new_session=True,
                                   preexec_fn=limits, env={"PATH": "/usr/bin:/bin", "LANG": "C",
                                                         "CUDA_VISIBLE_DEVICES": "", "OMP_NUM_THREADS": "1"})
        try:
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                raise ScreenError("audio decode timed out; completed checkpoints are preserved") from None
            if process.returncode != 0:
                raise ScreenError("bounded local audio decode failed; completed windows are preserved")
            output.seek(0)
            body = output.read(MAX_PCM + 1)
            if not body or len(body) > maximum or len(body) % 2:
                raise ScreenError("decoded audio is empty, oversized, or malformed")
            # Materially short audio means a bad duration/offset, not negative evidence.
            if len(body) != maximum:
                raise ScreenError("decoded window is short; check recording duration/timeline")
            return body
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()


def send_message(connection, value, deadline):
    body = canonical(value)
    if len(body) > MAX_MESSAGE:
        raise ScreenError("worker message exceeds its size bound")
    connection.settimeout(remaining(deadline))
    connection.sendall(len(body).to_bytes(4, "big") + body)


def receive_message(connection, deadline):
    def receive(count):
        body = bytearray()
        while len(body) < count:
            connection.settimeout(remaining(deadline))
            part = connection.recv(count - len(body))
            if not part:
                raise EOFError("CPU worker connection closed")
            body.extend(part)
        return bytes(body)
    length = int.from_bytes(receive(4), "big")
    if not 0 < length <= MAX_MESSAGE:
        raise ScreenError("worker message exceeds its size bound")
    value = json.loads(receive(length))
    canonical(value)
    if not isinstance(value, dict):
        raise ScreenError("invalid CPU worker response")
    return value


def _model_child(connection, models, threads, parent_pid, cpu_seconds, implementation):
    try:
        os.setsid()
        die_with_parent(parent_pid)
        resource.setrlimit(resource.RLIMIT_CPU, (cpu_seconds, cpu_seconds))
        os.environ.update({"CUDA_VISIBLE_DEVICES": "", "HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
                           "HF_HUB_DISABLE_TELEMETRY": "1", "PYANNOTE_METRICS_ENABLED": "0",
                           "OMP_NUM_THREADS": str(threads), "MKL_NUM_THREADS": str(threads),
                           "OPENBLAS_NUM_THREADS": str(threads), "NUMEXPR_NUM_THREADS": str(threads)})
        child_limits()
        if implementation_hashes() != implementation:
            raise ScreenError("screen implementation changed before model startup")
        worker = engine.CpuScreenEngine(models, threads=threads)
        deadline = time.monotonic() + cpu_seconds
        while True:
            request = receive_message(connection, deadline)
            exact(request, {"pcm_base64", "window"}, "model request")
            pcm = base64.b64decode(request["pcm_base64"], validate=True)
            window = request["window"]
            observation = worker.analyze(pcm, **window)
            if implementation_hashes() != implementation:
                raise ScreenError("screen implementation changed during model inference")
            send_message(connection, {"observation": observation, "runtime": engine.runtime_versions()}, deadline)
    except EOFError:
        pass
    except BaseException as error:
        try:
            send_message(connection, {"error": f"{type(error).__name__}: {str(error)[:1500]}"}, time.monotonic() + 1)
        except (BrokenPipeError, EOFError, OSError, ScreenError):
            pass
    finally:
        connection.close()


class ModelProcess:
    def __init__(self, models, threads, cpu_seconds=610, implementation=None):
        context = multiprocessing.get_context("spawn")
        self.connection, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        self.process = context.Process(target=_model_child,
                                       args=(child, models, threads, os.getpid(), cpu_seconds,
                                             implementation or implementation_hashes()), daemon=True)
        self.process.start()
        child.close()

    def analyze(self, pcm, window, timeout):
        deadline = time.monotonic() + timeout
        try:
            send_message(self.connection, {"pcm_base64": base64.b64encode(pcm).decode("ascii"), "window": window}, deadline)
            value = receive_message(self.connection, deadline)
        except TimeoutError:
            raise ScreenError("CPU model window timed out; completed checkpoints are preserved") from None
        except (EOFError, BrokenPipeError, ConnectionResetError):
            raise ScreenError("CPU model worker exited without a result") from None
        if "error" in value:
            raise ScreenError(value["error"])
        exact(value, {"observation", "runtime"}, "model response")
        return value

    def close(self):
        self.connection.close()
        if self.process.is_alive():
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                self.process.kill()
        self.process.join(timeout=2)
        if self.process.is_alive():
            self.process.kill()
            self.process.join(timeout=2)


def checkpoint_path(job, index):
    return job / f"window-{index:04d}.json"


def observations_for(plan, job, binding, *, cache=None):
    observations, checksums, runtime = [], [], None
    for window in plan["windows"]:
        path = checkpoint_path(job, window["index"])
        if not exists(path):
            continue
        row = read_json(path)
        exact(row, {"kind", "schema_version", "plan_id", "binding_sha256", "window", "pcm_sha256",
                    "observation", "runtime"}, "window checkpoint")
        if (row["kind"] != "himr_speaker_screen_window" or type(row["schema_version"]) is not int or row["schema_version"] != 1
                or row["plan_id"] != plan["plan_id"] or row["binding_sha256"] != digest(binding)
                or canonical(row["window"]) != canonical(window) or not isinstance(row["pcm_sha256"], str)
                or not SHA.fullmatch(row["pcm_sha256"])):
            raise ScreenError("checkpoint does not match its source/plan/window")
        if not isinstance(row["observation"], dict) or row["observation"].get("index") != window["index"]:
            raise ScreenError("checkpoint observation belongs to a different probe")
        if not isinstance(row["runtime"], dict) or not row["runtime"]:
            raise ScreenError("checkpoint has no runtime provenance")
        if runtime is not None and runtime != row["runtime"]:
            raise ScreenError("checkpoint runtime versions differ")
        runtime = row["runtime"]
        observations.append(row["observation"])
        checksums.append({"index": window["index"], "sha256": digest(row)})
    # Recompute, never trust a cached count or classifier result.
    summary = core.summarize(plan["order"]["recording"]["duration_ms"], plan["windows"],
                             observations, plan["order"]["policy"], cache=cache)
    return observations, checksums, runtime, summary


def _result_document(plan, binding, observations, checksums, runtime, summary):
    complete = len(observations) == len(plan["windows"])
    positive_stop = bool(plan["order"]["resources"].get("early_stop_on_positive", False)
                         and summary["status"] == "multiple_speaker_candidate")
    return {"kind": "himr_cpu_speaker_screen_result", "schema_version": 1,
            "plan_id": plan["plan_id"], "recording": plan["order"]["recording"],
            "state": "completed" if complete else "paused", "completed_windows": len(observations),
            "screening_decision_complete": complete or positive_stop,
            "stop_reason": ("sampling_plan_completed" if complete else
                            "supported_multiple_speakers" if positive_stop else "invocation_limit"),
            "planned_windows": len(plan["windows"]), "remaining_windows": len(plan["windows"]) - len(observations),
            "source_binding": binding, "runtime": runtime, "checkpoint_hashes": checksums,
            "summary": summary, "policy": {"visibility": "private", "cpu_only": True,
            "scores_calibrated": False, "human_review_required": True, "person_identity_claimed": False,
            "full_diarization": False, "whole_recording_solo_claim": False,
            "catalogue_mutation": False, "source_mutation": False, "publication_authority": "none"}}


def report(plan, job, binding, *, cache=None):
    return _result_document(plan, binding, *observations_for(plan, job, binding, cache=cache))


def validate_binding(binding, plan):
    exact(binding, {"kind", "schema_version", "plan_id", "source_witness", "source_sha256_reverified"}, "source binding")
    if (binding["kind"] != "himr_speaker_screen_source_binding" or type(binding["schema_version"]) is not int or binding["schema_version"] != 1
            or binding["plan_id"] != plan["plan_id"]
            or binding["source_sha256_reverified"] is not (plan["order"]["source_verification"] == "sha256")):
        raise ScreenError("source binding differs from plan")
    exact(binding["source_witness"], {"st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns",
                                      "st_mode", "st_uid", "st_nlink"}, "source witness")
    if any(type(value) is not int for value in binding["source_witness"].values()):
        raise ScreenError("invalid source witness")


def read_status(plan):
    root = Path(plan["order"]["output_root"])
    if not exists(root):
        return {"state": "not_started", "plan_id": plan["plan_id"], "planned_windows": len(plan["windows"])}
    with paths.retained_directory(root):
        return _read_status_at(plan, root)


def _read_status_at(plan, root):
    if read_json(root / "workspace.json") != MARKER:
        raise ScreenError("screen workspace marker differs")
    job = root / plan["plan_id"]
    if not exists(job):
        return {"state": "not_started", "plan_id": plan["plan_id"], "planned_windows": len(plan["windows"])}
    with paths.retained_directory(job):
        return _read_job_status(plan, job)


def _read_job_status(plan, job):
    if not exists(job / "source-binding.json"):
        # A crash can occur between creating the private directory, publishing
        # the exact plan, and publishing the source witness. Only those two
        # pristine initialization states may be resumed without existing proof.
        with os.scandir(paths.anchor(job)) as entries:
            names = {entry.name for entry in entries}
        if not names <= {"plan.json"}:
            raise ScreenError("incomplete source initialization has unexpected artifacts")
        if "plan.json" in names and read_json(job / "plan.json") != plan:
            raise ScreenError("saved plan differs")
        return {"state": "not_started", "plan_id": plan["plan_id"],
                "planned_windows": len(plan["windows"]), "initialization_pending": True}
    if read_json(job / "plan.json") != plan:
        raise ScreenError("saved plan differs")
    binding = read_json(job / "source-binding.json")
    validate_binding(binding, plan)
    result = report(plan, job, binding)
    saved = job / "result.json"
    if exists(saved):
        if read_json(saved) != result:
            raise ScreenError("saved final result differs from checkpoint replay")
    # This is a read-only snapshot; it does not claim to inspect source availability.
    return {**result, "source_currently_rechecked": False}


def run_screen(plan):
    order = plan["order"]
    deadline = time.monotonic() + order["resources"]["max_run_seconds"]
    root = workspace(Path(order["output_root"]), create=True)
    with locked(root) as root_fd, opened(order["recording"]["path"]) as source, \
            opened(order["ffmpeg"]["path"], executable=True) as ffmpeg:
        source_witness = witness(source)
        if source_witness["st_size"] != order["recording"]["byte_count"]:
            raise ScreenError("source size differs from supplied provenance")
        if hash_fd(ffmpeg, 256 * 1024**2, deadline) != order["ffmpeg"]["sha256"]:
            raise ScreenError("FFmpeg SHA-256 mismatch")
        ffmpeg_witness = witness(ffmpeg)
        if order["source_verification"] == "sha256":
            if hash_fd(source, 64 * 1024**3, deadline) != order["recording"]["sha256"]:
                raise ScreenError("source SHA-256 mismatch")
        binding = {"kind": "himr_speaker_screen_source_binding", "schema_version": 1,
                   "plan_id": plan["plan_id"], "source_witness": source_witness,
                   "source_sha256_reverified": order["source_verification"] == "sha256"}
        job = root / plan["plan_id"]
        try:
            os.mkdir(plan["plan_id"], 0o700, dir_fd=root_fd)
            sync_directory(root)
        except FileExistsError:
            pass
        with paths.retained_directory(job):
            return _run_job(plan, job, binding, source, ffmpeg, source_witness, ffmpeg_witness, deadline)


def _run_job(plan, job, binding, source, ffmpeg, source_witness, ffmpeg_witness, deadline):
    order = plan["order"]
    write_immutable(job / "plan.json", plan)
    write_immutable(job / "source-binding.json", binding)
    cache = core.SummaryCache()
    observations, checksums, previous_runtime, summary = observations_for(plan, job, binding, cache=cache)
    initial = _result_document(plan, binding, observations, checksums, previous_runtime, summary)
    if exists(job / "result.json"):
        # Includes a positive decision with explicitly incomplete sampling.
        # Never continue a sealed decision or repair altered checkpoint history.
        if read_json(job / "result.json") != initial:
            raise ScreenError("saved final result differs from checkpoint replay")
        return initial
    if initial["screening_decision_complete"]:
        write_immutable(job / "result.json", initial)
        return initial
    completed = {row["index"] for row in observations}
    worker = None
    added = 0
    try:
        for window in plan["windows"]:
            if window["index"] in completed:
                continue
            if added >= order["resources"]["max_windows_per_run"] or time.monotonic() >= deadline:
                break
            if witness(source) != source_witness:
                raise ScreenError("source changed during screen")
            if witness(ffmpeg) != ffmpeg_witness:
                raise ScreenError("FFmpeg changed during screen")
            if implementation_hashes() != plan["implementation"]:
                raise ScreenError("screen implementation changed during run")
            timeout = min(order["resources"]["window_timeout_seconds"], remaining(deadline))
            pcm = decode_window(source, ffmpeg, window, timeout)
            if worker is None:
                worker = ModelProcess(order["models"], order["resources"]["threads"],
                                      order["resources"]["max_run_seconds"] + 10, plan["implementation"])
            answer = worker.analyze(pcm, window, min(order["resources"]["window_timeout_seconds"], remaining(deadline)))
            if previous_runtime is not None and answer["runtime"] != previous_runtime:
                raise ScreenError("runtime changed; do not mix embeddings across environments")
            previous_runtime = answer["runtime"]
            observation = answer["observation"]
            if not isinstance(observation, dict) or observation.get("index") != window["index"]:
                raise ScreenError("CPU observation belongs to a different probe")
            if (observation.get("embedding") is not None
                    and type(observation.get("speech_ms")) is int
                    and observation["speech_ms"] < order["policy"]["min_speech_ms"]):
                observation = {**observation, "embedding": None}
            # Verify observation semantics and source identity before publishing.
            core.summarize(order["recording"]["duration_ms"], plan["windows"],
                           [observation], order["policy"])
            with opened(order["recording"]["path"]) as current:
                if (witness(source) != source_witness or witness(current) != source_witness
                        or witness(ffmpeg) != ffmpeg_witness):
                    raise ScreenError("source changed or was replaced during screen")
            row = {"kind": "himr_speaker_screen_window", "schema_version": 1,
                   "plan_id": plan["plan_id"], "binding_sha256": digest(binding), "window": window,
                   "pcm_sha256": hashlib.sha256(pcm).hexdigest(), "observation": observation,
                   "runtime": answer["runtime"]}
            write_immutable(checkpoint_path(job, window["index"]), row)
            observations.append(observation)
            added += 1
            count = len(observations)
            # Geometric checks bound classification work even for a long solo
            # recording. Only supported positive evidence may stop early.
            if (order["resources"].get("early_stop_on_positive", False)
                    and count >= 2 * order["policy"]["min_support"]
                    and count & (count - 1) == 0):
                summary = core.summarize(order["recording"]["duration_ms"], plan["windows"],
                                         observations, order["policy"], cache=cache)
                if summary["status"] == "multiple_speaker_candidate":
                    break
    finally:
        if worker is not None:
            worker.close()
    result = report(plan, job, binding, cache=cache)
    if result["screening_decision_complete"]:
        write_immutable(job / "result.json", result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "plan", "run", "status"))
    parser.add_argument("--work-order", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--preset", choices=("throughput", "fast-triage"))
    parser.add_argument("--output")
    parser.add_argument("--output-root")
    parser.add_argument("--threads", type=int, choices=(1, 2))
    arguments = parser.parse_args(argv)
    try:
        if not SHA.fullmatch(arguments.expected_sha256):
            raise ScreenError("expected SHA-256 must be lowercase hexadecimal")
        order = read_json(path_value(arguments.work_order), arguments.expected_sha256)
        if arguments.command == "prepare":
            if not arguments.preset or not arguments.output or not arguments.output_root:
                raise ScreenError("prepare requires --preset, --output, and a separate --output-root")
            prepared = prepare_order(order, arguments.preset, arguments.output_root,
                                     threads=arguments.threads or 1)
            output = path_value(arguments.output)
            if output == path_value(arguments.work_order):
                raise ScreenError("prepare may not replace its source work order")
            old_root = path_value(order["output_root"])
            new_root = path_value(prepared["output_root"])
            if old_root == new_root or old_root in new_root.parents or new_root in old_root.parents:
                raise ScreenError("prepare requires a disjoint new screening workspace")
            if any(root == output or root in output.parents for root in (old_root, new_root)):
                raise ScreenError("prepared work order must be outside both screening workspaces")
            write_immutable(output, prepared)
            print(json.dumps({"status": "prepared", "work_order": str(output),
                              "sha256": digest(prepared), "preset": arguments.preset,
                              "planned_windows": len(build_plan(prepared)["windows"]),
                              "screening_started": False}, sort_keys=True))
            return 0
        if any(value is not None for value in (arguments.preset, arguments.output,
                                                arguments.output_root, arguments.threads)):
            raise ScreenError("preset/output/thread overrides are allowed only during prepare")
        plan = build_plan(order)
        if arguments.command == "plan":
            result = plan
        elif arguments.command == "status":
            result = read_status(plan)
        else:
            result = run_screen(plan)
        # Never print embeddings, even though checkpoint vectors are private.
        print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
        return 0
    except (ScreenError, OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"SpeakerScreenError: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Speaker screen interrupted; completed checkpoints are preserved.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
