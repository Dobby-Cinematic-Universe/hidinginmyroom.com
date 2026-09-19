#!/usr/bin/env python3
"""Opt-in, resumable, one-model long-form ASR plan runner.

This command is deliberately separate from the active autonomous controller.  It
replays a hash-pinned controller configuration and production profile, derives
the controller's exact GPU UUID lock, and binds CUDA visibility to that UUID.  It
loads one admitted Faster-Whisper model, retains and authenticates the normalized
parent FLAC once, and executes missing plan spans in order.  Adaptive spans share
one forward-only FFmpeg decode and a rolling in-memory overlap buffer.  No audio
chunk is written to disk.
"""

from __future__ import annotations

import argparse
import ctypes
import fcntl
import hashlib
import importlib.metadata
import importlib.util
import json
import os
import platform
import stat
import sys
import sysconfig
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import Any, Iterator, Sequence


ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATH = Path(__file__).resolve()
EXECUTOR_PATH = ROOT / "pipeline" / "gpu" / "longform_asr_execution_v1.py"
BINDINGS_KIND = "himr_longform_span_transcript_bindings"
BINDINGS_POLICY = {
    "visibility": "private",
    "source_artifact_mutation": False,
    "catalogue_mutation_authority": "none",
    "publication_authority": "none",
}
GPU_UUID_PREFIX = "GPU-"
REVIEWED_LONGFORM_RUNTIME_SHA256 = (
    "129f5a7013ad8213a079ff2755778cba16ebbfae00d20eafe0b40d406244bed7"
)
REVIEWED_RUNTIME_HELPER_SHA256 = (
    "84834a81aea2f7bc7e93e02e1c4915201bd6d297b4849baaea843bfa4b407b1b"
)
_HISTORICAL_BWRAP = {
    "name": "bubblewrap", "path": "/usr/bin/bwrap",
    "sha256": "139bf12775025adf5c8523d119c5ad2950281335573708fd839c60181a3886dc",
    "byte_count": 86552, "mode": "0755", "uid": 0,
}
# Fedora bubblewrap-0.12.0-1.fc44, verified with rpm -V on 2026-09-06.
# Long-form uses its pinned Python/FFmpeg directly, never this sandbox helper.
# This is historical receipt replay only, not approval for the sealed launcher.
_REVIEWED_BWRAP_SUCCESSOR = {
    **_HISTORICAL_BWRAP,
    "sha256": "6da06f152b0865172d73348c34cb88487c326ce2f21cd980fc25ff10c4dbcdfb",
    "byte_count": 90696,
}


class RunnerError(RuntimeError):
    """The opt-in runner failed a control, replay, or execution invariant."""


def _load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RunnerError(f"cannot load required module {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(name, None)
        raise
    return module


E = _load_module("himr_longform_asr_execution_v1_for_runner", EXECUTOR_PATH)


def _load_json(path: Path, label: str, maximum: int = 64 * 1024 * 1024) -> tuple[dict[str, Any], bytes]:
    try:
        body = path.read_bytes()
    except OSError as error:
        raise RunnerError(f"{label} cannot be read: {error}") from error
    if not body or len(body) > maximum:
        raise RunnerError(f"{label} is empty or exceeds its byte bound")
    value = E.parse_json(body, label)
    if not isinstance(value, dict):
        raise RunnerError(f"{label} must contain one JSON object")
    return value, body


def _hotwords(path: Path | None) -> list[str]:
    if path is None:
        return []
    value, _ = _load_json(path, "hotwords", 1024 * 1024)
    if set(value) != {"hotwords"} or not isinstance(value["hotwords"], list):
        raise RunnerError("hotwords file must be one exact {hotwords:[...]} object")
    return value["hotwords"]


def _work_orders(
    plan: dict[str, Any],
    output_root: Path,
    *,
    profile: dict[str, Any],
    initial_prompt: str | None,
    hotwords: Sequence[str],
) -> list[dict[str, Any]]:
    admitted = profile["decoding"]
    decoding = {
        "language": admitted["language"],
        "beam_size": admitted["beam_size"],
        "best_of": admitted["best_of"],
        "temperature": admitted["temperature"],
        "word_timestamps": admitted["word_timestamps"],
        "vad_filter": admitted["vad_filter"],
        "condition_on_previous_text": admitted["condition_on_previous_text"],
        "initial_prompt": initial_prompt,
        "hotwords": sorted(set(hotwords), key=str.casefold),
    }
    execution_lineage = {
        "longform_scope_status": "candidate_unsoaked",
        "production_profile": {
            "profile_id": profile["profile_id"],
            "identity_sha256": profile["identity_sha256"],
            "physical_sha256": profile["_physical_sha256"],
        },
        "runtime_admission": {
            "receipt_id": profile["_runtime_receipt_id"],
            "identity_sha256": profile["_runtime_identity_sha256"],
            "physical_sha256": profile["_runtime_physical_sha256"],
            "status": profile["_runtime_status"],
        },
        "model": {
            "repository": profile["model"]["repository"],
            "revision": profile["model"]["revision"],
            "identity_sha256": profile["model"]["identity_sha256"],
        },
    }
    return E.make_work_orders_from_plan(
        plan,
        output_root=output_root,
        execution_lineage=execution_lineage,
        decoding=decoding,
    )


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            body = handle.read(1024 * 1024)
            if not body:
                break
            digest.update(body)
    return digest.hexdigest()


def _binding(order: dict[str, Any]) -> dict[str, Any]:
    plan = E.result_plan(order)
    directory = Path(plan["result_directory"])
    if not directory.exists():
        return {
            "span_id": order["span"]["span_id"],
            "status": "pending",
            "result": None,
            "transcript": None,
            "failure_code": None,
        }
    bundle = E.replay_materialized_result(order)
    result_path = Path(plan["result_path"])
    transcript_path = Path(plan["transcript_path"])
    return {
        "span_id": order["span"]["span_id"],
        "status": "completed",
        "result": {"path": str(result_path), "sha256": _sha256_path(result_path)},
        "transcript": {
            "path": str(transcript_path),
            "sha256": _sha256_path(transcript_path),
        },
        "failure_code": None,
    }


def _controller_stop_requested(control: dict[str, Any]) -> bool:
    """Read only the exact small control projection between logical spans."""

    path = control["controller_state_root"] / "control.json"
    value, _body = _load_json(path, "controller Stop control", 32 * 1024)
    if set(value) != {
        "kind",
        "schema_version",
        "config_id",
        "generation",
        "desired_state",
        "requested_at",
    }:
        raise RunnerError("controller Stop control fields differ")
    if (
        value["kind"] != "himr_autonomous_controller_control"
        or value["schema_version"] != 1
        or value["config_id"] != control["controller_config_id"]
        or isinstance(value["generation"], bool)
        or not isinstance(value["generation"], int)
        or value["generation"] < 0
        or value["desired_state"] not in {"running", "stopped"}
        or (
            value["requested_at"] is not None
            and not isinstance(value["requested_at"], str)
        )
    ):
        raise RunnerError("controller Stop control is invalid")
    return value["desired_state"] == "stopped"


def _ordinary_gpu_demand(control: dict[str, Any]) -> bool:
    """Return whether the primary controller has work ready for its GPU lane.

    The opportunity lock closes the race with child launch.  This small cached
    projection is consulted only after that lock is held and between completed
    long-form spans, allowing a newly materialized ordinary batch to preempt the
    recording without discarding any published span result.
    """

    path = control["controller_state_root"] / "status.json"
    value, _body = _load_json(path, "controller GPU status", 256 * 1024)
    if (
        value.get("kind") != "himr_autonomous_controller_status"
        or value.get("schema_version") != 1
        or value.get("config_id") != control["controller_config_id"]
        or value.get("config_sha256") != control["config_sha256"]
    ):
        raise RunnerError("controller GPU status binding is invalid")
    if (
        value.get("desired_state") != "running"
        or value.get("actual_state") != "running"
        or value.get("lifecycle") != "running"
    ):
        return True
    monitor = value.get("monitor")
    gpu = monitor.get("gpu_readiness") if isinstance(monitor, dict) else None
    if not isinstance(gpu, dict) or gpu.get("status") not in {
        "complete",
        "held",
        "progressed",
    }:
        raise RunnerError("controller GPU status projection is invalid")
    counters: list[int] = []
    for name in (
        "pending_batches",
        "pending_items",
        "ready_batches",
        "active_children",
        "buffered_ready_items",
    ):
        observed = gpu.get(name)
        if (
            isinstance(observed, bool)
            or not isinstance(observed, int)
            or observed < 0
        ):
            raise RunnerError(f"controller GPU status {name} is invalid")
        counters.append(observed)
    current = gpu.get("current_gpu_child")
    if current is not None and not isinstance(current, dict):
        raise RunnerError("controller current GPU child projection is invalid")
    return any(counters) or current is not None


def _bindings(plan_sha256: str, orders: Sequence[dict[str, Any]]) -> dict[str, Any]:
    return {
        "kind": BINDINGS_KIND,
        "schema_version": 1,
        "parent_manifest_sha256": plan_sha256,
        "spans": [_binding(order) for order in orders],
        "policy": dict(BINDINGS_POLICY),
    }


def _write_exclusive_or_identical(path: Path, value: dict[str, Any]) -> None:
    body = E.canonical_bytes(value)
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    if path.exists():
        observed = path.read_bytes()
        if observed != body:
            raise RunnerError(f"refusing to replace differing bindings manifest {path}")
        return
    temporary = path.parent / f".{path.name}.tmp-{os.getpid()}"
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
        0o400,
    )
    try:
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise RunnerError("bindings manifest write was short")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        os.link(temporary, path, follow_symlinks=False)
        temporary.unlink()
        parent_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


@contextmanager
def _gpu_lock(path: Path, gpu_uuid: str) -> Iterator[None]:
    if not gpu_uuid.startswith(GPU_UUID_PREFIX) or len(gpu_uuid) > 100:
        raise RunnerError("GPU UUID is invalid")
    if path.name != f"gpu-{gpu_uuid}.lock":
        raise RunnerError("GPU lock filename does not bind the requested UUID")
    root_flags = (
        os.O_RDONLY
        | os.O_DIRECTORY
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        root_descriptor = os.open(path.parent, root_flags)
    except OSError as error:
        raise RunnerError(f"GPU lock root cannot be retained: {error}") from error
    try:
        root_info = os.fstat(root_descriptor)
        if (
            not stat.S_ISDIR(root_info.st_mode)
            or stat.S_IMODE(root_info.st_mode) != 0o700
            or root_info.st_uid != os.geteuid()
        ):
            raise RunnerError("GPU lock root must be an owned mode-0700 directory")
        root_linked = path.parent.stat(follow_symlinks=False)
        if (root_linked.st_dev, root_linked.st_ino) != (
            root_info.st_dev,
            root_info.st_ino,
        ):
            raise RunnerError("GPU lock root directory entry differs")
        flags = os.O_RDWR | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path.name, flags, dir_fd=root_descriptor)
        except OSError as error:
            raise RunnerError(f"GPU lock cannot be opened: {error}") from error
        try:
            info = os.fstat(descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_nlink != 1
                or info.st_size != 0
                or info.st_uid != os.geteuid()
            ):
                raise RunnerError("GPU lock must be an owned empty mode-0600 singleton file")
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise RunnerError("GPU UUID lock is occupied") from error
            linked = os.stat(path.name, dir_fd=root_descriptor, follow_symlinks=False)
            if (linked.st_dev, linked.st_ino) != (info.st_dev, info.st_ino):
                raise RunnerError("GPU lock directory entry differs from the held inode")
            try:
                yield
            finally:
                linked_after = os.stat(
                    path.name, dir_fd=root_descriptor, follow_symlinks=False
                )
                held_after = os.fstat(descriptor)
                root_after = path.parent.stat(follow_symlinks=False)
                if (root_after.st_dev, root_after.st_ino) != (
                    root_info.st_dev,
                    root_info.st_ino,
                ):
                    raise RunnerError("GPU lock root directory entry changed while held")
                if (linked_after.st_dev, linked_after.st_ino) != (
                    held_after.st_dev,
                    held_after.st_ino,
                ):
                    raise RunnerError("GPU lock directory entry changed while held")
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
    finally:
        os.close(root_descriptor)


@contextmanager
def _gpu_opportunity_lock(path: Path, gpu_uuid: str) -> Iterator[None]:
    """Reserve one schedulable GPU opportunity before either ASR lane starts.

    The ordinary controller holds this same lock for the lifetime of its child.
    Long-form acquisition is nonblocking so an ordinary batch always wins without
    creating a failed ordinary child attempt.
    """

    if not gpu_uuid.startswith(GPU_UUID_PREFIX) or len(gpu_uuid) > 100:
        raise RunnerError("GPU UUID is invalid")
    if path.name != f"gpu-{gpu_uuid}.opportunity.lock":
        raise RunnerError(
            "GPU opportunity lock filename does not bind the requested UUID"
        )
    root_flags = (
        os.O_RDONLY
        | os.O_DIRECTORY
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        root_descriptor = os.open(path.parent, root_flags)
    except OSError as error:
        raise RunnerError(
            f"GPU opportunity lock root cannot be retained: {error}"
        ) from error
    descriptor = -1
    try:
        root_info = os.fstat(root_descriptor)
        if (
            not stat.S_ISDIR(root_info.st_mode)
            or stat.S_IMODE(root_info.st_mode) != 0o700
            or root_info.st_uid != os.geteuid()
        ):
            raise RunnerError(
                "GPU opportunity lock root must be an owned mode-0700 directory"
            )
        root_linked = path.parent.stat(follow_symlinks=False)
        if (root_linked.st_dev, root_linked.st_ino) != (
            root_info.st_dev,
            root_info.st_ino,
        ):
            raise RunnerError("GPU opportunity lock root directory entry differs")
        flags = (
            os.O_RDWR
            | os.O_CREAT
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            descriptor = os.open(
                path.name, flags, 0o600, dir_fd=root_descriptor
            )
        except OSError as error:
            raise RunnerError(f"GPU opportunity lock cannot be opened: {error}") from error
        info = os.fstat(descriptor)
        linked = os.stat(path.name, dir_fd=root_descriptor, follow_symlinks=False)
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
            or info.st_size != 0
            or info.st_uid != os.geteuid()
            or (linked.st_dev, linked.st_ino) != (info.st_dev, info.st_ino)
        ):
            raise RunnerError(
                "GPU opportunity lock must be an owned empty mode-0600 singleton file"
            )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RunnerError("GPU opportunity lock is occupied") from error
        linked = os.stat(path.name, dir_fd=root_descriptor, follow_symlinks=False)
        if (linked.st_dev, linked.st_ino) != (info.st_dev, info.st_ino):
            raise RunnerError(
                "GPU opportunity lock directory entry differs from the held inode"
            )
        try:
            yield
        finally:
            linked_after = os.stat(
                path.name, dir_fd=root_descriptor, follow_symlinks=False
            )
            held_after = os.fstat(descriptor)
            root_after = path.parent.stat(follow_symlinks=False)
            if (root_after.st_dev, root_after.st_ino) != (
                root_info.st_dev,
                root_info.st_ino,
            ):
                raise RunnerError(
                    "GPU opportunity lock root directory entry changed while held"
                )
            if (linked_after.st_dev, linked_after.st_ino) != (
                held_after.st_dev,
                held_after.st_ino,
            ):
                raise RunnerError(
                    "GPU opportunity lock directory entry changed while held"
                )
    finally:
        if descriptor >= 0:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
        os.close(root_descriptor)


def _verify_gpu_uuid(
    gpu_uuid: str,
    telemetry: dict[str, Any],
    *,
    pynvml_module: Any | None = None,
) -> dict[str, Any]:
    """Admit the exact GPU and its idle/free/cool resource envelope."""

    if pynvml_module is None:
        try:
            import pynvml as pynvml_module
        except ImportError as error:
            raise RunnerError("pynvml is required for live GPU admission") from error
    pynvml_module.nvmlInit()
    try:
        try:
            handle = pynvml_module.nvmlDeviceGetHandleByUUID(gpu_uuid)
        except TypeError:
            handle = pynvml_module.nvmlDeviceGetHandleByUUID(gpu_uuid.encode("ascii"))
        observed = pynvml_module.nvmlDeviceGetUUID(handle)
        if isinstance(observed, bytes):
            observed = observed.decode("ascii")
        if observed != gpu_uuid:
            raise RunnerError("live GPU UUID differs")
        try:
            compute = list(pynvml_module.nvmlDeviceGetComputeRunningProcesses(handle))
            graphics = list(pynvml_module.nvmlDeviceGetGraphicsRunningProcesses(handle))
        except Exception as error:
            raise RunnerError(f"NVML process admission query failed: {error}") from error
        # Recent NVIDIA stacks can expose desktop graphics contexts in both
        # NVML process lists.  Match the production batch worker's admission
        # policy: only a foreign compute PID that is not also a graphics PID is
        # evidence of a competing CUDA job.
        graphics_pids = {
            int(row.pid)
            for row in graphics
            if isinstance(getattr(row, "pid", None), int)
            and not isinstance(getattr(row, "pid", None), bool)
        }
        foreign_cuda_pids = sorted(
            {
                int(row.pid)
                for row in compute
                if isinstance(getattr(row, "pid", None), int)
                and not isinstance(getattr(row, "pid", None), bool)
                and row.pid != os.getpid()
                and row.pid not in graphics_pids
            }
        )
        if foreign_cuda_pids:
            raise RunnerError(
                f"other CUDA compute processes are active: {foreign_cuda_pids}"
            )
        minimum_free = telemetry.get("minimum_free_vram_bytes")
        maximum_temperature = telemetry.get("maximum_temperature_c")
        if (
            not isinstance(minimum_free, int)
            or isinstance(minimum_free, bool)
            or minimum_free < 0
            or not isinstance(maximum_temperature, int)
            or isinstance(maximum_temperature, bool)
            or maximum_temperature < 1
        ):
            raise RunnerError("production profile telemetry limits are invalid")
        try:
            memory = pynvml_module.nvmlDeviceGetMemoryInfo(handle)
            total = int(memory.total)
            used = int(memory.used)
            free = int(memory.free)
            temperature = int(
                pynvml_module.nvmlDeviceGetTemperature(
                    handle, pynvml_module.NVML_TEMPERATURE_GPU
                )
            )
        except Exception as error:
            raise RunnerError(f"NVML resource admission query failed: {error}") from error
        if (
            min(total, used, free) < 0
            or total < 1
            or used > total
            or free > total
            or abs((used + free) - total) > 64 * 1024**2
        ):
            raise RunnerError("NVML memory admission observation is inconsistent")
        if free < minimum_free:
            raise RunnerError(
                f"GPU free VRAM {free} is below the admitted reserve {minimum_free}"
            )
        if temperature < 0 or temperature > maximum_temperature:
            raise RunnerError(
                f"GPU temperature {temperature}C exceeds the admitted maximum "
                f"{maximum_temperature}C"
            )
        return {
            "gpu_uuid": gpu_uuid,
            "foreign_cuda_pids": [],
            "compute_process_count": len(compute),
            "graphics_process_count": len(graphics),
            "memory": {"total_bytes": total, "used_bytes": used, "free_bytes": free},
            "temperature_c": temperature,
        }
    finally:
        pynvml_module.nvmlShutdown()


def _ensure_cuda_wheel_libraries(control: dict[str, Any]) -> None:
    """Re-exec with only the cuBLAS directory admitted by the pinned runtime.

    The supervisor intentionally starts this worker with a small clean
    environment.  NVIDIA's Python wheel installs cuBLAS outside the dynamic
    loader's default search path, so CTranslate2 otherwise imports successfully
    but fails on its first lazy CUDA operation.  Re-exec must happen before any
    source descriptor or GPU lock is retained because glibc snapshots
    ``LD_LIBRARY_PATH`` at process startup.
    """

    packages = control.get("runtime", {}).get("runtime", {}).get("packages")
    if not isinstance(packages, dict) or not isinstance(
        packages.get("nvidia-cublas-cu12"), str
    ):
        raise RunnerError("runtime admission lacks its pinned CUDA 12 cuBLAS package")
    purelib_value = sysconfig.get_paths().get("purelib")
    if not isinstance(purelib_value, str) or not purelib_value:
        raise RunnerError("pinned Python purelib path is unavailable")
    library_directory = Path(purelib_value) / "nvidia" / "cublas" / "lib"
    required_libraries = (
        library_directory / "libcublasLt.so.12",
        library_directory / "libcublas.so.12",
    )
    try:
        library_metadata = [path.lstat() for path in required_libraries]
    except OSError as error:
        raise RunnerError(
            "pinned CUDA 12 cuBLAS wheel libraries are missing from "
            f"{library_directory}: {error}"
        ) from error
    if not library_directory.is_dir() or any(
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_nlink != 1
        or metadata.st_size < 1
        or stat.S_IMODE(metadata.st_mode) & 0o022
        for metadata in library_metadata
    ):
        raise RunnerError(
            "pinned CUDA 12 cuBLAS wheel libraries have unsafe metadata under "
            f"{library_directory}"
        )
    expected_library_path = str(library_directory)
    if ":" in expected_library_path:
        raise RunnerError("pinned cuBLAS library path contains a loader separator")
    if os.environ.get("LD_LIBRARY_PATH") != expected_library_path:
        environment = dict(os.environ)
        environment.pop("PYTHONHOME", None)
        environment.pop("PYTHONPATH", None)
        environment.update(
            {
                "LD_LIBRARY_PATH": expected_library_path,
                "HF_HUB_OFFLINE": "1",
                "HF_DATASETS_OFFLINE": "1",
                "HF_HUB_DISABLE_TELEMETRY": "1",
                "DO_NOT_TRACK": "1",
            }
        )
        os.execve(
            sys.executable,
            [sys.executable, "-B", "-I", str(SOURCE_PATH), *sys.argv[1:]],
            environment,
        )
    try:
        ctypes.CDLL("libcublasLt.so.12", mode=ctypes.RTLD_GLOBAL)
        ctypes.CDLL("libcublas.so.12", mode=ctypes.RTLD_GLOBAL)
    except OSError as error:
        raise RunnerError(f"pinned CUDA 12 cuBLAS loader probe failed: {error}") from error


def _model(control: dict[str, Any], decoder: Any | None) -> Any:
    gpu_uuid = control["gpu_uuid"]
    visible = os.environ.get("CUDA_VISIBLE_DEVICES")
    if visible is not None and visible != gpu_uuid:
        raise RunnerError(
            "preexisting CUDA_VISIBLE_DEVICES differs from the admitted GPU UUID"
        )
    os.environ["CUDA_VISIBLE_DEVICES"] = gpu_uuid
    offline_environment = {
        "CUDA_DEVICE_ORDER": "PCI_BUS_ID",
        "DO_NOT_TRACK": "1",
        "HF_DATASETS_OFFLINE": "1",
        "HF_HUB_DISABLE_TELEMETRY": "1",
        "HF_HUB_OFFLINE": "1",
        "TRANSFORMERS_OFFLINE": "1",
    }
    for name, expected in offline_environment.items():
        observed = os.environ.get(name)
        if observed is not None and observed != expected:
            raise RunnerError(f"preexisting {name} differs from the offline runtime policy")
        os.environ[name] = expected
    try:
        from faster_whisper import WhisperModel
    except ImportError as error:
        raise RunnerError(
            "faster-whisper is unavailable; run this command with the pinned GPU venv Python"
        ) from error
    model = WhisperModel(
        str(control["model_root"]),
        device="cuda",
        device_index=0,
        compute_type=control["profile"]["hardware"]["compute_type"],
        cpu_threads=control["profile"]["decoding"]["cpu_threads"],
        num_workers=control["profile"]["decoding"]["num_workers"],
        local_files_only=True,
    )
    return E.FasterWhisperModelEngine(
        model,
        library_version=importlib.metadata.version("faster-whisper"),
        model_revision=control["profile"]["model"]["revision"],
        model_identity_sha256=control["profile"]["model"]["identity_sha256"],
        ephemeral_decoder=decoder,
    )


def _replay_longform_runtime(
    module: ModuleType, runtime: dict[str, Any], physical_sha256: str
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    """Replay one historical candidate across an exact, unused-tool upgrade.

    All other runtime/model/image/ownership checks remain the legacy validator's.
    The old receipt is not rewritten, the helper is restored on every exit, and
    an arbitrary changed executable cannot pass either allowed SHA-256.
    """

    migrations: list[dict[str, str]] = []
    if physical_sha256 != REVIEWED_LONGFORM_RUNTIME_SHA256:
        return module.validate_receipt(runtime, require_admitted=False, deep_image=False), migrations
    if (
        hashlib.sha256(E.canonical_bytes(runtime)).hexdigest() != physical_sha256
        or runtime.get("status") != "candidate"
        or _HISTORICAL_BWRAP not in runtime.get("trusted_install", {}).get("system_tools", [])
        or hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
        != REVIEWED_RUNTIME_HELPER_SHA256
    ):
        raise RunnerError("reviewed long-form runtime tool transition binding differs")
    original = module._tool_reference
    old_reference = {key: _HISTORICAL_BWRAP[key] for key in ("name", "path", "sha256")}
    new_reference = {key: _REVIEWED_BWRAP_SUCCESSOR[key] for key in ("name", "path", "sha256")}

    def replay_tool(value: Any, label: str) -> dict[str, Any]:
        try:
            return original(value, label)
        except module.RuntimeAdmissionV2Error as error:
            if value != old_reference or str(error) != f"{label} SHA-256 differs from its reference":
                raise
            observed = original(new_reference, label)
            if observed != _REVIEWED_BWRAP_SUCCESSOR:
                raise RunnerError("reviewed bubblewrap successor metadata differs")
            migrations.append({
                "tool": "bubblewrap",
                "from_sha256": _HISTORICAL_BWRAP["sha256"],
                "to_sha256": _REVIEWED_BWRAP_SUCCESSOR["sha256"],
                "scope": "historical_unused_launcher_tool_in_longform_candidate_only",
            })
            return dict(_HISTORICAL_BWRAP)

    module._tool_reference = replay_tool
    try:
        validated = module.validate_receipt(runtime, require_admitted=False, deep_image=False)
    finally:
        module._tool_reference = original
    return validated, migrations


def _admitted_control(args: argparse.Namespace, *, validate_model: bool) -> dict[str, Any]:
    config, config_body = _load_json(args.controller_config, "controller config")
    observed_config_sha = hashlib.sha256(config_body).hexdigest()
    if observed_config_sha != args.controller_config_sha256:
        raise RunnerError("controller config SHA-256 differs")
    if config.get("kind") != "himr_autonomous_archive_controller_config":
        raise RunnerError("controller config kind is unsupported")
    gpu = config.get("gpu_readiness")
    if not isinstance(gpu, dict) or gpu.get("enabled") is not True:
        raise RunnerError("controller GPU readiness is not enabled")
    profile_path = Path(gpu.get("production_profile", ""))
    profile_expected_sha = gpu.get("production_profile_sha256")
    profile, profile_body = _load_json(profile_path, "production profile", 1024 * 1024)
    if hashlib.sha256(profile_body).hexdigest() != profile_expected_sha:
        raise RunnerError("production profile SHA-256 differs")
    profile_module = _load_module(
        "himr_gpu_profile_v2_for_longform_runner",
        ROOT / "pipeline/gpu/production_profile_v2.py",
    )
    try:
        profile = profile_module.validate_profile(profile)
    except Exception as error:
        raise RunnerError(f"production profile replay failed: {error}") from error
    gpu_uuid = profile["hardware"]["gpu_uuid"]
    if profile["hardware"]["device_index"] != 0:
        raise RunnerError("long-form runner requires admitted visible device index zero")
    lock_root = Path(gpu.get("lock_root", ""))
    if not lock_root.is_absolute():
        raise RunnerError("controller GPU lock root is not absolute")
    lock_path = lock_root / f"gpu-{gpu_uuid}.lock"
    opportunity_lock_path = lock_root / f"gpu-{gpu_uuid}.opportunity.lock"
    runtime_path = Path(gpu.get("runtime_admission", ""))
    runtime_expected_sha = gpu.get("runtime_admission_sha256")
    runtime, runtime_body = _load_json(
        runtime_path, "runtime admission", 8 * 1024 * 1024
    )
    runtime_physical_sha = hashlib.sha256(runtime_body).hexdigest()
    if runtime_physical_sha != runtime_expected_sha:
        raise RunnerError("runtime admission SHA-256 differs")
    if (
        runtime.get("kind") != "himr_gpu_runtime_admission_receipt"
        or runtime.get("schema_version") != 2
        or runtime.get("status") not in {"candidate", "admitted"}
    ):
        raise RunnerError("runtime admission kind, version, or status is unsupported")
    runtime_identity = runtime.get("identity_sha256")
    runtime_receipt_id = runtime.get("receipt_id")
    runtime_core = {
        name: value
        for name, value in runtime.items()
        if name not in {"identity_sha256", "receipt_id"}
    }
    expected_runtime_identity = hashlib.sha256(E.canonical_bytes(runtime_core)).hexdigest()
    if (
        runtime_identity != expected_runtime_identity
        or runtime_receipt_id != f"gpurtv2_{expected_runtime_identity[:32]}"
    ):
        raise RunnerError("runtime admission semantic identity differs")
    runtime_profile = runtime.get("production_profile")
    if (
        not isinstance(runtime_profile, dict)
        or runtime_profile.get("identity_sha256") != profile["identity_sha256"]
    ):
        raise RunnerError("runtime admission production profile differs")
    runtime_packages = runtime.get("runtime", {}).get("packages")
    if not isinstance(runtime_packages, dict) or not runtime_packages:
        raise RunnerError("runtime admission lacks its package closure")
    for name, version in runtime_packages.items():
        if not isinstance(name, str) or not name or not isinstance(version, str) or not version:
            raise RunnerError("runtime admission package closure is malformed")
    profile_for_orders = dict(profile)
    profile_for_orders.update(
        {
            "_physical_sha256": profile_expected_sha,
            "_runtime_receipt_id": runtime_receipt_id,
            "_runtime_identity_sha256": runtime_identity,
            "_runtime_physical_sha256": runtime_physical_sha,
            "_runtime_status": runtime["status"],
        }
    )
    control = {
        "config_sha256": observed_config_sha,
        "controller_config_id": config.get("config_id"),
        "controller_state_root": Path(config.get("state_root", "")),
        "profile": profile_for_orders,
        "profile_path": profile_path,
        "profile_sha256": profile_expected_sha,
        "gpu_uuid": gpu_uuid,
        "lock_path": lock_path,
        "opportunity_lock_path": opportunity_lock_path,
        "runtime": runtime,
        "runtime_path": runtime_path,
        "runtime_sha256": runtime_physical_sha,
        "model_root": None,
    }
    if (
        getattr(args, "honor_controller_stop", False)
        or getattr(args, "yield_to_ordinary_gpu", False)
    ) and (
        not isinstance(control["controller_config_id"], str)
        or not control["controller_config_id"].startswith("himrautocfg_")
        or not control["controller_state_root"].is_absolute()
    ):
        raise RunnerError("controller Stop binding is invalid")
    if not validate_model:
        return control
    runtime_module = _load_module(
        "himr_admit_runtime_v2_for_longform_runner",
        ROOT / "pipeline/gpu/admit_runtime_v2.py",
    )
    try:
        validated_runtime, tool_migrations = _replay_longform_runtime(
            runtime_module, runtime, runtime_physical_sha
        )
    except Exception as error:
        raise RunnerError(f"runtime admission replay failed: {error}") from error
    if validated_runtime != runtime:
        raise RunnerError("runtime admission replay changed the receipt")
    control["runtime_tool_migrations"] = tool_migrations
    expected_python = runtime.get("runtime", {}).get("python_version")
    if expected_python != platform.python_version():
        raise RunnerError(
            f"runtime Python differs: {platform.python_version()} != {expected_python}"
        )
    for name, version in sorted(runtime_packages.items()):
        try:
            observed_version = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError as error:
            raise RunnerError(f"runtime package is absent: {name}") from error
        if observed_version != version:
            raise RunnerError(
                f"runtime package {name} differs: {observed_version} != {version}"
            )
    admissions_root = profile_path.parent.parent / "production-model-admissions"
    candidates = []
    for manifest_path in admissions_root.glob("*/manifest.json"):
        try:
            manifest, _ = _load_json(manifest_path, "model admission manifest", 32 * 1024 * 1024)
        except RunnerError:
            continue
        if manifest.get("identity_sha256") == profile["model"]["identity_sha256"]:
            candidates.append(manifest_path.parent)
    if len(candidates) != 1:
        raise RunnerError("admitted production model bundle is absent or ambiguous")
    admission_module = _load_module(
        "himr_admit_hf_model_for_longform_runner",
        ROOT / "pipeline/gpu/admit_hf_model.py",
    )
    try:
        evidence = admission_module.validate_bundle(candidates[0])
    except Exception as error:
        raise RunnerError(f"production model bundle replay failed: {error}") from error
    if (
        evidence["manifest_identity_sha256"]
        != profile["model"]["identity_sha256"]
        or evidence["revision"] != profile["model"]["revision"]
        or evidence["repository"] != profile["model"]["repository"]
    ):
        raise RunnerError("production model bundle differs from the production profile")
    control["model_root"] = candidates[0] / "snapshot"
    return control


def _common(
    args: argparse.Namespace, *, validate_model: bool = False
) -> tuple[dict[str, Any], bytes, list[dict[str, Any]], dict[str, Any]]:
    plan, plan_body = _load_json(args.plan, "long-form plan")
    control = _admitted_control(args, validate_model=validate_model)
    # The batch projector performs one full deterministic planner replay.
    orders = _work_orders(
        plan,
        args.output_root,
        profile=control["profile"],
        initial_prompt=args.initial_prompt,
        hotwords=_hotwords(args.hotwords_json),
    )
    return plan, plan_body, orders, control


def command_status(args: argparse.Namespace) -> dict[str, Any]:
    plan, plan_body, orders, _ = _common(args)
    bindings = _bindings(hashlib.sha256(plan_body).hexdigest(), orders)
    counts = {"completed": 0, "pending": 0, "failed": 0}
    for row in bindings["spans"]:
        counts[row["status"]] += 1
    return {
        "status": "complete" if counts["pending"] == 0 else "incomplete",
        "plan_id": plan["plan_id"],
        "strategy": plan["strategy"],
        "span_counts": counts,
        "bindings": bindings,
        "gpu_invoked": False,
    }


def command_run(args: argparse.Namespace) -> dict[str, Any]:
    plan, plan_body, orders, initial_control = _common(args)
    pending = []
    gpu_admission: dict[str, Any] | None = None
    for order in orders:
        if _binding(order)["status"] == "pending":
            pending.append(order)
    stop_observed = False
    ordinary_yield_observed = False
    model_load_count = 0
    executed: list[dict[str, Any]] = []
    if pending:
        control = _admitted_control(args, validate_model=True)
        if {
            key: control[key]
            for key in (
                "config_sha256",
                "profile_sha256",
                "runtime_sha256",
                "gpu_uuid",
                "lock_path",
                "opportunity_lock_path",
            )
        } != {
            key: initial_control[key]
            for key in (
                "config_sha256",
                "profile_sha256",
                "runtime_sha256",
                "gpu_uuid",
                "lock_path",
                "opportunity_lock_path",
            )
        }:
            raise RunnerError("admitted GPU controls changed before execution")
        _ensure_cuda_wheel_libraries(control)
        honor_stop = bool(getattr(args, "honor_controller_stop", False))
        honor_ordinary = bool(getattr(args, "yield_to_ordinary_gpu", False))
        if honor_stop and _controller_stop_requested(control):
            stop_observed = True
        if not stop_observed:
            with E.RetainedSource(plan["recording"]["input"]) as source:
                with _gpu_opportunity_lock(
                    control["opportunity_lock_path"], control["gpu_uuid"]
                ):
                    # Recheck after the atomic scheduler handoff.  If an ordinary
                    # batch appeared after campaign admission, leave without a
                    # model load or a failed primary child attempt.
                    if honor_ordinary and _ordinary_gpu_demand(control):
                        ordinary_yield_observed = True
                    if not ordinary_yield_observed:
                        with _gpu_lock(control["lock_path"], control["gpu_uuid"]):
                            gpu_admission = _verify_gpu_uuid(
                                control["gpu_uuid"],
                                control["profile"]["telemetry"],
                            )
                            if plan["strategy"] == "adaptive_spans":
                                decoder_context: Any = E.SequentialFFmpegSpanDecoder(
                                    source,
                                    total_samples=plan["recording"]["input"][
                                        "total_samples"
                                    ],
                                    executable=args.ffmpeg,
                                    executable_sha256=args.ffmpeg_sha256,
                                )
                            else:
                                decoder_context = None
                            try:
                                engine = _model(control, decoder_context)
                                model_load_count = 1
                                for order in pending:
                                    if honor_stop and _controller_stop_requested(control):
                                        stop_observed = True
                                        break
                                    if honor_ordinary and _ordinary_gpu_demand(control):
                                        ordinary_yield_observed = True
                                        break
                                    bundle = E.execute_work_order(
                                        order,
                                        engine,
                                        retained_source=source,
                                    )
                                    E.materialize_result_bundle(bundle)
                                    executed.append(order)
                                    if honor_stop and _controller_stop_requested(control):
                                        stop_observed = True
                                        break
                                    if honor_ordinary and _ordinary_gpu_demand(control):
                                        ordinary_yield_observed = True
                                        break
                                if decoder_context is not None and executed:
                                    decoder_context.finish(
                                        required_end_sample=executed[-1]["span"][
                                            "analysis"
                                        ]["end_sample"]
                                    )
                            finally:
                                if decoder_context is not None:
                                    decoder_context.close()
    bindings = _bindings(hashlib.sha256(plan_body).hexdigest(), orders)
    remaining = sum(row["status"] != "completed" for row in bindings["spans"])
    if remaining:
        if not stop_observed and not ordinary_yield_observed:
            raise RunnerError("run ended without all plan spans completed")
        return {
            "status": "incomplete",
            "reason": (
                "durable_controller_stop_observed_between_spans"
                if stop_observed
                else "ordinary_gpu_work_observed_between_spans"
            ),
            "plan_id": plan["plan_id"],
            "strategy": plan["strategy"],
            "completed_spans": len(orders) - remaining,
            "remaining_spans": remaining,
            "newly_completed_spans": len(executed),
            "bindings_materialized": False,
            "status_reconstructs_completed_and_pending_bindings": True,
            "model_load_count": model_load_count,
            "gpu_admission": gpu_admission,
            "persistent_audio_chunks_created": False,
        }
    _write_exclusive_or_identical(args.bindings_output, bindings)
    return {
        "status": "completed",
        "plan_id": plan["plan_id"],
        "strategy": plan["strategy"],
        "completed_spans": len(orders),
        "bindings_path": str(args.bindings_output),
        "bindings_sha256": hashlib.sha256(E.canonical_bytes(bindings)).hexdigest(),
        "model_load_count": model_load_count,
        "gpu_admission": gpu_admission,
        "persistent_audio_chunks_created": False,
    }


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--initial-prompt")
    parser.add_argument("--hotwords-json", type=Path)
    parser.add_argument("--controller-config", required=True, type=Path)
    parser.add_argument("--controller-config-sha256", required=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    status = subparsers.add_parser("status", help="Replay local results without GPU use")
    _add_common(status)
    status.set_defaults(handler=command_status)

    run = subparsers.add_parser("run-plan", help="Resume and complete a validated plan")
    _add_common(run)
    run.add_argument("--bindings-output", required=True, type=Path)
    run.add_argument("--ffmpeg", default=Path("/usr/bin/ffmpeg"), type=Path)
    run.add_argument("--ffmpeg-sha256", required=True)
    run.add_argument(
        "--honor-controller-stop",
        action="store_true",
        help="finish the current logical span, then return resumable incomplete status",
    )
    run.add_argument(
        "--yield-to-ordinary-gpu",
        action="store_true",
        help=(
            "yield resumably between spans whenever the primary GPU lane has demand"
        ),
    )
    run.set_defaults(handler=command_run)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = args.handler(args)
    except Exception as error:
        failure = {
            "status": "failed",
            "error": {"type": type(error).__name__, "message": str(error)},
        }
        sys.stdout.buffer.write(E.canonical_bytes(failure))
        return 1
    sys.stdout.buffer.write(E.canonical_bytes(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
