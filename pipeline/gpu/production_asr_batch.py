#!/usr/bin/env python3
"""Finite, restart-safe resident-model batch runner for private v3 GPU ASR.

This is model-residency batching, not neural batching.  A sealed manifest binds up
to 32 ordinary media-local v3 work orders with one exact model, runtime, GPU,
inference profile, output root, and safety policy.  One isolated process holds the
UUID lock and loads ``WhisperModel`` once, then executes the unchanged admitted
``WhisperModel.transcribe`` call sequentially.  Every item is published through the
ordinary v3 content-addressed no-replace result store and replayed before the next
item.  Existing exact results are reused on restart.

The worker has no catalogue, identity, biometric, wiki, archival, deletion, or
publication authority.  It requires an external loopback-only network namespace.
"""

from __future__ import annotations

import argparse
import fcntl
import gc
import importlib.util
import json
import os
import platform
import secrets
import shutil
import stat
import subprocess
import sys
import sysconfig
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any, Sequence


KIND = "himr_faster_whisper_gpu_batch_manifest"
COMPLETION_KIND = "himr_faster_whisper_gpu_batch_completion"
FAILURE_KIND = "himr_faster_whisper_gpu_batch_failure"
SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
MATERIALIZER = "himr-faster-whisper-gpu-resident-batch"
MAX_ITEMS = 32
MAX_TOTAL_AUDIO_MS = 12 * 60 * 60 * 1_000
MAX_BATCH_WALL_SECONDS = 24 * 60 * 60
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_WORK_ORDER_BYTES = 4 * 1024 * 1024
MAX_COMPLETION_BYTES = 4 * 1024 * 1024
MAX_PROCESS_VRAM_BYTES = 4 * 1024 * 1024 * 1024

SAFETY_POLICY = {
    "visibility": "private",
    "network_access": False,
    "publication_authority": "none",
    "catalogue_mutation_authority": "none",
    "identity_authority": "none",
    "biometric_authority": "none",
    "wiki_authority": "none",
    "archive_authority": "none",
    "deletion_authority": "none",
    "dispatch_order": "sealed_ordinal_fail_stop",
    "resume_policy": "exact_completed_v3_results_are_replayed_and_reused",
    "inference_mode": "single_resident_model_sequential_whispermodel_transcribe",
    "neural_batching": False,
}


class BatchError(RuntimeError):
    """A manifest, integrity, resource, or execution contract failed."""


class BatchRunFailure(BatchError):
    """One ordered item failed; no later item was attempted."""

    def __init__(self, document: dict[str, Any]):
        self.document = document
        failed = document.get("failed_item") or {}
        super().__init__(
            f"batch item {failed.get('ordinal', '?')} failed: "
            f"{(failed.get('error') or {}).get('message', 'unknown error')}"
        )


class MemberResultFailure(BatchError):
    """An item failed during sealing/replay, possibly after atomic publication."""

    def __init__(self, message: str, *, files_published: bool) -> None:
        self.files_published = files_published
        super().__init__(message)


class BatchNVMLSampler:
    """Fail-closed process-VRAM sampler with a freshness heartbeat."""

    def __init__(
        self, pynvml: Any, handle: Any, maximum_process_vram_bytes: int
    ) -> None:
        self.pynvml = pynvml
        self.handle = handle
        self.maximum_process_vram_bytes = maximum_process_vram_bytes
        self.stop_event = threading.Event()
        self.process_event = threading.Event()
        self.guard = threading.Lock()
        self.global_peak_bytes = 0
        self.process_peak_bytes = 0
        self.process_measurement_seen = False
        self.last_process_sample_monotonic: float | None = None
        self.sample_error: str | None = None
        self.thread = threading.Thread(
            target=self._sample,
            name="himr-production-gpu-asr-batch-nvml",
            daemon=True,
        )

    def _sample(self) -> None:
        while not self.stop_event.is_set():
            try:
                memory = self.pynvml.nvmlDeviceGetMemoryInfo(self.handle)
                processes = self.pynvml.nvmlDeviceGetComputeRunningProcesses(
                    self.handle
                )
                process_values = []
                for process in processes:
                    used = getattr(process, "usedGpuMemory", None)
                    if (
                        process.pid == os.getpid()
                        and isinstance(used, int)
                        and 0 <= used < 2**63
                    ):
                        process_values.append(used)
                now = time.monotonic()
                with self.guard:
                    self.global_peak_bytes = max(
                        self.global_peak_bytes, int(memory.used)
                    )
                    if process_values:
                        observed = max(process_values)
                        self.process_measurement_seen = True
                        self.last_process_sample_monotonic = now
                        self.process_peak_bytes = max(
                            self.process_peak_bytes, observed
                        )
                        self.process_event.set()
                        if observed > self.maximum_process_vram_bytes:
                            os._exit(125)
            except self.pynvml.NVMLError as error:
                with self.guard:
                    self.sample_error = f"{type(error).__name__}: {error}"
                self.stop_event.set()
                return
            self.stop_event.wait(0.05)

    def start(self) -> None:
        self.thread.start()

    def wait_for_process_sample(self, timeout_seconds: float) -> None:
        if not self.process_event.wait(timeout_seconds):
            self.snapshot(maximum_age_seconds=timeout_seconds)
            raise BatchError("NVML did not observe the resident model process")
        self.snapshot(maximum_age_seconds=timeout_seconds)

    def snapshot(self, *, maximum_age_seconds: float = 0.5) -> dict[str, Any]:
        with self.guard:
            error = self.sample_error
            seen = self.process_measurement_seen
            last = self.last_process_sample_monotonic
            global_peak = self.global_peak_bytes
            process_peak = self.process_peak_bytes
        if error is not None:
            raise BatchError(f"NVML sampler failed closed: {error}")
        if not self.thread.is_alive() and not self.stop_event.is_set():
            raise BatchError("NVML sampler thread stopped unexpectedly")
        if not seen or last is None:
            raise BatchError("NVML has not measured the resident model process")
        age = time.monotonic() - last
        if age > maximum_age_seconds:
            raise BatchError(
                f"NVML process measurement is stale ({age:.3f} seconds)"
            )
        if process_peak > self.maximum_process_vram_bytes:
            raise BatchError("sampled process VRAM exceeded the admitted ceiling")
        return {
            "global_peak_used_bytes": global_peak,
            "process_peak_used_bytes": process_peak,
            "process_vram_measurement_seen": True,
            "process_sample_age_seconds": age,
            "sampler_error": None,
        }

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=2)
        if self.thread.is_alive():
            raise BatchError("NVML sampler thread did not stop")
        with self.guard:
            error = self.sample_error
        if error is not None:
            raise BatchError(f"NVML sampler failed closed: {error}")


def _load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise BatchError(f"could not load required module: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SOURCE_PATH = Path(__file__).resolve()
GPU_DIR = SOURCE_PATH.parent
REPOSITORY_ROOT = GPU_DIR.parent.parent
V3_PATH = GPU_DIR / "production_asr_v3.py"
V2_PATH = GPU_DIR / "production_asr_v2.py"
V1_PATH = GPU_DIR / "production_asr.py"
WRAPPER_PATH = REPOSITORY_ROOT / "pipeline/bin/asr-faster-whisper-gpu-batch"
V3 = _load_module("himr_production_asr_v3_for_batch", V3_PATH)
BASE = V3._V1


def canonical_bytes(value: Any) -> bytes:
    return BASE.canonical_bytes(value)


def sha256_bytes(value: bytes) -> str:
    return BASE.sha256_bytes(value)


def utc_now() -> str:
    return BASE.utc_now()


def _process_elapsed_seconds() -> float:
    """Return elapsed wall time for this PID, surviving an LD_LIBRARY_PATH exec."""

    try:
        stat_body = Path("/proc/self/stat").read_text(encoding="ascii")
        remainder = stat_body[stat_body.rindex(") ") + 2 :].split()
        start_ticks = int(remainder[19])
        ticks_per_second = os.sysconf("SC_CLK_TCK")
        uptime_seconds = float(
            Path("/proc/uptime").read_text(encoding="ascii").split()[0]
        )
        elapsed = uptime_seconds - start_ticks / ticks_per_second
    except (OSError, ValueError, IndexError) as error:
        raise BatchError(f"process-start clock evidence is unavailable: {error}") from error
    if not 0 <= elapsed <= 365 * 24 * 60 * 60:
        raise BatchError("process-start clock evidence is outside its safe bound")
    return elapsed


def _remaining_batch_wall_seconds(manifest: dict[str, Any]) -> float:
    remaining = (
        manifest["limits"]["maximum_batch_wall_seconds"]
        - _process_elapsed_seconds()
    )
    if remaining <= 0:
        raise BatchError("batch wall-time limit expired before execution")
    return remaining


def ensure_cuda_wheel_libraries() -> None:
    """Re-exec this batch worker with only the pinned CUDA wheel libraries."""

    purelib = Path(sysconfig.get_paths()["purelib"])
    required = [
        purelib / "nvidia" / "cublas" / "lib",
        purelib / "nvidia" / "cudnn" / "lib",
    ]
    missing = [str(path) for path in required if not path.is_dir()]
    if missing:
        raise BatchError(
            f"pinned CUDA wheel library directories are missing: {missing}"
        )
    current = [
        part for part in os.environ.get("LD_LIBRARY_PATH", "").split(":") if part
    ]
    if all(str(path) in current for path in required):
        return
    environment = os.environ.copy()
    environment.pop("PYTHONHOME", None)
    environment.pop("PYTHONPATH", None)
    environment.update(
        {
            "LD_LIBRARY_PATH": ":".join(
                [str(path) for path in required] + current
            ),
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


def _exact(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    try:
        return BASE.require_exact_keys(value, label, keys)
    except BASE.ProductionASRError as error:
        raise BatchError(str(error)) from error


def _identifier(value: Any, label: str) -> str:
    try:
        return BASE.identifier(value, label)
    except BASE.ProductionASRError as error:
        raise BatchError(str(error)) from error


def _sha256(value: Any, label: str) -> str:
    try:
        return BASE.sha256_value(value, label)
    except BASE.ProductionASRError as error:
        raise BatchError(str(error)) from error


def _integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    try:
        return BASE.integer(value, label, minimum, maximum)
    except BASE.ProductionASRError as error:
        raise BatchError(str(error)) from error


def _finite(value: Any, label: str, minimum: float, maximum: float) -> float:
    try:
        return BASE.finite_number(value, label, minimum, maximum)
    except BASE.ProductionASRError as error:
        raise BatchError(str(error)) from error


def _utc_timestamp(value: Any, label: str) -> datetime:
    if not isinstance(value, str):
        raise BatchError(f"{label} must be a UTC timestamp string")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=UTC
        )
    except ValueError as error:
        raise BatchError(f"{label} must use YYYY-MM-DDTHH:MM:SSZ") from error
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        raise BatchError(f"{label} is not a canonical UTC timestamp")
    return parsed


def _private_root(path: Path, label: str) -> Path:
    try:
        return BASE.existing_private_directory(str(path), label, mode=0o700)
    except BASE.ProductionASRError as error:
        raise BatchError(str(error)) from error


def _stable_body(
    path: Path,
    label: str,
    maximum: int,
    *,
    mode: int | None = None,
    single_link: bool = False,
) -> bytes:
    try:
        return BASE.stable_file_bytes(
            path,
            label=label,
            maximum_bytes=maximum,
            exact_mode=mode,
            single_link=single_link,
        )
    except (BASE.ProductionASRError, OSError) as error:
        raise BatchError(str(error)) from error


def _parse(body: bytes, label: str) -> Any:
    try:
        return BASE.parse_json_bytes(body, label)
    except BASE.ProductionASRError as error:
        raise BatchError(str(error)) from error


def _file_reference(path: Path, label: str, *, executable: bool = False) -> dict[str, Any]:
    try:
        normalized = BASE.existing_regular_file(
            str(path), label, executable=executable
        )
        observed = BASE.stable_hash_file(normalized, label, maximum_bytes=MAX_MANIFEST_BYTES)
    except (BASE.ProductionASRError, OSError) as error:
        raise BatchError(str(error)) from error
    return {
        "path": str(normalized),
        "sha256": observed["sha256"],
        "byte_count": observed["byte_count"],
    }


def _verify_file_reference(value: Any, label: str, *, executable: bool = False) -> dict[str, Any]:
    item = _exact(value, label, {"path", "sha256", "byte_count"})
    expected = {
        "path": str(BASE.normalized_absolute_path(item["path"], f"{label}.path")),
        "sha256": _sha256(item["sha256"], f"{label}.sha256"),
        "byte_count": _integer(
            item["byte_count"], f"{label}.byte_count", 1, MAX_MANIFEST_BYTES
        ),
    }
    observed = _file_reference(Path(expected["path"]), label, executable=executable)
    if observed != expected:
        raise BatchError(f"{label} changed from its sealed manifest binding")
    return observed


def software_document() -> dict[str, Any]:
    return {
        "batch_worker": _file_reference(SOURCE_PATH, "batch worker source"),
        "batch_wrapper": _file_reference(
            WRAPPER_PATH, "batch worker wrapper", executable=True
        ),
        "v3_adapter": _file_reference(V3_PATH, "v3 adapter source"),
        "v2_preserved": _file_reference(V2_PATH, "preserved v2 source"),
        "v1_preserved": _file_reference(V1_PATH, "preserved v1 source"),
    }


def verify_software(value: Any) -> dict[str, Any]:
    item = _exact(
        value,
        "software",
        {
            "batch_worker",
            "batch_wrapper",
            "v3_adapter",
            "v2_preserved",
            "v1_preserved",
        },
    )
    return {
        "batch_worker": _verify_file_reference(
            item["batch_worker"], "batch worker source"
        ),
        "batch_wrapper": _verify_file_reference(
            item["batch_wrapper"], "batch worker wrapper", executable=True
        ),
        "v3_adapter": _verify_file_reference(item["v3_adapter"], "v3 adapter source"),
        "v2_preserved": _verify_file_reference(
            item["v2_preserved"], "preserved v2 source"
        ),
        "v1_preserved": _verify_file_reference(
            item["v1_preserved"], "preserved v1 source"
        ),
    }


def contract_document() -> dict[str, Any]:
    descriptor = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "maximum_items": MAX_ITEMS,
        "maximum_total_audio_ms": MAX_TOTAL_AUDIO_MS,
        "maximum_process_vram_bytes": MAX_PROCESS_VRAM_BYTES,
        "member_contract": {
            "producer": "production_asr_v3",
            "work_order_contract_sha256": V3.contract_document()["work_order"][
                "identity_sha256"
            ],
            "result_contract_sha256": V3.contract_document()["result"][
                "identity_sha256"
            ],
            "coordinates": "media_ms_null_catalog_context",
            "result_store": "ordinary_v3_atomic_no_replace_exact_replay",
        },
        "execution": {
            "gpu_lock": "one_uuid_lock_held_for_finite_batch",
            "model_loads": 1,
            "dispatch": "sequential_unchanged_whispermodel_transcribe",
            "item_failure": "fail_stop",
            "resume": "reuse_only_exact_completed_member_results",
            "network": "external_loopback_only_namespace",
        },
        "policy": SAFETY_POLICY,
    }
    return {
        "kind": "himr_faster_whisper_gpu_batch_contract",
        "schema_version": SCHEMA_VERSION,
        "descriptor": descriptor,
        "identity_sha256": sha256_bytes(canonical_bytes(descriptor)),
    }


def _path_relationship_forbidden(first: Path, second: Path) -> bool:
    return first == second or first in second.parents or second in first.parents


def _validate_roots(
    batch_root: Path, receipt_root: Path, first_work_order: dict[str, Any]
) -> tuple[Path, Path]:
    batch_root = _private_root(batch_root, "batch root")
    receipt_root = _private_root(receipt_root, "batch receipt root")
    expected_device = first_work_order["runtime"]["expected_device"]
    if batch_root.stat().st_dev != expected_device or receipt_root.stat().st_dev != expected_device:
        raise BatchError("batch and receipt roots must remain on the admitted main drive")
    output_root = Path(first_work_order["output"]["root"])
    if _path_relationship_forbidden(batch_root, receipt_root):
        raise BatchError("batch root and batch receipt root must be disjoint")
    if _path_relationship_forbidden(batch_root, output_root) or _path_relationship_forbidden(
        receipt_root, output_root
    ):
        raise BatchError("batch/receipt roots and member result root must be disjoint")
    protected = [
        Path(first_work_order["runtime"]["root"]),
        Path(first_work_order["model"]["snapshot_root"]),
    ]
    for path in protected:
        if _path_relationship_forbidden(batch_root, path) or _path_relationship_forbidden(
            receipt_root, path
        ):
            raise BatchError("batch/receipt roots may not contain runtime or model inputs")
    return batch_root, receipt_root


def _common_document(work_order: dict[str, Any]) -> dict[str, Any]:
    contracts = V3.contract_document()
    return {
        "model": work_order["model"],
        "runtime": work_order["runtime"],
        "gpu": work_order["gpu"],
        "inference": work_order["inference"],
        "inference_profile_sha256": sha256_bytes(
            canonical_bytes(BASE.inference_profile(work_order))
        ),
        "output": work_order["output"],
        "member_policy": work_order["policy"],
        "contracts": {
            "work_order_sha256": contracts["work_order"]["identity_sha256"],
            "result_sha256": contracts["result"]["identity_sha256"],
        },
    }


def _validate_member_constraints(work_orders: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if not 1 <= len(work_orders) <= MAX_ITEMS:
        raise BatchError(f"a batch requires 1 to {MAX_ITEMS} work orders")
    first = work_orders[0]
    common = _common_document(first)
    expected_adapter = {
        "path": str(V3_PATH),
        "expected_sha256": _file_reference(V3_PATH, "v3 adapter source")[
            "sha256"
        ],
    }
    if first["runtime"]["adapter"] != expected_adapter:
        raise BatchError("member runtime.adapter is not the exact v3 source")
    if first["inference"]["num_workers"] != 1:
        raise BatchError("resident batching requires inference.num_workers=1")
    if first["gpu"]["maximum_process_vram_bytes"] > MAX_PROCESS_VRAM_BYTES:
        raise BatchError("member process VRAM bound exceeds the 4 GiB batch ceiling")

    seen_jobs: set[str] = set()
    seen_work_orders: set[str] = set()
    seen_inputs: set[str] = set()
    seen_results: set[str] = set()
    total_ms = 0
    total_bytes = 0
    for ordinal, work_order in enumerate(work_orders, start=1):
        if work_order["catalog_context"] is not None:
            raise BatchError(f"member {ordinal} must have null catalog_context")
        if work_order["input"]["timeline_offset_ms"] != 0:
            raise BatchError(f"member {ordinal} must have zero timeline offset")
        try:
            input_device = Path(work_order["input"]["path"]).stat().st_dev
        except OSError as error:
            raise BatchError(f"member {ordinal} input stat failed: {error}") from error
        if input_device != work_order["runtime"]["expected_device"]:
            raise BatchError(
                f"member {ordinal} input must remain on the admitted main drive"
            )
        if _common_document(work_order) != common:
            raise BatchError(
                f"member {ordinal} differs in model/runtime/GPU/inference/output/policy"
            )
        plan = V3.result_plan(work_order)
        uniqueness = (
            (seen_jobs, work_order["job_id"], "job_id"),
            (
                seen_work_orders,
                work_order["identity_sha256"],
                "work-order identity",
            ),
            (seen_inputs, work_order["input"]["expected_sha256"], "input SHA-256"),
            (seen_results, plan["result_key"], "result key"),
        )
        for seen, value, label in uniqueness:
            if value in seen:
                raise BatchError(f"duplicate member {label}: {value}")
            seen.add(value)
        total_ms += work_order["input"]["expected_duration_ms"]
        total_bytes += work_order["input"]["expected_byte_count"]
    if total_ms > MAX_TOTAL_AUDIO_MS:
        raise BatchError("batch source duration exceeds 12 hours")
    return {
        "common": common,
        "total_audio_ms": total_ms,
        "total_audio_bytes": total_bytes,
    }


def _manifest_from_records(
    *,
    records: Sequence[dict[str, Any]],
    batch_root: Path,
    receipt_root: Path,
    maximum_batch_wall_seconds: int,
    software: dict[str, Any],
) -> dict[str, Any]:
    work_orders = [record["work_order"] for record in records]
    constraints = _validate_member_constraints(work_orders)
    batch_root, receipt_root = _validate_roots(batch_root, receipt_root, work_orders[0])
    wall = _integer(
        maximum_batch_wall_seconds,
        "maximum_batch_wall_seconds",
        1,
        MAX_BATCH_WALL_SECONDS,
    )
    entries: list[dict[str, Any]] = []
    for ordinal, record in enumerate(records, start=1):
        work_order = record["work_order"]
        body = record["body"]
        source_reference = _normalize_source_work_order_reference(
            record["source_reference"], ordinal
        )
        if (
            source_reference["sha256"] != sha256_bytes(body)
            or source_reference["byte_count"] != len(body)
        ):
            raise BatchError(
                f"source work order reference {ordinal} does not bind its canonical bytes"
            )
        plan = V3.result_plan(work_order)
        entries.append(
            {
                "ordinal": ordinal,
                "job_id": work_order["job_id"],
                "work_order_id": work_order["work_order_id"],
                "work_order_identity_sha256": work_order["identity_sha256"],
                "source_work_order": source_reference,
                "batch_work_order_path": f"work-orders/{ordinal:06d}.json",
                "work_order_sha256": sha256_bytes(body),
                "work_order_byte_count": len(body),
                "input_sha256": work_order["input"]["expected_sha256"],
                "input_duration_ms": work_order["input"]["expected_duration_ms"],
                "result_key": plan["result_key"],
                "result_path": plan["result_path"],
                "recipe_sha256": plan["recipe_sha256"],
            }
        )
    identity_core = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "materializer": MATERIALIZER,
        "contract": contract_document(),
        "software": software,
        "common": constraints["common"],
        "limits": {
            "maximum_items": MAX_ITEMS,
            "maximum_total_audio_ms": MAX_TOTAL_AUDIO_MS,
            "maximum_batch_wall_seconds": wall,
            "maximum_process_vram_bytes": MAX_PROCESS_VRAM_BYTES,
            "model_load_count": 1,
            "inference_concurrency": 1,
        },
        "totals": {
            "item_count": len(entries),
            "audio_duration_ms": constraints["total_audio_ms"],
            "audio_byte_count": constraints["total_audio_bytes"],
        },
        "output": {
            "batch_root": str(batch_root),
            "receipt_root": str(receipt_root),
        },
        "items": entries,
        "safety": SAFETY_POLICY,
    }
    identity = sha256_bytes(canonical_bytes(identity_core))
    batch_id = f"gpuasrbatch_{identity[:32]}"
    return {
        **identity_core,
        "identity_sha256": identity,
        "batch_id": batch_id,
        "batch_relative_path": f"batches/{batch_id}",
    }


def _normalize_source_work_order_reference(
    value: Any, ordinal: int
) -> dict[str, Any]:
    item = _exact(
        value,
        f"source work order reference {ordinal}",
        {"path", "sha256", "byte_count"},
    )
    try:
        path = BASE.normalized_absolute_path(
            item["path"], f"source work order reference {ordinal}.path"
        )
    except BASE.ProductionASRError as error:
        raise BatchError(str(error)) from error
    return {
        "path": str(path),
        "sha256": _sha256(
            item["sha256"], f"source work order reference {ordinal}.sha256"
        ),
        "byte_count": _integer(
            item["byte_count"],
            f"source work order reference {ordinal}.byte_count",
            1,
            MAX_WORK_ORDER_BYTES,
        ),
    }


def _load_source_record(path: Path) -> dict[str, Any]:
    try:
        work_order, reference = BASE.load_work_order(str(path))
    except (BASE.ProductionASRError, OSError) as error:
        raise BatchError(str(error)) from error
    body = _stable_body(
        Path(reference["path"]),
        "source v3 work order",
        MAX_WORK_ORDER_BYTES,
        mode=0o400,
        single_link=True,
    )
    if work_order["schema_version"] != 3 or work_order["implementation_version"] != "0.3.0":
        raise BatchError("batch members must be exact production v3 work orders")
    return {
        "work_order": work_order,
        "body": body,
        "source_reference": {
            "path": reference["path"],
            "sha256": reference["sha256"],
            "byte_count": reference["byte_count"],
        },
    }


def _ensure_private_collection(parent: Path, name: str) -> Path:
    try:
        return BASE.ensure_private_subdirectory(parent, name)
    except (BASE.ProductionASRError, OSError) as error:
        raise BatchError(str(error)) from error


def _safe_remove_staging(path: Path) -> None:
    if not path.exists():
        return
    observed = path.lstat()
    if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
        raise BatchError("refusing to clean unsafe batch staging path")
    os.chmod(path, 0o700)
    for child in sorted(path.rglob("*"), reverse=True):
        observed_child = child.lstat()
        if stat.S_ISLNK(observed_child.st_mode):
            raise BatchError("refusing to clean symlinked batch staging entry")
        os.chmod(child, 0o700 if stat.S_ISDIR(observed_child.st_mode) else 0o600)
    shutil.rmtree(path)


def _materialization_lock(batch_root: Path) -> int:
    path = batch_root / ".gpu-asr-batch-materialize.lock"
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    os.fchmod(descriptor, 0o600)
    opened = os.fstat(descriptor)
    current = path.lstat()
    if (
        not stat.S_ISREG(opened.st_mode)
        or opened.st_uid != os.getuid()
        or opened.st_nlink != 1
        or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
    ):
        os.close(descriptor)
        raise BatchError("batch materialization lock is unsafe")
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    return descriptor


def materialize_batch(
    *,
    work_order_paths: Sequence[Path],
    batch_root: Path,
    receipt_root: Path,
    maximum_batch_wall_seconds: int,
) -> tuple[dict[str, Any], Path]:
    if not 1 <= len(work_order_paths) <= MAX_ITEMS:
        raise BatchError(f"a batch requires 1 to {MAX_ITEMS} work orders")
    records = [_load_source_record(path) for path in work_order_paths]
    manifest = _manifest_from_records(
        records=records,
        batch_root=batch_root,
        receipt_root=receipt_root,
        maximum_batch_wall_seconds=maximum_batch_wall_seconds,
        software=software_document(),
    )
    batch_root = Path(manifest["output"]["batch_root"])
    batches = _ensure_private_collection(batch_root, "batches")
    final = batches / manifest["batch_id"]
    descriptor = _materialization_lock(batch_root)
    try:
        manifest_path = final / "manifest.json"
        if final.exists() or final.is_symlink():
            observed, _ = load_manifest(manifest_path, replay_runtime=False)
            if observed != manifest:
                raise BatchError("existing deterministic batch differs from reconstruction")
            return observed, manifest_path
        staging = batches / f".{manifest['batch_id']}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
        try:
            staging.mkdir(mode=0o700)
            orders_dir = staging / "work-orders"
            orders_dir.mkdir(mode=0o700)
            for ordinal, record in enumerate(records, start=1):
                BASE.write_new_private_file(
                    orders_dir / f"{ordinal:06d}.json", record["body"]
                )
            BASE.write_new_private_file(staging / "manifest.json", canonical_bytes(manifest))
            os.chmod(orders_dir, 0o500)
            BASE.sync_directory(orders_dir)
            os.chmod(staging, 0o500)
            BASE.sync_directory(staging)
            BASE.publish_directory_no_replace(staging, final)
        finally:
            _safe_remove_staging(staging)
        observed, _ = load_manifest(manifest_path, replay_runtime=False)
        if observed != manifest:
            raise BatchError("published batch failed exact reconstruction")
        return observed, manifest_path
    finally:
        os.close(descriptor)


MANIFEST_KEYS = {
    "kind",
    "schema_version",
    "implementation_version",
    "materializer",
    "contract",
    "software",
    "common",
    "limits",
    "totals",
    "output",
    "items",
    "safety",
    "identity_sha256",
    "batch_id",
    "batch_relative_path",
}


def _verify_manifest_layout(path: Path, manifest: dict[str, Any]) -> Path:
    batch_dir = path.parent
    orders_dir = batch_dir / "work-orders"
    for directory, label in ((batch_dir, "batch directory"), (orders_dir, "work-order directory")):
        observed = directory.lstat()
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISDIR(observed.st_mode)
            or stat.S_IMODE(observed.st_mode) != 0o500
            or observed.st_uid != os.getuid()
        ):
            raise BatchError(f"sealed {label} must be owner mode 0500")
    if {child.name for child in batch_dir.iterdir()} != {"manifest.json", "work-orders"}:
        raise BatchError("sealed batch directory has missing or unexpected entries")
    if batch_dir.name != manifest["batch_id"] or batch_dir.parent.name != "batches":
        raise BatchError("manifest is not at its deterministic batch path")
    expected_root = Path(manifest["output"]["batch_root"])
    if batch_dir.parent.parent != expected_root:
        raise BatchError("manifest path differs from output.batch_root")
    return orders_dir


def _pretty_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            indent=2,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _replay_frozen_runtime_binding(
    work_order: dict[str, Any], *, require_current: bool
) -> dict[str, Any]:
    """Replay sealed runtime evidence without importing or querying CUDA/NVML."""

    runtime = work_order["runtime"]
    try:
        observations = {
            name: BASE.verify_reference(runtime[name], f"runtime {name}")
            for name in (
                "python",
                "pyproject",
                "lock",
                "runtime_manifest",
                "ffprobe",
                "adapter",
            )
        }
        receipt_ref = runtime["runtime_manifest"]
        receipt_path = Path(receipt_ref["path"])
        body = BASE.stable_file_bytes(
            receipt_path,
            label="batch runtime admission receipt",
            maximum_bytes=BASE.MAX_JSON_INPUT_BYTES,
            exact_mode=0o400,
            single_link=True,
        )
        if sha256_bytes(body) != receipt_ref["expected_sha256"]:
            raise BatchError("batch runtime receipt hash changed during replay")
        receipt = _parse(body, "batch runtime admission receipt")
        receipt = _exact(
            receipt,
            "batch runtime admission receipt",
            {
                "kind",
                "schema_version",
                "implementation_version",
                "status",
                "admitted_at",
                "receipt_path",
                "configuration",
                "evidence",
                "policy",
                "identity_sha256",
                "receipt_id",
            },
        )
        if body != _pretty_bytes(receipt):
            raise BatchError("batch runtime receipt is not canonical pretty JSON")
        admission_module = BASE.load_local_gpu_module(
            "himr_batch_frozen_runtime_admission", "admit_runtime.py"
        )
        if (
            receipt["kind"] != admission_module.KIND
            or receipt["schema_version"] != admission_module.SCHEMA_VERSION
            or receipt["implementation_version"]
            != admission_module.IMPLEMENTATION_VERSION
            or receipt["status"] != "admitted"
            or receipt["receipt_path"] != str(receipt_path)
        ):
            raise BatchError("batch runtime receipt header/path is invalid")
        semantic = {
            key: value
            for key, value in receipt.items()
            if key not in {"identity_sha256", "receipt_id"}
        }
        identity = sha256_bytes(canonical_bytes(semantic))
        if (
            receipt["identity_sha256"] != identity
            or receipt["receipt_id"] != f"gpurtadmit_{identity[:32]}"
        ):
            raise BatchError("batch runtime receipt semantic identity is invalid")
        configuration = admission_module.validate_config_shape(
            receipt["configuration"]
        )
        evidence = receipt["evidence"]
        if not isinstance(evidence, dict):
            raise BatchError("batch runtime receipt evidence must be an object")
        expected_device = runtime["expected_device"]
        if (
            receipt_path.stat().st_dev != expected_device
            or configuration["expected_main_drive_device"] != expected_device
        ):
            raise BatchError("batch runtime receipt left the admitted main drive")
        expected_bindings = {
            "runtime_root": runtime["root"],
            "python_path": runtime["python"]["path"],
            "pyproject_path": runtime["pyproject"]["path"],
            "pyproject_sha256": runtime["pyproject"]["expected_sha256"],
            "lock_path": runtime["lock"]["path"],
            "lock_sha256": runtime["lock"]["expected_sha256"],
            "gpu_uuid": work_order["gpu"]["expected_uuid"],
            "device_index": work_order["gpu"]["device_index"],
            "compute_type": work_order["gpu"]["compute_type"],
            "scheduler_lock_path": work_order["gpu"]["lock_path"],
        }
        observed_bindings = {
            "runtime_root": configuration["runtime_root"],
            "python_path": configuration["python_executable"]["path"],
            "pyproject_path": configuration["pyproject"]["path"],
            "pyproject_sha256": configuration["pyproject"]["expected_sha256"],
            "lock_path": configuration["lock"]["path"],
            "lock_sha256": configuration["lock"]["expected_sha256"],
            "gpu_uuid": evidence["hardware"]["uuid"],
            "device_index": evidence["hardware"]["device_index"],
            "compute_type": evidence["hardware"]["selected_compute_type"],
            "scheduler_lock_path": evidence["scheduler_lock"]["lock_path"],
        }
        if observed_bindings != expected_bindings:
            raise BatchError("frozen runtime evidence differs from the batch profile")
        if (
            configuration["hardware"]["expected_gpu_uuid"]
            != work_order["gpu"]["expected_uuid"]
            or configuration["hardware"]["device_index"]
            != work_order["gpu"]["device_index"]
            or configuration["hardware"]["expected_compute_type"]
            != work_order["gpu"]["compute_type"]
        ):
            raise BatchError("runtime configuration GPU binding is inconsistent")

        admitted_profile = evidence["benchmark"]["profile"]
        admitted_thresholds = evidence["benchmark"]["thresholds"]
        contracts = V3.contract_document()
        inference = BASE.inference_profile(work_order)
        expected_profile = {
            "model_identity_sha256": work_order["model"]["identity_sha256"],
            "work_order_contract_sha256": contracts["work_order"][
                "identity_sha256"
            ],
            "result_contract_sha256": contracts["result"]["identity_sha256"],
            "inference": inference,
            "inference_profile_sha256": sha256_bytes(canonical_bytes(inference)),
        }
        if {key: admitted_profile[key] for key in expected_profile} != expected_profile:
            raise BatchError("frozen benchmark profile differs from the v3 member")
        if (
            evidence["benchmark"].get("accepted") is not True
            or evidence["scheduler_lock"].get("accepted") is not True
            or work_order["gpu"]["maximum_process_vram_bytes"]
            > admitted_thresholds["maximum_peak_process_vram_bytes"]
            or work_order["gpu"]["minimum_free_vram_bytes"]
            < admitted_thresholds["minimum_vram_reserve_bytes"]
        ):
            raise BatchError("frozen benchmark/scheduler resource policy is not admitted")

        runtime_summary, _ = admission_module.stable_tree_snapshot(
            Path(runtime["root"]), "batch runtime tree", expected_device
        )
        if (
            runtime_summary["tree_sha256"]
            != configuration["expected_runtime_tree_sha256"]
            or runtime_summary != evidence["runtime"]["tree"]
        ):
            raise BatchError("batch runtime tree differs from frozen admission")

        configuration_sources = {
            (item["path"], item["expected_sha256"])
            for item in configuration["sources"]
        }
        evidence_sources = {
            (item["requested_path"], item["sha256"])
            for item in evidence["bindings"]["sources"]
        }
        if configuration_sources != evidence_sources:
            raise BatchError("runtime source configuration/evidence differs")
        configuration_executables = {
            (item["path"], item["expected_sha256"])
            for item in configuration["executables"]
        }
        evidence_executables = {
            (item["requested_path"], item["sha256"])
            for item in evidence["bindings"]["executables"]
        }
        if configuration_executables != evidence_executables:
            raise BatchError("runtime executable configuration/evidence differs")
        packages: dict[str, str] = {}
        if require_current:
            if Path(sys.executable).resolve() != Path(runtime["python"]["path"]):
                raise BatchError("current Python differs from the admitted runtime")
            if sys.version_info[:3] != (3, 12, 14):
                raise BatchError("production batch runtime requires CPython 3.12.14")
            for name, expected in runtime["packages"].items():
                observed = BASE.package_version(name)
                if observed != expected:
                    raise BatchError(
                        f"runtime package {name} mismatch: expected {expected}, "
                        f"observed {observed}"
                    )
                packages[name] = observed
    except (BatchError, BASE.ProductionASRError):
        raise
    except Exception as error:
        raise BatchError(f"frozen batch runtime replay failed: {error}") from error
    return {
        "root": runtime["root"],
        "expected_device": runtime["expected_device"],
        "files": observations,
        "admission": {
            "receipt_id": receipt["receipt_id"],
            "identity_sha256": receipt["identity_sha256"],
            "benchmark_accepted": evidence["benchmark"]["accepted"],
            "benchmark_id": evidence["benchmark"]["benchmark_id"],
            "benchmark_profile_name": admitted_profile["name"],
            "benchmark_inference_profile_sha256": admitted_profile[
                "inference_profile_sha256"
            ],
            "scheduler_lock_accepted": evidence["scheduler_lock"]["accepted"],
            "wheelhouse_tree_sha256": evidence["wheelhouse"]["tree_sha256"],
            "runtime_tree_sha256": evidence["runtime"]["tree"]["tree_sha256"],
        },
        "packages": packages or dict(runtime["packages"]),
        "python_version": platform.python_version(),
        "offline_policy": runtime["offline_policy"],
    }


def _runtime_source_binding(
    work_order: dict[str, Any],
    software: dict[str, Any],
    *,
    require_current: bool,
    live_hardware: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        runtime = (
            BASE.replay_runtime_binding(work_order, require_current=require_current)
            if live_hardware
            else _replay_frozen_runtime_binding(
                work_order, require_current=require_current
            )
        )
        receipt_ref = work_order["runtime"]["runtime_manifest"]
        body = BASE.stable_file_bytes(
            Path(receipt_ref["path"]),
            label="batch runtime admission receipt",
            maximum_bytes=BASE.MAX_JSON_INPUT_BYTES,
            exact_mode=0o400,
            single_link=True,
        )
        if sha256_bytes(body) != receipt_ref["expected_sha256"]:
            raise BatchError("batch runtime receipt hash changed after replay")
        receipt = json.loads(body)
        sources = {
            item["requested_path"]: item["sha256"]
            for item in receipt["evidence"]["bindings"]["sources"]
        }
        executables = {
            item["requested_path"]: item["sha256"]
            for item in receipt["evidence"]["bindings"]["executables"]
        }
    except (BASE.ProductionASRError, OSError, KeyError, TypeError, ValueError) as error:
        raise BatchError(f"batch runtime replay failed: {error}") from error
    for name, reference in software.items():
        bindings = executables if name == "batch_wrapper" else sources
        if bindings.get(reference["path"]) != reference["sha256"]:
            raise BatchError(
                f"runtime admission does not bind exact batch software component {name}"
            )
    return runtime, {
        "receipt_id": runtime["admission"]["receipt_id"],
        "identity_sha256": runtime["admission"]["identity_sha256"],
        "source_binding_count": len(sources) + len(executables),
        "all_batch_sources_bound": True,
    }


def replay_common_bindings(
    manifest: dict[str, Any],
    members: Sequence[dict[str, Any]],
    *,
    require_current: bool,
    live_hardware: bool,
) -> dict[str, Any]:
    first = members[0]["work_order"]
    runtime, source_admission = _runtime_source_binding(
        first,
        manifest["software"],
        require_current=require_current,
        live_hardware=live_hardware,
    )
    try:
        model = BASE.replay_model_binding(first)
    except (BASE.ProductionASRError, OSError) as error:
        raise BatchError(f"batch model replay failed: {error}") from error
    return {
        "runtime": runtime,
        "model": model,
        "batch_source_admission": source_admission,
    }


def load_manifest(
    path: Path, *, replay_runtime: bool
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    try:
        normalized = BASE.existing_regular_file(
            str(path), "batch manifest", exact_mode=0o400, single_link=True
        )
    except BASE.ProductionASRError as error:
        raise BatchError(str(error)) from error
    body = _stable_body(
        normalized,
        "batch manifest",
        MAX_MANIFEST_BYTES,
        mode=0o400,
        single_link=True,
    )
    supplied = _exact(_parse(body, "batch manifest"), "batch manifest", MANIFEST_KEYS)
    if body != canonical_bytes(supplied):
        raise BatchError("batch manifest is not canonical JSON")
    if (
        supplied["kind"] != KIND
        or supplied["schema_version"] != SCHEMA_VERSION
        or supplied["implementation_version"] != IMPLEMENTATION_VERSION
        or supplied["materializer"] != MATERIALIZER
        or supplied["safety"] != SAFETY_POLICY
        or supplied["contract"] != contract_document()
    ):
        raise BatchError("batch manifest contract or policy is unsupported")
    identity_core = {
        key: value
        for key, value in supplied.items()
        if key not in {"identity_sha256", "batch_id", "batch_relative_path"}
    }
    identity = sha256_bytes(canonical_bytes(identity_core))
    if (
        supplied["identity_sha256"] != identity
        or supplied["batch_id"] != f"gpuasrbatch_{identity[:32]}"
        or supplied["batch_relative_path"] != f"batches/gpuasrbatch_{identity[:32]}"
    ):
        raise BatchError("batch semantic identity or deterministic path is invalid")
    software = verify_software(supplied["software"])
    orders_dir = _verify_manifest_layout(normalized, supplied)
    items = supplied["items"]
    if not isinstance(items, list) or not 1 <= len(items) <= MAX_ITEMS:
        raise BatchError("batch item array is outside the supported bound")
    expected_names = {f"{ordinal:06d}.json" for ordinal in range(1, len(items) + 1)}
    if {child.name for child in orders_dir.iterdir()} != expected_names:
        raise BatchError("sealed batch work-order directory differs from the manifest")

    records: list[dict[str, Any]] = []
    members: list[dict[str, Any]] = []
    for ordinal, entry in enumerate(items, start=1):
        if not isinstance(entry, dict) or entry.get("ordinal") != ordinal:
            raise BatchError("batch item ordinals are not canonical")
        expected_relative = f"work-orders/{ordinal:06d}.json"
        if entry.get("batch_work_order_path") != expected_relative:
            raise BatchError("batch work-order relative path is inconsistent")
        order_path = orders_dir / f"{ordinal:06d}.json"
        try:
            work_order, work_order_file = BASE.load_work_order(str(order_path))
        except (BASE.ProductionASRError, OSError) as error:
            raise BatchError(f"sealed member {ordinal} is invalid: {error}") from error
        work_order_body = _stable_body(
            order_path,
            f"sealed member {ordinal}",
            MAX_WORK_ORDER_BYTES,
            mode=0o400,
            single_link=True,
        )
        records.append(
            {
                "work_order": work_order,
                "body": work_order_body,
                "source_reference": entry.get("source_work_order"),
            }
        )
        members.append(
            {
                "entry": entry,
                "work_order": work_order,
                "work_order_file": work_order_file,
            }
        )
    limits = supplied.get("limits")
    if not isinstance(limits, dict):
        raise BatchError("batch limits must be an object")
    rebuilt = _manifest_from_records(
        records=records,
        batch_root=Path(supplied["output"]["batch_root"]),
        receipt_root=Path(supplied["output"]["receipt_root"]),
        maximum_batch_wall_seconds=limits.get("maximum_batch_wall_seconds"),
        software=software,
    )
    if rebuilt != supplied:
        raise BatchError("batch manifest failed deterministic member reconstruction")
    if replay_runtime:
        replay_common_bindings(
            supplied, members, require_current=False, live_hardware=False
        )
    return supplied, members


def _member_states(members: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    states: list[dict[str, Any]] = []
    for member in members:
        work_order = member["work_order"]
        plan = V3.result_plan(work_order)
        try:
            state, result = BASE.completed_or_pending(work_order, plan)
        except (BASE.ProductionASRError, OSError) as error:
            raise BatchError(
                f"member {member['entry']['ordinal']} result state is invalid: {error}"
            ) from error
        states.append(
            {
                "ordinal": member["entry"]["ordinal"],
                "job_id": work_order["job_id"],
                "state": state,
                "result_key": plan["result_key"],
                "result_path": plan["result_path"],
                "result_identity_sha256": None
                if result is None
                else result["identity_sha256"],
            }
        )
    return states


def _input_preflight(work_order: dict[str, Any]) -> dict[str, Any]:
    try:
        observation = BASE.stable_hash_file(
            Path(work_order["input"]["path"]),
            "batch member input",
            maximum_bytes=work_order["inference"]["max_audio_bytes"],
        )
        if (
            observation["sha256"] != work_order["input"]["expected_sha256"]
            or observation["byte_count"] != work_order["input"]["expected_byte_count"]
            or observation["mode"] != int(work_order["input"]["sealed_mode"], 8)
            or observation["link_count"] != 1
        ):
            raise BatchError("batch member input hash/size/mode/link binding failed")
        probe, command = BASE.probe_audio(work_order)
    except (BASE.ProductionASRError, OSError, subprocess.SubprocessError) as error:
        if isinstance(error, BatchError):
            raise
        raise BatchError(f"batch member input preflight failed: {error}") from error
    return {"input": observation | {"probe": probe}, "commands": [command]}


def dry_run_document(
    manifest_path: Path,
) -> dict[str, Any]:
    manifest, members = load_manifest(manifest_path, replay_runtime=False)
    common = replay_common_bindings(
        manifest, members, require_current=False, live_hardware=False
    )
    states = _member_states(members)
    inputs: list[dict[str, Any]] = []
    for member, state in zip(members, states, strict=True):
        inputs.append(
            {
                "ordinal": state["ordinal"],
                "state": state["state"],
                "preflight": None
                if state["state"] == "completed"
                else _input_preflight(member["work_order"]),
            }
        )
    return {
        "kind": "himr_faster_whisper_gpu_batch_dry_run",
        "schema_version": SCHEMA_VERSION,
        "status": "completed" if all(item["state"] == "completed" for item in states) else "planned",
        "dry_run": True,
        "manifest": {
            "path": str(manifest_path),
            "sha256": sha256_bytes(
                _stable_body(
                    manifest_path,
                    "batch manifest",
                    MAX_MANIFEST_BYTES,
                    mode=0o400,
                    single_link=True,
                )
            ),
            "identity_sha256": manifest["identity_sha256"],
            "batch_id": manifest["batch_id"],
        },
        "common_bindings": common,
        "items": states,
        "input_preflights": inputs,
        "gpu": {
            "queried": False,
            "lock_path": manifest["common"]["gpu"]["lock_path"],
            "lock_state": BASE.lock_state(
                Path(manifest["common"]["gpu"]["lock_path"])
            ),
        },
        "inference_executed": False,
        "files_written": False,
        "safety": SAFETY_POLICY,
    }


def _result_reference(member: dict[str, Any], result: dict[str, Any], disposition: str) -> dict[str, Any]:
    result_path = Path(result["result_path"])
    body = _stable_body(
        result_path,
        "completed batch member result",
        member["work_order"]["inference"]["max_result_bytes"],
        mode=0o400,
        single_link=True,
    )
    return {
        "ordinal": member["entry"]["ordinal"],
        "job_id": member["work_order"]["job_id"],
        "disposition": disposition,
        "result_key": result["result_key"],
        "result_path": str(result_path),
        "result_sha256": sha256_bytes(body),
        "result_byte_count": len(body),
        "result_identity_sha256": result["identity_sha256"],
        "result_id": result["result_id"],
        "processing_run_id": result["processing_run"]["processing_run_id"],
    }


def _completion_path(manifest: dict[str, Any]) -> Path:
    return (
        Path(manifest["output"]["receipt_root"])
        / "batches"
        / manifest["batch_id"]
        / "completion"
        / "receipt.json"
    )


COMPLETION_KEYS = {
    "kind",
    "schema_version",
    "implementation_version",
    "status",
    "batch_id",
    "manifest",
    "execution",
    "common_bindings",
    "hardware",
    "results",
    "safety",
    "identity_sha256",
    "completion_id",
}
COMPLETION_MANIFEST_KEYS = {
    "path",
    "sha256",
    "byte_count",
    "identity_sha256",
}
COMPLETION_EXECUTION_KEYS = {
    "batch_run_id",
    "started_at",
    "completed_at",
    "duration_ms",
    "model_loaded",
    "model_load_count",
    "model_load_seconds",
    "inference_item_count",
    "reused_item_count",
    "inference_seconds",
    "resume_completion_only",
}
COMPLETION_COMMON_KEYS = {"runtime", "model", "batch_source_admission"}
COMPLETION_RESULT_KEYS = {
    "ordinal",
    "job_id",
    "disposition",
    "result_key",
    "result_path",
    "result_sha256",
    "result_byte_count",
    "result_identity_sha256",
    "result_id",
    "processing_run_id",
}
COMPLETION_HARDWARE_KEYS = {
    "device_index",
    "name",
    "uuid",
    "driver_version",
    "cuda_driver_version",
    "compute_capability",
    "ctranslate2_cuda_device_count",
    "ctranslate2_supported_compute_types",
    "memory_before",
    "memory_after",
    "global_peak_used_bytes",
    "process_peak_used_bytes",
    "process_vram_measurement_seen",
    "process_sample_age_seconds",
    "sampler_error",
}


def _validate_memory_evidence(value: Any, label: str) -> dict[str, Any]:
    memory = _exact(value, label, {"total_bytes", "free_bytes", "used_bytes"})
    total = _integer(memory["total_bytes"], f"{label}.total_bytes", 1, 2**63 - 1)
    free = _integer(memory["free_bytes"], f"{label}.free_bytes", 0, total)
    used = _integer(memory["used_bytes"], f"{label}.used_bytes", 0, total)
    if free + used != total:
        raise BatchError(f"{label} free/used bytes do not sum to total bytes")
    return memory


def _validate_completion_execution(
    execution_value: Any,
    hardware_value: Any,
    manifest: dict[str, Any],
    members: Sequence[dict[str, Any]],
) -> tuple[dict[str, Any], int, int]:
    execution = _exact(
        execution_value, "batch completion execution", COMPLETION_EXECUTION_KEYS
    )
    _identifier(execution["batch_run_id"], "completion batch_run_id")
    started = _utc_timestamp(execution["started_at"], "completion started_at")
    completed = _utc_timestamp(execution["completed_at"], "completion completed_at")
    if completed < started:
        raise BatchError("completion completed_at precedes started_at")
    maximum_wall = manifest["limits"]["maximum_batch_wall_seconds"]
    duration_ms = _integer(
        execution["duration_ms"],
        "completion duration_ms",
        0,
        maximum_wall * 1_000,
    )
    model_load_count = _integer(
        execution["model_load_count"], "completion model_load_count", 0, 1
    )
    model_load_seconds = _finite(
        execution["model_load_seconds"],
        "completion model_load_seconds",
        0,
        maximum_wall,
    )
    inference_seconds = _finite(
        execution["inference_seconds"],
        "completion inference_seconds",
        0,
        maximum_wall,
    )
    inferred_count = _integer(
        execution["inference_item_count"],
        "completion inference item count",
        0,
        len(members),
    )
    reused_count = _integer(
        execution["reused_item_count"],
        "completion reused item count",
        0,
        len(members),
    )
    if inferred_count + reused_count != len(members):
        raise BatchError("batch completion execution counts differ from the manifest")
    if execution["resume_completion_only"] is True:
        if (
            execution["model_loaded"] is not False
            or model_load_count != 0
            or model_load_seconds != 0
            or inference_seconds != 0
            or inferred_count != 0
            or duration_ms != 0
            or hardware_value is not None
        ):
            raise BatchError("completion-only execution evidence is inconsistent")
        return execution, inferred_count, reused_count
    if execution["resume_completion_only"] is not False:
        raise BatchError("completion resume flag must be boolean")

    hardware = _exact(
        hardware_value, "batch completion hardware", COMPLETION_HARDWARE_KEYS
    )
    gpu = manifest["common"]["gpu"]
    device_index = _integer(
        hardware["device_index"], "completion hardware device_index", 0, 63
    )
    _integer(
        hardware["cuda_driver_version"],
        "completion hardware cuda_driver_version",
        0,
        2**63 - 1,
    )
    device_count = _integer(
        hardware["ctranslate2_cuda_device_count"],
        "completion hardware CTranslate2 device count",
        1,
        64,
    )
    before = _validate_memory_evidence(
        hardware["memory_before"], "completion hardware memory_before"
    )
    after = _validate_memory_evidence(
        hardware["memory_after"], "completion hardware memory_after"
    )
    global_peak = _integer(
        hardware["global_peak_used_bytes"],
        "completion hardware global peak VRAM",
        0,
        before["total_bytes"],
    )
    process_peak = _integer(
        hardware["process_peak_used_bytes"],
        "completion hardware process peak VRAM",
        0,
        before["total_bytes"],
    )
    sample_age = _finite(
        hardware["process_sample_age_seconds"],
        "completion hardware process sample age",
        0,
        0.5,
    )
    capability = hardware["compute_capability"]
    supported = hardware["ctranslate2_supported_compute_types"]
    if (
        execution["model_loaded"] is not True
        or model_load_count != 1
        or model_load_seconds > execution["duration_ms"] / 1_000
        or inference_seconds > execution["duration_ms"] / 1_000
        or not isinstance(capability, list)
        or len(capability) != 2
        or any(
            isinstance(item, bool) or not isinstance(item, int) or item < 0
            for item in capability
        )
        or not isinstance(supported, list)
        or not supported
        or any(not isinstance(item, str) or not item for item in supported)
        or gpu["compute_type"] not in supported
        or hardware["uuid"] != gpu["expected_uuid"]
        or device_index != gpu["device_index"]
        or device_index >= device_count
        or hardware["process_vram_measurement_seen"] is not True
        or hardware["sampler_error"] is not None
        or process_peak > gpu["maximum_process_vram_bytes"]
        or global_peak > before["total_bytes"]
        or after["total_bytes"] != before["total_bytes"]
        or sample_age > 0.5
    ):
        raise BatchError("batch completion model/hardware evidence is inconsistent")
    return execution, inferred_count, reused_count


def validate_completion(
    manifest: dict[str, Any], members: Sequence[dict[str, Any]]
) -> dict[str, Any] | None:
    path = _completion_path(manifest)
    if not path.exists() and not path.is_symlink():
        return None
    if not path.is_file():
        raise BatchError("batch completion path exists without a receipt")
    receipt_root = Path(manifest["output"]["receipt_root"])
    expected_path = (
        receipt_root
        / "batches"
        / manifest["batch_id"]
        / "completion"
        / "receipt.json"
    )
    if path != expected_path:
        raise BatchError("batch completion escaped its deterministic receipt path")
    ancestry = (
        (receipt_root, 0o700, "batch receipt root"),
        (path.parents[2], 0o700, "batch receipt collection"),
        (path.parents[1], 0o700, "batch receipt member directory"),
        (path.parent, 0o500, "batch completion directory"),
    )
    for directory, mode, label in ancestry:
        observed = directory.lstat()
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISDIR(observed.st_mode)
            or stat.S_IMODE(observed.st_mode) != mode
            or observed.st_uid != os.getuid()
            or observed.st_dev != members[0]["work_order"]["runtime"][
                "expected_device"
            ]
        ):
            raise BatchError(f"{label} ownership/mode/device binding is invalid")
    if path.parents[2].name != "batches" or path.parents[1].name != manifest["batch_id"]:
        raise BatchError("batch completion ancestry is inconsistent")
    if {child.name for child in path.parents[1].iterdir()} != {"completion"}:
        raise BatchError("batch completion member directory has unexpected entries")
    directory = path.parent
    observed_dir = directory.lstat()
    if (
        stat.S_ISLNK(observed_dir.st_mode)
        or not stat.S_ISDIR(observed_dir.st_mode)
        or stat.S_IMODE(observed_dir.st_mode) != 0o500
        or observed_dir.st_uid != os.getuid()
        or {child.name for child in directory.iterdir()} != {"receipt.json"}
    ):
        raise BatchError("batch completion directory is not sealed exact mode 0500")
    body = _stable_body(
        path,
        "batch completion receipt",
        MAX_COMPLETION_BYTES,
        mode=0o400,
        single_link=True,
    )
    receipt = _exact(
        _parse(body, "batch completion receipt"),
        "batch completion receipt",
        COMPLETION_KEYS,
    )
    if body != canonical_bytes(receipt):
        raise BatchError("batch completion receipt is not canonical JSON")
    core = {
        key: value
        for key, value in receipt.items()
        if key not in {"identity_sha256", "completion_id"}
    }
    identity = sha256_bytes(canonical_bytes(core))
    if (
        receipt["identity_sha256"] != identity
        or receipt["completion_id"] != f"gpuasrbatchdone_{identity[:32]}"
        or receipt["kind"] != COMPLETION_KIND
        or receipt["schema_version"] != SCHEMA_VERSION
        or receipt["implementation_version"] != IMPLEMENTATION_VERSION
        or receipt["status"] != "completed"
        or receipt["batch_id"] != manifest["batch_id"]
        or receipt["safety"] != SAFETY_POLICY
    ):
        raise BatchError("batch completion identity/header/policy is invalid")
    expected_manifest_path = (
        Path(manifest["output"]["batch_root"])
        / manifest["batch_relative_path"]
        / "manifest.json"
    )
    manifest_reference = _exact(
        receipt["manifest"],
        "batch completion manifest reference",
        COMPLETION_MANIFEST_KEYS,
    )
    if manifest_reference["path"] != str(expected_manifest_path):
        raise BatchError("batch completion points at a different manifest path")
    manifest_body = _stable_body(
        Path(manifest_reference["path"]),
        "completion-bound manifest",
        MAX_MANIFEST_BYTES,
        mode=0o400,
        single_link=True,
    )
    if (
        manifest_reference["sha256"] != sha256_bytes(manifest_body)
        or manifest_reference["byte_count"] != len(manifest_body)
        or manifest_reference["identity_sha256"] != manifest["identity_sha256"]
    ):
        raise BatchError("batch completion manifest binding is invalid")
    execution, inferred_count, reused_count = _validate_completion_execution(
        receipt["execution"], receipt["hardware"], manifest, members
    )
    common = _exact(
        receipt["common_bindings"],
        "batch completion common bindings",
        COMPLETION_COMMON_KEYS,
    )
    replayed_common = replay_common_bindings(
        manifest, members, require_current=False, live_hardware=False
    )
    if common != replayed_common:
        raise BatchError("batch completion common bindings failed frozen replay")
    results = receipt["results"]
    if not isinstance(results, list) or len(results) != len(members):
        raise BatchError("batch completion result count differs from manifest")
    observed_inferred = 0
    for member, reference in zip(members, results, strict=True):
        reference = _exact(
            reference, "batch completion result reference", COMPLETION_RESULT_KEYS
        )
        if reference["disposition"] not in {
            "inferred",
            "reused",
        }:
            raise BatchError("batch completion has an invalid member disposition")
        observed_inferred += reference["disposition"] == "inferred"
        plan = V3.result_plan(member["work_order"])
        try:
            result = V3.validate_completed_result(member["work_order"], plan)
        except (BASE.ProductionASRError, OSError) as error:
            raise BatchError(f"batch completion member replay failed: {error}") from error
        expected = _result_reference(member, result, reference["disposition"])
        if reference != expected:
            raise BatchError("batch completion result reference failed exact replay")
    if (
        observed_inferred != inferred_count
        or len(results) - observed_inferred != reused_count
    ):
        raise BatchError("batch completion result dispositions differ from execution counts")
    return receipt


def status_document(manifest_path: Path) -> dict[str, Any]:
    manifest, members = load_manifest(manifest_path, replay_runtime=False)
    completion = validate_completion(manifest, members)
    states = _member_states(members)
    return {
        "kind": "himr_faster_whisper_gpu_batch_status",
        "schema_version": SCHEMA_VERSION,
        "status": "completed" if completion is not None else "pending",
        "batch_id": manifest["batch_id"],
        "manifest_identity_sha256": manifest["identity_sha256"],
        "completion": None
        if completion is None
        else {
            "path": str(_completion_path(manifest)),
            "identity_sha256": completion["identity_sha256"],
            "completion_id": completion["completion_id"],
        },
        "items": states,
        "counts": {
            "completed": sum(item["state"] == "completed" for item in states),
            "pending": sum(item["state"] == "pending" for item in states),
        },
        "gpu_lock": {
            "path": manifest["common"]["gpu"]["lock_path"],
            "state": BASE.lock_state(Path(manifest["common"]["gpu"]["lock_path"])),
        },
        "inference_executed": False,
        "files_written": False,
        "safety": SAFETY_POLICY,
    }


def _publish_completion(
    manifest: dict[str, Any],
    members: Sequence[dict[str, Any]],
    core: dict[str, Any],
) -> dict[str, Any]:
    existing = validate_completion(manifest, members)
    if existing is not None:
        return existing
    identity = sha256_bytes(canonical_bytes(core))
    receipt = {
        **core,
        "identity_sha256": identity,
        "completion_id": f"gpuasrbatchdone_{identity[:32]}",
    }
    body = canonical_bytes(receipt)
    if len(body) > MAX_COMPLETION_BYTES:
        raise BatchError("batch completion receipt exceeds its byte cap")
    receipt_root = Path(manifest["output"]["receipt_root"])
    batches = _ensure_private_collection(receipt_root, "batches")
    batch_parent = _ensure_private_collection(batches, manifest["batch_id"])
    final = batch_parent / "completion"
    if final.exists() or final.is_symlink():
        existing = validate_completion(manifest, members)
        if existing is None:
            raise BatchError("completion path appeared without a valid receipt")
        return existing
    staging = batch_parent / f".completion.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    try:
        staging.mkdir(mode=0o700)
        BASE.write_new_private_file(staging / "receipt.json", body)
        BASE.sync_directory(staging)
        os.chmod(staging, 0o500)
        try:
            BASE.publish_directory_no_replace(staging, final)
        except FileExistsError:
            _safe_remove_staging(staging)
            existing = validate_completion(manifest, members)
            if existing is None:
                raise BatchError("completion race produced no valid receipt")
            return existing
    finally:
        _safe_remove_staging(staging)
    observed = validate_completion(manifest, members)
    if observed is None or observed != receipt:
        raise BatchError("published completion receipt failed exact replay")
    return observed


def _batch_manifest_reference(manifest_path: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    body = _stable_body(
        manifest_path,
        "batch manifest",
        MAX_MANIFEST_BYTES,
        mode=0o400,
        single_link=True,
    )
    return {
        "path": str(manifest_path),
        "sha256": sha256_bytes(body),
        "byte_count": len(body),
        "identity_sha256": manifest["identity_sha256"],
    }


def _completed_results(
    members: Sequence[dict[str, Any]], dispositions: dict[int, str]
) -> list[dict[str, Any]]:
    references: list[dict[str, Any]] = []
    for member in members:
        plan = V3.result_plan(member["work_order"])
        try:
            result = V3.validate_completed_result(member["work_order"], plan)
        except (BASE.ProductionASRError, OSError) as error:
            raise BatchError(f"final member result replay failed: {error}") from error
        references.append(
            _result_reference(
                member,
                result,
                dispositions.get(member["entry"]["ordinal"], "reused"),
            )
        )
    return references


def _completion_without_cuda(
    manifest_path: Path,
    manifest: dict[str, Any],
    members: Sequence[dict[str, Any]],
    common_bindings: dict[str, Any],
) -> dict[str, Any]:
    started = utc_now()
    results = _completed_results(members, {})
    core = {
        "kind": COMPLETION_KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": "completed",
        "batch_id": manifest["batch_id"],
        "manifest": _batch_manifest_reference(manifest_path, manifest),
        "execution": {
            "batch_run_id": f"run_gpu_asr_batch_{uuid.uuid4().hex}",
            "started_at": started,
            "completed_at": utc_now(),
            "duration_ms": 0,
            "model_loaded": False,
            "model_load_count": 0,
            "model_load_seconds": 0.0,
            "inference_item_count": 0,
            "reused_item_count": len(members),
            "inference_seconds": 0.0,
            "resume_completion_only": True,
        },
        "common_bindings": common_bindings,
        "hardware": None,
        "results": results,
        "safety": SAFETY_POLICY,
    }
    return _publish_completion(manifest, members, core)


def _hardware_snapshot(
    *,
    pynvml: Any,
    handle: Any,
    sampler: Any,
    work_order: dict[str, Any],
    device_count: int,
    supported: list[str],
    memory_before: dict[str, int],
) -> dict[str, Any]:
    sampled = sampler.snapshot()
    return {
        "device_index": work_order["gpu"]["device_index"],
        "name": pynvml.nvmlDeviceGetName(handle),
        "uuid": pynvml.nvmlDeviceGetUUID(handle),
        "driver_version": pynvml.nvmlSystemGetDriverVersion(),
        "cuda_driver_version": pynvml.nvmlSystemGetCudaDriverVersion_v2(),
        "compute_capability": list(pynvml.nvmlDeviceGetCudaComputeCapability(handle)),
        "ctranslate2_cuda_device_count": device_count,
        "ctranslate2_supported_compute_types": supported,
        "memory_before": memory_before,
        "memory_after": BASE.nvml_memory(pynvml, handle),
        **sampled,
    }


def _transcribe_one_transaction(
    *,
    model: Any,
    member: dict[str, Any],
    common: dict[str, Any],
    hardware_provider: Any,
    lock_evidence: dict[str, Any],
    network: dict[str, Any],
    model_load_seconds: float,
    batch_provenance: dict[str, Any],
) -> tuple[dict[str, Any], float]:
    work_order = member["work_order"]
    plan = V3.result_plan(work_order)
    state, existing = BASE.completed_or_pending(work_order, plan)
    if state == "completed" and existing is not None:
        return existing, 0.0
    started_at = utc_now()
    started_clock = time.monotonic()
    raw: dict[str, Any]
    normalized: dict[str, Any]
    raw_body: bytes
    normalized_body: bytes
    input_observation: dict[str, Any]
    retained_probe: dict[str, Any]
    retained_probe_command: list[str]
    inference_seconds: float
    with BASE.HardDeadline(work_order["inference"]["max_wall_seconds"]):
        try:
            with BASE.retained_verified_input(work_order["input"]) as (
                descriptor,
                retained_path,
                input_observation,
            ):
                retained_probe, retained_probe_command = BASE.probe_audio(
                    work_order, retained_path, pass_fds=(descriptor,)
                )
                if (
                    input_observation["path"] != work_order["input"]["path"]
                    or input_observation["sha256"]
                    != work_order["input"]["expected_sha256"]
                    or input_observation["byte_count"]
                    != work_order["input"]["expected_byte_count"]
                    or input_observation["mode"]
                    != int(work_order["input"]["sealed_mode"], 8)
                    or input_observation["link_count"] != 1
                    or input_observation["device"]
                    != work_order["runtime"]["expected_device"]
                ):
                    raise BatchError(
                        "retained member input escaped its sealed main-drive binding"
                    )
                inference_start = time.monotonic()
                iterator, info = model.transcribe(
                    retained_path,
                    task="transcribe",
                    language=None
                    if work_order["inference"]["language"] == "auto"
                    else work_order["inference"]["language"],
                    beam_size=work_order["inference"]["beam_size"],
                    best_of=work_order["inference"]["best_of"],
                    temperature=work_order["inference"]["temperature"],
                    word_timestamps=True,
                    vad_filter=False,
                    condition_on_previous_text=False,
                    without_timestamps=False,
                )
                segments: list[dict[str, Any]] = []
                word_count = 0
                for ordinal, segment in enumerate(iterator):
                    if ordinal >= work_order["inference"]["max_segments"]:
                        raise BatchError("inference exceeded member max_segments")
                    item = V3.raw_segment(segment, ordinal)
                    word_count += len(item["words"])
                    if word_count > work_order["inference"]["max_words"]:
                        raise BatchError("inference exceeded member max_words")
                    segments.append(item)
                inference_seconds = time.monotonic() - inference_start
                raw = BASE.build_raw_transcript(info, segments, work_order)
                normalized = V3.normalize_raw_transcript(raw, work_order)
                raw_body, normalized_body = BASE.serialize_transcripts(
                    raw, normalized, work_order["inference"]["max_result_bytes"]
                )
        except (BASE.ProductionASRError, OSError, subprocess.SubprocessError) as error:
            raise BatchError(str(error)) from error
    static = {
        "input": input_observation | {"probe": retained_probe},
        "model": common["model"],
        "runtime": common["runtime"],
        "commands": [retained_probe_command],
    }
    runtime_evidence = {
        **common["runtime"],
        "platform": platform.platform(),
        "python": sys.version,
        "python_isolated_mode": bool(sys.flags.isolated),
        "network": network,
        "offline_environment": {
            name: os.environ.get(name)
            for name in (
                "HF_HUB_OFFLINE",
                "HF_DATASETS_OFFLINE",
                "HF_HUB_DISABLE_TELEMETRY",
                "DO_NOT_TRACK",
            )
        },
        "model_load_seconds": model_load_seconds,
        "inference_seconds": inference_seconds,
        "inference_real_time_factor": inference_seconds
        / (work_order["input"]["expected_duration_ms"] / 1_000),
        "resident_batch": batch_provenance,
    }
    hardware = hardware_provider()
    if not hardware["process_vram_measurement_seen"]:
        raise BatchError("NVML did not observe the resident model before publication")
    if (
        hardware["process_peak_used_bytes"]
        > work_order["gpu"]["maximum_process_vram_bytes"]
    ):
        raise BatchError("sampled process VRAM exceeded the admitted ceiling")
    completed_at = utc_now()
    try:
        result = BASE.build_result(
            work_order=work_order,
            work_order_file=member["work_order_file"],
            plan=plan,
            static_evidence=static,
            raw=raw,
            normalized=normalized,
            raw_body=raw_body,
            normalized_body=normalized_body,
            hardware=hardware,
            runtime_evidence=runtime_evidence,
            lock_evidence=lock_evidence,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=round((time.monotonic() - started_clock) * 1_000),
        )
        published = BASE.publish_result(
            work_order, plan, raw_body, normalized_body, result
        )
        replayed = V3.validate_completed_result(work_order, plan)
    except (BASE.ProductionASRError, OSError) as error:
        result_dir = Path(plan["result_dir"])
        published_or_partial = (
            Path(plan["result_path"]).is_file()
            or result_dir.exists()
            or result_dir.is_symlink()
        )
        raise MemberResultFailure(
            f"member result publication/replay failed: {error}",
            files_published=published_or_partial,
        ) from error
    if published != replayed:
        raise MemberResultFailure(
            "published member result differs from immediate exact replay",
            files_published=True,
        )
    return replayed, inference_seconds


def _transcribe_one(
    *,
    model: Any,
    member: dict[str, Any],
    common: dict[str, Any],
    hardware_provider: Any,
    lock_evidence: dict[str, Any],
    network: dict[str, Any],
    model_load_seconds: float,
    batch_provenance: dict[str, Any],
) -> tuple[dict[str, Any], float]:
    """Bound hashing, inference, result sealing, and exact replay as one item."""

    with BASE.HardDeadline(
        member["work_order"]["inference"]["max_wall_seconds"]
    ):
        return _transcribe_one_transaction(
            model=model,
            member=member,
            common=common,
            hardware_provider=hardware_provider,
            lock_evidence=lock_evidence,
            network=network,
            model_load_seconds=model_load_seconds,
            batch_provenance=batch_provenance,
        )


def run_batch(manifest_path: Path, expected_parent_network_namespace: str) -> dict[str, Any]:
    if not sys.flags.isolated:
        raise BatchError("production batch run requires Python isolated mode (-I)")
    if os.environ.get("CUDA_VISIBLE_DEVICES") not in (None, "", "0"):
        raise BatchError("CUDA_VISIBLE_DEVICES must be unset, empty, or 0")
    BASE.ensure_offline_environment()
    manifest, members = load_manifest(manifest_path, replay_runtime=False)
    _remaining_batch_wall_seconds(manifest)
    completed = validate_completion(manifest, members)
    if completed is not None:
        return completed
    states = _member_states(members)
    if all(item["state"] == "completed" for item in states):
        with BASE.HardDeadline(_remaining_batch_wall_seconds(manifest)):
            BASE.network_isolation_evidence(expected_parent_network_namespace)
            common = replay_common_bindings(
                manifest,
                members,
                require_current=True,
                live_hardware=False,
            )
            return _completion_without_cuda(
                manifest_path, manifest, members, common
            )
    try:
        ensure_cuda_wheel_libraries()
    except (BatchError, BASE.ProductionASRError) as error:
        raise BatchError(str(error)) from error

    first = members[0]["work_order"]
    process_elapsed = _process_elapsed_seconds()
    started_at = (
        datetime.now(UTC) - timedelta(seconds=process_elapsed)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    started_clock = time.monotonic() - process_elapsed
    run_id = f"run_gpu_asr_batch_{uuid.uuid4().hex}"
    dispositions = {
        state["ordinal"]: "reused"
        for state in states
        if state["state"] == "completed"
    }
    completed_during_attempt: list[dict[str, Any]] = []
    inference_seconds_total = 0.0
    inference_count = 0
    current_member: dict[str, Any] | None = None
    current_result_published = False
    common_start: dict[str, Any] | None = None
    final_hardware: dict[str, Any] | None = None
    model_load_seconds = 0.0

    try:
        with BASE.gpu_advisory_lock(first) as lock_evidence:
            with BASE.HardDeadline(_remaining_batch_wall_seconds(manifest)):
                network = BASE.network_isolation_evidence(
                    expected_parent_network_namespace
                )
                common_start = replay_common_bindings(
                    manifest,
                    members,
                    require_current=True,
                    live_hardware=True,
                )
                import ctranslate2
                import pynvml
                from faster_whisper import WhisperModel

                pynvml.nvmlInit()
                sampler = None
                model = None
                try:
                    device_count = ctranslate2.get_cuda_device_count()
                    if device_count < 1:
                        raise BatchError("CTranslate2 reports no CUDA device")
                    index = first["gpu"]["device_index"]
                    handle = pynvml.nvmlDeviceGetHandleByIndex(index)
                    observed_uuid = pynvml.nvmlDeviceGetUUID(handle)
                    if observed_uuid != first["gpu"]["expected_uuid"]:
                        raise BatchError("NVML UUID differs from the manifest GPU")
                    supported = sorted(ctranslate2.get_supported_compute_types("cuda", index))
                    if first["gpu"]["compute_type"] not in supported:
                        raise BatchError("manifest compute type is unsupported")
                    memory_before = BASE.nvml_memory(pynvml, handle)
                    if memory_before["free_bytes"] < first["gpu"]["minimum_free_vram_bytes"]:
                        raise BatchError("free VRAM is below the admitted minimum")
                    if first["gpu"]["maximum_process_vram_bytes"] > MAX_PROCESS_VRAM_BYTES:
                        raise BatchError("manifest process VRAM exceeds batch ceiling")
                    if (
                        first["gpu"]["maximum_process_vram_bytes"]
                        > memory_before["total_bytes"]
                    ):
                        raise BatchError(
                            "manifest process VRAM limit exceeds physical VRAM"
                        )
                    sampler = BatchNVMLSampler(
                        pynvml,
                        handle,
                        first["gpu"]["maximum_process_vram_bytes"],
                    )
                    sampler.start()
                    load_start = time.monotonic()
                    model = WhisperModel(
                        first["model"]["snapshot_root"],
                        device="cuda",
                        device_index=index,
                        compute_type=first["gpu"]["compute_type"],
                        local_files_only=True,
                        cpu_threads=first["inference"]["cpu_threads"],
                        num_workers=1,
                    )
                    model_load_seconds = time.monotonic() - load_start
                    sampler.wait_for_process_sample(2.0)
                    batch_provenance = {
                        "batch_id": manifest["batch_id"],
                        "manifest_identity_sha256": manifest["identity_sha256"],
                        "worker_source_sha256": manifest["software"]["batch_worker"]["sha256"],
                        "batch_run_id": run_id,
                        "completion_receipt_required": True,
                        "model_resident": True,
                        "neural_batching": False,
                    }
                    for member in members:
                        current_member = member
                        current_result_published = False
                        ordinal = member["entry"]["ordinal"]
                        plan = V3.result_plan(member["work_order"])
                        state, existing = BASE.completed_or_pending(
                            member["work_order"], plan
                        )
                        if state == "completed" and existing is not None:
                            dispositions[ordinal] = "reused"
                            continue
                        result, item_inference_seconds = _transcribe_one(
                            model=model,
                            member=member,
                            common=common_start,
                            hardware_provider=lambda: _hardware_snapshot(
                                pynvml=pynvml,
                                handle=handle,
                                sampler=sampler,
                                work_order=first,
                                device_count=device_count,
                                supported=supported,
                                memory_before=memory_before,
                            ),
                            lock_evidence=lock_evidence,
                            network=network,
                            model_load_seconds=model_load_seconds,
                            batch_provenance=batch_provenance | {"ordinal": ordinal},
                        )
                        current_result_published = True
                        dispositions[ordinal] = "inferred"
                        inference_seconds_total += item_inference_seconds
                        inference_count += 1
                        completed_during_attempt.append(
                            _result_reference(member, result, "inferred")
                        )
                        current_result_published = False
                        del result
                        gc.collect()
                    current_member = None
                    final_hardware = _hardware_snapshot(
                        pynvml=pynvml,
                        handle=handle,
                        sampler=sampler,
                        work_order=first,
                        device_count=device_count,
                        supported=supported,
                        memory_before=memory_before,
                    )
                finally:
                    if model is not None:
                        del model
                        gc.collect()
                    if sampler is not None:
                        sampler.stop()
                    pynvml.nvmlShutdown()

                common_end = replay_common_bindings(
                    manifest,
                    members,
                    require_current=True,
                    live_hardware=True,
                )
                if common_end != common_start:
                    raise BatchError("model/runtime/source binding drifted during batch")
                replayed_manifest, replayed_members = load_manifest(
                    manifest_path, replay_runtime=False
                )
                if replayed_manifest != manifest or len(replayed_members) != len(members):
                    raise BatchError("batch manifest changed during execution")
                results = _completed_results(members, dispositions)
                completed_at = utc_now()
                core = {
                    "kind": COMPLETION_KIND,
                    "schema_version": SCHEMA_VERSION,
                    "implementation_version": IMPLEMENTATION_VERSION,
                    "status": "completed",
                    "batch_id": manifest["batch_id"],
                    "manifest": _batch_manifest_reference(manifest_path, manifest),
                    "execution": {
                        "batch_run_id": run_id,
                        "started_at": started_at,
                        "completed_at": completed_at,
                        "duration_ms": round((time.monotonic() - started_clock) * 1_000),
                        "model_loaded": True,
                        "model_load_count": 1,
                        "model_load_seconds": model_load_seconds,
                        "inference_item_count": inference_count,
                        "reused_item_count": len(members) - inference_count,
                        "inference_seconds": inference_seconds_total,
                        "resume_completion_only": False,
                    },
                    "common_bindings": common_end,
                    "hardware": final_hardware,
                    "results": results,
                    "safety": SAFETY_POLICY,
                }
                return _publish_completion(manifest, members, core)
    except Exception as error:
        if isinstance(error, MemberResultFailure) and error.files_published:
            current_result_published = True
        failed = None
        if current_member is not None:
            failed = {
                "ordinal": current_member["entry"]["ordinal"],
                "job_id": current_member["work_order"]["job_id"],
                "work_order_identity_sha256": current_member["work_order"][
                    "identity_sha256"
                ],
                "error": {"type": type(error).__name__, "message": str(error)},
            }
        document = {
            "kind": FAILURE_KIND,
            "schema_version": SCHEMA_VERSION,
            "implementation_version": IMPLEMENTATION_VERSION,
            "status": "failed",
            "batch_id": manifest["batch_id"],
            "manifest_identity_sha256": manifest["identity_sha256"],
            "batch_run_id": run_id,
            "failed_item": failed,
            "completed_in_this_attempt": completed_during_attempt,
            "later_items_attempted": False,
            "files_published": bool(completed_during_attempt)
            or current_result_published,
            "completion_published": False,
            "safety": SAFETY_POLICY,
        }
        raise BatchRunFailure(document) from error


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("contracts", help="emit the canonical batch contract")
    materialize = commands.add_parser(
        "materialize", help="atomically seal an ordered finite v3 work-order batch"
    )
    materialize.add_argument("--work-order", action="append", required=True)
    materialize.add_argument("--batch-root", required=True)
    materialize.add_argument("--receipt-root", required=True)
    materialize.add_argument("--maximum-batch-wall-seconds", required=True, type=int)
    validate = commands.add_parser("validate", help="exactly replay a sealed batch")
    validate.add_argument("--manifest", required=True)
    dry = commands.add_parser("dry-run", help="replay all bindings without CUDA or writes")
    dry.add_argument("--manifest", required=True)
    status = commands.add_parser("status", help="read-only member/completion status")
    status.add_argument("--manifest", required=True)
    run = commands.add_parser("run", help="run or resume the finite resident-model batch")
    run.add_argument("--manifest", required=True)
    run.add_argument("--expected-parent-network-namespace", required=True)
    return parser


def _absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve(strict=False)
    return path


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "contracts":
            result: Any = contract_document()
        elif args.command == "materialize":
            manifest, path = materialize_batch(
                work_order_paths=[_absolute_path(value) for value in args.work_order],
                batch_root=_absolute_path(args.batch_root),
                receipt_root=_absolute_path(args.receipt_root),
                maximum_batch_wall_seconds=args.maximum_batch_wall_seconds,
            )
            result = {"manifest": manifest, "manifest_path": str(path)}
        elif args.command == "validate":
            manifest_path = _absolute_path(args.manifest)
            manifest, members = load_manifest(manifest_path, replay_runtime=True)
            result = {
                "status": "validated",
                "manifest": manifest,
                "member_states": _member_states(members),
                "inference_executed": False,
                "files_written": False,
            }
        elif args.command == "dry-run":
            result = dry_run_document(_absolute_path(args.manifest))
        elif args.command == "status":
            result = status_document(_absolute_path(args.manifest))
        elif args.command == "run":
            result = run_batch(
                _absolute_path(args.manifest), args.expected_parent_network_namespace
            )
        else:  # pragma: no cover
            raise BatchError(f"unsupported command: {args.command}")
        sys.stdout.buffer.write(canonical_bytes(result))
        return 0
    except BatchRunFailure as error:
        sys.stderr.buffer.write(canonical_bytes(error.document))
        return 2
    except Exception as error:
        failure = {
            "kind": FAILURE_KIND,
            "schema_version": SCHEMA_VERSION,
            "implementation_version": IMPLEMENTATION_VERSION,
            "status": "failed",
            "error": {"type": type(error).__name__, "message": str(error)},
            "inference_executed": False,
            "files_published": False,
            "safety": SAFETY_POLICY,
        }
        sys.stderr.buffer.write(canonical_bytes(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
