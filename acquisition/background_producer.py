#!/usr/bin/env python3
"""Run one finite, backpressured cycle of a sealed public acquisition queue.

This is a foreground supervisor, not a daemon.  It never discovers or rewrites a
source and it delegates every network operation to the existing queue runner.  Its
only completion state is the queue's strictly replayed acquisition results.  Exact
preprocess receipts may acknowledge those results for ready-buffer accounting.
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

try:
    from . import acquire, materialize_queue, queue_runner
except ImportError:  # pragma: no cover - direct script execution
    import acquire  # type: ignore[no-redef]
    import materialize_queue  # type: ignore[no-redef]
    import queue_runner  # type: ignore[no-redef]


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.2.0"
PRODUCER_NAME = "himr-background-acquisition-producer"
SCHEDULE_KIND = "sealed_public_background_acquisition"

MAX_SCHEDULE_BYTES = 4 * 1024 * 1024
MAX_REPORT_BYTES = 64 * 1024 * 1024
MAX_PREPROCESS_RECEIPT_BYTES = 4 * 1024 * 1024
MAX_PREPROCESS_RESULT_BYTES = 64 * 1024 * 1024
MAX_ACK_RECEIPTS = 100_000
MAX_ITEMS = 10_000
MAX_RUN_SECONDS = queue_runner.MAX_RUN_SECONDS
MAX_INTEGER = 2**63 - 1

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SCHEDULE_ID_RE = re.compile(r"^bgacqsched_[0-9a-f]{32}$")
PREPROCESS_BUNDLE_RE = re.compile(r"^ppbatch_[0-9a-f]{32}$")
PREPROCESS_RECEIPT_RE = re.compile(r"^[0-9]{6}\.json$")
PREPROCESS_LOCK_RE = re.compile(r"^\.preprocess-batch-ppbatch_[0-9a-f]{32}\.lock$")
COLD_ARCHIVE_ROOT = Path("/mnt/archive/HIMR")

SCHEDULE_SAFETY = {
    "access_policy": "sealed_public_work_orders_only",
    "catalog_access": "forbidden",
    "catalog_writes": False,
    "cold_storage_access": "forbidden",
    "credentials_allowed": False,
    "deletion_authority": "none",
    "discovery_allowed": False,
    "event_authority": "none",
    "export_authority": "none",
    "identity_authority": "none",
    "publication_authority": "none",
    "substitutions_allowed": False,
}

RUNTIME_SAFETY = {
    **SCHEDULE_SAFETY,
    "credentials_used": False,
    "maximum_network_concurrency": 1,
}

COLD_PRIMARY_SCHEDULE_SAFETY = {
    **SCHEDULE_SAFETY,
    "cold_storage_access": "sealed_queue_media_output_root_write_only",
}

COLD_PRIMARY_RUNTIME_SAFETY = {
    **COLD_PRIMARY_SCHEDULE_SAFETY,
    "credentials_used": False,
    "maximum_network_concurrency": 1,
}


class BackgroundProducerError(RuntimeError):
    """A schedule, acknowledgement, bound, or immutable replay failed closed."""


class BackgroundProducerFailure(BackgroundProducerError):
    """The delegated queue failed after zero or more durable completions."""

    def __init__(self, summary: dict[str, Any]):
        super().__init__("the delegated sealed acquisition queue failed")
        self.summary = summary


def canonical_bytes(value: Any) -> bytes:
    return materialize_queue.canonical_bytes(value)


def pretty_bytes(value: Any) -> bytes:
    return materialize_queue.pretty_bytes(value)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _exact_object(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BackgroundProducerError(f"{label} must be an object")
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    if missing or unknown:
        raise BackgroundProducerError(
            f"{label} keys differ from the exact contract; "
            f"missing={missing}, unknown={unknown}"
        )
    return value


def _integer(
    value: Any,
    label: str,
    minimum: int = 0,
    maximum: int = MAX_INTEGER,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise BackgroundProducerError(
            f"{label} must be an integer from {minimum} through {maximum}"
        )
    return value


def _text(value: Any, label: str, maximum: int = 4_096) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise BackgroundProducerError(f"{label} must be bounded non-empty text")
    return value


def _sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise BackgroundProducerError(f"{label} must be a lowercase SHA-256")
    return value


def _absolute_lexical_path(value: Any, label: str) -> Path:
    raw = _text(value, label, 16_384)
    if "://" in raw:
        raise BackgroundProducerError(f"{label} must be a local path")
    path = Path(raw)
    if not path.is_absolute() or str(path) != raw:
        raise BackgroundProducerError(f"{label} must be a lexical absolute path")
    return path


def _is_within(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _reject_cold_path(path: Path, label: str) -> None:
    # This is a lexical comparison.  It deliberately does not stat or enumerate the
    # cold archive mount.
    if _is_within(path, COLD_ARCHIVE_ROOT):
        raise BackgroundProducerError(f"{label} may not use the cold archive mount")


def _queue_storage_safety(output_root: Path) -> dict[str, Any]:
    """Return the exact capability declaration for a sealed queue output root.

    A cold-primary queue must use a dedicated descendant of the fixed archive
    mount.  Naming the mount itself would let one queue manage unrelated archival
    objects, while naming an ancestor could make capacity accounting traverse or
    stage across a broader tree than the queue owns.

    Queue materialization normalizes ``media_output_root`` through
    :func:`acquire.absolute_path`, so this lexical boundary is also the resolved
    boundary pinned in every generated work order.
    """

    if output_root == COLD_ARCHIVE_ROOT:
        raise BackgroundProducerError(
            "queue media output root must be a dedicated descendant of the cold archive"
        )
    if COLD_ARCHIVE_ROOT in output_root.parents:
        return COLD_PRIMARY_SCHEDULE_SAFETY
    if output_root in COLD_ARCHIVE_ROOT.parents:
        raise BackgroundProducerError(
            "queue media output root may not contain the cold archive mount"
        )
    return SCHEDULE_SAFETY


def _runtime_safety(schedule: dict[str, Any]) -> dict[str, Any]:
    if schedule.get("safety") == COLD_PRIMARY_SCHEDULE_SAFETY:
        return COLD_PRIMARY_RUNTIME_SAFETY
    return RUNTIME_SAFETY


def _producer_observation() -> dict[str, Any]:
    path = Path(__file__).resolve()
    body, observed = queue_runner._stable_read(
        path,
        maximum=4 * 1024 * 1024,
        label="background producer source",
    )
    return {
        "name": PRODUCER_NAME,
        "version": IMPLEMENTATION_VERSION,
        "source_path": str(path),
        "source_sha256": sha256_bytes(body),
        "source_byte_count": observed.st_size,
    }


def _strict_json(body: bytes, label: str) -> dict[str, Any]:
    try:
        value = materialize_queue.load_json_bytes(body, label)
    except materialize_queue.MaterializationError as error:
        raise BackgroundProducerError(str(error)) from error
    if not isinstance(value, dict):
        raise BackgroundProducerError(f"{label} must be a JSON object")
    return value


def _schedule_core(
    *,
    queue: dict[str, Any],
    consumer: dict[str, Any],
    policy: dict[str, Any],
    producer: dict[str, Any],
    safety: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "schedule_kind": SCHEDULE_KIND,
        "producer": producer,
        "queue": queue,
        "consumer": consumer,
        "policy": policy,
        "safety": safety,
    }


def build_schedule(
    *,
    manifest_path: Path,
    preprocess_state_root: Path,
    ready_high_items: int,
    ready_low_items: int,
    ready_high_bytes: int,
    ready_low_bytes: int,
    maximum_dispatch_items_per_run: int,
    maximum_dispatch_bytes_per_run: int,
    maximum_run_seconds: int,
    free_space_floor_bytes: int,
) -> dict[str, Any]:
    bundle = queue_runner._load_bundle(manifest_path)
    manifest = bundle["manifest"]
    preprocess_state_root = _absolute_lexical_path(
        str(preprocess_state_root), "preprocess state root"
    )
    _reject_cold_path(preprocess_state_root, "preprocess state root")
    output_root = Path(manifest["policy"]["media_output_root"])
    if _is_within(preprocess_state_root, output_root):
        raise BackgroundProducerError(
            "preprocess state root must be outside the managed acquisition output root"
        )

    high_items = _integer(ready_high_items, "ready high items", 1, MAX_ITEMS)
    low_items = _integer(ready_low_items, "ready low items", 0, MAX_ITEMS)
    high_bytes = _integer(ready_high_bytes, "ready high bytes", 1)
    low_bytes = _integer(ready_low_bytes, "ready low bytes", 0)
    if low_items >= high_items or low_bytes >= high_bytes:
        raise BackgroundProducerError("ready low-water marks must be below high-water marks")
    dispatch_items = _integer(
        maximum_dispatch_items_per_run,
        "maximum dispatch items per run",
        1,
        min(MAX_ITEMS, high_items),
    )
    dispatch_bytes = _integer(
        maximum_dispatch_bytes_per_run,
        "maximum dispatch bytes per run",
        1,
        high_bytes,
    )
    run_seconds = _integer(
        maximum_run_seconds,
        "maximum run seconds",
        1,
        MAX_RUN_SECONDS,
    )
    floor = _integer(free_space_floor_bytes, "free-space floor bytes")
    if floor < manifest["policy"]["free_space_floor_bytes"]:
        raise BackgroundProducerError(
            "producer free-space floor may not weaken the sealed queue floor"
        )

    queue_document = {
        "manifest_path": str(bundle["path"]),
        "manifest_sha256": sha256_bytes(bundle["body"]),
        "bundle_id": manifest["bundle_id"],
        "software": queue_runner._software_document(),
    }
    consumer = {
        "preprocess_state_root": str(preprocess_state_root),
        "acknowledgement_contract": "completed_private_media_preprocess_batch_item_v1",
    }
    policy = {
        "maximum_network_concurrency": 1,
        "dispatch_order": "sealed_ordinal_sequential_bounded_failure_isolation",
        "ready_high_items": high_items,
        "ready_low_items": low_items,
        "ready_high_bytes": high_bytes,
        "ready_low_bytes": low_bytes,
        "maximum_dispatch_items_per_run": dispatch_items,
        "maximum_dispatch_bytes_per_run": dispatch_bytes,
        "maximum_run_seconds": run_seconds,
        "free_space_floor_bytes": floor,
        "reservation_accounting": "full_sealed_max_job_bytes",
        "hysteresis_resume": "both_low_water_conditions",
    }
    core = _schedule_core(
        queue=queue_document,
        consumer=consumer,
        policy=policy,
        producer=_producer_observation(),
        safety=_queue_storage_safety(output_root),
    )
    digest = sha256_bytes(canonical_bytes(core))
    return {
        **core,
        "schedule_id": f"bgacqsched_{digest[:32]}",
        "identity_sha256": digest,
    }


def validate_schedule(value: Any) -> dict[str, Any]:
    schedule = _exact_object(
        value,
        "background acquisition schedule",
        {
            "schema_version",
            "schedule_kind",
            "producer",
            "queue",
            "consumer",
            "policy",
            "safety",
            "schedule_id",
            "identity_sha256",
        },
    )
    if (
        schedule["schema_version"] != SCHEMA_VERSION
        or schedule["schedule_kind"] != SCHEDULE_KIND
    ):
        raise BackgroundProducerError("background acquisition schedule is unsupported")

    producer = _exact_object(
        schedule["producer"],
        "schedule producer",
        {"name", "version", "source_path", "source_sha256", "source_byte_count"},
    )
    current = _producer_observation()
    if producer != current:
        raise BackgroundProducerError("background producer source differs from schedule pin")

    queue = _exact_object(
        schedule["queue"],
        "schedule queue",
        {"manifest_path", "manifest_sha256", "bundle_id", "software"},
    )
    manifest_path = _absolute_lexical_path(queue["manifest_path"], "queue manifest path")
    _reject_cold_path(manifest_path, "queue manifest path")
    manifest_sha = _sha256(queue["manifest_sha256"], "queue manifest SHA-256")
    bundle_id = _text(queue["bundle_id"], "queue bundle ID", 80)
    if queue["software"] != queue_runner._software_document():
        raise BackgroundProducerError(
            "queue runner/acquisition/materializer software differs from schedule pin"
        )

    consumer = _exact_object(
        schedule["consumer"],
        "schedule consumer",
        {"preprocess_state_root", "acknowledgement_contract"},
    )
    state_root = _absolute_lexical_path(
        consumer["preprocess_state_root"], "preprocess state root"
    )
    _reject_cold_path(state_root, "preprocess state root")
    if (
        consumer["acknowledgement_contract"]
        != "completed_private_media_preprocess_batch_item_v1"
    ):
        raise BackgroundProducerError("preprocess acknowledgement contract is unsupported")

    policy = _exact_object(
        schedule["policy"],
        "schedule policy",
        {
            "maximum_network_concurrency",
            "dispatch_order",
            "ready_high_items",
            "ready_low_items",
            "ready_high_bytes",
            "ready_low_bytes",
            "maximum_dispatch_items_per_run",
            "maximum_dispatch_bytes_per_run",
            "maximum_run_seconds",
            "free_space_floor_bytes",
            "reservation_accounting",
            "hysteresis_resume",
        },
    )
    if (
        policy["maximum_network_concurrency"] != 1
        or policy["dispatch_order"]
        != "sealed_ordinal_sequential_bounded_failure_isolation"
        or policy["reservation_accounting"] != "full_sealed_max_job_bytes"
        or policy["hysteresis_resume"] != "both_low_water_conditions"
    ):
        raise BackgroundProducerError("schedule weakens producer execution policy")
    high_items = _integer(policy["ready_high_items"], "ready high items", 1, MAX_ITEMS)
    low_items = _integer(policy["ready_low_items"], "ready low items", 0, MAX_ITEMS)
    high_bytes = _integer(policy["ready_high_bytes"], "ready high bytes", 1)
    low_bytes = _integer(policy["ready_low_bytes"], "ready low bytes", 0)
    if low_items >= high_items or low_bytes >= high_bytes:
        raise BackgroundProducerError("ready low-water marks must be below high-water marks")
    _integer(
        policy["maximum_dispatch_items_per_run"],
        "maximum dispatch items per run",
        1,
        min(MAX_ITEMS, high_items),
    )
    _integer(
        policy["maximum_dispatch_bytes_per_run"],
        "maximum dispatch bytes per run",
        1,
        high_bytes,
    )
    _integer(policy["maximum_run_seconds"], "maximum run seconds", 1, MAX_RUN_SECONDS)
    _integer(policy["free_space_floor_bytes"], "free-space floor bytes")
    identity = _sha256(schedule["identity_sha256"], "schedule identity SHA-256")
    schedule_id = _text(schedule["schedule_id"], "schedule ID", 80)
    bundle = queue_runner._load_bundle(manifest_path)
    if (
        sha256_bytes(bundle["body"]) != manifest_sha
        or bundle["manifest"]["bundle_id"] != bundle_id
    ):
        raise BackgroundProducerError("sealed queue differs from schedule binding")
    output_root = Path(bundle["manifest"]["policy"]["media_output_root"])
    expected_safety = _queue_storage_safety(output_root)
    if schedule["safety"] != expected_safety:
        raise BackgroundProducerError(
            "schedule safety block differs from its sealed queue storage topology"
        )
    core = _schedule_core(
        queue=queue,
        consumer=consumer,
        policy=policy,
        producer=producer,
        safety=expected_safety,
    )
    digest = sha256_bytes(canonical_bytes(core))
    if (
        identity != digest
        or schedule_id != f"bgacqsched_{digest[:32]}"
        or not SCHEDULE_ID_RE.fullmatch(schedule_id)
    ):
        raise BackgroundProducerError("schedule identity is inconsistent")
    if _is_within(state_root, output_root):
        raise BackgroundProducerError(
            "preprocess state root must be outside the managed acquisition output root"
        )
    if policy["free_space_floor_bytes"] < bundle["manifest"]["policy"][
        "free_space_floor_bytes"
    ]:
        raise BackgroundProducerError(
            "producer free-space floor weakens the sealed queue floor"
        )
    return schedule


def load_schedule(path: Path) -> tuple[dict[str, Any], Path, bytes]:
    resolved = queue_runner._absolute_lexical_path(path, "--schedule")
    body, _ = queue_runner._stable_read(
        resolved,
        maximum=MAX_SCHEDULE_BYTES,
        label="background acquisition schedule",
        required_mode=0o400,
    )
    schedule = validate_schedule(_strict_json(body, "background acquisition schedule"))
    if body != pretty_bytes(schedule):
        raise BackgroundProducerError("schedule is not in canonical sealed serialization")
    return schedule, resolved, body


def _safe_private_directory(path: Path, label: str) -> None:
    try:
        observed = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise BackgroundProducerError(f"{label} cannot be inspected") from error
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) & 0o077
        or resolved != path
    ):
        raise BackgroundProducerError(
            f"{label} must be an owner-only non-symlink directory"
        )


def _preprocess_receipt_paths(root: Path) -> list[Path]:
    if not root.exists() and not root.is_symlink():
        return []
    _safe_private_directory(root, "preprocess state root")
    root_entries = {entry.name: entry for entry in root.iterdir()}
    unexpected_root = sorted(
        name
        for name in root_entries
        if name != "runs" and not PREPROCESS_LOCK_RE.fullmatch(name)
    )
    if unexpected_root:
        raise BackgroundProducerError(
            f"preprocess state root contains unexpected entries: {unexpected_root}"
        )
    for name, entry in root_entries.items():
        if name == "runs":
            continue
        observed = entry.lstat()
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.getuid()
            or stat.S_IMODE(observed.st_mode) != 0o600
        ):
            raise BackgroundProducerError("preprocess state lock file is unsafe")
    runs = root / "runs"
    if not runs.exists() and not runs.is_symlink():
        return []
    _safe_private_directory(runs, "preprocess runs directory")
    receipts: list[Path] = []
    for run_dir in sorted(runs.iterdir(), key=lambda value: value.name):
        if not PREPROCESS_BUNDLE_RE.fullmatch(run_dir.name):
            raise BackgroundProducerError("preprocess runs directory has an invalid entry")
        _safe_private_directory(run_dir, "preprocess batch run directory")
        entries = list(run_dir.iterdir())
        if [entry.name for entry in entries] != ["receipts"]:
            raise BackgroundProducerError("preprocess batch run has missing or extra entries")
        receipts_dir = run_dir / "receipts"
        _safe_private_directory(receipts_dir, "preprocess receipts directory")
        for receipt in sorted(receipts_dir.iterdir(), key=lambda value: value.name):
            if not PREPROCESS_RECEIPT_RE.fullmatch(receipt.name):
                raise BackgroundProducerError(
                    "preprocess receipts directory has an unsupported entry"
                )
            receipts.append(receipt)
            if len(receipts) > MAX_ACK_RECEIPTS:
                raise BackgroundProducerError("preprocess acknowledgement count exceeds cap")
    return receipts


def _validate_receipt_digest(receipt: dict[str, Any]) -> None:
    keys = {
        "schema_version",
        "receipt_kind",
        "bundle_id",
        "bundle_manifest_sha256",
        "ordinal",
        "entry_id",
        "job_id",
        "work_order",
        "acquisition_result",
        "source_media",
        "preprocess_result",
        "artifacts",
        "safety",
        "receipt_id",
        "receipt_sha256",
    }
    if "handling_boundary" in receipt:
        keys.add("handling_boundary")
    _exact_object(receipt, "preprocess acknowledgement receipt", keys)
    if (
        receipt["schema_version"] != 1
        or receipt["receipt_kind"]
        != "completed_private_media_preprocess_batch_item"
        or not PREPROCESS_BUNDLE_RE.fullmatch(
            _text(receipt["bundle_id"], "preprocess bundle ID", 80)
        )
    ):
        raise BackgroundProducerError("preprocess acknowledgement is unsupported")
    _sha256(receipt["bundle_manifest_sha256"], "preprocess bundle manifest SHA-256")
    _integer(receipt["ordinal"], "preprocess receipt ordinal", 1, 128)
    _text(receipt["entry_id"], "preprocess entry ID", 80)
    _text(receipt["job_id"], "preprocess job ID", 128)
    safety = receipt["safety"]
    if not isinstance(safety, dict) or (
        safety.get("exact_validation_completed") is not True
        or safety.get("network_access_performed") is not False
        or safety.get("publication_performed") is not False
        or safety.get("publication_authority") != "none"
    ):
        raise BackgroundProducerError("preprocess acknowledgement safety is invalid")
    digest = _sha256(receipt["receipt_sha256"], "preprocess receipt SHA-256")
    receipt_id = _text(receipt["receipt_id"], "preprocess receipt ID", 80)
    core = {
        key: value
        for key, value in receipt.items()
        if key not in {"receipt_id", "receipt_sha256"}
    }
    expected = sha256_bytes(canonical_bytes(core))
    if digest != expected or receipt_id != f"ppreceipt_{expected[:32]}":
        raise BackgroundProducerError("preprocess acknowledgement identity is inconsistent")


def _acknowledged_results(
    root: Path,
    *,
    bundle: dict[str, Any],
    states: list[dict[str, Any] | None],
) -> set[str]:
    order_paths = {str(queue_runner._result_path(order)) for order in bundle["orders"]}
    completed: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for order, state in zip(bundle["orders"], states, strict=True):
        if _completed_state(state):
            completed[str(queue_runner._result_path(order))] = (order, state)
    acknowledged: set[str] = set()
    for path in _preprocess_receipt_paths(root):
        body, _ = queue_runner._stable_read(
            path,
            maximum=MAX_PREPROCESS_RECEIPT_BYTES,
            label="preprocess acknowledgement receipt",
            required_mode=0o400,
        )
        receipt = _strict_json(body, "preprocess acknowledgement receipt")
        _validate_receipt_digest(receipt)
        if body != pretty_bytes(receipt):
            raise BackgroundProducerError(
                "preprocess acknowledgement is not in canonical sealed serialization"
            )
        acquisition_ref = _exact_object(
            receipt["acquisition_result"],
            "preprocess acquisition result reference",
            {
                "path",
                "sha256",
                "byte_count",
                "job_id",
                "work_order_sha256",
                "completed_at",
            },
        )
        acquisition_path = str(
            _absolute_lexical_path(
                acquisition_ref["path"], "preprocess acquisition result path"
            )
        )
        if acquisition_path not in order_paths:
            continue
        if acquisition_path not in completed:
            raise BackgroundProducerError(
                "preprocess receipt acknowledges a non-completed queue result"
            )
        _order, state = completed[acquisition_path]
        result_body, _ = queue_runner._stable_read(
            Path(acquisition_path),
            maximum=acquire.MAX_DURABLE_RESULT_BYTES,
            label="acknowledged acquisition result",
        )
        if sha256_bytes(result_body) != state["result_sha256"]:
            raise BackgroundProducerError(
                "acknowledged acquisition result changed after queue validation"
            )
        result = _strict_json(result_body, "acknowledged acquisition result")
        if result_body != acquire.pretty_json(result).encode("utf-8"):
            raise BackgroundProducerError(
                "acknowledged acquisition result is not in canonical serialization"
            )
        expected_acquisition = {
            "path": acquisition_path,
            "sha256": state["result_sha256"],
            "byte_count": len(result_body),
            "job_id": result["job_id"],
            "work_order_sha256": result["work_order_sha256"],
            "completed_at": result["completed_at"],
        }
        if acquisition_ref != expected_acquisition:
            raise BackgroundProducerError(
                "preprocess acknowledgement differs from completed acquisition result"
            )
        source_ref = _exact_object(
            receipt["source_media"],
            "preprocess source media reference",
            {
                "path",
                "media_id",
                "sha256",
                "byte_count",
                "duration_ms",
                "first_cataloged_at",
            },
        )
        admission = result["admission"]
        expected_source = {
            "path": admission["path"],
            "media_id": admission["media_id"],
            "sha256": admission["sha256"],
            "byte_count": admission["byte_count"],
            "duration_ms": admission["normalized_probe"]["format"].get(
                "duration_ms"
            ),
            "first_cataloged_at": result["completed_at"],
        }
        if source_ref != expected_source:
            raise BackgroundProducerError(
                "preprocess acknowledgement differs from admitted source media"
            )
        preprocess_ref = _exact_object(
            receipt["preprocess_result"],
            "preprocess result reference",
            {
                "path",
                "sha256",
                "byte_count",
                "processing_run_id",
                "recipe_sha256",
                "reuse_mode",
            },
        )
        preprocess_path = _absolute_lexical_path(
            preprocess_ref["path"], "preprocess result path"
        )
        preprocess_body, preprocess_stat = queue_runner._stable_read(
            preprocess_path,
            maximum=MAX_PREPROCESS_RESULT_BYTES,
            label="acknowledged preprocess result",
        )
        if stat.S_IMODE(preprocess_stat.st_mode) & 0o022:
            raise BackgroundProducerError("acknowledged preprocess result is peer-writable")
        if (
            _sha256(preprocess_ref["sha256"], "preprocess result SHA-256")
            != sha256_bytes(preprocess_body)
            or _integer(
                preprocess_ref["byte_count"], "preprocess result byte count", 1
            )
            != len(preprocess_body)
        ):
            raise BackgroundProducerError("acknowledged preprocess result differs from receipt")
        acknowledged.add(acquisition_path)
    return acknowledged


QUEUE_SUMMARY_KEYS = {
    "schema_version",
    "runner_name",
    "implementation_version",
    "mode",
    "status",
    "bundle_id",
    "manifest_path",
    "manifest_sha256",
    "plan_id",
    "job_count",
    "completed_before",
    "quarantined_before",
    "pending_before",
    "completed_count",
    "quarantined_count",
    "parked_count",
    "pending_count",
    "terminal_count",
    "retryable_failed_count",
    "failed_attempt_count",
    "new_failed_attempt_count",
    "new_quarantined_count",
    "new_item_count",
    "new_byte_count",
    "adapter_invocation_count",
    "dispatch_reservation_bytes",
    "limits",
    "stop_reason",
    "results",
    "software",
    "safety",
    "summary_sha256",
}

QUEUE_RESULT_KEYS = {
    "ordinal",
    "job_id",
    "adapter",
    "status",
    "action",
    "adapter_invoked",
    "work_order_sha256",
    "work_order_file_sha256",
    "reservation_bytes",
    "failure_attempt_count",
    "latest_failure",
    "quarantine_receipt_sha256",
    "result_sha256",
    "media_sha256",
    "media_byte_count",
}


def _completed_state(state: dict[str, Any] | None) -> bool:
    if not isinstance(state, dict):
        return False
    marker = state.get("queue_state")
    return marker == "completed" or (
        marker is None
        and {"result_sha256", "media_sha256", "byte_count"} <= set(state)
    )


def _quarantined_state(state: dict[str, Any] | None) -> bool:
    return isinstance(state, dict) and state.get("queue_state") == "quarantined"


def _failure_summary(value: Any, label: str) -> dict[str, Any] | None:
    if value is None:
        return None
    failure = _exact_object(
        value,
        label,
        {"attempt_number", "observed_at", "error", "receipt_sha256"},
    )
    _integer(
        failure["attempt_number"],
        f"{label}.attempt_number",
        1,
        queue_runner.FAILURE_QUARANTINE_ATTEMPTS,
    )
    try:
        materialize_queue.validate_utc_timestamp(
            failure["observed_at"], f"{label}.observed_at"
        )
    except materialize_queue.MaterializationError as error:
        raise BackgroundProducerError(str(error)) from error
    error = _exact_object(failure["error"], f"{label}.error", {"type", "message"})
    _text(error["type"], f"{label}.error.type", 256)
    _text(error["message"], f"{label}.error.message", 8_192)
    _sha256(failure["receipt_sha256"], f"{label}.receipt_sha256")
    return failure


def _states_from_queue_summary(
    schedule: dict[str, Any],
    bundle: dict[str, Any],
    summary: Any,
    *,
    expected_mode: str,
    expected_statuses: set[str],
) -> list[dict[str, Any] | None]:
    summary = _exact_object(summary, "queue runner summary", QUEUE_SUMMARY_KEYS)
    digest = _sha256(summary["summary_sha256"], "queue summary SHA-256")
    core = {key: value for key, value in summary.items() if key != "summary_sha256"}
    if digest != sha256_bytes(canonical_bytes(core)):
        raise BackgroundProducerError("queue summary identity is inconsistent")

    manifest = bundle["manifest"]
    orders = bundle["orders"]
    results = summary["results"]
    job_count = _integer(summary["job_count"], "queue summary job count", 0, MAX_ITEMS)
    completed_before = _integer(
        summary["completed_before"], "queue summary completed-before count", 0, MAX_ITEMS
    )
    quarantined_before = _integer(
        summary["quarantined_before"],
        "queue summary quarantined-before count",
        0,
        MAX_ITEMS,
    )
    pending_before = _integer(
        summary["pending_before"], "queue summary pending-before count", 0, MAX_ITEMS
    )
    completed_count = _integer(
        summary["completed_count"], "queue summary completed count", 0, MAX_ITEMS
    )
    quarantined_count = _integer(
        summary["quarantined_count"],
        "queue summary quarantined count",
        0,
        MAX_ITEMS,
    )
    parked_count = _integer(
        summary["parked_count"], "queue summary parked count", 0, MAX_ITEMS
    )
    pending_count = _integer(
        summary["pending_count"], "queue summary pending count", 0, MAX_ITEMS
    )
    terminal_count = _integer(
        summary["terminal_count"], "queue summary terminal count", 0, MAX_ITEMS
    )
    retryable_failed_count = _integer(
        summary["retryable_failed_count"],
        "queue summary retryable-failed count",
        0,
        MAX_ITEMS,
    )
    failed_attempt_count = _integer(
        summary["failed_attempt_count"], "queue summary failed-attempt count"
    )
    new_failed_attempt_count = _integer(
        summary["new_failed_attempt_count"],
        "queue summary new failed-attempt count",
    )
    new_quarantined_count = _integer(
        summary["new_quarantined_count"],
        "queue summary new quarantined count",
        0,
        MAX_ITEMS,
    )
    new_item_count = _integer(
        summary["new_item_count"], "queue summary new-item count", 0, MAX_ITEMS
    )
    new_byte_count = _integer(
        summary["new_byte_count"], "queue summary new-byte count"
    )
    adapter_invocation_count = _integer(
        summary["adapter_invocation_count"],
        "queue summary adapter-invocation count",
        0,
        MAX_ITEMS,
    )
    dispatch_reservation_bytes = _integer(
        summary["dispatch_reservation_bytes"],
        "queue summary dispatch reservation bytes",
    )
    if (
        summary["schema_version"] != queue_runner.SCHEMA_VERSION
        or summary["runner_name"] != queue_runner.RUNNER_NAME
        or summary["implementation_version"] != queue_runner.IMPLEMENTATION_VERSION
        or summary["mode"] != expected_mode
        or summary["status"] not in expected_statuses
        or summary["bundle_id"] != manifest["bundle_id"]
        or summary["manifest_path"] != str(bundle["path"])
        or summary["manifest_sha256"] != sha256_bytes(bundle["body"])
        or summary["plan_id"] != manifest["plan"]["plan_id"]
        or summary["software"] != schedule["queue"]["software"]
        or summary["safety"] != queue_runner.RUNNER_SAFETY
        or not isinstance(results, list)
        or len(results) != len(orders)
        or job_count != len(orders)
    ):
        raise BackgroundProducerError(
            "queue summary differs from the sealed producer queue binding"
        )

    states: list[dict[str, Any] | None] = []
    for entry, order, raw_row in zip(
        manifest["work_orders"], orders, results, strict=True
    ):
        row = _exact_object(raw_row, "queue result summary row", QUEUE_RESULT_KEYS)
        failure_attempt_count = _integer(
            row["failure_attempt_count"],
            "queue result failure-attempt count",
            0,
            queue_runner.FAILURE_QUARANTINE_ATTEMPTS,
        )
        latest_failure = _failure_summary(
            row["latest_failure"], "queue result latest failure"
        )
        quarantine_receipt_sha256 = row["quarantine_receipt_sha256"]
        if quarantine_receipt_sha256 is not None:
            quarantine_receipt_sha256 = _sha256(
                quarantine_receipt_sha256,
                "queue result quarantine receipt SHA-256",
            )
        if (latest_failure is None) != (failure_attempt_count == 0):
            raise BackgroundProducerError(
                "queue result failure count differs from its latest failure"
            )
        if (
            latest_failure is not None
            and latest_failure["attempt_number"] != failure_attempt_count
        ):
            raise BackgroundProducerError(
                "queue result latest failure is not its final attempt"
            )
        if (
            row["ordinal"] != entry["queue_ordinal"]
            or row["job_id"] != entry["job_id"]
            or row["adapter"] != entry["adapter"]
            or row["work_order_sha256"]
            != sha256_bytes(canonical_bytes(order))
            or row["work_order_file_sha256"] != entry["sha256"]
            or row["reservation_bytes"] != queue_runner._reservation_bytes(order)
            or not isinstance(row["adapter_invoked"], bool)
            or not isinstance(row["action"], str)
            or not row["action"]
        ):
            raise BackgroundProducerError(
                "queue result summary row differs from its sealed work order"
            )
        if row["status"] == "pending":
            if any(
                row[key] is not None
                for key in ("result_sha256", "media_sha256", "media_byte_count")
            ):
                raise BackgroundProducerError(
                    "pending queue result summary contains completed-result metadata"
                )
            if quarantine_receipt_sha256 is not None:
                raise BackgroundProducerError(
                    "pending queue result summary contains quarantine metadata"
                )
            states.append(None)
            continue
        if row["status"] == "quarantined":
            if any(
                row[key] is not None
                for key in ("result_sha256", "media_sha256", "media_byte_count")
            ):
                raise BackgroundProducerError(
                    "quarantined queue result contains completed-result metadata"
                )
            if (
                failure_attempt_count != queue_runner.FAILURE_QUARANTINE_ATTEMPTS
                or latest_failure is None
                or quarantine_receipt_sha256 is None
            ):
                raise BackgroundProducerError(
                    "quarantined queue result has incomplete failure authority"
                )
            states.append(
                {
                    "queue_state": "quarantined",
                    "failure_attempt_count": failure_attempt_count,
                    "quarantine_receipt_sha256": quarantine_receipt_sha256,
                }
            )
            continue
        if row["status"] != "completed":
            raise BackgroundProducerError("queue result summary status is unsupported")
        if quarantine_receipt_sha256 is not None:
            raise BackgroundProducerError(
                "completed queue result contains quarantine metadata"
            )
        states.append(
            {
                "queue_state": "completed",
                "result_sha256": _sha256(
                    row["result_sha256"], "completed result SHA-256"
                ),
                "media_sha256": _sha256(
                    row["media_sha256"], "completed media SHA-256"
                ),
                "byte_count": _integer(
                    row["media_byte_count"], "completed media byte count", 1
                ),
            }
        )

    completed = sum(_completed_state(state) for state in states)
    quarantined = sum(_quarantined_state(state) for state in states)
    pending = len(states) - completed - quarantined
    retryable_failed = sum(
        row["status"] == "pending" and row["failure_attempt_count"] > 0
        for row in results
    )
    observed_failed_attempts = sum(row["failure_attempt_count"] for row in results)
    if (
        completed_count != completed
        or quarantined_count != quarantined
        or parked_count != quarantined
        or pending_count != pending
        or terminal_count != completed + quarantined
        or retryable_failed_count != retryable_failed
        or failed_attempt_count != observed_failed_attempts
        or completed_before + quarantined_before + pending_before != len(states)
        or completed_before + new_item_count != completed
        or quarantined_before + new_quarantined_count != quarantined
        or new_failed_attempt_count > failed_attempt_count
    ):
        raise BackgroundProducerError("queue summary counts differ from its result rows")
    if expected_mode == "validate" and (
        completed_before != completed
        or quarantined_before != quarantined
        or pending_before != pending
        or new_item_count != 0
        or new_byte_count != 0
        or new_failed_attempt_count != 0
        or new_quarantined_count != 0
        or adapter_invocation_count != 0
        or dispatch_reservation_bytes != 0
        or summary["limits"] is not None
        or summary["stop_reason"] is not None
    ):
        raise BackgroundProducerError("offline queue summary has invalid run accounting")
    return states


def _runtime_from_queue_summary(
    schedule: dict[str, Any],
    queue_summary: dict[str, Any],
    *,
    expected_mode: str,
    expected_statuses: set[str],
) -> tuple[dict[str, Any], list[dict[str, Any] | None], dict[str, Any]]:
    bundle = queue_runner._load_bundle(Path(schedule["queue"]["manifest_path"]))
    if (
        sha256_bytes(bundle["body"]) != schedule["queue"]["manifest_sha256"]
        or bundle["manifest"]["bundle_id"] != schedule["queue"]["bundle_id"]
    ):
        raise BackgroundProducerError("queue changed after schedule validation")
    states = _states_from_queue_summary(
        schedule,
        bundle,
        queue_summary,
        expected_mode=expected_mode,
        expected_statuses=expected_statuses,
    )
    acknowledged = _acknowledged_results(
        Path(schedule["consumer"]["preprocess_state_root"]),
        bundle=bundle,
        states=states,
    )
    return bundle, states, _ready_snapshot(schedule, bundle, states, acknowledged)


def _load_runtime(
    schedule: dict[str, Any],
) -> tuple[
    dict[str, Any],
    list[dict[str, Any] | None],
    dict[str, Any],
    dict[str, Any],
]:
    # The expensive snapshot is deliberately bounded to this synchronous call.  A
    # later process/invocation always starts with queue_runner's fresh two-pass replay.
    queue_summary = queue_runner.validate_queue(
        Path(schedule["queue"]["manifest_path"])
    )
    bundle, states, ready = _runtime_from_queue_summary(
        schedule,
        queue_summary,
        expected_mode="validate",
        expected_statuses={"validated"},
    )
    return bundle, states, ready, queue_summary


def _ready_snapshot(
    schedule: dict[str, Any],
    bundle: dict[str, Any],
    states: list[dict[str, Any] | None],
    acknowledged: set[str],
) -> dict[str, Any]:
    rows = []
    for entry, order, state in zip(
        bundle["manifest"]["work_orders"], bundle["orders"], states, strict=True
    ):
        if not _completed_state(state):
            continue
        result_path = str(queue_runner._result_path(order))
        if result_path in acknowledged:
            continue
        rows.append(
            {
                "ordinal": entry["queue_ordinal"],
                "job_id": entry["job_id"],
                "media_sha256": state["media_sha256"],
                "media_byte_count": state["byte_count"],
            }
        )
    items = len(rows)
    byte_count = sum(row["media_byte_count"] for row in rows)
    policy = schedule["policy"]
    if items >= policy["ready_high_items"] or byte_count >= policy["ready_high_bytes"]:
        zone = "at_or_above_high_water"
    elif items <= policy["ready_low_items"] and byte_count <= policy["ready_low_bytes"]:
        zone = "at_or_below_low_water"
    else:
        zone = "hysteresis_hold"
    return {
        "completed_acquisition_count": sum(_completed_state(state) for state in states),
        "quarantined_acquisition_count": sum(
            _quarantined_state(state) for state in states
        ),
        "acknowledged_preprocess_count": len(acknowledged),
        "ready_item_count": items,
        "ready_byte_count": byte_count,
        "zone": zone,
        "items": rows,
    }


def _run_limits(schedule: dict[str, Any], **raw: Any) -> dict[str, int]:
    policy = schedule["policy"]
    values = {
        "max_new_items": _integer(
            raw["max_new_items"],
            "--max-new-items",
            1,
            policy["maximum_dispatch_items_per_run"],
        ),
        "max_new_bytes": _integer(
            raw["max_new_bytes"],
            "--max-new-bytes",
            1,
            policy["maximum_dispatch_bytes_per_run"],
        ),
        "max_run_seconds": _integer(
            raw["max_run_seconds"],
            "--max-run-seconds",
            1,
            policy["maximum_run_seconds"],
        ),
        "free_space_floor_bytes": _integer(
            raw["free_space_floor_bytes"], "--free-space-floor-bytes"
        ),
    }
    if values["free_space_floor_bytes"] < policy["free_space_floor_bytes"]:
        raise BackgroundProducerError(
            "--free-space-floor-bytes may not weaken the sealed producer floor"
        )
    return values


def _dispatch_prefix(
    schedule: dict[str, Any],
    bundle: dict[str, Any],
    states: list[dict[str, Any] | None],
    ready: dict[str, Any],
    limits: dict[str, int],
) -> tuple[list[int], int, str | None]:
    if ready["zone"] != "at_or_below_low_water":
        return [], 0, ready["zone"]
    selected: list[int] = []
    reservations = 0
    policy = schedule["policy"]
    for entry, order, state in zip(
        bundle["manifest"]["work_orders"], bundle["orders"], states, strict=True
    ):
        if state is not None:
            continue
        reservation = queue_runner._reservation_bytes(order)
        if len(selected) >= limits["max_new_items"]:
            return selected, reservations, "max_new_items"
        if reservations + reservation > limits["max_new_bytes"]:
            return selected, reservations, "max_new_bytes"
        if ready["ready_item_count"] + len(selected) + 1 > policy["ready_high_items"]:
            return selected, reservations, "ready_high_items"
        if ready["ready_byte_count"] + reservations + reservation > policy[
            "ready_high_bytes"
        ]:
            return selected, reservations, "ready_high_bytes"
        selected.append(entry["queue_ordinal"])
        reservations += reservation
    return selected, reservations, None


def _summary(
    *,
    mode: str,
    status: str,
    schedule: dict[str, Any],
    schedule_path: Path,
    schedule_body: bytes,
    limits: dict[str, int] | None,
    ready_before: dict[str, Any],
    ready_after: dict[str, Any],
    planned_ordinals: list[int],
    planned_reservation_bytes: int,
    stop_reason: str | None,
    queue_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    core = {
        "schema_version": SCHEMA_VERSION,
        "producer": _producer_observation(),
        "mode": mode,
        "status": status,
        "schedule_id": schedule["schedule_id"],
        "schedule_path": str(schedule_path),
        "schedule_physical_sha256": sha256_bytes(schedule_body),
        "schedule_identity_sha256": schedule["identity_sha256"],
        "queue_bundle_id": schedule["queue"]["bundle_id"],
        "limits": limits,
        "ready_before": ready_before,
        "ready_after": ready_after,
        "planned_ordinals": planned_ordinals,
        "planned_reservation_bytes": planned_reservation_bytes,
        "stop_reason": stop_reason,
        "queue_summary": queue_summary,
        "safety": _runtime_safety(schedule),
    }
    return {**core, "summary_sha256": sha256_bytes(canonical_bytes(core))}


def validate_producer(schedule_path: Path) -> dict[str, Any]:
    schedule, resolved, body = load_schedule(schedule_path)
    _bundle, _states, ready, queue_summary = _load_runtime(schedule)
    return _summary(
        mode="validate",
        status="validated",
        schedule=schedule,
        schedule_path=resolved,
        schedule_body=body,
        limits=None,
        ready_before=ready,
        ready_after=ready,
        planned_ordinals=[],
        planned_reservation_bytes=0,
        stop_reason=None,
        queue_summary=queue_summary,
    )


def run_producer(
    schedule_path: Path,
    *,
    max_new_items: int,
    max_new_bytes: int,
    max_run_seconds: int,
    free_space_floor_bytes: int,
) -> dict[str, Any]:
    schedule, resolved, body = load_schedule(schedule_path)
    limits = _run_limits(
        schedule,
        max_new_items=max_new_items,
        max_new_bytes=max_new_bytes,
        max_run_seconds=max_run_seconds,
        free_space_floor_bytes=free_space_floor_bytes,
    )
    bundle, states, ready_before, _validation_summary = _load_runtime(schedule)
    planned, reservation_bytes, planning_stop = _dispatch_prefix(
        schedule, bundle, states, ready_before, limits
    )
    pending_count = sum(state is None for state in states)
    quarantined_count = sum(_quarantined_state(state) for state in states)
    if not planned:
        if pending_count == 0:
            status = "parked" if quarantined_count else "completed"
            stop_reason = (
                "all_runnable_work_exhausted_with_quarantine"
                if quarantined_count
                else "all_acquired"
            )
        else:
            status = "held"
            stop_reason = planning_stop or "no_dispatch_capacity"
        return _summary(
            mode="run",
            status=status,
            schedule=schedule,
            schedule_path=resolved,
            schedule_body=body,
            limits=limits,
            ready_before=ready_before,
            ready_after=ready_before,
            planned_ordinals=[],
            planned_reservation_bytes=0,
            stop_reason=stop_reason,
            queue_summary=None,
        )
    try:
        queue_summary = queue_runner.run_queue(
            bundle["path"],
            max_new_items=len(planned),
            max_new_bytes=reservation_bytes,
            max_run_seconds=limits["max_run_seconds"],
            free_space_floor_bytes=limits["free_space_floor_bytes"],
        )
    except queue_runner.QueueRunnerFailure as error:
        _bundle, _states, ready_after, _validation_summary = _load_runtime(schedule)
        summary = _summary(
            mode="run",
            status="failed",
            schedule=schedule,
            schedule_path=resolved,
            schedule_body=body,
            limits=limits,
            ready_before=ready_before,
            ready_after=ready_after,
            planned_ordinals=planned,
            planned_reservation_bytes=reservation_bytes,
            stop_reason="delegated_queue_failure",
            queue_summary=error.summary,
        )
        raise BackgroundProducerFailure(summary) from error

    _bundle, _final_states, ready_after = _runtime_from_queue_summary(
        schedule,
        queue_summary,
        expected_mode="run",
        expected_statuses={"bounded", "completed", "parked"},
    )
    final_quarantined = sum(
        _quarantined_state(state) for state in _final_states
    )
    status = (
        "parked"
        if queue_summary["pending_count"] == 0 and final_quarantined
        else "completed"
        if queue_summary["pending_count"] == 0
        else "bounded"
    )
    return _summary(
        mode="run",
        status=status,
        schedule=schedule,
        schedule_path=resolved,
        schedule_body=body,
        limits=limits,
        ready_before=ready_before,
        ready_after=ready_after,
        planned_ordinals=planned,
        planned_reservation_bytes=reservation_bytes,
        stop_reason=(
            "all_runnable_work_exhausted_with_quarantine"
            if status == "parked"
            else "all_acquired"
            if status == "completed"
            else queue_summary["stop_reason"] or planning_stop
        ),
        queue_summary=queue_summary,
    )


@contextmanager
def _writer_lock(root: Path) -> Iterator[None]:
    lock_path = root / ".background-producer-materializer.lock"
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as error:
        if error.errno in {errno.ELOOP, errno.EINVAL}:
            raise BackgroundProducerError("schedule writer lock is unsafe") from error
        raise
    with os.fdopen(descriptor, "a+b") as handle:
        if stat.S_IMODE(os.fstat(handle.fileno()).st_mode) != 0o600:
            raise BackgroundProducerError("schedule writer lock must have mode 0600")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise BackgroundProducerError("another schedule materializer holds the lock") from error
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _ensure_private_parent(path: Path) -> Path:
    parent = path.parent
    missing: list[Path] = []
    cursor = parent
    while not cursor.exists():
        missing.append(cursor)
        if cursor.parent == cursor:
            raise BackgroundProducerError("cannot find an existing schedule parent")
        cursor = cursor.parent
    if cursor.resolve(strict=True) != cursor or not cursor.is_dir():
        raise BackgroundProducerError("schedule parent traverses an unsafe path")
    for directory in reversed(missing):
        directory.mkdir(mode=0o700)
        directory.chmod(0o700)
    _safe_private_directory(parent, "schedule output parent")
    return parent


def _write_immutable(path: Path, body: bytes, label: str) -> None:
    path = _absolute_lexical_path(str(path), label)
    _reject_cold_path(path, label)
    parent = _ensure_private_parent(path)
    with _writer_lock(parent):
        if path.exists() or path.is_symlink():
            existing, _ = queue_runner._stable_read(
                path, maximum=max(len(body), 1), label=label, required_mode=0o400
            )
            if existing != body:
                raise BackgroundProducerError(f"existing {label} differs from exact replay")
            return
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.tmp-", dir=parent
        )
        temporary = Path(temporary_name)
        try:
            offset = 0
            while offset < len(body):
                offset += os.write(descriptor, body[offset:])
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            temporary.chmod(0o400)
            try:
                os.link(temporary, path)
            except FileExistsError as error:
                raise BackgroundProducerError(f"immutable {label} admission raced") from error
            directory_descriptor = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            temporary.unlink(missing_ok=True)


def _emit(value: dict[str, Any], report: str | None) -> None:
    body = pretty_bytes(value)
    if len(body) > MAX_REPORT_BYTES:
        raise BackgroundProducerError("producer report exceeds its byte cap")
    if report is not None:
        _write_immutable(Path(report), body, "background producer report")
    sys.stdout.buffer.write(body)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a finite backpressured cycle of one sealed public acquisition queue"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    materialize = commands.add_parser("materialize", help="seal a producer schedule offline")
    materialize.add_argument("--manifest", required=True)
    materialize.add_argument("--preprocess-state-root", required=True)
    materialize.add_argument("--output", required=True)
    materialize.add_argument("--ready-high-items", type=int, default=8)
    materialize.add_argument("--ready-low-items", type=int, default=4)
    materialize.add_argument("--ready-high-bytes", type=int, default=16 * 1024**3)
    materialize.add_argument("--ready-low-bytes", type=int, default=8 * 1024**3)
    materialize.add_argument("--maximum-dispatch-items-per-run", type=int, default=8)
    materialize.add_argument(
        "--maximum-dispatch-bytes-per-run", type=int, default=16 * 1024**3
    )
    materialize.add_argument("--maximum-run-seconds", type=int, default=14_400)
    materialize.add_argument("--free-space-floor-bytes", type=int, required=True)

    validate = commands.add_parser("validate", help="offline, read-only exact replay")
    validate.add_argument("--schedule", required=True)
    validate.add_argument("--report")

    run = commands.add_parser("run", help="run one finite foreground producer cycle")
    run.add_argument("--schedule", required=True)
    run.add_argument("--max-new-items", type=int)
    run.add_argument("--max-new-bytes", type=int)
    run.add_argument("--max-run-seconds", type=int)
    run.add_argument("--free-space-floor-bytes", type=int)
    run.add_argument("--report")
    return parser


def _error_document(error: Exception) -> dict[str, Any]:
    core = {
        "schema_version": SCHEMA_VERSION,
        "producer": _producer_observation(),
        "status": "failed",
        "error": {"type": type(error).__name__, "message": str(error)},
        "safety": RUNTIME_SAFETY,
    }
    return {**core, "summary_sha256": sha256_bytes(canonical_bytes(core))}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "materialize":
            schedule = build_schedule(
                manifest_path=Path(args.manifest),
                preprocess_state_root=Path(args.preprocess_state_root),
                ready_high_items=args.ready_high_items,
                ready_low_items=args.ready_low_items,
                ready_high_bytes=args.ready_high_bytes,
                ready_low_bytes=args.ready_low_bytes,
                maximum_dispatch_items_per_run=args.maximum_dispatch_items_per_run,
                maximum_dispatch_bytes_per_run=args.maximum_dispatch_bytes_per_run,
                maximum_run_seconds=args.maximum_run_seconds,
                free_space_floor_bytes=args.free_space_floor_bytes,
            )
            _write_immutable(
                Path(args.output), pretty_bytes(schedule), "background acquisition schedule"
            )
            sys.stdout.buffer.write(pretty_bytes(schedule))
            return 0

        schedule, _path, _body = load_schedule(Path(args.schedule))
        if args.command == "validate":
            _emit(validate_producer(Path(args.schedule)), args.report)
            return 0
        policy = schedule["policy"]
        result = run_producer(
            Path(args.schedule),
            max_new_items=(
                policy["maximum_dispatch_items_per_run"]
                if args.max_new_items is None
                else args.max_new_items
            ),
            max_new_bytes=(
                policy["maximum_dispatch_bytes_per_run"]
                if args.max_new_bytes is None
                else args.max_new_bytes
            ),
            max_run_seconds=(
                policy["maximum_run_seconds"]
                if args.max_run_seconds is None
                else args.max_run_seconds
            ),
            free_space_floor_bytes=(
                policy["free_space_floor_bytes"]
                if args.free_space_floor_bytes is None
                else args.free_space_floor_bytes
            ),
        )
        _emit(result, args.report)
        return 0
    except BackgroundProducerFailure as error:
        try:
            _emit(error.summary, getattr(args, "report", None))
        except (BackgroundProducerError, OSError):
            sys.stderr.buffer.write(pretty_bytes(error.summary))
        return 2
    except (
        BackgroundProducerError,
        queue_runner.QueueRunnerError,
        acquire.AcquisitionError,
        OSError,
    ) as error:
        sys.stderr.buffer.write(pretty_bytes(_error_document(error)))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
