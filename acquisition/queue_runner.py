#!/usr/bin/env python3
"""Validate or sequentially dispatch one sealed acquisition queue bundle.

The validator is offline and read-only.  A real run considers every work order in
its sealed ordinal order, skips only strictly revalidated completed results, and
delegates each pending order exclusively to :func:`acquire.run_acquisition`.
There is no credential, fallback, substitution, subset, catalog, publication,
export, identity, event, or deletion surface in this runner.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import stat
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

try:
    from . import acquire, materialize_queue
except ImportError:  # pragma: no cover - direct script execution
    import acquire  # type: ignore[no-redef]
    import materialize_queue  # type: ignore[no-redef]


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.2.0"
RUNNER_NAME = "himr-acquisition-queue-runner"
SUPPORTED_MATERIALIZER = {"name": "himr-queue-materializer", "version": "0.1.0"}

MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_WORK_ORDER_BYTES = 4 * 1024 * 1024
MAX_EXECUTABLE_BYTES = 256 * 1024 * 1024
MAX_ITEMS = 10_000
MAX_RUN_SECONDS = 7 * 24 * 60 * 60
MAX_INTEGER = 2**63 - 1

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
BUNDLE_ID_RE = re.compile(r"^acqbundle_[0-9a-f]{32}$")
PLAN_ID_RE = re.compile(r"^acqplan_[0-9a-f]{32}$")
JOB_ID_RE = re.compile(r"^acq-[0-9a-f]{32}-[0-9]{6}$")
FAILURE_STATE_DIRECTORY = ".queue-failure-state-v1"
FAILURE_ATTEMPT_RE = re.compile(r"^[0-9]{6}\.json$")
FAILURE_ORDINAL_RE = re.compile(r"^[0-9]{6}$")
FAILURE_QUARANTINE_ATTEMPTS = 3
MAX_FAILURE_RECEIPT_BYTES = 64 * 1024

MANIFEST_KEYS = {
    "bundle_id",
    "bundle_relative_path",
    "schema_version",
    "materializer",
    "plan",
    "policy",
    "safety",
    "work_order_count",
    "work_orders",
}
ENTRY_KEYS = {
    "queue_ordinal",
    "recording_id",
    "source_id",
    "job_id",
    "adapter",
    "path",
    "sha256",
    "byte_count",
}
EXPECTED_SAFETY = {
    "bundle_class": "private_acquisition_work_orders",
    "access_policy": "public_only",
    "publication_authority": "none",
    "credentials_allowed": False,
    "network_access_performed": False,
    "catalog_mutated": False,
    "selected_ready_candidates_only": True,
}
RUNNER_SAFETY = {
    "access_policy": "sealed_public_work_orders_only",
    "catalog_access": "forbidden",
    "catalog_writes": False,
    "credentials_used": False,
    "deletion_authority": "none",
    "dispatch_order": "sealed_ordinal_sequential_bounded_failure_isolation",
    "event_authority": "none",
    "export_authority": "none",
    "fallbacks_allowed": False,
    "identity_authority": "none",
    "maximum_concurrency": 1,
    "publication_authority": "none",
    "provider_failure_attempts_before_quarantine": FAILURE_QUARANTINE_ATTEMPTS,
    "quarantine_completion_authority": False,
    "quarantine_retry_contract": "future_sealed_queue_or_explicit_policy_only",
    "result_reuse": "strict_completed_envelope_and_payload_replay",
    "substitutions_allowed": False,
}

FAILURE_RECEIPT_SAFETY = {
    "catalog_mutated": False,
    "completed_result_authority": False,
    "credentials_used": False,
    "deletion_authority": "none",
    "event_authority": "none",
    "export_authority": "none",
    "publication_authority": "none",
}


class QueueRunnerError(RuntimeError):
    """A sealed input, strict replay, bound, or dispatch invariant failed."""


class QueueDeadlineError(QueueRunnerError):
    """The hard per-run wall-clock deadline interrupted an acquisition."""


class QueueRunnerFailure(QueueRunnerError):
    """Sequential dispatch failed after zero or more durable completions."""

    def __init__(self, summary: dict[str, Any]):
        failed = summary["failed_job"]
        super().__init__(
            f"acquisition queue failed at ordinal {failed['ordinal']}/"
            f"{summary['job_count']}: {failed['error']['message']}"
        )
        self.summary = summary


def canonical_bytes(value: Any) -> bytes:
    return materialize_queue.canonical_bytes(value)


def pretty_bytes(value: Any) -> bytes:
    return materialize_queue.pretty_bytes(value)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _exact_object(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise QueueRunnerError(f"{label} must be an object")
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    if missing or unknown:
        raise QueueRunnerError(
            f"{label} keys differ from the exact contract; "
            f"missing={missing}, unknown={unknown}"
        )
    return value


def _text(value: Any, label: str, maximum: int = 2_000) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise QueueRunnerError(f"{label} must be bounded non-empty text")
    return value


def _integer(
    value: Any, label: str, minimum: int = 0, maximum: int = MAX_INTEGER
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise QueueRunnerError(
            f"{label} must be an integer from {minimum} through {maximum}"
        )
    return value


def _sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise QueueRunnerError(f"{label} must be a lowercase SHA-256")
    return value


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        value.st_mode,
        value.st_nlink,
        value.st_uid,
    )


def _absolute_lexical_path(value: Any, label: str) -> Path:
    try:
        raw = os.fspath(value)
    except TypeError as error:
        raise QueueRunnerError(f"{label} must be path-like") from error
    if not isinstance(raw, str) or not raw or "\x00" in raw or "://" in raw:
        raise QueueRunnerError(f"{label} must be a bounded local path")
    return Path(os.path.abspath(raw))


def _stable_read(
    path: Path,
    *,
    maximum: int,
    label: str,
    required_mode: int | None = None,
) -> tuple[bytes, os.stat_result]:
    requested = _absolute_lexical_path(path, label)
    try:
        if requested.resolve(strict=True) != requested:
            raise QueueRunnerError(f"{label} may not traverse a symlink")
        path_before = requested.lstat()
    except QueueRunnerError:
        raise
    except OSError as error:
        raise QueueRunnerError(f"{label} cannot be inspected") from error
    if stat.S_ISLNK(path_before.st_mode):
        raise QueueRunnerError(f"{label} may not be a symlink")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(requested, flags)
    except OSError as error:
        raise QueueRunnerError(f"{label} cannot be opened safely") from error
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != os.getuid()
        ):
            raise QueueRunnerError(
                f"{label} must be an owner-controlled single-link regular file"
            )
        if not 1 <= before.st_size <= maximum:
            raise QueueRunnerError(f"{label} exceeds its {maximum}-byte cap")
        mode = stat.S_IMODE(before.st_mode)
        if required_mode is not None and mode != required_mode:
            raise QueueRunnerError(
                f"{label} must have mode {required_mode:04o}, observed {mode:04o}"
            )
        chunks: list[bytes] = []
        offset = 0
        while offset < before.st_size:
            chunk = os.pread(
                descriptor, min(1024 * 1024, before.st_size - offset), offset
            )
            if not chunk:
                raise QueueRunnerError(f"{label} ended during its stable read")
            chunks.append(chunk)
            offset += len(chunk)
        after = os.fstat(descriptor)
        try:
            path_after = requested.lstat()
        except OSError as error:
            raise QueueRunnerError(f"{label} changed during its stable read") from error
        if (
            stat.S_ISLNK(path_after.st_mode)
            or _stat_identity(path_before) != _stat_identity(before)
            or _stat_identity(before) != _stat_identity(after)
            or _stat_identity(after) != _stat_identity(path_after)
        ):
            raise QueueRunnerError(f"{label} changed during its stable read")
        return b"".join(chunks), after
    finally:
        os.close(descriptor)


def _safe_directory(path: Path, label: str, *, required_mode: int | None = None) -> None:
    requested = _absolute_lexical_path(path, label)
    try:
        if requested.resolve(strict=True) != requested:
            raise QueueRunnerError(f"{label} may not traverse a symlink")
        observed = requested.lstat()
    except QueueRunnerError:
        raise
    except OSError as error:
        raise QueueRunnerError(f"{label} cannot be inspected") from error
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.getuid()
    ):
        raise QueueRunnerError(f"{label} must be an owner-controlled real directory")
    mode = stat.S_IMODE(observed.st_mode)
    if required_mode is not None and mode != required_mode:
        raise QueueRunnerError(
            f"{label} must have mode {required_mode:04o}, observed {mode:04o}"
        )
    if required_mode is None and mode & 0o022:
        raise QueueRunnerError(f"{label} may not be group/other writable")


def _directory_names(path: Path, label: str) -> set[str]:
    try:
        entries = list(path.iterdir())
    except OSError as error:
        raise QueueRunnerError(f"{label} cannot be enumerated") from error
    if any(entry.is_symlink() for entry in entries):
        raise QueueRunnerError(f"{label} contains a symlink")
    return {entry.name for entry in entries}


def _strict_json(body: bytes, label: str) -> dict[str, Any]:
    try:
        value = materialize_queue.load_json_bytes(body, label)
    except materialize_queue.MaterializationError as error:
        raise QueueRunnerError(str(error)) from error
    if not isinstance(value, dict):
        raise QueueRunnerError(f"{label} must be an object")
    return value


def _validate_manifest(raw: dict[str, Any]) -> dict[str, Any]:
    manifest = _exact_object(raw, "queue bundle manifest", MANIFEST_KEYS)
    if manifest["schema_version"] != SCHEMA_VERSION:
        raise QueueRunnerError(f"queue schema_version must be {SCHEMA_VERSION}")
    bundle_id = _text(manifest["bundle_id"], "bundle_id", 42)
    if not BUNDLE_ID_RE.fullmatch(bundle_id):
        raise QueueRunnerError("bundle_id is invalid")
    if manifest["bundle_relative_path"] != f"bundles/{bundle_id}":
        raise QueueRunnerError("bundle_relative_path does not match bundle_id")
    if manifest["materializer"] != SUPPORTED_MATERIALIZER:
        raise QueueRunnerError("queue materializer name/version is unsupported")

    plan = _exact_object(
        manifest["plan"],
        "queue plan identity",
        {"plan_id", "canonical_sha256", "core_sha256", "planned_at"},
    )
    plan_id = _text(plan["plan_id"], "plan.plan_id", 40)
    if not PLAN_ID_RE.fullmatch(plan_id):
        raise QueueRunnerError("plan.plan_id is invalid")
    canonical_plan_sha = _sha256(plan["canonical_sha256"], "plan.canonical_sha256")
    core_plan_sha = _sha256(plan["core_sha256"], "plan.core_sha256")
    del canonical_plan_sha  # The bundle identity retains this opaque full-plan pin.
    if plan_id != materialize_queue.stable_id("acqplan", core_plan_sha):
        raise QueueRunnerError("plan.plan_id does not match plan.core_sha256")
    try:
        materialize_queue.validate_utc_timestamp(plan["planned_at"], "plan.planned_at")
    except materialize_queue.MaterializationError as error:
        raise QueueRunnerError(str(error)) from error

    policy = _exact_object(
        manifest["policy"],
        "queue policy",
        {
            "media_output_root",
            "max_job_bytes",
            "global_cache_cap_bytes",
            "free_space_floor_bytes",
            "direct_http_resume",
            "direct_http_timeout_seconds",
            "yt_dlp",
        },
    )
    output_root_text = _text(policy["media_output_root"], "policy.media_output_root", 4096)
    output_root = Path(output_root_text)
    if not output_root.is_absolute() or str(output_root) != output_root_text:
        raise QueueRunnerError("policy.media_output_root must be a lexical absolute path")
    try:
        acquire.validate_output_root(output_root)
    except acquire.AcquisitionError as error:
        raise QueueRunnerError(f"queue output policy is invalid: {error}") from error
    max_job = _integer(policy["max_job_bytes"], "policy.max_job_bytes", 1)
    global_cap = _integer(
        policy["global_cache_cap_bytes"], "policy.global_cache_cap_bytes", 1
    )
    if max_job > global_cap:
        raise QueueRunnerError("policy.max_job_bytes exceeds global_cache_cap_bytes")
    _integer(policy["free_space_floor_bytes"], "policy.free_space_floor_bytes")
    if policy["direct_http_resume"] is not True:
        raise QueueRunnerError("queue policy must retain direct HTTP resume")
    _integer(
        policy["direct_http_timeout_seconds"],
        "policy.direct_http_timeout_seconds",
        1,
        3600,
    )
    yt_dlp = _exact_object(
        policy["yt_dlp"],
        "queue yt-dlp policy",
        {"executable", "sha256", "byte_count", "format_selector"},
    )
    executable = _text(yt_dlp["executable"], "policy.yt_dlp.executable", 4096)
    if not Path(executable).is_absolute() or str(Path(executable)) != executable:
        raise QueueRunnerError("policy.yt_dlp.executable must be a lexical absolute path")
    _sha256(yt_dlp["sha256"], "policy.yt_dlp.sha256")
    _integer(yt_dlp["byte_count"], "policy.yt_dlp.byte_count", 1, MAX_EXECUTABLE_BYTES)
    selector = _text(yt_dlp["format_selector"], "policy.yt_dlp.format_selector", 256)
    if "\n" in selector or "\r" in selector or not acquire.safe_ytdlp_format_selector(selector):
        raise QueueRunnerError("policy.yt_dlp.format_selector is outside the safe policy")

    if manifest["safety"] != EXPECTED_SAFETY:
        raise QueueRunnerError("queue safety block differs from the sealed no-authority policy")
    count = _integer(manifest["work_order_count"], "work_order_count", 0, MAX_ITEMS)
    entries = manifest["work_orders"]
    if not isinstance(entries, list) or len(entries) != count:
        raise QueueRunnerError("work_orders length differs from work_order_count")
    seen_jobs: set[str] = set()
    seen_paths: set[str] = set()
    seen_sources: set[str] = set()
    for ordinal, entry_value in enumerate(entries, 1):
        entry = _exact_object(entry_value, f"work_orders[{ordinal - 1}]", ENTRY_KEYS)
        if entry["queue_ordinal"] != ordinal:
            raise QueueRunnerError("queue ordinals must be contiguous and one-based")
        _text(entry["recording_id"], f"work_orders[{ordinal - 1}].recording_id", 500)
        source_id = _text(
            entry["source_id"], f"work_orders[{ordinal - 1}].source_id", 500
        )
        expected_job = f"acq-{plan_id.removeprefix('acqplan_')}-{ordinal:06d}"
        if entry["job_id"] != expected_job or not JOB_ID_RE.fullmatch(expected_job):
            raise QueueRunnerError(f"work order {ordinal} job_id is not canonical")
        if entry["adapter"] not in {"direct_http", "yt_dlp"}:
            raise QueueRunnerError(f"work order {ordinal} adapter is unsupported")
        expected_path = f"work-orders/{ordinal:06d}.json"
        if entry["path"] != expected_path:
            raise QueueRunnerError(f"work order {ordinal} path is not canonical")
        _sha256(entry["sha256"], f"work_orders[{ordinal - 1}].sha256")
        _integer(
            entry["byte_count"],
            f"work_orders[{ordinal - 1}].byte_count",
            1,
            MAX_WORK_ORDER_BYTES,
        )
        if entry["job_id"] in seen_jobs or expected_path in seen_paths or source_id in seen_sources:
            raise QueueRunnerError("queue work-order job, path, and source IDs must be unique")
        seen_jobs.add(entry["job_id"])
        seen_paths.add(expected_path)
        seen_sources.add(source_id)

    core = {
        key: value
        for key, value in manifest.items()
        if key not in {"bundle_id", "bundle_relative_path"}
    }
    expected_bundle_id = materialize_queue.stable_id(
        "acqbundle", sha256_bytes(canonical_bytes(core))
    )
    if bundle_id != expected_bundle_id:
        raise QueueRunnerError("bundle_id does not match the canonical manifest core")
    return manifest


def _validate_order(
    body: bytes,
    *,
    entry: dict[str, Any],
    manifest: dict[str, Any],
) -> dict[str, Any]:
    raw = _strict_json(body, f"work order {entry['queue_ordinal']}")
    try:
        order = acquire.validate_work_order(raw)
    except acquire.AcquisitionError as error:
        raise QueueRunnerError(
            f"work order {entry['queue_ordinal']} failed guarded validation: {error}"
        ) from error
    if raw != order or body != pretty_bytes(order):
        raise QueueRunnerError(
            f"work order {entry['queue_ordinal']} is not in canonical materializer serialization"
        )
    if "handling_policy" in order:
        raise QueueRunnerError("public acquisition queues may not carry private-source substitutions")
    if order["job_id"] != entry["job_id"] or order["adapter"] != entry["adapter"]:
        raise QueueRunnerError(f"work order {entry['queue_ordinal']} differs from its manifest entry")
    if order["output"] != {"root": manifest["policy"]["media_output_root"]}:
        raise QueueRunnerError("work-order output root differs from queue policy")
    expected_limits = {
        key: manifest["policy"][key]
        for key in ("max_job_bytes", "global_cache_cap_bytes", "free_space_floor_bytes")
    }
    if order["limits"] != expected_limits or order["source"]["access_state"] != "public":
        raise QueueRunnerError("work order weakens the queue public-access/capacity policy")
    config = order["adapter_config"]
    if order["adapter"] == "direct_http":
        if config != {
            "url": config.get("url"),
            "resume": True,
            "timeout_seconds": manifest["policy"]["direct_http_timeout_seconds"],
            "expected_sha256": config.get("expected_sha256"),
            "expected_byte_count": config.get("expected_byte_count"),
        }:
            raise QueueRunnerError("direct HTTP work order differs from sealed queue policy")
    else:
        yt = manifest["policy"]["yt_dlp"]
        if config != {
            "url": config.get("url"),
            "executable": yt["executable"],
            "format_selector": yt["format_selector"],
            "expected_executable_sha256": yt["sha256"],
            "expected_sha256": config.get("expected_sha256"),
            "expected_byte_count": config.get("expected_byte_count"),
        }:
            raise QueueRunnerError("yt-dlp work order differs from sealed queue policy")
    if sha256_bytes(body) != entry["sha256"] or len(body) != entry["byte_count"]:
        raise QueueRunnerError(f"work order {entry['queue_ordinal']} physical pin differs")
    return order


def _validate_bundle_layout(path: Path, manifest: dict[str, Any]) -> None:
    bundle_dir = path.parent
    if path.name != "manifest.json" or bundle_dir.name != manifest["bundle_id"]:
        raise QueueRunnerError("--manifest must identify the canonical bundle manifest path")
    _safe_directory(bundle_dir, "queue bundle directory", required_mode=0o500)
    expected_root_names = {"manifest.json"}
    if manifest["work_order_count"]:
        expected_root_names.add("work-orders")
    if _directory_names(bundle_dir, "queue bundle directory") != expected_root_names:
        raise QueueRunnerError("sealed queue bundle has missing or extra entries")
    if manifest["work_order_count"]:
        work_orders = bundle_dir / "work-orders"
        _safe_directory(work_orders, "queue work-orders directory", required_mode=0o500)
        expected = {f"{ordinal:06d}.json" for ordinal in range(1, manifest["work_order_count"] + 1)}
        if _directory_names(work_orders, "queue work-orders directory") != expected:
            raise QueueRunnerError("sealed queue work-orders directory has missing or extra entries")


def _validate_executable(manifest: dict[str, Any]) -> dict[str, Any]:
    policy = manifest["policy"]["yt_dlp"]
    path = Path(policy["executable"])
    body, observed = _stable_read(
        path,
        maximum=MAX_EXECUTABLE_BYTES,
        label="sealed yt-dlp executable",
    )
    if not os.access(path, os.X_OK):
        raise QueueRunnerError("sealed yt-dlp executable is not executable")
    digest = sha256_bytes(body)
    if observed.st_size != policy["byte_count"] or digest != policy["sha256"]:
        raise QueueRunnerError("sealed yt-dlp executable differs from the queue pin")
    return {"sha256": digest, "byte_count": observed.st_size}


def _software_document() -> dict[str, Any]:
    components = (
        ("runner", Path(__file__).resolve(), IMPLEMENTATION_VERSION),
        ("acquire", Path(acquire.__file__).resolve(), acquire.IMPLEMENTATION_VERSION),
        (
            "materializer",
            Path(materialize_queue.__file__).resolve(),
            materialize_queue.IMPLEMENTATION_VERSION,
        ),
    )
    result: dict[str, Any] = {}
    for name, path, version in components:
        body, _ = _stable_read(path, maximum=4 * 1024 * 1024, label=f"{name} source")
        result[name] = {
            "name": path.name,
            "version": version,
            "sha256": sha256_bytes(body),
            "byte_count": len(body),
        }
    return result


def _load_bundle(manifest_path: Path) -> dict[str, Any]:
    path = _absolute_lexical_path(manifest_path, "--manifest")
    body, _ = _stable_read(
        path,
        maximum=MAX_MANIFEST_BYTES,
        label="queue bundle manifest",
        required_mode=0o400,
    )
    manifest = _validate_manifest(_strict_json(body, "queue bundle manifest"))
    if body != pretty_bytes(manifest):
        raise QueueRunnerError("queue bundle manifest is not in canonical serialization")
    _validate_bundle_layout(path, manifest)
    executable = _validate_executable(manifest)
    orders: list[dict[str, Any]] = []
    order_bodies: list[bytes] = []
    for entry in manifest["work_orders"]:
        order_path = path.parent / entry["path"]
        order_body, _ = _stable_read(
            order_path,
            maximum=MAX_WORK_ORDER_BYTES,
            label=f"sealed work order {entry['queue_ordinal']}",
            required_mode=0o400,
        )
        orders.append(_validate_order(order_body, entry=entry, manifest=manifest))
        order_bodies.append(order_body)
    replay, _ = _stable_read(
        path,
        maximum=MAX_MANIFEST_BYTES,
        label="queue bundle manifest replay",
        required_mode=0o400,
    )
    if replay != body:
        raise QueueRunnerError("queue bundle manifest changed during validation")
    return {
        "path": path,
        "body": body,
        "manifest": manifest,
        "orders": orders,
        "order_bodies": order_bodies,
        "executable": executable,
    }


def _result_path(order: dict[str, Any]) -> Path:
    work_order_sha = sha256_bytes(canonical_bytes(order))
    return (
        Path(order["output"]["root"])
        / "jobs"
        / order["job_id"]
        / work_order_sha
        / "result.json"
    )


def _safe_existing_result_parents(result_path: Path, output_root: Path) -> None:
    if not output_root.exists() and not output_root.is_symlink():
        return
    _safe_directory(output_root, "managed acquisition output root")
    current = output_root
    relative = result_path.relative_to(output_root)
    for component in relative.parts[:-1]:
        current = current / component
        if not current.exists() and not current.is_symlink():
            return
        _safe_directory(current, f"completed-result path component {component}")


def _inspect_result(order: dict[str, Any]) -> dict[str, Any] | None:
    output_root = Path(order["output"]["root"])
    path = _result_path(order)
    _safe_existing_result_parents(path, output_root)
    try:
        observed = path.lstat()
    except FileNotFoundError:
        if path.parent.exists() or path.parent.is_symlink():
            raise QueueRunnerError(
                f"result directory exists without result.json for {order['job_id']}"
            )
        return None
    except OSError as error:
        raise QueueRunnerError(f"completed result cannot be inspected for {order['job_id']}") from error
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or observed.st_nlink != 1
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) & 0o022
    ):
        raise QueueRunnerError("completed result must be owner-controlled and non-writable by peers")
    if _directory_names(path.parent, "completed result directory") != {"result.json"}:
        raise QueueRunnerError("completed result directory has missing or extra entries")
    before, _ = _stable_read(
        path,
        maximum=acquire.MAX_DURABLE_RESULT_BYTES,
        label="completed acquisition result",
    )
    try:
        raw = acquire.strict_json_object(before, "completed acquisition result")
        validated = acquire.load_reusable_result(path, output_root, order)
    except acquire.AcquisitionError as error:
        raise QueueRunnerError(f"completed result failed immutable reuse: {error}") from error
    if validated is None:
        raise QueueRunnerError("completed result failed immutable envelope/payload validation")
    after, _ = _stable_read(
        path,
        maximum=acquire.MAX_DURABLE_RESULT_BYTES,
        label="completed acquisition result replay",
    )
    if (
        before != after
        or raw != validated
        or before != acquire.pretty_json(validated).encode("utf-8")
        or _directory_names(path.parent, "completed result directory replay")
        != {"result.json"}
    ):
        raise QueueRunnerError("completed result changed across strict immutable replay")
    admission = validated["admission"]
    return {
        "result": validated,
        "result_sha256": sha256_bytes(before),
        "media_sha256": admission["sha256"],
        "byte_count": admission["byte_count"],
    }


def _scan_results(bundle: dict[str, Any]) -> list[dict[str, Any] | None]:
    return [_inspect_result(order) for order in bundle["orders"]]


def _failure_state_root(bundle: dict[str, Any]) -> Path:
    return (
        Path(bundle["manifest"]["policy"]["media_output_root"])
        / FAILURE_STATE_DIRECTORY
        / bundle["manifest"]["bundle_id"]
    )


def _failure_order_root(bundle: dict[str, Any], ordinal: int) -> Path:
    return _failure_state_root(bundle) / "ordinals" / f"{ordinal:06d}"


def _empty_failure_state() -> dict[str, Any]:
    return {"attempts": [], "quarantine": None}


def _receipt_digest(value: dict[str, Any]) -> str:
    core = {key: item for key, item in value.items() if key != "receipt_sha256"}
    return sha256_bytes(canonical_bytes(core))


def _failure_binding(
    bundle: dict[str, Any], entry: dict[str, Any], order: dict[str, Any]
) -> dict[str, Any]:
    return {
        "bundle_id": bundle["manifest"]["bundle_id"],
        "manifest_sha256": sha256_bytes(bundle["body"]),
        "ordinal": entry["queue_ordinal"],
        "job_id": entry["job_id"],
        "work_order_sha256": sha256_bytes(canonical_bytes(order)),
    }


def _validate_failure_attempt(
    raw: Any,
    *,
    bundle: dict[str, Any],
    entry: dict[str, Any],
    order: dict[str, Any],
    attempt_number: int,
) -> dict[str, Any]:
    receipt = _exact_object(
        raw,
        "acquisition failure-attempt receipt",
        {
            "schema_version",
            "receipt_kind",
            "runner_name",
            "receipt_contract_version",
            "bundle_id",
            "manifest_sha256",
            "ordinal",
            "job_id",
            "work_order_sha256",
            "attempt_number",
            "observed_at",
            "error",
            "safety",
            "receipt_sha256",
        },
    )
    if (
        receipt["schema_version"] != SCHEMA_VERSION
        or receipt["receipt_kind"] != "public_acquisition_failed_attempt"
        or receipt["runner_name"] != RUNNER_NAME
        or receipt["receipt_contract_version"] != 1
        or {
            key: receipt[key]
            for key in (
                "bundle_id",
                "manifest_sha256",
                "ordinal",
                "job_id",
                "work_order_sha256",
            )
        }
        != _failure_binding(bundle, entry, order)
        or receipt["attempt_number"] != attempt_number
        or receipt["safety"] != FAILURE_RECEIPT_SAFETY
    ):
        raise QueueRunnerError("failure-attempt receipt differs from its sealed order")
    try:
        materialize_queue.validate_utc_timestamp(
            receipt["observed_at"], "failure-attempt observed_at"
        )
    except materialize_queue.MaterializationError as error:
        raise QueueRunnerError(str(error)) from error
    failure = _exact_object(receipt["error"], "failure-attempt error", {"type", "message"})
    _text(failure["type"], "failure-attempt error type", 200)
    _text(failure["message"], "failure-attempt error message", 8_192)
    digest = _sha256(receipt["receipt_sha256"], "failure-attempt receipt SHA-256")
    if digest != _receipt_digest(receipt):
        raise QueueRunnerError("failure-attempt receipt identity is inconsistent")
    return receipt


def _validate_quarantine_receipt(
    raw: Any,
    *,
    bundle: dict[str, Any],
    entry: dict[str, Any],
    order: dict[str, Any],
    attempts: list[dict[str, Any]],
) -> dict[str, Any]:
    receipt = _exact_object(
        raw,
        "acquisition quarantine receipt",
        {
            "schema_version",
            "receipt_kind",
            "runner_name",
            "receipt_contract_version",
            "bundle_id",
            "manifest_sha256",
            "ordinal",
            "job_id",
            "work_order_sha256",
            "failure_attempt_limit",
            "attempt_receipts",
            "quarantined_at",
            "retry_contract",
            "safety",
            "receipt_sha256",
        },
    )
    expected_refs = [
        {
            "attempt_number": attempt["attempt_number"],
            "receipt_sha256": attempt["receipt_sha256"],
            "error": attempt["error"],
        }
        for attempt in attempts
    ]
    if (
        receipt["schema_version"] != SCHEMA_VERSION
        or receipt["receipt_kind"] != "public_acquisition_quarantine"
        or receipt["runner_name"] != RUNNER_NAME
        or receipt["receipt_contract_version"] != 1
        or {
            key: receipt[key]
            for key in (
                "bundle_id",
                "manifest_sha256",
                "ordinal",
                "job_id",
                "work_order_sha256",
            )
        }
        != _failure_binding(bundle, entry, order)
        or receipt["failure_attempt_limit"] != FAILURE_QUARANTINE_ATTEMPTS
        or len(attempts) != FAILURE_QUARANTINE_ATTEMPTS
        or receipt["attempt_receipts"] != expected_refs
        or receipt["quarantined_at"] != attempts[-1]["observed_at"]
        or receipt["retry_contract"]
        != "future_sealed_queue_or_explicit_policy_only"
        or receipt["safety"] != FAILURE_RECEIPT_SAFETY
    ):
        raise QueueRunnerError("quarantine receipt differs from its failure history")
    digest = _sha256(receipt["receipt_sha256"], "quarantine receipt SHA-256")
    if digest != _receipt_digest(receipt):
        raise QueueRunnerError("quarantine receipt identity is inconsistent")
    return receipt


def _inspect_failure_state(
    bundle: dict[str, Any],
    entry: dict[str, Any],
    order: dict[str, Any],
) -> dict[str, Any]:
    order_root = _failure_order_root(bundle, entry["queue_ordinal"])
    if not order_root.exists() and not order_root.is_symlink():
        return _empty_failure_state()
    _safe_directory(order_root, "failure ordinal directory", required_mode=0o700)
    order_names = _directory_names(order_root, "failure ordinal directory")
    if "attempts" not in order_names or not order_names <= {"attempts", "quarantine.json"}:
        raise QueueRunnerError("failure ordinal directory has missing or extra entries")
    attempts_root = order_root / "attempts"
    _safe_directory(attempts_root, "failure attempts directory", required_mode=0o700)
    attempt_names = _directory_names(attempts_root, "failure attempts directory")
    expected_names = {
        f"{number:06d}.json" for number in range(1, len(attempt_names) + 1)
    }
    if (
        attempt_names != expected_names
        or len(attempt_names) < 1
        or len(attempt_names) > FAILURE_QUARANTINE_ATTEMPTS
        or any(not FAILURE_ATTEMPT_RE.fullmatch(name) for name in attempt_names)
    ):
        raise QueueRunnerError("failure-attempt receipts are not a bounded contiguous sequence")
    attempts = []
    for number in range(1, len(attempt_names) + 1):
        path = attempts_root / f"{number:06d}.json"
        body, _ = _stable_read(
            path,
            maximum=MAX_FAILURE_RECEIPT_BYTES,
            label="failure-attempt receipt",
            required_mode=0o400,
        )
        receipt = _validate_failure_attempt(
            _strict_json(body, "failure-attempt receipt"),
            bundle=bundle,
            entry=entry,
            order=order,
            attempt_number=number,
        )
        if body != pretty_bytes(receipt):
            raise QueueRunnerError("failure-attempt receipt is not canonical")
        attempts.append(receipt)
    quarantine = None
    quarantine_path = order_root / "quarantine.json"
    if "quarantine.json" in order_names:
        body, _ = _stable_read(
            quarantine_path,
            maximum=MAX_FAILURE_RECEIPT_BYTES,
            label="quarantine receipt",
            required_mode=0o400,
        )
        quarantine = _validate_quarantine_receipt(
            _strict_json(body, "quarantine receipt"),
            bundle=bundle,
            entry=entry,
            order=order,
            attempts=attempts,
        )
        if body != pretty_bytes(quarantine):
            raise QueueRunnerError("quarantine receipt is not canonical")
    return {"attempts": attempts, "quarantine": quarantine}


def _scan_failure_states(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    root = _failure_state_root(bundle)
    if root.exists() or root.is_symlink():
        _safe_directory(root, "queue failure-state directory", required_mode=0o700)
        names = _directory_names(root, "queue failure-state directory")
        if names != {"ordinals"}:
            raise QueueRunnerError("queue failure-state directory has missing or extra entries")
        ordinals = root / "ordinals"
        _safe_directory(ordinals, "failure ordinals directory", required_mode=0o700)
        ordinal_names = _directory_names(ordinals, "failure ordinals directory")
        allowed = {
            f"{entry['queue_ordinal']:06d}" for entry in bundle["manifest"]["work_orders"]
        }
        if not ordinal_names <= allowed or any(
            not FAILURE_ORDINAL_RE.fullmatch(name) for name in ordinal_names
        ):
            raise QueueRunnerError("failure ordinals directory contains an unknown ordinal")
    return [
        _inspect_failure_state(bundle, entry, order)
        for entry, order in zip(
            bundle["manifest"]["work_orders"], bundle["orders"], strict=True
        )
    ]


def _ensure_private_directory(path: Path, label: str) -> None:
    missing: list[Path] = []
    cursor = path
    while not cursor.exists() and not cursor.is_symlink():
        missing.append(cursor)
        if cursor.parent == cursor:
            raise QueueRunnerError(f"cannot create {label} without an existing parent")
        cursor = cursor.parent
    if cursor.is_symlink() or not cursor.is_dir():
        raise QueueRunnerError(f"{label} traverses an unsafe path")
    if cursor.resolve(strict=True) != cursor:
        raise QueueRunnerError(f"{label} may not traverse a symlink")
    _safe_directory(cursor, f"existing parent for {label}")
    for directory in reversed(missing):
        try:
            directory.mkdir(mode=0o700)
        except FileExistsError:
            pass
        _safe_directory(directory, label, required_mode=0o700)
    _safe_directory(path, label, required_mode=0o700)


def _write_immutable_receipt(path: Path, receipt: dict[str, Any], label: str) -> None:
    body = pretty_bytes(receipt)
    if len(body) > MAX_FAILURE_RECEIPT_BYTES:
        raise QueueRunnerError(f"{label} exceeds its byte cap")
    _ensure_private_directory(path.parent, f"{label} parent")
    if path.exists() or path.is_symlink():
        existing, _ = _stable_read(
            path,
            maximum=MAX_FAILURE_RECEIPT_BYTES,
            label=label,
            required_mode=0o400,
        )
        if existing != body:
            raise QueueRunnerError(f"existing {label} differs from exact replay")
        return
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.tmp-", dir=path.parent)
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
            raise QueueRunnerError(f"immutable {label} admission raced") from error
        directory_descriptor = os.open(
            path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _build_failure_attempt(
    bundle: dict[str, Any],
    entry: dict[str, Any],
    order: dict[str, Any],
    *,
    attempt_number: int,
    error: acquire.AcquisitionError,
) -> dict[str, Any]:
    message = str(error)
    if not message:
        message = "acquisition adapter failed without a message"
    message = message.replace("\x00", "\N{REPLACEMENT CHARACTER}")[:8_192]
    core = {
        "schema_version": SCHEMA_VERSION,
        "receipt_kind": "public_acquisition_failed_attempt",
        "runner_name": RUNNER_NAME,
        "receipt_contract_version": 1,
        **_failure_binding(bundle, entry, order),
        "attempt_number": attempt_number,
        "observed_at": acquire.utc_now(),
        "error": {"type": type(error).__name__, "message": message},
        "safety": FAILURE_RECEIPT_SAFETY,
    }
    return {**core, "receipt_sha256": sha256_bytes(canonical_bytes(core))}


def _record_failure_attempt(
    bundle: dict[str, Any],
    entry: dict[str, Any],
    order: dict[str, Any],
    previous: dict[str, Any],
    error: acquire.AcquisitionError,
) -> dict[str, Any]:
    attempt_number = len(previous["attempts"]) + 1
    if attempt_number > FAILURE_QUARANTINE_ATTEMPTS:
        raise QueueRunnerError("failure-attempt limit was exceeded without quarantine")
    receipt = _build_failure_attempt(
        bundle,
        entry,
        order,
        attempt_number=attempt_number,
        error=error,
    )
    path = _failure_order_root(bundle, entry["queue_ordinal"]) / "attempts" / f"{attempt_number:06d}.json"
    _write_immutable_receipt(path, receipt, "failure-attempt receipt")
    return _inspect_failure_state(bundle, entry, order)


def _seal_quarantine(
    bundle: dict[str, Any],
    entry: dict[str, Any],
    order: dict[str, Any],
    failure_state: dict[str, Any],
) -> dict[str, Any]:
    attempts = failure_state["attempts"]
    if len(attempts) != FAILURE_QUARANTINE_ATTEMPTS:
        raise QueueRunnerError("quarantine requires the exact failure-attempt limit")
    core = {
        "schema_version": SCHEMA_VERSION,
        "receipt_kind": "public_acquisition_quarantine",
        "runner_name": RUNNER_NAME,
        "receipt_contract_version": 1,
        **_failure_binding(bundle, entry, order),
        "failure_attempt_limit": FAILURE_QUARANTINE_ATTEMPTS,
        "attempt_receipts": [
            {
                "attempt_number": attempt["attempt_number"],
                "receipt_sha256": attempt["receipt_sha256"],
                "error": attempt["error"],
            }
            for attempt in attempts
        ],
        "quarantined_at": attempts[-1]["observed_at"],
        "retry_contract": "future_sealed_queue_or_explicit_policy_only",
        "safety": FAILURE_RECEIPT_SAFETY,
    }
    receipt = {**core, "receipt_sha256": sha256_bytes(canonical_bytes(core))}
    path = _failure_order_root(bundle, entry["queue_ordinal"]) / "quarantine.json"
    _write_immutable_receipt(path, receipt, "quarantine receipt")
    return _inspect_failure_state(bundle, entry, order)


def _replay_bundle(bundle: dict[str, Any], software: dict[str, Any]) -> dict[str, Any]:
    replay = _load_bundle(bundle["path"])
    if (
        replay["path"] != bundle["path"]
        or replay["body"] != bundle["body"]
        or replay["manifest"] != bundle["manifest"]
        or replay["orders"] != bundle["orders"]
        or replay["order_bodies"] != bundle["order_bodies"]
        or replay["executable"] != bundle["executable"]
        or _software_document() != software
    ):
        raise QueueRunnerError("queue bundle or acquisition software changed during dispatch")
    return replay


def _reservation_bytes(order: dict[str, Any]) -> int:
    return order["limits"]["max_job_bytes"]


def _result_action(
    *,
    entry: dict[str, Any],
    order: dict[str, Any],
    state: dict[str, Any] | None,
    failure_state: dict[str, Any],
    action: str,
    adapter_invoked: bool,
) -> dict[str, Any]:
    quarantine = failure_state["quarantine"]
    latest_failure = (
        None
        if not failure_state["attempts"]
        else {
            "attempt_number": failure_state["attempts"][-1]["attempt_number"],
            "observed_at": failure_state["attempts"][-1]["observed_at"],
            "error": failure_state["attempts"][-1]["error"],
            "receipt_sha256": failure_state["attempts"][-1]["receipt_sha256"],
        }
    )
    status = (
        "completed"
        if state is not None
        else "quarantined"
        if quarantine is not None
        else "pending"
    )
    return {
        "ordinal": entry["queue_ordinal"],
        "job_id": entry["job_id"],
        "adapter": entry["adapter"],
        "status": status,
        "action": action,
        "adapter_invoked": adapter_invoked,
        "work_order_sha256": sha256_bytes(canonical_bytes(order)),
        "work_order_file_sha256": entry["sha256"],
        "reservation_bytes": _reservation_bytes(order),
        "failure_attempt_count": len(failure_state["attempts"]),
        "latest_failure": latest_failure,
        "quarantine_receipt_sha256": (
            None if quarantine is None else quarantine["receipt_sha256"]
        ),
        "result_sha256": None if state is None else state["result_sha256"],
        "media_sha256": None if state is None else state["media_sha256"],
        "media_byte_count": None if state is None else state["byte_count"],
    }


def _summary(
    *,
    bundle: dict[str, Any],
    software: dict[str, Any],
    mode: str,
    status: str,
    limits: dict[str, int] | None,
    initial: list[dict[str, Any] | None],
    final: list[dict[str, Any] | None],
    initial_failures: list[dict[str, Any]],
    final_failures: list[dict[str, Any]],
    actions: dict[int, tuple[str, bool]],
    adapter_invocation_count: int,
    dispatch_reservation_bytes: int,
    stop_reason: str | None,
    failed_job: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = bundle["manifest"]
    results = []
    for entry, order, state, failure_state in zip(
        manifest["work_orders"],
        bundle["orders"],
        final,
        final_failures,
        strict=True,
    ):
        action, invoked = actions[entry["queue_ordinal"]]
        results.append(
            _result_action(
                entry=entry,
                order=order,
                state=state,
                failure_state=failure_state,
                action=action,
                adapter_invoked=invoked,
            )
        )
    completed_before = sum(value is not None for value in initial)
    quarantined_before = sum(
        result is None and failure["quarantine"] is not None
        for result, failure in zip(initial, initial_failures, strict=True)
    )
    completed = sum(value is not None for value in final)
    quarantined = sum(
        result is None and failure["quarantine"] is not None
        for result, failure in zip(final, final_failures, strict=True)
    )
    failed_attempts_before = sum(len(value["attempts"]) for value in initial_failures)
    failed_attempts = sum(len(value["attempts"]) for value in final_failures)
    core: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "runner_name": RUNNER_NAME,
        "implementation_version": IMPLEMENTATION_VERSION,
        "mode": mode,
        "status": status,
        "bundle_id": manifest["bundle_id"],
        "manifest_path": str(bundle["path"]),
        "manifest_sha256": sha256_bytes(bundle["body"]),
        "plan_id": manifest["plan"]["plan_id"],
        "job_count": len(final),
        "completed_before": completed_before,
        "quarantined_before": quarantined_before,
        "pending_before": len(initial) - completed_before - quarantined_before,
        "completed_count": completed,
        "quarantined_count": quarantined,
        "parked_count": quarantined,
        "pending_count": len(final) - completed - quarantined,
        "terminal_count": completed + quarantined,
        "retryable_failed_count": sum(
            result is None
            and failure["quarantine"] is None
            and bool(failure["attempts"])
            for result, failure in zip(final, final_failures, strict=True)
        ),
        "failed_attempt_count": failed_attempts,
        "new_failed_attempt_count": failed_attempts - failed_attempts_before,
        "new_quarantined_count": sum(
            before_result is None
            and before_failure["quarantine"] is None
            and after_result is None
            and after_failure["quarantine"] is not None
            for before_result, before_failure, after_result, after_failure in zip(
                initial, initial_failures, final, final_failures, strict=True
            )
        ),
        "new_item_count": sum(
            before is None and after is not None
            for before, after in zip(initial, final, strict=True)
        ),
        "new_byte_count": sum(
            after["byte_count"]
            for before, after in zip(initial, final, strict=True)
            if before is None and after is not None
        ),
        "adapter_invocation_count": adapter_invocation_count,
        "dispatch_reservation_bytes": dispatch_reservation_bytes,
        "limits": limits,
        "stop_reason": stop_reason,
        "results": results,
        "software": software,
        "safety": RUNNER_SAFETY,
    }
    if failed_job is not None:
        core["failed_job"] = failed_job
    return {**core, "summary_sha256": sha256_bytes(canonical_bytes(core))}


def validate_queue(manifest_path: Path) -> dict[str, Any]:
    software = _software_document()
    bundle = _load_bundle(manifest_path)
    states = _scan_results(bundle)
    failure_states = _scan_failure_states(bundle)
    bundle = _replay_bundle(bundle, software)
    replayed_states = _scan_results(bundle)
    replayed_failures = _scan_failure_states(bundle)
    if replayed_states != states or replayed_failures != failure_states:
        raise QueueRunnerError(
            "completed/pending/quarantine state changed during offline validation"
        )
    actions = {
        ordinal: (
            "validated_reuse"
            if state is not None
            else "validated_quarantine"
            if failure["quarantine"] is not None
            else "validated_retryable_failure"
            if failure["attempts"]
            else "validated_pending",
            False,
        )
        for ordinal, (state, failure) in enumerate(
            zip(states, failure_states, strict=True), 1
        )
    }
    return _summary(
        bundle=bundle,
        software=software,
        mode="validate",
        status="validated",
        limits=None,
        initial=states,
        final=replayed_states,
        initial_failures=failure_states,
        final_failures=replayed_failures,
        actions=actions,
        adapter_invocation_count=0,
        dispatch_reservation_bytes=0,
        stop_reason=None,
    )


def _run_limits(
    bundle: dict[str, Any],
    states: list[dict[str, Any] | None],
    failure_states: list[dict[str, Any]],
    *,
    max_new_items: Any,
    max_new_bytes: Any,
    max_run_seconds: Any,
    free_space_floor_bytes: Any,
) -> dict[str, int]:
    pending_orders = [
        order
        for order, state, failure in zip(
            bundle["orders"], states, failure_states, strict=True
        )
        if state is None and failure["quarantine"] is None
    ]
    pending_count = len(pending_orders)
    max_possible_bytes = sum(_reservation_bytes(order) for order in pending_orders)
    values = {
        "max_new_items": _integer(max_new_items, "--max-new-items", 0, pending_count),
        "max_new_bytes": _integer(max_new_bytes, "--max-new-bytes", 0, max_possible_bytes),
        "max_run_seconds": _integer(
            max_run_seconds, "--max-run-seconds", 1, MAX_RUN_SECONDS
        ),
        "free_space_floor_bytes": _integer(
            free_space_floor_bytes, "--free-space-floor-bytes"
        ),
    }
    sealed_floor = bundle["manifest"]["policy"]["free_space_floor_bytes"]
    if values["free_space_floor_bytes"] < sealed_floor:
        raise QueueRunnerError(
            "--free-space-floor-bytes may not weaken the sealed queue floor"
        )
    return values


def _capacity_allows(
    order: dict[str, Any], *, free_space_floor_bytes: int
) -> bool:
    limits = {
        **order["limits"],
        "free_space_floor_bytes": free_space_floor_bytes,
    }
    snapshot = acquire.capacity_snapshot(
        Path(order["output"]["root"]),
        limits,
        reserve_bytes=_reservation_bytes(order),
        enforce=False,
    )
    if snapshot["projected_managed_bytes"] > limits["global_cache_cap_bytes"]:
        raise QueueRunnerError("sealed global cache cap cannot admit the next ordinal")
    return snapshot["projected_free_bytes"] >= free_space_floor_bytes


@contextmanager
def _hard_deadline(seconds: float) -> Iterator[None]:
    if seconds <= 0:
        raise QueueDeadlineError("the per-run acquisition deadline has elapsed")
    if (
        threading.current_thread() is not threading.main_thread()
        or not hasattr(signal, "setitimer")
        or not hasattr(signal, "SIGALRM")
    ):
        raise QueueRunnerError("hard acquisition deadlines require the POSIX main thread")
    if signal.getitimer(signal.ITIMER_REAL)[0] > 0:
        raise QueueRunnerError("an existing process alarm prevents a safe queue deadline")

    def expire(_signum: int, _frame: Any) -> None:
        raise QueueDeadlineError("the per-run acquisition deadline elapsed during dispatch")

    previous = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def _validate_adapter_return(
    returned: Any,
    persisted: dict[str, Any],
) -> str:
    durable = persisted["result"]
    if returned == durable:
        return "completed"
    if not isinstance(returned, dict) or "reuse_verified_at" not in returned:
        raise QueueRunnerError("acquire return differs from its durable completed result")
    replayed = dict(returned)
    reuse_timestamp = replayed.pop("reuse_verified_at")
    try:
        materialize_queue.validate_utc_timestamp(
            reuse_timestamp, "acquire reuse_verified_at"
        )
    except materialize_queue.MaterializationError as error:
        raise QueueRunnerError(str(error)) from error
    replayed["reused"] = durable["reused"]
    if returned.get("reused") is not True or replayed != durable:
        raise QueueRunnerError("acquire reuse return differs from its durable result")
    return "reused_after_dispatch"


def _dispatch_one(order: dict[str, Any], remaining_seconds: float) -> tuple[dict[str, Any], str]:
    with _hard_deadline(remaining_seconds):
        returned = acquire.run_acquisition(order, dry_run=False)
    persisted = _inspect_result(order)
    if persisted is None:
        raise QueueRunnerError("acquire returned without a durable completed result")
    return persisted, _validate_adapter_return(returned, persisted)


def run_queue(
    manifest_path: Path,
    *,
    max_new_items: int,
    max_new_bytes: int,
    max_run_seconds: int,
    free_space_floor_bytes: int,
) -> dict[str, Any]:
    software = _software_document()
    bundle = _load_bundle(manifest_path)
    initial = _scan_results(bundle)
    initial_failures = _scan_failure_states(bundle)
    limits = _run_limits(
        bundle,
        initial,
        initial_failures,
        max_new_items=max_new_items,
        max_new_bytes=max_new_bytes,
        max_run_seconds=max_run_seconds,
        free_space_floor_bytes=free_space_floor_bytes,
    )
    states = list(initial)
    failure_states = list(initial_failures)
    actions: dict[int, tuple[str, bool]] = {
        ordinal: (
            "reused"
            if state is not None
            else "quarantined"
            if failure["quarantine"] is not None
            else "retryable_failure_pending"
            if failure["attempts"]
            else "pending",
            False,
        )
        for ordinal, (state, failure) in enumerate(
            zip(states, failure_states, strict=True), 1
        )
    }
    adapter_invocations = 0
    reserved_bytes = 0
    stop_reason: str | None = None
    started = time.monotonic()

    for index, (entry, initial_order) in enumerate(
        zip(bundle["manifest"]["work_orders"], bundle["orders"], strict=True)
    ):
        if states[index] is not None or failure_states[index]["quarantine"] is not None:
            continue
        if len(failure_states[index]["attempts"]) == FAILURE_QUARANTINE_ATTEMPTS:
            bundle = _replay_bundle(bundle, software)
            failure_states[index] = _seal_quarantine(
                bundle, entry, bundle["orders"][index], failure_states[index]
            )
            actions[entry["queue_ordinal"]] = ("recovered_quarantine", False)
            continue
        reservation = _reservation_bytes(initial_order)
        elapsed = time.monotonic() - started
        if adapter_invocations >= limits["max_new_items"]:
            stop_reason = "max_new_items"
            break
        if reserved_bytes + reservation > limits["max_new_bytes"]:
            stop_reason = "max_new_bytes"
            break
        if elapsed >= limits["max_run_seconds"]:
            stop_reason = "max_run_seconds"
            break
        fatal_error: Exception | None = None
        try:
            if not _capacity_allows(
                initial_order,
                free_space_floor_bytes=limits["free_space_floor_bytes"],
            ):
                stop_reason = "free_space_floor"
                break
            replay = _replay_bundle(bundle, software)
            order = replay["orders"][index]
            remaining = limits["max_run_seconds"] - (time.monotonic() - started)
            adapter_invocations += 1
            reserved_bytes += reservation
            persisted, action = _dispatch_one(order, remaining)
            states[index] = persisted
            actions[entry["queue_ordinal"]] = (action, True)
            continue
        except QueueDeadlineError:
            actions[entry["queue_ordinal"]] = ("deadline_interrupted", True)
            stop_reason = "max_run_seconds"
            break
        except acquire.AcquisitionError as error:
            try:
                bundle = _replay_bundle(bundle, software)
                observed_states = _scan_results(bundle)
                observed_failures = _scan_failure_states(bundle)
                for ordinal, state in enumerate(observed_states, 1):
                    if state is not None and states[ordinal - 1] is None:
                        actions[ordinal] = ("completed_concurrently", False)
                states = observed_states
                failure_states = observed_failures
                if states[index] is not None:
                    actions[entry["queue_ordinal"]] = (
                        "completed_during_failed_dispatch",
                        True,
                    )
                    continue
                if failure_states[index]["quarantine"] is not None:
                    actions[entry["queue_ordinal"]] = (
                        "quarantined_concurrently",
                        False,
                    )
                    continue
                failure_states[index] = _record_failure_attempt(
                    bundle,
                    entry,
                    bundle["orders"][index],
                    failure_states[index],
                    error,
                )
                if (
                    len(failure_states[index]["attempts"])
                    == FAILURE_QUARANTINE_ATTEMPTS
                ):
                    failure_states[index] = _seal_quarantine(
                        bundle,
                        entry,
                        bundle["orders"][index],
                        failure_states[index],
                    )
                    actions[entry["queue_ordinal"]] = (
                        "failed_attempt_quarantined",
                        True,
                    )
                else:
                    actions[entry["queue_ordinal"]] = (
                        "failed_attempt_recorded",
                        True,
                    )
                continue
            except (QueueRunnerError, acquire.AcquisitionError, OSError) as replayed:
                fatal_error = QueueRunnerError(
                    f"{error}; failure isolation replay/write failed: {replayed}"
                )
        except (QueueRunnerError, OSError) as error:
            fatal_error = error

        # Only local integrity/replay/write failures reach this point.  Provider
        # failures above are durably counted and isolated without stopping later
        # sealed ordinals.
        try:
            bundle = _replay_bundle(bundle, software)
            states = _scan_results(bundle)
            failure_states = _scan_failure_states(bundle)
            replay_error = None
        except (QueueRunnerError, acquire.AcquisitionError, OSError) as replayed:
            replay_error = str(replayed)
        if fatal_error is None:
            raise QueueRunnerError("dispatch exited without a terminal state")
        message = str(fatal_error)
        if replay_error is not None:
            message = f"{message}; post-failure immutable replay failed: {replay_error}"
        for ordinal, state in enumerate(states, 1):
            if state is not None and actions[ordinal][0] in {
                "pending",
                "retryable_failure_pending",
            }:
                actions[ordinal] = ("completed_during_failed_dispatch", False)
        failure = {
            "ordinal": entry["queue_ordinal"],
            "job_id": entry["job_id"],
            "error": {"type": type(fatal_error).__name__, "message": message},
        }
        summary = _summary(
            bundle=bundle,
            software=software,
            mode="run",
            status="failed",
            limits=limits,
            initial=initial,
            final=states,
            initial_failures=initial_failures,
            final_failures=failure_states,
            actions=actions,
            adapter_invocation_count=adapter_invocations,
            dispatch_reservation_bytes=reserved_bytes,
            stop_reason="integrity_failure",
            failed_job=failure,
        )
        raise QueueRunnerFailure(summary) from fatal_error

    bundle = _replay_bundle(bundle, software)
    final = _scan_results(bundle)
    final_failures = _scan_failure_states(bundle)
    for ordinal, (before, observed) in enumerate(zip(states, final, strict=True), 1):
        if before is not None and observed != before:
            raise QueueRunnerError(
                f"completed result {ordinal} changed during final queue replay"
            )
        if before is None and observed is not None:
            actions[ordinal] = ("completed_concurrently", False)
    for ordinal, (before, observed) in enumerate(
        zip(failure_states, final_failures, strict=True), 1
    ):
        if before["attempts"] != observed["attempts"][: len(before["attempts"])]:
            raise QueueRunnerError(
                f"failure-attempt history {ordinal} changed during final queue replay"
            )
        if before["quarantine"] is not None and observed["quarantine"] != before[
            "quarantine"
        ]:
            raise QueueRunnerError(
                f"quarantine receipt {ordinal} changed during final queue replay"
            )
        if (
            final[ordinal - 1] is None
            and before["quarantine"] is None
            and observed["quarantine"] is not None
        ):
            actions[ordinal] = ("quarantined_concurrently", False)
    quarantined = sum(
        result is None and failure["quarantine"] is not None
        for result, failure in zip(final, final_failures, strict=True)
    )
    pending = len(final) - sum(value is not None for value in final) - quarantined
    if pending == 0:
        if quarantined:
            status = "parked"
            stop_reason = "all_runnable_work_exhausted_with_quarantine"
        else:
            status = "completed"
            stop_reason = "all_completed"
    else:
        status = "bounded"
        if stop_reason is None:
            stop_reason = "retryable_failures_recorded"
    return _summary(
        bundle=bundle,
        software=software,
        mode="run",
        status=status,
        limits=limits,
        initial=initial,
        final=final,
        initial_failures=initial_failures,
        final_failures=final_failures,
        actions=actions,
        adapter_invocation_count=adapter_invocations,
        dispatch_reservation_bytes=reserved_bytes,
        stop_reason=stop_reason,
    )


def _error_document(error: Exception) -> dict[str, Any]:
    core = {
        "schema_version": SCHEMA_VERSION,
        "runner_name": RUNNER_NAME,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": "failed",
        "error": {"type": type(error).__name__, "message": str(error)},
        "safety": RUNNER_SAFETY,
    }
    return {**core, "summary_sha256": sha256_bytes(canonical_bytes(core))}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Offline-validate or bounded-sequentially run one sealed acquisition queue"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate", help="offline replay of bundle/results")
    validate.add_argument("--manifest", required=True)
    run = commands.add_parser("run", help="run earliest pending ordinals sequentially")
    run.add_argument("--manifest", required=True)
    run.add_argument("--max-new-items", required=True, type=int)
    run.add_argument("--max-new-bytes", required=True, type=int)
    run.add_argument("--max-run-seconds", required=True, type=int)
    run.add_argument("--free-space-floor-bytes", required=True, type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "validate":
            result = validate_queue(Path(args.manifest))
        else:
            result = run_queue(
                Path(args.manifest),
                max_new_items=args.max_new_items,
                max_new_bytes=args.max_new_bytes,
                max_run_seconds=args.max_run_seconds,
                free_space_floor_bytes=args.free_space_floor_bytes,
            )
        sys.stdout.buffer.write(pretty_bytes(result))
        return 0
    except QueueRunnerFailure as error:
        sys.stderr.buffer.write(pretty_bytes(error.summary))
        return 2
    except (QueueRunnerError, acquire.AcquisitionError, OSError) as error:
        sys.stderr.buffer.write(pretty_bytes(_error_document(error)))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
