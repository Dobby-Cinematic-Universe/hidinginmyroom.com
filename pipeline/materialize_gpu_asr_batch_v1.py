#!/usr/bin/env python3
"""Materialize one finite preprocess-v0.3 GPU handoff into a v5 batch.

The bridge accepts only an exact validated GPU queue and explicit queue ordinals.
It replays the portable-root, production-profile, admitted-runtime, and receipt
lineage controls, writes content-addressed v5 work orders, delegates batch creation
to production_asr_batch_v2, and seals a replayable private receipt.  It never scans
for media, reads an input audio payload, performs inference, imports a result,
publishes, archives, or grants deletion authority.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
from pathlib import Path
from typing import Any, Sequence


try:
    from . import preprocess_gpu_asr_queue_v1 as GPU_QUEUE
    from .gpu import production_asr_batch_v2 as BATCH_V2
    from .gpu import production_asr_v5 as ASR_V5
except ImportError:  # pragma: no cover - direct isolated script execution.
    pipeline_directory = Path(__file__).resolve().parent
    sys.path.insert(0, str(pipeline_directory))
    import preprocess_gpu_asr_queue_v1 as GPU_QUEUE  # type: ignore[no-redef]
    from gpu import production_asr_batch_v2 as BATCH_V2  # type: ignore[no-redef]
    from gpu import production_asr_v5 as ASR_V5  # type: ignore[no-redef]


KIND = "himr_gpu_preprocess_v03_batch_materialization_receipt"
SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.2.0"
CONTRACT_KIND = "himr_gpu_preprocess_v03_batch_materializer_contract"
MAX_ITEMS = 32
MAX_RECEIPT_BYTES = 8 * 1024 * 1024
MAX_PATH_BYTES = 4096
COLD_ROOT = Path("/mnt/archive/HIMR")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
QUEUE_ID_RE = re.compile(r"gpuasrqueue_[0-9a-f]{32}\Z")
MEMBER_ID_RE = re.compile(r"gpuasrmember_[0-9a-f]{32}\Z")
WORK_ORDER_ID_RE = re.compile(r"gpuasrwo5_[0-9a-f]{32}\Z")
BATCH_ID_RE = re.compile(r"gpuasrbatch2_[0-9a-f]{32}\Z")
RECEIPT_ID_RE = re.compile(r"gpuasrmat1_[0-9a-f]{32}\Z")

POLICY = {
    "visibility": "private",
    "network_access": False,
    "cold_storage_access": False,
    "recursive_discovery": False,
    "input_media_payload_read": False,
    "inference_authority": "none",
    "result_import_authority": "none",
    "publication_authority": "none",
    "catalogue_mutation_authority": "none",
    "identity_authority": "none",
    "biometric_authority": "none",
    "wiki_authority": "none",
    "archive_authority": "none",
    "deletion_authority": "none",
}

EXECUTION_MODE_CLASSES = {
    "production": BATCH_V2.EXECUTION_CLASS_PRODUCTION,
    "local-private-production": BATCH_V2.EXECUTION_CLASS_LOCAL_PRIVATE,
}


def _class_for_runtime_status(status: str) -> str:
    if status == "admitted":
        return BATCH_V2.EXECUTION_CLASS_PRODUCTION
    if status == "candidate":
        return BATCH_V2.EXECUTION_CLASS_LOCAL_PRIVATE
    raise MaterializerError("runtime status is unsupported")

RECEIPT_CORE_FIELDS = {
    "kind",
    "schema_version",
    "implementation_version",
    "status",
    "source_queue",
    "selection",
    "root_registration",
    "production_profile",
    "runtime_admission",
    "output_roots",
    "work_orders",
    "batch",
    "policy",
}


class MaterializerError(RuntimeError):
    """The finite receipt-to-batch bridge failed closed."""


def canonical_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise MaterializerError(f"value is not canonical JSON: {error}") from error


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _exact(value: Any, label: str, fields: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        observed = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise MaterializerError(f"{label} has unexpected fields: {observed}")
    return value


def _integer(value: Any, label: str, minimum: int, maximum: int = 2**63 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise MaterializerError(f"{label} must be an integer in [{minimum}, {maximum}]")
    return value


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise MaterializerError(f"{label} must be a lowercase SHA-256")
    return value


def _identifier(value: Any, label: str, expression: re.Pattern[str]) -> str:
    if not isinstance(value, str) or not expression.fullmatch(value):
        raise MaterializerError(f"{label} is invalid")
    return value


def _absolute(value: str | os.PathLike[str], label: str) -> Path:
    try:
        raw = os.fspath(value)
    except TypeError as error:
        raise MaterializerError(f"{label} must be a path") from error
    if (
        not isinstance(raw, str)
        or not raw
        or len(raw.encode("utf-8")) > MAX_PATH_BYTES
        or "\x00" in raw
        or "\\" in raw
        or "//" in raw
        or not raw.startswith("/")
        or raw == "/"
        or os.path.normpath(raw) != raw
    ):
        raise MaterializerError(f"{label} must be one normalized absolute non-root path")
    path = Path(raw)
    if path == COLD_ROOT or COLD_ROOT in path.parents:
        raise MaterializerError(f"{label} may not reference cold storage")
    return path


def _descendant(path: Path, root: Path, label: str) -> None:
    if root not in path.parents:
        raise MaterializerError(f"{label} must be a strict descendant of the registered hot root")


def _safe_private_directory(path_value: str | Path, label: str) -> Path:
    path = _absolute(path_value, label)
    try:
        info = path.lstat()
    except OSError as error:
        raise MaterializerError(f"cannot inspect {label}: {error}") from error
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise MaterializerError(f"{label} must be current-user-owned mode 0700")
    return path


def _fixed_private_child(parent_value: str | Path, name: str, label: str) -> Path:
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,63}", name):
        raise MaterializerError("fixed materializer directory name is invalid")
    parent = _safe_private_directory(parent_value, f"{label} parent")
    descriptor = os.open(
        parent,
        os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        try:
            os.mkdir(name, 0o700, dir_fd=descriptor)
        except FileExistsError:
            pass
        observed = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISDIR(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or stat.S_IMODE(observed.st_mode) != 0o700
        ):
            raise MaterializerError(f"{label} is not a private fixed directory")
    finally:
        os.close(descriptor)
    return _safe_private_directory(parent / name, label)


def _stable_document(
    path_value: str | Path,
    expected_sha256: str,
    label: str,
    *,
    maximum_bytes: int = MAX_RECEIPT_BYTES,
) -> tuple[Any, bytes]:
    path = _absolute(path_value, f"{label} path")
    try:
        body = ASR_V5._stable_file_bytes(
            path,
            label=label,
            maximum_bytes=maximum_bytes,
            expected_sha256=_digest(expected_sha256, f"{label} expected SHA-256"),
        )
        value = ASR_V5.parse_json_bytes(body, label)
    except Exception as error:
        raise MaterializerError(f"cannot replay {label}: {error}") from error
    if body != canonical_bytes(value):
        raise MaterializerError(f"{label} is not canonical JSON")
    return value, body


def _write_exact(path: Path, value: Any, label: str) -> tuple[str, str, int]:
    path = _absolute(path, f"{label} path")
    body = canonical_bytes(value)
    if len(body) > MAX_RECEIPT_BYTES:
        raise MaterializerError(f"{label} exceeds its byte bound")
    digest = sha256_bytes(body)

    def replay() -> tuple[str, str, int]:
        observed = ASR_V5._stable_file_bytes(
            path,
            label=label,
            maximum_bytes=MAX_RECEIPT_BYTES,
            expected_sha256=digest,
        )
        if observed != body:
            raise MaterializerError(f"existing {label} differs from content-addressed bytes")
        return "reused", digest, len(body)

    if path.exists() or path.is_symlink():
        return replay()
    try:
        ASR_V5._write_exclusive(path, value)
    except FileExistsError:
        return replay()
    return "created", digest, len(body)


def _output_roots(
    *,
    hot_root: Path,
    root_registration: dict[str, Any],
    work_order_root: Path,
    receipt_root: Path,
    result_root: Path,
    batch_root: Path,
    event_root: Path,
    lock_root: Path,
) -> dict[str, str]:
    roots = {
        "work_order": _safe_private_directory(work_order_root, "work-order root"),
        "receipt": _safe_private_directory(receipt_root, "materialization receipt root"),
        "result": _safe_private_directory(result_root, "result root"),
        "batch": _safe_private_directory(batch_root, "batch root"),
        "event": _safe_private_directory(event_root, "event root"),
        "lock": _safe_private_directory(lock_root, "lock root"),
    }
    for name, path in roots.items():
        _descendant(path, hot_root, f"{name} output root")
        try:
            ASR_V5._verify_output_directory(root_registration, path)
        except Exception as error:
            raise MaterializerError(
                f"{name} output root is not confined by the portable root: {error}"
            ) from error
    values = list(roots.values())
    if len(set(values)) != len(values) or any(
        left in right.parents or right in left.parents
        for index, left in enumerate(values)
        for right in values[index + 1 :]
    ):
        raise MaterializerError("materializer output roots must be distinct and non-nested")
    return {name: str(path) for name, path in sorted(roots.items())}


def _require_queue_profile_root(
    manifest: dict[str, Any],
    *,
    profile_path: Path,
    profile_sha256: str,
    root_registration_path: Path,
    root_registration_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    try:
        compact_profile, queue_profile = ASR_V5.profile_from_gpu_queue_manifest(manifest)
        hot_root = ASR_V5.hot_root_from_gpu_queue_manifest(manifest)
    except Exception as error:
        raise MaterializerError(f"GPU queue profile/root projection failed: {error}") from error
    if compact_profile != {
        "path": str(profile_path),
        "sha256": profile_sha256,
        "profile_id": queue_profile["profile_id"],
        "identity_sha256": queue_profile["identity_sha256"],
    }:
        raise MaterializerError("explicit production profile differs from the GPU queue")
    if (
        hot_root["registration_path"] != str(root_registration_path)
        or hot_root["registration_sha256"] != root_registration_sha256
    ):
        raise MaterializerError("explicit root registration differs from the GPU queue")
    try:
        external_hot_root, root_registration = ASR_V5.hot_root_reference(
            root_registration_path,
            root_registration_sha256,
        )
    except Exception as error:
        raise MaterializerError(f"portable root replay failed: {error}") from error
    if external_hot_root != hot_root:
        raise MaterializerError("GPU queue root differs from its external registration")
    try:
        external_profile = ASR_V5.load_profile_document(profile_path, profile_sha256)
    except Exception as error:
        raise MaterializerError(f"production profile replay failed: {error}") from error
    if external_profile != queue_profile:
        raise MaterializerError("GPU queue profile document differs from its external file")
    return compact_profile, queue_profile, hot_root, root_registration


def _require_runtime_bindings(
    runtime_reference: dict[str, Any],
    runtime_receipt: dict[str, Any],
    *,
    profile_reference: dict[str, Any],
    hot_root: dict[str, Any],
    expected_status: str,
) -> None:
    if expected_status not in {"admitted", "candidate"}:
        raise MaterializerError("expected runtime status is unsupported")
    if (
        runtime_reference.get("status") != expected_status
        or runtime_receipt.get("status") != expected_status
    ):
        raise MaterializerError(
            f"materialization requires a {expected_status} runtime"
        )
    profile_document = runtime_receipt.get("production_profile")
    profile_file = runtime_receipt.get("production_profile_file")
    root_document = runtime_receipt.get("root")
    root_file = runtime_receipt.get("root_registration")
    if (
        not isinstance(profile_document, dict)
        or not isinstance(profile_file, dict)
        or profile_document.get("identity_sha256") != profile_reference["identity_sha256"]
        or any(
            profile_file.get(name) != profile_reference[name]
            for name in ("path", "sha256", "identity_sha256")
        )
    ):
        raise MaterializerError("admitted runtime binds a different production profile")
    if (
        not isinstance(root_document, dict)
        or not isinstance(root_file, dict)
        or root_document.get("root_id") != hot_root["root_id"]
        or root_document.get("tier") != hot_root["tier"]
        or root_document.get("path") != hot_root["path"]
        or root_document.get("filesystem")
        != {"type": "btrfs", "uuid": hot_root["filesystem_uuid"]}
        or root_file.get("registration_id") != hot_root["registration_id"]
        or root_file.get("identity_sha256") != hot_root["identity_sha256"]
        or root_file.get("path") != hot_root["registration_path"]
        or root_file.get("sha256") != hot_root["registration_sha256"]
    ):
        raise MaterializerError("admitted runtime binds a different portable root")


def _selected_members(manifest: dict[str, Any], ordinals: Sequence[int]) -> list[dict[str, Any]]:
    if not isinstance(ordinals, (list, tuple)) or not 1 <= len(ordinals) <= MAX_ITEMS:
        raise MaterializerError("selection must contain a finite 1..32 queue-ordinal list")
    normalized = [
        _integer(value, f"queue ordinal {index}", 1, GPU_QUEUE.MAX_ITEMS)
        for index, value in enumerate(ordinals, 1)
    ]
    if len(set(normalized)) != len(normalized):
        raise MaterializerError("queue ordinals must be unique")
    members = manifest.get("members")
    if not isinstance(members, list):
        raise MaterializerError("GPU queue member array is invalid")
    by_ordinal = {
        row.get("ordinal"): row
        for row in members
        if isinstance(row, dict) and isinstance(row.get("ordinal"), int)
    }
    selected = []
    for ordinal in normalized:
        row = by_ordinal.get(ordinal)
        if row is None:
            raise MaterializerError(f"queue ordinal {ordinal} is absent")
        disposition = row.get("resource_disposition")
        if not isinstance(disposition, dict) or disposition.get("state") != "ready":
            raise MaterializerError(f"queue ordinal {ordinal} is not ready")
        selected.append(row)
    return selected


def make_receipt(core: Any) -> dict[str, Any]:
    item = _exact(core, "materialization receipt", RECEIPT_CORE_FIELDS)
    if (
        item["kind"] != KIND
        or item["schema_version"] != SCHEMA_VERSION
        or item["implementation_version"] != IMPLEMENTATION_VERSION
        or item["status"] != "materialized"
        or item["policy"] != POLICY
    ):
        raise MaterializerError("materialization receipt header or policy is unsupported")
    queue = _exact(
        item["source_queue"],
        "source queue",
        {"path", "sha256", "queue_id", "identity_sha256", "implementation_version"},
    )
    queue_path = _absolute(queue["path"], "source queue path")
    queue_id = _identifier(queue["queue_id"], "source queue ID", QUEUE_ID_RE)
    queue_identity = _digest(queue["identity_sha256"], "source queue identity")
    if (
        queue_id != f"gpuasrqueue_{queue_identity[:32]}"
        or queue["implementation_version"] != GPU_QUEUE.IMPLEMENTATION_VERSION
    ):
        raise MaterializerError("source queue identity/version is inconsistent")

    root = ASR_V5._validate_hot_root(item["root_registration"])
    hot_root = _absolute(root["path"], "registered hot root")
    _descendant(queue_path, hot_root, "source queue")
    profile = ASR_V5._validate_profile_reference(item["production_profile"], hot_root)
    runtime = ASR_V5._validate_runtime_reference(item["runtime_admission"], hot_root)
    expected_class = _class_for_runtime_status(runtime["status"])

    roots = _exact(
        item["output_roots"],
        "output roots",
        {"work_order", "receipt", "result", "batch", "event", "lock"},
    )
    normalized_roots = {
        name: _absolute(path, f"{name} output root")
        for name, path in roots.items()
    }
    for name, path in normalized_roots.items():
        _descendant(path, hot_root, f"{name} output root")
    root_values = list(normalized_roots.values())
    if len(set(root_values)) != len(root_values) or any(
        left in right.parents or right in left.parents
        for index, left in enumerate(root_values)
        for right in root_values[index + 1 :]
    ):
        raise MaterializerError("receipt output roots are not distinct and non-nested")

    selection = _exact(
        item["selection"],
        "selection",
        {"queue_ordinals", "preprocess_ordinals", "member_ids"},
    )
    queue_ordinals = selection["queue_ordinals"]
    preprocess_ordinals = selection["preprocess_ordinals"]
    member_ids = selection["member_ids"]
    if not all(
        isinstance(rows, list)
        for rows in (queue_ordinals, preprocess_ordinals, member_ids)
    ):
        raise MaterializerError("selection fields must be arrays")
    count = len(queue_ordinals)
    if (
        not 1 <= count <= MAX_ITEMS
        or len(preprocess_ordinals) != count
        or len(member_ids) != count
    ):
        raise MaterializerError("selection arrays have inconsistent finite cardinality")
    normalized_queue_ordinals = [
        _integer(value, f"selection queue ordinal {ordinal}", 1, GPU_QUEUE.MAX_ITEMS)
        for ordinal, value in enumerate(queue_ordinals, 1)
    ]
    normalized_preprocess_ordinals = [
        _integer(value, f"selection preprocess ordinal {ordinal}", 1, GPU_QUEUE.MAX_ITEMS)
        for ordinal, value in enumerate(preprocess_ordinals, 1)
    ]
    normalized_member_ids = [
        _identifier(value, f"selection member ID {ordinal}", MEMBER_ID_RE)
        for ordinal, value in enumerate(member_ids, 1)
    ]
    if any(
        len(set(rows)) != len(rows)
        for rows in (
            normalized_queue_ordinals,
            normalized_preprocess_ordinals,
            normalized_member_ids,
        )
    ):
        raise MaterializerError("selection identities must be unique")

    work_orders = item["work_orders"]
    if not isinstance(work_orders, list) or len(work_orders) != count:
        raise MaterializerError("work-order receipt set differs from selection")
    normalized_work_orders: list[dict[str, Any]] = []
    for ordinal, row_value in enumerate(work_orders, 1):
        row = _exact(
            row_value,
            f"work order {ordinal}",
            {
                "ordinal", "queue_ordinal", "preprocess_ordinal", "member_id",
                "path", "sha256", "byte_count", "work_order_id", "identity_sha256",
            },
        )
        path = _absolute(row["path"], f"work order {ordinal} path")
        _descendant(path, normalized_roots["work_order"], f"work order {ordinal}")
        identity = _digest(row["identity_sha256"], f"work order {ordinal} identity")
        work_order_id = _identifier(
            row["work_order_id"], f"work order {ordinal} ID", WORK_ORDER_ID_RE
        )
        normalized = {
            "ordinal": _integer(
                row["ordinal"], f"work order {ordinal}.ordinal", ordinal, ordinal
            ),
            "queue_ordinal": _integer(
                row["queue_ordinal"],
                f"work order {ordinal}.queue_ordinal",
                1,
                GPU_QUEUE.MAX_ITEMS,
            ),
            "preprocess_ordinal": _integer(
                row["preprocess_ordinal"],
                f"work order {ordinal}.preprocess_ordinal",
                1,
                GPU_QUEUE.MAX_ITEMS,
            ),
            "member_id": _identifier(
                row["member_id"],
                f"work order {ordinal}.member_id",
                MEMBER_ID_RE,
            ),
            "path": str(path),
            "sha256": _digest(row["sha256"], f"work order {ordinal} SHA-256"),
            "byte_count": _integer(
                row["byte_count"],
                f"work order {ordinal} byte_count",
                1,
                ASR_V5.MAX_JSON_BYTES,
            ),
            "work_order_id": work_order_id,
            "identity_sha256": identity,
        }
        if work_order_id != f"gpuasrwo5_{identity[:32]}" or (
            normalized["queue_ordinal"], normalized["preprocess_ordinal"], normalized["member_id"]
        ) != (
            normalized_queue_ordinals[ordinal - 1],
            normalized_preprocess_ordinals[ordinal - 1],
            normalized_member_ids[ordinal - 1],
        ):
            raise MaterializerError(f"work order {ordinal} differs from selection")
        normalized_work_orders.append(normalized)

    batch = _exact(
        item["batch"],
        "batch",
        {"path", "sha256", "byte_count", "batch_id", "identity_sha256", "execution_class"},
    )
    batch_path = _absolute(batch["path"], "batch manifest path")
    _descendant(batch_path, normalized_roots["batch"], "batch manifest")
    batch_identity = _digest(batch["identity_sha256"], "batch identity")
    batch_id = _identifier(batch["batch_id"], "batch ID", BATCH_ID_RE)
    if batch_id != f"gpuasrbatch2_{batch_identity[:32]}" or batch["execution_class"] != expected_class:
        raise MaterializerError("batch identity or execution class is inconsistent")

    normalized = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": "materialized",
        "source_queue": {
            "path": str(queue_path),
            "sha256": _digest(queue["sha256"], "source queue SHA-256"),
            "queue_id": queue_id,
            "identity_sha256": queue_identity,
            "implementation_version": GPU_QUEUE.IMPLEMENTATION_VERSION,
        },
        "selection": {
            "queue_ordinals": normalized_queue_ordinals,
            "preprocess_ordinals": normalized_preprocess_ordinals,
            "member_ids": normalized_member_ids,
        },
        "root_registration": root,
        "production_profile": profile,
        "runtime_admission": runtime,
        "output_roots": {name: str(path) for name, path in sorted(normalized_roots.items())},
        "work_orders": normalized_work_orders,
        "batch": {
            "path": str(batch_path),
            "sha256": _digest(batch["sha256"], "batch manifest SHA-256"),
            "byte_count": _integer(batch["byte_count"], "batch manifest byte_count", 1, BATCH_V2.MAX_JSON_BYTES),
            "batch_id": batch_id,
            "identity_sha256": batch_identity,
            "execution_class": expected_class,
        },
        "policy": dict(POLICY),
    }
    identity = sha256_bytes(canonical_bytes(normalized))
    return {
        **normalized,
        "identity_sha256": identity,
        "receipt_id": f"gpuasrmat1_{identity[:32]}",
    }


def validate_receipt(value: Any) -> dict[str, Any]:
    item = _exact(
        value,
        "materialization receipt",
        RECEIPT_CORE_FIELDS | {"identity_sha256", "receipt_id"},
    )
    expected = make_receipt({key: item[key] for key in RECEIPT_CORE_FIELDS})
    if canonical_bytes(item) != canonical_bytes(expected):
        raise MaterializerError("materialization receipt is noncanonical or its identity is invalid")
    return expected


def materialize(
    *,
    queue_manifest_path: Path,
    expected_queue_sha256: str,
    root_registration_path: Path,
    expected_root_registration_sha256: str,
    runtime_admission_path: Path,
    expected_runtime_admission_sha256: str,
    production_profile_path: Path,
    expected_production_profile_sha256: str,
    queue_ordinals: Sequence[int],
    work_order_root: Path,
    receipt_root: Path,
    result_root: Path,
    batch_root: Path,
    event_root: Path,
    lock_root: Path,
    execution_mode: str = "production",
) -> tuple[dict[str, Any], Path, dict[str, int]]:
    if execution_mode not in EXECUTION_MODE_CLASSES:
        raise MaterializerError("execution mode is unsupported")
    expected_execution_class = EXECUTION_MODE_CLASSES[execution_mode]
    expected_runtime_status = (
        "admitted" if execution_mode == "production" else "candidate"
    )
    queue_path = _absolute(queue_manifest_path, "GPU queue manifest")
    registration_path = _absolute(root_registration_path, "root registration")
    runtime_path = _absolute(runtime_admission_path, "runtime admission")
    profile_path = _absolute(production_profile_path, "production profile")
    queue_sha256 = _digest(expected_queue_sha256, "expected GPU queue SHA-256")
    root_sha256 = _digest(expected_root_registration_sha256, "expected root registration SHA-256")
    runtime_sha256 = _digest(expected_runtime_admission_sha256, "expected runtime admission SHA-256")
    profile_sha256 = _digest(expected_production_profile_sha256, "expected production profile SHA-256")
    try:
        manifest = GPU_QUEUE.validate_queue(
            manifest_path=queue_path,
            root_registration_path=registration_path,
            root_registration_sha256=root_sha256,
        )
    except Exception as error:
        raise MaterializerError(f"GPU queue replay failed: {error}") from error
    if sha256_bytes(GPU_QUEUE.canonical_bytes(manifest)) != queue_sha256:
        raise MaterializerError("GPU queue physical SHA-256 differs from explicit binding")
    profile_reference, profile, hot_root, root_registration = _require_queue_profile_root(
        manifest,
        profile_path=profile_path,
        profile_sha256=profile_sha256,
        root_registration_path=registration_path,
        root_registration_sha256=root_sha256,
    )
    try:
        runtime_reference, runtime_receipt = ASR_V5.runtime_admission_reference(
            runtime_path,
            runtime_sha256,
            require_admitted=execution_mode == "production",
        )
    except Exception as error:
        raise MaterializerError(f"{execution_mode} runtime replay failed: {error}") from error
    if runtime_reference["status"] != expected_runtime_status:
        raise MaterializerError(
            f"{execution_mode} requires a {expected_runtime_status} runtime"
        )
    _require_runtime_bindings(
        runtime_reference,
        runtime_receipt,
        profile_reference=profile_reference,
        hot_root=hot_root,
        expected_status=expected_runtime_status,
    )
    selected = _selected_members(manifest, queue_ordinals)
    limits = profile["batch_limits"]
    if len(selected) > limits["maximum_items"]:
        raise MaterializerError("selection exceeds the production profile batch limit")
    roots = _output_roots(
        hot_root=_absolute(hot_root["path"], "registered hot root"),
        root_registration=root_registration,
        work_order_root=work_order_root,
        receipt_root=receipt_root,
        result_root=result_root,
        batch_root=batch_root,
        event_root=event_root,
        lock_root=lock_root,
    )
    orders_directory = _fixed_private_child(roots["work_order"], "work-orders", "work-order directory")
    work_order_paths: list[Path] = []
    work_order_rows: list[dict[str, Any]] = []
    created_orders = 0
    for ordinal, entry in enumerate(selected, 1):
        try:
            order, projected_profile = ASR_V5.work_order_from_preprocess_descriptor(
                entry=entry,
                manifest=manifest,
                runtime_admission=runtime_reference,
                output_root=roots["result"],
                sealed_mode=entry["audio"]["sealed_mode"],
                replay_bindings=False,
            )
        except Exception as error:
            raise MaterializerError(f"queue member {entry.get('ordinal')} cannot become v5: {error}") from error
        if projected_profile != profile:
            raise MaterializerError("v5 helper projected a different production profile")
        path = orders_directory / f"{order['work_order_id']}.json"
        disposition, digest, byte_count = _write_exact(path, order, f"v5 work order {ordinal}")
        created_orders += disposition == "created"
        work_order_paths.append(path)
        work_order_rows.append(
            {
                "ordinal": ordinal,
                "queue_ordinal": entry["ordinal"],
                "preprocess_ordinal": entry["preprocess_ordinal"],
                "member_id": entry["member_id"],
                "path": str(path),
                "sha256": digest,
                "byte_count": byte_count,
                "work_order_id": order["work_order_id"],
                "identity_sha256": order["identity_sha256"],
            }
        )
    try:
        batch, batch_path = BATCH_V2.materialize_batch(
            work_order_paths=work_order_paths,
            profile_path=profile_path,
            expected_profile_sha256=profile_sha256,
            batch_root=Path(roots["batch"]),
            event_root=Path(roots["event"]),
            lock_root=Path(roots["lock"]),
        )
    except Exception as error:
        raise MaterializerError(f"batch-v2 materialization failed: {error}") from error
    if batch.get("execution_class") != expected_execution_class:
        raise MaterializerError("batch-v2 execution class differs from the requested mode")
    batch_body = BATCH_V2.canonical_bytes(batch)
    observed_batch, observed_body = _stable_document(
        batch_path,
        sha256_bytes(batch_body),
        "batch-v2 manifest",
        maximum_bytes=BATCH_V2.MAX_JSON_BYTES,
    )
    if observed_body != batch_body or observed_batch != batch:
        raise MaterializerError("batch-v2 returned bytes different from its sealed manifest")
    receipt_core = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": "materialized",
        "source_queue": {
            "path": str(queue_path),
            "sha256": queue_sha256,
            "queue_id": manifest["queue_id"],
            "identity_sha256": manifest["identity_sha256"],
            "implementation_version": manifest["implementation_version"],
        },
        "selection": {
            "queue_ordinals": [entry["ordinal"] for entry in selected],
            "preprocess_ordinals": [entry["preprocess_ordinal"] for entry in selected],
            "member_ids": [entry["member_id"] for entry in selected],
        },
        "root_registration": hot_root,
        "production_profile": profile_reference,
        "runtime_admission": runtime_reference,
        "output_roots": roots,
        "work_orders": work_order_rows,
        "batch": {
            "path": str(batch_path),
            "sha256": sha256_bytes(batch_body),
            "byte_count": len(batch_body),
            "batch_id": batch["batch_id"],
            "identity_sha256": batch["identity_sha256"],
            "execution_class": batch["execution_class"],
        },
        "policy": dict(POLICY),
    }
    receipt = make_receipt(receipt_core)
    receipt_directory = _fixed_private_child(roots["receipt"], "materializations", "materialization receipt directory")
    receipt_path = receipt_directory / f"{receipt['receipt_id']}.json"
    receipt_disposition, _, _ = _write_exact(receipt_path, receipt, "materialization receipt")
    return receipt, receipt_path, {
        "created_work_orders": created_orders,
        "reused_work_orders": len(work_order_rows) - created_orders,
        "created_receipt": int(receipt_disposition == "created"),
        "reused_receipt": int(receipt_disposition == "reused"),
    }


def replay_receipt(value: Any) -> dict[str, Any]:
    receipt = validate_receipt(value)
    source = receipt["source_queue"]
    root = receipt["root_registration"]
    try:
        manifest = GPU_QUEUE.validate_queue(
            manifest_path=Path(source["path"]),
            root_registration_path=Path(root["registration_path"]),
            root_registration_sha256=root["registration_sha256"],
        )
    except Exception as error:
        raise MaterializerError(f"receipt GPU queue replay failed: {error}") from error
    if (
        sha256_bytes(GPU_QUEUE.canonical_bytes(manifest)) != source["sha256"]
        or manifest.get("queue_id") != source["queue_id"]
        or manifest.get("identity_sha256") != source["identity_sha256"]
    ):
        raise MaterializerError("receipt source queue differs from external replay")
    profile_reference, profile, hot_root, _root_registration = _require_queue_profile_root(
        manifest,
        profile_path=Path(receipt["production_profile"]["path"]),
        profile_sha256=receipt["production_profile"]["sha256"],
        root_registration_path=Path(root["registration_path"]),
        root_registration_sha256=root["registration_sha256"],
    )
    if hot_root != root or profile_reference != receipt["production_profile"]:
        raise MaterializerError("receipt profile/root differs from queue replay")
    runtime_reference, runtime_value = ASR_V5.runtime_admission_reference(
        receipt["runtime_admission"]["receipt_path"],
        receipt["runtime_admission"]["receipt_sha256"],
        require_admitted=(
            receipt["batch"]["execution_class"]
            == BATCH_V2.EXECUTION_CLASS_PRODUCTION
        ),
    )
    if runtime_reference != receipt["runtime_admission"]:
        raise MaterializerError("receipt runtime differs from external replay")
    _require_runtime_bindings(
        runtime_reference,
        runtime_value,
        profile_reference=profile_reference,
        hot_root=hot_root,
        expected_status=receipt["runtime_admission"]["status"],
    )
    selected = _selected_members(manifest, receipt["selection"]["queue_ordinals"])
    if (
        [row["preprocess_ordinal"] for row in selected]
        != receipt["selection"]["preprocess_ordinals"]
        or [row["member_id"] for row in selected] != receipt["selection"]["member_ids"]
    ):
        raise MaterializerError("receipt selection differs from queue replay")
    orders: list[dict[str, Any]] = []
    for ordinal, (row, entry) in enumerate(zip(receipt["work_orders"], selected, strict=True), 1):
        order = ASR_V5.load_work_order(
            row["path"],
            profile_document=profile,
            expected_sha256=row["sha256"],
            # The queue, root, profile, admitted runtime, and every receipt
            # lineage were replayed once above.  Repeating the same runtime gate
            # replay for every member would add O(items * gate-bytes) work without
            # adding authority; exact per-order projections are compared below.
            replay_bindings=False,
        )
        body = ASR_V5.canonical_bytes(order)
        lineage = order["source_lineage"]["gpu_handoff"]
        if (
            len(body) != row["byte_count"]
            or order["work_order_id"] != row["work_order_id"]
            or order["identity_sha256"] != row["identity_sha256"]
            or lineage["queue_ordinal"] != entry["ordinal"]
            or lineage["preprocess_ordinal"] != entry["preprocess_ordinal"]
            or lineage["member_id"] != entry["member_id"]
            or order["runtime_admission"] != runtime_reference
            or order["production_profile"] != profile_reference
            or order["hot_root"] != hot_root
            or order["output"]["root"] != receipt["output_roots"]["result"]
        ):
            raise MaterializerError(f"receipt work order {ordinal} differs from replay")
        orders.append(order)
    batch, batch_body = BATCH_V2.load_manifest(
        receipt["batch"]["path"],
        receipt["batch"]["sha256"],
        profile=profile,
    )
    if (
        len(batch_body) != receipt["batch"]["byte_count"]
        or batch["batch_id"] != receipt["batch"]["batch_id"]
        or batch["identity_sha256"] != receipt["batch"]["identity_sha256"]
        or batch["execution_class"] != receipt["batch"]["execution_class"]
        or [member["work_order"] for member in batch["items"]] != orders
        or batch["writable_roots"]
        != {
            "batch": receipt["output_roots"]["batch"],
            "event": receipt["output_roots"]["event"],
            "lock": receipt["output_roots"]["lock"],
            "result": receipt["output_roots"]["result"],
        }
    ):
        raise MaterializerError("receipt batch differs from batch-v2 replay")
    return receipt


def load_receipt(path_value: str | Path, expected_sha256: str, *, replay: bool) -> dict[str, Any]:
    value, _ = _stable_document(path_value, expected_sha256, "materialization receipt")
    return replay_receipt(value) if replay else validate_receipt(value)


def contract_document() -> dict[str, Any]:
    descriptor = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "source": "validated_himr_preprocess_gpu_asr_queue_v1_from_v03_receipts",
        "selection": "explicit_unique_queue_ordinals_1_to_32",
        "work_order": "production_asr_v5_work_order_from_preprocess_descriptor",
        "batch": "production_asr_batch_v2_materialize_batch",
        "runtime": "admitted_for_production_candidate_for_local_private_production",
        "commands": ["contract", "materialize", "validate"],
        "policy": dict(POLICY),
    }
    return {
        "kind": CONTRACT_KIND,
        "schema_version": SCHEMA_VERSION,
        "descriptor": descriptor,
        "identity_sha256": sha256_bytes(canonical_bytes(descriptor)),
    }


def _positive_ordinal(value: str) -> int:
    try:
        parsed = int(value, 10)
    except ValueError as error:
        raise argparse.ArgumentTypeError("queue ordinal must be a base-10 integer") from error
    if not 1 <= parsed <= GPU_QUEUE.MAX_ITEMS:
        raise argparse.ArgumentTypeError(f"queue ordinal must be within 1..{GPU_QUEUE.MAX_ITEMS}")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("contract")
    materialize_command = commands.add_parser("materialize")
    materialize_command.add_argument("--queue-manifest", required=True)
    materialize_command.add_argument("--expected-queue-sha256", required=True)
    materialize_command.add_argument("--root-registration", required=True)
    materialize_command.add_argument("--expected-root-registration-sha256", required=True)
    materialize_command.add_argument("--runtime-admission", required=True)
    materialize_command.add_argument("--expected-runtime-admission-sha256", required=True)
    materialize_command.add_argument("--production-profile", required=True)
    materialize_command.add_argument("--expected-production-profile-sha256", required=True)
    materialize_command.add_argument("--queue-ordinal", action="append", required=True, type=_positive_ordinal)
    materialize_command.add_argument("--work-order-root", required=True)
    materialize_command.add_argument("--receipt-root", required=True)
    materialize_command.add_argument("--result-root", required=True)
    materialize_command.add_argument("--batch-root", required=True)
    materialize_command.add_argument("--event-root", required=True)
    materialize_command.add_argument("--lock-root", required=True)
    materialize_command.add_argument(
        "--execution-mode",
        choices=tuple(sorted(EXECUTION_MODE_CLASSES)),
        default="production",
    )
    validate_command = commands.add_parser("validate")
    validate_command.add_argument("--receipt", required=True)
    validate_command.add_argument("--expected-receipt-sha256", required=True)
    validate_command.add_argument("--replay", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "contract":
            response = contract_document()
        elif args.command == "materialize":
            receipt, path, disposition = materialize(
                queue_manifest_path=Path(args.queue_manifest),
                expected_queue_sha256=args.expected_queue_sha256,
                root_registration_path=Path(args.root_registration),
                expected_root_registration_sha256=args.expected_root_registration_sha256,
                runtime_admission_path=Path(args.runtime_admission),
                expected_runtime_admission_sha256=args.expected_runtime_admission_sha256,
                production_profile_path=Path(args.production_profile),
                expected_production_profile_sha256=args.expected_production_profile_sha256,
                queue_ordinals=args.queue_ordinal,
                work_order_root=Path(args.work_order_root),
                receipt_root=Path(args.receipt_root),
                result_root=Path(args.result_root),
                batch_root=Path(args.batch_root),
                event_root=Path(args.event_root),
                lock_root=Path(args.lock_root),
                execution_mode=args.execution_mode,
            )
            response = {
                "status": "materialized",
                "receipt_id": receipt["receipt_id"],
                "identity_sha256": receipt["identity_sha256"],
                "receipt_path": str(path),
                "receipt_sha256": sha256_bytes(canonical_bytes(receipt)),
                "batch_id": receipt["batch"]["batch_id"],
                "batch_manifest_path": receipt["batch"]["path"],
                "work_order_count": len(receipt["work_orders"]),
                "disposition": disposition,
                "inference_performed": False,
                "input_media_payload_read": False,
                "publication_performed": False,
                "import_performed": False,
            }
        else:
            receipt = load_receipt(
                args.receipt,
                args.expected_receipt_sha256,
                replay=args.replay,
            )
            response = {
                "status": "validated",
                "receipt_id": receipt["receipt_id"],
                "identity_sha256": receipt["identity_sha256"],
                "external_bindings_replayed": args.replay,
                "files_written": False,
                "inference_performed": False,
                "input_media_payload_read": False,
            }
    except (
        MaterializerError,
        GPU_QUEUE.QueueError,
        ASR_V5.ProductionASRV5Error,
        BATCH_V2.BatchV2Error,
        OSError,
        ValueError,
    ) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    sys.stdout.buffer.write(canonical_bytes(response))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
