#!/usr/bin/env python3
"""Plan, apply, and validate immutable permissions for completed ASR results.

This is a deliberately separate administrative lane.  It can only inspect completed
``asr_whispercpp`` result envelopes, create a content-preserving seal plan, apply the
plan's chmod-only transition, and validate the resulting receipt.  It has no ASR,
catalog import, publication, identity, database, or network operation.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import stat
import sys
import uuid
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable


IMPLEMENTATION_VERSION = "0.4.0"
TOOL_NAME = "himr-asr-whispercpp-result-store-seal"
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CORPUS_SOURCE_ROOT = REPOSITORY_ROOT / "corpus" / "src"
IMPORTER_SOURCE = (
    CORPUS_SOURCE_ROOT / "himr_corpus" / "asr_result_importer.py"
)
PREPROCESS_QUEUE_SOURCE = REPOSITORY_ROOT / "pipeline" / "preprocess_asr_queue.py"
PREPROCESS_QUEUE_SCHEMA_SOURCE = (
    REPOSITORY_ROOT / "pipeline" / "schemas" / "preprocess-asr-queue-manifest.schema.json"
)
ASR_BATCH_SOURCE = REPOSITORY_ROOT / "pipeline" / "asr_whispercpp_batch.py"
ASR_BATCH_SCHEMA_SOURCE = (
    REPOSITORY_ROOT / "pipeline" / "schemas" / "asr-whispercpp-batch-manifest.schema.json"
)
DEFAULT_STORE_ROOT = REPOSITORY_ROOT / "research/corpus/private-asr-results"
DEFAULT_CONTROL_ROOT = DEFAULT_STORE_ROOT / "sealing-control"

PLAN_KIND = "asr_whispercpp_completed_result_seal_plan"
RECEIPT_KIND = "asr_whispercpp_completed_result_seal_receipt"
TWO_SOURCE_PLAN_SCHEMA_VERSION = 1
QUEUE_ONLY_PLAN_SCHEMA_VERSION = 2
BATCH_ONLY_PLAN_SCHEMA_VERSION = 3
TWO_SOURCE_RECEIPT_SCHEMA_VERSION = 1
QUEUE_ONLY_RECEIPT_SCHEMA_VERSION = 2
BATCH_ONLY_RECEIPT_SCHEMA_VERSION = 3
# Backward-compatible public names continue to describe the default v1 lane.
PLAN_SCHEMA_VERSION = TWO_SOURCE_PLAN_SCHEMA_VERSION
RECEIPT_SCHEMA_VERSION = TWO_SOURCE_RECEIPT_SCHEMA_VERSION
SUPPORTED_PLAN_SCHEMA_VERSIONS = {
    TWO_SOURCE_PLAN_SCHEMA_VERSION,
    QUEUE_ONLY_PLAN_SCHEMA_VERSION,
    BATCH_ONLY_PLAN_SCHEMA_VERSION,
}
SUPPORTED_RECEIPT_SCHEMA_VERSIONS = {
    TWO_SOURCE_RECEIPT_SCHEMA_VERSION,
    QUEUE_ONLY_RECEIPT_SCHEMA_VERSION,
    BATCH_ONLY_RECEIPT_SCHEMA_VERSION,
}
TWO_SOURCE_CLI_MODE = "two-source-v1"
QUEUE_ONLY_CLI_MODE = "queue-only-v2"
BATCH_ONLY_CLI_MODE = "batch-only-v3"
RESULT_FILENAMES = (
    "result.json",
    "transcript.normalized.json",
    "whisper.raw.json",
)
ROLE_BY_NAME = {
    "result.json": "result_envelope",
    "transcript.normalized.json": "normalized_transcript",
    "whisper.raw.json": "raw_whisper_output",
}
MAX_FILE_BYTES = {
    "result.json": 512 * 1024 * 1024,
    "transcript.normalized.json": 512 * 1024 * 1024,
    "whisper.raw.json": 512 * 1024 * 1024,
}
PLAN_MAX_BYTES = 16 * 1024 * 1024
RECEIPT_MAX_BYTES = 16 * 1024 * 1024
MANIFEST_MAX_BYTES = 32 * 1024 * 1024
WORK_ORDER_MAX_BYTES = 8 * 1024 * 1024
SOURCE_FILE_MODE = 0o400
PLAN_FILE_MODE = 0o400
RESULT_DIRECTORY_MODE_BEFORE = 0o700
RESULT_DIRECTORY_MODE_AFTER = 0o500
RESULT_FILE_MODE_BEFORE = 0o644
RESULT_FILE_MODE_AFTER = 0o400
CONTROL_DIRECTORY_MODE = 0o700
STORE_ROOT_MODE = 0o700
APPLY_LOCK_FILE_MODE = 0o600
APPLY_LOCK_FILENAME = ".asr-whispercpp-result-seal.lock"
SHA256_ZERO = "0" * 64
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
PLAN_ID_RE = re.compile(r"^asrsealplan_[0-9a-f]{32}$")
RECEIPT_ID_RE = re.compile(r"^asrsealreceipt_[0-9a-f]{32}$")
QUEUE_ID_RE = re.compile(r"^asrppqueue_[0-9a-f]{32}$")
SOURCE_ID_RE = re.compile(r"^asr(?:batch|ppqueue)_[0-9a-f]{32}$")
RECIPE_ID_RE = re.compile(r"^recipe_asr_whispercpp_[0-9a-f]{32}$")
CANONICAL_UTC_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
)

AUTHORITY = {
    "asr_execution": False,
    "catalog_import": False,
    "database_writes": False,
    "identity_authority": "none",
    "network_access": "none",
    "publication_authority": "none",
}
POLICY = {
    "allowed_entries": list(RESULT_FILENAMES),
    "catalog_validation": "validate_asr_whispercpp_result_file_catalog_free",
    "container_changes": [],
    "container_policy": (
        "no_descendant_container_change_private_store_root_already_mode_0700"
    ),
    "directory_mode_after": RESULT_DIRECTORY_MODE_AFTER,
    "directory_mode_before": RESULT_DIRECTORY_MODE_BEFORE,
    "file_mode_after": RESULT_FILE_MODE_AFTER,
    "file_mode_before": RESULT_FILE_MODE_BEFORE,
    "hardlink_policy": "all_three_files_must_have_nlink_1",
    "symlink_policy": "resolved_absolute_paths_and_O_NOFOLLOW",
}

# Updating this implementation necessarily changes the byte identity embedded in
# already-applied v1 plans.  Keep the one deployed v1 implementation as an explicit,
# closed replay identity so its immutable 25-result receipt remains verifiable.  New
# plans always record the current implementation bytes; v2 never accepts this legacy
# identity.
LEGACY_V1_IMPLEMENTATION_IDENTITIES = {
    (
        70987,
        "5bae3b2cbc656f9781a2f6fac2aafed122036205ac63eee73fb01c281300c04a",
    )
}

QUEUE_ONLY_SOURCE_AUTHORITY_VERSION = 1
QUEUE_ONLY_SELECTION_POLICY = "all_and_only_routing_hint_process_entries"
BATCH_ONLY_SOURCE_AUTHORITY_VERSION = 1
BATCH_ONLY_SELECTION_POLICY = "all_batch_work_orders"


class SealError(RuntimeError):
    """A fail-closed plan, application, or validation error."""


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _canonical_utc_timestamp(value: object, label: str) -> str:
    if not isinstance(value, str) or CANONICAL_UTC_RE.fullmatch(value) is None:
        raise SealError(
            f"{label} must be canonical RFC3339 UTC YYYY-MM-DDTHH:MM:SSZ"
        )
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as error:
        raise SealError(f"{label} is not a real UTC calendar time") from error
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        raise SealError(f"{label} is not canonical UTC")
    return value


def _strict_integer(value: object, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise SealError(f"{label} must be an integer >= {minimum}")
    return value


def _strict_string(
    value: object,
    label: str,
    *,
    minimum_length: int = 1,
    maximum_length: int | None = None,
) -> str:
    if not isinstance(value, str) or len(value) < minimum_length:
        raise SealError(f"{label} must be a string of length >= {minimum_length}")
    if maximum_length is not None and len(value) > maximum_length:
        raise SealError(f"{label} exceeds {maximum_length} characters")
    return value


def _strict_sha256(value: object, label: str) -> str:
    text = _strict_string(value, label, minimum_length=64, maximum_length=64)
    if SHA256_RE.fullmatch(text) is None:
        raise SealError(f"{label} must be a lowercase SHA-256 digest")
    return text


def _strict_absolute_path(value: object, label: str) -> str:
    text = _strict_string(value, label)
    if not Path(text).is_absolute() or "\x00" in text:
        raise SealError(f"{label} must be an absolute path without NUL")
    return text


def _strict_boolean(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise SealError(f"{label} must be a boolean")
    return value


def _mode(value: os.stat_result) -> int:
    return stat.S_IMODE(value.st_mode)


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        stat.S_IFMT(value.st_mode),
        _mode(value),
        value.st_nlink,
    )


def _same_object(left: os.stat_result, right: os.stat_result) -> bool:
    return left.st_dev == right.st_dev and left.st_ino == right.st_ino


def _regular_record(value: os.stat_result, body: bytes, *, mode_after: int) -> dict[str, Any]:
    return {
        "byte_count": len(body),
        "ctime_ns_before": value.st_ctime_ns,
        "device": value.st_dev,
        "inode": value.st_ino,
        "mode_after": mode_after,
        "mode_before": _mode(value),
        "mtime_ns": value.st_mtime_ns,
        "nlink": value.st_nlink,
        "sha256": sha256_bytes(body),
    }


def _directory_record(value: os.stat_result) -> dict[str, Any]:
    return {
        "ctime_ns_before": value.st_ctime_ns,
        "device": value.st_dev,
        "inode": value.st_ino,
        "mode_after": RESULT_DIRECTORY_MODE_AFTER,
        "mode_before": _mode(value),
        "mtime_ns": value.st_mtime_ns,
        "nlink": value.st_nlink,
    }


def _require_absolute_resolved(path: Path, label: str) -> Path:
    if not path.is_absolute():
        raise SealError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=True)
    except (OSError, FileNotFoundError) as error:
        raise SealError(f"{label} is unavailable: {error}") from error
    if resolved != path:
        raise SealError(f"{label} must be resolved without symlinks or traversal")
    return path


def _require_beneath(path: Path, root: Path, label: str) -> None:
    try:
        path.relative_to(root)
    except ValueError as error:
        raise SealError(f"{label} is outside the allowlisted store root") from error


def _pread_all(fd: int, expected_size: int, maximum_bytes: int, label: str) -> bytes:
    if expected_size > maximum_bytes:
        raise SealError(f"{label} exceeds the {maximum_bytes}-byte limit")
    chunks: list[bytes] = []
    offset = 0
    while offset < expected_size:
        chunk = os.pread(fd, min(1024 * 1024, expected_size - offset), offset)
        if not chunk:
            break
        chunks.append(chunk)
        offset += len(chunk)
    body = b"".join(chunks)
    if len(body) != expected_size:
        raise SealError(f"{label} changed size while being read")
    return body


@dataclass
class RetainedFile:
    path: Path
    fd: int
    opened: os.stat_result
    body: bytes
    label: str

    def verify(
        self,
        *,
        modes: set[int],
        expected_sha256: str | None = None,
        expected_mtime_ns: int | None = None,
        expected_ctime_ns: int | None = None,
        expected_device: int | None = None,
        expected_inode: int | None = None,
        expected_nlink: int = 1,
    ) -> os.stat_result:
        descriptor = os.fstat(self.fd)
        try:
            logical = self.path.lstat()
        except OSError as error:
            raise SealError(f"{self.label} disappeared: {error}") from error
        if not stat.S_ISREG(descriptor.st_mode) or not stat.S_ISREG(logical.st_mode):
            raise SealError(f"{self.label} must remain a regular file")
        if not _same_object(descriptor, logical):
            raise SealError(f"{self.label} path was replaced")
        if descriptor.st_nlink != expected_nlink or logical.st_nlink != expected_nlink:
            raise SealError(f"{self.label} must have nlink {expected_nlink}")
        if _mode(descriptor) not in modes or _mode(logical) not in modes:
            rendered = ", ".join(f"{mode:04o}" for mode in sorted(modes))
            raise SealError(f"{self.label} mode must be one of {rendered}")
        if expected_device is not None and descriptor.st_dev != expected_device:
            raise SealError(f"{self.label} device differs from the plan")
        if expected_inode is not None and descriptor.st_ino != expected_inode:
            raise SealError(f"{self.label} inode differs from the plan")
        if expected_mtime_ns is not None and descriptor.st_mtime_ns != expected_mtime_ns:
            raise SealError(f"{self.label} mtime differs from the plan")
        if expected_ctime_ns is not None and descriptor.st_ctime_ns != expected_ctime_ns:
            raise SealError(f"{self.label} ctime changed after the authorized transition")
        if (
            expected_ctime_ns is None
            and _mode(descriptor) == _mode(self.opened)
            and descriptor.st_ctime_ns != self.opened.st_ctime_ns
        ):
            raise SealError(f"{self.label} metadata changed during this operation")
        if descriptor.st_size != len(self.body):
            raise SealError(f"{self.label} size changed")
        current = _pread_all(
            self.fd, descriptor.st_size, max(descriptor.st_size, 1), self.label
        )
        current_after = os.fstat(self.fd)
        if _identity(descriptor) != _identity(current_after):
            raise SealError(f"{self.label} changed while being reverified")
        digest = sha256_bytes(current)
        if expected_sha256 is not None and digest != expected_sha256:
            raise SealError(f"{self.label} content digest differs from the plan")
        if current != self.body:
            raise SealError(f"{self.label} content changed during this operation")
        return descriptor


@dataclass
class RetainedDirectory:
    path: Path
    fd: int
    opened: os.stat_result
    label: str

    def verify(
        self,
        *,
        modes: set[int],
        entries: set[str] | None = None,
        expected_mtime_ns: int | None = None,
        expected_ctime_ns: int | None = None,
        expected_device: int | None = None,
        expected_inode: int | None = None,
        allow_metadata_change: bool = False,
    ) -> os.stat_result:
        descriptor = os.fstat(self.fd)
        try:
            logical = self.path.lstat()
        except OSError as error:
            raise SealError(f"{self.label} disappeared: {error}") from error
        if not stat.S_ISDIR(descriptor.st_mode) or not stat.S_ISDIR(logical.st_mode):
            raise SealError(f"{self.label} must remain a directory")
        if not _same_object(descriptor, logical):
            raise SealError(f"{self.label} path was replaced")
        if _mode(descriptor) not in modes or _mode(logical) not in modes:
            rendered = ", ".join(f"{mode:04o}" for mode in sorted(modes))
            raise SealError(f"{self.label} mode must be one of {rendered}")
        if expected_device is not None and descriptor.st_dev != expected_device:
            raise SealError(f"{self.label} device differs from the plan")
        if expected_inode is not None and descriptor.st_ino != expected_inode:
            raise SealError(f"{self.label} inode differs from the plan")
        if expected_mtime_ns is not None and descriptor.st_mtime_ns != expected_mtime_ns:
            raise SealError(f"{self.label} mtime differs from the plan")
        if expected_ctime_ns is not None and descriptor.st_ctime_ns != expected_ctime_ns:
            raise SealError(f"{self.label} ctime changed after the authorized transition")
        if (
            expected_ctime_ns is None
            and not allow_metadata_change
            and _mode(descriptor) == _mode(self.opened)
            and descriptor.st_ctime_ns != self.opened.st_ctime_ns
        ):
            raise SealError(f"{self.label} metadata changed during this operation")
        if entries is not None:
            observed = set(os.listdir(self.fd))
            if observed != entries:
                raise SealError(
                    f"{self.label} entries differ: expected {sorted(entries)}, "
                    f"observed {sorted(observed)}"
                )
        return descriptor


def _open_retained_file(
    stack: ExitStack,
    path: Path,
    label: str,
    *,
    maximum_bytes: int,
    exact_mode: int | None = None,
    dir_fd: int | None = None,
    name: str | None = None,
) -> RetainedFile:
    _require_absolute_resolved(path, label)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(name if name is not None else path, flags, dir_fd=dir_fd)
    except OSError as error:
        raise SealError(f"cannot retain {label}: {error}") from error
    stack.callback(os.close, fd)
    descriptor = os.fstat(fd)
    logical = path.lstat()
    if not stat.S_ISREG(descriptor.st_mode) or not _same_object(descriptor, logical):
        raise SealError(f"{label} was replaced while being opened")
    if descriptor.st_nlink != 1:
        raise SealError(f"{label} must have nlink 1")
    if exact_mode is not None and _mode(descriptor) != exact_mode:
        raise SealError(f"{label} mode must be {exact_mode:04o}")
    before = _identity(descriptor)
    body = _pread_all(fd, descriptor.st_size, maximum_bytes, label)
    after = os.fstat(fd)
    logical_after = path.lstat()
    if before != _identity(after) or before != _identity(logical_after):
        raise SealError(f"{label} changed while being retained")
    return RetainedFile(path=path, fd=fd, opened=descriptor, body=body, label=label)


def _open_retained_directory(
    stack: ExitStack,
    path: Path,
    label: str,
    *,
    exact_mode: int | None = None,
) -> RetainedDirectory:
    _require_absolute_resolved(path, label)
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_DIRECTORY", 0)
    )
    try:
        fd = os.open(path, flags)
    except OSError as error:
        raise SealError(f"cannot retain {label}: {error}") from error
    stack.callback(os.close, fd)
    descriptor = os.fstat(fd)
    logical = path.lstat()
    if not stat.S_ISDIR(descriptor.st_mode) or not _same_object(descriptor, logical):
        raise SealError(f"{label} was replaced while being opened")
    if exact_mode is not None and _mode(descriptor) != exact_mode:
        raise SealError(f"{label} mode must be {exact_mode:04o}")
    return RetainedDirectory(path=path, fd=fd, opened=descriptor, label=label)


def _parse_json(body: bytes, label: str) -> dict[str, Any]:
    def reject_duplicate_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise SealError(f"{label} contains duplicate JSON key {key!r}")
            value[key] = item
        return value

    def reject_nonfinite(value: str) -> None:
        raise SealError(f"{label} contains non-finite JSON number {value}")

    try:
        raw = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=reject_duplicate_pairs,
            parse_constant=reject_nonfinite,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SealError(f"{label} is not valid UTF-8 JSON: {error}") from error
    if not isinstance(raw, dict):
        raise SealError(f"{label} must contain a JSON object")
    return raw


def _file_identity(path: Path) -> dict[str, Any]:
    with ExitStack() as stack:
        retained = _open_retained_file(
            stack, path, str(path), maximum_bytes=32 * 1024 * 1024
        )
        return {
            "byte_count": len(retained.body),
            "path": str(path),
            "sha256": sha256_bytes(retained.body),
        }


def catalog_free_validate(result_path: Path) -> dict[str, Any]:
    """Invoke only the importer's catalog-free byte validator."""

    source_text = str(CORPUS_SOURCE_ROOT)
    if source_text not in sys.path:
        sys.path.insert(0, source_text)
    from himr_corpus.asr_result_importer import (  # noqa: PLC0415
        validate_asr_whispercpp_result_file,
    )

    return validate_asr_whispercpp_result_file(result_path)


CatalogValidator = Callable[[Path], dict[str, Any]]
QueueValidator = Callable[[Path], tuple[dict[str, Any], list[dict[str, Any]]]]
BatchValidator = Callable[[Path], tuple[dict[str, Any], list[dict[str, Any]]]]


def authoritative_queue_validate(
    manifest_path: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Run the queue materializer's deterministic contract/evidence replay."""

    pipeline_source = str(REPOSITORY_ROOT / "pipeline")
    if pipeline_source not in sys.path:
        sys.path.insert(0, pipeline_source)
    from preprocess_asr_queue import validate_queue  # noqa: PLC0415

    return validate_queue(manifest_path)


def authoritative_batch_validate(
    manifest_path: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Run the batch materializer's catalog/input deterministic replay."""

    pipeline_source = str(REPOSITORY_ROOT / "pipeline")
    if pipeline_source not in sys.path:
        sys.path.insert(0, pipeline_source)
    from asr_whispercpp_batch import validate_batch  # noqa: PLC0415

    return validate_batch(manifest_path)


def _source_kind(manifest: dict[str, Any], path: Path) -> str:
    if "batch_id" in manifest and "queue_id" not in manifest:
        return "long_window_batch"
    if "queue_id" in manifest and "batch_id" not in manifest:
        return "short_preprocess_queue"
    raise SealError(f"{path} is neither one batch nor one queue manifest")


def _manifest_id(manifest: dict[str, Any], kind: str) -> str:
    key = "batch_id" if kind == "long_window_batch" else "queue_id"
    value = manifest.get(key)
    if not isinstance(value, str) or not value:
        raise SealError(f"source manifest requires {key}")
    identity = manifest.get("identity_sha256")
    if not isinstance(identity, str) or len(identity) != 64:
        raise SealError("source manifest identity_sha256 is invalid")
    if value.rsplit("_", 1)[-1] != identity[:32]:
        raise SealError(f"{key} does not match identity_sha256")
    return value


def _source_eligibility(
    stack: ExitStack,
    manifest_path: Path,
    *,
    authoritative_queue: bool = False,
    authoritative_batch: bool = False,
    queue_validator: QueueValidator = authoritative_queue_validate,
    batch_validator: BatchValidator = authoritative_batch_validate,
) -> tuple[
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[RetainedFile],
    dict[str, Any] | None,
]:
    retained = _open_retained_file(
        stack,
        manifest_path,
        f"source manifest {manifest_path}",
        maximum_bytes=MANIFEST_MAX_BYTES,
        exact_mode=SOURCE_FILE_MODE,
    )
    manifest = _parse_json(retained.body, f"source manifest {manifest_path}")
    kind = _source_kind(manifest, manifest_path)
    source_id = _manifest_id(manifest, kind)
    if authoritative_queue and authoritative_batch:
        raise SealError("one source cannot use both queue and batch authority")
    authority_contract: dict[str, Any] | None = None
    validated_source_orders: list[dict[str, Any]] | None = None
    if authoritative_queue and kind == "short_preprocess_queue":
        try:
            validated_manifest, validated_source_orders = queue_validator(manifest_path)
        except Exception as error:
            raise SealError(
                f"authoritative preprocess ASR queue validation failed: {error}"
            ) from error
        retained.verify(
            modes={SOURCE_FILE_MODE}, expected_sha256=sha256_bytes(retained.body)
        )
        if validated_manifest != manifest:
            raise SealError("authoritative queue manifest differs from retained bytes")
        if not isinstance(validated_source_orders, list) or len(
            validated_source_orders
        ) != manifest.get("work_order_count"):
            raise SealError("authoritative queue work-order coverage differs")
        safety = manifest.get("safety")
        if not isinstance(safety, dict):
            raise SealError("authoritative queue safety contract is missing")
        authority_contract = {
            "manifest_schema_version": manifest.get("schema_version"),
            "materializer": manifest.get("materializer"),
            "materializer_implementation_version": manifest.get(
                "implementation_version"
            ),
            "queue_manifest_schema": _file_identity(
                PREPROCESS_QUEUE_SCHEMA_SOURCE.resolve()
            ),
            "queue_validator": _file_identity(PREPROCESS_QUEUE_SOURCE.resolve()),
            "safety_sha256": sha256_bytes(canonical_bytes(safety)),
        }
    elif authoritative_batch and kind == "long_window_batch":
        try:
            validated_manifest, validated_source_orders = batch_validator(manifest_path)
        except Exception as error:
            raise SealError(
                f"authoritative raw ASR batch validation failed: {error}"
            ) from error
        retained.verify(
            modes={SOURCE_FILE_MODE}, expected_sha256=sha256_bytes(retained.body)
        )
        if validated_manifest != manifest:
            raise SealError("authoritative batch manifest differs from retained bytes")
        if not isinstance(validated_source_orders, list) or len(
            validated_source_orders
        ) != manifest.get("work_order_count"):
            raise SealError("authoritative batch work-order coverage differs")
        safety = manifest.get("safety")
        catalog = manifest.get("catalog")
        if not isinstance(safety, dict) or not isinstance(catalog, dict):
            raise SealError("authoritative batch safety/catalog contract is missing")
        catalog_binding_sha256 = catalog.get("binding_sha256")
        if not isinstance(catalog_binding_sha256, str):
            raise SealError("authoritative batch catalog binding is missing")
        authority_contract = {
            "batch_manifest_schema": _file_identity(
                ASR_BATCH_SCHEMA_SOURCE.resolve()
            ),
            "batch_validator": _file_identity(ASR_BATCH_SOURCE.resolve()),
            "catalog_binding_sha256": catalog_binding_sha256,
            "manifest_schema_version": manifest.get("schema_version"),
            "materializer": manifest.get("materializer"),
            "materializer_implementation_version": manifest.get(
                "implementation_version"
            ),
            "safety_sha256": sha256_bytes(canonical_bytes(safety)),
        }
    records = manifest.get("work_orders")
    if not isinstance(records, list) or not records:
        raise SealError(f"source {source_id} requires a non-empty work_orders array")
    if manifest.get("work_order_count") != len(records):
        raise SealError(f"source {source_id} work_order_count is inconsistent")
    if validated_source_orders is not None:
        for ordinal, (entry, order) in enumerate(
            zip(records, validated_source_orders, strict=True), 1
        ):
            if (
                not isinstance(order, dict)
                or entry.get("job_id") != order.get("job_id")
                or entry.get("canonical_sha256")
                != sha256_bytes(canonical_bytes(order))
            ):
                raise SealError(
                    f"authoritative source work order {ordinal} differs from its manifest pin"
                )
    selected: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    retained_files = [retained]
    seen: set[tuple[str, str]] = set()
    for expected_ordinal, record in enumerate(records, 1):
        if not isinstance(record, dict) or record.get("ordinal") != expected_ordinal:
            raise SealError(f"source {source_id} work orders are not strictly ordered")
        relative = record.get("path")
        job_id = record.get("job_id")
        canonical_sha = record.get("canonical_sha256")
        physical_sha = record.get("sha256")
        byte_count = record.get("byte_count")
        if not all(isinstance(value, str) and value for value in (relative, job_id)):
            raise SealError(f"source {source_id} work-order reference is incomplete")
        if not all(
            isinstance(value, str) and len(value) == 64
            for value in (canonical_sha, physical_sha)
        ):
            raise SealError(f"source {source_id} work-order hashes are invalid")
        work_order_path = manifest_path.parent / relative
        _require_absolute_resolved(work_order_path, f"source work order {source_id}/{expected_ordinal}")
        if work_order_path.parent != manifest_path.parent / "work-orders":
            raise SealError(f"source {source_id} work-order path escapes its closed directory")
        work = _open_retained_file(
            stack,
            work_order_path,
            f"source work order {source_id}/{expected_ordinal}",
            maximum_bytes=WORK_ORDER_MAX_BYTES,
            exact_mode=SOURCE_FILE_MODE,
        )
        retained_files.append(work)
        if len(work.body) != byte_count or sha256_bytes(work.body) != physical_sha:
            raise SealError(f"source {source_id} work-order physical identity differs")
        work_raw = _parse_json(work.body, f"source work order {source_id}/{expected_ordinal}")
        if sha256_bytes(canonical_bytes(work_raw)) != canonical_sha:
            raise SealError(f"source {source_id} work-order canonical identity differs")
        if work_raw.get("job_id") != job_id:
            raise SealError(f"source {source_id} work-order job_id differs")
        input_value = work_raw.get("input")
        if not isinstance(input_value, dict):
            raise SealError(f"source {source_id} work-order input is invalid")
        input_sha256 = input_value.get("expected_sha256")
        if not isinstance(input_sha256, str) or len(input_sha256) != 64:
            raise SealError(f"source {source_id} work-order input digest is invalid")
        key = (job_id, canonical_sha)
        if key in seen:
            raise SealError(f"source {source_id} contains a duplicate job/work-order tuple")
        seen.add(key)
        routing_hint = record.get("routing_hint") if kind == "short_preprocess_queue" else None
        if kind == "short_preprocess_queue" and not (
            routing_hint is None
            or (isinstance(routing_hint, str) and len(routing_hint) <= 256)
        ):
            raise SealError(
                f"source {source_id} work-order routing_hint must be string|null "
                "with at most 256 characters"
            )
        common = {
            "canonical_sha256": canonical_sha,
            "job_id": job_id,
            "input_sha256": input_sha256,
            "ordinal": expected_ordinal,
            "physical_sha256": physical_sha,
            "work_order_path": str(work_order_path),
        }
        if kind == "short_preprocess_queue" and routing_hint != "process":
            excluded.append(
                {
                    **common,
                    "reason": "sealed_queue_routing_hint_not_process",
                    "routing_hint": routing_hint,
                }
            )
        else:
            selected.append(common)
    software = manifest.get("software")
    engine = manifest.get("engine")
    if not isinstance(software, dict) or not isinstance(engine, dict):
        raise SealError(f"source {source_id} lacks software or engine identity")
    adapter = software.get("asr_adapter")
    if not isinstance(adapter, dict):
        raise SealError(f"source {source_id} lacks adapter identity")
    source_record = {
        "adapter": {
            "byte_count": adapter.get("byte_count"),
            "implementation_version": adapter.get("implementation_version"),
            "name": adapter.get("name"),
            "sha256": adapter.get("sha256"),
        },
        "byte_count": len(retained.body),
        "identity_sha256": manifest["identity_sha256"],
        "kind": kind,
        "manifest_path": str(manifest_path),
        "manifest_sha256": sha256_bytes(retained.body),
        "source_id": source_id,
        "engine_sha256": engine.get("expected_sha256"),
        "engine_version": engine.get("version_label"),
    }
    for key in ("byte_count", "implementation_version", "name", "sha256"):
        if source_record["adapter"][key] is None:
            raise SealError(f"source {source_id} adapter identity is incomplete")
    if not isinstance(source_record["engine_sha256"], str) or not isinstance(
        source_record["engine_version"], str
    ):
        raise SealError(f"source {source_id} engine identity is incomplete")
    for source_file in retained_files:
        source_file.verify(
            modes={SOURCE_FILE_MODE}, expected_sha256=sha256_bytes(source_file.body)
        )
    return source_record, selected, excluded, retained_files, authority_contract


@dataclass
class RetainedResult:
    ancestors: list[RetainedDirectory]
    directory: RetainedDirectory
    files: dict[str, RetainedFile]
    raw: dict[str, Any]
    record: dict[str, Any]


def _open_result_ancestors(
    stack: ExitStack, result_directory: Path, store_root: Path
) -> list[RetainedDirectory]:
    _require_beneath(result_directory, store_root, f"result directory {result_directory}")
    relative = result_directory.relative_to(store_root)
    ancestors: list[RetainedDirectory] = []
    current = store_root
    paths = [store_root]
    for component in relative.parts[:-1]:
        current = current / component
        paths.append(current)
    for path in paths:
        retained = _open_retained_directory(
            stack, path, f"retained result ancestor {path}"
        )
        retained.verify(modes={_mode(retained.opened)})
        ancestors.append(retained)
    return ancestors


def _verify_result_ancestors(results: list[RetainedResult]) -> None:
    for result in results:
        for ancestor in result.ancestors:
            ancestor.verify(modes={_mode(ancestor.opened)})


def _catalog_summary(value: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "artifact_count",
        "job_id",
        "model_id",
        "processing_run_id",
        "recipe_id",
        "recording_id",
        "rendition_id",
        "result_envelope_sha256",
        "result_key",
        "segment_count",
        "token_count",
    )
    return {key: value.get(key) for key in keys}


def _open_result(
    stack: ExitStack,
    result_path: Path,
    store_root: Path,
    eligibility: dict[
        tuple[str, str], tuple[dict[str, Any], dict[str, Any]]
    ],
    validator: CatalogValidator,
) -> RetainedResult:
    _require_absolute_resolved(result_path, f"allowlisted result {result_path}")
    _require_beneath(result_path, store_root, f"allowlisted result {result_path}")
    if result_path.name != "result.json":
        raise SealError(f"allowlisted path must end in result.json: {result_path}")
    ancestors = _open_result_ancestors(stack, result_path.parent, store_root)
    directory = _open_retained_directory(
        stack, result_path.parent, f"result directory {result_path.parent}"
    )
    directory.verify(
        modes={RESULT_DIRECTORY_MODE_BEFORE}, entries=set(RESULT_FILENAMES)
    )
    files: dict[str, RetainedFile] = {}
    for name in RESULT_FILENAMES:
        path = result_path.parent / name
        retained = _open_retained_file(
            stack,
            path,
            f"result artifact {path}",
            maximum_bytes=MAX_FILE_BYTES[name],
            exact_mode=RESULT_FILE_MODE_BEFORE,
            dir_fd=directory.fd,
            name=name,
        )
        files[name] = retained
    raw = _parse_json(files["result.json"].body, f"result envelope {result_path}")
    source_key = (raw.get("job_id"), raw.get("work_order_sha256"))
    if source_key not in eligibility:
        raise SealError(f"result is not selected by the sealed sources: {result_path}")
    source, eligible = eligibility[source_key]
    if raw.get("job_id") != eligible["job_id"]:
        raise SealError(f"{result_path} does not match its allowlisted source job")
    if raw.get("work_order_sha256") != eligible["canonical_sha256"]:
        raise SealError(f"{result_path} does not match its source work-order identity")
    if raw.get("input", {}).get("sha256") != eligible["input_sha256"]:
        raise SealError(f"{result_path} does not match its source input identity")
    if raw.get("result_path") != str(result_path):
        raise SealError(f"{result_path} envelope names a different result path")
    if raw.get("processing_run", {}).get("implementation_version") != source["adapter"][
        "implementation_version"
    ]:
        raise SealError(f"{result_path} adapter version differs from its source manifest")
    if raw.get("engine", {}).get("version") != source["engine_version"]:
        raise SealError(f"{result_path} engine version differs from its source manifest")
    if raw.get("engine", {}).get("sha256") != source["engine_sha256"]:
        raise SealError(f"{result_path} engine digest differs from its source manifest")
    result_key = raw.get("result_key")
    if not isinstance(result_key, str) or len(result_key) != 64:
        raise SealError(f"{result_path} has an invalid result_key")
    if result_path.parent.name != result_key:
        raise SealError(f"{result_path} result-key directory differs from its envelope")
    # The authoritative validator opens paths itself.  Retained descriptors span it,
    # and every descriptor/path pair is reverified immediately afterwards.
    try:
        validation = validator(result_path)
    except Exception as error:
        raise SealError(f"catalog-free validation failed for {result_path}: {error}") from error
    if validation.get("result_envelope_sha256") != sha256_bytes(canonical_bytes(raw)):
        raise SealError(f"catalog-free validator canonical digest differs for {result_path}")
    for retained in files.values():
        retained.verify(modes={RESULT_FILE_MODE_BEFORE})
    directory.verify(
        modes={RESULT_DIRECTORY_MODE_BEFORE}, entries=set(RESULT_FILENAMES)
    )
    file_records = []
    for name in RESULT_FILENAMES:
        retained = files[name]
        file_records.append(
            {
                "name": name,
                "path": str(retained.path),
                "role": ROLE_BY_NAME[name],
                **_regular_record(
                    retained.opened, retained.body, mode_after=RESULT_FILE_MODE_AFTER
                ),
            }
        )
    record = {
        "catalog_free_validation": _catalog_summary(validation),
        "directory": {
            "path": str(result_path.parent),
            **_directory_record(directory.opened),
        },
        "files": file_records,
        "job_id": raw["job_id"],
        "input_sha256": eligible["input_sha256"],
        "ordinal": 0,
        "recipe_id": raw.get("recipe_id"),
        "result_key": result_key,
        "result_path": str(result_path),
        "source_id": source["source_id"],
        "source_kind": source["kind"],
        "source_ordinal": eligible["ordinal"],
        "work_order_sha256": eligible["canonical_sha256"],
    }
    return RetainedResult(
        ancestors=ancestors, directory=directory, files=files, raw=raw, record=record
    )


def _document_identity(document: dict[str, Any], omitted: set[str]) -> str:
    return sha256_bytes(canonical_bytes({k: v for k, v in document.items() if k not in omitted}))


def _queue_only_source_authority(
    source: dict[str, Any], queue_contract: dict[str, Any] | None
) -> dict[str, Any]:
    """Bind a v2 plan/receipt to one exact sealed process-routed queue."""

    if source.get("kind") != "short_preprocess_queue":
        raise SealError("queue-only source authority requires a short preprocess queue")
    if queue_contract is None:
        raise SealError("queue-only source authority requires validated queue provenance")
    return {
        "mode": "queue_only",
        "queue_id": source["source_id"],
        "queue_identity_sha256": source["identity_sha256"],
        "queue_manifest_path": source["manifest_path"],
        "queue_manifest_sha256": source["manifest_sha256"],
        "queue_contract": queue_contract,
        "schema_version": QUEUE_ONLY_SOURCE_AUTHORITY_VERSION,
        "selection_policy": QUEUE_ONLY_SELECTION_POLICY,
    }


def _batch_only_source_authority(
    source: dict[str, Any], batch_contract: dict[str, Any] | None
) -> dict[str, Any]:
    """Bind a v3 plan/receipt to one authoritatively replayed raw ASR batch."""

    if source.get("kind") != "long_window_batch":
        raise SealError("batch-only source authority requires a long-window batch")
    if batch_contract is None:
        raise SealError("batch-only source authority requires validated batch provenance")
    return {
        "batch_contract": batch_contract,
        "batch_id": source["source_id"],
        "batch_identity_sha256": source["identity_sha256"],
        "batch_manifest_path": source["manifest_path"],
        "batch_manifest_sha256": source["manifest_sha256"],
        "mode": "batch_only",
        "schema_version": BATCH_ONLY_SOURCE_AUTHORITY_VERSION,
        "selection_policy": BATCH_ONLY_SELECTION_POLICY,
    }


def _ensure_private_directory(path: Path) -> None:
    if not path.is_absolute():
        raise SealError("control directory must be absolute")
    missing: list[Path] = []
    current = path
    while not current.exists():
        missing.append(current)
        current = current.parent
    _require_absolute_resolved(current, f"existing control ancestor {current}")
    for item in reversed(missing):
        try:
            item.mkdir(mode=CONTROL_DIRECTORY_MODE)
        except OSError as error:
            raise SealError(f"cannot create private control directory {item}: {error}") from error
    for item in (path, path.parent):
        if not item.is_dir() or item.is_symlink():
            raise SealError(f"control path {item} must be a real directory")
        if _mode(item.lstat()) != CONTROL_DIRECTORY_MODE:
            raise SealError(f"control directory {item} must be mode 0700")


def _atomic_private_json(
    path: Path,
    value: dict[str, Any],
    *,
    maximum: int,
    retained_parent: RetainedDirectory | None = None,
) -> None:
    body = canonical_bytes(value) + b"\n"
    if len(body) > maximum:
        raise SealError(f"control document exceeds the {maximum}-byte limit")
    if not path.is_absolute() or path.name in {"", ".", ".."}:
        raise SealError("control document path must be absolute and named")
    own_stack = ExitStack()
    parent = retained_parent
    if parent is None:
        parent = _open_retained_directory(
            own_stack,
            _require_absolute_resolved(path.parent, "control document directory"),
            f"control document directory {path.parent}",
            exact_mode=CONTROL_DIRECTORY_MODE,
        )
    if parent.path != path.parent:
        own_stack.close()
        raise SealError("retained control directory differs from document parent")
    parent.verify(modes={CONTROL_DIRECTORY_MODE}, allow_metadata_change=True)
    try:
        os.stat(path.name, dir_fd=parent.fd, follow_symlinks=False)
    except FileNotFoundError:
        pass
    except OSError as error:
        own_stack.close()
        raise SealError(f"cannot inspect control target {path}: {error}") from error
    else:
        own_stack.close()
        raise SealError(f"refusing to replace existing control document {path}")
    temp_name = f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    flags = (
        os.O_RDWR
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    fd: int | None = None
    linked = False
    try:
        fd = os.open(temp_name, flags, 0o600, dir_fd=parent.fd)
        offset = 0
        while offset < len(body):
            offset += os.write(fd, body[offset:])
        os.fsync(fd)
        os.fchmod(fd, PLAN_FILE_MODE)
        os.fsync(fd)
        if _pread_all(fd, len(body), maximum, f"control temp {path}") != body:
            raise SealError("retained control temp bytes differ before publication")
        os.link(
            temp_name,
            path.name,
            src_dir_fd=parent.fd,
            dst_dir_fd=parent.fd,
            follow_symlinks=False,
        )
        linked = True
        descriptor = os.fstat(fd)
        target = os.stat(path.name, dir_fd=parent.fd, follow_symlinks=False)
        logical = path.lstat()
        if not _same_object(descriptor, target) or not _same_object(descriptor, logical):
            raise SealError(
                f"committed control path {path} differs from its retained temp"
            )
        os.unlink(temp_name, dir_fd=parent.fd)
        descriptor = os.fstat(fd)
        target = os.stat(path.name, dir_fd=parent.fd, follow_symlinks=False)
        logical = path.lstat()
        if (
            not _same_object(descriptor, target)
            or not _same_object(descriptor, logical)
            or descriptor.st_nlink != 1
            or target.st_nlink != 1
            or _mode(descriptor) != PLAN_FILE_MODE
            or _mode(target) != PLAN_FILE_MODE
            or descriptor.st_size != len(body)
            or _pread_all(fd, descriptor.st_size, maximum, f"committed control {path}")
            != body
        ):
            raise SealError(f"committed control document {path} failed final verification")
        os.fsync(parent.fd)
        parent.verify(modes={CONTROL_DIRECTORY_MODE}, allow_metadata_change=True)
        os.close(fd)
        fd = None
    except Exception as error:
        if linked and fd is not None:
            try:
                target = os.stat(path.name, dir_fd=parent.fd, follow_symlinks=False)
                if _same_object(target, os.fstat(fd)):
                    os.unlink(path.name, dir_fd=parent.fd)
            except OSError:
                pass
        try:
            os.unlink(temp_name, dir_fd=parent.fd)
        except OSError:
            pass
        try:
            os.fsync(parent.fd)
        except OSError:
            pass
        if fd is not None:
            os.close(fd)
        if isinstance(error, SealError):
            raise
        raise SealError(f"cannot commit private control document {path}: {error}") from error
    finally:
        own_stack.close()


def _ensure_apply_lock(control_root: Path) -> Path:
    control_root = _require_absolute_resolved(control_root, "seal control root")
    with ExitStack() as stack:
        directory = _open_retained_directory(
            stack,
            control_root,
            "seal control root",
            exact_mode=CONTROL_DIRECTORY_MODE,
        )
        flags = (
            os.O_RDWR
            | os.O_CREAT
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            fd = os.open(
                APPLY_LOCK_FILENAME,
                flags,
                APPLY_LOCK_FILE_MODE,
                dir_fd=directory.fd,
            )
        except OSError as error:
            raise SealError(f"cannot create/open seal apply lock: {error}") from error
        stack.callback(os.close, fd)
        descriptor = os.fstat(fd)
        logical = os.stat(
            APPLY_LOCK_FILENAME, dir_fd=directory.fd, follow_symlinks=False
        )
        if (
            not stat.S_ISREG(descriptor.st_mode)
            or not stat.S_ISREG(logical.st_mode)
            or not _same_object(descriptor, logical)
            or descriptor.st_nlink != 1
            or logical.st_nlink != 1
        ):
            raise SealError("seal apply lock must be one retained single-link file")
        os.fchmod(fd, APPLY_LOCK_FILE_MODE)
        os.fsync(fd)
        os.fsync(directory.fd)
        descriptor = os.fstat(fd)
        logical = (control_root / APPLY_LOCK_FILENAME).lstat()
        if (
            not _same_object(descriptor, logical)
            or _mode(descriptor) != APPLY_LOCK_FILE_MODE
            or _mode(logical) != APPLY_LOCK_FILE_MODE
            or descriptor.st_nlink != 1
        ):
            raise SealError("seal apply lock changed during creation")
        directory.verify(modes={CONTROL_DIRECTORY_MODE}, allow_metadata_change=True)
    return control_root / APPLY_LOCK_FILENAME


def _retain_apply_lock(
    stack: ExitStack, control_root: Path, *, acquire: bool
) -> int:
    path = _require_absolute_resolved(
        control_root / APPLY_LOCK_FILENAME, "seal apply lock"
    )
    flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as error:
        raise SealError(f"cannot retain seal apply lock: {error}") from error
    stack.callback(os.close, fd)
    descriptor = os.fstat(fd)
    logical = path.lstat()
    if (
        not stat.S_ISREG(descriptor.st_mode)
        or not stat.S_ISREG(logical.st_mode)
        or not _same_object(descriptor, logical)
        or descriptor.st_nlink != 1
        or logical.st_nlink != 1
        or _mode(descriptor) != APPLY_LOCK_FILE_MODE
        or _mode(logical) != APPLY_LOCK_FILE_MODE
    ):
        raise SealError("seal apply lock must remain one retained mode-0600 file")
    if acquire:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
        except OSError as error:
            raise SealError(f"cannot acquire seal apply lock: {error}") from error
        descriptor = os.fstat(fd)
        logical = path.lstat()
        if (
            not _same_object(descriptor, logical)
            or descriptor.st_nlink != 1
            or _mode(descriptor) != APPLY_LOCK_FILE_MODE
            or _mode(logical) != APPLY_LOCK_FILE_MODE
        ):
            raise SealError("seal apply lock was replaced after acquisition")
    return fd


def build_plan(
    *,
    batch_manifest: Path | None = None,
    queue_manifest: Path | None = None,
    result_paths: Iterable[Path],
    output_directory: Path,
    store_root: Path = DEFAULT_STORE_ROOT,
    validator: CatalogValidator = catalog_free_validate,
    queue_validator: QueueValidator = authoritative_queue_validate,
    batch_validator: BatchValidator = authoritative_batch_validate,
    source_mode: str = TWO_SOURCE_CLI_MODE,
) -> Path:
    if source_mode == TWO_SOURCE_CLI_MODE:
        if batch_manifest is None:
            raise SealError("two-source-v1 requires --batch-manifest")
        if queue_manifest is None:
            raise SealError("two-source-v1 requires --queue-manifest")
        source_paths = (batch_manifest, queue_manifest)
        plan_schema_version = TWO_SOURCE_PLAN_SCHEMA_VERSION
        required_kinds = {"long_window_batch", "short_preprocess_queue"}
    elif source_mode == QUEUE_ONLY_CLI_MODE:
        if batch_manifest is not None:
            raise SealError("queue-only-v2 forbids --batch-manifest")
        if queue_manifest is None:
            raise SealError("queue-only-v2 requires --queue-manifest")
        source_paths = (queue_manifest,)
        plan_schema_version = QUEUE_ONLY_PLAN_SCHEMA_VERSION
        required_kinds = {"short_preprocess_queue"}
    elif source_mode == BATCH_ONLY_CLI_MODE:
        if batch_manifest is None:
            raise SealError("batch-only-v3 requires --batch-manifest")
        if queue_manifest is not None:
            raise SealError("batch-only-v3 forbids --queue-manifest")
        source_paths = (batch_manifest,)
        plan_schema_version = BATCH_ONLY_PLAN_SCHEMA_VERSION
        required_kinds = {"long_window_batch"}
    else:
        raise SealError(f"unsupported source mode: {source_mode}")
    store_root = _require_absolute_resolved(store_root, "ASR result store root")
    if _mode(store_root.lstat()) != STORE_ROOT_MODE:
        raise SealError("ASR result store root must be mode 0700")
    paths = list(result_paths)
    if not paths or len(paths) != len(set(paths)):
        raise SealError("result allowlist must be non-empty and contain no duplicates")
    expected_output_directory = store_root / "sealing-control" / "plans"
    if output_directory != expected_output_directory:
        raise SealError(
            f"plan output directory must be the closed private path {expected_output_directory}"
        )
    # Establish the private control containers before retaining the result-store
    # ancestor chain, so this authorized setup is outside the observation window.
    control_root = output_directory.parent
    _ensure_private_directory(output_directory)
    _ensure_private_directory(control_root / "receipts")
    _ensure_apply_lock(control_root)
    with ExitStack() as stack:
        retained_control = {
            path.name if path != control_root else "control": _open_retained_directory(
                stack,
                path,
                f"retained seal control directory {path}",
                exact_mode=CONTROL_DIRECTORY_MODE,
            )
            for path in (control_root, output_directory, control_root / "receipts")
        }
        _retain_apply_lock(stack, control_root, acquire=False)
        source_records: list[dict[str, Any]] = []
        authority_contract_by_source: dict[str, dict[str, Any]] = {}
        retained_source_files: list[RetainedFile] = []
        selected_by_tuple: dict[tuple[str, str], tuple[dict[str, Any], dict[str, Any]]] = {}
        excluded: list[dict[str, Any]] = []
        for source_path in source_paths:
            (
                source,
                selected,
                source_excluded,
                source_files,
                authority_contract,
            ) = _source_eligibility(
                stack,
                source_path,
                authoritative_queue=(
                    plan_schema_version == QUEUE_ONLY_PLAN_SCHEMA_VERSION
                ),
                authoritative_batch=(
                    plan_schema_version == BATCH_ONLY_PLAN_SCHEMA_VERSION
                ),
                queue_validator=queue_validator,
                batch_validator=batch_validator,
            )
            retained_source_files.extend(source_files)
            if any(existing["kind"] == source["kind"] for existing in source_records):
                raise SealError("seal-plan sources must have unique kinds")
            source_records.append(source)
            if authority_contract is not None:
                authority_contract_by_source[source["source_id"]] = authority_contract
            for item in selected:
                key = (item["job_id"], item["canonical_sha256"])
                if key in selected_by_tuple:
                    raise SealError("source manifests overlap on a selected job/work-order tuple")
                selected_by_tuple[key] = (source, item)
            excluded.extend({"source_id": source["source_id"], "source_kind": source["kind"], **item} for item in source_excluded)
        if (
            {record["kind"] for record in source_records} != required_kinds
            or len(source_records) != len(required_kinds)
        ):
            required = ", ".join(sorted(required_kinds))
            raise SealError(
                f"{source_mode} requires exactly these source kinds: {required}"
            )
        if len(paths) != len(selected_by_tuple):
            raise SealError(
                f"explicit allowlist has {len(paths)} results but sealed sources select "
                f"{len(selected_by_tuple)}"
            )
        retained_results: list[RetainedResult] = []
        matched: set[tuple[str, str]] = set()
        for result_path in paths:
            retained_result = _open_result(
                stack,
                result_path,
                store_root,
                selected_by_tuple,
                validator,
            )
            key = (
                retained_result.raw.get("job_id"),
                retained_result.raw.get("work_order_sha256"),
            )
            if key in matched:
                raise SealError(f"multiple results claim one selected source tuple: {result_path}")
            matched.add(key)
            retained_results.append(retained_result)
        if matched != set(selected_by_tuple):
            missing = sorted(set(selected_by_tuple) - matched)
            raise SealError(f"allowlist omits selected source tuples: {missing}")
        retained_results.sort(
            key=lambda result: (
                0 if result.record["source_kind"] == "long_window_batch" else 1,
                result.record["source_ordinal"],
            )
        )
        for ordinal, result in enumerate(retained_results, 1):
            result.record["ordinal"] = ordinal
        source_records.sort(key=lambda record: record["kind"])
        excluded.sort(key=lambda record: (record["source_id"], record["ordinal"]))
        plan: dict[str, Any] = {
            "authority": AUTHORITY,
            "created_at": utc_now(),
            "excluded_source_entries": excluded,
            "implementation": _file_identity(Path(__file__).resolve()),
            "importer_validator": _file_identity(IMPORTER_SOURCE.resolve()),
            "kind": PLAN_KIND,
            "policy": POLICY,
            "result_count": len(retained_results),
            "results": [result.record for result in retained_results],
            "schema_version": plan_schema_version,
            "sources": source_records,
            "state": "prepared_not_applied",
            "store_root": str(store_root),
        }
        if plan_schema_version == QUEUE_ONLY_PLAN_SCHEMA_VERSION:
            plan["source_authority"] = _queue_only_source_authority(
                source_records[0],
                authority_contract_by_source.get(source_records[0]["source_id"]),
            )
        elif plan_schema_version == BATCH_ONLY_PLAN_SCHEMA_VERSION:
            plan["source_authority"] = _batch_only_source_authority(
                source_records[0],
                authority_contract_by_source.get(source_records[0]["source_id"]),
            )
        identity = _document_identity(plan, set())
        plan["identity_sha256"] = identity
        plan["plan_id"] = f"asrsealplan_{identity[:32]}"
        receipt_name = f"asrsealreceipt_{identity[:32]}.json"
        plan["receipt_path"] = str(output_directory.parent / "receipts" / receipt_name)
        # IDs and the derived receipt path are intentionally outside the semantic
        # identity.  Validation recomputes all three from the semantic payload.
        for result in retained_results:
            for retained in result.files.values():
                retained.verify(modes={RESULT_FILE_MODE_BEFORE})
            result.directory.verify(
                modes={RESULT_DIRECTORY_MODE_BEFORE}, entries=set(RESULT_FILENAMES)
            )
        _verify_result_ancestors(retained_results)
        for source_file in retained_source_files:
            source_file.verify(
                modes={SOURCE_FILE_MODE}, expected_sha256=sha256_bytes(source_file.body)
            )
        output_path = output_directory / f"{plan['plan_id']}.json"
        _validate_plan_shape(plan)
        _atomic_private_json(
            output_path,
            plan,
            maximum=PLAN_MAX_BYTES,
            retained_parent=retained_control["plans"],
        )
        retained_control["plans"].verify(
            modes={CONTROL_DIRECTORY_MODE}, allow_metadata_change=True
        )
        retained_control["receipts"].verify(modes={CONTROL_DIRECTORY_MODE})
        retained_control["control"].verify(modes={CONTROL_DIRECTORY_MODE})
        return output_path


PLAN_KEYS_V1 = {
    "authority",
    "created_at",
    "excluded_source_entries",
    "identity_sha256",
    "implementation",
    "importer_validator",
    "kind",
    "plan_id",
    "policy",
    "receipt_path",
    "result_count",
    "results",
    "schema_version",
    "sources",
    "state",
    "store_root",
}
PLAN_KEYS_V2 = PLAN_KEYS_V1 | {"source_authority"}

SOURCE_KEYS = {
    "adapter", "byte_count", "engine_sha256", "engine_version",
    "identity_sha256", "kind", "manifest_path", "manifest_sha256", "source_id",
}
ADAPTER_KEYS = {"byte_count", "implementation_version", "name", "sha256"}
FILE_IDENTITY_KEYS = {"byte_count", "path", "sha256"}
EXCLUDED_KEYS = {
    "canonical_sha256", "input_sha256", "job_id", "ordinal", "physical_sha256",
    "reason", "routing_hint", "source_id", "source_kind", "work_order_path",
}
RESULT_KEYS = {
    "catalog_free_validation", "directory", "files", "input_sha256", "job_id",
    "ordinal", "recipe_id", "result_key", "result_path", "source_id",
    "source_kind", "source_ordinal", "work_order_sha256",
}
DIRECTORY_BEFORE_KEYS = {
    "ctime_ns_before", "device", "inode", "mode_after", "mode_before", "mtime_ns",
    "nlink", "path",
}
FILE_BEFORE_KEYS = {
    "byte_count", "ctime_ns_before", "device", "inode", "mode_after", "mode_before",
    "mtime_ns", "name", "nlink", "path", "role", "sha256",
}
CATALOG_SUMMARY_KEYS = {
    "artifact_count", "job_id", "model_id", "processing_run_id", "recipe_id",
    "recording_id", "rendition_id", "result_envelope_sha256", "result_key",
    "segment_count", "token_count",
}
SOURCE_AUTHORITY_KEYS = {
    "mode",
    "queue_id",
    "queue_identity_sha256",
    "queue_contract",
    "queue_manifest_path",
    "queue_manifest_sha256",
    "schema_version",
    "selection_policy",
}
BATCH_SOURCE_AUTHORITY_KEYS = {
    "batch_contract",
    "batch_id",
    "batch_identity_sha256",
    "batch_manifest_path",
    "batch_manifest_sha256",
    "mode",
    "schema_version",
    "selection_policy",
}
QUEUE_CONTRACT_KEYS = {
    "manifest_schema_version",
    "materializer",
    "materializer_implementation_version",
    "queue_manifest_schema",
    "queue_validator",
    "safety_sha256",
}
BATCH_CONTRACT_KEYS = {
    "batch_manifest_schema",
    "batch_validator",
    "catalog_binding_sha256",
    "manifest_schema_version",
    "materializer",
    "materializer_implementation_version",
    "safety_sha256",
}


def _exact_keys(value: object, expected: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        observed = set(value) if isinstance(value, dict) else set()
        raise SealError(
            f"{label} fields differ (missing={sorted(expected - observed)}, "
            f"unknown={sorted(observed - expected)})"
        )
    return value


def _validate_file_identity(value: dict[str, Any], label: str) -> None:
    _strict_integer(value.get("byte_count"), f"{label}.byte_count", minimum=1)
    _strict_absolute_path(value.get("path"), f"{label}.path")
    _strict_sha256(value.get("sha256"), f"{label}.sha256")


def _validate_queue_source_authority_values(
    source_authority: dict[str, Any], label: str
) -> None:
    if source_authority.get("mode") != "queue_only":
        raise SealError(f"{label}.mode is invalid")
    queue_id = _strict_string(source_authority.get("queue_id"), f"{label}.queue_id")
    if QUEUE_ID_RE.fullmatch(queue_id) is None:
        raise SealError(f"{label}.queue_id is invalid")
    _strict_sha256(
        source_authority.get("queue_identity_sha256"),
        f"{label}.queue_identity_sha256",
    )
    _strict_absolute_path(
        source_authority.get("queue_manifest_path"),
        f"{label}.queue_manifest_path",
    )
    _strict_sha256(
        source_authority.get("queue_manifest_sha256"),
        f"{label}.queue_manifest_sha256",
    )
    authority_version = _strict_integer(
        source_authority.get("schema_version"), f"{label}.schema_version", minimum=1
    )
    if authority_version != QUEUE_ONLY_SOURCE_AUTHORITY_VERSION:
        raise SealError(f"{label}.schema_version is invalid")
    if source_authority.get("selection_policy") != QUEUE_ONLY_SELECTION_POLICY:
        raise SealError(f"{label}.selection_policy is invalid")
    contract = source_authority["queue_contract"]
    _strict_integer(
        contract.get("manifest_schema_version"),
        f"{label}.queue_contract.manifest_schema_version",
        minimum=1,
    )
    _strict_string(
        contract.get("materializer"), f"{label}.queue_contract.materializer"
    )
    _strict_string(
        contract.get("materializer_implementation_version"),
        f"{label}.queue_contract.materializer_implementation_version",
    )
    for name in ("queue_manifest_schema", "queue_validator"):
        _validate_file_identity(
            contract[name], f"{label}.queue_contract.{name}"
        )
    _strict_sha256(
        contract.get("safety_sha256"), f"{label}.queue_contract.safety_sha256"
    )


def _validate_batch_source_authority_values(
    source_authority: dict[str, Any], label: str
) -> None:
    if source_authority.get("mode") != "batch_only":
        raise SealError(f"{label}.mode is invalid")
    batch_id = _strict_string(source_authority.get("batch_id"), f"{label}.batch_id")
    if not batch_id.startswith("asrbatch_") or SOURCE_ID_RE.fullmatch(batch_id) is None:
        raise SealError(f"{label}.batch_id is invalid")
    _strict_sha256(
        source_authority.get("batch_identity_sha256"),
        f"{label}.batch_identity_sha256",
    )
    _strict_absolute_path(
        source_authority.get("batch_manifest_path"),
        f"{label}.batch_manifest_path",
    )
    _strict_sha256(
        source_authority.get("batch_manifest_sha256"),
        f"{label}.batch_manifest_sha256",
    )
    authority_version = _strict_integer(
        source_authority.get("schema_version"), f"{label}.schema_version", minimum=1
    )
    if authority_version != BATCH_ONLY_SOURCE_AUTHORITY_VERSION:
        raise SealError(f"{label}.schema_version is invalid")
    if source_authority.get("selection_policy") != BATCH_ONLY_SELECTION_POLICY:
        raise SealError(f"{label}.selection_policy is invalid")
    contract = source_authority["batch_contract"]
    _strict_integer(
        contract.get("manifest_schema_version"),
        f"{label}.batch_contract.manifest_schema_version",
        minimum=1,
    )
    _strict_string(
        contract.get("materializer"), f"{label}.batch_contract.materializer"
    )
    _strict_string(
        contract.get("materializer_implementation_version"),
        f"{label}.batch_contract.materializer_implementation_version",
    )
    for name in ("batch_manifest_schema", "batch_validator"):
        _validate_file_identity(contract[name], f"{label}.batch_contract.{name}")
    for name in ("catalog_binding_sha256", "safety_sha256"):
        _strict_sha256(contract.get(name), f"{label}.batch_contract.{name}")


def _validate_source_authority_values(
    source_authority: dict[str, Any], label: str
) -> None:
    if source_authority.get("mode") == "queue_only":
        _validate_queue_source_authority_values(source_authority, label)
    elif source_authority.get("mode") == "batch_only":
        _validate_batch_source_authority_values(source_authority, label)
    else:
        raise SealError(f"{label}.mode is invalid")


def _validate_source_values(source: dict[str, Any], label: str) -> None:
    adapter = source["adapter"]
    _strict_integer(adapter.get("byte_count"), f"{label}.adapter.byte_count", minimum=1)
    _strict_string(
        adapter.get("implementation_version"),
        f"{label}.adapter.implementation_version",
    )
    _strict_string(adapter.get("name"), f"{label}.adapter.name")
    _strict_sha256(adapter.get("sha256"), f"{label}.adapter.sha256")
    _strict_integer(source.get("byte_count"), f"{label}.byte_count", minimum=1)
    _strict_sha256(source.get("engine_sha256"), f"{label}.engine_sha256")
    _strict_string(source.get("engine_version"), f"{label}.engine_version")
    _strict_sha256(source.get("identity_sha256"), f"{label}.identity_sha256")
    if source.get("kind") not in {"long_window_batch", "short_preprocess_queue"}:
        raise SealError(f"{label}.kind is invalid")
    _strict_absolute_path(source.get("manifest_path"), f"{label}.manifest_path")
    _strict_sha256(source.get("manifest_sha256"), f"{label}.manifest_sha256")
    source_id = _strict_string(source.get("source_id"), f"{label}.source_id")
    if SOURCE_ID_RE.fullmatch(source_id) is None:
        raise SealError(f"{label}.source_id is invalid")
    prefix = "asrbatch_" if source["kind"] == "long_window_batch" else "asrppqueue_"
    if not source_id.startswith(prefix):
        raise SealError(f"{label}.source_id does not match source kind")


def _validate_catalog_summary_values(value: dict[str, Any], label: str) -> None:
    for name in ("artifact_count", "segment_count", "token_count"):
        _strict_integer(value.get(name), f"{label}.{name}")
    for name in ("job_id", "model_id", "processing_run_id", "recipe_id"):
        _strict_string(value.get(name), f"{label}.{name}")
    for name in ("recording_id", "rendition_id"):
        item = value.get(name)
        if item is not None and not isinstance(item, str):
            raise SealError(f"{label}.{name} must be string|null")
    _strict_sha256(
        value.get("result_envelope_sha256"),
        f"{label}.result_envelope_sha256",
    )
    _strict_sha256(value.get("result_key"), f"{label}.result_key")


def _validate_plan_scalar_values(plan: dict[str, Any]) -> None:
    schema_version = _strict_integer(
        plan.get("schema_version"), "seal plan schema_version", minimum=1
    )
    if schema_version not in SUPPORTED_PLAN_SCHEMA_VERSIONS:
        raise SealError("seal plan schema version is unsupported")
    if plan.get("kind") != PLAN_KIND:
        raise SealError("seal plan kind is unsupported")
    _canonical_utc_timestamp(plan.get("created_at"), "seal plan created_at")
    _strict_sha256(plan.get("identity_sha256"), "seal plan identity_sha256")
    plan_id = _strict_string(plan.get("plan_id"), "seal plan plan_id")
    if PLAN_ID_RE.fullmatch(plan_id) is None:
        raise SealError("seal plan plan_id is invalid")
    _strict_absolute_path(plan.get("receipt_path"), "seal plan receipt_path")
    _strict_absolute_path(plan.get("store_root"), "seal plan store_root")
    if plan.get("state") != "prepared_not_applied":
        raise SealError("seal plan state is invalid")
    _strict_integer(plan.get("result_count"), "seal plan result_count", minimum=1)
    for name in ("implementation", "importer_validator"):
        _validate_file_identity(plan[name], f"seal plan {name}")
    for index, source in enumerate(plan["sources"], 1):
        _validate_source_values(source, f"seal plan source {index}")
    if schema_version in {
        QUEUE_ONLY_PLAN_SCHEMA_VERSION,
        BATCH_ONLY_PLAN_SCHEMA_VERSION,
    }:
        _validate_source_authority_values(
            plan["source_authority"], "seal plan source_authority"
        )
    for index, item in enumerate(plan["excluded_source_entries"], 1):
        label = f"seal plan exclusion {index}"
        for name in ("canonical_sha256", "input_sha256", "physical_sha256"):
            _strict_sha256(item.get(name), f"{label}.{name}")
        _strict_string(item.get("job_id"), f"{label}.job_id")
        _strict_integer(item.get("ordinal"), f"{label}.ordinal", minimum=1)
        if item.get("reason") != "sealed_queue_routing_hint_not_process":
            raise SealError(f"{label}.reason is invalid")
        routing_hint = item.get("routing_hint")
        if routing_hint is not None and not (
            isinstance(routing_hint, str) and len(routing_hint) <= 256
        ):
            raise SealError(f"{label}.routing_hint must be string|null <= 256 chars")
        _strict_string(item.get("source_id"), f"{label}.source_id")
        if item.get("source_kind") != "short_preprocess_queue":
            raise SealError(f"{label}.source_kind is invalid")
        _strict_absolute_path(item.get("work_order_path"), f"{label}.work_order_path")
    for index, result in enumerate(plan["results"], 1):
        label = f"seal plan result {index}"
        for name in ("input_sha256", "result_key", "work_order_sha256"):
            _strict_sha256(result.get(name), f"{label}.{name}")
        _strict_string(result.get("job_id"), f"{label}.job_id")
        _strict_integer(result.get("ordinal"), f"{label}.ordinal", minimum=1)
        recipe_id = _strict_string(result.get("recipe_id"), f"{label}.recipe_id")
        if RECIPE_ID_RE.fullmatch(recipe_id) is None:
            raise SealError(f"{label}.recipe_id is invalid")
        _strict_absolute_path(result.get("result_path"), f"{label}.result_path")
        _strict_string(result.get("source_id"), f"{label}.source_id")
        if result.get("source_kind") not in {
            "long_window_batch",
            "short_preprocess_queue",
        }:
            raise SealError(f"{label}.source_kind is invalid")
        _strict_integer(
            result.get("source_ordinal"), f"{label}.source_ordinal", minimum=1
        )
        directory = result["directory"]
        for name, minimum in (
            ("ctime_ns_before", 0),
            ("device", 0),
            ("inode", 1),
            ("mtime_ns", 0),
            ("nlink", 1),
        ):
            _strict_integer(directory.get(name), f"{label}.directory.{name}", minimum=minimum)
        _strict_absolute_path(directory.get("path"), f"{label}.directory.path")
        for file_index, item in enumerate(result["files"], 1):
            file_label = f"{label}.files[{file_index}]"
            for name, minimum in (
                ("byte_count", 1),
                ("ctime_ns_before", 0),
                ("device", 0),
                ("inode", 1),
                ("mtime_ns", 0),
            ):
                _strict_integer(item.get(name), f"{file_label}.{name}", minimum=minimum)
            _strict_string(item.get("name"), f"{file_label}.name")
            _strict_integer(item.get("nlink"), f"{file_label}.nlink", minimum=1)
            _strict_absolute_path(item.get("path"), f"{file_label}.path")
            _strict_string(item.get("role"), f"{file_label}.role")
            _strict_sha256(item.get("sha256"), f"{file_label}.sha256")
        _validate_catalog_summary_values(
            result["catalog_free_validation"], f"{label}.catalog_free_validation"
        )


def _validate_plan_shape(plan: dict[str, Any]) -> None:
    schema_version = plan.get("schema_version")
    if schema_version == TWO_SOURCE_PLAN_SCHEMA_VERSION:
        _exact_keys(plan, PLAN_KEYS_V1, "v1 seal plan")
        expected_source_count = 2
        expected_source_kinds = {"long_window_batch", "short_preprocess_queue"}
    elif schema_version == QUEUE_ONLY_PLAN_SCHEMA_VERSION:
        _exact_keys(plan, PLAN_KEYS_V2, "v2 queue-only seal plan")
        expected_source_count = 1
        expected_source_kinds = {"short_preprocess_queue"}
    elif schema_version == BATCH_ONLY_PLAN_SCHEMA_VERSION:
        _exact_keys(plan, PLAN_KEYS_V2, "v3 batch-only seal plan")
        expected_source_count = 1
        expected_source_kinds = {"long_window_batch"}
    else:
        raise SealError("seal plan schema version is unsupported")
    for label in ("implementation", "importer_validator"):
        _exact_keys(plan[label], FILE_IDENTITY_KEYS, f"seal plan {label}")
    sources = plan.get("sources")
    if not isinstance(sources, list) or len(sources) != expected_source_count:
        raise SealError(
            f"seal plan schema v{schema_version} requires exactly "
            f"{expected_source_count} source(s)"
        )
    for index, source in enumerate(sources, 1):
        _exact_keys(source, SOURCE_KEYS, f"seal plan source {index}")
        _exact_keys(source["adapter"], ADAPTER_KEYS, f"seal plan source {index} adapter")
    if (
        {source["kind"] for source in sources} != expected_source_kinds
        or len({source["source_id"] for source in sources}) != expected_source_count
    ):
        raise SealError("seal plan source kinds or IDs are not one-to-one")
    if schema_version == QUEUE_ONLY_PLAN_SCHEMA_VERSION:
        source_authority = _exact_keys(
            plan.get("source_authority"),
            SOURCE_AUTHORITY_KEYS,
            "v2 seal plan source authority",
        )
        queue_contract = _exact_keys(
            source_authority.get("queue_contract"),
            QUEUE_CONTRACT_KEYS,
            "v2 seal plan queue contract",
        )
        for label in ("queue_manifest_schema", "queue_validator"):
            _exact_keys(
                queue_contract.get(label),
                FILE_IDENTITY_KEYS,
                f"v2 seal plan {label}",
            )
        if source_authority != _queue_only_source_authority(
            sources[0], queue_contract
        ):
            raise SealError("v2 seal plan source authority differs from its queue")
    elif schema_version == BATCH_ONLY_PLAN_SCHEMA_VERSION:
        source_authority = _exact_keys(
            plan.get("source_authority"),
            BATCH_SOURCE_AUTHORITY_KEYS,
            "v3 seal plan source authority",
        )
        batch_contract = _exact_keys(
            source_authority.get("batch_contract"),
            BATCH_CONTRACT_KEYS,
            "v3 seal plan batch contract",
        )
        for label in ("batch_manifest_schema", "batch_validator"):
            _exact_keys(
                batch_contract.get(label),
                FILE_IDENTITY_KEYS,
                f"v3 seal plan {label}",
            )
        if source_authority != _batch_only_source_authority(
            sources[0], batch_contract
        ):
            raise SealError("v3 seal plan source authority differs from its batch")
    excluded = plan.get("excluded_source_entries")
    if not isinstance(excluded, list):
        raise SealError("seal plan exclusions must be an array")
    for index, item in enumerate(excluded, 1):
        _exact_keys(item, EXCLUDED_KEYS, f"seal plan exclusion {index}")
        source = next(
            (candidate for candidate in sources if candidate["source_id"] == item["source_id"]),
            None,
        )
        if source is None or source["kind"] != item.get("source_kind"):
            raise SealError("seal plan exclusion source binding is invalid")
    results = plan.get("results")
    if not isinstance(results, list) or not results:
        raise SealError("seal plan results must be a non-empty array")
    seen_paths: set[str] = set()
    seen_source_tuples: set[tuple[str, int]] = set()
    source_by_id = {source["source_id"]: source for source in sources}
    for ordinal, result in enumerate(results, 1):
        _exact_keys(result, RESULT_KEYS, f"seal plan result {ordinal}")
        if result.get("ordinal") != ordinal:
            raise SealError("seal plan result ordinals are not contiguous")
        source = source_by_id.get(result.get("source_id"))
        if source is None or source["kind"] != result.get("source_kind"):
            raise SealError("seal plan result source binding is invalid")
        source_tuple = (result["source_id"], result.get("source_ordinal"))
        if source_tuple in seen_source_tuples:
            raise SealError("seal plan duplicates a source ordinal")
        seen_source_tuples.add(source_tuple)
        result_path = result.get("result_path")
        if not isinstance(result_path, str) or result_path in seen_paths:
            raise SealError("seal plan result paths are invalid or duplicated")
        seen_paths.add(result_path)
        directory = _exact_keys(
            result.get("directory"), DIRECTORY_BEFORE_KEYS,
            f"seal plan result {ordinal} directory",
        )
        if directory.get("path") != str(Path(result_path).parent):
            raise SealError("seal plan result directory path is inconsistent")
        if directory.get("mode_before") != RESULT_DIRECTORY_MODE_BEFORE or directory.get("mode_after") != RESULT_DIRECTORY_MODE_AFTER:
            raise SealError("seal plan result directory modes are invalid")
        files = result.get("files")
        if not isinstance(files, list) or [
            item.get("name") for item in files if isinstance(item, dict)
        ] != list(RESULT_FILENAMES):
            raise SealError("seal plan result file order is invalid")
        for item in files:
            _exact_keys(item, FILE_BEFORE_KEYS, f"seal plan result {ordinal} file")
            name = item["name"]
            if item.get("path") != str(Path(result_path).parent / name):
                raise SealError("seal plan result artifact path is inconsistent")
            if item.get("role") != ROLE_BY_NAME[name] or item.get("nlink") != 1:
                raise SealError("seal plan result artifact role or nlink is invalid")
            if item.get("mode_before") != RESULT_FILE_MODE_BEFORE or item.get("mode_after") != RESULT_FILE_MODE_AFTER:
                raise SealError("seal plan result artifact modes are invalid")
        _exact_keys(
            result.get("catalog_free_validation"), CATALOG_SUMMARY_KEYS,
            f"seal plan result {ordinal} catalog-free validation",
        )
    _validate_plan_scalar_values(plan)


def _read_plan(path: Path) -> tuple[dict[str, Any], RetainedFile, ExitStack]:
    stack = ExitStack()
    try:
        retained = _open_retained_file(
            stack,
            path,
            f"seal plan {path}",
            maximum_bytes=PLAN_MAX_BYTES,
            exact_mode=PLAN_FILE_MODE,
        )
        plan = _parse_json(retained.body, f"seal plan {path}")
        if retained.body != canonical_bytes(plan) + b"\n":
            raise SealError("seal plan must use canonical JSON plus one newline")
        _validate_plan_shape(plan)
        _canonical_utc_timestamp(plan.get("created_at"), "seal plan created_at")
        if (
            plan.get("kind") != PLAN_KIND
            or plan.get("schema_version") not in SUPPORTED_PLAN_SCHEMA_VERSIONS
        ):
            raise SealError("seal plan kind or schema version is unsupported")
        if plan.get("authority") != AUTHORITY or plan.get("policy") != POLICY:
            raise SealError("seal plan authority or policy differs from the closed lane")
        store_root = _require_absolute_resolved(
            Path(plan["store_root"]), "planned ASR result store root"
        )
        if _mode(store_root.lstat()) != STORE_ROOT_MODE:
            raise SealError("planned ASR result store root must remain mode 0700")
        control_root = store_root / "sealing-control"
        control_directories = (
            control_root,
            control_root / "plans",
            control_root / "receipts",
        )
        for control_directory in control_directories:
            _open_retained_directory(
                stack,
                _require_absolute_resolved(
                    control_directory, f"seal control directory {control_directory}"
                ),
                f"seal control directory {control_directory}",
                exact_mode=CONTROL_DIRECTORY_MODE,
            )
        if plan["schema_version"] in {
            QUEUE_ONLY_PLAN_SCHEMA_VERSION,
            BATCH_ONLY_PLAN_SCHEMA_VERSION,
        }:
            _retain_apply_lock(stack, control_root, acquire=False)
        expected_plan_path = control_root / "plans" / path.name
        if path != expected_plan_path:
            raise SealError("seal plan is outside the planned private control directory")
        semantic = {
            key: value
            for key, value in plan.items()
            if key not in {"identity_sha256", "plan_id", "receipt_path"}
        }
        identity = _document_identity(semantic, set())
        if plan.get("identity_sha256") != identity:
            raise SealError("seal plan semantic identity is invalid")
        if plan.get("plan_id") != f"asrsealplan_{identity[:32]}":
            raise SealError("seal plan ID is invalid")
        expected_receipt = (
            store_root
            / "sealing-control"
            / "receipts"
            / f"asrsealreceipt_{identity[:32]}.json"
        )
        if plan.get("receipt_path") != str(expected_receipt):
            raise SealError("seal plan receipt path is not the closed derived path")
        if path.name != f"{plan['plan_id']}.json":
            raise SealError("seal plan filename differs from its plan ID")
        if plan.get("state") != "prepared_not_applied":
            raise SealError("seal plan is not in prepared_not_applied state")
        if plan.get("result_count") != len(plan.get("results", [])):
            raise SealError("seal plan result_count is inconsistent")
        return plan, retained, stack
    except Exception:
        stack.close()
        raise


def _verify_implementation(plan: dict[str, Any]) -> None:
    current = _file_identity(Path(__file__).resolve())
    planned = plan.get("implementation")
    if current != planned:
        legacy_v1 = (
            plan.get("schema_version") == TWO_SOURCE_PLAN_SCHEMA_VERSION
            and isinstance(planned, dict)
            and planned.get("path") == str(Path(__file__).resolve())
            and (planned.get("byte_count"), planned.get("sha256"))
            in LEGACY_V1_IMPLEMENTATION_IDENTITIES
        )
        if not legacy_v1:
            raise SealError("sealing implementation identity differs from the plan")
    if _file_identity(IMPORTER_SOURCE.resolve()) != plan.get("importer_validator"):
        raise SealError("catalog-free importer validator identity differs from the plan")
    if plan.get("schema_version") == QUEUE_ONLY_PLAN_SCHEMA_VERSION:
        queue_contract = plan["source_authority"]["queue_contract"]
        if _file_identity(PREPROCESS_QUEUE_SOURCE.resolve()) != queue_contract.get(
            "queue_validator"
        ):
            raise SealError("preprocess ASR queue validator identity differs from the plan")
        if _file_identity(
            PREPROCESS_QUEUE_SCHEMA_SOURCE.resolve()
        ) != queue_contract.get("queue_manifest_schema"):
            raise SealError("preprocess ASR queue schema identity differs from the plan")
    elif plan.get("schema_version") == BATCH_ONLY_PLAN_SCHEMA_VERSION:
        batch_contract = plan["source_authority"]["batch_contract"]
        if _file_identity(ASR_BATCH_SOURCE.resolve()) != batch_contract.get(
            "batch_validator"
        ):
            raise SealError("raw ASR batch validator identity differs from the plan")
        if _file_identity(
            ASR_BATCH_SCHEMA_SOURCE.resolve()
        ) != batch_contract.get("batch_manifest_schema"):
            raise SealError("raw ASR batch schema identity differs from the plan")


def _verify_sources(
    plan: dict[str, Any],
    stack: ExitStack,
    queue_validator: QueueValidator = authoritative_queue_validate,
    batch_validator: BatchValidator = authoritative_batch_validate,
) -> list[RetainedFile]:
    observed: list[dict[str, Any]] = []
    observed_excluded: list[dict[str, Any]] = []
    observed_selected: set[tuple[str, int, str, str, str]] = set()
    retained_files: list[RetainedFile] = []
    for expected in plan["sources"]:
        (
            source,
            selected,
            excluded,
            source_files,
            authority_contract,
        ) = _source_eligibility(
            stack,
            Path(expected["manifest_path"]),
            authoritative_queue=(
                plan["schema_version"] == QUEUE_ONLY_PLAN_SCHEMA_VERSION
            ),
            authoritative_batch=(
                plan["schema_version"] == BATCH_ONLY_PLAN_SCHEMA_VERSION
            ),
            queue_validator=queue_validator,
            batch_validator=batch_validator,
        )
        retained_files.extend(source_files)
        observed.append(source)
        if plan["schema_version"] == QUEUE_ONLY_PLAN_SCHEMA_VERSION:
            if plan.get("source_authority") != _queue_only_source_authority(
                source, authority_contract
            ):
                raise SealError(
                    "authoritative queue contract differs from plan source authority"
                )
        elif plan["schema_version"] == BATCH_ONLY_PLAN_SCHEMA_VERSION:
            if plan.get("source_authority") != _batch_only_source_authority(
                source, authority_contract
            ):
                raise SealError(
                    "authoritative batch contract differs from plan source authority"
                )
        observed_selected.update(
            (
                source["source_id"],
                item["ordinal"],
                item["job_id"],
                item["canonical_sha256"],
                item["input_sha256"],
            )
            for item in selected
        )
        observed_excluded.extend(
            {"source_id": source["source_id"], "source_kind": source["kind"], **item}
            for item in excluded
        )
    observed.sort(key=lambda record: record["kind"])
    observed_excluded.sort(key=lambda record: (record["source_id"], record["ordinal"]))
    if observed != plan["sources"]:
        raise SealError("source manifest identities differ from the plan")
    if observed_excluded != plan["excluded_source_entries"]:
        raise SealError("source exclusions differ from the plan")
    planned_selected = {
        (
            item["source_id"],
            item["source_ordinal"],
            item["job_id"],
            item["work_order_sha256"],
            item["input_sha256"],
        )
        for item in plan["results"]
    }
    if planned_selected != observed_selected:
        raise SealError("planned results do not exactly cover source-selected tuples")
    return retained_files


def _verify_retained_sources(retained_files: list[RetainedFile]) -> None:
    for source_file in retained_files:
        source_file.verify(
            modes={SOURCE_FILE_MODE}, expected_sha256=sha256_bytes(source_file.body)
        )


def _open_planned_results(
    plan: dict[str, Any],
    stack: ExitStack,
    validator: CatalogValidator,
    *,
    allowed_file_modes: set[int],
    allowed_directory_modes: set[int],
) -> list[RetainedResult]:
    store_root = _require_absolute_resolved(
        Path(plan["store_root"]), "planned ASR result store root"
    )
    sources = {source["source_id"]: source for source in plan["sources"]}
    retained_results: list[RetainedResult] = []
    for expected_ordinal, expected in enumerate(plan["results"], 1):
        if expected.get("ordinal") != expected_ordinal:
            raise SealError("planned results are not strictly ordered")
        source = sources.get(expected.get("source_id"))
        if source is None:
            raise SealError("planned result names an unknown source")
        eligible = {
            "canonical_sha256": expected.get("work_order_sha256"),
            "job_id": expected.get("job_id"),
            "input_sha256": expected.get("input_sha256"),
            "ordinal": expected.get("source_ordinal"),
        }
        # Open with relaxed mode then compare the full recorded identity below.
        result_path = Path(expected["result_path"])
        _require_beneath(
            result_path, store_root, f"planned result {result_path}"
        )
        ancestors = _open_result_ancestors(stack, result_path.parent, store_root)
        directory = _open_retained_directory(
            stack, result_path.parent, f"planned result directory {result_path.parent}"
        )
        directory_stat = directory.verify(
            modes=allowed_directory_modes, entries=set(RESULT_FILENAMES)
        )
        files: dict[str, RetainedFile] = {}
        raw: dict[str, Any] | None = None
        expected_files = expected.get("files")
        if not isinstance(expected_files, list) or [item.get("name") for item in expected_files] != list(RESULT_FILENAMES):
            raise SealError("planned result file list is not the exact ordered allowlist")
        for expected_file in expected_files:
            name = expected_file["name"]
            retained = _open_retained_file(
                stack,
                result_path.parent / name,
                f"planned result artifact {result_path.parent / name}",
                maximum_bytes=MAX_FILE_BYTES[name],
                dir_fd=directory.fd,
                name=name,
            )
            descriptor = retained.verify(
                modes=allowed_file_modes,
                expected_sha256=expected_file["sha256"],
                expected_mtime_ns=expected_file["mtime_ns"],
                expected_device=expected_file["device"],
                expected_inode=expected_file["inode"],
            )
            if descriptor.st_size != expected_file["byte_count"]:
                raise SealError(f"planned artifact size differs: {retained.path}")
            if _mode(descriptor) == RESULT_FILE_MODE_BEFORE and descriptor.st_ctime_ns != expected_file["ctime_ns_before"]:
                raise SealError(f"planned artifact ctime differs before sealing: {retained.path}")
            files[name] = retained
            if name == "result.json":
                raw = _parse_json(retained.body, f"planned result envelope {retained.path}")
        assert raw is not None
        if raw.get("job_id") != eligible["job_id"] or raw.get("work_order_sha256") != eligible["canonical_sha256"]:
            raise SealError(f"planned result source identity differs: {result_path}")
        if raw.get("input", {}).get("sha256") != eligible["input_sha256"]:
            raise SealError(f"planned result input identity differs: {result_path}")
        if raw.get("result_path") != str(result_path) or raw.get("result_key") != expected.get("result_key"):
            raise SealError(f"planned result envelope identity differs: {result_path}")
        expected_directory = expected["directory"]
        if directory_stat.st_dev != expected_directory["device"] or directory_stat.st_ino != expected_directory["inode"]:
            raise SealError(f"planned result directory object differs: {result_path.parent}")
        if directory_stat.st_mtime_ns != expected_directory["mtime_ns"]:
            raise SealError(f"planned result directory mtime differs: {result_path.parent}")
        if _mode(directory_stat) == RESULT_DIRECTORY_MODE_BEFORE and directory_stat.st_ctime_ns != expected_directory["ctime_ns_before"]:
            raise SealError(f"planned result directory ctime differs before sealing: {result_path.parent}")
        try:
            validation = validator(result_path)
        except Exception as error:
            raise SealError(f"catalog-free validation failed for {result_path}: {error}") from error
        if _catalog_summary(validation) != expected["catalog_free_validation"]:
            raise SealError(f"catalog-free validation identity differs: {result_path}")
        for retained in files.values():
            retained.verify(
                modes=allowed_file_modes,
                expected_sha256=next(item["sha256"] for item in expected_files if item["name"] == retained.path.name),
            )
        directory.verify(modes=allowed_directory_modes, entries=set(RESULT_FILENAMES))
        retained_results.append(
            RetainedResult(
                ancestors=ancestors,
                directory=directory,
                files=files,
                raw=raw,
                record=expected,
            )
        )
    return retained_results


def validate_plan(
    plan_path: Path,
    *,
    validator: CatalogValidator = catalog_free_validate,
    queue_validator: QueueValidator = authoritative_queue_validate,
    batch_validator: BatchValidator = authoritative_batch_validate,
) -> dict[str, Any]:
    plan, retained_plan, stack = _read_plan(plan_path)
    with stack:
        _verify_implementation(plan)
        store_root = _require_absolute_resolved(Path(plan["store_root"]), "planned store root")
        if _mode(store_root.lstat()) != STORE_ROOT_MODE:
            raise SealError("planned store root mode differs")
        retained_sources = _verify_sources(
            plan, stack, queue_validator, batch_validator
        )
        results = _open_planned_results(
            plan,
            stack,
            validator,
            allowed_file_modes={RESULT_FILE_MODE_BEFORE},
            allowed_directory_modes={RESULT_DIRECTORY_MODE_BEFORE},
        )
        retained_plan.verify(modes={PLAN_FILE_MODE})
        _verify_result_ancestors(results)
        _verify_retained_sources(retained_sources)
        return {
            "plan_id": plan["plan_id"],
            "result_count": len(results),
            "state": "valid_prepared_plan",
        }


def _after_file_record(
    retained: RetainedFile,
    expected: dict[str, Any],
    *,
    expected_ctime_ns: int | None = None,
) -> dict[str, Any]:
    value = retained.verify(
        modes={RESULT_FILE_MODE_AFTER},
        expected_sha256=expected["sha256"],
        expected_mtime_ns=expected["mtime_ns"],
        expected_ctime_ns=expected_ctime_ns,
        expected_device=expected["device"],
        expected_inode=expected["inode"],
    )
    return {
        "byte_count": value.st_size,
        "content_unchanged": True,
        "ctime_ns_after": value.st_ctime_ns,
        "device": value.st_dev,
        "inode": value.st_ino,
        "mode_after": _mode(value),
        "mtime_ns": value.st_mtime_ns,
        "mtime_unchanged": value.st_mtime_ns == expected["mtime_ns"],
        "name": retained.path.name,
        "nlink": value.st_nlink,
        "path": str(retained.path),
        "sha256": expected["sha256"],
    }


def _build_receipt(
    plan_path: Path,
    plan_body: bytes,
    plan: dict[str, Any],
    retained_results: list[RetainedResult],
    *,
    applied_at: str,
    sealed_ctimes: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    applied_at = _canonical_utc_timestamp(applied_at, "seal receipt applied_at")
    result_records: list[dict[str, Any]] = []
    for retained_result, expected in zip(retained_results, plan["results"], strict=True):
        ctimes = sealed_ctimes[len(result_records)] if sealed_ctimes is not None else None
        files = [
            _after_file_record(
                retained_result.files[item["name"]],
                item,
                expected_ctime_ns=(ctimes["files"][item["name"]] if ctimes else None),
            )
            for item in expected["files"]
        ]
        directory = retained_result.directory.verify(
            modes={RESULT_DIRECTORY_MODE_AFTER},
            entries=set(RESULT_FILENAMES),
            expected_mtime_ns=expected["directory"]["mtime_ns"],
            expected_ctime_ns=(ctimes["directory"] if ctimes else None),
            expected_device=expected["directory"]["device"],
            expected_inode=expected["directory"]["inode"],
        )
        result_records.append(
            {
                "directory": {
                    "content_entries_unchanged": True,
                    "ctime_ns_after": directory.st_ctime_ns,
                    "device": directory.st_dev,
                    "inode": directory.st_ino,
                    "mode_after": _mode(directory),
                    "mtime_ns": directory.st_mtime_ns,
                    "mtime_unchanged": directory.st_mtime_ns
                    == expected["directory"]["mtime_ns"],
                    "nlink": directory.st_nlink,
                    "path": str(retained_result.directory.path),
                },
                "files": files,
                "job_id": expected["job_id"],
                "input_sha256": expected["input_sha256"],
                "ordinal": expected["ordinal"],
                "result_key": expected["result_key"],
                "result_path": expected["result_path"],
                "source_id": expected["source_id"],
                "source_ordinal": expected["source_ordinal"],
                "work_order_sha256": expected["work_order_sha256"],
            }
        )
    receipt: dict[str, Any] = {
        "applied_at": applied_at,
        "authority": AUTHORITY,
        "kind": RECEIPT_KIND,
        "plan": {
            "byte_count": len(plan_body),
            "identity_sha256": plan["identity_sha256"],
            "path": str(plan_path),
            "plan_id": plan["plan_id"],
            "sha256": sha256_bytes(plan_body),
        },
        "policy": POLICY,
        "result_count": len(result_records),
        "results": result_records,
        "schema_version": {
            TWO_SOURCE_PLAN_SCHEMA_VERSION: TWO_SOURCE_RECEIPT_SCHEMA_VERSION,
            QUEUE_ONLY_PLAN_SCHEMA_VERSION: QUEUE_ONLY_RECEIPT_SCHEMA_VERSION,
            BATCH_ONLY_PLAN_SCHEMA_VERSION: BATCH_ONLY_RECEIPT_SCHEMA_VERSION,
        }[plan["schema_version"]],
        "state": "applied_content_and_mtime_preserved",
    }
    if plan["schema_version"] in {
        QUEUE_ONLY_PLAN_SCHEMA_VERSION,
        BATCH_ONLY_PLAN_SCHEMA_VERSION,
    }:
        receipt["source_authority"] = plan["source_authority"]
    identity = _document_identity(receipt, set())
    receipt["identity_sha256"] = identity
    receipt["receipt_id"] = f"asrsealreceipt_{plan['identity_sha256'][:32]}"
    return receipt


def _mode_state(
    plan: dict[str, Any], retained_results: list[RetainedResult]
) -> str:
    states: list[str] = []
    for retained, expected in zip(retained_results, plan["results"], strict=True):
        directory_mode = _mode(os.fstat(retained.directory.fd))
        if directory_mode == expected["directory"]["mode_before"]:
            states.append("before")
        elif directory_mode == expected["directory"]["mode_after"]:
            states.append("after")
        else:
            states.append("invalid")
        for file_expected in expected["files"]:
            file_mode = _mode(os.fstat(retained.files[file_expected["name"]].fd))
            if file_mode == file_expected["mode_before"]:
                states.append("before")
            elif file_mode == file_expected["mode_after"]:
                states.append("after")
            else:
                states.append("invalid")
    unique = set(states)
    if unique == {"before"}:
        return "exact_all_before"
    if unique == {"after"}:
        return "exact_all_after"
    return "mixed_or_invalid"


def _rollback_modes(
    plan: dict[str, Any], retained_results: list[RetainedResult]
) -> list[str]:
    failures: list[str] = []
    for retained, expected in reversed(
        list(zip(retained_results, plan["results"], strict=True))
    ):
        try:
            before = expected["directory"]["mode_before"]
            if _mode(os.fstat(retained.directory.fd)) != before:
                os.fchmod(retained.directory.fd, before)
                os.fsync(retained.directory.fd)
        except OSError as error:
            failures.append(f"{retained.directory.path}: {error}")
        for file_expected in reversed(expected["files"]):
            item = retained.files[file_expected["name"]]
            try:
                before = file_expected["mode_before"]
                if _mode(os.fstat(item.fd)) != before:
                    os.fchmod(item.fd, before)
                    os.fsync(item.fd)
            except OSError as error:
                failures.append(f"{item.path}: {error}")
    return failures


def _control_entry_exists(directory: RetainedDirectory, name: str) -> bool:
    try:
        os.stat(name, dir_fd=directory.fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    except OSError as error:
        raise SealError(f"cannot inspect control entry {name}: {error}") from error
    return True


def apply_plan(
    plan_path: Path,
    *,
    validator: CatalogValidator = catalog_free_validate,
    queue_validator: QueueValidator = authoritative_queue_validate,
    batch_validator: BatchValidator = authoritative_batch_validate,
    applied_at: str | None = None,
) -> Path:
    plan, retained_plan, stack = _read_plan(plan_path)
    receipt_path = Path(plan["receipt_path"])
    with stack:
        control_root = Path(plan["store_root"]) / "sealing-control"
        if plan["schema_version"] == TWO_SOURCE_PLAN_SCHEMA_VERSION:
            # Historical v1 plans predate the apply lock.  Creating only this
            # private control file before inspecting any results preserves their
            # apply/replay compatibility while serializing them with v2.
            _ensure_apply_lock(control_root)
        _retain_apply_lock(stack, control_root, acquire=True)
        receipts = _open_retained_directory(
            stack,
            _require_absolute_resolved(
                receipt_path.parent, "seal receipt control directory"
            ),
            "seal receipt control directory",
            exact_mode=CONTROL_DIRECTORY_MODE,
        )
        if _control_entry_exists(receipts, receipt_path.name):
            try:
                validate_receipt(
                    receipt_path,
                    validator=validator,
                    queue_validator=queue_validator,
                    batch_validator=batch_validator,
                )
            except Exception as error:
                raise SealError(
                    "receipt target already exists but is not the exact valid "
                    f"receipt; refusing to change results: {receipt_path}: {error}"
                ) from error
            return receipt_path
        _verify_implementation(plan)
        store_root = _require_absolute_resolved(Path(plan["store_root"]), "planned store root")
        if _mode(store_root.lstat()) != STORE_ROOT_MODE:
            raise SealError("planned store root mode differs")
        retained_sources = _verify_sources(
            plan, stack, queue_validator, batch_validator
        )
        retained_results = _open_planned_results(
            plan,
            stack,
            validator,
            allowed_file_modes={RESULT_FILE_MODE_BEFORE, RESULT_FILE_MODE_AFTER},
            allowed_directory_modes={
                RESULT_DIRECTORY_MODE_BEFORE,
                RESULT_DIRECTORY_MODE_AFTER,
            },
        )
        _verify_result_ancestors(retained_results)
        _verify_retained_sources(retained_sources)
        state = _mode_state(plan, retained_results)
        if state == "mixed_or_invalid":
            failures = _rollback_modes(plan, retained_results)
            suffix = (
                f"; rollback failures: {failures}"
                if failures
                else "; recorded modes restored"
            )
            raise SealError("partial or invalid seal mode state detected" + suffix)
        transitioned = state == "exact_all_before"
        receipt_committed = False
        try:
            sealed_ctimes: list[dict[str, Any]] = []
            for result in retained_results:
                file_ctimes: dict[str, int] = {}
                for name in RESULT_FILENAMES:
                    retained = result.files[name]
                    if transitioned:
                        os.fchmod(retained.fd, RESULT_FILE_MODE_AFTER)
                    os.fsync(retained.fd)
                    file_ctimes[name] = os.fstat(retained.fd).st_ctime_ns
                if transitioned:
                    os.fchmod(result.directory.fd, RESULT_DIRECTORY_MODE_AFTER)
                os.fsync(result.directory.fd)
                sealed_ctimes.append(
                    {
                        "directory": os.fstat(result.directory.fd).st_ctime_ns,
                        "files": file_ctimes,
                    }
                )
            # Complete every fallible source/result verification before the receipt
            # becomes terminal evidence.  Captured ctimes bind the exact chmods.
            for result, expected, ctimes in zip(
                retained_results, plan["results"], sealed_ctimes, strict=True
            ):
                for expected_file in expected["files"]:
                    result.files[expected_file["name"]].verify(
                        modes={RESULT_FILE_MODE_AFTER},
                        expected_sha256=expected_file["sha256"],
                        expected_mtime_ns=expected_file["mtime_ns"],
                        expected_ctime_ns=ctimes["files"][expected_file["name"]],
                        expected_device=expected_file["device"],
                        expected_inode=expected_file["inode"],
                    )
                result.directory.verify(
                    modes={RESULT_DIRECTORY_MODE_AFTER},
                    entries=set(RESULT_FILENAMES),
                    expected_mtime_ns=expected["directory"]["mtime_ns"],
                    expected_ctime_ns=ctimes["directory"],
                    expected_device=expected["directory"]["device"],
                    expected_inode=expected["directory"]["inode"],
                )
                try:
                    validation = validator(Path(expected["result_path"]))
                except Exception as error:
                    raise SealError(
                        "post-seal catalog-free validation failed for "
                        f"{expected['result_path']}: {error}"
                    ) from error
                if _catalog_summary(validation) != expected["catalog_free_validation"]:
                    raise SealError(
                        f"post-seal catalog identity differs: {expected['result_path']}"
                    )
            retained_plan.verify(modes={PLAN_FILE_MODE})
            _verify_result_ancestors(retained_results)
            _verify_retained_sources(retained_sources)
            receipts.verify(modes={CONTROL_DIRECTORY_MODE})
            receipt = _build_receipt(
                plan_path,
                retained_plan.body,
                plan,
                retained_results,
                applied_at=applied_at or utc_now(),
                sealed_ctimes=sealed_ctimes,
            )
            _validate_receipt_shape(receipt)
            _atomic_private_json(
                receipt_path,
                receipt,
                maximum=RECEIPT_MAX_BYTES,
                retained_parent=receipts,
            )
            receipt_committed = True
            return receipt_path
        except Exception as error:
            if receipt_committed:
                raise
            if _control_entry_exists(receipts, receipt_path.name):
                try:
                    validate_receipt(
                        receipt_path,
                        validator=validator,
                        queue_validator=queue_validator,
                        batch_validator=batch_validator,
                    )
                except Exception:
                    pass
                else:
                    return receipt_path
            failures = _rollback_modes(plan, retained_results)
            if failures:
                raise SealError(
                    f"seal failed ({error}); rollback failures: {failures}"
                ) from error
            raise SealError(
                f"seal failed and recorded modes were restored: {error}"
            ) from error


RECEIPT_KEYS_V1 = {
    "applied_at",
    "authority",
    "identity_sha256",
    "kind",
    "plan",
    "policy",
    "receipt_id",
    "result_count",
    "results",
    "schema_version",
    "state",
}
RECEIPT_KEYS_V2 = RECEIPT_KEYS_V1 | {"source_authority"}
PLAN_REFERENCE_KEYS = {"byte_count", "identity_sha256", "path", "plan_id", "sha256"}
RESULT_AFTER_KEYS = {
    "directory", "files", "input_sha256", "job_id", "ordinal", "result_key",
    "result_path", "source_id", "source_ordinal", "work_order_sha256",
}
DIRECTORY_AFTER_KEYS = {
    "content_entries_unchanged", "ctime_ns_after", "device", "inode", "mode_after",
    "mtime_ns", "mtime_unchanged", "nlink", "path",
}
FILE_AFTER_KEYS = {
    "byte_count", "content_unchanged", "ctime_ns_after", "device", "inode",
    "mode_after", "mtime_ns", "mtime_unchanged", "name", "nlink", "path", "sha256",
}


def _validate_receipt_shape(receipt: dict[str, Any]) -> None:
    schema_version = receipt.get("schema_version")
    if schema_version == TWO_SOURCE_RECEIPT_SCHEMA_VERSION:
        _exact_keys(receipt, RECEIPT_KEYS_V1, "v1 seal receipt")
    elif schema_version == QUEUE_ONLY_RECEIPT_SCHEMA_VERSION:
        _exact_keys(receipt, RECEIPT_KEYS_V2, "v2 queue-only seal receipt")
        source_authority = _exact_keys(
            receipt.get("source_authority"),
            SOURCE_AUTHORITY_KEYS,
            "v2 seal receipt source authority",
        )
        queue_contract = _exact_keys(
            source_authority.get("queue_contract"),
            QUEUE_CONTRACT_KEYS,
            "v2 seal receipt queue contract",
        )
        for label in ("queue_manifest_schema", "queue_validator"):
            _exact_keys(
                queue_contract.get(label),
                FILE_IDENTITY_KEYS,
                f"v2 seal receipt {label}",
            )
    elif schema_version == BATCH_ONLY_RECEIPT_SCHEMA_VERSION:
        _exact_keys(receipt, RECEIPT_KEYS_V2, "v3 batch-only seal receipt")
        source_authority = _exact_keys(
            receipt.get("source_authority"),
            BATCH_SOURCE_AUTHORITY_KEYS,
            "v3 seal receipt source authority",
        )
        batch_contract = _exact_keys(
            source_authority.get("batch_contract"),
            BATCH_CONTRACT_KEYS,
            "v3 seal receipt batch contract",
        )
        for label in ("batch_manifest_schema", "batch_validator"):
            _exact_keys(
                batch_contract.get(label),
                FILE_IDENTITY_KEYS,
                f"v3 seal receipt {label}",
            )
    else:
        raise SealError("seal receipt schema version is unsupported")
    _exact_keys(receipt.get("plan"), PLAN_REFERENCE_KEYS, "seal receipt plan reference")
    results = receipt.get("results")
    if not isinstance(results, list) or not results:
        raise SealError("seal receipt results must be a non-empty array")
    for ordinal, result in enumerate(results, 1):
        _exact_keys(result, RESULT_AFTER_KEYS, f"seal receipt result {ordinal}")
        if result.get("ordinal") != ordinal:
            raise SealError("seal receipt result ordinals are not contiguous")
        directory = _exact_keys(
            result.get("directory"), DIRECTORY_AFTER_KEYS,
            f"seal receipt result {ordinal} directory",
        )
        if directory.get("mode_after") != RESULT_DIRECTORY_MODE_AFTER:
            raise SealError("seal receipt directory target mode is invalid")
        if directory.get("content_entries_unchanged") is not True or directory.get("mtime_unchanged") is not True:
            raise SealError("seal receipt directory invariants are not affirmative")
        files = result.get("files")
        if not isinstance(files, list) or [
            item.get("name") for item in files if isinstance(item, dict)
        ] != list(RESULT_FILENAMES):
            raise SealError("seal receipt result file order is invalid")
        for item in files:
            _exact_keys(item, FILE_AFTER_KEYS, f"seal receipt result {ordinal} file")
            if item.get("mode_after") != RESULT_FILE_MODE_AFTER or item.get("nlink") != 1:
                raise SealError("seal receipt artifact target mode or nlink is invalid")
            if item.get("content_unchanged") is not True or item.get("mtime_unchanged") is not True:
                raise SealError("seal receipt artifact invariants are not affirmative")
    _validate_receipt_scalar_values(receipt)


def _validate_receipt_scalar_values(receipt: dict[str, Any]) -> None:
    schema_version = _strict_integer(
        receipt.get("schema_version"), "seal receipt schema_version", minimum=1
    )
    if schema_version not in SUPPORTED_RECEIPT_SCHEMA_VERSIONS:
        raise SealError("seal receipt schema version is unsupported")
    if receipt.get("kind") != RECEIPT_KIND:
        raise SealError("seal receipt kind is unsupported")
    _canonical_utc_timestamp(receipt.get("applied_at"), "seal receipt applied_at")
    _strict_sha256(receipt.get("identity_sha256"), "seal receipt identity_sha256")
    receipt_id = _strict_string(receipt.get("receipt_id"), "seal receipt receipt_id")
    if RECEIPT_ID_RE.fullmatch(receipt_id) is None:
        raise SealError("seal receipt receipt_id is invalid")
    _strict_integer(
        receipt.get("result_count"), "seal receipt result_count", minimum=1
    )
    if receipt.get("state") != "applied_content_and_mtime_preserved":
        raise SealError("seal receipt state is invalid")
    if schema_version in {
        QUEUE_ONLY_RECEIPT_SCHEMA_VERSION,
        BATCH_ONLY_RECEIPT_SCHEMA_VERSION,
    }:
        _validate_source_authority_values(
            receipt["source_authority"], "seal receipt source_authority"
        )
    plan_ref = receipt["plan"]
    _strict_integer(
        plan_ref.get("byte_count"), "seal receipt plan.byte_count", minimum=1
    )
    _strict_sha256(
        plan_ref.get("identity_sha256"), "seal receipt plan.identity_sha256"
    )
    _strict_absolute_path(plan_ref.get("path"), "seal receipt plan.path")
    plan_id = _strict_string(plan_ref.get("plan_id"), "seal receipt plan.plan_id")
    if PLAN_ID_RE.fullmatch(plan_id) is None:
        raise SealError("seal receipt plan.plan_id is invalid")
    _strict_sha256(plan_ref.get("sha256"), "seal receipt plan.sha256")
    for index, result in enumerate(receipt["results"], 1):
        label = f"seal receipt result {index}"
        for name in ("input_sha256", "result_key", "work_order_sha256"):
            _strict_sha256(result.get(name), f"{label}.{name}")
        _strict_string(result.get("job_id"), f"{label}.job_id")
        _strict_integer(result.get("ordinal"), f"{label}.ordinal", minimum=1)
        _strict_absolute_path(result.get("result_path"), f"{label}.result_path")
        _strict_string(result.get("source_id"), f"{label}.source_id")
        _strict_integer(
            result.get("source_ordinal"), f"{label}.source_ordinal", minimum=1
        )
        directory = result["directory"]
        for name, minimum in (
            ("ctime_ns_after", 0),
            ("device", 0),
            ("inode", 1),
            ("mtime_ns", 0),
            ("nlink", 1),
        ):
            _strict_integer(
                directory.get(name), f"{label}.directory.{name}", minimum=minimum
            )
        _strict_boolean(
            directory.get("content_entries_unchanged"),
            f"{label}.directory.content_entries_unchanged",
        )
        _strict_boolean(
            directory.get("mtime_unchanged"),
            f"{label}.directory.mtime_unchanged",
        )
        _strict_absolute_path(directory.get("path"), f"{label}.directory.path")
        for file_index, item in enumerate(result["files"], 1):
            file_label = f"{label}.files[{file_index}]"
            for name, minimum in (
                ("byte_count", 1),
                ("ctime_ns_after", 0),
                ("device", 0),
                ("inode", 1),
                ("mtime_ns", 0),
                ("nlink", 1),
            ):
                _strict_integer(item.get(name), f"{file_label}.{name}", minimum=minimum)
            _strict_boolean(
                item.get("content_unchanged"), f"{file_label}.content_unchanged"
            )
            _strict_boolean(
                item.get("mtime_unchanged"), f"{file_label}.mtime_unchanged"
            )
            _strict_string(item.get("name"), f"{file_label}.name")
            _strict_absolute_path(item.get("path"), f"{file_label}.path")
            _strict_sha256(item.get("sha256"), f"{file_label}.sha256")


def validate_receipt(
    receipt_path: Path,
    *,
    validator: CatalogValidator = catalog_free_validate,
    queue_validator: QueueValidator = authoritative_queue_validate,
    batch_validator: BatchValidator = authoritative_batch_validate,
) -> dict[str, Any]:
    with ExitStack() as stack:
        retained_receipt = _open_retained_file(
            stack,
            receipt_path,
            f"seal receipt {receipt_path}",
            maximum_bytes=RECEIPT_MAX_BYTES,
            exact_mode=PLAN_FILE_MODE,
        )
        receipt = _parse_json(retained_receipt.body, f"seal receipt {receipt_path}")
        if retained_receipt.body != canonical_bytes(receipt) + b"\n":
            raise SealError("seal receipt must use canonical JSON plus one newline")
        _validate_receipt_shape(receipt)
        if (
            receipt.get("kind") != RECEIPT_KIND
            or receipt.get("schema_version") not in SUPPORTED_RECEIPT_SCHEMA_VERSIONS
        ):
            raise SealError("seal receipt kind or schema version is unsupported")
        if receipt.get("authority") != AUTHORITY or receipt.get("policy") != POLICY:
            raise SealError("seal receipt authority or policy differs")
        semantic = {key: value for key, value in receipt.items() if key not in {"identity_sha256", "receipt_id"}}
        if receipt.get("identity_sha256") != _document_identity(semantic, set()):
            raise SealError("seal receipt semantic identity is invalid")
        plan_ref = receipt.get("plan")
        if not isinstance(plan_ref, dict):
            raise SealError("seal receipt plan reference is invalid")
        plan_path = Path(plan_ref["path"])
        plan, retained_plan, plan_stack = _read_plan(plan_path)
        stack.enter_context(plan_stack)
        if receipt["schema_version"] != plan["schema_version"]:
            raise SealError("seal receipt schema version differs from its plan")
        if plan["schema_version"] in {
            QUEUE_ONLY_PLAN_SCHEMA_VERSION,
            BATCH_ONLY_PLAN_SCHEMA_VERSION,
        } and receipt.get("source_authority") != plan.get("source_authority"):
            raise SealError("receipt source authority differs from its plan")
        if receipt.get("receipt_id") != f"asrsealreceipt_{plan['identity_sha256'][:32]}":
            raise SealError("seal receipt ID differs from the plan")
        if receipt_path != Path(plan["receipt_path"]):
            raise SealError("seal receipt path differs from the plan")
        if sha256_bytes(retained_plan.body) != plan_ref.get("sha256") or len(retained_plan.body) != plan_ref.get("byte_count"):
            raise SealError("seal receipt plan bytes differ")
        if plan_ref.get("plan_id") != plan["plan_id"] or plan_ref.get("identity_sha256") != plan["identity_sha256"]:
            raise SealError("seal receipt plan identity differs")
        if receipt.get("result_count") != plan["result_count"] or receipt.get("result_count") != len(receipt.get("results", [])):
            raise SealError("seal receipt result_count is inconsistent")
        _verify_implementation(plan)
        retained_sources = _verify_sources(
            plan, stack, queue_validator, batch_validator
        )
        results = _open_planned_results(
            plan,
            stack,
            validator,
            allowed_file_modes={RESULT_FILE_MODE_AFTER},
            allowed_directory_modes={RESULT_DIRECTORY_MODE_AFTER},
        )
        observed = _build_receipt(
            plan_path,
            retained_plan.body,
            plan,
            results,
            applied_at=receipt["applied_at"],
        )
        # applied_at and ctime values are historical receipt evidence.  Compare all
        # current invariants independently, then the exact stable fields.
        for actual, expected in zip(receipt["results"], observed["results"], strict=True):
            if actual != expected:
                raise SealError(f"sealed result differs from receipt: {actual.get('result_path')}")
        retained_receipt.verify(modes={PLAN_FILE_MODE})
        retained_plan.verify(modes={PLAN_FILE_MODE})
        _verify_result_ancestors(results)
        _verify_retained_sources(retained_sources)
        return {
            "receipt_id": receipt["receipt_id"],
            "result_count": len(results),
            "state": "valid_applied_receipt",
        }


def _absolute_cli_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError("path must be absolute")
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan", help="create a prepared, non-applying seal plan")
    plan.add_argument(
        "--source-mode",
        choices=(TWO_SOURCE_CLI_MODE, QUEUE_ONLY_CLI_MODE, BATCH_ONLY_CLI_MODE),
        default=TWO_SOURCE_CLI_MODE,
        help=(
            "two-source-v1 (default) requires one batch and one queue; "
            "queue-only-v2 authorizes exactly one process-routed queue; "
            "batch-only-v3 authorizes exactly one long-window batch"
        ),
    )
    plan.add_argument("--batch-manifest", type=_absolute_cli_path)
    plan.add_argument("--queue-manifest", type=_absolute_cli_path)
    plan.add_argument("--result", action="append", required=True, type=_absolute_cli_path)
    plan.add_argument("--output-directory", required=True, type=_absolute_cli_path)
    plan.add_argument("--store-root", default=DEFAULT_STORE_ROOT, type=_absolute_cli_path)
    validate_plan_parser = subparsers.add_parser(
        "validate-plan", help="validate a prepared plan and unchanged source results"
    )
    validate_plan_parser.add_argument("--plan", required=True, type=_absolute_cli_path)
    apply_parser = subparsers.add_parser("apply", help="apply exactly one reviewed plan")
    apply_parser.add_argument("--plan", required=True, type=_absolute_cli_path)
    receipt_parser = subparsers.add_parser(
        "validate-receipt", help="validate one applied receipt and sealed result set"
    )
    receipt_parser.add_argument("--receipt", required=True, type=_absolute_cli_path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "plan":
            path = build_plan(
                batch_manifest=args.batch_manifest,
                queue_manifest=args.queue_manifest,
                result_paths=args.result,
                output_directory=args.output_directory,
                store_root=args.store_root,
                source_mode=args.source_mode,
            )
            output = {"plan_path": str(path), "state": "prepared_not_applied"}
        elif args.command == "validate-plan":
            output = validate_plan(args.plan)
        elif args.command == "apply":
            path = apply_plan(args.plan)
            output = {"receipt_path": str(path), "state": "applied"}
        else:
            output = validate_receipt(args.receipt)
    except SealError as error:
        print(f"seal error: {error}", file=sys.stderr)
        return 1
    print(json.dumps(output, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
