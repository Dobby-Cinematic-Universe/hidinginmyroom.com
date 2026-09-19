#!/usr/bin/env python3
"""Plan and apply an immutable, manifest-bound contextual-ASR result seal.

This administrative lane is intentionally independent of the raw-ASR result
sealer.  It reads one sealed contextual batch plus an explicit, exact result
allowlist, creates a text-free plan, and can later perform a chmod-only
transition.  It has no ASR, catalog, review, preference, publication, database,
or network operation.
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
import urllib.parse
import uuid
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable


sys.dont_write_bytecode = True

IMPLEMENTATION_VERSION = "0.1.0"
TOOL_NAME = "himr-contextual-asr-result-store-seal"
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CORPUS_SOURCE_ROOT = REPOSITORY_ROOT / "corpus" / "src"
IMPORTER_SOURCE = CORPUS_SOURCE_ROOT / "himr_corpus" / "asr_result_importer.py"
CONTEXTUAL_BATCH_SOURCE = REPOSITORY_ROOT / "pipeline" / "contextual_asr_batch.py"

PLAN_KIND = "contextual_asr_completed_result_seal_plan"
RECEIPT_KIND = "contextual_asr_completed_result_seal_receipt"
SCHEMA_VERSION = 1
RESULT_FILENAMES = (
    "result.json",
    "transcript.normalized.json",
    "whisper.raw.json",
)
ARTIFACT_NAME_BY_KIND = {
    "transcript_normalized_json": "transcript.normalized.json",
    "whispercpp_output_json_full": "whisper.raw.json",
}
ROLE_BY_NAME = {
    "result.json": "result_envelope",
    "transcript.normalized.json": "normalized_transcript",
    "whisper.raw.json": "raw_whisper_output",
}
MAX_FILE_BYTES = 512 * 1024 * 1024
MAX_MANIFEST_BYTES = 32 * 1024 * 1024
MAX_WORK_ORDER_BYTES = 8 * 1024 * 1024
MAX_CONTROL_BYTES = 32 * 1024 * 1024
SOURCE_FILE_MODE = 0o400
PLAN_FILE_MODE = 0o400
RESULT_DIRECTORY_MODE_BEFORE = 0o700
RESULT_DIRECTORY_MODE_AFTER = 0o500
RESULT_FILE_MODES_BEFORE = frozenset({0o600, 0o644})
RESULT_FILE_MODE_AFTER = 0o400
STORE_ROOT_MODE = 0o700
CONTROL_DIRECTORY_MODE = 0o700
APPLY_LOCK_FILE_MODE = 0o600
APPLY_LOCK_FILENAME = ".contextual-asr-result-seal.lock"
CANONICAL_UTC_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")

AUTHORITY = {
    "asr_execution": False,
    "catalog_import": False,
    "database_writes": False,
    "human_review_authority": "none",
    "identity_authority": "none",
    "network_access": "none",
    "publication_authority": "none",
    "transcript_preference_authority": "none",
}
POLICY = {
    "apply_lock_policy": "retained_single_link_control_root_flock_receipt_check_through_commit",
    "allowed_entries": list(RESULT_FILENAMES),
    "catalog_validation": "validate_asr_whispercpp_result_file_catalog_free",
    "directory_mode_after": RESULT_DIRECTORY_MODE_AFTER,
    "directory_mode_before": RESULT_DIRECTORY_MODE_BEFORE,
    "file_mode_after": RESULT_FILE_MODE_AFTER,
    "file_modes_before": sorted(RESULT_FILE_MODES_BEFORE),
    "hardlink_policy": "source_control_and_result_files_require_nlink_1",
    "result_set_policy": "one_explicit_result_per_contextual_manifest_work_order",
    "rollback_policy": "best_effort_restore_exact_recorded_modes_before_receipt_commit",
    "symlink_policy": "resolved_absolute_paths_plus_O_NOFOLLOW_retained_descriptors",
    "transition_policy": "mode_only_two_phase_plan_then_apply",
}


class ContextualSealError(RuntimeError):
    """A fail-closed source, result, plan, apply, or receipt error."""


CatalogValidator = Callable[[Path], dict[str, Any]]


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def pretty_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _canonical_utc_timestamp(value: Any, label: str) -> str:
    if not isinstance(value, str) or CANONICAL_UTC_RE.fullmatch(value) is None:
        raise ContextualSealError(
            f"{label} must be canonical RFC3339 UTC YYYY-MM-DDTHH:MM:SSZ"
        )
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as error:
        raise ContextualSealError(f"{label} is not a real UTC calendar time") from error
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        raise ContextualSealError(f"{label} is not canonical UTC")
    return value


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number {value} is forbidden")


def _reject_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def parse_json(body: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=_reject_pairs,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise ContextualSealError(f"{label} is not strict UTF-8 JSON: {error}") from error
    if not isinstance(value, dict):
        raise ContextualSealError(f"{label} must contain one JSON object")
    return value


def _mode(value: os.stat_result) -> int:
    return stat.S_IMODE(value.st_mode)


def _same_object(left: os.stat_result, right: os.stat_result) -> bool:
    return left.st_dev == right.st_dev and left.st_ino == right.st_ino


def _fingerprint(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        value.st_nlink,
        stat.S_IFMT(value.st_mode),
        _mode(value),
    )


def _require_absolute_resolved(path: Path, label: str) -> Path:
    if not path.is_absolute():
        raise ContextualSealError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise ContextualSealError(f"{label} is unavailable: {error}") from error
    if resolved != path:
        raise ContextualSealError(
            f"{label} must be resolved and contain no symlink or traversal component"
        )
    return path


def _require_beneath(path: Path, root: Path, label: str) -> None:
    try:
        path.relative_to(root)
    except ValueError as error:
        raise ContextualSealError(f"{label} escapes the contextual output root") from error


def _pread_all(fd: int, size: int, maximum: int, label: str) -> bytes:
    if size < 1 or size > maximum:
        raise ContextualSealError(
            f"{label} byte count {size} is outside 1..{maximum}"
        )
    parts: list[bytes] = []
    offset = 0
    while offset < size:
        chunk = os.pread(fd, min(1024 * 1024, size - offset), offset)
        if not chunk:
            break
        parts.append(chunk)
        offset += len(chunk)
    body = b"".join(parts)
    if len(body) != size:
        raise ContextualSealError(f"{label} changed size while being read")
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
        modes: set[int] | frozenset[int],
        expected_sha256: str | None = None,
        expected_device: int | None = None,
        expected_inode: int | None = None,
        expected_mtime_ns: int | None = None,
        expected_ctime_ns: int | None = None,
        require_opened_ctime_when_unmodified: bool = False,
    ) -> os.stat_result:
        descriptor = os.fstat(self.fd)
        try:
            logical = self.path.lstat()
        except OSError as error:
            raise ContextualSealError(f"{self.label} disappeared: {error}") from error
        if (
            not stat.S_ISREG(descriptor.st_mode)
            or not stat.S_ISREG(logical.st_mode)
            or not _same_object(descriptor, logical)
        ):
            raise ContextualSealError(f"{self.label} path was replaced or is not regular")
        if descriptor.st_nlink != 1 or logical.st_nlink != 1:
            raise ContextualSealError(f"{self.label} must have exactly one hard link")
        if _mode(descriptor) not in modes or _mode(logical) not in modes:
            allowed = ", ".join(f"{item:04o}" for item in sorted(modes))
            raise ContextualSealError(f"{self.label} mode must be one of {allowed}")
        if expected_device is not None and descriptor.st_dev != expected_device:
            raise ContextualSealError(f"{self.label} device differs from the plan")
        if expected_inode is not None and descriptor.st_ino != expected_inode:
            raise ContextualSealError(f"{self.label} inode differs from the plan")
        if expected_mtime_ns is not None and descriptor.st_mtime_ns != expected_mtime_ns:
            raise ContextualSealError(f"{self.label} mtime differs from the plan")
        if expected_ctime_ns is not None and descriptor.st_ctime_ns != expected_ctime_ns:
            raise ContextualSealError(f"{self.label} ctime differs from the receipt")
        if (
            require_opened_ctime_when_unmodified
            and _mode(descriptor) == _mode(self.opened)
            and descriptor.st_ctime_ns != self.opened.st_ctime_ns
        ):
            raise ContextualSealError(f"{self.label} metadata changed during the operation")
        if descriptor.st_size != len(self.body):
            raise ContextualSealError(f"{self.label} byte count changed")
        current = _pread_all(self.fd, descriptor.st_size, max(descriptor.st_size, 1), self.label)
        after = os.fstat(self.fd)
        if _fingerprint(descriptor) != _fingerprint(after):
            raise ContextualSealError(f"{self.label} changed while being re-read")
        digest = sha256_bytes(current)
        if expected_sha256 is not None and digest != expected_sha256:
            raise ContextualSealError(f"{self.label} digest differs from the plan")
        if current != self.body:
            raise ContextualSealError(f"{self.label} content changed during the operation")
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
        modes: set[int] | frozenset[int],
        entries: set[str] | None = None,
        expected_device: int | None = None,
        expected_inode: int | None = None,
        expected_mtime_ns: int | None = None,
        expected_ctime_ns: int | None = None,
        require_opened_ctime_when_unmodified: bool = False,
    ) -> os.stat_result:
        descriptor = os.fstat(self.fd)
        try:
            logical = self.path.lstat()
        except OSError as error:
            raise ContextualSealError(f"{self.label} disappeared: {error}") from error
        if (
            not stat.S_ISDIR(descriptor.st_mode)
            or not stat.S_ISDIR(logical.st_mode)
            or not _same_object(descriptor, logical)
        ):
            raise ContextualSealError(f"{self.label} path was replaced or is not a directory")
        if _mode(descriptor) not in modes or _mode(logical) not in modes:
            allowed = ", ".join(f"{item:04o}" for item in sorted(modes))
            raise ContextualSealError(f"{self.label} mode must be one of {allowed}")
        if expected_device is not None and descriptor.st_dev != expected_device:
            raise ContextualSealError(f"{self.label} device differs from the plan")
        if expected_inode is not None and descriptor.st_ino != expected_inode:
            raise ContextualSealError(f"{self.label} inode differs from the plan")
        if expected_mtime_ns is not None and descriptor.st_mtime_ns != expected_mtime_ns:
            raise ContextualSealError(f"{self.label} mtime differs from the plan")
        if expected_ctime_ns is not None and descriptor.st_ctime_ns != expected_ctime_ns:
            raise ContextualSealError(f"{self.label} ctime differs from the receipt")
        if (
            require_opened_ctime_when_unmodified
            and _mode(descriptor) == _mode(self.opened)
            and descriptor.st_ctime_ns != self.opened.st_ctime_ns
        ):
            raise ContextualSealError(f"{self.label} metadata changed during the operation")
        if entries is not None:
            observed = set(os.listdir(self.fd))
            if observed != entries:
                raise ContextualSealError(
                    f"{self.label} entries differ: expected {sorted(entries)}, "
                    f"observed {sorted(observed)}"
                )
        return descriptor


def _open_file(
    stack: ExitStack,
    path: Path,
    label: str,
    *,
    maximum: int,
    modes: set[int] | frozenset[int] | None = None,
    dir_fd: int | None = None,
    name: str | None = None,
) -> RetainedFile:
    _require_absolute_resolved(path, label)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(name if name is not None else path, flags, dir_fd=dir_fd)
    except OSError as error:
        raise ContextualSealError(f"cannot retain {label}: {error}") from error
    stack.callback(os.close, fd)
    opened = os.fstat(fd)
    logical = path.lstat()
    if (
        not stat.S_ISREG(opened.st_mode)
        or not _same_object(opened, logical)
        or opened.st_nlink != 1
    ):
        raise ContextualSealError(f"{label} is not one single-link retained regular file")
    if modes is not None and _mode(opened) not in modes:
        allowed = ", ".join(f"{item:04o}" for item in sorted(modes))
        raise ContextualSealError(f"{label} mode must be one of {allowed}")
    before = _fingerprint(opened)
    body = _pread_all(fd, opened.st_size, maximum, label)
    if before != _fingerprint(os.fstat(fd)) or before != _fingerprint(path.lstat()):
        raise ContextualSealError(f"{label} changed while being retained")
    return RetainedFile(path=path, fd=fd, opened=opened, body=body, label=label)


def _open_directory(
    stack: ExitStack,
    path: Path,
    label: str,
    *,
    modes: set[int] | frozenset[int] | None = None,
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
        raise ContextualSealError(f"cannot retain {label}: {error}") from error
    stack.callback(os.close, fd)
    opened = os.fstat(fd)
    logical = path.lstat()
    if not stat.S_ISDIR(opened.st_mode) or not _same_object(opened, logical):
        raise ContextualSealError(f"{label} is not one retained real directory")
    if modes is not None and _mode(opened) not in modes:
        allowed = ", ".join(f"{item:04o}" for item in sorted(modes))
        raise ContextualSealError(f"{label} mode must be one of {allowed}")
    return RetainedDirectory(path=path, fd=fd, opened=opened, label=label)


def _file_identity(path: Path) -> dict[str, Any]:
    with ExitStack() as stack:
        item = _open_file(stack, path, str(path), maximum=32 * 1024 * 1024)
        return {
            "byte_count": len(item.body),
            "path": str(path),
            "sha256": sha256_bytes(item.body),
        }


def catalog_free_validate(result_path: Path) -> dict[str, Any]:
    source_text = str(CORPUS_SOURCE_ROOT)
    added = False
    if source_text not in sys.path:
        sys.path.insert(0, source_text)
        added = True
    try:
        from himr_corpus.asr_result_importer import (  # noqa: PLC0415
            validate_asr_whispercpp_result_file,
        )

        return validate_asr_whispercpp_result_file(result_path)
    finally:
        if added:
            sys.path.remove(source_text)


def _contextual_module() -> Any:
    pipeline_text = str(CONTEXTUAL_BATCH_SOURCE.parent)
    added = False
    if pipeline_text not in sys.path:
        sys.path.insert(0, pipeline_text)
        added = True
    try:
        import contextual_asr_batch  # noqa: PLC0415

        return contextual_asr_batch
    finally:
        if added:
            sys.path.remove(pipeline_text)


def _catalog_summary(value: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "artifact_count",
        "glossary_revision_id",
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


def _file_uri_path(value: Any, label: str) -> Path:
    if not isinstance(value, str):
        raise ContextualSealError(f"{label} must be a file URI")
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme != "file"
        or parsed.netloc not in ("", "localhost")
        or parsed.query
        or parsed.fragment
    ):
        raise ContextualSealError(f"{label} must be one local file URI")
    path = Path(urllib.parse.unquote(parsed.path))
    return _require_absolute_resolved(path, label)


@dataclass
class RetainedSource:
    manifest: dict[str, Any]
    orders: list[dict[str, Any]]
    files: list[RetainedFile]
    directories: list[RetainedDirectory]
    record: dict[str, Any]


def _open_source(stack: ExitStack, manifest_path: Path) -> RetainedSource:
    manifest_path = _require_absolute_resolved(manifest_path, "contextual manifest")
    batch_dir = _open_directory(
        stack, manifest_path.parent, "contextual batch directory", modes={0o500}
    )
    order_dir = _open_directory(
        stack,
        manifest_path.parent / "work-orders",
        "contextual work-order directory",
        modes={0o500},
    )
    batch_dir.verify(modes={0o500}, entries={"manifest.json", "work-orders"})
    manifest_file = _open_file(
        stack,
        manifest_path,
        "contextual manifest",
        maximum=MAX_MANIFEST_BYTES,
        modes={SOURCE_FILE_MODE},
        dir_fd=batch_dir.fd,
        name="manifest.json",
    )
    supplied = parse_json(manifest_file.body, "contextual manifest")
    entries = supplied.get("work_orders")
    if not isinstance(entries, list) or not entries:
        raise ContextualSealError("contextual manifest needs a non-empty work_orders array")
    if supplied.get("work_order_count") != len(entries):
        raise ContextualSealError("contextual manifest work_order_count is inconsistent")
    expected_names = {f"{ordinal:06d}.json" for ordinal in range(1, len(entries) + 1)}
    order_dir.verify(modes={0o500}, entries=expected_names)
    source_files = [manifest_file]
    order_records: list[dict[str, Any]] = []
    order_bodies: list[bytes] = []
    for ordinal, entry in enumerate(entries, 1):
        if not isinstance(entry, dict):
            raise ContextualSealError(f"contextual manifest work order {ordinal} is malformed")
        name = f"{ordinal:06d}.json"
        if entry.get("ordinal") != ordinal or entry.get("path") != f"work-orders/{name}":
            raise ContextualSealError("contextual work-order ordinal/path closure differs")
        path = manifest_path.parent / "work-orders" / name
        retained = _open_file(
            stack,
            path,
            f"contextual work order {ordinal}",
            maximum=MAX_WORK_ORDER_BYTES,
            modes={SOURCE_FILE_MODE},
            dir_fd=order_dir.fd,
            name=name,
        )
        body = retained.body
        raw = parse_json(body, f"contextual work order {ordinal}")
        raw_sha = sha256_bytes(body)
        canonical_sha = sha256_bytes(canonical_bytes(raw))
        if (
            entry.get("byte_count") != len(body)
            or entry.get("sha256") != raw_sha
            or entry.get("canonical_sha256") != canonical_sha
            or entry.get("job_id") != raw.get("job_id")
        ):
            raise ContextualSealError(
                f"contextual work order {ordinal} differs from the manifest pin"
            )
        source_files.append(retained)
        order_bodies.append(body)
        order_records.append(
            {
                "byte_count": len(body),
                "canonical_sha256": canonical_sha,
                "input_sha256": raw.get("input", {}).get("expected_sha256"),
                "job_id": raw.get("job_id"),
                "ordinal": ordinal,
                "path": str(path),
                "sha256": raw_sha,
            }
        )
    contextual = _contextual_module()
    try:
        validated_manifest, orders = contextual.validate_batch(manifest_path)
    except Exception as error:
        raise ContextualSealError(
            f"contextual batch deterministic validation failed: {error}"
        ) from error
    if validated_manifest != supplied or len(orders) != len(order_records):
        raise ContextualSealError("contextual batch validator replay differs from retained source")
    for ordinal, (order, body, record) in enumerate(
        zip(orders, order_bodies, order_records, strict=True), 1
    ):
        canonical_sha = sha256_bytes(canonical_bytes(order))
        if canonical_sha != record["canonical_sha256"]:
            raise ContextualSealError(
                f"validated contextual work order {ordinal} canonical digest differs"
            )
    for retained in source_files:
        retained.verify(
            modes={SOURCE_FILE_MODE},
            expected_sha256=sha256_bytes(retained.body),
            require_opened_ctime_when_unmodified=True,
        )
    batch_dir.verify(
        modes={0o500},
        entries={"manifest.json", "work-orders"},
        require_opened_ctime_when_unmodified=True,
    )
    order_dir.verify(
        modes={0o500},
        entries=expected_names,
        require_opened_ctime_when_unmodified=True,
    )
    output = supplied.get("output")
    glossary = supplied.get("glossary")
    if not isinstance(output, dict) or not isinstance(glossary, dict):
        raise ContextualSealError("contextual manifest output/glossary blocks are malformed")
    record = {
        "asr_output_root": output.get("asr_output_root"),
        "batch_id": supplied.get("batch_id"),
        "byte_count": len(manifest_file.body),
        "canonical_sha256": sha256_bytes(canonical_bytes(supplied)),
        "glossary_revision_id": glossary.get("glossary_revision_id"),
        "identity_sha256": supplied.get("identity_sha256"),
        "manifest_path": str(manifest_path),
        "sha256": sha256_bytes(manifest_file.body),
        "work_order_count": len(order_records),
        "work_orders": order_records,
    }
    return RetainedSource(
        manifest=supplied,
        orders=orders,
        files=source_files,
        directories=[batch_dir, order_dir],
        record=record,
    )


def _verify_source_retained(source: RetainedSource) -> None:
    for retained in source.files:
        retained.verify(
            modes={SOURCE_FILE_MODE}, expected_sha256=sha256_bytes(retained.body)
        )
    source.directories[0].verify(
        modes={0o500}, entries={"manifest.json", "work-orders"}
    )
    source.directories[1].verify(
        modes={0o500},
        entries={f"{ordinal:06d}.json" for ordinal in range(1, len(source.orders) + 1)},
    )


@dataclass
class RetainedResult:
    ancestors: list[RetainedDirectory]
    directory: RetainedDirectory
    files: dict[str, RetainedFile]
    raw: dict[str, Any]
    record: dict[str, Any]


def _open_ancestors(
    stack: ExitStack, result_directory: Path, store_root: Path
) -> list[RetainedDirectory]:
    _require_beneath(result_directory, store_root, "contextual result directory")
    relative = result_directory.relative_to(store_root)
    paths = [store_root]
    current = store_root
    for component in relative.parts[:-1]:
        current = current / component
        paths.append(current)
    retained: list[RetainedDirectory] = []
    for path in paths:
        item = _open_directory(stack, path, f"contextual result ancestor {path}")
        item.verify(modes={_mode(item.opened)})
        retained.append(item)
    return retained


def _verify_ancestors(results: Iterable[RetainedResult]) -> None:
    seen: set[tuple[int, int]] = set()
    for result in results:
        for ancestor in result.ancestors:
            key = (ancestor.opened.st_dev, ancestor.opened.st_ino)
            if key in seen:
                continue
            seen.add(key)
            ancestor.verify(modes={_mode(ancestor.opened)})


def _stat_record(value: os.stat_result, *, mode_after: int) -> dict[str, Any]:
    return {
        "ctime_ns_before": value.st_ctime_ns,
        "device": value.st_dev,
        "inode": value.st_ino,
        "mode_after": mode_after,
        "mode_before": _mode(value),
        "mtime_ns": value.st_mtime_ns,
        "nlink": value.st_nlink,
    }


def _expected_layout(root: Path, input_sha: str, result_key: str) -> Path:
    return (
        root
        / "asr"
        / "whispercpp"
        / "sha256"
        / input_sha[:2]
        / input_sha
        / "results"
        / result_key
        / "result.json"
    )


def _open_result(
    stack: ExitStack,
    result_path: Path,
    store_root: Path,
    entry_by_tuple: dict[tuple[str, str], tuple[int, dict[str, Any], dict[str, Any]]],
    source: RetainedSource,
    validator: CatalogValidator,
    *,
    allowed_file_modes: set[int] | frozenset[int],
    allowed_directory_modes: set[int] | frozenset[int],
) -> RetainedResult:
    result_path = _require_absolute_resolved(result_path, "contextual result")
    _require_beneath(result_path, store_root, "contextual result")
    if result_path.name != "result.json":
        raise ContextualSealError("each contextual result path must end in result.json")
    ancestors = _open_ancestors(stack, result_path.parent, store_root)
    directory = _open_directory(
        stack,
        result_path.parent,
        f"contextual result directory {result_path.parent}",
        modes=allowed_directory_modes,
    )
    directory_stat = directory.verify(
        modes=allowed_directory_modes, entries=set(RESULT_FILENAMES)
    )
    files: dict[str, RetainedFile] = {}
    for name in RESULT_FILENAMES:
        files[name] = _open_file(
            stack,
            result_path.parent / name,
            f"contextual result file {result_path.parent / name}",
            maximum=MAX_FILE_BYTES,
            modes=allowed_file_modes,
            dir_fd=directory.fd,
            name=name,
        )
    raw = parse_json(files["result.json"].body, f"contextual result {result_path}")
    key = (raw.get("job_id"), raw.get("work_order_sha256"))
    if key not in entry_by_tuple:
        raise ContextualSealError(
            f"contextual result is not selected by the sealed manifest: {result_path}"
        )
    source_ordinal, entry, order = entry_by_tuple[key]
    input_sha = order.get("input", {}).get("expected_sha256")
    result_key = raw.get("result_key")
    if not isinstance(input_sha, str) or not isinstance(result_key, str):
        raise ContextualSealError("contextual result input/result identity is malformed")
    if raw.get("result_path") != str(result_path):
        raise ContextualSealError("contextual result envelope names a different path")
    if _expected_layout(store_root, input_sha, result_key) != result_path:
        raise ContextualSealError("contextual result path differs from the closed adapter layout")
    contextual = _contextual_module()
    try:
        adapter_summary = contextual._validate_adapter_result(  # noqa: SLF001
            raw,
            order,
            entry,
            source.manifest["glossary"],
            source.manifest["engine"],
            source.manifest["model"],
            dry_run=False,
        )
        catalog = validator(result_path)
    except Exception as error:
        raise ContextualSealError(
            f"strict contextual result validation failed for {result_path}: {error}"
        ) from error
    canonical_result_sha = sha256_bytes(canonical_bytes(raw))
    if catalog.get("result_envelope_sha256") != canonical_result_sha:
        raise ContextualSealError("catalog-free result canonical digest differs")
    if catalog.get("glossary_revision_id") != source.record["glossary_revision_id"]:
        raise ContextualSealError("catalog-free result glossary binding differs")
    artifacts = raw.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 2:
        raise ContextualSealError("contextual result must bind exactly two artifacts")
    seen_artifacts: set[str] = set()
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            raise ContextualSealError("contextual artifact reference must be an object")
        name = ARTIFACT_NAME_BY_KIND.get(artifact.get("artifact_kind"))
        if name is None or name in seen_artifacts:
            raise ContextualSealError("contextual artifact kinds are missing or duplicated")
        seen_artifacts.add(name)
        retained = files[name]
        if (
            _file_uri_path(artifact.get("storage_uri"), "contextual artifact URI")
            != retained.path
            or artifact.get("sha256") != sha256_bytes(retained.body)
            or artifact.get("byte_count") != len(retained.body)
            or artifact.get("visibility") != "private"
        ):
            raise ContextualSealError("contextual artifact reference differs from retained bytes")
    file_records: list[dict[str, Any]] = []
    for name in RESULT_FILENAMES:
        retained = files[name]
        file_records.append(
            {
                "byte_count": len(retained.body),
                "name": name,
                "path": str(retained.path),
                "role": ROLE_BY_NAME[name],
                "sha256": sha256_bytes(retained.body),
                **_stat_record(retained.opened, mode_after=RESULT_FILE_MODE_AFTER),
            }
        )
    record = {
        "adapter_validation": adapter_summary,
        "catalog_free_validation": _catalog_summary(catalog),
        "directory": {
            "path": str(result_path.parent),
            **_stat_record(directory_stat, mode_after=RESULT_DIRECTORY_MODE_AFTER),
        },
        "files": file_records,
        "input_sha256": input_sha,
        "job_id": raw["job_id"],
        "ordinal": source_ordinal,
        "recipe_id": raw.get("recipe_id"),
        "result_canonical_sha256": canonical_result_sha,
        "result_key": result_key,
        "result_path": str(result_path),
        "source_ordinal": source_ordinal,
        "work_order_physical_sha256": source.record["work_orders"][source_ordinal - 1]["sha256"],
        "work_order_sha256": raw["work_order_sha256"],
    }
    for retained in files.values():
        retained.verify(
            modes=allowed_file_modes,
            expected_sha256=sha256_bytes(retained.body),
            require_opened_ctime_when_unmodified=True,
        )
    directory.verify(
        modes=allowed_directory_modes,
        entries=set(RESULT_FILENAMES),
        require_opened_ctime_when_unmodified=True,
    )
    return RetainedResult(
        ancestors=ancestors,
        directory=directory,
        files=files,
        raw=raw,
        record=record,
    )


def _source_entry_map(
    source: RetainedSource,
) -> dict[tuple[str, str], tuple[int, dict[str, Any], dict[str, Any]]]:
    result: dict[tuple[str, str], tuple[int, dict[str, Any], dict[str, Any]]] = {}
    for ordinal, (entry, order) in enumerate(
        zip(source.manifest["work_orders"], source.orders, strict=True), 1
    ):
        key = (order["job_id"], sha256_bytes(canonical_bytes(order)))
        if key in result:
            raise ContextualSealError("contextual manifest contains a duplicate result tuple")
        result[key] = (ordinal, entry, order)
    return result


def _open_exact_results(
    stack: ExitStack,
    paths: Iterable[Path],
    source: RetainedSource,
    validator: CatalogValidator,
    *,
    allowed_file_modes: set[int] | frozenset[int],
    allowed_directory_modes: set[int] | frozenset[int],
) -> list[RetainedResult]:
    store_root = _require_absolute_resolved(
        Path(source.record["asr_output_root"]), "contextual output root"
    )
    if _mode(store_root.lstat()) != STORE_ROOT_MODE:
        raise ContextualSealError("contextual output root must remain mode 0700")
    supplied = list(paths)
    if len(supplied) != len(set(supplied)):
        raise ContextualSealError("contextual result allowlist contains duplicate paths")
    entry_map = _source_entry_map(source)
    if len(supplied) != len(entry_map):
        raise ContextualSealError(
            f"exact result closure requires {len(entry_map)} paths, got {len(supplied)}"
        )
    results: list[RetainedResult] = []
    matched: set[tuple[str, str]] = set()
    for path in supplied:
        retained = _open_result(
            stack,
            path,
            store_root,
            entry_map,
            source,
            validator,
            allowed_file_modes=allowed_file_modes,
            allowed_directory_modes=allowed_directory_modes,
        )
        key = (retained.raw.get("job_id"), retained.raw.get("work_order_sha256"))
        if key in matched:
            raise ContextualSealError("multiple results claim one manifest work order")
        matched.add(key)
        results.append(retained)
    if matched != set(entry_map):
        raise ContextualSealError("contextual results do not exactly cover the manifest")
    results.sort(key=lambda item: item.record["source_ordinal"])
    for ordinal, result in enumerate(results, 1):
        if result.record["ordinal"] != ordinal:
            raise ContextualSealError("contextual result ordinals are not contiguous")
    return results


def _document_identity(document: dict[str, Any], omitted: set[str]) -> str:
    return sha256_bytes(canonical_bytes({k: v for k, v in document.items() if k not in omitted}))


def _control_root(store_root: Path, batch_id: str) -> Path:
    return store_root / "contextual-sealing-control" / batch_id


def _ensure_private_directory(path: Path) -> None:
    missing: list[Path] = []
    current = path
    while not current.exists():
        if current.is_symlink():
            raise ContextualSealError(f"control path {current} is a symlink")
        missing.append(current)
        current = current.parent
    if not current.is_dir() or current.is_symlink():
        raise ContextualSealError(f"control ancestor {current} is not a real directory")
    for item in reversed(missing):
        try:
            item.mkdir(mode=CONTROL_DIRECTORY_MODE)
        except OSError as error:
            raise ContextualSealError(f"cannot create control directory {item}: {error}") from error
    for item in (path, path.parent):
        if not item.is_dir() or item.is_symlink() or _mode(item.lstat()) != CONTROL_DIRECTORY_MODE:
            raise ContextualSealError(f"control directory {item} must be real mode 0700")


def _atomic_control_json(path: Path, document: dict[str, Any]) -> None:
    body = canonical_bytes(document) + b"\n"
    if len(body) > MAX_CONTROL_BYTES:
        raise ContextualSealError("control document exceeds its byte limit")
    _ensure_private_directory(path.parent)
    if path.exists() or path.is_symlink():
        raise ContextualSealError(f"refusing to replace control document {path}")
    temp = path.parent / f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    fd: int | None = None
    linked = False
    try:
        fd = os.open(temp, flags, 0o600)
        offset = 0
        while offset < len(body):
            offset += os.write(fd, body[offset:])
        os.fsync(fd)
        os.fchmod(fd, PLAN_FILE_MODE)
        os.fsync(fd)
        os.link(temp, path, follow_symlinks=False)
        linked = True
        target_stat = path.lstat()
        descriptor_stat = os.fstat(fd)
        if not _same_object(target_stat, descriptor_stat):
            raise ContextualSealError(
                f"committed control path {path} differs from its retained file"
            )
        os.unlink(temp)
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        os.close(fd)
        fd = None
    except OSError as error:
        if linked and fd is not None:
            try:
                target_stat = path.lstat()
                if _same_object(target_stat, os.fstat(fd)):
                    path.unlink()
            except OSError:
                pass
        try:
            temp.unlink()
        except OSError:
            pass
        if fd is not None:
            os.close(fd)
        raise ContextualSealError(f"cannot atomically commit {path}: {error}") from error
    except Exception:
        if linked and fd is not None:
            try:
                target_stat = path.lstat()
                if _same_object(target_stat, os.fstat(fd)):
                    path.unlink()
            except OSError:
                pass
        try:
            temp.unlink()
        except OSError:
            pass
        if fd is not None:
            os.close(fd)
        raise


def _ensure_apply_lock(control_root: Path) -> Path:
    path = control_root / APPLY_LOCK_FILENAME
    flags = (
        os.O_RDWR
        | os.O_CREAT
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        fd = os.open(path, flags, APPLY_LOCK_FILE_MODE)
    except OSError as error:
        raise ContextualSealError(f"cannot create/open contextual apply lock: {error}") from error
    try:
        descriptor = os.fstat(fd)
        logical = path.lstat()
        if (
            not stat.S_ISREG(descriptor.st_mode)
            or not stat.S_ISREG(logical.st_mode)
            or not _same_object(descriptor, logical)
            or descriptor.st_nlink != 1
            or logical.st_nlink != 1
        ):
            raise ContextualSealError(
                "contextual apply lock must be one retained single-link regular file"
            )
        os.fchmod(fd, APPLY_LOCK_FILE_MODE)
        os.fsync(fd)
        descriptor = os.fstat(fd)
        logical = path.lstat()
        if (
            not _same_object(descriptor, logical)
            or _mode(descriptor) != APPLY_LOCK_FILE_MODE
            or _mode(logical) != APPLY_LOCK_FILE_MODE
            or descriptor.st_nlink != 1
        ):
            raise ContextualSealError("contextual apply lock changed during creation")
    finally:
        os.close(fd)
    return path


def _retain_apply_lock(
    stack: ExitStack, control_root: Path, *, acquire: bool
) -> int:
    path = _require_absolute_resolved(
        control_root / APPLY_LOCK_FILENAME, "contextual apply lock"
    )
    flags = (
        os.O_RDWR
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        fd = os.open(path, flags)
    except OSError as error:
        raise ContextualSealError(f"cannot retain contextual apply lock: {error}") from error
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
        raise ContextualSealError(
            "contextual apply lock must remain one retained mode-0600 file"
        )
    if acquire:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
        except OSError as error:
            raise ContextualSealError(f"cannot acquire contextual apply lock: {error}") from error
        descriptor = os.fstat(fd)
        logical = path.lstat()
        if (
            not _same_object(descriptor, logical)
            or descriptor.st_nlink != 1
            or _mode(descriptor) != APPLY_LOCK_FILE_MODE
            or _mode(logical) != APPLY_LOCK_FILE_MODE
        ):
            raise ContextualSealError("contextual apply lock was replaced after acquisition")
    return fd


def _plan_document(
    source: RetainedSource, results: list[RetainedResult], *, created_at: str
) -> dict[str, Any]:
    created_at = _canonical_utc_timestamp(created_at, "plan created_at")
    store_root = Path(source.record["asr_output_root"])
    document: dict[str, Any] = {
        "authority": AUTHORITY,
        "created_at": created_at,
        "implementation": _file_identity(Path(__file__).resolve()),
        "importer_validator": _file_identity(IMPORTER_SOURCE.resolve()),
        "kind": PLAN_KIND,
        "policy": POLICY,
        "result_count": len(results),
        "results": [item.record for item in results],
        "schema_version": SCHEMA_VERSION,
        "source": source.record,
        "state": "prepared_not_applied",
        "store_root": str(store_root),
    }
    identity = _document_identity(document, set())
    plan_id = f"ctxasrsealplan_{identity[:32]}"
    control = _control_root(store_root, source.record["batch_id"])
    document.update(
        {
            "identity_sha256": identity,
            "plan_id": plan_id,
            "receipt_path": str(
                control / "receipts" / f"ctxasrsealreceipt_{identity[:32]}.json"
            ),
        }
    )
    return document


def build_plan(
    *,
    manifest_path: Path,
    result_paths: Iterable[Path],
    dry_run: bool = False,
    validator: CatalogValidator = catalog_free_validate,
    created_at: str | None = None,
) -> Path | dict[str, Any]:
    paths = list(result_paths)
    with ExitStack() as stack:
        source = _open_source(stack, manifest_path)
        store_root = _require_absolute_resolved(
            Path(source.record["asr_output_root"]), "contextual output root"
        )
        if not dry_run:
            # Authorized control-container creation precedes the result observation
            # window and never changes a result directory or result file.
            control = _control_root(store_root, source.record["batch_id"])
            _ensure_private_directory(control / "plans")
            _ensure_private_directory(control / "receipts")
            _ensure_apply_lock(control)
        results = _open_exact_results(
            stack,
            paths,
            source,
            validator,
            allowed_file_modes=RESULT_FILE_MODES_BEFORE,
            allowed_directory_modes={RESULT_DIRECTORY_MODE_BEFORE},
        )
        plan = _plan_document(source, results, created_at=created_at or utc_now())
        _verify_source_retained(source)
        _verify_ancestors(results)
        for result in results:
            for item in result.files.values():
                item.verify(
                    modes=RESULT_FILE_MODES_BEFORE,
                    expected_sha256=sha256_bytes(item.body),
                )
            result.directory.verify(
                modes={RESULT_DIRECTORY_MODE_BEFORE}, entries=set(RESULT_FILENAMES)
            )
        if dry_run:
            return {
                "batch_id": source.record["batch_id"],
                "identity_sha256": plan["identity_sha256"],
                "plan_id": plan["plan_id"],
                "result_count": len(results),
                "state": "dry_run_valid_no_writes",
            }
        plan_path = (
            _control_root(store_root, source.record["batch_id"])
            / "plans"
            / f"{plan['plan_id']}.json"
        )
        _atomic_control_json(plan_path, plan)
        return plan_path


PLAN_KEYS = {
    "authority",
    "created_at",
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
    "source",
    "state",
    "store_root",
}
SOURCE_KEYS = {
    "asr_output_root",
    "batch_id",
    "byte_count",
    "canonical_sha256",
    "glossary_revision_id",
    "identity_sha256",
    "manifest_path",
    "sha256",
    "work_order_count",
    "work_orders",
}
WORK_ORDER_KEYS = {
    "byte_count",
    "canonical_sha256",
    "input_sha256",
    "job_id",
    "ordinal",
    "path",
    "sha256",
}
FILE_IDENTITY_KEYS = {"byte_count", "path", "sha256"}
RESULT_KEYS = {
    "adapter_validation",
    "catalog_free_validation",
    "directory",
    "files",
    "input_sha256",
    "job_id",
    "ordinal",
    "recipe_id",
    "result_canonical_sha256",
    "result_key",
    "result_path",
    "source_ordinal",
    "work_order_physical_sha256",
    "work_order_sha256",
}
DIRECTORY_KEYS = {
    "ctime_ns_before",
    "device",
    "inode",
    "mode_after",
    "mode_before",
    "mtime_ns",
    "nlink",
    "path",
}
FILE_KEYS = DIRECTORY_KEYS | {"byte_count", "name", "role", "sha256"}
ADAPTER_VALIDATION_KEYS = {
    "baseline_result_canonical_sha256",
    "glossary_revision_id",
    "processing_run_id",
    "recipe_id",
    "result_key",
    "result_path",
    "status",
    "work_order_sha256",
}
CATALOG_VALIDATION_KEYS = {
    "artifact_count",
    "glossary_revision_id",
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
}


def _exact_keys(value: Any, expected: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        observed = set(value) if isinstance(value, dict) else set()
        raise ContextualSealError(
            f"{label} fields differ (missing={sorted(expected - observed)}, "
            f"unknown={sorted(observed - expected)})"
        )
    return value


def _validate_plan_shape(plan: dict[str, Any]) -> None:
    _exact_keys(plan, PLAN_KEYS, "contextual seal plan")
    _exact_keys(plan["implementation"], FILE_IDENTITY_KEYS, "plan implementation")
    _exact_keys(plan["importer_validator"], FILE_IDENTITY_KEYS, "plan importer")
    source = _exact_keys(plan["source"], SOURCE_KEYS, "plan source")
    orders = source.get("work_orders")
    if not isinstance(orders, list) or not orders:
        raise ContextualSealError("plan source work_orders must be non-empty")
    for ordinal, order in enumerate(orders, 1):
        _exact_keys(order, WORK_ORDER_KEYS, f"plan source work order {ordinal}")
        if order.get("ordinal") != ordinal:
            raise ContextualSealError("plan source work-order ordinals are not contiguous")
    results = plan.get("results")
    if not isinstance(results, list) or len(results) != len(orders):
        raise ContextualSealError("plan results do not exactly cover source work orders")
    seen_paths: set[str] = set()
    for ordinal, result in enumerate(results, 1):
        _exact_keys(result, RESULT_KEYS, f"plan result {ordinal}")
        if result.get("ordinal") != ordinal or result.get("source_ordinal") != ordinal:
            raise ContextualSealError("plan result ordinals are not contiguous")
        if result.get("result_path") in seen_paths:
            raise ContextualSealError("plan result paths are duplicated")
        seen_paths.add(result["result_path"])
        directory = _exact_keys(result["directory"], DIRECTORY_KEYS, "plan result directory")
        if (
            directory.get("path") != str(Path(result["result_path"]).parent)
            or directory.get("mode_before") != RESULT_DIRECTORY_MODE_BEFORE
            or directory.get("mode_after") != RESULT_DIRECTORY_MODE_AFTER
        ):
            raise ContextualSealError("plan result directory record is inconsistent")
        files = result.get("files")
        if not isinstance(files, list) or [item.get("name") for item in files] != list(RESULT_FILENAMES):
            raise ContextualSealError("plan result files are not the exact ordered allowlist")
        for item in files:
            _exact_keys(item, FILE_KEYS, "plan result file")
            if (
                item["path"] != str(Path(result["result_path"]).parent / item["name"])
                or item["role"] != ROLE_BY_NAME[item["name"]]
                or item["mode_before"] not in RESULT_FILE_MODES_BEFORE
                or item["mode_after"] != RESULT_FILE_MODE_AFTER
                or item["nlink"] != 1
            ):
                raise ContextualSealError("plan result file record is inconsistent")
        _exact_keys(result["adapter_validation"], ADAPTER_VALIDATION_KEYS, "adapter validation")
        _exact_keys(result["catalog_free_validation"], CATALOG_VALIDATION_KEYS, "catalog validation")
    if plan.get("result_count") != len(results) or source.get("work_order_count") != len(orders):
        raise ContextualSealError("plan result/source counts are inconsistent")


def _read_plan(path: Path) -> tuple[dict[str, Any], RetainedFile, ExitStack]:
    stack = ExitStack()
    try:
        path = _require_absolute_resolved(path, "contextual seal plan")
        retained = _open_file(
            stack,
            path,
            "contextual seal plan",
            maximum=MAX_CONTROL_BYTES,
            modes={PLAN_FILE_MODE},
        )
        plan = parse_json(retained.body, "contextual seal plan")
        if retained.body != canonical_bytes(plan) + b"\n":
            raise ContextualSealError("contextual seal plan is not canonical JSON plus newline")
        _validate_plan_shape(plan)
        _canonical_utc_timestamp(plan.get("created_at"), "plan created_at")
        if (
            plan.get("kind") != PLAN_KIND
            or plan.get("schema_version") != SCHEMA_VERSION
            or plan.get("state") != "prepared_not_applied"
            or plan.get("authority") != AUTHORITY
            or plan.get("policy") != POLICY
        ):
            raise ContextualSealError("contextual seal plan contract is unsupported")
        semantic = {k: v for k, v in plan.items() if k not in {"identity_sha256", "plan_id", "receipt_path"}}
        identity = _document_identity(semantic, set())
        if plan.get("identity_sha256") != identity:
            raise ContextualSealError("contextual seal plan semantic identity differs")
        if plan.get("plan_id") != f"ctxasrsealplan_{identity[:32]}":
            raise ContextualSealError("contextual seal plan ID differs")
        planned_root = plan.get("store_root")
        source_root = plan["source"].get("asr_output_root")
        if (
            not isinstance(planned_root, str)
            or not isinstance(source_root, str)
            or planned_root != source_root
        ):
            raise ContextualSealError(
                "planned store root differs from the manifest-bound ASR output root"
            )
        root = _require_absolute_resolved(
            Path(planned_root), "planned contextual ASR output root"
        )
        if _mode(root.lstat()) != STORE_ROOT_MODE:
            raise ContextualSealError(
                "planned contextual ASR output root must remain mode 0700"
            )
        control = _control_root(root, plan["source"]["batch_id"])
        for directory, label in (
            (control, "contextual seal control root"),
            (control / "plans", "contextual seal plan directory"),
            (control / "receipts", "contextual seal receipt directory"),
        ):
            _require_absolute_resolved(directory, label)
            if not directory.is_dir() or _mode(directory.lstat()) != CONTROL_DIRECTORY_MODE:
                raise ContextualSealError(f"{label} must remain a real mode-0700 directory")
        _retain_apply_lock(stack, control, acquire=False)
        expected_path = control / "plans" / f"{plan['plan_id']}.json"
        expected_receipt = control / "receipts" / f"ctxasrsealreceipt_{identity[:32]}.json"
        _require_beneath(expected_path, control, "contextual seal plan path")
        _require_beneath(expected_receipt, control, "contextual seal receipt path")
        if path != expected_path or plan.get("receipt_path") != str(expected_receipt):
            raise ContextualSealError("plan or receipt path differs from its closed control layout")
        return plan, retained, stack
    except Exception:
        stack.close()
        raise


def _verify_implementation(plan: dict[str, Any]) -> None:
    if _file_identity(Path(__file__).resolve()) != plan["implementation"]:
        raise ContextualSealError("contextual sealing implementation differs from the plan")
    if _file_identity(IMPORTER_SOURCE.resolve()) != plan["importer_validator"]:
        raise ContextualSealError("catalog-free validator implementation differs from the plan")


def _compare_source(expected: dict[str, Any], observed: RetainedSource) -> None:
    if observed.record != expected:
        raise ContextualSealError("sealed contextual source differs from the plan")


def _open_planned_results(
    stack: ExitStack,
    plan: dict[str, Any],
    source: RetainedSource,
    validator: CatalogValidator,
    *,
    allow_mixed_modes: bool,
) -> list[RetainedResult]:
    file_modes = set(RESULT_FILE_MODES_BEFORE) | {RESULT_FILE_MODE_AFTER}
    directory_modes = {RESULT_DIRECTORY_MODE_BEFORE, RESULT_DIRECTORY_MODE_AFTER}
    results = _open_exact_results(
        stack,
        [Path(item["result_path"]) for item in plan["results"]],
        source,
        validator,
        allowed_file_modes=file_modes if allow_mixed_modes else RESULT_FILE_MODES_BEFORE,
        allowed_directory_modes=directory_modes if allow_mixed_modes else {RESULT_DIRECTORY_MODE_BEFORE},
    )
    for retained, expected in zip(results, plan["results"], strict=True):
        stable_identity_keys = (
            "input_sha256",
            "job_id",
            "ordinal",
            "recipe_id",
            "result_canonical_sha256",
            "result_key",
            "result_path",
            "source_ordinal",
            "work_order_physical_sha256",
            "work_order_sha256",
        )
        if any(retained.record[key] != expected[key] for key in stable_identity_keys):
            raise ContextualSealError(
                "stable contextual result identity differs from the plan"
            )
        if retained.record["adapter_validation"] != expected["adapter_validation"]:
            raise ContextualSealError("adapter validation identity differs from the plan")
        if retained.record["catalog_free_validation"] != expected["catalog_free_validation"]:
            raise ContextualSealError("catalog-free validation identity differs from the plan")
        if retained.record["result_canonical_sha256"] != expected["result_canonical_sha256"]:
            raise ContextualSealError("result canonical digest differs from the plan")
        source_order = source.record["work_orders"][expected["source_ordinal"] - 1]
        adapter = expected["adapter_validation"]
        catalog = expected["catalog_free_validation"]
        if (
            expected["ordinal"] != expected["source_ordinal"]
            or expected["input_sha256"] != source_order["input_sha256"]
            or expected["job_id"] != source_order["job_id"]
            or expected["work_order_physical_sha256"] != source_order["sha256"]
            or expected["work_order_sha256"] != source_order["canonical_sha256"]
            or adapter["work_order_sha256"] != expected["work_order_sha256"]
            or adapter["recipe_id"] != expected["recipe_id"]
            or adapter["result_key"] != expected["result_key"]
            or adapter["result_path"] != expected["result_path"]
            or adapter["glossary_revision_id"]
            != source.record["glossary_revision_id"]
            or catalog["job_id"] != expected["job_id"]
            or catalog["recipe_id"] != expected["recipe_id"]
            or catalog["result_key"] != expected["result_key"]
            or catalog["result_envelope_sha256"]
            != expected["result_canonical_sha256"]
            or catalog["glossary_revision_id"]
            != source.record["glossary_revision_id"]
            or catalog["processing_run_id"] != adapter["processing_run_id"]
        ):
            raise ContextualSealError(
                "contextual result/source/validator cross-link differs from the plan"
            )
        directory = retained.directory.verify(
            modes=(
                directory_modes
                if allow_mixed_modes
                else {expected["directory"]["mode_before"]}
            ),
            entries=set(RESULT_FILENAMES),
            expected_device=expected["directory"]["device"],
            expected_inode=expected["directory"]["inode"],
            expected_mtime_ns=expected["directory"]["mtime_ns"],
        )
        if _mode(directory) == expected["directory"]["mode_before"] and directory.st_ctime_ns != expected["directory"]["ctime_ns_before"]:
            raise ContextualSealError("result directory ctime changed before sealing")
        for file_expected in expected["files"]:
            item = retained.files[file_expected["name"]]
            descriptor = item.verify(
                modes=(
                    file_modes
                    if allow_mixed_modes
                    else {file_expected["mode_before"]}
                ),
                expected_sha256=file_expected["sha256"],
                expected_device=file_expected["device"],
                expected_inode=file_expected["inode"],
                expected_mtime_ns=file_expected["mtime_ns"],
            )
            if descriptor.st_size != file_expected["byte_count"]:
                raise ContextualSealError("result file byte count differs from the plan")
            if _mode(descriptor) == file_expected["mode_before"] and descriptor.st_ctime_ns != file_expected["ctime_ns_before"]:
                raise ContextualSealError("result file ctime changed before sealing")
    return results


def validate_plan(
    plan_path: Path, *, validator: CatalogValidator = catalog_free_validate
) -> dict[str, Any]:
    plan, retained_plan, stack = _read_plan(plan_path)
    with stack:
        _verify_implementation(plan)
        source = _open_source(stack, Path(plan["source"]["manifest_path"]))
        _compare_source(plan["source"], source)
        results = _open_planned_results(
            stack, plan, source, validator, allow_mixed_modes=False
        )
        retained_plan.verify(modes={PLAN_FILE_MODE})
        _verify_source_retained(source)
        _verify_ancestors(results)
        return {
            "batch_id": source.record["batch_id"],
            "plan_id": plan["plan_id"],
            "result_count": len(results),
            "state": "valid_prepared_plan",
        }


def _mode_state(plan: dict[str, Any], results: list[RetainedResult]) -> str:
    states: list[str] = []
    for retained, expected in zip(results, plan["results"], strict=True):
        directory_mode = _mode(os.fstat(retained.directory.fd))
        if directory_mode == expected["directory"]["mode_before"]:
            states.append("before")
        elif directory_mode == expected["directory"]["mode_after"]:
            states.append("after")
        else:
            states.append("invalid")
        for file_expected in expected["files"]:
            current = _mode(os.fstat(retained.files[file_expected["name"]].fd))
            if current == file_expected["mode_before"]:
                states.append("before")
            elif current == file_expected["mode_after"]:
                states.append("after")
            else:
                # The other globally permitted pre-seal mode is still invalid for
                # this exact plan entry.  It must never be mistaken for an after
                # state merely because it differs from the recorded before mode.
                states.append("invalid")
    unique = set(states)
    if unique == {"before"}:
        return "exact_all_before"
    if unique == {"after"}:
        return "exact_all_after"
    return "mixed_or_invalid"


def _rollback_modes(
    plan: dict[str, Any], results: list[RetainedResult]
) -> list[str]:
    failures: list[str] = []
    for retained, expected in reversed(list(zip(results, plan["results"], strict=True))):
        try:
            if _mode(os.fstat(retained.directory.fd)) != expected["directory"]["mode_before"]:
                os.fchmod(retained.directory.fd, expected["directory"]["mode_before"])
                os.fsync(retained.directory.fd)
        except OSError as error:
            failures.append(f"{retained.directory.path}: {error}")
        for file_expected in reversed(expected["files"]):
            item = retained.files[file_expected["name"]]
            try:
                if _mode(os.fstat(item.fd)) != file_expected["mode_before"]:
                    os.fchmod(item.fd, file_expected["mode_before"])
                    os.fsync(item.fd)
            except OSError as error:
                failures.append(f"{item.path}: {error}")
    return failures


def _receipt_document(
    plan_path: Path,
    plan_body: bytes,
    plan: dict[str, Any],
    results: list[RetainedResult],
    *,
    applied_at: str,
) -> dict[str, Any]:
    applied_at = _canonical_utc_timestamp(applied_at, "receipt applied_at")
    receipt_results: list[dict[str, Any]] = []
    for retained, expected in zip(results, plan["results"], strict=True):
        directory = os.fstat(retained.directory.fd)
        files: list[dict[str, Any]] = []
        for file_expected in expected["files"]:
            item = retained.files[file_expected["name"]]
            value = os.fstat(item.fd)
            files.append(
                {
                    "byte_count": value.st_size,
                    "content_unchanged": True,
                    "ctime_ns_after": value.st_ctime_ns,
                    "device": value.st_dev,
                    "inode": value.st_ino,
                    "mode_after": _mode(value),
                    "mtime_ns": value.st_mtime_ns,
                    "mtime_unchanged": value.st_mtime_ns == file_expected["mtime_ns"],
                    "name": file_expected["name"],
                    "nlink": value.st_nlink,
                    "path": file_expected["path"],
                    "sha256": file_expected["sha256"],
                }
            )
        receipt_results.append(
            {
                "directory": {
                    "content_entries_unchanged": True,
                    "ctime_ns_after": directory.st_ctime_ns,
                    "device": directory.st_dev,
                    "inode": directory.st_ino,
                    "mode_after": _mode(directory),
                    "mtime_ns": directory.st_mtime_ns,
                    "mtime_unchanged": directory.st_mtime_ns == expected["directory"]["mtime_ns"],
                    "nlink": directory.st_nlink,
                    "path": expected["directory"]["path"],
                },
                "job_id": expected["job_id"],
                "ordinal": expected["ordinal"],
                "result_key": expected["result_key"],
                "result_path": expected["result_path"],
                "files": files,
                "source_ordinal": expected["source_ordinal"],
                "work_order_sha256": expected["work_order_sha256"],
            }
        )
    receipt: dict[str, Any] = {
        "applied_at": applied_at,
        "authority": AUTHORITY,
        "batch_id": plan["source"]["batch_id"],
        "kind": RECEIPT_KIND,
        "plan": {
            "byte_count": len(plan_body),
            "identity_sha256": plan["identity_sha256"],
            "path": str(plan_path),
            "plan_id": plan["plan_id"],
            "sha256": sha256_bytes(plan_body),
        },
        "policy": POLICY,
        "result_count": len(receipt_results),
        "results": receipt_results,
        "schema_version": SCHEMA_VERSION,
        "state": "applied_content_and_mtime_preserved",
    }
    identity = _document_identity(receipt, set())
    receipt["identity_sha256"] = identity
    receipt["receipt_id"] = f"ctxasrsealreceipt_{plan['identity_sha256'][:32]}"
    return receipt


def apply_plan(
    plan_path: Path,
    *,
    validator: CatalogValidator = catalog_free_validate,
    applied_at: str | None = None,
) -> Path:
    plan, retained_plan, stack = _read_plan(plan_path)
    receipt_path = Path(plan["receipt_path"])
    with stack:
        control = _control_root(
            Path(plan["store_root"]), plan["source"]["batch_id"]
        )
        # A retained, single-link flock serializes every cooperating apply from
        # receipt existence inspection through transition and durable receipt
        # commit.  Validation-only commands never take this exclusive lock.
        _retain_apply_lock(stack, control, acquire=True)
        if receipt_path.exists() or receipt_path.is_symlink():
            validate_receipt(receipt_path, validator=validator)
            return receipt_path
        _verify_implementation(plan)
        source = _open_source(stack, Path(plan["source"]["manifest_path"]))
        _compare_source(plan["source"], source)
        results = _open_planned_results(
            stack, plan, source, validator, allow_mixed_modes=True
        )
        state = _mode_state(plan, results)
        if state not in {"exact_all_before", "exact_all_after"}:
            failures = _rollback_modes(plan, results)
            suffix = f"; rollback failures: {failures}" if failures else "; recorded modes restored"
            raise ContextualSealError(
                "partial or invalid contextual seal mode state detected" + suffix
            )
        transitioned = state == "exact_all_before"
        receipt_committed = False
        try:
            if transitioned:
                for retained in results:
                    for name in RESULT_FILENAMES:
                        os.fchmod(retained.files[name].fd, RESULT_FILE_MODE_AFTER)
                        os.fsync(retained.files[name].fd)
                    os.fchmod(retained.directory.fd, RESULT_DIRECTORY_MODE_AFTER)
                    os.fsync(retained.directory.fd)
            for retained, expected in zip(results, plan["results"], strict=True):
                for file_expected in expected["files"]:
                    retained.files[file_expected["name"]].verify(
                        modes={RESULT_FILE_MODE_AFTER},
                        expected_sha256=file_expected["sha256"],
                        expected_device=file_expected["device"],
                        expected_inode=file_expected["inode"],
                        expected_mtime_ns=file_expected["mtime_ns"],
                    )
                retained.directory.verify(
                    modes={RESULT_DIRECTORY_MODE_AFTER},
                    entries=set(RESULT_FILENAMES),
                    expected_device=expected["directory"]["device"],
                    expected_inode=expected["directory"]["inode"],
                    expected_mtime_ns=expected["directory"]["mtime_ns"],
                )
                observed = validator(Path(expected["result_path"]))
                if _catalog_summary(observed) != expected["catalog_free_validation"]:
                    raise ContextualSealError("post-transition catalog validation differs")
            _verify_source_retained(source)
            _verify_ancestors(results)
            retained_plan.verify(modes={PLAN_FILE_MODE})
            receipt = _receipt_document(
                plan_path,
                retained_plan.body,
                plan,
                results,
                applied_at=applied_at or utc_now(),
            )
            _atomic_control_json(receipt_path, receipt)
            receipt_committed = True
            return receipt_path
        except Exception as error:
            if receipt_committed:
                # A committed receipt is the terminal point.  Atomic creation either
                # succeeded completely or did not occur; no rollback is attempted
                # after that durable fact exists.
                raise
            if receipt_path.exists() or receipt_path.is_symlink():
                try:
                    validate_receipt(receipt_path, validator=validator)
                except Exception:
                    # An invalid or unrelated path has no authority.  The exact
                    # recorded before modes are restored below when this call made
                    # the transition.
                    pass
                else:
                    # A non-cooperating writer may still win the no-replace link.
                    # A fully valid exact receipt is terminal evidence for these
                    # currently sealed bytes; never roll its targets back.
                    return receipt_path
            failures = _rollback_modes(plan, results) if transitioned else []
            if failures:
                raise ContextualSealError(
                    f"contextual seal failed ({error}); rollback failures: {failures}"
                ) from error
            raise ContextualSealError(
                f"contextual seal failed and recorded modes were restored: {error}"
            ) from error


RECEIPT_KEYS = {
    "applied_at",
    "authority",
    "batch_id",
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
PLAN_REFERENCE_KEYS = {"byte_count", "identity_sha256", "path", "plan_id", "sha256"}
RECEIPT_RESULT_KEYS = {
    "directory",
    "files",
    "job_id",
    "ordinal",
    "result_key",
    "result_path",
    "source_ordinal",
    "work_order_sha256",
}
AFTER_DIRECTORY_KEYS = {
    "content_entries_unchanged",
    "ctime_ns_after",
    "device",
    "inode",
    "mode_after",
    "mtime_ns",
    "mtime_unchanged",
    "nlink",
    "path",
}
AFTER_FILE_KEYS = {
    "byte_count",
    "content_unchanged",
    "ctime_ns_after",
    "device",
    "inode",
    "mode_after",
    "mtime_ns",
    "mtime_unchanged",
    "name",
    "nlink",
    "path",
    "sha256",
}


def _validate_receipt_shape(receipt: dict[str, Any]) -> None:
    _exact_keys(receipt, RECEIPT_KEYS, "contextual seal receipt")
    _exact_keys(receipt["plan"], PLAN_REFERENCE_KEYS, "receipt plan reference")
    results = receipt.get("results")
    if not isinstance(results, list) or not results:
        raise ContextualSealError("receipt results must be non-empty")
    for ordinal, result in enumerate(results, 1):
        _exact_keys(result, RECEIPT_RESULT_KEYS, f"receipt result {ordinal}")
        if result.get("ordinal") != ordinal or result.get("source_ordinal") != ordinal:
            raise ContextualSealError("receipt result ordinals are not contiguous")
        _exact_keys(result["directory"], AFTER_DIRECTORY_KEYS, "receipt directory")
        files = result.get("files")
        if not isinstance(files, list) or [item.get("name") for item in files] != list(RESULT_FILENAMES):
            raise ContextualSealError("receipt files are not the exact ordered allowlist")
        for item in files:
            _exact_keys(item, AFTER_FILE_KEYS, "receipt file")


def validate_receipt(
    receipt_path: Path, *, validator: CatalogValidator = catalog_free_validate
) -> dict[str, Any]:
    with ExitStack() as receipt_stack:
        receipt_path = _require_absolute_resolved(receipt_path, "contextual seal receipt")
        retained_receipt = _open_file(
            receipt_stack,
            receipt_path,
            "contextual seal receipt",
            maximum=MAX_CONTROL_BYTES,
            modes={PLAN_FILE_MODE},
        )
        receipt = parse_json(retained_receipt.body, "contextual seal receipt")
        if retained_receipt.body != canonical_bytes(receipt) + b"\n":
            raise ContextualSealError("receipt is not canonical JSON plus newline")
        _validate_receipt_shape(receipt)
        _canonical_utc_timestamp(receipt.get("applied_at"), "receipt applied_at")
        if (
            receipt.get("kind") != RECEIPT_KIND
            or receipt.get("schema_version") != SCHEMA_VERSION
            or receipt.get("state") != "applied_content_and_mtime_preserved"
            or receipt.get("authority") != AUTHORITY
            or receipt.get("policy") != POLICY
        ):
            raise ContextualSealError("receipt contract is unsupported")
        identity = _document_identity(
            {k: v for k, v in receipt.items() if k not in {"identity_sha256", "receipt_id"}},
            set(),
        )
        if receipt.get("identity_sha256") != identity:
            raise ContextualSealError("receipt semantic identity differs")
        plan_reference = receipt["plan"]
        plan_path = Path(plan_reference["path"])
        plan, retained_plan, plan_stack = _read_plan(plan_path)
        with plan_stack:
            if (
                plan_reference["identity_sha256"] != plan["identity_sha256"]
                or plan_reference["plan_id"] != plan["plan_id"]
                or plan_reference["byte_count"] != len(retained_plan.body)
                or plan_reference["sha256"] != sha256_bytes(retained_plan.body)
                or receipt_path != Path(plan["receipt_path"])
                or receipt.get("receipt_id") != f"ctxasrsealreceipt_{plan['identity_sha256'][:32]}"
                or receipt.get("batch_id") != plan["source"]["batch_id"]
            ):
                raise ContextualSealError("receipt plan/batch/path binding differs")
            _verify_implementation(plan)
            source = _open_source(plan_stack, Path(plan["source"]["manifest_path"]))
            _compare_source(plan["source"], source)
            results = _open_planned_results(
                plan_stack, plan, source, validator, allow_mixed_modes=True
            )
            if _mode_state(plan, results) != "exact_all_after":
                raise ContextualSealError("receipt targets are not fully sealed")
            if receipt.get("result_count") != len(results) or len(receipt["results"]) != len(results):
                raise ContextualSealError("receipt result count differs")
            for retained, expected, after in zip(
                results, plan["results"], receipt["results"], strict=True
            ):
                directory = retained.directory.verify(
                    modes={RESULT_DIRECTORY_MODE_AFTER},
                    entries=set(RESULT_FILENAMES),
                    expected_device=after["directory"]["device"],
                    expected_inode=after["directory"]["inode"],
                    expected_mtime_ns=after["directory"]["mtime_ns"],
                    expected_ctime_ns=after["directory"]["ctime_ns_after"],
                )
                if (
                    after["directory"]["path"] != expected["directory"]["path"]
                    or after["directory"]["mode_after"] != RESULT_DIRECTORY_MODE_AFTER
                    or after["directory"]["nlink"] != directory.st_nlink
                    or after["directory"]["content_entries_unchanged"] is not True
                    or after["directory"]["mtime_unchanged"] is not True
                ):
                    raise ContextualSealError("receipt directory evidence differs")
                if any(after.get(key) != expected.get(key) for key in ("job_id", "ordinal", "result_key", "result_path", "source_ordinal", "work_order_sha256")):
                    raise ContextualSealError("receipt result identity differs from the plan")
                for file_expected, file_after in zip(
                    expected["files"], after["files"], strict=True
                ):
                    value = retained.files[file_expected["name"]].verify(
                        modes={RESULT_FILE_MODE_AFTER},
                        expected_sha256=file_after["sha256"],
                        expected_device=file_after["device"],
                        expected_inode=file_after["inode"],
                        expected_mtime_ns=file_after["mtime_ns"],
                        expected_ctime_ns=file_after["ctime_ns_after"],
                    )
                    if (
                        file_after["path"] != file_expected["path"]
                        or file_after["name"] != file_expected["name"]
                        or file_after["sha256"] != file_expected["sha256"]
                        or file_after["byte_count"] != value.st_size
                        or file_after["mode_after"] != RESULT_FILE_MODE_AFTER
                        or file_after["nlink"] != 1
                        or file_after["content_unchanged"] is not True
                        or file_after["mtime_unchanged"] is not True
                    ):
                        raise ContextualSealError("receipt file evidence differs")
            retained_receipt.verify(modes={PLAN_FILE_MODE})
            retained_plan.verify(modes={PLAN_FILE_MODE})
            _verify_source_retained(source)
            _verify_ancestors(results)
            return {
                "batch_id": receipt["batch_id"],
                "receipt_id": receipt["receipt_id"],
                "result_count": len(results),
                "state": "valid_applied_receipt",
            }


def _absolute_cli_path(value: str, label: str, *, existing: bool = True) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = Path.cwd() / path
    path = Path(os.path.abspath(path))
    if existing:
        return _require_absolute_resolved(path, label)
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan", help="validate exact inputs and create a seal plan")
    plan.add_argument("--manifest", required=True)
    plan.add_argument("--result", action="append", required=True)
    plan.add_argument("--dry-run", action="store_true")
    validate_plan_parser = subparsers.add_parser("validate-plan")
    validate_plan_parser.add_argument("--plan", required=True)
    apply = subparsers.add_parser("apply")
    apply.add_argument("--plan", required=True)
    validate_receipt_parser = subparsers.add_parser("validate-receipt")
    validate_receipt_parser.add_argument("--receipt", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "plan":
            outcome = build_plan(
                manifest_path=_absolute_cli_path(args.manifest, "contextual manifest"),
                result_paths=[
                    _absolute_cli_path(item, "contextual result") for item in args.result
                ],
                dry_run=args.dry_run,
            )
            payload = outcome if isinstance(outcome, dict) else {
                "plan_path": str(outcome),
                "state": "prepared_not_applied",
            }
        elif args.command == "validate-plan":
            payload = validate_plan(_absolute_cli_path(args.plan, "contextual seal plan"))
        elif args.command == "apply":
            receipt = apply_plan(_absolute_cli_path(args.plan, "contextual seal plan"))
            payload = {"receipt_path": str(receipt), "state": "applied_or_exact_replay"}
        else:
            payload = validate_receipt(
                _absolute_cli_path(args.receipt, "contextual seal receipt")
            )
    except ContextualSealError as error:
        print(str(error), file=sys.stderr)
        return 1
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
