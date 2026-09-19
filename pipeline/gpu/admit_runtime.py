#!/usr/bin/env python3
"""Admit or offline-replay one exact private production GPU runtime.

This utility never runs model inference and never downloads anything.  ``admit``
hashes an already materialized wheelhouse and runtime, inventories installed
package metadata, binds executable/source bytes, checks static CUDA/NVML device
identity, evaluates an independently frozen benchmark against explicit limits,
validates independently produced UUID-keyed scheduler-lock evidence, and writes
one owner-private receipt without replacement.  ``validate`` repeats those checks
from the receipt and rejects any drift.

The benchmark and scheduler documents are identity-bearing JSON objects.  Their
``identity_sha256`` is SHA-256 over canonical JSON (sorted keys, compact
separators, one trailing newline) of every member except ``identity_sha256``.
Use ``--help`` on ``admit`` for the complete input contract.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import secrets
import stat
import sys
import tomllib
import urllib.parse
import zipfile
from datetime import datetime, timezone
from email.parser import BytesParser
from email.policy import default as email_policy
from importlib import metadata as importlib_metadata
from pathlib import Path, PurePosixPath
from typing import Any, Iterable


KIND = "himr_gpu_runtime_admission_receipt"
SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
BENCHMARK_KIND = "himr_gpu_frozen_benchmark_result"
SCHEDULER_KIND = "himr_gpu_scheduler_lock_evidence"
SCHEDULER_TEST_KIND = "himr_gpu_scheduler_lock_test_result"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$")
GPU_UUID_RE = re.compile(r"^GPU-[0-9a-fA-F-]{16,}$")
METRIC_RE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_TREE_FILES = 250_000
MAX_TREE_BYTES = 16 * 1024**3
MAX_SINGLE_FILE_BYTES = 4 * 1024**3


class RuntimeAdmissionError(RuntimeError):
    """Raised when exact production-runtime admission cannot be proved."""


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def pretty_bytes(value: Any) -> bytes:
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


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise RuntimeAdmissionError(f"duplicate JSON member: {key}")
        result[key] = value
    return result


def reject_constant(value: str) -> Any:
    raise RuntimeAdmissionError(f"non-finite JSON number is forbidden: {value}")


def load_json_bytes(body: bytes, label: str) -> Any:
    if len(body) > MAX_JSON_BYTES:
        raise RuntimeAdmissionError(f"{label} exceeds {MAX_JSON_BYTES} bytes")
    try:
        return json.loads(
            body,
            object_pairs_hook=strict_object,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeAdmissionError(f"{label} is not strict UTF-8 JSON: {error}") from error


def strict_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise RuntimeAdmissionError(f"{label} must be a lowercase SHA-256 digest")
    return value


def strict_name(value: Any, label: str) -> str:
    if not isinstance(value, str) or not NAME_RE.fullmatch(value):
        raise RuntimeAdmissionError(f"{label} is not a safe name")
    return value


def strict_string(value: Any, label: str, maximum: int = 1000) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum or "\x00" in value:
        raise RuntimeAdmissionError(f"{label} is invalid")
    return value


def strict_int(value: Any, label: str, minimum: int = 0, maximum: int = 2**63 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise RuntimeAdmissionError(f"{label} must be an integer in [{minimum}, {maximum}]")
    return value


def finite_number(
    value: Any,
    label: str,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuntimeAdmissionError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise RuntimeAdmissionError(f"{label} must be finite")
    if minimum is not None and result < minimum:
        raise RuntimeAdmissionError(f"{label} must be at least {minimum}")
    if maximum is not None and result > maximum:
        raise RuntimeAdmissionError(f"{label} must be at most {maximum}")
    return result


def parse_utc(value: Any, label: str) -> str:
    text = strict_string(value, label, 64)
    if not text.endswith("Z"):
        raise RuntimeAdmissionError(f"{label} must be UTC with a trailing Z")
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as error:
        raise RuntimeAdmissionError(f"{label} is not an ISO-8601 timestamp") from error
    if parsed.tzinfo != timezone.utc:
        raise RuntimeAdmissionError(f"{label} must be UTC")
    return text


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalized_distribution_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def is_within(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def absolute_path(value: str | Path, label: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise RuntimeAdmissionError(f"{label} must be absolute")
    return path


def stable_file_bytes(path: Path, label: str, maximum: int = MAX_SINGLE_FILE_BYTES) -> tuple[bytes, os.stat_result]:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except OSError as error:
        raise RuntimeAdmissionError(f"cannot open {label} without following symlinks: {error}") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RuntimeAdmissionError(f"{label} is not a regular file")
        if before.st_size > maximum:
            raise RuntimeAdmissionError(f"{label} exceeds the {maximum}-byte safety bound")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise RuntimeAdmissionError(f"{label} ended before its declared size")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise RuntimeAdmissionError(f"{label} grew while it was read")
        after = os.fstat(descriptor)
        stable_fields = ("st_dev", "st_ino", "st_mode", "st_nlink", "st_uid", "st_size", "st_mtime_ns")
        if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
            raise RuntimeAdmissionError(f"{label} changed while it was read")
        return b"".join(chunks), after
    finally:
        os.close(descriptor)


def stable_file_digest(
    path: Path,
    label: str,
    maximum: int = MAX_SINGLE_FILE_BYTES,
) -> tuple[str, os.stat_result]:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except OSError as error:
        raise RuntimeAdmissionError(f"cannot open {label} without following symlinks: {error}") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RuntimeAdmissionError(f"{label} is not a regular file")
        if before.st_size > maximum:
            raise RuntimeAdmissionError(f"{label} exceeds the {maximum}-byte safety bound")
        digest = hashlib.sha256()
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise RuntimeAdmissionError(f"{label} ended before its declared size")
            digest.update(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise RuntimeAdmissionError(f"{label} grew while it was hashed")
        after = os.fstat(descriptor)
        stable_fields = ("st_dev", "st_ino", "st_mode", "st_nlink", "st_uid", "st_size", "st_mtime_ns")
        if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
            raise RuntimeAdmissionError(f"{label} changed while it was hashed")
        return digest.hexdigest(), after
    finally:
        os.close(descriptor)


def file_binding(
    requested: str | Path,
    label: str,
    expected_sha256: str | None = None,
    *,
    executable: bool = False,
    expected_device: int | None = None,
) -> dict[str, Any]:
    requested_path = absolute_path(requested, label)
    try:
        requested_metadata = requested_path.lstat()
        resolved = requested_path.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise RuntimeAdmissionError(f"cannot resolve {label}: {error}") from error
    requested_symlink = stat.S_ISLNK(requested_metadata.st_mode)
    if not requested_symlink and requested_path != resolved:
        raise RuntimeAdmissionError(f"{label} has an unexpected non-canonical path")
    digest, metadata = stable_file_digest(resolved, label)
    if metadata.st_uid != os.getuid():
        raise RuntimeAdmissionError(f"{label} is not owned by the current user")
    mode = stat.S_IMODE(metadata.st_mode)
    if mode & 0o022:
        raise RuntimeAdmissionError(f"{label} is group/other writable")
    if executable and not mode & 0o111:
        raise RuntimeAdmissionError(f"{label} is not executable")
    if expected_device is not None and metadata.st_dev != expected_device:
        raise RuntimeAdmissionError(f"{label} is not on the expected main-drive device")
    if expected_sha256 is not None and digest != strict_sha256(expected_sha256, f"expected {label} SHA-256"):
        raise RuntimeAdmissionError(f"{label} SHA-256 differs")
    return {
        "requested_path": str(requested_path),
        "requested_symlink": requested_symlink,
        "requested_symlink_target": os.readlink(requested_path) if requested_symlink else None,
        "resolved_path": str(resolved),
        "sha256": digest,
        "byte_count": metadata.st_size,
        "mode": mode,
        "uid": metadata.st_uid,
        "device": metadata.st_dev,
        "link_count": metadata.st_nlink,
        "executable": bool(mode & 0o111),
    }


def directory_binding(root: Path, label: str, expected_device: int) -> dict[str, Any]:
    if not root.is_absolute() or root.resolve() != root:
        raise RuntimeAdmissionError(f"{label} must be an absolute non-symlinked directory")
    try:
        metadata = root.lstat()
    except OSError as error:
        raise RuntimeAdmissionError(f"cannot inspect {label}: {error}") from error
    if not stat.S_ISDIR(metadata.st_mode):
        raise RuntimeAdmissionError(f"{label} is not a directory")
    mode = stat.S_IMODE(metadata.st_mode)
    if metadata.st_uid != os.getuid() or mode & 0o022:
        raise RuntimeAdmissionError(f"{label} must be owner-controlled and not group/other writable")
    if metadata.st_dev != expected_device:
        raise RuntimeAdmissionError(f"{label} is not on the expected main-drive device")
    return {
        "path": str(root),
        "mode": mode,
        "uid": metadata.st_uid,
        "device": metadata.st_dev,
    }


def symlink_tree_row(path: Path, root: Path, label: str, expected_device: int) -> tuple[dict[str, Any], int]:
    metadata = path.lstat()
    if metadata.st_uid != os.getuid() or metadata.st_dev != expected_device:
        raise RuntimeAdmissionError(f"{label} symlink is not owner-controlled on the expected device: {path}")
    try:
        target = os.readlink(path)
        resolved = path.resolve(strict=True)
        resolved_metadata = resolved.stat()
    except (OSError, RuntimeError) as error:
        raise RuntimeAdmissionError(f"{label} contains an unsafe symlink {path}: {error}") from error
    resolved_kind: str
    target_sha256: str | None = None
    target_byte_count: int | None = None
    hashed_bytes = 0
    if stat.S_ISDIR(resolved_metadata.st_mode):
        if not is_within(resolved, root):
            raise RuntimeAdmissionError(f"{label} contains a directory symlink outside its root: {path}")
        resolved_kind = "directory"
    elif stat.S_ISREG(resolved_metadata.st_mode):
        target_sha256, stable_metadata = stable_file_digest(resolved, f"{label} symlink target")
        if stable_metadata.st_uid != os.getuid() or stable_metadata.st_dev != expected_device:
            raise RuntimeAdmissionError(f"{label} symlink target is not owner-controlled on the expected device")
        if stat.S_IMODE(stable_metadata.st_mode) & 0o022:
            raise RuntimeAdmissionError(f"{label} symlink target is group/other writable")
        resolved_kind = "regular_file"
        target_byte_count = stable_metadata.st_size
        hashed_bytes = stable_metadata.st_size
    else:
        raise RuntimeAdmissionError(f"{label} symlink does not resolve to a regular file or directory: {path}")
    return (
        {
            "path": path.relative_to(root).as_posix(),
            "kind": "symlink",
            "target": target,
            "resolved_path": str(resolved),
            "resolved_kind": resolved_kind,
            "target_sha256": target_sha256,
            "target_byte_count": target_byte_count,
            "uid": metadata.st_uid,
            "device": metadata.st_dev,
        },
        hashed_bytes,
    )


def tree_snapshot(
    root_value: str | Path,
    label: str,
    expected_device: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    root = absolute_path(root_value, label)
    root_record = directory_binding(root, label, expected_device)
    rows: list[dict[str, Any]] = []
    internal_bytes = 0
    hashed_bytes = 0
    regular_count = 0
    directory_count = 1
    symlink_count = 0
    for current, directory_names, file_names in os.walk(root, topdown=True, followlinks=False):
        directory_path = Path(current)
        current_metadata = directory_path.lstat()
        if current_metadata.st_uid != os.getuid() or current_metadata.st_dev != expected_device:
            raise RuntimeAdmissionError(f"{label} directory is not owner-controlled on the expected device")
        if stat.S_IMODE(current_metadata.st_mode) & 0o022:
            raise RuntimeAdmissionError(f"{label} directory is group/other writable: {directory_path}")
        directory_names.sort()
        file_names.sort()
        retained_directories: list[str] = []
        for name in directory_names:
            path = directory_path / name
            metadata = path.lstat()
            if stat.S_ISLNK(metadata.st_mode):
                row, external_bytes = symlink_tree_row(path, root, label, expected_device)
                rows.append(row)
                symlink_count += 1
                hashed_bytes += external_bytes
            elif stat.S_ISDIR(metadata.st_mode):
                mode = stat.S_IMODE(metadata.st_mode)
                if metadata.st_uid != os.getuid() or metadata.st_dev != expected_device or mode & 0o022:
                    raise RuntimeAdmissionError(f"{label} contains an unsafe directory: {path}")
                rows.append(
                    {
                        "path": path.relative_to(root).as_posix(),
                        "kind": "directory",
                        "mode": mode,
                        "uid": metadata.st_uid,
                        "device": metadata.st_dev,
                    }
                )
                directory_count += 1
                retained_directories.append(name)
            else:
                raise RuntimeAdmissionError(f"{label} contains a non-directory tree entry: {path}")
        directory_names[:] = retained_directories
        for name in file_names:
            path = directory_path / name
            metadata = path.lstat()
            if stat.S_ISLNK(metadata.st_mode):
                row, external_bytes = symlink_tree_row(path, root, label, expected_device)
                rows.append(row)
                symlink_count += 1
                hashed_bytes += external_bytes
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise RuntimeAdmissionError(f"{label} contains a non-regular file: {path}")
            digest, stable_metadata = stable_file_digest(path, f"{label} file")
            mode = stat.S_IMODE(stable_metadata.st_mode)
            if stable_metadata.st_uid != os.getuid() or stable_metadata.st_dev != expected_device or mode & 0o022:
                raise RuntimeAdmissionError(f"{label} contains an unsafe regular file: {path}")
            internal_bytes += stable_metadata.st_size
            hashed_bytes += stable_metadata.st_size
            regular_count += 1
            rows.append(
                {
                    "path": path.relative_to(root).as_posix(),
                    "kind": "regular_file",
                    "sha256": digest,
                    "byte_count": stable_metadata.st_size,
                    "mode": mode,
                    "uid": stable_metadata.st_uid,
                    "device": stable_metadata.st_dev,
                    "link_count": stable_metadata.st_nlink,
                }
            )
            if len(rows) > MAX_TREE_FILES or hashed_bytes > MAX_TREE_BYTES:
                raise RuntimeAdmissionError(f"{label} exceeds tree safety bounds")
    rows.sort(key=lambda item: item["path"])
    if not rows or regular_count == 0:
        raise RuntimeAdmissionError(f"{label} is empty")
    summary = {
        "root": root_record,
        "tree_sha256": sha256_bytes(canonical_bytes(rows)),
        "entry_count": len(rows),
        "regular_file_count": regular_count,
        "directory_count": directory_count,
        "symlink_count": symlink_count,
        "internal_byte_count": internal_bytes,
        "hashed_byte_count": hashed_bytes,
    }
    return summary, rows


def stable_tree_snapshot(
    root: str | Path,
    label: str,
    expected_device: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    first_summary, first_rows = tree_snapshot(root, label, expected_device)
    second_summary, second_rows = tree_snapshot(root, label, expected_device)
    if first_summary != second_summary or first_rows != second_rows:
        raise RuntimeAdmissionError(f"{label} changed during its two-pass snapshot")
    return second_summary, second_rows


def inspect_wheel(path: Path, row: dict[str, Any]) -> dict[str, Any]:
    descriptor: int | None = None
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size != row["byte_count"]:
            raise RuntimeAdmissionError(f"wheel changed before metadata inspection: {path}")
        with os.fdopen(descriptor, "rb", closefd=False) as handle, zipfile.ZipFile(handle) as archive:
            members = archive.infolist()
            if not members or len(members) > MAX_TREE_FILES:
                raise RuntimeAdmissionError(f"wheel member count is unsafe: {path}")
            names: set[str] = set()
            total_uncompressed = 0
            metadata_members: list[zipfile.ZipInfo] = []
            for member in members:
                member_path = PurePosixPath(member.filename)
                if (
                    member.filename in names
                    or member_path.is_absolute()
                    or ".." in member_path.parts
                    or "\x00" in member.filename
                ):
                    raise RuntimeAdmissionError(f"wheel contains an unsafe or duplicate member: {path}")
                names.add(member.filename)
                if member.flag_bits & 0x1:
                    raise RuntimeAdmissionError(f"wheel contains an encrypted member: {path}")
                unix_mode = (member.external_attr >> 16) & 0xFFFF
                if unix_mode and stat.S_ISLNK(unix_mode):
                    raise RuntimeAdmissionError(f"wheel contains a symlink member: {path}")
                total_uncompressed += member.file_size
                if total_uncompressed > MAX_TREE_BYTES:
                    raise RuntimeAdmissionError(f"wheel uncompressed size exceeds safety bounds: {path}")
                if (
                    len(member_path.parts) == 2
                    and member_path.parts[0].endswith(".dist-info")
                    and member_path.parts[1] == "METADATA"
                ):
                    metadata_members.append(member)
            if len(metadata_members) != 1 or metadata_members[0].file_size > MAX_JSON_BYTES:
                raise RuntimeAdmissionError(f"wheel must contain one bounded dist-info/METADATA: {path}")
            metadata_body = archive.read(metadata_members[0])
        after = os.fstat(descriptor)
        stable_fields = ("st_dev", "st_ino", "st_mode", "st_nlink", "st_uid", "st_size", "st_mtime_ns")
        if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
            raise RuntimeAdmissionError(f"wheel changed during metadata inspection: {path}")
    except (OSError, zipfile.BadZipFile, RuntimeError) as error:
        if isinstance(error, RuntimeAdmissionError):
            raise
        raise RuntimeAdmissionError(f"cannot inspect wheel {path}: {error}") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
    message = BytesParser(policy=email_policy).parsebytes(metadata_body)
    name = message.get("Name")
    version = message.get("Version")
    if not name or not version:
        raise RuntimeAdmissionError(f"wheel METADATA lacks Name or Version: {path}")
    return {
        "path": row["path"],
        "sha256": row["sha256"],
        "byte_count": row["byte_count"],
        "name": name,
        "normalized_name": normalized_distribution_name(name),
        "version": version,
        "member_count": len(members),
        "uncompressed_byte_count": total_uncompressed,
        "metadata_sha256": sha256_bytes(metadata_body),
    }


def wheelhouse_evidence(root: str | Path, expected_device: int) -> dict[str, Any]:
    summary, rows = stable_tree_snapshot(root, "wheelhouse", expected_device)
    if summary["symlink_count"]:
        raise RuntimeAdmissionError("wheelhouse may not contain symlinks")
    wheel_rows = [
        row for row in rows if row["kind"] == "regular_file" and row["path"].endswith(".whl")
    ]
    wheels = [inspect_wheel(Path(summary["root"]["path"]) / row["path"], row) for row in wheel_rows]
    if not wheels:
        raise RuntimeAdmissionError("wheelhouse contains no wheel files")
    replay_summary, replay_rows = tree_snapshot(root, "wheelhouse", expected_device)
    if replay_summary != summary or replay_rows != rows:
        raise RuntimeAdmissionError("wheelhouse changed during wheel metadata inspection")
    return {**summary, "wheels": wheels}


def validate_wheelhouse_packages(
    wheelhouse: dict[str, Any],
    inventory: dict[str, Any],
    lock_body: bytes,
) -> dict[str, Any]:
    try:
        lock_document = tomllib.loads(lock_body.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise RuntimeAdmissionError(f"cannot parse uv.lock wheel records: {error}") from error
    lock_packages = lock_document.get("package")
    if not isinstance(lock_packages, list) or not lock_packages:
        raise RuntimeAdmissionError("uv.lock contains no package wheel records")
    locked_wheels: dict[tuple[str, str], list[dict[str, str]]] = {}
    for package in lock_packages:
        if not isinstance(package, dict) or package.get("version") is None:
            continue
        name = normalized_distribution_name(
            strict_string(package.get("name"), "uv.lock wheel package name", 200)
        )
        version = strict_string(package["version"], "uv.lock wheel package version", 200)
        candidates = package.get("wheels", [])
        if not isinstance(candidates, list):
            raise RuntimeAdmissionError(f"uv.lock wheels are invalid for {name}=={version}")
        for candidate in candidates:
            if not isinstance(candidate, dict):
                raise RuntimeAdmissionError(f"uv.lock wheel entry is invalid for {name}=={version}")
            url = strict_string(candidate.get("url"), "uv.lock wheel URL", 8192)
            digest_value = strict_string(candidate.get("hash"), "uv.lock wheel hash", 128)
            if not digest_value.startswith("sha256:"):
                raise RuntimeAdmissionError("uv.lock wheel hash is not SHA-256")
            digest = strict_sha256(digest_value.removeprefix("sha256:"), "uv.lock wheel SHA-256")
            basename = Path(urllib.parse.unquote(urllib.parse.urlsplit(url).path)).name
            if not basename.endswith(".whl"):
                raise RuntimeAdmissionError("uv.lock wheel URL has no wheel basename")
            locked_wheels.setdefault((name, version), []).append(
                {"url": url, "basename": basename, "sha256": digest}
            )
    by_pair: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for wheel in wheelhouse["wheels"]:
        pair = (wheel["normalized_name"], wheel["version"])
        by_pair.setdefault(pair, []).append(wheel)
    installed_pairs = {
        (package["normalized_name"], package["version"])
        for package in inventory["packages"]
    }
    extra_pairs = sorted(set(by_pair) - installed_pairs)
    if extra_pairs:
        raise RuntimeAdmissionError(
            f"wheelhouse contains packages not installed in the runtime: {extra_pairs}"
        )
    missing: list[str] = []
    ambiguous: dict[str, list[str]] = {}
    unlocked: list[str] = []
    bindings: list[dict[str, str]] = []
    for package in inventory["packages"]:
        pair = (package["normalized_name"], package["version"])
        matches = by_pair.get(pair, [])
        label = f"{pair[0]}=={pair[1]}"
        if not matches:
            missing.append(label)
        elif len(matches) > 1:
            ambiguous[label] = sorted(item["path"] for item in matches)
        else:
            wheel = matches[0]
            candidates = [
                item
                for item in locked_wheels.get(pair, [])
                if item["sha256"] == wheel["sha256"]
                and item["basename"] == Path(wheel["path"]).name
            ]
            if len(candidates) != 1:
                unlocked.append(label)
                continue
            bindings.append(
                {
                    "package": label,
                    "wheel_path": wheel["path"],
                    "wheel_sha256": wheel["sha256"],
                    "uv_lock_url": candidates[0]["url"],
                }
            )
    if missing or ambiguous or unlocked:
        raise RuntimeAdmissionError(
            "wheelhouse is not an exact uv.lock-backed cover of installed packages: "
            f"missing={missing}, ambiguous={ambiguous}, unlocked={unlocked}"
        )
    bindings.sort(key=lambda item: item["package"])
    return {
        "installed_package_count": len(inventory["packages"]),
        "covered_package_count": len(bindings),
        "extra_wheel_package_count": 0,
        "all_wheel_hashes_bound_to_uv_lock": True,
        "bindings": bindings,
        "identity_sha256": sha256_bytes(canonical_bytes(bindings)),
    }


def distribution_root(distribution: importlib_metadata.Distribution) -> Path:
    private_path = getattr(distribution, "_path", None)
    if private_path is None:
        raise RuntimeAdmissionError("installed distribution has no inspectable metadata root")
    try:
        return Path(private_path).resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise RuntimeAdmissionError(f"cannot resolve installed distribution metadata: {error}") from error


def package_inventory(runtime_root: Path, runtime_rows: list[dict[str, Any]]) -> dict[str, Any]:
    regular_rows = {
        row["path"]: row for row in runtime_rows if row["kind"] == "regular_file"
    }
    packages: list[dict[str, Any]] = []
    seen: set[str] = set()
    for distribution in importlib_metadata.distributions():
        name = distribution.metadata.get("Name")
        version = distribution.version
        if not name or not version:
            raise RuntimeAdmissionError("installed distribution lacks Name or Version metadata")
        normalized = normalized_distribution_name(name)
        if normalized in seen:
            raise RuntimeAdmissionError(f"duplicate installed distribution metadata: {normalized}")
        seen.add(normalized)
        root = distribution_root(distribution)
        if not is_within(root, runtime_root):
            raise RuntimeAdmissionError(f"installed distribution lies outside runtime root: {root}")
        relative_root = root.relative_to(runtime_root).as_posix()
        prefix = relative_root + "/"
        metadata_rows = [
            {
                "path": path[len(prefix) :],
                "sha256": row["sha256"],
                "byte_count": row["byte_count"],
            }
            for path, row in regular_rows.items()
            if path.startswith(prefix)
        ]
        metadata_rows.sort(key=lambda item: item["path"])
        if not metadata_rows:
            raise RuntimeAdmissionError(f"distribution metadata tree is absent from runtime snapshot: {root}")
        by_basename = {Path(item["path"]).name: item for item in metadata_rows}
        metadata_item = by_basename.get("METADATA")
        if metadata_item is None:
            raise RuntimeAdmissionError(f"distribution lacks METADATA: {root}")
        metadata_path = root / metadata_item["path"]
        metadata_body, _ = stable_file_bytes(metadata_path, f"{normalized} METADATA", MAX_JSON_BYTES)
        message = BytesParser(policy=email_policy).parsebytes(metadata_body)
        if message.get("Name") != name or message.get("Version") != version:
            raise RuntimeAdmissionError(f"distribution API and METADATA disagree: {normalized}")
        packages.append(
            {
                "name": name,
                "normalized_name": normalized,
                "version": version,
                "metadata_root": str(root),
                "metadata_tree_sha256": sha256_bytes(canonical_bytes(metadata_rows)),
                "metadata_file_count": len(metadata_rows),
                "metadata_byte_count": sum(item["byte_count"] for item in metadata_rows),
                "metadata_sha256": metadata_item["sha256"],
                "record_sha256": by_basename.get("RECORD", {}).get("sha256"),
                "wheel_sha256": by_basename.get("WHEEL", {}).get("sha256"),
                "direct_url_sha256": by_basename.get("direct_url.json", {}).get("sha256"),
            }
        )
    packages.sort(key=lambda item: item["normalized_name"])
    if not packages:
        raise RuntimeAdmissionError("runtime exposes no installed package metadata")
    return {
        "packages": packages,
        "package_count": len(packages),
        "identity_sha256": sha256_bytes(canonical_bytes(packages)),
    }


def parse_required_package(value: str) -> tuple[str, str]:
    if value.count("==") != 1:
        raise RuntimeAdmissionError(f"required package must use NAME==VERSION: {value}")
    name, version = value.split("==", 1)
    strict_name(name, "required package name")
    strict_string(version, "required package version", 200)
    return normalized_distribution_name(name), version


def locked_project_evidence(
    pyproject_body: bytes,
    lock_body: bytes,
    inventory: dict[str, Any],
    extra_required: Iterable[str],
) -> dict[str, Any]:
    try:
        project_document = tomllib.loads(pyproject_body.decode("utf-8"))
        lock_document = tomllib.loads(lock_body.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise RuntimeAdmissionError(f"cannot parse locked project metadata: {error}") from error
    project = project_document.get("project")
    if not isinstance(project, dict):
        raise RuntimeAdmissionError("pyproject lacks [project]")
    required_python = project.get("requires-python")
    actual_python = ".".join(str(part) for part in sys.version_info[:3])
    if required_python != f"=={actual_python}":
        raise RuntimeAdmissionError("pyproject must pin the exact executing CPython version")
    dependencies = project.get("dependencies")
    if not isinstance(dependencies, list) or not dependencies:
        raise RuntimeAdmissionError("pyproject must contain exact direct dependencies")
    required: dict[str, str] = {}
    for dependency in [*dependencies, *extra_required]:
        if not isinstance(dependency, str):
            raise RuntimeAdmissionError("project dependency is not a string")
        name, version = parse_required_package(dependency)
        if name in required and required[name] != version:
            raise RuntimeAdmissionError(f"conflicting required package versions: {name}")
        required[name] = version
    installed = {item["normalized_name"]: item["version"] for item in inventory["packages"]}
    mismatches = {
        name: {"expected": version, "actual": installed.get(name)}
        for name, version in sorted(required.items())
        if installed.get(name) != version
    }
    if mismatches:
        raise RuntimeAdmissionError(f"required installed package mismatch: {mismatches}")
    lock_requires_python = lock_document.get("requires-python")
    if lock_requires_python != required_python:
        raise RuntimeAdmissionError("uv.lock and pyproject Python pins differ")
    lock_packages = lock_document.get("package")
    if not isinstance(lock_packages, list) or not lock_packages:
        raise RuntimeAdmissionError("uv.lock contains no packages")
    projection: list[dict[str, Any]] = []
    locked_versions: dict[str, set[str]] = {}
    for package in lock_packages:
        if not isinstance(package, dict):
            raise RuntimeAdmissionError("uv.lock package entry is invalid")
        name = strict_string(package.get("name"), "uv.lock package name", 200)
        version = package.get("version")
        if version is not None:
            version = strict_string(version, "uv.lock package version", 200)
            locked_versions.setdefault(normalized_distribution_name(name), set()).add(version)
        source = package.get("source")
        projection.append({"name": name, "version": version, "source": source})
    projection.sort(key=lambda item: (normalized_distribution_name(item["name"]), item["version"] or ""))
    for name, version in required.items():
        if version not in locked_versions.get(name, set()):
            raise RuntimeAdmissionError(f"direct dependency is not exact in uv.lock: {name}=={version}")
    unlocked_installed = {
        name: version
        for name, version in sorted(installed.items())
        if version not in locked_versions.get(name, set())
    }
    if unlocked_installed:
        raise RuntimeAdmissionError(
            f"installed packages are absent from the exact uv.lock closure: {unlocked_installed}"
        )
    return {
        "python_version": actual_python,
        "requires_python": required_python,
        "required_packages": [
            {"name": name, "version": version} for name, version in sorted(required.items())
        ],
        "uv_lock_version": strict_int(lock_document.get("version"), "uv.lock version", 1, 100),
        "uv_lock_revision": strict_int(lock_document.get("revision"), "uv.lock revision", 0, 1000),
        "locked_package_count": len(lock_packages),
        "all_installed_packages_present_in_lock": True,
        "locked_package_projection_sha256": sha256_bytes(canonical_bytes(projection)),
    }


def character_device_binding(path_value: str | Path) -> dict[str, Any]:
    path = absolute_path(path_value, "GPU device node")
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISCHR(metadata.st_mode):
        raise RuntimeAdmissionError(f"GPU device node is not a direct character device: {path}")
    # Unprivileged Bubblewrap maps host root-owned device nodes to the overflow
    # UID/GID.  Major, minor, type, mode, and the explicit path remain stable
    # across that production namespace and are the security-relevant binding.
    return {
        "path": str(path),
        "major": os.major(metadata.st_rdev),
        "minor": os.minor(metadata.st_rdev),
        "mode": stat.S_IMODE(metadata.st_mode),
    }


def hardware_evidence(config: dict[str, Any]) -> dict[str, Any]:
    try:
        import ctranslate2
        import pynvml
    except ImportError as error:
        raise RuntimeAdmissionError(f"locked GPU packages cannot be imported: {error}") from error
    expected_uuid = strict_string(config["expected_gpu_uuid"], "expected GPU UUID", 200)
    if not GPU_UUID_RE.fullmatch(expected_uuid):
        raise RuntimeAdmissionError("expected GPU UUID is invalid")
    device_index = strict_int(config["device_index"], "GPU device index", 0, 63)
    expected_driver = strict_string(config["expected_driver_version"], "expected driver version", 100)
    expected_compute_type = strict_string(config["expected_compute_type"], "expected compute type", 100)
    pynvml.nvmlInit()
    try:
        nvml_count = int(pynvml.nvmlDeviceGetCount())
        ctranslate2_count = int(ctranslate2.get_cuda_device_count())
        if device_index >= nvml_count or device_index >= ctranslate2_count:
            raise RuntimeAdmissionError("selected GPU index is not visible through both NVML and CTranslate2")
        handle = pynvml.nvmlDeviceGetHandleByIndex(device_index)
        uuid = pynvml.nvmlDeviceGetUUID(handle)
        name = pynvml.nvmlDeviceGetName(handle)
        driver = pynvml.nvmlSystemGetDriverVersion()
        if isinstance(uuid, bytes):
            uuid = uuid.decode("utf-8")
        if isinstance(name, bytes):
            name = name.decode("utf-8")
        if isinstance(driver, bytes):
            driver = driver.decode("utf-8")
        supported = sorted(str(item) for item in ctranslate2.get_supported_compute_types("cuda", device_index))
        if uuid != expected_uuid:
            raise RuntimeAdmissionError("NVML GPU UUID differs from the admitted UUID")
        if driver != expected_driver:
            raise RuntimeAdmissionError("NVML driver version differs from the admitted version")
        if expected_compute_type not in supported:
            raise RuntimeAdmissionError("required compute type is not supported on the selected device")
        memory = pynvml.nvmlDeviceGetMemoryInfo(handle)
        total_memory = int(memory.total)
        minimum_memory = strict_int(config["minimum_total_vram_bytes"], "minimum total VRAM bytes", 1)
        if total_memory < minimum_memory:
            raise RuntimeAdmissionError("GPU total VRAM is below the admitted floor")
        evidence = {
            "device_index": device_index,
            "uuid": uuid,
            "name": name,
            "driver_version": driver,
            "cuda_driver_version": int(pynvml.nvmlSystemGetCudaDriverVersion_v2()),
            "compute_capability": list(pynvml.nvmlDeviceGetCudaComputeCapability(handle)),
            "total_memory_bytes": total_memory,
            "nvml_device_count": nvml_count,
            "ctranslate2_cuda_device_count": ctranslate2_count,
            "ctranslate2_supported_compute_types": supported,
            "selected_compute_type": expected_compute_type,
            "device_nodes": [character_device_binding(path) for path in config["device_nodes"]],
        }
    finally:
        pynvml.nvmlShutdown()
    return evidence


def identity_document(
    path_value: str | Path,
    expected_sha256: str,
    expected_kind: str,
    label: str,
    expected_device: int | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    binding = file_binding(path_value, label, expected_sha256, expected_device=expected_device)
    body, _ = stable_file_bytes(Path(binding["resolved_path"]), label, MAX_JSON_BYTES)
    if sha256_bytes(body) != binding["sha256"]:
        raise RuntimeAdmissionError(f"{label} changed between binding and parsing")
    document = load_json_bytes(body, label)
    if not isinstance(document, dict):
        raise RuntimeAdmissionError(f"{label} must be a JSON object")
    if document.get("kind") != expected_kind or document.get("schema_version") != 1:
        raise RuntimeAdmissionError(f"{label} kind or schema is unsupported")
    identity = strict_sha256(document.get("identity_sha256"), f"{label} identity")
    core = {key: value for key, value in document.items() if key != "identity_sha256"}
    if sha256_bytes(canonical_bytes(core)) != identity:
        raise RuntimeAdmissionError(f"{label} semantic identity is invalid")
    return document, binding


def metric_thresholds(config: dict[str, Any]) -> list[dict[str, Any]]:
    thresholds: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in config["additional_metric_thresholds"]:
        if not isinstance(item, dict) or set(item) != {"metric", "operator", "value"}:
            raise RuntimeAdmissionError("additional metric threshold shape is invalid")
        metric = strict_string(item["metric"], "threshold metric", 128)
        if not METRIC_RE.fullmatch(metric) or metric in seen:
            raise RuntimeAdmissionError(f"threshold metric is invalid or duplicated: {metric}")
        seen.add(metric)
        operator = item["operator"]
        if operator not in {"le", "lt", "ge", "gt", "eq"}:
            raise RuntimeAdmissionError(f"threshold operator is invalid: {operator}")
        value = finite_number(item["value"], f"threshold {metric}")
        thresholds.append({"metric": metric, "operator": operator, "value": value})
    return sorted(thresholds, key=lambda item: item["metric"])


def compare_metric(actual: float, operator: str, expected: float) -> bool:
    return {
        "le": actual <= expected,
        "lt": actual < expected,
        "ge": actual >= expected,
        "gt": actual > expected,
        "eq": actual == expected,
    }[operator]


def benchmark_evidence(
    config: dict[str, Any],
    runtime_tree_sha256: str,
    wheelhouse_tree_sha256: str,
    hardware: dict[str, Any],
) -> dict[str, Any]:
    expected_device = strict_int(config["expected_main_drive_device"], "expected main-drive device", 1)
    document, binding = identity_document(
        config["benchmark"]["path"],
        config["benchmark"]["expected_sha256"],
        BENCHMARK_KIND,
        "frozen benchmark",
        expected_device,
    )
    required = {
        "kind",
        "schema_version",
        "benchmark_id",
        "frozen",
        "runtime_tree_sha256",
        "wheelhouse_tree_sha256",
        "gpu",
        "profile",
        "dataset",
        "raw_result",
        "metrics",
        "policy",
        "identity_sha256",
    }
    if set(document) != required:
        raise RuntimeAdmissionError("frozen benchmark has an unexpected top-level shape")
    strict_name(document["benchmark_id"], "benchmark ID")
    if document["frozen"] is not True:
        raise RuntimeAdmissionError("benchmark must be explicitly frozen")
    if document["runtime_tree_sha256"] != runtime_tree_sha256:
        raise RuntimeAdmissionError("benchmark runtime-tree digest differs")
    if document["wheelhouse_tree_sha256"] != wheelhouse_tree_sha256:
        raise RuntimeAdmissionError("benchmark wheelhouse digest differs")
    gpu = document["gpu"]
    if not isinstance(gpu, dict) or set(gpu) != {"uuid", "driver_version", "device_index", "compute_type"}:
        raise RuntimeAdmissionError("benchmark GPU binding is invalid")
    if (
        gpu["uuid"] != hardware["uuid"]
        or gpu["driver_version"] != hardware["driver_version"]
        or gpu["device_index"] != hardware["device_index"]
        or gpu["compute_type"] != hardware["selected_compute_type"]
    ):
        raise RuntimeAdmissionError("benchmark GPU/profile binding differs from current hardware")
    profile = document["profile"]
    if not isinstance(profile, dict) or set(profile) != {
        "name",
        "model_identity_sha256",
        "work_order_contract_sha256",
        "result_contract_sha256",
        "inference",
        "inference_profile_sha256",
    }:
        raise RuntimeAdmissionError("benchmark production profile binding is invalid")
    strict_name(profile["name"], "benchmark profile name")
    for key in (
        "model_identity_sha256",
        "work_order_contract_sha256",
        "result_contract_sha256",
        "inference_profile_sha256",
    ):
        strict_sha256(profile[key], f"benchmark profile {key}")
    inference = profile["inference"]
    inference_keys = {
        "compute_type",
        "language",
        "beam_size",
        "best_of",
        "temperature",
        "condition_on_previous_text",
        "word_timestamps",
        "vad_filter",
        "cpu_threads",
        "num_workers",
    }
    if not isinstance(inference, dict) or set(inference) != inference_keys:
        raise RuntimeAdmissionError("benchmark inference profile shape is invalid")
    if sha256_bytes(canonical_bytes(inference)) != profile["inference_profile_sha256"]:
        raise RuntimeAdmissionError("benchmark inference profile identity is invalid")
    if inference["compute_type"] != hardware["selected_compute_type"]:
        raise RuntimeAdmissionError("benchmark inference compute type differs")
    if inference["language"] != "en":
        raise RuntimeAdmissionError("production benchmark language must be en")
    for name, maximum in (("beam_size", 20), ("best_of", 20), ("cpu_threads", 32), ("num_workers", 4)):
        strict_int(inference[name], f"benchmark inference {name}", 1, maximum)
    finite_number(inference["temperature"], "benchmark inference temperature", 0.0, 1.0)
    if (
        inference["condition_on_previous_text"] is not False
        or inference["word_timestamps"] is not True
        or inference["vad_filter"] is not False
    ):
        raise RuntimeAdmissionError("benchmark inference invariants differ from production")
    dataset = document["dataset"]
    if not isinstance(dataset, dict) or set(dataset) != {
        "manifest_sha256",
        "recording_disjoint",
        "case_count",
        "total_audio_seconds",
        "repetitions",
    }:
        raise RuntimeAdmissionError("benchmark dataset binding is invalid")
    strict_sha256(dataset["manifest_sha256"], "benchmark dataset manifest")
    if dataset["recording_disjoint"] is not True:
        raise RuntimeAdmissionError("benchmark dataset must be recording-disjoint")
    case_count = strict_int(dataset["case_count"], "benchmark case count", 1)
    repetitions = strict_int(dataset["repetitions"], "benchmark repetitions", 1)
    total_audio_seconds = finite_number(dataset["total_audio_seconds"], "benchmark audio seconds", 0.000001)
    metrics = document["metrics"]
    required_metrics = {
        "completed_cases",
        "failed_cases",
        "p95_real_time_factor",
        "peak_process_vram_bytes",
        "peak_global_vram_bytes",
    }
    if not isinstance(metrics, dict) or not required_metrics.issubset(metrics):
        raise RuntimeAdmissionError("benchmark metrics are incomplete")
    normalized_metrics: dict[str, int | float] = {}
    for name, value in metrics.items():
        if not isinstance(name, str) or not METRIC_RE.fullmatch(name):
            raise RuntimeAdmissionError(f"benchmark metric name is invalid: {name!r}")
        if name in {"completed_cases", "failed_cases", "peak_process_vram_bytes", "peak_global_vram_bytes"}:
            normalized_metrics[name] = strict_int(value, f"benchmark metric {name}")
        else:
            normalized_metrics[name] = finite_number(value, f"benchmark metric {name}")
    if normalized_metrics["p95_real_time_factor"] <= 0:
        raise RuntimeAdmissionError("benchmark p95 real-time factor must be positive")
    if not (
        normalized_metrics["peak_process_vram_bytes"]
        <= normalized_metrics["peak_global_vram_bytes"]
        <= hardware["total_memory_bytes"]
    ):
        raise RuntimeAdmissionError("benchmark VRAM accounting is inconsistent with physical VRAM")
    if normalized_metrics["completed_cases"] + normalized_metrics["failed_cases"] != case_count * repetitions:
        raise RuntimeAdmissionError("benchmark case accounting does not match cases multiplied by repetitions")
    raw_reference = document["raw_result"]
    if not isinstance(raw_reference, dict) or set(raw_reference) != {
        "path",
        "sha256",
        "byte_count",
        "identity_sha256",
    }:
        raise RuntimeAdmissionError("benchmark raw-result reference is invalid")
    raw_binding = file_binding(
        raw_reference["path"],
        "raw production benchmark result",
        strict_sha256(raw_reference["sha256"], "raw benchmark SHA-256"),
        expected_device=expected_device,
    )
    if (
        raw_binding["byte_count"]
        != strict_int(raw_reference["byte_count"], "raw benchmark byte count", 1)
        or raw_binding["mode"] != 0o400
        or raw_binding["link_count"] != 1
        or raw_binding["requested_symlink"]
    ):
        raise RuntimeAdmissionError(
            "raw benchmark must be a direct mode-0400 single-link file"
        )
    raw_body, _ = stable_file_bytes(
        Path(raw_binding["resolved_path"]),
        "raw production benchmark result",
        MAX_JSON_BYTES,
    )
    raw_result = load_json_bytes(raw_body, "raw production benchmark result")
    if not isinstance(raw_result, dict) or raw_body != canonical_bytes(raw_result):
        raise RuntimeAdmissionError("raw benchmark is not canonical JSON")
    raw_identity = strict_sha256(
        raw_result.get("identity_sha256"), "raw benchmark identity"
    )
    if (
        raw_identity
        != strict_sha256(
            raw_reference["identity_sha256"], "raw benchmark referenced identity"
        )
        or sha256_bytes(
            canonical_bytes(
                {key: value for key, value in raw_result.items() if key != "identity_sha256"}
            )
        )
        != raw_identity
    ):
        raise RuntimeAdmissionError("raw benchmark semantic identity is invalid")
    if (
        raw_result.get("kind") != "himr_gpu_smoke_result"
        or raw_result.get("schema_version") != 2
        or raw_result.get("status") != "completed"
    ):
        raise RuntimeAdmissionError("raw benchmark result kind or status is unsupported")
    if case_count != 1 or repetitions != 1:
        raise RuntimeAdmissionError("benchmark schema v1 binds exactly one raw case and repetition")
    try:
        raw_profile = {
            key: raw_result["parameters"][key]
            for key in inference_keys
        }
        raw_metric_projection = {
            "completed_cases": 1,
            "failed_cases": 0,
            "p95_real_time_factor": raw_result["metrics"]["real_time_factor_inference"],
            "peak_process_vram_bytes": raw_result["hardware"]["process_peak_used_bytes"],
            "peak_global_vram_bytes": raw_result["hardware"]["global_peak_used_bytes"],
        }
        raw_gpu_projection = {
            "uuid": raw_result["hardware"]["uuid"],
            "driver_version": raw_result["hardware"]["driver_version"],
            "device_index": raw_result["hardware"]["device_index"],
            "compute_type": raw_result["parameters"]["compute_type"],
        }
    except (KeyError, TypeError) as error:
        raise RuntimeAdmissionError("raw benchmark result is incomplete") from error
    if (
        raw_profile != inference
        or raw_gpu_projection != gpu
        or raw_result["model"]["identity_sha256"] != profile["model_identity_sha256"]
        or raw_result["input"]["fixture_manifest_sha256"] != dataset["manifest_sha256"]
        or float(raw_result["input"]["duration_seconds"]) != float(total_audio_seconds)
        or raw_metric_projection != {
            key: normalized_metrics[key] for key in raw_metric_projection
        }
    ):
        raise RuntimeAdmissionError(
            "frozen benchmark does not exactly project its raw production run"
        )
    raw_policy = raw_result.get("policy")
    if (
        not isinstance(raw_policy, dict)
        or raw_policy.get("corpus_media_processed") is not False
        or raw_policy.get("publication_authority") != "none"
        or raw_policy.get("identity_authority") != "none"
    ):
        raise RuntimeAdmissionError("raw benchmark policy is not synthetic and authority-free")
    policy = document["policy"]
    if not isinstance(policy, dict) or set(policy) != {
        "private_only",
        "publication_authority",
        "identity_authority",
        "network_used",
    }:
        raise RuntimeAdmissionError("benchmark policy is invalid")
    if (
        policy["private_only"] is not True
        or policy["publication_authority"] != "none"
        or policy["identity_authority"] != "none"
        or policy["network_used"] is not False
    ):
        raise RuntimeAdmissionError("benchmark policy does not satisfy private offline admission")
    thresholds = {
        "minimum_completed_cases": strict_int(config["thresholds"]["minimum_completed_cases"], "minimum completed cases", 1),
        "minimum_total_audio_seconds": finite_number(config["thresholds"]["minimum_total_audio_seconds"], "minimum total audio seconds", 0.000001),
        "minimum_repetitions": strict_int(config["thresholds"]["minimum_repetitions"], "minimum repetitions", 1),
        "maximum_failed_cases": strict_int(config["thresholds"]["maximum_failed_cases"], "maximum failed cases"),
        "maximum_p95_real_time_factor": finite_number(config["thresholds"]["maximum_p95_real_time_factor"], "maximum p95 real-time factor", 0.000001),
        "maximum_peak_process_vram_bytes": strict_int(config["thresholds"]["maximum_peak_process_vram_bytes"], "maximum peak process VRAM", 1),
        "minimum_vram_reserve_bytes": strict_int(config["thresholds"]["minimum_vram_reserve_bytes"], "minimum VRAM reserve"),
        "additional_metrics": metric_thresholds(config["thresholds"]),
    }
    checks = [
        {"name": "completed_cases", "actual": normalized_metrics["completed_cases"], "operator": "ge", "threshold": thresholds["minimum_completed_cases"]},
        {"name": "total_audio_seconds", "actual": total_audio_seconds, "operator": "ge", "threshold": thresholds["minimum_total_audio_seconds"]},
        {"name": "repetitions", "actual": repetitions, "operator": "ge", "threshold": thresholds["minimum_repetitions"]},
        {"name": "failed_cases", "actual": normalized_metrics["failed_cases"], "operator": "le", "threshold": thresholds["maximum_failed_cases"]},
        {"name": "p95_real_time_factor", "actual": normalized_metrics["p95_real_time_factor"], "operator": "le", "threshold": thresholds["maximum_p95_real_time_factor"]},
        {"name": "peak_process_vram_bytes", "actual": normalized_metrics["peak_process_vram_bytes"], "operator": "le", "threshold": thresholds["maximum_peak_process_vram_bytes"]},
        {
            "name": "vram_reserve_bytes",
            "actual": hardware["total_memory_bytes"] - normalized_metrics["peak_process_vram_bytes"],
            "operator": "ge",
            "threshold": thresholds["minimum_vram_reserve_bytes"],
        },
    ]
    for threshold in thresholds["additional_metrics"]:
        if threshold["metric"] not in normalized_metrics:
            raise RuntimeAdmissionError(f"threshold metric is absent from benchmark: {threshold['metric']}")
        checks.append(
            {
                "name": threshold["metric"],
                "actual": normalized_metrics[threshold["metric"]],
                "operator": threshold["operator"],
                "threshold": threshold["value"],
            }
        )
    failed = [
        item
        for item in checks
        if not compare_metric(float(item["actual"]), item["operator"], float(item["threshold"]))
    ]
    if failed:
        raise RuntimeAdmissionError(f"frozen benchmark fails acceptance thresholds: {failed}")
    return {
        "artifact": binding,
        "identity_sha256": document["identity_sha256"],
        "benchmark_id": document["benchmark_id"],
        "profile": profile,
        "dataset": dataset,
        "raw_result": {
            "artifact": raw_binding,
            "identity_sha256": raw_identity,
        },
        "metrics": normalized_metrics,
        "thresholds": thresholds,
        "checks": checks,
        "accepted": True,
    }


def validate_reference_binding(
    reference: Any,
    label: str,
    expected_device: int,
) -> dict[str, Any]:
    if not isinstance(reference, dict) or set(reference) != {"name", "path", "sha256", "byte_count"}:
        raise RuntimeAdmissionError(f"{label} binding shape is invalid")
    name = strict_name(reference["name"], f"{label} name")
    expected_size = strict_int(reference["byte_count"], f"{label} byte count", 1)
    binding = file_binding(
        reference["path"],
        label,
        strict_sha256(reference["sha256"], f"{label} SHA-256"),
        expected_device=expected_device,
    )
    if binding["byte_count"] != expected_size:
        raise RuntimeAdmissionError(f"{label} byte count differs")
    return {"name": name, **binding}


def scheduler_evidence(
    config: dict[str, Any],
    runtime_tree_sha256: str,
    hardware: dict[str, Any],
) -> dict[str, Any]:
    expected_device = strict_int(config["expected_main_drive_device"], "expected main-drive device", 1)
    document, binding = identity_document(
        config["scheduler_evidence"]["path"],
        config["scheduler_evidence"]["expected_sha256"],
        SCHEDULER_KIND,
        "scheduler-lock evidence",
        expected_device,
    )
    required = {
        "kind",
        "schema_version",
        "evidence_id",
        "gpu_uuid",
        "device_index",
        "runtime_tree_sha256",
        "lock_path",
        "lock_key",
        "lock_scope",
        "mechanism",
        "maximum_concurrent_gpu_jobs",
        "implementation",
        "test_result",
        "policy",
        "identity_sha256",
    }
    if set(document) != required:
        raise RuntimeAdmissionError("scheduler-lock evidence has an unexpected shape")
    strict_name(document["evidence_id"], "scheduler evidence ID")
    if (
        document["gpu_uuid"] != hardware["uuid"]
        or document["device_index"] != hardware["device_index"]
        or document["runtime_tree_sha256"] != runtime_tree_sha256
    ):
        raise RuntimeAdmissionError("scheduler evidence is not bound to this runtime and GPU")
    if document["lock_scope"] != "gpu_uuid" or document["lock_key"] != hardware["uuid"]:
        raise RuntimeAdmissionError("scheduler lock is not keyed to the exact GPU UUID")
    if document["mechanism"] not in {"flock", "fcntl"}:
        raise RuntimeAdmissionError("scheduler lock mechanism is unsupported")
    if document["maximum_concurrent_gpu_jobs"] != 1:
        raise RuntimeAdmissionError("scheduler must enforce exactly one concurrent GPU job")
    lock_path = absolute_path(document["lock_path"], "scheduler lock path")
    lock_parent = lock_path.parent
    parent_binding = directory_binding(lock_parent, "scheduler lock parent", expected_device)
    if parent_binding["mode"] != 0o700:
        raise RuntimeAdmissionError("scheduler lock parent must be owner-private mode 0700")
    if not lock_path.exists() or lock_path.is_symlink():
        raise RuntimeAdmissionError("scheduler lock file must be pre-created as a direct regular file")
    lock_binding = file_binding(lock_path, "scheduler lock file", expected_device=expected_device)
    if lock_binding["mode"] != 0o600 or lock_binding["link_count"] != 1 or lock_binding["byte_count"] != 0:
        raise RuntimeAdmissionError("scheduler lock file must be empty, mode 0600, and single-linked")
    implementation = document["implementation"]
    if not isinstance(implementation, list) or not implementation:
        raise RuntimeAdmissionError("scheduler evidence has no implementation bindings")
    implementation_bindings = [
        validate_reference_binding(item, "scheduler implementation", expected_device)
        for item in implementation
    ]
    names = [item["name"] for item in implementation_bindings]
    if len(names) != len(set(names)):
        raise RuntimeAdmissionError("scheduler implementation names are duplicated")
    test_reference = document["test_result"]
    if not isinstance(test_reference, dict) or set(test_reference) != {
        "path",
        "sha256",
        "byte_count",
        "identity_sha256",
    }:
        raise RuntimeAdmissionError("scheduler test-result reference is invalid")
    test_document, test_binding = identity_document(
        test_reference["path"],
        strict_sha256(test_reference["sha256"], "scheduler test-result SHA-256"),
        SCHEDULER_TEST_KIND,
        "scheduler lock test result",
        expected_device,
    )
    if test_binding["byte_count"] != strict_int(test_reference["byte_count"], "scheduler test-result byte count", 1):
        raise RuntimeAdmissionError("scheduler test-result byte count differs")
    if test_document["identity_sha256"] != strict_sha256(
        test_reference["identity_sha256"], "scheduler test-result identity"
    ):
        raise RuntimeAdmissionError("scheduler test-result identity differs")
    test_required = {
        "kind",
        "schema_version",
        "gpu_uuid",
        "lock_path",
        "attempted_workers",
        "successful_holders",
        "rejected_contenders",
        "maximum_simultaneous_holders",
        "crash_release_verified",
        "completed_at",
        "identity_sha256",
    }
    if set(test_document) != test_required:
        raise RuntimeAdmissionError("scheduler lock test result has an unexpected shape")
    if test_document["gpu_uuid"] != hardware["uuid"] or test_document["lock_path"] != str(lock_path):
        raise RuntimeAdmissionError("scheduler test result is not bound to this UUID and lock path")
    attempted = strict_int(test_document["attempted_workers"], "scheduler attempted workers", 2)
    successful = strict_int(test_document["successful_holders"], "scheduler successful holders", 1)
    rejected = strict_int(test_document["rejected_contenders"], "scheduler rejected contenders", 1)
    maximum_holders = strict_int(test_document["maximum_simultaneous_holders"], "maximum simultaneous holders", 1)
    if maximum_holders != 1 or successful < 1 or rejected < 1 or attempted != successful + rejected:
        raise RuntimeAdmissionError("scheduler contention test does not prove exclusive admission")
    if test_document["crash_release_verified"] is not True:
        raise RuntimeAdmissionError("scheduler test does not prove crash-release behavior")
    parse_utc(test_document["completed_at"], "scheduler test completion")
    policy = document["policy"]
    if not isinstance(policy, dict) or set(policy) != {
        "network_used",
        "credential_used",
        "catalogue_authority",
        "publication_authority",
    }:
        raise RuntimeAdmissionError("scheduler evidence policy is invalid")
    if (
        policy["network_used"] is not False
        or policy["credential_used"] is not False
        or policy["catalogue_authority"] != "none"
        or policy["publication_authority"] != "none"
    ):
        raise RuntimeAdmissionError("scheduler evidence policy is not offline and authority-free")
    return {
        "artifact": binding,
        "identity_sha256": document["identity_sha256"],
        "evidence_id": document["evidence_id"],
        "gpu_uuid": document["gpu_uuid"],
        "lock_path": str(lock_path),
        "lock_parent": parent_binding,
        "lock_file_present_during_admission": True,
        "lock_file_binding": lock_binding,
        "mechanism": document["mechanism"],
        "maximum_concurrent_gpu_jobs": 1,
        "implementation": implementation_bindings,
        "test_result": {
            "artifact": test_binding,
            "identity_sha256": test_document["identity_sha256"],
            "attempted_workers": attempted,
            "successful_holders": successful,
            "rejected_contenders": rejected,
            "maximum_simultaneous_holders": maximum_holders,
            "crash_release_verified": True,
        },
        "accepted": True,
    }


def named_bindings(
    specifications: list[dict[str, str]],
    label: str,
    *,
    executable: bool,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for specification in specifications:
        if not isinstance(specification, dict) or set(specification) != {"name", "path", "expected_sha256"}:
            raise RuntimeAdmissionError(f"{label} specification shape is invalid")
        name = strict_name(specification["name"], f"{label} name")
        if name in seen:
            raise RuntimeAdmissionError(f"duplicate {label} name: {name}")
        seen.add(name)
        result.append(
            {
                "name": name,
                **file_binding(
                    specification["path"],
                    f"{label} {name}",
                    specification["expected_sha256"],
                    executable=executable,
                ),
            }
        )
    if not result:
        raise RuntimeAdmissionError(f"at least one {label} binding is required")
    return result


def collect_evidence(config: dict[str, Any]) -> dict[str, Any]:
    if not sys.flags.isolated:
        raise RuntimeAdmissionError("admission and replay require Python isolated mode (-I)")
    expected_device = strict_int(config["expected_main_drive_device"], "expected main-drive device", 1)
    runtime_root = absolute_path(config["runtime_root"], "runtime root")
    wheelhouse_root = absolute_path(config["wheelhouse_root"], "wheelhouse root")
    runtime_summary, runtime_rows = stable_tree_snapshot(runtime_root, "runtime tree", expected_device)
    expected_runtime = strict_sha256(config["expected_runtime_tree_sha256"], "expected runtime tree")
    if runtime_summary["tree_sha256"] != expected_runtime:
        raise RuntimeAdmissionError("runtime tree SHA-256 differs from its expected digest")
    packages = package_inventory(runtime_root, runtime_rows)
    replay_summary, replay_rows = tree_snapshot(runtime_root, "runtime tree", expected_device)
    if replay_summary != runtime_summary or replay_rows != runtime_rows:
        raise RuntimeAdmissionError("runtime tree changed during package metadata inspection")
    wheelhouse = wheelhouse_evidence(wheelhouse_root, expected_device)
    expected_wheelhouse = strict_sha256(config["expected_wheelhouse_tree_sha256"], "expected wheelhouse tree")
    if wheelhouse["tree_sha256"] != expected_wheelhouse:
        raise RuntimeAdmissionError("wheelhouse tree SHA-256 differs from its expected digest")
    utility_source = file_binding(
        Path(__file__).resolve(),
        "runtime-admission utility",
        config["expected_admission_source_sha256"],
        expected_device=expected_device,
    )
    python_binding = file_binding(
        config["python_executable"]["path"],
        "Python executable",
        config["python_executable"]["expected_sha256"],
        executable=True,
    )
    if Path(python_binding["resolved_path"]) != Path(sys.executable).resolve():
        raise RuntimeAdmissionError("bound Python executable is not the executing interpreter")
    pyproject_binding = file_binding(
        config["pyproject"]["path"],
        "GPU pyproject",
        config["pyproject"]["expected_sha256"],
        expected_device=expected_device,
    )
    lock_binding = file_binding(
        config["lock"]["path"],
        "GPU uv.lock",
        config["lock"]["expected_sha256"],
        expected_device=expected_device,
    )
    pyproject_body, _ = stable_file_bytes(Path(pyproject_binding["resolved_path"]), "GPU pyproject", MAX_JSON_BYTES)
    lock_body, _ = stable_file_bytes(Path(lock_binding["resolved_path"]), "GPU uv.lock", MAX_JSON_BYTES)
    project = locked_project_evidence(pyproject_body, lock_body, packages, config["required_packages"])
    wheel_package_binding = validate_wheelhouse_packages(
        wheelhouse, packages, lock_body
    )
    source_bindings = named_bindings(config["sources"], "production source", executable=False)
    executable_bindings = named_bindings(config["executables"], "production executable", executable=True)
    hardware = hardware_evidence(config["hardware"])
    benchmark = benchmark_evidence(
        config,
        runtime_summary["tree_sha256"],
        wheelhouse["tree_sha256"],
        hardware,
    )
    scheduler = scheduler_evidence(config, runtime_summary["tree_sha256"], hardware)
    final_runtime_summary, final_runtime_rows = tree_snapshot(
        runtime_root, "runtime tree", expected_device
    )
    if final_runtime_summary != runtime_summary or final_runtime_rows != runtime_rows:
        raise RuntimeAdmissionError("runtime tree changed before admission completed")
    final_wheelhouse_summary, final_wheelhouse_rows = tree_snapshot(
        wheelhouse_root, "wheelhouse", expected_device
    )
    wheelhouse_summary_projection = {
        key: value for key, value in wheelhouse.items() if key != "wheels"
    }
    if (
        final_wheelhouse_summary != wheelhouse_summary_projection
        or sha256_bytes(canonical_bytes(final_wheelhouse_rows)) != wheelhouse["tree_sha256"]
    ):
        raise RuntimeAdmissionError("wheelhouse changed before admission completed")
    return {
        "runtime": {
            "tree": runtime_summary,
            "packages": packages,
            "locked_project": project,
            "python": {
                "version": sys.version,
                "version_info": list(sys.version_info[:3]),
                "implementation": sys.implementation.name,
                "cache_tag": sys.implementation.cache_tag,
                "isolated_mode": bool(sys.flags.isolated),
                "executable": python_binding,
            },
        },
        "wheelhouse": {**wheelhouse, "installed_package_binding": wheel_package_binding},
        "bindings": {
            "admission_utility": utility_source,
            "pyproject": pyproject_binding,
            "lock": lock_binding,
            "sources": source_bindings,
            "executables": executable_bindings,
        },
        "hardware": hardware,
        "benchmark": benchmark,
        "scheduler_lock": scheduler,
    }


def validate_config_shape(config: Any) -> dict[str, Any]:
    required = {
        "expected_main_drive_device",
        "runtime_root",
        "expected_runtime_tree_sha256",
        "wheelhouse_root",
        "expected_wheelhouse_tree_sha256",
        "python_executable",
        "pyproject",
        "lock",
        "expected_admission_source_sha256",
        "sources",
        "executables",
        "required_packages",
        "hardware",
        "benchmark",
        "scheduler_evidence",
        "thresholds",
    }
    if not isinstance(config, dict) or set(config) != required:
        raise RuntimeAdmissionError("receipt configuration shape is invalid")
    for key in ("python_executable", "pyproject", "lock", "benchmark", "scheduler_evidence"):
        value = config[key]
        if not isinstance(value, dict) or set(value) != {"path", "expected_sha256"}:
            raise RuntimeAdmissionError(f"configuration {key} binding is invalid")
    if not isinstance(config["hardware"], dict) or set(config["hardware"]) != {
        "expected_gpu_uuid",
        "device_index",
        "expected_driver_version",
        "expected_compute_type",
        "minimum_total_vram_bytes",
        "device_nodes",
    }:
        raise RuntimeAdmissionError("configuration hardware shape is invalid")
    if not isinstance(config["thresholds"], dict) or set(config["thresholds"]) != {
        "minimum_completed_cases",
        "minimum_total_audio_seconds",
        "minimum_repetitions",
        "maximum_failed_cases",
        "maximum_p95_real_time_factor",
        "maximum_peak_process_vram_bytes",
        "minimum_vram_reserve_bytes",
        "additional_metric_thresholds",
    }:
        raise RuntimeAdmissionError("configuration threshold shape is invalid")
    for key in ("sources", "executables", "required_packages"):
        if not isinstance(config[key], list):
            raise RuntimeAdmissionError(f"configuration {key} must be a list")
    if not isinstance(config["hardware"]["device_nodes"], list) or not config["hardware"]["device_nodes"]:
        raise RuntimeAdmissionError("at least one GPU device node is required")
    return config


def build_semantic(config: dict[str, Any], receipt_path: Path, admitted_at: str) -> dict[str, Any]:
    config = validate_config_shape(config)
    evidence = collect_evidence(config)
    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": "admitted",
        "admitted_at": parse_utc(admitted_at, "admission timestamp"),
        "receipt_path": str(receipt_path),
        "configuration": config,
        "evidence": evidence,
        "policy": {
            "private_only": True,
            "network_access_performed": False,
            "inference_performed": False,
            "corpus_media_processed": False,
            "catalogue_mutated": False,
            "identity_authority": "none",
            "publication_authority": "none",
            "wiki_authority": "none",
            "runtime_admission_only": True,
        },
    }


def atomic_private_receipt(path: Path, body: bytes, expected_device: int) -> None:
    if not path.is_absolute():
        raise RuntimeAdmissionError("receipt output must be absolute")
    parent = path.parent
    parent_binding = directory_binding(parent, "receipt parent", expected_device)
    if parent_binding["mode"] != 0o700:
        raise RuntimeAdmissionError("receipt parent must be owner-private mode 0700")
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"refusing to replace receipt: {path}")
    parent_descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    temporary_name = f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    descriptor: int | None = None
    try:
        descriptor = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o400,
            dir_fd=parent_descriptor,
        )
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.fchmod(descriptor, 0o400)
        os.link(
            temporary_name,
            path.name,
            src_dir_fd=parent_descriptor,
            dst_dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        os.unlink(temporary_name, dir_fd=parent_descriptor)
        os.fsync(parent_descriptor)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            os.unlink(temporary_name, dir_fd=parent_descriptor)
        except FileNotFoundError:
            pass
        os.close(parent_descriptor)


def config_from_args(args: argparse.Namespace) -> dict[str, Any]:
    def reference(values: list[str]) -> dict[str, str]:
        name, path, digest = values
        return {"name": name, "path": path, "expected_sha256": digest}

    additional = [
        {"metric": metric, "operator": operator, "value": float(value)}
        for metric, operator, value in (args.metric_threshold or [])
    ]
    return {
        "expected_main_drive_device": args.expected_main_drive_device,
        "runtime_root": str(args.runtime_root),
        "expected_runtime_tree_sha256": args.expected_runtime_tree_sha256,
        "wheelhouse_root": str(args.wheelhouse_root),
        "expected_wheelhouse_tree_sha256": args.expected_wheelhouse_tree_sha256,
        "python_executable": {"path": str(args.python_executable), "expected_sha256": args.expected_python_executable_sha256},
        "pyproject": {"path": str(args.pyproject), "expected_sha256": args.expected_pyproject_sha256},
        "lock": {"path": str(args.lock), "expected_sha256": args.expected_lock_sha256},
        "expected_admission_source_sha256": args.expected_admission_source_sha256,
        "sources": [reference(value) for value in args.source],
        "executables": [reference(value) for value in args.executable],
        "required_packages": args.required_package or [],
        "hardware": {
            "expected_gpu_uuid": args.expected_gpu_uuid,
            "device_index": args.device_index,
            "expected_driver_version": args.expected_driver_version,
            "expected_compute_type": args.expected_compute_type,
            "minimum_total_vram_bytes": args.minimum_total_vram_bytes,
            "device_nodes": [str(path) for path in args.device_node],
        },
        "benchmark": {"path": str(args.benchmark), "expected_sha256": args.expected_benchmark_sha256},
        "scheduler_evidence": {
            "path": str(args.scheduler_evidence),
            "expected_sha256": args.expected_scheduler_evidence_sha256,
        },
        "thresholds": {
            "minimum_completed_cases": args.minimum_completed_cases,
            "minimum_total_audio_seconds": args.minimum_total_audio_seconds,
            "minimum_repetitions": args.minimum_repetitions,
            "maximum_failed_cases": args.maximum_failed_cases,
            "maximum_p95_real_time_factor": args.maximum_p95_real_time_factor,
            "maximum_peak_process_vram_bytes": args.maximum_peak_process_vram_bytes,
            "minimum_vram_reserve_bytes": args.minimum_vram_reserve_bytes,
            "additional_metric_thresholds": additional,
        },
    }


def produce(args: argparse.Namespace) -> dict[str, Any]:
    output = absolute_path(args.output, "receipt output")
    config = config_from_args(args)
    expected_device = strict_int(config["expected_main_drive_device"], "expected main-drive device", 1)
    runtime_root = absolute_path(config["runtime_root"], "runtime root")
    wheelhouse_root = absolute_path(config["wheelhouse_root"], "wheelhouse root")
    if is_within(output, runtime_root) or is_within(output, wheelhouse_root):
        raise RuntimeAdmissionError("receipt output must be outside hashed runtime and wheelhouse trees")
    for input_path in (Path(config["benchmark"]["path"]), Path(config["scheduler_evidence"]["path"])):
        if output == input_path:
            raise RuntimeAdmissionError("receipt output may not replace an evidence input")
    semantic = build_semantic(config, output, utc_now())
    identity = sha256_bytes(canonical_bytes(semantic))
    receipt = {
        **semantic,
        "identity_sha256": identity,
        "receipt_id": f"gpurtadmit_{identity[:32]}",
    }
    body = pretty_bytes(receipt)
    atomic_private_receipt(output, body, expected_device)
    binding = file_binding(output, "runtime-admission receipt", sha256_bytes(body), expected_device=expected_device)
    if binding["mode"] != 0o400 or binding["link_count"] != 1:
        raise RuntimeAdmissionError("receipt was not sealed mode 0400 with one link")
    return receipt


def validate_receipt(path_value: str | Path, expected_receipt_sha256: str) -> dict[str, Any]:
    path = absolute_path(path_value, "receipt")
    binding = file_binding(path, "runtime-admission receipt", expected_receipt_sha256)
    if binding["mode"] != 0o400 or binding["link_count"] != 1 or binding["requested_symlink"]:
        raise RuntimeAdmissionError("receipt must be a direct owner-controlled mode-0400 single-link file")
    body, _ = stable_file_bytes(Path(binding["resolved_path"]), "runtime-admission receipt", MAX_JSON_BYTES)
    receipt = load_json_bytes(body, "runtime-admission receipt")
    if not isinstance(receipt, dict) or body != pretty_bytes(receipt):
        raise RuntimeAdmissionError("receipt is not canonical pretty JSON plus one newline")
    required = {
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
    }
    if set(receipt) != required:
        raise RuntimeAdmissionError("receipt has an unexpected shape")
    if (
        receipt["kind"] != KIND
        or receipt["schema_version"] != SCHEMA_VERSION
        or receipt["implementation_version"] != IMPLEMENTATION_VERSION
        or receipt["status"] != "admitted"
        or receipt["receipt_path"] != str(path)
    ):
        raise RuntimeAdmissionError("receipt header or path binding is invalid")
    semantic = {key: value for key, value in receipt.items() if key not in {"identity_sha256", "receipt_id"}}
    identity = sha256_bytes(canonical_bytes(semantic))
    if receipt["identity_sha256"] != identity or receipt["receipt_id"] != f"gpurtadmit_{identity[:32]}":
        raise RuntimeAdmissionError("receipt semantic identity or ID is invalid")
    config = validate_config_shape(receipt["configuration"])
    expected_device = strict_int(config["expected_main_drive_device"], "expected main-drive device", 1)
    if binding["device"] != expected_device:
        raise RuntimeAdmissionError("receipt is not on the admitted main-drive device")
    rebuilt = build_semantic(config, path, receipt["admitted_at"])
    if rebuilt != semantic:
        raise RuntimeAdmissionError("receipt does not offline-replay from current exact evidence")
    return receipt


def add_admit_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--runtime-root", required=True, type=Path)
    parser.add_argument("--expected-runtime-tree-sha256", required=True)
    parser.add_argument("--wheelhouse-root", required=True, type=Path)
    parser.add_argument("--expected-wheelhouse-tree-sha256", required=True)
    parser.add_argument("--expected-main-drive-device", required=True, type=int)
    parser.add_argument("--python-executable", required=True, type=Path)
    parser.add_argument("--expected-python-executable-sha256", required=True)
    parser.add_argument("--pyproject", required=True, type=Path)
    parser.add_argument("--expected-pyproject-sha256", required=True)
    parser.add_argument("--lock", required=True, type=Path)
    parser.add_argument("--expected-lock-sha256", required=True)
    parser.add_argument("--expected-admission-source-sha256", required=True)
    parser.add_argument(
        "--source",
        required=True,
        action="append",
        nargs=3,
        metavar=("NAME", "ABSOLUTE_PATH", "SHA256"),
        help="repeat for every production adapter/source file",
    )
    parser.add_argument(
        "--executable",
        required=True,
        action="append",
        nargs=3,
        metavar=("NAME", "ABSOLUTE_PATH", "SHA256"),
        help="repeat for every production executable",
    )
    parser.add_argument("--required-package", action="append", help="additional exact NAME==VERSION requirement")
    parser.add_argument("--expected-gpu-uuid", required=True)
    parser.add_argument("--device-index", type=int, default=0)
    parser.add_argument("--expected-driver-version", required=True)
    parser.add_argument("--expected-compute-type", required=True)
    parser.add_argument("--minimum-total-vram-bytes", required=True, type=int)
    parser.add_argument("--device-node", required=True, action="append", type=Path)
    parser.add_argument("--benchmark", required=True, type=Path)
    parser.add_argument("--expected-benchmark-sha256", required=True)
    parser.add_argument("--scheduler-evidence", required=True, type=Path)
    parser.add_argument("--expected-scheduler-evidence-sha256", required=True)
    parser.add_argument("--minimum-completed-cases", required=True, type=int)
    parser.add_argument("--minimum-total-audio-seconds", required=True, type=float)
    parser.add_argument("--minimum-repetitions", required=True, type=int)
    parser.add_argument("--maximum-failed-cases", required=True, type=int)
    parser.add_argument("--maximum-p95-real-time-factor", required=True, type=float)
    parser.add_argument("--maximum-peak-process-vram-bytes", required=True, type=int)
    parser.add_argument("--minimum-vram-reserve-bytes", required=True, type=int)
    parser.add_argument(
        "--metric-threshold",
        action="append",
        nargs=3,
        metavar=("METRIC", "OPERATOR", "VALUE"),
        help="optional numeric benchmark condition; OPERATOR is le, lt, ge, gt, or eq",
    )
    parser.add_argument("--output", required=True, type=Path)


def argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    admit = subparsers.add_parser("admit", help="verify exact evidence and atomically seal a receipt")
    add_admit_arguments(admit)
    validate = subparsers.add_parser("validate", help="offline-replay an existing sealed receipt")
    validate.add_argument("--receipt", required=True, type=Path)
    validate.add_argument("--expected-receipt-sha256", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
    os.environ["DO_NOT_TRACK"] = "1"
    sys.dont_write_bytecode = True
    args = argument_parser().parse_args(argv)
    try:
        if args.command == "admit":
            receipt = produce(args)
            receipt_path = Path(receipt["receipt_path"])
            receipt_sha256 = sha256_bytes(receipt_path.read_bytes())
            mode = "admit"
        else:
            receipt = validate_receipt(args.receipt, args.expected_receipt_sha256)
            receipt_path = Path(receipt["receipt_path"])
            receipt_sha256 = args.expected_receipt_sha256
            mode = "validate"
        summary = {
            "schema_version": SCHEMA_VERSION,
            "status": "validated" if mode == "validate" else "admitted",
            "mode": mode,
            "receipt_path": str(receipt_path),
            "receipt_sha256": receipt_sha256,
            "receipt_id": receipt["receipt_id"],
            "identity_sha256": receipt["identity_sha256"],
            "gpu_uuid": receipt["evidence"]["hardware"]["uuid"],
            "runtime_tree_sha256": receipt["evidence"]["runtime"]["tree"]["tree_sha256"],
            "wheelhouse_tree_sha256": receipt["evidence"]["wheelhouse"]["tree_sha256"],
            "benchmark_accepted": receipt["evidence"]["benchmark"]["accepted"],
            "scheduler_lock_accepted": receipt["evidence"]["scheduler_lock"]["accepted"],
            "inference_performed": False,
            "publication_authority": "none",
        }
        sys.stdout.write(json.dumps(summary, sort_keys=True, indent=2) + "\n")
        return 0
    except (RuntimeAdmissionError, OSError, ValueError, KeyError, ImportError) as error:
        failure = {
            "schema_version": SCHEMA_VERSION,
            "status": "failed",
            "error": {"type": type(error).__name__, "message": str(error)},
            "inference_performed": False,
            "publication_authority": "none",
        }
        sys.stderr.write(json.dumps(failure, sort_keys=True, indent=2) + "\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
