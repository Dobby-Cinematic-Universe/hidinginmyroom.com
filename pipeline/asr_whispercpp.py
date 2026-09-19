#!/usr/bin/env python3
"""Strict, offline whisper.cpp ASR adapter for normalized local audio."""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import math
import mimetypes
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
import urllib.parse
import uuid
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

try:
    from . import whispercpp_engine_profiles
except ImportError:  # Direct execution places pipeline/ itself on sys.path.
    import whispercpp_engine_profiles


CONTRACT_VERSION = 1
IMPLEMENTATION_VERSION = "0.3.0"
STAGE = "asr_whispercpp"
MAX_WINDOW_MS = 24 * 60 * 60 * 1_000
MAX_SEGMENT_BOUNDARY_OVERRUN_MS = 30_000
MAX_PROMPT_CHARACTERS = 2_048
MAX_RAW_JSON_BYTES = 512 * 1024 * 1024
MAX_LOG_BYTES = 8 * 1024 * 1024
DESCRIPTOR_EXECUTION_POLICY = "linux_proc_self_fd_retained_verified_v1"
PROC_SELF_FD_ROOT = Path("/proc/self/fd")
JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
LANGUAGE_RE = re.compile(r"^(?:auto|[a-z]{2,3}(?:-[a-z0-9]{2,8})*)$")


class ASRError(RuntimeError):
    """A strict contract, provenance, integrity, or inference failure."""


class InvalidUTF8OutputError(ASRError):
    """whisper.cpp produced non-UTF-8 JSON whose exact bytes were quarantined."""

    def __init__(self, quarantine: dict[str, Any]):
        invalid = quarantine["invalid_utf8"]
        super().__init__(
            "whisper.cpp output-json-full is not strict UTF-8 "
            f"at byte range [{invalid['start_byte']},{invalid['end_byte']}); "
            f"exact bytes quarantined at {quarantine['raw_artifact']['path']}"
        )
        self.quarantine = quarantine


class CommandError(ASRError):
    def __init__(self, command: list[str], returncode: int, stderr: str):
        tail = "\n".join(stderr.splitlines()[-40:])
        super().__init__(
            f"Command exited with status {returncode}: {command[0]}\n{tail}".rstrip()
        )
        self.command = command
        self.returncode = returncode


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def canonical_json_text(value: Any) -> str:
    return canonical_bytes(value).decode("utf-8")


def pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def stable_id(prefix: str, *parts: Any) -> str:
    return f"{prefix}_" + sha256_bytes(canonical_bytes(list(parts)))[:32]


def atomic_write(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("xb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_json(path: Path, value: Any) -> None:
    atomic_write(path, pretty_json(value).encode("utf-8"))


def load_json(path: Path, *, maximum_bytes: int | None = None) -> Any:
    if maximum_bytes is not None and path.stat().st_size > maximum_bytes:
        raise ASRError(f"JSON file exceeds the {maximum_bytes}-byte limit: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except UnicodeDecodeError as error:
        raise ASRError(
            f"Invalid UTF-8 in JSON file {path} at byte range "
            f"[{error.start},{error.end}): {error.reason}"
        ) from error
    except json.JSONDecodeError as error:
        raise ASRError(f"Invalid JSON in {path}: {error}") from error


def parse_json_bytes(body: bytes, label: str) -> Any:
    """Decode one retained JSON input without replacement or ignored bytes."""

    try:
        text = body.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise ASRError(
            f"Invalid UTF-8 in {label} at byte range "
            f"[{error.start},{error.end}): {error.reason}"
        ) from error
    try:
        return json.loads(text)
    except json.JSONDecodeError as error:
        raise ASRError(f"Invalid JSON in {label}: {error}") from error


def ensure_private_directory(path: Path, label: str) -> None:
    """Create or validate one owner-only, non-symlink directory."""

    path.mkdir(parents=True, mode=0o700, exist_ok=True)
    observed = path.lstat()
    if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
        raise ASRError(f"{label} must be a non-symlink directory")
    if stat.S_IMODE(observed.st_mode) & 0o077:
        raise ASRError(f"{label} must not grant group or world permissions")


def sync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def stable_stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        stat.S_IFMT(value.st_mode),
        stat.S_IMODE(value.st_mode),
        value.st_nlink,
    )


def public_file_stat(value: os.stat_result) -> dict[str, int]:
    """Project an internal race fingerprint into the stable result-schema shape."""

    return {
        "device": value.st_dev,
        "inode": value.st_ino,
        "byte_count": value.st_size,
        "mtime_ns": value.st_mtime_ns,
    }


def descriptor_sha256(descriptor: int) -> tuple[str, int]:
    """Hash a retained regular-file descriptor without changing its shared offset."""

    digest = hashlib.sha256()
    offset = 0
    while chunk := os.pread(descriptor, 8 * 1024 * 1024, offset):
        digest.update(chunk)
        offset += len(chunk)
    return digest.hexdigest(), offset


def require_descriptor_transport(label: str) -> None:
    """Fail closed before descriptor hashing or child execution off Linux procfs."""

    if (
        not sys.platform.startswith("linux")
        or not hasattr(os, "pread")
        or not PROC_SELF_FD_ROOT.is_dir()
    ):
        raise ASRError(
            f"{label} requires a mounted Linux /proc/self/fd descriptor transport"
        )


def descriptor_bytes(
    retained: "RetainedFile", *, maximum_bytes: int, label: str
) -> bytes:
    """Read bounded bytes from a retained descriptor and reject any concurrent change."""

    if retained.identity[2] > maximum_bytes:
        raise ASRError(f"{label} exceeds the {maximum_bytes}-byte limit")
    chunks: list[bytes] = []
    offset = 0
    while offset <= maximum_bytes:
        chunk = os.pread(
            retained.descriptor,
            min(1024 * 1024, maximum_bytes + 1 - offset),
            offset,
        )
        if not chunk:
            break
        chunks.append(chunk)
        offset += len(chunk)
    body = b"".join(chunks)
    descriptor_after = os.fstat(retained.descriptor)
    path_after = retained.path.lstat()
    if (
        len(body) != retained.identity[2]
        or len(body) > maximum_bytes
        or stable_stat_identity(descriptor_after) != retained.identity
        or stable_stat_identity(path_after) != retained.identity
    ):
        raise ASRError(f"{label} changed while being read")
    return body


def proc_descriptor_path(
    descriptor: int,
    identity: tuple[int, ...],
    label: str,
    *,
    executable: bool = False,
) -> str:
    """Resolve the Linux child-visible path for one retained verified descriptor."""

    require_descriptor_transport(label)
    path = PROC_SELF_FD_ROOT / str(descriptor)
    try:
        observed = path.stat()
    except OSError as error:
        raise ASRError(f"{label} descriptor transport is unavailable: {error}") from error
    if stable_stat_identity(observed) != identity:
        raise ASRError(f"{label} descriptor transport resolves to a different file")
    if executable and not os.access(path, os.X_OK):
        raise ASRError(f"{label} retained descriptor is not executable")
    return str(path)


@dataclass
class RetainedFile:
    path: Path
    descriptor: int
    identity: tuple[int, ...]
    observation: dict[str, Any]
    proc_path: str


@contextmanager
def retained_verified_file(
    path: Path,
    expected_sha256: str,
    label: str,
    *,
    executable: bool = False,
) -> Iterator[RetainedFile]:
    """Open, hash, and retain one exact inode for child execution and post-checking."""

    require_descriptor_transport(label)
    try:
        before = path.lstat()
    except OSError as error:
        raise ASRError(f"{label} cannot be inspected: {error}") from error
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ASRError(f"{label} must be a regular non-symlink file")
    if before.st_size == 0:
        raise ASRError(f"{label} may not be empty")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        identity = stable_stat_identity(opened)
        if not stat.S_ISREG(opened.st_mode) or identity != stable_stat_identity(before):
            raise ASRError(f"{label} changed while being opened")
        digest, byte_count = descriptor_sha256(descriptor)
        after_hash = os.fstat(descriptor)
        path_after_hash = path.lstat()
        if (
            byte_count != opened.st_size
            or stable_stat_identity(after_hash) != identity
            or stable_stat_identity(path_after_hash) != identity
        ):
            raise ASRError(f"{label} changed while being hashed")
        if digest != expected_sha256:
            raise ASRError(
                f"{label} SHA-256 mismatch: expected {expected_sha256}, observed {digest}"
            )
        proc_path = proc_descriptor_path(
            descriptor,
            identity,
            label,
            executable=executable,
        )
        observation = {
            "path": str(path),
            "sha256": digest,
            "byte_count": byte_count,
            "stat_before": public_file_stat(opened),
            "stat_after": None,
            "unchanged": None,
        }
        yield RetainedFile(path, descriptor, identity, observation, proc_path)
    finally:
        os.close(descriptor)


def verify_retained_file(retained: RetainedFile, label: str) -> None:
    """Rehash the retained inode and prove its logical path never changed or returned."""

    digest, byte_count = descriptor_sha256(retained.descriptor)
    descriptor_after = os.fstat(retained.descriptor)
    try:
        path_after = retained.path.lstat()
    except OSError as error:
        raise ASRError(f"{label} disappeared during ASR execution: {error}") from error
    if (
        stable_stat_identity(descriptor_after) != retained.identity
        or stable_stat_identity(path_after) != retained.identity
        or byte_count != retained.identity[2]
        or digest != retained.observation["sha256"]
    ):
        raise ASRError(f"{label} changed during ASR execution")
    retained.observation["stat_after"] = public_file_stat(descriptor_after)
    retained.observation["unchanged"] = True


def stable_file_bytes(
    path: Path,
    *,
    maximum_bytes: int,
    label: str,
    exact_mode: int | None = None,
    require_single_link: bool = False,
) -> bytes:
    before = path.lstat()
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ASRError(f"{label} must be a regular non-symlink file")
    if exact_mode is not None and stat.S_IMODE(before.st_mode) != exact_mode:
        raise ASRError(f"{label} must have mode {exact_mode:04o}")
    if require_single_link and before.st_nlink != 1:
        raise ASRError(f"{label} must have exactly one hard link")
    if before.st_size > maximum_bytes:
        raise ASRError(f"{label} exceeds the {maximum_bytes}-byte limit")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or stable_stat_identity(opened) != stable_stat_identity(before)
        ):
            raise ASRError(f"{label} changed while being opened")
        chunks: list[bytes] = []
        remaining = maximum_bytes + 1
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        body = b"".join(chunks)
        after_fd = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    after_path = path.lstat()
    expected = stable_stat_identity(before)
    if (
        len(body) != before.st_size
        or len(body) > maximum_bytes
        or stable_stat_identity(after_fd) != expected
        or stable_stat_identity(after_path) != expected
    ):
        raise ASRError(f"{label} changed while being read")
    return body


def verify_quarantine_bundle(
    bundle: Path,
    *,
    raw_body: bytes,
    receipt_body: bytes,
) -> None:
    observed = bundle.lstat()
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or stat.S_IMODE(observed.st_mode) != 0o500
    ):
        raise ASRError("existing quarantine bundle is not an immutable private directory")
    if {item.name for item in bundle.iterdir()} != {"raw-output.bin", "receipt.json"}:
        raise ASRError("existing quarantine bundle has missing or extra entries")
    for name, expected in (("raw-output.bin", raw_body), ("receipt.json", receipt_body)):
        path = bundle / name
        item = path.lstat()
        if (
            stat.S_ISLNK(item.st_mode)
            or not stat.S_ISREG(item.st_mode)
            or stat.S_IMODE(item.st_mode) != 0o400
            or item.st_nlink != 1
        ):
            raise ASRError(f"existing quarantine {name} failed exact immutable replay")
        observed = stable_file_bytes(
            path,
            maximum_bytes=len(expected),
            label=f"existing quarantine {name}",
            exact_mode=0o400,
            require_single_link=True,
        )
        if observed != expected:
            raise ASRError(f"existing quarantine {name} failed exact immutable replay")


def quarantine_invalid_utf8_output(
    *,
    raw_body: bytes,
    decode_error: UnicodeDecodeError,
    output_root: Path,
    work_order: dict[str, Any],
    work_order_sha256: str,
    recipe_id: str,
    result_key: str,
    input_sha256: str,
    engine_sha256: str,
) -> dict[str, Any]:
    """Atomically retain exact invalid engine bytes and a deterministic receipt."""

    raw_sha256 = sha256_bytes(raw_body)
    invalid_utf8 = {
        "decoder": "utf-8-strict",
        "start_byte": decode_error.start,
        "end_byte": decode_error.end,
        "reason": decode_error.reason,
    }
    lineage = {
        "job_id": work_order["job_id"],
        "work_order_sha256": work_order_sha256,
        "recipe_id": recipe_id,
        "result_key": result_key,
        "adapter_implementation_version": IMPLEMENTATION_VERSION,
        "input_sha256": input_sha256,
        "input_artifact_id": work_order["input"]["artifact_id"],
        "parent_processing_run_id": work_order["input"]["parent_processing_run_id"],
        "engine_sha256": engine_sha256,
        "engine_version": work_order["engine"]["version_label"],
        "engine_revision": work_order["engine"]["build"]["revision"],
    }
    failure_identity = {
        "receipt_kind": "whispercpp_output_json_full_invalid_utf8",
        "raw_sha256": raw_sha256,
        "raw_byte_count": len(raw_body),
        "invalid_utf8": invalid_utf8,
        "lineage": lineage,
    }
    failure_key = sha256_bytes(canonical_bytes(failure_identity))
    quarantine_root = output_root / "quarantine" / "whispercpp-output-json-full"
    raw_root = quarantine_root / "sha256" / raw_sha256[:2] / raw_sha256
    bundle = raw_root / failure_key
    raw_path = bundle / "raw-output.bin"
    receipt_path = bundle / "receipt.json"
    raw_artifact = {
        "artifact_kind": "whispercpp_output_json_full_quarantine",
        "path": str(raw_path),
        "storage_uri": raw_path.as_uri(),
        "byte_count": len(raw_body),
        "sha256": raw_sha256,
        "visibility": "private",
    }
    receipt = {
        "schema_version": 1,
        "receipt_kind": "whispercpp_output_json_full_invalid_utf8",
        "failure_key": failure_key,
        "invalid_utf8": invalid_utf8,
        "raw_artifact": raw_artifact,
        "lineage": lineage,
        "safety": {
            "exact_raw_bytes_preserved": True,
            "decoded_text_emitted": False,
            "invalid_bytes_ignored_or_replaced": False,
            "catalog_writes": False,
            "publication_authority": "none",
            "visibility": "private",
        },
    }
    receipt_body = pretty_json(receipt).encode("utf-8")
    ensure_private_directory(output_root, "ASR output root")
    current = output_root
    for component in (
        "quarantine",
        "whispercpp-output-json-full",
        "sha256",
        raw_sha256[:2],
        raw_sha256,
    ):
        current = current / component
        ensure_private_directory(current, "ASR quarantine directory")
    if bundle.exists():
        verify_quarantine_bundle(bundle, raw_body=raw_body, receipt_body=receipt_body)
    else:
        staging = raw_root / f".{failure_key}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
        try:
            staging.mkdir(mode=0o700)
            atomic_write(staging / "raw-output.bin", raw_body)
            atomic_write(staging / "receipt.json", receipt_body)
            os.chmod(staging / "raw-output.bin", 0o400)
            os.chmod(staging / "receipt.json", 0o400)
            sync_directory(staging)
            os.chmod(staging, 0o500)
            try:
                os.rename(staging, bundle)
                sync_directory(raw_root)
            except OSError as error:
                if error.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                    raise
                os.chmod(staging, 0o700)
                for child in staging.iterdir():
                    os.chmod(child, 0o600)
                shutil.rmtree(staging)
                verify_quarantine_bundle(
                    bundle, raw_body=raw_body, receipt_body=receipt_body
                )
        finally:
            if staging.exists():
                os.chmod(staging, 0o700)
                for child in staging.iterdir():
                    os.chmod(child, 0o600)
                shutil.rmtree(staging)
        verify_quarantine_bundle(bundle, raw_body=raw_body, receipt_body=receipt_body)
    return {
        "failure_key": failure_key,
        "receipt_kind": receipt["receipt_kind"],
        "receipt_path": str(receipt_path),
        "receipt_sha256": sha256_bytes(receipt_body),
        "invalid_utf8": invalid_utf8,
        "raw_artifact": raw_artifact,
    }


def require_exact_keys(value: dict[str, Any], label: str, keys: set[str]) -> None:
    missing = sorted(keys - set(value))
    unexpected = sorted(set(value) - keys)
    if missing:
        raise ASRError(f"{label} is missing keys: {', '.join(missing)}")
    if unexpected:
        raise ASRError(f"{label} has unsupported keys: {', '.join(unexpected)}")


def bounded_text(value: Any, label: str, maximum: int = 2_000) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise ASRError(f"{label} must be a non-empty bounded string")
    return value


def identifier(value: Any, label: str) -> str:
    result = bounded_text(value, label, 256)
    if not ID_RE.fullmatch(result):
        raise ASRError(f"{label} contains unsupported characters")
    return result


def sha256_value(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise ASRError(f"{label} must be a lowercase SHA-256")
    return value


def integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ASRError(f"{label} must be an integer")
    if not minimum <= value <= maximum:
        raise ASRError(f"{label} must be between {minimum} and {maximum}")
    return value


def number(value: Any, label: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ASRError(f"{label} must be a number")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise ASRError(f"{label} must be between {minimum} and {maximum}")
    return result


def absolute_file(value: Any, label: str, *, executable: bool = False) -> Path:
    if not isinstance(value, str) or not value or "://" in value:
        raise ASRError(f"{label} must be an absolute local file path")
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise ASRError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=True)
    except FileNotFoundError as error:
        raise ASRError(f"{label} does not exist: {path}") from error
    if not resolved.is_file():
        raise ASRError(f"{label} must identify a regular file")
    if executable and not os.access(resolved, os.X_OK):
        raise ASRError(f"{label} is not executable")
    return resolved


def absolute_output_root(value: Any) -> Path:
    if not isinstance(value, str) or not value or "://" in value:
        raise ASRError("output.root must be an absolute local directory path")
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise ASRError("output.root must be absolute")
    resolved = path.resolve(strict=False)
    if resolved == Path("/"):
        raise ASRError("output.root may not be the filesystem root")
    if resolved.exists() and not resolved.is_dir():
        raise ASRError("output.root must identify a directory or a new path")
    for forbidden in (Path("/tmp"), Path("/var/tmp")):
        if resolved == forbidden or forbidden in resolved.parents:
            raise ASRError(f"output.root may not be under {forbidden}")
    return resolved


def validate_string_array(value: Any, label: str, maximum_items: int = 64) -> list[str]:
    if not isinstance(value, list) or len(value) > maximum_items:
        raise ASRError(f"{label} must be an array with at most {maximum_items} items")
    return [bounded_text(item, f"{label}[{index}]", 256) for index, item in enumerate(value)]


def validate_input(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ASRError("input must be a JSON object")
    keys = {
        "path",
        "expected_sha256",
        "media_id",
        "artifact_id",
        "parent_processing_run_id",
    }
    require_exact_keys(raw, "input", keys)
    path = absolute_file(raw["path"], "input.path")
    expected = sha256_value(raw["expected_sha256"], "input.expected_sha256")
    media_id = identifier(raw["media_id"], "input.media_id")
    if media_id != f"media_sha256_{expected}":
        raise ASRError("input.media_id must equal media_sha256_<input.expected_sha256>")
    return {
        "path": str(path),
        "expected_sha256": expected,
        "media_id": media_id,
        "artifact_id": identifier(raw["artifact_id"], "input.artifact_id"),
        "parent_processing_run_id": identifier(
            raw["parent_processing_run_id"], "input.parent_processing_run_id"
        ),
    }


def validate_engine(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ASRError("engine must be a JSON object")
    require_exact_keys(
        raw,
        "engine",
        {
            "executable",
            "expected_sha256",
            "version_label",
            "version_evidence",
            "build",
        },
    )
    build = raw["build"]
    if not isinstance(build, dict):
        raise ASRError("engine.build must be a JSON object")
    require_exact_keys(build, "engine.build", {"repository", "revision", "target", "configuration"})
    if raw["version_evidence"] != "source_revision_plus_executable_sha256":
        raise ASRError(
            "engine.version_evidence must be source_revision_plus_executable_sha256"
        )
    return {
        "executable": str(absolute_file(raw["executable"], "engine.executable", executable=True)),
        "expected_sha256": sha256_value(raw["expected_sha256"], "engine.expected_sha256"),
        "version_label": bounded_text(raw["version_label"], "engine.version_label", 500),
        "version_evidence": bounded_text(
            raw["version_evidence"], "engine.version_evidence", 500
        ),
        "build": {
            "repository": bounded_text(build["repository"], "engine.build.repository", 1_000),
            "revision": bounded_text(build["revision"], "engine.build.revision", 256),
            "target": bounded_text(build["target"], "engine.build.target", 256),
            "configuration": validate_string_array(
                build["configuration"], "engine.build.configuration"
            ),
        },
    }


def validate_model(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ASRError("model must be a JSON object")
    keys = {
        "path",
        "expected_sha256",
        "model_id",
        "name",
        "revision",
        "source",
        "license_label",
    }
    require_exact_keys(raw, "model", keys)
    return {
        "path": str(absolute_file(raw["path"], "model.path")),
        "expected_sha256": sha256_value(raw["expected_sha256"], "model.expected_sha256"),
        "model_id": identifier(raw["model_id"], "model.model_id"),
        "name": bounded_text(raw["name"], "model.name", 500),
        "revision": bounded_text(raw["revision"], "model.revision", 500),
        "source": bounded_text(raw["source"], "model.source", 1_000),
        "license_label": bounded_text(raw["license_label"], "model.license_label", 500),
    }


def validate_window(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ASRError("window must be a JSON object")
    require_exact_keys(raw, "window", {"offset_ms", "duration_ms"})
    duration = raw["duration_ms"]
    return {
        "offset_ms": integer(raw["offset_ms"], "window.offset_ms", 0, MAX_WINDOW_MS),
        "duration_ms": None
        if duration is None
        else integer(duration, "window.duration_ms", 1, MAX_WINDOW_MS),
    }


def validate_inference(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ASRError("inference must be a JSON object")
    keys = {
        "language",
        "threads",
        "translate",
        "split_on_word",
        "best_of",
        "beam_size",
        "max_segment_characters",
        "word_threshold",
        "entropy_threshold",
        "logprob_threshold",
        "no_speech_threshold",
        "temperature",
        "temperature_increment",
        "no_fallback",
        "timeout_seconds",
    }
    require_exact_keys(raw, "inference", keys)
    language = bounded_text(raw["language"], "inference.language", 64).lower()
    if not LANGUAGE_RE.fullmatch(language):
        raise ASRError("inference.language must be 'auto' or a lowercase language tag")
    if raw["translate"] is not False:
        raise ASRError("inference.translate must be false in ASR contract version 1")
    for name in ("split_on_word", "no_fallback"):
        if not isinstance(raw[name], bool):
            raise ASRError(f"inference.{name} must be boolean")
    return {
        "language": language,
        "threads": integer(raw["threads"], "inference.threads", 1, 32),
        "translate": False,
        "split_on_word": raw["split_on_word"],
        "best_of": integer(raw["best_of"], "inference.best_of", 1, 20),
        "beam_size": integer(raw["beam_size"], "inference.beam_size", 1, 20),
        "max_segment_characters": integer(
            raw["max_segment_characters"],
            "inference.max_segment_characters",
            0,
            10_000,
        ),
        "word_threshold": number(raw["word_threshold"], "inference.word_threshold", 0, 1),
        "entropy_threshold": number(
            raw["entropy_threshold"], "inference.entropy_threshold", 0, 100
        ),
        "logprob_threshold": number(
            raw["logprob_threshold"], "inference.logprob_threshold", -100, 0
        ),
        "no_speech_threshold": number(
            raw["no_speech_threshold"], "inference.no_speech_threshold", 0, 1
        ),
        "temperature": number(raw["temperature"], "inference.temperature", 0, 1),
        "temperature_increment": number(
            raw["temperature_increment"], "inference.temperature_increment", 0, 1
        ),
        "no_fallback": raw["no_fallback"],
        "timeout_seconds": integer(
            raw["timeout_seconds"], "inference.timeout_seconds", 1, 86_400
        ),
    }


def validate_glossary_reference(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ASRError("glossary must be null or a JSON object")
    require_exact_keys(raw, "glossary", {"path", "expected_sha256"})
    return {
        "path": str(absolute_file(raw["path"], "glossary.path")),
        "expected_sha256": sha256_value(
            raw["expected_sha256"], "glossary.expected_sha256"
        ),
    }


def validate_catalog_context(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ASRError("catalog_context must be null or a JSON object")
    require_exact_keys(raw, "catalog_context", {"recording_id", "rendition_id"})
    rendition = raw["rendition_id"]
    return {
        "recording_id": identifier(raw["recording_id"], "catalog_context.recording_id"),
        "rendition_id": None
        if rendition is None
        else identifier(rendition, "catalog_context.rendition_id"),
    }


def validate_work_order(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ASRError("work order must be a JSON object")
    keys = {
        "schema_version",
        "job_id",
        "input",
        "engine",
        "model",
        "window",
        "inference",
        "glossary",
        "catalog_context",
        "output",
    }
    require_exact_keys(raw, "work order", keys)
    if raw["schema_version"] != CONTRACT_VERSION:
        raise ASRError(f"schema_version must be {CONTRACT_VERSION}")
    job_id = raw["job_id"]
    if not isinstance(job_id, str) or not JOB_ID_RE.fullmatch(job_id):
        raise ASRError("job_id contains unsupported characters or is too long")
    output = raw["output"]
    if not isinstance(output, dict):
        raise ASRError("output must be a JSON object")
    require_exact_keys(output, "output", {"root"})
    result = {
        "schema_version": CONTRACT_VERSION,
        "job_id": job_id,
        "input": validate_input(raw["input"]),
        "engine": validate_engine(raw["engine"]),
        "model": validate_model(raw["model"]),
        "window": validate_window(raw["window"]),
        "inference": validate_inference(raw["inference"]),
        "glossary": validate_glossary_reference(raw["glossary"]),
        "catalog_context": validate_catalog_context(raw["catalog_context"]),
        "output": {"root": str(absolute_output_root(output["root"]))},
    }
    output_root = Path(result["output"]["root"])
    owned_inputs = [
        Path(result["input"]["path"]),
        Path(result["engine"]["executable"]),
        Path(result["model"]["path"]),
    ]
    if result["glossary"]:
        owned_inputs.append(Path(result["glossary"]["path"]))
    for path in owned_inputs:
        if path == output_root or output_root in path.parents:
            raise ASRError("input, model, executable, and glossary must be outside output.root")
    return result


def minimal_environment(home: Path) -> dict[str, str]:
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(home),
        "XDG_CONFIG_HOME": str(home / ".config"),
        "XDG_CACHE_HOME": str(home / ".cache"),
        "LC_ALL": "C",
        "LANG": "C",
        "TZ": "UTC",
        "NO_PROXY": "*",
        "no_proxy": "*",
    }


def bounded_output(value: str) -> str:
    encoded = value.encode("utf-8", errors="replace")
    if len(encoded) <= MAX_LOG_BYTES:
        return value
    suffix = b"\n[output truncated by ASR adapter]\n"
    return (encoded[: MAX_LOG_BYTES - len(suffix)] + suffix).decode(
        "utf-8", errors="replace"
    )


def terminate_group(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=3)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    process.wait(timeout=3)


def run_command(
    command: list[str],
    *,
    environment: dict[str, str],
    timeout_seconds: int,
    pass_fds: tuple[int, ...] = (),
) -> subprocess.CompletedProcess[str]:
    process = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=environment,
        start_new_session=True,
        pass_fds=pass_fds,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as error:
        terminate_group(process)
        raise ASRError(
            f"Command exceeded the {timeout_seconds}-second timeout: {command[0]}"
        ) from error
    stdout = bounded_output(stdout)
    stderr = bounded_output(stderr)
    if process.returncode != 0:
        raise CommandError(command, process.returncode, stderr)
    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)


def require_ffprobe() -> Path:
    value = shutil.which("ffprobe")
    if not value:
        raise ASRError("ffprobe is required on PATH")
    return Path(value).resolve()


def probe_audio(
    path: Path, retained_input: RetainedFile
) -> tuple[dict[str, Any], list[str], list[str]]:
    ffprobe = require_ffprobe()
    logical_command = [
        str(ffprobe),
        "-v",
        "error",
        "-select_streams",
        "a:0",
        "-show_entries",
        "format=duration,format_name:stream=index,codec_name,sample_fmt,sample_rate,channels,channel_layout,duration",
        "-of",
        "json",
        str(path),
    ]
    command = [*logical_command[:-1], retained_input.proc_path]
    completed = run_command(
        command,
        environment=minimal_environment(Path("/nonexistent-home")),
        timeout_seconds=120,
        pass_fds=(retained_input.descriptor,),
    )
    try:
        raw = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise ASRError("ffprobe returned invalid JSON") from error
    streams = raw.get("streams") if isinstance(raw, dict) else None
    if not isinstance(streams, list) or len(streams) != 1 or not isinstance(streams[0], dict):
        raise ASRError("input must contain exactly one selected primary audio stream")
    stream = streams[0]
    format_raw = raw.get("format") if isinstance(raw.get("format"), dict) else {}
    try:
        duration_ms = round(float(format_raw.get("duration") or stream.get("duration")) * 1_000)
    except (TypeError, ValueError) as error:
        raise ASRError("normalized input audio must have a finite duration") from error
    normalized = {
        "ffprobe_path": str(ffprobe),
        "codec_name": stream.get("codec_name"),
        "sample_format": stream.get("sample_fmt"),
        "sample_rate_hz": int(stream["sample_rate"])
        if str(stream.get("sample_rate", "")).isdigit()
        else None,
        "channels": stream.get("channels"),
        "channel_layout": stream.get("channel_layout"),
        "format_name": format_raw.get("format_name"),
        "duration_ms": duration_ms,
    }
    expected = {
        "codec_name": "flac",
        "sample_format": "s16",
        "sample_rate_hz": 16_000,
        "channels": 1,
    }
    for key, value in expected.items():
        if normalized[key] != value:
            raise ASRError(
                f"input is not normalized 16 kHz mono s16 FLAC: {key}={normalized[key]!r}"
            )
    if duration_ms <= 0 or duration_ms > MAX_WINDOW_MS:
        raise ASRError("input duration is outside the supported 1 ms to 24 hour range")
    return normalized, logical_command, command


def validate_glossary_document(raw: Any, requested_language: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ASRError("glossary document must be a JSON object")
    keys = {"schema_version", "glossary_revision_id", "revision", "language", "terms"}
    require_exact_keys(raw, "glossary document", keys)
    if raw["schema_version"] != 1:
        raise ASRError("glossary schema_version must be 1")
    language = bounded_text(raw["language"], "glossary.language", 64).lower()
    if not LANGUAGE_RE.fullmatch(language) or language == "auto":
        raise ASRError("glossary.language must be a concrete lowercase language tag")
    if requested_language != "auto" and language != requested_language:
        raise ASRError("glossary.language must match inference.language")
    terms_raw = raw["terms"]
    if not isinstance(terms_raw, list) or not 1 <= len(terms_raw) <= 128:
        raise ASRError("glossary.terms must contain between 1 and 128 terms")
    terms: list[str] = []
    seen: set[str] = set()
    for index, value in enumerate(terms_raw):
        term = bounded_text(value, f"glossary.terms[{index}]", 80).strip()
        if not term or any(ord(character) < 32 for character in term):
            raise ASRError("glossary terms may not contain control characters")
        folded = term.casefold()
        if folded in seen:
            raise ASRError("glossary terms must be unique ignoring case")
        seen.add(folded)
        terms.append(term)
    prompt = "Vocabulary terms, spelling only: " + ", ".join(terms)
    if len(prompt) > MAX_PROMPT_CHARACTERS:
        raise ASRError(f"constructed neutral glossary prompt exceeds {MAX_PROMPT_CHARACTERS} characters")
    return {
        "schema_version": 1,
        "glossary_revision_id": identifier(
            raw["glossary_revision_id"], "glossary.glossary_revision_id"
        ),
        "revision": bounded_text(raw["revision"], "glossary.revision", 256),
        "language": language,
        "terms": terms,
        "prompt": prompt,
        "prompt_sha256": sha256_bytes(prompt.encode("utf-8")),
    }


def reject_execution_ineligible_engine(executable_observation: dict[str, Any]) -> None:
    """Keep historical engine pins parseable without permitting their execution."""

    try:
        profile = whispercpp_engine_profiles.match_engine_profile(
            executable_observation["sha256"],
            executable_observation["byte_count"],
            allow_legacy_manifest_replay=True,
        )
    except whispercpp_engine_profiles.EngineProfileError:
        # The standalone adapter remains usable with caller-pinned fixture or reviewed
        # engines. The batch lanes impose the narrower shared allowlist themselves.
        return
    if profile["admission"] == "legacy_manifest_replay_only":
        raise ASRError(
            f"engine profile {profile['profile_id']} is validation-only and cannot be "
            "executed; materialize a work order with the current UTF-8-safe profile"
        )


def resolve_window(window: dict[str, Any], duration_ms: int) -> dict[str, int]:
    offset = window["offset_ms"]
    if offset >= duration_ms:
        raise ASRError("window.offset_ms must be before the end of the input audio")
    duration = window["duration_ms"] or (duration_ms - offset)
    if offset + duration > duration_ms:
        raise ASRError("window offset plus duration exceeds the input audio duration")
    return {"offset_ms": offset, "duration_ms": duration, "end_ms": offset + duration}


def build_command(
    work_order: dict[str, Any],
    output_prefix: Path,
    window: dict[str, int],
    prompt: str | None,
    *,
    executable_path: str | None = None,
    model_path: str | None = None,
    input_path: str | None = None,
) -> list[str]:
    engine = work_order["engine"]
    inference = work_order["inference"]
    command = [
        executable_path or engine["executable"],
        "--model",
        model_path or work_order["model"]["path"],
        "--file",
        input_path or work_order["input"]["path"],
        "--language",
        inference["language"],
        "--threads",
        str(inference["threads"]),
        "--offset-t",
        str(window["offset_ms"]),
        "--duration",
        str(window["duration_ms"]),
        "--max-len",
        str(inference["max_segment_characters"]),
        "--best-of",
        str(inference["best_of"]),
        "--beam-size",
        str(inference["beam_size"]),
        "--word-thold",
        str(inference["word_threshold"]),
        "--entropy-thold",
        str(inference["entropy_threshold"]),
        "--logprob-thold",
        str(inference["logprob_threshold"]),
        "--no-speech-thold",
        str(inference["no_speech_threshold"]),
        "--temperature",
        str(inference["temperature"]),
        "--temperature-inc",
        str(inference["temperature_increment"]),
        "--output-json-full",
        "--output-file",
        str(output_prefix),
        "--no-prints",
        "--no-gpu",
    ]
    if inference["split_on_word"]:
        command.append("--split-on-word")
    if inference["no_fallback"]:
        command.append("--no-fallback")
    if prompt is not None:
        command.extend(["--prompt", prompt])
    return command


def offsets(raw: Any, label: str, *, require_positive: bool) -> tuple[int | None, int | None]:
    if raw is None:
        return None, None
    if not isinstance(raw, dict) or set(raw) != {"from", "to"}:
        raise ASRError(f"{label}.offsets must contain exactly from and to")
    start = integer(raw["from"], f"{label}.offsets.from", 0, MAX_WINDOW_MS)
    end = integer(raw["to"], f"{label}.offsets.to", 0, MAX_WINDOW_MS)
    if end < start or (require_positive and end == start):
        raise ASRError(f"{label} must use a valid half-open millisecond span")
    return start, end


def token_offsets(
    raw: Any, label: str
) -> tuple[
    int | None,
    int | None,
    str,
    list[str],
    dict[str, int] | None,
]:
    """Normalize token offsets without fabricating an alignment.

    whisper.cpp occasionally emits a token whose two otherwise-valid millisecond
    offsets are inverted.  The segment remains usable, but there is no defensible
    token span to publish.  Retain those exact engine offsets as anomaly evidence and
    mark normalized timing unavailable with null endpoints.  All malformed, negative,
    or oversized values still fail through the same strict integer checks as segment
    offsets.
    """

    if raw is None:
        return None, None, "missing", [], None
    if not isinstance(raw, dict) or set(raw) != {"from", "to"}:
        raise ASRError(f"{label}.offsets must contain exactly from and to")
    start = integer(raw["from"], f"{label}.offsets.from", 0, MAX_WINDOW_MS)
    end = integer(raw["to"], f"{label}.offsets.to", 0, MAX_WINDOW_MS)
    if end < start:
        return (
            None,
            None,
            "unavailable",
            ["invalid_upstream_inverted"],
            {"from": start, "to": end},
        )
    return start, end, "observed", [], None


def normalize_engine_output(
    raw: Any, *, requested_language: str, window: dict[str, int]
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ASRError("whisper.cpp full JSON output must be an object")
    transcription = raw.get("transcription")
    result_raw = raw.get("result")
    params_raw = raw.get("params")
    if not isinstance(transcription, list) or not isinstance(result_raw, dict) or not isinstance(params_raw, dict):
        raise ASRError("whisper.cpp JSON is missing transcription, result, or params")
    detected_language = bounded_text(result_raw.get("language"), "engine result.language", 64).lower()
    if requested_language != "auto" and detected_language != requested_language:
        raise ASRError("whisper.cpp detected language does not match the requested language")
    if params_raw.get("translate") is not False:
        raise ASRError("whisper.cpp output unexpectedly reports translation")
    segments: list[dict[str, Any]] = []
    token_total = 0
    transcript_quality_flags: set[str] = set()
    for segment_index, segment_raw in enumerate(transcription):
        if not isinstance(segment_raw, dict):
            raise ASRError(f"transcription[{segment_index}] must be an object")
        start, end = offsets(segment_raw.get("offsets"), f"transcription[{segment_index}]", require_positive=True)
        assert start is not None and end is not None
        if start < window["offset_ms"] or start >= window["end_ms"]:
            raise ASRError("whisper.cpp segment lies outside the requested audio window")
        window_overrun_ms = max(0, end - window["end_ms"])
        if window_overrun_ms > MAX_SEGMENT_BOUNDARY_OVERRUN_MS:
            raise ASRError("whisper.cpp segment exceeds the bounded window overrun allowance")
        quality_flags: list[str] = []
        if window_overrun_ms:
            quality_flags.append("end_after_requested_window")
            transcript_quality_flags.add("segment_end_after_requested_window")
        text = segment_raw.get("text")
        if not isinstance(text, str) or "\x00" in text:
            raise ASRError("whisper.cpp segment text must be a string")
        tokens_raw = segment_raw.get("tokens")
        if not isinstance(tokens_raw, list):
            raise ASRError("whisper.cpp full JSON must include a tokens array per segment")
        tokens: list[dict[str, Any]] = []
        token_timing_unavailable = False
        for token_index, token_raw in enumerate(tokens_raw):
            if not isinstance(token_raw, dict):
                raise ASRError("whisper.cpp token must be an object")
            token_text = token_raw.get("text")
            token_id = token_raw.get("id")
            probability = token_raw.get("p")
            dtw = token_raw.get("t_dtw")
            if not isinstance(token_text, str) or "\x00" in token_text:
                raise ASRError("whisper.cpp token text must be a string")
            token_id = integer(token_id, "token.id", -2**31, 2**31 - 1)
            probability = number(probability, "token.p", 0, 1)
            dtw = number(dtw, "token.t_dtw", -1e12, 1e12)
            (
                token_start,
                token_end,
                timing_state,
                timing_quality_flags,
                original_offsets,
            ) = token_offsets(
                token_raw.get("offsets"),
                f"transcription[{segment_index}].tokens[{token_index}]",
            )
            if token_start is not None and (token_start < start or token_end > end):
                raise ASRError("whisper.cpp token timestamp lies outside its segment")
            if timing_state == "unavailable":
                token_timing_unavailable = True
            normalized_token = {
                "ordinal": token_index,
                "start_ms": token_start,
                "end_ms": token_end,
                "text": token_text,
                "token_id": token_id,
                "raw_probability": probability,
                "raw_dtw_timestamp": dtw,
                "metadata_json": canonical_json_text(token_raw),
            }
            if timing_state == "unavailable":
                normalized_token.update(
                    {
                        "timing_state": timing_state,
                        "timing_quality_flags": timing_quality_flags,
                        "original_offsets": original_offsets,
                    }
                )
            tokens.append(normalized_token)
        if token_timing_unavailable:
            quality_flags.append("token_timing_unavailable")
            transcript_quality_flags.add("token_timing_unavailable")
        token_total += len(tokens)
        segments.append(
            {
                "ordinal": segment_index,
                "start_ms": start,
                "end_ms": end,
                "text": text,
                "tokens": tokens,
                "quality_flags": quality_flags,
                "window_overrun_ms": window_overrun_ms,
                "metadata_json": canonical_json_text(segment_raw),
            }
        )
    return {
        "schema_version": 1,
        "language": {
            "requested": requested_language,
            "detected": detected_language,
        },
        "window": window,
        "segment_count": len(segments),
        "token_count": token_total,
        "quality_flags": sorted(transcript_quality_flags),
        "segments": segments,
        "engine_metadata_json": canonical_json_text(
            {key: value for key, value in raw.items() if key != "transcription"}
        ),
    }


def artifact_row(
    *, processing_run_id: str, kind: str, final_path: Path, staged_path: Path
) -> dict[str, Any]:
    digest = sha256_file(staged_path)
    return {
        "artifact_id": stable_id("artifact", processing_run_id, kind, digest),
        "processing_run_id": processing_run_id,
        "artifact_kind": kind,
        "storage_uri": final_path.as_uri(),
        "sha256": digest,
        "byte_count": staged_path.stat().st_size,
        "schema_version": 1,
        "visibility": "private",
        "metadata_json": canonical_json_text(
            {
                "mime_type": mimetypes.guess_type(final_path.name)[0]
                or "application/json"
            }
        ),
    }


def catalog_transcript_rows(
    *,
    context: dict[str, Any],
    processing_run: dict[str, Any],
    transcript: dict[str, Any],
    glossary_revision_id: str | None,
) -> dict[str, Any]:
    run_id = processing_run["processing_run_id"]
    revision_id = stable_id(
        "transcript_revision",
        run_id,
        context["recording_id"],
        context["rendition_id"],
    )
    revision = {
        "revision_id": revision_id,
        "recording_id": context["recording_id"],
        "rendition_id": context["rendition_id"],
        "processing_run_id": run_id,
        "revision_kind": "contextual_asr" if glossary_revision_id else "raw_asr",
        "origin": "whisper.cpp output-json-full",
        "language": transcript["language"]["detected"],
        "glossary_revision_id": glossary_revision_id,
        "review_state": "machine",
        "created_at": processing_run["completed_at"],
        "metadata_json": canonical_json_text(
            {
                "confidence_calibration": "none",
                "quality_flags": transcript["quality_flags"],
                "raw_scores_preserved": True,
            }
        ),
    }
    segments = []
    words = []
    for segment in transcript["segments"]:
        segment_id = stable_id("transcript_segment", revision_id, segment["ordinal"])
        token_timing_anomalies = [
            {
                "ordinal": token["ordinal"],
                "timing_state": token["timing_state"],
                "timing_quality_flags": token["timing_quality_flags"],
                "original_offsets": token["original_offsets"],
            }
            for token in segment["tokens"]
            if token.get("timing_state") == "unavailable"
        ]
        segment_metadata = {
            "engine_segment": json.loads(segment["metadata_json"]),
            "quality_flags": segment["quality_flags"],
            "window_overrun_ms": segment["window_overrun_ms"],
        }
        if token_timing_anomalies:
            segment_metadata["token_timing_anomalies"] = token_timing_anomalies
        segments.append(
            {
                "segment_id": segment_id,
                "revision_id": revision_id,
                "ordinal": segment["ordinal"],
                "start_ms": segment["start_ms"],
                "end_ms": segment["end_ms"],
                "text": segment["text"],
                "normalized_text": None,
                "speaker_label": None,
                "language": transcript["language"]["detected"],
                "confidence_band": None,
                "calibrated_probability": None,
                "metadata_json": canonical_json_text(segment_metadata),
            }
        )
        for token in segment["tokens"]:
            probability = token["raw_probability"]
            words.append(
                {
                    "word_id": stable_id(
                        "transcript_word", segment_id, token["ordinal"]
                    ),
                    "segment_id": segment_id,
                    "ordinal": token["ordinal"],
                    "start_ms": token["start_ms"],
                    "end_ms": token["end_ms"],
                    "token": token["text"],
                    "normalized_token": None,
                    "asr_log_probability": math.log(probability)
                    if probability > 0
                    else None,
                    "alignment_score": None,
                    "calibrated_probability": None,
                }
            )
    return {
        "transcript_revisions": [revision],
        "transcript_segments": segments,
        "transcript_words": words,
    }


def validate_completed_reuse(
    result_path: Path, *, result_key: str, recipe_id: str, run_dir: Path
) -> dict[str, Any]:
    result = load_json(result_path, maximum_bytes=MAX_RAW_JSON_BYTES)
    if not isinstance(result, dict) or result.get("status") != "completed":
        raise ASRError("existing immutable ASR result is not a completed envelope")
    if result.get("result_key") != result_key or result.get("recipe_id") != recipe_id:
        raise ASRError("existing immutable ASR result identity does not match this work order")
    if result.get("result_path") != str(result_path):
        raise ASRError("existing immutable ASR result_path is invalid")
    artifacts = result.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 2:
        raise ASRError("existing immutable ASR result must have exactly two artifacts")
    artifact_paths: dict[str, Path] = {}
    processing_run = result.get("processing_run")
    if not isinstance(processing_run, dict) or not isinstance(
        processing_run.get("processing_run_id"), str
    ):
        raise ASRError("existing immutable ASR processing run is invalid")
    for artifact in artifacts:
        if not isinstance(artifact, dict) or artifact.get("visibility") != "private":
            raise ASRError("existing immutable ASR artifact metadata is invalid")
        storage_uri = artifact.get("storage_uri")
        if not isinstance(storage_uri, str) or not storage_uri.startswith("file://"):
            raise ASRError("existing immutable ASR artifact URI is invalid")
        parsed = urllib.parse.urlsplit(storage_uri)
        if parsed.scheme != "file" or parsed.netloc not in ("", "localhost"):
            raise ASRError("existing immutable ASR artifact URI is not a local file URI")
        path = Path(urllib.parse.unquote(parsed.path)).resolve(strict=True)
        if path != run_dir and run_dir not in path.parents:
            raise ASRError("existing immutable ASR artifact escapes its run directory")
        if sha256_file(path) != artifact.get("sha256") or path.stat().st_size != artifact.get("byte_count"):
            raise ASRError(
                f"immutable completed result artifact hash mismatch: {path}"
            )
        expected_artifact_id = stable_id(
            "artifact",
            processing_run["processing_run_id"],
            artifact.get("artifact_kind"),
            artifact.get("sha256"),
        )
        if artifact.get("artifact_id") != expected_artifact_id:
            raise ASRError("existing immutable ASR artifact identity is invalid")
        artifact_paths[str(artifact.get("artifact_kind"))] = path
    required_kinds = {
        "whispercpp_output_json_full",
        "transcript_normalized_json",
    }
    if set(artifact_paths) != required_kinds:
        raise ASRError("existing immutable ASR artifact kinds are invalid")
    load_json(
        artifact_paths["whispercpp_output_json_full"],
        maximum_bytes=MAX_RAW_JSON_BYTES,
    )
    normalized = load_json(
        artifact_paths["transcript_normalized_json"],
        maximum_bytes=MAX_RAW_JSON_BYTES,
    )
    if normalized != result.get("transcript"):
        raise ASRError("existing immutable normalized transcript does not match result.json")
    return result


def verify_retained_inputs(
    input_file: RetainedFile,
    engine_file: RetainedFile,
    model_file: RetainedFile,
    glossary_file: RetainedFile | None,
) -> None:
    for retained, label in (
        (input_file, "input audio"),
        (engine_file, "whisper.cpp executable"),
        (model_file, "whisper.cpp model"),
    ):
        verify_retained_file(retained, label)
    if glossary_file is not None:
        verify_retained_file(glossary_file, "neutral glossary")


def execution_environment(
    logical_commands: list[list[str]],
    *,
    command_states: list[str],
    version: str,
    version_evidence: str,
) -> dict[str, Any]:
    return {
        "python": sys.version.split()[0],
        "cpu_only": True,
        "network": "not_used",
        "engine_version": version,
        "engine_version_evidence": version_evidence,
        "command_provenance": {
            "descriptor_execution_policy": DESCRIPTOR_EXECUTION_POLICY,
            "logical_commands": logical_commands,
            "logical_commands_definition": (
                "deterministic_original_input_paths_and_final_output_path_v1"
            ),
            "result_commands_location": "result.commands",
            "result_commands_definition": "exact_child_facing_argv_v1",
            "result_command_states": command_states,
        },
    }


def run_asr(work_order: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    started_at = utc_now()
    started_clock = time.monotonic()
    with ExitStack() as retained_stack:
        input_file = retained_stack.enter_context(
            retained_verified_file(
                Path(work_order["input"]["path"]),
                work_order["input"]["expected_sha256"],
                "input audio",
            )
        )
        engine_file = retained_stack.enter_context(
            retained_verified_file(
                Path(work_order["engine"]["executable"]),
                work_order["engine"]["expected_sha256"],
                "whisper.cpp executable",
                executable=True,
            )
        )
        reject_execution_ineligible_engine(engine_file.observation)
        model_file = retained_stack.enter_context(
            retained_verified_file(
                Path(work_order["model"]["path"]),
                work_order["model"]["expected_sha256"],
                "whisper.cpp model",
            )
        )
        glossary_file = None
        if work_order["glossary"]:
            glossary_file = retained_stack.enter_context(
                retained_verified_file(
                    Path(work_order["glossary"]["path"]),
                    work_order["glossary"]["expected_sha256"],
                    "neutral glossary",
                )
            )
        try:
            return _run_asr_retained(
                work_order,
                dry_run=dry_run,
                started_at=started_at,
                started_clock=started_clock,
                input_file=input_file,
                engine_file=engine_file,
                model_file=model_file,
                glossary_file=glossary_file,
            )
        finally:
            # Span probing, execution/reuse validation, artifact admission, and return.
            # This is the last operation before ExitStack closes the retained inodes.
            verify_retained_inputs(
                input_file, engine_file, model_file, glossary_file
            )


def _run_asr_retained(
    work_order: dict[str, Any],
    *,
    dry_run: bool,
    started_at: str,
    started_clock: float,
    input_file: RetainedFile,
    engine_file: RetainedFile,
    model_file: RetainedFile,
    glossary_file: RetainedFile | None,
) -> dict[str, Any]:
    input_path = input_file.path
    input_observation = input_file.observation
    executable_observation = engine_file.observation
    model_observation = model_file.observation
    try:
        probe, logical_probe_command, probe_command = probe_audio(
            input_path, input_file
        )
    finally:
        verify_retained_inputs(
            input_file, engine_file, model_file, glossary_file
        )
    window = resolve_window(work_order["window"], probe["duration_ms"])

    # Runtime admission does not depend on optional CLI version-output behavior.
    # Identity is the exact executable hash plus the caller-recorded source revision
    # and build configuration; version_label is an audit label, not command output.
    version = work_order["engine"]["version_label"]
    version_evidence = work_order["engine"]["version_evidence"]

    glossary_observation = None if glossary_file is None else glossary_file.observation
    glossary_document = None
    prompt = None
    if glossary_file is not None:
        glossary_document = validate_glossary_document(
            parse_json_bytes(
                descriptor_bytes(
                    glossary_file,
                    maximum_bytes=1024 * 1024,
                    label="neutral glossary",
                ),
                "neutral glossary",
            ),
            work_order["inference"]["language"],
        )
        prompt = glossary_document["prompt"]

    recipe = {
        "contract_version": CONTRACT_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "stage": STAGE,
        "descriptor_execution_policy": DESCRIPTOR_EXECUTION_POLICY,
        "engine": {
            "sha256": executable_observation["sha256"],
            "version": version,
            "version_evidence": version_evidence,
            "build": work_order["engine"]["build"],
        },
        "model": {
            key: work_order["model"][key]
            for key in ("model_id", "name", "revision", "source", "license_label")
        }
        | {"sha256": model_observation["sha256"]},
        "window": window,
        "inference": work_order["inference"],
        "glossary": None
        if glossary_document is None
        else {
            "glossary_revision_id": glossary_document["glossary_revision_id"],
            "revision": glossary_document["revision"],
            "sha256": glossary_observation["sha256"],
            "prompt_sha256": glossary_document["prompt_sha256"],
        },
        "output_contract": "whisper.cpp-output-json-full-normalized-v1",
    }
    recipe_sha256 = sha256_bytes(canonical_bytes(recipe))
    recipe_id = f"recipe_asr_whispercpp_{recipe_sha256[:32]}"
    work_order_sha256 = sha256_bytes(canonical_bytes(work_order))
    result_identity = {
        "work_order_sha256": work_order_sha256,
        "input_sha256": input_observation["sha256"],
        "input_media_id": work_order["input"]["media_id"],
        "input_artifact_id": work_order["input"]["artifact_id"],
        "parent_processing_run_id": work_order["input"]["parent_processing_run_id"],
        "recipe_id": recipe_id,
        "catalog_context": work_order["catalog_context"],
    }
    result_key = sha256_bytes(canonical_bytes(result_identity))
    output_root = Path(work_order["output"]["root"])
    run_dir = (
        output_root
        / "asr"
        / "whispercpp"
        / "sha256"
        / input_observation["sha256"][:2]
        / input_observation["sha256"]
        / "results"
        / result_key
    )
    result_path = run_dir / "result.json"
    execution_run_id = f"run_asr_whispercpp_{uuid.uuid4().hex}"
    output_prefix = run_dir / "whisper-output"
    logical_planned_command = build_command(
        work_order, output_prefix, window, prompt
    )
    planned_command = build_command(
        work_order,
        output_prefix,
        window,
        prompt,
        executable_path=engine_file.proc_path,
        model_path=model_file.proc_path,
        input_path=input_file.proc_path,
    )
    logical_commands = [logical_probe_command, logical_planned_command]

    if dry_run:
        verify_retained_inputs(
            input_file, engine_file, model_file, glossary_file
        )
        completed_at = utc_now()
        return {
            "schema_version": 1,
            "job_id": work_order["job_id"],
            "status": "planned",
            "dry_run": True,
            "work_order_sha256": work_order_sha256,
            "recipe_id": recipe_id,
            "recipe_sha256": recipe_sha256,
            "result_key": result_key,
            "processing_run": {
                "processing_run_id": execution_run_id,
                "stage": STAGE,
                "implementation_version": IMPLEMENTATION_VERSION,
                "model_id": work_order["model"]["model_id"],
                "glossary_revision_id": None
                if glossary_document is None
                else glossary_document["glossary_revision_id"],
                "parameters_json": canonical_json_text(recipe),
                "environment_json": canonical_json_text(
                    execution_environment(
                        logical_commands,
                        command_states=["executed", "planned"],
                        version=version,
                        version_evidence=version_evidence,
                    )
                ),
                "random_seed": None,
                "started_at": started_at,
                "completed_at": completed_at,
                "status": "queued",
                "error_text": None,
            },
            "input": {
                **input_observation,
                "media_id": work_order["input"]["media_id"],
                "artifact_id": work_order["input"]["artifact_id"],
                "parent_processing_run_id": work_order["input"]["parent_processing_run_id"],
                "probe": probe,
            },
            "engine": {
                **executable_observation,
                "version": version,
                "version_evidence": version_evidence,
                "build": work_order["engine"]["build"],
            },
            "model": {**model_observation, **{key: work_order["model"][key] for key in ("model_id", "name", "revision", "source", "license_label")}},
            "glossary": None if glossary_document is None else {**glossary_observation, **{key: glossary_document[key] for key in ("glossary_revision_id", "revision", "language", "prompt_sha256")}, "term_count": len(glossary_document["terms"])},
            "catalog_context": work_order["catalog_context"],
            "window": window,
            "commands": [probe_command, planned_command],
            "artifacts": [],
            "transcript": None,
            "catalog_records": None,
            "result_path": str(result_path),
            "duration_ms": round((time.monotonic() - started_clock) * 1_000),
            "errors": [],
        }

    ensure_private_directory(output_root, "ASR output root")
    if result_path.is_file():
        verify_retained_inputs(
            input_file, engine_file, model_file, glossary_file
        )
        return validate_completed_reuse(
            result_path, result_key=result_key, recipe_id=recipe_id, run_dir=run_dir
        )
    if run_dir.exists():
        raise ASRError("immutable ASR result directory exists without a reusable result")

    run_dir.parent.mkdir(parents=True, exist_ok=True)
    stage_dir = run_dir.parent / f".{result_key}.tmp-{execution_run_id}"
    stage_dir.mkdir(mode=0o700)
    staged_prefix = stage_dir / "whisper-output"
    command = build_command(
        work_order,
        staged_prefix,
        window,
        prompt,
        executable_path=engine_file.proc_path,
        model_path=model_file.proc_path,
        input_path=input_file.proc_path,
    )
    raw_staged = stage_dir / "whisper.raw.json"
    normalized_staged = stage_dir / "transcript.normalized.json"
    try:
        try:
            run_command(
                command,
                environment=minimal_environment(stage_dir / "home"),
                timeout_seconds=work_order["inference"]["timeout_seconds"],
                pass_fds=(
                    engine_file.descriptor,
                    model_file.descriptor,
                    input_file.descriptor,
                ),
            )
        finally:
            verify_retained_inputs(
                input_file, engine_file, model_file, glossary_file
            )
        produced = Path(str(staged_prefix) + ".json")
        if not produced.is_file() or produced.stat().st_size == 0:
            raise ASRError("whisper.cpp did not create output-json-full at the requested path")
        if produced.stat().st_size > MAX_RAW_JSON_BYTES:
            raise ASRError("whisper.cpp raw JSON exceeds the 512 MiB result limit")
        raw_body = stable_file_bytes(
            produced,
            maximum_bytes=MAX_RAW_JSON_BYTES,
            label="whisper.cpp output-json-full",
        )
        try:
            raw_text = raw_body.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            quarantine = quarantine_invalid_utf8_output(
                raw_body=raw_body,
                decode_error=error,
                output_root=output_root,
                work_order=work_order,
                work_order_sha256=work_order_sha256,
                recipe_id=recipe_id,
                result_key=result_key,
                input_sha256=input_observation["sha256"],
                engine_sha256=executable_observation["sha256"],
            )
            raise InvalidUTF8OutputError(quarantine) from error
        try:
            raw_output = json.loads(raw_text)
        except json.JSONDecodeError as error:
            raise ASRError(f"whisper.cpp returned invalid strict JSON: {error}") from error
        transcript = normalize_engine_output(
            raw_output,
            requested_language=work_order["inference"]["language"],
            window=window,
        )
        os.replace(produced, raw_staged)
        atomic_write_json(normalized_staged, transcript)

        verify_retained_inputs(
            input_file, engine_file, model_file, glossary_file
        )

        completed_at = utc_now()
        final_raw = run_dir / raw_staged.name
        final_normalized = run_dir / normalized_staged.name
        artifacts = [
            artifact_row(
                processing_run_id=execution_run_id,
                kind="whispercpp_output_json_full",
                final_path=final_raw,
                staged_path=raw_staged,
            ),
            artifact_row(
                processing_run_id=execution_run_id,
                kind="transcript_normalized_json",
                final_path=final_normalized,
                staged_path=normalized_staged,
            ),
        ]
        processing_run = {
            "processing_run_id": execution_run_id,
            "stage": STAGE,
            "implementation_version": IMPLEMENTATION_VERSION,
            "model_id": work_order["model"]["model_id"],
            "glossary_revision_id": None
            if glossary_document is None
            else glossary_document["glossary_revision_id"],
            "parameters_json": canonical_json_text(recipe),
            "environment_json": canonical_json_text(
                execution_environment(
                    logical_commands,
                    command_states=["executed", "executed"],
                    version=version,
                    version_evidence=version_evidence,
                )
            ),
            "random_seed": None,
            "started_at": started_at,
            "completed_at": completed_at,
            "status": "completed",
            "error_text": None,
        }
        run_input = {
            "run_input_id": stable_id(
                "run_input",
                execution_run_id,
                work_order["input"]["media_id"],
                work_order["input"]["artifact_id"],
            ),
            "processing_run_id": execution_run_id,
            "object_type": "media",
            "object_id": work_order["input"]["media_id"],
            "input_role": "normalized_audio",
            "input_sha256": input_observation["sha256"],
        }
        catalog_records: dict[str, Any] = {
            "processing_runs": [processing_run],
            "run_inputs": [run_input],
            "artifacts": artifacts,
        }
        if work_order["catalog_context"]:
            catalog_records.update(
                catalog_transcript_rows(
                    context=work_order["catalog_context"],
                    processing_run=processing_run,
                    transcript=transcript,
                    glossary_revision_id=processing_run["glossary_revision_id"],
                )
            )
        result = {
            "schema_version": 1,
            "job_id": work_order["job_id"],
            "status": "completed",
            "dry_run": False,
            "work_order_sha256": work_order_sha256,
            "recipe_id": recipe_id,
            "recipe_sha256": recipe_sha256,
            "result_key": result_key,
            "processing_run": processing_run,
            "run_input": run_input,
            "input": {
                **input_observation,
                "media_id": work_order["input"]["media_id"],
                "artifact_id": work_order["input"]["artifact_id"],
                "parent_processing_run_id": work_order["input"]["parent_processing_run_id"],
                "probe": probe,
            },
            "engine": {
                **executable_observation,
                "version": version,
                "version_evidence": version_evidence,
                "build": work_order["engine"]["build"],
            },
            "model": {
                **model_observation,
                **{
                    key: work_order["model"][key]
                    for key in ("model_id", "name", "revision", "source", "license_label")
                },
            },
            "glossary": None
            if glossary_document is None
            else {
                **glossary_observation,
                **{
                    key: glossary_document[key]
                    for key in (
                        "glossary_revision_id",
                        "revision",
                        "language",
                        "prompt_sha256",
                    )
                },
                "term_count": len(glossary_document["terms"]),
            },
            "catalog_context": work_order["catalog_context"],
            "window": window,
            "commands": [probe_command, command],
            "artifacts": artifacts,
            "transcript": transcript,
            "catalog_records": catalog_records,
            "result_path": str(result_path),
            "duration_ms": round((time.monotonic() - started_clock) * 1_000),
            "errors": [],
        }
        atomic_write_json(stage_dir / "result.json", result)
        try:
            os.rename(stage_dir, run_dir)
        except OSError as error:
            if error.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                raise
            shutil.rmtree(stage_dir)
            return validate_completed_reuse(
                result_path,
                result_key=result_key,
                recipe_id=recipe_id,
                run_dir=run_dir,
            )
        return result
    finally:
        if stage_dir.exists():
            shutil.rmtree(stage_dir)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Offline, hash-pinned whisper.cpp ASR adapter"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate")
    validate.add_argument("--work-order", required=True)
    run = commands.add_parser("run")
    run.add_argument("--work-order", required=True)
    run.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    job_id: str | None = None
    try:
        work_order_path = absolute_file(args.work_order, "--work-order")
        raw = load_json(work_order_path, maximum_bytes=4 * 1024 * 1024)
        if (
            isinstance(raw, dict)
            and isinstance(raw.get("job_id"), str)
            and JOB_ID_RE.fullmatch(raw["job_id"])
        ):
            job_id = raw["job_id"]
        work_order = validate_work_order(raw)
        result = work_order
        if args.command == "run":
            result = run_asr(work_order, dry_run=args.dry_run)
        sys.stdout.write(pretty_json(result))
        return 0
    except (ASRError, OSError, subprocess.SubprocessError) as error:
        failure = {
            "schema_version": 1,
            "status": "failed",
            "job_id": job_id,
            "error": {"type": type(error).__name__, "message": str(error)},
            "errors": [{"type": type(error).__name__, "message": str(error)}],
        }
        quarantine = getattr(error, "quarantine", None)
        if quarantine is not None:
            failure["quarantine"] = quarantine
        sys.stderr.write(pretty_json(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
