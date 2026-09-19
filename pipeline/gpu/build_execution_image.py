#!/usr/bin/env python3
"""Build and replay one deterministic private GPU execution SquashFS image.

The input specification enumerates every directory and regular file.  No recursive
source-directory shorthand, symlink, special file, or multiply-linked regular file
is accepted.  Building performs the expensive source-byte audit once.  Ordinary
``load_receipt`` replay authenticates the small receipt and the immutable image; it
does not traverse the source runtime, model, or build wheelhouse.

This utility builds an execution artifact only.  It does not mount or execute the
image, access the archive tier, run inference, mutate the catalogue, or publish
anything.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import secrets
import stat
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Sequence


SPEC_KIND = "himr_gpu_execution_image_spec"
RECEIPT_KIND = "himr_gpu_execution_image_receipt"
SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.2.0"
BUILDER_NAME = "himr-gpu-execution-image-builder"

# These field sets are public import API for the production successor.
SPEC_FIELDS = frozenset(
    {
        "kind",
        "schema_version",
        "source_epoch",
        "intended_mount_path",
        "entries",
        "logical_mappings",
        "policy",
    }
)
ENTRY_FIELDS = frozenset(
    {"kind", "source_path", "image_relative_path", "image_mode"}
)
MAPPING_FIELDS = frozenset(
    {"name", "image_relative_path", "sandbox_path", "role"}
)
SPEC_POLICY = {
    "visibility": "private",
    "archive_access": False,
    "wheelhouse_execution_dependency": False,
    "network_access": False,
    "inference_authority": "none",
    "catalogue_mutation_authority": "none",
    "publication_authority": "none",
}
RECEIPT_FIELDS = frozenset(
    {
        "kind",
        "schema_version",
        "implementation_version",
        "builder",
        "source_spec",
        "source_tree",
        "image",
        "intended_mount_path",
        "logical_mappings",
        "build",
        "policy",
        "identity_sha256",
        "receipt_id",
    }
)
SOURCE_SPEC_REFERENCE_FIELDS = frozenset(
    {"path", "sha256", "byte_count", "identity_sha256"}
)
SOURCE_TREE_FIELDS = frozenset(
    {
        "identity_sha256",
        "entry_count",
        "regular_file_count",
        "directory_count",
        "total_file_bytes",
        "entries",
    }
)
SOURCE_PROJECTION_FILE_FIELDS = frozenset(
    {
        "kind",
        "source_path",
        "image_relative_path",
        "source_mode",
        "image_mode",
        "sha256",
        "byte_count",
    }
)
SOURCE_PROJECTION_DIRECTORY_FIELDS = frozenset(
    {
        "kind",
        "source_path",
        "image_relative_path",
        "source_mode",
        "image_mode",
    }
)
IMAGE_FIELDS = frozenset(
    {"path", "sha256", "byte_count", "mode", "filesystem"}
)
FILESYSTEM_FIELDS = frozenset({"type", "uuid"})
BUILDER_FIELDS = frozenset(
    {
        "name",
        "source_path",
        "source_sha256",
        "portable_root_path",
        "portable_root_sha256",
    }
)
# One already-sealed local-private image predates the retained-directory
# child-churn correction. Its full receipt identity binds this exact historical
# builder tuple and every other receipt field. Accepting only this one identity
# preserves replay of the immutable image without granting compatibility to a
# newly fabricated receipt that merely copies an old source hash.
LEGACY_BUILDER_COMPATIBILITY = {
    "cd12833be1183e5813fb0f32cd74c034e2e07abc9e24e8f3d88397755a96bbf0": {
        "physical_receipt_sha256": (
            "9da089609a6bfcc7bbc0028ace9592b542a08c600de4a319159f7855d1a40a81"
        ),
        "receipt_id": "gpuexecimg_cd12833be1183e5813fb0f32cd74c034",
        "builder": {
            "name": "himr-gpu-execution-image-builder",
            "source_path": (
                str(Path(__file__).resolve())
            ),
            "source_sha256": (
                "57c2c9ec6fe7a077e54809e8cec6be8b09eb005ebd0084578fbbb5af7ee64721"
            ),
            "portable_root_path": (
                str(Path(__file__).resolve().with_name("portable_root.py"))
            ),
            "portable_root_sha256": (
                "6be504bbebebb75683c962fe95f5e0152858990b71ffc92246d1d0ae93ee3414"
            ),
        },
    }
}
BUILD_FIELDS = frozenset({"tool", "settings"})
TOOL_FIELDS = frozenset(
    {"path", "sha256", "byte_count", "uid", "mode", "version"}
)
SETTINGS_FIELDS = frozenset(
    {
        "source_epoch",
        "compression",
        "compression_level",
        "block_size",
        "processors",
        "all_root",
        "xattrs",
        "exports",
        "append",
        "hardlinks",
        "sort_sha256",
        "command_options",
    }
)
RECEIPT_POLICY = {
    **SPEC_POLICY,
    "source_full_sha256_audit": "completed_once_at_build",
    "ordinary_replay_source_tree_traversal": False,
    "image_mount_authority": "none",
    "image_execution_authority": "none",
    "deletion_authority": "temporary_builder_artifacts_only",
}

MKSQUASHFS = Path("/usr/bin/mksquashfs")
PORTABLE_ROOT_PATH = Path(__file__).resolve().with_name("portable_root.py")
COLD_ROOT = PurePosixPath("/mnt/archive/HIMR")
MAX_SPEC_BYTES = 64 * 1024 * 1024
MAX_RECEIPT_BYTES = 128 * 1024 * 1024
MAX_ENTRIES = 50_000
MAX_MAPPINGS = 256
MAX_TOTAL_FILE_BYTES = 16 * 1024 * 1024 * 1024
MAX_FILE_BYTES = 8 * 1024 * 1024 * 1024
MAX_PATH_BYTES = 4096
MAX_TOOL_OUTPUT_BYTES = 128 * 1024
SOURCE_EPOCH_MAX = 2**32 - 1
# A production image is root-owned but mounted by an unprivileged systemd --user
# worker.  Read-only 0444 under root-owned, non-writable ancestors keeps the bytes
# immutable to that worker while still allowing it to authenticate and mount them.
# Candidate images remain private because their output parent is mode 0700.
IMAGE_MODE = 0o444
PRIVATE_IMAGE_MODE = 0o400
ALLOWED_IMAGE_MODES = frozenset({PRIVATE_IMAGE_MODE, IMAGE_MODE})
# Four workers keep candidate builds fast on the 16-thread workstation without
# monopolizing it.  Level 9 is the conservative candidate setting for largely
# pre-compressed model weights and shared objects; admission records it exactly.
ZSTD_LEVEL = 9
BLOCK_SIZE = 131_072
PROCESSORS = 4
UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
IMAGE_COMPONENT_RE = re.compile(r"^[A-Za-z0-9._+@-]{1,255}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ALLOWED_FILE_MODES = {0o400, 0o444, 0o500, 0o555}
ALLOWED_DIRECTORY_MODES = {0o500, 0o555}


class ExecutionImageError(RuntimeError):
    """The source, build, image, or receipt contract failed closed."""


def _load_portable_root() -> Any:
    spec = importlib.util.spec_from_file_location(
        "himr_gpu_execution_image_portable_root", PORTABLE_ROOT_PATH
    )
    if spec is None or spec.loader is None:
        raise ExecutionImageError("portable-root module cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    previous = sys.modules.get(spec.name)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        if previous is None:
            sys.modules.pop(spec.name, None)
        else:
            sys.modules[spec.name] = previous
        raise
    return module


PORTABLE_ROOT = _load_portable_root()


def canonical_bytes(value: Any) -> bytes:
    try:
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
    except (TypeError, ValueError) as error:
        raise ExecutionImageError(f"value is not canonical JSON: {error}") from error


def sha256_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _exact(value: Any, label: str, fields: frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise ExecutionImageError(f"{label} has an unexpected shape")
    return value


def _bounded_string(value: Any, label: str, maximum: int = 1024) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > maximum
        or "\x00" in value
        or "\r" in value
        or "\n" in value
    ):
        raise ExecutionImageError(f"{label} must be a bounded non-empty string")
    return value


def _integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ExecutionImageError(f"{label} must be an integer")
    if value < minimum or value > maximum:
        raise ExecutionImageError(f"{label} is outside {minimum}..{maximum}")
    return value


def _sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise ExecutionImageError(f"{label} must be lowercase SHA-256")
    return value


def _uuid(value: Any, label: str) -> str:
    if not isinstance(value, str) or not UUID_RE.fullmatch(value):
        raise ExecutionImageError(f"{label} must be a canonical lowercase UUID")
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise ExecutionImageError(f"{label} is not a valid UUID") from error
    if parsed.int == 0 or str(parsed) != value:
        raise ExecutionImageError(f"{label} must be a nonzero canonical UUID")
    return value


def _lexically_cold(path: PurePosixPath) -> bool:
    return path == COLD_ROOT or COLD_ROOT in path.parents


def _absolute_path(value: Any, label: str, *, cold_forbidden: bool = True) -> Path:
    text = _bounded_string(value, label, MAX_PATH_BYTES)
    path = Path(text)
    if not path.is_absolute() or path == Path("/"):
        raise ExecutionImageError(f"{label} must be an absolute non-root path")
    normalized = Path(os.path.normpath(text))
    if normalized != path or str(normalized) != text:
        raise ExecutionImageError(f"{label} must already be lexically normalized")
    if cold_forbidden and _lexically_cold(PurePosixPath(text)):
        raise ExecutionImageError(f"{label} may not reference the archive tier")
    return path


def _relative_path(value: Any, label: str) -> PurePosixPath:
    text = _bounded_string(value, label, MAX_PATH_BYTES)
    path = PurePosixPath(text)
    if (
        path.is_absolute()
        or text in {".", ".."}
        or str(path) != text
        or any(part in {"", ".", ".."} for part in path.parts)
        or any(not IMAGE_COMPONENT_RE.fullmatch(part) for part in path.parts)
        or _lexically_cold(PurePosixPath("/") / path)
    ):
        raise ExecutionImageError(
            f"{label} must be a normalized relative path without traversal"
        )
    return path


def _mode(value: Any, label: str, kind: str) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"0[0-7]{3}", value):
        raise ExecutionImageError(f"{label} must be a four-digit octal string")
    parsed = int(value, 8)
    allowed = ALLOWED_DIRECTORY_MODES if kind == "directory" else ALLOWED_FILE_MODES
    if parsed not in allowed:
        raise ExecutionImageError(f"{label} is not an admitted read-only image mode")
    return parsed


def _parse_json(body: bytes, label: str) -> Any:
    try:
        text = body.decode("utf-8")
        return json.loads(
            text,
            parse_constant=lambda token: (_ for _ in ()).throw(
                ValueError(f"non-finite number {token}")
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise ExecutionImageError(f"{label} is not strict UTF-8 JSON: {error}") from error


def _open_parent(path: Path) -> tuple[int, str]:
    """Open every absolute parent component with no symlink following."""

    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for component in path.parts[1:-1]:
            next_descriptor = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = next_descriptor
        return descriptor, path.name
    except Exception:
        os.close(descriptor)
        raise


def _stat_identity(
    value: os.stat_result,
) -> tuple[int, int, int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_nlink,
        value.st_uid,
        value.st_gid,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _require_current_path_identity(
    path: Path, expected: os.stat_result, label: str
) -> None:
    """Reopen the no-follow absolute component chain and bind it to the retained inode."""

    parent, name = _open_parent(path)
    try:
        observed = os.stat(name, dir_fd=parent, follow_symlinks=False)
    except OSError as error:
        raise ExecutionImageError(f"{label} path replay failed: {error}") from error
    finally:
        os.close(parent)
    if _stat_identity(observed) != _stat_identity(expected):
        raise ExecutionImageError(f"{label} absolute path changed during the operation")


def _validate_owner_and_mode(metadata: os.stat_result, label: str) -> None:
    if metadata.st_uid not in {0, os.getuid()}:
        raise ExecutionImageError(f"{label} is not owned by root or the current user")
    admitted_mode = stat.S_IMODE(metadata.st_mode)
    if admitted_mode & 0o7000:
        raise ExecutionImageError(f"{label} has set-id or sticky permission bits")
    if admitted_mode & 0o022:
        raise ExecutionImageError(f"{label} is group/other writable")


def _read_regular(
    path: Path,
    label: str,
    maximum_bytes: int,
    *,
    exact_mode: int | None = None,
    single_link: bool = True,
) -> tuple[bytes, os.stat_result]:
    parent, name = _open_parent(path)
    descriptor: int | None = None
    try:
        before = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode):
            raise ExecutionImageError(f"{label} must be a regular file")
        if before.st_size < 0 or before.st_size > maximum_bytes:
            raise ExecutionImageError(f"{label} exceeds its byte bound")
        if single_link and before.st_nlink != 1:
            raise ExecutionImageError(f"{label} must have exactly one hard link")
        _validate_owner_and_mode(before, label)
        if exact_mode is not None and stat.S_IMODE(before.st_mode) != exact_mode:
            raise ExecutionImageError(
                f"{label} must have exact mode {exact_mode:04o}"
            )
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
            dir_fd=parent,
        )
        opened = os.fstat(descriptor)
        if _stat_identity(opened) != _stat_identity(before):
            raise ExecutionImageError(f"{label} changed while being opened")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(8 * 1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        body = b"".join(chunks)
        after_fd = os.fstat(descriptor)
        after_path = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if (
            len(body) != before.st_size
            or _stat_identity(after_fd) != _stat_identity(before)
            or _stat_identity(after_path) != _stat_identity(before)
        ):
            raise ExecutionImageError(f"{label} changed while being read")
        _require_current_path_identity(path, before, label)
        return body, before
    except OSError as error:
        raise ExecutionImageError(f"{label} could not be read safely: {error}") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(parent)


def _hash_regular_to(
    source: Path,
    destination: BinaryIO,
    label: str,
    maximum_bytes: int,
) -> tuple[str, int, int]:
    """Copy and hash one retained source descriptor with before/after race checks."""

    parent, name = _open_parent(source)
    descriptor: int | None = None
    try:
        before = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode):
            raise ExecutionImageError(f"{label} must be a regular file")
        if before.st_nlink != 1:
            raise ExecutionImageError(f"{label} must have exactly one hard link")
        if before.st_size < 0 or before.st_size > maximum_bytes:
            raise ExecutionImageError(f"{label} exceeds its byte bound")
        _validate_owner_and_mode(before, label)
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
            dir_fd=parent,
        )
        opened = os.fstat(descriptor)
        if _stat_identity(opened) != _stat_identity(before):
            raise ExecutionImageError(f"{label} changed while being opened")
        digest = hashlib.sha256()
        byte_count = 0
        while chunk := os.read(descriptor, 8 * 1024 * 1024):
            byte_count += len(chunk)
            if byte_count > maximum_bytes:
                raise ExecutionImageError(f"{label} exceeded its byte bound")
            digest.update(chunk)
            destination.write(chunk)
        after_fd = os.fstat(descriptor)
        after_path = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if (
            byte_count != before.st_size
            or _stat_identity(after_fd) != _stat_identity(before)
            or _stat_identity(after_path) != _stat_identity(before)
        ):
            raise ExecutionImageError(f"{label} changed during the full SHA-256 audit")
        _require_current_path_identity(source, before, label)
        return digest.hexdigest(), byte_count, stat.S_IMODE(before.st_mode)
    except OSError as error:
        raise ExecutionImageError(f"{label} could not be copied safely: {error}") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(parent)


def _directory_observation(path: Path, label: str) -> int:
    parent, name = _open_parent(path)
    descriptor: int | None = None
    try:
        before = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if not stat.S_ISDIR(before.st_mode):
            raise ExecutionImageError(f"{label} must be a directory")
        _validate_owner_and_mode(before, label)
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
            dir_fd=parent,
        )
        opened = os.fstat(descriptor)
        after = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if _stat_identity(opened) != _stat_identity(before) or _stat_identity(
            after
        ) != _stat_identity(before):
            raise ExecutionImageError(f"{label} changed while being inspected")
        _require_current_path_identity(path, before, label)
        return stat.S_IMODE(before.st_mode)
    except OSError as error:
        raise ExecutionImageError(f"{label} could not be inspected safely: {error}") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(parent)


def _hash_path(
    path: Path,
    label: str,
    maximum_bytes: int,
    *,
    exact_mode: int | None = None,
    required_uid: int | None = None,
    sync_before_return: bool = False,
) -> tuple[str, int]:
    parent, name = _open_parent(path)
    descriptor: int | None = None
    try:
        before = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode) or before.st_size > maximum_bytes:
            raise ExecutionImageError(f"{label} is not a bounded regular file")
        if before.st_nlink != 1:
            raise ExecutionImageError(f"{label} must have exactly one hard link")
        _validate_owner_and_mode(before, label)
        if required_uid is not None and before.st_uid != required_uid:
            raise ExecutionImageError(f"{label} has an unexpected owner")
        if exact_mode is not None and stat.S_IMODE(before.st_mode) != exact_mode:
            raise ExecutionImageError(f"{label} must have exact mode {exact_mode:04o}")
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
            dir_fd=parent,
        )
        opened = os.fstat(descriptor)
        if _stat_identity(opened) != _stat_identity(before):
            raise ExecutionImageError(f"{label} changed while being opened")
        digest = hashlib.sha256()
        byte_count = 0
        while chunk := os.read(descriptor, 8 * 1024 * 1024):
            byte_count += len(chunk)
            if byte_count > maximum_bytes:
                raise ExecutionImageError(f"{label} exceeded its byte bound")
            digest.update(chunk)
        if sync_before_return:
            os.fsync(descriptor)
        after_fd = os.fstat(descriptor)
        after_path = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if (
            byte_count != before.st_size
            or _stat_identity(after_fd) != _stat_identity(before)
            or _stat_identity(after_path) != _stat_identity(before)
        ):
            raise ExecutionImageError(f"{label} changed while being hashed")
        _require_current_path_identity(path, before, label)
        return digest.hexdigest(), byte_count
    except OSError as error:
        raise ExecutionImageError(f"{label} could not be hashed safely: {error}") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(parent)


def _tool_reference(path: Path, label: str, *, executable: bool) -> dict[str, Any]:
    parent, name = _open_parent(path)
    descriptor: int | None = None
    try:
        before = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != 0
            or before.st_nlink < 1
            or stat.S_IMODE(before.st_mode) & 0o022
            or (executable and not stat.S_IMODE(before.st_mode) & 0o111)
        ):
            raise ExecutionImageError(
                f"{label} must be an exact root-owned non-writable regular executable"
            )
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
            dir_fd=parent,
        )
        opened = os.fstat(descriptor)
        if _stat_identity(opened) != _stat_identity(before):
            raise ExecutionImageError(f"{label} changed while being opened")
        digest = hashlib.sha256()
        count = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
            count += len(chunk)
        after = os.fstat(descriptor)
        if count != before.st_size or _stat_identity(after) != _stat_identity(before):
            raise ExecutionImageError(f"{label} changed while being hashed")
        _require_current_path_identity(path, before, label)
        return {
            "path": str(path),
            "sha256": digest.hexdigest(),
            "byte_count": count,
            "uid": before.st_uid,
            "mode": f"{stat.S_IMODE(before.st_mode):04o}",
        }
    except OSError as error:
        raise ExecutionImageError(f"{label} validation failed: {error}") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(parent)


def _mksquashfs_reference() -> dict[str, Any]:
    reference = _tool_reference(MKSQUASHFS, "mksquashfs", executable=True)
    try:
        completed = subprocess.run(
            [str(MKSQUASHFS), "-version"],
            check=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env={"PATH": "/usr/bin:/bin", "LC_ALL": "C", "LANG": "C", "TZ": "UTC"},
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise ExecutionImageError(f"mksquashfs version query failed: {error}") from error
    if len(completed.stdout) > MAX_TOOL_OUTPUT_BYTES:
        raise ExecutionImageError("mksquashfs version output exceeds its bound")
    try:
        first_line = completed.stdout.decode("utf-8", errors="strict").splitlines()[0]
    except (UnicodeDecodeError, IndexError) as error:
        raise ExecutionImageError("mksquashfs version output is invalid") from error
    if not re.fullmatch(r"mksquashfs version [0-9][0-9A-Za-z. ()/_-]{1,127}", first_line):
        raise ExecutionImageError("mksquashfs version line is unsupported")
    return {**reference, "version": first_line}


def _filesystem_for(path: Path) -> dict[str, str]:
    parent, name = _open_parent(path)
    descriptor: int | None = None
    try:
        before = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if not (stat.S_ISDIR(before.st_mode) or stat.S_ISREG(before.st_mode)):
            raise ExecutionImageError("filesystem UUID target is not a file or directory")
        flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
        if stat.S_ISDIR(before.st_mode):
            flags |= os.O_DIRECTORY
        descriptor = os.open(
            name,
            flags,
            dir_fd=parent,
        )
        opened = os.fstat(descriptor)
        after = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if _stat_identity(opened) != _stat_identity(before) or _stat_identity(
            after
        ) != _stat_identity(before):
            raise ExecutionImageError("filesystem UUID target changed during probe")
        observed = PORTABLE_ROOT.btrfs_filesystem_uuid(descriptor)
        _require_current_path_identity(path, before, "filesystem UUID target")
        return {"type": "btrfs", "uuid": _uuid(observed, "Btrfs UUID")}
    except PORTABLE_ROOT.PortableRootError as error:
        raise ExecutionImageError(f"Btrfs filesystem UUID probe failed: {error}") from error
    except OSError as error:
        raise ExecutionImageError(f"filesystem UUID probe failed: {error}") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(parent)


def _stable_json_file(
    path: Path,
    label: str,
    maximum_bytes: int,
    *,
    expected_sha256: str | None = None,
    allowed_owner_modes: set[tuple[int, int]] | None = None,
) -> tuple[Any, bytes]:
    body, info = _read_regular(
        path,
        label,
        maximum_bytes,
        exact_mode=None,
        single_link=True,
    )
    owner_modes = (
        {(os.getuid(), 0o400)}
        if allowed_owner_modes is None
        else allowed_owner_modes
    )
    if (info.st_uid, stat.S_IMODE(info.st_mode)) not in owner_modes:
        rendered = ", ".join(
            f"uid {uid} mode {mode:04o}" for uid, mode in sorted(owner_modes)
        )
        raise ExecutionImageError(f"{label} must be one of: {rendered}")
    if expected_sha256 is not None and sha256_bytes(body) != _sha256(
        expected_sha256, f"expected {label} SHA-256"
    ):
        raise ExecutionImageError(f"{label} SHA-256 differs from its expected value")
    value = _parse_json(body, label)
    if body != canonical_bytes(value):
        raise ExecutionImageError(f"{label} is not canonical JSON")
    return value, body


def _normalize_mapping(value: Any, entry_by_path: dict[str, dict[str, Any]]) -> dict[str, str]:
    item = _exact(value, "logical mapping", MAPPING_FIELDS)
    name = _bounded_string(item["name"], "logical mapping name", 64)
    role = _bounded_string(item["role"], "logical mapping role", 64)
    if not NAME_RE.fullmatch(name) or not NAME_RE.fullmatch(role):
        raise ExecutionImageError("logical mapping name and role must be lowercase identifiers")
    image_path = str(
        _relative_path(item["image_relative_path"], "logical mapping image path")
    )
    if image_path not in entry_by_path:
        raise ExecutionImageError("logical mapping does not target an enumerated image entry")
    sandbox_path = str(
        _absolute_path(item["sandbox_path"], "logical mapping sandbox path")
    )
    if "wheelhouse" in name or "wheelhouse" in role:
        raise ExecutionImageError("wheelhouse may not be an execution mapping")
    target = entry_by_path[image_path]
    if role.endswith("directory") or role in {
        "directory",
        "runtime_root",
        "application_root",
        "model_bundle",
        "model_root",
        "shared_library_directory",
    }:
        if target["kind"] != "directory":
            raise ExecutionImageError("directory-role mapping must target a directory")
    if role == "executable" and not int(target["image_mode"], 8) & 0o100:
        raise ExecutionImageError("executable mapping must target an executable image mode")
    return {
        "name": name,
        "image_relative_path": image_path,
        "sandbox_path": sandbox_path,
        "role": role,
    }


def normalize_spec(value: Any) -> dict[str, Any]:
    spec = _exact(value, "execution-image source spec", SPEC_FIELDS)
    if spec["kind"] != SPEC_KIND or spec["schema_version"] != SCHEMA_VERSION:
        raise ExecutionImageError("execution-image source spec kind/version is unsupported")
    source_epoch = _integer(
        spec["source_epoch"], "source_epoch", 0, SOURCE_EPOCH_MAX
    )
    intended_mount = str(
        _absolute_path(spec["intended_mount_path"], "intended_mount_path")
    )
    if spec["policy"] != SPEC_POLICY:
        raise ExecutionImageError("execution-image source spec policy is unsupported")
    entries_value = spec["entries"]
    if not isinstance(entries_value, list) or not 1 <= len(entries_value) <= MAX_ENTRIES:
        raise ExecutionImageError("entries must be a non-empty bounded array")
    entries: list[dict[str, Any]] = []
    seen_images: set[str] = set()
    for ordinal, raw in enumerate(entries_value, start=1):
        item = _exact(raw, f"source entry {ordinal}", ENTRY_FIELDS)
        kind = item["kind"]
        if kind not in {"directory", "regular_file"}:
            raise ExecutionImageError(f"source entry {ordinal} kind is unsupported")
        source_path = str(
            _absolute_path(item["source_path"], f"source entry {ordinal} path")
        )
        image_path = str(
            _relative_path(
                item["image_relative_path"],
                f"source entry {ordinal} image_relative_path",
            )
        )
        if any("wheelhouse" in part.lower() for part in PurePosixPath(image_path).parts):
            raise ExecutionImageError("wheelhouse may not be embedded as an execution path")
        mode = _mode(item["image_mode"], f"source entry {ordinal} image_mode", kind)
        if image_path in seen_images:
            raise ExecutionImageError(f"duplicate image path: {image_path}")
        seen_images.add(image_path)
        entries.append(
            {
                "kind": kind,
                "source_path": source_path,
                "image_relative_path": image_path,
                "image_mode": f"{mode:04o}",
            }
        )
    entries.sort(key=lambda item: item["image_relative_path"])
    entry_by_path = {item["image_relative_path"]: item for item in entries}
    for item in entries:
        relative = PurePosixPath(item["image_relative_path"])
        for parent in relative.parents:
            if str(parent) == ".":
                continue
            parent_item = entry_by_path.get(str(parent))
            if parent_item is None or parent_item["kind"] != "directory":
                raise ExecutionImageError(
                    f"image parent directory is not explicitly enumerated: {parent}"
                )
    mappings_value = spec["logical_mappings"]
    if not isinstance(mappings_value, list) or not 1 <= len(mappings_value) <= MAX_MAPPINGS:
        raise ExecutionImageError("logical_mappings must be a non-empty bounded array")
    mappings = [_normalize_mapping(item, entry_by_path) for item in mappings_value]
    mappings.sort(key=lambda item: item["name"])
    if len({item["name"] for item in mappings}) != len(mappings):
        raise ExecutionImageError("logical mapping names must be unique")
    if len({item["image_relative_path"] for item in mappings}) != len(mappings):
        raise ExecutionImageError("logical mapping image paths must be unique")
    if len({item["sandbox_path"] for item in mappings}) != len(mappings):
        raise ExecutionImageError("logical mapping sandbox paths must be unique")
    normalized = {
        "kind": SPEC_KIND,
        "schema_version": SCHEMA_VERSION,
        "source_epoch": source_epoch,
        "intended_mount_path": intended_mount,
        "entries": entries,
        "logical_mappings": mappings,
        "policy": dict(SPEC_POLICY),
    }
    return normalized


def load_spec(path: Path, expected_sha256: str) -> tuple[dict[str, Any], bytes]:
    path = _absolute_path(str(path), "source spec path")
    value, body = _stable_json_file(
        path,
        "execution-image source spec",
        MAX_SPEC_BYTES,
        expected_sha256=expected_sha256,
    )
    normalized = normalize_spec(value)
    if normalized != value:
        raise ExecutionImageError("source spec is not already normalized")
    return normalized, body


def _source_tree_core(entries: list[dict[str, Any]]) -> dict[str, Any]:
    regular = [item for item in entries if item["kind"] == "regular_file"]
    directories = [item for item in entries if item["kind"] == "directory"]
    return {
        "entry_count": len(entries),
        "regular_file_count": len(regular),
        "directory_count": len(directories),
        "total_file_bytes": sum(item["byte_count"] for item in regular),
        "entries": entries,
    }


def _materialize_source_tree(spec: dict[str, Any], staging: Path) -> dict[str, Any]:
    projections: list[dict[str, Any]] = []
    total = 0
    for item in spec["entries"]:
        source = Path(item["source_path"])
        destination = staging / item["image_relative_path"]
        label = f"source {item['image_relative_path']}"
        mode = int(item["image_mode"], 8)
        if item["kind"] == "directory":
            source_mode = _directory_observation(source, label)
            destination.mkdir(mode=0o700)
            projections.append(
                {
                    "kind": "directory",
                    "source_path": str(source),
                    "image_relative_path": item["image_relative_path"],
                    "source_mode": f"{source_mode:04o}",
                    "image_mode": item["image_mode"],
                }
            )
            continue
        destination.parent.chmod(0o700)
        descriptor = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
        )
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as output:
                digest, byte_count, source_mode = _hash_regular_to(
                    source, output, label, min(MAX_FILE_BYTES, MAX_TOTAL_FILE_BYTES - total)
                )
                output.flush()
                os.fsync(output.fileno())
            os.fchmod(descriptor, mode)
        finally:
            os.close(descriptor)
        total += byte_count
        if total > MAX_TOTAL_FILE_BYTES:
            raise ExecutionImageError("source tree exceeds its total byte bound")
        projections.append(
            {
                "kind": "regular_file",
                "source_path": str(source),
                "image_relative_path": item["image_relative_path"],
                "source_mode": f"{source_mode:04o}",
                "image_mode": item["image_mode"],
                "sha256": digest,
                "byte_count": byte_count,
            }
        )
    directory_modes = {
        item["image_relative_path"]: int(item["image_mode"], 8)
        for item in spec["entries"]
        if item["kind"] == "directory"
    }
    for relative, mode in sorted(
        directory_modes.items(),
        key=lambda pair: len(PurePosixPath(pair[0]).parts),
        reverse=True,
    ):
        os.chmod(staging / relative, mode, follow_symlinks=False)
    projections.sort(key=lambda item: item["image_relative_path"])
    core = _source_tree_core(projections)
    if core["total_file_bytes"] > MAX_TOTAL_FILE_BYTES:
        raise ExecutionImageError("source tree exceeds its total byte bound")
    return {"identity_sha256": sha256_bytes(canonical_bytes(core)), **core}


def _safe_cleanup_tree(path: Path) -> None:
    if not path.exists():
        return
    observed = path.lstat()
    if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
        raise ExecutionImageError("refusing to clean a non-directory staging path")
    for current, directory_names, file_names in os.walk(path, topdown=False, followlinks=False):
        directory = Path(current)
        os.chmod(directory, 0o700, follow_symlinks=False)
        for name in file_names:
            child = directory / name
            metadata = child.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
                raise ExecutionImageError("refusing unsafe staging cleanup entry")
            os.chmod(child, 0o600, follow_symlinks=False)
            child.unlink()
        for name in directory_names:
            child = directory / name
            metadata = child.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
                raise ExecutionImageError("refusing unsafe staging cleanup directory")
            os.chmod(child, 0o700, follow_symlinks=False)
            child.rmdir()
    os.chmod(path, 0o700, follow_symlinks=False)
    path.rmdir()


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish_no_replace(temporary: Path, final: Path) -> None:
    if temporary.parent != final.parent:
        raise ExecutionImageError("temporary and final publication paths differ in parent")
    parent, name = _open_parent(final)
    try:
        os.link(
            temporary.name,
            name,
            src_dir_fd=parent,
            dst_dir_fd=parent,
            follow_symlinks=False,
        )
        os.unlink(temporary.name, dir_fd=parent)
        os.fsync(parent)
    except FileExistsError as error:
        raise ExecutionImageError(f"refusing to replace existing output: {final}") from error
    finally:
        os.close(parent)


def _write_private_temporary(path: Path, body: bytes) -> None:
    parent, name = _open_parent(path)
    descriptor: int | None = None
    try:
        descriptor = os.open(
            name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o400,
            dir_fd=parent,
        )
        offset = 0
        while offset < len(body):
            offset += os.write(descriptor, body[offset:])
        os.fchmod(descriptor, 0o400)
        os.fsync(descriptor)
        os.fsync(parent)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(parent)


def _builder_reference() -> dict[str, str]:
    source = Path(__file__).resolve()
    digest, _ = _hash_path(source, "execution-image builder source", 4 * 1024 * 1024)
    portable_digest, _ = _hash_path(
        PORTABLE_ROOT_PATH, "portable-root source", 4 * 1024 * 1024
    )
    return {
        "name": BUILDER_NAME,
        "source_path": str(source),
        "source_sha256": digest,
        "portable_root_path": str(PORTABLE_ROOT_PATH),
        "portable_root_sha256": portable_digest,
    }


def _sort_file(entries: Sequence[dict[str, Any]], path: Path) -> str:
    # Entries without a leading slash are resolved beneath the sole source directory
    # by mksquashfs.  Keeping the private staging path out of this file makes both its
    # digest and the ordering proof independent of the randomized build directory.
    # A unique priority removes reliance on filesystem readdir order for ties.
    lines = [
        f"{item['image_relative_path']} {32767 - ordinal}\n"
        for ordinal, item in enumerate(entries)
    ]
    body = "".join(lines).encode("utf-8")
    _write_private_temporary(path, body)
    return sha256_bytes(body)


def _command_options(source_epoch: int, sort_path: str | Path) -> list[str]:
    return [
        "-noappend",
        "-comp",
        "zstd",
        "-Xcompression-level",
        str(ZSTD_LEVEL),
        "-b",
        str(BLOCK_SIZE),
        "-processors",
        str(PROCESSORS),
        "-reproducible",
        "-mkfs-time",
        str(source_epoch),
        "-all-time",
        str(source_epoch),
        "-root-time",
        str(source_epoch),
        "-root-mode",
        "0555",
        "-all-root",
        "-no-xattrs",
        "-no-exports",
        "-no-hardlinks",
        "-no-recovery",
        "-exit-on-error",
        "-quiet",
        "-no-progress",
        "-sort",
        str(sort_path),
    ]


def _run_mksquashfs(
    staging: Path,
    temporary_image: Path,
    source_epoch: int,
    sort_path: Path,
    maximum_wall_seconds: int,
) -> tuple[dict[str, Any], list[str]]:
    tool_before = _mksquashfs_reference()
    options = _command_options(source_epoch, sort_path)
    command = [str(MKSQUASHFS), str(staging), str(temporary_image), *options]
    environment = {
        "PATH": "/usr/bin:/bin",
        "LC_ALL": "C",
        "LANG": "C",
        "TZ": "UTC",
    }
    try:
        completed = subprocess.run(
            command,
            check=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            timeout=maximum_wall_seconds,
            umask=0o077,
        )
    except subprocess.CalledProcessError as error:
        stderr = error.stderr if isinstance(error.stderr, bytes) else b""
        detail = stderr[:MAX_TOOL_OUTPUT_BYTES].decode("utf-8", errors="replace").strip()
        raise ExecutionImageError(
            f"deterministic mksquashfs build failed ({error.returncode}): {detail}"
        ) from error
    except (OSError, subprocess.SubprocessError) as error:
        raise ExecutionImageError(f"deterministic mksquashfs build failed: {error}") from error
    if len(completed.stdout) > MAX_TOOL_OUTPUT_BYTES or len(completed.stderr) > MAX_TOOL_OUTPUT_BYTES:
        raise ExecutionImageError("mksquashfs output exceeded its bound")
    if not temporary_image.is_file() or temporary_image.is_symlink():
        raise ExecutionImageError("mksquashfs did not create a regular image")
    tool_after = _mksquashfs_reference()
    if tool_after != tool_before:
        raise ExecutionImageError("mksquashfs changed during image construction")
    receipt_options = _command_options(source_epoch, "$PRIVATE_SORT_FILE")
    return tool_after, receipt_options


def _validate_output_parent(path: Path, label: str, filesystem_uuid: str) -> None:
    parent = path.parent
    if path.exists() or path.is_symlink():
        raise ExecutionImageError(f"{label} already exists")
    mode = _directory_observation(parent, f"{label} parent")
    metadata = parent.stat()
    if metadata.st_uid != os.getuid() or mode != 0o700:
        raise ExecutionImageError(f"{label} parent must be current-user mode 0700")
    observed = _filesystem_for(parent)
    if observed != {"type": "btrfs", "uuid": filesystem_uuid}:
        raise ExecutionImageError(f"{label} parent differs from the expected Btrfs UUID")


def _receipt_core(
    *,
    spec_path: Path,
    spec_body: bytes,
    spec: dict[str, Any],
    source_tree: dict[str, Any],
    image_path: Path,
    image_sha256: str,
    image_byte_count: int,
    filesystem_uuid: str,
    tool: dict[str, Any],
    sort_sha256: str,
    options: list[str],
    image_mode: int,
) -> dict[str, Any]:
    spec_identity = sha256_bytes(canonical_bytes(spec))
    return {
        "kind": RECEIPT_KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "builder": _builder_reference(),
        "source_spec": {
            "path": str(spec_path),
            "sha256": sha256_bytes(spec_body),
            "byte_count": len(spec_body),
            "identity_sha256": spec_identity,
        },
        "source_tree": source_tree,
        "image": {
            "path": str(image_path),
            "sha256": image_sha256,
            "byte_count": image_byte_count,
            "mode": f"{image_mode:04o}",
            "filesystem": {"type": "btrfs", "uuid": filesystem_uuid},
        },
        "intended_mount_path": spec["intended_mount_path"],
        "logical_mappings": spec["logical_mappings"],
        "build": {
            "tool": tool,
            "settings": {
                "source_epoch": spec["source_epoch"],
                "compression": "zstd",
                "compression_level": ZSTD_LEVEL,
                "block_size": BLOCK_SIZE,
                "processors": PROCESSORS,
                "all_root": True,
                "xattrs": False,
                "exports": False,
                "append": False,
                "hardlinks": False,
                "sort_sha256": sort_sha256,
                "command_options": options,
            },
        },
        "policy": dict(RECEIPT_POLICY),
    }


def build_execution_image(
    *,
    spec_path: Path,
    expected_spec_sha256: str,
    image_path: Path,
    receipt_path: Path,
    filesystem_uuid: str,
    maximum_wall_seconds: int,
    image_mode: int = IMAGE_MODE,
) -> dict[str, Any]:
    """Perform one full source audit and atomically publish an image and receipt."""

    spec_path = _absolute_path(str(spec_path), "source spec path")
    image_path = _absolute_path(str(image_path), "image path")
    receipt_path = _absolute_path(str(receipt_path), "receipt path")
    filesystem_uuid = _uuid(filesystem_uuid, "expected Btrfs UUID")
    maximum_wall_seconds = _integer(
        maximum_wall_seconds, "maximum_wall_seconds", 1, 24 * 60 * 60
    )
    if image_mode not in ALLOWED_IMAGE_MODES:
        raise ExecutionImageError("image_mode must be 0400 or 0444")
    if image_path == receipt_path:
        raise ExecutionImageError("image and receipt paths must differ")
    _validate_output_parent(image_path, "image", filesystem_uuid)
    _validate_output_parent(receipt_path, "receipt", filesystem_uuid)
    spec, spec_body = load_spec(spec_path, expected_spec_sha256)

    control = Path(
        tempfile.mkdtemp(prefix=".gpu-execution-image-", dir=str(image_path.parent))
    )
    os.chmod(control, 0o700)
    staging = control / "root"
    staging.mkdir(mode=0o700)
    sort_path = control / "sort.txt"
    temporary_image = image_path.parent / (
        f".{image_path.name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    )
    temporary_receipt = receipt_path.parent / (
        f".{receipt_path.name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    )
    image_published = False
    try:
        source_tree = _materialize_source_tree(spec, staging)
        os.chmod(staging, 0o500, follow_symlinks=False)
        sort_sha256 = _sort_file(spec["entries"], sort_path)
        tool, options = _run_mksquashfs(
            staging,
            temporary_image,
            spec["source_epoch"],
            sort_path,
            maximum_wall_seconds,
        )
        os.chmod(temporary_image, image_mode, follow_symlinks=False)
        image_sha256, image_byte_count = _hash_path(
            temporary_image,
            "new execution image",
            MAX_TOTAL_FILE_BYTES,
            sync_before_return=True,
        )
        core = _receipt_core(
            spec_path=spec_path,
            spec_body=spec_body,
            spec=spec,
            source_tree=source_tree,
            image_path=image_path,
            image_sha256=image_sha256,
            image_byte_count=image_byte_count,
            filesystem_uuid=filesystem_uuid,
            tool=tool,
            sort_sha256=sort_sha256,
            options=options,
            image_mode=image_mode,
        )
        identity = sha256_bytes(canonical_bytes(core))
        receipt = {
            **core,
            "identity_sha256": identity,
            "receipt_id": f"gpuexecimg_{identity[:32]}",
        }
        receipt_body = canonical_bytes(receipt)
        if len(receipt_body) > MAX_RECEIPT_BYTES:
            raise ExecutionImageError("execution-image receipt exceeds its byte bound")
        _write_private_temporary(temporary_receipt, receipt_body)
        _publish_no_replace(temporary_image, image_path)
        image_published = True
        _publish_no_replace(temporary_receipt, receipt_path)
        replayed = load_receipt(
            receipt_path,
            expected_sha256=sha256_bytes(receipt_body),
            verify_image=True,
        )
        if replayed != receipt:
            raise ExecutionImageError("published receipt differs from immediate replay")
        return receipt
    except Exception:
        if image_published and image_path.exists() and not receipt_path.exists():
            observed = image_path.lstat()
            if (
                stat.S_ISREG(observed.st_mode)
                and observed.st_uid == os.getuid()
                and observed.st_nlink == 1
            ):
                os.chmod(image_path, 0o600, follow_symlinks=False)
                image_path.unlink()
                _sync_directory(image_path.parent)
        raise
    finally:
        for temporary in (temporary_image, temporary_receipt):
            try:
                observed = temporary.lstat()
            except FileNotFoundError:
                continue
            if stat.S_ISREG(observed.st_mode) and observed.st_uid == os.getuid():
                os.chmod(temporary, 0o600, follow_symlinks=False)
                temporary.unlink()
        _safe_cleanup_tree(control)


def _validate_source_tree(value: Any) -> dict[str, Any]:
    tree = _exact(value, "receipt source_tree", SOURCE_TREE_FIELDS)
    entries = tree["entries"]
    count = _integer(tree["entry_count"], "source_tree.entry_count", 1, MAX_ENTRIES)
    regular_count = _integer(
        tree["regular_file_count"], "source_tree.regular_file_count", 0, count
    )
    directory_count = _integer(
        tree["directory_count"], "source_tree.directory_count", 0, count
    )
    total = _integer(
        tree["total_file_bytes"],
        "source_tree.total_file_bytes",
        0,
        MAX_TOTAL_FILE_BYTES,
    )
    if not isinstance(entries, list) or len(entries) != count:
        raise ExecutionImageError("source_tree entry count is inconsistent")
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    observed_total = 0
    observed_regular = 0
    for ordinal, raw in enumerate(entries, start=1):
        if not isinstance(raw, dict):
            raise ExecutionImageError("source projection entry must be an object")
        kind = raw.get("kind")
        fields = (
            SOURCE_PROJECTION_FILE_FIELDS
            if kind == "regular_file"
            else SOURCE_PROJECTION_DIRECTORY_FIELDS
            if kind == "directory"
            else frozenset()
        )
        item = _exact(raw, f"source projection {ordinal}", fields)
        source = str(
            _absolute_path(item["source_path"], f"source projection {ordinal} path")
        )
        image = str(
            _relative_path(
                item["image_relative_path"],
                f"source projection {ordinal} image path",
            )
        )
        if any("wheelhouse" in part.lower() for part in PurePosixPath(image).parts):
            raise ExecutionImageError(
                "wheelhouse may not be embedded in the execution-image projection"
            )
        if image in seen:
            raise ExecutionImageError("source projection contains a duplicate image path")
        seen.add(image)
        source_mode = item["source_mode"]
        if not isinstance(source_mode, str) or not re.fullmatch(r"0[0-7]{3}", source_mode):
            raise ExecutionImageError("source projection source_mode is invalid")
        image_mode = f"{_mode(item['image_mode'], 'source projection image_mode', kind):04o}"
        normalized_item: dict[str, Any] = {
            "kind": kind,
            "source_path": source,
            "image_relative_path": image,
            "source_mode": source_mode,
            "image_mode": image_mode,
        }
        if kind == "regular_file":
            byte_count = _integer(
                item["byte_count"], "source projection byte_count", 0, MAX_FILE_BYTES
            )
            normalized_item.update(
                {
                    "sha256": _sha256(item["sha256"], "source projection SHA-256"),
                    "byte_count": byte_count,
                }
            )
            observed_regular += 1
            observed_total += byte_count
        normalized.append(normalized_item)
    normalized.sort(key=lambda item: item["image_relative_path"])
    by_image = {item["image_relative_path"]: item for item in normalized}
    for item in normalized:
        for parent in PurePosixPath(item["image_relative_path"]).parents:
            if str(parent) == ".":
                continue
            parent_item = by_image.get(str(parent))
            if parent_item is None or parent_item["kind"] != "directory":
                raise ExecutionImageError(
                    "source projection omits an explicit parent directory"
                )
    core = _source_tree_core(normalized)
    if (
        regular_count != observed_regular
        or directory_count != count - observed_regular
        or total != observed_total
        or core["entry_count"] != count
        or tree["identity_sha256"] != sha256_bytes(canonical_bytes(core))
    ):
        raise ExecutionImageError("source_tree counts or identity are inconsistent")
    return {"identity_sha256": tree["identity_sha256"], **core}


def _validate_tool(value: Any) -> dict[str, Any]:
    item = _exact(value, "receipt build tool", TOOL_FIELDS)
    observed = _mksquashfs_reference()
    expected = {
        "path": str(_absolute_path(item["path"], "receipt mksquashfs path")),
        "sha256": _sha256(item["sha256"], "receipt mksquashfs SHA-256"),
        "byte_count": _integer(
            item["byte_count"], "receipt mksquashfs byte_count", 1, 64 * 1024 * 1024
        ),
        "uid": _integer(item["uid"], "receipt mksquashfs uid", 0, 2**31 - 1),
        "mode": item["mode"],
        "version": _bounded_string(item["version"], "receipt mksquashfs version", 160),
    }
    if expected != observed:
        raise ExecutionImageError("current mksquashfs differs from the receipt")
    return expected


def _validate_settings(value: Any) -> dict[str, Any]:
    settings = _exact(value, "receipt build settings", SETTINGS_FIELDS)
    epoch = _integer(settings["source_epoch"], "build source_epoch", 0, SOURCE_EPOCH_MAX)
    expected_options = _command_options(epoch, "$PRIVATE_SORT_FILE")
    supplied_options = settings["command_options"]
    if not isinstance(supplied_options, list) or not all(
        isinstance(item, str) for item in supplied_options
    ):
        raise ExecutionImageError("receipt command_options must be a string array")
    # The receipt binds a stable token, while execution substitutes a private path.
    if supplied_options != expected_options:
        raise ExecutionImageError("receipt mksquashfs options are unsupported")
    if (
        settings["compression"] != "zstd"
        or settings["compression_level"] != ZSTD_LEVEL
        or settings["block_size"] != BLOCK_SIZE
        or settings["processors"] != PROCESSORS
        or settings["all_root"] is not True
        or settings["xattrs"] is not False
        or settings["exports"] is not False
        or settings["append"] is not False
        or settings["hardlinks"] is not False
    ):
        raise ExecutionImageError("receipt build settings are unsupported")
    return {
        **settings,
        "source_epoch": epoch,
        "sort_sha256": _sha256(settings["sort_sha256"], "sort file SHA-256"),
        "command_options": supplied_options,
    }


def _validate_receipt_document(
    value: Any, *, physical_receipt_sha256: str | None = None
) -> dict[str, Any]:
    receipt = _exact(value, "execution-image receipt", RECEIPT_FIELDS)
    if (
        receipt["kind"] != RECEIPT_KIND
        or receipt["schema_version"] != SCHEMA_VERSION
        or receipt["implementation_version"] != IMPLEMENTATION_VERSION
        or receipt["policy"] != RECEIPT_POLICY
    ):
        raise ExecutionImageError("execution-image receipt header or policy is unsupported")
    builder = _exact(receipt["builder"], "receipt builder", BUILDER_FIELDS)
    expected_builder = _builder_reference()
    source_spec = _exact(
        receipt["source_spec"], "receipt source_spec", SOURCE_SPEC_REFERENCE_FIELDS
    )
    source_spec = {
        "path": str(_absolute_path(source_spec["path"], "source_spec.path")),
        "sha256": _sha256(source_spec["sha256"], "source_spec.sha256"),
        "byte_count": _integer(
            source_spec["byte_count"], "source_spec.byte_count", 1, MAX_SPEC_BYTES
        ),
        "identity_sha256": _sha256(
            source_spec["identity_sha256"], "source_spec.identity_sha256"
        ),
    }
    if source_spec["identity_sha256"] != source_spec["sha256"]:
        raise ExecutionImageError(
            "canonical source-spec physical and semantic SHA-256 values differ"
        )
    source_tree = _validate_source_tree(receipt["source_tree"])
    image_value = _exact(receipt["image"], "receipt image", IMAGE_FIELDS)
    filesystem = _exact(
        image_value["filesystem"], "receipt image filesystem", FILESYSTEM_FIELDS
    )
    image = {
        "path": str(_absolute_path(image_value["path"], "receipt image path")),
        "sha256": _sha256(image_value["sha256"], "receipt image SHA-256"),
        "byte_count": _integer(
            image_value["byte_count"],
            "receipt image byte_count",
            1,
            MAX_TOTAL_FILE_BYTES,
        ),
        "mode": image_value["mode"],
        "filesystem": {
            "type": filesystem["type"],
            "uuid": _uuid(filesystem["uuid"], "receipt Btrfs UUID"),
        },
    }
    if image["mode"] not in {f"{mode:04o}" for mode in ALLOWED_IMAGE_MODES} or image["filesystem"]["type"] != "btrfs":
        raise ExecutionImageError("receipt image mode or filesystem type is unsupported")
    intended_mount = str(
        _absolute_path(receipt["intended_mount_path"], "receipt intended_mount_path")
    )
    projection_entries = {
        item["image_relative_path"]: {
            "kind": item["kind"],
            "image_mode": item["image_mode"],
        }
        for item in source_tree["entries"]
    }
    mappings_value = receipt["logical_mappings"]
    if not isinstance(mappings_value, list) or not 1 <= len(mappings_value) <= MAX_MAPPINGS:
        raise ExecutionImageError("receipt logical_mappings is invalid")
    mappings = [_normalize_mapping(item, projection_entries) for item in mappings_value]
    mappings.sort(key=lambda item: item["name"])
    if mappings != mappings_value:
        raise ExecutionImageError("receipt logical_mappings is not normalized")
    if len({item["name"] for item in mappings}) != len(mappings):
        raise ExecutionImageError("receipt mapping names are duplicated")
    if len({item["image_relative_path"] for item in mappings}) != len(mappings):
        raise ExecutionImageError("receipt mapping image paths are duplicated")
    if len({item["sandbox_path"] for item in mappings}) != len(mappings):
        raise ExecutionImageError("receipt mapping sandbox paths are duplicated")
    build = _exact(receipt["build"], "receipt build", BUILD_FIELDS)
    normalized_build = {
        "tool": _validate_tool(build["tool"]),
        "settings": _validate_settings(build["settings"]),
    }
    core = {
        "kind": RECEIPT_KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "builder": builder,
        "source_spec": source_spec,
        "source_tree": source_tree,
        "image": image,
        "intended_mount_path": intended_mount,
        "logical_mappings": mappings,
        "build": normalized_build,
        "policy": dict(RECEIPT_POLICY),
    }
    identity = sha256_bytes(canonical_bytes(core))
    if (
        receipt["identity_sha256"] != identity
        or receipt["receipt_id"] != f"gpuexecimg_{identity[:32]}"
    ):
        raise ExecutionImageError("receipt identity or ID is inconsistent")
    if builder != expected_builder:
        legacy = LEGACY_BUILDER_COMPATIBILITY.get(identity)
        if (
            legacy is None
            or physical_receipt_sha256 != legacy["physical_receipt_sha256"]
            or receipt["receipt_id"] != legacy["receipt_id"]
            or builder != legacy["builder"]
        ):
            raise ExecutionImageError("current builder source differs from the receipt")
    return {
        **core,
        "identity_sha256": identity,
        "receipt_id": receipt["receipt_id"],
    }


def load_receipt(
    path: str | Path,
    expected_sha256: str | None = None,
    verify_image: bool = True,
    expected_image_uid: int | None = None,
) -> dict[str, Any]:
    """Load and exactly replay a receipt, optionally hashing its immutable image.

    This is the stable import API for the GPU production successor.  It never reads
    source-tree entries, build-wheelhouse bytes, media, the archive tier, or CUDA.
    Candidate replay requires current-user image ownership by default.  A production
    launcher can require root ownership with ``expected_image_uid=0``.  Skipping the
    image is safe only when a trusted launch attestation already binds a
    launcher-verified image digest.
    """

    receipt_path = _absolute_path(str(path), "execution-image receipt path")
    value, body = _stable_json_file(
        receipt_path,
        "execution-image receipt",
        MAX_RECEIPT_BYTES,
        expected_sha256=expected_sha256,
        allowed_owner_modes=(
            {(os.getuid(), 0o400)}
            if expected_image_uid is None
            else {
                (
                    expected_image_uid,
                    0o444 if expected_image_uid == 0 else 0o400,
                )
            }
        ),
    )
    normalized = _validate_receipt_document(
        value, physical_receipt_sha256=sha256_bytes(body)
    )
    if normalized != value:
        raise ExecutionImageError("execution-image receipt is not normalized")
    if verify_image:
        required_uid = (
            os.getuid()
            if expected_image_uid is None
            else _integer(
                expected_image_uid,
                "expected_image_uid",
                0,
                2**31 - 1,
            )
        )
        image = normalized["image"]
        image_path = Path(image["path"])
        observed_sha256, observed_bytes = _hash_path(
            image_path,
            "execution image",
            MAX_TOTAL_FILE_BYTES,
            exact_mode=int(image["mode"], 8),
            required_uid=required_uid,
        )
        if observed_bytes != image["byte_count"] or observed_sha256 != image["sha256"]:
            raise ExecutionImageError("execution image bytes drifted from the receipt")
        if _filesystem_for(image_path) != image["filesystem"]:
            raise ExecutionImageError("execution image left its admitted Btrfs filesystem")
    return normalized


def contract_document() -> dict[str, Any]:
    descriptor = {
        "spec_kind": SPEC_KIND,
        "receipt_kind": RECEIPT_KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "maximum_entries": MAX_ENTRIES,
        "maximum_mappings": MAX_MAPPINGS,
        "maximum_total_file_bytes": MAX_TOTAL_FILE_BYTES,
        "mksquashfs": str(MKSQUASHFS),
        "compression": "zstd",
        "compression_level": ZSTD_LEVEL,
        "block_size": BLOCK_SIZE,
        "processors": PROCESSORS,
        "image_modes": ["0400", "0444"],
        "policy": RECEIPT_POLICY,
    }
    return {"descriptor": descriptor, "identity_sha256": sha256_bytes(canonical_bytes(descriptor))}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("contracts", help="emit the immutable image contract")
    build = commands.add_parser("build", help="audit sources and build one image")
    build.add_argument("--spec", required=True)
    build.add_argument("--expected-spec-sha256", required=True)
    build.add_argument("--image", required=True)
    build.add_argument("--receipt", required=True)
    build.add_argument("--filesystem-uuid", required=True)
    build.add_argument("--maximum-wall-seconds", required=True, type=int)
    build.add_argument(
        "--image-mode",
        choices=("0400", "0444"),
        default="0444",
        help="seal a same-UID private image (0400) or root-installable image (0444)",
    )
    validate = commands.add_parser("validate", help="replay a receipt and image")
    validate.add_argument("--receipt", required=True)
    validate.add_argument("--expected-receipt-sha256")
    validate.add_argument("--expected-image-uid", type=int)
    validate.add_argument(
        "--receipt-only",
        action="store_true",
        help="validate the receipt contract without opening the image",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "contracts":
            result: Any = contract_document()
        elif args.command == "build":
            result = build_execution_image(
                spec_path=Path(args.spec),
                expected_spec_sha256=args.expected_spec_sha256,
                image_path=Path(args.image),
                receipt_path=Path(args.receipt),
                filesystem_uuid=args.filesystem_uuid,
                maximum_wall_seconds=args.maximum_wall_seconds,
                image_mode=int(args.image_mode, 8),
            )
        elif args.command == "validate":
            result = load_receipt(
                args.receipt,
                expected_sha256=args.expected_receipt_sha256,
                verify_image=not args.receipt_only,
                expected_image_uid=args.expected_image_uid,
            )
        else:  # pragma: no cover
            raise ExecutionImageError(f"unsupported command: {args.command}")
        sys.stdout.buffer.write(canonical_bytes(result))
        return 0
    except Exception as error:
        failure = {
            "kind": "himr_gpu_execution_image_failure",
            "schema_version": SCHEMA_VERSION,
            "status": "failed",
            "error": {"type": type(error).__name__, "message": str(error)},
            "inference_executed": False,
            "archive_accessed": False,
            "catalogue_mutated": False,
        }
        sys.stderr.buffer.write(canonical_bytes(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
