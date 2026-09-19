#!/usr/bin/env python3
"""Hash-anchored successor for safe single-link media-preprocess reuse.

The v0.3.3 producer is immutable historical evidence and remains the source of
the media-preprocess contract.  This successor verifies those exact source
bytes before delegating to it, replacing only the physical reuse operation:
every reused artifact receives a distinct inode through Btrfs FICLONE when
available or a verified byte copy otherwise.
"""

from __future__ import annotations

import ctypes
import errno
import fcntl
import hashlib
import os
import stat
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

try:
    from . import media_preprocess as _legacy
except ImportError:  # pragma: no cover - direct isolated script execution.
    import media_preprocess as _legacy  # type: ignore[no-redef]


LEGACY_SOURCE_PATH = Path(_legacy.__file__).resolve()
LEGACY_SOURCE_SHA256 = (
    "b85d846e0ac9d6705372cd892f1133e4f8eea342b43a950e0bef65b5d0920810"
)
PRODUCER_NAME = "himr-media-preprocess-single-link-reuse"
PRODUCER_VERSION = "0.1.0"

# Keep result/work-order compatibility exact.  The successor path and physical
# SHA-256 distinguish newly admitted bundles; changing this value would prevent
# verified v0.3.3 results from serving as immutable reuse sources.
CONTRACT_VERSION = _legacy.CONTRACT_VERSION
IMPLEMENTATION_VERSION = _legacy.IMPLEMENTATION_VERSION
PipelineError = _legacy.PipelineError
CommandError = _legacy.CommandError

FICLONE = 0x40049409
RENAME_NOREPLACE = 1
COPY_CHUNK_BYTES = 8 * 1024 * 1024
_REFLINK_FALLBACK_ERRNOS = frozenset(
    {
        errno.EXDEV,
        errno.EINVAL,
        errno.ENOTTY,
        errno.EOPNOTSUPP,
        errno.ENOSYS,
    }
)
_DELEGATE_LOCK = threading.RLock()

# preprocess_batch intentionally replaces these two hooks while it executes a
# bounded item.  The delegation context mirrors them into the legacy module for
# the duration of each call and restores the legacy globals afterward.
run_command = _legacy.run_command
subprocess_environment = _legacy.subprocess_environment


def _sha256_fd(descriptor: int, byte_count: int) -> str:
    digest = hashlib.sha256()
    offset = 0
    while offset < byte_count:
        chunk = os.pread(
            descriptor, min(COPY_CHUNK_BYTES, byte_count - offset), offset
        )
        if not chunk:
            raise PipelineError("immutable reuse source ended while it was hashed")
        digest.update(chunk)
        offset += len(chunk)
    if os.pread(descriptor, 1, byte_count):
        raise PipelineError("immutable reuse source grew while it was hashed")
    return digest.hexdigest()


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_uid,
        value.st_gid,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _require_source(path: Path, label: str) -> tuple[int, os.stat_result]:
    try:
        before = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as error:
        raise PipelineError(f"cannot retain {label}: {path}: {error}") from error
    try:
        opened = os.fstat(descriptor)
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or _stat_identity(before) != _stat_identity(opened)
            or opened.st_uid != os.geteuid()
            or stat.S_IMODE(opened.st_mode) & 0o222
            or opened.st_size <= 0
        ):
            raise PipelineError(
                f"{label} must be a stable, non-empty, read-only regular file: {path}"
            )
        return descriptor, opened
    except Exception:
        os.close(descriptor)
        raise


def _require_legacy_source() -> tuple[int, os.stat_result]:
    try:
        before = LEGACY_SOURCE_PATH.lstat()
        descriptor = os.open(
            LEGACY_SOURCE_PATH,
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as error:
        raise PipelineError(
            f"cannot retain legacy media-preprocess source: {error}"
        ) from error
    try:
        opened = os.fstat(descriptor)
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or _stat_identity(before) != _stat_identity(opened)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o755
            or not 1 <= opened.st_size <= 4 * 1024 * 1024
        ):
            raise PipelineError(
                "legacy media-preprocess source metadata differs from its exact pin"
            )
        return descriptor, opened
    except Exception:
        os.close(descriptor)
        raise


def _copy_descriptor(source: int, destination: int, byte_count: int) -> None:
    os.ftruncate(destination, 0)
    offset = 0
    while offset < byte_count:
        chunk = os.pread(
            source, min(COPY_CHUNK_BYTES, byte_count - offset), offset
        )
        if not chunk:
            raise PipelineError("immutable reuse source ended during byte copy")
        written = 0
        while written < len(chunk):
            count = os.pwrite(destination, chunk[written:], offset + written)
            if count <= 0:
                raise PipelineError("immutable reuse byte copy made no progress")
            written += count
        offset += len(chunk)
    if os.pread(source, 1, byte_count):
        raise PipelineError("immutable reuse source grew during byte copy")


def _rename_noreplace(
    source_directory: int,
    source_name: str,
    destination_directory: int,
    destination_name: str,
) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise PipelineError("atomic renameat2(RENAME_NOREPLACE) is unavailable")
    renameat2.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    renameat2.restype = ctypes.c_int
    result = renameat2(
        source_directory,
        os.fsencode(source_name),
        destination_directory,
        os.fsencode(destination_name),
        RENAME_NOREPLACE,
    )
    if result == 0:
        return
    observed_errno = ctypes.get_errno()
    if observed_errno == errno.EEXIST:
        raise PipelineError(
            f"immutable reuse destination already exists: {destination_name}"
        )
    raise PipelineError(
        f"atomic immutable reuse publication failed: {os.strerror(observed_errno)}"
    )


def verify_legacy_source() -> dict[str, Any]:
    """Verify and describe the exact contract implementation we delegate to."""

    descriptor, opened = _require_legacy_source()
    try:
        observed = _sha256_fd(descriptor, opened.st_size)
        after = os.fstat(descriptor)
        current = LEGACY_SOURCE_PATH.lstat()
    finally:
        os.close(descriptor)
    if (
        observed != LEGACY_SOURCE_SHA256
        or _stat_identity(opened) != _stat_identity(after)
        or _stat_identity(opened) != _stat_identity(current)
    ):
        raise PipelineError(
            "legacy media-preprocess source differs from the successor pin"
        )
    return {
        "path": str(LEGACY_SOURCE_PATH),
        "sha256": observed,
        "byte_count": opened.st_size,
        "implementation_version": _legacy.IMPLEMENTATION_VERSION,
    }


def single_link_immutable_reuse(
    source: Path, destination: Path, label: str
) -> None:
    """Publish a verified immutable reuse artifact with one distinct inode."""

    source = Path(source)
    destination = Path(destination)
    if (
        not source.is_absolute()
        or not destination.is_absolute()
        or os.path.normpath(str(source)) != str(source)
        or os.path.normpath(str(destination)) != str(destination)
        or destination.name in {"", ".", ".."}
    ):
        raise PipelineError("immutable reuse paths must be normalized absolute files")
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_descriptor, source_info = _require_source(source, label)
    parent_descriptor: int | None = None
    temporary_descriptor: int | None = None
    destination_name = destination.name
    temporary_name = (
        f".{destination_name}.reuse-{os.getpid()}-{uuid.uuid4().hex}"
    )
    published = False
    published_identity: tuple[int, ...] | None = None
    try:
        parent_descriptor = os.open(
            destination.parent,
            os.O_RDONLY
            | os.O_CLOEXEC
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        parent_info = os.fstat(parent_descriptor)
        if not stat.S_ISDIR(parent_info.st_mode):
            raise PipelineError("immutable reuse destination parent is not a directory")
        try:
            os.stat(destination_name, dir_fd=parent_descriptor, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise PipelineError(
                f"immutable reuse destination already exists: {destination}"
            )
        temporary_descriptor = os.open(
            temporary_name,
            os.O_RDWR
            | os.O_CREAT
            | os.O_EXCL
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=parent_descriptor,
        )
        source_sha256 = _sha256_fd(source_descriptor, source_info.st_size)
        try:
            fcntl.ioctl(temporary_descriptor, FICLONE, source_descriptor)
        except OSError as error:
            if error.errno not in _REFLINK_FALLBACK_ERRNOS:
                raise PipelineError(f"immutable reuse reflink failed: {error}") from error
            _copy_descriptor(
                source_descriptor, temporary_descriptor, source_info.st_size
            )
        temporary_info = os.fstat(temporary_descriptor)
        if (
            not stat.S_ISREG(temporary_info.st_mode)
            or temporary_info.st_size != source_info.st_size
            or (temporary_info.st_dev, temporary_info.st_ino)
            == (source_info.st_dev, source_info.st_ino)
        ):
            raise PipelineError("immutable reuse did not create a distinct complete inode")
        destination_sha256 = _sha256_fd(
            temporary_descriptor, temporary_info.st_size
        )
        source_after = os.fstat(source_descriptor)
        source_path_after = source.lstat()
        if (
            destination_sha256 != source_sha256
            or _stat_identity(source_after) != _stat_identity(source_info)
            or _stat_identity(source_path_after) != _stat_identity(source_info)
        ):
            raise PipelineError("immutable reuse source or destination changed during copy")

        os.fchmod(temporary_descriptor, stat.S_IMODE(source_info.st_mode))
        os.fsync(temporary_descriptor)
        sealed_info = os.fstat(temporary_descriptor)
        if sealed_info.st_nlink != 1 or stat.S_IMODE(sealed_info.st_mode) & 0o222:
            raise PipelineError("immutable reuse temporary did not seal as one link")
        _rename_noreplace(
            parent_descriptor,
            temporary_name,
            parent_descriptor,
            destination_name,
        )
        published = True
        published_identity = _stat_identity(os.fstat(temporary_descriptor))
        os.fsync(parent_descriptor)
        observed = os.stat(
            destination_name, dir_fd=parent_descriptor, follow_symlinks=False
        )
        current_parent = destination.parent.lstat()
        if (
            _stat_identity(observed) != published_identity
            or observed.st_nlink != 1
            or (observed.st_dev, observed.st_ino)
            == (source_info.st_dev, source_info.st_ino)
            or stat.S_IMODE(observed.st_mode) & 0o222
            or (
                current_parent.st_dev,
                current_parent.st_ino,
                current_parent.st_mode,
                current_parent.st_uid,
                current_parent.st_gid,
            )
            != (
                parent_info.st_dev,
                parent_info.st_ino,
                parent_info.st_mode,
                parent_info.st_uid,
                parent_info.st_gid,
            )
        ):
            raise PipelineError("published immutable reuse artifact is not single-link")
    except Exception:
        if published and published_identity is not None:
            try:
                current = os.stat(
                    destination_name,
                    dir_fd=parent_descriptor,
                    follow_symlinks=False,
                )
                if _stat_identity(current) == published_identity:
                    os.unlink(destination_name, dir_fd=parent_descriptor)
                    if parent_descriptor is not None:
                        os.fsync(parent_descriptor)
            except OSError:
                pass
        raise
    finally:
        if parent_descriptor is not None:
            try:
                os.unlink(temporary_name, dir_fd=parent_descriptor)
            except OSError:
                pass
        if temporary_descriptor is not None:
            os.close(temporary_descriptor)
        if parent_descriptor is not None:
            os.close(parent_descriptor)
        os.close(source_descriptor)


@contextmanager
def _delegated_runtime(*, single_link_reuse: bool = False) -> Iterator[None]:
    with _DELEGATE_LOCK:
        verify_legacy_source()
        original_command = _legacy.run_command
        original_environment = _legacy.subprocess_environment
        original_reuse = _legacy.hardlink_immutable
        _legacy.run_command = run_command
        _legacy.subprocess_environment = subprocess_environment
        if single_link_reuse:
            _legacy.hardlink_immutable = single_link_immutable_reuse
        try:
            yield
        finally:
            _legacy.run_command = original_command
            _legacy.subprocess_environment = original_environment
            _legacy.hardlink_immutable = original_reuse


def executable_provenance(*args: Any, **kwargs: Any) -> dict[str, Any]:
    with _delegated_runtime():
        return _legacy.executable_provenance(*args, **kwargs)


def verify_executable_provenance(*args: Any, **kwargs: Any) -> None:
    with _delegated_runtime():
        _legacy.verify_executable_provenance(*args, **kwargs)


def probe_file(*args: Any, **kwargs: Any) -> tuple[dict[str, Any], list[str]]:
    with _delegated_runtime():
        return _legacy.probe_file(*args, **kwargs)


def validate_prior_result(*args: Any, **kwargs: Any) -> dict[str, Any]:
    with _delegated_runtime():
        return _legacy.validate_prior_result(*args, **kwargs)


def run_work_order(*args: Any, **kwargs: Any) -> dict[str, Any]:
    with _delegated_runtime(single_link_reuse=True):
        return _legacy.run_work_order(*args, **kwargs)


# Pure contract helpers retain their exact v0.3.3 behavior and schema.
require_tool = _legacy.require_tool
validate_profile = _legacy.validate_profile
validate_work_order = _legacy.validate_work_order
work_recipe = _legacy.work_recipe
