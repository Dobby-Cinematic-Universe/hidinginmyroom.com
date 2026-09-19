#!/usr/bin/env python3
"""Copy one sealed main-drive object to content-addressed cold storage.

This boundary deliberately does less than the acquisition and processing tools.  It
copies one explicitly named, already-hashed regular file, performs one bounded
readback from the cold filesystem, and writes an immutable receipt on the source
filesystem.  It never deletes the source, scans the destination tree, decodes media,
updates the catalogue, or grants publication authority.
"""

from __future__ import annotations

import argparse
import ctypes
import errno
import fcntl
import hashlib
import os
import re
import stat
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

try:
    from acquire import (
        AcquisitionError,
        PinnedRegularFile,
        canonical_bytes,
        pretty_json,
        sha256_bytes,
        strict_json_object,
    )
except ModuleNotFoundError:  # Imported as ``acquisition.cold_storage_transfer``.
    from acquisition.acquire import (  # type: ignore[no-redef]
        AcquisitionError,
        PinnedRegularFile,
        canonical_bytes,
        pretty_json,
        sha256_bytes,
        strict_json_object,
    )


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.3.0"
PRODUCER_NAME = "himr-cold-storage-transfer"
CHUNK_SIZE = 8 * 1024 * 1024
MAX_RECEIPT_BYTES = 1024 * 1024
MAX_FDINFO_BYTES = 64 * 1024
MAX_BYTE_COUNT = 2**63 - 1
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
TIMESTAMP_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
)
AT_EMPTY_PATH = 0x1000


class ColdStorageError(RuntimeError):
    """A cold-storage policy, integrity, or durability failure."""


@dataclass(frozen=True)
class TransferRequest:
    source_root: Path
    source_relative_path: Path
    destination_root: Path
    receipt_root: Path
    expected_sha256: str
    expected_byte_count: int
    free_space_floor_bytes: int


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _fingerprint(value: os.stat_result) -> tuple[int, ...]:
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


def _directory_fingerprint(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
    )


def _directory_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_DIRECTORY", 0)
    )


def _file_read_flags() -> int:
    return os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(
        os, "O_NOFOLLOW", 0
    )


def _mount_id(descriptor: int) -> int:
    """Return Linux's mount ID for a retained descriptor without resolving a path."""

    fdinfo_path = f"/proc/self/fdinfo/{descriptor}"
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(
        os, "O_NOFOLLOW", 0
    )
    try:
        fdinfo_fd = os.open(fdinfo_path, flags)
    except OSError as error:
        raise ColdStorageError(
            f"cannot inspect retained descriptor mount identity: {error}"
        ) from error
    try:
        body = bytearray()
        while len(body) <= MAX_FDINFO_BYTES:
            chunk = os.read(fdinfo_fd, min(4096, MAX_FDINFO_BYTES + 1 - len(body)))
            if not chunk:
                break
            body.extend(chunk)
        if len(body) > MAX_FDINFO_BYTES:
            raise ColdStorageError("descriptor mount metadata exceeds its fixed bound")
    finally:
        os.close(fdinfo_fd)
    try:
        text = body.decode("ascii")
    except UnicodeDecodeError as error:
        raise ColdStorageError("descriptor mount metadata is not ASCII") from error
    values = [
        line.split(":", 1)[1].strip()
        for line in text.splitlines()
        if line.startswith("mnt_id:")
    ]
    if len(values) != 1 or not values[0].isdigit():
        raise ColdStorageError("descriptor mount metadata lacks one valid mount ID")
    return int(values[0])


def _validate_absolute_root(path: Path, label: str) -> Path:
    raw = os.fspath(path)
    if (
        not raw
        or len(raw) > 4096
        or "\x00" in raw
        or "\\" in raw
        or "//" in raw
        or not os.path.isabs(raw)
    ):
        raise ColdStorageError(f"{label} must be an explicit absolute path")
    if raw == "/" or os.path.normpath(raw) != raw:
        raise ColdStorageError(f"{label} must be normalized and cannot be filesystem root")
    return Path(raw)


def _validate_relative_path(path: Path, label: str) -> Path:
    raw = os.fspath(path)
    if (
        not raw
        or len(raw) > 4096
        or "\x00" in raw
        or "\\" in raw
        or "//" in raw
        or os.path.isabs(raw)
        or os.path.normpath(raw) != raw
    ):
        raise ColdStorageError(f"{label} must be one normalized relative path")
    parts = Path(raw).parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ColdStorageError(f"{label} contains an unsafe path component")
    return Path(*parts)


class RetainedRoot:
    """Retain every descriptor from ``/`` through one explicit managed root."""

    def __init__(
        self,
        *,
        path: Path,
        descriptors: list[int],
        components: list[tuple[int, str, int, os.stat_result]],
        component_mount_ids: list[int],
    ) -> None:
        self.path = path
        self.descriptors = descriptors
        self.components = components
        self.component_mount_ids = component_mount_ids
        self.descriptor = descriptors[-1]
        self.initial_stat = os.fstat(self.descriptor)
        self.mount_id = _mount_id(self.descriptor)

    @classmethod
    def open(cls, path: Path, label: str) -> "RetainedRoot":
        path = _validate_absolute_root(path, label)
        flags = _directory_flags()
        descriptors: list[int] = []
        components: list[tuple[int, str, int, os.stat_result]] = []
        component_mount_ids: list[int] = []
        try:
            parent_fd = os.open("/", flags)
            descriptors.append(parent_fd)
            for component in path.parts[1:]:
                try:
                    inspected = os.stat(
                        component, dir_fd=parent_fd, follow_symlinks=False
                    )
                except OSError as error:
                    raise ColdStorageError(f"cannot inspect {label}: {error}") from error
                if stat.S_ISLNK(inspected.st_mode) or not stat.S_ISDIR(
                    inspected.st_mode
                ):
                    raise ColdStorageError(
                        f"{label} contains a symlink or non-directory component"
                    )
                try:
                    child_fd = os.open(component, flags, dir_fd=parent_fd)
                except OSError as error:
                    raise ColdStorageError(f"cannot retain {label}: {error}") from error
                descriptors.append(child_fd)
                opened = os.fstat(child_fd)
                if _directory_fingerprint(opened) != _directory_fingerprint(inspected):
                    raise ColdStorageError(f"{label} changed while it was opened")
                components.append((parent_fd, component, child_fd, opened))
                component_mount_ids.append(_mount_id(child_fd))
                parent_fd = child_fd
            final = os.fstat(descriptors[-1])
            if final.st_uid != os.geteuid() or stat.S_IMODE(final.st_mode) & 0o022:
                raise ColdStorageError(
                    f"{label} must be current-user-owned and not group/world writable"
                )
            retained = cls(
                path=path,
                descriptors=descriptors,
                components=components,
                component_mount_ids=component_mount_ids,
            )
            retained.verify()
            return retained
        except Exception:
            for descriptor in reversed(descriptors):
                os.close(descriptor)
            raise

    def verify(self) -> None:
        for index, (parent_fd, name, child_fd, initial) in enumerate(self.components):
            linked_fd: int | None = None
            try:
                try:
                    linked = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                    opened = os.fstat(child_fd)
                    linked_fd = os.open(name, _directory_flags(), dir_fd=parent_fd)
                    linked_mount_id = _mount_id(linked_fd)
                except OSError as error:
                    raise ColdStorageError(
                        f"retained root changed or disappeared: {self.path}"
                    ) from error
                expected = _directory_fingerprint(initial)
                if (
                    stat.S_ISLNK(linked.st_mode)
                    or not stat.S_ISDIR(linked.st_mode)
                    or _directory_fingerprint(linked) != expected
                    or _directory_fingerprint(opened) != expected
                    or linked_mount_id != self.component_mount_ids[index]
                    or _mount_id(child_fd) != self.component_mount_ids[index]
                ):
                    raise ColdStorageError(
                        f"retained root identity changed: {self.path}"
                    )
            finally:
                if linked_fd is not None:
                    os.close(linked_fd)
        final = os.fstat(self.descriptor)
        if (
            _directory_fingerprint(final) != _directory_fingerprint(self.initial_stat)
            or _mount_id(self.descriptor) != self.mount_id
        ):
            raise ColdStorageError(f"retained root metadata changed: {self.path}")

    def close(self) -> None:
        for descriptor in reversed(self.descriptors):
            os.close(descriptor)
        self.descriptors.clear()


class RetainedDirectoryChain:
    """Descriptors for private managed directories beneath a retained root."""

    def __init__(self, root: RetainedRoot) -> None:
        self.root = root
        self.descriptors: list[int] = []
        self.components: list[tuple[int, str, int, os.stat_result]] = []
        self.component_mount_ids: list[int] = []
        self.created_parent_fds: list[int] = []

    def ensure(self, parts: tuple[str, ...]) -> int:
        parent_fd = self.root.descriptor
        for component in parts:
            if not component or component in {".", ".."} or "/" in component:
                raise ColdStorageError("managed destination contains unsafe component")
            created = False
            try:
                inspected = os.stat(
                    component, dir_fd=parent_fd, follow_symlinks=False
                )
            except FileNotFoundError:
                try:
                    os.mkdir(component, mode=0o700, dir_fd=parent_fd)
                    os.fsync(parent_fd)
                    created = True
                    inspected = os.stat(
                        component, dir_fd=parent_fd, follow_symlinks=False
                    )
                except FileExistsError:
                    inspected = os.stat(
                        component, dir_fd=parent_fd, follow_symlinks=False
                    )
                except OSError as error:
                    raise ColdStorageError(
                        f"cannot create managed destination directory: {error}"
                    ) from error
            except OSError as error:
                raise ColdStorageError(
                    f"cannot inspect managed destination directory: {error}"
                ) from error
            if stat.S_ISLNK(inspected.st_mode) or not stat.S_ISDIR(inspected.st_mode):
                raise ColdStorageError(
                    "managed destination component is a symlink or non-directory"
                )
            try:
                child_fd = os.open(component, _directory_flags(), dir_fd=parent_fd)
            except OSError as error:
                raise ColdStorageError(
                    f"cannot retain managed destination directory: {error}"
                ) from error
            self.descriptors.append(child_fd)
            opened = os.fstat(child_fd)
            if _directory_fingerprint(opened) != _directory_fingerprint(inspected):
                raise ColdStorageError("managed destination changed while opening")
            if opened.st_uid != os.geteuid() or stat.S_IMODE(opened.st_mode) & 0o077:
                raise ColdStorageError(
                    "managed destination directories must be owner-private"
                )
            if opened.st_dev != self.root.initial_stat.st_dev:
                raise ColdStorageError(
                    "managed directory crossed a nested filesystem boundary"
                )
            opened_mount_id = _mount_id(child_fd)
            if opened_mount_id != self.root.mount_id:
                raise ColdStorageError(
                    "managed directory crossed a nested mount boundary"
                )
            self.components.append((parent_fd, component, child_fd, opened))
            self.component_mount_ids.append(opened_mount_id)
            if created:
                self.created_parent_fds.append(parent_fd)
            parent_fd = child_fd
        self.verify()
        return parent_fd

    def open_existing(self, parts: tuple[str, ...]) -> int | None:
        """Retain one exact existing chain without creating any component."""

        parent_fd = self.root.descriptor
        for component in parts:
            if not component or component in {".", ".."} or "/" in component:
                raise ColdStorageError("managed path contains an unsafe component")
            try:
                inspected = os.stat(
                    component, dir_fd=parent_fd, follow_symlinks=False
                )
            except FileNotFoundError:
                return None
            except OSError as error:
                raise ColdStorageError(
                    f"cannot inspect managed directory: {error}"
                ) from error
            if stat.S_ISLNK(inspected.st_mode) or not stat.S_ISDIR(inspected.st_mode):
                raise ColdStorageError(
                    "managed path component is a symlink or non-directory"
                )
            try:
                child_fd = os.open(component, _directory_flags(), dir_fd=parent_fd)
            except OSError as error:
                raise ColdStorageError(
                    f"cannot retain managed directory: {error}"
                ) from error
            self.descriptors.append(child_fd)
            opened = os.fstat(child_fd)
            if _directory_fingerprint(opened) != _directory_fingerprint(inspected):
                raise ColdStorageError("managed directory changed while opening")
            if opened.st_uid != os.geteuid() or stat.S_IMODE(opened.st_mode) & 0o077:
                raise ColdStorageError("managed directories must be owner-private")
            if opened.st_dev != self.root.initial_stat.st_dev:
                raise ColdStorageError(
                    "managed directory crossed a nested filesystem boundary"
                )
            opened_mount_id = _mount_id(child_fd)
            if opened_mount_id != self.root.mount_id:
                raise ColdStorageError(
                    "managed directory crossed a nested mount boundary"
                )
            self.components.append((parent_fd, component, child_fd, opened))
            self.component_mount_ids.append(opened_mount_id)
            parent_fd = child_fd
        self.verify()
        return parent_fd

    def verify(self) -> None:
        self.root.verify()
        for index, (parent_fd, name, child_fd, initial) in enumerate(self.components):
            linked_fd: int | None = None
            try:
                try:
                    linked = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                    opened = os.fstat(child_fd)
                    linked_fd = os.open(name, _directory_flags(), dir_fd=parent_fd)
                    linked_mount_id = _mount_id(linked_fd)
                except OSError as error:
                    raise ColdStorageError("managed destination changed") from error
                expected = _directory_fingerprint(initial)
                if (
                    stat.S_ISLNK(linked.st_mode)
                    or _directory_fingerprint(linked) != expected
                    or _directory_fingerprint(opened) != expected
                    or linked_mount_id != self.component_mount_ids[index]
                    or _mount_id(child_fd) != self.component_mount_ids[index]
                ):
                    raise ColdStorageError("managed destination identity changed")
            finally:
                if linked_fd is not None:
                    os.close(linked_fd)

    def sync(self) -> None:
        for descriptor in reversed(self.descriptors):
            os.fsync(descriptor)
        self.verify()

    def close(self) -> None:
        for descriptor in reversed(self.descriptors):
            os.close(descriptor)
        self.descriptors.clear()


def _hash_descriptor(descriptor: int, expected_size: int, label: str) -> str:
    digest = hashlib.sha256()
    offset = 0
    while offset < expected_size:
        try:
            chunk = os.pread(descriptor, min(CHUNK_SIZE, expected_size - offset), offset)
        except OSError as error:
            raise ColdStorageError(f"cannot read {label}: {error}") from error
        if not chunk:
            raise ColdStorageError(f"{label} ended before its expected byte count")
        digest.update(chunk)
        offset += len(chunk)
    try:
        if os.pread(descriptor, 1, expected_size):
            raise ColdStorageError(f"{label} exceeds its expected byte count")
    except OSError as error:
        raise ColdStorageError(f"cannot finish reading {label}: {error}") from error
    return digest.hexdigest()


def _verify_source(source: PinnedRegularFile) -> os.stat_result:
    try:
        source.verify()
    except AcquisitionError as error:
        raise ColdStorageError(str(error)) from error
    return os.fstat(source.descriptor)


def _file_identity(value: os.stat_result) -> dict[str, Any]:
    return {
        "device": value.st_dev,
        "inode": value.st_ino,
        "byte_count": value.st_size,
        "mtime_ns": value.st_mtime_ns,
        "ctime_ns": value.st_ctime_ns,
        "owner_uid": value.st_uid,
        "mode": f"{stat.S_IMODE(value.st_mode):04o}",
        "nlink": value.st_nlink,
    }


def _entry_exists(directory_fd: int, name: str) -> bool:
    try:
        os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    except OSError as error:
        raise ColdStorageError(f"cannot inspect managed object: {error}") from error
    return True


def _probe_target_without_read(
    root: RetainedRoot, parts: tuple[str, ...], expected_byte_count: int
) -> bool:
    """Inspect only the exact target topology; never read its payload bytes."""

    chain = RetainedDirectoryChain(root)
    try:
        directory_fd = chain.open_existing(parts)
        if directory_fd is None or not _entry_exists(directory_fd, "payload"):
            return False
        inspected = os.stat(
            "payload", dir_fd=directory_fd, follow_symlinks=False
        )
        if (
            stat.S_ISLNK(inspected.st_mode)
            or not stat.S_ISREG(inspected.st_mode)
            or inspected.st_uid != os.geteuid()
            or inspected.st_nlink != 1
            or stat.S_IMODE(inspected.st_mode) != 0o400
            or inspected.st_size != expected_byte_count
            or inspected.st_dev != root.initial_stat.st_dev
        ):
            raise ColdStorageError(
                "existing cold-storage target has unsafe topology or metadata"
            )
        payload_fd = os.open("payload", _file_read_flags(), dir_fd=directory_fd)
        try:
            if _mount_id(payload_fd) != root.mount_id:
                raise ColdStorageError(
                    "existing cold-storage target crossed a nested mount boundary"
                )
        finally:
            os.close(payload_fd)
        chain.verify()
        return True
    finally:
        chain.close()


def _preflight_source_path(
    root: RetainedRoot,
    relative_path: Path,
    *,
    expected_byte_count: int,
) -> None:
    """Reject unsafe topology and nested mounts before hashing source bytes."""

    descriptors: list[int] = []
    parent_fd = root.descriptor
    try:
        for component in relative_path.parts[:-1]:
            try:
                inspected = os.stat(
                    component, dir_fd=parent_fd, follow_symlinks=False
                )
            except OSError as error:
                raise ColdStorageError(
                    f"cannot inspect sealed source path: {error}"
                ) from error
            if (
                stat.S_ISLNK(inspected.st_mode)
                or not stat.S_ISDIR(inspected.st_mode)
                or inspected.st_dev != root.initial_stat.st_dev
            ):
                raise ColdStorageError(
                    "sealed source path contains a symlink, non-directory, or nested mount"
                )
            try:
                child_fd = os.open(component, _directory_flags(), dir_fd=parent_fd)
            except OSError as error:
                raise ColdStorageError(
                    f"cannot retain sealed source path: {error}"
                ) from error
            descriptors.append(child_fd)
            opened = os.fstat(child_fd)
            if _directory_fingerprint(opened) != _directory_fingerprint(inspected):
                raise ColdStorageError("sealed source path changed during preflight")
            if _mount_id(child_fd) != root.mount_id:
                raise ColdStorageError(
                    "sealed source path crossed a nested mount boundary"
                )
            parent_fd = child_fd
        leaf = relative_path.parts[-1]
        inspected_leaf = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
        if (
            stat.S_ISLNK(inspected_leaf.st_mode)
            or not stat.S_ISREG(inspected_leaf.st_mode)
            or inspected_leaf.st_dev != root.initial_stat.st_dev
            or inspected_leaf.st_uid != os.geteuid()
            or inspected_leaf.st_nlink != 1
            or stat.S_IMODE(inspected_leaf.st_mode) != 0o400
            or inspected_leaf.st_size != expected_byte_count
        ):
            raise ColdStorageError(
                "source must be a same-filesystem owner-owned mode-0400 "
                "single-link regular file, not a symlink, with the expected byte count"
            )
        leaf_fd = os.open(leaf, _file_read_flags(), dir_fd=parent_fd)
        try:
            opened_leaf = os.fstat(leaf_fd)
            if (
                _fingerprint(opened_leaf) != _fingerprint(inspected_leaf)
                or _mount_id(leaf_fd) != root.mount_id
            ):
                raise ColdStorageError(
                    "sealed source leaf changed or crossed a nested mount boundary"
                )
        finally:
            os.close(leaf_fd)
    except FileNotFoundError as error:
        raise ColdStorageError("sealed source path does not exist") from error
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _default_device_id_provider(path: Path, descriptor: int) -> int:
    del path
    return os.fstat(descriptor).st_dev


def _link_fd_noreplace(
    source_descriptor: int,
    _source_name: str,
    destination_directory_fd: int,
    destination_name: str,
) -> None:
    """Publish the retained source inode without consulting its mutable name."""

    libc = ctypes.CDLL(None, use_errno=True)
    operation = getattr(libc, "linkat", None)
    if operation is None:
        raise ColdStorageError(
            "host libc lacks linkat; refusing unsafe cold-storage publication"
        )
    operation.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
    ]
    operation.restype = ctypes.c_int
    if (
        operation(
            source_descriptor,
            b"",
            destination_directory_fd,
            os.fsencode(destination_name),
            AT_EMPTY_PATH,
        )
        == 0
    ):
        return
    error_number = ctypes.get_errno()
    if error_number in (errno.EEXIST, errno.ENOTEMPTY):
        raise FileExistsError(error_number, os.strerror(error_number), destination_name)
    raise ColdStorageError(
        f"fd-bound no-replace publication failed: {os.strerror(error_number)}"
    )


def _validate_request(request: TransferRequest) -> TransferRequest:
    source_root = _validate_absolute_root(request.source_root, "source root")
    destination_root = _validate_absolute_root(
        request.destination_root, "destination root"
    )
    receipt_root = _validate_absolute_root(request.receipt_root, "receipt root")
    source_relative_path = _validate_relative_path(
        request.source_relative_path, "source relative path"
    )
    if source_root == receipt_root:
        raise ColdStorageError(
            "receipt root must be a separate directory from the sealed source root"
        )
    if source_root in receipt_root.parents or receipt_root in source_root.parents:
        raise ColdStorageError(
            "source and receipt roots must be disjoint directory trees"
        )
    if not SHA256_RE.fullmatch(request.expected_sha256):
        raise ColdStorageError("expected SHA-256 must be 64 lowercase hexadecimal digits")
    if (
        isinstance(request.expected_byte_count, bool)
        or not isinstance(request.expected_byte_count, int)
        or request.expected_byte_count < 1
        or request.expected_byte_count > MAX_BYTE_COUNT
    ):
        raise ColdStorageError("expected byte count must be an integer in 1..2^63-1")
    if (
        isinstance(request.free_space_floor_bytes, bool)
        or not isinstance(request.free_space_floor_bytes, int)
        or request.free_space_floor_bytes < 0
        or request.free_space_floor_bytes > MAX_BYTE_COUNT
    ):
        raise ColdStorageError("free-space floor must be an integer in 0..2^63-1")
    if request.expected_byte_count > MAX_BYTE_COUNT - request.free_space_floor_bytes:
        raise ColdStorageError("required destination space exceeds the supported range")
    return TransferRequest(
        source_root=source_root,
        source_relative_path=source_relative_path,
        destination_root=destination_root,
        receipt_root=receipt_root,
        expected_sha256=request.expected_sha256,
        expected_byte_count=request.expected_byte_count,
        free_space_floor_bytes=request.free_space_floor_bytes,
    )


def _request_object(request: TransferRequest) -> dict[str, Any]:
    return {
        "source_root": str(request.source_root),
        "source_relative_path": request.source_relative_path.as_posix(),
        "destination_root": str(request.destination_root),
        "receipt_root": str(request.receipt_root),
        "expected_sha256": request.expected_sha256,
        "expected_byte_count": request.expected_byte_count,
        "free_space_floor_bytes": request.free_space_floor_bytes,
    }


def _validate_sealed_file(
    directory_fd: int,
    name: str,
    *,
    expected_sha256: str,
    expected_byte_count: int,
    label: str,
) -> tuple[str, os.stat_result]:
    try:
        inspected = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except OSError as error:
        raise ColdStorageError(f"cannot inspect {label}: {error}") from error
    if (
        stat.S_ISLNK(inspected.st_mode)
        or not stat.S_ISREG(inspected.st_mode)
        or inspected.st_nlink != 1
        or inspected.st_uid != os.geteuid()
        or stat.S_IMODE(inspected.st_mode) != 0o400
        or inspected.st_size != expected_byte_count
        or inspected.st_dev != os.fstat(directory_fd).st_dev
    ):
        raise ColdStorageError(
            f"{label} must be an owner-owned mode-0400 single-link regular file "
            "with the expected byte count"
        )
    try:
        descriptor = os.open(name, _file_read_flags(), dir_fd=directory_fd)
    except OSError as error:
        raise ColdStorageError(f"cannot open {label}: {error}") from error
    try:
        opened = os.fstat(descriptor)
        if _fingerprint(opened) != _fingerprint(inspected):
            raise ColdStorageError(f"{label} changed while opening")
        if _mount_id(descriptor) != _mount_id(directory_fd):
            raise ColdStorageError(f"{label} crossed a nested mount boundary")
        observed_sha256 = _hash_descriptor(descriptor, expected_byte_count, label)
        if observed_sha256 != expected_sha256:
            raise ColdStorageError(f"{label} does not match its content address")
        if _fingerprint(os.fstat(descriptor)) != _fingerprint(opened):
            raise ColdStorageError(f"{label} changed during readback")
        linked = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if _fingerprint(linked) != _fingerprint(opened):
            raise ColdStorageError(f"{label} path changed during readback")
        os.fsync(descriptor)
        return observed_sha256, opened
    finally:
        os.close(descriptor)


def _retain_verified_target(
    directory_fd: int, expected: os.stat_result
) -> tuple[int, os.stat_result]:
    try:
        descriptor = os.open("payload", _file_read_flags(), dir_fd=directory_fd)
    except OSError as error:
        raise ColdStorageError(
            f"cannot retain verified cold-storage target: {error}"
        ) from error
    try:
        opened = os.fstat(descriptor)
        linked = os.stat("payload", dir_fd=directory_fd, follow_symlinks=False)
        if (
            _fingerprint(opened) != _fingerprint(expected)
            or _fingerprint(linked) != _fingerprint(expected)
            or _mount_id(descriptor) != _mount_id(directory_fd)
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o400
        ):
            raise ColdStorageError(
                "verified cold-storage target changed before retention"
            )
        return descriptor, opened
    except Exception:
        os.close(descriptor)
        raise


def _verify_retained_target(
    directory_fd: int, descriptor: int, expected: os.stat_result
) -> None:
    try:
        opened = os.fstat(descriptor)
        linked = os.stat("payload", dir_fd=directory_fd, follow_symlinks=False)
    except OSError as error:
        raise ColdStorageError(
            "verified cold-storage target disappeared before receipt publication"
        ) from error
    if (
        stat.S_ISLNK(linked.st_mode)
        or _fingerprint(opened) != _fingerprint(expected)
        or _fingerprint(linked) != _fingerprint(expected)
        or opened.st_nlink != 1
        or stat.S_IMODE(opened.st_mode) != 0o400
        or _mount_id(descriptor) != _mount_id(directory_fd)
    ):
        raise ColdStorageError(
            "verified cold-storage target changed before receipt publication"
        )


def _copy_and_publish(
    *,
    source: PinnedRegularFile,
    destination_directory_fd: int,
    expected_sha256: str,
    expected_byte_count: int,
    expected_target_present: bool,
    publish_fd_noreplace: Callable[[int, str, int, str], None],
    fault_hook: Callable[[str], None] | None,
) -> tuple[str, dict[str, Any], os.stat_result]:
    final_name = "payload"
    target_present_now = _entry_exists(destination_directory_fd, final_name)
    if expected_target_present and not target_present_now:
        raise ColdStorageError(
            "cold-storage target disappeared after the locked capacity preflight"
        )
    if target_present_now:
        archive_sha256, existing_stat = _validate_sealed_file(
            destination_directory_fd,
            final_name,
            expected_sha256=expected_sha256,
            expected_byte_count=expected_byte_count,
            label="existing cold-storage object",
        )
        _verify_source(source)
        return "recovered_existing", {
            "copy_stream_sha256": None,
            "archive_readback_sha256": archive_sha256,
            "archive_readback_byte_count": expected_byte_count,
            "archive_full_read_count": 1,
            "bounded_archive_read": True,
            "temporary_file_fsynced": False,
            "final_file_fsynced": True,
            "destination_directories_fsynced": False,
            "atomic_no_replace": True,
            "source_reverified": True,
        }, existing_stat

    temporary_label = "<unnamed-payload>"
    tmpfile_flag = getattr(os, "O_TMPFILE", 0)
    if not tmpfile_flag:
        raise ColdStorageError(
            "host Python lacks O_TMPFILE; refusing crash-unsafe staging"
        )
    flags = (
        os.O_RDWR
        | tmpfile_flag
        | getattr(os, "O_CLOEXEC", 0)
    )
    temporary_fd: int | None = None
    try:
        try:
            temporary_fd = os.open(
                ".", flags, 0o600, dir_fd=destination_directory_fd
            )
        except OSError as error:
            raise ColdStorageError(
                f"cannot create unnamed cold-storage staging inode: {error}"
            ) from error
        temporary_opened = os.fstat(temporary_fd)
        if (
            not stat.S_ISREG(temporary_opened.st_mode)
            or temporary_opened.st_uid != os.geteuid()
            or temporary_opened.st_nlink != 0
            or temporary_opened.st_dev != os.fstat(destination_directory_fd).st_dev
            or _mount_id(temporary_fd) != _mount_id(destination_directory_fd)
        ):
            raise ColdStorageError(
                "unnamed cold-storage staging inode has unsafe topology"
            )
        copied_digest = hashlib.sha256()
        offset = 0
        while offset < expected_byte_count:
            try:
                chunk = os.pread(
                    source.descriptor,
                    min(CHUNK_SIZE, expected_byte_count - offset),
                    offset,
                )
            except OSError as error:
                raise ColdStorageError(f"cannot read sealed source: {error}") from error
            if not chunk:
                raise ColdStorageError("sealed source ended during cold copy")
            copied_digest.update(chunk)
            written = 0
            while written < len(chunk):
                try:
                    count = os.write(temporary_fd, chunk[written:])
                except OSError as error:
                    raise ColdStorageError(
                        f"cannot write cold-storage temporary object: {error}"
                    ) from error
                if count <= 0:
                    raise ColdStorageError("cold-storage temporary write made no progress")
                written += count
            offset += len(chunk)
        copied_sha256 = copied_digest.hexdigest()
        if copied_sha256 != expected_sha256:
            raise ColdStorageError("copy-time SHA-256 differs from the sealed request")
        if os.fstat(temporary_fd).st_size != expected_byte_count:
            raise ColdStorageError("cold-storage temporary object has wrong byte count")
        os.fsync(temporary_fd)
        if fault_hook is not None:
            fault_hook("after_temp_fsync")
        archive_sha256 = _hash_descriptor(
            temporary_fd, expected_byte_count, "cold-storage readback"
        )
        if archive_sha256 != expected_sha256:
            raise ColdStorageError("cold-storage readback SHA-256 differs from request")
        _verify_source(source)
        os.fchmod(temporary_fd, 0o400)
        os.fsync(temporary_fd)
        sealed_temporary = os.fstat(temporary_fd)
        try:
            publish_fd_noreplace(
                temporary_fd,
                temporary_label,
                destination_directory_fd,
                final_name,
            )
            os.fsync(temporary_fd)
            admission = "copied"
        except FileExistsError as error:
            raise ColdStorageError(
                "cold-storage target appeared during no-replace publication; "
                "rerun to verify it without a second archive read"
            ) from error
        os.fsync(destination_directory_fd)
        if fault_hook is not None:
            fault_hook("after_payload_publish")
        linked = os.stat(
            final_name, dir_fd=destination_directory_fd, follow_symlinks=False
        )
        opened_after_publish = os.fstat(temporary_fd)
        stable_fields = (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_nlink",
            "st_uid",
            "st_gid",
            "st_size",
        )
        if (
            sealed_temporary.st_nlink != 0
            or linked.st_nlink != 1
            or opened_after_publish.st_nlink != 1
            or any(
                getattr(linked, field) != getattr(opened_after_publish, field)
                for field in stable_fields
            )
        ):
            raise ColdStorageError(
                "published cold-storage object is not the verified single-link "
                "temporary inode"
            )
        copy_stream_sha256: str | None = copied_sha256
        temporary_file_fsynced = True
        _verify_source(source)
        return admission, {
            "copy_stream_sha256": copy_stream_sha256,
            "archive_readback_sha256": archive_sha256,
            "archive_readback_byte_count": expected_byte_count,
            "archive_full_read_count": 1,
            "bounded_archive_read": True,
            "temporary_file_fsynced": temporary_file_fsynced,
            "final_file_fsynced": True,
            "destination_directories_fsynced": False,
            "atomic_no_replace": True,
            "source_reverified": True,
        }, opened_after_publish
    finally:
        if temporary_fd is not None:
            os.close(temporary_fd)


def _receipt_identity(receipt: dict[str, Any]) -> str:
    material = dict(receipt)
    material.pop("identity_sha256", None)
    material.pop("receipt_id", None)
    return sha256_bytes(canonical_bytes(material))


def _exact_object_keys(value: Any, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise ColdStorageError(f"existing receipt has invalid {label} fields")
    return value


def _receipt_nonnegative_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ColdStorageError(f"existing receipt has invalid {label}")
    return value


def _validate_receipt_contract(
    receipt: dict[str, Any],
    *,
    expected_request: dict[str, Any],
    expected_transfer_id: str,
    expected_destination_relative_path: str,
) -> None:
    """Replay the strict receipt schema with standard-library checks."""

    _exact_object_keys(
        receipt,
        {
            "schema_version",
            "kind",
            "producer",
            "transfer_id",
            "status",
            "completed_at",
            "request",
            "source",
            "preflight",
            "destination",
            "verification",
            "policy",
            "catalog_location_candidate",
            "identity_sha256",
            "receipt_id",
        },
        "top-level",
    )
    completed_at = receipt.get("completed_at")
    producer = _exact_object_keys(
        receipt.get("producer"), {"name", "version"}, "producer"
    )
    valid_timestamp = False
    if isinstance(completed_at, str) and TIMESTAMP_RE.fullmatch(completed_at):
        try:
            datetime.strptime(completed_at, "%Y-%m-%dT%H:%M:%SZ")
            valid_timestamp = True
        except ValueError:
            pass
    if (
        type(receipt.get("schema_version")) is not int
        or receipt.get("schema_version") != SCHEMA_VERSION
        or receipt.get("kind") != "cold_storage_transfer_receipt"
        or producer.get("name") != PRODUCER_NAME
        or not isinstance(producer.get("version"), str)
        or re.fullmatch(r"^[0-9]+\.[0-9]+\.[0-9]+$", producer["version"])
        is None
        or receipt.get("transfer_id") != expected_transfer_id
        or receipt.get("status") != "completed"
        or not valid_timestamp
        or receipt.get("request") != expected_request
    ):
        raise ColdStorageError("existing transfer receipt has invalid identity fields")

    expected_sha256 = expected_request["expected_sha256"]
    expected_byte_count = expected_request["expected_byte_count"]
    expected_uri = (
        Path(expected_request["destination_root"])
        / expected_destination_relative_path
    ).as_uri()

    source = _exact_object_keys(
        receipt.get("source"),
        {
            "sha256",
            "byte_count",
            "identity_before",
            "identity_after",
            "regular_file",
            "single_link",
            "owner_sealed",
            "symlink_components_followed",
            "unchanged",
        },
        "source",
    )
    identity_keys = {
        "device",
        "inode",
        "byte_count",
        "mtime_ns",
        "ctime_ns",
        "owner_uid",
        "mode",
        "nlink",
    }
    identity_before = _exact_object_keys(
        source.get("identity_before"), identity_keys, "source identity_before"
    )
    identity_after = _exact_object_keys(
        source.get("identity_after"), identity_keys, "source identity_after"
    )
    source_byte_count = _receipt_nonnegative_integer(
        source.get("byte_count"), "source byte_count"
    )
    for identity in (identity_before, identity_after):
        for key in ("device", "inode", "byte_count", "owner_uid", "nlink"):
            _receipt_nonnegative_integer(identity.get(key), f"source identity {key}")
        if (
            isinstance(identity.get("mtime_ns"), bool)
            or not isinstance(identity.get("mtime_ns"), int)
            or isinstance(identity.get("ctime_ns"), bool)
            or not isinstance(identity.get("ctime_ns"), int)
            or identity.get("byte_count") != expected_byte_count
            or identity.get("mode") != "0400"
            or identity.get("nlink") != 1
        ):
            raise ColdStorageError("existing receipt has invalid source identity")
    if (
        source.get("sha256") != expected_sha256
        or source_byte_count != expected_byte_count
        or identity_before != identity_after
        or source.get("regular_file") is not True
        or source.get("single_link") is not True
        or source.get("owner_sealed") is not True
        or source.get("symlink_components_followed") is not False
        or source.get("unchanged") is not True
    ):
        raise ColdStorageError("existing receipt has invalid sealed-source evidence")

    preflight = _exact_object_keys(
        receipt.get("preflight"),
        {
            "source_root_device",
            "receipt_root_device",
            "destination_root_device",
            "source_receipt_same_filesystem",
            "destination_distinct_filesystem",
            "destination_free_bytes_before",
            "required_new_bytes",
            "free_space_floor_bytes",
            "projected_free_bytes",
            "space_sufficient",
            "archive_tree_scanned",
        },
        "preflight",
    )
    for key in (
        "source_root_device",
        "receipt_root_device",
        "destination_root_device",
        "destination_free_bytes_before",
        "required_new_bytes",
        "free_space_floor_bytes",
        "projected_free_bytes",
    ):
        _receipt_nonnegative_integer(preflight.get(key), f"preflight {key}")
    if (
        preflight.get("source_root_device")
        != preflight.get("receipt_root_device")
        or preflight.get("source_root_device")
        == preflight.get("destination_root_device")
        or preflight.get("source_receipt_same_filesystem") is not True
        or preflight.get("destination_distinct_filesystem") is not True
        or preflight.get("required_new_bytes") not in {0, expected_byte_count}
        or preflight.get("free_space_floor_bytes")
        != expected_request["free_space_floor_bytes"]
        or preflight.get("projected_free_bytes")
        != preflight.get("destination_free_bytes_before")
        - preflight.get("required_new_bytes")
        or preflight.get("projected_free_bytes")
        < preflight.get("free_space_floor_bytes")
        or preflight.get("space_sufficient") is not True
        or preflight.get("archive_tree_scanned") is not False
    ):
        raise ColdStorageError("existing receipt has invalid capacity preflight")

    destination = _exact_object_keys(
        receipt.get("destination"),
        {
            "relative_path",
            "storage_uri",
            "storage_class",
            "sha256",
            "byte_count",
            "mode",
            "nlink",
            "admission",
            "verified_at",
        },
        "destination",
    )
    admission = destination.get("admission")
    destination_byte_count = _receipt_nonnegative_integer(
        destination.get("byte_count"), "destination byte_count"
    )
    destination_nlink = _receipt_nonnegative_integer(
        destination.get("nlink"), "destination nlink"
    )
    if (
        destination.get("relative_path") != expected_destination_relative_path
        or destination.get("storage_uri") != expected_uri
        or destination.get("storage_class") != "local_cold_archive"
        or destination.get("sha256") != expected_sha256
        or destination_byte_count != expected_byte_count
        or destination.get("mode") != "0400"
        or destination_nlink != 1
        or admission not in {"copied", "existing_verified", "recovered_existing"}
        or destination.get("verified_at") != completed_at
    ):
        raise ColdStorageError("existing receipt has invalid destination evidence")

    verification = _exact_object_keys(
        receipt.get("verification"),
        {
            "copy_stream_sha256",
            "archive_readback_sha256",
            "archive_readback_byte_count",
            "archive_full_read_count",
            "bounded_archive_read",
            "temporary_file_fsynced",
            "final_file_fsynced",
            "destination_directories_fsynced",
            "atomic_no_replace",
            "source_reverified",
        },
        "verification",
    )
    copied = admission == "copied"
    archive_readback_byte_count = _receipt_nonnegative_integer(
        verification.get("archive_readback_byte_count"),
        "verification archive_readback_byte_count",
    )
    archive_full_read_count = _receipt_nonnegative_integer(
        verification.get("archive_full_read_count"),
        "verification archive_full_read_count",
    )
    if (
        verification.get("copy_stream_sha256")
        != (expected_sha256 if copied else None)
        or verification.get("archive_readback_sha256") != expected_sha256
        or archive_readback_byte_count != expected_byte_count
        or archive_full_read_count != 1
        or verification.get("bounded_archive_read") is not True
        or verification.get("temporary_file_fsynced") is not copied
        or verification.get("final_file_fsynced") is not True
        or verification.get("destination_directories_fsynced") is not True
        or verification.get("atomic_no_replace") is not True
        or verification.get("source_reverified") is not True
    ):
        raise ColdStorageError("existing receipt has invalid verification evidence")

    policy = _exact_object_keys(
        receipt.get("policy"),
        {
            "source_deleted",
            "source_mutated",
            "network_actions_performed",
            "media_processing_performed",
            "archive_indexing_performed",
            "catalogue_mutated",
            "catalogue_import_authority",
            "publication_authority",
            "archive_read_policy",
        },
        "policy",
    )
    if (
        any(
            policy.get(key) is not False
            for key in (
                "source_deleted",
                "source_mutated",
                "network_actions_performed",
                "media_processing_performed",
                "archive_indexing_performed",
                "catalogue_mutated",
            )
        )
        or policy.get("catalogue_import_authority") != "none"
        or policy.get("publication_authority") != "none"
        or policy.get("archive_read_policy")
        != "one_explicit_object_checksum_verification_only"
    ):
        raise ColdStorageError("existing receipt has invalid policy boundary")
    catalog_candidate = _exact_object_keys(
        receipt.get("catalog_location_candidate"),
        {
            "media_id",
            "storage_uri",
            "storage_class",
            "verified_at",
            "is_primary",
            "import_authority",
        },
        "catalogue handoff",
    )
    if (
        catalog_candidate.get("media_id") != f"media_sha256_{expected_sha256}"
        or catalog_candidate.get("storage_uri") != expected_uri
        or catalog_candidate.get("storage_class") != "local_cold_archive"
        or catalog_candidate.get("verified_at") != completed_at
        or type(catalog_candidate.get("is_primary")) is not int
        or catalog_candidate.get("is_primary") != 0
        or catalog_candidate.get("import_authority") != "none"
    ):
        raise ColdStorageError("existing receipt has invalid catalogue handoff")
    identity_sha256 = receipt.get("identity_sha256")
    if (
        not isinstance(identity_sha256, str)
        or SHA256_RE.fullmatch(identity_sha256) is None
        or identity_sha256 != _receipt_identity(receipt)
        or receipt.get("receipt_id") != f"coldreceipt_{identity_sha256[:32]}"
    ):
        raise ColdStorageError("existing receipt has invalid receipt identity")


def _read_receipt(
    directory_fd: int,
    *,
    expected_request: dict[str, Any],
    expected_transfer_id: str,
    expected_destination_relative_path: str,
) -> dict[str, Any]:
    try:
        inspected = os.stat("receipt.json", dir_fd=directory_fd, follow_symlinks=False)
    except OSError as error:
        raise ColdStorageError(f"cannot inspect existing transfer receipt: {error}") from error
    if (
        stat.S_ISLNK(inspected.st_mode)
        or not stat.S_ISREG(inspected.st_mode)
        or inspected.st_nlink != 1
        or inspected.st_uid != os.geteuid()
        or stat.S_IMODE(inspected.st_mode) != 0o400
        or inspected.st_size < 1
        or inspected.st_size > MAX_RECEIPT_BYTES
    ):
        raise ColdStorageError(
            "existing transfer receipt is not an immutable owner-private single-link file"
        )
    descriptor = os.open("receipt.json", _file_read_flags(), dir_fd=directory_fd)
    try:
        opened = os.fstat(descriptor)
        if _fingerprint(opened) != _fingerprint(inspected):
            raise ColdStorageError("existing transfer receipt changed while opening")
        if _mount_id(descriptor) != _mount_id(directory_fd):
            raise ColdStorageError("existing transfer receipt crossed a nested mount")
        body = bytearray()
        offset = 0
        while offset < opened.st_size:
            chunk = os.pread(descriptor, opened.st_size - offset, offset)
            if not chunk:
                raise ColdStorageError("existing transfer receipt ended during read")
            body.extend(chunk)
            offset += len(chunk)
        if os.pread(descriptor, 1, opened.st_size):
            raise ColdStorageError("existing transfer receipt grew during read")
        linked = os.stat("receipt.json", dir_fd=directory_fd, follow_symlinks=False)
        if _fingerprint(linked) != _fingerprint(opened):
            raise ColdStorageError("existing transfer receipt changed during read")
    finally:
        os.close(descriptor)
    try:
        receipt = strict_json_object(
            bytes(body), "existing cold-storage transfer receipt"
        )
    except AcquisitionError as error:
        raise ColdStorageError(str(error)) from error
    _validate_receipt_contract(
        receipt,
        expected_request=expected_request,
        expected_transfer_id=expected_transfer_id,
        expected_destination_relative_path=expected_destination_relative_path,
    )
    return receipt


def _write_or_replay_receipt(
    *,
    directory_fd: int,
    receipt: dict[str, Any],
    publish_fd_noreplace: Callable[[int, str, int, str], None],
    pre_publish_check: Callable[[], None],
) -> dict[str, Any]:
    body = pretty_json(receipt).encode("utf-8")
    if len(body) > MAX_RECEIPT_BYTES:
        raise ColdStorageError("cold-storage receipt exceeds its fixed size bound")
    temporary_label = "<unnamed-receipt>"
    tmpfile_flag = getattr(os, "O_TMPFILE", 0)
    if not tmpfile_flag:
        raise ColdStorageError(
            "host Python lacks O_TMPFILE; refusing crash-unsafe receipt staging"
        )
    flags = (
        os.O_RDWR
        | tmpfile_flag
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptor: int | None = None
    try:
        try:
            descriptor = os.open(".", flags, 0o600, dir_fd=directory_fd)
        except OSError as error:
            raise ColdStorageError(
                f"cannot create unnamed receipt staging inode: {error}"
            ) from error
        temporary_opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(temporary_opened.st_mode)
            or temporary_opened.st_uid != os.geteuid()
            or temporary_opened.st_nlink != 0
            or temporary_opened.st_dev != os.fstat(directory_fd).st_dev
            or _mount_id(descriptor) != _mount_id(directory_fd)
        ):
            raise ColdStorageError("unnamed receipt staging inode has unsafe topology")
        offset = 0
        while offset < len(body):
            count = os.write(descriptor, body[offset:])
            if count <= 0:
                raise ColdStorageError("receipt write made no progress")
            offset += count
        os.fsync(descriptor)
        os.fchmod(descriptor, 0o400)
        os.fsync(descriptor)
        pre_publish_check()
        try:
            publish_fd_noreplace(
                descriptor,
                temporary_label,
                directory_fd,
                "receipt.json",
            )
            os.fsync(descriptor)
            os.fsync(directory_fd)
            published = _read_receipt(
                directory_fd,
                expected_request=receipt["request"],
                expected_transfer_id=receipt["transfer_id"],
                expected_destination_relative_path=receipt["destination"]["relative_path"],
            )
            if published != receipt:
                raise ColdStorageError(
                    "published transfer receipt bytes differ from the sealed receipt"
                )
            return published
        except FileExistsError:
            existing = _read_receipt(
                directory_fd,
                expected_request=receipt["request"],
                expected_transfer_id=receipt["transfer_id"],
                expected_destination_relative_path=receipt["destination"]["relative_path"],
            )
            if existing != receipt:
                raise ColdStorageError(
                    "pre-existing transfer receipt differs from the sealed receipt"
                )
            return existing
    finally:
        if descriptor is not None:
            os.close(descriptor)


def run_transfer(
    request: TransferRequest,
    *,
    dry_run: bool = False,
    device_id_provider: Callable[[Path, int], int] | None = None,
    statvfs_provider: Callable[[int], Any] = os.fstatvfs,
    publish_fd_noreplace: Callable[
        [int, str, int, str], None
    ] = _link_fd_noreplace,
    fault_hook: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Validate and optionally perform one bounded, non-destructive transfer."""

    request = _validate_request(request)
    device_provider = device_id_provider or _default_device_id_provider
    request_object = _request_object(request)
    request_sha256 = sha256_bytes(canonical_bytes(request_object))
    transfer_id = f"coldtx_{request_sha256[:32]}"
    destination_parts = (
        "media",
        "sha256",
        request.expected_sha256[:2],
        request.expected_sha256,
    )
    destination_relative_path = "/".join((*destination_parts, "payload"))
    receipt_parts = ("cold-storage-transfers", transfer_id)

    roots: list[RetainedRoot] = []
    source: PinnedRegularFile | None = None
    destination_chain: RetainedDirectoryChain | None = None
    receipt_chain: RetainedDirectoryChain | None = None
    destination_payload_fd: int | None = None
    destination_payload_stat: os.stat_result | None = None
    destination_lock_held = False
    try:
        source_root = RetainedRoot.open(request.source_root, "source root")
        roots.append(source_root)
        destination_root = RetainedRoot.open(
            request.destination_root, "destination root"
        )
        roots.append(destination_root)
        receipt_root = RetainedRoot.open(request.receipt_root, "receipt root")
        roots.append(receipt_root)
        if (
            source_root.initial_stat.st_dev,
            source_root.initial_stat.st_ino,
        ) == (
            receipt_root.initial_stat.st_dev,
            receipt_root.initial_stat.st_ino,
        ):
            raise ColdStorageError(
                "source and receipt roots resolve to the same directory identity"
            )

        source_device = device_provider(source_root.path, source_root.descriptor)
        destination_device = device_provider(
            destination_root.path, destination_root.descriptor
        )
        receipt_device = device_provider(receipt_root.path, receipt_root.descriptor)
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in (source_device, destination_device, receipt_device)
        ):
            raise ColdStorageError("device provider returned an invalid device identity")
        if source_device != receipt_device:
            raise ColdStorageError(
                "receipt root must be on the source/main-drive filesystem"
            )
        if source_device == destination_device:
            raise ColdStorageError(
                "destination root must be on a filesystem distinct from the source"
            )

        _preflight_source_path(
            source_root,
            request.source_relative_path,
            expected_byte_count=request.expected_byte_count,
        )
        source_path = request.source_root / request.source_relative_path
        try:
            source = PinnedRegularFile.open(
                source_path,
                root=request.source_root,
                maximum=request.expected_byte_count,
                capture=False,
                label="sealed transfer source",
            )
        except AcquisitionError as error:
            raise ColdStorageError(str(error)) from error
        if (
            _directory_fingerprint(source.root_stat)
            != _directory_fingerprint(source_root.initial_stat)
        ):
            raise ColdStorageError("source root changed between retained opens")
        observed = source.initial_stat
        if (
            observed.st_uid != os.geteuid()
            or stat.S_IMODE(observed.st_mode) != 0o400
            or observed.st_nlink != 1
            or observed.st_size != request.expected_byte_count
        ):
            raise ColdStorageError(
                "source must be an owner-owned mode-0400 single-link regular file "
                "with the expected byte count"
            )
        if source.digest != request.expected_sha256:
            raise ColdStorageError("source SHA-256 does not match the transfer request")
        if observed.st_dev != source_root.initial_stat.st_dev or any(
            component_stat.st_dev != source_root.initial_stat.st_dev
            for _, _, _, component_stat in source.components
        ):
            raise ColdStorageError("source path crossed a nested filesystem boundary")
        if _mount_id(source.descriptor) != source_root.mount_id or any(
            _mount_id(component_fd) != source_root.mount_id
            for _, _, component_fd, _ in source.components
        ):
            raise ColdStorageError("source path crossed a nested mount boundary")

        if not dry_run:
            try:
                fcntl.flock(
                    destination_root.descriptor,
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
            except OSError as error:
                raise ColdStorageError(
                    "another cold-storage transfer holds the destination-root lock"
                ) from error
            destination_lock_held = True

            # A completed receipt is authoritative durable state. Replay reads
            # exactly that receipt and the one named cold object, but writes nothing.
            receipt_probe = RetainedDirectoryChain(receipt_root)
            try:
                existing_receipt_directory = receipt_probe.open_existing(receipt_parts)
                receipt_exists = (
                    existing_receipt_directory is not None
                    and _entry_exists(existing_receipt_directory, "receipt.json")
                )
                if receipt_exists:
                    existing_receipt = _read_receipt(
                        existing_receipt_directory,
                        expected_request=request_object,
                        expected_transfer_id=transfer_id,
                        expected_destination_relative_path=destination_relative_path,
                    )
                    replay_preflight = existing_receipt["preflight"]
                    if (
                        replay_preflight["source_root_device"] != source_device
                        or replay_preflight["receipt_root_device"] != receipt_device
                        or replay_preflight["destination_root_device"]
                        != destination_device
                    ):
                        raise ColdStorageError(
                            "existing receipt filesystem identities differ from the "
                            "retained roots"
                        )
                    if existing_receipt.get("source", {}).get(
                        "identity_before"
                    ) != _file_identity(observed):
                        raise ColdStorageError(
                            "existing receipt source identity differs from the "
                            "retained source"
                        )
                    destination_probe = RetainedDirectoryChain(destination_root)
                    try:
                        existing_destination_directory = (
                            destination_probe.open_existing(destination_parts)
                        )
                        if existing_destination_directory is None:
                            raise ColdStorageError(
                                "existing transfer receipt has no exact "
                                "cold-storage target"
                            )
                        _validate_sealed_file(
                            existing_destination_directory,
                            "payload",
                            expected_sha256=request.expected_sha256,
                            expected_byte_count=request.expected_byte_count,
                            label="receipt-bound cold-storage object",
                        )
                        destination_probe.verify()
                    finally:
                        destination_probe.close()
                    _verify_source(source)
                    receipt_probe.verify()
                    for root in roots:
                        root.verify()
                    return existing_receipt
            finally:
                receipt_probe.close()

        target_present = _probe_target_without_read(
            destination_root, destination_parts, request.expected_byte_count
        )

        try:
            capacity = statvfs_provider(destination_root.descriptor)
            free_bytes_before = capacity.f_bavail * capacity.f_frsize
        except (AttributeError, OSError, OverflowError, TypeError) as error:
            raise ColdStorageError(
                f"cannot read destination free-space capacity: {error}"
            ) from error
        if (
            isinstance(free_bytes_before, bool)
            or not isinstance(free_bytes_before, int)
            or free_bytes_before < 0
            or free_bytes_before > MAX_BYTE_COUNT
        ):
            raise ColdStorageError("destination capacity is outside the supported range")
        required_new_bytes = 0 if target_present else request.expected_byte_count
        projected_free_bytes = free_bytes_before - required_new_bytes
        if projected_free_bytes < request.free_space_floor_bytes:
            raise ColdStorageError(
                "destination lacks the requested post-transfer free-space floor"
            )

        preflight = {
            "source_root_device": source_device,
            "receipt_root_device": receipt_device,
            "destination_root_device": destination_device,
            "source_receipt_same_filesystem": True,
            "destination_distinct_filesystem": True,
            "destination_free_bytes_before": free_bytes_before,
            "required_new_bytes": required_new_bytes,
            "free_space_floor_bytes": request.free_space_floor_bytes,
            "projected_free_bytes": projected_free_bytes,
            "space_sufficient": True,
            "archive_tree_scanned": False,
        }
        if dry_run:
            _verify_source(source)
            for root in roots:
                root.verify()
            return {
                "schema_version": SCHEMA_VERSION,
                "kind": "cold_storage_transfer_dry_run",
                "implementation_version": IMPLEMENTATION_VERSION,
                "transfer_id": transfer_id,
                "status": "validated",
                "request": request_object,
                "destination": {
                    "relative_path": destination_relative_path,
                    "storage_uri": (
                        request.destination_root / destination_relative_path
                    ).as_uri(),
                    "storage_class": "local_cold_archive",
                    "target_present_unverified": target_present,
                },
                "preflight": preflight,
                "writes_performed": False,
            }

        destination_chain = RetainedDirectoryChain(destination_root)
        destination_directory_fd = destination_chain.ensure(destination_parts)
        admission, verification, verified_target_stat = _copy_and_publish(
            source=source,
            destination_directory_fd=destination_directory_fd,
            expected_sha256=request.expected_sha256,
            expected_byte_count=request.expected_byte_count,
            expected_target_present=target_present,
            publish_fd_noreplace=publish_fd_noreplace,
            fault_hook=fault_hook,
        )
        destination_payload_fd, destination_payload_stat = _retain_verified_target(
            destination_directory_fd, verified_target_stat
        )
        destination_chain.sync()
        verification["destination_directories_fsynced"] = True
        after = _verify_source(source)
        for root in roots:
            root.verify()
        if fault_hook is not None:
            fault_hook("before_receipt_write")

        completed_at = utc_now()
        destination_uri = (
            request.destination_root / destination_relative_path
        ).as_uri()
        receipt = {
            "schema_version": SCHEMA_VERSION,
            "kind": "cold_storage_transfer_receipt",
            "producer": {
                "name": PRODUCER_NAME,
                "version": IMPLEMENTATION_VERSION,
            },
            "transfer_id": transfer_id,
            "status": "completed",
            "completed_at": completed_at,
            "request": request_object,
            "source": {
                "sha256": source.digest,
                "byte_count": observed.st_size,
                "identity_before": _file_identity(observed),
                "identity_after": _file_identity(after),
                "regular_file": True,
                "single_link": True,
                "owner_sealed": True,
                "symlink_components_followed": False,
                "unchanged": True,
            },
            "destination": {
                "relative_path": destination_relative_path,
                "storage_uri": destination_uri,
                "storage_class": "local_cold_archive",
                "sha256": request.expected_sha256,
                "byte_count": request.expected_byte_count,
                "mode": "0400",
                "nlink": 1,
                "admission": admission,
                "verified_at": completed_at,
            },
            "preflight": preflight,
            "verification": verification,
            "policy": {
                "source_deleted": False,
                "source_mutated": False,
                "network_actions_performed": False,
                "media_processing_performed": False,
                "archive_indexing_performed": False,
                "catalogue_mutated": False,
                "catalogue_import_authority": "none",
                "publication_authority": "none",
                "archive_read_policy": (
                    "one_explicit_object_checksum_verification_only"
                ),
            },
            "catalog_location_candidate": {
                "media_id": f"media_sha256_{request.expected_sha256}",
                "storage_uri": destination_uri,
                "storage_class": "local_cold_archive",
                "verified_at": completed_at,
                "is_primary": 0,
                "import_authority": "none",
            },
        }
        receipt["identity_sha256"] = _receipt_identity(receipt)
        receipt["receipt_id"] = (
            f"coldreceipt_{receipt['identity_sha256'][:32]}"
        )

        receipt_chain = RetainedDirectoryChain(receipt_root)
        receipt_directory_fd = receipt_chain.ensure(receipt_parts)

        def verify_before_receipt_publish() -> None:
            final_source = _verify_source(source)
            if _file_identity(final_source) != _file_identity(observed):
                raise ColdStorageError(
                    "sealed source changed before durable receipt publication"
                )
            if destination_payload_fd is None or destination_payload_stat is None:
                raise ColdStorageError("verified cold-storage target is not retained")
            _verify_retained_target(
                destination_directory_fd,
                destination_payload_fd,
                destination_payload_stat,
            )
            destination_chain.verify()
            for retained_root in roots:
                retained_root.verify()

        completed = _write_or_replay_receipt(
            directory_fd=receipt_directory_fd,
            receipt=receipt,
            publish_fd_noreplace=publish_fd_noreplace,
            pre_publish_check=verify_before_receipt_publish,
        )
        receipt_chain.sync()
        if completed.get("source", {}).get("identity_before") != _file_identity(
            observed
        ):
            raise ColdStorageError(
                "durable receipt source identity differs from this transfer source"
            )
        for root in roots:
            root.verify()
        return completed
    finally:
        if destination_payload_fd is not None:
            os.close(destination_payload_fd)
        if receipt_chain is not None:
            receipt_chain.close()
        if destination_chain is not None:
            destination_chain.close()
        if source is not None:
            source.close()
        if destination_lock_held:
            try:
                fcntl.flock(destination_root.descriptor, fcntl.LOCK_UN)
            except OSError:
                pass
        for root in reversed(roots):
            root.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser(
        "run", help="validate and copy one explicitly sealed file"
    )
    run_parser.add_argument("--source-root", type=Path, required=True)
    run_parser.add_argument("--source-relative-path", type=Path, required=True)
    run_parser.add_argument("--destination-root", type=Path, required=True)
    run_parser.add_argument("--receipt-root", type=Path, required=True)
    run_parser.add_argument("--expected-sha256", required=True)
    run_parser.add_argument("--expected-byte-count", type=int, required=True)
    run_parser.add_argument("--free-space-floor-bytes", type=int, required=True)
    run_parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    if arguments.command != "run":
        raise AssertionError("unreachable command")
    request = TransferRequest(
        source_root=arguments.source_root,
        source_relative_path=arguments.source_relative_path,
        destination_root=arguments.destination_root,
        receipt_root=arguments.receipt_root,
        expected_sha256=arguments.expected_sha256,
        expected_byte_count=arguments.expected_byte_count,
        free_space_floor_bytes=arguments.free_space_floor_bytes,
    )
    try:
        result = run_transfer(request, dry_run=arguments.dry_run)
    except (ColdStorageError, AcquisitionError, OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    sys.stdout.write(pretty_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
