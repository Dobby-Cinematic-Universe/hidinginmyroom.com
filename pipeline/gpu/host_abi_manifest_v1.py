#!/usr/bin/env python3
"""Canonical, replayable host shared-library closure for GPU ASR.

The execution image contains the application runtime and CUDA user libraries, but
the NVIDIA driver ABI and a small glibc/C++ support closure necessarily come from
the booted host.  This module turns that residual host ABI into an explicit
content-addressed contract.  It never executes an inspected ELF object and does
not trust ``ldd`` or the mutable loader cache.

Production launchers embed the validated manifest in their root-owned profile,
replay every alias and regular file through retained descriptors, and bind only
the named files into the sandbox.  Merely matching a package or driver version is
not sufficient authority.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib.util
import json
import mmap
import os
import posixpath
import re
import secrets
import stat
import struct
import sys
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Sequence


KIND = "himr_gpu_host_abi_manifest"
SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
MAX_JSON_BYTES = 4 * 1024 * 1024
MAX_LIBRARY_BYTES = 512 * 1024 * 1024
MAX_PROJECTION_FILE_BYTES = 2 * 1024 * 1024 * 1024
MAX_LIBRARIES = 256
MAX_CONSUMER_ELVES = 4096
MAX_RESOLUTION_STATES = 65536
MAX_DEPENDENCIES_PER_LIBRARY = 256
MAX_ALIAS_DEPTH = 16
MAX_KERNEL_REPORT_BYTES = 64 * 1024
PRODUCTION_LIBRARY_ROOTS = ("/usr/lib64",)
COLD_ROOT = PurePosixPath("/mnt/archive/HIMR")
# The launcher's LD_LIBRARY_PATH contains this one projected directory followed
# by the exact host ABI directory.  av.libs, ctranslate2.libs, and numpy.libs
# are reachable only when an individual consumer names them through $ORIGIN.
# The schema records DT_RPATH and DT_RUNPATH separately for inheritance, but a
# single context still rejects duplicate reachable providers instead of using
# loader precedence to choose between two projected files.
GLOBAL_IMAGE_LIBRARY_DIRECTORIES = frozenset(
    {"runtime/lib/python3.12/site-packages/nvidia/cublas/lib"}
)
PRIVATE_IMAGE_LIBRARY_DIRECTORIES = frozenset(
    {
        "runtime/lib/python3.12/site-packages/av.libs",
        "runtime/lib/python3.12/site-packages/ctranslate2.libs",
        "runtime/lib/python3.12/site-packages/numpy.libs",
    }
)
PYTHON_SITE_PACKAGES = PurePosixPath(
    "runtime/lib/python3.12/site-packages"
)
EXPLICIT_IMAGE_DLOPEN_ROOTS = frozenset(
    {
        "runtime/lib/python3.12/site-packages/nvidia/cublas/lib/libcublas.so.12",
        "runtime/lib/python3.12/site-packages/nvidia/cublas/lib/libcublasLt.so.12",
    }
)
SANDBOX_INTERPRETER = "/lib64/ld-linux-x86-64.so.2"
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
VERSION_RE = re.compile(r"[0-9]+(?:\.[0-9]+){2,3}\Z")
SONAME_RE = re.compile(r"[A-Za-z0-9_+.-]{1,255}\Z")
ORIGIN_NEEDED_RE = re.compile(
    r"\$ORIGIN(?:/[A-Za-z0-9_+.-]{1,255}){1,16}\Z"
)
MODE_RE = re.compile(r"[0-7]{4}\Z")
SAFE_IMAGE_COMPONENT_RE = re.compile(r"[A-Za-z0-9._+@-]{1,255}\Z")

PT_LOAD = 1
PT_DYNAMIC = 2
PT_INTERP = 3
DT_NULL = 0
DT_NEEDED = 1
DT_STRTAB = 5
DT_STRSZ = 10
DT_SONAME = 14
DT_RPATH = 15
DT_RUNPATH = 29
EM_X86_64 = 62

POLICY = {
    "network_access": False,
    "loader_cache_authority": False,
    "ldd_execution": False,
    "broad_library_directory_bind": False,
    "recursive_dt_needed_closure_required": True,
    "runtime_loaded_libraries_are_explicit_roots": True,
    "descriptor_stable_launch_replay_required": True,
    "nvidia_tileir_runtime_loading": "prohibited",
    "nvidia_pkcs11_runtime_loading": "prohibited",
    "unsupported_loader_search_paths": "rejected",
    "publication_authority": "none",
    "archive_authority": "none",
    "deletion_authority": "none",
}


class HostABIManifestError(RuntimeError):
    """A host ABI inventory is unsafe, incomplete, or no longer reproducible."""


class HostABIManifestPublishedError(HostABIManifestError):
    """Durability finalization failed after the requested output became visible."""


def canonical_bytes(value: Any) -> bytes:
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


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _exact(value: Any, label: str, fields: set[str] | frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != set(fields):
        observed = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise HostABIManifestError(f"{label} has unexpected fields: {observed}")
    return value


def _integer(value: Any, label: str, minimum: int, maximum: int = 2**63 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise HostABIManifestError(
            f"{label} must be an integer within [{minimum}, {maximum}]"
        )
    return value


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise HostABIManifestError(f"{label} must be a lowercase SHA-256")
    return value


def _bounded_text(value: Any, label: str, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > maximum
        or any(character in value for character in ("\x00", "\r", "\n"))
    ):
        raise HostABIManifestError(f"{label} is absent or outside its bound")
    return value


def _absolute(value: Any, label: str) -> Path:
    if not isinstance(value, (str, os.PathLike)):
        raise HostABIManifestError(f"{label} must be an absolute path")
    raw = os.fspath(value)
    if (
        not raw
        or len(raw.encode("utf-8")) > 4096
        or any(character in raw for character in ("\x00", "\r", "\n"))
        or not os.path.isabs(raw)
    ):
        raise HostABIManifestError(f"{label} must be an absolute path")
    normalized = os.path.normpath(raw)
    if normalized != raw or raw == "/" or "\\" in raw or "//" in raw:
        raise HostABIManifestError(f"{label} must be lexically normalized")
    pure = PurePosixPath(raw)
    if pure == COLD_ROOT or COLD_ROOT in pure.parents:
        raise HostABIManifestError(f"{label} may not reference the archive tier")
    return Path(normalized)


def _beneath(path: Path, roots: Sequence[Path], label: str) -> None:
    if not any(path == root or root in path.parents for root in roots):
        raise HostABIManifestError(f"{label} is outside the declared library roots")


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


class _RetainedLibraryRoot:
    """Descriptor-anchored absolute directory chain used for host DSO replay."""

    def __init__(
        self,
        path: Path,
        descriptors: list[int],
        observations: list[os.stat_result],
    ) -> None:
        self.path = path
        self.descriptors = descriptors
        self.observations = observations
        self.components = tuple(path.parts[1:])

    @property
    def descriptor(self) -> int:
        if not self.descriptors:
            raise HostABIManifestError("retained library root is closed")
        return self.descriptors[-1]

    def verify(self) -> None:
        if len(self.descriptors) != len(self.observations):
            raise HostABIManifestError("retained library-root chain is incomplete")
        if len(self.descriptors) != len(self.components) + 1:
            raise HostABIManifestError("retained library-root chain has wrong depth")
        for descriptor, expected in zip(
            self.descriptors, self.observations, strict=True
        ):
            if _stat_identity(os.fstat(descriptor)) != _stat_identity(expected):
                raise HostABIManifestError("retained library-root directory changed")
        for ordinal, component in enumerate(self.components, 1):
            linked = os.stat(
                component,
                dir_fd=self.descriptors[ordinal - 1],
                follow_symlinks=False,
            )
            if _stat_identity(linked) != _stat_identity(
                self.observations[ordinal]
            ):
                raise HostABIManifestError("retained library-root ancestry changed")

    def close(self) -> None:
        for descriptor in reversed(self.descriptors):
            with contextlib.suppress(OSError):
                os.close(descriptor)
        self.descriptors.clear()
        self.observations.clear()


def _retain_library_root(
    path: Path,
    *,
    expected_uid: int | None,
    expected_gid: int | None,
    require_trusted_chain: bool,
) -> _RetainedLibraryRoot:
    descriptors: list[int] = []
    observations: list[os.stat_result] = []
    try:
        descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        descriptors.append(descriptor)
        root_info = os.fstat(descriptor)
        observations.append(root_info)
        if not stat.S_ISDIR(root_info.st_mode):
            raise HostABIManifestError("filesystem root is not a directory")
        if require_trusted_chain and (
            expected_uid is None
            or expected_gid is None
            or
            (root_info.st_uid, root_info.st_gid) != (0, 0)
            or stat.S_IMODE(root_info.st_mode) & 0o022
        ):
            raise HostABIManifestError(
                "filesystem root is not trusted for host ABI replay"
            )
        for component in path.parts[1:]:
            try:
                before = os.stat(
                    component, dir_fd=descriptor, follow_symlinks=False
                )
                child = os.open(
                    component,
                    os.O_RDONLY
                    | os.O_DIRECTORY
                    | os.O_CLOEXEC
                    | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=descriptor,
                )
            except OSError as error:
                raise HostABIManifestError(
                    f"cannot retain trusted library-root component {component!r}: {error}"
                ) from error
            after = os.fstat(child)
            if (
                stat.S_ISLNK(before.st_mode)
                or not stat.S_ISDIR(before.st_mode)
                or _stat_identity(before) != _stat_identity(after)
            ):
                os.close(child)
                raise HostABIManifestError(
                    f"trusted library-root component changed or is unsafe: {component}"
                )
            if require_trusted_chain and (
                expected_uid is None
                or expected_gid is None
                or
                (after.st_uid, after.st_gid)
                not in {(0, 0), (expected_uid, expected_gid)}
                or stat.S_IMODE(after.st_mode) & 0o022
            ):
                os.close(child)
                raise HostABIManifestError(
                    f"trusted library-root component is mutable or unsafe: {component}"
                )
            descriptors.append(child)
            observations.append(after)
            descriptor = child
        final = observations[-1]
        if expected_uid is not None and expected_gid is not None and (
            final.st_uid,
            final.st_gid,
        ) != (expected_uid, expected_gid):
            raise HostABIManifestError(
                "trusted library root has an unexpected final owner"
            )
        retained = _RetainedLibraryRoot(path, descriptors, observations)
        retained.verify()
        return retained
    except Exception:
        for descriptor in reversed(descriptors):
            with contextlib.suppress(OSError):
                os.close(descriptor)
        raise


def _retain_library_roots(
    roots: Sequence[Path],
    *,
    expected_uid: int | None,
    expected_gid: int | None,
    require_trusted_chain: bool,
) -> list[_RetainedLibraryRoot]:
    retained: list[_RetainedLibraryRoot] = []
    try:
        for root in roots:
            retained.append(
                _retain_library_root(
                    root,
                    expected_uid=expected_uid,
                    expected_gid=expected_gid,
                    require_trusted_chain=require_trusted_chain,
                )
            )
        return retained
    except Exception:
        for root in retained:
            root.close()
        raise


def verify_trusted_directory_chain(
    path_value: str | os.PathLike[str],
    *,
    expected_uid: int,
    expected_gid: int,
) -> None:
    """Open every directory component without following links and replay metadata."""
    path = _absolute(path_value, "trusted library root")
    uid = _integer(expected_uid, "trusted directory UID", 0, 2**31 - 1)
    gid = _integer(expected_gid, "trusted directory GID", 0, 2**31 - 1)
    retained = _retain_library_root(
        path,
        expected_uid=uid,
        expected_gid=gid,
        require_trusted_chain=True,
    )
    try:
        retained.verify()
    finally:
        retained.close()


def _stable_regular(
    path: Path, label: str, *, maximum: int = MAX_LIBRARY_BYTES
) -> tuple[bytes, os.stat_result]:
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        lexical = path.lstat()
        descriptor = os.open(path, flags)
    except OSError as error:
        raise HostABIManifestError(f"cannot open {label}: {error}") from error
    try:
        opened = os.fstat(descriptor)
        if (
            stat.S_ISLNK(lexical.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or _stat_identity(lexical) != _stat_identity(opened)
            or opened.st_size < 1
            or opened.st_size > maximum
        ):
            raise HostABIManifestError(f"{label} metadata is unsafe")
        chunks: list[bytes] = []
        remaining = opened.st_size
        while remaining:
            chunk = os.read(descriptor, min(4 * 1024 * 1024, remaining))
            if not chunk:
                raise HostABIManifestError(f"{label} ended before its recorded size")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise HostABIManifestError(f"{label} grew while read")
        after = os.fstat(descriptor)
        if _stat_identity(opened) != _stat_identity(after):
            raise HostABIManifestError(f"{label} changed while read")
        return b"".join(chunks), after
    finally:
        os.close(descriptor)


def _inspect_projection_file(
    path: Path,
    *,
    expected_sha256: str,
    expected_byte_count: int,
    label: str,
) -> dict[str, Any] | None:
    """Stream-hash one projected source and sparsely parse it when it is ELF.

    cuBLAS Lt is substantially larger than the host-library cap. Mapping it
    read-only avoids a second 749 MB Python allocation while retaining exact
    before/after metadata and digest checks.
    """

    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        lexical = path.lstat()
        descriptor = os.open(path, flags)
    except OSError as error:
        raise HostABIManifestError(f"cannot open {label}: {error}") from error
    mapping: mmap.mmap | None = None
    try:
        opened = os.fstat(descriptor)
        if (
            stat.S_ISLNK(lexical.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or _stat_identity(lexical) != _stat_identity(opened)
            or opened.st_size != expected_byte_count
        ):
            raise HostABIManifestError(f"{label} metadata is unsafe")
        if expected_byte_count == 0:
            if expected_sha256 != sha256_bytes(b""):
                raise HostABIManifestError(f"{label} differs from the execution image receipt")
            return None
        mapping = mmap.mmap(descriptor, 0, access=mmap.ACCESS_READ)
        digest = hashlib.sha256()
        view = memoryview(mapping)
        try:
            for offset in range(0, expected_byte_count, 8 * 1024 * 1024):
                digest.update(view[offset : offset + 8 * 1024 * 1024])
        finally:
            view.release()
        if digest.hexdigest() != expected_sha256:
            raise HostABIManifestError(f"{label} differs from the execution image receipt")
        metadata = (
            parse_elf64_x86_64(
                mapping,
                label,
                allow_origin_needed=True,
            )
            if mapping[:4] == b"\x7fELF"
            else None
        )
        after_fd = os.fstat(descriptor)
        after_path = path.lstat()
        if (
            _stat_identity(opened) != _stat_identity(after_fd)
            or _stat_identity(opened) != _stat_identity(after_path)
        ):
            raise HostABIManifestError(f"{label} changed while inspected")
        return metadata
    finally:
        if mapping is not None:
            mapping.close()
        os.close(descriptor)


def _c_string(table: bytes, offset: int, label: str) -> str:
    if not 0 <= offset < len(table):
        raise HostABIManifestError(f"{label} string offset is outside DT_STRTAB")
    end = table.find(b"\x00", offset)
    if end < 0 or end - offset > 4096:
        raise HostABIManifestError(f"{label} string is unterminated or oversized")
    try:
        value = table[offset:end].decode("ascii")
    except UnicodeDecodeError as error:
        raise HostABIManifestError(f"{label} is not ASCII") from error
    return value


def parse_elf64_x86_64(
    body: bytes,
    label: str = "ELF object",
    *,
    allow_origin_needed: bool = False,
) -> dict[str, Any]:
    """Parse the small ELF subset needed for a non-executing DT_NEEDED audit."""

    if len(body) < 64 or body[:4] != b"\x7fELF":
        raise HostABIManifestError(f"{label} is not ELF")
    ident = body[:16]
    if ident[4] != 2 or ident[5] != 1 or ident[6] != 1:
        raise HostABIManifestError(f"{label} must be little-endian ELF64 version 1")
    try:
        (
            _ident,
            elf_type,
            machine,
            version,
            _entry,
            program_offset,
            _section_offset,
            _flags,
            header_size,
            program_entry_size,
            program_count,
            _section_entry_size,
            _section_count,
            _section_names,
        ) = struct.unpack_from("<16sHHIQQQIHHHHHH", body, 0)
    except struct.error as error:
        raise HostABIManifestError(f"{label} has a truncated ELF header") from error
    if (
        machine != EM_X86_64
        or version != 1
        or elf_type not in {2, 3}
        or header_size != 64
        or program_entry_size < 56
        or not 1 <= program_count <= 1024
        or program_offset + program_entry_size * program_count > len(body)
    ):
        raise HostABIManifestError(f"{label} has an unsupported ELF header")

    loads: list[tuple[int, int, int]] = []
    dynamic: tuple[int, int] | None = None
    interpreter: str | None = None
    for ordinal in range(program_count):
        offset = program_offset + ordinal * program_entry_size
        try:
            (
                segment_type,
                _segment_flags,
                file_offset,
                virtual_address,
                _physical_address,
                file_size,
                memory_size,
                _alignment,
            ) = struct.unpack_from("<IIQQQQQQ", body, offset)
        except struct.error as error:
            raise HostABIManifestError(f"{label} has a truncated program header") from error
        if file_offset + file_size > len(body):
            raise HostABIManifestError(f"{label} has a program segment outside the file")
        if segment_type == PT_LOAD and file_size > memory_size:
            raise HostABIManifestError(
                f"{label} has a PT_LOAD file size above its memory size"
            )
        if segment_type == PT_LOAD:
            loads.append((virtual_address, file_size, file_offset))
        elif segment_type == PT_DYNAMIC:
            if dynamic is not None:
                raise HostABIManifestError(f"{label} has multiple PT_DYNAMIC segments")
            dynamic = (file_offset, file_size)
        elif segment_type == PT_INTERP:
            if interpreter is not None:
                raise HostABIManifestError(
                    f"{label} has multiple PT_INTERP segments"
                )
            raw = body[file_offset : file_offset + file_size]
            if not raw.endswith(b"\x00") or raw.count(b"\x00") != 1:
                raise HostABIManifestError(f"{label} has an invalid PT_INTERP")
            try:
                decoded = raw[:-1].decode("ascii")
            except UnicodeDecodeError as error:
                raise HostABIManifestError(f"{label} PT_INTERP is not ASCII") from error
            interpreter = str(_absolute(decoded, f"{label} PT_INTERP"))

    dynamic_values: dict[int, list[int]] = {}
    if dynamic is not None:
        offset, size = dynamic
        if size % 16 or size // 16 > 65536:
            raise HostABIManifestError(f"{label} has an invalid PT_DYNAMIC size")
        terminated = False
        for cursor in range(offset, offset + size, 16):
            tag, value = struct.unpack_from("<qQ", body, cursor)
            if tag == DT_NULL:
                terminated = True
                break
            if tag in {DT_NEEDED, DT_STRTAB, DT_STRSZ, DT_SONAME, DT_RPATH, DT_RUNPATH}:
                dynamic_values.setdefault(tag, []).append(value)
        if not terminated:
            raise HostABIManifestError(f"{label} dynamic table lacks DT_NULL")

    needed: list[str] = []
    soname: str | None = None
    rpath: list[str] = []
    runpath: list[str] = []
    if dynamic_values.get(DT_NEEDED) or dynamic_values.get(DT_SONAME) or dynamic_values.get(DT_RPATH) or dynamic_values.get(DT_RUNPATH):
        if len(dynamic_values.get(DT_STRTAB, [])) != 1 or len(dynamic_values.get(DT_STRSZ, [])) != 1:
            raise HostABIManifestError(f"{label} lacks one bounded dynamic string table")
        address = dynamic_values[DT_STRTAB][0]
        size = dynamic_values[DT_STRSZ][0]
        if not 1 <= size <= len(body):
            raise HostABIManifestError(f"{label} DT_STRSZ is outside its bound")
        def mapped_offset(virtual: int) -> int | None:
            for virtual_address, file_size, file_offset in loads:
                if virtual_address <= virtual < virtual_address + file_size:
                    return file_offset + virtual - virtual_address
            return None

        table_offset = mapped_offset(address)
        table_end = mapped_offset(address + size - 1)
        if (
            table_offset is None
            or table_end is None
            or table_end != table_offset + size - 1
            or table_offset + size > len(body)
        ):
            raise HostABIManifestError(f"{label} DT_STRTAB is not file-backed")
        table = body[table_offset : table_offset + size]
        needed = [_c_string(table, value, f"{label} DT_NEEDED") for value in dynamic_values.get(DT_NEEDED, [])]
        if len(needed) > MAX_DEPENDENCIES_PER_LIBRARY or len(set(needed)) != len(needed):
            raise HostABIManifestError(f"{label} DT_NEEDED list is duplicated or oversized")
        if any(
            not SONAME_RE.fullmatch(value)
            and not (allow_origin_needed and ORIGIN_NEEDED_RE.fullmatch(value))
            for value in needed
        ):
            raise HostABIManifestError(f"{label} contains a non-basename DT_NEEDED")
        sonames = [_c_string(table, value, f"{label} DT_SONAME") for value in dynamic_values.get(DT_SONAME, [])]
        if len(sonames) > 1 or (sonames and not SONAME_RE.fullmatch(sonames[0])):
            raise HostABIManifestError(f"{label} has an invalid DT_SONAME")
        soname = sonames[0] if sonames else None
        rpath_values = dynamic_values.get(DT_RPATH, [])
        runpath_values = dynamic_values.get(DT_RUNPATH, [])
        if len(rpath_values) > 1 or len(runpath_values) > 1:
            raise HostABIManifestError(f"{label} has multiple loader search paths")
        if rpath_values and runpath_values:
            raise HostABIManifestError(
                f"{label} contains both DT_RPATH and DT_RUNPATH"
            )
        for values, destination, tag_label in (
            (rpath_values, rpath, "DT_RPATH"),
            (runpath_values, runpath, "DT_RUNPATH"),
        ):
            if not values:
                continue
            raw_path = _c_string(table, values[0], f"{label} {tag_label}")
            # Preserve an explicitly empty component as ``""`` so schema
            # normalization can reject its current-working-directory semantics.
            destination.extend(raw_path.split(":"))
            if len(destination) > 64 or any(
                len(value.encode("utf-8")) > 4096 or "\x00" in value
                for value in destination
            ):
                raise HostABIManifestError(f"{label} loader search path is oversized")

    return {
        "elf_class": 64,
        "endianness": "little",
        "machine": EM_X86_64,
        "elf_type": elf_type,
        "soname": soname,
        "needed": sorted(needed),
        "interpreter": interpreter,
        "rpath": rpath,
        "runpath": runpath,
    }


def _normalized_roots(values: Iterable[str | os.PathLike[str]]) -> list[Path]:
    roots = [_absolute(value, "library root") for value in values]
    if not roots or len(roots) > 16 or len(set(roots)) != len(roots):
        raise HostABIManifestError("library roots are absent, duplicated, or oversized")
    if roots != sorted(roots, key=str):
        raise HostABIManifestError("library roots must be sorted")
    return roots


def _root_for_direct_child(
    path: Path, roots: Sequence[_RetainedLibraryRoot]
) -> _RetainedLibraryRoot:
    candidates = [root for root in roots if path.parent == root.path]
    if len(candidates) != 1:
        raise HostABIManifestError(
            f"library aliases and sources must be direct children of one declared root: {path}"
        )
    return candidates[0]


def _resolve_alias(
    path: Path, roots: Sequence[_RetainedLibraryRoot]
) -> tuple[Path, list[dict[str, Any]]]:
    root_paths = [root.path for root in roots]
    _beneath(path, root_paths, "library alias")
    aliases: list[dict[str, Any]] = []
    current = path
    visited: set[Path] = set()
    for _ in range(MAX_ALIAS_DEPTH + 1):
        if current in visited:
            raise HostABIManifestError(f"library alias cycle at {current}")
        visited.add(current)
        _beneath(current, root_paths, "resolved library path")
        root = _root_for_direct_child(current, roots)
        root.verify()
        try:
            before = os.stat(
                current.name,
                dir_fd=root.descriptor,
                follow_symlinks=False,
            )
        except OSError as error:
            raise HostABIManifestError(f"cannot inspect library alias {current}: {error}") from error
        if not stat.S_ISLNK(before.st_mode):
            if not stat.S_ISREG(before.st_mode):
                raise HostABIManifestError(f"resolved library is not regular: {current}")
            root.verify()
            return current, aliases
        try:
            target = os.readlink(current.name, dir_fd=root.descriptor)
            after = os.stat(
                current.name,
                dir_fd=root.descriptor,
                follow_symlinks=False,
            )
        except OSError as error:
            raise HostABIManifestError(
                f"cannot replay library alias {current}: {error}"
            ) from error
        if (
            not stat.S_ISLNK(after.st_mode)
            or _stat_identity(before) != _stat_identity(after)
        ):
            raise HostABIManifestError(f"library alias changed while read: {current}")
        if not target or "\x00" in target or len(os.fsencode(target)) > 4096:
            raise HostABIManifestError(f"library alias target is invalid: {current}")
        aliases.append(
            {
                "path": str(current),
                "target": target,
                "uid": after.st_uid,
                "gid": after.st_gid,
                "mode": f"{stat.S_IMODE(after.st_mode):04o}",
            }
        )
        root.verify()
        next_path = Path(target) if os.path.isabs(target) else current.parent / target
        current = Path(os.path.normpath(str(next_path)))
    raise HostABIManifestError(f"library alias chain exceeds {MAX_ALIAS_DEPTH}: {path}")


def _stable_regular_at(
    root: _RetainedLibraryRoot,
    path: Path,
    label: str,
) -> tuple[bytes, os.stat_result]:
    root.verify()
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        lexical = os.stat(
            path.name, dir_fd=root.descriptor, follow_symlinks=False
        )
        descriptor = os.open(path.name, flags, dir_fd=root.descriptor)
    except OSError as error:
        raise HostABIManifestError(f"cannot open {label}: {error}") from error
    try:
        opened = os.fstat(descriptor)
        if (
            stat.S_ISLNK(lexical.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or _stat_identity(lexical) != _stat_identity(opened)
            or opened.st_size < 1
            or opened.st_size > MAX_LIBRARY_BYTES
        ):
            raise HostABIManifestError(f"{label} metadata is unsafe")
        chunks: list[bytes] = []
        remaining = opened.st_size
        while remaining:
            chunk = os.read(descriptor, min(4 * 1024 * 1024, remaining))
            if not chunk:
                raise HostABIManifestError(
                    f"{label} ended before its recorded size"
                )
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise HostABIManifestError(f"{label} grew while read")
        after_fd = os.fstat(descriptor)
        after_link = os.stat(
            path.name, dir_fd=root.descriptor, follow_symlinks=False
        )
        if (
            _stat_identity(opened) != _stat_identity(after_fd)
            or _stat_identity(opened) != _stat_identity(after_link)
        ):
            raise HostABIManifestError(f"{label} changed while read")
        root.verify()
        return b"".join(chunks), after_fd
    finally:
        os.close(descriptor)


def _library_row(
    alias_path: Path, roots: Sequence[_RetainedLibraryRoot]
) -> dict[str, Any]:
    resolved, aliases = _resolve_alias(alias_path, roots)
    root = _root_for_direct_child(resolved, roots)
    body, info = _stable_regular_at(root, resolved, f"host library {alias_path}")
    metadata = _normalize_elf(
        parse_elf64_x86_64(body, f"host library {alias_path}"),
        f"host library {alias_path} ELF",
    )
    return {
        "sandbox_path": str(alias_path),
        "source_path": str(resolved),
        "sha256": sha256_bytes(body),
        "byte_count": len(body),
        "uid": info.st_uid,
        "gid": info.st_gid,
        "mode": f"{stat.S_IMODE(info.st_mode):04o}",
        "aliases": aliases,
        "elf": metadata,
    }


def _read_kernel_report(path: Path) -> bytes:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0))
    except OSError as error:
        raise HostABIManifestError(f"cannot read NVIDIA kernel-module report: {error}") from error
    try:
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, 8192)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_KERNEL_REPORT_BYTES:
                raise HostABIManifestError("NVIDIA kernel-module report exceeds its bound")
            chunks.append(chunk)
        body = b"".join(chunks)
    finally:
        os.close(descriptor)
    if not body:
        raise HostABIManifestError("NVIDIA kernel-module report is empty")
    return body


def observe_platform(
    nvidia_driver_version: str,
    *,
    module_report_path: Path = Path("/proc/driver/nvidia/version"),
) -> dict[str, Any]:
    if not isinstance(nvidia_driver_version, str) or not VERSION_RE.fullmatch(nvidia_driver_version):
        raise HostABIManifestError("NVIDIA driver version is invalid")
    report = _read_kernel_report(module_report_path)
    try:
        text = report.decode("utf-8")
    except UnicodeDecodeError as error:
        raise HostABIManifestError("NVIDIA kernel-module report is not UTF-8") from error
    match = re.search(r"NVRM version:.*?\b([0-9]+(?:\.[0-9]+){2,3})\b", text, re.DOTALL)
    if match is None:
        raise HostABIManifestError("NVIDIA kernel-module version is absent")
    module_version = match.group(1)
    if module_version != nvidia_driver_version:
        raise HostABIManifestError("NVIDIA user/kernel driver versions differ")
    observed = os.uname()
    return {
        "sysname": _bounded_text(observed.sysname, "kernel sysname", 64),
        "release": _bounded_text(observed.release, "kernel release", 256),
        "version": _bounded_text(observed.version, "kernel version", 1024),
        "machine": _bounded_text(observed.machine, "kernel machine", 64),
        "nvidia_driver_version": nvidia_driver_version,
        "nvidia_kernel_module_version": module_version,
        "nvidia_kernel_module_report_sha256": sha256_bytes(report),
        "nvidia_kernel_module_report_byte_count": len(report),
    }


def _platform(value: Any) -> dict[str, Any]:
    item = _exact(
        value,
        "host ABI platform",
        {
            "sysname",
            "release",
            "version",
            "machine",
            "nvidia_driver_version",
            "nvidia_kernel_module_version",
            "nvidia_kernel_module_report_sha256",
            "nvidia_kernel_module_report_byte_count",
        },
    )
    result = {
        "sysname": _bounded_text(item["sysname"], "kernel sysname", 64),
        "release": _bounded_text(item["release"], "kernel release", 256),
        "version": _bounded_text(item["version"], "kernel version", 1024),
        "machine": _bounded_text(item["machine"], "kernel machine", 64),
        "nvidia_driver_version": _bounded_text(item["nvidia_driver_version"], "NVIDIA driver version", 64),
        "nvidia_kernel_module_version": _bounded_text(item["nvidia_kernel_module_version"], "NVIDIA kernel module version", 64),
        "nvidia_kernel_module_report_sha256": _digest(item["nvidia_kernel_module_report_sha256"], "NVIDIA kernel report SHA-256"),
        "nvidia_kernel_module_report_byte_count": _integer(item["nvidia_kernel_module_report_byte_count"], "NVIDIA kernel report byte_count", 1, MAX_KERNEL_REPORT_BYTES),
    }
    if (
        not VERSION_RE.fullmatch(result["nvidia_driver_version"])
        or result["nvidia_kernel_module_version"] != result["nvidia_driver_version"]
        or result["sysname"] != "Linux"
        or result["machine"] != "x86_64"
    ):
        raise HostABIManifestError("host ABI platform is unsupported")
    return result


def production_runtime_loaded_sonames(driver_version: str) -> list[str]:
    """Return the audited non-DT_NEEDED roots used by the production lane.

    ``libcuda`` and NVML are loaded through Python/native APIs rather than a
    static dependency of the bundled interpreter.  NVIDIA's driver can in turn
    open its JIT components by name.  Keeping those roots explicit prevents a
    successful recursive DT_NEEDED walk from being mistaken for a complete
    dynamic-load contract.
    """

    version = _bounded_text(driver_version, "NVIDIA driver version", 64)
    if not VERSION_RE.fullmatch(version):
        raise HostABIManifestError("NVIDIA driver version is invalid")
    return sorted(
        {
            "ld-linux-x86-64.so.2",
            "libcuda.so.1",
            "libnvidia-gpucomp.so." + version,
            "libnvidia-ml.so.1",
            "libnvidia-nvvm.so.4",
            "libnvidia-nvvm70.so.4",
            "libnvidia-ptxjitcompiler.so.1",
        }
    )


def reviewed_excluded_driver_dlopen_sonames(driver_version: str) -> list[str]:
    """Feature-gated driver DSOs intentionally outside the RTX 3050 lane."""

    version = _bounded_text(driver_version, "NVIDIA driver version", 64)
    if not VERSION_RE.fullmatch(version):
        raise HostABIManifestError("NVIDIA driver version is invalid")
    return sorted(
        {
            "libnvidia-pkcs11-openssl3.so." + version,
            "libnvidia-pkcs11.so." + version,
            "libnvidia-tileiras.so." + version,
        }
    )


def _relative_image_path(value: Any, label: str) -> PurePosixPath:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > 4096
        or any(character in value for character in ("\x00", "\r", "\n"))
    ):
        raise HostABIManifestError(f"{label} is invalid")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or str(path) != value
        or not path.parts
        or any(
            part in {"", ".", ".."}
            or not SAFE_IMAGE_COMPONENT_RE.fullmatch(part)
            for part in path.parts
        )
    ):
        raise HostABIManifestError(f"{label} must be a normalized relative path")
    return path


def _loader_search_paths(value: Any, label: str) -> list[str]:
    if not isinstance(value, list) or len(value) > 64:
        raise HostABIManifestError(f"{label} is invalid")
    paths: list[str] = []
    for entry in value:
        if (
            not isinstance(entry, str)
            or len(entry.encode("utf-8")) > 4096
            or any(character in entry for character in ("\x00", "\r", "\n"))
        ):
            raise HostABIManifestError(f"{label} is invalid")
        if entry == "$ORIGIN":
            paths.append(entry)
            continue
        if not entry.startswith("$ORIGIN/"):
            raise HostABIManifestError(
                f"{label} contains an unsupported loader substitution or path"
            )
        suffix_text = entry[len("$ORIGIN/") :]
        suffix = PurePosixPath(suffix_text)
        if (
            not suffix_text
            or suffix.is_absolute()
            or str(suffix) != suffix_text
            or any(
                part in {"", "."}
                or (part != ".." and not SAFE_IMAGE_COMPONENT_RE.fullmatch(part))
                for part in suffix.parts
            )
        ):
            raise HostABIManifestError(
                f"{label} contains a noncanonical $ORIGIN path"
            )
        paths.append(entry)
    return paths


def _consumer_elf(value: Any, label: str) -> dict[str, Any]:
    item = _exact(
        value,
        label,
        {
            "elf_class",
            "endianness",
            "machine",
            "elf_type",
            "soname",
            "needed",
            "interpreter",
            "rpath",
            "runpath",
        },
    )
    if item["elf_class"] != 64 or item["endianness"] != "little" or item["machine"] != EM_X86_64 or item["elf_type"] not in {2, 3}:
        raise HostABIManifestError(f"{label} has an unsupported architecture")
    soname = item["soname"]
    if soname is not None and (not isinstance(soname, str) or not SONAME_RE.fullmatch(soname)):
        raise HostABIManifestError(f"{label}.soname is invalid")
    needed = item["needed"]
    if (
        not isinstance(needed, list)
        or needed != sorted(needed)
        or len(needed) > MAX_DEPENDENCIES_PER_LIBRARY
        or len(set(needed)) != len(needed)
        or any(
            not isinstance(name, str)
            or not (SONAME_RE.fullmatch(name) or ORIGIN_NEEDED_RE.fullmatch(name))
            for name in needed
        )
    ):
        raise HostABIManifestError(f"{label}.needed is invalid")
    interpreter = item["interpreter"]
    if interpreter is not None:
        interpreter = str(_absolute(interpreter, f"{label}.interpreter"))
        if interpreter != SANDBOX_INTERPRETER:
            raise HostABIManifestError(
                f"{label}.interpreter is outside the exact sandbox ABI"
            )
    loader_paths: dict[str, list[str]] = {}
    for field in ("rpath", "runpath"):
        loader_paths[field] = _loader_search_paths(
            item[field], f"{label}.{field}"
        )
    if loader_paths["rpath"] and loader_paths["runpath"]:
        raise HostABIManifestError(
            f"{label} cannot contain both DT_RPATH and DT_RUNPATH"
        )
    return {
        **item,
        "soname": soname,
        "needed": needed,
        "interpreter": interpreter,
        "rpath": loader_paths["rpath"],
        "runpath": loader_paths["runpath"],
    }


def _canonical_image_load_roots(
    consumers: Sequence[dict[str, Any]],
) -> list[str]:
    """Return the exact projected objects admitted as initial loader roots."""

    by_image = {row["image_relative_path"]: row for row in consumers}
    python_rows = [
        row["image_relative_path"]
        for row in consumers
        if row["elf"]["interpreter"] is not None
    ]
    if python_rows != ["runtime/bin/python3.12"]:
        raise HostABIManifestError(
            "consumer scan must contain the one exact standalone Python executable"
        )
    missing_dlopen = sorted(EXPLICIT_IMAGE_DLOPEN_ROOTS - set(by_image))
    if missing_dlopen:
        raise HostABIManifestError(
            f"consumer scan omits explicit image dlopen roots: {missing_dlopen}"
        )
    roots = set(python_rows) | set(EXPLICIT_IMAGE_DLOPEN_ROOTS)
    for row in consumers:
        path = PurePosixPath(row["image_relative_path"])
        parent = str(path.parent)
        if (
            PYTHON_SITE_PACKAGES in path.parents
            and path.name.endswith(".so")
            and parent not in PRIVATE_IMAGE_LIBRARY_DIRECTORIES
            and parent not in GLOBAL_IMAGE_LIBRARY_DIRECTORIES
        ):
            roots.add(str(path))
    return sorted(roots)


def _expanded_origin_directories(
    row: dict[str, Any], field: str
) -> frozenset[str]:
    parent = PurePosixPath(row["image_relative_path"]).parent
    directories: set[str] = set()
    for search in row["elf"][field]:
        if search == "$ORIGIN":
            expanded = str(parent)
        elif search.startswith("$ORIGIN/"):
            expanded = posixpath.normpath(
                str(parent / search[len("$ORIGIN/") :])
            )
        else:
            # Absolute paths, empty components, and other loader substitutions
            # never confer projected provider authority in this sealed lane.
            continue
        directories.add(
            str(
                _relative_image_path(
                    expanded,
                    f"consumer {row['image_relative_path']} {field} directory",
                )
            )
        )
    return frozenset(directories)


def _consumer_external_sonames(
    consumers: Sequence[dict[str, Any]],
    runtime_loaded_sonames: Sequence[str],
    load_roots: Sequence[str],
) -> list[str]:
    by_image = {row["image_relative_path"]: row for row in consumers}
    providers: dict[str, set[str]] = {}
    for row in consumers:
        # The execution image projection contains regular files only.  A
        # DT_SONAME is metadata, not a loader-visible filename or symlink, so
        # only the literal projected basename can provide a DT_NEEDED name.
        name = PurePosixPath(row["image_relative_path"]).name
        providers.setdefault(name, set()).add(row["image_relative_path"])

    if list(load_roots) != sorted(load_roots) or len(set(load_roots)) != len(
        load_roots
    ):
        raise HostABIManifestError("consumer load roots are noncanonical")
    if any(path not in by_image for path in load_roots):
        raise HostABIManifestError("consumer load root is absent from the image")

    external = set(runtime_loaded_sonames)
    pending: list[tuple[str, frozenset[str]]] = [
        (path, frozenset()) for path in load_roots
    ]
    visited: set[tuple[str, frozenset[str]]] = set()
    while pending:
        path, inherited_rpath = pending.pop(0)
        state = (path, inherited_rpath)
        if state in visited:
            continue
        visited.add(state)
        if len(visited) > MAX_RESOLUTION_STATES:
            raise HostABIManifestError(
                "consumer dependency contexts exceed their bound"
            )
        row = by_image[path]
        parent = PurePosixPath(path).parent
        own_rpath = _expanded_origin_directories(row, "rpath")
        own_runpath = _expanded_origin_directories(row, "runpath")
        propagated_rpath = frozenset(set(inherited_rpath) | set(own_rpath))
        search_directories = (
            set(GLOBAL_IMAGE_LIBRARY_DIRECTORIES)
            | set(inherited_rpath)
            | set(own_rpath)
            | set(own_runpath)
        )
        for needed in row["elf"]["needed"]:
            provider: str | None = None
            if needed.startswith("$ORIGIN/"):
                suffix = needed[len("$ORIGIN/") :]
                resolved_text = posixpath.normpath(str(parent / suffix))
                resolved = _relative_image_path(
                    resolved_text,
                    f"consumer {row['image_relative_path']} origin dependency",
                )
                if str(resolved) not in by_image:
                    raise HostABIManifestError(
                        f"execution image lacks $ORIGIN provider {resolved}"
                    )
                provider = str(resolved)
            else:
                reachable = sorted(
                    candidate
                    for candidate in providers.get(needed, set())
                    if str(PurePosixPath(candidate).parent)
                    in search_directories
                )
                if len(reachable) > 1:
                    raise HostABIManifestError(
                        f"consumer {path} has ambiguous reachable provider {needed!r}"
                    )
                if reachable:
                    provider = reachable[0]
            if provider is None:
                external.add(needed)
            else:
                next_state = (provider, propagated_rpath)
                if next_state not in visited and next_state not in pending:
                    pending.append(next_state)
        pending.sort(key=lambda value: (value[0], sorted(value[1])))
    return sorted(external)


def normalize_consumer_scan(value: Any) -> dict[str, Any]:
    item = _exact(
        value,
        "execution-image ELF consumer scan",
        {
            "execution_image_identity_sha256",
            "source_tree_identity_sha256",
            "source_tree_regular_file_count",
            "elf_file_count",
            "consumers",
            "load_roots",
            "runtime_loaded_sonames",
            "external_sonames",
            "identity_sha256",
        },
    )
    image_identity = _digest(
        item["execution_image_identity_sha256"], "consumer scan execution-image identity"
    )
    tree_identity = _digest(
        item["source_tree_identity_sha256"], "consumer scan source-tree identity"
    )
    regular_count = _integer(
        item["source_tree_regular_file_count"],
        "consumer scan regular_file_count",
        1,
        100_000,
    )
    values = item["consumers"]
    if not isinstance(values, list) or not 1 <= len(values) <= MAX_CONSUMER_ELVES:
        raise HostABIManifestError("consumer ELF inventory is outside its bound")
    consumers: list[dict[str, Any]] = []
    for ordinal, value_row in enumerate(values, 1):
        row = _exact(
            value_row,
            f"consumer ELF {ordinal}",
            {"image_relative_path", "sha256", "byte_count", "elf"},
        )
        consumers.append(
            {
                "image_relative_path": str(
                    _relative_image_path(
                        row["image_relative_path"], f"consumer ELF {ordinal} image path"
                    )
                ),
                "sha256": _digest(row["sha256"], f"consumer ELF {ordinal} SHA-256"),
                "byte_count": _integer(
                    row["byte_count"],
                    f"consumer ELF {ordinal} byte_count",
                    64,
                    MAX_PROJECTION_FILE_BYTES,
                ),
                "elf": _consumer_elf(row["elf"], f"consumer ELF {ordinal} metadata"),
            }
        )
    if consumers != sorted(consumers, key=lambda row: row["image_relative_path"]) or len({row["image_relative_path"] for row in consumers}) != len(consumers):
        raise HostABIManifestError("consumer ELF inventory is duplicated or noncanonical")
    if item["elf_file_count"] != len(consumers) or len(consumers) > regular_count:
        raise HostABIManifestError("consumer ELF counts are inconsistent")
    load_roots = item["load_roots"]
    if (
        not isinstance(load_roots, list)
        or load_roots != sorted(load_roots)
        or len(set(load_roots)) != len(load_roots)
        or any(
            not isinstance(path, str)
            or str(_relative_image_path(path, "consumer load root")) != path
            for path in load_roots
        )
        or load_roots != _canonical_image_load_roots(consumers)
    ):
        raise HostABIManifestError("consumer load roots differ from exact image policy")
    runtime_loaded = item["runtime_loaded_sonames"]
    if (
        not isinstance(runtime_loaded, list)
        or runtime_loaded != sorted(runtime_loaded)
        or len(set(runtime_loaded)) != len(runtime_loaded)
        or not runtime_loaded
        or any(not isinstance(name, str) or not SONAME_RE.fullmatch(name) for name in runtime_loaded)
    ):
        raise HostABIManifestError("runtime-loaded SONAME roots are invalid")
    external = _consumer_external_sonames(consumers, runtime_loaded, load_roots)
    if item["external_sonames"] != external:
        raise HostABIManifestError("consumer scan external SONAME set is inconsistent")
    core = {
        "execution_image_identity_sha256": image_identity,
        "source_tree_identity_sha256": tree_identity,
        "source_tree_regular_file_count": regular_count,
        "elf_file_count": len(consumers),
        "consumers": consumers,
        "load_roots": load_roots,
        "runtime_loaded_sonames": runtime_loaded,
        "external_sonames": external,
    }
    identity = sha256_bytes(canonical_bytes(core))
    expected = {**core, "identity_sha256": identity}
    if item != expected:
        raise HostABIManifestError("consumer scan is noncanonical or has an invalid identity")
    return expected


def derive_consumer_scan(
    execution_image_receipt: dict[str, Any],
    *,
    runtime_loaded_sonames: Sequence[str],
) -> dict[str, Any]:
    """Classify every regular projection entry from an already validated receipt.

    This is deliberately a one-time, sequential source audit.  Every regular
    source is rehashed against the image receipt so a changed non-ELF cannot be
    misclassified as harmless.  The resulting scan is then bound to both the
    source-tree and final execution-image identities.
    """

    if not isinstance(execution_image_receipt, dict):
        raise HostABIManifestError("execution-image receipt must be an object")
    image_identity = _digest(
        execution_image_receipt.get("identity_sha256"), "execution-image identity"
    )
    tree = execution_image_receipt.get("source_tree")
    if not isinstance(tree, dict):
        raise HostABIManifestError("execution-image receipt lacks its source tree")
    tree_identity = _digest(tree.get("identity_sha256"), "source-tree identity")
    entries = tree.get("entries")
    regular_count = tree.get("regular_file_count")
    if (
        not isinstance(entries, list)
        or isinstance(regular_count, bool)
        or not isinstance(regular_count, int)
        or regular_count < 1
    ):
        raise HostABIManifestError("execution-image source tree is invalid")
    regular_entries = [row for row in entries if isinstance(row, dict) and row.get("kind") == "regular_file"]
    if len(regular_entries) != regular_count:
        raise HostABIManifestError("execution-image regular-file count is inconsistent")
    consumers: list[dict[str, Any]] = []
    for ordinal, row in enumerate(regular_entries, 1):
        required = {"source_path", "image_relative_path", "sha256", "byte_count"}
        if not required <= set(row):
            raise HostABIManifestError(f"source-tree regular entry {ordinal} is incomplete")
        source = _absolute(row["source_path"], f"source-tree regular entry {ordinal} path")
        image_path = _relative_image_path(
            row["image_relative_path"], f"source-tree regular entry {ordinal} image path"
        )
        expected_sha = _digest(row["sha256"], f"source-tree regular entry {ordinal} SHA-256")
        expected_count = _integer(
            row["byte_count"],
            f"source-tree regular entry {ordinal} byte_count",
            0,
            MAX_PROJECTION_FILE_BYTES,
        )
        elf = _inspect_projection_file(
            source,
            expected_sha256=expected_sha,
            expected_byte_count=expected_count,
            label=f"source-tree regular entry {ordinal}",
        )
        if elf is None:
            continue
        consumers.append(
            {
                "image_relative_path": str(image_path),
                "sha256": expected_sha,
                "byte_count": expected_count,
                "elf": elf,
            }
        )
    consumers.sort(key=lambda row: row["image_relative_path"])
    runtime_roots = sorted(runtime_loaded_sonames)
    load_roots = _canonical_image_load_roots(consumers)
    external = _consumer_external_sonames(
        consumers, runtime_roots, load_roots
    )
    core = {
        "execution_image_identity_sha256": image_identity,
        "source_tree_identity_sha256": tree_identity,
        "source_tree_regular_file_count": regular_count,
        "elf_file_count": len(consumers),
        "consumers": consumers,
        "load_roots": load_roots,
        "runtime_loaded_sonames": runtime_roots,
        "external_sonames": external,
    }
    result = {**core, "identity_sha256": sha256_bytes(canonical_bytes(core))}
    return normalize_consumer_scan(result)


def root_library_paths_for_consumer_scan(
    consumer_scan: dict[str, Any], library_roots: Sequence[str | os.PathLike[str]]
) -> list[str]:
    scan = normalize_consumer_scan(consumer_scan)
    roots = _normalized_roots(library_roots)
    production_roots = [str(root) for root in roots] == list(
        PRODUCTION_LIBRARY_ROOTS
    )
    retained = _retain_library_roots(
        roots,
        expected_uid=0 if production_roots else None,
        expected_gid=0 if production_roots else None,
        require_trusted_chain=production_roots,
    )
    try:
        paths: list[str] = []
        for soname in scan["external_sonames"]:
            candidates: list[Path] = []
            for root in retained:
                candidate = root.path / soname
                try:
                    os.stat(
                        soname,
                        dir_fd=root.descriptor,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    continue
                except OSError as error:
                    raise HostABIManifestError(
                        f"cannot inspect external SONAME {soname!r}: {error}"
                    ) from error
                _resolve_alias(candidate, retained)
                candidates.append(candidate)
            if len(candidates) != 1:
                raise HostABIManifestError(
                    f"external SONAME {soname!r} resolves to {len(candidates)} declared providers"
                )
            paths.append(str(candidates[0]))
        for root in retained:
            root.verify()
        return sorted(paths)
    finally:
        for root in retained:
            root.close()


def build_manifest(
    root_library_paths: Sequence[str | os.PathLike[str]],
    *,
    consumer_scan: dict[str, Any],
    library_roots: Sequence[str | os.PathLike[str]] = ("/usr/lib64",),
    nvidia_driver_version: str | None = None,
    platform: dict[str, Any] | None = None,
    required_owner_uid: int = 0,
    required_owner_gid: int = 0,
) -> dict[str, Any]:
    """Resolve explicit runtime roots and their complete host DT_NEEDED graph."""

    roots = _normalized_roots(library_roots)
    normalized_scan = normalize_consumer_scan(consumer_scan)
    requested = [_absolute(value, "root library") for value in root_library_paths]
    if not requested or len(requested) > MAX_LIBRARIES or len(set(requested)) != len(requested):
        raise HostABIManifestError("root library set is absent, duplicated, or oversized")
    if requested != sorted(requested, key=str):
        raise HostABIManifestError("root libraries must be sorted")
    for path in requested:
        _beneath(path, roots, "root library")
    if {path.name for path in requested} != set(normalized_scan["external_sonames"]):
        raise HostABIManifestError(
            "root libraries do not exactly match the consumer scan external SONAMEs"
        )
    owner_uid = _integer(required_owner_uid, "required owner UID", 0, 2**31 - 1)
    owner_gid = _integer(required_owner_gid, "required owner GID", 0, 2**31 - 1)
    if owner_uid == 0 and owner_gid == 0:
        if [str(root) for root in roots] != list(PRODUCTION_LIBRARY_ROOTS):
            raise HostABIManifestError(
                "production host ABI library root must be exactly /usr/lib64"
            )
    observed_platform = _platform(
        platform
        if platform is not None
        else observe_platform(
            _bounded_text(nvidia_driver_version, "NVIDIA driver version", 64)
        )
    )

    retained_roots = _retain_library_roots(
        roots,
        expected_uid=owner_uid,
        expected_gid=owner_gid,
        require_trusted_chain=(owner_uid, owner_gid) == (0, 0),
    )
    try:
        pending = list(requested)
        by_path: dict[str, dict[str, Any]] = {}
        while pending:
            alias = pending.pop(0)
            if str(alias) in by_path:
                continue
            if len(by_path) >= MAX_LIBRARIES:
                raise HostABIManifestError(
                    "host ABI library closure exceeds its bound"
                )
            row = _library_row(alias, retained_roots)
            if row["uid"] != owner_uid or row["gid"] != owner_gid:
                raise HostABIManifestError(
                    f"host library has the wrong owner: {alias}"
                )
            mode = int(row["mode"], 8)
            if mode & 0o7022 or not mode & 0o444:
                raise HostABIManifestError(
                    f"host library mode is unsafe: {alias}"
                )
            if any(
                value["uid"] != owner_uid or value["gid"] != owner_gid
                for value in row["aliases"]
            ):
                raise HostABIManifestError(
                    f"host library alias has the wrong owner: {alias}"
                )
            by_path[str(alias)] = row
            for needed in row["elf"]["needed"]:
                candidates: list[Path] = []
                for retained_root in retained_roots:
                    candidate = retained_root.path / needed
                    try:
                        os.stat(
                            needed,
                            dir_fd=retained_root.descriptor,
                            follow_symlinks=False,
                        )
                    except FileNotFoundError:
                        continue
                    except OSError as error:
                        raise HostABIManifestError(
                            f"cannot inspect DT_NEEDED {needed!r}: {error}"
                        ) from error
                    _resolve_alias(candidate, retained_roots)
                    candidates.append(candidate)
                if len(candidates) != 1:
                    raise HostABIManifestError(
                        f"DT_NEEDED {needed!r} resolves to {len(candidates)} declared providers"
                    )
                if (
                    str(candidates[0]) not in by_path
                    and candidates[0] not in pending
                ):
                    pending.append(candidates[0])
            pending.sort(key=str)

        libraries = sorted(
            by_path.values(), key=lambda row: row["sandbox_path"]
        )
        providers = {
            Path(row["sandbox_path"]).name: row["sandbox_path"]
            for row in libraries
        }
        if len(providers) != len(libraries):
            raise HostABIManifestError(
                "host ABI provider basenames are duplicated"
            )
        edges: list[dict[str, str]] = []
        for row in libraries:
            for needed in row["elf"]["needed"]:
                provider = providers.get(needed)
                if provider is None:
                    raise HostABIManifestError(
                        f"host ABI closure lacks provider for {needed}"
                    )
                edges.append(
                    {
                        "consumer": row["sandbox_path"],
                        "needed": needed,
                        "provider": provider,
                    }
                )
        edges.sort(
            key=lambda row: (row["consumer"], row["needed"], row["provider"])
        )
        for retained_root in retained_roots:
            retained_root.verify()
        core = {
            "kind": KIND,
            "schema_version": SCHEMA_VERSION,
            "implementation_version": IMPLEMENTATION_VERSION,
            "platform": observed_platform,
            "consumer_scan": normalized_scan,
            "library_roots": [str(root) for root in roots],
            "root_libraries": [str(path) for path in requested],
            "required_owner": {"uid": owner_uid, "gid": owner_gid},
            "libraries": libraries,
            "dependency_edges": edges,
            "policy": dict(POLICY),
        }
        identity = sha256_bytes(canonical_bytes(core))
        result = {
            **core,
            "identity_sha256": identity,
            "manifest_id": f"gpuhostabi_{identity[:32]}",
        }
        if len(canonical_bytes(result)) > MAX_JSON_BYTES:
            raise HostABIManifestError(
                "canonical host ABI manifest exceeds the replay size bound"
            )
        return validate_manifest(
            result,
            require_production_owner=(owner_uid, owner_gid) == (0, 0),
        )
    finally:
        for retained_root in retained_roots:
            retained_root.close()


def _normalize_elf(value: Any, label: str) -> dict[str, Any]:
    item = _exact(
        value,
        label,
        {
            "elf_class",
            "endianness",
            "machine",
            "elf_type",
            "soname",
            "needed",
            "interpreter",
            "rpath",
            "runpath",
        },
    )
    if item["elf_class"] != 64 or item["endianness"] != "little" or item["machine"] != EM_X86_64 or item["elf_type"] not in {2, 3}:
        raise HostABIManifestError(f"{label} has an unsupported architecture")
    soname = item["soname"]
    if soname is not None and (not isinstance(soname, str) or not SONAME_RE.fullmatch(soname)):
        raise HostABIManifestError(f"{label}.soname is invalid")
    needed = item["needed"]
    if (
        not isinstance(needed, list)
        or needed != sorted(needed)
        or len(needed) > MAX_DEPENDENCIES_PER_LIBRARY
        or len(set(needed)) != len(needed)
        or any(not isinstance(name, str) or not SONAME_RE.fullmatch(name) for name in needed)
    ):
        raise HostABIManifestError(f"{label}.needed is invalid")
    interpreter = item["interpreter"]
    if interpreter is not None:
        interpreter = str(_absolute(interpreter, f"{label}.interpreter"))
        if interpreter != SANDBOX_INTERPRETER:
            raise HostABIManifestError(
                f"{label}.interpreter is outside the exact sandbox ABI"
            )
    loader_paths: dict[str, list[str]] = {}
    for field in ("rpath", "runpath"):
        loader_paths[field] = _loader_search_paths(
            item[field], f"{label}.{field}"
        )
    if loader_paths["rpath"] and loader_paths["runpath"]:
        raise HostABIManifestError(
            f"{label} cannot contain both DT_RPATH and DT_RUNPATH"
        )
    return {
        **item,
        "soname": soname,
        "needed": needed,
        "interpreter": interpreter,
        "rpath": loader_paths["rpath"],
        "runpath": loader_paths["runpath"],
    }


def validate_manifest(value: Any, *, require_production_owner: bool = False) -> dict[str, Any]:
    fields = {
        "kind",
        "schema_version",
        "implementation_version",
        "platform",
        "consumer_scan",
        "library_roots",
        "root_libraries",
        "required_owner",
        "libraries",
        "dependency_edges",
        "policy",
        "identity_sha256",
        "manifest_id",
    }
    item = _exact(value, "host ABI manifest", fields)
    if (
        item["kind"] != KIND
        or item["schema_version"] != SCHEMA_VERSION
        or item["implementation_version"] != IMPLEMENTATION_VERSION
        or item["policy"] != POLICY
    ):
        raise HostABIManifestError("host ABI manifest header/policy is unsupported")
    platform = _platform(item["platform"])
    normalized_scan = normalize_consumer_scan(item["consumer_scan"])
    root_values = item["library_roots"]
    if not isinstance(root_values, list):
        raise HostABIManifestError("host ABI library_roots must be an array")
    roots = [_absolute(value, "host ABI library root") for value in root_values]
    if (
        not roots
        or len(roots) > 16
        or roots != sorted(roots, key=str)
        or len(set(roots)) != len(roots)
    ):
        raise HostABIManifestError("host ABI library_roots are noncanonical")
    roots_text = [str(root) for root in roots]
    requested_value = item["root_libraries"]
    if not isinstance(requested_value, list):
        raise HostABIManifestError("host ABI root_libraries must be an array")
    requested = [_absolute(value, "host ABI root library") for value in requested_value]
    if not requested or requested != sorted(requested, key=str) or len(set(requested)) != len(requested):
        raise HostABIManifestError("host ABI root_libraries are noncanonical")
    for path in requested:
        _beneath(path, roots, "host ABI root library")
    if {path.name for path in requested} != set(normalized_scan["external_sonames"]):
        raise HostABIManifestError(
            "host ABI root libraries differ from the consumer scan"
        )
    owner = _exact(item["required_owner"], "host ABI required owner", {"uid", "gid"})
    owner = {
        "uid": _integer(owner["uid"], "host ABI owner UID", 0, 2**31 - 1),
        "gid": _integer(owner["gid"], "host ABI owner GID", 0, 2**31 - 1),
    }
    if require_production_owner and owner != {"uid": 0, "gid": 0}:
        raise HostABIManifestError("production host ABI libraries must be root-owned")
    if require_production_owner and roots_text != list(PRODUCTION_LIBRARY_ROOTS):
        raise HostABIManifestError(
            "production host ABI library root must be exactly /usr/lib64"
        )
    if require_production_owner and normalized_scan["runtime_loaded_sonames"] != production_runtime_loaded_sonames(
        platform["nvidia_driver_version"]
    ):
        raise HostABIManifestError(
            "production runtime-loaded SONAME roots are not the exact audited set"
        )
    values = item["libraries"]
    if not isinstance(values, list) or not 1 <= len(values) <= MAX_LIBRARIES:
        raise HostABIManifestError("host ABI library set is outside its bound")
    libraries: list[dict[str, Any]] = []
    for ordinal, value in enumerate(values, 1):
        row = _exact(
            value,
            f"host ABI library {ordinal}",
            {"sandbox_path", "source_path", "sha256", "byte_count", "uid", "gid", "mode", "aliases", "elf"},
        )
        sandbox_path = _absolute(row["sandbox_path"], f"host ABI library {ordinal} sandbox path")
        source_path = _absolute(row["source_path"], f"host ABI library {ordinal} source path")
        _beneath(sandbox_path, roots, "host ABI sandbox library")
        _beneath(source_path, roots, "host ABI source library")
        if sandbox_path.parent not in roots or source_path.parent not in roots:
            raise HostABIManifestError("host ABI libraries must be direct children of a declared root")
        mode = _bounded_text(row["mode"], f"host ABI library {ordinal} mode", 4)
        if not MODE_RE.fullmatch(mode) or int(mode, 8) & 0o7022 or not int(mode, 8) & 0o444:
            raise HostABIManifestError(f"host ABI library {ordinal} mode is unsafe")
        uid = _integer(row["uid"], f"host ABI library {ordinal} UID", 0, 2**31 - 1)
        gid = _integer(row["gid"], f"host ABI library {ordinal} GID", 0, 2**31 - 1)
        if (uid, gid) != (owner["uid"], owner["gid"]):
            raise HostABIManifestError(f"host ABI library {ordinal} owner differs")
        aliases_value = row["aliases"]
        if not isinstance(aliases_value, list) or len(aliases_value) > MAX_ALIAS_DEPTH:
            raise HostABIManifestError(f"host ABI library {ordinal} aliases are invalid")
        aliases: list[dict[str, Any]] = []
        for alias_ordinal, alias_value in enumerate(aliases_value, 1):
            alias = _exact(alias_value, f"host ABI library {ordinal} alias {alias_ordinal}", {"path", "target", "uid", "gid", "mode"})
            alias_path = _absolute(alias["path"], f"host ABI library {ordinal} alias path")
            _beneath(alias_path, roots, "host ABI alias")
            target = _bounded_text(alias["target"], f"host ABI library {ordinal} alias target", 4096)
            alias_mode = _bounded_text(alias["mode"], f"host ABI library {ordinal} alias mode", 4)
            alias_uid = _integer(alias["uid"], f"host ABI library {ordinal} alias UID", 0, 2**31 - 1)
            alias_gid = _integer(alias["gid"], f"host ABI library {ordinal} alias GID", 0, 2**31 - 1)
            if not MODE_RE.fullmatch(alias_mode) or (alias_uid, alias_gid) != (owner["uid"], owner["gid"]):
                raise HostABIManifestError(f"host ABI library {ordinal} alias metadata is unsafe")
            aliases.append({"path": str(alias_path), "target": target, "uid": alias_uid, "gid": alias_gid, "mode": alias_mode})
        expected_cursor = sandbox_path
        visited_alias_paths: set[Path] = set()
        for alias in aliases:
            if expected_cursor in visited_alias_paths:
                raise HostABIManifestError(
                    f"host ABI library {ordinal} alias chain cycles"
                )
            visited_alias_paths.add(expected_cursor)
            if alias["path"] != str(expected_cursor):
                raise HostABIManifestError(
                    f"host ABI library {ordinal} alias chain is discontinuous"
                )
            next_path = (
                Path(alias["target"])
                if os.path.isabs(alias["target"])
                else expected_cursor.parent / alias["target"]
            )
            expected_cursor = Path(os.path.normpath(str(next_path)))
            _beneath(expected_cursor, roots, "host ABI alias target")
            if expected_cursor.parent not in roots:
                raise HostABIManifestError(
                    f"host ABI library {ordinal} alias target is not a direct child"
                )
        if expected_cursor != source_path:
            raise HostABIManifestError(
                f"host ABI library {ordinal} alias chain does not reach its source"
            )
        normalized = {
            "sandbox_path": str(sandbox_path),
            "source_path": str(source_path),
            "sha256": _digest(row["sha256"], f"host ABI library {ordinal} SHA-256"),
            "byte_count": _integer(row["byte_count"], f"host ABI library {ordinal} byte_count", 1, MAX_LIBRARY_BYTES),
            "uid": uid,
            "gid": gid,
            "mode": mode,
            "aliases": aliases,
            "elf": _normalize_elf(row["elf"], f"host ABI library {ordinal} ELF"),
        }
        libraries.append(normalized)
    if libraries != sorted(libraries, key=lambda row: row["sandbox_path"]):
        raise HostABIManifestError("host ABI libraries are not sorted")
    sandbox_paths = [row["sandbox_path"] for row in libraries]
    if len(set(sandbox_paths)) != len(sandbox_paths) or not set(map(str, requested)) <= set(sandbox_paths):
        raise HostABIManifestError("host ABI library paths are duplicated or omit a root")
    providers = {Path(path).name: path for path in sandbox_paths}
    if len(providers) != len(libraries):
        raise HostABIManifestError("host ABI provider basenames are duplicated")
    expected_edges = sorted(
        (
            {"consumer": row["sandbox_path"], "needed": needed, "provider": providers.get(needed, "")}
            for row in libraries
            for needed in row["elf"]["needed"]
        ),
        key=lambda row: (row["consumer"], row["needed"], row["provider"]),
    )
    if any(not row["provider"] for row in expected_edges) or item["dependency_edges"] != expected_edges:
        raise HostABIManifestError("host ABI dependency graph is incomplete or noncanonical")
    # A content-addressed but disconnected library is still excess ambient
    # authority.  The inventory must equal, rather than merely contain, the
    # recursive graph reachable from the consumer/runtime roots.
    by_path = {row["sandbox_path"]: row for row in libraries}
    reachable = set(map(str, requested))
    pending = sorted(reachable)
    while pending:
        consumer_path = pending.pop(0)
        consumer = by_path[consumer_path]
        for needed in consumer["elf"]["needed"]:
            provider = providers[needed]
            if provider not in reachable:
                reachable.add(provider)
                pending.append(provider)
        pending.sort()
    if reachable != set(sandbox_paths):
        raise HostABIManifestError(
            "host ABI library set contains a disconnected or unreachable provider"
        )
    core = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "platform": platform,
        "consumer_scan": normalized_scan,
        "library_roots": roots_text,
        "root_libraries": [str(path) for path in requested],
        "required_owner": owner,
        "libraries": libraries,
        "dependency_edges": expected_edges,
        "policy": dict(POLICY),
    }
    identity = sha256_bytes(canonical_bytes(core))
    expected = {**core, "identity_sha256": identity, "manifest_id": f"gpuhostabi_{identity[:32]}"}
    if len(canonical_bytes(expected)) > MAX_JSON_BYTES:
        raise HostABIManifestError(
            "canonical host ABI manifest exceeds the replay size bound"
        )
    if item != expected:
        raise HostABIManifestError("host ABI manifest is noncanonical or has an invalid identity")
    return expected


def replay_manifest(
    value: Any,
    *,
    observed_driver_version: str | None = None,
    observed_platform: dict[str, Any] | None = None,
    module_report_path: Path = Path("/proc/driver/nvidia/version"),
    require_production_owner: bool = True,
) -> dict[str, Any]:
    manifest = validate_manifest(value, require_production_owner=require_production_owner)
    expected_platform = manifest["platform"]
    driver = observed_driver_version or expected_platform["nvidia_driver_version"]
    current_platform = _platform(
        observed_platform
        if observed_platform is not None
        else observe_platform(driver, module_report_path=module_report_path)
    )
    if current_platform != expected_platform:
        raise HostABIManifestError("booted kernel/NVIDIA platform differs from the host ABI manifest")
    roots = [Path(value) for value in manifest["library_roots"]]
    owner = manifest["required_owner"]
    retained_roots = _retain_library_roots(
        roots,
        expected_uid=owner["uid"],
        expected_gid=owner["gid"],
        require_trusted_chain=require_production_owner,
    )
    try:
        for ordinal, expected in enumerate(manifest["libraries"], 1):
            resolved, aliases = _resolve_alias(
                Path(expected["sandbox_path"]), retained_roots
            )
            if (
                str(resolved) != expected["source_path"]
                or aliases != expected["aliases"]
            ):
                raise HostABIManifestError(
                    f"host ABI library {ordinal} alias chain changed"
                )
            root = _root_for_direct_child(resolved, retained_roots)
            body, info = _stable_regular_at(
                root, resolved, f"host ABI library {ordinal}"
            )
            observed = {
                "sha256": sha256_bytes(body),
                "byte_count": len(body),
                "uid": info.st_uid,
                "gid": info.st_gid,
                "mode": f"{stat.S_IMODE(info.st_mode):04o}",
                "elf": parse_elf64_x86_64(
                    body, f"host ABI library {ordinal}"
                ),
            }
            if any(observed[key] != expected[key] for key in observed):
                raise HostABIManifestError(
                    f"host ABI library {ordinal} changed"
                )
        for retained_root in retained_roots:
            retained_root.verify()
        return manifest
    finally:
        for retained_root in retained_roots:
            retained_root.close()


def _strict_json(body: bytes, label: str) -> Any:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise HostABIManifestError(f"{label} contains duplicate key {key!r}")
            result[key] = value
        return result

    def reject(value: str) -> None:
        raise HostABIManifestError(f"{label} contains non-finite value {value}")

    try:
        return json.loads(body.decode("utf-8"), object_pairs_hook=pairs, parse_constant=reject)
    except HostABIManifestError:
        raise
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise HostABIManifestError(f"{label} is not strict JSON: {error}") from error


def _load_execution_image_receipt(
    path: Path, expected_sha256: str
) -> dict[str, Any]:
    """Use the authoritative sibling validator, including the complete image hash."""

    module_path = Path(__file__).resolve().with_name("build_execution_image.py")
    spec = importlib.util.spec_from_file_location(
        "himr_host_abi_execution_image_validator", module_path
    )
    if spec is None or spec.loader is None:
        raise HostABIManifestError(
            "cannot load the authoritative execution-image validator"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
        return module.load_receipt(
            str(path), expected_sha256, verify_image=True
        )
    except Exception as error:
        error_type = getattr(module, "ExecutionImageError", ())
        if error_type and isinstance(error, error_type):
            raise HostABIManifestError(
                f"execution-image receipt failed authoritative replay: {error}"
            ) from error
        raise
    finally:
        sys.modules.pop(spec.name, None)


def load_manifest(path_value: str | Path, expected_sha256: str | None = None, *, replay: bool = False, observed_driver_version: str | None = None) -> dict[str, Any]:
    path = _absolute(path_value, "host ABI manifest path")
    body, _ = _stable_regular(path, "host ABI manifest", maximum=MAX_JSON_BYTES)
    if expected_sha256 is not None and sha256_bytes(body) != _digest(expected_sha256, "expected host ABI manifest SHA-256"):
        raise HostABIManifestError("host ABI manifest SHA-256 differs")
    value = _strict_json(body, "host ABI manifest")
    if body != canonical_bytes(value):
        raise HostABIManifestError("host ABI manifest is not canonical JSON")
    return replay_manifest(value, observed_driver_version=observed_driver_version) if replay else validate_manifest(value)


def _write_exclusive(path: Path, value: Any) -> None:
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"refusing to replace {path}")
    parent = path.parent
    info = parent.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise HostABIManifestError("manifest parent must be current-user-owned mode 0700")
    body = canonical_bytes(value)
    if len(body) > MAX_JSON_BYTES:
        raise HostABIManifestError(
            "canonical host ABI manifest exceeds the replay size bound"
        )
    temporary = parent / f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0), 0o400)
    published = False
    try:
        try:
            offset = 0
            while offset < len(body):
                offset += os.write(descriptor, body[offset:])
            os.fsync(descriptor)
            os.fchmod(descriptor, 0o400)
        finally:
            os.close(descriptor)
        os.link(temporary, path, follow_symlinks=False)
        published = True
        temporary.unlink()
        directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except Exception as error:
        if published:
            raise HostABIManifestPublishedError(
                f"manifest is visible at {path} but durability finalization failed: {error}"
            ) from error
        raise
    finally:
        with contextlib.suppress(OSError):
            temporary.unlink()


def contract_document() -> dict[str, Any]:
    return {
        "kind": "himr_gpu_host_abi_manifest_v1_contract",
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "architecture": {"elf_class": 64, "endianness": "little", "machine": EM_X86_64},
        "maximum_libraries": MAX_LIBRARIES,
        "maximum_consumer_elves": MAX_CONSUMER_ELVES,
        "maximum_resolution_states": MAX_RESOLUTION_STATES,
        "maximum_dependencies_per_library": MAX_DEPENDENCIES_PER_LIBRARY,
        "manifest_kind": KIND,
        "production_library_roots": list(PRODUCTION_LIBRARY_ROOTS),
        "global_image_library_directories": sorted(
            GLOBAL_IMAGE_LIBRARY_DIRECTORIES
        ),
        "private_image_library_directories": sorted(
            PRIVATE_IMAGE_LIBRARY_DIRECTORIES
        ),
        "explicit_image_dlopen_roots": sorted(EXPLICIT_IMAGE_DLOPEN_ROOTS),
        "sandbox_interpreter": SANDBOX_INTERPRETER,
        "production_runtime_loaded_sonames_example": production_runtime_loaded_sonames(
            "610.57.04"
        ),
        "policy": dict(POLICY),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("contracts")
    create = commands.add_parser("create")
    create.add_argument("--execution-image-receipt", required=True)
    create.add_argument("--expected-execution-image-receipt-sha256", required=True)
    create.add_argument("--nvidia-driver-version", required=True)
    create.add_argument("--output", required=True)
    validate = commands.add_parser("validate")
    validate.add_argument("--manifest", required=True)
    validate.add_argument("--expected-manifest-sha256")
    validate.add_argument("--replay", action="store_true")
    validate.add_argument("--observed-driver-version")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "contracts":
            response = contract_document()
        elif args.command == "create":
            receipt_path = _absolute(
                args.execution_image_receipt, "execution-image receipt path"
            )
            receipt_sha256 = _digest(
                args.expected_execution_image_receipt_sha256,
                "expected execution-image receipt SHA-256",
            )
            receipt = _load_execution_image_receipt(
                receipt_path, receipt_sha256
            )
            library_roots = list(PRODUCTION_LIBRARY_ROOTS)
            scan = derive_consumer_scan(
                receipt,
                runtime_loaded_sonames=production_runtime_loaded_sonames(
                    args.nvidia_driver_version
                ),
            )
            root_libraries = root_library_paths_for_consumer_scan(
                scan, library_roots
            )
            manifest = build_manifest(
                root_libraries,
                consumer_scan=scan,
                library_roots=library_roots,
                nvidia_driver_version=args.nvidia_driver_version,
            )
            output = _absolute(args.output, "manifest output path")
            _write_exclusive(output, manifest)
            response = {
                "status": "created",
                "path": str(output),
                "sha256": sha256_bytes(canonical_bytes(manifest)),
                "identity_sha256": manifest["identity_sha256"],
                "manifest_id": manifest["manifest_id"],
                "library_count": len(manifest["libraries"]),
                "consumer_elf_count": manifest["consumer_scan"]["elf_file_count"],
                "source_tree_regular_file_count": manifest["consumer_scan"][
                    "source_tree_regular_file_count"
                ],
            }
        else:
            manifest = load_manifest(
                args.manifest,
                args.expected_manifest_sha256,
                replay=args.replay,
                observed_driver_version=args.observed_driver_version,
            )
            response = {
                "status": "replayed" if args.replay else "validated",
                "identity_sha256": manifest["identity_sha256"],
                "manifest_id": manifest["manifest_id"],
                "library_count": len(manifest["libraries"]),
                "files_written": False,
            }
        sys.stdout.buffer.write(canonical_bytes(response))
        return 0
    except (HostABIManifestError, FileExistsError, OSError, ValueError) as error:
        failure = {
            "status": "failed",
            "command": args.command,
            "error": {"type": type(error).__name__, "message": str(error)},
            "files_written": isinstance(
                error, HostABIManifestPublishedError
            ),
        }
        sys.stderr.buffer.write(canonical_bytes(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
