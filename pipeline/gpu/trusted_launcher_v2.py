#!/usr/bin/python3.14 -IB
"""Root-anchored, restart-portable launcher for private GPU ASR batches.

This file is deliberately Python-standard-library only.  The installed copy is
the trust anchor outside the execution SquashFS: it authenticates all small
control documents, performs one complete image hash, resolves the admitted GPU
UUID to its current host index, mounts that exact retained image descriptor, and
only then exposes bundled code to a networkless Bubblewrap process.

The launcher has no acquisition, catalogue, publication, identity, biometric,
wiki, archive, or deletion authority.  Candidate mode is an explicit synthetic
canary lane.  The separately named ``local-private-production`` lane may consume
production lineage with a validated candidate runtime, but records that its
sealed controls remain mutable by the same host UID; it is never equivalent to
root-admitted production.
"""

from __future__ import annotations

import argparse
import contextlib
import ctypes
import fcntl
import hashlib
import json
import mmap
import os
import posixpath
import re
import resource
import secrets
import selectors
import signal
import stat
import struct
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Sequence


PROFILE_KIND = "himr_gpu_trusted_launcher_profile"
INSTALL_SPEC_KIND = "himr_gpu_trusted_launcher_install_spec"
INSTALL_MANIFEST_KIND = "himr_gpu_trusted_launcher_install_manifest"
ATTESTATION_KIND = "himr_gpu_trusted_launch_attestation"
LINEAGE_ATTESTATION_KIND = "himr_gpu_asr_lineage_preflight"
LOCAL_READINESS_KIND = "himr_gpu_local_private_readiness"
HOST_ABI_KIND = "himr_gpu_host_abi_manifest"
RUNTIME_KIND = "himr_gpu_runtime_admission_receipt"
ROOT_KIND = "himr_gpu_root_registration"
PRODUCTION_PROFILE_KIND = "himr_gpu_production_profile"
SCHEMA_VERSION = 2
IMPLEMENTATION_VERSION = "0.3.0"
MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_IMAGE_BYTES = 16 * 1024 * 1024 * 1024
MAX_TOOL_BYTES = 256 * 1024 * 1024
MAX_TOOL_OUTPUT_BYTES = 64 * 1024
MAX_NVIDIA_XML_BYTES = 4 * 1024 * 1024
MAX_MAPPINGS = 64
MAX_ITEM_READ_BINDINGS = 6
MAX_TOTAL_READ_BINDINGS = 32 * MAX_ITEM_READ_BINDINGS
MAX_LINEAGE_BYTES = 64 * 1024 * 1024
MAX_CGROUP_BYTES = 64 * 1024
MAX_LINEAGE_ATTESTATION_BYTES = 1024 * 1024
MAX_HOST_ABI_LIBRARY_BYTES = 512 * 1024 * 1024
MAX_HOST_ABI_PROJECTION_BYTES = 2 * 1024 * 1024 * 1024
MAX_HOST_ABI_LIBRARIES = 256
MAX_HOST_ABI_CONSUMERS = 4096
MAX_HOST_ABI_RESOLUTION_STATES = 65536
MAX_HOST_ABI_DEPENDENCIES = 256
MAX_HOST_ABI_ALIAS_DEPTH = 16
MAX_NVIDIA_KERNEL_REPORT_BYTES = 64 * 1024
MAX_PREFLIGHT_STDIO_BYTES = 1024 * 1024
LINEAGE_PREFLIGHT_SECONDS = 10 * 60
MAX_HOST_MEMORY_BYTES = 12 * 1024**3
MAX_HOST_PIDS = 64
MAX_HOST_NOFILE = 1024
MAX_HOST_FSIZE_BYTES = 32 * 1024**2
HASH_CHUNK_BYTES = 4 * 1024 * 1024
MOUNT_READY_SECONDS = 8.0
PROCESS_TERMINATE_SECONDS = 5.0
PR_SET_PDEATHSIG = 1
PDEATHSIG_FAILURE_STATUS = 126
PRIVATE_RUNTIME_DIRECTORY = "himr-gpu-launcher-v2"
TRANSIENT_DIRECTORY = "transient"
LAUNCHER_LOCK_FILE = "launcher.lock"
ATTESTATION_FILE = "launch-attestation.json"
MOUNT_DIRECTORY = "image"
PREFLIGHT_DIRECTORY = "preflight-output"
IMAGE_PRODUCTION_MODE = 0o444
IMAGE_CANDIDATE_MODE = 0o400
MODE_PRODUCTION = "production"
MODE_LOCAL_PRIVATE = "local-private-production"
MODE_SYNTHETIC = "candidate-synthetic-canary"
MODES = frozenset({MODE_PRODUCTION, MODE_LOCAL_PRIVATE, MODE_SYNTHETIC})
EXECUTION_CLASS_PRODUCTION = "production_private_asr"
EXECUTION_CLASS_LOCAL_PRIVATE = "local_private_production_asr"
EXECUTION_CLASS_SYNTHETIC = "synthetic_canary"
MODE_EXECUTION_CLASS = {
    MODE_PRODUCTION: EXECUTION_CLASS_PRODUCTION,
    MODE_LOCAL_PRIVATE: EXECUTION_CLASS_LOCAL_PRIVATE,
    MODE_SYNTHETIC: EXECUTION_CLASS_SYNTHETIC,
}
MODE_RUNTIME_STATUS = {
    MODE_PRODUCTION: "admitted",
    MODE_LOCAL_PRIVATE: "candidate",
    MODE_SYNTHETIC: "candidate",
}
COLD_ROOT = PurePosixPath("/mnt/archive/HIMR")
IMAGE_MAPPING_PREFIX = PurePosixPath("/opt/himr-gpu")
CONTROL_ROOT = PurePosixPath("/run/himr-gpu/control")
INPUT_ROOT = PurePosixPath("/run/himr-gpu/input")
OUTPUT_ROOT = PurePosixPath("/run/himr-gpu/output")
STATE_ROOT = PurePosixPath("/run/himr-gpu/state")

SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
GPU_UUID_RE = re.compile(r"GPU-[A-Za-z0-9-]{8,92}\Z")
REGISTRATION_ID_RE = re.compile(r"gpurootreg_[0-9a-f]{32}\Z")
ROOT_ID_RE = re.compile(r"[a-z][a-z0-9._-]{0,127}\Z")
NAME_RE = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
SAFE_COMPONENT_RE = re.compile(r"[A-Za-z0-9._+@-]{1,255}\Z")
HOST_ABI_VERSION_RE = re.compile(r"[0-9]+(?:\.[0-9]+){2,3}\Z")
HOST_ABI_SONAME_RE = re.compile(r"[A-Za-z0-9_+.-]{1,255}\Z")
HOST_ABI_ORIGIN_NEEDED_RE = re.compile(
    r"\$ORIGIN(?:/[A-Za-z0-9_+.-]{1,255}){1,16}\Z"
)
MODE_RE = re.compile(r"[0-7]{4}\Z")

HOST_ABI_GLOBAL_IMAGE_LIBRARY_DIRECTORIES = frozenset(
    {"runtime/lib/python3.12/site-packages/nvidia/cublas/lib"}
)
HOST_ABI_PRIVATE_IMAGE_LIBRARY_DIRECTORIES = frozenset(
    {
        "runtime/lib/python3.12/site-packages/av.libs",
        "runtime/lib/python3.12/site-packages/ctranslate2.libs",
        "runtime/lib/python3.12/site-packages/numpy.libs",
    }
)
HOST_ABI_PYTHON_SITE_PACKAGES = PurePosixPath(
    "runtime/lib/python3.12/site-packages"
)
HOST_ABI_EXPLICIT_IMAGE_DLOPEN_ROOTS = frozenset(
    {
        "runtime/lib/python3.12/site-packages/nvidia/cublas/lib/libcublas.so.12",
        "runtime/lib/python3.12/site-packages/nvidia/cublas/lib/libcublasLt.so.12",
    }
)

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

BTRFS_IOC_FS_INFO = 0x8400941F
BTRFS_FS_INFO_SIZE = 1024
BTRFS_FSID_OFFSET = 16
BTRFS_FSID_BYTES = 16

SYSTEM_TOOL_PATHS = {
    "bubblewrap": "/usr/bin/bwrap",
    "squashfuse": "/usr/bin/squashfuse_ll",
    "fusermount": "/usr/bin/fusermount3",
    "nvidia_smi": "/usr/bin/nvidia-smi",
    "host_python": "/usr/bin/python3.14",
    "systemctl": "/usr/bin/systemctl",
    "systemd_run": "/usr/bin/systemd-run",
}
RUNTIME_TOOL_NAMES = frozenset(SYSTEM_TOOL_PATHS)
REQUIRED_MAPPING_LAYOUT = {
    "application_root": ("/opt/himr-gpu/app", "application_root"),
    "application_support_root": (
        "/opt/himr-gpu/corpus/src",
        "application_support_root",
    ),
    "runtime_root": ("/opt/himr-gpu/runtime", "runtime_root"),
    "python_executable": ("/opt/himr-gpu/runtime/bin/python3.12", "executable"),
    "model_bundle": ("/opt/himr-gpu/model", "model_bundle"),
    "model_root": ("/opt/himr-gpu/model/snapshot", "model_root"),
    "adapter_source": ("/opt/himr-gpu/app/production_asr_v5.py", "python_source"),
    "worker_source": ("/opt/himr-gpu/app/production_asr_batch_v2.py", "python_source"),
    "verified_loader": ("/opt/himr-gpu/app/verified_dependency_loader.py", "python_source"),
    "model_admission_helper": ("/opt/himr-gpu/app/admit_hf_model.py", "python_source"),
    "runtime_admission_helper": ("/opt/himr-gpu/app/admit_runtime_v2.py", "python_source"),
    "cublas_library_directory": (
        "/opt/himr-gpu/runtime/lib/python3.12/site-packages/nvidia/cublas/lib",
        "shared_library_directory",
    ),
}
REQUIRED_MAPPING_NAMES = frozenset(REQUIRED_MAPPING_LAYOUT)
GATE_NAMES = (
    "accuracy",
    "semantic_compatibility",
    "packed_30m",
    "packed_2h",
    "thermal_8h",
    "batch_32",
    "scheduler",
    "crash_recovery",
    "launcher_isolation",
)

WORK_ORDER_FIELDS = frozenset(
    {
        "kind",
        "schema_version",
        "implementation_version",
        "job_id",
        "input",
        "source_lineage",
        "runtime_admission",
        "production_profile",
        "hot_root",
        "execution_contract",
        "transcript_semantics",
        "catalog_context",
        "output",
        "policy",
        "identity_sha256",
        "work_order_id",
    }
)
WORK_ORDER_KIND = "himr_faster_whisper_gpu_work_order"
WORK_ORDER_SCHEMA_VERSION = 5
WORK_ORDER_IMPLEMENTATION_VERSION = "0.5.0"

POLICY = {
    "visibility": "private",
    "network_access": False,
    "archive_access": False,
    "wheelhouse_execution_dependency": False,
    "publication_authority": "none",
    "catalogue_mutation_authority": "none",
    "identity_authority": "none",
    "biometric_authority": "none",
    "wiki_authority": "none",
    "deletion_authority": "launcher_owned_transient_mount_only",
    "candidate_authority": "sealed_synthetic_canary_only",
    "local_private_production_authority": "production_lineage_candidate_runtime_same_uid",
}

RUNTIME_POLICY = {
    "visibility": "private",
    "network_access": False,
    "runtime_model_tree_scan_per_execution": False,
    "wheelhouse_execution_dependency": False,
    "deep_audit_is_separate": True,
    "persisted_numeric_device_authority": False,
    "external_trust_anchor_required": True,
    "publication_authority": "none",
    "catalogue_mutation_authority": "none",
    "identity_authority": "none",
    "archive_authority": "none",
    "deletion_authority": "none",
}

ROOT_POLICY = {
    "append_only_successor_documents": True,
    "filesystem_uuid_is_placement_authority": True,
    "historical_stat_fields_are_authoritative": False,
    "live_retained_descriptor_checks_required": True,
}

LINEAGE_POLICY = {
    "network_access": False,
    "gpu_access": False,
    "inference_authority": "none",
    "output_authority": "single_lineage_attestation_only",
    "publication_authority": "none",
    "catalogue_mutation_authority": "none",
    "identity_authority": "none",
    "archive_authority": "none",
    "deletion_authority": "none",
}

HOST_ABI_POLICY = {
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


class TrustedLauncherError(RuntimeError):
    """A trusted input, host invariant, mount, sandbox, or child failed."""


class LaunchInterrupted(TrustedLauncherError):
    """The launcher received an operator interruption signal."""


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
        raise TrustedLauncherError(f"value is not canonical JSON: {error}") from error


def sha256_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise TrustedLauncherError(f"JSON contains duplicate key {key!r}")
        result[key] = value
    return result


def parse_json(body: bytes, label: str) -> Any:
    def reject(value: str) -> None:
        raise TrustedLauncherError(f"{label} contains non-finite value {value}")

    try:
        return json.loads(
            body.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_pairs,
            parse_constant=reject,
        )
    except TrustedLauncherError:
        raise
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise TrustedLauncherError(f"{label} is not strict JSON: {error}") from error


def _exact(value: Any, label: str, fields: set[str] | frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != set(fields):
        observed = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise TrustedLauncherError(f"{label} has unexpected fields: {observed}")
    return value


def _integer(value: Any, label: str, minimum: int, maximum: int = 2**63 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise TrustedLauncherError(f"{label} must be an integer within [{minimum}, {maximum}]")
    return value


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise TrustedLauncherError(f"{label} must be a lowercase SHA-256")
    return value


def _bounded_text(value: Any, label: str, maximum: int = 4096) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > maximum
        or any(character in value for character in ("\x00", "\r", "\n"))
    ):
        raise TrustedLauncherError(f"{label} must be bounded non-empty text")
    return value


def normalized_absolute_path(value: Any, label: str) -> Path:
    text = _bounded_text(value, label)
    path = Path(text)
    if (
        not path.is_absolute()
        or path == Path("/")
        or os.path.normpath(text) != text
        or "\\" in text
        or "//" in text
    ):
        raise TrustedLauncherError(f"{label} must be one normalized absolute non-root path")
    pure = PurePosixPath(text)
    if pure == COLD_ROOT or COLD_ROOT in pure.parents:
        raise TrustedLauncherError(f"{label} may not reference the archive tier")
    return path


def normalized_relative_path(value: Any, label: str) -> PurePosixPath:
    text = _bounded_text(value, label)
    path = PurePosixPath(text)
    if (
        path.is_absolute()
        or str(path) != text
        or not path.parts
        or any(part in {"", ".", ".."} or not SAFE_COMPONENT_RE.fullmatch(part) for part in path.parts)
    ):
        raise TrustedLauncherError(f"{label} must be a normalized traversal-free relative path")
    return path


def _is_within(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _reject_symlink_components(path: Path, label: str) -> None:
    """Reject lexical path indirection before any caller opens the target."""

    current = Path("/")
    for component in path.parts[1:]:
        current /= component
        try:
            info = current.lstat()
        except OSError as error:
            raise TrustedLauncherError(f"cannot inspect {label} component {current}: {error}") from error
        if stat.S_ISLNK(info.st_mode):
            raise TrustedLauncherError(f"{label} contains a symlink component: {current}")


def _identity(value: os.stat_result) -> tuple[int, ...]:
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


def _directory_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_uid,
        value.st_gid,
        value.st_ctime_ns,
    )


def _writable_directory_authority_identity(value: os.stat_result) -> tuple[int, ...]:
    """Identity fields that remain stable while an authorized child mutates.

    Creating a result/event below an exposed writable root legitimately changes
    that directory's link count and ctime.  The retained descriptor plus
    device/inode, type/mode, and owner still prove that the lexical path names
    the same writable authority after the child exits.
    """

    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
    )


def _read_fd(descriptor: int, expected_size: int, maximum: int, label: str) -> bytes:
    if expected_size < 1 or expected_size > maximum:
        raise TrustedLauncherError(f"{label} size is outside its bound")
    chunks: list[bytes] = []
    remaining = expected_size
    while remaining:
        chunk = os.read(descriptor, min(HASH_CHUNK_BYTES, remaining))
        if not chunk:
            raise TrustedLauncherError(f"{label} ended before its sealed size")
        chunks.append(chunk)
        remaining -= len(chunk)
    if os.read(descriptor, 1):
        raise TrustedLauncherError(f"{label} grew while read")
    return b"".join(chunks)


def _hash_fd(descriptor: int, expected_size: int, maximum: int, label: str) -> str:
    """Hash a retained file with bounded memory (the image may be many GiB)."""

    if expected_size < 1 or expected_size > maximum:
        raise TrustedLauncherError(f"{label} size is outside its bound")
    digest = hashlib.sha256()
    remaining = expected_size
    while remaining:
        chunk = os.read(descriptor, min(HASH_CHUNK_BYTES, remaining))
        if not chunk:
            raise TrustedLauncherError(f"{label} ended before its sealed size")
        digest.update(chunk)
        remaining -= len(chunk)
    if os.read(descriptor, 1):
        raise TrustedLauncherError(f"{label} grew while hashed")
    return digest.hexdigest()


@dataclass
class RetainedFile:
    path: Path
    descriptor: int
    info: os.stat_result
    sha256: str
    body: bytes | None

    @property
    def fd_path(self) -> str:
        return f"/proc/self/fd/{self.descriptor}"

    def verify(self) -> None:
        current = os.fstat(self.descriptor)
        try:
            linked = self.path.lstat()
        except OSError as error:
            raise TrustedLauncherError(f"retained file disappeared: {self.path}: {error}") from error
        if _identity(current) != _identity(self.info) or _identity(linked) != _identity(self.info):
            raise TrustedLauncherError(f"retained file changed: {self.path}")

    def close(self) -> None:
        if self.descriptor >= 0:
            os.close(self.descriptor)
            self.descriptor = -1

    def __enter__(self) -> "RetainedFile":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


@dataclass
class RetainedHostABIRoot:
    descriptors: list[int]
    observations: list[os.stat_result]
    components: tuple[str, ...] = ("usr", "lib64")

    @property
    def descriptor(self) -> int:
        return self.descriptors[-1]

    def verify(self) -> None:
        if len(self.descriptors) != 3 or len(self.observations) != 3:
            raise TrustedLauncherError("retained host ABI root chain is incomplete")
        for descriptor, expected in zip(
            self.descriptors, self.observations, strict=True
        ):
            if _directory_identity(os.fstat(descriptor)) != _directory_identity(expected):
                raise TrustedLauncherError("retained host ABI root directory changed")
        for ordinal, component in enumerate(self.components, 1):
            linked = os.stat(
                component,
                dir_fd=self.descriptors[ordinal - 1],
                follow_symlinks=False,
            )
            if _directory_identity(linked) != _directory_identity(
                self.observations[ordinal]
            ):
                raise TrustedLauncherError("retained host ABI ancestry link changed")

    def close(self) -> None:
        for descriptor in reversed(self.descriptors):
            with contextlib.suppress(OSError):
                os.close(descriptor)
        self.descriptors.clear()
        self.observations.clear()


@dataclass
class RetainedHostABILibrary(RetainedFile):
    root: RetainedHostABIRoot
    basename: str

    def verify(self) -> None:
        self.root.verify()
        current = os.fstat(self.descriptor)
        linked = os.stat(
            self.basename,
            dir_fd=self.root.descriptor,
            follow_symlinks=False,
        )
        if _identity(current) != _identity(self.info) or _identity(linked) != _identity(
            self.info
        ):
            raise TrustedLauncherError(f"retained host ABI library changed: {self.path}")


@dataclass
class RetainedMapping:
    """One immutable execution-image object retained across Bubblewrap exec.

    The lexical FUSE path is evidence only after this object is created. All
    sandbox binds use the retained descriptor, so replacing or remounting the
    launcher-visible pathname cannot redirect Bubblewrap after verification. Bubblewrap
    receives ``descriptor`` through ``--ro-bind-fd`` and consumes it before the
    sandbox payload starts.
    """

    name: str
    path: Path
    descriptor: int
    info: os.stat_result
    is_directory: bool

    def verify(self) -> None:
        current = os.fstat(self.descriptor)
        identity = _directory_identity if self.is_directory else _identity
        if identity(current) != identity(self.info):
            raise TrustedLauncherError(f"retained image mapping changed: {self.name}")

    def close(self) -> None:
        if self.descriptor >= 0:
            os.close(self.descriptor)
            self.descriptor = -1


@dataclass(frozen=True)
class ReadBindingSpec:
    ordinal: int
    work_order_identity_sha256: str
    role: str
    path: Path
    sha256: str
    expected_byte_count: int | None
    declared_mode: int
    allowed_owner_modes: frozenset[tuple[int, int]]


@dataclass
class RetainedReadBinding:
    spec: ReadBindingSpec
    retained: RetainedFile

    def attestation_row(self) -> dict[str, Any]:
        return {
            "role": self.spec.role,
            "path": str(self.spec.path),
            "target": str(self.spec.path),
            "sha256": self.retained.sha256,
            "byte_count": self.retained.info.st_size,
            "mode": f"{stat.S_IMODE(self.retained.info.st_mode):04o}",
        }


def retain_file(
    path_value: str | Path,
    label: str,
    *,
    expected_sha256: str | None,
    maximum: int,
    allowed_owner_modes: set[tuple[int, int]],
    keep_body: bool,
) -> RetainedFile:
    path = normalized_absolute_path(str(path_value), f"{label} path")
    _reject_symlink_components(path, label)
    try:
        lexical = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NONBLOCK", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as error:
        raise TrustedLauncherError(f"cannot retain {label}: {error}") from error
    try:
        opened = os.fstat(descriptor)
        if (
            stat.S_ISLNK(lexical.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or _identity(lexical) != _identity(opened)
            or opened.st_nlink != 1
            or (opened.st_uid, stat.S_IMODE(opened.st_mode)) not in allowed_owner_modes
        ):
            raise TrustedLauncherError(f"{label} metadata is outside its exact policy")
        body = _read_fd(descriptor, opened.st_size, maximum, label) if keep_body else None
        digest = sha256_bytes(body) if body is not None else _hash_fd(descriptor, opened.st_size, maximum, label)
        if expected_sha256 is not None and digest != _digest(
            expected_sha256, f"expected {label} SHA-256"
        ):
            raise TrustedLauncherError(f"{label} SHA-256 differs from its expected digest")
        retained = RetainedFile(path, descriptor, opened, digest, body)
        retained.verify()
        os.lseek(descriptor, 0, os.SEEK_SET)
        return retained
    except Exception:
        os.close(descriptor)
        raise


def _trusted_ancestors(path: Path, *, uid: int = 0) -> None:
    current = path
    while True:
        try:
            info = current.lstat()
        except OSError as error:
            raise TrustedLauncherError(f"cannot inspect trusted ancestor {current}: {error}") from error
        if stat.S_ISLNK(info.st_mode) or info.st_uid != uid or stat.S_IMODE(info.st_mode) & 0o022:
            raise TrustedLauncherError(f"trusted path has mutable/untrusted ancestor: {current}")
        if current == Path("/"):
            return
        current = current.parent


def _canonical_document(retained: RetainedFile, label: str) -> Any:
    if retained.body is None:
        raise TrustedLauncherError(f"{label} was not retained with its body")
    value = parse_json(retained.body, label)
    if retained.body != canonical_bytes(value):
        raise TrustedLauncherError(f"{label} is not canonical JSON")
    return value


def _reference(value: Any, label: str, *, identity: bool = False) -> dict[str, Any]:
    fields = {"path", "sha256", "identity_sha256"} if identity else {"path", "sha256"}
    item = _exact(value, label, fields)
    result = {
        "path": str(normalized_absolute_path(item["path"], f"{label}.path")),
        "sha256": _digest(item["sha256"], f"{label}.sha256"),
    }
    if identity:
        result["identity_sha256"] = _digest(item["identity_sha256"], f"{label}.identity_sha256")
    return result


def _observed_tool(path_value: Any, label: str) -> dict[str, Any]:
    path = normalized_absolute_path(path_value, f"{label} path")
    try:
        info = path.lstat()
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except OSError as error:
        raise TrustedLauncherError(f"cannot inspect {label}: {error}") from error
    try:
        opened = os.fstat(descriptor)
        mode = stat.S_IMODE(opened.st_mode)
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or _identity(info) != _identity(opened)
            or opened.st_nlink != 1
            or opened.st_uid != 0
            or not mode & 0o100
            or mode & 0o022
        ):
            raise TrustedLauncherError(f"{label} is not a stable trusted executable")
        digest = _hash_fd(descriptor, opened.st_size, MAX_TOOL_BYTES, label)
        if _identity(os.fstat(descriptor)) != _identity(opened) or _identity(path.lstat()) != _identity(opened):
            raise TrustedLauncherError(f"{label} changed while hashed")
        _trusted_ancestors(path)
        return {
            "path": str(path),
            "sha256": digest,
            "byte_count": opened.st_size,
            "uid": 0,
            "mode": f"{mode:04o}",
        }
    finally:
        os.close(descriptor)


def _normalize_mapping(value: Any, label: str) -> dict[str, str]:
    item = _exact(value, label, {"name", "image_relative_path", "sandbox_path", "role"})
    name = _bounded_text(item["name"], f"{label}.name", 64)
    role = _bounded_text(item["role"], f"{label}.role", 64)
    if not NAME_RE.fullmatch(name) or not NAME_RE.fullmatch(role):
        raise TrustedLauncherError(f"{label} name/role is invalid")
    relative = normalized_relative_path(item["image_relative_path"], f"{label}.image_relative_path")
    sandbox = normalized_absolute_path(item["sandbox_path"], f"{label}.sandbox_path")
    sandbox_pure = PurePosixPath(str(sandbox))
    if IMAGE_MAPPING_PREFIX not in sandbox_pure.parents:
        raise TrustedLauncherError(f"{label} must remain below {IMAGE_MAPPING_PREFIX}")
    return {
        "name": name,
        "image_relative_path": str(relative),
        "sandbox_path": str(sandbox),
        "role": role,
    }


def _normalize_host_abi_loader_search_paths(
    value: Any, label: str
) -> list[str]:
    if not isinstance(value, list) or len(value) > 64:
        raise TrustedLauncherError(f"{label} is invalid")
    paths: list[str] = []
    for entry in value:
        if (
            not isinstance(entry, str)
            or len(entry.encode("utf-8")) > 4096
            or any(character in entry for character in ("\x00", "\r", "\n"))
        ):
            raise TrustedLauncherError(f"{label} is invalid")
        if entry == "$ORIGIN":
            paths.append(entry)
            continue
        if not entry.startswith("$ORIGIN/"):
            raise TrustedLauncherError(
                f"{label} contains an unsupported loader substitution or path"
            )
        suffix_text = entry.removeprefix("$ORIGIN/")
        suffix = PurePosixPath(suffix_text)
        if (
            not suffix_text
            or suffix.is_absolute()
            or str(suffix) != suffix_text
            or any(
                part in {"", "."}
                or (part != ".." and not SAFE_COMPONENT_RE.fullmatch(part))
                for part in suffix.parts
            )
        ):
            raise TrustedLauncherError(
                f"{label} contains a noncanonical $ORIGIN path"
            )
        paths.append(entry)
    return paths


def _normalize_host_abi_elf(
    value: Any, label: str, *, allow_origin_needed: bool = False
) -> dict[str, Any]:
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
    if (
        item["elf_class"] != 64
        or item["endianness"] != "little"
        or item["machine"] != EM_X86_64
        or item["elf_type"] not in {2, 3}
    ):
        raise TrustedLauncherError(f"{label} has an unsupported architecture")
    soname = item["soname"]
    if soname is not None and (
        not isinstance(soname, str) or not HOST_ABI_SONAME_RE.fullmatch(soname)
    ):
        raise TrustedLauncherError(f"{label}.soname is invalid")
    needed = item["needed"]
    if (
        not isinstance(needed, list)
        or needed != sorted(needed)
        or len(needed) > MAX_HOST_ABI_DEPENDENCIES
        or len(set(needed)) != len(needed)
        or any(
            not isinstance(name, str)
            or not (
                HOST_ABI_SONAME_RE.fullmatch(name)
                or (allow_origin_needed and HOST_ABI_ORIGIN_NEEDED_RE.fullmatch(name))
            )
            for name in needed
        )
    ):
        raise TrustedLauncherError(f"{label}.needed is invalid")
    interpreter = item["interpreter"]
    if interpreter is not None:
        interpreter = str(normalized_absolute_path(interpreter, f"{label}.interpreter"))
        if interpreter != "/lib64/ld-linux-x86-64.so.2":
            raise TrustedLauncherError(
                f"{label}.interpreter is outside the exact sandbox ABI"
            )
    loader_paths: dict[str, list[str]] = {}
    for field in ("rpath", "runpath"):
        loader_paths[field] = _normalize_host_abi_loader_search_paths(
            item[field], f"{label}.{field}"
        )
    if loader_paths["rpath"] and loader_paths["runpath"]:
        raise TrustedLauncherError(
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


def _canonical_host_abi_image_load_roots(
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
        raise TrustedLauncherError(
            "host ABI consumer scan must contain the one exact Python executable"
        )
    missing_dlopen = sorted(HOST_ABI_EXPLICIT_IMAGE_DLOPEN_ROOTS - set(by_image))
    if missing_dlopen:
        raise TrustedLauncherError(
            f"host ABI consumer scan omits explicit image dlopen roots: {missing_dlopen}"
        )
    roots = set(python_rows) | set(HOST_ABI_EXPLICIT_IMAGE_DLOPEN_ROOTS)
    for row in consumers:
        path = PurePosixPath(row["image_relative_path"])
        parent = str(path.parent)
        if (
            HOST_ABI_PYTHON_SITE_PACKAGES in path.parents
            and path.name.endswith(".so")
            and parent not in HOST_ABI_PRIVATE_IMAGE_LIBRARY_DIRECTORIES
            and parent not in HOST_ABI_GLOBAL_IMAGE_LIBRARY_DIRECTORIES
        ):
            roots.add(str(path))
    return sorted(roots)


def _expanded_host_abi_origin_directories(
    row: dict[str, Any], field: str
) -> frozenset[str]:
    parent = PurePosixPath(row["image_relative_path"]).parent
    directories: set[str] = set()
    for search in row["elf"][field]:
        if search == "$ORIGIN":
            expanded = str(parent)
        elif search.startswith("$ORIGIN/"):
            expanded = posixpath.normpath(
                str(parent / search.removeprefix("$ORIGIN/"))
            )
        else:
            # Host paths, empty components, and unsupported substitutions do
            # not confer projected-provider authority in this closed lane.
            continue
        directories.add(
            str(
                normalized_relative_path(
                    expanded,
                    f"consumer {row['image_relative_path']} {field} directory",
                )
            )
        )
    return frozenset(directories)


def _host_abi_external_sonames(
    consumers: Sequence[dict[str, Any]],
    runtime_loaded: Sequence[str],
    load_roots: Sequence[str],
) -> list[str]:
    by_image = {row["image_relative_path"]: row for row in consumers}
    providers: dict[str, set[str]] = {}
    for row in consumers:
        name = PurePosixPath(row["image_relative_path"]).name
        providers.setdefault(name, set()).add(row["image_relative_path"])
    if list(load_roots) != sorted(load_roots) or len(set(load_roots)) != len(
        load_roots
    ):
        raise TrustedLauncherError("host ABI consumer load roots are noncanonical")
    if any(path not in by_image for path in load_roots):
        raise TrustedLauncherError("host ABI consumer load root is absent")

    external = set(runtime_loaded)
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
        if len(visited) > MAX_HOST_ABI_RESOLUTION_STATES:
            raise TrustedLauncherError(
                "host ABI consumer dependency contexts exceed their bound"
            )
        row = by_image[path]
        parent = PurePosixPath(path).parent
        own_rpath = _expanded_host_abi_origin_directories(row, "rpath")
        own_runpath = _expanded_host_abi_origin_directories(row, "runpath")
        propagated_rpath = frozenset(set(inherited_rpath) | set(own_rpath))
        search_directories = (
            set(HOST_ABI_GLOBAL_IMAGE_LIBRARY_DIRECTORIES)
            | set(inherited_rpath)
            | set(own_rpath)
            | set(own_runpath)
        )
        for needed in row["elf"]["needed"]:
            provider: str | None = None
            if needed.startswith("$ORIGIN/"):
                resolved = normalized_relative_path(
                    posixpath.normpath(str(parent / needed.removeprefix("$ORIGIN/"))),
                    f"consumer {row['image_relative_path']} origin dependency",
                )
                if str(resolved) not in by_image:
                    raise TrustedLauncherError(
                        f"execution image lacks $ORIGIN provider {resolved}"
                    )
                provider = str(resolved)
            else:
                reachable = sorted(
                    candidate
                    for candidate in providers.get(needed, set())
                    if str(PurePosixPath(candidate).parent) in search_directories
                )
                if len(reachable) > 1:
                    raise TrustedLauncherError(
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


def _production_runtime_loaded_sonames(driver_version: str) -> list[str]:
    version = _bounded_text(driver_version, "host ABI NVIDIA driver version", 64)
    if not HOST_ABI_VERSION_RE.fullmatch(version):
        raise TrustedLauncherError("host ABI NVIDIA driver version is invalid")
    return sorted(
        {
            "ld-linux-x86-64.so.2",
            "libcuda.so.1",
            f"libnvidia-gpucomp.so.{version}",
            "libnvidia-ml.so.1",
            "libnvidia-nvvm.so.4",
            "libnvidia-nvvm70.so.4",
            "libnvidia-ptxjitcompiler.so.1",
        }
    )


def _normalize_host_abi_consumer_scan(value: Any) -> dict[str, Any]:
    item = _exact(
        value,
        "host ABI consumer scan",
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
    consumers_value = item["consumers"]
    if not isinstance(consumers_value, list) or not 1 <= len(consumers_value) <= MAX_HOST_ABI_CONSUMERS:
        raise TrustedLauncherError("host ABI consumer ELF inventory is outside its bound")
    consumers: list[dict[str, Any]] = []
    for ordinal, value_row in enumerate(consumers_value, 1):
        row = _exact(
            value_row,
            f"host ABI consumer {ordinal}",
            {"image_relative_path", "sha256", "byte_count", "elf"},
        )
        elf = _normalize_host_abi_elf(
            row["elf"],
            f"host ABI consumer {ordinal} ELF",
            allow_origin_needed=True,
        )
        if elf["interpreter"] not in {None, "/lib64/ld-linux-x86-64.so.2"}:
            raise TrustedLauncherError(
                f"host ABI consumer {ordinal} uses an unsupported interpreter"
            )
        consumers.append(
            {
                "image_relative_path": str(
                    normalized_relative_path(
                        row["image_relative_path"],
                        f"host ABI consumer {ordinal} image path",
                    )
                ),
                "sha256": _digest(
                    row["sha256"], f"host ABI consumer {ordinal} SHA-256"
                ),
                "byte_count": _integer(
                    row["byte_count"],
                    f"host ABI consumer {ordinal} byte_count",
                    64,
                    MAX_HOST_ABI_PROJECTION_BYTES,
                ),
                "elf": elf,
            }
        )
    if (
        consumers != sorted(consumers, key=lambda row: row["image_relative_path"])
        or len({row["image_relative_path"] for row in consumers}) != len(consumers)
    ):
        raise TrustedLauncherError("host ABI consumer inventory is noncanonical")
    regular_count = _integer(
        item["source_tree_regular_file_count"],
        "host ABI consumer source-tree file count",
        1,
        100_000,
    )
    if item["elf_file_count"] != len(consumers) or len(consumers) > regular_count:
        raise TrustedLauncherError("host ABI consumer counts are inconsistent")
    load_roots = item["load_roots"]
    if (
        not isinstance(load_roots, list)
        or load_roots != sorted(load_roots)
        or len(set(load_roots)) != len(load_roots)
        or any(
            not isinstance(path, str)
            or str(normalized_relative_path(path, "host ABI consumer load root"))
            != path
            for path in load_roots
        )
        or load_roots != _canonical_host_abi_image_load_roots(consumers)
    ):
        raise TrustedLauncherError(
            "host ABI consumer load roots differ from exact image policy"
        )
    runtime_loaded = item["runtime_loaded_sonames"]
    if (
        not isinstance(runtime_loaded, list)
        or not runtime_loaded
        or runtime_loaded != sorted(runtime_loaded)
        or len(set(runtime_loaded)) != len(runtime_loaded)
        or any(
            not isinstance(name, str) or not HOST_ABI_SONAME_RE.fullmatch(name)
            for name in runtime_loaded
        )
    ):
        raise TrustedLauncherError("host ABI runtime-loaded roots are invalid")
    external = _host_abi_external_sonames(consumers, runtime_loaded, load_roots)
    if item["external_sonames"] != external:
        raise TrustedLauncherError("host ABI external SONAME set is inconsistent")
    core = {
        "execution_image_identity_sha256": _digest(
            item["execution_image_identity_sha256"],
            "host ABI consumer execution-image identity",
        ),
        "source_tree_identity_sha256": _digest(
            item["source_tree_identity_sha256"],
            "host ABI consumer source-tree identity",
        ),
        "source_tree_regular_file_count": regular_count,
        "elf_file_count": len(consumers),
        "consumers": consumers,
        "load_roots": load_roots,
        "runtime_loaded_sonames": runtime_loaded,
        "external_sonames": external,
    }
    expected = {**core, "identity_sha256": sha256_bytes(canonical_bytes(core))}
    if item != expected:
        raise TrustedLauncherError("host ABI consumer scan identity is invalid")
    return expected


def validate_host_abi_manifest(value: Any) -> dict[str, Any]:
    item = _exact(
        value,
        "host ABI manifest",
        {
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
        },
    )
    if (
        item["kind"] != HOST_ABI_KIND
        or item["schema_version"] != 1
        or item["implementation_version"] != "0.1.0"
        or item["policy"] != HOST_ABI_POLICY
    ):
        raise TrustedLauncherError("host ABI manifest header/policy is unsupported")
    platform_value = _exact(
        item["platform"],
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
    platform = {
        "sysname": _bounded_text(platform_value["sysname"], "host ABI sysname", 64),
        "release": _bounded_text(platform_value["release"], "host ABI release", 256),
        "version": _bounded_text(platform_value["version"], "host ABI kernel version", 1024),
        "machine": _bounded_text(platform_value["machine"], "host ABI machine", 64),
        "nvidia_driver_version": _bounded_text(
            platform_value["nvidia_driver_version"], "host ABI driver", 64
        ),
        "nvidia_kernel_module_version": _bounded_text(
            platform_value["nvidia_kernel_module_version"],
            "host ABI kernel-module version",
            64,
        ),
        "nvidia_kernel_module_report_sha256": _digest(
            platform_value["nvidia_kernel_module_report_sha256"],
            "host ABI kernel-module report SHA-256",
        ),
        "nvidia_kernel_module_report_byte_count": _integer(
            platform_value["nvidia_kernel_module_report_byte_count"],
            "host ABI kernel-module report byte_count",
            1,
            MAX_NVIDIA_KERNEL_REPORT_BYTES,
        ),
    }
    if (
        platform["sysname"] != "Linux"
        or platform["machine"] != "x86_64"
        or not HOST_ABI_VERSION_RE.fullmatch(platform["nvidia_driver_version"])
        or platform["nvidia_kernel_module_version"]
        != platform["nvidia_driver_version"]
    ):
        raise TrustedLauncherError("host ABI platform is unsupported")
    consumer_scan = _normalize_host_abi_consumer_scan(item["consumer_scan"])
    if consumer_scan["runtime_loaded_sonames"] != _production_runtime_loaded_sonames(
        platform["nvidia_driver_version"]
    ):
        raise TrustedLauncherError("host ABI runtime-loaded roots are not exact")
    roots = item["library_roots"]
    if roots != ["/usr/lib64"]:
        raise TrustedLauncherError("host ABI production library root is not exact")
    requested_value = item["root_libraries"]
    if not isinstance(requested_value, list) or not requested_value:
        raise TrustedLauncherError("host ABI root libraries are absent")
    requested = [
        str(normalized_absolute_path(path, "host ABI root library"))
        for path in requested_value
    ]
    if (
        requested != sorted(requested)
        or len(set(requested)) != len(requested)
        or any(Path(path).parent != Path("/usr/lib64") for path in requested)
        or {Path(path).name for path in requested}
        != set(consumer_scan["external_sonames"])
    ):
        raise TrustedLauncherError("host ABI root library set is noncanonical")
    owner = _exact(item["required_owner"], "host ABI owner", {"uid", "gid"})
    if owner != {"uid": 0, "gid": 0}:
        raise TrustedLauncherError("host ABI production owner is not root")
    values = item["libraries"]
    if not isinstance(values, list) or not 1 <= len(values) <= MAX_HOST_ABI_LIBRARIES:
        raise TrustedLauncherError("host ABI library closure is outside its bound")
    libraries: list[dict[str, Any]] = []
    for ordinal, value_row in enumerate(values, 1):
        row = _exact(
            value_row,
            f"host ABI library {ordinal}",
            {
                "sandbox_path",
                "source_path",
                "sha256",
                "byte_count",
                "uid",
                "gid",
                "mode",
                "aliases",
                "elf",
            },
        )
        sandbox_path = normalized_absolute_path(
            row["sandbox_path"], f"host ABI library {ordinal} sandbox path"
        )
        source_path = normalized_absolute_path(
            row["source_path"], f"host ABI library {ordinal} source path"
        )
        if sandbox_path.parent != Path("/usr/lib64") or source_path.parent != Path("/usr/lib64"):
            raise TrustedLauncherError("host ABI libraries must be direct root children")
        mode = _bounded_text(row["mode"], f"host ABI library {ordinal} mode", 4)
        if (
            not MODE_RE.fullmatch(mode)
            or int(mode, 8) & 0o7022
            or not int(mode, 8) & 0o444
            or row["uid"] != 0
            or row["gid"] != 0
        ):
            raise TrustedLauncherError(f"host ABI library {ordinal} metadata is unsafe")
        aliases_value = row["aliases"]
        if not isinstance(aliases_value, list) or len(aliases_value) > MAX_HOST_ABI_ALIAS_DEPTH:
            raise TrustedLauncherError(f"host ABI library {ordinal} aliases are invalid")
        aliases: list[dict[str, Any]] = []
        cursor = sandbox_path
        visited_alias_paths: set[Path] = set()
        for alias_ordinal, alias_value in enumerate(aliases_value, 1):
            if cursor in visited_alias_paths:
                raise TrustedLauncherError(
                    f"host ABI library {ordinal} alias chain cycles"
                )
            visited_alias_paths.add(cursor)
            alias = _exact(
                alias_value,
                f"host ABI library {ordinal} alias {alias_ordinal}",
                {"path", "target", "uid", "gid", "mode"},
            )
            alias_path = normalized_absolute_path(
                alias["path"], f"host ABI library {ordinal} alias path"
            )
            target = _bounded_text(
                alias["target"], f"host ABI library {ordinal} alias target", 4096
            )
            alias_mode = _bounded_text(
                alias["mode"], f"host ABI library {ordinal} alias mode", 4
            )
            if (
                alias_path != cursor
                or alias_path.parent != Path("/usr/lib64")
                or not MODE_RE.fullmatch(alias_mode)
                or alias["uid"] != 0
                or alias["gid"] != 0
            ):
                raise TrustedLauncherError(f"host ABI library {ordinal} alias is unsafe")
            aliases.append(
                {
                    "path": str(alias_path),
                    "target": target,
                    "uid": 0,
                    "gid": 0,
                    "mode": alias_mode,
                }
            )
            cursor = Path(
                os.path.normpath(
                    str(Path(target) if os.path.isabs(target) else cursor.parent / target)
                )
            )
            if cursor.parent != Path("/usr/lib64"):
                raise TrustedLauncherError(f"host ABI library {ordinal} alias escapes")
        if cursor != source_path:
            raise TrustedLauncherError(f"host ABI library {ordinal} alias chain is incomplete")
        libraries.append(
            {
                "sandbox_path": str(sandbox_path),
                "source_path": str(source_path),
                "sha256": _digest(row["sha256"], f"host ABI library {ordinal} SHA-256"),
                "byte_count": _integer(
                    row["byte_count"],
                    f"host ABI library {ordinal} byte_count",
                    1,
                    MAX_HOST_ABI_LIBRARY_BYTES,
                ),
                "uid": 0,
                "gid": 0,
                "mode": mode,
                "aliases": aliases,
                "elf": _normalize_host_abi_elf(
                    row["elf"], f"host ABI library {ordinal} ELF"
                ),
            }
        )
    if (
        libraries != sorted(libraries, key=lambda row: row["sandbox_path"])
        or len({row["sandbox_path"] for row in libraries}) != len(libraries)
        or not set(requested) <= {row["sandbox_path"] for row in libraries}
    ):
        raise TrustedLauncherError("host ABI library closure is noncanonical")
    providers = {Path(row["sandbox_path"]).name: row["sandbox_path"] for row in libraries}
    if len(providers) != len(libraries):
        raise TrustedLauncherError("host ABI provider basenames are duplicated")
    edges = sorted(
        (
            {
                "consumer": row["sandbox_path"],
                "needed": needed,
                "provider": providers.get(needed, ""),
            }
            for row in libraries
            for needed in row["elf"]["needed"]
        ),
        key=lambda row: (row["consumer"], row["needed"], row["provider"]),
    )
    if any(not row["provider"] for row in edges) or item["dependency_edges"] != edges:
        raise TrustedLauncherError("host ABI dependency graph is incomplete")
    by_sandbox = {row["sandbox_path"]: row for row in libraries}
    reachable = set(requested)
    pending = list(requested)
    while pending:
        consumer = pending.pop()
        for needed in by_sandbox[consumer]["elf"]["needed"]:
            provider = providers[needed]
            if provider not in reachable:
                reachable.add(provider)
                pending.append(provider)
    if reachable != set(by_sandbox):
        raise TrustedLauncherError(
            "host ABI library closure contains disconnected extra objects"
        )
    core = {
        "kind": HOST_ABI_KIND,
        "schema_version": 1,
        "implementation_version": "0.1.0",
        "platform": platform,
        "consumer_scan": consumer_scan,
        "library_roots": ["/usr/lib64"],
        "root_libraries": requested,
        "required_owner": {"uid": 0, "gid": 0},
        "libraries": libraries,
        "dependency_edges": edges,
        "policy": dict(HOST_ABI_POLICY),
    }
    identity = sha256_bytes(canonical_bytes(core))
    expected = {
        **core,
        "identity_sha256": identity,
        "manifest_id": f"gpuhostabi_{identity[:32]}",
    }
    if item != expected:
        raise TrustedLauncherError("host ABI manifest identity is invalid")
    return expected


def _host_abi_c_string(table: bytes, offset: int, label: str) -> str:
    if not 0 <= offset < len(table):
        raise TrustedLauncherError(f"{label} string offset is outside DT_STRTAB")
    end = table.find(b"\x00", offset)
    if end < 0 or end - offset > 4096:
        raise TrustedLauncherError(f"{label} string is unterminated or oversized")
    try:
        return table[offset:end].decode("ascii")
    except UnicodeDecodeError as error:
        raise TrustedLauncherError(f"{label} string is not ASCII") from error


def _parse_host_abi_elf(body: bytes, label: str) -> dict[str, Any]:
    """Reparse the admitted ELF subset without executing the host object."""

    if len(body) < 64 or body[:4] != b"\x7fELF":
        raise TrustedLauncherError(f"{label} is not ELF")
    ident = body[:16]
    if ident[4] != 2 or ident[5] != 1 or ident[6] != 1:
        raise TrustedLauncherError(f"{label} is not little-endian ELF64")
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
        raise TrustedLauncherError(f"{label} has a truncated ELF header") from error
    if (
        machine != EM_X86_64
        or version != 1
        or elf_type not in {2, 3}
        or header_size != 64
        or program_entry_size < 56
        or not 1 <= program_count <= 1024
        or program_offset + program_entry_size * program_count > len(body)
    ):
        raise TrustedLauncherError(f"{label} has an unsupported ELF header")
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
                _memory_size,
                _alignment,
            ) = struct.unpack_from("<IIQQQQQQ", body, offset)
        except struct.error as error:
            raise TrustedLauncherError(f"{label} has a truncated program header") from error
        if segment_type == PT_LOAD and file_size > _memory_size:
            raise TrustedLauncherError(
                f"{label} has a segment with file size larger than memory size"
            )
        if file_offset + file_size > len(body):
            raise TrustedLauncherError(f"{label} has a segment outside the file")
        if segment_type == PT_LOAD:
            loads.append((virtual_address, file_size, file_offset))
        elif segment_type == PT_DYNAMIC:
            if dynamic is not None:
                raise TrustedLauncherError(f"{label} has multiple PT_DYNAMIC segments")
            dynamic = (file_offset, file_size)
        elif segment_type == PT_INTERP:
            if interpreter is not None:
                raise TrustedLauncherError(f"{label} has multiple PT_INTERP segments")
            raw = body[file_offset : file_offset + file_size]
            if not raw.endswith(b"\x00") or raw.count(b"\x00") != 1:
                raise TrustedLauncherError(f"{label} has an invalid PT_INTERP")
            try:
                decoded = raw[:-1].decode("ascii")
            except UnicodeDecodeError as error:
                raise TrustedLauncherError(f"{label} PT_INTERP is not ASCII") from error
            interpreter = str(
                normalized_absolute_path(decoded, f"{label} PT_INTERP")
            )
    dynamic_values: dict[int, list[int]] = {}
    if dynamic is not None:
        offset, size = dynamic
        if size % 16 or size // 16 > 65536:
            raise TrustedLauncherError(f"{label} has an invalid PT_DYNAMIC size")
        terminated = False
        for cursor in range(offset, offset + size, 16):
            tag, value = struct.unpack_from("<qQ", body, cursor)
            if tag == DT_NULL:
                terminated = True
                break
            if tag in {
                DT_NEEDED,
                DT_STRTAB,
                DT_STRSZ,
                DT_SONAME,
                DT_RPATH,
                DT_RUNPATH,
            }:
                dynamic_values.setdefault(tag, []).append(value)
        if not terminated:
            raise TrustedLauncherError(f"{label} dynamic table lacks DT_NULL")
    needed: list[str] = []
    soname: str | None = None
    rpath: list[str] = []
    runpath: list[str] = []
    if any(
        dynamic_values.get(tag)
        for tag in (DT_NEEDED, DT_SONAME, DT_RPATH, DT_RUNPATH)
    ):
        if (
            len(dynamic_values.get(DT_STRTAB, [])) != 1
            or len(dynamic_values.get(DT_STRSZ, [])) != 1
        ):
            raise TrustedLauncherError(f"{label} lacks one dynamic string table")
        address = dynamic_values[DT_STRTAB][0]
        size = dynamic_values[DT_STRSZ][0]
        if not 1 <= size <= len(body):
            raise TrustedLauncherError(f"{label} DT_STRSZ is outside its bound")
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
            raise TrustedLauncherError(f"{label} DT_STRTAB is not file-backed")
        table = body[table_offset : table_offset + size]
        needed = [
            _host_abi_c_string(table, value, f"{label} DT_NEEDED")
            for value in dynamic_values.get(DT_NEEDED, [])
        ]
        if (
            len(needed) > MAX_HOST_ABI_DEPENDENCIES
            or len(set(needed)) != len(needed)
            or any(not HOST_ABI_SONAME_RE.fullmatch(value) for value in needed)
        ):
            raise TrustedLauncherError(f"{label} DT_NEEDED is invalid")
        sonames = [
            _host_abi_c_string(table, value, f"{label} DT_SONAME")
            for value in dynamic_values.get(DT_SONAME, [])
        ]
        if len(sonames) > 1 or (
            sonames and not HOST_ABI_SONAME_RE.fullmatch(sonames[0])
        ):
            raise TrustedLauncherError(f"{label} DT_SONAME is invalid")
        soname = sonames[0] if sonames else None
        rpath_values = dynamic_values.get(DT_RPATH, [])
        runpath_values = dynamic_values.get(DT_RUNPATH, [])
        if len(rpath_values) > 1 or len(runpath_values) > 1:
            raise TrustedLauncherError(f"{label} has multiple loader search paths")
        if rpath_values and runpath_values:
            raise TrustedLauncherError(
                f"{label} contains both DT_RPATH and DT_RUNPATH"
            )
        for values, destination, tag_label in (
            (rpath_values, rpath, "DT_RPATH"),
            (runpath_values, runpath, "DT_RUNPATH"),
        ):
            if not values:
                continue
            raw_path = _host_abi_c_string(
                table, values[0], f"{label} {tag_label}"
            )
            # An explicitly present but empty tag means a current-working-
            # directory search component. Preserve it as [""] so canonical
            # manifest normalization rejects it; only an absent tag is [].
            destination.extend(raw_path.split(":"))
            if len(destination) > 64 or any(
                len(value.encode("utf-8")) > 4096 or "\x00" in value
                for value in destination
            ):
                raise TrustedLauncherError(f"{label} loader search path is invalid")
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


def observe_host_abi_platform(driver_version: str) -> dict[str, Any]:
    driver = _bounded_text(driver_version, "host ABI observed driver", 64)
    if not HOST_ABI_VERSION_RE.fullmatch(driver):
        raise TrustedLauncherError("host ABI observed driver version is invalid")
    report_path = Path("/proc/driver/nvidia/version")
    try:
        descriptor = os.open(
            report_path,
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as error:
        raise TrustedLauncherError(
            f"cannot read NVIDIA kernel-module report: {error}"
        ) from error
    try:
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, 8192)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_NVIDIA_KERNEL_REPORT_BYTES:
                raise TrustedLauncherError(
                    "NVIDIA kernel-module report exceeds its bound"
                )
            chunks.append(chunk)
        report = b"".join(chunks)
    finally:
        os.close(descriptor)
    if not report:
        raise TrustedLauncherError("NVIDIA kernel-module report is empty")
    try:
        report_text = report.decode("utf-8")
    except UnicodeDecodeError as error:
        raise TrustedLauncherError(
            "NVIDIA kernel-module report is not UTF-8"
        ) from error
    match = re.search(
        r"NVRM version:.*?\b([0-9]+(?:\.[0-9]+){2,3})\b",
        report_text,
        re.DOTALL,
    )
    if match is None or match.group(1) != driver:
        raise TrustedLauncherError("NVIDIA user/kernel driver versions differ")
    observed = os.uname()
    return {
        "sysname": observed.sysname,
        "release": observed.release,
        "version": observed.version,
        "machine": observed.machine,
        "nvidia_driver_version": driver,
        "nvidia_kernel_module_version": driver,
        "nvidia_kernel_module_report_sha256": sha256_bytes(report),
        "nvidia_kernel_module_report_byte_count": len(report),
    }


def _retain_host_abi_root() -> RetainedHostABIRoot:
    descriptors: list[int] = []
    observations: list[os.stat_result] = []
    try:
        descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        descriptors.append(descriptor)
        root_info = os.fstat(descriptor)
        observations.append(root_info)
        if (
            not stat.S_ISDIR(root_info.st_mode)
            or (root_info.st_uid, root_info.st_gid) != (0, 0)
            or stat.S_IMODE(root_info.st_mode) & 0o022
        ):
            raise TrustedLauncherError("host ABI filesystem root is unsafe")
        for component in ("usr", "lib64"):
            before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            child = os.open(
                component,
                os.O_RDONLY
                | os.O_DIRECTORY
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=descriptor,
            )
            descriptors.append(child)
            after = os.fstat(child)
            if (
                stat.S_ISLNK(before.st_mode)
                or not stat.S_ISDIR(before.st_mode)
                or _directory_identity(before) != _directory_identity(after)
                or (after.st_uid, after.st_gid) != (0, 0)
                or stat.S_IMODE(after.st_mode) & 0o022
            ):
                raise TrustedLauncherError(
                    f"host ABI directory component {component!r} is unsafe"
                )
            observations.append(after)
            descriptor = child
        retained = RetainedHostABIRoot(descriptors, observations)
        retained.verify()
        return retained
    except Exception:
        for descriptor in reversed(descriptors):
            with contextlib.suppress(OSError):
                os.close(descriptor)
        raise


def _resolve_host_abi_aliases(
    root: RetainedHostABIRoot, sandbox_path: Path
) -> tuple[Path, list[dict[str, Any]]]:
    current = sandbox_path
    aliases: list[dict[str, Any]] = []
    visited: set[Path] = set()
    for _ in range(MAX_HOST_ABI_ALIAS_DEPTH + 1):
        if current in visited or current.parent != Path("/usr/lib64"):
            raise TrustedLauncherError("host ABI alias chain cycles or escapes")
        visited.add(current)
        try:
            info = os.stat(
                current.name,
                dir_fd=root.descriptor,
                follow_symlinks=False,
            )
        except OSError as error:
            raise TrustedLauncherError(
                f"cannot inspect host ABI alias {current}: {error}"
            ) from error
        if not stat.S_ISLNK(info.st_mode):
            if not stat.S_ISREG(info.st_mode):
                raise TrustedLauncherError("host ABI alias does not resolve to a file")
            return current, aliases
        target = os.readlink(current.name, dir_fd=root.descriptor)
        after = os.stat(
            current.name,
            dir_fd=root.descriptor,
            follow_symlinks=False,
        )
        if _identity(info) != _identity(after):
            raise TrustedLauncherError("host ABI alias changed while read")
        if not target or "\x00" in target or len(os.fsencode(target)) > 4096:
            raise TrustedLauncherError("host ABI alias target is invalid")
        aliases.append(
            {
                "path": str(current),
                "target": target,
                "uid": info.st_uid,
                "gid": info.st_gid,
                "mode": f"{stat.S_IMODE(info.st_mode):04o}",
            }
        )
        current = Path(
            os.path.normpath(
                str(Path(target) if os.path.isabs(target) else current.parent / target)
            )
        )
    raise TrustedLauncherError("host ABI alias chain exceeds its bound")


def _retain_host_abi_library(
    expected: dict[str, Any], ordinal: int, root: RetainedHostABIRoot
) -> tuple[RetainedHostABILibrary, dict[str, Any]]:
    path = Path(expected["source_path"])
    label = f"host ABI library {ordinal}"
    try:
        lexical = os.stat(
            path.name, dir_fd=root.descriptor, follow_symlinks=False
        )
        descriptor = os.open(
            path.name,
            os.O_RDONLY
            | os.O_CLOEXEC
            | getattr(os, "O_NONBLOCK", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=root.descriptor,
        )
    except OSError as error:
        raise TrustedLauncherError(f"cannot retain {label}: {error}") from error
    try:
        opened = os.fstat(descriptor)
        if (
            stat.S_ISLNK(lexical.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or _identity(lexical) != _identity(opened)
            or opened.st_size != expected["byte_count"]
            or opened.st_uid != expected["uid"]
            or opened.st_gid != expected["gid"]
            or stat.S_IMODE(opened.st_mode) != int(expected["mode"], 8)
        ):
            raise TrustedLauncherError(f"{label} metadata changed")
        if not 1 <= opened.st_size <= MAX_HOST_ABI_LIBRARY_BYTES:
            raise TrustedLauncherError(f"{label} size is outside its bound")
        mapping = mmap.mmap(descriptor, 0, access=mmap.ACCESS_READ)
        try:
            digest = hashlib.sha256()
            view = memoryview(mapping)
            try:
                for offset in range(0, opened.st_size, 8 * 1024 * 1024):
                    digest.update(view[offset : offset + 8 * 1024 * 1024])
            finally:
                view.release()
            if digest.hexdigest() != expected["sha256"]:
                raise TrustedLauncherError(f"{label} SHA-256 changed")
            observed_elf = _parse_host_abi_elf(mapping, label)
        finally:
            mapping.close()
        if observed_elf != expected["elf"]:
            raise TrustedLauncherError(f"{label} ELF metadata changed")
        if _identity(os.fstat(descriptor)) != _identity(opened):
            raise TrustedLauncherError(f"{label} changed while replayed")
        retained = RetainedHostABILibrary(
            path,
            descriptor,
            opened,
            expected["sha256"],
            None,
            root,
            path.name,
        )
        retained.verify()
        os.lseek(descriptor, 0, os.SEEK_SET)
        return retained, observed_elf
    except Exception:
        os.close(descriptor)
        raise


def replay_host_abi_manifest(
    value: Any, *, observed_driver_version: str
) -> tuple[
    list[RetainedHostABILibrary],
    list[dict[str, Any]],
    dict[str, Any],
    RetainedHostABIRoot,
]:
    manifest = validate_host_abi_manifest(value)
    platform = observe_host_abi_platform(observed_driver_version)
    if platform != manifest["platform"]:
        raise TrustedLauncherError("booted platform differs from the host ABI manifest")
    root = _retain_host_abi_root()
    retained: list[RetainedHostABILibrary] = []
    bindings: list[dict[str, Any]] = []
    try:
        for ordinal, expected in enumerate(manifest["libraries"], 1):
            resolved, aliases = _resolve_host_abi_aliases(
                root,
                Path(expected["sandbox_path"])
            )
            if str(resolved) != expected["source_path"] or aliases != expected["aliases"]:
                raise TrustedLauncherError(
                    f"host ABI library {ordinal} alias chain changed"
                )
            source, _elf = _retain_host_abi_library(expected, ordinal, root)
            retained.append(source)
            bindings.append(
                {
                    "role": "host_abi_library",
                    "source_path": expected["source_path"],
                    "target": expected["sandbox_path"],
                    "sha256": expected["sha256"],
                    "byte_count": expected["byte_count"],
                    "uid": expected["uid"],
                    "gid": expected["gid"],
                    "mode": expected["mode"],
                }
            )
        bindings.sort(key=lambda row: row["target"])
        summary = {
            "identity_sha256": manifest["identity_sha256"],
            "manifest_id": manifest["manifest_id"],
            "platform_replayed": True,
            "libraries_replayed": True,
            "library_count": len(manifest["libraries"]),
            "binding_count": len(bindings),
        }
        root.verify()
        return retained, bindings, summary, root
    except Exception:
        for source in retained:
            source.close()
        root.close()
        raise


PROFILE_CORE_FIELDS = {
    "kind",
    "schema_version",
    "implementation_version",
    "launcher",
    "runtime_admission_install_path",
    "execution_image",
    "production_profile",
    "root_registration",
    "system_tools",
    "host_abi",
    "sandbox",
    "policy",
}


def validate_launcher_profile(value: Any) -> dict[str, Any]:
    item = _exact(value, "launcher profile", PROFILE_CORE_FIELDS | {"identity_sha256", "profile_id"})
    if (
        item["kind"] != PROFILE_KIND
        or item["schema_version"] != SCHEMA_VERSION
        or item["implementation_version"] != IMPLEMENTATION_VERSION
        or item["policy"] != POLICY
    ):
        raise TrustedLauncherError("launcher profile header/policy is unsupported")
    launcher = _reference(item["launcher"], "launcher")
    runtime_path = str(normalized_absolute_path(item["runtime_admission_install_path"], "runtime admission install path"))
    image = _exact(
        item["execution_image"],
        "execution_image",
        {"path", "sha256", "byte_count", "identity_sha256", "receipt_path", "receipt_sha256"},
    )
    normalized_image = {
        "path": str(normalized_absolute_path(image["path"], "execution image path")),
        "sha256": _digest(image["sha256"], "execution image SHA-256"),
        "byte_count": _integer(image["byte_count"], "execution image byte_count", 1, MAX_IMAGE_BYTES),
        "identity_sha256": _digest(image["identity_sha256"], "execution image identity"),
        "receipt_path": str(normalized_absolute_path(image["receipt_path"], "execution image receipt path")),
        "receipt_sha256": _digest(image["receipt_sha256"], "execution image receipt SHA-256"),
    }
    production_profile = _reference(item["production_profile"], "production_profile", identity=True)
    root = _exact(
        item["root_registration"],
        "root_registration",
        {"path", "sha256", "identity_sha256", "registration_id", "root_id"},
    )
    registration_id = _bounded_text(root["registration_id"], "root registration ID", 64)
    root_id = _bounded_text(root["root_id"], "root ID", 128)
    if not REGISTRATION_ID_RE.fullmatch(registration_id) or not ROOT_ID_RE.fullmatch(root_id):
        raise TrustedLauncherError("root registration identifiers are invalid")
    normalized_root = {
        **_reference(
            {"path": root["path"], "sha256": root["sha256"], "identity_sha256": root["identity_sha256"]},
            "root_registration",
            identity=True,
        ),
        "registration_id": registration_id,
        "root_id": root_id,
    }
    tools_value = item["system_tools"]
    if not isinstance(tools_value, dict) or set(tools_value) != set(SYSTEM_TOOL_PATHS):
        raise TrustedLauncherError("launcher system tool set is not exact")
    tools: dict[str, dict[str, Any]] = {}
    for name in sorted(SYSTEM_TOOL_PATHS):
        tool = _exact(tools_value[name], f"tool {name}", {"path", "sha256", "byte_count", "uid", "mode"})
        if tool["path"] != SYSTEM_TOOL_PATHS[name] or tool["uid"] != 0:
            raise TrustedLauncherError(f"tool {name} path/owner is unsupported")
        mode = _bounded_text(tool["mode"], f"tool {name} mode", 4)
        if not re.fullmatch(r"[0-7]{4}", mode) or not int(mode, 8) & 0o100 or int(mode, 8) & 0o022:
            raise TrustedLauncherError(f"tool {name} mode is unsafe")
        tools[name] = {
            "path": tool["path"],
            "sha256": _digest(tool["sha256"], f"tool {name} SHA-256"),
            "byte_count": _integer(tool["byte_count"], f"tool {name} byte_count", 1, MAX_TOOL_BYTES),
            "uid": 0,
            "mode": mode,
        }
    host_abi = validate_host_abi_manifest(item["host_abi"])
    if (
        host_abi["consumer_scan"]["execution_image_identity_sha256"]
        != normalized_image["identity_sha256"]
    ):
        raise TrustedLauncherError(
            "host ABI consumer scan binds a different execution image"
        )
    sandbox = _exact(
        item["sandbox"],
        "sandbox",
        {
            "image_mapping_prefix",
            "control_root",
            "input_root",
            "output_root",
            "state_root",
            "system_library_directories",
            "system_readonly_files",
            "gpu_control_devices",
        },
    )
    expected_fixed = {
        "image_mapping_prefix": str(IMAGE_MAPPING_PREFIX),
        "control_root": str(CONTROL_ROOT),
        "input_root": str(INPUT_ROOT),
        "output_root": str(OUTPUT_ROOT),
        "state_root": str(STATE_ROOT),
    }
    if any(sandbox[key] != expected for key, expected in expected_fixed.items()):
        raise TrustedLauncherError("launcher sandbox roots are unsupported")
    libraries = sandbox["system_library_directories"]
    readonly = sandbox["system_readonly_files"]
    devices = sandbox["gpu_control_devices"]
    if libraries != [] or readonly != []:
        raise TrustedLauncherError(
            "broad system-library and mutable loader-file allowlists must be empty"
        )
    if devices != ["/dev/nvidiactl", "/dev/nvidia-uvm", "/dev/nvidia-uvm-tools"]:
        raise TrustedLauncherError("GPU control-device allowlist is not exact")
    core = {
        "kind": PROFILE_KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "launcher": launcher,
        "runtime_admission_install_path": runtime_path,
        "execution_image": normalized_image,
        "production_profile": production_profile,
        "root_registration": normalized_root,
        "system_tools": tools,
        "host_abi": host_abi,
        "sandbox": {**expected_fixed, "system_library_directories": libraries, "system_readonly_files": readonly, "gpu_control_devices": devices},
        "policy": dict(POLICY),
    }
    identity = sha256_bytes(canonical_bytes(core))
    expected = {**core, "identity_sha256": identity, "profile_id": f"gpulaunchprofile_{identity[:32]}"}
    if item != expected:
        raise TrustedLauncherError("launcher profile is noncanonical or has an invalid identity")
    return expected


def _validate_semantic_identity(value: dict[str, Any], identity_key: str, id_key: str, prefix: str, label: str) -> None:
    identity = _digest(value.get(identity_key), f"{label} identity")
    identifier = value.get(id_key)
    core = {key: item for key, item in value.items() if key not in {identity_key, id_key}}
    observed = sha256_bytes(canonical_bytes(core))
    if identity != observed or identifier != f"{prefix}{identity[:32]}":
        raise TrustedLauncherError(f"{label} semantic identity is invalid")


def validate_production_profile(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TrustedLauncherError("production profile must be an object")
    if value.get("kind") != PRODUCTION_PROFILE_KIND or value.get("schema_version") != 2:
        raise TrustedLauncherError("production profile kind/version is unsupported")
    _validate_semantic_identity(value, "identity_sha256", "profile_id", "gpuprofile_", "production profile")
    hardware = value.get("hardware")
    if not isinstance(hardware, dict) or set(hardware) != {
        "gpu_uuid", "device_index", "compute_type", "minimum_driver_version", "minimum_compute_capability"
    }:
        raise TrustedLauncherError("production profile hardware is invalid")
    gpu_uuid = hardware["gpu_uuid"]
    if not isinstance(gpu_uuid, str) or not GPU_UUID_RE.fullmatch(gpu_uuid) or hardware["device_index"] != 0:
        raise TrustedLauncherError("production profile GPU binding is invalid")
    safety = value.get("safety")
    if not isinstance(safety, dict) or safety.get("network_access") is not False or safety.get("visibility") != "private":
        raise TrustedLauncherError("production profile safety is not private/offline")
    return value


def validate_root_registration(value: Any) -> dict[str, Any]:
    fields = {
        "kind", "schema_version", "root_id", "tier", "path", "filesystem", "owner",
        "predecessor", "historical_observation", "policy", "identity_sha256", "registration_id",
    }
    item = _exact(value, "root registration", fields)
    if item["kind"] != ROOT_KIND or item["schema_version"] != 1 or item["tier"] != "hot_main_drive":
        raise TrustedLauncherError("root registration kind/tier is unsupported")
    if item["policy"] != ROOT_POLICY:
        raise TrustedLauncherError("root registration policy is unsupported")
    if not isinstance(item["root_id"], str) or not ROOT_ID_RE.fullmatch(item["root_id"]):
        raise TrustedLauncherError("root ID is invalid")
    normalized_absolute_path(item["path"], "registered root path")
    filesystem = _exact(item["filesystem"], "root filesystem", {"type", "uuid"})
    try:
        parsed_uuid = uuid.UUID(filesystem["uuid"])
    except (TypeError, ValueError, AttributeError) as error:
        raise TrustedLauncherError("root filesystem UUID is invalid") from error
    if filesystem["type"] != "btrfs" or parsed_uuid.int == 0 or str(parsed_uuid) != filesystem["uuid"]:
        raise TrustedLauncherError("root filesystem binding is invalid")
    owner = _exact(item["owner"], "root owner", {"policy", "uid"})
    if owner["policy"] != "exact_uid" or owner["uid"] != os.geteuid():
        raise TrustedLauncherError("registered root owner differs from launcher user")
    _validate_semantic_identity(item, "identity_sha256", "registration_id", "gpurootreg_", "root registration")
    return item


@dataclass
class RetainedRoot:
    registration: dict[str, Any]
    descriptors: list[int]
    component_stats: list[tuple[int, str, int, os.stat_result]]
    filesystem_uuid: str

    @property
    def descriptor(self) -> int:
        return self.descriptors[-1]

    def verify(self) -> None:
        for parent_fd, name, descriptor, expected in self.component_stats:
            try:
                linked = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            except OSError as error:
                raise TrustedLauncherError("registered root path disappeared while retained") from error
            if (
                stat.S_ISLNK(linked.st_mode)
                or _directory_identity(linked) != _directory_identity(expected)
                or _directory_identity(os.fstat(descriptor)) != _directory_identity(expected)
            ):
                raise TrustedLauncherError("registered root changed while retained")
        if _btrfs_uuid(self.descriptor) != self.filesystem_uuid:
            raise TrustedLauncherError("registered root filesystem UUID changed")

    def close(self) -> None:
        for descriptor in reversed(self.descriptors):
            os.close(descriptor)
        self.descriptors.clear()

    def __enter__(self) -> "RetainedRoot":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


def _btrfs_uuid(descriptor: int) -> str:
    buffer = bytearray(BTRFS_FS_INFO_SIZE)
    try:
        fcntl.ioctl(descriptor, BTRFS_IOC_FS_INFO, buffer, True)
    except OSError as error:
        raise TrustedLauncherError(f"Btrfs FSID probe failed: {error}") from error
    raw = bytes(buffer[BTRFS_FSID_OFFSET : BTRFS_FSID_OFFSET + BTRFS_FSID_BYTES])
    if not any(raw):
        raise TrustedLauncherError("Btrfs FSID probe returned a zero UUID")
    return str(uuid.UUID(bytes=raw))


def retain_root(registration: dict[str, Any]) -> RetainedRoot:
    path = Path(registration["path"])
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_DIRECTORY
    descriptors: list[int] = []
    component_stats: list[tuple[int, str, int, os.stat_result]] = []
    owner_uid = registration["owner"]["uid"]
    try:
        parent = os.open("/", flags)
        descriptors.append(parent)
        for component in path.parts[1:]:
            inspected = os.stat(component, dir_fd=parent, follow_symlinks=False)
            mode = stat.S_IMODE(inspected.st_mode)
            if (
                stat.S_ISLNK(inspected.st_mode)
                or not stat.S_ISDIR(inspected.st_mode)
                or inspected.st_uid not in {0, owner_uid}
                or (inspected.st_uid == 0 and mode & 0o022)
                or (inspected.st_uid == owner_uid and mode & 0o002)
            ):
                raise TrustedLauncherError("registered root has an unsafe path component")
            child = os.open(component, flags, dir_fd=parent)
            opened = os.fstat(child)
            if _directory_identity(opened) != _directory_identity(inspected):
                os.close(child)
                raise TrustedLauncherError("registered root changed while opened")
            descriptors.append(child)
            component_stats.append((parent, component, child, opened))
            parent = child
        final = os.fstat(descriptors[-1])
        if final.st_uid != owner_uid or stat.S_IMODE(final.st_mode) & 0o022:
            raise TrustedLauncherError("registered root owner/mode is unsafe")
        observed = _btrfs_uuid(descriptors[-1])
        if observed != registration["filesystem"]["uuid"]:
            raise TrustedLauncherError("registered root is on the wrong Btrfs filesystem")
        retained = RetainedRoot(registration, descriptors, component_stats, observed)
        retained.verify()
        return retained
    except Exception:
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except OSError:
                pass
        raise


def validate_runtime_receipt(
    value: Any,
    *,
    mode: str,
    launcher_profile: dict[str, Any],
    launcher_profile_path: str,
    profile_sha256: str,
) -> dict[str, Any]:
    fields = {
        "kind", "schema_version", "implementation_version", "status", "root_registration",
        "root", "execution_image", "production_profile", "production_profile_file",
        "trusted_install", "runtime_candidate_identity_sha256", "runtime", "gates", "policy",
        "identity_sha256", "receipt_id",
    }
    item = _exact(value, "runtime admission receipt", fields)
    if (
        item["kind"] != RUNTIME_KIND
        or item["schema_version"] != 2
        or item["implementation_version"] != IMPLEMENTATION_VERSION
        or item["policy"] != RUNTIME_POLICY
    ):
        raise TrustedLauncherError("runtime admission header/policy is unsupported")
    if mode not in MODES:
        raise TrustedLauncherError("runtime launch mode is unsupported")
    expected_status = MODE_RUNTIME_STATUS[mode]
    if item["status"] != expected_status:
        raise TrustedLauncherError(f"{mode} launch requires a {expected_status} runtime receipt")
    _validate_semantic_identity(item, "identity_sha256", "receipt_id", "gpurtv2_", "runtime admission receipt")
    root_ref = item["root_registration"]
    root_binding = launcher_profile["root_registration"]
    if any(root_ref.get(key) != root_binding[key] for key in ("path", "sha256", "identity_sha256", "registration_id")):
        raise TrustedLauncherError("runtime root registration differs from launcher profile")
    if item["root"].get("root_id") != root_binding["root_id"] or item["root"].get("tier") != "hot_main_drive":
        raise TrustedLauncherError("runtime root identity/tier differs from launcher profile")
    profile_ref = item["production_profile_file"]
    expected_profile = launcher_profile["production_profile"]
    if any(profile_ref.get(key) != expected_profile[key] for key in ("path", "sha256", "identity_sha256")):
        raise TrustedLauncherError("runtime production profile differs from launcher profile")
    if item["production_profile"].get("identity_sha256") != expected_profile["identity_sha256"]:
        raise TrustedLauncherError("runtime embedded production profile identity differs")
    image = item["execution_image"]
    expected_image = launcher_profile["execution_image"]
    comparisons = {
        "receipt_path": "receipt_path",
        "receipt_sha256": "receipt_sha256",
        "identity_sha256": "identity_sha256",
    }
    if any(image.get(left) != expected_image[right] for left, right in comparisons.items()):
        raise TrustedLauncherError("runtime execution-image reference differs from launcher profile")
    image_file = image.get("image")
    if not isinstance(image_file, dict) or any(
        image_file.get(key) != expected_image[key] for key in ("path", "sha256", "byte_count")
    ):
        raise TrustedLauncherError("runtime execution-image bytes differ from launcher profile")
    mappings_value = image.get("logical_mappings")
    if not isinstance(mappings_value, list) or not 1 <= len(mappings_value) <= MAX_MAPPINGS:
        raise TrustedLauncherError("runtime execution mappings are invalid")
    mappings = [_normalize_mapping(row, f"runtime mapping {ordinal}") for ordinal, row in enumerate(mappings_value, 1)]
    if mappings != sorted(mappings, key=lambda row: row["name"]):
        raise TrustedLauncherError("runtime execution mappings are not sorted")
    if {row["name"] for row in mappings} != REQUIRED_MAPPING_NAMES:
        raise TrustedLauncherError("runtime execution mapping closure is not exact")
    observed_layout = {
        row["name"]: (row["sandbox_path"], row["role"]) for row in mappings
    }
    if observed_layout != REQUIRED_MAPPING_LAYOUT:
        raise TrustedLauncherError("runtime execution mapping layout is not exact")
    if len({row["sandbox_path"] for row in mappings}) != len(mappings):
        raise TrustedLauncherError("runtime sandbox mapping paths are duplicated")
    trusted = _exact(
        item["trusted_install"],
        "runtime trusted install",
        {
            "owner_uid",
            "launcher",
            "launcher_profile",
            "system_tools",
            "root_ownership_enforced",
        },
    )
    expected_owner = os.geteuid() if mode == MODE_LOCAL_PRIVATE else 0
    if mode == MODE_SYNTHETIC:
        expected_owner = trusted["owner_uid"]
        if expected_owner not in {0, os.geteuid()}:
            raise TrustedLauncherError("synthetic runtime trusted owner is unsupported")
    if (
        trusted["owner_uid"] != expected_owner
        or trusted["root_ownership_enforced"] is not (mode == MODE_PRODUCTION)
    ):
        raise TrustedLauncherError("runtime trusted-install ownership state differs")
    launcher = _exact(
        trusted["launcher"],
        "runtime trusted launcher",
        {"path", "sha256", "byte_count", "uid", "mode"},
    )
    launcher_profile_ref = _exact(
        trusted["launcher_profile"],
        "runtime trusted launcher profile",
        {"path", "sha256", "byte_count", "uid", "mode"},
    )
    if any(
        launcher[key] != launcher_profile["launcher"][key]
        for key in ("path", "sha256")
    ):
        raise TrustedLauncherError("runtime trusted launcher differs from installed profile")
    if mode == MODE_LOCAL_PRIVATE and (
        launcher.get("uid") != os.geteuid()
        or launcher.get("mode") != "0500"
        or launcher_profile_ref.get("uid") != os.geteuid()
        or launcher_profile_ref.get("mode") != "0400"
    ):
        raise TrustedLauncherError(
            "local-private runtime launcher/profile must be current-user mode 0500/0400"
        )
    if (
        not isinstance(launcher_profile_ref, dict)
        or launcher_profile_ref.get("path") != launcher_profile_path
        or launcher_profile_ref.get("sha256") != profile_sha256
    ):
        raise TrustedLauncherError("runtime launcher-profile digest differs from the installed profile")
    tools = trusted.get("system_tools") if isinstance(trusted, dict) else None
    if not isinstance(tools, list) or len(tools) != len(RUNTIME_TOOL_NAMES):
        raise TrustedLauncherError("runtime trusted tool set is not exact")
    launcher_tools = launcher_profile["system_tools"]
    normalized_tools: list[dict[str, Any]] = []
    for ordinal, value in enumerate(tools, 1):
        row = _exact(
            value,
            f"runtime trusted tool {ordinal}",
            {"name", "path", "sha256", "byte_count", "uid", "mode"},
        )
        name = row["name"]
        if name not in RUNTIME_TOOL_NAMES:
            raise TrustedLauncherError("runtime trusted tool name is unsupported")
        expected = {"name": name, **launcher_tools[name]}
        if row != expected:
            raise TrustedLauncherError(f"runtime tool {row['name']} differs from launcher profile")
        normalized_tools.append(row)
    if normalized_tools != sorted(normalized_tools, key=lambda row: row["name"]):
        raise TrustedLauncherError("runtime trusted tool set is not sorted")
    if {row["name"] for row in normalized_tools} != RUNTIME_TOOL_NAMES:
        raise TrustedLauncherError("runtime trusted tool set is not exact")
    candidate_identity = _digest(
        item["runtime_candidate_identity_sha256"],
        "runtime candidate closure identity",
    )
    gates = item["gates"]
    if not isinstance(gates, dict) or set(gates) != set(GATE_NAMES):
        raise TrustedLauncherError("runtime gate set is not exact")
    for name in GATE_NAMES:
        if gates[name] is None:
            if mode == MODE_PRODUCTION:
                raise TrustedLauncherError(
                    f"production runtime lacks passing admission gate {name}"
                )
            continue
        gate = _exact(
            gates[name],
            f"runtime gate {name}",
            {
                "path",
                "sha256",
                "byte_count",
                "identity_sha256",
                "gate_id",
                "status",
                "runtime_candidate_identity_sha256",
            },
        )
        _reference(
            {"path": gate["path"], "sha256": gate["sha256"]},
            f"runtime gate {name}",
        )
        _integer(
            gate["byte_count"], f"runtime gate {name} byte_count", 1, MAX_JSON_BYTES
        )
        gate_identity = _digest(
            gate["identity_sha256"], f"runtime gate {name} identity"
        )
        if (
            gate["status"] != "passed"
            or gate["gate_id"] != f"gpugate_{gate_identity[:32]}"
            or gate["runtime_candidate_identity_sha256"] != candidate_identity
        ):
            raise TrustedLauncherError(
                f"runtime gate {name} has inconsistent authority"
            )
    return {**item, "execution_image": {**image, "logical_mappings": mappings}}


def _execution_class(manifest: dict[str, Any]) -> str:
    value = manifest.get("execution_class")
    if value not in set(MODE_EXECUTION_CLASS.values()):
        raise TrustedLauncherError("batch execution_class is absent or unsupported")
    return value


def _hot_descendant(path: Path, hot_root: Path, label: str) -> None:
    if hot_root not in path.parents:
        raise TrustedLauncherError(f"{label} must be a strict descendant of the registered hot root")


def _validated_work_order_envelope(
    value: Any,
    *,
    ordinal: int,
    mode: str,
    manifest: dict[str, Any],
) -> dict[str, Any]:
    order = _exact(value, f"batch item {ordinal} v5 work order", WORK_ORDER_FIELDS)
    if (
        order["kind"] != WORK_ORDER_KIND
        or order["schema_version"] != WORK_ORDER_SCHEMA_VERSION
        or order["implementation_version"] != WORK_ORDER_IMPLEMENTATION_VERSION
    ):
        raise TrustedLauncherError(f"batch item {ordinal} is not an exact v5 work order")
    identity = _digest(order["identity_sha256"], f"batch item {ordinal} work-order identity")
    core = {
        key: item
        for key, item in order.items()
        if key not in {"identity_sha256", "work_order_id"}
    }
    if (
        sha256_bytes(canonical_bytes(core)) != identity
        or order["work_order_id"] != f"gpuasrwo5_{identity[:32]}"
    ):
        raise TrustedLauncherError(f"batch item {ordinal} work-order identity is invalid")
    expected_lineage = (
        "production_preprocess_v03"
        if mode in {MODE_PRODUCTION, MODE_LOCAL_PRIVATE}
        else "synthetic_canary"
    )
    lineage = order.get("source_lineage")
    if not isinstance(lineage, dict) or lineage.get("kind") != expected_lineage:
        raise TrustedLauncherError(
            f"batch item {ordinal} source lineage is not {expected_lineage}"
        )
    if mode == MODE_PRODUCTION:
        runtime = order.get("runtime_admission")
        if not isinstance(runtime, dict) or runtime.get("status") != "admitted":
            raise TrustedLauncherError(f"batch item {ordinal} runtime is not admitted")
    elif mode == MODE_LOCAL_PRIVATE:
        runtime = order.get("runtime_admission")
        if not isinstance(runtime, dict) or runtime.get("status") != "candidate":
            raise TrustedLauncherError(
                f"batch item {ordinal} runtime is not a candidate"
            )
    elif (
        lineage.get("contains_corpus_media") is not False
        or lineage.get("scope") != "purpose_built_synthetic_only"
        or lineage.get("corpus_authority") != "none"
    ):
        raise TrustedLauncherError(f"batch item {ordinal} synthetic scope is not exact")
    for common in ("hot_root", "production_profile", "runtime_admission"):
        if order.get(common) != manifest.get(common):
            raise TrustedLauncherError(
                f"batch item {ordinal} {common} differs from the batch common binding"
            )
    return order


def validate_batch_execution_class(manifest: Any, mode: str) -> str:
    if not isinstance(manifest, dict):
        raise TrustedLauncherError("batch manifest must be an object")
    observed = _execution_class(manifest)
    try:
        expected = MODE_EXECUTION_CLASS[mode]
    except KeyError as error:
        raise TrustedLauncherError("batch launch mode is unsupported") from error
    if observed != expected:
        raise TrustedLauncherError(f"{mode} launcher cannot execute {observed}")
    items = manifest.get("items")
    if not isinstance(items, list) or not 1 <= len(items) <= 32:
        raise TrustedLauncherError("batch item array is outside 1..32")
    # The bundled worker performs full v5 normalization.  The external launcher
    # still seals the complete envelope and all path-bearing security fields
    # before it opens or exposes a source byte.
    for ordinal, row in enumerate(items, 1):
        member = _exact(
            row,
            f"batch item {ordinal}",
            {"ordinal", "work_order", "input", "result"},
        )
        if member["ordinal"] != ordinal:
            raise TrustedLauncherError(f"batch item {ordinal} ordinal is invalid")
        order = _validated_work_order_envelope(
            member["work_order"], ordinal=ordinal, mode=mode, manifest=manifest
        )
        summary = _exact(
            member["input"],
            f"batch item {ordinal} input summary",
            {"sha256", "byte_count", "duration_ms"},
        )
        order_input = order.get("input")
        if not isinstance(order_input, dict) or summary != {
            "sha256": order_input.get("expected_sha256"),
            "byte_count": order_input.get("expected_byte_count"),
            "duration_ms": order_input.get("expected_duration_ms"),
        }:
            raise TrustedLauncherError(f"batch item {ordinal} input projection differs")
    return observed


def authorized_read_binding_specs(
    manifest: dict[str, Any], mode: str, hot_root: Path
) -> list[list[ReadBindingSpec]]:
    """Derive the only corpus/synthetic bytes the sandbox may read.

    This deliberately does not accept generic directory references.  Every row
    is a single digest-bound file from a semantically sealed v5 work order.
    """

    validate_batch_execution_class(manifest, mode)
    groups: list[list[ReadBindingSpec]] = []
    for ordinal, member in enumerate(manifest["items"], 1):
        order = member["work_order"]
        identity = order["identity_sha256"]
        input_item = _exact(
            order["input"],
            f"batch item {ordinal} input",
            {
                "path",
                "expected_sha256",
                "expected_byte_count",
                "expected_duration_ms",
                "media_id",
                "artifact_id",
                "parent_processing_run_id",
                "media_format",
                "sealed_mode",
                "timeline_offset_ms",
            },
        )
        input_path = normalized_absolute_path(
            input_item["path"], f"batch item {ordinal} input path"
        )
        _hot_descendant(input_path, hot_root, f"batch item {ordinal} input")
        input_mode_text = input_item["sealed_mode"]
        if input_mode_text == "0400":
            input_mode = 0o400
            input_owners = frozenset({(os.geteuid(), input_mode)})
        elif input_mode_text == "0444":
            input_mode = 0o444
            input_owners = frozenset({(os.geteuid(), input_mode)})
        else:
            raise TrustedLauncherError(
                f"batch item {ordinal} input sealed mode is unsupported"
            )
        group = [
            ReadBindingSpec(
                ordinal=ordinal,
                work_order_identity_sha256=identity,
                role="input_audio",
                path=input_path,
                sha256=_digest(
                    input_item["expected_sha256"],
                    f"batch item {ordinal} input SHA-256",
                ),
                expected_byte_count=_integer(
                    input_item["expected_byte_count"],
                    f"batch item {ordinal} input byte_count",
                    1,
                    2**30,
                ),
                declared_mode=input_mode,
                allowed_owner_modes=input_owners,
            )
        ]
        lineage = order["source_lineage"]
        references: list[tuple[str, Any, str, int | None]]
        if mode in {MODE_PRODUCTION, MODE_LOCAL_PRIVATE}:
            production_lineage = _exact(
                lineage,
                f"batch item {ordinal} production lineage",
                {
                    "kind",
                    "implementation_version",
                    "gpu_handoff",
                    "bundle_manifest",
                    "receipt",
                    "preprocess_result",
                    "handling",
                    "corpus_authority",
                },
            )
            handoff = _exact(
                production_lineage["gpu_handoff"],
                f"batch item {ordinal} GPU handoff",
                {
                    "manifest_path",
                    "manifest_sha256",
                    "queue_id",
                    "identity_sha256",
                    "member_id",
                    "member_identity_sha256",
                    "queue_ordinal",
                    "preprocess_ordinal",
                },
            )
            bundle = _exact(
                production_lineage["bundle_manifest"],
                f"batch item {ordinal} bundle manifest",
                {"path", "sha256", "bundle_id", "identity_sha256", "manifest_sha256"},
            )
            receipt = _exact(
                production_lineage["receipt"],
                f"batch item {ordinal} preprocess receipt",
                {"path", "sha256", "receipt_id", "receipt_sha256", "ordinal"},
            )
            result = _exact(
                production_lineage["preprocess_result"],
                f"batch item {ordinal} preprocess result",
                {"path", "sha256", "byte_count", "processing_run_id", "recipe_sha256"},
            )
            references = [
                ("gpu_handoff_manifest", handoff["manifest_path"], handoff["manifest_sha256"], None),
                ("bundle_manifest", bundle["path"], bundle["sha256"], None),
                ("preprocess_receipt", receipt["path"], receipt["sha256"], None),
                (
                    "preprocess_result",
                    result["path"],
                    result["sha256"],
                    _integer(
                        result["byte_count"],
                        f"batch item {ordinal} preprocess result byte_count",
                        1,
                        MAX_LINEAGE_BYTES,
                    ),
                ),
            ]
        else:
            synthetic = _exact(
                lineage,
                f"batch item {ordinal} synthetic lineage",
                {
                    "kind",
                    "fixture_manifest",
                    "fixture_case_id",
                    "contains_corpus_media",
                    "scope",
                    "corpus_authority",
                },
            )
            fixture = _exact(
                synthetic["fixture_manifest"],
                f"batch item {ordinal} fixture manifest",
                {"path", "sha256", "identity_sha256", "fixture_id"},
            )
            references = [("fixture_manifest", fixture["path"], fixture["sha256"], None)]
        for role, path_value, digest_value, byte_count in references:
            path = normalized_absolute_path(
                path_value, f"batch item {ordinal} {role} path"
            )
            _hot_descendant(path, hot_root, f"batch item {ordinal} {role}")
            group.append(
                ReadBindingSpec(
                    ordinal=ordinal,
                    work_order_identity_sha256=identity,
                    role=role,
                    path=path,
                    sha256=_digest(
                        digest_value, f"batch item {ordinal} {role} SHA-256"
                    ),
                    expected_byte_count=byte_count,
                    declared_mode=(0o400 if role != "preprocess_result" else 0),
                    allowed_owner_modes=(
                        frozenset({(os.geteuid(), 0o400)})
                        if role != "preprocess_result"
                        else frozenset(
                            {
                                (os.geteuid(), 0o400),
                                (os.geteuid(), 0o440),
                                (os.geteuid(), 0o444),
                            }
                        )
                    ),
                )
            )
        if not 2 <= len(group) <= MAX_ITEM_READ_BINDINGS:
            raise TrustedLauncherError(
                f"batch item {ordinal} read-binding closure is outside its bound"
            )
        group.sort(key=lambda row: (row.role, str(row.path)))
        groups.append(group)
    if sum(map(len, groups)) > MAX_TOTAL_READ_BINDINGS:
        raise TrustedLauncherError("batch read-binding closure exceeds its finite bound")
    return groups


def retain_authorized_read_bindings(
    groups: Sequence[Sequence[ReadBindingSpec]],
) -> tuple[list[RetainedFile], list[dict[str, Any]], dict[str, RetainedFile]]:
    """Retain/deduplicate exact work-order files and make attestation rows."""

    retained_by_path: dict[str, RetainedFile] = {}
    policy_by_path: dict[str, tuple[Any, ...]] = {}
    item_rows: list[dict[str, Any]] = []
    try:
        for expected_ordinal, group in enumerate(groups, 1):
            rows: list[dict[str, Any]] = []
            identity: str | None = None
            for spec in group:
                if spec.ordinal != expected_ordinal:
                    raise TrustedLauncherError("read-binding item order is invalid")
                if identity is None:
                    identity = spec.work_order_identity_sha256
                elif identity != spec.work_order_identity_sha256:
                    raise TrustedLauncherError("one item has multiple work-order identities")
                path_text = str(spec.path)
                observed_policy = (
                    spec.sha256,
                    spec.expected_byte_count,
                    spec.declared_mode,
                    spec.allowed_owner_modes,
                    spec.role,
                )
                previous = policy_by_path.get(path_text)
                if previous is not None and previous != observed_policy:
                    raise TrustedLauncherError(
                        f"read-binding path has conflicting authority: {path_text}"
                    )
                retained = retained_by_path.get(path_text)
                if retained is None:
                    retained = retain_file(
                        spec.path,
                        f"work-order {spec.ordinal} {spec.role}",
                        expected_sha256=spec.sha256,
                        maximum=(
                            spec.expected_byte_count
                            if spec.expected_byte_count is not None
                            else MAX_LINEAGE_BYTES
                        ),
                        allowed_owner_modes=set(spec.allowed_owner_modes),
                        keep_body=False,
                    )
                    if (
                        spec.expected_byte_count is not None
                        and retained.info.st_size != spec.expected_byte_count
                    ):
                        retained.close()
                        raise TrustedLauncherError(
                            f"work-order {spec.ordinal} {spec.role} byte count differs"
                        )
                    retained_by_path[path_text] = retained
                    policy_by_path[path_text] = observed_policy
                rows.append(RetainedReadBinding(spec, retained).attestation_row())
            item_rows.append(
                {
                    "ordinal": expected_ordinal,
                    "work_order_identity_sha256": _digest(
                        identity, f"batch item {expected_ordinal} work-order identity"
                    ),
                    "bindings": rows,
                }
            )
        return list(retained_by_path.values()), item_rows, retained_by_path
    except Exception:
        for retained in retained_by_path.values():
            retained.close()
        raise


def validate_lineage_preflight(
    value: Any,
    *,
    manifest: dict[str, Any],
    manifest_sha256: str,
    production_profile: dict[str, Any],
    root_registration: dict[str, Any],
) -> dict[str, Any]:
    item = _exact(
        value,
        "lineage preflight attestation",
        {
            "kind",
            "schema_version",
            "implementation_version",
            "batch",
            "production_profile",
            "root_registration",
            "items",
            "policy",
            "identity_sha256",
            "attestation_id",
        },
    )
    if (
        item["kind"] != LINEAGE_ATTESTATION_KIND
        or item["schema_version"] != SCHEMA_VERSION
        or item["implementation_version"] != IMPLEMENTATION_VERSION
        or item["policy"] != LINEAGE_POLICY
        or item["batch"]
        != {
            "batch_id": manifest.get("batch_id"),
            "identity_sha256": manifest.get("identity_sha256"),
            "physical_sha256": manifest_sha256,
        }
        or item["production_profile"]
        != {
            "profile_id": production_profile.get("profile_id"),
            "identity_sha256": production_profile.get("identity_sha256"),
        }
        or item["root_registration"]
        != {
            "registration_id": root_registration.get("registration_id"),
            "identity_sha256": root_registration.get("identity_sha256"),
            "filesystem_uuid": root_registration.get("filesystem", {}).get("uuid"),
        }
    ):
        raise TrustedLauncherError("lineage preflight common bindings are invalid")
    rows = item["items"]
    members = manifest.get("items")
    if not isinstance(rows, list) or not isinstance(members, list) or len(rows) != len(members):
        raise TrustedLauncherError("lineage preflight item set is incomplete")
    for ordinal, (row_value, member) in enumerate(zip(rows, members, strict=True), 1):
        row = _exact(
            row_value,
            f"lineage preflight item {ordinal}",
            {"ordinal", "work_order_identity_sha256", "lineage", "input", "status"},
        )
        order = member["work_order"]
        lineage = _exact(
            row["lineage"],
            f"lineage preflight item {ordinal} lineage",
            {"kind", "identity_sha256", "source_id", "member_id", "case_id"},
        )
        source = order["source_lineage"]
        if source["kind"] == "production_preprocess_v03":
            handoff = source["gpu_handoff"]
            expected_lineage = {
                "kind": source["kind"],
                "identity_sha256": handoff["identity_sha256"],
                "source_id": handoff["queue_id"],
                "member_id": handoff["member_id"],
                "case_id": None,
            }
        else:
            fixture = source["fixture_manifest"]
            expected_lineage = {
                "kind": source["kind"],
                "identity_sha256": fixture["identity_sha256"],
                "source_id": fixture["fixture_id"],
                "member_id": None,
                "case_id": source["fixture_case_id"],
            }
        if (
            row["ordinal"] != ordinal
            or row["work_order_identity_sha256"] != order["identity_sha256"]
            or lineage != expected_lineage
            or row["input"]
            != {
                "sha256": order["input"]["expected_sha256"],
                "byte_count": order["input"]["expected_byte_count"],
            }
            or row["status"] != "passed"
        ):
            raise TrustedLauncherError(
                f"lineage preflight item {ordinal} differs from its v5 work order"
            )
    _validate_semantic_identity(
        item,
        "identity_sha256",
        "attestation_id",
        "gpuasrlineage2_",
        "lineage preflight attestation",
    )
    return item


def _version_tuple(value: Any, label: str, *, parts: int) -> tuple[int, ...]:
    text = _bounded_text(value, label, 64)
    values = text.split(".")
    if len(values) != parts or any(not part.isdigit() for part in values):
        raise TrustedLauncherError(f"{label} is not a {parts}-part numeric version")
    return tuple(int(part) for part in values)


def resolve_gpu_observation(
    nvidia_smi: str,
    expected_uuid: str,
    minimum_driver_version: str,
    minimum_compute_capability: Sequence[int],
    *,
    runner: Any = subprocess.run,
) -> dict[str, Any]:
    if not GPU_UUID_RE.fullmatch(expected_uuid):
        raise TrustedLauncherError("expected GPU UUID is invalid")
    required_driver = _version_tuple(minimum_driver_version, "minimum driver version", parts=3)
    if (
        not isinstance(minimum_compute_capability, (list, tuple))
        or len(minimum_compute_capability) != 2
        or any(isinstance(part, bool) or not isinstance(part, int) or part < 0 for part in minimum_compute_capability)
    ):
        raise TrustedLauncherError("minimum compute capability is invalid")
    command = [
        nvidia_smi,
        "--query-gpu=index,uuid,driver_version,compute_cap",
        "--format=csv,noheader,nounits",
    ]
    try:
        completed = runner(
            command,
            check=False,
            capture_output=True,
            text=False,
            timeout=5,
            env={"PATH": "/usr/bin", "LANG": "C", "LC_ALL": "C"},
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise TrustedLauncherError(f"nvidia-smi GPU UUID lookup failed: {error}") from error
    stdout = completed.stdout if isinstance(completed.stdout, bytes) else b""
    stderr = completed.stderr if isinstance(completed.stderr, bytes) else b""
    if completed.returncode != 0 or len(stdout) > MAX_TOOL_OUTPUT_BYTES or len(stderr) > MAX_TOOL_OUTPUT_BYTES:
        raise TrustedLauncherError("nvidia-smi GPU UUID lookup failed closed")
    matches: list[dict[str, Any]] = []
    seen_indices: set[int] = set()
    seen_uuids: set[str] = set()
    try:
        lines = stdout.decode("ascii", errors="strict").splitlines()
    except UnicodeDecodeError as error:
        raise TrustedLauncherError("nvidia-smi output is not ASCII") from error
    for raw in lines:
        parts = [part.strip() for part in raw.split(",")]
        if len(parts) != 4 or not parts[0].isdigit() or not GPU_UUID_RE.fullmatch(parts[1]):
            raise TrustedLauncherError("nvidia-smi returned a malformed GPU row")
        index = int(parts[0])
        if not 0 <= index <= 255:
            raise TrustedLauncherError("nvidia-smi returned an out-of-range GPU index")
        driver = _version_tuple(parts[2], "observed driver version", parts=3)
        capability = _version_tuple(parts[3], "observed compute capability", parts=2)
        if index in seen_indices or parts[1] in seen_uuids:
            raise TrustedLauncherError("nvidia-smi returned duplicate GPU identities")
        seen_indices.add(index)
        seen_uuids.add(parts[1])
        if parts[1] == expected_uuid:
            matches.append(
                {
                    "uuid": parts[1],
                    "host_index": index,
                    "visible_index": 0,
                    "driver_version": parts[2],
                    "compute_capability": list(capability),
                    "minimum_driver_version": minimum_driver_version,
                    "minimum_compute_capability": list(minimum_compute_capability),
                }
            )
    if len(matches) != 1:
        raise TrustedLauncherError("admitted GPU UUID is absent or ambiguous")
    match = matches[0]
    xml_command = [nvidia_smi, "-q", "-x"]
    try:
        xml_result = runner(
            xml_command,
            check=False,
            capture_output=True,
            text=False,
            timeout=5,
            env={"PATH": "/usr/bin", "LANG": "C", "LC_ALL": "C"},
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise TrustedLauncherError(f"nvidia-smi device-minor lookup failed: {error}") from error
    xml_stdout = xml_result.stdout if isinstance(xml_result.stdout, bytes) else b""
    xml_stderr = xml_result.stderr if isinstance(xml_result.stderr, bytes) else b""
    if (
        xml_result.returncode != 0
        or not xml_stdout
        or len(xml_stdout) > MAX_NVIDIA_XML_BYTES
        or len(xml_stderr) > MAX_TOOL_OUTPUT_BYTES
    ):
        raise TrustedLauncherError("nvidia-smi device-minor lookup failed closed")
    try:
        xml_root = ET.fromstring(xml_stdout)
    except ET.ParseError as error:
        raise TrustedLauncherError("nvidia-smi XML is malformed") from error
    xml_driver_text = (xml_root.findtext("driver_version") or "").strip()
    xml_driver = _version_tuple(
        xml_driver_text, "nvidia-smi XML driver version", parts=3
    )
    if xml_driver != _version_tuple(
        match["driver_version"], "matched driver version", parts=3
    ):
        raise TrustedLauncherError(
            "nvidia-smi driver version changed between identity observations"
        )
    minor_matches: list[int] = []
    seen_minors: set[int] = set()
    for gpu in xml_root.findall("gpu"):
        observed_uuid = (gpu.findtext("uuid") or "").strip()
        minor_text = (gpu.findtext("minor_number") or "").strip()
        if not GPU_UUID_RE.fullmatch(observed_uuid) or not minor_text.isdigit():
            raise TrustedLauncherError("nvidia-smi XML GPU identity is malformed")
        minor = int(minor_text)
        if not 0 <= minor <= 255 or minor in seen_minors:
            raise TrustedLauncherError("nvidia-smi XML device minor is invalid or duplicated")
        seen_minors.add(minor)
        if observed_uuid == expected_uuid:
            minor_matches.append(minor)
    if len(minor_matches) != 1:
        raise TrustedLauncherError("admitted GPU UUID lacks one device minor")
    match["device_minor"] = minor_matches[0]
    matched_driver = _version_tuple(match["driver_version"], "matched driver version", parts=3)
    matched_capability = tuple(match["compute_capability"])
    if (
        matched_driver != required_driver
        or matched_capability < tuple(minimum_compute_capability)
    ):
        raise TrustedLauncherError(
            "GPU driver differs from the exact admitted version or compute "
            "capability is below the admitted minimum"
        )
    return match


def validate_gpu_devices(index: int, controls: Sequence[str]) -> list[str]:
    if isinstance(index, bool) or not isinstance(index, int) or not 0 <= index <= 255:
        raise TrustedLauncherError("GPU host index is invalid")
    paths = [*controls, f"/dev/nvidia{index}"]
    if len(set(paths)) != 4:
        raise TrustedLauncherError("GPU device set is not exact")
    for value in paths:
        path = normalized_absolute_path(value, "GPU device path")
        try:
            info = path.lstat()
        except OSError as error:
            raise TrustedLauncherError(f"GPU device is unavailable: {path}: {error}") from error
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISCHR(info.st_mode) or info.st_uid != 0:
            raise TrustedLauncherError(f"GPU path is not a direct root-owned character device: {path}")
    return paths


def _bounded_kernel_text(path: Path, label: str) -> str:
    try:
        body = path.read_bytes()
    except OSError as error:
        raise TrustedLauncherError(f"cannot read {label}: {error}") from error
    if not body or len(body) > MAX_CGROUP_BYTES:
        raise TrustedLauncherError(f"{label} is empty or exceeds its bound")
    try:
        text = body.decode("ascii", errors="strict").strip()
    except UnicodeDecodeError as error:
        raise TrustedLauncherError(f"{label} is not ASCII") from error
    if not text or "\x00" in text:
        raise TrustedLauncherError(f"{label} is malformed")
    return text


def _current_cgroup_v2() -> tuple[Path, str]:
    rows = _bounded_kernel_text(Path("/proc/self/cgroup"), "current cgroup").splitlines()
    unified = [row[3:] for row in rows if row.startswith("0::")]
    if len(unified) != 1:
        raise TrustedLauncherError("current process lacks one cgroup-v2 membership")
    relative_text = unified[0]
    if not relative_text.startswith("/") or ".." in PurePosixPath(relative_text).parts:
        raise TrustedLauncherError("current cgroup-v2 path is unsafe")
    root = Path("/sys/fs/cgroup")
    current = Path(os.path.normpath(str(root) + relative_text))
    if current != root and root not in current.parents:
        raise TrustedLauncherError("current cgroup-v2 path escapes its mount")
    _reject_symlink_components(current, "current cgroup-v2 path")
    return current, relative_text


def _cgroup_effective_limit(current: Path, filename: str, label: str) -> int | None:
    values: list[int] = []
    root = Path("/sys/fs/cgroup")
    cursor = current
    while True:
        control = cursor / filename
        if control.exists():
            text = _bounded_kernel_text(control, f"{label} at {cursor}")
            if text != "max":
                if not text.isdigit():
                    raise TrustedLauncherError(f"{label} is neither max nor an integer")
                values.append(int(text))
        if cursor == root:
            break
        cursor = cursor.parent
    return min(values) if values else None


def validate_host_envelope_observation(
    value: Any,
    *,
    minimum_result_bytes: int,
    enforce: bool,
) -> dict[str, Any]:
    item = _exact(
        value,
        "host resource envelope",
        {
            "cgroup_version",
            "cgroup_path",
            "effective",
            "rlimits",
            "requirements",
            "status",
            "violations",
        },
    )
    effective = _exact(
        item["effective"],
        "host effective cgroup limits",
        {"memory_max_bytes", "memory_swap_max_bytes", "pids_max"},
    )
    limits = _exact(item["rlimits"], "host rlimits", {"core", "fsize", "nofile"})
    normalized_limits: dict[str, dict[str, int | None]] = {}
    for name, row in limits.items():
        limit = _exact(row, f"host {name} rlimit", {"soft", "hard"})
        normalized_limits[name] = {}
        for bound in ("soft", "hard"):
            observed = limit[bound]
            normalized_limits[name][bound] = (
                None
                if observed is None
                else _integer(observed, f"host {name} {bound} rlimit", 0)
            )
    minimum = _integer(
        minimum_result_bytes,
        "minimum result file-size allowance",
        1,
        MAX_HOST_FSIZE_BYTES,
    )
    requirements = {
        "memory_max_bytes_at_most": MAX_HOST_MEMORY_BYTES,
        "memory_swap_max_bytes": 0,
        "pids_max_at_most": MAX_HOST_PIDS,
        "nofile_at_most": MAX_HOST_NOFILE,
        "core_bytes": 0,
        "fsize_bytes_at_least": minimum,
        "fsize_bytes_at_most": MAX_HOST_FSIZE_BYTES,
    }
    violations: list[str] = []
    memory = effective.get("memory_max_bytes")
    swap = effective.get("memory_swap_max_bytes")
    pids = effective.get("pids_max")
    if isinstance(memory, bool) or not isinstance(memory, int) or not 1 <= memory <= MAX_HOST_MEMORY_BYTES:
        violations.append("memory.max")
    if swap != 0:
        violations.append("memory.swap.max")
    if isinstance(pids, bool) or not isinstance(pids, int) or not 1 <= pids <= MAX_HOST_PIDS:
        violations.append("pids.max")
    if any(normalized_limits["core"][key] != 0 for key in ("soft", "hard")):
        violations.append("RLIMIT_CORE")
    if any(
        not isinstance(normalized_limits["nofile"][key], int)
        or not 1 <= normalized_limits["nofile"][key] <= MAX_HOST_NOFILE
        for key in ("soft", "hard")
    ):
        violations.append("RLIMIT_NOFILE")
    if any(
        not isinstance(normalized_limits["fsize"][key], int)
        or not minimum <= normalized_limits["fsize"][key] <= MAX_HOST_FSIZE_BYTES
        for key in ("soft", "hard")
    ):
        violations.append("RLIMIT_FSIZE")
    violations = sorted(set(violations))
    expected_status = "passed" if not violations else "report_only_failed"
    normalized = {
        "cgroup_version": 2,
        "cgroup_path": _bounded_text(item["cgroup_path"], "host cgroup path", 4096),
        "effective": {
            "memory_max_bytes": memory,
            "memory_swap_max_bytes": swap,
            "pids_max": pids,
        },
        "rlimits": normalized_limits,
        "requirements": requirements,
        "status": expected_status,
        "violations": violations,
    }
    if item["cgroup_version"] != 2 or item != normalized:
        raise TrustedLauncherError("host resource envelope observation is noncanonical")
    if enforce and violations:
        raise TrustedLauncherError(
            "production host resource envelope failed: " + ", ".join(violations)
        )
    return normalized


def observe_host_envelope(
    production_profile: dict[str, Any], *, enforce: bool
) -> dict[str, Any]:
    cgroup, relative = _current_cgroup_v2()

    def finite(name: str, label: str) -> int | None:
        return _cgroup_effective_limit(cgroup, name, label)

    def rlimit(which: int) -> dict[str, int | None]:
        soft, hard = resource.getrlimit(which)
        return {
            "soft": None if soft == resource.RLIM_INFINITY else int(soft),
            "hard": None if hard == resource.RLIM_INFINITY else int(hard),
        }

    maximum_result = _integer(
        production_profile.get("item_limits", {}).get("maximum_result_bytes"),
        "profile maximum_result_bytes",
        1,
        MAX_HOST_FSIZE_BYTES,
    )
    raw = {
        "cgroup_version": 2,
        "cgroup_path": relative,
        "effective": {
            "memory_max_bytes": finite("memory.max", "memory.max"),
            "memory_swap_max_bytes": finite("memory.swap.max", "memory.swap.max"),
            "pids_max": finite("pids.max", "pids.max"),
        },
        "rlimits": {
            "core": rlimit(resource.RLIMIT_CORE),
            "fsize": rlimit(resource.RLIMIT_FSIZE),
            "nofile": rlimit(resource.RLIMIT_NOFILE),
        },
        "requirements": {
            "memory_max_bytes_at_most": MAX_HOST_MEMORY_BYTES,
            "memory_swap_max_bytes": 0,
            "pids_max_at_most": MAX_HOST_PIDS,
            "nofile_at_most": MAX_HOST_NOFILE,
            "core_bytes": 0,
            "fsize_bytes_at_least": maximum_result,
            "fsize_bytes_at_most": MAX_HOST_FSIZE_BYTES,
        },
        "status": "passed",
        "violations": [],
    }
    # Let the validator derive status/violations so caller-supplied claims never
    # influence enforcement.
    provisional = {**raw, "status": "report_only_failed", "violations": []}
    try:
        return validate_host_envelope_observation(
            raw, minimum_result_bytes=maximum_result, enforce=enforce
        )
    except TrustedLauncherError as first:
        # Reconstruct deterministic violations without weakening production:
        # validation of a forged status is expected to fail once, then the same
        # checks run with the derived report-only envelope.
        effective = raw["effective"]
        limits = raw["rlimits"]
        violations = []
        if not isinstance(effective["memory_max_bytes"], int) or not 1 <= effective["memory_max_bytes"] <= MAX_HOST_MEMORY_BYTES:
            violations.append("memory.max")
        if effective["memory_swap_max_bytes"] != 0:
            violations.append("memory.swap.max")
        if not isinstance(effective["pids_max"], int) or not 1 <= effective["pids_max"] <= MAX_HOST_PIDS:
            violations.append("pids.max")
        if any(limits["core"][key] != 0 for key in ("soft", "hard")):
            violations.append("RLIMIT_CORE")
        if any(not isinstance(limits["nofile"][key], int) or not 1 <= limits["nofile"][key] <= MAX_HOST_NOFILE for key in ("soft", "hard")):
            violations.append("RLIMIT_NOFILE")
        if any(not isinstance(limits["fsize"][key], int) or not maximum_result <= limits["fsize"][key] <= MAX_HOST_FSIZE_BYTES for key in ("soft", "hard")):
            violations.append("RLIMIT_FSIZE")
        provisional["violations"] = sorted(set(violations))
        if not provisional["violations"]:
            raise first
        return validate_host_envelope_observation(
            provisional, minimum_result_bytes=maximum_result, enforce=enforce
        )


def _parent_directories(paths: Iterable[str]) -> list[str]:
    result: set[PurePosixPath] = set()
    for text in paths:
        path = PurePosixPath(text)
        for parent in path.parents:
            if parent == PurePosixPath("/"):
                continue
            result.add(parent)
    return [str(path) for path in sorted(result, key=lambda row: (len(row.parts), str(row)))]


def _descriptor_number(value: Any, label: str) -> int:
    """Normalize a non-stdio descriptor passed to Bubblewrap's FD APIs."""

    return _integer(value, label, 3, 2**31 - 1)


def sandbox_plan(
    *,
    mappings: Sequence[dict[str, str]],
    root_path: str,
    manifest_file: dict[str, Any],
    control_bindings: dict[str, dict[str, Any]],
    item_read_bindings: Sequence[dict[str, Any]],
    writable_roots: dict[str, str],
    gpu_devices: Sequence[str],
    gpu_uuid: str,
    gpu_index: int,
    host_abi_bindings: Sequence[dict[str, Any]],
    launcher_profile: dict[str, Any],
) -> dict[str, Any]:
    return {
        "mappings": list(mappings),
        "hot_root": {"path": root_path, "bound": False},
        "batch_manifest": {
            **manifest_file,
            "target": f"{INPUT_ROOT}/batch.json",
        },
        "controls": {
            name: dict(binding)
            for name, binding in sorted(control_bindings.items())
        },
        "item_read_bindings": list(item_read_bindings),
        "writable_roots": {
            "result": {"source": writable_roots["result"], "sandbox": writable_roots["result"]},
            "event": {"source": writable_roots["event"], "sandbox": writable_roots["event"]},
            "lock": {"source": writable_roots["lock"], "sandbox": writable_roots["lock"]},
        },
        "gpu": {"uuid": gpu_uuid, "host_index": gpu_index, "visible_index": 0, "devices": list(gpu_devices)},
        "host_abi_bindings": list(host_abi_bindings),
        "system_library_directories": launcher_profile["sandbox"]["system_library_directories"],
        "system_readonly_files": launcher_profile["sandbox"]["system_readonly_files"],
        "namespaces": ["cgroup_try", "ipc", "network", "pid", "user", "uts"],
        "network_access": False,
    }


def make_local_readiness(
    *,
    launcher_profile: dict[str, Any],
    launcher_profile_file: dict[str, str],
    runtime: dict[str, Any],
    runtime_file: dict[str, str],
    production_profile: dict[str, Any],
    production_profile_file: dict[str, str],
    registration: dict[str, Any],
    root_file: dict[str, str],
    host_abi_identity_sha256: str,
    gpu: dict[str, Any],
    host_envelope: dict[str, Any],
) -> dict[str, Any]:
    core = {
        "kind": LOCAL_READINESS_KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": "passed",
        "mode": MODE_LOCAL_PRIVATE,
        "execution_class": EXECUTION_CLASS_LOCAL_PRIVATE,
        "launcher": {
            **launcher_profile["launcher"],
            "profile": {
                **launcher_profile_file,
                "identity_sha256": launcher_profile["identity_sha256"],
            },
        },
        "runtime_admission": {
            **runtime_file,
            "identity_sha256": runtime["identity_sha256"],
            "status": runtime["status"],
        },
        "execution_image": dict(launcher_profile["execution_image"]),
        "production_profile": {
            **production_profile_file,
            "identity_sha256": production_profile["identity_sha256"],
        },
        "root_registration": {
            **root_file,
            "identity_sha256": registration["identity_sha256"],
            "registration_id": registration["registration_id"],
        },
        "host_abi_identity_sha256": _digest(
            host_abi_identity_sha256, "readiness host ABI identity"
        ),
        "gpu": dict(gpu),
        "host_envelope": host_envelope,
        "trust_boundary": {
            "kind": "current_user_same_uid",
            "same_uid_mutation_resistance": False,
        },
        "checks": {
            "same_image_current_launcher": True,
            "candidate_runtime_replayed": True,
            "image_full_sha256_checked": True,
            "host_abi_replayed": True,
            "gpu_uuid_resolved": True,
            "resource_envelope_enforced": True,
            "media_read": False,
            "inference_performed": False,
        },
        "policy": dict(POLICY),
    }
    identity = sha256_bytes(canonical_bytes(core))
    return {
        **core,
        "identity_sha256": identity,
        "readiness_id": f"gpulocalready_{identity[:32]}",
    }


def validate_local_readiness(value: Any) -> dict[str, Any]:
    fields = {
        "kind", "schema_version", "implementation_version", "status", "mode",
        "execution_class", "launcher", "runtime_admission", "execution_image",
        "production_profile", "root_registration", "host_abi_identity_sha256",
        "gpu", "host_envelope", "trust_boundary", "checks", "policy",
        "identity_sha256", "readiness_id",
    }
    item = _exact(value, "local readiness receipt", fields)
    if (
        item["kind"] != LOCAL_READINESS_KIND
        or item["schema_version"] != SCHEMA_VERSION
        or item["implementation_version"] != IMPLEMENTATION_VERSION
        or item["status"] != "passed"
        or item["mode"] != MODE_LOCAL_PRIVATE
        or item["execution_class"] != EXECUTION_CLASS_LOCAL_PRIVATE
        or item["policy"] != POLICY
        or item["trust_boundary"]
        != {
            "kind": "current_user_same_uid",
            "same_uid_mutation_resistance": False,
        }
        or item["checks"]
        != {
            "same_image_current_launcher": True,
            "candidate_runtime_replayed": True,
            "image_full_sha256_checked": True,
            "host_abi_replayed": True,
            "gpu_uuid_resolved": True,
            "resource_envelope_enforced": True,
            "media_read": False,
            "inference_performed": False,
        }
    ):
        raise TrustedLauncherError("local readiness header/policy/checks are invalid")
    _validate_semantic_identity(
        item,
        "identity_sha256",
        "readiness_id",
        "gpulocalready_",
        "local readiness receipt",
    )
    return item


def make_launch_attestation(
    *,
    mode: str,
    launcher_profile: dict[str, Any],
    launcher_profile_file: dict[str, str],
    runtime_receipt: dict[str, Any],
    runtime_file: dict[str, str],
    root_registration: dict[str, Any],
    root_file: dict[str, str],
    production_profile: dict[str, Any],
    production_profile_file: dict[str, str],
    manifest: dict[str, Any],
    manifest_file: dict[str, str],
    lineage_preflight_file: dict[str, str],
    host_abi: dict[str, Any],
    plan: dict[str, Any],
    host_envelope: dict[str, Any],
    parent_network_namespace: str,
    local_readiness_file: dict[str, str] | None = None,
) -> dict[str, Any]:
    trust_boundary = {
        MODE_PRODUCTION: {
            "kind": "root_admitted",
            "control_owner": "root",
            "same_uid_mutation_resistance": True,
        },
        MODE_LOCAL_PRIVATE: {
            "kind": "current_user_same_uid",
            "control_owner": "current_user",
            "same_uid_mutation_resistance": False,
        },
        MODE_SYNTHETIC: {
            "kind": "synthetic_candidate",
            "control_owner": "candidate_binding",
            "same_uid_mutation_resistance": False,
        },
    }.get(mode)
    if trust_boundary is None:
        raise TrustedLauncherError("launch attestation mode is unsupported")
    core = {
        "kind": ATTESTATION_KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "mode": mode,
        "nonce": secrets.token_hex(32),
        "launcher": {
            **launcher_profile["launcher"],
            "profile": {**launcher_profile_file, "identity_sha256": launcher_profile["identity_sha256"]},
        },
        "runtime_admission": {**runtime_file, "identity_sha256": runtime_receipt["identity_sha256"], "status": runtime_receipt["status"]},
        "root_registration": {**root_file, "identity_sha256": root_registration["identity_sha256"], "registration_id": root_registration["registration_id"], "root_id": root_registration["root_id"], "filesystem_uuid": root_registration["filesystem"]["uuid"]},
        "execution_image": dict(launcher_profile["execution_image"]),
        "production_profile": {**production_profile_file, "identity_sha256": production_profile["identity_sha256"], "gpu_uuid": production_profile["hardware"]["gpu_uuid"]},
        "batch": {
            **manifest_file,
            "execution_class": manifest["execution_class"],
            "identity_sha256": _digest(
                manifest.get("identity_sha256"), "batch manifest identity"
            ),
        },
        "lineage_preflight": dict(lineage_preflight_file),
        "host_abi": dict(host_abi),
        "sandbox_plan": plan,
        "sandbox_plan_identity_sha256": sha256_bytes(canonical_bytes(plan)),
        "host_envelope": host_envelope,
        "parent_network_namespace": _bounded_text(parent_network_namespace, "parent network namespace", 256),
        "child_network_namespace_must_differ": True,
        "local_readiness": local_readiness_file,
        "trust_boundary": trust_boundary,
        "host_trust_checks": {
            "runtime_receipt_owner_checked": True,
            "root_registration_replayed": True,
            "execution_image_full_sha256_checked": True,
            "launcher_and_tools_sha256_checked": True,
            "host_abi_manifest_replayed": True,
            "gpu_uuid_resolved_at_launch": True,
        },
        "policy": dict(POLICY),
    }
    identity = sha256_bytes(canonical_bytes(core))
    return validate_launch_attestation(
        {**core, "identity_sha256": identity, "attestation_id": f"gpulaunch_{identity[:32]}"}
    )


def validate_launch_attestation(value: Any) -> dict[str, Any]:
    fields = {
        "kind",
        "schema_version",
        "implementation_version",
        "mode",
        "nonce",
        "launcher",
        "runtime_admission",
        "root_registration",
        "execution_image",
        "production_profile",
        "batch",
        "lineage_preflight",
        "host_abi",
        "sandbox_plan",
        "sandbox_plan_identity_sha256",
        "host_envelope",
        "parent_network_namespace",
        "child_network_namespace_must_differ",
        "local_readiness",
        "trust_boundary",
        "host_trust_checks",
        "policy",
        "identity_sha256",
        "attestation_id",
    }
    item = _exact(value, "launch attestation", fields)
    if (
        item["kind"] != ATTESTATION_KIND
        or item["schema_version"] != SCHEMA_VERSION
        or item["implementation_version"] != IMPLEMENTATION_VERSION
        or item["mode"] not in MODES
        or item["policy"] != POLICY
    ):
        raise TrustedLauncherError("launch attestation header/policy is unsupported")
    nonce = item["nonce"]
    if not isinstance(nonce, str) or not re.fullmatch(r"[0-9a-f]{64}", nonce):
        raise TrustedLauncherError("launch attestation nonce is invalid")
    launcher = _exact(item["launcher"], "attestation launcher", {"path", "sha256", "profile"})
    _reference({"path": launcher["path"], "sha256": launcher["sha256"]}, "attestation launcher")
    _reference(launcher["profile"], "attestation launcher profile", identity=True)
    runtime = _exact(item["runtime_admission"], "attestation runtime", {"path", "sha256", "identity_sha256", "status"})
    _reference({key: runtime[key] for key in ("path", "sha256", "identity_sha256")}, "attestation runtime", identity=True)
    expected_status = MODE_RUNTIME_STATUS[item["mode"]]
    if runtime["status"] != expected_status:
        raise TrustedLauncherError("launch attestation runtime status differs from mode")
    root = _exact(
        item["root_registration"],
        "attestation root",
        {"path", "sha256", "identity_sha256", "registration_id", "root_id", "filesystem_uuid"},
    )
    _reference({key: root[key] for key in ("path", "sha256", "identity_sha256")}, "attestation root", identity=True)
    if not REGISTRATION_ID_RE.fullmatch(root["registration_id"]) or not ROOT_ID_RE.fullmatch(root["root_id"]):
        raise TrustedLauncherError("launch attestation root identifiers are invalid")
    try:
        if str(uuid.UUID(root["filesystem_uuid"])) != root["filesystem_uuid"]:
            raise ValueError("noncanonical UUID")
    except (TypeError, ValueError, AttributeError) as error:
        raise TrustedLauncherError("launch attestation filesystem UUID is invalid") from error
    image = _exact(
        item["execution_image"],
        "attestation image",
        {"path", "sha256", "byte_count", "identity_sha256", "receipt_path", "receipt_sha256"},
    )
    normalized_absolute_path(image["path"], "attestation image path")
    normalized_absolute_path(image["receipt_path"], "attestation image receipt path")
    for name in ("sha256", "identity_sha256", "receipt_sha256"):
        _digest(image[name], f"attestation image {name}")
    _integer(image["byte_count"], "attestation image byte_count", 1, MAX_IMAGE_BYTES)
    production = _exact(
        item["production_profile"],
        "attestation production profile",
        {"path", "sha256", "identity_sha256", "gpu_uuid"},
    )
    _reference({key: production[key] for key in ("path", "sha256", "identity_sha256")}, "attestation production profile", identity=True)
    if not isinstance(production["gpu_uuid"], str) or not GPU_UUID_RE.fullmatch(production["gpu_uuid"]):
        raise TrustedLauncherError("attestation production GPU UUID is invalid")
    batch = _exact(
        item["batch"],
        "attestation batch",
        {"path", "sha256", "execution_class", "identity_sha256"},
    )
    normalized_absolute_path(batch["path"], "attestation batch path")
    _digest(batch["sha256"], "attestation batch SHA-256")
    _digest(batch["identity_sha256"], "attestation batch identity")
    expected_class = MODE_EXECUTION_CLASS[item["mode"]]
    if batch["execution_class"] != expected_class:
        raise TrustedLauncherError("attestation execution class differs from mode")
    lineage = _exact(
        item["lineage_preflight"],
        "attestation lineage preflight",
        {"path", "sha256", "identity_sha256", "attestation_id"},
    )
    normalized_absolute_path(lineage["path"], "attestation lineage preflight path")
    identity = _digest(lineage["identity_sha256"], "attestation lineage preflight identity")
    _digest(lineage["sha256"], "attestation lineage preflight SHA-256")
    if lineage["attestation_id"] != f"gpuasrlineage2_{identity[:32]}":
        raise TrustedLauncherError("attestation lineage preflight ID is inconsistent")
    host_abi = _exact(
        item["host_abi"],
        "attestation host ABI",
        {
            "identity_sha256",
            "manifest_id",
            "platform_replayed",
            "libraries_replayed",
            "library_count",
            "binding_count",
        },
    )
    host_abi_identity = _digest(
        host_abi["identity_sha256"], "attestation host ABI identity"
    )
    host_abi_library_count = _integer(
        host_abi["library_count"], "attestation host ABI library count", 1, 256
    )
    host_abi_binding_count = _integer(
        host_abi["binding_count"], "attestation host ABI binding count", 1, 256
    )
    if (
        host_abi["manifest_id"] != f"gpuhostabi_{host_abi_identity[:32]}"
        or host_abi["platform_replayed"] is not True
        or host_abi["libraries_replayed"] is not True
        or host_abi_library_count != host_abi_binding_count
    ):
        raise TrustedLauncherError("attestation host ABI replay is inconsistent")
    plan = item["sandbox_plan"]
    if not isinstance(plan, dict) or item["sandbox_plan_identity_sha256"] != sha256_bytes(canonical_bytes(plan)):
        raise TrustedLauncherError("attestation sandbox-plan identity is invalid")
    if (
        plan.get("system_library_directories", []) != []
        or plan.get("system_readonly_files", []) != []
    ):
        raise TrustedLauncherError("attestation broad host-library exposure is forbidden")
    host_bindings = plan.get("host_abi_bindings")
    if not isinstance(host_bindings, list) or len(host_bindings) != host_abi["binding_count"]:
        raise TrustedLauncherError("attestation host ABI binding count is invalid")
    normalized_host_targets: list[str] = []
    for ordinal, value_row in enumerate(host_bindings, 1):
        row = _exact(
            value_row,
            f"attestation host ABI binding {ordinal}",
            {
                "role",
                "source_path",
                "target",
                "sha256",
                "byte_count",
                "uid",
                "gid",
                "mode",
            },
        )
        source = normalized_absolute_path(
            row["source_path"], f"attestation host ABI binding {ordinal} source"
        )
        target = normalized_absolute_path(
            row["target"], f"attestation host ABI binding {ordinal} target"
        )
        mode = row["mode"]
        if (
            row["role"] != "host_abi_library"
            or source.parent != Path("/usr/lib64")
            or target.parent != Path("/usr/lib64")
            or row["uid"] != 0
            or row["gid"] != 0
            or not isinstance(mode, str)
            or not MODE_RE.fullmatch(mode)
            or int(mode, 8) & 0o7022
            or not int(mode, 8) & 0o444
        ):
            raise TrustedLauncherError("attestation host ABI binding policy is invalid")
        _digest(row["sha256"], f"attestation host ABI binding {ordinal} SHA-256")
        _integer(
            row["byte_count"],
            f"attestation host ABI binding {ordinal} byte_count",
            1,
            MAX_HOST_ABI_LIBRARY_BYTES,
        )
        normalized_host_targets.append(str(target))
    if (
        host_bindings != sorted(host_bindings, key=lambda row: row["target"])
        or len(set(normalized_host_targets)) != len(normalized_host_targets)
    ):
        raise TrustedLauncherError("attestation host ABI bindings are noncanonical")
    gpu = _exact(
        plan.get("gpu"),
        "attestation GPU plan",
        {
            "uuid",
            "host_index",
            "device_minor",
            "visible_index",
            "devices",
            "driver_version",
            "compute_capability",
            "minimum_driver_version",
            "minimum_compute_capability",
        },
    )
    host_index = _integer(gpu["host_index"], "attestation GPU host index", 0, 255)
    device_minor = _integer(
        gpu["device_minor"], "attestation GPU device minor", 0, 255
    )
    driver = _version_tuple(
        gpu["driver_version"], "attestation GPU driver version", parts=3
    )
    required_driver = _version_tuple(
        gpu["minimum_driver_version"],
        "attestation exact admitted driver version",
        parts=3,
    )
    capability = gpu["compute_capability"]
    required_capability = gpu["minimum_compute_capability"]
    if (
        gpu["uuid"] != production["gpu_uuid"]
        or gpu["visible_index"] != 0
        or host_index != gpu["host_index"]
        or device_minor != gpu["device_minor"]
        or driver != required_driver
        or not isinstance(capability, list)
        or not isinstance(required_capability, list)
        or len(capability) != 2
        or len(required_capability) != 2
        or any(
            isinstance(part, bool) or not isinstance(part, int) or part < 0
            for part in [*capability, *required_capability]
        )
        or tuple(capability) < tuple(required_capability)
        or gpu["devices"]
        != [
            "/dev/nvidiactl",
            "/dev/nvidia-uvm",
            "/dev/nvidia-uvm-tools",
            f"/dev/nvidia{device_minor}",
        ]
    ):
        raise TrustedLauncherError("attestation GPU plan is inconsistent")
    if plan.get("network_access") is not False or plan.get("namespaces") != [
        "cgroup_try", "ipc", "network", "pid", "user", "uts"
    ]:
        raise TrustedLauncherError("attestation namespace plan is not exact")
    envelope = item["host_envelope"]
    minimum_result = envelope.get("requirements", {}).get("fsize_bytes_at_least") if isinstance(envelope, dict) else None
    validate_host_envelope_observation(
        envelope,
        minimum_result_bytes=minimum_result,
        enforce=item["mode"] in {MODE_PRODUCTION, MODE_LOCAL_PRIVATE},
    )
    _bounded_text(item["parent_network_namespace"], "attestation parent network namespace", 256)
    if item["child_network_namespace_must_differ"] is not True:
        raise TrustedLauncherError("attestation does not require a new network namespace")
    readiness = item["local_readiness"]
    if item["mode"] == MODE_LOCAL_PRIVATE:
        readiness = _exact(
            readiness,
            "attestation local readiness",
            {"path", "sha256", "identity_sha256", "readiness_id"},
        )
        _reference(
            {key: readiness[key] for key in ("path", "sha256", "identity_sha256")},
            "attestation local readiness",
            identity=True,
        )
        identity = _digest(
            readiness["identity_sha256"], "attestation readiness identity"
        )
        if readiness["readiness_id"] != f"gpulocalready_{identity[:32]}":
            raise TrustedLauncherError("attestation readiness ID is inconsistent")
    elif readiness is not None:
        raise TrustedLauncherError("non-local attestation may not bind local readiness")
    expected_checks = {
        "runtime_receipt_owner_checked": True,
        "root_registration_replayed": True,
        "execution_image_full_sha256_checked": True,
        "launcher_and_tools_sha256_checked": True,
        "host_abi_manifest_replayed": True,
        "gpu_uuid_resolved_at_launch": True,
    }
    if item["host_trust_checks"] != expected_checks:
        raise TrustedLauncherError("attestation host trust checks are not exact")
    expected_boundary = {
        MODE_PRODUCTION: {
            "kind": "root_admitted",
            "control_owner": "root",
            "same_uid_mutation_resistance": True,
        },
        MODE_LOCAL_PRIVATE: {
            "kind": "current_user_same_uid",
            "control_owner": "current_user",
            "same_uid_mutation_resistance": False,
        },
        MODE_SYNTHETIC: {
            "kind": "synthetic_candidate",
            "control_owner": "candidate_binding",
            "same_uid_mutation_resistance": False,
        },
    }[item["mode"]]
    if item["trust_boundary"] != expected_boundary:
        raise TrustedLauncherError("attestation trust boundary is not exact")
    _validate_semantic_identity(item, "identity_sha256", "attestation_id", "gpulaunch_", "launch attestation")
    return item


def build_bwrap_argv(
    *,
    launcher_profile: dict[str, Any],
    mappings: Sequence[dict[str, str]],
    mapping_sources: dict[str, int],
    host_abi_sources: dict[str, int],
    manifest_source: int,
    control_sources: dict[str, dict[str, int]],
    control_original_targets: dict[str, str],
    lineage_preflight_source: int,
    lineage_preflight_sha256: str,
    read_binding_sources: dict[str, int],
    writable_sources: dict[str, int],
    writable_targets: dict[str, str],
    attestation_source: int,
    attestation_sha256: str,
    gpu_devices: Sequence[str],
    gpu_uuid: str,
    gpu_index: int,
    batch_manifest_sha256: str,
    runtime_sha256: str,
    profile_sha256: str,
    root_sha256: str,
) -> list[str]:
    by_name = {row["name"]: row for row in mappings}
    if set(by_name) != REQUIRED_MAPPING_NAMES or set(mapping_sources) != REQUIRED_MAPPING_NAMES:
        raise TrustedLauncherError("bwrap mapping closure is incomplete")
    normalized_mapping_sources = {
        name: _descriptor_number(value, f"mapping {name} descriptor")
        for name, value in mapping_sources.items()
    }
    normalized_host_abi_sources: dict[str, int] = {}
    for target, source in host_abi_sources.items():
        target_path = normalized_absolute_path(target, "host ABI sandbox target")
        if str(target_path) != target or target_path.parent != Path("/usr/lib64"):
            raise TrustedLauncherError("host ABI target is not normalized")
        normalized_host_abi_sources[target] = _descriptor_number(
            source, f"host ABI {target} descriptor"
        )
    if not normalized_host_abi_sources:
        raise TrustedLauncherError("bwrap host ABI closure is empty")
    manifest_descriptor = _descriptor_number(
        manifest_source, "batch manifest descriptor"
    )
    controls = {
        "runtime": f"{CONTROL_ROOT}/runtime.json",
        "profile": f"{CONTROL_ROOT}/profile.json",
        "root": f"{CONTROL_ROOT}/root.json",
    }
    lineage_target = f"{CONTROL_ROOT}/lineage-preflight.json"
    if set(control_sources) != set(controls):
        raise TrustedLauncherError("bwrap control-source set is not exact")
    normalized_control_sources: dict[str, dict[str, int]] = {}
    for name, value in control_sources.items():
        row = _exact(
            value,
            f"bwrap {name} control descriptors",
            {"control", "work_order"},
        )
        normalized_control_sources[name] = {
            key: _descriptor_number(
                row[key], f"bwrap {name} {key} control descriptor"
            )
            for key in ("control", "work_order")
        }
    if set(control_original_targets) != set(controls):
        raise TrustedLauncherError("bwrap original control-target set is not exact")
    if set(writable_sources) != {"result", "event", "lock"} or set(writable_targets) != set(writable_sources):
        raise TrustedLauncherError("bwrap writable-root set is not exact")
    normalized_read_sources: dict[str, int] = {}
    for target, source in read_binding_sources.items():
        target_path = normalized_absolute_path(target, "read-binding sandbox target")
        if str(target_path) != target:
            raise TrustedLauncherError("read-binding target is not normalized")
        normalized_read_sources[target] = _descriptor_number(
            source, f"read-binding {target} descriptor"
        )
    normalized_writable_sources = {
        name: _descriptor_number(value, f"writable {name} descriptor")
        for name, value in writable_sources.items()
    }
    lineage_descriptor = _descriptor_number(
        lineage_preflight_source, "lineage preflight descriptor"
    )
    attestation_descriptor = _descriptor_number(
        attestation_source, "launch attestation descriptor"
    )
    normalized_control_targets = {
        name: str(
            normalized_absolute_path(target, f"{name} original control target")
        )
        for name, target in control_original_targets.items()
    }
    if len(set(normalized_control_targets.values())) != len(normalized_control_targets):
        raise TrustedLauncherError("original control targets are duplicated")
    collision = set(normalized_read_sources) & set(normalized_control_targets.values())
    if collision:
        raise TrustedLauncherError(f"read/control bind target collision: {sorted(collision)}")
    python = by_name["python_executable"]["sandbox_path"]
    # Execute the admitted Python source directly; no shell-wrapper mapping is
    # part of the execution closure.
    worker = by_name["worker_source"]["sandbox_path"]
    cublas = by_name["cublas_library_directory"]["sandbox_path"]
    execution_bindings = _minimal_execution_bindings(mappings)
    consumed_descriptors = [
        *(normalized_mapping_sources[row["name"]] for row in execution_bindings),
        *(normalized_host_abi_sources[target] for target in sorted(normalized_host_abi_sources)),
        manifest_descriptor,
        *(
            normalized_control_sources[name][role]
            for name in sorted(normalized_control_sources)
            for role in ("control", "work_order")
        ),
        lineage_descriptor,
        *(normalized_read_sources[target] for target in sorted(normalized_read_sources)),
        attestation_descriptor,
        *(normalized_writable_sources[name] for name in ("result", "event", "lock")),
    ]
    if len(consumed_descriptors) != len(set(consumed_descriptors)):
        raise TrustedLauncherError(
            "each Bubblewrap descriptor bind must consume one unique descriptor"
        )
    all_targets = [
        *(row["sandbox_path"] for row in mappings),
        *normalized_host_abi_sources,
        f"{INPUT_ROOT}/batch.json",
        *controls.values(),
        lineage_target,
        *normalized_control_targets.values(),
        *normalized_read_sources,
        f"{CONTROL_ROOT}/launch-attestation.json",
        *writable_targets.values(),
        *launcher_profile["sandbox"]["system_library_directories"],
        *launcher_profile["sandbox"]["system_readonly_files"],
        *gpu_devices,
        "/proc",
        "/dev",
        "/tmp",
        "/run",
    ]
    argv = [
        launcher_profile["system_tools"]["bubblewrap"]["path"],
        "--die-with-parent",
        "--new-session",
        "--unshare-all",
        # Bubblewrap 0.11 requires an explicit --unshare-user token before
        # --disable-userns; --unshare-all alone does not satisfy that check.
        "--unshare-user",
        "--uid",
        str(os.geteuid()),
        "--gid",
        str(os.getegid()),
        "--disable-userns",
        "--proc",
        "/proc",
        "--dev",
        "/dev",
        "--tmpfs",
        "/tmp",
        "--tmpfs",
        "/run",
    ]
    for directory in _parent_directories(all_targets):
        if directory not in {"/proc", "/dev", "/tmp", "/run"}:
            argv.extend(["--dir", directory])
    if launcher_profile["sandbox"]["system_library_directories"]:
        raise TrustedLauncherError("broad system-library binds are forbidden")
    argv.extend(["--symlink", "usr/lib64", "/lib64"])
    if launcher_profile["sandbox"]["system_readonly_files"]:
        raise TrustedLauncherError("mutable host loader files are forbidden")
    for target in sorted(normalized_host_abi_sources):
        argv.extend(
            ["--ro-bind-fd", str(normalized_host_abi_sources[target]), target]
        )
    # Bind three admitted directory roots once.  Their nested logical mappings
    # remain identity/evidence but do not become redundant mount operations.
    for row in execution_bindings:
        argv.extend(
            [
                "--ro-bind-fd",
                str(normalized_mapping_sources[row["name"]]),
                row["sandbox_path"],
            ]
        )
    argv.extend(
        ["--ro-bind-fd", str(manifest_descriptor), f"{INPUT_ROOT}/batch.json"]
    )
    for name in sorted(controls):
        argv.extend(
            [
                "--ro-bind-fd",
                str(normalized_control_sources[name]["control"]),
                controls[name],
            ]
        )
        argv.extend(
            [
                "--ro-bind-fd",
                str(normalized_control_sources[name]["work_order"]),
                normalized_control_targets[name],
            ]
        )
    argv.extend(["--ro-bind-fd", str(lineage_descriptor), lineage_target])
    for target in sorted(normalized_read_sources):
        argv.extend(
            ["--ro-bind-fd", str(normalized_read_sources[target]), target]
        )
    argv.extend(
        [
            "--ro-bind-fd",
            str(attestation_descriptor),
            f"{CONTROL_ROOT}/launch-attestation.json",
        ]
    )
    for name in ("result", "event", "lock"):
        argv.extend(
            [
                "--bind-fd",
                str(normalized_writable_sources[name]),
                writable_targets[name],
            ]
        )
    for device in gpu_devices:
        argv.extend(["--dev-bind", device, device])
    # Bubblewrap's synthetic --dir parents are owned by the sandbox UID and are
    # writable unless the completed mount graph is sealed.  A non-recursive
    # root remount leaves the three explicit writable submounts usable while
    # preventing unmanifested DSO/control creation in their parents.
    argv.extend(["--remount-ro", "/"])
    environment = {
        "CUDA_DEVICE_ORDER": "PCI_BUS_ID",
        # CUDA accepts UUID selectors.  nvidia-smi's numeric index is only an
        # observation and is not guaranteed to be CUDA's ordinal.
        "CUDA_VISIBLE_DEVICES": gpu_uuid,
        "DO_NOT_TRACK": "1",
        "HF_DATASETS_OFFLINE": "1",
        "HF_HUB_DISABLE_TELEMETRY": "1",
        "HF_HUB_OFFLINE": "1",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "LD_LIBRARY_PATH": f"{cublas}:/usr/lib64",
        "PATH": str(Path(python).parent),
        "PYTHONNOUSERSITE": "1",
    }
    argv.append("--clearenv")
    for name, value in sorted(environment.items()):
        argv.extend(["--setenv", name, value])
    argv.extend(
        [
            "--chdir",
            "/",
            "--",
            python,
            "-B",
            "-I",
            worker,
            "run",
            "--batch-manifest",
            f"{INPUT_ROOT}/batch.json",
            "--expected-batch-sha256",
            _digest(batch_manifest_sha256, "batch manifest SHA-256"),
            "--runtime-admission",
            controls["runtime"],
            "--expected-runtime-admission-sha256",
            _digest(runtime_sha256, "runtime admission SHA-256"),
            "--production-profile",
            controls["profile"],
            "--expected-production-profile-sha256",
            _digest(profile_sha256, "production profile SHA-256"),
            "--root-registration",
            controls["root"],
            "--expected-root-registration-sha256",
            _digest(root_sha256, "root registration SHA-256"),
            "--lineage-preflight-attestation",
            lineage_target,
            "--expected-lineage-preflight-attestation-sha256",
            _digest(lineage_preflight_sha256, "lineage preflight SHA-256"),
            "--launch-attestation",
            f"{CONTROL_ROOT}/launch-attestation.json",
            "--expected-launch-attestation-sha256",
            _digest(attestation_sha256, "launch attestation SHA-256"),
        ]
    )
    if any(argv[index:index + 3] == ["--ro-bind", "/", "/"] for index in range(len(argv) - 2)):
        raise TrustedLauncherError("sandbox may not bind the host root")
    if argv.count("--remount-ro") != 1:
        raise TrustedLauncherError("sandbox root must be remounted read-only exactly once")
    return argv


def build_lineage_preflight_bwrap_argv(
    *,
    launcher_profile: dict[str, Any],
    mappings: Sequence[dict[str, str]],
    mapping_sources: dict[str, int],
    host_abi_sources: dict[str, int],
    mode: str,
    root_source: int,
    root_target: str,
    manifest_source: int,
    profile_source: int,
    root_control_source: int,
    root_control_target: str,
    candidate_read_sources: dict[str, int],
    output_directory_source: int,
    batch_manifest_sha256: str,
    profile_sha256: str,
    root_sha256: str,
) -> list[str]:
    """Construct the short, GPU-less lineage-replay namespace."""

    by_name = {row["name"]: row for row in mappings}
    if set(by_name) != REQUIRED_MAPPING_NAMES or set(mapping_sources) != REQUIRED_MAPPING_NAMES:
        raise TrustedLauncherError("lineage preflight mapping closure is incomplete")
    normalized_mapping_sources = {
        name: _descriptor_number(value, f"preflight mapping {name} descriptor")
        for name, value in mapping_sources.items()
    }
    normalized_host_abi_sources: dict[str, int] = {}
    for target, source in host_abi_sources.items():
        target_path = normalized_absolute_path(target, "preflight host ABI target")
        if str(target_path) != target or target_path.parent != Path("/usr/lib64"):
            raise TrustedLauncherError("preflight host ABI target is not normalized")
        normalized_host_abi_sources[target] = _descriptor_number(
            source, f"preflight host ABI {target} descriptor"
        )
    if not normalized_host_abi_sources:
        raise TrustedLauncherError("lineage preflight host ABI closure is empty")
    root_descriptor = _descriptor_number(root_source, "preflight hot-root descriptor")
    manifest_descriptor = _descriptor_number(
        manifest_source, "preflight batch manifest descriptor"
    )
    profile_descriptor = _descriptor_number(
        profile_source, "preflight production profile descriptor"
    )
    root_control_descriptor = _descriptor_number(
        root_control_source, "preflight root-registration descriptor"
    )
    output_descriptor = _descriptor_number(
        output_directory_source, "preflight output-directory descriptor"
    )
    normalized_candidate_sources = {
        target: _descriptor_number(
            source, f"preflight synthetic binding {target} descriptor"
        )
        for target, source in candidate_read_sources.items()
    }
    production_lineage = mode in {MODE_PRODUCTION, MODE_LOCAL_PRIVATE}
    root_trusted = mode == MODE_PRODUCTION
    if production_lineage and normalized_candidate_sources:
        raise TrustedLauncherError(
            "production-lineage preflight may not add synthetic read binds"
        )
    if not production_lineage and not normalized_candidate_sources:
        raise TrustedLauncherError(
            "non-root preflight requires exact lineage/audio descriptor binds"
        )
    python = by_name["python_executable"]["sandbox_path"]
    worker = by_name["worker_source"]["sandbox_path"]
    controls = {
        "batch": f"{INPUT_ROOT}/batch.json",
        "profile": f"{CONTROL_ROOT}/profile.json",
        "root": f"{CONTROL_ROOT}/root.json",
        "output": "/run/himr-gpu/preflight",
    }
    if production_lineage:
        original_root_control = normalized_absolute_path(
            root_control_target, "preflight original root-registration target"
        )
        normalized_hot_root = normalized_absolute_path(
            root_target, "preflight hot-root target"
        )
        _hot_descendant(
            original_root_control,
            normalized_hot_root,
            "preflight original root-registration target",
        )
        controls["root"] = str(original_root_control)
    preflight_mappings = [
        by_name["application_root"],
        by_name["application_support_root"],
        by_name["runtime_root"],
    ]
    consumed_descriptors = [
        *(normalized_mapping_sources[row["name"]] for row in preflight_mappings),
        *(normalized_host_abi_sources[target] for target in sorted(normalized_host_abi_sources)),
        manifest_descriptor,
        profile_descriptor,
        root_control_descriptor,
        *([root_descriptor] if production_lineage else []),
        *(
            normalized_candidate_sources[target]
            for target in sorted(normalized_candidate_sources)
        ),
        output_descriptor,
    ]
    if len(consumed_descriptors) != len(set(consumed_descriptors)):
        raise TrustedLauncherError(
            "each preflight descriptor bind must consume one unique descriptor"
        )
    targets = [
        *(row["sandbox_path"] for row in preflight_mappings),
        *normalized_host_abi_sources,
        *controls.values(),
        *launcher_profile["sandbox"]["system_library_directories"],
        *launcher_profile["sandbox"]["system_readonly_files"],
        "/proc",
        "/dev",
        "/tmp",
        "/run",
    ]
    if production_lineage:
        normalized_root = str(normalized_absolute_path(root_target, "preflight hot-root target"))
        targets.append(normalized_root)
    else:
        normalized_root = root_target
        for target in normalized_candidate_sources:
            targets.append(str(normalized_absolute_path(target, "synthetic preflight target")))
    argv = [
        launcher_profile["system_tools"]["bubblewrap"]["path"],
        "--die-with-parent",
        "--new-session",
        "--unshare-all",
        "--unshare-user",
        "--uid",
        str(os.geteuid()),
        "--gid",
        str(os.getegid()),
        "--disable-userns",
        "--proc",
        "/proc",
        "--dev",
        "/dev",
        "--tmpfs",
        "/tmp",
        "--tmpfs",
        "/run",
    ]
    for directory in _parent_directories(targets):
        if directory not in {"/proc", "/dev", "/tmp", "/run"}:
            argv.extend(["--dir", directory])
    if launcher_profile["sandbox"]["system_library_directories"]:
        raise TrustedLauncherError("preflight broad system-library binds are forbidden")
    argv.extend(["--symlink", "usr/lib64", "/lib64"])
    if launcher_profile["sandbox"]["system_readonly_files"]:
        raise TrustedLauncherError("preflight mutable host loader files are forbidden")
    for target in sorted(normalized_host_abi_sources):
        argv.extend(
            ["--ro-bind-fd", str(normalized_host_abi_sources[target]), target]
        )
    for row in preflight_mappings:
        argv.extend(
            [
                "--ro-bind-fd",
                str(normalized_mapping_sources[row["name"]]),
                row["sandbox_path"],
            ]
        )
    argv.extend(["--ro-bind-fd", str(manifest_descriptor), controls["batch"]])
    argv.extend(["--ro-bind-fd", str(profile_descriptor), controls["profile"]])
    if production_lineage:
        argv.extend(["--ro-bind-fd", str(root_descriptor), normalized_root])
    else:
        for target in sorted(normalized_candidate_sources):
            argv.extend(
                [
                    "--ro-bind-fd",
                    str(normalized_candidate_sources[target]),
                    target,
                ]
            )
    # The retained registration descriptor is the byte-exact authority.  For
    # production lineage it must be mounted *after* its containing hot root so
    # the parent mount cannot obscure the retained child mount.
    argv.extend(
        ["--ro-bind-fd", str(root_control_descriptor), controls["root"]]
    )
    argv.extend(["--bind-fd", str(output_descriptor), controls["output"]])
    argv.extend(["--remount-ro", "/"])
    environment = {
        "DO_NOT_TRACK": "1",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": str(Path(python).parent),
        "PYTHONNOUSERSITE": "1",
    }
    argv.append("--clearenv")
    for name, value in sorted(environment.items()):
        argv.extend(["--setenv", name, value])
    output_path = f"{controls['output']}/lineage-preflight.json"
    argv.extend(
        [
            "--chdir",
            "/",
            "--",
            python,
            "-B",
            "-I",
            worker,
            "preflight-lineage",
            "--batch-manifest",
            controls["batch"],
            "--expected-batch-sha256",
            _digest(batch_manifest_sha256, "preflight batch manifest SHA-256"),
            "--production-profile",
            controls["profile"],
            "--expected-production-profile-sha256",
            _digest(profile_sha256, "preflight production profile SHA-256"),
            "--root-registration",
            controls["root"],
            "--expected-root-registration-sha256",
            _digest(root_sha256, "preflight root registration SHA-256"),
            "--lineage-attestation-output",
            output_path,
        ]
    )
    if any(argv[index:index + 3] == ["--ro-bind", "/", "/"] for index in range(len(argv) - 2)):
        raise TrustedLauncherError("lineage preflight may not bind the host root")
    if argv.count("--remount-ro") != 1:
        raise TrustedLauncherError(
            "lineage preflight root must be remounted read-only exactly once"
        )
    if not production_lineage and any(
        argv[index : index + 3]
        == ["--ro-bind-fd", str(root_descriptor), root_target]
        for index in range(len(argv) - 2)
    ):
        raise TrustedLauncherError("synthetic lineage preflight may not expose the hot root")
    if mode == MODE_LOCAL_PRIVATE and root_trusted:
        raise TrustedLauncherError("local-private preflight cannot claim a root trust anchor")
    if any("/dev/nvidia" in value for value in argv):
        raise TrustedLauncherError("lineage preflight may not expose a GPU")
    return argv


def _run_bounded_child(
    argv: Sequence[str],
    pass_fds: Sequence[int],
    *,
    maximum_seconds: int,
    maximum_output_bytes: int,
) -> tuple[int, bytes, bytes]:
    try:
        process = subprocess.Popen(
            list(argv),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
            pass_fds=tuple(sorted(set(pass_fds))),
            env=_clean_helper_environment(),
        )
    except OSError as error:
        raise TrustedLauncherError(f"cannot start lineage preflight: {error}") from error
    if process.stdout is None or process.stderr is None:  # pragma: no cover - Popen contract.
        _terminate(process)
        raise TrustedLauncherError("lineage preflight pipes are unavailable")
    streams = {process.stdout: bytearray(), process.stderr: bytearray()}
    selector = selectors.DefaultSelector()
    for stream in streams:
        os.set_blocking(stream.fileno(), False)
        selector.register(stream, selectors.EVENT_READ)
    deadline = time.monotonic() + maximum_seconds
    try:
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                _terminate(process)
                raise TrustedLauncherError("lineage preflight exceeded its wall bound")
            for key, _ in selector.select(min(0.25, remaining)):
                stream = key.fileobj
                chunk = os.read(stream.fileno(), 64 * 1024)
                if not chunk:
                    selector.unregister(stream)
                    continue
                streams[stream].extend(chunk)
                if sum(len(body) for body in streams.values()) > maximum_output_bytes:
                    _terminate(process)
                    raise TrustedLauncherError("lineage preflight output exceeds its bound")
        status = process.wait(timeout=max(0.1, deadline - time.monotonic()))
        return status, bytes(streams[process.stdout]), bytes(streams[process.stderr])
    except BaseException:
        _terminate(process)
        raise
    finally:
        selector.close()
        process.stdout.close()
        process.stderr.close()


def _mount_present(mountpoint: Path) -> bool:
    try:
        body = Path("/proc/self/mountinfo").read_bytes()
    except OSError as error:
        raise TrustedLauncherError(f"cannot inspect mount table: {error}") from error
    if len(body) > 16 * 1024 * 1024:
        raise TrustedLauncherError("mount table exceeds its bound")
    escaped = str(mountpoint).replace(" ", "\\040")
    for line in body.decode("utf-8", errors="strict").splitlines():
        fields = line.split()
        if len(fields) >= 10 and fields[4] == escaped:
            separator = fields.index("-") if "-" in fields else -1
            if separator < 0 or separator + 1 >= len(fields):
                raise TrustedLauncherError("mounted image has malformed mount metadata")
            if fields[separator + 1] not in {
                "fuse.squashfuse",
                "fuse.squashfuse_ll",
                "squashfs",
                "fuse",
            }:
                raise TrustedLauncherError("image mount filesystem type is unsupported")
            return True
    return False


def _clean_helper_environment() -> dict[str, str]:
    return {"PATH": "/usr/bin", "LANG": "C", "LC_ALL": "C"}


def _parent_death_setup(expected_parent: int) -> Any:
    """Return the minimal pre-exec hook which makes foreground FUSE non-orphanable."""

    def arm() -> None:
        libc = ctypes.CDLL(None, use_errno=True)
        result = libc.prctl(
            PR_SET_PDEATHSIG,
            signal.SIGKILL,
            0,
            0,
            0,
        )
        if result != 0 or os.getppid() != expected_parent:
            os._exit(PDEATHSIG_FAILURE_STATUS)

    return arm


def start_squashfuse(tool: str, image: RetainedFile, mountpoint: Path) -> subprocess.Popen[bytes]:
    command = [tool, "-f", image.fd_path, str(mountpoint)]
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            pass_fds=(image.descriptor,),
            env=_clean_helper_environment(),
            start_new_session=True,
            preexec_fn=_parent_death_setup(os.getpid()),
        )
    except OSError as error:
        raise TrustedLauncherError(f"cannot start squashfuse: {error}") from error
    deadline = time.monotonic() + MOUNT_READY_SECONDS
    while time.monotonic() < deadline:
        status = process.poll()
        if status is not None:
            raise TrustedLauncherError(f"squashfuse exited before mount readiness ({status})")
        if _mount_present(mountpoint):
            return process
        time.sleep(0.025)
    process.terminate()
    try:
        process.wait(PROCESS_TERMINATE_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(PROCESS_TERMINATE_SECONDS)
    raise TrustedLauncherError("squashfuse mount readiness timed out")


def _private_runtime_root(base: Path | None = None) -> Path:
    parent = Path(f"/run/user/{os.geteuid()}") if base is None else base
    _reject_symlink_components(parent, "launcher runtime parent")
    info = parent.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise TrustedLauncherError(
            "launcher runtime parent must be current-user-owned mode 0700"
        )
    root = parent / PRIVATE_RUNTIME_DIRECTORY
    try:
        root.mkdir(mode=0o700)
    except FileExistsError:
        pass
    _reject_symlink_components(root, "launcher private runtime root")
    observed = root.lstat()
    if (
        not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        raise TrustedLauncherError("launcher private runtime root is unsafe")
    return root


def _acquire_launcher_lock(runtime_root: Path) -> int:
    descriptor = os.open(
        runtime_root / LAUNCHER_LOCK_FILE,
        os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW,
        0o600,
    )
    try:
        info = os.fstat(descriptor)
        lexical = (runtime_root / LAUNCHER_LOCK_FILE).lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
            or _identity(info) != _identity(lexical)
        ):
            raise TrustedLauncherError("launcher lock inode is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise TrustedLauncherError("another trusted launcher owns the runtime") from error
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _unmount(tool: str, mountpoint: Path) -> bool:
    completed = subprocess.run(
        [tool, "-u", str(mountpoint)],
        check=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=5,
        env=_clean_helper_environment(),
    )
    return completed.returncode == 0


def cleanup_transient_root(
    transient: Path,
    fusermount: str,
    *,
    fuse_process: subprocess.Popen[Any] | None = None,
) -> None:
    """Recover/remove only the launcher's fixed, allowlisted transient tree."""

    if not transient.exists() and not transient.is_symlink():
        _terminate(fuse_process)
        return
    _reject_symlink_components(transient, "launcher transient root")
    info = transient.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise TrustedLauncherError("launcher transient root is unsafe")
    allowed = {MOUNT_DIRECTORY, ATTESTATION_FILE, PREFLIGHT_DIRECTORY}
    observed_names = {entry.name for entry in transient.iterdir()}
    if not observed_names <= allowed:
        raise TrustedLauncherError("launcher transient root contains an unknown entry")
    mountpoint = transient / MOUNT_DIRECTORY
    if mountpoint.exists() and _mount_present(mountpoint):
        if not _unmount(fusermount, mountpoint):
            _terminate(fuse_process)
            fuse_process = None
            if _mount_present(mountpoint) and not _unmount(fusermount, mountpoint):
                raise TrustedLauncherError("clean SquashFS unmount failed")
        if _mount_present(mountpoint):
            raise TrustedLauncherError("SquashFS mount remains after unmount")
    _terminate(fuse_process)
    for name in (ATTESTATION_FILE,):
        path = transient / name
        if path.exists() or path.is_symlink():
            file_info = path.lstat()
            if (
                stat.S_ISLNK(file_info.st_mode)
                or not stat.S_ISREG(file_info.st_mode)
                or file_info.st_uid != os.geteuid()
                or file_info.st_nlink != 1
                or stat.S_IMODE(file_info.st_mode) != 0o400
            ):
                raise TrustedLauncherError(f"launcher transient {name} is unsafe")
            path.unlink()
    preflight = transient / PREFLIGHT_DIRECTORY
    if preflight.exists() or preflight.is_symlink():
        preflight_info = preflight.lstat()
        if (
            stat.S_ISLNK(preflight_info.st_mode)
            or not stat.S_ISDIR(preflight_info.st_mode)
            or preflight_info.st_uid != os.geteuid()
            or stat.S_IMODE(preflight_info.st_mode) != 0o700
        ):
            raise TrustedLauncherError("launcher preflight output directory is unsafe")
        output = preflight / "lineage-preflight.json"
        names = {entry.name for entry in preflight.iterdir()}
        if not names <= {"lineage-preflight.json"}:
            raise TrustedLauncherError("launcher preflight directory contains an unknown entry")
        if output.exists() or output.is_symlink():
            output_info = output.lstat()
            if (
                stat.S_ISLNK(output_info.st_mode)
                or not stat.S_ISREG(output_info.st_mode)
                or output_info.st_uid != os.geteuid()
                or output_info.st_nlink != 1
                or stat.S_IMODE(output_info.st_mode) != 0o400
            ):
                raise TrustedLauncherError("lineage preflight output is unsafe")
            output.unlink()
        preflight.rmdir()
    if mountpoint.exists() or mountpoint.is_symlink():
        mount_info = mountpoint.lstat()
        if (
            stat.S_ISLNK(mount_info.st_mode)
            or not stat.S_ISDIR(mount_info.st_mode)
            or mount_info.st_uid != os.geteuid()
            or stat.S_IMODE(mount_info.st_mode) != 0o700
            or any(mountpoint.iterdir())
        ):
            raise TrustedLauncherError("launcher mountpoint is unsafe or nonempty")
        mountpoint.rmdir()
    transient.rmdir()


def prepare_transient_root(
    fusermount: str, *, runtime_parent: Path | None = None
) -> tuple[Path, int]:
    runtime_root = _private_runtime_root(runtime_parent)
    lock_descriptor = _acquire_launcher_lock(runtime_root)
    transient = runtime_root / TRANSIENT_DIRECTORY
    try:
        cleanup_transient_root(transient, fusermount)
        transient.mkdir(mode=0o700)
        mountpoint = transient / MOUNT_DIRECTORY
        mountpoint.mkdir(mode=0o700)
        (transient / PREFLIGHT_DIRECTORY).mkdir(mode=0o700)
        return transient, lock_descriptor
    except Exception:
        with contextlib.suppress(OSError):
            fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
        os.close(lock_descriptor)
        raise


def _open_mapping_beneath(
    root_descriptor: int,
    relative: PurePosixPath,
    *,
    is_directory: bool,
    label: str,
) -> tuple[int, os.stat_result]:
    """Open a mapping component-by-component without following any symlink."""

    current = os.dup(root_descriptor)
    try:
        for index, component in enumerate(relative.parts):
            final = index == len(relative.parts) - 1
            flags = (
                os.O_RDONLY
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0)
            )
            if not final or is_directory:
                flags |= os.O_DIRECTORY
            child = os.open(component, flags, dir_fd=current)
            os.close(current)
            current = child
        info = os.fstat(current)
        if (is_directory and not stat.S_ISDIR(info.st_mode)) or (
            not is_directory and not stat.S_ISREG(info.st_mode)
        ):
            raise TrustedLauncherError(f"{label} has the wrong file kind")
        return current, info
    except Exception:
        with contextlib.suppress(OSError):
            os.close(current)
        raise


def verify_mounted_mappings(
    mountpoint: Path, mappings: Sequence[dict[str, str]]
) -> dict[str, RetainedMapping]:
    sources: dict[str, RetainedMapping] = {}
    directory_roles = {
        "application_root",
        "application_support_root",
        "directory",
        "runtime_root",
        "model_bundle",
        "model_root",
        "shared_library_directory",
    }
    _reject_symlink_components(mountpoint, "execution image mountpoint")
    root_descriptor = os.open(
        mountpoint, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_DIRECTORY
    )
    try:
        for row in mappings:
            relative = normalized_relative_path(
                row["image_relative_path"], f"mounted mapping {row['name']} path"
            )
            source = mountpoint.joinpath(*relative.parts)
            is_directory = row["role"].endswith("directory") or row["role"] in directory_roles
            try:
                descriptor, info = _open_mapping_beneath(
                    root_descriptor,
                    relative,
                    is_directory=is_directory,
                    label=f"mounted mapping {row['name']}",
                )
            except OSError as error:
                raise TrustedLauncherError(
                    f"mounted mapping {row['name']} is unavailable: {error}"
                ) from error
            mode = stat.S_IMODE(info.st_mode)
            if mode & 0o222 or (row["role"] == "executable" and not mode & 0o111):
                os.close(descriptor)
                raise TrustedLauncherError(
                    f"mounted mapping {row['name']} has an unsafe mode"
                )
            retained = RetainedMapping(
                row["name"], source, descriptor, info, is_directory
            )
            retained.verify()
            sources[row["name"]] = retained
        return sources
    except Exception:
        for retained in sources.values():
            retained.close()
        raise
    finally:
        os.close(root_descriptor)


def _minimal_execution_bindings(
    mappings: Sequence[dict[str, str]],
) -> list[dict[str, str]]:
    by_name = {row["name"]: row for row in mappings}
    coverage = {
        "application_root": {
            "adapter_source",
            "worker_source",
            "verified_loader",
            "model_admission_helper",
            "runtime_admission_helper",
        },
        "runtime_root": {
            "python_executable",
            "cublas_library_directory",
        },
        "model_bundle": {"model_root"},
    }
    for parent_name, child_names in coverage.items():
        parent = by_name[parent_name]
        parent_image = PurePosixPath(parent["image_relative_path"])
        parent_sandbox = PurePosixPath(parent["sandbox_path"])
        for child_name in child_names:
            child = by_name[child_name]
            if (
                parent_image not in PurePosixPath(child["image_relative_path"]).parents
                or parent_sandbox not in PurePosixPath(child["sandbox_path"]).parents
            ):
                raise TrustedLauncherError(
                    f"logical mapping {child_name} is not covered by {parent_name}"
                )
    return [by_name[name] for name in (
        "application_root",
        "application_support_root",
        "runtime_root",
        "model_bundle",
    )]


def _safe_writable_root(
    path_value: str, label: str, hot_root: Path, *, allow_runtime: bool = False
) -> tuple[Path, int, os.stat_result]:
    path = normalized_absolute_path(path_value, label)
    _reject_symlink_components(path, label)
    if not _is_within(path, hot_root):
        expected_runtime = Path(f"/run/user/{os.geteuid()}")
        if not allow_runtime or not _is_within(path, expected_runtime):
            raise TrustedLauncherError(f"{label} must be inside the registered hot root")
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise TrustedLauncherError(f"{label} must be a current-user-owned mode-0700 directory")
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_DIRECTORY)
    if _writable_directory_authority_identity(
        os.fstat(descriptor)
    ) != _writable_directory_authority_identity(info):
        os.close(descriptor)
        raise TrustedLauncherError(f"{label} changed while opened")
    return path, descriptor, info


def _write_new(path: Path, body: bytes, mode: int) -> None:
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"refusing to replace {path}")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    try:
        offset = 0
        while offset < len(body):
            offset += os.write(descriptor, body[offset:])
        os.fsync(descriptor)
        os.fchmod(descriptor, mode)
    finally:
        os.close(descriptor)


def _terminate(process: subprocess.Popen[Any] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(PROCESS_TERMINATE_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(PROCESS_TERMINATE_SECONDS)


def _run_child(argv: Sequence[str], pass_fds: Sequence[int], maximum_seconds: int) -> int:
    try:
        process = subprocess.Popen(
            list(argv),
            close_fds=True,
            pass_fds=tuple(sorted(set(pass_fds))),
            start_new_session=False,
            env=_clean_helper_environment(),
        )
    except OSError as error:
        raise TrustedLauncherError(f"cannot start Bubblewrap: {error}") from error
    try:
        return process.wait(timeout=maximum_seconds)
    except subprocess.TimeoutExpired as error:
        _terminate(process)
        raise TrustedLauncherError("GPU batch exceeded its complete launch wall bound") from error
    except BaseException:
        _terminate(process)
        raise


def _load_control(
    path: str,
    sha256: str,
    label: str,
    *,
    production_root_owned: bool,
    local_user_owned: bool = False,
) -> tuple[RetainedFile, Any]:
    if production_root_owned and local_user_owned:
        raise TrustedLauncherError("control ownership policy is contradictory")
    allowed = (
        {(0, 0o444)}
        if production_root_owned
        else {(os.geteuid(), 0o400)}
        if local_user_owned
        else {(os.geteuid(), 0o400), (0, 0o444)}
    )
    retained = retain_file(
        path,
        label,
        expected_sha256=sha256,
        maximum=MAX_JSON_BYTES,
        allowed_owner_modes=allowed,
        keep_body=True,
    )
    if production_root_owned:
        _trusted_ancestors(retained.path)
    return retained, _canonical_document(retained, label)


def launch(args: argparse.Namespace) -> int:
    production = args.mode == MODE_PRODUCTION
    local_private = args.mode == MODE_LOCAL_PRIVATE
    resource_enforced = production or local_private
    readiness_path_arg = getattr(args, "local_readiness", None)
    readiness_sha_arg = getattr(args, "expected_local_readiness_sha256", None)
    if local_private and (not readiness_path_arg or not readiness_sha_arg):
        raise TrustedLauncherError(
            "local-private production requires a passed readiness receipt"
        )
    if not local_private and (readiness_path_arg is not None or readiness_sha_arg is not None):
        raise TrustedLauncherError(
            "only local-private production may bind a local readiness receipt"
        )
    if resource_enforced and (
        sys.flags.isolated != 1
        or sys.flags.no_user_site != 1
        or not sys.dont_write_bytecode
    ):
        raise TrustedLauncherError(
            f"{args.mode} launcher must start with Python -IB isolation"
        )
    os.umask(0o077)
    retained_files: list[RetainedFile] = []
    retained_mappings: dict[str, RetainedMapping] = {}
    writable_descriptors: list[int] = []
    duplicated_descriptors: list[int] = []
    writable_observations: dict[str, tuple[Path, int, os.stat_result]] = {}
    root: RetainedRoot | None = None
    readiness_file: RetainedFile | None = None
    readiness_value: dict[str, Any] | None = None
    host_abi_root: RetainedHostABIRoot | None = None
    fuse_process: subprocess.Popen[bytes] | None = None
    temporary_root: Path | None = None
    launcher_lock_descriptor: int | None = None
    preflight_output_descriptor: int | None = None
    attestation_fd: int | None = None
    previous_handlers: dict[int, Any] = {}

    def interrupted(signum: int, _frame: Any) -> None:
        raise LaunchInterrupted(f"launcher interrupted by signal {signum}")

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[signum] = signal.signal(signum, interrupted)
    try:
        profile_file, profile_value = _load_control(
            args.launcher_profile,
            args.expected_launcher_profile_sha256,
            "launcher profile",
            production_root_owned=production,
            local_user_owned=local_private,
        )
        retained_files.append(profile_file)
        launcher_profile = validate_launcher_profile(profile_value)
        current_launcher = normalized_absolute_path(__file__, "running launcher path")
        _reject_symlink_components(current_launcher, "running launcher")
        if str(current_launcher) != launcher_profile["launcher"]["path"]:
            raise TrustedLauncherError("running launcher path differs from the installed profile")
        launcher_mode = (
            0o555
            if production
            else 0o500
            if local_private
            else stat.S_IMODE(current_launcher.lstat().st_mode)
        )
        with retain_file(
            current_launcher,
            "trusted launcher",
            expected_sha256=launcher_profile["launcher"]["sha256"],
            maximum=MAX_TOOL_BYTES,
            allowed_owner_modes=(
                {(0, 0o555)}
                if production
                else {(os.geteuid(), launcher_mode), (0, 0o555)}
            ),
            keep_body=False,
        ) as launcher_file:
            if production:
                _trusted_ancestors(current_launcher)
            del launcher_file
        for name, reference in launcher_profile["system_tools"].items():
            observed = _observed_tool(reference["path"], f"system tool {name}")
            if observed != reference:
                raise TrustedLauncherError(f"system tool {name} differs from launcher profile")
        if production and (
            os.path.normpath(sys.executable)
            != launcher_profile["system_tools"]["host_python"]["path"]
        ):
            raise TrustedLauncherError(
                "running Python differs from the pinned isolated host interpreter"
            )

        runtime_file, runtime_value = _load_control(
            args.runtime_admission,
            args.expected_runtime_admission_sha256,
            "runtime admission",
            production_root_owned=production,
            local_user_owned=local_private,
        )
        retained_files.append(runtime_file)
        if (production or local_private) and str(runtime_file.path) != launcher_profile["runtime_admission_install_path"]:
            raise TrustedLauncherError(f"{args.mode} runtime receipt is not at its bound path")
        runtime_receipt = validate_runtime_receipt(
            runtime_value,
            mode=args.mode,
            launcher_profile=launcher_profile,
            launcher_profile_path=str(profile_file.path),
            profile_sha256=profile_file.sha256,
        )

        production_file, production_value = _load_control(
            args.production_profile,
            args.expected_production_profile_sha256,
            "production profile",
            production_root_owned=False,
            local_user_owned=local_private,
        )
        retained_files.append(production_file)
        production_profile = validate_production_profile(production_value)
        expected_profile = launcher_profile["production_profile"]
        if str(production_file.path) != expected_profile["path"] or production_file.sha256 != expected_profile["sha256"] or production_profile["identity_sha256"] != expected_profile["identity_sha256"]:
            raise TrustedLauncherError("production profile differs from launcher binding")
        host_envelope = observe_host_envelope(
            production_profile, enforce=resource_enforced
        )

        root_file, root_value = _load_control(
            args.root_registration,
            args.expected_root_registration_sha256,
            "root registration",
            production_root_owned=False,
            local_user_owned=local_private,
        )
        retained_files.append(root_file)
        registration = validate_root_registration(root_value)
        expected_root = launcher_profile["root_registration"]
        if str(root_file.path) != expected_root["path"] or root_file.sha256 != expected_root["sha256"] or any(registration[key] != expected_root[key] for key in ("identity_sha256", "registration_id", "root_id")):
            raise TrustedLauncherError("root registration differs from launcher binding")
        root = retain_root(registration)
        hot_root = Path(registration["path"])

        if local_private:
            readiness_file, readiness_document = _load_control(
                readiness_path_arg,
                readiness_sha_arg,
                "local readiness receipt",
                production_root_owned=False,
                local_user_owned=True,
            )
            retained_files.append(readiness_file)
            readiness_value = validate_local_readiness(readiness_document)
            if (
                readiness_value["launcher"]
                != {
                    **launcher_profile["launcher"],
                    "profile": {
                        "path": str(profile_file.path),
                        "sha256": profile_file.sha256,
                        "identity_sha256": launcher_profile["identity_sha256"],
                    },
                }
                or readiness_value["runtime_admission"]
                != {
                    "path": str(runtime_file.path),
                    "sha256": runtime_file.sha256,
                    "identity_sha256": runtime_receipt["identity_sha256"],
                    "status": runtime_receipt["status"],
                }
                or readiness_value["execution_image"]
                != launcher_profile["execution_image"]
                or readiness_value["production_profile"]
                != {
                    "path": str(production_file.path),
                    "sha256": production_file.sha256,
                    "identity_sha256": production_profile["identity_sha256"],
                }
                or readiness_value["root_registration"]
                != {
                    "path": str(root_file.path),
                    "sha256": root_file.sha256,
                    "identity_sha256": registration["identity_sha256"],
                    "registration_id": registration["registration_id"],
                }
                or readiness_value["host_abi_identity_sha256"]
                != launcher_profile["host_abi"]["identity_sha256"]
            ):
                raise TrustedLauncherError(
                    "local readiness receipt differs from current launch bindings"
                )

        manifest_file, manifest_value = _load_control(
            args.batch_manifest,
            args.expected_batch_sha256,
            "batch manifest",
            production_root_owned=False,
            local_user_owned=local_private,
        )
        retained_files.append(manifest_file)
        if production and (
            manifest_file.info.st_uid,
            stat.S_IMODE(manifest_file.info.st_mode),
        ) != (0, 0o444):
            raise TrustedLauncherError(
                "production batch manifest must be reviewed/root-owned mode 0444"
            )
        if not _is_within(manifest_file.path, hot_root):
            raise TrustedLauncherError("batch manifest must be inside the registered hot root")
        validate_batch_execution_class(manifest_value, args.mode)
        if manifest_value.get("production_profile") != manifest_value["items"][0]["work_order"]["production_profile"]:
            raise TrustedLauncherError("batch production profile projection is inconsistent")
        work_profile = manifest_value["production_profile"]
        if (
            work_profile.get("sha256") != production_file.sha256
            or work_profile.get("identity_sha256")
            != production_profile["identity_sha256"]
        ):
            raise TrustedLauncherError("batch profile differs from the retained control")
        work_runtime = manifest_value["runtime_admission"]
        if (
            work_runtime.get("receipt_sha256") != runtime_file.sha256
            or work_runtime.get("identity_sha256")
            != runtime_receipt["identity_sha256"]
            or work_runtime.get("status") != runtime_receipt["status"]
        ):
            raise TrustedLauncherError("batch runtime differs from the retained control")
        work_root = manifest_value["hot_root"]
        if (
            work_root.get("registration_sha256") != root_file.sha256
            or work_root.get("identity_sha256") != registration["identity_sha256"]
            or work_root.get("registration_id") != registration["registration_id"]
            or work_root.get("path") != str(hot_root)
        ):
            raise TrustedLauncherError("batch hot root differs from the retained control")
        read_groups = authorized_read_binding_specs(
            manifest_value, args.mode, hot_root
        )
        read_files, item_read_rows, read_files_by_path = retain_authorized_read_bindings(
            read_groups
        )
        retained_files.extend(read_files)

        image_expected = launcher_profile["execution_image"]
        image_allowed = (
            {(0, IMAGE_PRODUCTION_MODE)}
            if production
            else {(os.geteuid(), IMAGE_CANDIDATE_MODE)}
            if local_private
            else {(os.geteuid(), IMAGE_CANDIDATE_MODE), (0, IMAGE_PRODUCTION_MODE)}
        )
        image = retain_file(
            image_expected["path"],
            "execution image",
            expected_sha256=image_expected["sha256"],
            maximum=MAX_IMAGE_BYTES,
            allowed_owner_modes=image_allowed,
            keep_body=False,
        )
        retained_files.append(image)
        if image.info.st_size != image_expected["byte_count"]:
            raise TrustedLauncherError("execution image byte count differs from launcher profile")
        if production:
            _trusted_ancestors(image.path)

        gpu_uuid = production_profile["hardware"]["gpu_uuid"]
        hardware = production_profile["hardware"]
        gpu_observation = resolve_gpu_observation(
            launcher_profile["system_tools"]["nvidia_smi"]["path"],
            gpu_uuid,
            hardware["minimum_driver_version"],
            hardware["minimum_compute_capability"],
        )
        gpu_index = gpu_observation["host_index"]
        gpu_devices = validate_gpu_devices(
            gpu_observation["device_minor"],
            launcher_profile["sandbox"]["gpu_control_devices"],
        )
        (
            host_abi_files,
            host_abi_bindings,
            host_abi_summary,
            host_abi_root,
        ) = replay_host_abi_manifest(
            launcher_profile["host_abi"],
            observed_driver_version=gpu_observation["driver_version"],
        )
        retained_files.extend(host_abi_files)
        host_abi_sources: dict[str, int] = {}
        for binding, retained in zip(
            host_abi_bindings, host_abi_files, strict=True
        ):
            if binding["source_path"] != str(retained.path):
                raise TrustedLauncherError(
                    "host ABI binding/source descriptor order is inconsistent"
                )
            host_abi_sources[binding["target"]] = retained.descriptor

        writable: dict[str, Path] = {}
        for name, value, allow_runtime in (
            ("result", args.writable_result_root, False),
            ("event", args.writable_event_root, False),
            ("lock", args.writable_lock_root, True),
        ):
            path, descriptor, info = _safe_writable_root(
                value, f"writable {name} root", hot_root, allow_runtime=allow_runtime
            )
            writable[name] = path
            writable_descriptors.append(descriptor)
            writable_observations[name] = (path, descriptor, info)
        writable_values = list(writable.values())
        if len(set(writable_values)) != 3 or any(
            left in right.parents or right in left.parents
            for index, left in enumerate(writable_values)
            for right in writable_values[index + 1 :]
        ):
            raise TrustedLauncherError("writable roots must be distinct and non-nested")

        readonly_targets = {
            *read_files_by_path,
            str(manifest_file.path),
            work_runtime["receipt_path"],
            work_profile["path"],
            work_root["registration_path"],
            *(
                [str(readiness_file.path)]
                if readiness_file is not None
                else []
            ),
        }
        if any(
            Path(target) == writable_root
            or writable_root in Path(target).parents
            or Path(target) in writable_root.parents
            for target in readonly_targets
            for writable_root in writable_values
        ):
            raise TrustedLauncherError(
                "writable roots overlap an exact read/control binding"
            )

        temporary_root, launcher_lock_descriptor = prepare_transient_root(
            launcher_profile["system_tools"]["fusermount"]["path"]
        )
        mountpoint = temporary_root / MOUNT_DIRECTORY
        fuse_process = start_squashfuse(
            launcher_profile["system_tools"]["squashfuse"]["path"],
            image,
            mountpoint,
        )
        retained_mappings = verify_mounted_mappings(
            mountpoint, runtime_receipt["execution_image"]["logical_mappings"]
        )
        mapping_sources = {
            name: retained.descriptor
            for name, retained in retained_mappings.items()
        }

        preflight_output = temporary_root / PREFLIGHT_DIRECTORY
        preflight_output_descriptor = os.open(
            preflight_output,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_DIRECTORY,
        )
        production_lineage = args.mode in {MODE_PRODUCTION, MODE_LOCAL_PRIVATE}
        candidate_preflight_sources = (
            {}
            if production_lineage
            else {
                path: retained.descriptor
                for path, retained in sorted(read_files_by_path.items())
            }
        )
        preflight_argv = build_lineage_preflight_bwrap_argv(
            launcher_profile=launcher_profile,
            mappings=runtime_receipt["execution_image"]["logical_mappings"],
            mapping_sources=mapping_sources,
            host_abi_sources=host_abi_sources,
            mode=args.mode,
            root_source=root.descriptor,
            root_target=str(hot_root),
            manifest_source=manifest_file.descriptor,
            profile_source=production_file.descriptor,
            root_control_source=root_file.descriptor,
            root_control_target=work_root["registration_path"],
            candidate_read_sources=candidate_preflight_sources,
            output_directory_source=preflight_output_descriptor,
            batch_manifest_sha256=manifest_file.sha256,
            profile_sha256=production_file.sha256,
            root_sha256=root_file.sha256,
        )
        preflight_mapping_names = {
            "application_root",
            "application_support_root",
            "runtime_root",
        }
        preflight_pass_fds = [
            *([root.descriptor] if production_lineage else []),
            manifest_file.descriptor,
            production_file.descriptor,
            root_file.descriptor,
            preflight_output_descriptor,
            *(
                retained_mappings[name].descriptor
                for name in sorted(preflight_mapping_names)
            ),
            *(
                retained.descriptor
                for retained in (() if production_lineage else read_files)
            ),
            *(retained.descriptor for retained in host_abi_files),
        ]
        preflight_status, preflight_stdout, preflight_stderr = _run_bounded_child(
            preflight_argv,
            preflight_pass_fds,
            maximum_seconds=LINEAGE_PREFLIGHT_SECONDS,
            maximum_output_bytes=MAX_PREFLIGHT_STDIO_BYTES,
        )
        os.close(preflight_output_descriptor)
        preflight_output_descriptor = None
        if preflight_status != 0 or preflight_stderr:
            raise TrustedLauncherError(
                f"lineage preflight failed closed with status {preflight_status}"
            )
        lineage_path = preflight_output / "lineage-preflight.json"
        lineage_file = retain_file(
            lineage_path,
            "lineage preflight attestation",
            expected_sha256=None,
            maximum=MAX_LINEAGE_ATTESTATION_BYTES,
            allowed_owner_modes={(os.geteuid(), 0o400)},
            keep_body=True,
        )
        retained_files.append(lineage_file)
        lineage_value = _canonical_document(
            lineage_file, "lineage preflight attestation"
        )
        lineage_value = validate_lineage_preflight(
            lineage_value,
            manifest=manifest_value,
            manifest_sha256=manifest_file.sha256,
            production_profile=production_profile,
            root_registration=registration,
        )
        summary = parse_json(
            preflight_stdout.rstrip(b"\n"), "lineage preflight stdout"
        )
        expected_summary = {
            "status": "passed",
            "attestation_id": lineage_value["attestation_id"],
            "identity_sha256": lineage_value["identity_sha256"],
            "physical_sha256": lineage_file.sha256,
        }
        if summary != expected_summary:
            raise TrustedLauncherError("lineage preflight stdout differs from its output")

        control_bindings = {
            "runtime": {
                "path": str(runtime_file.path),
                "target": f"{CONTROL_ROOT}/runtime.json",
                "work_order_target": work_runtime["receipt_path"],
                "sha256": runtime_file.sha256,
                "identity_sha256": runtime_receipt["identity_sha256"],
            },
            "profile": {
                "path": str(production_file.path),
                "target": f"{CONTROL_ROOT}/profile.json",
                "work_order_target": work_profile["path"],
                "sha256": production_file.sha256,
                "identity_sha256": production_profile["identity_sha256"],
            },
            "root": {
                "path": str(root_file.path),
                "target": f"{CONTROL_ROOT}/root.json",
                "work_order_target": work_root["registration_path"],
                "sha256": root_file.sha256,
                "identity_sha256": registration["identity_sha256"],
            },
            "lineage_preflight": {
                "path": str(lineage_file.path),
                "target": f"{CONTROL_ROOT}/lineage-preflight.json",
                "work_order_target": None,
                "sha256": lineage_file.sha256,
                "identity_sha256": lineage_value["identity_sha256"],
            },
        }
        writable_paths = {name: str(path) for name, path in writable.items()}
        plan = sandbox_plan(
            mappings=runtime_receipt["execution_image"]["logical_mappings"],
            root_path=str(hot_root),
            manifest_file={
                "path": str(manifest_file.path),
                "sha256": manifest_file.sha256,
                "identity_sha256": manifest_value["identity_sha256"],
            },
            control_bindings=control_bindings,
            item_read_bindings=item_read_rows,
            writable_roots=writable_paths,
            gpu_devices=gpu_devices,
            gpu_uuid=gpu_uuid,
            gpu_index=gpu_index,
            host_abi_bindings=host_abi_bindings,
            launcher_profile=launcher_profile,
        )
        plan["gpu"] = {**plan["gpu"], **gpu_observation, "devices": gpu_devices}
        parent_netns = os.readlink("/proc/self/ns/net")
        attestation = make_launch_attestation(
            mode=args.mode,
            launcher_profile=launcher_profile,
            launcher_profile_file={"path": str(profile_file.path), "sha256": profile_file.sha256},
            runtime_receipt=runtime_receipt,
            runtime_file={"path": str(runtime_file.path), "sha256": runtime_file.sha256},
            root_registration=registration,
            root_file={"path": str(root_file.path), "sha256": root_file.sha256},
            production_profile=production_profile,
            production_profile_file={"path": str(production_file.path), "sha256": production_file.sha256},
            manifest=manifest_value,
            manifest_file={"path": str(manifest_file.path), "sha256": manifest_file.sha256},
            lineage_preflight_file={
                "path": str(lineage_file.path),
                "sha256": lineage_file.sha256,
                "identity_sha256": lineage_value["identity_sha256"],
                "attestation_id": lineage_value["attestation_id"],
            },
            host_abi=host_abi_summary,
            plan=plan,
            host_envelope=host_envelope,
            parent_network_namespace=parent_netns,
            local_readiness_file=(
                None
                if readiness_file is None or readiness_value is None
                else {
                    "path": str(readiness_file.path),
                    "sha256": readiness_file.sha256,
                    "identity_sha256": readiness_value["identity_sha256"],
                    "readiness_id": readiness_value["readiness_id"],
                }
            ),
        )
        attestation_path = temporary_root / ATTESTATION_FILE
        attestation_body = canonical_bytes(attestation)
        _write_new(attestation_path, attestation_body, 0o400)
        attestation_fd = os.open(
            attestation_path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
        )
        writable_targets = {
            "result": str(writable["result"]),
            "event": str(writable["event"]),
            "lock": str(writable["lock"]),
        }
        writable_sources = {
            name: descriptor
            for name, descriptor in zip(("result", "event", "lock"), writable_descriptors, strict=True)
        }
        main_control_sources: dict[str, dict[str, int]] = {}
        for name, retained in (
            ("runtime", runtime_file),
            ("profile", production_file),
            ("root", root_file),
        ):
            duplicate = os.dup(retained.descriptor)
            duplicated_descriptors.append(duplicate)
            main_control_sources[name] = {
                "control": retained.descriptor,
                "work_order": duplicate,
            }
        argv = build_bwrap_argv(
            launcher_profile=launcher_profile,
            mappings=runtime_receipt["execution_image"]["logical_mappings"],
            mapping_sources=mapping_sources,
            host_abi_sources=host_abi_sources,
            manifest_source=manifest_file.descriptor,
            control_sources=main_control_sources,
            control_original_targets={
                "runtime": work_runtime["receipt_path"],
                "profile": work_profile["path"],
                "root": work_root["registration_path"],
            },
            lineage_preflight_source=lineage_file.descriptor,
            lineage_preflight_sha256=lineage_file.sha256,
            read_binding_sources={
                path: retained.descriptor
                for path, retained in sorted(read_files_by_path.items())
            },
            writable_sources=writable_sources,
            writable_targets=writable_targets,
            attestation_source=attestation_fd,
            attestation_sha256=sha256_bytes(attestation_body),
            gpu_devices=gpu_devices,
            gpu_uuid=gpu_uuid,
            gpu_index=gpu_index,
            batch_manifest_sha256=manifest_file.sha256,
            runtime_sha256=runtime_file.sha256,
            profile_sha256=production_file.sha256,
            root_sha256=root_file.sha256,
        )
        main_mapping_names = {
            row["name"]
            for row in _minimal_execution_bindings(
                runtime_receipt["execution_image"]["logical_mappings"]
            )
        }
        pass_fds = [
            manifest_file.descriptor,
            runtime_file.descriptor,
            production_file.descriptor,
            root_file.descriptor,
            lineage_file.descriptor,
            attestation_fd,
            *writable_descriptors,
            *duplicated_descriptors,
            *(
                retained_mappings[name].descriptor
                for name in sorted(main_mapping_names)
            ),
            *(retained.descriptor for retained in read_files),
            *(retained.descriptor for retained in host_abi_files),
        ]
        maximum_wall = _integer(production_profile["batch_limits"]["maximum_wall_seconds"], "batch maximum wall", 1, 24 * 60 * 60)
        try:
            status = _run_child(argv, pass_fds, maximum_wall + 60)
        finally:
            os.close(attestation_fd)
            attestation_fd = None
        if status != 0:
            raise TrustedLauncherError(f"isolated GPU batch exited with status {status}")
        if observe_host_envelope(production_profile, enforce=resource_enforced) != host_envelope:
            raise TrustedLauncherError("host resource envelope changed during execution")
        if (
            observe_host_abi_platform(gpu_observation["driver_version"])
            != launcher_profile["host_abi"]["platform"]
        ):
            raise TrustedLauncherError("host ABI platform changed during execution")
        root.verify()
        host_abi_root.verify()
        for retained in retained_files:
            retained.verify()
        for name, (path, descriptor, expected) in writable_observations.items():
            try:
                linked = path.lstat()
            except OSError as error:
                raise TrustedLauncherError(f"writable {name} root disappeared") from error
            if (
                _writable_directory_authority_identity(os.fstat(descriptor))
                != _writable_directory_authority_identity(expected)
                or _writable_directory_authority_identity(linked)
                != _writable_directory_authority_identity(expected)
            ):
                raise TrustedLauncherError(f"writable {name} root changed during execution")
        return 0
    finally:
        cleanup_error: BaseException | None = None
        if preflight_output_descriptor is not None:
            with contextlib.suppress(OSError):
                os.close(preflight_output_descriptor)
        if attestation_fd is not None:
            with contextlib.suppress(OSError):
                os.close(attestation_fd)
        for retained in retained_mappings.values():
            retained.close()
        retained_mappings.clear()
        if temporary_root is not None:
            try:
                tool = launcher_profile["system_tools"]["fusermount"]["path"]
            except (NameError, KeyError):
                _terminate(fuse_process)
            else:
                try:
                    cleanup_transient_root(
                        temporary_root, tool, fuse_process=fuse_process
                    )
                except BaseException as error:
                    cleanup_error = error
        if launcher_lock_descriptor is not None:
            with contextlib.suppress(OSError):
                fcntl.flock(launcher_lock_descriptor, fcntl.LOCK_UN)
            with contextlib.suppress(OSError):
                os.close(launcher_lock_descriptor)
        for descriptor in writable_descriptors:
            try:
                os.close(descriptor)
            except OSError:
                pass
        for descriptor in duplicated_descriptors:
            with contextlib.suppress(OSError):
                os.close(descriptor)
        if root is not None:
            root.close()
        for retained in retained_files:
            retained.close()
        if host_abi_root is not None:
            host_abi_root.close()
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
        if cleanup_error is not None:
            raise cleanup_error


def _normalize_install_spec(value: Any) -> dict[str, Any]:
    fields = {
        "kind", "schema_version", "launcher_install_path", "launcher_profile_install_path",
        "runtime_admission_install_path", "execution_image", "production_profile",
        "root_registration", "system_tool_paths", "host_abi", "sandbox", "policy",
    }
    item = _exact(value, "launcher install spec", fields)
    if item["kind"] != INSTALL_SPEC_KIND or item["schema_version"] != SCHEMA_VERSION or item["policy"] != POLICY:
        raise TrustedLauncherError("launcher install spec header/policy is unsupported")
    launcher_path = normalized_absolute_path(item["launcher_install_path"], "launcher install path")
    profile_path = normalized_absolute_path(item["launcher_profile_install_path"], "launcher profile install path")
    runtime_path = normalized_absolute_path(item["runtime_admission_install_path"], "runtime admission install path")
    tool_paths = item["system_tool_paths"]
    if tool_paths != SYSTEM_TOOL_PATHS:
        raise TrustedLauncherError("install spec tool paths are not exact")
    # Reuse the profile validator by supplying observed tools and a source hash.
    image = _exact(item["execution_image"], "execution_image", {"path", "sha256", "byte_count", "identity_sha256", "receipt_path", "receipt_sha256"})
    root_value = _exact(item["root_registration"], "root_registration", {"path", "sha256", "identity_sha256", "registration_id", "root_id"})
    profile_ref = _reference(item["production_profile"], "production_profile", identity=True)
    sandbox = item["sandbox"]
    provisional = {
        "kind": PROFILE_KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "launcher": {"path": str(launcher_path), "sha256": "0" * 64},
        "runtime_admission_install_path": str(runtime_path),
        "execution_image": image,
        "production_profile": profile_ref,
        "root_registration": root_value,
        "system_tools": {
            name: {"path": path, "sha256": "0" * 64, "byte_count": 1, "uid": 0, "mode": "0755"}
            for name, path in sorted(tool_paths.items())
        },
        "host_abi": item["host_abi"],
        "sandbox": sandbox,
        "policy": dict(POLICY),
    }
    identity = sha256_bytes(canonical_bytes(provisional))
    validate_launcher_profile({**provisional, "identity_sha256": identity, "profile_id": f"gpulaunchprofile_{identity[:32]}"})
    return {
        "kind": INSTALL_SPEC_KIND,
        "schema_version": SCHEMA_VERSION,
        "launcher_install_path": str(launcher_path),
        "launcher_profile_install_path": str(profile_path),
        "runtime_admission_install_path": str(runtime_path),
        "execution_image": image,
        "production_profile": profile_ref,
        "root_registration": root_value,
        "system_tool_paths": dict(SYSTEM_TOOL_PATHS),
        "host_abi": validate_host_abi_manifest(item["host_abi"]),
        "sandbox": sandbox,
        "policy": dict(POLICY),
    }


def _make_profile_from_spec(spec: dict[str, Any], launcher_sha256: str) -> dict[str, Any]:
    tools = {name: _observed_tool(path, f"system tool {name}") for name, path in sorted(spec["system_tool_paths"].items())}
    core = {
        "kind": PROFILE_KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "launcher": {"path": spec["launcher_install_path"], "sha256": launcher_sha256},
        "runtime_admission_install_path": spec["runtime_admission_install_path"],
        "execution_image": spec["execution_image"],
        "production_profile": spec["production_profile"],
        "root_registration": spec["root_registration"],
        "system_tools": tools,
        "host_abi": spec["host_abi"],
        "sandbox": spec["sandbox"],
        "policy": dict(POLICY),
    }
    identity = sha256_bytes(canonical_bytes(core))
    return validate_launcher_profile({**core, "identity_sha256": identity, "profile_id": f"gpulaunchprofile_{identity[:32]}"})


def stage_install(args: argparse.Namespace) -> dict[str, Any]:
    source = Path(__file__).resolve()
    source_body = source.read_bytes()
    source_sha = sha256_bytes(source_body)
    spec_path = normalized_absolute_path(args.spec, "install spec path")
    with retain_file(
        spec_path,
        "launcher install spec",
        expected_sha256=args.expected_spec_sha256,
        maximum=MAX_JSON_BYTES,
        allowed_owner_modes={(os.geteuid(), 0o400)},
        keep_body=True,
    ) as retained:
        spec_value = _canonical_document(retained, "launcher install spec")
    spec = _normalize_install_spec(spec_value)
    if spec != spec_value:
        raise TrustedLauncherError("launcher install spec is not normalized")
    staging = normalized_absolute_path(args.staging_dir, "staging directory")
    info = staging.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700 or any(staging.iterdir()):
        raise TrustedLauncherError("staging directory must be empty, current-user-owned, and mode 0700")
    profile = _make_profile_from_spec(spec, source_sha)
    launcher_stage = staging / "trusted-launcher-v2"
    profile_stage = staging / "launcher-profile-v2.json"
    manifest_stage = staging / "install-manifest-v2.json"
    _write_new(launcher_stage, source_body, 0o500)
    _write_new(profile_stage, canonical_bytes(profile), 0o400)
    install_dirs = sorted({str(Path(spec["launcher_install_path"]).parent), str(Path(spec["launcher_profile_install_path"]).parent), str(Path(spec["runtime_admission_install_path"]).parent)})
    install_argv: list[list[str]] = [
        ["/usr/bin/install", "-d", "-o", "root", "-g", "root", "-m", "0755", directory]
        for directory in install_dirs
    ]
    install_argv.extend(
        [
            ["/usr/bin/install", "-o", "root", "-g", "root", "-m", "0555", str(launcher_stage), spec["launcher_install_path"]],
            ["/usr/bin/install", "-o", "root", "-g", "root", "-m", "0444", str(profile_stage), spec["launcher_profile_install_path"]],
        ]
    )
    core = {
        "kind": INSTALL_MANIFEST_KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "source": {"path": str(source), "sha256": source_sha},
        "spec": {"path": str(spec_path), "sha256": args.expected_spec_sha256},
        "staged": {
            "launcher": {"path": str(launcher_stage), "sha256": source_sha, "mode": "0500"},
            "profile": {"path": str(profile_stage), "sha256": sha256_bytes(canonical_bytes(profile)), "mode": "0400", "identity_sha256": profile["identity_sha256"]},
        },
        "destinations": {
            "launcher": {"path": spec["launcher_install_path"], "uid": 0, "gid": 0, "mode": "0555"},
            "profile": {"path": spec["launcher_profile_install_path"], "uid": 0, "gid": 0, "mode": "0444"},
            "runtime_admission": {"path": spec["runtime_admission_install_path"], "uid": 0, "gid": 0, "mode": "0444", "installed_in_later_review_step": True},
        },
        "install_argv": install_argv,
        "policy": dict(POLICY),
    }
    identity = sha256_bytes(canonical_bytes(core))
    manifest = {**core, "identity_sha256": identity, "manifest_id": f"gpulaunchinstall_{identity[:32]}"}
    _write_new(manifest_stage, canonical_bytes(manifest), 0o400)
    return manifest


def doctor_local_private(args: argparse.Namespace) -> dict[str, Any]:
    """Replay local controls, image, host ABI, GPU, and cgroup without media."""

    if (
        sys.flags.isolated != 1
        or sys.flags.no_user_site != 1
        or not sys.dont_write_bytecode
    ):
        raise TrustedLauncherError(
            "local-private doctor must start with Python -IB isolation"
        )
    retained: list[RetainedFile] = []
    root: RetainedRoot | None = None
    host_abi_root: RetainedHostABIRoot | None = None
    host_abi_files: list[RetainedHostABILibrary] = []
    try:
        profile_file, profile_value = _load_control(
            args.launcher_profile,
            args.expected_launcher_profile_sha256,
            "launcher profile",
            production_root_owned=False,
            local_user_owned=True,
        )
        retained.append(profile_file)
        launcher_profile = validate_launcher_profile(profile_value)
        current_launcher = Path(__file__).resolve()
        if str(current_launcher) != launcher_profile["launcher"]["path"]:
            raise TrustedLauncherError(
                "doctor must execute the local launcher bound by its profile"
            )
        with retain_file(
            current_launcher,
            "local trusted launcher",
            expected_sha256=launcher_profile["launcher"]["sha256"],
            maximum=MAX_TOOL_BYTES,
            allowed_owner_modes={(os.geteuid(), 0o500)},
            keep_body=False,
        ):
            pass
        runtime_file, runtime_value = _load_control(
            args.runtime_admission,
            args.expected_runtime_admission_sha256,
            "runtime admission",
            production_root_owned=False,
            local_user_owned=True,
        )
        retained.append(runtime_file)
        if str(runtime_file.path) != launcher_profile["runtime_admission_install_path"]:
            raise TrustedLauncherError("local runtime is not at its bound path")
        runtime = validate_runtime_receipt(
            runtime_value,
            mode=MODE_LOCAL_PRIVATE,
            launcher_profile=launcher_profile,
            launcher_profile_path=str(profile_file.path),
            profile_sha256=profile_file.sha256,
        )
        if runtime["execution_image"]["image"].get("mode") != "0400":
            raise TrustedLauncherError("local-private execution image is not mode 0400")
        production_file, production_value = _load_control(
            args.production_profile,
            args.expected_production_profile_sha256,
            "production profile",
            production_root_owned=False,
            local_user_owned=True,
        )
        retained.append(production_file)
        production_profile = validate_production_profile(production_value)
        expected_profile = launcher_profile["production_profile"]
        if (
            str(production_file.path) != expected_profile["path"]
            or production_file.sha256 != expected_profile["sha256"]
            or production_profile["identity_sha256"]
            != expected_profile["identity_sha256"]
        ):
            raise TrustedLauncherError("doctor production profile differs")
        root_file, root_value = _load_control(
            args.root_registration,
            args.expected_root_registration_sha256,
            "root registration",
            production_root_owned=False,
            local_user_owned=True,
        )
        retained.append(root_file)
        registration = validate_root_registration(root_value)
        expected_root = launcher_profile["root_registration"]
        if (
            str(root_file.path) != expected_root["path"]
            or root_file.sha256 != expected_root["sha256"]
            or any(
                registration[key] != expected_root[key]
                for key in ("identity_sha256", "registration_id", "root_id")
            )
        ):
            raise TrustedLauncherError("doctor root registration differs")
        root = retain_root(registration)
        image_expected = launcher_profile["execution_image"]
        image = retain_file(
            image_expected["path"],
            "local-private execution image",
            expected_sha256=image_expected["sha256"],
            maximum=MAX_IMAGE_BYTES,
            allowed_owner_modes={(os.geteuid(), IMAGE_CANDIDATE_MODE)},
            keep_body=False,
        )
        retained.append(image)
        if image.info.st_size != image_expected["byte_count"]:
            raise TrustedLauncherError("doctor execution-image byte count differs")
        envelope = observe_host_envelope(production_profile, enforce=True)
        hardware = production_profile["hardware"]
        gpu = resolve_gpu_observation(
            launcher_profile["system_tools"]["nvidia_smi"]["path"],
            hardware["gpu_uuid"],
            hardware["minimum_driver_version"],
            hardware["minimum_compute_capability"],
        )
        validate_gpu_devices(
            gpu["device_minor"],
            launcher_profile["sandbox"]["gpu_control_devices"],
        )
        (
            host_abi_files,
            _bindings,
            host_abi_summary,
            host_abi_root,
        ) = replay_host_abi_manifest(
            launcher_profile["host_abi"],
            observed_driver_version=gpu["driver_version"],
        )
        readiness = make_local_readiness(
            launcher_profile=launcher_profile,
            launcher_profile_file={
                "path": str(profile_file.path),
                "sha256": profile_file.sha256,
            },
            runtime=runtime,
            runtime_file={
                "path": str(runtime_file.path),
                "sha256": runtime_file.sha256,
            },
            production_profile=production_profile,
            production_profile_file={
                "path": str(production_file.path),
                "sha256": production_file.sha256,
            },
            registration=registration,
            root_file={"path": str(root_file.path), "sha256": root_file.sha256},
            host_abi_identity_sha256=host_abi_summary["identity_sha256"],
            gpu=gpu,
            host_envelope=envelope,
        )
        readiness_path = normalized_absolute_path(
            args.readiness_output, "local readiness output"
        )
        if Path(registration["path"]) not in readiness_path.parents:
            raise TrustedLauncherError(
                "local readiness output must be a strict hot-root descendant"
            )
        parent = readiness_path.parent.lstat()
        if (
            stat.S_ISLNK(parent.st_mode)
            or not stat.S_ISDIR(parent.st_mode)
            or parent.st_uid != os.geteuid()
            or stat.S_IMODE(parent.st_mode) != 0o700
        ):
            raise TrustedLauncherError(
                "local readiness output parent must be current-user mode 0700"
            )
        readiness_body = canonical_bytes(readiness)
        _write_new(readiness_path, readiness_body, 0o400)
        return {
            "status": "ready",
            "mode": MODE_LOCAL_PRIVATE,
            "execution_class": EXECUTION_CLASS_LOCAL_PRIVATE,
            "runtime_status": runtime["status"],
            "runtime_identity_sha256": runtime["identity_sha256"],
            "execution_image_identity_sha256": image_expected["identity_sha256"],
            "host_abi_identity_sha256": host_abi_summary["identity_sha256"],
            "gpu_uuid": hardware["gpu_uuid"],
            "host_envelope": envelope,
            "trust_boundary": {
                "kind": "current_user_same_uid",
                "same_uid_mutation_resistance": False,
            },
            "readiness_receipt": {
                "path": str(readiness_path),
                "sha256": sha256_bytes(readiness_body),
                "identity_sha256": readiness["identity_sha256"],
                "readiness_id": readiness["readiness_id"],
            },
            "input_media_read": False,
            "inference_performed": False,
            "files_written": True,
            "network_access": False,
        }
    finally:
        for value in host_abi_files:
            value.close()
        if host_abi_root is not None:
            host_abi_root.close()
        if root is not None:
            root.close()
        for value in retained:
            value.close()


def contract_document() -> dict[str, Any]:
    return {
        "kind": "himr_gpu_trusted_launcher_v2_contract",
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "modes": sorted(MODES),
        "execution_classes_by_mode": dict(sorted(MODE_EXECUTION_CLASS.items())),
        "required_mapping_names": sorted(REQUIRED_MAPPING_NAMES),
        "required_mapping_layout": {
            name: {"sandbox_path": path, "role": role}
            for name, (path, role) in sorted(REQUIRED_MAPPING_LAYOUT.items())
        },
        "system_tools": dict(SYSTEM_TOOL_PATHS),
        "host_abi": {
            "kind": HOST_ABI_KIND,
            "schema_version": 1,
            "library_root": "/usr/lib64",
            "descriptor_bind_per_library": True,
            "sandbox_root_remounted_read_only": True,
            "policy": dict(HOST_ABI_POLICY),
        },
        "image_modes": {
            "candidate": "0400",
            "local_private_production": "0400",
            "production": "0444",
        },
        "policy": dict(POLICY),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("contracts")
    stage = commands.add_parser("stage-install")
    stage.add_argument("--spec", required=True)
    stage.add_argument("--expected-spec-sha256", required=True)
    stage.add_argument("--staging-dir", required=True)
    doctor = commands.add_parser("doctor-local-private")
    doctor.add_argument("--runtime-admission", required=True)
    doctor.add_argument("--expected-runtime-admission-sha256", required=True)
    doctor.add_argument("--production-profile", required=True)
    doctor.add_argument("--expected-production-profile-sha256", required=True)
    doctor.add_argument("--root-registration", required=True)
    doctor.add_argument("--expected-root-registration-sha256", required=True)
    doctor.add_argument("--launcher-profile", required=True)
    doctor.add_argument("--expected-launcher-profile-sha256", required=True)
    doctor.add_argument("--readiness-output", required=True)
    run = commands.add_parser("run")
    run.add_argument("--mode", choices=tuple(sorted(MODES)), required=True)
    run.add_argument("--batch-manifest", required=True)
    run.add_argument("--expected-batch-sha256", required=True)
    run.add_argument("--runtime-admission", required=True)
    run.add_argument("--expected-runtime-admission-sha256", required=True)
    run.add_argument("--production-profile", required=True)
    run.add_argument("--expected-production-profile-sha256", required=True)
    run.add_argument("--root-registration", required=True)
    run.add_argument("--expected-root-registration-sha256", required=True)
    run.add_argument("--launcher-profile", required=True)
    run.add_argument("--expected-launcher-profile-sha256", required=True)
    run.add_argument("--writable-result-root", required=True)
    run.add_argument("--writable-event-root", required=True)
    run.add_argument("--writable-lock-root", required=True)
    run.add_argument("--local-readiness")
    run.add_argument("--expected-local-readiness-sha256")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "contracts":
            print(json.dumps(contract_document(), sort_keys=True, indent=2))
            return 0
        if args.command == "stage-install":
            manifest = stage_install(args)
            print(json.dumps({"status": "staged", "manifest": manifest}, sort_keys=True, indent=2))
            return 0
        if args.command == "doctor-local-private":
            result = doctor_local_private(args)
            print(json.dumps(result, sort_keys=True, indent=2))
            return 0
        if args.command == "run":
            return launch(args)
        parser.error("unsupported command")
    except (TrustedLauncherError, FileExistsError, OSError, subprocess.SubprocessError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
