#!/usr/bin/env python3
"""Finite, attested, two-worker resident GPU ASR batches for v5 work orders.

This worker is intentionally a small execution engine, not a scheduler.  A trusted
launcher authenticates host-owned state and enters the networkless sandbox.  The
worker then authenticates the launch attestation and compact controls, preflights
every retained input before taking the UUID lock, loads exactly one WhisperModel,
and consumes ordinary ``transcribe`` iterators two at a time.  The two concurrent
calls use CTranslate2's admitted ``num_workers=2`` support; this is not neural
batching and every decoding argument remains the exact v2 production profile.

The GPU execution phase exposes no corpus database, catalogue, archive tier, or
publication surface.  Its preceding GPU-less lineage phase receives the
registered hot root read-only solely for typed sealed-receipt replay.  Neither
phase has import or deletion authority.  Results are private, content addressed,
immutable, and require a separate reviewed import operation.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import ctypes
import errno
import fcntl
import hashlib
import importlib.metadata
import importlib.util
import json
import math
import os
import re
import secrets
import stat
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Iterator, Sequence


KIND = "himr_faster_whisper_gpu_batch_manifest"
COMPLETION_KIND = "himr_faster_whisper_gpu_batch_completion"
EVENT_KIND = "himr_faster_whisper_gpu_batch_event"
CONTRACT_KIND = "himr_faster_whisper_gpu_batch_contract"
SCHEMA_VERSION = 2
IMPLEMENTATION_VERSION = "0.3.0"
MATERIALIZER = "himr-faster-whisper-gpu-resident-pairs-v2"

MAX_ITEMS = 32
MAX_JSON_BYTES = 32 * 1024 * 1024
MAX_WORK_ORDER_BYTES = 32 * 1024 * 1024
MAX_EVENT_BYTES = 2 * 1024 * 1024
MAX_EVENTS_PER_ATTEMPT = 40
MAX_HOST_MEMORY_BYTES = 12 * 1024**3
MAX_HOST_PIDS = 64
MAX_HOST_NOFILE = 1024
MAX_HOST_FSIZE_BYTES = 32 * 1024**2
HASH_CHUNK_BYTES = 1024 * 1024
ATTESTATION_KIND = "himr_gpu_trusted_launch_attestation"
ATTESTATION_VERSION = "0.3.0"
LINEAGE_ATTESTATION_KIND = "himr_gpu_asr_lineage_preflight"
GPU_UUID_RE = re.compile(r"GPU-[A-Za-z0-9-]{8,92}\Z")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
IDENTIFIER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,255}\Z")
NETWORK_NAMESPACE_RE = re.compile(r"net:\[[0-9]+\]\Z")

EXECUTION_CLASS_PRODUCTION = "production_private_asr"
EXECUTION_CLASS_LOCAL_PRIVATE = "local_private_production_asr"
EXECUTION_CLASS_SYNTHETIC = "synthetic_canary"
EXECUTION_CLASSES = frozenset(
    {
        EXECUTION_CLASS_PRODUCTION,
        EXECUTION_CLASS_LOCAL_PRIVATE,
        EXECUTION_CLASS_SYNTHETIC,
    }
)


def _production_lineage_class(execution_class: str) -> bool:
    return execution_class in {
        EXECUTION_CLASS_PRODUCTION,
        EXECUTION_CLASS_LOCAL_PRIVATE,
    }


def _runtime_status_for_class(execution_class: str) -> str:
    return "admitted" if execution_class == EXECUTION_CLASS_PRODUCTION else "candidate"


def _launcher_mode_for_class(execution_class: str) -> str:
    return {
        EXECUTION_CLASS_PRODUCTION: "production",
        EXECUTION_CLASS_LOCAL_PRIVATE: "local-private-production",
        EXECUTION_CLASS_SYNTHETIC: "candidate-synthetic-canary",
    }[execution_class]

REQUIRED_MAPPING_LAYOUT = {
    "application_root": ("/opt/himr-gpu/app", "application_root"),
    "application_support_root": ("/opt/himr-gpu/corpus/src", "application_support_root"),
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

V5_RESULT_PHASES = (
    "preflight_input_hash",
    "preflight_ffprobe",
    "transcribe_setup",
    "model_iterator",
    "transcript_normalization",
    "artifact_serialization",
)

POLICY = {
    "visibility": "private",
    "network_access": False,
    "publication_authority": "none",
    "catalogue_mutation_authority": "none",
    "identity_authority": "none",
    "biometric_authority": "none",
    "wiki_authority": "none",
    "archive_authority": "none",
    "deletion_authority": "none",
    "dispatch_order": "sealed_ordinal_resident_pairs",
    "resume_policy": "reuse_only_exact_completed_v5_results",
    "pair_failure_policy": "discard_unpublished_pair_fail_stop",
    "inference_mode": "one_model_two_ordinary_transcribe_iterators",
    "neural_batching": False,
}

LAUNCH_POLICY = {
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


class BatchV2Error(RuntimeError):
    """A sealed batch, launch invariant, resource guard, or item failed."""


class GPUResourceBusy(BatchV2Error):
    """The admitted UUID lock is already held."""


class PairFailure(BatchV2Error):
    """At least one item in an unpublished pair failed."""


def _load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise BatchV2Error(f"cannot load required module {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(name, None)
        raise
    return module


GPU_DIR = Path(__file__).resolve().parent
V5 = _load_module("himr_production_asr_v5_for_batch_v2", GPU_DIR / "production_asr_v5.py")
PROFILE = _load_module("himr_gpu_profile_for_batch_v2", GPU_DIR / "production_profile_v2.py")
TELEMETRY = _load_module("himr_gpu_telemetry_for_batch_v2", GPU_DIR / "gpu_telemetry.py")
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
        raise BatchV2Error(f"value is not canonical JSON: {error}") from error


def sha256_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _exact(value: Any, label: str, fields: set[str] | frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != set(fields):
        observed = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise BatchV2Error(f"{label} has unexpected fields: {observed}")
    return value


def _integer(value: Any, label: str, minimum: int, maximum: int = 2**63 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise BatchV2Error(f"{label} must be an integer within [{minimum}, {maximum}]")
    return value


def _number(value: Any, label: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BatchV2Error(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise BatchV2Error(f"{label} must be finite within [{minimum}, {maximum}]")
    return result


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise BatchV2Error(f"{label} must be a lowercase SHA-256")
    return value


def _identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER_RE.fullmatch(value):
        raise BatchV2Error(f"{label} is invalid")
    return value


def _version_tuple(value: Any, label: str, *, parts: int) -> tuple[int, ...]:
    if not isinstance(value, str):
        raise BatchV2Error(f"{label} must be a dotted numeric version")
    rows = value.split(".")
    if len(rows) != parts or any(not row.isdigit() for row in rows):
        raise BatchV2Error(f"{label} must be a {parts}-part dotted numeric version")
    return tuple(int(row) for row in rows)


def _absolute(value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 4096:
        raise BatchV2Error(f"{label} must be a bounded absolute path")
    path = Path(value)
    if not path.is_absolute() or path == Path("/") or os.path.normpath(value) != value or "//" in value:
        raise BatchV2Error(f"{label} must be one normalized absolute non-root path")
    cold = Path("/mnt/archive/HIMR")
    if path == cold or cold in path.parents:
        raise BatchV2Error(f"{label} may not reference cold storage")
    return path


def _descendant(path: Path, root: Path, label: str, *, equal: bool = False) -> None:
    if not (path == root if equal else False) and root not in path.parents:
        raise BatchV2Error(f"{label} must remain beneath {root}")


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise BatchV2Error(f"JSON contains duplicate key {key!r}")
        value[key] = item
    return value


def parse_json(body: bytes, label: str) -> Any:
    def reject(value: str) -> None:
        raise BatchV2Error(f"{label} contains non-finite value {value}")

    try:
        return json.loads(
            body.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_pairs,
            parse_constant=reject,
        )
    except BatchV2Error:
        raise
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise BatchV2Error(f"{label} is not strict JSON: {error}") from error


def _identity(info: os.stat_result) -> tuple[int, ...]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_nlink,
        info.st_uid,
        info.st_gid,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def stable_file(
    path_value: str | Path,
    label: str,
    *,
    maximum: int,
    expected_sha256: str | None = None,
    exact_mode: int | None = None,
) -> tuple[bytes, os.stat_result]:
    path = _absolute(str(path_value), f"{label} path")
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        before = path.lstat()
        descriptor = os.open(path, flags)
    except OSError as error:
        raise BatchV2Error(f"cannot open {label}: {error}") from error
    try:
        opened = os.fstat(descriptor)
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or _identity(before) != _identity(opened)
            or opened.st_nlink != 1
            or opened.st_size < 1
            or opened.st_size > maximum
            or (exact_mode is not None and stat.S_IMODE(opened.st_mode) != exact_mode)
        ):
            raise BatchV2Error(f"{label} metadata is unsafe")
        parts: list[bytes] = []
        remaining = opened.st_size
        while remaining:
            part = os.read(descriptor, min(remaining, HASH_CHUNK_BYTES))
            if not part:
                raise BatchV2Error(f"{label} ended before its sealed size")
            parts.append(part)
            remaining -= len(part)
        if os.read(descriptor, 1):
            raise BatchV2Error(f"{label} grew while read")
        body = b"".join(parts)
        if expected_sha256 is not None and sha256_bytes(body) != _digest(expected_sha256, f"{label} SHA-256"):
            raise BatchV2Error(f"{label} SHA-256 differs")
        if _identity(os.fstat(descriptor)) != _identity(opened) or _identity(path.lstat()) != _identity(opened):
            raise BatchV2Error(f"{label} changed while read")
        return body, opened
    finally:
        os.close(descriptor)


def load_canonical_document(
    path_value: str | Path,
    expected_sha256: str,
    label: str,
    *,
    maximum: int = MAX_JSON_BYTES,
) -> tuple[Any, bytes]:
    body, _ = stable_file(path_value, label, maximum=maximum, expected_sha256=expected_sha256)
    value = parse_json(body, label)
    if body != canonical_bytes(value):
        raise BatchV2Error(f"{label} is not canonical JSON")
    return value, body


def _safe_directory(path_value: str | Path, label: str, *, create: bool = False) -> Path:
    path = _absolute(str(path_value), label)
    if create:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        info = path.lstat()
    except OSError as error:
        raise BatchV2Error(f"cannot inspect {label}: {error}") from error
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise BatchV2Error(f"{label} must be current-user-owned mode 0700")
    return path


class RetainedDirectory:
    """A no-follow directory descriptor used as the only mutation authority."""

    def __init__(
        self,
        path: Path,
        descriptor: int,
        info: os.stat_result,
        link_chain: Sequence[tuple[int, str, tuple[int, ...]]] = (),
    ) -> None:
        self.path = path
        self.descriptor = descriptor
        self.info = info
        # Own duplicates of every parent descriptor so containment can be
        # replayed immediately around a commit, even after builder FDs close.
        self.link_chain = [
            (os.dup(parent_fd), name, expected)
            for parent_fd, name, expected in link_chain
        ]

    @classmethod
    def open(cls, path_value: str | Path, label: str) -> "RetainedDirectory":
        path = _absolute(str(path_value), label)
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
        before = path.lstat()
        descriptor = os.open(path, flags)
        try:
            opened = os.fstat(descriptor)
            if (
                stat.S_ISLNK(before.st_mode)
                or not stat.S_ISDIR(opened.st_mode)
                or _identity(before) != _identity(opened)
                or opened.st_uid != os.geteuid()
                or stat.S_IMODE(opened.st_mode) != 0o700
            ):
                raise BatchV2Error(f"{label} is not a retained current-user mode-0700 directory")
            return cls(path, descriptor, opened)
        except Exception:
            os.close(descriptor)
            raise

    @classmethod
    def from_fd(
        cls,
        path: Path,
        descriptor: int,
        label: str,
        *,
        link_chain: Sequence[tuple[int, str, tuple[int, ...]]] = (),
    ) -> "RetainedDirectory":
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or stat.S_IMODE(opened.st_mode) != 0o700
        ):
            os.close(descriptor)
            raise BatchV2Error(f"{label} is not a current-user mode-0700 directory")
        return cls(path, descriptor, opened, link_chain)

    def verify(self) -> None:
        if _directory_authority_identity(os.fstat(self.descriptor)) != _directory_authority_identity(self.info):
            raise BatchV2Error(f"retained directory changed: {self.path}")
        for parent_fd, name, expected in self.link_chain:
            try:
                linked = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            except OSError as error:
                raise BatchV2Error(
                    f"retained directory escaped its anchored chain: {self.path}"
                ) from error
            if _directory_authority_identity(linked) != expected:
                raise BatchV2Error(
                    f"retained directory link changed: {self.path}"
                )

    def close(self) -> None:
        try:
            os.close(self.descriptor)
        finally:
            for parent_fd, _name, _expected in self.link_chain:
                os.close(parent_fd)
            self.link_chain = []

    def __enter__(self) -> "RetainedDirectory":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


def _directory_authority_identity(info: os.stat_result) -> tuple[int, ...]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_uid,
        info.st_gid,
    )


def _component(value: str, label: str) -> str:
    if not value or value in {".", ".."} or "/" in value or "\x00" in value:
        raise BatchV2Error(f"{label} is not a safe path component")
    return value


def _open_or_create_private_at(parent_fd: int, name: str, label: str) -> int:
    name = _component(name, label)
    try:
        os.mkdir(name, 0o700, dir_fd=parent_fd)
    except FileExistsError:
        pass
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(name, flags, dir_fd=parent_fd)
    info = os.fstat(descriptor)
    linked = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o700
        or _directory_authority_identity(info) != _directory_authority_identity(linked)
    ):
        os.close(descriptor)
        raise BatchV2Error(f"{label} has unsafe owner, mode, or type")
    return descriptor


@contextlib.contextmanager
def retained_private_chain(
    root: RetainedDirectory, relative: Path, label: str
) -> Iterator[RetainedDirectory]:
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise BatchV2Error(f"{label} relative path is unsafe")
    root.verify()
    parent_fd = root.descriptor
    descriptors: list[int] = []
    links: list[tuple[int, str, tuple[int, ...]]] = []
    try:
        for component in relative.parts:
            descriptor = _open_or_create_private_at(parent_fd, component, label)
            linked = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
            links.append(
                (parent_fd, component, _directory_authority_identity(linked))
            )
            descriptors.append(descriptor)
            parent_fd = descriptor
        if not descriptors:
            yield root
        else:
            retained = RetainedDirectory.from_fd(
                root.path / relative,
                os.dup(descriptors[-1]),
                label,
                link_chain=links,
            )
            try:
                yield retained
                retained.verify()
            finally:
                retained.close()
        root.verify()
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


@contextlib.contextmanager
def retained_existing_chain(
    root: RetainedDirectory, relative: Path, label: str
) -> Iterator[RetainedDirectory]:
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise BatchV2Error(f"{label} relative path is unsafe")
    root.verify()
    parent_fd = root.descriptor
    descriptors: list[int] = []
    links: list[tuple[int, str, tuple[int, ...]]] = []
    try:
        for component in relative.parts:
            descriptor = os.open(
                _component(component, label),
                os.O_RDONLY
                | os.O_DIRECTORY
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
            info = os.fstat(descriptor)
            if (
                not stat.S_ISDIR(info.st_mode)
                or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o700
            ):
                os.close(descriptor)
                raise BatchV2Error(f"{label} has unsafe owner, mode, or type")
            linked = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
            if _directory_authority_identity(info) != _directory_authority_identity(linked):
                os.close(descriptor)
                raise BatchV2Error(f"{label} link changed while opened")
            links.append(
                (parent_fd, component, _directory_authority_identity(linked))
            )
            descriptors.append(descriptor)
            parent_fd = descriptor
        retained = root if not descriptors else RetainedDirectory.from_fd(
            root.path / relative,
            os.dup(descriptors[-1]),
            label,
            link_chain=links,
        )
        try:
            yield retained
            retained.verify()
        finally:
            if retained is not root:
                retained.close()
        root.verify()
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _write_new_at(parent_fd: int, name: str, body: bytes, *, mode: int = 0o400) -> None:
    name = _component(name, "publication filename")
    if not body:
        raise BatchV2Error("refusing to publish an empty document")
    temporary = f".{name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(temporary, flags, mode, dir_fd=parent_fd)
    try:
        offset = 0
        while offset < len(body):
            count = os.write(descriptor, body[offset:])
            if count <= 0:
                raise BatchV2Error("publication write made no progress")
            offset += count
        os.fchmod(descriptor, mode)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        os.link(
            temporary,
            name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
            follow_symlinks=False,
        )
        os.unlink(temporary, dir_fd=parent_fd)
        os.fsync(parent_fd)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary, dir_fd=parent_fd)


def _read_regular_at(
    parent_fd: int,
    name: str,
    label: str,
    *,
    maximum: int,
    exact_mode: int = 0o400,
) -> bytes:
    name = _component(name, label)
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(name, flags, dir_fd=parent_fd)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != exact_mode
            or info.st_nlink != 1
            or not 1 <= info.st_size <= maximum
        ):
            raise BatchV2Error(f"{label} metadata is unsafe")
        parts = []
        offset = 0
        while offset < info.st_size:
            part = os.pread(descriptor, min(HASH_CHUNK_BYTES, info.st_size - offset), offset)
            if not part:
                raise BatchV2Error(f"{label} ended before its sealed size")
            parts.append(part)
            offset += len(part)
        if _identity(os.fstat(descriptor)) != _identity(info):
            raise BatchV2Error(f"{label} changed while read")
        return b"".join(parts)
    finally:
        os.close(descriptor)


def _retained_result_bodies(
    result_root: RetainedDirectory,
    result_directory: Path,
    *,
    maximum: int,
) -> dict[str, bytes]:
    relative = result_directory.relative_to(result_root.path)
    names = ("result.json", "transcript.raw.json", "transcript.normalized.json")
    if not relative.parts:
        raise BatchV2Error("completed result directory may not equal the result root")
    with retained_existing_chain(
        result_root, relative.parent, "completed result parent"
    ) as parent:
        leaf = _component(relative.name, "completed result directory")
        linked_before = os.stat(
            leaf, dir_fd=parent.descriptor, follow_symlinks=False
        )
        directory_fd = os.open(
            leaf,
            os.O_RDONLY
            | os.O_DIRECTORY
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent.descriptor,
        )
        directory = RetainedDirectory.from_fd(
            result_directory, directory_fd, "completed result directory"
        )
        if _directory_authority_identity(linked_before) != _directory_authority_identity(directory.info):
            directory.close()
            raise BatchV2Error("completed result directory link changed while opened")
        descriptors: dict[str, int] = {}
        observations: dict[str, os.stat_result] = {}
        try:
            for name in names:
                descriptor = os.open(
                    name,
                    os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=directory.descriptor,
                )
                descriptors[name] = descriptor
                info = os.fstat(descriptor)
                observations[name] = info
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_uid != os.geteuid()
                    or stat.S_IMODE(info.st_mode) != 0o400
                    or info.st_nlink != 1
                    or not 1 <= info.st_size <= maximum
                ):
                    raise BatchV2Error(f"completed {name} metadata is unsafe")
            bodies: dict[str, bytes] = {}
            for name in names:
                descriptor = descriptors[name]
                info = observations[name]
                parts = []
                offset = 0
                while offset < info.st_size:
                    part = os.pread(
                        descriptor,
                        min(HASH_CHUNK_BYTES, info.st_size - offset),
                        offset,
                    )
                    if not part:
                        raise BatchV2Error(f"completed {name} ended early")
                    parts.append(part)
                    offset += len(part)
                bodies[name] = b"".join(parts)
            for name in names:
                if _identity(os.fstat(descriptors[name])) != _identity(observations[name]):
                    raise BatchV2Error(f"completed {name} changed during retained replay")
            linked_after = os.stat(
                leaf, dir_fd=parent.descriptor, follow_symlinks=False
            )
            if (
                _directory_authority_identity(linked_after)
                != _directory_authority_identity(directory.info)
            ):
                raise BatchV2Error("completed result directory link changed during replay")
            directory.verify()
            return bodies
        finally:
            for descriptor in descriptors.values():
                os.close(descriptor)
            directory.close()


def _result_directory_present(
    result_root: RetainedDirectory, result_directory: Path
) -> bool:
    relative = result_directory.relative_to(result_root.path)
    try:
        with retained_existing_chain(
            result_root, relative, "completed result directory"
        ):
            return True
    except FileNotFoundError:
        return False


def _write_new(path: Path, body: bytes, *, mode: int = 0o400) -> None:
    if not body:
        raise BatchV2Error("refusing to publish an empty document")
    parent = _safe_directory(path.parent, "publication parent")
    temporary = parent / f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0), mode)
    try:
        offset = 0
        while offset < len(body):
            offset += os.write(descriptor, body[offset:])
        os.fchmod(descriptor, mode)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        os.link(temporary, path, follow_symlinks=False)
        temporary.unlink()
        directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def _write_json_new(path: Path, value: Any, *, maximum: int = MAX_JSON_BYTES) -> str:
    body = canonical_bytes(value)
    if len(body) > maximum:
        raise BatchV2Error(f"document exceeds {maximum} bytes")
    _write_new(path, body)
    return sha256_bytes(body)


def _ensure_private_chain(root: Path, relative: Path) -> Path:
    if relative.is_absolute() or ".." in relative.parts:
        raise BatchV2Error("private directory chain is invalid")
    current = _safe_directory(root, "private chain root")
    for component in relative.parts:
        candidate = current / component
        try:
            candidate.mkdir(mode=0o700)
        except FileExistsError:
            pass
        current = _safe_directory(candidate, "private chain member")
    return current


@contextlib.contextmanager
def nonblocking_lock(path: Path, label: str) -> Iterator[dict[str, Any]]:
    parent = _safe_directory(path.parent, f"{label} parent")
    flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(parent / path.name, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        info = os.fstat(descriptor)
        lexical = (parent / path.name).lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1 or _identity(info) != _identity(lexical):
            raise BatchV2Error(f"{label} inode is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise GPUResourceBusy(f"{label} is already held") from error
        yield {"path": str(path), "held": True, "policy": "linux_flock_exclusive_nonblocking_v2"}
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)


@contextlib.contextmanager
def nonblocking_lock_at(
    root: RetainedDirectory, name: str, label: str
) -> Iterator[dict[str, Any]]:
    root.verify()
    name = _component(name, f"{label} filename")
    flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(name, flags, 0o600, dir_fd=root.descriptor)
    try:
        opened = os.fstat(descriptor)
        linked = os.stat(name, dir_fd=root.descriptor, follow_symlinks=False)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
            or _identity(opened) != _identity(linked)
        ):
            raise BatchV2Error(f"{label} inode is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise GPUResourceBusy(f"{label} is already held") from error
        linked = os.stat(name, dir_fd=root.descriptor, follow_symlinks=False)
        if _identity(linked) != _identity(opened):
            raise BatchV2Error(f"{label} link changed while acquiring the lock")
        yield {
            "path": str(root.path / name),
            "held": True,
            "policy": "linux_flock_exclusive_nonblocking_v2",
        }
        linked = os.stat(name, dir_fd=root.descriptor, follow_symlinks=False)
        if _identity(linked) != _identity(opened):
            raise BatchV2Error(f"{label} link changed while the lock was held")
        root.verify()
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


MANIFEST_CORE_FIELDS = frozenset(
    {
        "kind",
        "schema_version",
        "implementation_version",
        "materializer",
        "execution_class",
        "production_profile",
        "runtime_admission",
        "hot_root",
        "limits",
        "totals",
        "writable_roots",
        "items",
        "policy",
    }
)


def _member(ordinal: int, order: dict[str, Any], profile: dict[str, Any]) -> dict[str, Any]:
    plan = V5.result_plan(order, profile_document=profile)
    return {
        "ordinal": ordinal,
        # The full canonical object is the sole semantic member authority.  It
        # is content-addressed by the manifest; mutable source-work-order paths
        # are deliberately not part of the execution contract.
        "work_order": order,
        "input": {
            "sha256": order["input"]["expected_sha256"],
            "byte_count": order["input"]["expected_byte_count"],
            "duration_ms": order["input"]["expected_duration_ms"],
        },
        "result": {
            "result_key": plan["result_key"],
            "result_path": plan["result_path"],
            "raw_transcript_path": plan["raw_transcript_path"],
            "normalized_transcript_path": plan["normalized_transcript_path"],
        },
    }


def make_manifest(
    *,
    work_order_records: Sequence[tuple[Path, dict[str, Any], bytes]],
    profile: dict[str, Any],
    batch_root: Path,
    event_root: Path,
    lock_root: Path,
) -> dict[str, Any]:
    profile = PROFILE.validate_profile(profile)
    limits = profile["batch_limits"]
    if not 1 <= len(work_order_records) <= limits["maximum_items"]:
        raise BatchV2Error("batch item count exceeds the exact production profile")
    orders = [record[1] for record in work_order_records]
    lineage_kinds = {order["source_lineage"]["kind"] for order in orders}
    if lineage_kinds == {V5.SOURCE_LINEAGE_PRODUCTION}:
        statuses = {order["runtime_admission"]["status"] for order in orders}
        if statuses == {"admitted"}:
            execution_class = EXECUTION_CLASS_PRODUCTION
        elif statuses == {"candidate"}:
            execution_class = EXECUTION_CLASS_LOCAL_PRIVATE
        else:
            raise BatchV2Error("production work orders have mixed runtime states")
    elif lineage_kinds == {V5.SOURCE_LINEAGE_SYNTHETIC}:
        execution_class = EXECUTION_CLASS_SYNTHETIC
    else:
        raise BatchV2Error("production and synthetic work orders may not be mixed")
    first = orders[0]
    common_keys = ("production_profile", "runtime_admission", "hot_root", "output")
    for ordinal, order in enumerate(orders, 1):
        V5.validate_work_order(order, profile_document=profile)
        if any(order[key] != first[key] for key in common_keys):
            raise BatchV2Error(f"work order {ordinal} differs from the batch common bindings")
        expected_lineage = V5.SOURCE_LINEAGE_PRODUCTION if _production_lineage_class(execution_class) else V5.SOURCE_LINEAGE_SYNTHETIC
        if order["source_lineage"]["kind"] != expected_lineage:
            raise BatchV2Error(f"work order {ordinal} has the wrong source class")
        if order["runtime_admission"]["status"] != _runtime_status_for_class(execution_class):
            raise BatchV2Error("work-order runtime status differs from its execution class")
        if execution_class == EXECUTION_CLASS_SYNTHETIC and (
            order["source_lineage"].get("contains_corpus_media") is not False
            or order["source_lineage"].get("corpus_authority") != "none"
        ):
            raise BatchV2Error("synthetic work orders must deny corpus authority")
    identities = [order["identity_sha256"] for order in orders]
    input_hashes = [order["input"]["expected_sha256"] for order in orders]
    result_keys = [V5.result_plan(order, profile_document=profile)["result_key"] for order in orders]
    if len(set(identities)) != len(identities) or len(set(input_hashes)) != len(input_hashes) or len(set(result_keys)) != len(result_keys):
        raise BatchV2Error("batch work-order, input, and result identities must be unique")
    total_duration = sum(order["input"]["expected_duration_ms"] for order in orders)
    total_bytes = sum(order["input"]["expected_byte_count"] for order in orders)
    if total_duration > limits["maximum_total_audio_ms"]:
        raise BatchV2Error("batch audio duration exceeds the exact production profile")
    hot_root = Path(first["hot_root"]["path"])
    roots = {
        "batch": _safe_directory(batch_root, "batch root"),
        "event": _safe_directory(event_root, "event root"),
        "lock": _safe_directory(lock_root, "lock root"),
        "result": _safe_directory(first["output"]["root"], "result root"),
    }
    for name, path in roots.items():
        _descendant(path, hot_root, f"{name} root")
    values = list(roots.values())
    if len(set(values)) != 4 or any(left in right.parents or right in left.parents for index, left in enumerate(values) for right in values[index + 1 :]):
        raise BatchV2Error("batch writable roots must be distinct and non-nested")
    core = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "materializer": MATERIALIZER,
        "execution_class": execution_class,
        "production_profile": first["production_profile"],
        "runtime_admission": first["runtime_admission"],
        "hot_root": first["hot_root"],
        "limits": {
            "maximum_items": limits["maximum_items"],
            "maximum_total_audio_ms": limits["maximum_total_audio_ms"],
            "preferred_total_audio_ms": limits["preferred_total_audio_ms"],
            "maximum_wall_seconds": limits["maximum_wall_seconds"],
            "model_load_count": limits["model_load_count"],
            "inference_concurrency": limits["inference_concurrency"],
            "ready_batch_high_water": limits["ready_batch_high_water"],
        },
        "totals": {
            "item_count": len(orders),
            "audio_duration_ms": total_duration,
            "audio_byte_count": total_bytes,
        },
        "writable_roots": {name: str(path) for name, path in sorted(roots.items())},
        "items": [
            _member(ordinal, order, profile)
            for ordinal, (_path, order, _body) in enumerate(work_order_records, 1)
        ],
        "policy": dict(POLICY),
    }
    identity = sha256_bytes(canonical_bytes(core))
    return {**core, "identity_sha256": identity, "batch_id": f"gpuasrbatch2_{identity[:32]}"}


def validate_manifest(value: Any, *, profile: dict[str, Any]) -> dict[str, Any]:
    item = _exact(value, "batch manifest", MANIFEST_CORE_FIELDS | {"identity_sha256", "batch_id"})
    if (
        item["kind"] != KIND
        or item["schema_version"] != SCHEMA_VERSION
        or item["implementation_version"] != IMPLEMENTATION_VERSION
        or item["materializer"] != MATERIALIZER
        or item["execution_class"] not in EXECUTION_CLASSES
        or item["policy"] != POLICY
    ):
        raise BatchV2Error("batch manifest header or policy is unsupported")
    profile = PROFILE.validate_profile(profile)
    if item["production_profile"]["identity_sha256"] != profile["identity_sha256"]:
        raise BatchV2Error("batch binds a different production profile")
    limits = profile["batch_limits"]
    expected_limits = {name: limits[name] for name in (
        "maximum_items", "maximum_total_audio_ms", "preferred_total_audio_ms",
        "maximum_wall_seconds", "model_load_count", "inference_concurrency", "ready_batch_high_water",
    )}
    if item["limits"] != expected_limits:
        raise BatchV2Error("batch limits are not the exact profile projection")
    members = item["items"]
    if not isinstance(members, list) or not 1 <= len(members) <= limits["maximum_items"]:
        raise BatchV2Error("batch item count is invalid")
    expected_lineage = V5.SOURCE_LINEAGE_PRODUCTION if _production_lineage_class(item["execution_class"]) else V5.SOURCE_LINEAGE_SYNTHETIC
    total_ms = 0
    total_bytes = 0
    seen: dict[str, set[Any]] = {"work": set(), "input": set(), "result": set()}
    for ordinal, member in enumerate(members, 1):
        row = _exact(member, f"batch item {ordinal}", {
            "ordinal", "work_order", "input", "result",
        })
        work_order = row["work_order"]
        try:
            V5.validate_work_order(work_order, profile_document=profile)
        except Exception as error:
            raise BatchV2Error(f"batch item {ordinal} embeds an invalid v5 work order: {error}") from error
        if row["ordinal"] != ordinal or work_order["source_lineage"].get("kind") != expected_lineage:
            raise BatchV2Error(f"batch item {ordinal} ordering or lineage is invalid")
        if (
            work_order["production_profile"] != item["production_profile"]
            or work_order["hot_root"] != item["hot_root"]
        ):
            raise BatchV2Error(f"batch item {ordinal} profile or hot root differs from the manifest")
        if work_order["runtime_admission"] != item["runtime_admission"]:
            raise BatchV2Error(f"batch item {ordinal} runtime differs")
        if work_order["runtime_admission"].get("status") != _runtime_status_for_class(item["execution_class"]):
            raise BatchV2Error("batch runtime status differs from its execution class")
        if item["execution_class"] == EXECUTION_CLASS_SYNTHETIC and (
            work_order["source_lineage"].get("contains_corpus_media") is not False
            or work_order["source_lineage"].get("corpus_authority") != "none"
        ):
            raise BatchV2Error("synthetic batch claims corpus authority")
        summary = _exact(row["input"], f"batch item {ordinal} input", {"sha256", "byte_count", "duration_ms"})
        _digest(summary["sha256"], f"batch item {ordinal} input SHA-256")
        total_bytes += _integer(summary["byte_count"], f"batch item {ordinal} input bytes", 1, profile["item_limits"]["maximum_audio_bytes"])
        total_ms += _integer(summary["duration_ms"], f"batch item {ordinal} duration", 1, round(profile["item_limits"]["maximum_audio_seconds"] * 1000))
        result = _exact(row["result"], f"batch item {ordinal} result", {"result_key", "result_path", "raw_transcript_path", "normalized_transcript_path"})
        for path_name in ("result_path", "raw_transcript_path", "normalized_transcript_path"):
            _absolute(result[path_name], f"batch item {ordinal} {path_name}")
        expected_input = {
            "sha256": work_order["input"]["expected_sha256"],
            "byte_count": work_order["input"]["expected_byte_count"],
            "duration_ms": work_order["input"]["expected_duration_ms"],
        }
        expected_plan = V5.result_plan(work_order, profile_document=profile)
        expected_result = {
            "result_key": expected_plan["result_key"],
            "result_path": expected_plan["result_path"],
            "raw_transcript_path": expected_plan["raw_transcript_path"],
            "normalized_transcript_path": expected_plan["normalized_transcript_path"],
        }
        if summary != expected_input or result != expected_result:
            raise BatchV2Error(f"batch item {ordinal} input or result projection differs from its work order")
        for collection, key in (("work", work_order["identity_sha256"]), ("input", summary["sha256"]), ("result", result["result_key"])):
            if key in seen[collection]:
                raise BatchV2Error(f"duplicate batch item {collection} identity")
            seen[collection].add(key)
    expected_totals = {"item_count": len(members), "audio_duration_ms": total_ms, "audio_byte_count": total_bytes}
    if item["totals"] != expected_totals or total_ms > limits["maximum_total_audio_ms"]:
        raise BatchV2Error("batch totals are invalid")
    roots = _exact(item["writable_roots"], "batch writable roots", {"batch", "event", "lock", "result"})
    hot = _absolute(item["hot_root"]["path"], "batch hot root")
    normalized_roots = {name: _absolute(value, f"batch {name} root") for name, value in roots.items()}
    for name, path in normalized_roots.items():
        _descendant(path, hot, f"batch {name} root")
    root_values = list(normalized_roots.values())
    if len(set(root_values)) != 4 or any(
        left in right.parents or right in left.parents
        for index, left in enumerate(root_values)
        for right in root_values[index + 1 :]
    ):
        raise BatchV2Error("batch writable roots must be distinct and non-nested")
    for ordinal, member in enumerate(members, 1):
        if member["work_order"]["output"]["root"] != str(normalized_roots["result"]):
            raise BatchV2Error(f"batch item {ordinal} output root differs from the result writable root")
    core = {key: item[key] for key in MANIFEST_CORE_FIELDS}
    identity = sha256_bytes(canonical_bytes(core))
    if item["identity_sha256"] != identity or item["batch_id"] != f"gpuasrbatch2_{identity[:32]}":
        raise BatchV2Error("batch manifest semantic identity is invalid")
    return item


def load_manifest(path_value: str | Path, expected_sha256: str, *, profile: dict[str, Any]) -> tuple[dict[str, Any], bytes]:
    value, body = load_canonical_document(path_value, expected_sha256, "batch manifest")
    return validate_manifest(value, profile=profile), body


def materialize_batch(
    *,
    work_order_paths: Sequence[Path],
    profile_path: Path,
    expected_profile_sha256: str,
    batch_root: Path,
    event_root: Path,
    lock_root: Path,
) -> tuple[dict[str, Any], Path]:
    profile = V5.load_profile_document(profile_path, expected_profile_sha256)
    if not 1 <= len(work_order_paths) <= profile["batch_limits"]["maximum_items"]:
        raise BatchV2Error("materialize requires a finite 1..32 work-order set")
    records: list[tuple[Path, dict[str, Any], bytes]] = []
    for ordinal, path in enumerate(work_order_paths, 1):
        body, _ = stable_file(path, f"source work order {ordinal}", maximum=MAX_WORK_ORDER_BYTES, exact_mode=0o400)
        order = V5.load_work_order(path, profile_document=profile, expected_sha256=sha256_bytes(body), replay_bindings=True)
        records.append((path, order, body))
    manifest = make_manifest(
        work_order_records=records,
        profile=profile,
        batch_root=_safe_directory(batch_root, "batch root"),
        event_root=_safe_directory(event_root, "event root"),
        lock_root=_safe_directory(lock_root, "lock root"),
    )
    batch_root = Path(manifest["writable_roots"]["batch"])
    lock_path = batch_root / ".materialize-v2.lock"
    with nonblocking_lock(lock_path, "batch materialization lock"):
        batches = _ensure_private_chain(batch_root, Path("batches"))
        target = batches / manifest["batch_id"]
        try:
            target.mkdir(mode=0o700)
        except FileExistsError:
            existing_path = target / "manifest.json"
            existing_body, _ = stable_file(existing_path, "existing batch manifest", maximum=MAX_JSON_BYTES, exact_mode=0o400)
            if existing_body != canonical_bytes(manifest):
                raise BatchV2Error("content-addressed batch directory contains different bytes")
            return manifest, existing_path
        target = _safe_directory(target, "batch directory")
        manifest_path = target / "manifest.json"
        try:
            _write_json_new(manifest_path, manifest)
        except Exception:
            with contextlib.suppress(OSError):
                target.rmdir()
            raise
    return manifest, manifest_path


def _strict_fixture(
    order: dict[str, Any], *, body: bytes, value: Any
) -> dict[str, Any]:
    """Validate the single-artifact synthetic fixture/case contract."""

    fixture = _exact(
        value,
        "synthetic fixture manifest",
        {
            "kind",
            "schema_version",
            "created_at",
            "synthetic",
            "corpus_evidence",
            "generator",
            "normalization",
            "artifact",
            "policy",
        },
    )
    if (
        fixture["kind"] != "himr_synthetic_audio_fixture"
        or fixture["schema_version"] != 1
        or fixture["synthetic"] is not True
        or fixture["corpus_evidence"] is not False
        or not isinstance(fixture["created_at"], str)
        or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", fixture["created_at"])
    ):
        raise BatchV2Error("synthetic fixture header or corpus scope is invalid")
    generator = _exact(
        fixture["generator"],
        "synthetic fixture generator",
        {
            "name", "version", "executable", "executable_sha256", "text",
            "voice", "speed_words_per_minute", "command", "wav_sha256",
            "wav_byte_count",
        },
    )
    for name in ("name", "version", "voice"):
        _identifier(generator[name], f"synthetic generator {name}")
    _absolute(generator["executable"], "synthetic generator executable")
    _digest(generator["executable_sha256"], "synthetic generator executable SHA-256")
    _digest(generator["wav_sha256"], "synthetic generator WAV SHA-256")
    _integer(generator["speed_words_per_minute"], "synthetic generator speed", 1, 1000)
    _integer(generator["wav_byte_count"], "synthetic generator WAV bytes", 1, 64 * 1024 * 1024)
    if (
        not isinstance(generator["text"], str)
        or not 1 <= len(generator["text"].encode("utf-8")) <= 4096
        or not isinstance(generator["command"], list)
        or not 1 <= len(generator["command"]) <= 64
        or any(not isinstance(value, str) or not value or len(value.encode("utf-8")) > 4096 for value in generator["command"])
    ):
        raise BatchV2Error("synthetic fixture generator evidence is malformed")
    normalization = _exact(
        fixture["normalization"],
        "synthetic fixture normalization",
        {
            "name", "version", "executable", "executable_sha256", "command",
            "codec", "sample_rate_hz", "channels", "sample_format",
            "duration_seconds",
        },
    )
    for name in ("name", "version"):
        _identifier(normalization[name], f"synthetic normalization {name}")
    _absolute(normalization["executable"], "synthetic normalization executable")
    _digest(normalization["executable_sha256"], "synthetic normalization executable SHA-256")
    if (
        normalization["codec"] != V5.MEDIA_FORMAT["codec"]
        or normalization["sample_rate_hz"] != V5.MEDIA_FORMAT["sample_rate_hz"]
        or normalization["channels"] != V5.MEDIA_FORMAT["channels"]
        or normalization["sample_format"] != V5.MEDIA_FORMAT["sample_format"]
        or not isinstance(normalization["command"], list)
        or not 1 <= len(normalization["command"]) <= 128
        or any(not isinstance(value, str) or not value or len(value.encode("utf-8")) > 4096 for value in normalization["command"])
    ):
        raise BatchV2Error("synthetic fixture normalization is not exact 16 kHz mono FLAC")
    duration = _number(
        normalization["duration_seconds"],
        "synthetic fixture duration",
        0.001,
        3600,
    )
    expected_policy = {
        "catalogue_authority": "none",
        "identity_authority": "none",
        "publication_authority": "none",
        "wiki_authority": "none",
        "existing_corpus_asr_rerun": False,
    }
    if fixture["policy"] != expected_policy:
        raise BatchV2Error("synthetic fixture safety policy is not exact")
    artifact = _exact(
        fixture["artifact"],
        "synthetic fixture artifact",
        {"path", "sha256", "byte_count"},
    )
    input_item = order["input"]
    if artifact != {
        "path": input_item["path"],
        "sha256": input_item["expected_sha256"],
        "byte_count": input_item["expected_byte_count"],
    }:
        raise BatchV2Error("synthetic fixture artifact differs from the v5 input")
    if round(duration * 1000) != input_item["expected_duration_ms"]:
        raise BatchV2Error("synthetic fixture duration differs from the v5 input")
    digest = sha256_bytes(body)
    lineage = order["source_lineage"]
    reference = lineage["fixture_manifest"]
    if (
        reference["sha256"] != digest
        or reference["identity_sha256"] != digest
    ):
        raise BatchV2Error("synthetic fixture identity or case differs from v5 lineage")
    return {
        "kind": V5.SOURCE_LINEAGE_SYNTHETIC,
        "identity_sha256": digest,
        "source_id": reference["fixture_id"],
        "member_id": None,
        "case_id": lineage["fixture_case_id"],
    }


def _deep_validate_production_order(
    order: dict[str, Any],
    *,
    profile: dict[str, Any],
    queue_cache: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    lineage = order["source_lineage"]
    handoff = lineage["gpu_handoff"]
    key = (handoff["manifest_path"], handoff["manifest_sha256"])
    manifest = queue_cache.get(key)
    if manifest is None:
        body, _ = stable_file(
            handoff["manifest_path"],
            "sealed GPU queue manifest",
            maximum=MAX_JSON_BYTES,
            expected_sha256=handoff["manifest_sha256"],
            exact_mode=0o400,
        )
        manifest = parse_json(body, "sealed GPU queue manifest")
        if body != canonical_bytes(manifest):
            raise BatchV2Error("sealed GPU queue manifest is not canonical JSON")
        if not isinstance(manifest, dict):
            raise BatchV2Error("sealed GPU queue manifest must be an object")
        queue_cache[key] = manifest
    if (
        manifest.get("queue_id") != handoff["queue_id"]
        or manifest.get("identity_sha256") != handoff["identity_sha256"]
    ):
        raise BatchV2Error("validated queue identity differs from v5 lineage")
    candidates = [
        row
        for row in manifest.get("members", [])
        if isinstance(row, dict)
        and row.get("member_id") == handoff["member_id"]
        and row.get("identity_sha256") == handoff["member_identity_sha256"]
        and row.get("ordinal") == handoff["queue_ordinal"]
        and row.get("preprocess_ordinal") == handoff["preprocess_ordinal"]
    ]
    if len(candidates) != 1:
        raise BatchV2Error("validated queue does not contain one exact v5 member")
    member = candidates[0]
    try:
        projected_lineage = V5.source_lineage_from_preprocess_descriptor(member, manifest)
        projected_input = V5.input_from_preprocess_descriptor(
            member, sealed_mode=order["input"]["sealed_mode"]
        )
        projected_hot_root = V5.hot_root_from_gpu_queue_manifest(manifest)
        projected_profile, projected_profile_document = V5.profile_from_gpu_queue_manifest(manifest)
    except Exception as error:
        raise BatchV2Error(f"validated queue member cannot rederive v5: {error}") from error
    if (
        projected_lineage != order["source_lineage"]
        or projected_input != order["input"]
        or projected_hot_root != order["hot_root"]
        or projected_profile != order["production_profile"]
        or projected_profile_document != profile
    ):
        raise BatchV2Error("validated queue member projection differs from the v5 work order")
    return {
        "kind": V5.SOURCE_LINEAGE_PRODUCTION,
        "identity_sha256": manifest["identity_sha256"],
        "source_id": manifest["queue_id"],
        "member_id": member["member_id"],
        "case_id": None,
    }


def _lineage_item(
    ordinal: int,
    order: dict[str, Any],
    lineage_summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "ordinal": ordinal,
        "work_order_identity_sha256": order["identity_sha256"],
        "lineage": lineage_summary,
        "input": {
            "sha256": order["input"]["expected_sha256"],
            "byte_count": order["input"]["expected_byte_count"],
        },
        "status": "passed",
    }


def make_lineage_preflight(
    *,
    manifest: dict[str, Any],
    manifest_physical_sha256: str,
    profile: dict[str, Any],
    root_registration: dict[str, Any],
    root_registration_path: Path,
    root_registration_sha256: str,
) -> dict[str, Any]:
    """Deep-replay every lineage without touching a media byte or GPU API."""

    root_control_path = _absolute(
        str(root_registration_path), "lineage root-registration control path"
    )
    root_control_sha256 = _digest(
        root_registration_sha256, "lineage root-registration control SHA-256"
    )
    queue_cache: dict[tuple[str, str], dict[str, Any]] = {}
    items = []
    for ordinal, member in enumerate(manifest["items"], 1):
        order = V5.validate_work_order(
            member["work_order"], profile_document=profile, replay_bindings=False
        )
        lineage = order["source_lineage"]
        if lineage["kind"] == V5.SOURCE_LINEAGE_PRODUCTION:
            if (
                str(root_control_path)
                != order["hot_root"]["registration_path"]
                or root_control_sha256
                != order["hot_root"]["registration_sha256"]
            ):
                raise BatchV2Error(
                    "production lineage root control differs from its sealed anchor"
                )
            summary = _deep_validate_production_order(
                order,
                profile=profile,
                queue_cache=queue_cache,
            )
        else:
            reference = lineage["fixture_manifest"]
            body, _ = stable_file(
                reference["path"],
                "synthetic fixture manifest",
                maximum=MAX_JSON_BYTES,
                expected_sha256=reference["sha256"],
                exact_mode=0o400,
            )
            summary = _strict_fixture(
                order, body=body, value=parse_json(body, "synthetic fixture manifest")
            )
        items.append(_lineage_item(ordinal, order, summary))
    core = {
        "kind": LINEAGE_ATTESTATION_KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "batch": {
            "batch_id": manifest["batch_id"],
            "identity_sha256": manifest["identity_sha256"],
            "physical_sha256": _digest(
                manifest_physical_sha256, "batch physical SHA-256"
            ),
        },
        "production_profile": {
            "profile_id": profile["profile_id"],
            "identity_sha256": profile["identity_sha256"],
        },
        "root_registration": {
            "registration_id": root_registration["registration_id"],
            "identity_sha256": root_registration["identity_sha256"],
            "filesystem_uuid": root_registration["filesystem"]["uuid"],
        },
        "items": items,
        "policy": dict(LINEAGE_POLICY),
    }
    identity = sha256_bytes(canonical_bytes(core))
    result = {
        **core,
        "identity_sha256": identity,
        "attestation_id": f"gpuasrlineage2_{identity[:32]}",
    }
    body = canonical_bytes(result)
    if len(body) > 1024 * 1024:
        raise BatchV2Error("lineage preflight attestation exceeds 1 MiB")
    return result


def validate_lineage_preflight(
    value: Any,
    *,
    manifest: dict[str, Any],
    manifest_physical_sha256: str,
    profile: dict[str, Any],
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
            "batch_id": manifest["batch_id"],
            "identity_sha256": manifest["identity_sha256"],
            "physical_sha256": manifest_physical_sha256,
        }
        or item["production_profile"]
        != {
            "profile_id": profile["profile_id"],
            "identity_sha256": profile["identity_sha256"],
        }
        or item["root_registration"]
        != {
            "registration_id": root_registration["registration_id"],
            "identity_sha256": root_registration["identity_sha256"],
            "filesystem_uuid": root_registration["filesystem"]["uuid"],
        }
    ):
        raise BatchV2Error("lineage preflight common bindings are invalid")
    rows = item["items"]
    if not isinstance(rows, list) or len(rows) != len(manifest["items"]):
        raise BatchV2Error("lineage preflight item set is incomplete")
    for ordinal, (row, member) in enumerate(
        zip(rows, manifest["items"], strict=True), 1
    ):
        row = _exact(
            row,
            f"lineage preflight item {ordinal}",
            {
                "ordinal",
                "work_order_identity_sha256",
                "lineage",
                "input",
                "status",
            },
        )
        order = member["work_order"]
        lineage = _exact(
            row["lineage"],
            f"lineage preflight item {ordinal} lineage",
            {"kind", "identity_sha256", "source_id", "member_id", "case_id"},
        )
        expected_kind = order["source_lineage"]["kind"]
        if expected_kind == V5.SOURCE_LINEAGE_PRODUCTION:
            source = order["source_lineage"]["gpu_handoff"]
            expected_lineage = {
                "kind": expected_kind,
                "identity_sha256": source["identity_sha256"],
                "source_id": source["queue_id"],
                "member_id": source["member_id"],
                "case_id": None,
            }
        else:
            source = order["source_lineage"]
            expected_lineage = {
                "kind": expected_kind,
                "identity_sha256": source["fixture_manifest"]["identity_sha256"],
                "source_id": source["fixture_manifest"]["fixture_id"],
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
            raise BatchV2Error(f"lineage preflight item {ordinal} differs from v5")
    _semantic_identity(
        item,
        id_name="attestation_id",
        prefix="gpuasrlineage2_",
        label="lineage preflight attestation",
    )
    return item


def _semantic_identity(value: dict[str, Any], *, id_name: str, prefix: str, label: str) -> None:
    identity = _digest(value.get("identity_sha256"), f"{label} identity")
    identifier = value.get(id_name)
    if identifier != f"{prefix}{identity[:32]}":
        raise BatchV2Error(f"{label} ID is inconsistent")
    core = {key: item for key, item in value.items() if key not in {"identity_sha256", id_name}}
    if sha256_bytes(canonical_bytes(core)) != identity:
        raise BatchV2Error(f"{label} semantic identity is invalid")


def expected_item_read_bindings(order: dict[str, Any]) -> list[dict[str, Any]]:
    """Project the only item files that may be visible inside the sandbox."""

    input_item = order["input"]
    bindings: list[dict[str, Any]] = [
        {
            "role": "input_audio",
            "path": input_item["path"],
            "sha256": input_item["expected_sha256"],
            "byte_count": input_item["expected_byte_count"],
            "mode": input_item["sealed_mode"],
        }
    ]
    lineage = order["source_lineage"]
    if lineage["kind"] == V5.SOURCE_LINEAGE_PRODUCTION:
        references = (
            (
                "gpu_handoff_manifest",
                lineage["gpu_handoff"]["manifest_path"],
                lineage["gpu_handoff"]["manifest_sha256"],
                None,
            ),
            (
                "bundle_manifest",
                lineage["bundle_manifest"]["path"],
                lineage["bundle_manifest"]["sha256"],
                None,
            ),
            (
                "preprocess_receipt",
                lineage["receipt"]["path"],
                lineage["receipt"]["sha256"],
                None,
            ),
            (
                "preprocess_result",
                lineage["preprocess_result"]["path"],
                lineage["preprocess_result"]["sha256"],
                lineage["preprocess_result"]["byte_count"],
            ),
        )
    else:
        fixture = lineage["fixture_manifest"]
        references = (("fixture_manifest", fixture["path"], fixture["sha256"], None),)
    for role, path, digest, byte_count in references:
        bindings.append(
            {
                "role": role,
                "path": path,
                "sha256": digest,
                "byte_count": byte_count,
                "mode": None if role == "preprocess_result" else "0400",
            }
        )
    return sorted(bindings, key=lambda row: (row["role"], row["path"]))


def validate_item_read_bindings(plan: dict[str, Any], manifest: dict[str, Any]) -> None:
    observed = plan.get("item_read_bindings")
    if not isinstance(observed, list) or len(observed) != len(manifest["items"]):
        raise BatchV2Error("attested item read-binding set is incomplete")
    for ordinal, (row, member) in enumerate(zip(observed, manifest["items"], strict=True), 1):
        row = _exact(
            row,
            f"attested item read bindings {ordinal}",
            {"ordinal", "work_order_identity_sha256", "bindings"},
        )
        order = member["work_order"]
        if row["ordinal"] != ordinal or row["work_order_identity_sha256"] != order["identity_sha256"]:
            raise BatchV2Error(f"attested item read bindings {ordinal} identify a different work order")
        values = row["bindings"]
        expected = expected_item_read_bindings(order)
        if not isinstance(values, list) or len(values) != len(expected):
            raise BatchV2Error(f"attested item read bindings {ordinal} have the wrong cardinality")
        normalized = []
        for binding_ordinal, (binding, expected_row) in enumerate(zip(values, expected, strict=True), 1):
            binding = _exact(
                binding,
                f"attested binding {ordinal}.{binding_ordinal}",
                {"role", "path", "target", "sha256", "byte_count", "mode"},
            )
            if (
                binding["role"] != expected_row["role"]
                or binding["path"] != expected_row["path"]
                or binding["target"] != expected_row["path"]
                or binding["sha256"] != expected_row["sha256"]
                or (
                    expected_row["mode"] is not None
                    and binding["mode"] != expected_row["mode"]
                )
            ):
                raise BatchV2Error(f"attested binding {ordinal}.{binding_ordinal} differs from the v5 work order")
            if expected_row["mode"] is None and binding["mode"] not in {
                "0400",
                "0440",
                "0444",
            }:
                raise BatchV2Error(
                    f"attested binding {ordinal}.{binding_ordinal} has an unsafe preprocess-result mode"
                )
            byte_count = _integer(
                binding["byte_count"],
                f"attested binding {ordinal}.{binding_ordinal} byte_count",
                1,
                64 * 1024 * 1024,
            )
            if expected_row["byte_count"] is not None and byte_count != expected_row["byte_count"]:
                raise BatchV2Error(f"attested binding {ordinal}.{binding_ordinal} byte count differs")
            normalized.append(binding)
        if normalized != sorted(normalized, key=lambda item: (item["role"], item["path"])):
            raise BatchV2Error(f"attested bindings for item {ordinal} are not deterministically ordered")


def validate_host_envelope(
    value: Any, profile: dict[str, Any], *, enforce: bool
) -> dict[str, Any]:
    """Replay the launcher's bounded cgroup/rlimit observation, not host state."""

    item = _exact(
        value,
        "attested host envelope",
        {
            "cgroup_version", "cgroup_path", "effective", "rlimits",
            "requirements", "status", "violations",
        },
    )
    effective = _exact(
        item["effective"],
        "attested effective cgroup limits",
        {"memory_max_bytes", "memory_swap_max_bytes", "pids_max"},
    )
    limits = _exact(item["rlimits"], "attested rlimits", {"core", "fsize", "nofile"})
    normalized_limits: dict[str, dict[str, int | None]] = {}
    for name in ("core", "fsize", "nofile"):
        row = _exact(limits[name], f"attested {name} rlimit", {"soft", "hard"})
        normalized_limits[name] = {
            bound: (
                None
                if row[bound] is None
                else _integer(row[bound], f"attested {name} {bound}", 0)
            )
            for bound in ("soft", "hard")
        }
    maximum_result = _integer(
        profile["item_limits"]["maximum_result_bytes"],
        "profile maximum_result_bytes",
        1,
        MAX_HOST_FSIZE_BYTES,
    )
    expected_requirements = {
        "memory_max_bytes_at_most": MAX_HOST_MEMORY_BYTES,
        "memory_swap_max_bytes": 0,
        "pids_max_at_most": MAX_HOST_PIDS,
        "nofile_at_most": MAX_HOST_NOFILE,
        "core_bytes": 0,
        "fsize_bytes_at_least": maximum_result,
        "fsize_bytes_at_most": MAX_HOST_FSIZE_BYTES,
    }
    violations = []
    memory = effective["memory_max_bytes"]
    swap = effective["memory_swap_max_bytes"]
    pids = effective["pids_max"]
    if isinstance(memory, bool) or not isinstance(memory, int) or not 1 <= memory <= MAX_HOST_MEMORY_BYTES:
        violations.append("memory.max")
    if swap != 0:
        violations.append("memory.swap.max")
    if isinstance(pids, bool) or not isinstance(pids, int) or not 1 <= pids <= MAX_HOST_PIDS:
        violations.append("pids.max")
    if any(normalized_limits["core"][bound] != 0 for bound in ("soft", "hard")):
        violations.append("RLIMIT_CORE")
    if any(
        not isinstance(normalized_limits["nofile"][bound], int)
        or not 1 <= normalized_limits["nofile"][bound] <= MAX_HOST_NOFILE
        for bound in ("soft", "hard")
    ):
        violations.append("RLIMIT_NOFILE")
    if any(
        not isinstance(normalized_limits["fsize"][bound], int)
        or not maximum_result <= normalized_limits["fsize"][bound] <= MAX_HOST_FSIZE_BYTES
        for bound in ("soft", "hard")
    ):
        violations.append("RLIMIT_FSIZE")
    violations = sorted(set(violations))
    if (
        item["cgroup_version"] != 2
        or not isinstance(item["cgroup_path"], str)
        or not 1 <= len(item["cgroup_path"].encode("utf-8")) <= 4096
        or item["requirements"] != expected_requirements
        or item["violations"] != violations
        or item["status"] != ("passed" if not violations else "report_only_failed")
    ):
        raise BatchV2Error("attested host envelope is noncanonical")
    if enforce and violations:
        raise BatchV2Error("production host envelope failed: " + ", ".join(violations))
    return item


def validate_launch_attestation(
    value: Any,
    *,
    manifest: dict[str, Any],
    manifest_sha256: str,
    runtime: dict[str, Any],
    runtime_sha256: str,
    profile: dict[str, Any],
    profile_sha256: str,
    root: dict[str, Any],
    root_sha256: str,
    lineage_preflight: dict[str, Any],
    lineage_preflight_sha256: str,
) -> dict[str, Any]:
    fields = {
        "kind", "schema_version", "implementation_version", "mode", "nonce",
        "launcher", "runtime_admission", "root_registration", "execution_image",
        "production_profile", "batch", "lineage_preflight", "host_abi",
        "sandbox_plan", "sandbox_plan_identity_sha256",
        "host_envelope",
        "parent_network_namespace", "child_network_namespace_must_differ",
        "host_trust_checks", "trust_boundary", "local_readiness", "policy",
        "identity_sha256", "attestation_id",
    }
    item = _exact(value, "launch attestation", fields)
    expected_mode = _launcher_mode_for_class(manifest["execution_class"])
    if (
        item["kind"] != ATTESTATION_KIND
        or item["schema_version"] != 2
        or item["implementation_version"] != ATTESTATION_VERSION
        or item["mode"] != expected_mode
        or item["policy"] != LAUNCH_POLICY
        or not isinstance(item["nonce"], str)
        or not re.fullmatch(r"[0-9a-f]{64}", item["nonce"])
    ):
        raise BatchV2Error("launch attestation header, mode, nonce, or policy is invalid")
    expected_boundary = {
        "production": {
            "kind": "root_admitted",
            "control_owner": "root",
            "same_uid_mutation_resistance": True,
        },
        "local-private-production": {
            "kind": "current_user_same_uid",
            "control_owner": "current_user",
            "same_uid_mutation_resistance": False,
        },
        "candidate-synthetic-canary": {
            "kind": "synthetic_candidate",
            "control_owner": "candidate_binding",
            "same_uid_mutation_resistance": False,
        },
    }[expected_mode]
    if item["trust_boundary"] != expected_boundary:
        raise BatchV2Error("launch attestation trust boundary is invalid")
    readiness = item["local_readiness"]
    if expected_mode == "local-private-production":
        readiness = _exact(
            readiness,
            "attested local readiness",
            {"path", "sha256", "identity_sha256", "readiness_id"},
        )
        _absolute(readiness["path"], "attested local readiness path")
        identity = _digest(
            readiness["identity_sha256"], "attested local readiness identity"
        )
        _digest(readiness["sha256"], "attested local readiness SHA-256")
        if readiness["readiness_id"] != f"gpulocalready_{identity[:32]}":
            raise BatchV2Error("attested local readiness ID is inconsistent")
    elif readiness is not None:
        raise BatchV2Error("non-local launch may not bind local readiness")
    _semantic_identity(item, id_name="attestation_id", prefix="gpulaunch_", label="launch attestation")
    launcher = _exact(item["launcher"], "attested launcher", {"path", "sha256", "profile"})
    _absolute(launcher["path"], "attested launcher path")
    _digest(launcher["sha256"], "attested launcher SHA-256")
    launcher_profile = _exact(
        launcher["profile"],
        "attested launcher profile",
        {"path", "sha256", "identity_sha256"},
    )
    _absolute(launcher_profile["path"], "attested launcher profile path")
    _digest(launcher_profile["sha256"], "attested launcher profile SHA-256")
    _digest(launcher_profile["identity_sha256"], "attested launcher profile identity")
    execution_image = _exact(
        item["execution_image"],
        "attested execution image",
        {
            "path", "sha256", "byte_count", "identity_sha256",
            "receipt_path", "receipt_sha256",
        },
    )
    for name in ("path", "receipt_path"):
        _absolute(execution_image[name], f"attested execution image {name}")
    for name in ("sha256", "identity_sha256", "receipt_sha256"):
        _digest(execution_image[name], f"attested execution image {name}")
    _integer(execution_image["byte_count"], "attested execution image byte_count", 1, 16 * 1024**3)
    launch_runtime = _exact(item["runtime_admission"], "attested runtime", {"path", "sha256", "identity_sha256", "status"})
    expected_status = _runtime_status_for_class(manifest["execution_class"])
    if (
        launch_runtime["sha256"] != runtime_sha256
        or launch_runtime["identity_sha256"] != runtime.get("identity_sha256")
        or launch_runtime["status"] != expected_status
    ):
        raise BatchV2Error("attested runtime differs from the sealed control")
    launch_profile = _exact(item["production_profile"], "attested profile", {"path", "sha256", "identity_sha256", "gpu_uuid"})
    if (
        launch_profile["sha256"] != profile_sha256
        or launch_profile["identity_sha256"] != profile["identity_sha256"]
        or launch_profile["gpu_uuid"] != profile["hardware"]["gpu_uuid"]
    ):
        raise BatchV2Error("attested production profile differs from the sealed control")
    launch_root = _exact(item["root_registration"], "attested root", {"path", "sha256", "identity_sha256", "registration_id", "root_id", "filesystem_uuid"})
    if (
        launch_root["sha256"] != root_sha256
        or launch_root["identity_sha256"] != root.get("identity_sha256")
        or launch_root["registration_id"] != root.get("registration_id")
        or launch_root["root_id"] != root.get("root_id")
        or launch_root["filesystem_uuid"] != root.get("filesystem", {}).get("uuid")
    ):
        raise BatchV2Error("attested root registration differs from the sealed control")
    batch = _exact(item["batch"], "attested batch", {"path", "sha256", "execution_class", "identity_sha256"})
    if (
        batch["sha256"] != manifest_sha256
        or batch["identity_sha256"] != manifest["identity_sha256"]
        or batch["execution_class"] != manifest["execution_class"]
    ):
        raise BatchV2Error("attested batch differs from the sealed manifest")
    launch_lineage = _exact(
        item["lineage_preflight"],
        "attested lineage preflight",
        {"path", "sha256", "identity_sha256", "attestation_id"},
    )
    if launch_lineage != {
        "path": launch_lineage["path"],
        "sha256": lineage_preflight_sha256,
        "identity_sha256": lineage_preflight["identity_sha256"],
        "attestation_id": lineage_preflight["attestation_id"],
    }:
        raise BatchV2Error("attested lineage preflight differs from the sealed control")
    _absolute(launch_lineage["path"], "attested lineage preflight path")
    host_abi = _exact(
        item["host_abi"],
        "attested host ABI",
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
        host_abi["identity_sha256"], "attested host ABI identity"
    )
    host_abi_count = _integer(
        host_abi["library_count"], "attested host ABI library count", 1, 256
    )
    host_abi_binding_count = _integer(
        host_abi["binding_count"], "attested host ABI binding count", 1, 256
    )
    if (
        host_abi["manifest_id"] != f"gpuhostabi_{host_abi_identity[:32]}"
        or host_abi["platform_replayed"] is not True
        or host_abi["libraries_replayed"] is not True
        or host_abi_binding_count != host_abi_count
    ):
        raise BatchV2Error("attested host ABI replay is inconsistent")
    plan = item["sandbox_plan"]
    plan = _exact(
        plan,
        "attested sandbox plan",
        {
            "mappings", "hot_root", "batch_manifest", "controls",
            "item_read_bindings", "writable_roots", "gpu",
            "host_abi_bindings",
            "system_library_directories", "system_readonly_files",
            "namespaces", "network_access",
        },
    )
    if item["sandbox_plan_identity_sha256"] != sha256_bytes(canonical_bytes(plan)):
        raise BatchV2Error("attested sandbox plan identity is invalid")
    mappings = plan["mappings"]
    if not isinstance(mappings, list) or len(mappings) != len(REQUIRED_MAPPING_LAYOUT):
        raise BatchV2Error("attested execution mapping closure is incomplete")
    mapping_layout: dict[str, tuple[str, str]] = {}
    for ordinal, mapping in enumerate(mappings, 1):
        row = _exact(
            mapping,
            f"attested execution mapping {ordinal}",
            {"name", "image_relative_path", "sandbox_path", "role"},
        )
        if (
            not isinstance(row["name"], str)
            or not isinstance(row["image_relative_path"], str)
            or Path(row["image_relative_path"]).is_absolute()
            or ".." in Path(row["image_relative_path"]).parts
        ):
            raise BatchV2Error("attested execution mapping is malformed")
        mapping_layout[row["name"]] = (row["sandbox_path"], row["role"])
    if (
        mappings != sorted(mappings, key=lambda row: row["name"])
        or mapping_layout != REQUIRED_MAPPING_LAYOUT
    ):
        raise BatchV2Error("attested execution mapping layout is not exact")
    if plan["system_library_directories"] != [] or plan["system_readonly_files"] != []:
        raise BatchV2Error("attested broad host-library allowlist must be empty")
    host_bindings_value = plan["host_abi_bindings"]
    if not isinstance(host_bindings_value, list) or len(host_bindings_value) != host_abi_count:
        raise BatchV2Error("attested host ABI binding count is invalid")
    host_binding_targets: list[str] = []
    for ordinal, value_row in enumerate(host_bindings_value, 1):
        row = _exact(
            value_row,
            f"attested host ABI binding {ordinal}",
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
        source = _absolute(row["source_path"], f"attested host ABI binding {ordinal} source")
        target = _absolute(row["target"], f"attested host ABI binding {ordinal} target")
        if (
            row["role"] != "host_abi_library"
            or source.parent != Path("/usr/lib64")
            or target.parent != Path("/usr/lib64")
            or row["uid"] != 0
            or row["gid"] != 0
            or not isinstance(row["mode"], str)
            or not re.fullmatch(r"[0-7]{4}", row["mode"])
            or int(row["mode"], 8) & 0o7022
            or not int(row["mode"], 8) & 0o444
        ):
            raise BatchV2Error("attested host ABI binding policy is invalid")
        _digest(row["sha256"], f"attested host ABI binding {ordinal} SHA-256")
        _integer(
            row["byte_count"],
            f"attested host ABI binding {ordinal} byte_count",
            1,
            512 * 1024 * 1024,
        )
        host_binding_targets.append(str(target))
    if (
        host_bindings_value != sorted(host_bindings_value, key=lambda row: row["target"])
        or len(set(host_binding_targets)) != len(host_binding_targets)
    ):
        raise BatchV2Error("attested host ABI bindings are duplicated or noncanonical")
    if plan.get("namespaces") != ["cgroup_try", "ipc", "network", "pid", "user", "uts"] or plan.get("network_access") is not False:
        raise BatchV2Error("attested namespace plan is not exact")
    hot_root = _exact(plan["hot_root"], "attested hot-root plan", {"path", "bound"})
    if hot_root != {"path": manifest["hot_root"]["path"], "bound": False}:
        raise BatchV2Error("main sandbox must not expose the registered hot root")
    gpu = _exact(
        plan["gpu"],
        "attested GPU plan",
        {
            "uuid", "host_index", "device_minor", "visible_index", "devices", "driver_version",
            "compute_capability", "minimum_driver_version",
            "minimum_compute_capability",
        },
    )
    observed_capability = gpu["compute_capability"]
    minimum_capability = gpu["minimum_compute_capability"]
    capability_valid = (
        isinstance(observed_capability, list)
        and isinstance(minimum_capability, list)
        and len(observed_capability) == len(minimum_capability) == 2
        and all(
            isinstance(value, int) and not isinstance(value, bool) and value >= 0
            for value in [*observed_capability, *minimum_capability]
        )
    )
    try:
        observed_driver = _version_tuple(
            gpu["driver_version"], "attested GPU driver", parts=3
        )
        minimum_driver = _version_tuple(
            gpu["minimum_driver_version"], "attested minimum GPU driver", parts=3
        )
    except BatchV2Error:
        raise
    if (
        gpu["uuid"] != profile["hardware"]["gpu_uuid"]
        or gpu["visible_index"] != 0
        or isinstance(gpu["host_index"], bool)
        or not isinstance(gpu["host_index"], int)
        or gpu["host_index"] < 0
        or isinstance(gpu["device_minor"], bool)
        or not isinstance(gpu["device_minor"], int)
        or gpu["device_minor"] < 0
        or gpu["minimum_driver_version"] != profile["hardware"]["minimum_driver_version"]
        or gpu["driver_version"] != profile["hardware"]["minimum_driver_version"]
        or gpu["minimum_compute_capability"] != profile["hardware"]["minimum_compute_capability"]
        or not capability_valid
        or tuple(observed_capability) < tuple(minimum_capability)
        or observed_driver < minimum_driver
        or not isinstance(gpu["devices"], list)
        or not gpu["devices"]
        or gpu["devices"]
        != [
            "/dev/nvidiactl",
            "/dev/nvidia-uvm",
            "/dev/nvidia-uvm-tools",
            f"/dev/nvidia{gpu['device_minor']}",
        ]
    ):
        raise BatchV2Error("attested GPU plan is invalid")
    expected_controls = {
        "runtime": "/run/himr-gpu/control/runtime.json",
        "profile": "/run/himr-gpu/control/profile.json",
        "root": "/run/himr-gpu/control/root.json",
        "lineage_preflight": "/run/himr-gpu/control/lineage-preflight.json",
    }
    controls = plan.get("controls")
    control_sources = {
        "runtime": (item["runtime_admission"], runtime.get("identity_sha256")),
        "profile": (item["production_profile"], profile.get("identity_sha256")),
        "root": (item["root_registration"], root.get("identity_sha256")),
        "lineage_preflight": (item["lineage_preflight"], lineage_preflight.get("identity_sha256")),
    }
    if not isinstance(controls, dict) or set(controls) != set(expected_controls):
        raise BatchV2Error("attested control mounts are invalid")
    for name, target in expected_controls.items():
        control = _exact(
            controls[name],
            f"attested {name} control",
            {"path", "target", "work_order_target", "sha256", "identity_sha256"},
        )
        top, identity = control_sources[name]
        work_order_targets = {
            "runtime": manifest["runtime_admission"]["receipt_path"],
            "profile": manifest["production_profile"]["path"],
            "root": manifest["hot_root"]["registration_path"],
            "lineage_preflight": None,
        }
        if control != {
            "path": top["path"],
            "target": target,
            "work_order_target": work_order_targets[name],
            "sha256": top["sha256"],
            "identity_sha256": identity,
        }:
            raise BatchV2Error(f"attested {name} control differs from its top-level binding")
    batch_mount = plan.get("batch_manifest")
    batch_mount = _exact(
        batch_mount,
        "attested batch mount",
        {"path", "sha256", "identity_sha256", "target"},
    )
    if batch_mount != {
        "path": batch["path"],
        "sha256": batch["sha256"],
        "identity_sha256": batch["identity_sha256"],
        "target": "/run/himr-gpu/input/batch.json",
    }:
        raise BatchV2Error("attested batch mount is invalid")
    writable = plan.get("writable_roots")
    expected_writable = {name: manifest["writable_roots"][name] for name in ("result", "event", "lock")}
    if not isinstance(writable, dict) or set(writable) != set(expected_writable):
        raise BatchV2Error("attested writable-root set is invalid")
    for name, path in expected_writable.items():
        if writable[name] != {"source": path, "sandbox": path}:
            raise BatchV2Error(f"attested writable {name} root differs from the manifest")
    validate_item_read_bindings(plan, manifest)
    if item["child_network_namespace_must_differ"] is not True or not isinstance(item["parent_network_namespace"], str) or not NETWORK_NAMESPACE_RE.fullmatch(item["parent_network_namespace"]):
        raise BatchV2Error("attested parent network namespace is invalid")
    expected_checks = {
        "runtime_receipt_owner_checked": True,
        "root_registration_replayed": True,
        "execution_image_full_sha256_checked": True,
        "launcher_and_tools_sha256_checked": True,
        "host_abi_manifest_replayed": True,
        "gpu_uuid_resolved_at_launch": True,
    }
    if item["host_trust_checks"] != expected_checks:
        raise BatchV2Error("attested host trust checks are not exact")
    validate_host_envelope(
        item["host_envelope"],
        profile,
        enforce=expected_mode in {"production", "local-private-production"},
    )
    return item


def network_isolation(attestation: dict[str, Any]) -> dict[str, Any]:
    try:
        current = os.readlink("/proc/self/ns/net")
        lines = Path("/proc/net/dev").read_text(encoding="utf-8").splitlines()[2:]
    except OSError as error:
        raise BatchV2Error(f"network namespace evidence is unavailable: {error}") from error
    parent = attestation["parent_network_namespace"]
    if current == parent:
        raise BatchV2Error("worker did not enter a distinct network namespace")
    interfaces = sorted({line.split(":", 1)[0].strip() for line in lines if ":" in line})
    if any(name != "lo" for name in interfaces):
        raise BatchV2Error(f"network namespace exposes non-loopback interfaces: {interfaces}")
    return {"parent": parent, "current": current, "interfaces": interfaces, "verified": True}


def require_loopback_only_network() -> dict[str, Any]:
    """Verify phase-one has no externally routable network interface."""

    try:
        namespace = os.readlink("/proc/self/ns/net")
        lines = Path("/proc/net/dev").read_text(encoding="utf-8").splitlines()[2:]
    except OSError as error:
        raise BatchV2Error(f"network namespace evidence is unavailable: {error}") from error
    interfaces = sorted({line.split(":", 1)[0].strip() for line in lines if ":" in line})
    if any(name != "lo" for name in interfaces):
        raise BatchV2Error(f"lineage preflight exposes non-loopback interfaces: {interfaces}")
    return {"namespace": namespace, "interfaces": interfaces, "verified": True}


@dataclass
class RetainedInput:
    ordinal: int
    work_order: dict[str, Any]
    descriptor: int
    info: os.stat_result
    hash_seconds: float
    probe_seconds: float
    probe: dict[str, Any]
    memfd_seals: int

    @property
    def fd_path(self) -> str:
        return f"/proc/self/fd/{self.descriptor}"

    def verify(self) -> None:
        if _identity(os.fstat(self.descriptor)) != _identity(self.info):
            raise BatchV2Error(f"input {self.ordinal} sealed memfd changed")
        observed_seals = fcntl.fcntl(self.descriptor, F_GET_SEALS)
        if (
            self.memfd_seals != MEMFD_REQUIRED_SEALS
            or observed_seals != MEMFD_REQUIRED_SEALS
            or stat.S_IMODE(os.fstat(self.descriptor).st_mode) != 0o400
        ):
            raise BatchV2Error(f"input {self.ordinal} memfd is not irrevocably sealed")

    def close(self) -> None:
        os.close(self.descriptor)


def _hash_descriptor(descriptor: int, size: int) -> str:
    digest = hashlib.sha256()
    offset = 0
    while offset < size:
        part = os.pread(descriptor, min(HASH_CHUNK_BYTES, size - offset), offset)
        if not part:
            raise BatchV2Error("retained input ended before its sealed size")
        digest.update(part)
        offset += len(part)
    if os.pread(descriptor, 1, size):
        raise BatchV2Error("retained input grew beyond its sealed size")
    return digest.hexdigest()


if not sys.platform.startswith("linux"):
    raise BatchV2Error("production batch v2 requires Linux memfd sealing")

# Stable Linux UAPI values.  CPython 3.12.14 in the admitted runtime omits
# these attributes even though the kernel supports the underlying fcntl calls.
F_ADD_SEALS = getattr(fcntl, "F_ADD_SEALS", 1033)
F_GET_SEALS = getattr(fcntl, "F_GET_SEALS", 1034)
F_SEAL_SEAL = getattr(fcntl, "F_SEAL_SEAL", 0x0001)
F_SEAL_SHRINK = getattr(fcntl, "F_SEAL_SHRINK", 0x0002)
F_SEAL_GROW = getattr(fcntl, "F_SEAL_GROW", 0x0004)
F_SEAL_WRITE = getattr(fcntl, "F_SEAL_WRITE", 0x0008)
MEMFD_REQUIRED_SEALS = F_SEAL_WRITE | F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL


def sealed_memfd_from_source(
    source_descriptor: int,
    source_info: os.stat_result,
    *,
    expected_sha256: str,
    label: str,
) -> tuple[int, os.stat_result, float]:
    """Copy/hash one source into an irrevocably sealed anonymous file."""

    if not hasattr(os, "memfd_create"):
        raise BatchV2Error("Linux memfd_create is required for immutable inference input")
    descriptor = os.memfd_create(
        f"himr-gpu-asr-{secrets.token_hex(8)}",
        os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING,
    )
    started = time.monotonic()
    digest = hashlib.sha256()
    offset = 0
    try:
        while offset < source_info.st_size:
            part = os.pread(
                source_descriptor,
                min(HASH_CHUNK_BYTES, source_info.st_size - offset),
                offset,
            )
            if not part:
                raise BatchV2Error(f"{label} ended during immutable copy")
            digest.update(part)
            written = 0
            while written < len(part):
                count = os.write(descriptor, part[written:])
                if count <= 0:
                    raise BatchV2Error(f"{label} immutable copy made no progress")
                written += count
            offset += len(part)
        if os.pread(source_descriptor, 1, source_info.st_size):
            raise BatchV2Error(f"{label} grew during immutable copy")
        if digest.hexdigest() != expected_sha256:
            raise BatchV2Error(f"{label} SHA-256 differs during immutable copy")
        os.fchmod(descriptor, 0o400)
        fcntl.fcntl(descriptor, F_ADD_SEALS, MEMFD_REQUIRED_SEALS)
        observed_seals = fcntl.fcntl(descriptor, F_GET_SEALS)
        observed = os.fstat(descriptor)
        if (
            observed_seals != MEMFD_REQUIRED_SEALS
            or observed.st_size != source_info.st_size
            or stat.S_IMODE(observed.st_mode) != 0o400
            or not stat.S_ISREG(observed.st_mode)
        ):
            raise BatchV2Error(f"{label} memfd seal verification failed")
        os.lseek(descriptor, 0, os.SEEK_SET)
        return descriptor, observed, time.monotonic() - started
    except Exception:
        os.close(descriptor)
        raise


def _pyav_module() -> Any:
    try:
        import av
    except ImportError as error:
        raise BatchV2Error(f"authenticated PyAV runtime is unavailable: {error}") from error
    return av


def _probe_fd(
    retained: RetainedInput | tuple[int, dict[str, Any]],
    *,
    av_module: Any | None = None,
    expected_av_version: str | None = None,
) -> dict[str, Any]:
    """Probe the sealed memfd through the authenticated decode dependency."""

    if isinstance(retained, RetainedInput):
        descriptor = retained.descriptor
        order = retained.work_order
    else:
        descriptor, order = retained
    av_module = _pyav_module() if av_module is None else av_module
    observed_av_version = str(getattr(av_module, "__version__", ""))
    if expected_av_version is not None and observed_av_version != expected_av_version:
        raise BatchV2Error("imported PyAV version differs from runtime admission")
    os.lseek(descriptor, 0, os.SEEK_SET)
    container = None
    try:
        container = av_module.open(f"/proc/self/fd/{descriptor}", mode="r")
        streams = list(container.streams)
        audio_streams = [stream for stream in streams if getattr(stream, "type", None) == "audio"]
        if len(streams) != 1 or len(audio_streams) != 1:
            raise BatchV2Error("input must contain exactly one audio stream and no other streams")
        stream = audio_streams[0]
        codec = stream.codec_context
        durations: list[float] = []
        container_duration = getattr(container, "duration", None)
        if isinstance(container_duration, int) and not isinstance(container_duration, bool) and container_duration > 0:
            durations.append(container_duration / int(av_module.time_base))
        stream_duration = getattr(stream, "duration", None)
        stream_time_base = getattr(stream, "time_base", None)
        if isinstance(stream_duration, int) and not isinstance(stream_duration, bool) and stream_duration > 0 and stream_time_base is not None:
            durations.append(stream_duration * float(stream_time_base))
        if not durations or any(not math.isfinite(value) or value <= 0 for value in durations):
            raise BatchV2Error("PyAV returned no finite positive duration")
        if max(durations) - min(durations) > 0.001:
            raise BatchV2Error("PyAV container and stream durations disagree")
        duration = durations[0]
        format_name = str(container.format.name).split(",", 1)[0]
        sample_format = getattr(getattr(codec, "format", None), "name", None)
        layout = getattr(getattr(codec, "layout", None), "name", None)
        observed = {
            "engine": "pyav_authenticated_execution_image",
            "engine_version": observed_av_version,
            "container": format_name,
            "codec": getattr(codec, "name", None),
            "sample_rate_hz": int(getattr(codec, "sample_rate", 0)),
            "channels": int(getattr(codec, "channels", 0)),
            "channel_layout": layout,
            "sample_format": sample_format,
            "duration_ms": round(duration * 1000),
            "duration_source_count": len(durations),
        }
    except BatchV2Error:
        raise
    except Exception as error:
        raise BatchV2Error(f"authenticated PyAV probe failed: {error}") from error
    finally:
        if container is not None:
            with contextlib.suppress(Exception):
                container.close()
    expected = order["input"]["media_format"]
    for name in ("container", "codec", "sample_rate_hz", "channels", "sample_format"):
        if observed[name] != expected[name]:
            raise BatchV2Error(f"PyAV {name} differs from the work order")
    if observed["channel_layout"] != "mono":
        raise BatchV2Error("PyAV channel layout differs from normalized mono")
    if observed["duration_ms"] != order["input"]["expected_duration_ms"]:
        raise BatchV2Error("PyAV duration differs from the work order")
    return observed


def retain_and_preflight_input(
    ordinal: int,
    order: dict[str, Any],
    *,
    av_module: Any | None = None,
    expected_av_version: str | None = None,
) -> RetainedInput:
    path = _absolute(order["input"]["path"], f"input {ordinal} path")
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    before = path.lstat()
    source_descriptor = os.open(path, flags)
    memfd_descriptor: int | None = None
    try:
        opened = os.fstat(source_descriptor)
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or _identity(before) != _identity(opened)
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != int(order["input"]["sealed_mode"], 8)
            or stat.S_IMODE(opened.st_mode) & 0o222
            or opened.st_size != order["input"]["expected_byte_count"]
        ):
            raise BatchV2Error(f"input {ordinal} metadata differs from its sealed work order")
        memfd_descriptor, memfd_info, hash_seconds = sealed_memfd_from_source(
            source_descriptor,
            opened,
            expected_sha256=order["input"]["expected_sha256"],
            label=f"input {ordinal}",
        )
        if _identity(os.fstat(source_descriptor)) != _identity(opened) or _identity(path.lstat()) != _identity(opened):
            raise BatchV2Error(f"input {ordinal} changed during immutable copy")
        os.close(source_descriptor)
        source_descriptor = -1
        probe_started = time.monotonic()
        probe = _probe_fd(
            (memfd_descriptor, order),
            av_module=av_module,
            expected_av_version=expected_av_version,
        )
        probe_seconds = time.monotonic() - probe_started
        os.lseek(memfd_descriptor, 0, os.SEEK_SET)
        retained = RetainedInput(
            ordinal,
            order,
            memfd_descriptor,
            memfd_info,
            hash_seconds,
            probe_seconds,
            probe,
            MEMFD_REQUIRED_SEALS,
        )
        retained.verify()
        memfd_descriptor = None
        return retained
    except Exception:
        if source_descriptor >= 0:
            os.close(source_descriptor)
        if memfd_descriptor is not None:
            os.close(memfd_descriptor)
        raise


def _transcript_document(core: dict[str, Any], prefix: str) -> dict[str, Any]:
    identity = sha256_bytes(canonical_bytes(core))
    return {**core, "identity_sha256": identity, "document_id": f"{prefix}_{identity[:32]}"}


def _finite_optional(value: Any, label: str, minimum: float, maximum: float) -> float | None:
    if value is None:
        return None
    return _number(value, label, minimum, maximum)


def raw_segment(segment: Any, ordinal: int, profile: dict[str, Any]) -> dict[str, Any]:
    maximum = profile["item_limits"]["maximum_audio_seconds"] + 10
    start = _number(segment.start, f"segment {ordinal} start", 0, maximum)
    end = _number(segment.end, f"segment {ordinal} end", start, maximum)
    text = str(segment.text)
    if len(text) > V5.MAX_TEXT_CHARACTERS:
        raise BatchV2Error(f"segment {ordinal} text is too large")
    words = []
    for word_ordinal, word in enumerate(getattr(segment, "words", None) or []):
        word_start = _finite_optional(getattr(word, "start", None), f"word {ordinal}.{word_ordinal} start", 0, maximum)
        word_end = _finite_optional(getattr(word, "end", None), f"word {ordinal}.{word_ordinal} end", 0, maximum)
        if word_start is not None and word_end is not None and word_end < word_start:
            raise BatchV2Error(f"word {ordinal}.{word_ordinal} has inverted timing")
        words.append({
            "ordinal": word_ordinal,
            "start_seconds": word_start,
            "end_seconds": word_end,
            "text": str(word.word),
            "probability_raw": _finite_optional(getattr(word, "probability", None), f"word {ordinal}.{word_ordinal} probability", 0, 1),
        })
    tokens = list(getattr(segment, "tokens", None) or [])
    if any(isinstance(token, bool) or not isinstance(token, int) for token in tokens):
        raise BatchV2Error(f"segment {ordinal} token IDs are invalid")
    return {
        "ordinal": ordinal,
        "engine_segment_id": int(getattr(segment, "id", ordinal)),
        "seek": int(getattr(segment, "seek", 0)),
        "start_seconds": start,
        "end_seconds": end,
        "text": text,
        "token_ids": tokens,
        "temperature_raw": _finite_optional(getattr(segment, "temperature", None), f"segment {ordinal} temperature", 0, 10),
        "average_log_probability_raw": _finite_optional(getattr(segment, "avg_logprob", None), f"segment {ordinal} average log probability", -1000, 1000),
        "compression_ratio_raw": _finite_optional(getattr(segment, "compression_ratio", None), f"segment {ordinal} compression ratio", 0, 1_000_000),
        "no_speech_probability_raw": _finite_optional(getattr(segment, "no_speech_prob", None), f"segment {ordinal} no-speech probability", 0, 1),
        "words": words,
    }


def build_raw_transcript(info: Any, segments: Sequence[dict[str, Any]], order: dict[str, Any], profile: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
    core = {
        "kind": "faster_whisper_raw_transcript",
        "schema_version": 5,
        "engine": {
            "library": "faster-whisper",
            "library_version": runtime["runtime"]["packages"]["faster-whisper"],
            "model_identity_sha256": profile["model"]["identity_sha256"],
            "model_revision": profile["model"]["revision"],
        },
        "input": {"sha256": order["input"]["expected_sha256"], "duration_ms": order["input"]["expected_duration_ms"], "timeline_offset_ms": 0},
        "language": {"value": "en", "selection_basis": "forced_by_inference_profile", "detection_performed": False, "probability_raw": None, "all_probabilities_raw": []},
        "duration_seconds_raw": _number(getattr(info, "duration", order["input"]["expected_duration_ms"] / 1000), "transcript duration", 0, profile["item_limits"]["maximum_audio_seconds"] + 10),
        "duration_after_vad_seconds_raw": _finite_optional(getattr(info, "duration_after_vad", None), "duration after VAD", 0, profile["item_limits"]["maximum_audio_seconds"] + 10),
        "segments": list(segments),
        "score_semantics": "raw_model_outputs_uncalibrated",
        "transcript_semantics": V5.TRANSCRIPT_SEMANTICS,
        "policy": dict(V5.POLICY),
    }
    return _transcript_document(core, "gpuasrraw")


def _timestamp_ms(value: Any, label: str, duration_ms: int, maximum_seconds: float) -> tuple[int, bool]:
    milliseconds = round(_number(value, label, 0, maximum_seconds + 10) * 1000)
    if milliseconds > duration_ms + V5.MAX_TIMESTAMP_OVERRUN_MS:
        raise BatchV2Error(f"{label} exceeds the admitted input duration")
    return min(milliseconds, duration_ms), milliseconds > duration_ms


def normalize_raw_transcript(raw: dict[str, Any], order: dict[str, Any], profile: dict[str, Any]) -> dict[str, Any]:
    duration_ms = order["input"]["expected_duration_ms"]
    rows = raw.get("segments")
    if not isinstance(rows, list) or len(rows) > profile["item_limits"]["maximum_segments"]:
        raise BatchV2Error("raw transcript segment array exceeds its bound")
    normalized_segments = []
    flag_counts = {name: 0 for name in V5.WORD_TIMING_FLAG_NAMES}
    anomalous_words = 0
    word_count = 0
    previous_segment_start = 0
    for ordinal, row in enumerate(rows):
        if not isinstance(row, dict):
            raise BatchV2Error(f"raw segment {ordinal} is not an object")
        start_ms, start_clipped = _timestamp_ms(row.get("start_seconds"), f"segment {ordinal} start", duration_ms, profile["item_limits"]["maximum_audio_seconds"])
        end_ms, end_clipped = _timestamp_ms(row.get("end_seconds"), f"segment {ordinal} end", duration_ms, profile["item_limits"]["maximum_audio_seconds"])
        if end_ms < start_ms or start_ms < previous_segment_start:
            raise BatchV2Error("raw segment timing is inverted or non-monotonic")
        previous_segment_start = start_ms
        raw_words = row.get("words")
        if not isinstance(raw_words, list):
            raise BatchV2Error(f"raw segment {ordinal} words are invalid")
        words = []
        previous_raw_start: float | None = None
        previous_raw_end: float | None = None
        segment_flags = {name: 0 for name in V5.WORD_TIMING_FLAG_NAMES}
        segment_anomalies = 0
        for word_ordinal, word in enumerate(raw_words):
            if not isinstance(word, dict):
                raise BatchV2Error(f"raw word {ordinal}.{word_ordinal} is invalid")
            raw_start = word.get("start_seconds")
            raw_end = word.get("end_seconds")
            flags = {name: False for name in V5.WORD_TIMING_FLAG_NAMES}
            clipped = False
            if raw_start is None or raw_end is None:
                local_start = None
                local_end = None
            else:
                raw_start = _number(raw_start, f"word {ordinal}.{word_ordinal} start", 0, profile["item_limits"]["maximum_audio_seconds"] + 10)
                raw_end = _number(raw_end, f"word {ordinal}.{word_ordinal} end", raw_start, profile["item_limits"]["maximum_audio_seconds"] + 10)
                local_start, clipped_start = _timestamp_ms(raw_start, f"word {ordinal}.{word_ordinal} start", duration_ms, profile["item_limits"]["maximum_audio_seconds"])
                local_end, clipped_end = _timestamp_ms(raw_end, f"word {ordinal}.{word_ordinal} end", duration_ms, profile["item_limits"]["maximum_audio_seconds"])
                clipped = clipped_start or clipped_end
                flags = {
                    "precedes_segment_start": raw_start < row["start_seconds"],
                    "extends_beyond_segment_end": raw_end > row["end_seconds"],
                    "start_regresses_from_previous": previous_raw_start is not None and raw_start < previous_raw_start,
                    "overlaps_previous": previous_raw_end is not None and raw_start < previous_raw_end,
                }
                previous_raw_start = raw_start
                previous_raw_end = raw_end
            if any(flags.values()):
                anomalous_words += 1
                segment_anomalies += 1
            for name, present in flags.items():
                if present:
                    flag_counts[name] += 1
                    segment_flags[name] += 1
            words.append({
                "ordinal": word_ordinal,
                "start_ms": local_start,
                "end_ms": local_end,
                "source_start_ms": local_start,
                "source_end_ms": local_end,
                "text": str(word.get("text", "")),
                "probability_raw": word.get("probability_raw"),
                "probability_calibrated": None,
                "timing_clipped_to_input": clipped,
                "timing_anomaly_flags": flags,
            })
        word_count += len(words)
        if word_count > profile["item_limits"]["maximum_words"]:
            raise BatchV2Error("normalized transcript word count exceeds its bound")
        normalized_segments.append({
            "ordinal": ordinal,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "source_start_ms": start_ms,
            "source_end_ms": end_ms,
            "timing_clipped_to_input": start_clipped or end_clipped,
            "text": str(row.get("text", "")),
            "words": words,
            "score_provenance": {
                "temperature_raw": row.get("temperature_raw"),
                "average_log_probability_raw": row.get("average_log_probability_raw"),
                "compression_ratio_raw": row.get("compression_ratio_raw"),
                "no_speech_probability_raw": row.get("no_speech_probability_raw"),
                "scores_calibrated": False,
            },
            "word_timing_anomaly_count": segment_anomalies,
            "word_timing_anomaly_flag_counts": segment_flags,
        })
    language = {"value": "en", "selection_basis": "forced_by_inference_profile", "detection_performed": False, "raw_probability": None, "calibrated_probability": None}
    anomalies = {
        "anomalous_word_count": anomalous_words,
        "total_flag_count": sum(flag_counts.values()),
        "flag_counts": flag_counts,
        "human_review_required": True,
    }
    text_value = "".join(str(row["text"]) for row in normalized_segments).strip()
    core = {
        "kind": "transcript_normalized",
        "schema_version": 5,
        "input": {"sha256": order["input"]["expected_sha256"], "duration_ms": duration_ms, "timeline_offset_ms": 0},
        "language": language,
        "timeline": {"coordinate_system": "media_ms", "source_duration_ms": duration_ms, "source_offset_ms": 0, "end_ms": duration_ms},
        "segments": normalized_segments,
        "text": text_value,
        "segment_count": len(normalized_segments),
        "word_count": word_count,
        "word_timing_anomalies": anomalies,
        "review_status": "unreviewed_machine_output",
        "semantics": V5.TRANSCRIPT_SEMANTICS,
        "policy": dict(V5.POLICY),
    }
    return _transcript_document(core, "gpuasrnorm")


def validate_transcript_document(value: Any, *, prefix: str, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) < {"identity_sha256", "document_id"}:
        raise BatchV2Error(f"{label} is not an identity document")
    _semantic_identity(value, id_name="document_id", prefix=f"{prefix}_", label=label)
    return value


class HardDeadline:
    """Process-wide fail-stop deadline for native code that cannot be cancelled."""

    def __init__(self, seconds: float, *, fatal: Callable[[int], None] = os._exit) -> None:
        self.seconds = seconds
        self.fatal = fatal
        self.cancel = threading.Event()
        self.thread = threading.Thread(target=self._watch, name="himr-gpu-batch-v2-deadline", daemon=True)

    def _watch(self) -> None:
        if not self.cancel.wait(self.seconds):
            self.fatal(124)

    def __enter__(self) -> "HardDeadline":
        self.thread.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.cancel.set()
        self.thread.join(timeout=2)
        if self.thread.is_alive():
            raise BatchV2Error("deadline watchdog did not stop")


@dataclass
class ComputedItem:
    retained: RetainedInput
    started_at: str
    completed_at: str
    started_monotonic: float
    completed_monotonic: float
    setup_seconds: float
    iterator_seconds: float
    normalization_seconds: float
    serialization_seconds: float
    raw: dict[str, Any]
    normalized: dict[str, Any]
    raw_body: bytes
    normalized_body: bytes


def compute_item(model: Any, retained: RetainedInput, *, profile: dict[str, Any], runtime: dict[str, Any]) -> ComputedItem:
    order = retained.work_order
    decoding = profile["decoding"]
    start_at = utc_now()
    start = time.monotonic()
    setup_start = time.monotonic()
    retained.verify()
    os.lseek(retained.descriptor, 0, os.SEEK_SET)
    segments, info = model.transcribe(
        retained.fd_path,
        language=decoding["language"],
        beam_size=decoding["beam_size"],
        best_of=decoding["best_of"],
        temperature=decoding["temperature"],
        condition_on_previous_text=decoding["condition_on_previous_text"],
        word_timestamps=decoding["word_timestamps"],
        vad_filter=decoding["vad_filter"],
    )
    setup_seconds = time.monotonic() - setup_start
    iterator_start = time.monotonic()
    raw_rows = []
    for ordinal, segment in enumerate(segments):
        if ordinal >= profile["item_limits"]["maximum_segments"]:
            raise PairFailure("model iterator exceeded maximum_segments")
        raw_rows.append(raw_segment(segment, ordinal, profile))
    iterator_seconds = time.monotonic() - iterator_start
    normalize_start = time.monotonic()
    raw = build_raw_transcript(info, raw_rows, order, profile, runtime)
    normalized = normalize_raw_transcript(raw, order, profile)
    normalization_seconds = time.monotonic() - normalize_start
    serialize_start = time.monotonic()
    raw_body = canonical_bytes(raw)
    normalized_body = canonical_bytes(normalized)
    serialization_seconds = time.monotonic() - serialize_start
    maximum = profile["item_limits"]["maximum_result_bytes"]
    if len(raw_body) > maximum or len(normalized_body) > maximum:
        raise PairFailure("transcript artifact exceeds maximum_result_bytes")
    completed = time.monotonic()
    return ComputedItem(
        retained, start_at, utc_now(), start, completed, setup_seconds,
        iterator_seconds, normalization_seconds, serialization_seconds,
        raw, normalized, raw_body, normalized_body,
    )


def _artifact(document: dict[str, Any], body: bytes, kind: str, path: str) -> dict[str, Any]:
    return {
        "artifact_kind": kind,
        "path": path,
        "sha256": sha256_bytes(body),
        "byte_count": len(body),
        "document_id": document["document_id"],
        "identity_sha256": document["identity_sha256"],
    }


def make_item_result(
    computed: ComputedItem,
    *,
    profile: dict[str, Any],
    telemetry: dict[str, Any],
    runtime: dict[str, Any],
    attempt_id: str,
    model_load_seconds: float,
) -> tuple[dict[str, Any], bytes]:
    order = computed.retained.work_order
    plan = V5.result_plan(order, profile_document=profile)
    phases = {
        "preflight_input_hash": computed.retained.hash_seconds,
        # V5 retains the historical phase key; batch v2 now performs this
        # probe through authenticated PyAV rather than a host ffprobe process.
        "preflight_ffprobe": computed.retained.probe_seconds,
        "transcribe_setup": computed.setup_seconds,
        "model_iterator": computed.iterator_seconds,
        "transcript_normalization": computed.normalization_seconds,
        "artifact_serialization": computed.serialization_seconds,
    }
    inference = computed.setup_seconds + computed.iterator_seconds
    wall = (
        computed.completed_monotonic
        - computed.started_monotonic
        + computed.retained.hash_seconds
        + computed.retained.probe_seconds
        + model_load_seconds
    )
    normalized = computed.normalized
    core = {
        "kind": V5.RESULT_KIND,
        "schema_version": V5.CONTRACT_VERSION,
        "implementation_version": V5.IMPLEMENTATION_VERSION,
        "status": "completed",
        "work_order": {"work_order_id": order["work_order_id"], "identity_sha256": order["identity_sha256"]},
        "runtime_admission": {"receipt_id": order["runtime_admission"]["receipt_id"], "identity_sha256": order["runtime_admission"]["identity_sha256"]},
        "production_profile": {"profile_id": order["production_profile"]["profile_id"], "identity_sha256": order["production_profile"]["identity_sha256"]},
        "input": {
            "sha256": order["input"]["expected_sha256"],
            "byte_count": order["input"]["expected_byte_count"],
            "duration_ms": order["input"]["expected_duration_ms"],
            "media_id": order["input"]["media_id"],
            "artifact_id": order["input"]["artifact_id"],
            "timeline_offset_ms": 0,
        },
        "execution": {
            "attempt_id": attempt_id,
            "started_at": computed.started_at,
            "completed_at": computed.completed_at,
            "wall_seconds": wall,
            # This is the same shared batch load observation in every member
            # envelope; it does not imply one physical load per item.
            "model_load_seconds": model_load_seconds,
            "inference_seconds": inference,
            "phase_seconds": phases,
            "model_load_count": profile["batch_limits"]["model_load_count"],
            "live_gpu_uuid": profile["hardware"]["gpu_uuid"],
            "telemetry": telemetry,
        },
        "transcript": {
            "language": normalized["language"],
            "timeline": normalized["timeline"],
            "segment_count": normalized["segment_count"],
            "word_count": normalized["word_count"],
            "text_character_count": len(normalized["text"]),
            "word_timing_anomalies": normalized["word_timing_anomalies"],
            "review_status": "unreviewed_machine_output",
            "semantics": V5.TRANSCRIPT_SEMANTICS,
        },
        "artifacts": [
            _artifact(computed.raw, computed.raw_body, "faster_whisper_raw_transcript_json", plan["raw_transcript_path"]),
            _artifact(computed.normalized, computed.normalized_body, "transcript_normalized_json", plan["normalized_transcript_path"]),
        ],
        "policy": dict(V5.POLICY),
    }
    try:
        result = V5.make_result(core, work_order=order, profile_document=profile)
    except Exception as error:
        raise PairFailure(f"v5 result validation failed: {error}") from error
    body = canonical_bytes(result)
    if len(body) > profile["item_limits"]["maximum_result_bytes"]:
        raise PairFailure("result envelope exceeds maximum_result_bytes")
    return result, body


def _rename_noreplace_at(
    source_parent_fd: int,
    source_name: str,
    target_parent_fd: int,
    target_name: str,
) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise BatchV2Error("Linux renameat2 is required for atomic no-replace publication")
    renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    renameat2.restype = ctypes.c_int
    source_name = _component(source_name, "staging directory name")
    target_name = _component(target_name, "result directory name")
    result = renameat2(
        source_parent_fd,
        os.fsencode(source_name),
        target_parent_fd,
        os.fsencode(target_name),
        1,
    )
    if result != 0:
        observed = ctypes.get_errno()
        raise OSError(observed, os.strerror(observed), target_name)


def publish_result_bundle(
    computed: ComputedItem,
    result: dict[str, Any],
    result_body: bytes,
    *,
    profile: dict[str, Any],
    attempt_id: str,
    result_root: RetainedDirectory,
) -> float:
    plan = V5.result_plan(computed.retained.work_order, profile_document=profile)
    result_dir = Path(plan["result_directory"])
    output_root = Path(computed.retained.work_order["output"]["root"])
    if result_root.path != output_root:
        raise BatchV2Error("retained result root differs from the work order")
    relative_parent = result_dir.parent.relative_to(output_root)
    stage_name = f".{result_dir.name}.stage-{attempt_id}-{secrets.token_hex(8)}"
    started = time.monotonic()
    with retained_private_chain(result_root, relative_parent, "result parent") as parent:
        parent.verify()
        try:
            os.stat(result_dir.name, dir_fd=parent.descriptor, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise BatchV2Error(f"refusing to replace result directory {result_dir}")
        os.mkdir(stage_name, 0o700, dir_fd=parent.descriptor)
        stage_fd = os.open(
            stage_name,
            os.O_RDONLY
            | os.O_DIRECTORY
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent.descriptor,
        )
        published = False
        known_leaves = (
            "transcript.raw.json",
            "transcript.normalized.json",
            "result.json",
        )
        try:
            stage_info = os.fstat(stage_fd)
            if (
                not stat.S_ISDIR(stage_info.st_mode)
                or stage_info.st_uid != os.geteuid()
                or stat.S_IMODE(stage_info.st_mode) != 0o700
            ):
                raise BatchV2Error("result staging directory is unsafe")
            parent.verify()
            _write_new_at(stage_fd, known_leaves[0], computed.raw_body)
            _write_new_at(stage_fd, known_leaves[1], computed.normalized_body)
            _write_new_at(stage_fd, known_leaves[2], result_body)
            os.fsync(stage_fd)
            parent.verify()
            _rename_noreplace_at(
                parent.descriptor,
                stage_name,
                parent.descriptor,
                result_dir.name,
            )
            linked_result = os.stat(
                result_dir.name,
                dir_fd=parent.descriptor,
                follow_symlinks=False,
            )
            if (
                _directory_authority_identity(linked_result)
                != _directory_authority_identity(stage_info)
            ):
                raise BatchV2Error("published result link differs from the staging inode")
            published = True
            os.fsync(parent.descriptor)
            parent.verify()
        finally:
            if not published:
                # Cleanup only exact leaves in the exact retained staging inode.
                primary_error = sys.exc_info()[1]
                cleanup_error: BaseException | None = None
                try:
                    try:
                        try:
                            for leaf in known_leaves:
                                with contextlib.suppress(FileNotFoundError):
                                    os.unlink(leaf, dir_fd=stage_fd)
                            linked = os.stat(
                                stage_name,
                                dir_fd=parent.descriptor,
                                follow_symlinks=False,
                            )
                        except FileNotFoundError:
                            linked = None
                        opened = os.fstat(stage_fd)
                    finally:
                        os.close(stage_fd)
                    if linked is not None:
                        if _directory_authority_identity(linked) != _directory_authority_identity(opened):
                            raise BatchV2Error("result staging link changed during cleanup")
                        parent.verify()
                        os.rmdir(stage_name, dir_fd=parent.descriptor)
                        parent.verify()
                except BaseException as error:
                    cleanup_error = error
                if cleanup_error is not None:
                    if primary_error is None:
                        raise cleanup_error
                    with contextlib.suppress(Exception):
                        primary_error.add_note(
                            f"staging cleanup also failed: {type(cleanup_error).__name__}: {cleanup_error}"
                        )
            else:
                os.close(stage_fd)
    result_root.verify()
    return time.monotonic() - started


def replay_completed_result(
    order: dict[str, Any],
    *,
    profile: dict[str, Any],
    result_root: RetainedDirectory | None = None,
) -> dict[str, Any]:
    plan = V5.result_plan(order, profile_document=profile)
    maximum = profile["item_limits"]["maximum_result_bytes"]
    if result_root is None:
        result_body, _ = stable_file(
            plan["result_path"], "completed result", maximum=maximum
        )
        raw_body, _ = stable_file(
            plan["raw_transcript_path"], "raw transcript", maximum=maximum
        )
        normalized_body, _ = stable_file(
            plan["normalized_transcript_path"],
            "normalized transcript",
            maximum=maximum,
        )
    else:
        bodies = _retained_result_bodies(
            result_root, Path(plan["result_directory"]), maximum=maximum
        )
        result_body = bodies["result.json"]
        raw_body = bodies["transcript.raw.json"]
        normalized_body = bodies["transcript.normalized.json"]
    result_value = parse_json(result_body, "completed result")
    if result_body != canonical_bytes(result_value):
        raise BatchV2Error("completed result is not canonical JSON")
    try:
        result = V5.validate_result(result_value, work_order=order, profile_document=profile)
    except Exception as error:
        raise BatchV2Error(f"completed result envelope failed replay: {error}") from error
    artifacts = {row["artifact_kind"]: row for row in result["artifacts"]}
    if sha256_bytes(raw_body) != artifacts["faster_whisper_raw_transcript_json"]["sha256"] or sha256_bytes(normalized_body) != artifacts["transcript_normalized_json"]["sha256"]:
        raise BatchV2Error("completed transcript artifact SHA-256 differs from result")
    raw_value = parse_json(raw_body, "raw transcript")
    normalized_value = parse_json(normalized_body, "normalized transcript")
    if raw_body != canonical_bytes(raw_value) or normalized_body != canonical_bytes(normalized_value):
        raise BatchV2Error("completed transcript artifact is not canonical JSON")
    raw = validate_transcript_document(raw_value, prefix="gpuasrraw", label="raw transcript")
    normalized = validate_transcript_document(normalized_value, prefix="gpuasrnorm", label="normalized transcript")
    if normalize_raw_transcript(raw, order, profile) != normalized:
        raise BatchV2Error("normalized transcript does not exactly replay from raw")
    if artifacts["faster_whisper_raw_transcript_json"]["identity_sha256"] != raw["identity_sha256"] or artifacts["transcript_normalized_json"]["identity_sha256"] != normalized["identity_sha256"]:
        raise BatchV2Error("result artifact identities differ from transcript documents")
    return {"result": result, "result_sha256": sha256_bytes(result_body), "raw_sha256": sha256_bytes(raw_body), "normalized_sha256": sha256_bytes(normalized_body)}


class EventJournal:
    def __init__(
        self, root: RetainedDirectory | Path, batch_id: str, attempt_id: str
    ) -> None:
        self.attempt_id = attempt_id
        self._owns_root = not isinstance(root, RetainedDirectory)
        self.root = (
            RetainedDirectory.open(root, "event root")
            if self._owns_root
            else root
        )
        assert isinstance(self.root, RetainedDirectory)
        relative = Path(batch_id) / "attempts" / attempt_id
        parent_fd = self.root.descriptor
        descriptors = []
        links: list[tuple[int, str, tuple[int, ...]]] = []
        try:
            for component in relative.parts:
                descriptor = _open_or_create_private_at(
                    parent_fd, component, "event journal directory"
                )
                linked = os.stat(
                    component, dir_fd=parent_fd, follow_symlinks=False
                )
                links.append(
                    (
                        parent_fd,
                        component,
                        _directory_authority_identity(linked),
                    )
                )
                descriptors.append(descriptor)
                parent_fd = descriptor
            self.directory = RetainedDirectory.from_fd(
                self.root.path / relative,
                os.dup(descriptors[-1]),
                "event attempt directory",
                link_chain=links,
            )
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)
        self.path = self.directory.path
        self.ordinal = 0

    def emit(self, event: str, detail: dict[str, Any]) -> Path:
        if self.ordinal >= MAX_EVENTS_PER_ATTEMPT:
            raise BatchV2Error("event journal exceeded its finite bound")
        core = {
            "kind": EVENT_KIND,
            "schema_version": SCHEMA_VERSION,
            "implementation_version": IMPLEMENTATION_VERSION,
            "attempt_id": self.attempt_id,
            "event_ordinal": self.ordinal,
            "event": event,
            "created_at": utc_now(),
            "detail": detail,
        }
        identity = sha256_bytes(canonical_bytes(core))
        value = {**core, "identity_sha256": identity, "event_id": f"gpuasrevent2_{identity[:32]}"}
        path = self.path / f"{self.ordinal:06d}-{event}.json"
        body = canonical_bytes(value)
        if len(body) > MAX_EVENT_BYTES:
            raise BatchV2Error("event document exceeds its byte cap")
        self.directory.verify()
        self.root.verify()
        _write_new_at(self.directory.descriptor, path.name, body)
        self.directory.verify()
        self.root.verify()
        self.ordinal += 1
        return path

    def close(self) -> None:
        self.directory.close()
        if self._owns_root:
            self.root.close()


def _load_controls(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], bytes]:
    profile_value, _ = load_canonical_document(args.production_profile, args.expected_production_profile_sha256, "production profile", maximum=1024 * 1024)
    try:
        profile = PROFILE.validate_profile(profile_value)
    except Exception as error:
        raise BatchV2Error(f"production profile is invalid: {error}") from error
    manifest, manifest_body = load_manifest(args.batch_manifest, args.expected_batch_sha256, profile=profile)
    runtime, _ = load_canonical_document(args.runtime_admission, args.expected_runtime_admission_sha256, "runtime admission")
    root, _ = load_canonical_document(args.root_registration, args.expected_root_registration_sha256, "root registration")
    lineage_preflight, _ = load_canonical_document(
        args.lineage_preflight_attestation,
        args.expected_lineage_preflight_attestation_sha256,
        "lineage preflight attestation",
        maximum=1024 * 1024,
    )
    validate_lineage_preflight(
        lineage_preflight,
        manifest=manifest,
        manifest_physical_sha256=sha256_bytes(manifest_body),
        profile=profile,
        root_registration=root,
    )
    attestation, _ = load_canonical_document(args.launch_attestation, args.expected_launch_attestation_sha256, "launch attestation")
    validate_launch_attestation(
        attestation,
        manifest=manifest,
        manifest_sha256=sha256_bytes(manifest_body),
        runtime=runtime,
        runtime_sha256=args.expected_runtime_admission_sha256,
        profile=profile,
        profile_sha256=args.expected_production_profile_sha256,
        root=root,
        root_sha256=args.expected_root_registration_sha256,
        lineage_preflight=lineage_preflight,
        lineage_preflight_sha256=args.expected_lineage_preflight_attestation_sha256,
    )
    return manifest, profile, runtime, attestation, lineage_preflight, manifest_body


def _load_batch_orders(manifest: dict[str, Any], profile: dict[str, Any]) -> list[dict[str, Any]]:
    orders = []
    for ordinal, member in enumerate(manifest["items"], 1):
        try:
            order = V5.validate_work_order(
                member["work_order"],
                profile_document=profile,
                replay_bindings=False,
            )
        except Exception as error:
            raise BatchV2Error(f"embedded work order {ordinal} failed semantic replay: {error}") from error
        if order != member["work_order"] or _member(ordinal, order, profile) != member:
            raise BatchV2Error(f"work order {ordinal} differs from its batch projection")
        if order["production_profile"] != manifest["production_profile"] or order["runtime_admission"] != manifest["runtime_admission"] or order["hot_root"] != manifest["hot_root"]:
            raise BatchV2Error(f"work order {ordinal} differs from common controls")
        orders.append(order)
    return orders


def _package_closure(runtime: dict[str, Any]) -> dict[str, str]:
    expected = runtime.get("runtime", {}).get("packages")
    if not isinstance(expected, dict):
        raise BatchV2Error("runtime receipt lacks the package closure")
    observed = {}
    for name, version in expected.items():
        try:
            current = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError as error:
            raise BatchV2Error(f"runtime package is absent: {name}") from error
        if current != version:
            raise BatchV2Error(f"runtime package {name} differs: {current} != {version}")
        observed[name] = current
    return observed


def _nvml_gpu(pynvml: Any, expected_uuid: str) -> Any:
    pynvml.nvmlInit()
    try:
        try:
            handle = pynvml.nvmlDeviceGetHandleByUUID(expected_uuid)
        except TypeError:
            handle = pynvml.nvmlDeviceGetHandleByUUID(expected_uuid.encode("ascii"))
        observed = pynvml.nvmlDeviceGetUUID(handle)
        if isinstance(observed, bytes):
            observed = observed.decode("ascii")
        if observed != expected_uuid:
            raise BatchV2Error("live GPU UUID differs from the production profile")
        return handle
    except Exception:
        pynvml.nvmlShutdown()
        raise


def gpu_process_baseline(pynvml: Any, handle: Any) -> dict[str, Any]:
    """Reject other CUDA jobs while allowing desktop graphics clients.

    Recent NVIDIA stacks may report compositor/application graphics contexts in
    both process lists.  A foreign PID is therefore a conflicting CUDA job only
    when it appears in the compute list and not in the graphics list.
    """

    try:
        compute = list(pynvml.nvmlDeviceGetComputeRunningProcesses(handle))
        graphics = list(pynvml.nvmlDeviceGetGraphicsRunningProcesses(handle))
    except Exception as error:
        raise BatchV2Error(f"NVML process admission query failed: {error}") from error
    graphics_pids = {
        int(row.pid)
        for row in graphics
        if isinstance(getattr(row, "pid", None), int)
        and not isinstance(getattr(row, "pid", None), bool)
    }
    foreign_cuda = sorted(
        {
            int(row.pid)
            for row in compute
            if isinstance(getattr(row, "pid", None), int)
            and not isinstance(getattr(row, "pid", None), bool)
            and row.pid != os.getpid()
            and row.pid not in graphics_pids
        }
    )
    if foreign_cuda:
        raise GPUResourceBusy(f"other CUDA compute processes are active: {foreign_cuda}")
    graphics_bytes = 0
    for row in graphics:
        used = getattr(row, "usedGpuMemory", None)
        if isinstance(used, int) and not isinstance(used, bool) and 0 <= used < 2**63:
            graphics_bytes += used
    return {
        "compute_process_count": len(compute),
        "graphics_process_count": len(graphics),
        "graphics_baseline_bytes": graphics_bytes,
        "foreign_cuda_pids": [],
        "policy": "compute_minus_graphics_excluding_self_must_be_empty",
    }


def gpu_memory_admission(
    pynvml: Any, handle: Any, *, minimum_free_bytes: int
) -> dict[str, int]:
    try:
        memory = pynvml.nvmlDeviceGetMemoryInfo(handle)
        total = int(memory.total)
        used = int(memory.used)
        free = int(memory.free)
    except Exception as error:
        raise BatchV2Error(f"NVML memory admission query failed: {error}") from error
    if (
        min(total, used, free) < 0
        or total < 1
        or used > total
        or free > total
        or abs((used + free) - total) > 64 * 1024**2
    ):
        raise BatchV2Error("NVML memory admission observation is inconsistent")
    if free < minimum_free_bytes:
        raise GPUResourceBusy(
            f"GPU free VRAM {free} is below the admitted reserve {minimum_free_bytes}"
        )
    return {"total_bytes": total, "used_bytes": used, "free_bytes": free}


def _model_class() -> Any:
    try:
        from faster_whisper import WhisperModel
    except ImportError as error:
        raise BatchV2Error(f"cannot import faster-whisper: {error}") from error
    return WhisperModel


def _pairs(values: Sequence[RetainedInput]) -> Iterator[list[RetainedInput]]:
    # Preserve the manifest's fixed (1,2), (3,4), ... transaction groups even
    # after an interrupted run has already published one exact member.
    current: list[RetainedInput] = []
    pair_number: int | None = None
    for value in values:
        observed = (value.ordinal - 1) // 2
        if pair_number is not None and observed != pair_number:
            yield current
            current = []
        pair_number = observed
        current.append(value)
    if current:
        yield current


def execute_batch(
    args: argparse.Namespace,
    *,
    model_class: Any | None = None,
    pynvml: Any | None = None,
    av_module: Any | None = None,
    fatal: Callable[[int], None] = os._exit,
) -> dict[str, Any]:
    process_start = time.monotonic()
    manifest, profile, runtime, attestation, lineage_preflight, manifest_body = _load_controls(args)
    del lineage_preflight
    network = network_isolation(attestation)
    orders = _load_batch_orders(manifest, profile)
    expected_status = _runtime_status_for_class(manifest["execution_class"])
    if runtime.get("status") != expected_status or runtime.get("identity_sha256") != manifest["runtime_admission"]["identity_sha256"]:
        raise BatchV2Error("runtime control status or identity differs from the batch")
    if os.environ.get("CUDA_VISIBLE_DEVICES") != profile["hardware"]["gpu_uuid"]:
        raise BatchV2Error("CUDA_VISIBLE_DEVICES differs from the admitted stable GPU UUID")
    if os.environ.get("HF_HUB_OFFLINE") != "1" or os.environ.get("HF_DATASETS_OFFLINE") != "1":
        raise BatchV2Error("offline model environment is incomplete")
    if model_class is None:
        _package_closure(runtime)
    batch_deadline = HardDeadline(
        profile["batch_limits"]["maximum_wall_seconds"], fatal=fatal
    )
    batch_deadline.__enter__()
    root_stack = contextlib.ExitStack()
    try:
        result_root = root_stack.enter_context(
            RetainedDirectory.open(manifest["writable_roots"]["result"], "result root")
        )
        event_root = root_stack.enter_context(
            RetainedDirectory.open(manifest["writable_roots"]["event"], "event root")
        )
        lock_root = root_stack.enter_context(
            RetainedDirectory.open(manifest["writable_roots"]["lock"], "lock root")
        )
    except Exception:
        try:
            root_stack.close()
        finally:
            batch_deadline.__exit__(None, None, None)
        raise
    completed: dict[int, dict[str, Any]] = {}
    pending_orders: list[tuple[int, dict[str, Any]]] = []
    try:
        for ordinal, order in enumerate(orders, 1):
            plan = V5.result_plan(order, profile_document=profile)
            if _result_directory_present(result_root, Path(plan["result_directory"])):
                completed[ordinal] = replay_completed_result(
                    order, profile=profile, result_root=result_root
                )
            else:
                pending_orders.append((ordinal, order))
    except Exception:
        try:
            root_stack.close()
        finally:
            batch_deadline.__exit__(None, None, None)
        raise
    retained: list[RetainedInput] = []
    journal: EventJournal | None = None
    try:
        # Deliberately before the UUID reservation: audio I/O and PyAV probing never
        # consume scarce GPU residency time.
        for ordinal, order in pending_orders:
            retained.append(
                retain_and_preflight_input(
                    ordinal,
                    order,
                    av_module=av_module,
                    expected_av_version=runtime["runtime"]["packages"]["av"],
                )
            )
        attempt_id = f"gpuasrattempt_{secrets.token_hex(16)}"
        journal = EventJournal(event_root, manifest["batch_id"], attempt_id)
        journal.emit("attempt-start", {"batch_id": manifest["batch_id"], "resumed_ordinals": sorted(completed), "pending_ordinals": [row.ordinal for row in retained], "network_isolation": network})
        journal.emit(
            "input-preflight-completed",
            {
                "items": [
                    {
                        "ordinal": row.ordinal,
                        "sha256": row.work_order["input"]["expected_sha256"],
                        "byte_count": row.info.st_size,
                        "hash_and_memfd_copy_seconds": row.hash_seconds,
                        "probe_seconds": row.probe_seconds,
                        "probe": row.probe,
                        "memfd_seals": row.memfd_seals,
                    }
                    for row in retained
                ],
                "source_descriptors_closed": True,
                "inference_reads_only_sealed_memfds": True,
            },
        )
        if not retained:
            completion = _completion(manifest, profile, attempt_id, completed, process_start, model_load_seconds=0.0, event_paths=[str(journal.emit("batch-completed", {"all_items_reused": True}))])
            return completion
        if pynvml is None:
            try:
                import pynvml as pynvml_module
            except ImportError as error:
                raise BatchV2Error(f"cannot import pynvml: {error}") from error
            pynvml = pynvml_module
        gpu_uuid = profile["hardware"]["gpu_uuid"]
        handle = _nvml_gpu(pynvml, gpu_uuid)
        limits = profile["telemetry"]
        sampler: Any | None = None
        lock_name = f"gpu-{gpu_uuid}.lock"
        event_paths: list[str] = []
        try:
            with nonblocking_lock_at(lock_root, lock_name, f"GPU UUID {gpu_uuid}"):
                process_baseline = gpu_process_baseline(pynvml, handle)
                memory_baseline = gpu_memory_admission(
                    pynvml,
                    handle,
                    minimum_free_bytes=limits["minimum_free_vram_bytes"],
                )
                # Construct no sampler thread and no model object until UUID,
                # foreign-process, and free-VRAM admission all pass.
                sampler = TELEMETRY.NVMLTelemetrySampler(
                    pynvml,
                    handle,
                    TELEMETRY.TelemetryLimits(
                        maximum_process_vram_bytes=limits["maximum_process_vram_bytes"],
                        maximum_temperature_c=limits["maximum_temperature_c"],
                        minimum_free_vram_bytes=limits["minimum_free_vram_bytes"],
                    ),
                    fatal=fatal,
                )
                event_paths.append(
                    str(
                        journal.emit(
                            "gpu-admitted",
                            {
                                "processes": process_baseline,
                                "memory": memory_baseline,
                            },
                        )
                    )
                )
                sampler.start()
                model_started = time.monotonic()
                cls = _model_class() if model_class is None else model_class
                decoding = profile["decoding"]
                model = cls(
                    "/opt/himr-gpu/model/snapshot",
                    device="cuda",
                    device_index=profile["hardware"]["device_index"],
                    compute_type=profile["hardware"]["compute_type"],
                    cpu_threads=decoding["cpu_threads"],
                    num_workers=decoding["num_workers"],
                    local_files_only=True,
                )
                model_load_seconds = time.monotonic() - model_started
                # Wait for the process VRAM lane to become visible and fail closed
                # before any item can be published.
                deadline = time.monotonic() + limits["maximum_heartbeat_age_ms"] / 1000
                while True:
                    try:
                        sampler.snapshot(maximum_age_seconds=limits["maximum_heartbeat_age_ms"] / 1000)
                        break
                    except Exception:
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(0.01)
                with concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="himr-gpu-asr-pair") as pool:
                    for pair in _pairs(retained):
                        pair_started = time.monotonic()
                        with HardDeadline(profile["item_limits"]["maximum_wall_seconds"], fatal=fatal):
                            futures = [pool.submit(compute_item, model, row, profile=profile, runtime=runtime) for row in pair]
                            computed_rows: list[ComputedItem] = []
                            errors: list[str] = []
                            for future in futures:
                                try:
                                    computed_rows.append(future.result())
                                except Exception as error:
                                    errors.append(f"{type(error).__name__}: {error}")
                            if errors:
                                raise PairFailure("; ".join(errors))
                        computed_rows.sort(key=lambda row: row.retained.ordinal)
                        telemetry_snapshot = sampler.snapshot(maximum_age_seconds=limits["maximum_heartbeat_age_ms"] / 1000)
                        sealed = []
                        for row in computed_rows:
                            result, body = make_item_result(
                                row,
                                profile=profile,
                                telemetry=telemetry_snapshot,
                                runtime=runtime,
                                attempt_id=attempt_id,
                                model_load_seconds=model_load_seconds,
                            )
                            sealed.append((row, result, body))
                        event_paths.append(
                            str(
                                journal.emit(
                                    "pair-ready",
                                    {
                                        "ordinals": [row.retained.ordinal for row, _, _ in sealed],
                                        "items": [
                                            {
                                                "ordinal": row.retained.ordinal,
                                                "result_id": result["result_id"],
                                                "result_sha256": sha256_bytes(body),
                                            }
                                            for row, result, body in sealed
                                        ],
                                        "publication_state": "none_published",
                                    },
                                )
                            )
                        )
                        publications = []
                        for row, result, body in sealed:
                            publication_seconds = publish_result_bundle(
                                row,
                                result,
                                body,
                                profile=profile,
                                attempt_id=attempt_id,
                                result_root=result_root,
                            )
                            replay = replay_completed_result(
                                row.retained.work_order,
                                profile=profile,
                                result_root=result_root,
                            )
                            completed[row.retained.ordinal] = replay
                            publications.append({
                                "ordinal": row.retained.ordinal,
                                "result_id": result["result_id"],
                                "result_sha256": replay["result_sha256"],
                                "setup_seconds": row.setup_seconds,
                                "iterator_seconds": row.iterator_seconds,
                                "normalization_seconds": row.normalization_seconds,
                                "serialization_seconds": row.serialization_seconds,
                                "publication_seconds": publication_seconds,
                            })
                        path = journal.emit("pair-completed", {"ordinals": [row["ordinal"] for row in publications], "pair_wall_seconds": time.monotonic() - pair_started, "items": publications})
                        event_paths.append(str(path))
                        for row in retained:
                            row.verify()
                        # Cheap end check: exact already-retained launch bytes and
                        # content-addressed control identities, never image trees.
                        current_attestation, current_body = load_canonical_document(args.launch_attestation, args.expected_launch_attestation_sha256, "launch attestation end replay")
                        if current_attestation != attestation or sha256_bytes(current_body) != args.expected_launch_attestation_sha256:
                            raise BatchV2Error("launch attestation changed during execution")
                final_telemetry = sampler.snapshot(maximum_age_seconds=limits["maximum_heartbeat_age_ms"] / 1000)
                path = journal.emit("batch-completed", {"completed_ordinals": sorted(completed), "telemetry": final_telemetry})
                event_paths.append(str(path))
            completion = _completion(manifest, profile, attempt_id, completed, process_start, model_load_seconds=model_load_seconds, event_paths=event_paths)
            return completion
        except Exception as error:
            with contextlib.suppress(Exception):
                journal.emit("batch-failed", {"error_type": type(error).__name__, "message": str(error)[:2048], "completed_ordinals": sorted(completed)})
            raise
        finally:
            with contextlib.suppress(Exception):
                if sampler is not None:
                    sampler.stop()
            with contextlib.suppress(Exception):
                pynvml.nvmlShutdown()
    finally:
        for row in retained:
            with contextlib.suppress(OSError):
                row.close()
        if journal is not None:
            with contextlib.suppress(OSError):
                journal.close()
        batch_deadline.__exit__(None, None, None)
        root_stack.close()


def _completion(manifest: dict[str, Any], profile: dict[str, Any], attempt_id: str, completed: dict[int, dict[str, Any]], process_start: float, *, model_load_seconds: float, event_paths: Sequence[str]) -> dict[str, Any]:
    if sorted(completed) != list(range(1, len(manifest["items"]) + 1)):
        raise BatchV2Error("completion cannot be created before every ordinal replays")
    core = {
        "kind": COMPLETION_KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": "completed",
        "batch": {"batch_id": manifest["batch_id"], "identity_sha256": manifest["identity_sha256"]},
        "attempt_id": attempt_id,
        "completed_at": utc_now(),
        "wall_seconds": time.monotonic() - process_start,
        "model_load_seconds": model_load_seconds,
        "model_load_count": 0 if model_load_seconds == 0 else 1,
        "inference_concurrency": profile["batch_limits"]["inference_concurrency"],
        "items": [
            {"ordinal": ordinal, "result_sha256": completed[ordinal]["result_sha256"], "result_id": completed[ordinal]["result"]["result_id"]}
            for ordinal in sorted(completed)
        ],
        "event_paths": list(event_paths),
        "policy": dict(POLICY),
    }
    identity = sha256_bytes(canonical_bytes(core))
    return {**core, "identity_sha256": identity, "completion_id": f"gpuasrcompletion2_{identity[:32]}"}


def batch_status(manifest: dict[str, Any], profile: dict[str, Any]) -> dict[str, Any]:
    completed = []
    absent = []
    invalid = []
    for row in manifest["items"]:
        ordinal = row["ordinal"]
        result_path = Path(row["result"]["result_path"])
        if not result_path.exists() and not result_path.is_symlink():
            absent.append(ordinal)
            continue
        try:
            order = V5.validate_work_order(row["work_order"], profile_document=profile)
            replay_completed_result(order, profile=profile)
            completed.append(ordinal)
        except Exception as error:
            invalid.append({"ordinal": ordinal, "error_type": type(error).__name__, "message": str(error)})
    return {
        "status": "completed" if len(completed) == len(manifest["items"]) else "invalid" if invalid else "pending",
        "batch_id": manifest["batch_id"],
        "completed_ordinals": completed,
        "absent_ordinals": absent,
        "invalid": invalid,
        "inference_performed": False,
        "files_written": False,
    }


def contract_document() -> dict[str, Any]:
    descriptor = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "member_contract": "production_asr_v5_contract_only",
        "maximum_items": MAX_ITEMS,
        "execution_classes": sorted(EXECUTION_CLASSES),
        "execution": {
            "lineage_preflight": "gpu_less_typed_queue_or_fixture_replay_attested_before_run",
            "input_preflight": "all_nofollow_copy_hash_sealed_memfd_authenticated_pyav_probe_before_gpu_lock",
            "model_loads": 1,
            "concurrency": 2,
            "dispatch": "ordinary_transcribe_calls_in_ordinal_pairs",
            "pair_failure": "discard_unpublished_pair_fail_stop",
            "publication": "pair_ready_journal_then_strict_ordinal_atomic_directory_rename_noreplace",
            "resume": "exact_v5_result_and_artifact_replay",
            "writable_paths": "retained_dirfd_component_link_replay_no_recursive_cleanup",
            "telemetry": "20hz_vram_guard_4hz_bounded_metrics",
            "watchdogs": "process_wide_batch_and_per_pair",
        },
        "policy": dict(POLICY),
    }
    return {"kind": CONTRACT_KIND, "schema_version": SCHEMA_VERSION, "descriptor": descriptor, "identity_sha256": sha256_bytes(canonical_bytes(descriptor))}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("contracts")
    materialize = sub.add_parser("materialize")
    materialize.add_argument("--work-order", action="append", required=True)
    materialize.add_argument("--production-profile", required=True)
    materialize.add_argument("--expected-production-profile-sha256", required=True)
    materialize.add_argument("--batch-root", required=True)
    materialize.add_argument("--event-root", required=True)
    materialize.add_argument("--lock-root", required=True)
    lineage = sub.add_parser("preflight-lineage")
    lineage.add_argument("--batch-manifest", required=True)
    lineage.add_argument("--expected-batch-sha256", required=True)
    lineage.add_argument("--production-profile", required=True)
    lineage.add_argument("--expected-production-profile-sha256", required=True)
    lineage.add_argument("--root-registration", required=True)
    lineage.add_argument("--expected-root-registration-sha256", required=True)
    lineage.add_argument("--lineage-attestation-output", required=True)
    for name in ("validate", "status"):
        command = sub.add_parser(name)
        command.add_argument("--batch-manifest", required=True)
        command.add_argument("--expected-batch-sha256", required=True)
        command.add_argument("--production-profile", required=True)
        command.add_argument("--expected-production-profile-sha256", required=True)
    run = sub.add_parser("run")
    run.add_argument("--batch-manifest", required=True)
    run.add_argument("--expected-batch-sha256", required=True)
    run.add_argument("--runtime-admission", required=True)
    run.add_argument("--expected-runtime-admission-sha256", required=True)
    run.add_argument("--production-profile", required=True)
    run.add_argument("--expected-production-profile-sha256", required=True)
    run.add_argument("--root-registration", required=True)
    run.add_argument("--expected-root-registration-sha256", required=True)
    run.add_argument("--lineage-preflight-attestation", required=True)
    run.add_argument("--expected-lineage-preflight-attestation-sha256", required=True)
    run.add_argument("--launch-attestation", required=True)
    run.add_argument("--expected-launch-attestation-sha256", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "contracts":
        print(json.dumps(contract_document(), sort_keys=True, indent=2))
        return 0
    if args.command == "materialize":
        manifest, path = materialize_batch(
            work_order_paths=[Path(value) for value in args.work_order],
            profile_path=Path(args.production_profile),
            expected_profile_sha256=args.expected_production_profile_sha256,
            batch_root=Path(args.batch_root),
            event_root=Path(args.event_root),
            lock_root=Path(args.lock_root),
        )
        print(json.dumps({"status": "materialized", "batch_id": manifest["batch_id"], "manifest_path": str(path), "manifest_sha256": sha256_bytes(canonical_bytes(manifest))}, sort_keys=True, indent=2))
        return 0
    if args.command == "preflight-lineage":
        require_loopback_only_network()
        profile = V5.load_profile_document(
            args.production_profile,
            args.expected_production_profile_sha256,
        )
        manifest, manifest_body = load_manifest(
            args.batch_manifest,
            args.expected_batch_sha256,
            profile=profile,
        )
        root, _ = load_canonical_document(
            args.root_registration,
            args.expected_root_registration_sha256,
            "root registration",
            maximum=1024 * 1024,
        )
        value = make_lineage_preflight(
            manifest=manifest,
            manifest_physical_sha256=sha256_bytes(manifest_body),
            profile=profile,
            root_registration=root,
            root_registration_path=Path(args.root_registration),
            root_registration_sha256=args.expected_root_registration_sha256,
        )
        output = _absolute(
            args.lineage_attestation_output, "lineage attestation output"
        )
        body = canonical_bytes(value)
        if len(body) > 1024 * 1024:
            raise BatchV2Error("lineage preflight attestation exceeds 1 MiB")
        with RetainedDirectory.open(
            output.parent, "lineage attestation output root"
        ) as output_root:
            _write_new_at(output_root.descriptor, output.name, body)
            output_root.verify()
        digest = sha256_bytes(body)
        print(
            json.dumps(
                {
                    "status": "passed",
                    "attestation_id": value["attestation_id"],
                    "identity_sha256": value["identity_sha256"],
                    "physical_sha256": digest,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0
    if args.command in {"validate", "status"}:
        profile = V5.load_profile_document(args.production_profile, args.expected_production_profile_sha256)
        manifest, _ = load_manifest(args.batch_manifest, args.expected_batch_sha256, profile=profile)
        value = {"status": "valid", "batch_id": manifest["batch_id"], "files_written": False}
        if args.command == "status":
            value = batch_status(manifest, profile)
        print(json.dumps(value, sort_keys=True, indent=2))
        return 0
    completion = execute_batch(args)
    print(json.dumps(completion, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (BatchV2Error, OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
