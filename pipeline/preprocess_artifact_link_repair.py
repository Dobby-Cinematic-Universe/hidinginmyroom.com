#!/usr/bin/env python3
"""Detach one sealed preprocess audio artifact from an immutable hardlink set.

This is an administrative compatibility repair, not a preprocessing lane.  A
plan is bound to one completed preprocess result and its exact normalized-audio
artifact.  Apply copies those exact bytes to a new inode in the same directory,
seals it to the original mode, and atomically exchanges only that artifact path.
The completed result and all existing receipts remain byte-for-byte unchanged.
"""

from __future__ import annotations

import argparse
import ctypes
import errno
import fcntl
import hashlib
import json
import os
import re
import stat
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


sys.dont_write_bytecode = True

TOOL_NAME = "himr-preprocess-artifact-link-repair"
IMPLEMENTATION_VERSION = "0.1.0"
PLAN_KIND = "preprocess_artifact_single_link_repair_plan"
RECEIPT_KIND = "preprocess_artifact_single_link_repair_receipt"
SCHEMA_VERSION = 1

ARTIFACT_KIND = "audio_16khz_mono_flac"
ARTIFACT_BASENAME = "audio-16khz-mono.flac"
ARTIFACT_MODES = frozenset({0o400, 0o444})
RESULT_MODE = 0o444
CONTROL_MODE = 0o400
CONTROL_DIRECTORY_MODE = 0o700
CONTROL_DIRECTORY_BASENAME = "repair-control"
LOCK_MODE = 0o600
LOCK_BASENAME = ".preprocess.lock"
CONTROLLER_LOCK_BASENAME = "controller.lock"
MAX_RESULT_BYTES = 64 * 1024 * 1024
MAX_CONTROL_BYTES = 2 * 1024 * 1024
COPY_CHUNK_BYTES = 4 * 1024 * 1024
RENAME_EXCHANGE = 2
RENAME_NOREPLACE = 1
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

AUTHORITY = {
    "artifact_path_replacement": "one_plan_bound_normalized_audio_path",
    "catalog_writes": False,
    "existing_receipt_writes": False,
    "network_access": False,
    "preprocess_execution": False,
    "result_envelope_writes": False,
}
POLICY = {
    "apply_lock": (
        "explicit_controller_lock_then_exact_recipe_preprocess_lock_held_"
        "exclusively_through_commit"
    ),
    "artifact_kind": ARTIFACT_KIND,
    "copy_policy": "exact_bytes_to_distinct_inode_in_same_directory",
    "hardlink_precondition": "source_link_count_greater_than_one",
    "mode_policy": "preserve_exact_0400_or_0444",
    "publication": (
        "fsynced_sibling_then_renameat2_exchange_displaced_identity_check_"
        "and_parent_fsync"
    ),
    "recovery": "deterministic_swap_or_exact_single_link_receipt_reconciliation",
    "result_policy": "completed_immutable_result_retained_and_unchanged",
    "scope": "one_artifact_per_plan",
    "symlink_policy": "resolved_root_and_retained_O_NOFOLLOW_directory_chain",
}


class LinkRepairError(RuntimeError):
    """A plan, retention, copy, or commit invariant failed closed."""


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def _sealed_json_bytes(value: object) -> bytes:
    return canonical_bytes(value) + b"\n"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _mode(value: os.stat_result) -> int:
    return stat.S_IMODE(value.st_mode)


def _same_object(left: os.stat_result, right: os.stat_result) -> bool:
    return left.st_dev == right.st_dev and left.st_ino == right.st_ino


def _file_fingerprint(value: os.stat_result) -> dict[str, int]:
    return {
        "byte_count": value.st_size,
        "ctime_ns": value.st_ctime_ns,
        "device": value.st_dev,
        "gid": value.st_gid,
        "inode": value.st_ino,
        "link_count": value.st_nlink,
        "mode": _mode(value),
        "mtime_ns": value.st_mtime_ns,
        "uid": value.st_uid,
    }


def _directory_fingerprint(value: os.stat_result, path: Path) -> dict[str, Any]:
    return {
        "device": value.st_dev,
        "gid": value.st_gid,
        "inode": value.st_ino,
        "mode": _mode(value),
        "path": str(path),
        "uid": value.st_uid,
    }


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number {value} is forbidden")


def _reject_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _parse_json(body: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=_reject_pairs,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise LinkRepairError(f"{label} is not strict UTF-8 JSON: {error}") from error
    if not isinstance(value, dict):
        raise LinkRepairError(f"{label} must contain one JSON object")
    return value


def _require_exact_keys(value: dict[str, Any], expected: set[str], label: str) -> None:
    missing = sorted(expected - set(value))
    unknown = sorted(set(value) - expected)
    if missing or unknown:
        raise LinkRepairError(
            f"{label} keys differ from the exact contract; "
            f"missing={missing}, unknown={unknown}"
        )


def _require_absolute_resolved(path: Path, label: str, *, directory: bool) -> Path:
    if not path.is_absolute():
        raise LinkRepairError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise LinkRepairError(f"{label} is unavailable: {error}") from error
    if resolved != path:
        raise LinkRepairError(
            f"{label} must be resolved and contain no symlink or traversal component"
        )
    observed = path.lstat()
    if directory and not stat.S_ISDIR(observed.st_mode):
        raise LinkRepairError(f"{label} must be a directory")
    if not directory and not stat.S_ISREG(observed.st_mode):
        raise LinkRepairError(f"{label} must be a regular file")
    return path


def _require_beneath(path: Path, root: Path, label: str) -> tuple[str, ...]:
    try:
        relative = path.relative_to(root)
    except ValueError as error:
        raise LinkRepairError(f"{label} escapes the preprocess output root") from error
    if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
        raise LinkRepairError(f"{label} has an invalid relative path")
    return relative.parts


def _validate_owned_directory(value: os.stat_result, label: str) -> None:
    if not stat.S_ISDIR(value.st_mode):
        raise LinkRepairError(f"{label} is not a directory")
    if value.st_uid != os.geteuid():
        raise LinkRepairError(f"{label} is not owned by the invoking uid")
    if _mode(value) & 0o022:
        raise LinkRepairError(f"{label} is group- or other-writable")


@dataclass
class RetainedDirectoryChain:
    root: Path
    directory: Path
    descriptors: list[int]
    names: list[str]
    opened: list[os.stat_result]

    @classmethod
    def open(cls, root: Path, directory: Path) -> "RetainedDirectoryChain":
        root = _require_absolute_resolved(root, "preprocess output root", directory=True)
        directory = _require_absolute_resolved(
            directory, "retained directory", directory=True
        )
        parts = _require_beneath(directory, root, "retained directory") if directory != root else ()
        flags = (
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptors: list[int] = []
        names: list[str] = []
        opened: list[os.stat_result] = []
        try:
            root_before = root.lstat()
            root_fd = os.open(root, flags)
            root_after = os.fstat(root_fd)
            if not _same_object(root_before, root_after):
                raise LinkRepairError("preprocess output root changed while opening")
            _validate_owned_directory(root_after, "preprocess output root")
            descriptors.append(root_fd)
            opened.append(root_after)
            for component in parts:
                parent_fd = descriptors[-1]
                before = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
                child_fd = os.open(component, flags, dir_fd=parent_fd)
                after = os.fstat(child_fd)
                if not _same_object(before, after):
                    os.close(child_fd)
                    raise LinkRepairError(
                        f"retained directory component changed while opening: {component}"
                    )
                descriptors.append(child_fd)
                names.append(component)
                opened.append(after)
                _validate_owned_directory(after, f"retained directory {component}")
            chain = cls(root, directory, descriptors, names, opened)
            chain.verify()
            return chain
        except Exception:
            for descriptor in reversed(descriptors):
                os.close(descriptor)
            raise

    @property
    def fd(self) -> int:
        return self.descriptors[-1]

    def verify(self) -> None:
        root_path = self.root.lstat()
        if not _same_object(root_path, os.fstat(self.descriptors[0])):
            raise LinkRepairError("preprocess output root path identity changed")
        for index, expected in enumerate(self.opened):
            observed = os.fstat(self.descriptors[index])
            if _directory_fingerprint(observed, self.root) | {"path": ""} != (
                _directory_fingerprint(expected, self.root) | {"path": ""}
            ):
                raise LinkRepairError("retained directory metadata changed")
            _validate_owned_directory(observed, "retained directory")
            if index:
                current = os.stat(
                    self.names[index - 1],
                    dir_fd=self.descriptors[index - 1],
                    follow_symlinks=False,
                )
                if not _same_object(current, observed):
                    raise LinkRepairError("retained directory path identity changed")

    def close(self) -> None:
        for descriptor in reversed(self.descriptors):
            os.close(descriptor)
        self.descriptors.clear()

    def __enter__(self) -> "RetainedDirectoryChain":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


@dataclass
class RetainedFile:
    path: Path
    chain: RetainedDirectoryChain
    descriptor: int
    opened: os.stat_result

    @classmethod
    def open(
        cls,
        root: Path,
        path: Path,
        *,
        modes: set[int] | frozenset[int],
        link_count: int | None = None,
        minimum_links: int | None = None,
        writable: bool = False,
    ) -> "RetainedFile":
        path = _require_absolute_resolved(path, "retained file", directory=False)
        _require_beneath(path, root, "retained file")
        chain = RetainedDirectoryChain.open(root, path.parent)
        flags = (
            (os.O_RDWR if writable else os.O_RDONLY)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        descriptor: int | None = None
        try:
            before = os.stat(path.name, dir_fd=chain.fd, follow_symlinks=False)
            descriptor = os.open(path.name, flags, dir_fd=chain.fd)
            opened = os.fstat(descriptor)
            if not _same_object(before, opened):
                raise LinkRepairError(f"retained file changed while opening: {path}")
            cls._validate(
                opened,
                path,
                modes=modes,
                link_count=link_count,
                minimum_links=minimum_links,
            )
            retained = cls(path, chain, descriptor, opened)
            retained.verify_path(
                modes=modes,
                link_count=link_count,
                minimum_links=minimum_links,
            )
            return retained
        except Exception:
            if descriptor is not None:
                os.close(descriptor)
            chain.close()
            raise

    @staticmethod
    def _validate(
        value: os.stat_result,
        path: Path,
        *,
        modes: set[int] | frozenset[int],
        link_count: int | None,
        minimum_links: int | None,
    ) -> None:
        if not stat.S_ISREG(value.st_mode):
            raise LinkRepairError(f"retained path is not a regular file: {path}")
        if value.st_uid != os.geteuid():
            raise LinkRepairError(f"retained file is not owned by the invoking uid: {path}")
        if _mode(value) not in modes:
            raise LinkRepairError(
                f"retained file mode {_mode(value):04o} is not allowed: {path}"
            )
        if link_count is not None and value.st_nlink != link_count:
            raise LinkRepairError(
                f"retained file link count changed from {link_count}: {path}"
            )
        if minimum_links is not None and value.st_nlink < minimum_links:
            raise LinkRepairError(
                f"retained file requires at least {minimum_links} links: {path}"
            )

    def verify_descriptor(self, expected: dict[str, int] | None = None) -> os.stat_result:
        observed = os.fstat(self.descriptor)
        if expected is not None and _file_fingerprint(observed) != expected:
            raise LinkRepairError(f"retained file metadata changed: {self.path}")
        return observed

    def verify_path(
        self,
        *,
        modes: set[int] | frozenset[int],
        link_count: int | None = None,
        minimum_links: int | None = None,
        expected: dict[str, int] | None = None,
    ) -> os.stat_result:
        self.chain.verify()
        descriptor = self.verify_descriptor(expected)
        current = os.stat(
            self.path.name, dir_fd=self.chain.fd, follow_symlinks=False
        )
        if not _same_object(descriptor, current):
            raise LinkRepairError(f"retained file path identity changed: {self.path}")
        self._validate(
            current,
            self.path,
            modes=modes,
            link_count=link_count,
            minimum_links=minimum_links,
        )
        if expected is not None and _file_fingerprint(current) != expected:
            raise LinkRepairError(f"retained file path metadata changed: {self.path}")
        return current

    def close(self) -> None:
        os.close(self.descriptor)
        self.chain.close()

    def __enter__(self) -> "RetainedFile":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _pread_exact(descriptor: int, byte_count: int, label: str) -> bytes:
    if byte_count < 0:
        raise LinkRepairError(f"{label} has a negative byte count")
    parts: list[bytes] = []
    offset = 0
    while offset < byte_count:
        chunk = os.pread(descriptor, min(COPY_CHUNK_BYTES, byte_count - offset), offset)
        if not chunk:
            break
        parts.append(chunk)
        offset += len(chunk)
    body = b"".join(parts)
    if len(body) != byte_count:
        raise LinkRepairError(f"{label} changed size while being read")
    return body


def _sha256_fd(descriptor: int, byte_count: int, label: str) -> str:
    digest = hashlib.sha256()
    offset = 0
    while offset < byte_count:
        chunk = os.pread(descriptor, min(COPY_CHUNK_BYTES, byte_count - offset), offset)
        if not chunk:
            break
        digest.update(chunk)
        offset += len(chunk)
    if offset != byte_count:
        raise LinkRepairError(f"{label} changed size while it was hashed")
    return digest.hexdigest()


def _implementation_identity() -> dict[str, Any]:
    source = Path(__file__).resolve(strict=True)
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(source, flags)
    try:
        before = os.fstat(descriptor)
        digest = _sha256_fd(descriptor, before.st_size, "repair implementation")
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if _file_fingerprint(before) != _file_fingerprint(after):
        raise LinkRepairError("repair implementation changed while it was hashed")
    return {
        "byte_count": before.st_size,
        "name": TOOL_NAME,
        "sha256": digest,
        "version": IMPLEMENTATION_VERSION,
    }


@dataclass(frozen=True)
class ResultBinding:
    artifact_path: Path
    artifact_sha256: str
    artifact_byte_count: int
    processing_run_id: str
    recipe_dir: Path
    run_dir: Path


def _result_binding(
    value: dict[str, Any], root: Path, result_path: Path
) -> ResultBinding:
    if value.get("schema_version") != 1 or value.get("status") != "completed":
        raise LinkRepairError("preprocess result must be a completed schema-v1 result")
    if value.get("result_path") != str(result_path):
        raise LinkRepairError("preprocess result_path does not name the retained result")
    layout = value.get("layout")
    run = value.get("processing_run")
    if not isinstance(layout, dict) or not isinstance(run, dict):
        raise LinkRepairError("preprocess result layout or processing_run is missing")
    run_dir = result_path.parent
    recipe_dir = run_dir.parent.parent
    if (
        layout.get("output_root") != str(root)
        or layout.get("run_dir") != str(run_dir)
        or layout.get("recipe_dir") != str(recipe_dir)
        or result_path != run_dir / "result.json"
        or run_dir.parent.name != "executions"
        or not run_dir.name.startswith("run_preprocess_")
    ):
        raise LinkRepairError("preprocess result layout is not the exact run-local layout")
    processing_run_id = run.get("processing_run_id")
    if (
        not isinstance(processing_run_id, str)
        or processing_run_id != run_dir.name
        or run.get("status") != "completed"
        or run.get("implementation_version") != "0.3.3"
    ):
        raise LinkRepairError("preprocess processing_run identity is inconsistent")
    reuse = value.get("reuse")
    if not isinstance(reuse, dict) or reuse.get("mode") != "verified_prior_result":
        raise LinkRepairError(
            "repair accepts only legacy v0.3.3 verified-prior hardlink reuse results"
        )
    artifacts = value.get("artifacts")
    if not isinstance(artifacts, list):
        raise LinkRepairError("preprocess result artifacts must be an array")
    matches = [
        artifact
        for artifact in artifacts
        if isinstance(artifact, dict) and artifact.get("artifact_kind") == ARTIFACT_KIND
    ]
    if len(matches) != 1:
        raise LinkRepairError(
            "preprocess result must contain exactly one normalized-audio artifact"
        )
    artifact = matches[0]
    artifact_path = run_dir / "artifacts" / ARTIFACT_BASENAME
    digest = artifact.get("sha256")
    byte_count = artifact.get("byte_count")
    if (
        artifact.get("path") != str(artifact_path)
        or artifact.get("storage_uri") != artifact_path.as_uri()
        or artifact.get("processing_run_id") != processing_run_id
        or artifact.get("schema_version") != 1
        or artifact.get("visibility") != "private"
        or artifact.get("media_kind") != "audio"
        or artifact.get("mime_type") != "audio/flac"
        or not isinstance(digest, str)
        or SHA256_RE.fullmatch(digest) is None
        or isinstance(byte_count, bool)
        or not isinstance(byte_count, int)
        or byte_count < 1
    ):
        raise LinkRepairError("normalized-audio artifact descriptor is inconsistent")
    expected_artifact_id = "artifact_" + _sha256_bytes(
        canonical_bytes(
            {
                "kind": ARTIFACT_KIND,
                "processing_run_id": processing_run_id,
                "sha256": digest,
            }
        )
    )[:32]
    if artifact.get("artifact_id") != expected_artifact_id:
        raise LinkRepairError("normalized-audio artifact id is inconsistent")
    _require_beneath(recipe_dir, root, "recipe directory")
    _require_beneath(artifact_path, root, "normalized-audio artifact")
    return ResultBinding(
        artifact_path=artifact_path,
        artifact_sha256=digest,
        artifact_byte_count=byte_count,
        processing_run_id=processing_run_id,
        recipe_dir=recipe_dir,
        run_dir=run_dir,
    )


def _inspect_result(
    root: Path, result_path: Path
) -> tuple[RetainedFile, bytes, str, ResultBinding]:
    retained = RetainedFile.open(
        root, result_path, modes={RESULT_MODE}, link_count=1
    )
    try:
        if retained.opened.st_size < 1 or retained.opened.st_size > MAX_RESULT_BYTES:
            raise LinkRepairError("preprocess result size is outside the allowed range")
        body = _pread_exact(retained.descriptor, retained.opened.st_size, "preprocess result")
        retained.verify_path(
            modes={RESULT_MODE}, link_count=1, expected=_file_fingerprint(retained.opened)
        )
        value = _parse_json(body, "preprocess result")
        binding = _result_binding(value, root, result_path)
        return retained, body, _sha256_bytes(body), binding
    except Exception:
        retained.close()
        raise


def _assert_outside(path: Path, root: Path, label: str) -> None:
    try:
        path.relative_to(root)
    except ValueError:
        return
    raise LinkRepairError(f"{label} must be outside the preprocess output root")


def _require_control_location(path: Path, root: Path, label: str) -> None:
    expected = root.parent / CONTROL_DIRECTORY_BASENAME
    if path.parent != expected:
        raise LinkRepairError(f"{label} must be directly under {expected}")


def _open_private_control_parent(
    path: Path, label: str, *, allow_existing: bool = False
) -> RetainedDirectoryChain:
    if not path.is_absolute() or path.name in {"", ".", ".."}:
        raise LinkRepairError(f"{label} must be an absolute named path")
    parent = _require_absolute_resolved(path.parent, f"{label} parent", directory=True)
    chain = RetainedDirectoryChain.open(parent, parent)
    if _mode(chain.opened[-1]) != CONTROL_DIRECTORY_MODE:
        chain.close()
        raise LinkRepairError(f"{label} parent must be mode 0700")
    try:
        os.stat(path.name, dir_fd=chain.fd, follow_symlinks=False)
    except FileNotFoundError:
        return chain
    except OSError as error:
        chain.close()
        raise LinkRepairError(f"cannot inspect {label}: {error}") from error
    if allow_existing:
        return chain
    chain.close()
    raise LinkRepairError(f"refusing to replace existing {label}: {path}")


def _publish_control_json(
    chain: RetainedDirectoryChain, path: Path, value: dict[str, Any], label: str
) -> tuple[int, str]:
    body = _sealed_json_bytes(value)
    if len(body) > MAX_CONTROL_BYTES:
        raise LinkRepairError(f"{label} exceeds the control document size limit")
    temporary = f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    flags = (
        os.O_RDWR
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor: int | None = None
    published = False
    try:
        descriptor = os.open(temporary, flags, 0o600, dir_fd=chain.fd)
        offset = 0
        while offset < len(body):
            written = os.write(descriptor, body[offset:])
            if written <= 0:
                raise LinkRepairError(f"short write while publishing {label}")
            offset += written
        os.fsync(descriptor)
        os.fchmod(descriptor, CONTROL_MODE)
        os.fsync(descriptor)
        if _pread_exact(descriptor, len(body), label) != body:
            raise LinkRepairError(f"{label} temp bytes differ before publication")
        _rename_noreplace(chain.fd, temporary, path.name)
        published = True
        committed = os.stat(path.name, dir_fd=chain.fd, follow_symlinks=False)
        opened = os.fstat(descriptor)
        if (
            not _same_object(committed, opened)
            or committed.st_nlink != 1
            or _mode(committed) != CONTROL_MODE
            or committed.st_uid != os.geteuid()
            or committed.st_size != len(body)
        ):
            raise LinkRepairError(f"committed {label} failed final verification")
        os.fsync(chain.fd)
        chain.verify()
        return len(body), _sha256_bytes(body)
    except Exception as error:
        if not published:
            try:
                os.unlink(temporary, dir_fd=chain.fd)
            except OSError:
                pass
        if isinstance(error, LinkRepairError):
            raise
        raise LinkRepairError(f"cannot publish {label}: {error}") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _read_control(path: Path, label: str) -> tuple[RetainedFile, bytes, dict[str, Any]]:
    parent = _require_absolute_resolved(path.parent, f"{label} parent", directory=True)
    retained = RetainedFile.open(
        parent, path, modes={CONTROL_MODE}, link_count=1
    )
    try:
        if retained.opened.st_size < 1 or retained.opened.st_size > MAX_CONTROL_BYTES:
            raise LinkRepairError(f"{label} size is outside the allowed range")
        body = _pread_exact(retained.descriptor, retained.opened.st_size, label)
        retained.verify_path(
            modes={CONTROL_MODE},
            link_count=1,
            expected=_file_fingerprint(retained.opened),
        )
        value = _parse_json(body, label)
        if body != _sealed_json_bytes(value):
            raise LinkRepairError(f"{label} is not exact canonical sealed JSON")
        return retained, body, value
    except Exception:
        retained.close()
        raise


def _plan_id(value_without_id: dict[str, Any]) -> str:
    return "repair_plan_" + _sha256_bytes(canonical_bytes(value_without_id))[:32]


def _expected_controller_lock(root: Path) -> Path:
    if root.name != "preprocess-output":
        raise LinkRepairError(
            "preprocess output root must use the autonomous campaign preprocess-output layout"
        )
    return root.parent / "state" / CONTROLLER_LOCK_BASENAME


def _inspect_controller_lock(root: Path, path: Path) -> dict[str, int]:
    expected_path = _expected_controller_lock(root)
    if path != expected_path:
        raise LinkRepairError(
            f"controller lock must be the campaign lock at {expected_path}"
        )
    parent = _require_absolute_resolved(
        path.parent, "controller state root", directory=True
    )
    if _mode(parent.lstat()) != CONTROL_DIRECTORY_MODE:
        raise LinkRepairError("controller state root must be mode 0700")
    with RetainedFile.open(
        parent, path, modes={LOCK_MODE}, link_count=1
    ) as retained:
        if retained.opened.st_size != 0:
            raise LinkRepairError("controller lock must be empty")
        expected = _file_fingerprint(retained.opened)
        retained.verify_path(
            modes={LOCK_MODE}, link_count=1, expected=expected
        )
        return expected


def build_plan(
    root: Path,
    result_path: Path,
    plan_path: Path,
    controller_lock_path: Path,
) -> dict[str, Any]:
    root = _require_absolute_resolved(root, "preprocess output root", directory=True)
    result_path = _require_absolute_resolved(
        result_path, "preprocess result", directory=False
    )
    _require_beneath(result_path, root, "preprocess result")
    _assert_outside(plan_path, root, "repair plan")
    _require_control_location(plan_path, root, "repair plan")
    plan_parent = _open_private_control_parent(plan_path, "repair plan")
    result: RetainedFile | None = None
    try:
        result, result_body, result_sha256, binding = _inspect_result(root, result_path)
        controller_lock_stat = _inspect_controller_lock(root, controller_lock_path)
        with RetainedFile.open(
            root,
            binding.artifact_path,
            modes=ARTIFACT_MODES,
            minimum_links=2,
        ) as artifact:
            artifact_before = _file_fingerprint(artifact.opened)
            if artifact_before["byte_count"] != binding.artifact_byte_count:
                raise LinkRepairError("artifact byte count disagrees with result envelope")
            observed_sha256 = _sha256_fd(
                artifact.descriptor, artifact.opened.st_size, "normalized-audio artifact"
            )
            artifact.verify_path(
                modes=ARTIFACT_MODES,
                minimum_links=2,
                expected=artifact_before,
            )
            if observed_sha256 != binding.artifact_sha256:
                raise LinkRepairError("artifact SHA-256 disagrees with result envelope")
            root_stat = root.lstat()
            if (
                not _same_object(root_stat, result.chain.opened[0])
                or not _same_object(root_stat, artifact.chain.opened[0])
            ):
                raise LinkRepairError(
                    "preprocess output root changed while the plan was captured"
                )
            result.verify_path(
                modes={RESULT_MODE},
                link_count=1,
                expected=_file_fingerprint(result.opened),
            )
            artifact.verify_path(
                modes=ARTIFACT_MODES,
                minimum_links=2,
                expected=artifact_before,
            )
            base: dict[str, Any] = {
                "artifact": {
                    "artifact_kind": ARTIFACT_KIND,
                    "byte_count": binding.artifact_byte_count,
                    "path": str(binding.artifact_path),
                    "sha256": binding.artifact_sha256,
                    "stat": artifact_before,
                },
                "authority": AUTHORITY,
                "coordination": {
                    "controller_lock_path": str(controller_lock_path),
                    "controller_lock_stat": controller_lock_stat,
                },
                "created_at": _utc_now(),
                "implementation": _implementation_identity(),
                "kind": PLAN_KIND,
                "plan_path": str(plan_path),
                "policy": POLICY,
                "result": {
                    "byte_count": len(result_body),
                    "path": str(result_path),
                    "processing_run_id": binding.processing_run_id,
                    "recipe_dir": str(binding.recipe_dir),
                    "sha256": result_sha256,
                    "stat": _file_fingerprint(result.opened),
                },
                "root": _directory_fingerprint(root_stat, root),
                "schema_version": SCHEMA_VERSION,
            }
            plan = dict(base)
            plan["plan_id"] = _plan_id(base)
            _validate_plan_document(plan, plan_path)
            byte_count, digest = _publish_control_json(
                plan_parent, plan_path, plan, "repair plan"
            )
            return {
                "artifact_path": str(binding.artifact_path),
                "plan_path": str(plan_path),
                "plan_sha256": digest,
                "plan_byte_count": byte_count,
                "state": "prepared_not_applied",
            }
    finally:
        if result is not None:
            result.close()
        plan_parent.close()


def _validate_file_stat(value: Any, label: str) -> dict[str, int]:
    if not isinstance(value, dict):
        raise LinkRepairError(f"{label} must be an object")
    keys = {
        "byte_count",
        "ctime_ns",
        "device",
        "gid",
        "inode",
        "link_count",
        "mode",
        "mtime_ns",
        "uid",
    }
    _require_exact_keys(value, keys, label)
    if any(isinstance(item, bool) or not isinstance(item, int) for item in value.values()):
        raise LinkRepairError(f"{label} fields must be integers")
    return value


def _validate_plan_document(plan: dict[str, Any], plan_path: Path) -> None:
    keys = {
        "artifact",
        "authority",
        "coordination",
        "created_at",
        "implementation",
        "kind",
        "plan_id",
        "plan_path",
        "policy",
        "result",
        "root",
        "schema_version",
    }
    _require_exact_keys(plan, keys, "repair plan")
    if (
        plan.get("schema_version") != SCHEMA_VERSION
        or plan.get("kind") != PLAN_KIND
        or plan.get("authority") != AUTHORITY
        or plan.get("policy") != POLICY
        or plan.get("plan_path") != str(plan_path)
        or plan.get("implementation") != _implementation_identity()
    ):
        raise LinkRepairError("repair plan contract or implementation binding differs")
    base = {key: value for key, value in plan.items() if key != "plan_id"}
    if plan.get("plan_id") != _plan_id(base):
        raise LinkRepairError("repair plan id is inconsistent")
    created_at = plan.get("created_at")
    if not isinstance(created_at, str):
        raise LinkRepairError("repair plan created_at must be a string")
    try:
        datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as error:
        raise LinkRepairError("repair plan created_at is not canonical UTC") from error
    root = plan.get("root")
    coordination = plan.get("coordination")
    result = plan.get("result")
    artifact = plan.get("artifact")
    if (
        not isinstance(root, dict)
        or not isinstance(coordination, dict)
        or not isinstance(result, dict)
        or not isinstance(artifact, dict)
    ):
        raise LinkRepairError(
            "repair plan coordination, root, result, and artifact must be objects"
        )
    _require_exact_keys(root, {"device", "gid", "inode", "mode", "path", "uid"}, "plan root")
    if not isinstance(root.get("path"), str) or any(
        isinstance(root.get(key), bool) or not isinstance(root.get(key), int)
        for key in ("device", "gid", "inode", "mode", "uid")
    ):
        raise LinkRepairError("repair plan root fields are invalid")
    root_path = Path(root["path"])
    if not root_path.is_absolute():
        raise LinkRepairError("repair plan root path must be absolute")
    _require_control_location(plan_path, root_path, "repair plan")
    _require_exact_keys(
        coordination,
        {"controller_lock_path", "controller_lock_stat"},
        "plan coordination",
    )
    if not isinstance(coordination.get("controller_lock_path"), str):
        raise LinkRepairError("repair plan controller lock path is invalid")
    if coordination["controller_lock_path"] != str(_expected_controller_lock(root_path)):
        raise LinkRepairError("repair plan does not bind the campaign controller lock")
    controller_lock_stat = _validate_file_stat(
        coordination.get("controller_lock_stat"), "plan controller lock stat"
    )
    if (
        controller_lock_stat["byte_count"] != 0
        or controller_lock_stat["mode"] != LOCK_MODE
        or controller_lock_stat["link_count"] != 1
    ):
        raise LinkRepairError("repair plan controller lock metadata is invalid")
    _require_exact_keys(
        result,
        {"byte_count", "path", "processing_run_id", "recipe_dir", "sha256", "stat"},
        "plan result",
    )
    _require_exact_keys(
        artifact,
        {"artifact_kind", "byte_count", "path", "sha256", "stat"},
        "plan artifact",
    )
    for label, row in (("plan result", result), ("plan artifact", artifact)):
        if (
            not isinstance(row.get("path"), str)
            or not isinstance(row.get("sha256"), str)
            or SHA256_RE.fullmatch(row["sha256"]) is None
            or isinstance(row.get("byte_count"), bool)
            or not isinstance(row.get("byte_count"), int)
            or row["byte_count"] < 1
        ):
            raise LinkRepairError(f"{label} identity fields are invalid")
        observed = _validate_file_stat(row.get("stat"), f"{label} stat")
        if observed["byte_count"] != row["byte_count"]:
            raise LinkRepairError(f"{label} stat byte count is inconsistent")
    if artifact.get("artifact_kind") != ARTIFACT_KIND:
        raise LinkRepairError("repair plan artifact kind is outside the allowlist")
    if artifact["stat"]["mode"] not in ARTIFACT_MODES or artifact["stat"]["link_count"] < 2:
        raise LinkRepairError("repair plan artifact is not an allowed immutable hardlink")
    if result["stat"]["mode"] != RESULT_MODE or result["stat"]["link_count"] != 1:
        raise LinkRepairError("repair plan result is not sealed mode 0444 and single-link")
    if not isinstance(result.get("processing_run_id"), str) or not isinstance(
        result.get("recipe_dir"), str
    ):
        raise LinkRepairError("repair plan result run identity is invalid")


def validate_plan(plan_path: Path) -> dict[str, Any]:
    retained, body, plan = _read_control(plan_path, "repair plan")
    try:
        _validate_plan_document(plan, plan_path)
        return {
            "artifact_path": plan["artifact"]["path"],
            "plan_id": plan["plan_id"],
            "plan_path": str(plan_path),
            "plan_sha256": _sha256_bytes(body),
            "state": "valid_not_applied",
        }
    finally:
        retained.close()


def _control_target_exists(chain: RetainedDirectoryChain, path: Path) -> bool:
    try:
        os.stat(path.name, dir_fd=chain.fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    except OSError as error:
        raise LinkRepairError(f"cannot inspect control target {path}: {error}") from error
    return True


def _verify_exact_single_link(
    artifact: RetainedFile, before: dict[str, int], expected_sha256: str
) -> dict[str, int]:
    current = _file_fingerprint(artifact.opened)
    preserved = ("byte_count", "gid", "mode", "uid")
    if current["link_count"] != 1 or any(current[key] != before[key] for key in preserved):
        raise LinkRepairError(
            "artifact differs from both the planned hardlink and exact repaired state"
        )
    artifact.verify_path(modes=ARTIFACT_MODES, link_count=1, expected=current)
    if (
        _sha256_fd(
            artifact.descriptor, current["byte_count"], "reconciled normalized audio"
        )
        != expected_sha256
    ):
        raise LinkRepairError("reconciled normalized-audio SHA-256 differs")
    # A retry after rename but before the original directory fsync must make the
    # already-observed exact target durable before it can publish a receipt.
    os.fsync(artifact.chain.fd)
    artifact.verify_path(modes=ARTIFACT_MODES, link_count=1, expected=current)
    return current


def _verify_published_artifact(
    root: Path,
    path: Path,
    expected: dict[str, int],
    expected_sha256: str,
) -> None:
    with RetainedFile.open(
        root, path, modes=ARTIFACT_MODES, link_count=1
    ) as retained:
        retained.verify_path(
            modes=ARTIFACT_MODES, link_count=1, expected=expected
        )
        if (
            _sha256_fd(
                retained.descriptor,
                expected["byte_count"],
                "final published normalized audio",
            )
            != expected_sha256
        ):
            raise LinkRepairError("final published normalized-audio SHA-256 differs")


def _validate_existing_receipt(
    *,
    receipt_path: Path,
    plan: dict[str, Any],
    plan_body: bytes,
    result_path: Path,
    result_sha256: str,
    current_artifact: dict[str, int],
) -> tuple[int, str]:
    retained, body, receipt = _read_control(receipt_path, "repair receipt")
    try:
        keys = {
            "artifact",
            "authority",
            "completed_at",
            "coordination",
            "existing_documents",
            "implementation",
            "kind",
            "plan_id",
            "plan_path",
            "plan_sha256",
            "policy",
            "receipt_path",
            "schema_version",
            "status",
        }
        _require_exact_keys(receipt, keys, "repair receipt")
        if (
            receipt.get("kind") != RECEIPT_KIND
            or receipt.get("schema_version") != SCHEMA_VERSION
            or receipt.get("status") != "completed"
            or receipt.get("authority") != AUTHORITY
            or receipt.get("policy") != POLICY
            or receipt.get("implementation") != plan["implementation"]
            or receipt.get("plan_id") != plan["plan_id"]
            or receipt.get("plan_path") != plan["plan_path"]
            or receipt.get("plan_sha256") != _sha256_bytes(plan_body)
            or receipt.get("receipt_path") != str(receipt_path)
        ):
            raise LinkRepairError("existing repair receipt is not bound to this plan")
        artifact = receipt.get("artifact")
        coordination = receipt.get("coordination")
        existing = receipt.get("existing_documents")
        if (
            not isinstance(artifact, dict)
            or not isinstance(coordination, dict)
            or not isinstance(existing, dict)
        ):
            raise LinkRepairError("existing repair receipt sections are invalid")
        _require_exact_keys(
            artifact,
            {
                "artifact_kind",
                "before",
                "detached_source_after",
                "outcome",
                "path",
                "replacement_after",
                "sha256",
            },
            "receipt artifact",
        )
        if (
            artifact.get("artifact_kind") != ARTIFACT_KIND
            or artifact.get("before") != plan["artifact"]["stat"]
            or artifact.get("path") != plan["artifact"]["path"]
            or artifact.get("sha256") != plan["artifact"]["sha256"]
            or artifact.get("replacement_after") != current_artifact
            or artifact.get("outcome")
            not in {
                "recovered_exchange",
                "reconciled_exact_single_link",
                "replaced",
            }
        ):
            raise LinkRepairError("existing repair receipt artifact binding differs")
        detached = artifact.get("detached_source_after")
        if detached is not None:
            detached = _validate_file_stat(detached, "detached source stat")
            if detached["link_count"] != plan["artifact"]["stat"]["link_count"] - 1:
                raise LinkRepairError("existing receipt detached-source count differs")
        if artifact["outcome"] in {"replaced", "recovered_exchange"} and detached is None:
            raise LinkRepairError("replacement receipt lacks detached-source evidence")
        if artifact["outcome"] == "reconciled_exact_single_link" and detached is not None:
            raise LinkRepairError("reconciliation receipt overclaims detached-source evidence")
        if coordination != {
            "controller_lock_path": plan["coordination"]["controller_lock_path"],
            "controller_lock_stat": plan["coordination"]["controller_lock_stat"],
            "policy": "nonblocking_exclusive_flock_held_through_receipt_commit",
        }:
            raise LinkRepairError("existing receipt coordination binding differs")
        if existing != {
            "receipts_modified": False,
            "result_envelope_modified": False,
            "result_path": str(result_path),
            "result_sha256": result_sha256,
        }:
            raise LinkRepairError("existing receipt result binding differs")
        completed_at = receipt.get("completed_at")
        if not isinstance(completed_at, str):
            raise LinkRepairError("existing receipt completed_at is invalid")
        try:
            datetime.strptime(completed_at, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError as error:
            raise LinkRepairError("existing receipt completed_at is not canonical UTC") from error
        return len(body), _sha256_bytes(body)
    finally:
        retained.close()


def _acquire_recipe_lock(root: Path, recipe_dir: Path) -> RetainedFile:
    lock_path = recipe_dir / LOCK_BASENAME
    retained = RetainedFile.open(
        root, lock_path, modes={LOCK_MODE}, link_count=1, writable=True
    )
    try:
        if retained.opened.st_size != 0:
            raise LinkRepairError("preprocess recipe lock must be empty")
        try:
            fcntl.flock(retained.descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise LinkRepairError("another preprocessing writer holds the recipe lock") from error
        retained.verify_path(modes={LOCK_MODE}, link_count=1)
        return retained
    except Exception:
        retained.close()
        raise


def _acquire_controller_lock(
    root: Path, path: Path, expected: dict[str, int]
) -> RetainedFile:
    if path != _expected_controller_lock(root):
        raise LinkRepairError("controller lock is not the campaign's exact run lock")
    if path.name != CONTROLLER_LOCK_BASENAME:
        raise LinkRepairError("controller lock path must end in controller.lock")
    parent = _require_absolute_resolved(
        path.parent, "controller state root", directory=True
    )
    if _mode(parent.lstat()) != CONTROL_DIRECTORY_MODE:
        raise LinkRepairError("controller state root must be mode 0700")
    retained = RetainedFile.open(
        parent, path, modes={LOCK_MODE}, link_count=1, writable=True
    )
    try:
        if retained.opened.st_size != 0:
            raise LinkRepairError("controller lock must be empty")
        if _file_fingerprint(retained.opened) != expected:
            raise LinkRepairError("controller lock identity changed after planning")
        try:
            fcntl.flock(retained.descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            if error.errno in {errno.EACCES, errno.EAGAIN}:
                raise LinkRepairError(
                    "controller.lock is occupied; the autonomous controller may be active"
                ) from error
            raise LinkRepairError(f"controller.lock acquisition failed: {error}") from error
        retained.verify_path(
            modes={LOCK_MODE}, link_count=1, expected=expected
        )
        return retained
    except Exception:
        retained.close()
        raise


def _write_all(descriptor: int, value: bytes) -> None:
    offset = 0
    while offset < len(value):
        written = os.write(descriptor, value[offset:])
        if written <= 0:
            raise LinkRepairError("short write while copying normalized audio")
        offset += written


def _rename_exchange(directory_fd: int, left: str, right: str) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise LinkRepairError(
            "atomic renameat2(RENAME_EXCHANGE) is unavailable; refusing repair"
        )
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    if (
        renameat2(
            directory_fd,
            os.fsencode(left),
            directory_fd,
            os.fsencode(right),
            RENAME_EXCHANGE,
        )
        != 0
    ):
        error_number = ctypes.get_errno()
        raise LinkRepairError(
            f"renameat2(RENAME_EXCHANGE) failed: {os.strerror(error_number)}"
        )


def _rename_noreplace(directory_fd: int, source: str, target: str) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise LinkRepairError(
            "atomic renameat2(RENAME_NOREPLACE) is unavailable; refusing publication"
        )
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    if (
        renameat2(
            directory_fd,
            os.fsencode(source),
            directory_fd,
            os.fsencode(target),
            RENAME_NOREPLACE,
        )
        != 0
    ):
        error_number = ctypes.get_errno()
        raise LinkRepairError(
            f"renameat2(RENAME_NOREPLACE) failed: {os.strerror(error_number)}"
        )


def _swap_name(path: Path, plan_id: str) -> str:
    if re.fullmatch(r"repair_plan_[0-9a-f]{32}", plan_id) is None:
        raise LinkRepairError("repair plan id is unsafe for transaction naming")
    return f".{path.name}.{plan_id}.swap"


def _named_stat(chain: RetainedDirectoryChain, name: str) -> os.stat_result | None:
    try:
        return os.stat(name, dir_fd=chain.fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise LinkRepairError(f"cannot inspect repair transaction path {name}: {error}") from error


def _discard_uncommitted_swap(
    artifact: RetainedFile,
    swap_name: str,
    before: dict[str, int],
) -> None:
    observed = _named_stat(artifact.chain, swap_name)
    if observed is None:
        return
    if (
        not stat.S_ISREG(observed.st_mode)
        or observed.st_uid != before["uid"]
        or observed.st_gid != before["gid"]
        or observed.st_nlink != 1
        or _mode(observed) not in {0o600, before["mode"]}
        or observed.st_size > before["byte_count"]
        or _same_object(observed, artifact.opened)
    ):
        raise LinkRepairError("stale repair swap has unsafe or ambiguous metadata")
    os.unlink(swap_name, dir_fd=artifact.chain.fd)
    os.fsync(artifact.chain.fd)
    artifact.verify_path(
        modes=ARTIFACT_MODES, minimum_links=2, expected=before
    )


def _finish_exchanged_swap(
    artifact: RetainedFile,
    swap_name: str,
    before: dict[str, int],
    expected_sha256: str,
) -> dict[str, int] | None:
    observed = _named_stat(artifact.chain, swap_name)
    if observed is None:
        return None
    swap_path = artifact.path.with_name(swap_name)
    with RetainedFile.open(
        artifact.chain.root,
        swap_path,
        modes=ARTIFACT_MODES,
        link_count=before["link_count"],
    ) as displaced:
        current = _file_fingerprint(displaced.opened)
        preserved = ("byte_count", "device", "gid", "inode", "mode", "uid")
        if any(current[key] != before[key] for key in preserved):
            raise LinkRepairError("pending repair swap is not the planned old inode")
        if (
            _sha256_fd(
                displaced.descriptor,
                before["byte_count"],
                "pending displaced normalized audio",
            )
            != expected_sha256
        ):
            raise LinkRepairError("pending repair swap bytes differ from the plan")
        displaced.verify_path(
            modes=ARTIFACT_MODES,
            link_count=before["link_count"],
            expected=current,
        )
        os.fsync(artifact.chain.fd)
        os.unlink(swap_name, dir_fd=artifact.chain.fd)
        os.fsync(artifact.chain.fd)
        detached = _file_fingerprint(os.fstat(displaced.descriptor))
        if detached["link_count"] != before["link_count"] - 1:
            raise LinkRepairError("recovered displaced source link count differs")
        stable = ("byte_count", "device", "gid", "inode", "mode", "mtime_ns", "uid")
        if any(detached[key] != before[key] for key in stable) or (
            _sha256_fd(
                displaced.descriptor,
                before["byte_count"],
                "detached recovered normalized audio",
            )
            != expected_sha256
        ):
            raise LinkRepairError("recovered displaced source changed during cleanup")
        return detached


def _copy_replace(
    artifact: RetainedFile,
    expected: dict[str, int],
    expected_sha256: str,
    swap_name: str,
) -> tuple[dict[str, int], dict[str, int]]:
    temporary = swap_name
    flags = (
        os.O_RDWR
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    temp_fd: int | None = None
    committed = False
    try:
        temp_fd = os.open(temporary, flags, 0o600, dir_fd=artifact.chain.fd)
        digest = hashlib.sha256()
        offset = 0
        byte_count = expected["byte_count"]
        while offset < byte_count:
            chunk = os.pread(
                artifact.descriptor, min(COPY_CHUNK_BYTES, byte_count - offset), offset
            )
            if not chunk:
                break
            digest.update(chunk)
            _write_all(temp_fd, chunk)
            offset += len(chunk)
        if offset != byte_count or digest.hexdigest() != expected_sha256:
            raise LinkRepairError("normalized-audio bytes differ while being copied")
        os.fsync(temp_fd)
        temp_stat = os.fstat(temp_fd)
        if (temp_stat.st_uid, temp_stat.st_gid) != (expected["uid"], expected["gid"]):
            try:
                os.fchown(temp_fd, expected["uid"], expected["gid"])
            except OSError as error:
                raise LinkRepairError("cannot preserve artifact uid/gid") from error
        os.fchmod(temp_fd, expected["mode"])
        os.fsync(temp_fd)
        temp_stat = os.fstat(temp_fd)
        if (
            not stat.S_ISREG(temp_stat.st_mode)
            or temp_stat.st_size != byte_count
            or temp_stat.st_uid != expected["uid"]
            or temp_stat.st_gid != expected["gid"]
            or _mode(temp_stat) != expected["mode"]
            or temp_stat.st_nlink != 1
            or _sha256_fd(temp_fd, byte_count, "replacement artifact")
            != expected_sha256
        ):
            raise LinkRepairError("replacement temp failed exact verification")
        artifact.verify_path(
            modes=ARTIFACT_MODES,
            minimum_links=2,
            expected=expected,
        )
        _rename_exchange(artifact.chain.fd, temporary, artifact.path.name)
        committed = True
        displaced = os.stat(
            temporary, dir_fd=artifact.chain.fd, follow_symlinks=False
        )
        current = os.stat(
            artifact.path.name, dir_fd=artifact.chain.fd, follow_symlinks=False
        )
        preserved = ("byte_count", "device", "gid", "inode", "mode", "uid")
        displaced_fingerprint = _file_fingerprint(displaced)
        if (
            any(displaced_fingerprint[key] != expected[key] for key in preserved)
            or displaced.st_nlink != expected["link_count"]
            or not _same_object(current, temp_stat)
        ):
            # Exchange is conditional in effect: the displaced name reveals the
            # exact entry present at the atomic boundary. Restore it rather than
            # overwriting an entry that raced the precondition check.
            _rename_exchange(artifact.chain.fd, temporary, artifact.path.name)
            committed = False
            restored = os.stat(
                artifact.path.name,
                dir_fd=artifact.chain.fd,
                follow_symlinks=False,
            )
            staged = os.stat(
                temporary, dir_fd=artifact.chain.fd, follow_symlinks=False
            )
            if not _same_object(restored, displaced) or not _same_object(
                staged, temp_stat
            ):
                raise LinkRepairError("repair target raced and atomic rollback failed")
            os.unlink(temporary, dir_fd=artifact.chain.fd)
            os.fsync(artifact.chain.fd)
            raise LinkRepairError("repair target changed at the atomic exchange boundary")
        os.fsync(artifact.chain.fd)
        artifact.chain.verify()
        target_fd = os.open(
            artifact.path.name,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
            dir_fd=artifact.chain.fd,
        )
        try:
            target_stat = os.fstat(target_fd)
            if (
                not _same_object(current, target_stat)
                or not _same_object(temp_stat, target_stat)
                or target_stat.st_size != byte_count
                or target_stat.st_uid != expected["uid"]
                or target_stat.st_gid != expected["gid"]
                or _mode(target_stat) != expected["mode"]
                or target_stat.st_nlink != 1
                or _sha256_fd(target_fd, byte_count, "committed replacement artifact")
                != expected_sha256
            ):
                raise LinkRepairError("committed replacement failed exact verification")
            replacement = _file_fingerprint(target_stat)
        finally:
            os.close(target_fd)
        os.unlink(temporary, dir_fd=artifact.chain.fd)
        os.fsync(artifact.chain.fd)
        detached_source = _file_fingerprint(os.fstat(artifact.descriptor))
        if detached_source["link_count"] != expected["link_count"] - 1:
            raise LinkRepairError("detached source link count changed unexpectedly")
        stable = ("byte_count", "device", "gid", "inode", "mode", "mtime_ns", "uid")
        if any(detached_source[key] != expected[key] for key in stable) or (
            _sha256_fd(
                artifact.descriptor,
                expected["byte_count"],
                "detached normalized-audio source",
            )
            != expected_sha256
        ):
            raise LinkRepairError("detached source changed during atomic exchange")
        return replacement, detached_source
    except Exception as error:
        if not committed:
            try:
                os.unlink(temporary, dir_fd=artifact.chain.fd)
            except OSError:
                pass
        if isinstance(error, LinkRepairError):
            raise
        raise LinkRepairError(f"cannot detach normalized-audio hardlink: {error}") from error
    finally:
        if temp_fd is not None:
            os.close(temp_fd)


def apply_plan(
    plan_path: Path, receipt_path: Path, controller_lock_path: Path
) -> dict[str, Any]:
    plan_file, plan_body, plan = _read_control(plan_path, "repair plan")
    receipt_parent: RetainedDirectoryChain | None = None
    controller_lock: RetainedFile | None = None
    lock: RetainedFile | None = None
    result: RetainedFile | None = None
    artifact: RetainedFile | None = None
    try:
        _validate_plan_document(plan, plan_path)
        root = Path(plan["root"]["path"])
        root = _require_absolute_resolved(root, "preprocess output root", directory=True)
        if str(controller_lock_path) != plan["coordination"]["controller_lock_path"]:
            raise LinkRepairError("apply controller lock differs from the reviewed plan")
        controller_lock = _acquire_controller_lock(
            root,
            controller_lock_path,
            plan["coordination"]["controller_lock_stat"],
        )
        _assert_outside(receipt_path, root, "repair receipt")
        _require_control_location(receipt_path, root, "repair receipt")
        receipt_parent = _open_private_control_parent(
            receipt_path, "repair receipt", allow_existing=True
        )
        receipt_exists = _control_target_exists(receipt_parent, receipt_path)
        root_stat = root.lstat()
        if _directory_fingerprint(root_stat, root) != plan["root"]:
            raise LinkRepairError("preprocess output root identity changed after planning")
        recipe_dir = Path(plan["result"]["recipe_dir"])
        lock = _acquire_recipe_lock(root, recipe_dir)
        result, result_body, result_sha256, binding = _inspect_result(
            root, Path(plan["result"]["path"])
        )
        if (
            _file_fingerprint(result.opened) != plan["result"]["stat"]
            or len(result_body) != plan["result"]["byte_count"]
            or result_sha256 != plan["result"]["sha256"]
            or binding.artifact_path != Path(plan["artifact"]["path"])
            or binding.artifact_sha256 != plan["artifact"]["sha256"]
            or binding.artifact_byte_count != plan["artifact"]["byte_count"]
            or binding.processing_run_id != plan["result"]["processing_run_id"]
            or binding.recipe_dir != recipe_dir
        ):
            raise LinkRepairError("preprocess result or artifact binding changed after planning")
        artifact = RetainedFile.open(
            root,
            binding.artifact_path,
            modes=ARTIFACT_MODES,
        )
        artifact_before = plan["artifact"]["stat"]
        transaction_name = _swap_name(binding.artifact_path, plan["plan_id"])
        current_artifact = _file_fingerprint(artifact.opened)
        if current_artifact == artifact_before:
            artifact.verify_path(
                modes=ARTIFACT_MODES,
                minimum_links=2,
                expected=artifact_before,
            )
            if (
                _sha256_fd(
                    artifact.descriptor,
                    artifact_before["byte_count"],
                    "normalized-audio artifact",
                )
                != binding.artifact_sha256
            ):
                raise LinkRepairError("normalized-audio SHA-256 changed after planning")
            _discard_uncommitted_swap(
                artifact, transaction_name, artifact_before
            )
            repaired_state = False
            recovered_detached_source = None
        else:
            current_artifact = _verify_exact_single_link(
                artifact, artifact_before, binding.artifact_sha256
            )
            recovered_detached_source = _finish_exchanged_swap(
                artifact,
                transaction_name,
                artifact_before,
                binding.artifact_sha256,
            )
            repaired_state = True
        result.verify_path(
            modes={RESULT_MODE}, link_count=1, expected=plan["result"]["stat"]
        )
        if receipt_exists:
            if not repaired_state:
                raise LinkRepairError(
                    "a completed repair receipt exists but the artifact remains hardlinked"
                )
            receipt_byte_count, receipt_sha256 = _validate_existing_receipt(
                receipt_path=receipt_path,
                plan=plan,
                plan_body=plan_body,
                result_path=result.path,
                result_sha256=result_sha256,
                current_artifact=current_artifact,
            )
            return {
                "artifact_path": str(binding.artifact_path),
                "receipt_byte_count": receipt_byte_count,
                "receipt_path": str(receipt_path),
                "receipt_sha256": receipt_sha256,
                "state": "applied_or_exact_replay",
            }
        if repaired_state:
            replacement = current_artifact
            detached_source = recovered_detached_source
            outcome = (
                "recovered_exchange"
                if detached_source is not None
                else "reconciled_exact_single_link"
            )
        else:
            replacement, detached_source = _copy_replace(
                artifact,
                artifact_before,
                binding.artifact_sha256,
                transaction_name,
            )
            outcome = "replaced"
        _verify_published_artifact(
            root,
            binding.artifact_path,
            replacement,
            binding.artifact_sha256,
        )
        result.verify_path(
            modes={RESULT_MODE}, link_count=1, expected=plan["result"]["stat"]
        )
        if (
            _sha256_fd(result.descriptor, len(result_body), "preprocess result after repair")
            != result_sha256
        ):
            raise LinkRepairError("preprocess result bytes changed during repair")
        plan_file.verify_path(
            modes={CONTROL_MODE},
            link_count=1,
            expected=_file_fingerprint(plan_file.opened),
        )
        receipt: dict[str, Any] = {
            "artifact": {
                "artifact_kind": ARTIFACT_KIND,
                "before": artifact_before,
                "detached_source_after": detached_source,
                "outcome": outcome,
                "path": str(binding.artifact_path),
                "replacement_after": replacement,
                "sha256": binding.artifact_sha256,
            },
            "authority": AUTHORITY,
            "completed_at": _utc_now(),
            "coordination": {
                "controller_lock_path": str(controller_lock_path),
                "controller_lock_stat": _file_fingerprint(controller_lock.opened),
                "policy": "nonblocking_exclusive_flock_held_through_receipt_commit",
            },
            "existing_documents": {
                "receipts_modified": False,
                "result_envelope_modified": False,
                "result_path": str(result.path),
                "result_sha256": result_sha256,
            },
            "implementation": _implementation_identity(),
            "kind": RECEIPT_KIND,
            "plan_id": plan["plan_id"],
            "plan_path": str(plan_path),
            "plan_sha256": _sha256_bytes(plan_body),
            "policy": POLICY,
            "receipt_path": str(receipt_path),
            "schema_version": SCHEMA_VERSION,
            "status": "completed",
        }
        receipt_byte_count, receipt_sha256 = _publish_control_json(
            receipt_parent, receipt_path, receipt, "repair receipt"
        )
        return {
            "artifact_path": str(binding.artifact_path),
            "receipt_byte_count": receipt_byte_count,
            "receipt_path": str(receipt_path),
            "receipt_sha256": receipt_sha256,
            "state": "applied",
        }
    finally:
        if artifact is not None:
            artifact.close()
        if result is not None:
            result.close()
        if lock is not None:
            try:
                fcntl.flock(lock.descriptor, fcntl.LOCK_UN)
            finally:
                lock.close()
        if receipt_parent is not None:
            receipt_parent.close()
        if controller_lock is not None:
            try:
                fcntl.flock(controller_lock.descriptor, fcntl.LOCK_UN)
            finally:
                controller_lock.close()
        plan_file.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan", help="create one exact immutable repair plan")
    plan.add_argument("--root", required=True)
    plan.add_argument("--result", required=True)
    plan.add_argument("--output", required=True)
    plan.add_argument("--controller-lock", required=True)
    validate = subparsers.add_parser("validate-plan", help="validate one sealed plan")
    validate.add_argument("--plan", required=True)
    apply = subparsers.add_parser("apply", help="apply one exact repair plan")
    apply.add_argument("--plan", required=True)
    apply.add_argument("--receipt", required=True)
    apply.add_argument("--controller-lock", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "plan":
            payload = build_plan(
                Path(args.root),
                Path(args.result),
                Path(args.output),
                Path(args.controller_lock),
            )
        elif args.command == "validate-plan":
            payload = validate_plan(Path(args.plan))
        else:
            payload = apply_plan(
                Path(args.plan), Path(args.receipt), Path(args.controller_lock)
            )
    except (LinkRepairError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 1
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
