"""Durable private-acquisition policy and review-only filesystem sealing.

The seal workflow never changes permissions.  ``plan`` describes the exact files
and owner-only modes an operator should review; ``validate`` reopens the portable
paths beneath an explicit root, verifies the requested modes and exact bytes, and
emits a receipt.  Import admission replays that validation, so a stale or copied
receipt is not authority by itself.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any


HANDLING_POLICY_KEYS = {
    "storage_scope",
    "publication_disposition",
    "publication_authority",
    "basis",
}
PUBLICATION_DISPOSITIONS = {"no_publication_authority", "never_publish"}
PLAN_KIND = "private_acquisition_seal_plan"
RECEIPT_KIND = "private_acquisition_seal_receipt"
SEAL_SCHEMA_VERSION = 1
MAX_JSON_BYTES = 32 * 1024 * 1024
SHA256_RE_LENGTH = 64


class PrivateAcquisitionError(ValueError):
    """Private-acquisition policy, path, or receipt validation failed."""


def canonical_json(value: object) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def validate_handling_policy(
    value: object, label: str = "handling_policy"
) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != HANDLING_POLICY_KEYS:
        raise PrivateAcquisitionError(
            f"{label} must contain exactly {sorted(HANDLING_POLICY_KEYS)}"
        )
    if value.get("storage_scope") != "private_canonical_cache":
        raise PrivateAcquisitionError(
            f"{label}.storage_scope must be private_canonical_cache"
        )
    disposition = value.get("publication_disposition")
    if disposition not in PUBLICATION_DISPOSITIONS:
        raise PrivateAcquisitionError(
            f"{label}.publication_disposition must be no_publication_authority "
            "or never_publish"
        )
    if value.get("publication_authority") != "none":
        raise PrivateAcquisitionError(
            f"{label}.publication_authority must be none"
        )
    basis = value.get("basis")
    if (
        not isinstance(basis, str)
        or not basis.strip()
        or len(basis) > 1_000
        or "\x00" in basis
    ):
        raise PrivateAcquisitionError(
            f"{label}.basis must be non-empty text of at most 1000 characters"
        )
    return {
        "storage_scope": "private_canonical_cache",
        "publication_disposition": disposition,
        "publication_authority": "none",
        "basis": basis,
    }


def _strict_json(body: bytes, label: str) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise PrivateAcquisitionError(
                    f"{label} contains duplicate JSON key {key!r}"
                )
            result[key] = value
        return result

    try:
        value = json.loads(body, object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PrivateAcquisitionError(f"{label} is not valid UTF-8 JSON") from error
    if not isinstance(value, dict):
        raise PrivateAcquisitionError(f"{label} must be a JSON object")
    return value


def _mode(value: os.stat_result) -> str:
    return f"{stat.S_IMODE(value.st_mode):04o}"


def _fingerprint(value: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        value.st_mode,
        value.st_nlink,
    )


def _portable_path(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise PrivateAcquisitionError(f"{label} must be a portable relative POSIX path")
    path = PurePosixPath(value)
    if path.is_absolute() or value != path.as_posix():
        raise PrivateAcquisitionError(f"{label} must be a canonical relative POSIX path")
    if any(part in {"", ".", ".."} for part in path.parts):
        raise PrivateAcquisitionError(f"{label} may not contain dot traversal")
    return path.parts


@dataclass
class _PinnedFile:
    role: str
    relative_path: str
    descriptor: int
    parent_descriptor: int
    leaf_name: str
    initial_stat: os.stat_result
    sha256: str
    body: bytes | None


class _PinnedTree:
    """Open a small explicit file set with no symlink-following or path escape."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(os.path.abspath(os.fspath(root)))
        try:
            root_lstat = self.root.lstat()
        except OSError as error:
            raise PrivateAcquisitionError(f"artifact root cannot be inspected: {error}") from error
        if stat.S_ISLNK(root_lstat.st_mode) or not stat.S_ISDIR(root_lstat.st_mode):
            raise PrivateAcquisitionError(
                "artifact root must be an existing regular non-symlink directory"
            )
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            self.root_descriptor = os.open(self.root, flags)
        except OSError as error:
            raise PrivateAcquisitionError(f"artifact root cannot be opened safely: {error}") from error
        root_opened = os.fstat(self.root_descriptor)
        if _fingerprint(root_opened) != _fingerprint(root_lstat):
            os.close(self.root_descriptor)
            raise PrivateAcquisitionError("artifact root changed while it was opened")
        self.root_stat = root_opened
        self._directories: dict[tuple[str, ...], tuple[int, int, str, os.stat_result]] = {
            (): (self.root_descriptor, -1, "", root_opened)
        }
        self._files: list[_PinnedFile] = []

    def close(self) -> None:
        for pinned in reversed(self._files):
            try:
                os.close(pinned.descriptor)
            except OSError:
                pass
        for parts, (descriptor, _, _, _) in sorted(
            self._directories.items(), key=lambda item: len(item[0]), reverse=True
        ):
            if not parts:
                continue
            try:
                os.close(descriptor)
            except OSError:
                pass
        try:
            os.close(self.root_descriptor)
        except OSError:
            pass

    def __enter__(self) -> "_PinnedTree":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _directory(self, parts: tuple[str, ...]) -> int:
        if parts in self._directories:
            return self._directories[parts][0]
        parent_parts = parts[:-1]
        parent = self._directory(parent_parts)
        name = parts[-1]
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            descriptor = os.open(name, flags, dir_fd=parent)
            opened = os.fstat(descriptor)
            current = os.stat(name, dir_fd=parent, follow_symlinks=False)
        except OSError as error:
            raise PrivateAcquisitionError(
                f"artifact directory {'/'.join(parts)!r} cannot be opened safely: {error}"
            ) from error
        if not stat.S_ISDIR(opened.st_mode) or _fingerprint(opened) != _fingerprint(current):
            os.close(descriptor)
            raise PrivateAcquisitionError(
                f"artifact directory {'/'.join(parts)!r} changed while opening"
            )
        self._directories[parts] = (descriptor, parent, name, opened)
        return descriptor

    def open_file(
        self,
        role: str,
        relative_path: object,
        *,
        capture: bool,
        maximum: int | None = None,
    ) -> _PinnedFile:
        parts = _portable_path(relative_path, f"{role} path")
        parent = self._directory(parts[:-1])
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(parts[-1], flags, dir_fd=parent)
            before = os.fstat(descriptor)
        except OSError as error:
            raise PrivateAcquisitionError(
                f"{role} artifact {PurePosixPath(*parts)} cannot be opened safely: {error}"
            ) from error
        if not stat.S_ISREG(before.st_mode):
            os.close(descriptor)
            raise PrivateAcquisitionError(f"{role} artifact must be a regular file")
        if maximum is not None and before.st_size > maximum:
            os.close(descriptor)
            raise PrivateAcquisitionError(f"{role} artifact exceeds its size bound")
        digest = hashlib.sha256()
        chunks: list[bytes] | None = [] if capture else None
        offset = 0
        try:
            while offset < before.st_size:
                chunk = os.pread(descriptor, min(8 * 1024 * 1024, before.st_size - offset), offset)
                if not chunk:
                    raise PrivateAcquisitionError(f"{role} artifact ended while hashing")
                digest.update(chunk)
                if chunks is not None:
                    chunks.append(chunk)
                offset += len(chunk)
            if os.pread(descriptor, 1, before.st_size):
                raise PrivateAcquisitionError(f"{role} artifact grew while hashing")
            after = os.fstat(descriptor)
            current = os.stat(parts[-1], dir_fd=parent, follow_symlinks=False)
        except OSError as error:
            os.close(descriptor)
            raise PrivateAcquisitionError(f"{role} artifact cannot be hashed safely: {error}") from error
        if _fingerprint(before) != _fingerprint(after) or _fingerprint(before) != _fingerprint(current):
            os.close(descriptor)
            raise PrivateAcquisitionError(f"{role} artifact changed while hashing")
        pinned = _PinnedFile(
            role=role,
            relative_path=PurePosixPath(*parts).as_posix(),
            descriptor=descriptor,
            parent_descriptor=parent,
            leaf_name=parts[-1],
            initial_stat=before,
            sha256=digest.hexdigest(),
            body=None if chunks is None else b"".join(chunks),
        )
        self._files.append(pinned)
        return pinned

    def verify(self) -> None:
        try:
            root_current = self.root.lstat()
        except OSError as error:
            raise PrivateAcquisitionError(f"artifact root disappeared: {error}") from error
        if _fingerprint(root_current) != _fingerprint(self.root_stat):
            raise PrivateAcquisitionError("artifact root changed during validation")
        for parts, (descriptor, parent, name, initial) in self._directories.items():
            opened = os.fstat(descriptor)
            current = opened if not parts else os.stat(name, dir_fd=parent, follow_symlinks=False)
            if _fingerprint(initial) != _fingerprint(opened) or _fingerprint(initial) != _fingerprint(current):
                raise PrivateAcquisitionError(
                    f"artifact directory {PurePosixPath(*parts)} changed during validation"
                )
        for pinned in self._files:
            opened = os.fstat(pinned.descriptor)
            current = os.stat(
                pinned.leaf_name,
                dir_fd=pinned.parent_descriptor,
                follow_symlinks=False,
            )
            if (
                _fingerprint(pinned.initial_stat) != _fingerprint(opened)
                or _fingerprint(pinned.initial_stat) != _fingerprint(current)
            ):
                raise PrivateAcquisitionError(
                    f"{pinned.role} artifact changed during validation"
                )

    def directory_rows(self) -> list[dict[str, str]]:
        rows = []
        for parts, (_, _, _, value) in self._directories.items():
            rows.append(
                {
                    "relative_path": "." if not parts else PurePosixPath(*parts).as_posix(),
                    "current_mode": _mode(value),
                    "required_mode": "0700",
                }
            )
        return sorted(rows, key=lambda row: row["relative_path"])


def _file_row(pinned: _PinnedFile) -> dict[str, object]:
    return {
        "role": pinned.role,
        "relative_path": pinned.relative_path,
        "sha256": pinned.sha256,
        "byte_count": pinned.initial_stat.st_size,
        "current_mode": _mode(pinned.initial_stat),
        "required_mode": "0600",
    }


def _validate_bound_artifacts(
    work_order: dict[str, Any], result: dict[str, Any], media: _PinnedFile
) -> dict[str, Any]:
    if work_order.get("schema_version") != 1 or work_order.get("adapter") != "local_file":
        raise PrivateAcquisitionError("seal work order must be local_file schema version 1")
    policy = validate_handling_policy(work_order.get("handling_policy"), "work order handling_policy")
    source = work_order.get("source")
    if not isinstance(source, dict) or source.get("access_state") != "unknown":
        raise PrivateAcquisitionError(
            "sealed local_file work order source.access_state must be unknown"
        )
    for key in ("platform", "source_kind", "native_id"):
        if not isinstance(source.get(key), str) or not source[key]:
            raise PrivateAcquisitionError(f"work order source.{key} is invalid")
    if (
        result.get("schema_version") != 1
        or result.get("adapter") != "local_file"
        or result.get("status") != "completed"
        or result.get("dry_run") is not False
    ):
        raise PrivateAcquisitionError("seal result must be a completed local_file result-v1")
    work_order_sha256 = sha256_bytes(canonical_json(work_order).encode("utf-8"))
    if result.get("work_order_sha256") != work_order_sha256:
        raise PrivateAcquisitionError("seal result does not bind the exact work order")
    if result.get("source") != source:
        raise PrivateAcquisitionError("seal result source differs from its work order")
    if result.get("handling_policy") != policy:
        raise PrivateAcquisitionError("seal result handling_policy differs from its work order")
    try:
        source_row = result["catalog_records"]["sources"][0]
        media_row = result["catalog_records"]["media_objects"][0]
        media_source_row = result["catalog_records"]["media_sources"][0]
        admission = result["admission"]
    except (KeyError, IndexError, TypeError) as error:
        raise PrivateAcquisitionError("seal result lacks its exact catalog binding") from error
    metadata = source_row.get("metadata_json")
    expected_metadata = {
        "acquisition_adapter": "local_file",
        "selected_remote_metadata": result.get("selected_remote_metadata"),
        "handling_policy": policy,
    }
    if metadata != expected_metadata:
        raise PrivateAcquisitionError(
            "seal result source metadata does not preserve the exact handling policy"
        )
    if any(source_row.get(key) != source.get(key) for key in source):
        raise PrivateAcquisitionError("seal catalog source differs from the result source")
    digest = admission.get("sha256")
    byte_count = admission.get("byte_count")
    media_id = admission.get("media_id")
    if (
        not isinstance(digest, str)
        or len(digest) != SHA256_RE_LENGTH
        or any(character not in "0123456789abcdef" for character in digest)
        or media_id != f"media_sha256_{digest}"
        or byte_count != media.initial_stat.st_size
        or digest != media.sha256
    ):
        raise PrivateAcquisitionError("seal media bytes differ from the result admission")
    if (
        media_row.get("media_id") != media_id
        or media_row.get("sha256") != digest
        or media_row.get("byte_count") != byte_count
        or media_source_row.get("media_id") != media_id
        or media_source_row.get("source_id") != source_row.get("source_id")
    ):
        raise PrivateAcquisitionError("seal catalog media/source binding is inconsistent")
    return {
        "work_order_sha256": work_order_sha256,
        "media_id": media_id,
        "media_sha256": digest,
        "media_byte_count": byte_count,
        "source": {
            "source_id": source_row.get("source_id"),
            "platform": source["platform"],
            "source_kind": source["source_kind"],
            "native_id": source["native_id"],
            "access_state": "unknown",
        },
        "handling_policy": policy,
    }


def build_private_acquisition_seal_plan(
    artifact_root: str | Path,
    *,
    work_order_path: str,
    result_path: str,
    media_path: str,
) -> dict[str, Any]:
    """Build a portable, non-mutating permission plan for three exact artifacts."""

    requested = [work_order_path, result_path, media_path]
    if len(set(requested)) != 3:
        raise PrivateAcquisitionError("work order, result, and media paths must be distinct")
    with _PinnedTree(artifact_root) as tree:
        work_order_file = tree.open_file(
            "work_order", work_order_path, capture=True, maximum=MAX_JSON_BYTES
        )
        result_file = tree.open_file(
            "result", result_path, capture=True, maximum=MAX_JSON_BYTES
        )
        media_file = tree.open_file("media", media_path, capture=False)
        assert work_order_file.body is not None and result_file.body is not None
        work_order = _strict_json(work_order_file.body, "seal work order")
        result = _strict_json(result_file.body, "seal result")
        binding = _validate_bound_artifacts(work_order, result, media_file)
        tree.verify()
        return {
            "schema_version": SEAL_SCHEMA_VERSION,
            "kind": PLAN_KIND,
            **binding,
            "result_canonical_sha256": sha256_bytes(
                canonical_json(result).encode("utf-8")
            ),
            "source_byte_identity_claimed": False,
            "artifacts": [
                _file_row(work_order_file),
                _file_row(result_file),
                _file_row(media_file),
            ],
            "directories": tree.directory_rows(),
        }


def _validate_plan_shape(plan: object) -> dict[str, Any]:
    if not isinstance(plan, dict):
        raise PrivateAcquisitionError("seal plan must be a JSON object")
    expected = {
        "schema_version",
        "kind",
        "work_order_sha256",
        "result_canonical_sha256",
        "media_id",
        "media_sha256",
        "media_byte_count",
        "source",
        "handling_policy",
        "source_byte_identity_claimed",
        "artifacts",
        "directories",
    }
    if set(plan) != expected or plan.get("schema_version") != SEAL_SCHEMA_VERSION or plan.get("kind") != PLAN_KIND:
        raise PrivateAcquisitionError("seal plan has unsupported or unknown fields")
    if plan.get("source_byte_identity_claimed") is not False:
        raise PrivateAcquisitionError("seal plan may not claim source byte identity")
    validate_handling_policy(plan.get("handling_policy"), "seal plan handling_policy")
    artifacts = plan.get("artifacts")
    directories = plan.get("directories")
    if not isinstance(artifacts, list) or len(artifacts) != 3:
        raise PrivateAcquisitionError("seal plan must contain three artifacts")
    if not isinstance(directories, list) or not directories:
        raise PrivateAcquisitionError("seal plan must contain its directory chain")
    roles = {row.get("role") for row in artifacts if isinstance(row, dict)}
    if roles != {"work_order", "result", "media"}:
        raise PrivateAcquisitionError("seal plan artifact roles are invalid")
    for row in artifacts:
        if not isinstance(row, dict) or set(row) != {
            "role", "relative_path", "sha256", "byte_count", "current_mode", "required_mode"
        }:
            raise PrivateAcquisitionError("seal plan artifact row is invalid")
        _portable_path(row["relative_path"], "seal plan artifact path")
        if row["required_mode"] != "0600":
            raise PrivateAcquisitionError("seal plan file mode must be 0600")
    for row in directories:
        if not isinstance(row, dict) or set(row) != {
            "relative_path", "current_mode", "required_mode"
        }:
            raise PrivateAcquisitionError("seal plan directory row is invalid")
        if row["relative_path"] != ".":
            _portable_path(row["relative_path"], "seal plan directory path")
        if row["required_mode"] != "0700":
            raise PrivateAcquisitionError("seal plan directory mode must be 0700")
    return plan


def validate_private_acquisition_seal_plan(
    artifact_root: str | Path, plan: object
) -> dict[str, Any]:
    """Reopen a reviewed plan and emit a receipt only when owner-only modes hold."""

    plan = _validate_plan_shape(plan)
    paths = {row["role"]: row["relative_path"] for row in plan["artifacts"]}
    current = build_private_acquisition_seal_plan(
        artifact_root,
        work_order_path=paths["work_order"],
        result_path=paths["result"],
        media_path=paths["media"],
    )
    binding_fields = {
        "work_order_sha256",
        "result_canonical_sha256",
        "media_id",
        "media_sha256",
        "media_byte_count",
        "source",
        "handling_policy",
        "source_byte_identity_claimed",
    }
    if any(current[field] != plan[field] for field in binding_fields):
        raise PrivateAcquisitionError("seal plan binding differs from current artifacts")
    planned_files = {
        (row["role"], row["relative_path"], row["sha256"], row["byte_count"], row["required_mode"])
        for row in plan["artifacts"]
    }
    current_files = {
        (row["role"], row["relative_path"], row["sha256"], row["byte_count"], row["required_mode"])
        for row in current["artifacts"]
    }
    if planned_files != current_files:
        raise PrivateAcquisitionError("seal plan artifact set differs from current files")
    planned_directories = {
        (row["relative_path"], row["required_mode"]) for row in plan["directories"]
    }
    current_directories = {
        (row["relative_path"], row["required_mode"]) for row in current["directories"]
    }
    if planned_directories != current_directories:
        raise PrivateAcquisitionError("seal plan directory set differs from current paths")
    unsealed_files = [
        row["relative_path"]
        for row in current["artifacts"]
        if row["current_mode"] != row["required_mode"]
    ]
    unsealed_directories = [
        row["relative_path"]
        for row in current["directories"]
        if row["current_mode"] != row["required_mode"]
    ]
    if unsealed_files or unsealed_directories:
        raise PrivateAcquisitionError(
            "private artifacts are not owner-only; files="
            f"{unsealed_files}, directories={unsealed_directories}"
        )
    return {
        "schema_version": SEAL_SCHEMA_VERSION,
        "kind": RECEIPT_KIND,
        "validated_at": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "plan_sha256": sha256_bytes(canonical_json(plan).encode("utf-8")),
        "plan": plan,
        "sealed_artifacts": current["artifacts"],
        "sealed_directories": current["directories"],
    }


def validate_private_acquisition_seal_receipt(
    artifact_root: str | Path, receipt: object
) -> dict[str, Any]:
    """Replay a receipt against current descriptors; receipts are never bearer tokens."""

    if not isinstance(receipt, dict) or set(receipt) != {
        "schema_version",
        "kind",
        "validated_at",
        "plan_sha256",
        "plan",
        "sealed_artifacts",
        "sealed_directories",
    }:
        raise PrivateAcquisitionError("private acquisition seal receipt is malformed")
    if receipt.get("schema_version") != SEAL_SCHEMA_VERSION or receipt.get("kind") != RECEIPT_KIND:
        raise PrivateAcquisitionError("private acquisition seal receipt version is unsupported")
    timestamp = receipt.get("validated_at")
    if not isinstance(timestamp, str):
        raise PrivateAcquisitionError("seal receipt validated_at is invalid")
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as error:
        raise PrivateAcquisitionError("seal receipt validated_at is invalid") from error
    if parsed.tzinfo is None:
        raise PrivateAcquisitionError("seal receipt validated_at must include a UTC offset")
    plan = _validate_plan_shape(receipt.get("plan"))
    expected_plan_sha256 = sha256_bytes(canonical_json(plan).encode("utf-8"))
    if receipt.get("plan_sha256") != expected_plan_sha256:
        raise PrivateAcquisitionError("seal receipt plan digest is inconsistent")
    replay = validate_private_acquisition_seal_plan(artifact_root, plan)
    if (
        replay["sealed_artifacts"] != receipt.get("sealed_artifacts")
        or replay["sealed_directories"] != receipt.get("sealed_directories")
    ):
        raise PrivateAcquisitionError("seal receipt differs from current artifact replay")
    return receipt


def load_seal_receipt(path: str | Path) -> dict[str, Any]:
    try:
        body = Path(path).read_bytes()
    except OSError as error:
        raise PrivateAcquisitionError(f"cannot read private acquisition seal receipt: {error}") from error
    if len(body) > MAX_JSON_BYTES:
        raise PrivateAcquisitionError("private acquisition seal receipt is too large")
    return _strict_json(body, "private acquisition seal receipt")


def publication_restriction(
    connection: sqlite3.Connection, object_type: str, object_id: str
) -> sqlite3.Row | None:
    """Return an effective restriction, failing closed when migration 0030 is absent."""

    try:
        return connection.execute(
            """
            SELECT object_type, object_id, publication_disposition,
                   publication_authority
            FROM effective_acquisition_handling_restrictions
            WHERE object_type = ? AND object_id = ?
            """,
            (object_type, object_id),
        ).fetchone()
    except sqlite3.OperationalError as error:
        raise PrivateAcquisitionError(
            "private acquisition restriction schema is unavailable; apply pending "
            "migrations before publication or export"
        ) from error


def assert_no_restricted_publication_state(connection: sqlite3.Connection) -> None:
    """Reject export/validation if a restriction conflicts with current publish state."""

    try:
        conflict = connection.execute(
            """
            SELECT restriction.object_type, restriction.object_id,
                   restriction.publication_disposition
            FROM effective_acquisition_handling_restrictions AS restriction
            JOIN current_publication_decisions AS publication
              ON publication.object_type = restriction.object_type
             AND publication.object_id = restriction.object_id
             AND publication.decision = 'publish'
            ORDER BY restriction.object_type, restriction.object_id
            LIMIT 1
            """
        ).fetchone()
    except sqlite3.OperationalError as error:
        raise PrivateAcquisitionError(
            "private acquisition restriction schema is unavailable; export fails closed"
        ) from error
    if conflict is not None:
        raise PrivateAcquisitionError(
            "current publish state conflicts with private acquisition restriction: "
            f"{conflict['object_type']} {conflict['object_id']} "
            f"({conflict['publication_disposition']})"
        )


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Plan or validate owner-only private acquisition artifact sealing"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    plan_parser = commands.add_parser("plan")
    plan_parser.add_argument("--artifact-root", required=True)
    plan_parser.add_argument("--work-order", required=True, help="portable path beneath root")
    plan_parser.add_argument("--result", required=True, help="portable path beneath root")
    plan_parser.add_argument("--media", required=True, help="portable path beneath root")
    validate_parser = commands.add_parser("validate")
    validate_parser.add_argument("--artifact-root", required=True)
    validate_parser.add_argument("--plan", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            value = build_private_acquisition_seal_plan(
                args.artifact_root,
                work_order_path=args.work_order,
                result_path=args.result,
                media_path=args.media,
            )
        else:
            try:
                plan_body = Path(args.plan).read_bytes()
            except OSError as error:
                raise PrivateAcquisitionError(f"cannot read seal plan: {error}") from error
            value = validate_private_acquisition_seal_plan(
                args.artifact_root, _strict_json(plan_body, "seal plan")
            )
    except PrivateAcquisitionError as error:
        print(json.dumps({"error": str(error)}, sort_keys=True), file=os.sys.stderr)
        return 2
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
