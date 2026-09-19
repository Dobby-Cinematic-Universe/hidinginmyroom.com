#!/usr/bin/env python3
"""Build a deterministic explicit-selection addendum for one Archive.org item.

This is an offline planning boundary.  It reads a successor catalog, a newly sealed
Archive.org metadata snapshot, and a predecessor queue/snapshot evidence set.  It
never mutates the catalog, performs network access, acquires media, or grants
publication authority.

Only four exact equivalence signals are admitted:

* a recording_id already present in the predecessor queue plan;
* an exact terminal YouTube ID in a dash or bracket Archive.org filename form;
* an exact provider-declared SHA-1 for original files; or
* an exact provider-declared MD5 for original files.

Titles are deliberately absent from the matching implementation.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
from typing import Any, Iterable
import unicodedata
import uuid


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT))
sys.path.insert(0, str(REPOSITORY_ROOT / "acquisition"))
sys.path.insert(0, str(REPOSITORY_ROOT / "corpus" / "src"))

from acquisition.archive_org_metadata import (  # noqa: E402
    ArchiveMetadataError,
    validate_snapshot,
)
from acquisition.materialize_queue import (  # noqa: E402
    MaterializationError,
    validate_plan,
)
from acquisition.plan_queue import PlanningError, validate_selection_manifest  # noqa: E402
from himr_corpus.ids import source_id as canonical_source_id  # noqa: E402


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
MAX_PLAN_BYTES = 64 * 1024 * 1024
MAX_REPORT_BYTES = 64 * 1024 * 1024
MAX_DATABASE_BYTES = 4 * 1024**4
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SHA1_RE = re.compile(r"^[0-9a-fA-F]{40}$")
MD5_RE = re.compile(r"^[0-9a-fA-F]{32}$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")
YOUTUBE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
VIDEO_EXTENSION_RE = re.compile(
    r"\.(?:mp4|webm|ogv|mkv|mov|m4v)$", re.IGNORECASE
)
BRACKET_VIDEO_ID_RE = re.compile(r"\[([A-Za-z0-9_-]{11})\]$")
DASH_VIDEO_ID_RE = re.compile(r"-([A-Za-z0-9_-]{11})(?:-\d+)?$")
PROVIDER_HASH_PATTERNS = {"sha1": SHA1_RE, "md5": MD5_RE}


class ArchiveCollectionAddendumError(RuntimeError):
    """An input, integrity, equivalence, or immutable-output check failed."""


def canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ArchiveCollectionAddendumError(
            f"value cannot be canonically encoded: {error}"
        ) from error


def pretty_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ArchiveCollectionAddendumError(
            f"value cannot be encoded as canonical pretty JSON: {error}"
        ) from error


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def stable_id(prefix: str, value: Any) -> str:
    return f"{prefix}_{sha256_bytes(canonical_bytes(value))[:32]}"


def _reject_json_constant(value: str) -> None:
    raise ArchiveCollectionAddendumError(
        f"JSON contains forbidden non-finite constant {value}"
    )


def _reject_duplicate_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ArchiveCollectionAddendumError(
                f"JSON contains duplicate key {key!r}"
            )
        result[key] = value
    return result


def strict_json(body: bytes, label: str) -> Any:
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ArchiveCollectionAddendumError(f"{label} is not UTF-8") from error
    try:
        return json.loads(
            text,
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=_reject_json_constant,
        )
    except json.JSONDecodeError as error:
        raise ArchiveCollectionAddendumError(
            f"{label} is not strict JSON: {error}"
        ) from error


def _normalized_absolute(path: str | Path, label: str, *, existing: bool) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        raise ArchiveCollectionAddendumError(f"{label} must be an absolute path")
    if Path(os.path.normpath(str(candidate))) != candidate:
        raise ArchiveCollectionAddendumError(
            f"{label} must not contain redundant or traversal components"
        )
    try:
        resolved = candidate.resolve(strict=existing)
    except (OSError, RuntimeError) as error:
        raise ArchiveCollectionAddendumError(
            f"cannot resolve {label}: {error}"
        ) from error
    if resolved != candidate:
        raise ArchiveCollectionAddendumError(
            f"{label} must be resolved and contain no symlink components"
        )
    return candidate


def _stable_regular_file(
    path: str | Path,
    label: str,
    *,
    maximum: int,
    sealed: bool,
) -> tuple[Path, bytes, os.stat_result]:
    resolved = _normalized_absolute(path, label, existing=True)
    try:
        before = resolved.lstat()
    except OSError as error:
        raise ArchiveCollectionAddendumError(f"cannot inspect {label}: {error}") from error
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ArchiveCollectionAddendumError(f"{label} must be a regular non-symlink file")
    if sealed and before.st_mode & 0o222:
        raise ArchiveCollectionAddendumError(f"{label} must be sealed read-only")
    if before.st_size > maximum:
        raise ArchiveCollectionAddendumError(f"{label} exceeds {maximum} bytes")
    try:
        with resolved.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            identity = (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
            )
            expected_identity = (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
            )
            if identity != expected_identity:
                raise ArchiveCollectionAddendumError(f"{label} changed while opening")
            body = handle.read()
            after_fd = os.fstat(handle.fileno())
        after = resolved.stat()
    except OSError as error:
        raise ArchiveCollectionAddendumError(f"cannot read {label}: {error}") from error
    for observed in (after_fd, after):
        if (
            observed.st_dev,
            observed.st_ino,
            observed.st_size,
            observed.st_mtime_ns,
        ) != identity:
            raise ArchiveCollectionAddendumError(f"{label} changed while reading")
    return resolved, body, before


def _required_expected_sha256(value: str, label: str) -> str:
    if not SHA256_RE.fullmatch(value):
        raise ArchiveCollectionAddendumError(
            f"{label} must be a lowercase SHA-256"
        )
    return value


def _verify_digest(body: bytes, expected: str, label: str) -> str:
    expected = _required_expected_sha256(expected, f"expected {label} SHA-256")
    observed = sha256_bytes(body)
    if observed != expected:
        raise ArchiveCollectionAddendumError(
            f"{label} SHA-256 differs: expected {expected}, observed {observed}"
        )
    return observed


def _hash_file_stably(path: Path, label: str, expected: str) -> tuple[str, int]:
    expected = _required_expected_sha256(expected, f"expected {label} SHA-256")
    before = path.stat()
    if not stat.S_ISREG(before.st_mode) or path.is_symlink():
        raise ArchiveCollectionAddendumError(f"{label} must be a regular non-symlink file")
    if before.st_size > MAX_DATABASE_BYTES:
        raise ArchiveCollectionAddendumError(
            f"{label} exceeds {MAX_DATABASE_BYTES} bytes"
        )
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            identity = (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
            )
            if identity != (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
            ):
                raise ArchiveCollectionAddendumError(f"{label} changed while opening")
            while block := handle.read(8 * 1024 * 1024):
                digest.update(block)
            after_fd = os.fstat(handle.fileno())
        after = path.stat()
    except OSError as error:
        raise ArchiveCollectionAddendumError(f"cannot hash {label}: {error}") from error
    for observed in (after_fd, after):
        if (
            observed.st_dev,
            observed.st_ino,
            observed.st_size,
            observed.st_mtime_ns,
        ) != identity:
            raise ArchiveCollectionAddendumError(f"{label} changed while hashing")
    observed_digest = digest.hexdigest()
    if observed_digest != expected:
        raise ArchiveCollectionAddendumError(
            f"{label} SHA-256 differs: expected {expected}, observed {observed_digest}"
        )
    return observed_digest, before.st_size


def terminal_youtube_id(filename: str | None) -> str | None:
    """Return only an exact terminal dash/bracket YouTube ID filename signal."""

    if not isinstance(filename, str) or not filename:
        return None
    value = unicodedata.normalize("NFC", filename)
    value = re.sub(r"\.ia(?=\.[^.]+$)", "", value, flags=re.IGNORECASE)
    value = VIDEO_EXTENSION_RE.sub("", value)
    bracket = BRACKET_VIDEO_ID_RE.search(value)
    if bracket is not None:
        return bracket.group(1)
    dash = DASH_VIDEO_ID_RE.search(value)
    return dash.group(1) if dash is not None else None


def _candidate_youtube_id(candidate: dict[str, Any]) -> str | None:
    if candidate["platform"] == "youtube":
        native_id = candidate["native_id"]
        return native_id if YOUTUBE_ID_RE.fullmatch(native_id) else None
    if candidate["platform"] == "internet_archive":
        return terminal_youtube_id(candidate["native_id"])
    return None


def _provider_hash(value: Any, algorithm: str, label: str) -> str | None:
    if value in (None, ""):
        return None
    pattern = PROVIDER_HASH_PATTERNS[algorithm]
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ArchiveCollectionAddendumError(
            f"{label}.{algorithm} is not a valid provider digest"
        )
    return value.lower()


def _provider_integer(value: Any, label: str) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        raise ArchiveCollectionAddendumError(f"{label} is not numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise ArchiveCollectionAddendumError(f"{label} is not numeric") from error
    if not math.isfinite(parsed) or parsed < 0:
        raise ArchiveCollectionAddendumError(f"{label} is negative or non-finite")
    return int(parsed)


def _video_originals(
    snapshot: dict[str, Any],
    *,
    only_identifier: str | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in snapshot["items"]:
        identifier = item["identifier"]
        if only_identifier is not None and identifier != only_identifier:
            continue
        payload_path = snapshot["_path"].parent / item["payload_file"]
        _resolved_payload, raw_body, _payload_stat = _stable_regular_file(
            payload_path,
            f"Archive.org payload for {identifier}",
            maximum=item["byte_count"],
            sealed=True,
        )
        if (
            len(raw_body) != item["byte_count"]
            or sha256_bytes(raw_body) != item["payload_sha256"]
        ):
            raise ArchiveCollectionAddendumError(
                f"Archive.org payload evidence changed for {identifier}"
            )
        document = strict_json(raw_body, f"Archive.org payload for {identifier}")
        if not isinstance(document, dict) or not isinstance(document.get("files"), list):
            raise ArchiveCollectionAddendumError(
                f"Archive.org payload files are invalid for {identifier}"
            )
        for ordinal, file_record in enumerate(document["files"]):
            if not isinstance(file_record, dict):
                raise ArchiveCollectionAddendumError(
                    f"Archive.org {identifier} files[{ordinal}] is not an object"
                )
            filename = file_record.get("name")
            if not isinstance(filename, str):
                continue
            if (
                file_record.get("source") != "original"
                or not VIDEO_EXTENSION_RE.search(filename)
            ):
                continue
            if (
                any(part in {".", ".."} for part in filename.split("/"))
                or "\x00" in filename
            ):
                raise ArchiveCollectionAddendumError(
                    f"Archive.org original filename is unsafe: {identifier}/{filename}"
                )
            native_id = f"{identifier}/{filename}"
            rows.append(
                {
                    "archive_identifier": identifier,
                    "filename": filename,
                    "native_id": native_id,
                    "source_id": canonical_source_id(
                        "internet_archive", "archive_media_file", native_id
                    ),
                    "youtube_video_id": terminal_youtube_id(filename),
                    "byte_count": _provider_integer(
                        file_record.get("size"), f"{native_id}.size"
                    ),
                    "sha1": _provider_hash(file_record.get("sha1"), "sha1", native_id),
                    "md5": _provider_hash(file_record.get("md5"), "md5", native_id),
                }
            )
    return sorted(rows, key=lambda row: (row["native_id"], row["source_id"]))


def _load_snapshot(
    path: str | Path,
    expected_sha256: str,
    label: str,
) -> dict[str, Any]:
    resolved, body, _before = _stable_regular_file(
        path, label, maximum=64 * 1024 * 1024, sealed=True
    )
    _verify_digest(body, expected_sha256, label)
    try:
        snapshot = validate_snapshot(resolved)
    except ArchiveMetadataError as error:
        raise ArchiveCollectionAddendumError(f"{label} is invalid: {error}") from error
    if snapshot["_sha256"] != expected_sha256:
        raise ArchiveCollectionAddendumError(f"{label} validator digest differs")
    return snapshot


def _load_plan(
    path: str | Path,
    expected_sha256: str,
) -> tuple[Path, dict[str, Any], int]:
    resolved, body, before = _stable_regular_file(
        path,
        "predecessor queue plan",
        maximum=MAX_PLAN_BYTES,
        sealed=True,
    )
    _verify_digest(body, expected_sha256, "predecessor queue plan")
    raw = strict_json(body, "predecessor queue plan")
    if body != pretty_bytes(raw):
        raise ArchiveCollectionAddendumError(
            "predecessor queue plan is not canonical pretty JSON"
        )
    try:
        plan = validate_plan(raw)
    except MaterializationError as error:
        raise ArchiveCollectionAddendumError(
            f"predecessor queue plan is invalid: {error}"
        ) from error
    if body != pretty_bytes(plan):
        raise ArchiveCollectionAddendumError(
            "predecessor queue plan required normalization during validation"
        )
    return resolved, plan, before.st_size


def _required_columns(
    connection: sqlite3.Connection, table: str, required: set[str]
) -> None:
    table_row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    if table_row is None:
        raise ArchiveCollectionAddendumError(f"successor catalog lacks table {table}")
    columns = {
        row["name"] for row in connection.execute(f'PRAGMA table_info("{table}")')
    }
    missing = sorted(required - columns)
    if missing:
        raise ArchiveCollectionAddendumError(
            f"successor catalog {table} lacks columns {missing}"
        )


def _database_sources(
    database_path: Path,
    collection_identifier: str,
    snapshot_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    sidecars = [
        Path(f"{database_path}{suffix}")
        for suffix in ("-wal", "-shm", "-journal")
        if Path(f"{database_path}{suffix}").exists()
    ]
    if sidecars:
        raise ArchiveCollectionAddendumError(
            "successor catalog must be closed and checkpointed with no SQLite "
            "sidecars; found " + ", ".join(path.name for path in sidecars)
        )
    uri = f"{database_path.as_uri()}?mode=ro&immutable=1"
    try:
        connection = sqlite3.connect(uri, uri=True, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA foreign_keys=ON")
        quick_check = connection.execute("PRAGMA quick_check").fetchall()
        if [tuple(row) for row in quick_check] != [("ok",)]:
            raise ArchiveCollectionAddendumError(
                f"successor catalog quick_check failed: {[tuple(row) for row in quick_check]}"
            )
        foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_keys:
            raise ArchiveCollectionAddendumError(
                "successor catalog foreign_key_check found violations"
            )
        _required_columns(connection, "schema_migrations", {"version"})
        _required_columns(
            connection,
            "sources",
            {
                "source_id",
                "platform",
                "source_kind",
                "native_id",
                "access_state",
                "metadata_json",
            },
        )
        _required_columns(
            connection,
            "recordings",
            {"recording_id", "merged_into_recording_id"},
        )
        _required_columns(
            connection,
            "recording_sources",
            {"recording_id", "source_id"},
        )
        _required_columns(
            connection,
            "source_hashes",
            {"source_id", "algorithm", "digest", "declared_by"},
        )
        row = connection.execute(
            "SELECT max(version) AS version FROM schema_migrations"
        ).fetchone()
        if row is None or not isinstance(row["version"], int) or row["version"] < 1:
            raise ArchiveCollectionAddendumError(
                "successor catalog migration ledger is empty or invalid"
            )
        schema_version = row["version"]
        native_prefix = f"{collection_identifier}/"
        source_rows = connection.execute(
            """
            SELECT s.source_id, s.native_id, s.access_state, s.metadata_json,
                   rs.recording_id, r.merged_into_recording_id
            FROM sources AS s
            JOIN recording_sources AS rs ON rs.source_id = s.source_id
            JOIN recordings AS r ON r.recording_id = rs.recording_id
            WHERE s.platform = 'internet_archive'
              AND s.source_kind = 'archive_media_file'
              AND substr(s.native_id, 1, ?) = ?
              AND json_extract(s.metadata_json, '$.source_class') = 'original'
            ORDER BY s.source_id, rs.recording_id
            """,
            (len(native_prefix), native_prefix),
        ).fetchall()
        hashes = connection.execute(
            """
            SELECT h.source_id, h.algorithm, lower(h.digest) AS digest, h.declared_by
            FROM source_hashes AS h
            JOIN sources AS s ON s.source_id = h.source_id
            WHERE s.platform = 'internet_archive'
              AND s.source_kind = 'archive_media_file'
              AND substr(s.native_id, 1, ?) = ?
              AND json_extract(s.metadata_json, '$.source_class') = 'original'
              AND h.algorithm IN ('sha1', 'md5')
            ORDER BY h.source_id, h.algorithm, h.digest, h.declared_by
            """,
            (len(native_prefix), native_prefix),
        ).fetchall()
    except sqlite3.Error as error:
        raise ArchiveCollectionAddendumError(
            f"cannot audit successor catalog: {error}"
        ) from error
    finally:
        if "connection" in locals():
            connection.close()

    snapshot_by_source = {row["source_id"]: row for row in snapshot_rows}
    mappings: dict[str, set[str]] = defaultdict(set)
    db_rows: dict[str, sqlite3.Row] = {}
    for row in source_rows:
        db_rows[row["source_id"]] = row
        mappings[row["source_id"]].add(row["recording_id"])
    if set(db_rows) != set(snapshot_by_source):
        missing = sorted(set(snapshot_by_source) - set(db_rows))
        unknown = sorted(set(db_rows) - set(snapshot_by_source))
        raise ArchiveCollectionAddendumError(
            "successor catalog public-original source set differs from the new "
            f"snapshot; missing={missing[:10]}, unknown={unknown[:10]}"
        )

    hashes_by_source: dict[str, dict[str, list[tuple[str, str]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in hashes:
        hashes_by_source[row["source_id"]][row["algorithm"]].append(
            (row["digest"], row["declared_by"])
        )

    result: list[dict[str, Any]] = []
    for source in sorted(snapshot_rows, key=lambda row: row["source_id"]):
        source_key = source["source_id"]
        db_row = db_rows[source_key]
        if db_row["native_id"] != source["native_id"]:
            raise ArchiveCollectionAddendumError(
                f"successor catalog native identity differs for {source_key}"
            )
        if db_row["access_state"] != "public":
            raise ArchiveCollectionAddendumError(
                f"new original source is not public in successor catalog: {source_key}"
            )
        recording_ids = mappings[source_key]
        if len(recording_ids) != 1:
            raise ArchiveCollectionAddendumError(
                f"new original source must map to exactly one recording: {source_key}"
            )
        if db_row["merged_into_recording_id"] is not None:
            raise ArchiveCollectionAddendumError(
                f"new original source maps to a merged recording: {source_key}"
            )
        metadata = strict_json(
            db_row["metadata_json"].encode("utf-8"),
            f"successor catalog metadata_json for {source_key}",
        )
        if not isinstance(metadata, dict):
            raise ArchiveCollectionAddendumError(
                f"successor catalog metadata_json is not an object for {source_key}"
            )
        expected_metadata = {
            "internet_archive_item": collection_identifier,
            "filename": source["filename"],
            "source_class": "original",
            "byte_count": source["byte_count"],
        }
        for key, expected_value in expected_metadata.items():
            if metadata.get(key) != expected_value:
                raise ArchiveCollectionAddendumError(
                    f"successor catalog metadata {key} differs for {source_key}"
                )
        for algorithm in ("sha1", "md5"):
            expected_digest = source[algorithm]
            rows = hashes_by_source[source_key].get(algorithm, [])
            declared_digests = {digest for digest, _declared_by in rows}
            if expected_digest is None:
                if declared_digests:
                    raise ArchiveCollectionAddendumError(
                        f"catalog has {algorithm} absent from provider snapshot for {source_key}"
                    )
                continue
            if declared_digests != {expected_digest}:
                raise ArchiveCollectionAddendumError(
                    f"catalog/provider {algorithm} evidence differs for {source_key}"
                )
            if not any(
                digest == expected_digest and declared_by == "internet_archive_metadata"
                for digest, declared_by in rows
            ):
                raise ArchiveCollectionAddendumError(
                    f"catalog {algorithm} lacks Archive.org declaration provenance for {source_key}"
                )
        result.append({**source, "recording_id": next(iter(recording_ids))})
    return sorted(result, key=lambda row: (row["native_id"], row["source_id"])), schema_version


class _UnionFind:
    def __init__(self, keys: Iterable[str]):
        self.parent = {key: key for key in keys}

    def find(self, key: str) -> str:
        parent = self.parent[key]
        if parent != key:
            self.parent[key] = self.find(parent)
        return self.parent[key]

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        winner, loser = sorted((left_root, right_root))
        self.parent[loser] = winner


def _logical_groups(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    union = _UnionFind(row["source_id"] for row in rows)
    indexes: dict[tuple[str, str], str] = {}
    for row in rows:
        signals = [("recording_id", row["recording_id"])]
        if row["youtube_video_id"] is not None:
            signals.append(("youtube_video_id", row["youtube_video_id"]))
        for algorithm in ("sha1", "md5"):
            if row[algorithm] is not None:
                signals.append((algorithm, row[algorithm]))
        for signal in signals:
            previous = indexes.setdefault(signal, row["source_id"])
            union.union(previous, row["source_id"])
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[union.find(row["source_id"])].append(row)
    normalized = [
        sorted(group, key=lambda row: (row["native_id"], row["source_id"]))
        for group in groups.values()
    ]
    return sorted(normalized, key=lambda group: tuple(row["source_id"] for row in group))


def _public_source_evidence(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_id": row["source_id"],
        "recording_id": row["recording_id"],
        "native_id": row["native_id"],
        "filename": row["filename"],
        "youtube_video_id": row["youtube_video_id"],
        "byte_count": row["byte_count"],
        "provider_sha1": row["sha1"],
        "provider_md5": row["md5"],
    }


def _group_identity(group: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "recording_ids": sorted({row["recording_id"] for row in group}),
        "youtube_video_ids": sorted(
            {row["youtube_video_id"] for row in group if row["youtube_video_id"]}
        ),
        "provider_sha1": sorted({row["sha1"] for row in group if row["sha1"]}),
        "provider_md5": sorted({row["md5"] for row in group if row["md5"]}),
        "source_ids": sorted(row["source_id"] for row in group),
    }


def _new_grouping_methods(group: list[dict[str, Any]]) -> list[str]:
    methods: list[str] = []
    signals = (
        ("exact_recording_id", "recording_id"),
        ("exact_terminal_youtube_id", "youtube_video_id"),
        ("exact_provider_sha1", "sha1"),
        ("exact_provider_md5", "md5"),
    )
    for method, field in signals:
        counts: dict[str, int] = defaultdict(int)
        for row in group:
            value = row[field]
            if value is not None:
                counts[value] += 1
        if any(count > 1 for count in counts.values()):
            methods.append(method)
    return sorted(methods)


def _chosen_source(group: list[dict[str, Any]]) -> dict[str, Any]:
    return min(
        group,
        key=lambda row: (
            row["byte_count"] if row["byte_count"] is not None else 2**63,
            row["native_id"],
            row["source_id"],
        ),
    )


def build_documents(
    *,
    database_path: Path,
    database_sha256: str,
    database_byte_count: int,
    schema_version: int,
    new_snapshot: dict[str, Any],
    predecessor_plan_path: Path,
    predecessor_plan_sha256: str,
    predecessor_plan_byte_count: int,
    predecessor_plan: dict[str, Any],
    predecessor_snapshot: dict[str, Any],
    collection_identifier: str,
    purpose: str,
    new_rows: list[dict[str, Any]],
    predecessor_rows: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    plan_recordings: dict[str, list[dict[str, Any]]] = defaultdict(list)
    plan_video_ids: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in predecessor_plan["candidates"]:
        evidence = {
            "recording_id": candidate["recording_id"],
            "source_id": candidate["source_id"],
            "platform": candidate["platform"],
            "native_id": candidate["native_id"],
        }
        plan_recordings[candidate["recording_id"]].append(evidence)
        video_id = _candidate_youtube_id(candidate)
        if video_id is not None:
            plan_video_ids[video_id].append(evidence)

    snapshot_video_ids: dict[str, list[dict[str, Any]]] = defaultdict(list)
    snapshot_hashes: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in predecessor_rows:
        evidence = {
            "archive_identifier": row["archive_identifier"],
            "source_id": row["source_id"],
            "native_id": row["native_id"],
            "filename": row["filename"],
            "youtube_video_id": row["youtube_video_id"],
            "provider_sha1": row["sha1"],
            "provider_md5": row["md5"],
        }
        if row["youtube_video_id"] is not None:
            snapshot_video_ids[row["youtube_video_id"]].append(evidence)
        for algorithm in ("sha1", "md5"):
            if row[algorithm] is not None:
                snapshot_hashes[(algorithm, row[algorithm])].append(evidence)

    overlaps: list[dict[str, Any]] = []
    selected_groups: list[dict[str, Any]] = []
    selected_source_ids: list[str] = []
    method_counts = {
        "exact_recording_id": 0,
        "exact_terminal_youtube_id": 0,
        "exact_provider_sha1": 0,
        "exact_provider_md5": 0,
    }
    collapsed_alternates = 0
    multi_source_groups = 0
    for group in _logical_groups(new_rows):
        identity = _group_identity(group)
        group_id = stable_id("arcgrp", identity)
        grouping_methods = _new_grouping_methods(group)
        if len(group) > 1:
            multi_source_groups += 1
        methods: set[str] = set()
        plan_evidence: dict[tuple[str, str], dict[str, Any]] = {}
        snapshot_evidence: dict[tuple[str, str], dict[str, Any]] = {}
        for recording_id in identity["recording_ids"]:
            for evidence in plan_recordings.get(recording_id, []):
                methods.add("exact_recording_id")
                plan_evidence[(evidence["recording_id"], evidence["source_id"])] = evidence
        for video_id in identity["youtube_video_ids"]:
            for evidence in plan_video_ids.get(video_id, []):
                methods.add("exact_terminal_youtube_id")
                plan_evidence[(evidence["recording_id"], evidence["source_id"])] = evidence
            for evidence in snapshot_video_ids.get(video_id, []):
                methods.add("exact_terminal_youtube_id")
                snapshot_evidence[(evidence["archive_identifier"], evidence["native_id"])] = evidence
        for algorithm, report_method in (
            ("sha1", "exact_provider_sha1"),
            ("md5", "exact_provider_md5"),
        ):
            for digest in identity[f"provider_{algorithm}"]:
                for evidence in snapshot_hashes.get((algorithm, digest), []):
                    methods.add(report_method)
                    snapshot_evidence[(evidence["archive_identifier"], evidence["native_id"])] = evidence

        sources = [_public_source_evidence(row) for row in group]
        if methods:
            ordered_methods = sorted(methods)
            for method in ordered_methods:
                method_counts[method] += 1
            overlaps.append(
                {
                    "logical_group_id": group_id,
                    "new_grouping_methods": grouping_methods,
                    "new_identity": identity,
                    "new_sources": sources,
                    "match_methods": ordered_methods,
                    "predecessor_evidence": {
                        "queue_plan_candidates": sorted(
                            plan_evidence.values(),
                            key=lambda row: (
                                row["recording_id"], row["source_id"], row["native_id"]
                            ),
                        ),
                        "snapshot_originals": sorted(
                            snapshot_evidence.values(),
                            key=lambda row: (
                                row["archive_identifier"], row["native_id"]
                            ),
                        ),
                    },
                }
            )
            continue

        chosen = _chosen_source(group)
        alternates = sorted(
            row["source_id"] for row in group if row["source_id"] != chosen["source_id"]
        )
        collapsed_alternates += len(alternates)
        selected_source_ids.append(chosen["source_id"])
        selected_groups.append(
            {
                "logical_group_id": group_id,
                "new_grouping_methods": grouping_methods,
                "identity": identity,
                "selected_source": _public_source_evidence(chosen),
                "sources": sources,
                "alternate_source_ids": alternates,
                "selection_rule": "smallest_provider_byte_count_then_native_id_then_source_id",
            }
        )

    selection = {
        "schema_version": 1,
        "purpose": purpose,
        "youtube_video_ids": [],
        "source_ids": sorted(selected_source_ids),
        "recording_ids": [],
    }
    try:
        normalized_selection = validate_selection_manifest(selection)
    except PlanningError as error:
        raise ArchiveCollectionAddendumError(
            f"generated selection manifest is invalid: {error}"
        ) from error
    if normalized_selection != selection:
        raise ArchiveCollectionAddendumError(
            "generated selection manifest is not canonical"
        )
    selection_body = pretty_bytes(selection)

    groups_count = len(overlaps) + len(selected_groups)
    report_body = {
        "schema_version": SCHEMA_VERSION,
        "report_kind": "archive_collection_addendum_overlap_report",
        "implementation_version": IMPLEMENTATION_VERSION,
        "collection_identifier": collection_identifier,
        "purpose": purpose,
        "inputs": {
            "successor_catalog": {
                "path": str(database_path),
                "sha256": database_sha256,
                "byte_count": database_byte_count,
                "schema_version": schema_version,
            },
            "new_archive_snapshot": {
                "path": str(new_snapshot["_path"]),
                "snapshot_id": new_snapshot["snapshot_id"],
                "sha256": new_snapshot["_sha256"],
            },
            "predecessor_queue_plan": {
                "path": str(predecessor_plan_path),
                "plan_id": predecessor_plan["plan_id"],
                "sha256": predecessor_plan_sha256,
                "byte_count": predecessor_plan_byte_count,
            },
            "predecessor_archive_snapshot": {
                "path": str(predecessor_snapshot["_path"]),
                "snapshot_id": predecessor_snapshot["snapshot_id"],
                "sha256": predecessor_snapshot["_sha256"],
            },
        },
        "equivalence_policy": {
            "allowed_exact_signals": [
                "recording_id_in_predecessor_queue_plan",
                "terminal_youtube_id_in_dash_or_bracket_filename",
                "provider_sha1_for_original_file",
                "provider_md5_for_original_file",
            ],
            "fuzzy_title_matching": False,
            "titles_examined": False,
            "provider_files_must_be_original": True,
            "catalog_sources_must_be_public": True,
            "publication_authority": False,
        },
        "counts": {
            "new_snapshot_original_video_sources": len(new_rows),
            "new_catalog_public_original_video_sources": len(new_rows),
            "new_logical_recordings": groups_count,
            "covered_logical_recordings": len(overlaps),
            "selected_logical_recordings": len(selected_groups),
            "selected_source_ids": len(selection["source_ids"]),
            "collapsed_unselected_alternate_sources": collapsed_alternates,
            "new_logical_groups_with_multiple_sources": multi_source_groups,
            "new_sources_collapsed_into_logical_groups": len(new_rows) - groups_count,
            "predecessor_queue_candidates": len(predecessor_plan["candidates"]),
            "predecessor_snapshot_original_video_sources": len(predecessor_rows),
            "overlap_logical_recordings_by_method": method_counts,
        },
        "selection_manifest": {
            "sha256": sha256_bytes(selection_body),
            "byte_count": len(selection_body),
            "source_ids": selection["source_ids"],
        },
        "overlaps": sorted(overlaps, key=lambda row: row["logical_group_id"]),
        "selected_groups": sorted(
            selected_groups, key=lambda row: row["logical_group_id"]
        ),
        "assertion_policy": {
            "state": "private_deterministic_acquisition_planning",
            "provider_metadata_is_content_truth": False,
            "network_access_performed": False,
            "catalog_mutated": False,
            "media_acquired": False,
            "publication_authority": False,
        },
    }
    report = {
        "report_id": stable_id("arcaor", report_body),
        **report_body,
    }
    if len(pretty_bytes(report)) > MAX_REPORT_BYTES:
        raise ArchiveCollectionAddendumError(
            f"generated overlap report exceeds {MAX_REPORT_BYTES} bytes"
        )
    return selection, report


def _write_readonly_exact(path: Path, body: bytes, label: str) -> bool:
    path = _normalized_absolute(path, label, existing=False)
    if not path.parent.is_dir():
        raise ArchiveCollectionAddendumError(f"{label} parent directory does not exist")
    if path.parent.resolve(strict=True) != path.parent:
        raise ArchiveCollectionAddendumError(
            f"{label} parent must contain no symlink components"
        )
    if path.exists() or path.is_symlink():
        existing_path, existing, before = _stable_regular_file(
            path, label, maximum=MAX_REPORT_BYTES, sealed=True
        )
        if existing_path != path or existing != body:
            raise ArchiveCollectionAddendumError(
                f"refusing to overwrite non-identical existing {label}"
            )
        if stat.S_IMODE(before.st_mode) != 0o400:
            raise ArchiveCollectionAddendumError(
                f"existing {label} must have exact mode 0400"
            )
        return True

    temporary = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    descriptor: int | None = None
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(temporary, flags, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            descriptor = None
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o400, follow_symlinks=False)
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError as error:
            raise ArchiveCollectionAddendumError(
                f"{label} appeared concurrently; refusing to overwrite"
            ) from error
        os.unlink(temporary)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        if descriptor is not None:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise
    return False


def build_addendum(
    *,
    database: str | Path,
    expected_database_sha256: str,
    new_snapshot_path: str | Path,
    expected_new_snapshot_sha256: str,
    predecessor_plan_path: str | Path,
    expected_predecessor_plan_sha256: str,
    predecessor_snapshot_path: str | Path,
    expected_predecessor_snapshot_sha256: str,
    collection_identifier: str,
    purpose: str,
    selection_output: str | Path,
    report_output: str | Path,
) -> dict[str, Any]:
    if not IDENTIFIER_RE.fullmatch(collection_identifier) or collection_identifier in {
        ".",
        "..",
    }:
        raise ArchiveCollectionAddendumError(
            "collection identifier is not a safe Archive.org identifier"
        )
    if not isinstance(purpose, str) or not (1 <= len(purpose) <= 500):
        raise ArchiveCollectionAddendumError("purpose must be 1..500 characters")
    selection_path = _normalized_absolute(
        selection_output, "selection output", existing=False
    )
    report_path = _normalized_absolute(report_output, "report output", existing=False)
    if selection_path == report_path:
        raise ArchiveCollectionAddendumError(
            "selection and report outputs must be different paths"
        )

    database_path = _normalized_absolute(database, "successor catalog", existing=True)
    database_stat = database_path.lstat()
    if database_path.is_symlink() or not stat.S_ISREG(database_stat.st_mode):
        raise ArchiveCollectionAddendumError(
            "successor catalog must be a regular non-symlink file"
        )
    database_sha256, database_byte_count = _hash_file_stably(
        database_path, "successor catalog", expected_database_sha256
    )
    new_snapshot = _load_snapshot(
        new_snapshot_path,
        expected_new_snapshot_sha256,
        "new Archive.org snapshot",
    )
    matching_items = [
        item
        for item in new_snapshot["items"]
        if item["identifier"] == collection_identifier
    ]
    if len(new_snapshot["items"]) != 1 or len(matching_items) != 1:
        raise ArchiveCollectionAddendumError(
            "new Archive.org snapshot must contain exactly the requested collection"
        )
    predecessor_plan_resolved, predecessor_plan, predecessor_plan_bytes = _load_plan(
        predecessor_plan_path, expected_predecessor_plan_sha256
    )
    predecessor_snapshot = _load_snapshot(
        predecessor_snapshot_path,
        expected_predecessor_snapshot_sha256,
        "predecessor Archive.org snapshot",
    )

    new_snapshot_rows = _video_originals(
        new_snapshot, only_identifier=collection_identifier
    )
    if not new_snapshot_rows:
        raise ArchiveCollectionAddendumError(
            "new Archive.org collection contains no original video files"
        )
    predecessor_rows = _video_originals(
        predecessor_snapshot, only_identifier=None
    )
    new_rows, schema_version = _database_sources(
        database_path, collection_identifier, new_snapshot_rows
    )
    selection, report = build_documents(
        database_path=database_path,
        database_sha256=database_sha256,
        database_byte_count=database_byte_count,
        schema_version=schema_version,
        new_snapshot=new_snapshot,
        predecessor_plan_path=predecessor_plan_resolved,
        predecessor_plan_sha256=expected_predecessor_plan_sha256,
        predecessor_plan_byte_count=predecessor_plan_bytes,
        predecessor_plan=predecessor_plan,
        predecessor_snapshot=predecessor_snapshot,
        collection_identifier=collection_identifier,
        purpose=purpose,
        new_rows=new_rows,
        predecessor_rows=predecessor_rows,
    )

    # Rehash mutable catalog input after all reads.  Sealed JSON inputs were read and
    # independently revalidated by their native strict validators.
    _hash_file_stably(database_path, "successor catalog", expected_database_sha256)
    selection_body = pretty_bytes(selection)
    report_body = pretty_bytes(report)
    selection_reused = _write_readonly_exact(
        selection_path, selection_body, "selection output"
    )
    report_reused = _write_readonly_exact(report_path, report_body, "report output")
    return {
        "schema_version": 1,
        "operation": "build_archive_collection_addendum",
        "collection_identifier": collection_identifier,
        "selection": {
            "path": str(selection_path),
            "sha256": sha256_bytes(selection_body),
            "byte_count": len(selection_body),
            "source_id_count": len(selection["source_ids"]),
            "reused": selection_reused,
        },
        "overlap_report": {
            "path": str(report_path),
            "report_id": report["report_id"],
            "sha256": sha256_bytes(report_body),
            "byte_count": len(report_body),
            "covered_logical_recordings": report["counts"][
                "covered_logical_recordings"
            ],
            "exact_terminal_youtube_id_overlaps": report["counts"][
                "overlap_logical_recordings_by_method"
            ]["exact_terminal_youtube_id"],
            "reused": report_reused,
        },
        "safety": {
            "network_access_performed": False,
            "catalog_mutated": False,
            "media_acquired": False,
            "publication_authority": False,
        },
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--expected-database-sha256", required=True)
    parser.add_argument("--new-snapshot", required=True)
    parser.add_argument("--expected-new-snapshot-sha256", required=True)
    parser.add_argument("--predecessor-plan", required=True)
    parser.add_argument("--expected-predecessor-plan-sha256", required=True)
    parser.add_argument("--predecessor-snapshot", required=True)
    parser.add_argument("--expected-predecessor-snapshot-sha256", required=True)
    parser.add_argument("--collection-identifier", required=True)
    parser.add_argument("--purpose", required=True)
    parser.add_argument("--selection-output", required=True)
    parser.add_argument("--report-output", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        result = build_addendum(
            database=arguments.database,
            expected_database_sha256=arguments.expected_database_sha256,
            new_snapshot_path=arguments.new_snapshot,
            expected_new_snapshot_sha256=arguments.expected_new_snapshot_sha256,
            predecessor_plan_path=arguments.predecessor_plan,
            expected_predecessor_plan_sha256=arguments.expected_predecessor_plan_sha256,
            predecessor_snapshot_path=arguments.predecessor_snapshot,
            expected_predecessor_snapshot_sha256=arguments.expected_predecessor_snapshot_sha256,
            collection_identifier=arguments.collection_identifier,
            purpose=arguments.purpose,
            selection_output=arguments.selection_output,
            report_output=arguments.report_output,
        )
    except ArchiveCollectionAddendumError as error:
        print(f"archive collection addendum failed: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
