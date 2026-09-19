"""Private candidate reconciliation for the retained 1q2sk8g Archive URL list.

This lane is deliberately fixed to one historical evidence set.  It replays the
retained Catbox complement bytes, their normalization record, the original catalog
hint import, and the later sealed Archive.org metadata snapshot.  An exact URL and
Archive ``native_id`` join is still only a provenance-alignment candidate: this
module never merges/deletes sources, copies external IDs, creates source relations,
changes recording mappings, or grants publication authority.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlsplit

from .archive_bracket_reconciler import _require_strict_snapshot_import
from .archive_metadata_snapshot_importer import (
    ArchiveMetadataSnapshotImportError,
    MAX_PAYLOAD_BYTES,
    _stable_read,
    _strict_json_loads,
    validate_archive_metadata_snapshot,
)
from .db import transaction
from .ids import source_id, stable_id
from .importers import (
    _begin_batch,
    _complete_batch,
    byte_count,
    canonical_json,
    duration_ms,
    sha256_bytes,
    title_from_media_filename,
)


IMPORTER_NAME = "archive_hint_provider_reconciliation_v1"
PLAN_KIND = "archive_hint_provider_source_reconciliation_review_plan"
PLANNER_VERSION = "archive_hint_provider_reconciliation_plan_v1"
MATCH_METHOD = "archive_hint_exact_native_id_candidate_v1"
TASK_KIND = "archive_hint_provider_source_reconciliation_candidate"
TASK_REASON = (
    "Review whether this retained URL-only Archive hint should later be superseded "
    "by the same-native provider source. Exact URL/native-ID alignment is provenance "
    "evidence only, not downloaded-byte identity, a source merge, or publication authority."
)
TASK_PRIORITY = 65

REDDIT_POST_ID = "1q2sk8g"
CATBOX_RETRIEVAL_URL = "https://files.catbox.moe/f5yr6k.txt"
ORIGINAL_HINTS_SHA256 = (
    "80fb016eccfd04dfbbf4df6d27f9ac54c5f80bd8e86ef37b0670c7095e04e587"
)
NORMALIZED_HINTS_SHA256 = (
    "0006f09a7c4e024c2f155e803f22329ca9d94d25e5c8cc105be1a8e9ea754ab7"
)
DISCOVERY_SHA256 = (
    "35bc77da77e4e7249217bdb1d44d7fd55122b99b664a8b59c35c60f82ca9155a"
)
ARCHIVE_SNAPSHOT_ID = "iams_4f920fcb565f826b8352b450247d18d3"
ARCHIVE_SNAPSHOT_SHA256 = (
    "45b4aab1690a2c7610844904e1f56f2279f194b420578343beae6d16966874ba"
)
ARCHIVE_REQUEST_ID = "iamr_e84fa2fd7fbd7bada30691e34329ea51"

EXPECTED_RETAINED_URLS = 731
EXPECTED_ALREADY_PROVIDER = 510
EXPECTED_LATE_HINTS = 221
EXPECTED_URLS_BY_ITEM = {
    "699992": 510,
    "hidinginmyroom": 79,
    "hidinginmyroom2": 52,
    "hidinginmyroom3": 90,
}
LATE_ARCHIVE_ITEMS = ("hidinginmyroom", "hidinginmyroom2", "hidinginmyroom3")

MAX_EVIDENCE_BYTES = 2 * 1024 * 1024
MAX_URLS = 1_000
MAX_URL_LENGTH = 8_192
MAX_NATIVE_ID_LENGTH = 8_192
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
HASH_PATTERNS = {
    "crc32": re.compile(r"^[0-9a-f]{8}$"),
    "md5": re.compile(r"^[0-9a-f]{32}$"),
    "sha1": re.compile(r"^[0-9a-f]{40}$"),
}

ALLOWED_WRITE_TABLES = frozenset(
    {
        "import_batches",
        "import_observations",
        "match_candidates",
        "review_tasks",
        "archive_hint_reconciliation_imports",
        "archive_hint_provider_candidates",
    }
)


class ArchiveHintReconciliationError(ValueError):
    """Raised when the fixed evidence or its catalog projection is not exact."""


def _expect_keys(value: object, expected: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ArchiveHintReconciliationError(f"{label} must be an object")
    missing = sorted(expected - set(value))
    extra = sorted(set(value) - expected)
    if missing or extra:
        raise ArchiveHintReconciliationError(
            f"{label} has unknown shape (missing={missing}, extra={extra})"
        )
    return value


def _stable_file(path: Path, label: str) -> tuple[Path, bytes]:
    """Read one bounded regular file while pinning its caller-named final entry."""

    requested = Path(os.path.abspath(os.fspath(Path(path))))
    try:
        lexical = requested.lstat()
    except OSError as error:
        raise ArchiveHintReconciliationError(f"{label} cannot be inspected") from error
    if stat.S_ISLNK(lexical.st_mode) or not stat.S_ISREG(lexical.st_mode):
        raise ArchiveHintReconciliationError(f"{label} must be a non-symlink regular file")
    if lexical.st_size < 1 or lexical.st_size > MAX_EVIDENCE_BYTES:
        raise ArchiveHintReconciliationError(f"{label} exceeds its byte limit")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(requested, flags)
    except OSError as error:
        raise ArchiveHintReconciliationError(f"{label} cannot be opened safely") from error
    try:
        opened = os.fstat(descriptor)
        chunks: list[bytes] = []
        offset = 0
        while offset < opened.st_size:
            chunk = os.pread(
                descriptor, min(1024 * 1024, opened.st_size - offset), offset
            )
            if not chunk:
                raise ArchiveHintReconciliationError(f"{label} ended during stable read")
            chunks.append(chunk)
            offset += len(chunk)
        closed = os.fstat(descriptor)
        try:
            final = requested.lstat()
        except OSError as error:
            raise ArchiveHintReconciliationError(
                f"{label} changed during stable read"
            ) from error

        def identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
            return (
                value.st_dev,
                value.st_ino,
                value.st_size,
                value.st_mtime_ns,
                value.st_ctime_ns,
            )

        if (
            stat.S_ISLNK(final.st_mode)
            or identity(lexical) != identity(opened)
            or identity(opened) != identity(closed)
            or identity(closed) != identity(final)
        ):
            raise ArchiveHintReconciliationError(f"{label} changed during stable read")
        return requested, b"".join(chunks)
    finally:
        os.close(descriptor)


def _strict_json(body: bytes, label: str) -> dict[str, Any]:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise ArchiveHintReconciliationError(
                    f"{label} contains duplicate JSON key {key!r}"
                )
            result[key] = value
        return result

    def integer(token: str) -> int:
        if len(token.lstrip("-")) > 19:
            raise ArchiveHintReconciliationError(f"{label} contains an oversized integer")
        return int(token)

    def floating(token: str) -> float:
        value = float(token)
        if value != value or value in (float("inf"), float("-inf")):
            raise ArchiveHintReconciliationError(f"{label} contains invalid number {token}")
        return value

    def invalid_number(token: str) -> None:
        raise ArchiveHintReconciliationError(f"{label} contains invalid number {token}")

    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_int=integer,
            parse_float=floating,
            parse_constant=invalid_number,
        )
    except ArchiveHintReconciliationError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as error:
        raise ArchiveHintReconciliationError(f"{label} is not strict UTF-8 JSON") from error
    if not isinstance(value, dict):
        raise ArchiveHintReconciliationError(f"{label} must contain one object")
    return value


def _require_digest(body: bytes, expected: str, label: str) -> str:
    digest = sha256_bytes(body)
    if digest != expected:
        raise ArchiveHintReconciliationError(
            f"{label} is outside the fixed 1q2sk8g evidence boundary"
        )
    return digest


def _archive_url(value: object) -> tuple[str, str, str]:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= MAX_URL_LENGTH
        or any(character.isspace() or ord(character) < 0x20 for character in value)
    ):
        raise ArchiveHintReconciliationError("retained Archive URL is invalid")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as error:
        raise ArchiveHintReconciliationError("retained Archive URL is invalid") from error
    raw_parts = parsed.path.split("/")
    if (
        parsed.scheme != "https"
        or parsed.hostname != "archive.org"
        or port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or len(raw_parts) != 4
        or raw_parts[:2] != ["", "download"]
        or not raw_parts[2]
        or not raw_parts[3]
    ):
        raise ArchiveHintReconciliationError("retained URL leaves the exact Archive download grammar")
    try:
        item = unquote(raw_parts[2], encoding="utf-8", errors="strict")
        filename = unquote(raw_parts[3], encoding="utf-8", errors="strict")
    except (UnicodeDecodeError, ValueError) as error:
        raise ArchiveHintReconciliationError("retained Archive URL is not canonical UTF-8") from error
    if (
        not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", item)
        or item in {".", ".."}
        or not filename
        or len(filename) > 4096
        or "\x00" in filename
        or "/" in filename
        or "\\" in filename
    ):
        raise ArchiveHintReconciliationError("retained Archive URL has an unsafe native ID")
    canonical_path = f"/download/{quote(item, safe='')}/{quote(filename, safe='')}"
    if parsed.path != canonical_path:
        raise ArchiveHintReconciliationError("retained Archive URL is not canonically escaped")
    native_id = f"{item}/{filename}"
    if len(native_id) > MAX_NATIVE_ID_LENGTH:
        raise ArchiveHintReconciliationError("retained Archive native ID is oversized")
    return item, filename, native_id


def _validate_discovery(value: dict[str, Any]) -> dict[str, Any]:
    value = _expect_keys(
        value,
        {
            "schema_version",
            "discovered_at",
            "discovery_source",
            "archive_item",
            "complement_list",
            "interpretation_warning",
        },
        "Archive hint discovery metadata",
    )
    source = _expect_keys(
        value["discovery_source"],
        {"platform", "subreddit", "post_id", "url", "title", "published_at"},
        "discovery_source",
    )
    complement = _expect_keys(
        value["complement_list"],
        {
            "retrieval_url",
            "retained_original_path",
            "retained_original_sha256",
            "encoding",
            "normalized_utf8_path",
            "normalized_utf8_sha256",
            "nonempty_url_count",
            "distinct_url_count",
        },
        "complement_list",
    )
    _expect_keys(
        value["archive_item"],
        {
            "identifier",
            "url",
            "title",
            "metadata_path",
            "metadata_sha256",
            "file_count",
            "video_extension_file_count",
            "video_original_count",
            "video_declared_bytes",
            "video_declared_seconds",
        },
        "archive_item",
    )
    if (
        value["schema_version"] != 1
        or source["platform"] != "reddit"
        or source["subreddit"] != "HIMRFAM"
        or source["post_id"] != REDDIT_POST_ID
        or f"/comments/{REDDIT_POST_ID}/" not in source["url"]
        or complement["retrieval_url"] != CATBOX_RETRIEVAL_URL
        or complement["retained_original_sha256"] != ORIGINAL_HINTS_SHA256
        or complement["normalized_utf8_sha256"] != NORMALIZED_HINTS_SHA256
        or complement["encoding"] != "UTF-16LE with BOM"
        or complement["nonempty_url_count"] != EXPECTED_RETAINED_URLS
        or complement["distinct_url_count"] != EXPECTED_RETAINED_URLS
        or not isinstance(value["interpretation_warning"], str)
        or not value["interpretation_warning"]
    ):
        raise ArchiveHintReconciliationError("discovery metadata differs from the fixed complement")
    if value["discovered_at"] != "2026-08-26T18:29:44Z":
        raise ArchiveHintReconciliationError("discovery observation time differs")
    return value


def _retained_evidence(
    original_path: Path, normalized_path: Path, discovery_path: Path
) -> dict[str, Any]:
    original_file, original = _stable_file(original_path, "original Catbox complement")
    normalized_file, normalized = _stable_file(
        normalized_path, "normalized Catbox complement"
    )
    discovery_file, discovery_body = _stable_file(
        discovery_path, "Archive hint discovery metadata"
    )
    _require_digest(original, ORIGINAL_HINTS_SHA256, "original Catbox complement")
    _require_digest(normalized, NORMALIZED_HINTS_SHA256, "normalized Catbox complement")
    _require_digest(discovery_body, DISCOVERY_SHA256, "Archive hint discovery metadata")
    if not original.startswith(b"\xff\xfe"):
        raise ArchiveHintReconciliationError("original complement is not UTF-16LE with BOM")
    try:
        decoded = original.decode("utf-16")
    except UnicodeDecodeError as error:
        raise ArchiveHintReconciliationError("original complement is not strict UTF-16") from error
    reproduced = ("\n".join(decoded.splitlines()) + "\n").encode("utf-8")
    if reproduced != normalized:
        raise ArchiveHintReconciliationError("normalized complement does not reproduce the original")
    try:
        text = normalized.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ArchiveHintReconciliationError("normalized complement is not UTF-8") from error
    raw_lines = text.splitlines()
    if (
        not normalized.endswith(b"\n")
        or len(raw_lines) != EXPECTED_RETAINED_URLS
        or any(not line for line in raw_lines)
        or len(raw_lines) > MAX_URLS
        or len(set(raw_lines)) != len(raw_lines)
    ):
        raise ArchiveHintReconciliationError("retained complement URL accounting differs")
    urls: list[dict[str, str]] = []
    item_counts: Counter[str] = Counter()
    native_ids: set[str] = set()
    for raw_url in raw_lines:
        item, filename, native_id = _archive_url(raw_url)
        if native_id in native_ids:
            raise ArchiveHintReconciliationError("retained complement has duplicate native IDs")
        native_ids.add(native_id)
        item_counts[item] += 1
        urls.append(
            {"archive_url": raw_url, "item": item, "filename": filename, "native_id": native_id}
        )
    if dict(sorted(item_counts.items())) != dict(sorted(EXPECTED_URLS_BY_ITEM.items())):
        raise ArchiveHintReconciliationError("retained complement item distribution differs")
    discovery = _validate_discovery(_strict_json(discovery_body, "discovery metadata"))
    return {
        "original_path": original_file,
        "normalized_path": normalized_file,
        "discovery_path": discovery_file,
        "original_body": original,
        "normalized_body": normalized,
        "discovery_body": discovery_body,
        "discovery": discovery,
        "urls": urls,
    }


def _archive_snapshot_evidence(snapshot_path: Path) -> dict[str, Any]:
    try:
        snapshot = validate_archive_metadata_snapshot(Path(snapshot_path))
    except ArchiveMetadataSnapshotImportError as error:
        raise ArchiveHintReconciliationError(str(error)) from error
    if (
        snapshot["snapshot_id"] != ARCHIVE_SNAPSHOT_ID
        or snapshot["_sha256"] != ARCHIVE_SNAPSHOT_SHA256
        or snapshot["request"]["request_id"] != ARCHIVE_REQUEST_ID
        or tuple(item["identifier"] for item in snapshot["items"])
        != (
            "28766",
            "69999",
            "699992",
            "699994",
            "hidinginmyroom",
            "hidinginmyroom2",
            "hidinginmyroom3",
        )
    ):
        raise ArchiveHintReconciliationError("Archive snapshot leaves the fixed seven-item audit")
    records: dict[str, dict[str, Any]] = {}
    item_evidence: dict[str, dict[str, Any]] = {}
    for item in snapshot["items"]:
        identifier = item["identifier"]
        if identifier not in LATE_ARCHIVE_ITEMS:
            continue
        payload = _stable_read(
            item["_payload_path"],
            min(item["byte_count"], MAX_PAYLOAD_BYTES),
            f"Archive snapshot payload {identifier}",
            sealed=True,
        )
        document = _strict_json_loads(payload, f"Archive snapshot payload {identifier}")
        files = document.get("files") if isinstance(document, dict) else None
        if not isinstance(files, list):
            raise ArchiveHintReconciliationError("Archive snapshot payload changed shape")
        for record in files:
            if not isinstance(record, dict) or not isinstance(record.get("name"), str):
                raise ArchiveHintReconciliationError("Archive snapshot file record changed shape")
            native_id = f"{identifier}/{record['name']}"
            if native_id in records:
                raise ArchiveHintReconciliationError("Archive snapshot has duplicate native IDs")
            records[native_id] = record
        item_source = source_id("internet_archive", "archive_item", identifier)
        item_evidence[identifier] = {
            "archive_item_source_id": item_source,
            "capture_snapshot_id": stable_id(
                "ssn", item_source, item["observed_at"], item["payload_sha256"]
            ),
            "payload_sha256": item["payload_sha256"],
            "payload_filename": item["_payload_path"].name,
            "observed_at": item["observed_at"],
        }
    return {"snapshot": snapshot, "records": records, "items": item_evidence}


def _one(connection: sqlite3.Connection, query: str, parameters: tuple[Any, ...], label: str) -> sqlite3.Row:
    rows = connection.execute(query, parameters).fetchall()
    if len(rows) != 1:
        raise ArchiveHintReconciliationError(f"{label} is not exactly one row")
    return rows[0]


def _json_object(value: str, label: str) -> dict[str, Any]:
    try:
        decoded = json.loads(value)
    except (TypeError, json.JSONDecodeError) as error:
        raise ArchiveHintReconciliationError(f"{label} is invalid JSON") from error
    if not isinstance(decoded, dict):
        raise ArchiveHintReconciliationError(f"{label} is not a JSON object")
    return decoded


def _archive_catalog_binding(
    connection: sqlite3.Connection, archive: dict[str, Any]
) -> dict[str, Any]:
    snapshot = archive["snapshot"]
    try:
        _require_strict_snapshot_import(connection, snapshot)
    except Exception as error:
        raise ArchiveHintReconciliationError(
            "sealed Archive snapshot must be admitted before reconciliation"
        ) from error
    batches: set[str] = set()
    capture_rows: list[dict[str, Any]] = []
    for identifier in LATE_ARCHIVE_ITEMS:
        evidence = archive["items"][identifier]
        row = _one(
            connection,
            """
            SELECT source_id, observed_at, request_url, final_url, http_status,
                   payload_sha256, import_batch_id, metadata_json
            FROM source_snapshots WHERE source_snapshot_id = ?
            """,
            (evidence["capture_snapshot_id"],),
            f"strict Archive capture {identifier}",
        )
        metadata = _json_object(row["metadata_json"], "strict Archive capture metadata")
        if (
            row["source_id"] != evidence["archive_item_source_id"]
            or row["observed_at"] != evidence["observed_at"]
            or row["http_status"] != 200
            or row["payload_sha256"] != evidence["payload_sha256"]
            or metadata.get("archive_metadata_snapshot_id") != snapshot["snapshot_id"]
            or metadata.get("archive_metadata_snapshot_sha256") != snapshot["_sha256"]
            or metadata.get("publication_authority") is not False
        ):
            raise ArchiveHintReconciliationError("strict Archive capture differs from snapshot")
        batches.add(row["import_batch_id"])
        capture_rows.append(
            {
                "archive_item": identifier,
                "archive_item_source_id": row["source_id"],
                "capture_snapshot_id": evidence["capture_snapshot_id"],
                "payload_sha256": row["payload_sha256"],
                "observed_at": row["observed_at"],
            }
        )
    if len(batches) != 1:
        raise ArchiveHintReconciliationError("late Archive captures do not share one import")
    batch_id = next(iter(batches))
    batch = _one(
        connection,
        """
        SELECT importer_name, importer_version, input_sha256, source_snapshot_date,
               started_at, completed_at, status, statistics_json
        FROM import_batches WHERE import_batch_id = ?
        """,
        (batch_id,),
        "strict Archive import batch",
    )
    if batch["importer_name"] != "internet_archive_metadata" or batch["status"] != "completed":
        raise ArchiveHintReconciliationError("strict Archive import batch is not completed")
    return {
        "archive_import_batch_id": batch_id,
        "archive_importer_version": batch["importer_version"],
        "archive_import_input_sha256": batch["input_sha256"],
        "capture_rows": capture_rows,
    }


def _hint_import_binding(
    connection: sqlite3.Connection, evidence: dict[str, Any]
) -> dict[str, Any]:
    batch = _one(
        connection,
        """
        SELECT import_batch_id, importer_version, source_snapshot_date, started_at,
               completed_at, status, statistics_json
        FROM import_batches
        WHERE importer_name = 'archive_url_discovery_hints' AND input_sha256 = ?
        """,
        (NORMALIZED_HINTS_SHA256,),
        "original Archive hint import batch",
    )
    expected_statistics = {
        "new_hint_sources": EXPECTED_LATE_HINTS,
        "resolved_existing_sources": EXPECTED_ALREADY_PROVIDER,
        "valid_hints": EXPECTED_RETAINED_URLS,
    }
    if (
        batch["status"] != "completed"
        or batch["started_at"] != evidence["discovery"]["discovered_at"]
        or batch["completed_at"] != evidence["discovery"]["discovered_at"]
        or _json_object(batch["statistics_json"], "hint import statistics")
        != expected_statistics
    ):
        raise ArchiveHintReconciliationError("original hint import receipt differs")
    observation = _one(
        connection,
        """
        SELECT import_observation_id, importer_version, source_snapshot_date,
               observed_at, completed_at, status, statistics_json
        FROM import_observations WHERE import_batch_id = ? AND observed_at = ?
        """,
        (batch["import_batch_id"], batch["started_at"]),
        "original Archive hint import observation",
    )
    if (
        observation["status"] != "completed"
        or observation["completed_at"] != batch["completed_at"]
        or _json_object(observation["statistics_json"], "hint observation statistics")
        != expected_statistics
    ):
        raise ArchiveHintReconciliationError("original hint import observation differs")
    post_source_id = source_id("reddit", "post", REDDIT_POST_ID)
    post = _one(
        connection,
        """
        SELECT platform, source_kind, native_id, created_by_import_batch_id
        FROM sources WHERE source_id = ?
        """,
        (post_source_id,),
        "Reddit discovery source",
    )
    if tuple(post) != ("reddit", "post", REDDIT_POST_ID, batch["import_batch_id"]):
        raise ArchiveHintReconciliationError("Reddit discovery source identity differs")
    post_origin = _one(
        connection,
        """
        SELECT source_metadata_observation_id, candidate_sha256
        FROM source_metadata_observations
        WHERE source_id = ? AND import_batch_id = ? AND import_observation_id = ?
        """,
        (post_source_id, batch["import_batch_id"], observation["import_observation_id"]),
        "Reddit discovery-source origin observation",
    )
    return {
        "hint_import_batch_id": batch["import_batch_id"],
        "hint_importer_version": batch["importer_version"],
        "hint_import_observation_id": observation["import_observation_id"],
        "hint_observed_at": batch["started_at"],
        "reddit_post_source_id": post_source_id,
        "post_origin_observation_id": post_origin["source_metadata_observation_id"],
        "post_origin_candidate_sha256": post_origin["candidate_sha256"],
    }


def _provider_record(record: dict[str, Any], native_id: str) -> dict[str, Any]:
    if record.get("source") != "original" or record.get("original") not in (None, ""):
        raise ArchiveHintReconciliationError(f"{native_id!r} is not provider-original")
    declared_bytes = byte_count(record.get("size"))
    declared_duration_ms = duration_ms(record.get("length"))
    provider_format = record.get("format")
    if (
        declared_bytes is None
        or declared_duration_ms is None
        or not isinstance(provider_format, str)
        or not provider_format
        or len(provider_format) > 256
    ):
        raise ArchiveHintReconciliationError(f"{native_id!r} lacks bounded provider metrics")
    hashes: dict[str, str] = {}
    for algorithm, pattern in HASH_PATTERNS.items():
        value = record.get(algorithm)
        if not isinstance(value, str) or not pattern.fullmatch(value):
            raise ArchiveHintReconciliationError(
                f"{native_id!r} lacks a canonical provider {algorithm}"
            )
        hashes[algorithm] = value
    return {
        "byte_count": declared_bytes,
        "duration_ms": declared_duration_ms,
        "format": provider_format,
        "hashes": hashes,
    }


def _provider_catalog_evidence(
    connection: sqlite3.Connection,
    *,
    hint_source_id: str,
    provider_source_id: str,
    archive_url: str,
    item: str,
    filename: str,
    native_id: str,
    provider_record: dict[str, Any],
    archive_import_batch_id: str,
    archive: dict[str, Any],
) -> dict[str, Any]:
    declared = _provider_record(provider_record, native_id)
    hint = _one(
        connection,
        """
        SELECT platform, source_kind, native_id, canonical_url, title,
               created_by_import_batch_id, created_at
        FROM sources WHERE source_id = ?
        """,
        (hint_source_id,),
        f"hint source {native_id}",
    )
    provider = _one(
        connection,
        """
        SELECT platform, source_kind, native_id, parent_source_id, canonical_url,
               title, created_by_import_batch_id, created_at
        FROM sources WHERE source_id = ?
        """,
        (provider_source_id,),
        f"provider source {native_id}",
    )
    item_source_id = source_id("internet_archive", "archive_item", item)
    title = title_from_media_filename(filename)
    if (
        tuple(hint[:5])
        != ("internet_archive", "archive_url_discovery_hint", native_id, archive_url, title)
        or tuple(provider[:6])
        != (
            "internet_archive",
            "archive_media_file",
            native_id,
            item_source_id,
            archive_url,
            title,
        )
        or provider["created_by_import_batch_id"] != archive_import_batch_id
        or provider["created_at"] <= hint["created_at"]
    ):
        raise ArchiveHintReconciliationError(f"late source projection differs for {native_id!r}")

    hint_observation = _one(
        connection,
        """
        SELECT source_metadata_observation_id, import_observation_id, observed_at,
               quality_rank, quality_basis, candidate_sha256, canonical_url,
               title, access_state, review_state, metadata_json
        FROM source_metadata_observations
        WHERE source_id = ? AND import_batch_id = ?
        """,
        (hint_source_id, hint["created_by_import_batch_id"]),
        f"hint origin observation {native_id}",
    )
    hint_metadata = _json_object(hint_observation["metadata_json"], "hint metadata")
    if (
        hint_observation["canonical_url"] != archive_url
        or hint_observation["title"] != title
        or hint_observation["access_state"] != "unknown"
        or hint_observation["review_state"] != "unreviewed"
        or hint_metadata != {"discovery_state": "url_hint_only", "reddit_post_id": REDDIT_POST_ID}
    ):
        raise ArchiveHintReconciliationError(f"hint origin differs for {native_id!r}")

    provider_observation = _one(
        connection,
        """
        SELECT source_metadata_observation_id, import_observation_id, observed_at,
               quality_rank, quality_basis, candidate_sha256, parent_source_id,
               canonical_url, title, access_state, review_state, metadata_json
        FROM source_metadata_observations
        WHERE source_id = ? AND import_batch_id = ?
        """,
        (provider_source_id, archive_import_batch_id),
        f"provider origin observation {native_id}",
    )
    selected_metadata = {
        "internet_archive_item": item,
        "filename": filename,
        "format": declared["format"],
        "source_class": "original",
        "derivative_of": None,
        "byte_count": declared["byte_count"],
        "duration_ms": declared["duration_ms"],
    }
    if (
        provider_observation["parent_source_id"] != item_source_id
        or provider_observation["canonical_url"] != archive_url
        or provider_observation["title"] != title
        or provider_observation["access_state"] != "public"
        or provider_observation["review_state"] != "metadata_only"
        or _json_object(provider_observation["metadata_json"], "provider metadata")
        != selected_metadata
    ):
        raise ArchiveHintReconciliationError(f"provider origin differs for {native_id!r}")

    projection_snapshot = _one(
        connection,
        """
        SELECT source_snapshot_id, observed_at, request_url, final_url, http_status,
               payload_sha256, artifact_path, metadata_json
        FROM source_snapshots WHERE source_id = ? AND import_batch_id = ?
        """,
        (provider_source_id, archive_import_batch_id),
        f"provider projection snapshot {native_id}",
    )
    expected_projection_sha = sha256_bytes(canonical_json(selected_metadata).encode("utf-8"))
    expected_payload_path = archive["snapshot"]["_path"].parent / f"item-{item}.metadata.json"
    try:
        artifact_path = Path(projection_snapshot["artifact_path"]).resolve(strict=True)
    except (TypeError, OSError) as error:
        raise ArchiveHintReconciliationError("provider projection artifact is unavailable") from error
    if (
        projection_snapshot["observed_at"] != provider_observation["observed_at"]
        or projection_snapshot["request_url"] != f"https://archive.org/metadata/{quote(item, safe='')}"
        or projection_snapshot["final_url"] != projection_snapshot["request_url"]
        or projection_snapshot["http_status"] is not None
        or projection_snapshot["payload_sha256"] != expected_projection_sha
        or artifact_path != expected_payload_path
        or _json_object(projection_snapshot["metadata_json"], "provider snapshot metadata") != {}
    ):
        raise ArchiveHintReconciliationError(f"provider projection snapshot differs for {native_id!r}")

    hash_rows = connection.execute(
        """
        SELECT algorithm, digest, declared_by, observed_at
        FROM source_hashes WHERE source_id = ? ORDER BY algorithm, digest
        """,
        (provider_source_id,),
    ).fetchall()
    expected_hash_rows = [
        (algorithm, digest, "internet_archive_metadata", provider_observation["observed_at"])
        for algorithm, digest in sorted(declared["hashes"].items())
    ]
    if [tuple(row) for row in hash_rows] != expected_hash_rows:
        raise ArchiveHintReconciliationError(f"provider hashes differ for {native_id!r}")

    recording_rows = connection.execute(
        """
        SELECT recording_source_id, recording_id, mapping_role, source_start_ms,
               source_end_ms, recording_start_ms, recording_end_ms, mapping_method,
               confidence_state, metadata_json
        FROM recording_sources WHERE source_id = ? ORDER BY recording_source_id
        """,
        (provider_source_id,),
    ).fetchall()
    if len(recording_rows) != 1:
        raise ArchiveHintReconciliationError(f"provider recording projection is not unique for {native_id!r}")
    recording = recording_rows[0]
    if (
        recording["mapping_role"] != "archive_original_file"
        or any(recording[key] is not None for key in (
            "source_start_ms", "source_end_ms", "recording_start_ms", "recording_end_ms"
        ))
        or recording["mapping_method"] != "archive_filename_platform_id_grouping"
        or recording["confidence_state"] != "metadata_only"
        or _json_object(recording["metadata_json"], "recording mapping metadata")
        != {"source_class": "original"}
    ):
        raise ArchiveHintReconciliationError(f"provider recording projection differs for {native_id!r}")
    media_sources = connection.execute(
        "SELECT count(*) FROM media_sources WHERE source_id = ?", (provider_source_id,)
    ).fetchone()[0]
    if media_sources != 0:
        raise ArchiveHintReconciliationError(f"provider source already has downloaded-byte lineage for {native_id!r}")
    direct_relations = connection.execute(
        """
        SELECT count(*) FROM source_relations
        WHERE (from_source_id = ? AND to_source_id = ?)
           OR (from_source_id = ? AND to_source_id = ?)
        """,
        (hint_source_id, provider_source_id, provider_source_id, hint_source_id),
    ).fetchone()[0]
    if direct_relations:
        raise ArchiveHintReconciliationError(f"hint/provider sources already have relation state for {native_id!r}")
    return {
        "hint_metadata_observation_id": hint_observation["source_metadata_observation_id"],
        "hint_candidate_sha256": hint_observation["candidate_sha256"],
        "provider_metadata_observation_id": provider_observation["source_metadata_observation_id"],
        "provider_candidate_sha256": provider_observation["candidate_sha256"],
        "provider_projection_snapshot_id": projection_snapshot["source_snapshot_id"],
        "provider_projection_sha256": projection_snapshot["payload_sha256"],
        "archive_item_capture_snapshot_id": archive["items"][item]["capture_snapshot_id"],
        "provider_recording_source_id": recording["recording_source_id"],
        "provider_recording_id": recording["recording_id"],
        "provider_format": declared["format"],
        "provider_declared_byte_count": declared["byte_count"],
        "provider_declared_duration_ms": declared["duration_ms"],
        "provider_declared_crc32": declared["hashes"]["crc32"],
        "provider_declared_md5": declared["hashes"]["md5"],
        "provider_declared_sha1": declared["hashes"]["sha1"],
    }


def _relation_external_binding(
    connection: sqlite3.Connection,
    *,
    hint_binding: dict[str, Any],
    target_source_id: str,
    archive_url: str,
    native_id: str,
) -> dict[str, Any]:
    external = _one(
        connection,
        """
        SELECT external_id_id, object_type, object_id, namespace, external_value,
               confidence_state, basis, source_id
        FROM external_ids
        WHERE namespace = 'reddit_archive_url_hint' AND external_value = ?
        """,
        (archive_url,),
        f"retained URL external ID {native_id}",
    )
    if tuple(external)[1:] != (
        "source",
        target_source_id,
        "reddit_archive_url_hint",
        archive_url,
        "metadata_only",
        f"Reddit post {REDDIT_POST_ID} URL list",
        hint_binding["reddit_post_source_id"],
    ):
        raise ArchiveHintReconciliationError(f"retained URL external ID differs for {native_id!r}")
    external_observation = _one(
        connection,
        """
        SELECT external_id_observation_id, candidate_sha256, source_id,
               confidence_state, basis
        FROM external_id_observations
        WHERE external_id_id = ? AND import_batch_id = ?
          AND import_observation_id = ?
        """,
        (
            external["external_id_id"],
            hint_binding["hint_import_batch_id"],
            hint_binding["hint_import_observation_id"],
        ),
        f"retained URL external-ID observation {native_id}",
    )
    if tuple(external_observation)[2:] != (
        hint_binding["reddit_post_source_id"],
        "metadata_only",
        f"Reddit post {REDDIT_POST_ID} URL list",
    ):
        raise ArchiveHintReconciliationError(f"external-ID observation differs for {native_id!r}")
    relation = _one(
        connection,
        """
        SELECT source_relation_id, from_source_id, relation_kind, to_source_id,
               basis, confidence_state, metadata_json
        FROM source_relations
        WHERE from_source_id = ? AND relation_kind = 'references' AND to_source_id = ?
        """,
        (hint_binding["reddit_post_source_id"], target_source_id),
        f"retained URL source relation {native_id}",
    )
    if (
        tuple(relation)[1:6]
        != (
            hint_binding["reddit_post_source_id"],
            "references",
            target_source_id,
            "URL present in contributor-linked archive complement list",
            "metadata_only",
        )
        or _json_object(relation["metadata_json"], "retained relation metadata")
        != {"coverage_hint_only": True}
    ):
        raise ArchiveHintReconciliationError(f"retained source relation differs for {native_id!r}")
    relation_observation = _one(
        connection,
        """
        SELECT source_relation_observation_id, candidate_sha256, basis,
               confidence_state, metadata_json
        FROM source_relation_observations
        WHERE source_relation_id = ? AND import_batch_id = ?
          AND import_observation_id = ?
        """,
        (
            relation["source_relation_id"],
            hint_binding["hint_import_batch_id"],
            hint_binding["hint_import_observation_id"],
        ),
        f"retained URL relation observation {native_id}",
    )
    if (
        relation_observation["basis"]
        != "URL present in contributor-linked archive complement list"
        or relation_observation["confidence_state"] != "metadata_only"
        or _json_object(relation_observation["metadata_json"], "relation observation metadata")
        != {"coverage_hint_only": True}
    ):
        raise ArchiveHintReconciliationError(f"relation observation differs for {native_id!r}")
    return {
        "external_id_id": external["external_id_id"],
        "external_id_observation_id": external_observation["external_id_observation_id"],
        "external_id_candidate_sha256": external_observation["candidate_sha256"],
        "reference_relation_id": relation["source_relation_id"],
        "reference_relation_observation_id": relation_observation[
            "source_relation_observation_id"
        ],
        "reference_relation_candidate_sha256": relation_observation["candidate_sha256"],
    }


def build_archive_hint_reconciliation_plan(
    connection: sqlite3.Connection,
    original_hints_path: Path,
    normalized_hints_path: Path,
    discovery_metadata_path: Path,
    archive_snapshot_path: Path,
) -> dict[str, Any]:
    """Build the fixed deterministic plan without writing catalog rows."""

    owns_transaction = not connection.in_transaction
    if owns_transaction:
        connection.execute("BEGIN")
    try:
        return _build_archive_hint_reconciliation_plan_in_snapshot(
            connection,
            Path(original_hints_path),
            Path(normalized_hints_path),
            Path(discovery_metadata_path),
            Path(archive_snapshot_path),
        )
    finally:
        if owns_transaction and connection.in_transaction:
            connection.rollback()


def _build_archive_hint_reconciliation_plan_in_snapshot(
    connection: sqlite3.Connection,
    original_hints_path: Path,
    normalized_hints_path: Path,
    discovery_metadata_path: Path,
    archive_snapshot_path: Path,
) -> dict[str, Any]:
    evidence = _retained_evidence(
        original_hints_path, normalized_hints_path, discovery_metadata_path
    )
    archive = _archive_snapshot_evidence(archive_snapshot_path)
    archive_binding = _archive_catalog_binding(connection, archive)
    hint_binding = _hint_import_binding(connection, evidence)
    if archive["snapshot"]["observed_at"] <= hint_binding["hint_observed_at"]:
        raise ArchiveHintReconciliationError("Archive resolution snapshot does not postdate hints")

    candidates: list[dict[str, Any]] = []
    catalog_rows: list[dict[str, Any]] = []
    item_counts: Counter[str] = Counter()
    candidate_item_counts: Counter[str] = Counter()
    already_provider = 0
    for index, locator in enumerate(evidence["urls"]):
        item = locator["item"]
        native_id = locator["native_id"]
        archive_url = locator["archive_url"]
        item_counts[item] += 1
        expected_provider = source_id(
            "internet_archive", "archive_media_file", native_id
        )
        expected_hint = source_id(
            "internet_archive", "archive_url_discovery_hint", native_id
        )
        provider = connection.execute(
            "SELECT source_id, canonical_url FROM sources WHERE source_id = ?",
            (expected_provider,),
        ).fetchone()
        hint = connection.execute(
            "SELECT source_id, canonical_url FROM sources WHERE source_id = ?",
            (expected_hint,),
        ).fetchone()
        if provider is None or provider["canonical_url"] != archive_url:
            raise ArchiveHintReconciliationError(f"provider source missing for {native_id!r}")
        if hint is None:
            target_source_id = expected_provider
            already_provider += 1
            classification = "already_provider_at_hint_import"
        else:
            if hint["canonical_url"] != archive_url:
                raise ArchiveHintReconciliationError(f"hint URL differs for {native_id!r}")
            target_source_id = expected_hint
            classification = "late_exact_provider_candidate"
        provenance = _relation_external_binding(
            connection,
            hint_binding=hint_binding,
            target_source_id=target_source_id,
            archive_url=archive_url,
            native_id=native_id,
        )
        binding_row: dict[str, Any] = {
            "list_index": index,
            "archive_url": archive_url,
            "native_id": native_id,
            "original_target_source_id": target_source_id,
            "classification": classification,
            **provenance,
        }
        if hint is not None:
            if item not in LATE_ARCHIVE_ITEMS:
                raise ArchiveHintReconciliationError("late hint escaped reviewed Archive items")
            record = archive["records"].get(native_id)
            if record is None or record.get("name") != locator["filename"]:
                raise ArchiveHintReconciliationError(f"sealed provider record missing for {native_id!r}")
            provider_evidence = _provider_catalog_evidence(
                connection,
                hint_source_id=expected_hint,
                provider_source_id=expected_provider,
                archive_url=archive_url,
                item=item,
                filename=locator["filename"],
                native_id=native_id,
                provider_record=record,
                archive_import_batch_id=archive_binding["archive_import_batch_id"],
                archive=archive,
            )
            duplicate_external = connection.execute(
                """
                SELECT count(*) FROM external_ids
                WHERE object_type = 'source' AND object_id = ?
                  AND namespace = 'reddit_archive_url_hint' AND external_value = ?
                """,
                (expected_provider, archive_url),
            ).fetchone()[0]
            if duplicate_external:
                raise ArchiveHintReconciliationError("hint external ID was copied to provider")
            match_candidate_id = stable_id(
                "mat",
                IMPORTER_NAME,
                NORMALIZED_HINTS_SHA256,
                ARCHIVE_SNAPSHOT_SHA256,
                expected_hint,
                expected_provider,
            )
            review_task_id = stable_id(
                "rtk", TASK_KIND, "match_candidate", match_candidate_id
            )
            generic_metadata = {
                "schema_version": 1,
                "candidate_kind": "archive_hint_to_provider_source",
                "evidence_basis": "exact_archive_native_id_and_canonical_url",
                "archive_native_id": native_id,
                "calibration_state": "not_calibrated",
                "candidate_state": "candidate_only_unreviewed",
                "requires_human_review": True,
                "relationship_asserted": False,
                "source_merge_performed": False,
                "external_id_copied": False,
                "publication_authority": False,
            }
            typed_evidence = {
                "match_candidate_id": match_candidate_id,
                "review_task_id": review_task_id,
                "hint_source_id": expected_hint,
                "provider_source_id": expected_provider,
                "archive_item": item,
                "archive_filename": locator["filename"],
                "archive_native_id": native_id,
                "archive_url": archive_url,
                **provenance,
                **provider_evidence,
                "candidate_state": "candidate_only_unreviewed",
                "match_basis": "exact_archive_native_id_and_canonical_url",
                "requires_human_review": True,
                "relationship_asserted": False,
                "source_merge_performed": False,
                "external_id_copied": False,
                "visibility": "private",
                "publication_authority": "none",
                "provider_fields_are_content_truth": False,
                "payload_downloaded_or_read": False,
            }
            candidates.append(
                {
                    "generic": {
                        "match_candidate_id": match_candidate_id,
                        "left_object_type": "source",
                        "left_object_id": expected_hint,
                        "right_object_type": "source",
                        "right_object_id": expected_provider,
                        "match_method": MATCH_METHOD,
                        "raw_score": None,
                        "calibrated_probability": None,
                        "decision_state": "candidate",
                        "metadata_json": generic_metadata,
                    },
                    "typed_evidence": typed_evidence,
                    "review_task": {
                        "review_task_id": review_task_id,
                        "task_kind": TASK_KIND,
                        "target_type": "match_candidate",
                        "target_id": match_candidate_id,
                        "reason": TASK_REASON,
                        "priority": TASK_PRIORITY,
                        "created_at": archive["snapshot"]["observed_at"],
                    },
                }
            )
            candidate_item_counts[item] += 1
            binding_row.update(provider_evidence)
        catalog_rows.append(binding_row)

    candidates.sort(key=lambda row: row["generic"]["match_candidate_id"])
    candidate_ids = [row["generic"]["match_candidate_id"] for row in candidates]
    if len(candidate_ids) != len(set(candidate_ids)):
        raise ArchiveHintReconciliationError("candidate identities collide")
    if (
        already_provider != EXPECTED_ALREADY_PROVIDER
        or len(candidates) != EXPECTED_LATE_HINTS
        or dict(sorted(item_counts.items())) != dict(sorted(EXPECTED_URLS_BY_ITEM.items()))
        or dict(sorted(candidate_item_counts.items()))
        != {item: EXPECTED_URLS_BY_ITEM[item] for item in LATE_ARCHIVE_ITEMS}
    ):
        raise ArchiveHintReconciliationError("exact one-to-one audit accounting differs")

    catalog_evidence = {
        "hint_binding": hint_binding,
        "archive_binding": archive_binding,
        "retained_url_bindings": catalog_rows,
    }
    catalog_evidence_sha256 = sha256_bytes(
        canonical_json(catalog_evidence).encode("utf-8")
    )
    statistics = {
        "retained_urls_total": EXPECTED_RETAINED_URLS,
        "already_provider_at_hint_import": EXPECTED_ALREADY_PROVIDER,
        "late_exact_provider_candidates": EXPECTED_LATE_HINTS,
        "candidates_by_archive_item": dict(sorted(candidate_item_counts.items())),
        "review_tasks_total": EXPECTED_LATE_HINTS,
        "media_payloads_read": 0,
        "media_payload_bytes_read": 0,
        "sources_created": 0,
        "sources_updated": 0,
        "sources_deleted": 0,
        "source_relations_created": 0,
        "external_ids_created": 0,
        "recording_relations_created": 0,
        "recording_merges": 0,
        "review_decisions": 0,
        "publication_decisions": 0,
        "public_rows": 0,
        "identity_assertions": 0,
        "claims": 0,
    }
    combined = hashlib.sha256()
    combined.update(b"archive-hint-provider-reconciliation-input-v1\0")
    for label, body in (
        (b"original", evidence["original_body"]),
        (b"normalized", evidence["normalized_body"]),
        (b"discovery", evidence["discovery_body"]),
        (b"archive-snapshot", archive["snapshot"]["_sha256"].encode("ascii")),
    ):
        combined.update(len(label).to_bytes(2, "big"))
        combined.update(label)
        combined.update(len(body).to_bytes(8, "big"))
        combined.update(body)
    core = {
        "schema_version": 1,
        "plan_kind": PLAN_KIND,
        "planner_version": PLANNER_VERSION,
        "inputs": {
            "original_hints_sha256": ORIGINAL_HINTS_SHA256,
            "original_hints_byte_count": len(evidence["original_body"]),
            "original_hints_filename": evidence["original_path"].name,
            "normalized_hints_sha256": NORMALIZED_HINTS_SHA256,
            "normalized_hints_byte_count": len(evidence["normalized_body"]),
            "normalized_hints_filename": evidence["normalized_path"].name,
            "discovery_sha256": DISCOVERY_SHA256,
            "discovery_byte_count": len(evidence["discovery_body"]),
            "discovery_filename": evidence["discovery_path"].name,
            "catbox_retrieval_url": CATBOX_RETRIEVAL_URL,
            "archive_snapshot_id": archive["snapshot"]["snapshot_id"],
            "archive_snapshot_sha256": archive["snapshot"]["_sha256"],
            "archive_request_id": archive["snapshot"]["request"]["request_id"],
            "combined_input_sha256": combined.hexdigest(),
        },
        "catalog_binding": {
            **hint_binding,
            "archive_import_batch_id": archive_binding["archive_import_batch_id"],
            "archive_importer_version": archive_binding["archive_importer_version"],
            "archive_import_input_sha256": archive_binding["archive_import_input_sha256"],
            "catalog_evidence_sha256": catalog_evidence_sha256,
            "resolved_at": archive["snapshot"]["observed_at"],
        },
        "scope": {
            "reddit_post_id": REDDIT_POST_ID,
            "retained_urls": EXPECTED_RETAINED_URLS,
            "already_provider_sources": EXPECTED_ALREADY_PROVIDER,
            "late_hint_sources": EXPECTED_LATE_HINTS,
            "late_archive_items": list(LATE_ARCHIVE_ITEMS),
            "match_basis": "exact_archive_native_id_and_canonical_url",
            "source_capture_state": "retained_contributor_complement_without_exact_catbox_response_envelope",
        },
        "candidates": candidates,
        "statistics": statistics,
        "policy": {
            "plan_is_private": True,
            "expected_plan_sha256_required_for_import": True,
            "candidate_only": True,
            "requires_human_review": True,
            "relationship_asserted": False,
            "source_merge_performed": False,
            "source_rows_mutated": False,
            "historical_observations_mutated": False,
            "external_id_copied": False,
            "provider_metadata_is_content_truth": False,
            "media_payload_downloaded_or_read": False,
            "review_outcome_asserted": False,
            "publication_authority": False,
            "catbox_response_envelope_captured": False,
            "direct_post_body_catbox_link_asserted": False,
            "capacity_limits": {"retained_urls": MAX_URLS, "url_length": MAX_URL_LENGTH},
        },
    }
    plan_sha256 = sha256_bytes(canonical_json(core).encode("utf-8"))
    return {
        **core,
        "plan_id": f"ahprp_{plan_sha256[:32]}",
        "plan_sha256": plan_sha256,
    }


def summarize_archive_hint_reconciliation_plan(plan: dict[str, Any]) -> dict[str, Any]:
    """Return a URL/native-ID-free default CLI summary."""

    return {
        "valid": True,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "planner_version": plan["planner_version"],
        "original_hints_sha256": plan["inputs"]["original_hints_sha256"],
        "normalized_hints_sha256": plan["inputs"]["normalized_hints_sha256"],
        "discovery_sha256": plan["inputs"]["discovery_sha256"],
        "archive_snapshot_id": plan["inputs"]["archive_snapshot_id"],
        "archive_snapshot_sha256": plan["inputs"]["archive_snapshot_sha256"],
        "catalog_evidence_sha256": plan["catalog_binding"]["catalog_evidence_sha256"],
        "scope": plan["scope"],
        "statistics": plan["statistics"],
        "expected_plan_sha256_required_for_import": True,
        "requires_human_review": True,
        "relationship_asserted": False,
        "source_merge_performed": False,
        "publication_authority": False,
    }


def _protected_tables(connection: sqlite3.Connection) -> list[str]:
    return [
        row["name"]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        if row["name"] not in ALLOWED_WRITE_TABLES
    ]


def _protected_counts(connection: sqlite3.Connection) -> dict[str, int]:
    result: dict[str, int] = {}
    for table in _protected_tables(connection):
        quoted = table.replace('"', '""')
        result[table] = int(
            connection.execute(f'SELECT count(*) FROM "{quoted}"').fetchone()[0]
        )
    return result


def _install_write_guards(connection: sqlite3.Connection) -> list[str]:
    names: list[str] = []
    for table in _protected_tables(connection):
        # SQLite rejects triggers declared directly on virtual tables.  Their
        # ordinary shadow tables remain in the protected set, so guarding those
        # still rejects writes routed through a virtual table while allowing this
        # generic guard to coexist with private FTS indexes.
        table_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()[0]
        if table_sql and table_sql.lstrip().upper().startswith("CREATE VIRTUAL TABLE"):
            continue
        token = hashlib.sha256(table.encode("utf-8")).hexdigest()[:16]
        quoted_table = table.replace('"', '""')
        for operation in ("INSERT", "UPDATE", "DELETE"):
            name = f"ahpr_protected_{token}_{operation.lower()}"
            quoted_name = name.replace('"', '""')
            try:
                connection.execute(
                    f'CREATE TEMP TRIGGER "{quoted_name}" BEFORE {operation} '
                    f'ON main."{quoted_table}" BEGIN '
                    "SELECT RAISE(ABORT, 'archive hint reconciliation protected-table write'); END"
                )
            except Exception:
                _drop_write_guards(connection, names)
                raise
            names.append(name)
    return names


def _drop_write_guards(connection: sqlite3.Connection, names: list[str]) -> None:
    first_error: Exception | None = None
    for name in reversed(names):
        quoted = name.replace('"', '""')
        try:
            connection.execute(f'DROP TRIGGER temp."{quoted}"')
        except Exception as error:  # pragma: no cover - defensive connection failure
            if first_error is None:
                first_error = error
    if first_error is not None:
        raise first_error


@contextmanager
def _protected_write_guard(connection: sqlite3.Connection):
    names = _install_write_guards(connection)
    try:
        yield
    finally:
        _drop_write_guards(connection, names)


def _insert_review_task(
    connection: sqlite3.Connection, task: dict[str, Any], *, allow_insert: bool
) -> None:
    values = (
        task["task_kind"],
        task["target_type"],
        task["target_id"],
        task["reason"],
        task["priority"],
        task["created_at"],
    )
    if allow_insert:
        connection.execute(
            """
            INSERT OR IGNORE INTO review_tasks(
                review_task_id, task_kind, target_type, target_id, reason, priority,
                status, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, 'open', ?, ?)
            """,
            (task["review_task_id"], *values, task["created_at"]),
        )
    row = connection.execute(
        """
        SELECT task_kind, target_type, target_id, reason, priority, created_at
        FROM review_tasks WHERE review_task_id = ?
        """,
        (task["review_task_id"],),
    ).fetchone()
    if row is None or tuple(row) != values:
        raise ArchiveHintReconciliationError("existing review task conflicts with plan")


def _insert_candidate(
    connection: sqlite3.Connection,
    candidate: dict[str, Any],
    import_batch_id: str,
    *,
    allow_insert: bool,
) -> None:
    generic = candidate["generic"]
    generic_values = (
        generic["left_object_type"],
        generic["left_object_id"],
        generic["right_object_type"],
        generic["right_object_id"],
        generic["match_method"],
        None,
        None,
        "candidate",
        canonical_json(generic["metadata_json"]),
    )
    if allow_insert:
        connection.execute(
            """
            INSERT OR IGNORE INTO match_candidates(
                match_candidate_id, left_object_type, left_object_id,
                right_object_type, right_object_id, match_method, raw_score,
                calibrated_probability, decision_state, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (generic["match_candidate_id"], *generic_values),
        )
    row = connection.execute(
        """
        SELECT left_object_type, left_object_id, right_object_type, right_object_id,
               match_method, raw_score, calibrated_probability, decision_state,
               metadata_json
        FROM match_candidates WHERE match_candidate_id = ?
        """,
        (generic["match_candidate_id"],),
    ).fetchone()
    if row is None or tuple(row) != generic_values:
        raise ArchiveHintReconciliationError("existing generic candidate conflicts")

    typed = candidate["typed_evidence"]
    evidence_json = {
        "schema_version": 1,
        "archive_snapshot_id": ARCHIVE_SNAPSHOT_ID,
        "archive_snapshot_sha256": ARCHIVE_SNAPSHOT_SHA256,
        "original_hints_sha256": ORIGINAL_HINTS_SHA256,
        "normalized_hints_sha256": NORMALIZED_HINTS_SHA256,
        "archive_item": typed["archive_item"],
        "archive_filename": typed["archive_filename"],
        "archive_native_id": typed["archive_native_id"],
        "archive_url": typed["archive_url"],
        "hint_metadata_observation_id": typed["hint_metadata_observation_id"],
        "provider_metadata_observation_id": typed["provider_metadata_observation_id"],
        "provider_projection_snapshot_id": typed["provider_projection_snapshot_id"],
        "provider_projection_sha256": typed["provider_projection_sha256"],
        "archive_item_capture_snapshot_id": typed["archive_item_capture_snapshot_id"],
        "reference_relation_observation_id": typed["reference_relation_observation_id"],
        "external_id_observation_id": typed["external_id_observation_id"],
        "provider_recording_source_id": typed["provider_recording_source_id"],
        "provider_recording_id": typed["provider_recording_id"],
        "provider_format": typed["provider_format"],
        "provider_declared_byte_count": typed["provider_declared_byte_count"],
        "provider_declared_duration_ms": typed["provider_declared_duration_ms"],
        "provider_declared_crc32": typed["provider_declared_crc32"],
        "provider_declared_md5": typed["provider_declared_md5"],
        "provider_declared_sha1": typed["provider_declared_sha1"],
        "match_basis": typed["match_basis"],
        "candidate_state": typed["candidate_state"],
        "requires_human_review": True,
        "relationship_asserted": False,
        "source_merge_performed": False,
        "external_id_copied": False,
        "provider_fields_are_content_truth": False,
        "payload_downloaded_or_read": False,
        "publication_authority": False,
    }
    values = (
        import_batch_id,
        typed["review_task_id"],
        typed["hint_source_id"],
        typed["provider_source_id"],
        typed["hint_metadata_observation_id"],
        typed["provider_metadata_observation_id"],
        typed["provider_projection_snapshot_id"],
        typed["provider_projection_sha256"],
        typed["archive_item_capture_snapshot_id"],
        typed["reference_relation_id"],
        typed["reference_relation_observation_id"],
        typed["external_id_id"],
        typed["external_id_observation_id"],
        typed["provider_recording_source_id"],
        typed["provider_recording_id"],
        typed["archive_item"],
        typed["archive_filename"],
        typed["archive_native_id"],
        typed["archive_url"],
        typed["provider_format"],
        typed["provider_declared_byte_count"],
        typed["provider_declared_duration_ms"],
        typed["provider_declared_crc32"],
        typed["provider_declared_md5"],
        typed["provider_declared_sha1"],
        "exact_archive_native_id_and_canonical_url",
        "candidate_only_unreviewed",
        1,
        0,
        0,
        0,
        "private",
        "none",
        canonical_json(evidence_json),
    )
    if allow_insert:
        connection.execute(
            """
            INSERT OR IGNORE INTO archive_hint_provider_candidates(
                match_candidate_id, import_batch_id, review_task_id, hint_source_id,
                provider_source_id, hint_metadata_observation_id,
                provider_metadata_observation_id, provider_projection_snapshot_id,
                provider_projection_sha256, archive_item_capture_snapshot_id, reference_relation_id,
                reference_relation_observation_id, external_id_id,
                external_id_observation_id, provider_recording_source_id,
                provider_recording_id, archive_item, archive_filename,
                archive_native_id, archive_url, provider_format,
                provider_declared_byte_count, provider_declared_duration_ms,
                provider_declared_crc32, provider_declared_md5,
                provider_declared_sha1, match_basis, candidate_state,
                requires_human_review, relationship_asserted, source_merge_performed,
                external_id_copied, visibility, publication_authority, evidence_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                     ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (generic["match_candidate_id"], *values),
        )
    row = connection.execute(
        """
        SELECT import_batch_id, review_task_id, hint_source_id, provider_source_id,
               hint_metadata_observation_id, provider_metadata_observation_id,
               provider_projection_snapshot_id, provider_projection_sha256,
               archive_item_capture_snapshot_id,
               reference_relation_id, reference_relation_observation_id,
               external_id_id, external_id_observation_id,
               provider_recording_source_id, provider_recording_id, archive_item,
               archive_filename, archive_native_id, archive_url, provider_format,
               provider_declared_byte_count, provider_declared_duration_ms,
               provider_declared_crc32, provider_declared_md5, provider_declared_sha1,
               match_basis, candidate_state, requires_human_review,
               relationship_asserted, source_merge_performed, external_id_copied,
               visibility, publication_authority, evidence_json
        FROM archive_hint_provider_candidates WHERE match_candidate_id = ?
        """,
        (generic["match_candidate_id"],),
    ).fetchone()
    if row is None or tuple(row) != values:
        raise ArchiveHintReconciliationError("existing typed candidate conflicts")


def _receipt_values(plan: dict[str, Any]) -> tuple[Any, ...]:
    stats = plan["statistics"]
    return (
        plan["catalog_binding"]["hint_import_batch_id"],
        plan["catalog_binding"]["archive_import_batch_id"],
        plan["catalog_binding"]["reddit_post_source_id"],
        plan["inputs"]["original_hints_sha256"],
        plan["inputs"]["normalized_hints_sha256"],
        plan["inputs"]["discovery_sha256"],
        plan["inputs"]["archive_snapshot_id"],
        plan["inputs"]["archive_snapshot_sha256"],
        plan["inputs"]["archive_request_id"],
        plan["inputs"]["combined_input_sha256"],
        plan["catalog_binding"]["catalog_evidence_sha256"],
        plan["plan_sha256"],
        plan["catalog_binding"]["resolved_at"],
        canonical_json(plan["scope"]["late_archive_items"]),
        stats["retained_urls_total"],
        stats["already_provider_at_hint_import"],
        stats["late_exact_provider_candidates"],
        stats["review_tasks_total"],
        canonical_json(stats),
        plan["catalog_binding"]["resolved_at"],
    )


def _verify_rows(
    connection: sqlite3.Connection,
    plan: dict[str, Any],
    batch_id: str,
    *,
    allow_insert: bool,
) -> None:
    values = _receipt_values(plan)
    if allow_insert:
        connection.execute(
            """
            INSERT INTO archive_hint_reconciliation_imports(
                import_batch_id, hint_import_batch_id, archive_import_batch_id,
                reddit_post_source_id, original_hints_sha256,
                normalized_hints_sha256, discovery_sha256, archive_snapshot_id,
                archive_snapshot_sha256, archive_request_id, combined_input_sha256,
                catalog_evidence_sha256, plan_sha256, observed_at,
                late_archive_items_json, retained_url_count,
                already_provider_count, candidate_count, review_task_count,
                statistics_json, imported_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (batch_id, *values),
        )
    row = connection.execute(
        """
        SELECT hint_import_batch_id, archive_import_batch_id, reddit_post_source_id,
               original_hints_sha256, normalized_hints_sha256, discovery_sha256,
               archive_snapshot_id, archive_snapshot_sha256, archive_request_id,
               combined_input_sha256, catalog_evidence_sha256, plan_sha256,
               observed_at, late_archive_items_json, retained_url_count,
               already_provider_count, candidate_count, review_task_count,
               statistics_json, imported_at
        FROM archive_hint_reconciliation_imports WHERE import_batch_id = ?
        """,
        (batch_id,),
    ).fetchone()
    if row is None or tuple(row) != values:
        raise ArchiveHintReconciliationError("archive hint import receipt differs")
    for candidate in plan["candidates"]:
        _insert_review_task(
            connection, candidate["review_task"], allow_insert=allow_insert
        )
        _insert_candidate(
            connection, candidate, batch_id, allow_insert=allow_insert
        )


def _verify_completed_batch(
    connection: sqlite3.Connection, plan: dict[str, Any], batch_id: str
) -> None:
    row = _one(
        connection,
        """
        SELECT importer_name, input_sha256, source_snapshot_date, started_at,
               completed_at, status, statistics_json
        FROM import_batches WHERE import_batch_id = ?
        """,
        (batch_id,),
        "archive hint candidate import batch",
    )
    expected = (
        IMPORTER_NAME,
        plan["plan_sha256"],
        plan["catalog_binding"]["resolved_at"][:10],
        plan["catalog_binding"]["resolved_at"],
        plan["catalog_binding"]["resolved_at"],
        "completed",
        canonical_json(plan["statistics"]),
    )
    if tuple(row) != expected:
        raise ArchiveHintReconciliationError("completed candidate import batch differs")


def import_archive_hint_reconciliation(
    connection: sqlite3.Connection,
    original_hints_path: Path,
    normalized_hints_path: Path,
    discovery_metadata_path: Path,
    archive_snapshot_path: Path,
    *,
    expected_plan_sha256: str,
) -> dict[str, Any]:
    """Admit only the reviewed plan identity as immutable private candidates."""

    if not isinstance(expected_plan_sha256, str) or not SHA256_RE.fullmatch(
        expected_plan_sha256
    ):
        raise ArchiveHintReconciliationError("expected plan SHA-256 is required")
    paths = (
        Path(original_hints_path),
        Path(normalized_hints_path),
        Path(discovery_metadata_path),
        Path(archive_snapshot_path),
    )
    initial = build_archive_hint_reconciliation_plan(connection, *paths)
    if initial["plan_sha256"] != expected_plan_sha256:
        raise ArchiveHintReconciliationError("expected plan SHA-256 differs from rebuilt plan")
    with transaction(connection), _protected_write_guard(connection):
        protected_before = _protected_counts(connection)
        locked = build_archive_hint_reconciliation_plan(connection, *paths)
        if locked["plan_sha256"] != expected_plan_sha256:
            raise ArchiveHintReconciliationError("reconciliation plan changed before write lock")
        batch_id, existing = _begin_batch(
            connection,
            IMPORTER_NAME,
            locked["plan_sha256"],
            locked["catalog_binding"]["resolved_at"][:10],
            locked["catalog_binding"]["resolved_at"],
        )
        receipt_exists = connection.execute(
            "SELECT 1 FROM archive_hint_reconciliation_imports WHERE import_batch_id = ?",
            (batch_id,),
        ).fetchone() is not None
        allow_insert = existing is None and not receipt_exists
        _verify_rows(connection, locked, batch_id, allow_insert=allow_insert)
        if existing is None:
            _complete_batch(
                connection,
                batch_id,
                locked["catalog_binding"]["resolved_at"],
                locked["statistics"],
            )
        elif existing != locked["statistics"]:
            raise ArchiveHintReconciliationError("completed import statistics differ")
        _verify_completed_batch(connection, locked, batch_id)
        final = build_archive_hint_reconciliation_plan(connection, *paths)
        if final["plan_sha256"] != expected_plan_sha256:
            raise ArchiveHintReconciliationError("reconciliation plan changed during import")
        protected_after = _protected_counts(connection)
        if protected_after != protected_before:
            changed = {
                table: [protected_before[table], protected_after[table]]
                for table in protected_before
                if protected_before[table] != protected_after[table]
            }
            raise ArchiveHintReconciliationError(
                f"reconciliation changed protected catalog tables: {changed}"
            )
        return {
            "import_batch_id": batch_id,
            "replayed": existing is not None,
            **summarize_archive_hint_reconciliation_plan(locked),
        }
