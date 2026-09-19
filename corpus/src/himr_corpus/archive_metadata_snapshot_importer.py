"""Strict admission of sealed public Archive.org metadata snapshots.

This boundary independently rehashes the exact request, response payloads, and
snapshot result before and after the existing metadata-only catalog importer runs.
It has no network or media-download capability and cannot create publication,
gate, biometric, or identity-assertion records.  Provider metadata remains private,
unreviewed discovery evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any

from .db import transaction
from .ids import source_id, stable_id
from .importers import (
    canonical_json,
    combined_digest,
    import_internet_archive,
)


SCHEMA_VERSION = 1
ACCEPT = "application/json"
USER_AGENT = "hidinginmyroom-corpus-archive-metadata/1.0 (public metadata research)"
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
BASES = frozenset({"catalog_archive_item", "catalog_file_hint", "manual_public_lead"})
ALLOWED_HOSTS = frozenset({"archive.org", "www.archive.org"})
MAX_ITEMS = 100
MAX_FILES_PER_ITEM = 200_000
MAX_COLLECTIONS_PER_ITEM = 10_000
MAX_SNAPSHOT_BYTES = 64 * 1024 * 1024
MAX_REQUEST_BYTES = 1024 * 1024
MAX_PAYLOAD_BYTES = 512 * 1024 * 1024

FORBIDDEN_WRITE_TABLES = (
    "media_objects",
    "artifacts",
    "publication_decisions",
    "publication_gate_decisions",
    "publication_manifest_imports",
    "identity_clusters",
    "identity_assertions",
    "identity_cluster_versions",
    "identity_cluster_memberships",
    "identity_cannot_link_decisions",
    "identity_cluster_version_artifacts",
    "identity_cluster_version_review_decisions",
    "biometric_artifacts",
    "identity_assertion_subjects",
    "identity_assertion_decisions",
)


class ArchiveMetadataSnapshotImportError(ValueError):
    pass


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _pretty_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        + "\n"
    ).encode("utf-8")


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _derived_id(prefix: str, value: Any) -> str:
    return f"{prefix}_{_digest(_canonical_bytes(value))[:32]}"


def _exact(value: Any, expected: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ArchiveMetadataSnapshotImportError(f"{label} must be an object")
    missing = sorted(expected - set(value))
    unknown = sorted(set(value) - expected)
    if missing or unknown:
        raise ArchiveMetadataSnapshotImportError(
            f"{label} keys differ from the exact contract; missing={missing}, unknown={unknown}"
        )
    return value


def _timestamp(value: Any, label: str) -> str:
    if not isinstance(value, str) or not UTC_RE.fullmatch(value):
        raise ArchiveMetadataSnapshotImportError(
            f"{label} must be a whole-second UTC timestamp"
        )
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ArchiveMetadataSnapshotImportError(f"{label} is invalid") from error
    return value


def _identifier(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or not IDENTIFIER_RE.fullmatch(value)
        or value in {".", ".."}
    ):
        raise ArchiveMetadataSnapshotImportError(
            f"{label} is not a safe Archive.org identifier"
        )
    return value


def _sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise ArchiveMetadataSnapshotImportError(f"{label} is not a lowercase SHA-256")
    return value


def _integer(
    value: Any, label: str, *, minimum: int = 0, maximum: int | None = None
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ArchiveMetadataSnapshotImportError(f"{label} is not a valid integer")
    if maximum is not None and value > maximum:
        raise ArchiveMetadataSnapshotImportError(f"{label} exceeds {maximum}")
    return value


def _stable_read(path: Path, maximum: int, label: str, *, sealed: bool) -> bytes:
    try:
        before = path.lstat()
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise ArchiveMetadataSnapshotImportError(f"cannot resolve {label}: {error}") from error
    if path != resolved or stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ArchiveMetadataSnapshotImportError(f"{label} must be a resolved regular file")
    if sealed and before.st_mode & 0o222:
        raise ArchiveMetadataSnapshotImportError(f"{label} must be sealed read-only")
    if before.st_size > maximum:
        raise ArchiveMetadataSnapshotImportError(f"{label} exceeds {maximum} bytes")
    try:
        with path.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            identity = (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
            if identity != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns):
                raise ArchiveMetadataSnapshotImportError(f"{label} changed while opening")
            body = handle.read()
            after_fd = os.fstat(handle.fileno())
        after = path.stat()
    except OSError as error:
        raise ArchiveMetadataSnapshotImportError(f"cannot read {label}: {error}") from error
    final_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    fd_identity = (after_fd.st_dev, after_fd.st_ino, after_fd.st_size, after_fd.st_mtime_ns)
    if identity != fd_identity or identity != final_identity:
        raise ArchiveMetadataSnapshotImportError(f"{label} changed while reading")
    return body


def _strict_json_loads(body: bytes, label: str) -> Any:
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ArchiveMetadataSnapshotImportError(f"{label} is not UTF-8 JSON") from error

    def no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, child in pairs:
            if key in value:
                raise ArchiveMetadataSnapshotImportError(
                    f"{label} contains duplicate JSON key {key!r}"
                )
            value[key] = child
        return value

    def no_nonfinite_constant(value: str) -> None:
        raise ArchiveMetadataSnapshotImportError(
            f"{label} contains non-finite JSON constant {value}"
        )

    try:
        return json.loads(
            text,
            object_pairs_hook=no_duplicate_keys,
            parse_constant=no_nonfinite_constant,
        )
    except json.JSONDecodeError as error:
        raise ArchiveMetadataSnapshotImportError(f"{label} is not UTF-8 JSON") from error


def _json(path: Path, maximum: int, label: str, *, sealed: bool) -> tuple[Any, bytes]:
    body = _stable_read(path, maximum, label, sealed=sealed)
    return _strict_json_loads(body, label), body


def _archive_url(value: Any, identifier: str, label: str) -> str:
    if not isinstance(value, str) or len(value) > 4096:
        raise ArchiveMetadataSnapshotImportError(f"{label} must be an HTTPS URL")
    try:
        parsed = urllib.parse.urlsplit(value)
    except ValueError as error:
        raise ArchiveMetadataSnapshotImportError(f"{label} is invalid") from error
    parts = [urllib.parse.unquote(part) for part in parsed.path.split("/") if part]
    if (
        parsed.scheme != "https"
        or (parsed.hostname or "").lower() not in ALLOWED_HOSTS
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parts != ["metadata", identifier]
    ):
        raise ArchiveMetadataSnapshotImportError(
            f"{label} leaves the exact public Archive.org metadata endpoint"
        )
    return value


def _document(payload: bytes, identifier: str) -> dict[str, Any]:
    document = _strict_json_loads(payload, f"Archive.org metadata for {identifier}")
    if not isinstance(document, dict):
        raise ArchiveMetadataSnapshotImportError(
            f"Archive.org metadata for {identifier} must be an object"
        )
    metadata = document.get("metadata")
    files = document.get("files")
    if not isinstance(metadata, dict) or str(metadata.get("identifier") or "") != identifier:
        raise ArchiveMetadataSnapshotImportError(
            f"Archive.org metadata identifier differs for {identifier}"
        )
    if not isinstance(files, list) or len(files) > MAX_FILES_PER_ITEM:
        raise ArchiveMetadataSnapshotImportError(
            f"Archive.org files for {identifier} are missing or exceed the cap"
        )
    names: list[str] = []
    for index, record in enumerate(files):
        if not isinstance(record, dict):
            raise ArchiveMetadataSnapshotImportError(
                f"Archive.org {identifier} files[{index}] must be an object"
            )
        name = record.get("name")
        if not isinstance(name, str) or not name or len(name) > 4096 or "\x00" in name:
            raise ArchiveMetadataSnapshotImportError(
                f"Archive.org {identifier} files[{index}].name is invalid"
            )
        names.append(name)
    if len(names) != len(set(names)):
        raise ArchiveMetadataSnapshotImportError(
            f"Archive.org {identifier} has duplicate file names"
        )
    title = metadata.get("title")
    if not isinstance(title, str):
        title = identifier
    collection = metadata.get("collection")
    if isinstance(collection, str):
        collections = [collection]
    elif isinstance(collection, list):
        collections = sorted(
            {item for item in collection if isinstance(item, str) and item}
        )
    else:
        collections = []
    if len(collections) > MAX_COLLECTIONS_PER_ITEM:
        raise ArchiveMetadataSnapshotImportError(
            f"Archive.org {identifier} collections exceed the cap"
        )
    return {
        "title": title[:1000],
        "collections": collections,
        "file_count": len(files),
    }


def _validate_request(path: Path) -> dict[str, Any]:
    value, body = _json(path, MAX_REQUEST_BYTES, "Archive.org request", sealed=True)
    value = _exact(
        value,
        {"schema_version", "request_id", "request_kind", "requested_at", "items", "policy"},
        "request",
    )
    if value["schema_version"] != 1 or value["request_kind"] != "archive_org_metadata_targets":
        raise ArchiveMetadataSnapshotImportError("unsupported Archive.org request")
    _timestamp(value["requested_at"], "request.requested_at")
    items = value["items"]
    if not isinstance(items, list) or not 1 <= len(items) <= MAX_ITEMS:
        raise ArchiveMetadataSnapshotImportError("request.items must contain 1..100 targets")
    identifiers: list[str] = []
    for index, raw in enumerate(items):
        row = _exact(raw, {"identifier", "basis"}, f"request.items[{index}]")
        identifiers.append(_identifier(row["identifier"], f"request.items[{index}].identifier"))
        if row["basis"] not in BASES:
            raise ArchiveMetadataSnapshotImportError(f"request.items[{index}].basis is invalid")
    if identifiers != sorted(set(identifiers)):
        raise ArchiveMetadataSnapshotImportError("request.items must be unique and sorted")
    expected_policy = {
        "public_unauthenticated_metadata_only": True,
        "media_download": False,
        "cookies_sent": False,
        "authorization_sent": False,
        "publication_authority": False,
    }
    if _exact(value["policy"], set(expected_policy), "request.policy") != expected_policy:
        raise ArchiveMetadataSnapshotImportError("request policy is not metadata-only")
    identity = {key: value[key] for key in value if key != "request_id"}
    if value["request_id"] != _derived_id("iamr", identity):
        raise ArchiveMetadataSnapshotImportError("request_id is inconsistent")
    return {**value, "_sha256": _digest(body), "_byte_count": len(body), "_path": path}


def validate_archive_metadata_snapshot(path: Path) -> dict[str, Any]:
    """Reproduce a sealed snapshot's complete evidence identity."""

    path = Path(path).resolve()
    value, body = _json(path, MAX_SNAPSHOT_BYTES, "Archive.org snapshot", sealed=True)
    value = _exact(
        value,
        {
            "schema_version", "snapshot_id", "snapshot_kind", "observed_at",
            "request", "http_policy", "items", "assertion_policy", "errors",
        },
        "snapshot",
    )
    if (
        body != _pretty_bytes(value)
        or value["schema_version"] != SCHEMA_VERSION
        or value["snapshot_kind"] != "archive_org_item_metadata"
        or value["errors"] != []
    ):
        raise ArchiveMetadataSnapshotImportError(
            "snapshot is not a canonical completed v1 result"
        )
    observed_at = _timestamp(value["observed_at"], "snapshot.observed_at")
    if path.parent.stat().st_mode & 0o222:
        raise ArchiveMetadataSnapshotImportError("snapshot directory must be sealed read-only")

    request_evidence = _exact(
        value["request"],
        {"request_id", "request_sha256", "request_byte_count", "request_file"},
        "snapshot.request",
    )
    if request_evidence["request_file"] != "request.json":
        raise ArchiveMetadataSnapshotImportError("snapshot request filename is invalid")
    request = _validate_request(path.parent / "request.json")
    if (
        request["request_id"] != request_evidence["request_id"]
        or request["_sha256"] != _sha256(request_evidence["request_sha256"], "snapshot.request.request_sha256")
        or request["_byte_count"]
        != _integer(
            request_evidence["request_byte_count"],
            "snapshot.request.request_byte_count",
            minimum=1,
            maximum=MAX_REQUEST_BYTES,
        )
    ):
        raise ArchiveMetadataSnapshotImportError("snapshot request evidence differs")

    expected_http = {
        "method": "GET",
        "accept": ACCEPT,
        "user_agent": USER_AGENT,
        "cookies_sent": False,
        "authorization_sent": False,
    }
    if _exact(value["http_policy"], set(expected_http), "snapshot.http_policy") != expected_http:
        raise ArchiveMetadataSnapshotImportError("snapshot HTTP policy differs")
    expected_assertions = {
        "state": "unreviewed_provider_metadata",
        "provider_fields_are_content_truth": False,
        "media_downloaded": False,
        "identity_assertions": False,
        "publication_authority": False,
    }
    if (
        _exact(value["assertion_policy"], set(expected_assertions), "snapshot.assertion_policy")
        != expected_assertions
    ):
        raise ArchiveMetadataSnapshotImportError("snapshot assertion policy differs")

    items = value["items"]
    if not isinstance(items, list) or len(items) != len(request["items"]):
        raise ArchiveMetadataSnapshotImportError("snapshot item count differs from request")
    normalized_items: list[dict[str, Any]] = []
    for index, (raw, target) in enumerate(zip(items, request["items"], strict=True)):
        label = f"snapshot.items[{index}]"
        row = _exact(
            raw,
            {
                "identifier", "basis", "request_url", "final_url", "http_status",
                "started_at", "observed_at", "content_type", "etag", "last_modified",
                "response_date", "payload_sha256", "byte_count", "payload_file",
                "item_title", "collections", "file_count",
            },
            label,
        )
        identifier = _identifier(row["identifier"], f"{label}.identifier")
        if identifier != target["identifier"] or row["basis"] != target["basis"]:
            raise ArchiveMetadataSnapshotImportError(
                f"{label} order or target differs from the sealed request"
            )
        expected_url = f"https://archive.org/metadata/{urllib.parse.quote(identifier, safe='')}"
        if row["request_url"] != expected_url or row["http_status"] != 200:
            raise ArchiveMetadataSnapshotImportError(f"{label} request/status is invalid")
        _archive_url(row["final_url"], identifier, f"{label}.final_url")
        started = _timestamp(row["started_at"], f"{label}.started_at")
        observed = _timestamp(row["observed_at"], f"{label}.observed_at")
        if (
            datetime.fromisoformat(started.replace("Z", "+00:00"))
            < datetime.fromisoformat(request["requested_at"].replace("Z", "+00:00"))
            or datetime.fromisoformat(observed.replace("Z", "+00:00"))
            < datetime.fromisoformat(started.replace("Z", "+00:00"))
        ):
            raise ArchiveMetadataSnapshotImportError(f"{label} timestamps are nonchronological")
        if not isinstance(row["content_type"], str) or "json" not in row["content_type"].lower() or len(row["content_type"]) > 200:
            raise ArchiveMetadataSnapshotImportError(f"{label}.content_type is invalid")
        for key in ("etag", "last_modified", "response_date"):
            if row[key] is not None and (
                not isinstance(row[key], str) or len(row[key]) > 1000
            ):
                raise ArchiveMetadataSnapshotImportError(f"{label}.{key} is invalid")
        digest = _sha256(row["payload_sha256"], f"{label}.payload_sha256")
        size = _integer(
            row["byte_count"], f"{label}.byte_count", minimum=1, maximum=MAX_PAYLOAD_BYTES
        )
        filename = f"item-{identifier}.metadata.json"
        if row["payload_file"] != filename:
            raise ArchiveMetadataSnapshotImportError(f"{label}.payload_file is invalid")
        payload_path = path.parent / filename
        payload = _stable_read(payload_path, size, f"{label} payload", sealed=True)
        if len(payload) != size or _digest(payload) != digest:
            raise ArchiveMetadataSnapshotImportError(f"{label} payload bytes differ")
        summary = _document(payload, identifier)
        if (
            row["item_title"] != summary["title"]
            or row["collections"] != summary["collections"]
            or row["file_count"] != summary["file_count"]
        ):
            raise ArchiveMetadataSnapshotImportError(f"{label} summary does not reproduce")
        normalized_items.append({**row, "_payload_path": payload_path})

    if observed_at != max(item["observed_at"] for item in normalized_items):
        raise ArchiveMetadataSnapshotImportError(
            "snapshot observation time differs from its responses"
        )
    identity = {
        "request_id": request["request_id"],
        "observed_at": observed_at,
        "items": [
            {
                "identifier": item["identifier"],
                "payload_sha256": item["payload_sha256"],
                "final_url": item["final_url"],
            }
            for item in normalized_items
        ],
    }
    if value["snapshot_id"] != _derived_id("iams", identity) or path.parent.name != value["snapshot_id"]:
        raise ArchiveMetadataSnapshotImportError("snapshot ID/path is inconsistent")
    return {
        **value,
        "items": normalized_items,
        "_path": path,
        "_sha256": _digest(body),
        "_byte_count": len(body),
        "_request": request,
    }


def _table_counts(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        table: int(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
        for table in FORBIDDEN_WRITE_TABLES
    }


def _record_exact_capture_evidence(
    connection: sqlite3.Connection,
    snapshot: dict[str, Any],
    batch_id: str,
    *,
    allow_insert: bool,
) -> int:
    inserted = 0
    for item in snapshot["items"]:
        item_source = source_id("internet_archive", "archive_item", item["identifier"])
        evidence_id = stable_id(
            "ssn", item_source, item["observed_at"], item["payload_sha256"]
        )
        metadata = canonical_json(
            {
                "archive_metadata_snapshot_id": snapshot["snapshot_id"],
                "archive_metadata_snapshot_sha256": snapshot["_sha256"],
                "request_id": snapshot["request"]["request_id"],
                "request_sha256": snapshot["request"]["request_sha256"],
                "request_byte_count": snapshot["request"]["request_byte_count"],
                "capture_started_at": item["started_at"],
                "content_type": item["content_type"],
                "etag": item["etag"],
                "last_modified": item["last_modified"],
                "response_date": item["response_date"],
                "byte_count": item["byte_count"],
                "basis": item["basis"],
                "assertion_state": "unreviewed_provider_metadata",
                "provider_fields_are_content_truth": False,
                "publication_authority": False,
            }
        )
        row = connection.execute(
            """
            SELECT source_id, observed_at, request_url, final_url, http_status,
                   payload_sha256, metadata_json, import_batch_id
            FROM source_snapshots WHERE source_snapshot_id = ?
            """,
            (evidence_id,),
        ).fetchone()
        if row is None:
            if not allow_insert:
                raise ArchiveMetadataSnapshotImportError(
                    "completed Archive.org snapshot replay is missing exact capture evidence"
                )
            before_changes = connection.total_changes
            connection.execute(
                """
                INSERT INTO source_snapshots(
                    source_snapshot_id, source_id, observed_at, request_url, final_url,
                    http_status, payload_sha256, artifact_path, metadata_json,
                    import_batch_id
                ) VALUES(?, ?, ?, ?, ?, 200, ?, ?, ?, ?)
                """,
                (
                    evidence_id,
                    item_source,
                    item["observed_at"],
                    item["request_url"],
                    item["final_url"],
                    item["payload_sha256"],
                    str(item["_payload_path"]),
                    metadata,
                    batch_id,
                ),
            )
            inserted += connection.total_changes - before_changes
            row = connection.execute(
                """
                SELECT source_id, observed_at, request_url, final_url, http_status,
                       payload_sha256, metadata_json, import_batch_id
                FROM source_snapshots WHERE source_snapshot_id = ?
                """,
                (evidence_id,),
            ).fetchone()
        expected = (
            item_source,
            item["observed_at"],
            item["request_url"],
            item["final_url"],
            200,
            item["payload_sha256"],
            metadata,
            batch_id,
        )
        if row is None or tuple(row) != expected:
            raise ArchiveMetadataSnapshotImportError(
                "existing exact Archive.org capture evidence conflicts with this snapshot"
            )
    return inserted


def import_archive_metadata_snapshot(
    connection: sqlite3.Connection, snapshot_path: Path
) -> dict[str, Any]:
    """Atomically admit a validated snapshot into private metadata tables only."""

    snapshot_path = Path(snapshot_path).resolve()
    validated = validate_archive_metadata_snapshot(snapshot_path)
    with transaction(connection):
        before = _table_counts(connection)
        # Revalidate inside the write lock, then let the established Archive.org
        # importer build only metadata-only catalog projections.
        locked = validate_archive_metadata_snapshot(snapshot_path)
        payload_paths = [item["_payload_path"] for item in locked["items"]]
        batch_id = stable_id(
            "imp", "internet_archive_metadata", combined_digest(payload_paths)
        )
        completed_observation = connection.execute(
            """
            SELECT 1 FROM import_observations
            WHERE import_batch_id = ? AND observed_at = ? AND status = 'completed'
            """,
            (batch_id, locked["observed_at"]),
        ).fetchone()
        statistics = import_internet_archive(
            connection,
            payload_paths,
            snapshot_date=locked["observed_at"][:10],
            observed_at=locked["observed_at"],
            manage_transaction=False,
        )
        _record_exact_capture_evidence(
            connection,
            locked,
            batch_id,
            allow_insert=completed_observation is None,
        )
        final = validate_archive_metadata_snapshot(snapshot_path)
        if (
            final["snapshot_id"] != validated["snapshot_id"]
            or final["_sha256"] != validated["_sha256"]
            or final["_request"]["_sha256"] != validated["_request"]["_sha256"]
            or [item["payload_sha256"] for item in final["items"]]
            != [item["payload_sha256"] for item in validated["items"]]
        ):
            raise ArchiveMetadataSnapshotImportError(
                "sealed Archive.org snapshot changed during import"
            )
        after = _table_counts(connection)
        if after != before:
            changed = {
                table: [before[table], after[table]]
                for table in before
                if before[table] != after[table]
            }
            raise ArchiveMetadataSnapshotImportError(
                f"Archive.org metadata import touched forbidden tables: {changed}"
            )
        return {
            "snapshot_id": locked["snapshot_id"],
            "snapshot_sha256": locked["_sha256"],
            "request_id": locked["request"]["request_id"],
            "observed_at": locked["observed_at"],
            "items": len(locked["items"]),
            "exact_capture_evidence_rows": len(locked["items"]),
            "catalog_statistics": statistics,
            "media_downloads": 0,
            "publication_decisions_created": 0,
            "publication_gate_clears_created": 0,
            "identity_assertions_created": 0,
        }
