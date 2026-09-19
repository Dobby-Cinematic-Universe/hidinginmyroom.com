"""Transactional importers for acquisition and media-preprocessing result envelopes.

The acquisition and preprocessing programs deliberately do not open SQLite.  This
module is their strict admission boundary: it validates cross-field consistency,
translates producer-local identifiers where necessary, and writes only private or
unreviewed catalog rows.  It never creates a publication decision.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from . import __version__
from .db import transaction
from .ids import source_id, stable_id
from .importers import (
    _import_observation_id,
    _upsert_source as _upsert_catalog_source,
    canonical_json,
    sha256_bytes,
)
from .private_acquisition import (
    PrivateAcquisitionError,
    load_seal_receipt,
    validate_handling_policy,
    validate_private_acquisition_seal_receipt,
)


RESULT_SCHEMA_VERSION = 1
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ACCESS_STATES = {
    "public",
    "members_only",
    "private",
    "removed",
    "unavailable",
    "unknown",
}
MEDIA_KINDS = {"video", "audio", "image", "subtitle", "document", "other"}


class ResultImportError(ValueError):
    """A result envelope is unsupported or internally inconsistent."""


def _exact_keys(
    value: dict[str, Any],
    label: str,
    required: set[str],
    optional: set[str] | None = None,
) -> None:
    optional = optional or set()
    missing = required - set(value)
    unknown = set(value) - required - optional
    if missing or unknown:
        details: list[str] = []
        if missing:
            details.append(f"missing {sorted(missing)}")
        if unknown:
            details.append(f"unknown {sorted(unknown)}")
        raise ResultImportError(f"{label} has " + "; ".join(details))


def _object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ResultImportError(f"{label} must be an object")
    return value


def _array(value: object, label: str, *, nonempty: bool = False) -> list[Any]:
    if not isinstance(value, list) or (nonempty and not value):
        qualifier = "a non-empty array" if nonempty else "an array"
        raise ResultImportError(f"{label} must be {qualifier}")
    return value


def _string(value: object, label: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or not value:
        raise ResultImportError(f"{label} must be a non-empty string")
    return value


def _integer(value: object, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ResultImportError(f"{label} must be an integer >= {minimum}")
    return value


def _number(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ResultImportError(f"{label} must be a finite number")
    result = float(value)
    if result != result or abs(result) == float("inf"):
        raise ResultImportError(f"{label} must be a finite number")
    return result


def _nullable_string(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _string(value, label)


def _validate_file_uri(path_value: object, uri_value: object, label: str) -> None:
    path_text = _string(path_value, f"{label}.path")
    uri_text = _string(uri_value, f"{label}.storage_uri")
    assert path_text is not None and uri_text is not None
    path = Path(path_text)
    if not path.is_absolute():
        raise ResultImportError(f"{label}.path must be absolute")
    if path.resolve(strict=False).as_uri() != uri_text:
        raise ResultImportError(f"{label}.path and storage_uri disagree")


def _verify_exact_file(
    path_value: object,
    *,
    expected_sha256: str,
    expected_byte_count: int,
    label: str,
    require_sealed: bool = False,
) -> None:
    """Hash a regular local file while detecting replacement or in-place mutation."""

    path_text = _string(path_value, f"{label}.path")
    assert path_text is not None
    path = Path(path_text)
    if require_sealed:
        try:
            link_stat = path.lstat()
        except (FileNotFoundError, OSError) as error:
            raise ResultImportError(f"{label} is not a readable current file: {error}") from error
        if stat.S_ISLNK(link_stat.st_mode) or not stat.S_ISREG(link_stat.st_mode):
            raise ResultImportError(f"{label} must be a regular non-symlink file")
        if link_stat.st_mode & 0o222:
            raise ResultImportError(f"{label} must be sealed read-only")
    if not path.is_file():
        raise ResultImportError(f"{label} is not a readable current file")
    try:
        path_before = path.stat()
        handle = path.open("rb")
    except (FileNotFoundError, OSError) as error:
        raise ResultImportError(f"{label} is not a readable current file: {error}") from error
    try:
        descriptor_before = os.fstat(handle.fileno())
        if not stat.S_ISREG(descriptor_before.st_mode):
            raise ResultImportError(f"{label} must be a regular file")
        if (
            path_before.st_dev,
            path_before.st_ino,
            path_before.st_size,
            path_before.st_mtime_ns,
        ) != (
            descriptor_before.st_dev,
            descriptor_before.st_ino,
            descriptor_before.st_size,
            descriptor_before.st_mtime_ns,
        ):
            raise ResultImportError(f"{label} was replaced while being opened")
        if descriptor_before.st_size != expected_byte_count:
            raise ResultImportError(f"{label} byte_count differs from the result envelope")
        digest = hashlib.sha256()
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
        descriptor_after = os.fstat(handle.fileno())
    finally:
        handle.close()
    try:
        path_after = path.stat()
    except (FileNotFoundError, OSError) as error:
        raise ResultImportError(f"{label} disappeared after verification: {error}") from error
    before_identity = (
        descriptor_before.st_dev,
        descriptor_before.st_ino,
        descriptor_before.st_size,
        descriptor_before.st_mtime_ns,
    )
    after_descriptor_identity = (
        descriptor_after.st_dev,
        descriptor_after.st_ino,
        descriptor_after.st_size,
        descriptor_after.st_mtime_ns,
    )
    after_path_identity = (
        path_after.st_dev,
        path_after.st_ino,
        path_after.st_size,
        path_after.st_mtime_ns,
    )
    if before_identity != after_descriptor_identity or before_identity != after_path_identity:
        raise ResultImportError(f"{label} changed while it was being verified")
    if digest.hexdigest() != expected_sha256:
        raise ResultImportError(f"{label} SHA-256 differs from the result envelope")


def _read_exact_json_file(
    path_value: object,
    *,
    expected_sha256: str,
    expected_byte_count: int,
    label: str,
) -> dict[str, Any]:
    if expected_byte_count > 32 * 1024 * 1024:
        raise ResultImportError(f"{label} exceeds the bounded JSON artifact size")
    _verify_exact_file(
        path_value,
        expected_sha256=expected_sha256,
        expected_byte_count=expected_byte_count,
        label=label,
        require_sealed=True,
    )
    path_text = _string(path_value, f"{label}.path")
    assert path_text is not None
    try:
        body = Path(path_text).read_bytes()
    except OSError as error:
        raise ResultImportError(f"{label} cannot be read: {error}") from error
    if len(body) != expected_byte_count or sha256_bytes(body) != expected_sha256:
        raise ResultImportError(f"{label} changed after verification")
    try:
        return _object(json.loads(body), label)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResultImportError(f"{label} is invalid UTF-8 JSON") from error


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise ResultImportError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _schema_v1(value: object, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value != RESULT_SCHEMA_VERSION:
        raise ResultImportError(f"{label} must equal integer {RESULT_SCHEMA_VERSION}")


def _media_id(digest: str) -> str:
    return f"media_sha256_{digest}"


def _producer_stable_id(prefix: str, *parts: object) -> str:
    digest = sha256_bytes(canonical_json(list(parts)).encode("utf-8"))
    return f"{prefix}_{digest[:32]}"


def _validate_preprocess_tool_provenance(
    value: object, label: str, expected_name: str
) -> dict[str, Any]:
    tool = _object(value, label)
    _exact_keys(
        tool,
        label,
        {
            "name",
            "executable_sha256",
            "executable_byte_count",
            "version",
            "version_output",
            "version_output_sha256",
            "build_configuration",
        },
    )
    if tool.get("name") != expected_name:
        raise ResultImportError(f"{label}.name must equal {expected_name}")
    _sha256(tool.get("executable_sha256"), f"{label}.executable_sha256")
    _integer(tool.get("executable_byte_count"), f"{label}.executable_byte_count", minimum=1)
    version = _string(tool.get("version"), f"{label}.version")
    version_output = _string(tool.get("version_output"), f"{label}.version_output")
    assert version is not None and version_output is not None
    if version_output.splitlines()[0].strip() != version:
        raise ResultImportError(f"{label}.version must be the first build-output line")
    version_output_sha256 = _sha256(
        tool.get("version_output_sha256"), f"{label}.version_output_sha256"
    )
    if sha256_bytes(version_output.encode("utf-8")) != version_output_sha256:
        raise ResultImportError(f"{label}.version_output_sha256 is inconsistent")
    _nullable_string(tool.get("build_configuration"), f"{label}.build_configuration")
    return tool


def _timestamp(value: object, label: str) -> str:
    text = _string(value, label)
    assert text is not None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise ResultImportError(f"{label} must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None:
        raise ResultImportError(f"{label} must include a UTC offset")
    normalized = parsed.astimezone(timezone.utc)
    if normalized.microsecond:
        return normalized.isoformat(timespec="microseconds").replace("+00:00", "Z")
    return normalized.isoformat(timespec="seconds").replace("+00:00", "Z")


def _optional_timestamp(value: object, label: str) -> str | None:
    return None if value is None else _timestamp(value, label)


def _timestamp_value(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _result_file(path: str | Path) -> tuple[dict[str, Any], str, str]:
    result_path = Path(path)
    try:
        body = result_path.read_bytes()
        value = json.loads(body)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResultImportError(f"Cannot read result JSON {result_path}: {error}") from error
    result = _object(value, "result")
    digest = sha256_bytes(canonical_json(result).encode("utf-8"))
    return result, digest, sha256_bytes(body)


def _require_completed_envelope(result: dict[str, Any], label: str) -> tuple[str, str]:
    _schema_v1(result.get("schema_version"), f"{label}.schema_version")
    if result.get("status") != "completed" or result.get("dry_run") is not False:
        raise ResultImportError(f"{label} must be a completed, non-dry-run result")
    errors = _array(result.get("errors"), f"{label}.errors")
    if errors:
        raise ResultImportError(f"{label}.errors must be empty for completed ingestion")
    started_at = _timestamp(result.get("started_at"), f"{label}.started_at")
    completed_at = _timestamp(result.get("completed_at"), f"{label}.completed_at")
    if _timestamp_value(completed_at) < _timestamp_value(started_at):
        raise ResultImportError(f"{label}.completed_at precedes started_at")
    _string(result.get("job_id"), f"{label}.job_id")
    return started_at, completed_at


def _validate_probe_identity(
    probe: object,
    *,
    digest: str,
    byte_count: int,
    media_id: str,
    label: str,
) -> None:
    value = _object(probe, label)
    media = value.get("media")
    if media is None:
        return
    media_value = _object(media, f"{label}.media")
    if media_value.get("sha256") != digest:
        raise ResultImportError(f"{label}.media.sha256 disagrees with its media object")
    if media_value.get("media_id") != media_id:
        raise ResultImportError(f"{label}.media.media_id disagrees with its media object")
    if media_value.get("byte_count") != byte_count:
        raise ResultImportError(f"{label}.media.byte_count disagrees with its media object")


def _validate_media_row(
    row_value: object,
    label: str,
    *,
    expected_sha256: str | None = None,
    expected_byte_count: int | None = None,
) -> dict[str, Any]:
    row = _object(row_value, label)
    _exact_keys(
        row,
        label,
        {
            "media_id",
            "sha256",
            "byte_count",
            "media_kind",
            "mime_type",
            "container",
            "duration_ms",
            "ffprobe_json",
            "first_cataloged_at",
            "integrity_state",
        },
    )
    digest = _sha256(row.get("sha256"), f"{label}.sha256")
    media_id = _media_id(digest)
    if row.get("media_id") != media_id:
        raise ResultImportError(f"{label}.media_id must be derived from sha256")
    byte_count = _integer(row.get("byte_count"), f"{label}.byte_count")
    if expected_sha256 is not None and digest != expected_sha256:
        raise ResultImportError(f"{label}.sha256 disagrees with the enclosing result")
    if expected_byte_count is not None and byte_count != expected_byte_count:
        raise ResultImportError(f"{label}.byte_count disagrees with the enclosing result")
    if row.get("media_kind") not in MEDIA_KINDS:
        raise ResultImportError(f"{label}.media_kind is unsupported")
    _nullable_string(row.get("mime_type"), f"{label}.mime_type")
    _nullable_string(row.get("container"), f"{label}.container")
    if row.get("duration_ms") is not None:
        _integer(row.get("duration_ms"), f"{label}.duration_ms")
    first_cataloged_at = _timestamp(
        row.get("first_cataloged_at"), f"{label}.first_cataloged_at"
    )
    if row.get("integrity_state") != "verified":
        raise ResultImportError(f"{label}.integrity_state must be verified")
    if row.get("ffprobe_json") is not None:
        _validate_probe_identity(
            row["ffprobe_json"],
            digest=digest,
            byte_count=byte_count,
            media_id=media_id,
            label=f"{label}.ffprobe_json",
        )
    normalized = dict(row)
    normalized["first_cataloged_at"] = first_cataloged_at
    return normalized


def validate_acquisition_result(result: object) -> dict[str, Any]:
    """Validate an acquisition result and all producer catalog cross-references."""

    envelope = _object(result, "acquisition result")
    _exact_keys(
        envelope,
        "acquisition result",
        {
            "schema_version",
            "job_id",
            "adapter",
            "status",
            "dry_run",
            "reused",
            "work_order_sha256",
            "started_at",
            "completed_at",
            "duration_ms",
            "source",
            "limits",
            "capacity_before",
            "capacity_after",
            "commands",
            "source_observation",
            "selected_remote_metadata",
            "admission",
            "catalog_records",
            "result_path",
            "errors",
        },
        {"reuse_verified_at", "handling_policy"},
    )
    started_at, completed_at = _require_completed_envelope(
        envelope, "acquisition result"
    )
    if envelope.get("adapter") not in {"local_file", "direct_http", "yt_dlp"}:
        raise ResultImportError("acquisition result.adapter is unsupported")
    policy: dict[str, str] | None = None
    if "handling_policy" in envelope:
        try:
            policy = validate_handling_policy(
                envelope["handling_policy"], "acquisition result.handling_policy"
            )
        except PrivateAcquisitionError as error:
            raise ResultImportError(str(error)) from error
        if envelope.get("adapter") != "local_file":
            raise ResultImportError(
                "acquisition handling_policy is supported only for local_file results"
            )
    if not isinstance(envelope.get("reused"), bool):
        raise ResultImportError("acquisition result.reused must be boolean")
    if envelope.get("reuse_verified_at") is not None:
        _timestamp(envelope["reuse_verified_at"], "acquisition result.reuse_verified_at")
    _integer(envelope.get("duration_ms"), "acquisition result.duration_ms")
    _string(envelope.get("result_path"), "acquisition result.result_path")
    _sha256(
        envelope.get("work_order_sha256"),
        "acquisition result.work_order_sha256",
    )
    source = _object(envelope.get("source"), "acquisition result.source")
    _exact_keys(
        source,
        "acquisition result.source",
        {
            "platform",
            "source_kind",
            "native_id",
            "canonical_url",
            "title",
            "published_at",
            "access_state",
        },
    )
    for key in ("platform", "source_kind", "native_id"):
        _string(source.get(key), f"acquisition result.source.{key}")
    if source.get("access_state") not in ACCESS_STATES:
        raise ResultImportError("acquisition result.source.access_state is unsupported")
    if envelope.get("adapter") == "local_file" and source.get("access_state") != "unknown":
        raise ResultImportError(
            "local_file acquisition source.access_state must be unknown; local "
            "possession is not independent public-access evidence"
        )
    _nullable_string(source.get("canonical_url"), "acquisition result.source.canonical_url")
    _nullable_string(source.get("title"), "acquisition result.source.title")
    if source.get("published_at") is not None:
        _timestamp(source["published_at"], "acquisition result.source.published_at")
    limits = _object(envelope.get("limits"), "acquisition result.limits")
    _exact_keys(
        limits,
        "acquisition result.limits",
        {"max_job_bytes", "global_cache_cap_bytes", "free_space_floor_bytes"},
    )
    for key in limits:
        _integer(limits[key], f"acquisition result.limits.{key}", minimum=1 if key != "free_space_floor_bytes" else 0)
    for capacity_name in ("capacity_before", "capacity_after"):
        capacity = _object(envelope.get(capacity_name), f"acquisition result.{capacity_name}")
        _exact_keys(
            capacity,
            f"acquisition result.{capacity_name}",
            {
                "filesystem_path",
                "managed_bytes",
                "free_bytes",
                "reserve_bytes",
                "projected_managed_bytes",
                "projected_free_bytes",
                "global_cache_cap_bytes",
                "free_space_floor_bytes",
            },
        )
        _string(capacity["filesystem_path"], f"acquisition result.{capacity_name}.filesystem_path")
        for key, value in capacity.items():
            if key != "filesystem_path":
                _integer(value, f"acquisition result.{capacity_name}.{key}", minimum=0)
    commands = _array(envelope.get("commands"), "acquisition result.commands")
    for command_index, command_value in enumerate(commands):
        command = _array(command_value, f"acquisition result.commands[{command_index}]", nonempty=True)
        for argument_index, argument in enumerate(command):
            _string(argument, f"acquisition result.commands[{command_index}][{argument_index}]")
    _object(envelope.get("source_observation"), "acquisition result.source_observation")
    _object(envelope.get("selected_remote_metadata"), "acquisition result.selected_remote_metadata")

    admission = _object(envelope.get("admission"), "acquisition result.admission")
    _exact_keys(
        admission,
        "acquisition result.admission",
        {"media_id", "sha256", "byte_count", "path", "storage_uri", "normalized_probe"},
    )
    digest = _sha256(admission.get("sha256"), "acquisition result.admission.sha256")
    byte_count = _integer(
        admission.get("byte_count"), "acquisition result.admission.byte_count", minimum=1
    )
    media_id = _media_id(digest)
    if admission.get("media_id") != media_id:
        raise ResultImportError("acquisition result.admission.media_id must derive from sha256")
    _string(admission.get("path"), "acquisition result.admission.path")
    storage_uri = _string(
        admission.get("storage_uri"), "acquisition result.admission.storage_uri"
    )
    _validate_file_uri(
        admission.get("path"), admission.get("storage_uri"), "acquisition result.admission"
    )
    _validate_probe_identity(
        admission.get("normalized_probe"),
        digest=digest,
        byte_count=byte_count,
        media_id=media_id,
        label="acquisition result.admission.normalized_probe",
    )

    records = _object(
        envelope.get("catalog_records"), "acquisition result.catalog_records"
    )
    _exact_keys(
        records,
        "acquisition result.catalog_records",
        {"sources", "media_objects", "media_locations", "media_sources"},
    )
    source_rows = _array(
        records.get("sources"), "acquisition result.catalog_records.sources", nonempty=True
    )
    media_rows = _array(
        records.get("media_objects"),
        "acquisition result.catalog_records.media_objects",
        nonempty=True,
    )
    location_rows = _array(
        records.get("media_locations"),
        "acquisition result.catalog_records.media_locations",
        nonempty=True,
    )
    media_source_rows = _array(
        records.get("media_sources"),
        "acquisition result.catalog_records.media_sources",
        nonempty=True,
    )
    if not all(len(rows) == 1 for rows in (source_rows, media_rows, location_rows, media_source_rows)):
        raise ResultImportError("acquisition result-v1 must contain one row of each catalog type")

    source_row = _object(source_rows[0], "acquisition catalog source")
    _exact_keys(
        source_row,
        "acquisition catalog source",
        {
            "source_id",
            "platform",
            "source_kind",
            "native_id",
            "parent_source_id",
            "canonical_url",
            "historical_url",
            "title",
            "published_at",
            "observed_at",
            "access_state",
            "review_state",
            "metadata_json",
            "created_at",
            "updated_at",
        },
    )
    producer_source_id = _string(source_row.get("source_id"), "acquisition catalog source.source_id")
    expected_producer_source_id = _producer_stable_id(
        "source", source["platform"], source["source_kind"], source["native_id"]
    )
    if producer_source_id != expected_producer_source_id:
        raise ResultImportError("acquisition producer source_id is not deterministic")
    for key in (
        "platform",
        "source_kind",
        "native_id",
        "canonical_url",
        "title",
        "published_at",
        "access_state",
    ):
        if source_row.get(key) != source.get(key):
            raise ResultImportError(f"acquisition catalog source.{key} disagrees with source")
    if source_row.get("review_state") != "metadata_only":
        raise ResultImportError("acquisition catalog source must remain metadata_only")
    if source_row.get("parent_source_id") is not None:
        raise ResultImportError("acquisition result-v1 source parent must be null")
    _nullable_string(source_row.get("historical_url"), "acquisition catalog source.historical_url")
    source_metadata = _object(
        source_row.get("metadata_json"), "acquisition catalog source.metadata_json"
    )
    embedded_policy = source_metadata.get("handling_policy")
    if policy is None and embedded_policy is not None:
        raise ResultImportError(
            "acquisition source metadata may not introduce a handling_policy absent "
            "from the result envelope"
        )
    if policy is not None:
        expected_source_metadata = {
            "acquisition_adapter": envelope["adapter"],
            "selected_remote_metadata": envelope["selected_remote_metadata"],
            "handling_policy": policy,
        }
        if canonical_json(source_metadata) != canonical_json(expected_source_metadata):
            raise ResultImportError(
                "acquisition source metadata must preserve the exact result "
                "handling_policy without additional or missing fields"
            )
    if _timestamp(source_row.get("observed_at"), "acquisition catalog source.observed_at") != completed_at:
        raise ResultImportError("acquisition source observation must equal completion time")
    if _timestamp(source_row.get("created_at"), "acquisition catalog source.created_at") != completed_at:
        raise ResultImportError("acquisition source created_at must equal completion time")
    if _timestamp(source_row.get("updated_at"), "acquisition catalog source.updated_at") != completed_at:
        raise ResultImportError("acquisition source updated_at must equal completion time")

    media_row = _validate_media_row(
        media_rows[0],
        "acquisition catalog media object",
        expected_sha256=digest,
        expected_byte_count=byte_count,
    )
    if media_row["first_cataloged_at"] != completed_at:
        raise ResultImportError("acquisition first_cataloged_at must equal admission completion")
    if canonical_json(media_row.get("ffprobe_json")) != canonical_json(admission["normalized_probe"]):
        raise ResultImportError("acquisition probe differs between admission and media object")

    location = _object(location_rows[0], "acquisition catalog media location")
    _exact_keys(
        location,
        "acquisition catalog media location",
        {"media_location_id", "media_id", "storage_uri", "storage_class", "verified_at", "is_primary"},
    )
    if location.get("media_id") != media_id or location.get("storage_uri") != storage_uri:
        raise ResultImportError("acquisition media location disagrees with admission")
    _string(location.get("media_location_id"), "acquisition media location.media_location_id")
    if isinstance(location.get("is_primary"), bool) or location.get("is_primary") not in (0, 1):
        raise ResultImportError("acquisition media location.is_primary must be 0 or 1")
    _string(location.get("storage_class"), "acquisition media location.storage_class")
    if _timestamp(location.get("verified_at"), "acquisition media location.verified_at") != completed_at:
        raise ResultImportError("acquisition media location verification must equal completion time")
    expected_location_id = _producer_stable_id(
        "media_location", media_id, location["storage_uri"]
    )
    if location["media_location_id"] != expected_location_id:
        raise ResultImportError("acquisition producer media_location_id is not deterministic")

    media_source = _object(media_source_rows[0], "acquisition catalog media source")
    _exact_keys(
        media_source,
        "acquisition catalog media source",
        {
            "media_source_id",
            "media_id",
            "source_id",
            "retrieved_at",
            "retrieval_tool",
            "retrieval_tool_version",
            "source_snapshot_id",
        },
    )
    if media_source.get("media_id") != media_id or media_source.get("source_id") != producer_source_id:
        raise ResultImportError("acquisition media source references disagree with catalog rows")
    if _timestamp(media_source.get("retrieved_at"), "acquisition media source.retrieved_at") != completed_at:
        raise ResultImportError("acquisition retrieved_at must equal retrieval completion")
    _string(media_source.get("retrieval_tool"), "acquisition media source.retrieval_tool")
    _nullable_string(media_source.get("retrieval_tool_version"), "acquisition media source.retrieval_tool_version")
    if media_source.get("source_snapshot_id") is not None:
        _string(media_source["source_snapshot_id"], "acquisition media source.source_snapshot_id")
    expected_media_source_id = _producer_stable_id(
        "media_source", media_id, producer_source_id
    )
    if media_source.get("media_source_id") != expected_media_source_id:
        raise ResultImportError("acquisition producer media_source_id is not deterministic")

    normalized = dict(envelope)
    normalized["started_at"] = started_at
    normalized["completed_at"] = completed_at
    if policy is not None:
        normalized["handling_policy"] = policy
    return normalized


def _validate_preprocess_routing(
    routing_value: object,
    *,
    media_id: str,
    source_duration_ms: int | None,
    has_video: bool,
    has_audio: bool,
    profile: dict[str, Any],
) -> dict[str, Any]:
    routing = _object(routing_value, "preprocess routing")
    _exact_keys(
        routing,
        "preprocess routing",
        {
            "schema_version",
            "parameters",
            "coverage",
            "scene_changes",
            "silence_intervals",
            "summary",
            "routing_candidates",
            "warning",
            "source_media_id",
        },
    )
    _schema_v1(routing.get("schema_version"), "preprocess routing.schema_version")
    if routing.get("source_media_id") != media_id:
        raise ResultImportError("preprocess routing identity disagrees with input media")

    coverage = _object(routing.get("coverage"), "preprocess routing.coverage")
    _exact_keys(
        coverage,
        "preprocess routing.coverage",
        {"duration_ms", "has_video", "has_audio"},
    )
    duration_ms = _integer(
        coverage.get("duration_ms"), "preprocess routing.coverage.duration_ms"
    )
    if duration_ms != source_duration_ms:
        raise ResultImportError(
            "preprocess routing coverage duration disagrees with source media"
        )
    for key, expected in (("has_video", has_video), ("has_audio", has_audio)):
        if coverage.get(key) is not expected:
            raise ResultImportError(
                f"preprocess routing.coverage.{key} disagrees with source streams"
            )

    parameters = _object(routing.get("parameters"), "preprocess routing.parameters")
    _exact_keys(
        parameters,
        "preprocess routing.parameters",
        {
            "scene_threshold_percent",
            "silence_noise_db",
            "silence_min_duration_ms",
            "near_silent_fraction",
        },
    )
    _number(
        parameters["scene_threshold_percent"],
        "preprocess routing.parameters.scene_threshold_percent",
    )
    _number(
        parameters["silence_noise_db"],
        "preprocess routing.parameters.silence_noise_db",
    )
    _integer(
        parameters["silence_min_duration_ms"],
        "preprocess routing.parameters.silence_min_duration_ms",
        minimum=1,
    )
    near_silent = _number(
        parameters["near_silent_fraction"],
        "preprocess routing.parameters.near_silent_fraction",
    )
    if not 0 <= near_silent <= 1:
        raise ResultImportError("preprocess routing near_silent_fraction is out of range")
    expected_parameters = {
        "scene_threshold_percent": profile["scene_threshold_percent"],
        "silence_noise_db": profile["silence_noise_db"],
        "silence_min_duration_ms": profile["silence_min_duration_ms"],
        "near_silent_fraction": profile["near_silent_fraction"],
    }
    if canonical_json(parameters) != canonical_json(expected_parameters):
        raise ResultImportError("preprocess routing parameters disagree with recipe profile")

    scenes = _array(routing.get("scene_changes"), "preprocess routing.scene_changes")
    previous_timestamp: int | None = None
    for index, scene in enumerate(scenes):
        scene_row = _object(scene, f"preprocess routing.scene_changes[{index}]")
        _exact_keys(
            scene_row,
            f"preprocess routing.scene_changes[{index}]",
            {"timestamp_ms", "score_percent"},
        )
        timestamp_ms = _integer(scene_row.get("timestamp_ms"), "scene timestamp_ms")
        score = _number(scene_row.get("score_percent"), "scene score_percent")
        if timestamp_ms > duration_ms or not 0 <= score <= 100:
            raise ResultImportError("preprocess scene-change candidate is out of range")
        if previous_timestamp is not None and timestamp_ms <= previous_timestamp:
            raise ResultImportError(
                "preprocess scene-change candidates must be strictly sorted and unique"
            )
        previous_timestamp = timestamp_ms
    if scenes and not has_video:
        raise ResultImportError("preprocess scene-change candidates require video")

    silences = _array(
        routing.get("silence_intervals"), "preprocess routing.silence_intervals"
    )
    previous_end: int | None = None
    for index, silence in enumerate(silences):
        silence_row = _object(
            silence, f"preprocess routing.silence_intervals[{index}]"
        )
        _exact_keys(
            silence_row,
            f"preprocess routing.silence_intervals[{index}]",
            {"start_ms", "end_ms", "duration_ms"},
        )
        start_ms = _integer(silence_row.get("start_ms"), "silence start_ms")
        end_ms = _integer(silence_row.get("end_ms"), "silence end_ms")
        interval_duration = _integer(
            silence_row.get("duration_ms"), "silence duration_ms"
        )
        if (
            end_ms <= start_ms
            or end_ms > duration_ms
            or end_ms - start_ms != interval_duration
        ):
            raise ResultImportError("preprocess silence interval is inconsistent")
        if previous_end is not None and start_ms < previous_end:
            raise ResultImportError(
                "preprocess silence intervals must be sorted, unique, and nonoverlapping"
            )
        previous_end = end_ms
    if silences and not has_audio:
        raise ResultImportError("preprocess silence intervals require audio")

    summary = _object(routing.get("summary"), "preprocess routing.summary")
    _exact_keys(
        summary,
        "preprocess routing.summary",
        {
            "scene_change_count",
            "silence_interval_count",
            "silent_duration_ms",
            "silent_fraction",
        },
    )
    _integer(
        summary.get("scene_change_count"),
        "preprocess routing.summary.scene_change_count",
    )
    _integer(
        summary.get("silence_interval_count"),
        "preprocess routing.summary.silence_interval_count",
    )
    silent_ms = sum(row["duration_ms"] for row in silences)
    silent_ms = min(silent_ms, duration_ms)
    expected_summary = {
        "scene_change_count": len(scenes),
        "silence_interval_count": len(silences),
        "silent_duration_ms": silent_ms if has_audio else None,
        "silent_fraction": (
            round(silent_ms / duration_ms, 6)
            if has_audio and duration_ms > 0
            else None
        ),
    }
    if summary.get("silent_duration_ms") is not None:
        _integer(
            summary["silent_duration_ms"],
            "preprocess routing.summary.silent_duration_ms",
        )
    if summary.get("silent_fraction") is not None:
        silent_fraction = _number(
            summary["silent_fraction"],
            "preprocess routing.summary.silent_fraction",
        )
        if not 0 <= silent_fraction <= 1:
            raise ResultImportError("preprocess routing silent_fraction is out of range")
    if canonical_json(summary) != canonical_json(expected_summary):
        raise ResultImportError(
            "preprocess routing summary disagrees with exact interval arithmetic"
        )

    routes = _object(
        routing.get("routing_candidates"), "preprocess routing.routing_candidates"
    )
    _exact_keys(
        routes,
        "preprocess routing.routing_candidates",
        {"asr", "ocr", "visual", "diarization", "active_speaker"},
    )
    for key, value in routes.items():
        _string(value, f"preprocess routing.routing_candidates.{key}")
    _string(routing.get("warning"), "preprocess routing.warning")
    return routing


def validate_preprocess_result(result: object) -> dict[str, Any]:
    """Validate a completed media-preprocess result and its catalog handoff."""

    envelope = _object(result, "preprocess result")
    _exact_keys(
        envelope,
        "preprocess result",
        {
            "schema_version",
            "job_id",
            "status",
            "dry_run",
            "duration_ms",
            "processing_run",
            "input",
            "layout",
            "steps",
            "artifacts",
            "routing",
            "reuse",
            "catalog_records",
            "errors",
            "result_path",
        },
    )
    _schema_v1(envelope.get("schema_version"), "preprocess result.schema_version")
    if envelope.get("status") != "completed" or envelope.get("dry_run") is not False:
        raise ResultImportError("preprocess result must be a completed, non-dry-run result")
    if _array(envelope.get("errors"), "preprocess result.errors"):
        raise ResultImportError("preprocess result.errors must be empty")
    _string(envelope.get("job_id"), "preprocess result.job_id")
    _integer(envelope.get("duration_ms"), "preprocess result.duration_ms")
    _string(envelope.get("result_path"), "preprocess result.result_path")

    run = _object(envelope.get("processing_run"), "preprocess result.processing_run")
    _exact_keys(
        run,
        "preprocess result.processing_run",
        {
            "processing_run_id",
            "stage",
            "implementation_version",
            "parameters_json",
            "environment_json",
            "started_at",
            "completed_at",
            "status",
        },
    )
    if run.get("stage") != "media_preprocess" or run.get("status") != "completed":
        raise ResultImportError("preprocess processing_run must be completed media_preprocess")
    run_id = _string(run.get("processing_run_id"), "preprocess processing_run.processing_run_id")
    implementation_version = _string(
        run.get("implementation_version"), "preprocess run.implementation_version"
    )
    parameters = _object(run.get("parameters_json"), "preprocess run.parameters_json")
    _exact_keys(
        parameters,
        "preprocess run.parameters_json",
        {"contract_version", "implementation_version", "operations", "profile", "tools"},
    )
    _schema_v1(parameters.get("contract_version"), "preprocess parameters.contract_version")
    if parameters.get("implementation_version") != implementation_version:
        raise ResultImportError("preprocess implementation versions disagree")
    operations = _object(parameters.get("operations"), "preprocess parameters.operations")
    _exact_keys(operations, "preprocess parameters.operations", {"probe", "audio_flac", "proxy", "routing"})
    for operation_name, enabled in operations.items():
        if not isinstance(enabled, bool):
            raise ResultImportError(f"preprocess operation {operation_name} must be boolean")
    if operations["probe"] is not True:
        raise ResultImportError("preprocess probe operation must be enabled")
    profile = _object(parameters.get("profile"), "preprocess parameters.profile")
    expected_profile_keys = {
        "profile_id", "ffmpeg_threads", "audio_sample_rate_hz", "audio_channels",
        "audio_sample_format", "flac_compression_level", "proxy_width", "proxy_height",
        "proxy_fps", "proxy_video_codec", "proxy_preset", "proxy_crf",
        "proxy_audio_codec", "proxy_audio_bitrate", "scene_threshold_percent",
        "silence_noise_db", "silence_min_duration_ms", "near_silent_fraction",
    }
    _exact_keys(profile, "preprocess parameters.profile", expected_profile_keys)
    tools = _object(parameters.get("tools"), "preprocess parameters.tools")
    _exact_keys(tools, "preprocess parameters.tools", {"ffmpeg", "ffprobe"})
    _validate_preprocess_tool_provenance(tools.get("ffmpeg"), "preprocess tool ffmpeg", "ffmpeg")
    _validate_preprocess_tool_provenance(tools.get("ffprobe"), "preprocess tool ffprobe", "ffprobe")
    environment = _object(run.get("environment_json"), "preprocess run.environment_json")
    _exact_keys(
        environment,
        "preprocess run.environment_json",
        {"platform", "python", "cpu_only", "execution_nonce", "tool_paths"},
    )
    _string(environment.get("platform"), "preprocess environment.platform")
    _string(environment.get("python"), "preprocess environment.python")
    if environment.get("cpu_only") is not True:
        raise ResultImportError("preprocess environment.cpu_only must be true")
    execution_nonce = _string(
        environment.get("execution_nonce"), "preprocess environment.execution_nonce"
    )
    assert execution_nonce is not None
    if not re.fullmatch(r"[0-9a-f]{32}", execution_nonce):
        raise ResultImportError("preprocess execution_nonce must be 32 lowercase hex characters")
    tool_paths = _object(environment.get("tool_paths"), "preprocess environment.tool_paths")
    _exact_keys(tool_paths, "preprocess environment.tool_paths", {"ffmpeg", "ffprobe"})
    for tool_name, tool_path in tool_paths.items():
        path_text = _string(tool_path, f"preprocess environment.tool_paths.{tool_name}")
        assert path_text is not None
        if not Path(path_text).is_absolute():
            raise ResultImportError(f"preprocess tool path {tool_name} must be absolute")
    started_at = _timestamp(run.get("started_at"), "preprocess run.started_at")
    completed_at = _timestamp(run.get("completed_at"), "preprocess run.completed_at")
    if _timestamp_value(completed_at) < _timestamp_value(started_at):
        raise ResultImportError("preprocess run completed_at precedes started_at")

    input_row = _object(envelope.get("input"), "preprocess result.input")
    _exact_keys(
        input_row,
        "preprocess result.input",
        {
            "path",
            "storage_uri",
            "media_id",
            "sha256",
            "byte_count",
            "stat_before",
            "stat_after",
            "unchanged",
            "catalog_observation",
        },
    )
    digest = _sha256(input_row.get("sha256"), "preprocess input.sha256")
    byte_count = _integer(input_row.get("byte_count"), "preprocess input.byte_count")
    media_id = _media_id(digest)
    if input_row.get("media_id") != media_id:
        raise ResultImportError("preprocess input.media_id must derive from sha256")
    if input_row.get("unchanged") is not True:
        raise ResultImportError("preprocess input must be unchanged across the run")
    _string(input_row.get("path"), "preprocess input.path")
    _string(input_row.get("storage_uri"), "preprocess input.storage_uri")
    _validate_file_uri(
        input_row.get("path"), input_row.get("storage_uri"), "preprocess result.input"
    )
    for stat_name in ("stat_before", "stat_after"):
        stat = _object(input_row.get(stat_name), f"preprocess input.{stat_name}")
        _exact_keys(
            stat,
            f"preprocess input.{stat_name}",
            {"device", "inode", "byte_count", "mtime_ns"},
        )
        for key, value in stat.items():
            _integer(value, f"preprocess input.{stat_name}.{key}")
        if stat["byte_count"] != byte_count:
            raise ResultImportError(f"preprocess input.{stat_name}.byte_count disagrees with input")
    if input_row["stat_before"] != input_row["stat_after"]:
        raise ResultImportError("preprocess input stat changed despite unchanged=true")
    catalog_observation = _object(
        input_row.get("catalog_observation"), "preprocess input.catalog_observation"
    )
    _exact_keys(
        catalog_observation,
        "preprocess input.catalog_observation",
        {"first_cataloged_at", "basis", "acquisition_timestamp_state"},
    )
    first_cataloged_at = _timestamp(
        catalog_observation.get("first_cataloged_at"),
        "preprocess input.catalog_observation.first_cataloged_at",
    )
    if catalog_observation.get("basis") not in {
        "upstream_work_order",
        "preprocessing_observation",
    }:
        raise ResultImportError("preprocess catalog observation basis is unsupported")
    if catalog_observation.get("acquisition_timestamp_state") != "not_claimed_by_preprocessing":
        raise ResultImportError("preprocessing must not claim an acquisition timestamp")

    layout = _object(envelope.get("layout"), "preprocess result.layout")
    _exact_keys(
        layout,
        "preprocess result.layout",
        {"output_root", "object_dir", "recipe_dir", "run_dir", "recipe_id", "recipe_sha256"},
    )
    for key in ("output_root", "object_dir", "recipe_dir", "run_dir"):
        _string(layout.get(key), f"preprocess result.layout.{key}")
    _sha256(layout.get("recipe_sha256"), "preprocess result.layout.recipe_sha256")
    expected_recipe_sha256 = sha256_bytes(
        canonical_json(run["parameters_json"]).encode("utf-8")
    )
    if layout["recipe_sha256"] != expected_recipe_sha256:
        raise ResultImportError("preprocess recipe_sha256 disagrees with processing parameters")
    expected_recipe_id = f"recipe_preprocess_{expected_recipe_sha256[:32]}"
    if layout.get("recipe_id") != expected_recipe_id:
        raise ResultImportError("preprocess recipe_id disagrees with recipe_sha256")
    expected_run_id = _producer_stable_id(
        "run_preprocess", digest, layout["recipe_sha256"], execution_nonce
    )
    if run_id != expected_run_id:
        raise ResultImportError("preprocess processing_run_id disagrees with its unique execution nonce")
    output_root = Path(layout["output_root"])
    object_dir = Path(layout["object_dir"])
    recipe_dir = Path(layout["recipe_dir"])
    run_dir = Path(layout["run_dir"])
    if not all(path.is_absolute() for path in (output_root, object_dir, recipe_dir, run_dir)):
        raise ResultImportError("preprocess layout paths must be absolute")
    if any(path.resolve(strict=False) != path for path in (output_root, object_dir, recipe_dir, run_dir)):
        raise ResultImportError("preprocess layout paths may not traverse symlinks")
    expected_object_dir = output_root / "media" / "sha256" / digest[:2] / digest
    expected_recipe_dir = expected_object_dir / "recipes" / expected_recipe_sha256
    expected_run_dir = expected_recipe_dir / "executions" / run_id
    if object_dir != expected_object_dir or recipe_dir != expected_recipe_dir or run_dir != expected_run_dir:
        raise ResultImportError("preprocess content-addressed layout is inconsistent")
    if envelope.get("result_path") != str(run_dir / "result.json"):
        raise ResultImportError("preprocess result_path disagrees with its execution directory")

    reuse = _object(envelope.get("reuse"), "preprocess result.reuse")
    _exact_keys(
        reuse,
        "preprocess result.reuse",
        {
            "mode", "prior_processing_run_id", "prior_result_path",
            "prior_result_sha256", "verified_at",
        },
    )
    if reuse.get("mode") == "none":
        if any(reuse.get(key) is not None for key in reuse if key != "mode"):
            raise ResultImportError("preprocess initial execution has inconsistent reuse lineage")
    elif reuse.get("mode") == "verified_prior_result":
        prior_run_id = _string(
            reuse.get("prior_processing_run_id"),
            "preprocess reuse.prior_processing_run_id",
        )
        prior_result_path = _string(
            reuse.get("prior_result_path"), "preprocess reuse.prior_result_path"
        )
        _sha256(reuse.get("prior_result_sha256"), "preprocess reuse.prior_result_sha256")
        verified_at = _timestamp(reuse.get("verified_at"), "preprocess reuse.verified_at")
        assert prior_run_id is not None and prior_result_path is not None
        if prior_run_id == run_id:
            raise ResultImportError("preprocess reuse lineage cannot reference itself")
        expected_prior_path = recipe_dir / "executions" / prior_run_id / "result.json"
        if Path(prior_result_path) != expected_prior_path:
            raise ResultImportError("preprocess reuse lineage leaves the recipe execution tree")
        if _timestamp_value(verified_at) < _timestamp_value(started_at):
            raise ResultImportError("preprocess reuse verification precedes execution start")
        if _timestamp_value(verified_at) > _timestamp_value(completed_at):
            raise ResultImportError("preprocess reuse verification follows execution completion")
    else:
        raise ResultImportError("preprocess result.reuse.mode is unsupported")
    steps = _array(envelope.get("steps"), "preprocess result.steps", nonempty=True)
    expected_step_names = {"probe", "audio_flac", "proxy", "routing"}
    observed_step_names: set[str] = set()
    step_by_name: dict[str, dict[str, Any]] = {}
    for index, value in enumerate(steps):
        step = _object(value, f"preprocess result.steps[{index}]")
        _exact_keys(step, f"preprocess result.steps[{index}]", {"name", "status", "command", "output_path"})
        name = _string(step.get("name"), f"preprocess result.steps[{index}].name")
        assert name is not None
        observed_step_names.add(name)
        step_by_name[name] = step
        if step.get("status") not in {"completed", "reused", "not_applicable", "disabled"}:
            raise ResultImportError("preprocess completed result contains a non-terminal step")
        command = step.get("command")
        if command is not None:
            command_args = _array(command, f"preprocess result.steps[{index}].command", nonempty=True)
            for argument_index, argument in enumerate(command_args):
                _string(argument, f"preprocess step command[{argument_index}]")
        _nullable_string(step.get("output_path"), f"preprocess result.steps[{index}].output_path")
    if observed_step_names != expected_step_names or len(steps) != len(expected_step_names):
        raise ResultImportError("preprocess result must contain each known step exactly once")

    records = _object(envelope.get("catalog_records"), "preprocess catalog_records")
    _exact_keys(
        records,
        "preprocess catalog_records",
        {"processing_runs", "run_inputs", "media_objects", "media_locations", "media_derivations", "artifacts"},
    )
    processing_runs = _array(
        records.get("processing_runs"), "preprocess catalog processing_runs", nonempty=True
    )
    if len(processing_runs) != 1 or canonical_json(processing_runs[0]) != canonical_json(run):
        raise ResultImportError("preprocess catalog processing run differs from envelope run")
    run_inputs = _array(records.get("run_inputs"), "preprocess catalog run_inputs", nonempty=True)
    media_rows_raw = _array(
        records.get("media_objects"), "preprocess catalog media_objects", nonempty=True
    )
    location_rows = _array(
        records.get("media_locations"), "preprocess catalog media_locations", nonempty=True
    )
    derivations = _array(
        records.get("media_derivations"), "preprocess catalog media_derivations"
    )
    catalog_artifacts = _array(
        records.get("artifacts"), "preprocess catalog artifacts", nonempty=True
    )
    artifacts = _array(envelope.get("artifacts"), "preprocess result.artifacts", nonempty=True)

    normalized_media_rows: list[dict[str, Any]] = []
    media_by_id: dict[str, dict[str, Any]] = {}
    for index, value in enumerate(media_rows_raw):
        row = _validate_media_row(value, f"preprocess media_objects[{index}]")
        if row["media_id"] in media_by_id:
            raise ResultImportError("preprocess catalog contains duplicate media IDs")
        media_by_id[row["media_id"]] = row
        normalized_media_rows.append(row)
    source_media = media_by_id.get(media_id)
    if source_media is None or source_media["byte_count"] != byte_count:
        raise ResultImportError("preprocess input is absent or inconsistent in media_objects")
    if source_media["first_cataloged_at"] != first_cataloged_at:
        raise ResultImportError("preprocess input first_cataloged_at disagrees with catalog observation")

    if len(run_inputs) != 1:
        raise ResultImportError("preprocess result-v1 must contain one source run input")
    run_input = _object(run_inputs[0], "preprocess catalog run input")
    _exact_keys(
        run_input,
        "preprocess catalog run input",
        {"run_input_id", "processing_run_id", "object_type", "object_id", "input_role", "input_sha256"},
    )
    _string(run_input.get("run_input_id"), "preprocess catalog run input.run_input_id")
    if (
        run_input.get("processing_run_id") != run_id
        or run_input.get("object_type") != "media"
        or run_input.get("object_id") != media_id
        or run_input.get("input_role") != "source_media"
        or run_input.get("input_sha256") != digest
    ):
        raise ResultImportError("preprocess run input disagrees with envelope input")
    if run_input["run_input_id"] != _producer_stable_id(
        "run_input", run_id, media_id, "source_media"
    ):
        raise ResultImportError("preprocess producer run_input_id is not deterministic")

    locations_seen: set[tuple[str, str]] = set()
    for index, value in enumerate(location_rows):
        row = _object(value, f"preprocess media_locations[{index}]")
        _exact_keys(
            row,
            f"preprocess media_locations[{index}]",
            {"media_location_id", "media_id", "storage_uri", "storage_class", "verified_at", "is_primary"},
        )
        _string(row.get("media_location_id"), f"preprocess media_locations[{index}].media_location_id")
        location_media_id = _string(row.get("media_id"), f"preprocess media_locations[{index}].media_id")
        uri = _string(row.get("storage_uri"), f"preprocess media_locations[{index}].storage_uri")
        if location_media_id not in media_by_id:
            raise ResultImportError("preprocess media location references unknown media")
        if (location_media_id, uri) in locations_seen:
            raise ResultImportError("preprocess catalog contains a duplicate media location")
        locations_seen.add((location_media_id, uri))
        if isinstance(row.get("is_primary"), bool) or row.get("is_primary") not in (0, 1):
            raise ResultImportError("preprocess media location.is_primary must be 0 or 1")
        _string(row.get("storage_class"), f"preprocess media_locations[{index}].storage_class")
        _optional_timestamp(row.get("verified_at"), f"preprocess media_locations[{index}].verified_at")
        if row["media_location_id"] != _producer_stable_id(
            "media_location", location_media_id, uri
        ):
            raise ResultImportError("preprocess producer media_location_id is not deterministic")
    input_storage_uri = input_row.get("storage_uri")
    if input_storage_uri is not None and (media_id, input_storage_uri) not in locations_seen:
        raise ResultImportError("preprocess input storage_uri is absent from media_locations")

    for index, value in enumerate(derivations):
        row = _object(value, f"preprocess media_derivations[{index}]")
        _exact_keys(
            row,
            f"preprocess media_derivations[{index}]",
            {"child_media_id", "parent_media_id", "derivation_kind", "processing_run_id", "metadata_json"},
        )
        if row.get("processing_run_id") != run_id:
            raise ResultImportError("preprocess derivation references the wrong run")
        if row.get("parent_media_id") != media_id:
            raise ResultImportError("preprocess derivation parent must be the input media")
        child = row.get("child_media_id")
        if child not in media_by_id or child == media_id:
            raise ResultImportError("preprocess derivation references invalid child media")
        _string(row.get("derivation_kind"), "preprocess derivation.derivation_kind")
        _object(row.get("metadata_json"), "preprocess derivation.metadata_json")

    expected_artifact_paths = {
        "ffprobe_normalized_json": run_dir / "probe.normalized.json",
        "audio_16khz_mono_flac": run_dir / "artifacts" / "audio-16khz-mono.flac",
        "low_resolution_cfr_proxy": run_dir
        / "artifacts"
        / f"proxy-{profile['proxy_width']}x{profile['proxy_height']}-{profile['proxy_fps']}fps.mp4",
        "scene_silence_routing_json": run_dir / "routing.json",
    }
    expected_step_kinds = {
        "probe": "ffprobe_normalized_json",
        "audio_flac": "audio_16khz_mono_flac",
        "proxy": "low_resolution_cfr_proxy",
        "routing": "scene_silence_routing_json",
    }
    artifact_by_id: dict[str, dict[str, Any]] = {}
    artifact_by_kind: dict[str, dict[str, Any]] = {}
    for index, value in enumerate(artifacts):
        row = _object(value, f"preprocess artifacts[{index}]")
        _exact_keys(
            row,
            f"preprocess artifacts[{index}]",
            {
                "artifact_id",
                "processing_run_id",
                "artifact_kind",
                "storage_uri",
                "path",
                "sha256",
                "byte_count",
                "schema_version",
                "visibility",
                "media_kind",
                "mime_type",
                "normalized_probe",
            },
        )
        artifact_id = _string(row.get("artifact_id"), f"preprocess artifacts[{index}].artifact_id")
        if artifact_id in artifact_by_id:
            raise ResultImportError("preprocess result contains duplicate artifact IDs")
        artifact_kind = _string(
            row.get("artifact_kind"), f"preprocess artifacts[{index}].artifact_kind"
        )
        assert artifact_kind is not None
        if artifact_kind not in expected_artifact_paths or artifact_kind in artifact_by_kind:
            raise ResultImportError("preprocess result contains an unsupported or duplicate artifact kind")
        if row.get("processing_run_id") != run_id or row.get("visibility") != "private":
            raise ResultImportError("preprocess artifacts must belong to the run and stay private")
        _sha256(row.get("sha256"), f"preprocess artifacts[{index}].sha256")
        _integer(row.get("byte_count"), f"preprocess artifacts[{index}].byte_count")
        _integer(row.get("schema_version"), f"preprocess artifacts[{index}].schema_version", minimum=1)
        _string(row.get("storage_uri"), f"preprocess artifacts[{index}].storage_uri")
        _string(row.get("path"), f"preprocess artifacts[{index}].path")
        _validate_file_uri(
            row.get("path"), row.get("storage_uri"), f"preprocess artifacts[{index}]"
        )
        if row.get("media_kind") not in MEDIA_KINDS:
            raise ResultImportError("preprocess artifact.media_kind is unsupported")
        _nullable_string(row.get("mime_type"), f"preprocess artifacts[{index}].mime_type")
        if row.get("normalized_probe") is not None:
            _object(row["normalized_probe"], f"preprocess artifacts[{index}].normalized_probe")
        expected_path = expected_artifact_paths[artifact_kind].resolve()
        if row["path"] != str(expected_path) or row["storage_uri"] != expected_path.as_uri():
            raise ResultImportError("preprocess artifact leaves its run-local execution path")
        expected_artifact_id = "artifact_" + sha256_bytes(
            canonical_json(
                {
                    "processing_run_id": run_id,
                    "kind": row["artifact_kind"],
                    "sha256": row["sha256"],
                }
            ).encode("utf-8")
        )[:32]
        if artifact_id != expected_artifact_id:
            raise ResultImportError("preprocess producer artifact_id is not deterministic")
        artifact_by_id[artifact_id] = row
        artifact_by_kind[artifact_kind] = row
    for step_name, artifact_kind in expected_step_kinds.items():
        step = step_by_name[step_name]
        artifact = artifact_by_kind.get(artifact_kind)
        if artifact is None:
            if step["status"] not in {"not_applicable", "disabled"} or step["output_path"] is not None:
                raise ResultImportError(f"preprocess {step_name} step/artifact relationship is inconsistent")
        elif (
            step["status"] not in {"completed", "reused"}
            or step["output_path"] != artifact["path"]
        ):
            raise ResultImportError(f"preprocess {step_name} step points at the wrong artifact")
        elif reuse["mode"] == "verified_prior_result" and step["status"] != "reused":
            raise ResultImportError("preprocess reuse lineage requires every artifact step to be reused")
        elif reuse["mode"] == "none" and step["status"] != "completed":
            raise ResultImportError("preprocess initial execution cannot claim artifact reuse")
    primary_streams = _object(
        source_media["ffprobe_json"].get("primary_streams"),
        "preprocess source probe.primary_streams",
    )
    _exact_keys(
        primary_streams,
        "preprocess source probe.primary_streams",
        {"video_index", "audio_index"},
    )
    has_video = primary_streams["video_index"] is not None
    has_audio = primary_streams["audio_index"] is not None
    source_probe_format = _object(
        source_media["ffprobe_json"].get("format"),
        "preprocess source probe.format",
    )
    probe_duration_ms = source_probe_format.get("duration_ms")
    if probe_duration_ms is not None:
        _integer(
            probe_duration_ms,
            "preprocess source probe.format.duration_ms",
        )
    if source_media["duration_ms"] != probe_duration_ms:
        raise ResultImportError(
            "preprocess source media duration disagrees with normalized probe"
        )
    expected_artifact_kinds = {"ffprobe_normalized_json"}
    if operations["audio_flac"] and has_audio:
        expected_artifact_kinds.add("audio_16khz_mono_flac")
    if operations["proxy"] and has_video:
        expected_artifact_kinds.add("low_resolution_cfr_proxy")
    if operations["routing"] and (has_video or has_audio):
        expected_artifact_kinds.add("scene_silence_routing_json")
    if set(artifact_by_kind) != expected_artifact_kinds:
        raise ResultImportError("preprocess artifact set disagrees with operations and source streams")
    if len(catalog_artifacts) != len(artifact_by_id):
        raise ResultImportError("preprocess catalog artifacts differ in count from envelope artifacts")
    for index, value in enumerate(catalog_artifacts):
        row = _object(value, f"preprocess catalog artifacts[{index}]")
        _exact_keys(
            row,
            f"preprocess catalog artifacts[{index}]",
            {"artifact_id", "processing_run_id", "artifact_kind", "storage_uri", "sha256", "byte_count", "schema_version", "visibility"},
        )
        top = artifact_by_id.get(row.get("artifact_id"))
        if top is None:
            raise ResultImportError("preprocess catalog artifact is absent from envelope artifacts")
        for key in (
            "processing_run_id",
            "artifact_kind",
            "storage_uri",
            "sha256",
            "byte_count",
            "schema_version",
            "visibility",
        ):
            if row.get(key) != top.get(key):
                raise ResultImportError(f"preprocess catalog artifact.{key} disagrees with envelope")

    derived_media_ids = set(media_by_id) - {media_id}
    artifact_media_ids: set[str] = set()
    for artifact in artifact_by_id.values():
        normalized_probe = artifact.get("normalized_probe")
        if normalized_probe is None:
            continue
        artifact_digest = artifact["sha256"]
        artifact_media_id = _media_id(artifact_digest)
        media_row = media_by_id.get(artifact_media_id)
        if media_row is None:
            raise ResultImportError("preprocess media artifact is absent from media_objects")
        if (
            media_row["byte_count"] != artifact["byte_count"]
            or media_row["media_kind"] != artifact["media_kind"]
            or media_row.get("mime_type") != artifact.get("mime_type")
            or canonical_json(media_row.get("ffprobe_json")) != canonical_json(normalized_probe)
        ):
            raise ResultImportError("preprocess media artifact disagrees with its media object")
        if (artifact_media_id, artifact["storage_uri"]) not in locations_seen:
            raise ResultImportError("preprocess media artifact location is absent from media_locations")
        artifact_media_ids.add(artifact_media_id)
    if artifact_media_ids != derived_media_ids:
        raise ResultImportError("preprocess derived media and probed media artifacts differ")
    expected_media_locations = {(media_id, input_row["storage_uri"])} | {
        (_media_id(artifact["sha256"]), artifact["storage_uri"])
        for artifact in artifact_by_id.values()
        if artifact.get("normalized_probe") is not None
    }
    if locations_seen != expected_media_locations:
        raise ResultImportError("preprocess media_locations contain unverified or missing paths")
    derivation_child_ids = {row["child_media_id"] for row in derivations}
    if derivation_child_ids != derived_media_ids or len(derivations) != len(derived_media_ids):
        raise ResultImportError("preprocess derivations do not cover each derived media exactly once")

    routing = envelope.get("routing")
    if routing is not None:
        _validate_preprocess_routing(
            routing,
            media_id=media_id,
            source_duration_ms=source_media["duration_ms"],
            has_video=has_video,
            has_audio=has_audio,
            profile=profile,
        )

    normalized = dict(envelope)
    normalized["processing_run"] = {**run, "started_at": started_at, "completed_at": completed_at}
    normalized["_normalized_media_rows"] = normalized_media_rows
    return normalized


def _begin_result_batch(
    connection: sqlite3.Connection,
    *,
    importer_name: str,
    digest: str,
    started_at: str,
    observation_at: str | None = None,
) -> str:
    batch_id = stable_id("imp", importer_name, digest)
    connection.execute(
        """
        INSERT INTO import_batches(
            import_batch_id, importer_name, importer_version, input_sha256,
            source_snapshot_date, started_at, status, statistics_json
        ) VALUES(?, ?, ?, ?, NULL, ?, 'running', '{}')
        ON CONFLICT(import_batch_id) DO UPDATE SET
            started_at = excluded.started_at,
            completed_at = NULL,
            status = 'running'
        """,
        (batch_id, importer_name, __version__, digest, started_at),
    )
    if observation_at is not None:
        observation_id = _import_observation_id(batch_id, observation_at)
        existing_observation = connection.execute(
            """
            SELECT import_batch_id, importer_version, source_snapshot_date, observed_at
            FROM import_observations
            WHERE import_observation_id = ?
            """,
            (observation_id,),
        ).fetchone()
        if existing_observation and dict(existing_observation) != {
            "import_batch_id": batch_id,
            "importer_version": __version__,
            "source_snapshot_date": None,
            "observed_at": observation_at,
        }:
            raise ResultImportError(
                "acquisition import-observation identity conflicts with existing state"
            )
        connection.execute(
            """
            INSERT INTO import_observations(
                import_observation_id, import_batch_id, importer_version,
                source_snapshot_date, observed_at, status, statistics_json
            ) VALUES(?, ?, ?, NULL, ?, 'running', '{}')
            ON CONFLICT(import_observation_id) DO UPDATE SET
                status = 'running',
                completed_at = NULL,
                statistics_json = '{}'
            """,
            (observation_id, batch_id, __version__, observation_at),
        )
    return batch_id


def _complete_result_batch(
    connection: sqlite3.Connection,
    batch_id: str,
    completed_at: str,
    statistics: dict[str, Any],
    *,
    observation_at: str | None = None,
) -> None:
    statistics_text = canonical_json(statistics)
    connection.execute(
        """
        UPDATE import_batches
        SET completed_at = ?, status = 'completed', statistics_json = ?
        WHERE import_batch_id = ?
        """,
        (completed_at, statistics_text, batch_id),
    )
    if observation_at is not None:
        observation_id = _import_observation_id(batch_id, observation_at)
        connection.execute(
            """
            UPDATE import_observations
            SET completed_at = ?, status = 'completed', statistics_json = ?
            WHERE import_observation_id = ?
            """,
            (completed_at, statistics_text, observation_id),
        )
        if connection.execute("SELECT changes()").fetchone()[0] != 1:
            raise RuntimeError(
                "Acquisition import observation was not started before completion"
            )


def _insert_external_id(
    connection: sqlite3.Connection,
    *,
    object_type: str,
    object_id: str,
    namespace: str,
    value: str,
    basis: str,
) -> None:
    connection.execute(
        """
        INSERT OR IGNORE INTO external_ids(
            external_id_id, object_type, object_id, namespace, external_value,
            confidence_state, basis, source_id
        ) VALUES(?, ?, ?, ?, ?, 'metadata_only', ?, NULL)
        """,
        (
            stable_id("ext", object_type, object_id, namespace, value),
            object_type,
            object_id,
            namespace,
            value,
            basis,
        ),
    )


def _upsert_source(
    connection: sqlite3.Connection,
    row: dict[str, Any],
    *,
    batch_id: str,
) -> str:
    canonical_id = source_id(row["platform"], row["source_kind"], row["native_id"])
    conflicting = connection.execute(
        """
        SELECT source_id FROM sources
        WHERE platform = ? AND source_kind = ? AND native_id = ?
        """,
        (row["platform"], row["source_kind"], row["native_id"]),
    ).fetchone()
    if conflicting and conflicting["source_id"] != canonical_id:
        raise ResultImportError("catalog source identity conflicts with deterministic source ID")
    observed_at = _timestamp(row["observed_at"], "catalog source.observed_at")
    _timestamp(row.get("created_at", observed_at), "catalog source.created_at")
    _timestamp(row.get("updated_at", observed_at), "catalog source.updated_at")
    _upsert_catalog_source(
        connection,
        source=canonical_id,
        platform=row["platform"],
        source_kind=row["source_kind"],
        native_id=row["native_id"],
        parent_source=row.get("parent_source_id"),
        canonical_url=row.get("canonical_url"),
        historical_url=row.get("historical_url"),
        title=row.get("title"),
        published_at=row.get("published_at"),
        observed_at=observed_at,
        access_state=row["access_state"],
        review_state=row["review_state"],
        metadata=row.get("metadata_json") or {},
        batch_id=batch_id,
    )
    return canonical_id


def _upsert_media_object(connection: sqlite3.Connection, row: dict[str, Any]) -> str:
    media_id = row["media_id"]
    existing = connection.execute(
        "SELECT media_id, sha256, byte_count FROM media_objects WHERE sha256 = ? OR media_id = ?",
        (row["sha256"], media_id),
    ).fetchall()
    for current in existing:
        if (
            current["media_id"] != media_id
            or current["sha256"] != row["sha256"]
            or current["byte_count"] != row["byte_count"]
        ):
            raise ResultImportError("existing media identity conflicts with result bytes")
    ffprobe_json = row.get("ffprobe_json")
    connection.execute(
        """
        INSERT INTO media_objects(
            media_id, sha256, byte_count, media_kind, mime_type, container,
            duration_ms, ffprobe_json, first_cataloged_at, integrity_state
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'verified')
        ON CONFLICT(media_id) DO UPDATE SET
            media_kind = CASE WHEN media_objects.media_kind = 'other'
                THEN excluded.media_kind ELSE media_objects.media_kind END,
            mime_type = COALESCE(media_objects.mime_type, excluded.mime_type),
            container = COALESCE(media_objects.container, excluded.container),
            duration_ms = COALESCE(media_objects.duration_ms, excluded.duration_ms),
            ffprobe_json = COALESCE(media_objects.ffprobe_json, excluded.ffprobe_json),
            first_cataloged_at = CASE
                WHEN julianday(excluded.first_cataloged_at) < julianday(media_objects.first_cataloged_at)
                THEN excluded.first_cataloged_at ELSE media_objects.first_cataloged_at END,
            integrity_state = 'verified'
        """,
        (
            media_id,
            row["sha256"],
            row["byte_count"],
            row["media_kind"],
            row.get("mime_type"),
            row.get("container"),
            row.get("duration_ms"),
            canonical_json(ffprobe_json) if ffprobe_json is not None else None,
            row["first_cataloged_at"],
        ),
    )
    return media_id


def _upsert_media_location(connection: sqlite3.Connection, row: dict[str, Any]) -> str:
    media_id = row["media_id"]
    uri = row["storage_uri"]
    existing = connection.execute(
        "SELECT media_location_id FROM media_locations WHERE media_id = ? AND storage_uri = ?",
        (media_id, uri),
    ).fetchone()
    location_id = (
        existing["media_location_id"]
        if existing
        else stable_id("mlc", media_id, uri)
    )
    verified_at = _optional_timestamp(row.get("verified_at"), "media location.verified_at")
    connection.execute(
        """
        INSERT INTO media_locations(
            media_location_id, media_id, storage_uri, storage_class, verified_at, is_primary
        ) VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(media_location_id) DO UPDATE SET
            verified_at = CASE
                WHEN media_locations.verified_at IS NULL THEN excluded.verified_at
                WHEN excluded.verified_at IS NULL THEN media_locations.verified_at
                WHEN julianday(excluded.verified_at) > julianday(media_locations.verified_at)
                    THEN excluded.verified_at
                ELSE media_locations.verified_at
            END,
            is_primary = MAX(media_locations.is_primary, excluded.is_primary)
        """,
        (
            location_id,
            media_id,
            uri,
            row.get("storage_class") or "local",
            verified_at,
            row.get("is_primary", 0),
        ),
    )
    return location_id


def _upsert_media_source(
    connection: sqlite3.Connection,
    row: dict[str, Any],
    *,
    canonical_source_id: str,
) -> str:
    media_id = row["media_id"]
    existing = connection.execute(
        "SELECT media_source_id FROM media_sources WHERE media_id = ? AND source_id = ?",
        (media_id, canonical_source_id),
    ).fetchone()
    media_source_id = (
        existing["media_source_id"]
        if existing
        else stable_id("mso", media_id, canonical_source_id)
    )
    retrieved_at = _timestamp(row["retrieved_at"], "media source.retrieved_at")
    connection.execute(
        """
        INSERT INTO media_sources(
            media_source_id, media_id, source_id, retrieved_at, retrieval_tool,
            retrieval_tool_version, source_snapshot_id
        ) VALUES(?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(media_source_id) DO UPDATE SET
            retrieval_tool = CASE WHEN julianday(excluded.retrieved_at) < julianday(media_sources.retrieved_at)
                THEN excluded.retrieval_tool ELSE media_sources.retrieval_tool END,
            retrieval_tool_version = CASE WHEN julianday(excluded.retrieved_at) < julianday(media_sources.retrieved_at)
                THEN excluded.retrieval_tool_version ELSE media_sources.retrieval_tool_version END,
            source_snapshot_id = COALESCE(media_sources.source_snapshot_id, excluded.source_snapshot_id),
            retrieved_at = CASE
                WHEN julianday(excluded.retrieved_at) < julianday(media_sources.retrieved_at)
                THEN excluded.retrieved_at ELSE media_sources.retrieved_at END
        """,
        (
            media_source_id,
            media_id,
            canonical_source_id,
            retrieved_at,
            row["retrieval_tool"],
            row.get("retrieval_tool_version"),
            row.get("source_snapshot_id"),
        ),
    )
    return media_source_id


def _insert_processing_run(connection: sqlite3.Connection, row: dict[str, Any]) -> str:
    run_id = row["processing_run_id"]
    values = {
        "stage": row["stage"],
        "implementation_version": row["implementation_version"],
        "parameters_json": canonical_json(row.get("parameters_json") or {}),
        "environment_json": canonical_json(row.get("environment_json") or {}),
        "started_at": _timestamp(row["started_at"], "processing run.started_at"),
        "completed_at": _optional_timestamp(row.get("completed_at"), "processing run.completed_at"),
        "status": row["status"],
    }
    existing = connection.execute(
        """
        SELECT stage, implementation_version, parameters_json, environment_json,
               started_at, completed_at, status
        FROM processing_runs WHERE processing_run_id = ?
        """,
        (run_id,),
    ).fetchone()
    if existing:
        if any(existing[key] != value for key, value in values.items()):
            raise ResultImportError("processing_run_id already has different run data")
        return run_id
    connection.execute(
        """
        INSERT INTO processing_runs(
            processing_run_id, stage, implementation_version, model_id,
            glossary_revision_id, parameters_json, environment_json, random_seed,
            started_at, completed_at, status, error_text
        ) VALUES(?, ?, ?, NULL, NULL, ?, ?, NULL, ?, ?, ?, NULL)
        """,
        (
            run_id,
            values["stage"],
            values["implementation_version"],
            values["parameters_json"],
            values["environment_json"],
            values["started_at"],
            values["completed_at"],
            values["status"],
        ),
    )
    return run_id


def _insert_run_input(connection: sqlite3.Connection, row: dict[str, Any]) -> str:
    run_input_id = stable_id(
        "rin",
        row["processing_run_id"],
        row["object_type"],
        row["object_id"],
        row["input_role"],
    )
    connection.execute(
        """
        INSERT OR IGNORE INTO run_inputs(
            run_input_id, processing_run_id, object_type, object_id, input_role, input_sha256
        ) VALUES(?, ?, ?, ?, ?, ?)
        """,
        (
            run_input_id,
            row["processing_run_id"],
            row["object_type"],
            row["object_id"],
            row["input_role"],
            row.get("input_sha256"),
        ),
    )
    return run_input_id


def _upsert_job_and_attempt(
    connection: sqlite3.Connection,
    *,
    stage: str,
    target_type: str,
    target_id: str,
    run_id: str,
    producer_job_id: str,
    started_at: str,
    completed_at: str,
) -> tuple[str, str]:
    job_id = stable_id("job", stage, target_type, target_id)
    existing = connection.execute(
        "SELECT job_id FROM jobs WHERE stage = ? AND target_type = ? AND target_id = ?",
        (stage, target_type, target_id),
    ).fetchone()
    if existing and existing["job_id"] != job_id:
        raise ResultImportError("existing logical job uses a non-deterministic identifier")
    connection.execute(
        """
        INSERT INTO jobs(
            job_id, stage, target_type, target_id, priority, state,
            max_attempts, created_at, updated_at
        ) VALUES(?, ?, ?, ?, 100, 'completed', 3, ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET
            state = 'completed',
            created_at = CASE WHEN julianday(excluded.created_at) < julianday(jobs.created_at)
                THEN excluded.created_at ELSE jobs.created_at END,
            updated_at = CASE WHEN julianday(excluded.updated_at) > julianday(jobs.updated_at)
                THEN excluded.updated_at ELSE jobs.updated_at END
        """,
        (job_id, stage, target_type, target_id, started_at, completed_at),
    )
    attempt_id = stable_id("jat", job_id, run_id)
    existing_attempt = connection.execute(
        """
        SELECT processing_run_id, started_at, completed_at, status
        FROM job_attempts WHERE job_attempt_id = ?
        """,
        (attempt_id,),
    ).fetchone()
    if existing_attempt:
        if (
            existing_attempt["processing_run_id"] != run_id
            or existing_attempt["started_at"] != started_at
            or existing_attempt["completed_at"] != completed_at
            or existing_attempt["status"] != "completed"
        ):
            raise ResultImportError("job attempt ID already has different attempt data")
    else:
        temporary_number = connection.execute(
            "SELECT COALESCE(MAX(attempt_number), 0) + 1 FROM job_attempts WHERE job_id = ?",
            (job_id,),
        ).fetchone()[0]
        connection.execute(
            """
            INSERT INTO job_attempts(
                job_attempt_id, job_id, attempt_number, processing_run_id,
                started_at, completed_at, status, error_text
            ) VALUES(?, ?, ?, ?, ?, ?, 'completed', NULL)
            """,
            (attempt_id, job_id, temporary_number, run_id, started_at, completed_at),
        )

    # Attempt ordinals are stable for a given set of runs regardless of import order.
    # Move all numbers out of the destination range before assigning the sorted order.
    attempts = connection.execute(
        """
        SELECT job_attempt_id FROM job_attempts
        WHERE job_id = ?
        ORDER BY started_at, COALESCE(processing_run_id, ''), job_attempt_id
        """,
        (job_id,),
    ).fetchall()
    current_max = connection.execute(
        "SELECT COALESCE(MAX(attempt_number), 0) FROM job_attempts WHERE job_id = ?",
        (job_id,),
    ).fetchone()[0]
    connection.execute(
        "UPDATE job_attempts SET attempt_number = attempt_number + ? WHERE job_id = ?",
        (current_max + len(attempts) + 1, job_id),
    )
    for ordinal, attempt in enumerate(attempts, 1):
        connection.execute(
            "UPDATE job_attempts SET attempt_number = ? WHERE job_attempt_id = ?",
            (ordinal, attempt["job_attempt_id"]),
        )
    connection.execute(
        "UPDATE jobs SET max_attempts = MAX(max_attempts, ?) WHERE job_id = ?",
        (len(attempts), job_id),
    )
    _insert_external_id(
        connection,
        object_type="processing_run",
        object_id=run_id,
        namespace=f"{stage}_producer_job_id",
        value=producer_job_id,
        basis="Completed result envelope job_id",
    )
    return job_id, attempt_id


def _source_renditions(
    connection: sqlite3.Connection,
    *,
    source_id_value: str,
    media_id: str,
) -> list[str]:
    rendition_ids: list[str] = []
    mappings = connection.execute(
        """
        SELECT DISTINCT recording_id FROM recording_sources
        WHERE source_id = ? AND confidence_state <> 'rejected'
        ORDER BY recording_id
        """,
        (source_id_value,),
    ).fetchall()
    for mapping in mappings:
        recording = mapping["recording_id"]
        rendition_id = stable_id("rnd", recording, media_id, "acquired_source_media")
        connection.execute(
            """
            INSERT OR IGNORE INTO renditions(
                rendition_id, recording_id, media_id, rendition_kind,
                label, review_state, metadata_json
            ) VALUES(?, ?, ?, 'acquired_source_media',
                     'Acquired source bytes', 'unreviewed', ?)
            """,
            (
                rendition_id,
                recording,
                media_id,
                canonical_json({"publication_state": "withheld_by_default"}),
            ),
        )
        rendition_ids.append(rendition_id)
    return rendition_ids


def _derived_renditions(
    connection: sqlite3.Connection,
    *,
    source_media_id: str,
    derivations: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    source_rows = connection.execute(
        """
        SELECT rendition_id, recording_id FROM renditions
        WHERE media_id = ? AND review_state <> 'rejected'
        ORDER BY recording_id, rendition_id
        """,
        (source_media_id,),
    ).fetchall()
    source_rendition_ids = [row["rendition_id"] for row in source_rows]
    derived_ids: list[str] = []
    for derivation in derivations:
        for source_rendition in source_rows:
            recording = source_rendition["recording_id"]
            media_id = derivation["child_media_id"]
            kind = derivation["derivation_kind"]
            rendition_id = stable_id("rnd", recording, media_id, kind)
            connection.execute(
                """
                INSERT OR IGNORE INTO renditions(
                    rendition_id, recording_id, media_id, rendition_kind,
                    label, review_state, metadata_json
                ) VALUES(?, ?, ?, ?, 'Machine-derived media', 'unreviewed', ?)
                """,
                (
                    rendition_id,
                    recording,
                    media_id,
                    kind,
                    canonical_json(
                        {
                            "derived_from_rendition_id": source_rendition["rendition_id"],
                            "publication_state": "withheld_by_default",
                        }
                    ),
                ),
            )
            derived_ids.append(rendition_id)
    return source_rendition_ids, derived_ids


def _insert_artifact(
    connection: sqlite3.Connection,
    row: dict[str, Any],
    *,
    descriptor: dict[str, Any] | None,
) -> str:
    artifact_id = row["artifact_id"]
    metadata = {}
    if descriptor:
        metadata = {
            key: descriptor[key]
            for key in ("media_kind", "mime_type", "normalized_probe")
            if descriptor.get(key) is not None
        }
    existing = connection.execute(
        """
        SELECT processing_run_id, artifact_kind, storage_uri, sha256,
               byte_count, schema_version, visibility
        FROM artifacts WHERE artifact_id = ?
        """,
        (artifact_id,),
    ).fetchone()
    expected = {
        "processing_run_id": row["processing_run_id"],
        "artifact_kind": row["artifact_kind"],
        "storage_uri": row["storage_uri"],
        "sha256": row["sha256"],
        "byte_count": row["byte_count"],
        "schema_version": row["schema_version"],
        "visibility": "private",
    }
    if existing:
        if any(existing[key] != value for key, value in expected.items()):
            raise ResultImportError("artifact_id already has different artifact data")
        return artifact_id
    uri_collision = connection.execute(
        "SELECT artifact_id FROM artifacts WHERE storage_uri = ? AND sha256 = ?",
        (row["storage_uri"], row["sha256"]),
    ).fetchone()
    if uri_collision and uri_collision["artifact_id"] != artifact_id:
        raise ResultImportError("artifact storage URI and digest already use a different ID")
    connection.execute(
        """
        INSERT INTO artifacts(
            artifact_id, processing_run_id, artifact_kind, storage_uri, sha256,
            byte_count, schema_version, visibility, metadata_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, 'private', ?)
        """,
        (
            artifact_id,
            row["processing_run_id"],
            row["artifact_kind"],
            row["storage_uri"],
            row["sha256"],
            row["byte_count"],
            row["schema_version"],
            canonical_json(metadata),
        ),
    )
    return artifact_id


def _insert_routing_observations(
    connection: sqlite3.Connection,
    *,
    routing: dict[str, Any] | None,
    run_id: str,
    rendition_ids: list[str],
    created_at: str,
) -> tuple[int, int]:
    if not routing:
        return 0, 0
    candidates = len(routing["scene_changes"]) + len(routing["silence_intervals"])
    if not rendition_ids:
        return 0, candidates
    duration_ms = routing["coverage"]["duration_ms"]
    if duration_ms <= 0:
        return 0, candidates
    inserted_ids: list[str] = []
    for rendition_id in rendition_ids:
        rendition = connection.execute(
            "SELECT recording_id FROM renditions WHERE rendition_id = ?", (rendition_id,)
        ).fetchone()
        assert rendition is not None
        for ordinal, scene in enumerate(routing["scene_changes"]):
            timestamp = scene["timestamp_ms"]
            start_ms = min(timestamp, duration_ms - 1)
            end_ms = start_ms + 1
            observation_id = stable_id(
                "obs", run_id, rendition_id, "scene_change_candidate", ordinal, timestamp
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO observations(
                    observation_id, observation_kind, recording_id, rendition_id,
                    processing_run_id, start_ms, end_ms, visibility, review_state,
                    payload_schema_version, metadata_json, created_at
                ) VALUES(?, 'scene_change_candidate', ?, ?, ?, ?, ?, 'private',
                         'machine', 1, ?, ?)
                """,
                (
                    observation_id,
                    rendition["recording_id"],
                    rendition_id,
                    run_id,
                    start_ms,
                    end_ms,
                    canonical_json(
                        {
                            "coordinate_space": "rendition_media",
                            "timestamp_ms": timestamp,
                            "routing_only": True,
                        }
                    ),
                    created_at,
                ),
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO observation_scores(
                    observation_score_id, observation_id, score_name, raw_score,
                    calibrated_probability, calibration_set_id, quality_flags_json
                ) VALUES(?, ?, 'scene_score_percent', ?, NULL, NULL, '[]')
                """,
                (
                    stable_id("osc", observation_id, "scene_score_percent"),
                    observation_id,
                    scene["score_percent"],
                ),
            )
            inserted_ids.append(observation_id)
        for ordinal, silence in enumerate(routing["silence_intervals"]):
            observation_id = stable_id(
                "obs",
                run_id,
                rendition_id,
                "silence_interval_candidate",
                ordinal,
                silence["start_ms"],
                silence["end_ms"],
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO observations(
                    observation_id, observation_kind, recording_id, rendition_id,
                    processing_run_id, start_ms, end_ms, visibility, review_state,
                    payload_schema_version, metadata_json, created_at
                ) VALUES(?, 'silence_interval_candidate', ?, ?, ?, ?, ?, 'private',
                         'machine', 1, ?, ?)
                """,
                (
                    observation_id,
                    rendition["recording_id"],
                    rendition_id,
                    run_id,
                    silence["start_ms"],
                    silence["end_ms"],
                    canonical_json(
                        {
                            "coordinate_space": "rendition_media",
                            "duration_ms": silence["duration_ms"],
                            "routing_only": True,
                        }
                    ),
                    created_at,
                ),
            )
            inserted_ids.append(observation_id)
    return len(set(inserted_ids)), 0


def _exact_replay_rows(
    label: str,
    observed_rows: list[dict[str, Any]],
    expected_rows: list[dict[str, Any]],
) -> None:
    """Compare one replay-owned catalog row set without mutating the catalog."""

    observed = sorted(
        (canonical_json(row) for row in observed_rows),
    )
    expected = sorted(
        (canonical_json(row) for row in expected_rows),
    )
    if observed != expected:
        raise ResultImportError(
            f"completed preprocess replay {label} is missing, extra, or different"
        )


def _query_dicts(
    connection: sqlite3.Connection,
    query: str,
    parameters: tuple[Any, ...] = (),
) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(query, parameters).fetchall()]


def _preprocess_replay_batch(
    connection: sqlite3.Connection,
    *,
    importer_name: str,
    digest: str,
    run_id: str,
) -> dict[str, Any] | None:
    """Find an exact prior import without creating or repairing replay state."""

    batch_id = stable_id("imp", importer_name, digest)
    rows = _query_dicts(
        connection,
        """
        SELECT import_batch_id, importer_name, importer_version, input_sha256,
               source_snapshot_date, started_at, completed_at, status,
               statistics_json
        FROM import_batches
        WHERE import_batch_id = ? OR (importer_name = ? AND input_sha256 = ?)
        """,
        (batch_id, importer_name, digest),
    )
    if len(rows) > 1:
        raise ResultImportError(
            "completed preprocess replay has conflicting import-batch identities"
        )
    if rows:
        return rows[0]

    # A preprocessing run is unique to its sealed execution.  Seeing it without the
    # deterministic result-import batch is partial or deleted replay state, not a
    # first import that may be repaired with INSERT OR IGNORE.
    if connection.execute(
        "SELECT 1 FROM processing_runs WHERE processing_run_id = ?",
        (run_id,),
    ).fetchone():
        raise ResultImportError(
            "preprocess run exists without its exact result-import batch"
        )
    return None


def _expected_preprocess_observations(
    *,
    routing: dict[str, Any] | None,
    run_id: str,
    source_renditions: list[dict[str, Any]],
    created_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    if not routing:
        return [], [], 0
    candidate_count = len(routing["scene_changes"]) + len(
        routing["silence_intervals"]
    )
    duration_ms = routing["coverage"]["duration_ms"]
    if not source_renditions or duration_ms <= 0:
        return [], [], candidate_count

    observations: list[dict[str, Any]] = []
    scores: list[dict[str, Any]] = []
    for rendition in source_renditions:
        rendition_id = rendition["rendition_id"]
        recording_id = rendition["recording_id"]
        for ordinal, scene in enumerate(routing["scene_changes"]):
            timestamp = scene["timestamp_ms"]
            start_ms = min(timestamp, duration_ms - 1)
            observation_id = stable_id(
                "obs",
                run_id,
                rendition_id,
                "scene_change_candidate",
                ordinal,
                timestamp,
            )
            observations.append(
                {
                    "observation_id": observation_id,
                    "observation_kind": "scene_change_candidate",
                    "recording_id": recording_id,
                    "rendition_id": rendition_id,
                    "processing_run_id": run_id,
                    "start_ms": start_ms,
                    "end_ms": start_ms + 1,
                    "visibility": "private",
                    "review_state": "machine",
                    "payload_schema_version": 1,
                    "metadata_json": canonical_json(
                        {
                            "coordinate_space": "rendition_media",
                            "timestamp_ms": timestamp,
                            "routing_only": True,
                        }
                    ),
                    "created_at": created_at,
                }
            )
            scores.append(
                {
                    "observation_score_id": stable_id(
                        "osc", observation_id, "scene_score_percent"
                    ),
                    "observation_id": observation_id,
                    "score_name": "scene_score_percent",
                    "raw_score": float(scene["score_percent"]),
                    "calibrated_probability": None,
                    "calibration_set_id": None,
                    "quality_flags_json": "[]",
                }
            )
        for ordinal, silence in enumerate(routing["silence_intervals"]):
            observation_id = stable_id(
                "obs",
                run_id,
                rendition_id,
                "silence_interval_candidate",
                ordinal,
                silence["start_ms"],
                silence["end_ms"],
            )
            observations.append(
                {
                    "observation_id": observation_id,
                    "observation_kind": "silence_interval_candidate",
                    "recording_id": recording_id,
                    "rendition_id": rendition_id,
                    "processing_run_id": run_id,
                    "start_ms": silence["start_ms"],
                    "end_ms": silence["end_ms"],
                    "visibility": "private",
                    "review_state": "machine",
                    "payload_schema_version": 1,
                    "metadata_json": canonical_json(
                        {
                            "coordinate_space": "rendition_media",
                            "duration_ms": silence["duration_ms"],
                            "routing_only": True,
                        }
                    ),
                    "created_at": created_at,
                }
            )
    return observations, scores, 0


def _verify_completed_preprocess_replay(
    connection: sqlite3.Connection,
    *,
    result: dict[str, Any],
    digest: str,
    importer_name: str,
    batch_row: dict[str, Any],
) -> dict[str, Any]:
    """Verify the full completed catalog footprint using SELECT statements only."""

    records = result["catalog_records"]
    run = result["processing_run"]
    run_id = run["processing_run_id"]
    source_media_id = result["input"]["media_id"]
    started_at = run["started_at"]
    completed_at = run["completed_at"]
    normalized_media_rows = result["_normalized_media_rows"]
    descriptor_by_id = {row["artifact_id"]: row for row in result["artifacts"]}

    expected_run = {
        "processing_run_id": run_id,
        "stage": run["stage"],
        "implementation_version": run["implementation_version"],
        "model_id": None,
        "glossary_revision_id": None,
        "parameters_json": canonical_json(run.get("parameters_json") or {}),
        "environment_json": canonical_json(run.get("environment_json") or {}),
        "random_seed": None,
        "started_at": started_at,
        "completed_at": completed_at,
        "status": run["status"],
        "error_text": None,
    }
    _exact_replay_rows(
        "processing run",
        _query_dicts(
            connection,
            "SELECT * FROM processing_runs WHERE processing_run_id = ?",
            (run_id,),
        ),
        [expected_run],
    )

    expected_inputs = [
        {
            "run_input_id": stable_id(
                "rin",
                row["processing_run_id"],
                row["object_type"],
                row["object_id"],
                row["input_role"],
            ),
            "processing_run_id": row["processing_run_id"],
            "object_type": row["object_type"],
            "object_id": row["object_id"],
            "input_role": row["input_role"],
            "input_sha256": row.get("input_sha256"),
        }
        for row in records["run_inputs"]
    ]
    _exact_replay_rows(
        "run inputs",
        _query_dicts(
            connection,
            "SELECT * FROM run_inputs WHERE processing_run_id = ?",
            (run_id,),
        ),
        expected_inputs,
    )

    expected_media = [
        {
            "media_id": row["media_id"],
            "sha256": row["sha256"],
            "byte_count": row["byte_count"],
            "media_kind": row["media_kind"],
            "mime_type": row.get("mime_type"),
            "container": row.get("container"),
            "duration_ms": row.get("duration_ms"),
            "ffprobe_json": (
                canonical_json(row["ffprobe_json"])
                if row.get("ffprobe_json") is not None
                else None
            ),
            "first_cataloged_at": row["first_cataloged_at"],
            "integrity_state": "verified",
        }
        for row in normalized_media_rows
    ]
    media_ids = [row["media_id"] for row in expected_media]
    placeholders = ",".join("?" for _ in media_ids)
    observed_media = _query_dicts(
        connection,
        f"SELECT * FROM media_objects WHERE media_id IN ({placeholders})",
        tuple(media_ids),
    )
    for row in observed_media:
        if row["ffprobe_json"] is not None:
            try:
                observed_probe = json.loads(row["ffprobe_json"])
            except json.JSONDecodeError as error:
                raise ResultImportError(
                    "completed preprocess replay media probe is invalid JSON"
                ) from error
            _validate_probe_identity(
                observed_probe,
                digest=row["sha256"],
                byte_count=row["byte_count"],
                media_id=row["media_id"],
                label="completed preprocess replay media probe",
            )
            if row["media_id"] == source_media_id:
                observed_format = _object(
                    observed_probe.get("format"),
                    "completed preprocess replay source media probe.format",
                )
                if (
                    row["duration_ms"] is not None
                    and observed_format.get("duration_ms") is not None
                    and observed_format["duration_ms"] != row["duration_ms"]
                ):
                    raise ResultImportError(
                        "completed preprocess replay source media probe duration differs"
                    )
                if observed_probe.get("media") is None:
                    _schema_v1(
                        observed_probe.get("schema_version"),
                        "completed preprocess replay source media probe.schema_version",
                    )
                    _array(
                        observed_probe.get("streams"),
                        "completed preprocess replay source media probe.streams",
                    )
                    _object(
                        observed_probe.get("tool"),
                        "completed preprocess replay source media probe.tool",
                    )
            row["ffprobe_json"] = canonical_json(observed_probe)
    # A media object can legitimately predate this execution: the input usually
    # came from acquisition, and a verified-prior-result execution reuses the
    # same derived bytes.  Reconstruct the exact postconditions of
    # _upsert_media_object rather than pretending this run owns first_cataloged_at
    # or pre-existing input metadata.
    observed_media_by_id = {row["media_id"]: row for row in observed_media}
    expected_media_by_id = {row["media_id"]: row for row in expected_media}
    if set(observed_media_by_id) != set(expected_media_by_id):
        raise ResultImportError(
            "completed preprocess replay media objects are missing or different"
        )
    for media_id, expected_media_row in expected_media_by_id.items():
        observed_media_row = observed_media_by_id[media_id]
        for key in ("media_id", "sha256", "byte_count", "integrity_state"):
            if observed_media_row[key] != expected_media_row[key]:
                raise ResultImportError(
                    "completed preprocess replay media identity differs"
                )
        if media_id == source_media_id:
            if observed_media_row["media_kind"] != expected_media_row["media_kind"]:
                raise ResultImportError(
                    "completed preprocess replay source media kind differs"
                )
            for key in ("mime_type", "container", "duration_ms"):
                if (
                    expected_media_row[key] is not None
                    and observed_media_row[key] != expected_media_row[key]
                ):
                    raise ResultImportError(
                        "completed preprocess replay source media enrichment differs"
                    )
            if (
                expected_media_row["ffprobe_json"] is not None
                and observed_media_row["ffprobe_json"] is None
            ):
                raise ResultImportError(
                    "completed preprocess replay source media probe is missing"
                )
        else:
            for key in (
                "media_kind",
                "mime_type",
                "container",
                "duration_ms",
                "ffprobe_json",
            ):
                if observed_media_row[key] != expected_media_row[key]:
                    raise ResultImportError(
                        "completed preprocess replay derived media differs"
                    )
        observed_cataloged_at = _timestamp(
            observed_media_row["first_cataloged_at"],
            "completed preprocess replay media.first_cataloged_at",
        )
        if _timestamp_value(observed_cataloged_at) > _timestamp_value(
            expected_media_row["first_cataloged_at"]
        ):
            raise ResultImportError(
                "completed preprocess replay media first_cataloged_at differs"
            )

    expected_locations: list[dict[str, Any]] = []
    observed_locations: list[dict[str, Any]] = []
    for row in records["media_locations"]:
        expected_locations.append(
            {
                "media_location_id": stable_id(
                    "mlc", row["media_id"], row["storage_uri"]
                ),
                "media_id": row["media_id"],
                "storage_uri": row["storage_uri"],
                "storage_class": row.get("storage_class") or "local",
                "verified_at": _optional_timestamp(
                    row.get("verified_at"), "replay media location.verified_at"
                ),
                "is_primary": row.get("is_primary", 0),
            }
        )
        observed_locations.extend(
            _query_dicts(
                connection,
                """
                SELECT * FROM media_locations
                WHERE media_id = ? AND storage_uri = ?
                """,
                (row["media_id"], row["storage_uri"]),
            )
        )
    expected_location_keys = {
        (row["media_id"], row["storage_uri"]) for row in expected_locations
    }
    observed_locations_by_key = {
        (row["media_id"], row["storage_uri"]): row
        for row in observed_locations
    }
    if (
        len(observed_locations) != len(expected_locations)
        or set(observed_locations_by_key) != expected_location_keys
    ):
        raise ResultImportError(
            "completed preprocess replay media locations are missing or duplicated"
        )
    for expected_location in expected_locations:
        observed_location = observed_locations_by_key[
            (expected_location["media_id"], expected_location["storage_uri"])
        ]
        if (
            observed_location["media_location_id"]
            != expected_location["media_location_id"]
            or observed_location["is_primary"] < expected_location["is_primary"]
        ):
            raise ResultImportError(
                "completed preprocess replay media location differs"
            )
        if expected_location["media_id"] == source_media_id:
            allowed_storage_classes = {
                expected_location["storage_class"],
                "local_hot_cache",
            }
        else:
            allowed_storage_classes = {expected_location["storage_class"]}
        if observed_location["storage_class"] not in allowed_storage_classes:
            raise ResultImportError(
                "completed preprocess replay media location storage class differs"
            )
        expected_verified_at = expected_location["verified_at"]
        observed_verified_at = observed_location["verified_at"]
        if expected_verified_at is not None:
            if observed_verified_at is None:
                raise ResultImportError(
                    "completed preprocess replay media verification is missing"
                )
            normalized_observed_verified_at = _timestamp(
                observed_verified_at,
                "completed preprocess replay media location.verified_at",
            )
            if _timestamp_value(normalized_observed_verified_at) < _timestamp_value(
                expected_verified_at
            ):
                raise ResultImportError(
                    "completed preprocess replay media verification differs"
                )

    expected_derivations = [
        {
            "child_media_id": row["child_media_id"],
            "parent_media_id": row["parent_media_id"],
            "derivation_kind": row["derivation_kind"],
            "processing_run_id": row["processing_run_id"],
            "metadata_json": canonical_json(row.get("metadata_json") or {}),
        }
        for row in records["media_derivations"]
    ]
    observed_derivations: list[dict[str, Any]] = []
    allowed_derivation_run_ids = {run_id}
    if result["reuse"]["mode"] == "verified_prior_result":
        allowed_derivation_run_ids.add(result["reuse"]["prior_processing_run_id"])
    for expected_derivation in expected_derivations:
        observed_rows = _query_dicts(
            connection,
            """
            SELECT * FROM media_derivations
            WHERE child_media_id = ? AND parent_media_id = ?
              AND derivation_kind = ?
            """,
            (
                expected_derivation["child_media_id"],
                expected_derivation["parent_media_id"],
                expected_derivation["derivation_kind"],
            ),
        )
        if len(observed_rows) != 1:
            raise ResultImportError(
                "completed preprocess replay media derivation is missing or duplicated"
            )
        observed_derivation = observed_rows[0]
        if observed_derivation["processing_run_id"] not in allowed_derivation_run_ids:
            raise ResultImportError(
                "completed preprocess replay media derivation lineage differs"
            )
        if observed_derivation["processing_run_id"] != run_id and not connection.execute(
            "SELECT 1 FROM processing_runs WHERE processing_run_id = ?",
            (observed_derivation["processing_run_id"],),
        ).fetchone():
            raise ResultImportError(
                "completed preprocess replay prior derivation run is missing"
            )
        # The derivation primary key intentionally represents one content
        # relation.  A verified reuse execution therefore retains the earlier
        # run ID when that relation was already cataloged.
        expected_derivation["processing_run_id"] = observed_derivation[
            "processing_run_id"
        ]
        observed_derivations.append(observed_derivation)
    _exact_replay_rows(
        "current-run media derivations",
        _query_dicts(
            connection,
            "SELECT * FROM media_derivations WHERE processing_run_id = ?",
            (run_id,),
        ),
        [
            row
            for row in expected_derivations
            if row["processing_run_id"] == run_id
        ],
    )
    _exact_replay_rows(
        "media derivations",
        observed_derivations,
        expected_derivations,
    )

    expected_artifacts: list[dict[str, Any]] = []
    for row in records["artifacts"]:
        descriptor = descriptor_by_id.get(row["artifact_id"])
        metadata = {}
        if descriptor:
            metadata = {
                key: descriptor[key]
                for key in ("media_kind", "mime_type", "normalized_probe")
                if descriptor.get(key) is not None
            }
        expected_artifacts.append(
            {
                "artifact_id": row["artifact_id"],
                "processing_run_id": row["processing_run_id"],
                "artifact_kind": row["artifact_kind"],
                "storage_uri": row["storage_uri"],
                "sha256": row["sha256"],
                "byte_count": row["byte_count"],
                "schema_version": row["schema_version"],
                "visibility": "private",
                "metadata_json": canonical_json(metadata),
            }
        )
    _exact_replay_rows(
        "artifacts",
        _query_dicts(
            connection,
            "SELECT * FROM artifacts WHERE processing_run_id = ?",
            (run_id,),
        ),
        expected_artifacts,
    )

    job_id = stable_id("job", "media_preprocess", "media", source_media_id)
    attempts = _query_dicts(
        connection,
        "SELECT * FROM job_attempts WHERE job_id = ?",
        (job_id,),
    )
    ordered_attempts = sorted(
        attempts,
        key=lambda row: (
            row["started_at"],
            row["processing_run_id"] or "",
            row["job_attempt_id"],
        ),
    )
    for ordinal, attempt in enumerate(ordered_attempts, 1):
        if attempt["attempt_number"] != ordinal:
            raise ResultImportError(
                "completed preprocess replay job-attempt ordinals differ"
            )
    attempt_id = stable_id("jat", job_id, run_id)
    expected_attempt_number = next(
        (
            ordinal
            for ordinal, attempt in enumerate(ordered_attempts, 1)
            if attempt["job_attempt_id"] == attempt_id
        ),
        None,
    )
    expected_attempt = {
        "job_attempt_id": attempt_id,
        "job_id": job_id,
        "attempt_number": expected_attempt_number,
        "processing_run_id": run_id,
        "started_at": started_at,
        "completed_at": completed_at,
        "status": "completed",
        "error_text": None,
    }
    _exact_replay_rows(
        "job attempt",
        [row for row in attempts if row["processing_run_id"] == run_id],
        [expected_attempt],
    )
    completed_attempts = [
        row for row in attempts if row["completed_at"] is not None
    ]
    expected_job = {
        "job_id": job_id,
        "stage": "media_preprocess",
        "target_type": "media",
        "target_id": source_media_id,
        "priority": 100,
        "state": "completed",
        "max_attempts": max(3, len(attempts)),
        "created_at": min(row["started_at"] for row in attempts),
        "updated_at": max(row["completed_at"] for row in completed_attempts),
    }
    _exact_replay_rows(
        "job",
        _query_dicts(connection, "SELECT * FROM jobs WHERE job_id = ?", (job_id,)),
        [expected_job],
    )

    external_namespace = "media_preprocess_producer_job_id"
    external_value = result["job_id"]
    expected_external = {
        "external_id_id": stable_id(
            "ext",
            "processing_run",
            run_id,
            external_namespace,
            external_value,
        ),
        "object_type": "processing_run",
        "object_id": run_id,
        "namespace": external_namespace,
        "external_value": external_value,
        "confidence_state": "metadata_only",
        "basis": "Completed result envelope job_id",
        "source_id": None,
        "current_external_id_observation_id": None,
    }
    _exact_replay_rows(
        "producer external ID",
        _query_dicts(
            connection,
            """
            SELECT * FROM external_ids
            WHERE object_type = 'processing_run' AND object_id = ?
              AND namespace = ?
            """,
            (run_id, external_namespace),
        ),
        [expected_external],
    )

    all_source_renditions = _query_dicts(
        connection,
        """
        SELECT rendition_id, recording_id, review_state
        FROM renditions WHERE media_id = ?
        ORDER BY recording_id, rendition_id
        """,
        (source_media_id,),
    )
    source_renditions = [
        row for row in all_source_renditions if row["review_state"] != "rejected"
    ]
    expected_derived_renditions: list[dict[str, Any]] = []
    expected_derived_rendition_ids: set[str] = set()
    derived_rendition_attempt_count = 0
    for derivation in records["media_derivations"]:
        for source_rendition in source_renditions:
            derived_rendition_attempt_count += 1
            rendition_id = stable_id(
                "rnd",
                source_rendition["recording_id"],
                derivation["child_media_id"],
                derivation["derivation_kind"],
            )
            if rendition_id in expected_derived_rendition_ids:
                continue
            expected_derived_rendition_ids.add(rendition_id)
            expected_derived_renditions.append(
                {
                    "rendition_id": rendition_id,
                    "recording_id": source_rendition["recording_id"],
                    "media_id": derivation["child_media_id"],
                    "rendition_kind": derivation["derivation_kind"],
                    "label": "Machine-derived media",
                    "review_state": "unreviewed",
                    "metadata_json": canonical_json(
                        {
                            "derived_from_rendition_id": source_rendition[
                                "rendition_id"
                            ],
                            "publication_state": "withheld_by_default",
                        }
                    ),
                }
            )
    observed_derived_renditions: list[dict[str, Any]] = []
    if records["media_derivations"] and all_source_renditions:
        child_ids = sorted(
            {row["child_media_id"] for row in records["media_derivations"]}
        )
        recording_ids = sorted(
            {row["recording_id"] for row in all_source_renditions}
        )
        child_placeholders = ",".join("?" for _ in child_ids)
        recording_placeholders = ",".join("?" for _ in recording_ids)
        observed_derived_renditions = _query_dicts(
            connection,
            f"""
            SELECT * FROM renditions
            WHERE media_id IN ({child_placeholders})
              AND recording_id IN ({recording_placeholders})
            """,
            tuple(child_ids + recording_ids),
        )
    _exact_replay_rows(
        "derived renditions",
        observed_derived_renditions,
        expected_derived_renditions,
    )

    expected_observations, expected_scores, deferred = (
        _expected_preprocess_observations(
            routing=result.get("routing"),
            run_id=run_id,
            source_renditions=source_renditions,
            created_at=completed_at,
        )
    )
    _exact_replay_rows(
        "routing observations",
        _query_dicts(
            connection,
            "SELECT * FROM observations WHERE processing_run_id = ?",
            (run_id,),
        ),
        expected_observations,
    )
    _exact_replay_rows(
        "routing observation scores",
        _query_dicts(
            connection,
            """
            SELECT scores.* FROM observation_scores AS scores
            JOIN observations AS observations
              ON observations.observation_id = scores.observation_id
            WHERE observations.processing_run_id = ?
            """,
            (run_id,),
        ),
        expected_scores,
    )

    statistics = {
        "artifacts": len(records["artifacts"]),
        "derived_renditions": derived_rendition_attempt_count,
        "jobs": 1,
        "media_derivations": len(records["media_derivations"]),
        "media_locations": len(records["media_locations"]),
        "media_objects": len(normalized_media_rows),
        "observations": len(expected_observations),
        "processing_runs": 1,
        "publication_decisions_added": 0,
        "routing_observations_deferred": deferred,
        "run_inputs": len(records["run_inputs"]),
        "source_renditions": len(source_renditions),
    }
    expected_batch = {
        "import_batch_id": stable_id("imp", importer_name, digest),
        "importer_name": importer_name,
        "importer_version": __version__,
        "input_sha256": digest,
        "source_snapshot_date": None,
        "started_at": started_at,
        "completed_at": completed_at,
        "status": "completed",
        "statistics_json": canonical_json(statistics),
    }
    _exact_replay_rows("import batch", [batch_row], [expected_batch])
    return {
        "import_batch_id": expected_batch["import_batch_id"],
        "job_id": job_id,
        "processing_run_id": run_id,
        "source_media_id": source_media_id,
        **statistics,
    }


def _private_policy_seal_binding(
    result: dict[str, Any],
    *,
    result_path: str | Path,
    result_canonical_sha256: str,
    result_raw_sha256: str,
    artifact_root: str | Path | None,
    receipt_path: str | Path | None,
) -> dict[str, str] | None:
    policy = result.get("handling_policy")
    if policy is None:
        if artifact_root is not None or receipt_path is not None:
            raise ResultImportError(
                "private seal inputs are valid only for a policy-bearing acquisition result"
            )
        return None
    if artifact_root is None or receipt_path is None:
        raise ResultImportError(
            "policy-bearing acquisition import requires --private-artifact-root and "
            "--private-seal-receipt"
        )
    try:
        receipt = load_seal_receipt(receipt_path)
        validate_private_acquisition_seal_receipt(artifact_root, receipt)
    except PrivateAcquisitionError as error:
        raise ResultImportError(f"private acquisition seal rejected: {error}") from error
    plan = receipt["plan"]
    artifact_rows = {
        row["role"]: row for row in plan["artifacts"]
    }
    expected_result_path = Path(os.path.abspath(os.fspath(artifact_root))).joinpath(
        *PurePosixPath(artifact_rows["result"]["relative_path"]).parts
    )
    if Path(os.path.abspath(os.fspath(result_path))) != expected_result_path:
        raise ResultImportError(
            "imported result path differs from the portable path in its seal receipt"
        )
    source_row = result["catalog_records"]["sources"][0]
    expected = {
        "work_order_sha256": result["work_order_sha256"],
        "result_canonical_sha256": result_canonical_sha256,
        "media_id": result["admission"]["media_id"],
        "media_sha256": result["admission"]["sha256"],
        "media_byte_count": result["admission"]["byte_count"],
        "source": {
            "source_id": source_row["source_id"],
            "platform": result["source"]["platform"],
            "source_kind": result["source"]["source_kind"],
            "native_id": result["source"]["native_id"],
            "access_state": "unknown",
        },
        "handling_policy": policy,
        "source_byte_identity_claimed": False,
    }
    for key, value in expected.items():
        if plan.get(key) != value:
            raise ResultImportError(
                f"private acquisition seal receipt {key} differs from the result"
            )
    if artifact_rows["result"]["sha256"] != result_raw_sha256:
        raise ResultImportError(
            "private acquisition seal receipt does not bind the exact result bytes"
        )
    return {
        "seal_plan_sha256": receipt["plan_sha256"],
        "seal_validated_at": receipt["validated_at"],
    }


def _existing_private_restrictions(
    connection: sqlite3.Connection, *, source_id_value: str, media_id: str
) -> list[sqlite3.Row]:
    try:
        return connection.execute(
            """
            SELECT acquisition_handling_restriction_id,
                   result_canonical_sha256, publication_disposition
            FROM acquisition_handling_restrictions
            WHERE source_id = ? OR media_id = ?
            ORDER BY restriction_sequence
            """,
            (source_id_value, media_id),
        ).fetchall()
    except sqlite3.OperationalError as error:
        raise ResultImportError(
            "private acquisition restriction migration 0030 is required before "
            "acquisition result import"
        ) from error


def _insert_acquisition_handling_restriction(
    connection: sqlite3.Connection,
    *,
    result: dict[str, Any],
    result_canonical_sha256: str,
    batch_id: str,
    canonical_source_id: str,
    media_id: str,
    seal_binding: dict[str, str] | None,
) -> str | None:
    policy = result.get("handling_policy")
    existing = _existing_private_restrictions(
        connection, source_id_value=canonical_source_id, media_id=media_id
    )
    exact = next(
        (
            row
            for row in existing
            if row["result_canonical_sha256"] == result_canonical_sha256
        ),
        None,
    )
    if policy is None:
        if existing:
            raise ResultImportError(
                "acquisition result omits an effective source/media handling_policy"
            )
        return None
    if seal_binding is None:
        raise ResultImportError("policy-bearing restriction lacks its seal binding")
    if exact is None and any(
        row["publication_disposition"] == "never_publish" for row in existing
    ) and policy["publication_disposition"] != "never_publish":
        raise ResultImportError(
            "acquisition result attempts to weaken an effective never_publish restriction"
        )
    source_metadata = result["catalog_records"]["sources"][0]["metadata_json"]
    policy_json = canonical_json(policy)
    values = {
        "acquisition_handling_restriction_id": stable_id(
            "ahr", result_canonical_sha256, canonical_source_id, media_id
        ),
        "import_batch_id": batch_id,
        "source_id": canonical_source_id,
        "media_id": media_id,
        "work_order_sha256": result["work_order_sha256"],
        "result_canonical_sha256": result_canonical_sha256,
        "source_metadata_sha256": sha256_bytes(
            canonical_json(source_metadata).encode("utf-8")
        ),
        "policy_sha256": sha256_bytes(policy_json.encode("utf-8")),
        "seal_plan_sha256": seal_binding["seal_plan_sha256"],
        "storage_scope": policy["storage_scope"],
        "publication_disposition": policy["publication_disposition"],
        "publication_authority": policy["publication_authority"],
        "basis": policy["basis"],
        "policy_json": policy_json,
        "seal_validated_at": seal_binding["seal_validated_at"],
        "recorded_at": result["completed_at"],
    }
    if exact is not None:
        stored = connection.execute(
            """
            SELECT acquisition_handling_restriction_id, import_batch_id, source_id,
                   media_id, work_order_sha256, result_canonical_sha256,
                   source_metadata_sha256, policy_sha256, storage_scope,
                   seal_plan_sha256,
                   publication_disposition, publication_authority, basis,
                   policy_json, seal_validated_at, recorded_at
            FROM acquisition_handling_restrictions
            WHERE result_canonical_sha256 = ?
            """,
            (result_canonical_sha256,),
        ).fetchone()
        if stored is None or dict(stored) != values:
            raise ResultImportError(
                "existing acquisition handling restriction differs from exact replay"
            )
        return values["acquisition_handling_restriction_id"]
    connection.execute(
        """
        INSERT INTO acquisition_handling_restrictions(
            acquisition_handling_restriction_id, import_batch_id, source_id,
            media_id, work_order_sha256, result_canonical_sha256,
            source_metadata_sha256, policy_sha256, storage_scope,
            seal_plan_sha256,
            publication_disposition, publication_authority, basis, policy_json,
            seal_validated_at, recorded_at
        ) VALUES(
            :acquisition_handling_restriction_id, :import_batch_id, :source_id,
            :media_id, :work_order_sha256, :result_canonical_sha256,
            :source_metadata_sha256, :policy_sha256, :storage_scope,
            :seal_plan_sha256,
            :publication_disposition, :publication_authority, :basis, :policy_json,
            :seal_validated_at, :recorded_at
        )
        """,
        values,
    )
    return values["acquisition_handling_restriction_id"]


def import_acquisition_result(
    connection: sqlite3.Connection,
    result_path: str | Path,
    *,
    private_artifact_root: str | Path | None = None,
    private_seal_receipt_path: str | Path | None = None,
) -> dict[str, Any]:
    raw, digest, raw_digest = _result_file(result_path)
    result = validate_acquisition_result(raw)
    seal_binding = _private_policy_seal_binding(
        result,
        result_path=result_path,
        result_canonical_sha256=digest,
        result_raw_sha256=raw_digest,
        artifact_root=private_artifact_root,
        receipt_path=private_seal_receipt_path,
    )
    _verify_exact_file(
        result["admission"]["path"],
        expected_sha256=result["admission"]["sha256"],
        expected_byte_count=result["admission"]["byte_count"],
        label="acquisition admitted media",
    )
    records = result["catalog_records"]
    source_row = records["sources"][0]
    media_row = _validate_media_row(records["media_objects"][0], "acquisition media object")
    producer_source_id = source_row["source_id"]
    completed_at = result["completed_at"]
    started_at = result["started_at"]
    importer_name = "acquisition_result_v1"
    with transaction(connection):
        batch_id = _begin_result_batch(
            connection,
            importer_name=importer_name,
            digest=digest,
            started_at=started_at,
            observation_at=completed_at,
        )
        canonical_source_id = _upsert_source(connection, source_row, batch_id=batch_id)
        _insert_external_id(
            connection,
            object_type="source",
            object_id=canonical_source_id,
            namespace="acquisition_result_source_id",
            value=producer_source_id,
            basis="Producer-local source identifier translated during result ingestion",
        )
        media_id = _upsert_media_object(connection, media_row)
        _upsert_media_location(connection, records["media_locations"][0])
        _upsert_media_source(
            connection,
            records["media_sources"][0],
            canonical_source_id=canonical_source_id,
        )
        handling_restriction_id = _insert_acquisition_handling_restriction(
            connection,
            result=result,
            result_canonical_sha256=digest,
            batch_id=batch_id,
            canonical_source_id=canonical_source_id,
            media_id=media_id,
            seal_binding=seal_binding,
        )
        run_id = stable_id(
            "run", "media_acquisition", result["work_order_sha256"], media_id
        )
        run_row = {
            "processing_run_id": run_id,
            "stage": "media_acquisition",
            "implementation_version": "acquisition-result-contract-v1",
            "parameters_json": {
                "adapter": result["adapter"],
                "work_order_sha256": result["work_order_sha256"],
                **(
                    {"handling_policy": result["handling_policy"]}
                    if "handling_policy" in result
                    else {}
                ),
            },
            "environment_json": {},
            "started_at": started_at,
            "completed_at": completed_at,
            "status": "completed",
        }
        _insert_processing_run(connection, run_row)
        _insert_run_input(
            connection,
            {
                "processing_run_id": run_id,
                "object_type": "source",
                "object_id": canonical_source_id,
                "input_role": "retrieval_source",
                "input_sha256": None,
            },
        )
        job_id, _ = _upsert_job_and_attempt(
            connection,
            stage="media_acquisition",
            target_type="source",
            target_id=canonical_source_id,
            run_id=run_id,
            producer_job_id=result["job_id"],
            started_at=started_at,
            completed_at=completed_at,
        )
        rendition_ids = _source_renditions(
            connection,
            source_id_value=canonical_source_id,
            media_id=media_id,
        )
        statistics = {
            "artifacts": 0,
            "jobs": 1,
            "media_locations": 1,
            "media_objects": 1,
            "media_sources": 1,
            "observations": 0,
            "processing_runs": 1,
            "publication_decisions_added": 0,
            "renditions": len(rendition_ids),
            "sources": 1,
            **(
                {"acquisition_handling_restrictions": 1}
                if handling_restriction_id is not None
                else {}
            ),
        }
        _complete_result_batch(
            connection,
            batch_id,
            completed_at,
            statistics,
            observation_at=completed_at,
        )
    return {
        "import_batch_id": batch_id,
        "job_id": job_id,
        "media_id": media_id,
        "processing_run_id": run_id,
        "source_id": canonical_source_id,
        **(
            {"acquisition_handling_restriction_id": handling_restriction_id}
            if handling_restriction_id is not None
            else {}
        ),
        **statistics,
    }


def import_preprocess_result(
    connection: sqlite3.Connection, result_path: str | Path
) -> dict[str, Any]:
    raw, digest, _ = _result_file(result_path)
    result = validate_preprocess_result(raw)
    actual_result_path = Path(result_path).resolve(strict=True)
    if actual_result_path != Path(result["result_path"]).resolve(strict=True):
        raise ResultImportError("preprocess envelope result_path is not the imported file")
    try:
        result_stat = actual_result_path.lstat()
    except OSError as error:
        raise ResultImportError(f"preprocess result envelope cannot be inspected: {error}") from error
    if (
        stat.S_ISLNK(result_stat.st_mode)
        or not stat.S_ISREG(result_stat.st_mode)
        or result_stat.st_mode & 0o222
    ):
        raise ResultImportError("preprocess result envelope must be a sealed regular file")
    _verify_exact_file(
        result["input"]["path"],
        expected_sha256=result["input"]["sha256"],
        expected_byte_count=result["input"]["byte_count"],
        label="preprocess input media",
    )
    for index, artifact in enumerate(result["artifacts"]):
        _verify_exact_file(
            artifact["path"],
            expected_sha256=artifact["sha256"],
            expected_byte_count=artifact["byte_count"],
            label=f"preprocess artifact[{index}]",
            require_sealed=True,
        )
    reuse = result["reuse"]
    if reuse["mode"] == "verified_prior_result":
        prior_path = Path(reuse["prior_result_path"])
        try:
            prior_stat = prior_path.lstat()
            prior_body = prior_path.read_bytes()
        except OSError as error:
            raise ResultImportError(f"preprocess prior result cannot be verified: {error}") from error
        if (
            stat.S_ISLNK(prior_stat.st_mode)
            or not stat.S_ISREG(prior_stat.st_mode)
            or prior_stat.st_mode & 0o222
        ):
            raise ResultImportError("preprocess prior result must be a sealed regular file")
        if sha256_bytes(prior_body) != reuse["prior_result_sha256"]:
            raise ResultImportError("preprocess prior result SHA-256 disagrees with reuse lineage")
        try:
            prior = _object(json.loads(prior_body), "preprocess prior result")
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ResultImportError("preprocess prior result is invalid JSON") from error
        prior_run = _object(prior.get("processing_run"), "preprocess prior processing_run")
        prior_layout = _object(prior.get("layout"), "preprocess prior layout")
        if (
            prior.get("status") != "completed"
            or prior.get("dry_run") is not False
            or prior.get("result_path") != str(prior_path)
            or prior_run.get("processing_run_id") != reuse["prior_processing_run_id"]
            or prior_layout.get("recipe_sha256") != result["layout"]["recipe_sha256"]
        ):
            raise ResultImportError("preprocess prior result identity disagrees with reuse lineage")
        prior_artifacts_raw = _array(
            prior.get("artifacts"), "preprocess prior result.artifacts", nonempty=True
        )
        prior_artifacts: dict[str, dict[str, Any]] = {}
        for index, value in enumerate(prior_artifacts_raw):
            artifact = _object(value, f"preprocess prior artifact[{index}]")
            kind = _string(
                artifact.get("artifact_kind"),
                f"preprocess prior artifact[{index}].artifact_kind",
            )
            assert kind is not None
            if kind in prior_artifacts:
                raise ResultImportError("preprocess prior result has duplicate artifact kinds")
            prior_artifacts[kind] = artifact
        current_by_kind = {artifact["artifact_kind"]: artifact for artifact in result["artifacts"]}
        if set(prior_artifacts) != set(current_by_kind):
            raise ResultImportError("preprocess reused artifact set differs from prior result")
        for kind, current in current_by_kind.items():
            prior_artifact = prior_artifacts[kind]
            if (
                prior_artifact.get("sha256") != current["sha256"]
                or prior_artifact.get("byte_count") != current["byte_count"]
                or prior_artifact.get("media_kind") != current["media_kind"]
                or prior_artifact.get("mime_type") != current["mime_type"]
            ):
                raise ResultImportError("preprocess reused artifact differs from prior result")
            _verify_exact_file(
                prior_artifact.get("path"),
                expected_sha256=current["sha256"],
                expected_byte_count=current["byte_count"],
                label=f"preprocess prior {kind} artifact",
                require_sealed=True,
            )
    records = result["catalog_records"]
    run = result["processing_run"]
    run_id = run["processing_run_id"]
    source_media_id = result["input"]["media_id"]
    started_at = run["started_at"]
    completed_at = run["completed_at"]
    normalized_media_rows = result["_normalized_media_rows"]
    descriptor_by_id = {row["artifact_id"]: row for row in result["artifacts"]}
    descriptor_by_kind = {
        row["artifact_kind"]: row for row in result["artifacts"]
    }
    source_media_row = next(
        row for row in normalized_media_rows if row["media_id"] == source_media_id
    )
    probe_descriptor = descriptor_by_kind["ffprobe_normalized_json"]
    probe_document = _read_exact_json_file(
        probe_descriptor["path"],
        expected_sha256=probe_descriptor["sha256"],
        expected_byte_count=probe_descriptor["byte_count"],
        label="preprocess normalized source probe artifact",
    )
    if canonical_json(probe_document) != canonical_json(source_media_row["ffprobe_json"]):
        raise ResultImportError("preprocess source probe artifact disagrees with media_objects")
    routing_descriptor = descriptor_by_kind.get("scene_silence_routing_json")
    if routing_descriptor is None:
        if result["routing"] is not None:
            raise ResultImportError("preprocess routing data is missing its JSON artifact")
    else:
        routing_document = _read_exact_json_file(
            routing_descriptor["path"],
            expected_sha256=routing_descriptor["sha256"],
            expected_byte_count=routing_descriptor["byte_count"],
            label="preprocess routing JSON artifact",
        )
        if canonical_json(routing_document) != canonical_json(result["routing"]):
            raise ResultImportError("preprocess routing artifact disagrees with envelope routing")
    importer_name = "media_preprocess_result_v1"
    owns_replay_snapshot = not connection.in_transaction
    if owns_replay_snapshot:
        connection.execute("BEGIN")
    replay_response: dict[str, Any] | None = None
    try:
        replay_batch = _preprocess_replay_batch(
            connection,
            importer_name=importer_name,
            digest=digest,
            run_id=run_id,
        )
        if replay_batch is not None:
            replay_response = _verify_completed_preprocess_replay(
                connection,
                result=result,
                digest=digest,
                importer_name=importer_name,
                batch_row=replay_batch,
            )
    finally:
        if owns_replay_snapshot:
            connection.rollback()
    if replay_response is not None:
        return replay_response
    with transaction(connection):
        batch_id = _begin_result_batch(
            connection,
            importer_name=importer_name,
            digest=digest,
            started_at=started_at,
        )
        for row in normalized_media_rows:
            _upsert_media_object(connection, row)
        for row in records["media_locations"]:
            _upsert_media_location(connection, row)
        _insert_processing_run(connection, run)
        for row in records["run_inputs"]:
            _insert_run_input(connection, row)
        for row in records["media_derivations"]:
            connection.execute(
                """
                INSERT OR IGNORE INTO media_derivations(
                    child_media_id, parent_media_id, derivation_kind,
                    processing_run_id, metadata_json
                ) VALUES(?, ?, ?, ?, ?)
                """,
                (
                    row["child_media_id"],
                    row["parent_media_id"],
                    row["derivation_kind"],
                    row["processing_run_id"],
                    canonical_json(row.get("metadata_json") or {}),
                ),
            )
        for row in records["artifacts"]:
            _insert_artifact(
                connection,
                row,
                descriptor=descriptor_by_id.get(row["artifact_id"]),
            )
        job_id, _ = _upsert_job_and_attempt(
            connection,
            stage="media_preprocess",
            target_type="media",
            target_id=source_media_id,
            run_id=run_id,
            producer_job_id=result["job_id"],
            started_at=started_at,
            completed_at=completed_at,
        )
        source_rendition_ids, derived_rendition_ids = _derived_renditions(
            connection,
            source_media_id=source_media_id,
            derivations=records["media_derivations"],
        )
        observations, deferred = _insert_routing_observations(
            connection,
            routing=result.get("routing"),
            run_id=run_id,
            rendition_ids=source_rendition_ids,
            created_at=completed_at,
        )
        statistics = {
            "artifacts": len(records["artifacts"]),
            "derived_renditions": len(derived_rendition_ids),
            "jobs": 1,
            "media_derivations": len(records["media_derivations"]),
            "media_locations": len(records["media_locations"]),
            "media_objects": len(normalized_media_rows),
            "observations": observations,
            "processing_runs": 1,
            "publication_decisions_added": 0,
            "routing_observations_deferred": deferred,
            "run_inputs": len(records["run_inputs"]),
            "source_renditions": len(source_rendition_ids),
        }
        _complete_result_batch(connection, batch_id, completed_at, statistics)
    return {
        "import_batch_id": batch_id,
        "job_id": job_id,
        "processing_run_id": run_id,
        "source_media_id": source_media_id,
        **statistics,
    }
