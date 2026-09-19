"""Strict private administration of source-anchored entity and event maps.

This importer is intentionally conservative.  A manifest may add private map
records and candidate catalog links, but it cannot create identity assertions,
publication decisions, gate clearances, or public visibility.  Source media is the
anchor: every appearance and event-evidence edge names a current source, recording,
and rendition tuple admitted by the catalog.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from . import __version__
from .asr_result_importer import _absolute_observed_path, _stable_read, _verify_hash
from .db import transaction, utc_now
from .ids import stable_id
from .importers import canonical_json
from .result_importers import ResultImportError


SCHEMA_VERSION = 1
IMPORTER_NAME = "entity_event_map_manifest_v1"
MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_ITEMS_PER_COLLECTION = 10_000
MAX_TOTAL_ITEMS = 50_000
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
YEAR_RE = re.compile(r"^\d{4}$")

ENTITY_TYPES = {
    "person",
    "community_figure",
    "animal",
    "place",
    "organization",
    "platform",
    "term",
    "object",
    "unknown",
}
PRIVACY_CLASSIFICATIONS = {
    "public_figure",
    "living_private_person",
    "deceased_person",
    "non_person",
    "unknown",
}
ALIAS_KINDS = {
    "name",
    "nickname",
    "display_name",
    "username",
    "handle",
    "account_handle",
    "historical_name",
    "other",
}
HANDLE_ALIAS_KINDS = {"username", "handle", "account_handle"}
EVIDENCE_BASIS_KINDS = {
    "direct_media_observation",
    "subject_unverified_claim",
    "third_party_unverified_claim",
    "documentary_context",
}
SUPPORT_KINDS = {"direct", "contextual", "corroborating", "contradicting"}
DATE_PRECISIONS = {"day", "month", "year", "range", "circa", "unknown"}
DATE_CERTAINTIES = {"certain", "probable", "uncertain", "disputed"}
REVIEWED_TRANSCRIPT_STATES = {"human_corrected", "media_checked"}


class EntityEventMapManifestError(ResultImportError):
    """The private mapping manifest is malformed, unsafe, or inconsistent."""


@dataclass(frozen=True)
class EntityEventMapManifest:
    value: dict[str, Any]
    input_sha256: str
    path: Path

    @property
    def manifest_id(self) -> str:
        return self.value["manifest_id"]


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise EntityEventMapManifestError(
                f"entity/event map manifest contains duplicate JSON key {key!r}"
            )
        result[key] = value
    return result


def _exact_keys(
    value: dict[str, Any],
    label: str,
    required: set[str],
    optional: set[str] | None = None,
) -> None:
    optional = optional or set()
    missing = sorted(required - set(value))
    unknown = sorted(set(value) - required - optional)
    if missing or unknown:
        details: list[str] = []
        if missing:
            details.append(f"missing {missing}")
        if unknown:
            details.append(f"unknown {unknown}")
        raise EntityEventMapManifestError(f"{label} has " + "; ".join(details))


def _object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise EntityEventMapManifestError(f"{label} must be an object")
    return value


def _array(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise EntityEventMapManifestError(f"{label} must be an array")
    if len(value) > MAX_ITEMS_PER_COLLECTION:
        raise EntityEventMapManifestError(
            f"{label} exceeds the {MAX_ITEMS_PER_COLLECTION}-item limit"
        )
    return value


def _text(
    value: object,
    label: str,
    *,
    maximum: int = 8_192,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str) or (not allow_empty and not value.strip()):
        raise EntityEventMapManifestError(f"{label} must be a non-empty string")
    if len(value) > maximum or "\x00" in value:
        raise EntityEventMapManifestError(f"{label} must be a bounded safe string")
    return value


def _nullable_text(value: object, label: str, *, maximum: int = 8_192) -> str | None:
    return None if value is None else _text(value, label, maximum=maximum)


def _identifier(value: object, label: str) -> str:
    result = _text(value, label, maximum=256)
    if not IDENTIFIER_RE.fullmatch(result):
        raise EntityEventMapManifestError(
            f"{label} contains unsupported identifier characters"
        )
    return result


def _slug(value: object, label: str) -> str:
    result = _text(value, label, maximum=160)
    if not SLUG_RE.fullmatch(result):
        raise EntityEventMapManifestError(
            f"{label} must be a lowercase ASCII slug separated by hyphens"
        )
    return result


def _enum(value: object, label: str, allowed: set[str]) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise EntityEventMapManifestError(f"{label} must be one of {sorted(allowed)}")
    return value


def _boolean(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise EntityEventMapManifestError(f"{label} must be a boolean")
    return value


def _integer(value: object, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise EntityEventMapManifestError(f"{label} must be an integer >= {minimum}")
    return value


def _nullable_identifier(value: object, label: str) -> str | None:
    return None if value is None else _identifier(value, label)


def _canonical_timestamp(value: object, label: str) -> str:
    text = _text(value, label, maximum=64)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise EntityEventMapManifestError(f"{label} must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None:
        raise EntityEventMapManifestError(f"{label} must include a UTC offset")
    parsed = parsed.astimezone(timezone.utc)
    normalized = parsed.isoformat(timespec="seconds").replace("+00:00", "Z")
    if parsed.microsecond or text != normalized:
        raise EntityEventMapManifestError(
            f"{label} must be a whole-second canonical UTC timestamp ending in Z"
        )
    return normalized


def _metadata(value: object, label: str) -> dict[str, Any]:
    result = _object(value, label)
    if "_entity_event_map" in result:
        raise EntityEventMapManifestError(
            f"{label} may not set the reserved _entity_event_map key"
        )
    try:
        encoded = canonical_json(result)
    except (TypeError, ValueError) as error:
        raise EntityEventMapManifestError(f"{label} is not JSON-serializable") from error
    if len(encoded.encode("utf-8")) > 64 * 1024:
        raise EntityEventMapManifestError(f"{label} exceeds 64 KiB")
    return result


def _optional_day(value: object, label: str) -> str | None:
    if value is None:
        return None
    text = _text(value, label, maximum=10)
    if not DATE_RE.fullmatch(text):
        raise EntityEventMapManifestError(f"{label} must be YYYY-MM-DD or null")
    try:
        date.fromisoformat(text)
    except ValueError as error:
        raise EntityEventMapManifestError(f"{label} is not a valid calendar day") from error
    return text


def _date_value(value: object, label: str) -> str | None:
    if value is None:
        return None
    text = _text(value, label, maximum=10)
    if DATE_RE.fullmatch(text):
        try:
            date.fromisoformat(text)
        except ValueError as error:
            raise EntityEventMapManifestError(f"{label} is not a valid date") from error
        return text
    if MONTH_RE.fullmatch(text):
        month = int(text[-2:])
        if not 1 <= month <= 12:
            raise EntityEventMapManifestError(f"{label} has an invalid month")
        return text
    if YEAR_RE.fullmatch(text):
        return text
    raise EntityEventMapManifestError(f"{label} must be YYYY, YYYY-MM, YYYY-MM-DD, or null")


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise EntityEventMapManifestError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _parse_local_evidence(value: object, label: str) -> dict[str, Any] | None:
    if value is None:
        return None
    item = _object(value, label)
    _exact_keys(item, label, {"artifact_id", "path", "sha256", "byte_count"})
    try:
        path = _absolute_observed_path(item["path"], f"{label}.path")
    except ResultImportError as error:
        raise EntityEventMapManifestError(str(error)) from error
    for forbidden in (
        REPOSITORY_ROOT / "public",
        REPOSITORY_ROOT / "dist",
        REPOSITORY_ROOT / "src" / "data" / "corpus",
    ):
        try:
            path.relative_to(forbidden)
        except ValueError:
            continue
        raise EntityEventMapManifestError(
            f"{label}.path must remain outside public/static build directories"
        )
    return {
        "artifact_id": _identifier(item["artifact_id"], f"{label}.artifact_id"),
        "path": path,
        "sha256": _sha256(item["sha256"], f"{label}.sha256"),
        "byte_count": _integer(item["byte_count"], f"{label}.byte_count"),
    }


def _verify_local_hash(
    path: Path, digest: str, byte_count: int, label: str
) -> None:
    try:
        _verify_hash(path, digest, byte_count, label)
    except ResultImportError as error:
        raise EntityEventMapManifestError(str(error)) from error


def _unique_ids(items: Iterable[dict[str, Any]], key: str, label: str) -> None:
    values = [item[key] for item in items]
    duplicates = sorted(value for value, count in Counter(values).items() if count > 1)
    if duplicates:
        raise EntityEventMapManifestError(f"{label} contains duplicate IDs {duplicates[:10]}")


def _reject_duplicate_natural_keys(
    items: Iterable[dict[str, Any]], keys: tuple[str, ...], label: str
) -> None:
    seen: set[tuple[object, ...]] = set()
    for item in items:
        natural = tuple(item[key] for key in keys)
        if natural in seen:
            raise EntityEventMapManifestError(
                f"{label} contains duplicate natural key {natural!r}"
            )
        seen.add(natural)


def _parse_entity(raw: object, index: int) -> dict[str, Any]:
    label = f"entities[{index}]"
    item = _object(raw, label)
    _exact_keys(
        item,
        label,
        {
            "entity_id",
            "entity_type",
            "canonical_label",
            "slug",
            "privacy_classification",
            "metadata",
        },
    )
    entity_type = _enum(item["entity_type"], f"{label}.entity_type", ENTITY_TYPES)
    privacy = _enum(
        item["privacy_classification"],
        f"{label}.privacy_classification",
        PRIVACY_CLASSIFICATIONS,
    )
    if entity_type not in {"person", "community_figure", "unknown"} and privacy not in {
        "non_person",
        "unknown",
    }:
        raise EntityEventMapManifestError(
            f"{label}.privacy_classification is incompatible with entity_type"
        )
    return {
        "entity_id": _identifier(item["entity_id"], f"{label}.entity_id"),
        "entity_type": entity_type,
        "canonical_label": _text(
            item["canonical_label"], f"{label}.canonical_label", maximum=1_000
        ),
        "slug": _slug(item["slug"], f"{label}.slug"),
        "privacy_classification": privacy,
        "metadata": _metadata(item["metadata"], f"{label}.metadata"),
    }


def _parse_alias(raw: object, index: int) -> dict[str, Any]:
    label = f"aliases[{index}]"
    item = _object(raw, label)
    _exact_keys(
        item,
        label,
        {
            "entity_alias_id",
            "entity_id",
            "alias",
            "alias_kind",
            "source_id",
            "valid_from",
            "valid_to",
            "sensitive",
            "privacy_review_required",
        },
    )
    valid_from = _optional_day(item["valid_from"], f"{label}.valid_from")
    valid_to = _optional_day(item["valid_to"], f"{label}.valid_to")
    if valid_from is not None and valid_to is not None and valid_to < valid_from:
        raise EntityEventMapManifestError(f"{label} valid_to precedes valid_from")
    return {
        "entity_alias_id": _identifier(
            item["entity_alias_id"], f"{label}.entity_alias_id"
        ),
        "entity_id": _identifier(item["entity_id"], f"{label}.entity_id"),
        "alias": _text(item["alias"], f"{label}.alias", maximum=1_000),
        "alias_kind": _enum(item["alias_kind"], f"{label}.alias_kind", ALIAS_KINDS),
        "source_id": _identifier(item["source_id"], f"{label}.source_id"),
        "valid_from": valid_from,
        "valid_to": valid_to,
        "sensitive": _boolean(item["sensitive"], f"{label}.sensitive"),
        "privacy_review_required": _boolean(
            item["privacy_review_required"], f"{label}.privacy_review_required"
        ),
    }


def _parse_timed_edge(raw: object, index: int, collection: str) -> dict[str, Any]:
    label = f"{collection}[{index}]"
    item = _object(raw, label)
    identifier_key = "appearance_id" if collection == "appearances" else "event_evidence_id"
    subject_key = "entity_id" if collection == "appearances" else "event_id"
    required = {
        identifier_key,
        subject_key,
        "source_id",
        "recording_id",
        "rendition_id",
        "start_ms",
        "end_ms",
        "evidence_basis_kind",
        "transcript_revision_id",
        "local_evidence",
        "basis",
    }
    if collection == "appearances":
        required.add("appearance_role")
    else:
        required.add("support_kind")
    _exact_keys(item, label, required)

    start_raw = item["start_ms"]
    end_raw = item["end_ms"]
    if collection == "appearances" or start_raw is not None or end_raw is not None:
        if start_raw is None or end_raw is None:
            raise EntityEventMapManifestError(
                f"{label} start_ms and end_ms must both be integers or both be null"
            )
        start_ms = _integer(start_raw, f"{label}.start_ms")
        end_ms = _integer(end_raw, f"{label}.end_ms", minimum=1)
        if end_ms <= start_ms:
            raise EntityEventMapManifestError(
                f"{label} must use a non-empty half-open [start_ms, end_ms) interval"
            )
    else:
        start_ms = None
        end_ms = None

    result = {
        identifier_key: _identifier(item[identifier_key], f"{label}.{identifier_key}"),
        subject_key: _identifier(item[subject_key], f"{label}.{subject_key}"),
        "source_id": _identifier(item["source_id"], f"{label}.source_id"),
        "recording_id": _identifier(item["recording_id"], f"{label}.recording_id"),
        "rendition_id": _identifier(item["rendition_id"], f"{label}.rendition_id"),
        "start_ms": start_ms,
        "end_ms": end_ms,
        "evidence_basis_kind": _enum(
            item["evidence_basis_kind"],
            f"{label}.evidence_basis_kind",
            EVIDENCE_BASIS_KINDS,
        ),
        "transcript_revision_id": _nullable_identifier(
            item["transcript_revision_id"], f"{label}.transcript_revision_id"
        ),
        "local_evidence": _parse_local_evidence(
            item["local_evidence"], f"{label}.local_evidence"
        ),
        "basis": _text(item["basis"], f"{label}.basis", maximum=8_192),
    }
    if collection == "appearances":
        result["appearance_role"] = _text(
            item["appearance_role"], f"{label}.appearance_role", maximum=500
        )
    else:
        result["support_kind"] = _enum(
            item["support_kind"], f"{label}.support_kind", SUPPORT_KINDS
        )
    return result


def _parse_event(raw: object, index: int) -> dict[str, Any]:
    label = f"events[{index}]"
    item = _object(raw, label)
    _exact_keys(
        item,
        label,
        {"event_id", "canonical_label", "slug", "event_kind", "description", "metadata"},
    )
    return {
        "event_id": _identifier(item["event_id"], f"{label}.event_id"),
        "canonical_label": _text(
            item["canonical_label"], f"{label}.canonical_label", maximum=1_000
        ),
        "slug": _slug(item["slug"], f"{label}.slug"),
        "event_kind": _text(item["event_kind"], f"{label}.event_kind", maximum=500),
        "description": _nullable_text(
            item["description"], f"{label}.description", maximum=16_384
        ),
        "metadata": _metadata(item["metadata"], f"{label}.metadata"),
    }


def _parse_event_date(raw: object, index: int) -> dict[str, Any]:
    label = f"event_dates[{index}]"
    item = _object(raw, label)
    _exact_keys(
        item,
        label,
        {
            "event_date_id",
            "event_id",
            "date_kind",
            "value_start",
            "value_end",
            "precision",
            "basis",
            "certainty",
        },
    )
    precision = _enum(item["precision"], f"{label}.precision", DATE_PRECISIONS)
    start = _date_value(item["value_start"], f"{label}.value_start")
    end = _date_value(item["value_end"], f"{label}.value_end")
    expected_pattern = {"day": DATE_RE, "month": MONTH_RE, "year": YEAR_RE}
    if precision == "unknown" and (start is not None or end is not None):
        raise EntityEventMapManifestError(f"{label} unknown precision requires null values")
    if precision in expected_pattern:
        if start is None or not expected_pattern[precision].fullmatch(start) or end is not None:
            raise EntityEventMapManifestError(
                f"{label} {precision} precision requires one matching value_start and null value_end"
            )
    if precision == "range" and (start is None or end is None):
        raise EntityEventMapManifestError(f"{label} range precision requires both values")
    if precision == "circa" and start is None:
        raise EntityEventMapManifestError(f"{label} circa precision requires value_start")
    if start is not None and end is not None:
        if len(start) != len(end):
            raise EntityEventMapManifestError(f"{label} range endpoints need equal precision")
        if end < start:
            raise EntityEventMapManifestError(f"{label} value_end precedes value_start")
    return {
        "event_date_id": _identifier(item["event_date_id"], f"{label}.event_date_id"),
        "event_id": _identifier(item["event_id"], f"{label}.event_id"),
        "date_kind": _text(item["date_kind"], f"{label}.date_kind", maximum=500),
        "value_start": start,
        "value_end": end,
        "precision": precision,
        "basis": _text(item["basis"], f"{label}.basis", maximum=8_192),
        "certainty": _enum(item["certainty"], f"{label}.certainty", DATE_CERTAINTIES),
    }


def _parse_participant(raw: object, index: int) -> dict[str, Any]:
    label = f"event_participants[{index}]"
    item = _object(raw, label)
    _exact_keys(item, label, {"event_id", "entity_id", "participant_role"})
    return {
        "event_id": _identifier(item["event_id"], f"{label}.event_id"),
        "entity_id": _identifier(item["entity_id"], f"{label}.entity_id"),
        "participant_role": _text(
            item["participant_role"], f"{label}.participant_role", maximum=500
        ),
    }


def _parse_relation(raw: object, index: int) -> dict[str, Any]:
    label = f"event_relations[{index}]"
    item = _object(raw, label)
    _exact_keys(
        item,
        label,
        {"event_relation_id", "from_event_id", "relation_kind", "to_event_id", "basis"},
    )
    from_event = _identifier(item["from_event_id"], f"{label}.from_event_id")
    to_event = _identifier(item["to_event_id"], f"{label}.to_event_id")
    if from_event == to_event:
        raise EntityEventMapManifestError(f"{label} may not relate an event to itself")
    return {
        "event_relation_id": _identifier(
            item["event_relation_id"], f"{label}.event_relation_id"
        ),
        "from_event_id": from_event,
        "relation_kind": _text(
            item["relation_kind"], f"{label}.relation_kind", maximum=500
        ),
        "to_event_id": to_event,
        "basis": _text(item["basis"], f"{label}.basis", maximum=8_192),
    }


def load_entity_event_map_manifest(path_value: str | Path) -> EntityEventMapManifest:
    """Read and structurally validate an exact private manifest."""

    path = Path(path_value)
    if not path.is_absolute():
        path = path.resolve()
    try:
        body = _stable_read(
            path, "entity/event map manifest", maximum_bytes=MAX_MANIFEST_BYTES
        )
    except ResultImportError as error:
        raise EntityEventMapManifestError(str(error)) from error
    digest = hashlib.sha256(body).hexdigest()
    try:
        raw = json.loads(body.decode("utf-8"), object_pairs_hook=_reject_duplicate_json_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise EntityEventMapManifestError(
            f"entity/event map manifest is invalid JSON: {error}"
        ) from error
    value = _object(raw, "manifest")
    collections = (
        "entities",
        "aliases",
        "appearances",
        "events",
        "event_dates",
        "event_participants",
        "event_relations",
        "event_evidence",
    )
    _exact_keys(
        value,
        "manifest",
        {
            "schema_version",
            "manifest_id",
            "created_at",
            "created_by",
            "basis",
            *collections,
        },
    )
    if type(value["schema_version"]) is not int or value["schema_version"] != SCHEMA_VERSION:
        raise EntityEventMapManifestError("manifest.schema_version must equal 1")
    normalized: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "manifest_id": _identifier(value["manifest_id"], "manifest.manifest_id"),
        "created_at": _canonical_timestamp(value["created_at"], "manifest.created_at"),
        "created_by": _text(value["created_by"], "manifest.created_by", maximum=500),
        "basis": _text(value["basis"], "manifest.basis", maximum=8_192),
    }
    parsers = {
        "entities": _parse_entity,
        "aliases": _parse_alias,
        "appearances": lambda item, index: _parse_timed_edge(item, index, "appearances"),
        "events": _parse_event,
        "event_dates": _parse_event_date,
        "event_participants": _parse_participant,
        "event_relations": _parse_relation,
        "event_evidence": lambda item, index: _parse_timed_edge(
            item, index, "event_evidence"
        ),
    }
    total = 0
    for collection in collections:
        raw_items = _array(value[collection], f"manifest.{collection}")
        normalized[collection] = [
            parsers[collection](item, index) for index, item in enumerate(raw_items)
        ]
        total += len(raw_items)
    if total == 0:
        raise EntityEventMapManifestError("manifest must contain at least one map record")
    if total > MAX_TOTAL_ITEMS:
        raise EntityEventMapManifestError(
            f"manifest exceeds the {MAX_TOTAL_ITEMS}-record safety limit"
        )

    for collection, key in (
        ("entities", "entity_id"),
        ("aliases", "entity_alias_id"),
        ("appearances", "appearance_id"),
        ("events", "event_id"),
        ("event_dates", "event_date_id"),
        ("event_relations", "event_relation_id"),
        ("event_evidence", "event_evidence_id"),
    ):
        _unique_ids(normalized[collection], key, f"manifest.{collection}")
    _reject_duplicate_natural_keys(
        normalized["entities"], ("slug",), "manifest.entities"
    )
    _reject_duplicate_natural_keys(
        normalized["aliases"],
        ("entity_id", "alias", "alias_kind"),
        "manifest.aliases",
    )
    _reject_duplicate_natural_keys(
        normalized["event_participants"],
        ("event_id", "entity_id", "participant_role"),
        "manifest.event_participants",
    )
    _reject_duplicate_natural_keys(
        normalized["event_relations"],
        ("from_event_id", "relation_kind", "to_event_id"),
        "manifest.event_relations",
    )
    _reject_duplicate_natural_keys(
        normalized["event_dates"],
        ("event_id", "date_kind", "value_start", "value_end", "precision", "basis"),
        "manifest.event_dates",
    )

    local_items = [
        edge["local_evidence"]
        for collection in ("appearances", "event_evidence")
        for edge in normalized[collection]
        if edge["local_evidence"] is not None
    ]
    artifact_ids: dict[str, tuple[str, str, int]] = {}
    for local in local_items:
        assert local is not None
        snapshot = (str(local["path"]), local["sha256"], local["byte_count"])
        previous = artifact_ids.setdefault(local["artifact_id"], snapshot)
        if previous != snapshot:
            raise EntityEventMapManifestError(
                f"local evidence artifact {local['artifact_id']} has conflicting definitions"
            )
        _verify_local_hash(
            local["path"],
            local["sha256"],
            local["byte_count"],
            f"local evidence {local['artifact_id']}",
        )
    return EntityEventMapManifest(normalized, digest, path.resolve())


def _entity_metadata(entity: dict[str, Any]) -> str:
    value = dict(entity["metadata"])
    value["_entity_event_map"] = {
        "schema_version": SCHEMA_VERSION,
        "privacy_classification": entity["privacy_classification"],
    }
    return canonical_json(value)


def _event_metadata(event: dict[str, Any]) -> str:
    value = dict(event["metadata"])
    value["_entity_event_map"] = {"schema_version": SCHEMA_VERSION}
    return canonical_json(value)


def _database_privacy_classification(row: sqlite3.Row) -> str:
    try:
        metadata = json.loads(row["metadata_json"])
    except (TypeError, json.JSONDecodeError) as error:
        raise EntityEventMapManifestError(
            f"catalog entity {row['entity_id']} has invalid metadata_json"
        ) from error
    value = metadata.get("_entity_event_map", {}).get("privacy_classification")
    return value if value in PRIVACY_CLASSIFICATIONS else "unknown"


def _require_catalog_entity(
    connection: sqlite3.Connection,
    entity_id: str,
    manifest_entities: dict[str, dict[str, Any]],
) -> str:
    if entity_id in manifest_entities:
        return manifest_entities[entity_id]["privacy_classification"]
    row = connection.execute(
        "SELECT entity_id, visibility, review_state, metadata_json FROM entities WHERE entity_id = ?",
        (entity_id,),
    ).fetchone()
    if row is None:
        raise EntityEventMapManifestError(f"unknown entity_id {entity_id}")
    if row["visibility"] != "private" or row["review_state"] == "rejected":
        raise EntityEventMapManifestError(
            f"entity {entity_id} is not a current private catalog entity"
        )
    return _database_privacy_classification(row)


def _require_catalog_event(
    connection: sqlite3.Connection,
    event_id: str,
    manifest_events: dict[str, dict[str, Any]],
) -> None:
    if event_id in manifest_events:
        return
    row = connection.execute(
        "SELECT visibility, review_state FROM events WHERE event_id = ?", (event_id,)
    ).fetchone()
    if row is None:
        raise EntityEventMapManifestError(f"unknown event_id {event_id}")
    if row["visibility"] != "private" or row["review_state"] == "rejected":
        raise EntityEventMapManifestError(
            f"event {event_id} is not a current private catalog event"
        )


def _require_current_source(connection: sqlite3.Connection, source_id: str) -> None:
    row = connection.execute(
        "SELECT review_state FROM sources WHERE source_id = ?", (source_id,)
    ).fetchone()
    if row is None:
        raise EntityEventMapManifestError(f"unknown source_id {source_id}")
    if row["review_state"] == "rejected":
        raise EntityEventMapManifestError(f"source {source_id} is rejected")


def _catalog_anchor(
    connection: sqlite3.Connection,
    edge: dict[str, Any],
    label: str,
) -> int | None:
    row = connection.execute(
        """
        SELECT source.review_state AS source_review_state,
               recording.review_state AS recording_review_state,
               recording.merged_into_recording_id,
               rendition.review_state AS rendition_review_state,
               media.duration_ms AS rendition_duration_ms,
               media.integrity_state AS media_integrity_state
        FROM sources AS source
        JOIN recording_sources AS link
          ON link.source_id = source.source_id
         AND link.recording_id = ?
         AND link.confidence_state <> 'rejected'
        JOIN recordings AS recording
          ON recording.recording_id = link.recording_id
        JOIN renditions AS rendition
          ON rendition.recording_id = recording.recording_id
         AND rendition.rendition_id = ?
        JOIN media_objects AS media ON media.media_id = rendition.media_id
        WHERE source.source_id = ?
        """,
        (edge["recording_id"], edge["rendition_id"], edge["source_id"]),
    ).fetchone()
    if row is None:
        raise EntityEventMapManifestError(
            f"{label} does not resolve to one current source/recording/rendition tuple"
        )
    if (
        row["source_review_state"] == "rejected"
        or row["recording_review_state"] in {"rejected", "merged"}
        or row["merged_into_recording_id"] is not None
        or row["rendition_review_state"] == "rejected"
        or row["media_integrity_state"] != "verified"
    ):
        raise EntityEventMapManifestError(f"{label} anchor is rejected or superseded")
    duration_ms = row["rendition_duration_ms"]
    if edge["start_ms"] is not None:
        if duration_ms is None:
            raise EntityEventMapManifestError(
                f"{label} has an interval but rendition duration is unknown"
            )
        if edge["end_ms"] > duration_ms:
            raise EntityEventMapManifestError(
                f"{label} half-open interval exceeds rendition duration {duration_ms} ms"
            )
    return duration_ms


def _require_reviewed_transcript(
    connection: sqlite3.Connection, edge: dict[str, Any], label: str
) -> None:
    revision_id = edge["transcript_revision_id"]
    if revision_id is None:
        return
    row = connection.execute(
        """
        SELECT recording_id, rendition_id, review_state
        FROM transcript_revisions WHERE revision_id = ?
        """,
        (revision_id,),
    ).fetchone()
    if row is None:
        raise EntityEventMapManifestError(f"{label} uses unknown transcript revision {revision_id}")
    if row["review_state"] not in REVIEWED_TRANSCRIPT_STATES:
        raise EntityEventMapManifestError(
            f"{label} transcript-derived evidence requires a human-reviewed revision"
        )
    if row["recording_id"] != edge["recording_id"]:
        raise EntityEventMapManifestError(
            f"{label} transcript revision belongs to another recording"
        )
    if row["rendition_id"] is not None and row["rendition_id"] != edge["rendition_id"]:
        raise EntityEventMapManifestError(
            f"{label} transcript revision belongs to another rendition"
        )


def _path_exists(graph: dict[str, set[str]], start: str, target: str) -> bool:
    pending = [start]
    visited: set[str] = set()
    while pending:
        node = pending.pop()
        if node == target:
            return True
        if node in visited:
            continue
        visited.add(node)
        pending.extend(graph.get(node, ()))
    return False


def _reject_new_relation_cycles(
    connection: sqlite3.Connection, relations: list[dict[str, Any]]
) -> None:
    graph: dict[str, set[str]] = {}
    existing_keys: set[tuple[str, str, str]] = set()
    for row in connection.execute(
        "SELECT from_event_id, relation_kind, to_event_id FROM event_relations"
    ):
        graph.setdefault(row["from_event_id"], set()).add(row["to_event_id"])
        existing_keys.add(
            (row["from_event_id"], row["relation_kind"], row["to_event_id"])
        )
    for relation in relations:
        key = (
            relation["from_event_id"],
            relation["relation_kind"],
            relation["to_event_id"],
        )
        if key in existing_keys:
            continue
        if _path_exists(graph, relation["to_event_id"], relation["from_event_id"]):
            raise EntityEventMapManifestError(
                f"event relation {relation['event_relation_id']} would create a cycle"
            )
        graph.setdefault(relation["from_event_id"], set()).add(relation["to_event_id"])
        existing_keys.add(key)


def _validate_catalog_references(
    connection: sqlite3.Connection, manifest: EntityEventMapManifest
) -> None:
    value = manifest.value
    entities = {item["entity_id"]: item for item in value["entities"]}
    events = {item["event_id"]: item for item in value["events"]}

    for entity in value["entities"]:
        existing = connection.execute(
            "SELECT visibility, review_state FROM entities WHERE entity_id = ?",
            (entity["entity_id"],),
        ).fetchone()
        if existing is not None and (
            existing["visibility"] != "private" or existing["review_state"] == "rejected"
        ):
            raise EntityEventMapManifestError(
                f"entity {entity['entity_id']} is not a current private catalog entity"
            )
    for event in value["events"]:
        existing = connection.execute(
            "SELECT visibility, review_state FROM events WHERE event_id = ?",
            (event["event_id"],),
        ).fetchone()
        if existing is not None and (
            existing["visibility"] != "private" or existing["review_state"] == "rejected"
        ):
            raise EntityEventMapManifestError(
                f"event {event['event_id']} is not a current private catalog event"
            )

    for alias in value["aliases"]:
        privacy = _require_catalog_entity(connection, alias["entity_id"], entities)
        _require_current_source(connection, alias["source_id"])
        sensitive_handle = alias["sensitive"] or alias["alias_kind"] in HANDLE_ALIAS_KINDS
        if privacy in {"living_private_person", "unknown"} and sensitive_handle:
            if not alias["privacy_review_required"]:
                raise EntityEventMapManifestError(
                    f"alias {alias['entity_alias_id']} is a sensitive living/unknown-private "
                    "alias or handle and must be explicitly marked for privacy review"
                )

    for index, appearance in enumerate(value["appearances"]):
        _require_catalog_entity(connection, appearance["entity_id"], entities)
        _catalog_anchor(connection, appearance, f"appearances[{index}]")
        _require_reviewed_transcript(connection, appearance, f"appearances[{index}]")

    for item in value["event_dates"]:
        _require_catalog_event(connection, item["event_id"], events)
    for item in value["event_participants"]:
        _require_catalog_event(connection, item["event_id"], events)
        _require_catalog_entity(connection, item["entity_id"], entities)
    for item in value["event_relations"]:
        _require_catalog_event(connection, item["from_event_id"], events)
        _require_catalog_event(connection, item["to_event_id"], events)
    for index, evidence in enumerate(value["event_evidence"]):
        _require_catalog_event(connection, evidence["event_id"], events)
        _catalog_anchor(connection, evidence, f"event_evidence[{index}]")
        _require_reviewed_transcript(connection, evidence, f"event_evidence[{index}]")

    _reject_new_relation_cycles(connection, value["event_relations"])

    # An event must describe a source-anchored occurrence, not merely reserve a
    # label or serve as a relation endpoint.  Count existing and incoming edges.
    incoming_participants = Counter(
        item["event_id"] for item in value["event_participants"]
    )
    incoming_evidence = Counter(item["event_id"] for item in value["event_evidence"])
    for event_id in events:
        participant_count = connection.execute(
            "SELECT count(*) FROM event_participants WHERE event_id = ?",
            (event_id,),
        ).fetchone()[0]
        evidence_count = connection.execute(
            "SELECT count(*) FROM event_evidence WHERE event_id = ?", (event_id,)
        ).fetchone()[0]
        if participant_count + incoming_participants[event_id] == 0:
            raise EntityEventMapManifestError(
                f"orphan event {event_id} has no participant"
            )
        if evidence_count + incoming_evidence[event_id] == 0:
            raise EntityEventMapManifestError(
                f"orphan event {event_id} has no source-anchored evidence"
            )


def _row_matches(row: sqlite3.Row, expected: dict[str, object]) -> bool:
    return all(row[key] == value for key, value in expected.items())


def _insert_or_match(
    connection: sqlite3.Connection,
    *,
    table: str,
    key_column: str,
    key_value: str,
    values: dict[str, object],
    mutable_columns: set[str] | None = None,
) -> bool:
    mutable_columns = mutable_columns or set()
    existing = connection.execute(
        f"SELECT * FROM {table} WHERE {key_column} = ?", (key_value,)
    ).fetchone()
    expected = {key: value for key, value in values.items() if key not in mutable_columns}
    if existing is not None:
        if not _row_matches(existing, expected):
            raise EntityEventMapManifestError(
                f"{table} ID {key_value} already has different data"
            )
        return False
    columns = [key_column, *values]
    placeholders = ", ".join("?" for _ in columns)
    try:
        connection.execute(
            f"INSERT INTO {table}({', '.join(columns)}) VALUES({placeholders})",
            (key_value, *(values[column] for column in values)),
        )
    except sqlite3.IntegrityError as error:
        raise EntityEventMapManifestError(
            f"cannot insert {table} {key_value}: natural key or catalog constraint conflict"
        ) from error
    return True


def _review_reason(evidence_basis_kind: str | None, support_kind: str | None) -> str:
    if evidence_basis_kind == "subject_unverified_claim":
        reason = (
            "Review the cited media directly; this edge records a subject's unverified "
            "claim, not the claimed occurrence as fact."
        )
    elif evidence_basis_kind == "third_party_unverified_claim":
        reason = (
            "Review the cited media directly; this edge records a third party's unverified "
            "claim, not the claimed occurrence as fact."
        )
    elif evidence_basis_kind == "direct_media_observation":
        reason = "Review the cited media directly and confirm the described visible/audible occurrence."
    else:
        reason = "Review the cited source and its context before accepting this private map edge."
    if support_kind == "contradicting":
        reason += " The evidence is explicitly recorded as contradicting."
    return reason


def _ensure_review_task(
    connection: sqlite3.Connection,
    *,
    task_kind: str,
    target_type: str,
    target_id: str,
    reason: str,
    priority: int,
    created_at: str,
) -> bool:
    existing = connection.execute(
        """
        SELECT * FROM review_tasks
        WHERE task_kind = ? AND target_type = ? AND target_id = ?
        """,
        (task_kind, target_type, target_id),
    ).fetchone()
    if existing is not None:
        if existing["reason"] != reason or existing["priority"] != priority:
            raise EntityEventMapManifestError(
                f"review task {task_kind}/{target_type}/{target_id} has different data"
            )
        return False
    review_task_id = stable_id(
        "review", "entity_event_map_v1", task_kind, target_type, target_id
    )
    _insert_or_match(
        connection,
        table="review_tasks",
        key_column="review_task_id",
        key_value=review_task_id,
        values={
            "task_kind": task_kind,
            "target_type": target_type,
            "target_id": target_id,
            "reason": reason,
            "priority": priority,
            "status": "open",
            "created_at": created_at,
            "updated_at": created_at,
        },
        mutable_columns={"status", "updated_at"},
    )
    return True


def _ensure_artifact(
    connection: sqlite3.Connection, local: dict[str, Any] | None
) -> str | None:
    if local is None:
        return None
    storage_uri = local["path"].as_uri()
    _insert_or_match(
        connection,
        table="artifacts",
        key_column="artifact_id",
        key_value=local["artifact_id"],
        values={
            "processing_run_id": None,
            "artifact_kind": "entity_event_source_evidence",
            "storage_uri": storage_uri,
            "sha256": local["sha256"],
            "byte_count": local["byte_count"],
            "schema_version": SCHEMA_VERSION,
            "visibility": "private",
            "metadata_json": canonical_json(
                {"entity_event_map": {"schema_version": SCHEMA_VERSION}}
            ),
        },
    )
    return local["artifact_id"]


def _observation_metadata(edge_type: str, edge: dict[str, Any], artifact_id: str | None) -> str:
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "edge_type": edge_type,
        "source_id": edge["source_id"],
        "recording_id": edge["recording_id"],
        "rendition_id": edge["rendition_id"],
        "time_coordinate_system": "rendition_media_ms",
        "evidence_basis_kind": edge["evidence_basis_kind"],
        "basis": edge["basis"],
        "transcript_revision_id": edge["transcript_revision_id"],
        "local_evidence_artifact_id": artifact_id,
    }
    return canonical_json({"entity_event_map": value})


def _ensure_observation(
    connection: sqlite3.Connection,
    *,
    edge_type: str,
    edge_id: str,
    edge: dict[str, Any],
    artifact_id: str | None,
    created_at: str,
) -> tuple[str | None, bool]:
    if edge["start_ms"] is None:
        return None, False
    observation_id = stable_id("obs", "entity_event_map_v1", edge_type, edge_id)
    inserted = _insert_or_match(
        connection,
        table="observations",
        key_column="observation_id",
        key_value=observation_id,
        values={
            "observation_kind": f"entity_event_{edge_type}_evidence",
            "recording_id": edge["recording_id"],
            "rendition_id": edge["rendition_id"],
            "processing_run_id": None,
            "start_ms": edge["start_ms"],
            "end_ms": edge["end_ms"],
            "visibility": "private",
            "review_state": "machine",
            "payload_schema_version": SCHEMA_VERSION,
            "metadata_json": _observation_metadata(edge_type, edge, artifact_id),
            "created_at": created_at,
        },
        mutable_columns={"review_state"},
    )
    return observation_id, inserted


def _anchor_basis(edge: dict[str, Any], artifact_id: str | None) -> str:
    prefix = (
        f"entity-event-map-v1; evidence_basis_kind={edge['evidence_basis_kind']}; "
        f"support_kind={edge.get('support_kind', 'appearance')}; "
        "time_coordinate_system=rendition_media_ms; "
        f"artifact_id={artifact_id or 'none'}"
    )
    return f"{prefix}. {edge['basis']}"


def _ensure_catalog_anchor_link(
    connection: sqlite3.Connection,
    *,
    edge_type: str,
    edge_id: str,
    edge: dict[str, Any],
    observation_id: str | None,
    artifact_id: str | None,
) -> bool:
    link_id = stable_id("ccl", "entity_event_map_v1", edge_type, edge_id)
    return _insert_or_match(
        connection,
        table="claim_catalog_links",
        key_column="claim_catalog_link_id",
        key_value=link_id,
        values={
            "claim_id": f"entity_event_map:{edge_type}:{edge_id}",
            "evidence_index": 0,
            "source_id": edge["source_id"],
            "recording_id": edge["recording_id"],
            "rendition_id": edge["rendition_id"],
            "transcript_revision_id": edge["transcript_revision_id"],
            "observation_id": observation_id,
            "start_ms": edge["start_ms"],
            "end_ms": edge["end_ms"],
            "link_state": "candidate",
            "basis": _anchor_basis(edge, artifact_id),
        },
        mutable_columns={"link_state"},
    )


def _assert_forbidden_counts_unchanged(
    connection: sqlite3.Connection, before: dict[str, int]
) -> None:
    for table, expected in before.items():
        observed = connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        if observed != expected:
            raise EntityEventMapManifestError(
                f"entity/event map import attempted a forbidden write to {table}"
            )


def _reject_preexisting_rows_without_ledger(
    connection: sqlite3.Connection, manifest: EntityEventMapManifest
) -> None:
    checks = (
        ("entities", "entities", "entity_id"),
        ("aliases", "entity_aliases", "entity_alias_id"),
        ("appearances", "appearances", "appearance_id"),
        ("events", "events", "event_id"),
        ("event_dates", "event_dates", "event_date_id"),
        ("event_relations", "event_relations", "event_relation_id"),
        ("event_evidence", "event_evidence", "event_evidence_id"),
    )
    for collection, table, column in checks:
        for item in manifest.value[collection]:
            identifier = item[column]
            if connection.execute(
                f"SELECT 1 FROM {table} WHERE {column} = ?", (identifier,)
            ).fetchone():
                raise EntityEventMapManifestError(
                    f"manifest ledger is missing but {table} row {identifier} already exists"
                )
    for participant in manifest.value["event_participants"]:
        if connection.execute(
            """
            SELECT 1 FROM event_participants
            WHERE event_id = ? AND entity_id = ? AND participant_role = ?
            """,
            (
                participant["event_id"],
                participant["entity_id"],
                participant["participant_role"],
            ),
        ).fetchone():
            raise EntityEventMapManifestError(
                "manifest ledger is missing but an asserted event participant already exists"
            )


FORBIDDEN_TABLES = (
    "identity_assertions",
    "identity_assertion_subjects",
    "identity_assertion_decisions",
    "publication_decisions",
    "publication_gate_decisions",
)

REPLAY_AUDIT_TABLES = (
    "entities",
    "entity_aliases",
    "appearances",
    "events",
    "event_dates",
    "event_participants",
    "event_participant_publication_subjects",
    "event_relations",
    "event_evidence",
    "observations",
    "claim_catalog_links",
    "artifacts",
    "review_tasks",
)


def _apply_manifest_rows(
    connection: sqlite3.Connection, manifest: EntityEventMapManifest
) -> dict[str, int]:
    value = manifest.value
    created_at = value["created_at"]
    inserted = Counter()

    for entity in value["entities"]:
        if _insert_or_match(
            connection,
            table="entities",
            key_column="entity_id",
            key_value=entity["entity_id"],
            values={
                "entity_type": entity["entity_type"],
                "canonical_label": entity["canonical_label"],
                "slug": entity["slug"],
                "visibility": "private",
                "review_state": "unreviewed",
                "metadata_json": _entity_metadata(entity),
                "created_at": created_at,
            },
            mutable_columns={"review_state"},
        ):
            inserted["entities"] += 1
        _ensure_review_task(
            connection,
            task_kind="entity_map_review",
            target_type="entity",
            target_id=entity["entity_id"],
            reason="Review this private entity-map record before any editorial or publication action.",
            priority=100,
            created_at=created_at,
        )

    for alias in value["aliases"]:
        if _insert_or_match(
            connection,
            table="entity_aliases",
            key_column="entity_alias_id",
            key_value=alias["entity_alias_id"],
            values={
                "entity_id": alias["entity_id"],
                "alias": alias["alias"],
                "alias_kind": alias["alias_kind"],
                "visibility": "private",
                "source_id": alias["source_id"],
                "valid_from": alias["valid_from"],
                "valid_to": alias["valid_to"],
            },
        ):
            inserted["aliases"] += 1
        _ensure_review_task(
            connection,
            task_kind="entity_alias_review",
            target_type="entity_alias",
            target_id=alias["entity_alias_id"],
            reason="Review the cited source and alias scope; the alias remains private.",
            priority=70,
            created_at=created_at,
        )
        if alias["privacy_review_required"]:
            _ensure_review_task(
                connection,
                task_kind="entity_alias_privacy_review",
                target_type="entity_alias",
                target_id=alias["entity_alias_id"],
                reason="Perform privacy review of this private sensitive alias or account handle.",
                priority=20,
                created_at=created_at,
            )

    for appearance in value["appearances"]:
        artifact_id = _ensure_artifact(connection, appearance["local_evidence"])
        observation_id, observation_inserted = _ensure_observation(
            connection,
            edge_type="appearance",
            edge_id=appearance["appearance_id"],
            edge=appearance,
            artifact_id=artifact_id,
            created_at=created_at,
        )
        assert observation_id is not None
        if observation_inserted:
            inserted["observations"] += 1
        if _ensure_catalog_anchor_link(
            connection,
            edge_type="appearance",
            edge_id=appearance["appearance_id"],
            edge=appearance,
            observation_id=observation_id,
            artifact_id=artifact_id,
        ):
            inserted["catalog_anchor_links"] += 1
        if _insert_or_match(
            connection,
            table="appearances",
            key_column="appearance_id",
            key_value=appearance["appearance_id"],
            values={
                "entity_id": appearance["entity_id"],
                "recording_id": appearance["recording_id"],
                "start_ms": appearance["start_ms"],
                "end_ms": appearance["end_ms"],
                "appearance_role": appearance["appearance_role"],
                "observation_id": observation_id,
                "review_state": "unreviewed",
            },
            mutable_columns={"review_state"},
        ):
            inserted["appearances"] += 1
        _ensure_review_task(
            connection,
            task_kind="appearance_media_review",
            target_type="appearance",
            target_id=appearance["appearance_id"],
            reason=_review_reason(appearance["evidence_basis_kind"], None),
            priority=(40 if "unverified_claim" in appearance["evidence_basis_kind"] else 70),
            created_at=created_at,
        )

    for event in value["events"]:
        if _insert_or_match(
            connection,
            table="events",
            key_column="event_id",
            key_value=event["event_id"],
            values={
                "canonical_label": event["canonical_label"],
                "slug": event["slug"],
                "event_kind": event["event_kind"],
                "description": event["description"],
                "visibility": "private",
                "review_state": "unreviewed",
                "metadata_json": _event_metadata(event),
                "created_at": created_at,
            },
            mutable_columns={"review_state"},
        ):
            inserted["events"] += 1
        _ensure_review_task(
            connection,
            task_kind="event_map_review",
            target_type="event",
            target_id=event["event_id"],
            reason="Review this private event record together with all supporting and contradicting edges.",
            priority=80,
            created_at=created_at,
        )

    for event_date in value["event_dates"]:
        if _insert_or_match(
            connection,
            table="event_dates",
            key_column="event_date_id",
            key_value=event_date["event_date_id"],
            values={key: event_date[key] for key in (
                "event_id", "date_kind", "value_start", "value_end", "precision", "basis", "certainty"
            )},
        ):
            inserted["event_dates"] += 1
        _ensure_review_task(
            connection,
            task_kind="event_date_review",
            target_type="event_date",
            target_id=event_date["event_date_id"],
            reason="Review the date precision, certainty, and cited event evidence.",
            priority=90,
            created_at=created_at,
        )

    for participant in value["event_participants"]:
        participant_id = stable_id(
            "participant",
            participant["event_id"],
            participant["entity_id"],
            participant["participant_role"],
        )
        existing = connection.execute(
            """
            SELECT review_state FROM event_participants
            WHERE event_id = ? AND entity_id = ? AND participant_role = ?
            """,
            (
                participant["event_id"],
                participant["entity_id"],
                participant["participant_role"],
            ),
        ).fetchone()
        if existing is None:
            connection.execute(
                """
                INSERT INTO event_participants(
                    event_id, entity_id, participant_role, review_state
                ) VALUES(?, ?, ?, 'unreviewed')
                """,
                (
                    participant["event_id"],
                    participant["entity_id"],
                    participant["participant_role"],
                ),
            )
            inserted["event_participants"] += 1
        if _insert_or_match(
            connection,
            table="event_participant_publication_subjects",
            key_column="event_participant_id",
            key_value=participant_id,
            values={
                "event_id": participant["event_id"],
                "entity_id": participant["entity_id"],
                "participant_role": participant["participant_role"],
                "created_at": created_at,
            },
        ):
            inserted["event_participant_publication_subjects"] += 1
        _ensure_review_task(
            connection,
            task_kind="event_participant_review",
            target_type="event_participant",
            target_id=participant_id,
            reason="Review this participant role against the event's source-anchored evidence.",
            priority=80,
            created_at=created_at,
        )

    for relation in value["event_relations"]:
        if _insert_or_match(
            connection,
            table="event_relations",
            key_column="event_relation_id",
            key_value=relation["event_relation_id"],
            values={key: relation[key] for key in (
                "from_event_id", "relation_kind", "to_event_id", "basis"
            )},
        ):
            inserted["event_relations"] += 1
        _ensure_review_task(
            connection,
            task_kind="event_relation_review",
            target_type="event_relation",
            target_id=relation["event_relation_id"],
            reason="Review the directed relation and confirm that it does not imply unsupported causality.",
            priority=90,
            created_at=created_at,
        )

    for evidence in value["event_evidence"]:
        artifact_id = _ensure_artifact(connection, evidence["local_evidence"])
        observation_id, observation_inserted = _ensure_observation(
            connection,
            edge_type="event_evidence",
            edge_id=evidence["event_evidence_id"],
            edge=evidence,
            artifact_id=artifact_id,
            created_at=created_at,
        )
        if observation_inserted:
            inserted["observations"] += 1
        if _ensure_catalog_anchor_link(
            connection,
            edge_type="event_evidence",
            edge_id=evidence["event_evidence_id"],
            edge=evidence,
            observation_id=observation_id,
            artifact_id=artifact_id,
        ):
            inserted["catalog_anchor_links"] += 1
        if _insert_or_match(
            connection,
            table="event_evidence",
            key_column="event_evidence_id",
            key_value=evidence["event_evidence_id"],
            values={
                "event_id": evidence["event_id"],
                "recording_id": evidence["recording_id"],
                "source_id": evidence["source_id"],
                "transcript_revision_id": evidence["transcript_revision_id"],
                "observation_id": observation_id,
                "start_ms": evidence["start_ms"],
                "end_ms": evidence["end_ms"],
                "support_kind": evidence["support_kind"],
            },
        ):
            inserted["event_evidence"] += 1
        _ensure_review_task(
            connection,
            task_kind="event_evidence_review",
            target_type="event_evidence",
            target_id=evidence["event_evidence_id"],
            reason=_review_reason(
                evidence["evidence_basis_kind"], evidence["support_kind"]
            ),
            priority=(
                30
                if "unverified_claim" in evidence["evidence_basis_kind"]
                else 50 if evidence["support_kind"] == "contradicting" else 70
            ),
            created_at=created_at,
        )

    return dict(inserted)


def _rehash_inputs(manifest: EntityEventMapManifest) -> None:
    try:
        body = _stable_read(
            manifest.path, "entity/event map manifest", maximum_bytes=MAX_MANIFEST_BYTES
        )
    except ResultImportError as error:
        raise EntityEventMapManifestError(str(error)) from error
    if hashlib.sha256(body).hexdigest() != manifest.input_sha256:
        raise EntityEventMapManifestError(
            "entity/event map manifest changed after validation"
        )
    verified: set[tuple[str, str, int]] = set()
    for collection in ("appearances", "event_evidence"):
        for edge in manifest.value[collection]:
            local = edge["local_evidence"]
            if local is None:
                continue
            key = (str(local["path"]), local["sha256"], local["byte_count"])
            if key in verified:
                continue
            _verify_local_hash(
                local["path"],
                local["sha256"],
                local["byte_count"],
                f"local evidence {local['artifact_id']}",
            )
            verified.add(key)


def validate_entity_event_map_manifest(
    connection: sqlite3.Connection, manifest_path: str | Path
) -> dict[str, Any]:
    """Validate exact bytes, local evidence, catalog anchors, privacy, and graph shape."""

    manifest = load_entity_event_map_manifest(manifest_path)
    _validate_catalog_references(connection, manifest)
    return {
        "validated": True,
        "dry_run": True,
        "schema_version": SCHEMA_VERSION,
        "manifest_id": manifest.manifest_id,
        "input_sha256": manifest.input_sha256,
        "counts": {
            collection: len(manifest.value[collection])
            for collection in (
                "entities",
                "aliases",
                "appearances",
                "events",
                "event_dates",
                "event_participants",
                "event_relations",
                "event_evidence",
            )
        },
    }


def import_entity_event_map_manifest(
    connection: sqlite3.Connection, manifest_path: str | Path
) -> dict[str, Any]:
    """Atomically append or exactly replay one private entity/event map manifest."""

    manifest = load_entity_event_map_manifest(manifest_path)
    batch_id = stable_id("imp", IMPORTER_NAME, manifest.manifest_id)
    forbidden_before = {
        table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        for table in FORBIDDEN_TABLES
    }
    with transaction(connection):
        _rehash_inputs(manifest)
        _validate_catalog_references(connection, manifest)
        existing_batch = connection.execute(
            "SELECT * FROM import_batches WHERE import_batch_id = ?", (batch_id,)
        ).fetchone()
        if existing_batch is not None:
            if (
                existing_batch["importer_name"] != IMPORTER_NAME
                or existing_batch["input_sha256"] != manifest.input_sha256
            ):
                raise EntityEventMapManifestError(
                    "manifest_id was already imported with different exact bytes"
                )
            if existing_batch["status"] != "completed":
                raise EntityEventMapManifestError(
                    "manifest import ledger is not in the completed state"
                )
        else:
            _reject_preexisting_rows_without_ledger(connection, manifest)
            try:
                connection.execute(
                    """
                    INSERT INTO import_batches(
                        import_batch_id, importer_name, importer_version, input_sha256,
                        source_snapshot_date, started_at, completed_at, status,
                        statistics_json
                    ) VALUES(?, ?, ?, ?, NULL, ?, NULL, 'running', '{}')
                    """,
                    (
                        batch_id,
                        IMPORTER_NAME,
                        __version__,
                        manifest.input_sha256,
                        manifest.value["created_at"],
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise EntityEventMapManifestError(
                    "manifest bytes or deterministic batch identity conflict with an existing import"
                ) from error

        replay_counts_before = {
            table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in REPLAY_AUDIT_TABLES
        }
        inserted = _apply_manifest_rows(connection, manifest)
        if existing_batch is not None:
            replay_counts_after = {
                table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in REPLAY_AUDIT_TABLES
            }
            changed_tables = sorted(
                table
                for table in REPLAY_AUDIT_TABLES
                if replay_counts_after[table] != replay_counts_before[table]
            )
            if changed_tables:
                raise EntityEventMapManifestError(
                    "completed manifest replay found missing catalog rows; refusing to "
                    f"repair a tampered ledger implicitly ({', '.join(changed_tables)})"
                )
        counts = {
            collection: len(manifest.value[collection])
            for collection in (
                "entities",
                "aliases",
                "appearances",
                "events",
                "event_dates",
                "event_participants",
                "event_relations",
                "event_evidence",
            )
        }
        statistics = {
            "manifest_id": manifest.manifest_id,
            "schema_version": SCHEMA_VERSION,
            "created_by": manifest.value["created_by"],
            "counts": counts,
        }
        if existing_batch is None:
            completed_at = utc_now()
            connection.execute(
                """
                UPDATE import_batches
                SET completed_at = ?, status = 'completed', statistics_json = ?
                WHERE import_batch_id = ? AND status = 'running'
                """,
                (completed_at, canonical_json(statistics), batch_id),
            )
        else:
            completed_at = existing_batch["completed_at"]
            try:
                recorded_statistics = json.loads(existing_batch["statistics_json"])
            except json.JSONDecodeError as error:
                raise EntityEventMapManifestError(
                    "manifest import ledger has invalid statistics JSON"
                ) from error
            if recorded_statistics != statistics:
                raise EntityEventMapManifestError(
                    "manifest import ledger has different immutable statistics"
                )
        _assert_forbidden_counts_unchanged(connection, forbidden_before)
    return {
        "manifest_id": manifest.manifest_id,
        "import_batch_id": batch_id,
        "input_sha256": manifest.input_sha256,
        "completed_at": completed_at,
        "counts": counts,
        "inserted": inserted,
        "idempotent_replay": existing_batch is not None,
        "visibility": "private",
    }
