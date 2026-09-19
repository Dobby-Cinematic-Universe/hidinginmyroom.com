"""Strict, transactional administration of publication and clearance decisions.

Publication manifests are deliberately separate from source/result imports. Nothing
in this module infers or automatically clears rights, privacy, or sensitivity gates.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .db import transaction, utc_now
from .machine_transcript_publication import DECISION_ID_PREFIX, MANIFEST_ID_PREFIX
from .private_acquisition import PrivateAcquisitionError, publication_restriction
from .reviewer_admin import (
    MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
    PUBLIC_METADATA_POLICY_REVIEWER_ID,
)


PUBLICATION_MANIFEST_SCHEMA_VERSION = 1
OBJECT_TABLES = {
    "source": ("sources", "source_id"),
    "recording": ("recordings", "recording_id"),
    "transcript_revision": ("transcript_revisions", "revision_id"),
    "entity": ("entities", "entity_id"),
    "event": ("events", "event_id"),
    "appearance": ("appearances", "appearance_id"),
    "event_date": ("event_dates", "event_date_id"),
    "event_participant": (
        "event_participant_publication_subjects",
        "event_participant_id",
    ),
    "event_relation": ("event_relations", "event_relation_id"),
    "event_evidence": ("event_evidence", "event_evidence_id"),
}
PUBLICATION_DECISIONS = {"publish", "withhold", "remove"}
PUBLICATION_PRIORITY = {"publish": 0, "withhold": 1, "remove": 2}
GATE_KINDS = {"rights", "privacy", "sensitivity"}
GATE_DECISIONS = {"clear", "withhold"}
GATE_PRIORITY = {"clear": 0, "withhold": 1}
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")
UTC_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
MAX_MANIFEST_DECISIONS = 10_000


class PublicationManifestError(ValueError):
    """A publication manifest is unsafe, ambiguous, or inconsistent."""


@dataclass(frozen=True)
class PublicationDecision:
    publication_decision_id: str
    object_type: str
    object_id: str
    decision: str
    reviewer_id: str
    review_decision_id: str | None
    decided_at: str
    decided_at_value: datetime
    basis: str
    note: str
    public_label: str | None

    @property
    def stream(self) -> tuple[str, str]:
        return self.object_type, self.object_id


@dataclass(frozen=True)
class GateDecision:
    publication_gate_decision_id: str
    object_type: str
    object_id: str
    gate_kind: str
    decision: str
    reviewer_id: str
    review_decision_id: str | None
    decided_at: str
    decided_at_value: datetime
    basis: str
    note: str

    @property
    def stream(self) -> tuple[str, str, str]:
        return self.object_type, self.object_id, self.gate_kind


@dataclass(frozen=True)
class PublicationManifest:
    manifest_id: str
    publication_decisions: tuple[PublicationDecision, ...]
    gate_decisions: tuple[GateDecision, ...]
    input_sha256: str


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
        raise PublicationManifestError(f"{label} has " + "; ".join(details))


def _object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PublicationManifestError(f"{label} must be an object")
    return value


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise PublicationManifestError(f"JSON object contains duplicate key {key!r}")
        value[key] = item
    return value


def _array(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise PublicationManifestError(f"{label} must be an array")
    return value


def _text(value: object, label: str, *, maximum: int = 8_192) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PublicationManifestError(f"{label} must be a non-empty string")
    if len(value) > maximum:
        raise PublicationManifestError(f"{label} exceeds {maximum} characters")
    if any(ord(character) < 32 and character not in "\t\n\r" for character in value):
        raise PublicationManifestError(f"{label} contains control characters")
    return value


def _optional_text(value: object, label: str, *, maximum: int = 8_192) -> str | None:
    if value is None:
        return None
    return _text(value, label, maximum=maximum)


def _identifier(value: object, label: str) -> str:
    text = _text(value, label, maximum=200)
    if not IDENTIFIER_RE.fullmatch(text):
        raise PublicationManifestError(
            f"{label} must use letters, digits, dot, underscore, colon, or hyphen"
        )
    return text


def _object_id(value: object, label: str) -> str:
    return _text(value, label, maximum=512)


def _enum(value: object, label: str, allowed: set[str]) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise PublicationManifestError(f"{label} must be one of {sorted(allowed)}")
    return value


def _utc_timestamp(value: object, label: str) -> tuple[str, datetime]:
    if not isinstance(value, str) or not UTC_TIMESTAMP_RE.fullmatch(value):
        raise PublicationManifestError(
            f"{label} must be a whole-second UTC timestamp ending in Z"
        )
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        raise PublicationManifestError(f"{label} is not a valid UTC timestamp") from error
    if parsed > datetime.now(timezone.utc):
        raise PublicationManifestError(
            f"{label} must not be later than the current UTC time"
        )
    return value, parsed


def _database_timestamp(value: str, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise PublicationManifestError(
            f"catalog {label} is not a valid timestamp: {value!r}"
        ) from error
    if parsed.tzinfo is None:
        raise PublicationManifestError(
            f"catalog {label} is missing a UTC offset: {value!r}"
        )
    return parsed.astimezone(timezone.utc)


def _parse_publication_decision(
    raw_value: object, index: int
) -> PublicationDecision:
    label = f"publication_decisions[{index}]"
    value = _object(raw_value, label)
    _exact_keys(
        value,
        label,
        {
            "publication_decision_id",
            "object_type",
            "object_id",
            "decision",
            "reviewer_id",
            "decided_at",
            "basis",
            "note",
        },
        {"review_decision_id", "public_label"},
    )
    decided_at, decided_at_value = _utc_timestamp(
        value["decided_at"], f"{label}.decided_at"
    )
    return PublicationDecision(
        publication_decision_id=_identifier(
            value["publication_decision_id"], f"{label}.publication_decision_id"
        ),
        object_type=_enum(
            value["object_type"], f"{label}.object_type", set(OBJECT_TABLES)
        ),
        object_id=_object_id(value["object_id"], f"{label}.object_id"),
        decision=_enum(
            value["decision"], f"{label}.decision", PUBLICATION_DECISIONS
        ),
        reviewer_id=_identifier(value["reviewer_id"], f"{label}.reviewer_id"),
        review_decision_id=(
            _identifier(value["review_decision_id"], f"{label}.review_decision_id")
            if value.get("review_decision_id") is not None
            else None
        ),
        decided_at=decided_at,
        decided_at_value=decided_at_value,
        basis=_text(value["basis"], f"{label}.basis", maximum=4_096),
        note=_text(value["note"], f"{label}.note"),
        public_label=_optional_text(
            value.get("public_label"), f"{label}.public_label", maximum=512
        ),
    )


def _parse_gate_decision(raw_value: object, index: int) -> GateDecision:
    label = f"gate_decisions[{index}]"
    value = _object(raw_value, label)
    _exact_keys(
        value,
        label,
        {
            "publication_gate_decision_id",
            "object_type",
            "object_id",
            "gate_kind",
            "decision",
            "reviewer_id",
            "decided_at",
            "basis",
            "note",
        },
        {"review_decision_id"},
    )
    decided_at, decided_at_value = _utc_timestamp(
        value["decided_at"], f"{label}.decided_at"
    )
    return GateDecision(
        publication_gate_decision_id=_identifier(
            value["publication_gate_decision_id"],
            f"{label}.publication_gate_decision_id",
        ),
        object_type=_enum(
            value["object_type"], f"{label}.object_type", set(OBJECT_TABLES)
        ),
        object_id=_object_id(value["object_id"], f"{label}.object_id"),
        gate_kind=_enum(value["gate_kind"], f"{label}.gate_kind", GATE_KINDS),
        decision=_enum(value["decision"], f"{label}.decision", GATE_DECISIONS),
        reviewer_id=_identifier(value["reviewer_id"], f"{label}.reviewer_id"),
        review_decision_id=(
            _identifier(value["review_decision_id"], f"{label}.review_decision_id")
            if value.get("review_decision_id") is not None
            else None
        ),
        decided_at=decided_at,
        decided_at_value=decided_at_value,
        basis=_text(value["basis"], f"{label}.basis", maximum=4_096),
        note=_text(value["note"], f"{label}.note"),
    )


def _validate_manifest_order(
    decisions: Iterable[PublicationDecision | GateDecision],
    *,
    priority: dict[str, int],
    label: str,
) -> None:
    previous: dict[tuple[str, ...], PublicationDecision | GateDecision] = {}
    for decision in decisions:
        prior = previous.get(decision.stream)
        if prior is not None:
            if decision.decided_at_value == prior.decided_at_value:
                if priority[prior.decision] > priority[decision.decision]:
                    raise PublicationManifestError(
                        f"{label} contains an unsafe same-time weakening for "
                        f"{decision.stream}"
                    )
                raise PublicationManifestError(
                    f"{label} contains ambiguous same-time decisions for "
                    f"{decision.stream}"
                )
            if decision.decided_at_value < prior.decided_at_value:
                raise PublicationManifestError(
                    f"{label} is not chronological for {decision.stream}"
                )
        previous[decision.stream] = decision


def load_publication_manifest(path: str | Path) -> PublicationManifest:
    """Parse and structurally validate one strict publication manifest."""

    manifest_path = Path(path)
    try:
        raw = json.loads(
            manifest_path.read_text(encoding="utf-8"),
            object_pairs_hook=_reject_duplicate_json_keys,
        )
    except (OSError, json.JSONDecodeError) as error:
        raise PublicationManifestError(
            f"cannot read publication manifest {manifest_path}: {error}"
        ) from error
    value = _object(raw, "manifest")
    _exact_keys(
        value,
        "manifest",
        {"schema_version", "manifest_id", "publication_decisions", "gate_decisions"},
    )
    if (
        type(value["schema_version"]) is not int
        or value["schema_version"] != PUBLICATION_MANIFEST_SCHEMA_VERSION
    ):
        raise PublicationManifestError(
            "manifest.schema_version must equal "
            f"{PUBLICATION_MANIFEST_SCHEMA_VERSION}"
        )
    manifest_id = _identifier(value["manifest_id"], "manifest.manifest_id")
    publication_values = _array(
        value["publication_decisions"], "manifest.publication_decisions"
    )
    gate_values = _array(value["gate_decisions"], "manifest.gate_decisions")
    total = len(publication_values) + len(gate_values)
    if total == 0:
        raise PublicationManifestError("manifest must contain at least one decision")
    if total > MAX_MANIFEST_DECISIONS:
        raise PublicationManifestError(
            f"manifest exceeds the {MAX_MANIFEST_DECISIONS}-decision safety limit"
        )
    publication_decisions = tuple(
        _parse_publication_decision(item, index)
        for index, item in enumerate(publication_values)
    )
    gate_decisions = tuple(
        _parse_gate_decision(item, index) for index, item in enumerate(gate_values)
    )
    if (
        manifest_id.startswith(MANIFEST_ID_PREFIX)
        or any(
            decision.publication_decision_id.startswith(DECISION_ID_PREFIX)
            for decision in publication_decisions
        )
        or any(
            decision.publication_gate_decision_id.startswith(DECISION_ID_PREFIX)
            for decision in gate_decisions
        )
    ):
        raise PublicationManifestError(
            "machine transcript policy manifest and decision prefixes are reserved "
            "for the dedicated closed policy"
        )

    identifiers = [
        decision.publication_decision_id for decision in publication_decisions
    ] + [decision.publication_gate_decision_id for decision in gate_decisions]
    duplicates = sorted(
        identifier for identifier, count in Counter(identifiers).items() if count > 1
    )
    if duplicates:
        raise PublicationManifestError(
            f"manifest contains duplicate decision IDs: {duplicates}"
        )
    _validate_manifest_order(
        publication_decisions,
        priority=PUBLICATION_PRIORITY,
        label="manifest.publication_decisions",
    )
    _validate_manifest_order(
        gate_decisions,
        priority=GATE_PRIORITY,
        label="manifest.gate_decisions",
    )
    canonical = json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return PublicationManifest(
        manifest_id=manifest_id,
        publication_decisions=publication_decisions,
        gate_decisions=gate_decisions,
        input_sha256=hashlib.sha256(canonical).hexdigest(),
    )


def _validate_reviewer(
    connection: sqlite3.Connection,
    reviewer_id: str,
    decided_at: datetime,
    label: str,
) -> str:
    reviewer = connection.execute(
        "SELECT active, reviewer_kind FROM reviewers WHERE reviewer_id = ?",
        (reviewer_id,),
    ).fetchone()
    if reviewer is None:
        raise PublicationManifestError(f"{label} references unknown reviewer {reviewer_id!r}")
    if reviewer["active"] != 1:
        raise PublicationManifestError(f"{label} references inactive reviewer {reviewer_id!r}")
    effective = connection.execute(
        """
        SELECT new_active
        FROM reviewer_admin_events
        WHERE reviewer_id = ?
          AND julianday(effective_at) <= julianday(?)
        ORDER BY julianday(effective_at) DESC, event_sequence DESC
        LIMIT 1
        """,
        (reviewer_id, decided_at.isoformat()),
    ).fetchone()
    if effective is None or effective["new_active"] != 1:
        raise PublicationManifestError(
            f"{label} reviewer {reviewer_id!r} was not active at decided_at"
        )
    return reviewer["reviewer_kind"]


def _validate_object(
    connection: sqlite3.Connection, object_type: str, object_id: str, label: str
) -> None:
    table, primary_key = OBJECT_TABLES[object_type]
    row = connection.execute(
        f"SELECT 1 FROM {table} WHERE {primary_key} = ?", (object_id,)
    ).fetchone()
    if row is None:
        raise PublicationManifestError(
            f"{label} references unknown {object_type} {object_id!r}"
        )


def _automated_policy_publish_is_authorized(
    connection: sqlite3.Connection, decision: PublicationDecision
) -> bool:
    """Match the exact database capability granted to the built-in policy."""

    if decision.reviewer_id != PUBLIC_METADATA_POLICY_REVIEWER_ID:
        return False
    return connection.execute(
        """
        SELECT 1
        FROM public_metadata_policy_publish_scope
        WHERE object_type = ?
          AND object_id = ?
          AND public_label = ?
          AND basis = ?
        LIMIT 1
        """,
        (
            decision.object_type,
            decision.object_id,
            decision.public_label,
            decision.basis,
        ),
    ).fetchone() is not None


def _validate_review_decision(
    connection: sqlite3.Connection,
    *,
    review_decision_id: str | None,
    object_type: str,
    object_id: str,
    reviewer_id: str,
    decided_at: datetime,
    label: str,
) -> None:
    if review_decision_id is None:
        return
    row = connection.execute(
        """
        SELECT target_type, target_id, reviewer_id, decided_at
        FROM review_decisions
        WHERE review_decision_id = ?
        """,
        (review_decision_id,),
    ).fetchone()
    if row is None:
        raise PublicationManifestError(
            f"{label} references unknown review decision {review_decision_id!r}"
        )
    if (
        row["target_type"] != object_type
        or row["target_id"] != object_id
        or row["reviewer_id"] != reviewer_id
    ):
        raise PublicationManifestError(
            f"{label} review decision does not match its object and reviewer"
        )
    if _database_timestamp(row["decided_at"], f"review decision {review_decision_id}") > decided_at:
        raise PublicationManifestError(
            f"{label} precedes its referenced review decision"
        )


def _validate_publication_against_database(
    connection: sqlite3.Connection, decision: PublicationDecision, index: int
) -> None:
    label = f"publication_decisions[{index}]"
    reviewer_kind = _validate_reviewer(
        connection, decision.reviewer_id, decision.decided_at_value, label
    )
    built_in_policy = decision.reviewer_id in {
        PUBLIC_METADATA_POLICY_REVIEWER_ID,
        MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
    }
    if (
        reviewer_kind == "imported_legacy"
        or (
            built_in_policy
            and (
                decision.decision != "publish"
                or not _automated_policy_publish_is_authorized(connection, decision)
            )
        )
        or (
            reviewer_kind == "automated_policy"
            and decision.decision == "publish"
            and not _automated_policy_publish_is_authorized(connection, decision)
        )
    ):
        raise PublicationManifestError(
            f"{label} reviewer kind {reviewer_kind!r} is not authorized for "
            f"publication decision {decision.decision!r}"
        )
    _validate_object(connection, decision.object_type, decision.object_id, label)
    if decision.decision == "publish":
        try:
            restriction = publication_restriction(
                connection, decision.object_type, decision.object_id
            )
        except PrivateAcquisitionError as error:
            raise PublicationManifestError(str(error)) from error
        if restriction is not None:
            raise PublicationManifestError(
                f"{label} is blocked by effective private acquisition "
                f"{restriction['publication_disposition']} policy with no "
                "publication authority"
            )
    _validate_review_decision(
        connection,
        review_decision_id=decision.review_decision_id,
        object_type=decision.object_type,
        object_id=decision.object_id,
        reviewer_id=decision.reviewer_id,
        decided_at=decision.decided_at_value,
        label=label,
    )
    if connection.execute(
        """
        SELECT 1 FROM publication_decisions WHERE publication_decision_id = ?
        UNION ALL
        SELECT 1 FROM publication_gate_decisions
        WHERE publication_gate_decision_id = ?
        LIMIT 1
        """,
        (decision.publication_decision_id, decision.publication_decision_id),
    ).fetchone():
        raise PublicationManifestError(
            f"{label} duplicates publication decision ID "
            f"{decision.publication_decision_id!r}"
        )
    previous = connection.execute(
        """
        SELECT decision, decided_at
        FROM publication_decisions
        WHERE object_type = ? AND object_id = ?
        ORDER BY julianday(decided_at) DESC, decision_sequence DESC
        LIMIT 1
        """,
        (decision.object_type, decision.object_id),
    ).fetchone()
    if previous is None:
        return
    previous_time = _database_timestamp(
        previous["decided_at"], f"publication stream {decision.stream}"
    )
    if decision.decided_at_value == previous_time:
        if PUBLICATION_PRIORITY[previous["decision"]] > PUBLICATION_PRIORITY[decision.decision]:
            raise PublicationManifestError(
                f"{label} is an unsafe same-time weakening of "
                f"{previous['decision']!r}"
            )
        raise PublicationManifestError(
            f"{label} is ambiguous with an existing same-time decision"
        )
    if decision.decided_at_value < previous_time:
        raise PublicationManifestError(
            f"{label}.decided_at must be later than the existing decision stream"
        )


def _validate_gate_against_database(
    connection: sqlite3.Connection, decision: GateDecision, index: int
) -> None:
    label = f"gate_decisions[{index}]"
    reviewer_kind = _validate_reviewer(
        connection, decision.reviewer_id, decision.decided_at_value, label
    )
    if (
        reviewer_kind == "imported_legacy"
        or decision.reviewer_id
        in {
            PUBLIC_METADATA_POLICY_REVIEWER_ID,
            MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
        }
        or (reviewer_kind == "automated_policy" and decision.decision == "clear")
    ):
        raise PublicationManifestError(
            f"{label} reviewer kind {reviewer_kind!r} is not authorized for "
            f"gate decision {decision.decision!r}"
        )
    _validate_object(connection, decision.object_type, decision.object_id, label)
    if decision.decision == "clear":
        try:
            restriction = publication_restriction(
                connection, decision.object_type, decision.object_id
            )
        except PrivateAcquisitionError as error:
            raise PublicationManifestError(str(error)) from error
        if restriction is not None:
            raise PublicationManifestError(
                f"{label} is blocked by effective private acquisition "
                f"{restriction['publication_disposition']} policy with no "
                "publication authority"
            )
    _validate_review_decision(
        connection,
        review_decision_id=decision.review_decision_id,
        object_type=decision.object_type,
        object_id=decision.object_id,
        reviewer_id=decision.reviewer_id,
        decided_at=decision.decided_at_value,
        label=label,
    )
    if connection.execute(
        """
        SELECT 1 FROM publication_decisions WHERE publication_decision_id = ?
        UNION ALL
        SELECT 1 FROM publication_gate_decisions
        WHERE publication_gate_decision_id = ?
        LIMIT 1
        """,
        (
            decision.publication_gate_decision_id,
            decision.publication_gate_decision_id,
        ),
    ).fetchone():
        raise PublicationManifestError(
            f"{label} duplicates gate decision ID "
            f"{decision.publication_gate_decision_id!r}"
        )
    previous = connection.execute(
        """
        SELECT decision, decided_at
        FROM publication_gate_decisions
        WHERE object_type = ? AND object_id = ? AND gate_kind = ?
        ORDER BY julianday(decided_at) DESC, gate_decision_sequence DESC
        LIMIT 1
        """,
        (decision.object_type, decision.object_id, decision.gate_kind),
    ).fetchone()
    if previous is None:
        return
    previous_time = _database_timestamp(
        previous["decided_at"], f"publication gate stream {decision.stream}"
    )
    if decision.decided_at_value == previous_time:
        if GATE_PRIORITY[previous["decision"]] > GATE_PRIORITY[decision.decision]:
            raise PublicationManifestError(
                f"{label} is an unsafe same-time weakening of "
                f"{previous['decision']!r}"
            )
        raise PublicationManifestError(
            f"{label} is ambiguous with an existing same-time decision"
        )
    if decision.decided_at_value < previous_time:
        raise PublicationManifestError(
            f"{label}.decided_at must be later than the existing gate stream"
        )


def _validate_database_references(
    connection: sqlite3.Connection, manifest: PublicationManifest
) -> None:
    duplicate_manifest = connection.execute(
        """
        SELECT manifest_id, input_sha256
        FROM publication_manifest_imports
        WHERE manifest_id = ? OR input_sha256 = ?
        LIMIT 1
        """,
        (manifest.manifest_id, manifest.input_sha256),
    ).fetchone()
    if duplicate_manifest is not None:
        if duplicate_manifest["manifest_id"] == manifest.manifest_id:
            raise PublicationManifestError(
                f"manifest_id {manifest.manifest_id!r} was already imported"
            )
        raise PublicationManifestError(
            "the same canonical manifest content was already imported as "
            f"{duplicate_manifest['manifest_id']!r}"
        )
    for index, decision in enumerate(manifest.publication_decisions):
        _validate_publication_against_database(connection, decision, index)
    for index, decision in enumerate(manifest.gate_decisions):
        _validate_gate_against_database(connection, decision, index)


def _result(manifest: PublicationManifest, *, dry_run: bool) -> dict[str, object]:
    return {
        "manifest_id": manifest.manifest_id,
        "schema_version": PUBLICATION_MANIFEST_SCHEMA_VERSION,
        "input_sha256": manifest.input_sha256,
        "validated": True,
        "dry_run": dry_run,
        "publication_decisions": len(manifest.publication_decisions),
        "gate_decisions": len(manifest.gate_decisions),
        "decisions_inserted": (
            0
            if dry_run
            else len(manifest.publication_decisions) + len(manifest.gate_decisions)
        ),
    }


def apply_publication_manifest(
    connection: sqlite3.Connection,
    path: str | Path,
    *,
    dry_run: bool = False,
) -> dict[str, object]:
    """Validate and atomically import decisions, or perform the same checks dry."""

    manifest = load_publication_manifest(path)
    if dry_run:
        try:
            _validate_database_references(connection, manifest)
        except sqlite3.DatabaseError as error:
            raise PublicationManifestError(
                f"catalog rejected publication manifest dry-run: {error}"
            ) from error
        return _result(manifest, dry_run=True)

    try:
        with transaction(connection):
            _validate_database_references(connection, manifest)
            if not dry_run:
                connection.execute(
                    """
                    INSERT INTO publication_manifest_imports(
                        manifest_id, input_sha256, schema_version,
                        publication_decision_count, gate_decision_count, imported_at
                    ) VALUES(?, ?, ?, ?, ?, ?)
                    """,
                    (
                        manifest.manifest_id,
                        manifest.input_sha256,
                        PUBLICATION_MANIFEST_SCHEMA_VERSION,
                        len(manifest.publication_decisions),
                        len(manifest.gate_decisions),
                        utc_now(),
                    ),
                )
                connection.executemany(
                    """
                    INSERT INTO publication_decisions(
                        publication_decision_id, object_type, object_id, decision,
                        reviewer_id, review_decision_id, decided_at, basis, notes,
                        public_label, manifest_id
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            decision.publication_decision_id,
                            decision.object_type,
                            decision.object_id,
                            decision.decision,
                            decision.reviewer_id,
                            decision.review_decision_id,
                            decision.decided_at,
                            decision.basis,
                            decision.note,
                            decision.public_label,
                            manifest.manifest_id,
                        )
                        for decision in manifest.publication_decisions
                    ],
                )
                connection.executemany(
                    """
                    INSERT INTO publication_gate_decisions(
                        publication_gate_decision_id, object_type, object_id,
                        gate_kind, decision, reviewer_id, review_decision_id,
                        decided_at, basis, notes, manifest_id
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            decision.publication_gate_decision_id,
                            decision.object_type,
                            decision.object_id,
                            decision.gate_kind,
                            decision.decision,
                            decision.reviewer_id,
                            decision.review_decision_id,
                            decision.decided_at,
                            decision.basis,
                            decision.note,
                            manifest.manifest_id,
                        )
                        for decision in manifest.gate_decisions
                    ],
                )
    except sqlite3.IntegrityError as error:
        raise PublicationManifestError(
            f"database rejected publication manifest atomically: {error}"
        ) from error

    return _result(manifest, dry_run=False)


def validate_publication_manifest(
    connection: sqlite3.Connection, path: str | Path
) -> dict[str, object]:
    """Run the exact import admission checks without inserting decisions."""

    return apply_publication_manifest(connection, path, dry_run=True)
