"""Strict, auditable administration of reviewer identities and active state.

Reviewer registration is deliberately separate from publication administration.
New identities are always inserted inactive.  A later, explicit ``set_active``
event is the only way this workflow grants review authority.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .db import transaction, utc_now


REVIEWER_ADMIN_SCHEMA_VERSION = 1
REVIEWER_KINDS = {"human", "automated_policy", "imported_legacy"}
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")
DISPLAY_LABEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 .,'()_-]{0,118}[A-Za-z0-9)]$")
UTC_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_OPERATIONS_PER_KIND = 1_000
MAX_OPERATIONS = 2_000
PUBLIC_METADATA_POLICY_MANIFEST_ID = "reviewer-admin-public-metadata-policy-v1"
PUBLIC_METADATA_POLICY_REVIEWER_ID = "reviewer_public_metadata_policy_v1"
PUBLIC_METADATA_POLICY_DISPLAY_LABEL = "Public source metadata policy v1"
MACHINE_TRANSCRIPT_POLICY_MANIFEST_ID = (
    "reviewer-admin-machine-transcript-default-policy-v1"
)
MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID = (
    "reviewer_machine_transcript_default_policy_v1"
)
MACHINE_TRANSCRIPT_POLICY_DISPLAY_LABEL = (
    "Machine transcript default publication policy v1"
)
MACHINE_TRANSCRIPT_POLICY_REGISTRATION_EVENT_ID = (
    "register_machine_transcript_default_policy_v1"
)
MACHINE_TRANSCRIPT_POLICY_ACTIVATION_EVENT_ID = (
    "activate_machine_transcript_default_policy_v1"
)
MACHINE_TRANSCRIPT_POLICY_AUTHORIZED_BY = (
    "builtin_machine_transcript_default_policy_v1"
)
MACHINE_TRANSCRIPT_POLICY_MANIFEST_BASIS = (
    "Constrained built-in publication policy for ordinary-coordinate machine "
    "transcripts after independent human gate clearances."
)
MACHINE_TRANSCRIPT_POLICY_REGISTRATION_BASIS = (
    "Register the exact built-in machine transcript publication policy inactive."
)
MACHINE_TRANSCRIPT_POLICY_ACTIVATION_BASIS = (
    "Activate only the gate-dependent machine transcript publication policy."
)


class ReviewerAdminManifestError(ValueError):
    """A reviewer-administration manifest is unsafe or inconsistent."""


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        stat.S_IFMT(value.st_mode),
        stat.S_IMODE(value.st_mode),
        value.st_nlink,
    )


def _stable_read_manifest(path: Path) -> bytes:
    """Read one bounded regular file and reject replacement or mutation."""

    try:
        path_before = path.stat()
        handle = path.open("rb")
    except (FileNotFoundError, OSError) as error:
        raise ReviewerAdminManifestError(
            f"reviewer admin manifest is not a readable current file: {error}"
        ) from error
    try:
        descriptor_before = os.fstat(handle.fileno())
        if not stat.S_ISREG(descriptor_before.st_mode):
            raise ReviewerAdminManifestError(
                "reviewer admin manifest must be a regular file"
            )
        before = _stat_identity(descriptor_before)
        if before != _stat_identity(path_before):
            raise ReviewerAdminManifestError(
                "reviewer admin manifest was replaced while being opened"
            )
        if descriptor_before.st_size > MAX_MANIFEST_BYTES:
            raise ReviewerAdminManifestError(
                f"reviewer admin manifest exceeds {MAX_MANIFEST_BYTES} bytes"
            )
        body = handle.read()
        descriptor_after = os.fstat(handle.fileno())
    finally:
        handle.close()
    try:
        path_after = path.stat()
    except (FileNotFoundError, OSError) as error:
        raise ReviewerAdminManifestError(
            f"reviewer admin manifest disappeared after verification: {error}"
        ) from error
    if (
        before != _stat_identity(descriptor_after)
        or before != _stat_identity(path_after)
        or len(body) != before[2]
    ):
        raise ReviewerAdminManifestError(
            "reviewer admin manifest changed while it was being verified"
        )
    return body


@dataclass(frozen=True)
class ReviewerRegistration:
    reviewer_registration_id: str
    reviewer_id: str
    display_label: str
    reviewer_kind: str
    registered_at: str
    registered_at_value: datetime
    basis: str


@dataclass(frozen=True)
class ReviewerStateChange:
    reviewer_state_change_id: str
    reviewer_id: str
    active: bool
    changed_at: str
    changed_at_value: datetime
    basis: str


@dataclass(frozen=True)
class ReviewerAdminManifest:
    manifest_id: str
    created_at: str
    created_at_value: datetime
    authorized_by: str
    basis: str
    registrations: tuple[ReviewerRegistration, ...]
    state_changes: tuple[ReviewerStateChange, ...]
    input_sha256: str


@dataclass
class _ProposedReviewer:
    display_label: str
    reviewer_kind: str
    display_label_is_safe: bool
    active: bool
    last_effective_at: datetime | None


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ReviewerAdminManifestError(
                f"JSON object contains duplicate key {key!r}"
            )
        value[key] = item
    return value


def _exact_keys(
    value: dict[str, Any],
    label: str,
    required: set[str],
) -> None:
    missing = required - set(value)
    unknown = set(value) - required
    if missing or unknown:
        details: list[str] = []
        if missing:
            details.append(f"missing {sorted(missing)}")
        if unknown:
            details.append(f"unknown {sorted(unknown)}")
        raise ReviewerAdminManifestError(f"{label} has " + "; ".join(details))


def _object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ReviewerAdminManifestError(f"{label} must be an object")
    return value


def _array(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ReviewerAdminManifestError(f"{label} must be an array")
    return value


def _text(value: object, label: str, *, maximum: int) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ReviewerAdminManifestError(
            f"{label} must be a non-empty string of at most {maximum} characters"
        )
    if value != value.strip():
        raise ReviewerAdminManifestError(f"{label} must not have surrounding whitespace")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ReviewerAdminManifestError(f"{label} contains control characters")
    return value


def _identifier(value: object, label: str) -> str:
    text = _text(value, label, maximum=200)
    if not IDENTIFIER_RE.fullmatch(text):
        raise ReviewerAdminManifestError(
            f"{label} must use letters, digits, dot, underscore, colon, or hyphen"
        )
    return text


def _display_label(value: object, label: str) -> str:
    text = _text(value, label, maximum=120)
    if "  " in text or not DISPLAY_LABEL_RE.fullmatch(text):
        raise ReviewerAdminManifestError(
            f"{label} must be a normalized ASCII display label without markup"
        )
    return text


def _enum(value: object, label: str, allowed: set[str]) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise ReviewerAdminManifestError(f"{label} must be one of {sorted(allowed)}")
    return value


def _boolean(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise ReviewerAdminManifestError(f"{label} must be a boolean")
    return value


def _utc_timestamp(value: object, label: str) -> tuple[str, datetime]:
    if not isinstance(value, str) or not UTC_TIMESTAMP_RE.fullmatch(value):
        raise ReviewerAdminManifestError(
            f"{label} must be a whole-second UTC timestamp ending in Z"
        )
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        raise ReviewerAdminManifestError(
            f"{label} is not a valid UTC timestamp"
        ) from error
    return value, parsed


def _parse_registration(value: object, index: int) -> ReviewerRegistration:
    label = f"registrations[{index}]"
    item = _object(value, label)
    _exact_keys(
        item,
        label,
        {
            "reviewer_registration_id",
            "reviewer_id",
            "display_label",
            "reviewer_kind",
            "registered_at",
            "basis",
        },
    )
    registered_at, registered_at_value = _utc_timestamp(
        item["registered_at"], f"{label}.registered_at"
    )
    return ReviewerRegistration(
        reviewer_registration_id=_identifier(
            item["reviewer_registration_id"],
            f"{label}.reviewer_registration_id",
        ),
        reviewer_id=_identifier(item["reviewer_id"], f"{label}.reviewer_id"),
        display_label=_display_label(
            item["display_label"], f"{label}.display_label"
        ),
        reviewer_kind=_enum(
            item["reviewer_kind"], f"{label}.reviewer_kind", REVIEWER_KINDS
        ),
        registered_at=registered_at,
        registered_at_value=registered_at_value,
        basis=_text(item["basis"], f"{label}.basis", maximum=4_096),
    )


def _parse_state_change(value: object, index: int) -> ReviewerStateChange:
    label = f"state_changes[{index}]"
    item = _object(value, label)
    _exact_keys(
        item,
        label,
        {
            "reviewer_state_change_id",
            "reviewer_id",
            "active",
            "changed_at",
            "basis",
        },
    )
    changed_at, changed_at_value = _utc_timestamp(
        item["changed_at"], f"{label}.changed_at"
    )
    return ReviewerStateChange(
        reviewer_state_change_id=_identifier(
            item["reviewer_state_change_id"],
            f"{label}.reviewer_state_change_id",
        ),
        reviewer_id=_identifier(item["reviewer_id"], f"{label}.reviewer_id"),
        active=_boolean(item["active"], f"{label}.active"),
        changed_at=changed_at,
        changed_at_value=changed_at_value,
        basis=_text(item["basis"], f"{label}.basis", maximum=4_096),
    )


def load_reviewer_admin_manifest(path: str | Path) -> ReviewerAdminManifest:
    """Read and structurally validate one stable, strict JSON manifest."""

    manifest_path = Path(path).resolve()
    try:
        body = _stable_read_manifest(manifest_path)
        raw = json.loads(
            body.decode("utf-8"), object_pairs_hook=_reject_duplicate_json_keys
        )
    except ReviewerAdminManifestError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReviewerAdminManifestError(
            f"cannot read reviewer admin manifest {manifest_path}: {error}"
        ) from error

    value = _object(raw, "manifest")
    _exact_keys(
        value,
        "manifest",
        {
            "schema_version",
            "manifest_id",
            "created_at",
            "authorized_by",
            "basis",
            "registrations",
            "state_changes",
        },
    )
    if (
        type(value["schema_version"]) is not int
        or value["schema_version"] != REVIEWER_ADMIN_SCHEMA_VERSION
    ):
        raise ReviewerAdminManifestError("manifest.schema_version must equal 1")
    created_at, created_at_value = _utc_timestamp(
        value["created_at"], "manifest.created_at"
    )
    now = datetime.now(timezone.utc)
    if created_at_value > now:
        raise ReviewerAdminManifestError(
            "manifest.created_at must not be later than the current UTC time"
        )
    registration_values = _array(value["registrations"], "manifest.registrations")
    state_values = _array(value["state_changes"], "manifest.state_changes")
    if len(registration_values) > MAX_OPERATIONS_PER_KIND:
        raise ReviewerAdminManifestError(
            "manifest.registrations exceeds the 1000-operation safety limit"
        )
    if len(state_values) > MAX_OPERATIONS_PER_KIND:
        raise ReviewerAdminManifestError(
            "manifest.state_changes exceeds the 1000-operation safety limit"
        )
    operation_count = len(registration_values) + len(state_values)
    if operation_count == 0:
        raise ReviewerAdminManifestError(
            "manifest must contain at least one registration or state change"
        )
    if operation_count > MAX_OPERATIONS:
        raise ReviewerAdminManifestError(
            f"manifest exceeds the {MAX_OPERATIONS}-operation safety limit"
        )

    registrations = tuple(
        _parse_registration(item, index)
        for index, item in enumerate(registration_values)
    )
    state_changes = tuple(
        _parse_state_change(item, index) for index, item in enumerate(state_values)
    )
    manifest_id = _identifier(value["manifest_id"], "manifest.manifest_id")
    if (
        manifest_id == MACHINE_TRANSCRIPT_POLICY_MANIFEST_ID
        or any(
            registration.reviewer_id == MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID
            or registration.reviewer_registration_id
            in {
                MACHINE_TRANSCRIPT_POLICY_REGISTRATION_EVENT_ID,
                MACHINE_TRANSCRIPT_POLICY_ACTIVATION_EVENT_ID,
            }
            for registration in registrations
        )
        or any(
            change.reviewer_id == MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID
            or change.reviewer_state_change_id
            in {
                MACHINE_TRANSCRIPT_POLICY_REGISTRATION_EVENT_ID,
                MACHINE_TRANSCRIPT_POLICY_ACTIVATION_EVENT_ID,
            }
            for change in state_changes
        )
    ):
        raise ReviewerAdminManifestError(
            "machine transcript policy reviewer, manifest, and event IDs are "
            "reserved for the dedicated closed policy"
        )
    operation_ids = [
        registration.reviewer_registration_id for registration in registrations
    ] + [change.reviewer_state_change_id for change in state_changes]
    duplicate_operations = sorted(
        operation_id
        for operation_id, count in Counter(operation_ids).items()
        if count > 1
    )
    if duplicate_operations:
        raise ReviewerAdminManifestError(
            f"manifest contains duplicate operation IDs: {duplicate_operations}"
        )
    duplicate_reviewers = sorted(
        reviewer_id
        for reviewer_id, count in Counter(
            registration.reviewer_id for registration in registrations
        ).items()
        if count > 1
    )
    if duplicate_reviewers:
        raise ReviewerAdminManifestError(
            f"manifest contains duplicate reviewer registrations: {duplicate_reviewers}"
        )
    for label, effective_at in [
        *[
            (f"registrations[{index}].registered_at", item.registered_at_value)
            for index, item in enumerate(registrations)
        ],
        *[
            (f"state_changes[{index}].changed_at", item.changed_at_value)
            for index, item in enumerate(state_changes)
        ],
    ]:
        if effective_at > created_at_value:
            raise ReviewerAdminManifestError(
                f"{label} must not be later than manifest.created_at"
            )

    canonical = json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return ReviewerAdminManifest(
        manifest_id=manifest_id,
        created_at=created_at,
        created_at_value=created_at_value,
        authorized_by=_identifier(value["authorized_by"], "manifest.authorized_by"),
        basis=_text(value["basis"], "manifest.basis", maximum=4_096),
        registrations=registrations,
        state_changes=state_changes,
        input_sha256=hashlib.sha256(canonical).hexdigest(),
    )


def _database_timestamp(value: str, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ReviewerAdminManifestError(
            f"catalog {label} is not a valid timestamp: {value!r}"
        ) from error
    if parsed.tzinfo is None:
        raise ReviewerAdminManifestError(
            f"catalog {label} is missing a UTC offset: {value!r}"
        )
    return parsed.astimezone(timezone.utc)


def _validate_database_references(
    connection: sqlite3.Connection, manifest: ReviewerAdminManifest
) -> None:
    duplicate_manifest = connection.execute(
        """
        SELECT manifest_id, input_sha256
        FROM reviewer_admin_manifest_imports
        WHERE manifest_id = ? OR input_sha256 = ?
        LIMIT 1
        """,
        (manifest.manifest_id, manifest.input_sha256),
    ).fetchone()
    if duplicate_manifest is not None:
        if duplicate_manifest["manifest_id"] == manifest.manifest_id:
            raise ReviewerAdminManifestError(
                f"manifest_id {manifest.manifest_id!r} was already imported"
            )
        raise ReviewerAdminManifestError(
            "the same canonical manifest content was already imported as "
            f"{duplicate_manifest['manifest_id']!r}"
        )

    proposed: dict[str, _ProposedReviewer] = {}
    for index, registration in enumerate(manifest.registrations):
        label = f"registrations[{index}]"
        existing = connection.execute(
            """
            SELECT display_label, reviewer_kind, active
            FROM reviewers WHERE reviewer_id = ?
            """,
            (registration.reviewer_id,),
        ).fetchone()
        if existing is not None:
            raise ReviewerAdminManifestError(
                f"{label} conflicts with existing reviewer {registration.reviewer_id!r}"
            )
        proposed[registration.reviewer_id] = _ProposedReviewer(
            display_label=registration.display_label,
            reviewer_kind=registration.reviewer_kind,
            display_label_is_safe=True,
            active=False,
            last_effective_at=registration.registered_at_value,
        )

    operation_ids = [
        registration.reviewer_registration_id
        for registration in manifest.registrations
    ] + [change.reviewer_state_change_id for change in manifest.state_changes]
    if operation_ids:
        placeholders = ",".join("?" for _ in operation_ids)
        existing_events = connection.execute(
            f"""
            SELECT reviewer_admin_event_id
            FROM reviewer_admin_events
            WHERE reviewer_admin_event_id IN ({placeholders})
            ORDER BY reviewer_admin_event_id
            """,
            operation_ids,
        ).fetchall()
        if existing_events:
            raise ReviewerAdminManifestError(
                "manifest operation IDs already exist: "
                f"{[row['reviewer_admin_event_id'] for row in existing_events]}"
            )

    for index, change in enumerate(manifest.state_changes):
        label = f"state_changes[{index}]"
        current = proposed.get(change.reviewer_id)
        if current is None:
            reviewer = connection.execute(
                """
                SELECT display_label, reviewer_kind, active
                FROM reviewers WHERE reviewer_id = ?
                """,
                (change.reviewer_id,),
            ).fetchone()
            if reviewer is None:
                raise ReviewerAdminManifestError(
                    f"{label} references unknown reviewer {change.reviewer_id!r}"
                )
            raw_display_label = reviewer["display_label"]
            display_label_is_safe = True
            try:
                display_label = _display_label(
                    raw_display_label,
                    f"catalog reviewer {change.reviewer_id}.display_label",
                )
            except ReviewerAdminManifestError:
                display_label_is_safe = False
                if change.active:
                    raise ReviewerAdminManifestError(
                        f"{label} cannot activate reviewer {change.reviewer_id!r} "
                        "with an unsafe legacy display label"
                    )
                if not isinstance(raw_display_label, str) or not raw_display_label:
                    raise ReviewerAdminManifestError(
                        f"catalog reviewer {change.reviewer_id!r} has an invalid display label"
                    )
                display_label = raw_display_label
            reviewer_kind = _enum(
                reviewer["reviewer_kind"],
                f"catalog reviewer {change.reviewer_id}.reviewer_kind",
                REVIEWER_KINDS,
            )
            if reviewer["active"] not in (0, 1):
                raise ReviewerAdminManifestError(
                    f"catalog reviewer {change.reviewer_id!r} has invalid active state"
                )
            latest_event = connection.execute(
                """
                SELECT new_active, effective_at
                FROM reviewer_admin_events
                WHERE reviewer_id = ?
                ORDER BY event_sequence DESC
                LIMIT 1
                """,
                (change.reviewer_id,),
            ).fetchone()
            last_effective_at = None
            if latest_event is not None:
                if latest_event["new_active"] != reviewer["active"]:
                    raise ReviewerAdminManifestError(
                        f"managed reviewer {change.reviewer_id!r} disagrees with its audit stream"
                    )
                last_effective_at = _database_timestamp(
                    latest_event["effective_at"],
                    f"reviewer event stream {change.reviewer_id}",
                )
            current = _ProposedReviewer(
                display_label=display_label,
                reviewer_kind=reviewer_kind,
                display_label_is_safe=display_label_is_safe,
                active=bool(reviewer["active"]),
                last_effective_at=last_effective_at,
            )
            proposed[change.reviewer_id] = current

        if current.active == change.active:
            raise ReviewerAdminManifestError(
                f"{label} is a no-op for reviewer {change.reviewer_id!r}"
            )
        if change.active and not current.display_label_is_safe:
            raise ReviewerAdminManifestError(
                f"{label} cannot activate reviewer {change.reviewer_id!r} "
                "with an unsafe legacy display label"
            )
        if (
            current.last_effective_at is not None
            and change.changed_at_value <= current.last_effective_at
        ):
            raise ReviewerAdminManifestError(
                f"{label}.changed_at must be later than the existing reviewer stream"
            )
        current.active = change.active
        current.last_effective_at = change.changed_at_value


def _result(
    manifest: ReviewerAdminManifest, *, dry_run: bool, imported_at: str | None
) -> dict[str, object]:
    return {
        "manifest_id": manifest.manifest_id,
        "schema_version": REVIEWER_ADMIN_SCHEMA_VERSION,
        "input_sha256": manifest.input_sha256,
        "validated": True,
        "dry_run": dry_run,
        "authorized_by": manifest.authorized_by,
        "registration_count": len(manifest.registrations),
        "state_change_count": len(manifest.state_changes),
        "reviewers_inserted": 0 if dry_run else len(manifest.registrations),
        "events_inserted": (
            0
            if dry_run
            else len(manifest.registrations) + len(manifest.state_changes)
        ),
        "active_state_updates": 0 if dry_run else len(manifest.state_changes),
        "imported_at": imported_at,
        "publication_decisions_created": 0,
        "publication_gate_decisions_created": 0,
    }


def apply_reviewer_admin_manifest(
    connection: sqlite3.Connection,
    path: str | Path,
    *,
    dry_run: bool = True,
) -> dict[str, object]:
    """Validate a manifest and, only when explicitly requested, apply it atomically."""

    manifest = load_reviewer_admin_manifest(path)
    if dry_run:
        try:
            _validate_database_references(connection, manifest)
        except sqlite3.DatabaseError as error:
            raise ReviewerAdminManifestError(
                f"catalog rejected reviewer admin dry-run: {error}"
            ) from error
        return _result(manifest, dry_run=True, imported_at=None)

    imported_at = utc_now()
    try:
        with transaction(connection):
            _validate_database_references(connection, manifest)
            connection.execute(
                """
                INSERT INTO reviewer_admin_manifest_imports(
                    manifest_id, input_sha256, schema_version,
                    manifest_created_at, imported_at, authorized_by, basis,
                    registration_count, state_change_count, adoption_count
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    manifest.manifest_id,
                    manifest.input_sha256,
                    REVIEWER_ADMIN_SCHEMA_VERSION,
                    manifest.created_at,
                    imported_at,
                    manifest.authorized_by,
                    manifest.basis,
                    len(manifest.registrations),
                    len(manifest.state_changes),
                ),
            )
            ordinal = 0
            for registration in manifest.registrations:
                connection.execute(
                    """
                    INSERT INTO reviewer_admin_events(
                        reviewer_admin_event_id, manifest_id, ordinal,
                        event_kind, reviewer_id, display_label, reviewer_kind,
                        previous_active, new_active, effective_at, basis
                    ) VALUES(?, ?, ?, 'register', ?, ?, ?, NULL, 0, ?, ?)
                    """,
                    (
                        registration.reviewer_registration_id,
                        manifest.manifest_id,
                        ordinal,
                        registration.reviewer_id,
                        registration.display_label,
                        registration.reviewer_kind,
                        registration.registered_at,
                        registration.basis,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO reviewers(
                        reviewer_id, display_label, reviewer_kind, active
                    ) VALUES(?, ?, ?, 0)
                    """,
                    (
                        registration.reviewer_id,
                        registration.display_label,
                        registration.reviewer_kind,
                    ),
                )
                ordinal += 1

            for change in manifest.state_changes:
                reviewer = connection.execute(
                    """
                    SELECT display_label, reviewer_kind, active
                    FROM reviewers WHERE reviewer_id = ?
                    """,
                    (change.reviewer_id,),
                ).fetchone()
                if reviewer is None:  # pragma: no cover - revalidation already proves this.
                    raise ReviewerAdminManifestError(
                        f"state change references missing reviewer {change.reviewer_id!r}"
                    )
                connection.execute(
                    """
                    INSERT INTO reviewer_admin_events(
                        reviewer_admin_event_id, manifest_id, ordinal,
                        event_kind, reviewer_id, display_label, reviewer_kind,
                        previous_active, new_active, effective_at, basis
                    ) VALUES(?, ?, ?, 'set_active', ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        change.reviewer_state_change_id,
                        manifest.manifest_id,
                        ordinal,
                        change.reviewer_id,
                        reviewer["display_label"],
                        reviewer["reviewer_kind"],
                        reviewer["active"],
                        int(change.active),
                        change.changed_at,
                        change.basis,
                    ),
                )
                ordinal += 1
    except sqlite3.DatabaseError as error:
        raise ReviewerAdminManifestError(
            f"database rejected reviewer admin manifest atomically: {error}"
        ) from error

    return _result(manifest, dry_run=False, imported_at=imported_at)


def validate_reviewer_admin_manifest(
    connection: sqlite3.Connection, path: str | Path
) -> dict[str, object]:
    """Run the exact admission checks without starting a write transaction."""

    return apply_reviewer_admin_manifest(connection, path, dry_run=True)


def ensure_public_metadata_policy_reviewer(connection: sqlite3.Connection) -> bool:
    """Create the exact built-in metadata-policy identity inside a caller transaction.

    The path is deliberately closed over one ID, label, kind, and event sequence. It
    is not a general reviewer-registration bypass. Existing migration-adopted policy
    rows are accepted only when their current identity and active state match exactly.
    """

    if not connection.in_transaction:
        raise ReviewerAdminManifestError(
            "public metadata policy reviewer must be ensured inside a transaction"
        )
    reviewer = connection.execute(
        """
        SELECT display_label, reviewer_kind, active
        FROM reviewers WHERE reviewer_id = ?
        """,
        (PUBLIC_METADATA_POLICY_REVIEWER_ID,),
    ).fetchone()
    if reviewer is not None:
        expected = (
            PUBLIC_METADATA_POLICY_DISPLAY_LABEL,
            "automated_policy",
            1,
        )
        if tuple(reviewer) != expected:
            raise ReviewerAdminManifestError(
                "existing public metadata policy reviewer conflicts with the built-in identity"
            )
        current = connection.execute(
            """
            SELECT display_label, reviewer_kind, new_active
            FROM current_reviewer_admin_events
            WHERE reviewer_id = ?
            """,
            (PUBLIC_METADATA_POLICY_REVIEWER_ID,),
        ).fetchone()
        if current is None or tuple(current) != expected:
            raise ReviewerAdminManifestError(
                "public metadata policy reviewer lacks a matching current audit event"
            )
        return False

    conflicts = connection.execute(
        """
        SELECT 1 FROM reviewer_admin_manifest_imports WHERE manifest_id = ?
        UNION ALL
        SELECT 1 FROM reviewer_admin_events
        WHERE reviewer_admin_event_id IN (?, ?)
        LIMIT 1
        """,
        (
            PUBLIC_METADATA_POLICY_MANIFEST_ID,
            "register_public_metadata_policy_v1",
            "activate_public_metadata_policy_v1",
        ),
    ).fetchone()
    if conflicts is not None:
        raise ReviewerAdminManifestError(
            "built-in public metadata policy audit IDs conflict with existing rows"
        )

    created_at_value = datetime.now(timezone.utc).replace(microsecond=0)
    activated_at_value = created_at_value - timedelta(seconds=1)
    registered_at_value = created_at_value - timedelta(seconds=2)
    created_at = created_at_value.isoformat().replace("+00:00", "Z")
    activated_at = activated_at_value.isoformat().replace("+00:00", "Z")
    registered_at = registered_at_value.isoformat().replace("+00:00", "Z")

    canonical_manifest = {
        "schema_version": REVIEWER_ADMIN_SCHEMA_VERSION,
        "manifest_id": PUBLIC_METADATA_POLICY_MANIFEST_ID,
        "created_at": created_at,
        "authorized_by": "builtin_public_metadata_policy_v1",
        "basis": "Constrained built-in allowlist for reviewed public source metadata.",
        "registrations": [
            {
                "reviewer_registration_id": "register_public_metadata_policy_v1",
                "reviewer_id": PUBLIC_METADATA_POLICY_REVIEWER_ID,
                "display_label": PUBLIC_METADATA_POLICY_DISPLAY_LABEL,
                "reviewer_kind": "automated_policy",
                "registered_at": registered_at,
                "basis": "Register the exact built-in metadata policy inactive.",
            }
        ],
        "state_changes": [
            {
                "reviewer_state_change_id": "activate_public_metadata_policy_v1",
                "reviewer_id": PUBLIC_METADATA_POLICY_REVIEWER_ID,
                "active": True,
                "changed_at": activated_at,
                "basis": "Activate only the constrained built-in metadata policy.",
            }
        ],
    }
    digest = hashlib.sha256(
        json.dumps(
            canonical_manifest,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    connection.execute(
        """
        INSERT INTO reviewer_admin_manifest_imports(
            manifest_id, input_sha256, schema_version, manifest_created_at,
            imported_at, authorized_by, basis, registration_count,
            state_change_count, adoption_count
        ) VALUES(?, ?, 1, ?, ?, ?, ?, 1, 1, 0)
        """,
        (
            PUBLIC_METADATA_POLICY_MANIFEST_ID,
            digest,
            created_at,
            utc_now(),
            canonical_manifest["authorized_by"],
            canonical_manifest["basis"],
        ),
    )
    connection.execute(
        """
        INSERT INTO reviewer_admin_events(
            reviewer_admin_event_id, manifest_id, ordinal, event_kind,
            reviewer_id, display_label, reviewer_kind, previous_active,
            new_active, effective_at, basis
        ) VALUES('register_public_metadata_policy_v1', ?, 0, 'register', ?, ?,
                 'automated_policy', NULL, 0, ?, ?)
        """,
        (
            PUBLIC_METADATA_POLICY_MANIFEST_ID,
            PUBLIC_METADATA_POLICY_REVIEWER_ID,
            PUBLIC_METADATA_POLICY_DISPLAY_LABEL,
            registered_at,
            canonical_manifest["registrations"][0]["basis"],
        ),
    )
    connection.execute(
        """
        INSERT INTO reviewers(reviewer_id, display_label, reviewer_kind, active)
        VALUES(?, ?, 'automated_policy', 0)
        """,
        (PUBLIC_METADATA_POLICY_REVIEWER_ID, PUBLIC_METADATA_POLICY_DISPLAY_LABEL),
    )
    connection.execute(
        """
        INSERT INTO reviewer_admin_events(
            reviewer_admin_event_id, manifest_id, ordinal, event_kind,
            reviewer_id, display_label, reviewer_kind, previous_active,
            new_active, effective_at, basis
        ) VALUES('activate_public_metadata_policy_v1', ?, 1, 'set_active', ?, ?,
                 'automated_policy', 0, 1, ?, ?)
        """,
        (
            PUBLIC_METADATA_POLICY_MANIFEST_ID,
            PUBLIC_METADATA_POLICY_REVIEWER_ID,
            PUBLIC_METADATA_POLICY_DISPLAY_LABEL,
            activated_at,
            canonical_manifest["state_changes"][0]["basis"],
        ),
    )
    return True


def _machine_transcript_policy_provenance_is_exact(
    connection: sqlite3.Connection,
) -> bool:
    manifest = connection.execute(
        """
        SELECT input_sha256, schema_version, manifest_created_at, imported_at,
               authorized_by, basis, registration_count, state_change_count,
               adoption_count
        FROM reviewer_admin_manifest_imports
        WHERE manifest_id = ?
        """,
        (MACHINE_TRANSCRIPT_POLICY_MANIFEST_ID,),
    ).fetchone()
    events = connection.execute(
        """
        SELECT reviewer_admin_event_id, manifest_id, ordinal, event_kind,
               reviewer_id, display_label, reviewer_kind, previous_active,
               new_active, effective_at, basis
        FROM reviewer_admin_events
        WHERE manifest_id = ?
        ORDER BY event_sequence
        """,
        (MACHINE_TRANSCRIPT_POLICY_MANIFEST_ID,),
    ).fetchall()
    if (
        manifest is None
        or tuple(
            manifest[key]
            for key in (
                "schema_version",
                "authorized_by",
                "basis",
                "registration_count",
                "state_change_count",
                "adoption_count",
            )
        )
        != (
            REVIEWER_ADMIN_SCHEMA_VERSION,
            MACHINE_TRANSCRIPT_POLICY_AUTHORIZED_BY,
            MACHINE_TRANSCRIPT_POLICY_MANIFEST_BASIS,
            1,
            1,
            0,
        )
        or len(events) != 2
    ):
        return False
    registration, activation = events
    expected_registration = (
        MACHINE_TRANSCRIPT_POLICY_REGISTRATION_EVENT_ID,
        MACHINE_TRANSCRIPT_POLICY_MANIFEST_ID,
        0,
        "register",
        MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
        MACHINE_TRANSCRIPT_POLICY_DISPLAY_LABEL,
        "automated_policy",
        None,
        0,
        MACHINE_TRANSCRIPT_POLICY_REGISTRATION_BASIS,
    )
    expected_activation = (
        MACHINE_TRANSCRIPT_POLICY_ACTIVATION_EVENT_ID,
        MACHINE_TRANSCRIPT_POLICY_MANIFEST_ID,
        1,
        "set_active",
        MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
        MACHINE_TRANSCRIPT_POLICY_DISPLAY_LABEL,
        "automated_policy",
        0,
        1,
        MACHINE_TRANSCRIPT_POLICY_ACTIVATION_BASIS,
    )
    if (
        tuple(
            registration[key]
            for key in (
                "reviewer_admin_event_id",
                "manifest_id",
                "ordinal",
                "event_kind",
                "reviewer_id",
                "display_label",
                "reviewer_kind",
                "previous_active",
                "new_active",
                "basis",
            )
        )
        != expected_registration
        or tuple(
            activation[key]
            for key in (
                "reviewer_admin_event_id",
                "manifest_id",
                "ordinal",
                "event_kind",
                "reviewer_id",
                "display_label",
                "reviewer_kind",
                "previous_active",
                "new_active",
                "basis",
            )
        )
        != expected_activation
    ):
        return False
    canonical_manifest = {
        "schema_version": REVIEWER_ADMIN_SCHEMA_VERSION,
        "manifest_id": MACHINE_TRANSCRIPT_POLICY_MANIFEST_ID,
        "created_at": manifest["manifest_created_at"],
        "authorized_by": MACHINE_TRANSCRIPT_POLICY_AUTHORIZED_BY,
        "basis": MACHINE_TRANSCRIPT_POLICY_MANIFEST_BASIS,
        "registrations": [
            {
                "reviewer_registration_id":
                    MACHINE_TRANSCRIPT_POLICY_REGISTRATION_EVENT_ID,
                "reviewer_id": MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
                "display_label": MACHINE_TRANSCRIPT_POLICY_DISPLAY_LABEL,
                "reviewer_kind": "automated_policy",
                "registered_at": registration["effective_at"],
                "basis": MACHINE_TRANSCRIPT_POLICY_REGISTRATION_BASIS,
            }
        ],
        "state_changes": [
            {
                "reviewer_state_change_id":
                    MACHINE_TRANSCRIPT_POLICY_ACTIVATION_EVENT_ID,
                "reviewer_id": MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
                "active": True,
                "changed_at": activation["effective_at"],
                "basis": MACHINE_TRANSCRIPT_POLICY_ACTIVATION_BASIS,
            }
        ],
    }
    digest = hashlib.sha256(
        json.dumps(
            canonical_manifest,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return (
        manifest["input_sha256"] == digest
        and _database_timestamp(
            registration["effective_at"], "machine policy registration"
        )
        < _database_timestamp(
            activation["effective_at"], "machine policy activation"
        )
        < _database_timestamp(
            manifest["manifest_created_at"], "machine policy manifest creation"
        )
        <= _database_timestamp(
            manifest["imported_at"], "machine policy manifest import"
        )
    )


def ensure_machine_transcript_policy_reviewer(
    connection: sqlite3.Connection,
) -> bool:
    """Create and activate only the closed machine-transcript publication policy.

    This is deliberately not a general reviewer-registration bypass. The reserved
    identity can publish only through migration 0028's digest-gated policy-run path;
    database triggers deny it gate, review, correction, and lifecycle authority.
    An existing inactive identity is not silently reactivated.
    """

    if not connection.in_transaction:
        raise ReviewerAdminManifestError(
            "machine transcript policy reviewer must be ensured inside a transaction"
        )
    reviewer = connection.execute(
        """
        SELECT display_label, reviewer_kind, active
        FROM reviewers WHERE reviewer_id = ?
        """,
        (MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,),
    ).fetchone()
    if reviewer is not None:
        expected = (
            MACHINE_TRANSCRIPT_POLICY_DISPLAY_LABEL,
            "automated_policy",
            1,
        )
        if tuple(reviewer) != expected:
            raise ReviewerAdminManifestError(
                "existing machine transcript policy reviewer conflicts with the "
                "built-in identity or is inactive"
            )
        current = connection.execute(
            """
            SELECT reviewer_admin_event_id, manifest_id, display_label,
                   reviewer_kind, new_active
            FROM current_reviewer_admin_events
            WHERE reviewer_id = ?
            """,
            (MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,),
        ).fetchone()
        expected_current = (
            MACHINE_TRANSCRIPT_POLICY_ACTIVATION_EVENT_ID,
            MACHINE_TRANSCRIPT_POLICY_MANIFEST_ID,
            *expected,
        )
        if (
            current is None
            or tuple(current) != expected_current
            or not _machine_transcript_policy_provenance_is_exact(connection)
        ):
            raise ReviewerAdminManifestError(
                "machine transcript policy reviewer lacks exact built-in provenance"
            )
        return False

    conflicts = connection.execute(
        """
        SELECT 1 FROM reviewer_admin_manifest_imports WHERE manifest_id = ?
        UNION ALL
        SELECT 1 FROM reviewer_admin_events
        WHERE reviewer_admin_event_id IN (?, ?)
        LIMIT 1
        """,
        (
            MACHINE_TRANSCRIPT_POLICY_MANIFEST_ID,
            MACHINE_TRANSCRIPT_POLICY_REGISTRATION_EVENT_ID,
            MACHINE_TRANSCRIPT_POLICY_ACTIVATION_EVENT_ID,
        ),
    ).fetchone()
    if conflicts is not None:
        raise ReviewerAdminManifestError(
            "built-in machine transcript policy audit IDs conflict with existing rows"
        )

    created_at_value = datetime.now(timezone.utc).replace(microsecond=0)
    activated_at_value = created_at_value - timedelta(seconds=1)
    registered_at_value = created_at_value - timedelta(seconds=2)
    created_at = created_at_value.isoformat().replace("+00:00", "Z")
    activated_at = activated_at_value.isoformat().replace("+00:00", "Z")
    registered_at = registered_at_value.isoformat().replace("+00:00", "Z")
    canonical_manifest = {
        "schema_version": REVIEWER_ADMIN_SCHEMA_VERSION,
        "manifest_id": MACHINE_TRANSCRIPT_POLICY_MANIFEST_ID,
        "created_at": created_at,
        "authorized_by": MACHINE_TRANSCRIPT_POLICY_AUTHORIZED_BY,
        "basis": MACHINE_TRANSCRIPT_POLICY_MANIFEST_BASIS,
        "registrations": [
            {
                "reviewer_registration_id":
                    MACHINE_TRANSCRIPT_POLICY_REGISTRATION_EVENT_ID,
                "reviewer_id": MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
                "display_label": MACHINE_TRANSCRIPT_POLICY_DISPLAY_LABEL,
                "reviewer_kind": "automated_policy",
                "registered_at": registered_at,
                "basis": MACHINE_TRANSCRIPT_POLICY_REGISTRATION_BASIS,
            }
        ],
        "state_changes": [
            {
                "reviewer_state_change_id":
                    MACHINE_TRANSCRIPT_POLICY_ACTIVATION_EVENT_ID,
                "reviewer_id": MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
                "active": True,
                "changed_at": activated_at,
                "basis": MACHINE_TRANSCRIPT_POLICY_ACTIVATION_BASIS,
            }
        ],
    }
    digest = hashlib.sha256(
        json.dumps(
            canonical_manifest,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    connection.execute(
        """
        INSERT INTO reviewer_admin_manifest_imports(
            manifest_id, input_sha256, schema_version, manifest_created_at,
            imported_at, authorized_by, basis, registration_count,
            state_change_count, adoption_count
        ) VALUES(?, ?, 1, ?, ?, ?, ?, 1, 1, 0)
        """,
        (
            MACHINE_TRANSCRIPT_POLICY_MANIFEST_ID,
            digest,
            created_at,
            utc_now(),
            canonical_manifest["authorized_by"],
            canonical_manifest["basis"],
        ),
    )
    connection.execute(
        """
        INSERT INTO reviewer_admin_events(
            reviewer_admin_event_id, manifest_id, ordinal, event_kind,
            reviewer_id, display_label, reviewer_kind, previous_active,
            new_active, effective_at, basis
        ) VALUES(?, ?, 0, 'register', ?, ?, 'automated_policy', NULL, 0, ?, ?)
        """,
        (
            MACHINE_TRANSCRIPT_POLICY_REGISTRATION_EVENT_ID,
            MACHINE_TRANSCRIPT_POLICY_MANIFEST_ID,
            MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
            MACHINE_TRANSCRIPT_POLICY_DISPLAY_LABEL,
            registered_at,
            MACHINE_TRANSCRIPT_POLICY_REGISTRATION_BASIS,
        ),
    )
    connection.execute(
        """
        INSERT INTO reviewers(reviewer_id, display_label, reviewer_kind, active)
        VALUES(?, ?, 'automated_policy', 0)
        """,
        (
            MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
            MACHINE_TRANSCRIPT_POLICY_DISPLAY_LABEL,
        ),
    )
    connection.execute(
        """
        INSERT INTO reviewer_admin_events(
            reviewer_admin_event_id, manifest_id, ordinal, event_kind,
            reviewer_id, display_label, reviewer_kind, previous_active,
            new_active, effective_at, basis
        ) VALUES(?, ?, 1, 'set_active', ?, ?, 'automated_policy', 0, 1, ?, ?)
        """,
        (
            MACHINE_TRANSCRIPT_POLICY_ACTIVATION_EVENT_ID,
            MACHINE_TRANSCRIPT_POLICY_MANIFEST_ID,
            MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
            MACHINE_TRANSCRIPT_POLICY_DISPLAY_LABEL,
            activated_at,
            MACHINE_TRANSCRIPT_POLICY_ACTIVATION_BASIS,
        ),
    )
    return True
