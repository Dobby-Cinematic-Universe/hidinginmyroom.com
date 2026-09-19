"""Private, human-governed named solo-voice attestations.

This administrative lane binds a named entity to an exact rendition-media interval
only after direct listening and an independent privacy/biometric review.  It never
accepts a model score, transcript wording, source ownership, channel context, face
track, or biometric artifact as identity evidence, and it creates no public object.
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

from .asr_result_importer import _stable_read
from .db import transaction, utc_now
from .result_importers import ResultImportError


SCHEMA_VERSION = 1
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_ITEMS_PER_COLLECTION = 1_000
MAX_TOTAL_ITEMS = 3_000
SQLITE_INTEGER_MAX = 9_223_372_036_854_775_807
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
DIRECT_AUDIO_ATTESTATION = (
    "I directly listened to the complete anchored interval and identify exactly "
    "one live human voice as the named entity without relying on source or channel "
    "context, transcript text, machine identity output, or machine confidence."
)
PRIVACY_ATTESTATION = {
    "named_voice_personal_data_reviewed": True,
    "private_storage_only": True,
    "public_export_approved": False,
    "biometric_artifacts_used": False,
    "machine_identity_outputs_used": False,
}
DIRECT_AUDIO_FIELDS = {
    "attestation": DIRECT_AUDIO_ATTESTATION,
    "audio_directly_perceived": True,
    "reviewed_entire_interval": True,
    "exactly_one_live_human_speaker": True,
    "overlap_detected": False,
    "playback_detected": False,
    "tts_detected": False,
    "synthetic_voice_detected": False,
    "unknown_audio_origin_detected": False,
    "source_metadata_used_as_identity_evidence": False,
    "channel_context_used_as_identity_evidence": False,
    "transcript_text_used_as_identity_evidence": False,
    "machine_identity_output_used": False,
    "machine_confidence_used": False,
    "speaking_face_claimed": False,
}


class SoloVoiceAttestationManifestError(ResultImportError):
    """A private solo-voice manifest is malformed or unsafe."""


@dataclass(frozen=True)
class SoloVoiceAttestationManifest:
    value: dict[str, Any]
    input_sha256: str
    path: Path

    @property
    def manifest_id(self) -> str:
        return self.value["manifest_id"]


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise SoloVoiceAttestationManifestError(
                f"solo voice manifest contains duplicate JSON key {key!r}"
            )
        value[key] = item
    return value


def _object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SoloVoiceAttestationManifestError(f"{label} must be an object")
    return value


def _array(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise SoloVoiceAttestationManifestError(f"{label} must be an array")
    if len(value) > MAX_ITEMS_PER_COLLECTION:
        raise SoloVoiceAttestationManifestError(
            f"{label} exceeds the {MAX_ITEMS_PER_COLLECTION}-item limit"
        )
    return value


def _exact_keys(
    value: dict[str, Any], label: str, required: set[str]
) -> None:
    missing = sorted(required - set(value))
    unknown = sorted(set(value) - required)
    if missing or unknown:
        details: list[str] = []
        if missing:
            details.append(f"missing {missing}")
        if unknown:
            details.append(f"unknown {unknown}")
        raise SoloVoiceAttestationManifestError(f"{label} has " + "; ".join(details))


def _text(value: object, label: str, *, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > maximum
        or "\x00" in value
    ):
        raise SoloVoiceAttestationManifestError(
            f"{label} must be a non-empty bounded string"
        )
    return value


def _identifier(value: object, label: str) -> str:
    text = _text(value, label, maximum=256)
    if not IDENTIFIER_RE.fullmatch(text):
        raise SoloVoiceAttestationManifestError(
            f"{label} contains unsupported identifier characters"
        )
    return text


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise SoloVoiceAttestationManifestError(
            f"{label} must be a lowercase SHA-256 digest"
        )
    return value


def _integer(
    value: object,
    label: str,
    *,
    minimum: int,
    maximum: int = SQLITE_INTEGER_MAX,
) -> int:
    if type(value) is not int or value < minimum or value > maximum:
        raise SoloVoiceAttestationManifestError(
            f"{label} must be an integer from {minimum} through {maximum}"
        )
    return value


def _timestamp(value: object, label: str) -> tuple[str, datetime]:
    text = _text(value, label, maximum=64)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise SoloVoiceAttestationManifestError(
            f"{label} must be an RFC 3339 timestamp"
        ) from error
    if parsed.tzinfo is None:
        raise SoloVoiceAttestationManifestError(f"{label} must include a UTC offset")
    normalized_value = parsed.astimezone(timezone.utc)
    normalized = normalized_value.isoformat(timespec="seconds").replace("+00:00", "Z")
    if parsed.microsecond or text != normalized:
        raise SoloVoiceAttestationManifestError(
            f"{label} must be a whole-second canonical UTC timestamp ending in Z"
        )
    return normalized, normalized_value


def _enum(value: object, label: str, allowed: set[str]) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise SoloVoiceAttestationManifestError(
            f"{label} must be one of {sorted(allowed)}"
        )
    return value


def _exact_boolean_object(
    value: object, label: str, expected: dict[str, bool | str]
) -> dict[str, bool | str]:
    item = _object(value, label)
    _exact_keys(item, label, set(expected))
    for key, expected_value in expected.items():
        if item[key] is not expected_value and item[key] != expected_value:
            raise SoloVoiceAttestationManifestError(
                f"{label}.{key} must equal {expected_value!r}"
            )
        if isinstance(expected_value, bool) and type(item[key]) is not bool:
            raise SoloVoiceAttestationManifestError(
                f"{label}.{key} must be a boolean"
            )
    return dict(expected)


def _unique_ids(items: Iterable[dict[str, Any]], key: str, label: str) -> None:
    values = [item[key] for item in items]
    duplicates = sorted(value for value, count in Counter(values).items() if count > 1)
    if duplicates:
        raise SoloVoiceAttestationManifestError(
            f"{label} contains duplicate IDs {duplicates[:10]}"
        )


def _parse_subject(raw: object, index: int) -> dict[str, Any]:
    label = f"subjects[{index}]"
    item = _object(raw, label)
    keys = {
        "solo_voice_subject_id",
        "entity_id",
        "source_id",
        "recording_id",
        "rendition_id",
        "media_id",
        "media_sha256",
        "start_ms",
        "end_ms",
        "coordinate_system",
    }
    _exact_keys(item, label, keys)
    start_ms = _integer(item["start_ms"], f"{label}.start_ms", minimum=0)
    end_ms = _integer(item["end_ms"], f"{label}.end_ms", minimum=1)
    if end_ms <= start_ms:
        raise SoloVoiceAttestationManifestError(
            f"{label} must use a non-empty half-open [start_ms, end_ms) interval"
        )
    if item["coordinate_system"] != "rendition_media_ms":
        raise SoloVoiceAttestationManifestError(
            f"{label}.coordinate_system must equal 'rendition_media_ms'"
        )
    return {
        "solo_voice_subject_id": _identifier(
            item["solo_voice_subject_id"], f"{label}.solo_voice_subject_id"
        ),
        "entity_id": _identifier(item["entity_id"], f"{label}.entity_id"),
        "source_id": _identifier(item["source_id"], f"{label}.source_id"),
        "recording_id": _identifier(item["recording_id"], f"{label}.recording_id"),
        "rendition_id": _identifier(item["rendition_id"], f"{label}.rendition_id"),
        "media_id": _identifier(item["media_id"], f"{label}.media_id"),
        "media_sha256": _sha256(item["media_sha256"], f"{label}.media_sha256"),
        "start_ms": start_ms,
        "end_ms": end_ms,
        "coordinate_system": "rendition_media_ms",
    }


def _parse_privacy_review(raw: object, index: int) -> dict[str, Any]:
    label = f"privacy_reviews[{index}]"
    item = _object(raw, label)
    keys = {
        "solo_voice_privacy_review_id",
        "review_decision_id",
        "solo_voice_subject_id",
        "decision",
        "reviewer_id",
        "reviewed_at",
        "basis",
        "privacy_attestation",
    }
    _exact_keys(item, label, keys)
    reviewed_at, reviewed_at_value = _timestamp(
        item["reviewed_at"], f"{label}.reviewed_at"
    )
    return {
        "solo_voice_privacy_review_id": _identifier(
            item["solo_voice_privacy_review_id"],
            f"{label}.solo_voice_privacy_review_id",
        ),
        "review_decision_id": _identifier(
            item["review_decision_id"], f"{label}.review_decision_id"
        ),
        "solo_voice_subject_id": _identifier(
            item["solo_voice_subject_id"], f"{label}.solo_voice_subject_id"
        ),
        "decision": _enum(
            item["decision"], f"{label}.decision", {"clear_private_use", "withhold"}
        ),
        "reviewer_id": _identifier(item["reviewer_id"], f"{label}.reviewer_id"),
        "reviewed_at": reviewed_at,
        "reviewed_at_value": reviewed_at_value,
        "basis": _text(item["basis"], f"{label}.basis", maximum=8_192),
        "privacy_attestation": _exact_boolean_object(
            item["privacy_attestation"],
            f"{label}.privacy_attestation",
            PRIVACY_ATTESTATION,
        ),
    }


def _parse_speaker_decision(raw: object, index: int) -> dict[str, Any]:
    label = f"speaker_decisions[{index}]"
    item = _object(raw, label)
    keys = {
        "solo_voice_attestation_decision_id",
        "review_decision_id",
        "solo_voice_subject_id",
        "decision",
        "reviewer_id",
        "decided_at",
        "basis",
        "direct_audio_attestation",
    }
    _exact_keys(item, label, keys)
    decision = _enum(
        item["decision"],
        f"{label}.decision",
        {"assert", "withdraw", "reject", "dispute"},
    )
    attestation: dict[str, bool | str] | None
    if decision == "assert":
        attestation = _exact_boolean_object(
            item["direct_audio_attestation"],
            f"{label}.direct_audio_attestation",
            DIRECT_AUDIO_FIELDS,
        )
    elif item["direct_audio_attestation"] is not None:
        raise SoloVoiceAttestationManifestError(
            f"{label}.direct_audio_attestation must be null for {decision!r}"
        )
    else:
        attestation = None
    decided_at, decided_at_value = _timestamp(
        item["decided_at"], f"{label}.decided_at"
    )
    return {
        "solo_voice_attestation_decision_id": _identifier(
            item["solo_voice_attestation_decision_id"],
            f"{label}.solo_voice_attestation_decision_id",
        ),
        "review_decision_id": _identifier(
            item["review_decision_id"], f"{label}.review_decision_id"
        ),
        "solo_voice_subject_id": _identifier(
            item["solo_voice_subject_id"], f"{label}.solo_voice_subject_id"
        ),
        "decision": decision,
        "reviewer_id": _identifier(item["reviewer_id"], f"{label}.reviewer_id"),
        "decided_at": decided_at,
        "decided_at_value": decided_at_value,
        "basis": _text(item["basis"], f"{label}.basis", maximum=8_192),
        "direct_audio_attestation": attestation,
    }


def load_solo_voice_attestation_manifest(
    path_value: str | Path,
) -> SoloVoiceAttestationManifest:
    """Read and strictly normalize one immutable private manifest."""

    path = Path(path_value)
    if not path.is_absolute():
        path = path.resolve()
    try:
        body = _stable_read(path, "solo voice manifest", maximum_bytes=MAX_MANIFEST_BYTES)
    except ResultImportError as error:
        raise SoloVoiceAttestationManifestError(str(error)) from error
    digest = hashlib.sha256(body).hexdigest()
    try:
        raw = json.loads(body.decode("utf-8"), object_pairs_hook=_reject_duplicate_json_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SoloVoiceAttestationManifestError(
            f"solo voice manifest is invalid JSON: {error}"
        ) from error
    value = _object(raw, "manifest")
    keys = {
        "schema_version",
        "manifest_id",
        "created_at",
        "authorized_by",
        "basis",
        "subjects",
        "privacy_reviews",
        "speaker_decisions",
    }
    _exact_keys(value, "manifest", keys)
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise SoloVoiceAttestationManifestError("manifest.schema_version must equal 1")
    created_at, created_at_value = _timestamp(value["created_at"], "manifest.created_at")
    if created_at_value > datetime.now(timezone.utc):
        raise SoloVoiceAttestationManifestError("manifest.created_at must not be in the future")
    subjects = [
        _parse_subject(item, index)
        for index, item in enumerate(_array(value["subjects"], "manifest.subjects"))
    ]
    privacy_reviews = [
        _parse_privacy_review(item, index)
        for index, item in enumerate(
            _array(value["privacy_reviews"], "manifest.privacy_reviews")
        )
    ]
    speaker_decisions = [
        _parse_speaker_decision(item, index)
        for index, item in enumerate(
            _array(value["speaker_decisions"], "manifest.speaker_decisions")
        )
    ]
    total = len(subjects) + len(privacy_reviews) + len(speaker_decisions)
    if total == 0:
        raise SoloVoiceAttestationManifestError(
            "manifest must contain at least one subject, privacy review, or speaker decision"
        )
    if total > MAX_TOTAL_ITEMS:
        raise SoloVoiceAttestationManifestError(
            f"manifest exceeds the {MAX_TOTAL_ITEMS}-operation safety limit"
        )
    for item, label in (
        (privacy_reviews, "privacy_reviews"),
        (speaker_decisions, "speaker_decisions"),
    ):
        for index, operation in enumerate(item):
            timestamp_value = (
                operation["reviewed_at_value"]
                if label == "privacy_reviews"
                else operation["decided_at_value"]
            )
            if timestamp_value > created_at_value:
                raise SoloVoiceAttestationManifestError(
                    f"{label}[{index}] may not postdate manifest.created_at"
                )
    _unique_ids(subjects, "solo_voice_subject_id", "manifest.subjects")
    _unique_ids(
        privacy_reviews,
        "solo_voice_privacy_review_id",
        "manifest.privacy_reviews",
    )
    _unique_ids(
        speaker_decisions,
        "solo_voice_attestation_decision_id",
        "manifest.speaker_decisions",
    )
    review_ids = [item["review_decision_id"] for item in privacy_reviews + speaker_decisions]
    duplicates = sorted(value for value, count in Counter(review_ids).items() if count > 1)
    if duplicates:
        raise SoloVoiceAttestationManifestError(
            f"manifest contains duplicate review_decision IDs {duplicates[:10]}"
        )
    operation_ids = [
        *(item["solo_voice_privacy_review_id"] for item in privacy_reviews),
        *(
            item["solo_voice_attestation_decision_id"]
            for item in speaker_decisions
        ),
    ]
    duplicates = sorted(
        value for value, count in Counter(operation_ids).items() if count > 1
    )
    if duplicates:
        raise SoloVoiceAttestationManifestError(
            f"manifest contains cross-stream duplicate operation IDs {duplicates[:10]}"
        )
    natural_subjects = [
        (
            item["entity_id"], item["source_id"], item["recording_id"],
            item["rendition_id"], item["media_id"], item["start_ms"], item["end_ms"],
        )
        for item in subjects
    ]
    natural_duplicates = [
        value for value, count in Counter(natural_subjects).items() if count > 1
    ]
    if natural_duplicates:
        raise SoloVoiceAttestationManifestError(
            "manifest.subjects contains duplicate exact entity/media intervals"
        )
    for operations, timestamp_key, label in (
        (privacy_reviews, "reviewed_at_value", "privacy review"),
        (speaker_decisions, "decided_at_value", "speaker decision"),
    ):
        prior: dict[str, datetime] = {}
        for operation in operations:
            subject_id = operation["solo_voice_subject_id"]
            timestamp_value = operation[timestamp_key]
            if subject_id in prior and timestamp_value <= prior[subject_id]:
                raise SoloVoiceAttestationManifestError(
                    f"{label} streams must be strictly chronological in manifest order"
                )
            prior[subject_id] = timestamp_value
    normalized = {
        "schema_version": 1,
        "manifest_id": _identifier(value["manifest_id"], "manifest.manifest_id"),
        "created_at": created_at,
        "created_at_value": created_at_value,
        "authorized_by": _text(
            value["authorized_by"], "manifest.authorized_by", maximum=500
        ),
        "basis": _text(value["basis"], "manifest.basis", maximum=8_192),
        "subjects": subjects,
        "privacy_reviews": privacy_reviews,
        "speaker_decisions": speaker_decisions,
    }
    return SoloVoiceAttestationManifest(normalized, digest, path.resolve())


def _reviewer_is_active_human_at(
    connection: sqlite3.Connection, reviewer_id: str, effective_at: str, label: str
) -> None:
    row = connection.execute(
        """
        SELECT reviewer.reviewer_kind, reviewer.active, state.new_active
        FROM reviewers AS reviewer
        LEFT JOIN reviewer_admin_events AS state
          ON state.reviewer_id = reviewer.reviewer_id
         AND state.event_sequence = (
             SELECT MAX(candidate.event_sequence)
             FROM reviewer_admin_events AS candidate
             WHERE candidate.reviewer_id = reviewer.reviewer_id
               AND julianday(candidate.effective_at) <= julianday(?)
         )
        WHERE reviewer.reviewer_id = ?
        """,
        (effective_at, reviewer_id),
    ).fetchone()
    if (
        row is None
        or row["reviewer_kind"] != "human"
        or row["active"] != 1
        or row["new_active"] != 1
    ):
        raise SoloVoiceAttestationManifestError(
            f"{label} requires a governed human reviewer active then and now"
        )


def _subject_from_database(
    connection: sqlite3.Connection, subject_id: str
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT solo_voice_subject_id, entity_id, source_id, recording_id,
               rendition_id, media_id, media_sha256, start_ms, end_ms,
               coordinate_system
        FROM solo_voice_subjects
        WHERE solo_voice_subject_id = ?
        """,
        (subject_id,),
    ).fetchone()
    return None if row is None else dict(row)


def _require_confirmed_anchor(
    connection: sqlite3.Connection, subject: dict[str, Any], label: str
) -> None:
    row = connection.execute(
        """
        SELECT 1
        FROM entities AS entity
        JOIN sources AS source ON source.source_id = ?
        JOIN recordings AS recording ON recording.recording_id = ?
        JOIN recording_sources AS recording_source
          ON recording_source.recording_id = recording.recording_id
         AND recording_source.source_id = source.source_id
        JOIN renditions AS rendition ON rendition.rendition_id = ?
        JOIN media_objects AS media ON media.media_id = ?
        JOIN media_sources AS media_source
          ON media_source.media_id = media.media_id
         AND media_source.source_id = source.source_id
        WHERE entity.entity_id = ?
          AND entity.entity_type IN ('person', 'community_figure')
          AND entity.review_state = 'reviewed'
          AND source.review_state = 'reviewed'
          AND recording.review_state = 'reviewed'
          AND recording.merged_into_recording_id IS NULL
          AND recording_source.confidence_state = 'reviewed'
          AND rendition.recording_id = recording.recording_id
          AND rendition.media_id = media.media_id
          AND rendition.review_state = 'reviewed'
          AND media.sha256 = ?
          AND media.integrity_state = 'verified'
          AND media.media_kind IN ('audio', 'video')
          AND media.duration_ms IS NOT NULL
          AND ? <= media.duration_ms
        LIMIT 1
        """,
        (
            subject["source_id"],
            subject["recording_id"],
            subject["rendition_id"],
            subject["media_id"],
            subject["entity_id"],
            subject["media_sha256"],
            subject["end_ms"],
        ),
    ).fetchone()
    if row is None:
        raise SoloVoiceAttestationManifestError(
            f"{label} does not resolve to one reviewed entity and exact confirmed "
            "source/recording/rendition/media anchor"
        )


def _existing_manifest_state(
    connection: sqlite3.Connection, manifest: SoloVoiceAttestationManifest
) -> bool:
    rows = connection.execute(
        """
        SELECT * FROM solo_voice_manifest_imports
        WHERE manifest_id = ? OR input_sha256 = ?
        """,
        (manifest.manifest_id, manifest.input_sha256),
    ).fetchall()
    if not rows:
        return False
    if len(rows) != 1:
        raise SoloVoiceAttestationManifestError(
            "solo voice manifest ID/hash resolve to conflicting import rows"
        )
    row = rows[0]
    expected = (
        manifest.manifest_id,
        manifest.input_sha256,
        SCHEMA_VERSION,
        manifest.value["created_at"],
        manifest.value["authorized_by"],
        manifest.value["basis"],
        len(manifest.value["subjects"]),
        len(manifest.value["privacy_reviews"]),
        len(manifest.value["speaker_decisions"]),
    )
    actual = (
        row["manifest_id"], row["input_sha256"], row["schema_version"],
        row["manifest_created_at"], row["authorized_by"], row["basis"],
        row["subject_count"], row["privacy_review_count"],
        row["speaker_decision_count"],
    )
    if actual != expected:
        raise SoloVoiceAttestationManifestError(
            "solo voice manifest ID or SHA-256 conflicts with a different import"
        )
    subject_rows = connection.execute(
        "SELECT * FROM solo_voice_subjects WHERE manifest_id = ? ORDER BY subject_ordinal",
        (manifest.manifest_id,),
    ).fetchall()
    if len(subject_rows) != len(manifest.value["subjects"]):
        raise SoloVoiceAttestationManifestError(
            "existing solo voice manifest import has incomplete subject rows"
        )
    for ordinal, (subject, stored) in enumerate(
        zip(manifest.value["subjects"], subject_rows, strict=True)
    ):
        expected_subject = (
            subject["solo_voice_subject_id"], subject["entity_id"],
            subject["source_id"], subject["recording_id"], subject["rendition_id"],
            subject["media_id"], subject["media_sha256"], subject["start_ms"],
            subject["end_ms"], subject["coordinate_system"], "private", "none",
            manifest.value["created_at"], manifest.manifest_id, ordinal,
        )
        actual_subject = tuple(
            stored[key]
            for key in (
                "solo_voice_subject_id", "entity_id", "source_id", "recording_id",
                "rendition_id", "media_id", "media_sha256", "start_ms", "end_ms",
                "coordinate_system", "visibility", "publication_authority",
                "created_at", "manifest_id", "subject_ordinal",
            )
        )
        if actual_subject != expected_subject:
            raise SoloVoiceAttestationManifestError(
                "existing solo voice subject differs from the exact manifest"
            )

    privacy_rows = connection.execute(
        "SELECT * FROM solo_voice_privacy_reviews WHERE manifest_id = ? "
        "ORDER BY review_ordinal",
        (manifest.manifest_id,),
    ).fetchall()
    if len(privacy_rows) != len(manifest.value["privacy_reviews"]):
        raise SoloVoiceAttestationManifestError(
            "existing solo voice manifest import has incomplete privacy rows"
        )
    for ordinal, (review, stored) in enumerate(
        zip(manifest.value["privacy_reviews"], privacy_rows, strict=True)
    ):
        attestation = review["privacy_attestation"]
        expected_review = (
            review["solo_voice_privacy_review_id"],
            review["solo_voice_subject_id"], manifest.manifest_id, ordinal,
            review["decision"], review["reviewer_id"], review["review_decision_id"],
            review["reviewed_at"], review["basis"],
            int(attestation["named_voice_personal_data_reviewed"]),
            int(attestation["private_storage_only"]),
            int(attestation["public_export_approved"]),
            int(attestation["biometric_artifacts_used"]),
            int(attestation["machine_identity_outputs_used"]), "private", "none",
        )
        actual_review = tuple(
            stored[key]
            for key in (
                "solo_voice_privacy_review_id", "solo_voice_subject_id",
                "manifest_id", "review_ordinal", "decision", "reviewer_id",
                "review_decision_id", "reviewed_at", "basis",
                "named_voice_personal_data_reviewed", "private_storage_only",
                "public_export_approved", "biometric_artifacts_used",
                "machine_identity_outputs_used", "visibility",
                "publication_authority",
            )
        )
        if actual_review != expected_review:
            raise SoloVoiceAttestationManifestError(
                "existing solo voice privacy review differs from the exact manifest"
            )
        generic = connection.execute(
            "SELECT * FROM review_decisions WHERE review_decision_id = ?",
            (review["review_decision_id"],),
        ).fetchone()
        expected_generic = (
            review["review_decision_id"], "solo_voice_privacy_review",
            review["solo_voice_privacy_review_id"], review["reviewer_id"],
            "accept" if review["decision"] == "clear_private_use" else "reject",
            review["reviewed_at"], 0, 0, 1, None, None, review["basis"],
            None, None,
        )
        if generic is None or tuple(
            generic[key]
            for key in (
                "review_decision_id", "target_type", "target_id", "reviewer_id",
                "decision", "decided_at", "audio_directly_perceived",
                "video_directly_perceived", "reviewed_complete_item",
                "context_start_ms", "context_end_ms", "basis", "review_task_id",
                "notes",
            )
        ) != expected_generic:
            raise SoloVoiceAttestationManifestError(
                "existing solo voice privacy review lacks its exact generic review"
            )

    decision_rows = connection.execute(
        "SELECT * FROM solo_voice_attestation_decisions WHERE manifest_id = ? "
        "ORDER BY decision_ordinal",
        (manifest.manifest_id,),
    ).fetchall()
    if len(decision_rows) != len(manifest.value["speaker_decisions"]):
        raise SoloVoiceAttestationManifestError(
            "existing solo voice manifest import has incomplete speaker rows"
        )
    subject_bounds = {
        item["solo_voice_subject_id"]: (item["start_ms"], item["end_ms"])
        for item in manifest.value["subjects"]
    }
    for decision in manifest.value["speaker_decisions"]:
        if decision["solo_voice_subject_id"] not in subject_bounds:
            subject = _subject_from_database(
                connection, decision["solo_voice_subject_id"]
            )
            if subject is None:
                raise SoloVoiceAttestationManifestError(
                    "existing solo voice decision references a missing subject"
                )
            subject_bounds[decision["solo_voice_subject_id"]] = (
                subject["start_ms"], subject["end_ms"]
            )
    generic_map = {
        "assert": "accept", "withdraw": "correct",
        "reject": "reject", "dispute": "dispute",
    }
    for ordinal, (decision, stored) in enumerate(
        zip(manifest.value["speaker_decisions"], decision_rows, strict=True)
    ):
        attestation = decision["direct_audio_attestation"]
        attestation_values = (
            tuple(
                [attestation["attestation"]]
                + [
                    int(attestation[key])
                    for key in (
                        "audio_directly_perceived", "reviewed_entire_interval",
                        "exactly_one_live_human_speaker", "overlap_detected",
                        "playback_detected", "tts_detected",
                        "synthetic_voice_detected", "unknown_audio_origin_detected",
                        "source_metadata_used_as_identity_evidence",
                        "channel_context_used_as_identity_evidence",
                        "transcript_text_used_as_identity_evidence",
                        "machine_identity_output_used", "machine_confidence_used",
                        "speaking_face_claimed",
                    )
                ]
            )
            if attestation is not None
            else (None,) * 15
        )
        expected_decision = (
            decision["solo_voice_attestation_decision_id"],
            decision["solo_voice_subject_id"], manifest.manifest_id, ordinal,
            decision["decision"], decision["reviewer_id"],
            decision["review_decision_id"], decision["decided_at"],
            decision["basis"], *attestation_values, "private", "none",
        )
        actual_decision = tuple(
            stored[key]
            for key in (
                "solo_voice_attestation_decision_id", "solo_voice_subject_id",
                "manifest_id", "decision_ordinal", "decision", "reviewer_id",
                "review_decision_id", "decided_at", "basis",
                "direct_audio_attestation", "audio_directly_perceived",
                "reviewed_entire_interval", "exactly_one_live_human_speaker",
                "overlap_detected", "playback_detected", "tts_detected",
                "synthetic_voice_detected", "unknown_audio_origin_detected",
                "source_metadata_used_as_identity_evidence",
                "channel_context_used_as_identity_evidence",
                "transcript_text_used_as_identity_evidence",
                "machine_identity_output_used", "machine_confidence_used",
                "speaking_face_claimed", "visibility", "publication_authority",
            )
        )
        if actual_decision != expected_decision:
            raise SoloVoiceAttestationManifestError(
                "existing solo voice decision differs from the exact manifest"
            )
        generic = connection.execute(
            "SELECT * FROM review_decisions WHERE review_decision_id = ?",
            (decision["review_decision_id"],),
        ).fetchone()
        start_ms, end_ms = subject_bounds[decision["solo_voice_subject_id"]]
        is_assert = decision["decision"] == "assert"
        expected_generic = (
            decision["review_decision_id"], "solo_voice_subject",
            decision["solo_voice_subject_id"], decision["reviewer_id"],
            generic_map[decision["decision"]], decision["decided_at"],
            int(is_assert), 0, int(is_assert), start_ms, end_ms, decision["basis"],
            None, None,
        )
        if generic is None or tuple(
            generic[key]
            for key in (
                "review_decision_id", "target_type", "target_id", "reviewer_id",
                "decision", "decided_at", "audio_directly_perceived",
                "video_directly_perceived", "reviewed_complete_item",
                "context_start_ms", "context_end_ms", "basis", "review_task_id",
                "notes",
            )
        ) != expected_generic:
            raise SoloVoiceAttestationManifestError(
                "existing solo voice decision lacks its exact generic review"
            )
    return True


def _validate_database_references(
    connection: sqlite3.Connection, manifest: SoloVoiceAttestationManifest
) -> bool:
    reserved_publication_state = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM publication_decisions
           WHERE object_type IN (
               'solo_voice_subject', 'solo_voice_privacy_review',
               'solo_voice_attestation_decision'
           ))
          +
          (SELECT count(*) FROM publication_gate_decisions
           WHERE object_type IN (
               'solo_voice_subject', 'solo_voice_privacy_review',
               'solo_voice_attestation_decision'
           ))
        """
    ).fetchone()[0]
    if reserved_publication_state:
        raise SoloVoiceAttestationManifestError(
            "catalog contains forbidden reserved solo voice publication state"
        )
    if _existing_manifest_state(connection, manifest):
        return True

    manifest_subjects = {
        item["solo_voice_subject_id"]: item for item in manifest.value["subjects"]
    }
    subjects: dict[str, dict[str, Any]] = dict(manifest_subjects)
    referenced_ids = {
        item["solo_voice_subject_id"]
        for item in manifest.value["privacy_reviews"] + manifest.value["speaker_decisions"]
    }
    for subject_id in sorted(referenced_ids - set(subjects)):
        existing = _subject_from_database(connection, subject_id)
        if existing is None:
            raise SoloVoiceAttestationManifestError(
                f"operation references unknown solo voice subject {subject_id!r}"
            )
        subjects[subject_id] = existing

    for index, subject in enumerate(manifest.value["subjects"]):
        if _subject_from_database(connection, subject["solo_voice_subject_id"]) is not None:
            raise SoloVoiceAttestationManifestError(
                f"subjects[{index}] reuses an existing solo_voice_subject_id"
            )
        collision = connection.execute(
            """
            SELECT 1 FROM solo_voice_subjects
            WHERE entity_id = ? AND source_id = ? AND recording_id = ?
              AND rendition_id = ? AND media_id = ? AND start_ms = ? AND end_ms = ?
            """,
            (
                subject["entity_id"], subject["source_id"], subject["recording_id"],
                subject["rendition_id"], subject["media_id"], subject["start_ms"],
                subject["end_ms"],
            ),
        ).fetchone()
        if collision is not None:
            raise SoloVoiceAttestationManifestError(
                f"subjects[{index}] duplicates an existing exact entity/media interval"
            )
        _require_confirmed_anchor(connection, subject, f"subjects[{index}]")

    all_operation_ids = [
        *(item["solo_voice_privacy_review_id"] for item in manifest.value["privacy_reviews"]),
        *(
            item["solo_voice_attestation_decision_id"]
            for item in manifest.value["speaker_decisions"]
        ),
    ]
    review_ids = [
        item["review_decision_id"]
        for item in manifest.value["privacy_reviews"] + manifest.value["speaker_decisions"]
    ]
    if all_operation_ids:
        placeholders = ",".join("?" for _ in all_operation_ids)
        collision = connection.execute(
            f"""
            SELECT 1 FROM solo_voice_privacy_reviews
            WHERE solo_voice_privacy_review_id IN ({placeholders})
            UNION ALL
            SELECT 1 FROM solo_voice_attestation_decisions
            WHERE solo_voice_attestation_decision_id IN ({placeholders})
            LIMIT 1
            """,
            tuple(all_operation_ids + all_operation_ids),
        ).fetchone()
        if collision is not None:
            raise SoloVoiceAttestationManifestError(
                "manifest operation ID collides with an existing solo voice row"
            )
    if review_ids:
        placeholders = ",".join("?" for _ in review_ids)
        if connection.execute(
            f"SELECT 1 FROM review_decisions WHERE review_decision_id IN ({placeholders}) LIMIT 1",
            tuple(review_ids),
        ).fetchone() is not None:
            raise SoloVoiceAttestationManifestError(
                "manifest review_decision_id collides with an existing review"
            )

    last_privacy_time: dict[str, datetime] = {}
    privacy_state: dict[str, dict[str, Any]] = {}
    for subject_id in referenced_ids:
        row = connection.execute(
            """
            SELECT *
            FROM current_solo_voice_privacy_reviews
            WHERE solo_voice_subject_id = ?
            """,
            (subject_id,),
        ).fetchone()
        if row is not None:
            timestamp = datetime.fromisoformat(row["reviewed_at"].replace("Z", "+00:00"))
            last_privacy_time[subject_id] = timestamp
            privacy_state[subject_id] = {"_existing": True, **dict(row)}
    for index, review in enumerate(manifest.value["privacy_reviews"]):
        subject_id = review["solo_voice_subject_id"]
        prior = last_privacy_time.get(subject_id)
        if prior is not None and review["reviewed_at_value"] <= prior:
            raise SoloVoiceAttestationManifestError(
                f"privacy_reviews[{index}] must postdate the existing subject stream"
            )
        _reviewer_is_active_human_at(
            connection,
            review["reviewer_id"],
            review["reviewed_at"],
            f"privacy_reviews[{index}]",
        )
        last_privacy_time[subject_id] = review["reviewed_at_value"]
        privacy_state[subject_id] = review

    current_decisions = {
        row["solo_voice_subject_id"]: row["decision"]
        for row in connection.execute(
            "SELECT solo_voice_subject_id, decision "
            "FROM current_solo_voice_attestation_decisions"
        )
    }
    last_decision_time = {
        row["solo_voice_subject_id"]: datetime.fromisoformat(
            row["decided_at"].replace("Z", "+00:00")
        )
        for row in connection.execute(
            "SELECT solo_voice_subject_id, decided_at "
            "FROM current_solo_voice_attestation_decisions"
        )
    }
    for index, decision in enumerate(manifest.value["speaker_decisions"]):
        subject_id = decision["solo_voice_subject_id"]
        prior = last_decision_time.get(subject_id)
        if prior is not None and decision["decided_at_value"] <= prior:
            raise SoloVoiceAttestationManifestError(
                f"speaker_decisions[{index}] must postdate the existing subject stream"
            )
        _reviewer_is_active_human_at(
            connection,
            decision["reviewer_id"],
            decision["decided_at"],
            f"speaker_decisions[{index}]",
        )
        subject = subjects[subject_id]
        if decision["decision"] == "assert":
            _require_confirmed_anchor(
                connection, subject, f"speaker_decisions[{index}] subject"
            )
            privacy = privacy_state.get(subject_id)
            if (
                privacy is None
                or privacy["decision"] != "clear_private_use"
                or privacy["reviewer_id"] == decision["reviewer_id"]
                or datetime.fromisoformat(
                    privacy["reviewed_at"].replace("Z", "+00:00")
                ) >= decision["decided_at_value"]
            ):
                raise SoloVoiceAttestationManifestError(
                    f"speaker_decisions[{index}] requires an earlier independent current "
                    "privacy/biometric clearance"
                )
            _reviewer_is_active_human_at(
                connection,
                privacy["reviewer_id"],
                privacy["reviewed_at"],
                f"speaker_decisions[{index}] current privacy clearance",
            )
            if privacy.get("_existing"):
                if (
                    privacy["named_voice_personal_data_reviewed"] != 1
                    or privacy["private_storage_only"] != 1
                    or privacy["public_export_approved"] != 0
                    or privacy["biometric_artifacts_used"] != 0
                    or privacy["machine_identity_outputs_used"] != 0
                    or privacy["visibility"] != "private"
                    or privacy["publication_authority"] != "none"
                ):
                    raise SoloVoiceAttestationManifestError(
                        f"speaker_decisions[{index}] current privacy clearance "
                        "violates private no-biometric/no-machine authority"
                    )
                generic = connection.execute(
                    "SELECT * FROM review_decisions WHERE review_decision_id = ?",
                    (privacy["review_decision_id"],),
                ).fetchone()
                expected_generic = (
                    privacy["review_decision_id"],
                    "solo_voice_privacy_review",
                    privacy["solo_voice_privacy_review_id"],
                    privacy["reviewer_id"],
                    "accept",
                    privacy["reviewed_at"],
                    0,
                    0,
                    1,
                    None,
                    None,
                    privacy["basis"],
                    None,
                    None,
                )
                if generic is None or tuple(
                    generic[key]
                    for key in (
                        "review_decision_id", "target_type", "target_id",
                        "reviewer_id", "decision", "decided_at",
                        "audio_directly_perceived", "video_directly_perceived",
                        "reviewed_complete_item", "context_start_ms",
                        "context_end_ms", "basis", "review_task_id", "notes",
                    )
                ) != expected_generic:
                    raise SoloVoiceAttestationManifestError(
                        f"speaker_decisions[{index}] current privacy clearance lacks "
                        "exact governed review lineage"
                    )
            for existing_id, state in current_decisions.items():
                if state != "assert" or existing_id == subject_id:
                    continue
                existing = subjects.get(existing_id) or _subject_from_database(
                    connection, existing_id
                )
                if existing is None:
                    raise SoloVoiceAttestationManifestError(
                        "current solo voice decision references a missing subject"
                    )
                if (
                    existing["media_id"] == subject["media_id"]
                    and existing["start_ms"] < subject["end_ms"]
                    and subject["start_ms"] < existing["end_ms"]
                    and existing["entity_id"] != subject["entity_id"]
                ):
                    raise SoloVoiceAttestationManifestError(
                        f"speaker_decisions[{index}] conflicts with current overlapping "
                        "named solo voice identity"
                    )
        current_decisions[subject_id] = decision["decision"]
        last_decision_time[subject_id] = decision["decided_at_value"]
    return False


def _result(
    manifest: SoloVoiceAttestationManifest,
    *,
    dry_run: bool,
    idempotent_replay: bool,
    imported_at: str | None,
) -> dict[str, object]:
    return {
        "manifest_id": manifest.manifest_id,
        "schema_version": SCHEMA_VERSION,
        "input_sha256": manifest.input_sha256,
        "validated": True,
        "dry_run": dry_run,
        "idempotent_replay": idempotent_replay,
        "subject_count": len(manifest.value["subjects"]),
        "privacy_review_count": len(manifest.value["privacy_reviews"]),
        "speaker_decision_count": len(manifest.value["speaker_decisions"]),
        "imported_at": imported_at,
        "visibility": "private",
        "publication_authority": False,
        "biometric_artifacts_used": False,
        "machine_identity_outputs_used": False,
        "machine_confidence_used": False,
    }


def apply_solo_voice_attestation_manifest(
    connection: sqlite3.Connection,
    path: str | Path,
    *,
    dry_run: bool = True,
    expected_input_sha256: str | None = None,
) -> dict[str, object]:
    """Validate, or explicitly apply, one manifest as a single transaction."""

    manifest = load_solo_voice_attestation_manifest(path)
    if not dry_run and expected_input_sha256 is None:
        raise SoloVoiceAttestationManifestError(
            "expected_input_sha256 is required for an applying import"
        )
    if expected_input_sha256 is not None:
        expected_digest = _sha256(
            expected_input_sha256, "expected_input_sha256"
        )
        if expected_digest != manifest.input_sha256:
            raise SoloVoiceAttestationManifestError(
                "expected_input_sha256 does not match the stable-read manifest digest"
            )
    try:
        replay = _validate_database_references(connection, manifest)
    except sqlite3.DatabaseError as error:
        raise SoloVoiceAttestationManifestError(
            f"catalog rejected solo voice manifest: {error}"
        ) from error
    if dry_run or replay:
        return _result(
            manifest,
            dry_run=dry_run,
            idempotent_replay=replay,
            imported_at=None,
        )

    imported_at = utc_now()
    try:
        with transaction(connection):
            if _validate_database_references(connection, manifest):
                return _result(
                    manifest,
                    dry_run=False,
                    idempotent_replay=True,
                    imported_at=None,
                )
            connection.execute(
                """
                INSERT INTO solo_voice_manifest_imports(
                    manifest_id, input_sha256, schema_version,
                    manifest_created_at, imported_at, authorized_by, basis,
                    subject_count, privacy_review_count, speaker_decision_count
                ) VALUES(?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    manifest.manifest_id,
                    manifest.input_sha256,
                    manifest.value["created_at"],
                    imported_at,
                    manifest.value["authorized_by"],
                    manifest.value["basis"],
                    len(manifest.value["subjects"]),
                    len(manifest.value["privacy_reviews"]),
                    len(manifest.value["speaker_decisions"]),
                ),
            )
            for ordinal, subject in enumerate(manifest.value["subjects"]):
                connection.execute(
                    """
                    INSERT INTO solo_voice_subjects(
                        solo_voice_subject_id, entity_id, source_id, recording_id,
                        rendition_id, media_id, media_sha256, start_ms, end_ms,
                        coordinate_system, visibility, publication_authority,
                        created_at, manifest_id, subject_ordinal
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'private', 'none', ?, ?, ?)
                    """,
                    (
                        subject["solo_voice_subject_id"], subject["entity_id"],
                        subject["source_id"], subject["recording_id"],
                        subject["rendition_id"], subject["media_id"],
                        subject["media_sha256"], subject["start_ms"], subject["end_ms"],
                        subject["coordinate_system"], manifest.value["created_at"],
                        manifest.manifest_id, ordinal,
                    ),
                )
            for ordinal, review in enumerate(manifest.value["privacy_reviews"]):
                generic_decision = "accept" if review["decision"] == "clear_private_use" else "reject"
                connection.execute(
                    """
                    INSERT INTO review_decisions(
                        review_decision_id, target_type, target_id, reviewer_id,
                        decision, decided_at, audio_directly_perceived,
                        video_directly_perceived, reviewed_complete_item,
                        context_start_ms, context_end_ms, basis
                    ) VALUES(?, 'solo_voice_privacy_review', ?, ?, ?, ?, 0, 0, 1,
                             NULL, NULL, ?)
                    """,
                    (
                        review["review_decision_id"],
                        review["solo_voice_privacy_review_id"],
                        review["reviewer_id"], generic_decision,
                        review["reviewed_at"], review["basis"],
                    ),
                )
                attestation = review["privacy_attestation"]
                connection.execute(
                    """
                    INSERT INTO solo_voice_privacy_reviews(
                        solo_voice_privacy_review_id, solo_voice_subject_id,
                        manifest_id, review_ordinal, decision, reviewer_id,
                        review_decision_id, reviewed_at, basis,
                        named_voice_personal_data_reviewed, private_storage_only,
                        public_export_approved, biometric_artifacts_used,
                        machine_identity_outputs_used, visibility,
                        publication_authority
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'private', 'none')
                    """,
                    (
                        review["solo_voice_privacy_review_id"],
                        review["solo_voice_subject_id"], manifest.manifest_id, ordinal,
                        review["decision"], review["reviewer_id"],
                        review["review_decision_id"], review["reviewed_at"],
                        review["basis"],
                        int(attestation["named_voice_personal_data_reviewed"]),
                        int(attestation["private_storage_only"]),
                        int(attestation["public_export_approved"]),
                        int(attestation["biometric_artifacts_used"]),
                        int(attestation["machine_identity_outputs_used"]),
                    ),
                )
            decision_map = {
                "assert": "accept",
                "withdraw": "correct",
                "reject": "reject",
                "dispute": "dispute",
            }
            for ordinal, decision in enumerate(manifest.value["speaker_decisions"]):
                subject = connection.execute(
                    "SELECT start_ms, end_ms FROM solo_voice_subjects "
                    "WHERE solo_voice_subject_id = ?",
                    (decision["solo_voice_subject_id"],),
                ).fetchone()
                if subject is None:  # pragma: no cover - prevalidation proves this.
                    raise SoloVoiceAttestationManifestError(
                        "speaker decision references a missing subject"
                    )
                is_assert = decision["decision"] == "assert"
                connection.execute(
                    """
                    INSERT INTO review_decisions(
                        review_decision_id, target_type, target_id, reviewer_id,
                        decision, decided_at, audio_directly_perceived,
                        video_directly_perceived, reviewed_complete_item,
                        context_start_ms, context_end_ms, basis
                    ) VALUES(?, 'solo_voice_subject', ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
                    """,
                    (
                        decision["review_decision_id"],
                        decision["solo_voice_subject_id"], decision["reviewer_id"],
                        decision_map[decision["decision"]], decision["decided_at"],
                        int(is_assert), int(is_assert), subject["start_ms"],
                        subject["end_ms"], decision["basis"],
                    ),
                )
                attestation = decision["direct_audio_attestation"]
                values = (
                    (
                        attestation["attestation"],
                        int(attestation["audio_directly_perceived"]),
                        int(attestation["reviewed_entire_interval"]),
                        int(attestation["exactly_one_live_human_speaker"]),
                        int(attestation["overlap_detected"]),
                        int(attestation["playback_detected"]),
                        int(attestation["tts_detected"]),
                        int(attestation["synthetic_voice_detected"]),
                        int(attestation["unknown_audio_origin_detected"]),
                        int(attestation["source_metadata_used_as_identity_evidence"]),
                        int(attestation["channel_context_used_as_identity_evidence"]),
                        int(attestation["transcript_text_used_as_identity_evidence"]),
                        int(attestation["machine_identity_output_used"]),
                        int(attestation["machine_confidence_used"]),
                        int(attestation["speaking_face_claimed"]),
                    )
                    if attestation is not None
                    else (None,) * 15
                )
                connection.execute(
                    """
                    INSERT INTO solo_voice_attestation_decisions(
                        solo_voice_attestation_decision_id, solo_voice_subject_id,
                        manifest_id, decision_ordinal, decision, reviewer_id,
                        review_decision_id, decided_at, basis,
                        direct_audio_attestation, audio_directly_perceived,
                        reviewed_entire_interval, exactly_one_live_human_speaker,
                        overlap_detected, playback_detected, tts_detected,
                        synthetic_voice_detected, unknown_audio_origin_detected,
                        source_metadata_used_as_identity_evidence,
                        channel_context_used_as_identity_evidence,
                        transcript_text_used_as_identity_evidence,
                        machine_identity_output_used, machine_confidence_used,
                        speaking_face_claimed, visibility, publication_authority
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                             ?, ?, ?, ?, ?, ?, 'private', 'none')
                    """,
                    (
                        decision["solo_voice_attestation_decision_id"],
                        decision["solo_voice_subject_id"], manifest.manifest_id,
                        ordinal, decision["decision"], decision["reviewer_id"],
                        decision["review_decision_id"], decision["decided_at"],
                        decision["basis"], *values,
                    ),
                )
    except sqlite3.DatabaseError as error:
        raise SoloVoiceAttestationManifestError(
            f"database rejected solo voice manifest atomically: {error}"
        ) from error
    return _result(
        manifest,
        dry_run=False,
        idempotent_replay=False,
        imported_at=imported_at,
    )


def validate_solo_voice_attestation_manifest(
    connection: sqlite3.Connection, path: str | Path
) -> dict[str, object]:
    """Run every admission check without creating any row."""

    return apply_solo_voice_attestation_manifest(connection, path, dry_run=True)
