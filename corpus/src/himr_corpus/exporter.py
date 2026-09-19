"""Deny-by-default deterministic static corpus release exporter."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from .importers import canonical_json
from .private_acquisition import assert_no_restricted_publication_state


PUBLIC_SCHEMA_VERSION = 1

_ID_PATTERNS = {
    "recording_id": re.compile(r"^rec_[a-f0-9]{32}$"),
    "source_id": re.compile(r"^src_[a-f0-9]{32}$"),
    "revision_id": re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$"),
    "segment_id": re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$"),
}
_RELEASE_ID_PATTERN = re.compile(r"^release_[a-f0-9]{24}$")
_SLUG_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")
_UTC_TIMESTAMP_PATTERN = re.compile(
    r"^(?:19|20)\d{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])"
    r"T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?Z$"
)
_RECORDING_TYPES = {
    "video",
    "livestream",
    "short",
    "guest_appearance",
    "compilation",
    "unknown",
}
_RECORDING_REVIEW_STATES = {"metadata_only", "unreviewed", "reviewed", "disputed"}
_TRANSCRIPT_REVIEW_STATES = {
    "machine",
    "human_corrected",
    "media_checked",
    "disputed",
}
_TRANSCRIPT_REVISION_KINDS = {
    "raw_asr",
    "contextual_asr",
    "human_verbatim",
    "readability_edit",
}
_TRANSCRIPT_LIFECYCLE_STATES = {
    "active",
    "retracted",
    "disputed",
    "reinstated",
}
_TRANSCRIPT_LIFECYCLE_REASON_CODES = {
    "transcription_error",
    "speaker_misattribution",
    "source_mismatch",
    "privacy",
    "rights",
    "sensitivity",
    "editorial_decision",
    "other",
}
_TRANSCRIPT_DISCLAIMER_CODES = {
    "machine_generated_unreviewed_not_verified_quotation_v1",
    "disputed_transcript_not_verified_quotation_v1",
    "reviewed_transcript_not_fact_checked_v1",
    "retracted_transcript_text_withdrawn_v1",
}
_CONFIDENCE_BANDS = {"low", "medium", "high", "human", None}


def _is_integer(value: object) -> bool:
    return type(value) is int


def _require_identifier(value: object, field: str) -> str:
    if not isinstance(value, str) or not _ID_PATTERNS[field].fullmatch(value):
        raise ValueError(f"Invalid {field}")
    return value


def _require_nonempty_string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Invalid {field}")
    return value


def _require_utc_timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str) or not _UTC_TIMESTAMP_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be an explicit UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field} must be a valid UTC timestamp") from error
    if parsed.tzinfo != timezone.utc:
        raise ValueError(f"{field} must use the UTC Z designator")
    return parsed


def _canonical_utc_timestamp(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a timestamp string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field} must be a valid timestamp") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must include a UTC offset")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _release_payload(release: dict) -> dict:
    return {
        "schema_version": release["schema_version"],
        "generated_at": release["generated_at"],
        "counts": release["counts"],
        "recordings": release["recordings"],
    }


def build_release(connection: sqlite3.Connection) -> dict:
    assert_no_restricted_publication_state(connection)
    generated_row = connection.execute(
        """
        WITH visible_objects(object_type, object_id) AS (
            SELECT 'source', source_id FROM public_sources
            UNION
            SELECT 'recording', recording_id FROM public_recordings
            UNION
            SELECT 'transcript_revision', revision_id
            FROM public_transcript_revisions
            UNION
            SELECT 'transcript_revision', revision_id
            FROM public_transcript_revision_tombstones
        ),
        relevant_decisions(decided_at) AS (
            SELECT publication.decided_at
            FROM current_publication_decisions AS publication
            JOIN visible_objects AS visible
              ON visible.object_type = publication.object_type
             AND visible.object_id = publication.object_id
            UNION ALL
            SELECT gate.decided_at
            FROM current_publication_gate_decisions AS gate
            JOIN visible_objects AS visible
              ON visible.object_type = gate.object_type
             AND visible.object_id = gate.object_id
            UNION ALL
            SELECT lifecycle.decided_at
            FROM transcript_lifecycle_decisions AS lifecycle
            JOIN visible_objects AS visible
              ON visible.object_type = 'transcript_revision'
             AND visible.object_id = lifecycle.revision_id
        )
        SELECT decided_at AS generated_at
        FROM relevant_decisions
        ORDER BY julianday(decided_at) DESC, decided_at DESC
        LIMIT 1
        """
    ).fetchone()
    generated_at = (
        _canonical_utc_timestamp(generated_row["generated_at"], "release decision time")
        if generated_row is not None and generated_row["generated_at"] is not None
        else "1970-01-01T00:00:00Z"
    )
    recordings: list[dict] = []
    sources_by_recording: dict[str, list[dict]] = {}
    for source in connection.execute(
        """
        SELECT DISTINCT link.recording_id, source.source_id, source.platform,
               source.canonical_url, source.native_id, source.access_state
        FROM recording_sources AS link
        JOIN public_sources AS source ON source.source_id = link.source_id
        JOIN public_recordings AS recording ON recording.recording_id = link.recording_id
        WHERE source.canonical_url IS NOT NULL
        ORDER BY link.recording_id, source.platform, source.native_id, source.source_id
        """
    ):
        sources_by_recording.setdefault(source["recording_id"], []).append(
            {
                "source_id": source["source_id"],
                "platform": source["platform"],
                "url": source["canonical_url"],
                "native_id": source["native_id"],
                "access_state": source["access_state"],
            }
        )

    segments_by_revision: dict[str, list[dict]] = {}
    for segment in connection.execute(
        """
        SELECT segment.revision_id, segment.segment_id, segment.start_ms,
               segment.end_ms, segment.text, segment.speaker_label,
               segment.confidence_band, segment.calibrated_probability
        FROM public_transcript_segments AS segment
        ORDER BY segment.revision_id, segment.ordinal, segment.start_ms, segment.segment_id
        """
    ):
        segments_by_revision.setdefault(segment["revision_id"], []).append(
            {
                "segment_id": segment["segment_id"],
                "start_ms": segment["start_ms"],
                "end_ms": segment["end_ms"],
                "text": segment["text"],
                "speaker_label": segment["speaker_label"],
                "confidence_band": segment["confidence_band"],
                "calibrated_probability": segment["calibrated_probability"],
            }
        )

    lifecycle_by_revision: dict[str, list[dict]] = {}
    for lifecycle in connection.execute(
        """
        SELECT revision_id, lifecycle_state, reason_code, decided_at,
               public_explanation
        FROM transcript_lifecycle_decisions
        ORDER BY revision_id, lifecycle_decision_sequence
        """
    ):
        lifecycle_by_revision.setdefault(lifecycle["revision_id"], []).append(
            {
                "state": lifecycle["lifecycle_state"],
                "reason_code": lifecycle["reason_code"],
                "decided_at": lifecycle["decided_at"],
                "explanation": lifecycle["public_explanation"],
            }
        )

    revisions_by_recording: dict[str, list[dict]] = {}
    for revision in connection.execute(
        """
        SELECT recording_id, revision_id, revision_kind, language, review_state,
               machine_generated, verified_quotation, disclaimer_code,
               lifecycle_state, created_at
        FROM public_transcript_revisions
        ORDER BY recording_id, created_at, revision_id
        """
    ):
        revisions_by_recording.setdefault(revision["recording_id"], []).append(
            {
                "revision_id": revision["revision_id"],
                "revision_kind": revision["revision_kind"],
                "language": revision["language"],
                "review_state": revision["review_state"],
                "machine_generated": bool(revision["machine_generated"]),
                "unreviewed": revision["review_state"] == "machine",
                "verified_quotation": bool(revision["verified_quotation"]),
                "disclaimer_code": revision["disclaimer_code"],
                "lifecycle_state": revision["lifecycle_state"],
                "lifecycle_history": lifecycle_by_revision.get(
                    revision["revision_id"], []
                ),
                "segments": segments_by_revision.get(revision["revision_id"], []),
            }
        )

    for tombstone in connection.execute(
        """
        SELECT recording_id, revision_id, revision_kind, language, review_state,
               machine_generated, verified_quotation, disclaimer_code,
               lifecycle_state, created_at
        FROM public_transcript_revision_tombstones
        ORDER BY recording_id, created_at, revision_id
        """
    ):
        revisions_by_recording.setdefault(tombstone["recording_id"], []).append(
            {
                "revision_id": tombstone["revision_id"],
                "revision_kind": tombstone["revision_kind"],
                "language": tombstone["language"],
                "review_state": tombstone["review_state"],
                "machine_generated": bool(tombstone["machine_generated"]),
                "unreviewed": tombstone["review_state"] == "machine",
                "verified_quotation": bool(tombstone["verified_quotation"]),
                "disclaimer_code": tombstone["disclaimer_code"],
                "lifecycle_state": tombstone["lifecycle_state"],
                "lifecycle_history": lifecycle_by_revision.get(
                    tombstone["revision_id"], []
                ),
                "segments": [],
            }
        )

    for revisions in revisions_by_recording.values():
        revisions.sort(key=lambda value: value["revision_id"])

    public_recordings = connection.execute(
        """
        SELECT recording_id, slug, title, date_label, date_year, date_basis,
               duration_ms, recording_type, review_state
        FROM public_recordings
        ORDER BY COALESCE(date_label, '9999-99-99'), title COLLATE NOCASE, recording_id
        """
    ).fetchall()
    source_total = 0
    revision_total = 0
    segment_total = 0
    for row in public_recordings:
        sources = sources_by_recording.get(row["recording_id"], [])
        source_total += len(sources)
        revisions = revisions_by_recording.get(row["recording_id"], [])
        segment_total += sum(len(revision["segments"]) for revision in revisions)
        revision_total += len(revisions)
        recordings.append(
            {
                "recording_id": row["recording_id"],
                "slug": row["slug"],
                "title": row["title"],
                "date_label": row["date_label"],
                "date_year": row["date_year"],
                "date_basis": row["date_basis"],
                "duration_ms": row["duration_ms"],
                "recording_type": row["recording_type"],
                "review_state": row["review_state"],
                "sources": sources,
                "transcript_revisions": revisions,
            }
        )

    payload = {
        "schema_version": PUBLIC_SCHEMA_VERSION,
        "generated_at": generated_at,
        "counts": {
            "recordings": len(recordings),
            "sources": source_total,
            "transcript_revisions": revision_total,
            "segments": segment_total,
        },
        "recordings": recordings,
    }
    release_digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    return {
        "schema_version": PUBLIC_SCHEMA_VERSION,
        "release_id": f"release_{release_digest[:24]}",
        "generated_at": generated_at,
        "counts": payload["counts"],
        "recordings": recordings,
    }


def export_release(connection: sqlite3.Connection, output_path: str | Path) -> dict:
    release = build_release(connection)
    validate_release_shape(release)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(release, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    temporary = output.with_name(f".{output.name}.tmp")
    temporary.write_bytes(encoded)
    os.replace(temporary, output)
    return {
        "path": str(output),
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "bytes": len(encoded),
        **release["counts"],
        "release_id": release["release_id"],
        "generated_at": release["generated_at"],
    }


def validate_release_shape(release: object) -> None:
    if not isinstance(release, dict):
        raise ValueError("Release must be an object")
    expected_keys = {
        "schema_version",
        "release_id",
        "generated_at",
        "counts",
        "recordings",
    }
    if set(release) != expected_keys:
        raise ValueError(f"Release keys differ: {set(release) ^ expected_keys}")
    if release["schema_version"] != PUBLIC_SCHEMA_VERSION:
        raise ValueError("Unsupported release schema version")
    if not isinstance(release["release_id"], str) or not _RELEASE_ID_PATTERN.fullmatch(
        release["release_id"]
    ):
        raise ValueError("Invalid release_id")
    _require_utc_timestamp(release["generated_at"], "generated_at")
    counts = release["counts"]
    if not isinstance(counts, dict) or set(counts) != {
        "recordings",
        "sources",
        "transcript_revisions",
        "segments",
    }:
        raise ValueError("Invalid counts object")
    if any(not _is_integer(value) or value < 0 for value in counts.values()):
        raise ValueError("Counts must be non-negative integers")
    recordings = release["recordings"]
    if not isinstance(recordings, list) or counts["recordings"] != len(recordings):
        raise ValueError("Recording count mismatch")
    source_count = revision_count = segment_count = 0
    recording_keys = {
        "recording_id",
        "slug",
        "title",
        "date_label",
        "date_year",
        "date_basis",
        "duration_ms",
        "recording_type",
        "review_state",
        "sources",
        "transcript_revisions",
    }
    source_keys = {"source_id", "platform", "url", "native_id", "access_state"}
    revision_keys = {
        "revision_id",
        "revision_kind",
        "language",
        "review_state",
        "machine_generated",
        "unreviewed",
        "verified_quotation",
        "disclaimer_code",
        "lifecycle_state",
        "lifecycle_history",
        "segments",
    }
    lifecycle_keys = {"state", "reason_code", "decided_at", "explanation"}
    segment_keys = {
        "segment_id",
        "start_ms",
        "end_ms",
        "text",
        "speaker_label",
        "confidence_band",
        "calibrated_probability",
    }
    seen_recording_ids: set[str] = set()
    seen_slugs: set[str] = set()
    seen_revision_ids: set[str] = set()
    seen_segment_ids: set[str] = set()
    for recording in recordings:
        if not isinstance(recording, dict) or set(recording) != recording_keys:
            raise ValueError("Invalid recording object")
        recording_id = _require_identifier(recording["recording_id"], "recording_id")
        if recording_id in seen_recording_ids:
            raise ValueError("Duplicate recording_id")
        seen_recording_ids.add(recording_id)
        slug = _require_nonempty_string(recording["slug"], "recording slug")
        if not _SLUG_PATTERN.fullmatch(slug) or slug in seen_slugs:
            raise ValueError("Invalid or duplicate recording slug")
        seen_slugs.add(slug)
        _require_nonempty_string(recording["title"], "recording title")
        _require_nonempty_string(recording["date_basis"], "recording date_basis")
        if recording["date_label"] is not None and not isinstance(
            recording["date_label"], str
        ):
            raise ValueError("Invalid recording date_label")
        if recording["date_year"] is not None and not _is_integer(recording["date_year"]):
            raise ValueError("Invalid recording date_year")
        if recording["recording_type"] not in _RECORDING_TYPES:
            raise ValueError("Invalid recording_type")
        if recording["review_state"] not in _RECORDING_REVIEW_STATES:
            raise ValueError("Unsafe recording review_state")
        if recording["duration_ms"] is not None and (
            not _is_integer(recording["duration_ms"]) or recording["duration_ms"] < 0
        ):
            raise ValueError("Invalid duration_ms")
        if not isinstance(recording["sources"], list):
            raise ValueError("Recording sources must be an array")
        if not recording["sources"]:
            raise ValueError("Published recording has no public source")
        seen_source_ids: set[str] = set()
        for source in recording["sources"]:
            if not isinstance(source, dict) or set(source) != source_keys:
                raise ValueError("Invalid source object")
            source_id = _require_identifier(source["source_id"], "source_id")
            if source_id in seen_source_ids:
                raise ValueError("Duplicate source within recording")
            seen_source_ids.add(source_id)
            _require_nonempty_string(source["platform"], "source platform")
            _require_nonempty_string(source["native_id"], "source native_id")
            source_url = _require_nonempty_string(source["url"], "source URL")
            parsed_url = urlsplit(source_url)
            if (
                parsed_url.scheme not in {"http", "https"}
                or not parsed_url.hostname
                or parsed_url.username is not None
                or parsed_url.password is not None
            ):
                raise ValueError("Public source URL must be an uncredentialed HTTP(S) URL")
            if source["access_state"] != "public":
                raise ValueError("Public release contains a non-public source")
        source_count += len(recording["sources"])
        if not isinstance(recording["transcript_revisions"], list):
            raise ValueError("Transcript revisions must be an array")
        for revision in recording["transcript_revisions"]:
            if not isinstance(revision, dict) or set(revision) != revision_keys:
                raise ValueError("Invalid transcript revision")
            revision_id = _require_identifier(revision["revision_id"], "revision_id")
            if revision_id in seen_revision_ids:
                raise ValueError("Duplicate transcript revision_id")
            seen_revision_ids.add(revision_id)
            if revision["revision_kind"] not in _TRANSCRIPT_REVISION_KINDS:
                raise ValueError("Invalid transcript revision_kind")
            _require_nonempty_string(revision["language"], "transcript language")
            if revision["review_state"] not in _TRANSCRIPT_REVIEW_STATES:
                raise ValueError("Unsafe transcript review_state entered release")
            if type(revision["machine_generated"]) is not bool:
                raise ValueError("machine_generated must be a boolean")
            expected_machine = (
                revision["review_state"] == "machine"
                or revision["revision_kind"] in {"raw_asr", "contextual_asr"}
            )
            if revision["machine_generated"] is not expected_machine:
                raise ValueError("machine_generated differs from revision provenance")
            if type(revision["unreviewed"]) is not bool:
                raise ValueError("unreviewed must be a boolean")
            if revision["unreviewed"] is not (revision["review_state"] == "machine"):
                raise ValueError("unreviewed differs from transcript review_state")
            if revision["verified_quotation"] is not False:
                raise ValueError("A public transcript is not a verified quotation")
            if revision["disclaimer_code"] not in _TRANSCRIPT_DISCLAIMER_CODES:
                raise ValueError("Invalid transcript disclaimer_code")
            if revision["lifecycle_state"] not in _TRANSCRIPT_LIFECYCLE_STATES:
                raise ValueError("Invalid transcript lifecycle_state")
            history = revision["lifecycle_history"]
            if not isinstance(history, list):
                raise ValueError("Transcript lifecycle_history must be an array")
            prior_decided_at: datetime | None = None
            expected_transitions = {
                "active": {"disputed", "retracted"},
                "disputed": {"retracted", "reinstated"},
                "retracted": {"reinstated"},
                "reinstated": {"disputed", "retracted"},
            }
            prior_lifecycle_state = "active"
            for lifecycle in history:
                if not isinstance(lifecycle, dict) or set(lifecycle) != lifecycle_keys:
                    raise ValueError("Invalid transcript lifecycle entry")
                if lifecycle["state"] not in {
                    "retracted",
                    "disputed",
                    "reinstated",
                }:
                    raise ValueError("Invalid transcript lifecycle history state")
                if lifecycle["reason_code"] not in _TRANSCRIPT_LIFECYCLE_REASON_CODES:
                    raise ValueError("Invalid transcript lifecycle reason_code")
                explanation = _require_nonempty_string(
                    lifecycle["explanation"], "transcript lifecycle explanation"
                )
                if len(explanation) > 2048:
                    raise ValueError("Transcript lifecycle explanation is too long")
                decided_at = _require_nonempty_string(
                    lifecycle["decided_at"], "transcript lifecycle decided_at"
                )
                decided_at_value = _require_utc_timestamp(
                    decided_at, "transcript lifecycle decided_at"
                )
                if prior_decided_at is not None and decided_at_value <= prior_decided_at:
                    raise ValueError("Transcript lifecycle history is not chronological")
                lifecycle_state = lifecycle["state"]
                if lifecycle_state not in expected_transitions[prior_lifecycle_state]:
                    raise ValueError(
                        "Transcript lifecycle history contains an illegal transition"
                    )
                prior_decided_at = decided_at_value
                prior_lifecycle_state = lifecycle_state
            expected_lifecycle = prior_lifecycle_state
            if revision["lifecycle_state"] != expected_lifecycle:
                raise ValueError("Transcript lifecycle state differs from history")
            if revision["lifecycle_state"] == "retracted":
                expected_disclaimer = "retracted_transcript_text_withdrawn_v1"
            elif (
                revision["lifecycle_state"] == "disputed"
                or revision["review_state"] == "disputed"
            ):
                expected_disclaimer = "disputed_transcript_not_verified_quotation_v1"
            elif revision["review_state"] == "machine":
                expected_disclaimer = (
                    "machine_generated_unreviewed_not_verified_quotation_v1"
                )
            else:
                expected_disclaimer = "reviewed_transcript_not_fact_checked_v1"
            if revision["disclaimer_code"] != expected_disclaimer:
                raise ValueError("Transcript disclaimer differs from its state")
            if not isinstance(revision["segments"], list):
                raise ValueError("Transcript segments must be an array")
            if revision["lifecycle_state"] == "retracted" and revision["segments"]:
                raise ValueError("Retracted transcript tombstone contains text")
            prior_start_ms = -1
            for segment in revision["segments"]:
                if not isinstance(segment, dict) or set(segment) != segment_keys:
                    raise ValueError("Invalid transcript segment")
                segment_id = _require_identifier(segment["segment_id"], "segment_id")
                if segment_id in seen_segment_ids:
                    raise ValueError("Duplicate transcript segment_id")
                seen_segment_ids.add(segment_id)
                if (
                    not _is_integer(segment["start_ms"])
                    or not _is_integer(segment["end_ms"])
                    or segment["start_ms"] < 0
                    or segment["end_ms"] <= segment["start_ms"]
                ):
                    raise ValueError("Transcript segment has invalid interval")
                if segment["start_ms"] < prior_start_ms:
                    raise ValueError("Transcript segments are not ordered by start time")
                prior_start_ms = segment["start_ms"]
                if not isinstance(segment["text"], str):
                    raise ValueError("Transcript segment text must be a string")
                if segment["speaker_label"] is not None and not isinstance(
                    segment["speaker_label"], str
                ):
                    raise ValueError("Invalid transcript speaker_label")
                if segment["confidence_band"] not in _CONFIDENCE_BANDS:
                    raise ValueError("Invalid transcript confidence_band")
                probability = segment["calibrated_probability"]
                if probability is not None and (
                    isinstance(probability, bool)
                    or not isinstance(probability, (int, float))
                    or not (0 <= probability <= 1)
                ):
                    raise ValueError("Invalid calibrated probability")
            segment_count += len(revision["segments"])
        revision_count += len(recording["transcript_revisions"])
    if counts["sources"] != source_count:
        raise ValueError("Source count mismatch")
    if counts["transcript_revisions"] != revision_count:
        raise ValueError("Transcript revision count mismatch")
    if counts["segments"] != segment_count:
        raise ValueError("Segment count mismatch")
    expected_digest = hashlib.sha256(
        canonical_json(_release_payload(release)).encode("utf-8")
    ).hexdigest()
    expected_release_id = f"release_{expected_digest[:24]}"
    if release["release_id"] != expected_release_id:
        raise ValueError(
            f"Release identity mismatch: expected {expected_release_id}, "
            f"received {release['release_id']}"
        )
