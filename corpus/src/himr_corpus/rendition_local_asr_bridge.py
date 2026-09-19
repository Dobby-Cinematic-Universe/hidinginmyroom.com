"""Fail-closed admission and search for rendition-local whisper.cpp transcripts.

The ordinary transcript tables use recording coordinates.  A normalized analysis
window has its own zero-based media timeline, so this bridge admits the machine text
to a private, separate lane and preserves only an uncalibrated arithmetic source-time
view.  It never writes a recording timestamp or timeline span.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from . import __version__
from .asr_result_importer import (
    MAX_RESULT_BYTES,
    _insert_exact_artifact,
    _insert_exact_processing_run,
    _insert_exact_run_input,
    _local_file_uri,
    _require_catalog_dependencies,
    _stable_read,
    _upsert_job,
    _validate_artifact_contents,
    validate_asr_whispercpp_result,
)
from .db import transaction
from .ids import stable_id
from .importers import canonical_json, sha256_bytes
from .result_importers import ResultImportError


SCHEMA_VERSION = 1
BRIDGE_VERSION = "rendition-local-asr-bridge/1"
IMPORTER_NAME = "rendition_local_asr_result_v1"
MAX_LOCAL_WINDOW_RESULT_BYTES = 16 * 1024 * 1024
MAX_ARTIFACT_SOURCE_DURATION_DELTA_MS = 30_000


def _strict_json(body: bytes, label: str) -> dict[str, Any]:
    def reject_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ResultImportError(f"{label} contains duplicate key {key!r}")
            value[key] = item
        return value

    def reject_constant(value: str) -> None:
        raise ResultImportError(f"{label} contains non-finite number {value}")

    try:
        parsed = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=reject_pairs,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResultImportError(f"{label} is not valid UTF-8 JSON: {error}") from error
    if not isinstance(parsed, dict):
        raise ResultImportError(f"{label} must contain a JSON object")
    return parsed


def _catalog_object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, str):
        raise ResultImportError(f"{label} must contain catalog JSON")
    parsed = _strict_json(value.encode("utf-8"), label)
    if canonical_json(parsed) != value:
        raise ResultImportError(f"{label} must use canonical JSON encoding")
    return parsed


def _read_validated_result(path_value: str | Path) -> tuple[dict[str, Any], bytes, str, str]:
    path = Path(path_value)
    if not path.is_absolute():
        path = path.resolve()
    body = _stable_read(path, "rendition-local ASR result", maximum_bytes=MAX_RESULT_BYTES)
    raw = _strict_json(body, "rendition-local ASR result")
    result = validate_asr_whispercpp_result(raw, result_file_path=path.resolve())
    _validate_artifact_contents(result)
    raw_sha = sha256_bytes(body)
    canonical_sha = sha256_bytes(canonical_json(raw).encode("utf-8"))
    return result, body, raw_sha, canonical_sha


def _require_exact_local_lineage(
    connection: sqlite3.Connection,
    result: dict[str, Any],
) -> dict[str, Any]:
    context = result["catalog_context"]
    if context is None or context["rendition_id"] is None:
        raise ResultImportError(
            "rendition-local ASR requires exact recording and rendition context"
        )
    if result["window"]["offset_ms"] != 0:
        raise ResultImportError(
            "rendition-local ASR must begin at artifact-local zero"
        )
    if result["window"]["duration_ms"] != result["input"]["probe"]["duration_ms"]:
        raise ResultImportError(
            "rendition-local ASR must cover the complete normalized audio artifact"
        )

    # This validates exact input/model/glossary registries and requires that the
    # context really is a local-window rendition, while leaving the ordinary
    # recording-coordinate importer fail-closed.
    _require_catalog_dependencies(
        connection, result, allow_rendition_local=True
    )

    artifact = connection.execute(
        """
        SELECT artifact_id, processing_run_id, artifact_kind, storage_uri,
               sha256, byte_count, visibility, metadata_json
        FROM artifacts WHERE artifact_id = ?
        """,
        (result["input"]["artifact_id"],),
    ).fetchone()
    if artifact is None:  # guarded above; retained for type/narrowing clarity.
        raise ResultImportError("rendition-local input artifact is absent")
    if artifact["artifact_kind"] != "window_audio_16khz_mono_flac":
        raise ResultImportError("rendition-local input is not the normalized window audio")
    artifact_metadata = _catalog_object(
        artifact["metadata_json"], "rendition-local input artifact metadata"
    )

    rendition = connection.execute(
        """
        SELECT rendition_id, recording_id, media_id, rendition_kind,
               review_state, metadata_json
        FROM renditions WHERE rendition_id = ?
        """,
        (context["rendition_id"],),
    ).fetchone()
    if rendition is None:
        raise ResultImportError("rendition-local context rendition is absent")
    rendition_metadata = _catalog_object(
        rendition["metadata_json"], "rendition-local rendition metadata"
    )
    expected_kind_prefix = "local_window:window_audio_16khz_mono_flac:"
    if not rendition["rendition_kind"].startswith(expected_kind_prefix):
        raise ResultImportError("rendition-local context has the wrong rendition kind")
    if rendition["review_state"] not in {"unreviewed", "reviewed"}:
        raise ResultImportError("rendition-local context rendition is disputed or rejected")

    input_duration = result["input"]["probe"]["duration_ms"]
    spans = connection.execute(
        """
        SELECT timeline_map_span_id, ordinal, media_start_ms, media_end_ms,
               recording_start_ms, recording_end_ms, mapping_kind, confidence_state
        FROM timeline_map_spans WHERE rendition_id = ? ORDER BY ordinal
        """,
        (context["rendition_id"],),
    ).fetchall()
    if len(spans) != 1:
        raise ResultImportError(
            "rendition-local context must have exactly one artifact-local timeline span"
        )
    span = spans[0]
    if dict(span) != {
        "timeline_map_span_id": span["timeline_map_span_id"],
        "ordinal": 0,
        "media_start_ms": 0,
        "media_end_ms": input_duration,
        "recording_start_ms": None,
        "recording_end_ms": None,
        "mapping_kind": "unknown",
        "confidence_state": "metadata_only",
    }:
        raise ResultImportError(
            "rendition-local bridge requires an unresolved full-artifact timeline; "
            "it will not replace or reinterpret catalog coordinates"
        )

    parent_run = connection.execute(
        """
        SELECT processing_run_id, stage, implementation_version, parameters_json,
               environment_json, started_at, completed_at, status, error_text
        FROM processing_runs WHERE processing_run_id = ?
        """,
        (result["input"]["parent_processing_run_id"],),
    ).fetchone()
    if parent_run is None:
        raise ResultImportError("rendition-local parent admission run is absent")
    if (
        parent_run["stage"] != "local_window_result_admission"
        or parent_run["implementation_version"] != "local-window-catalog-bridge/1"
        or parent_run["status"] != "completed"
        or parent_run["completed_at"] is None
        or parent_run["error_text"] is not None
    ):
        raise ResultImportError("rendition-local parent run is not an exact completed admission")
    parent_parameters = _catalog_object(
        parent_run["parameters_json"], "rendition-local parent parameters"
    )
    parent_environment = _catalog_object(
        parent_run["environment_json"], "rendition-local parent environment"
    )
    if (
        parent_parameters.get("run_semantics")
        != "catalog_admission_verification_not_extraction_execution"
        or parent_environment.get("publication_authority") != "none"
        or parent_environment.get("identity_claims_allowed") is not False
        or parent_environment.get("network_access_performed") is not False
        or parent_environment.get("credentials_used") is not False
    ):
        raise ResultImportError("rendition-local parent admission safety fields are invalid")

    common_keys = {
        "local_window_result_sha256",
        "local_window_result_uri",
        "source_time_mapping",
        "source_media_id",
    }
    for key in common_keys:
        if artifact_metadata.get(key) != rendition_metadata.get(key):
            raise ResultImportError(
                f"rendition-local artifact/rendition lineage disagrees on {key}"
            )
    if (
        parent_parameters.get("local_window_result_sha256")
        != artifact_metadata.get("local_window_result_sha256")
        or parent_parameters.get("local_window_result_uri")
        != artifact_metadata.get("local_window_result_uri")
        or parent_parameters.get("time_mapping")
        != artifact_metadata.get("source_time_mapping")
    ):
        raise ResultImportError("rendition-local parent run disagrees with artifact lineage")

    mapping = artifact_metadata.get("source_time_mapping")
    if not isinstance(mapping, dict) or mapping != {
        "artifact_zero_maps_to_source_ms": mapping.get("artifact_zero_maps_to_source_ms"),
        "boundary": "half_open",
        "byte_exact_source_fragment": False,
        "coordinate_precision": "integer_millisecond_contract",
        "extraction_method": "ffmpeg_accurate_seek_transcode",
        "source_end_ms": mapping.get("source_end_ms"),
        "source_start_ms": mapping.get("source_start_ms"),
    }:
        raise ResultImportError("rendition-local source-time mapping is unsupported")
    source_start = mapping["source_start_ms"]
    source_end = mapping["source_end_ms"]
    if (
        isinstance(source_start, bool)
        or not isinstance(source_start, int)
        or source_start < 0
        or isinstance(source_end, bool)
        or not isinstance(source_end, int)
        or source_end <= source_start
        or mapping["artifact_zero_maps_to_source_ms"] != source_start
    ):
        raise ResultImportError("rendition-local source-time mapping bounds are invalid")
    duration_delta = input_duration - (source_end - source_start)
    if abs(duration_delta) > MAX_ARTIFACT_SOURCE_DURATION_DELTA_MS:
        raise ResultImportError(
            "rendition-local artifact/source duration disagreement exceeds the "
            f"{MAX_ARTIFACT_SOURCE_DURATION_DELTA_MS}-ms bridge bound"
        )

    local_result_sha = artifact_metadata.get("local_window_result_sha256")
    local_result_uri = artifact_metadata.get("local_window_result_uri")
    if not isinstance(local_result_sha, str) or len(local_result_sha) != 64:
        raise ResultImportError("rendition-local result SHA-256 is invalid")
    local_result_path = _local_file_uri(
        local_result_uri, "rendition-local local-window result URI"
    )
    local_result_body = _stable_read(
        local_result_path,
        "rendition-local local-window result",
        maximum_bytes=MAX_LOCAL_WINDOW_RESULT_BYTES,
    )
    if sha256_bytes(local_result_body) != local_result_sha:
        raise ResultImportError(
            "rendition-local local-window result bytes differ from catalog lineage"
        )
    local_result = _strict_json(local_result_body, "rendition-local local-window result")
    if local_result.get("time_mapping") != mapping:
        raise ResultImportError(
            "rendition-local local-window result mapping differs from catalog lineage"
        )
    local_audio = [
        row
        for row in local_result.get("artifacts", [])
        if isinstance(row, dict)
        and row.get("artifact_kind") == "window_audio_16khz_mono_flac"
    ]
    if len(local_audio) != 1:
        raise ResultImportError("rendition-local local-window result has no unique audio artifact")
    expected_audio = {
        "path": result["input"]["path"],
        "sha256": result["input"]["sha256"],
        "byte_count": result["input"]["byte_count"],
    }
    if any(local_audio[0].get(key) != value for key, value in expected_audio.items()):
        raise ResultImportError(
            "rendition-local ASR input differs from the pinned local-window audio"
        )

    import_batch = connection.execute(
        """
        SELECT import_batch_id, importer_name, importer_version, input_sha256,
               started_at, completed_at, status, statistics_json
        FROM import_batches
        WHERE importer_name = 'local_window_result_admission_v1'
          AND input_sha256 = ?
        """,
        (local_result_sha,),
    ).fetchall()
    if len(import_batch) != 1:
        raise ResultImportError(
            "rendition-local result lacks one exact local-window admission receipt"
        )
    local_batch = import_batch[0]
    if (
        local_batch["importer_version"] != "local-window-catalog-bridge/1"
        or local_batch["status"] != "completed"
        or local_batch["started_at"] != parent_run["started_at"]
        or local_batch["completed_at"] != parent_run["completed_at"]
    ):
        raise ResultImportError("rendition-local local-window admission receipt is inconsistent")

    source_media_id = artifact_metadata.get("source_media_id")
    if not isinstance(source_media_id, str):
        raise ResultImportError("rendition-local source media identity is absent")
    parent_rendition_id = rendition_metadata.get("parent_rendition_id")
    parent_rendition = connection.execute(
        """
        SELECT rendition_id, recording_id, media_id, rendition_kind, review_state
        FROM renditions WHERE rendition_id = ?
        """,
        (parent_rendition_id,),
    ).fetchone()
    if (
        parent_rendition is None
        or parent_rendition["recording_id"] != context["recording_id"]
        or parent_rendition["media_id"] != source_media_id
        or parent_rendition["rendition_kind"] != "acquired_source_media"
        or parent_rendition["review_state"] not in {"unreviewed", "reviewed"}
    ):
        raise ResultImportError("rendition-local acquired parent rendition is inconsistent")
    source_media = connection.execute(
        """
        SELECT media_id, duration_ms, integrity_state, first_cataloged_at
        FROM media_objects WHERE media_id = ?
        """,
        (source_media_id,),
    ).fetchone()
    if (
        source_media is None
        or source_media["duration_ms"] is None
        or source_media["duration_ms"] <= 0
        or source_media["integrity_state"] != "verified"
    ):
        raise ResultImportError("rendition-local acquired source duration is unavailable")
    if source_end > source_media["duration_ms"]:
        raise ResultImportError("rendition-local mapping exceeds acquired source media")

    source_rows = connection.execute(
        """
        SELECT DISTINCT mapping.source_id
        FROM recording_sources AS mapping
        JOIN media_sources AS media_link ON media_link.source_id = mapping.source_id
        WHERE mapping.recording_id = ?
          AND media_link.media_id = ?
          AND mapping.confidence_state IN ('metadata_only', 'reviewed')
          AND mapping.mapping_role IN (
              'archive_original_file', 'complete_source', 'current_platform_listing',
              'legacy_catalog_mapping', 'validated_platform_listing'
          )
        ORDER BY mapping.source_id
        """,
        (context["recording_id"], source_media_id),
    ).fetchall()
    if len(source_rows) != 1:
        raise ResultImportError(
            "rendition-local acquired media does not resolve to one eligible source"
        )
    source_id = source_rows[0]["source_id"]

    recording = connection.execute(
        """
        SELECT recording_id, duration_ms, review_state, merged_into_recording_id
        FROM recordings WHERE recording_id = ?
        """,
        (context["recording_id"],),
    ).fetchone()
    if (
        recording is None
        or recording["duration_ms"] is None
        or recording["duration_ms"] <= 0
        or recording["review_state"] not in {"metadata_only", "unreviewed", "reviewed"}
        or recording["merged_into_recording_id"] is not None
    ):
        raise ResultImportError("rendition-local recording duration/context is unavailable")

    return {
        "artifact_metadata": artifact_metadata,
        "rendition_metadata": rendition_metadata,
        "timeline_span_id": span["timeline_map_span_id"],
        "source_time_mapping": mapping,
        "source_id": source_id,
        "source_media_id": source_media_id,
        "source_rendition_id": parent_rendition_id,
        "source_media_duration_ms": source_media["duration_ms"],
        "recording_declared_duration_ms": recording["duration_ms"],
        "transform_candidate_timestamp": source_media["first_cataloged_at"],
        "local_window_result_uri": local_result_uri,
        "local_window_result_sha256": local_result_sha,
        "local_window_import_batch_id": local_batch["import_batch_id"],
        "artifact_source_duration_delta_ms": duration_delta,
    }


def _transform_candidate_rows(
    *,
    context: dict[str, Any],
    lineage: dict[str, Any],
    created_at: str,
) -> list[dict[str, Any]]:
    source_duration = lineage["source_media_duration_ms"]
    recording_duration = lineage["recording_declared_duration_ms"]
    candidates = [
        (
            "identity_from_acquired_media_duration_hypothesis",
            source_duration,
            1,
            1,
        )
    ]
    if recording_duration != source_duration:
        candidates.append(
            (
                "linear_scale_to_declared_recording_duration_hypothesis",
                recording_duration,
                recording_duration,
                source_duration,
            )
        )
    rows: list[dict[str, Any]] = []
    for kind, proposed_end, numerator, denominator in candidates:
        candidate_id = stable_id(
            "srtc",
            SCHEMA_VERSION,
            context["recording_id"],
            lineage["source_id"],
            lineage["source_media_id"],
            kind,
        )
        review_task_id = stable_id(
            "rtk", "source_recording_transform_candidate", candidate_id
        )
        rows.append(
            {
                "transform_candidate_id": candidate_id,
                "recording_id": context["recording_id"],
                "source_id": lineage["source_id"],
                "source_media_id": lineage["source_media_id"],
                "source_rendition_id": lineage["source_rendition_id"],
                "candidate_kind": kind,
                "source_start_ms": 0,
                "source_end_ms": source_duration,
                "proposed_recording_start_ms": 0,
                "proposed_recording_end_ms": proposed_end,
                "scale_numerator": numerator,
                "scale_denominator": denominator,
                "declared_recording_duration_ms": recording_duration,
                "acquired_media_duration_ms": source_duration,
                "duration_delta_ms": source_duration - recording_duration,
                "evidence_state": "hypothesis_only_unreviewed",
                "timeline_application_allowed": 0,
                "relationship_asserted": 0,
                "review_task_id": review_task_id,
                "created_at": created_at,
                "metadata_json": canonical_json(
                    {
                        "boundary": "half_open",
                        "duration_conflict_preserved": (
                            source_duration != recording_duration
                        ),
                        "hypothesis_is_not_a_timeline_assertion": True,
                        "requires_direct_media_boundary_review": True,
                        "review_task_timestamp_basis":
                            "source_media_first_cataloged_at",
                    }
                ),
            }
        )
    return rows


def _build_plan(connection: sqlite3.Connection, result_path: str | Path) -> dict[str, Any]:
    result, result_body, raw_sha, canonical_sha = _read_validated_result(result_path)
    lineage = _require_exact_local_lineage(connection, result)
    context = result["catalog_context"]
    assert context is not None and context["rendition_id"] is not None
    producer = result["catalog_records"]
    producer_revision = producer["transcript_revisions"][0]
    local_revision_id = stable_id(
        "ltr", SCHEMA_VERSION, producer_revision["revision_id"], raw_sha
    )
    mapping = lineage["source_time_mapping"]
    source_zero = mapping["artifact_zero_maps_to_source_ms"]

    segment_rows: list[dict[str, Any]] = []
    producer_to_local_segment: dict[str, str] = {}
    max_segment_end = 0
    max_segment_source_end = source_zero
    for segment in producer["transcript_segments"]:
        local_segment_id = stable_id(
            "lts", local_revision_id, segment["segment_id"]
        )
        producer_to_local_segment[segment["segment_id"]] = local_segment_id
        source_start = source_zero + segment["start_ms"]
        source_end = source_zero + segment["end_ms"]
        max_segment_end = max(max_segment_end, segment["end_ms"])
        max_segment_source_end = max(max_segment_source_end, source_end)
        source_overrun = max(0, source_end - mapping["source_end_ms"])
        metadata = json.loads(segment["metadata_json"])
        metadata["coordinate_provenance"] = {
            "coordinate_system": "rendition_media_ms",
            "recording_transform_state": "unresolved",
            "source_coordinate_method": "add_unreviewed_integer_source_offset",
            "source_boundary_overrun_ms": source_overrun,
        }
        segment_rows.append(
            {
                "local_segment_id": local_segment_id,
                "local_revision_id": local_revision_id,
                "producer_segment_id": segment["segment_id"],
                "ordinal": segment["ordinal"],
                "start_ms": segment["start_ms"],
                "end_ms": segment["end_ms"],
                "source_start_ms": source_start,
                "source_end_ms": source_end,
                "source_boundary_overrun_ms": source_overrun,
                "text": segment["text"],
                "normalized_text": segment["normalized_text"],
                "speaker_label": segment["speaker_label"],
                "language": segment["language"],
                "confidence_band": segment["confidence_band"],
                "calibrated_probability": segment["calibrated_probability"],
                "metadata_json": canonical_json(metadata),
            }
        )

    word_rows: list[dict[str, Any]] = []
    for word in producer["transcript_words"]:
        local_segment_id = producer_to_local_segment[word["segment_id"]]
        if word["start_ms"] is None:
            source_start = None
            source_end = None
        else:
            source_start = source_zero + word["start_ms"]
            source_end = source_zero + word["end_ms"]
        word_rows.append(
            {
                "local_word_id": stable_id(
                    "ltw", local_segment_id, word["word_id"]
                ),
                "local_segment_id": local_segment_id,
                "producer_word_id": word["word_id"],
                "ordinal": word["ordinal"],
                "start_ms": word["start_ms"],
                "end_ms": word["end_ms"],
                "source_start_ms": source_start,
                "source_end_ms": source_end,
                "token": word["token"],
                "normalized_token": word["normalized_token"],
                "asr_log_probability": word["asr_log_probability"],
                "alignment_score": word["alignment_score"],
                "calibrated_probability": word["calibrated_probability"],
            }
        )

    input_duration = result["input"]["probe"]["duration_ms"]
    requested_end = result["window"]["end_ms"]
    input_overrun = max(0, max_segment_end - requested_end)
    source_overrun = max(0, max_segment_source_end - mapping["source_end_ms"])
    revision_metadata = json.loads(producer_revision["metadata_json"])
    revision_metadata.update(
        {
            "asr_result_canonical_sha256": canonical_sha,
            "asr_result_raw_sha256": raw_sha,
            "coordinate_system": "rendition_media_ms",
            "local_window_result_sha256": lineage["local_window_result_sha256"],
            "recording_coordinates_asserted": False,
            "recording_transform_state": "unresolved",
            "source_time_mapping": mapping,
        }
    )
    revision_row = {
        "local_revision_id": local_revision_id,
        "producer_revision_id": producer_revision["revision_id"],
        "recording_id": context["recording_id"],
        "rendition_id": context["rendition_id"],
        "processing_run_id": result["processing_run"]["processing_run_id"],
        "revision_kind": producer_revision["revision_kind"],
        "origin": producer_revision["origin"],
        "language": producer_revision["language"],
        "glossary_revision_id": producer_revision["glossary_revision_id"],
        "review_state": "machine",
        "coordinate_system": "rendition_media_ms",
        "boundary": "half_open",
        "input_duration_ms": input_duration,
        "requested_start_ms": 0,
        "requested_end_ms": requested_end,
        "max_segment_end_ms": max_segment_end,
        "input_boundary_overrun_ms": input_overrun,
        "source_mapping_state": "integer_contract_not_boundary_calibrated",
        "recording_transform_state": "unresolved",
        "created_at": result["processing_run"]["completed_at"],
        "metadata_json": canonical_json(revision_metadata),
    }
    candidates = _transform_candidate_rows(
        context=context,
        lineage=lineage,
        created_at=lineage["transform_candidate_timestamp"],
    )

    import_batch_id = stable_id("imp", IMPORTER_NAME, canonical_sha)
    local_import_id = stable_id("rlai", SCHEMA_VERSION, raw_sha, local_revision_id)
    statistics = {
        "processing_runs": 1,
        "run_inputs": 1,
        "artifacts": 2,
        "rendition_local_revisions": 1,
        "rendition_local_segments": len(segment_rows),
        "rendition_local_words": len(word_rows),
        "rendition_local_import_receipts": 1,
        "transform_candidates": len(candidates),
        "review_tasks": len(candidates),
        "recording_scoped_transcript_revisions": 0,
        "timeline_map_spans": 0,
        "publication_decisions": 0,
    }
    public_core = {
        "schema_version": SCHEMA_VERSION,
        "bridge_version": BRIDGE_VERSION,
        "result": {
            "asr_result_uri": Path(result["result_path"]).resolve().as_uri(),
            "raw_sha256": raw_sha,
            "canonical_sha256": canonical_sha,
            "byte_count": len(result_body),
            "result_key": result["result_key"],
            "processing_run_id": result["processing_run"]["processing_run_id"],
            "input_artifact_id": result["input"]["artifact_id"],
            "local_window_result_uri": lineage["local_window_result_uri"],
            "local_window_result_sha256": lineage["local_window_result_sha256"],
            "local_revision_id": local_revision_id,
        },
        "catalog_context": {
            "recording_id": context["recording_id"],
            "rendition_id": context["rendition_id"],
            "source_id": lineage["source_id"],
            "source_media_id": lineage["source_media_id"],
            "source_rendition_id": lineage["source_rendition_id"],
            "recording_declared_duration_ms": lineage[
                "recording_declared_duration_ms"
            ],
            "source_media_duration_ms": lineage["source_media_duration_ms"],
            "duration_delta_ms": (
                lineage["source_media_duration_ms"]
                - lineage["recording_declared_duration_ms"]
            ),
        },
        "coordinate_contract": {
            "coordinate_system": "rendition_media_ms",
            "boundary": "half_open",
            "requested_start_ms": 0,
            "requested_end_ms": requested_end,
            "input_duration_ms": input_duration,
            "max_segment_end_ms": max_segment_end,
            "input_boundary_overrun_ms": input_overrun,
            "source_time_mapping": mapping,
            "artifact_source_duration_delta_ms": lineage[
                "artifact_source_duration_delta_ms"
            ],
            "max_segment_source_end_ms": max_segment_source_end,
            "source_boundary_overrun_ms": source_overrun,
            "source_mapping_state": "integer_contract_not_boundary_calibrated",
            "recording_transform_state": "unresolved",
            "recording_coordinates_asserted": False,
        },
        "transform_candidates": [
            {
                "transform_candidate_id": row["transform_candidate_id"],
                "review_task_id": row["review_task_id"],
                "candidate_kind": row["candidate_kind"],
                "source_start_ms": row["source_start_ms"],
                "source_end_ms": row["source_end_ms"],
                "proposed_recording_start_ms": row[
                    "proposed_recording_start_ms"
                ],
                "proposed_recording_end_ms": row["proposed_recording_end_ms"],
                "scale_numerator": row["scale_numerator"],
                "scale_denominator": row["scale_denominator"],
                "declared_recording_duration_ms": row[
                    "declared_recording_duration_ms"
                ],
                "acquired_media_duration_ms": row["acquired_media_duration_ms"],
                "duration_delta_ms": row["duration_delta_ms"],
                "evidence_state": row["evidence_state"],
                "timeline_application_allowed": False,
                "relationship_asserted": False,
                "created_at": row["created_at"],
                "timestamp_basis": "source_media_first_cataloged_at",
                "human_review_required": True,
            }
            for row in candidates
        ],
        "statistics": statistics,
        "safety": {
            "visibility": "private",
            "publication_authority": "none",
            "identity_authority": "none",
            "network_access_performed": False,
            "credentials_used": False,
            "source_boundary_calibrated": False,
            "transform_applied": False,
            "transcript_text_in_plan": False,
        },
    }
    plan_sha = sha256_bytes(canonical_json(public_core).encode("utf-8"))
    public = {
        "schema_version": SCHEMA_VERSION,
        "status": "validated",
        "bridge_version": BRIDGE_VERSION,
        "plan_sha256": plan_sha,
        **{key: value for key, value in public_core.items() if key not in {
            "schema_version", "bridge_version"
        }},
    }
    receipt_row = {
        "local_asr_import_id": local_import_id,
        "import_batch_id": import_batch_id,
        "local_revision_id": local_revision_id,
        "asr_result_uri": public["result"]["asr_result_uri"],
        "asr_result_raw_sha256": raw_sha,
        "asr_result_canonical_sha256": canonical_sha,
        "asr_result_byte_count": len(result_body),
        "local_window_result_uri": lineage["local_window_result_uri"],
        "local_window_result_sha256": lineage["local_window_result_sha256"],
        "input_artifact_id": result["input"]["artifact_id"],
        "source_id": lineage["source_id"],
        "source_media_id": lineage["source_media_id"],
        "source_start_ms": mapping["source_start_ms"],
        "source_end_ms": mapping["source_end_ms"],
        "artifact_duration_ms": input_duration,
        "artifact_source_duration_delta_ms": lineage[
            "artifact_source_duration_delta_ms"
        ],
        "max_segment_source_end_ms": max_segment_source_end,
        "source_boundary_overrun_ms": source_overrun,
        "plan_sha256": plan_sha,
        "imported_at": result["processing_run"]["completed_at"],
        "metadata_json": canonical_json(
            {
                "local_window_import_batch_id": lineage[
                    "local_window_import_batch_id"
                ],
                "timeline_span_id": lineage["timeline_span_id"],
                "transform_applied": False,
            }
        ),
    }
    return {
        "public": public,
        "result": result,
        "revision": revision_row,
        "segments": segment_rows,
        "words": word_rows,
        "transform_candidates": candidates,
        "receipt": receipt_row,
        "import_batch_id": import_batch_id,
    }


def build_rendition_local_asr_admission_plan(
    connection: sqlite3.Connection, result_path: str | Path
) -> dict[str, Any]:
    """Validate exact bytes and catalog lineage, returning a text-free plan."""

    return _build_plan(connection, result_path)["public"]


def _insert_or_match(
    connection: sqlite3.Connection,
    *,
    table: str,
    key_column: str,
    row: dict[str, Any],
) -> None:
    key = row[key_column]
    columns = tuple(column for column in row if column != key_column)
    existing = connection.execute(
        f"SELECT {', '.join(columns)} FROM {table} WHERE {key_column} = ?",
        (key,),
    ).fetchone()
    if existing is not None:
        if any(existing[column] != row[column] for column in columns):
            raise ResultImportError(f"{table} {key_column} collision")
        return
    connection.execute(
        f"INSERT INTO {table}({key_column}, {', '.join(columns)}) "
        f"VALUES({', '.join('?' for _ in row)})",
        (key, *(row[column] for column in columns)),
    )


def _insert_import_batch(
    connection: sqlite3.Connection,
    plan: dict[str, Any],
) -> None:
    result = plan["result"]
    public = plan["public"]
    run = result["processing_run"]
    row = {
        "import_batch_id": plan["import_batch_id"],
        "importer_name": IMPORTER_NAME,
        "importer_version": BRIDGE_VERSION,
        "input_sha256": public["result"]["canonical_sha256"],
        "source_snapshot_date": None,
        "started_at": run["started_at"],
        "completed_at": run["completed_at"],
        "status": "completed",
        "statistics_json": canonical_json(public["statistics"]),
    }
    _insert_or_match(
        connection,
        table="import_batches",
        key_column="import_batch_id",
        row=row,
    )


def _insert_review_task(
    connection: sqlite3.Connection,
    candidate: dict[str, Any],
) -> None:
    row = {
        "review_task_id": candidate["review_task_id"],
        "task_kind": "source_recording_transform_review",
        "target_type": "source_recording_transform_candidate",
        "target_id": candidate["transform_candidate_id"],
        "reason": (
            "Review source-to-recording boundary/rate hypothesis against the complete "
            "acquired media; do not copy candidate coordinates into timeline_map_spans."
        ),
        "priority": 80,
        "status": "open",
        "created_at": candidate["created_at"],
        "updated_at": candidate["created_at"],
    }
    _insert_or_match(
        connection, table="review_tasks", key_column="review_task_id", row=row
    )


def import_rendition_local_asr_result(
    connection: sqlite3.Connection,
    result_path: str | Path,
    *,
    expected_plan_sha256: str,
) -> dict[str, Any]:
    """Revalidate and atomically admit one result to the private local lane."""

    preflight = _build_plan(connection, result_path)
    if preflight["public"]["plan_sha256"] != expected_plan_sha256:
        raise ResultImportError(
            "rendition-local ASR plan changed or was not the separately reviewed plan"
        )
    with transaction(connection):
        plan = _build_plan(connection, result_path)
        if plan["public"]["plan_sha256"] != expected_plan_sha256:
            raise ResultImportError(
                "rendition-local ASR plan changed inside the admission transaction"
            )
        _insert_import_batch(connection, plan)
        result = plan["result"]
        _insert_exact_processing_run(connection, result["processing_run"])
        _insert_exact_run_input(connection, result["run_input"])
        for artifact in result["artifacts"]:
            _insert_exact_artifact(connection, artifact)
        for candidate in plan["transform_candidates"]:
            _insert_review_task(connection, candidate)
            _insert_or_match(
                connection,
                table="source_recording_transform_candidates",
                key_column="transform_candidate_id",
                row=candidate,
            )
        _insert_or_match(
            connection,
            table="rendition_local_transcript_revisions",
            key_column="local_revision_id",
            row=plan["revision"],
        )
        for segment in plan["segments"]:
            _insert_or_match(
                connection,
                table="rendition_local_transcript_segments",
                key_column="local_segment_id",
                row=segment,
            )
        for word in plan["words"]:
            _insert_or_match(
                connection,
                table="rendition_local_transcript_words",
                key_column="local_word_id",
                row=word,
            )
        _insert_or_match(
            connection,
            table="rendition_local_asr_imports",
            key_column="local_asr_import_id",
            row=plan["receipt"],
        )
        _upsert_job(connection, result)
    return {**plan["public"], "status": "admitted"}


def search_rendition_local_transcripts(
    connection: sqlite3.Connection,
    query: str,
    *,
    limit: int = 25,
    recording_id: str | None = None,
) -> dict[str, Any]:
    """Search private rendition-local segment text without coordinate promotion."""

    if not isinstance(query, str) or not query.strip() or "\x00" in query or len(query) > 1_000:
        raise ResultImportError("rendition-local search query must be a bounded string")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 200:
        raise ResultImportError("rendition-local search limit must be between 1 and 200")
    parameters: list[Any] = [query]
    recording_clause = ""
    if recording_id is not None:
        if not recording_id or len(recording_id) > 256:
            raise ResultImportError("rendition-local recording filter is invalid")
        recording_clause = "AND revision.recording_id = ?"
        parameters.append(recording_id)
    parameters.append(limit)
    try:
        rows = connection.execute(
            f"""
            SELECT segment.local_segment_id, segment.local_revision_id,
                   revision.recording_id, revision.rendition_id,
                   segment.start_ms, segment.end_ms,
                   segment.source_start_ms, segment.source_end_ms,
                   segment.source_boundary_overrun_ms, segment.text,
                   segment.speaker_label, segment.language,
                   bm25(rendition_local_transcript_fts) AS rank
            FROM rendition_local_transcript_fts
            JOIN rendition_local_transcript_segments AS segment
              ON segment.local_segment_id = rendition_local_transcript_fts.local_segment_id
            JOIN rendition_local_transcript_revisions AS revision
              ON revision.local_revision_id = segment.local_revision_id
            WHERE rendition_local_transcript_fts MATCH ?
              {recording_clause}
            ORDER BY rank, segment.local_revision_id, segment.ordinal
            LIMIT ?
            """,
            tuple(parameters),
        ).fetchall()
    except sqlite3.OperationalError as error:
        raise ResultImportError(f"rendition-local full-text query is invalid: {error}") from error
    return {
        "query": query,
        "limit": limit,
        "recording_id": recording_id,
        "coordinate_system": "rendition_media_ms",
        "source_coordinate_state": "integer_contract_not_boundary_calibrated",
        "recording_transform_state": "unresolved",
        "result_count": len(rows),
        "results": [
            {
                **dict(row),
                "recording_start_ms": None,
                "recording_end_ms": None,
            }
            for row in rows
        ],
    }
