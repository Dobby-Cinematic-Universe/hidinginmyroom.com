"""Reviewed exact identity projection from media-local to recording coordinates.

This lane copies complete raw/contextual machine pairs.  Its manifest review is
authority only for the pinned media lineage and integer identity time mapping;
wording, speakers, accuracy, preference, and publication remain unasserted.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import sqlite3
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .db import transaction, utc_now
from .ids import stable_id
from .importers import canonical_json


MANIFEST_SCHEMA_VERSION = 1
PLAN_SCHEMA_VERSION = 1
POLICY_ID = "media_local_full_file_identity_v1"
BRIDGE_VERSION = "media-local-transcript-projection/2"
IMPORTER_NAME = "media_local_transcript_identity_projection_v1"
REVISION_ORIGIN = "media_local_full_file_identity_projection_v1"
TRANSFORM_EXPRESSION = "recording_ms=media_ms"
MAPPING_BASIS = (
    "human_reviewed_timestamp_preserving_normalization_and_direct_public_file_lineage"
)
COORDINATE_ATTESTATION = (
    "reviewed_exact_full_file_identity_no_wording_or_speaker_claim_v1"
)
MAX_RECORDING_DURATION_DELTA_MS = 5
MIN_RECORDINGS = 3
MAX_RECORDINGS = 5
MAX_MANIFEST_BYTES = 512 * 1024
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,299}$")
UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


class MediaLocalTranscriptProjectionError(ValueError):
    """The manifest, source graph, plan, or stored projection is unsafe."""


@dataclass(frozen=True)
class ProjectionManifest:
    path: Path
    raw_json: str
    value: dict[str, Any]
    core: dict[str, Any]
    raw_sha256: str
    canonical_sha256: str
    core_sha256: str
    byte_count: int


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_json(value: object) -> str:
    return _sha256_bytes(canonical_json(value).encode("utf-8"))


def _row(row: sqlite3.Row | None, label: str) -> dict[str, Any]:
    if row is None:
        raise MediaLocalTranscriptProjectionError(f"missing {label}")
    return dict(row)


def _rows(rows: Iterable[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def _exact_keys(value: dict[str, Any], label: str, required: set[str]) -> None:
    missing = required - set(value)
    unknown = set(value) - required
    if missing or unknown:
        details: list[str] = []
        if missing:
            details.append(f"missing {sorted(missing)}")
        if unknown:
            details.append(f"unknown {sorted(unknown)}")
        raise MediaLocalTranscriptProjectionError(
            f"{label} has " + "; ".join(details)
        )


def _object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise MediaLocalTranscriptProjectionError(f"{label} must be an object")
    return value


def _text(value: object, label: str, maximum: int = 4096) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise MediaLocalTranscriptProjectionError(
            f"{label} must be a non-empty string of at most {maximum} characters"
        )
    if value != value.strip() or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise MediaLocalTranscriptProjectionError(
            f"{label} must be trimmed and contain no control characters"
        )
    return value


def _identifier(value: object, label: str) -> str:
    result = _text(value, label, 300)
    if not IDENTIFIER_RE.fullmatch(result):
        raise MediaLocalTranscriptProjectionError(f"{label} is not a safe identifier")
    return result


def _positive_integer(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise MediaLocalTranscriptProjectionError(f"{label} must be a positive integer")
    return value


def _utc(value: object, label: str) -> tuple[str, datetime]:
    if not isinstance(value, str) or not UTC_RE.fullmatch(value):
        raise MediaLocalTranscriptProjectionError(
            f"{label} must be a whole-second UTC timestamp ending in Z"
        )
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        raise MediaLocalTranscriptProjectionError(f"{label} is invalid") from error
    return value, parsed


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise MediaLocalTranscriptProjectionError(
                f"projection manifest contains duplicate JSON key {key!r}"
            )
        result[key] = value
    return result


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


def _stable_read(path: Path) -> bytes:
    try:
        path_before = path.stat()
        handle = path.open("rb")
    except OSError as error:
        raise MediaLocalTranscriptProjectionError(
            f"projection manifest is not a readable current file: {error}"
        ) from error
    try:
        descriptor_before = os.fstat(handle.fileno())
        before = _stat_identity(descriptor_before)
        if not stat.S_ISREG(descriptor_before.st_mode):
            raise MediaLocalTranscriptProjectionError(
                "projection manifest must be a regular file"
            )
        if before != _stat_identity(path_before):
            raise MediaLocalTranscriptProjectionError(
                "projection manifest was replaced while opening"
            )
        if not 0 < descriptor_before.st_size <= MAX_MANIFEST_BYTES:
            raise MediaLocalTranscriptProjectionError(
                f"projection manifest must contain 1 to {MAX_MANIFEST_BYTES} bytes"
            )
        body = handle.read()
        descriptor_after = os.fstat(handle.fileno())
    finally:
        handle.close()
    try:
        path_after = path.stat()
    except OSError as error:
        raise MediaLocalTranscriptProjectionError(
            f"projection manifest disappeared after reading: {error}"
        ) from error
    if (
        before != _stat_identity(descriptor_after)
        or before != _stat_identity(path_after)
        or len(body) != before[2]
    ):
        raise MediaLocalTranscriptProjectionError(
            "projection manifest changed while being verified"
        )
    return body


SELECTION_KEYS = {
    "ordinal",
    "recording_id",
    "canonical_key",
    "source_id",
    "media_source_id",
    "recording_source_id",
    "public_source_metadata_observation_id",
    "public_source_import_observation_id",
    "public_source_observed_at",
    "parent_media_id",
    "parent_rendition_id",
    "normalized_media_id",
    "input_duration_ms",
    "parent_duration_ms",
    "recording_duration_ms",
    "raw_revision_id",
    "raw_import_id",
    "contextual_revision_id",
    "contextual_import_id",
    "contextual_pair_id",
    "contextual_diff_id",
}


def _parse_media_local_transcript_projection_manifest(
    body: bytes, manifest_path: Path
) -> ProjectionManifest:
    """Strictly validate exact UTF-8 manifest bytes without opening a path."""

    if not 0 < len(body) <= MAX_MANIFEST_BYTES:
        raise MediaLocalTranscriptProjectionError(
            f"projection manifest must contain 1 to {MAX_MANIFEST_BYTES} bytes"
        )
    try:
        raw_json = body.decode("utf-8")
        parsed = json.loads(
            raw_json,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda value: (_ for _ in ()).throw(
                MediaLocalTranscriptProjectionError(
                    f"projection manifest contains non-finite number {value}"
                )
            ),
        )
    except MediaLocalTranscriptProjectionError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise MediaLocalTranscriptProjectionError(
            f"cannot decode projection manifest: {error}"
        ) from error
    value = _object(parsed, "manifest")
    _exact_keys(
        value,
        "manifest",
        {"schema_version", "manifest_id", "policy_id", "selection", "review"},
    )
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise MediaLocalTranscriptProjectionError("manifest.schema_version must equal 1")
    _identifier(value["manifest_id"], "manifest.manifest_id")
    if value["policy_id"] != POLICY_ID:
        raise MediaLocalTranscriptProjectionError(
            f"manifest.policy_id must equal {POLICY_ID!r}"
        )
    selection = value["selection"]
    if not isinstance(selection, list) or not MIN_RECORDINGS <= len(selection) <= MAX_RECORDINGS:
        raise MediaLocalTranscriptProjectionError(
            f"manifest.selection must contain {MIN_RECORDINGS} to {MAX_RECORDINGS} recordings"
        )
    normalized_selection: list[dict[str, Any]] = []
    for index, untyped in enumerate(selection):
        item = _object(untyped, f"selection[{index}]")
        _exact_keys(item, f"selection[{index}]", SELECTION_KEYS)
        if type(item["ordinal"]) is not int or item["ordinal"] != index:
            raise MediaLocalTranscriptProjectionError(
                "selection ordinals must be consecutive and begin at zero"
            )
        normalized: dict[str, Any] = {"ordinal": index}
        for key in SELECTION_KEYS - {
            "ordinal", "canonical_key", "public_source_observed_at", "input_duration_ms",
            "parent_duration_ms", "recording_duration_ms",
        }:
            normalized[key] = _identifier(item[key], f"selection[{index}].{key}")
        normalized["canonical_key"] = _text(
            item["canonical_key"], f"selection[{index}].canonical_key", 500
        )
        normalized["public_source_observed_at"], _ = _utc(
            item["public_source_observed_at"],
            f"selection[{index}].public_source_observed_at",
        )
        for key in ("input_duration_ms", "parent_duration_ms", "recording_duration_ms"):
            normalized[key] = _positive_integer(item[key], f"selection[{index}].{key}")
        normalized_selection.append(normalized)
    for key in SELECTION_KEYS - {
        "ordinal", "canonical_key", "public_source_observed_at", "input_duration_ms",
        "parent_duration_ms", "recording_duration_ms",
    }:
        values = [item[key] for item in normalized_selection]
        if len(values) != len(set(values)):
            raise MediaLocalTranscriptProjectionError(
                f"manifest.selection repeats {key}"
            )

    review = _object(value["review"], "manifest.review")
    _exact_keys(
        review,
        "manifest.review",
        {
            "reviewer_id", "reviewed_at", "basis",
            "coordinate_attestation", "approved_plan_sha256",
        },
    )
    reviewer_id = _identifier(review["reviewer_id"], "manifest.review.reviewer_id")
    reviewed_at, reviewed_value = _utc(review["reviewed_at"], "manifest.review.reviewed_at")
    if reviewed_value > datetime.now(timezone.utc):
        raise MediaLocalTranscriptProjectionError(
            "manifest.review.reviewed_at must not be in the future"
        )
    basis = _text(review["basis"], "manifest.review.basis")
    if review["coordinate_attestation"] != COORDINATE_ATTESTATION:
        raise MediaLocalTranscriptProjectionError(
            "manifest.review.coordinate_attestation is not the exact v1 attestation"
        )
    approved = review["approved_plan_sha256"]
    if approved is not None and (
        not isinstance(approved, str) or not SHA256_RE.fullmatch(approved)
    ):
        raise MediaLocalTranscriptProjectionError(
            "manifest.review.approved_plan_sha256 must be null or lowercase SHA-256"
        )
    normalized_value = {
        "schema_version": 1,
        "manifest_id": value["manifest_id"],
        "policy_id": POLICY_ID,
        "selection": normalized_selection,
        "review": {
            "reviewer_id": reviewer_id,
            "reviewed_at": reviewed_at,
            "basis": basis,
            "coordinate_attestation": COORDINATE_ATTESTATION,
            "approved_plan_sha256": approved,
        },
    }
    core = copy.deepcopy(normalized_value)
    del core["review"]["approved_plan_sha256"]
    return ProjectionManifest(
        path=manifest_path,
        raw_json=raw_json,
        value=normalized_value,
        core=core,
        raw_sha256=_sha256_bytes(body),
        canonical_sha256=_sha256_json(normalized_value),
        core_sha256=_sha256_json(core),
        byte_count=len(body),
    )


def load_media_local_transcript_projection_manifest(
    path: str | Path,
) -> ProjectionManifest:
    """Descriptor-safely read and strictly validate one reviewed manifest."""

    manifest_path = Path(path).resolve()
    return _parse_media_local_transcript_projection_manifest(
        _stable_read(manifest_path), manifest_path
    )


def _require_human_coordinate_reviewer(
    connection: sqlite3.Connection, reviewer_id: str, reviewed_at: str
) -> None:
    row = connection.execute(
        """
        SELECT reviewer.reviewer_kind, state.new_active
        FROM reviewers AS reviewer
        JOIN reviewer_admin_events AS state ON state.reviewer_id = reviewer.reviewer_id
        WHERE reviewer.reviewer_id = ?
          AND state.event_sequence = (
              SELECT max(candidate.event_sequence)
              FROM reviewer_admin_events AS candidate
              WHERE candidate.reviewer_id = reviewer.reviewer_id
                AND julianday(candidate.effective_at) <= julianday(?)
          )
        """,
        (reviewer_id, reviewed_at),
    ).fetchone()
    if row is None or row["reviewer_kind"] != "human" or row["new_active"] != 1:
        raise MediaLocalTranscriptProjectionError(
            "coordinate reviewer must be a governed human active at reviewed_at"
        )


def _public_source_access_evidence(
    connection: sqlite3.Connection, entry: dict[str, Any]
) -> dict[str, Any]:
    """Resolve one immutable rank-700 acquisition observation proving public access."""

    observation = _row(
        connection.execute(
            "SELECT * FROM source_metadata_observations "
            "WHERE source_metadata_observation_id = ?",
            (entry["public_source_metadata_observation_id"],),
        ).fetchone(),
        "public source metadata observation",
    )
    if (
        observation["source_id"] != entry["source_id"]
        or observation["import_observation_id"]
        != entry["public_source_import_observation_id"]
        or observation["observed_at"] != entry["public_source_observed_at"]
        or observation["quality_rank"] != 700
        or observation["quality_basis"]
        != "acquisition_result_v1: locally verified acquisition result"
        or observation["access_state"] != "public"
        or observation["review_state"] != "metadata_only"
    ):
        raise MediaLocalTranscriptProjectionError(
            "pinned source observation is not exact rank-700 public acquisition evidence"
        )
    import_batch = _row(
        connection.execute(
            "SELECT * FROM import_batches WHERE import_batch_id = ?",
            (observation["import_batch_id"],),
        ).fetchone(),
        "public source acquisition import batch",
    )
    import_observation = _row(
        connection.execute(
            "SELECT * FROM import_observations WHERE import_observation_id = ?",
            (entry["public_source_import_observation_id"],),
        ).fetchone(),
        "public source acquisition import observation",
    )
    if (
        import_batch["importer_name"] != "acquisition_result_v1"
        or import_batch["status"] != "completed"
        or import_batch["completed_at"] is None
        or import_observation["import_batch_id"] != import_batch["import_batch_id"]
        or import_observation["importer_version"] != import_batch["importer_version"]
        or import_observation["observed_at"] != observation["observed_at"]
        or import_observation["status"] != "completed"
        or import_observation["completed_at"] is None
    ):
        raise MediaLocalTranscriptProjectionError(
            "pinned public acquisition import receipt is incomplete or inconsistent"
        )
    valid_times = connection.execute(
        """
        SELECT
          julianday(?) IS NOT NULL AND julianday(?) IS NOT NULL
          AND julianday(?) IS NOT NULL AND julianday(?) IS NOT NULL
          AND julianday(?) <= julianday(?)
          AND julianday(?) <= julianday(?)
        """,
        (
            import_batch["started_at"], import_batch["completed_at"],
            import_observation["observed_at"], import_observation["completed_at"],
            import_batch["started_at"], import_batch["completed_at"],
            import_observation["observed_at"], import_observation["completed_at"],
        ),
    ).fetchone()[0]
    if valid_times != 1:
        raise MediaLocalTranscriptProjectionError(
            "pinned public acquisition import receipt has invalid chronology"
        )
    graph = {
        "source_metadata_observation": observation,
        "import_batch": import_batch,
        "import_observation": import_observation,
    }
    return {
        "public_source_metadata_observation_id": observation[
            "source_metadata_observation_id"
        ],
        "public_source_import_batch_id": import_batch["import_batch_id"],
        "public_source_import_completed_at": import_batch["completed_at"],
        "public_source_import_observation_id": import_observation[
            "import_observation_id"
        ],
        "public_source_import_observation_completed_at": import_observation[
            "completed_at"
        ],
        "public_source_observed_at": observation["observed_at"],
        "public_source_evidence_sha256": _sha256_json(graph),
    }


RECORDING_SOURCE_MAPPING_FIELDS = (
    "recording_source_id",
    "recording_id",
    "source_id",
    "mapping_role",
    "source_start_ms",
    "source_end_ms",
    "recording_start_ms",
    "recording_end_ms",
    "mapping_method",
)


def _recording_source_mapping_digest(row: dict[str, Any]) -> str:
    return _sha256_json({key: row[key] for key in RECORDING_SOURCE_MAPPING_FIELDS})


def _source_receipt_graph(
    connection: sqlite3.Connection,
    *,
    variant: str,
    receipt_id: str,
    revision: dict[str, Any],
    parent_media_id: str,
) -> dict[str, Any]:
    if variant == "raw":
        receipt = _row(
            connection.execute(
                "SELECT * FROM media_local_asr_imports WHERE media_local_asr_import_id = ?",
                (receipt_id,),
            ).fetchone(),
            f"raw ASR import {receipt_id}",
        )
        if receipt["media_local_revision_id"] != revision["media_local_revision_id"]:
            raise MediaLocalTranscriptProjectionError("raw receipt points to another revision")
        batch_id = receipt["import_batch_id"]
        preprocess_import_batch = _row(
            connection.execute(
                "SELECT * FROM import_batches WHERE import_batch_id = ?",
                (receipt["preprocess_import_batch_id"],),
            ).fetchone(),
            "raw ASR preprocess import batch",
        )
        if (
            preprocess_import_batch["importer_name"] != "media_preprocess_result_v1"
            or preprocess_import_batch["status"] != "completed"
            or preprocess_import_batch["completed_at"] is None
            or preprocess_import_batch["input_sha256"]
            != receipt["preprocess_result_canonical_sha256"]
        ):
            raise MediaLocalTranscriptProjectionError(
                "raw ASR preprocess import batch is not exact and completed"
            )
        extra: dict[str, Any] = {
            "preprocess_import_batch": preprocess_import_batch
        }
        expected_importer = "media_local_asr_result_v1"
        expected_input_sha256 = receipt["asr_result_canonical_sha256"]
    else:
        receipt = _row(
            connection.execute(
                "SELECT * FROM contextual_media_local_asr_imports WHERE contextual_asr_import_id = ?",
                (receipt_id,),
            ).fetchone(),
            f"contextual ASR import {receipt_id}",
        )
        if receipt["media_local_revision_id"] != revision["media_local_revision_id"]:
            raise MediaLocalTranscriptProjectionError(
                "contextual receipt points to another revision"
            )
        batch_id = receipt["import_batch_id"]
        contextual_batch = _row(
            connection.execute(
                "SELECT * FROM contextual_asr_batch_registrations WHERE contextual_batch_id = ?",
                (receipt["contextual_batch_id"],),
            ).fetchone(),
            "contextual batch registration",
        )
        glossary = _row(
            connection.execute(
                "SELECT * FROM private_glossary_registrations WHERE glossary_revision_id = ?",
                (contextual_batch["glossary_revision_id"],),
            ).fetchone(),
            "private glossary registration",
        )
        extra = {"contextual_batch": contextual_batch, "glossary": glossary}
        expected_importer = "contextual_media_local_asr_result_v1"
        expected_input_sha256 = receipt["result_canonical_sha256"]
    import_batch = _row(
        connection.execute(
            "SELECT * FROM import_batches WHERE import_batch_id = ?", (batch_id,)
        ).fetchone(),
        f"source import batch {batch_id}",
    )
    if (
        import_batch["importer_name"] != expected_importer
        or import_batch["status"] != "completed"
        or import_batch["completed_at"] is None
        or import_batch["input_sha256"] != expected_input_sha256
    ):
        raise MediaLocalTranscriptProjectionError(
            f"{variant} ASR import batch is not exact and completed"
        )
    input_artifact = _row(
        connection.execute(
            "SELECT * FROM artifacts WHERE artifact_id = ?",
            (revision["input_artifact_id"],),
        ).fetchone(),
        f"{variant} normalized input artifact",
    )
    normalized = _row(
        connection.execute(
            "SELECT * FROM media_objects WHERE media_id = ?",
            (revision["media_id"],),
        ).fetchone(),
        f"{variant} normalized input media",
    )
    if (
        input_artifact["artifact_kind"] != "audio_16khz_mono_flac"
        or input_artifact["visibility"] != "private"
        or input_artifact["sha256"] != normalized["sha256"]
        or input_artifact["byte_count"] != normalized["byte_count"]
    ):
        raise MediaLocalTranscriptProjectionError(
            f"{variant} 16 kHz mono FLAC artifact differs from normalized media identity"
        )
    preprocess_run = _row(
        connection.execute(
            "SELECT * FROM processing_runs WHERE processing_run_id = ?",
            (input_artifact["processing_run_id"],),
        ).fetchone(),
        f"{variant} normalized input artifact processing run",
    )
    if (
        preprocess_run["stage"] != "media_preprocess"
        or preprocess_run["status"] != "completed"
        or preprocess_run["completed_at"] is None
        or preprocess_run["error_text"] is not None
    ):
        raise MediaLocalTranscriptProjectionError(
            f"{variant} input artifact producer is not a completed preprocess run"
        )
    preprocess_run_inputs = _rows(
        connection.execute(
            "SELECT * FROM run_inputs WHERE processing_run_id = ? ORDER BY run_input_id",
            (preprocess_run["processing_run_id"],),
        )
    )
    preprocess_run_artifacts = _rows(
        connection.execute(
            "SELECT * FROM artifacts WHERE processing_run_id = ? ORDER BY artifact_id",
            (preprocess_run["processing_run_id"],),
        )
    )
    if len(preprocess_run_inputs) != 1 or (
        preprocess_run_inputs[0]["object_type"],
        preprocess_run_inputs[0]["object_id"],
        preprocess_run_inputs[0]["input_role"],
    ) != ("media", parent_media_id, "source_media"):
        raise MediaLocalTranscriptProjectionError(
            f"{variant} input artifact producer must have one exact parent-media input"
        )
    parent = _row(
        connection.execute(
            "SELECT * FROM media_objects WHERE media_id = ?",
            (parent_media_id,),
        ).fetchone(),
        f"{variant} preprocess parent media",
    )
    if preprocess_run_inputs[0]["input_sha256"] != parent["sha256"]:
        raise MediaLocalTranscriptProjectionError(
            f"{variant} input artifact producer input digest differs from parent media"
        )
    if [row["artifact_id"] for row in preprocess_run_artifacts].count(
        input_artifact["artifact_id"]
    ) != 1:
        raise MediaLocalTranscriptProjectionError(
            f"{variant} normalized input artifact is absent from its producer artifact set"
        )
    for field in (
        "input_media_id", "input_artifact_id", "input_duration_ms",
        "max_segment_end_ms", "input_boundary_overrun_ms",
    ):
        revision_field = {
            "input_media_id": "media_id",
            "input_artifact_id": "input_artifact_id",
            "input_duration_ms": "input_duration_ms",
            "max_segment_end_ms": "max_segment_end_ms",
            "input_boundary_overrun_ms": "input_boundary_overrun_ms",
        }[field]
        if receipt[field] != revision[revision_field]:
            raise MediaLocalTranscriptProjectionError(
                f"{variant} receipt differs from source revision field {field}"
            )
    return {
        "receipt": receipt,
        "import_batch": import_batch,
        "input_artifact": input_artifact,
        "input_artifact_processing_run": preprocess_run,
        "input_artifact_processing_run_inputs": preprocess_run_inputs,
        "input_artifact_processing_run_artifacts": preprocess_run_artifacts,
        **extra,
    }


def _source_transcript_graph(
    connection: sqlite3.Connection,
    *,
    variant: str,
    receipt_id: str,
    revision: dict[str, Any],
    parent_media_id: str,
) -> dict[str, Any]:
    run = _row(
        connection.execute(
            "SELECT * FROM processing_runs WHERE processing_run_id = ?",
            (revision["processing_run_id"],),
        ).fetchone(),
        "source processing run",
    )
    if run["glossary_revision_id"] != revision["glossary_revision_id"]:
        raise MediaLocalTranscriptProjectionError(
            "source processing run glossary differs from source revision"
        )
    if (
        run["stage"] != "asr_whispercpp"
        or run["status"] != "completed"
        or run["completed_at"] is None
        or run["error_text"] is not None
    ):
        raise MediaLocalTranscriptProjectionError(
            "source ASR processing run is not an exact completed asr_whispercpp run"
        )
    run_inputs = _rows(
        connection.execute(
            "SELECT * FROM run_inputs WHERE processing_run_id = ? ORDER BY run_input_id",
            (revision["processing_run_id"],),
        )
    )
    media = _row(
        connection.execute("SELECT * FROM media_objects WHERE media_id = ?", (revision["media_id"],)).fetchone(),
        "normalized source media",
    )
    if len(run_inputs) != 1 or (
        run_inputs[0]["object_type"], run_inputs[0]["object_id"],
        run_inputs[0]["input_role"], run_inputs[0]["input_sha256"],
    ) != ("media", revision["media_id"], "normalized_audio", media["sha256"]):
        raise MediaLocalTranscriptProjectionError(
            "source ASR run must have exactly one matching normalized_audio media input"
        )
    artifacts = _rows(
        connection.execute(
            "SELECT * FROM artifacts WHERE processing_run_id = ? ORDER BY artifact_id",
            (revision["processing_run_id"],),
        )
    )
    segments = _rows(
        connection.execute(
            "SELECT * FROM media_local_transcript_segments WHERE media_local_revision_id = ? ORDER BY ordinal",
            (revision["media_local_revision_id"],),
        )
    )
    words = _rows(
        connection.execute(
            """
            SELECT word.*
            FROM media_local_transcript_words AS word
            JOIN media_local_transcript_segments AS segment
              ON segment.media_local_segment_id = word.media_local_segment_id
            WHERE segment.media_local_revision_id = ?
            ORDER BY segment.ordinal, word.ordinal
            """,
            (revision["media_local_revision_id"],),
        )
    )
    receipt_graph = _source_receipt_graph(
        connection,
        variant=variant,
        receipt_id=receipt_id,
        revision=revision,
        parent_media_id=parent_media_id,
    )
    return {
        "revision": revision,
        "run": run,
        "run_inputs": run_inputs,
        "artifacts": artifacts,
        "receipt_graph": receipt_graph,
        "segments": segments,
        "words": words,
    }


def _projection_source(
    connection: sqlite3.Connection,
    entry: dict[str, Any],
    *,
    variant: str,
    historical_item: dict[str, Any] | None = None,
) -> dict[str, Any]:
    strict_current_catalog = historical_item is None
    revision_id = entry[f"{variant}_revision_id"]
    receipt_id = entry[f"{variant}_import_id"]
    revision = _row(
        connection.execute(
            "SELECT * FROM media_local_transcript_revisions WHERE media_local_revision_id = ?",
            (revision_id,),
        ).fetchone(),
        f"media-local revision {revision_id}",
    )
    expected_kind = "raw_asr" if variant == "raw" else "contextual_asr"
    if (
        revision["revision_kind"] != expected_kind
        or revision["review_state"] != "machine"
        or revision["coordinate_system"] != "media_ms"
        or revision["boundary"] != "half_open"
        or revision["requested_start_ms"] != 0
        or revision["requested_end_ms"] != revision["input_duration_ms"]
        or revision["input_boundary_overrun_ms"] != 0
        or revision["source_coordinate_state"] != "unasserted_catalog_context_null"
        or revision["recording_coordinate_state"] != "unasserted_catalog_context_null"
        or (variant == "raw" and revision["glossary_revision_id"] is not None)
        or (variant == "contextual" and revision["glossary_revision_id"] is None)
    ):
        raise MediaLocalTranscriptProjectionError(
            f"{revision_id} is not an exact full-file {variant} machine hypothesis"
        )
    if revision["media_id"] != entry["normalized_media_id"]:
        raise MediaLocalTranscriptProjectionError("manifest pins the wrong normalized media")
    normalized = _row(
        connection.execute("SELECT * FROM media_objects WHERE media_id = ?", (entry["normalized_media_id"],)).fetchone(),
        "normalized media",
    )
    if (
        normalized["media_kind"] != "audio"
        or normalized["mime_type"] != "audio/flac"
        or normalized["container"] != "flac"
    ):
        raise MediaLocalTranscriptProjectionError(
            "normalized ASR media must be an audio/flac FLAC object"
        )
    derivation = _row(
        connection.execute(
            """
            SELECT * FROM media_derivations
            WHERE child_media_id = ? AND parent_media_id = ?
              AND derivation_kind = 'audio_normalization_16khz_mono_flac'
            """,
            (entry["normalized_media_id"], entry["parent_media_id"]),
        ).fetchone(),
        "pinned normalized-media derivation",
    )
    try:
        derivation_metadata = json.loads(
            derivation["metadata_json"], object_pairs_hook=_reject_duplicate_keys
        )
    except (TypeError, json.JSONDecodeError) as error:
        raise MediaLocalTranscriptProjectionError(
            "normalized-media derivation metadata is invalid"
        ) from error
    if (
        not isinstance(derivation_metadata, dict)
        or derivation_metadata.get("channels") != 1
        or derivation_metadata.get("sample_rate_hz") != 16000
    ):
        raise MediaLocalTranscriptProjectionError(
            "normalization derivation does not assert 16 kHz mono audio"
        )
    if strict_current_catalog and connection.execute(
        "SELECT count(*) FROM media_derivations WHERE child_media_id = ?",
        (entry["normalized_media_id"],),
    ).fetchone()[0] != 1:
        raise MediaLocalTranscriptProjectionError(
            "normalized media lineage is not unique and exact"
        )
    if strict_current_catalog and connection.execute(
        "SELECT count(*) FROM renditions WHERE media_id = ?", (entry["normalized_media_id"],)
    ).fetchone()[0] != 0:
        raise MediaLocalTranscriptProjectionError(
            "v1 requires normalized ASR media without a rendition"
        )
    parent = _row(
        connection.execute("SELECT * FROM media_objects WHERE media_id = ?", (entry["parent_media_id"],)).fetchone(),
        "parent media",
    )
    if parent["media_kind"] != "video":
        raise MediaLocalTranscriptProjectionError("acquired parent media must be video")
    parent_rendition = _row(
        connection.execute(
            "SELECT * FROM renditions WHERE rendition_id = ?",
            (entry["parent_rendition_id"],),
        ).fetchone(),
        "pinned parent rendition",
    )
    if (
        parent_rendition["media_id"] != entry["parent_media_id"]
        or parent_rendition["recording_id"] != entry["recording_id"]
    ):
        raise MediaLocalTranscriptProjectionError(
            "pinned parent rendition has different media or recording identity"
        )
    if strict_current_catalog and connection.execute(
        "SELECT count(*) FROM renditions WHERE media_id = ?",
        (entry["parent_media_id"],),
    ).fetchone()[0] != 1:
        raise MediaLocalTranscriptProjectionError("parent media must have one pinned rendition")
    recording = _row(
        connection.execute("SELECT * FROM recordings WHERE recording_id = ?", (entry["recording_id"],)).fetchone(),
        "recording",
    )
    if strict_current_catalog and recording["canonical_key"] != entry["canonical_key"]:
        raise MediaLocalTranscriptProjectionError("manifest canonical key differs from recording")
    source = _row(
        connection.execute("SELECT * FROM sources WHERE source_id = ?", (entry["source_id"],)).fetchone(),
        "qualifying source",
    )
    media_source = _row(
        connection.execute(
            "SELECT * FROM media_sources WHERE media_source_id = ?", (entry["media_source_id"],)
        ).fetchone(),
        "media-source lineage",
    )
    recording_source = _row(
        connection.execute(
            "SELECT * FROM recording_sources WHERE recording_source_id = ?",
            (entry["recording_source_id"],),
        ).fetchone(),
        "recording-source mapping",
    )
    if (
        media_source["media_id"] != entry["parent_media_id"]
        or media_source["source_id"] != entry["source_id"]
        or recording_source["recording_id"] != entry["recording_id"]
        or recording_source["source_id"] != entry["source_id"]
    ):
        raise MediaLocalTranscriptProjectionError(
            "manifest lacks exact direct acquired-file lineage"
        )
    if media_source["source_snapshot_id"] is not None:
        raise MediaLocalTranscriptProjectionError(
            "v1 requires direct media-source lineage without a mutable source snapshot"
        )
    if strict_current_catalog and (
        source["access_state"] != "public"
        or recording_source["confidence_state"]
        not in {"metadata_only", "candidate", "reviewed"}
    ):
        raise MediaLocalTranscriptProjectionError(
            "current catalog does not admit direct public acquired-file lineage"
        )
    access_evidence = _public_source_access_evidence(connection, entry)
    if historical_item is None:
        recording_duration = recording["duration_ms"]
        source_mapping_confidence_state = recording_source["confidence_state"]
    else:
        recording_duration = historical_item.get("recording_duration_ms")
        source_mapping_confidence_state = historical_item.get(
            "source_mapping_confidence_state"
        )
        if source_mapping_confidence_state not in {
            "metadata_only", "candidate", "reviewed"
        }:
            raise MediaLocalTranscriptProjectionError(
                "stored source mapping confidence snapshot is invalid"
            )
    durations = (revision["input_duration_ms"], parent["duration_ms"], recording_duration)
    if durations != (
        entry["input_duration_ms"], entry["parent_duration_ms"], entry["recording_duration_ms"]
    ):
        raise MediaLocalTranscriptProjectionError("manifest duration snapshot is stale")
    if normalized["duration_ms"] != revision["input_duration_ms"]:
        raise MediaLocalTranscriptProjectionError("normalized media duration differs from ASR input")
    parent_delta = abs(durations[0] - durations[1])
    recording_delta = abs(durations[0] - durations[2])
    if parent_delta != 0:
        raise MediaLocalTranscriptProjectionError("normalized-parent duration delta must equal 0 ms")
    if recording_delta > MAX_RECORDING_DURATION_DELTA_MS:
        raise MediaLocalTranscriptProjectionError(
            "normalized-recording duration delta exceeds 5 ms"
        )
    graph = _source_transcript_graph(
        connection,
        variant=variant,
        receipt_id=receipt_id,
        revision=revision,
        parent_media_id=entry["parent_media_id"],
    )
    if derivation["processing_run_id"] != graph["receipt_graph"][
        "input_artifact_processing_run"
    ]["processing_run_id"]:
        raise MediaLocalTranscriptProjectionError(
            "normalization derivation and input artifact name different preprocess runs"
        )
    segments = graph["segments"]
    words = graph["words"]
    if not segments:
        raise MediaLocalTranscriptProjectionError("source hypothesis has no segments")
    actual_max = max(segment["media_end_ms"] for segment in segments)
    shared_end = min(durations)
    if revision["max_segment_end_ms"] != actual_max or actual_max > shared_end:
        raise MediaLocalTranscriptProjectionError("source segment boundary facts are inconsistent")
    segment_by_id = {segment["media_local_segment_id"]: segment for segment in segments}
    if any(segment["speaker_label"] is not None for segment in segments):
        raise MediaLocalTranscriptProjectionError(
            "machine-default projection requires null source speaker labels"
        )
    for segment in segments:
        metadata = json.loads(segment["metadata_json"])
        if "coordinate_projection" in metadata:
            raise MediaLocalTranscriptProjectionError(
                "source segment already contains coordinate_projection metadata"
            )
    for word in words:
        segment = segment_by_id[word["media_local_segment_id"]]
        if (word["media_start_ms"] is None) != (word["media_end_ms"] is None):
            raise MediaLocalTranscriptProjectionError("word timing nullability is inconsistent")
        if word["media_start_ms"] is not None and (
            word["media_start_ms"] < segment["media_start_ms"]
            or word["media_end_ms"] > segment["media_end_ms"]
            or word["media_end_ms"] > shared_end
        ):
            raise MediaLocalTranscriptProjectionError("word lies outside its source segment or shared duration")
    receipt_graph = graph["receipt_graph"]
    receipt_digest = _sha256_json(receipt_graph)
    transcript_digest = _sha256_json(graph)
    projection_id = stable_id("mltp", POLICY_ID, revision_id, entry["recording_id"])
    item = {
        "selection_ordinal": entry["ordinal"],
        "projection_ordinal": entry["ordinal"] * 2 + (0 if variant == "raw" else 1),
        "variant": variant,
        "media_local_revision_id": revision_id,
        "media_local_asr_import_id": receipt_id if variant == "raw" else None,
        "contextual_asr_import_id": receipt_id if variant == "contextual" else None,
        "source_receipt_sha256": receipt_digest,
        "source_transcript_sha256": transcript_digest,
        "source_receipt_imported_at": receipt_graph["receipt"]["imported_at"],
        "source_receipt_import_completed_at": receipt_graph["import_batch"][
            "completed_at"
        ],
        "source_processing_run_completed_at": graph["run"]["completed_at"],
        "source_preprocess_import_completed_at": (
            receipt_graph["preprocess_import_batch"]["completed_at"]
            if variant == "raw" else None
        ),
        "input_artifact_id": receipt_graph["input_artifact"]["artifact_id"],
        "input_artifact_sha256": receipt_graph["input_artifact"]["sha256"],
        "input_artifact_processing_run_id": receipt_graph[
            "input_artifact_processing_run"
        ]["processing_run_id"],
        "input_artifact_processing_run_completed_at": receipt_graph[
            "input_artifact_processing_run"
        ]["completed_at"],
        "revision_kind": revision["revision_kind"],
        "language": revision["language"],
        "glossary_revision_id": revision["glossary_revision_id"],
        "source_processing_run_id": revision["processing_run_id"],
        "normalized_media_id": entry["normalized_media_id"],
        "parent_media_id": entry["parent_media_id"],
        "parent_rendition_id": entry["parent_rendition_id"],
        "parent_rendition_identity_sha256": _sha256_json(
            {
                "rendition_id": parent_rendition["rendition_id"],
                "recording_id": parent_rendition["recording_id"],
                "media_id": parent_rendition["media_id"],
                "rendition_kind": parent_rendition["rendition_kind"],
            }
        ),
        "recording_id": entry["recording_id"],
        "qualifying_source_id": entry["source_id"],
        "source_identity_sha256": _sha256_json(
            {
                "source_id": source["source_id"],
                "platform": source["platform"],
                "source_kind": source["source_kind"],
                "native_id": source["native_id"],
            }
        ),
        "qualifying_media_source_id": entry["media_source_id"],
        "qualifying_recording_source_id": entry["recording_source_id"],
        "normalized_media_sha256": normalized["sha256"],
        "parent_media_sha256": parent["sha256"],
        "media_derivation_sha256": _sha256_json(derivation),
        "media_source_sha256": _sha256_json(media_source),
        "recording_source_mapping_sha256": _recording_source_mapping_digest(
            recording_source
        ),
        **access_evidence,
        "source_mapping_confidence_state": source_mapping_confidence_state,
        "input_duration_ms": durations[0],
        "parent_duration_ms": durations[1],
        "recording_duration_ms": durations[2],
        "normalized_parent_delta_ms": parent_delta,
        "normalized_recording_delta_ms": recording_delta,
        "max_segment_end_ms": actual_max,
        "segment_count": len(segments),
        "word_count": len(words),
        "projection_id": projection_id,
        "projection_processing_run_id": stable_id("run", POLICY_ID, revision_id),
        "target_revision_id": stable_id("tr", POLICY_ID, revision_id, entry["recording_id"]),
    }
    item["target_child_identity_sha256"] = _target_child_identity_sha256(
        item, segments, words
    )
    item["generated_payload_sha256"] = _sha256_json(
        {
            "processing_run_parameters_json": _projection_parameters(item),
            "processing_run_environment_json": _projection_environment(),
            "revision_metadata_json": _revision_metadata(item),
            "projection_metadata_json": _projection_metadata(item),
        }
    )
    return item


PAIR_FLAG_FIELDS = (
    "input_equal", "engine_equal", "model_equal", "window_equal",
    "inference_equal", "catalog_context_equal", "only_glossary_job_output_differ",
    "correction_asserted", "accuracy_claimed", "improvement_claimed",
    "human_review_claimed", "automatic_merge_allowed",
)


def _projection_pair(
    connection: sqlite3.Connection,
    entry: dict[str, Any],
    raw: dict[str, Any],
    contextual: dict[str, Any],
) -> dict[str, Any]:
    pair = _row(
        connection.execute(
            "SELECT * FROM contextual_media_local_asr_pairs WHERE contextual_pair_id = ?",
            (entry["contextual_pair_id"],),
        ).fetchone(),
        "contextual source pair",
    )
    diff = _row(
        connection.execute(
            "SELECT * FROM contextual_asr_text_private_diffs WHERE contextual_diff_id = ?",
            (entry["contextual_diff_id"],),
        ).fetchone(),
        "contextual private diff",
    )
    if (
        pair["baseline_media_local_revision_id"] != raw["media_local_revision_id"]
        or pair["contextual_media_local_revision_id"] != contextual["media_local_revision_id"]
        or pair["contextual_asr_import_id"] != contextual["contextual_asr_import_id"]
        or diff["contextual_pair_id"] != pair["contextual_pair_id"]
        or pair["pair_state"] != "competing_machine_revisions_no_preference"
        or pair["preferred_revision_id"] is not None
        or pair["publication_authority"] != "none"
        or diff["visibility"] != "private"
        or diff["publication_authority"] != "none"
        or diff["preferred_revision_selected"] != 0
    ):
        raise MediaLocalTranscriptProjectionError(
            "contextual pair/diff does not match the pinned no-preference graph"
        )
    required_pair_values = {
        "input_equal": 1, "engine_equal": 1, "model_equal": 1,
        "window_equal": 1, "inference_equal": 1, "catalog_context_equal": 1,
        "only_glossary_job_output_differ": 1, "correction_asserted": 0,
        "accuracy_claimed": 0, "improvement_claimed": 0,
        "human_review_claimed": 0, "automatic_merge_allowed": 0,
    }
    if any(pair[key] != value for key, value in required_pair_values.items()):
        raise MediaLocalTranscriptProjectionError("source pair contains a forbidden claim")
    if any(
        diff[key] != 0
        for key in (
            "accuracy_claimed", "improvement_claimed", "human_review_claimed",
            "automatic_merge_allowed",
        )
    ):
        raise MediaLocalTranscriptProjectionError("source diff contains a forbidden claim")
    shared_fields = (
        "recording_id", "normalized_media_id", "parent_media_id",
        "parent_rendition_id", "parent_rendition_identity_sha256",
        "qualifying_source_id",
        "source_identity_sha256",
        "qualifying_media_source_id", "qualifying_recording_source_id",
        "normalized_media_sha256", "parent_media_sha256",
        "media_derivation_sha256", "media_source_sha256",
        "recording_source_mapping_sha256",
        "input_artifact_id", "input_artifact_sha256",
        "input_artifact_processing_run_id",
        "public_source_metadata_observation_id",
        "public_source_import_batch_id", "public_source_import_observation_id",
        "public_source_import_completed_at",
        "public_source_import_observation_completed_at",
        "public_source_observed_at", "public_source_evidence_sha256",
        "input_duration_ms",
        "parent_duration_ms", "recording_duration_ms",
    )
    if any(raw[key] != contextual[key] for key in shared_fields):
        raise MediaLocalTranscriptProjectionError("raw/contextual projection facts differ")
    pair_graph_digest = _sha256_json({"pair": pair, "diff": diff})
    return {
        "recording_ordinal": entry["ordinal"],
        "projection_pair_id": stable_id("mltpp", POLICY_ID, pair["contextual_pair_id"]),
        "contextual_pair_id": pair["contextual_pair_id"],
        "contextual_diff_id": diff["contextual_diff_id"],
        "source_pair_sha256": pair_graph_digest,
        "source_pair_created_at": pair["created_at"],
        "source_diff_created_at": diff["created_at"],
        "baseline_projection_id": raw["projection_id"],
        "contextual_projection_id": contextual["projection_id"],
        "recording_id": raw["recording_id"],
        "pair_state": pair["pair_state"],
        **{key: pair[key] for key in PAIR_FLAG_FIELDS},
        "preferred_revision_id": None,
    }


def _require_projection_evidence_chronology(
    connection: sqlite3.Connection,
    *,
    reviewed_at: str,
    projections: list[dict[str, Any]],
    pairs: list[dict[str, Any]],
) -> None:
    evidence_times = [
        *(item["source_receipt_imported_at"] for item in projections),
        *(item["source_receipt_import_completed_at"] for item in projections),
        *(item["source_processing_run_completed_at"] for item in projections),
        *(
            item["input_artifact_processing_run_completed_at"]
            for item in projections
        ),
        *(
            item["source_preprocess_import_completed_at"]
            for item in projections
            if item["source_preprocess_import_completed_at"] is not None
        ),
        *(item["public_source_observed_at"] for item in projections),
        *(item["public_source_import_completed_at"] for item in projections),
        *(
            item["public_source_import_observation_completed_at"]
            for item in projections
        ),
        *(pair["source_pair_created_at"] for pair in pairs),
        *(pair["source_diff_created_at"] for pair in pairs),
    ]
    for evidence_time in evidence_times:
        valid = connection.execute(
            "SELECT julianday(?) IS NOT NULL AND julianday(?) <= julianday(?)",
            (evidence_time, evidence_time, reviewed_at),
        ).fetchone()[0]
        if valid != 1:
            raise MediaLocalTranscriptProjectionError(
                "coordinate review predates source access, receipt, pair, or diff evidence"
            )


def _assembled_plan_payload(
    connection: sqlite3.Connection,
    manifest: ProjectionManifest,
    projections: list[dict[str, Any]],
    pairs: list[dict[str, Any]],
) -> dict[str, Any]:
    _require_projection_evidence_chronology(
        connection,
        reviewed_at=manifest.value["review"]["reviewed_at"],
        projections=projections,
        pairs=pairs,
    )
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "policy_id": POLICY_ID,
        "bridge_version": BRIDGE_VERSION,
        "manifest_core_sha256": manifest.core_sha256,
        "manifest_core": manifest.core,
        "transform": {
            "expression": TRANSFORM_EXPRESSION,
            "boundary": "half_open",
            "normalized_parent_delta_ms": 0,
            "maximum_normalized_recording_delta_ms": MAX_RECORDING_DURATION_DELTA_MS,
            "endpoint_repair": "forbidden",
            "interpolation": "forbidden",
            "rescaling": "forbidden",
        },
        "projection_count": len(projections),
        "recording_count": len(manifest.value["selection"]),
        "pair_count": len(pairs),
        "segment_count": sum(item["segment_count"] for item in projections),
        "word_count": sum(item["word_count"] for item in projections),
        "projections": projections,
        "pairs": pairs,
        "safety": {
            "transcript_text_in_plan": False,
            "target_rendition_id": None,
            "wording_reviewed": False,
            "speaker_identity_asserted": False,
            "accuracy_claimed": False,
            "preferred_revision_selected": False,
            "publication_authority": "none",
            "human_gate_decisions_created": 0,
        },
    }


def _plan_payload(
    connection: sqlite3.Connection, manifest: ProjectionManifest
) -> dict[str, Any]:
    review = manifest.value["review"]
    _require_human_coordinate_reviewer(
        connection, review["reviewer_id"], review["reviewed_at"]
    )
    projections: list[dict[str, Any]] = []
    pairs: list[dict[str, Any]] = []
    for entry in manifest.value["selection"]:
        raw = _projection_source(connection, entry, variant="raw")
        contextual = _projection_source(connection, entry, variant="contextual")
        projections.extend((raw, contextual))
        pairs.append(_projection_pair(connection, entry, raw, contextual))
    return _assembled_plan_payload(connection, manifest, projections, pairs)


def build_media_local_transcript_projection_plan(
    connection: sqlite3.Connection, manifest_path: str | Path
) -> dict[str, Any]:
    """Build a deterministic, transcript-text-free plan from a strict manifest."""

    manifest = load_media_local_transcript_projection_manifest(manifest_path)
    payload = _plan_payload(connection, manifest)
    return {**payload, "plan_sha256": _sha256_json(payload)}


def _projection_parameters(item: dict[str, Any]) -> str:
    return canonical_json(
        {
            "boundary": "half_open",
            "maximum_normalized_recording_delta_ms": MAX_RECORDING_DURATION_DELTA_MS,
            "normalized_parent_delta_ms": 0,
            "projection_id": item["projection_id"],
            "source_processing_run_id": item["source_processing_run_id"],
            "transform_expression": TRANSFORM_EXPRESSION,
        }
    )


def _projection_environment() -> str:
    return canonical_json(
        {
            "catalog_only": True,
            "network_access_performed": False,
            "timestamp_repair_performed": False,
            "wording_inference_performed": False,
        }
    )


def _revision_metadata(item: dict[str, Any]) -> str:
    return canonical_json(
        {
            "coordinate_projection": {
                "boundary": "half_open",
                "parent_rendition_id": item["parent_rendition_id"],
                "projection_id": item["projection_id"],
                "source_media_local_revision_id": item["media_local_revision_id"],
                "source_transcript_sha256": item["source_transcript_sha256"],
                "target_rendition_id": None,
                "transform_expression": TRANSFORM_EXPRESSION,
            },
            "accuracy_claimed": False,
            "preferred_revision_selected": False,
            "speaker_identity_asserted": False,
            "wording_reviewed": False,
        }
    )


def _projection_metadata(item: dict[str, Any]) -> str:
    return canonical_json(
        {
            "qualifying_media_source_id": item["qualifying_media_source_id"],
            "qualifying_recording_source_id": item[
                "qualifying_recording_source_id"
            ],
            "source_identity_sha256": item["source_identity_sha256"],
            "media_derivation_sha256": item["media_derivation_sha256"],
            "media_source_sha256": item["media_source_sha256"],
            "parent_rendition_identity_sha256": item[
                "parent_rendition_identity_sha256"
            ],
            "public_source_evidence_sha256": item[
                "public_source_evidence_sha256"
            ],
            "recording_source_mapping_sha256": item[
                "recording_source_mapping_sha256"
            ],
            "source_receipt_sha256": item["source_receipt_sha256"],
            "source_transcript_sha256": item["source_transcript_sha256"],
            "input_artifact_id": item["input_artifact_id"],
            "input_artifact_sha256": item["input_artifact_sha256"],
            "input_artifact_processing_run_id": item[
                "input_artifact_processing_run_id"
            ],
            "target_child_identity_sha256": item[
                "target_child_identity_sha256"
            ],
            "target_rendition_id": None,
        }
    )


def _target_child_identity_payload(
    item: dict[str, Any],
    source_segments: list[dict[str, Any]],
    source_words: list[dict[str, Any]],
) -> dict[str, Any]:
    target_segment_ids = {
        segment["media_local_segment_id"]: stable_id(
            "ts", item["target_revision_id"], segment["media_local_segment_id"]
        )
        for segment in source_segments
    }
    return {
        "segments": [
            {
                "ordinal": segment["ordinal"],
                "source_segment_id": segment["media_local_segment_id"],
                "target_segment_id": target_segment_ids[
                    segment["media_local_segment_id"]
                ],
            }
            for segment in source_segments
        ],
        "words": [
            {
                "source_segment_id": word["media_local_segment_id"],
                "ordinal": word["ordinal"],
                "source_word_id": word["media_local_word_id"],
                "target_segment_id": target_segment_ids[
                    word["media_local_segment_id"]
                ],
                "target_word_id": stable_id(
                    "tw",
                    target_segment_ids[word["media_local_segment_id"]],
                    word["media_local_word_id"],
                ),
            }
            for word in source_words
        ],
    }


def _target_child_identity_sha256(
    item: dict[str, Any],
    source_segments: list[dict[str, Any]],
    source_words: list[dict[str, Any]],
) -> str:
    return _sha256_json(
        _target_child_identity_payload(item, source_segments, source_words)
    )


def _actual_target_child_identity_sha256(
    connection: sqlite3.Connection, item: dict[str, Any]
) -> str:
    source_segments = _rows(
        connection.execute(
            "SELECT * FROM media_local_transcript_segments "
            "WHERE media_local_revision_id = ? ORDER BY ordinal",
            (item["media_local_revision_id"],),
        )
    )
    source_words = _rows(
        connection.execute(
            """
            SELECT word.*
            FROM media_local_transcript_words AS word
            JOIN media_local_transcript_segments AS segment
              ON segment.media_local_segment_id = word.media_local_segment_id
            WHERE segment.media_local_revision_id = ?
            ORDER BY segment.ordinal, word.ordinal
            """,
            (item["media_local_revision_id"],),
        )
    )
    target_segments = _rows(
        connection.execute(
            "SELECT segment_id, ordinal FROM transcript_segments "
            "WHERE revision_id = ? ORDER BY ordinal",
            (item["target_revision_id"],),
        )
    )
    target_words = _rows(
        connection.execute(
            """
            SELECT word.word_id, word.segment_id, word.ordinal,
                   segment.ordinal AS segment_ordinal
            FROM transcript_words AS word
            JOIN transcript_segments AS segment
              ON segment.segment_id = word.segment_id
            WHERE segment.revision_id = ?
            ORDER BY segment.ordinal, word.ordinal
            """,
            (item["target_revision_id"],),
        )
    )
    if len(source_segments) != len(target_segments) or len(source_words) != len(
        target_words
    ):
        raise MediaLocalTranscriptProjectionError(
            "stored projected child-ID sets have different cardinality"
        )
    target_segment_by_ordinal = {
        row["ordinal"]: row["segment_id"] for row in target_segments
    }
    source_segment_by_id = {
        row["media_local_segment_id"]: row for row in source_segments
    }
    target_word_by_key = {
        (row["segment_ordinal"], row["ordinal"]): row for row in target_words
    }
    payload = {
        "segments": [
            {
                "ordinal": source["ordinal"],
                "source_segment_id": source["media_local_segment_id"],
                "target_segment_id": target_segment_by_ordinal.get(
                    source["ordinal"]
                ),
            }
            for source in source_segments
        ],
        "words": [],
    }
    for source_word in source_words:
        source_segment = source_segment_by_id[source_word["media_local_segment_id"]]
        target_word = target_word_by_key.get(
            (source_segment["ordinal"], source_word["ordinal"])
        )
        payload["words"].append(
            {
                "source_segment_id": source_word["media_local_segment_id"],
                "ordinal": source_word["ordinal"],
                "source_word_id": source_word["media_local_word_id"],
                "target_segment_id": (
                    target_word["segment_id"] if target_word is not None else None
                ),
                "target_word_id": (
                    target_word["word_id"] if target_word is not None else None
                ),
            }
        )
    return _sha256_json(payload)


def _expected_target_rows(
    connection: sqlite3.Connection, item: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    source_segments = _rows(
        connection.execute(
            "SELECT * FROM media_local_transcript_segments WHERE media_local_revision_id = ? ORDER BY ordinal",
            (item["media_local_revision_id"],),
        )
    )
    segments: list[dict[str, Any]] = []
    segment_ids: dict[str, str] = {}
    for source in source_segments:
        segment_id = stable_id(
            "ts", item["target_revision_id"], source["media_local_segment_id"]
        )
        segment_ids[source["media_local_segment_id"]] = segment_id
        metadata = json.loads(source["metadata_json"])
        if "coordinate_projection" in metadata:
            raise MediaLocalTranscriptProjectionError(
                "source segment already contains coordinate_projection metadata"
            )
        metadata["coordinate_projection"] = {
            "projection_id": item["projection_id"],
            "source_media_local_segment_id": source["media_local_segment_id"],
            "transform_expression": TRANSFORM_EXPRESSION,
        }
        segments.append(
            {
                "segment_id": segment_id,
                "revision_id": item["target_revision_id"],
                "ordinal": source["ordinal"],
                "start_ms": source["media_start_ms"],
                "end_ms": source["media_end_ms"],
                "text": source["text"],
                "normalized_text": source["normalized_text"],
                "speaker_label": source["speaker_label"],
                "language": source["language"],
                "confidence_band": source["confidence_band"],
                "calibrated_probability": source["calibrated_probability"],
                "metadata_json": canonical_json(metadata),
            }
        )
    source_words = _rows(
        connection.execute(
            """
            SELECT word.*
            FROM media_local_transcript_words AS word
            JOIN media_local_transcript_segments AS segment
              ON segment.media_local_segment_id = word.media_local_segment_id
            WHERE segment.media_local_revision_id = ?
            ORDER BY segment.ordinal, word.ordinal
            """,
            (item["media_local_revision_id"],),
        )
    )
    words: list[dict[str, Any]] = []
    for source in source_words:
        segment_id = segment_ids[source["media_local_segment_id"]]
        words.append(
            {
                "word_id": stable_id("tw", segment_id, source["media_local_word_id"]),
                "segment_id": segment_id,
                "ordinal": source["ordinal"],
                "start_ms": source["media_start_ms"],
                "end_ms": source["media_end_ms"],
                "token": source["token"],
                "normalized_token": source["normalized_token"],
                "asr_log_probability": source["asr_log_probability"],
                "alignment_score": source["alignment_score"],
                "calibrated_probability": source["calibrated_probability"],
            }
        )
    return segments, words


def _copy_projection(
    connection: sqlite3.Connection,
    item: dict[str, Any],
    *,
    projection_batch_id: str,
    projected_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO processing_runs(
            processing_run_id, stage, implementation_version, model_id,
            glossary_revision_id, parameters_json, environment_json,
            random_seed, started_at, completed_at, status, error_text
        ) VALUES(?, 'media_local_transcript_identity_projection', ?, NULL, NULL,
                 ?, ?, NULL, ?, ?, 'completed', NULL)
        """,
        (
            item["projection_processing_run_id"], BRIDGE_VERSION,
            _projection_parameters(item), _projection_environment(),
            projected_at, projected_at,
        ),
    )
    connection.execute(
        """
        INSERT INTO run_inputs(
            run_input_id, processing_run_id, object_type, object_id,
            input_role, input_sha256
        ) VALUES(?, ?, 'media_local_transcript_revision', ?,
                 'identity_projection_source', ?)
        """,
        (
            stable_id(
                "rinput", item["projection_processing_run_id"],
                item["media_local_revision_id"],
            ),
            item["projection_processing_run_id"],
            item["media_local_revision_id"],
            item["source_transcript_sha256"],
        ),
    )
    connection.execute(
        """
        INSERT INTO transcript_revisions(
            revision_id, recording_id, rendition_id, processing_run_id,
            revision_kind, origin, language, glossary_revision_id,
            review_state, created_at, metadata_json
        ) VALUES(?, ?, NULL, ?, ?, ?, ?, ?, 'machine', ?, ?)
        """,
        (
            item["target_revision_id"], item["recording_id"],
            item["projection_processing_run_id"], item["revision_kind"],
            REVISION_ORIGIN, item["language"], item["glossary_revision_id"],
            projected_at, _revision_metadata(item),
        ),
    )
    segments, words = _expected_target_rows(connection, item)
    for row in segments:
        connection.execute(
            """
            INSERT INTO transcript_segments(
                segment_id, revision_id, ordinal, start_ms, end_ms, text,
                normalized_text, speaker_label, language, confidence_band,
                calibrated_probability, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tuple(row[key] for key in row),
        )
    for row in words:
        connection.execute(
            """
            INSERT INTO transcript_words(
                word_id, segment_id, ordinal, start_ms, end_ms, token,
                normalized_token, asr_log_probability, alignment_score,
                calibrated_probability
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tuple(row[key] for key in row),
        )
    connection.execute(
        """
        INSERT INTO media_local_transcript_projections(
            projection_id, projection_batch_id, projection_ordinal, variant,
            media_local_revision_id, media_local_asr_import_id,
            contextual_asr_import_id, source_receipt_sha256,
            source_transcript_sha256, input_artifact_id, input_artifact_sha256,
            input_artifact_processing_run_id, revision_id,
            projection_processing_run_id,
            normalized_media_id, normalized_media_sha256,
            parent_media_id, parent_media_sha256, parent_rendition_id,
            parent_rendition_identity_sha256,
            recording_id, qualifying_source_id, source_identity_sha256,
            qualifying_media_source_id,
            qualifying_recording_source_id, media_derivation_sha256,
            media_source_sha256, recording_source_mapping_sha256,
            public_source_metadata_observation_id,
            public_source_import_batch_id, public_source_import_observation_id,
            public_source_observed_at, public_source_evidence_sha256,
            projection_policy,
            transform_expression, boundary, mapping_basis,
            source_mapping_confidence_state, input_duration_ms,
            parent_duration_ms, recording_duration_ms,
            normalized_parent_delta_ms, normalized_recording_delta_ms,
            maximum_recording_duration_delta_ms, max_segment_end_ms,
            segment_count, word_count, target_child_identity_sha256,
            coordinate_review_state,
            wording_reviewed, speaker_identity_asserted, accuracy_claimed,
            publication_authority, projected_at, metadata_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'half_open', ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?,
                 'human_reviewed_exact_identity_projection', 0, 0, 0,
                 'none', ?, ?)
        """,
        (
            item["projection_id"], projection_batch_id,
            item["projection_ordinal"], item["variant"],
            item["media_local_revision_id"], item["media_local_asr_import_id"],
            item["contextual_asr_import_id"], item["source_receipt_sha256"],
            item["source_transcript_sha256"], item["input_artifact_id"],
            item["input_artifact_sha256"],
            item["input_artifact_processing_run_id"], item["target_revision_id"],
            item["projection_processing_run_id"], item["normalized_media_id"],
            item["normalized_media_sha256"], item["parent_media_id"],
            item["parent_media_sha256"], item["parent_rendition_id"],
            item["parent_rendition_identity_sha256"],
            item["recording_id"], item["qualifying_source_id"],
            item["source_identity_sha256"],
            item["qualifying_media_source_id"],
            item["qualifying_recording_source_id"],
            item["media_derivation_sha256"], item["media_source_sha256"],
            item["recording_source_mapping_sha256"],
            item["public_source_metadata_observation_id"],
            item["public_source_import_batch_id"],
            item["public_source_import_observation_id"],
            item["public_source_observed_at"],
            item["public_source_evidence_sha256"], POLICY_ID,
            TRANSFORM_EXPRESSION, MAPPING_BASIS,
            item["source_mapping_confidence_state"], item["input_duration_ms"],
            item["parent_duration_ms"], item["recording_duration_ms"],
            item["normalized_parent_delta_ms"],
            item["normalized_recording_delta_ms"],
            MAX_RECORDING_DURATION_DELTA_MS, item["max_segment_end_ms"],
            item["segment_count"], item["word_count"],
            item["target_child_identity_sha256"], projected_at,
            _projection_metadata(item),
        ),
    )


def _insert_projection_pair(
    connection: sqlite3.Connection,
    pair: dict[str, Any],
    *,
    projection_batch_id: str,
    created_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO media_local_transcript_projection_pairs(
            projection_pair_id, projection_batch_id, recording_ordinal,
            contextual_pair_id, contextual_diff_id, baseline_projection_id,
            contextual_projection_id, pair_state, input_equal, engine_equal,
            model_equal, window_equal, inference_equal, catalog_context_equal,
            only_glossary_job_output_differ, preferred_revision_id,
            correction_asserted, accuracy_claimed, improvement_claimed,
            human_review_claimed, automatic_merge_allowed, wording_reviewed,
            publication_authority, created_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL,
                 ?, ?, ?, ?, ?, 0, 'none', ?)
        """,
        (
            pair["projection_pair_id"], projection_batch_id,
            pair["recording_ordinal"], pair["contextual_pair_id"],
            pair["contextual_diff_id"], pair["baseline_projection_id"],
            pair["contextual_projection_id"], pair["pair_state"],
            pair["input_equal"], pair["engine_equal"], pair["model_equal"],
            pair["window_equal"], pair["inference_equal"],
            pair["catalog_context_equal"],
            pair["only_glossary_job_output_differ"],
            pair["correction_asserted"], pair["accuracy_claimed"],
            pair["improvement_claimed"], pair["human_review_claimed"],
            pair["automatic_merge_allowed"], created_at,
        ),
    )


def _require_exact_row(
    actual: sqlite3.Row | None, expected: dict[str, Any], label: str
) -> None:
    if actual is None:
        raise MediaLocalTranscriptProjectionError(f"stored {label} is missing")
    actual_dict = dict(actual)
    if actual_dict != expected:
        keys = sorted(
            key for key in set(actual_dict) | set(expected)
            if actual_dict.get(key) != expected.get(key)
        )
        raise MediaLocalTranscriptProjectionError(
            f"stored {label} differs in fields {keys}"
        )


def _expected_projection_row(
    item: dict[str, Any], batch_id: str, projected_at: str
) -> dict[str, Any]:
    return {
        "projection_id": item["projection_id"],
        "projection_batch_id": batch_id,
        "projection_ordinal": item["projection_ordinal"],
        "variant": item["variant"],
        "media_local_revision_id": item["media_local_revision_id"],
        "media_local_asr_import_id": item["media_local_asr_import_id"],
        "contextual_asr_import_id": item["contextual_asr_import_id"],
        "source_receipt_sha256": item["source_receipt_sha256"],
        "source_transcript_sha256": item["source_transcript_sha256"],
        "input_artifact_id": item["input_artifact_id"],
        "input_artifact_sha256": item["input_artifact_sha256"],
        "input_artifact_processing_run_id": item[
            "input_artifact_processing_run_id"
        ],
        "revision_id": item["target_revision_id"],
        "projection_processing_run_id": item["projection_processing_run_id"],
        "normalized_media_id": item["normalized_media_id"],
        "normalized_media_sha256": item["normalized_media_sha256"],
        "parent_media_id": item["parent_media_id"],
        "parent_media_sha256": item["parent_media_sha256"],
        "parent_rendition_id": item["parent_rendition_id"],
        "parent_rendition_identity_sha256": item[
            "parent_rendition_identity_sha256"
        ],
        "recording_id": item["recording_id"],
        "qualifying_source_id": item["qualifying_source_id"],
        "source_identity_sha256": item["source_identity_sha256"],
        "qualifying_media_source_id": item["qualifying_media_source_id"],
        "qualifying_recording_source_id": item["qualifying_recording_source_id"],
        "media_derivation_sha256": item["media_derivation_sha256"],
        "media_source_sha256": item["media_source_sha256"],
        "recording_source_mapping_sha256": item[
            "recording_source_mapping_sha256"
        ],
        "public_source_metadata_observation_id": item[
            "public_source_metadata_observation_id"
        ],
        "public_source_import_batch_id": item["public_source_import_batch_id"],
        "public_source_import_observation_id": item[
            "public_source_import_observation_id"
        ],
        "public_source_observed_at": item["public_source_observed_at"],
        "public_source_evidence_sha256": item["public_source_evidence_sha256"],
        "projection_policy": POLICY_ID,
        "transform_expression": TRANSFORM_EXPRESSION,
        "boundary": "half_open",
        "mapping_basis": MAPPING_BASIS,
        "source_mapping_confidence_state": item["source_mapping_confidence_state"],
        "input_duration_ms": item["input_duration_ms"],
        "parent_duration_ms": item["parent_duration_ms"],
        "recording_duration_ms": item["recording_duration_ms"],
        "normalized_parent_delta_ms": 0,
        "normalized_recording_delta_ms": item["normalized_recording_delta_ms"],
        "maximum_recording_duration_delta_ms": MAX_RECORDING_DURATION_DELTA_MS,
        "max_segment_end_ms": item["max_segment_end_ms"],
        "segment_count": item["segment_count"],
        "word_count": item["word_count"],
        "target_child_identity_sha256": item["target_child_identity_sha256"],
        "coordinate_review_state": "human_reviewed_exact_identity_projection",
        "wording_reviewed": 0,
        "speaker_identity_asserted": 0,
        "accuracy_claimed": 0,
        "publication_authority": "none",
        "projected_at": projected_at,
        "metadata_json": _projection_metadata(item),
    }


def _require_exact_projection_copy(
    connection: sqlite3.Connection,
    item: dict[str, Any],
    *,
    batch_id: str,
    projected_at: str,
) -> None:
    _require_exact_row(
        connection.execute(
            "SELECT * FROM media_local_transcript_projections WHERE projection_id = ?",
            (item["projection_id"],),
        ).fetchone(),
        _expected_projection_row(item, batch_id, projected_at),
        f"projection {item['projection_id']}",
    )
    run_expected = {
        "processing_run_id": item["projection_processing_run_id"],
        "stage": "media_local_transcript_identity_projection",
        "implementation_version": BRIDGE_VERSION,
        "model_id": None,
        "glossary_revision_id": None,
        "parameters_json": _projection_parameters(item),
        "environment_json": _projection_environment(),
        "random_seed": None,
        "started_at": projected_at,
        "completed_at": projected_at,
        "status": "completed",
        "error_text": None,
    }
    _require_exact_row(
        connection.execute(
            "SELECT * FROM processing_runs WHERE processing_run_id = ?",
            (item["projection_processing_run_id"],),
        ).fetchone(),
        run_expected,
        "projection processing run",
    )
    run_inputs = connection.execute(
        "SELECT * FROM run_inputs WHERE processing_run_id = ? ORDER BY run_input_id",
        (item["projection_processing_run_id"],),
    ).fetchall()
    expected_input = {
        "run_input_id": stable_id(
            "rinput", item["projection_processing_run_id"],
            item["media_local_revision_id"],
        ),
        "processing_run_id": item["projection_processing_run_id"],
        "object_type": "media_local_transcript_revision",
        "object_id": item["media_local_revision_id"],
        "input_role": "identity_projection_source",
        "input_sha256": item["source_transcript_sha256"],
    }
    if len(run_inputs) != 1:
        raise MediaLocalTranscriptProjectionError(
            "stored projection run does not have exactly one input"
        )
    _require_exact_row(run_inputs[0], expected_input, "projection run input")
    if connection.execute(
        "SELECT count(*) FROM artifacts WHERE processing_run_id = ?",
        (item["projection_processing_run_id"],),
    ).fetchone()[0]:
        raise MediaLocalTranscriptProjectionError("stored projection run has artifacts")

    revision_expected = {
        "revision_id": item["target_revision_id"],
        "recording_id": item["recording_id"],
        "rendition_id": None,
        "processing_run_id": item["projection_processing_run_id"],
        "revision_kind": item["revision_kind"],
        "origin": REVISION_ORIGIN,
        "language": item["language"],
        "glossary_revision_id": item["glossary_revision_id"],
        "review_state": "machine",
        "created_at": projected_at,
        "metadata_json": _revision_metadata(item),
    }
    _require_exact_row(
        connection.execute(
            "SELECT * FROM transcript_revisions WHERE revision_id = ?",
            (item["target_revision_id"],),
        ).fetchone(),
        revision_expected,
        "projected transcript revision",
    )
    if _actual_target_child_identity_sha256(connection, item) != item[
        "target_child_identity_sha256"
    ]:
        raise MediaLocalTranscriptProjectionError(
            "stored projected deterministic child IDs differ from approved plan"
        )
    expected_segments, expected_words = _expected_target_rows(connection, item)
    actual_segments = _rows(
        connection.execute(
            "SELECT * FROM transcript_segments WHERE revision_id = ? ORDER BY ordinal",
            (item["target_revision_id"],),
        )
    )
    actual_words = _rows(
        connection.execute(
            """
            SELECT word.* FROM transcript_words AS word
            JOIN transcript_segments AS segment ON segment.segment_id = word.segment_id
            WHERE segment.revision_id = ? ORDER BY segment.ordinal, word.ordinal
            """,
            (item["target_revision_id"],),
        )
    )
    if actual_segments != expected_segments or actual_words != expected_words:
        raise MediaLocalTranscriptProjectionError(
            "stored projected segment/word rows differ from exact deterministic copy"
        )
    if connection.execute(
        "SELECT count(*) FROM transcript_revision_parents WHERE revision_id = ?",
        (item["target_revision_id"],),
    ).fetchone()[0]:
        raise MediaLocalTranscriptProjectionError("projected transcript has a parent")
    predated = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM publication_decisions
           WHERE object_type = 'transcript_revision' AND object_id = ?
             AND julianday(decided_at) < julianday(?))
          +
          (SELECT count(*) FROM publication_gate_decisions
           WHERE object_type = 'transcript_revision' AND object_id = ?
             AND julianday(decided_at) < julianday(?))
        """,
        (item["target_revision_id"], projected_at,
         item["target_revision_id"], projected_at),
    ).fetchone()[0]
    if predated:
        raise MediaLocalTranscriptProjectionError(
            "projected transcript has a decision predating projection"
        )


def _expected_pair_row(
    pair: dict[str, Any], batch_id: str, created_at: str
) -> dict[str, Any]:
    return {
        "projection_pair_id": pair["projection_pair_id"],
        "projection_batch_id": batch_id,
        "recording_ordinal": pair["recording_ordinal"],
        "contextual_pair_id": pair["contextual_pair_id"],
        "contextual_diff_id": pair["contextual_diff_id"],
        "baseline_projection_id": pair["baseline_projection_id"],
        "contextual_projection_id": pair["contextual_projection_id"],
        "pair_state": pair["pair_state"],
        "input_equal": pair["input_equal"],
        "engine_equal": pair["engine_equal"],
        "model_equal": pair["model_equal"],
        "window_equal": pair["window_equal"],
        "inference_equal": pair["inference_equal"],
        "catalog_context_equal": pair["catalog_context_equal"],
        "only_glossary_job_output_differ": pair["only_glossary_job_output_differ"],
        "preferred_revision_id": None,
        "correction_asserted": 0,
        "accuracy_claimed": 0,
        "improvement_claimed": 0,
        "human_review_claimed": 0,
        "automatic_merge_allowed": 0,
        "wording_reviewed": 0,
        "publication_authority": "none",
        "created_at": created_at,
    }


def require_exact_projection_batch(
    connection: sqlite3.Connection, projection_batch_id: str
) -> dict[str, Any]:
    """Deeply replay one stored batch; aggregate equality is never sufficient."""

    batch = connection.execute(
        "SELECT * FROM media_local_transcript_projection_batches WHERE projection_batch_id = ?",
        (projection_batch_id,),
    ).fetchone()
    if batch is None:
        raise MediaLocalTranscriptProjectionError(
            f"unknown projection batch {projection_batch_id}"
        )
    try:
        payload = json.loads(batch["plan_json"], object_pairs_hook=_reject_duplicate_keys)
        stored_manifest = json.loads(
            batch["manifest_json"], object_pairs_hook=_reject_duplicate_keys
        )
        stored_raw_manifest = json.loads(
            batch["manifest_raw_json"],
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda value: (_ for _ in ()).throw(
                MediaLocalTranscriptProjectionError(
                    f"stored raw manifest contains non-finite number {value}"
                )
            ),
        )
    except (TypeError, json.JSONDecodeError) as error:
        raise MediaLocalTranscriptProjectionError(
            "stored projection JSON is invalid"
        ) from error
    raw_manifest_bytes = batch["manifest_raw_json"].encode("utf-8")
    payload = _object(payload, "stored projection plan")
    stored_manifest = _object(stored_manifest, "stored normalized manifest")
    _exact_keys(
        payload,
        "stored projection plan",
        {
            "schema_version", "policy_id", "bridge_version",
            "manifest_core_sha256", "manifest_core", "transform",
            "projection_count", "recording_count", "pair_count",
            "segment_count", "word_count", "projections", "pairs", "safety",
        },
    )
    embedded_manifest = _parse_media_local_transcript_projection_manifest(
        raw_manifest_bytes, Path(batch["manifest_uri"])
    )
    if (
        _sha256_bytes(raw_manifest_bytes) != batch["manifest_raw_sha256"]
        or len(raw_manifest_bytes) != batch["manifest_byte_count"]
        or embedded_manifest.raw_sha256 != batch["manifest_raw_sha256"]
        or embedded_manifest.canonical_sha256
            != batch["manifest_canonical_sha256"]
        or embedded_manifest.core_sha256 != batch["manifest_core_sha256"]
        or embedded_manifest.value != stored_manifest
        or canonical_json(stored_raw_manifest) != batch["manifest_json"]
        or canonical_json(payload) != batch["plan_json"]
        or _sha256_json(payload) != batch["plan_sha256"]
        or canonical_json(stored_manifest) != batch["manifest_json"]
        or _sha256_json(stored_manifest) != batch["manifest_canonical_sha256"]
    ):
        raise MediaLocalTranscriptProjectionError(
            "stored projection plan or manifest canonical digest differs"
        )
    manifest_core = embedded_manifest.core
    approval = embedded_manifest.value["review"]["approved_plan_sha256"]
    if (
        approval != batch["plan_sha256"]
        or _sha256_json(manifest_core) != batch["manifest_core_sha256"]
        or payload.get("manifest_core") != manifest_core
        or payload.get("manifest_core_sha256") != batch["manifest_core_sha256"]
        or payload.get("schema_version") != PLAN_SCHEMA_VERSION
        or payload.get("policy_id") != POLICY_ID
        or payload.get("bridge_version") != BRIDGE_VERSION
    ):
        raise MediaLocalTranscriptProjectionError(
            "stored manifest approval/core does not bind the exact plan"
        )
    review = manifest_core["review"]
    _require_human_coordinate_reviewer(
        connection, review["reviewer_id"], review["reviewed_at"]
    )
    if (
        batch["projection_batch_id"]
        != stable_id("mltpbatch", POLICY_ID, batch["plan_sha256"])
        or batch["import_batch_id"]
        != stable_id("imp", IMPORTER_NAME, batch["plan_sha256"])
        or batch["manifest_id"] != manifest_core["manifest_id"]
        or batch["schema_version"] != 1
        or batch["policy_id"] != POLICY_ID
        or batch["reviewer_id"] != review["reviewer_id"]
        or batch["reviewed_at"] != review["reviewed_at"]
        or batch["review_basis"] != review["basis"]
        or batch["coordinate_attestation"] != review["coordinate_attestation"]
        or batch["manifest_byte_count"] <= 0
        or not isinstance(batch["manifest_raw_json"], str)
        or not batch["manifest_uri"]
        or not SHA256_RE.fullmatch(batch["manifest_raw_sha256"])
        or batch["projection_count"] != payload.get("projection_count")
        or batch["recording_count"] != payload.get("recording_count")
        or batch["pair_count"] != payload.get("pair_count")
        or batch["segment_count"] != payload.get("segment_count")
        or batch["word_count"] != payload.get("word_count")
        or not MIN_RECORDINGS <= batch["recording_count"] <= MAX_RECORDINGS
        or batch["projection_count"] != batch["recording_count"] * 2
        or batch["pair_count"] != batch["recording_count"]
    ):
        raise MediaLocalTranscriptProjectionError(
            "stored projection batch header differs from exact plan"
        )
    statistics = {
        "identity_assertions": 0,
        "pairs": batch["pair_count"],
        "projection_batches": 1,
        "projections": batch["projection_count"],
        "publication_decisions": 0,
        "publication_gate_decisions": 0,
        "recordings": batch["recording_count"],
        "transcript_revisions": batch["projection_count"],
        "transcript_segments": batch["segment_count"],
        "transcript_words": batch["word_count"],
    }
    import_expected = {
        "import_batch_id": batch["import_batch_id"],
        "importer_name": IMPORTER_NAME,
        "importer_version": BRIDGE_VERSION,
        "input_sha256": batch["manifest_raw_sha256"],
        "source_snapshot_date": None,
        "started_at": batch["applied_at"],
        "completed_at": batch["applied_at"],
        "status": "completed",
        "statistics_json": canonical_json(statistics),
    }
    _require_exact_row(
        connection.execute(
            "SELECT * FROM import_batches WHERE import_batch_id = ?",
            (batch["import_batch_id"],),
        ).fetchone(),
        import_expected,
        "projection import batch",
    )

    plan_projections = payload.get("projections")
    plan_pairs = payload.get("pairs")
    selection = manifest_core.get("selection")
    if (
        not isinstance(plan_projections, list)
        or not isinstance(plan_pairs, list)
        or not isinstance(selection, list)
    ):
        raise MediaLocalTranscriptProjectionError(
            "stored projection plan arrays are invalid"
        )
    plan_projections = [
        _object(item, f"stored plan projection[{index}]")
        for index, item in enumerate(plan_projections)
    ]
    plan_pairs = [
        _object(item, f"stored plan pair[{index}]")
        for index, item in enumerate(plan_pairs)
    ]
    rebuilt: dict[tuple[int, str], dict[str, Any]] = {}
    for item in plan_projections:
        ordinal = item.get("selection_ordinal")
        variant = item.get("variant")
        if type(ordinal) is not int or not 0 <= ordinal < len(selection) or variant not in {"raw", "contextual"}:
            raise MediaLocalTranscriptProjectionError("stored projection selector is invalid")
        fresh = _projection_source(
            connection,
            selection[ordinal],
            variant=variant,
            historical_item=item,
        )
        _exact_keys(
            item,
            f"stored plan projection[{ordinal}:{variant}]",
            set(fresh),
        )
        if fresh != item:
            raise MediaLocalTranscriptProjectionError(
                "stored plan projection differs from sealed exact source replay"
            )
        if (ordinal, variant) in rebuilt:
            raise MediaLocalTranscriptProjectionError(
                "stored plan repeats a projection selector"
            )
        rebuilt[(ordinal, variant)] = fresh
        _require_exact_projection_copy(
            connection, item, batch_id=projection_batch_id,
            projected_at=batch["applied_at"],
        )
    if len(rebuilt) != len(selection) * 2 or any(
        (ordinal, variant) not in rebuilt
        for ordinal in range(len(selection)) for variant in ("raw", "contextual")
    ):
        raise MediaLocalTranscriptProjectionError(
            "stored plan does not contain one complete pair per recording"
        )
    expected_projection_ids = {item["projection_id"] for item in plan_projections}
    actual_projection_ids = {
        row[0]
        for row in connection.execute(
            "SELECT projection_id FROM media_local_transcript_projections WHERE projection_batch_id = ?",
            (projection_batch_id,),
        )
    }
    if actual_projection_ids != expected_projection_ids:
        raise MediaLocalTranscriptProjectionError(
            "stored projection ID set differs from exact plan"
        )

    rebuilt_pairs: dict[int, dict[str, Any]] = {}
    for pair in plan_pairs:
        ordinal = pair.get("recording_ordinal")
        if type(ordinal) is not int or not 0 <= ordinal < len(selection):
            raise MediaLocalTranscriptProjectionError("stored pair ordinal is invalid")
        fresh = _projection_pair(
            connection, selection[ordinal], rebuilt[(ordinal, "raw")],
            rebuilt[(ordinal, "contextual")],
        )
        _exact_keys(pair, f"stored plan pair[{ordinal}]", set(fresh))
        if fresh != pair:
            raise MediaLocalTranscriptProjectionError(
                "stored plan pair differs from sealed exact pair/diff replay"
            )
        if ordinal in rebuilt_pairs:
            raise MediaLocalTranscriptProjectionError(
                "stored plan repeats a projection-pair ordinal"
            )
        rebuilt_pairs[ordinal] = fresh
        _require_exact_row(
            connection.execute(
                "SELECT * FROM media_local_transcript_projection_pairs WHERE projection_pair_id = ?",
                (pair["projection_pair_id"],),
            ).fetchone(),
            _expected_pair_row(pair, projection_batch_id, batch["applied_at"]),
            "projection pair",
        )
    if set(rebuilt_pairs) != set(range(len(selection))):
        raise MediaLocalTranscriptProjectionError(
            "stored plan does not contain one pair per recording ordinal"
        )
    expected_pair_ids = {pair["projection_pair_id"] for pair in plan_pairs}
    actual_pair_ids = {
        row[0]
        for row in connection.execute(
            "SELECT projection_pair_id FROM media_local_transcript_projection_pairs WHERE projection_batch_id = ?",
            (projection_batch_id,),
        )
    }
    if actual_pair_ids != expected_pair_ids:
        raise MediaLocalTranscriptProjectionError(
            "stored projection pair ID set differs from exact plan"
        )
    historical_payload = _assembled_plan_payload(
        connection,
        embedded_manifest,
        [
            rebuilt[(ordinal, variant)]
            for ordinal in range(len(selection))
            for variant in ("raw", "contextual")
        ],
        [rebuilt_pairs[ordinal] for ordinal in range(len(selection))],
    )
    if historical_payload != payload:
        raise MediaLocalTranscriptProjectionError(
            "stored plan differs from exact historical evidence reconstruction"
        )
    forbidden_decisions = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM publication_decisions
           WHERE object_type IN (
             'media_local_transcript_projection_batch',
             'media_local_transcript_projection',
             'media_local_transcript_projection_pair'))
          +
          (SELECT count(*) FROM publication_gate_decisions
           WHERE object_type IN (
             'media_local_transcript_projection_batch',
             'media_local_transcript_projection',
             'media_local_transcript_projection_pair'))
        """
    ).fetchone()[0]
    if forbidden_decisions:
        raise MediaLocalTranscriptProjectionError(
            "coordinate-projection evidence has a forbidden publication decision"
        )
    return {
        **payload,
        "plan_sha256": batch["plan_sha256"],
        "projection_batch_id": projection_batch_id,
        "import_batch_id": batch["import_batch_id"],
        "applied_at": batch["applied_at"],
    }


def apply_media_local_transcript_projection_plan(
    connection: sqlite3.Connection,
    manifest_path: str | Path,
    *,
    expected_plan_sha256: str,
) -> dict[str, Any]:
    """Rebuild and atomically apply one human-approved manifest and digest."""

    if not SHA256_RE.fullmatch(expected_plan_sha256):
        raise MediaLocalTranscriptProjectionError(
            "expected plan SHA-256 must be 64 lowercase hexadecimal characters"
        )
    with transaction(connection):
        manifest = load_media_local_transcript_projection_manifest(manifest_path)
        approval = manifest.value["review"]["approved_plan_sha256"]
        if approval is None:
            raise MediaLocalTranscriptProjectionError(
                "reviewed manifest must include approved_plan_sha256 before apply"
            )
        if approval != expected_plan_sha256:
            raise MediaLocalTranscriptProjectionError(
                "expected plan digest differs from the manifest approval"
            )
        existing = connection.execute(
            "SELECT * FROM media_local_transcript_projection_batches WHERE plan_sha256 = ?",
            (approval,),
        ).fetchone()
        if existing is not None:
            if (
                existing["manifest_raw_sha256"] != manifest.raw_sha256
                or existing["manifest_canonical_sha256"] != manifest.canonical_sha256
                or existing["manifest_core_sha256"] != manifest.core_sha256
                or existing["manifest_byte_count"] != manifest.byte_count
                or existing["manifest_raw_json"] != manifest.raw_json
                or existing["manifest_id"] != manifest.value["manifest_id"]
            ):
                raise MediaLocalTranscriptProjectionError(
                    "existing plan digest is bound to different manifest bytes"
                )
            result = require_exact_projection_batch(
                connection, existing["projection_batch_id"]
            )
            return {**result, "status": "already_applied"}

        # A new batch must still satisfy the mutable current catalog projection.
        # Historical idempotency above deliberately replays only sealed evidence.
        plan_payload = _plan_payload(connection, manifest)
        plan_sha256 = _sha256_json(plan_payload)
        if plan_sha256 != expected_plan_sha256:
            raise MediaLocalTranscriptProjectionError(
                "projection plan changed or differs from human-approved digest"
            )
        batch_id = stable_id("mltpbatch", POLICY_ID, plan_sha256)

        generated_ids: list[tuple[str, str]] = []
        for item in plan_payload["projections"]:
            generated_ids.extend(
                (
                    ("media_local_transcript_projections", item["projection_id"]),
                    ("transcript_revisions", item["target_revision_id"]),
                    ("processing_runs", item["projection_processing_run_id"]),
                )
            )
            decisions = connection.execute(
                """
                SELECT
                  (SELECT count(*) FROM publication_decisions
                   WHERE object_type = 'transcript_revision' AND object_id = ?)
                  +
                  (SELECT count(*) FROM publication_gate_decisions
                   WHERE object_type = 'transcript_revision' AND object_id = ?)
                """,
                (item["target_revision_id"], item["target_revision_id"]),
            ).fetchone()[0]
            if decisions:
                raise MediaLocalTranscriptProjectionError(
                    "deterministic target revision has preexisting decisions"
                )
        primary_keys = {
            "media_local_transcript_projections": "projection_id",
            "transcript_revisions": "revision_id",
            "processing_runs": "processing_run_id",
        }
        for table, object_id in generated_ids:
            if connection.execute(
                f"SELECT 1 FROM {table} WHERE {primary_keys[table]} = ?", (object_id,)
            ).fetchone() is not None:
                raise MediaLocalTranscriptProjectionError(
                    f"deterministic projection object already exists without this batch: {object_id}"
                )

        applied_at = utc_now()
        reviewed_at = manifest.value["review"]["reviewed_at"]
        if datetime.strptime(applied_at, "%Y-%m-%dT%H:%M:%SZ") < datetime.strptime(
            reviewed_at, "%Y-%m-%dT%H:%M:%SZ"
        ):
            raise MediaLocalTranscriptProjectionError(
                "projection cannot be applied before reviewed_at"
            )
        import_batch_id = stable_id("imp", IMPORTER_NAME, plan_sha256)
        statistics = {
            "identity_assertions": 0,
            "pairs": plan_payload["pair_count"],
            "projection_batches": 1,
            "projections": plan_payload["projection_count"],
            "publication_decisions": 0,
            "publication_gate_decisions": 0,
            "recordings": plan_payload["recording_count"],
            "transcript_revisions": plan_payload["projection_count"],
            "transcript_segments": plan_payload["segment_count"],
            "transcript_words": plan_payload["word_count"],
        }
        connection.execute(
            """
            INSERT INTO import_batches(
                import_batch_id, importer_name, importer_version, input_sha256,
                source_snapshot_date, started_at, completed_at, status,
                statistics_json
            ) VALUES(?, ?, ?, ?, NULL, ?, ?, 'completed', ?)
            """,
            (
                import_batch_id, IMPORTER_NAME, BRIDGE_VERSION,
                manifest.raw_sha256, applied_at, applied_at,
                canonical_json(statistics),
            ),
        )
        for item in plan_payload["projections"]:
            _copy_projection(
                connection, item, projection_batch_id=batch_id,
                projected_at=applied_at,
            )
        for pair in plan_payload["pairs"]:
            _insert_projection_pair(
                connection, pair, projection_batch_id=batch_id,
                created_at=applied_at,
            )
        # The batch header closes the deferred child graph and is inserted last.
        connection.execute(
            """
            INSERT INTO media_local_transcript_projection_batches(
                projection_batch_id, import_batch_id, manifest_id, manifest_uri,
                manifest_raw_sha256, manifest_canonical_sha256,
                manifest_core_sha256, manifest_byte_count, manifest_raw_json,
                manifest_json,
                plan_sha256, schema_version, policy_id, plan_json, reviewer_id,
                reviewed_at, review_basis, coordinate_attestation,
                projection_count, recording_count, pair_count, segment_count,
                word_count, applied_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                batch_id, import_batch_id, manifest.value["manifest_id"],
                str(manifest.path), manifest.raw_sha256,
                manifest.canonical_sha256, manifest.core_sha256,
                manifest.byte_count, manifest.raw_json,
                canonical_json(manifest.value), plan_sha256,
                POLICY_ID, canonical_json(plan_payload),
                manifest.value["review"]["reviewer_id"], reviewed_at,
                manifest.value["review"]["basis"], COORDINATE_ATTESTATION,
                plan_payload["projection_count"], plan_payload["recording_count"],
                plan_payload["pair_count"], plan_payload["segment_count"],
                plan_payload["word_count"], applied_at,
            ),
        )
        result = require_exact_projection_batch(connection, batch_id)
        return {**result, "status": "applied"}
