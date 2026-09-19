"""ASR-blind, proposal-only interval routing for transcript evaluation.

This module deliberately sits before the human interval-freeze workflow.  It
reads only sealed media-preprocess results (probe/scene/silence metadata), optional
sealed local-window results, and a query-only catalogue snapshot.  It
never accepts transcript/ASR results, never writes an output file, and never
turns a machine-routed interval into reference data.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import stat
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote, unquote

from .validation import (
    ContractError,
    _array,
    _boolean,
    _canonical_bytes,
    _choice,
    _constant,
    _expected_rendition_id,
    _id,
    _integer,
    _object,
    _sha256,
    _stable_id,
    _string,
    _timestamp,
    _verify_manifest_digest,
    canonical_manifest_sha256,
    validate_candidate_cohort,
)


MAX_JSON_BYTES = 64 * 1024 * 1024
MAX_RECORDINGS = 12
MAX_WINDOWS_PER_RECORDING = 256
MAX_TOTAL_WINDOWS = 384
MAX_ROUTING_ROWS = 200_000
MAX_INTERVAL_DURATION_MS = 120_000
MAX_INTERVALS_PER_RECORDING = 32
MAX_TOTAL_INTERVALS = MAX_RECORDINGS * MAX_INTERVALS_PER_RECORDING
MAX_TOTAL_DURATION_MS = 8 * 60 * 60 * 1000
MAX_MEDIA_DURATION_MS = 48 * 60 * 60 * 1000
ALLOWED_PREPROCESS_ARTIFACT_KINDS = {
    "ffprobe_normalized_json",
    "audio_16khz_mono_flac",
    "low_resolution_cfr_proxy",
    "scene_silence_routing_json",
}
FORBIDDEN_INPUT_COMPONENT = re.compile(
    r"(?:^|[._-])(?:asr|speech[._-]?to[._-]?text|transcripts?|transcriptions?|"
    r"subtitles?|captions?|hypotheses?|references?)(?:$|[._-])",
    re.IGNORECASE,
)
FORBIDDEN_DATA_KEY = re.compile(
    r"(?:^|[._-])(?:asr[._-](?:result|output)|speech[._-]?to[._-]?text|"
    r"transcripts?|transcriptions?|subtitles?|captions?|hypotheses?|utterances?|"
    r"word[._-]segments|transcript[._-](?:segments|words)|"
    r"reference[._-](?:text|segments?|transcripts?)|tokens?)(?:$|[._-])",
    re.IGNORECASE,
)
ROUTING_WARNING = (
    "Routing values are machine-generated workload suggestions, not content findings. "
    "They do not identify a speaker or establish that a video is single-speaker."
)
LOCAL_WINDOW_RENDITION_KIND = re.compile(
    r"local_window:(window_audio_16khz_mono_flac|"
    r"window_low_resolution_cfr_proxy):(windowbundle_[0-9a-f]{32}):"
    r"(window_[0-9]{6})"
)
PREPROCESS_IMPORTER_NAME = "media_preprocess_result_v1"


DEFAULT_POLICY: dict[str, int] = {
    "interval_duration_ms": 30_000,
    "minimum_interval_duration_ms": 10_000,
    "max_intervals_per_recording": 12,
    "max_total_intervals": 120,
    "max_total_duration_ms": 3_600_000,
    "minimum_gap_ms": 5_000,
    "periodic_stride_ms": 120_000,
    "max_silence_fraction_millionths": 750_000,
}


def _fail(path: str, message: str) -> None:
    raise ContractError(f"{path}: {message}")


def _media_id(value: object, path: str) -> str:
    digest = _sha256(_string(value, path).removeprefix("media_sha256_"), path)
    expected = f"media_sha256_{digest}"
    if value != expected:
        _fail(path, f"must equal {expected!r}")
    return expected


def _nullable_sha256(value: object, path: str) -> str | None:
    if value is None:
        return None
    return _sha256(value, path)


def _nullable_string(value: object, path: str) -> str | None:
    if value is None:
        return None
    return _string(value, path)


def _number(value: object, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail(path, "must be a finite number")
    number = float(value)
    if not (float("-inf") < number < float("inf")):
        _fail(path, "must be a finite number")
    return number


def _producer_stable_id(prefix: str, *parts: object) -> str:
    """Recompute IDs emitted by the media-preprocess producer contract."""

    return f"{prefix}_{hashlib.sha256(_canonical_bytes(list(parts))).hexdigest()[:32]}"


def _preprocess_import_envelope_sha256(value: dict[str, Any]) -> str:
    """Reproduce ``result_importers._result_file`` for a parsed JSON envelope."""

    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _preprocess_import_batch_id(import_envelope_sha256: str) -> str:
    return _stable_id(
        "imp", PREPROCESS_IMPORTER_NAME, import_envelope_sha256
    )


def _bounded_integer(
    value: object, path: str, *, minimum: int = 0, maximum: int
) -> int:
    parsed = _integer(value, path, minimum=minimum)
    if parsed > maximum:
        _fail(path, f"must be <= {maximum}")
    return parsed


def _reject_forbidden_input_path(path: Path, label: str) -> None:
    for component in path.parts:
        if FORBIDDEN_INPUT_COMPONENT.search(component):
            _fail(label, "ASR/transcript/reference-shaped input paths are forbidden")


def _reject_forbidden_data_keys(value: object, label: str) -> None:
    """Reject result fields that could turn a metadata envelope into ASR input.

    The sole key named ``asr`` allowed by the preprocessing contract is the
    workload-routing suggestion at ``routing.routing_candidates.asr``.  Its value
    is a process/skip enum, never system output.
    """

    def walk(item: object, path: tuple[str, ...]) -> None:
        if isinstance(item, dict):
            for raw_key, child in item.items():
                key = str(raw_key)
                next_path = (*path, key)
                if key.lower() == "asr":
                    if next_path != ("routing", "routing_candidates", "asr"):
                        _fail(label, f"forbidden ASR field at {'.'.join(next_path)}")
                elif FORBIDDEN_DATA_KEY.search(key):
                    _fail(label, f"forbidden transcript/ASR field at {'.'.join(next_path)}")
                walk(child, next_path)
        elif isinstance(item, list):
            for index, child in enumerate(item):
                walk(child, (*path, str(index)))

    walk(value, ())


def _read_pinned_json(
    path_text: object, expected_sha256: object, label: str
) -> tuple[dict[str, Any], str, Path]:
    text = _string(path_text, f"{label}.path")
    path = Path(text)
    if not path.is_absolute():
        _fail(f"{label}.path", "must be an absolute local path")
    _reject_forbidden_input_path(path, f"{label}.path")
    try:
        before = path.lstat()
    except OSError as error:
        _fail(f"{label}.path", f"cannot stat input: {error}")
    if (
        stat.S_ISLNK(before.st_mode)
        or not stat.S_ISREG(before.st_mode)
        or before.st_mode & 0o222
    ):
        _fail(
            f"{label}.path",
            "must be a sealed read-only non-symlink regular file",
        )
    if before.st_size > MAX_JSON_BYTES:
        _fail(f"{label}.path", f"exceeds the {MAX_JSON_BYTES}-byte metadata limit")
    try:
        body = path.read_bytes()
        after = path.lstat()
    except OSError as error:
        _fail(f"{label}.path", f"cannot read input: {error}")
    fingerprint_before = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
        before.st_mode,
    )
    fingerprint_after = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
        after.st_mode,
    )
    if (
        fingerprint_before != fingerprint_after
        or stat.S_ISLNK(after.st_mode)
        or not stat.S_ISREG(after.st_mode)
        or after.st_mode & 0o222
    ):
        _fail(f"{label}.path", "changed while being read")
    observed = hashlib.sha256(body).hexdigest()
    declared = _sha256(expected_sha256, f"{label}.sha256")
    if observed != declared:
        _fail(f"{label}.sha256", f"file digest mismatch; observed {observed}")

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, item in pairs:
            if key in output:
                raise ContractError(f"{label}: duplicate JSON object key {key!r}")
            output[key] = item
        return output

    def reject_constant(value: str) -> None:
        raise ContractError(f"{label}: non-finite JSON number {value!r} is forbidden")

    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        _fail(label, f"cannot parse JSON: {error}")
    if not isinstance(value, dict):
        _fail(label, "top level must be an object")
    return value, observed, path


def _validate_preprocess_catalog_records(
    value: object,
    request_row: dict[str, Any],
    run: dict[str, Any],
    run_id: str,
    path: str,
) -> dict[str, list[dict[str, Any]]]:
    """Validate producer catalogue rows before matching them query-only.

    Older sealed preprocessing results may contain empty arrays, so emptiness is
    retained as a compatibility case. Every row that is present must have an
    exact media-preprocess producer shape; `_catalog_binding` then matches it to
    the read-only catalogue.
    """

    records = _object(
        value,
        path,
        {
            "media_objects",
            "media_locations",
            "processing_runs",
            "run_inputs",
            "artifacts",
            "media_derivations",
        },
    )
    limits = {
        "media_objects": 3,
        "media_locations": 3,
        "processing_runs": 1,
        "run_inputs": 1,
        "artifacts": 4,
        "media_derivations": 2,
    }
    arrays: dict[str, list[Any]] = {}
    for key, maximum in limits.items():
        rows = _array(records[key], f"{path}.{key}")
        if len(rows) > maximum:
            _fail(f"{path}.{key}", f"must contain at most {maximum} rows")
        arrays[key] = rows

    processing_runs: list[dict[str, Any]] = []
    for index, raw in enumerate(arrays["processing_runs"]):
        row_path = f"{path}.processing_runs[{index}]"
        row = _object(
            raw,
            row_path,
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
        _id(row["processing_run_id"], f"{row_path}.processing_run_id")
        _constant(row["stage"], "media_preprocess", f"{row_path}.stage")
        _string(row["implementation_version"], f"{row_path}.implementation_version")
        if not isinstance(row["parameters_json"], dict) or not isinstance(
            row["environment_json"], dict
        ):
            _fail(row_path, "parameters_json and environment_json must be objects")
        _timestamp(row["started_at"], f"{row_path}.started_at")
        _timestamp(row["completed_at"], f"{row_path}.completed_at")
        _constant(row["status"], "completed", f"{row_path}.status")
        if row != run:
            _fail(row_path, "must exactly equal the enclosing processing_run")
        processing_runs.append(dict(row))

    run_inputs: list[dict[str, Any]] = []
    for index, raw in enumerate(arrays["run_inputs"]):
        row_path = f"{path}.run_inputs[{index}]"
        row = _object(
            raw,
            row_path,
            {
                "run_input_id",
                "processing_run_id",
                "object_type",
                "object_id",
                "input_role",
                "input_sha256",
            },
        )
        _id(row["run_input_id"], f"{row_path}.run_input_id")
        _constant(row["processing_run_id"], run_id, f"{row_path}.processing_run_id")
        _constant(row["object_type"], "media", f"{row_path}.object_type")
        _constant(
            row["object_id"], request_row["analysis_media_id"], f"{row_path}.object_id"
        )
        _constant(row["input_role"], "source_media", f"{row_path}.input_role")
        _constant(
            row["input_sha256"],
            request_row["analysis_media_sha256"],
            f"{row_path}.input_sha256",
        )
        expected_run_input_id = _producer_stable_id(
            "run_input", run_id, request_row["analysis_media_id"], "source_media"
        )
        _constant(
            row["run_input_id"], expected_run_input_id, f"{row_path}.run_input_id"
        )
        run_inputs.append(dict(row))

    media_objects: list[dict[str, Any]] = []
    media_ids: set[str] = set()
    for index, raw in enumerate(arrays["media_objects"]):
        row_path = f"{path}.media_objects[{index}]"
        row = _object(
            raw,
            row_path,
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
        media_id = _media_id(row["media_id"], f"{row_path}.media_id")
        digest = _sha256(row["sha256"], f"{row_path}.sha256")
        if media_id != f"media_sha256_{digest}":
            _fail(f"{row_path}.media_id", "must be derived from sha256")
        if media_id in media_ids:
            _fail(f"{row_path}.media_id", "must be unique")
        media_ids.add(media_id)
        _integer(row["byte_count"], f"{row_path}.byte_count", minimum=0)
        _choice(row["media_kind"], {"video", "audio", "other"}, f"{row_path}.media_kind")
        _nullable_string(row["mime_type"], f"{row_path}.mime_type")
        _nullable_string(row["container"], f"{row_path}.container")
        if row["duration_ms"] is not None:
            _bounded_integer(
                row["duration_ms"],
                f"{row_path}.duration_ms",
                maximum=MAX_MEDIA_DURATION_MS,
            )
        if not isinstance(row["ffprobe_json"], dict):
            _fail(f"{row_path}.ffprobe_json", "must be an object")
        _timestamp(row["first_cataloged_at"], f"{row_path}.first_cataloged_at")
        _constant(row["integrity_state"], "verified", f"{row_path}.integrity_state")
        probe_media = row["ffprobe_json"].get("media")
        if probe_media is not None:
            if not isinstance(probe_media, dict):
                _fail(f"{row_path}.ffprobe_json.media", "must be an object")
            for key, expected in (
                ("media_id", media_id),
                ("sha256", digest),
                ("byte_count", row["byte_count"]),
            ):
                if probe_media.get(key) != expected:
                    _fail(
                        f"{row_path}.ffprobe_json.media.{key}",
                        "does not match its media row",
                    )
        media_objects.append(dict(row))

    derivations: list[dict[str, Any]] = []
    derived_ids: set[str] = set()
    for index, raw in enumerate(arrays["media_derivations"]):
        row_path = f"{path}.media_derivations[{index}]"
        row = _object(
            raw,
            row_path,
            {
                "child_media_id",
                "parent_media_id",
                "derivation_kind",
                "processing_run_id",
                "metadata_json",
            },
        )
        child = _media_id(row["child_media_id"], f"{row_path}.child_media_id")
        _constant(
            row["parent_media_id"],
            request_row["analysis_media_id"],
            f"{row_path}.parent_media_id",
        )
        _constant(row["processing_run_id"], run_id, f"{row_path}.processing_run_id")
        kind = _choice(
            row["derivation_kind"],
            {"audio_normalization_16khz_mono_flac", "low_resolution_cfr_proxy"},
            f"{row_path}.derivation_kind",
        )
        if child == request_row["analysis_media_id"] or child in derived_ids:
            _fail(f"{row_path}.child_media_id", "must be a unique derived media object")
        derived_ids.add(child)
        metadata = row["metadata_json"]
        if kind == "audio_normalization_16khz_mono_flac":
            allowed_audio_shapes = (
                {"sample_rate_hz", "channels"},
                {"sample_rate_hz", "channels", "sample_format"},
            )
            if not isinstance(metadata, dict) or set(metadata) not in allowed_audio_shapes:
                _fail(f"{row_path}.metadata_json", "has an unsupported audio derivation shape")
            _constant(metadata["sample_rate_hz"], 16_000, f"{row_path}.metadata_json.sample_rate_hz")
            _constant(metadata["channels"], 1, f"{row_path}.metadata_json.channels")
            if "sample_format" in metadata:
                _constant(metadata["sample_format"], "s16", f"{row_path}.metadata_json.sample_format")
        else:
            metadata = _object(
                metadata,
                f"{row_path}.metadata_json",
                {"width", "height", "fps"},
            )
            _integer(metadata["width"], f"{row_path}.metadata_json.width", minimum=2)
            _integer(metadata["height"], f"{row_path}.metadata_json.height", minimum=2)
            _integer(metadata["fps"], f"{row_path}.metadata_json.fps", minimum=1)
        derivations.append(dict(row))

    if media_objects:
        if request_row["analysis_media_id"] not in media_ids:
            _fail(f"{path}.media_objects", "must include the analysis media object")
        if not derived_ids.issubset(media_ids):
            _fail(f"{path}.media_derivations", "child media must be present in media_objects")
        allowed_media_ids = {request_row["analysis_media_id"], *derived_ids}
        if not media_ids.issubset(allowed_media_ids):
            _fail(f"{path}.media_objects", "contains media unrelated to the preprocessing run")
    elif derivations:
        _fail(f"{path}.media_derivations", "requires declared media_objects")

    locations: list[dict[str, Any]] = []
    location_ids: set[str] = set()
    for index, raw in enumerate(arrays["media_locations"]):
        row_path = f"{path}.media_locations[{index}]"
        row = _object(
            raw,
            row_path,
            {
                "media_location_id",
                "media_id",
                "storage_uri",
                "storage_class",
                "verified_at",
                "is_primary",
            },
        )
        location_id = _id(row["media_location_id"], f"{row_path}.media_location_id")
        if location_id in location_ids:
            _fail(f"{row_path}.media_location_id", "must be unique")
        location_ids.add(location_id)
        location_media_id = _media_id(row["media_id"], f"{row_path}.media_id")
        if media_objects and location_media_id not in media_ids:
            _fail(f"{row_path}.media_id", "must reference a declared media object")
        storage_uri = _string(row["storage_uri"], f"{row_path}.storage_uri")
        if not storage_uri.startswith("file://"):
            _fail(f"{row_path}.storage_uri", "must be an absolute file URI")
        _choice(
            row["storage_class"],
            {"local", "local_derived"},
            f"{row_path}.storage_class",
        )
        _timestamp(row["verified_at"], f"{row_path}.verified_at")
        _constant(row["is_primary"], 1, f"{row_path}.is_primary")
        _constant(
            row["media_location_id"],
            _producer_stable_id("media_location", location_media_id, storage_uri),
            f"{row_path}.media_location_id",
        )
        locations.append(dict(row))
    if locations and not media_objects:
        _fail(f"{path}.media_locations", "requires declared media_objects")

    catalog_artifacts: list[dict[str, Any]] = []
    artifact_ids: set[str] = set()
    for index, raw in enumerate(arrays["artifacts"]):
        row_path = f"{path}.artifacts[{index}]"
        row = _object(
            raw,
            row_path,
            {
                "artifact_id",
                "processing_run_id",
                "artifact_kind",
                "storage_uri",
                "sha256",
                "byte_count",
                "schema_version",
                "visibility",
            },
        )
        artifact_id = _id(row["artifact_id"], f"{row_path}.artifact_id")
        if artifact_id in artifact_ids:
            _fail(f"{row_path}.artifact_id", "must be unique")
        artifact_ids.add(artifact_id)
        _constant(row["processing_run_id"], run_id, f"{row_path}.processing_run_id")
        _choice(
            row["artifact_kind"],
            ALLOWED_PREPROCESS_ARTIFACT_KINDS,
            f"{row_path}.artifact_kind",
        )
        storage_uri = _string(row["storage_uri"], f"{row_path}.storage_uri")
        if not storage_uri.startswith("file://"):
            _fail(f"{row_path}.storage_uri", "must be an absolute file URI")
        _sha256(row["sha256"], f"{row_path}.sha256")
        _integer(row["byte_count"], f"{row_path}.byte_count", minimum=0)
        _constant(row["schema_version"], 1, f"{row_path}.schema_version")
        _constant(row["visibility"], "private", f"{row_path}.visibility")
        catalog_artifacts.append(dict(row))

    return {
        "media_objects": media_objects,
        "media_locations": locations,
        "processing_runs": processing_runs,
        "run_inputs": run_inputs,
        "artifacts": catalog_artifacts,
        "media_derivations": derivations,
    }


def _policy(value: object, path: str = "$.policy") -> dict[str, int]:
    policy = _object(value, path, set(DEFAULT_POLICY))
    duration = _bounded_integer(
        policy["interval_duration_ms"],
        f"{path}.interval_duration_ms",
        minimum=1,
        maximum=MAX_INTERVAL_DURATION_MS,
    )
    minimum_duration = _bounded_integer(
        policy["minimum_interval_duration_ms"],
        f"{path}.minimum_interval_duration_ms",
        minimum=1,
        maximum=MAX_INTERVAL_DURATION_MS,
    )
    if minimum_duration > duration:
        _fail(f"{path}.minimum_interval_duration_ms", "cannot exceed interval_duration_ms")
    per_recording = _bounded_integer(
        policy["max_intervals_per_recording"],
        f"{path}.max_intervals_per_recording",
        minimum=1,
        maximum=MAX_INTERVALS_PER_RECORDING,
    )
    total = _bounded_integer(
        policy["max_total_intervals"],
        f"{path}.max_total_intervals",
        minimum=1,
        maximum=MAX_TOTAL_INTERVALS,
    )
    total_duration = _bounded_integer(
        policy["max_total_duration_ms"],
        f"{path}.max_total_duration_ms",
        minimum=minimum_duration,
        maximum=MAX_TOTAL_DURATION_MS,
    )
    gap = _bounded_integer(
        policy["minimum_gap_ms"],
        f"{path}.minimum_gap_ms",
        maximum=MAX_INTERVAL_DURATION_MS,
    )
    stride = _bounded_integer(
        policy["periodic_stride_ms"],
        f"{path}.periodic_stride_ms",
        minimum=1,
        maximum=MAX_MEDIA_DURATION_MS,
    )
    silence = _bounded_integer(
        policy["max_silence_fraction_millionths"],
        f"{path}.max_silence_fraction_millionths",
        maximum=1_000_000,
    )
    return {
        "interval_duration_ms": duration,
        "minimum_interval_duration_ms": minimum_duration,
        "max_intervals_per_recording": per_recording,
        "max_total_intervals": total,
        "max_total_duration_ms": total_duration,
        "minimum_gap_ms": gap,
        "periodic_stride_ms": stride,
        "max_silence_fraction_millionths": silence,
    }


def _request_safety(value: object, path: str = "$.safety") -> dict[str, Any]:
    safety = _object(
        value,
        path,
        {
            "selection_basis",
            "asr_output_inputs_allowed",
            "transcript_inputs_allowed",
            "direct_media_content_inspection_allowed",
            "output_path_allowed",
            "catalog_write_allowed",
            "freeze_authority",
            "reference_quality_authority",
        },
    )
    _constant(
        safety["selection_basis"],
        "sealed_media_metadata_scene_silence_routing_only",
        f"{path}.selection_basis",
    )
    for key in (
        "asr_output_inputs_allowed",
        "transcript_inputs_allowed",
        "direct_media_content_inspection_allowed",
        "output_path_allowed",
        "catalog_write_allowed",
    ):
        _constant(safety[key], False, f"{path}.{key}")
    _constant(safety["freeze_authority"], "none", f"{path}.freeze_authority")
    _constant(
        safety["reference_quality_authority"],
        "none",
        f"{path}.reference_quality_authority",
    )
    return safety


def _request_timeline(value: object, path: str) -> dict[str, Any]:
    timeline = _object(
        value,
        path,
        {
            "binding_kind",
            "source_offset_ms",
            "source_end_ms",
            "local_window_result_path",
            "local_window_result_sha256",
        },
    )
    kind = _choice(
        timeline["binding_kind"], {"full_rendition", "local_window"}, f"{path}.binding_kind"
    )
    start = _bounded_integer(
        timeline["source_offset_ms"],
        f"{path}.source_offset_ms",
        maximum=MAX_MEDIA_DURATION_MS,
    )
    end = _bounded_integer(
        timeline["source_end_ms"],
        f"{path}.source_end_ms",
        minimum=1,
        maximum=MAX_MEDIA_DURATION_MS,
    )
    if end <= start:
        _fail(path, "must describe a non-empty half-open source window")
    local_path = _nullable_string(
        timeline["local_window_result_path"], f"{path}.local_window_result_path"
    )
    local_sha = _nullable_sha256(
        timeline["local_window_result_sha256"], f"{path}.local_window_result_sha256"
    )
    if kind == "full_rendition":
        if start != 0 or local_path is not None or local_sha is not None:
            _fail(path, "full_rendition requires offset 0 and null local-window binding")
    else:
        if local_path is None or local_sha is None:
            _fail(path, "local_window requires a pinned local-window result")
        candidate = Path(local_path)
        if not candidate.is_absolute():
            _fail(f"{path}.local_window_result_path", "must be absolute")
        _reject_forbidden_input_path(candidate, f"{path}.local_window_result_path")
    return timeline


def _request_recording(value: object, path: str) -> dict[str, Any]:
    row = _object(
        value,
        path,
        {
            "candidate_id",
            "recording_id",
            "source_id",
            "source_native_id",
            "source_locator",
            "rendition_id",
            "rendition_kind",
            "parent_media_id",
            "parent_media_sha256",
            "parent_media_byte_count",
            "parent_media_duration_ms",
            "analysis_media_id",
            "analysis_media_sha256",
            "analysis_media_byte_count",
            "analysis_media_duration_ms",
            "preprocess_result_path",
            "preprocess_result_sha256",
            "timeline",
        },
    )
    _id(row["candidate_id"], f"{path}.candidate_id")
    _id(row["recording_id"], f"{path}.recording_id")
    _id(row["source_id"], f"{path}.source_id")
    _string(row["source_native_id"], f"{path}.source_native_id")
    _string(row["source_locator"], f"{path}.source_locator")
    _id(row["rendition_id"], f"{path}.rendition_id")
    _id(row["rendition_kind"], f"{path}.rendition_kind")
    parent_id = _media_id(row["parent_media_id"], f"{path}.parent_media_id")
    parent_sha = _sha256(row["parent_media_sha256"], f"{path}.parent_media_sha256")
    if parent_id != f"media_sha256_{parent_sha}":
        _fail(f"{path}.parent_media_id", "does not match parent_media_sha256")
    parent_bytes = _bounded_integer(
        row["parent_media_byte_count"],
        f"{path}.parent_media_byte_count",
        minimum=1,
        maximum=1 << 63,
    )
    parent_duration = _bounded_integer(
        row["parent_media_duration_ms"],
        f"{path}.parent_media_duration_ms",
        minimum=1,
        maximum=MAX_MEDIA_DURATION_MS,
    )
    analysis_id = _media_id(row["analysis_media_id"], f"{path}.analysis_media_id")
    analysis_sha = _sha256(row["analysis_media_sha256"], f"{path}.analysis_media_sha256")
    if analysis_id != f"media_sha256_{analysis_sha}":
        _fail(f"{path}.analysis_media_id", "does not match analysis_media_sha256")
    _bounded_integer(
        row["analysis_media_byte_count"],
        f"{path}.analysis_media_byte_count",
        minimum=1,
        maximum=1 << 63,
    )
    analysis_duration = _bounded_integer(
        row["analysis_media_duration_ms"],
        f"{path}.analysis_media_duration_ms",
        minimum=1,
        maximum=MAX_MEDIA_DURATION_MS,
    )
    preprocess_path = Path(
        _string(row["preprocess_result_path"], f"{path}.preprocess_result_path")
    )
    if not preprocess_path.is_absolute():
        _fail(f"{path}.preprocess_result_path", "must be absolute")
    _reject_forbidden_input_path(preprocess_path, f"{path}.preprocess_result_path")
    _sha256(row["preprocess_result_sha256"], f"{path}.preprocess_result_sha256")
    timeline = _request_timeline(row["timeline"], f"{path}.timeline")
    if timeline["source_end_ms"] > parent_duration:
        _fail(f"{path}.timeline.source_end_ms", "exceeds parent_media_duration_ms")
    if timeline["binding_kind"] == "full_rendition":
        if timeline["source_end_ms"] != parent_duration:
            _fail(f"{path}.timeline.source_end_ms", "must equal parent media duration")
        if (analysis_id, analysis_sha, row["analysis_media_byte_count"]) != (
            parent_id,
            parent_sha,
            parent_bytes,
        ):
            _fail(path, "full_rendition analysis media must be the exact parent media")
        if analysis_duration != parent_duration:
            _fail(path, "full_rendition analysis duration must equal parent duration")
    return row


def _expected_request_id(request: dict[str, Any]) -> str:
    if request.get("schema_version") == 2:
        return _stable_id(
            "proposal_request",
            2,
            request["cohort_id"],
            request["created_at"],
            hashlib.sha256(_canonical_bytes(request["policy"])).hexdigest(),
            hashlib.sha256(_canonical_bytes(request["recordings"])).hexdigest(),
        )
    return _stable_id(
        "proposal_request",
        request["cohort_id"],
        request["created_at"],
        hashlib.sha256(_canonical_bytes(request["policy"])).hexdigest(),
        hashlib.sha256(_canonical_bytes(request["recordings"])).hexdigest(),
    )


def _request_window_v2(value: object, parent: dict[str, Any], path: str) -> dict[str, Any]:
    row = _object(
        value,
        path,
        {
            "window_id",
            "window_ordinal",
            "analysis_rendition_id",
            "analysis_rendition_kind",
            "analysis_media_id",
            "analysis_media_sha256",
            "analysis_media_byte_count",
            "analysis_media_duration_ms",
            "preprocess_result_path",
            "preprocess_result_raw_sha256",
            "preprocess_import_envelope_sha256",
            "preprocess_import_batch_id",
            "local_window_result_path",
            "local_window_result_sha256",
            "source_offset_ms",
            "source_end_ms",
        },
    )
    window_id = _string(row["window_id"], f"{path}.window_id")
    ordinal = _bounded_integer(
        row["window_ordinal"],
        f"{path}.window_ordinal",
        minimum=1,
        maximum=MAX_WINDOWS_PER_RECORDING,
    )
    if window_id != f"window_{ordinal:06d}":
        _fail(f"{path}.window_id", "must exactly encode window_ordinal")
    analysis_id = _media_id(row["analysis_media_id"], f"{path}.analysis_media_id")
    analysis_sha = _sha256(
        row["analysis_media_sha256"], f"{path}.analysis_media_sha256"
    )
    if analysis_id != f"media_sha256_{analysis_sha}":
        _fail(f"{path}.analysis_media_id", "does not match analysis_media_sha256")
    if analysis_id == parent["parent_media_id"]:
        _fail(f"{path}.analysis_media_id", "must be a local-window derivative")
    _bounded_integer(
        row["analysis_media_byte_count"],
        f"{path}.analysis_media_byte_count",
        minimum=1,
        maximum=1 << 63,
    )
    analysis_duration = _bounded_integer(
        row["analysis_media_duration_ms"],
        f"{path}.analysis_media_duration_ms",
        minimum=1,
        maximum=MAX_MEDIA_DURATION_MS,
    )
    rendition_kind = _string(
        row["analysis_rendition_kind"], f"{path}.analysis_rendition_kind"
    )
    rendition_match = LOCAL_WINDOW_RENDITION_KIND.fullmatch(rendition_kind)
    if rendition_match is None or rendition_match.group(3) != window_id:
        _fail(
            f"{path}.analysis_rendition_kind",
            "must exactly encode an admitted artifact, bundle, and window",
        )
    rendition_id = _id(
        row["analysis_rendition_id"], f"{path}.analysis_rendition_id"
    )
    expected_rendition_id = _expected_rendition_id(
        parent["recording_id"], analysis_id, rendition_kind
    )
    if rendition_id != expected_rendition_id:
        _fail(
            f"{path}.analysis_rendition_id",
            "does not match recording, analysis media, and rendition kind",
        )
    for key in ("preprocess_result_path", "local_window_result_path"):
        candidate = Path(_string(row[key], f"{path}.{key}"))
        if not candidate.is_absolute():
            _fail(f"{path}.{key}", "must be an absolute local path")
        _reject_forbidden_input_path(candidate, f"{path}.{key}")
    _sha256(
        row["preprocess_result_raw_sha256"],
        f"{path}.preprocess_result_raw_sha256",
    )
    import_envelope_sha256 = _sha256(
        row["preprocess_import_envelope_sha256"],
        f"{path}.preprocess_import_envelope_sha256",
    )
    import_batch_id = _id(
        row["preprocess_import_batch_id"],
        f"{path}.preprocess_import_batch_id",
    )
    expected_import_batch_id = _preprocess_import_batch_id(
        import_envelope_sha256
    )
    if import_batch_id != expected_import_batch_id:
        _fail(
            f"{path}.preprocess_import_batch_id",
            f"must equal deterministic ID {expected_import_batch_id!r}",
        )
    _sha256(
        row["local_window_result_sha256"], f"{path}.local_window_result_sha256"
    )
    start = _bounded_integer(
        row["source_offset_ms"],
        f"{path}.source_offset_ms",
        maximum=MAX_MEDIA_DURATION_MS,
    )
    end = _bounded_integer(
        row["source_end_ms"],
        f"{path}.source_end_ms",
        minimum=1,
        maximum=MAX_MEDIA_DURATION_MS,
    )
    if end <= start:
        _fail(path, "must describe a non-empty half-open parent source window")
    if end > parent["parent_media_duration_ms"]:
        _fail(f"{path}.source_end_ms", "exceeds parent_media_duration_ms")
    if abs(analysis_duration - (end - start)) > 250:
        _fail(
            f"{path}.analysis_media_duration_ms",
            "differs from the exact source window by more than 250 ms",
        )
    return row


def _request_parent_v2(value: object, path: str) -> dict[str, Any]:
    row = _object(
        value,
        path,
        {
            "candidate_id",
            "recording_id",
            "source_id",
            "source_native_id",
            "source_locator",
            "parent_rendition_id",
            "parent_rendition_kind",
            "parent_media_id",
            "parent_media_sha256",
            "parent_media_byte_count",
            "parent_media_duration_ms",
            "windows",
        },
    )
    _id(row["candidate_id"], f"{path}.candidate_id")
    _id(row["recording_id"], f"{path}.recording_id")
    _id(row["source_id"], f"{path}.source_id")
    _string(row["source_native_id"], f"{path}.source_native_id")
    _string(row["source_locator"], f"{path}.source_locator")
    _id(row["parent_rendition_id"], f"{path}.parent_rendition_id")
    _constant(
        row["parent_rendition_kind"],
        "acquired_source_media",
        f"{path}.parent_rendition_kind",
    )
    parent_id = _media_id(row["parent_media_id"], f"{path}.parent_media_id")
    parent_sha = _sha256(row["parent_media_sha256"], f"{path}.parent_media_sha256")
    if parent_id != f"media_sha256_{parent_sha}":
        _fail(f"{path}.parent_media_id", "does not match parent_media_sha256")
    _bounded_integer(
        row["parent_media_byte_count"],
        f"{path}.parent_media_byte_count",
        minimum=1,
        maximum=1 << 63,
    )
    _bounded_integer(
        row["parent_media_duration_ms"],
        f"{path}.parent_media_duration_ms",
        minimum=1,
        maximum=MAX_MEDIA_DURATION_MS,
    )
    windows = _array(row["windows"], f"{path}.windows")
    if not windows or len(windows) > MAX_WINDOWS_PER_RECORDING:
        _fail(
            f"{path}.windows",
            f"must contain 1..{MAX_WINDOWS_PER_RECORDING} local windows",
        )
    seen_analysis: set[str] = set()
    seen_analysis_renditions: set[str] = set()
    seen_window_ids: set[str] = set()
    seen_window_ordinals: set[int] = set()
    seen_preprocess_paths: set[str] = set()
    seen_preprocess_hashes: set[str] = set()
    seen_preprocess_import_hashes: set[str] = set()
    seen_preprocess_import_batches: set[str] = set()
    seen_local_paths: set[str] = set()
    seen_local_hashes: set[str] = set()
    seen_ranges: set[tuple[int, int]] = set()
    previous_end = -1
    parsed_windows: list[dict[str, Any]] = []
    for index, raw_window in enumerate(windows):
        window_path = f"{path}.windows[{index}]"
        window = _request_window_v2(raw_window, row, window_path)
        start = window["source_offset_ms"]
        end = window["source_end_ms"]
        if (start, end) in seen_ranges:
            _fail(window_path, "duplicates a parent source window")
        if start < previous_end:
            _fail(window_path, "local windows must be sorted and nonoverlapping")
        previous_end = end
        seen_ranges.add((start, end))
        for seen, item, label in (
            (seen_window_ids, window["window_id"], "window_id"),
            (
                seen_window_ordinals,
                window["window_ordinal"],
                "window_ordinal",
            ),
            (seen_analysis, window["analysis_media_id"], "analysis_media_id"),
            (
                seen_analysis_renditions,
                window["analysis_rendition_id"],
                "analysis_rendition_id",
            ),
            (
                seen_preprocess_paths,
                window["preprocess_result_path"],
                "preprocess_result_path",
            ),
            (
                seen_preprocess_hashes,
                window["preprocess_result_raw_sha256"],
                "preprocess_result_raw_sha256",
            ),
            (
                seen_preprocess_import_hashes,
                window["preprocess_import_envelope_sha256"],
                "preprocess_import_envelope_sha256",
            ),
            (
                seen_preprocess_import_batches,
                window["preprocess_import_batch_id"],
                "preprocess_import_batch_id",
            ),
            (
                seen_local_paths,
                window["local_window_result_path"],
                "local_window_result_path",
            ),
            (
                seen_local_hashes,
                window["local_window_result_sha256"],
                "local_window_result_sha256",
            ),
        ):
            if item in seen:
                _fail(f"{window_path}.{label}", "must be unique within its parent")
            seen.add(item)
        parsed_windows.append(window)
    row["windows"] = parsed_windows
    return row


def _validate_interval_proposal_request_v2(
    value: object, cohort: dict[str, Any]
) -> dict[str, Any]:
    request = _object(
        value,
        "$",
        {
            "schema_version",
            "manifest_kind",
            "manifest_sha256",
            "request_id",
            "created_at",
            "cohort_id",
            "cohort_manifest_sha256",
            "policy",
            "safety",
            "recordings",
        },
    )
    _constant(request["schema_version"], 2, "$.schema_version")
    _constant(request["manifest_kind"], "interval_proposal_request", "$.manifest_kind")
    _id(request["request_id"], "$.request_id")
    _timestamp(request["created_at"], "$.created_at")
    if request["cohort_id"] != cohort["cohort_id"]:
        _fail("$.cohort_id", "does not match the candidate cohort")
    if request["cohort_manifest_sha256"] != cohort["manifest_sha256"]:
        _fail("$.cohort_manifest_sha256", "does not bind the exact candidate cohort")
    _policy(request["policy"])
    _request_safety(request["safety"])
    rows = _array(request["recordings"], "$.recordings")
    if not rows or len(rows) > MAX_RECORDINGS:
        _fail("$.recordings", f"must contain 1..{MAX_RECORDINGS} parent recordings")
    candidates = {row["candidate_id"]: row for row in cohort["candidates"]}
    cohort_order = {
        row["candidate_id"]: index for index, row in enumerate(cohort["candidates"])
    }
    seen_candidates: set[str] = set()
    seen_recordings: set[str] = set()
    seen_parent_media: set[str] = set()
    previous_order = -1
    total_windows = 0
    parsed_rows: list[dict[str, Any]] = []
    for index, raw in enumerate(rows):
        path = f"$.recordings[{index}]"
        row = _request_parent_v2(raw, path)
        candidate = candidates.get(row["candidate_id"])
        if candidate is None:
            _fail(f"{path}.candidate_id", "is not in the candidate cohort")
        if candidate["eligibility_state"] == "ineligible":
            _fail(f"{path}.candidate_id", "ineligible candidates cannot be proposed")
        for key, candidate_key in (
            ("recording_id", "recording_id"),
            ("source_id", "source_id"),
            ("source_native_id", "native_id"),
            ("source_locator", "public_locator"),
        ):
            if row[key] != candidate[candidate_key]:
                _fail(f"{path}.{key}", "does not match its candidate cohort row")
        expected_parent_rendition = _expected_rendition_id(
            row["recording_id"],
            row["parent_media_id"],
            row["parent_rendition_kind"],
        )
        if row["parent_rendition_id"] != expected_parent_rendition:
            _fail(
                f"{path}.parent_rendition_id",
                "does not match recording, parent media, and rendition kind",
            )
        order = cohort_order[row["candidate_id"]]
        if order <= previous_order:
            _fail(path, "parent recordings must be unique and follow cohort order")
        previous_order = order
        for seen, item, label in (
            (seen_candidates, row["candidate_id"], "candidate_id"),
            (seen_recordings, row["recording_id"], "recording_id"),
            (seen_parent_media, row["parent_media_id"], "parent_media_id"),
        ):
            if item in seen:
                _fail(f"{path}.{label}", "must be unique")
            seen.add(item)
        total_windows += len(row["windows"])
        if total_windows > MAX_TOTAL_WINDOWS:
            _fail("$.recordings", f"contains more than {MAX_TOTAL_WINDOWS} windows")
        parsed_rows.append(row)
    request["recordings"] = parsed_rows
    expected_id = _expected_request_id(request)
    if request["request_id"] != expected_id:
        _fail("$.request_id", f"must equal deterministic ID {expected_id!r}")
    _verify_manifest_digest(request)
    return request


def _window_request_adapter(
    parent: dict[str, Any], window: dict[str, Any]
) -> dict[str, Any]:
    """Present one v2 child window to the sealed v1 producer validators."""

    return {
        "candidate_id": parent["candidate_id"],
        "recording_id": parent["recording_id"],
        "source_id": parent["source_id"],
        "source_native_id": parent["source_native_id"],
        "source_locator": parent["source_locator"],
        "rendition_id": parent["parent_rendition_id"],
        "rendition_kind": parent["parent_rendition_kind"],
        "parent_media_id": parent["parent_media_id"],
        "parent_media_sha256": parent["parent_media_sha256"],
        "parent_media_byte_count": parent["parent_media_byte_count"],
        "parent_media_duration_ms": parent["parent_media_duration_ms"],
        "analysis_media_id": window["analysis_media_id"],
        "analysis_media_sha256": window["analysis_media_sha256"],
        "analysis_media_byte_count": window["analysis_media_byte_count"],
        "analysis_media_duration_ms": window["analysis_media_duration_ms"],
        "preprocess_result_path": window["preprocess_result_path"],
        "preprocess_result_raw_sha256": window[
            "preprocess_result_raw_sha256"
        ],
        "preprocess_import_envelope_sha256": window[
            "preprocess_import_envelope_sha256"
        ],
        "preprocess_import_batch_id": window["preprocess_import_batch_id"],
        "timeline": {
            "binding_kind": "local_window",
            "source_offset_ms": window["source_offset_ms"],
            "source_end_ms": window["source_end_ms"],
            "local_window_result_path": window["local_window_result_path"],
            "local_window_result_sha256": window["local_window_result_sha256"],
        },
        "_v2_window_id": window["window_id"],
        "_v2_window_ordinal": window["window_ordinal"],
        "_v2_analysis_rendition_id": window["analysis_rendition_id"],
        "_v2_analysis_rendition_kind": window["analysis_rendition_kind"],
    }


def validate_interval_proposal_request(
    value: object, candidate_cohort: object
) -> dict[str, Any]:
    """Validate a proposal request without reading any referenced files."""

    cohort = validate_candidate_cohort(candidate_cohort)
    if isinstance(value, dict) and value.get("schema_version") == 2:
        return _validate_interval_proposal_request_v2(value, cohort)
    request = _object(
        value,
        "$",
        {
            "schema_version",
            "manifest_kind",
            "manifest_sha256",
            "request_id",
            "created_at",
            "cohort_id",
            "cohort_manifest_sha256",
            "policy",
            "safety",
            "recordings",
        },
    )
    _constant(request["schema_version"], 1, "$.schema_version")
    _constant(request["manifest_kind"], "interval_proposal_request", "$.manifest_kind")
    _id(request["request_id"], "$.request_id")
    _timestamp(request["created_at"], "$.created_at")
    if request["cohort_id"] != cohort["cohort_id"]:
        _fail("$.cohort_id", "does not match the candidate cohort")
    if request["cohort_manifest_sha256"] != cohort["manifest_sha256"]:
        _fail("$.cohort_manifest_sha256", "does not bind the exact candidate cohort")
    _policy(request["policy"])
    _request_safety(request["safety"])
    candidates = {row["candidate_id"]: row for row in cohort["candidates"]}
    rows = _array(request["recordings"], "$.recordings")
    if not rows or len(rows) > MAX_RECORDINGS:
        _fail("$.recordings", f"must contain 1..{MAX_RECORDINGS} recordings")
    seen_candidates: set[str] = set()
    seen_recordings: set[str] = set()
    seen_analysis: set[str] = set()
    cohort_order = {row["candidate_id"]: index for index, row in enumerate(cohort["candidates"])}
    previous_order = -1
    for index, raw in enumerate(rows):
        path = f"$.recordings[{index}]"
        row = _request_recording(raw, path)
        candidate = candidates.get(row["candidate_id"])
        if candidate is None:
            _fail(f"{path}.candidate_id", "is not in the candidate cohort")
        if candidate["eligibility_state"] == "ineligible":
            _fail(f"{path}.candidate_id", "ineligible candidates cannot be proposed")
        for key, candidate_key in (
            ("recording_id", "recording_id"),
            ("source_id", "source_id"),
            ("source_native_id", "native_id"),
            ("source_locator", "public_locator"),
        ):
            if row[key] != candidate[candidate_key]:
                _fail(f"{path}.{key}", "does not match its candidate cohort row")
        if row["rendition_id"] != _expected_rendition_id(
            row["recording_id"], row["parent_media_id"], row["rendition_kind"]
        ):
            _fail(f"{path}.rendition_id", "does not match recording, parent media, and kind")
        order = cohort_order[row["candidate_id"]]
        if order <= previous_order:
            _fail(path, "recordings must be unique and follow candidate cohort order")
        previous_order = order
        for seen, item, label in (
            (seen_candidates, row["candidate_id"], "candidate_id"),
            (seen_recordings, row["recording_id"], "recording_id"),
            (seen_analysis, row["analysis_media_id"], "analysis_media_id"),
        ):
            if item in seen:
                _fail(f"{path}.{label}", "must be unique")
            seen.add(item)
    expected_id = _expected_request_id(request)
    if request["request_id"] != expected_id:
        _fail("$.request_id", f"must equal deterministic ID {expected_id!r}")
    _verify_manifest_digest(request)
    return request


def _validate_preprocess_result(
    value: dict[str, Any], request_row: dict[str, Any], path: str
) -> dict[str, Any]:
    strict_v2 = "_v2_window_id" in request_row
    base_keys = {
        "schema_version",
        "status",
        "dry_run",
        "job_id",
        "input",
        "layout",
        "processing_run",
        "steps",
        "artifacts",
        "routing",
        "duration_ms",
        "errors",
        "catalog_records",
        "result_path",
    }
    if not isinstance(value, dict):
        _fail(path, "must be an object")
    # Preprocessor 0.3 added a sealed reuse-attestation object.  Supporting that
    # additive metadata keeps older 0.1/0.2 results valid without opening the
    # contract to arbitrary (especially ASR/transcript) result fields.
    if set(value) not in (base_keys, base_keys | {"reuse"}):
        missing = sorted(base_keys - set(value))
        extra = sorted(set(value) - (base_keys | {"reuse"}))
        _fail(path, f"unexpected preprocess result shape; missing={missing}, extra={extra}")
    top = value
    _reject_forbidden_data_keys(top, path)
    _constant(top["schema_version"], 1, f"{path}.schema_version")
    _constant(top["status"], "completed", f"{path}.status")
    _constant(top["dry_run"], False, f"{path}.dry_run")
    _id(top["job_id"], f"{path}.job_id")
    _bounded_integer(
        top["duration_ms"], f"{path}.duration_ms", maximum=MAX_MEDIA_DURATION_MS
    )
    if top["result_path"] != request_row["preprocess_result_path"]:
        _fail(
            f"{path}.result_path",
            "must equal the exact pinned preprocess result path",
        )
    if _array(top["errors"], f"{path}.errors"):
        _fail(f"{path}.errors", "completed preprocess result must have no errors")
    run = _object(
        top["processing_run"],
        f"{path}.processing_run",
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
    _constant(run.get("stage"), "media_preprocess", f"{path}.processing_run.stage")
    _constant(run.get("status"), "completed", f"{path}.processing_run.status")
    run_id = _id(run.get("processing_run_id"), f"{path}.processing_run.processing_run_id")
    implementation = _string(
        run.get("implementation_version"), f"{path}.processing_run.implementation_version"
    )
    if not isinstance(run["parameters_json"], dict) or not isinstance(
        run["environment_json"], dict
    ):
        _fail(f"{path}.processing_run", "parameters and environment must be objects")
    _timestamp(run["started_at"], f"{path}.processing_run.started_at")
    _timestamp(run["completed_at"], f"{path}.processing_run.completed_at")
    input_base = {
        "media_id",
        "sha256",
        "byte_count",
        "path",
        "storage_uri",
        "stat_before",
        "stat_after",
        "unchanged",
    }
    if not isinstance(top["input"], dict) or set(top["input"]) not in (
        input_base,
        input_base | {"catalog_observation"},
    ):
        _fail(f"{path}.input", "does not match a supported exact preprocess input shape")
    input_row = top["input"]
    if input_row["media_id"] != request_row["analysis_media_id"]:
        _fail(f"{path}.input.media_id", "does not match requested analysis media")
    if input_row["sha256"] != request_row["analysis_media_sha256"]:
        _fail(f"{path}.input.sha256", "does not match requested analysis media")
    if input_row["byte_count"] != request_row["analysis_media_byte_count"]:
        _fail(f"{path}.input.byte_count", "does not match requested analysis media")
    _constant(input_row.get("unchanged"), True, f"{path}.input.unchanged")
    media_input_path = Path(_string(input_row["path"], f"{path}.input.path"))
    if not media_input_path.is_absolute():
        _fail(f"{path}.input.path", "must be absolute")
    _reject_forbidden_input_path(media_input_path, f"{path}.input.path")
    _constant(
        input_row["storage_uri"],
        media_input_path.as_uri(),
        f"{path}.input.storage_uri",
    )
    if not isinstance(input_row["stat_before"], dict) or not isinstance(
        input_row["stat_after"], dict
    ):
        _fail(f"{path}.input", "requires stable before/after file-stat objects")
    old_layout = {"object_dir", "output_root", "recipe_sha256", "run_dir"}
    new_layout = old_layout | {"recipe_dir", "recipe_id"}
    if not isinstance(top["layout"], dict) or set(top["layout"]) not in (
        old_layout,
        new_layout,
    ):
        _fail(f"{path}.layout", "does not match a supported exact preprocess layout")
    layout = top["layout"]
    recipe_sha = _sha256(layout.get("recipe_sha256"), f"{path}.layout.recipe_sha256")
    if strict_v2:
        for key in ("object_dir", "output_root", "run_dir"):
            layout_path = Path(_string(layout[key], f"{path}.layout.{key}"))
            if not layout_path.is_absolute():
                _fail(f"{path}.layout.{key}", "must be an absolute local path")
            _reject_forbidden_input_path(layout_path, f"{path}.layout.{key}")
        if "recipe_dir" in layout:
            recipe_dir = Path(
                _string(layout["recipe_dir"], f"{path}.layout.recipe_dir")
            )
            if not recipe_dir.is_absolute():
                _fail(f"{path}.layout.recipe_dir", "must be an absolute local path")
            _reject_forbidden_input_path(recipe_dir, f"{path}.layout.recipe_dir")
            _id(layout["recipe_id"], f"{path}.layout.recipe_id")
    parameter_sha = hashlib.sha256(_canonical_bytes(run["parameters_json"])).hexdigest()
    if recipe_sha != parameter_sha:
        _fail(
            f"{path}.layout.recipe_sha256",
            "must equal canonical processing_run.parameters_json SHA-256",
        )
    steps = _array(top["steps"], f"{path}.steps")
    if len(steps) != 4:
        _fail(f"{path}.steps", "must contain the four preprocessing-only steps")
    for index, (raw_step, expected_name) in enumerate(
        zip(steps, ("probe", "audio_flac", "proxy", "routing"))
    ):
        step_path = f"{path}.steps[{index}]"
        step = _object(raw_step, step_path, {"name", "status", "command", "output_path"})
        _constant(step["name"], expected_name, f"{step_path}.name")
        _choice(
            step["status"],
            {"completed", "completed_read_only", "reused", "not_applicable", "disabled"},
            f"{step_path}.status",
        )
        if step["command"] is not None:
            command = _array(step["command"], f"{step_path}.command")
            if not command:
                _fail(f"{step_path}.command", "must be nonempty when present")
            for command_index, argument in enumerate(command):
                argument_path = f"{step_path}.command[{command_index}]"
                argument_text = _string(argument, argument_path)
                if strict_v2:
                    _reject_forbidden_input_path(Path(argument_text), argument_path)
        if step["output_path"] is not None:
            output_path = Path(
                _string(step["output_path"], f"{step_path}.output_path")
            )
            if strict_v2:
                if not output_path.is_absolute():
                    _fail(f"{step_path}.output_path", "must be an absolute local path")
                _reject_forbidden_input_path(output_path, f"{step_path}.output_path")
    catalog_records = _validate_preprocess_catalog_records(
        top["catalog_records"],
        request_row,
        run,
        run_id,
        f"{path}.catalog_records",
    )
    if "reuse" in top:
        if top["reuse"] is not None:
            reuse = _object(
                top["reuse"],
                f"{path}.reuse",
                {
                    "mode",
                    "prior_processing_run_id",
                    "prior_result_path",
                    "prior_result_sha256",
                    "verified_at",
                },
            )
            mode = _choice(
                reuse["mode"], {"none", "verified_prior_result"}, f"{path}.reuse.mode"
            )
            if mode == "none" and any(
                reuse[key] is not None
                for key in (
                    "prior_processing_run_id",
                    "prior_result_path",
                    "prior_result_sha256",
                    "verified_at",
                )
            ):
                _fail(f"{path}.reuse", "mode none requires null prior-result fields")
            if strict_v2 and mode == "verified_prior_result":
                _id(
                    reuse["prior_processing_run_id"],
                    f"{path}.reuse.prior_processing_run_id",
                )
                prior_path = Path(
                    _string(
                        reuse["prior_result_path"],
                        f"{path}.reuse.prior_result_path",
                    )
                )
                if not prior_path.is_absolute():
                    _fail(
                        f"{path}.reuse.prior_result_path",
                        "must be an absolute local path",
                    )
                _reject_forbidden_input_path(
                    prior_path, f"{path}.reuse.prior_result_path"
                )
                _sha256(
                    reuse["prior_result_sha256"],
                    f"{path}.reuse.prior_result_sha256",
                )
                _timestamp(reuse["verified_at"], f"{path}.reuse.verified_at")
    artifacts = _array(top["artifacts"], f"{path}.artifacts")
    if not artifacts:
        _fail(f"{path}.artifacts", "must not be empty")
    artifact_bindings: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(artifacts):
        artifact_path = f"{path}.artifacts[{index}]"
        if not isinstance(raw, dict):
            _fail(artifact_path, "must be an object")
        kind = _string(raw.get("artifact_kind"), f"{artifact_path}.artifact_kind")
        if kind not in ALLOWED_PREPROCESS_ARTIFACT_KINDS:
            _fail(
                f"{artifact_path}.artifact_kind",
                "ASR/transcript or unknown preprocess artifacts are forbidden",
            )
        if kind in artifact_bindings:
            _fail(f"{artifact_path}.artifact_kind", "must be unique")
        raw = _object(
            raw,
            artifact_path,
            {
                "schema_version",
                "artifact_id",
                "processing_run_id",
                "artifact_kind",
                "path",
                "storage_uri",
                "sha256",
                "byte_count",
                "media_kind",
                "mime_type",
                "visibility",
                "normalized_probe",
            },
        )
        binding = {
            "artifact_id": _id(raw.get("artifact_id"), f"{artifact_path}.artifact_id"),
            "artifact_kind": kind,
            "sha256": _sha256(raw.get("sha256"), f"{artifact_path}.sha256"),
            "byte_count": _integer(raw.get("byte_count"), f"{artifact_path}.byte_count", minimum=1),
            "schema_version": _integer(
                raw.get("schema_version"), f"{artifact_path}.schema_version", minimum=1
            ),
            "visibility": _choice(
                raw.get("visibility"),
                {"private", "review"},
                f"{artifact_path}.visibility",
            ),
            "path": _string(raw.get("path"), f"{artifact_path}.path"),
            "storage_uri": _string(
                raw.get("storage_uri"), f"{artifact_path}.storage_uri"
            ),
        }
        if raw.get("processing_run_id") != run_id:
            _fail(f"{artifact_path}.processing_run_id", "does not match preprocess run")
        if strict_v2:
            artifact_file = Path(binding["path"])
            if not artifact_file.is_absolute():
                _fail(f"{artifact_path}.path", "must be an absolute local path")
            _reject_forbidden_input_path(artifact_file, f"{artifact_path}.path")
        _constant(
            binding["storage_uri"],
            (
                Path(binding["path"]).as_uri()
                if Path(binding["path"]).is_absolute()
                else "file://" + binding["path"]
            ),
            f"{artifact_path}.storage_uri",
        )
        artifact_bindings[kind] = binding
    if strict_v2:
        for record_kind in ("media_locations", "artifacts"):
            for index, record in enumerate(catalog_records[record_kind]):
                storage_uri = record["storage_uri"]
                _reject_forbidden_input_path(
                    Path(unquote(storage_uri.removeprefix("file://"))),
                    f"{path}.catalog_records.{record_kind}[{index}].storage_uri",
                )
    if catalog_records["artifacts"]:
        expected_catalog_artifacts = sorted(
            (
                {
                    "artifact_id": item["artifact_id"],
                    "processing_run_id": run_id,
                    "artifact_kind": item["artifact_kind"],
                    "storage_uri": item["storage_uri"],
                    "sha256": item["sha256"],
                    "byte_count": item["byte_count"],
                    "schema_version": item["schema_version"],
                    "visibility": item["visibility"],
                }
                for item in artifact_bindings.values()
            ),
            key=lambda item: (item["artifact_kind"], item["artifact_id"]),
        )
        observed_catalog_artifacts = sorted(
            catalog_records["artifacts"],
            key=lambda item: (item["artifact_kind"], item["artifact_id"]),
        )
        if observed_catalog_artifacts != expected_catalog_artifacts:
            _fail(
                f"{path}.catalog_records.artifacts",
                "must exactly match the enclosing artifact descriptors",
            )
    if catalog_records["media_objects"]:
        probe_binding = artifact_bindings.get("ffprobe_normalized_json")
        if probe_binding is None:
            _fail(
                f"{path}.catalog_records.media_objects",
                "requires a hash-pinned ffprobe_normalized_json artifact",
            )
        probe_value, probe_sha, probe_path = _read_pinned_json(
            probe_binding["path"],
            probe_binding["sha256"],
            f"{path}.probe_artifact",
        )
        if probe_path.stat().st_size != probe_binding["byte_count"]:
            _fail(f"{path}.probe_artifact", "byte count does not match descriptor")
        if probe_sha != probe_binding["sha256"]:
            _fail(f"{path}.probe_artifact", "digest does not match descriptor")
        source_media_row = next(
            item
            for item in catalog_records["media_objects"]
            if item["media_id"] == request_row["analysis_media_id"]
        )
        if source_media_row["ffprobe_json"] != probe_value:
            _fail(
                f"{path}.catalog_records.media_objects",
                "analysis ffprobe_json must exactly equal the hash-pinned probe artifact",
            )
    if "scene_silence_routing_json" not in artifact_bindings:
        _fail(f"{path}.artifacts", "requires scene_silence_routing_json")
    routing_binding = artifact_bindings["scene_silence_routing_json"]
    routing_value, routing_sha, routing_path = _read_pinned_json(
        routing_binding["path"],
        routing_binding["sha256"],
        f"{path}.routing_artifact",
    )
    if routing_path.stat().st_size != routing_binding["byte_count"]:
        _fail(f"{path}.routing_artifact", "byte count does not match artifact descriptor")
    if routing_sha != routing_binding["sha256"] or routing_value != top["routing"]:
        _fail(
            f"{path}.routing",
            "embedded routing must exactly equal the hash-pinned routing artifact",
        )
    routing = _validate_routing(routing_value, request_row, f"{path}.routing")
    return {
        "processing_run_id": run_id,
        "implementation_version": implementation,
        "parameters_json": run["parameters_json"],
        "environment_json": run["environment_json"],
        "started_at": run["started_at"],
        "completed_at": run["completed_at"],
        "recipe_sha256": recipe_sha,
        "routing": routing,
        "routing_metadata_sha256": hashlib.sha256(_canonical_bytes(top["routing"])).hexdigest(),
        "artifacts": artifact_bindings,
        "catalog_records": catalog_records,
    }


def _validate_routing(
    value: object, request_row: dict[str, Any], path: str
) -> dict[str, Any]:
    routing = _object(
        value,
        path,
        {
            "schema_version",
            "source_media_id",
            "coverage",
            "parameters",
            "routing_candidates",
            "scene_changes",
            "silence_intervals",
            "summary",
            "warning",
        },
    )
    _constant(routing["schema_version"], 1, f"{path}.schema_version")
    if routing["source_media_id"] != request_row["analysis_media_id"]:
        _fail(f"{path}.source_media_id", "does not match requested analysis media")
    coverage = _object(
        routing["coverage"], f"{path}.coverage", {"duration_ms", "has_audio", "has_video"}
    )
    duration = _bounded_integer(
        coverage["duration_ms"],
        f"{path}.coverage.duration_ms",
        minimum=1,
        maximum=MAX_MEDIA_DURATION_MS,
    )
    _boolean(coverage["has_audio"], f"{path}.coverage.has_audio")
    _boolean(coverage["has_video"], f"{path}.coverage.has_video")
    if abs(duration - request_row["analysis_media_duration_ms"]) > 250:
        _fail(
            f"{path}.coverage.duration_ms",
            "differs from pinned analysis media duration by more than 250 ms",
        )
    parameters = _object(
        routing["parameters"],
        f"{path}.parameters",
        {
            "scene_threshold_percent",
            "silence_noise_db",
            "silence_min_duration_ms",
            "near_silent_fraction",
        },
    )
    scene_threshold = _number(
        parameters["scene_threshold_percent"], f"{path}.parameters.scene_threshold_percent"
    )
    silence_noise = _number(
        parameters["silence_noise_db"], f"{path}.parameters.silence_noise_db"
    )
    silence_minimum = _integer(
        parameters["silence_min_duration_ms"],
        f"{path}.parameters.silence_min_duration_ms",
        minimum=1,
    )
    near_silent = _number(
        parameters["near_silent_fraction"], f"{path}.parameters.near_silent_fraction"
    )
    if not 0 <= scene_threshold <= 100:
        _fail(f"{path}.parameters.scene_threshold_percent", "must be in [0, 100]")
    if not -100 <= silence_noise <= 0:
        _fail(f"{path}.parameters.silence_noise_db", "must be in [-100, 0]")
    if silence_minimum > MAX_MEDIA_DURATION_MS:
        _fail(f"{path}.parameters.silence_min_duration_ms", "exceeds hard bound")
    if not 0 <= near_silent <= 1:
        _fail(f"{path}.parameters.near_silent_fraction", "must be in [0, 1]")
    candidates = _object(
        routing["routing_candidates"],
        f"{path}.routing_candidates",
        {"asr", "ocr", "visual", "diarization", "active_speaker"},
    )
    _choice(
        candidates["asr"],
        {"process", "skip_no_audio", "review_near_silent_candidate"},
        f"{path}.routing_candidates.asr",
    )
    _choice(
        candidates["ocr"],
        {"scene_keyframes", "sparse_keyframes", "skip_no_video"},
        f"{path}.routing_candidates.ocr",
    )
    _choice(
        candidates["visual"],
        {"scene_and_speech_windows", "scene_keyframes", "skip_no_video"},
        f"{path}.routing_candidates.visual",
    )
    _constant(
        candidates["diarization"], "router_pending", f"{path}.routing_candidates.diarization"
    )
    _constant(
        candidates["active_speaker"],
        "router_pending",
        f"{path}.routing_candidates.active_speaker",
    )
    _constant(routing["warning"], ROUTING_WARNING, f"{path}.warning")
    scenes = _array(routing["scene_changes"], f"{path}.scene_changes")
    silences = _array(routing["silence_intervals"], f"{path}.silence_intervals")
    if len(scenes) + len(silences) > MAX_ROUTING_ROWS:
        _fail(path, f"routing rows exceed hard cap {MAX_ROUTING_ROWS}")
    parsed_scenes: list[dict[str, Any]] = []
    previous_timestamp = -1
    for index, raw in enumerate(scenes):
        item_path = f"{path}.scene_changes[{index}]"
        item = _object(raw, item_path, {"timestamp_ms", "score_percent"})
        timestamp = _bounded_integer(
            item["timestamp_ms"], item_path + ".timestamp_ms", maximum=duration
        )
        if timestamp < previous_timestamp:
            _fail(item_path + ".timestamp_ms", "must be sorted")
        previous_timestamp = timestamp
        score = _number(item["score_percent"], item_path + ".score_percent")
        if score < 0 or score > 100:
            _fail(item_path + ".score_percent", "must be in [0, 100]")
        parsed_scenes.append({"timestamp_ms": timestamp, "score_percent": score})
    parsed_silences: list[dict[str, int]] = []
    previous_start = -1
    for index, raw in enumerate(silences):
        item_path = f"{path}.silence_intervals[{index}]"
        item = _object(raw, item_path, {"start_ms", "end_ms", "duration_ms"})
        start = _bounded_integer(item["start_ms"], item_path + ".start_ms", maximum=duration)
        end = _bounded_integer(
            item["end_ms"], item_path + ".end_ms", minimum=1, maximum=duration
        )
        if end <= start or item["duration_ms"] != end - start:
            _fail(item_path, "must be an exact non-empty half-open interval")
        if start < previous_start:
            _fail(item_path + ".start_ms", "must be sorted")
        previous_start = start
        parsed_silences.append({"start_ms": start, "end_ms": end})
    merged = _merge_ranges((item["start_ms"], item["end_ms"]) for item in parsed_silences)
    summary = _object(
        routing["summary"],
        f"{path}.summary",
        {
            "scene_change_count",
            "silence_interval_count",
            "silent_duration_ms",
            "silent_fraction",
        },
    )
    _constant(
        summary["scene_change_count"], len(parsed_scenes), f"{path}.summary.scene_change_count"
    )
    _constant(
        summary["silence_interval_count"],
        len(parsed_silences),
        f"{path}.summary.silence_interval_count",
    )
    silent_duration = sum(end - start for start, end in merged)
    _constant(
        summary["silent_duration_ms"], silent_duration, f"{path}.summary.silent_duration_ms"
    )
    silent_fraction = _number(summary["silent_fraction"], f"{path}.summary.silent_fraction")
    expected_fraction = round(silent_duration / duration, 6)
    if abs(silent_fraction - expected_fraction) > 0.0000005:
        _fail(
            f"{path}.summary.silent_fraction",
            f"must equal six-decimal routing arithmetic {expected_fraction}",
        )
    return {
        "duration_ms": duration,
        "has_audio": coverage["has_audio"],
        "has_video": coverage["has_video"],
        "scenes": parsed_scenes,
        "silences": merged,
        "scene_change_count": len(parsed_scenes),
        "silence_interval_count": len(parsed_silences),
    }


def _merge_ranges(ranges: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    output: list[list[int]] = []
    for start, end in sorted(ranges):
        if output and start <= output[-1][1]:
            output[-1][1] = max(output[-1][1], end)
        else:
            output.append([start, end])
    return [(start, end) for start, end in output]


def _validate_local_window_result(
    value: dict[str, Any], request_row: dict[str, Any], path: str
) -> dict[str, Any]:
    strict_v2 = "_v2_window_id" in request_row
    row = _object(
        value,
        path,
        {
            "schema_version",
            "implementation_version",
            "status",
            "dry_run",
            "job_id",
            "bundle_id",
            "work_order_sha256",
            "source",
            "window",
            "tools",
            "profile",
            "limits",
            "commands",
            "artifacts",
            "time_mapping",
            "safety",
            "result_path",
        },
    )
    _reject_forbidden_data_keys(row, path)
    _constant(row["schema_version"], 1, f"{path}.schema_version")
    _constant(row["status"], "completed", f"{path}.status")
    _constant(row["dry_run"], False, f"{path}.dry_run")
    _string(row["implementation_version"], f"{path}.implementation_version")
    job_id = _id(row["job_id"], f"{path}.job_id")
    bundle_id = _id(row["bundle_id"], f"{path}.bundle_id")
    if strict_v2 and re.fullmatch(r"windowbundle_[0-9a-f]{32}", bundle_id) is None:
        _fail(f"{path}.bundle_id", "is not an admitted local-window bundle ID")
    _sha256(row["work_order_sha256"], f"{path}.work_order_sha256")
    if row["result_path"] != request_row["timeline"]["local_window_result_path"]:
        _fail(f"{path}.result_path", "must equal the exact pinned local-window result path")
    source = _object(
        row["source"],
        f"{path}.source",
        {
            "path",
            "expected_sha256",
            "byte_count",
            "media_id",
            "duration_ms",
            "acquisition_result_path",
            "acquisition_result_sha256",
            "stat_before",
            "stat_after",
            "unchanged",
        },
    )
    for key, expected in (
        ("media_id", request_row["parent_media_id"]),
        ("expected_sha256", request_row["parent_media_sha256"]),
        ("byte_count", request_row["parent_media_byte_count"]),
        ("duration_ms", request_row["parent_media_duration_ms"]),
    ):
        if source.get(key) != expected:
            _fail(f"{path}.source.{key}", "does not match pinned parent media")
    for key in ("path", "acquisition_result_path"):
        candidate_path = Path(_string(source[key], f"{path}.source.{key}"))
        if not candidate_path.is_absolute():
            _fail(f"{path}.source.{key}", "must be absolute")
        _reject_forbidden_input_path(candidate_path, f"{path}.source.{key}")
    _sha256(source["acquisition_result_sha256"], f"{path}.source.acquisition_result_sha256")
    _constant(source["unchanged"], True, f"{path}.source.unchanged")
    stats: list[dict[str, Any]] = []
    for stat_name in ("stat_before", "stat_after"):
        stat_row = _object(
            source[stat_name],
            f"{path}.source.{stat_name}",
            {"device", "inode", "byte_count", "mtime_ns"},
        )
        for key in stat_row:
            _integer(stat_row[key], f"{path}.source.{stat_name}.{key}")
        stats.append(stat_row)
    if stats[0] != stats[1] or stats[0]["byte_count"] != source["byte_count"]:
        _fail(f"{path}.source", "before/after file identity must be stable and byte-exact")
    window = _object(
        row["window"],
        f"{path}.window",
        {"window_id", "ordinal", "start_ms", "end_ms", "boundary", "is_partial_tail"},
    )
    _string(window["window_id"], f"{path}.window.window_id")
    ordinal = _bounded_integer(
        window["ordinal"], f"{path}.window.ordinal", minimum=1, maximum=256
    )
    if strict_v2:
        if window["window_id"] != request_row["_v2_window_id"]:
            _fail(f"{path}.window.window_id", "does not match the pinned request window")
        if ordinal != request_row["_v2_window_ordinal"]:
            _fail(f"{path}.window.ordinal", "does not match the pinned request window")
        if window["window_id"] != f"window_{ordinal:06d}":
            _fail(f"{path}.window.window_id", "does not exactly encode ordinal")
        if job_id != f"local-window-{ordinal:06d}":
            _fail(f"{path}.job_id", "does not exactly encode the window ordinal")
    _constant(window["boundary"], "half_open", f"{path}.window.boundary")
    _boolean(window["is_partial_tail"], f"{path}.window.is_partial_tail")
    if (
        strict_v2
        and window["is_partial_tail"]
        and window["end_ms"] != request_row["parent_media_duration_ms"]
    ):
        _fail(f"{path}.window.is_partial_tail", "tail window must end at parent duration")
    mapping = _object(
        row["time_mapping"],
        f"{path}.time_mapping",
        {
            "boundary",
            "source_start_ms",
            "source_end_ms",
            "artifact_zero_maps_to_source_ms",
            "coordinate_precision",
            "extraction_method",
            "byte_exact_source_fragment",
        },
    )
    start = request_row["timeline"]["source_offset_ms"]
    end = request_row["timeline"]["source_end_ms"]
    for field, expected in (("start_ms", start), ("end_ms", end)):
        if window.get(field) != expected:
            _fail(f"{path}.window.{field}", "does not match requested local source window")
    for field, expected in (
        ("boundary", "half_open"),
        ("source_start_ms", start),
        ("source_end_ms", end),
        ("artifact_zero_maps_to_source_ms", start),
    ):
        if mapping.get(field) != expected:
            _fail(f"{path}.time_mapping.{field}", "does not preserve exact source offset")
    _constant(
        mapping["coordinate_precision"],
        "integer_millisecond_contract",
        f"{path}.time_mapping.coordinate_precision",
    )
    _constant(
        mapping["extraction_method"],
        "ffmpeg_accurate_seek_transcode",
        f"{path}.time_mapping.extraction_method",
    )
    _constant(
        mapping["byte_exact_source_fragment"],
        False,
        f"{path}.time_mapping.byte_exact_source_fragment",
    )
    tools = _object(row["tools"], f"{path}.tools", {"ffmpeg", "ffprobe"})
    for tool_name, raw_tool in tools.items():
        tool = _object(
            raw_tool,
            f"{path}.tools.{tool_name}",
            {"path", "sha256", "byte_count", "version_output_sha256", "version_first_line"},
        )
        tool_path = Path(_string(tool["path"], f"{path}.tools.{tool_name}.path"))
        if not tool_path.is_absolute():
            _fail(f"{path}.tools.{tool_name}.path", "must be absolute")
        if strict_v2:
            _reject_forbidden_input_path(tool_path, f"{path}.tools.{tool_name}.path")
        _sha256(tool["sha256"], f"{path}.tools.{tool_name}.sha256")
        _integer(tool["byte_count"], f"{path}.tools.{tool_name}.byte_count", minimum=1)
        _sha256(
            tool["version_output_sha256"],
            f"{path}.tools.{tool_name}.version_output_sha256",
        )
        _string(tool["version_first_line"], f"{path}.tools.{tool_name}.version_first_line")
    profile = _object(
        row["profile"],
        f"{path}.profile",
        {
            "profile_id",
            "ffmpeg_threads",
            "audio_sample_rate_hz",
            "audio_channels",
            "audio_sample_format",
            "flac_compression_level",
            "proxy_width",
            "proxy_height",
            "proxy_fps",
            "proxy_video_codec",
            "proxy_preset",
            "proxy_crf",
            "proxy_audio_codec",
            "proxy_audio_bitrate",
            "video_stream_index",
            "audio_stream_index",
        },
    )
    _constant(profile["profile_id"], "long-window-cpu-v1", f"{path}.profile.profile_id")
    _constant(profile["audio_sample_rate_hz"], 16000, f"{path}.profile.audio_sample_rate_hz")
    _constant(profile["audio_channels"], 1, f"{path}.profile.audio_channels")
    _constant(profile["audio_sample_format"], "s16", f"{path}.profile.audio_sample_format")
    for key in ("ffmpeg_threads", "proxy_width", "proxy_height", "proxy_fps"):
        _integer(profile[key], f"{path}.profile.{key}", minimum=1)
    for key in ("flac_compression_level", "proxy_crf"):
        _integer(profile[key], f"{path}.profile.{key}")
    for key in (
        "proxy_video_codec",
        "proxy_preset",
        "proxy_audio_codec",
        "proxy_audio_bitrate",
    ):
        _string(profile[key], f"{path}.profile.{key}")
    for key in ("video_stream_index", "audio_stream_index"):
        if profile[key] is not None:
            _integer(profile[key], f"{path}.profile.{key}")
    limits = _object(
        row["limits"],
        f"{path}.limits",
        {"max_window_output_bytes", "free_space_floor_bytes", "timeout_seconds"},
    )
    _integer(limits["max_window_output_bytes"], f"{path}.limits.max_window_output_bytes", minimum=1)
    _integer(limits["free_space_floor_bytes"], f"{path}.limits.free_space_floor_bytes")
    _bounded_integer(
        limits["timeout_seconds"], f"{path}.limits.timeout_seconds", minimum=1, maximum=86400
    )
    commands = _array(row["commands"], f"{path}.commands")
    if (strict_v2 and len(commands) != 4) or (not strict_v2 and len(commands) < 2):
        _fail(
            f"{path}.commands",
            "completed admitted local window requires exactly four commands"
            if strict_v2
            else "completed local window requires at least two commands",
        )
    for command_index, raw_command in enumerate(commands):
        command = _array(raw_command, f"{path}.commands[{command_index}]")
        if not command:
            _fail(f"{path}.commands[{command_index}]", "must be nonempty")
        for argument_index, argument in enumerate(command):
            argument_path = f"{path}.commands[{command_index}][{argument_index}]"
            argument_text = _string(argument, argument_path)
            if strict_v2:
                _reject_forbidden_input_path(Path(argument_text), argument_path)
    artifacts = _array(row["artifacts"], f"{path}.artifacts")
    if (strict_v2 and len(artifacts) != 2) or not 1 <= len(artifacts) <= 2:
        _fail(
            f"{path}.artifacts",
            "must contain exactly two admitted local-window artifacts"
            if strict_v2
            else "must contain one or two local-window artifacts",
        )
    matches: list[dict[str, Any]] = []
    seen_kinds: set[str] = set()
    for index, raw_artifact in enumerate(artifacts):
        artifact_path = f"{path}.artifacts[{index}]"
        artifact = _object(
            raw_artifact,
            artifact_path,
            {
                "artifact_id",
                "artifact_kind",
                "path",
                "sha256",
                "byte_count",
                "visibility",
                "normalized_probe",
            },
        )
        _id(artifact["artifact_id"], f"{artifact_path}.artifact_id")
        kind = _choice(
            artifact["artifact_kind"],
            {"window_audio_16khz_mono_flac", "window_low_resolution_cfr_proxy"},
            f"{artifact_path}.artifact_kind",
        )
        if kind in seen_kinds:
            _fail(f"{artifact_path}.artifact_kind", "must be unique")
        seen_kinds.add(kind)
        artifact_file = Path(_string(artifact["path"], f"{artifact_path}.path"))
        if not artifact_file.is_absolute():
            _fail(f"{artifact_path}.path", "must be absolute")
        _reject_forbidden_input_path(artifact_file, f"{artifact_path}.path")
        digest = _sha256(artifact["sha256"], f"{artifact_path}.sha256")
        byte_count = _integer(artifact["byte_count"], f"{artifact_path}.byte_count", minimum=1)
        _constant(artifact["visibility"], "private", f"{artifact_path}.visibility")
        probe = _object(
            artifact["normalized_probe"],
            f"{artifact_path}.normalized_probe",
            {"duration_ms", "video_stream_index", "audio_stream_index", "video", "audio"},
        )
        probe_duration = _integer(
            probe["duration_ms"], f"{artifact_path}.normalized_probe.duration_ms", minimum=1
        )
        for key in ("video_stream_index", "audio_stream_index"):
            if probe[key] is not None:
                _integer(probe[key], f"{artifact_path}.normalized_probe.{key}")
        if strict_v2:
            if abs(probe_duration - (end - start)) > 250:
                _fail(
                    f"{artifact_path}.normalized_probe.duration_ms",
                    "differs from the exact source window by more than 250 ms",
                )
            video = (
                None
                if probe["video"] is None
                else _object(
                    probe["video"],
                    f"{artifact_path}.normalized_probe.video",
                    {
                        "codec_name",
                        "width",
                        "height",
                        "pixel_format",
                        "average_frame_rate",
                    },
                )
            )
            audio = (
                None
                if probe["audio"] is None
                else _object(
                    probe["audio"],
                    f"{artifact_path}.normalized_probe.audio",
                    {"codec_name", "sample_rate_hz", "channels", "sample_format"},
                )
            )
            if kind == "window_audio_16khz_mono_flac":
                if (
                    probe["video_stream_index"] is not None
                    or probe["audio_stream_index"] is None
                    or video is not None
                    or audio is None
                ):
                    _fail(artifact_path, "normalized audio stream layout is inconsistent")
                for key, expected in (
                    ("codec_name", "flac"),
                    ("sample_rate_hz", 16_000),
                    ("channels", 1),
                    ("sample_format", "s16"),
                ):
                    _constant(
                        audio[key],
                        expected,
                        f"{artifact_path}.normalized_probe.audio.{key}",
                    )
            else:
                if (
                    probe["video_stream_index"] is None
                    or probe["audio_stream_index"] is None
                    or video is None
                    or audio is None
                ):
                    _fail(artifact_path, "normalized proxy stream layout is inconsistent")
                for key, expected in (
                    ("codec_name", "h264"),
                    ("width", profile["proxy_width"]),
                    ("height", profile["proxy_height"]),
                    ("pixel_format", "yuv420p"),
                ):
                    _constant(
                        video[key],
                        expected,
                        f"{artifact_path}.normalized_probe.video.{key}",
                    )
                fps = _number(
                    video["average_frame_rate"],
                    f"{artifact_path}.normalized_probe.video.average_frame_rate",
                )
                if abs(fps - profile["proxy_fps"]) > 0.001:
                    _fail(
                        f"{artifact_path}.normalized_probe.video.average_frame_rate",
                        "does not match the local-window profile",
                    )
                for key, expected in (
                    ("codec_name", "aac"),
                    ("sample_rate_hz", 48_000),
                    ("channels", 2),
                ):
                    _constant(
                        audio[key],
                        expected,
                        f"{artifact_path}.normalized_probe.audio.{key}",
                    )
            expected_artifact_id = "artifact_" + hashlib.sha256(
                "\x1f".join(
                    (bundle_id, window["window_id"], kind, digest)
                ).encode()
            ).hexdigest()[:32]
            if artifact["artifact_id"] != expected_artifact_id:
                _fail(
                    f"{artifact_path}.artifact_id",
                    "does not match sealed producer identity",
                )
        else:
            for key in ("video", "audio"):
                if probe[key] is not None and not isinstance(probe[key], dict):
                    _fail(
                        f"{artifact_path}.normalized_probe.{key}",
                        "must be object or null",
                    )
        if (
            digest == request_row["analysis_media_sha256"]
            and f"media_sha256_{digest}" == request_row["analysis_media_id"]
            and byte_count == request_row["analysis_media_byte_count"]
            and abs(probe_duration - request_row["analysis_media_duration_ms"]) <= 250
        ):
            matches.append(artifact)
    if len(matches) != 1:
        _fail(f"{path}.artifacts", "must bind exactly one requested analysis-media artifact")
    if strict_v2 and seen_kinds != {
        "window_audio_16khz_mono_flac",
        "window_low_resolution_cfr_proxy",
    }:
        _fail(f"{path}.artifacts", "must contain both admitted artifact kinds")
    analysis_artifact = matches[0]
    if strict_v2:
        if (
            analysis_artifact["normalized_probe"]["duration_ms"]
            != request_row["analysis_media_duration_ms"]
        ):
            _fail(
                f"{path}.artifacts",
                "selected artifact probe duration does not match catalogue media duration",
            )
        expected_kind = (
            f"local_window:{analysis_artifact['artifact_kind']}:"
            f"{bundle_id}:{window['window_id']}"
        )
        if request_row["_v2_analysis_rendition_kind"] != expected_kind:
            _fail(
                f"{path}.artifacts",
                "selected artifact does not match the pinned analysis rendition kind",
            )
        producer_artifact_id = "artifact_" + hashlib.sha256(
            "\x1f".join(
                (
                    bundle_id,
                    window["window_id"],
                    analysis_artifact["artifact_kind"],
                    analysis_artifact["sha256"],
                )
            ).encode()
        ).hexdigest()[:32]
        if analysis_artifact["artifact_id"] != producer_artifact_id:
            _fail(
                f"{path}.artifacts",
                "selected artifact ID does not match sealed producer identity",
            )
    safety = _object(
        row["safety"],
        f"{path}.safety",
        {
            "network_allowed",
            "credentials_allowed",
            "publication_authority",
            "identity_claims_allowed",
            "source_bytes_preserved",
            "remote_section_download",
        },
    )
    for key in ("network_allowed", "credentials_allowed", "identity_claims_allowed"):
        _constant(safety.get(key), False, f"{path}.safety.{key}")
    _constant(safety.get("publication_authority"), "none", f"{path}.safety.publication_authority")
    _constant(safety["source_bytes_preserved"], True, f"{path}.safety.source_bytes_preserved")
    _constant(safety["remote_section_download"], False, f"{path}.safety.remote_section_download")
    return {
        "bundle_id": bundle_id,
        "window_id": window["window_id"],
        "window_ordinal": ordinal,
        "work_order_sha256": row["work_order_sha256"],
        "implementation_version": row["implementation_version"],
        "source": dict(source),
        "window": dict(window),
        "time_mapping": dict(mapping),
        "analysis_artifact": dict(analysis_artifact),
        "artifacts": [dict(item) for item in artifacts],
    }


class _Catalog:
    def __init__(self, path: Path):
        if not path.is_absolute():
            path = path.resolve()
        self.connection: sqlite3.Connection | None = None
        try:
            before = path.lstat()
        except OSError as error:
            raise ContractError(f"catalog: cannot stat {path}: {error}") from error
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
            raise ContractError("catalog: must be a non-symlink regular SQLite file")
        sidecars = [
            path.with_name(path.name + suffix)
            for suffix in ("-wal", "-shm", "-journal")
            if path.with_name(path.name + suffix).exists()
        ]
        if sidecars:
            raise ContractError(
                "catalog: audit requires a closed, checkpointed SQLite file with no "
                "sidecars; found: " + ", ".join(item.name for item in sidecars)
            )
        uri = f"file:{quote(str(path), safe='/')}?mode=ro&immutable=1"
        try:
            self.connection = sqlite3.connect(uri, uri=True, isolation_level=None)
            self.connection.row_factory = sqlite3.Row
            self.connection.execute("PRAGMA query_only=ON")
            self.connection.execute("BEGIN")
            check = self.connection.execute("PRAGMA quick_check").fetchall()
        except sqlite3.Error as error:
            self.close()
            raise ContractError(
                f"catalog: cannot open query-only snapshot: {error}"
            ) from error
        try:
            after = path.lstat()
        except OSError as error:
            self.close()
            raise ContractError(f"catalog: cannot restat {path}: {error}") from error
        before_identity = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
            before.st_mode,
        )
        after_identity = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
            after.st_mode,
        )
        appeared_sidecars = [
            path.with_name(path.name + suffix)
            for suffix in ("-wal", "-shm", "-journal")
            if path.with_name(path.name + suffix).exists()
        ]
        if before_identity != after_identity or appeared_sidecars:
            self.close()
            raise ContractError(
                "catalog: file identity or sidecar state changed while opening the "
                "immutable audit snapshot"
            )
        if [row[0] for row in check] != ["ok"]:
            self.close()
            raise ContractError(f"catalog: quick_check failed: {[row[0] for row in check]!r}")
        try:
            migrations = [dict(row) for row in self.connection.execute(
                "SELECT version,name,sha256,applied_at FROM schema_migrations ORDER BY version"
            )]
        except sqlite3.Error as error:
            self.close()
            raise ContractError(f"catalog: cannot read migration ledger: {error}") from error
        self.schema_migrations_sha256 = hashlib.sha256(
            _canonical_bytes(migrations)
        ).hexdigest()

    def close(self) -> None:
        connection = getattr(self, "connection", None)
        if connection is not None:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            connection.close()
            self.connection = None

    def __enter__(self) -> "_Catalog":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def one(self, query: str, parameters: tuple[Any, ...], label: str) -> dict[str, Any]:
        try:
            rows = self.connection.execute(query, parameters).fetchall()
        except sqlite3.Error as error:
            _fail(label, f"catalog query failed: {error}")
        if len(rows) != 1:
            _fail(label, f"catalog query expected exactly one row, found {len(rows)}")
        return dict(rows[0])

    def many(self, query: str, parameters: tuple[Any, ...]) -> list[dict[str, Any]]:
        try:
            return [dict(row) for row in self.connection.execute(query, parameters)]
        except sqlite3.Error as error:
            raise ContractError(f"catalog query failed: {error}") from error


def _catalog_binding(
    catalog: _Catalog,
    request_row: dict[str, Any],
    preprocess: dict[str, Any],
    path: str,
    import_envelope_sha256: str | None = None,
    expected_import_batch_id: str | None = None,
) -> dict[str, Any]:
    source = catalog.one(
        "SELECT source_id,platform,source_kind,native_id,canonical_url,access_state "
        "FROM sources WHERE source_id=?",
        (request_row["source_id"],),
        f"{path}.source_id",
    )
    expected_source = {
        "source_id": request_row["source_id"],
        "platform": "youtube",
        "source_kind": "youtube_video",
        "native_id": request_row["source_native_id"],
        "canonical_url": request_row["source_locator"],
        "access_state": "public",
    }
    if source != expected_source:
        _fail(path, "catalog source row does not match the public cohort binding")
    if "_v2_window_id" in request_row:
        recording = catalog.one(
            "SELECT recording_id,review_state,merged_into_recording_id "
            "FROM recordings WHERE recording_id=?",
            (request_row["recording_id"],),
            f"{path}.recording_id",
        )
        if (
            recording["review_state"]
            not in {"metadata_only", "unreviewed", "reviewed"}
            or recording["merged_into_recording_id"] is not None
        ):
            _fail(
                f"{path}.recording_id",
                "catalog recording is no longer an admissible unmerged parent",
            )
    else:
        recording = catalog.one(
            "SELECT recording_id FROM recordings WHERE recording_id=?",
            (request_row["recording_id"],),
            f"{path}.recording_id",
        )
    mappings = catalog.many(
        "SELECT recording_source_id,recording_id,source_id,mapping_role,source_start_ms,"
        "source_end_ms,recording_start_ms,recording_end_ms,mapping_method,confidence_state "
        "FROM recording_sources WHERE recording_id=? AND source_id=? ORDER BY recording_source_id",
        (request_row["recording_id"], request_row["source_id"]),
    )
    if not mappings:
        _fail(path, "catalog has no recording-source mapping")
    rendition = catalog.one(
        "SELECT rendition_id,recording_id,media_id,rendition_kind,review_state FROM renditions "
        "WHERE rendition_id=?",
        (request_row["rendition_id"],),
        f"{path}.rendition_id",
    )
    expected_rendition = {
        "rendition_id": request_row["rendition_id"],
        "recording_id": request_row["recording_id"],
        "media_id": request_row["parent_media_id"],
        "rendition_kind": request_row["rendition_kind"],
        "review_state": rendition["review_state"],
    }
    if rendition != expected_rendition:
        _fail(path, "catalog rendition row does not match request")
    parent = catalog.one(
        "SELECT media_id,sha256,byte_count,duration_ms,integrity_state FROM media_objects WHERE media_id=?",
        (request_row["parent_media_id"],),
        f"{path}.parent_media_id",
    )
    for key, expected in (
        ("sha256", request_row["parent_media_sha256"]),
        ("byte_count", request_row["parent_media_byte_count"]),
        ("duration_ms", request_row["parent_media_duration_ms"]),
        ("integrity_state", "verified"),
    ):
        if parent[key] != expected:
            _fail(f"{path}.parent_media_id", f"catalog {key} does not match request")
    analysis_rows = catalog.many(
        "SELECT media_id,sha256,byte_count,duration_ms,integrity_state FROM media_objects WHERE media_id=?",
        (request_row["analysis_media_id"],),
    )
    if len(analysis_rows) != 1:
        _fail(f"{path}.analysis_media_id", "must exist exactly once in the catalogue")
    analysis = analysis_rows[0]
    for key, expected in (
        ("sha256", request_row["analysis_media_sha256"]),
        ("byte_count", request_row["analysis_media_byte_count"]),
        ("duration_ms", request_row["analysis_media_duration_ms"]),
        ("integrity_state", "verified"),
    ):
        if analysis[key] != expected:
            _fail(f"{path}.analysis_media_id", f"catalog {key} does not match request")
    run = catalog.one(
        "SELECT processing_run_id,stage,implementation_version,model_id,"
        "glossary_revision_id,parameters_json,environment_json,random_seed,"
        "started_at,completed_at,status,error_text FROM processing_runs WHERE processing_run_id=?",
        (preprocess["processing_run_id"],),
        f"{path}.preprocess_result",
    )
    try:
        database_parameters = json.loads(run["parameters_json"])
        database_environment = json.loads(run["environment_json"])
    except (TypeError, json.JSONDecodeError) as error:
        _fail(f"{path}.preprocess_result", f"catalog run JSON is invalid: {error}")
    if (
        run["processing_run_id"] != preprocess["processing_run_id"]
        or run["stage"] != "media_preprocess"
        or run["implementation_version"] != preprocess["implementation_version"]
        or run["model_id"] is not None
        or run["glossary_revision_id"] is not None
        or run["random_seed"] is not None
        or run["started_at"] != preprocess["started_at"]
        or run["completed_at"] != preprocess["completed_at"]
        or run["status"] != "completed"
        or run["error_text"] is not None
        or database_parameters != preprocess["parameters_json"]
        or database_environment != preprocess["environment_json"]
        or hashlib.sha256(_canonical_bytes(database_parameters)).hexdigest()
        != preprocess["recipe_sha256"]
    ):
        _fail(f"{path}.preprocess_result", "catalog processing run does not match result")
    inputs = catalog.many(
        "SELECT run_input_id,processing_run_id,object_type,object_id,input_role,input_sha256 "
        "FROM run_inputs WHERE processing_run_id=? ORDER BY run_input_id",
        (preprocess["processing_run_id"],),
    )
    if len(inputs) != 1:
        _fail(f"{path}.preprocess_result", "preprocess run must have exactly one input")
    input_binding = inputs[0]
    if (
        input_binding["processing_run_id"] != preprocess["processing_run_id"]
        or input_binding["object_type"] != "media"
        or input_binding["object_id"] != request_row["analysis_media_id"]
        or input_binding["input_role"] != "source_media"
        or input_binding["input_sha256"] != request_row["analysis_media_sha256"]
    ):
        _fail(
            f"{path}.preprocess_result",
            "catalog input must be exactly one source_media media object",
        )
    artifacts = catalog.many(
        "SELECT artifact_id,processing_run_id,artifact_kind,sha256,byte_count,schema_version,visibility "
        "FROM artifacts WHERE processing_run_id=? ORDER BY artifact_kind,artifact_id",
        (preprocess["processing_run_id"],),
    )
    expected_artifacts = sorted(
        (
            item["artifact_id"],
            item["artifact_kind"],
            item["sha256"],
            item["byte_count"],
            item["visibility"],
        )
        for item in preprocess["artifacts"].values()
    )
    observed_artifacts = sorted(
        (
            item["artifact_id"],
            item["artifact_kind"],
            item["sha256"],
            item["byte_count"],
            item["visibility"],
        )
        for item in artifacts
    )
    if observed_artifacts != expected_artifacts:
        _fail(f"{path}.preprocess_result", "catalog artifact rows do not match sealed result")

    preprocess_import_batch: dict[str, Any] | None = None
    if import_envelope_sha256 is not None:
        canonical_envelope_sha256 = _sha256(
            import_envelope_sha256,
            f"{path}.preprocess_import_envelope_sha256",
        )
        expected_batch_id = _preprocess_import_batch_id(
            canonical_envelope_sha256
        )
        if expected_import_batch_id is None:
            _fail(
                f"{path}.preprocess_import_batch_id",
                "is required with an import-envelope digest",
            )
        declared_batch_id = _id(
            expected_import_batch_id,
            f"{path}.preprocess_import_batch_id",
        )
        if declared_batch_id != expected_batch_id:
            _fail(
                f"{path}.preprocess_import_batch_id",
                f"must equal deterministic ID {expected_batch_id!r}",
            )
        preprocess_import_batch = catalog.one(
            "SELECT import_batch_id,importer_name,importer_version,input_sha256,"
            "source_snapshot_date,started_at,completed_at,status,statistics_json "
            "FROM import_batches WHERE import_batch_id=?",
            (expected_batch_id,),
            f"{path}.preprocess_result.import_batch",
        )
        expected_batch = {
            "import_batch_id": expected_batch_id,
            "importer_name": PREPROCESS_IMPORTER_NAME,
            "input_sha256": canonical_envelope_sha256,
            "source_snapshot_date": None,
            "started_at": preprocess["started_at"],
            "completed_at": preprocess["completed_at"],
            "status": "completed",
        }
        for key, expected in expected_batch.items():
            if preprocess_import_batch[key] != expected:
                _fail(
                    f"{path}.preprocess_result.import_batch.{key}",
                    "does not match the completed canonical-envelope import; "
                    "raw byte formatting is separately pinned by "
                    "preprocess_result_raw_sha256",
                )
        _string(
            preprocess_import_batch["importer_version"],
            f"{path}.preprocess_result.import_batch.importer_version",
        )
        preprocess_import_batch["statistics_json"] = _catalog_json(
            preprocess_import_batch["statistics_json"],
            f"{path}.preprocess_result.import_batch.statistics_json",
        )

    # `catalog_records` is producer-supplied convenience data, not authority.
    # When present, every exact-shaped row must resolve to the identical row in
    # this read-only catalogue snapshot. This prevents neutral keys or altered
    # nested JSON from turning the preprocess envelope into a transcript input.
    declared_catalog = preprocess["catalog_records"]
    matched_catalog: dict[str, list[dict[str, Any]]] = {
        key: [] for key in declared_catalog
    }

    def parse_database_json(row: dict[str, Any], key: str, label: str) -> None:
        if row[key] is None:
            return
        try:
            row[key] = json.loads(row[key])
        except (TypeError, json.JSONDecodeError) as error:
            _fail(label, f"catalog {key} is invalid JSON: {error}")

    catalog_queries: dict[
        str, tuple[str, tuple[str, ...], tuple[str, ...], tuple[str, ...]]
    ] = {
        "media_objects": (
            "SELECT media_id,sha256,byte_count,media_kind,mime_type,container,duration_ms,"
            "ffprobe_json,first_cataloged_at,integrity_state FROM media_objects WHERE media_id=?",
            ("media_id",),
            ("ffprobe_json",),
            (),
        ),
        "media_locations": (
            "SELECT media_location_id,media_id,storage_uri,storage_class,verified_at,is_primary "
            "FROM media_locations WHERE media_id=? AND storage_uri=?",
            ("media_id", "storage_uri"),
            (),
            ("media_location_id",),
        ),
        "processing_runs": (
            "SELECT processing_run_id,stage,implementation_version,parameters_json,"
            "environment_json,started_at,completed_at,status FROM processing_runs "
            "WHERE processing_run_id=?",
            ("processing_run_id",),
            ("parameters_json", "environment_json"),
            (),
        ),
        "run_inputs": (
            "SELECT run_input_id,processing_run_id,object_type,object_id,input_role,input_sha256 "
            "FROM run_inputs WHERE processing_run_id=? AND object_type=? AND object_id=? "
            "AND input_role=?",
            ("processing_run_id", "object_type", "object_id", "input_role"),
            (),
            ("run_input_id",),
        ),
        "artifacts": (
            "SELECT artifact_id,processing_run_id,artifact_kind,storage_uri,sha256,byte_count,"
            "schema_version,visibility FROM artifacts WHERE artifact_id=?",
            ("artifact_id",),
            (),
            (),
        ),
        "media_derivations": (
            "SELECT child_media_id,parent_media_id,derivation_kind,processing_run_id,metadata_json "
            "FROM media_derivations WHERE child_media_id=? AND parent_media_id=? "
            "AND derivation_kind=?",
            ("child_media_id", "parent_media_id", "derivation_kind"),
            ("metadata_json",),
            (),
        ),
    }
    for record_kind, declared_rows in declared_catalog.items():
        query, identity_keys, json_keys, producer_local_id_keys = catalog_queries[
            record_kind
        ]
        for index, declared in enumerate(declared_rows):
            label = f"{path}.preprocess_result.catalog_records.{record_kind}[{index}]"
            observed = catalog.one(
                query,
                tuple(declared[key] for key in identity_keys),
                label,
            )
            for key in json_keys:
                parse_database_json(observed, key, label)
            comparable_observed = dict(observed)
            comparable_declared = dict(declared)
            for key in producer_local_id_keys:
                comparable_observed.pop(key)
                comparable_declared.pop(key)
            if (
                record_kind == "media_objects"
                and declared["media_id"] == request_row["analysis_media_id"]
            ):
                # Acquisition may already have catalogued this source with its
                # own normalized probe. The preprocessing probe is instead
                # bound above to the registered, hash-pinned probe artifact.
                comparable_observed.pop("ffprobe_json")
                comparable_declared.pop("ffprobe_json")
                if (
                    request_row.get("_v2_analysis_rendition_kind", "").startswith(
                        "local_window:window_low_resolution_cfr_proxy:"
                    )
                    and observed["container"] == "mp4"
                    and declared["container"]
                    in {"mp4", "mov,mp4,m4a,3gp,3g2,mj2"}
                ):
                    # Local-window admission records the normalized proxy
                    # container as ``mp4``. A later ffprobe-based preprocessing
                    # import declares ffprobe's equivalent multi-name string,
                    # while the catalogue deliberately preserves the earlier
                    # non-null admission value.
                    comparable_observed.pop("container")
                    comparable_declared.pop("container")
            if (
                record_kind == "media_locations"
                and declared["media_id"] == request_row["analysis_media_id"]
            ):
                # Import may classify the immutable input cache more narrowly
                # than the producer's generic `local` label.
                if observed["storage_class"] not in {
                    "local",
                    "local_derived",
                    "local_hot_cache",
                    "private_local",
                }:
                    _fail(label, "catalog source-media storage_class is unsupported")
                comparable_observed.pop("storage_class")
                comparable_declared.pop("storage_class")
            if comparable_observed != comparable_declared:
                _fail(label, "does not exactly match the query-only catalogue row")
            matched_catalog[record_kind].append(observed)
    relevant = {
        "source": source,
        "recording": recording,
        "recording_sources": mappings,
        "rendition": rendition,
        "parent_media": parent,
        "analysis_media": analysis,
        "processing_run": {
            **run,
            "parameters_json": database_parameters,
            "environment_json": database_environment,
        },
        "run_inputs": inputs,
        "artifacts": artifacts,
        "declared_preprocess_catalog_records": matched_catalog,
    }
    if preprocess_import_batch is not None:
        # This is a schema-v2 lineage extension. Omitting the key entirely for
        # schema v1 preserves the historical relevant-row hash and proposal ID;
        # hashing a new JSON null here would be a compatibility break.
        relevant["preprocess_import_batch"] = preprocess_import_batch
    return {
        "relevant_rows_sha256": hashlib.sha256(_canonical_bytes(relevant)).hexdigest(),
        "relevant_rows": relevant,
    }


def _catalog_json(value: object, path: str) -> dict[str, Any]:
    if not isinstance(value, str):
        _fail(path, "catalog value must be serialized JSON")

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, item in pairs:
            if key in output:
                _fail(path, f"catalog JSON contains duplicate key {key!r}")
            output[key] = item
        return output

    def reject_constant(constant: str) -> None:
        _fail(path, f"catalog JSON contains non-finite number {constant!r}")

    try:
        parsed = json.loads(
            value,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except json.JSONDecodeError as error:
        _fail(path, f"catalog JSON is invalid: {error}")
    if not isinstance(parsed, dict):
        _fail(path, "catalog JSON must decode to an object")
    _reject_forbidden_data_keys(parsed, path)
    return parsed


def _catalog_local_window_binding(
    catalog: _Catalog,
    request_row: dict[str, Any],
    local: dict[str, Any],
    local_result_sha256: str,
    path: str,
) -> dict[str, Any]:
    """Bind a selected local derivative to its exact catalogue admission lineage."""

    artifact = local["analysis_artifact"]
    parent_rendition = catalog.one(
        "SELECT rendition_id,recording_id,media_id,rendition_kind,review_state "
        "FROM renditions WHERE rendition_id=?",
        (request_row["rendition_id"],),
        f"{path}.parent_rendition_id",
    )
    if parent_rendition != {
        "rendition_id": request_row["rendition_id"],
        "recording_id": request_row["recording_id"],
        "media_id": request_row["parent_media_id"],
        "rendition_kind": "acquired_source_media",
        "review_state": parent_rendition["review_state"],
    } or parent_rendition["review_state"] not in {"unreviewed", "reviewed"}:
        _fail(f"{path}.parent_rendition_id", "is not an admissible acquired parent")
    rendition = catalog.one(
        "SELECT rendition_id,recording_id,media_id,rendition_kind,review_state,metadata_json "
        "FROM renditions WHERE rendition_id=?",
        (request_row["_v2_analysis_rendition_id"],),
        f"{path}.analysis_rendition_id",
    )
    expected_rendition = {
        "rendition_id": request_row["_v2_analysis_rendition_id"],
        "recording_id": request_row["recording_id"],
        "media_id": request_row["analysis_media_id"],
        "rendition_kind": request_row["_v2_analysis_rendition_kind"],
    }
    for key, expected in expected_rendition.items():
        if rendition[key] != expected:
            _fail(f"{path}.analysis_rendition_id", f"catalogue {key} does not match")
    if rendition["review_state"] not in {"unreviewed", "reviewed"}:
        _fail(f"{path}.analysis_rendition_id", "catalogue rendition is not admissible")

    result_uri = Path(request_row["timeline"]["local_window_result_path"]).as_uri()
    evidence = {
        "contract_version": 1,
        "run_semantics": "catalog_admission_verification_not_extraction_execution",
        "local_window_result_sha256": local_result_sha256,
        "local_window_result_uri": result_uri,
        "acquisition_result_sha256": local["source"]["acquisition_result_sha256"],
        "source_media_id": request_row["parent_media_id"],
        "source_time_mapping": local["time_mapping"],
        "window": local["window"],
        "normalized_probe": artifact["normalized_probe"],
        "boundary_calibration_state": "not_calibrated",
        "representation_is_original_source": False,
        "publication_state": "withheld_by_default",
        "identity_authority": "none",
    }
    rendition_metadata = _catalog_json(
        rendition["metadata_json"], f"{path}.analysis_rendition.metadata_json"
    )
    expected_rendition_metadata_keys = set(evidence) | {
        "acquisition_import_batch_id",
        "parent_rendition_id",
        "recording_coordinate_mapping",
    }
    _object(
        rendition_metadata,
        f"{path}.analysis_rendition.metadata_json",
        expected_rendition_metadata_keys,
    )
    for key, expected in evidence.items():
        if rendition_metadata[key] != expected:
            _fail(
                f"{path}.analysis_rendition.metadata_json.{key}",
                "does not match the sealed local-window result",
            )
    _id(
        rendition_metadata["acquisition_import_batch_id"],
        f"{path}.analysis_rendition.metadata_json.acquisition_import_batch_id",
    )
    _constant(
        rendition_metadata["parent_rendition_id"],
        request_row["rendition_id"],
        f"{path}.analysis_rendition.metadata_json.parent_rendition_id",
    )
    coordinate_mapping = _object(
        rendition_metadata["recording_coordinate_mapping"],
        f"{path}.analysis_rendition.metadata_json.recording_coordinate_mapping",
        {
            "recording_start_ms",
            "recording_end_ms",
            "state",
            "basis_ids",
            "timeline_mapping_kind",
        },
    )
    for key in ("recording_start_ms", "recording_end_ms"):
        if coordinate_mapping[key] is not None:
            _bounded_integer(
                coordinate_mapping[key],
                f"{path}.analysis_rendition.metadata_json.recording_coordinate_mapping.{key}",
                maximum=MAX_MEDIA_DURATION_MS,
            )
    if (coordinate_mapping["recording_start_ms"] is None) != (
        coordinate_mapping["recording_end_ms"] is None
    ):
        _fail(
            f"{path}.analysis_rendition.metadata_json.recording_coordinate_mapping",
            "recording coordinates must both be present or both be null",
        )
    coordinates_present = coordinate_mapping["recording_start_ms"] is not None
    if (
        coordinate_mapping["recording_start_ms"] is not None
        and coordinate_mapping["recording_end_ms"]
        <= coordinate_mapping["recording_start_ms"]
    ):
        _fail(
            f"{path}.analysis_rendition.metadata_json.recording_coordinate_mapping",
            "recording coordinates must form a non-empty interval",
        )
    mapping_kind = _choice(
        coordinate_mapping["timeline_mapping_kind"],
        {"estimated", "unknown"},
        f"{path}.analysis_rendition.metadata_json.recording_coordinate_mapping.timeline_mapping_kind",
    )
    basis_ids = _array(
        coordinate_mapping["basis_ids"],
        f"{path}.analysis_rendition.metadata_json.recording_coordinate_mapping.basis_ids",
    )
    if not basis_ids:
        _fail(
            f"{path}.analysis_rendition.metadata_json.recording_coordinate_mapping.basis_ids",
            "must identify its catalogue mapping basis",
        )
    for index, basis_id in enumerate(basis_ids):
        _id(
            basis_id,
            f"{path}.analysis_rendition.metadata_json.recording_coordinate_mapping.basis_ids[{index}]",
        )
    _string(
        coordinate_mapping["state"],
        f"{path}.analysis_rendition.metadata_json.recording_coordinate_mapping.state",
    )
    if coordinates_present:
        if mapping_kind != "estimated" or not coordinate_mapping["state"].startswith(
            "estimated_from_"
        ):
            _fail(
                f"{path}.analysis_rendition.metadata_json.recording_coordinate_mapping",
                "asserted recording coordinates require an estimated mapping basis",
            )
    elif (
        mapping_kind != "unknown"
        or coordinate_mapping["state"] != "unasserted_no_unique_catalog_transform"
    ):
        _fail(
            f"{path}.analysis_rendition.metadata_json.recording_coordinate_mapping",
            "null recording coordinates require the exact unasserted mapping state",
        )

    derivation_kind = request_row["_v2_analysis_rendition_kind"]
    derivation = catalog.one(
        "SELECT child_media_id,parent_media_id,derivation_kind,processing_run_id,metadata_json "
        "FROM media_derivations WHERE child_media_id=? AND parent_media_id=? "
        "AND derivation_kind=?",
        (
            request_row["analysis_media_id"],
            request_row["parent_media_id"],
            derivation_kind,
        ),
        f"{path}.media_derivation",
    )
    derivation_metadata = _catalog_json(
        derivation["metadata_json"], f"{path}.media_derivation.metadata_json"
    )
    expected_derivation_metadata = {
        **evidence,
        "acquisition_import_batch_id": rendition_metadata[
            "acquisition_import_batch_id"
        ],
    }
    if derivation_metadata != expected_derivation_metadata:
        _fail(
            f"{path}.media_derivation.metadata_json",
            "does not exactly match sealed local-window lineage",
        )

    catalog_artifact = catalog.one(
        "SELECT artifact_id,processing_run_id,artifact_kind,storage_uri,sha256,byte_count,"
        "schema_version,visibility,metadata_json FROM artifacts WHERE artifact_id=?",
        (artifact["artifact_id"],),
        f"{path}.artifact_id",
    )
    expected_artifact = {
        "artifact_id": artifact["artifact_id"],
        "processing_run_id": derivation["processing_run_id"],
        "artifact_kind": artifact["artifact_kind"],
        "storage_uri": Path(artifact["path"]).as_uri(),
        "sha256": artifact["sha256"],
        "byte_count": artifact["byte_count"],
        "schema_version": 1,
        "visibility": "private",
    }
    for key, expected in expected_artifact.items():
        if catalog_artifact[key] != expected:
            _fail(f"{path}.artifact_id", f"catalogue {key} does not match")
    artifact_metadata = _catalog_json(
        catalog_artifact["metadata_json"], f"{path}.artifact.metadata_json"
    )
    if artifact_metadata != {
        **expected_derivation_metadata,
        "media_id": request_row["analysis_media_id"],
    }:
        _fail(
            f"{path}.artifact.metadata_json",
            "does not exactly match sealed local-window lineage",
        )
    if derivation["processing_run_id"] != catalog_artifact["processing_run_id"]:
        _fail(path, "catalogue derivative and artifact admission runs differ")

    admission_run = catalog.one(
        "SELECT processing_run_id,stage,implementation_version,model_id,"
        "glossary_revision_id,parameters_json,environment_json,random_seed,"
        "started_at,completed_at,status,error_text FROM processing_runs "
        "WHERE processing_run_id=?",
        (derivation["processing_run_id"],),
        f"{path}.admission_processing_run",
    )
    if (
        admission_run["stage"] != "local_window_result_admission"
        or admission_run["implementation_version"] != "local-window-catalog-bridge/1"
        or admission_run["model_id"] is not None
        or admission_run["glossary_revision_id"] is not None
        or admission_run["random_seed"] is not None
        or admission_run["status"] != "completed"
        or admission_run["error_text"] is not None
        or admission_run["completed_at"] != admission_run["started_at"]
    ):
        _fail(
            f"{path}.admission_processing_run",
            "is not an exact completed local-window admission run",
        )
    admission_artifact_rows = catalog.many(
        "SELECT artifact_id,processing_run_id,artifact_kind,storage_uri,sha256,byte_count,"
        "schema_version,visibility,metadata_json FROM artifacts WHERE processing_run_id=? "
        "ORDER BY artifact_kind,artifact_id",
        (admission_run["processing_run_id"],),
    )
    local_artifacts_by_id = {
        item["artifact_id"]: item for item in local["artifacts"]
    }
    if len(local_artifacts_by_id) != 2 or {
        item["artifact_id"] for item in admission_artifact_rows
    } != set(local_artifacts_by_id):
        _fail(
            f"{path}.admission_processing_run",
            "catalogue admission artifact set differs from the sealed two-artifact result",
        )
    parsed_admission_artifacts: list[dict[str, Any]] = []
    for index, admitted_artifact in enumerate(admission_artifact_rows):
        result_artifact = local_artifacts_by_id[admitted_artifact["artifact_id"]]
        artifact_label = f"{path}.admission_artifacts[{index}]"
        expected_descriptor = {
            "artifact_id": result_artifact["artifact_id"],
            "processing_run_id": admission_run["processing_run_id"],
            "artifact_kind": result_artifact["artifact_kind"],
            "storage_uri": Path(result_artifact["path"]).as_uri(),
            "sha256": result_artifact["sha256"],
            "byte_count": result_artifact["byte_count"],
            "schema_version": 1,
            "visibility": "private",
        }
        for key, expected in expected_descriptor.items():
            if admitted_artifact[key] != expected:
                _fail(artifact_label, f"catalogue {key} does not match sealed result")
        admitted_metadata = _catalog_json(
            admitted_artifact["metadata_json"], f"{artifact_label}.metadata_json"
        )
        expected_metadata = {
            **expected_derivation_metadata,
            "normalized_probe": result_artifact["normalized_probe"],
            "media_id": f"media_sha256_{result_artifact['sha256']}",
        }
        if admitted_metadata != expected_metadata:
            _fail(
                f"{artifact_label}.metadata_json",
                "does not exactly match sealed local-window lineage",
            )
        parsed_admission_artifacts.append(
            {**admitted_artifact, "metadata_json": admitted_metadata}
        )
    parameters = _catalog_json(
        admission_run["parameters_json"],
        f"{path}.admission_processing_run.parameters_json",
    )
    environment = _catalog_json(
        admission_run["environment_json"],
        f"{path}.admission_processing_run.environment_json",
    )
    _object(
        parameters,
        f"{path}.admission_processing_run.parameters_json",
        {
            "contract_version",
            "run_semantics",
            "local_window_result_sha256",
            "local_window_result_uri",
            "local_window_implementation_version",
            "work_order_sha256",
            "bundle_id",
            "window",
            "time_mapping",
            "acquisition_result_sha256",
            "acquisition_import_sha256",
            "acquisition_import_batch_id",
            "acquisition_processing_run_id",
            "catalog_context_basis",
        },
    )
    for key, expected in (
        ("contract_version", 1),
        ("run_semantics", "catalog_admission_verification_not_extraction_execution"),
        ("local_window_result_sha256", local_result_sha256),
        ("local_window_result_uri", result_uri),
        ("local_window_implementation_version", local["implementation_version"]),
        ("work_order_sha256", local["work_order_sha256"]),
        ("bundle_id", local["bundle_id"]),
        ("window", local["window"]),
        ("time_mapping", local["time_mapping"]),
        ("acquisition_result_sha256", local["source"]["acquisition_result_sha256"]),
        (
            "acquisition_import_batch_id",
            rendition_metadata["acquisition_import_batch_id"],
        ),
    ):
        if parameters[key] != expected:
            _fail(
                f"{path}.admission_processing_run.parameters_json.{key}",
                "does not match the sealed result or catalogue lineage",
            )
    for key in (
        "acquisition_import_sha256",
        "acquisition_processing_run_id",
    ):
        (_sha256 if key.endswith("sha256") else _id)(
            parameters[key], f"{path}.admission_processing_run.parameters_json.{key}"
        )
    contexts = _array(
        parameters["catalog_context_basis"],
        f"{path}.admission_processing_run.parameters_json.catalog_context_basis",
    )
    if not contexts:
        _fail(
            f"{path}.admission_processing_run.parameters_json.catalog_context_basis",
            "must not be empty",
        )
    matched_context = 0
    parsed_contexts: list[dict[str, Any]] = []
    context_mapping_sources: list[dict[str, str]] = []
    for index, raw_context in enumerate(contexts):
        context_path = (
            f"{path}.admission_processing_run.parameters_json."
            f"catalog_context_basis[{index}]"
        )
        context = _object(
            raw_context,
            context_path,
            {
                "recording_id",
                "parent_rendition_id",
                "parent_rendition_review_state",
                "source_mappings",
            },
        )
        _id(context["recording_id"], f"{context_path}.recording_id")
        _id(context["parent_rendition_id"], f"{context_path}.parent_rendition_id")
        _choice(
            context["parent_rendition_review_state"],
            {"unreviewed", "reviewed"},
            f"{context_path}.parent_rendition_review_state",
        )
        source_mappings = _array(
            context["source_mappings"], f"{context_path}.source_mappings"
        )
        if not source_mappings:
            _fail(f"{context_path}.source_mappings", "must not be empty")
        parsed_mappings: list[dict[str, Any]] = []
        for mapping_index, raw_mapping in enumerate(source_mappings):
            mapping_path = f"{context_path}.source_mappings[{mapping_index}]"
            mapping = _object(
                raw_mapping,
                mapping_path,
                {
                    "recording_source_id",
                    "mapping_role",
                    "source_start_ms",
                    "source_end_ms",
                    "recording_start_ms",
                    "recording_end_ms",
                    "mapping_method",
                    "confidence_state",
                },
            )
            observed_mapping = catalog.one(
                "SELECT recording_source_id,source_id,mapping_role,source_start_ms,source_end_ms,"
                "recording_start_ms,recording_end_ms,mapping_method,confidence_state "
                "FROM recording_sources WHERE recording_source_id=? AND recording_id=?",
                (
                    mapping["recording_source_id"],
                    context["recording_id"],
                ),
                mapping_path,
            )
            mapping_source_id = _id(
                observed_mapping.pop("source_id"), f"{mapping_path}.catalog_source_id"
            )
            if observed_mapping != mapping:
                _fail(mapping_path, "does not exactly match the query-only catalogue")
            context_mapping_sources.append(
                {
                    "recording_source_id": mapping["recording_source_id"],
                    "source_id": mapping_source_id,
                }
            )
            if mapping["confidence_state"] not in {"metadata_only", "reviewed"}:
                _fail(mapping_path, "does not have admissible mapping confidence")
            full_source_roles = {
                "archive_original_file",
                "complete_source",
                "current_platform_listing",
                "legacy_catalog_mapping",
                "validated_platform_listing",
            }
            coordinates = (
                mapping["source_start_ms"],
                mapping["source_end_ms"],
                mapping["recording_start_ms"],
                mapping["recording_end_ms"],
            )
            explicitly_covers_window = (
                mapping["mapping_role"] in full_source_roles
                and mapping["source_start_ms"] is not None
                and mapping["source_end_ms"] is not None
                and mapping["source_start_ms"]
                <= request_row["timeline"]["source_offset_ms"]
                and mapping["source_end_ms"]
                >= request_row["timeline"]["source_end_ms"]
            )
            declares_full_source = (
                all(value is None for value in coordinates)
                and mapping["mapping_role"] in full_source_roles
            )
            if not (explicitly_covers_window or declares_full_source):
                _fail(
                    mapping_path,
                    "does not safely cover the sealed local source window",
                )
            parsed_mappings.append(dict(mapping))
        parsed_contexts.append({**dict(context), "source_mappings": parsed_mappings})
        if (
            context["recording_id"] == request_row["recording_id"]
            and context["parent_rendition_id"] == request_row["rendition_id"]
        ):
            matched_context += 1
    if matched_context != 1:
        _fail(
            f"{path}.admission_processing_run.parameters_json.catalog_context_basis",
            "must contain exactly one matching parent recording/rendition context",
        )
    expected_environment = {
        "observation_timestamp_basis": "operator_supplied_catalog_admission_time",
        "producer_extraction_time_state": "not_present_in_local_window_result_v1",
        "network_access_performed": False,
        "credentials_used": False,
        "publication_authority": "none",
        "identity_claims_allowed": False,
    }
    if environment != expected_environment:
        _fail(
            f"{path}.admission_processing_run.environment_json",
            "does not match the fail-closed admission environment",
        )
    expected_run_id = _stable_id(
        "run",
        "local_window_result_admission",
        local_result_sha256,
        request_row["parent_media_id"],
        parameters["acquisition_import_batch_id"],
    )
    if admission_run["processing_run_id"] != expected_run_id:
        _fail(
            f"{path}.admission_processing_run.processing_run_id",
            "does not match deterministic local-window admission identity",
        )
    inputs = catalog.many(
        "SELECT run_input_id,processing_run_id,object_type,object_id,input_role,input_sha256 "
        "FROM run_inputs WHERE processing_run_id=? ORDER BY object_type,object_id,input_role",
        (admission_run["processing_run_id"],),
    )
    expected_inputs = sorted(
        (
            {
                "run_input_id": _stable_id(
                    "rin",
                    admission_run["processing_run_id"],
                    "media",
                    request_row["parent_media_id"],
                    "verified_acquired_parent_media",
                ),
                "processing_run_id": admission_run["processing_run_id"],
                "object_type": "media",
                "object_id": request_row["parent_media_id"],
                "input_role": "verified_acquired_parent_media",
                "input_sha256": request_row["parent_media_sha256"],
            },
            {
                "run_input_id": _stable_id(
                    "rin",
                    admission_run["processing_run_id"],
                    "import_batch",
                    parameters["acquisition_import_batch_id"],
                    "verified_acquisition_catalog_admission",
                ),
                "processing_run_id": admission_run["processing_run_id"],
                "object_type": "import_batch",
                "object_id": parameters["acquisition_import_batch_id"],
                "input_role": "verified_acquisition_catalog_admission",
                "input_sha256": parameters["acquisition_result_sha256"],
            },
        ),
        key=lambda item: (item["object_type"], item["object_id"], item["input_role"]),
    )
    if inputs != expected_inputs:
        _fail(
            f"{path}.admission_processing_run",
            "catalogue admission inputs do not exactly match local-window lineage",
        )

    spans = catalog.many(
        "SELECT timeline_map_span_id,rendition_id,ordinal,media_start_ms,media_end_ms,"
        "recording_start_ms,recording_end_ms,mapping_kind,confidence_state "
        "FROM timeline_map_spans WHERE rendition_id=? ORDER BY ordinal,timeline_map_span_id",
        (request_row["_v2_analysis_rendition_id"],),
    )
    if len(spans) != 1:
        _fail(f"{path}.timeline_map_spans", "must contain exactly one local span")
    span = spans[0]
    mapped_duration = min(
        request_row["analysis_media_duration_ms"],
        request_row["timeline"]["source_end_ms"]
        - request_row["timeline"]["source_offset_ms"],
    )
    for key, expected in (
        ("timeline_map_span_id", _stable_id("tms", request_row["_v2_analysis_rendition_id"], 0)),
        ("rendition_id", request_row["_v2_analysis_rendition_id"]),
        ("ordinal", 0),
        ("media_start_ms", 0),
        ("media_end_ms", mapped_duration),
        ("recording_start_ms", coordinate_mapping["recording_start_ms"]),
        ("recording_end_ms", coordinate_mapping["recording_end_ms"]),
        ("mapping_kind", coordinate_mapping["timeline_mapping_kind"]),
        ("confidence_state", "metadata_only"),
    ):
        if span[key] != expected:
            _fail(f"{path}.timeline_map_spans", f"catalogue {key} does not match")

    relevant = {
        "parent_rendition": parent_rendition,
        "analysis_rendition": {**rendition, "metadata_json": rendition_metadata},
        "media_derivation": {**derivation, "metadata_json": derivation_metadata},
        "artifacts": parsed_admission_artifacts,
        "admission_processing_run": {
            **admission_run,
            "parameters_json": {**parameters, "catalog_context_basis": parsed_contexts},
            "environment_json": environment,
        },
        "admission_run_inputs": inputs,
        "catalog_context_mapping_sources": sorted(
            context_mapping_sources,
            key=lambda item: (item["recording_source_id"], item["source_id"]),
        ),
        "timeline_map_span": span,
    }
    return {
        "relevant_rows": relevant,
        "relevant_rows_sha256": hashlib.sha256(_canonical_bytes(relevant)).hexdigest(),
        "processing_run_id": admission_run["processing_run_id"],
    }


def _silence_overlap(start: int, end: int, silences: list[tuple[int, int]]) -> int:
    total = 0
    for silence_start, silence_end in silences:
        if silence_end <= start:
            continue
        if silence_start >= end:
            break
        total += max(0, min(end, silence_end) - max(start, silence_start))
    return total


def _route_recording(
    request_id: str,
    row: dict[str, Any],
    preprocess: dict[str, Any],
    policy: dict[str, int],
) -> list[dict[str, Any]]:
    routing = preprocess["routing"]
    span = row["timeline"]["source_end_ms"] - row["timeline"]["source_offset_ms"]
    effective_duration = min(
        routing["duration_ms"], row["analysis_media_duration_ms"], span
    )
    if effective_duration < policy["minimum_interval_duration_ms"]:
        return []
    target = min(policy["interval_duration_ms"], effective_duration)
    if target < policy["minimum_interval_duration_ms"]:
        return []

    candidates: dict[tuple[int, int], tuple[str, int]] = {}

    def add(start: int, basis: str, anchor: int) -> None:
        bounded = max(0, min(start, effective_duration - target))
        end = bounded + target
        key = (bounded, end)
        priorities = {"scene_anchor": 0, "silence_boundary": 1, "periodic": 2}
        previous = candidates.get(key)
        if previous is None or priorities[basis] < priorities[previous[0]]:
            candidates[key] = (basis, anchor)

    for scene in routing["scenes"]:
        add(scene["timestamp_ms"] - target // 2, "scene_anchor", scene["timestamp_ms"])
    for _, silence_end in routing["silences"]:
        if silence_end < effective_duration:
            add(silence_end, "silence_boundary", silence_end)
    periodic = 0
    while periodic < effective_duration:
        add(periodic, "periodic", periodic)
        periodic += policy["periodic_stride_ms"]

    ranked: list[dict[str, Any]] = []
    basis_priority = {"scene_anchor": 0, "silence_boundary": 1, "periodic": 2}
    for (start, end), (basis, anchor) in candidates.items():
        overlap = _silence_overlap(start, end, routing["silences"])
        fraction = overlap * 1_000_000 // (end - start)
        if fraction > policy["max_silence_fraction_millionths"]:
            continue
        ranked.append(
            {
                "local_start_ms": start,
                "local_end_ms": end,
                "proposal_basis": basis,
                "anchor_local_ms": anchor,
                "estimated_silence_overlap_ms": overlap,
                "estimated_silence_fraction_millionths": fraction,
                "_rank": (fraction, basis_priority[basis], start, end),
            }
        )
    ranked.sort(key=lambda item: item["_rank"])
    selected: list[dict[str, Any]] = []
    gap = policy["minimum_gap_ms"]
    for candidate in ranked:
        start = candidate["local_start_ms"]
        end = candidate["local_end_ms"]
        if any(
            not (
                end + gap <= old["local_start_ms"]
                or start >= old["local_end_ms"] + gap
            )
            for old in selected
        ):
            continue
        candidate.pop("_rank")
        selected.append(candidate)
        if len(selected) >= policy["max_intervals_per_recording"]:
            break
    selected.sort(key=lambda item: (item["local_start_ms"], item["local_end_ms"]))
    offset = row["timeline"]["source_offset_ms"]
    output: list[dict[str, Any]] = []
    for item in selected:
        start = offset + item["local_start_ms"]
        end = offset + item["local_end_ms"]
        output.append(
            {
                "interval_id": _stable_id(
                    "proposal_interval",
                    request_id,
                    row["recording_id"],
                    start,
                    end,
                    item["proposal_basis"],
                ),
                "status": "proposal_unreviewed",
                "boundary": "half_open",
                "local_start_ms": item["local_start_ms"],
                "local_end_ms": item["local_end_ms"],
                "start_ms": start,
                "end_ms": end,
                "duration_ms": end - start,
                "proposal_basis": item["proposal_basis"],
                "anchor_local_ms": item["anchor_local_ms"],
                "estimated_silence_overlap_ms": item["estimated_silence_overlap_ms"],
                "estimated_silence_fraction_millionths": item[
                    "estimated_silence_fraction_millionths"
                ],
                "reviewer_completion": {
                    "include": None,
                    "split": None,
                    "stratum_id": None,
                    "flags": {
                        "language_tags": None,
                        "code_switch": None,
                        "speaker_overlap": None,
                        "playback_speech": None,
                        "noise": None,
                    },
                },
            }
        )
    return output


def _rank_window_candidates(
    row: dict[str, Any],
    preprocess: dict[str, Any],
    policy: dict[str, int],
) -> list[dict[str, Any]]:
    """Rank one window without applying a per-window interval cap."""

    routing = preprocess["routing"]
    span = row["timeline"]["source_end_ms"] - row["timeline"]["source_offset_ms"]
    effective_duration = min(
        routing["duration_ms"], row["analysis_media_duration_ms"], span
    )
    if effective_duration < policy["minimum_interval_duration_ms"]:
        return []
    target = min(policy["interval_duration_ms"], effective_duration)
    if target < policy["minimum_interval_duration_ms"]:
        return []
    candidates: dict[tuple[int, int], tuple[str, int]] = {}
    priority = {"scene_anchor": 0, "silence_boundary": 1, "periodic": 2}

    def add(start: int, basis: str, anchor: int) -> None:
        bounded = max(0, min(start, effective_duration - target))
        key = (bounded, bounded + target)
        previous = candidates.get(key)
        if previous is None or priority[basis] < priority[previous[0]]:
            candidates[key] = (basis, anchor)

    for scene in routing["scenes"]:
        if scene["timestamp_ms"] <= effective_duration:
            add(
                scene["timestamp_ms"] - target // 2,
                "scene_anchor",
                scene["timestamp_ms"],
            )
    for _, silence_end in routing["silences"]:
        if silence_end < effective_duration:
            add(silence_end, "silence_boundary", silence_end)
    periodic = 0
    while periodic < effective_duration:
        add(periodic, "periodic", periodic)
        periodic += policy["periodic_stride_ms"]

    ranked: list[dict[str, Any]] = []
    offset = row["timeline"]["source_offset_ms"]
    for (start, end), (basis, anchor) in candidates.items():
        overlap = _silence_overlap(start, end, routing["silences"])
        fraction = overlap * 1_000_000 // (end - start)
        if fraction > policy["max_silence_fraction_millionths"]:
            continue
        ranked.append(
            {
                "local_start_ms": start,
                "local_end_ms": end,
                "parent_start_ms": offset + start,
                "parent_end_ms": offset + end,
                "proposal_basis": basis,
                "anchor_local_ms": anchor,
                "anchor_parent_ms": offset + anchor,
                "estimated_silence_overlap_ms": overlap,
                "estimated_silence_fraction_millionths": fraction,
                "_rank": (
                    fraction,
                    priority[basis],
                    offset + start,
                    offset + end,
                    row["analysis_media_id"],
                ),
            }
        )
    ranked.sort(key=lambda item: item["_rank"])
    return ranked


def _route_parent_windows(
    request_id: str,
    parent: dict[str, Any],
    windows: list[tuple[dict[str, Any], dict[str, Any]]],
    policy: dict[str, int],
) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for window, preprocess in windows:
        row = _window_request_adapter(parent, window)
        for candidate in _rank_window_candidates(row, preprocess, policy):
            candidate["window"] = window
            ranked.append(candidate)
    ranked.sort(key=lambda item: item["_rank"])
    gap = policy["minimum_gap_ms"]
    selected: list[dict[str, Any]] = []
    for candidate in ranked:
        start = candidate["parent_start_ms"]
        end = candidate["parent_end_ms"]
        if any(
            not (
                end + gap <= old["parent_start_ms"]
                or start >= old["parent_end_ms"] + gap
            )
            for old in selected
        ):
            continue
        selected.append(candidate)
        if len(selected) >= policy["max_intervals_per_recording"]:
            break
    selected.sort(
        key=lambda item: (
            item["parent_start_ms"],
            item["parent_end_ms"],
            item["window"]["analysis_media_id"],
        )
    )
    output: list[dict[str, Any]] = []
    for item in selected:
        window = item["window"]
        output.append(
            {
                "interval_id": _stable_id(
                    "proposal_interval",
                    request_id,
                    parent["recording_id"],
                    window["analysis_rendition_id"],
                    item["parent_start_ms"],
                    item["parent_end_ms"],
                    item["proposal_basis"],
                ),
                "status": "proposal_unreviewed",
                "boundary": "half_open",
                "window_id": window["window_id"],
                "window_ordinal": window["window_ordinal"],
                "analysis_media_id": window["analysis_media_id"],
                "analysis_rendition_id": window["analysis_rendition_id"],
                "local_start_ms": item["local_start_ms"],
                "local_end_ms": item["local_end_ms"],
                "start_ms": item["parent_start_ms"],
                "end_ms": item["parent_end_ms"],
                "parent_start_ms": item["parent_start_ms"],
                "parent_end_ms": item["parent_end_ms"],
                "duration_ms": item["parent_end_ms"] - item["parent_start_ms"],
                "proposal_basis": item["proposal_basis"],
                "anchor_local_ms": item["anchor_local_ms"],
                "anchor_parent_ms": item["anchor_parent_ms"],
                "estimated_silence_overlap_ms": item[
                    "estimated_silence_overlap_ms"
                ],
                "estimated_silence_fraction_millionths": item[
                    "estimated_silence_fraction_millionths"
                ],
                "reviewer_completion": {
                    "include": None,
                    "split": None,
                    "stratum_id": None,
                    "flags": {
                        "language_tags": None,
                        "code_switch": None,
                        "speaker_overlap": None,
                        "playback_speech": None,
                        "noise": None,
                    },
                },
            }
        )
    return output


def _proposal_safety() -> dict[str, Any]:
    return {
        "proposal_only": True,
        "asr_output_inspected": False,
        "transcript_text_inspected": False,
        "direct_media_content_inspected": False,
        "output_path_used": False,
        "catalog_opened_query_only": True,
        "catalog_mutated": False,
        "freeze_created": False,
        "reference_text_present": False,
        "reference_quality_claimed": False,
        "human_review_required": True,
    }


def _apply_global_caps(
    prepared: list[dict[str, Any]], policy: dict[str, int]
) -> tuple[int, int]:
    """Apply count/duration caps one recording per round, mutating only queues."""

    queues = [list(row["intervals"]) for row in prepared]
    for row in prepared:
        row["intervals"] = []
    accepted_count = 0
    accepted_duration = 0
    progress = True
    while progress and accepted_count < policy["max_total_intervals"]:
        progress = False
        for row, queue in zip(prepared, queues):
            if not queue or accepted_count >= policy["max_total_intervals"]:
                continue
            candidate = queue.pop(0)
            if accepted_duration + candidate["duration_ms"] > policy["max_total_duration_ms"]:
                continue
            row["intervals"].append(candidate)
            accepted_count += 1
            accepted_duration += candidate["duration_ms"]
            progress = True
    return accepted_count, accepted_duration


def _apply_global_caps_v2(
    prepared: list[dict[str, Any]], policy: dict[str, int]
) -> tuple[int, int]:
    """Apply v2 caps fairly without letting an oversized head hide a later fit.

    The schema-v1 helper above is deliberately unchanged for regeneration
    compatibility. Grouped-window v2 queues may contain differently sized tail
    intervals, so each recording's turn scans past candidates that cannot fit the
    remaining global duration budget.
    """

    queues = [list(row["intervals"]) for row in prepared]
    for row in prepared:
        row["intervals"] = []
    accepted_count = 0
    accepted_duration = 0
    progress = True
    while progress and accepted_count < policy["max_total_intervals"]:
        progress = False
        for row, queue in zip(prepared, queues):
            if accepted_count >= policy["max_total_intervals"]:
                break
            while queue:
                candidate = queue.pop(0)
                if (
                    accepted_duration + candidate["duration_ms"]
                    > policy["max_total_duration_ms"]
                ):
                    continue
                row["intervals"].append(candidate)
                accepted_count += 1
                accepted_duration += candidate["duration_ms"]
                progress = True
                break
    return accepted_count, accepted_duration


def _preprocess_binding_value(
    preprocess: dict[str, Any],
    result_raw_sha256: str,
    import_envelope_sha256: str,
    import_batch_id: str,
    catalog_rows_sha256: str,
) -> dict[str, Any]:
    artifacts = preprocess["artifacts"]
    return {
        "result_raw_sha256": result_raw_sha256,
        "import_envelope_sha256": import_envelope_sha256,
        "import_batch_id": import_batch_id,
        "processing_run_id": preprocess["processing_run_id"],
        "implementation_version": preprocess["implementation_version"],
        "recipe_sha256": preprocess["recipe_sha256"],
        "routing_artifact_sha256": artifacts["scene_silence_routing_json"]["sha256"],
        "routing_metadata_sha256": preprocess["routing_metadata_sha256"],
        "audio_artifact_sha256": artifacts.get("audio_16khz_mono_flac", {}).get(
            "sha256"
        ),
        "proxy_artifact_sha256": artifacts.get(
            "low_resolution_cfr_proxy", {}
        ).get("sha256"),
        "catalog_rows_sha256": catalog_rows_sha256,
    }


def _routing_summary_value(preprocess: dict[str, Any]) -> dict[str, Any]:
    routing = preprocess["routing"]
    return {
        "analysis_coverage_duration_ms": routing["duration_ms"],
        "has_audio": routing["has_audio"],
        "has_video": routing["has_video"],
        "scene_change_count": routing["scene_change_count"],
        "silence_interval_count": routing["silence_interval_count"],
        "content_interpretation": "none_machine_routing_only",
    }


def _prepare_interval_proposal_v2(
    request: dict[str, Any],
    cohort: dict[str, Any],
    catalog_path: str | Path,
) -> dict[str, Any]:
    policy = _policy(request["policy"])
    prepared: list[dict[str, Any]] = []
    relevant_hashes: list[dict[str, Any]] = []
    total_routing_rows = 0
    total_route_candidates = 0
    with _Catalog(Path(catalog_path)) as catalog:
        for parent_index, parent in enumerate(request["recordings"]):
            parent_path = f"$.recordings[{parent_index}]"
            prepared_windows: list[dict[str, Any]] = []
            routing_inputs: list[tuple[dict[str, Any], dict[str, Any]]] = []
            parent_hashes: list[dict[str, str]] = []
            for window_index, window in enumerate(parent["windows"]):
                window_path = f"{parent_path}.windows[{window_index}]"
                request_row = _window_request_adapter(parent, window)
                local_value, local_sha, _ = _read_pinned_json(
                    window["local_window_result_path"],
                    window["local_window_result_sha256"],
                    f"{window_path}.local_window_result",
                )
                local = _validate_local_window_result(
                    local_value, request_row, f"{window_path}.local_window_result"
                )
                preprocess_value, preprocess_sha, _ = _read_pinned_json(
                    window["preprocess_result_path"],
                    window["preprocess_result_raw_sha256"],
                    f"{window_path}.preprocess_result",
                )
                observed_import_envelope_sha256 = (
                    _preprocess_import_envelope_sha256(preprocess_value)
                )
                if (
                    observed_import_envelope_sha256
                    != window["preprocess_import_envelope_sha256"]
                ):
                    _fail(
                        f"{window_path}.preprocess_import_envelope_sha256",
                        "does not match canonical JSON of the sealed result envelope",
                    )
                preprocess = _validate_preprocess_result(
                    preprocess_value, request_row, f"{window_path}.preprocess_result"
                )
                total_routing_rows += (
                    preprocess["routing"]["scene_change_count"]
                    + preprocess["routing"]["silence_interval_count"]
                )
                if total_routing_rows > MAX_ROUTING_ROWS:
                    _fail(
                        "$.recordings",
                        f"aggregate routing rows exceed hard cap {MAX_ROUTING_ROWS}",
                    )
                source_span = window["source_end_ms"] - window["source_offset_ms"]
                effective_duration = min(
                    preprocess["routing"]["duration_ms"],
                    window["analysis_media_duration_ms"],
                    source_span,
                )
                periodic_candidates = (
                    effective_duration + policy["periodic_stride_ms"] - 1
                ) // policy["periodic_stride_ms"]
                total_route_candidates += (
                    preprocess["routing"]["scene_change_count"]
                    + preprocess["routing"]["silence_interval_count"]
                    + periodic_candidates
                )
                if total_route_candidates > MAX_ROUTING_ROWS:
                    _fail(
                        "$.recordings",
                        f"aggregate routing candidates exceed hard cap {MAX_ROUTING_ROWS}",
                    )
                preprocess_catalog = _catalog_binding(
                    catalog,
                    request_row,
                    preprocess,
                    window_path,
                    import_envelope_sha256=observed_import_envelope_sha256,
                    expected_import_batch_id=window["preprocess_import_batch_id"],
                )
                local_catalog = _catalog_local_window_binding(
                    catalog,
                    request_row,
                    local,
                    local_sha,
                    f"{window_path}.local_window_catalog_lineage",
                )
                parent_hashes.append(
                    {
                        "analysis_media_id": window["analysis_media_id"],
                        "preprocess_catalog_rows_sha256": preprocess_catalog[
                            "relevant_rows_sha256"
                        ],
                        "local_window_catalog_rows_sha256": local_catalog[
                            "relevant_rows_sha256"
                        ],
                    }
                )
                prepared_windows.append(
                    {
                        "window_id": window["window_id"],
                        "window_ordinal": window["window_ordinal"],
                        "analysis_rendition_id": window["analysis_rendition_id"],
                        "analysis_rendition_kind": window[
                            "analysis_rendition_kind"
                        ],
                        "analysis_media_id": window["analysis_media_id"],
                        "analysis_media_sha256": window["analysis_media_sha256"],
                        "analysis_media_byte_count": window[
                            "analysis_media_byte_count"
                        ],
                        "analysis_media_duration_ms": window[
                            "analysis_media_duration_ms"
                        ],
                        "preprocess_binding": _preprocess_binding_value(
                            preprocess,
                            preprocess_sha,
                            observed_import_envelope_sha256,
                            window["preprocess_import_batch_id"],
                            preprocess_catalog["relevant_rows_sha256"],
                        ),
                        "timeline": {
                            "binding_kind": "local_window",
                            "parent_coordinate_system": "parent_rendition_media_ms",
                            "local_coordinate_system": "analysis_media_ms",
                            "source_offset_ms": window["source_offset_ms"],
                            "source_end_ms": window["source_end_ms"],
                            "local_window_binding": {
                                "result_sha256": local_sha,
                                "bundle_id": local["bundle_id"],
                                "window_id": local["window_id"],
                                "work_order_sha256": local["work_order_sha256"],
                                "artifact_id": local["analysis_artifact"][
                                    "artifact_id"
                                ],
                                "artifact_kind": local["analysis_artifact"][
                                    "artifact_kind"
                                ],
                                "artifact_sha256": local["analysis_artifact"][
                                    "sha256"
                                ],
                                "admission_processing_run_id": local_catalog[
                                    "processing_run_id"
                                ],
                                "catalog_rows_sha256": local_catalog[
                                    "relevant_rows_sha256"
                                ],
                            },
                        },
                        "routing_summary": _routing_summary_value(preprocess),
                    }
                )
                routing_inputs.append((window, preprocess))
            intervals = _route_parent_windows(
                request["request_id"], parent, routing_inputs, policy
            )
            prepared.append(
                {
                    "candidate_id": parent["candidate_id"],
                    "recording_id": parent["recording_id"],
                    "source_id": parent["source_id"],
                    "source_native_id": parent["source_native_id"],
                    "source_locator": parent["source_locator"],
                    "parent_rendition_id": parent["parent_rendition_id"],
                    "parent_rendition_kind": parent["parent_rendition_kind"],
                    "parent_media_id": parent["parent_media_id"],
                    "parent_media_sha256": parent["parent_media_sha256"],
                    "parent_media_byte_count": parent["parent_media_byte_count"],
                    "parent_media_duration_ms": parent["parent_media_duration_ms"],
                    "windows": prepared_windows,
                    "intervals": intervals,
                }
            )
            relevant_hashes.append(
                {
                    "recording_id": parent["recording_id"],
                    "windows": parent_hashes,
                }
            )
        catalog_basis = {
            "open_mode": "sqlite_read_only_transaction",
            "quick_check": "ok",
            "schema_migrations_sha256": catalog.schema_migrations_sha256,
            "relevant_rows_sha256": hashlib.sha256(
                _canonical_bytes(relevant_hashes)
            ).hexdigest(),
        }
    accepted_count, accepted_duration = _apply_global_caps_v2(prepared, policy)
    proposal = {
        "schema_version": 2,
        "manifest_kind": "interval_proposal",
        "manifest_sha256": "0" * 64,
        "proposal_id": _stable_id(
            "interval_proposal",
            request["request_id"],
            request["manifest_sha256"],
            hashlib.sha256(_canonical_bytes(catalog_basis)).hexdigest(),
        ),
        "proposal_state": "proposal_unreviewed",
        "created_at": request["created_at"],
        "cohort_id": cohort["cohort_id"],
        "cohort_manifest_sha256": cohort["manifest_sha256"],
        "request_id": request["request_id"],
        "request_manifest_sha256": request["manifest_sha256"],
        "selection_basis": "scene_silence_and_periodic_routing_without_asr_or_transcript",
        "policy": policy,
        "catalog_basis": catalog_basis,
        "recordings": prepared,
        "accounting": {
            "recording_count": len(prepared),
            "window_count": sum(len(row["windows"]) for row in prepared),
            "recordings_with_intervals": sum(
                bool(row["intervals"]) for row in prepared
            ),
            "interval_count": accepted_count,
            "total_duration_ms": accepted_duration,
        },
        "safety": _proposal_safety(),
    }
    proposal["manifest_sha256"] = canonical_manifest_sha256(proposal)
    return proposal


def prepare_interval_proposal(
    request_value: object,
    candidate_cohort: object,
    catalog_path: str | Path,
) -> dict[str, Any]:
    """Generate a deterministic, ASR-blind and explicitly unreviewed proposal."""

    request = validate_interval_proposal_request(request_value, candidate_cohort)
    cohort = validate_candidate_cohort(candidate_cohort)
    if request["schema_version"] == 2:
        return _prepare_interval_proposal_v2(request, cohort, catalog_path)
    policy = _policy(request["policy"])
    prepared: list[dict[str, Any]] = []
    relevant_hashes: list[dict[str, str]] = []
    with _Catalog(Path(catalog_path)) as catalog:
        for index, row in enumerate(request["recordings"]):
            path = f"$.recordings[{index}]"
            preprocess_value, preprocess_sha, _ = _read_pinned_json(
                row["preprocess_result_path"],
                row["preprocess_result_sha256"],
                f"{path}.preprocess_result",
            )
            preprocess = _validate_preprocess_result(
                preprocess_value, row, f"{path}.preprocess_result"
            )
            local_binding: dict[str, Any] | None = None
            if row["timeline"]["binding_kind"] == "local_window":
                local_value, local_sha, _ = _read_pinned_json(
                    row["timeline"]["local_window_result_path"],
                    row["timeline"]["local_window_result_sha256"],
                    f"{path}.local_window_result",
                )
                local = _validate_local_window_result(
                    local_value, row, f"{path}.local_window_result"
                )
                local_binding = {
                    "result_sha256": local_sha,
                    "bundle_id": local["bundle_id"],
                }
            binding = _catalog_binding(catalog, row, preprocess, path)
            relevant_hashes.append(
                {
                    "recording_id": row["recording_id"],
                    "relevant_rows_sha256": binding["relevant_rows_sha256"],
                }
            )
            artifact = preprocess["artifacts"]
            intervals = _route_recording(request["request_id"], row, preprocess, policy)
            prepared.append(
                {
                    "candidate_id": row["candidate_id"],
                    "recording_id": row["recording_id"],
                    "source_id": row["source_id"],
                    "source_native_id": row["source_native_id"],
                    "rendition_id": row["rendition_id"],
                    "rendition_kind": row["rendition_kind"],
                    "parent_media_id": row["parent_media_id"],
                    "parent_media_sha256": row["parent_media_sha256"],
                    "parent_media_byte_count": row["parent_media_byte_count"],
                    "parent_media_duration_ms": row["parent_media_duration_ms"],
                    "analysis_media_id": row["analysis_media_id"],
                    "analysis_media_sha256": row["analysis_media_sha256"],
                    "analysis_media_byte_count": row["analysis_media_byte_count"],
                    "analysis_media_duration_ms": row["analysis_media_duration_ms"],
                    "preprocess_binding": {
                        "result_sha256": preprocess_sha,
                        "processing_run_id": preprocess["processing_run_id"],
                        "implementation_version": preprocess["implementation_version"],
                        "recipe_sha256": preprocess["recipe_sha256"],
                        "routing_artifact_sha256": artifact["scene_silence_routing_json"]["sha256"],
                        "routing_metadata_sha256": preprocess["routing_metadata_sha256"],
                        "audio_artifact_sha256": artifact.get("audio_16khz_mono_flac", {}).get("sha256"),
                        "proxy_artifact_sha256": artifact.get("low_resolution_cfr_proxy", {}).get("sha256"),
                        "catalog_rows_sha256": binding["relevant_rows_sha256"],
                    },
                    "timeline": {
                        "binding_kind": row["timeline"]["binding_kind"],
                        "coordinate_system": "parent_rendition_media_ms",
                        "source_offset_ms": row["timeline"]["source_offset_ms"],
                        "source_end_ms": row["timeline"]["source_end_ms"],
                        "local_window_binding": local_binding,
                    },
                    "routing_summary": {
                        "analysis_coverage_duration_ms": preprocess["routing"]["duration_ms"],
                        "has_audio": preprocess["routing"]["has_audio"],
                        "has_video": preprocess["routing"]["has_video"],
                        "scene_change_count": preprocess["routing"]["scene_change_count"],
                        "silence_interval_count": preprocess["routing"]["silence_interval_count"],
                        "content_interpretation": "none_machine_routing_only",
                    },
                    "intervals": intervals,
                }
            )
        catalog_basis = {
            "open_mode": "sqlite_read_only_transaction",
            "quick_check": "ok",
            "schema_migrations_sha256": catalog.schema_migrations_sha256,
            "relevant_rows_sha256": hashlib.sha256(_canonical_bytes(relevant_hashes)).hexdigest(),
        }

    # Apply global caps fairly: one interval per recording per pass.  This keeps a
    # low global cap from silently assigning the entire proposal to the first file.
    accepted_count, accepted_duration = _apply_global_caps(prepared, policy)
    proposal = {
        "schema_version": 1,
        "manifest_kind": "interval_proposal",
        "manifest_sha256": "0" * 64,
        "proposal_id": _stable_id(
            "interval_proposal",
            request["request_id"],
            request["manifest_sha256"],
            hashlib.sha256(_canonical_bytes(catalog_basis)).hexdigest(),
        ),
        "proposal_state": "proposal_unreviewed",
        "created_at": request["created_at"],
        "cohort_id": cohort["cohort_id"],
        "cohort_manifest_sha256": cohort["manifest_sha256"],
        "request_id": request["request_id"],
        "request_manifest_sha256": request["manifest_sha256"],
        "selection_basis": "scene_silence_and_periodic_routing_without_asr_or_transcript",
        "policy": policy,
        "catalog_basis": catalog_basis,
        "recordings": prepared,
        "accounting": {
            "recording_count": len(prepared),
            "recordings_with_intervals": sum(bool(row["intervals"]) for row in prepared),
            "interval_count": accepted_count,
            "total_duration_ms": accepted_duration,
        },
        "safety": _proposal_safety(),
    }
    proposal["manifest_sha256"] = canonical_manifest_sha256(proposal)
    return proposal


def validate_interval_proposal(
    value: object,
    request_value: object,
    candidate_cohort: object,
    catalog_path: str | Path,
) -> dict[str, Any]:
    """Regenerate and byte-compare a proposal against all sealed inputs."""

    if not isinstance(value, dict):
        _fail("$", "must be an object")
    expected = prepare_interval_proposal(request_value, candidate_cohort, catalog_path)
    if value != expected:
        _fail("$", "proposal differs from deterministic regeneration")
    _verify_manifest_digest(value)
    return value


def _request_safety_value() -> dict[str, Any]:
    return {
        "selection_basis": "sealed_media_metadata_scene_silence_routing_only",
        "asr_output_inputs_allowed": False,
        "transcript_inputs_allowed": False,
        "direct_media_content_inspection_allowed": False,
        "output_path_allowed": False,
        "catalog_write_allowed": False,
        "freeze_authority": "none",
        "reference_quality_authority": "none",
    }


def _read_cli_sealed_json(path: Path, label: str) -> tuple[dict[str, Any], str, Path]:
    supplied = Path(path)
    _reject_forbidden_input_path(supplied, label)
    try:
        supplied_stat = supplied.lstat()
        if stat.S_ISLNK(supplied_stat.st_mode):
            _fail(label, "must not be a symbolic link")
        resolved = supplied.resolve(strict=True)
        before = resolved.lstat()
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or before.st_size > MAX_JSON_BYTES
            or before.st_mode & 0o222
        ):
            _fail(
                label,
                "must be a bounded sealed read-only non-symlink regular file",
            )
        body = resolved.read_bytes()
        after = resolved.lstat()
    except OSError as error:
        _fail(label, f"cannot read: {error}")
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
        before.st_mode,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
        after.st_mode,
    ) or (
        stat.S_ISLNK(after.st_mode)
        or not stat.S_ISREG(after.st_mode)
        or after.st_mode & 0o222
    ):
        _fail(label, "changed while being read")
    digest = hashlib.sha256(body).hexdigest()
    return _read_pinned_json(str(resolved), digest, label)


def emit_local_window_proposal_request(
    candidate_cohort: object,
    catalog_path: str | Path,
    local_window_result_paths: list[Path],
    preprocess_result_paths: list[Path],
    created_at: str,
    policy: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Build a v2 request grouped by admitted parent recording, read-only."""

    cohort = validate_candidate_cohort(candidate_cohort)
    _timestamp(created_at, "created_at")
    parsed_policy = _policy(dict(DEFAULT_POLICY if policy is None else policy), "policy")
    if len(local_window_result_paths) != len(preprocess_result_paths):
        _fail(
            "local_window_result_paths",
            "must have one matching preprocess result for every local-window result",
        )
    if not local_window_result_paths or len(local_window_result_paths) > MAX_TOTAL_WINDOWS:
        _fail(
            "local_window_result_paths",
            f"must contain 1..{MAX_TOTAL_WINDOWS} paths",
        )
    candidates = {row["candidate_id"]: row for row in cohort["candidates"]}
    parents_by_candidate: dict[str, dict[str, Any]] = {}
    with _Catalog(Path(catalog_path)) as catalog:
        for index, (local_path, preprocess_path) in enumerate(
            zip(local_window_result_paths, preprocess_result_paths)
        ):
            item_path = f"local_window_pairs[{index}]"
            local_value, local_sha, local_resolved = _read_cli_sealed_json(
                Path(local_path), f"{item_path}.local_window_result"
            )
            preprocess_value, preprocess_sha, preprocess_resolved = _read_cli_sealed_json(
                Path(preprocess_path), f"{item_path}.preprocess_result"
            )
            preprocess_import_sha = _preprocess_import_envelope_sha256(
                preprocess_value
            )
            preprocess_import_batch_id = _preprocess_import_batch_id(
                preprocess_import_sha
            )
            _reject_forbidden_data_keys(
                preprocess_value, f"{item_path}.preprocess_result"
            )
            local_top = _object(
                local_value,
                f"{item_path}.local_window_result",
                {
                    "schema_version",
                    "implementation_version",
                    "status",
                    "dry_run",
                    "job_id",
                    "bundle_id",
                    "work_order_sha256",
                    "source",
                    "window",
                    "tools",
                    "profile",
                    "limits",
                    "commands",
                    "artifacts",
                    "time_mapping",
                    "safety",
                    "result_path",
                },
            )
            _reject_forbidden_data_keys(local_top, f"{item_path}.local_window_result")
            _constant(
                local_top["schema_version"], 1, f"{item_path}.local_window_result.schema_version"
            )
            _constant(
                local_top["status"], "completed", f"{item_path}.local_window_result.status"
            )
            source = _object(
                local_top["source"],
                f"{item_path}.local_window_result.source",
                {
                    "path",
                    "expected_sha256",
                    "byte_count",
                    "media_id",
                    "duration_ms",
                    "acquisition_result_path",
                    "acquisition_result_sha256",
                    "stat_before",
                    "stat_after",
                    "unchanged",
                },
            )
            parent_media_id = _media_id(
                source["media_id"], f"{item_path}.local_window_result.source.media_id"
            )
            parent_sha = _sha256(
                source["expected_sha256"],
                f"{item_path}.local_window_result.source.expected_sha256",
            )
            if parent_media_id != f"media_sha256_{parent_sha}":
                _fail(
                    f"{item_path}.local_window_result.source.media_id",
                    "does not match expected_sha256",
                )
            parent_bytes = _bounded_integer(
                source["byte_count"],
                f"{item_path}.local_window_result.source.byte_count",
                minimum=1,
                maximum=1 << 63,
            )
            parent_duration = _bounded_integer(
                source["duration_ms"],
                f"{item_path}.local_window_result.source.duration_ms",
                minimum=1,
                maximum=MAX_MEDIA_DURATION_MS,
            )
            local_window = _object(
                local_top["window"],
                f"{item_path}.local_window_result.window",
                {
                    "window_id",
                    "ordinal",
                    "start_ms",
                    "end_ms",
                    "boundary",
                    "is_partial_tail",
                },
            )
            window_id = _string(
                local_window["window_id"],
                f"{item_path}.local_window_result.window.window_id",
            )
            window_ordinal = _bounded_integer(
                local_window["ordinal"],
                f"{item_path}.local_window_result.window.ordinal",
                minimum=1,
                maximum=MAX_WINDOWS_PER_RECORDING,
            )
            start = _bounded_integer(
                local_window["start_ms"],
                f"{item_path}.local_window_result.window.start_ms",
                maximum=MAX_MEDIA_DURATION_MS,
            )
            end = _bounded_integer(
                local_window["end_ms"],
                f"{item_path}.local_window_result.window.end_ms",
                minimum=1,
                maximum=MAX_MEDIA_DURATION_MS,
            )
            if (
                window_id != f"window_{window_ordinal:06d}"
                or end <= start
                or end > parent_duration
                or local_window["boundary"] != "half_open"
            ):
                _fail(
                    f"{item_path}.local_window_result.window",
                    "is not an exact bounded half-open producer window",
                )
            if (
                not isinstance(preprocess_value.get("processing_run"), dict)
                or preprocess_value["processing_run"].get("stage") != "media_preprocess"
            ):
                _fail(
                    f"{item_path}.preprocess_result",
                    "must be media_preprocess routing metadata, never ASR/transcript",
                )
            preprocess_input = preprocess_value.get("input")
            if not isinstance(preprocess_input, dict):
                _fail(f"{item_path}.preprocess_result.input", "must be an object")
            analysis_media_id = _media_id(
                preprocess_input.get("media_id"),
                f"{item_path}.preprocess_result.input.media_id",
            )
            analysis_sha = _sha256(
                preprocess_input.get("sha256"),
                f"{item_path}.preprocess_result.input.sha256",
            )
            analysis_bytes = _bounded_integer(
                preprocess_input.get("byte_count"),
                f"{item_path}.preprocess_result.input.byte_count",
                minimum=1,
                maximum=1 << 63,
            )
            if analysis_media_id != f"media_sha256_{analysis_sha}":
                _fail(
                    f"{item_path}.preprocess_result.input.media_id",
                    "does not match sha256",
                )
            raw_artifacts = _array(
                local_top["artifacts"], f"{item_path}.local_window_result.artifacts"
            )
            matching_artifacts: list[dict[str, Any]] = []
            for artifact_index, raw_artifact in enumerate(raw_artifacts):
                artifact_path = (
                    f"{item_path}.local_window_result.artifacts[{artifact_index}]"
                )
                artifact = _object(
                    raw_artifact,
                    artifact_path,
                    {
                        "artifact_id",
                        "artifact_kind",
                        "path",
                        "sha256",
                        "byte_count",
                        "visibility",
                        "normalized_probe",
                    },
                )
                if (
                    artifact["sha256"] == analysis_sha
                    and artifact["byte_count"] == analysis_bytes
                    and f"media_sha256_{artifact['sha256']}" == analysis_media_id
                ):
                    matching_artifacts.append(artifact)
            if len(matching_artifacts) != 1:
                _fail(
                    f"{item_path}.preprocess_result.input",
                    "must match exactly one sealed local-window artifact",
                )
            selected_artifact = matching_artifacts[0]
            artifact_kind = _choice(
                selected_artifact["artifact_kind"],
                {"window_audio_16khz_mono_flac", "window_low_resolution_cfr_proxy"},
                f"{item_path}.local_window_result.artifacts",
            )
            analysis = catalog.one(
                "SELECT media_id,sha256,byte_count,duration_ms,integrity_state "
                "FROM media_objects WHERE media_id=?",
                (analysis_media_id,),
                f"{item_path}.analysis_media_id",
            )
            if (
                analysis["sha256"] != analysis_sha
                or analysis["byte_count"] != analysis_bytes
                or analysis["integrity_state"] != "verified"
            ):
                _fail(
                    f"{item_path}.analysis_media_id",
                    "does not match the verified catalogue media row",
                )
            analysis_duration = _bounded_integer(
                analysis["duration_ms"],
                f"{item_path}.analysis_media_duration_ms",
                minimum=1,
                maximum=MAX_MEDIA_DURATION_MS,
            )
            matches = catalog.many(
                "SELECT DISTINCT s.source_id,s.native_id,rs.recording_id,r.rendition_id,"
                "r.rendition_kind,m.media_id,m.sha256,m.byte_count,m.duration_ms "
                "FROM renditions r JOIN media_objects m ON m.media_id=r.media_id "
                "JOIN recording_sources rs ON rs.recording_id=r.recording_id "
                "JOIN sources s ON s.source_id=rs.source_id "
                "WHERE r.media_id=? AND r.rendition_kind='acquired_source_media' "
                "ORDER BY s.source_id,rs.recording_id,r.rendition_id",
                (parent_media_id,),
            )
            eligible: list[tuple[dict[str, Any], dict[str, Any]]] = []
            for match in matches:
                candidate_id = f"candidate_youtube_{match['native_id']}"
                candidate = candidates.get(candidate_id)
                if candidate and (
                    candidate["source_id"] == match["source_id"]
                    and candidate["recording_id"] == match["recording_id"]
                ):
                    eligible.append((candidate, match))
            if len(eligible) != 1:
                _fail(
                    f"{item_path}.local_window_result",
                    "must resolve to exactly one cohort acquired parent rendition; "
                    f"found {len(eligible)}",
                )
            candidate, parent_match = eligible[0]
            analysis_rendition_kind = (
                f"local_window:{artifact_kind}:{local_top['bundle_id']}:{window_id}"
            )
            analysis_rendition_id = _expected_rendition_id(
                candidate["recording_id"], analysis_media_id, analysis_rendition_kind
            )
            analysis_rendition = catalog.one(
                "SELECT rendition_id FROM renditions WHERE recording_id=? AND media_id=? "
                "AND rendition_kind=?",
                (
                    candidate["recording_id"],
                    analysis_media_id,
                    analysis_rendition_kind,
                ),
                f"{item_path}.analysis_rendition_id",
            )
            if analysis_rendition["rendition_id"] != analysis_rendition_id:
                _fail(
                    f"{item_path}.analysis_rendition_id",
                    "catalogue local-window rendition ID is not deterministic",
                )
            parent_keys = {
                "candidate_id": candidate["candidate_id"],
                "recording_id": candidate["recording_id"],
                "source_id": candidate["source_id"],
                "source_native_id": candidate["native_id"],
                "source_locator": candidate["public_locator"],
                "parent_rendition_id": parent_match["rendition_id"],
                "parent_rendition_kind": parent_match["rendition_kind"],
                "parent_media_id": parent_match["media_id"],
                "parent_media_sha256": parent_match["sha256"],
                "parent_media_byte_count": parent_match["byte_count"],
                "parent_media_duration_ms": parent_match["duration_ms"],
            }
            if (
                parent_match["sha256"] != parent_sha
                or parent_match["byte_count"] != parent_bytes
                or parent_match["duration_ms"] != parent_duration
            ):
                _fail(
                    f"{item_path}.local_window_result.source",
                    "does not exactly match its acquired catalogue parent",
                )
            window_request = {
                "window_id": window_id,
                "window_ordinal": window_ordinal,
                "analysis_rendition_id": analysis_rendition_id,
                "analysis_rendition_kind": analysis_rendition_kind,
                "analysis_media_id": analysis_media_id,
                "analysis_media_sha256": analysis_sha,
                "analysis_media_byte_count": analysis_bytes,
                "analysis_media_duration_ms": analysis_duration,
                "preprocess_result_path": str(preprocess_resolved),
                "preprocess_result_raw_sha256": preprocess_sha,
                "preprocess_import_envelope_sha256": preprocess_import_sha,
                "preprocess_import_batch_id": preprocess_import_batch_id,
                "local_window_result_path": str(local_resolved),
                "local_window_result_sha256": local_sha,
                "source_offset_ms": start,
                "source_end_ms": end,
            }
            parent_request = {**parent_keys, "windows": [window_request]}
            request_row = _window_request_adapter(parent_request, window_request)
            local = _validate_local_window_result(
                local_value, request_row, f"{item_path}.local_window_result"
            )
            preprocess = _validate_preprocess_result(
                preprocess_value, request_row, f"{item_path}.preprocess_result"
            )
            _catalog_binding(
                catalog,
                request_row,
                preprocess,
                item_path,
                import_envelope_sha256=preprocess_import_sha,
                expected_import_batch_id=preprocess_import_batch_id,
            )
            _catalog_local_window_binding(
                catalog,
                request_row,
                local,
                local_sha,
                f"{item_path}.local_window_catalog_lineage",
            )
            existing = parents_by_candidate.get(candidate["candidate_id"])
            if existing is None:
                parents_by_candidate[candidate["candidate_id"]] = parent_request
            else:
                for key, expected in parent_keys.items():
                    if existing[key] != expected:
                        _fail(item_path, f"grouped parent {key} differs across windows")
                existing["windows"].append(window_request)
    ordered: list[dict[str, Any]] = []
    for candidate in cohort["candidates"]:
        parent = parents_by_candidate.get(candidate["candidate_id"])
        if parent is None:
            continue
        parent["windows"].sort(
            key=lambda row: (
                row["source_offset_ms"],
                row["source_end_ms"],
                row["analysis_media_id"],
            )
        )
        ordered.append(parent)
    request = {
        "schema_version": 2,
        "manifest_kind": "interval_proposal_request",
        "manifest_sha256": "0" * 64,
        "request_id": "proposal_request_placeholder",
        "created_at": created_at,
        "cohort_id": cohort["cohort_id"],
        "cohort_manifest_sha256": cohort["manifest_sha256"],
        "policy": parsed_policy,
        "safety": _request_safety_value(),
        "recordings": ordered,
    }
    request["request_id"] = _expected_request_id(request)
    request["manifest_sha256"] = canonical_manifest_sha256(request)
    validate_interval_proposal_request(request, cohort)
    return request


def emit_full_rendition_request(
    candidate_cohort: object,
    catalog_path: str | Path,
    preprocess_result_paths: list[Path],
    created_at: str,
    policy: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Build a strict full-rendition request for already processed cohort media."""

    cohort = validate_candidate_cohort(candidate_cohort)
    _timestamp(created_at, "created_at")
    parsed_policy = _policy(dict(DEFAULT_POLICY if policy is None else policy), "policy")
    if not preprocess_result_paths or len(preprocess_result_paths) > MAX_RECORDINGS:
        _fail("preprocess_result_paths", f"must contain 1..{MAX_RECORDINGS} paths")
    candidates = {row["candidate_id"]: row for row in cohort["candidates"]}
    rows_by_candidate: dict[str, dict[str, Any]] = {}
    with _Catalog(Path(catalog_path)) as catalog:
        for index, path in enumerate(preprocess_result_paths):
            supplied = Path(path)
            _reject_forbidden_input_path(
                supplied, f"preprocess_result_paths[{index}]"
            )
            # First hash establishes the pin that the request will carry.  The
            # exact preprocess validator below still refuses non-preprocess stages.
            try:
                supplied_stat = supplied.lstat()
                if stat.S_ISLNK(supplied_stat.st_mode):
                    _fail(
                        f"preprocess_result_paths[{index}]",
                        "must not be a symbolic link",
                    )
                resolved = supplied.resolve(strict=True)
                before = resolved.lstat()
                if (
                    stat.S_ISLNK(before.st_mode)
                    or not stat.S_ISREG(before.st_mode)
                    or before.st_size > MAX_JSON_BYTES
                ):
                    _fail(
                        f"preprocess_result_paths[{index}]",
                        "must be a bounded non-symlink regular file",
                    )
                body = resolved.read_bytes()
                after = resolved.lstat()
            except OSError as error:
                _fail(f"preprocess_result_paths[{index}]", f"cannot read: {error}")
            if (
                before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns
            ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
                _fail(f"preprocess_result_paths[{index}]", "changed while being read")
            digest = hashlib.sha256(body).hexdigest()
            value, _, _ = _read_pinned_json(
                str(resolved), digest, f"preprocess_result_paths[{index}]"
            )
            if (
                not isinstance(value.get("processing_run"), dict)
                or value["processing_run"].get("stage") != "media_preprocess"
            ):
                _fail(
                    f"preprocess_result_paths[{index}]",
                    "must be a media_preprocess result, never ASR/transcript",
                )
            input_row = value.get("input")
            if not isinstance(input_row, dict):
                _fail(f"preprocess_result_paths[{index}].input", "must be an object")
            media_id = input_row.get("media_id")
            matches = catalog.many(
                "SELECT DISTINCT s.source_id,s.native_id,rs.recording_id,r.rendition_id,"
                "r.rendition_kind,m.media_id,m.sha256,m.byte_count,m.duration_ms "
                "FROM renditions r JOIN media_objects m ON m.media_id=r.media_id "
                "JOIN recording_sources rs ON rs.recording_id=r.recording_id "
                "JOIN sources s ON s.source_id=rs.source_id "
                "WHERE r.media_id=? AND r.rendition_kind='acquired_source_media' "
                "ORDER BY s.source_id,rs.recording_id,r.rendition_id",
                (media_id,),
            )
            eligible = []
            for match in matches:
                candidate_id = f"candidate_youtube_{match['native_id']}"
                candidate = candidates.get(candidate_id)
                if candidate and (
                    candidate["source_id"] == match["source_id"]
                    and candidate["recording_id"] == match["recording_id"]
                ):
                    eligible.append((candidate, match))
            if len(eligible) != 1:
                _fail(
                    f"preprocess_result_paths[{index}]",
                    f"must resolve to exactly one cohort acquired rendition, found {len(eligible)}",
                )
            candidate, match = eligible[0]
            if candidate["candidate_id"] in rows_by_candidate:
                _fail(f"preprocess_result_paths[{index}]", "duplicates a cohort candidate")
            request_row = {
                "candidate_id": candidate["candidate_id"],
                "recording_id": candidate["recording_id"],
                "source_id": candidate["source_id"],
                "source_native_id": candidate["native_id"],
                "source_locator": candidate["public_locator"],
                "rendition_id": match["rendition_id"],
                "rendition_kind": match["rendition_kind"],
                "parent_media_id": match["media_id"],
                "parent_media_sha256": match["sha256"],
                "parent_media_byte_count": match["byte_count"],
                "parent_media_duration_ms": match["duration_ms"],
                "analysis_media_id": match["media_id"],
                "analysis_media_sha256": match["sha256"],
                "analysis_media_byte_count": match["byte_count"],
                "analysis_media_duration_ms": match["duration_ms"],
                "preprocess_result_path": str(resolved),
                "preprocess_result_sha256": digest,
                "timeline": {
                    "binding_kind": "full_rendition",
                    "source_offset_ms": 0,
                    "source_end_ms": match["duration_ms"],
                    "local_window_result_path": None,
                    "local_window_result_sha256": None,
                },
            }
            preprocess = _validate_preprocess_result(
                value, request_row, f"preprocess_result_paths[{index}]"
            )
            _catalog_binding(
                catalog,
                request_row,
                preprocess,
                f"preprocess_result_paths[{index}]",
            )
            rows_by_candidate[candidate["candidate_id"]] = request_row
    ordered = [
        rows_by_candidate[row["candidate_id"]]
        for row in cohort["candidates"]
        if row["candidate_id"] in rows_by_candidate
    ]
    request = {
        "schema_version": 1,
        "manifest_kind": "interval_proposal_request",
        "manifest_sha256": "0" * 64,
        "request_id": "proposal_request_placeholder",
        "created_at": created_at,
        "cohort_id": cohort["cohort_id"],
        "cohort_manifest_sha256": cohort["manifest_sha256"],
        "policy": parsed_policy,
        "safety": _request_safety_value(),
        "recordings": ordered,
    }
    request["request_id"] = _expected_request_id(request)
    request["manifest_sha256"] = canonical_manifest_sha256(request)
    validate_interval_proposal_request(request, cohort)
    return request
