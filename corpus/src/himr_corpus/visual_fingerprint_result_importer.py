"""Strict admission of private sparse visual-fingerprint evidence.

The producer's JSON and local files are untrusted at this boundary.  Admission
independently reconstructs its recipe, IDs, commands, decoded timing, 32x32 gray
digests, and fixed-integer pHash.  A pHash is retained only as uncalibrated review
routing evidence; this module cannot create identity, duplicate, relationship, or
publication decisions.  Comparison envelopes are independently recomputed.  Only a
producer-emitted threshold candidate receives a private generic candidate and review
task; a below-threshold result remains a measurement and never becomes a rejection or
an ``unrelated`` assertion.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from fractions import Fraction
from pathlib import Path
from typing import Any

from . import __version__
from .asr_result_importer import (
    _absolute_observed_path,
    _array,
    _exact_keys,
    _identifier,
    _integer,
    _local_file_uri,
    _object,
    _producer_id,
    _sha256,
    _stable_read,
    _string,
    _timestamp,
    _timestamp_value,
    _verify_hash,
)
from .db import transaction
from .importers import canonical_json, sha256_bytes
from .result_importers import ResultImportError


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
STAGE = "visual_fingerprint_extract"
COMPARE_STAGE = "visual_fingerprint_compare"
COMPARE_METHOD = "minimum_pairwise_phash_hamming_v1"
COMPARE_MATCH_METHOD = "visual_phash_minimum_hamming_v1"
ALGORITHM = "fixed_q20_dct_phash_8x8_v1"
WIDTH = 32
HEIGHT = 32
GRAY_BYTES = WIDTH * HEIGHT
PHASH_BITS = 64
MAX_FRAMES = 4_096
MAX_RESULT_BYTES = 64 * 1024 * 1024
MAX_SELECTED_FRAMES = 256
MAX_PAIRWISE_COMPARISONS = 65_536
PHASH_RE = re.compile(r"^[0-9a-f]{16}$")
COMPARE_WARNING = (
    "Candidate-routing evidence only. The raw threshold is not calibrated and "
    "does not establish identity, duplicate/parent status, ownership, a relationship, "
    "or that below-threshold material is unrelated."
)
COMPARE_REVIEW_REASON = (
    "Raw visual pHash threshold met; direct-media review is required before any "
    "duplicate, parent, identity, ownership, or relationship conclusion."
)
QUALITY_FLAGS = {
    "low_visual_variance",
    "decoded_timestamp_differs_from_request",
    "requested_keyframe_timestamp_decoded_non_keyframe",
}

# This is the producer's committed signed-Q20 8x32 cosine matrix.  Duplicating it
# at the admission boundary is intentional: an imported result cannot redefine its
# own hash algorithm by pointing at executable Python.
DCT_Q20: tuple[tuple[int, ...], ...] = (
    (1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576),
    (1047313, 1037227, 1017151, 987281, 947901, 899394, 842224, 776944, 704181, 624636, 539076, 448324, 353255, 254783, 153858, 51451, -51451, -153858, -254783, -353255, -448324, -539076, -624636, -704181, -776944, -842224, -899394, -947901, -987281, -1017151, -1037227, -1047313),
    (1043527, 1003425, 924761, 810560, 665210, 494295, 304386, 102778, -102778, -304386, -494295, -665210, -810560, -924761, -1003425, -1043527, -1043527, -1003425, -924761, -810560, -665210, -494295, -304386, -102778, 102778, 304386, 494295, 665210, 810560, 924761, 1003425, 1043527),
    (1037227, 947901, 776944, 539076, 254783, -51451, -353255, -624636, -842224, -987281, -1047313, -1017151, -899394, -704181, -448324, -153858, 153858, 448324, 704181, 899394, 1017151, 1047313, 987281, 842224, 624636, 353255, 51451, -254783, -539076, -776944, -947901, -1037227),
    (1028428, 871859, 582558, 204567, -204567, -582558, -871859, -1028428, -1028428, -871859, -582558, -204567, 204567, 582558, 871859, 1028428, 1028428, 871859, 582558, 204567, -204567, -582558, -871859, -1028428, -1028428, -871859, -582558, -204567, 204567, 582558, 871859, 1028428),
    (1017151, 776944, 353255, -153858, -624636, -947901, -1047313, -899394, -539076, -51451, 448324, 842224, 1037227, 987281, 704181, 254783, -254783, -704181, -987281, -1037227, -842224, -448324, 51451, 539076, 899394, 1047313, 947901, 624636, 153858, -353255, -776944, -1017151),
    (1003425, 665210, 102778, -494295, -924761, -1043527, -810560, -304386, 304386, 810560, 1043527, 924761, 494295, -102778, -665210, -1003425, -1003425, -665210, -102778, 494295, 924761, 1043527, 810560, 304386, -304386, -810560, -1043527, -924761, -494295, 102778, 665210, 1003425),
    (987281, 539076, -153858, -776944, -1047313, -842224, -254783, 448324, 947901, 1017151, 624636, -51451, -704181, -1037227, -899394, -353255, 353255, 899394, 1037227, 704181, 51451, -624636, -1017151, -947901, -448324, 254783, 842224, 1047313, 776944, 153858, -539076, -987281),
)


def _canonical_bytes(value: object) -> bytes:
    return canonical_json(value).encode("utf-8")


def _validated_flags(
    value: object, label: str, *, require_sorted: bool
) -> list[str]:
    raw = _array(value, label)
    flags = [_string(item, f"{label}[]", maximum=128) for item in raw]
    if len(flags) != len(set(flags)) or not set(flags) <= QUALITY_FLAGS:
        raise ResultImportError(f"{label} is unsupported or duplicated")
    if require_sorted and flags != sorted(flags):
        raise ResultImportError(f"{label} must be sorted")
    return flags


def _perceptual_hash(gray: bytes) -> tuple[str, int]:
    if len(gray) != GRAY_BYTES:
        raise ResultImportError("visual fingerprint grayscale evidence is not 32x32")
    rows = [
        [sum(gray[y * WIDTH + x] * DCT_Q20[u][x] for x in range(WIDTH)) for u in range(8)]
        for y in range(HEIGHT)
    ]
    coefficients = [
        sum(rows[y][u] * DCT_Q20[v][y] for y in range(HEIGHT))
        for v in range(8)
        for u in range(8)
    ]
    ac = sorted(coefficients[1:])
    median = ac[len(ac) // 2]
    bits = [False, *[coefficient > median for coefficient in coefficients[1:]]]
    value = 0
    for bit in bits:
        value = (value << 1) | int(bit)
    return f"{value:016x}", value.bit_count()


def _quality_flags(
    gray: bytes, sample: dict[str, Any], timestamp: dict[str, Any], drift_us: int
) -> list[str]:
    flags: list[str] = []
    if len(set(gray)) < 4 or max(gray) - min(gray) < 8:
        flags.append("low_visual_variance")
    if drift_us:
        flags.append("decoded_timestamp_differs_from_request")
    if sample["timestamp_kind"] == "keyframe" and not timestamp["is_keyframe"]:
        flags.append("requested_keyframe_timestamp_decoded_non_keyframe")
    return flags


def _frame_command(engine: Path, input_path: Path, seek_ms: int) -> list[str]:
    seconds = f"{seek_ms // 1000}.{seek_ms % 1000:03d}"
    return [
        str(engine), "-hide_banner", "-nostdin", "-nostats", "-loglevel", "info",
        "-threads", "1", "-fflags", "+bitexact", "-copyts", "-ss", seconds,
        "-i", str(input_path), "-map", "0:v:0", "-frames:v", "1", "-an",
        "-sn", "-dn", "-map_metadata", "-1", "-map_chapters", "-1", "-vf",
        "scale=32:32:flags=bilinear,format=gray,showinfo", "-fps_mode", "passthrough",
        "-pix_fmt", "gray", "-c:v", "rawvideo", "-flags:v", "+bitexact",
        "-threads:v", "1", "-f", "rawvideo", "pipe:1",
    ]


def _context(value: object) -> dict[str, str] | None:
    if value is None:
        return None
    row = _object(value, "visual fingerprint catalog_context")
    _exact_keys(row, "visual fingerprint catalog_context", {"recording_id", "rendition_id"})
    return {
        "recording_id": _identifier(row["recording_id"], "visual fingerprint catalog_context.recording_id"),
        "rendition_id": _identifier(row["rendition_id"], "visual fingerprint catalog_context.rendition_id"),
    }


def _load_result(path: Path) -> tuple[dict[str, Any], bytes, Path]:
    if not path.is_absolute():
        path = path.resolve()
    observed = _absolute_observed_path(str(path), "visual fingerprint result")
    if observed.lstat().st_mode & 0o222:
        raise ResultImportError("visual fingerprint result must be sealed read-only")
    body = _stable_read(observed, "visual fingerprint result", maximum_bytes=MAX_RESULT_BYTES)
    try:
        value = _object(json.loads(body), "visual fingerprint result")
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResultImportError("visual fingerprint result must be UTF-8 JSON") from error
    return value, body, observed


def _stat(value: object, label: str) -> dict[str, int]:
    row = _object(value, label)
    _exact_keys(row, label, {"device", "inode", "mtime_ns"})
    return {
        "device": _integer(row["device"], f"{label}.device", minimum=0),
        "inode": _integer(row["inode"], f"{label}.inode", minimum=0),
        "mtime_ns": _integer(row["mtime_ns"], f"{label}.mtime_ns"),
    }


def _input(value: object) -> tuple[dict[str, Any], Path]:
    row = _object(value, "visual fingerprint input")
    _exact_keys(
        row,
        "visual fingerprint input",
        {
            "path", "storage_uri", "sha256", "byte_count", "stat", "unchanged",
            "media_id", "artifact_id", "parent_processing_run_id", "duration_ms",
            "timeline_origin_ms", "video_stream_selector", "sealed",
        },
    )
    path = _absolute_observed_path(row["path"], "visual fingerprint input.path")
    if path.lstat().st_mode & 0o222:
        raise ResultImportError("visual fingerprint input must be sealed read-only")
    if _local_file_uri(row["storage_uri"], "visual fingerprint input.storage_uri") != path:
        raise ResultImportError("visual fingerprint input path and URI disagree")
    digest = _sha256(row["sha256"], "visual fingerprint input.sha256")
    byte_count = _integer(row["byte_count"], "visual fingerprint input.byte_count", minimum=1)
    if _identifier(row["media_id"], "visual fingerprint input.media_id") != f"media_sha256_{digest}":
        raise ResultImportError("visual fingerprint media ID is not derived from input bytes")
    _identifier(row["artifact_id"], "visual fingerprint input.artifact_id")
    _identifier(row["parent_processing_run_id"], "visual fingerprint input.parent_processing_run_id")
    duration = _integer(row["duration_ms"], "visual fingerprint input.duration_ms", minimum=1, maximum=604_800_000)
    origin = _integer(row["timeline_origin_ms"], "visual fingerprint input.timeline_origin_ms", minimum=0, maximum=604_800_000)
    if row["video_stream_selector"] != "0:v:0" or row["unchanged"] is not True or row["sealed"] is not True:
        raise ResultImportError("visual fingerprint input selector/integrity state is invalid")
    observed_stat = path.stat()
    expected_stat = _stat(row["stat"], "visual fingerprint input.stat")
    current_stat = {
        "device": observed_stat.st_dev,
        "inode": observed_stat.st_ino,
        "mtime_ns": observed_stat.st_mtime_ns,
    }
    if current_stat != expected_stat:
        raise ResultImportError("visual fingerprint input stat differs from producer observation")
    _verify_hash(path, digest, byte_count, "visual fingerprint input")
    return {**row, "duration_ms": duration, "timeline_origin_ms": origin, "stat": expected_stat}, path


def _engine(value: object) -> tuple[dict[str, Any], Path]:
    row = _object(value, "visual fingerprint engine")
    _exact_keys(
        row,
        "visual fingerprint engine",
        {
            "name", "path", "sha256", "byte_count", "version_label", "version_output",
            "version_output_sha256", "capabilities_output", "capabilities_output_sha256",
        },
    )
    if row["name"] != "ffmpeg":
        raise ResultImportError("visual fingerprint engine.name must equal ffmpeg")
    path = _absolute_observed_path(row["path"], "visual fingerprint engine.path")
    digest = _sha256(row["sha256"], "visual fingerprint engine.sha256")
    byte_count = _integer(row["byte_count"], "visual fingerprint engine.byte_count", minimum=1)
    version = _string(row["version_output"], "visual fingerprint engine.version_output", maximum=1_000_000)
    version_digest = _sha256(row["version_output_sha256"], "visual fingerprint engine.version_output_sha256")
    if sha256_bytes(version.encode("utf-8")) != version_digest:
        raise ResultImportError("visual fingerprint FFmpeg version-output digest is inconsistent")
    version_label = _string(row["version_label"], "visual fingerprint engine.version_label", maximum=512)
    if not version.splitlines() or version.splitlines()[0].strip() != version_label:
        raise ResultImportError("visual fingerprint FFmpeg version label is inconsistent")
    capabilities = _string(row["capabilities_output"], "visual fingerprint engine.capabilities_output", maximum=4_000_000)
    capabilities_digest = _sha256(row["capabilities_output_sha256"], "visual fingerprint engine.capabilities_output_sha256")
    if sha256_bytes(capabilities.encode("utf-8")) != capabilities_digest:
        raise ResultImportError("visual fingerprint FFmpeg capability digest is inconsistent")
    try:
        capability_rows = json.loads(capabilities)
    except json.JSONDecodeError as error:
        raise ResultImportError("visual fingerprint FFmpeg capabilities are not JSON") from error
    if canonical_json(capability_rows) != capabilities or not isinstance(capability_rows, list) or len(capability_rows) != 3:
        raise ResultImportError("visual fingerprint FFmpeg capabilities are not canonical evidence")
    expected_names = ("scale", "showinfo", "rawvideo")
    for index, (capability, expected_name) in enumerate(zip(capability_rows, expected_names, strict=True)):
        item = _object(capability, f"visual fingerprint capability[{index}]")
        _exact_keys(item, f"visual fingerprint capability[{index}]", {"name", "command", "output"})
        if item["name"] != expected_name:
            raise ResultImportError("visual fingerprint FFmpeg capability ordering/name is inconsistent")
        command = _array(item["command"], f"visual fingerprint capability[{index}].command")
        expected_command = [str(path), "-hide_banner", "-h", f"filter={expected_name}"]
        if expected_name == "rawvideo":
            expected_command = [str(path), "-hide_banner", "-h", "encoder=rawvideo"]
        if command != expected_command or not _string(item["output"], f"visual fingerprint capability[{index}].output", maximum=2_000_000):
            raise ResultImportError("visual fingerprint FFmpeg capability command/output is inconsistent")
    _verify_hash(path, digest, byte_count, "visual fingerprint FFmpeg executable")
    return dict(row), path


def _implementation(value: object) -> tuple[dict[str, Any], Path]:
    row = _object(value, "visual fingerprint implementation")
    _exact_keys(row, "visual fingerprint implementation", {"path", "sha256", "byte_count", "python_version"})
    path = _absolute_observed_path(row["path"], "visual fingerprint implementation.path")
    digest = _sha256(row["sha256"], "visual fingerprint implementation.sha256")
    byte_count = _integer(row["byte_count"], "visual fingerprint implementation.byte_count", minimum=1)
    _string(row["python_version"], "visual fingerprint implementation.python_version", maximum=128)
    _verify_hash(path, digest, byte_count, "visual fingerprint implementation")
    return dict(row), path


def _sample(value: object, index: int, duration_ms: int) -> dict[str, Any]:
    label = f"visual fingerprint sample[{index}]"
    row = _object(value, label)
    _exact_keys(row, label, {"sample_id", "window_id", "start_ms", "end_ms", "requested_timestamp_ms", "timestamp_kind"})
    normalized = {
        "sample_id": _identifier(row["sample_id"], f"{label}.sample_id"),
        "window_id": _identifier(row["window_id"], f"{label}.window_id"),
        "start_ms": _integer(row["start_ms"], f"{label}.start_ms", minimum=0),
        "end_ms": _integer(row["end_ms"], f"{label}.end_ms", minimum=1),
        "requested_timestamp_ms": _integer(row["requested_timestamp_ms"], f"{label}.requested_timestamp_ms", minimum=0),
        "timestamp_kind": _string(row["timestamp_kind"], f"{label}.timestamp_kind", maximum=32),
    }
    if (
        normalized["timestamp_kind"] not in {"explicit", "keyframe"}
        or not normalized["start_ms"] <= normalized["requested_timestamp_ms"] < normalized["end_ms"]
        or normalized["end_ms"] > duration_ms
    ):
        raise ResultImportError("visual fingerprint sample has invalid half-open timing or kind")
    return normalized


def _configuration(value: object, duration_ms: int) -> dict[str, Any]:
    row = _object(value, "visual fingerprint configuration")
    _exact_keys(
        row,
        "visual fingerprint configuration",
        {"algorithm", "pixel_format", "width", "height", "scale_flags", "threads", "timeout_seconds_per_frame", "max_timestamp_drift_ms", "samples"},
    )
    width = _integer(row["width"], "visual fingerprint width", minimum=WIDTH, maximum=WIDTH)
    height = _integer(row["height"], "visual fingerprint height", minimum=HEIGHT, maximum=HEIGHT)
    threads = _integer(row["threads"], "visual fingerprint threads", minimum=1, maximum=1)
    if row["algorithm"] != ALGORITHM or row["pixel_format"] != "gray" or width != WIDTH or height != HEIGHT or row["scale_flags"] != "bilinear" or threads != 1:
        raise ResultImportError("visual fingerprint configuration is unsupported")
    timeout = _integer(row["timeout_seconds_per_frame"], "visual fingerprint timeout", minimum=1, maximum=600)
    maximum_drift = _integer(row["max_timestamp_drift_ms"], "visual fingerprint max drift", minimum=0, maximum=10_000)
    raw_samples = _array(row["samples"], "visual fingerprint samples")
    if not 1 <= len(raw_samples) <= MAX_FRAMES:
        raise ResultImportError("visual fingerprint samples must contain 1..4096 items")
    samples = [_sample(value, index, duration_ms) for index, value in enumerate(raw_samples)]
    if len({sample["sample_id"] for sample in samples}) != len(samples):
        raise ResultImportError("visual fingerprint sample IDs must be unique")
    return {**row, "timeout_seconds_per_frame": timeout, "max_timestamp_drift_ms": maximum_drift, "samples": samples}


def _hash_definition() -> dict[str, Any]:
    return {
        "grayscale_bytes": GRAY_BYTES,
        "dct_matrix": "committed_signed_q20_cosine_8x32",
        "coefficients": "top_left_8x8",
        "dc_bit": 0,
        "threshold": "strictly_greater_than_median_of_63_ac_coefficients",
        "bit_order": "row_major_most_significant_bit_first",
    }


def _recipe(
    value: object,
    *,
    input_row: dict[str, Any],
    engine: dict[str, Any],
    implementation: dict[str, Any],
    configuration: dict[str, Any],
) -> dict[str, Any]:
    row = _object(value, "visual fingerprint recipe")
    _exact_keys(
        row,
        "visual fingerprint recipe",
        {"schema_version", "stage", "implementation_version", "implementation_sha256", "input", "engine", "extraction", "hash_definition"},
    )
    expected = {
        "schema_version": SCHEMA_VERSION,
        "stage": STAGE,
        "implementation_version": IMPLEMENTATION_VERSION,
        "implementation_sha256": implementation["sha256"],
        "input": {
            "expected_sha256": input_row["sha256"],
            "expected_byte_count": input_row["byte_count"],
            "media_id": input_row["media_id"],
            "artifact_id": input_row["artifact_id"],
            "parent_processing_run_id": input_row["parent_processing_run_id"],
            "duration_ms": input_row["duration_ms"],
            "timeline_origin_ms": input_row["timeline_origin_ms"],
            "video_stream_selector": input_row["video_stream_selector"],
        },
        "engine": {
            "sha256": engine["sha256"],
            "version_output_sha256": engine["version_output_sha256"],
            "capabilities_output_sha256": engine["capabilities_output_sha256"],
        },
        "extraction": configuration,
        "hash_definition": _hash_definition(),
    }
    if canonical_json(row) != canonical_json(expected):
        raise ResultImportError("visual fingerprint recipe dependencies are inconsistent")
    return expected


def _decoded_timestamp(
    value: object,
    *,
    sample: dict[str, Any],
    timeline_origin_ms: int,
    maximum_drift_ms: int,
) -> tuple[dict[str, Any], int]:
    label = f"visual fingerprint frame {sample['sample_id']} decoded_timestamp"
    row = _object(value, label)
    _exact_keys(
        row,
        label,
        {"pts", "duration_pts", "time_base_numerator", "time_base_denominator", "absolute_timestamp_us", "relative_timestamp_us", "relative_timestamp_ms", "duration_us", "is_keyframe"},
    )
    pts = _integer(row["pts"], f"{label}.pts", minimum=0)
    duration_pts = _integer(row["duration_pts"], f"{label}.duration_pts", minimum=1)
    numerator = _integer(row["time_base_numerator"], f"{label}.time_base_numerator", minimum=1)
    denominator = _integer(row["time_base_denominator"], f"{label}.time_base_denominator", minimum=1)
    if not isinstance(row["is_keyframe"], bool):
        raise ResultImportError(f"{label}.is_keyframe must be boolean")
    _integer(row["absolute_timestamp_us"], f"{label}.absolute_timestamp_us", minimum=0)
    _integer(row["relative_timestamp_us"], f"{label}.relative_timestamp_us", minimum=0)
    _integer(row["relative_timestamp_ms"], f"{label}.relative_timestamp_ms", minimum=0)
    _integer(row["duration_us"], f"{label}.duration_us", minimum=1)
    absolute = Fraction(pts * numerator, denominator)
    relative = absolute - Fraction(timeline_origin_ms, 1_000)
    duration = Fraction(duration_pts * numerator, denominator)
    expected = {
        "pts": pts,
        "duration_pts": duration_pts,
        "time_base_numerator": numerator,
        "time_base_denominator": denominator,
        "absolute_timestamp_us": round(absolute * 1_000_000),
        "relative_timestamp_us": round(relative * 1_000_000),
        "relative_timestamp_ms": round(relative * 1_000),
        "duration_us": round(duration * 1_000_000),
        "is_keyframe": row["is_keyframe"],
    }
    drift = abs(relative * 1_000 - sample["requested_timestamp_ms"])
    if (
        row != expected
        or not Fraction(sample["start_ms"], 1) <= relative * 1_000 < Fraction(sample["end_ms"], 1)
        or drift > maximum_drift_ms
        or expected["relative_timestamp_us"] < 0
        or expected["relative_timestamp_ms"] < 0
    ):
        raise ResultImportError("visual fingerprint decoded timing evidence is invalid")
    return expected, round(drift * 1_000)


def _frame(
    value: object,
    *,
    ordinal: int,
    sample: dict[str, Any],
    result_dir: Path,
    result_key: str,
    timeline_origin_ms: int,
    maximum_drift_ms: int,
) -> dict[str, Any]:
    label = f"visual fingerprint frame[{ordinal}]"
    row = _object(value, label)
    _exact_keys(
        row,
        label,
        {
            "fingerprint_id", "ordinal", "sample_id", "window_id", "start_ms", "end_ms",
            "requested_timestamp_ms", "timestamp_kind", "decoded_timestamp",
            "timestamp_drift_us", "algorithm", "phash_bits", "phash_hex",
            "phash_popcount", "exact_gray_sha256", "quality_flags", "artifact",
        },
    )
    if _integer(row["ordinal"], f"{label}.ordinal", minimum=0) != ordinal:
        raise ResultImportError("visual fingerprint frame ordinal is inconsistent")
    _identifier(row["sample_id"], f"{label}.sample_id")
    _identifier(row["window_id"], f"{label}.window_id")
    _integer(row["start_ms"], f"{label}.start_ms", minimum=0)
    _integer(row["end_ms"], f"{label}.end_ms", minimum=1)
    _integer(row["requested_timestamp_ms"], f"{label}.requested_timestamp_ms", minimum=0)
    _string(row["timestamp_kind"], f"{label}.timestamp_kind", maximum=32)
    if row["timestamp_kind"] not in {"explicit", "keyframe"}:
        raise ResultImportError("visual fingerprint frame timestamp kind is invalid")
    if any(row[key] != sample[key] for key in sample):
        raise ResultImportError("visual fingerprint frame does not mirror its sample")
    if row["algorithm"] != ALGORITHM or row["phash_bits"] != PHASH_BITS:
        raise ResultImportError("visual fingerprint frame algorithm is unsupported")
    phash = row["phash_hex"]
    if not isinstance(phash, str) or not PHASH_RE.fullmatch(phash):
        raise ResultImportError("visual fingerprint frame pHash is invalid")
    popcount = _integer(row["phash_popcount"], f"{label}.phash_popcount", minimum=0, maximum=PHASH_BITS)
    if popcount != int(phash, 16).bit_count():
        raise ResultImportError("visual fingerprint frame pHash popcount is invalid")
    timestamp, expected_drift_us = _decoded_timestamp(
        row["decoded_timestamp"],
        sample=sample,
        timeline_origin_ms=timeline_origin_ms,
        maximum_drift_ms=maximum_drift_ms,
    )
    drift_us = _integer(row["timestamp_drift_us"], f"{label}.timestamp_drift_us", minimum=0)
    if drift_us != expected_drift_us:
        raise ResultImportError("visual fingerprint timestamp drift is inconsistent")
    artifact = _object(row["artifact"], f"{label}.artifact")
    _exact_keys(
        artifact,
        f"{label}.artifact",
        {"artifact_id", "artifact_kind", "path", "storage_uri", "sha256", "byte_count", "width", "height", "pixel_format", "visibility"},
    )
    path = _absolute_observed_path(artifact["path"], f"{label}.artifact.path")
    if path.lstat().st_mode & 0o222:
        raise ResultImportError("visual fingerprint grayscale artifact must be sealed read-only")
    if _local_file_uri(artifact["storage_uri"], f"{label}.artifact.storage_uri") != path:
        raise ResultImportError("visual fingerprint artifact path and URI disagree")
    expected_path = result_dir / "frames" / f"frame-{ordinal:04d}-{sample['sample_id']}.gray"
    if path != expected_path:
        raise ResultImportError("visual fingerprint artifact escapes its exact result frame path")
    if artifact["artifact_kind"] != "visual_fingerprint_gray32" or artifact["visibility"] != "private" or artifact["width"] != WIDTH or artifact["height"] != HEIGHT or artifact["pixel_format"] != "gray" or artifact["byte_count"] != GRAY_BYTES:
        raise ResultImportError("visual fingerprint artifact catalog attributes are invalid")
    digest = _sha256(artifact["sha256"], f"{label}.artifact.sha256")
    exact_digest = _sha256(row["exact_gray_sha256"], f"{label}.exact_gray_sha256")
    if digest != exact_digest:
        raise ResultImportError("visual fingerprint exact-gray/artifact digests disagree")
    gray = _stable_read(path, f"{label}.artifact", maximum_bytes=GRAY_BYTES)
    if len(gray) != GRAY_BYTES or sha256_bytes(gray) != digest:
        raise ResultImportError("visual fingerprint grayscale bytes differ from their envelope")
    observed_phash, observed_popcount = _perceptual_hash(gray)
    if (phash, popcount) != (observed_phash, observed_popcount):
        raise ResultImportError("visual fingerprint grayscale evidence does not reproduce its pHash")
    expected_artifact_id = _producer_id("artifact", result_key, ordinal, digest)
    if _identifier(artifact["artifact_id"], f"{label}.artifact.artifact_id") != expected_artifact_id:
        raise ResultImportError("visual fingerprint artifact ID is inconsistent")
    expected_fingerprint_id = _producer_id(
        "visual_fingerprint", result_key, sample["sample_id"], timestamp, digest, phash
    )
    if _identifier(row["fingerprint_id"], f"{label}.fingerprint_id") != expected_fingerprint_id:
        raise ResultImportError("visual fingerprint ID is inconsistent")
    flags = _validated_flags(
        row["quality_flags"], f"{label}.quality_flags", require_sorted=False
    )
    if flags != _quality_flags(gray, sample, timestamp, drift_us):
        raise ResultImportError("visual fingerprint frame quality flags are inconsistent")
    return {
        **row,
        "decoded_timestamp": timestamp,
        "timestamp_drift_us": drift_us,
        "quality_flags": flags,
        "artifact": dict(artifact),
        "_artifact_path": path,
    }


def _sealed_directory(path: Path, label: str) -> None:
    try:
        resolved = path.resolve(strict=True)
        mode = path.lstat().st_mode
    except (FileNotFoundError, OSError) as error:
        raise ResultImportError(f"{label} is unavailable: {error}") from error
    if resolved != path or not path.is_dir() or mode & 0o222:
        raise ResultImportError(f"{label} must be a sealed resolved directory")


def validate_visual_fingerprint_result_file(path: Path) -> dict[str, Any]:
    result, body, result_path = _load_result(path)
    _exact_keys(
        result,
        "visual fingerprint result",
        {
            "schema_version", "stage", "implementation_version", "status", "dry_run",
            "job_id", "work_order_sha256", "recipe", "recipe_id", "recipe_sha256",
            "result_key", "implementation", "input", "engine", "configuration",
            "processing_run", "commands", "frames", "quality_flags", "catalog_context",
            "result_path", "errors",
        },
    )
    result_schema = _integer(
        result["schema_version"], "visual fingerprint result.schema_version", minimum=1, maximum=1
    )
    if (
        result_schema != SCHEMA_VERSION
        or result["stage"] != STAGE
        or result["implementation_version"] != IMPLEMENTATION_VERSION
        or result["status"] != "completed"
        or result["dry_run"] is not False
        or result["errors"] != []
        or result["result_path"] != str(result_path)
    ):
        raise ResultImportError("only completed non-dry-run visual fingerprint v1 results are importable")
    expected_body = (json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    if body != expected_body:
        raise ResultImportError("visual fingerprint result JSON is not producer-canonical")
    job_id = _identifier(result["job_id"], "visual fingerprint job_id")
    work_order_sha = _sha256(result["work_order_sha256"], "visual fingerprint work_order_sha256")
    input_row, input_path = _input(result["input"])
    engine, engine_path = _engine(result["engine"])
    implementation, implementation_path = _implementation(result["implementation"])
    configuration = _configuration(result["configuration"], input_row["duration_ms"])
    recipe = _recipe(
        result["recipe"],
        input_row=input_row,
        engine=engine,
        implementation=implementation,
        configuration=configuration,
    )
    recipe_sha = sha256_bytes(_canonical_bytes(recipe))
    recipe_id = f"recipe_visual_fingerprint_{recipe_sha[:32]}"
    if _sha256(result["recipe_sha256"], "visual fingerprint recipe_sha256") != recipe_sha or result["recipe_id"] != recipe_id:
        raise ResultImportError("visual fingerprint recipe identity is inconsistent")
    expected_key = sha256_bytes(
        _canonical_bytes(
            {"job_id": job_id, "work_order_sha256": work_order_sha, "recipe_sha256": recipe_sha}
        )
    )
    result_key = _sha256(result["result_key"], "visual fingerprint result_key")
    if result_key != expected_key:
        raise ResultImportError("visual fingerprint result key is inconsistent")

    result_dir = result_path.parent
    results_dir = result_dir.parent
    recipe_dir = results_dir.parent
    recipes_dir = recipe_dir.parent
    input_dir = recipes_dir.parent
    prefix_dir = input_dir.parent
    sha_dir = prefix_dir.parent
    fingerprint_dir = sha_dir.parent
    vision_dir = fingerprint_dir.parent
    if (
        result_dir.name != result_key
        or results_dir.name != "results"
        or recipe_dir.name != recipe_sha
        or recipes_dir.name != "recipes"
        or input_dir.name != input_row["sha256"]
        or prefix_dir.name != input_row["sha256"][:2]
        or sha_dir.name != "sha256"
        or fingerprint_dir.name != "visual-fingerprints"
        or vision_dir.name != "vision"
    ):
        raise ResultImportError("visual fingerprint result leaves its exact immutable layout")
    _sealed_directory(result_dir, "visual fingerprint result directory")
    _sealed_directory(result_dir / "frames", "visual fingerprint frames directory")
    context = _context(result["catalog_context"])
    reconstructed_order = {
        "schema_version": 1,
        "job_id": job_id,
        "input": {
            "path": str(input_path),
            "expected_sha256": input_row["sha256"],
            "expected_byte_count": input_row["byte_count"],
            "media_id": input_row["media_id"],
            "artifact_id": input_row["artifact_id"],
            "parent_processing_run_id": input_row["parent_processing_run_id"],
            "duration_ms": input_row["duration_ms"],
            "timeline_origin_ms": input_row["timeline_origin_ms"],
            "video_stream_selector": "0:v:0",
            "sealed": True,
        },
        "engine": {
            "executable": str(engine_path),
            "expected_sha256": engine["sha256"],
            "expected_byte_count": engine["byte_count"],
            "expected_version_output_sha256": engine["version_output_sha256"],
            "expected_capabilities_output_sha256": engine["capabilities_output_sha256"],
            "version_label": engine["version_label"],
        },
        "extraction": configuration,
        "catalog_context": context,
        "output": {"root": str(vision_dir.parent)},
    }
    if sha256_bytes(_canonical_bytes(reconstructed_order)) != work_order_sha:
        raise ResultImportError("visual fingerprint work-order digest is inconsistent")

    raw_frames = _array(result["frames"], "visual fingerprint frames")
    if len(raw_frames) != len(configuration["samples"]):
        raise ResultImportError("visual fingerprint frame/sample counts disagree")
    frames = [
        _frame(
            value,
            ordinal=index,
            sample=configuration["samples"][index],
            result_dir=result_dir,
            result_key=result_key,
            timeline_origin_ms=input_row["timeline_origin_ms"],
            maximum_drift_ms=configuration["max_timestamp_drift_ms"],
        )
        for index, value in enumerate(raw_frames)
    ]
    commands = _array(result["commands"], "visual fingerprint commands")
    expected_commands = [
        _frame_command(engine_path, input_path, input_row["timeline_origin_ms"] + sample["requested_timestamp_ms"])
        for sample in configuration["samples"]
    ]
    if commands != expected_commands:
        raise ResultImportError("visual fingerprint commands disagree with the exact recipe")
    aggregate_flags = _validated_flags(
        result["quality_flags"],
        "visual fingerprint aggregate quality_flags",
        require_sorted=True,
    )
    if aggregate_flags != sorted({flag for frame in frames for flag in frame["quality_flags"]}):
        raise ResultImportError("visual fingerprint aggregate quality flags are inconsistent")
    run = _object(result["processing_run"], "visual fingerprint processing_run")
    _exact_keys(run, "visual fingerprint processing_run", {"processing_run_id", "started_at", "completed_at", "status"})
    run_id = _identifier(run["processing_run_id"], "visual fingerprint processing_run_id")
    if run_id != f"run_visual_fingerprint_{result_key[:32]}" or run["status"] != "completed":
        raise ResultImportError("visual fingerprint processing-run identity/status is inconsistent")
    started_at = _timestamp(run["started_at"], "visual fingerprint started_at")
    completed_at = _timestamp(run["completed_at"], "visual fingerprint completed_at")
    if started_at != run["started_at"] or completed_at != run["completed_at"] or _timestamp_value(completed_at) < _timestamp_value(started_at):
        raise ResultImportError("visual fingerprint run timestamps are noncanonical or inconsistent")

    normalized = dict(result)
    normalized.update(
        {
            "job_id": job_id,
            "work_order_sha256": work_order_sha,
            "recipe": recipe,
            "recipe_id": recipe_id,
            "recipe_sha256": recipe_sha,
            "result_key": result_key,
            "input": input_row,
            "engine": engine,
            "implementation": implementation,
            "configuration": configuration,
            "processing_run": {**run, "started_at": started_at, "completed_at": completed_at},
            "commands": commands,
            "frames": frames,
            "quality_flags": aggregate_flags,
            "catalog_context": context,
            "_result_path": result_path,
            "_result_sha256": sha256_bytes(body),
            "_result_byte_count": len(body),
            "_input_path": input_path,
            "_engine_path": engine_path,
            "_implementation_path": implementation_path,
        }
    )
    return normalized


def _reverify_files(result: dict[str, Any]) -> None:
    """Close the validation/transaction gap for all local provenance and evidence."""

    sealed = (
        (result["_result_path"], result["_result_sha256"], result["_result_byte_count"], "visual fingerprint result"),
        (result["_input_path"], result["input"]["sha256"], result["input"]["byte_count"], "visual fingerprint input"),
    )
    for path, digest, byte_count, label in sealed:
        if path.lstat().st_mode & 0o222:
            raise ResultImportError(f"{label} is no longer sealed read-only")
        _verify_hash(path, digest, byte_count, label)
    current_stat = result["_input_path"].stat()
    if result["input"]["stat"] != {
        "device": current_stat.st_dev,
        "inode": current_stat.st_ino,
        "mtime_ns": current_stat.st_mtime_ns,
    }:
        raise ResultImportError("visual fingerprint input stat changed before transaction")
    for path, digest, byte_count, label in (
        (result["_engine_path"], result["engine"]["sha256"], result["engine"]["byte_count"], "visual fingerprint FFmpeg executable"),
        (result["_implementation_path"], result["implementation"]["sha256"], result["implementation"]["byte_count"], "visual fingerprint implementation"),
    ):
        _verify_hash(path, digest, byte_count, label)
    for frame in result["frames"]:
        path = frame["_artifact_path"]
        if path.lstat().st_mode & 0o222:
            raise ResultImportError("visual fingerprint grayscale artifact is no longer sealed")
        body = _stable_read(path, "visual fingerprint grayscale artifact", maximum_bytes=GRAY_BYTES)
        if len(body) != GRAY_BYTES or sha256_bytes(body) != frame["artifact"]["sha256"]:
            raise ResultImportError("visual fingerprint grayscale artifact changed before transaction")
        if _perceptual_hash(body) != (frame["phash_hex"], frame["phash_popcount"]):
            raise ResultImportError("visual fingerprint pHash changed before transaction")


def _require_dependencies(connection, result: dict[str, Any]) -> None:
    input_row = result["input"]
    media = connection.execute(
        "SELECT sha256, byte_count, media_kind, duration_ms, integrity_state FROM media_objects WHERE media_id = ?",
        (input_row["media_id"],),
    ).fetchone()
    if (
        media is None
        or media["sha256"] != input_row["sha256"]
        or media["byte_count"] != input_row["byte_count"]
        or media["media_kind"] != "video"
        or media["duration_ms"] != input_row["duration_ms"]
        or media["integrity_state"] != "verified"
    ):
        raise ResultImportError("visual fingerprint input media dependency is missing or differs")
    location = connection.execute(
        "SELECT 1 FROM media_locations WHERE media_id = ? AND storage_uri = ?",
        (input_row["media_id"], input_row["storage_uri"]),
    ).fetchone()
    if location is None:
        raise ResultImportError("visual fingerprint input media location dependency is missing")
    artifact = connection.execute(
        "SELECT processing_run_id, storage_uri, sha256, byte_count, visibility FROM artifacts WHERE artifact_id = ?",
        (input_row["artifact_id"],),
    ).fetchone()
    if (
        artifact is None
        or artifact["processing_run_id"] != input_row["parent_processing_run_id"]
        or artifact["storage_uri"] != input_row["storage_uri"]
        or artifact["sha256"] != input_row["sha256"]
        or artifact["byte_count"] != input_row["byte_count"]
        or artifact["visibility"] != "private"
    ):
        raise ResultImportError("visual fingerprint input artifact dependency is missing or differs")
    parent_run = connection.execute(
        "SELECT status FROM processing_runs WHERE processing_run_id = ?",
        (input_row["parent_processing_run_id"],),
    ).fetchone()
    if parent_run is None or parent_run["status"] != "completed":
        raise ResultImportError("visual fingerprint parent processing run is missing or incomplete")
    context = result["catalog_context"]
    if context is None:
        return
    rendition = connection.execute(
        "SELECT recording_id, media_id FROM renditions WHERE rendition_id = ?",
        (context["rendition_id"],),
    ).fetchone()
    if rendition is None or rendition["recording_id"] != context["recording_id"] or rendition["media_id"] != input_row["media_id"]:
        raise ResultImportError("visual fingerprint context rendition/recording/media dependency differs")


def _insert_processing_run(connection, result: dict[str, Any]) -> None:
    run = result["processing_run"]
    parameters = canonical_json(
        {
            "recipe_id": result["recipe_id"],
            "recipe_sha256": result["recipe_sha256"],
            "result_key": result["result_key"],
            "extraction": result["configuration"],
            "hash_definition": result["recipe"]["hash_definition"],
            "calibration_state": "not_calibrated",
            "requires_human_review": True,
        }
    )
    environment = canonical_json(
        {"engine": result["engine"], "implementation": result["implementation"]}
    )
    values = {
        "stage": STAGE,
        "implementation_version": IMPLEMENTATION_VERSION,
        "model_id": None,
        "glossary_revision_id": None,
        "parameters_json": parameters,
        "environment_json": environment,
        "random_seed": None,
        "started_at": run["started_at"],
        "completed_at": run["completed_at"],
        "status": "completed",
        "error_text": None,
    }
    existing = connection.execute(
        f"SELECT {', '.join(values)} FROM processing_runs WHERE processing_run_id = ?",
        (run["processing_run_id"],),
    ).fetchone()
    if existing is not None:
        if any(existing[key] != value for key, value in values.items()):
            raise ResultImportError("visual fingerprint processing-run ID already has different data")
        return
    connection.execute(
        f"INSERT INTO processing_runs(processing_run_id, {', '.join(values)}) VALUES(?, {', '.join('?' for _ in values)})",
        (run["processing_run_id"], *values.values()),
    )


def _insert_run_input(
    connection,
    *,
    run_id: str,
    object_type: str,
    object_id: str,
    role: str,
    digest: str,
) -> None:
    run_input_id = _producer_id("run_input", run_id, object_type, object_id, role)
    values = (run_id, object_type, object_id, role, digest)
    existing = connection.execute(
        "SELECT processing_run_id, object_type, object_id, input_role, input_sha256 FROM run_inputs WHERE run_input_id = ?",
        (run_input_id,),
    ).fetchone()
    if existing is not None:
        if tuple(existing) != values:
            raise ResultImportError("visual fingerprint run-input ID already has different data")
        return
    collision = connection.execute(
        "SELECT run_input_id FROM run_inputs WHERE processing_run_id = ? AND object_type = ? AND object_id = ? AND input_role = ?",
        values[:4],
    ).fetchone()
    if collision is not None:
        raise ResultImportError("visual fingerprint logical run input already has a different ID")
    connection.execute(
        "INSERT INTO run_inputs(run_input_id, processing_run_id, object_type, object_id, input_role, input_sha256) VALUES(?, ?, ?, ?, ?, ?)",
        (run_input_id, *values),
    )


def _artifact_row(result: dict[str, Any], frame: dict[str, Any]) -> dict[str, Any]:
    artifact = frame["artifact"]
    metadata = canonical_json(
        {
            "algorithm": ALGORITHM,
            "calibrated_probability": None,
            "calibration_state": "not_calibrated",
            "duplicate_asserted": False,
            "fingerprint_id": frame["fingerprint_id"],
            "height": HEIGHT,
            "identity_asserted": False,
            "pixel_format": "gray",
            "quality_flags": frame["quality_flags"],
            "relationship_asserted": False,
            "requires_human_review": True,
            "sample_id": frame["sample_id"],
            "width": WIDTH,
        }
    )
    return {
        "artifact_id": artifact["artifact_id"],
        "processing_run_id": result["processing_run"]["processing_run_id"],
        "artifact_kind": "visual_fingerprint_gray32",
        "storage_uri": artifact["storage_uri"],
        "sha256": artifact["sha256"],
        "byte_count": GRAY_BYTES,
        "schema_version": 1,
        "visibility": "private",
        "metadata_json": metadata,
    }


def _insert_artifact(connection, row: dict[str, Any]) -> None:
    columns = (
        "processing_run_id", "artifact_kind", "storage_uri", "sha256", "byte_count",
        "schema_version", "visibility", "metadata_json",
    )
    existing = connection.execute(
        f"SELECT {', '.join(columns)} FROM artifacts WHERE artifact_id = ?",
        (row["artifact_id"],),
    ).fetchone()
    if existing is not None:
        if any(existing[key] != row[key] for key in columns):
            raise ResultImportError("visual fingerprint artifact ID already has different data")
        return
    collision = connection.execute(
        "SELECT artifact_id FROM artifacts WHERE storage_uri = ? AND sha256 = ?",
        (row["storage_uri"], row["sha256"]),
    ).fetchone()
    if collision is not None:
        raise ResultImportError("visual fingerprint artifact URI/digest already has a different ID")
    connection.execute(
        "INSERT INTO artifacts(artifact_id, processing_run_id, artifact_kind, storage_uri, sha256, byte_count, schema_version, visibility, metadata_json) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
        tuple(row[key] for key in ("artifact_id", *columns)),
    )


def _fingerprint_implementation(result: dict[str, Any], frame: dict[str, Any]) -> str:
    # Generic fingerprints have a semantic uniqueness constraint.  The producer's
    # result key and sample ID distinguish intentionally repeated sample windows.
    return (
        f"visual-fingerprint/{IMPLEMENTATION_VERSION}/{result['recipe_id']}/"
        f"{result['result_key']}/{frame['sample_id']}"
    )


def _insert_contextual_evidence(connection, result: dict[str, Any]) -> None:
    context = result["catalog_context"]
    if context is None:
        return
    run_id = result["processing_run"]["processing_run_id"]
    for frame in result["frames"]:
        artifact = frame["artifact"]
        implementation = _fingerprint_implementation(result, frame)
        fingerprint_values = {
            "media_id": result["input"]["media_id"],
            "fingerprint_kind": ALGORITHM,
            "implementation_version": implementation,
            "start_ms": frame["start_ms"],
            "end_ms": frame["end_ms"],
            "value_text": frame["phash_hex"],
            "artifact_uri": artifact["storage_uri"],
        }
        existing_fingerprint = connection.execute(
            f"SELECT {', '.join(fingerprint_values)} FROM fingerprints WHERE fingerprint_id = ?",
            (frame["fingerprint_id"],),
        ).fetchone()
        if existing_fingerprint is None:
            collision = connection.execute(
                """
                SELECT fingerprint_id FROM fingerprints
                WHERE media_id = ? AND fingerprint_kind = ?
                  AND implementation_version = ? AND start_ms = ? AND end_ms = ?
                """,
                (
                    fingerprint_values["media_id"],
                    fingerprint_values["fingerprint_kind"],
                    fingerprint_values["implementation_version"],
                    fingerprint_values["start_ms"],
                    fingerprint_values["end_ms"],
                ),
            ).fetchone()
            if collision is not None:
                raise ResultImportError("visual fingerprint semantic identity already uses a different ID")
            connection.execute(
                f"INSERT INTO fingerprints(fingerprint_id, {', '.join(fingerprint_values)}) VALUES(?, {', '.join('?' for _ in fingerprint_values)})",
                (frame["fingerprint_id"], *fingerprint_values.values()),
            )
        elif any(existing_fingerprint[key] != value for key, value in fingerprint_values.items()):
            raise ResultImportError("visual fingerprint ID already has different semantic data")

        observation_id = _producer_id(
            "observation_visual_fingerprint",
            run_id,
            frame["fingerprint_id"],
            context["recording_id"],
            context["rendition_id"],
        )
        metadata = canonical_json(
            {
                "algorithm": ALGORITHM,
                "artifact_id": artifact["artifact_id"],
                "calibrated_probability": None,
                "calibration_state": "not_calibrated",
                "coordinate_space": "rendition_media",
                "decoded_relative_timestamp_ms": frame["decoded_timestamp"]["relative_timestamp_ms"],
                "duplicate_asserted": False,
                "fingerprint_id": frame["fingerprint_id"],
                "identity_asserted": False,
                "quality_flags": frame["quality_flags"],
                "relationship_asserted": False,
                "requested_timestamp_ms": frame["requested_timestamp_ms"],
                "requires_human_review": True,
                "sample_id": frame["sample_id"],
                "window_id": frame["window_id"],
            }
        )
        observation_values = {
            "observation_kind": "visual_fingerprint",
            "recording_id": context["recording_id"],
            "rendition_id": context["rendition_id"],
            "processing_run_id": run_id,
            "start_ms": frame["start_ms"],
            "end_ms": frame["end_ms"],
            "visibility": "private",
            "review_state": "machine",
            "payload_schema_version": 1,
            "metadata_json": metadata,
            "created_at": result["processing_run"]["completed_at"],
        }
        existing_observation = connection.execute(
            f"SELECT {', '.join(observation_values)} FROM observations WHERE observation_id = ?",
            (observation_id,),
        ).fetchone()
        timestamp = frame["decoded_timestamp"]
        subtype_values = {
            "fingerprint_id": frame["fingerprint_id"],
            "artifact_id": artifact["artifact_id"],
            "ordinal": frame["ordinal"],
            "sample_id": frame["sample_id"],
            "window_id": frame["window_id"],
            "requested_timestamp_ms": frame["requested_timestamp_ms"],
            "timestamp_kind": frame["timestamp_kind"],
            "decoded_pts": timestamp["pts"],
            "decoded_duration_pts": timestamp["duration_pts"],
            "time_base_numerator": timestamp["time_base_numerator"],
            "time_base_denominator": timestamp["time_base_denominator"],
            "absolute_timestamp_us": timestamp["absolute_timestamp_us"],
            "relative_timestamp_us": timestamp["relative_timestamp_us"],
            "relative_timestamp_ms": timestamp["relative_timestamp_ms"],
            "decoded_duration_us": timestamp["duration_us"],
            "is_keyframe": int(timestamp["is_keyframe"]),
            "timestamp_drift_us": frame["timestamp_drift_us"],
            "algorithm": ALGORITHM,
            "phash_bits": PHASH_BITS,
            "phash_hex": frame["phash_hex"],
            "phash_popcount": frame["phash_popcount"],
            "exact_gray_sha256": frame["exact_gray_sha256"],
            "calibration_state": "not_calibrated",
            "calibrated_probability": None,
            "requires_human_review": 1,
            "identity_asserted": 0,
            "duplicate_asserted": 0,
            "relationship_asserted": 0,
            "quality_flags_json": canonical_json(frame["quality_flags"]),
        }
        if existing_observation is None:
            connection.execute(
                f"INSERT INTO observations(observation_id, {', '.join(observation_values)}) VALUES(?, {', '.join('?' for _ in observation_values)})",
                (observation_id, *observation_values.values()),
            )
            connection.execute(
                f"INSERT INTO visual_fingerprint_observations(observation_id, {', '.join(subtype_values)}) VALUES(?, {', '.join('?' for _ in subtype_values)})",
                (observation_id, *subtype_values.values()),
            )
        elif any(existing_observation[key] != value for key, value in observation_values.items()):
            raise ResultImportError("visual fingerprint observation ID already has different data")
        else:
            existing_subtype = connection.execute(
                f"SELECT {', '.join(subtype_values)} FROM visual_fingerprint_observations WHERE observation_id = ?",
                (observation_id,),
            ).fetchone()
            if existing_subtype is None or any(existing_subtype[key] != value for key, value in subtype_values.items()):
                raise ResultImportError("visual fingerprint observation subtype is missing or differs")


def _insert_import_ledger(connection, result: dict[str, Any]) -> None:
    batch_id = _producer_id("visual_fingerprint_import", result["_result_sha256"])
    values = {
        "result_sha256": result["_result_sha256"],
        "processing_run_id": result["processing_run"]["processing_run_id"],
        "recipe_id": result["recipe_id"],
        "catalog_context_json": canonical_json(result["catalog_context"]),
    }
    existing = connection.execute(
        f"SELECT {', '.join(values)} FROM visual_fingerprint_result_imports WHERE import_batch_id = ?",
        (batch_id,),
    ).fetchone()
    if existing is not None:
        if any(existing[key] != value for key, value in values.items()):
            raise ResultImportError("visual fingerprint import batch ID already has different data")
        return
    collision = connection.execute(
        "SELECT import_batch_id FROM visual_fingerprint_result_imports WHERE result_sha256 = ?",
        (result["_result_sha256"],),
    ).fetchone()
    if collision is not None:
        raise ResultImportError("visual fingerprint result digest already uses a different import batch")
    imported_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    connection.execute(
        f"INSERT INTO visual_fingerprint_result_imports(import_batch_id, {', '.join(values)}, imported_at) VALUES(?, {', '.join('?' for _ in values)}, ?)",
        (batch_id, *values.values(), imported_at),
    )


def import_visual_fingerprint_result(connection, path: Path) -> dict[str, Any]:
    result = validate_visual_fingerprint_result_file(path)
    run_id = result["processing_run"]["processing_run_id"]
    with transaction(connection):
        _reverify_files(result)
        _require_dependencies(connection, result)
        _insert_processing_run(connection, result)
        _insert_run_input(
            connection,
            run_id=run_id,
            object_type="media_object",
            object_id=result["input"]["media_id"],
            role="visual_input_media",
            digest=result["input"]["sha256"],
        )
        _insert_run_input(
            connection,
            run_id=run_id,
            object_type="artifact",
            object_id=result["input"]["artifact_id"],
            role="visual_input_artifact",
            digest=result["input"]["sha256"],
        )
        for frame in result["frames"]:
            _insert_artifact(connection, _artifact_row(result, frame))
        _insert_contextual_evidence(connection, result)
        _insert_import_ledger(connection, result)
    contextual_count = len(result["frames"]) if result["catalog_context"] else 0
    return {
        "importer_version": __version__,
        "result_sha256": result["_result_sha256"],
        "recipe_id": result["recipe_id"],
        "processing_run_id": run_id,
        "catalog_context": result["catalog_context"],
        "artifacts_inserted_or_present": len(result["frames"]),
        "fingerprints_inserted_or_present": contextual_count,
        "observations_inserted_or_present": contextual_count,
        "provenance_only": result["catalog_context"] is None,
        "calibration_state": "not_calibrated",
        "requires_human_review": True,
        "identity_assertions": 0,
        "duplicate_assertions": 0,
        "recording_relations": 0,
        "publication_decisions": 0,
        "publication_gate_decisions": 0,
    }


def _load_compare_result(path: Path) -> tuple[dict[str, Any], bytes, Path]:
    if not path.is_absolute():
        path = path.resolve()
    observed = _absolute_observed_path(str(path), "visual comparison result")
    if observed.lstat().st_mode & 0o222:
        raise ResultImportError("visual comparison result must be sealed read-only")
    body = _stable_read(
        observed, "visual comparison result", maximum_bytes=MAX_RESULT_BYTES
    )
    try:
        value = _object(json.loads(body), "visual comparison result")
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResultImportError("visual comparison result must be UTF-8 JSON") from error
    return value, body, observed


def _compare_context(value: object) -> dict[str, dict[str, str]] | None:
    if value is None:
        return None
    row = _object(value, "visual comparison catalog_context")
    _exact_keys(row, "visual comparison catalog_context", {"query", "candidate"})
    normalized: dict[str, dict[str, str]] = {}
    for role in ("query", "candidate"):
        side = _object(row[role], f"visual comparison catalog_context.{role}")
        _exact_keys(
            side,
            f"visual comparison catalog_context.{role}",
            {"recording_id", "rendition_id"},
        )
        normalized[role] = {
            "recording_id": _identifier(
                side["recording_id"],
                f"visual comparison catalog_context.{role}.recording_id",
            ),
            "rendition_id": _identifier(
                side["rendition_id"],
                f"visual comparison catalog_context.{role}.rendition_id",
            ),
        }
    return normalized


def _compare_threshold(value: object) -> dict[str, int]:
    row = _object(value, "visual comparison threshold")
    _exact_keys(
        row,
        "visual comparison threshold",
        {"maximum_hamming_distance", "top_k", "max_pairwise_comparisons"},
    )
    return {
        "maximum_hamming_distance": _integer(
            row["maximum_hamming_distance"],
            "visual comparison threshold.maximum_hamming_distance",
            minimum=0,
            maximum=PHASH_BITS,
        ),
        "top_k": _integer(
            row["top_k"], "visual comparison threshold.top_k", minimum=1, maximum=100
        ),
        "max_pairwise_comparisons": _integer(
            row["max_pairwise_comparisons"],
            "visual comparison threshold.max_pairwise_comparisons",
            minimum=1,
            maximum=MAX_PAIRWISE_COMPARISONS,
        ),
    }


def _compare_side(value: object, role: str) -> dict[str, Any]:
    label = f"visual comparison {role}"
    row = _object(value, label)
    _exact_keys(
        row,
        label,
        {
            "role", "result_path", "result_sha256", "result_key", "media_id",
            "algorithm", "phash_bits", "frames", "unchanged",
        },
    )
    if row["role"] != role or row["algorithm"] != ALGORITHM:
        raise ResultImportError(f"{label} role/algorithm is inconsistent")
    if _integer(row["phash_bits"], f"{label}.phash_bits", minimum=64, maximum=64) != 64:
        raise ResultImportError(f"{label} pHash width is unsupported")
    if row["unchanged"] is not True:
        raise ResultImportError(f"{label} must assert unchanged true")
    result_path = _absolute_observed_path(row["result_path"], f"{label}.result_path")
    extraction = validate_visual_fingerprint_result_file(result_path)
    result_sha = _sha256(row["result_sha256"], f"{label}.result_sha256")
    result_key = _sha256(row["result_key"], f"{label}.result_key")
    media_id = _identifier(row["media_id"], f"{label}.media_id")
    if (
        result_sha != extraction["_result_sha256"]
        or result_key != extraction["result_key"]
        or media_id != extraction["input"]["media_id"]
    ):
        raise ResultImportError(f"{label} does not match its extraction envelope")
    raw_frames = _array(row["frames"], f"{label}.frames")
    if not 1 <= len(raw_frames) <= MAX_SELECTED_FRAMES:
        raise ResultImportError(f"{label}.frames must contain 1..256 items")
    extraction_by_id = {
        frame["fingerprint_id"]: frame for frame in extraction["frames"]
    }
    frames: list[dict[str, Any]] = []
    for ordinal, raw_frame in enumerate(raw_frames):
        frame_label = f"{label}.frames[{ordinal}]"
        frame = _object(raw_frame, frame_label)
        _exact_keys(
            frame,
            frame_label,
            {
                "fingerprint_id", "sample_id", "requested_timestamp_ms",
                "decoded_relative_timestamp_ms", "phash_hex", "exact_gray_sha256",
                "quality_flags",
            },
        )
        fingerprint_id = _identifier(
            frame["fingerprint_id"], f"{frame_label}.fingerprint_id"
        )
        source = extraction_by_id.get(fingerprint_id)
        if source is None:
            raise ResultImportError(f"{frame_label} is absent from its extraction result")
        normalized = {
            "fingerprint_id": fingerprint_id,
            "sample_id": _identifier(frame["sample_id"], f"{frame_label}.sample_id"),
            "requested_timestamp_ms": _integer(
                frame["requested_timestamp_ms"],
                f"{frame_label}.requested_timestamp_ms",
                minimum=0,
            ),
            "decoded_relative_timestamp_ms": _integer(
                frame["decoded_relative_timestamp_ms"],
                f"{frame_label}.decoded_relative_timestamp_ms",
                minimum=0,
            ),
            "phash_hex": _string(frame["phash_hex"], f"{frame_label}.phash_hex", maximum=16),
            "exact_gray_sha256": _sha256(
                frame["exact_gray_sha256"], f"{frame_label}.exact_gray_sha256"
            ),
            "quality_flags": _validated_flags(
                frame["quality_flags"], f"{frame_label}.quality_flags", require_sorted=False
            ),
        }
        expected = {
            "fingerprint_id": source["fingerprint_id"],
            "sample_id": source["sample_id"],
            "requested_timestamp_ms": source["requested_timestamp_ms"],
            "decoded_relative_timestamp_ms": source["decoded_timestamp"]["relative_timestamp_ms"],
            "phash_hex": source["phash_hex"],
            "exact_gray_sha256": source["exact_gray_sha256"],
            "quality_flags": source["quality_flags"],
        }
        if normalized != expected or not PHASH_RE.fullmatch(normalized["phash_hex"]):
            raise ResultImportError(f"{frame_label} differs from its extraction evidence")
        frames.append(normalized)
    if len({frame["fingerprint_id"] for frame in frames}) != len(frames):
        raise ResultImportError(f"{label} contains duplicate fingerprint IDs")
    return {
        "role": role,
        "result_path": str(result_path),
        "result_sha256": result_sha,
        "result_key": result_key,
        "media_id": media_id,
        "algorithm": ALGORITHM,
        "phash_bits": PHASH_BITS,
        "frames": frames,
        "unchanged": True,
        "_extraction": extraction,
    }


def _compare_recipe(
    value: object,
    *,
    implementation: dict[str, Any],
    query: dict[str, Any],
    candidate: dict[str, Any],
    threshold: dict[str, int],
) -> dict[str, Any]:
    row = _object(value, "visual comparison recipe")
    _exact_keys(
        row,
        "visual comparison recipe",
        {
            "schema_version", "stage", "implementation_version",
            "implementation_sha256", "method", "algorithm", "phash_bits",
            "query_result_sha256", "candidate_result_sha256", "query_frame_ids",
            "candidate_frame_ids", "threshold", "calibration_state",
        },
    )
    expected = {
        "schema_version": SCHEMA_VERSION,
        "stage": COMPARE_STAGE,
        "implementation_version": IMPLEMENTATION_VERSION,
        "implementation_sha256": implementation["sha256"],
        "method": COMPARE_METHOD,
        "algorithm": ALGORITHM,
        "phash_bits": PHASH_BITS,
        "query_result_sha256": query["result_sha256"],
        "candidate_result_sha256": candidate["result_sha256"],
        "query_frame_ids": [frame["fingerprint_id"] for frame in query["frames"]],
        "candidate_frame_ids": [frame["fingerprint_id"] for frame in candidate["frames"]],
        "threshold": threshold,
        "calibration_state": "not_calibrated",
    }
    if canonical_json(row) != canonical_json(expected):
        raise ResultImportError("visual comparison recipe dependencies are inconsistent")
    return expected


def _expected_comparison(
    query: dict[str, Any], candidate: dict[str, Any], threshold: dict[str, int]
) -> dict[str, Any]:
    pairs: list[dict[str, Any]] = []
    for query_frame in query["frames"]:
        query_hash = int(query_frame["phash_hex"], 16)
        for candidate_frame in candidate["frames"]:
            distance = (
                query_hash ^ int(candidate_frame["phash_hex"], 16)
            ).bit_count()
            pairs.append(
                {
                    "query_fingerprint_id": query_frame["fingerprint_id"],
                    "candidate_fingerprint_id": candidate_frame["fingerprint_id"],
                    "hamming_distance": distance,
                    "normalized_hamming_distance": round(distance / PHASH_BITS, 6),
                    "raw_similarity": round((PHASH_BITS - distance) / PHASH_BITS, 6),
                    "exact_gray_equal": (
                        query_frame["exact_gray_sha256"]
                        == candidate_frame["exact_gray_sha256"]
                    ),
                }
            )
    pairs.sort(
        key=lambda pair: (
            pair["hamming_distance"],
            pair["query_fingerprint_id"],
            pair["candidate_fingerprint_id"],
        )
    )
    best = pairs[0]["hamming_distance"]
    meets = best <= threshold["maximum_hamming_distance"]
    return {
        "match_candidate_id": _producer_id(
            "visual_match_candidate",
            query["result_sha256"],
            candidate["result_sha256"],
            [frame["fingerprint_id"] for frame in query["frames"]],
            [frame["fingerprint_id"] for frame in candidate["frames"]],
            threshold,
        ),
        "pairwise_comparisons": len(pairs),
        "best_hamming_distance": best,
        "best_normalized_hamming_distance": round(best / PHASH_BITS, 6),
        "best_raw_similarity": round((PHASH_BITS - best) / PHASH_BITS, 6),
        "exact_gray_pair_count": sum(pair["exact_gray_equal"] for pair in pairs),
        "top_pairs": pairs[: threshold["top_k"]],
        "threshold_state": (
            "meets_configured_threshold"
            if meets
            else "does_not_meet_configured_threshold"
        ),
        "candidate_emitted": meets,
        "decision_state": (
            "candidate_for_human_review" if meets else "below_configured_threshold"
        ),
        "score_semantics": "raw_64_bit_phash_hamming_not_probability",
        "calibration_state": "not_calibrated",
        "calibrated_probability": None,
        "requires_human_review": True,
        "assertions": {
            "person_identity": False,
            "duplicate": False,
            "parent": False,
            "ownership": False,
            "relationship": False,
            "unrelated": False,
        },
        "warning": COMPARE_WARNING,
    }


def validate_visual_fingerprint_compare_result_file(path: Path) -> dict[str, Any]:
    result, body, result_path = _load_compare_result(path)
    _exact_keys(
        result,
        "visual comparison result",
        {
            "schema_version", "stage", "implementation_version", "status", "dry_run",
            "job_id", "work_order_sha256", "recipe", "recipe_id", "recipe_sha256",
            "result_key", "implementation", "method", "query", "candidate",
            "threshold", "comparison", "processing_run", "catalog_context",
            "result_path", "errors",
        },
    )
    if (
        _integer(result["schema_version"], "visual comparison schema_version", minimum=1, maximum=1)
        != SCHEMA_VERSION
        or result["stage"] != COMPARE_STAGE
        or result["implementation_version"] != IMPLEMENTATION_VERSION
        or result["status"] != "completed"
        or result["dry_run"] is not False
        or result["errors"] != []
        or result["result_path"] != str(result_path)
    ):
        raise ResultImportError("only completed non-dry-run visual comparison v1 results are importable")
    expected_body = (
        json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")
    if body != expected_body:
        raise ResultImportError("visual comparison result JSON is not producer-canonical")
    job_id = _identifier(result["job_id"], "visual comparison job_id")
    work_order_sha = _sha256(
        result["work_order_sha256"], "visual comparison work_order_sha256"
    )
    implementation, implementation_path = _implementation(result["implementation"])
    query = _compare_side(result["query"], "query")
    candidate = _compare_side(result["candidate"], "candidate")
    if query["result_sha256"] == candidate["result_sha256"]:
        raise ResultImportError("visual comparison rejects the same extraction on both sides")
    if set(frame["fingerprint_id"] for frame in query["frames"]) & set(
        frame["fingerprint_id"] for frame in candidate["frames"]
    ):
        raise ResultImportError("visual comparison sides share a fingerprint ID")
    threshold = _compare_threshold(result["threshold"])
    if len(query["frames"]) * len(candidate["frames"]) > threshold["max_pairwise_comparisons"]:
        raise ResultImportError("visual comparison selected frames exceed the pair cap")
    context = _compare_context(result["catalog_context"])
    recipe = _compare_recipe(
        result["recipe"],
        implementation=implementation,
        query=query,
        candidate=candidate,
        threshold=threshold,
    )
    recipe_sha = sha256_bytes(_canonical_bytes(recipe))
    recipe_id = f"recipe_visual_fingerprint_compare_{recipe_sha[:32]}"
    if (
        _sha256(result["recipe_sha256"], "visual comparison recipe_sha256") != recipe_sha
        or result["recipe_id"] != recipe_id
        or result["method"] != COMPARE_METHOD
    ):
        raise ResultImportError("visual comparison recipe identity is inconsistent")
    result_key = _sha256(result["result_key"], "visual comparison result_key")
    expected_key = sha256_bytes(
        _canonical_bytes(
            {
                "job_id": job_id,
                "work_order_sha256": work_order_sha,
                "recipe_sha256": recipe_sha,
            }
        )
    )
    if result_key != expected_key:
        raise ResultImportError("visual comparison result key is inconsistent")

    result_dir = result_path.parent
    results_dir = result_dir.parent
    pair_dir = results_dir.parent
    prefix_dir = pair_dir.parent
    sha_dir = prefix_dir.parent
    comparison_dir = sha_dir.parent
    vision_dir = comparison_dir.parent
    expected_pair_digest = sha256_bytes(
        _canonical_bytes(sorted((query["result_sha256"], candidate["result_sha256"])))
    )
    if (
        result_dir.name != result_key
        or results_dir.name != "results"
        or pair_dir.name != expected_pair_digest
        or prefix_dir.name != expected_pair_digest[:2]
        or sha_dir.name != "sha256"
        or comparison_dir.name != "visual-fingerprint-comparisons"
        or vision_dir.name != "vision"
    ):
        raise ResultImportError("visual comparison result leaves its exact immutable layout")
    _sealed_directory(result_dir, "visual comparison result directory")
    reconstructed_order = {
        "schema_version": 1,
        "job_id": job_id,
        "method": COMPARE_METHOD,
        "query": {
            "role": "query",
            "result_path": query["result_path"],
            "expected_sha256": query["result_sha256"],
            "frame_ids": [frame["fingerprint_id"] for frame in query["frames"]],
        },
        "candidate": {
            "role": "candidate",
            "result_path": candidate["result_path"],
            "expected_sha256": candidate["result_sha256"],
            "frame_ids": [frame["fingerprint_id"] for frame in candidate["frames"]],
        },
        "threshold": threshold,
        "catalog_context": context,
        "output": {"root": str(vision_dir.parent)},
    }
    if sha256_bytes(_canonical_bytes(reconstructed_order)) != work_order_sha:
        raise ResultImportError("visual comparison work-order digest is inconsistent")
    expected_comparison = _expected_comparison(query, candidate, threshold)
    comparison = _object(result["comparison"], "visual comparison evidence")
    if canonical_json(comparison) != canonical_json(expected_comparison):
        raise ResultImportError("visual comparison evidence is inconsistent")
    run = _object(result["processing_run"], "visual comparison processing_run")
    _exact_keys(
        run,
        "visual comparison processing_run",
        {"processing_run_id", "started_at", "completed_at", "status"},
    )
    run_id = _identifier(run["processing_run_id"], "visual comparison processing_run_id")
    started_at = _timestamp(run["started_at"], "visual comparison started_at")
    completed_at = _timestamp(run["completed_at"], "visual comparison completed_at")
    if (
        run_id != f"run_visual_fingerprint_compare_{result_key[:32]}"
        or run["status"] != "completed"
        or started_at != run["started_at"]
        or completed_at != run["completed_at"]
        or _timestamp_value(completed_at) < _timestamp_value(started_at)
    ):
        raise ResultImportError("visual comparison processing-run evidence is inconsistent")
    normalized = dict(result)
    normalized.update(
        {
            "job_id": job_id,
            "work_order_sha256": work_order_sha,
            "recipe": recipe,
            "recipe_id": recipe_id,
            "recipe_sha256": recipe_sha,
            "result_key": result_key,
            "implementation": implementation,
            "query": query,
            "candidate": candidate,
            "threshold": threshold,
            "comparison": expected_comparison,
            "processing_run": {**run, "started_at": started_at, "completed_at": completed_at},
            "catalog_context": context,
            "_result_path": result_path,
            "_result_sha256": sha256_bytes(body),
            "_result_byte_count": len(body),
            "_implementation_path": implementation_path,
        }
    )
    return normalized


def _reverify_compare_files(result: dict[str, Any]) -> None:
    result_path = result["_result_path"]
    if result_path.lstat().st_mode & 0o222:
        raise ResultImportError("visual comparison result is no longer sealed read-only")
    _verify_hash(
        result_path,
        result["_result_sha256"],
        result["_result_byte_count"],
        "visual comparison result",
    )
    _verify_hash(
        result["_implementation_path"],
        result["implementation"]["sha256"],
        result["implementation"]["byte_count"],
        "visual comparison implementation",
    )
    for role in ("query", "candidate"):
        _reverify_files(result[role]["_extraction"])


def _require_compare_dependencies(connection, result: dict[str, Any]) -> None:
    context = result["catalog_context"]
    if context is None:
        raise ResultImportError(
            "visual comparison import requires explicit query and candidate catalog context"
        )
    for role in ("query", "candidate"):
        side = result[role]
        extraction = side["_extraction"]
        if extraction["catalog_context"] != context[role]:
            raise ResultImportError(
                f"visual comparison {role} context differs from its extraction result"
            )
        _require_dependencies(connection, extraction)
        receipt = connection.execute(
            """
            SELECT import_batch_id, processing_run_id, recipe_id, catalog_context_json
            FROM visual_fingerprint_result_imports
            WHERE result_sha256 = ?
            """,
            (side["result_sha256"],),
        ).fetchone()
        if (
            receipt is None
            or receipt["processing_run_id"]
            != extraction["processing_run"]["processing_run_id"]
            or receipt["recipe_id"] != extraction["recipe_id"]
            or receipt["catalog_context_json"] != canonical_json(context[role])
        ):
            raise ResultImportError(
                f"visual comparison {role} extraction receipt is missing or differs"
            )
        side["_extraction_import_batch_id"] = receipt["import_batch_id"]
        for frame in side["frames"]:
            row = connection.execute(
                """
                SELECT visual.sample_id, visual.requested_timestamp_ms,
                       visual.relative_timestamp_ms, visual.phash_hex,
                       visual.exact_gray_sha256, visual.quality_flags_json,
                       observation.processing_run_id, observation.recording_id,
                       observation.rendition_id, observation.visibility,
                       observation.review_state, fingerprint.media_id
                FROM visual_fingerprint_observations AS visual
                JOIN observations AS observation
                  ON observation.observation_id = visual.observation_id
                JOIN fingerprints AS fingerprint
                  ON fingerprint.fingerprint_id = visual.fingerprint_id
                WHERE visual.fingerprint_id = ?
                """,
                (frame["fingerprint_id"],),
            ).fetchone()
            expected = (
                frame["sample_id"],
                frame["requested_timestamp_ms"],
                frame["decoded_relative_timestamp_ms"],
                frame["phash_hex"],
                frame["exact_gray_sha256"],
                canonical_json(frame["quality_flags"]),
                extraction["processing_run"]["processing_run_id"],
                context[role]["recording_id"],
                context[role]["rendition_id"],
                "private",
                "machine",
                side["media_id"],
            )
            if row is None or tuple(row) != expected:
                raise ResultImportError(
                    f"visual comparison {role} selected fingerprint is missing or differs"
                )


def _insert_exact_row(
    connection,
    *,
    table: str,
    key_column: str,
    key_value: str,
    values: dict[str, Any],
    label: str,
) -> bool:
    existing = connection.execute(
        f"SELECT {', '.join(values)} FROM {table} WHERE {key_column} = ?",
        (key_value,),
    ).fetchone()
    if existing is not None:
        if any(existing[key] != value for key, value in values.items()):
            raise ResultImportError(f"{label} ID already has different data")
        return False
    connection.execute(
        f"INSERT INTO {table}({key_column}, {', '.join(values)}) "
        f"VALUES(?, {', '.join('?' for _ in values)})",
        (key_value, *values.values()),
    )
    return True


def _insert_compare_processing_run(connection, result: dict[str, Any]) -> None:
    run = result["processing_run"]
    parameters = canonical_json(
        {
            "algorithm": ALGORITHM,
            "assertions": result["comparison"]["assertions"],
            "calibration_state": "not_calibrated",
            "method": COMPARE_METHOD,
            "recipe_id": result["recipe_id"],
            "recipe_sha256": result["recipe_sha256"],
            "result_key": result["result_key"],
            "threshold": result["threshold"],
        }
    )
    values = {
        "stage": COMPARE_STAGE,
        "implementation_version": IMPLEMENTATION_VERSION,
        "model_id": None,
        "glossary_revision_id": None,
        "parameters_json": parameters,
        "environment_json": canonical_json(
            {"implementation": result["implementation"], "network": "not_used"}
        ),
        "random_seed": None,
        "started_at": run["started_at"],
        "completed_at": run["completed_at"],
        "status": "completed",
        "error_text": None,
    }
    _insert_exact_row(
        connection,
        table="processing_runs",
        key_column="processing_run_id",
        key_value=run["processing_run_id"],
        values=values,
        label="visual comparison processing run",
    )


def _insert_compare_import(connection, result: dict[str, Any]) -> tuple[str, str]:
    import_batch_id = _producer_id(
        "visual_fingerprint_compare_import", result["_result_sha256"]
    )
    imported_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )
    values = {
        "comparison_id": result["comparison"]["match_candidate_id"],
        "result_sha256": result["_result_sha256"],
        "result_path": str(result["_result_path"]),
        "result_byte_count": result["_result_byte_count"],
        "processing_run_id": result["processing_run"]["processing_run_id"],
        "recipe_id": result["recipe_id"],
        "recipe_sha256": result["recipe_sha256"],
        "result_key": result["result_key"],
        "implementation_sha256": result["implementation"]["sha256"],
        "implementation_byte_count": result["implementation"]["byte_count"],
        "query_result_sha256": result["query"]["result_sha256"],
        "candidate_result_sha256": result["candidate"]["result_sha256"],
        "catalog_context_json": canonical_json(result["catalog_context"]),
        "visibility": "private",
        "publication_authority": "none",
        "imported_at": imported_at,
    }
    existing = connection.execute(
        "SELECT imported_at FROM visual_fingerprint_compare_imports WHERE import_batch_id = ?",
        (import_batch_id,),
    ).fetchone()
    if existing is not None:
        values["imported_at"] = existing["imported_at"]
    _insert_exact_row(
        connection,
        table="visual_fingerprint_compare_imports",
        key_column="import_batch_id",
        key_value=import_batch_id,
        values=values,
        label="visual comparison import",
    )
    return import_batch_id, values["imported_at"]


def _insert_compare_side(connection, result: dict[str, Any], role: str) -> None:
    comparison_id = result["comparison"]["match_candidate_id"]
    side = result[role]
    extraction = side["_extraction"]
    context = result["catalog_context"][role]
    values = {
        "extraction_run_id": extraction["processing_run"]["processing_run_id"],
        "extraction_import_batch_id": side["_extraction_import_batch_id"],
        "extraction_result_sha256": side["result_sha256"],
        "extraction_result_path": side["result_path"],
        "extraction_result_byte_count": extraction["_result_byte_count"],
        "extraction_result_key": side["result_key"],
        "extraction_recipe_id": extraction["recipe_id"],
        "media_id": side["media_id"],
        "recording_id": context["recording_id"],
        "rendition_id": context["rendition_id"],
        "frame_count": len(side["frames"]),
        "algorithm": ALGORITHM,
        "phash_bits": PHASH_BITS,
        "unchanged": 1,
    }
    existing = connection.execute(
        f"SELECT {', '.join(values)} FROM visual_fingerprint_compare_sides "
        "WHERE comparison_id = ? AND role = ?",
        (comparison_id, role),
    ).fetchone()
    if existing is None:
        connection.execute(
            f"INSERT INTO visual_fingerprint_compare_sides(comparison_id, role, {', '.join(values)}) "
            f"VALUES(?, ?, {', '.join('?' for _ in values)})",
            (comparison_id, role, *values.values()),
        )
    elif any(existing[key] != value for key, value in values.items()):
        raise ResultImportError(f"visual comparison {role} side already has different data")

    for ordinal, frame in enumerate(side["frames"]):
        frame_values = {
            "fingerprint_id": frame["fingerprint_id"],
            "sample_id": frame["sample_id"],
            "requested_timestamp_ms": frame["requested_timestamp_ms"],
            "decoded_relative_timestamp_ms": frame["decoded_relative_timestamp_ms"],
            "phash_hex": frame["phash_hex"],
            "exact_gray_sha256": frame["exact_gray_sha256"],
            "quality_flags_json": canonical_json(frame["quality_flags"]),
        }
        existing_frame = connection.execute(
            f"SELECT {', '.join(frame_values)} FROM visual_fingerprint_compare_side_frames "
            "WHERE comparison_id = ? AND role = ? AND ordinal = ?",
            (comparison_id, role, ordinal),
        ).fetchone()
        if existing_frame is None:
            connection.execute(
                f"INSERT INTO visual_fingerprint_compare_side_frames("
                f"comparison_id, role, ordinal, {', '.join(frame_values)}) "
                f"VALUES(?, ?, ?, {', '.join('?' for _ in frame_values)})",
                (comparison_id, role, ordinal, *frame_values.values()),
            )
        elif any(
            existing_frame[key] != value for key, value in frame_values.items()
        ):
            raise ResultImportError(
                f"visual comparison {role} selected frame already has different data"
            )


def _comparison_review_task_id(comparison_id: str) -> str:
    return _producer_id("review_task_visual_fingerprint_compare", comparison_id)


def _insert_comparison_summary(connection, result: dict[str, Any]) -> None:
    comparison = result["comparison"]
    comparison_id = comparison["match_candidate_id"]
    emitted = comparison["candidate_emitted"]
    assertions = comparison["assertions"]
    values = {
        "processing_run_id": result["processing_run"]["processing_run_id"],
        "method": COMPARE_METHOD,
        "algorithm": ALGORITHM,
        "phash_bits": PHASH_BITS,
        "pairwise_comparisons": comparison["pairwise_comparisons"],
        "best_hamming_distance": comparison["best_hamming_distance"],
        "best_normalized_hamming_distance": comparison["best_normalized_hamming_distance"],
        "best_raw_similarity": comparison["best_raw_similarity"],
        "exact_gray_pair_count": comparison["exact_gray_pair_count"],
        "maximum_hamming_distance": result["threshold"]["maximum_hamming_distance"],
        "top_k": result["threshold"]["top_k"],
        "max_pairwise_comparisons": result["threshold"]["max_pairwise_comparisons"],
        "threshold_state": comparison["threshold_state"],
        "candidate_emitted": int(emitted),
        "decision_state": comparison["decision_state"],
        "score_semantics": comparison["score_semantics"],
        "calibration_state": "not_calibrated",
        "calibrated_probability": None,
        "requires_human_review": 1,
        "person_identity_asserted": int(assertions["person_identity"]),
        "duplicate_asserted": int(assertions["duplicate"]),
        "parent_asserted": int(assertions["parent"]),
        "ownership_asserted": int(assertions["ownership"]),
        "relationship_asserted": int(assertions["relationship"]),
        "unrelated_asserted": int(assertions["unrelated"]),
        "match_candidate_id": comparison_id if emitted else None,
        "review_task_id": _comparison_review_task_id(comparison_id) if emitted else None,
        "warning": comparison["warning"],
        "visibility": "private",
        "publication_authority": "none",
        "created_at": result["processing_run"]["completed_at"],
    }
    _insert_exact_row(
        connection,
        table="visual_fingerprint_comparisons",
        key_column="comparison_id",
        key_value=comparison_id,
        values=values,
        label="visual comparison summary",
    )
    for rank, pair in enumerate(comparison["top_pairs"]):
        pair_values = {
            "query_fingerprint_id": pair["query_fingerprint_id"],
            "candidate_fingerprint_id": pair["candidate_fingerprint_id"],
            "hamming_distance": pair["hamming_distance"],
            "normalized_hamming_distance": pair["normalized_hamming_distance"],
            "raw_similarity": pair["raw_similarity"],
            "exact_gray_equal": int(pair["exact_gray_equal"]),
        }
        existing = connection.execute(
            f"SELECT {', '.join(pair_values)} FROM visual_fingerprint_compare_top_pairs "
            "WHERE comparison_id = ? AND rank = ?",
            (comparison_id, rank),
        ).fetchone()
        if existing is None:
            connection.execute(
                f"INSERT INTO visual_fingerprint_compare_top_pairs("
                f"comparison_id, rank, {', '.join(pair_values)}) "
                f"VALUES(?, ?, {', '.join('?' for _ in pair_values)})",
                (comparison_id, rank, *pair_values.values()),
            )
        elif any(existing[key] != value for key, value in pair_values.items()):
            raise ResultImportError("visual comparison top pair already has different data")


def _insert_compare_candidate_and_task(connection, result: dict[str, Any]) -> None:
    comparison = result["comparison"]
    if not comparison["candidate_emitted"]:
        return
    comparison_id = comparison["match_candidate_id"]
    query_run = result["query"]["_extraction"]["processing_run"]["processing_run_id"]
    candidate_run = result["candidate"]["_extraction"]["processing_run"]["processing_run_id"]
    metadata = canonical_json(
        {
            "calibration_state": "not_calibrated",
            "candidate_emitted": True,
            "duplicate_asserted": False,
            "ownership_asserted": False,
            "parent_asserted": False,
            "person_identity_asserted": False,
            "publication_authority": "none",
            "relationship_asserted": False,
            "requires_human_review": True,
            "score_semantics": comparison["score_semantics"],
            "unrelated_asserted": False,
            "visibility": "private",
        }
    )
    match_values = {
        "left_object_type": "visual_fingerprint_extraction_result",
        "left_object_id": query_run,
        "right_object_type": "visual_fingerprint_extraction_result",
        "right_object_id": candidate_run,
        "match_method": COMPARE_MATCH_METHOD,
        "raw_score": comparison["best_raw_similarity"],
        "calibrated_probability": None,
        "decision_state": "candidate",
        "metadata_json": metadata,
    }
    _insert_exact_row(
        connection,
        table="match_candidates",
        key_column="match_candidate_id",
        key_value=comparison_id,
        values=match_values,
        label="visual comparison match candidate",
    )
    task_id = _comparison_review_task_id(comparison_id)
    task_values = {
        "task_kind": "visual_fingerprint_comparison_review",
        "target_type": "visual_fingerprint_comparison",
        "target_id": comparison_id,
        "reason": COMPARE_REVIEW_REASON,
        "priority": 75,
        "status": "open",
        "created_at": result["processing_run"]["completed_at"],
        "updated_at": result["processing_run"]["completed_at"],
    }
    existing = connection.execute(
        """
        SELECT task_kind, target_type, target_id, reason, priority, status,
               created_at, updated_at
        FROM review_tasks WHERE review_task_id = ?
        """,
        (task_id,),
    ).fetchone()
    if existing is None:
        connection.execute(
            """
            INSERT INTO review_tasks(
                review_task_id, task_kind, target_type, target_id, reason,
                priority, status, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (task_id, *task_values.values()),
        )
    else:
        immutable = (
            "task_kind", "target_type", "target_id", "reason", "priority", "created_at"
        )
        if any(existing[key] != task_values[key] for key in immutable):
            raise ResultImportError("visual comparison review task already has different data")


def _insert_compare_completion(
    connection, result: dict[str, Any], import_batch_id: str
) -> None:
    comparison_id = result["comparison"]["match_candidate_id"]
    receipt_id = _producer_id(
        "visual_fingerprint_compare_completion", result["_result_sha256"]
    )
    _insert_exact_row(
        connection,
        table="visual_fingerprint_compare_completion_receipts",
        key_column="completion_receipt_id",
        key_value=receipt_id,
        values={
            "import_batch_id": import_batch_id,
            "comparison_id": comparison_id,
            "completed_at": result["processing_run"]["completed_at"],
        },
        label="visual comparison completion receipt",
    )


def import_visual_fingerprint_compare_result(connection, path: Path) -> dict[str, Any]:
    result = validate_visual_fingerprint_compare_result_file(path)
    run_id = result["processing_run"]["processing_run_id"]
    comparison_id = result["comparison"]["match_candidate_id"]
    with transaction(connection):
        _reverify_compare_files(result)
        _require_compare_dependencies(connection, result)
        _insert_compare_processing_run(connection, result)
        for role in ("query", "candidate"):
            extraction = result[role]["_extraction"]
            _insert_run_input(
                connection,
                run_id=run_id,
                object_type="visual_fingerprint_extraction_result",
                object_id=extraction["processing_run"]["processing_run_id"],
                role=f"{role}_result",
                digest=result[role]["result_sha256"],
            )
        import_batch_id, _ = _insert_compare_import(connection, result)
        for role in ("query", "candidate"):
            _insert_compare_side(connection, result, role)
        _insert_comparison_summary(connection, result)
        _insert_compare_candidate_and_task(connection, result)
        _insert_compare_completion(connection, result, import_batch_id)
    emitted = result["comparison"]["candidate_emitted"]
    return {
        "importer_version": __version__,
        "result_sha256": result["_result_sha256"],
        "recipe_id": result["recipe_id"],
        "processing_run_id": run_id,
        "comparison_id": comparison_id,
        "catalog_context": result["catalog_context"],
        "selected_frames_inserted_or_present": (
            len(result["query"]["frames"]) + len(result["candidate"]["frames"])
        ),
        "top_pairs_inserted_or_present": len(result["comparison"]["top_pairs"]),
        "candidate_emitted": emitted,
        "decision_state": result["comparison"]["decision_state"],
        "generic_match_candidates_inserted_or_present": int(emitted),
        "review_tasks_inserted_or_present": int(emitted),
        "calibration_state": "not_calibrated",
        "calibrated_probability": None,
        "requires_human_review": True,
        "person_identity_assertions": 0,
        "duplicate_assertions": 0,
        "parent_assertions": 0,
        "ownership_assertions": 0,
        "recording_relations": 0,
        "unrelated_assertions": 0,
        "publication_decisions": 0,
        "publication_gate_decisions": 0,
    }
