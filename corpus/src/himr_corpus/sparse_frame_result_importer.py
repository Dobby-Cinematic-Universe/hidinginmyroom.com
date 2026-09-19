"""Strict, private-only admission for completed sparse-frame routing results.

The sparse-frame producer cannot open SQLite.  This module is its independent trust
boundary: it rehashes every referenced file, reconstructs producer identities and
selection decisions, binds the proxy to an already-imported preprocessing lineage,
and writes only private artifacts plus machine routing candidates.  It deliberately
has no OCR-text, face, identity, content-claim, or publication write path.
"""

from __future__ import annotations

import json
import math
import stat
import struct
import zlib
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
    _number,
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
from .ids import stable_id
from .importers import canonical_json, sha256_bytes
from .result_importers import (
    ResultImportError,
    _begin_result_batch,
    _complete_result_batch,
    _upsert_job_and_attempt,
    validate_preprocess_result,
)


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
STAGE = "sparse_frame_router"
MAX_RESULT_BYTES = 128 * 1024 * 1024
MAX_FRAME_BYTES = 64 * 1024 * 1024
MAX_FRAMES = 256
MAX_DURATION_MS = 7 * 24 * 60 * 60 * 1_000
MAX_PIXELS = 1280 * 720
SELECTION_REASON_ORDER = (
    "FRAME_RECORDING_START",
    "FRAME_SCENE_CHANGE",
    "FRAME_PERIODIC_COVERAGE",
)
OCR_REASON_BY_SELECTION = {
    "FRAME_RECORDING_START": "OCR_CANDIDATE_RECORDING_START",
    "FRAME_SCENE_CHANGE": "OCR_CANDIDATE_SCENE_CHANGE",
    "FRAME_PERIODIC_COVERAGE": "OCR_CANDIDATE_PERIODIC_COVERAGE",
}
OCR_WARNING = (
    "This is an extraction/routing candidate only. OCR and text-presence "
    "evaluation have not run, and no person or content is identified."
)


def _canonical_bytes(value: object) -> bytes:
    return canonical_json(value).encode("utf-8")


def _canonical_equal(left: object, right: object) -> bool:
    """Compare JSON values without Python's ``True == 1`` coercion."""

    return canonical_json(left) == canonical_json(right)


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ResultImportError(
                f"sparse-frame input contains duplicate JSON key {key!r}"
            )
        value[key] = item
    return value


def _load_json_bytes(body: bytes, label: str) -> dict[str, Any]:
    try:
        return _object(
            json.loads(body, object_pairs_hook=_reject_duplicate_json_keys), label
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResultImportError(f"{label} must be UTF-8 JSON") from error


def _sealed_file(value: object, label: str) -> Path:
    path = _absolute_observed_path(value, label)
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise ResultImportError(f"{label} cannot be inspected: {error}") from error
    if not stat.S_ISREG(mode) or stat.S_ISLNK(mode) or mode & 0o222:
        raise ResultImportError(f"{label} must be a sealed read-only regular file")
    return path


def _sealed_directory(path: Path, label: str) -> None:
    if not path.is_absolute():
        raise ResultImportError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=True)
        mode = path.lstat().st_mode
    except OSError as error:
        raise ResultImportError(f"{label} cannot be inspected: {error}") from error
    if path != resolved or not stat.S_ISDIR(mode) or stat.S_ISLNK(mode) or mode & 0o222:
        raise ResultImportError(f"{label} must be a sealed non-symlink directory")


def _stat_observation(value: object, label: str) -> dict[str, int]:
    row = _object(value, label)
    _exact_keys(row, label, {"device", "inode", "byte_count", "mtime_ns"})
    return {
        "device": _integer(row["device"], f"{label}.device"),
        "inode": _integer(row["inode"], f"{label}.inode", minimum=0),
        "byte_count": _integer(row["byte_count"], f"{label}.byte_count"),
        "mtime_ns": _integer(row["mtime_ns"], f"{label}.mtime_ns"),
    }


def _file_observation(
    value: object,
    label: str,
    *,
    sealed: bool,
    maximum_bytes: int | None = None,
) -> tuple[dict[str, Any], Path]:
    row = _object(value, label)
    required = {"path", "sha256", "byte_count", "stat_before", "stat_after", "unchanged"}
    _exact_keys(row, label, required)
    path = _sealed_file(row["path"], f"{label}.path") if sealed else _absolute_observed_path(
        row["path"], f"{label}.path"
    )
    digest = _sha256(row["sha256"], f"{label}.sha256")
    byte_count = _integer(row["byte_count"], f"{label}.byte_count", minimum=1)
    if maximum_bytes is not None and byte_count > maximum_bytes:
        raise ResultImportError(f"{label}.byte_count exceeds the admission limit")
    before = _stat_observation(row["stat_before"], f"{label}.stat_before")
    after = _stat_observation(row["stat_after"], f"{label}.stat_after")
    if row["unchanged"] is not True or before != after or before["byte_count"] != byte_count:
        raise ResultImportError(f"{label} does not carry an unchanged file observation")
    _verify_hash(path, digest, byte_count, label)
    return dict(row), path


def _parse_canonical_json_string(value: object, label: str) -> tuple[str, object]:
    text = _string(value, label)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as error:
        raise ResultImportError(f"{label} must contain JSON") from error
    if canonical_json(parsed) != text:
        raise ResultImportError(f"{label} must use canonical JSON encoding")
    return text, parsed


def _sampling(value: object) -> dict[str, Any]:
    row = _object(value, "sparse-frame sampling")
    _exact_keys(
        row,
        "sparse-frame sampling",
        {"include_recording_start", "scene_changes", "periodic", "min_separation_ms"},
    )
    if row["include_recording_start"] is not True:
        raise ResultImportError("sparse-frame sampling must include recording start")
    scene = _object(row["scene_changes"], "sparse-frame scene sampling")
    periodic = _object(row["periodic"], "sparse-frame periodic sampling")
    _exact_keys(scene, "sparse-frame scene sampling", {"enabled", "max_frames", "offset_ms"})
    _exact_keys(periodic, "sparse-frame periodic sampling", {"enabled", "interval_ms", "max_frames"})
    if not isinstance(scene["enabled"], bool) or not isinstance(periodic["enabled"], bool):
        raise ResultImportError("sparse-frame sampling enabled fields must be boolean")
    normalized = {
        "include_recording_start": True,
        "scene_changes": {
            "enabled": scene["enabled"],
            "max_frames": _integer(scene["max_frames"], "scene max_frames", minimum=0, maximum=255),
            "offset_ms": _integer(scene["offset_ms"], "scene offset_ms", minimum=0, maximum=60_000),
        },
        "periodic": {
            "enabled": periodic["enabled"],
            "interval_ms": _integer(
                periodic["interval_ms"], "periodic interval_ms", minimum=1_000, maximum=3_600_000
            ),
            "max_frames": _integer(periodic["max_frames"], "periodic max_frames", minimum=0, maximum=255),
        },
        "min_separation_ms": _integer(
            row["min_separation_ms"], "sampling min_separation_ms", minimum=0, maximum=60_000
        ),
    }
    if normalized["scene_changes"]["enabled"] != (normalized["scene_changes"]["max_frames"] > 0):
        raise ResultImportError("scene sampling enabled/cap relationship is invalid")
    if normalized["periodic"]["enabled"] != (normalized["periodic"]["max_frames"] > 0):
        raise ResultImportError("periodic sampling enabled/cap relationship is invalid")
    return normalized


def _limits(value: object, sampling: dict[str, Any]) -> dict[str, int]:
    row = _object(value, "sparse-frame limits")
    _exact_keys(
        row,
        "sparse-frame limits",
        {
            "max_frames", "max_media_duration_ms", "max_input_pixels", "max_frame_bytes",
            "max_timestamp_drift_ms", "timeout_seconds_per_frame",
        },
    )
    normalized = {
        "max_frames": _integer(row["max_frames"], "limits.max_frames", minimum=1, maximum=MAX_FRAMES),
        "max_media_duration_ms": _integer(
            row["max_media_duration_ms"], "limits.max_media_duration_ms", minimum=1, maximum=MAX_DURATION_MS
        ),
        "max_input_pixels": _integer(
            row["max_input_pixels"], "limits.max_input_pixels", minimum=1, maximum=MAX_PIXELS
        ),
        "max_frame_bytes": _integer(
            row["max_frame_bytes"], "limits.max_frame_bytes", minimum=1_024, maximum=MAX_FRAME_BYTES
        ),
        "max_timestamp_drift_ms": _integer(
            row["max_timestamp_drift_ms"], "limits.max_timestamp_drift_ms", maximum=10_000
        ),
        "timeout_seconds_per_frame": _integer(
            row["timeout_seconds_per_frame"], "limits.timeout_seconds_per_frame", minimum=1, maximum=600
        ),
    }
    reserved = 1
    if sampling["scene_changes"]["enabled"]:
        reserved += sampling["scene_changes"]["max_frames"]
    if sampling["periodic"]["enabled"]:
        reserved += sampling["periodic"]["max_frames"]
    if reserved > normalized["max_frames"]:
        raise ResultImportError("sparse-frame sampling caps exceed limits.max_frames")
    return normalized


def _proxy_probe_from_preprocess(
    artifact: dict[str, Any], digest: str, byte_count: int, limits: dict[str, int]
) -> dict[str, Any]:
    probe = _object(artifact.get("normalized_probe"), "preprocess proxy normalized_probe")
    media = _object(probe.get("media"), "preprocess proxy normalized_probe.media")
    primary = _object(probe.get("primary_streams"), "preprocess proxy primary_streams")
    format_row = _object(probe.get("format"), "preprocess proxy format")
    streams = _array(probe.get("streams"), "preprocess proxy streams")
    if (
        media.get("media_id") != f"media_sha256_{digest}"
        or media.get("sha256") != digest
        or media.get("byte_count") != byte_count
    ):
        raise ResultImportError("preprocess proxy normalized probe disagrees with proxy bytes")
    video_index = _integer(primary.get("video_index"), "preprocess proxy primary video index", minimum=0)
    matches = [stream for stream in streams if isinstance(stream, dict) and stream.get("index") == video_index]
    if len(matches) != 1 or matches[0].get("codec_type") != "video":
        raise ResultImportError("preprocess proxy normalized probe has no unique primary video")
    stream = matches[0]
    video = _object(stream.get("video"), "preprocess proxy primary video")
    width = _integer(video.get("width"), "preprocess proxy width", minimum=1, maximum=1280)
    height = _integer(video.get("height"), "preprocess proxy height", minimum=1, maximum=1280)
    if width * height > limits["max_input_pixels"]:
        raise ResultImportError("preprocess proxy dimensions exceed sparse-frame limits")
    duration_value = format_row.get("duration_ms")
    if duration_value is None:
        duration_value = stream.get("duration_ms")
    duration_ms = _integer(
        duration_value, "preprocess proxy duration_ms", minimum=1, maximum=limits["max_media_duration_ms"]
    )
    frame_rate = video.get("average_frame_rate")
    if frame_rate is not None:
        rate = _object(frame_rate, "preprocess proxy average_frame_rate")
        _exact_keys(rate, "preprocess proxy average_frame_rate", {"text", "numerator", "denominator", "decimal"})
        numerator = _integer(rate["numerator"], "average frame-rate numerator", minimum=1)
        denominator = _integer(rate["denominator"], "average frame-rate denominator", minimum=1)
        decimal = _number(rate["decimal"], "average frame-rate decimal")
        if rate["text"] != f"{numerator}/{denominator}" or not math.isclose(
            decimal, numerator / denominator, rel_tol=0, abs_tol=1e-12
        ):
            raise ResultImportError("preprocess proxy average frame rate is inconsistent")
    return {
        "video_stream_index": video_index,
        "width": width,
        "height": height,
        "pixel_format": video.get("pixel_format"),
        "average_frame_rate": frame_rate,
        "duration_ms": duration_ms,
        "start_ms": format_row.get("start_ms"),
        "video_start_ms": stream.get("start_ms"),
    }


def _uniform_cap(values: list[Any], limit: int) -> tuple[list[Any], bool]:
    if len(values) <= limit:
        return values, False
    if limit == 1:
        return [values[0]], True
    indexes = [index * (len(values) - 1) // (limit - 1) for index in range(limit)]
    return [values[index] for index in indexes], True


def _build_selection(
    *, duration_ms: int, scenes: list[dict[str, Any]], sampling: dict[str, Any]
) -> dict[str, Any]:
    limit_reasons: list[str] = []
    eligible_scenes: list[dict[str, Any]] = []
    if sampling["scene_changes"]["enabled"]:
        offset = sampling["scene_changes"]["offset_ms"]
        for scene in scenes:
            requested = scene["timestamp_ms"] + offset
            if requested < duration_ms:
                eligible_scenes.append({**scene, "requested_timestamp_ms": requested})
    selected_scenes, scene_capped = _uniform_cap(
        eligible_scenes, sampling["scene_changes"]["max_frames"]
    )
    if scene_capped:
        limit_reasons.append("SCENE_CANDIDATES_UNIFORMLY_CAPPED")
    periodic_candidates: list[int] = []
    if sampling["periodic"]["enabled"]:
        periodic_candidates = list(range(sampling["periodic"]["interval_ms"], duration_ms, sampling["periodic"]["interval_ms"]))
    selected_periodic, periodic_capped = _uniform_cap(
        periodic_candidates, sampling["periodic"]["max_frames"]
    )
    if periodic_capped:
        limit_reasons.append("PERIODIC_CANDIDATES_UNIFORMLY_CAPPED")
    accepted: list[dict[str, Any]] = [{
        "requested_timestamp_ms": 0,
        "selection_reason_codes": ["FRAME_RECORDING_START"],
        "source_scene_timestamps_ms": [],
    }]
    merged = 0

    def add(timestamp_ms: int, reason: str, source_scene: int | None = None) -> None:
        nonlocal merged
        nearby = [
            value for value in accepted
            if abs(value["requested_timestamp_ms"] - timestamp_ms) <= sampling["min_separation_ms"]
        ]
        if nearby:
            target = min(nearby, key=lambda value: (
                abs(value["requested_timestamp_ms"] - timestamp_ms), value["requested_timestamp_ms"]
            ))
            if reason not in target["selection_reason_codes"]:
                target["selection_reason_codes"].append(reason)
            if source_scene is not None and source_scene not in target["source_scene_timestamps_ms"]:
                target["source_scene_timestamps_ms"].append(source_scene)
            merged += 1
            return
        accepted.append({
            "requested_timestamp_ms": timestamp_ms,
            "selection_reason_codes": [reason],
            "source_scene_timestamps_ms": [] if source_scene is None else [source_scene],
        })

    for scene in selected_scenes:
        add(scene["requested_timestamp_ms"], "FRAME_SCENE_CHANGE", scene["timestamp_ms"])
    for timestamp in selected_periodic:
        add(timestamp, "FRAME_PERIODIC_COVERAGE")
    if merged:
        limit_reasons.append("NEARBY_CANDIDATES_MERGED")
    rank = {reason: index for index, reason in enumerate(SELECTION_REASON_ORDER)}
    accepted.sort(key=lambda value: value["requested_timestamp_ms"])
    for ordinal, row in enumerate(accepted):
        row["ordinal"] = ordinal
        row["selection_reason_codes"].sort(key=rank.__getitem__)
        row["source_scene_timestamps_ms"].sort()
    return {
        "parameters": sampling,
        "coverage": {"duration_ms": duration_ms, "timeline_origin_ms": 0},
        "candidate_counts": {
            "preprocess_scene_changes": len(scenes),
            "eligible_scene_candidates": len(eligible_scenes),
            "retained_scene_candidates": len(selected_scenes),
            "periodic_candidates": len(periodic_candidates),
            "retained_periodic_candidates": len(selected_periodic),
            "merged_candidates": merged,
            "planned_frames": len(accepted),
        },
        "limit_reason_codes": limit_reasons,
        "planned_frames": accepted,
    }


def _ocr_route(reasons: list[str]) -> dict[str, Any]:
    return {
        "route": "queue_candidate",
        "evaluation_state": "not_evaluated",
        "text_presence": "unknown",
        "reason_codes": [
            OCR_REASON_BY_SELECTION[reason]
            for reason in SELECTION_REASON_ORDER
            if reason in reasons
        ],
        "warning": OCR_WARNING,
    }


def _inspect_png(path: Path, *, maximum_bytes: int) -> dict[str, Any]:
    body = _stable_read(path, "sparse-frame PNG", maximum_bytes=maximum_bytes)
    if len(body) < 33 or body[:8] != b"\x89PNG\r\n\x1a\n":
        raise ResultImportError("sparse-frame artifact is not a PNG")
    offset = 8
    chunks: list[bytes] = []
    ihdr: bytes | None = None
    saw_iend = False
    while offset < len(body):
        if offset + 12 > len(body):
            raise ResultImportError("sparse-frame PNG has a truncated chunk")
        length = int.from_bytes(body[offset : offset + 4], "big")
        kind = body[offset + 4 : offset + 8]
        end = offset + 12 + length
        if end > len(body):
            raise ResultImportError("sparse-frame PNG has a truncated payload")
        payload = body[offset + 8 : offset + 8 + length]
        expected_crc = int.from_bytes(body[offset + 8 + length : end], "big")
        if zlib.crc32(kind + payload) & 0xFFFFFFFF != expected_crc:
            raise ResultImportError("sparse-frame PNG has an invalid chunk checksum")
        chunks.append(kind)
        if len(chunks) == 1:
            if kind != b"IHDR" or length != 13:
                raise ResultImportError("sparse-frame PNG does not start with IHDR")
            ihdr = payload
        if kind == b"IEND":
            if length != 0 or end != len(body):
                raise ResultImportError("sparse-frame PNG has invalid data after IEND")
            saw_iend = True
            break
        offset = end
    if ihdr is None or not saw_iend or b"IDAT" not in chunks:
        raise ResultImportError("sparse-frame PNG lacks required image chunks")
    width, height, bit_depth, color_type, compression, filtering, interlace = struct.unpack(
        ">IIBBBBB", ihdr
    )
    if (
        width < 1 or height < 1 or width > 1280 or height > 1280
        or bit_depth != 8 or color_type != 2 or compression != 0
        or filtering != 0 or interlace != 0
    ):
        raise ResultImportError("sparse-frame PNG is not non-interlaced 8-bit RGB")
    return {
        "width": width,
        "height": height,
        "pixel_format": "rgb24",
        "bit_depth": 8,
        "color_type": "truecolor",
        "interlaced": False,
    }


def _expected_frame_command(
    ffmpeg: Path,
    proxy: Path,
    video_stream_index: int,
    requested_ms: int,
    output: Path,
) -> list[str]:
    return [
        str(ffmpeg), "-hide_banner", "-nostdin", "-nostats", "-loglevel", "info",
        "-threads", "1", "-fflags", "+bitexact", "-copyts", "-ss",
        f"{requested_ms // 1000}.{requested_ms % 1000:03d}", "-i", str(proxy),
        "-map", f"0:{video_stream_index}", "-frames:v", "1", "-an", "-sn", "-dn",
        "-map_metadata", "-1", "-map_chapters", "-1", "-vf", "format=rgb24,showinfo",
        "-fps_mode", "passthrough", "-c:v", "png", "-compression_level", "9",
        "-pred", "mixed", "-flags:v", "+bitexact", "-threads:v", "1", "-f",
        "image2", str(output),
    ]


def _validate_recipe(
    value: object, ffmpeg: dict[str, Any], sampling: dict[str, Any], limits: dict[str, int]
) -> dict[str, Any]:
    recipe = _object(value, "sparse-frame recipe")
    _exact_keys(
        recipe,
        "sparse-frame recipe",
        {"contract_version", "implementation_version", "stage", "sampling", "limits", "decoder", "extraction", "output_contract"},
    )
    decoder = _object(recipe["decoder"], "sparse-frame recipe.decoder")
    extraction = _object(recipe["extraction"], "sparse-frame recipe.extraction")
    expected_decoder = {
        "name": "ffmpeg",
        "executable_sha256": ffmpeg["sha256"],
        "version_output_sha256": ffmpeg["version_output_sha256"],
        "version": ffmpeg["version"],
    }
    expected_extraction = {
        "input": "media_preprocess.low_resolution_cfr_proxy",
        "seek": "input_accurate_copyts",
        "threads": 1,
        "format": "png",
        "pixel_format": "rgb24",
        "compression_level": 9,
        "prediction": "mixed",
        "bitexact_flags": True,
        "metadata_removed": True,
        "timestamp_evidence": "ffmpeg_showinfo_pts_and_time_base",
    }
    contract_version = _integer(
        recipe["contract_version"], "sparse-frame recipe.contract_version", minimum=1, maximum=1
    )
    if (
        contract_version != 1
        or recipe["implementation_version"] != IMPLEMENTATION_VERSION
        or recipe["stage"] != STAGE
        or not _canonical_equal(recipe["sampling"], sampling)
        or not _canonical_equal(recipe["limits"], limits)
        or not _canonical_equal(decoder, expected_decoder)
        or not _canonical_equal(extraction, expected_extraction)
        or recipe["output_contract"] != "sparse-frame-png-ocr-candidate-routing-v1"
    ):
        raise ResultImportError("sparse-frame recipe is unsupported or internally inconsistent")
    return dict(recipe)


def _validate_result(raw: dict[str, Any], result_path: Path, body: bytes) -> dict[str, Any]:
    _exact_keys(
        raw,
        "sparse-frame result",
        {
            "schema_version", "job_id", "status", "dry_run", "work_order_sha256",
            "recipe_id", "recipe_sha256", "result_key", "processing_run",
            "preprocess_result", "input_proxy", "ffmpeg", "selection", "commands",
            "frames", "artifacts", "result_path", "duration_ms", "errors",
        },
    )
    schema_version = _integer(
        raw["schema_version"], "sparse-frame schema_version", minimum=1, maximum=1
    )
    if (
        schema_version != SCHEMA_VERSION
        or raw["status"] != "completed"
        or raw["dry_run"] is not False
        or raw["errors"] != []
        or raw["result_path"] != str(result_path)
    ):
        raise ResultImportError("sparse-frame result is not a completed immutable version-1 result")
    _identifier(raw["job_id"], "sparse-frame job_id")
    work_order_sha = _sha256(raw["work_order_sha256"], "sparse-frame work_order_sha256")
    recipe_sha = _sha256(raw["recipe_sha256"], "sparse-frame recipe_sha256")
    result_key = _sha256(raw["result_key"], "sparse-frame result_key")
    _integer(raw["duration_ms"], "sparse-frame duration_ms")

    preprocess = _object(raw["preprocess_result"], "sparse-frame preprocess_result")
    _exact_keys(
        preprocess,
        "sparse-frame preprocess_result",
        {"path", "sha256", "byte_count", "stat_before", "stat_after", "unchanged", "processing_run_id", "implementation_version"},
    )
    preprocess_base = {key: preprocess[key] for key in ("path", "sha256", "byte_count", "stat_before", "stat_after", "unchanged")}
    _, preprocess_path = _file_observation(preprocess_base, "sparse-frame preprocess_result", sealed=True, maximum_bytes=MAX_RESULT_BYTES)
    preprocess_body = _stable_read(preprocess_path, "sparse-frame preprocess_result", maximum_bytes=MAX_RESULT_BYTES)
    preprocess_digest = sha256_bytes(preprocess_body)
    if preprocess_digest != preprocess["sha256"] or len(preprocess_body) != preprocess["byte_count"]:
        raise ResultImportError("sparse-frame preprocess result changed after verification")
    preprocess_envelope = validate_preprocess_result(
        _load_json_bytes(preprocess_body, "sparse-frame referenced preprocess result")
    )
    if (
        preprocess_envelope["result_path"] != str(preprocess_path)
        or preprocess_envelope["processing_run"]["processing_run_id"] != preprocess["processing_run_id"]
        or preprocess_envelope["processing_run"]["implementation_version"] != preprocess["implementation_version"]
    ):
        raise ResultImportError("sparse-frame preprocess observation disagrees with its envelope")

    proxy = _object(raw["input_proxy"], "sparse-frame input_proxy")
    _exact_keys(
        proxy,
        "sparse-frame input_proxy",
        {"path", "sha256", "byte_count", "stat_before", "stat_after", "unchanged", "media_id", "artifact_id", "parent_processing_run_id", "probe"},
    )
    proxy_base = {key: proxy[key] for key in ("path", "sha256", "byte_count", "stat_before", "stat_after", "unchanged")}
    _, proxy_path = _file_observation(proxy_base, "sparse-frame input_proxy", sealed=True)
    proxy_digest = _sha256(proxy["sha256"], "sparse-frame input_proxy.sha256")
    proxy_bytes = _integer(proxy["byte_count"], "sparse-frame input_proxy.byte_count", minimum=1)
    if proxy["media_id"] != f"media_sha256_{proxy_digest}":
        raise ResultImportError("sparse-frame proxy media ID is not byte-derived")
    _identifier(proxy["artifact_id"], "sparse-frame proxy artifact_id")
    if proxy["parent_processing_run_id"] != preprocess["processing_run_id"]:
        raise ResultImportError("sparse-frame proxy and preprocess run lineage disagree")
    proxy_artifacts = [
        artifact for artifact in preprocess_envelope["artifacts"]
        if artifact["artifact_kind"] == "low_resolution_cfr_proxy"
    ]
    if len(proxy_artifacts) != 1:
        raise ResultImportError("referenced preprocess result lacks one CFR proxy")
    proxy_artifact = proxy_artifacts[0]
    if any(
        proxy_artifact[key] != proxy[key]
        for key in ("path", "sha256", "byte_count", "artifact_id", "processing_run_id")
        if key in proxy_artifact and key in proxy
    ) or proxy_artifact["processing_run_id"] != proxy["parent_processing_run_id"]:
        raise ResultImportError("sparse-frame proxy observation differs from the preprocess artifact")
    if proxy_artifact["storage_uri"] != proxy_path.as_uri():
        raise ResultImportError("sparse-frame proxy path/URI lineage disagrees")

    ffmpeg = _object(raw["ffmpeg"], "sparse-frame ffmpeg")
    _exact_keys(
        ffmpeg,
        "sparse-frame ffmpeg",
        {"path", "sha256", "byte_count", "stat_before", "stat_after", "unchanged", "version", "version_output", "version_output_sha256"},
    )
    ffmpeg_base = {key: ffmpeg[key] for key in ("path", "sha256", "byte_count", "stat_before", "stat_after", "unchanged")}
    _, ffmpeg_path = _file_observation(ffmpeg_base, "sparse-frame ffmpeg", sealed=False)
    version = _string(ffmpeg["version"], "sparse-frame ffmpeg.version")
    version_output = _string(ffmpeg["version_output"], "sparse-frame ffmpeg.version_output")
    version_sha = _sha256(ffmpeg["version_output_sha256"], "sparse-frame ffmpeg.version_output_sha256")
    if version_output.splitlines()[0].strip() != version or sha256_bytes(version_output.encode("utf-8")) != version_sha:
        raise ResultImportError("sparse-frame FFmpeg version provenance is inconsistent")

    processing = _object(raw["processing_run"], "sparse-frame processing_run")
    _exact_keys(
        processing,
        "sparse-frame processing_run",
        {"processing_run_id", "stage", "implementation_version", "parameters_json", "environment_json", "started_at", "completed_at", "status", "error_text"},
    )
    started = _timestamp(processing["started_at"], "sparse-frame started_at")
    completed = _timestamp(processing["completed_at"], "sparse-frame completed_at")
    if (
        processing["stage"] != STAGE
        or processing["implementation_version"] != IMPLEMENTATION_VERSION
        or processing["status"] != "completed"
        or processing["error_text"] is not None
        or _timestamp_value(completed) < _timestamp_value(started)
    ):
        raise ResultImportError("sparse-frame processing run is unsupported or incomplete")
    environment = _object(processing["environment_json"], "sparse-frame environment_json")
    _exact_keys(environment, "sparse-frame environment_json", {"python", "cpu_only", "network", "tool_paths"})
    tool_paths = _object(environment["tool_paths"], "sparse-frame tool_paths")
    _exact_keys(tool_paths, "sparse-frame tool_paths", {"ffmpeg"})
    if (
        not _string(environment["python"], "sparse-frame Python version")
        or environment["cpu_only"] is not True
        or environment["network"] != "not_used"
        or tool_paths["ffmpeg"] != str(ffmpeg_path)
    ):
        raise ResultImportError("sparse-frame runtime environment is inconsistent")
    parameters_text, recipe_value = _parse_canonical_json_string(
        processing["parameters_json"], "sparse-frame processing_run.parameters_json"
    )
    recipe_untrusted = _object(recipe_value, "sparse-frame recipe")
    sampling = _sampling(recipe_untrusted.get("sampling"))
    limits = _limits(recipe_untrusted.get("limits"), sampling)
    recipe = _validate_recipe(recipe_untrusted, ffmpeg, sampling, limits)
    if sha256_bytes(_canonical_bytes(recipe)) != recipe_sha:
        raise ResultImportError("sparse-frame recipe digest is inconsistent")
    recipe_id = f"recipe_sparse_frames_{recipe_sha[:32]}"
    if raw["recipe_id"] != recipe_id:
        raise ResultImportError("sparse-frame recipe ID is inconsistent")

    probe = _proxy_probe_from_preprocess(proxy_artifact, proxy_digest, proxy_bytes, limits)
    if not _canonical_equal(proxy["probe"], probe):
        raise ResultImportError("sparse-frame proxy probe differs from the preprocess probe")
    routing = _object(preprocess_envelope.get("routing"), "preprocess routing")
    coverage = _object(routing.get("coverage"), "preprocess routing coverage")
    # Preprocessing routes the source timeline while extraction consumes the CFR
    # proxy. Container rounding can make those durations differ by a frame, so the
    # proxy probe is authoritative for frame bounds.  The source-routing duration is
    # still type/bound checked and remains sealed in the preprocess envelope.
    _integer(
        coverage.get("duration_ms"),
        "preprocess routing coverage.duration_ms",
        minimum=1,
        maximum=MAX_DURATION_MS,
    )
    if coverage.get("has_video") is not True:
        raise ResultImportError("preprocess routing has no video coverage")
    scenes_raw = _array(routing.get("scene_changes"), "preprocess scene changes")
    scenes: list[dict[str, Any]] = []
    for index, value in enumerate(scenes_raw):
        scene = _object(value, f"preprocess scene_changes[{index}]")
        _exact_keys(scene, f"preprocess scene_changes[{index}]", {"timestamp_ms", "score_percent"})
        scenes.append({
            "timestamp_ms": _integer(scene["timestamp_ms"], f"scene[{index}].timestamp_ms", maximum=MAX_DURATION_MS),
            "score_percent": _number(scene["score_percent"], f"scene[{index}].score_percent", minimum=0, maximum=100),
        })
    scenes.sort(key=lambda value: (value["timestamp_ms"], value["score_percent"]))
    selection = _build_selection(duration_ms=probe["duration_ms"], scenes=scenes, sampling=sampling)
    if not _canonical_equal(raw["selection"], selection) or len(selection["planned_frames"]) > limits["max_frames"]:
        raise ResultImportError("sparse-frame selected frame plan is inconsistent")

    identity = {
        "work_order_sha256": work_order_sha,
        "preprocess_result_sha256": preprocess_digest,
        "proxy_sha256": proxy_digest,
        "proxy_artifact_id": proxy["artifact_id"],
        "parent_processing_run_id": proxy["parent_processing_run_id"],
        "recipe_id": recipe_id,
        "selection": selection,
    }
    if sha256_bytes(_canonical_bytes(identity)) != result_key:
        raise ResultImportError("sparse-frame result key is inconsistent")
    run_id = f"run_sparse_frames_{result_key[:32]}"
    if processing["processing_run_id"] != run_id:
        raise ResultImportError("sparse-frame processing-run ID is inconsistent")

    run_dir = result_path.parent
    suffix = ("vision", "sparse-frames", "sha256", proxy_digest[:2], proxy_digest, "results", result_key)
    if tuple(run_dir.parts[-len(suffix):]) != suffix:
        raise ResultImportError("sparse-frame result path leaves its content-addressed tree")
    _sealed_directory(run_dir, "sparse-frame result directory")
    _sealed_directory(run_dir / "frames", "sparse-frame frames directory")

    frames = _array(raw["frames"], "sparse-frame frames")
    artifacts = _array(raw["artifacts"], "sparse-frame artifacts")
    commands = _array(raw["commands"], "sparse-frame commands")
    planned = selection["planned_frames"]
    if not planned or not (len(frames) == len(artifacts) == len(commands) == len(planned)):
        raise ResultImportError("sparse-frame result counts do not match the selected plan")
    normalized_artifacts: list[dict[str, Any]] = []
    artifact_by_id: dict[str, dict[str, Any]] = {}
    for ordinal, value in enumerate(artifacts):
        artifact = _object(value, f"sparse-frame artifacts[{ordinal}]")
        _exact_keys(
            artifact,
            f"sparse-frame artifacts[{ordinal}]",
            {"artifact_id", "processing_run_id", "artifact_kind", "ordinal", "storage_uri", "path", "sha256", "byte_count", "schema_version", "visibility", "media_kind", "mime_type", "image"},
        )
        requested = planned[ordinal]["requested_timestamp_ms"]
        expected_name = f"frame-{ordinal:04d}-{requested:012d}.png"
        path = _sealed_file(artifact["path"], f"sparse-frame artifact[{ordinal}].path")
        uri_path = _local_file_uri(artifact["storage_uri"], f"sparse-frame artifact[{ordinal}].storage_uri")
        digest = _sha256(artifact["sha256"], f"sparse-frame artifact[{ordinal}].sha256")
        byte_count = _integer(
            artifact["byte_count"], f"sparse-frame artifact[{ordinal}].byte_count", minimum=1, maximum=limits["max_frame_bytes"]
        )
        if (
            path != uri_path or path.parent != run_dir / "frames" or path.name != expected_name
            or artifact["processing_run_id"] != run_id or artifact["artifact_kind"] != "sparse_frame_png"
            or _integer(artifact["ordinal"], f"sparse-frame artifact[{ordinal}].ordinal", minimum=0, maximum=255) != ordinal
            or _integer(artifact["schema_version"], f"sparse-frame artifact[{ordinal}].schema_version", minimum=1, maximum=1) != 1
            or artifact["visibility"] != "private" or artifact["media_kind"] != "image"
            or artifact["mime_type"] != "image/png"
        ):
            raise ResultImportError("sparse-frame artifact path or private metadata is inconsistent")
        _verify_hash(path, digest, byte_count, f"sparse-frame artifact[{ordinal}]")
        image = _inspect_png(path, maximum_bytes=limits["max_frame_bytes"])
        if not _canonical_equal(artifact["image"], image) or image["width"] != probe["width"] or image["height"] != probe["height"]:
            raise ResultImportError("sparse-frame PNG metadata/dimensions disagree")
        expected_artifact_id = _producer_id("artifact", run_id, "sparse_frame_png", ordinal, digest)
        if artifact["artifact_id"] != expected_artifact_id or expected_artifact_id in artifact_by_id:
            raise ResultImportError("sparse-frame artifact identity is inconsistent or duplicated")
        normalized = {**artifact, "_path": path}
        normalized_artifacts.append(normalized)
        artifact_by_id[expected_artifact_id] = normalized

    normalized_frames: list[dict[str, Any]] = []
    for ordinal, value in enumerate(frames):
        frame = _object(value, f"sparse-frame frames[{ordinal}]")
        _exact_keys(
            frame,
            f"sparse-frame frames[{ordinal}]",
            {"frame_id", "ordinal", "requested_timestamp_ms", "selection_reason_codes", "source_scene_timestamps_ms", "timestamp", "timestamp_drift_ms", "artifact_id", "ocr_routing"},
        )
        plan = planned[ordinal]
        if not _canonical_equal({key: frame.get(key) for key in plan}, plan):
            raise ResultImportError("sparse-frame frame does not mirror the selected plan")
        frame_artifact_id = _identifier(
            frame["artifact_id"], f"sparse-frame frames[{ordinal}].artifact_id"
        )
        artifact = artifact_by_id.get(frame_artifact_id)
        if artifact is None or artifact["ordinal"] != ordinal:
            raise ResultImportError("sparse-frame frame points to the wrong artifact")
        timestamp = _object(frame["timestamp"], f"sparse-frame frames[{ordinal}].timestamp")
        _exact_keys(
            timestamp,
            f"sparse-frame frames[{ordinal}].timestamp",
            {"pts", "duration_pts", "time_base_numerator", "time_base_denominator", "timestamp_us", "timestamp_ms", "duration_us"},
        )
        pts = _integer(timestamp["pts"], "frame PTS", minimum=0)
        duration_pts = _integer(timestamp["duration_pts"], "frame duration PTS", minimum=1)
        numerator = _integer(timestamp["time_base_numerator"], "frame time-base numerator", minimum=1)
        denominator = _integer(timestamp["time_base_denominator"], "frame time-base denominator", minimum=1)
        exact_timestamp = Fraction(pts * numerator, denominator)
        exact_duration = Fraction(duration_pts * numerator, denominator)
        expected_time = {
            "timestamp_us": round(exact_timestamp * 1_000_000),
            "timestamp_ms": round(exact_timestamp * 1_000),
            "duration_us": round(exact_duration * 1_000_000),
        }
        for key, expected in expected_time.items():
            _integer(timestamp[key], f"frame {key}", minimum=0 if key != "duration_us" else 1)
        if any(timestamp[key] != expected for key, expected in expected_time.items()):
            raise ResultImportError("sparse-frame rounded timestamp fields are inconsistent")
        if (
            not (0 <= timestamp["timestamp_us"] < probe["duration_ms"] * 1_000)
            or not (0 <= timestamp["timestamp_ms"] < probe["duration_ms"])
            or timestamp["duration_us"] < 1
        ):
            raise ResultImportError("sparse-frame timestamp/duration leaves proxy coverage")
        frame_rate = probe["average_frame_rate"]
        if frame_rate is not None:
            expected_duration = Fraction(
                frame_rate["denominator"], frame_rate["numerator"]
            )
            if exact_duration != expected_duration:
                raise ResultImportError(
                    "sparse-frame duration disagrees with the CFR proxy frame rate"
                )
        drift = abs(timestamp["timestamp_ms"] - plan["requested_timestamp_ms"])
        observed_drift = _integer(
            frame["timestamp_drift_ms"], "frame timestamp_drift_ms", minimum=0, maximum=10_000
        )
        if observed_drift != drift or drift > limits["max_timestamp_drift_ms"]:
            raise ResultImportError("sparse-frame timestamp drift is inconsistent or excessive")
        if not _canonical_equal(frame["ocr_routing"], _ocr_route(plan["selection_reason_codes"])):
            raise ResultImportError("sparse-frame OCR candidate route makes an unsupported claim")
        expected_frame_id = _producer_id(
            "frame", result_key, ordinal, plan["requested_timestamp_ms"], pts,
            numerator, denominator, artifact["sha256"],
        )
        if frame["frame_id"] != expected_frame_id:
            raise ResultImportError("sparse-frame frame ID is inconsistent")
        normalized_frames.append(dict(frame))

        command = commands[ordinal]
        if not isinstance(command, list) or not command or any(not isinstance(arg, str) or not arg for arg in command):
            raise ResultImportError("sparse-frame command provenance is malformed")
        command_output = Path(command[-1])
        staging_parent = command_output.parent.parent
        prefix = f".{result_key}.tmp-"
        nonce = staging_parent.name.removeprefix(prefix)
        if (
            not command_output.is_absolute() or command_output.name != artifact["_path"].name
            or command_output.parent.name != "frames" or staging_parent.parent != run_dir.parent
            or not staging_parent.name.startswith(prefix) or len(nonce) != 32
            or any(character not in "0123456789abcdef" for character in nonce)
            or command != _expected_frame_command(
                ffmpeg_path, proxy_path, probe["video_stream_index"], plan["requested_timestamp_ms"], command_output
            )
        ):
            raise ResultImportError("sparse-frame FFmpeg command provenance is inconsistent")

    normalized = dict(raw)
    normalized["processing_run"] = {**processing, "started_at": started, "completed_at": completed, "parameters_json": parameters_text}
    normalized["preprocess_result"] = dict(preprocess)
    normalized["input_proxy"] = {**proxy, "_path": proxy_path}
    normalized["ffmpeg"] = {**ffmpeg, "_path": ffmpeg_path}
    normalized["frames"] = normalized_frames
    normalized["artifacts"] = normalized_artifacts
    normalized["_path"] = result_path
    normalized["_body"] = body
    normalized["_sha256"] = sha256_bytes(body)
    normalized["_preprocess_path"] = preprocess_path
    normalized["_preprocess_envelope"] = preprocess_envelope
    return normalized


def _read_result(result_path: str | Path) -> dict[str, Any]:
    path = Path(result_path)
    if not path.is_absolute():
        path = path.resolve()
    observed = _sealed_file(str(path), "sparse-frame result")
    body = _stable_read(observed, "sparse-frame result", maximum_bytes=MAX_RESULT_BYTES)
    return _validate_result(_load_json_bytes(body, "sparse-frame result"), observed, body)


def _reverify_files(result: dict[str, Any]) -> None:
    result_path: Path = result["_path"]
    body = _stable_read(result_path, "sparse-frame result", maximum_bytes=MAX_RESULT_BYTES)
    if body != result["_body"] or sha256_bytes(body) != result["_sha256"]:
        raise ResultImportError("sparse-frame result changed before transaction admission")
    for label, path, digest, byte_count, sealed in (
        (
            "sparse-frame preprocess result", result["_preprocess_path"],
            result["preprocess_result"]["sha256"], result["preprocess_result"]["byte_count"], True,
        ),
        (
            "sparse-frame input proxy", result["input_proxy"]["_path"],
            result["input_proxy"]["sha256"], result["input_proxy"]["byte_count"], True,
        ),
        (
            "sparse-frame FFmpeg", result["ffmpeg"]["_path"],
            result["ffmpeg"]["sha256"], result["ffmpeg"]["byte_count"], False,
        ),
    ):
        if sealed and path.lstat().st_mode & 0o222:
            raise ResultImportError(f"{label} is no longer sealed")
        _verify_hash(path, digest, byte_count, label)
    for index, artifact in enumerate(result["artifacts"]):
        path = artifact["_path"]
        if path.lstat().st_mode & 0o222:
            raise ResultImportError(f"sparse-frame artifact[{index}] is no longer sealed")
        _verify_hash(path, artifact["sha256"], artifact["byte_count"], f"sparse-frame artifact[{index}]")


def _require_catalog_lineage(connection, result: dict[str, Any]) -> list[dict[str, str]]:
    preprocess = result["_preprocess_envelope"]
    parent_run = preprocess["processing_run"]
    run_id = parent_run["processing_run_id"]
    expected_run = {
        "stage": parent_run["stage"],
        "implementation_version": parent_run["implementation_version"],
        "model_id": None,
        "glossary_revision_id": None,
        "parameters_json": canonical_json(parent_run.get("parameters_json") or {}),
        "environment_json": canonical_json(parent_run.get("environment_json") or {}),
        "random_seed": None,
        "started_at": _timestamp(parent_run["started_at"], "preprocess started_at"),
        "completed_at": _timestamp(parent_run["completed_at"], "preprocess completed_at"),
        "status": "completed",
        "error_text": None,
    }
    row = connection.execute(
        f"SELECT {', '.join(expected_run)} FROM processing_runs WHERE processing_run_id = ?", (run_id,)
    ).fetchone()
    if row is None or any(row[key] != expected for key, expected in expected_run.items()):
        raise ResultImportError("sparse-frame parent preprocess run is missing or differs")

    proxy = result["input_proxy"]
    artifact_expected = {
        "processing_run_id": run_id,
        "artifact_kind": "low_resolution_cfr_proxy",
        "storage_uri": proxy["_path"].as_uri(),
        "sha256": proxy["sha256"],
        "byte_count": proxy["byte_count"],
        "schema_version": 1,
        "visibility": "private",
    }
    artifact_row = connection.execute(
        f"SELECT {', '.join(artifact_expected)}, metadata_json FROM artifacts WHERE artifact_id = ?",
        (proxy["artifact_id"],),
    ).fetchone()
    if artifact_row is None or any(artifact_row[key] != expected for key, expected in artifact_expected.items()):
        raise ResultImportError("sparse-frame proxy artifact is absent or differs in the catalog")
    descriptor = next(
        item for item in preprocess["artifacts"] if item["artifact_kind"] == "low_resolution_cfr_proxy"
    )
    expected_artifact_metadata = canonical_json({
        "media_kind": descriptor["media_kind"],
        "mime_type": descriptor["mime_type"],
        "normalized_probe": descriptor["normalized_probe"],
    })
    if artifact_row["metadata_json"] != expected_artifact_metadata:
        raise ResultImportError("sparse-frame proxy artifact metadata lineage differs")

    media = connection.execute(
        "SELECT sha256, byte_count, media_kind, mime_type, duration_ms, ffprobe_json, integrity_state FROM media_objects WHERE media_id = ?",
        (proxy["media_id"],),
    ).fetchone()
    if (
        media is None or media["sha256"] != proxy["sha256"] or media["byte_count"] != proxy["byte_count"]
        or media["media_kind"] != "video" or media["mime_type"] != "video/mp4"
        or media["duration_ms"] != proxy["probe"]["duration_ms"] or media["integrity_state"] != "verified"
        or media["ffprobe_json"] != canonical_json(descriptor["normalized_probe"])
    ):
        raise ResultImportError("sparse-frame proxy media object is missing or differs")
    location = connection.execute(
        "SELECT 1 FROM media_locations WHERE media_id = ? AND storage_uri = ?",
        (proxy["media_id"], proxy["_path"].as_uri()),
    ).fetchone()
    if location is None:
        raise ResultImportError("sparse-frame proxy media location is not cataloged")

    derivations = [
        item for item in preprocess["catalog_records"]["media_derivations"]
        if item["child_media_id"] == proxy["media_id"] and item["derivation_kind"] == "low_resolution_cfr_proxy"
    ]
    if len(derivations) != 1:
        raise ResultImportError("preprocess envelope has no unique proxy media derivation")
    derivation = derivations[0]
    catalog_derivation = connection.execute(
        "SELECT processing_run_id, metadata_json FROM media_derivations WHERE child_media_id = ? AND parent_media_id = ? AND derivation_kind = ?",
        (derivation["child_media_id"], derivation["parent_media_id"], derivation["derivation_kind"]),
    ).fetchone()
    if (
        catalog_derivation is None or catalog_derivation["processing_run_id"] != run_id
        or catalog_derivation["metadata_json"] != canonical_json(derivation.get("metadata_json") or {})
    ):
        raise ResultImportError("sparse-frame proxy derivation is missing or differs")

    parent_input = connection.execute(
        "SELECT input_sha256 FROM run_inputs WHERE processing_run_id = ? AND object_type = 'media' AND object_id = ? AND input_role = 'source_media'",
        (run_id, derivation["parent_media_id"]),
    ).fetchone()
    parent_media = connection.execute(
        "SELECT sha256 FROM media_objects WHERE media_id = ?", (derivation["parent_media_id"],)
    ).fetchone()
    if parent_input is None or parent_media is None or parent_input["input_sha256"] != parent_media["sha256"]:
        raise ResultImportError("sparse-frame parent media/run-input lineage is incomplete")

    rendition_rows = connection.execute(
        "SELECT rendition_id, recording_id, metadata_json FROM renditions WHERE media_id = ? AND rendition_kind = 'low_resolution_cfr_proxy' AND review_state <> 'rejected' ORDER BY rendition_id",
        (proxy["media_id"],),
    ).fetchall()
    if not rendition_rows:
        raise ResultImportError("sparse-frame proxy has no eligible catalog rendition")
    normalized: list[dict[str, str]] = []
    for rendition in rendition_rows:
        expected_id = stable_id(
            "rnd", rendition["recording_id"], proxy["media_id"], "low_resolution_cfr_proxy"
        )
        try:
            metadata = _object(json.loads(rendition["metadata_json"]), "proxy rendition metadata")
        except json.JSONDecodeError as error:
            raise ResultImportError("proxy rendition metadata is invalid JSON") from error
        parent_rendition_id = metadata.get("derived_from_rendition_id")
        source_rendition = connection.execute(
            "SELECT recording_id, media_id, review_state FROM renditions WHERE rendition_id = ?",
            (parent_rendition_id,),
        ).fetchone()
        if (
            rendition["rendition_id"] != expected_id or metadata.get("publication_state") != "withheld_by_default"
            or source_rendition is None or source_rendition["recording_id"] != rendition["recording_id"]
            or source_rendition["media_id"] != derivation["parent_media_id"]
            or source_rendition["review_state"] == "rejected"
        ):
            raise ResultImportError("sparse-frame proxy rendition/source-rendition lineage differs")
        normalized.append({"rendition_id": rendition["rendition_id"], "recording_id": rendition["recording_id"]})
    return normalized


def _insert_processing_run(connection, result: dict[str, Any]) -> None:
    run = result["processing_run"]
    expected = {
        "stage": STAGE,
        "implementation_version": IMPLEMENTATION_VERSION,
        "model_id": None,
        "glossary_revision_id": None,
        "parameters_json": run["parameters_json"],
        "environment_json": canonical_json(run["environment_json"]),
        "random_seed": None,
        "started_at": run["started_at"],
        "completed_at": run["completed_at"],
        "status": "completed",
        "error_text": None,
    }
    run_id = run["processing_run_id"]
    existing = connection.execute(
        f"SELECT {', '.join(expected)} FROM processing_runs WHERE processing_run_id = ?", (run_id,)
    ).fetchone()
    if existing is not None:
        if any(existing[key] != value for key, value in expected.items()):
            raise ResultImportError("sparse-frame processing-run ID already has different data")
        return
    connection.execute(
        "INSERT INTO processing_runs(processing_run_id, stage, implementation_version, model_id, glossary_revision_id, parameters_json, environment_json, random_seed, started_at, completed_at, status, error_text) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (run_id, *expected.values()),
    )


def _insert_run_input(
    connection, *, run_id: str, object_type: str, object_id: str, role: str, digest: str
) -> None:
    row_id = stable_id("rin", run_id, object_type, object_id, role)
    expected = (run_id, object_type, object_id, role, digest)
    existing = connection.execute(
        "SELECT processing_run_id, object_type, object_id, input_role, input_sha256 FROM run_inputs WHERE run_input_id = ?",
        (row_id,),
    ).fetchone()
    if existing is not None:
        if tuple(existing) != expected:
            raise ResultImportError("sparse-frame run-input ID already has different data")
        return
    collision = connection.execute(
        "SELECT run_input_id FROM run_inputs WHERE processing_run_id = ? AND object_type = ? AND object_id = ? AND input_role = ?",
        expected[:4],
    ).fetchone()
    if collision is not None:
        raise ResultImportError("sparse-frame logical run input already has a different ID")
    connection.execute(
        "INSERT INTO run_inputs(run_input_id, processing_run_id, object_type, object_id, input_role, input_sha256) VALUES(?, ?, ?, ?, ?, ?)",
        (row_id, *expected),
    )


def _artifact_metadata(result: dict[str, Any], ordinal: int) -> str:
    frame = result["frames"][ordinal]
    artifact = result["artifacts"][ordinal]
    return canonical_json({
        "frame_id": frame["frame_id"],
        "image": artifact["image"],
        "ordinal": ordinal,
        "requested_timestamp_ms": frame["requested_timestamp_ms"],
        "routing_only": True,
    })


def _insert_artifact(connection, result: dict[str, Any], ordinal: int) -> None:
    artifact = result["artifacts"][ordinal]
    expected = {
        "processing_run_id": result["processing_run"]["processing_run_id"],
        "artifact_kind": "sparse_frame_png",
        "storage_uri": artifact["storage_uri"],
        "sha256": artifact["sha256"],
        "byte_count": artifact["byte_count"],
        "schema_version": 1,
        "visibility": "private",
        "metadata_json": _artifact_metadata(result, ordinal),
    }
    existing = connection.execute(
        f"SELECT {', '.join(expected)} FROM artifacts WHERE artifact_id = ?", (artifact["artifact_id"],)
    ).fetchone()
    if existing is not None:
        if any(existing[key] != value for key, value in expected.items()):
            raise ResultImportError("sparse-frame artifact ID already has different catalog data")
        return
    collision = connection.execute(
        "SELECT artifact_id FROM artifacts WHERE storage_uri = ? AND sha256 = ?",
        (artifact["storage_uri"], artifact["sha256"]),
    ).fetchone()
    if collision is not None:
        raise ResultImportError("sparse-frame artifact URI/digest already has a different ID")
    connection.execute(
        "INSERT INTO artifacts(artifact_id, processing_run_id, artifact_kind, storage_uri, sha256, byte_count, schema_version, visibility, metadata_json) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (artifact["artifact_id"], *expected.values()),
    )


def _observation_interval(frame: dict[str, Any], duration_ms: int) -> tuple[int, int]:
    timestamp = frame["timestamp"]
    start = timestamp["timestamp_ms"]
    exact_end = (timestamp["timestamp_us"] + timestamp["duration_us"] + 999) // 1_000
    end = min(duration_ms, max(start + 1, exact_end))
    if not 0 <= start < end <= duration_ms:
        raise ResultImportError("sparse-frame observation interval is outside proxy duration")
    return start, end


def _observation_metadata(frame: dict[str, Any]) -> str:
    return canonical_json({
        "artifact_id": frame["artifact_id"],
        "coordinate_space": "rendition_media",
        "frame_id": frame["frame_id"],
        "ocr_routing": frame["ocr_routing"],
        "requested_timestamp_ms": frame["requested_timestamp_ms"],
        "requires_downstream_evaluation": True,
        "routing_only": True,
        "selection_reason_codes": frame["selection_reason_codes"],
        "source_scene_timestamps_ms": frame["source_scene_timestamps_ms"],
        "timestamp": frame["timestamp"],
        "timestamp_drift_ms": frame["timestamp_drift_ms"],
    })


def _insert_observation(
    connection, result: dict[str, Any], rendition: dict[str, str], frame: dict[str, Any]
) -> None:
    run = result["processing_run"]
    observation_id = stable_id(
        "obs", run["processing_run_id"], rendition["rendition_id"],
        frame["frame_id"], "sparse_frame_routing_candidate",
    )
    start_ms, end_ms = _observation_interval(
        frame, result["input_proxy"]["probe"]["duration_ms"]
    )
    expected = {
        "observation_kind": "sparse_frame_routing_candidate",
        "recording_id": rendition["recording_id"],
        "rendition_id": rendition["rendition_id"],
        "processing_run_id": run["processing_run_id"],
        "start_ms": start_ms,
        "end_ms": end_ms,
        "visibility": "private",
        "review_state": "machine",
        "payload_schema_version": 1,
        "metadata_json": _observation_metadata(frame),
        "created_at": run["completed_at"],
    }
    existing = connection.execute(
        f"SELECT {', '.join(expected)} FROM observations WHERE observation_id = ?",
        (observation_id,),
    ).fetchone()
    if existing is not None:
        if any(existing[key] != value for key, value in expected.items()):
            raise ResultImportError("sparse-frame observation ID already has different data")
        return
    connection.execute(
        f"INSERT INTO observations(observation_id, {', '.join(expected)}) VALUES(?, {', '.join('?' for _ in expected)})",
        (observation_id, *expected.values()),
    )


def validate_sparse_frame_result_file(result_path: str | Path) -> dict[str, Any]:
    """Validate the envelope and every current local byte without opening SQLite."""

    result = _read_result(result_path)
    return {
        "valid": True,
        "result_sha256": result["_sha256"],
        "result_key": result["result_key"],
        "recipe_id": result["recipe_id"],
        "processing_run_id": result["processing_run"]["processing_run_id"],
        "parent_processing_run_id": result["input_proxy"]["parent_processing_run_id"],
        "proxy_media_id": result["input_proxy"]["media_id"],
        "proxy_artifact_id": result["input_proxy"]["artifact_id"],
        "frame_count": len(result["frames"]),
        "routing_semantics": "candidate_only_not_evaluated",
        "publication_decisions": 0,
    }


def import_sparse_frame_result(connection, result_path: str | Path) -> dict[str, Any]:
    """Transactionally admit one completed result as private routing evidence."""

    result = _read_result(result_path)
    run = result["processing_run"]
    with transaction(connection):
        _reverify_files(result)
        renditions = _require_catalog_lineage(connection, result)
        batch_id = _begin_result_batch(
            connection,
            importer_name="sparse_frame_result_v1",
            digest=result["_sha256"],
            started_at=run["started_at"],
        )
        _insert_processing_run(connection, result)
        _insert_run_input(
            connection,
            run_id=run["processing_run_id"],
            object_type="processing_run",
            object_id=result["input_proxy"]["parent_processing_run_id"],
            role="preprocess_result",
            digest=result["preprocess_result"]["sha256"],
        )
        _insert_run_input(
            connection,
            run_id=run["processing_run_id"],
            object_type="media",
            object_id=result["input_proxy"]["media_id"],
            role="low_resolution_cfr_proxy",
            digest=result["input_proxy"]["sha256"],
        )
        _insert_run_input(
            connection,
            run_id=run["processing_run_id"],
            object_type="artifact",
            object_id=result["input_proxy"]["artifact_id"],
            role="low_resolution_cfr_proxy_artifact",
            digest=result["input_proxy"]["sha256"],
        )
        for ordinal in range(len(result["artifacts"])):
            _insert_artifact(connection, result, ordinal)
        for rendition in renditions:
            for frame in result["frames"]:
                _insert_observation(connection, result, rendition, frame)
        job_id, attempt_id = _upsert_job_and_attempt(
            connection,
            stage=STAGE,
            target_type="media",
            target_id=result["input_proxy"]["media_id"],
            run_id=run["processing_run_id"],
            producer_job_id=result["job_id"],
            started_at=run["started_at"],
            completed_at=run["completed_at"],
        )
        statistics = {
            "artifacts": len(result["artifacts"]),
            "jobs": 1,
            "observations": len(result["frames"]) * len(renditions),
            "processing_runs": 1,
            "publication_decisions_added": 0,
            "renditions": len(renditions),
            "run_inputs": 3,
        }
        _complete_result_batch(connection, batch_id, run["completed_at"], statistics)
    return {
        "importer_version": __version__,
        "import_batch_id": batch_id,
        "job_id": job_id,
        "job_attempt_id": attempt_id,
        "processing_run_id": run["processing_run_id"],
        "parent_processing_run_id": result["input_proxy"]["parent_processing_run_id"],
        "proxy_media_id": result["input_proxy"]["media_id"],
        "result_key": result["result_key"],
        **statistics,
    }
