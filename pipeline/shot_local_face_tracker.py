#!/usr/bin/env python3
"""Deterministic, offline, shot-local face tracking without identity inference.

The adapter consumes a sealed JSON artifact of frame-local face detections.  It uses
only box geometry: constant-velocity prediction followed by deterministic Hungarian
IoU assignment.  Tracks are scoped to exactly one declared shot.  It does not load
media, models, crops, embeddings, a database, or the network, and it has no identity,
active-speaker, or publication authority.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import stat
import sys
import uuid
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.2.0"
STAGE = "shot_local_face_tracker"
MAX_JSON_BYTES = 64 * 1024 * 1024
MAX_RESULT_BYTES = 256 * 1024 * 1024
MAX_FRAMES = 100_000
MAX_DETECTIONS_PER_FRAME = 64
MAX_ACTIVE_TRACKS = 128
MAX_SHOTS = 50_000
MAX_FRAME_INDEX = 100_000_000
MAX_TIMESTAMP_MS = 14 * 24 * 60 * 60 * 1000
SCORE_SCALE = 100_000_000
MIN_PREDICTED_EXTENT = 0.000001
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class FaceTrackingError(RuntimeError):
    """A strict contract, integrity, association, or output-safety failure."""


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def pretty_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
        + "\n"
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def stable_id(prefix: str, *parts: Any) -> str:
    return f"{prefix}_{sha256_bytes(canonical_bytes(list(parts)))[:32]}"


def exact_keys(value: dict[str, Any], label: str, keys: set[str]) -> None:
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    if missing or unknown:
        details = []
        if missing:
            details.append(f"missing {missing}")
        if unknown:
            details.append(f"unknown {unknown}")
        raise FaceTrackingError(f"{label} has " + "; ".join(details))


def object_value(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise FaceTrackingError(f"{label} must be an object")
    return value


def list_value(value: object, label: str, minimum: int, maximum: int) -> list[Any]:
    if not isinstance(value, list) or not minimum <= len(value) <= maximum:
        raise FaceTrackingError(
            f"{label} must be an array with {minimum}..{maximum} entries"
        )
    return value


def string_value(value: object, label: str, maximum: int = 4096) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise FaceTrackingError(f"{label} must be a non-empty bounded string")
    return value


def identifier(value: object, label: str) -> str:
    text = string_value(value, label, 128)
    if not ID_RE.fullmatch(text):
        raise FaceTrackingError(f"{label} contains unsupported characters")
    return text


def integer(value: object, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FaceTrackingError(f"{label} must be an integer")
    if not minimum <= value <= maximum:
        raise FaceTrackingError(f"{label} must be between {minimum} and {maximum}")
    return value


def number(value: object, label: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FaceTrackingError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise FaceTrackingError(f"{label} must be finite and in [{minimum}, {maximum}]")
    return result


def false_value(value: object, label: str) -> bool:
    if value is not False:
        raise FaceTrackingError(f"{label} must be false")
    return False


def null_value(value: object, label: str) -> None:
    if value is not None:
        raise FaceTrackingError(f"{label} must be null")
    return None


def digest_value(value: object, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise FaceTrackingError(f"{label} must be a lowercase SHA-256")
    return value


def resolved_regular_file(
    value: object, label: str, *, sealed: bool, owner_private: bool = False
) -> Path:
    text = string_value(value, label)
    if "://" in text:
        raise FaceTrackingError(f"{label} must be a local path")
    path = Path(text)
    if not path.is_absolute():
        raise FaceTrackingError(f"{label} must be absolute")
    try:
        mode = path.lstat().st_mode
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise FaceTrackingError(f"{label} is not a readable current file: {error}") from error
    if path != resolved or stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise FaceTrackingError(f"{label} must be a resolved regular file")
    if sealed and mode & 0o222:
        raise FaceTrackingError(f"{label} must be sealed read-only")
    if owner_private and mode & 0o077:
        raise FaceTrackingError(f"{label} must be owner-private")
    return path


def stable_read(path: Path, label: str, *, maximum: int = MAX_JSON_BYTES) -> bytes:
    before = path.stat()
    if before.st_size > maximum:
        raise FaceTrackingError(f"{label} exceeds the {maximum}-byte limit")
    try:
        with path.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            identity = (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
            if identity != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns):
                raise FaceTrackingError(f"{label} changed while opening")
            body = handle.read(maximum + 1)
            after_fd = os.fstat(handle.fileno())
    except OSError as error:
        raise FaceTrackingError(f"cannot read {label}: {error}") from error
    after = path.stat()
    if len(body) > maximum:
        raise FaceTrackingError(f"{label} exceeds the {maximum}-byte limit")
    if identity != (
        after_fd.st_dev,
        after_fd.st_ino,
        after_fd.st_size,
        after_fd.st_mtime_ns,
    ) or identity != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise FaceTrackingError(f"{label} changed while reading")
    return body


def stable_capture(
    path: Path,
    expected_sha256: str,
    expected_byte_count: int,
    label: str,
    *,
    visibility: str,
) -> tuple[dict[str, Any], bytes]:
    """Hash and return the exact immutable byte capture used by the parser."""

    body = stable_read(path, label)
    actual_sha256 = sha256_bytes(body)
    if actual_sha256 != expected_sha256:
        raise FaceTrackingError(f"{label} SHA-256 mismatch")
    if len(body) != expected_byte_count:
        raise FaceTrackingError(f"{label} byte-count mismatch")
    return (
        {
            "path": str(path),
            "storage_uri": path.as_uri(),
            "sha256": actual_sha256,
            "byte_count": len(body),
            "visibility": visibility,
        },
        body,
    )


def stable_observe(
    path: Path,
    expected_sha256: str,
    expected_byte_count: int,
    label: str,
    *,
    visibility: str,
) -> dict[str, Any]:
    observation, _ = stable_capture(
        path,
        expected_sha256,
        expected_byte_count,
        label,
        visibility=visibility,
    )
    return observation


def load_json_bytes(body: bytes, label: str) -> object:
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FaceTrackingError(f"{label} must be valid UTF-8 JSON: {error}") from error


def validate_pin(
    raw: object, label: str, *, sealed: bool, owner_private: bool = False
) -> tuple[dict[str, Any], bytes]:
    value = object_value(raw, label)
    exact_keys(value, label, {"path", "expected_sha256", "expected_byte_count"})
    path = resolved_regular_file(
        value["path"],
        f"{label}.path",
        sealed=sealed,
        owner_private=owner_private,
    )
    return stable_capture(
        path,
        digest_value(value["expected_sha256"], f"{label}.expected_sha256"),
        integer(
            value["expected_byte_count"],
            f"{label}.expected_byte_count",
            1,
            MAX_JSON_BYTES,
        ),
        label,
        visibility="private",
    )


def validate_box(raw: object, label: str, width: int, height: int) -> dict[str, float]:
    value = object_value(raw, label)
    exact_keys(value, label, {"x", "y", "width", "height"})
    box = {
        "x": number(value["x"], f"{label}.x", 0, width),
        "y": number(value["y"], f"{label}.y", 0, height),
        "width": number(value["width"], f"{label}.width", 0.000001, width),
        "height": number(value["height"], f"{label}.height", 0.000001, height),
    }
    if box["x"] + box["width"] > width + 1e-9:
        raise FaceTrackingError(f"{label} exceeds frame width")
    if box["y"] + box["height"] > height + 1e-9:
        raise FaceTrackingError(f"{label} exceeds frame height")
    return box


def validate_detection_artifact(raw: object) -> dict[str, Any]:
    value = object_value(raw, "detections artifact")
    exact_keys(
        value,
        "detections artifact",
        {
            "schema_version",
            "artifact_type",
            "recording_id",
            "coordinate_system",
            "detector",
            "authority",
            "shots",
            "frames",
        },
    )
    if value["schema_version"] != SCHEMA_VERSION:
        raise FaceTrackingError("detections artifact schema_version must be 1")
    if value["artifact_type"] != "frame_local_face_detections":
        raise FaceTrackingError("detections artifact type is unsupported")
    if value["coordinate_system"] != "pixel_xywh_top_left":
        raise FaceTrackingError("coordinate_system must be pixel_xywh_top_left")

    detector_raw = object_value(value["detector"], "detector")
    exact_keys(detector_raw, "detector", {"name", "version", "recipe_id"})
    detector = {
        "name": string_value(detector_raw["name"], "detector.name", 128),
        "version": string_value(detector_raw["version"], "detector.version", 128),
        "recipe_id": identifier(detector_raw["recipe_id"], "detector.recipe_id"),
    }
    authority_raw = object_value(value["authority"], "authority")
    exact_keys(
        authority_raw,
        "authority",
        {
            "embeddings_present",
            "identity_state",
            "identity_label",
            "active_speaker_state",
            "active_speaker_score",
            "publication",
        },
    )
    false_value(authority_raw["embeddings_present"], "authority.embeddings_present")
    if authority_raw["identity_state"] != "unknown":
        raise FaceTrackingError("authority.identity_state must be unknown")
    null_value(authority_raw["identity_label"], "authority.identity_label")
    if authority_raw["active_speaker_state"] != "unknown":
        raise FaceTrackingError("authority.active_speaker_state must be unknown")
    null_value(authority_raw["active_speaker_score"], "authority.active_speaker_score")
    false_value(authority_raw["publication"], "authority.publication")

    shots_raw = list_value(value["shots"], "shots", 1, MAX_SHOTS)
    shots: list[dict[str, Any]] = []
    shot_ids: set[str] = set()
    previous_frame_end = -1
    previous_time_end = -1
    for ordinal, raw_shot in enumerate(shots_raw):
        label = f"shots[{ordinal}]"
        shot = object_value(raw_shot, label)
        exact_keys(
            shot,
            label,
            {
                "shot_id",
                "shot_ordinal",
                "start_frame_index",
                "end_frame_index_exclusive",
                "start_timestamp_ms",
                "end_timestamp_ms_exclusive",
            },
        )
        if shot["shot_ordinal"] != ordinal:
            raise FaceTrackingError(f"{label}.shot_ordinal must equal array position")
        shot_id = identifier(shot["shot_id"], f"{label}.shot_id")
        if shot_id in shot_ids:
            raise FaceTrackingError("shot IDs must be unique")
        shot_ids.add(shot_id)
        start_frame = integer(
            shot["start_frame_index"], f"{label}.start_frame_index", 0, MAX_FRAME_INDEX
        )
        end_frame = integer(
            shot["end_frame_index_exclusive"],
            f"{label}.end_frame_index_exclusive",
            1,
            MAX_FRAME_INDEX + 1,
        )
        start_time = integer(
            shot["start_timestamp_ms"], f"{label}.start_timestamp_ms", 0, MAX_TIMESTAMP_MS
        )
        end_time = integer(
            shot["end_timestamp_ms_exclusive"],
            f"{label}.end_timestamp_ms_exclusive",
            1,
            MAX_TIMESTAMP_MS + 1,
        )
        if end_frame <= start_frame or end_time <= start_time:
            raise FaceTrackingError(f"{label} must have non-empty half-open bounds")
        if start_frame < previous_frame_end or start_time < previous_time_end:
            raise FaceTrackingError("shots must be ordered and non-overlapping")
        previous_frame_end, previous_time_end = end_frame, end_time
        shots.append(
            {
                "shot_id": shot_id,
                "shot_ordinal": ordinal,
                "start_frame_index": start_frame,
                "end_frame_index_exclusive": end_frame,
                "start_timestamp_ms": start_time,
                "end_timestamp_ms_exclusive": end_time,
            }
        )

    frames_raw = list_value(value["frames"], "frames", 1, MAX_FRAMES)
    frames: list[dict[str, Any]] = []
    frame_ids: set[str] = set()
    detection_ids: set[str] = set()
    shot_by_id = {shot["shot_id"]: shot for shot in shots}
    frames_by_shot: dict[str, list[dict[str, Any]]] = {shot_id: [] for shot_id in shot_ids}
    dimensions_by_shot: dict[str, tuple[int, int]] = {}
    previous_frame_index = -1
    previous_timestamp = -1
    for ordinal, raw_frame in enumerate(frames_raw):
        label = f"frames[{ordinal}]"
        frame = object_value(raw_frame, label)
        exact_keys(
            frame,
            label,
            {
                "frame_id",
                "shot_id",
                "frame_index",
                "timestamp_ms",
                "width",
                "height",
                "detections",
            },
        )
        frame_id = identifier(frame["frame_id"], f"{label}.frame_id")
        if frame_id in frame_ids:
            raise FaceTrackingError("frame IDs must be unique")
        frame_ids.add(frame_id)
        shot_id = identifier(frame["shot_id"], f"{label}.shot_id")
        if shot_id not in shot_by_id:
            raise FaceTrackingError(f"{label}.shot_id is not declared")
        frame_index = integer(
            frame["frame_index"], f"{label}.frame_index", 0, MAX_FRAME_INDEX
        )
        timestamp_ms = integer(
            frame["timestamp_ms"], f"{label}.timestamp_ms", 0, MAX_TIMESTAMP_MS
        )
        if frame_index <= previous_frame_index or timestamp_ms <= previous_timestamp:
            raise FaceTrackingError("frames must have strictly increasing indices and timestamps")
        previous_frame_index, previous_timestamp = frame_index, timestamp_ms
        shot = shot_by_id[shot_id]
        if not shot["start_frame_index"] <= frame_index < shot["end_frame_index_exclusive"]:
            raise FaceTrackingError(f"{label}.frame_index is outside its shot bounds")
        if not shot["start_timestamp_ms"] <= timestamp_ms < shot["end_timestamp_ms_exclusive"]:
            raise FaceTrackingError(f"{label}.timestamp_ms is outside its shot bounds")
        width = integer(frame["width"], f"{label}.width", 1, 16_384)
        height = integer(frame["height"], f"{label}.height", 1, 16_384)
        dimensions = (width, height)
        expected_dimensions = dimensions_by_shot.setdefault(shot_id, dimensions)
        if dimensions != expected_dimensions:
            raise FaceTrackingError(
                f"{label} dimensions must remain constant within shot {shot_id}"
            )
        detections_raw = list_value(
            frame["detections"], f"{label}.detections", 0, MAX_DETECTIONS_PER_FRAME
        )
        detections: list[dict[str, Any]] = []
        for detection_ordinal, raw_detection in enumerate(detections_raw):
            dlabel = f"{label}.detections[{detection_ordinal}]"
            detection = object_value(raw_detection, dlabel)
            exact_keys(
                detection,
                dlabel,
                {
                    "detection_id",
                    "detection_ordinal",
                    "box",
                    "landmarks",
                    "detector_score",
                    "below_64px_width",
                },
            )
            if detection["detection_ordinal"] != detection_ordinal:
                raise FaceTrackingError(
                    f"{dlabel}.detection_ordinal must equal array position"
                )
            detection_id = identifier(
                detection["detection_id"], f"{dlabel}.detection_id"
            )
            if detection_id in detection_ids:
                raise FaceTrackingError("detection IDs must be globally unique")
            detection_ids.add(detection_id)
            box = validate_box(detection["box"], f"{dlabel}.box", width, height)
            landmarks_raw = list_value(detection["landmarks"], f"{dlabel}.landmarks", 5, 5)
            landmarks = []
            for point_ordinal, raw_point in enumerate(landmarks_raw):
                plabel = f"{dlabel}.landmarks[{point_ordinal}]"
                point = object_value(raw_point, plabel)
                exact_keys(point, plabel, {"x", "y"})
                landmarks.append(
                    {
                        "x": number(point["x"], f"{plabel}.x", 0, width),
                        "y": number(point["y"], f"{plabel}.y", 0, height),
                    }
                )
            below = detection["below_64px_width"]
            if not isinstance(below, bool) or below != (box["width"] < 64.0):
                raise FaceTrackingError(f"{dlabel}.below_64px_width is inconsistent")
            detections.append(
                {
                    "detection_id": detection_id,
                    "detection_ordinal": detection_ordinal,
                    "box": box,
                    "landmarks": landmarks,
                    "detector_score": number(
                        detection["detector_score"], f"{dlabel}.detector_score", 0, 1
                    ),
                    "below_64px_width": below,
                }
            )
        normalized = {
            "frame_id": frame_id,
            "shot_id": shot_id,
            "frame_index": frame_index,
            "timestamp_ms": timestamp_ms,
            "width": width,
            "height": height,
            "detections": detections,
        }
        frames.append(normalized)
        frames_by_shot[shot_id].append(normalized)

    for shot in shots:
        shot_frames = frames_by_shot[shot["shot_id"]]
        expected_count = shot["end_frame_index_exclusive"] - shot["start_frame_index"]
        complete = len(shot_frames) == expected_count and all(
            frame["frame_index"] == shot["start_frame_index"] + ordinal
            for ordinal, frame in enumerate(shot_frames)
        )
        if not complete:
            raise FaceTrackingError(
                f"shot {shot['shot_id']} must contain every frame index in its declared bounds"
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "artifact_type": "frame_local_face_detections",
        "recording_id": identifier(value["recording_id"], "recording_id"),
        "coordinate_system": "pixel_xywh_top_left",
        "detector": detector,
        "authority": {
            "embeddings_present": False,
            "identity_state": "unknown",
            "identity_label": None,
            "active_speaker_state": "unknown",
            "active_speaker_score": None,
            "publication": False,
        },
        "shots": shots,
        "frames": frames,
    }


def validate_output_root(value: object) -> Path:
    text = string_value(value, "output.root")
    if "://" in text:
        raise FaceTrackingError("output.root must be a local path")
    path = Path(text)
    if not path.is_absolute() or path == Path("/") or Path(os.path.normpath(text)) != path:
        raise FaceTrackingError("output.root must be a specific normalized absolute path")
    try:
        mode = path.lstat().st_mode
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise FaceTrackingError(f"output.root must already exist: {error}") from error
    if path != resolved or stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise FaceTrackingError("output.root must be a resolved directory")
    if mode & 0o077:
        raise FaceTrackingError("output.root must be owner-private (no group/other mode bits)")
    if not mode & stat.S_IWUSR or not mode & stat.S_IXUSR:
        raise FaceTrackingError("output.root must be owner-writable and traversable")
    return path


def ensure_private_directory(path: Path, label: str) -> None:
    """Create or verify one owner-private, non-symlink directory component."""

    if path.exists() or path.is_symlink():
        try:
            mode = path.lstat().st_mode
            resolved = path.resolve(strict=True)
        except (FileNotFoundError, OSError) as error:
            raise FaceTrackingError(f"{label} is unsafe: {error}") from error
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode) or resolved != path:
            raise FaceTrackingError(f"{label} must be a resolved directory")
    else:
        try:
            path.mkdir(mode=0o700)
        except OSError as error:
            raise FaceTrackingError(f"cannot create {label}: {error}") from error
    try:
        path.chmod(0o700)
        final_mode = path.lstat().st_mode
    except OSError as error:
        raise FaceTrackingError(f"cannot secure {label}: {error}") from error
    if final_mode & 0o077:
        raise FaceTrackingError(f"{label} must remain owner-private")


def validate_work_order(raw: object) -> dict[str, Any]:
    value = object_value(raw, "work order")
    exact_keys(
        value,
        "work order",
        {"schema_version", "job_id", "policy", "runtime", "input", "recipe", "output"},
    )
    if value["schema_version"] != SCHEMA_VERSION:
        raise FaceTrackingError("work order schema_version must be 1")
    policy = object_value(value["policy"], "policy")
    exact_keys(
        policy,
        "policy",
        {
            "visibility",
            "network_allowed",
            "embeddings_allowed",
            "cross_shot_join_allowed",
            "identity_decision_allowed",
            "active_speaker_decision_allowed",
            "publication_authority",
            "human_review_required",
        },
    )
    if policy["visibility"] != "private" or policy["publication_authority"] != "none":
        raise FaceTrackingError("policy must remain private with no publication authority")
    for key in (
        "network_allowed",
        "embeddings_allowed",
        "cross_shot_join_allowed",
        "identity_decision_allowed",
        "active_speaker_decision_allowed",
    ):
        false_value(policy[key], f"policy.{key}")
    if policy["human_review_required"] is not True:
        raise FaceTrackingError("policy.human_review_required must be true")

    runtime_raw = object_value(value["runtime"], "runtime")
    exact_keys(runtime_raw, "runtime", {"python"})
    python_raw = object_value(runtime_raw["python"], "runtime.python")
    exact_keys(
        python_raw,
        "runtime.python",
        {"path", "expected_sha256", "expected_byte_count", "expected_version"},
    )
    python_path = resolved_regular_file(
        python_raw["path"], "runtime.python.path", sealed=False
    )
    if python_path != Path(sys.executable).resolve(strict=True):
        raise FaceTrackingError("runtime.python.path must identify the executing interpreter")
    python_observation = stable_observe(
        python_path,
        digest_value(python_raw["expected_sha256"], "runtime.python.expected_sha256"),
        integer(
            python_raw["expected_byte_count"],
            "runtime.python.expected_byte_count",
            1,
            MAX_JSON_BYTES,
        ),
        "runtime.python",
        visibility="host_runtime",
    )
    expected_version = string_value(
        python_raw["expected_version"], "runtime.python.expected_version", 64
    )
    actual_version = ".".join(str(part) for part in sys.version_info[:3])
    if expected_version != actual_version:
        raise FaceTrackingError(
            "runtime Python version mismatch: "
            f"expected {expected_version}, observed {actual_version}"
        )

    input_raw = object_value(value["input"], "input")
    exact_keys(input_raw, "input", {"detections"})
    detections_observation, detections_body = validate_pin(
        input_raw["detections"],
        "input.detections",
        sealed=True,
        owner_private=True,
    )
    detections = validate_detection_artifact(
        load_json_bytes(detections_body, "input.detections")
    )

    recipe_raw = object_value(value["recipe"], "recipe")
    exact_keys(
        recipe_raw,
        "recipe",
        {
            "recipe_id",
            "algorithm",
            "prediction",
            "assignment_objective",
            "minimum_assignment_iou",
            "max_gap_frames",
            "minimum_confirmed_detections",
            "iou_round_decimals",
        },
    )
    if recipe_raw["algorithm"] != "constant_velocity_hungarian_iou_v1":
        raise FaceTrackingError("recipe.algorithm is unsupported")
    if recipe_raw["prediction"] != "last_two_matched_boxes_per_frame_delta_v1":
        raise FaceTrackingError("recipe.prediction is unsupported")
    if recipe_raw["assignment_objective"] != "max_cardinality_then_max_total_iou_stable_v1":
        raise FaceTrackingError("recipe.assignment_objective is unsupported")
    if recipe_raw["iou_round_decimals"] != 8:
        raise FaceTrackingError("recipe.iou_round_decimals must be 8")
    minimum_assignment_iou = number(
        recipe_raw["minimum_assignment_iou"],
        "recipe.minimum_assignment_iou",
        0.00000001,
        1,
    )
    if round_score(minimum_assignment_iou) != minimum_assignment_iou:
        raise FaceTrackingError(
            "recipe.minimum_assignment_iou must have at most eight decimal places"
        )
    recipe = {
        "recipe_id": identifier(recipe_raw["recipe_id"], "recipe.recipe_id"),
        "algorithm": recipe_raw["algorithm"],
        "prediction": recipe_raw["prediction"],
        "assignment_objective": recipe_raw["assignment_objective"],
        "minimum_assignment_iou": minimum_assignment_iou,
        "max_gap_frames": integer(
            recipe_raw["max_gap_frames"], "recipe.max_gap_frames", 0, 250
        ),
        "minimum_confirmed_detections": integer(
            recipe_raw["minimum_confirmed_detections"],
            "recipe.minimum_confirmed_detections",
            1,
            250,
        ),
        "iou_round_decimals": 8,
    }
    output_raw = object_value(value["output"], "output")
    exact_keys(output_raw, "output", {"root"})
    return {
        "schema_version": SCHEMA_VERSION,
        "job_id": identifier(value["job_id"], "job_id"),
        "policy": {
            "visibility": "private",
            "network_allowed": False,
            "embeddings_allowed": False,
            "cross_shot_join_allowed": False,
            "identity_decision_allowed": False,
            "active_speaker_decision_allowed": False,
            "publication_authority": "none",
            "human_review_required": True,
        },
        "runtime": {
            "python": {
                **python_observation,
                "expected_version": expected_version,
            }
        },
        "input": {
            "detections": detections_observation,
            "artifact": detections,
        },
        "recipe": recipe,
        "output": {"root": str(validate_output_root(output_raw["root"]))},
    }


def round_score(value: float) -> float:
    return math.floor(value * SCORE_SCALE + 0.5) / SCORE_SCALE


def iou(left: dict[str, float], right: dict[str, float]) -> float:
    x1 = max(left["x"], right["x"])
    y1 = max(left["y"], right["y"])
    x2 = min(left["x"] + left["width"], right["x"] + right["width"])
    y2 = min(left["y"] + left["height"], right["y"] + right["height"])
    intersection = max(0.0, x2 - x1) * max(0.0, y2 - y1)
    union = left["width"] * left["height"] + right["width"] * right["height"] - intersection
    return 0.0 if union <= 0 else intersection / union


def predicted_box(track: dict[str, Any], frame_index: int) -> dict[str, float]:
    matches = track["matches"]
    last = matches[-1]
    if len(matches) < 2:
        return dict(last["box"])
    previous = matches[-2]
    match_delta = last["frame_index"] - previous["frame_index"]
    prediction_delta = frame_index - last["frame_index"]
    prediction = {
        key: last["box"][key]
        + (last["box"][key] - previous["box"][key]) / match_delta * prediction_delta
        for key in ("x", "y", "width", "height")
    }
    if not all(math.isfinite(value) for value in prediction.values()):
        raise FaceTrackingError("constant-velocity prediction became non-finite")
    prediction["width"] = max(MIN_PREDICTED_EXTENT, prediction["width"])
    prediction["height"] = max(MIN_PREDICTED_EXTENT, prediction["height"])
    return prediction


def hungarian_minimize(cost: list[list[int]]) -> list[int]:
    """Return the chosen column per row for a square integer cost matrix.

    The classical potential-based algorithm is deterministic because rows, columns,
    and equal-cost scans are visited in ascending order.
    """

    size = len(cost)
    if size == 0 or any(len(row) != size for row in cost):
        raise FaceTrackingError("internal Hungarian matrix must be non-empty and square")
    u = [0] * (size + 1)
    v = [0] * (size + 1)
    p = [0] * (size + 1)
    way = [0] * (size + 1)
    infinity = 10**30
    for row in range(1, size + 1):
        p[0] = row
        column0 = 0
        minimum = [infinity] * (size + 1)
        used = [False] * (size + 1)
        while True:
            used[column0] = True
            row0 = p[column0]
            delta = infinity
            column1 = 0
            for column in range(1, size + 1):
                if used[column]:
                    continue
                current = cost[row0 - 1][column - 1] - u[row0] - v[column]
                if current < minimum[column]:
                    minimum[column] = current
                    way[column] = column0
                if minimum[column] < delta:
                    delta = minimum[column]
                    column1 = column
            for column in range(size + 1):
                if used[column]:
                    u[p[column]] += delta
                    v[column] -= delta
                else:
                    minimum[column] -= delta
            column0 = column1
            if p[column0] == 0:
                break
        while True:
            column1 = way[column0]
            p[column0] = p[column1]
            column0 = column1
            if column0 == 0:
                break
    answer = [-1] * size
    for column in range(1, size + 1):
        if p[column] != 0:
            answer[p[column] - 1] = column - 1
    return answer


def assign_tracks(
    tracks: list[dict[str, Any]],
    detections: list[dict[str, Any]],
    frame_index: int,
    threshold: float,
) -> tuple[list[tuple[int, int, dict[str, float], float]], list[int], list[int]]:
    if len(tracks) > MAX_ACTIVE_TRACKS:
        raise FaceTrackingError(
            f"active track count exceeds the fixed limit of {MAX_ACTIVE_TRACKS}"
        )
    if not tracks or not detections:
        return [], list(range(len(tracks))), list(range(len(detections)))
    track_count, detection_count = len(tracks), len(detections)
    size = track_count + detection_count
    match_bonus = (size + 1) * SCORE_SCALE
    forbidden = match_bonus * (size + 1)
    costs = [[0] * size for _ in range(size)]
    predictions = [predicted_box(track, frame_index) for track in tracks]
    raw_ious: list[list[float]] = []
    for track_ordinal, prediction in enumerate(predictions):
        row = []
        for detection_ordinal, detection in enumerate(detections):
            raw_score = iou(prediction, detection["box"])
            row.append(raw_score)
            if raw_score >= threshold:
                costs[track_ordinal][detection_ordinal] = -(
                    match_bonus
                    + int(math.floor(raw_score * SCORE_SCALE + 0.5))
                )
            else:
                costs[track_ordinal][detection_ordinal] = forbidden
        raw_ious.append(row)
    chosen = hungarian_minimize(costs)
    assignments = []
    matched_tracks: set[int] = set()
    matched_detections: set[int] = set()
    for track_ordinal in range(track_count):
        detection_ordinal = chosen[track_ordinal]
        if detection_ordinal < detection_count:
            raw_score = raw_ious[track_ordinal][detection_ordinal]
            if raw_score >= threshold:
                assignments.append(
                    (
                        track_ordinal,
                        detection_ordinal,
                        predictions[track_ordinal],
                        round_score(raw_score),
                    )
                )
                matched_tracks.add(track_ordinal)
                matched_detections.add(detection_ordinal)
    return (
        assignments,
        [index for index in range(track_count) if index not in matched_tracks],
        [index for index in range(detection_count) if index not in matched_detections],
    )


def public_box(box: dict[str, float]) -> dict[str, float]:
    return {key: round_score(box[key]) for key in ("x", "y", "width", "height")}


def track_detections(
    artifact: dict[str, Any], recipe: dict[str, Any], run_scope_id: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    frames_by_shot: dict[str, list[dict[str, Any]]] = {
        shot["shot_id"]: [] for shot in artifact["shots"]
    }
    for frame in artifact["frames"]:
        frames_by_shot[frame["shot_id"]].append(frame)
    all_frames: list[dict[str, Any]] = []
    all_tracks: list[dict[str, Any]] = []
    for shot in artifact["shots"]:
        active: list[dict[str, Any]] = []
        shot_tracks: list[dict[str, Any]] = []
        for frame in frames_by_shot[shot["shot_id"]]:
            assignments, unmatched_track_ordinals, unmatched_detection_ordinals = assign_tracks(
                active,
                frame["detections"],
                frame["frame_index"],
                recipe["minimum_assignment_iou"],
            )
            assignment_rows = []
            gaps = []
            closed_ids = []
            for track_ordinal, detection_ordinal, prediction, score in assignments:
                track = active[track_ordinal]
                detection = frame["detections"][detection_ordinal]
                track["matches"].append(
                    {"frame_index": frame["frame_index"], "box": detection["box"]}
                )
                track["consecutive_gap_count"] = 0
                track["detection_count"] += 1
                track["last_detection_frame_index"] = frame["frame_index"]
                track["last_detection_timestamp_ms"] = frame["timestamp_ms"]
                if (
                    track["confirmed_at_frame_index"] is None
                    and track["detection_count"] >= recipe["minimum_confirmed_detections"]
                ):
                    track["confirmed_at_frame_index"] = frame["frame_index"]
                    track["confirmed_at_detection_id"] = detection["detection_id"]
                observation = {
                    "event": "assigned_detection",
                    "frame_id": frame["frame_id"],
                    "frame_index": frame["frame_index"],
                    "timestamp_ms": frame["timestamp_ms"],
                    "detection_id": detection["detection_id"],
                    "observed_box": public_box(detection["box"]),
                    "predicted_box": public_box(prediction),
                    "assignment_iou": score,
                    "consecutive_gap_count": 0,
                }
                track["observations"].append(observation)
                assignment_rows.append(
                    {
                        "track_id": track["track_id"],
                        "detection_id": detection["detection_id"],
                        "predicted_box": public_box(prediction),
                        "assignment_iou": score,
                    }
                )
            for track_ordinal in unmatched_track_ordinals:
                track = active[track_ordinal]
                prediction = predicted_box(track, frame["frame_index"])
                track["consecutive_gap_count"] += 1
                track["total_gap_count"] += 1
                closes = track["consecutive_gap_count"] > recipe["max_gap_frames"]
                track["max_consecutive_gap_count"] = max(
                    track["max_consecutive_gap_count"], track["consecutive_gap_count"]
                )
                track["observations"].append(
                    {
                        "event": "gap",
                        "frame_id": frame["frame_id"],
                        "frame_index": frame["frame_index"],
                        "timestamp_ms": frame["timestamp_ms"],
                        "detection_id": None,
                        "observed_box": None,
                        "predicted_box": public_box(prediction),
                        "assignment_iou": None,
                        "consecutive_gap_count": track["consecutive_gap_count"],
                    }
                )
                gaps.append(
                    {
                        "track_id": track["track_id"],
                        "predicted_box": public_box(prediction),
                        "consecutive_gap_count": track["consecutive_gap_count"],
                        "closed": closes,
                    }
                )
                if closes:
                    track["ended_frame_index"] = frame["frame_index"]
                    track["ended_timestamp_ms"] = frame["timestamp_ms"]
                    track["end_reason"] = "max_gap_exceeded"
                    closed_ids.append(track["track_id"])

            starts = []
            for detection_ordinal in unmatched_detection_ordinals:
                detection = frame["detections"][detection_ordinal]
                track_ordinal = len(shot_tracks)
                track_id = stable_id(
                    "face_track",
                    run_scope_id,
                    shot["shot_id"],
                    track_ordinal,
                    detection["detection_id"],
                )
                confirmed = recipe["minimum_confirmed_detections"] == 1
                track = {
                    "track_id": track_id,
                    "track_ordinal": track_ordinal,
                    "shot_id": shot["shot_id"],
                    "created_frame_index": frame["frame_index"],
                    "created_timestamp_ms": frame["timestamp_ms"],
                    "last_detection_frame_index": frame["frame_index"],
                    "last_detection_timestamp_ms": frame["timestamp_ms"],
                    "ended_frame_index": None,
                    "ended_timestamp_ms": None,
                    "end_reason": None,
                    "detection_count": 1,
                    "total_gap_count": 0,
                    "max_consecutive_gap_count": 0,
                    "consecutive_gap_count": 0,
                    "confirmed_at_frame_index": frame["frame_index"] if confirmed else None,
                    "confirmed_at_detection_id": detection["detection_id"] if confirmed else None,
                    "matches": [{"frame_index": frame["frame_index"], "box": detection["box"]}],
                    "observations": [
                        {
                            "event": "track_started",
                            "frame_id": frame["frame_id"],
                            "frame_index": frame["frame_index"],
                            "timestamp_ms": frame["timestamp_ms"],
                            "detection_id": detection["detection_id"],
                            "observed_box": public_box(detection["box"]),
                            "predicted_box": None,
                            "assignment_iou": None,
                            "consecutive_gap_count": 0,
                        }
                    ],
                    "identity_state": "unknown",
                    "identity_label": None,
                    "identity_probability": None,
                    "active_speaker_state": "unknown",
                    "active_speaker_score": None,
                    "publication": False,
                }
                shot_tracks.append(track)
                active.append(track)
                if len(active) > MAX_ACTIVE_TRACKS:
                    raise FaceTrackingError(
                        "active track count exceeds the fixed limit of "
                        f"{MAX_ACTIVE_TRACKS}"
                    )
                starts.append({"track_id": track_id, "detection_id": detection["detection_id"]})

            closed_set = set(closed_ids)
            active = [track for track in active if track["track_id"] not in closed_set]
            all_frames.append(
                {
                    "frame_id": frame["frame_id"],
                    "shot_id": frame["shot_id"],
                    "frame_index": frame["frame_index"],
                    "timestamp_ms": frame["timestamp_ms"],
                    "detection_count": len(frame["detections"]),
                    "assignments": assignment_rows,
                    "gaps": gaps,
                    "starts": starts,
                    "closed_track_ids": closed_ids,
                }
            )
        final_frame = frames_by_shot[shot["shot_id"]][-1]
        for track in active:
            track["ended_frame_index"] = final_frame["frame_index"]
            track["ended_timestamp_ms"] = final_frame["timestamp_ms"]
            track["end_reason"] = "shot_end"
        if active:
            all_frames[-1]["closed_track_ids"].extend(
                track["track_id"] for track in active
            )
        for track in shot_tracks:
            public_track = {
                key: value
                for key, value in track.items()
                if key not in {"matches", "consecutive_gap_count"}
            }
            public_track["confirmation_state"] = (
                "confirmed" if track["confirmed_at_frame_index"] is not None else "unconfirmed"
            )
            all_tracks.append(public_track)
    return all_frames, all_tracks


def implementation_observation() -> dict[str, Any]:
    path = Path(__file__).resolve(strict=True)
    body = stable_read(path, "tracker implementation", maximum=4 * 1024 * 1024)
    return {
        "path": str(path),
        "sha256": sha256_bytes(body),
        "byte_count": len(body),
        "implementation_version": IMPLEMENTATION_VERSION,
    }


def result_layout(order: dict[str, Any], implementation: dict[str, Any]) -> tuple[str, Path]:
    key_payload = {
        "job_id": order["job_id"],
        "policy": order["policy"],
        "runtime": order["runtime"],
        "input": {"detections": order["input"]["detections"]},
        "recipe": order["recipe"],
        "implementation": implementation,
    }
    result_key = sha256_bytes(canonical_bytes(key_payload))
    final_dir = (
        Path(order["output"]["root"])
        / "vision"
        / "shot-local-face-tracks"
        / result_key
    )
    return result_key, final_dir


def recheck_inputs(order: dict[str, Any]) -> None:
    for observation, label, sealed in (
        (order["runtime"]["python"], "runtime.python", False),
        (order["input"]["detections"], "input.detections", True),
    ):
        path = resolved_regular_file(
            observation["path"],
            f"{label}.path",
            sealed=sealed,
            owner_private=label == "input.detections",
        )
        stable_observe(
            path,
            observation["sha256"],
            observation["byte_count"],
            label,
            visibility=(
                "private" if label == "input.detections" else "host_runtime"
            ),
        )


def run(order: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    implementation = implementation_observation()
    result_key, final_dir = result_layout(order, implementation)
    work_order_sha256 = sha256_bytes(
        canonical_bytes(
            {
                **order,
                "input": {"detections": order["input"]["detections"]},
            }
        )
    )
    if dry_run:
        return {
            "schema_version": SCHEMA_VERSION,
            "stage": STAGE,
            "status": "planned",
            "dry_run": True,
            "job_id": order["job_id"],
            "result_key": result_key,
            "result_path": str(final_dir / "result.json"),
            "frame_count": len(order["input"]["artifact"]["frames"]),
            "shot_count": len(order["input"]["artifact"]["shots"]),
            "network_allowed": False,
            "identity_decision_allowed": False,
            "active_speaker_decision_allowed": False,
            "publication_authority": "none",
        }
    output_root = Path(order["output"]["root"])
    vision_root = output_root / "vision"
    result_parent = vision_root / "shot-local-face-tracks"
    for directory in (vision_root, result_parent):
        ensure_private_directory(directory, str(directory))
    if final_dir.exists() or final_dir.is_symlink():
        raise FaceTrackingError(
            "result directory already exists; refusing to overwrite or reuse it"
        )
    recheck_inputs(order)
    frame_results, tracks = track_detections(
        order["input"]["artifact"], order["recipe"], result_key
    )
    result = {
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "stage": STAGE,
        "status": "completed",
        "dry_run": False,
        "job_id": order["job_id"],
        "work_order_sha256": work_order_sha256,
        "result_key": result_key,
        "result_path": str(final_dir / "result.json"),
        "implementation": implementation,
        "policy": order["policy"],
        "runtime": {
            "python": order["runtime"]["python"],
            "observation": {
                "python_version": ".".join(str(part) for part in sys.version_info[:3]),
                "cpu_threads": 1,
                "network_used": False,
            },
        },
        "input": {
            "detections": order["input"]["detections"],
            "recording_id": order["input"]["artifact"]["recording_id"],
            "detector": order["input"]["artifact"]["detector"],
            "coordinate_system": order["input"]["artifact"]["coordinate_system"],
        },
        "recipe": order["recipe"],
        "shots": order["input"]["artifact"]["shots"],
        "frames": frame_results,
        "tracks": tracks,
        "authority": {
            "identity_state": "unknown",
            "identity_label": None,
            "identity_probability": None,
            "identity_decision": False,
            "active_speaker_state": "unknown",
            "active_speaker_score": None,
            "active_speaker_decision": False,
            "cross_shot_join": False,
            "publication": False,
            "human_review_required": True,
        },
    }
    staging = result_parent / (
        f".staging-{result_key}-{os.getpid()}-{uuid.uuid4().hex}"
    )
    staging.mkdir(mode=0o700)
    try:
        result_path = staging / "result.json"
        result_body = pretty_bytes(result)
        if len(result_body) > MAX_RESULT_BYTES:
            raise FaceTrackingError(
                f"result exceeds the fixed {MAX_RESULT_BYTES}-byte limit"
            )
        result_path.write_bytes(result_body)
        result_path.chmod(0o400)
        recheck_inputs(order)
        ensure_private_directory(result_parent, str(result_parent))
        if final_dir.exists() or final_dir.is_symlink():
            raise FaceTrackingError("result directory appeared during execution")
        staging.chmod(0o500)
        staging.rename(final_dir)
    except Exception:
        if staging.exists():
            for child in staging.rglob("*"):
                if child.is_file():
                    child.chmod(0o600)
            for child in sorted(staging.rglob("*"), reverse=True):
                if child.is_dir():
                    child.chmod(0o700)
            staging.chmod(0o700)
            import shutil

            shutil.rmtree(staging)
        raise
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-order", required=True, type=Path)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        work_order_path = (
            args.work_order
            if args.work_order.is_absolute()
            else Path.cwd() / args.work_order
        )
        path = resolved_regular_file(str(work_order_path), "work order", sealed=False)
        order = validate_work_order(
            load_json_bytes(stable_read(path, "work order"), "work order")
        )
        result = run(order, dry_run=args.dry_run)
    except FaceTrackingError as error:
        print(f"shot-local-face-tracker: {error}", file=sys.stderr)
        return 1
    receipt = result
    if not args.dry_run:
        receipt = {
            "schema_version": SCHEMA_VERSION,
            "stage": STAGE,
            "status": "completed",
            "dry_run": False,
            "job_id": result["job_id"],
            "result_key": result["result_key"],
            "result_path": result["result_path"],
            "shot_count": len(result["shots"]),
            "frame_count": len(result["frames"]),
            "track_count": len(result["tracks"]),
            "identity_decision": False,
            "active_speaker_decision": False,
            "publication": False,
        }
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
