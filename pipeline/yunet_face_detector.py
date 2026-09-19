#!/usr/bin/env python3
"""Deterministic offline YuNet bridge for anonymous frame-local detections.

This bounded adapter accepts a complete, explicitly shot-aligned PNG sequence under
either the synthetic-fixture profile or the private, human-reviewed public single-shot
pilot profile. It runs the pinned FP32 YuNet detector with one OpenCV CPU thread and
emits the existing ``frame_local_face_detections`` artifact consumed by the shot-local
geometry tracker. It has no crop, embedding, recognition, identity, active-speaker,
database, network, or publication interface.
"""

from __future__ import annotations

import argparse
import ctypes
import errno
import hashlib
import importlib
import json
import math
import os
import re
import shutil
import stat
import struct
import sys
import uuid
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.2.0"
STAGE = "yunet_face_detector_bridge"
RECIPE_ID = "yunet_2023mar_opencv_cpu_0.9_0.3_5000_v1"
MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_RESULT_BYTES = 64 * 1024 * 1024
MAX_MODEL_BYTES = 16 * 1024 * 1024
MAX_RUNTIME_FILE_BYTES = 128 * 1024 * 1024
MAX_FRAME_BYTES = 16 * 1024 * 1024
MAX_TOTAL_FRAME_BYTES = 64 * 1024 * 1024
MAX_FRAMES = 64
MAX_SHOTS = 64
MAX_DETECTIONS_PER_FRAME = 64
MAX_PIXELS_PER_FRAME = 1920 * 1080
MAX_FRAME_INDEX = 100_000_000
MAX_TIMESTAMP_MS = 14 * 24 * 60 * 60 * 1000
FRAME_PERIOD_MS = 40
SUPPORTED_MATERIAL_SCOPES = {
    "synthetic_fixture_only",
    "public_single_shot_pilot_only",
}
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_OUTPUT_ROOTS = tuple(
    REPOSITORY_ROOT / name for name in ("src", "public", "dist", ".git")
)


class YuNetDetectorError(RuntimeError):
    """A strict contract, integrity, runtime, inference, or output failure."""


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
        raise YuNetDetectorError(f"{label} has " + "; ".join(details))


def object_value(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise YuNetDetectorError(f"{label} must be an object")
    return value


def list_value(value: object, label: str, minimum: int, maximum: int) -> list[Any]:
    if not isinstance(value, list) or not minimum <= len(value) <= maximum:
        raise YuNetDetectorError(
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
        raise YuNetDetectorError(f"{label} must be a non-empty bounded string")
    return value


def identifier(value: object, label: str) -> str:
    text = string_value(value, label, 128)
    if not ID_RE.fullmatch(text):
        raise YuNetDetectorError(f"{label} contains unsupported characters")
    return text


def integer(value: object, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise YuNetDetectorError(f"{label} must be an integer")
    if not minimum <= value <= maximum:
        raise YuNetDetectorError(f"{label} must be between {minimum} and {maximum}")
    return value


def number(value: object, label: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise YuNetDetectorError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise YuNetDetectorError(f"{label} must be finite and in [{minimum}, {maximum}]")
    return result


def false_value(value: object, label: str) -> bool:
    if value is not False:
        raise YuNetDetectorError(f"{label} must be false")
    return False


def digest_value(value: object, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise YuNetDetectorError(f"{label} must be a lowercase SHA-256")
    return value


def resolved_regular_file(
    value: object,
    label: str,
    *,
    sealed: bool,
    owner_private: bool,
) -> Path:
    text = string_value(value, label)
    if "://" in text:
        raise YuNetDetectorError(f"{label} must be a local path")
    path = Path(text)
    if not path.is_absolute():
        raise YuNetDetectorError(f"{label} must be absolute")
    try:
        metadata = path.lstat()
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise YuNetDetectorError(f"{label} is not a readable current file: {error}") from error
    if path != resolved or stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise YuNetDetectorError(f"{label} must be a resolved regular file")
    if sealed and metadata.st_mode & 0o222:
        raise YuNetDetectorError(f"{label} must be sealed read-only")
    if owner_private and metadata.st_mode & 0o077:
        raise YuNetDetectorError(f"{label} must be owner-private")
    if owner_private and metadata.st_nlink != 1:
        raise YuNetDetectorError(f"{label} must have exactly one hard link")
    return path


def resolved_private_directory(value: object, label: str, *, sealed: bool) -> Path:
    text = string_value(value, label)
    if "://" in text:
        raise YuNetDetectorError(f"{label} must be a local path")
    path = Path(text)
    if not path.is_absolute():
        raise YuNetDetectorError(f"{label} must be absolute")
    try:
        metadata = path.lstat()
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise YuNetDetectorError(f"{label} is not a readable directory: {error}") from error
    if path != resolved or stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise YuNetDetectorError(f"{label} must be a resolved directory")
    if metadata.st_mode & 0o077:
        raise YuNetDetectorError(f"{label} must be owner-private")
    if sealed and metadata.st_mode & 0o222:
        raise YuNetDetectorError(f"{label} must be sealed read-only")
    return path


def stable_read(path: Path, label: str, maximum: int) -> bytes:
    before = path.stat()
    if before.st_size > maximum:
        raise YuNetDetectorError(f"{label} exceeds the {maximum}-byte limit")
    try:
        with path.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            identity = (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
            if identity != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns):
                raise YuNetDetectorError(f"{label} changed while opening")
            body = handle.read(maximum + 1)
            after_fd = os.fstat(handle.fileno())
    except OSError as error:
        raise YuNetDetectorError(f"cannot read {label}: {error}") from error
    after = path.stat()
    if len(body) > maximum:
        raise YuNetDetectorError(f"{label} exceeds the {maximum}-byte limit")
    if identity != (
        after_fd.st_dev,
        after_fd.st_ino,
        after_fd.st_size,
        after_fd.st_mtime_ns,
    ) or identity != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise YuNetDetectorError(f"{label} changed while reading")
    return body


def stable_digest(path: Path, label: str, maximum: int) -> tuple[str, int]:
    before = path.stat()
    if before.st_size > maximum:
        raise YuNetDetectorError(f"{label} exceeds the {maximum}-byte limit")
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            identity = (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
            if identity != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns):
                raise YuNetDetectorError(f"{label} changed while opening")
            total = 0
            while chunk := handle.read(8 * 1024 * 1024):
                total += len(chunk)
                if total > maximum:
                    raise YuNetDetectorError(f"{label} exceeds the {maximum}-byte limit")
                digest.update(chunk)
            after_fd = os.fstat(handle.fileno())
    except OSError as error:
        raise YuNetDetectorError(f"cannot hash {label}: {error}") from error
    after = path.stat()
    if identity != (
        after_fd.st_dev,
        after_fd.st_ino,
        after_fd.st_size,
        after_fd.st_mtime_ns,
    ) or identity != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise YuNetDetectorError(f"{label} changed while hashing")
    return digest.hexdigest(), total


def file_observation(
    path: Path,
    expected_sha256: str,
    expected_byte_count: int,
    label: str,
    *,
    visibility: str,
    maximum: int,
) -> dict[str, Any]:
    actual_sha256, actual_byte_count = stable_digest(path, label, maximum)
    if actual_sha256 != expected_sha256:
        raise YuNetDetectorError(f"{label} SHA-256 mismatch")
    if actual_byte_count != expected_byte_count:
        raise YuNetDetectorError(f"{label} byte-count mismatch")
    return {
        "path": str(path),
        "storage_uri": path.as_uri(),
        "sha256": actual_sha256,
        "byte_count": actual_byte_count,
        "visibility": visibility,
    }


def validate_file_pin(
    raw: object,
    label: str,
    *,
    sealed: bool = True,
    owner_private: bool = True,
    visibility: str = "private",
    maximum: int = MAX_RUNTIME_FILE_BYTES,
) -> tuple[dict[str, Any], Path]:
    value = object_value(raw, label)
    exact_keys(value, label, {"path", "expected_sha256", "expected_byte_count"})
    path = resolved_regular_file(
        value["path"],
        f"{label}.path",
        sealed=sealed,
        owner_private=owner_private,
    )
    observation = file_observation(
        path,
        digest_value(value["expected_sha256"], f"{label}.expected_sha256"),
        integer(
            value["expected_byte_count"],
            f"{label}.expected_byte_count",
            1,
            maximum,
        ),
        label,
        visibility=visibility,
        maximum=maximum,
    )
    return observation, path


def validate_captured_pin(
    raw: object,
    label: str,
    *,
    maximum: int,
) -> tuple[dict[str, Any], Path, bytes]:
    value = object_value(raw, label)
    exact_keys(value, label, {"path", "expected_sha256", "expected_byte_count"})
    path = resolved_regular_file(
        value["path"],
        f"{label}.path",
        sealed=True,
        owner_private=True,
    )
    expected_sha256 = digest_value(
        value["expected_sha256"], f"{label}.expected_sha256"
    )
    expected_byte_count = integer(
        value["expected_byte_count"],
        f"{label}.expected_byte_count",
        1,
        maximum,
    )
    body = stable_read(path, label, maximum)
    if sha256_bytes(body) != expected_sha256:
        raise YuNetDetectorError(f"{label} SHA-256 mismatch")
    if len(body) != expected_byte_count:
        raise YuNetDetectorError(f"{label} byte-count mismatch")
    return (
        {
            "path": str(path),
            "storage_uri": path.as_uri(),
            "sha256": expected_sha256,
            "byte_count": expected_byte_count,
            "visibility": "private",
        },
        path,
        body,
    )


def load_json_bytes(body: bytes, label: str) -> object:
    def reject_constant(value: str) -> object:
        raise YuNetDetectorError(f"{label} contains non-finite constant {value}")

    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise YuNetDetectorError(f"{label} contains duplicate key {key!r}")
            result[key] = value
        return result

    try:
        return json.loads(
            body.decode("utf-8"),
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except YuNetDetectorError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise YuNetDetectorError(f"{label} must be valid UTF-8 JSON: {error}") from error


def png_dimensions(body: bytes, label: str) -> tuple[int, int]:
    if len(body) < 24 or body[:8] != PNG_SIGNATURE or body[12:16] != b"IHDR":
        raise YuNetDetectorError(f"{label} is not a supported PNG")
    width, height = struct.unpack(">II", body[16:24])
    if width < 1 or height < 1 or width * height > MAX_PIXELS_PER_FRAME:
        raise YuNetDetectorError(f"{label} dimensions exceed the bounded profile")
    return width, height


def validate_policy(raw: object) -> dict[str, Any]:
    value = object_value(raw, "policy")
    exact_keys(
        value,
        "policy",
        {
            "visibility",
            "material_scope",
            "network_allowed",
            "embeddings_allowed",
            "identity_decision_allowed",
            "active_speaker_decision_allowed",
            "publication_authority",
            "human_review_required",
        },
    )
    if value["visibility"] != "private":
        raise YuNetDetectorError("policy.visibility must be private")
    material_scope = string_value(
        value["material_scope"], "policy.material_scope", 64
    )
    if material_scope not in SUPPORTED_MATERIAL_SCOPES:
        raise YuNetDetectorError(
            "policy.material_scope must be synthetic_fixture_only or "
            "public_single_shot_pilot_only"
        )
    for key in (
        "network_allowed",
        "embeddings_allowed",
        "identity_decision_allowed",
        "active_speaker_decision_allowed",
    ):
        false_value(value[key], f"policy.{key}")
    if value["publication_authority"] != "none":
        raise YuNetDetectorError("policy.publication_authority must be none")
    if value["human_review_required"] is not True:
        raise YuNetDetectorError("policy.human_review_required must be true")
    return {
        "visibility": "private",
        "material_scope": material_scope,
        "network_allowed": False,
        "embeddings_allowed": False,
        "identity_decision_allowed": False,
        "active_speaker_decision_allowed": False,
        "publication_authority": "none",
        "human_review_required": True,
    }


def validate_runtime(raw: object) -> dict[str, Any]:
    value = object_value(raw, "runtime")
    exact_keys(value, "runtime", {"module_root", "python", "opencv", "numpy"})
    module_root = resolved_private_directory(
        value["module_root"], "runtime.module_root", sealed=True
    )
    python_raw = object_value(value["python"], "runtime.python")
    exact_keys(
        python_raw,
        "runtime.python",
        {"path", "expected_sha256", "expected_byte_count", "expected_version"},
    )
    python_observation, python_path = validate_file_pin(
        {
            "path": python_raw["path"],
            "expected_sha256": python_raw["expected_sha256"],
            "expected_byte_count": python_raw["expected_byte_count"],
        },
        "runtime.python",
        sealed=False,
        owner_private=False,
        visibility="host_runtime",
        maximum=MAX_RUNTIME_FILE_BYTES,
    )
    if python_path != Path(sys.executable).resolve(strict=True):
        raise YuNetDetectorError("runtime.python.path must identify this interpreter")
    expected_python_version = string_value(
        python_raw["expected_version"], "runtime.python.expected_version", 64
    )
    actual_python_version = ".".join(str(part) for part in sys.version_info[:3])
    if expected_python_version != actual_python_version:
        raise YuNetDetectorError("runtime Python version differs from its pin")

    packages: dict[str, Any] = {}
    for name in ("opencv", "numpy"):
        package_raw = object_value(value[name], f"runtime.{name}")
        required = {"wheel", "binary", "expected_version"}
        if name == "opencv":
            required.add("expected_build_information_sha256")
        exact_keys(package_raw, f"runtime.{name}", required)
        wheel_observation, wheel_path = validate_file_pin(
            package_raw["wheel"], f"runtime.{name}.wheel"
        )
        binary_observation, binary_path = validate_file_pin(
            package_raw["binary"], f"runtime.{name}.binary"
        )
        try:
            wheel_path.relative_to(module_root.parent)
            binary_path.relative_to(module_root)
        except ValueError as error:
            raise YuNetDetectorError(
                f"runtime.{name} wheel/binary must be inside the isolated asset tree"
            ) from error
        package = {
            "wheel": wheel_observation,
            "binary": binary_observation,
            "expected_version": string_value(
                package_raw["expected_version"],
                f"runtime.{name}.expected_version",
                64,
            ),
        }
        if name == "opencv":
            package["expected_build_information_sha256"] = digest_value(
                package_raw["expected_build_information_sha256"],
                "runtime.opencv.expected_build_information_sha256",
            )
        packages[name] = package
    return {
        "module_root": str(module_root),
        "python": {**python_observation, "expected_version": expected_python_version},
        **packages,
    }


def validate_model(raw: object) -> tuple[dict[str, Any], bytes]:
    value = object_value(raw, "model")
    exact_keys(
        value,
        "model",
        {
            "artifact",
            "license",
            "upstream_url",
            "upstream_commit",
            "license_expression",
        },
    )
    artifact_observation, _, artifact_body = validate_captured_pin(
        value["artifact"], "model.artifact", maximum=MAX_MODEL_BYTES
    )
    license_observation, _ = validate_file_pin(
        value["license"], "model.license", maximum=MAX_MODEL_BYTES
    )
    upstream_url = string_value(value["upstream_url"], "model.upstream_url", 2048)
    commit = string_value(value["upstream_commit"], "model.upstream_commit", 40)
    if (
        not upstream_url.startswith("https://github.com/opencv/opencv_zoo/")
        or not COMMIT_RE.fullmatch(commit)
        or commit not in upstream_url
    ):
        raise YuNetDetectorError("model upstream must be an immutable OpenCV Zoo URL")
    if value["license_expression"] != "MIT":
        raise YuNetDetectorError("model.license_expression must be MIT")
    return (
        {
            "artifact": artifact_observation,
            "license": license_observation,
            "upstream_url": upstream_url,
            "upstream_commit": commit,
            "license_expression": "MIT",
        },
        artifact_body,
    )


def validate_parameters(raw: object) -> dict[str, Any]:
    value = object_value(raw, "parameters")
    exact_keys(
        value,
        "parameters",
        {
            "recipe_id",
            "score_threshold",
            "nms_threshold",
            "top_k",
            "threads",
            "backend",
            "target",
            "frame_period_ms",
            "max_frames",
            "max_detections_per_frame",
        },
    )
    if identifier(value["recipe_id"], "parameters.recipe_id") != RECIPE_ID:
        raise YuNetDetectorError(f"parameters.recipe_id must be {RECIPE_ID}")
    if number(value["score_threshold"], "parameters.score_threshold", 0, 1) != 0.9:
        raise YuNetDetectorError("parameters.score_threshold must be 0.9")
    if number(value["nms_threshold"], "parameters.nms_threshold", 0, 1) != 0.3:
        raise YuNetDetectorError("parameters.nms_threshold must be 0.3")
    if integer(value["top_k"], "parameters.top_k", 1, 5000) != 5000:
        raise YuNetDetectorError("parameters.top_k must be 5000")
    if integer(value["threads"], "parameters.threads", 1, 1) != 1:
        raise YuNetDetectorError("parameters.threads must be 1")
    if value["backend"] != "opencv_cpu" or value["target"] != "cpu":
        raise YuNetDetectorError("parameters backend/target must be opencv_cpu/cpu")
    if integer(value["frame_period_ms"], "parameters.frame_period_ms", 1, 1000) != 40:
        raise YuNetDetectorError("parameters.frame_period_ms must be 40")
    return {
        "recipe_id": RECIPE_ID,
        "score_threshold": 0.9,
        "nms_threshold": 0.3,
        "top_k": 5000,
        "threads": 1,
        "backend": "opencv_cpu",
        "target": "cpu",
        "frame_period_ms": FRAME_PERIOD_MS,
        "max_frames": integer(
            value["max_frames"], "parameters.max_frames", 1, MAX_FRAMES
        ),
        "max_detections_per_frame": integer(
            value["max_detections_per_frame"],
            "parameters.max_detections_per_frame",
            1,
            MAX_DETECTIONS_PER_FRAME,
        ),
    }


def validate_shots(raw: object) -> list[dict[str, Any]]:
    rows = list_value(raw, "shots", 1, MAX_SHOTS)
    shots: list[dict[str, Any]] = []
    ids: set[str] = set()
    previous_frame_end = -1
    previous_time_end = -1
    for ordinal, raw_shot in enumerate(rows):
        label = f"shots[{ordinal}]"
        value = object_value(raw_shot, label)
        exact_keys(
            value,
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
        shot_ordinal = integer(
            value["shot_ordinal"], f"{label}.shot_ordinal", 0, MAX_SHOTS - 1
        )
        if shot_ordinal != ordinal:
            raise YuNetDetectorError(f"{label}.shot_ordinal must equal array position")
        shot_id = identifier(value["shot_id"], f"{label}.shot_id")
        if shot_id in ids:
            raise YuNetDetectorError("shot IDs must be unique")
        ids.add(shot_id)
        start_frame = integer(
            value["start_frame_index"], f"{label}.start_frame_index", 0, MAX_FRAME_INDEX
        )
        end_frame = integer(
            value["end_frame_index_exclusive"],
            f"{label}.end_frame_index_exclusive",
            1,
            MAX_FRAME_INDEX + 1,
        )
        start_time = integer(
            value["start_timestamp_ms"], f"{label}.start_timestamp_ms", 0, MAX_TIMESTAMP_MS
        )
        end_time = integer(
            value["end_timestamp_ms_exclusive"],
            f"{label}.end_timestamp_ms_exclusive",
            1,
            MAX_TIMESTAMP_MS + 1,
        )
        if end_frame <= start_frame or end_time <= start_time:
            raise YuNetDetectorError(f"{label} must have non-empty half-open bounds")
        if start_frame < previous_frame_end or start_time < previous_time_end:
            raise YuNetDetectorError("shots must be ordered and non-overlapping")
        frame_count = end_frame - start_frame
        if end_time != start_time + frame_count * FRAME_PERIOD_MS:
            raise YuNetDetectorError(
                f"{label} time bounds must exactly cover its 25 fps frame indices"
            )
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
    return shots


def validate_frames(
    raw: object,
    shots: list[dict[str, Any]],
    parameters: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, bytes]]:
    rows = list_value(raw, "frames", 1, parameters["max_frames"])
    frames: list[dict[str, Any]] = []
    bodies: dict[str, bytes] = {}
    ids: set[str] = set()
    previous_frame_index = -1
    previous_timestamp = -1
    total_bytes = 0
    dimensions_by_shot: dict[str, tuple[int, int]] = {}
    frames_by_shot: dict[str, list[dict[str, Any]]] = {
        shot["shot_id"]: [] for shot in shots
    }
    for ordinal, raw_frame in enumerate(rows):
        label = f"frames[{ordinal}]"
        value = object_value(raw_frame, label)
        exact_keys(
            value,
            label,
            {
                "frame_id",
                "shot_id",
                "frame_index",
                "timestamp_ms",
                "media_type",
                "input",
            },
        )
        frame_id = identifier(value["frame_id"], f"{label}.frame_id")
        if frame_id in ids:
            raise YuNetDetectorError("frame IDs must be unique")
        ids.add(frame_id)
        shot_id = identifier(value["shot_id"], f"{label}.shot_id")
        frame_index = integer(
            value["frame_index"], f"{label}.frame_index", 0, MAX_FRAME_INDEX
        )
        timestamp_ms = integer(
            value["timestamp_ms"], f"{label}.timestamp_ms", 0, MAX_TIMESTAMP_MS
        )
        if frame_index <= previous_frame_index or timestamp_ms <= previous_timestamp:
            raise YuNetDetectorError(
                "frames must have strictly increasing indices and timestamps"
            )
        previous_frame_index, previous_timestamp = frame_index, timestamp_ms
        if value["media_type"] != "image/png":
            raise YuNetDetectorError(f"{label}.media_type must be image/png")
        matches = [
            shot
            for shot in shots
            if shot["start_frame_index"]
            <= frame_index
            < shot["end_frame_index_exclusive"]
            and shot["start_timestamp_ms"]
            <= timestamp_ms
            < shot["end_timestamp_ms_exclusive"]
        ]
        if len(matches) != 1 or matches[0]["shot_id"] != shot_id:
            raise YuNetDetectorError(
                f"{label} has an ambiguous, missing, or mismatched shot assignment"
            )
        shot = matches[0]
        expected_timestamp = shot["start_timestamp_ms"] + (
            frame_index - shot["start_frame_index"]
        ) * FRAME_PERIOD_MS
        if timestamp_ms != expected_timestamp:
            raise YuNetDetectorError(f"{label}.timestamp_ms breaks dense 25 fps lineage")
        observation, _, body = validate_captured_pin(
            value["input"], f"{label}.input", maximum=MAX_FRAME_BYTES
        )
        total_bytes += len(body)
        if total_bytes > MAX_TOTAL_FRAME_BYTES:
            raise YuNetDetectorError(
                f"frame bytes exceed the {MAX_TOTAL_FRAME_BYTES}-byte aggregate limit"
            )
        width, height = png_dimensions(body, f"{label}.input")
        dimensions = (width, height)
        expected_dimensions = dimensions_by_shot.setdefault(shot_id, dimensions)
        if dimensions != expected_dimensions:
            raise YuNetDetectorError(
                f"{label} dimensions must remain constant within shot {shot_id}"
            )
        frame = {
            "frame_id": frame_id,
            "shot_id": shot_id,
            "frame_index": frame_index,
            "timestamp_ms": timestamp_ms,
            "media_type": "image/png",
            "input": observation,
            "image": {"width": width, "height": height, "format": "png"},
        }
        frames.append(frame)
        bodies[frame_id] = body
        frames_by_shot[shot_id].append(frame)

    for shot in shots:
        shot_frames = frames_by_shot[shot["shot_id"]]
        expected_count = shot["end_frame_index_exclusive"] - shot["start_frame_index"]
        if len(shot_frames) != expected_count or any(
            frame["frame_index"] != shot["start_frame_index"] + ordinal
            for ordinal, frame in enumerate(shot_frames)
        ):
            raise YuNetDetectorError(
                f"shot {shot['shot_id']} must contain every dense frame exactly once"
            )
    return frames, bodies


def validate_output_root(value: object) -> Path:
    text = string_value(value, "output.root")
    if "://" in text:
        raise YuNetDetectorError("output.root must be a local path")
    path = Path(text)
    if not path.is_absolute() or path == Path("/") or Path(os.path.normpath(text)) != path:
        raise YuNetDetectorError("output.root must be a specific normalized absolute path")
    try:
        metadata = path.lstat()
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise YuNetDetectorError(f"output.root must already exist: {error}") from error
    if path != resolved or stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise YuNetDetectorError("output.root must be a resolved directory")
    if metadata.st_mode & 0o077:
        raise YuNetDetectorError("output.root must be owner-private")
    if not metadata.st_mode & stat.S_IWUSR or not metadata.st_mode & stat.S_IXUSR:
        raise YuNetDetectorError("output.root must be owner-writable and traversable")
    for forbidden in FORBIDDEN_OUTPUT_ROOTS:
        if path == forbidden or forbidden in path.parents:
            raise YuNetDetectorError(
                "output.root must not be inside a repository release or Git path"
            )
    return path


def validate_work_order(raw: object) -> dict[str, Any]:
    value = object_value(raw, "work order")
    exact_keys(
        value,
        "work order",
        {
            "schema_version",
            "job_id",
            "policy",
            "runtime",
            "model",
            "parameters",
            "recording_id",
            "shots",
            "frames",
            "output",
        },
    )
    if (
        isinstance(value["schema_version"], bool)
        or not isinstance(value["schema_version"], int)
        or value["schema_version"] != SCHEMA_VERSION
    ):
        raise YuNetDetectorError("work order schema_version must be 1")
    policy = validate_policy(value["policy"])
    runtime = validate_runtime(value["runtime"])
    model, model_body = validate_model(value["model"])
    parameters = validate_parameters(value["parameters"])
    shots = validate_shots(value["shots"])
    frames, frame_bodies = validate_frames(value["frames"], shots, parameters)
    if policy["material_scope"] == "public_single_shot_pilot_only":
        if len(shots) != 1:
            raise YuNetDetectorError(
                "public_single_shot_pilot_only requires exactly one reviewed shot"
            )
        shot_duration_ms = (
            shots[0]["end_timestamp_ms_exclusive"]
            - shots[0]["start_timestamp_ms"]
        )
        if shot_duration_ms > 5000:
            raise YuNetDetectorError(
                "public_single_shot_pilot_only is limited to five seconds"
            )
    output_raw = object_value(value["output"], "output")
    exact_keys(output_raw, "output", {"root"})
    output_root = validate_output_root(output_raw["root"])
    return {
        "schema_version": SCHEMA_VERSION,
        "job_id": identifier(value["job_id"], "job_id"),
        "policy": policy,
        "runtime": runtime,
        "model": model,
        "parameters": parameters,
        "recording_id": identifier(value["recording_id"], "recording_id"),
        "shots": shots,
        "frames": frames,
        "output": {"root": str(output_root)},
        "_model_body": model_body,
        "_frame_bodies": frame_bodies,
    }


def public_order(order: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in order.items() if not key.startswith("_")}


def ensure_private_directory(path: Path, label: str) -> None:
    if path.exists() or path.is_symlink():
        try:
            metadata = path.lstat()
            resolved = path.resolve(strict=True)
        except (FileNotFoundError, OSError) as error:
            raise YuNetDetectorError(f"{label} is unsafe: {error}") from error
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode) or path != resolved:
            raise YuNetDetectorError(f"{label} must be a resolved directory")
    else:
        path.mkdir(mode=0o700)
    path.chmod(0o700)
    if path.lstat().st_mode & 0o077:
        raise YuNetDetectorError(f"{label} must remain owner-private")


def implementation_observation() -> dict[str, Any]:
    path = Path(__file__).resolve(strict=True)
    body = stable_read(path, "detector implementation", 4 * 1024 * 1024)
    return {
        "path": str(path),
        "sha256": sha256_bytes(body),
        "byte_count": len(body),
        "implementation_version": IMPLEMENTATION_VERSION,
    }


def load_runtime(runtime: dict[str, Any]) -> tuple[Any, Any, dict[str, Any]]:
    if "cv2" in sys.modules or "numpy" in sys.modules:
        raise YuNetDetectorError("cv2/numpy must not be imported before isolated load")
    os.environ["OPENCV_FOR_THREADS_NUM"] = "1"
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    sys.path.insert(0, runtime["module_root"])
    try:
        numpy = importlib.import_module("numpy")
        cv2 = importlib.import_module("cv2")
    except Exception as error:
        raise YuNetDetectorError(f"cannot load isolated OpenCV runtime: {error}") from error
    module_root = Path(runtime["module_root"])
    for module, name in ((numpy, "numpy"), (cv2, "opencv")):
        try:
            Path(module.__file__).resolve(strict=True).relative_to(module_root)
        except (AttributeError, OSError, ValueError) as error:
            raise YuNetDetectorError(f"{name} did not load from the pinned module root") from error
        if module.__version__ != runtime[name]["expected_version"]:
            raise YuNetDetectorError(f"{name} version differs from its pin")
    build_hash = sha256_bytes(cv2.getBuildInformation().encode("utf-8"))
    if build_hash != runtime["opencv"]["expected_build_information_sha256"]:
        raise YuNetDetectorError("OpenCV build information differs from its pin")
    cv2.setNumThreads(1)
    if hasattr(cv2, "ocl"):
        cv2.ocl.setUseOpenCL(False)
    if cv2.getNumThreads() != 1:
        raise YuNetDetectorError("OpenCV did not accept the one-thread limit")
    if not hasattr(cv2, "FaceDetectorYN_create"):
        raise YuNetDetectorError("OpenCV runtime lacks FaceDetectorYN_create")
    recheck_runtime_files(runtime)
    return cv2, numpy, {
        "python_version": ".".join(str(part) for part in sys.version_info[:3]),
        "opencv_version": cv2.__version__,
        "numpy_version": numpy.__version__,
        "opencv_build_information_sha256": build_hash,
        "opencv_threads": cv2.getNumThreads(),
        "opencl_enabled": bool(cv2.ocl.useOpenCL()) if hasattr(cv2, "ocl") else False,
        "network_used": False,
    }


def recheck_observation(
    observation: dict[str, Any],
    label: str,
    *,
    sealed: bool,
    owner_private: bool,
    maximum: int,
) -> None:
    path = resolved_regular_file(
        observation["path"], label, sealed=sealed, owner_private=owner_private
    )
    file_observation(
        path,
        observation["sha256"],
        observation["byte_count"],
        label,
        visibility=observation["visibility"],
        maximum=maximum,
    )


def recheck_runtime_files(runtime: dict[str, Any]) -> None:
    recheck_observation(
        runtime["python"],
        "runtime.python",
        sealed=False,
        owner_private=False,
        maximum=MAX_RUNTIME_FILE_BYTES,
    )
    for name in ("opencv", "numpy"):
        for kind in ("wheel", "binary"):
            recheck_observation(
                runtime[name][kind],
                f"runtime.{name}.{kind}",
                sealed=True,
                owner_private=True,
                maximum=MAX_RUNTIME_FILE_BYTES,
            )


def recheck_inputs(order: dict[str, Any]) -> None:
    resolved_private_directory(order["runtime"]["module_root"], "runtime.module_root", sealed=True)
    recheck_runtime_files(order["runtime"])
    recheck_observation(
        order["model"]["artifact"],
        "model.artifact",
        sealed=True,
        owner_private=True,
        maximum=MAX_MODEL_BYTES,
    )
    recheck_observation(
        order["model"]["license"],
        "model.license",
        sealed=True,
        owner_private=True,
        maximum=MAX_MODEL_BYTES,
    )
    for frame in order["frames"]:
        recheck_observation(
            frame["input"],
            f"frame {frame['frame_id']}",
            sealed=True,
            owner_private=True,
            maximum=MAX_FRAME_BYTES,
        )


def finite_face_row(raw: object, label: str) -> list[float]:
    values_raw = raw.tolist() if hasattr(raw, "tolist") else raw
    if not isinstance(values_raw, list) or len(values_raw) != 15:
        raise YuNetDetectorError(f"{label} must contain 15 YuNet values")
    values: list[float] = []
    for ordinal, value in enumerate(values_raw):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise YuNetDetectorError(f"{label}[{ordinal}] must be numeric")
        item = float(value)
        if not math.isfinite(item):
            raise YuNetDetectorError(f"{label}[{ordinal}] must be finite")
        values.append(0.0 if item == 0 else item)
    return values


def validate_face_values(
    values: list[float], label: str, width: int, height: int
) -> None:
    x, y, box_width, box_height = values[:4]
    if x < 0 or y < 0 or box_width <= 0 or box_height <= 0:
        raise YuNetDetectorError(f"{label} emitted an invalid face box")
    if x + box_width > width + 1e-6 or y + box_height > height + 1e-6:
        raise YuNetDetectorError(f"{label} face box exceeds frame bounds")
    for index in range(4, 14, 2):
        if not 0 <= values[index] <= width or not 0 <= values[index + 1] <= height:
            raise YuNetDetectorError(f"{label} landmark exceeds frame bounds")
    if not 0 <= values[14] <= 1:
        raise YuNetDetectorError(f"{label} score must be in [0, 1]")


def create_detector(cv2: Any, model_path: Path, parameters: dict[str, Any]) -> Any:
    try:
        backend = cv2.dnn.DNN_BACKEND_OPENCV
        target = cv2.dnn.DNN_TARGET_CPU
        return cv2.FaceDetectorYN_create(
            str(model_path),
            "",
            (320, 320),
            parameters["score_threshold"],
            parameters["nms_threshold"],
            parameters["top_k"],
            backend,
            target,
        )
    except Exception as error:
        raise YuNetDetectorError(f"cannot construct pinned YuNet detector: {error}") from error


def detect_frames(
    order: dict[str, Any], cv2: Any, numpy: Any, detector: Any
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    artifact_frames: list[dict[str, Any]] = []
    receipt_frames: list[dict[str, Any]] = []
    bodies = order["_frame_bodies"]
    for frame in order["frames"]:
        encoded = numpy.frombuffer(bodies[frame["frame_id"]], dtype=numpy.uint8)
        image = cv2.imdecode(encoded, cv2.IMREAD_COLOR)
        if image is None or len(image.shape) != 3 or image.shape[2] != 3:
            raise YuNetDetectorError(f"OpenCV could not decode frame {frame['frame_id']}")
        height, width = int(image.shape[0]), int(image.shape[1])
        if (width, height) != (frame["image"]["width"], frame["image"]["height"]):
            raise YuNetDetectorError("OpenCV dimensions differ from the captured PNG header")
        detector.setInputSize((width, height))
        try:
            _, faces = detector.detect(image)
        except Exception as error:
            raise YuNetDetectorError(
                f"YuNet inference failed for frame {frame['frame_id']}: {error}"
            ) from error
        rows = [] if faces is None else [
            finite_face_row(row, f"frame {frame['frame_id']} detection") for row in faces
        ]
        rows.sort(key=lambda row: (-row[14], *row[:14]))
        if len(rows) > order["parameters"]["max_detections_per_frame"]:
            raise YuNetDetectorError("detector count exceeds max_detections_per_frame")
        detections: list[dict[str, Any]] = []
        for ordinal, values in enumerate(rows):
            label = f"frame {frame['frame_id']} detections[{ordinal}]"
            validate_face_values(values, label, width, height)
            detection_id = stable_id(
                "face_detection", frame["frame_id"], ordinal, values
            )
            detections.append(
                {
                    "detection_id": detection_id,
                    "detection_ordinal": ordinal,
                    "box": {
                        "x": values[0],
                        "y": values[1],
                        "width": values[2],
                        "height": values[3],
                    },
                    "landmarks": [
                        {"x": values[index], "y": values[index + 1]}
                        for index in range(4, 14, 2)
                    ],
                    "detector_score": values[14],
                    "below_64px_width": values[2] < 64.0,
                }
            )
        artifact_frames.append(
            {
                "frame_id": frame["frame_id"],
                "shot_id": frame["shot_id"],
                "frame_index": frame["frame_index"],
                "timestamp_ms": frame["timestamp_ms"],
                "width": width,
                "height": height,
                "detections": detections,
            }
        )
        receipt_frames.append(
            {
                **frame,
                "detection_count": len(detections),
            }
        )
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "artifact_type": "frame_local_face_detections",
        "recording_id": order["recording_id"],
        "coordinate_system": "pixel_xywh_top_left",
        "detector": {
            "name": "OpenCV FaceDetectorYN YuNet FP32",
            "version": f"2023mar-sha256-{order['model']['artifact']['sha256'][:12]}",
            "recipe_id": order["parameters"]["recipe_id"],
        },
        "authority": {
            "embeddings_present": False,
            "identity_state": "unknown",
            "identity_label": None,
            "active_speaker_state": "unknown",
            "active_speaker_score": None,
            "publication": False,
        },
        "shots": order["shots"],
        "frames": artifact_frames,
    }
    return artifact, receipt_frames


def validate_tracker_compatibility(artifact: dict[str, Any]) -> None:
    """Require the tracker to accept the exact anonymous artifact unchanged."""

    try:
        tracker_module = importlib.import_module("shot_local_face_tracker")
    except ImportError:
        try:
            tracker_module = importlib.import_module("pipeline.shot_local_face_tracker")
        except ImportError as error:
            raise YuNetDetectorError(
                f"cannot load the tracker contract validator: {error}"
            ) from error
    try:
        normalized = tracker_module.validate_detection_artifact(artifact)
    except tracker_module.FaceTrackingError as error:
        raise YuNetDetectorError(
            f"generated detections fail tracker contract: {error}"
        ) from error
    if canonical_bytes(normalized) != canonical_bytes(artifact):
        raise YuNetDetectorError(
            "tracker normalization changed generated detections; refusing seal"
        )


def validate_completed_result(
    raw: object,
    *,
    expected_job_id: str,
    expected_key: str,
    expected_detection_sha256: str,
    expected_frame_count: int,
) -> dict[str, Any]:
    """Strictly validate security-critical receipt semantics before sealing.

    Draft 2020-12 validation remains a repository test because adding an
    unpinned host ``jsonschema`` package to inference would weaken the isolated
    runtime boundary. This validator mirrors the closed result shape and the
    authority invariants used by the JSON Schema.
    """

    value = object_value(raw, "generated result")
    exact_keys(
        value,
        "generated result",
        {
            "schema_version",
            "implementation_version",
            "stage",
            "status",
            "dry_run",
            "job_id",
            "work_order_sha256",
            "result_key",
            "result_path",
            "implementation",
            "policy",
            "runtime",
            "model",
            "parameters",
            "recording_id",
            "shots",
            "frames",
            "detections_artifact",
            "score_semantics",
            "authority",
        },
    )
    constants = {
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "stage": STAGE,
        "status": "completed",
        "dry_run": False,
        "job_id": expected_job_id,
        "result_key": expected_key,
        "score_semantics": "raw_model_score_not_calibrated_probability",
    }
    for key, expected in constants.items():
        if value[key] != expected or type(value[key]) is not type(expected):
            raise YuNetDetectorError(f"generated result.{key} is inconsistent")
    digest_value(value["work_order_sha256"], "generated result.work_order_sha256")
    string_value(value["result_path"], "generated result.result_path")
    identifier(value["recording_id"], "generated result.recording_id")
    frames = list_value(
        value["frames"],
        "generated result.frames",
        expected_frame_count,
        expected_frame_count,
    )
    for ordinal, raw_frame in enumerate(frames):
        frame = object_value(raw_frame, f"generated result.frames[{ordinal}]")
        exact_keys(
            frame,
            f"generated result.frames[{ordinal}]",
            {
                "frame_id",
                "shot_id",
                "frame_index",
                "timestamp_ms",
                "media_type",
                "input",
                "image",
                "detection_count",
            },
        )
        integer(
            frame["detection_count"],
            f"generated result.frames[{ordinal}].detection_count",
            0,
            MAX_DETECTIONS_PER_FRAME,
        )
    artifact = object_value(
        value["detections_artifact"], "generated result.detections_artifact"
    )
    exact_keys(
        artifact,
        "generated result.detections_artifact",
        {
            "artifact_kind",
            "path",
            "storage_uri",
            "sha256",
            "byte_count",
            "visibility",
        },
    )
    if (
        artifact["artifact_kind"] != "frame_local_face_detections_json"
        or artifact["sha256"] != expected_detection_sha256
        or artifact["visibility"] != "private"
    ):
        raise YuNetDetectorError("generated detections observation is inconsistent")
    authority = object_value(value["authority"], "generated result.authority")
    exact_keys(
        authority,
        "generated result.authority",
        {
            "embeddings_present",
            "identity_state",
            "identity_label",
            "identity_decision",
            "active_speaker_state",
            "active_speaker_score",
            "active_speaker_decision",
            "publication",
            "human_review_required",
        },
    )
    if authority != {
        "embeddings_present": False,
        "identity_state": "unknown",
        "identity_label": None,
        "identity_decision": False,
        "active_speaker_state": "unknown",
        "active_speaker_score": None,
        "active_speaker_decision": False,
        "publication": False,
        "human_review_required": True,
    }:
        raise YuNetDetectorError("generated result authority must remain unknown/false")
    return value


def result_layout(order: dict[str, Any], implementation: dict[str, Any]) -> tuple[str, Path]:
    key_payload = {"work_order": public_order(order), "implementation": implementation}
    result_key = sha256_bytes(canonical_bytes(key_payload))
    final_dir = (
        Path(order["output"]["root"])
        / "vision"
        / "yunet-face-detections"
        / result_key
    )
    return result_key, final_dir


def artifact_observation(path: Path, final_path: Path, kind: str) -> dict[str, Any]:
    body = stable_read(path, kind, MAX_RESULT_BYTES)
    return {
        "artifact_kind": kind,
        "path": str(final_path),
        "storage_uri": final_path.as_uri(),
        "sha256": sha256_bytes(body),
        "byte_count": len(body),
        "visibility": "private",
    }


def cleanup_staging(staging: Path) -> None:
    if not staging.exists():
        return
    for child in staging.rglob("*"):
        if child.is_file():
            child.chmod(0o600)
    for child in sorted(staging.rglob("*"), reverse=True):
        if child.is_dir():
            child.chmod(0o700)
    staging.chmod(0o700)
    shutil.rmtree(staging)


def write_sealed_file(path: Path, body: bytes) -> None:
    """Create, flush, and seal a result file without an overwrite path."""

    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise YuNetDetectorError(f"short write while creating {path}")
            view = view[written:]
        os.fchmod(descriptor, 0o400)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_publish_directory(staging: Path, final_dir: Path) -> None:
    """Atomically publish a directory without replacing an existing target.

    The bridge is intentionally Linux-only because its isolated OpenCV wheel is
    Linux-only. ``renameat2(RENAME_NOREPLACE)`` closes the empty-directory
    replacement race left by ``Path.rename``.
    """

    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise YuNetDetectorError("host libc lacks renameat2; refusing unsafe publish")
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        -100,
        os.fsencode(staging),
        -100,
        os.fsencode(final_dir),
        1,
    )
    if result != 0:
        error_number = ctypes.get_errno()
        if error_number in (errno.EEXIST, errno.ENOTEMPTY):
            raise YuNetDetectorError(
                "result directory appeared during execution; refusing reuse"
            )
        raise YuNetDetectorError(
            f"atomic result publication failed: {os.strerror(error_number)}"
        )


def run(order: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    implementation = implementation_observation()
    result_key, final_dir = result_layout(order, implementation)
    normalized_order = public_order(order)
    work_order_sha256 = sha256_bytes(canonical_bytes(normalized_order))
    if dry_run:
        return {
            "schema_version": SCHEMA_VERSION,
            "stage": STAGE,
            "status": "planned",
            "dry_run": True,
            "job_id": order["job_id"],
            "work_order_sha256": work_order_sha256,
            "result_key": result_key,
            "result_path": str(final_dir / "result.json"),
            "detections_path": str(final_dir / "detections.json"),
            "shot_count": len(order["shots"]),
            "frame_count": len(order["frames"]),
            "network_allowed": False,
            "embeddings_allowed": False,
            "identity_decision_allowed": False,
            "active_speaker_decision_allowed": False,
            "publication_authority": "none",
        }

    output_root = Path(order["output"]["root"])
    vision_root = output_root / "vision"
    result_parent = vision_root / "yunet-face-detections"
    for directory in (vision_root, result_parent):
        ensure_private_directory(directory, str(directory))
    if final_dir.exists() or final_dir.is_symlink():
        raise YuNetDetectorError("result directory already exists; refusing reuse")
    recheck_inputs(order)
    cv2, numpy, runtime_observation = load_runtime(order["runtime"])
    staging = result_parent / (
        f".staging-{result_key}-{os.getpid()}-{uuid.uuid4().hex}"
    )
    staging.mkdir(mode=0o700)
    try:
        model_copy = staging / ".captured-yunet.onnx"
        write_sealed_file(model_copy, order["_model_body"])
        if (
            sha256_bytes(
                stable_read(model_copy, "captured YuNet model", MAX_MODEL_BYTES)
            )
            != order["model"]["artifact"]["sha256"]
        ):
            raise YuNetDetectorError("captured YuNet model copy failed integrity check")
        detector = create_detector(cv2, model_copy, order["parameters"])
        artifact, receipt_frames = detect_frames(order, cv2, numpy, detector)
        model_copy.unlink()
        validate_tracker_compatibility(artifact)

        detections_path = staging / "detections.json"
        detections_body = pretty_bytes(artifact)
        if len(detections_body) > MAX_RESULT_BYTES:
            raise YuNetDetectorError("detections artifact exceeds bounded size")
        reparsed_detections = load_json_bytes(
            detections_body, "generated detections artifact"
        )
        if not isinstance(reparsed_detections, dict):
            raise YuNetDetectorError("generated detections artifact must be an object")
        validate_tracker_compatibility(reparsed_detections)
        write_sealed_file(detections_path, detections_body)
        detections_final = final_dir / "detections.json"
        detections_observation = artifact_observation(
            detections_path,
            detections_final,
            "frame_local_face_detections_json",
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
            "runtime": {**order["runtime"], "observation": runtime_observation},
            "model": order["model"],
            "parameters": order["parameters"],
            "recording_id": order["recording_id"],
            "shots": order["shots"],
            "frames": receipt_frames,
            "detections_artifact": detections_observation,
            "score_semantics": "raw_model_score_not_calibrated_probability",
            "authority": {
                "embeddings_present": False,
                "identity_state": "unknown",
                "identity_label": None,
                "identity_decision": False,
                "active_speaker_state": "unknown",
                "active_speaker_score": None,
                "active_speaker_decision": False,
                "publication": False,
                "human_review_required": True,
            },
        }
        result_path = staging / "result.json"
        result_body = pretty_bytes(result)
        if len(result_body) > MAX_RESULT_BYTES:
            raise YuNetDetectorError("result receipt exceeds bounded size")
        reparsed_result = validate_completed_result(
            load_json_bytes(result_body, "generated result receipt"),
            expected_job_id=order["job_id"],
            expected_key=result_key,
            expected_detection_sha256=detections_observation["sha256"],
            expected_frame_count=len(order["frames"]),
        )
        if canonical_bytes(reparsed_result) != canonical_bytes(result):
            raise YuNetDetectorError("generated result changed during JSON round trip")
        write_sealed_file(result_path, result_body)

        recheck_inputs(order)
        if implementation_observation() != implementation:
            raise YuNetDetectorError("detector implementation changed during execution")
        ensure_private_directory(result_parent, str(result_parent))
        if final_dir.exists() or final_dir.is_symlink():
            raise YuNetDetectorError("result directory appeared during execution")
        if (
            stable_read(detections_path, "sealed detections", MAX_RESULT_BYTES)
            != detections_body
        ):
            raise YuNetDetectorError("sealed detections bytes differ before publication")
        if stable_read(result_path, "sealed result", MAX_RESULT_BYTES) != result_body:
            raise YuNetDetectorError("sealed result bytes differ before publication")
        fsync_directory(staging)
        fsync_directory(result_parent)
        staging.chmod(0o500)
        atomic_publish_directory(staging, final_dir)
    except Exception:
        cleanup_staging(staging)
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
        path = resolved_regular_file(
            str(work_order_path),
            "work order",
            sealed=False,
            owner_private=False,
        )
        order = validate_work_order(
            load_json_bytes(stable_read(path, "work order", MAX_JSON_BYTES), "work order")
        )
        result = run(order, dry_run=args.dry_run)
    except (YuNetDetectorError, OSError, ValueError) as error:
        print(f"yunet-face-detector: {error}", file=sys.stderr)
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
            "detections_path": result["detections_artifact"]["path"],
            "shot_count": len(result["shots"]),
            "frame_count": len(result["frames"]),
            "detection_count": sum(frame["detection_count"] for frame in result["frames"]),
            "embeddings_present": False,
            "identity_decision": False,
            "active_speaker_decision": False,
            "publication": False,
        }
    print(json.dumps(receipt, allow_nan=False, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
