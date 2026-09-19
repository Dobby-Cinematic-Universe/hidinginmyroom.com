#!/usr/bin/env python3
"""Offline, private YuNet/SFace frame-candidate pilot.

The adapter consumes an explicit bounded list of sealed PNG frames.  It detects
faces with a hash-pinned YuNet model, stores aligned crops and SFace embeddings as
private artifacts, and computes raw cosine similarities only for explicitly named
frame pairs that each contain exactly one detection.  It never turns a source
context label into a biometric identity, emits a calibrated probability, chooses an
identity threshold, opens the corpus database, or uses the network.
"""

from __future__ import annotations

import argparse
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
IMPLEMENTATION_VERSION = "0.1.0"
STAGE = "face_candidate_yunet_sface"
MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_FRAMES = 32
MAX_DETECTIONS_PER_FRAME = 32
MAX_PIXELS_PER_FRAME = 1920 * 1080
MAX_ARTIFACT_BYTES = 64 * 1024 * 1024
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


class FaceCandidateError(RuntimeError):
    """A strict contract, integrity, runtime, or inference failure."""


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def pretty_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def stable_id(prefix: str, *parts: Any) -> str:
    return f"{prefix}_{sha256_bytes(canonical_bytes(list(parts)))[:32]}"


def exact_keys(value: dict[str, Any], label: str, keys: set[str]) -> None:
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    if missing or unknown:
        details: list[str] = []
        if missing:
            details.append(f"missing {missing}")
        if unknown:
            details.append(f"unknown {unknown}")
        raise FaceCandidateError(f"{label} has " + "; ".join(details))


def object_value(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise FaceCandidateError(f"{label} must be an object")
    return value


def list_value(value: object, label: str, minimum: int, maximum: int) -> list[Any]:
    if not isinstance(value, list) or not minimum <= len(value) <= maximum:
        raise FaceCandidateError(
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
        raise FaceCandidateError(f"{label} must be a non-empty bounded string")
    return value


def nullable_string(value: object, label: str, maximum: int = 4096) -> str | None:
    if value is None:
        return None
    return string_value(value, label, maximum)


def identifier(value: object, label: str) -> str:
    text = string_value(value, label, 128)
    if not ID_RE.fullmatch(text):
        raise FaceCandidateError(f"{label} contains unsupported characters")
    return text


def digest_value(value: object, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise FaceCandidateError(f"{label} must be a lowercase SHA-256")
    return value


def integer(value: object, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FaceCandidateError(f"{label} must be an integer")
    if not minimum <= value <= maximum:
        raise FaceCandidateError(f"{label} must be between {minimum} and {maximum}")
    return value


def number(value: object, label: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FaceCandidateError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise FaceCandidateError(f"{label} must be finite and in [{minimum}, {maximum}]")
    return result


def false_value(value: object, label: str) -> bool:
    if value is not False:
        raise FaceCandidateError(f"{label} must be false")
    return False


def null_value(value: object, label: str) -> None:
    if value is not None:
        raise FaceCandidateError(f"{label} must be null")
    return None


def resolved_regular_file(value: object, label: str, *, sealed: bool = True) -> Path:
    text = string_value(value, label)
    if "://" in text:
        raise FaceCandidateError(f"{label} must be a local path, not a URL")
    path = Path(text)
    if not path.is_absolute():
        raise FaceCandidateError(f"{label} must be absolute")
    try:
        mode = path.lstat().st_mode
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise FaceCandidateError(f"{label} is not a readable current file: {error}") from error
    if path != resolved or stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise FaceCandidateError(f"{label} must be a resolved regular file")
    if sealed and mode & 0o222:
        raise FaceCandidateError(f"{label} must be sealed read-only")
    return path


def resolved_directory(value: object, label: str, *, sealed: bool = True) -> Path:
    text = string_value(value, label)
    if "://" in text:
        raise FaceCandidateError(f"{label} must be a local path, not a URL")
    path = Path(text)
    if not path.is_absolute():
        raise FaceCandidateError(f"{label} must be absolute")
    try:
        mode = path.lstat().st_mode
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise FaceCandidateError(f"{label} is not a readable current directory: {error}") from error
    if path != resolved or stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise FaceCandidateError(f"{label} must be a resolved directory")
    if sealed and mode & 0o222:
        raise FaceCandidateError(f"{label} must be sealed read-only")
    return path


def validate_output_root(value: object) -> Path:
    text = string_value(value, "output.root")
    if "://" in text:
        raise FaceCandidateError("output.root must be a local path, not a URL")
    path = Path(text)
    if not path.is_absolute() or path == Path("/"):
        raise FaceCandidateError("output.root must be a specific absolute directory")
    if Path(os.path.normpath(str(path))) != path:
        raise FaceCandidateError("output.root must not contain traversal")
    for unsafe in (Path("/tmp"), Path("/var/tmp")):
        try:
            path.relative_to(unsafe)
        except ValueError:
            pass
        else:
            raise FaceCandidateError(f"output.root must not be under {unsafe}")
    parent = path
    while not parent.exists():
        if parent.parent == parent:
            raise FaceCandidateError("output.root has no existing parent")
        parent = parent.parent
    if parent.resolve(strict=True) != parent:
        raise FaceCandidateError("output.root must not traverse a symlinked parent")
    return path


def load_json(path: Path) -> Any:
    try:
        if path.stat().st_size > MAX_JSON_BYTES:
            raise FaceCandidateError(f"JSON exceeds {MAX_JSON_BYTES} bytes: {path}")
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FaceCandidateError(f"cannot read JSON {path}: {error}") from error


def stable_observe(
    path: Path, expected_sha256: str, expected_size: int, label: str
) -> dict[str, Any]:
    before = path.stat()
    identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    observed_sha = sha256_file(path)
    after = path.stat()
    if identity != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise FaceCandidateError(f"{label} changed while hashing")
    if observed_sha != expected_sha256 or before.st_size != expected_size:
        raise FaceCandidateError(f"{label} differs from its pinned digest/size")
    return {
        "path": str(path),
        "storage_uri": path.as_uri(),
        "sha256": observed_sha,
        "byte_count": before.st_size,
        "visibility": "private",
    }


def validate_file_pin(raw: object, label: str) -> tuple[dict[str, Any], Path]:
    value = object_value(raw, label)
    exact_keys(value, label, {"path", "expected_sha256", "expected_byte_count"})
    path = resolved_regular_file(value["path"], f"{label}.path")
    expected_sha = digest_value(value["expected_sha256"], f"{label}.expected_sha256")
    expected_size = integer(
        value["expected_byte_count"], f"{label}.expected_byte_count", 1, 1 << 40
    )
    return stable_observe(path, expected_sha, expected_size, label), path


def png_dimensions(path: Path) -> tuple[int, int]:
    with path.open("rb") as handle:
        header = handle.read(24)
    if len(header) != 24 or header[:8] != PNG_SIGNATURE or header[12:16] != b"IHDR":
        raise FaceCandidateError(f"input frame is not a supported PNG: {path}")
    width, height = struct.unpack(">II", header[16:24])
    if width < 1 or height < 1 or width * height > MAX_PIXELS_PER_FRAME:
        raise FaceCandidateError(f"input frame dimensions exceed the bounded profile: {path}")
    return width, height


def validate_policy(raw: object) -> dict[str, Any]:
    value = object_value(raw, "policy")
    exact_keys(
        value,
        "policy",
        {
            "visibility",
            "network_allowed",
            "publication_authority",
            "identity_decision_allowed",
            "human_review_required",
            "calibration_state",
            "calibrated_probability",
            "source_context_is_identity",
        },
    )
    if value["visibility"] != "private":
        raise FaceCandidateError("policy.visibility must be private")
    false_value(value["network_allowed"], "policy.network_allowed")
    if value["publication_authority"] != "none":
        raise FaceCandidateError("policy.publication_authority must be none")
    false_value(value["identity_decision_allowed"], "policy.identity_decision_allowed")
    if value["human_review_required"] is not True:
        raise FaceCandidateError("policy.human_review_required must be true")
    if value["calibration_state"] != "not_calibrated":
        raise FaceCandidateError("policy.calibration_state must be not_calibrated")
    null_value(value["calibrated_probability"], "policy.calibrated_probability")
    false_value(value["source_context_is_identity"], "policy.source_context_is_identity")
    return dict(value)


def validate_runtime(raw: object) -> dict[str, Any]:
    value = object_value(raw, "runtime")
    exact_keys(value, "runtime", {"module_root", "python", "opencv", "numpy"})
    module_root = resolved_directory(value["module_root"], "runtime.module_root")

    python_raw = object_value(value["python"], "runtime.python")
    exact_keys(
        python_raw,
        "runtime.python",
        {"executable", "expected_sha256", "expected_byte_count", "expected_version"},
    )
    python_path = resolved_regular_file(
        python_raw["executable"], "runtime.python.executable", sealed=False
    )
    python_observation = stable_observe(
        python_path,
        digest_value(python_raw["expected_sha256"], "runtime.python.expected_sha256"),
        integer(
            python_raw["expected_byte_count"],
            "runtime.python.expected_byte_count",
            1,
            1 << 30,
        ),
        "runtime.python",
    )
    expected_python_version = string_value(
        python_raw["expected_version"], "runtime.python.expected_version", 64
    )

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
            raise FaceCandidateError(
                f"runtime.{name} wheel/binary must be inside the isolated asset tree"
            ) from error
        packages[name] = {
            "wheel": wheel_observation,
            "binary": binary_observation,
            "expected_version": string_value(
                package_raw["expected_version"], f"runtime.{name}.expected_version", 64
            ),
        }
        if name == "opencv":
            packages[name]["expected_build_information_sha256"] = digest_value(
                package_raw["expected_build_information_sha256"],
                "runtime.opencv.expected_build_information_sha256",
            )
    return {
        "module_root": str(module_root),
        "python": {
            **python_observation,
            "expected_version": expected_python_version,
        },
        **packages,
    }


def validate_model(raw: object, label: str, expected_license: str) -> dict[str, Any]:
    value = object_value(raw, label)
    exact_keys(
        value,
        label,
        {"artifact", "license", "upstream_url", "upstream_commit", "license_expression"},
    )
    artifact_observation, _ = validate_file_pin(value["artifact"], f"{label}.artifact")
    license_observation, _ = validate_file_pin(value["license"], f"{label}.license")
    upstream_url = string_value(value["upstream_url"], f"{label}.upstream_url", 2048)
    if not upstream_url.startswith("https://github.com/opencv/opencv_zoo/"):
        raise FaceCandidateError(f"{label}.upstream_url must be an official OpenCV Zoo URL")
    commit = string_value(value["upstream_commit"], f"{label}.upstream_commit", 40)
    if not COMMIT_RE.fullmatch(commit) or commit not in upstream_url:
        raise FaceCandidateError(f"{label}.upstream_commit must be immutable and appear in URL")
    if value["license_expression"] != expected_license:
        raise FaceCandidateError(f"{label}.license_expression must be {expected_license}")
    return {
        "artifact": artifact_observation,
        "license": license_observation,
        "upstream_url": upstream_url,
        "upstream_commit": commit,
        "license_expression": expected_license,
    }


def validate_context(raw: object, label: str) -> dict[str, Any]:
    value = object_value(raw, label)
    exact_keys(
        value,
        label,
        {
            "context_role",
            "source_native_id",
            "source_url",
            "source_context_label",
            "basis",
            "human_identity_attestation",
        },
    )
    role = value["context_role"]
    if role not in {"source_context_anchor", "comparison_query"}:
        raise FaceCandidateError(f"{label}.context_role is unsupported")
    source_url = string_value(value["source_url"], f"{label}.source_url", 2048)
    if not source_url.startswith("https://"):
        raise FaceCandidateError(f"{label}.source_url must be HTTPS provenance text")
    context_label = nullable_string(
        value["source_context_label"], f"{label}.source_context_label", 128
    )
    if role == "comparison_query" and context_label is not None:
        raise FaceCandidateError(
            f"{label}.source_context_label must be null for comparison queries"
        )
    false_value(value["human_identity_attestation"], f"{label}.human_identity_attestation")
    return {
        "context_role": role,
        "source_native_id": identifier(
            value["source_native_id"], f"{label}.source_native_id"
        ),
        "source_url": source_url,
        "source_context_label": context_label,
        "basis": string_value(value["basis"], f"{label}.basis", 2048),
        "human_identity_attestation": False,
    }


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
            "models",
            "parameters",
            "frames",
            "comparisons",
            "output",
        },
    )
    if value["schema_version"] != SCHEMA_VERSION:
        raise FaceCandidateError("schema_version must be 1")
    policy = validate_policy(value["policy"])
    runtime = validate_runtime(value["runtime"])
    models_raw = object_value(value["models"], "models")
    exact_keys(models_raw, "models", {"yunet", "sface"})
    models = {
        "yunet": validate_model(models_raw["yunet"], "models.yunet", "MIT"),
        "sface": validate_model(
            models_raw["sface"], "models.sface", "Apache-2.0"
        ),
    }

    parameters_raw = object_value(value["parameters"], "parameters")
    exact_keys(
        parameters_raw,
        "parameters",
        {
            "score_threshold",
            "nms_threshold",
            "top_k",
            "threads",
            "max_frames",
            "max_detections_per_frame",
            "comparison_metric",
            "embedding_format",
        },
    )
    parameters = {
        "score_threshold": number(
            parameters_raw["score_threshold"], "parameters.score_threshold", 0.01, 1.0
        ),
        "nms_threshold": number(
            parameters_raw["nms_threshold"], "parameters.nms_threshold", 0.01, 1.0
        ),
        "top_k": integer(parameters_raw["top_k"], "parameters.top_k", 1, 5000),
        "threads": integer(parameters_raw["threads"], "parameters.threads", 1, 1),
        "max_frames": integer(
            parameters_raw["max_frames"], "parameters.max_frames", 1, MAX_FRAMES
        ),
        "max_detections_per_frame": integer(
            parameters_raw["max_detections_per_frame"],
            "parameters.max_detections_per_frame",
            1,
            MAX_DETECTIONS_PER_FRAME,
        ),
        "comparison_metric": parameters_raw["comparison_metric"],
        "embedding_format": parameters_raw["embedding_format"],
    }
    if parameters["comparison_metric"] != "cosine_similarity_raw_v1":
        raise FaceCandidateError(
            "parameters.comparison_metric must be cosine_similarity_raw_v1"
        )
    if parameters["embedding_format"] != "float32_little_endian_v1":
        raise FaceCandidateError(
            "parameters.embedding_format must be float32_little_endian_v1"
        )

    frame_rows = list_value(value["frames"], "frames", 1, parameters["max_frames"])
    frames: list[dict[str, Any]] = []
    frame_ids: set[str] = set()
    for ordinal, raw_frame in enumerate(frame_rows):
        label = f"frames[{ordinal}]"
        frame = object_value(raw_frame, label)
        exact_keys(
            frame,
            label,
            {
                "frame_id",
                "path",
                "expected_sha256",
                "expected_byte_count",
                "requested_timestamp_ms",
                "observed_timestamp_ms",
                "timestamp_drift_ms",
                "coordinate_system",
                "source_context",
            },
        )
        frame_id = identifier(frame["frame_id"], f"{label}.frame_id")
        if frame_id in frame_ids:
            raise FaceCandidateError("frame IDs must be unique")
        frame_ids.add(frame_id)
        path = resolved_regular_file(frame["path"], f"{label}.path")
        observation = stable_observe(
            path,
            digest_value(frame["expected_sha256"], f"{label}.expected_sha256"),
            integer(
                frame["expected_byte_count"],
                f"{label}.expected_byte_count",
                1,
                MAX_ARTIFACT_BYTES,
            ),
            label,
        )
        width, height = png_dimensions(path)
        frames.append(
            {
                "frame_id": frame_id,
                "ordinal": ordinal,
                "input": observation,
                "image": {"width": width, "height": height, "format": "png"},
                "requested_timestamp_ms": integer(
                    frame["requested_timestamp_ms"],
                    f"{label}.requested_timestamp_ms",
                    0,
                    7 * 24 * 60 * 60 * 1000,
                ),
                "observed_timestamp_ms": integer(
                    frame["observed_timestamp_ms"],
                    f"{label}.observed_timestamp_ms",
                    0,
                    7 * 24 * 60 * 60 * 1000,
                ),
                "timestamp_drift_ms": integer(
                    frame["timestamp_drift_ms"],
                    f"{label}.timestamp_drift_ms",
                    -10_000,
                    10_000,
                ),
                "coordinate_system": identifier(
                    frame["coordinate_system"], f"{label}.coordinate_system"
                ),
                "source_context": validate_context(
                    frame["source_context"], f"{label}.source_context"
                ),
            }
        )
        if (
            frames[-1]["observed_timestamp_ms"]
            - frames[-1]["requested_timestamp_ms"]
            != frames[-1]["timestamp_drift_ms"]
        ):
            raise FaceCandidateError(f"{label}.timestamp_drift_ms is inconsistent")

    comparison_rows = list_value(
        value["comparisons"], "comparisons", 0, MAX_FRAMES * MAX_FRAMES
    )
    comparisons: list[dict[str, Any]] = []
    comparison_ids: set[str] = set()
    for ordinal, raw_comparison in enumerate(comparison_rows):
        label = f"comparisons[{ordinal}]"
        comparison = object_value(raw_comparison, label)
        exact_keys(
            comparison,
            label,
            {
                "comparison_id",
                "left_frame_id",
                "right_frame_id",
                "candidate_coordinate_mapping",
            },
        )
        comparison_id = identifier(
            comparison["comparison_id"], f"{label}.comparison_id"
        )
        if comparison_id in comparison_ids:
            raise FaceCandidateError("comparison IDs must be unique")
        comparison_ids.add(comparison_id)
        left_id = identifier(comparison["left_frame_id"], f"{label}.left_frame_id")
        right_id = identifier(comparison["right_frame_id"], f"{label}.right_frame_id")
        if left_id == right_id or left_id not in frame_ids or right_id not in frame_ids:
            raise FaceCandidateError(f"{label} must reference two distinct known frames")
        mapping_raw = object_value(
            comparison["candidate_coordinate_mapping"],
            f"{label}.candidate_coordinate_mapping",
        )
        exact_keys(
            mapping_raw,
            f"{label}.candidate_coordinate_mapping",
            {
                "left_source_timestamp_ms",
                "right_artifact_timestamp_ms",
                "basis",
                "recording_relationship_decision",
            },
        )
        false_value(
            mapping_raw["recording_relationship_decision"],
            f"{label}.candidate_coordinate_mapping.recording_relationship_decision",
        )
        comparisons.append(
            {
                "comparison_id": comparison_id,
                "left_frame_id": left_id,
                "right_frame_id": right_id,
                "candidate_coordinate_mapping": {
                    "left_source_timestamp_ms": integer(
                        mapping_raw["left_source_timestamp_ms"],
                        f"{label}.candidate_coordinate_mapping.left_source_timestamp_ms",
                        0,
                        7 * 24 * 60 * 60 * 1000,
                    ),
                    "right_artifact_timestamp_ms": integer(
                        mapping_raw["right_artifact_timestamp_ms"],
                        f"{label}.candidate_coordinate_mapping.right_artifact_timestamp_ms",
                        0,
                        7 * 24 * 60 * 60 * 1000,
                    ),
                    "basis": string_value(
                        mapping_raw["basis"],
                        f"{label}.candidate_coordinate_mapping.basis",
                        2048,
                    ),
                    "recording_relationship_decision": False,
                },
            }
        )

    output_raw = object_value(value["output"], "output")
    exact_keys(output_raw, "output", {"root"})
    return {
        "schema_version": SCHEMA_VERSION,
        "job_id": identifier(value["job_id"], "job_id"),
        "policy": policy,
        "runtime": runtime,
        "models": models,
        "parameters": parameters,
        "frames": frames,
        "comparisons": comparisons,
        "output": {"root": str(validate_output_root(output_raw["root"]))},
    }


def implementation_observation() -> dict[str, Any]:
    path = Path(__file__).resolve(strict=True)
    return {
        "path": str(path),
        "sha256": sha256_file(path),
        "byte_count": path.stat().st_size,
        "implementation_version": IMPLEMENTATION_VERSION,
    }


def load_runtime(runtime: dict[str, Any]) -> tuple[Any, Any, dict[str, Any]]:
    expected_python = runtime["python"]["expected_version"]
    actual_python = ".".join(str(value) for value in sys.version_info[:3])
    if actual_python != expected_python or Path(sys.executable).resolve() != Path(
        runtime["python"]["path"]
    ):
        raise FaceCandidateError("current Python executable/version differs from its pin")

    if "cv2" in sys.modules or "numpy" in sys.modules:
        raise FaceCandidateError("cv2/numpy must not be imported before isolated runtime load")
    os.environ["OPENCV_FOR_THREADS_NUM"] = "1"
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    sys.path.insert(0, runtime["module_root"])
    try:
        numpy = importlib.import_module("numpy")
        cv2 = importlib.import_module("cv2")
    except Exception as error:
        raise FaceCandidateError(f"cannot load isolated OpenCV runtime: {error}") from error

    module_root = Path(runtime["module_root"])
    for module, name in ((numpy, "numpy"), (cv2, "opencv")):
        try:
            Path(module.__file__).resolve(strict=True).relative_to(module_root)
        except (AttributeError, OSError, ValueError) as error:
            raise FaceCandidateError(f"{name} did not load from the pinned module root") from error
        if module.__version__ != runtime[name]["expected_version"]:
            raise FaceCandidateError(f"{name} version differs from its pin")

    cv2_binary = Path(runtime["opencv"]["binary"]["path"])
    numpy_binary = Path(runtime["numpy"]["binary"]["path"])
    stable_observe(
        cv2_binary,
        runtime["opencv"]["binary"]["sha256"],
        runtime["opencv"]["binary"]["byte_count"],
        "loaded OpenCV binary",
    )
    stable_observe(
        numpy_binary,
        runtime["numpy"]["binary"]["sha256"],
        runtime["numpy"]["binary"]["byte_count"],
        "loaded NumPy binary",
    )
    build_hash = sha256_bytes(cv2.getBuildInformation().encode("utf-8"))
    if build_hash != runtime["opencv"]["expected_build_information_sha256"]:
        raise FaceCandidateError("OpenCV build information differs from its pin")
    cv2.setNumThreads(1)
    if hasattr(cv2, "ocl"):
        cv2.ocl.setUseOpenCL(False)
    if cv2.getNumThreads() != 1:
        raise FaceCandidateError("OpenCV did not accept the one-thread limit")
    if not hasattr(cv2, "FaceDetectorYN_create") or not hasattr(
        cv2, "FaceRecognizerSF_create"
    ):
        raise FaceCandidateError("OpenCV runtime lacks YuNet/SFace APIs")
    return cv2, numpy, {
        "python_version": actual_python,
        "opencv_version": cv2.__version__,
        "numpy_version": numpy.__version__,
        "opencv_build_information_sha256": build_hash,
        "opencv_threads": cv2.getNumThreads(),
        "opencl_enabled": bool(cv2.ocl.useOpenCL()) if hasattr(cv2, "ocl") else False,
        "network_used": False,
    }


def round_float(value: float) -> float:
    result = round(float(value), 8)
    if not math.isfinite(result):
        raise FaceCandidateError("model emitted a non-finite numeric value")
    return result


def cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or len(left) != len(right):
        raise FaceCandidateError("embedding dimensions must match and be non-empty")
    dot = sum(a * b for a, b in zip(left, right, strict=True))
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    if left_norm <= 0 or right_norm <= 0:
        raise FaceCandidateError("embedding norm must be positive")
    return max(-1.0, min(1.0, dot / (left_norm * right_norm)))


def result_layout(order: dict[str, Any], implementation: dict[str, Any]) -> tuple[str, Path]:
    semantic = {
        **order,
        "runtime": order["runtime"],
        "implementation": implementation,
    }
    result_key = sha256_bytes(canonical_bytes(semantic))
    path = (
        Path(order["output"]["root"])
        / "vision"
        / "face-candidates"
        / "results"
        / result_key
    )
    return result_key, path


def artifact_record(path: Path, kind: str) -> dict[str, Any]:
    return {
        "artifact_kind": kind,
        "path": str(path),
        "storage_uri": path.as_uri(),
        "sha256": sha256_file(path),
        "byte_count": path.stat().st_size,
        "visibility": "private",
    }


def infer(
    order: dict[str, Any], cv2: Any, numpy: Any, final_dir: Path, staging: Path
) -> tuple[list[dict[str, Any]], dict[str, list[float]]]:
    parameters = order["parameters"]
    detector = cv2.FaceDetectorYN_create(
        order["models"]["yunet"]["artifact"]["path"],
        "",
        (320, 320),
        parameters["score_threshold"],
        parameters["nms_threshold"],
        parameters["top_k"],
    )
    recognizer = cv2.FaceRecognizerSF_create(
        order["models"]["sface"]["artifact"]["path"], ""
    )
    crops_dir = staging / "crops"
    embeddings_dir = staging / "embeddings"
    crops_dir.mkdir(parents=True)
    embeddings_dir.mkdir(parents=True)
    frame_results: list[dict[str, Any]] = []
    vectors: dict[str, list[float]] = {}

    for frame in order["frames"]:
        image = cv2.imread(frame["input"]["path"], cv2.IMREAD_COLOR)
        if image is None or len(image.shape) != 3 or image.shape[2] != 3:
            raise FaceCandidateError(f"OpenCV could not decode frame {frame['frame_id']}")
        height, width = int(image.shape[0]), int(image.shape[1])
        if (width, height) != (frame["image"]["width"], frame["image"]["height"]):
            raise FaceCandidateError("OpenCV dimensions differ from the sealed PNG header")
        detector.setInputSize((width, height))
        _, faces = detector.detect(image)
        rows: list[Any] = [] if faces is None else list(faces)
        rows.sort(
            key=lambda row: (
                -float(row[14]),
                float(row[0]),
                float(row[1]),
                float(row[2]),
                float(row[3]),
            )
        )
        if len(rows) > parameters["max_detections_per_frame"]:
            raise FaceCandidateError("detector count exceeds max_detections_per_frame")
        detections: list[dict[str, Any]] = []
        for ordinal, face in enumerate(rows):
            values = [round_float(value) for value in face.tolist()]
            if len(values) != 15:
                raise FaceCandidateError("YuNet detection row must contain 15 values")
            x, y, box_width, box_height = values[:4]
            if box_width <= 0 or box_height <= 0:
                raise FaceCandidateError("YuNet emitted a non-positive face box")
            detection_id = stable_id(
                "face_detection", frame["frame_id"], ordinal, values
            )
            aligned = recognizer.alignCrop(image, face)
            if aligned is None or len(aligned.shape) != 3:
                raise FaceCandidateError("SFace alignment failed")
            feature = recognizer.feature(aligned)
            vector_array = numpy.asarray(feature, dtype="<f4").reshape(-1)
            if vector_array.size < 16 or not bool(numpy.isfinite(vector_array).all()):
                raise FaceCandidateError("SFace emitted an invalid embedding")
            vector = [float(item) for item in vector_array.tolist()]
            vector_norm = math.sqrt(sum(item * item for item in vector))
            if vector_norm <= 0:
                raise FaceCandidateError("SFace emitted a zero embedding")

            crop_name = f"{frame['ordinal']:04d}-{ordinal:02d}-{detection_id}.png"
            embedding_name = f"{frame['ordinal']:04d}-{ordinal:02d}-{detection_id}.f32le"
            crop_stage = crops_dir / crop_name
            embedding_stage = embeddings_dir / embedding_name
            ok, encoded = cv2.imencode(
                ".png", aligned, [cv2.IMWRITE_PNG_COMPRESSION, 9]
            )
            if not ok:
                raise FaceCandidateError("OpenCV failed to encode aligned crop")
            crop_stage.write_bytes(encoded.tobytes())
            embedding_stage.write_bytes(vector_array.tobytes(order="C"))
            crop_final = final_dir / "crops" / crop_name
            embedding_final = final_dir / "embeddings" / embedding_name
            crop_artifact = artifact_record(crop_stage, "aligned_face_crop_png")
            embedding_artifact = artifact_record(
                embedding_stage, "sface_embedding_float32_little_endian"
            )
            crop_artifact.update(
                {"path": str(crop_final), "storage_uri": crop_final.as_uri()}
            )
            embedding_artifact.update(
                {"path": str(embedding_final), "storage_uri": embedding_final.as_uri()}
            )
            vectors[detection_id] = vector
            detections.append(
                {
                    "detection_id": detection_id,
                    "ordinal": ordinal,
                    "bounding_box_pixels": {
                        "x": x,
                        "y": y,
                        "width": box_width,
                        "height": box_height,
                    },
                    "landmarks_pixels": [
                        {"x": values[index], "y": values[index + 1]}
                        for index in range(4, 14, 2)
                    ],
                    "raw_detector_score": values[14],
                    "detector_score_semantics": "raw_model_score_not_calibrated_probability",
                    "calibration_state": "not_calibrated",
                    "calibrated_probability": None,
                    "identity_label": None,
                    "identity_decision": False,
                    "aligned_crop": crop_artifact,
                    "embedding": {
                        **embedding_artifact,
                        "dimension": int(vector_array.size),
                        "dtype": "float32",
                        "byte_order": "little",
                        "l2_norm": round_float(vector_norm),
                    },
                }
            )
        eligibility = (
            "single_detection_candidate"
            if len(detections) == 1
            else "abstained_no_detection"
            if len(detections) == 0
            else "abstained_multiple_detections"
        )
        frame_results.append(
            {
                **frame,
                "detections": detections,
                "comparison_eligibility": eligibility,
                "identity_decision": False,
            }
        )
    return frame_results, vectors


def compare_frames(
    order: dict[str, Any],
    frames: list[dict[str, Any]],
    vectors: dict[str, list[float]],
) -> list[dict[str, Any]]:
    by_id = {frame["frame_id"]: frame for frame in frames}
    output: list[dict[str, Any]] = []
    for comparison in order["comparisons"]:
        left = by_id[comparison["left_frame_id"]]
        right = by_id[comparison["right_frame_id"]]
        left_detections = left["detections"]
        right_detections = right["detections"]
        base = {
            **comparison,
            "calibration_state": "not_calibrated",
            "calibrated_probability": None,
            "threshold_decision": None,
            "identity_label": None,
            "identity_decision": False,
            "source_context_bridge": {
                "left_source_context_label": left["source_context"][
                    "source_context_label"
                ],
                "right_source_context_label": right["source_context"][
                    "source_context_label"
                ],
                "label_forwarded_by_model": False,
                "human_identity_attestation": False,
            },
            "warning": "Raw SFace cosine similarity is an uncalibrated private review signal, not a probability or identity decision.",
        }
        if len(left_detections) != 1 or len(right_detections) != 1:
            output.append(
                {
                    **base,
                    "state": "abstained_detection_cardinality",
                    "left_detection_id": None,
                    "right_detection_id": None,
                    "raw_cosine_similarity": None,
                }
            )
            continue
        left_detection = left_detections[0]["detection_id"]
        right_detection = right_detections[0]["detection_id"]
        similarity = cosine_similarity(
            vectors[left_detection], vectors[right_detection]
        )
        output.append(
            {
                **base,
                "state": "candidate_only_single_detection_each",
                "left_detection_id": left_detection,
                "right_detection_id": right_detection,
                "raw_cosine_similarity": round_float(similarity),
            }
        )
    return output


def seal_tree(path: Path) -> None:
    for child in path.rglob("*"):
        if child.is_file():
            child.chmod(0o400)
    for child in sorted(path.rglob("*"), reverse=True):
        if child.is_dir():
            child.chmod(0o500)
    path.chmod(0o500)


def recheck_normalized_inputs(order: dict[str, Any]) -> None:
    observations: list[tuple[dict[str, Any], str]] = [
        (order["runtime"]["python"], "runtime Python"),
        (order["runtime"]["opencv"]["wheel"], "OpenCV wheel"),
        (order["runtime"]["opencv"]["binary"], "OpenCV binary"),
        (order["runtime"]["numpy"]["wheel"], "NumPy wheel"),
        (order["runtime"]["numpy"]["binary"], "NumPy binary"),
        (order["models"]["yunet"]["artifact"], "YuNet model"),
        (order["models"]["yunet"]["license"], "YuNet license"),
        (order["models"]["sface"]["artifact"], "SFace model"),
        (order["models"]["sface"]["license"], "SFace license"),
    ]
    observations.extend((frame["input"], f"frame {frame['frame_id']}") for frame in order["frames"])
    for observation, label in observations:
        path = resolved_regular_file(
            observation["path"], label, sealed=label != "runtime Python"
        )
        stable_observe(
            path,
            observation["sha256"],
            observation["byte_count"],
            label,
        )


def run(order: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    implementation = implementation_observation()
    result_key, final_dir = result_layout(order, implementation)
    work_order_sha = sha256_bytes(canonical_bytes(order))
    if dry_run:
        return {
            "schema_version": SCHEMA_VERSION,
            "stage": STAGE,
            "status": "planned",
            "dry_run": True,
            "job_id": order["job_id"],
            "work_order_sha256": work_order_sha,
            "result_key": result_key,
            "result_path": str(final_dir / "result.json"),
            "frame_count": len(order["frames"]),
            "comparison_count": len(order["comparisons"]),
            "network_allowed": False,
            "publication_authority": "none",
            "identity_decision_allowed": False,
        }
    if final_dir.exists():
        raise FaceCandidateError(
            "result directory already exists; this pilot fails closed instead of silently reusing biometric output"
        )

    cv2, numpy, runtime_observation = load_runtime(order["runtime"])
    output_root = Path(order["output"]["root"])
    staging_parent = output_root / "vision" / "face-candidates" / ".staging"
    staging_parent.mkdir(parents=True, exist_ok=True)
    staging = staging_parent / f"{result_key}-{os.getpid()}-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    try:
        frame_results, vectors = infer(order, cv2, numpy, final_dir, staging)
        comparisons = compare_frames(order, frame_results, vectors)
        result = {
            "schema_version": SCHEMA_VERSION,
            "implementation_version": IMPLEMENTATION_VERSION,
            "stage": STAGE,
            "status": "completed",
            "dry_run": False,
            "job_id": order["job_id"],
            "work_order_sha256": work_order_sha,
            "result_key": result_key,
            "result_path": str(final_dir / "result.json"),
            "implementation": implementation,
            "policy": order["policy"],
            "runtime": {**order["runtime"], "observation": runtime_observation},
            "models": order["models"],
            "parameters": order["parameters"],
            "frames": frame_results,
            "comparisons": comparisons,
            "authority": {
                "publication": False,
                "identity_decision": False,
                "recording_relationship_decision": False,
                "human_review_required": True,
            },
        }
        result_path = staging / "result.json"
        result_path.write_bytes(pretty_bytes(result))
        if result_path.stat().st_size > MAX_JSON_BYTES:
            raise FaceCandidateError("result JSON exceeds bounded size")

        # Recheck every immutable input after inference, including model/runtime bytes.
        recheck_normalized_inputs(order)
        if implementation_observation() != implementation:
            raise FaceCandidateError("adapter implementation changed during inference")
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staging, final_dir)
        seal_tree(final_dir)
        return result
    except Exception:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Offline private YuNet/SFace frame-candidate pilot"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("validate", "run"):
        command = subparsers.add_parser(name)
        command.add_argument("--work-order", required=True)
        if name == "run":
            command.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    try:
        work_order_path = resolved_regular_file(
            args.work_order, "--work-order", sealed=False
        )
        order = validate_work_order(load_json(work_order_path))
        if args.command == "validate":
            cv2, numpy, observation = load_runtime(order["runtime"])
            # Constructing both networks catches malformed/incompatible model bytes
            # without consuming a corpus frame.
            cv2.FaceDetectorYN_create(
                order["models"]["yunet"]["artifact"]["path"],
                "",
                (320, 320),
                order["parameters"]["score_threshold"],
                order["parameters"]["nms_threshold"],
                order["parameters"]["top_k"],
            )
            cv2.FaceRecognizerSF_create(
                order["models"]["sface"]["artifact"]["path"], ""
            )
            del cv2, numpy
            print(
                json.dumps(
                    {
                        "status": "valid",
                        "work_order_sha256": sha256_bytes(canonical_bytes(order)),
                        "runtime_observation": observation,
                        "network_used": False,
                    },
                    sort_keys=True,
                )
            )
            return 0
        result = run(order, dry_run=args.dry_run)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    except (FaceCandidateError, OSError, ValueError) as error:
        print(f"face-candidate-adapter: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
