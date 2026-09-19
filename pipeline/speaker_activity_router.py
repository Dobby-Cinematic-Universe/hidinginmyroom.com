#!/usr/bin/env python3
"""Deterministic, offline speaker/face/active-speaker workload router.

This program performs no ML identity inference.  It binds a route plan to completed
preprocessing and ASR envelopes plus an explicitly reviewed hint document.  A narrow
public Daniel label can only forward a complete-recording human source/speaker
attestation; the router never derives that identity itself.  Pinned downstream
capabilities are only checked for immutable local tool/model/calibration files; the
router never executes them.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.2.1"
STAGE = "speaker_activity_router"
MAX_JSON_BYTES = 512 * 1024 * 1024
MAX_INTERVALS = 200_000
MAX_TASKS = 1_000_000
MAX_DURATION_MS = 7 * 24 * 60 * 60 * 1_000
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
ZONED_TIME_RE = re.compile(r"(?:Z|[+-][0-9]{2}:[0-9]{2})$")
CAPABILITY_TASKS = {
    "diarization": "overlap_aware_diarization",
    "face_tracking": "face_tracking",
    "active_speaker": "active_speaker_association",
}
ORIGIN_REVIEW_TASKS = {
    "playback_voice": "playback_voice_review",
    "reaction_insert_voice": "reaction_insert_review",
    "tts_voice": "tts_voice_review",
    "synthetic_voice": "synthetic_voice_review",
}
PUBLIC_DANIEL_LABEL = "Daniel"
PUBLIC_DANIEL_BASIS = "confirmed_daniel_source_solo_presumption"
CONFIRMED_DANIEL_SOURCE = "confirmed_daniel_owned_source"


class RoutingError(RuntimeError):
    """The route plan failed a strict contract or integrity check."""


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


def stable_id(prefix: str, *parts: Any) -> str:
    return f"{prefix}_{sha256_bytes(canonical_bytes(list(parts)))[:32]}"


def exact_keys(value: dict[str, Any], label: str, keys: set[str]) -> None:
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    details: list[str] = []
    if missing:
        details.append(f"missing {missing}")
    if unknown:
        details.append(f"unknown {unknown}")
    if details:
        raise RoutingError(f"{label} has " + "; ".join(details))


def object_value(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RoutingError(f"{label} must be an object")
    return value


def string_value(value: Any, label: str, maximum: int = 4_096) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum or "\x00" in value:
        raise RoutingError(f"{label} must be a non-empty bounded string")
    return value


def identifier(value: Any, label: str) -> str:
    text = string_value(value, label, 256)
    if not ID_RE.fullmatch(text):
        raise RoutingError(f"{label} contains unsupported characters")
    return text


def sha256_value(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise RoutingError(f"{label} must be a lowercase SHA-256")
    return value


def integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RoutingError(f"{label} must be an integer")
    if not minimum <= value <= maximum:
        raise RoutingError(f"{label} must be between {minimum} and {maximum}")
    return value


def boolean(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise RoutingError(f"{label} must be boolean")
    return value


def enum_value(value: Any, label: str, choices: set[str]) -> str:
    if not isinstance(value, str) or value not in choices:
        raise RoutingError(f"{label} must be one of {sorted(choices)}")
    return value


def zoned_time(value: Any, label: str) -> str:
    text = string_value(value, label, 100)
    if not ZONED_TIME_RE.search(text):
        raise RoutingError(f"{label} must include Z or an explicit UTC offset")
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    except ValueError as error:
        raise RoutingError(f"{label} must be an ISO-8601 date-time") from error
    if parsed.tzinfo is None:
        raise RoutingError(f"{label} must include a time-zone offset")
    return text


def absolute_file(value: Any, label: str) -> Path:
    text = string_value(value, label)
    if "://" in text:
        raise RoutingError(f"{label} must be a local path, not a URL")
    path = Path(text)
    if not path.is_absolute():
        raise RoutingError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=True)
        link_stat = path.lstat()
    except (FileNotFoundError, OSError) as error:
        raise RoutingError(f"{label} is not a readable current file: {error}") from error
    if resolved != path or path.is_symlink() or not path.is_file():
        raise RoutingError(f"{label} must be a resolved regular file without symlinks")
    if link_stat.st_size > MAX_JSON_BYTES and label.endswith("result.path"):
        raise RoutingError(f"{label} exceeds the {MAX_JSON_BYTES}-byte input limit")
    return path


def absolute_output(value: Any) -> Path:
    text = string_value(value, "output.result_path")
    if "://" in text:
        raise RoutingError("output.result_path must be a local path")
    path = Path(text)
    if not path.is_absolute() or path == Path("/"):
        raise RoutingError("output.result_path must be a specific absolute path")
    normalized = Path(os.path.normpath(str(path)))
    if normalized != path or path.name in ("", ".", ".."):
        raise RoutingError("output.result_path must not contain dot traversal")
    for unsafe in (Path("/tmp"), Path("/var/tmp")):
        try:
            path.relative_to(unsafe)
        except ValueError:
            pass
        else:
            raise RoutingError(f"output.result_path may not be under {unsafe}")
    parent = path.parent
    while not parent.exists():
        if parent.parent == parent:
            raise RoutingError("output.result_path has no existing parent")
        parent = parent.parent
    if parent.resolve(strict=True) != parent:
        raise RoutingError("output.result_path must not traverse a symlinked parent")
    if path.exists():
        if path.is_symlink() or path.resolve(strict=True) != path:
            raise RoutingError("output.result_path must not be a symlink")
        if not path.is_file():
            raise RoutingError("output.result_path must be a file or a new path")
    return path


def stable_read(path: Path, label: str) -> bytes:
    before_path = path.stat()
    try:
        with path.open("rb") as handle:
            before_fd = os.fstat(handle.fileno())
            identity = (
                before_fd.st_dev,
                before_fd.st_ino,
                before_fd.st_size,
                before_fd.st_mtime_ns,
            )
            if identity != (
                before_path.st_dev,
                before_path.st_ino,
                before_path.st_size,
                before_path.st_mtime_ns,
            ):
                raise RoutingError(f"{label} changed while opening")
            body = handle.read()
            after_fd = os.fstat(handle.fileno())
    except OSError as error:
        raise RoutingError(f"cannot read {label}: {error}") from error
    after_path = path.stat()
    if identity != (
        after_fd.st_dev,
        after_fd.st_ino,
        after_fd.st_size,
        after_fd.st_mtime_ns,
    ) or identity != (
        after_path.st_dev,
        after_path.st_ino,
        after_path.st_size,
        after_path.st_mtime_ns,
    ):
        raise RoutingError(f"{label} changed while reading")
    return body


def stable_hash(path: Path, label: str) -> tuple[str, int]:
    """Hash a potentially large pinned artifact without loading it into memory."""
    before_path = path.stat()
    digest = hashlib.sha256()
    byte_count = 0
    try:
        with path.open("rb") as handle:
            before_fd = os.fstat(handle.fileno())
            identity = (
                before_fd.st_dev,
                before_fd.st_ino,
                before_fd.st_size,
                before_fd.st_mtime_ns,
            )
            if identity != (
                before_path.st_dev,
                before_path.st_ino,
                before_path.st_size,
                before_path.st_mtime_ns,
            ):
                raise RoutingError(f"{label} changed while opening")
            while chunk := handle.read(8 * 1024 * 1024):
                digest.update(chunk)
                byte_count += len(chunk)
            after_fd = os.fstat(handle.fileno())
    except OSError as error:
        raise RoutingError(f"cannot hash {label}: {error}") from error
    after_path = path.stat()
    if byte_count != identity[2] or identity != (
        after_fd.st_dev,
        after_fd.st_ino,
        after_fd.st_size,
        after_fd.st_mtime_ns,
    ) or identity != (
        after_path.st_dev,
        after_path.st_ino,
        after_path.st_size,
        after_path.st_mtime_ns,
    ):
        raise RoutingError(f"{label} changed while hashing")
    return digest.hexdigest(), byte_count


def read_observed(reference: dict[str, Any], label: str) -> tuple[Path, bytes, str]:
    path = absolute_file(reference["path"], f"{label}.path")
    expected = sha256_value(reference["expected_sha256"], f"{label}.expected_sha256")
    if path.stat().st_size > MAX_JSON_BYTES:
        raise RoutingError(f"{label} exceeds the {MAX_JSON_BYTES}-byte JSON limit")
    body = stable_read(path, label)
    observed = sha256_bytes(body)
    if observed != expected:
        raise RoutingError(f"{label} SHA-256 mismatch: expected {expected}, observed {observed}")
    return path, body, observed


def observe_pinned_file(reference: dict[str, Any], label: str) -> dict[str, Any]:
    path = absolute_file(reference["path"], f"{label}.path")
    expected = sha256_value(reference["expected_sha256"], f"{label}.expected_sha256")
    observed, byte_count = stable_hash(path, label)
    if observed != expected:
        raise RoutingError(f"{label} SHA-256 mismatch: expected {expected}, observed {observed}")
    return {"path": str(path), "sha256": observed, "byte_count": byte_count}


def json_body(body: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RoutingError(f"{label} is not valid UTF-8 JSON: {error}") from error
    return object_value(value, label)


def validate_reference(raw: Any, label: str) -> dict[str, Any]:
    value = object_value(raw, label)
    exact_keys(value, label, {"path", "expected_sha256"})
    return {
        "path": str(absolute_file(value["path"], f"{label}.path")),
        "expected_sha256": sha256_value(
            value["expected_sha256"], f"{label}.expected_sha256"
        ),
    }


def validate_recording(raw: Any) -> dict[str, Any]:
    value = object_value(raw, "recording")
    exact_keys(value, "recording", {"recording_id", "duration_ms"})
    return {
        "recording_id": identifier(value["recording_id"], "recording.recording_id"),
        "duration_ms": integer(
            value["duration_ms"], "recording.duration_ms", 1, MAX_DURATION_MS
        ),
    }


def validate_inputs(raw: Any) -> dict[str, Any]:
    value = object_value(raw, "inputs")
    exact_keys(
        value,
        "inputs",
        {
            "preprocess_result",
            "asr_result",
            "normalized_audio_sha256",
            "proxy_video_sha256",
            "glossary_sha256",
        },
    )
    proxy = value["proxy_video_sha256"]
    glossary = value["glossary_sha256"]
    return {
        "preprocess_result": validate_reference(
            value["preprocess_result"], "inputs.preprocess_result"
        ),
        "asr_result": validate_reference(value["asr_result"], "inputs.asr_result"),
        "normalized_audio_sha256": sha256_value(
            value["normalized_audio_sha256"], "inputs.normalized_audio_sha256"
        ),
        "proxy_video_sha256": None
        if proxy is None
        else sha256_value(proxy, "inputs.proxy_video_sha256"),
        "glossary_sha256": None
        if glossary is None
        else sha256_value(glossary, "inputs.glossary_sha256"),
    }


def validate_file_pin(raw: Any, label: str) -> dict[str, Any]:
    return validate_reference(raw, label)


def validate_capability(raw: Any, name: str) -> dict[str, Any]:
    value = object_value(raw, f"capabilities.{name}")
    task = CAPABILITY_TASKS[name]
    status = value.get("status")
    if status == "unconfigured":
        exact_keys(value, f"capabilities.{name}", {"status", "task", "reason"})
        if value["task"] != task:
            raise RoutingError(f"capabilities.{name}.task must be {task}")
        return {
            "status": "unconfigured",
            "task": task,
            "reason": string_value(value["reason"], f"capabilities.{name}.reason", 500),
        }
    if status != "pinned":
        raise RoutingError(f"capabilities.{name}.status must be pinned or unconfigured")
    exact_keys(
        value,
        f"capabilities.{name}",
        {"status", "task", "tool", "model", "calibration", "requirements"},
    )
    if value["task"] != task:
        raise RoutingError(f"capabilities.{name}.task must be {task}")
    tool = object_value(value["tool"], f"capabilities.{name}.tool")
    exact_keys(tool, f"capabilities.{name}.tool", {"name", "version", "file"})
    model = object_value(value["model"], f"capabilities.{name}.model")
    exact_keys(
        model,
        f"capabilities.{name}.model",
        {"model_id", "revision", "manifest", "weights"},
    )
    calibration = object_value(
        value["calibration"], f"capabilities.{name}.calibration"
    )
    exact_keys(
        calibration,
        f"capabilities.{name}.calibration",
        {"method", "artifact"},
    )
    requirements = object_value(
        value["requirements"], f"capabilities.{name}.requirements"
    )
    exact_keys(
        requirements,
        f"capabilities.{name}.requirements",
        {"cpu_threads", "memory_mb", "gpu_required", "gpu_memory_mb"},
    )
    return {
        "status": "pinned",
        "task": task,
        "tool": {
            "name": string_value(tool["name"], f"capabilities.{name}.tool.name", 200),
            "version": string_value(
                tool["version"], f"capabilities.{name}.tool.version", 200
            ),
            "file": validate_file_pin(tool["file"], f"capabilities.{name}.tool.file"),
        },
        "model": {
            "model_id": identifier(
                model["model_id"], f"capabilities.{name}.model.model_id"
            ),
            "revision": string_value(
                model["revision"], f"capabilities.{name}.model.revision", 500
            ),
            "manifest": validate_file_pin(
                model["manifest"], f"capabilities.{name}.model.manifest"
            ),
            "weights": validate_file_pin(
                model["weights"], f"capabilities.{name}.model.weights"
            ),
        },
        "calibration": {
            "method": string_value(
                calibration["method"], f"capabilities.{name}.calibration.method", 500
            ),
            "artifact": validate_file_pin(
                calibration["artifact"], f"capabilities.{name}.calibration.artifact"
            ),
        },
        "requirements": {
            "cpu_threads": integer(
                requirements["cpu_threads"],
                f"capabilities.{name}.requirements.cpu_threads",
                1,
                128,
            ),
            "memory_mb": integer(
                requirements["memory_mb"],
                f"capabilities.{name}.requirements.memory_mb",
                1,
                1_048_576,
            ),
            "gpu_required": boolean(
                requirements["gpu_required"],
                f"capabilities.{name}.requirements.gpu_required",
            ),
            "gpu_memory_mb": integer(
                requirements["gpu_memory_mb"],
                f"capabilities.{name}.requirements.gpu_memory_mb",
                0,
                1_048_576,
            ),
        },
    }


def validate_capabilities(raw: Any) -> dict[str, Any]:
    value = object_value(raw, "capabilities")
    exact_keys(value, "capabilities", set(CAPABILITY_TASKS))
    return {name: validate_capability(value[name], name) for name in CAPABILITY_TASKS}


def validate_resources(raw: Any) -> dict[str, Any]:
    value = object_value(raw, "resources")
    exact_keys(
        value,
        "resources",
        {"cpu_threads", "memory_mb", "gpu_available", "gpu_memory_mb", "max_parallel_tasks"},
    )
    available = boolean(value["gpu_available"], "resources.gpu_available")
    memory = integer(value["gpu_memory_mb"], "resources.gpu_memory_mb", 0, 1_048_576)
    if not available and memory != 0:
        raise RoutingError("resources.gpu_memory_mb must be zero when no GPU is available")
    return {
        "cpu_threads": integer(value["cpu_threads"], "resources.cpu_threads", 1, 128),
        "memory_mb": integer(value["memory_mb"], "resources.memory_mb", 256, 1_048_576),
        "gpu_available": available,
        "gpu_memory_mb": memory,
        "max_parallel_tasks": integer(
            value["max_parallel_tasks"], "resources.max_parallel_tasks", 1, 128
        ),
    }


def validate_policy(raw: Any) -> dict[str, Any]:
    value = object_value(raw, "policy")
    exact_keys(
        value,
        "policy",
        {
            "diarization_chunk_ms",
            "face_chunk_ms",
            "active_speaker_chunk_ms",
            "route_visual_when_face_unknown",
        },
    )
    return {
        "diarization_chunk_ms": integer(
            value["diarization_chunk_ms"], "policy.diarization_chunk_ms", 1_000, 3_600_000
        ),
        "face_chunk_ms": integer(
            value["face_chunk_ms"], "policy.face_chunk_ms", 1_000, 3_600_000
        ),
        "active_speaker_chunk_ms": integer(
            value["active_speaker_chunk_ms"],
            "policy.active_speaker_chunk_ms",
            1_000,
            3_600_000,
        ),
        "route_visual_when_face_unknown": boolean(
            value["route_visual_when_face_unknown"],
            "policy.route_visual_when_face_unknown",
        ),
    }


def validate_work_order(raw: Any) -> dict[str, Any]:
    value = object_value(raw, "work order")
    exact_keys(
        value,
        "work order",
        {
            "schema_version",
            "job_id",
            "recording",
            "inputs",
            "reviewed_hints",
            "capabilities",
            "resources",
            "policy",
            "output",
        },
    )
    if value["schema_version"] != SCHEMA_VERSION:
        raise RoutingError(f"schema_version must be {SCHEMA_VERSION}")
    output = object_value(value["output"], "output")
    exact_keys(output, "output", {"result_path"})
    result = {
        "schema_version": SCHEMA_VERSION,
        "job_id": identifier(value["job_id"], "job_id"),
        "recording": validate_recording(value["recording"]),
        "inputs": validate_inputs(value["inputs"]),
        "reviewed_hints": validate_reference(value["reviewed_hints"], "reviewed_hints"),
        "capabilities": validate_capabilities(value["capabilities"]),
        "resources": validate_resources(value["resources"]),
        "policy": validate_policy(value["policy"]),
        "output": {"result_path": str(absolute_output(output["result_path"]))},
    }
    owned = [
        Path(result["inputs"]["preprocess_result"]["path"]),
        Path(result["inputs"]["asr_result"]["path"]),
        Path(result["reviewed_hints"]["path"]),
    ]
    for capability in result["capabilities"].values():
        if capability["status"] == "pinned":
            owned.extend(
                Path(reference["path"])
                for reference in (
                    capability["tool"]["file"],
                    capability["model"]["manifest"],
                    capability["model"]["weights"],
                    capability["calibration"]["artifact"],
                )
            )
    output_path = Path(result["output"]["result_path"])
    if output_path in owned:
        raise RoutingError("output.result_path must not replace an input or pinned artifact")
    return result


def validate_hint_document(raw: dict[str, Any], recording: dict[str, Any]) -> dict[str, Any]:
    exact_keys(
        raw,
        "reviewed hint document",
        {
            "schema_version",
            "review_batch_id",
            "recording_id",
            "duration_ms",
            "review_state",
            "reviewer_id",
            "reviewed_at",
            "source_identity_attestation",
            "intervals",
        },
    )
    if raw["schema_version"] != 1 or raw["review_state"] != "human_reviewed":
        raise RoutingError("hint document must be schema version 1 and human_reviewed")
    if raw["recording_id"] != recording["recording_id"]:
        raise RoutingError("hint document recording_id does not match the work order")
    if raw["duration_ms"] != recording["duration_ms"]:
        raise RoutingError("hint document duration_ms does not match the work order")
    intervals_raw = raw["intervals"]
    if not isinstance(intervals_raw, list) or len(intervals_raw) > MAX_INTERVALS:
        raise RoutingError(f"hint intervals must be an array of at most {MAX_INTERVALS} entries")
    intervals: list[dict[str, Any]] = []
    previous_end = 0
    hint_ids: set[str] = set()
    for index, item_raw in enumerate(intervals_raw):
        label = f"hint intervals[{index}]"
        item = object_value(item_raw, label)
        exact_keys(
            item,
            label,
            {
                "hint_id",
                "start_ms",
                "end_ms",
                "speech_presence",
                "speech_multiplicity",
                "audio_origin",
                "face_visibility",
                "speaker_visual_relation",
                "review_confidence",
                "comment",
            },
        )
        start = integer(item["start_ms"], f"{label}.start_ms", 0, recording["duration_ms"] - 1)
        end = integer(item["end_ms"], f"{label}.end_ms", 1, recording["duration_ms"])
        if end <= start:
            raise RoutingError(f"{label} must use a non-empty half-open [start_ms,end_ms) interval")
        if index and start < previous_end:
            raise RoutingError("reviewed hint intervals must be sorted and non-overlapping")
        presence = enum_value(
            item["speech_presence"], f"{label}.speech_presence", {"absent", "present", "unknown"}
        )
        multiplicity = enum_value(
            item["speech_multiplicity"],
            f"{label}.speech_multiplicity",
            {"none", "single", "overlap", "unknown"},
        )
        if presence == "absent" and multiplicity != "none":
            raise RoutingError(f"{label}: absent speech requires multiplicity none")
        if presence == "present" and multiplicity == "none":
            raise RoutingError(f"{label}: present speech cannot have multiplicity none")
        if presence == "unknown" and multiplicity not in {"none", "unknown"}:
            raise RoutingError(f"{label}: unknown speech cannot assert a speaker count")
        comment = item["comment"]
        if comment is not None:
            comment = string_value(comment, f"{label}.comment", 1_000)
        hint_id = identifier(item["hint_id"], f"{label}.hint_id")
        if hint_id in hint_ids:
            raise RoutingError("reviewed hint IDs must be unique")
        hint_ids.add(hint_id)
        intervals.append(
            {
                "hint_id": hint_id,
                "start_ms": start,
                "end_ms": end,
                "speech_presence": presence,
                "speech_multiplicity": multiplicity,
                "audio_origin": enum_value(
                    item["audio_origin"],
                    f"{label}.audio_origin",
                    {
                        "live_voice",
                        "playback_voice",
                        "reaction_insert_voice",
                        "tts_voice",
                        "synthetic_voice",
                        "unknown",
                    },
                ),
                "face_visibility": enum_value(
                    item["face_visibility"],
                    f"{label}.face_visibility",
                    {"none", "single_face", "multiple_faces", "unknown"},
                ),
                "speaker_visual_relation": enum_value(
                    item["speaker_visual_relation"],
                    f"{label}.speaker_visual_relation",
                    {"onscreen", "offscreen", "mixed", "unknown"},
                ),
                "review_confidence": enum_value(
                    item["review_confidence"],
                    f"{label}.review_confidence",
                    {"high", "medium", "low"},
                ),
                "comment": comment,
            }
        )
        previous_end = end
    reviewer_id = identifier(raw["reviewer_id"], "reviewer_id")
    reviewed_at = zoned_time(raw["reviewed_at"], "reviewed_at")
    attestation_raw = raw["source_identity_attestation"]
    attestation = None
    if attestation_raw is not None:
        value = object_value(attestation_raw, "source_identity_attestation")
        exact_keys(
            value,
            "source_identity_attestation",
            {
                "attestation_id",
                "source_confirmation",
                "source_confirmation_evidence",
                "public_label",
                "basis",
                "review_scope",
                "reviewed_live_speaker_count",
                "title_context_assessment",
                "reviewer_id",
                "reviewer_kind",
                "reviewed_at",
                "comment",
            },
        )
        attestation_reviewer = identifier(
            value["reviewer_id"], "source_identity_attestation.reviewer_id"
        )
        attestation_reviewed_at = zoned_time(
            value["reviewed_at"], "source_identity_attestation.reviewed_at"
        )
        if attestation_reviewer != reviewer_id:
            raise RoutingError(
                "source identity attestation must bind the hint document's human reviewer"
            )
        if attestation_reviewed_at != reviewed_at:
            raise RoutingError(
                "source identity attestation review time must match the hint document"
            )
        if value["reviewer_kind"] != "human":
            raise RoutingError("source identity attestation reviewer_kind must be human")
        if value["source_confirmation"] != CONFIRMED_DANIEL_SOURCE:
            raise RoutingError(
                "source identity attestation must explicitly confirm a Daniel-owned source"
            )
        if value["public_label"] != PUBLIC_DANIEL_LABEL:
            raise RoutingError("source identity attestation public_label must be Daniel")
        if value["basis"] != PUBLIC_DANIEL_BASIS:
            raise RoutingError(
                "source identity attestation must use the explicit solo-presumption basis"
            )
        if value["review_scope"] != "complete_recording":
            raise RoutingError("source identity attestation must cover the complete recording")
        comment = value["comment"]
        if comment is not None:
            comment = string_value(
                comment, "source_identity_attestation.comment", 1_000
            )
        attestation = {
            "attestation_id": identifier(
                value["attestation_id"], "source_identity_attestation.attestation_id"
            ),
            "source_confirmation": CONFIRMED_DANIEL_SOURCE,
            "source_confirmation_evidence": string_value(
                value["source_confirmation_evidence"],
                "source_identity_attestation.source_confirmation_evidence",
                2_000,
            ),
            "public_label": PUBLIC_DANIEL_LABEL,
            "basis": PUBLIC_DANIEL_BASIS,
            "review_scope": "complete_recording",
            "reviewed_live_speaker_count": integer(
                value["reviewed_live_speaker_count"],
                "source_identity_attestation.reviewed_live_speaker_count",
                0,
                1_000,
            ),
            "title_context_assessment": enum_value(
                value["title_context_assessment"],
                "source_identity_attestation.title_context_assessment",
                {
                    "no_contradiction_found",
                    "contradicts_solo_daniel_presumption",
                    "ambiguous_or_unresolved",
                },
            ),
            "reviewer_id": attestation_reviewer,
            "reviewer_kind": "human",
            "reviewed_at": attestation_reviewed_at,
            "comment": comment,
        }
        cursor = 0
        for item in intervals:
            if item["start_ms"] != cursor:
                raise RoutingError(
                    "complete-recording source identity attestation requires contiguous hints"
                )
            cursor = item["end_ms"]
        if cursor != recording["duration_ms"]:
            raise RoutingError(
                "complete-recording source identity attestation requires full-duration hints"
            )
        if (
            attestation["reviewed_live_speaker_count"] == 1
            and attestation["title_context_assessment"] == "no_contradiction_found"
        ):
            live_voice_seen = False
            for item in intervals:
                if item["speech_presence"] == "unknown":
                    raise RoutingError(
                        "solo Daniel presumption cannot coexist with unknown speech presence"
                    )
                if item["speech_presence"] != "present":
                    continue
                if item["audio_origin"] == "live_voice":
                    live_voice_seen = True
                    if item["speech_multiplicity"] != "single":
                        raise RoutingError(
                            "solo Daniel presumption cannot coexist with live overlap "
                            "or unresolved multiplicity"
                        )
            if not live_voice_seen:
                raise RoutingError(
                    "solo Daniel presumption requires a reviewed live-voice interval"
                )
    return {
        "schema_version": 1,
        "review_batch_id": identifier(raw["review_batch_id"], "review_batch_id"),
        "recording_id": recording["recording_id"],
        "duration_ms": recording["duration_ms"],
        "review_state": "human_reviewed",
        "reviewer_id": reviewer_id,
        "reviewed_at": reviewed_at,
        "source_identity_attestation": attestation,
        "intervals": intervals,
    }


def completed_preprocess(raw: dict[str, Any], recording: dict[str, Any]) -> dict[str, Any]:
    if raw.get("schema_version") != 1 or raw.get("status") != "completed" or raw.get("dry_run") is not False:
        raise RoutingError("preprocess result must be a completed, non-dry-run version-1 envelope")
    processing = object_value(raw.get("processing_run"), "preprocess processing_run")
    if processing.get("stage") != "media_preprocess":
        raise RoutingError("preprocess result has the wrong processing stage")
    run_id = identifier(processing.get("processing_run_id"), "preprocess processing_run_id")
    routing = object_value(raw.get("routing"), "preprocess routing")
    coverage = object_value(routing.get("coverage"), "preprocess routing.coverage")
    if coverage.get("duration_ms") != recording["duration_ms"]:
        raise RoutingError("preprocess duration does not match recording.duration_ms")
    has_video = coverage.get("has_video") is True
    artifacts = raw.get("artifacts")
    if not isinstance(artifacts, list):
        raise RoutingError("preprocess result artifacts must be an array")
    by_kind: dict[str, dict[str, Any]] = {}
    for artifact in artifacts:
        if isinstance(artifact, dict) and isinstance(artifact.get("artifact_kind"), str):
            by_kind[artifact["artifact_kind"]] = artifact
    audio = by_kind.get("audio_16khz_mono_flac")
    if not isinstance(audio, dict):
        raise RoutingError("preprocess result lacks normalized audio")
    proxy = by_kind.get("low_resolution_cfr_proxy")
    if has_video != isinstance(proxy, dict):
        raise RoutingError("preprocess video coverage and proxy artifact disagree")
    return {
        "processing_run_id": run_id,
        "normalized_audio_sha256": sha256_value(
            audio.get("sha256"), "preprocess normalized audio sha256"
        ),
        "proxy_video_sha256": None
        if proxy is None
        else sha256_value(proxy.get("sha256"), "preprocess proxy sha256"),
        "has_video": has_video,
    }


def completed_asr(
    raw: dict[str, Any],
    recording: dict[str, Any],
    preprocess: dict[str, Any],
) -> dict[str, Any]:
    if raw.get("schema_version") != 1 or raw.get("status") != "completed" or raw.get("dry_run") is not False:
        raise RoutingError("ASR result must be a completed, non-dry-run version-1 envelope")
    processing = object_value(raw.get("processing_run"), "ASR processing_run")
    if processing.get("stage") != "asr_whispercpp":
        raise RoutingError("ASR result has the wrong processing stage")
    run_id = identifier(processing.get("processing_run_id"), "ASR processing_run_id")
    input_value = object_value(raw.get("input"), "ASR input")
    if input_value.get("parent_processing_run_id") != preprocess["processing_run_id"]:
        raise RoutingError("ASR input is not linked to the supplied preprocessing run")
    audio_sha = sha256_value(input_value.get("sha256"), "ASR input.sha256")
    if audio_sha != preprocess["normalized_audio_sha256"]:
        raise RoutingError("ASR input hash does not match preprocessing normalized audio")
    glossary_raw = raw.get("glossary")
    glossary_sha = None
    if glossary_raw is not None:
        glossary_sha = sha256_value(
            object_value(glossary_raw, "ASR glossary").get("sha256"), "ASR glossary.sha256"
        )
    transcript = object_value(raw.get("transcript"), "ASR transcript")
    window = object_value(transcript.get("window"), "ASR transcript.window")
    window_start = integer(window.get("offset_ms"), "ASR window.offset_ms", 0, recording["duration_ms"] - 1)
    window_end_raw = integer(window.get("end_ms"), "ASR window.end_ms", 1, recording["duration_ms"] + 30_000)
    window_end = min(window_end_raw, recording["duration_ms"])
    if window_end <= window_start:
        raise RoutingError("ASR window has no overlap with the recording")
    segments_raw = transcript.get("segments")
    if not isinstance(segments_raw, list) or len(segments_raw) > MAX_INTERVALS:
        raise RoutingError(f"ASR segments must be an array of at most {MAX_INTERVALS} entries")
    segments: list[dict[str, int]] = []
    previous_ordinal = -1
    for index, segment_raw in enumerate(segments_raw):
        segment = object_value(segment_raw, f"ASR segments[{index}]")
        ordinal = integer(segment.get("ordinal"), f"ASR segments[{index}].ordinal", 0, MAX_INTERVALS)
        if ordinal <= previous_ordinal:
            raise RoutingError("ASR segment ordinals must be strictly increasing")
        start = integer(segment.get("start_ms"), f"ASR segments[{index}].start_ms", 0, recording["duration_ms"] + 30_000)
        end = integer(segment.get("end_ms"), f"ASR segments[{index}].end_ms", 1, recording["duration_ms"] + 30_000)
        start = max(window_start, min(start, recording["duration_ms"]))
        end = max(0, min(end, recording["duration_ms"]))
        if end > start:
            segments.append({"ordinal": ordinal, "start_ms": start, "end_ms": end})
        previous_ordinal = ordinal
    return {
        "processing_run_id": run_id,
        "input_audio_sha256": audio_sha,
        "glossary_sha256": glossary_sha,
        "window": {"start_ms": window_start, "end_ms": window_end},
        "segments": segments,
    }


def observe_capabilities(capabilities: dict[str, Any]) -> dict[str, Any]:
    observed: dict[str, Any] = {}
    for name, capability in capabilities.items():
        if capability["status"] == "unconfigured":
            observed[name] = capability
            continue
        references = {
            "tool": capability["tool"]["file"],
            "model_manifest": capability["model"]["manifest"],
            "model_weights": capability["model"]["weights"],
            "calibration": capability["calibration"]["artifact"],
        }
        evidence: dict[str, Any] = {}
        for label, reference in references.items():
            evidence[label] = observe_pinned_file(
                reference, f"capabilities.{name}.{label}"
            )
        normalized = {
            "status": "pinned",
            "task": capability["task"],
            "tool": {
                "name": capability["tool"]["name"],
                "version": capability["tool"]["version"],
                "file": evidence["tool"],
            },
            "model": {
                "model_id": capability["model"]["model_id"],
                "revision": capability["model"]["revision"],
                "manifest": evidence["model_manifest"],
                "weights": evidence["model_weights"],
            },
            "calibration": {
                "method": capability["calibration"]["method"],
                "artifact": evidence["calibration"],
            },
            "requirements": capability["requirements"],
        }
        normalized["capability_recipe_sha256"] = sha256_bytes(canonical_bytes(normalized))
        observed[name] = normalized
    return observed


def resource_state(
    capability_name: str,
    capabilities: dict[str, Any],
    resources: dict[str, Any],
) -> tuple[str, list[str], str | None]:
    capability = capabilities[capability_name]
    if capability["status"] == "unconfigured":
        return "blocked_unconfigured", ["no_hash_pinned_capability"], None
    requirements = capability["requirements"]
    missing: list[str] = []
    if resources["cpu_threads"] < requirements["cpu_threads"]:
        missing.append("cpu_threads")
    if resources["memory_mb"] < requirements["memory_mb"]:
        missing.append("memory_mb")
    if requirements["gpu_required"] and not resources["gpu_available"]:
        missing.append("gpu")
    if resources["gpu_memory_mb"] < requirements["gpu_memory_mb"]:
        missing.append("gpu_memory_mb")
    if missing:
        return "blocked_resources", missing, capability["capability_recipe_sha256"]
    return "ready_pinned", [], capability["capability_recipe_sha256"]


def chunks(start: int, end: int, size: int) -> list[tuple[int, int]]:
    result: list[tuple[int, int]] = []
    cursor = start
    while cursor < end:
        next_end = min(end, cursor + size)
        result.append((cursor, next_end))
        cursor = next_end
    return result


def route_plan(
    work_order: dict[str, Any],
    preprocess: dict[str, Any],
    asr: dict[str, Any],
    hints: dict[str, Any],
    capabilities: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    duration = work_order["recording"]["duration_ms"]
    boundaries = {asr["window"]["start_ms"], asr["window"]["end_ms"]}
    for segment in asr["segments"]:
        boundaries.update((segment["start_ms"], segment["end_ms"]))
    for hint in hints["intervals"]:
        boundaries.update((hint["start_ms"], hint["end_ms"]))
    ordered = sorted(value for value in boundaries if 0 <= value <= duration)
    # Segment and hint counts are independently bounded above.  Bound the merged
    # sweep before allocating route objects, then use events instead of a quadratic
    # interval-by-segment scan.
    if len(ordered) > (2 * MAX_INTERVALS) + 2:
        raise RoutingError("too many distinct ASR/review boundaries for one route plan")
    segment_starts: dict[int, list[int]] = {}
    segment_ends: dict[int, list[int]] = {}
    for segment in asr["segments"]:
        segment_starts.setdefault(segment["start_ms"], []).append(segment["ordinal"])
        segment_ends.setdefault(segment["end_ms"], []).append(segment["ordinal"])
    active_ordinals: set[int] = set()
    hint_index = 0
    intervals: list[dict[str, Any]] = []
    warnings: set[str] = set()
    source_attestation = hints["source_identity_attestation"]
    daniel_solo_source_eligible = (
        source_attestation is not None
        and source_attestation["reviewed_live_speaker_count"] == 1
        and source_attestation["title_context_assessment"] == "no_contradiction_found"
    )
    for start, end in zip(ordered, ordered[1:]):
        if end <= start:
            continue
        active_ordinals.difference_update(segment_ends.get(start, ()))
        active_ordinals.update(segment_starts.get(start, ()))
        ordinals = sorted(active_ordinals)
        while (
            hint_index < len(hints["intervals"])
            and hints["intervals"][hint_index]["end_ms"] <= start
        ):
            hint_index += 1
        hint = None
        if hint_index < len(hints["intervals"]):
            candidate = hints["intervals"][hint_index]
            if candidate["start_ms"] <= start and end <= candidate["end_ms"]:
                hint = candidate
        if not ordinals and hint is None:
            continue
        asr_speech = bool(ordinals)
        conflicts: list[str] = []
        if hint is None:
            presence = "present" if asr_speech else "unknown"
            multiplicity = "unknown"
            origin = "unknown"
            face = "unknown"
            relation = "unknown"
            confidence = None
            hint_id = None
        else:
            presence = hint["speech_presence"]
            multiplicity = hint["speech_multiplicity"]
            origin = hint["audio_origin"]
            face = hint["face_visibility"]
            relation = hint["speaker_visual_relation"]
            confidence = hint["review_confidence"]
            hint_id = hint["hint_id"]
            if asr_speech and presence == "absent":
                conflicts.append("asr_speech_overlaps_reviewed_absence")
                warnings.add("evidence_conflict_requires_review")
                presence = "unknown"
                multiplicity = "unknown"
            elif asr_speech and presence == "unknown":
                presence = "present"
        if presence == "absent" and not conflicts:
            # Retain reviewed non-speech visual/origin intervals only if they are
            # semantically meaningful; otherwise they do not create speaker work.
            if origin == "unknown" and face in {"none", "unknown"}:
                continue
        interval_id = stable_id(
            "routing_interval",
            work_order["recording"]["recording_id"],
            start,
            end,
            hint_id,
            ordinals,
        )
        public_identity_attribution = None
        if (
            daniel_solo_source_eligible
            and hint is not None
            and not conflicts
            and presence == "present"
            and multiplicity == "single"
            and origin == "live_voice"
            and confidence == "high"
        ):
            public_identity_attribution = {
                "public_label": PUBLIC_DANIEL_LABEL,
                "basis": PUBLIC_DANIEL_BASIS,
                "source_confirmation": CONFIRMED_DANIEL_SOURCE,
                "source_confirmation_attestation_id": source_attestation[
                    "attestation_id"
                ],
                "reviewer_id": source_attestation["reviewer_id"],
                "reviewer_kind": source_attestation["reviewer_kind"],
                "reviewed_at": source_attestation["reviewed_at"],
            }
        intervals.append(
            {
                "interval_id": interval_id,
                "start_ms": start,
                "end_ms": end,
                "timestamp_semantics": "half_open",
                "evidence": {
                    "asr_segment_ordinals": ordinals,
                    "reviewed_hint_id": hint_id,
                    "review_confidence": confidence,
                    "conflicts": conflicts,
                },
                "speech_observation": {
                    "presence": presence,
                    "multiplicity": multiplicity,
                    "provisional_speaker_label": "unknown_single"
                    if (
                        presence == "present"
                        and multiplicity == "single"
                        and origin == "live_voice"
                    )
                    else None,
                    "public_speaker_label": PUBLIC_DANIEL_LABEL
                    if public_identity_attribution is not None
                    else None,
                    "public_identity_attribution": public_identity_attribution,
                },
                "face_visibility_observation": {
                    "visibility": face,
                    "speaking_face_claimed": False,
                },
                "audio_origin_observation": {"category": origin},
                "speaker_visual_relation_observation": {"category": relation},
            }
        )
        if len(intervals) > MAX_INTERVALS:
            raise RoutingError(f"route plan exceeds {MAX_INTERVALS} atomic intervals")

    tasks: list[dict[str, Any]] = []
    placeholders: list[dict[str, Any]] = []

    def add_manual(interval: dict[str, Any], task_type: str, reasons: list[str]) -> None:
        task_id = stable_id("speaker_task", interval["interval_id"], task_type, reasons)
        tasks.append(
            {
                "task_id": task_id,
                "interval_id": interval["interval_id"],
                "start_ms": interval["start_ms"],
                "end_ms": interval["end_ms"],
                "timestamp_semantics": "half_open",
                "task_type": task_type,
                "execution_state": "human_review",
                "reason_codes": reasons,
                "capability_recipe_sha256": None,
                "resource_deficits": [],
            }
        )

    def add_capability(
        interval: dict[str, Any],
        task_type: str,
        capability_name: str,
        chunk_ms: int,
        reasons: list[str],
    ) -> None:
        state, deficits, recipe_sha = resource_state(
            capability_name, capabilities, work_order["resources"]
        )
        for chunk_start, chunk_end in chunks(interval["start_ms"], interval["end_ms"], chunk_ms):
            task_id = stable_id(
                "speaker_task", interval["interval_id"], task_type, chunk_start, chunk_end, reasons
            )
            tasks.append(
                {
                    "task_id": task_id,
                    "interval_id": interval["interval_id"],
                    "start_ms": chunk_start,
                    "end_ms": chunk_end,
                    "timestamp_semantics": "half_open",
                    "task_type": task_type,
                    "execution_state": state,
                    "reason_codes": reasons,
                    "capability_recipe_sha256": recipe_sha,
                    "resource_deficits": deficits,
                }
            )
            if task_type == "active_speaker_association":
                placeholders.append(
                    {
                        "association_id": stable_id("asd_placeholder", task_id),
                        "task_id": task_id,
                        "start_ms": chunk_start,
                        "end_ms": chunk_end,
                        "timestamp_semantics": "half_open",
                        "association_state": "unknown",
                        "face_track_id": None,
                        "diarization_run_id": None,
                        "diarization_label": None,
                        "raw_confidence": None,
                        "calibrated_confidence": None,
                        "calibration_artifact_sha256": None,
                        "observation_source": "routing_placeholder",
                    }
                )

    for interval in intervals:
        speech = interval["speech_observation"]
        face = interval["face_visibility_observation"]["visibility"]
        origin = interval["audio_origin_observation"]["category"]
        relation = interval["speaker_visual_relation_observation"]["category"]
        conflicts = interval["evidence"]["conflicts"]
        speech_present = speech["presence"] == "present"
        if conflicts:
            add_manual(interval, "evidence_conflict_review", conflicts)
        if relation in {"offscreen", "mixed"} and speech["presence"] != "absent":
            add_manual(interval, "offscreen_speech_review", [f"speaker_visual_relation_{relation}"])
        if origin in ORIGIN_REVIEW_TASKS:
            add_manual(interval, ORIGIN_REVIEW_TASKS[origin], [f"audio_origin_{origin}"])
        elif origin == "unknown" and speech_present:
            add_manual(interval, "audio_origin_review", ["audio_origin_unknown"])

        if speech_present and speech["multiplicity"] == "single" and origin == "live_voice":
            task_id = stable_id("speaker_task", interval["interval_id"], "solo_fast_path")
            tasks.append(
                {
                    "task_id": task_id,
                    "interval_id": interval["interval_id"],
                    "start_ms": interval["start_ms"],
                    "end_ms": interval["end_ms"],
                    "timestamp_semantics": "half_open",
                    "task_type": "solo_fast_path",
                    "execution_state": "no_inference_required",
                    "reason_codes": [
                        "reviewed_single_live_voice",
                        PUBLIC_DANIEL_BASIS
                        if speech["public_speaker_label"] == PUBLIC_DANIEL_LABEL
                        else "label_unknown_single_only",
                    ],
                    "capability_recipe_sha256": None,
                    "resource_deficits": [],
                }
            )
        elif speech_present or speech["presence"] == "unknown":
            reasons = [
                "reviewed_overlap" if speech["multiplicity"] == "overlap" else "speaker_count_unresolved"
            ]
            add_capability(
                interval,
                "overlap_aware_diarization",
                "diarization",
                work_order["policy"]["diarization_chunk_ms"],
                reasons,
            )

        visual_candidate = preprocess["has_video"] and (
            face in {"single_face", "multiple_faces"}
            or (face == "unknown" and work_order["policy"]["route_visual_when_face_unknown"])
        )
        if visual_candidate:
            add_capability(
                interval,
                "face_tracking",
                "face_tracking",
                work_order["policy"]["face_chunk_ms"],
                [f"face_visibility_{face}"],
            )
        trivial_solo_onscreen = (
            speech_present
            and speech["multiplicity"] == "single"
            and face == "single_face"
            and relation == "onscreen"
            and origin == "live_voice"
        )
        if visual_candidate and speech["presence"] != "absent" and not trivial_solo_onscreen:
            add_capability(
                interval,
                "active_speaker_association",
                "active_speaker",
                work_order["policy"]["active_speaker_chunk_ms"],
                ["speaking_face_not_assumed_from_visibility"],
            )
    tasks.sort(key=lambda item: (item["start_ms"], item["end_ms"], item["task_type"], item["task_id"]))
    placeholders.sort(key=lambda item: (item["start_ms"], item["end_ms"], item["task_id"]))
    if len(tasks) > MAX_TASKS or len(placeholders) > MAX_TASKS:
        raise RoutingError(f"route plan exceeds the {MAX_TASKS}-task safety limit")
    return intervals, tasks, placeholders, sorted(warnings)


def build_result(work_order: dict[str, Any]) -> dict[str, Any]:
    preprocess_path, preprocess_body, preprocess_digest = read_observed(
        work_order["inputs"]["preprocess_result"], "inputs.preprocess_result"
    )
    asr_path, asr_body, asr_digest = read_observed(
        work_order["inputs"]["asr_result"], "inputs.asr_result"
    )
    hints_path, hints_body, hints_digest = read_observed(
        work_order["reviewed_hints"], "reviewed_hints"
    )
    preprocess = completed_preprocess(
        json_body(preprocess_body, "preprocess result"), work_order["recording"]
    )
    asr = completed_asr(
        json_body(asr_body, "ASR result"), work_order["recording"], preprocess
    )
    hints = validate_hint_document(
        json_body(hints_body, "reviewed hint document"), work_order["recording"]
    )
    if preprocess["normalized_audio_sha256"] != work_order["inputs"]["normalized_audio_sha256"]:
        raise RoutingError("work-order normalized audio hash does not match preprocessing")
    if preprocess["proxy_video_sha256"] != work_order["inputs"]["proxy_video_sha256"]:
        raise RoutingError("work-order proxy video hash does not match preprocessing")
    if asr["glossary_sha256"] != work_order["inputs"]["glossary_sha256"]:
        raise RoutingError("work-order glossary hash does not match ASR provenance")
    capabilities = observe_capabilities(work_order["capabilities"])
    intervals, tasks, placeholders, warnings = route_plan(
        work_order, preprocess, asr, hints, capabilities
    )
    tool_path = Path(__file__).resolve(strict=True)
    tool_sha, _ = stable_hash(tool_path, "router tool")
    work_order_sha = sha256_bytes(canonical_bytes(work_order))
    routing_run_id = stable_id(
        "run_speaker_router", work_order_sha, preprocess_digest, asr_digest, hints_digest, tool_sha
    )
    return {
        "schema_version": 1,
        "job_id": work_order["job_id"],
        "status": "completed",
        "stage": STAGE,
        "implementation_version": IMPLEMENTATION_VERSION,
        "routing_run_id": routing_run_id,
        "work_order_sha256": work_order_sha,
        "router_tool": {"path": str(tool_path), "sha256": tool_sha},
        "recording": work_order["recording"],
        "inputs": {
            "preprocess_result": {"path": str(preprocess_path), "sha256": preprocess_digest},
            "asr_result": {"path": str(asr_path), "sha256": asr_digest},
            "reviewed_hints": {"path": str(hints_path), "sha256": hints_digest},
            "normalized_audio_sha256": preprocess["normalized_audio_sha256"],
            "proxy_video_sha256": preprocess["proxy_video_sha256"],
            "glossary_sha256": asr["glossary_sha256"],
            "preprocess_processing_run_id": preprocess["processing_run_id"],
            "asr_processing_run_id": asr["processing_run_id"],
            "review_batch_id": hints["review_batch_id"],
            "review_state": hints["review_state"],
            "reviewer_id": hints["reviewer_id"],
            "reviewed_at": hints["reviewed_at"],
            "source_identity_attestation": hints["source_identity_attestation"],
        },
        "capabilities": capabilities,
        "resources": work_order["resources"],
        "policy": work_order["policy"],
        "identity_safety": {
            "solo_label": "unknown_single",
            "allowed_named_public_label": "Daniel",
            "named_public_label_basis": PUBLIC_DANIEL_BASIS,
            "named_public_label_requires_human_reviewer": True,
            "named_public_label_requires_source_confirmation_attestation": True,
            "named_public_label_requires_no_title_context_contradiction": True,
            "named_public_label_requires_visual_confirmation": False,
            "diarization_label_scope": "downstream_processing_run_local_only",
            "cross_recording_identity_inference": "forbidden",
            "overlapping_speech_representable": True,
            "face_visibility_implies_speaking": False,
            "router_makes_identity_assertions": False,
        },
        "intervals": intervals,
        "tasks": tasks,
        "active_speaker_association_placeholders": placeholders,
        "warnings": warnings,
        "result_path": work_order["output"]["result_path"],
    }


def atomic_write_once(path: Path, body: bytes) -> None:
    if path.exists():
        if path.stat().st_mode & 0o222:
            raise RoutingError("existing route result is writable and cannot be reuse authority")
        existing = stable_read(path, "existing route result")
        if existing != body:
            raise RoutingError("output result already exists with different bytes")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        with temporary.open("xb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
            os.fchmod(handle.fileno(), 0o444)
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.stat().st_mode & 0o222:
                raise RoutingError("concurrent route result is writable")
            existing = stable_read(path, "concurrent route result")
            if existing != body:
                raise RoutingError("concurrent writer admitted different route bytes")
        finally:
            temporary.unlink(missing_ok=True)
    finally:
        temporary.unlink(missing_ok=True)


def load_work_order(path: Path) -> dict[str, Any]:
    body = stable_read(path, "work order")
    if len(body) > 16 * 1024 * 1024:
        raise RoutingError("work order exceeds 16 MiB")
    return validate_work_order(json_body(body, "work order"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("validate", "run"):
        child = subparsers.add_parser(command)
        child.add_argument("--work-order", required=True, type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        path = args.work_order.resolve(strict=True)
        work_order = load_work_order(path)
        if args.command == "validate":
            print(json.dumps(work_order, sort_keys=True, indent=2))
            return 0
        result = build_result(work_order)
        body = pretty_bytes(result)
        atomic_write_once(Path(work_order["output"]["result_path"]), body)
        sys.stdout.buffer.write(body)
        return 0
    except (RoutingError, FileNotFoundError, OSError) as error:
        print(f"speaker activity routing failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
