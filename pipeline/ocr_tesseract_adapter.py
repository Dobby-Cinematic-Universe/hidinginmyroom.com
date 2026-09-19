#!/usr/bin/env python3
"""Fail-closed offline Tesseract TSV adapter for sealed sparse-frame results.

This producer has no database, network, publication, or identity-inference path.
It accepts only an exact completed sparse-frame-router envelope and byte-pinned
local OCR assets, preserves every upstream frame locator, and emits owner-private
raw TSV plus uncalibrated word-region observations for later human review.
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import json
import math
import os
import re
import shutil
import signal
import stat
import struct
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
STAGE = "ocr_tesseract_tsv"
MAX_JSON_BYTES = 64 * 1024 * 1024
MAX_VERSION_BYTES = 1024 * 1024
MAX_ERROR_BYTES = 2 * 1024 * 1024
MAX_FRAMES = 256
MAX_TSV_BYTES = 64 * 1024 * 1024
MAX_WORDS = 100_000
JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
LANGUAGE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,31}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
RAW_SCORE_RE = re.compile(
    r"^(?:0(?:\.[0-9]+)?|[1-9][0-9]?(?:\.[0-9]+)?|100(?:\.0+)?)$"
)
TSV_HEADER = (
    "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\t"
    "left\ttop\twidth\theight\tconf\ttext"
)
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
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
RESULT_KEYS = {
    "schema_version",
    "job_id",
    "status",
    "dry_run",
    "work_order_sha256",
    "recipe_id",
    "recipe_sha256",
    "result_key",
    "processing_run",
    "sparse_frame_result",
    "source_lineage",
    "tesseract",
    "execution_selection",
    "parameters",
    "commands",
    "ocr_frames",
    "policy",
    "result_path",
    "duration_ms",
    "errors",
}


class OCRAdapterError(RuntimeError):
    """A contract, provenance, integrity, TSV, or private-output check failed."""


class OCRCommandError(OCRAdapterError):
    """The pinned OCR executable failed or exceeded its execution boundary."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def canonical_json_text(value: Any) -> str:
    return canonical_bytes(value).decode("utf-8")


def pretty_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def stable_id(prefix: str, *parts: Any) -> str:
    return f"{prefix}_" + sha256_bytes(canonical_bytes(list(parts)))[:32]


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise OCRAdapterError(f"JSON contains duplicate key {key!r}")
        result[key] = value
    return result


def load_json(path: Path, maximum_bytes: int = MAX_JSON_BYTES) -> Any:
    try:
        size = path.stat().st_size
    except OSError as error:
        raise OCRAdapterError(f"cannot stat JSON input {path}: {error}") from error
    if size <= 0 or size > maximum_bytes:
        raise OCRAdapterError(
            f"JSON input must contain 1..{maximum_bytes} bytes: {path}"
        )
    try:
        return json.loads(
            path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicate_keys
        )
    except UnicodeDecodeError as error:
        raise OCRAdapterError(f"JSON input is not strict UTF-8: {path}") from error
    except json.JSONDecodeError as error:
        raise OCRAdapterError(f"invalid JSON input {path}: {error}") from error


def exact_keys(value: dict[str, Any], label: str, keys: set[str]) -> None:
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    details = []
    if missing:
        details.append("missing keys: " + ", ".join(missing))
    if unknown:
        details.append("unsupported keys: " + ", ".join(unknown))
    if details:
        raise OCRAdapterError(f"{label} has " + "; ".join(details))


def object_value(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise OCRAdapterError(f"{label} must be an object")
    return value


def list_value(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise OCRAdapterError(f"{label} must be an array")
    return value


def text_value(value: Any, label: str, maximum: int = 10_000) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise OCRAdapterError(f"{label} must be a non-empty bounded string")
    return value


def identifier(value: Any, label: str) -> str:
    result = text_value(value, label, 256)
    if not ID_RE.fullmatch(result):
        raise OCRAdapterError(f"{label} contains unsupported characters")
    return result


def digest_value(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise OCRAdapterError(f"{label} must be a lowercase SHA-256")
    return value


def integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise OCRAdapterError(f"{label} must be an integer")
    if not minimum <= value <= maximum:
        raise OCRAdapterError(f"{label} must be between {minimum} and {maximum}")
    return value


def stat_row(value: os.stat_result) -> dict[str, int]:
    return {
        "device": value.st_dev,
        "inode": value.st_ino,
        "byte_count": value.st_size,
        "mtime_ns": value.st_mtime_ns,
    }


def same_stat(left: os.stat_result, right: os.stat_result) -> bool:
    return (
        left.st_dev,
        left.st_ino,
        left.st_size,
        left.st_mtime_ns,
    ) == (
        right.st_dev,
        right.st_ino,
        right.st_size,
        right.st_mtime_ns,
    )


def resolved_regular_file(value: Any, label: str, *, executable: bool = False) -> Path:
    raw = text_value(value, label)
    if "://" in raw:
        raise OCRAdapterError(f"{label} must be an absolute local path, not a URL")
    path = Path(raw)
    if not path.is_absolute():
        raise OCRAdapterError(f"{label} must be absolute")
    try:
        link_stat = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise OCRAdapterError(f"{label} is not a current local file: {error}") from error
    if path != resolved or stat.S_ISLNK(link_stat.st_mode):
        raise OCRAdapterError(f"{label} must be resolved without symlinks or traversal")
    if not stat.S_ISREG(link_stat.st_mode):
        raise OCRAdapterError(f"{label} must be a regular file")
    if executable and not os.access(path, os.X_OK):
        raise OCRAdapterError(f"{label} is not executable")
    return path


def resolved_directory(value: Any, label: str) -> Path:
    raw = text_value(value, label)
    if "://" in raw:
        raise OCRAdapterError(f"{label} must be an absolute local directory")
    path = Path(raw)
    if not path.is_absolute():
        raise OCRAdapterError(f"{label} must be absolute")
    try:
        link_stat = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise OCRAdapterError(f"{label} is unavailable: {error}") from error
    if path != resolved or stat.S_ISLNK(link_stat.st_mode):
        raise OCRAdapterError(f"{label} must be resolved without symlinks or traversal")
    if not stat.S_ISDIR(link_stat.st_mode):
        raise OCRAdapterError(f"{label} must be a directory")
    return path


def require_sealed_file(path: Path, label: str) -> None:
    try:
        value = path.lstat()
    except OSError as error:
        raise OCRAdapterError(f"{label} is unavailable: {error}") from error
    if stat.S_ISLNK(value.st_mode) or not stat.S_ISREG(value.st_mode):
        raise OCRAdapterError(f"{label} must be a regular non-symlink file")
    if value.st_nlink != 1:
        raise OCRAdapterError(f"{label} must not have alternate hard-link paths")
    if value.st_mode & 0o222:
        raise OCRAdapterError(f"{label} must be sealed with no write bits")


def require_sealed_directory(path: Path, label: str) -> None:
    try:
        value = path.lstat()
    except OSError as error:
        raise OCRAdapterError(f"{label} is unavailable: {error}") from error
    if stat.S_ISLNK(value.st_mode) or not stat.S_ISDIR(value.st_mode):
        raise OCRAdapterError(f"{label} must be a non-symlink directory")
    if value.st_mode & 0o222:
        raise OCRAdapterError(f"{label} must be sealed with no write bits")


def observe_file(
    path: Path,
    expected_sha256: str,
    label: str,
    *,
    expected_byte_count: int | None = None,
    sealed: bool = False,
) -> dict[str, Any]:
    if sealed:
        require_sealed_file(path, label)
    try:
        before_path = path.stat()
        handle = path.open("rb")
    except OSError as error:
        raise OCRAdapterError(f"{label} cannot be opened: {error}") from error
    digest = hashlib.sha256()
    try:
        before_fd = os.fstat(handle.fileno())
        if not same_stat(before_path, before_fd):
            raise OCRAdapterError(f"{label} was replaced while opening")
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
        after_fd = os.fstat(handle.fileno())
    finally:
        handle.close()
    after_path = path.stat()
    if not same_stat(before_fd, after_fd) or not same_stat(before_fd, after_path):
        raise OCRAdapterError(f"{label} changed while hashing")
    observed = digest.hexdigest()
    if observed != expected_sha256:
        raise OCRAdapterError(
            f"{label} SHA-256 mismatch: expected {expected_sha256}, observed {observed}"
        )
    if expected_byte_count is not None and before_fd.st_size != expected_byte_count:
        raise OCRAdapterError(f"{label} byte count differs from its work-order pin")
    if before_fd.st_size <= 0:
        raise OCRAdapterError(f"{label} must not be empty")
    return {
        "path": str(path),
        "sha256": observed,
        "byte_count": before_fd.st_size,
        "stat_before": stat_row(before_fd),
        "stat_after": stat_row(after_fd),
        "unchanged": True,
    }


def verify_observation(
    observation: dict[str, Any], label: str, *, sealed: bool
) -> dict[str, Any]:
    current = observe_file(
        Path(observation["path"]),
        observation["sha256"],
        label,
        expected_byte_count=observation["byte_count"],
        sealed=sealed,
    )
    if current["stat_before"] != observation["stat_before"]:
        raise OCRAdapterError(f"{label} stat identity changed during OCR processing")
    return {**observation, "stat_after": current["stat_after"], "unchanged": True}


def output_root(value: Any) -> Path:
    raw = text_value(value, "output.root")
    if "://" in raw:
        raise OCRAdapterError("output.root must be a local path")
    path = Path(raw)
    if not path.is_absolute() or path == Path("/"):
        raise OCRAdapterError("output.root must be a specific absolute directory")
    if Path(os.path.normpath(str(path))) != path:
        raise OCRAdapterError("output.root must not contain dot traversal")
    for unsafe in (Path("/tmp"), Path("/var/tmp")):
        try:
            path.relative_to(unsafe)
        except ValueError:
            pass
        else:
            raise OCRAdapterError(f"output.root must not be under {unsafe}")
    if path.exists() and not path.is_dir():
        raise OCRAdapterError("output.root must not be an existing file")
    parent = path
    while not parent.exists():
        if parent.parent == parent:
            raise OCRAdapterError("output.root has no existing parent")
        parent = parent.parent
    if parent.resolve(strict=True) != parent or parent.is_symlink():
        raise OCRAdapterError("output.root must not traverse a symlinked parent")
    return path


def inspect_png(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        header = handle.read(33)
    if len(header) < 33 or header[:8] != PNG_SIGNATURE or header[12:16] != b"IHDR":
        raise OCRAdapterError("sparse-frame artifact is not a PNG with an IHDR header")
    width, height, depth, color, compression, filtering, interlace = struct.unpack(
        ">IIBBBBB", header[16:29]
    )
    if (
        width <= 0
        or height <= 0
        or depth != 8
        or color != 2
        or compression != 0
        or filtering != 0
        or interlace != 0
    ):
        raise OCRAdapterError("sparse-frame PNG is not non-interlaced 8-bit RGB")
    return {
        "width": width,
        "height": height,
        "pixel_format": "rgb24",
        "bit_depth": 8,
        "color_type": "truecolor",
        "interlaced": False,
    }


def minimal_environment(thread_limit: int) -> dict[str, str]:
    return {
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "TZ": "UTC",
        "PATH": "/usr/bin:/bin",
        "OMP_THREAD_LIMIT": str(thread_limit),
    }


def terminate_group(process: subprocess.Popen[Any]) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def capture_version(executable: Path) -> tuple[str, str, str]:
    process = subprocess.Popen(
        [str(executable), "--version"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=minimal_environment(1),
        start_new_session=True,
    )
    try:
        output, _ = process.communicate(timeout=30)
    except subprocess.TimeoutExpired as error:
        terminate_group(process)
        raise OCRAdapterError("Tesseract --version exceeded 30 seconds") from error
    if process.returncode != 0:
        raise OCRAdapterError(
            f"Tesseract --version exited with status {process.returncode}"
        )
    if not output or len(output) > MAX_VERSION_BYTES:
        raise OCRAdapterError("Tesseract --version output is empty or oversized")
    try:
        version_output = output.decode("utf-8", errors="strict").strip()
    except UnicodeDecodeError as error:
        raise OCRAdapterError("Tesseract --version output is not UTF-8") from error
    if not version_output:
        raise OCRAdapterError("Tesseract --version returned no version label")
    return (
        version_output.splitlines()[0],
        version_output,
        sha256_bytes(version_output.encode("utf-8")),
    )


def validate_work_order(value: Any) -> dict[str, Any]:
    work = object_value(value, "work order")
    exact_keys(
        work,
        "work order",
        {
            "schema_version",
            "job_id",
            "sparse_frame_result",
            "execution_selection",
            "tesseract",
            "parameters",
            "limits",
            "output",
        },
    )
    if work.get("schema_version") != SCHEMA_VERSION:
        raise OCRAdapterError("unsupported work-order schema_version")
    job_id = text_value(work.get("job_id"), "job_id", 128)
    if not JOB_ID_RE.fullmatch(job_id):
        raise OCRAdapterError("job_id contains unsupported characters")

    source = object_value(work.get("sparse_frame_result"), "sparse_frame_result")
    exact_keys(source, "sparse_frame_result", {"path", "expected_sha256"})
    source_path = resolved_regular_file(source.get("path"), "sparse_frame_result.path")
    source_sha = digest_value(
        source.get("expected_sha256"), "sparse_frame_result.expected_sha256"
    )

    execution_selection = object_value(
        work.get("execution_selection"), "execution_selection"
    )
    exact_keys(
        execution_selection,
        "execution_selection",
        {"mode", "basis", "frame_ids"},
    )
    if execution_selection.get("mode") != "explicit_frame_ids":
        raise OCRAdapterError("execution_selection.mode must be explicit_frame_ids")
    basis = execution_selection.get("basis")
    if basis not in {"external_text_presence_candidate", "reviewer_selected"}:
        raise OCRAdapterError("execution_selection.basis is not an admitted routing basis")
    selected_frame_ids = list_value(
        execution_selection.get("frame_ids"), "execution_selection.frame_ids"
    )
    if (
        not 1 <= len(selected_frame_ids) <= MAX_FRAMES
        or len(set(selected_frame_ids)) != len(selected_frame_ids)
        or any(
            not isinstance(frame_id, str)
            or not re.fullmatch(r"frame_[0-9a-f]{32}", frame_id)
            for frame_id in selected_frame_ids
        )
    ):
        raise OCRAdapterError(
            "execution_selection.frame_ids must explicitly list 1..256 unique frame IDs"
        )

    engine = object_value(work.get("tesseract"), "tesseract")
    exact_keys(
        engine,
        "tesseract",
        {
            "executable",
            "expected_sha256",
            "expected_byte_count",
            "expected_version_output_sha256",
            "expected_version_label",
            "tessdata_dir",
            "models",
        },
    )
    executable = resolved_regular_file(
        engine.get("executable"), "tesseract.executable", executable=True
    )
    tessdata_dir = resolved_directory(engine.get("tessdata_dir"), "tesseract.tessdata_dir")
    normalized_models = []
    seen_languages: set[str] = set()
    models = list_value(engine.get("models"), "tesseract.models")
    if not 1 <= len(models) <= 32:
        raise OCRAdapterError("tesseract.models must contain 1..32 entries")
    for ordinal, raw_model in enumerate(models):
        model = object_value(raw_model, f"tesseract.models[{ordinal}]")
        exact_keys(
            model,
            f"tesseract.models[{ordinal}]",
            {"language", "path", "expected_sha256", "expected_byte_count"},
        )
        language = text_value(model.get("language"), "model.language", 32)
        if not LANGUAGE_RE.fullmatch(language) or language in seen_languages:
            raise OCRAdapterError("model languages must be unique Tesseract language codes")
        model_path = resolved_regular_file(model.get("path"), "model.path")
        if model_path.parent != tessdata_dir or model_path.name != f"{language}.traineddata":
            raise OCRAdapterError(
                "each model path must be tessdata_dir/<language>.traineddata"
            )
        seen_languages.add(language)
        normalized_models.append(
            {
                "language": language,
                "path": str(model_path),
                "expected_sha256": digest_value(
                    model.get("expected_sha256"), "model.expected_sha256"
                ),
                "expected_byte_count": integer(
                    model.get("expected_byte_count"),
                    "model.expected_byte_count",
                    1,
                    2 * 1024 * 1024 * 1024,
                ),
            }
        )
    if normalized_models != sorted(normalized_models, key=lambda row: row["language"]):
        raise OCRAdapterError("tesseract.models must be sorted by language")

    parameters = object_value(work.get("parameters"), "parameters")
    exact_keys(
        parameters,
        "parameters",
        {
            "languages",
            "oem",
            "psm",
            "dpi",
            "preserve_interword_spaces",
            "thread_limit",
            "tsv_creation",
        },
    )
    languages = list_value(parameters.get("languages"), "parameters.languages")
    if not 1 <= len(languages) <= 8 or len(set(languages)) != len(languages):
        raise OCRAdapterError("parameters.languages must contain 1..8 unique codes")
    if any(
        not isinstance(language, str)
        or not LANGUAGE_RE.fullmatch(language)
        or language not in seen_languages
        for language in languages
    ):
        raise OCRAdapterError("parameters.languages must select pinned model languages")
    if languages != sorted(languages):
        raise OCRAdapterError("parameters.languages must be sorted")
    oem = integer(parameters.get("oem"), "parameters.oem", 0, 3)
    psm = integer(parameters.get("psm"), "parameters.psm", 3, 13)
    if psm not in {3, 4, 5, 6, 7, 8, 9, 10, 11, 13}:
        raise OCRAdapterError("parameters.psm is not an admitted text-recognition mode")
    dpi = integer(parameters.get("dpi"), "parameters.dpi", 70, 1200)
    if not isinstance(parameters.get("preserve_interword_spaces"), bool):
        raise OCRAdapterError("parameters.preserve_interword_spaces must be boolean")
    if parameters.get("thread_limit") != 1:
        raise OCRAdapterError("parameters.thread_limit must remain exactly 1")
    if parameters.get("tsv_creation") != "explicit_tessedit_create_tsv_1":
        raise OCRAdapterError(
            "parameters.tsv_creation must pin explicit tessedit_create_tsv=1"
        )

    limits = object_value(work.get("limits"), "limits")
    exact_keys(
        limits,
        "limits",
        {
            "max_frames",
            "max_tsv_bytes_per_frame",
            "max_words_per_frame",
            "timeout_seconds_per_frame",
        },
    )
    normalized_limits = {
        "max_frames": integer(limits.get("max_frames"), "limits.max_frames", 1, MAX_FRAMES),
        "max_tsv_bytes_per_frame": integer(
            limits.get("max_tsv_bytes_per_frame"),
            "limits.max_tsv_bytes_per_frame",
            256,
            MAX_TSV_BYTES,
        ),
        "max_words_per_frame": integer(
            limits.get("max_words_per_frame"),
            "limits.max_words_per_frame",
            1,
            MAX_WORDS,
        ),
        "timeout_seconds_per_frame": integer(
            limits.get("timeout_seconds_per_frame"),
            "limits.timeout_seconds_per_frame",
            1,
            3600,
        ),
    }
    normalized_output = output_root(
        object_value(work.get("output"), "output").get("root")
    )
    exact_keys(object_value(work.get("output"), "output"), "output", {"root"})
    return {
        "schema_version": 1,
        "job_id": job_id,
        "sparse_frame_result": {
            "path": str(source_path),
            "expected_sha256": source_sha,
        },
        "execution_selection": {
            "mode": "explicit_frame_ids",
            "basis": basis,
            "frame_ids": selected_frame_ids,
        },
        "tesseract": {
            "executable": str(executable),
            "expected_sha256": digest_value(
                engine.get("expected_sha256"), "tesseract.expected_sha256"
            ),
            "expected_byte_count": integer(
                engine.get("expected_byte_count"),
                "tesseract.expected_byte_count",
                1,
                1024 * 1024 * 1024,
            ),
            "expected_version_output_sha256": digest_value(
                engine.get("expected_version_output_sha256"),
                "tesseract.expected_version_output_sha256",
            ),
            "expected_version_label": text_value(
                engine.get("expected_version_label"),
                "tesseract.expected_version_label",
                256,
            ),
            "tessdata_dir": str(tessdata_dir),
            "models": normalized_models,
        },
        "parameters": {
            "languages": languages,
            "oem": oem,
            "psm": psm,
            "dpi": dpi,
            "preserve_interword_spaces": parameters["preserve_interword_spaces"],
            "thread_limit": 1,
            "tsv_creation": "explicit_tessedit_create_tsv_1",
        },
        "limits": normalized_limits,
        "output": {"root": str(normalized_output)},
    }


def validate_timestamp(value: Any, label: str) -> dict[str, int]:
    timestamp = object_value(value, label)
    exact_keys(
        timestamp,
        label,
        {
            "pts",
            "duration_pts",
            "time_base_numerator",
            "time_base_denominator",
            "timestamp_us",
            "timestamp_ms",
            "duration_us",
        },
    )
    pts = integer(timestamp.get("pts"), f"{label}.pts", 0, 1 << 62)
    duration_pts = integer(
        timestamp.get("duration_pts"), f"{label}.duration_pts", 1, 1 << 62
    )
    numerator = integer(
        timestamp.get("time_base_numerator"),
        f"{label}.time_base_numerator",
        1,
        1 << 31,
    )
    denominator = integer(
        timestamp.get("time_base_denominator"),
        f"{label}.time_base_denominator",
        1,
        1 << 31,
    )
    exact_time = Fraction(pts * numerator, denominator)
    exact_duration = Fraction(duration_pts * numerator, denominator)
    expected = {
        "timestamp_us": round(exact_time * 1_000_000),
        "timestamp_ms": round(exact_time * 1_000),
        "duration_us": round(exact_duration * 1_000_000),
    }
    if any(timestamp.get(key) != expected_value for key, expected_value in expected.items()):
        raise OCRAdapterError(f"{label} rounded fields disagree with exact PTS/time base")
    return {
        "pts": pts,
        "duration_pts": duration_pts,
        "time_base_numerator": numerator,
        "time_base_denominator": denominator,
        **expected,
    }


def expected_ocr_reasons(selection_reasons: list[str]) -> list[str]:
    return [
        OCR_REASON_BY_SELECTION[reason]
        for reason in SELECTION_REASON_ORDER
        if reason in selection_reasons
    ]


def load_sparse_frame_result(
    work_order: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    """Rehash and strictly bind a completed sparse-frame result and every PNG."""

    reference = work_order["sparse_frame_result"]
    result_path = Path(reference["path"])
    require_sealed_file(result_path, "sparse-frame result")
    require_sealed_directory(result_path.parent, "sparse-frame result directory")
    require_sealed_directory(result_path.parent / "frames", "sparse-frame frames directory")
    result_observation = observe_file(
        result_path,
        reference["expected_sha256"],
        "sparse-frame result",
        sealed=True,
    )
    result = object_value(load_json(result_path), "sparse-frame result")
    exact_keys(
        result,
        "sparse-frame result",
        {
            "schema_version",
            "job_id",
            "status",
            "dry_run",
            "work_order_sha256",
            "recipe_id",
            "recipe_sha256",
            "result_key",
            "processing_run",
            "preprocess_result",
            "input_proxy",
            "ffmpeg",
            "selection",
            "commands",
            "frames",
            "artifacts",
            "result_path",
            "duration_ms",
            "errors",
        },
    )
    if (
        result.get("schema_version") != 1
        or result.get("status") != "completed"
        or result.get("dry_run") is not False
        or result.get("errors") != []
        or result.get("result_path") != str(result_path)
    ):
        raise OCRAdapterError(
            "sparse-frame result is not a completed sealed version-1 envelope"
        )
    result_key = digest_value(result.get("result_key"), "sparse-frame result_key")

    processing = object_value(result.get("processing_run"), "sparse processing_run")
    exact_keys(
        processing,
        "sparse processing_run",
        {
            "processing_run_id",
            "stage",
            "implementation_version",
            "parameters_json",
            "environment_json",
            "started_at",
            "completed_at",
            "status",
            "error_text",
        },
    )
    sparse_run_id = identifier(
        processing.get("processing_run_id"), "sparse processing_run_id"
    )
    if (
        not re.fullmatch(r"run_sparse_frames_[0-9a-f]{32}", sparse_run_id)
        or sparse_run_id != f"run_sparse_frames_{result_key[:32]}"
        or processing.get("stage") != "sparse_frame_router"
        or processing.get("status") != "completed"
        or processing.get("error_text") is not None
    ):
        raise OCRAdapterError("sparse processing_run is not a completed router run")

    preprocess = object_value(result.get("preprocess_result"), "preprocess_result")
    exact_keys(
        preprocess,
        "preprocess_result",
        {
            "path",
            "sha256",
            "byte_count",
            "stat_before",
            "stat_after",
            "unchanged",
            "processing_run_id",
            "implementation_version",
        },
    )
    preprocess_run_id = identifier(
        preprocess.get("processing_run_id"), "preprocess processing_run_id"
    )
    proxy = object_value(result.get("input_proxy"), "input_proxy")
    exact_keys(
        proxy,
        "input_proxy",
        {
            "path",
            "sha256",
            "byte_count",
            "stat_before",
            "stat_after",
            "unchanged",
            "media_id",
            "artifact_id",
            "parent_processing_run_id",
            "probe",
        },
    )
    proxy_sha = digest_value(proxy.get("sha256"), "input_proxy.sha256")
    media_id = text_value(proxy.get("media_id"), "input_proxy.media_id", 80)
    if media_id != f"media_sha256_{proxy_sha}":
        raise OCRAdapterError("input_proxy media_id does not bind its SHA-256")
    proxy_artifact_id = identifier(proxy.get("artifact_id"), "input_proxy.artifact_id")
    proxy_run_id = identifier(
        proxy.get("parent_processing_run_id"),
        "input_proxy.parent_processing_run_id",
    )
    if proxy_run_id != preprocess_run_id:
        raise OCRAdapterError("proxy and preprocess processing-run lineage disagree")

    selection = object_value(result.get("selection"), "selection")
    exact_keys(
        selection,
        "selection",
        {
            "parameters",
            "coverage",
            "candidate_counts",
            "limit_reason_codes",
            "planned_frames",
        },
    )
    planned_frames = list_value(selection.get("planned_frames"), "selection.planned_frames")
    frames = list_value(result.get("frames"), "frames")
    artifacts = list_value(result.get("artifacts"), "artifacts")
    if (
        not 1 <= len(frames) <= MAX_FRAMES
        or len(frames) != len(artifacts)
        or len(frames) != len(planned_frames)
    ):
        raise OCRAdapterError(
            "sparse frame count is empty, inconsistent, or exceeds limits.max_frames"
        )

    artifact_by_id: dict[str, tuple[dict[str, Any], dict[str, Any], Path]] = {}
    expected_paths: set[Path] = set()
    for ordinal, raw_artifact in enumerate(artifacts):
        artifact = object_value(raw_artifact, f"artifacts[{ordinal}]")
        exact_keys(
            artifact,
            f"artifacts[{ordinal}]",
            {
                "artifact_id",
                "processing_run_id",
                "artifact_kind",
                "ordinal",
                "storage_uri",
                "path",
                "sha256",
                "byte_count",
                "schema_version",
                "visibility",
                "media_kind",
                "mime_type",
                "image",
            },
        )
        artifact_id = identifier(artifact.get("artifact_id"), "frame artifact_id")
        if artifact_id in artifact_by_id:
            raise OCRAdapterError("sparse-frame artifact IDs must be unique")
        path = resolved_regular_file(artifact.get("path"), "sparse frame PNG")
        require_sealed_file(path, "sparse frame PNG")
        if path.parent != result_path.parent / "frames" or path in expected_paths:
            raise OCRAdapterError("sparse-frame artifact path escapes or is duplicated")
        expected_paths.add(path)
        png_sha = digest_value(artifact.get("sha256"), "frame artifact sha256")
        png_size = integer(
            artifact.get("byte_count"), "frame artifact byte_count", 1, 64 * 1024 * 1024
        )
        png_observation = observe_file(
            path,
            png_sha,
            "sparse frame PNG",
            expected_byte_count=png_size,
            sealed=True,
        )
        image = inspect_png(path)
        if (
            artifact.get("processing_run_id") != sparse_run_id
            or artifact.get("artifact_kind") != "sparse_frame_png"
            or artifact.get("ordinal") != ordinal
            or artifact.get("storage_uri") != path.as_uri()
            or artifact.get("schema_version") != 1
            or artifact.get("visibility") != "private"
            or artifact.get("media_kind") != "image"
            or artifact.get("mime_type") != "image/png"
            or artifact.get("image") != image
            or artifact_id
            != stable_id("artifact", sparse_run_id, "sparse_frame_png", ordinal, png_sha)
        ):
            raise OCRAdapterError("sparse-frame artifact metadata or identity is inconsistent")
        artifact_by_id[artifact_id] = (artifact, png_observation, path)

    actual_entries = set((result_path.parent / "frames").iterdir())
    if actual_entries != expected_paths or any(path.is_symlink() for path in actual_entries):
        raise OCRAdapterError("sparse-frame frames directory contains unsealed extra entries")

    lineage = {
        "source_media_id": media_id,
        "proxy_artifact_id": proxy_artifact_id,
        "proxy_parent_processing_run_id": proxy_run_id,
        "preprocess_processing_run_id": preprocess_run_id,
        "sparse_frame_result_key": result_key,
        "sparse_frame_processing_run_id": sparse_run_id,
    }
    normalized_frames: list[dict[str, Any]] = []
    for ordinal, (raw_frame, raw_planned) in enumerate(zip(frames, planned_frames, strict=True)):
        frame = object_value(raw_frame, f"frames[{ordinal}]")
        planned = object_value(raw_planned, f"selection.planned_frames[{ordinal}]")
        exact_keys(
            frame,
            f"frames[{ordinal}]",
            {
                "frame_id",
                "ordinal",
                "requested_timestamp_ms",
                "selection_reason_codes",
                "source_scene_timestamps_ms",
                "timestamp",
                "timestamp_drift_ms",
                "artifact_id",
                "ocr_routing",
            },
        )
        exact_keys(
            planned,
            f"selection.planned_frames[{ordinal}]",
            {
                "ordinal",
                "requested_timestamp_ms",
                "selection_reason_codes",
                "source_scene_timestamps_ms",
            },
        )
        if any(frame.get(key) != planned.get(key) for key in planned):
            raise OCRAdapterError("sparse-frame row differs from its exact selection row")
        if frame.get("ordinal") != ordinal:
            raise OCRAdapterError("sparse-frame ordinals must be contiguous from zero")
        reasons = list_value(frame.get("selection_reason_codes"), "selection_reason_codes")
        if (
            not reasons
            or len(set(reasons)) != len(reasons)
            or any(reason not in SELECTION_REASON_ORDER for reason in reasons)
            or reasons
            != [reason for reason in SELECTION_REASON_ORDER if reason in reasons]
        ):
            raise OCRAdapterError("sparse-frame selection reasons are invalid or unordered")
        scenes = list_value(
            frame.get("source_scene_timestamps_ms"), "source_scene_timestamps_ms"
        )
        if scenes != sorted(set(scenes)) or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in scenes
        ):
            raise OCRAdapterError("source scene timestamps are invalid or unordered")
        requested = integer(
            frame.get("requested_timestamp_ms"),
            "requested_timestamp_ms",
            0,
            604_800_000,
        )
        timestamp = validate_timestamp(frame.get("timestamp"), "frame.timestamp")
        drift = integer(frame.get("timestamp_drift_ms"), "timestamp_drift_ms", 0, 10_000)
        if drift != abs(timestamp["timestamp_ms"] - requested):
            raise OCRAdapterError("sparse-frame timestamp drift is inconsistent")
        artifact_id = identifier(frame.get("artifact_id"), "frame.artifact_id")
        artifact_entry = artifact_by_id.get(artifact_id)
        if artifact_entry is None:
            raise OCRAdapterError("sparse-frame row references an unknown artifact")
        artifact, png_observation, path = artifact_entry
        frame_id = identifier(frame.get("frame_id"), "frame.frame_id")
        expected_frame_id = stable_id(
            "frame",
            result_key,
            ordinal,
            requested,
            timestamp["pts"],
            timestamp["time_base_numerator"],
            timestamp["time_base_denominator"],
            artifact["sha256"],
        )
        route = object_value(frame.get("ocr_routing"), "frame.ocr_routing")
        exact_keys(
            route,
            "frame.ocr_routing",
            {"route", "evaluation_state", "text_presence", "reason_codes", "warning"},
        )
        ocr_reasons = expected_ocr_reasons(reasons)
        if (
            frame_id != expected_frame_id
            or route.get("route") != "queue_candidate"
            or route.get("evaluation_state") != "not_evaluated"
            or route.get("text_presence") != "unknown"
            or route.get("reason_codes") != ocr_reasons
            or not isinstance(route.get("warning"), str)
            or not route["warning"]
        ):
            raise OCRAdapterError("sparse-frame identity or OCR routing is inconsistent")
        locator = {
            **lineage,
            "frame_id": frame_id,
            "frame_artifact_id": artifact_id,
            "frame_png_sha256": artifact["sha256"],
            "ordinal": ordinal,
            "requested_timestamp_ms": requested,
            "selection_reason_codes": reasons,
            "source_scene_timestamps_ms": scenes,
            "timestamp": timestamp,
            "rounded_source_timestamp_ms": timestamp["timestamp_ms"],
            "timestamp_drift_ms": drift,
            "ocr_routing_reason_codes": ocr_reasons,
        }
        normalized_frames.append(
            {
                "locator": locator,
                "source_png": png_observation,
                "path": path,
                "width": artifact["image"]["width"],
                "height": artifact["image"]["height"],
            }
        )
    selected_ids = work_order["execution_selection"]["frame_ids"]
    selected_set = set(selected_ids)
    selected_frames = [
        frame for frame in normalized_frames if frame["locator"]["frame_id"] in selected_set
    ]
    if [frame["locator"]["frame_id"] for frame in selected_frames] != selected_ids:
        raise OCRAdapterError(
            "execution_selection.frame_ids must exist and follow upstream ordinal order"
        )
    if len(selected_frames) > work_order["limits"]["max_frames"]:
        raise OCRAdapterError("selected OCR frame count exceeds limits.max_frames")
    result_observation = verify_observation(
        result_observation, "sparse-frame result", sealed=True
    )
    return result_observation, lineage, selected_frames


def observe_tesseract(work_order: dict[str, Any]) -> dict[str, Any]:
    engine = work_order["tesseract"]
    executable = Path(engine["executable"])
    executable_observation = observe_file(
        executable,
        engine["expected_sha256"],
        "Tesseract executable",
        expected_byte_count=engine["expected_byte_count"],
        sealed=False,
    )
    version_label, version_output, version_sha = capture_version(executable)
    if (
        version_label != engine["expected_version_label"]
        or version_sha != engine["expected_version_output_sha256"]
    ):
        raise OCRAdapterError("Tesseract version output differs from its exact pin")
    executable_observation = verify_observation(
        executable_observation, "Tesseract executable", sealed=False
    )
    models = []
    for model in engine["models"]:
        observation = observe_file(
            Path(model["path"]),
            model["expected_sha256"],
            f"Tesseract {model['language']} model",
            expected_byte_count=model["expected_byte_count"],
            sealed=False,
        )
        models.append({"language": model["language"], **observation})
    return {
        "name": "tesseract",
        **executable_observation,
        "version_label": version_label,
        "version_output": version_output,
        "version_output_sha256": version_sha,
        "tessdata_dir": engine["tessdata_dir"],
        "models": models,
    }


def verify_tesseract(observation: dict[str, Any]) -> dict[str, Any]:
    executable = verify_observation(
        {
            key: observation[key]
            for key in (
                "path",
                "sha256",
                "byte_count",
                "stat_before",
                "stat_after",
                "unchanged",
            )
        },
        "Tesseract executable",
        sealed=False,
    )
    version_label, version_output, version_sha = capture_version(Path(observation["path"]))
    if (
        version_label != observation["version_label"]
        or version_output != observation["version_output"]
        or version_sha != observation["version_output_sha256"]
    ):
        raise OCRAdapterError("Tesseract version output changed during OCR processing")
    models = []
    for model in observation["models"]:
        verified = verify_observation(
            {key: model[key] for key in model if key != "language"},
            f"Tesseract {model['language']} model",
            sealed=False,
        )
        models.append({"language": model["language"], **verified})
    return {
        "name": "tesseract",
        **executable,
        "version_label": version_label,
        "version_output": version_output,
        "version_output_sha256": version_sha,
        "tessdata_dir": observation["tessdata_dir"],
        "models": models,
    }


def selected_model_digests(
    tesseract: dict[str, Any], languages: list[str]
) -> list[dict[str, str]]:
    by_language = {model["language"]: model for model in tesseract["models"]}
    return [
        {"language": language, "sha256": by_language[language]["sha256"]}
        for language in languages
    ]


def tesseract_command(
    executable: Path,
    png_path: Path,
    tessdata_dir: Path,
    parameters: dict[str, Any],
) -> list[str]:
    """Build the fixed TSV command without relying on the optional configs/tsv file."""

    return [
        str(executable),
        str(png_path),
        "stdout",
        "--tessdata-dir",
        str(tessdata_dir),
        "-l",
        "+".join(parameters["languages"]),
        "--oem",
        str(parameters["oem"]),
        "--psm",
        str(parameters["psm"]),
        "--dpi",
        str(parameters["dpi"]),
        "-c",
        "preserve_interword_spaces="
        + ("1" if parameters["preserve_interword_spaces"] else "0"),
        "-c",
        "tessedit_create_tsv=1",
    ]


def run_tesseract(
    command: list[str],
    output_path: Path,
    error_path: Path,
    *,
    timeout_seconds: int,
    max_tsv_bytes: int,
    thread_limit: int,
) -> None:
    with output_path.open("xb") as stdout, error_path.open("xb") as stderr:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            env=minimal_environment(thread_limit),
            start_new_session=True,
        )
        try:
            process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired as error:
            terminate_group(process)
            raise OCRCommandError(
                f"Tesseract exceeded the {timeout_seconds}-second frame timeout"
            ) from error
        stdout.flush()
        os.fsync(stdout.fileno())
        stderr.flush()
        os.fsync(stderr.fileno())
    if output_path.stat().st_size > max_tsv_bytes:
        raise OCRCommandError("Tesseract TSV exceeds max_tsv_bytes_per_frame")
    if error_path.stat().st_size > MAX_ERROR_BYTES:
        raise OCRCommandError("Tesseract stderr exceeds the fixed safety limit")
    if process.returncode != 0:
        error_body = error_path.read_bytes()
        tail = error_body[-16_384:].decode("utf-8", errors="replace")
        raise OCRCommandError(
            f"Tesseract exited with status {process.returncode}:\n{tail}".rstrip()
        )


def parse_tsv(
    body: bytes,
    *,
    width: int,
    height: int,
    max_words: int,
    frame_locator: dict[str, Any],
    processing_run_id: str,
    region_provenance: dict[str, Any],
) -> list[dict[str, Any]]:
    if not body or len(body) > MAX_TSV_BYTES:
        raise OCRAdapterError("Tesseract TSV is empty or exceeds the absolute limit")
    try:
        text = body.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise OCRAdapterError("Tesseract TSV is not strict UTF-8") from error
    if "\r" in text or "\x00" in text or not text.endswith("\n"):
        raise OCRAdapterError("Tesseract TSV must use terminated UTF-8 LF records")
    lines = text.splitlines()
    if not lines or lines[0] != TSV_HEADER:
        raise OCRAdapterError(
            "Tesseract output does not begin with the exact 12-column TSV header"
        )
    if len(lines) < 2:
        raise OCRAdapterError("Tesseract TSV is missing its page/hierarchy rows")
    words: list[dict[str, Any]] = []
    previous_order: tuple[int, int, int, int, int] | None = None
    for line_number, line in enumerate(lines[1:], start=2):
        if not line:
            raise OCRAdapterError(f"Tesseract TSV line {line_number} is empty")
        fields = line.split("\t")
        if len(fields) != 12:
            raise OCRAdapterError(
                f"Tesseract TSV line {line_number} does not have exactly 12 columns"
            )
        numeric_labels = (
            "level",
            "page_num",
            "block_num",
            "par_num",
            "line_num",
            "word_num",
            "left",
            "top",
            "width",
            "height",
        )
        numbers = []
        for label, raw in zip(numeric_labels, fields[:10], strict=True):
            if not re.fullmatch(r"-?[0-9]+", raw):
                raise OCRAdapterError(
                    f"Tesseract TSV line {line_number} has non-integer {label}"
                )
            numbers.append(int(raw))
        (
            level,
            page_num,
            block_num,
            par_num,
            line_num,
            word_num,
            left,
            top,
            box_width,
            box_height,
        ) = numbers
        if level not in {1, 2, 3, 4, 5}:
            raise OCRAdapterError(f"Tesseract TSV line {line_number} has invalid level")
        if min(left, top, box_width, box_height) < 0:
            raise OCRAdapterError(
                f"Tesseract TSV line {line_number} has negative box geometry"
            )
        if level != 5:
            if fields[10] != "-1" or fields[11] != "":
                raise OCRAdapterError(
                    f"Tesseract hierarchy line {line_number} has unexpected word data"
                )
            continue
        if min(page_num, block_num, par_num, line_num, word_num) < 1:
            raise OCRAdapterError(
                f"Tesseract word line {line_number} has incomplete reading-order fields"
            )
        if box_width <= 0 or box_height <= 0:
            raise OCRAdapterError(
                f"Tesseract word line {line_number} has a zero-area rectangle"
            )
        right = left + box_width
        bottom = top + box_height
        if right > width or bottom > height:
            raise OCRAdapterError(
                f"Tesseract word line {line_number} rectangle exceeds source-frame pixels"
            )
        raw_score_text = fields[10]
        if not RAW_SCORE_RE.fullmatch(raw_score_text):
            raise OCRAdapterError(
                f"Tesseract word line {line_number} score is not raw 0..100 text"
            )
        try:
            decimal_score = Decimal(raw_score_text)
        except InvalidOperation as error:
            raise OCRAdapterError(
                f"Tesseract word line {line_number} score is malformed"
            ) from error
        if not decimal_score.is_finite() or not Decimal(0) <= decimal_score <= Decimal(100):
            raise OCRAdapterError(
                f"Tesseract word line {line_number} score is outside raw 0..100"
            )
        raw_text = fields[11]
        if (
            not raw_text
            or len(raw_text) > 16_384
            or any(ord(character) < 32 for character in raw_text)
        ):
            raise OCRAdapterError(
                f"Tesseract word line {line_number} has missing or oversized raw text"
            )
        order = (page_num, block_num, par_num, line_num, word_num)
        if previous_order is not None and order <= previous_order:
            raise OCRAdapterError("Tesseract word reading order is not strictly increasing")
        previous_order = order
        if len(words) >= max_words:
            raise OCRAdapterError("Tesseract word count exceeds max_words_per_frame")
        raw_score: int | float
        if decimal_score == decimal_score.to_integral_value():
            raw_score = int(decimal_score)
        else:
            raw_score = float(decimal_score)
            if not math.isfinite(raw_score):
                raise OCRAdapterError("Tesseract raw score cannot be represented finitely")
        rectangle = {
            "left": left,
            "top": top,
            "width": box_width,
            "height": box_height,
            "right": right,
            "bottom": bottom,
            "coordinate_space": "source_frame_pixels",
        }
        reading_order = {
            "ordinal": len(words),
            "page_num": page_num,
            "block_num": block_num,
            "par_num": par_num,
            "line_num": line_num,
            "word_num": word_num,
        }
        region_id = stable_id(
            "ocr_region",
            processing_run_id,
            frame_locator["frame_id"],
            reading_order,
            rectangle,
            raw_score_text,
            raw_text,
        )
        words.append(
            {
                "region_id": region_id,
                "processing_run_id": processing_run_id,
                "frame_locator": frame_locator,
                "raw_text": raw_text,
                "raw_score": raw_score,
                "raw_score_text": raw_score_text,
                "score_scale": {
                    "minimum": 0,
                    "maximum": 100,
                    "label": "tesseract_raw_0_100",
                    "calibration_state": "not_calibrated",
                    "probability_interpretation": "not_a_probability",
                },
                "rectangle": rectangle,
                "reading_order": reading_order,
                "language_choice": {
                    "languages": region_provenance["parameters"]["languages"],
                    "tesseract_language_expression": "+".join(
                        region_provenance["parameters"]["languages"]
                    ),
                    "evidence_code": "WORK_ORDER_EXPLICIT_NOT_WORD_DETECTED",
                },
                "script_choice": {
                    "script": "unknown",
                    "evidence_code": "NOT_EMITTED_BY_TESSERACT_TSV",
                },
                "engine_provenance": region_provenance,
            }
        )
    return words


def recipe_for(
    work_order: dict[str, Any], tesseract: dict[str, Any]
) -> dict[str, Any]:
    selected = selected_model_digests(
        tesseract, work_order["parameters"]["languages"]
    )
    return {
        "contract_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "stage": STAGE,
        "engine": {
            "name": "tesseract",
            "executable_sha256": tesseract["sha256"],
            "version_output_sha256": tesseract["version_output_sha256"],
            "selected_models": selected,
        },
        "parameters": work_order["parameters"],
        "execution_selection": work_order["execution_selection"],
        "limits": work_order["limits"],
        "output_contract": "private-tesseract-tsv-word-regions-v1",
        "score_contract": {
            "scale": "raw_0_100",
            "calibration_state": "not_calibrated",
            "probability_interpretation": "not_a_probability",
        },
        "command_contract": {
            "tsv_activation": "-c tessedit_create_tsv=1",
            "named_tsv_config_used": False,
            "threads": 1,
            "network": "not_used",
        },
    }


def result_layout(
    root: Path, sparse_result_sha256: str, result_key: str
) -> tuple[Path, Path]:
    run_dir = (
        root
        / "vision"
        / "ocr"
        / "tesseract"
        / "sha256"
        / sparse_result_sha256[:2]
        / sparse_result_sha256
        / "results"
        / result_key
    )
    return run_dir, run_dir / "result.json"


def ensure_private_directory(path: Path, label: str) -> None:
    try:
        value = path.lstat()
    except OSError as error:
        raise OCRAdapterError(f"{label} is unavailable: {error}") from error
    if stat.S_ISLNK(value.st_mode) or not stat.S_ISDIR(value.st_mode):
        raise OCRAdapterError(f"{label} must be a non-symlink directory")
    if value.st_mode & 0o077:
        raise OCRAdapterError(f"{label} must not grant group/other permissions")


def prepare_private_parents(root: Path, parent: Path) -> None:
    existed = root.exists()
    root.mkdir(parents=True, mode=0o700, exist_ok=True)
    if not existed:
        os.chmod(root, 0o700)
    ensure_private_directory(root, "output.root")
    relative = parent.relative_to(root)
    current = root
    for part in relative.parts:
        current = current / part
        existed = current.exists()
        current.mkdir(mode=0o700, exist_ok=True)
        if not existed:
            os.chmod(current, 0o700)
        ensure_private_directory(current, f"private output directory {current}")


def atomic_write_private(path: Path, body: bytes) -> None:
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o400)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def open_private_lock(path: Path) -> Any:
    descriptor = os.open(
        path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    value = os.fstat(descriptor)
    if not stat.S_ISREG(value.st_mode) or value.st_nlink != 1 or value.st_mode & 0o077:
        os.close(descriptor)
        raise OCRAdapterError("OCR result lock is not a private regular file")
    return os.fdopen(descriptor, "a+b")


def tsv_artifact(
    *,
    processing_run_id: str,
    frame_id: str,
    final_path: Path,
    staged_path: Path,
) -> dict[str, Any]:
    observation = observe_file(
        staged_path,
        file_sha256(staged_path),
        "staged Tesseract TSV",
        sealed=False,
    )
    artifact_id = stable_id(
        "artifact",
        processing_run_id,
        "tesseract_tsv",
        frame_id,
        observation["sha256"],
    )
    return {
        "artifact_id": artifact_id,
        "processing_run_id": processing_run_id,
        "frame_id": frame_id,
        "path": str(final_path),
        "storage_uri": final_path.as_uri(),
        "sha256": observation["sha256"],
        "byte_count": observation["byte_count"],
        "mime_type": "text/tab-separated-values; charset=utf-8",
        "visibility": "private",
    }


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def policy_row() -> dict[str, str]:
    return {
        "visibility": "private",
        "human_review": "required",
        "publication_authority": "none",
        "identity_inference": "not_performed",
        "score_calibration": "not_calibrated",
    }


def region_provenance(
    tesseract: dict[str, Any], parameters: dict[str, Any]
) -> dict[str, Any]:
    return {
        "engine_name": "tesseract",
        "executable_sha256": tesseract["sha256"],
        "version_output_sha256": tesseract["version_output_sha256"],
        "models": selected_model_digests(tesseract, parameters["languages"]),
        "parameters": parameters,
    }


def processing_row(
    *,
    processing_run_id: str,
    recipe: dict[str, Any],
    started_at: str,
    completed_at: str,
    status: str,
) -> dict[str, Any]:
    return {
        "processing_run_id": processing_run_id,
        "stage": STAGE,
        "implementation_version": IMPLEMENTATION_VERSION,
        "parameters_json": canonical_json_text(recipe),
        "environment_json": {
            "python": sys.version.split()[0],
            "cpu_only": True,
            "network": "not_used",
            "database": "not_opened",
            "publication": "not_authorized",
            "identity_inference": "not_performed",
            "thread_limit": 1,
        },
        "started_at": started_at,
        "completed_at": completed_at,
        "status": status,
        "error_text": None,
    }


def extended_sparse_observation(
    observation: dict[str, Any], lineage: dict[str, Any]
) -> dict[str, Any]:
    return {
        **observation,
        "result_key": lineage["sparse_frame_result_key"],
        "processing_run_id": lineage["sparse_frame_processing_run_id"],
    }


def validate_completed_reuse(
    result_path: Path,
    *,
    work_order: dict[str, Any],
    work_order_sha256: str,
    recipe: dict[str, Any],
    recipe_id: str,
    recipe_sha256: str,
    result_key: str,
    processing_run_id: str,
    sparse_observation: dict[str, Any],
    lineage: dict[str, Any],
    source_frames: list[dict[str, Any]],
    tesseract: dict[str, Any],
    commands: list[list[str]],
) -> dict[str, Any]:
    require_sealed_file(result_path, "existing OCR result")
    require_sealed_directory(result_path.parent, "existing OCR result directory")
    require_sealed_directory(result_path.parent / "tsv", "existing OCR TSV directory")
    for path, mode in ((result_path, 0o400), (result_path.parent, 0o500), (result_path.parent / "tsv", 0o500)):
        if stat.S_IMODE(path.stat().st_mode) != mode:
            raise OCRAdapterError("existing OCR result is not sealed owner-private")
    result = object_value(load_json(result_path), "existing OCR result")
    exact_keys(result, "existing OCR result", RESULT_KEYS)
    expected_static = {
        "schema_version": 1,
        "job_id": work_order["job_id"],
        "status": "completed",
        "dry_run": False,
        "work_order_sha256": work_order_sha256,
        "recipe_id": recipe_id,
        "recipe_sha256": recipe_sha256,
        "result_key": result_key,
        "sparse_frame_result": extended_sparse_observation(sparse_observation, lineage),
        "source_lineage": lineage,
        "tesseract": tesseract,
        "execution_selection": work_order["execution_selection"],
        "parameters": work_order["parameters"],
        "commands": commands,
        "policy": policy_row(),
        "result_path": str(result_path),
        "errors": [],
    }
    if any(result.get(key) != value for key, value in expected_static.items()):
        raise OCRAdapterError("existing immutable OCR result identity or provenance differs")
    processing = object_value(result.get("processing_run"), "existing processing_run")
    exact_keys(
        processing,
        "existing processing_run",
        {
            "processing_run_id",
            "stage",
            "implementation_version",
            "parameters_json",
            "environment_json",
            "started_at",
            "completed_at",
            "status",
            "error_text",
        },
    )
    expected_processing = processing_row(
        processing_run_id=processing_run_id,
        recipe=recipe,
        started_at=processing.get("started_at"),
        completed_at=processing.get("completed_at"),
        status="completed",
    )
    if processing != expected_processing:
        raise OCRAdapterError("existing OCR processing-run provenance differs")
    try:
        started = datetime.fromisoformat(processing["started_at"].replace("Z", "+00:00"))
        completed = datetime.fromisoformat(
            processing["completed_at"].replace("Z", "+00:00")
        )
    except (AttributeError, ValueError) as error:
        raise OCRAdapterError("existing OCR timestamps are invalid") from error
    if not processing["started_at"].endswith("Z") or not processing["completed_at"].endswith("Z") or completed < started:
        raise OCRAdapterError("existing OCR timestamps are not ordered UTC values")
    if isinstance(result.get("duration_ms"), bool) or not isinstance(result.get("duration_ms"), int) or result["duration_ms"] < 0:
        raise OCRAdapterError("existing OCR duration_ms is invalid")

    ocr_frames = list_value(result.get("ocr_frames"), "existing ocr_frames")
    if len(ocr_frames) != len(source_frames):
        raise OCRAdapterError("existing OCR frame count differs from sparse input")
    provenance = region_provenance(tesseract, work_order["parameters"])
    expected_tsv_paths: set[Path] = set()
    for ordinal, (raw_ocr, source_frame) in enumerate(
        zip(ocr_frames, source_frames, strict=True)
    ):
        ocr = object_value(raw_ocr, f"existing ocr_frames[{ordinal}]")
        exact_keys(
            ocr,
            f"existing ocr_frames[{ordinal}]",
            {
                "frame_locator",
                "source_png",
                "tsv_artifact",
                "text_presence",
                "word_count",
                "words",
            },
        )
        if (
            ocr.get("frame_locator") != source_frame["locator"]
            or ocr.get("source_png") != source_frame["source_png"]
        ):
            raise OCRAdapterError("existing OCR frame lost its exact source locator")
        artifact = object_value(ocr.get("tsv_artifact"), "existing TSV artifact")
        exact_keys(
            artifact,
            "existing TSV artifact",
            {
                "artifact_id",
                "processing_run_id",
                "frame_id",
                "path",
                "storage_uri",
                "sha256",
                "byte_count",
                "mime_type",
                "visibility",
            },
        )
        expected_path = (
            result_path.parent
            / "tsv"
            / f"frame-{ordinal:04d}-{source_frame['locator']['frame_id']}.tsv"
        )
        path = resolved_regular_file(artifact.get("path"), "existing OCR TSV")
        require_sealed_file(path, "existing OCR TSV")
        if path != expected_path or stat.S_IMODE(path.stat().st_mode) != 0o400:
            raise OCRAdapterError("existing OCR TSV path/mode is not owner-private layout")
        expected_tsv_paths.add(path)
        tsv_observation = observe_file(
            path,
            digest_value(artifact.get("sha256"), "TSV artifact sha256"),
            "existing OCR TSV",
            expected_byte_count=integer(
                artifact.get("byte_count"), "TSV artifact byte_count", 1, MAX_TSV_BYTES
            ),
            sealed=True,
        )
        expected_artifact = {
            "artifact_id": stable_id(
                "artifact",
                processing_run_id,
                "tesseract_tsv",
                source_frame["locator"]["frame_id"],
                tsv_observation["sha256"],
            ),
            "processing_run_id": processing_run_id,
            "frame_id": source_frame["locator"]["frame_id"],
            "path": str(path),
            "storage_uri": path.as_uri(),
            "sha256": tsv_observation["sha256"],
            "byte_count": tsv_observation["byte_count"],
            "mime_type": "text/tab-separated-values; charset=utf-8",
            "visibility": "private",
        }
        if artifact != expected_artifact:
            raise OCRAdapterError("existing OCR TSV artifact identity differs")
        words = parse_tsv(
            path.read_bytes(),
            width=source_frame["width"],
            height=source_frame["height"],
            max_words=work_order["limits"]["max_words_per_frame"],
            frame_locator=source_frame["locator"],
            processing_run_id=processing_run_id,
            region_provenance=provenance,
        )
        if (
            ocr.get("words") != words
            or ocr.get("word_count") != len(words)
            or ocr.get("text_presence")
            != ("detected" if words else "not_detected")
        ):
            raise OCRAdapterError("existing OCR parsed word regions differ from raw TSV")
    actual_tsv_paths = set((result_path.parent / "tsv").iterdir())
    if actual_tsv_paths != expected_tsv_paths or any(
        path.is_symlink() for path in actual_tsv_paths
    ):
        raise OCRAdapterError("existing OCR TSV directory contains unexpected entries")
    if set(result_path.parent.iterdir()) != {result_path, result_path.parent / "tsv"}:
        raise OCRAdapterError("existing OCR result directory contains unexpected entries")
    return result


def run_ocr(work_order: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    started_at = utc_now()
    started_clock = time.monotonic()
    sparse_observation, lineage, source_frames = load_sparse_frame_result(work_order)
    tesseract = observe_tesseract(work_order)
    recipe = recipe_for(work_order, tesseract)
    recipe_sha256 = sha256_bytes(canonical_bytes(recipe))
    recipe_id = f"recipe_ocr_tesseract_{recipe_sha256[:32]}"
    work_order_sha256 = sha256_bytes(canonical_bytes(work_order))
    result_identity = {
        "work_order_sha256": work_order_sha256,
        "sparse_frame_result_sha256": sparse_observation["sha256"],
        "sparse_frame_result_key": lineage["sparse_frame_result_key"],
        "sparse_frame_processing_run_id": lineage["sparse_frame_processing_run_id"],
        "source_frames": [frame["locator"] for frame in source_frames],
        "recipe_id": recipe_id,
    }
    result_key = sha256_bytes(canonical_bytes(result_identity))
    processing_run_id = f"run_ocr_tesseract_{result_key[:32]}"
    root = Path(work_order["output"]["root"])
    source_run_dir = Path(work_order["sparse_frame_result"]["path"]).parent
    try:
        root.relative_to(source_run_dir)
    except ValueError:
        pass
    else:
        raise OCRAdapterError("output.root must not be inside the sealed sparse-frame result")
    run_dir, result_path = result_layout(root, sparse_observation["sha256"], result_key)
    commands = [
        tesseract_command(
            Path(tesseract["path"]),
            frame["path"],
            Path(tesseract["tessdata_dir"]),
            work_order["parameters"],
        )
        for frame in source_frames
    ]
    if not commands:
        raise OCRAdapterError("completed sparse-frame result unexpectedly contains no frames")
    queued_processing = processing_row(
        processing_run_id=processing_run_id,
        recipe=recipe,
        started_at=started_at,
        completed_at=utc_now(),
        status="queued" if dry_run else "completed",
    )
    common = {
        "schema_version": 1,
        "job_id": work_order["job_id"],
        "work_order_sha256": work_order_sha256,
        "recipe_id": recipe_id,
        "recipe_sha256": recipe_sha256,
        "result_key": result_key,
        "sparse_frame_result": extended_sparse_observation(
            sparse_observation, lineage
        ),
        "source_lineage": lineage,
        "tesseract": tesseract,
        "execution_selection": work_order["execution_selection"],
        "parameters": work_order["parameters"],
        "commands": commands,
        "policy": policy_row(),
        "result_path": str(result_path),
        "duration_ms": round((time.monotonic() - started_clock) * 1000),
        "errors": [],
    }
    if dry_run:
        sparse_observation = verify_observation(
            sparse_observation, "sparse-frame result", sealed=True
        )
        tesseract = verify_tesseract(tesseract)
        for frame in source_frames:
            frame["source_png"] = verify_observation(
                frame["source_png"], "sparse frame PNG", sealed=True
            )
        return {
            **common,
            "status": "planned",
            "dry_run": True,
            "processing_run": queued_processing,
            "sparse_frame_result": extended_sparse_observation(
                sparse_observation, lineage
            ),
            "tesseract": tesseract,
            "ocr_frames": [],
            "duration_ms": round((time.monotonic() - started_clock) * 1000),
        }

    prepare_private_parents(root, run_dir.parent)
    lock_path = run_dir.parent / f".{result_key}.lock"
    with open_private_lock(lock_path) as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise OCRAdapterError(
                f"another OCR writer holds the result lock: {run_dir}"
            ) from error
        sparse_observation = verify_observation(
            sparse_observation, "sparse-frame result", sealed=True
        )
        tesseract = verify_tesseract(tesseract)
        for frame in source_frames:
            frame["source_png"] = verify_observation(
                frame["source_png"], "sparse frame PNG", sealed=True
            )
        if result_path.is_file():
            return validate_completed_reuse(
                result_path,
                work_order=work_order,
                work_order_sha256=work_order_sha256,
                recipe=recipe,
                recipe_id=recipe_id,
                recipe_sha256=recipe_sha256,
                result_key=result_key,
                processing_run_id=processing_run_id,
                sparse_observation=sparse_observation,
                lineage=lineage,
                source_frames=source_frames,
                tesseract=tesseract,
                commands=commands,
            )
        if run_dir.exists() or run_dir.is_symlink():
            raise OCRAdapterError(
                "immutable OCR result directory exists without a reusable result"
            )

        stage_dir = run_dir.parent / f".{result_key}.tmp-{uuid.uuid4().hex}"
        stage_tsv = stage_dir / "tsv"
        stage_tsv.mkdir(parents=True, mode=0o700)
        os.chmod(stage_dir, 0o700)
        os.chmod(stage_tsv, 0o700)
        ocr_frames: list[dict[str, Any]] = []
        provenance = region_provenance(tesseract, work_order["parameters"])
        try:
            for ordinal, (source_frame, command) in enumerate(
                zip(source_frames, commands, strict=True)
            ):
                frame_id = source_frame["locator"]["frame_id"]
                filename = f"frame-{ordinal:04d}-{frame_id}.tsv"
                staged_path = stage_tsv / filename
                error_path = stage_tsv / f".{filename}.stderr"
                run_tesseract(
                    command,
                    staged_path,
                    error_path,
                    timeout_seconds=work_order["limits"][
                        "timeout_seconds_per_frame"
                    ],
                    max_tsv_bytes=work_order["limits"][
                        "max_tsv_bytes_per_frame"
                    ],
                    thread_limit=work_order["parameters"]["thread_limit"],
                )
                error_path.unlink(missing_ok=True)
                source_frame["source_png"] = verify_observation(
                    source_frame["source_png"], "sparse frame PNG", sealed=True
                )
                body = staged_path.read_bytes()
                words = parse_tsv(
                    body,
                    width=source_frame["width"],
                    height=source_frame["height"],
                    max_words=work_order["limits"]["max_words_per_frame"],
                    frame_locator=source_frame["locator"],
                    processing_run_id=processing_run_id,
                    region_provenance=provenance,
                )
                final_path = run_dir / "tsv" / filename
                artifact = tsv_artifact(
                    processing_run_id=processing_run_id,
                    frame_id=frame_id,
                    final_path=final_path,
                    staged_path=staged_path,
                )
                os.chmod(staged_path, 0o400)
                ocr_frames.append(
                    {
                        "frame_locator": source_frame["locator"],
                        "source_png": source_frame["source_png"],
                        "tsv_artifact": artifact,
                        "text_presence": "detected" if words else "not_detected",
                        "word_count": len(words),
                        "words": words,
                    }
                )

            sparse_observation = verify_observation(
                sparse_observation, "sparse-frame result", sealed=True
            )
            tesseract = verify_tesseract(tesseract)
            for ordinal, source_frame in enumerate(source_frames):
                source_frame["source_png"] = verify_observation(
                    source_frame["source_png"], "sparse frame PNG", sealed=True
                )
                ocr_frames[ordinal]["source_png"] = source_frame["source_png"]
            final_provenance = region_provenance(tesseract, work_order["parameters"])
            if final_provenance != provenance:
                raise OCRAdapterError("Tesseract provenance changed before result sealing")
            completed_at = utc_now()
            result = {
                **common,
                "status": "completed",
                "dry_run": False,
                "processing_run": processing_row(
                    processing_run_id=processing_run_id,
                    recipe=recipe,
                    started_at=started_at,
                    completed_at=completed_at,
                    status="completed",
                ),
                "sparse_frame_result": extended_sparse_observation(
                    sparse_observation, lineage
                ),
                "tesseract": tesseract,
                "ocr_frames": ocr_frames,
                "duration_ms": round((time.monotonic() - started_clock) * 1000),
            }
            atomic_write_private(stage_dir / "result.json", pretty_bytes(result))
            os.chmod(stage_tsv, 0o500)
            os.chmod(stage_dir, 0o500)
            try:
                os.rename(stage_dir, run_dir)
            except OSError as error:
                if error.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                    raise
                make_tree_removable(stage_dir)
                shutil.rmtree(stage_dir)
                return validate_completed_reuse(
                    result_path,
                    work_order=work_order,
                    work_order_sha256=work_order_sha256,
                    recipe=recipe,
                    recipe_id=recipe_id,
                    recipe_sha256=recipe_sha256,
                    result_key=result_key,
                    processing_run_id=processing_run_id,
                    sparse_observation=sparse_observation,
                    lineage=lineage,
                    source_frames=source_frames,
                    tesseract=tesseract,
                    commands=commands,
                )
            return result
        finally:
            if stage_dir.exists():
                make_tree_removable(stage_dir)
                shutil.rmtree(stage_dir)


def make_tree_removable(root: Path) -> None:
    for path in sorted(
        root.rglob("*"), key=lambda item: len(item.parts), reverse=True
    ):
        if path.is_symlink():
            path.unlink(missing_ok=True)
        else:
            try:
                os.chmod(path, 0o700 if path.is_dir() else 0o600)
            except FileNotFoundError:
                pass
    try:
        os.chmod(root, 0o700)
    except FileNotFoundError:
        pass


def current_pin(path: Path, label: str) -> tuple[str, int]:
    if not path.is_file() or path.is_symlink():
        raise OCRAdapterError(f"{label} must be a regular non-symlink file")
    before = path.stat()
    digest = file_sha256(path)
    after = path.stat()
    if not same_stat(before, after) or before.st_size <= 0:
        raise OCRAdapterError(f"{label} changed while its pin was generated")
    return digest, before.st_size


def default_work_order(
    *,
    job_id: str,
    sparse_result: Path,
    selection_basis: str,
    frame_ids: list[str],
    executable: Path,
    tessdata_dir: Path,
    models: list[tuple[str, Path]],
    languages: list[str],
    root: Path,
) -> dict[str, Any]:
    source_sha, _ = current_pin(sparse_result, "sparse-frame result")
    executable_sha, executable_size = current_pin(executable, "Tesseract executable")
    version_label, _, version_sha = capture_version(executable)
    model_rows = []
    for language, path in sorted(models):
        digest, byte_count = current_pin(path, f"Tesseract {language} model")
        model_rows.append(
            {
                "language": language,
                "path": str(path),
                "expected_sha256": digest,
                "expected_byte_count": byte_count,
            }
        )
    return {
        "schema_version": 1,
        "job_id": job_id,
        "sparse_frame_result": {
            "path": str(sparse_result),
            "expected_sha256": source_sha,
        },
        "execution_selection": {
            "mode": "explicit_frame_ids",
            "basis": selection_basis,
            "frame_ids": frame_ids,
        },
        "tesseract": {
            "executable": str(executable),
            "expected_sha256": executable_sha,
            "expected_byte_count": executable_size,
            "expected_version_output_sha256": version_sha,
            "expected_version_label": version_label,
            "tessdata_dir": str(tessdata_dir),
            "models": model_rows,
        },
        "parameters": {
            "languages": sorted(languages),
            "oem": 1,
            "psm": 6,
            "dpi": 300,
            "preserve_interword_spaces": True,
            "thread_limit": 1,
            "tsv_creation": "explicit_tessedit_create_tsv_1",
        },
        "limits": {
            "max_frames": len(frame_ids),
            "max_tsv_bytes_per_frame": 16 * 1024 * 1024,
            "max_words_per_frame": 25_000,
            "timeout_seconds_per_frame": 120,
        },
        "output": {"root": str(root)},
    }


def parse_model_arguments(values: list[str], tessdata_dir: Path) -> list[tuple[str, Path]]:
    result = []
    seen: set[str] = set()
    for value in values:
        language, separator, raw_path = value.partition("=")
        if (
            separator != "="
            or not LANGUAGE_RE.fullmatch(language)
            or language in seen
        ):
            raise OCRAdapterError(
                "--model must be unique LANGUAGE=/absolute/path.traineddata"
            )
        path = resolved_regular_file(raw_path, f"--model {language}")
        if path.parent != tessdata_dir or path.name != f"{language}.traineddata":
            raise OCRAdapterError(
                "--model must point to tessdata_dir/<language>.traineddata"
            )
        seen.add(language)
        result.append((language, path))
    if not result:
        raise OCRAdapterError("at least one --model is required")
    return sorted(result)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Offline private Tesseract TSV OCR over sealed sparse frames"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create-work-order")
    create.add_argument("--job-id", required=True)
    create.add_argument("--sparse-frame-result", required=True)
    create.add_argument(
        "--selection-basis",
        required=True,
        choices=("external_text_presence_candidate", "reviewer_selected"),
    )
    create.add_argument("--frame-id", action="append", required=True)
    create.add_argument("--tesseract", required=True)
    create.add_argument("--tessdata-dir", required=True)
    create.add_argument("--model", action="append", required=True)
    create.add_argument("--language", action="append", required=True)
    create.add_argument("--output-root", required=True)
    validate = commands.add_parser("validate")
    validate.add_argument("--work-order", required=True)
    run = commands.add_parser("run")
    run.add_argument("--work-order", required=True)
    run.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    job_id: str | None = None
    try:
        if args.command == "create-work-order":
            if not JOB_ID_RE.fullmatch(args.job_id):
                raise OCRAdapterError("job_id contains unsupported characters")
            sparse_result = resolved_regular_file(
                args.sparse_frame_result, "--sparse-frame-result"
            )
            executable = resolved_regular_file(
                args.tesseract, "--tesseract", executable=True
            )
            tessdata_dir = resolved_directory(args.tessdata_dir, "--tessdata-dir")
            models = parse_model_arguments(args.model, tessdata_dir)
            languages = args.language
            if (
                len(set(languages)) != len(languages)
                or any(not LANGUAGE_RE.fullmatch(value) for value in languages)
            ):
                raise OCRAdapterError("--language values must be unique Tesseract codes")
            generated = default_work_order(
                job_id=args.job_id,
                sparse_result=sparse_result,
                selection_basis=args.selection_basis,
                frame_ids=args.frame_id,
                executable=executable,
                tessdata_dir=tessdata_dir,
                models=models,
                languages=languages,
                root=output_root(args.output_root),
            )
            result = validate_work_order(generated)
            load_sparse_frame_result(result)
        else:
            work_order_path = resolved_regular_file(args.work_order, "--work-order")
            raw = load_json(work_order_path, maximum_bytes=4 * 1024 * 1024)
            if isinstance(raw, dict) and isinstance(raw.get("job_id"), str):
                job_id = raw["job_id"]
            result = validate_work_order(raw)
            if args.command == "validate":
                load_sparse_frame_result(result)
                observe_tesseract(result)
            else:
                result = run_ocr(result, dry_run=args.dry_run)
        sys.stdout.buffer.write(pretty_bytes(result))
        return 0
    except (OCRAdapterError, OSError, subprocess.SubprocessError) as error:
        failure = {
            "schema_version": 1,
            "status": "failed",
            "job_id": job_id,
            "error": {"type": type(error).__name__, "message": str(error)},
            "errors": [{"type": type(error).__name__, "message": str(error)}],
        }
        sys.stderr.buffer.write(pretty_bytes(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
