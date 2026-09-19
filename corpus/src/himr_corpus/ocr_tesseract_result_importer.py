"""Strict private admission and search for completed Tesseract TSV OCR results.

The producer result is untrusted input.  This boundary replays its immutable file,
upstream sparse-frame, PNG, TSV, coordinate, and deterministic-ID contracts before
writing searchable machine text.  Admitted text stays private, redaction-pending,
uncalibrated, and without identity, event, claim, publication, gate, or export
authority.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import stat
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from pathlib import Path
from typing import Any

from .asr_result_importer import (
    _absolute_observed_path,
    _array,
    _exact_keys,
    _identifier,
    _integer,
    _local_file_uri,
    _object,
    _sha256,
    _stable_read,
    _string,
    _timestamp,
    _timestamp_value,
    _verify_hash,
)
from .db import require_sqlite_version, transaction, utc_now
from .ids import stable_id
from .importers import canonical_json, sha256_bytes
from .result_importers import ResultImportError
from .sparse_frame_result_importer import (
    _observation_interval as _sparse_observation_interval,
    _observation_metadata as _sparse_observation_metadata,
    _read_result as _read_sparse_frame_result,
    _require_catalog_lineage as _require_sparse_catalog_lineage,
)


SCHEMA_VERSION = 1
STAGE = "ocr_tesseract_tsv"
IMPORTER_NAME = "private_ocr_tesseract_result_v1"
IMPLEMENTATION_VERSION = "private-ocr-tesseract-admission/2"
MAX_RESULT_BYTES = 128 * 1024 * 1024
MAX_TSV_BYTES = 64 * 1024 * 1024
MAX_PINNED_ASSET_BYTES = 2 * 1024 * 1024 * 1024
MAX_FRAMES = 256
MAX_WORDS_PER_FRAME = 100_000
TSV_HEADER = (
    "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\t"
    "left\ttop\twidth\theight\tconf\ttext"
)
RAW_SCORE_RE = re.compile(
    r"^(?:0(?:\.[0-9]+)?|[1-9][0-9]?(?:\.[0-9]+)?|100(?:\.0+)?)$"
)
RUN_ID_RE = re.compile(r"^run_ocr_tesseract_[0-9a-f]{32}$")
RECIPE_ID_RE = re.compile(r"^recipe_ocr_tesseract_[0-9a-f]{32}$")
FRAME_ID_RE = re.compile(r"^frame_[0-9a-f]{32}$")
ARTIFACT_ID_RE = re.compile(r"^artifact_[0-9a-f]{32}$")
LANGUAGE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$")
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
POLICY = {
    "visibility": "private",
    "human_review": "required",
    "publication_authority": "none",
    "identity_inference": "not_performed",
    "score_calibration": "not_calibrated",
}
FTS_INTEGRITY_MINIMUM_SQLITE = (3, 44, 0)
TOP_LEVEL_KEYS = {
    "schema_version", "job_id", "status", "dry_run", "work_order_sha256",
    "recipe_id", "recipe_sha256", "result_key", "processing_run",
    "sparse_frame_result", "source_lineage", "tesseract",
    "execution_selection", "parameters", "commands", "ocr_frames", "policy",
    "result_path", "duration_ms", "errors",
}


def _duplicate_key_guard(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ResultImportError(f"OCR result contains duplicate JSON key {key!r}")
        value[key] = item
    return value


def _canonical_equal(left: object, right: object) -> bool:
    return canonical_json(left) == canonical_json(right)


def _producer_id(prefix: str, *parts: object) -> str:
    """Rebuild the pipeline producer's SHA-based opaque identifiers."""

    return f"{prefix}_" + sha256_bytes(canonical_json(list(parts)).encode("utf-8"))[:32]


def _sealed_file(value: object, label: str) -> Path:
    path = _absolute_observed_path(value, label)
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise ResultImportError(f"{label} cannot be inspected: {error}") from error
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode) or mode & 0o222:
        raise ResultImportError(f"{label} must be a sealed read-only regular file")
    return path


def _sealed_directory(path: Path, label: str) -> None:
    try:
        resolved = path.resolve(strict=True)
        mode = path.lstat().st_mode
    except OSError as error:
        raise ResultImportError(f"{label} cannot be inspected: {error}") from error
    if path != resolved or stat.S_ISLNK(mode) or not stat.S_ISDIR(mode) or mode & 0o222:
        raise ResultImportError(f"{label} must be a sealed non-symlink directory")


def _current_stat(path: Path) -> dict[str, int]:
    value = path.stat()
    return {
        "device": value.st_dev,
        "inode": value.st_ino,
        "byte_count": value.st_size,
        "mtime_ns": value.st_mtime_ns,
    }


def _stat_observation(value: object, label: str, byte_count: int) -> dict[str, int]:
    row = _object(value, label)
    _exact_keys(row, label, {"device", "inode", "byte_count", "mtime_ns"})
    normalized = {
        "device": _integer(row["device"], f"{label}.device"),
        "inode": _integer(row["inode"], f"{label}.inode", minimum=0),
        "byte_count": _integer(row["byte_count"], f"{label}.byte_count", minimum=0),
        "mtime_ns": _integer(row["mtime_ns"], f"{label}.mtime_ns"),
    }
    if normalized["byte_count"] != byte_count:
        raise ResultImportError(f"{label}.byte_count disagrees with its file")
    return normalized


def _file_observation(
    value: object,
    label: str,
    *,
    sealed: bool,
    maximum_bytes: int,
    extra_keys: set[str] | None = None,
) -> tuple[dict[str, Any], Path]:
    row = _object(value, label)
    common = {"path", "sha256", "byte_count", "stat_before", "stat_after", "unchanged"}
    _exact_keys(row, label, common | (extra_keys or set()))
    path = _sealed_file(row["path"], f"{label}.path") if sealed else _absolute_observed_path(
        row["path"], f"{label}.path"
    )
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise ResultImportError(f"{label} cannot be inspected: {error}") from error
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise ResultImportError(f"{label} must be a regular non-symlink file")
    digest = _sha256(row["sha256"], f"{label}.sha256")
    byte_count = _integer(
        row["byte_count"], f"{label}.byte_count", minimum=1, maximum=maximum_bytes
    )
    before = _stat_observation(row["stat_before"], f"{label}.stat_before", byte_count)
    after = _stat_observation(row["stat_after"], f"{label}.stat_after", byte_count)
    if row["unchanged"] is not True or before != after:
        raise ResultImportError(f"{label} does not carry an unchanged observation")
    if _current_stat(path) != after:
        raise ResultImportError(f"{label} current stat differs from its sealed observation")
    _verify_hash(path, digest, byte_count, label)
    return dict(row), path


def _plain_file_pin(
    value: object, label: str, *, extra_keys: set[str]
) -> tuple[dict[str, Any], Path]:
    return _file_observation(
        value,
        label,
        sealed=False,
        maximum_bytes=MAX_PINNED_ASSET_BYTES,
        extra_keys=extra_keys,
    )


def _parameters(value: object) -> dict[str, Any]:
    row = _object(value, "OCR parameters")
    _exact_keys(
        row,
        "OCR parameters",
        {"languages", "oem", "psm", "dpi", "preserve_interword_spaces", "thread_limit", "tsv_creation"},
    )
    languages = _array(row["languages"], "OCR parameters.languages")
    if (
        not 1 <= len(languages) <= 8
        or len(set(languages)) != len(languages)
        or any(not isinstance(item, str) or not LANGUAGE_RE.fullmatch(item) for item in languages)
    ):
        raise ResultImportError("OCR languages must be unique bounded language IDs")
    if type(row["preserve_interword_spaces"]) is not bool:
        raise ResultImportError("OCR preserve_interword_spaces must be a boolean")
    normalized = {
        "languages": list(languages),
        "oem": _integer(row["oem"], "OCR parameters.oem", minimum=0, maximum=3),
        "psm": _integer(row["psm"], "OCR parameters.psm", minimum=3, maximum=13),
        "dpi": _integer(row["dpi"], "OCR parameters.dpi", minimum=70, maximum=1200),
        "preserve_interword_spaces": row["preserve_interword_spaces"],
        "thread_limit": _integer(row["thread_limit"], "OCR parameters.thread_limit", minimum=1, maximum=1),
        "tsv_creation": _string(row["tsv_creation"], "OCR parameters.tsv_creation", maximum=64),
    }
    if normalized["psm"] not in {3, 4, 5, 6, 7, 8, 9, 10, 11, 13}:
        raise ResultImportError("OCR psm is outside the admitted set")
    if normalized["tsv_creation"] != "explicit_tessedit_create_tsv_1":
        raise ResultImportError("OCR TSV creation mode is not explicit")
    return normalized


def _timestamp_locator(value: object, label: str) -> dict[str, int]:
    row = _object(value, label)
    _exact_keys(
        row,
        label,
        {"pts", "duration_pts", "time_base_numerator", "time_base_denominator", "timestamp_us", "timestamp_ms", "duration_us"},
    )
    pts = _integer(row["pts"], f"{label}.pts", minimum=0)
    duration_pts = _integer(row["duration_pts"], f"{label}.duration_pts", minimum=1)
    numerator = _integer(row["time_base_numerator"], f"{label}.time_base_numerator", minimum=1)
    denominator = _integer(row["time_base_denominator"], f"{label}.time_base_denominator", minimum=1)
    expected = {
        "timestamp_us": round(Fraction(pts * numerator * 1_000_000, denominator)),
        "timestamp_ms": round(Fraction(pts * numerator * 1_000, denominator)),
        "duration_us": round(
            Fraction(duration_pts * numerator * 1_000_000, denominator)
        ),
    }
    if any(row[key] != expected[key] for key in expected):
        raise ResultImportError(f"{label} rounded coordinates disagree with exact PTS")
    return {
        "pts": pts,
        "duration_pts": duration_pts,
        "time_base_numerator": numerator,
        "time_base_denominator": denominator,
        **expected,
    }


def _expected_locator(
    sparse: dict[str, Any], frame: dict[str, Any], artifact: dict[str, Any]
) -> dict[str, Any]:
    reasons = list(frame["selection_reason_codes"])
    return {
        "source_media_id": sparse["input_proxy"]["media_id"],
        "proxy_artifact_id": sparse["input_proxy"]["artifact_id"],
        "proxy_parent_processing_run_id": sparse["input_proxy"]["parent_processing_run_id"],
        "preprocess_processing_run_id": sparse["preprocess_result"]["processing_run_id"],
        "sparse_frame_result_key": sparse["result_key"],
        "sparse_frame_processing_run_id": sparse["processing_run"]["processing_run_id"],
        "frame_id": frame["frame_id"],
        "frame_artifact_id": artifact["artifact_id"],
        "frame_png_sha256": artifact["sha256"],
        "ordinal": frame["ordinal"],
        "requested_timestamp_ms": frame["requested_timestamp_ms"],
        "selection_reason_codes": reasons,
        "source_scene_timestamps_ms": frame["source_scene_timestamps_ms"],
        "timestamp": frame["timestamp"],
        "rounded_source_timestamp_ms": frame["timestamp"]["timestamp_ms"],
        "timestamp_drift_ms": frame["timestamp_drift_ms"],
        "ocr_routing_reason_codes": [
            OCR_REASON_BY_SELECTION[reason]
            for reason in SELECTION_REASON_ORDER
            if reason in reasons
        ],
    }


def _rectangle(value: object, label: str, width: int, height: int) -> dict[str, Any]:
    row = _object(value, label)
    _exact_keys(row, label, {"left", "top", "width", "height", "right", "bottom", "coordinate_space"})
    normalized = {
        "left": _integer(row["left"], f"{label}.left", minimum=0),
        "top": _integer(row["top"], f"{label}.top", minimum=0),
        "width": _integer(row["width"], f"{label}.width", minimum=1),
        "height": _integer(row["height"], f"{label}.height", minimum=1),
        "right": _integer(row["right"], f"{label}.right", minimum=1),
        "bottom": _integer(row["bottom"], f"{label}.bottom", minimum=1),
        "coordinate_space": row["coordinate_space"],
    }
    if (
        normalized["coordinate_space"] != "source_frame_pixels"
        or normalized["right"] != normalized["left"] + normalized["width"]
        or normalized["bottom"] != normalized["top"] + normalized["height"]
        or normalized["right"] > width
        or normalized["bottom"] > height
    ):
        raise ResultImportError(f"{label} is outside exact source-frame pixels")
    return normalized


def _region_provenance(engine: dict[str, Any], parameters: dict[str, Any]) -> dict[str, Any]:
    by_language = {model["language"]: model for model in engine["models"]}
    return {
        "engine_name": "tesseract",
        "executable_sha256": engine["sha256"],
        "version_output_sha256": engine["version_output_sha256"],
        "models": [
            {"language": language, "sha256": by_language[language]["sha256"]}
            for language in parameters["languages"]
        ],
        "parameters": parameters,
    }


def _parse_tsv(
    body: bytes,
    *,
    width: int,
    height: int,
    frame_locator: dict[str, Any],
    processing_run_id: str,
    provenance: dict[str, Any],
) -> list[dict[str, Any]]:
    if not body or len(body) > MAX_TSV_BYTES:
        raise ResultImportError("Tesseract TSV is empty or exceeds the admission limit")
    try:
        text = body.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise ResultImportError("Tesseract TSV is not strict UTF-8") from error
    if "\r" in text or "\x00" in text or not text.endswith("\n"):
        raise ResultImportError("Tesseract TSV must use terminated UTF-8 LF records")
    lines = text.splitlines()
    if not lines or lines[0] != TSV_HEADER or len(lines) < 2:
        raise ResultImportError("Tesseract TSV lacks its exact header or hierarchy")
    words: list[dict[str, Any]] = []
    previous_order: tuple[int, int, int, int, int] | None = None
    for line_number, line in enumerate(lines[1:], start=2):
        fields = line.split("\t")
        if not line or len(fields) != 12:
            raise ResultImportError(f"Tesseract TSV line {line_number} is malformed")
        if any(not re.fullmatch(r"-?[0-9]+", item) for item in fields[:10]):
            raise ResultImportError(f"Tesseract TSV line {line_number} has non-integer geometry")
        values = [int(item) for item in fields[:10]]
        level, page, block, paragraph, line_no, word_no, left, top, box_width, box_height = values
        if level not in {1, 2, 3, 4, 5} or min(left, top, box_width, box_height) < 0:
            raise ResultImportError(f"Tesseract TSV line {line_number} has invalid hierarchy or geometry")
        if level != 5:
            if fields[10] != "-1" or fields[11] != "":
                raise ResultImportError(f"Tesseract hierarchy line {line_number} carries word data")
            continue
        if min(page, block, paragraph, line_no, word_no, box_width, box_height) < 1:
            raise ResultImportError(f"Tesseract word line {line_number} is incomplete")
        right, bottom = left + box_width, top + box_height
        if right > width or bottom > height:
            raise ResultImportError(f"Tesseract word line {line_number} exceeds source pixels")
        raw_score_text = fields[10]
        if not RAW_SCORE_RE.fullmatch(raw_score_text):
            raise ResultImportError(f"Tesseract word line {line_number} has invalid raw score")
        try:
            score_decimal = Decimal(raw_score_text)
        except InvalidOperation as error:
            raise ResultImportError("Tesseract raw score is malformed") from error
        if not score_decimal.is_finite() or not Decimal(0) <= score_decimal <= Decimal(100):
            raise ResultImportError("Tesseract raw score is outside 0..100")
        raw_text = fields[11]
        if not raw_text or len(raw_text) > 16_384 or any(ord(char) < 32 for char in raw_text):
            raise ResultImportError(f"Tesseract word line {line_number} has invalid raw text")
        order = (page, block, paragraph, line_no, word_no)
        if previous_order is not None and order <= previous_order:
            raise ResultImportError("Tesseract word reading order is not strictly increasing")
        previous_order = order
        if len(words) >= MAX_WORDS_PER_FRAME:
            raise ResultImportError("Tesseract word count exceeds the admission limit")
        score: int | float
        if score_decimal == score_decimal.to_integral_value():
            score = int(score_decimal)
        else:
            score = float(score_decimal)
            if not math.isfinite(score):
                raise ResultImportError("Tesseract raw score cannot be represented finitely")
        rectangle = {
            "left": left, "top": top, "width": box_width, "height": box_height,
            "right": right, "bottom": bottom, "coordinate_space": "source_frame_pixels",
        }
        reading = {
            "ordinal": len(words), "page_num": page, "block_num": block,
            "par_num": paragraph, "line_num": line_no, "word_num": word_no,
        }
        region_id = _producer_id(
            "ocr_region", processing_run_id, frame_locator["frame_id"], reading,
            rectangle, raw_score_text, raw_text,
        )
        words.append(
            {
                "region_id": region_id,
                "processing_run_id": processing_run_id,
                "frame_locator": frame_locator,
                "raw_text": raw_text,
                "raw_score": score,
                "raw_score_text": raw_score_text,
                "score_scale": {
                    "minimum": 0,
                    "maximum": 100,
                    "label": "tesseract_raw_0_100",
                    "calibration_state": "not_calibrated",
                    "probability_interpretation": "not_a_probability",
                },
                "rectangle": rectangle,
                "reading_order": reading,
                "language_choice": {
                    "languages": provenance["parameters"]["languages"],
                    "tesseract_language_expression": "+".join(provenance["parameters"]["languages"]),
                    "evidence_code": "WORK_ORDER_EXPLICIT_NOT_WORD_DETECTED",
                },
                "script_choice": {
                    "script": "unknown",
                    "evidence_code": "NOT_EMITTED_BY_TESSERACT_TSV",
                },
                "engine_provenance": provenance,
            }
        )
    return words


def _validate_engine(value: object, parameters: dict[str, Any]) -> dict[str, Any]:
    extra = {"name", "version_label", "version_output", "version_output_sha256", "tessdata_dir", "models"}
    row, path = _plain_file_pin(value, "Tesseract executable", extra_keys=extra)
    if row["name"] != "tesseract" or not _string(row["version_label"], "Tesseract version label", maximum=256):
        raise ResultImportError("Tesseract engine identity is invalid")
    version_output = _string(row["version_output"], "Tesseract version output", maximum=64_000)
    version_sha = _sha256(row["version_output_sha256"], "Tesseract version output SHA-256")
    if sha256_bytes(version_output.encode("utf-8")) != version_sha:
        raise ResultImportError("Tesseract version-output SHA-256 differs")
    tessdata_text = _string(row["tessdata_dir"], "Tesseract tessdata_dir", maximum=4096)
    tessdata_dir = Path(tessdata_text)
    if not tessdata_dir.is_absolute() or tessdata_dir != tessdata_dir.resolve(strict=True) or not tessdata_dir.is_dir():
        raise ResultImportError("Tesseract tessdata_dir must be a resolved local directory")
    models = _array(row["models"], "Tesseract models")
    if not 1 <= len(models) <= 32:
        raise ResultImportError("Tesseract models count is invalid")
    normalized_models: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ordinal, raw_model in enumerate(models):
        model, model_path = _plain_file_pin(
            raw_model, f"Tesseract model[{ordinal}]", extra_keys={"language"}
        )
        language = _string(model["language"], f"Tesseract model[{ordinal}].language", maximum=32)
        if not LANGUAGE_RE.fullmatch(language) or language in seen:
            raise ResultImportError("Tesseract model languages are invalid or duplicated")
        if model_path.parent != tessdata_dir or model_path.name != f"{language}.traineddata":
            raise ResultImportError("Tesseract model path is outside the exact tessdata bundle")
        seen.add(language)
        normalized_models.append(dict(model))
    if normalized_models != sorted(normalized_models, key=lambda item: item["language"]):
        raise ResultImportError("Tesseract model pins are not sorted by language")
    if any(language not in seen for language in parameters["languages"]):
        raise ResultImportError("Tesseract selected language lacks an exact model pin")
    return {**dict(row), "path": str(path), "models": normalized_models}


def _read_ocr_result(result_path: str | Path) -> dict[str, Any]:
    path = Path(result_path)
    if not path.is_absolute():
        path = path.resolve()
    path = _sealed_file(str(path), "OCR result")
    _sealed_directory(path.parent, "OCR result directory")
    _sealed_directory(path.parent / "tsv", "OCR TSV directory")
    body = _stable_read(path, "OCR result", maximum_bytes=MAX_RESULT_BYTES)
    try:
        value = json.loads(body.decode("utf-8"), object_pairs_hook=_duplicate_key_guard)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResultImportError(f"OCR result must be strict UTF-8 JSON: {error}") from error
    result = _object(value, "OCR result")
    _exact_keys(result, "OCR result", TOP_LEVEL_KEYS)
    if (
        result["schema_version"] != SCHEMA_VERSION
        or result["status"] != "completed"
        or result["dry_run"] is not False
        or result["errors"] != []
        or result["result_path"] != str(path)
    ):
        raise ResultImportError("OCR result is not a completed non-dry sealed v1 envelope")
    _identifier(result["job_id"], "OCR job_id")
    work_order_sha = _sha256(result["work_order_sha256"], "OCR work_order_sha256")
    recipe_sha = _sha256(result["recipe_sha256"], "OCR recipe_sha256")
    recipe_id = _identifier(result["recipe_id"], "OCR recipe_id")
    result_key = _sha256(result["result_key"], "OCR result_key")
    if not RECIPE_ID_RE.fullmatch(recipe_id) or recipe_id != f"recipe_ocr_tesseract_{recipe_sha[:32]}":
        raise ResultImportError("OCR recipe identity is inconsistent")
    _integer(result["duration_ms"], "OCR duration_ms", minimum=0)
    if result["policy"] != POLICY:
        raise ResultImportError("OCR result weakens its private review policy")

    parameters = _parameters(result["parameters"])
    engine = _validate_engine(result["tesseract"], parameters)
    processing = _object(result["processing_run"], "OCR processing_run")
    _exact_keys(
        processing,
        "OCR processing_run",
        {"processing_run_id", "stage", "implementation_version", "parameters_json", "environment_json", "started_at", "completed_at", "status", "error_text"},
    )
    run_id = _identifier(processing["processing_run_id"], "OCR processing_run_id")
    if (
        not RUN_ID_RE.fullmatch(run_id)
        or run_id != f"run_ocr_tesseract_{result_key[:32]}"
        or processing["stage"] != STAGE
        or processing["status"] != "completed"
        or processing["error_text"] is not None
    ):
        raise ResultImportError("OCR processing run identity or state is inconsistent")
    started = _timestamp(processing["started_at"], "OCR started_at")
    completed = _timestamp(processing["completed_at"], "OCR completed_at")
    if started != processing["started_at"] or completed != processing["completed_at"] or _timestamp_value(completed) < _timestamp_value(started):
        raise ResultImportError("OCR processing timestamps must be ordered canonical UTC")
    environment = _object(processing["environment_json"], "OCR environment_json")
    _exact_keys(environment, "OCR environment_json", {"python", "cpu_only", "network", "database", "publication", "identity_inference", "thread_limit"})
    if (
        not _string(environment["python"], "OCR environment python", maximum=128)
        or environment["cpu_only"] is not True
        or environment["network"] != "not_used"
        or environment["database"] != "not_opened"
        or environment["publication"] != "not_authorized"
        or environment["identity_inference"] != "not_performed"
        or environment["thread_limit"] != 1
    ):
        raise ResultImportError("OCR processing environment weakens the offline boundary")
    parameters_text = _string(processing["parameters_json"], "OCR parameters_json", maximum=1_000_000)
    try:
        recipe = json.loads(parameters_text, object_pairs_hook=_duplicate_key_guard)
    except json.JSONDecodeError as error:
        raise ResultImportError("OCR parameters_json is invalid") from error
    if canonical_json(recipe) != parameters_text or sha256_bytes(parameters_text.encode("utf-8")) != recipe_sha:
        raise ResultImportError("OCR recipe JSON or digest is inconsistent")
    recipe_row = _object(recipe, "OCR recipe")
    _exact_keys(recipe_row, "OCR recipe", {"contract_version", "implementation_version", "stage", "engine", "execution_selection", "parameters", "limits", "output_contract", "score_contract", "command_contract"})
    if (
        recipe_row["contract_version"] != 1
        or recipe_row["implementation_version"] != processing["implementation_version"]
        or recipe_row["stage"] != STAGE
        or recipe_row["parameters"] != parameters
        or recipe_row["execution_selection"] != result["execution_selection"]
        or recipe_row["output_contract"] != "private-tesseract-tsv-word-regions-v1"
        or recipe_row["score_contract"] != {"scale": "raw_0_100", "calibration_state": "not_calibrated", "probability_interpretation": "not_a_probability"}
        or recipe_row["command_contract"] != {"tsv_activation": "-c tessedit_create_tsv=1", "named_tsv_config_used": False, "threads": 1, "network": "not_used"}
    ):
        raise ResultImportError("OCR recipe semantics differ from the completed result")
    recipe_engine = _object(recipe_row["engine"], "OCR recipe.engine")
    _exact_keys(recipe_engine, "OCR recipe.engine", {"name", "executable_sha256", "version_output_sha256", "selected_models"})
    model_by_language = {model["language"]: model for model in engine["models"]}
    expected_recipe_engine = {
        "name": "tesseract",
        "executable_sha256": engine["sha256"],
        "version_output_sha256": engine["version_output_sha256"],
        "selected_models": [
            {"language": language, "sha256": model_by_language[language]["sha256"]}
            for language in parameters["languages"]
        ],
    }
    if recipe_engine != expected_recipe_engine:
        raise ResultImportError("OCR recipe engine pins differ from current verified pins")
    limits = _object(recipe_row["limits"], "OCR recipe.limits")
    _exact_keys(limits, "OCR recipe.limits", {"max_frames", "max_tsv_bytes_per_frame", "max_words_per_frame", "timeout_seconds_per_frame"})
    max_frames = _integer(limits["max_frames"], "OCR limits.max_frames", minimum=1, maximum=MAX_FRAMES)
    max_tsv = _integer(limits["max_tsv_bytes_per_frame"], "OCR limits.max_tsv_bytes_per_frame", minimum=1, maximum=MAX_TSV_BYTES)
    max_words = _integer(limits["max_words_per_frame"], "OCR limits.max_words_per_frame", minimum=1, maximum=MAX_WORDS_PER_FRAME)
    _integer(limits["timeout_seconds_per_frame"], "OCR limits.timeout_seconds_per_frame", minimum=1, maximum=600)

    selection = _object(result["execution_selection"], "OCR execution_selection")
    _exact_keys(selection, "OCR execution_selection", {"mode", "basis", "frame_ids"})
    frame_ids = _array(selection["frame_ids"], "OCR execution_selection.frame_ids")
    if (
        selection["mode"] != "explicit_frame_ids"
        or selection["basis"] not in {"external_text_presence_candidate", "reviewer_selected"}
        or not 1 <= len(frame_ids) <= max_frames
        or len(set(frame_ids)) != len(frame_ids)
        or any(not isinstance(item, str) or not FRAME_ID_RE.fullmatch(item) for item in frame_ids)
    ):
        raise ResultImportError("OCR execution selection is invalid")

    sparse_reference, sparse_path = _file_observation(
        result["sparse_frame_result"],
        "OCR sparse-frame result",
        sealed=True,
        maximum_bytes=MAX_RESULT_BYTES,
        extra_keys={"result_key", "processing_run_id"},
    )
    if sparse_path.parent.name != sparse_reference["result_key"]:
        raise ResultImportError("OCR sparse-frame result layout and result key differ")
    sparse = _read_sparse_frame_result(sparse_path)
    if (
        sparse["_sha256"] != sparse_reference["sha256"]
        or len(sparse["_body"]) != sparse_reference["byte_count"]
        or sparse["result_key"] != sparse_reference["result_key"]
        or sparse["processing_run"]["processing_run_id"] != sparse_reference["processing_run_id"]
    ):
        raise ResultImportError("OCR sparse-frame reference differs from the strict upstream replay")
    lineage = {
        "source_media_id": sparse["input_proxy"]["media_id"],
        "proxy_artifact_id": sparse["input_proxy"]["artifact_id"],
        "proxy_parent_processing_run_id": sparse["input_proxy"]["parent_processing_run_id"],
        "preprocess_processing_run_id": sparse["preprocess_result"]["processing_run_id"],
        "sparse_frame_result_key": sparse["result_key"],
        "sparse_frame_processing_run_id": sparse["processing_run"]["processing_run_id"],
    }
    if result["source_lineage"] != lineage:
        raise ResultImportError("OCR source lineage differs from strict sparse-frame replay")

    upstream_by_id: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for frame, artifact in zip(sparse["frames"], sparse["artifacts"], strict=True):
        upstream_by_id[frame["frame_id"]] = (frame, artifact)
    selected_pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    last_ordinal = -1
    for frame_id in frame_ids:
        pair = upstream_by_id.get(frame_id)
        if pair is None or pair[0]["ordinal"] <= last_ordinal:
            raise ResultImportError("OCR selected frames are missing or out of upstream order")
        last_ordinal = pair[0]["ordinal"]
        selected_pairs.append(pair)

    ocr_frames = _array(result["ocr_frames"], "OCR frames")
    commands = _array(result["commands"], "OCR commands")
    if len(ocr_frames) != len(selected_pairs) or len(commands) != len(selected_pairs):
        raise ResultImportError("OCR frame, command, and selection counts differ")
    provenance = _region_provenance(engine, parameters)
    normalized_frames: list[dict[str, Any]] = []
    expected_tsv_paths: set[Path] = set()
    source_locators: list[dict[str, Any]] = []
    for selected_ordinal, ((frame, artifact), raw_ocr, raw_command) in enumerate(
        zip(selected_pairs, ocr_frames, commands, strict=True)
    ):
        expected_locator = _expected_locator(sparse, frame, artifact)
        source_locators.append(expected_locator)
        ocr = _object(raw_ocr, f"OCR frames[{selected_ordinal}]")
        _exact_keys(ocr, f"OCR frames[{selected_ordinal}]", {"frame_locator", "source_png", "tsv_artifact", "text_presence", "word_count", "words"})
        if not _canonical_equal(ocr["frame_locator"], expected_locator):
            raise ResultImportError("OCR frame lost its exact sparse-frame locator")
        _timestamp_locator(ocr["frame_locator"]["timestamp"], "OCR frame timestamp")
        source_png, source_png_path = _file_observation(
            ocr["source_png"], f"OCR source PNG[{selected_ordinal}]", sealed=True,
            maximum_bytes=64 * 1024 * 1024,
        )
        if (
            source_png_path != artifact["_path"]
            or source_png["sha256"] != artifact["sha256"]
            or source_png["byte_count"] != artifact["byte_count"]
        ):
            raise ResultImportError("OCR source PNG differs from strict sparse-frame bytes")
        tsv = _object(ocr["tsv_artifact"], f"OCR TSV artifact[{selected_ordinal}]")
        _exact_keys(tsv, f"OCR TSV artifact[{selected_ordinal}]", {"artifact_id", "processing_run_id", "frame_id", "path", "storage_uri", "sha256", "byte_count", "mime_type", "visibility"})
        tsv_path = _sealed_file(tsv["path"], f"OCR TSV[{selected_ordinal}]")
        expected_path = path.parent / "tsv" / f"frame-{selected_ordinal:04d}-{frame['frame_id']}.tsv"
        if tsv_path != expected_path or _local_file_uri(tsv["storage_uri"], "OCR TSV storage_uri") != tsv_path:
            raise ResultImportError("OCR TSV path or URI escapes its sealed result layout")
        digest = _sha256(tsv["sha256"], "OCR TSV SHA-256")
        byte_count = _integer(tsv["byte_count"], "OCR TSV byte_count", minimum=1, maximum=max_tsv)
        _verify_hash(tsv_path, digest, byte_count, "OCR TSV")
        expected_tsv_id = _producer_id("artifact", run_id, "tesseract_tsv", frame["frame_id"], digest)
        if (
            tsv["artifact_id"] != expected_tsv_id
            or not ARTIFACT_ID_RE.fullmatch(tsv["artifact_id"])
            or tsv["processing_run_id"] != run_id
            or tsv["frame_id"] != frame["frame_id"]
            or tsv["mime_type"] != "text/tab-separated-values; charset=utf-8"
            or tsv["visibility"] != "private"
        ):
            raise ResultImportError("OCR TSV artifact identity or private policy differs")
        expected_tsv_paths.add(tsv_path)
        image = artifact["image"]
        parsed_words = _parse_tsv(
            _stable_read(tsv_path, "OCR TSV", maximum_bytes=max_tsv),
            width=image["width"],
            height=image["height"],
            frame_locator=expected_locator,
            processing_run_id=run_id,
            provenance=provenance,
        )
        declared_words = _array(ocr["words"], f"OCR frames[{selected_ordinal}].words")
        if len(parsed_words) > max_words or not _canonical_equal(declared_words, parsed_words):
            raise ResultImportError("OCR word rows differ from raw TSV replay")
        if (
            ocr["word_count"] != len(parsed_words)
            or ocr["text_presence"] != ("detected" if parsed_words else "not_detected")
        ):
            raise ResultImportError("OCR word count or text-presence state differs")
        expected_command = [
            engine["path"], str(source_png_path), "stdout", "--tessdata-dir", engine["tessdata_dir"],
            "-l", "+".join(parameters["languages"]), "--oem", str(parameters["oem"]),
            "--psm", str(parameters["psm"]), "--dpi", str(parameters["dpi"]),
            "-c", "preserve_interword_spaces=" + ("1" if parameters["preserve_interword_spaces"] else "0"),
            "-c", "tessedit_create_tsv=1",
        ]
        if raw_command != expected_command:
            raise ResultImportError("OCR command differs from its pinned offline recipe")
        normalized_frames.append(
            {
                "locator": expected_locator,
                "source_png": source_png,
                "source_png_path": source_png_path,
                "tsv": dict(tsv),
                "tsv_path": tsv_path,
                "text_presence": ocr["text_presence"],
                "words": parsed_words,
                "sparse_frame": frame,
            }
        )
    if set((path.parent / "tsv").iterdir()) != expected_tsv_paths:
        raise ResultImportError("OCR TSV directory contains unexpected or missing entries")
    if set(path.parent.iterdir()) != {path, path.parent / "tsv"}:
        raise ResultImportError("OCR result directory contains unexpected entries")
    expected_result_key = sha256_bytes(
        canonical_json(
            {
                "work_order_sha256": work_order_sha,
                "sparse_frame_result_sha256": sparse_reference["sha256"],
                "sparse_frame_result_key": sparse["result_key"],
                "sparse_frame_processing_run_id": sparse["processing_run"]["processing_run_id"],
                "source_frames": source_locators,
                "recipe_id": recipe_id,
            }
        ).encode("utf-8")
    )
    if result_key != expected_result_key:
        raise ResultImportError("OCR result key differs from exact input identity")
    result["_path"] = path
    result["_body"] = body
    result["_raw_sha256"] = hashlib.sha256(body).hexdigest()
    result["_sparse"] = sparse
    result["_frames"] = normalized_frames
    result["_engine"] = engine
    result["_parameters"] = parameters
    return result


def _reverify_result_files(result: dict[str, Any]) -> None:
    body = _stable_read(result["_path"], "OCR result", maximum_bytes=MAX_RESULT_BYTES)
    if body != result["_body"] or hashlib.sha256(body).hexdigest() != result["_raw_sha256"]:
        raise ResultImportError("OCR result changed before transaction admission")
    # Replay the complete sparse envelope again to catch changes between planning
    # and the write transaction.
    sparse = _read_sparse_frame_result(result["sparse_frame_result"]["path"])
    if sparse["_sha256"] != result["sparse_frame_result"]["sha256"]:
        raise ResultImportError("OCR sparse-frame result changed before admission")
    runtime_pins = [
        ("Tesseract executable", result["_engine"]),
        *(
            (f"Tesseract {model['language']} model", model)
            for model in result["_engine"]["models"]
        ),
    ]
    for label, observation in runtime_pins:
        path = Path(observation["path"])
        try:
            mode = path.lstat().st_mode
        except OSError as error:
            raise ResultImportError(f"{label} cannot be re-inspected: {error}") from error
        if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
            raise ResultImportError(f"{label} is no longer a regular non-symlink file")
        if _current_stat(path) != observation["stat_after"]:
            raise ResultImportError(f"{label} changed before admission")
        _verify_hash(
            path, observation["sha256"], observation["byte_count"], label
        )
    for frame in result["_frames"]:
        for label, path, digest, byte_count in (
            ("OCR source PNG", frame["source_png_path"], frame["source_png"]["sha256"], frame["source_png"]["byte_count"]),
            ("OCR TSV", frame["tsv_path"], frame["tsv"]["sha256"], frame["tsv"]["byte_count"]),
        ):
            if path.lstat().st_mode & 0o222:
                raise ResultImportError(f"{label} is no longer sealed")
            _verify_hash(path, digest, byte_count, label)


def validate_ocr_tesseract_result_file(result_path: str | Path) -> dict[str, Any]:
    result = _read_ocr_result(result_path)
    return {
        "valid": True,
        "result_raw_sha256": result["_raw_sha256"],
        "result_byte_count": len(result["_body"]),
        "result_key": result["result_key"],
        "processing_run_id": result["processing_run"]["processing_run_id"],
        "sparse_frame_result_sha256": result["sparse_frame_result"]["sha256"],
        "selected_frame_count": len(result["_frames"]),
        "word_count": sum(len(frame["words"]) for frame in result["_frames"]),
        "coordinate_system": "rendition_media_ms",
        "time_coordinate_scope": "proxy_rendition_local_media_time",
        "source_time_mapping": "not_asserted",
        "recording_time_mapping": "not_asserted",
        "visibility": "private",
        "human_review": "required",
        "redaction_state": "pending",
        "score_calibration": "not_calibrated",
        "calibrated_probability": None,
        "publication_authority": "none",
        "identity_authority": "none",
        "event_authority": "none",
        "export_authority": "none",
    }


def _exact_source_anchors(
    connection: sqlite3.Connection, sparse: dict[str, Any]
) -> list[dict[str, str]]:
    renditions = _require_sparse_catalog_lineage(connection, sparse)
    anchors: list[dict[str, str]] = []
    for rendition in renditions:
        proxy = connection.execute(
            "SELECT media_id, metadata_json, review_state FROM renditions WHERE rendition_id = ?",
            (rendition["rendition_id"],),
        ).fetchone()
        if proxy is None:
            raise ResultImportError("OCR proxy rendition disappeared")
        try:
            metadata = _object(json.loads(proxy["metadata_json"]), "OCR proxy rendition metadata")
        except json.JSONDecodeError as error:
            raise ResultImportError("OCR proxy rendition metadata is invalid") from error
        source_rendition_id = metadata.get("derived_from_rendition_id")
        source_rendition = connection.execute(
            """
            SELECT rendition.media_id, rendition.recording_id,
                   rendition.review_state, media.integrity_state,
                   media.media_kind
            FROM renditions AS rendition
            JOIN media_objects AS media ON media.media_id = rendition.media_id
            WHERE rendition.rendition_id = ?
            """,
            (source_rendition_id,),
        ).fetchone()
        if (
            source_rendition is None
            or source_rendition["recording_id"] != rendition["recording_id"]
            or source_rendition["review_state"] == "rejected"
            or source_rendition["integrity_state"] != "verified"
            or source_rendition["media_kind"] != "video"
        ):
            raise ResultImportError(
                "OCR source rendition/media lineage is missing, rejected, or unverified"
            )
        source_rows = connection.execute(
            """
            SELECT DISTINCT media_source.source_id
            FROM media_sources AS media_source
            JOIN sources AS source ON source.source_id = media_source.source_id
            JOIN recording_sources AS recording_source
              ON recording_source.recording_id = ?
             AND recording_source.source_id = media_source.source_id
            JOIN recordings AS recording ON recording.recording_id = ?
            WHERE media_source.media_id = ?
              AND source.review_state <> 'rejected'
              AND recording.review_state <> 'rejected'
              AND recording.merged_into_recording_id IS NULL
              AND recording_source.confidence_state <> 'rejected'
            ORDER BY media_source.source_id
            """,
            (rendition["recording_id"], rendition["recording_id"], source_rendition["media_id"]),
        ).fetchall()
        if len(source_rows) != 1:
            raise ResultImportError("OCR rendition has no unique exact source-media anchor")
        anchors.append(
            {
                "source_id": source_rows[0]["source_id"],
                "recording_id": rendition["recording_id"],
                "source_rendition_id": source_rendition_id,
                "source_media_id": source_rendition["media_id"],
                "rendition_id": rendition["rendition_id"],
                "media_id": proxy["media_id"],
            }
        )
    return anchors


def _insert_or_match(
    connection: sqlite3.Connection,
    *,
    table: str,
    key_column: str,
    row: dict[str, Any],
) -> None:
    existing = connection.execute(
        f"SELECT {', '.join(row)} FROM {table} WHERE {key_column} = ?",
        (row[key_column],),
    ).fetchone()
    if existing is not None:
        if any(existing[key] != value for key, value in row.items()):
            raise ResultImportError(f"{table} ID already has different data")
        return
    columns = ", ".join(row)
    placeholders = ", ".join("?" for _ in row)
    connection.execute(
        f"INSERT INTO {table}({columns}) VALUES({placeholders})", tuple(row.values())
    )


def _insert_processing_run(connection: sqlite3.Connection, result: dict[str, Any]) -> None:
    _insert_or_match(
        connection,
        table="processing_runs",
        key_column="processing_run_id",
        row=_processing_run_row(result),
    )


def _frame_rows(
    connection: sqlite3.Connection,
    result: dict[str, Any],
    anchors: list[dict[str, str]],
    receipt_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    frame_rows: list[dict[str, Any]] = []
    word_rows: list[dict[str, Any]] = []
    sparse = result["_sparse"]
    proxy_duration = sparse["input_proxy"]["probe"]["duration_ms"]
    run_id = result["processing_run"]["processing_run_id"]
    for selected_ordinal, frame in enumerate(result["_frames"]):
        sparse_frame = frame["sparse_frame"]
        start_ms, end_ms = _sparse_observation_interval(sparse_frame, proxy_duration)
        for anchor in anchors:
            sparse_observation_id = stable_id(
                "obs", sparse["processing_run"]["processing_run_id"], anchor["rendition_id"],
                sparse_frame["frame_id"], "sparse_frame_routing_candidate",
            )
            sparse_observation = connection.execute(
                "SELECT * FROM observations WHERE observation_id = ?", (sparse_observation_id,)
            ).fetchone()
            expected_sparse_metadata = _sparse_observation_metadata(sparse_frame)
            if (
                sparse_observation is None
                or sparse_observation["observation_kind"] != "sparse_frame_routing_candidate"
                or sparse_observation["recording_id"] != anchor["recording_id"]
                or sparse_observation["rendition_id"] != anchor["rendition_id"]
                or sparse_observation["processing_run_id"] != sparse["processing_run"]["processing_run_id"]
                or sparse_observation["start_ms"] != start_ms
                or sparse_observation["end_ms"] != end_ms
                or sparse_observation["visibility"] != "private"
                or sparse_observation["review_state"] != "machine"
                or sparse_observation["metadata_json"] != expected_sparse_metadata
            ):
                raise ResultImportError("OCR input frame lacks its exact admitted sparse observation")
            frame_admission_id = stable_id(
                "ocrframe", receipt_id, anchor["rendition_id"], sparse_frame["frame_id"]
            )
            frame_row = {
                "private_ocr_frame_admission_id": frame_admission_id,
                "private_ocr_import_receipt_id": receipt_id,
                "frame_ordinal": selected_ordinal,
                "frame_id": sparse_frame["frame_id"],
                "sparse_frame_observation_id": sparse_observation_id,
                **anchor,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "coordinate_system": "rendition_media_ms",
                "boundary": "half_open",
                "requested_timestamp_ms": sparse_frame["requested_timestamp_ms"],
                "frame_artifact_id": frame["locator"]["frame_artifact_id"],
                "frame_png_sha256": frame["locator"]["frame_png_sha256"],
                "tsv_artifact_id": frame["tsv"]["artifact_id"],
                "tsv_sha256": frame["tsv"]["sha256"],
                "text_presence": frame["text_presence"],
                "word_count": len(frame["words"]),
                "visibility": "private",
                "human_review": "required",
                "redaction_state": "pending",
                "publication_authority": "none",
                "identity_authority": "none",
                "event_authority": "none",
            }
            frame_rows.append(frame_row)
            for word in frame["words"]:
                observation_id = stable_id(
                    "obs", run_id, anchor["rendition_id"], word["region_id"],
                    "ocr_tesseract_word_candidate",
                )
                word_rows.append(
                    {
                        "private_ocr_word_admission_id": stable_id(
                            "ocrword", run_id, anchor["rendition_id"], word["region_id"]
                        ),
                        "private_ocr_frame_admission_id": frame_admission_id,
                        "observation_id": observation_id,
                        "region_id": word["region_id"],
                        "word_ordinal": word["reading_order"]["ordinal"],
                        "raw_text": word["raw_text"],
                        "raw_score": float(word["raw_score"]),
                        "raw_score_text": word["raw_score_text"],
                        "score_name": "tesseract_raw_0_100_not_probability",
                        "score_calibration": "not_calibrated",
                        "probability_interpretation": "not_a_probability",
                        "calibrated_probability": None,
                        "calibration_set_id": None,
                        "rectangle_json": canonical_json(word["rectangle"]),
                        "reading_order_json": canonical_json(word["reading_order"]),
                        "language_choice_json": canonical_json(word["language_choice"]),
                        "script_choice_json": canonical_json(word["script_choice"]),
                        "engine_provenance_json": canonical_json(word["engine_provenance"]),
                        "visibility": "private",
                        "review_state": "machine",
                        "human_review": "required",
                        "redaction_state": "pending",
                        "publication_authority": "none",
                        "identity_authority": "none",
                        "event_authority": "none",
                        "_recording_id": anchor["recording_id"],
                        "_rendition_id": anchor["rendition_id"],
                        "_processing_run_id": run_id,
                        "_start_ms": start_ms,
                        "_end_ms": end_ms,
                        "_frame_id": sparse_frame["frame_id"],
                        "_source_id": anchor["source_id"],
                        "_language": "+".join(result["_parameters"]["languages"]),
                    }
                )
    return frame_rows, word_rows


def _processing_run_row(result: dict[str, Any]) -> dict[str, Any]:
    run = result["processing_run"]
    return {
        "processing_run_id": run["processing_run_id"],
        "stage": STAGE,
        "implementation_version": run["implementation_version"],
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


def _run_input_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    run_id = result["processing_run"]["processing_run_id"]
    rows = [
        {
            "run_input_id": stable_id(
                "rin",
                run_id,
                "processing_run",
                result["source_lineage"]["sparse_frame_processing_run_id"],
                "sparse_frame_result",
            ),
            "processing_run_id": run_id,
            "object_type": "processing_run",
            "object_id": result["source_lineage"]["sparse_frame_processing_run_id"],
            "input_role": "sparse_frame_result",
            "input_sha256": result["sparse_frame_result"]["sha256"],
        }
    ]
    for frame in result["_frames"]:
        object_id = frame["locator"]["frame_artifact_id"]
        role = f"source_frame_png:{frame['locator']['frame_id']}"
        rows.append(
            {
                "run_input_id": stable_id("rin", run_id, "artifact", object_id, role),
                "processing_run_id": run_id,
                "object_type": "artifact",
                "object_id": object_id,
                "input_role": role,
                "input_sha256": frame["locator"]["frame_png_sha256"],
            }
        )
    return rows


def _tsv_artifact_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    run_id = result["processing_run"]["processing_run_id"]
    return [
        {
            "artifact_id": frame["tsv"]["artifact_id"],
            "processing_run_id": run_id,
            "artifact_kind": "tesseract_tsv",
            "storage_uri": frame["tsv"]["storage_uri"],
            "sha256": frame["tsv"]["sha256"],
            "byte_count": frame["tsv"]["byte_count"],
            "schema_version": 1,
            "visibility": "private",
            "metadata_json": canonical_json(
                {
                    "calibration_state": "not_calibrated",
                    "frame_id": frame["locator"]["frame_id"],
                    "human_review": "required",
                    "mime_type": frame["tsv"]["mime_type"],
                    "publication_authority": "none",
                    "redaction_state": "pending",
                }
            ),
        }
        for frame in result["_frames"]
    ]


def _word_bundle_rows(
    result: dict[str, Any], raw_word_row: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    word_row = {
        key: value for key, value in raw_word_row.items() if not key.startswith("_")
    }
    observation_row = {
        "observation_id": word_row["observation_id"],
        "observation_kind": "ocr_tesseract_word_candidate",
        "recording_id": raw_word_row["_recording_id"],
        "rendition_id": raw_word_row["_rendition_id"],
        "processing_run_id": raw_word_row["_processing_run_id"],
        "start_ms": raw_word_row["_start_ms"],
        "end_ms": raw_word_row["_end_ms"],
        "visibility": "private",
        "review_state": "machine",
        "payload_schema_version": 1,
        "metadata_json": canonical_json(
            {
                "coordinate_system": "rendition_media_ms",
                "frame_id": raw_word_row["_frame_id"],
                "human_review": "required",
                "identity_authority": "none",
                "event_authority": "none",
                "publication_authority": "none",
                "redaction_state": "pending",
                "region_id": word_row["region_id"],
                "source_id": raw_word_row["_source_id"],
            }
        ),
        "created_at": result["processing_run"]["completed_at"],
    }
    rectangle = json.loads(word_row["rectangle_json"])
    detail_row = {
        "observation_id": word_row["observation_id"],
        "polygon_json": canonical_json(
            [
                [rectangle["left"], rectangle["top"]],
                [rectangle["right"], rectangle["top"]],
                [rectangle["right"], rectangle["bottom"]],
                [rectangle["left"], rectangle["bottom"]],
            ]
        ),
        "raw_text": word_row["raw_text"],
        "normalized_text": None,
        "language": raw_word_row["_language"],
        "redaction_state": "pending",
    }
    score_row = {
        "observation_score_id": stable_id(
            "score", word_row["observation_id"], word_row["score_name"]
        ),
        "observation_id": word_row["observation_id"],
        "score_name": word_row["score_name"],
        "raw_score": word_row["raw_score"],
        "calibrated_probability": None,
        "calibration_set_id": None,
        "quality_flags_json": canonical_json(
            [
                "human_review_required",
                "not_a_probability",
                "not_calibrated",
                "redaction_pending",
            ]
        ),
    }
    return word_row, observation_row, detail_row, score_row


def _statistics_row(
    result: dict[str, Any], frame_rows: list[dict[str, Any]], word_rows: list[dict[str, Any]]
) -> dict[str, int]:
    return {
        "admitted_frames": len(frame_rows),
        "calibrated_probabilities": 0,
        "event_rows": 0,
        "identity_rows": 0,
        "observations": len(word_rows),
        "publication_decisions": 0,
        "selected_frames": len(result["_frames"]),
        "tsv_artifacts": len(result["_frames"]),
        "words": len(word_rows),
    }


def _import_batch_row(
    result: dict[str, Any],
    frame_rows: list[dict[str, Any]],
    word_rows: list[dict[str, Any]],
    imported_at: str,
) -> dict[str, Any]:
    return {
        "import_batch_id": stable_id("imp", IMPORTER_NAME, result["_raw_sha256"]),
        "importer_name": IMPORTER_NAME,
        "importer_version": IMPLEMENTATION_VERSION,
        "input_sha256": result["_raw_sha256"],
        "source_snapshot_date": None,
        "started_at": imported_at,
        "completed_at": imported_at,
        "status": "completed",
        "statistics_json": canonical_json(
            _statistics_row(result, frame_rows, word_rows)
        ),
    }


def _receipt_row(
    result: dict[str, Any],
    frame_rows: list[dict[str, Any]],
    word_rows: list[dict[str, Any]],
    imported_at: str,
) -> dict[str, Any]:
    return {
        "private_ocr_import_receipt_id": stable_id(
            "ocrimp", IMPORTER_NAME, result["_raw_sha256"]
        ),
        "import_batch_id": stable_id("imp", IMPORTER_NAME, result["_raw_sha256"]),
        "result_uri": result["_path"].as_uri(),
        "result_raw_sha256": result["_raw_sha256"],
        "result_byte_count": len(result["_body"]),
        "result_key": result["result_key"],
        "processing_run_id": result["processing_run"]["processing_run_id"],
        "sparse_frame_result_uri": Path(result["sparse_frame_result"]["path"]).as_uri(),
        "sparse_frame_result_sha256": result["sparse_frame_result"]["sha256"],
        "sparse_frame_result_key": result["sparse_frame_result"]["result_key"],
        "sparse_frame_processing_run_id": result["sparse_frame_result"]["processing_run_id"],
        "proxy_media_id": result["source_lineage"]["source_media_id"],
        "imported_at": imported_at,
        "selected_frame_count": len(result["_frames"]),
        "admitted_frame_count": len(frame_rows),
        "word_count": len(word_rows),
        "observation_count": len(word_rows),
        "tsv_artifact_count": len(result["_frames"]),
        "coordinate_system": "rendition_media_ms",
        "boundary": "half_open",
        "visibility": "private",
        "human_review": "required",
        "redaction_state": "pending",
        "score_calibration": "not_calibrated",
        "calibrated_probabilities_present": 0,
        "publication_authority": "none",
        "identity_authority": "none",
        "event_authority": "none",
        "export_authority": "none",
    }


def _assert_exact_row(
    connection: sqlite3.Connection,
    *,
    table: str,
    key_column: str,
    expected: dict[str, Any],
    label: str,
) -> None:
    actual = connection.execute(
        f"SELECT {', '.join(expected)} FROM {table} WHERE {key_column} = ?",
        (expected[key_column],),
    ).fetchone()
    if actual is None or any(actual[key] != value for key, value in expected.items()):
        raise ResultImportError(f"OCR exact replay {label} differs from sealed input")


def _assert_exact_row_set(
    connection: sqlite3.Connection,
    *,
    table: str,
    key_column: str,
    expected_rows: list[dict[str, Any]],
    scope_sql: str,
    scope_parameters: tuple[Any, ...],
    label: str,
) -> None:
    expected_keys = {row[key_column] for row in expected_rows}
    actual_keys = {
        row[0]
        for row in connection.execute(
            f"SELECT {key_column} FROM {table} WHERE {scope_sql}", scope_parameters
        ).fetchall()
    }
    if actual_keys != expected_keys:
        raise ResultImportError(f"OCR exact replay {label} row set differs from sealed input")
    for row in expected_rows:
        _assert_exact_row(
            connection,
            table=table,
            key_column=key_column,
            expected=row,
            label=label,
        )


def _assert_no_private_ocr_authority(connection: sqlite3.Connection) -> None:
    leaks = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM publication_decisions AS decision
           WHERE decision.object_type IN (
             'private_ocr_import_receipt', 'private_ocr_frame_admission',
             'private_ocr_word_admission', 'ocr_tesseract_word_candidate'
           )
              OR (decision.object_type = 'observation' AND EXISTS (
                  SELECT 1 FROM observations AS observation
                  WHERE observation.observation_id = decision.object_id
                    AND observation.observation_kind = 'ocr_tesseract_word_candidate'
              ))
              OR (decision.object_type = 'artifact' AND EXISTS (
                  SELECT 1 FROM artifacts AS artifact
                  WHERE artifact.artifact_id = decision.object_id
                    AND artifact.artifact_kind = 'tesseract_tsv'
              )))
          +
          (SELECT count(*) FROM publication_gate_decisions AS decision
           WHERE decision.object_type IN (
             'private_ocr_import_receipt', 'private_ocr_frame_admission',
             'private_ocr_word_admission', 'ocr_tesseract_word_candidate'
           )
              OR (decision.object_type = 'observation' AND EXISTS (
                  SELECT 1 FROM observations AS observation
                  WHERE observation.observation_id = decision.object_id
                    AND observation.observation_kind = 'ocr_tesseract_word_candidate'
              ))
              OR (decision.object_type = 'artifact' AND EXISTS (
                  SELECT 1 FROM artifacts AS artifact
                  WHERE artifact.artifact_id = decision.object_id
                    AND artifact.artifact_kind = 'tesseract_tsv'
              )))
          +
          (SELECT count(*) FROM appearances
           WHERE observation_id IN (
             SELECT observation_id FROM observations
             WHERE observation_kind = 'ocr_tesseract_word_candidate'
           ))
          +
          (SELECT count(*) FROM identity_cluster_memberships
           WHERE observation_id IN (
             SELECT observation_id FROM observations
             WHERE observation_kind = 'ocr_tesseract_word_candidate'
           ))
          +
          (SELECT count(*) FROM event_evidence
           WHERE observation_id IN (
             SELECT observation_id FROM observations
             WHERE observation_kind = 'ocr_tesseract_word_candidate'
           ))
          +
          (SELECT count(*) FROM claim_catalog_links
           WHERE observation_id IN (
             SELECT observation_id FROM observations
             WHERE observation_kind = 'ocr_tesseract_word_candidate'
           ))
        """
    ).fetchone()[0]
    if leaks:
        raise ResultImportError("private OCR carries forbidden authority state")


def _assert_exact_replay(
    connection: sqlite3.Connection,
    receipt: sqlite3.Row,
    *,
    result: dict[str, Any],
    frame_rows: list[dict[str, Any]],
    word_rows: list[dict[str, Any]],
    check_fts: bool = True,
) -> None:
    imported_at = _timestamp(receipt["imported_at"], "OCR receipt imported_at")
    if imported_at != receipt["imported_at"]:
        raise ResultImportError("OCR exact replay receipt time is not canonical UTC")
    expected_receipt = _receipt_row(result, frame_rows, word_rows, imported_at)
    _assert_exact_row(
        connection,
        table="private_ocr_import_receipts",
        key_column="private_ocr_import_receipt_id",
        expected=expected_receipt,
        label="receipt",
    )
    _assert_exact_row(
        connection,
        table="import_batches",
        key_column="import_batch_id",
        expected=_import_batch_row(result, frame_rows, word_rows, imported_at),
        label="import batch",
    )
    run_id = result["processing_run"]["processing_run_id"]
    _assert_exact_row(
        connection,
        table="processing_runs",
        key_column="processing_run_id",
        expected=_processing_run_row(result),
        label="processing run",
    )
    _assert_exact_row_set(
        connection,
        table="run_inputs",
        key_column="run_input_id",
        expected_rows=_run_input_rows(result),
        scope_sql="processing_run_id = ?",
        scope_parameters=(run_id,),
        label="processing input",
    )
    _assert_exact_row_set(
        connection,
        table="artifacts",
        key_column="artifact_id",
        expected_rows=_tsv_artifact_rows(result),
        scope_sql="processing_run_id = ?",
        scope_parameters=(run_id,),
        label="TSV artifact",
    )
    _assert_exact_row_set(
        connection,
        table="private_ocr_frame_admissions",
        key_column="private_ocr_frame_admission_id",
        expected_rows=frame_rows,
        scope_sql="private_ocr_import_receipt_id = ?",
        scope_parameters=(receipt["private_ocr_import_receipt_id"],),
        label="frame admission",
    )
    bundles = [_word_bundle_rows(result, row) for row in word_rows]
    expected_words = [bundle[0] for bundle in bundles]
    expected_observations = [bundle[1] for bundle in bundles]
    expected_details = [bundle[2] for bundle in bundles]
    expected_scores = [bundle[3] for bundle in bundles]
    word_scope = (
        "private_ocr_frame_admission_id IN "
        "(SELECT private_ocr_frame_admission_id FROM private_ocr_frame_admissions "
        "WHERE private_ocr_import_receipt_id = ?)"
    )
    _assert_exact_row_set(
        connection,
        table="private_ocr_word_admissions",
        key_column="private_ocr_word_admission_id",
        expected_rows=expected_words,
        scope_sql=word_scope,
        scope_parameters=(receipt["private_ocr_import_receipt_id"],),
        label="word admission",
    )
    observation_scope = "processing_run_id = ?"
    _assert_exact_row_set(
        connection,
        table="observations",
        key_column="observation_id",
        expected_rows=expected_observations,
        scope_sql=observation_scope,
        scope_parameters=(run_id,),
        label="OCR observation",
    )
    detail_scope = (
        "observation_id IN (SELECT observation_id FROM observations "
        "WHERE processing_run_id = ?)"
    )
    _assert_exact_row_set(
        connection,
        table="ocr_observations",
        key_column="observation_id",
        expected_rows=expected_details,
        scope_sql=detail_scope,
        scope_parameters=(run_id,),
        label="OCR detail",
    )
    _assert_exact_row_set(
        connection,
        table="observation_scores",
        key_column="observation_score_id",
        expected_rows=expected_scores,
        scope_sql=detail_scope,
        scope_parameters=(run_id,),
        label="OCR score",
    )
    _assert_no_private_ocr_authority(connection)
    if check_fts:
        _assert_private_ocr_fts_exact(connection)


def _assert_private_ocr_fts_exact(connection: sqlite3.Connection) -> None:
    """Fail closed unless FTS is an exact projection of the immutable word ledger.

    FTS5 virtual tables cannot carry ordinary SQLite triggers of their own. Direct
    FTS DML is therefore treated as untrusted cache tampering: it has no authority,
    and replay/search refuse to proceed until the index again exactly matches the
    append-only admission rows.
    """

    try:
        require_sqlite_version(
            connection,
            FTS_INTEGRITY_MINIMUM_SQLITE,
            feature="private OCR FTS integrity validation",
        )
        integrity = [
            row[0]
            for row in connection.execute(
                "PRAGMA integrity_check('private_ocr_word_fts')"
            ).fetchall()
        ]
    except (RuntimeError, sqlite3.DatabaseError) as error:
        raise ResultImportError(
            f"private OCR FTS integrity check could not run: {error}"
        ) from error
    if integrity != ["ok"]:
        raise ResultImportError(
            "private OCR FTS integrity check failed: " + "; ".join(integrity)
        )

    drift = connection.execute(
        """
        SELECT
            abs((SELECT count(*) FROM private_ocr_word_admissions)
              - (SELECT count(*) FROM private_ocr_word_fts))
          + (SELECT count(*)
             FROM private_ocr_word_admissions AS word
             LEFT JOIN private_ocr_word_fts AS search
               ON search.private_ocr_word_admission_id =
                  word.private_ocr_word_admission_id
              AND search.raw_text = word.raw_text
             WHERE search.private_ocr_word_admission_id IS NULL)
          + (SELECT count(*)
             FROM private_ocr_word_fts AS search
             LEFT JOIN private_ocr_word_admissions AS word
               ON word.private_ocr_word_admission_id =
                  search.private_ocr_word_admission_id
              AND word.raw_text = search.raw_text
             WHERE word.private_ocr_word_admission_id IS NULL)
        """
    ).fetchone()[0]
    if drift:
        raise ResultImportError(
            "private OCR FTS differs from its append-only word admissions"
        )


def _assert_all_private_ocr_receipts_exact(
    connection: sqlite3.Connection,
) -> None:
    """Replay every sealed result before allowing raw OCR text to be searched."""

    for receipt in connection.execute(
        "SELECT * FROM private_ocr_import_receipts ORDER BY receipt_sequence"
    ):
        result_path = _local_file_uri(receipt["result_uri"], "private OCR result URI")
        result = _read_ocr_result(result_path)
        _reverify_result_files(result)
        expected_receipt_id = stable_id(
            "ocrimp", IMPORTER_NAME, result["_raw_sha256"]
        )
        anchors = _exact_source_anchors(connection, result["_sparse"])
        frame_rows, word_rows = _frame_rows(
            connection,
            result,
            anchors,
            expected_receipt_id,
        )
        _assert_exact_replay(
            connection,
            receipt,
            result=result,
            frame_rows=frame_rows,
            word_rows=word_rows,
            check_fts=False,
        )


def import_ocr_tesseract_result(
    connection: sqlite3.Connection,
    result_path: str | Path,
    *,
    expected_result_sha256: str,
) -> dict[str, Any]:
    """Transactionally admit one exact completed result into the private OCR lane."""

    expected_digest = _sha256(expected_result_sha256, "expected OCR result SHA-256")
    result = _read_ocr_result(result_path)
    if result["_raw_sha256"] != expected_digest:
        raise ResultImportError("OCR result differs from the separately reviewed SHA-256")
    receipt_id = stable_id("ocrimp", IMPORTER_NAME, result["_raw_sha256"])
    with transaction(connection):
        _reverify_result_files(result)
        anchors = _exact_source_anchors(connection, result["_sparse"])
        frame_rows, word_rows = _frame_rows(connection, result, anchors, receipt_id)
        existing = connection.execute(
            "SELECT * FROM private_ocr_import_receipts WHERE result_raw_sha256 = ?",
            (result["_raw_sha256"],),
        ).fetchone()
        if existing is not None:
            _assert_exact_replay(
                connection, existing, result=result, frame_rows=frame_rows, word_rows=word_rows
            )
            return {
                "status": "exact_replay",
                "private_ocr_import_receipt_id": existing["private_ocr_import_receipt_id"],
                "import_batch_id": existing["import_batch_id"],
                "result_raw_sha256": result["_raw_sha256"],
                "result_key": result["result_key"],
                "processing_run_id": result["processing_run"]["processing_run_id"],
                "selected_frame_count": len(result["_frames"]),
                "admitted_frame_count": len(frame_rows),
                "word_count": len(word_rows),
                "fts_rows_added": 0,
                "publication_decisions_added": 0,
                "identity_rows_added": 0,
                "event_rows_added": 0,
            }
        collision = connection.execute(
            """
            SELECT 1 FROM private_ocr_import_receipts
            WHERE result_key = ? OR processing_run_id = ?
               OR private_ocr_import_receipt_id = ?
            """,
            (result["result_key"], result["processing_run"]["processing_run_id"], receipt_id),
        ).fetchone()
        if collision is not None:
            raise ResultImportError("OCR result identity already has a different receipt digest")

        imported_at = utc_now()
        batch_row = _import_batch_row(result, frame_rows, word_rows, imported_at)
        batch_id = batch_row["import_batch_id"]
        _insert_or_match(
            connection,
            table="import_batches",
            key_column="import_batch_id",
            row=batch_row,
        )
        _insert_processing_run(connection, result)
        run_id = result["processing_run"]["processing_run_id"]
        for run_input_row in _run_input_rows(result):
            _insert_or_match(
                connection,
                table="run_inputs",
                key_column="run_input_id",
                row=run_input_row,
            )
        for artifact_row in _tsv_artifact_rows(result):
            _insert_or_match(
                connection,
                table="artifacts",
                key_column="artifact_id",
                row=artifact_row,
            )

        receipt_row = _receipt_row(result, frame_rows, word_rows, imported_at)
        _insert_or_match(
            connection,
            table="private_ocr_import_receipts",
            key_column="private_ocr_import_receipt_id",
            row=receipt_row,
        )
        for frame_row in frame_rows:
            _insert_or_match(
                connection,
                table="private_ocr_frame_admissions",
                key_column="private_ocr_frame_admission_id",
                row=frame_row,
            )
        for raw_word_row in word_rows:
            word_row, observation_row, detail_row, score_row = _word_bundle_rows(
                result, raw_word_row
            )
            _insert_or_match(
                connection,
                table="observations",
                key_column="observation_id",
                row=observation_row,
            )
            _insert_or_match(
                connection,
                table="ocr_observations",
                key_column="observation_id",
                row=detail_row,
            )
            _insert_or_match(
                connection,
                table="observation_scores",
                key_column="observation_score_id",
                row=score_row,
            )
            _insert_or_match(
                connection,
                table="private_ocr_word_admissions",
                key_column="private_ocr_word_admission_id",
                row=word_row,
            )
        _assert_exact_replay(
            connection,
            connection.execute(
                "SELECT * FROM private_ocr_import_receipts WHERE private_ocr_import_receipt_id = ?",
                (receipt_id,),
            ).fetchone(),
            result=result,
            frame_rows=frame_rows,
            word_rows=word_rows,
        )
    return {
        "status": "admitted",
        "importer_version": IMPLEMENTATION_VERSION,
        "private_ocr_import_receipt_id": receipt_id,
        "import_batch_id": batch_id,
        "result_raw_sha256": result["_raw_sha256"],
        "result_key": result["result_key"],
        "processing_run_id": result["processing_run"]["processing_run_id"],
        "selected_frame_count": len(result["_frames"]),
        "admitted_frame_count": len(frame_rows),
        "word_count": len(word_rows),
        "fts_rows_added": len(word_rows),
        "calibrated_probabilities_added": 0,
        "publication_decisions_added": 0,
        "identity_rows_added": 0,
        "event_rows_added": 0,
    }


def search_private_ocr(
    connection: sqlite3.Connection,
    query: str,
    *,
    limit: int = 25,
    source_id: str | None = None,
    recording_id: str | None = None,
    rendition_id: str | None = None,
) -> dict[str, Any]:
    """Search private, raw, redaction-pending machine OCR text."""

    if not isinstance(query, str) or not query.strip() or "\x00" in query or len(query) > 1_000:
        raise ResultImportError("private OCR search query must be a bounded string")
    if type(limit) is not int or not 1 <= limit <= 200:
        raise ResultImportError("private OCR search limit must be between 1 and 200")
    _assert_private_ocr_fts_exact(connection)
    _assert_all_private_ocr_receipts_exact(connection)
    filters: list[str] = []
    parameters: list[Any] = [query]
    for column, value in (
        ("frame.source_id", source_id),
        ("frame.recording_id", recording_id),
        ("frame.rendition_id", rendition_id),
    ):
        if value is not None:
            _identifier(value, f"private OCR {column} filter")
            filters.append(f"AND {column} = ?")
            parameters.append(value)
    parameters.append(limit)
    try:
        rows = connection.execute(
            f"""
            SELECT word.private_ocr_word_admission_id,
                   word.private_ocr_frame_admission_id,
                   word.observation_id, word.region_id, word.word_ordinal,
                   word.raw_text, word.raw_score, word.raw_score_text,
                   frame.source_id, frame.recording_id,
                   frame.source_rendition_id, frame.source_media_id,
                   frame.rendition_id, frame.media_id,
                   frame.start_ms, frame.end_ms, frame.frame_id,
                   frame.requested_timestamp_ms,
                   bm25(private_ocr_word_fts) AS rank
            FROM private_ocr_word_fts
            JOIN private_ocr_word_admissions AS word
              ON word.private_ocr_word_admission_id =
                 private_ocr_word_fts.private_ocr_word_admission_id
            JOIN private_ocr_frame_admissions AS frame
              ON frame.private_ocr_frame_admission_id =
                 word.private_ocr_frame_admission_id
            WHERE private_ocr_word_fts MATCH ?
              AND word.visibility = 'private'
              AND word.review_state = 'machine'
              AND word.human_review = 'required'
              AND word.redaction_state = 'pending'
              AND word.score_calibration = 'not_calibrated'
              AND word.calibrated_probability IS NULL
              {' '.join(filters)}
            ORDER BY rank, frame.rendition_id, frame.start_ms, word.word_ordinal
            LIMIT ?
            """,
            tuple(parameters),
        ).fetchall()
    except sqlite3.OperationalError as error:
        raise ResultImportError(f"private OCR full-text query is invalid: {error}") from error
    return {
        "query": query,
        "limit": limit,
        "source_id": source_id,
        "recording_id": recording_id,
        "rendition_id": rendition_id,
        "result_count": len(rows),
        "coordinate_system": "rendition_media_ms",
        "time_coordinate_scope": "proxy_rendition_local_media_time",
        "source_time_mapping": "not_asserted",
        "recording_time_mapping": "not_asserted",
        "boundary": "half_open",
        "visibility": "private",
        "human_review": "required",
        "redaction_state": "pending",
        "score_calibration": "not_calibrated",
        "score_interpretation": "raw_tesseract_0_100_not_a_probability",
        "publication_authority": "none",
        "identity_authority": "none",
        "event_authority": "none",
        "export_authority": "none",
        "results": [
            {
                **dict(row),
                "proxy_rendition_start_ms": row["start_ms"],
                "proxy_rendition_end_ms": row["end_ms"],
                "calibrated_probability": None,
                "source_start_ms": None,
                "source_end_ms": None,
                "recording_start_ms": None,
                "recording_end_ms": None,
            }
            for row in rows
        ],
    }
