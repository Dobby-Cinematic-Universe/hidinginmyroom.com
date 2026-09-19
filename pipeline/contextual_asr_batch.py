#!/usr/bin/env python3
"""Deterministic paired contextual-ASR batches over sealed raw baselines.

This additive control lane consumes explicit completed ``asr_whispercpp`` raw
result envelopes and one exact neutral glossary.  It emits immutable contextual
work orders that preserve each baseline's input, engine, model, window, decoding
parameters, and catalog context.  Only the job/output control identity and the
glossary-assisted recipe are new.

The module has no database, downloader, publication, transcript-selection, or
correction operation.  Manifests and run summaries deliberately contain no
transcript wording or glossary term strings.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import re
import shutil
import stat
import subprocess
import sys
import urllib.parse
import uuid
from pathlib import Path
from typing import Any

import asr_whispercpp
import whispercpp_engine_profiles


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
MATERIALIZER_NAME = "himr-contextual-asr-batch"
PASS_KIND = "contextual_full_input_paired_pilot"

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PROGRAM_SOURCE = Path(__file__).resolve()
ADAPTER_SOURCE = (REPOSITORY_ROOT / "pipeline" / "asr_whispercpp.py").resolve()
ENGINE_PROFILES_SOURCE = (
    REPOSITORY_ROOT / "pipeline" / "whispercpp_engine_profiles.py"
).resolve()
CORPUS_SOURCE_ROOT = (REPOSITORY_ROOT / "corpus" / "src").resolve()
CATALOG_VALIDATOR_SOURCE = (
    CORPUS_SOURCE_ROOT / "himr_corpus" / "asr_result_importer.py"
).resolve()

MAX_BASELINES = 64
MAX_JSON_DEPTH = 128
MAX_RESULT_BYTES = 512 * 1024 * 1024
MAX_ARTIFACT_BYTES = 512 * 1024 * 1024
MAX_MANIFEST_BYTES = 32 * 1024 * 1024
MAX_WORK_ORDER_BYTES = 4 * 1024 * 1024
MAX_GLOSSARY_BYTES = 1024 * 1024
MAX_SOURCE_BYTES = 16 * 1024 * 1024
MAX_TOTAL_INPUT_BYTES = 256 * 1024 * 1024 * 1024
MAX_TOTAL_INPUT_MS = 7 * 24 * 60 * 60 * 1000

RESULT_FILENAMES = (
    "result.json",
    "transcript.normalized.json",
    "whisper.raw.json",
)
ARTIFACT_FILENAME_BY_KIND = {
    "transcript_normalized_json": "transcript.normalized.json",
    "whispercpp_output_json_full": "whisper.raw.json",
}

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
BATCH_ID_RE = re.compile(r"^ctxasrbatch_[0-9a-f]{32}$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")

PROFILE = {
    "pass_kind": PASS_KIND,
    "selection_basis": "explicit_full_input_pilot_no_uncertainty_or_accuracy_claim",
    "baseline_policy": "sealed_completed_current_raw_no_glossary",
    "pairing_policy": (
        "same_input_engine_model_window_inference_and_catalog_context;"
        "new_job_output_and_exact_neutral_glossary_only"
    ),
    "text_selection": "none_machine_hypotheses_remain_separate",
    "confidence_calibration": "none",
}

SAFETY = {
    "catalog_writes": False,
    "database_access": "none",
    "identity_authority": "none",
    "manifest_contains_glossary_terms": False,
    "manifest_contains_transcript_text": False,
    "network_access": "none_by_controller_external_engine_sandbox_required",
    "publication_authority": "none",
    "review_authority": "none",
    "transcript_preference_authority": "none",
    "visibility": "private",
}

RUN_SAFETY = {
    "catalog_writes": False,
    "database_access": "none",
    "dispatch_order": "sealed_ordinal_fail_fast",
    "network_access": "none_by_controller_external_engine_sandbox_required",
    "publication_authority": "none",
    "resume_policy": "replay_manifest_reuses_content_addressed_completed_results",
    "review_authority": "none",
    "transcript_preference_authority": "none",
}

MANIFEST_KEYS = {
    "schema_version",
    "implementation_version",
    "materializer",
    "batch_id",
    "identity_sha256",
    "batch_relative_path",
    "software",
    "glossary",
    "engine",
    "model",
    "inference",
    "profile",
    "output",
    "work_order_count",
    "totals",
    "work_orders",
    "safety",
}


class ContextualBatchError(RuntimeError):
    """A fail-closed contextual batch contract or integrity failure."""


class ContextualBatchRunFailure(ContextualBatchError):
    """A serial dispatch failure with a schema-valid partial summary."""

    def __init__(self, result: dict[str, Any]):
        failed = result["failed_job"]
        super().__init__(
            f"contextual ASR job {failed['ordinal']}/{result['job_count']} "
            f"({failed['job_id']}) failed: {failed['error']['message']}"
        )
        self.result = result


def canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ContextualBatchError(
            f"value cannot be represented as strict canonical JSON: {error}"
        ) from error


def pretty_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ContextualBatchError(
            f"value cannot be represented as strict JSON: {error}"
        ) from error


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _reject_constant(value: str) -> None:
    raise ContextualBatchError(f"non-finite JSON constant is forbidden: {value}")


def _reject_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ContextualBatchError(f"JSON object contains duplicate key: {key}")
        result[key] = value
    return result


def _check_json_depth(value: Any, depth: int = 0) -> None:
    if depth > MAX_JSON_DEPTH:
        raise ContextualBatchError(
            f"JSON nesting exceeds the {MAX_JSON_DEPTH}-level limit"
        )
    if isinstance(value, dict):
        for child in value.values():
            _check_json_depth(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            _check_json_depth(child, depth + 1)
    elif isinstance(value, float) and not math.isfinite(value):
        raise ContextualBatchError("non-finite JSON number is forbidden")


def parse_json(body: bytes, label: str) -> Any:
    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=_reject_pairs,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContextualBatchError(f"{label} is not strict UTF-8 JSON: {error}") from error
    _check_json_depth(value)
    return value


def exact_object(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ContextualBatchError(f"{label} must be a JSON object")
    missing = keys - set(value)
    extra = set(value) - keys
    if missing or extra:
        raise ContextualBatchError(
            f"{label} keys differ; missing={sorted(missing)}, extra={sorted(extra)}"
        )
    return value


def identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER_RE.fullmatch(value):
        raise ContextualBatchError(f"{label} is not a bounded identifier")
    return value


def sha256_value(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise ContextualBatchError(f"{label} is not a lowercase SHA-256")
    return value


def integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContextualBatchError(f"{label} must be an integer")
    if value < minimum or value > maximum:
        raise ContextualBatchError(
            f"{label} must be between {minimum} and {maximum}"
        )
    return value


def _path_text(value: Any, label: str) -> Path:
    if not isinstance(value, (str, Path)):
        raise ContextualBatchError(f"{label} must be an absolute local path")
    text = str(value)
    if not text or "\x00" in text or "://" in text or len(text) > 4096:
        raise ContextualBatchError(f"{label} must be a bounded absolute local path")
    path = Path(text)
    if not path.is_absolute():
        raise ContextualBatchError(f"{label} must be absolute")
    resolved = path.resolve(strict=False)
    if resolved != path:
        raise ContextualBatchError(f"{label} must already be resolved")
    return path


def existing_regular_file(value: Any, label: str, *, executable: bool = False) -> Path:
    path = _path_text(value, label)
    try:
        link = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise ContextualBatchError(f"{label} cannot be inspected: {error}") from error
    if resolved != path or stat.S_ISLNK(link.st_mode) or not stat.S_ISREG(link.st_mode):
        raise ContextualBatchError(f"{label} must be a resolved regular non-symlink file")
    if executable and not os.access(path, os.X_OK):
        raise ContextualBatchError(f"{label} must be executable")
    return path


def private_directory_path(value: Any, label: str) -> Path:
    path = _path_text(value, label)
    if path == Path("/"):
        raise ContextualBatchError(f"{label} may not be the filesystem root")
    for forbidden in (Path("/tmp"), Path("/var/tmp")):
        if path == forbidden or forbidden in path.parents:
            raise ContextualBatchError(f"{label} may not be under {forbidden}")
    if path.exists():
        link = path.lstat()
        if stat.S_ISLNK(link.st_mode) or not stat.S_ISDIR(link.st_mode):
            raise ContextualBatchError(
                f"{label} must be a non-symlink directory or a new path"
            )
        if stat.S_IMODE(link.st_mode) & 0o077:
            raise ContextualBatchError(
                f"{label} must not grant group or world permissions"
            )
    return path


def _fingerprint(value: os.stat_result) -> tuple[int, ...]:
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


def _open_stable(path: Path, label: str) -> tuple[int, tuple[int, ...]]:
    try:
        before = path.lstat()
    except OSError as error:
        raise ContextualBatchError(f"{label} cannot be inspected: {error}") from error
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ContextualBatchError(f"{label} must be a regular non-symlink file")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ContextualBatchError(f"{label} cannot be opened safely: {error}") from error
    observed = os.fstat(descriptor)
    if not stat.S_ISREG(observed.st_mode) or _fingerprint(observed) != _fingerprint(before):
        os.close(descriptor)
        raise ContextualBatchError(f"{label} was replaced while opening")
    return descriptor, _fingerprint(observed)


def _verify_path_fingerprint(
    path: Path, descriptor: int, identity: tuple[int, ...], label: str
) -> None:
    descriptor_after = os.fstat(descriptor)
    try:
        path_after = path.lstat()
    except OSError as error:
        raise ContextualBatchError(
            f"{label} disappeared during inspection: {error}"
        ) from error
    if (
        stat.S_ISLNK(path_after.st_mode)
        or _fingerprint(descriptor_after) != identity
        or _fingerprint(path_after) != identity
    ):
        raise ContextualBatchError(f"{label} changed during inspection")


def stable_read(
    path: Path,
    label: str,
    *,
    maximum_bytes: int,
    exact_mode: int | None = None,
    require_single_link: bool = False,
) -> bytes:
    descriptor, identity = _open_stable(path, label)
    try:
        observed = os.fstat(descriptor)
        if exact_mode is not None and stat.S_IMODE(observed.st_mode) != exact_mode:
            raise ContextualBatchError(f"{label} mode must be exactly {exact_mode:04o}")
        if require_single_link and observed.st_nlink != 1:
            raise ContextualBatchError(f"{label} must have exactly one hard link")
        if observed.st_size > maximum_bytes:
            raise ContextualBatchError(
                f"{label} exceeds the {maximum_bytes}-byte limit"
            )
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, maximum_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > maximum_bytes:
                raise ContextualBatchError(
                    f"{label} exceeds the {maximum_bytes}-byte limit"
                )
        _verify_path_fingerprint(path, descriptor, identity, label)
    finally:
        os.close(descriptor)
    body = b"".join(chunks)
    if len(body) != identity[2]:
        raise ContextualBatchError(f"{label} produced a short read")
    return body


def stable_hash(
    path: Path,
    label: str,
    *,
    expected_sha256: str | None = None,
    expected_byte_count: int | None = None,
    maximum_bytes: int = MAX_TOTAL_INPUT_BYTES,
    exact_mode: int | None = None,
    require_single_link: bool = False,
) -> tuple[str, int]:
    descriptor, identity = _open_stable(path, label)
    try:
        observed = os.fstat(descriptor)
        if exact_mode is not None and stat.S_IMODE(observed.st_mode) != exact_mode:
            raise ContextualBatchError(f"{label} mode must be exactly {exact_mode:04o}")
        if require_single_link and observed.st_nlink != 1:
            raise ContextualBatchError(f"{label} must have exactly one hard link")
        if observed.st_size > maximum_bytes:
            raise ContextualBatchError(
                f"{label} exceeds the {maximum_bytes}-byte limit"
            )
        if expected_byte_count is not None and observed.st_size != expected_byte_count:
            raise ContextualBatchError(f"{label} byte count differs from its pin")
        digest = hashlib.sha256()
        total = 0
        while chunk := os.read(descriptor, 8 * 1024 * 1024):
            digest.update(chunk)
            total += len(chunk)
        _verify_path_fingerprint(path, descriptor, identity, label)
    finally:
        os.close(descriptor)
    observed_sha = digest.hexdigest()
    if total != identity[2]:
        raise ContextualBatchError(f"{label} produced a short read")
    if expected_sha256 is not None and observed_sha != expected_sha256:
        raise ContextualBatchError(f"{label} SHA-256 differs from its pin")
    return observed_sha, total


def _file_uri_path(value: Any, label: str) -> Path:
    if not isinstance(value, str):
        raise ContextualBatchError(f"{label} must be a local file URI")
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme != "file"
        or parsed.netloc not in ("", "localhost")
        or parsed.query
        or parsed.fragment
    ):
        raise ContextualBatchError(f"{label} must be a canonical local file URI")
    path = Path(urllib.parse.unquote(parsed.path))
    return existing_regular_file(path, label)


def _catalog_free_validate(path: Path) -> dict[str, Any]:
    source = str(CORPUS_SOURCE_ROOT)
    if source not in sys.path:
        sys.path.insert(0, source)
    try:
        from himr_corpus.asr_result_importer import (  # noqa: PLC0415
            ResultImportError,
            validate_asr_whispercpp_result_file,
        )
    except ImportError as error:
        raise ContextualBatchError(
            f"catalog-free ASR validator could not be loaded: {error}"
        ) from error
    try:
        return validate_asr_whispercpp_result_file(path)
    except ResultImportError as error:
        raise ContextualBatchError(
            f"baseline ASR result failed catalog-free validation: {error}"
        ) from error


def _parse_canonical_json_text(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, str) or len(value.encode("utf-8")) > MAX_RESULT_BYTES:
        raise ContextualBatchError(f"{label} must be bounded canonical JSON text")
    parsed = parse_json(value.encode("utf-8"), label)
    if not isinstance(parsed, dict) or canonical_bytes(parsed).decode("utf-8") != value:
        raise ContextualBatchError(f"{label} must be a canonical JSON object")
    return parsed


def _software_pin(path: Path, **identity: Any) -> dict[str, Any]:
    sha, size = stable_hash(
        path,
        f"software source {path.name}",
        maximum_bytes=MAX_SOURCE_BYTES,
    )
    return {"path": str(path), "sha256": sha, "byte_count": size, **identity}


def _software_document() -> dict[str, Any]:
    return {
        "contextual_batch": _software_pin(
            PROGRAM_SOURCE,
            name=MATERIALIZER_NAME,
            implementation_version=IMPLEMENTATION_VERSION,
        ),
        "asr_adapter": _software_pin(
            ADAPTER_SOURCE,
            name="himr-asr-whispercpp",
            contract_version=asr_whispercpp.CONTRACT_VERSION,
            implementation_version=asr_whispercpp.IMPLEMENTATION_VERSION,
        ),
        "engine_profiles": _software_pin(
            ENGINE_PROFILES_SOURCE,
            name="himr-whispercpp-engine-profiles",
            contract_version=whispercpp_engine_profiles.PROFILE_CONTRACT_VERSION,
            implementation_version=whispercpp_engine_profiles.PROFILE_IMPLEMENTATION_VERSION,
        ),
        "catalog_free_validator": _software_pin(
            CATALOG_VALIDATOR_SOURCE,
            name="himr-corpus-asr-result-validator",
        ),
    }


def _validate_glossary(path: Path) -> dict[str, Any]:
    path = existing_regular_file(path, "neutral glossary")
    body = stable_read(
        path,
        "neutral glossary",
        maximum_bytes=MAX_GLOSSARY_BYTES,
        exact_mode=0o400,
        require_single_link=True,
    )
    raw = parse_json(body, "neutral glossary")
    try:
        # A concrete baseline-language equality check is performed after all raw
        # baselines are loaded.  ``auto`` here lets the adapter validate shape.
        document = asr_whispercpp.validate_glossary_document(raw, "auto")
    except asr_whispercpp.ASRError as error:
        raise ContextualBatchError(f"neutral glossary is invalid: {error}") from error
    second = stable_read(
        path,
        "neutral glossary closing read",
        maximum_bytes=MAX_GLOSSARY_BYTES,
        exact_mode=0o400,
        require_single_link=True,
    )
    if second != body:
        raise ContextualBatchError("neutral glossary changed during validation")
    return {
        "path": path,
        "body": body,
        "raw_sha256": sha256_bytes(body),
        "canonical_sha256": sha256_bytes(canonical_bytes(raw)),
        "byte_count": len(body),
        "document": document,
        "manifest": {
            "path": str(path),
            "raw_sha256": sha256_bytes(body),
            "canonical_sha256": sha256_bytes(canonical_bytes(raw)),
            "byte_count": len(body),
            "schema_version": document["schema_version"],
            "glossary_revision_id": document["glossary_revision_id"],
            "revision_sha256": sha256_bytes(document["revision"].encode("utf-8")),
            "language": document["language"],
            "term_count": len(document["terms"]),
            "prompt_sha256": document["prompt_sha256"],
            "terms_in_manifest": False,
        },
    }


def _validate_sealed_result_directory(path: Path) -> None:
    if path.name != "result.json":
        raise ContextualBatchError("baseline path must end in result.json")
    directory = path.parent
    try:
        link = directory.lstat()
        entries = sorted(item.name for item in directory.iterdir())
    except OSError as error:
        raise ContextualBatchError(
            f"baseline result directory cannot be inspected: {error}"
        ) from error
    if (
        stat.S_ISLNK(link.st_mode)
        or not stat.S_ISDIR(link.st_mode)
        or stat.S_IMODE(link.st_mode) != 0o500
    ):
        raise ContextualBatchError(
            "baseline result directory must be a non-symlink directory with mode 0500"
        )
    if entries != sorted(RESULT_FILENAMES):
        raise ContextualBatchError(
            "baseline result directory must contain exactly the three ASR result files"
        )


def _validate_baseline(path: Path) -> dict[str, Any]:
    path = existing_regular_file(path, "raw baseline result")
    _validate_sealed_result_directory(path)
    body = stable_read(
        path,
        "raw baseline result",
        maximum_bytes=MAX_RESULT_BYTES,
        exact_mode=0o400,
        require_single_link=True,
    )
    raw = parse_json(body, "raw baseline result")
    if not isinstance(raw, dict):
        raise ContextualBatchError("raw baseline result must contain an object")
    if raw.get("result_path") != str(path):
        raise ContextualBatchError("raw baseline result_path differs from its sealed path")
    if (
        raw.get("schema_version") != 1
        or raw.get("status") != "completed"
        or raw.get("dry_run") is not False
        or raw.get("glossary") is not None
    ):
        raise ContextualBatchError(
            "baseline must be one current completed non-dry raw ASR result with null glossary"
        )
    run = raw.get("processing_run")
    if not isinstance(run, dict) or (
        run.get("implementation_version") != asr_whispercpp.IMPLEMENTATION_VERSION
        or run.get("glossary_revision_id") is not None
        or run.get("status") != "completed"
    ):
        raise ContextualBatchError("baseline processing run is not current raw ASR")
    recipe = _parse_canonical_json_text(
        run.get("parameters_json"), "baseline processing_run.parameters_json"
    )
    if (
        recipe.get("contract_version") != asr_whispercpp.CONTRACT_VERSION
        or recipe.get("implementation_version")
        != asr_whispercpp.IMPLEMENTATION_VERSION
        or recipe.get("stage") != asr_whispercpp.STAGE
        or recipe.get("glossary") is not None
    ):
        raise ContextualBatchError(
            "baseline recipe must be current raw ASR with a null glossary"
        )
    inference = recipe.get("inference")
    if not isinstance(inference, dict):
        raise ContextualBatchError("baseline recipe lacks decoding inference")

    validation = _catalog_free_validate(path)
    canonical_sha = sha256_bytes(canonical_bytes(raw))
    if (
        validation.get("result_envelope_sha256") != canonical_sha
        or validation.get("glossary_revision_id") is not None
        or validation.get("job_id") != raw.get("job_id")
        or validation.get("processing_run_id") != run.get("processing_run_id")
        or validation.get("recipe_id") != raw.get("recipe_id")
        or validation.get("result_key") != raw.get("result_key")
    ):
        raise ContextualBatchError("catalog-free baseline summary differs from its envelope")

    records = raw.get("catalog_records")
    if not isinstance(records, dict):
        raise ContextualBatchError("baseline lacks catalog record provenance")
    revisions = records.get("transcript_revisions", [])
    if not isinstance(revisions, list) or any(
        not isinstance(revision, dict)
        or revision.get("revision_kind") != "raw_asr"
        or revision.get("glossary_revision_id") is not None
        for revision in revisions
    ):
        raise ContextualBatchError("baseline catalog transcript provenance is not raw ASR")

    artifacts = raw.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 2:
        raise ContextualBatchError("baseline must reference exactly two ASR artifacts")
    artifact_pins: list[dict[str, Any]] = []
    observed_kinds: set[str] = set()
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            raise ContextualBatchError("baseline artifact row must be an object")
        kind = artifact.get("artifact_kind")
        expected_name = ARTIFACT_FILENAME_BY_KIND.get(kind)
        if expected_name is None or kind in observed_kinds:
            raise ContextualBatchError("baseline artifact kinds are incomplete or duplicated")
        observed_kinds.add(kind)
        artifact_path = _file_uri_path(
            artifact.get("storage_uri"), f"baseline {kind} artifact URI"
        )
        if artifact_path != path.parent / expected_name:
            raise ContextualBatchError(
                f"baseline {kind} artifact is outside its closed result directory"
            )
        artifact_body = stable_read(
            artifact_path,
            f"baseline {kind} artifact",
            maximum_bytes=MAX_ARTIFACT_BYTES,
            exact_mode=0o400,
            require_single_link=True,
        )
        parse_json(artifact_body, f"baseline {kind} artifact")
        observed_sha = sha256_bytes(artifact_body)
        if (
            artifact.get("sha256") != observed_sha
            or artifact.get("byte_count") != len(artifact_body)
            or artifact.get("visibility") != "private"
        ):
            raise ContextualBatchError(f"baseline {kind} artifact differs from its pin")
        artifact_pins.append(
            {
                "artifact_kind": kind,
                "path": str(artifact_path),
                "sha256": observed_sha,
                "byte_count": len(artifact_body),
            }
        )
    artifact_pins.sort(key=lambda row: row["artifact_kind"])

    input_row = raw.get("input")
    engine_row = raw.get("engine")
    model_row = raw.get("model")
    window = raw.get("window")
    if not all(isinstance(value, dict) for value in (input_row, engine_row, model_row, window)):
        raise ContextualBatchError("baseline input/engine/model/window blocks must be objects")

    input_path = existing_regular_file(input_row.get("path"), "baseline normalized input")
    input_sha, input_size = stable_hash(
        input_path,
        "baseline normalized input",
        expected_sha256=sha256_value(input_row.get("sha256"), "baseline input.sha256"),
        expected_byte_count=integer(
            input_row.get("byte_count"), "baseline input.byte_count", 1, MAX_TOTAL_INPUT_BYTES
        ),
    )
    duration_ms = integer(
        input_row.get("probe", {}).get("duration_ms")
        if isinstance(input_row.get("probe"), dict)
        else None,
        "baseline input.probe.duration_ms",
        1,
        asr_whispercpp.MAX_WINDOW_MS,
    )
    if window != {"offset_ms": 0, "duration_ms": duration_ms, "end_ms": duration_ms}:
        raise ContextualBatchError(
            "contextual pilot requires a raw baseline covering the full normalized input"
        )

    engine_path = existing_regular_file(
        engine_row.get("path"), "baseline whisper.cpp executable", executable=True
    )
    engine_sha, engine_size = stable_hash(
        engine_path,
        "baseline whisper.cpp executable",
        expected_sha256=sha256_value(engine_row.get("sha256"), "baseline engine.sha256"),
        expected_byte_count=integer(
            engine_row.get("byte_count"), "baseline engine.byte_count", 1, MAX_SOURCE_BYTES
        ),
        maximum_bytes=MAX_SOURCE_BYTES,
    )
    try:
        engine_profile = whispercpp_engine_profiles.match_engine_profile(
            engine_sha, engine_size, allow_legacy_manifest_replay=False
        )
    except whispercpp_engine_profiles.EngineProfileError as error:
        raise ContextualBatchError(
            f"baseline engine is not eligible for a new contextual batch: {error}"
        ) from error
    expected_engine = whispercpp_engine_profiles.public_engine_document(
        engine_profile, str(engine_path)
    )
    if any(
        engine_row.get(source_key) != expected_engine[target_key]
        for source_key, target_key in (
            ("sha256", "expected_sha256"),
            ("byte_count", "byte_count"),
            ("version", "version_label"),
            ("version_evidence", "version_evidence"),
            ("build", "build"),
        )
    ):
        raise ContextualBatchError("baseline engine provenance differs from its current profile")

    model_path = existing_regular_file(model_row.get("path"), "baseline ASR model")
    model_sha, model_size = stable_hash(
        model_path,
        "baseline ASR model",
        expected_sha256=sha256_value(model_row.get("sha256"), "baseline model.sha256"),
        expected_byte_count=integer(
            model_row.get("byte_count"), "baseline model.byte_count", 1, MAX_TOTAL_INPUT_BYTES
        ),
    )

    engine_order = {
        key: expected_engine[key]
        for key in (
            "executable",
            "expected_sha256",
            "version_label",
            "version_evidence",
            "build",
        )
    }
    model_order = {
        "path": str(model_path),
        "expected_sha256": model_sha,
        "model_id": identifier(model_row.get("model_id"), "baseline model.model_id"),
        "name": model_row.get("name"),
        "revision": model_row.get("revision"),
        "source": model_row.get("source"),
        "license_label": model_row.get("license_label"),
    }
    input_order = {
        "path": str(input_path),
        "expected_sha256": input_sha,
        "media_id": identifier(input_row.get("media_id"), "baseline input.media_id"),
        "artifact_id": identifier(
            input_row.get("artifact_id"), "baseline input.artifact_id"
        ),
        "parent_processing_run_id": identifier(
            input_row.get("parent_processing_run_id"),
            "baseline input.parent_processing_run_id",
        ),
    }
    catalog_context = raw.get("catalog_context")
    pair_projection = {
        "input": input_order,
        "engine": engine_order,
        "model": model_order,
        "window": {"offset_ms": 0, "duration_ms": duration_ms},
        "inference": inference,
        "catalog_context": catalog_context,
    }

    # Close the race around the importer validation and all derived projections.
    closing_body = stable_read(
        path,
        "raw baseline result closing read",
        maximum_bytes=MAX_RESULT_BYTES,
        exact_mode=0o400,
        require_single_link=True,
    )
    if closing_body != body:
        raise ContextualBatchError("raw baseline result changed during validation")
    for pin in artifact_pins:
        stable_hash(
            Path(pin["path"]),
            f"baseline {pin['artifact_kind']} closing hash",
            expected_sha256=pin["sha256"],
            expected_byte_count=pin["byte_count"],
            maximum_bytes=MAX_ARTIFACT_BYTES,
            exact_mode=0o400,
            require_single_link=True,
        )

    return {
        "path": path,
        "raw_sha256": sha256_bytes(body),
        "canonical_sha256": canonical_sha,
        "byte_count": len(body),
        "job_id": identifier(raw.get("job_id"), "baseline job_id"),
        "work_order_sha256": sha256_value(
            raw.get("work_order_sha256"), "baseline work_order_sha256"
        ),
        "recipe_id": identifier(raw.get("recipe_id"), "baseline recipe_id"),
        "result_key": sha256_value(raw.get("result_key"), "baseline result_key"),
        "processing_run_id": identifier(
            run.get("processing_run_id"), "baseline processing_run_id"
        ),
        "artifacts": artifact_pins,
        "input_order": input_order,
        "input_byte_count": input_size,
        "input_duration_ms": duration_ms,
        "engine_order": engine_order,
        "engine_manifest": {
            **expected_engine,
            "profile_id": engine_profile["profile_id"],
            "admission": engine_profile["admission"],
            "output_json_full_utf8_token_boundary_merge": engine_profile[
                "output_json_full_utf8_token_boundary_merge"
            ],
        },
        "model_order": model_order,
        "model_manifest": {**model_order, "byte_count": model_size},
        "window_order": {"offset_ms": 0, "duration_ms": duration_ms},
        "window_manifest": {
            "offset_ms": 0,
            "duration_ms": duration_ms,
            "end_ms": duration_ms,
        },
        "inference": inference,
        "catalog_context": catalog_context,
        "pair_projection_sha256": sha256_bytes(canonical_bytes(pair_projection)),
    }


def _same_document(rows: list[dict[str, Any]], key: str, label: str) -> dict[str, Any]:
    first = rows[0][key]
    expected = canonical_bytes(first)
    if any(canonical_bytes(row[key]) != expected for row in rows[1:]):
        raise ContextualBatchError(f"all contextual baselines must share one exact {label}")
    return first


def _validate_roots(batch_root: Path, asr_output_root: Path) -> tuple[Path, Path]:
    batch_root = private_directory_path(batch_root, "batch root")
    asr_output_root = private_directory_path(asr_output_root, "ASR output root")
    if (
        batch_root == asr_output_root
        or batch_root in asr_output_root.parents
        or asr_output_root in batch_root.parents
    ):
        raise ContextualBatchError("batch and ASR output roots must be disjoint")
    return batch_root, asr_output_root


def _build_contextual_order(
    baseline: dict[str, Any], glossary: dict[str, Any], asr_output_root: Path
) -> dict[str, Any]:
    job_identity = sha256_bytes(
        canonical_bytes(
            {
                "pass_kind": PASS_KIND,
                "baseline_result_canonical_sha256": baseline["canonical_sha256"],
                "glossary_raw_sha256": glossary["raw_sha256"],
            }
        )
    )
    order = {
        "schema_version": 1,
        "job_id": f"asr-contextual-{job_identity[:32]}",
        "input": baseline["input_order"],
        "engine": baseline["engine_order"],
        "model": baseline["model_order"],
        "window": baseline["window_order"],
        "inference": baseline["inference"],
        "glossary": {
            "path": str(glossary["path"]),
            "expected_sha256": glossary["raw_sha256"],
        },
        "catalog_context": baseline["catalog_context"],
        "output": {"root": str(asr_output_root)},
    }
    try:
        normalized = asr_whispercpp.validate_work_order(order)
    except asr_whispercpp.ASRError as error:
        raise ContextualBatchError(
            f"generated contextual work order is invalid: {error}"
        ) from error
    if normalized != order:
        raise ContextualBatchError("generated contextual work order is not canonical")
    projection = {
        key: order[key]
        for key in ("input", "engine", "model", "window", "inference", "catalog_context")
    }
    if sha256_bytes(canonical_bytes(projection)) != baseline["pair_projection_sha256"]:
        raise ContextualBatchError(
            "contextual work order changed baseline input/engine/model/window/inference/context"
        )
    return order


def build_batch(
    *,
    baseline_paths: list[Path],
    glossary_path: Path,
    batch_root: Path,
    asr_output_root: Path,
) -> tuple[dict[str, Any], list[bytes]]:
    if not 1 <= len(baseline_paths) <= MAX_BASELINES:
        raise ContextualBatchError(
            f"contextual batch requires between 1 and {MAX_BASELINES} explicit baselines"
        )
    batch_root, asr_output_root = _validate_roots(batch_root, asr_output_root)
    glossary = _validate_glossary(glossary_path)
    if batch_root == glossary["path"] or batch_root in glossary["path"].parents:
        raise ContextualBatchError("neutral glossary may not be stored under the batch root")
    if asr_output_root == glossary["path"] or asr_output_root in glossary["path"].parents:
        raise ContextualBatchError("neutral glossary may not be stored under the ASR output root")

    normalized_paths = [existing_regular_file(path, "raw baseline result") for path in baseline_paths]
    if len(set(normalized_paths)) != len(normalized_paths):
        raise ContextualBatchError("raw baseline result paths must be unique")
    if any(batch_root == path or batch_root in path.parents for path in normalized_paths):
        raise ContextualBatchError("raw baseline results may not be stored under the batch root")

    baselines = [_validate_baseline(path) for path in normalized_paths]
    baselines.sort(
        key=lambda row: (
            row["input_order"]["expected_sha256"],
            row["window_order"]["offset_ms"],
            row["window_order"]["duration_ms"],
            row["canonical_sha256"],
            str(row["path"]),
        )
    )
    pair_keys = [
        (
            row["input_order"]["expected_sha256"],
            row["window_order"]["offset_ms"],
            row["window_order"]["duration_ms"],
            row["pair_projection_sha256"],
        )
        for row in baselines
    ]
    if len(set(pair_keys)) != len(pair_keys):
        raise ContextualBatchError("raw baselines contain a duplicate contextual pair target")

    engine = _same_document(baselines, "engine_manifest", "engine")
    model = _same_document(baselines, "model_manifest", "model")
    inference = _same_document(baselines, "inference", "decoding policy")
    language = inference.get("language")
    if language == "auto" or language != glossary["document"]["language"]:
        raise ContextualBatchError(
            "contextual pilot requires one concrete inference language equal to glossary.language"
        )

    software = _software_document()
    work_order_bodies: list[bytes] = []
    entries: list[dict[str, Any]] = []
    total_input_bytes = 0
    total_input_ms = 0
    total_baseline_bytes = 0
    for ordinal, baseline in enumerate(baselines, start=1):
        order = _build_contextual_order(baseline, glossary, asr_output_root)
        body = pretty_bytes(order)
        if len(body) > MAX_WORK_ORDER_BYTES:
            raise ContextualBatchError("generated contextual work order exceeds its byte cap")
        canonical_sha = sha256_bytes(canonical_bytes(order))
        work_order_bodies.append(body)
        total_input_bytes += baseline["input_byte_count"]
        total_input_ms += baseline["input_duration_ms"]
        total_baseline_bytes += baseline["byte_count"]
        entries.append(
            {
                "ordinal": ordinal,
                "path": f"work-orders/{ordinal:06d}.json",
                "job_id": order["job_id"],
                "sha256": sha256_bytes(body),
                "canonical_sha256": canonical_sha,
                "byte_count": len(body),
                "pair_projection_sha256": baseline["pair_projection_sha256"],
                "baseline": {
                    "result_path": str(baseline["path"]),
                    "raw_sha256": baseline["raw_sha256"],
                    "canonical_sha256": baseline["canonical_sha256"],
                    "byte_count": baseline["byte_count"],
                    "job_id": baseline["job_id"],
                    "work_order_sha256": baseline["work_order_sha256"],
                    "recipe_id": baseline["recipe_id"],
                    "result_key": baseline["result_key"],
                    "processing_run_id": baseline["processing_run_id"],
                    "artifacts": baseline["artifacts"],
                },
                "input": {
                    "path": baseline["input_order"]["path"],
                    "sha256": baseline["input_order"]["expected_sha256"],
                    "byte_count": baseline["input_byte_count"],
                    "media_id": baseline["input_order"]["media_id"],
                    "artifact_id": baseline["input_order"]["artifact_id"],
                    "parent_processing_run_id": baseline["input_order"][
                        "parent_processing_run_id"
                    ],
                    "duration_ms": baseline["input_duration_ms"],
                },
                "window": baseline["window_manifest"],
                "catalog_context": baseline["catalog_context"],
            }
        )

    if total_input_bytes > MAX_TOTAL_INPUT_BYTES or total_input_ms > MAX_TOTAL_INPUT_MS:
        raise ContextualBatchError("contextual batch exceeds the total resource cap")
    core = {
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "materializer": MATERIALIZER_NAME,
        "software": software,
        "glossary": glossary["manifest"],
        "engine": engine,
        "model": model,
        "inference": inference,
        "profile": PROFILE,
        "output": {
            "batch_root": str(batch_root),
            "asr_output_root": str(asr_output_root),
        },
        "work_order_count": len(entries),
        "totals": {
            "input_byte_count": total_input_bytes,
            "input_duration_ms": total_input_ms,
            "baseline_result_byte_count": total_baseline_bytes,
        },
        "work_orders": entries,
        "safety": SAFETY,
    }
    identity = sha256_bytes(canonical_bytes(core))
    batch_id = f"ctxasrbatch_{identity[:32]}"
    manifest = {
        **core,
        "batch_id": batch_id,
        "identity_sha256": identity,
        "batch_relative_path": f"batches/{batch_id}",
    }
    # Bind the exact implementation sources around the full build.
    if _software_document() != software:
        raise ContextualBatchError("contextual batch software changed during materialization")
    if _validate_glossary(glossary["path"])["manifest"] != glossary["manifest"]:
        raise ContextualBatchError("neutral glossary changed during materialization")
    return manifest, work_order_bodies


def _ensure_private_root(path: Path, label: str) -> None:
    if path.exists():
        private_directory_path(path, label)
        return
    try:
        path.mkdir(parents=True, mode=0o700)
        os.chmod(path, 0o700)
    except OSError as error:
        raise ContextualBatchError(f"cannot create {label}: {error}") from error
    private_directory_path(path, label)


def _write_exclusive(path: Path, body: bytes, mode: int) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, mode)
    try:
        offset = 0
        while offset < len(body):
            offset += os.write(descriptor, body[offset:])
        os.fchmod(descriptor, mode)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _sync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _verify_sealed_batch(
    manifest_path: Path, manifest: dict[str, Any], work_order_bodies: list[bytes]
) -> None:
    final = manifest_path.parent
    orders = final / "work-orders"
    if (
        final.name != manifest["batch_id"]
        or final.parent.name != "batches"
        or final.parent.parent != Path(manifest["output"]["batch_root"])
    ):
        raise ContextualBatchError("manifest is not stored at its deterministic batch path")
    for directory, label in ((final, "batch directory"), (orders, "work-order directory")):
        link = directory.lstat()
        if (
            stat.S_ISLNK(link.st_mode)
            or not stat.S_ISDIR(link.st_mode)
            or stat.S_IMODE(link.st_mode) != 0o500
        ):
            raise ContextualBatchError(
                f"sealed {label} must be a non-symlink directory with mode 0500"
            )
    if {item.name for item in final.iterdir()} != {"manifest.json", "work-orders"}:
        raise ContextualBatchError("sealed batch directory has missing or extra entries")
    expected_names = {
        f"{ordinal:06d}.json" for ordinal in range(1, len(work_order_bodies) + 1)
    }
    if {item.name for item in orders.iterdir()} != expected_names:
        raise ContextualBatchError("sealed work-order directory has missing or extra entries")
    manifest_body = stable_read(
        manifest_path,
        "sealed contextual manifest",
        maximum_bytes=MAX_MANIFEST_BYTES,
        exact_mode=0o400,
        require_single_link=True,
    )
    if manifest_body != pretty_bytes(manifest):
        raise ContextualBatchError("sealed contextual manifest failed exact byte replay")
    for ordinal, expected_body in enumerate(work_order_bodies, start=1):
        path = orders / f"{ordinal:06d}.json"
        observed = stable_read(
            path,
            f"sealed contextual work order {ordinal}",
            maximum_bytes=MAX_WORK_ORDER_BYTES,
            exact_mode=0o400,
            require_single_link=True,
        )
        entry = manifest["work_orders"][ordinal - 1]
        if (
            observed != expected_body
            or sha256_bytes(observed) != entry["sha256"]
            or len(observed) != entry["byte_count"]
        ):
            raise ContextualBatchError(
                f"sealed contextual work order {ordinal} differs from its manifest"
            )


def materialize_batch(
    *,
    baseline_paths: list[Path],
    glossary_path: Path,
    batch_root: Path,
    asr_output_root: Path,
) -> tuple[dict[str, Any], Path]:
    manifest, work_order_bodies = build_batch(
        baseline_paths=baseline_paths,
        glossary_path=glossary_path,
        batch_root=batch_root,
        asr_output_root=asr_output_root,
    )
    batch_root = Path(manifest["output"]["batch_root"])
    _ensure_private_root(batch_root, "batch root")
    batches = batch_root / "batches"
    _ensure_private_root(batches, "batch collection")
    lock_path = batch_root / ".contextual-asr-batch.lock"
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(lock_path, flags, 0o600)
    final = batches / manifest["batch_id"]
    try:
        lock_stat = os.fstat(descriptor)
        path_stat = lock_path.lstat()
        if (
            not stat.S_ISREG(lock_stat.st_mode)
            or lock_stat.st_nlink != 1
            or (lock_stat.st_dev, lock_stat.st_ino) != (path_stat.st_dev, path_stat.st_ino)
        ):
            raise ContextualBatchError(
                "contextual batch lock must be a single-link regular file"
            )
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        path_stat = lock_path.lstat()
        lock_stat = os.fstat(descriptor)
        if (lock_stat.st_dev, lock_stat.st_ino) != (path_stat.st_dev, path_stat.st_ino):
            raise ContextualBatchError("contextual batch lock was replaced")
        manifest_path = final / "manifest.json"
        if final.exists():
            _verify_sealed_batch(manifest_path, manifest, work_order_bodies)
            return manifest, manifest_path
        staging = batches / f".{manifest['batch_id']}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
        try:
            staging.mkdir(mode=0o700)
            orders = staging / "work-orders"
            orders.mkdir(mode=0o700)
            for ordinal, body in enumerate(work_order_bodies, start=1):
                _write_exclusive(orders / f"{ordinal:06d}.json", body, 0o400)
            _write_exclusive(staging / "manifest.json", pretty_bytes(manifest), 0o400)
            os.chmod(orders, 0o500)
            _sync_directory(orders)
            os.chmod(staging, 0o500)
            _sync_directory(staging)
            os.rename(staging, final)
            _sync_directory(batches)
        finally:
            if staging.exists():
                os.chmod(staging, 0o700)
                for child in staging.rglob("*"):
                    try:
                        os.chmod(child, 0o700 if child.is_dir() else 0o600)
                    except OSError:
                        pass
                shutil.rmtree(staging)
        manifest_path = final / "manifest.json"
        _verify_sealed_batch(manifest_path, manifest, work_order_bodies)
        return manifest, manifest_path
    finally:
        os.close(descriptor)


def validate_batch(manifest_path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifest_path = existing_regular_file(manifest_path, "contextual batch manifest")
    body = stable_read(
        manifest_path,
        "contextual batch manifest",
        maximum_bytes=MAX_MANIFEST_BYTES,
        exact_mode=0o400,
        require_single_link=True,
    )
    supplied = exact_object(
        parse_json(body, "contextual batch manifest"),
        "contextual batch manifest",
        MANIFEST_KEYS,
    )
    if (
        supplied.get("schema_version") != SCHEMA_VERSION
        or supplied.get("implementation_version") != IMPLEMENTATION_VERSION
        or supplied.get("materializer") != MATERIALIZER_NAME
        or not isinstance(supplied.get("batch_id"), str)
        or not BATCH_ID_RE.fullmatch(supplied["batch_id"])
    ):
        raise ContextualBatchError("contextual batch manifest identity is unsupported")
    entries = supplied.get("work_orders")
    if not isinstance(entries, list) or not 1 <= len(entries) <= MAX_BASELINES:
        raise ContextualBatchError("contextual batch work-order count is invalid")
    baseline_paths: list[Path] = []
    for ordinal, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict) or not isinstance(entry.get("baseline"), dict):
            raise ContextualBatchError("contextual manifest contains a malformed entry")
        if entry.get("ordinal") != ordinal or entry.get("path") != f"work-orders/{ordinal:06d}.json":
            raise ContextualBatchError("contextual manifest ordinal/path order is invalid")
        baseline_paths.append(
            existing_regular_file(
                entry["baseline"].get("result_path"),
                f"contextual baseline {ordinal}",
            )
        )
    glossary = supplied.get("glossary")
    output = supplied.get("output")
    if not isinstance(glossary, dict) or not isinstance(output, dict):
        raise ContextualBatchError("contextual manifest glossary/output blocks are invalid")
    rebuilt, work_order_bodies = build_batch(
        baseline_paths=baseline_paths,
        glossary_path=existing_regular_file(glossary.get("path"), "manifest glossary"),
        batch_root=private_directory_path(output.get("batch_root"), "manifest batch root"),
        asr_output_root=private_directory_path(
            output.get("asr_output_root"), "manifest ASR output root"
        ),
    )
    if rebuilt != supplied or pretty_bytes(rebuilt) != body:
        raise ContextualBatchError("contextual manifest failed deterministic replay")
    _verify_sealed_batch(manifest_path, rebuilt, work_order_bodies)
    orders: list[dict[str, Any]] = []
    for ordinal, order_body in enumerate(work_order_bodies, start=1):
        raw = parse_json(order_body, f"contextual work order {ordinal}")
        try:
            order = asr_whispercpp.validate_work_order(raw)
        except asr_whispercpp.ASRError as error:
            raise ContextualBatchError(
                f"sealed contextual work order {ordinal} is invalid: {error}"
            ) from error
        entry = rebuilt["work_orders"][ordinal - 1]
        if sha256_bytes(canonical_bytes(order)) != entry["canonical_sha256"]:
            raise ContextualBatchError(
                f"sealed contextual work order {ordinal} canonical digest differs"
            )
        orders.append(order)
    return rebuilt, orders


def _verify_baseline_entry(entry: dict[str, Any]) -> None:
    baseline = entry["baseline"]
    path = Path(baseline["result_path"])
    _validate_sealed_result_directory(path)
    stable_hash(
        path,
        "dispatch baseline result",
        expected_sha256=baseline["raw_sha256"],
        expected_byte_count=baseline["byte_count"],
        maximum_bytes=MAX_RESULT_BYTES,
        exact_mode=0o400,
        require_single_link=True,
    )
    for artifact in baseline["artifacts"]:
        stable_hash(
            Path(artifact["path"]),
            f"dispatch baseline {artifact['artifact_kind']}",
            expected_sha256=artifact["sha256"],
            expected_byte_count=artifact["byte_count"],
            maximum_bytes=MAX_ARTIFACT_BYTES,
            exact_mode=0o400,
            require_single_link=True,
        )


def _validate_adapter_result(
    result: Any,
    order: dict[str, Any],
    entry: dict[str, Any],
    glossary: dict[str, Any],
    engine: dict[str, Any],
    model: dict[str, Any],
    *,
    dry_run: bool,
) -> dict[str, Any]:
    if not isinstance(result, dict):
        raise ContextualBatchError("ASR adapter returned a non-object result")
    expected_status = "planned" if dry_run else "completed"
    expected_order_sha = sha256_bytes(canonical_bytes(order))
    if (
        result.get("job_id") != order["job_id"]
        or result.get("status") != expected_status
        or result.get("dry_run") is not dry_run
        or result.get("work_order_sha256") != expected_order_sha
        or result.get("catalog_context") != order["catalog_context"]
    ):
        raise ContextualBatchError("ASR adapter returned an inconsistent contextual envelope")
    if result.get("window") != {
        "offset_ms": 0,
        "duration_ms": order["window"]["duration_ms"],
        "end_ms": order["window"]["duration_ms"],
    }:
        raise ContextualBatchError("ASR adapter changed the paired full-input window")
    result_path = (
        _path_text(result.get("result_path"), "planned adapter result")
        if dry_run
        else existing_regular_file(result.get("result_path"), "adapter result")
    )
    observed_glossary = result.get("glossary")
    if not isinstance(observed_glossary, dict) or any(
        observed_glossary.get(key) != glossary[key]
        for key in (
            "glossary_revision_id",
            "language",
            "prompt_sha256",
            "term_count",
        )
    ) or any(
        observed_glossary.get(key) != glossary[source]
        for key, source in (
            ("sha256", "raw_sha256"),
            ("byte_count", "byte_count"),
            ("path", "path"),
        )
    ):
        raise ContextualBatchError("ASR adapter glossary provenance differs from the batch")
    if sha256_bytes(observed_glossary.get("revision", "").encode("utf-8")) != glossary[
        "revision_sha256"
    ]:
        raise ContextualBatchError("ASR adapter glossary revision differs from the batch")
    adapter_input = result.get("input")
    if not isinstance(adapter_input, dict) or any(
        adapter_input.get(observed_key) != expected
        for observed_key, expected in (
            ("path", order["input"]["path"]),
            ("sha256", order["input"]["expected_sha256"]),
            ("byte_count", entry["input"]["byte_count"]),
            ("media_id", order["input"]["media_id"]),
            ("artifact_id", order["input"]["artifact_id"]),
            (
                "parent_processing_run_id",
                order["input"]["parent_processing_run_id"],
            ),
        )
    ):
        raise ContextualBatchError("ASR adapter changed the paired input lineage")
    adapter_engine = result.get("engine")
    if not isinstance(adapter_engine, dict) or any(
        adapter_engine.get(observed_key) != expected
        for observed_key, expected in (
            ("path", order["engine"]["executable"]),
            ("sha256", order["engine"]["expected_sha256"]),
            ("byte_count", engine["byte_count"]),
            ("version", order["engine"]["version_label"]),
            ("version_evidence", order["engine"]["version_evidence"]),
            ("build", order["engine"]["build"]),
        )
    ):
        raise ContextualBatchError("ASR adapter changed the paired engine identity")
    adapter_model = result.get("model")
    if not isinstance(adapter_model, dict) or any(
        adapter_model.get(observed_key) != expected
        for observed_key, expected in (
            ("path", order["model"]["path"]),
            ("sha256", order["model"]["expected_sha256"]),
            ("byte_count", model["byte_count"]),
            ("model_id", order["model"]["model_id"]),
            ("name", order["model"]["name"]),
            ("revision", order["model"]["revision"]),
            ("source", order["model"]["source"]),
            ("license_label", order["model"]["license_label"]),
        )
    ):
        raise ContextualBatchError("ASR adapter changed the paired model identity")
    processing_run = result.get("processing_run")
    if not isinstance(processing_run, dict) or (
        processing_run.get("glossary_revision_id") != glossary["glossary_revision_id"]
        or processing_run.get("implementation_version")
        != asr_whispercpp.IMPLEMENTATION_VERSION
        or processing_run.get("stage") != asr_whispercpp.STAGE
        or processing_run.get("status") != ("queued" if dry_run else "completed")
    ):
        raise ContextualBatchError("ASR adapter processing run lacks contextual provenance")
    recipe = _parse_canonical_json_text(
        processing_run.get("parameters_json"),
        "contextual processing_run.parameters_json",
    )
    recipe_glossary = recipe.get("glossary")
    if (
        recipe.get("contract_version") != asr_whispercpp.CONTRACT_VERSION
        or recipe.get("implementation_version")
        != asr_whispercpp.IMPLEMENTATION_VERSION
        or recipe.get("stage") != asr_whispercpp.STAGE
        or recipe.get("window") != result["window"]
        or recipe.get("inference") != order["inference"]
        or not isinstance(recipe.get("engine"), dict)
        or recipe["engine"].get("sha256") != order["engine"]["expected_sha256"]
        or not isinstance(recipe.get("model"), dict)
        or recipe["model"].get("sha256") != order["model"]["expected_sha256"]
        or not isinstance(recipe_glossary, dict)
        or recipe_glossary.get("glossary_revision_id")
        != glossary["glossary_revision_id"]
        or recipe_glossary.get("sha256") != glossary["raw_sha256"]
        or recipe_glossary.get("prompt_sha256") != glossary["prompt_sha256"]
        or sha256_bytes(recipe_glossary.get("revision", "").encode("utf-8"))
        != glossary["revision_sha256"]
    ):
        raise ContextualBatchError("ASR adapter changed the paired contextual recipe")
    recipe_sha = sha256_bytes(canonical_bytes(recipe))
    recipe_id = f"recipe_asr_whispercpp_{recipe_sha[:32]}"
    expected_result_key = sha256_bytes(
        canonical_bytes(
            {
                "work_order_sha256": expected_order_sha,
                "input_sha256": order["input"]["expected_sha256"],
                "input_media_id": order["input"]["media_id"],
                "input_artifact_id": order["input"]["artifact_id"],
                "parent_processing_run_id": order["input"][
                    "parent_processing_run_id"
                ],
                "recipe_id": recipe_id,
                "catalog_context": order["catalog_context"],
            }
        )
    )
    if (
        result.get("recipe_sha256") != recipe_sha
        or result.get("recipe_id") != recipe_id
        or result.get("result_key") != expected_result_key
    ):
        raise ContextualBatchError("ASR adapter changed contextual recipe/result identity")
    if not dry_run:
        records = result.get("catalog_records")
        revisions = (
            records.get("transcript_revisions", [])
            if isinstance(records, dict)
            else None
        )
        if not isinstance(revisions, list) or any(
            not isinstance(revision, dict)
            or revision.get("revision_kind") != "contextual_asr"
            or revision.get("review_state") != "machine"
            or revision.get("glossary_revision_id")
            != glossary["glossary_revision_id"]
            for revision in revisions
        ):
            raise ContextualBatchError("ASR adapter emitted non-contextual transcript provenance")
        summary = _catalog_free_validate(result_path)
        if summary.get("glossary_revision_id") != glossary["glossary_revision_id"]:
            raise ContextualBatchError("completed contextual result failed glossary replay")
    output_root = Path(order["output"]["root"])
    if output_root not in result_path.parents:
        raise ContextualBatchError("ASR adapter result path escapes the private output root")
    return {
        "work_order_sha256": expected_order_sha,
        "status": expected_status,
        "recipe_id": identifier(result.get("recipe_id"), "adapter recipe_id"),
        "result_key": sha256_value(result.get("result_key"), "adapter result_key"),
        "result_path": str(result_path),
        "processing_run_id": identifier(
            processing_run.get("processing_run_id"), "adapter processing_run_id"
        ),
        "baseline_result_canonical_sha256": entry["baseline"]["canonical_sha256"],
        "glossary_revision_id": glossary["glossary_revision_id"],
    }


def run_batch(manifest_path: Path, *, dry_run: bool) -> dict[str, Any]:
    manifest, orders = validate_batch(manifest_path)
    manifest_body = pretty_bytes(manifest)
    if not dry_run:
        _ensure_private_root(
            Path(manifest["output"]["asr_output_root"]), "ASR output root"
        )
    results: list[dict[str, Any]] = []
    for ordinal, order in enumerate(orders, start=1):
        entry = manifest["work_orders"][ordinal - 1]
        order_path = manifest_path.parent / entry["path"]
        observed_order = stable_read(
            order_path,
            f"dispatch contextual work order {ordinal}",
            maximum_bytes=MAX_WORK_ORDER_BYTES,
            exact_mode=0o400,
            require_single_link=True,
        )
        if sha256_bytes(observed_order) != entry["sha256"]:
            raise ContextualBatchError(
                f"contextual work order {ordinal} changed before dispatch"
            )
        _verify_baseline_entry(entry)
        try:
            result = asr_whispercpp.run_asr(order, dry_run=dry_run)
            validated = _validate_adapter_result(
                result,
                order,
                entry,
                manifest["glossary"],
                manifest["engine"],
                manifest["model"],
                dry_run=dry_run,
            )
            _verify_baseline_entry(entry)
        except (
            ContextualBatchError,
            asr_whispercpp.ASRError,
            OSError,
            subprocess.SubprocessError,
        ) as error:
            quarantine = getattr(error, "quarantine", None)
            failure = {
                "schema_version": SCHEMA_VERSION,
                "implementation_version": IMPLEMENTATION_VERSION,
                "batch_id": manifest["batch_id"],
                "manifest_sha256": sha256_bytes(manifest_body),
                "status": "failed",
                "dry_run": dry_run,
                "job_count": len(orders),
                "results": results,
                "failed_job": {
                    "ordinal": ordinal,
                    "job_id": order["job_id"],
                    "work_order_sha256": sha256_bytes(canonical_bytes(order)),
                    "baseline_result_canonical_sha256": entry["baseline"][
                        "canonical_sha256"
                    ],
                    "error": {"type": type(error).__name__, "message": str(error)},
                    "quarantine": quarantine,
                },
                "safety": RUN_SAFETY,
            }
            raise ContextualBatchRunFailure(failure) from error
        results.append({"ordinal": ordinal, "job_id": order["job_id"], **validated})
    replayed, _ = validate_batch(manifest_path)
    if pretty_bytes(replayed) != manifest_body:
        raise ContextualBatchError("contextual batch changed during serial dispatch")
    return {
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "batch_id": manifest["batch_id"],
        "manifest_sha256": sha256_bytes(manifest_body),
        "status": "planned" if dry_run else "completed",
        "dry_run": dry_run,
        "job_count": len(orders),
        "results": results,
        "safety": RUN_SAFETY,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize, validate, or serially run private contextual whisper.cpp "
            "ASR work orders paired to sealed raw baselines"
        )
    )
    commands = parser.add_subparsers(dest="command", required=True)
    materialize = commands.add_parser("materialize")
    materialize.add_argument("--baseline-result", action="append", required=True)
    materialize.add_argument("--glossary", required=True)
    materialize.add_argument("--batch-root", required=True)
    materialize.add_argument("--asr-output-root", required=True)
    validate = commands.add_parser("validate")
    validate.add_argument("--manifest", required=True)
    run = commands.add_parser("run")
    run.add_argument("--manifest", required=True)
    run.add_argument("--dry-run", action="store_true")
    return parser


def _absolute_cli_path(value: str, label: str, *, existing: bool = False) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    path = path.resolve(strict=False)
    return existing_regular_file(path, label) if existing else path


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "materialize":
            manifest, path = materialize_batch(
                baseline_paths=[
                    _absolute_cli_path(value, "--baseline-result", existing=True)
                    for value in args.baseline_result
                ],
                glossary_path=_absolute_cli_path(args.glossary, "--glossary", existing=True),
                batch_root=_absolute_cli_path(args.batch_root, "--batch-root"),
                asr_output_root=_absolute_cli_path(
                    args.asr_output_root, "--asr-output-root"
                ),
            )
            result: Any = {
                "batch_id": manifest["batch_id"],
                "identity_sha256": manifest["identity_sha256"],
                "manifest_path": str(path),
                "work_order_count": manifest["work_order_count"],
                "totals": manifest["totals"],
                "safety": manifest["safety"],
            }
        elif args.command == "validate":
            manifest_path = _absolute_cli_path(args.manifest, "--manifest", existing=True)
            manifest, _ = validate_batch(manifest_path)
            result = {
                "schema_version": SCHEMA_VERSION,
                "status": "valid",
                "batch_id": manifest["batch_id"],
                "identity_sha256": manifest["identity_sha256"],
                "manifest_sha256": sha256_bytes(pretty_bytes(manifest)),
                "work_order_count": manifest["work_order_count"],
                "totals": manifest["totals"],
                "safety": manifest["safety"],
            }
        else:
            manifest_path = _absolute_cli_path(args.manifest, "--manifest", existing=True)
            result = run_batch(manifest_path, dry_run=args.dry_run)
        sys.stdout.buffer.write(pretty_bytes(result))
        return 0
    except ContextualBatchRunFailure as error:
        sys.stderr.buffer.write(pretty_bytes(error.result))
        return 2
    except (
        ContextualBatchError,
        asr_whispercpp.ASRError,
        OSError,
        subprocess.SubprocessError,
    ) as error:
        failure = {
            "schema_version": SCHEMA_VERSION,
            "status": "failed",
            "error": {"type": type(error).__name__, "message": str(error)},
        }
        sys.stderr.buffer.write(pretty_bytes(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
