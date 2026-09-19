#!/usr/bin/env python3
"""Sequentially validate or dispatch a sealed preprocess-audio ASR queue.

The runner accepts only ``sealed_preprocess_receipts`` queues.  It rejects a
catalog-admitted queue before the queue validator can open SQLite, dispatches in
sealed ordinal order, and treats ``review_near_silent_candidate`` as a review
route unless the caller explicitly opts in.

Validation and dry-run perform no catalog, publication, identity, or runner
state writes.  Real dispatch delegates one work order at a time to the frozen
offline adapter, which retains and revalidates the input, engine, and model
descriptors.  No concurrency or arbitrary subset selection exists here; the only
default route change is the explicit near-silent review policy.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import re
import stat
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from . import asr_whispercpp, preprocess_asr_queue_v03, preprocess_batch

    preprocess_asr_queue = preprocess_asr_queue_v03
except ImportError:  # pragma: no cover - direct script execution
    import asr_whispercpp  # type: ignore[no-redef]
    import preprocess_asr_queue_v03 as preprocess_asr_queue  # type: ignore[no-redef]
    import preprocess_batch  # type: ignore[no-redef]


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.3.0"
RUNNER_NAME = "himr-preprocess-asr-queue-runner"

MAX_MANIFEST_BYTES = preprocess_asr_queue.MAX_MANIFEST_BYTES
MAX_WORK_ORDER_BYTES = preprocess_asr_queue.MAX_WORK_ORDER_BYTES
MAX_RESULT_BYTES = asr_whispercpp.MAX_RAW_JSON_BYTES
MAX_ITEMS = preprocess_asr_queue.MAX_ITEMS

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
QUEUE_ID_RE = re.compile(r"^asrppqueue_[0-9a-f]{32}$")
JOB_ID_RE = re.compile(r"^asr-preprocess-[0-9a-f]{32}$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
RUN_ID_RE = re.compile(r"^run_asr_whispercpp_[0-9a-f]{32}$")
PROC_FD_RE = re.compile(r"^/proc/self/fd/[0-9]+$")

REVIEW_ROUTING_HINT = "review_near_silent_candidate"
PROCESS_ROUTING_HINT = "process"

SAFETY = {
    "catalog_access": "forbidden_sealed_receipt_queues_only",
    "catalog_writes": False,
    "credentials_used": False,
    "dispatch_order": "sealed_ordinal_sequential_fail_fast",
    "identity_writes": False,
    "maximum_concurrency": 1,
    "network_access": "forbidden_by_runner_and_adapter_contract",
    "near_silent_default": "route_to_review_without_adapter_invocation",
    "publication_authority": "none",
    "resume_policy": "strictly_validate_then_reuse_content_addressed_completed_results",
    "runner_state_writes": False,
}

PRIVATE_SAFETY = {
    "handling_policy_propagation_required": True,
    "private_acquisition_seal_replay_required": True,
}


class RunnerError(RuntimeError):
    """A queue, routing, result, or immutable-replay check failed."""


class RunnerFailure(RunnerError):
    """Sequential adapter dispatch failed after zero or more completed actions."""

    def __init__(self, result: dict[str, Any]):
        failed = result["failed_job"]
        super().__init__(
            f"preprocess ASR queue failed at ordinal {failed['ordinal']}/"
            f"{result['job_count']}: {failed['error']['message']}"
        )
        self.result = result


_STRICT_RESULT_VALIDATOR: Any = None
_STRICT_RESULT_VALIDATOR_ERROR: type[Exception] | None = None
_STRICT_RESULT_VALIDATOR_SOURCE: dict[str, Any] | None = None


def canonical_bytes(value: Any) -> bytes:
    return preprocess_asr_queue.canonical_bytes(value)


def pretty_bytes(value: Any) -> bytes:
    return preprocess_asr_queue.pretty_bytes(value)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise RunnerError(f"{label} must be a lowercase SHA-256")
    return value


def _text(value: Any, label: str, maximum: int = 16_384) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise RunnerError(f"{label} must be bounded non-empty text")
    return value


def _integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise RunnerError(f"{label} must be an integer from {minimum} to {maximum}")
    return value


def _identifier(value: Any, label: str) -> str:
    text = _text(value, label, 256)
    if not IDENTIFIER_RE.fullmatch(text):
        raise RunnerError(f"{label} is not a supported identifier")
    return text


def _exact_object(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RunnerError(f"{label} must be an object")
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    if missing or unknown:
        raise RunnerError(
            f"{label} keys differ from the exact contract; "
            f"missing={missing}, unknown={unknown}"
        )
    return value


def _stable_read(
    path: Path,
    *,
    maximum: int,
    label: str,
    required_mode: int | None = None,
) -> tuple[Path, bytes, os.stat_result]:
    try:
        return preprocess_batch.stable_read(
            path,
            maximum,
            label,
            required_mode=required_mode,
        )
    except preprocess_batch.BatchError as error:
        raise RunnerError(str(error)) from error


def _absolute_existing_file(value: Any, label: str) -> Path:
    try:
        return preprocess_batch.absolute_path(value, label, must_exist=True)
    except preprocess_batch.BatchError as error:
        raise RunnerError(str(error)) from error


def _normalized_manifest_argument(value: Any) -> Path:
    try:
        raw = os.fspath(value)
    except TypeError as error:
        raise RunnerError("queue manifest path must be path-like") from error
    if not isinstance(raw, str) or not raw or "://" in raw or "\x00" in raw:
        raise RunnerError("queue manifest path must be a bounded local path")
    return Path(os.path.abspath(raw))


def _guard_sealed_manifest(manifest_path: Path) -> tuple[Path, bytes, dict[str, Any]]:
    path = _absolute_existing_file(
        _normalized_manifest_argument(manifest_path),
        "preprocess ASR queue manifest",
    )
    resolved, body, _ = _stable_read(
        path,
        maximum=MAX_MANIFEST_BYTES,
        label="preprocess ASR queue manifest",
        required_mode=0o400,
    )
    raw = preprocess_asr_queue.parse_json(body, "preprocess ASR queue manifest")
    if not isinstance(raw, dict):
        raise RunnerError("preprocess ASR queue manifest must be an object")
    origin = raw.get("origin")
    if not isinstance(origin, dict) or origin.get("mode") != "sealed_preprocess_receipts":
        raise RunnerError(
            "runner accepts sealed preprocess receipts only; catalog admission is "
            "rejected before queue validation"
        )
    if raw.get("materializer") != preprocess_asr_queue.MATERIALIZER_NAME:
        raise RunnerError("queue manifest materializer is unsupported")
    safety = raw.get("safety")
    if (
        not isinstance(safety, dict)
        or safety.get("catalog_writes") is not False
        or safety.get("publication_authority") != "none"
    ):
        raise RunnerError("queue manifest does not retain the no-catalog-write policy")
    if raw.get("handling_control") is not None and (
        safety.get("private_acquisition_seal_required") is not True
        or safety.get("handling_policy_propagation_required") is not True
    ):
        raise RunnerError("private queue manifest drops its handling safety policy")
    return resolved, body, raw


def _load_sealed_queue(
    manifest_path: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], bytes, Path]:
    path, guarded_body, guarded = _guard_sealed_manifest(manifest_path)
    try:
        manifest, orders = preprocess_asr_queue.validate_queue(path)
    except (
        preprocess_asr_queue.QueueError,
        preprocess_batch.BatchError,
        asr_whispercpp.ASRError,
        OSError,
    ) as error:
        raise RunnerError(f"sealed queue validation failed: {error}") from error
    if (
        manifest.get("origin", {}).get("mode") != "sealed_preprocess_receipts"
        or manifest != guarded
        or pretty_bytes(manifest) != guarded_body
        or not isinstance(orders, list)
        or len(orders) != manifest.get("work_order_count")
        or not 1 <= len(orders) <= MAX_ITEMS
    ):
        raise RunnerError("sealed queue changed across guarded deterministic validation")
    return manifest, orders, guarded_body, path


def _stable_source_document(
    path: Path,
    *,
    label: str,
    expected_sha256: str | None = None,
    expected_byte_count: int | None = None,
) -> dict[str, Any]:
    resolved, body, _ = _stable_read(
        path,
        maximum=4 * 1024 * 1024,
        label=label,
    )
    digest = sha256_bytes(body)
    if expected_sha256 is not None and digest != expected_sha256:
        raise RunnerError(f"{label} SHA-256 changed after queue admission")
    if expected_byte_count is not None and len(body) != expected_byte_count:
        raise RunnerError(f"{label} byte count changed after queue admission")
    return {
        "path": str(resolved),
        "sha256": digest,
        "byte_count": len(body),
    }


def _runner_document() -> dict[str, Any]:
    _validator, _error_type, validator_source = _load_strict_result_validator()
    return {
        "name": RUNNER_NAME,
        "implementation_version": IMPLEMENTATION_VERSION,
        "completed_result_validator": validator_source,
        **_stable_source_document(Path(__file__).resolve(), label="queue runner source"),
    }


def _load_strict_result_validator() -> tuple[Any, type[Exception], dict[str, Any]]:
    """Load the catalog-free importer validator without opening a database."""

    global _STRICT_RESULT_VALIDATOR
    global _STRICT_RESULT_VALIDATOR_ERROR
    global _STRICT_RESULT_VALIDATOR_SOURCE

    repository_root = Path(__file__).resolve().parent.parent
    corpus_source = repository_root / "corpus" / "src"
    module_source = corpus_source / "himr_corpus" / "asr_result_importer.py"
    source = {
        "name": "himr-corpus-asr-result-importer",
        **_stable_source_document(
            module_source,
            label="strict completed-result validator source",
        ),
    }
    if _STRICT_RESULT_VALIDATOR is None:
        inserted = str(corpus_source) not in sys.path
        if inserted:
            sys.path.insert(0, str(corpus_source))
        try:
            module = importlib.import_module("himr_corpus.asr_result_importer")
            errors = importlib.import_module("himr_corpus.result_importers")
        except (ImportError, OSError) as error:
            raise RunnerError(
                "strict completed-result validator could not be loaded"
            ) from error
        finally:
            if inserted:
                try:
                    sys.path.remove(str(corpus_source))
                except ValueError:  # pragma: no cover - defensive only
                    pass
        validator = getattr(module, "validate_asr_whispercpp_result_file", None)
        error_type = getattr(errors, "ResultImportError", None)
        if not callable(validator) or not isinstance(error_type, type):
            raise RunnerError("strict completed-result validator API is unavailable")
        replayed = {
            "name": "himr-corpus-asr-result-importer",
            **_stable_source_document(
                module_source,
                label="strict completed-result validator source replay",
            ),
        }
        if replayed != source:
            raise RunnerError("strict completed-result validator changed while loading")
        _STRICT_RESULT_VALIDATOR = validator
        _STRICT_RESULT_VALIDATOR_ERROR = error_type
        _STRICT_RESULT_VALIDATOR_SOURCE = source
    elif source != _STRICT_RESULT_VALIDATOR_SOURCE:
        raise RunnerError("strict completed-result validator changed during dispatch")
    assert _STRICT_RESULT_VALIDATOR_ERROR is not None
    assert _STRICT_RESULT_VALIDATOR_SOURCE is not None
    return (
        _STRICT_RESULT_VALIDATOR,
        _STRICT_RESULT_VALIDATOR_ERROR,
        dict(_STRICT_RESULT_VALIDATOR_SOURCE),
    )


def _revalidate_queue_software(manifest: dict[str, Any]) -> None:
    software = manifest.get("software")
    if not isinstance(software, dict):
        raise RunnerError("queue software block is malformed")
    expected_modules = {
        "materializer": Path(preprocess_asr_queue.__file__).resolve(),
        "asr_adapter": Path(asr_whispercpp.__file__).resolve(),
        "engine_profiles": Path(
            preprocess_asr_queue.whispercpp_engine_profiles.__file__
        ).resolve(),
    }
    for key, expected_path in expected_modules.items():
        component = software.get(key)
        if not isinstance(component, dict) or component.get("path") != str(expected_path):
            raise RunnerError(f"queue {key} source path differs from the loaded module")
        _stable_source_document(
            expected_path,
            label=f"queue {key} source",
            expected_sha256=_sha256(component.get("sha256"), f"queue {key} SHA-256"),
            expected_byte_count=_integer(
                component.get("byte_count"),
                f"queue {key} byte count",
                1,
                4 * 1024 * 1024,
            ),
        )


def _stable_dispatch_order(
    manifest_path: Path,
    manifest: dict[str, Any],
    initial_document: dict[str, Any],
    ordinal: int,
) -> dict[str, Any]:
    entry = manifest["work_orders"][ordinal - 1]
    expected_relative = f"work-orders/{ordinal:06d}.json"
    if entry.get("ordinal") != ordinal or entry.get("path") != expected_relative:
        raise RunnerError(f"queue work-order entry {ordinal} is not canonical")
    path = manifest_path.parent / expected_relative
    resolved, body, _ = _stable_read(
        path,
        maximum=MAX_WORK_ORDER_BYTES,
        label=f"dispatch work order {ordinal}",
        required_mode=0o400,
    )
    if (
        resolved != path
        or sha256_bytes(body) != entry.get("sha256")
        or len(body) != entry.get("byte_count")
    ):
        raise RunnerError(f"sealed work order {ordinal} changed before dispatch")
    raw = preprocess_asr_queue.parse_json(body, f"dispatch work order {ordinal}")
    try:
        order, document = preprocess_asr_queue.validate_queue_work_order(
            raw,
            entry=entry,
            manifest=manifest,
        )
    except (preprocess_asr_queue.QueueError, asr_whispercpp.ASRError) as error:
        raise RunnerError(f"sealed work order {ordinal} is invalid: {error}") from error
    if (
        document != initial_document
        or body != pretty_bytes(document)
        or order.get("job_id") != entry.get("job_id")
        or sha256_bytes(canonical_bytes(document)) != entry.get("canonical_sha256")
        or sha256_bytes(canonical_bytes(order))
        != entry.get("adapter_work_order_sha256", entry.get("canonical_sha256"))
    ):
        raise RunnerError(f"sealed work order {ordinal} differs from queue admission")
    return order


def _expected_adapter_identity(order: dict[str, Any]) -> dict[str, Any]:
    if order.get("glossary") is not None or order.get("catalog_context") is not None:
        raise RunnerError("preprocess queue work orders require null glossary/catalog context")
    window = {
        "offset_ms": 0,
        "duration_ms": order["window"]["duration_ms"],
        "end_ms": order["window"]["duration_ms"],
    }
    if order["window"] != {
        "offset_ms": 0,
        "duration_ms": order["window"]["duration_ms"],
    }:
        raise RunnerError("preprocess queue work order is not a full artifact-local window")
    recipe = {
        "contract_version": asr_whispercpp.CONTRACT_VERSION,
        "implementation_version": asr_whispercpp.IMPLEMENTATION_VERSION,
        "stage": asr_whispercpp.STAGE,
        "descriptor_execution_policy": asr_whispercpp.DESCRIPTOR_EXECUTION_POLICY,
        "engine": {
            "sha256": order["engine"]["expected_sha256"],
            "version": order["engine"]["version_label"],
            "version_evidence": order["engine"]["version_evidence"],
            "build": order["engine"]["build"],
        },
        "model": {
            key: order["model"][key]
            for key in ("model_id", "name", "revision", "source", "license_label")
        }
        | {"sha256": order["model"]["expected_sha256"]},
        "window": window,
        "inference": order["inference"],
        "glossary": None,
        "output_contract": "whisper.cpp-output-json-full-normalized-v1",
    }
    recipe_sha256 = sha256_bytes(asr_whispercpp.canonical_bytes(recipe))
    recipe_id = f"recipe_asr_whispercpp_{recipe_sha256[:32]}"
    work_order_sha256 = sha256_bytes(asr_whispercpp.canonical_bytes(order))
    result_identity = {
        "work_order_sha256": work_order_sha256,
        "input_sha256": order["input"]["expected_sha256"],
        "input_media_id": order["input"]["media_id"],
        "input_artifact_id": order["input"]["artifact_id"],
        "parent_processing_run_id": order["input"]["parent_processing_run_id"],
        "recipe_id": recipe_id,
        "catalog_context": None,
    }
    result_key = sha256_bytes(asr_whispercpp.canonical_bytes(result_identity))
    output_root = Path(order["output"]["root"])
    input_sha256 = order["input"]["expected_sha256"]
    run_dir = (
        output_root
        / "asr"
        / "whispercpp"
        / "sha256"
        / input_sha256[:2]
        / input_sha256
        / "results"
        / result_key
    )
    return {
        "work_order_sha256": work_order_sha256,
        "recipe": recipe,
        "recipe_sha256": recipe_sha256,
        "recipe_id": recipe_id,
        "result_key": result_key,
        "run_dir": run_dir,
        "result_path": run_dir / "result.json",
        "window": window,
    }


def _safe_result_directory(path: Path, label: str) -> set[str]:
    try:
        observed = path.lstat()
    except OSError as error:
        raise RunnerError(f"{label} cannot be inspected") from error
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        raise RunnerError(f"{label} must be an owner-private non-symlink directory")
    try:
        entries = list(path.iterdir())
    except OSError as error:
        raise RunnerError(f"{label} cannot be enumerated") from error
    if any(entry.is_symlink() for entry in entries):
        raise RunnerError(f"{label} contains a symlink")
    return {entry.name for entry in entries}


def _stable_result_document(path: Path) -> tuple[dict[str, Any], bytes]:
    resolved, body, observed = _stable_read(
        path,
        maximum=MAX_RESULT_BYTES,
        label="completed ASR result",
    )
    if resolved != path or stat.S_IMODE(observed.st_mode) & 0o022:
        raise RunnerError("completed ASR result is not an owner-controlled regular file")
    raw = preprocess_asr_queue.parse_json(body, "completed ASR result")
    if not isinstance(raw, dict):
        raise RunnerError("completed ASR result must be an object")
    if body != (asr_whispercpp.pretty_json(raw)).encode("utf-8"):
        raise RunnerError("completed ASR result is not in canonical adapter serialization")
    return raw, body


def _validate_completed_result_tree(
    identity: dict[str, Any],
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    run_dir = identity["run_dir"]
    result_path = identity["result_path"]
    expected_names = {
        "result.json",
        "transcript.normalized.json",
        "whisper.raw.json",
    }
    if _safe_result_directory(run_dir, "completed ASR run directory") != expected_names:
        raise RunnerError("completed ASR run directory has missing or extra entries")
    raw, before = _stable_result_document(result_path)
    try:
        validated = asr_whispercpp.validate_completed_reuse(
            result_path,
            result_key=identity["result_key"],
            recipe_id=identity["recipe_id"],
            run_dir=run_dir,
        )
    except (asr_whispercpp.ASRError, OSError) as error:
        raise RunnerError(f"completed ASR result failed immutable reuse: {error}") from error
    validator, validator_error, _source = _load_strict_result_validator()
    try:
        strict_summary = validator(result_path)
    except (validator_error, OSError) as error:
        raise RunnerError(
            f"completed ASR result failed catalog-free strict validation: {error}"
        ) from error
    after_raw, after = _stable_result_document(result_path)
    if (
        raw != validated
        or validated != after_raw
        or before != after
        or _safe_result_directory(run_dir, "completed ASR run directory replay")
        != expected_names
    ):
        raise RunnerError("completed ASR result changed during immutable reuse validation")
    return validated, sha256_bytes(before), strict_summary


def _preexisting_result(
    identity: dict[str, Any],
) -> tuple[dict[str, Any], str, dict[str, Any]] | None:
    result_path = identity["result_path"]
    run_dir = identity["run_dir"]
    try:
        result_lstat = result_path.lstat()
    except FileNotFoundError:
        result_lstat = None
    except OSError as error:
        raise RunnerError("expected ASR result path cannot be inspected") from error
    if result_lstat is not None:
        if stat.S_ISLNK(result_lstat.st_mode) or not stat.S_ISREG(result_lstat.st_mode):
            raise RunnerError("expected ASR result path must be a regular non-symlink file")
        return _validate_completed_result_tree(identity)
    if run_dir.exists() or run_dir.is_symlink():
        raise RunnerError("immutable ASR result directory exists without result.json")
    return None


def _strict_preexisting_result(
    existing: tuple[dict[str, Any], str, dict[str, Any]],
    *,
    order: dict[str, Any],
    entry: dict[str, Any],
    manifest: dict[str, Any],
) -> dict[str, Any]:
    validated = _validate_adapter_result(
        existing[0],
        order=order,
        entry=entry,
        manifest=manifest,
        dry_run=False,
    )
    if validated["result_sha256"] != existing[1]:
        raise RunnerError("completed ASR result changed across strict reuse checks")
    return validated


def _strict_json_text(value: Any, label: str) -> Any:
    text = _text(value, label, MAX_RESULT_BYTES)
    parsed = preprocess_asr_queue.parse_json(text.encode("utf-8"), label)
    if text != asr_whispercpp.canonical_json_text(parsed):
        raise RunnerError(f"{label} must use canonical JSON serialization")
    return parsed


def _validate_stat(value: Any, label: str, byte_count: int) -> dict[str, int]:
    row = _exact_object(value, label, {"device", "inode", "byte_count", "mtime_ns"})
    for key in ("device", "inode", "byte_count", "mtime_ns"):
        if isinstance(row[key], bool) or not isinstance(row[key], int):
            raise RunnerError(f"{label}.{key} must be an integer")
    if row["inode"] < 0 or row["byte_count"] != byte_count:
        raise RunnerError(f"{label} is inconsistent with the observed byte count")
    return row


def _validate_observation(
    row: Any,
    *,
    label: str,
    expected_path: str,
    expected_sha256: str,
    expected_byte_count: int,
) -> dict[str, Any]:
    observed = _exact_object(
        row,
        label,
        {"path", "sha256", "byte_count", "stat_before", "stat_after", "unchanged"},
    )
    if (
        observed["path"] != expected_path
        or observed["sha256"] != expected_sha256
        or observed["byte_count"] != expected_byte_count
        or observed["unchanged"] is not True
        or _validate_stat(
            observed["stat_before"], f"{label}.stat_before", expected_byte_count
        )
        != _validate_stat(
            observed["stat_after"], f"{label}.stat_after", expected_byte_count
        )
    ):
        raise RunnerError(f"{label} differs from its retained identity")
    return observed


def _validate_timestamp_order(processing_run: dict[str, Any]) -> None:
    parsed: list[datetime] = []
    for key in ("started_at", "completed_at"):
        value = _text(processing_run[key], f"ASR processing_run.{key}", 64)
        if not value.endswith("Z"):
            raise RunnerError(f"ASR processing_run.{key} must use canonical UTC")
        try:
            timestamp = datetime.fromisoformat(value[:-1] + "+00:00")
        except ValueError as error:
            raise RunnerError(f"ASR processing_run.{key} is invalid") from error
        canonical = timestamp.isoformat(
            timespec="microseconds" if timestamp.microsecond else "seconds"
        ).replace("+00:00", "Z")
        if value != canonical:
            raise RunnerError(f"ASR processing_run.{key} must use canonical UTC")
        parsed.append(timestamp)
    if parsed[1] < parsed[0]:
        raise RunnerError("ASR processing_run.completed_at precedes started_at")


def _validate_descriptor_commands(
    envelope: dict[str, Any],
    *,
    order: dict[str, Any],
    identity: dict[str, Any],
    processing_run_id: str,
    dry_run: bool,
) -> list[list[str]]:
    probe = envelope["input"]["probe"]
    logical_probe = [
        probe["ffprobe_path"],
        "-v",
        "error",
        "-select_streams",
        "a:0",
        "-show_entries",
        (
            "format=duration,format_name:stream=index,codec_name,sample_fmt,"
            "sample_rate,channels,channel_layout,duration"
        ),
        "-of",
        "json",
        order["input"]["path"],
    ]
    logical_whisper = asr_whispercpp.build_command(
        order,
        identity["run_dir"] / "whisper-output",
        identity["window"],
        None,
    )
    commands = envelope["commands"]
    if (
        not isinstance(commands, list)
        or len(commands) != 2
        or any(
            not isinstance(command, list)
            or not command
            or len(command) > 256
            or any(
                not isinstance(argument, str)
                or not argument
                or len(argument) > 32_768
                or "\x00" in argument
                for argument in command
            )
            for command in commands
        )
    ):
        raise RunnerError("ASR adapter commands violate their bounded argv contract")
    executed_probe, executed_whisper = commands
    if (
        len(executed_probe) != len(logical_probe)
        or executed_probe[:-1] != logical_probe[:-1]
        or not PROC_FD_RE.fullmatch(executed_probe[-1])
        or len(executed_whisper) != len(logical_whisper)
    ):
        raise RunnerError("ASR adapter probe/whisper command shape is inconsistent")
    model_index = logical_whisper.index("--model") + 1
    input_index = logical_whisper.index("--file") + 1
    output_index = logical_whisper.index("--output-file") + 1
    descriptor_indices = {0, model_index, input_index}
    for index, (logical, executed) in enumerate(
        zip(logical_whisper, executed_whisper, strict=True)
    ):
        if index in descriptor_indices:
            if not PROC_FD_RE.fullmatch(executed):
                raise RunnerError("ASR adapter inference command is not descriptor-backed")
        elif index == output_index and not dry_run:
            staged = (
                identity["run_dir"].parent
                / f".{identity['result_key']}.tmp-{processing_run_id}"
                / "whisper-output"
            )
            if executed != str(staged):
                raise RunnerError("ASR adapter inference staging path is inconsistent")
        elif executed != logical:
            raise RunnerError("ASR adapter child argv differs from logical provenance")
    descriptors = [executed_whisper[index] for index in descriptor_indices]
    if len(set(descriptors)) != 3 or executed_probe[-1] != executed_whisper[input_index]:
        raise RunnerError("ASR adapter descriptor bindings are inconsistent")
    return [logical_probe, logical_whisper]


def _validate_adapter_result(
    result: Any,
    *,
    order: dict[str, Any],
    entry: dict[str, Any],
    manifest: dict[str, Any],
    dry_run: bool,
) -> dict[str, Any]:
    identity = _expected_adapter_identity(order)
    common_keys = {
        "schema_version",
        "job_id",
        "status",
        "dry_run",
        "work_order_sha256",
        "recipe_id",
        "recipe_sha256",
        "result_key",
        "processing_run",
        "input",
        "engine",
        "model",
        "glossary",
        "catalog_context",
        "window",
        "commands",
        "artifacts",
        "transcript",
        "catalog_records",
        "result_path",
        "duration_ms",
        "errors",
    }
    expected_keys = common_keys if dry_run else common_keys | {"run_input"}
    envelope = _exact_object(result, "ASR adapter result", expected_keys)
    expected_status = "planned" if dry_run else "completed"
    if (
        envelope["schema_version"] != 1
        or envelope["job_id"] != order["job_id"]
        or envelope["status"] != expected_status
        or envelope["dry_run"] is not dry_run
        or envelope["work_order_sha256"] != identity["work_order_sha256"]
        or envelope["recipe_id"] != identity["recipe_id"]
        or envelope["recipe_sha256"] != identity["recipe_sha256"]
        or envelope["result_key"] != identity["result_key"]
        or envelope["result_path"] != str(identity["result_path"])
        or envelope["window"] != identity["window"]
        or envelope["glossary"] is not None
        or envelope["catalog_context"] is not None
        or envelope["errors"] != []
        or isinstance(envelope["duration_ms"], bool)
        or not isinstance(envelope["duration_ms"], int)
        or envelope["duration_ms"] < 0
    ):
        raise RunnerError("ASR adapter result identity/envelope is inconsistent")

    processing_run = _exact_object(
        envelope["processing_run"],
        "ASR adapter processing_run",
        {
            "processing_run_id",
            "stage",
            "implementation_version",
            "model_id",
            "glossary_revision_id",
            "parameters_json",
            "environment_json",
            "random_seed",
            "started_at",
            "completed_at",
            "status",
            "error_text",
        },
    )
    processing_run_id = _identifier(
        processing_run["processing_run_id"], "ASR processing_run_id"
    )
    if not RUN_ID_RE.fullmatch(processing_run_id):
        raise RunnerError("ASR processing_run_id does not use the producer identity form")
    expected_run_status = "queued" if dry_run else "completed"
    if (
        processing_run["stage"] != asr_whispercpp.STAGE
        or processing_run["implementation_version"]
        != asr_whispercpp.IMPLEMENTATION_VERSION
        or processing_run["model_id"] != order["model"]["model_id"]
        or processing_run["glossary_revision_id"] is not None
        or processing_run["parameters_json"]
        != asr_whispercpp.canonical_json_text(identity["recipe"])
        or processing_run["random_seed"] is not None
        or processing_run["status"] != expected_run_status
        or processing_run["error_text"] is not None
    ):
        raise RunnerError("ASR adapter processing run differs from the work order")
    _validate_timestamp_order(processing_run)
    environment = _strict_json_text(
        processing_run["environment_json"], "ASR processing environment"
    )
    if not isinstance(environment, dict):
        raise RunnerError("ASR processing environment must be an object")
    provenance = environment.get("command_provenance")
    expected_states = ["executed", "planned"] if dry_run else ["executed", "executed"]
    if (
        environment.get("cpu_only") is not True
        or environment.get("network") != "not_used"
        or environment.get("engine_version") != order["engine"]["version_label"]
        or environment.get("engine_version_evidence")
        != order["engine"]["version_evidence"]
        or not isinstance(provenance, dict)
        or provenance.get("descriptor_execution_policy")
        != asr_whispercpp.DESCRIPTOR_EXECUTION_POLICY
        or provenance.get("result_command_states") != expected_states
    ):
        raise RunnerError("ASR adapter environment violates the offline descriptor policy")

    audio = entry["audio_artifact"]
    input_row = _exact_object(
        envelope["input"],
        "ASR adapter input",
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
    _validate_observation(
        {key: input_row[key] for key in (
            "path", "sha256", "byte_count", "stat_before", "stat_after", "unchanged"
        )},
        label="ASR adapter input observation",
        expected_path=order["input"]["path"],
        expected_sha256=order["input"]["expected_sha256"],
        expected_byte_count=audio["byte_count"],
    )
    if any(
        input_row[key] != order["input"][key]
        for key in ("media_id", "artifact_id", "parent_processing_run_id")
    ) or not isinstance(input_row["probe"], dict):
        raise RunnerError("ASR adapter input lineage/probe is inconsistent")
    probe = _exact_object(
        input_row["probe"],
        "ASR adapter input probe",
        {
            "ffprobe_path",
            "codec_name",
            "sample_format",
            "sample_rate_hz",
            "channels",
            "channel_layout",
            "format_name",
            "duration_ms",
        },
    )
    if (
        not isinstance(probe["ffprobe_path"], str)
        or not Path(probe["ffprobe_path"]).is_absolute()
        or probe["codec_name"] != "flac"
        or probe["sample_format"] != "s16"
        or probe["sample_rate_hz"] != 16_000
        or probe["channels"] != 1
        or probe["duration_ms"] != audio["duration_ms"]
        or probe["channel_layout"] is not None
        and not isinstance(probe["channel_layout"], str)
        or probe["format_name"] is not None
        and not isinstance(probe["format_name"], str)
    ):
        raise RunnerError("ASR adapter probe duration differs from the queued artifact")

    engine_row = _exact_object(
        envelope["engine"],
        "ASR adapter engine",
        {
            "path",
            "sha256",
            "byte_count",
            "stat_before",
            "stat_after",
            "unchanged",
            "version",
            "version_evidence",
            "build",
        },
    )
    _validate_observation(
        {key: engine_row[key] for key in (
            "path", "sha256", "byte_count", "stat_before", "stat_after", "unchanged"
        )},
        label="ASR adapter engine observation",
        expected_path=order["engine"]["executable"],
        expected_sha256=order["engine"]["expected_sha256"],
        expected_byte_count=manifest["engine"]["byte_count"],
    )
    if (
        engine_row["version"] != order["engine"]["version_label"]
        or engine_row["version_evidence"] != order["engine"]["version_evidence"]
        or engine_row["build"] != order["engine"]["build"]
    ):
        raise RunnerError("ASR adapter engine provenance is inconsistent")

    model_row = _exact_object(
        envelope["model"],
        "ASR adapter model",
        {
            "path",
            "sha256",
            "byte_count",
            "stat_before",
            "stat_after",
            "unchanged",
            "model_id",
            "name",
            "revision",
            "source",
            "license_label",
        },
    )
    _validate_observation(
        {key: model_row[key] for key in (
            "path", "sha256", "byte_count", "stat_before", "stat_after", "unchanged"
        )},
        label="ASR adapter model observation",
        expected_path=order["model"]["path"],
        expected_sha256=order["model"]["expected_sha256"],
        expected_byte_count=manifest["model"]["byte_count"],
    )
    if any(
        model_row[key] != order["model"][key]
        for key in ("model_id", "name", "revision", "source", "license_label")
    ):
        raise RunnerError("ASR adapter model provenance is inconsistent")
    logical_commands = _validate_descriptor_commands(
        envelope,
        order=order,
        identity=identity,
        processing_run_id=processing_run_id,
        dry_run=dry_run,
    )
    expected_environment = asr_whispercpp.execution_environment(
        logical_commands,
        command_states=["executed", "planned"]
        if dry_run
        else ["executed", "executed"],
        version=order["engine"]["version_label"],
        version_evidence=order["engine"]["version_evidence"],
    )
    if environment != expected_environment:
        raise RunnerError("ASR adapter environment differs from logical command provenance")

    result_sha256: str | None = None
    if dry_run:
        if (
            envelope["artifacts"] != []
            or envelope["transcript"] is not None
            or envelope["catalog_records"] is not None
            or identity["result_path"].exists()
            or identity["result_path"].is_symlink()
        ):
            raise RunnerError("adapter dry-run wrote or claimed completed ASR output")
    else:
        run_input = _exact_object(
            envelope["run_input"],
            "ASR adapter run_input",
            {
                "run_input_id",
                "processing_run_id",
                "object_type",
                "object_id",
                "input_role",
                "input_sha256",
            },
        )
        if (
            run_input["processing_run_id"] != processing_run_id
            or run_input["object_type"] != "media"
            or run_input["object_id"] != order["input"]["media_id"]
            or run_input["input_role"] != "normalized_audio"
            or run_input["input_sha256"] != order["input"]["expected_sha256"]
            or not isinstance(envelope["artifacts"], list)
            or len(envelope["artifacts"]) != 2
            or not isinstance(envelope["transcript"], dict)
            or not isinstance(envelope["catalog_records"], dict)
        ):
            raise RunnerError("completed ASR lineage/artifacts are inconsistent")
        persisted, result_sha256, strict_summary = _validate_completed_result_tree(identity)
        if persisted != envelope:
            raise RunnerError("adapter return differs from the immutable completed result")
        if (
            strict_summary.get("result_envelope_sha256")
            != sha256_bytes(asr_whispercpp.canonical_bytes(envelope))
            or strict_summary.get("job_id") != order["job_id"]
            or strict_summary.get("processing_run_id") != processing_run_id
            or strict_summary.get("recipe_id") != identity["recipe_id"]
            or strict_summary.get("result_key") != identity["result_key"]
            or strict_summary.get("model_id") != order["model"]["model_id"]
            or strict_summary.get("glossary_revision_id") is not None
            or strict_summary.get("recording_id") is not None
            or strict_summary.get("rendition_id") is not None
            or strict_summary.get("artifact_count") != 2
        ):
            raise RunnerError("strict completed-result summary differs from queue identity")

    return {
        "recipe_id": identity["recipe_id"],
        "result_key": identity["result_key"],
        "result_path": str(identity["result_path"]),
        "processing_run_id": None if dry_run else processing_run_id,
        "result_sha256": result_sha256,
    }


def _base_action(
    *,
    entry: dict[str, Any],
    action: str,
    adapter_invoked: bool,
    identity: dict[str, Any] | None,
    processing_run_id: str | None = None,
    result_sha256: str | None = None,
) -> dict[str, Any]:
    return {
        "ordinal": entry["ordinal"],
        "job_id": entry["job_id"],
        "routing_hint": entry["routing_hint"],
        "action": action,
        "adapter_invoked": adapter_invoked,
        "work_order_sha256": entry.get(
            "adapter_work_order_sha256", entry["canonical_sha256"]
        ),
        "recipe_id": None if identity is None else identity["recipe_id"],
        "result_key": None if identity is None else identity["result_key"],
        "result_path": None if identity is None else str(identity["result_path"]),
        "processing_run_id": processing_run_id,
        "result_sha256": result_sha256,
        **({"handling": entry["handling"]} if "handling" in entry else {}),
    }


def _runner_safety(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        **SAFETY,
        **(
            PRIVATE_SAFETY
            if manifest.get("handling_control") is not None
            else {}
        ),
    }


def _selected(entry: dict[str, Any], *, include_near_silent: bool) -> bool:
    hint = entry.get("routing_hint")
    if hint == PROCESS_ROUTING_HINT:
        return True
    if hint == REVIEW_ROUTING_HINT:
        return include_near_silent
    raise RunnerError(
        f"work order {entry.get('ordinal')} has unsupported ASR routing hint: {hint!r}"
    )


def _summary(
    *,
    manifest: dict[str, Any],
    manifest_path: Path,
    manifest_body: bytes,
    mode: str,
    status: str,
    dry_run: bool | None,
    include_near_silent: bool,
    results: list[dict[str, Any]],
    runner: dict[str, Any],
    adapter_invocation_count: int,
    failed_job: dict[str, Any] | None = None,
) -> dict[str, Any]:
    selected_count = sum(
        _selected(entry, include_near_silent=include_near_silent)
        for entry in manifest["work_orders"]
    )
    review_count = len(manifest["work_orders"]) - selected_count
    value = {
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "runner_name": RUNNER_NAME,
        "mode": mode,
        "queue_id": manifest["queue_id"],
        "manifest_path": str(manifest_path),
        "manifest_sha256": sha256_bytes(manifest_body),
        "status": status,
        "dry_run": dry_run,
        "include_near_silent": include_near_silent,
        "job_count": len(manifest["work_orders"]),
        "selected_count": selected_count,
        "review_count": review_count,
        "adapter_invocation_count": adapter_invocation_count,
        "reused_count": sum(
            row["action"] in {"validated_reuse", "would_reuse", "reused"}
            for row in results
        ),
        "results": results,
        "runner": runner,
        **(
            {"handling_control": manifest["handling_control"]}
            if "handling_control" in manifest
            else {}
        ),
        "safety": _runner_safety(manifest),
    }
    if failed_job is not None:
        value["failed_job"] = failed_job
    return value


def _replay_dispatch_boundary(
    *,
    manifest: dict[str, Any],
    manifest_body: bytes,
    manifest_path: Path,
    runner: dict[str, Any],
) -> None:
    replayed, replay_orders, replay_body, replay_path = _load_sealed_queue(
        manifest_path
    )
    _revalidate_queue_software(replayed)
    if (
        replayed != manifest
        or len(replay_orders) != len(manifest["work_orders"])
        or replay_body != manifest_body
        or replay_path != manifest_path
        or _runner_document() != runner
    ):
        raise RunnerError("queue or runner changed during sequential dispatch")


def validate_dispatcher(
    manifest_path: Path,
    *,
    include_near_silent: bool = False,
) -> dict[str, Any]:
    runner_before = _runner_document()
    manifest, documents, manifest_body, resolved = _load_sealed_queue(manifest_path)
    _revalidate_queue_software(manifest)
    results: list[dict[str, Any]] = []
    for ordinal, initial_document in enumerate(documents, 1):
        order = _stable_dispatch_order(
            resolved, manifest, initial_document, ordinal
        )
        entry = manifest["work_orders"][ordinal - 1]
        identity = _expected_adapter_identity(order)
        if not _selected(entry, include_near_silent=include_near_silent):
            results.append(
                _base_action(
                    entry=entry,
                    action="review_required",
                    adapter_invoked=False,
                    identity=identity,
                )
            )
            continue
        existing = _preexisting_result(identity)
        strict_existing = (
            None
            if existing is None
            else _strict_preexisting_result(
                existing,
                order=order,
                entry=entry,
                manifest=manifest,
            )
        )
        results.append(
            _base_action(
                entry=entry,
                action="validated_reuse" if existing is not None else "validated_for_dispatch",
                adapter_invoked=False,
                identity=identity,
                processing_run_id=None
                if strict_existing is None
                else strict_existing["processing_run_id"],
                result_sha256=None
                if strict_existing is None
                else strict_existing["result_sha256"],
            )
        )
    _replay_dispatch_boundary(
        manifest=manifest,
        manifest_body=manifest_body,
        manifest_path=resolved,
        runner=runner_before,
    )
    return _summary(
        manifest=manifest,
        manifest_path=resolved,
        manifest_body=manifest_body,
        mode="validate",
        status="validated",
        dry_run=None,
        include_near_silent=include_near_silent,
        results=results,
        runner=runner_before,
        adapter_invocation_count=0,
    )


def _ensure_real_output_root(path: Path) -> None:
    try:
        preprocess_batch.ensure_private_directory(path, "ASR output root")
    except preprocess_batch.BatchError as error:
        raise RunnerError(str(error)) from error


def run_queue(
    manifest_path: Path,
    *,
    dry_run: bool,
    include_near_silent: bool = False,
) -> dict[str, Any]:
    runner_before = _runner_document()
    manifest, documents, manifest_body, resolved = _load_sealed_queue(manifest_path)
    _revalidate_queue_software(manifest)
    if not dry_run:
        _ensure_real_output_root(Path(manifest["output"]["asr_output_root"]))

    results: list[dict[str, Any]] = []
    adapter_invocation_count = 0
    mode = "dry_run" if dry_run else "run"
    for ordinal, initial_document in enumerate(documents, 1):
        entry = manifest["work_orders"][ordinal - 1]
        order: dict[str, Any] | None = None
        identity: dict[str, Any] | None = None
        try:
            order = _stable_dispatch_order(
                resolved, manifest, initial_document, ordinal
            )
            identity = _expected_adapter_identity(order)
            if not _selected(entry, include_near_silent=include_near_silent):
                results.append(
                    _base_action(
                        entry=entry,
                        action="review_required",
                        adapter_invoked=False,
                        identity=identity,
                    )
                )
                continue
            existing = _preexisting_result(identity)
            strict_existing = (
                None
                if existing is None
                else _strict_preexisting_result(
                    existing,
                    order=order,
                    entry=entry,
                    manifest=manifest,
                )
            )
            if dry_run and existing is not None:
                results.append(
                    _base_action(
                        entry=entry,
                        action="would_reuse",
                        adapter_invoked=False,
                        identity=identity,
                        processing_run_id=strict_existing["processing_run_id"],
                        result_sha256=strict_existing["result_sha256"],
                    )
                )
                continue
            _revalidate_queue_software(manifest)
            adapter_invocation_count += 1
            adapter_result = asr_whispercpp.run_asr(order, dry_run=dry_run)
            validated = _validate_adapter_result(
                adapter_result,
                order=order,
                entry=entry,
                manifest=manifest,
                dry_run=dry_run,
            )
            _revalidate_queue_software(manifest)
            results.append(
                _base_action(
                    entry=entry,
                    action=(
                        "planned"
                        if dry_run
                        else "reused"
                        if existing is not None
                        else "completed"
                    ),
                    adapter_invoked=True,
                    identity=identity,
                    processing_run_id=validated["processing_run_id"],
                    result_sha256=validated["result_sha256"],
                )
            )
        except (
            RunnerError,
            asr_whispercpp.ASRError,
            OSError,
            subprocess.SubprocessError,
        ) as error:
            quarantine = getattr(error, "quarantine", None)
            dispatch_error: Exception = error
            try:
                _replay_dispatch_boundary(
                    manifest=manifest,
                    manifest_body=manifest_body,
                    manifest_path=resolved,
                    runner=runner_before,
                )
            except (
                RunnerError,
                preprocess_asr_queue.QueueError,
                preprocess_batch.BatchError,
                asr_whispercpp.ASRError,
                OSError,
            ) as replay_error:
                dispatch_error = RunnerError(
                    "dispatch failed and the sealed post-failure replay also failed; "
                    f"dispatch={type(error).__name__}: {error}; "
                    f"replay={type(replay_error).__name__}: {replay_error}"
                )
            failed_job = {
                "ordinal": ordinal,
                "job_id": entry["job_id"] if order is None else order["job_id"],
                "work_order_sha256": entry.get(
                    "adapter_work_order_sha256", entry["canonical_sha256"]
                )
                if identity is None
                else identity["work_order_sha256"],
                "error": {
                    "type": type(dispatch_error).__name__,
                    "message": str(dispatch_error),
                },
                "quarantine": quarantine,
                **({"handling": entry["handling"]} if "handling" in entry else {}),
            }
            failure = _summary(
                manifest=manifest,
                manifest_path=resolved,
                manifest_body=manifest_body,
                mode=mode,
                status="failed",
                dry_run=dry_run,
                include_near_silent=include_near_silent,
                results=results,
                runner=runner_before,
                adapter_invocation_count=adapter_invocation_count,
                failed_job=failed_job,
            )
            raise RunnerFailure(failure) from error

    _replay_dispatch_boundary(
        manifest=manifest,
        manifest_body=manifest_body,
        manifest_path=resolved,
        runner=runner_before,
    )
    return _summary(
        manifest=manifest,
        manifest_path=resolved,
        manifest_body=manifest_body,
        mode=mode,
        status="planned" if dry_run else "completed",
        dry_run=dry_run,
        include_near_silent=include_near_silent,
        results=results,
        runner=runner_before,
        adapter_invocation_count=adapter_invocation_count,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate or sequentially dispatch a sealed preprocess-audio ASR queue"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate", help="replay queue and reusable results")
    validate.add_argument("--manifest", required=True)
    validate.add_argument(
        "--include-near-silent",
        action="store_true",
        help="treat reviewed near-silent candidates as dispatch-eligible",
    )
    run = commands.add_parser("run", help="dispatch eligible work orders sequentially")
    run.add_argument("--manifest", required=True)
    run.add_argument("--dry-run", action="store_true")
    run.add_argument(
        "--include-near-silent",
        action="store_true",
        help="explicitly override the default review route for near-silent candidates",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "validate":
            result = validate_dispatcher(
                Path(args.manifest),
                include_near_silent=args.include_near_silent,
            )
        else:
            result = run_queue(
                Path(args.manifest),
                dry_run=args.dry_run,
                include_near_silent=args.include_near_silent,
            )
        sys.stdout.buffer.write(pretty_bytes(result))
        return 0
    except RunnerFailure as error:
        sys.stderr.buffer.write(pretty_bytes(error.result))
        return 2
    except (
        RunnerError,
        preprocess_asr_queue.QueueError,
        preprocess_batch.BatchError,
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
