"""Private, digest-gated admission of paired contextual media-local ASR.

The module deliberately binds one closed pilot batch.  It stores the contextual
transcript as a competing machine revision and numeric, text-private diff evidence.
Nothing here chooses a preferred revision or grants publication authority.
"""

from __future__ import annotations

import json
import math
import sqlite3
import stat
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from .asr_result_importer import (
    MAX_RESULT_BYTES,
    _insert_exact_artifact,
    _insert_exact_processing_run,
    _insert_exact_run_input,
    _read_result,
    _require_catalog_dependencies,
    _stable_read,
    _upsert_job,
    _validate_artifact_contents,
    validate_asr_whispercpp_result,
)
from .db import transaction
from .ids import stable_id
from .importers import canonical_json, sha256_bytes
from .result_importers import ResultImportError


SCHEMA_VERSION = 1
BRIDGE_VERSION = "contextual-media-local-asr-bridge/2"
GLOSSARY_ADMIN_VERSION = "private-neutral-glossary-admin/2"
IMPORTER_NAME = "contextual_media_local_asr_result_v1"
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

PRIVATE_REFERENCE_PREFIX = "urn:private:sha256:"
CATALOG_ENVIRONMENT_PROJECTION_VERSION = (
    "contextual_asr_environment_redacted_v1"
)
PRIVATE_GLOSSARY_PATH = (
    REPOSITORY_ROOT
    / "research/corpus/private-admin/glossaries/himr-neutral-en-v1.json"
)
PRIVATE_BATCH_MANIFEST_PATH = (
    REPOSITORY_ROOT
    / "research/corpus/private-contextual-asr-work-orders/batches/"
    "ctxasrbatch_b696c14db13d7ae51a207e528c5e1d0e/manifest.json"
)
PRIVATE_RESULT_ROOT = (
    REPOSITORY_ROOT / "research/corpus/private-contextual-asr-results"
)
PRIVATE_DIFF_ROOT = (
    REPOSITORY_ROOT
    / "research/corpus/private-contextual-asr-diffs/"
    "ctxasrbatch_b696c14db13d7ae51a207e528c5e1d0e"
)

CONTEXTUAL_BATCH_ID = "ctxasrbatch_b696c14db13d7ae51a207e528c5e1d0e"
CONTEXTUAL_BATCH_IDENTITY_SHA256 = (
    "b696c14db13d7ae51a207e528c5e1d0eebb9161e1e12ecd1dc017d93c1b47de7"
)
CONTEXTUAL_BATCH_RAW_SHA256 = (
    "6742125ffb752778ca082c491f26d1c596c36e566f637ed9b9e8452d3708aa6f"
)
CONTEXTUAL_BATCH_CANONICAL_SHA256 = (
    "743b1a2dc3e0a10efc100c456f3f7613d68470266803df19437657aea6eee13c"
)
CONTEXTUAL_BATCH_WORK_ORDER_COUNT = 17

GLOSSARY_REVISION_ID = "glossary_himrverse_neutral_en_20260827_v1"
GLOSSARY_RAW_SHA256 = (
    "221543ce0a6ef220158d90c00bff95ec8d18aa911b11a40a3ac81e56e2a9b240"
)
GLOSSARY_CANONICAL_SHA256 = (
    "6bc1d768581d399c2ba846aba5454291b67ccea1c29efb452a4740ff3c1ab374"
)
GLOSSARY_REVISION_LABEL = "2026-08-27.1-machine-candidate"
GLOSSARY_REVISION_SHA256 = (
    "69dcc99a3598677ce56e27d4b2916c5b1c9f91d36d8e69c8f7f3b7778370a61a"
)
GLOSSARY_PROMPT_SHA256 = (
    "4ecd3ddb7e6546d145b260ab42ba64e003224e4b0e70d5d6847179fb01f29f96"
)
GLOSSARY_BYTE_COUNT = 613
GLOSSARY_TERM_COUNT = 26
GLOSSARY_LANGUAGE = "en"

MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_GLOSSARY_BYTES = 1024 * 1024
MAX_DIFF_BYTES = 16 * 1024 * 1024

DIFF_NUMERIC_PROJECTION_FIELDS = (
    "block_ms",
    "total_blocks",
    "changed_blocks",
    "unchanged_blocks",
    "total_character_edit_distance",
    "empty_nonempty_transitions",
    "maximum_absolute_first_token_start_drift_ms",
    "maximum_absolute_last_token_end_drift_ms",
    "baseline_lexical_tokens",
    "contextual_lexical_tokens",
    "baseline_untimed_lexical_tokens",
    "contextual_untimed_lexical_tokens",
    "glossary_term_metric_count",
)


def _strict_json(body: bytes, label: str) -> dict[str, Any]:
    def reject_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        parsed: dict[str, Any] = {}
        for key, value in pairs:
            if key in parsed:
                raise ResultImportError(f"{label} contains duplicate key {key!r}")
            parsed[key] = value
        return parsed

    def reject_constant(value: str) -> None:
        raise ResultImportError(f"{label} contains non-finite number {value}")

    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=reject_pairs,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResultImportError(f"{label} is not strict UTF-8 JSON: {error}") from error
    if not isinstance(value, dict):
        raise ResultImportError(f"{label} must contain an object")
    return value


def _resolved_file(
    path_value: str | Path,
    label: str,
    *,
    exact_mode: int | None = None,
) -> Path:
    path = Path(path_value)
    if not path.is_absolute():
        raise ResultImportError(f"{label} path must be absolute")
    try:
        link = path.lstat()
        resolved = path.resolve(strict=True)
    except (OSError, ValueError) as error:
        raise ResultImportError(f"{label} is not a current file: {error}") from error
    if (
        resolved != path
        or stat.S_ISLNK(link.st_mode)
        or not stat.S_ISREG(link.st_mode)
    ):
        raise ResultImportError(
            f"{label} must be a resolved regular path without traversal or symlinks"
        )
    if exact_mode is not None and stat.S_IMODE(link.st_mode) != exact_mode:
        raise ResultImportError(f"{label} mode must be exactly {exact_mode:04o}")
    if exact_mode is not None and link.st_nlink != 1:
        raise ResultImportError(f"{label} must have one hard link")
    return path


def _private_reference(digest: str) -> str:
    if (
        not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise ResultImportError("private reference requires a lowercase SHA-256 digest")
    return f"{PRIVATE_REFERENCE_PREFIX}{digest}"


def _require_private_reference(reference: str, digest: str, label: str) -> None:
    if reference != _private_reference(digest):
        raise ResultImportError(f"{label} is not the digest-addressed private reference")


def _require_within(path: Path, root: Path, label: str) -> None:
    root = root.resolve(strict=True)
    if not path.is_relative_to(root):
        raise ResultImportError(f"{label} is outside its closed-pilot private root")


def _require_sealed_result_directory(path: Path) -> None:
    directory = path.parent
    try:
        observed = directory.lstat()
    except OSError as error:
        raise ResultImportError(
            f"contextual ASR result directory is unavailable: {error}"
        ) from error
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or stat.S_IMODE(observed.st_mode) != 0o500
        or directory.resolve(strict=True) != directory
    ):
        raise ResultImportError(
            "contextual ASR result directory must be a resolved sealed mode-0500 directory"
        )
    try:
        names = {item.name for item in directory.iterdir()}
    except OSError as error:
        raise ResultImportError(
            f"contextual ASR result directory cannot be enumerated: {error}"
        ) from error
    if names != {"result.json", "transcript.normalized.json", "whisper.raw.json"}:
        raise ResultImportError("contextual ASR result directory has missing or extra entries")


def resolve_closed_pilot_private_reference(
    reference: str,
    *,
    digest: str,
    byte_count: int,
    kind: str,
) -> Path:
    """Resolve one opaque receipt through the fixed ignored pilot stores.

    Local paths are administrative inputs, not catalog payload.  Resolution is
    deliberately closed over the four pilot object classes and fails if the
    digest has disappeared, drifted, or occurs more than once.
    """

    _require_private_reference(reference, digest, f"contextual {kind} reference")
    if kind == "glossary":
        candidates = [PRIVATE_GLOSSARY_PATH]
    elif kind == "manifest":
        candidates = [PRIVATE_BATCH_MANIFEST_PATH]
    elif kind == "result":
        candidates = sorted(
            PRIVATE_RESULT_ROOT.glob(
                "asr/whispercpp/sha256/*/*/results/*/result.json"
            )
        )
    elif kind == "diff":
        candidates = sorted(PRIVATE_DIFF_ROOT.glob("[0-9][0-9][0-9][0-9][0-9][0-9].json"))
    else:
        raise ResultImportError(f"unsupported contextual private reference kind {kind!r}")

    matches: list[Path] = []
    for candidate in candidates:
        try:
            size = candidate.lstat().st_size
        except OSError:
            continue
        if size != byte_count:
            continue
        path = _resolved_file(candidate, f"contextual {kind} candidate", exact_mode=0o400)
        body = _stable_read(path, f"contextual {kind} candidate", maximum_bytes=MAX_RESULT_BYTES)
        if sha256_bytes(body) == digest:
            matches.append(path)
    if not matches:
        raise ResultImportError(
            f"contextual {kind} private reference has no current exact local object"
        )
    if len(matches) != 1:
        raise ResultImportError(
            f"contextual {kind} private reference is locally ambiguous"
        )
    return matches[0]


def _stable_document(
    path_value: str | Path,
    label: str,
    *,
    maximum_bytes: int,
    exact_mode: int | None = None,
) -> tuple[Path, bytes, dict[str, Any], str, str]:
    path = _resolved_file(path_value, label, exact_mode=exact_mode)
    body = _stable_read(path, label, maximum_bytes=maximum_bytes)
    raw = _strict_json(body, label)
    closing = _stable_read(path, f"{label} closing read", maximum_bytes=maximum_bytes)
    if closing != body:
        raise ResultImportError(f"{label} changed during validation")
    return (
        path,
        body,
        raw,
        sha256_bytes(body),
        sha256_bytes(canonical_json(raw).encode("utf-8")),
    )


def _timestamp(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ResultImportError(f"{label} must be an RFC 3339 UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as error:
        raise ResultImportError(f"{label} is invalid: {error}") from error
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise ResultImportError(f"{label} must be UTC")
    return value


def _pipeline_modules() -> tuple[Any, Any, Any]:
    pipeline_root = str(REPOSITORY_ROOT / "pipeline")
    added = pipeline_root not in sys.path
    if added:
        sys.path.insert(0, pipeline_root)
    try:
        import asr_whispercpp  # type: ignore  # noqa: PLC0415
        import contextual_asr_batch  # type: ignore  # noqa: PLC0415
        import contextual_asr_diff  # type: ignore  # noqa: PLC0415
    except ImportError as error:
        raise ResultImportError(
            f"contextual ASR pipeline validators could not be loaded: {error}"
        ) from error
    finally:
        if added:
            sys.path.remove(pipeline_root)
    return asr_whispercpp, contextual_asr_batch, contextual_asr_diff


def _read_private_glossary(path_value: str | Path) -> dict[str, Any]:
    path, body, raw, raw_sha, canonical_sha = _stable_document(
        path_value,
        "private neutral glossary",
        maximum_bytes=MAX_GLOSSARY_BYTES,
        exact_mode=0o400,
    )
    if path != PRIVATE_GLOSSARY_PATH.resolve(strict=True):
        raise ResultImportError("private neutral glossary is outside the closed pilot path")
    asr_whispercpp, _, _ = _pipeline_modules()
    try:
        glossary = asr_whispercpp.validate_glossary_document(raw, GLOSSARY_LANGUAGE)
    except asr_whispercpp.ASRError as error:
        raise ResultImportError(f"private neutral glossary is invalid: {error}") from error
    observed = {
        "path": path,
        "artifact_ref": _private_reference(raw_sha),
        "raw_sha256": raw_sha,
        "canonical_sha256": canonical_sha,
        "byte_count": len(body),
        "schema_version": glossary["schema_version"],
        "glossary_revision_id": glossary["glossary_revision_id"],
        "revision_label": glossary["revision"],
        "revision_sha256": sha256_bytes(glossary["revision"].encode("utf-8")),
        "language": glossary["language"],
        "prompt_sha256": glossary["prompt_sha256"],
        "term_count": len(glossary["terms"]),
    }
    expected = {
        "raw_sha256": GLOSSARY_RAW_SHA256,
        "canonical_sha256": GLOSSARY_CANONICAL_SHA256,
        "byte_count": GLOSSARY_BYTE_COUNT,
        "schema_version": 1,
        "glossary_revision_id": GLOSSARY_REVISION_ID,
        "revision_label": GLOSSARY_REVISION_LABEL,
        "revision_sha256": GLOSSARY_REVISION_SHA256,
        "language": GLOSSARY_LANGUAGE,
        "prompt_sha256": GLOSSARY_PROMPT_SHA256,
        "term_count": GLOSSARY_TERM_COUNT,
    }
    if any(observed[key] != value for key, value in expected.items()):
        raise ResultImportError("private neutral glossary differs from the closed pilot pin")
    return observed


def _insert_or_match(
    connection: sqlite3.Connection,
    *,
    table: str,
    key_column: str,
    row: dict[str, Any],
) -> None:
    key = row[key_column]
    columns = tuple(column for column in row if column != key_column)
    existing = connection.execute(
        f"SELECT {', '.join(columns)} FROM {table} WHERE {key_column} = ?", (key,)
    ).fetchone()
    if existing is not None:
        if any(existing[column] != row[column] for column in columns):
            raise ResultImportError(f"{table} {key_column} collision")
        return
    connection.execute(
        f"INSERT INTO {table}({key_column}, {', '.join(columns)}) "
        f"VALUES({', '.join('?' for _ in row)})",
        (key, *(row[column] for column in columns)),
    )


def _build_glossary_registration(
    glossary_path: str | Path, *, observed_at: str
) -> dict[str, Any]:
    observed_at = _timestamp(observed_at, "observed_at")
    glossary = _read_private_glossary(glossary_path)
    core = {
        "schema_version": SCHEMA_VERSION,
        "admin_version": GLOSSARY_ADMIN_VERSION,
        "glossary": {
            key: glossary[key]
            for key in (
                "artifact_ref",
                "raw_sha256",
                "canonical_sha256",
                "byte_count",
                "schema_version",
                "glossary_revision_id",
                "revision_label",
                "revision_sha256",
                "language",
                "prompt_sha256",
                "term_count",
            )
        },
        "registered_at": observed_at,
        "policy": {
            "visibility": "private",
            "terms_in_plan": False,
            "terms_stored_in_catalog": False,
            "review_state": "machine_candidate_unreviewed",
            "accuracy_claimed": False,
            "publication_authority": "none",
        },
    }
    plan_sha = sha256_bytes(canonical_json(core).encode("utf-8"))
    registration_id = stable_id(
        "pgr", SCHEMA_VERSION, glossary["glossary_revision_id"], glossary["raw_sha256"]
    )
    public = {
        "status": "validated",
        "plan_sha256": plan_sha,
        "private_glossary_registration_id": registration_id,
        **core,
    }
    row = {
        "private_glossary_registration_id": registration_id,
        "glossary_revision_id": glossary["glossary_revision_id"],
        "artifact_uri": glossary["artifact_ref"],
        "raw_sha256": glossary["raw_sha256"],
        "canonical_sha256": glossary["canonical_sha256"],
        "byte_count": glossary["byte_count"],
        "schema_version": glossary["schema_version"],
        "revision_label": glossary["revision_label"],
        "revision_sha256": glossary["revision_sha256"],
        "language": glossary["language"],
        "prompt_sha256": glossary["prompt_sha256"],
        "term_count": glossary["term_count"],
        "terms_stored_in_catalog": 0,
        "review_state": "machine_candidate_unreviewed",
        "accuracy_claimed": 0,
        "publication_authority": "none",
        "plan_sha256": plan_sha,
        "registered_at": observed_at,
    }
    return {"public": public, "row": row, "glossary": glossary}


def build_private_glossary_registration_plan(
    glossary_path: str | Path, *, observed_at: str
) -> dict[str, Any]:
    """Return a term-free plan for the one closed neutral glossary."""

    return _build_glossary_registration(glossary_path, observed_at=observed_at)["public"]


def import_private_glossary_registration(
    connection: sqlite3.Connection,
    glossary_path: str | Path,
    *,
    observed_at: str,
    expected_plan_sha256: str,
) -> dict[str, Any]:
    preflight = _build_glossary_registration(glossary_path, observed_at=observed_at)
    if preflight["public"]["plan_sha256"] != expected_plan_sha256:
        raise ResultImportError(
            "private glossary plan changed or does not match the expected digest"
        )
    with transaction(connection):
        plan = _build_glossary_registration(glossary_path, observed_at=observed_at)
        if plan["public"]["plan_sha256"] != expected_plan_sha256:
            raise ResultImportError("private glossary plan changed inside the transaction")
        glossary = plan["glossary"]
        glossary_row = {
            "glossary_revision_id": glossary["glossary_revision_id"],
            "parent_glossary_revision_id": None,
            "sha256": glossary["raw_sha256"],
            "created_at": observed_at,
            "description": "Private neutral spelling-only machine-candidate glossary.",
            "artifact_uri": glossary["artifact_ref"],
        }
        _insert_or_match(
            connection,
            table="glossary_revisions",
            key_column="glossary_revision_id",
            row=glossary_row,
        )
        _insert_or_match(
            connection,
            table="private_glossary_registrations",
            key_column="private_glossary_registration_id",
            row=plan["row"],
        )
    return {**plan["public"], "status": "registered"}


def _read_validated_batch(path_value: str | Path) -> dict[str, Any]:
    path = _resolved_file(path_value, "contextual ASR batch manifest", exact_mode=0o400)
    if path != PRIVATE_BATCH_MANIFEST_PATH.resolve(strict=True):
        raise ResultImportError("contextual ASR batch manifest is outside the closed pilot path")
    _, contextual_batch, _ = _pipeline_modules()
    try:
        manifest, orders = contextual_batch.validate_batch(path)
    except contextual_batch.ContextualBatchError as error:
        raise ResultImportError(f"contextual ASR batch failed replay: {error}") from error
    body = _stable_read(path, "contextual ASR batch manifest", maximum_bytes=MAX_MANIFEST_BYTES)
    raw = _strict_json(body, "contextual ASR batch manifest")
    observed = {
        "batch_id": manifest.get("batch_id"),
        "identity_sha256": manifest.get("identity_sha256"),
        "raw_sha256": sha256_bytes(body),
        "canonical_sha256": sha256_bytes(canonical_json(raw).encode("utf-8")),
        "byte_count": len(body),
        "work_order_count": manifest.get("work_order_count"),
    }
    expected = {
        "batch_id": CONTEXTUAL_BATCH_ID,
        "identity_sha256": CONTEXTUAL_BATCH_IDENTITY_SHA256,
        "raw_sha256": CONTEXTUAL_BATCH_RAW_SHA256,
        "canonical_sha256": CONTEXTUAL_BATCH_CANONICAL_SHA256,
        "byte_count": 60812,
        "work_order_count": CONTEXTUAL_BATCH_WORK_ORDER_COUNT,
    }
    if observed != expected or raw != manifest or len(orders) != CONTEXTUAL_BATCH_WORK_ORDER_COUNT:
        raise ResultImportError("contextual ASR batch differs from the closed pilot")
    return {
        "path": path,
        "reference": _private_reference(observed["raw_sha256"]),
        "body": body,
        "manifest": manifest,
        "orders": orders,
        **observed,
    }


def _read_contextual_result(path_value: str | Path) -> tuple[dict[str, Any], dict[str, Any]]:
    path, body, raw, raw_sha, canonical_sha = _stable_document(
        path_value,
        "contextual ASR result",
        maximum_bytes=MAX_RESULT_BYTES,
        exact_mode=0o400,
    )
    _require_within(path, PRIVATE_RESULT_ROOT, "contextual ASR result")
    _require_sealed_result_directory(path)
    imported_raw, importer_canonical_sha, imported_path = _read_result(path)
    if imported_raw != raw or importer_canonical_sha != canonical_sha or imported_path != path:
        raise ResultImportError("contextual ASR result changed across validator boundaries")
    result = validate_asr_whispercpp_result(raw, result_file_path=path)
    _validate_artifact_contents(result)
    for artifact_kind in ("whispercpp_output_json_full", "transcript_normalized_json"):
        _resolved_file(
            result["_paths"][artifact_kind],
            f"contextual ASR {artifact_kind} artifact",
            exact_mode=0o400,
        )
    if result["catalog_context"] is not None or result["glossary"] is None:
        raise ResultImportError("contextual media-local ASR requires null catalog context and a glossary")
    if (
        result["processing_run"]["glossary_revision_id"] != GLOSSARY_REVISION_ID
        or result["glossary"]["sha256"] != GLOSSARY_RAW_SHA256
        or result["glossary"]["prompt_sha256"] != GLOSSARY_PROMPT_SHA256
        or result["glossary"]["term_count"] != GLOSSARY_TERM_COUNT
    ):
        raise ResultImportError("contextual ASR result glossary differs from the registered pilot")
    glossary_path = _resolved_file(
        result["_paths"]["glossary"],
        "contextual ASR glossary",
        exact_mode=0o400,
    )
    if glossary_path != PRIVATE_GLOSSARY_PATH.resolve(strict=True):
        raise ResultImportError("contextual ASR result cites a non-pilot glossary path")
    resolved_result = resolve_closed_pilot_private_reference(
        _private_reference(raw_sha),
        digest=raw_sha,
        byte_count=len(body),
        kind="result",
    )
    if resolved_result != path:
        raise ResultImportError("contextual ASR result has an ambiguous local resolution")
    return result, {
        "path": path,
        "reference": _private_reference(raw_sha),
        "body": body,
        "raw": raw,
        "raw_sha256": raw_sha,
        "canonical_sha256": canonical_sha,
        "byte_count": len(body),
    }


def _read_diff(
    path_value: str | Path, *, baseline_path: str, contextual_path: str
) -> dict[str, Any]:
    path, body, raw, raw_sha, canonical_sha = _stable_document(
        path_value,
        "contextual ASR private diff",
        maximum_bytes=MAX_DIFF_BYTES,
        exact_mode=0o400,
    )
    _require_within(path, PRIVATE_DIFF_ROOT, "contextual ASR private diff")
    _, _, contextual_diff = _pipeline_modules()
    block_ms = raw.get("alignment", {}).get("block_ms")
    try:
        rebuilt = contextual_diff.build_diff(
            baseline_result=baseline_path,
            contextual_result=contextual_path,
            block_ms=block_ms,
        )
    except contextual_diff.DiffError as error:
        raise ResultImportError(f"contextual ASR diff failed replay: {error}") from error
    if raw != rebuilt or body != contextual_diff.pretty_bytes(rebuilt):
        raise ResultImportError("contextual ASR diff differs from deterministic replay")
    policy = raw["policy"]
    if policy != {
        "visibility": "private",
        "transcript_text_included": False,
        "decoder_scores_calibrated": False,
        "accuracy_claimed": False,
        "improvement_claimed": False,
        "preferred_revision_selected": False,
        "human_review_claimed": False,
        "automatic_merge_allowed": False,
        "publication_authority": "none",
    }:
        raise ResultImportError("contextual ASR diff policy is not fail-closed")
    resolved_diff = resolve_closed_pilot_private_reference(
        _private_reference(raw_sha),
        digest=raw_sha,
        byte_count=len(body),
        kind="diff",
    )
    if resolved_diff != path:
        raise ResultImportError("contextual ASR diff has an ambiguous local resolution")
    return {
        "path": path,
        "reference": _private_reference(raw_sha),
        "raw": raw,
        "raw_sha256": raw_sha,
        "canonical_sha256": canonical_sha,
        "byte_count": len(body),
    }


def _catalog_safe_processing_run(result: dict[str, Any]) -> dict[str, Any]:
    """Project a validated producer run without argv, prompt, or local paths."""

    run = result["processing_run"]
    environment = json.loads(run["environment_json"])
    provenance = environment["command_provenance"]
    logical_commands = provenance["logical_commands"]
    result_commands = result["commands"]
    projection = {
        "projection_version": CATALOG_ENVIRONMENT_PROJECTION_VERSION,
        "execution": {
            "cpu_only": environment["cpu_only"],
            "network": environment["network"],
            "engine_version": environment["engine_version"],
            "engine_version_evidence": environment["engine_version_evidence"],
            "python": environment["python"],
            "descriptor_execution_policy": provenance[
                "descriptor_execution_policy"
            ],
            "logical_command_count": len(logical_commands),
            "result_command_states": provenance["result_command_states"],
            "prompt_present": result["glossary"] is not None,
        },
        "integrity": {
            "source_environment_json_sha256": sha256_bytes(
                run["environment_json"].encode("utf-8")
            ),
            "parameters_json_sha256": sha256_bytes(
                run["parameters_json"].encode("utf-8")
            ),
            "logical_commands_sha256": sha256_bytes(
                canonical_json(logical_commands).encode("utf-8")
            ),
            "logical_command_sha256": [
                sha256_bytes(canonical_json(command).encode("utf-8"))
                for command in logical_commands
            ],
            "result_commands_sha256": sha256_bytes(
                canonical_json(result_commands).encode("utf-8")
            ),
            "result_command_sha256": [
                sha256_bytes(canonical_json(command).encode("utf-8"))
                for command in result_commands
            ],
            "prompt_sha256": result["glossary"]["prompt_sha256"],
        },
    }

    def projected_strings(value: Any) -> list[str]:
        if isinstance(value, str):
            return [value]
        if isinstance(value, dict):
            return [
                text
                for child in value.values()
                for text in projected_strings(child)
            ]
        if isinstance(value, list):
            return [text for child in value for text in projected_strings(child)]
        return []

    encoded = canonical_json(projection)
    if any(
        text.startswith(("/", "file:", "\\\\")) or "--prompt" in text
        for text in projected_strings(projection)
    ):
        raise ResultImportError("catalog processing-run projection contains private command data")
    return {**run, "environment_json": encoded}


def _catalog_safe_artifacts(result: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {**artifact, "storage_uri": _private_reference(artifact["sha256"])}
        for artifact in result["artifacts"]
    ]


def _require_registered_glossary(
    connection: sqlite3.Connection, result: dict[str, Any]
) -> sqlite3.Row:
    row = connection.execute(
        """
        SELECT registration.*, glossary.sha256 AS registry_sha256,
               glossary.artifact_uri AS registry_artifact_uri
        FROM private_glossary_registrations AS registration
        JOIN glossary_revisions AS glossary USING(glossary_revision_id)
        WHERE registration.glossary_revision_id = ?
        """,
        (GLOSSARY_REVISION_ID,),
    ).fetchone()
    glossary = result["glossary"]
    expected = {
        "artifact_uri": _private_reference(glossary["sha256"]),
        "raw_sha256": glossary["sha256"],
        "byte_count": glossary["byte_count"],
        "prompt_sha256": glossary["prompt_sha256"],
        "term_count": glossary["term_count"],
        "registry_sha256": glossary["sha256"],
        "registry_artifact_uri": _private_reference(glossary["sha256"]),
    }
    if row is None or any(row[key] != value for key, value in expected.items()):
        raise ResultImportError("contextual glossary is not exactly registered privately")
    return row


def _require_baseline(
    connection: sqlite3.Connection,
    entry: dict[str, Any],
    result: dict[str, Any],
) -> sqlite3.Row:
    baseline = entry["baseline"]
    rows = connection.execute(
        """
        SELECT revision.*, receipt.asr_result_raw_sha256,
               receipt.asr_result_canonical_sha256,
               receipt.asr_result_byte_count
        FROM media_local_transcript_revisions AS revision
        JOIN media_local_asr_imports AS receipt USING(media_local_revision_id)
        WHERE revision.processing_run_id = ?
        """,
        (baseline["processing_run_id"],),
    ).fetchall()
    if len(rows) != 1:
        raise ResultImportError("contextual pair requires one already admitted raw baseline")
    row = rows[0]
    if (
        row["revision_kind"] != "raw_asr"
        or row["glossary_revision_id"] is not None
        or row["review_state"] != "machine"
        or row["media_id"] != result["input"]["media_id"]
        or row["input_artifact_id"] != result["input"]["artifact_id"]
        or row["input_duration_ms"] != result["input"]["probe"]["duration_ms"]
        or row["asr_result_raw_sha256"] != baseline["raw_sha256"]
        or row["asr_result_canonical_sha256"] != baseline["canonical_sha256"]
        or row["asr_result_byte_count"] != baseline["byte_count"]
    ):
        raise ResultImportError("admitted raw baseline differs from the contextual batch pin")
    return row


def _media_rows(result: dict[str, Any], canonical_sha: str, raw_sha: str) -> dict[str, Any]:
    run = result["processing_run"]
    media_id = result["input"]["media_id"]
    input_artifact_id = result["input"]["artifact_id"]
    revision_id = stable_id(
        "mltr", SCHEMA_VERSION, run["processing_run_id"], media_id, raw_sha
    )
    duration = result["input"]["probe"]["duration_ms"]
    segments: list[dict[str, Any]] = []
    words: list[dict[str, Any]] = []
    max_segment_end = 0
    null_timed_words = 0
    for segment in result["transcript"]["segments"]:
        segment_id = stable_id("mlts", revision_id, segment["ordinal"])
        max_segment_end = max(max_segment_end, segment["end_ms"])
        segments.append(
            {
                "media_local_segment_id": segment_id,
                "media_local_revision_id": revision_id,
                "ordinal": segment["ordinal"],
                "media_start_ms": segment["start_ms"],
                "media_end_ms": segment["end_ms"],
                "input_boundary_overrun_ms": max(0, segment["end_ms"] - duration),
                "text": segment["text"],
                "normalized_text": None,
                "speaker_label": None,
                "language": result["transcript"]["language"]["detected"],
                "confidence_band": None,
                "calibrated_probability": None,
                "metadata_json": canonical_json(
                    {
                        "confidence_calibration": "none",
                        "engine_segment": json.loads(segment["metadata_json"]),
                        "quality_flags": segment["quality_flags"],
                        "window_overrun_ms": segment["window_overrun_ms"],
                    }
                ),
            }
        )
        for token in segment["tokens"]:
            if token["start_ms"] is None:
                null_timed_words += 1
            probability = token["raw_probability"]
            words.append(
                {
                    "media_local_word_id": stable_id("mltw", segment_id, token["ordinal"]),
                    "media_local_segment_id": segment_id,
                    "ordinal": token["ordinal"],
                    "media_start_ms": token["start_ms"],
                    "media_end_ms": token["end_ms"],
                    "token": token["text"],
                    "normalized_token": None,
                    "asr_log_probability": math.log(probability) if probability > 0 else None,
                    "alignment_score": None,
                    "calibrated_probability": None,
                    "metadata_json": canonical_json(
                        {
                            "confidence_calibration": "none",
                            "raw_dtw_timestamp": token["raw_dtw_timestamp"],
                            "raw_probability": probability,
                            "timing_quality_flags": token.get("timing_quality_flags"),
                            "timing_state": token.get("timing_state"),
                            "token_id": token["token_id"],
                        }
                    ),
                }
            )
    overrun = max(0, max_segment_end - duration)
    revision = {
        "media_local_revision_id": revision_id,
        "media_id": media_id,
        "input_artifact_id": input_artifact_id,
        "processing_run_id": run["processing_run_id"],
        "revision_kind": "contextual_asr",
        "origin": "whisper.cpp output-json-full; paired neutral-glossary pilot",
        "language": result["transcript"]["language"]["detected"],
        "glossary_revision_id": GLOSSARY_REVISION_ID,
        "review_state": "machine",
        "coordinate_system": "media_ms",
        "boundary": "half_open",
        "input_duration_ms": duration,
        "requested_start_ms": 0,
        "requested_end_ms": duration,
        "max_segment_end_ms": max_segment_end,
        "input_boundary_overrun_ms": overrun,
        "source_coordinate_state": "unasserted_catalog_context_null",
        "recording_coordinate_state": "unasserted_catalog_context_null",
        "created_at": run["completed_at"],
        "metadata_json": canonical_json(
            {
                "asr_result_canonical_sha256": canonical_sha,
                "asr_result_raw_sha256": raw_sha,
                "confidence_calibration": "none",
                "machine_hypothesis": True,
                "preference_selected": False,
                "recording_coordinates_asserted": False,
                "source_coordinates_asserted": False,
            }
        ),
    }
    return {
        "revision": revision,
        "segments": segments,
        "words": words,
        "max_segment_end_ms": max_segment_end,
        "input_boundary_overrun_ms": overrun,
        "null_timed_word_count": null_timed_words,
    }


def _require_no_preexisting_publication(
    connection: sqlite3.Connection, objects: list[tuple[str, str]]
) -> None:
    for object_type, object_id in objects:
        decision = connection.execute(
            """
            SELECT 1 FROM publication_decisions
            WHERE object_type = ? AND object_id = ?
            UNION ALL
            SELECT 1 FROM publication_gate_decisions
            WHERE object_type = ? AND object_id = ?
            LIMIT 1
            """,
            (object_type, object_id, object_type, object_id),
        ).fetchone()
        if decision is not None:
            raise ResultImportError(
                "contextual private object already has a generic publication decision"
            )


def require_exact_contextual_diff_projection(
    stored: dict[str, Any], replayed: dict[str, Any]
) -> None:
    """Compare identity, alignment, policy, and all numeric retained evidence."""

    required = {
        "contextual_diff_id",
        "contextual_pair_id",
        "diff_uri",
        "diff_raw_sha256",
        "diff_canonical_sha256",
        "diff_byte_count",
        "diff_identity_sha256",
        *DIFF_NUMERIC_PROJECTION_FIELDS,
        "transcript_text_stored",
        "decoder_scores_calibrated",
        "accuracy_claimed",
        "improvement_claimed",
        "preferred_revision_selected",
        "human_review_claimed",
        "automatic_merge_allowed",
        "visibility",
        "publication_authority",
        "created_at",
    }
    if set(replayed) != required or any(
        stored.get(field) != replayed[field] for field in required
    ):
        raise ResultImportError(
            "contextual diff catalog projection differs from deterministic replay"
        )


def _build_admission(
    connection: sqlite3.Connection,
    result_path: str | Path,
    batch_manifest_path: str | Path,
    diff_path: str | Path,
) -> dict[str, Any]:
    batch = _read_validated_batch(batch_manifest_path)
    result, result_file = _read_contextual_result(result_path)
    entries = [
        (entry, order)
        for entry, order in zip(batch["manifest"]["work_orders"], batch["orders"], strict=True)
        if entry["job_id"] == result["_job_id"]
    ]
    if len(entries) != 1:
        raise ResultImportError("contextual result is not one exact member of the pilot batch")
    entry, order = entries[0]
    _, contextual_batch, _ = _pipeline_modules()
    try:
        contextual_batch._validate_adapter_result(  # noqa: SLF001
            result_file["raw"],
            order,
            entry,
            batch["manifest"]["glossary"],
            batch["manifest"]["engine"],
            batch["manifest"]["model"],
            dry_run=False,
        )
    except contextual_batch.ContextualBatchError as error:
        raise ResultImportError(f"contextual result differs from its paired work order: {error}") from error
    catalog_run = _catalog_safe_processing_run(result)
    catalog_artifacts = _catalog_safe_artifacts(result)
    # The generic dependency checker predates opaque private references and would
    # compare the registered glossary URI to a local file URI.  The producer result
    # has already been validated with its glossary; omit only that one registry
    # lookup here, retain every input/model dependency check, and enforce the exact
    # opaque glossary registration immediately below.
    _require_catalog_dependencies(connection, {**result, "glossary": None})
    glossary_registration = _require_registered_glossary(connection, result)
    baseline = _require_baseline(connection, entry, result)
    diff = _read_diff(
        diff_path,
        baseline_path=entry["baseline"]["result_path"],
        contextual_path=str(result_file["path"]),
    )
    media = _media_rows(
        result, result_file["canonical_sha256"], result_file["raw_sha256"]
    )
    revision_id = media["revision"]["media_local_revision_id"]
    pair_id = stable_id(
        "casp",
        SCHEMA_VERSION,
        baseline["media_local_revision_id"],
        revision_id,
        entry["pair_projection_sha256"],
    )
    import_id = stable_id(
        "cmai", SCHEMA_VERSION, result_file["raw_sha256"], revision_id
    )
    diff_raw = diff["raw"]
    summary = diff_raw["summary"]
    core = {
        "schema_version": SCHEMA_VERSION,
        "bridge_version": BRIDGE_VERSION,
        "result": {
            "result_ref": result_file["reference"],
            "raw_sha256": result_file["raw_sha256"],
            "canonical_sha256": result_file["canonical_sha256"],
            "byte_count": result_file["byte_count"],
            "result_key": result["result_key"],
            "processing_run_id": result["processing_run"]["processing_run_id"],
            "media_local_revision_id": revision_id,
            "catalog_environment_projection_version": (
                CATALOG_ENVIRONMENT_PROJECTION_VERSION
            ),
            "catalog_environment_json_sha256": sha256_bytes(
                catalog_run["environment_json"].encode("utf-8")
            ),
            "parameters_json_sha256": sha256_bytes(
                catalog_run["parameters_json"].encode("utf-8")
            ),
            "artifacts": [
                {
                    key: artifact[key]
                    for key in (
                        "artifact_id",
                        "artifact_kind",
                        "storage_uri",
                        "sha256",
                        "byte_count",
                    )
                }
                for artifact in catalog_artifacts
            ],
        },
        "batch": {
            "batch_id": batch["batch_id"],
            "identity_sha256": batch["identity_sha256"],
            "manifest_ref": batch["reference"],
            "manifest_raw_sha256": batch["raw_sha256"],
            "manifest_canonical_sha256": batch["canonical_sha256"],
            "manifest_byte_count": batch["byte_count"],
            "ordinal": entry["ordinal"],
            "pair_projection_sha256": entry["pair_projection_sha256"],
            "work_order_ref": _private_reference(entry["sha256"]),
            "work_order_raw_sha256": entry["sha256"],
            "work_order_canonical_sha256": entry["canonical_sha256"],
        },
        "baseline": {
            "media_local_revision_id": baseline["media_local_revision_id"],
            "processing_run_id": baseline["processing_run_id"],
            "result_raw_sha256": entry["baseline"]["raw_sha256"],
            "result_canonical_sha256": entry["baseline"]["canonical_sha256"],
        },
        "glossary": {
            "private_glossary_registration_id": glossary_registration[
                "private_glossary_registration_id"
            ],
            "glossary_revision_id": GLOSSARY_REVISION_ID,
            "raw_sha256": GLOSSARY_RAW_SHA256,
            "canonical_sha256": GLOSSARY_CANONICAL_SHA256,
            "prompt_sha256": GLOSSARY_PROMPT_SHA256,
            "term_count": GLOSSARY_TERM_COUNT,
            "terms_in_plan": False,
        },
        "pair": {
            "contextual_pair_id": pair_id,
            "pair_state": "competing_machine_revisions_no_preference",
            "input_equal": True,
            "engine_equal": True,
            "model_equal": True,
            "window_equal": True,
            "inference_equal": True,
            "catalog_context_equal": True,
            "only_glossary_job_output_differ": True,
        },
        "diff": {
            "contextual_diff_id": stable_id(
                "casd", SCHEMA_VERSION, diff_raw["identity_sha256"], pair_id
            ),
            "diff_ref": diff["reference"],
            "raw_sha256": diff["raw_sha256"],
            "canonical_sha256": diff["canonical_sha256"],
            "byte_count": diff["byte_count"],
            "identity_sha256": diff_raw["identity_sha256"],
            "block_ms": diff_raw["alignment"]["block_ms"],
            "summary": {
                key: summary[key]
                for key in (
                    "total_blocks",
                    "changed_blocks",
                    "unchanged_blocks",
                    "total_character_edit_distance",
                    "empty_nonempty_transitions",
                    "maximum_absolute_first_token_start_drift_ms",
                    "maximum_absolute_last_token_end_drift_ms",
                    "baseline_lexical_tokens",
                    "contextual_lexical_tokens",
                    "baseline_untimed_lexical_tokens",
                    "contextual_untimed_lexical_tokens",
                )
            },
            "glossary_term_metric_count": len(summary["glossary_term_counts"]),
            "transcript_text_in_plan": False,
        },
        "coordinate_contract": {
            "coordinate_system": "media_ms",
            "boundary": "half_open",
            "input_duration_ms": media["revision"]["input_duration_ms"],
            "max_segment_end_ms": media["max_segment_end_ms"],
            "input_boundary_overrun_ms": media["input_boundary_overrun_ms"],
            "null_timed_word_count": media["null_timed_word_count"],
            "recording_coordinates_asserted": False,
            "source_coordinates_asserted": False,
        },
        "statistics": {
            "contextual_import_receipts": 1,
            "contextual_pairs": 1,
            "text_private_diffs": 1,
            "media_local_revisions": 1,
            "media_local_segments": len(media["segments"]),
            "media_local_words": len(media["words"]),
            "publication_decisions": 0,
            "preference_decisions": 0,
            "human_review_decisions": 0,
        },
        "policy": {
            "visibility": "private",
            "result_filesystem_state": "stable_hash_bound_no_seal_claim",
            "decoder_scores_calibrated": False,
            "accuracy_claimed": False,
            "improvement_claimed": False,
            "preferred_revision_selected": False,
            "correction_asserted": False,
            "human_review_claimed": False,
            "automatic_merge_allowed": False,
            "publication_authority": "none",
            "transcript_text_in_plan": False,
        },
    }
    plan_sha = sha256_bytes(canonical_json(core).encode("utf-8"))
    public = {
        "status": "validated",
        "plan_sha256": plan_sha,
        **core,
    }
    import_batch_id = stable_id(
        "imp", IMPORTER_NAME, result_file["canonical_sha256"]
    )
    import_receipt = {
        "contextual_asr_import_id": import_id,
        "import_batch_id": import_batch_id,
        "media_local_revision_id": revision_id,
        "contextual_batch_id": CONTEXTUAL_BATCH_ID,
        "batch_ordinal": entry["ordinal"],
        "pair_projection_sha256": entry["pair_projection_sha256"],
        "work_order_uri": public["batch"]["work_order_ref"],
        "work_order_raw_sha256": entry["sha256"],
        "work_order_canonical_sha256": entry["canonical_sha256"],
        "result_uri": result_file["reference"],
        "result_raw_sha256": result_file["raw_sha256"],
        "result_canonical_sha256": result_file["canonical_sha256"],
        "result_byte_count": result_file["byte_count"],
        "input_media_id": result["input"]["media_id"],
        "input_artifact_id": result["input"]["artifact_id"],
        "input_duration_ms": media["revision"]["input_duration_ms"],
        "max_segment_end_ms": media["max_segment_end_ms"],
        "input_boundary_overrun_ms": media["input_boundary_overrun_ms"],
        "null_timed_word_count": media["null_timed_word_count"],
        "result_filesystem_state": "stable_hash_bound_no_seal_claim",
        "plan_sha256": plan_sha,
        "imported_at": result["processing_run"]["completed_at"],
        "metadata_json": canonical_json(
            {
                "catalog_context": None,
                "confidence_calibration": "none",
                "preference_selected": False,
                "seal_claimed": False,
            }
        ),
    }
    pair_row = {
        "contextual_pair_id": pair_id,
        "baseline_media_local_revision_id": baseline["media_local_revision_id"],
        "contextual_media_local_revision_id": revision_id,
        "contextual_asr_import_id": import_id,
        "glossary_revision_id": GLOSSARY_REVISION_ID,
        "pair_projection_sha256": entry["pair_projection_sha256"],
        "pair_state": "competing_machine_revisions_no_preference",
        "input_equal": 1,
        "engine_equal": 1,
        "model_equal": 1,
        "window_equal": 1,
        "inference_equal": 1,
        "catalog_context_equal": 1,
        "only_glossary_job_output_differ": 1,
        "preferred_revision_id": None,
        "correction_asserted": 0,
        "accuracy_claimed": 0,
        "improvement_claimed": 0,
        "human_review_claimed": 0,
        "automatic_merge_allowed": 0,
        "publication_authority": "none",
        "created_at": result["processing_run"]["completed_at"],
    }
    summary_row = public["diff"]["summary"]
    diff_row = {
        "contextual_diff_id": public["diff"]["contextual_diff_id"],
        "contextual_pair_id": pair_id,
        "diff_uri": diff["reference"],
        "diff_raw_sha256": diff["raw_sha256"],
        "diff_canonical_sha256": diff["canonical_sha256"],
        "diff_byte_count": diff["byte_count"],
        "diff_identity_sha256": diff_raw["identity_sha256"],
        "block_ms": diff_raw["alignment"]["block_ms"],
        **summary_row,
        "glossary_term_metric_count": len(summary["glossary_term_counts"]),
        "transcript_text_stored": 0,
        "decoder_scores_calibrated": 0,
        "accuracy_claimed": 0,
        "improvement_claimed": 0,
        "preferred_revision_selected": 0,
        "human_review_claimed": 0,
        "automatic_merge_allowed": 0,
        "visibility": "private",
        "publication_authority": "none",
        "created_at": result["processing_run"]["completed_at"],
    }
    _require_no_preexisting_publication(
        connection,
        [
            ("contextual_asr_batch_registration", CONTEXTUAL_BATCH_ID),
            ("contextual_media_local_asr_import", import_id),
            ("contextual_media_local_asr_pair", pair_id),
            ("contextual_asr_text_private_diff", diff_row["contextual_diff_id"]),
            ("media_local_transcript_revision", revision_id),
            *[
                ("media_local_transcript_segment", row["media_local_segment_id"])
                for row in media["segments"]
            ],
            *[
                ("media_local_transcript_word", row["media_local_word_id"])
                for row in media["words"]
            ],
        ],
    )
    batch_row = {
        "contextual_batch_id": CONTEXTUAL_BATCH_ID,
        "identity_sha256": batch["identity_sha256"],
        "manifest_uri": batch["reference"],
        "manifest_raw_sha256": batch["raw_sha256"],
        "manifest_canonical_sha256": batch["canonical_sha256"],
        "manifest_byte_count": batch["byte_count"],
        "materializer": batch["manifest"]["materializer"],
        "materializer_version": batch["manifest"]["implementation_version"],
        "work_order_count": batch["work_order_count"],
        "glossary_revision_id": GLOSSARY_REVISION_ID,
        "visibility": "private",
        "publication_authority": "none",
        "registered_at": glossary_registration["registered_at"],
    }
    return {
        "public": public,
        "result": result,
        "catalog_run": catalog_run,
        "catalog_artifacts": catalog_artifacts,
        "media": media,
        "batch_row": batch_row,
        "import_batch_id": import_batch_id,
        "import_receipt": import_receipt,
        "pair_row": pair_row,
        "diff_row": diff_row,
    }


def build_contextual_media_local_asr_admission_plan(
    connection: sqlite3.Connection,
    result_path: str | Path,
    batch_manifest_path: str | Path,
    diff_path: str | Path,
) -> dict[str, Any]:
    """Return a wording-free digest-gated plan for one exact contextual pair."""

    return _build_admission(
        connection, result_path, batch_manifest_path, diff_path
    )["public"]


def import_contextual_media_local_asr_result(
    connection: sqlite3.Connection,
    result_path: str | Path,
    batch_manifest_path: str | Path,
    diff_path: str | Path,
    *,
    expected_plan_sha256: str,
) -> dict[str, Any]:
    preflight = _build_admission(connection, result_path, batch_manifest_path, diff_path)
    if preflight["public"]["plan_sha256"] != expected_plan_sha256:
        raise ResultImportError(
            "contextual ASR plan changed or does not match the expected digest"
        )
    with transaction(connection):
        plan = _build_admission(connection, result_path, batch_manifest_path, diff_path)
        if plan["public"]["plan_sha256"] != expected_plan_sha256:
            raise ResultImportError("contextual ASR plan changed inside the transaction")
        result = plan["result"]
        run = plan["catalog_run"]
        _insert_or_match(
            connection,
            table="contextual_asr_batch_registrations",
            key_column="contextual_batch_id",
            row=plan["batch_row"],
        )
        batch_row = {
            "import_batch_id": plan["import_batch_id"],
            "importer_name": IMPORTER_NAME,
            "importer_version": BRIDGE_VERSION,
            "input_sha256": plan["public"]["result"]["canonical_sha256"],
            "source_snapshot_date": None,
            "started_at": run["started_at"],
            "completed_at": run["completed_at"],
            "status": "completed",
            "statistics_json": canonical_json(plan["public"]["statistics"]),
        }
        _insert_or_match(
            connection, table="import_batches", key_column="import_batch_id", row=batch_row
        )
        _insert_exact_processing_run(connection, run)
        _insert_exact_run_input(connection, result["run_input"])
        for artifact in plan["catalog_artifacts"]:
            _insert_exact_artifact(connection, artifact)
        _insert_or_match(
            connection,
            table="media_local_transcript_revisions",
            key_column="media_local_revision_id",
            row=plan["media"]["revision"],
        )
        for segment in plan["media"]["segments"]:
            _insert_or_match(
                connection,
                table="media_local_transcript_segments",
                key_column="media_local_segment_id",
                row=segment,
            )
        for word in plan["media"]["words"]:
            _insert_or_match(
                connection,
                table="media_local_transcript_words",
                key_column="media_local_word_id",
                row=word,
            )
        _insert_or_match(
            connection,
            table="contextual_media_local_asr_imports",
            key_column="contextual_asr_import_id",
            row=plan["import_receipt"],
        )
        _insert_or_match(
            connection,
            table="contextual_media_local_asr_pairs",
            key_column="contextual_pair_id",
            row=plan["pair_row"],
        )
        _insert_or_match(
            connection,
            table="contextual_asr_text_private_diffs",
            key_column="contextual_diff_id",
            row=plan["diff_row"],
        )
        _upsert_job(connection, result)
    return {**plan["public"], "status": "admitted"}
