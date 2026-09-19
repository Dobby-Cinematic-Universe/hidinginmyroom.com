"""Fail-closed admission/search for catalog-free, media-local ASR transcripts."""

from __future__ import annotations

import json
import math
import sqlite3
import stat
from pathlib import Path
from typing import Any

from .asr_result_importer import (
    MAX_RESULT_BYTES,
    _insert_exact_artifact,
    _insert_exact_processing_run,
    _insert_exact_run_input,
    _require_catalog_dependencies,
    _stable_read,
    _upsert_job,
    _validate_artifact_contents,
    validate_asr_whispercpp_result,
)
from .db import transaction
from .ids import stable_id
from .importers import canonical_json, sha256_bytes
from .result_importers import ResultImportError, validate_preprocess_result


SCHEMA_VERSION = 1
BRIDGE_VERSION = "media-local-asr-bridge/1"
IMPORTER_NAME = "media_local_asr_result_v1"
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
MAX_QUEUE_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_WORK_ORDER_BYTES = 4 * 1024 * 1024
MAX_PREPROCESS_RESULT_BYTES = 32 * 1024 * 1024

QUEUE_ID = "asrppqueue_66349d9b85c74f2376830edf2a7d4f0c"
QUEUE_IDENTITY_SHA256 = (
    "66349d9b85c74f2376830edf2a7d4f0ccf9d4e093f9c55b8127429259f2948d1"
)
QUEUE_MANIFEST_RAW_SHA256 = (
    "100d142cf459a663dd0f899db74b9666fbe49942d7ff0960280424899d39b5a0"
)
PREPROCESS_BUNDLE_ID = "ppbatch_a92965b935f16539966cd28d2491f89c"
PREPROCESS_BUNDLE_IDENTITY_SHA256 = (
    "a92965b935f16539966cd28d2491f89cb08fbe8f2fc6f717a7cf37da519468da"
)
PREPROCESS_BUNDLE_MANIFEST_RAW_SHA256 = (
    "8026750181a97dfde2d2297673d90c7a147357d73d3cda9c8d6fcc776b0184f4"
)
PREPROCESS_BUNDLE_MANIFEST_CANONICAL_SHA256 = (
    "a7946fcceb10a14f62ae44a8a7c97ac2c6c1cacbf0322a838929f9d146da764f"
)
SEAL_RECEIPT_PATH = (
    REPOSITORY_ROOT
    / "research"
    / "corpus"
    / "private-asr-results"
    / "sealing-control"
    / "receipts"
    / "asrsealreceipt_c0694c5c36586eb433a766490f3dbc01.json"
)
SEAL_RECEIPT_RAW_SHA256 = (
    "806439f2736dae9b95ffdffd19c3464c9efc39e79a5fbc21f7223b9f2c717b7b"
)
SEAL_RECEIPT_ID = "asrsealreceipt_c0694c5c36586eb433a766490f3dbc01"
SEAL_RECEIPT_IDENTITY_SHA256 = (
    "9f29212c38a78ff91faaea5dc7d8eb10f3d0405c0075ce8e365d4b33598df524"
)
SEALED_RESULT_FILENAMES = (
    "result.json",
    "transcript.normalized.json",
    "whisper.raw.json",
)
REVIEW_ONLY_ORDINALS = (1, 13)
PROCESS_RESULT_COUNT = 17
QUEUE_WORK_ORDER_COUNT = 19


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
        raise ResultImportError(f"{label} is not valid UTF-8 JSON: {error}") from error
    if not isinstance(value, dict):
        raise ResultImportError(f"{label} must contain a JSON object")
    return value


def _resolved_file(path_value: str | Path, label: str) -> Path:
    path = Path(path_value)
    if not path.is_absolute():
        path = path.resolve()
    try:
        resolved = path.resolve(strict=True)
        link = path.lstat()
    except (FileNotFoundError, OSError) as error:
        raise ResultImportError(f"{label} is not a current file: {error}") from error
    if resolved != path or stat.S_ISLNK(link.st_mode) or not stat.S_ISREG(link.st_mode):
        raise ResultImportError(f"{label} must be a resolved regular non-symlink file")
    return resolved


def _sealed_json(
    path_value: str | Path,
    label: str,
    *,
    maximum_bytes: int,
    expected_sha256: str | None = None,
    expected_byte_count: int | None = None,
    expected_mode: int | None = None,
) -> tuple[Path, bytes, dict[str, Any], str, str]:
    path = _resolved_file(path_value, label)
    link = path.lstat()
    if link.st_nlink != 1 or link.st_mode & 0o222:
        raise ResultImportError(f"{label} must be a sealed single-link file")
    if expected_mode is not None and stat.S_IMODE(link.st_mode) != expected_mode:
        raise ResultImportError(f"{label} mode differs from the sealed evidence")
    body = _stable_read(path, label, maximum_bytes=maximum_bytes)
    raw_sha = sha256_bytes(body)
    if expected_sha256 is not None and raw_sha != expected_sha256:
        raise ResultImportError(f"{label} SHA-256 differs from its sealed pin")
    if expected_byte_count is not None and len(body) != expected_byte_count:
        raise ResultImportError(f"{label} byte count differs from its sealed pin")
    value = _strict_json(body, label)
    canonical_sha = sha256_bytes(canonical_json(value).encode("utf-8"))
    return path, body, value, raw_sha, canonical_sha


def _require_result_seal(
    path: Path,
    body: bytes,
    raw: dict[str, Any],
) -> dict[str, Any]:
    receipt_path, receipt_body, receipt, receipt_raw_sha, _ = _sealed_json(
        SEAL_RECEIPT_PATH,
        "ASR result-store seal receipt",
        maximum_bytes=MAX_QUEUE_MANIFEST_BYTES,
        expected_sha256=SEAL_RECEIPT_RAW_SHA256,
        expected_mode=0o400,
    )
    if receipt_body != canonical_json(receipt).encode("utf-8") + b"\n":
        raise ResultImportError("ASR result-store seal receipt is not canonical")
    semantic = {
        key: value
        for key, value in receipt.items()
        if key not in {"identity_sha256", "receipt_id"}
    }
    if (
        receipt.get("schema_version") != 1
        or receipt.get("kind")
        != "asr_whispercpp_completed_result_seal_receipt"
        or receipt.get("state") != "applied_content_and_mtime_preserved"
        or receipt.get("receipt_id") != SEAL_RECEIPT_ID
        or receipt.get("identity_sha256") != SEAL_RECEIPT_IDENTITY_SHA256
        or sha256_bytes(canonical_json(semantic).encode("utf-8"))
        != SEAL_RECEIPT_IDENTITY_SHA256
        or receipt.get("result_count") != 25
    ):
        raise ResultImportError("ASR result-store seal receipt identity is not exact")
    policy = receipt.get("policy")
    if not isinstance(policy, dict) or (
        policy.get("allowed_entries") != list(SEALED_RESULT_FILENAMES)
        or policy.get("directory_mode_after") != 0o500
        or policy.get("file_mode_after") != 0o400
        or policy.get("hardlink_policy")
        != "all_three_files_must_have_nlink_1"
    ):
        raise ResultImportError("ASR result-store seal policy differs")
    results = receipt.get("results")
    if not isinstance(results, list) or len(results) != 25:
        raise ResultImportError("ASR result-store seal membership is malformed")
    matching = [row for row in results if row.get("result_path") == str(path)]
    if len(matching) != 1:
        raise ResultImportError("ASR result is absent from the exact seal receipt")
    member = matching[0]
    if (
        member.get("source_id") != QUEUE_ID
        or member.get("source_ordinal") in REVIEW_ONLY_ORDINALS
        or member.get("input_sha256") != raw.get("input", {}).get("sha256")
        or member.get("job_id") != raw.get("job_id")
        or member.get("result_key") != raw.get("result_key")
        or member.get("work_order_sha256") != raw.get("work_order_sha256")
    ):
        raise ResultImportError("ASR result seal membership differs from the envelope")
    directory = member.get("directory")
    files = member.get("files")
    if not isinstance(directory, dict) or not isinstance(files, list):
        raise ResultImportError("ASR result seal membership lacks filesystem evidence")
    parent = path.parent
    try:
        parent_link = parent.lstat()
        entries = sorted(item.name for item in parent.iterdir())
    except OSError as error:
        raise ResultImportError(f"sealed ASR result directory is unreadable: {error}") from error
    if (
        parent.resolve(strict=True) != parent
        or stat.S_ISLNK(parent_link.st_mode)
        or not stat.S_ISDIR(parent_link.st_mode)
        or stat.S_IMODE(parent_link.st_mode) != 0o500
        or parent_link.st_nlink != 1
        or entries != sorted(SEALED_RESULT_FILENAMES)
        or directory.get("path") != str(parent)
        or directory.get("mode_after") != 0o500
        or directory.get("nlink") != 1
        or directory.get("device") != parent_link.st_dev
        or directory.get("inode") != parent_link.st_ino
        or directory.get("mtime_ns") != parent_link.st_mtime_ns
        or directory.get("ctime_ns_after") != parent_link.st_ctime_ns
        or directory.get("content_entries_unchanged") is not True
        or directory.get("mtime_unchanged") is not True
    ):
        raise ResultImportError("ASR result directory differs from its exact seal receipt")
    if [item.get("name") for item in files if isinstance(item, dict)] != list(
        SEALED_RESULT_FILENAMES
    ):
        raise ResultImportError("ASR result seal file order differs")
    observed_bodies: dict[str, bytes] = {"result.json": body}
    for file_record in files:
        name = file_record["name"]
        file_path = parent / name
        resolved = _resolved_file(file_path, f"sealed ASR {name}")
        link = resolved.lstat()
        if name not in observed_bodies:
            observed_bodies[name] = _stable_read(
                resolved, f"sealed ASR {name}", maximum_bytes=MAX_RESULT_BYTES
            )
        file_body = observed_bodies[name]
        if (
            str(resolved) != file_record.get("path")
            or stat.S_IMODE(link.st_mode) != 0o400
            or link.st_nlink != 1
            or link.st_dev != file_record.get("device")
            or link.st_ino != file_record.get("inode")
            or link.st_mtime_ns != file_record.get("mtime_ns")
            or link.st_ctime_ns != file_record.get("ctime_ns_after")
            or len(file_body) != file_record.get("byte_count")
            or sha256_bytes(file_body) != file_record.get("sha256")
            or file_record.get("mode_after") != 0o400
            or file_record.get("nlink") != 1
            or file_record.get("content_unchanged") is not True
            or file_record.get("mtime_unchanged") is not True
        ):
            raise ResultImportError(f"ASR {name} differs from its exact seal receipt")
    artifacts = raw.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 2:
        raise ResultImportError("sealed ASR result must name exactly two artifacts")
    expected_artifacts = {
        Path(str(row.get("storage_uri", "")).removeprefix("file://")).name:
            row.get("sha256")
        for row in artifacts
    }
    if expected_artifacts != {
        name: sha256_bytes(observed_bodies[name])
        for name in SEALED_RESULT_FILENAMES
        if name != "result.json"
    }:
        raise ResultImportError("ASR artifact references differ from sealed files")
    return {
        "seal_receipt_uri": receipt_path.as_uri(),
        "seal_receipt_raw_sha256": receipt_raw_sha,
        "seal_receipt_id": receipt["receipt_id"],
        "seal_receipt_identity_sha256": receipt["identity_sha256"],
        "seal_receipt_ordinal": member["ordinal"],
        "sealed_result_directory_uri": parent.as_uri(),
    }


def _read_validated_result(
    path_value: str | Path,
) -> tuple[dict[str, Any], bytes, str, str, dict[str, Any]]:
    path = _resolved_file(path_value, "media-local ASR result")
    body = _stable_read(path, "media-local ASR result", maximum_bytes=MAX_RESULT_BYTES)
    raw = _strict_json(body, "media-local ASR result")
    seal = _require_result_seal(path, body, raw)
    result = validate_asr_whispercpp_result(raw, result_file_path=path)
    _validate_artifact_contents(result)
    if _require_result_seal(path, body, raw) != seal:
        raise ResultImportError("ASR result seal evidence changed during validation")
    return (
        result,
        body,
        sha256_bytes(body),
        sha256_bytes(canonical_json(raw).encode("utf-8")),
        seal,
    )


def _queue_identity(manifest: dict[str, Any]) -> str:
    keys = (
        "schema_version",
        "implementation_version",
        "materializer",
        "origin",
        "software",
        "engine",
        "model",
        "profile",
        "output",
        "work_orders",
        "safety",
    )
    if any(key not in manifest for key in keys):
        raise ResultImportError("sealed queue manifest lacks identity fields")
    return sha256_bytes(canonical_json({key: manifest[key] for key in keys}).encode("utf-8"))


def _read_exact_queue(manifest_path: str | Path) -> dict[str, Any]:
    path, body, manifest, raw_sha, _ = _sealed_json(
        manifest_path,
        "preprocess ASR queue manifest",
        maximum_bytes=MAX_QUEUE_MANIFEST_BYTES,
        expected_sha256=QUEUE_MANIFEST_RAW_SHA256,
        expected_mode=0o400,
    )
    if (
        manifest.get("schema_version") != 1
        or manifest.get("implementation_version") != "0.1.0"
        or manifest.get("materializer") != "himr-preprocess-asr-queue"
        or manifest.get("queue_id") != QUEUE_ID
        or manifest.get("identity_sha256") != QUEUE_IDENTITY_SHA256
        or manifest.get("queue_relative_path") != f"queues/{QUEUE_ID}"
        or manifest.get("work_order_count") != QUEUE_WORK_ORDER_COUNT
        or _queue_identity(manifest) != QUEUE_IDENTITY_SHA256
    ):
        raise ResultImportError("preprocess ASR queue identity/contract is not exact")
    origin = manifest.get("origin")
    profile = manifest.get("profile")
    safety = manifest.get("safety")
    if not isinstance(origin, dict) or origin.get("mode") != "sealed_preprocess_receipts":
        raise ResultImportError("media-local bridge requires a sealed-receipt queue")
    bundle = origin.get("preprocess_bundle")
    if not isinstance(bundle, dict) or (
        bundle.get("bundle_id") != PREPROCESS_BUNDLE_ID
        or bundle.get("identity_sha256") != PREPROCESS_BUNDLE_IDENTITY_SHA256
        or bundle.get("manifest_physical_sha256")
        != PREPROCESS_BUNDLE_MANIFEST_RAW_SHA256
        or bundle.get("manifest_sha256")
        != PREPROCESS_BUNDLE_MANIFEST_CANONICAL_SHA256
        or origin.get("receipt_count") != QUEUE_WORK_ORDER_COUNT
    ):
        raise ResultImportError("sealed queue preprocess-bundle lineage differs")
    if not isinstance(profile, dict) or (
        profile.get("coordinate_system") != "artifact_local_milliseconds"
        or profile.get("window_offset_ms") != 0
        or profile.get("recording_transform_state") != "unresolved"
        or profile.get("catalog_context_policy")
        != "always_null_until_separate_translation_admission"
    ):
        raise ResultImportError("sealed queue media-coordinate policy differs")
    if not isinstance(safety, dict) or (
        safety.get("catalog_writes") is not False
        or safety.get("identity_authority") != "none"
        or safety.get("publication_authority") != "none"
        or safety.get("timestamp_translation_authority") != "none"
        or safety.get("visibility") != "private"
    ):
        raise ResultImportError("sealed queue safety policy differs")

    bundle_path, _, bundle_manifest, bundle_raw_sha, _ = _sealed_json(
        bundle.get("manifest_path"),
        "preprocess bundle manifest",
        maximum_bytes=MAX_QUEUE_MANIFEST_BYTES,
        expected_sha256=PREPROCESS_BUNDLE_MANIFEST_RAW_SHA256,
        expected_mode=0o400,
    )
    manifest_without_digest = {
        key: value
        for key, value in bundle_manifest.items()
        if key != "manifest_sha256"
    }
    bundle_semantic_sha = sha256_bytes(
        canonical_json(manifest_without_digest).encode("utf-8")
    )
    if (
        str(bundle_path) != bundle.get("manifest_path")
        or bundle_raw_sha != bundle.get("manifest_physical_sha256")
        or bundle_semantic_sha != bundle.get("manifest_sha256")
        or bundle_manifest.get("manifest_sha256") != bundle_semantic_sha
        or bundle_manifest.get("bundle_id") != PREPROCESS_BUNDLE_ID
        or bundle_manifest.get("identity_sha256") != PREPROCESS_BUNDLE_IDENTITY_SHA256
    ):
        raise ResultImportError("preprocess bundle manifest changed after queue sealing")

    entries = manifest.get("work_orders")
    if not isinstance(entries, list) or len(entries) != QUEUE_WORK_ORDER_COUNT:
        raise ResultImportError("sealed queue work-order count differs")
    process_ordinals: list[int] = []
    review_ordinals: list[int] = []
    for index, entry in enumerate(entries, 1):
        if not isinstance(entry, dict) or (
            entry.get("ordinal") != index
            or entry.get("path") != f"work-orders/{index:06d}.json"
        ):
            raise ResultImportError("sealed queue ordinal/path order differs")
        routing = entry.get("routing_hint")
        if routing == "process":
            process_ordinals.append(index)
        elif routing == "review_near_silent_candidate":
            review_ordinals.append(index)
        else:
            raise ResultImportError("sealed queue has unsupported routing")
    if (
        tuple(review_ordinals) != REVIEW_ONLY_ORDINALS
        or len(process_ordinals) != PROCESS_RESULT_COUNT
    ):
        raise ResultImportError("sealed queue process/review partition differs")
    return {
        "path": path,
        "body": body,
        "manifest": manifest,
        "raw_sha256": raw_sha,
        "process_ordinals": process_ordinals,
    }


def _catalog_json(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, str):
        raise ResultImportError(f"{label} must contain catalog JSON")
    parsed = _strict_json(value.encode("utf-8"), label)
    if canonical_json(parsed) != value:
        raise ResultImportError(f"{label} must use canonical JSON")
    return parsed


def _require_preprocess_catalog_lineage(
    connection: sqlite3.Connection,
    *,
    result: dict[str, Any],
    entry: dict[str, Any],
) -> dict[str, Any]:
    pin = entry.get("preprocess_result")
    if not isinstance(pin, dict):
        raise ResultImportError("queue entry lacks preprocess result lineage")
    path, body, raw, raw_sha, canonical_sha = _sealed_json(
        pin.get("path"),
        "preprocess result",
        maximum_bytes=MAX_PREPROCESS_RESULT_BYTES,
        expected_sha256=pin.get("sha256"),
        expected_byte_count=pin.get("byte_count"),
        expected_mode=0o444,
    )
    preprocess = validate_preprocess_result(raw)
    run = preprocess["processing_run"]
    if (
        str(path) != pin.get("path")
        or path.as_uri() != pin.get("uri")
        or preprocess.get("result_path") != str(path)
        or preprocess.get("status") != "completed"
        or preprocess.get("dry_run") is not False
        or preprocess.get("errors") != []
        or run.get("processing_run_id") != pin.get("processing_run_id")
        or preprocess.get("job_id") != pin.get("job_id")
        or preprocess.get("layout", {}).get("recipe_sha256")
        != pin.get("recipe_sha256")
        or preprocess.get("routing", {}).get("routing_candidates", {}).get("asr")
        != "process"
    ):
        raise ResultImportError("preprocess result differs from the process-routed queue pin")
    audio_rows = [
        artifact
        for artifact in preprocess.get("artifacts", [])
        if isinstance(artifact, dict)
        and artifact.get("artifact_kind") == "audio_16khz_mono_flac"
    ]
    if len(audio_rows) != 1:
        raise ResultImportError("preprocess result lacks one normalized audio artifact")
    audio = audio_rows[0]
    audio_pin = entry.get("audio_artifact")
    if not isinstance(audio_pin, dict):
        raise ResultImportError("queue entry lacks an audio artifact pin")
    expected_audio = {
        "artifact_id": audio.get("artifact_id"),
        "artifact_kind": audio.get("artifact_kind"),
        "byte_count": audio.get("byte_count"),
        "duration_ms": audio.get("normalized_probe", {}).get("format", {}).get(
            "duration_ms"
        ),
        "media_id": audio.get("normalized_probe", {}).get("media", {}).get("media_id"),
        "normalized_probe_sha256": sha256_bytes(
            canonical_json(audio.get("normalized_probe")).encode("utf-8")
        ),
        "path": audio.get("path"),
        "processing_run_id": audio.get("processing_run_id"),
        "sha256": audio.get("sha256"),
        "uri": audio.get("storage_uri"),
        "visibility": audio.get("visibility"),
    }
    if audio_pin != expected_audio:
        raise ResultImportError("preprocess audio differs from the sealed queue entry")
    input_row = result["input"]
    if any(
        input_row[key] != value
        for key, value in {
            "artifact_id": audio_pin["artifact_id"],
            "byte_count": audio_pin["byte_count"],
            "media_id": audio_pin["media_id"],
            "parent_processing_run_id": audio_pin["processing_run_id"],
            "path": audio_pin["path"],
            "sha256": audio_pin["sha256"],
        }.items()
    ):
        raise ResultImportError("ASR input differs from its sealed preprocess audio")
    if input_row["probe"]["duration_ms"] != audio_pin["duration_ms"]:
        raise ResultImportError("ASR probe duration differs from preprocess evidence")

    catalog_run = connection.execute(
        """
        SELECT stage, implementation_version, parameters_json, environment_json,
               started_at, completed_at, status, error_text
        FROM processing_runs WHERE processing_run_id = ?
        """,
        (run["processing_run_id"],),
    ).fetchone()
    if catalog_run is None:
        raise ResultImportError("preprocess processing run is absent from the catalog")
    expected_run = {
        "stage": run["stage"],
        "implementation_version": run["implementation_version"],
        "parameters_json": canonical_json(run["parameters_json"]),
        "environment_json": canonical_json(run["environment_json"]),
        "started_at": run["started_at"],
        "completed_at": run["completed_at"],
        "status": run["status"],
        "error_text": None,
    }
    if any(catalog_run[key] != value for key, value in expected_run.items()):
        raise ResultImportError("catalog preprocess run differs from sealed evidence")

    batches = connection.execute(
        """
        SELECT import_batch_id, importer_version, started_at, completed_at, status
        FROM import_batches
        WHERE importer_name = 'media_preprocess_result_v1' AND input_sha256 = ?
        """,
        (canonical_sha,),
    ).fetchall()
    if len(batches) != 1:
        raise ResultImportError("preprocess result lacks one exact catalog admission batch")
    batch = batches[0]
    if (
        batch["importer_version"] != "0.2.0"
        or batch["started_at"] != run["started_at"]
        or batch["completed_at"] != run["completed_at"]
        or batch["status"] != "completed"
    ):
        raise ResultImportError("preprocess admission batch differs from sealed evidence")

    artifact = connection.execute(
        """
        SELECT processing_run_id, artifact_kind, storage_uri, sha256, byte_count,
               visibility, metadata_json
        FROM artifacts WHERE artifact_id = ?
        """,
        (audio_pin["artifact_id"],),
    ).fetchone()
    if artifact is None or any(
        artifact[key] != value
        for key, value in {
            "processing_run_id": audio_pin["processing_run_id"],
            "artifact_kind": audio_pin["artifact_kind"],
            "storage_uri": audio_pin["uri"],
            "sha256": audio_pin["sha256"],
            "byte_count": audio_pin["byte_count"],
            "visibility": "private",
        }.items()
    ):
        raise ResultImportError("catalog input artifact differs from sealed evidence")
    artifact_metadata = _catalog_json(
        artifact["metadata_json"], "preprocess input artifact metadata"
    )
    if artifact_metadata.get("normalized_probe") != audio.get("normalized_probe"):
        raise ResultImportError("catalog normalized probe differs from preprocess result")

    media = connection.execute(
        """
        SELECT sha256, byte_count, media_kind, duration_ms, integrity_state
        FROM media_objects WHERE media_id = ?
        """,
        (audio_pin["media_id"],),
    ).fetchone()
    if media is None or dict(media) != {
        "sha256": audio_pin["sha256"],
        "byte_count": audio_pin["byte_count"],
        "media_kind": "audio",
        "duration_ms": audio_pin["duration_ms"],
        "integrity_state": "verified",
    }:
        raise ResultImportError("catalog input media differs from sealed evidence")
    derivations = connection.execute(
        """
        SELECT parent_media_id, processing_run_id, metadata_json
        FROM media_derivations
        WHERE child_media_id = ?
          AND derivation_kind = 'audio_normalization_16khz_mono_flac'
        """,
        (audio_pin["media_id"],),
    ).fetchall()
    if len(derivations) != 1 or (
        derivations[0]["parent_media_id"] != preprocess["input"]["media_id"]
        or derivations[0]["processing_run_id"] != run["processing_run_id"]
        or _catalog_json(
            derivations[0]["metadata_json"], "preprocess media derivation metadata"
        )
        != {"channels": 1, "sample_format": "s16", "sample_rate_hz": 16000}
    ):
        raise ResultImportError("catalog media derivation differs from sealed evidence")
    return {
        "preprocess_result_uri": path.as_uri(),
        "preprocess_result_raw_sha256": raw_sha,
        "preprocess_result_canonical_sha256": canonical_sha,
        "preprocess_result_byte_count": len(body),
        "preprocess_import_batch_id": batch["import_batch_id"],
        "preprocess_processing_run_id": run["processing_run_id"],
        "preprocess_source_media_id": preprocess["input"]["media_id"],
        "audio_artifact": audio_pin,
    }


def _require_exact_media_lineage(
    connection: sqlite3.Connection,
    result: dict[str, Any],
    queue_manifest_path: str | Path,
) -> dict[str, Any]:
    if result["catalog_context"] is not None:
        raise ResultImportError("media-local ASR requires an explicitly null catalog_context")
    if result["window"]["offset_ms"] != 0 or (
        result["window"]["duration_ms"] != result["input"]["probe"]["duration_ms"]
    ):
        raise ResultImportError("media-local ASR must cover the full input from media zero")
    _require_catalog_dependencies(connection, result)
    queue = _read_exact_queue(queue_manifest_path)
    entries = queue["manifest"]["work_orders"]
    matching = [
        entry
        for entry in entries
        if entry.get("canonical_sha256") == result["work_order_sha256"]
        and entry.get("job_id") == result["_job_id"]
        and entry.get("audio_artifact", {}).get("sha256") == result["input"]["sha256"]
    ]
    if len(matching) != 1:
        raise ResultImportError("ASR result is not one exact member of the sealed queue")
    entry = matching[0]
    ordinal = entry["ordinal"]
    if ordinal in REVIEW_ONLY_ORDINALS or entry.get("routing_hint") != "process":
        raise ResultImportError("review-only queue work orders cannot enter media-local ASR")
    work_order_path = queue["path"].parent / entry["path"]
    order_path, order_body, order, order_raw_sha, order_canonical_sha = _sealed_json(
        work_order_path,
        f"queue work order {ordinal}",
        maximum_bytes=MAX_WORK_ORDER_BYTES,
        expected_sha256=entry.get("sha256"),
        expected_mode=0o400,
    )
    if order_canonical_sha != entry.get("canonical_sha256"):
        raise ResultImportError("queue work-order canonical digest differs")
    audio = entry["audio_artifact"]
    if (
        order.get("schema_version") != 1
        or order.get("job_id") != entry["job_id"]
        or order.get("catalog_context") is not None
        or order.get("glossary") is not None
        or order.get("window")
        != {"duration_ms": audio["duration_ms"], "offset_ms": 0}
        or order.get("input")
        != {
            "artifact_id": audio["artifact_id"],
            "expected_sha256": audio["sha256"],
            "media_id": audio["media_id"],
            "parent_processing_run_id": audio["processing_run_id"],
            "path": audio["path"],
        }
    ):
        raise ResultImportError("queue work order differs from its manifest entry")
    preprocess = _require_preprocess_catalog_lineage(
        connection, result=result, entry=entry
    )
    return {
        "queue_manifest_uri": queue["path"].as_uri(),
        "queue_manifest_raw_sha256": queue["raw_sha256"],
        "queue_identity_sha256": queue["manifest"]["identity_sha256"],
        "queue_id": queue["manifest"]["queue_id"],
        "queue_ordinal": ordinal,
        "routing_hint": "process",
        "work_order_uri": order_path.as_uri(),
        "work_order_raw_sha256": order_raw_sha,
        "work_order_canonical_sha256": order_canonical_sha,
        "work_order_byte_count": len(order_body),
        **preprocess,
    }


def _build_plan(
    connection: sqlite3.Connection,
    result_path: str | Path,
    queue_manifest_path: str | Path,
) -> dict[str, Any]:
    result, result_body, raw_sha, canonical_sha, seal = _read_validated_result(
        result_path
    )
    lineage = _require_exact_media_lineage(connection, result, queue_manifest_path)
    transcript = result["transcript"]
    run = result["processing_run"]
    media_id = result["input"]["media_id"]
    input_artifact_id = result["input"]["artifact_id"]
    revision_id = stable_id(
        "mltr", SCHEMA_VERSION, run["processing_run_id"], media_id, raw_sha
    )
    segment_rows: list[dict[str, Any]] = []
    word_rows: list[dict[str, Any]] = []
    max_segment_end = 0
    null_timed_words = 0
    input_duration = result["input"]["probe"]["duration_ms"]
    for segment in transcript["segments"]:
        segment_id = stable_id("mlts", revision_id, segment["ordinal"])
        max_segment_end = max(max_segment_end, segment["end_ms"])
        segment_metadata: dict[str, Any] = {
            "coordinate_provenance": {
                "coordinate_system": "media_ms",
                "recording_coordinates_asserted": False,
                "source_coordinates_asserted": False,
            },
            "engine_segment": json.loads(segment["metadata_json"]),
            "quality_flags": segment["quality_flags"],
            "window_overrun_ms": segment["window_overrun_ms"],
        }
        anomalies = [
            {
                "ordinal": token["ordinal"],
                "original_offsets": token.get("original_offsets"),
                "timing_quality_flags": token.get("timing_quality_flags"),
                "timing_state": token.get("timing_state"),
            }
            for token in segment["tokens"]
            if token.get("timing_state") == "unavailable"
        ]
        if anomalies:
            segment_metadata["token_timing_anomalies"] = anomalies
        segment_rows.append(
            {
                "media_local_segment_id": segment_id,
                "media_local_revision_id": revision_id,
                "ordinal": segment["ordinal"],
                "media_start_ms": segment["start_ms"],
                "media_end_ms": segment["end_ms"],
                "input_boundary_overrun_ms": max(
                    0, segment["end_ms"] - input_duration
                ),
                "text": segment["text"],
                "normalized_text": None,
                "speaker_label": None,
                "language": transcript["language"]["detected"],
                "confidence_band": None,
                "calibrated_probability": None,
                "metadata_json": canonical_json(segment_metadata),
            }
        )
        for token in segment["tokens"]:
            probability = token["raw_probability"]
            if token["start_ms"] is None:
                null_timed_words += 1
            token_metadata = {
                "engine_token": json.loads(token["metadata_json"]),
                "original_offsets": token.get("original_offsets"),
                "raw_dtw_timestamp": token["raw_dtw_timestamp"],
                "raw_probability": probability,
                "timing_quality_flags": token.get("timing_quality_flags"),
                "timing_state": token.get("timing_state"),
                "token_id": token["token_id"],
            }
            word_rows.append(
                {
                    "media_local_word_id": stable_id(
                        "mltw", segment_id, token["ordinal"]
                    ),
                    "media_local_segment_id": segment_id,
                    "ordinal": token["ordinal"],
                    "media_start_ms": token["start_ms"],
                    "media_end_ms": token["end_ms"],
                    "token": token["text"],
                    "normalized_token": None,
                    "asr_log_probability": (
                        math.log(probability) if probability > 0 else None
                    ),
                    "alignment_score": None,
                    "calibrated_probability": None,
                    "metadata_json": canonical_json(token_metadata),
                }
            )
    overrun = max(0, max_segment_end - input_duration)
    revision = {
        "media_local_revision_id": revision_id,
        "media_id": media_id,
        "input_artifact_id": input_artifact_id,
        "processing_run_id": run["processing_run_id"],
        "revision_kind": "contextual_asr" if result["glossary"] else "raw_asr",
        "origin": "whisper.cpp output-json-full",
        "language": transcript["language"]["detected"],
        "glossary_revision_id": run["glossary_revision_id"],
        "review_state": "machine",
        "coordinate_system": "media_ms",
        "boundary": "half_open",
        "input_duration_ms": input_duration,
        "requested_start_ms": 0,
        "requested_end_ms": input_duration,
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
                "producer_catalog_context": None,
                "quality_flags": transcript["quality_flags"],
                "queue_id": lineage["queue_id"],
                "queue_ordinal": lineage["queue_ordinal"],
                "raw_scores_preserved": True,
                "recording_coordinates_asserted": False,
                "source_coordinates_asserted": False,
            }
        ),
    }
    statistics = {
        "artifacts": 2,
        "media_local_import_receipts": 1,
        "media_local_revisions": 1,
        "media_local_segments": len(segment_rows),
        "media_local_words": len(word_rows),
        "null_timed_words": null_timed_words,
        "processing_runs": 1,
        "publication_decisions": 0,
        "recording_scoped_transcript_revisions": 0,
        "run_inputs": 1,
        "source_coordinate_rows": 0,
        "timeline_map_spans": 0,
    }
    core = {
        "schema_version": SCHEMA_VERSION,
        "bridge_version": BRIDGE_VERSION,
        "result": {
            "asr_result_uri": Path(result["result_path"]).resolve().as_uri(),
            "raw_sha256": raw_sha,
            "canonical_sha256": canonical_sha,
            "byte_count": len(result_body),
            "result_key": result["result_key"],
            "processing_run_id": run["processing_run_id"],
            "media_local_revision_id": revision_id,
        },
        "sealed_queue": {
            "queue_id": lineage["queue_id"],
            "queue_identity_sha256": lineage["queue_identity_sha256"],
            "queue_manifest_uri": lineage["queue_manifest_uri"],
            "queue_manifest_raw_sha256": lineage["queue_manifest_raw_sha256"],
            "queue_ordinal": lineage["queue_ordinal"],
            "routing_hint": lineage["routing_hint"],
            "review_only_ordinals": list(REVIEW_ONLY_ORDINALS),
            "work_order_uri": lineage["work_order_uri"],
            "work_order_raw_sha256": lineage["work_order_raw_sha256"],
            "work_order_canonical_sha256": lineage[
                "work_order_canonical_sha256"
            ],
        },
        "preprocess_lineage": {
            "preprocess_result_uri": lineage["preprocess_result_uri"],
            "preprocess_result_raw_sha256": lineage[
                "preprocess_result_raw_sha256"
            ],
            "preprocess_result_canonical_sha256": lineage[
                "preprocess_result_canonical_sha256"
            ],
            "preprocess_import_batch_id": lineage["preprocess_import_batch_id"],
            "preprocess_processing_run_id": lineage[
                "preprocess_processing_run_id"
            ],
            "preprocess_source_media_id": lineage["preprocess_source_media_id"],
            "input_media_id": media_id,
            "input_artifact_id": input_artifact_id,
            "input_sha256": result["input"]["sha256"],
        },
        "sealed_result": seal,
        "catalog_context": {
            "producer_catalog_context": None,
            "recording_id": None,
            "rendition_id": None,
            "source_id": None,
        },
        "coordinate_contract": {
            "coordinate_system": "media_ms",
            "boundary": "half_open",
            "input_duration_ms": input_duration,
            "requested_start_ms": 0,
            "requested_end_ms": input_duration,
            "max_segment_end_ms": max_segment_end,
            "input_boundary_overrun_ms": overrun,
            "null_timed_word_count": null_timed_words,
            "recording_coordinates_asserted": False,
            "source_coordinates_asserted": False,
            "recording_transform_state": "unasserted_catalog_context_null",
            "source_transform_state": "unasserted_catalog_context_null",
        },
        "statistics": statistics,
        "safety": {
            "credentials_used": False,
            "identity_authority": "none",
            "network_access_performed": False,
            "publication_authority": "none",
            "transcript_text_in_plan": False,
            "visibility": "private",
        },
    }
    plan_sha = sha256_bytes(canonical_json(core).encode("utf-8"))
    public = {
        "schema_version": SCHEMA_VERSION,
        "status": "validated",
        "plan_sha256": plan_sha,
        **{key: value for key, value in core.items() if key != "schema_version"},
    }
    import_batch_id = stable_id("imp", IMPORTER_NAME, canonical_sha)
    receipt = {
        "media_local_asr_import_id": stable_id(
            "mlai", SCHEMA_VERSION, raw_sha, revision_id
        ),
        "import_batch_id": import_batch_id,
        "media_local_revision_id": revision_id,
        "asr_result_uri": public["result"]["asr_result_uri"],
        "asr_result_raw_sha256": raw_sha,
        "asr_result_canonical_sha256": canonical_sha,
        "asr_result_byte_count": len(result_body),
        "queue_manifest_uri": lineage["queue_manifest_uri"],
        "queue_manifest_raw_sha256": lineage["queue_manifest_raw_sha256"],
        "queue_identity_sha256": lineage["queue_identity_sha256"],
        "queue_id": lineage["queue_id"],
        "queue_ordinal": lineage["queue_ordinal"],
        "routing_hint": lineage["routing_hint"],
        "work_order_uri": lineage["work_order_uri"],
        "work_order_raw_sha256": lineage["work_order_raw_sha256"],
        "work_order_canonical_sha256": lineage[
            "work_order_canonical_sha256"
        ],
        "preprocess_result_uri": lineage["preprocess_result_uri"],
        "preprocess_result_raw_sha256": lineage["preprocess_result_raw_sha256"],
        "preprocess_result_canonical_sha256": lineage[
            "preprocess_result_canonical_sha256"
        ],
        "preprocess_import_batch_id": lineage["preprocess_import_batch_id"],
        "seal_receipt_uri": seal["seal_receipt_uri"],
        "seal_receipt_raw_sha256": seal["seal_receipt_raw_sha256"],
        "seal_receipt_id": seal["seal_receipt_id"],
        "seal_receipt_identity_sha256": seal[
            "seal_receipt_identity_sha256"
        ],
        "seal_receipt_ordinal": seal["seal_receipt_ordinal"],
        "sealed_result_directory_uri": seal["sealed_result_directory_uri"],
        "input_media_id": media_id,
        "input_artifact_id": input_artifact_id,
        "input_duration_ms": input_duration,
        "max_segment_end_ms": max_segment_end,
        "input_boundary_overrun_ms": overrun,
        "null_timed_word_count": null_timed_words,
        "plan_sha256": plan_sha,
        "imported_at": run["completed_at"],
        "metadata_json": canonical_json(
            {
                "catalog_context": None,
                "recording_coordinates_asserted": False,
                "source_coordinates_asserted": False,
            }
        ),
    }
    return {
        "public": public,
        "result": result,
        "revision": revision,
        "segments": segment_rows,
        "words": word_rows,
        "receipt": receipt,
        "import_batch_id": import_batch_id,
    }


def build_media_local_asr_admission_plan(
    connection: sqlite3.Connection,
    result_path: str | Path,
    queue_manifest_path: str | Path,
) -> dict[str, Any]:
    """Return a text-free digest-gated plan for one exact process-routed result."""

    return _build_plan(connection, result_path, queue_manifest_path)["public"]


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
        f"SELECT {', '.join(columns)} FROM {table} WHERE {key_column} = ?",
        (key,),
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


def _insert_import_batch(connection: sqlite3.Connection, plan: dict[str, Any]) -> None:
    run = plan["result"]["processing_run"]
    row = {
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
        connection, table="import_batches", key_column="import_batch_id", row=row
    )


def import_media_local_asr_result(
    connection: sqlite3.Connection,
    result_path: str | Path,
    queue_manifest_path: str | Path,
    *,
    expected_plan_sha256: str,
) -> dict[str, Any]:
    """Revalidate and atomically admit one reviewed media-local plan."""

    preflight = _build_plan(connection, result_path, queue_manifest_path)
    if preflight["public"]["plan_sha256"] != expected_plan_sha256:
        raise ResultImportError(
            "media-local ASR plan changed or was not the separately reviewed plan"
        )
    with transaction(connection):
        plan = _build_plan(connection, result_path, queue_manifest_path)
        if plan["public"]["plan_sha256"] != expected_plan_sha256:
            raise ResultImportError("media-local ASR plan changed inside the transaction")
        _insert_import_batch(connection, plan)
        result = plan["result"]
        _insert_exact_processing_run(connection, result["processing_run"])
        _insert_exact_run_input(connection, result["run_input"])
        for artifact in result["artifacts"]:
            _insert_exact_artifact(connection, artifact)
        _insert_or_match(
            connection,
            table="media_local_transcript_revisions",
            key_column="media_local_revision_id",
            row=plan["revision"],
        )
        for segment in plan["segments"]:
            _insert_or_match(
                connection,
                table="media_local_transcript_segments",
                key_column="media_local_segment_id",
                row=segment,
            )
        for word in plan["words"]:
            _insert_or_match(
                connection,
                table="media_local_transcript_words",
                key_column="media_local_word_id",
                row=word,
            )
        _insert_or_match(
            connection,
            table="media_local_asr_imports",
            key_column="media_local_asr_import_id",
            row=plan["receipt"],
        )
        _upsert_job(connection, result)
    return {**plan["public"], "status": "admitted"}


def search_media_local_transcripts(
    connection: sqlite3.Connection,
    query: str,
    *,
    limit: int = 25,
    media_id: str | None = None,
) -> dict[str, Any]:
    """Search private media-local text without source/recording promotion."""

    if not isinstance(query, str) or not query.strip() or "\x00" in query or len(query) > 1_000:
        raise ResultImportError("media-local search query must be a bounded string")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 200:
        raise ResultImportError("media-local search limit must be between 1 and 200")
    parameters: list[Any] = [query]
    media_clause = ""
    if media_id is not None:
        if not media_id or len(media_id) > 256:
            raise ResultImportError("media-local media filter is invalid")
        media_clause = "AND revision.media_id = ?"
        parameters.append(media_id)
    parameters.append(limit)
    try:
        rows = connection.execute(
            f"""
            SELECT segment.media_local_segment_id,
                   segment.media_local_revision_id,
                   revision.media_id, revision.input_artifact_id,
                   segment.media_start_ms, segment.media_end_ms,
                   segment.input_boundary_overrun_ms, segment.text,
                   segment.speaker_label, segment.language,
                   bm25(media_local_transcript_fts) AS rank
            FROM media_local_transcript_fts
            JOIN media_local_transcript_segments AS segment
              ON segment.media_local_segment_id =
                 media_local_transcript_fts.media_local_segment_id
            JOIN media_local_transcript_revisions AS revision
              ON revision.media_local_revision_id = segment.media_local_revision_id
            WHERE media_local_transcript_fts MATCH ?
              {media_clause}
            ORDER BY rank, segment.media_local_revision_id, segment.ordinal
            LIMIT ?
            """,
            tuple(parameters),
        ).fetchall()
    except sqlite3.OperationalError as error:
        raise ResultImportError(f"media-local full-text query is invalid: {error}") from error
    return {
        "query": query,
        "limit": limit,
        "media_id": media_id,
        "coordinate_system": "media_ms",
        "recording_transform_state": "unasserted_catalog_context_null",
        "source_transform_state": "unasserted_catalog_context_null",
        "result_count": len(rows),
        "results": [
            {
                **dict(row),
                "source_start_ms": None,
                "source_end_ms": None,
                "recording_id": None,
                "rendition_id": None,
                "recording_start_ms": None,
                "recording_end_ms": None,
            }
            for row in rows
        ],
    }
