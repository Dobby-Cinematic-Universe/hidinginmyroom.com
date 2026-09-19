#!/usr/bin/env python3
"""Deterministic, private batch materializer for raw whisper.cpp ASR windows.

The materializer consumes only completed, sealed local-window ``result.json``
files and an explicitly read-only corpus catalog.  It emits immutable work
orders for :mod:`asr_whispercpp`; it does not transcribe, publish, or write to
the catalog.  The runner replays the complete admission proof before and after
dispatching work orders in their sealed ordinal order.
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
import sqlite3
import stat
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any, Iterable

import asr_whispercpp
import whispercpp_engine_profiles


CONTRACT_VERSION = 1
IMPLEMENTATION_VERSION = "0.2.0"
LEGACY_IMPLEMENTATION_VERSION = "0.1.0"
MATERIALIZER_NAME = "himr-asr-whispercpp-raw-batch"

MAX_RESULTS = 64
MAX_RESULT_BYTES = 16 * 1024 * 1024
MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_WORK_ORDER_BYTES = 4 * 1024 * 1024
MAX_JSON_DEPTH = 128
MAX_PATH_CHARACTERS = 4_096
MAX_ARTIFACT_BYTES = 16 * 1024 * 1024 * 1024
MAX_TOTAL_AUDIO_BYTES = 256 * 1024 * 1024 * 1024
MAX_TOTAL_AUDIO_MS = 7 * 24 * 60 * 60 * 1_000
MAX_DATABASE_BYTES = 16 * 1024 * 1024 * 1024

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
BATCH_ID_RE = re.compile(r"^asrbatch_[0-9a-f]{32}$")
BUNDLE_ID_RE = re.compile(r"^windowbundle_[0-9a-f]{32}$")
WINDOW_ID_RE = re.compile(r"^window_[0-9]{6}$")

LEGACY_SOFTWARE_PROFILE = {
    "materializer": {
        "name": MATERIALIZER_NAME,
        "implementation_version": LEGACY_IMPLEMENTATION_VERSION,
        "sha256": "c940b93d11a22f057c4c244d0d17d0e933050f9dafe8f1c5ef8ede2bcf971df5",
        "byte_count": 71_128,
    },
    "asr_adapter": {
        "name": "himr-asr-whispercpp",
        "contract_version": 1,
        "implementation_version": "0.2.3",
        "sha256": "781f481a0f5ff155417795640ae089d484db0f56e7ec8e9e6e8375ef46c260f0",
        "byte_count": 57_311,
    },
}

EXPECTED_MODEL_SHA256 = (
    "c6138d6d58ecc8322097e0f987c32f1be8bb0a18532a3f88f734d1bbf9c41e5d"
)
EXPECTED_MODEL_BYTE_COUNT = 487_614_201
EXPECTED_MODEL = {
    "model_id": "model_whispercpp_small_en_c6138d6d58ec",
    "task": "asr",
    "name": "Whisper small.en ggml",
    "revision": "ggerganov/whisper.cpp@5359861c739e955e79d9a303bcbc70fb988958b1",
    "source": "https://huggingface.co/ggerganov/whisper.cpp/blob/5359861c739e955e79d9a303bcbc70fb988958b1/ggml-small.en.bin",
    "license_label": "MIT (model repository card reviewed 2026-08-26)",
}
EXPECTED_REGISTRY = {
    "manifest_id": "model-registry-whisper-small-en-c6138d6d-2026-08-26",
    "input_sha256": "9509fd279bc316c33e138f7a50ddfb77a170f9211b6a3f428eaa460add78641c",
    "schema_version": 1,
    "manifest_created_at": "2026-08-26T20:26:04Z",
    "imported_at": "2026-08-26T20:27:14Z",
    "registered_by": "local HIMR corpus maintainer",
    "basis": "Exact local weights hashed before registration; immutable upstream revision, source URL, and model-repository license label copied from the reviewed ASR work order.",
    "model_count": 1,
}

RAW_INFERENCE = {
    "language": "en",
    "threads": 6,
    "translate": False,
    "split_on_word": True,
    "best_of": 5,
    "beam_size": 5,
    "max_segment_characters": 0,
    "word_threshold": 0.01,
    "entropy_threshold": 2.4,
    "logprob_threshold": -1.0,
    "no_speech_threshold": 0.6,
    "temperature": 0.0,
    "temperature_increment": 0.2,
    "no_fallback": False,
    "timeout_seconds": 7_200,
}

RESULT_KEYS = {
    "schema_version",
    "implementation_version",
    "status",
    "dry_run",
    "job_id",
    "bundle_id",
    "work_order_sha256",
    "source",
    "window",
    "tools",
    "profile",
    "limits",
    "commands",
    "artifacts",
    "time_mapping",
    "safety",
    "result_path",
}
WINDOW_KEYS = {
    "boundary",
    "end_ms",
    "is_partial_tail",
    "ordinal",
    "start_ms",
    "window_id",
}
ARTIFACT_KEYS = {
    "artifact_id",
    "artifact_kind",
    "byte_count",
    "normalized_probe",
    "path",
    "sha256",
    "visibility",
}
EXPECTED_SAFETY = {
    "credentials_allowed": False,
    "identity_claims_allowed": False,
    "network_allowed": False,
    "publication_authority": "none",
    "remote_section_download": False,
    "source_bytes_preserved": True,
}


class BatchError(RuntimeError):
    """A batch contract, catalog-lineage, or integrity failure."""


class BatchRunFailure(BatchError):
    """A fail-fast dispatch error carrying the schema-valid partial run summary."""

    def __init__(self, result: dict[str, Any]):
        failed = result["failed_job"]
        super().__init__(
            f"ASR job {failed['ordinal']}/{result['job_count']} "
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
        raise BatchError(f"value cannot be represented as strict canonical JSON: {error}") from error


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
        raise BatchError(f"value cannot be represented as strict JSON: {error}") from error


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _reject_constant(value: str) -> None:
    raise BatchError(f"non-finite JSON constant is forbidden: {value}")


def _reject_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise BatchError(f"JSON object contains duplicate key: {key}")
        result[key] = value
    return result


def _check_json_depth(value: Any, depth: int = 0) -> None:
    if depth > MAX_JSON_DEPTH:
        raise BatchError(f"JSON nesting exceeds the {MAX_JSON_DEPTH}-level limit")
    if isinstance(value, dict):
        for child in value.values():
            _check_json_depth(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            _check_json_depth(child, depth + 1)
    elif isinstance(value, float) and not math.isfinite(value):
        raise BatchError("non-finite JSON number is forbidden")


def parse_json(body: bytes, label: str) -> Any:
    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=_reject_pairs,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BatchError(f"{label} is not strict UTF-8 JSON: {error}") from error
    _check_json_depth(value)
    return value


def exact_object(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BatchError(f"{label} must be a JSON object")
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    if missing or unknown:
        raise BatchError(
            f"{label} keys differ from the exact contract; missing={missing}, unknown={unknown}"
        )
    return value


def bounded_string(value: Any, label: str, maximum: int = 8_192) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise BatchError(f"{label} must be a non-empty bounded string")
    return value


def identifier(value: Any, label: str) -> str:
    text = bounded_string(value, label, 256)
    if not IDENTIFIER_RE.fullmatch(text):
        raise BatchError(f"{label} contains unsupported identifier characters")
    return text


def sha256_value(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise BatchError(f"{label} must be a lowercase SHA-256")
    return value


def integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise BatchError(f"{label} must be an integer between {minimum} and {maximum}")
    return value


def _path_text(value: Any, label: str) -> Path:
    text = bounded_string(value, label, MAX_PATH_CHARACTERS)
    if "://" in text or "\n" in text or "\r" in text:
        raise BatchError(f"{label} must be an absolute local path")
    path = Path(text)
    if not path.is_absolute():
        raise BatchError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=False)
    except (OSError, RuntimeError) as error:
        raise BatchError(f"{label} cannot be resolved safely: {error}") from error
    if resolved != path:
        raise BatchError(f"{label} must already be normalized and resolved")
    return path


def existing_regular_file(value: Any, label: str, *, executable: bool = False) -> Path:
    path = _path_text(value, label)
    try:
        link_stat = path.lstat()
    except OSError as error:
        raise BatchError(f"{label} cannot be inspected: {error}") from error
    if stat.S_ISLNK(link_stat.st_mode) or not stat.S_ISREG(link_stat.st_mode):
        raise BatchError(f"{label} must be a regular non-symlink file")
    if executable and not os.access(path, os.X_OK):
        raise BatchError(f"{label} must be executable")
    return path


def private_directory_path(value: Any, label: str) -> Path:
    path = _path_text(value, label)
    if path == Path("/"):
        raise BatchError(f"{label} may not be the filesystem root")
    for forbidden in (Path("/tmp"), Path("/var/tmp")):
        if path == forbidden or forbidden in path.parents:
            raise BatchError(f"{label} may not be under {forbidden}")
    if path.exists():
        link_stat = path.lstat()
        if stat.S_ISLNK(link_stat.st_mode) or not stat.S_ISDIR(link_stat.st_mode):
            raise BatchError(f"{label} must be a non-symlink directory or a new path")
        if stat.S_IMODE(link_stat.st_mode) & 0o077:
            raise BatchError(f"{label} must not grant group or world permissions")
    return path


def _fingerprint(value: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        stat.S_IFMT(value.st_mode),
        value.st_nlink,
    )


def _open_stable(path: Path, label: str) -> tuple[int, tuple[int, int, int, int, int, int, int]]:
    try:
        before = path.lstat()
    except OSError as error:
        raise BatchError(f"{label} cannot be inspected: {error}") from error
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise BatchError(f"{label} must be a regular non-symlink file")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise BatchError(f"{label} cannot be opened safely: {error}") from error
    observed = os.fstat(descriptor)
    if not stat.S_ISREG(observed.st_mode) or _fingerprint(observed) != _fingerprint(before):
        os.close(descriptor)
        raise BatchError(f"{label} was replaced while opening")
    return descriptor, _fingerprint(observed)


def _verify_path_fingerprint(
    path: Path,
    descriptor: int,
    identity: tuple[int, int, int, int, int, int, int],
    label: str,
) -> None:
    descriptor_after = os.fstat(descriptor)
    try:
        path_after = path.lstat()
    except OSError as error:
        raise BatchError(f"{label} disappeared during inspection: {error}") from error
    if (
        stat.S_ISLNK(path_after.st_mode)
        or _fingerprint(descriptor_after) != identity
        or _fingerprint(path_after) != identity
    ):
        raise BatchError(f"{label} changed during inspection")


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
        descriptor_stat = os.fstat(descriptor)
        if exact_mode is not None and stat.S_IMODE(descriptor_stat.st_mode) != exact_mode:
            raise BatchError(f"{label} mode must be exactly {exact_mode:04o}")
        if require_single_link and descriptor_stat.st_nlink != 1:
            raise BatchError(f"{label} must have exactly one hard link")
        if identity[2] > maximum_bytes:
            raise BatchError(f"{label} exceeds the {maximum_bytes}-byte limit")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, maximum_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > maximum_bytes:
                raise BatchError(f"{label} exceeds the {maximum_bytes}-byte limit")
        _verify_path_fingerprint(path, descriptor, identity, label)
    finally:
        os.close(descriptor)
    body = b"".join(chunks)
    if len(body) != identity[2]:
        raise BatchError(f"{label} produced a short read")
    return body


def stable_hash(
    path: Path,
    label: str,
    *,
    expected_sha256: str | None = None,
    expected_byte_count: int | None = None,
    exact_mode: int | None = None,
    maximum_bytes: int = MAX_ARTIFACT_BYTES,
    require_single_link: bool = False,
) -> tuple[str, int]:
    descriptor, identity = _open_stable(path, label)
    try:
        descriptor_stat = os.fstat(descriptor)
        if exact_mode is not None and stat.S_IMODE(descriptor_stat.st_mode) != exact_mode:
            raise BatchError(f"{label} mode must be exactly {exact_mode:04o}")
        if require_single_link and descriptor_stat.st_nlink != 1:
            raise BatchError(f"{label} must have exactly one hard link")
        if identity[2] > maximum_bytes:
            raise BatchError(f"{label} exceeds the {maximum_bytes}-byte limit")
        if expected_byte_count is not None and identity[2] != expected_byte_count:
            raise BatchError(f"{label} byte count differs from its pin")
        digest = hashlib.sha256()
        total = 0
        while chunk := os.read(descriptor, 8 * 1024 * 1024):
            digest.update(chunk)
            total += len(chunk)
        _verify_path_fingerprint(path, descriptor, identity, label)
    finally:
        os.close(descriptor)
    observed = digest.hexdigest()
    if total != identity[2]:
        raise BatchError(f"{label} produced a short read")
    if expected_sha256 is not None and observed != expected_sha256:
        raise BatchError(f"{label} SHA-256 differs from its pin")
    return observed, identity[2]


def _strict_database_json(value: Any, label: str) -> Any:
    if not isinstance(value, str) or len(value.encode("utf-8")) > MAX_RESULT_BYTES:
        raise BatchError(f"{label} is not bounded catalog JSON")
    parsed = parse_json(value.encode("utf-8"), label)
    if not isinstance(parsed, dict):
        raise BatchError(f"{label} must contain a JSON object")
    return parsed


def _row_snapshot(row: sqlite3.Row, json_columns: Iterable[str] = ()) -> dict[str, Any]:
    result = dict(row)
    for column in json_columns:
        _strict_database_json(result[column], f"catalog {column}")
    return result


def _readonly_authorizer(action: int, _one: str | None, _two: str | None, _db: str | None, _trigger: str | None) -> int:
    denied = {
        sqlite3.SQLITE_INSERT,
        sqlite3.SQLITE_UPDATE,
        sqlite3.SQLITE_DELETE,
        sqlite3.SQLITE_CREATE_INDEX,
        sqlite3.SQLITE_CREATE_TABLE,
        sqlite3.SQLITE_CREATE_TEMP_INDEX,
        sqlite3.SQLITE_CREATE_TEMP_TABLE,
        sqlite3.SQLITE_CREATE_TEMP_TRIGGER,
        sqlite3.SQLITE_CREATE_TEMP_VIEW,
        sqlite3.SQLITE_CREATE_TRIGGER,
        sqlite3.SQLITE_CREATE_VIEW,
        sqlite3.SQLITE_DROP_INDEX,
        sqlite3.SQLITE_DROP_TABLE,
        sqlite3.SQLITE_DROP_TEMP_INDEX,
        sqlite3.SQLITE_DROP_TEMP_TABLE,
        sqlite3.SQLITE_DROP_TEMP_TRIGGER,
        sqlite3.SQLITE_DROP_TEMP_VIEW,
        sqlite3.SQLITE_DROP_TRIGGER,
        sqlite3.SQLITE_DROP_VIEW,
        sqlite3.SQLITE_ALTER_TABLE,
        sqlite3.SQLITE_ATTACH,
        sqlite3.SQLITE_DETACH,
        sqlite3.SQLITE_REINDEX,
        sqlite3.SQLITE_ANALYZE,
    }
    return sqlite3.SQLITE_DENY if action in denied else sqlite3.SQLITE_OK


def open_readonly_catalog(path: Path) -> sqlite3.Connection:
    existing_regular_file(str(path), "catalog database")
    before = path.lstat()
    if before.st_size <= 0 or before.st_size > MAX_DATABASE_BYTES:
        raise BatchError(f"catalog database must be between 1 and {MAX_DATABASE_BYTES} bytes")
    try:
        connection = sqlite3.connect(
            f"{path.as_uri()}?mode=ro",
            uri=True,
            isolation_level=None,
            timeout=30,
        )
    except sqlite3.Error as error:
        raise BatchError(f"cannot open catalog read-only: {error}") from error
    connection.row_factory = sqlite3.Row
    try:
        after_open = path.lstat()
        if stat.S_ISLNK(after_open.st_mode) or _fingerprint(after_open) != _fingerprint(before):
            raise BatchError("catalog database was replaced while opening the read-only snapshot")
        connection.execute("PRAGMA query_only = ON")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.set_authorizer(_readonly_authorizer)
        connection.execute("BEGIN")
    except BatchError:
        connection.close()
        raise
    except (sqlite3.Error, OSError) as error:
        connection.close()
        raise BatchError(f"cannot establish a read-only catalog snapshot: {error}") from error
    return connection


def one_row(connection: sqlite3.Connection, query: str, parameters: tuple[Any, ...], label: str) -> sqlite3.Row:
    try:
        rows = connection.execute(query, parameters).fetchall()
    except sqlite3.Error as error:
        raise BatchError(f"cannot read {label}: {error}") from error
    if len(rows) != 1:
        raise BatchError(f"{label} must resolve to exactly one catalog row; observed {len(rows)}")
    return rows[0]


def all_rows(connection: sqlite3.Connection, query: str, parameters: tuple[Any, ...], label: str) -> list[sqlite3.Row]:
    try:
        return connection.execute(query, parameters).fetchall()
    except sqlite3.Error as error:
        raise BatchError(f"cannot read {label}: {error}") from error


def _validate_local_window_result(result_path: Path) -> dict[str, Any]:
    result_path = existing_regular_file(str(result_path), "local-window result")
    if result_path.name != "result.json":
        raise BatchError("local-window result path must end in result.json")
    result_dir = result_path.parent
    try:
        directory_stat = result_dir.lstat()
    except OSError as error:
        raise BatchError(f"local-window result directory cannot be inspected: {error}") from error
    if (
        stat.S_ISLNK(directory_stat.st_mode)
        or not stat.S_ISDIR(directory_stat.st_mode)
        or stat.S_IMODE(directory_stat.st_mode) != 0o500
    ):
        raise BatchError("local-window result directory must be a non-symlink directory with mode 0500")

    result_body = stable_read(
        result_path,
        "local-window result",
        maximum_bytes=MAX_RESULT_BYTES,
        exact_mode=0o400,
        require_single_link=True,
    )
    result_sha256 = sha256_bytes(result_body)
    result = exact_object(parse_json(result_body, "local-window result"), "local-window result", RESULT_KEYS)
    if (
        result["schema_version"] != 1
        or result["implementation_version"] != "0.1.0"
        or result["status"] != "completed"
        or result["dry_run"] is not False
    ):
        raise BatchError("local-window result is not a supported completed producer result")
    identifier(result["job_id"], "local-window result.job_id")
    bundle_id = bounded_string(result["bundle_id"], "local-window result.bundle_id", 64)
    if not BUNDLE_ID_RE.fullmatch(bundle_id):
        raise BatchError("local-window result.bundle_id is malformed")
    sha256_value(result["work_order_sha256"], "local-window result.work_order_sha256")
    if result["result_path"] != str(result_path):
        raise BatchError("local-window result.result_path does not match the supplied path")
    if result["safety"] != EXPECTED_SAFETY:
        raise BatchError("local-window result safety policy is not the private offline contract")

    window = exact_object(result["window"], "local-window result.window", WINDOW_KEYS)
    ordinal = integer(window["ordinal"], "local-window result.window.ordinal", 1, 999_999)
    start_ms = integer(window["start_ms"], "local-window result.window.start_ms", 0, MAX_TOTAL_AUDIO_MS)
    end_ms = integer(window["end_ms"], "local-window result.window.end_ms", 1, MAX_TOTAL_AUDIO_MS)
    if end_ms <= start_ms or end_ms - start_ms > asr_whispercpp.MAX_WINDOW_MS:
        raise BatchError("local-window result window has an invalid span")
    if window["boundary"] != "half_open" or not isinstance(window["is_partial_tail"], bool):
        raise BatchError("local-window result window boundary/tail state is malformed")
    window_id = bounded_string(window["window_id"], "local-window result.window.window_id", 64)
    if not WINDOW_ID_RE.fullmatch(window_id) or window_id != f"window_{ordinal:06d}":
        raise BatchError("local-window result window identifier/ordinal is inconsistent")
    expected_mapping = {
        "artifact_zero_maps_to_source_ms": start_ms,
        "boundary": "half_open",
        "byte_exact_source_fragment": False,
        "coordinate_precision": "integer_millisecond_contract",
        "extraction_method": "ffmpeg_accurate_seek_transcode",
        "source_end_ms": end_ms,
        "source_start_ms": start_ms,
    }
    if result["time_mapping"] != expected_mapping:
        raise BatchError("local-window result source-time mapping is inconsistent")

    artifacts = result["artifacts"]
    if not isinstance(artifacts, list) or not 1 <= len(artifacts) <= 2:
        raise BatchError("local-window result must contain one or two artifacts")
    parsed_artifacts: list[dict[str, Any]] = []
    kinds: set[str] = set()
    expected_entries = {"result.json"}
    for index, raw in enumerate(artifacts):
        artifact = exact_object(raw, f"local-window result.artifacts[{index}]", ARTIFACT_KEYS)
        kind = bounded_string(artifact["artifact_kind"], f"artifact[{index}].artifact_kind", 128)
        if kind not in {"window_audio_16khz_mono_flac", "window_low_resolution_cfr_proxy"} or kind in kinds:
            raise BatchError("local-window result has an unsupported or duplicate artifact kind")
        kinds.add(kind)
        digest = sha256_value(artifact["sha256"], f"artifact[{index}].sha256")
        byte_count = integer(artifact["byte_count"], f"artifact[{index}].byte_count", 1, MAX_ARTIFACT_BYTES)
        artifact_id = identifier(artifact["artifact_id"], f"artifact[{index}].artifact_id")
        expected_id = "artifact_" + hashlib.sha256(
            "\x1f".join((bundle_id, window_id, kind, digest)).encode("utf-8")
        ).hexdigest()[:32]
        if artifact_id != expected_id or artifact["visibility"] != "private":
            raise BatchError("local-window artifact deterministic identity/visibility is inconsistent")
        artifact_path = existing_regular_file(artifact["path"], f"artifact[{index}].path")
        if artifact_path.parent != result_dir or artifact_path.name in expected_entries:
            raise BatchError("local-window artifact path escapes or duplicates the sealed result directory")
        expected_entries.add(artifact_path.name)
        stable_hash(
            artifact_path,
            f"local-window artifact {artifact_id}",
            expected_sha256=digest,
            expected_byte_count=byte_count,
            exact_mode=0o400,
            require_single_link=True,
        )
        probe = artifact["normalized_probe"]
        if not isinstance(probe, dict):
            raise BatchError("local-window artifact normalized probe must be an object")
        parsed_artifacts.append({**artifact, "path": str(artifact_path)})

    try:
        observed_entries = {item.name for item in result_dir.iterdir()}
    except OSError as error:
        raise BatchError(f"cannot enumerate sealed local-window result: {error}") from error
    if observed_entries != expected_entries:
        raise BatchError("sealed local-window result directory has missing or extra entries")
    audio = next((item for item in parsed_artifacts if item["artifact_kind"] == "window_audio_16khz_mono_flac"), None)
    if audio is None:
        raise BatchError("local-window result has no normalized audio artifact")
    audio_probe = exact_object(
        audio["normalized_probe"],
        "local-window normalized audio probe",
        {"audio", "audio_stream_index", "duration_ms", "video", "video_stream_index"},
    )
    if audio_probe["video"] is not None or audio_probe["video_stream_index"] is not None:
        raise BatchError("normalized local-window audio unexpectedly contains video")
    if audio_probe["audio_stream_index"] != 0 or audio_probe["audio"] != {
        "channels": 1,
        "codec_name": "flac",
        "sample_format": "s16",
        "sample_rate_hz": 16_000,
    }:
        raise BatchError("local-window audio is not normalized 16 kHz mono s16 FLAC")
    duration_ms = integer(
        audio_probe["duration_ms"],
        "local-window normalized audio duration_ms",
        1,
        asr_whispercpp.MAX_WINDOW_MS,
    )
    if abs(duration_ms - (end_ms - start_ms)) > 100:
        raise BatchError("local-window audio duration differs materially from the source window span")

    # Re-read the exact result after artifact hashing so replacements during the
    # potentially long hash pass cannot silently change the controlling envelope.
    if stable_read(
        result_path,
        "local-window result replay",
        maximum_bytes=MAX_RESULT_BYTES,
        exact_mode=0o400,
        require_single_link=True,
    ) != result_body:
        raise BatchError("local-window result changed while its artifacts were verified")

    return {
        "path": str(result_path),
        "uri": result_path.as_uri(),
        "sha256": result_sha256,
        "byte_count": len(result_body),
        "job_id": result["job_id"],
        "bundle_id": bundle_id,
        "work_order_sha256": result["work_order_sha256"],
        "window": window,
        "source_time_mapping": expected_mapping,
        "audio": audio,
    }


def _validate_model_registry(connection: sqlite3.Connection, model_path: Path) -> dict[str, Any]:
    model_row = one_row(
        connection,
        "SELECT * FROM models WHERE model_id = ?",
        (EXPECTED_MODEL["model_id"],),
        "pinned ASR model",
    )
    model = _row_snapshot(model_row, ("configuration_json",))
    model_configuration = _strict_database_json(
        model["configuration_json"], "catalog model.configuration_json"
    )
    expected_model_row = {
        "model_id": EXPECTED_MODEL["model_id"],
        "task": EXPECTED_MODEL["task"],
        "name": EXPECTED_MODEL["name"],
        "version": EXPECTED_MODEL["revision"],
        "weights_sha256": EXPECTED_MODEL_SHA256,
        "license_label": EXPECTED_MODEL["license_label"],
        "configuration_json": model["configuration_json"],
    }
    if model != expected_model_row or model_configuration != {"source": EXPECTED_MODEL["source"]}:
        raise BatchError("catalog model row differs from the pinned small.en identity")

    registry_rows = all_rows(
        connection,
        """
        SELECT i.*, mm.ordinal, mm.model_id, mm.model_snapshot_json
        FROM model_registry_manifest_imports AS i
        JOIN model_registry_manifest_models AS mm ON mm.manifest_id = i.manifest_id
        WHERE mm.model_id = ?
        ORDER BY i.manifest_id, mm.ordinal
        """,
        (EXPECTED_MODEL["model_id"],),
        "pinned ASR model registry binding",
    )
    if len(registry_rows) != 1:
        raise BatchError("pinned small.en model must have exactly one registry-manifest binding")
    registry = _row_snapshot(registry_rows[0], ("model_snapshot_json",))
    snapshot = _strict_database_json(
        registry["model_snapshot_json"], "catalog registry.model_snapshot_json"
    )
    expected_snapshot = {
        "configuration_json": {"source": EXPECTED_MODEL["source"]},
        "license_label": EXPECTED_MODEL["license_label"],
        "model_id": EXPECTED_MODEL["model_id"],
        "name": EXPECTED_MODEL["name"],
        "task": EXPECTED_MODEL["task"],
        "version": EXPECTED_MODEL["revision"],
        "weights_byte_count": EXPECTED_MODEL_BYTE_COUNT,
        "weights_sha256": EXPECTED_MODEL_SHA256,
        "weights_uri": model_path.as_uri(),
    }
    expected_registry = {
        **EXPECTED_REGISTRY,
        "ordinal": 0,
        "model_id": EXPECTED_MODEL["model_id"],
        "model_snapshot_json": registry["model_snapshot_json"],
    }
    if registry != expected_registry or snapshot != expected_snapshot:
        raise BatchError("catalog registry snapshot differs from the exact pinned small.en weights")
    return {"model": model, "registry": registry, "binding_sha256": sha256_bytes(canonical_bytes({"model": model, "registry": registry}))}


def _validate_catalog_binding(
    connection: sqlite3.Connection,
    local_result: dict[str, Any],
) -> dict[str, Any]:
    audio = local_result["audio"]
    artifact_row = one_row(
        connection,
        "SELECT * FROM artifacts WHERE artifact_id = ?",
        (audio["artifact_id"],),
        f"catalog artifact {audio['artifact_id']}",
    )
    artifact = _row_snapshot(artifact_row, ("metadata_json",))
    metadata = _strict_database_json(
        artifact["metadata_json"], "catalog artifact.metadata_json"
    )
    expected_uri = Path(audio["path"]).as_uri()
    if (
        artifact["artifact_kind"] != "window_audio_16khz_mono_flac"
        or artifact["storage_uri"] != expected_uri
        or artifact["sha256"] != audio["sha256"]
        or artifact["byte_count"] != audio["byte_count"]
        or artifact["schema_version"] != 1
        or artifact["visibility"] != "private"
        or artifact["processing_run_id"] is None
    ):
        raise BatchError("catalog artifact does not exactly bind the sealed local-window audio")
    required_metadata = {
        "contract_version": 1,
        "local_window_result_sha256": local_result["sha256"],
        "local_window_result_uri": local_result["uri"],
        "normalized_probe": audio["normalized_probe"],
        "publication_state": "withheld_by_default",
        "identity_authority": "none",
        "representation_is_original_source": False,
        "run_semantics": "catalog_admission_verification_not_extraction_execution",
        "source_time_mapping": local_result["source_time_mapping"],
        "window": local_result["window"],
    }
    for key, expected in required_metadata.items():
        if metadata.get(key) != expected:
            raise BatchError(f"catalog artifact metadata has an inconsistent {key}")
    media_id = identifier(metadata.get("media_id"), "catalog artifact metadata.media_id")
    if media_id != f"media_sha256_{audio['sha256']}":
        raise BatchError("catalog media ID does not derive from the local-window audio SHA-256")
    source_media_id = identifier(metadata.get("source_media_id"), "catalog artifact metadata.source_media_id")

    processing_run_row = one_row(
        connection,
        "SELECT * FROM processing_runs WHERE processing_run_id = ?",
        (artifact["processing_run_id"],),
        "local-window catalog-admission processing run",
    )
    processing_run = _row_snapshot(processing_run_row, ("parameters_json", "environment_json"))
    if (
        processing_run["stage"] != "local_window_result_admission"
        or processing_run["implementation_version"] != "local-window-catalog-bridge/1"
        or processing_run["status"] != "completed"
        or processing_run["model_id"] is not None
        or processing_run["glossary_revision_id"] is not None
        or processing_run["completed_at"] is None
        or processing_run["error_text"] is not None
    ):
        raise BatchError("catalog processing run is not a completed local-window admission run")
    parameters = _strict_database_json(
        processing_run["parameters_json"], "catalog processing run.parameters_json"
    )
    environment = _strict_database_json(
        processing_run["environment_json"], "catalog processing run.environment_json"
    )
    required_parameters = {
        "bundle_id": local_result["bundle_id"],
        "local_window_result_sha256": local_result["sha256"],
        "local_window_result_uri": local_result["uri"],
        "run_semantics": "catalog_admission_verification_not_extraction_execution",
        "time_mapping": local_result["source_time_mapping"],
        "window": local_result["window"],
        "work_order_sha256": local_result["work_order_sha256"],
    }
    if not isinstance(parameters, dict) or any(parameters.get(key) != value for key, value in required_parameters.items()):
        raise BatchError("catalog processing-run parameters do not replay the local-window result")
    required_environment = {
        "credentials_used": False,
        "identity_claims_allowed": False,
        "network_access_performed": False,
        "publication_authority": "none",
    }
    if not isinstance(environment, dict) or any(environment.get(key) != value for key, value in required_environment.items()):
        raise BatchError("catalog processing-run environment is not private/offline/no-authority")

    media_row = one_row(
        connection,
        "SELECT * FROM media_objects WHERE media_id = ?",
        (media_id,),
        "local-window audio media",
    )
    media = _row_snapshot(media_row, ("ffprobe_json",))
    media_probe = _strict_database_json(
        media["ffprobe_json"], "catalog media.ffprobe_json"
    )
    if (
        media["sha256"] != audio["sha256"]
        or media["byte_count"] != audio["byte_count"]
        or media["media_kind"] != "audio"
        or media["mime_type"] != "audio/flac"
        or media["container"] != "flac"
        or media["duration_ms"] != audio["normalized_probe"]["duration_ms"]
        or media_probe != audio["normalized_probe"]
        or media["integrity_state"] != "verified"
    ):
        raise BatchError("catalog media row does not exactly bind the normalized audio")

    location_row = one_row(
        connection,
        "SELECT * FROM media_locations WHERE media_id = ? AND storage_uri = ?",
        (media_id, expected_uri),
        "local-window audio media location",
    )
    location = _row_snapshot(location_row)
    if location["storage_class"] != "private_local" or location["is_primary"] != 1 or location["verified_at"] is None:
        raise BatchError("catalog media location is not a verified primary private-local binding")

    rendition_rows = all_rows(
        connection,
        "SELECT * FROM renditions WHERE media_id = ? ORDER BY rendition_id",
        (media_id,),
        "local-window audio rendition",
    )
    if len(rendition_rows) != 1:
        raise BatchError("local-window audio must resolve to exactly one catalog rendition")
    rendition = _row_snapshot(rendition_rows[0], ("metadata_json",))
    rendition_metadata = _strict_database_json(
        rendition["metadata_json"], "catalog rendition.metadata_json"
    )
    if (
        rendition["review_state"] not in {"unreviewed", "reviewed"}
        or rendition_metadata.get("local_window_result_sha256") != local_result["sha256"]
        or rendition_metadata.get("source_time_mapping") != local_result["source_time_mapping"]
        or rendition_metadata.get("publication_state") != "withheld_by_default"
        or rendition_metadata.get("identity_authority") != "none"
    ):
        raise BatchError("catalog rendition does not preserve the private local-window lineage")
    recording_row = one_row(
        connection,
        "SELECT * FROM recordings WHERE recording_id = ?",
        (rendition["recording_id"],),
        "catalog recording",
    )
    recording = _row_snapshot(recording_row, ("metadata_json",))
    _strict_database_json(recording["metadata_json"], "catalog recording.metadata_json")
    if recording["review_state"] in {"disputed", "rejected", "merged"} or recording["merged_into_recording_id"] is not None:
        raise BatchError("catalog recording is disputed, rejected, or merged")

    timeline_rows = all_rows(
        connection,
        "SELECT * FROM timeline_map_spans WHERE rendition_id = ? ORDER BY ordinal",
        (rendition["rendition_id"],),
        "local-window rendition timeline",
    )
    if len(timeline_rows) != 1:
        raise BatchError("local-window rendition must have exactly one catalog timeline span")
    timeline = _row_snapshot(timeline_rows[0])
    if (
        timeline["ordinal"] != 0
        or timeline["media_start_ms"] != 0
        or timeline["media_end_ms"] != media["duration_ms"]
        or timeline["confidence_state"] in {"disputed", "rejected"}
    ):
        raise BatchError("catalog timeline does not cover the full local-window audio")

    derivation_rows = all_rows(
        connection,
        "SELECT * FROM media_derivations WHERE child_media_id = ? ORDER BY parent_media_id, derivation_kind",
        (media_id,),
        "local-window media derivation",
    )
    if len(derivation_rows) != 1:
        raise BatchError("local-window audio must have exactly one catalog parent derivation")
    derivation = _row_snapshot(derivation_rows[0], ("metadata_json",))
    derivation_metadata = _strict_database_json(
        derivation["metadata_json"], "catalog media derivation.metadata_json"
    )
    if (
        derivation["parent_media_id"] != source_media_id
        or derivation["processing_run_id"] != processing_run["processing_run_id"]
        or derivation_metadata.get("local_window_result_sha256") != local_result["sha256"]
        or derivation_metadata.get("source_time_mapping") != local_result["source_time_mapping"]
    ):
        raise BatchError("catalog media derivation does not bind the admitted source-time lineage")

    snapshot = {
        "artifact": artifact,
        "processing_run": processing_run,
        "media": media,
        "media_location": location,
        "rendition": rendition,
        "recording": recording,
        "timeline_span": timeline,
        "media_derivation": derivation,
    }
    return {**snapshot, "binding_sha256": sha256_bytes(canonical_bytes(snapshot))}


def _engine_document(
    path: Path, *, allow_legacy_manifest_replay: bool = False
) -> dict[str, Any]:
    digest, byte_count = stable_hash(
        path,
        "pinned whisper.cpp executable",
    )
    try:
        profile = whispercpp_engine_profiles.match_engine_profile(
            digest,
            byte_count,
            allow_legacy_manifest_replay=allow_legacy_manifest_replay,
        )
    except whispercpp_engine_profiles.EngineProfileError as error:
        raise BatchError(str(error)) from error
    return whispercpp_engine_profiles.public_engine_document(profile, str(path))


def _software_document() -> dict[str, Any]:
    materializer_path = existing_regular_file(
        str(Path(__file__).resolve(strict=True)),
        "batch materializer source",
    )
    adapter_path = existing_regular_file(
        str(Path(asr_whispercpp.__file__).resolve(strict=True)),
        "whisper.cpp ASR adapter source",
    )
    profiles_path = existing_regular_file(
        str(Path(whispercpp_engine_profiles.__file__).resolve(strict=True)),
        "whisper.cpp engine-profile source",
    )
    materializer_sha, materializer_bytes = stable_hash(
        materializer_path,
        "batch materializer source",
        maximum_bytes=MAX_WORK_ORDER_BYTES,
    )
    adapter_sha, adapter_bytes = stable_hash(
        adapter_path,
        "whisper.cpp ASR adapter source",
        maximum_bytes=MAX_WORK_ORDER_BYTES,
    )
    profiles_sha, profiles_bytes = stable_hash(
        profiles_path,
        "whisper.cpp engine-profile source",
        maximum_bytes=MAX_WORK_ORDER_BYTES,
    )
    return {
        "materializer": {
            "name": MATERIALIZER_NAME,
            "implementation_version": IMPLEMENTATION_VERSION,
            "path": str(materializer_path),
            "sha256": materializer_sha,
            "byte_count": materializer_bytes,
        },
        "asr_adapter": {
            "name": "himr-asr-whispercpp",
            "contract_version": asr_whispercpp.CONTRACT_VERSION,
            "implementation_version": asr_whispercpp.IMPLEMENTATION_VERSION,
            "path": str(adapter_path),
            "sha256": adapter_sha,
            "byte_count": adapter_bytes,
        },
        "engine_profiles": {
            "name": "himr-whispercpp-engine-profiles",
            "contract_version": whispercpp_engine_profiles.PROFILE_CONTRACT_VERSION,
            "implementation_version": whispercpp_engine_profiles.PROFILE_IMPLEMENTATION_VERSION,
            "path": str(profiles_path),
            "sha256": profiles_sha,
            "byte_count": profiles_bytes,
        },
    }


def _legacy_software_document(supplied: Any) -> dict[str, Any]:
    """Admit the one known sealed v0.1.0 software identity for replay validation."""

    if not isinstance(supplied, dict) or set(supplied) != set(LEGACY_SOFTWARE_PROFILE):
        raise BatchError("legacy batch software block is not the known sealed profile")
    expected_paths = {
        "materializer": str(Path(__file__).resolve(strict=True)),
        "asr_adapter": str(Path(asr_whispercpp.__file__).resolve(strict=True)),
    }
    normalized: dict[str, Any] = {}
    for name, expected in LEGACY_SOFTWARE_PROFILE.items():
        value = supplied.get(name)
        if not isinstance(value, dict) or value.get("path") != expected_paths[name]:
            raise BatchError(f"legacy {name} path is not the known pipeline location")
        if {key: value.get(key) for key in expected} != expected or set(value) != {
            *expected,
            "path",
        }:
            raise BatchError(f"legacy {name} identity is not allowlisted")
        normalized[name] = dict(value)
    return normalized


def _model_document(path: Path) -> dict[str, Any]:
    digest, byte_count = stable_hash(
        path,
        "pinned whisper.cpp model",
        expected_sha256=EXPECTED_MODEL_SHA256,
        expected_byte_count=EXPECTED_MODEL_BYTE_COUNT,
        maximum_bytes=2 * 1024 * 1024 * 1024,
    )
    return {
        "path": str(path),
        "expected_sha256": digest,
        "byte_count": byte_count,
        "model_id": EXPECTED_MODEL["model_id"],
        "name": EXPECTED_MODEL["name"],
        "revision": EXPECTED_MODEL["revision"],
        "source": EXPECTED_MODEL["source"],
        "license_label": EXPECTED_MODEL["license_label"],
    }


def _asr_work_order(
    *,
    local_result: dict[str, Any],
    binding: dict[str, Any],
    engine: dict[str, Any],
    model: dict[str, Any],
    output_root: Path,
) -> dict[str, Any]:
    audio = local_result["audio"]
    job_identity = {
        "artifact_id": audio["artifact_id"],
        "audio_sha256": audio["sha256"],
        "parent_processing_run_id": binding["processing_run"]["processing_run_id"],
        "recording_id": binding["recording"]["recording_id"],
        "rendition_id": binding["rendition"]["rendition_id"],
        "pass": "raw_full_local_window_small_en_v1",
    }
    job_id = "asr-raw-" + sha256_bytes(canonical_bytes(job_identity))[:32]
    order = {
        "schema_version": 1,
        "job_id": job_id,
        "input": {
            "path": audio["path"],
            "expected_sha256": audio["sha256"],
            "media_id": binding["media"]["media_id"],
            "artifact_id": audio["artifact_id"],
            "parent_processing_run_id": binding["processing_run"]["processing_run_id"],
        },
        "engine": {key: engine[key] for key in ("executable", "expected_sha256", "version_label", "version_evidence", "build")},
        "model": {key: model[key] for key in ("path", "expected_sha256", "model_id", "name", "revision", "source", "license_label")},
        # The input is already a local window.  ASR coordinates are local to the
        # artifact; source offsets stay in the bound local-window/catalog lineage.
        "window": {
            "offset_ms": 0,
            "duration_ms": audio["normalized_probe"]["duration_ms"],
        },
        "inference": dict(RAW_INFERENCE),
        "glossary": None,
        "catalog_context": {
            "recording_id": binding["recording"]["recording_id"],
            "rendition_id": binding["rendition"]["rendition_id"],
        },
        "output": {"root": str(output_root)},
    }
    try:
        normalized = asr_whispercpp.validate_work_order(order)
    except asr_whispercpp.ASRError as error:
        raise BatchError(f"generated ASR work order is invalid: {error}") from error
    if normalized != order:
        raise BatchError("generated ASR work order is not already in canonical adapter form")
    return order


def build_batch(
    *,
    database_path: Path,
    result_paths: list[Path],
    batch_root: Path,
    asr_output_root: Path,
    engine_path: Path,
    model_path: Path,
    identity_implementation_version: str = IMPLEMENTATION_VERSION,
    software_override: dict[str, Any] | None = None,
    allow_legacy_engine_replay: bool = False,
) -> tuple[dict[str, Any], list[bytes]]:
    database_path = existing_regular_file(str(database_path), "catalog database")
    batch_root = private_directory_path(str(batch_root), "batch root")
    asr_output_root = private_directory_path(str(asr_output_root), "ASR output root")
    engine_path = existing_regular_file(str(engine_path), "pinned whisper.cpp executable", executable=True)
    model_path = existing_regular_file(str(model_path), "pinned whisper.cpp model")
    if batch_root == asr_output_root or batch_root in asr_output_root.parents or asr_output_root in batch_root.parents:
        raise BatchError("batch root and ASR output root must be disjoint")
    for protected in (database_path, engine_path, model_path):
        if batch_root == protected or batch_root in protected.parents or asr_output_root == protected or asr_output_root in protected.parents:
            raise BatchError("batch/output roots may not contain catalog, engine, or model inputs")
    if not 1 <= len(result_paths) <= MAX_RESULTS:
        raise BatchError(f"one to {MAX_RESULTS} local-window results are required")

    normalized_result_paths = [existing_regular_file(str(path), "local-window result") for path in result_paths]
    if len(set(normalized_result_paths)) != len(normalized_result_paths):
        raise BatchError("duplicate local-window result paths are forbidden")
    for result_path in normalized_result_paths:
        if batch_root == result_path or batch_root in result_path.parents or asr_output_root == result_path or asr_output_root in result_path.parents:
            raise BatchError("batch/output roots may not contain local-window inputs")

    if identity_implementation_version not in {
        IMPLEMENTATION_VERSION,
        LEGACY_IMPLEMENTATION_VERSION,
    }:
        raise BatchError("unsupported batch identity implementation version")
    software = _software_document() if software_override is None else software_override
    for component in software.values():
        protected = Path(component["path"])
        if batch_root in protected.parents or asr_output_root in protected.parents:
            raise BatchError("batch/output roots may not contain pipeline implementation inputs")
    engine = _engine_document(
        engine_path,
        allow_legacy_manifest_replay=allow_legacy_engine_replay,
    )
    model = _model_document(model_path)
    local_results = [_validate_local_window_result(path) for path in normalized_result_paths]
    local_results.sort(key=lambda item: (item["path"], item["sha256"]))
    if len({item["audio"]["artifact_id"] for item in local_results}) != len(local_results):
        raise BatchError("duplicate local-window audio artifacts are forbidden")
    total_audio_bytes = sum(item["audio"]["byte_count"] for item in local_results)
    total_audio_ms = sum(item["audio"]["normalized_probe"]["duration_ms"] for item in local_results)
    if total_audio_bytes > MAX_TOTAL_AUDIO_BYTES or total_audio_ms > MAX_TOTAL_AUDIO_MS:
        raise BatchError("batch exceeds the cumulative private audio size/duration cap")

    connection = open_readonly_catalog(database_path)
    try:
        model_registry = _validate_model_registry(connection, model_path)
        bindings = [_validate_catalog_binding(connection, result) for result in local_results]
        try:
            integrity = connection.execute("PRAGMA integrity_check").fetchall()
        except sqlite3.Error as error:
            raise BatchError(f"cannot verify catalog integrity: {error}") from error
        if [row[0] for row in integrity] != ["ok"]:
            raise BatchError("catalog integrity_check did not return exactly 'ok'")
    finally:
        try:
            connection.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        connection.close()

    replayed_results = [_validate_local_window_result(Path(item["path"])) for item in local_results]
    if replayed_results != local_results:
        raise BatchError("local-window result or artifact identity changed during catalog replay")

    work_order_bodies: list[bytes] = []
    entries: list[dict[str, Any]] = []
    for ordinal, (local_result, binding) in enumerate(zip(local_results, bindings, strict=True), start=1):
        order = _asr_work_order(
            local_result=local_result,
            binding=binding,
            engine=engine,
            model=model,
            output_root=asr_output_root,
        )
        body = pretty_bytes(order)
        if len(body) > MAX_WORK_ORDER_BYTES:
            raise BatchError("generated ASR work order exceeds the byte cap")
        work_order_bodies.append(body)
        entries.append(
            {
                "ordinal": ordinal,
                "job_id": order["job_id"],
                "path": f"work-orders/{ordinal:06d}.json",
                "sha256": sha256_bytes(body),
                "canonical_sha256": sha256_bytes(canonical_bytes(order)),
                "byte_count": len(body),
                "local_window_result": {
                    key: local_result[key]
                    for key in (
                        "path",
                        "uri",
                        "sha256",
                        "byte_count",
                        "job_id",
                        "bundle_id",
                        "work_order_sha256",
                        "window",
                        "source_time_mapping",
                    )
                }
                | {"audio": local_result["audio"]},
                "catalog_binding": binding,
            }
        )
    if len({entry["job_id"] for entry in entries}) != len(entries):
        raise BatchError("generated ASR job identifiers collide")

    catalog_binding_sha = sha256_bytes(
        canonical_bytes([entry["catalog_binding"] for entry in entries])
    )
    identity = {
        "schema_version": CONTRACT_VERSION,
        "implementation_version": identity_implementation_version,
        "database_path": str(database_path),
        "software": software,
        "model_registry": model_registry,
        "engine": engine,
        "model": model,
        "profile": {
            "pass_kind": "raw_full_local_window",
            "coordinate_system": "artifact_local_milliseconds",
            "window_offset_ms": 0,
            "duration_basis": "sealed_normalized_probe_full_duration",
            "inference": dict(RAW_INFERENCE),
            "glossary": None,
        },
        "batch_root": str(batch_root),
        "asr_output_root": str(asr_output_root),
        "catalog_binding_sha256": catalog_binding_sha,
        "entries": entries,
    }
    identity_sha = sha256_bytes(canonical_bytes(identity))
    batch_id = f"asrbatch_{identity_sha[:32]}"
    manifest = {
        "schema_version": CONTRACT_VERSION,
        "implementation_version": identity_implementation_version,
        "materializer": MATERIALIZER_NAME,
        "batch_id": batch_id,
        "identity_sha256": identity_sha,
        "batch_relative_path": f"batches/{batch_id}",
        "catalog": {
            "database_path": str(database_path),
            "access_mode": "read_only_snapshot",
            "query_contract_version": 1,
            "binding_sha256": catalog_binding_sha,
            "model_registry": model_registry,
        },
        "software": software,
        "engine": engine,
        "model": model,
        "profile": identity["profile"],
        "output": {
            "batch_root": str(batch_root),
            "asr_output_root": str(asr_output_root),
        },
        "work_order_count": len(entries),
        "totals": {
            "audio_byte_count": total_audio_bytes,
            "audio_duration_ms": total_audio_ms,
        },
        "work_orders": entries,
        "safety": {
            "credentials_used": False,
            "network_access": "forbidden_by_contract_not_performed_by_materializer",
            "publication_authority": "none",
            "visibility": "private",
            "identity_authority": "none",
            "catalog_writes": False,
            "source_bytes_preserved": True,
            "dispatch_order": "sealed_ordinal_fail_fast",
        },
    }
    if len(pretty_bytes(manifest)) > MAX_MANIFEST_BYTES:
        raise BatchError("generated batch manifest exceeds the byte cap")
    return manifest, work_order_bodies


def _ensure_private_root(path: Path, label: str) -> None:
    if not path.exists():
        path.mkdir(parents=True, mode=0o700)
    private_directory_path(str(path), label)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        observed = os.fstat(descriptor)
        path_observed = path.lstat()
        if not stat.S_ISDIR(observed.st_mode) or (observed.st_dev, observed.st_ino) != (
            path_observed.st_dev,
            path_observed.st_ino,
        ):
            raise BatchError(f"{label} was replaced while opening")
        os.fchmod(descriptor, 0o700)
        after = os.fstat(descriptor)
        path_after = path.lstat()
        if (after.st_dev, after.st_ino) != (path_after.st_dev, path_after.st_ino):
            raise BatchError(f"{label} changed while being secured")
    finally:
        os.close(descriptor)


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


def _verify_sealed_batch(manifest_path: Path, manifest: dict[str, Any], work_order_bodies: list[bytes]) -> None:
    final = manifest_path.parent
    work_orders_dir = final / "work-orders"
    if final.name != manifest["batch_id"] or final.parent.name != "batches":
        raise BatchError("batch manifest is not stored at its deterministic batch path")
    if final.parent.parent != Path(manifest["output"]["batch_root"]):
        raise BatchError("batch manifest path does not match output.batch_root")
    for directory, label in ((final, "batch directory"), (work_orders_dir, "work-order directory")):
        link_stat = directory.lstat()
        if stat.S_ISLNK(link_stat.st_mode) or not stat.S_ISDIR(link_stat.st_mode) or stat.S_IMODE(link_stat.st_mode) != 0o500:
            raise BatchError(f"sealed {label} must be a non-symlink directory with mode 0500")
    if {item.name for item in final.iterdir()} != {"manifest.json", "work-orders"}:
        raise BatchError("sealed batch directory has missing or extra entries")
    expected_order_names = {f"{index:06d}.json" for index in range(1, len(work_order_bodies) + 1)}
    if {item.name for item in work_orders_dir.iterdir()} != expected_order_names:
        raise BatchError("sealed work-order directory has missing or extra entries")
    expected_manifest_body = pretty_bytes(manifest)
    observed_manifest_body = stable_read(
        manifest_path,
        "sealed batch manifest",
        maximum_bytes=MAX_MANIFEST_BYTES,
        exact_mode=0o400,
        require_single_link=True,
    )
    if observed_manifest_body != expected_manifest_body:
        raise BatchError("sealed batch manifest failed exact byte replay")
    for index, expected_body in enumerate(work_order_bodies, start=1):
        path = work_orders_dir / f"{index:06d}.json"
        observed = stable_read(
            path,
            f"sealed ASR work order {index}",
            maximum_bytes=MAX_WORK_ORDER_BYTES,
            exact_mode=0o400,
            require_single_link=True,
        )
        if observed != expected_body:
            raise BatchError(f"sealed ASR work order {index} failed exact byte replay")
        entry = manifest["work_orders"][index - 1]
        if sha256_bytes(observed) != entry["sha256"] or len(observed) != entry["byte_count"]:
            raise BatchError(f"sealed ASR work order {index} differs from its manifest pin")


def materialize_batch(
    *,
    database_path: Path,
    result_paths: list[Path],
    batch_root: Path,
    asr_output_root: Path,
    engine_path: Path,
    model_path: Path,
) -> tuple[dict[str, Any], Path]:
    manifest, work_order_bodies = build_batch(
        database_path=database_path,
        result_paths=result_paths,
        batch_root=batch_root,
        asr_output_root=asr_output_root,
        engine_path=engine_path,
        model_path=model_path,
    )
    batch_root = Path(manifest["output"]["batch_root"])
    _ensure_private_root(batch_root, "batch root")
    batches = batch_root / "batches"
    _ensure_private_root(batches, "batch collection")
    lock_path = batch_root / ".asr-whispercpp-batch.lock"
    lock_flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        lock_flags |= os.O_NOFOLLOW
    lock_descriptor = os.open(lock_path, lock_flags, 0o600)
    lock_stat = os.fstat(lock_descriptor)
    lock_path_stat = lock_path.lstat()
    if (
        not stat.S_ISREG(lock_stat.st_mode)
        or lock_stat.st_nlink != 1
        or (lock_stat.st_dev, lock_stat.st_ino) != (lock_path_stat.st_dev, lock_path_stat.st_ino)
    ):
        os.close(lock_descriptor)
        raise BatchError("batch admission lock must be a single-link regular file")
    os.fchmod(lock_descriptor, 0o600)
    final = batches / manifest["batch_id"]
    try:
        fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
        locked_path_stat = lock_path.lstat()
        locked_stat = os.fstat(lock_descriptor)
        if (locked_stat.st_dev, locked_stat.st_ino) != (
            locked_path_stat.st_dev,
            locked_path_stat.st_ino,
        ):
            raise BatchError("batch admission lock was replaced before admission")
        manifest_path = final / "manifest.json"
        if final.exists():
            _verify_sealed_batch(manifest_path, manifest, work_order_bodies)
            return manifest, manifest_path
        staging = batches / f".{manifest['batch_id']}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
        try:
            staging.mkdir(mode=0o700)
            work_orders_dir = staging / "work-orders"
            work_orders_dir.mkdir(mode=0o700)
            for index, body in enumerate(work_order_bodies, start=1):
                _write_exclusive(work_orders_dir / f"{index:06d}.json", body, 0o400)
            _write_exclusive(staging / "manifest.json", pretty_bytes(manifest), 0o400)
            os.chmod(work_orders_dir, 0o500)
            _sync_directory(work_orders_dir)
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
        os.close(lock_descriptor)


MANIFEST_KEYS = {
    "schema_version",
    "implementation_version",
    "materializer",
    "batch_id",
    "identity_sha256",
    "batch_relative_path",
    "catalog",
    "software",
    "engine",
    "model",
    "profile",
    "output",
    "work_order_count",
    "totals",
    "work_orders",
    "safety",
}


def validate_batch(manifest_path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifest_path = existing_regular_file(str(manifest_path), "batch manifest")
    body = stable_read(
        manifest_path,
        "batch manifest",
        maximum_bytes=MAX_MANIFEST_BYTES,
        exact_mode=0o400,
        require_single_link=True,
    )
    supplied = exact_object(parse_json(body, "batch manifest"), "batch manifest", MANIFEST_KEYS)
    identity_version = supplied.get("implementation_version")
    if (
        supplied["schema_version"] != CONTRACT_VERSION
        or identity_version
        not in {IMPLEMENTATION_VERSION, LEGACY_IMPLEMENTATION_VERSION}
        or supplied["materializer"] != MATERIALIZER_NAME
        or not isinstance(supplied["batch_id"], str)
        or not BATCH_ID_RE.fullmatch(supplied["batch_id"])
    ):
        raise BatchError("batch manifest has an unsupported contract/identity")
    work_orders = supplied["work_orders"]
    if not isinstance(work_orders, list) or not 1 <= len(work_orders) <= MAX_RESULTS:
        raise BatchError("batch manifest work_orders is outside the supported cap")
    result_paths: list[Path] = []
    for index, entry in enumerate(work_orders, start=1):
        if not isinstance(entry, dict) or not isinstance(entry.get("local_window_result"), dict):
            raise BatchError("batch manifest contains a malformed work-order entry")
        if entry.get("ordinal") != index or entry.get("path") != f"work-orders/{index:06d}.json":
            raise BatchError("batch manifest work-order ordinals/paths are not canonical")
        result_paths.append(existing_regular_file(entry["local_window_result"].get("path"), "manifest local-window result"))
    catalog = supplied.get("catalog")
    output = supplied.get("output")
    engine = supplied.get("engine")
    model = supplied.get("model")
    if not all(isinstance(value, dict) for value in (catalog, output, engine, model)):
        raise BatchError("batch manifest catalog/output/engine/model blocks must be objects")
    legacy_replay = identity_version == LEGACY_IMPLEMENTATION_VERSION
    software_override = (
        _legacy_software_document(supplied.get("software"))
        if legacy_replay
        else None
    )
    rebuilt, work_order_bodies = build_batch(
        database_path=existing_regular_file(catalog.get("database_path"), "manifest catalog database"),
        result_paths=result_paths,
        batch_root=private_directory_path(output.get("batch_root"), "manifest batch root"),
        asr_output_root=private_directory_path(output.get("asr_output_root"), "manifest ASR output root"),
        engine_path=existing_regular_file(engine.get("executable"), "manifest whisper.cpp executable", executable=True),
        model_path=existing_regular_file(model.get("path"), "manifest whisper.cpp model"),
        identity_implementation_version=identity_version,
        software_override=software_override,
        allow_legacy_engine_replay=legacy_replay,
    )
    if rebuilt != supplied or pretty_bytes(rebuilt) != body:
        raise BatchError("batch manifest failed deterministic catalog/input replay")
    _verify_sealed_batch(manifest_path, rebuilt, work_order_bodies)
    orders: list[dict[str, Any]] = []
    for index, order_body in enumerate(work_order_bodies, start=1):
        raw = parse_json(order_body, f"ASR work order {index}")
        try:
            order = asr_whispercpp.validate_work_order(raw)
        except asr_whispercpp.ASRError as error:
            raise BatchError(f"sealed ASR work order {index} is invalid: {error}") from error
        if sha256_bytes(canonical_bytes(order)) != rebuilt["work_orders"][index - 1]["canonical_sha256"]:
            raise BatchError(f"sealed ASR work order {index} canonical digest differs from the manifest")
        orders.append(order)
    return rebuilt, orders


def _validate_adapter_result(
    result: Any,
    order: dict[str, Any],
    *,
    dry_run: bool,
) -> dict[str, Any]:
    if not isinstance(result, dict):
        raise BatchError("ASR adapter returned a non-object result")
    expected_status = "planned" if dry_run else "completed"
    expected_work_order_sha = sha256_bytes(canonical_bytes(order))
    if (
        result.get("job_id") != order["job_id"]
        or result.get("status") != expected_status
        or result.get("dry_run") is not dry_run
        or result.get("work_order_sha256") != expected_work_order_sha
        or result.get("catalog_context") != order["catalog_context"]
        or result.get("glossary") is not None
    ):
        raise BatchError("ASR adapter returned an inconsistent result envelope")
    expected_window = {
        "offset_ms": 0,
        "duration_ms": order["window"]["duration_ms"],
        "end_ms": order["window"]["duration_ms"],
    }
    if result.get("window") != expected_window:
        raise BatchError("ASR adapter returned inconsistent artifact-local coordinates")
    adapter_input = result.get("input")
    if not isinstance(adapter_input, dict) or any(
        adapter_input.get(key) != order["input"][key]
        for key in ("media_id", "artifact_id", "parent_processing_run_id")
    ):
        raise BatchError("ASR adapter returned inconsistent input lineage")
    recipe_id = identifier(result.get("recipe_id"), "ASR adapter result.recipe_id")
    result_key = sha256_value(result.get("result_key"), "ASR adapter result.result_key")
    result_path = _path_text(result.get("result_path"), "ASR adapter result.result_path")
    output_root = Path(order["output"]["root"])
    if output_root not in result_path.parents:
        raise BatchError("ASR adapter result path escapes the private output root")
    processing_run = result.get("processing_run")
    if not isinstance(processing_run, dict):
        raise BatchError("ASR adapter result.processing_run must be an object")
    processing_run_id = identifier(
        processing_run.get("processing_run_id"),
        "ASR adapter result.processing_run.processing_run_id",
    )
    return {
        "work_order_sha256": expected_work_order_sha,
        "status": expected_status,
        "recipe_id": recipe_id,
        "result_key": result_key,
        "result_path": str(result_path),
        "processing_run_id": processing_run_id,
    }


def run_batch(manifest_path: Path, *, dry_run: bool) -> dict[str, Any]:
    manifest, orders = validate_batch(manifest_path)
    if manifest["implementation_version"] == LEGACY_IMPLEMENTATION_VERSION:
        raise BatchError(
            "sealed v0.1.0/v1.8.3 manifests remain validation-compatible but are "
            "dispatch-disabled; materialize an explicit subset with the current engine profile"
        )
    manifest_body = pretty_bytes(manifest)
    if not dry_run:
        # The underlying adapter creates deeper content-addressed directories.
        # Establish an owner-only traversal boundary before it writes anything;
        # validation and dry-run remain strictly no-write.
        _ensure_private_root(
            Path(manifest["output"]["asr_output_root"]),
            "ASR output root",
        )
    results: list[dict[str, Any]] = []
    for index, order in enumerate(orders, start=1):
        # Re-read the sealed order at dispatch so a change after admission cannot
        # substitute a different job in a long sequential batch.
        order_path = manifest_path.parent / manifest["work_orders"][index - 1]["path"]
        observed = stable_read(
            order_path,
            f"dispatch ASR work order {index}",
            maximum_bytes=MAX_WORK_ORDER_BYTES,
            exact_mode=0o400,
            require_single_link=True,
        )
        if sha256_bytes(observed) != manifest["work_orders"][index - 1]["sha256"]:
            raise BatchError(f"ASR work order {index} changed before dispatch")
        try:
            result = asr_whispercpp.run_asr(order, dry_run=dry_run)
        except (asr_whispercpp.ASRError, OSError, subprocess.SubprocessError) as error:
            quarantine = getattr(error, "quarantine", None)
            failure_result = {
                "schema_version": 1,
                "implementation_version": IMPLEMENTATION_VERSION,
                "batch_id": manifest["batch_id"],
                "manifest_sha256": sha256_bytes(manifest_body),
                "status": "failed",
                "dry_run": dry_run,
                "job_count": len(orders),
                "results": results,
                "failed_job": {
                    "ordinal": index,
                    "job_id": order["job_id"],
                    "work_order_sha256": sha256_bytes(canonical_bytes(order)),
                    "error": {
                        "type": type(error).__name__,
                        "message": str(error),
                    },
                    "quarantine": quarantine,
                },
                "safety": {
                    "dispatch_order": "sealed_ordinal_fail_fast",
                    "network_access": "forbidden_by_batch_contract",
                    "publication_authority": "none",
                    "catalog_writes": False,
                    "resume_policy": "replay_manifest_reuses_content_addressed_completed_results",
                    "subset_policy": "materialize_explicit_sealed_input_subset",
                },
            }
            raise BatchRunFailure(failure_result) from error
        validated_result = _validate_adapter_result(result, order, dry_run=dry_run)
        results.append(
            {
                "ordinal": index,
                "job_id": order["job_id"],
                **validated_result,
            }
        )

    replayed, _ = validate_batch(manifest_path)
    if pretty_bytes(replayed) != manifest_body:
        raise BatchError("batch changed during sequential dispatch")
    return {
        "schema_version": 1,
        "implementation_version": IMPLEMENTATION_VERSION,
        "batch_id": manifest["batch_id"],
        "manifest_sha256": sha256_bytes(manifest_body),
        "status": "planned" if dry_run else "completed",
        "dry_run": dry_run,
        "job_count": len(orders),
        "results": results,
        "safety": {
            "dispatch_order": "sealed_ordinal_fail_fast",
            "network_access": "forbidden_by_batch_contract",
            "publication_authority": "none",
            "catalog_writes": False,
            "resume_policy": "replay_manifest_reuses_content_addressed_completed_results",
            "subset_policy": "materialize_explicit_sealed_input_subset",
        },
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialize, validate, or sequentially dispatch private raw whisper.cpp ASR batches"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    materialize = commands.add_parser("materialize")
    materialize.add_argument("--db", required=True)
    materialize.add_argument("--local-window-result", action="append", required=True)
    materialize.add_argument("--batch-root", required=True)
    materialize.add_argument("--asr-output-root", required=True)
    materialize.add_argument("--engine", required=True)
    materialize.add_argument("--model", required=True)
    validate = commands.add_parser("validate")
    validate.add_argument("--manifest", required=True)
    run = commands.add_parser("run")
    run.add_argument("--manifest", required=True)
    run.add_argument("--dry-run", action="store_true")
    return parser


def _absolute_cli_path(value: str, label: str, *, existing: bool = False) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve(strict=False)
    else:
        path = path.resolve(strict=False)
    if existing:
        return existing_regular_file(str(path), label)
    return path


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "materialize":
            manifest, manifest_path = materialize_batch(
                database_path=_absolute_cli_path(args.db, "--db", existing=True),
                result_paths=[_absolute_cli_path(value, "--local-window-result", existing=True) for value in args.local_window_result],
                batch_root=_absolute_cli_path(args.batch_root, "--batch-root"),
                asr_output_root=_absolute_cli_path(args.asr_output_root, "--asr-output-root"),
                engine_path=_absolute_cli_path(args.engine, "--engine", existing=True),
                model_path=_absolute_cli_path(args.model, "--model", existing=True),
            )
            result: Any = {"manifest": manifest, "manifest_path": str(manifest_path)}
        elif args.command == "validate":
            manifest_path = _absolute_cli_path(args.manifest, "--manifest", existing=True)
            manifest, _ = validate_batch(manifest_path)
            result = manifest
        else:
            manifest_path = _absolute_cli_path(args.manifest, "--manifest", existing=True)
            result = run_batch(manifest_path, dry_run=args.dry_run)
        sys.stdout.buffer.write(pretty_bytes(result))
        return 0
    except BatchRunFailure as error:
        sys.stderr.buffer.write(pretty_bytes(error.result))
        return 2
    except (BatchError, OSError, sqlite3.Error, subprocess.SubprocessError) as error:
        failure = {
            "schema_version": 1,
            "status": "failed",
            "error": {"type": type(error).__name__, "message": str(error)},
        }
        sys.stderr.buffer.write(pretty_bytes(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
