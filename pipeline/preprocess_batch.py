#!/usr/bin/env python3
"""Private, resumable media-preprocess batches from acquisition results.

The batch layer never downloads media, opens the corpus database, or grants
publication authority.  It seals a deterministic selection and immutable set of
ordinary media_preprocess v1 work orders.  Execution is sequential and resumable
through immutable per-item receipts; an item without a valid receipt remains
pending.
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
import stat
import sys
import tempfile
import threading
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

try:
    from . import media_preprocess_v034 as media_preprocess
except ImportError:  # pragma: no cover - direct script execution
    import media_preprocess_v034 as media_preprocess  # type: ignore[no-redef]


# The private-acquisition receipt validator is deliberately reused instead of
# partially reimplementing its descriptor-safe replay here.  Keep this import
# local to the repository checkout: preprocess-batch remains an offline tool and
# does not open the corpus database.
CORPUS_SOURCE_ROOT = Path(__file__).resolve().parent.parent / "corpus" / "src"
if str(CORPUS_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORPUS_SOURCE_ROOT))
from himr_corpus import private_acquisition  # noqa: E402


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.3.0"
SUPPORTED_IMPLEMENTATION_VERSIONS = {"0.1.0", "0.2.0", IMPLEMENTATION_VERSION}
PIPELINE_ROOT = Path(__file__).resolve().parent
REPOSITORY_ROOT = PIPELINE_ROOT.parent
DEFAULT_PROFILE = PIPELINE_ROOT / "profiles" / "cpu-balanced-v1.json"
LEGACY_PRODUCER_PATH = PIPELINE_ROOT / "media_preprocess.py"
PRODUCER_PATH = PIPELINE_ROOT / "media_preprocess_v034.py"
RECOGNIZED_PRODUCER_PATHS = frozenset(
    {LEGACY_PRODUCER_PATH.resolve(), PRODUCER_PATH.resolve()}
)

MAX_BATCH_ITEMS = 128
MAX_SELECTION_BYTES = 4 * 1024 * 1024
MAX_BUNDLE_MANIFEST_BYTES = 8 * 1024 * 1024
MAX_ACQUISITION_RESULT_BYTES = 16 * 1024 * 1024
MAX_PREPROCESS_RESULT_BYTES = 64 * 1024 * 1024
MAX_WORK_ORDER_BYTES = 256 * 1024
MAX_RECEIPT_BYTES = 4 * 1024 * 1024
MAX_PRIVATE_SEAL_RECEIPT_BYTES = private_acquisition.MAX_JSON_BYTES
MAX_PATH_CHARACTERS = 16_384
MAX_ARTIFACTS = 16
DEFAULT_RUN_LIMIT = 1

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
STABLE_ID_RE = re.compile(r"^[a-z]+_[0-9a-f]{32}$")
MEDIA_ID_RE = re.compile(r"^media_sha256_([0-9a-f]{64})$")
RECEIPT_TEMP_RE = re.compile(
    r"^\.([0-9]{6}\.json)\.tmp-([1-9][0-9]{0,19})-([0-9a-f]{32})$"
)
LEGACY_RECEIPT_TEMP_RE = re.compile(
    r"^\.([0-9]{6}\.json)\.tmp-([1-9][0-9]{0,19})$"
)
MAX_RECEIPT_TEMP_FILES = MAX_BATCH_ITEMS * 2

FULL_OPERATIONS = {
    "probe": True,
    "audio_flac": True,
    "proxy": True,
    "routing": True,
}
ASR_READY_OPERATIONS = {
    "probe": True,
    "audio_flac": True,
    "proxy": False,
    "routing": False,
}
ENRICHMENT_ONLY_OPERATIONS = {
    "probe": True,
    "audio_flac": False,
    "proxy": True,
    "routing": True,
}
OPERATION_PROFILES = {
    "full": FULL_OPERATIONS,
    "asr-ready": ASR_READY_OPERATIONS,
    "enrichment-only": ENRICHMENT_ONLY_OPERATIONS,
}
# Backward-compatible public name used by existing callers and sealed fixtures.
OPERATIONS = FULL_OPERATIONS
SAFETY = {
    "visibility": "private",
    "network_allowed": False,
    "credentials_allowed": False,
    "publication_authority": "none",
    "identity_claims_allowed": False,
    "source_bytes_preserved": True,
    "automatic_catalog_admission": False,
}


class BatchError(RuntimeError):
    """A selection, bundle, receipt, or local-integrity check failed closed."""


class RestoreToolProvenanceError(BatchError):
    """A restore-scoped tool witness could not preserve its exact boundary."""


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
        raise BatchError(f"value cannot be encoded canonically: {error}") from error


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
        raise BatchError(f"value cannot be encoded as JSON: {error}") from error


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def stable_id(prefix: str, *parts: Any) -> str:
    return f"{prefix}_" + sha256_bytes(canonical_bytes(list(parts)))[:32]


def exact_object(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BatchError(f"{label} must be a JSON object")
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    if missing or unknown:
        raise BatchError(
            f"{label} keys differ from the exact contract; "
            f"missing={missing}, unknown={unknown}"
        )
    return value


def integer(
    value: Any,
    label: str,
    *,
    minimum: int = 0,
    maximum: int = 2**63 - 1,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or value > maximum
    ):
        raise BatchError(f"{label} must be an integer from {minimum} to {maximum}")
    return value


def text(value: Any, label: str, maximum: int = 2_000) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise BatchError(f"{label} must be bounded non-empty text")
    return value


def sha256_value(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise BatchError(f"{label} must be a lowercase SHA-256")
    return value


def timestamp(value: Any, label: str) -> str:
    raw = text(value, label, 100)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as error:
        raise BatchError(f"{label} must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None:
        raise BatchError(f"{label} must include a UTC offset")
    normalized = parsed.astimezone(timezone.utc)
    return normalized.isoformat(
        timespec="microseconds" if normalized.microsecond else "seconds"
    ).replace("+00:00", "Z")


def reject_constant(value: str) -> None:
    raise BatchError(f"non-finite JSON constant is forbidden: {value}")


def reject_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise BatchError(f"JSON object contains duplicate key: {key}")
        value[key] = item
    return value


def parse_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise BatchError("JSON floating-point value must be finite")
    return parsed


def parse_integer(value: str) -> int:
    if len(value.lstrip("-")) > 19:
        raise BatchError("JSON integer exceeds the 64-bit lexical cap")
    return int(value)


def json_value(body: bytes, label: str) -> Any:
    try:
        return json.loads(
            body.decode("utf-8"),
            object_pairs_hook=reject_pairs,
            parse_constant=reject_constant,
            parse_float=parse_float,
            parse_int=parse_integer,
        )
    except BatchError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as error:
        raise BatchError(f"{label} is not strict bounded UTF-8 JSON") from error


def absolute_path(value: Any, label: str, *, must_exist: bool) -> Path:
    try:
        path_value = os.fspath(value)
    except TypeError:
        path_value = value
    raw = text(path_value, label, MAX_PATH_CHARACTERS)
    if "://" in raw:
        raise BatchError(f"{label} must be a local path, not a URL")
    path = Path(raw)
    if not path.is_absolute():
        raise BatchError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=must_exist)
    except (OSError, RuntimeError) as error:
        raise BatchError(f"{label} is missing or unsafe: {path}") from error
    return resolved


def lexical_absolute_path(value: Any, label: str) -> Path:
    try:
        path_value = os.fspath(value)
    except TypeError:
        path_value = value
    raw = text(path_value, label, MAX_PATH_CHARACTERS)
    if "://" in raw:
        raise BatchError(f"{label} must be a local path, not a URL")
    path = Path(raw)
    if not path.is_absolute():
        raise BatchError(f"{label} must be absolute")
    return Path(os.path.abspath(os.fspath(path)))


def exact_existing_directory(value: Any, label: str) -> Path:
    requested = lexical_absolute_path(value, label)
    try:
        observed = requested.lstat()
        resolved = requested.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise BatchError(f"{label} is missing or unsafe: {requested}") from error
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or resolved != requested
    ):
        raise BatchError(f"{label} must be an exact non-symlink directory path")
    return requested


def validate_output_root(path: Path, label: str) -> None:
    if path == Path("/"):
        raise BatchError(f"{label} may not be the filesystem root")
    for forbidden in (Path("/tmp"), Path("/var/tmp")):
        if path == forbidden or forbidden in path.parents:
            raise BatchError(f"{label} may not be under {forbidden}")
    if path.exists():
        before = path.lstat()
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISDIR(before.st_mode):
            raise BatchError(f"{label} must be a real directory or a new path")
        if stat.S_IMODE(before.st_mode) & 0o022:
            raise BatchError(f"{label} may not be group/world writable")
    try:
        relative = path.relative_to(REPOSITORY_ROOT)
    except ValueError:
        return
    if relative.parts and relative.parts[0] in {"public", "src", "dist", ".git"}:
        raise BatchError(f"{label} may not be inside a public or repository-control path")


def private_output_root_reference(value: Any, label: str) -> Path:
    path = lexical_absolute_path(value, label)
    validate_output_root(path, label)
    if path.exists() or path.is_symlink():
        require_private_directory(path, label)
    else:
        try:
            resolved = path.resolve(strict=False)
        except (OSError, RuntimeError) as error:
            raise BatchError(f"{label} has an unsafe parent path") from error
        if resolved != path:
            raise BatchError(f"{label} may not traverse a symlink")
    return path


def stat_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        value.st_mode,
    )


def stable_read(
    path: Path,
    maximum: int,
    label: str,
    *,
    required_mode: int | None = None,
    allow_hardlinks: bool = False,
) -> tuple[Path, bytes, os.stat_result]:
    requested = Path(os.path.abspath(os.fspath(path)))
    try:
        path_before = requested.lstat()
    except OSError as error:
        raise BatchError(f"{label} cannot be inspected: {requested}") from error
    if stat.S_ISLNK(path_before.st_mode):
        raise BatchError(f"{label} may not be a symlink")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(requested, flags)
    except OSError as error:
        raise BatchError(f"{label} cannot be opened safely") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or (
            not allow_hardlinks and before.st_nlink != 1
        ):
            qualifier = "regular file" if allow_hardlinks else "single-link regular file"
            raise BatchError(f"{label} must be a {qualifier}")
        if before.st_size < 1 or before.st_size > maximum:
            raise BatchError(f"{label} exceeds its byte cap of {maximum}")
        if required_mode is not None and stat.S_IMODE(before.st_mode) != required_mode:
            raise BatchError(
                f"{label} must have mode {required_mode:04o}, observed "
                f"{stat.S_IMODE(before.st_mode):04o}"
            )
        chunks: list[bytes] = []
        offset = 0
        while offset < before.st_size:
            chunk = os.pread(descriptor, min(1024 * 1024, before.st_size - offset), offset)
            if not chunk:
                raise BatchError(f"{label} ended during its stable read")
            chunks.append(chunk)
            offset += len(chunk)
        after = os.fstat(descriptor)
        try:
            path_after = requested.lstat()
        except OSError as error:
            raise BatchError(f"{label} changed during its stable read") from error
        if (
            stat.S_ISLNK(path_after.st_mode)
            or stat_identity(path_before) != stat_identity(before)
            or stat_identity(before) != stat_identity(after)
            or stat_identity(after) != stat_identity(path_after)
        ):
            raise BatchError(f"{label} changed during its stable read")
        return requested, b"".join(chunks), after
    finally:
        os.close(descriptor)


def stable_hash_media(
    path: Path,
    *,
    expected_sha256: str,
    expected_byte_count: int,
    label: str,
) -> dict[str, int]:
    requested = Path(os.path.abspath(os.fspath(path)))
    try:
        path_before = requested.lstat()
    except OSError as error:
        raise BatchError(f"{label} cannot be inspected") from error
    if stat.S_ISLNK(path_before.st_mode):
        raise BatchError(f"{label} may not be a symlink")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(requested, flags)
    except OSError as error:
        raise BatchError(f"{label} cannot be opened safely") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size != expected_byte_count:
            raise BatchError(f"{label} byte count differs from its acquisition pin")
        digest = hashlib.sha256()
        offset = 0
        while offset < before.st_size:
            chunk = os.pread(descriptor, min(8 * 1024 * 1024, before.st_size - offset), offset)
            if not chunk:
                raise BatchError(f"{label} ended during hashing")
            digest.update(chunk)
            offset += len(chunk)
        after = os.fstat(descriptor)
        try:
            path_after = requested.lstat()
        except OSError as error:
            raise BatchError(f"{label} changed while being hashed") from error
        if (
            stat.S_ISLNK(path_after.st_mode)
            or stat_identity(path_before) != stat_identity(before)
            or stat_identity(before) != stat_identity(after)
            or stat_identity(after) != stat_identity(path_after)
        ):
            raise BatchError(f"{label} changed while being hashed")
        observed = digest.hexdigest()
        if observed != expected_sha256:
            raise BatchError(f"{label} SHA-256 differs from its acquisition pin")
        return {
            "device": before.st_dev,
            "inode": before.st_ino,
            "byte_count": before.st_size,
            "mtime_ns": before.st_mtime_ns,
        }
    finally:
        os.close(descriptor)


def handling_policy(value: Any, label: str) -> dict[str, str]:
    """Validate the exact v30 private-acquisition handling-policy shape."""

    try:
        return private_acquisition.validate_handling_policy(value, label)
    except private_acquisition.PrivateAcquisitionError as error:
        raise BatchError(str(error)) from error


def private_seal_binding(
    artifact_root: Path,
    receipt_path: Path,
) -> tuple[Path, dict[str, Any]]:
    """Replay one v30 seal receipt and return its batch-control binding.

    A receipt is not treated as a bearer token.  The private-acquisition validator
    reopens the exact work order, result and media beneath ``artifact_root`` and
    rechecks their owner-only modes and hashes on every call.
    """

    root = require_private_directory(artifact_root, "private acquisition artifact root")
    receipt_requested = lexical_absolute_path(
        receipt_path, "private acquisition seal receipt"
    )
    require_descendant(
        receipt_requested, root, "private acquisition seal receipt"
    )
    validate_private_ancestor_chain(
        receipt_requested.parent,
        root,
        "private acquisition seal receipt directory chain",
    )
    resolved_receipt, receipt_body, _ = stable_read(
        receipt_requested,
        MAX_PRIVATE_SEAL_RECEIPT_BYTES,
        "private acquisition seal receipt",
        required_mode=0o600,
    )
    try:
        receipt = json_value(receipt_body, "private acquisition seal receipt")
        private_acquisition.validate_private_acquisition_seal_receipt(root, receipt)
    except private_acquisition.PrivateAcquisitionError as error:
        raise BatchError(f"private acquisition seal receipt failed replay: {error}") from error

    plan = receipt["plan"]
    artifact_rows = {
        row["role"]: row for row in receipt["sealed_artifacts"]
    }
    result_row = artifact_rows["result"]
    result_path = root.joinpath(*Path(result_row["relative_path"]).parts)
    result_path = absolute_path(
        result_path, "sealed private acquisition result", must_exist=True
    )
    policy = handling_policy(
        plan["handling_policy"], "private acquisition seal handling_policy"
    )
    seal = {
        "artifact_root": str(root),
        "receipt_path": str(resolved_receipt),
        "receipt_sha256": sha256_bytes(receipt_body),
        "receipt_byte_count": len(receipt_body),
        "plan_sha256": sha256_value(
            receipt["plan_sha256"], "private acquisition seal plan SHA-256"
        ),
        "validated_at": timestamp(
            receipt["validated_at"], "private acquisition seal validated_at"
        ),
        "result_physical_sha256": sha256_value(
            result_row["sha256"], "sealed acquisition result SHA-256"
        ),
        "result_canonical_sha256": sha256_value(
            plan["result_canonical_sha256"],
            "sealed acquisition result canonical SHA-256",
        ),
        "work_order_sha256": sha256_value(
            plan["work_order_sha256"], "sealed acquisition work-order SHA-256"
        ),
        "media_id": text(plan["media_id"], "sealed acquisition media_id", 128),
        "media_sha256": sha256_value(
            plan["media_sha256"], "sealed acquisition media SHA-256"
        ),
        "media_byte_count": integer(
            plan["media_byte_count"],
            "sealed acquisition media byte count",
            minimum=1,
        ),
        "source": plan["source"],
        "source_byte_identity_claimed": plan["source_byte_identity_claimed"],
    }
    if seal["source_byte_identity_claimed"] is not False:
        raise BatchError("private acquisition seal may not claim source byte identity")
    return result_path, {
        "handling_policy": policy,
        "private_acquisition_seal": seal,
    }


def private_seal_bindings(
    artifact_root: Path | None,
    receipt_paths: list[Path] | None,
) -> dict[str, dict[str, Any]]:
    """Build an exact result-path lookup for explicitly supplied v30 receipts."""

    receipts = list(receipt_paths or [])
    if artifact_root is None and receipts:
        raise BatchError(
            "--private-acquisition-root is required with private seal receipts"
        )
    if artifact_root is not None and not receipts:
        raise BatchError(
            "at least one --private-seal-receipt is required with "
            "--private-acquisition-root"
        )
    if artifact_root is None:
        return {}
    if len(receipts) > MAX_BATCH_ITEMS:
        raise BatchError(f"private seal receipt count exceeds {MAX_BATCH_ITEMS}")
    bindings: dict[str, dict[str, Any]] = {}
    seen_receipts: set[str] = set()
    for receipt_path in receipts:
        resolved_receipt = str(
            lexical_absolute_path(receipt_path, "private acquisition seal receipt")
        )
        if resolved_receipt in seen_receipts:
            raise BatchError("private seal receipt path is repeated")
        seen_receipts.add(resolved_receipt)
        result_path, boundary = private_seal_binding(artifact_root, receipt_path)
        key = str(result_path)
        if key in bindings:
            raise BatchError("private seal receipts repeat an acquisition result")
        bindings[key] = boundary
    return bindings


def expected_entry_id(
    *,
    result_sha256: str,
    media_sha256: str,
    result_path: Path,
    media_path: Path,
    handling_boundary: dict[str, Any] | None,
) -> str:
    parts: list[Any] = [
        result_sha256,
        media_sha256,
        str(result_path),
        str(media_path),
    ]
    if handling_boundary is not None:
        parts.append(sha256_bytes(canonical_bytes(handling_boundary)))
    return stable_id("pbe", *parts)


def replay_handling_boundary(value: Any) -> dict[str, Any]:
    boundary = exact_object(
        value,
        "private acquisition handling boundary",
        {"handling_policy", "private_acquisition_seal"},
    )
    policy = handling_policy(
        boundary["handling_policy"], "private acquisition handling_policy"
    )
    seal = exact_object(
        boundary["private_acquisition_seal"],
        "private acquisition seal binding",
        {
            "artifact_root",
            "receipt_path",
            "receipt_sha256",
            "receipt_byte_count",
            "plan_sha256",
            "validated_at",
            "result_physical_sha256",
            "result_canonical_sha256",
            "work_order_sha256",
            "media_id",
            "media_sha256",
            "media_byte_count",
            "source",
            "source_byte_identity_claimed",
        },
    )
    artifact_root = absolute_path(
        seal["artifact_root"], "private acquisition artifact root", must_exist=True
    )
    receipt_path = absolute_path(
        seal["receipt_path"], "private acquisition seal receipt", must_exist=True
    )
    sha256_value(seal["receipt_sha256"], "private seal receipt SHA-256")
    integer(
        seal["receipt_byte_count"],
        "private seal receipt byte count",
        minimum=1,
        maximum=MAX_PRIVATE_SEAL_RECEIPT_BYTES,
    )
    for key in (
        "plan_sha256",
        "result_physical_sha256",
        "result_canonical_sha256",
        "work_order_sha256",
        "media_sha256",
    ):
        sha256_value(seal[key], f"private seal {key}")
    timestamp(seal["validated_at"], "private seal validated_at")
    text(seal["media_id"], "private seal media_id", 128)
    integer(seal["media_byte_count"], "private seal media byte count", minimum=1)
    if seal["source_byte_identity_claimed"] is not False:
        raise BatchError("private acquisition seal may not claim source byte identity")
    _result_path, observed = private_seal_binding(artifact_root, receipt_path)
    expected = {
        "handling_policy": policy,
        "private_acquisition_seal": seal,
    }
    if canonical_bytes(observed) != canonical_bytes(expected):
        raise BatchError(
            "private acquisition handling boundary differs from exact seal replay"
        )
    return expected


def media_entry_from_acquisition_result(
    result_path: Path,
    *,
    expected_result_sha256: str | None = None,
    expected_handling_boundary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved, body, _ = stable_read(
        result_path,
        MAX_ACQUISITION_RESULT_BYTES,
        "acquisition result",
    )
    result_sha256 = sha256_bytes(body)
    if expected_result_sha256 is not None and result_sha256 != expected_result_sha256:
        raise BatchError("acquisition result SHA-256 differs from the sealed selection")
    value = json_value(body, "acquisition result")
    if not isinstance(value, dict):
        raise BatchError("acquisition result must be a JSON object")
    required = {
        "schema_version",
        "job_id",
        "adapter",
        "status",
        "dry_run",
        "reused",
        "work_order_sha256",
        "started_at",
        "completed_at",
        "duration_ms",
        "source",
        "limits",
        "capacity_before",
        "capacity_after",
        "commands",
        "source_observation",
        "selected_remote_metadata",
        "admission",
        "catalog_records",
        "result_path",
        "errors",
    }
    # Durable `reused` records CAS admission deduplication. Whole-result replay
    # adds `reuse_verified_at` only to the ephemeral adapter return, never here.
    if "handling_policy" in value:
        required.add("handling_policy")
    exact_object(value, "acquisition result", required)
    if (
        value["schema_version"] != 1
        or value["status"] != "completed"
        or value["dry_run"] is not False
        or not isinstance(value["reused"], bool)
        or value["errors"] != []
    ):
        raise BatchError("acquisition result is not a completed successful version-1 result")
    if value["adapter"] not in {"local_file", "direct_http", "yt_dlp"}:
        raise BatchError("acquisition result adapter is unsupported")
    result_handling_policy: dict[str, str] | None = None
    if "handling_policy" in value:
        if value["adapter"] != "local_file":
            raise BatchError(
                "acquisition handling_policy is supported only for local_file results"
            )
        result_handling_policy = handling_policy(
            value["handling_policy"], "acquisition result handling_policy"
        )
        if expected_handling_boundary is None:
            raise BatchError(
                "private acquisition result requires an exact replayed seal receipt"
            )
    elif expected_handling_boundary is not None:
        raise BatchError(
            "a private seal receipt was supplied for an acquisition result without "
            "a handling_policy"
        )
    job_id = text(value["job_id"], "acquisition result job_id", 128)
    if not ID_RE.fullmatch(job_id):
        raise BatchError("acquisition result job_id is invalid")
    work_order_sha256 = sha256_value(
        value["work_order_sha256"], "acquisition result work_order_sha256"
    )
    if resolved != absolute_path(value["result_path"], "acquisition result result_path", must_exist=True):
        raise BatchError("acquisition result path disagrees with its envelope")
    if resolved.name != "result.json" or resolved.parent.name != work_order_sha256:
        raise BatchError("acquisition result path is not bound to its work-order digest")
    if resolved.parent.parent.name != job_id:
        raise BatchError("acquisition result path is not bound to its job_id")
    completed_at = timestamp(value["completed_at"], "acquisition result completed_at")
    timestamp(value["started_at"], "acquisition result started_at")
    integer(value["duration_ms"], "acquisition result duration_ms")

    admission = exact_object(
        value["admission"],
        "acquisition result admission",
        {"media_id", "sha256", "byte_count", "path", "storage_uri", "normalized_probe"},
    )
    media_sha256 = sha256_value(admission["sha256"], "acquisition media SHA-256")
    media_id = text(admission["media_id"], "acquisition media_id", 128)
    if media_id != f"media_sha256_{media_sha256}":
        raise BatchError("acquisition media_id disagrees with its SHA-256")
    media_byte_count = integer(
        admission["byte_count"],
        "acquisition media byte_count",
        minimum=1,
    )
    media_path = absolute_path(admission["path"], "acquisition media path", must_exist=True)
    if admission["storage_uri"] != media_path.as_uri():
        raise BatchError("acquisition media storage_uri disagrees with its path")
    stable_hash_media(
        media_path,
        expected_sha256=media_sha256,
        expected_byte_count=media_byte_count,
        label="acquired media",
    )
    probe = admission["normalized_probe"]
    if not isinstance(probe, dict) or not isinstance(probe.get("format"), dict):
        raise BatchError("acquisition normalized_probe is malformed")
    duration_ms = probe["format"].get("duration_ms")
    if duration_ms is not None:
        integer(duration_ms, "acquisition media duration_ms", minimum=0)

    catalog = exact_object(
        value["catalog_records"],
        "acquisition catalog_records",
        {"sources", "media_objects", "media_locations", "media_sources"},
    )
    for key in catalog:
        if not isinstance(catalog[key], list) or len(catalog[key]) != 1:
            raise BatchError(f"acquisition catalog_records.{key} must contain one row")
    media_object = exact_object(
        catalog["media_objects"][0],
        "acquisition media_object",
        {
            "media_id",
            "sha256",
            "byte_count",
            "media_kind",
            "mime_type",
            "container",
            "duration_ms",
            "ffprobe_json",
            "first_cataloged_at",
            "integrity_state",
        },
    )
    first_cataloged_at = timestamp(
        media_object["first_cataloged_at"], "acquisition media first_cataloged_at"
    )
    if (
        media_object["media_id"] != media_id
        or media_object["sha256"] != media_sha256
        or media_object["byte_count"] != media_byte_count
        or media_object["integrity_state"] != "verified"
        or canonical_bytes(media_object["ffprobe_json"]) != canonical_bytes(probe)
        or media_object["duration_ms"] != duration_ms
    ):
        raise BatchError("acquisition catalog media row disagrees with admission")
    location = catalog["media_locations"][0]
    if not isinstance(location, dict) or (
        location.get("media_id") != media_id
        or location.get("storage_uri") != media_path.as_uri()
        or location.get("is_primary") != 1
    ):
        raise BatchError("acquisition media location disagrees with admission")

    handling_boundary: dict[str, Any] | None = None
    if result_handling_policy is not None:
        boundary = exact_object(
            expected_handling_boundary,
            "private acquisition handling boundary",
            {"handling_policy", "private_acquisition_seal"},
        )
        if boundary["handling_policy"] != result_handling_policy:
            raise BatchError(
                "private acquisition seal handling_policy differs from its result"
            )
        seal = boundary["private_acquisition_seal"]
        if not isinstance(seal, dict):
            raise BatchError("private acquisition seal binding must be an object")
        if (
            seal.get("result_physical_sha256") != result_sha256
            or seal.get("work_order_sha256") != work_order_sha256
            or seal.get("media_id") != media_id
            or seal.get("media_sha256") != media_sha256
            or seal.get("media_byte_count") != media_byte_count
            or seal.get("source_byte_identity_claimed") is not False
        ):
            raise BatchError(
                "private acquisition seal binding differs from the result/media pins"
            )
        handling_boundary = boundary

    entry_core = {
        "acquisition_result": {
            "path": str(resolved),
            "sha256": result_sha256,
            "byte_count": len(body),
            "job_id": job_id,
            "work_order_sha256": work_order_sha256,
            "completed_at": completed_at,
        },
        "source_media": {
            "path": str(media_path),
            "media_id": media_id,
            "sha256": media_sha256,
            "byte_count": media_byte_count,
            "duration_ms": duration_ms,
            "first_cataloged_at": first_cataloged_at,
        },
    }
    return {
        **entry_core,
        **(
            {"handling_boundary": handling_boundary}
            if handling_boundary is not None
            else {}
        ),
        "entry_id": expected_entry_id(
            result_sha256=result_sha256,
            media_sha256=media_sha256,
            result_path=resolved,
            media_path=media_path,
            handling_boundary=handling_boundary,
        ),
    }


def build_selection(
    result_paths: list[Path],
    *,
    private_acquisition_root: Path | None = None,
    private_seal_receipt_paths: list[Path] | None = None,
) -> dict[str, Any]:
    if not result_paths or len(result_paths) > MAX_BATCH_ITEMS:
        raise BatchError(f"selection must contain 1 to {MAX_BATCH_ITEMS} results")
    seal_bindings = private_seal_bindings(
        private_acquisition_root, private_seal_receipt_paths
    )
    entries = []
    consumed_bindings: set[str] = set()
    for path in result_paths:
        resolved = str(absolute_path(path, "acquisition result", must_exist=True))
        boundary = seal_bindings.get(resolved)
        entries.append(
            media_entry_from_acquisition_result(
                path,
                expected_handling_boundary=boundary,
            )
        )
        if boundary is not None:
            consumed_bindings.add(resolved)
    unused = sorted(set(seal_bindings) - consumed_bindings)
    if unused:
        raise BatchError(
            "private seal receipt does not match an explicitly selected acquisition result"
        )
    entries.sort(
        key=lambda item: (
            item["source_media"]["sha256"],
            item["acquisition_result"]["sha256"],
            item["acquisition_result"]["path"],
        )
    )
    result_paths_seen = [entry["acquisition_result"]["path"] for entry in entries]
    media_seen = [entry["source_media"]["sha256"] for entry in entries]
    entry_ids = [entry["entry_id"] for entry in entries]
    if len(result_paths_seen) != len(set(result_paths_seen)):
        raise BatchError("selection repeats an acquisition result path")
    if len(media_seen) != len(set(media_seen)):
        raise BatchError("selection repeats exact media bytes")
    if len(entry_ids) != len(set(entry_ids)):
        raise BatchError("selection entry identities collide")
    numbered = [{"ordinal": index, **entry} for index, entry in enumerate(entries, 1)]
    core = {
        "schema_version": SCHEMA_VERSION,
        "selection_kind": "completed_acquisition_results_for_media_preprocess",
        "entries": numbered,
        "policy": {
            "maximum_items": MAX_BATCH_ITEMS,
            "completed_results_only": True,
            "exact_result_hash_required": True,
            "exact_media_hash_required": True,
            "duplicate_media_allowed": False,
            "writable_upstream_inputs_guarded": True,
            **(
                {
                    "private_acquisition_seal_required": True,
                    "handling_policy_propagation_required": True,
                }
                if any("handling_boundary" in entry for entry in numbered)
                else {}
            ),
            **SAFETY,
        },
    }
    digest = sha256_bytes(canonical_bytes(core))
    return {
        **core,
        "selection_id": f"pbsel_{digest[:32]}",
        "selection_sha256": digest,
    }


def validate_selection(value: Any) -> dict[str, Any]:
    selection = exact_object(
        value,
        "preprocess batch selection",
        {
            "schema_version",
            "selection_kind",
            "entries",
            "policy",
            "selection_id",
            "selection_sha256",
        },
    )
    if (
        selection["schema_version"] != SCHEMA_VERSION
        or selection["selection_kind"]
        != "completed_acquisition_results_for_media_preprocess"
    ):
        raise BatchError("selection identity is unsupported")
    entries = selection["entries"]
    if not isinstance(entries, list) or not 1 <= len(entries) <= MAX_BATCH_ITEMS:
        raise BatchError(f"selection entries must contain 1 to {MAX_BATCH_ITEMS} rows")
    has_private_entries = any(
        isinstance(entry, dict) and "handling_boundary" in entry
        for entry in entries
    )
    expected_policy = {
        "maximum_items": MAX_BATCH_ITEMS,
        "completed_results_only": True,
        "exact_result_hash_required": True,
        "exact_media_hash_required": True,
        "duplicate_media_allowed": False,
        "writable_upstream_inputs_guarded": True,
        **(
            {
                "private_acquisition_seal_required": True,
                "handling_policy_propagation_required": True,
            }
            if has_private_entries
            else {}
        ),
        **SAFETY,
    }
    if selection["policy"] != expected_policy:
        raise BatchError("selection policy differs from the fail-closed private policy")
    normalized_entries: list[dict[str, Any]] = []
    for expected_ordinal, raw_entry in enumerate(entries, 1):
        entry_keys = {"ordinal", "entry_id", "acquisition_result", "source_media"}
        if isinstance(raw_entry, dict) and "handling_boundary" in raw_entry:
            entry_keys.add("handling_boundary")
        entry = exact_object(
            raw_entry,
            f"selection entry {expected_ordinal}",
            entry_keys,
        )
        if entry["ordinal"] != expected_ordinal:
            raise BatchError("selection ordinals must be contiguous and ordered")
        acquisition = exact_object(
            entry["acquisition_result"],
            "selection acquisition_result",
            {"path", "sha256", "byte_count", "job_id", "work_order_sha256", "completed_at"},
        )
        source = exact_object(
            entry["source_media"],
            "selection source_media",
            {"path", "media_id", "sha256", "byte_count", "duration_ms", "first_cataloged_at"},
        )
        path = absolute_path(acquisition["path"], "selection acquisition result path", must_exist=True)
        result_sha = sha256_value(acquisition["sha256"], "selection acquisition result SHA-256")
        integer(acquisition["byte_count"], "selection acquisition result bytes", minimum=1, maximum=MAX_ACQUISITION_RESULT_BYTES)
        text(acquisition["job_id"], "selection acquisition job_id", 128)
        sha256_value(acquisition["work_order_sha256"], "selection acquisition work-order SHA-256")
        timestamp(acquisition["completed_at"], "selection acquisition completed_at")
        media_path = absolute_path(source["path"], "selection media path", must_exist=True)
        media_sha = sha256_value(source["sha256"], "selection media SHA-256")
        if source["media_id"] != f"media_sha256_{media_sha}":
            raise BatchError("selection media_id disagrees with media SHA-256")
        integer(source["byte_count"], "selection media bytes", minimum=1)
        if source["duration_ms"] is not None:
            integer(source["duration_ms"], "selection duration_ms")
        timestamp(source["first_cataloged_at"], "selection first_cataloged_at")
        boundary = (
            replay_handling_boundary(entry["handling_boundary"])
            if "handling_boundary" in entry
            else None
        )
        expected_entry_id_value = expected_entry_id(
            result_sha256=result_sha,
            media_sha256=media_sha,
            result_path=path,
            media_path=media_path,
            handling_boundary=boundary,
        )
        if entry["entry_id"] != expected_entry_id_value:
            raise BatchError("selection entry_id is inconsistent")
        normalized_entries.append(entry)
    sort_key = lambda item: (
        item["source_media"]["sha256"],
        item["acquisition_result"]["sha256"],
        item["acquisition_result"]["path"],
    )
    if normalized_entries != sorted(normalized_entries, key=sort_key):
        raise BatchError("selection entries are not in canonical order")
    if len({entry["source_media"]["sha256"] for entry in entries}) != len(entries):
        raise BatchError("selection repeats exact media bytes")
    core = {
        key: selection[key]
        for key in ("schema_version", "selection_kind", "entries", "policy")
    }
    digest = sha256_bytes(canonical_bytes(core))
    if (
        selection["selection_sha256"] != digest
        or selection["selection_id"] != f"pbsel_{digest[:32]}"
    ):
        raise BatchError("selection identity or digest is inconsistent")
    return selection


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def ensure_private_directory(path: Path, label: str) -> Path:
    path = Path(os.fspath(path))
    if not path.is_absolute():
        raise BatchError(f"{label} must be absolute")
    path = Path(os.path.abspath(os.fspath(path)))
    validate_output_root(path, label)
    missing: list[Path] = []
    cursor = path
    while not cursor.exists():
        missing.append(cursor)
        if cursor.parent == cursor:
            raise BatchError(f"cannot locate an existing ancestor for {label}")
        cursor = cursor.parent
    ancestor = cursor.lstat()
    if (
        stat.S_ISLNK(ancestor.st_mode)
        or not stat.S_ISDIR(ancestor.st_mode)
        or cursor.resolve(strict=True) != cursor
    ):
        raise BatchError(f"{label} existing ancestor is unsafe")
    for component in reversed(missing):
        component.mkdir(mode=0o700)
        component.chmod(0o700)
    observed = path.lstat()
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or stat.S_IMODE(observed.st_mode) != 0o700
        or observed.st_uid != os.getuid()
        or path.resolve(strict=True) != path
    ):
        raise BatchError(
            f"{label} must be an exact owner-only mode-0700 non-symlink directory"
        )
    return path


def remove_stage(path: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return
    for child in sorted(path.rglob("*"), reverse=True):
        try:
            child.chmod(0o700 if child.is_dir() else 0o600)
        except OSError:
            pass
    try:
        path.chmod(0o700)
    except OSError:
        pass
    shutil.rmtree(path, ignore_errors=True)


@contextmanager
def writer_lock(root: Path, filename: str) -> Iterator[None]:
    root = ensure_private_directory(root, "batch writer root")
    lock_path = root / filename
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as error:
        if error.errno in {errno.ELOOP, errno.EINVAL}:
            raise BatchError(f"unsafe batch lock path: {lock_path}") from error
        raise
    with os.fdopen(descriptor, "a+b") as handle:
        if stat.S_IMODE(os.fstat(handle.fileno()).st_mode) != 0o600:
            raise BatchError("batch lock file must have mode 0600")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise BatchError(f"another batch writer holds {lock_path}") from error
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def create_immutable_file(path: Path, body: bytes, label: str) -> None:
    path.parent.mkdir(parents=False, exist_ok=True)
    if path.exists() or path.is_symlink():
        _, existing, _ = stable_read(path, len(body), label, required_mode=0o400)
        if existing != body:
            raise BatchError(f"existing {label} differs from exact replay")
        return
    temporary = path.with_name(
        f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    )
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        descriptor = os.open(temporary, flags, 0o600)
        try:
            offset = 0
            while offset < len(body):
                offset += os.write(descriptor, body[offset:])
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.chmod(temporary, 0o400)
        try:
            os.link(temporary, path)
        except FileExistsError as error:
            raise BatchError(f"immutable {label} admission raced") from error
        fsync_directory(path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()
    _, observed, _ = stable_read(path, len(body), label, required_mode=0o400)
    if observed != body:
        raise BatchError(f"admitted {label} differs from intended bytes")


def write_selection(
    result_paths: list[Path],
    output_path: Path,
    *,
    private_acquisition_root: Path | None = None,
    private_seal_receipt_paths: list[Path] | None = None,
) -> dict[str, Any]:
    selection = build_selection(
        result_paths,
        private_acquisition_root=private_acquisition_root,
        private_seal_receipt_paths=private_seal_receipt_paths,
    )
    output = absolute_path(output_path, "--output", must_exist=False)
    validate_output_root(output.parent, "selection output parent")
    ensure_private_directory(output.parent, "selection output parent")
    body = pretty_bytes(selection)
    if len(body) > MAX_SELECTION_BYTES:
        raise BatchError("selection exceeds its serialized byte cap")
    with writer_lock(output.parent, ".preprocess-selection.lock"):
        create_immutable_file(output, body, "preprocess batch selection")
    return selection


def read_selection(path: Path) -> tuple[dict[str, Any], bytes, Path]:
    resolved, body, _ = stable_read(
        absolute_path(path, "selection path", must_exist=True),
        MAX_SELECTION_BYTES,
        "preprocess batch selection",
        required_mode=0o400,
    )
    selection = validate_selection(json_value(body, "preprocess batch selection"))
    if body != pretty_bytes(selection):
        raise BatchError("preprocess batch selection is not in canonical sealed serialization")
    return selection, body, resolved


def revalidate_selection_entries(selection: dict[str, Any]) -> None:
    for entry in selection["entries"]:
        boundary = (
            replay_handling_boundary(entry["handling_boundary"])
            if "handling_boundary" in entry
            else None
        )
        observed = media_entry_from_acquisition_result(
            Path(entry["acquisition_result"]["path"]),
            expected_result_sha256=entry["acquisition_result"]["sha256"],
            expected_handling_boundary=boundary,
        )
        expected_keys = ["entry_id", "acquisition_result", "source_media"]
        if boundary is not None:
            expected_keys.append("handling_boundary")
        expected = {key: entry[key] for key in expected_keys}
        if canonical_bytes(observed) != canonical_bytes(expected):
            raise BatchError(
                f"selection entry {entry['ordinal']} differs from current exact acquisition evidence"
            )


def profile_observation(profile_path: Path = DEFAULT_PROFILE) -> dict[str, Any]:
    resolved, body, _ = stable_read(
        profile_path,
        64 * 1024,
        "media preprocess CPU profile",
    )
    profile = media_preprocess.validate_profile(
        json_value(body, "media preprocess CPU profile")
    )
    if profile.get("profile_id") != "cpu-balanced-v1":
        raise BatchError("batch materialization requires cpu-balanced-v1")
    return {
        "path": str(resolved),
        "sha256": sha256_bytes(body),
        "byte_count": len(body),
        "profile": profile,
    }


def active_producer_module_path() -> Path:
    """Bind the producer pin to the successor module Python actually imported."""

    module_file = getattr(media_preprocess, "__file__", None)
    if not isinstance(module_file, str) or not module_file:
        raise BatchError("active media preprocess module has no source path")
    observed = Path(module_file).resolve()
    expected = PRODUCER_PATH.resolve()
    if observed != expected:
        raise BatchError(
            "active media preprocess module path differs from the producer pin"
        )
    return observed


def producer_observation() -> dict[str, Any]:
    active_path = active_producer_module_path()
    try:
        media_preprocess.verify_legacy_source()
    except media_preprocess.PipelineError as error:
        raise BatchError(f"media preprocess successor dependency failed: {error}") from error
    resolved, body, _ = stable_read(
        active_path,
        4 * 1024 * 1024,
        "media preprocess producer",
    )
    return {
        "path": str(resolved),
        "sha256": sha256_bytes(body),
        "byte_count": len(body),
        "implementation_version": media_preprocess.IMPLEMENTATION_VERSION,
    }


def restricted_child_environment() -> dict[str, str]:
    return {"LC_ALL": "C", "LANG": "C", "TZ": "UTC"}


class _PreprocessorEnvironmentDispatcher:
    """Route immutable module hooks through thread-confined guard policies.

    The predecessor implementation temporarily replaced two callables on the
    shared ``media_preprocess`` module.  Independent preprocess and GPU replay
    lanes could interleave save/set/restore operations, leave an old command
    wrapper installed, and eventually build a chain deep enough to raise
    ``RecursionError``.  These two wrappers are installed exactly once.  Guard
    contexts alter only a thread-local policy stack, so the lanes can overlap
    without changing process-global callables.
    """

    MARKER = "__himr_preprocess_environment_dispatcher_v1__"
    MAX_NESTING = 16

    def __init__(self, module: Any) -> None:
        self.module = module
        self._base_command = module.run_command
        self._base_environment = module.subprocess_environment
        self._local = threading.local()

        def command_dispatch(command: list[str]):
            guarded = [
                allowed
                for _token, allowed in self._active_frames()
                if allowed is not None
            ]
            if guarded:
                if not isinstance(command, list) or not command:
                    raise self.module.PipelineError(
                        "batch execution refused a command outside pinned FFmpeg/FFprobe"
                    )
                executable = str(Path(command[0]).resolve())
                if any(executable not in allowed for allowed in guarded):
                    raise self.module.PipelineError(
                        "batch execution refused a command outside pinned FFmpeg/FFprobe"
                    )
                for argument in command[1:]:
                    if not isinstance(argument, str) or re.match(
                        r"^[A-Za-z][A-Za-z0-9+.-]*://", argument
                    ):
                        raise self.module.PipelineError(
                            "batch execution refused a non-local command argument"
                        )
            return self._base_command(command)

        def environment_dispatch() -> dict[str, str]:
            if self._active_frames():
                return restricted_child_environment()
            return self._base_environment()

        setattr(command_dispatch, self.MARKER, self)
        setattr(environment_dispatch, self.MARKER, self)
        self.command_dispatch = command_dispatch
        self.environment_dispatch = environment_dispatch

    def _active_frames(self) -> list[tuple[object, frozenset[str] | None]]:
        frames = getattr(self._local, "frames", None)
        if frames is None:
            return []
        if not isinstance(frames, list) or any(
            not isinstance(frame, tuple)
            or len(frame) != 2
            or (
                frame[1] is not None
                and (not isinstance(frame[1], frozenset) or not frame[1])
            )
            for frame in frames
        ):
            raise BatchError("preprocessor environment policy stack is malformed")
        return frames

    def assert_installed(self) -> None:
        if (
            self.module.run_command is not self.command_dispatch
            or self.module.subprocess_environment is not self.environment_dispatch
        ):
            raise BatchError("media preprocess environment dispatcher was replaced")

    def install(self) -> None:
        if (
            self.module.run_command is not self._base_command
            or self.module.subprocess_environment is not self._base_environment
        ):
            raise BatchError(
                "media preprocess hooks changed before dispatcher installation"
            )
        self.module.run_command = self.command_dispatch
        self.module.subprocess_environment = self.environment_dispatch
        self.assert_installed()

    @contextmanager
    def policy(self, allowed: frozenset[str] | None) -> Iterator[None]:
        self.assert_installed()
        if allowed is not None and (not isinstance(allowed, frozenset) or not allowed):
            raise BatchError("preprocessor command policy is invalid")
        frames = getattr(self._local, "frames", None)
        if frames is None:
            frames = []
            self._local.frames = frames
        if not isinstance(frames, list) or len(frames) >= self.MAX_NESTING:
            raise BatchError("preprocessor environment policy nesting is invalid")
        token = object()
        frame = (token, allowed)
        frames.append(frame)
        cleanup_error: BatchError | None = None
        try:
            yield
        finally:
            current = getattr(self._local, "frames", None)
            if current is not frames or not frames or frames[-1] is not frame:
                cleanup_error = BatchError(
                    "preprocessor environment policy stack changed"
                )
            else:
                frames.pop()
                if not frames:
                    del self._local.frames
            self.assert_installed()
            if cleanup_error is not None:
                raise cleanup_error


def _install_preprocessor_environment_dispatcher() -> Any:
    command = getattr(media_preprocess, "run_command", None)
    environment = getattr(media_preprocess, "subprocess_environment", None)
    command_dispatcher = getattr(
        command, _PreprocessorEnvironmentDispatcher.MARKER, None
    )
    environment_dispatcher = getattr(
        environment, _PreprocessorEnvironmentDispatcher.MARKER, None
    )
    if command_dispatcher is not None or environment_dispatcher is not None:
        if (
            command_dispatcher is None
            or command_dispatcher is not environment_dispatcher
            or not callable(getattr(command_dispatcher, "assert_installed", None))
        ):
            raise BatchError("media preprocess dispatcher registration is inconsistent")
        command_dispatcher.assert_installed()
        return command_dispatcher
    if not callable(command) or not callable(environment):
        raise BatchError("media preprocess module lacks environment hooks")
    dispatcher = _PreprocessorEnvironmentDispatcher(media_preprocess)
    dispatcher.install()
    return dispatcher


_PREPROCESSOR_ENVIRONMENT_DISPATCHER = (
    _install_preprocessor_environment_dispatcher()
)


@contextmanager
def credential_free_tool_environment() -> Iterator[None]:
    with _PREPROCESSOR_ENVIRONMENT_DISPATCHER.policy(None):
        yield


def tool_observations() -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    with credential_free_tool_environment():
        for name in ("ffmpeg", "ffprobe"):
            executable = media_preprocess.require_tool(name)
            result[name] = media_preprocess.executable_provenance(executable, name)
    return result


def verify_producer(
    expected: dict[str, Any], *, require_active: bool = False
) -> None:
    expected = exact_object(
        expected,
        "batch producer pin",
        {"path", "sha256", "byte_count", "implementation_version"},
    )
    expected_path = absolute_path(
        expected["path"], "batch producer path", must_exist=True
    )
    if expected_path not in RECOGNIZED_PRODUCER_PATHS:
        raise BatchError("batch producer path is not an exact recognized producer")
    if require_active and expected_path != PRODUCER_PATH.resolve():
        raise BatchError("pending batch pins a historical inactive producer")
    resolved, body, _ = stable_read(
        expected_path,
        4 * 1024 * 1024,
        "pinned media preprocess producer",
    )
    observed = {
        "path": str(resolved),
        "sha256": sha256_bytes(body),
        "byte_count": len(body),
        "implementation_version": media_preprocess.IMPLEMENTATION_VERSION,
    }
    if observed != expected:
        raise BatchError("media preprocess producer differs from the bundle pin")
    if expected_path == PRODUCER_PATH.resolve():
        active_producer_module_path()
        try:
            media_preprocess.verify_legacy_source()
        except media_preprocess.PipelineError as error:
            raise BatchError(
                f"media preprocess successor dependency failed: {error}"
            ) from error


def _current_tool_path(name: str, row: dict[str, Any]) -> Path:
    current = Path(media_preprocess.require_tool(name)).resolve()
    if current != Path(row.get("path", "")):
        raise BatchError(f"current {name} path differs from the bundle pin")
    return current


def _verify_full_tool_provenance(name: str, row: dict[str, Any]) -> None:
    """Run the producer's complete hash-and-version provenance check."""

    _current_tool_path(name, row)
    try:
        media_preprocess.verify_executable_provenance(row)
    except media_preprocess.CommandError as error:
        if error.returncode < 0:
            raise BatchError(
                f"current {name} provenance check was interrupted by "
                f"signal {-error.returncode}"
            ) from error
        raise BatchError(
            f"current {name} provenance check failed with status "
            f"{error.returncode}"
        ) from error
    except media_preprocess.PipelineError as error:
        raise BatchError(f"current {name} build differs from the bundle pin") from error


def _verify_tool_executable_bytes(name: str, row: dict[str, Any]) -> None:
    """Hash one pinned executable through a stable descriptor without executing it.

    A restore witness is allowed to reuse only version-output work.  Every bundle
    replay still resolves the active PATH entry and hashes the executable bytes
    through a no-follow descriptor bracket, including ctime and path identity.
    """

    requested = _current_tool_path(name, row)
    try:
        path_before = requested.lstat()
    except OSError as error:
        raise BatchError(f"current {name} executable cannot be inspected") from error
    if stat.S_ISLNK(path_before.st_mode):
        raise BatchError(f"current {name} executable may not be a symlink")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(requested, flags)
    except OSError as error:
        raise BatchError(f"current {name} executable cannot be opened safely") from error
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size != row["executable_byte_count"]
        ):
            raise BatchError(
                f"current {name} executable byte count differs from the bundle pin"
            )
        digest = hashlib.sha256()
        offset = 0
        while offset < before.st_size:
            chunk = os.pread(
                descriptor,
                min(1024 * 1024, before.st_size - offset),
                offset,
            )
            if not chunk:
                raise BatchError(f"current {name} executable ended during hashing")
            digest.update(chunk)
            offset += len(chunk)
        after = os.fstat(descriptor)
        try:
            path_after = requested.lstat()
        except OSError as error:
            raise BatchError(
                f"current {name} executable changed while being hashed"
            ) from error
        if (
            stat.S_ISLNK(path_after.st_mode)
            or stat_identity(path_before) != stat_identity(before)
            or stat_identity(before) != stat_identity(after)
            or stat_identity(after) != stat_identity(path_after)
        ):
            raise BatchError(f"current {name} executable changed while being hashed")
        if digest.hexdigest() != row["executable_sha256"]:
            raise BatchError(
                f"current {name} executable SHA-256 differs from the bundle pin"
            )
    finally:
        os.close(descriptor)


class _RestoreToolProvenanceWitness:
    """Bounded, context-local witness for one controller restore.

    The first observation and scope exit execute the complete producer check.
    Intermediate observations reuse only the exact version-output witness while
    continuing to hash and stat-bracket both executable files for every bundle.
    """

    TOOL_NAMES = ("ffmpeg", "ffprobe")

    def __init__(self) -> None:
        self.pid = os.getpid()
        self.thread_id = threading.get_ident()
        self.rows: dict[str, tuple[bytes, dict[str, Any]]] = {}
        self.closed = False

    def _assert_owner(self) -> None:
        if self.closed:
            raise RestoreToolProvenanceError(
                "restore tool provenance witness is closed"
            )
        if os.getpid() != self.pid:
            raise RestoreToolProvenanceError(
                "restore tool provenance witness cannot survive a fork"
            )
        if threading.get_ident() != self.thread_id:
            raise RestoreToolProvenanceError(
                "restore tool provenance witness changed threads"
            )

    def verify(self, name: str, row: dict[str, Any]) -> None:
        try:
            self._assert_owner()
            if name not in self.TOOL_NAMES:
                raise RestoreToolProvenanceError(
                    "restore tool provenance witness received an unknown tool"
                )
            validated = validate_tool_row(row, name)
            row_body = canonical_bytes(validated)
            prior = self.rows.get(name)
            if prior is None:
                _verify_full_tool_provenance(name, validated)
                # Tool rows contain JSON scalars only. Parse canonical bytes to
                # keep the exit pin independent from later caller mutation.
                retained = json_value(row_body, f"retained {name} provenance")
                if not isinstance(retained, dict):  # pragma: no cover
                    raise RestoreToolProvenanceError(
                        "retained tool provenance is malformed"
                    )
                self.rows[name] = (row_body, retained)
                return
            if prior[0] != row_body:
                raise RestoreToolProvenanceError(
                    f"batch {name} provenance differs from the active restore witness"
                )
            _verify_tool_executable_bytes(name, validated)
        except RestoreToolProvenanceError:
            raise
        except BatchError as error:
            raise RestoreToolProvenanceError(str(error)) from error

    def close(self) -> None:
        try:
            try:
                self._assert_owner()
                for name in self.TOOL_NAMES:
                    prior = self.rows.get(name)
                    if prior is None:
                        continue
                    row_body, retained = prior
                    if canonical_bytes(retained) != row_body:
                        raise RestoreToolProvenanceError(
                            "retained tool provenance witness changed"
                        )
                    _verify_full_tool_provenance(name, retained)
            except RestoreToolProvenanceError:
                raise
            except BatchError as error:
                raise RestoreToolProvenanceError(str(error)) from error
        finally:
            self.closed = True
            self.rows.clear()


_RESTORE_TOOL_PROVENANCE_WITNESS: ContextVar[
    _RestoreToolProvenanceWitness | None
] = ContextVar("himr_restore_tool_provenance_witness", default=None)


@contextmanager
def restore_scoped_tool_provenance_witness() -> Iterator[None]:
    """Coalesce only identical tool version probes within one restore call."""

    if _RESTORE_TOOL_PROVENANCE_WITNESS.get() is not None:
        raise RestoreToolProvenanceError(
            "nested restore tool provenance witnesses are forbidden"
        )
    witness = _RestoreToolProvenanceWitness()
    token = _RESTORE_TOOL_PROVENANCE_WITNESS.set(witness)
    try:
        yield
    finally:
        try:
            witness.close()
        finally:
            _RESTORE_TOOL_PROVENANCE_WITNESS.reset(token)


def verify_tools(expected: dict[str, Any]) -> None:
    tools = exact_object(expected, "batch tools", {"ffmpeg", "ffprobe"})
    with credential_free_tool_environment():
        for name in ("ffmpeg", "ffprobe"):
            row = tools[name]
            if not isinstance(row, dict) or row.get("name") != name:
                raise BatchError(f"batch {name} observation is malformed")
            witness = _RESTORE_TOOL_PROVENANCE_WITNESS.get()
            if witness is None:
                _verify_full_tool_provenance(name, row)
            else:
                witness.verify(name, row)


def build_work_order(
    entry: dict[str, Any],
    *,
    selection_sha256: str,
    processing_output_root: Path,
    profile: dict[str, Any],
    operations: dict[str, bool] | None = None,
) -> dict[str, Any]:
    ordinal = entry["ordinal"]
    job_id = f"preprocess-{selection_sha256[:12]}-{ordinal:06d}"
    raw = {
        "schema_version": media_preprocess.CONTRACT_VERSION,
        "job_id": job_id,
        "source": {
            "path": entry["source_media"]["path"],
            "expected_sha256": entry["source_media"]["sha256"],
            "first_cataloged_at": entry["source_media"]["first_cataloged_at"],
        },
        "output": {"root": str(processing_output_root)},
        "operations": dict(FULL_OPERATIONS if operations is None else operations),
        "profile": profile,
    }
    try:
        return media_preprocess.validate_work_order(raw)
    except media_preprocess.PipelineError as error:
        raise BatchError(f"cannot build work order {ordinal}: {error}") from error


def handling_control(selection: dict[str, Any]) -> dict[str, Any] | None:
    rows = []
    for entry in selection["entries"]:
        boundary = entry.get("handling_boundary")
        if boundary is None:
            continue
        seal = boundary["private_acquisition_seal"]
        rows.append(
            {
                "ordinal": entry["ordinal"],
                "entry_id": entry["entry_id"],
                "handling_policy": boundary["handling_policy"],
                "handling_boundary_sha256": sha256_bytes(
                    canonical_bytes(boundary)
                ),
                "seal_receipt_sha256": seal["receipt_sha256"],
                "seal_plan_sha256": seal["plan_sha256"],
                "source_byte_identity_claimed": False,
            }
        )
    if not rows:
        return None
    core = {
        "kind": "v30_private_acquisition_handling_control",
        "private_entry_count": len(rows),
        "entries": rows,
        "seal_receipt_replay_required": True,
        "handling_policy_propagation_required": True,
        "publication_authority": "none",
    }
    return {
        **core,
        "identity_sha256": sha256_bytes(canonical_bytes(core)),
    }


def build_bundle(
    selection: dict[str, Any],
    selection_path: Path,
    selection_body: bytes,
    processing_output_root: Path,
    *,
    profile_path: Path = DEFAULT_PROFILE,
    operation_profile: str = "full",
) -> tuple[dict[str, Any], list[tuple[str, bytes]]]:
    revalidate_selection_entries(selection)
    processing_output_root = private_output_root_reference(
        processing_output_root, "processing output root"
    )
    profile = profile_observation(profile_path)
    if operation_profile not in OPERATION_PROFILES:
        raise BatchError(
            "operation_profile must be one of: "
            + ", ".join(sorted(OPERATION_PROFILES))
        )
    operations = OPERATION_PROFILES[operation_profile]
    producer = producer_observation()
    tools = tool_observations()
    order_files: list[tuple[str, bytes]] = []
    descriptors: list[dict[str, Any]] = []
    for entry in selection["entries"]:
        order = build_work_order(
            entry,
            selection_sha256=selection["selection_sha256"],
            processing_output_root=processing_output_root,
            profile=profile["profile"],
            operations=operations,
        )
        body = pretty_bytes(order)
        if len(body) > MAX_WORK_ORDER_BYTES:
            raise BatchError("generated work order exceeds its byte cap")
        relative = f"work-orders/{entry['ordinal']:06d}.json"
        order_files.append((relative, body))
        descriptor = {
                "ordinal": entry["ordinal"],
                "entry_id": entry["entry_id"],
                "job_id": order["job_id"],
                "path": relative,
                "sha256": sha256_bytes(body),
                "byte_count": len(body),
            }
        if "handling_boundary" in entry:
            descriptor["handling_boundary_sha256"] = sha256_bytes(
                canonical_bytes(entry["handling_boundary"])
            )
        descriptors.append(descriptor)
    private_control = handling_control(selection)
    core = {
        "schema_version": SCHEMA_VERSION,
        "bundle_kind": "private_media_preprocess_work_order_batch",
        "materializer": {
            "name": "himr-preprocess-batch",
            "version": IMPLEMENTATION_VERSION,
        },
        "selection": {
            "path": str(selection_path),
            "selection_id": selection["selection_id"],
            "sha256": sha256_bytes(selection_body),
            "byte_count": len(selection_body),
            "semantic_sha256": selection["selection_sha256"],
        },
        "profile": profile,
        "producer": producer,
        "tools": tools,
        "processing_output_root": str(processing_output_root),
        "work_order_count": len(descriptors),
        "work_orders": descriptors,
        **({"handling_control": private_control} if private_control is not None else {}),
        "safety": {
            **SAFETY,
            **(
                {
                    "private_acquisition_seal_required": True,
                    "handling_policy_propagation_required": True,
                }
                if private_control is not None
                else {}
            ),
            "sequential_execution_only": True,
            "default_run_limit": DEFAULT_RUN_LIMIT,
            "maximum_items": MAX_BATCH_ITEMS,
            "receipt_required_for_completion": True,
        },
    }
    identity_sha256 = sha256_bytes(canonical_bytes(core))
    bundle_id = f"ppbatch_{identity_sha256[:32]}"
    without_digest = {
        **core,
        "bundle_id": bundle_id,
        "bundle_relative_path": f"bundles/{bundle_id}",
        "identity_sha256": identity_sha256,
    }
    manifest_sha256 = sha256_bytes(canonical_bytes(without_digest))
    return {**without_digest, "manifest_sha256": manifest_sha256}, order_files


def validate_tool_row(value: Any, name: str) -> dict[str, Any]:
    row = exact_object(
        value,
        f"bundle tool {name}",
        {
            "name",
            "path",
            "executable_sha256",
            "executable_byte_count",
            "version",
            "version_output",
            "version_output_sha256",
            "build_configuration",
        },
    )
    if row["name"] != name:
        raise BatchError(f"bundle tool {name} has the wrong name")
    absolute_path(row["path"], f"bundle tool {name} path", must_exist=True)
    sha256_value(row["executable_sha256"], f"bundle tool {name} SHA-256")
    integer(row["executable_byte_count"], f"bundle tool {name} bytes", minimum=1)
    text(row["version"], f"bundle tool {name} version", 4_096)
    version_output = text(
        row["version_output"], f"bundle tool {name} version output", 512 * 1024
    )
    version_sha = sha256_value(
        row["version_output_sha256"], f"bundle tool {name} version SHA-256"
    )
    if sha256_bytes(version_output.encode("utf-8")) != version_sha:
        raise BatchError(f"bundle tool {name} version output hash is inconsistent")
    if row["build_configuration"] is not None:
        text(row["build_configuration"], f"bundle tool {name} build configuration", 256 * 1024)
    return row


def validate_bundle_manifest(
    value: Any,
    *,
    manifest_path: Path,
    verify_runtime: bool = True,
) -> tuple[dict[str, Any], dict[str, Any], bytes]:
    manifest_keys = {
        "schema_version",
        "bundle_kind",
        "materializer",
        "selection",
        "profile",
        "producer",
        "tools",
        "processing_output_root",
        "work_order_count",
        "work_orders",
        "safety",
        "bundle_id",
        "bundle_relative_path",
        "identity_sha256",
        "manifest_sha256",
    }
    if isinstance(value, dict) and "handling_control" in value:
        manifest_keys.add("handling_control")
    manifest = exact_object(
        value,
        "preprocess batch manifest",
        manifest_keys,
    )
    if (
        manifest["schema_version"] != SCHEMA_VERSION
        or manifest["bundle_kind"] != "private_media_preprocess_work_order_batch"
    ):
        raise BatchError("batch manifest identity is unsupported")
    materializer = exact_object(
        manifest["materializer"], "batch materializer", {"name", "version"}
    )
    if (
        materializer.get("name") != "himr-preprocess-batch"
        or materializer.get("version") not in SUPPORTED_IMPLEMENTATION_VERSIONS
    ):
        raise BatchError("batch materializer identity is unsupported")
    selection_ref = exact_object(
        manifest["selection"],
        "batch selection reference",
        {"path", "selection_id", "sha256", "byte_count", "semantic_sha256"},
    )
    selection_path = absolute_path(
        selection_ref["path"], "batch selection path", must_exist=True
    )
    selection, selection_body, observed_selection_path = read_selection(selection_path)
    if observed_selection_path != selection_path:
        raise BatchError("batch selection path resolved inconsistently")
    if (
        selection_ref["selection_id"] != selection["selection_id"]
        or selection_ref["sha256"] != sha256_bytes(selection_body)
        or selection_ref["byte_count"] != len(selection_body)
        or selection_ref["semantic_sha256"] != selection["selection_sha256"]
    ):
        raise BatchError("batch selection reference differs from exact selection bytes")

    profile_ref = exact_object(
        manifest["profile"],
        "batch profile",
        {"path", "sha256", "byte_count", "profile"},
    )
    profile_path = absolute_path(profile_ref["path"], "batch profile path", must_exist=True)
    profile = media_preprocess.validate_profile(profile_ref["profile"])
    if profile.get("profile_id") != "cpu-balanced-v1":
        raise BatchError("batch profile is not cpu-balanced-v1")
    sha256_value(profile_ref["sha256"], "batch profile SHA-256")
    integer(profile_ref["byte_count"], "batch profile bytes", minimum=1, maximum=64 * 1024)
    if verify_runtime:
        observed_profile = profile_observation(profile_path)
        if observed_profile != profile_ref:
            raise BatchError("current CPU profile differs from the batch pin")

    producer = exact_object(
        manifest["producer"],
        "batch producer",
        {"path", "sha256", "byte_count", "implementation_version"},
    )
    absolute_path(producer["path"], "batch producer path", must_exist=True)
    sha256_value(producer["sha256"], "batch producer SHA-256")
    integer(producer["byte_count"], "batch producer bytes", minimum=1, maximum=4 * 1024 * 1024)
    text(producer["implementation_version"], "batch producer version", 64)
    tools = exact_object(manifest["tools"], "batch tools", {"ffmpeg", "ffprobe"})
    for name in ("ffmpeg", "ffprobe"):
        validate_tool_row(tools[name], name)
    if verify_runtime:
        verify_producer(producer)
        verify_tools(tools)

    processing_output_root = private_output_root_reference(
        manifest["processing_output_root"],
        "batch processing output root",
    )
    count = integer(
        manifest["work_order_count"],
        "batch work_order_count",
        minimum=1,
        maximum=MAX_BATCH_ITEMS,
    )
    descriptors = manifest["work_orders"]
    if not isinstance(descriptors, list) or len(descriptors) != count:
        raise BatchError("batch work-order descriptor count is inconsistent")
    bundle_dir = manifest_path.parent
    if manifest_path != bundle_dir / "manifest.json":
        raise BatchError("batch manifest must be named manifest.json")
    seen_paths: set[str] = set()
    batch_operations: dict[str, bool] | None = None
    for ordinal, descriptor_raw in enumerate(descriptors, 1):
        descriptor_keys = {"ordinal", "entry_id", "job_id", "path", "sha256", "byte_count"}
        entry = selection["entries"][ordinal - 1]
        if "handling_boundary" in entry:
            descriptor_keys.add("handling_boundary_sha256")
        descriptor = exact_object(
            descriptor_raw,
            f"batch work-order descriptor {ordinal}",
            descriptor_keys,
        )
        expected_relative = f"work-orders/{ordinal:06d}.json"
        if (
            descriptor["ordinal"] != ordinal
            or descriptor["path"] != expected_relative
            or descriptor["entry_id"] != entry["entry_id"]
        ):
            raise BatchError("batch work-order descriptor ordering is inconsistent")
        if "handling_boundary" in entry:
            expected_boundary_sha256 = sha256_bytes(
                canonical_bytes(entry["handling_boundary"])
            )
            if descriptor["handling_boundary_sha256"] != expected_boundary_sha256:
                raise BatchError(
                    "batch work-order descriptor drops its private handling boundary"
                )
        if descriptor["path"] in seen_paths:
            raise BatchError("batch repeats a work-order path")
        seen_paths.add(descriptor["path"])
        if not ID_RE.fullmatch(text(descriptor["job_id"], "batch job_id", 128)):
            raise BatchError("batch work-order job_id is invalid")
        work_path = bundle_dir / descriptor["path"]
        resolved_work, work_body, _ = stable_read(
            work_path,
            MAX_WORK_ORDER_BYTES,
            f"batch work order {ordinal}",
            required_mode=0o400,
        )
        if resolved_work != work_path:
            raise BatchError("batch work-order path resolved inconsistently")
        if (
            descriptor["sha256"] != sha256_bytes(work_body)
            or descriptor["byte_count"] != len(work_body)
        ):
            raise BatchError("batch work-order bytes differ from their descriptor")
        order_raw = json_value(work_body, f"batch work order {ordinal}")
        try:
            order = media_preprocess.validate_work_order(order_raw)
        except media_preprocess.PipelineError as error:
            raise BatchError(f"batch work order {ordinal} is invalid: {error}") from error
        operations = order["operations"]
        if operations not in OPERATION_PROFILES.values():
            raise BatchError(
                "batch work order uses an unsupported preprocessing operation profile"
            )
        if materializer["version"] in {"0.1.0", "0.2.0"} and operations != FULL_OPERATIONS:
            raise BatchError(
                "legacy preprocess batches must use the full operation profile"
            )
        if batch_operations is None:
            batch_operations = operations
        elif operations != batch_operations:
            raise BatchError("batch mixes preprocessing operation profiles")
        expected_order = build_work_order(
            entry,
            selection_sha256=selection["selection_sha256"],
            processing_output_root=processing_output_root,
            profile=profile,
            operations=operations,
        )
        if order != expected_order or order["job_id"] != descriptor["job_id"]:
            raise BatchError("batch work order differs from deterministic reconstruction")

    expected_handling_control = handling_control(selection)
    if expected_handling_control is None:
        if "handling_control" in manifest:
            raise BatchError("public batch may not introduce private handling control")
    elif manifest.get("handling_control") != expected_handling_control:
        raise BatchError(
            "batch handling control differs from its exact private selection"
        )
    expected_safety = {
        **SAFETY,
        **(
            {
                "private_acquisition_seal_required": True,
                "handling_policy_propagation_required": True,
            }
            if expected_handling_control is not None
            else {}
        ),
        "sequential_execution_only": True,
        "default_run_limit": DEFAULT_RUN_LIMIT,
        "maximum_items": MAX_BATCH_ITEMS,
        "receipt_required_for_completion": True,
    }
    if manifest["safety"] != expected_safety:
        raise BatchError("batch safety policy differs from the private offline contract")
    core_keys = {
        "schema_version",
        "bundle_kind",
        "materializer",
        "selection",
        "profile",
        "producer",
        "tools",
        "processing_output_root",
        "work_order_count",
        "work_orders",
        "safety",
    }
    if expected_handling_control is not None:
        core_keys.add("handling_control")
    core = {key: manifest[key] for key in core_keys}
    identity_sha256 = sha256_bytes(canonical_bytes(core))
    bundle_id = f"ppbatch_{identity_sha256[:32]}"
    if (
        manifest["identity_sha256"] != identity_sha256
        or manifest["bundle_id"] != bundle_id
        or manifest["bundle_relative_path"] != f"bundles/{bundle_id}"
        or bundle_dir.name != bundle_id
    ):
        raise BatchError("batch identity or path is inconsistent")
    without_digest = {key: manifest[key] for key in manifest if key != "manifest_sha256"}
    manifest_sha256 = sha256_bytes(canonical_bytes(without_digest))
    if manifest["manifest_sha256"] != manifest_sha256:
        raise BatchError("batch manifest SHA-256 is inconsistent")
    return manifest, selection, selection_body


def expected_bundle_tree(manifest: dict[str, Any]) -> set[str]:
    return {
        "manifest.json",
        "work-orders",
        *[row["path"] for row in manifest["work_orders"]],
    }


def validate_bundle(
    bundle_dir: Path,
    *,
    verify_runtime: bool = True,
    revalidate_inputs: bool = True,
    expected_admission_parent: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    bundle = exact_existing_directory(bundle_dir, "bundle directory")
    if expected_admission_parent is not None:
        expected_parent = exact_existing_directory(
            expected_admission_parent, "batch admission directory"
        )
        if bundle.parent != expected_parent:
            raise BatchError("bundle is outside its exact admission directory")
    bundle_stat = bundle.lstat()
    if (
        stat.S_ISLNK(bundle_stat.st_mode)
        or not stat.S_ISDIR(bundle_stat.st_mode)
        or stat.S_IMODE(bundle_stat.st_mode) != 0o500
    ):
        raise BatchError("bundle directory must be immutable mode 0500")
    manifest_path = bundle / "manifest.json"
    _, manifest_body, _ = stable_read(
        manifest_path,
        MAX_BUNDLE_MANIFEST_BYTES,
        "preprocess batch manifest",
        required_mode=0o400,
    )
    manifest, selection, _ = validate_bundle_manifest(
        json_value(manifest_body, "preprocess batch manifest"),
        manifest_path=manifest_path,
        verify_runtime=verify_runtime,
    )
    if manifest_body != pretty_bytes(manifest):
        raise BatchError("preprocess batch manifest is not in canonical sealed serialization")
    observed_tree = {path.relative_to(bundle).as_posix() for path in bundle.rglob("*")}
    if observed_tree != expected_bundle_tree(manifest):
        raise BatchError("immutable batch bundle has missing or extra entries")
    work_orders_dir = bundle / "work-orders"
    if (
        work_orders_dir.is_symlink()
        or not work_orders_dir.is_dir()
        or stat.S_IMODE(work_orders_dir.stat().st_mode) != 0o500
    ):
        raise BatchError("batch work-orders directory must be immutable mode 0500")
    if revalidate_inputs:
        revalidate_selection_entries(selection)
    orders: list[dict[str, Any]] = []
    for descriptor in manifest["work_orders"]:
        _, body, _ = stable_read(
            bundle / descriptor["path"],
            MAX_WORK_ORDER_BYTES,
            "batch work order",
            required_mode=0o400,
        )
        try:
            orders.append(media_preprocess.validate_work_order(json_value(body, "batch work order")))
        except media_preprocess.PipelineError as error:
            raise BatchError(f"batch work order is invalid: {error}") from error
    return manifest, selection, orders


def verify_existing_bundle(
    bundle_dir: Path,
    expected_manifest: dict[str, Any],
    order_files: list[tuple[str, bytes]],
    *,
    expected_admission_parent: Path,
) -> None:
    manifest, _selection, _orders = validate_bundle(
        bundle_dir, expected_admission_parent=expected_admission_parent
    )
    if canonical_bytes(manifest) != canonical_bytes(expected_manifest):
        raise BatchError("existing immutable batch manifest differs from exact replay")
    for relative, expected in order_files:
        _, observed, _ = stable_read(
            bundle_dir / relative,
            MAX_WORK_ORDER_BYTES,
            "existing batch work order",
            required_mode=0o400,
        )
        if observed != expected:
            raise BatchError(f"existing batch work order differs from replay: {relative}")


def admit_bundle(
    bundle_root: Path,
    manifest: dict[str, Any],
    order_files: list[tuple[str, bytes]],
) -> Path:
    root = ensure_private_directory(bundle_root, "bundle root")
    with writer_lock(root, ".preprocess-batch-materializer.lock"):
        bundles = root / "bundles"
        if bundles.exists():
            ensure_private_directory(bundles, "batch admission directory")
        else:
            bundles.mkdir(mode=0o700)
        final = bundles / manifest["bundle_id"]
        if final.is_symlink():
            raise BatchError("immutable batch admission path may not be a symlink")
        if final.exists():
            verify_existing_bundle(
                final,
                manifest,
                order_files,
                expected_admission_parent=bundles,
            )
            return final
        staging = root / ".staging"
        if staging.exists():
            ensure_private_directory(staging, "batch staging directory")
        else:
            staging.mkdir(mode=0o700)
        stage = Path(tempfile.mkdtemp(prefix=f".{manifest['bundle_id']}.", dir=staging))
        stage.chmod(0o700)
        try:
            work_orders_dir = stage / "work-orders"
            work_orders_dir.mkdir(mode=0o700)
            for relative, body in order_files:
                path = stage / relative
                with path.open("xb") as handle:
                    handle.write(body)
                    handle.flush()
                    os.fsync(handle.fileno())
                path.chmod(0o400)
            manifest_body = pretty_bytes(manifest)
            if len(manifest_body) > MAX_BUNDLE_MANIFEST_BYTES:
                raise BatchError("batch manifest exceeds its serialized byte cap")
            with (stage / "manifest.json").open("xb") as handle:
                handle.write(manifest_body)
                handle.flush()
                os.fsync(handle.fileno())
            (stage / "manifest.json").chmod(0o400)
            work_orders_dir.chmod(0o500)
            fsync_directory(work_orders_dir)
            fsync_directory(stage)
            if final.exists() or final.is_symlink():
                raise BatchError("immutable batch admission target appeared during admission")
            os.rename(stage, final)
            final.chmod(0o500)
            fsync_directory(bundles)
        except Exception:
            remove_stage(stage)
            raise
        verify_existing_bundle(
            final,
            manifest,
            order_files,
            expected_admission_parent=bundles,
        )
        return final


def materialize_bundle(
    selection_path: Path,
    bundle_root: Path,
    processing_output_root: Path,
    *,
    profile_path: Path = DEFAULT_PROFILE,
    operation_profile: str = "full",
) -> Path:
    selection, selection_body, resolved_selection = read_selection(selection_path)
    processing_root = private_output_root_reference(
        processing_output_root, "--processing-output-root"
    )
    manifest, order_files = build_bundle(
        selection,
        resolved_selection,
        selection_body,
        processing_root,
        profile_path=profile_path,
        operation_profile=operation_profile,
    )
    return admit_bundle(bundle_root, manifest, order_files)


def readonly_file(
    path: Path,
    maximum: int,
    label: str,
    *,
    allow_hardlinks: bool = False,
) -> tuple[Path, bytes, os.stat_result]:
    """Read one sealed producer file while accepting 0400 or 0444."""

    resolved, body, observed = stable_read(
        path, maximum, label, allow_hardlinks=allow_hardlinks
    )
    if stat.S_IMODE(observed.st_mode) & 0o222:
        raise BatchError(f"{label} must be sealed read-only")
    return resolved, body, observed


def require_private_directory(path: Path, label: str) -> Path:
    resolved = exact_existing_directory(path, label)
    observed = resolved.lstat()
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or stat.S_IMODE(observed.st_mode) != 0o700
        or observed.st_uid != os.getuid()
    ):
        raise BatchError(
            f"{label} must be an exact owner-only mode-0700 non-symlink directory"
        )
    return resolved


def require_descendant(path: Path, root: Path, label: str) -> None:
    try:
        path.relative_to(root)
    except ValueError as error:
        raise BatchError(f"{label} escapes its private output root") from error


def validate_private_ancestor_chain(path: Path, root: Path, label: str) -> None:
    """Require every existing directory from root through path to be owner-only."""

    require_descendant(path, root, label)
    cursor = path
    while True:
        if cursor.exists() or cursor.is_symlink():
            observed = cursor.lstat()
            if (
                stat.S_ISLNK(observed.st_mode)
                or not stat.S_ISDIR(observed.st_mode)
                or stat.S_IMODE(observed.st_mode) != 0o700
                or observed.st_uid != os.getuid()
            ):
                raise BatchError(
                    f"{label} contains a non-private or unsafe directory: {cursor}"
                )
        if cursor == root:
            break
        cursor = cursor.parent


def result_validation_context(
    order: dict[str, Any],
    entry: dict[str, Any],
    manifest: dict[str, Any],
) -> dict[str, Any]:
    """Reconstruct the producer's exact validation context from sealed pins."""

    source = Path(order["source"]["path"])
    source_row = entry["source_media"]
    if (
        str(source) != source_row["path"]
        or order["source"]["expected_sha256"] != source_row["sha256"]
    ):
        raise BatchError("work order source differs from its selection entry")
    ffmpeg = manifest["tools"]["ffmpeg"]
    ffprobe = manifest["tools"]["ffprobe"]
    try:
        source_probe, _ = media_preprocess.probe_file(
            ffprobe["path"],
            source,
            media_id=source_row["media_id"],
            sha256=source_row["sha256"],
            ffprobe_version=ffprobe["version"],
        )
        recipe, recipe_sha256 = media_preprocess.work_recipe(order, ffmpeg, ffprobe)
    except media_preprocess.PipelineError as error:
        raise BatchError(f"cannot reconstruct preprocessing validation context: {error}") from error
    recipe_id = f"recipe_preprocess_{recipe_sha256[:32]}"
    output_root = Path(order["output"]["root"])
    recipe_dir = (
        output_root
        / "media"
        / "sha256"
        / source_row["sha256"][:2]
        / source_row["sha256"]
        / "recipes"
        / recipe_sha256
    )
    return {
        "source_sha256": source_row["sha256"],
        "source_byte_count": source_row["byte_count"],
        "recipe": recipe,
        "recipe_sha256": recipe_sha256,
        "recipe_id": recipe_id,
        "recipe_dir": recipe_dir,
        "work_order": order,
        "ffprobe": ffprobe["path"],
        "ffprobe_version": ffprobe["version"],
        "source_duration_ms": source_probe["format"]["duration_ms"],
        "video_index": source_probe["primary_streams"]["video_index"],
        "audio_index": source_probe["primary_streams"]["audio_index"],
    }


def validate_completed_result(
    result_path: Path,
    order: dict[str, Any],
    entry: dict[str, Any],
    manifest: dict[str, Any],
) -> dict[str, Any]:
    """Deeply validate one completed producer result and return receipt pins."""

    resolved_result, result_body, _ = readonly_file(
        result_path,
        MAX_PREPROCESS_RESULT_BYTES,
        "preprocess result",
    )
    result = json_value(result_body, "preprocess result")
    if not isinstance(result, dict):
        raise BatchError("preprocess result must be a JSON object")
    if result.get("result_path") != str(resolved_result):
        raise BatchError("preprocess result path disagrees with its envelope")
    if result.get("job_id") != order["job_id"]:
        raise BatchError("preprocess result job_id differs from its work order")
    input_row = result.get("input")
    source_row = entry["source_media"]
    source_path = Path(source_row["path"])
    if not isinstance(input_row, dict) or (
        input_row.get("path") != str(source_path)
        or input_row.get("storage_uri") != source_path.as_uri()
        or input_row.get("media_id") != source_row["media_id"]
        or input_row.get("sha256") != source_row["sha256"]
        or input_row.get("byte_count") != source_row["byte_count"]
    ):
        raise BatchError("preprocess result input differs from its selection pin")

    processing_root = Path(manifest["processing_output_root"])
    require_descendant(resolved_result, processing_root, "preprocess result")
    validate_private_ancestor_chain(
        resolved_result.parent, processing_root, "preprocess result directory chain"
    )
    with guarded_preprocessor_environment(manifest):
        context = result_validation_context(order, entry, manifest)
    expected_recipe_dir = context["recipe_dir"]
    require_descendant(resolved_result, expected_recipe_dir, "preprocess result")

    artifacts = result.get("artifacts")
    if not isinstance(artifacts, list) or not 1 <= len(artifacts) <= MAX_ARTIFACTS:
        raise BatchError(f"preprocess result must have 1 to {MAX_ARTIFACTS} artifacts")
    observed_artifacts: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    seen_kinds: set[str] = set()
    for index, raw in enumerate(artifacts, 1):
        if not isinstance(raw, dict):
            raise BatchError(f"preprocess artifact {index} must be an object")
        artifact_path = absolute_path(
            raw.get("path"), f"preprocess artifact {index} path", must_exist=True
        )
        kind = text(raw.get("artifact_kind"), f"preprocess artifact {index} kind", 128)
        if str(artifact_path) in seen_paths or kind in seen_kinds:
            raise BatchError("preprocess result repeats an artifact path or kind")
        seen_paths.add(str(artifact_path))
        seen_kinds.add(kind)
        require_descendant(artifact_path, resolved_result.parent, f"artifact {kind}")
        validate_private_ancestor_chain(
            artifact_path.parent, processing_root, f"artifact {kind} directory chain"
        )
        digest = sha256_value(raw.get("sha256"), f"artifact {kind} SHA-256")
        byte_count = integer(raw.get("byte_count"), f"artifact {kind} bytes", minimum=1)
        if raw.get("storage_uri") != artifact_path.as_uri():
            raise BatchError(f"artifact {kind} storage URI disagrees with its path")
        mime_type = raw.get("mime_type")
        if mime_type is not None:
            text(mime_type, f"artifact {kind} MIME type", 256)
        if mime_type == "application/json":
            _, json_body, json_stat = readonly_file(
                artifact_path,
                MAX_PREPROCESS_RESULT_BYTES,
                f"artifact {kind}",
                allow_hardlinks=True,
            )
            if json_stat.st_size != byte_count or sha256_bytes(json_body) != digest:
                raise BatchError(f"artifact {kind} differs from its result descriptor")
            json_value(json_body, f"artifact {kind}")
        else:
            artifact_stat = artifact_path.lstat()
            if stat.S_IMODE(artifact_stat.st_mode) & 0o222:
                raise BatchError(f"artifact {kind} must be sealed read-only")
            stable_hash_media(
                artifact_path,
                expected_sha256=digest,
                expected_byte_count=byte_count,
                label=f"artifact {kind}",
            )
        observed_artifacts.append(
            {
                "artifact_id": text(raw.get("artifact_id"), f"artifact {kind} id", 128),
                "artifact_kind": kind,
                "path": str(artifact_path),
                "storage_uri": artifact_path.as_uri(),
                "sha256": digest,
                "byte_count": byte_count,
                "media_kind": text(raw.get("media_kind"), f"artifact {kind} media kind", 64),
                "mime_type": mime_type,
            }
        )

    try:
        with guarded_preprocessor_environment(manifest):
            validated = media_preprocess.validate_prior_result(resolved_result, **context)
    except media_preprocess.PipelineError as error:
        raise BatchError(f"preprocess result failed producer validation: {error}") from error
    run = validated["processing_run"]
    reuse = validated["reuse"]
    observed_artifacts.sort(key=lambda row: (row["artifact_kind"], row["path"]))
    return {
        "result": {
            "path": str(resolved_result),
            "sha256": sha256_bytes(result_body),
            "byte_count": len(result_body),
            "processing_run_id": run["processing_run_id"],
            "recipe_sha256": context["recipe_sha256"],
            "reuse_mode": reuse["mode"],
        },
        "artifacts": observed_artifacts,
    }


def receipt_core(
    manifest: dict[str, Any],
    entry: dict[str, Any],
    descriptor: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "receipt_kind": "completed_private_media_preprocess_batch_item",
        "bundle_id": manifest["bundle_id"],
        "bundle_manifest_sha256": manifest["manifest_sha256"],
        "ordinal": entry["ordinal"],
        "entry_id": entry["entry_id"],
        "job_id": descriptor["job_id"],
        "work_order": {
            "path": descriptor["path"],
            "sha256": descriptor["sha256"],
            "byte_count": descriptor["byte_count"],
        },
        "acquisition_result": dict(entry["acquisition_result"]),
        "source_media": dict(entry["source_media"]),
        **(
            {"handling_boundary": entry["handling_boundary"]}
            if "handling_boundary" in entry
            else {}
        ),
        "preprocess_result": dict(observation["result"]),
        "artifacts": [dict(row) for row in observation["artifacts"]],
        "safety": {
            **SAFETY,
            **(
                {
                    "private_acquisition_seal_required": True,
                    "handling_policy_propagation_required": True,
                }
                if "handling_boundary" in entry
                else {}
            ),
            "network_access_performed": False,
            "publication_performed": False,
            "exact_validation_completed": True,
        },
    }


def build_receipt(
    manifest: dict[str, Any],
    entry: dict[str, Any],
    descriptor: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    core = receipt_core(manifest, entry, descriptor, observation)
    digest = sha256_bytes(canonical_bytes(core))
    return {
        **core,
        "receipt_id": f"ppreceipt_{digest[:32]}",
        "receipt_sha256": digest,
    }


def validate_receipt(
    value: Any,
    *,
    receipt_path: Path,
    manifest: dict[str, Any],
    selection: dict[str, Any],
    orders: list[dict[str, Any]],
) -> dict[str, Any]:
    receipt_keys = {
        "schema_version",
        "receipt_kind",
        "bundle_id",
        "bundle_manifest_sha256",
        "ordinal",
        "entry_id",
        "job_id",
        "work_order",
        "acquisition_result",
        "source_media",
        "preprocess_result",
        "artifacts",
        "safety",
        "receipt_id",
        "receipt_sha256",
    }
    if isinstance(value, dict) and "handling_boundary" in value:
        receipt_keys.add("handling_boundary")
    receipt = exact_object(
        value,
        "preprocess batch receipt",
        receipt_keys,
    )
    if (
        receipt["schema_version"] != SCHEMA_VERSION
        or receipt["receipt_kind"]
        != "completed_private_media_preprocess_batch_item"
        or receipt["bundle_id"] != manifest["bundle_id"]
        or receipt["bundle_manifest_sha256"] != manifest["manifest_sha256"]
    ):
        raise BatchError("receipt is for a different or unsupported batch")
    ordinal = integer(
        receipt["ordinal"],
        "receipt ordinal",
        minimum=1,
        maximum=manifest["work_order_count"],
    )
    expected_path = receipt_path.parent / f"{ordinal:06d}.json"
    if receipt_path != expected_path:
        raise BatchError("receipt path is inconsistent with its ordinal")
    entry = selection["entries"][ordinal - 1]
    if ("handling_boundary" in receipt) != ("handling_boundary" in entry):
        raise BatchError("receipt drops or invents a private handling boundary")
    if "handling_boundary" in receipt:
        replay_handling_boundary(receipt["handling_boundary"])
    descriptor = manifest["work_orders"][ordinal - 1]
    order = orders[ordinal - 1]
    result_ref = exact_object(
        receipt["preprocess_result"],
        "receipt preprocess_result",
        {
            "path",
            "sha256",
            "byte_count",
            "processing_run_id",
            "recipe_sha256",
            "reuse_mode",
        },
    )
    result_path = absolute_path(
        result_ref["path"], "receipt preprocess result path", must_exist=True
    )
    observation = validate_completed_result(result_path, order, entry, manifest)
    expected = build_receipt(manifest, entry, descriptor, observation)
    if canonical_bytes(receipt) != canonical_bytes(expected):
        raise BatchError("receipt differs from current exact result and batch pins")
    return receipt


def read_receipt(
    path: Path,
    *,
    manifest: dict[str, Any],
    selection: dict[str, Any],
    orders: list[dict[str, Any]],
    allow_recovery_hardlink: bool = False,
) -> tuple[dict[str, Any], str]:
    resolved, body, _ = stable_read(
        path,
        MAX_RECEIPT_BYTES,
        "preprocess batch receipt",
        required_mode=0o400,
        allow_hardlinks=allow_recovery_hardlink,
    )
    receipt = validate_receipt(
        json_value(body, "preprocess batch receipt"),
        receipt_path=resolved,
        manifest=manifest,
        selection=selection,
        orders=orders,
    )
    if body != pretty_bytes(receipt):
        raise BatchError("preprocess batch receipt is not in canonical sealed serialization")
    return receipt, sha256_bytes(body)


def receipt_temporary_target(name: str) -> str | None:
    match = RECEIPT_TEMP_RE.fullmatch(name) or LEGACY_RECEIPT_TEMP_RE.fullmatch(name)
    return match.group(1) if match else None


def scan_receipt_directory(
    receipts_dir: Path,
    *,
    expected_names: set[str],
) -> tuple[dict[str, Path], list[dict[str, Any]]]:
    final_paths: dict[str, Path] = {}
    temporary_rows: list[dict[str, Any]] = []
    for path in sorted(receipts_dir.iterdir()):
        if path.name in expected_names:
            final_paths[path.name] = path
            continue
        target_name = receipt_temporary_target(path.name)
        if target_name is None or target_name not in expected_names:
            raise BatchError(
                f"batch receipts directory contains an extra entry: {path.name}"
            )
        if len(temporary_rows) >= MAX_RECEIPT_TEMP_FILES:
            raise BatchError("batch receipts directory exceeds its temporary-file cap")
        try:
            observed = path.lstat()
        except OSError as error:
            raise BatchError("receipt temporary changed during state inspection") from error
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.getuid()
            or stat.S_IMODE(observed.st_mode) not in {0o400, 0o600}
            or observed.st_size > MAX_RECEIPT_BYTES
            or observed.st_nlink not in {1, 2}
        ):
            raise BatchError(f"unsafe receipt temporary file: {path.name}")
        target = receipts_dir / target_name
        linked_to_final = False
        if observed.st_nlink == 2:
            try:
                target_stat = target.lstat()
            except OSError as error:
                raise BatchError(
                    f"receipt temporary hard link has no final peer: {path.name}"
                ) from error
            if (
                stat.S_ISLNK(target_stat.st_mode)
                or not stat.S_ISREG(target_stat.st_mode)
                or target_stat.st_dev != observed.st_dev
                or target_stat.st_ino != observed.st_ino
                or target_stat.st_nlink != 2
            ):
                raise BatchError(
                    f"receipt temporary hard link has an unsafe final peer: {path.name}"
                )
            linked_to_final = True
        temporary_rows.append(
            {
                "path": path,
                "target_name": target_name,
                "linked_to_final": linked_to_final,
                "identity": stat_identity(observed),
            }
        )
    return final_paths, temporary_rows


def recover_receipt_temporaries(
    receipts_dir: Path,
    *,
    expected_names: set[str],
) -> int:
    _finals, temporary_rows = scan_receipt_directory(
        receipts_dir, expected_names=expected_names
    )
    for row in temporary_rows:
        path = row["path"]
        try:
            current = path.lstat()
        except OSError as error:
            raise BatchError("receipt temporary changed before recovery") from error
        if stat_identity(current) != row["identity"]:
            raise BatchError("receipt temporary changed before recovery")
        path.unlink()
    if temporary_rows:
        fsync_directory(receipts_dir)
    return len(temporary_rows)


def state_paths(state_root: Path, bundle_id: str) -> tuple[Path, Path, Path]:
    root = absolute_path(state_root, "batch state root", must_exist=False)
    run_dir = root / "runs" / bundle_id
    return root, run_dir, run_dir / "receipts"


def existing_receipts(
    state_root: Path,
    *,
    manifest: dict[str, Any],
    selection: dict[str, Any],
    orders: list[dict[str, Any]],
) -> dict[int, dict[str, Any]]:
    root, run_dir, receipts_dir = state_paths(state_root, manifest["bundle_id"])
    validate_output_root(root, "batch state root")
    if not root.exists() and not root.is_symlink():
        return {}
    require_private_directory(root, "batch state root")
    runs = root / "runs"
    if not runs.exists() and not runs.is_symlink():
        return {}
    require_private_directory(runs, "batch state runs directory")
    if not run_dir.exists() and not run_dir.is_symlink():
        return {}
    require_private_directory(run_dir, "batch state run directory")
    run_entries = sorted(run_dir.iterdir())
    if not receipts_dir.exists() and not receipts_dir.is_symlink():
        if run_entries:
            raise BatchError(
                "incomplete batch state run directory contains unexpected entries"
            )
        return {}
    require_private_directory(receipts_dir, "batch receipts directory")
    if [path.name for path in run_entries] != ["receipts"]:
        raise BatchError("batch state run directory contains an extra entry")
    expected_names = {
        f"{ordinal:06d}.json"
        for ordinal in range(1, manifest["work_order_count"] + 1)
    }
    final_paths, temporary_rows = scan_receipt_directory(
        receipts_dir, expected_names=expected_names
    )
    recovery_hardlink_inodes = {
        row["identity"][:2]
        for row in temporary_rows
        if row["linked_to_final"]
    }
    observed: dict[int, dict[str, Any]] = {}
    for name, path in sorted(final_paths.items()):
        path_stat = path.lstat()
        allow_recovery_hardlink = (
            path_stat.st_nlink == 2
            and (path_stat.st_dev, path_stat.st_ino) in recovery_hardlink_inodes
        )
        receipt, physical_sha256 = read_receipt(
            path,
            manifest=manifest,
            selection=selection,
            orders=orders,
            allow_recovery_hardlink=allow_recovery_hardlink,
        )
        observed[receipt["ordinal"]] = {
            "receipt": receipt,
            "physical_sha256": physical_sha256,
        }
    return observed


def state_digest(receipts: dict[int, dict[str, Any]]) -> str:
    return sha256_bytes(
        canonical_bytes(
            [
                {
                    "ordinal": ordinal,
                    "receipt_id": row["receipt"]["receipt_id"],
                    "receipt_sha256": row["receipt"]["receipt_sha256"],
                    "physical_sha256": row["physical_sha256"],
                }
                for ordinal, row in sorted(receipts.items())
            ]
        )
    )


def run_summary(
    manifest: dict[str, Any],
    receipts: dict[int, dict[str, Any]],
    *,
    dry_run: bool,
    newly_completed: list[int] | None = None,
) -> dict[str, Any]:
    completed = sorted(receipts)
    pending = [
        ordinal
        for ordinal in range(1, manifest["work_order_count"] + 1)
        if ordinal not in receipts
    ]
    private_control = manifest.get("handling_control")
    return {
        "schema_version": SCHEMA_VERSION,
        "stage": "private_media_preprocess_batch_runner",
        "status": "complete" if not pending else "pending",
        "dry_run": dry_run,
        "bundle_id": manifest["bundle_id"],
        "item_count": manifest["work_order_count"],
        "completed_count": len(completed),
        "pending_count": len(pending),
        "completed_ordinals": completed,
        "pending_ordinals": pending,
        "newly_completed_ordinals": sorted(newly_completed or []),
        "state_sha256": state_digest(receipts),
        **({"handling_control": private_control} if private_control is not None else {}),
        "safety": {
            **SAFETY,
            **(
                {
                    "private_acquisition_seal_required": True,
                    "handling_policy_propagation_required": True,
                }
                if private_control is not None
                else {}
            ),
            "network_access_performed": False,
            "publication_performed": False,
            "sequential_execution_only": True,
        },
    }


def batch_status(bundle_dir: Path, state_root: Path) -> dict[str, Any]:
    manifest, selection, orders = validate_bundle(bundle_dir)
    receipts = existing_receipts(
        state_root,
        manifest=manifest,
        selection=selection,
        orders=orders,
    )
    return run_summary(manifest, receipts, dry_run=True)


def ensure_state_directories(state_root: Path, bundle_id: str) -> tuple[Path, Path, Path]:
    root = ensure_private_directory(state_root, "batch state root")
    runs = root / "runs"
    if runs.exists() or runs.is_symlink():
        require_private_directory(runs, "batch state runs directory")
    else:
        runs.mkdir(mode=0o700)
    run_dir = runs / bundle_id
    if run_dir.exists() or run_dir.is_symlink():
        require_private_directory(run_dir, "batch state run directory")
    else:
        run_dir.mkdir(mode=0o700)
    receipts = run_dir / "receipts"
    if receipts.exists() or receipts.is_symlink():
        require_private_directory(receipts, "batch receipts directory")
        if sorted(path.name for path in run_dir.iterdir()) != ["receipts"]:
            raise BatchError("batch state run directory contains an extra entry")
    else:
        if list(run_dir.iterdir()):
            raise BatchError(
                "incomplete batch state run directory contains unexpected entries"
            )
        receipts.mkdir(mode=0o700)
    fsync_directory(receipts)
    fsync_directory(run_dir)
    fsync_directory(runs)
    return root, run_dir, receipts


@contextmanager
def private_umask() -> Iterator[None]:
    previous = os.umask(0o077)
    try:
        yield
    finally:
        os.umask(previous)


@contextmanager
def guarded_preprocessor_environment(manifest: dict[str, Any]) -> Iterator[None]:
    """Constrain child commands to pinned tools, local arguments, and no credentials."""

    try:
        allowed = frozenset(
            str(Path(manifest["tools"][name]["path"]).resolve())
            for name in ("ffmpeg", "ffprobe")
        )
    except (KeyError, TypeError) as error:
        raise BatchError("batch tool policy is incomplete") from error
    with _PREPROCESSOR_ENVIRONMENT_DISPATCHER.policy(allowed):
        yield


def run_batch(
    bundle_dir: Path,
    state_root: Path,
    *,
    limit: int = DEFAULT_RUN_LIMIT,
    dry_run: bool = False,
) -> dict[str, Any]:
    limit = integer(limit, "run limit", minimum=1, maximum=MAX_BATCH_ITEMS)
    manifest, selection, orders = validate_bundle(bundle_dir)
    receipts = existing_receipts(
        state_root,
        manifest=manifest,
        selection=selection,
        orders=orders,
    )
    if dry_run:
        return run_summary(manifest, receipts, dry_run=True)

    with private_umask():
        root, _run_dir, receipts_dir = ensure_state_directories(
            state_root, manifest["bundle_id"]
        )
        lock_name = f".preprocess-batch-{manifest['bundle_id']}.lock"
        with writer_lock(root, lock_name):
            manifest, selection, orders = validate_bundle(bundle_dir)
            expected_receipt_names = {
                f"{ordinal:06d}.json"
                for ordinal in range(1, manifest["work_order_count"] + 1)
            }
            recover_receipt_temporaries(
                receipts_dir, expected_names=expected_receipt_names
            )
            receipts = existing_receipts(
                state_root,
                manifest=manifest,
                selection=selection,
                orders=orders,
            )
            pending = [
                ordinal
                for ordinal in range(1, manifest["work_order_count"] + 1)
                if ordinal not in receipts
            ]
            processing_root = ensure_private_directory(
                Path(manifest["processing_output_root"]),
                "batch processing output root",
            )
            newly_completed: list[int] = []
            for ordinal in pending[:limit]:
                entry = selection["entries"][ordinal - 1]
                descriptor = manifest["work_orders"][ordinal - 1]
                order = orders[ordinal - 1]
                boundary = (
                    replay_handling_boundary(entry["handling_boundary"])
                    if "handling_boundary" in entry
                    else None
                )
                current_entry = media_entry_from_acquisition_result(
                    Path(entry["acquisition_result"]["path"]),
                    expected_result_sha256=entry["acquisition_result"]["sha256"],
                    expected_handling_boundary=boundary,
                )
                expected_keys = ["entry_id", "acquisition_result", "source_media"]
                if boundary is not None:
                    expected_keys.append("handling_boundary")
                expected_entry = {
                    key: entry[key]
                    for key in expected_keys
                }
                if canonical_bytes(current_entry) != canonical_bytes(expected_entry):
                    raise BatchError(f"batch item {ordinal} changed before execution")
                verify_producer(manifest["producer"], require_active=True)
                verify_tools(manifest["tools"])
                with guarded_preprocessor_environment(manifest):
                    context = result_validation_context(order, entry, manifest)
                    validate_private_ancestor_chain(
                        context["recipe_dir"],
                        processing_root,
                        f"batch item {ordinal} recipe directory chain",
                    )
                    try:
                        returned = media_preprocess.run_work_order(order, dry_run=False)
                    except media_preprocess.PipelineError as error:
                        raise BatchError(
                            f"media preprocessing failed for item {ordinal}: {error}"
                        ) from error
                    if not isinstance(returned, dict) or not isinstance(
                        returned.get("result_path"), str
                    ):
                        raise BatchError(
                            f"media preprocessing returned no completed result for item {ordinal}"
                        )
                    result_path = absolute_path(
                        returned["result_path"],
                        f"batch item {ordinal} returned result path",
                        must_exist=True,
                    )
                    _, result_body, _ = readonly_file(
                        result_path,
                        MAX_PREPROCESS_RESULT_BYTES,
                        f"batch item {ordinal} returned result",
                    )
                    if canonical_bytes(json_value(result_body, "returned preprocess result")) != canonical_bytes(returned):
                        raise BatchError(
                            f"batch item {ordinal} returned object differs from its immutable result"
                        )
                    observation = validate_completed_result(
                        result_path, order, entry, manifest
                    )
                verify_producer(manifest["producer"], require_active=True)
                verify_tools(manifest["tools"])
                current_entry = media_entry_from_acquisition_result(
                    Path(entry["acquisition_result"]["path"]),
                    expected_result_sha256=entry["acquisition_result"]["sha256"],
                    expected_handling_boundary=replay_handling_boundary(
                        entry["handling_boundary"]
                    )
                    if "handling_boundary" in entry
                    else None,
                )
                if canonical_bytes(current_entry) != canonical_bytes(expected_entry):
                    raise BatchError(f"batch item {ordinal} changed during execution")
                receipt = build_receipt(manifest, entry, descriptor, observation)
                receipt_body = pretty_bytes(receipt)
                if len(receipt_body) > MAX_RECEIPT_BYTES:
                    raise BatchError("generated preprocess batch receipt exceeds its byte cap")
                receipt_path = receipts_dir / f"{ordinal:06d}.json"
                create_immutable_file(
                    receipt_path,
                    receipt_body,
                    f"preprocess batch receipt {ordinal}",
                )
                read_receipt(
                    receipt_path,
                    manifest=manifest,
                    selection=selection,
                    orders=orders,
                )
                newly_completed.append(ordinal)
            receipts = existing_receipts(
                state_root,
                manifest=manifest,
                selection=selection,
                orders=orders,
            )
            return run_summary(
                manifest,
                receipts,
                dry_run=False,
                newly_completed=newly_completed,
            )


def validation_summary(
    *,
    kind: str,
    identifier: str,
    item_count: int,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "valid",
        "kind": kind,
        "identifier": identifier,
        "item_count": item_count,
        "safety": dict(SAFETY),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize and sequentially run private media-preprocess work-order batches"
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    create = subparsers.add_parser(
        "create-selection",
        help="seal completed acquisition result paths into one immutable selection",
    )
    create.add_argument(
        "--acquisition-result",
        action="append",
        required=True,
        help=f"absolute result.json path; repeat 1 to {MAX_BATCH_ITEMS} times",
    )
    create.add_argument(
        "--private-acquisition-root",
        help=(
            "owner-only v30 artifact root; required when any selected result carries "
            "a handling_policy"
        ),
    )
    create.add_argument(
        "--private-seal-receipt",
        action="append",
        help=(
            "exact v30 seal receipt below --private-acquisition-root; repeat once "
            "for every selected private result"
        ),
    )
    create.add_argument("--output", required=True, help="new immutable selection path")

    validate_selection_parser = subparsers.add_parser(
        "validate-selection",
        help="revalidate selection bytes and every exact acquisition/media pin",
    )
    validate_selection_parser.add_argument("--selection", required=True)

    materialize = subparsers.add_parser(
        "materialize",
        help="create an immutable deterministic bundle of CPU work orders",
    )
    materialize.add_argument("--selection", required=True)
    materialize.add_argument("--bundle-root", required=True)
    materialize.add_argument("--processing-output-root", required=True)
    materialize.add_argument(
        "--lane",
        choices=sorted(OPERATION_PROFILES),
        default="full",
        help=(
            "full creates audio, proxy, and routing artifacts; asr-ready creates "
            "only probe plus normalized FLAC; enrichment-only creates probe, proxy, "
            "and routing without regenerating normalized audio"
        ),
    )

    validate_bundle_parser = subparsers.add_parser(
        "validate-bundle",
        help="revalidate bundle bytes, runtime pins, and acquisition inputs",
    )
    validate_bundle_parser.add_argument("--bundle", required=True)

    status_parser = subparsers.add_parser(
        "status",
        help="validate immutable receipts and report completed/pending ordinals",
    )
    status_parser.add_argument("--bundle", required=True)
    status_parser.add_argument("--state-root", required=True)

    run_parser = subparsers.add_parser(
        "run",
        help="execute pending work orders sequentially and seal receipts",
    )
    run_parser.add_argument("--bundle", required=True)
    run_parser.add_argument("--state-root", required=True)
    run_parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_RUN_LIMIT,
        help=f"maximum new items this invocation (default {DEFAULT_RUN_LIMIT})",
    )
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate and report only; create no state or processing output",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "create-selection":
            selection = write_selection(
                [Path(path) for path in args.acquisition_result],
                Path(args.output),
                private_acquisition_root=(
                    Path(args.private_acquisition_root)
                    if args.private_acquisition_root
                    else None
                ),
                private_seal_receipt_paths=[
                    Path(path) for path in (args.private_seal_receipt or [])
                ],
            )
            result = validation_summary(
                kind="preprocess_batch_selection",
                identifier=selection["selection_id"],
                item_count=len(selection["entries"]),
            )
        elif args.command == "validate-selection":
            selection, _body, _path = read_selection(Path(args.selection))
            revalidate_selection_entries(selection)
            result = validation_summary(
                kind="preprocess_batch_selection",
                identifier=selection["selection_id"],
                item_count=len(selection["entries"]),
            )
        elif args.command == "materialize":
            bundle = materialize_bundle(
                Path(args.selection),
                Path(args.bundle_root),
                Path(args.processing_output_root),
                operation_profile=args.lane,
            )
            manifest, _selection, _orders = validate_bundle(bundle)
            result = validation_summary(
                kind="private_media_preprocess_work_order_batch",
                identifier=manifest["bundle_id"],
                item_count=manifest["work_order_count"],
            )
        elif args.command == "validate-bundle":
            manifest, _selection, _orders = validate_bundle(Path(args.bundle))
            result = validation_summary(
                kind="private_media_preprocess_work_order_batch",
                identifier=manifest["bundle_id"],
                item_count=manifest["work_order_count"],
            )
        elif args.command == "status":
            result = batch_status(Path(args.bundle), Path(args.state_root))
        else:
            result = run_batch(
                Path(args.bundle),
                Path(args.state_root),
                limit=args.limit,
                dry_run=args.dry_run,
            )
        sys.stdout.buffer.write(pretty_bytes(result))
        return 0
    except (BatchError, media_preprocess.PipelineError, OSError) as error:
        failure = {
            "schema_version": SCHEMA_VERSION,
            "status": "failed",
            "error": {"type": type(error).__name__, "message": str(error)},
        }
        sys.stderr.buffer.write(pretty_bytes(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
