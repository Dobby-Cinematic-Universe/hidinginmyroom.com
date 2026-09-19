#!/usr/bin/env python3
"""Materialize one validated queue-plan v1 into an immutable private work-order bundle.

This boundary performs no acquisition, network access, catalog import, or database
mutation.  It deliberately replays the queue planner's deterministic semantics instead
of trusting queue ordinals or candidate URLs carried by an input document.
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import json
import os
import re
import shutil
import stat
import sys
import tempfile
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote

import acquire


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
MAX_PLAN_BYTES = 64 * 1024 * 1024
LOCK_FILENAME = ".queue-materializer.lock"
DEFAULT_FORMAT_SELECTOR = "bv*[height<=720]+ba/b[height<=720]/b"
DEFAULT_HTTP_TIMEOUT_SECONDS = 60
YOUTUBE_ID = re.compile(r"^[A-Za-z0-9_-]{11}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
IDENTIFIER = re.compile(r"^[^\x00-\x1f\x7f]{1,500}$")
PLAN_ID = re.compile(r"^acqplan_[0-9a-f]{32}$")
DEFER_REASONS = frozenset(
    {
        "requires_metadata",
        "requires_chunking",
        "review_required",
        "plan_item_limit",
        "plan_byte_budget",
        "outside_explicit_selection",
    }
)
QUEUE_STATES = frozenset(
    {"ready", "requires_metadata", "requires_chunking", "review_required"}
)
PRIORITIES = {
    "explicit_selection": 10,
    "wiki_cited": 20,
    "current_public_upload": 30,
    "public_guest_or_clip": 35,
    "short_archive": 40,
    "medium_archive": 50,
    "long_recording": 60,
}


class MaterializationError(RuntimeError):
    """A queue contract, integrity, policy, or immutable-admission failure."""


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
        raise MaterializationError(f"value cannot be canonically encoded: {error}") from error


def pretty_bytes(value: Any) -> bytes:
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


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:32]}"


def reject_json_constant(value: str) -> None:
    raise MaterializationError(f"JSON non-finite numeric constant is forbidden: {value}")


def reject_duplicate_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise MaterializationError(f"JSON object contains duplicate key: {key}")
        result[key] = value
    return result


def load_json_bytes(body: bytes, label: str) -> Any:
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as error:
        raise MaterializationError(f"{label} must be UTF-8 JSON") from error
    try:
        return json.loads(
            text,
            object_pairs_hook=reject_duplicate_pairs,
            parse_constant=reject_json_constant,
        )
    except json.JSONDecodeError as error:
        raise MaterializationError(f"cannot parse {label}: {error}") from error


def exact_object(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise MaterializationError(f"{label} must be a JSON object")
    unknown = sorted(set(value) - keys)
    missing = sorted(keys - set(value))
    if unknown or missing:
        raise MaterializationError(
            f"{label} keys differ from the exact contract; "
            f"missing={missing}, unknown={unknown}"
        )
    return value


def integer(value: Any, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise MaterializationError(f"{label} must be an integer >= {minimum}")
    return value


def text(value: Any, label: str, maximum: int = 2_000) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise MaterializationError(f"{label} must be a non-empty bounded string")
    return value


def nullable_integer(value: Any, label: str) -> int | None:
    return None if value is None else integer(value, label, 1)


def nullable_sha256(value: Any, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not SHA256.fullmatch(value):
        raise MaterializationError(f"{label} must be null or a lowercase SHA-256")
    return value


def required_sha256(value: Any, label: str) -> str:
    result = nullable_sha256(value, label)
    if result is None:
        raise MaterializationError(f"{label} must be a lowercase SHA-256")
    return result


def sorted_unique_strings(
    value: Any,
    label: str,
    *,
    maximum: int = 500,
    pattern: re.Pattern[str] | None = None,
) -> list[str]:
    if not isinstance(value, list):
        raise MaterializationError(f"{label} must be a string array")
    result: list[str] = []
    for index, item in enumerate(value):
        item = text(item, f"{label}[{index}]", maximum)
        if pattern is not None and not pattern.fullmatch(item):
            raise MaterializationError(f"{label}[{index}] has an invalid identifier")
        result.append(item)
    if result != sorted(result) or len(result) != len(set(result)):
        raise MaterializationError(f"{label} must be sorted and unique")
    return result


def validate_utc_timestamp(value: Any, label: str) -> str:
    value = text(value, label, 100)
    if not value.endswith("Z"):
        raise MaterializationError(f"{label} must be an RFC 3339 UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise MaterializationError(f"{label} is not a valid RFC 3339 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise MaterializationError(f"{label} must use UTC")
    return value


def count_entries(values: list[str]) -> list[dict[str, Any]]:
    return [
        {"key": key, "count": count}
        for key, count in sorted(Counter(values).items())
    ]


def validate_count_entries(value: Any, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise MaterializationError(f"{label} must be an array")
    result = []
    for index, raw in enumerate(value):
        row = exact_object(raw, f"{label}[{index}]", {"key", "count"})
        result.append(
            {
                "key": text(row["key"], f"{label}[{index}].key", 500),
                "count": integer(row["count"], f"{label}[{index}].count", 1),
            }
        )
    if result != sorted(result, key=lambda row: row["key"]):
        raise MaterializationError(f"{label} must be sorted by key")
    if len({row["key"] for row in result}) != len(result):
        raise MaterializationError(f"{label} contains duplicate keys")
    return result


def reconstructed_source(candidate: dict[str, Any]) -> tuple[str, str]:
    platform = candidate["platform"]
    source_kind = candidate["source_kind"]
    native_id = candidate["native_id"]
    if platform == "youtube" and source_kind == "youtube_video":
        if not YOUTUBE_ID.fullmatch(native_id):
            raise MaterializationError("YouTube candidate native_id is invalid")
        return f"https://www.youtube.com/watch?v={native_id}", "yt_dlp"
    if platform == "internet_archive" and source_kind == "archive_media_file":
        if "/" not in native_id:
            raise MaterializationError("Archive.org candidate native_id requires item/file")
        item, filename = native_id.split("/", 1)
        if (
            not item
            or not filename
            or any(part in {".", ".."} for part in native_id.split("/"))
            or not IDENTIFIER.fullmatch(item)
            or "\x00" in filename
        ):
            raise MaterializationError("Archive.org candidate native_id is unsafe")
        url = (
            f"https://archive.org/download/{quote(item, safe='')}/"
            f"{quote(filename, safe='/()[],-_.')}"
        )
        return url, "direct_http"
    raise MaterializationError(
        f"unsupported candidate source type: {platform}/{source_kind}"
    )


def validate_candidate(
    raw: Any,
    index: int,
    *,
    limits: dict[str, Any],
    selection: dict[str, Any],
) -> dict[str, Any]:
    label = f"queue plan.candidates[{index}]"
    keys = {
        "recording_id",
        "source_id",
        "platform",
        "source_kind",
        "native_id",
        "title",
        "canonical_url",
        "adapter",
        "recording_type",
        "duration_ms",
        "estimated_bytes",
        "estimate_basis",
        "expected_byte_count",
        "expected_sha256",
        "source_class",
        "mapping_roles",
        "priority",
        "priority_tier",
        "reason_codes",
        "wiki_reference_count",
        "wiki_reference_paths",
        "queue_state",
        "queue_ordinal",
        "defer_reason",
    }
    row = exact_object(raw, label, keys)
    candidate: dict[str, Any] = {
        "recording_id": text(row["recording_id"], f"{label}.recording_id", 500),
        "source_id": text(row["source_id"], f"{label}.source_id", 500),
        "platform": row["platform"],
        "source_kind": text(row["source_kind"], f"{label}.source_kind", 500),
        "native_id": text(row["native_id"], f"{label}.native_id", 2_000),
        "title": text(row["title"], f"{label}.title", 2_000),
        "canonical_url": text(row["canonical_url"], f"{label}.canonical_url", 8_000),
        "adapter": row["adapter"],
        "recording_type": text(row["recording_type"], f"{label}.recording_type", 500),
        "duration_ms": nullable_integer(row["duration_ms"], f"{label}.duration_ms"),
        "estimated_bytes": integer(row["estimated_bytes"], f"{label}.estimated_bytes", 1),
        "estimate_basis": row["estimate_basis"],
        "expected_byte_count": nullable_integer(
            row["expected_byte_count"], f"{label}.expected_byte_count"
        ),
        "expected_sha256": nullable_sha256(
            row["expected_sha256"], f"{label}.expected_sha256"
        ),
        "source_class": row["source_class"],
        "mapping_roles": sorted_unique_strings(
            row["mapping_roles"], f"{label}.mapping_roles"
        ),
        "priority": integer(row["priority"], f"{label}.priority", 1),
        "priority_tier": row["priority_tier"],
        "reason_codes": sorted_unique_strings(
            row["reason_codes"], f"{label}.reason_codes"
        ),
        "wiki_reference_count": integer(
            row["wiki_reference_count"], f"{label}.wiki_reference_count"
        ),
        "wiki_reference_paths": sorted_unique_strings(
            row["wiki_reference_paths"], f"{label}.wiki_reference_paths", maximum=2_000
        ),
        "queue_state": row["queue_state"],
        "queue_ordinal": nullable_integer(
            row["queue_ordinal"], f"{label}.queue_ordinal"
        ),
        "defer_reason": row["defer_reason"],
    }
    if (
        not isinstance(candidate["platform"], str)
        or candidate["platform"] not in {"youtube", "internet_archive"}
    ):
        raise MaterializationError(f"{label}.platform is unsupported")
    canonical_url, adapter = reconstructed_source(candidate)
    if candidate["canonical_url"] != canonical_url:
        raise MaterializationError(f"{label}.canonical_url was not canonically reconstructed")
    if not isinstance(candidate["adapter"], str) or candidate["adapter"] != adapter:
        raise MaterializationError(f"{label}.adapter disagrees with its source type")
    if candidate["source_class"] is not None:
        text(candidate["source_class"], f"{label}.source_class", 500)
    if (
        not isinstance(candidate["priority_tier"], str)
        or candidate["priority_tier"] not in PRIORITIES
    ):
        raise MaterializationError(f"{label}.priority_tier is unsupported")
    if (
        not isinstance(candidate["queue_state"], str)
        or candidate["queue_state"] not in QUEUE_STATES
    ):
        raise MaterializationError(f"{label}.queue_state is unsupported")
    if candidate["defer_reason"] is not None and (
        not isinstance(candidate["defer_reason"], str)
        or candidate["defer_reason"] not in DEFER_REASONS
    ):
        raise MaterializationError(f"{label}.defer_reason is unsupported")
    if candidate["wiki_reference_count"] != len(candidate["wiki_reference_paths"]):
        raise MaterializationError(f"{label} wiki reference count does not match its paths")
    for path in candidate["wiki_reference_paths"]:
        parsed = Path(path)
        if parsed.is_absolute() or any(part == ".." for part in parsed.parts):
            raise MaterializationError(f"{label}.wiki_reference_paths contains an unsafe path")

    duration_ms = candidate["duration_ms"]
    expected_bytes = candidate["expected_byte_count"]
    estimate_basis = candidate["estimate_basis"]
    if estimate_basis == "provider_declared_byte_count":
        if expected_bytes is None or candidate["estimated_bytes"] != expected_bytes:
            raise MaterializationError(f"{label} provider byte estimate is inconsistent")
    elif estimate_basis == "duration_conservative_rate":
        if duration_ms is None or expected_bytes is not None:
            raise MaterializationError(f"{label} duration estimate is inconsistent")
        expected_estimate = (
            duration_ms * limits["youtube_bytes_per_second"] + 999
        ) // 1000 + limits["youtube_fixed_overhead_bytes"]
        if candidate["estimated_bytes"] != expected_estimate:
            raise MaterializationError(f"{label} conservative byte estimate was tampered")
    elif estimate_basis == "job_cap_unknown_duration":
        if (
            duration_ms is not None
            or expected_bytes is not None
            or candidate["estimated_bytes"] != limits["max_job_bytes"]
        ):
            raise MaterializationError(f"{label} unknown-duration estimate is inconsistent")
    else:
        raise MaterializationError(f"{label}.estimate_basis is unsupported")

    selected_native = (
        candidate["platform"] == "youtube"
        and candidate["native_id"] in selection["youtube_video_ids"]
    )
    selected_source = candidate["source_id"] in selection["source_ids"]
    selected_recording = candidate["recording_id"] in selection["recording_ids"]
    explicit = selected_native or selected_source or selected_recording
    reasons: list[str] = []
    if explicit:
        priority_tier = "explicit_selection"
        reasons.append("explicit_selection")
        if selected_native:
            reasons.append("explicit_native_id")
        if selected_source:
            reasons.append("explicit_source_id")
        if selected_recording:
            reasons.append("explicit_recording_id")
    elif candidate["wiki_reference_count"]:
        priority_tier = "wiki_cited"
        reasons.append("wiki_cited")
    elif "current_platform_listing" in candidate["mapping_roles"]:
        priority_tier = "current_public_upload"
        reasons.append("current_platform_listing")
    elif candidate["platform"] == "youtube":
        priority_tier = "public_guest_or_clip"
        reasons.append("public_youtube_guest_or_clip")
    elif duration_ms is not None and duration_ms <= 15 * 60 * 1000:
        priority_tier = "short_archive"
        reasons.append("short_archive_recording")
    elif duration_ms is not None and duration_ms <= limits["long_recording_ms"]:
        priority_tier = "medium_archive"
        reasons.append("medium_archive_recording")
    else:
        priority_tier = "long_recording"
        reasons.append("long_or_unknown_archive_recording")
    if candidate["source_class"] == "original":
        reasons.append("provider_original")
    elif candidate["source_class"] == "derivative":
        reasons.append("provider_derivative")
    if candidate["recording_type"] == "guest_appearance":
        reasons.append("guest_appearance")

    if "catalog_review_state_requires_attention" in candidate["reason_codes"]:
        queue_state = "review_required"
        reasons.append("catalog_review_state_requires_attention")
    elif duration_ms is None:
        queue_state = "requires_metadata"
        reasons.append("duration_unknown")
    elif (
        duration_ms > limits["long_recording_ms"]
        or candidate["estimated_bytes"] > limits["max_job_bytes"]
    ):
        queue_state = "requires_chunking"
        reasons.append("exceeds_single_job_policy")
    else:
        queue_state = "ready"
        reasons.append("bounded_single_job")

    if candidate["priority_tier"] != priority_tier:
        raise MaterializationError(f"{label}.priority_tier is inconsistent")
    if candidate["priority"] != PRIORITIES[priority_tier]:
        raise MaterializationError(f"{label}.priority is inconsistent")
    if candidate["queue_state"] != queue_state:
        raise MaterializationError(f"{label}.queue_state is inconsistent")
    if candidate["reason_codes"] != sorted(set(reasons)):
        raise MaterializationError(f"{label}.reason_codes are inconsistent")
    return candidate


def validate_plan(raw: Any) -> dict[str, Any]:
    keys = {
        "plan_id",
        "schema_version",
        "planned_at",
        "catalog_basis_sha256",
        "catalog_migrations",
        "selection_basis",
        "wiki_scan",
        "limits",
        "safety",
        "summary",
        "candidates",
    }
    plan = exact_object(raw, "queue plan", keys)
    if plan["schema_version"] != SCHEMA_VERSION:
        raise MaterializationError("queue plan.schema_version must equal 1")
    if not isinstance(plan["plan_id"], str) or not PLAN_ID.fullmatch(plan["plan_id"]):
        raise MaterializationError("queue plan.plan_id is invalid")
    validate_utc_timestamp(plan["planned_at"], "queue plan.planned_at")
    required_sha256(plan["catalog_basis_sha256"], "queue plan.catalog_basis_sha256")
    if not isinstance(plan["catalog_migrations"], list) or not plan["catalog_migrations"]:
        raise MaterializationError("queue plan.catalog_migrations must be non-empty")
    migration_versions = []
    for index, raw_migration in enumerate(plan["catalog_migrations"]):
        migration = exact_object(
            raw_migration,
            f"queue plan.catalog_migrations[{index}]",
            {"version", "name", "sha256"},
        )
        migration_versions.append(
            integer(
                migration["version"],
                f"queue plan.catalog_migrations[{index}].version",
                1,
            )
        )
        text(migration["name"], f"queue plan.catalog_migrations[{index}].name", 500)
        required_sha256(
            migration["sha256"], f"queue plan.catalog_migrations[{index}].sha256"
        )
    if migration_versions != sorted(set(migration_versions)):
        raise MaterializationError("queue plan.catalog_migrations must have increasing unique versions")

    selection = exact_object(
        plan["selection_basis"],
        "queue plan.selection_basis",
        {
            "purpose",
            "manifest_sha256",
            "youtube_video_ids",
            "source_ids",
            "recording_ids",
            "requested_identifiers_already_acquired",
            "requested_identifiers_not_eligible",
        },
    )
    text(selection["purpose"], "queue plan.selection_basis.purpose", 500)
    nullable_sha256(
        selection["manifest_sha256"], "queue plan.selection_basis.manifest_sha256"
    )
    for key, pattern in (
        ("youtube_video_ids", YOUTUBE_ID),
        ("source_ids", None),
        ("recording_ids", None),
        ("requested_identifiers_already_acquired", None),
        ("requested_identifiers_not_eligible", None),
    ):
        selection[key] = sorted_unique_strings(
            selection[key], f"queue plan.selection_basis.{key}", pattern=pattern
        )
    if set(selection["requested_identifiers_already_acquired"]) & set(
        selection["requested_identifiers_not_eligible"]
    ):
        raise MaterializationError("queue plan selection outcome sets overlap")
    requested_identifiers = set(selection["youtube_video_ids"]) | set(
        selection["source_ids"]
    ) | set(selection["recording_ids"])
    for key in (
        "requested_identifiers_already_acquired",
        "requested_identifiers_not_eligible",
    ):
        if not set(selection[key]).issubset(requested_identifiers):
            raise MaterializationError(
                f"queue plan.selection_basis.{key} contains an unrequested identifier"
            )

    wiki_scan = exact_object(
        plan["wiki_scan"],
        "queue plan.wiki_scan",
        {"root_count", "youtube_ids_cited", "archive_objects_cited"},
    )
    for key in wiki_scan:
        integer(wiki_scan[key], f"queue plan.wiki_scan.{key}")

    limits = exact_object(
        plan["limits"],
        "queue plan.limits",
        {
            "max_items",
            "plan_budget_bytes",
            "max_job_bytes",
            "long_recording_ms",
            "youtube_bytes_per_second",
            "youtube_fixed_overhead_bytes",
            "selection_only",
        },
    )
    for key in set(limits) - {"selection_only"}:
        integer(limits[key], f"queue plan.limits.{key}", 1)
    if not isinstance(limits["selection_only"], bool):
        raise MaterializationError("queue plan.limits.selection_only must be boolean")
    if limits["selection_only"] and not any(
        selection[key] for key in ("youtube_video_ids", "source_ids", "recording_ids")
    ):
        raise MaterializationError("selection_only requires an explicit selection")

    safety = exact_object(
        plan["safety"],
        "queue plan.safety",
        {
            "access_policy",
            "publication_authority",
            "credentials_allowed",
            "network_access_performed",
            "catalog_mutated",
        },
    )
    expected_safety = {
        "access_policy": "public_only",
        "publication_authority": "none",
        "credentials_allowed": False,
        "network_access_performed": False,
        "catalog_mutated": False,
    }
    if safety != expected_safety:
        raise MaterializationError(
            "queue plan.safety does not match the fail-closed public-only policy"
        )

    if not isinstance(plan["candidates"], list):
        raise MaterializationError("queue plan.candidates must be an array")
    candidates = [
        validate_candidate(raw_candidate, index, limits=limits, selection=selection)
        for index, raw_candidate in enumerate(plan["candidates"])
    ]
    for key in ("recording_id", "source_id"):
        values = [candidate[key] for candidate in candidates]
        if len(values) != len(set(values)):
            raise MaterializationError(f"queue plan contains duplicate candidate {key}s")
    native_keys = [
        (candidate["platform"], candidate["native_id"]) for candidate in candidates
    ]
    if len(native_keys) != len(set(native_keys)):
        raise MaterializationError("queue plan contains duplicate native media candidates")
    expected_order = sorted(
        candidates,
        key=lambda candidate: (
            candidate["priority"],
            candidate["duration_ms"]
            if candidate["duration_ms"] is not None
            else 2**63,
            candidate["recording_id"],
            candidate["source_id"],
        ),
    )
    if candidates != expected_order:
        raise MaterializationError("queue plan.candidates are not in deterministic priority order")

    selected_count = 0
    selected_bytes = 0
    for index, candidate in enumerate(candidates):
        expected_ordinal: int | None = None
        if candidate["queue_state"] != "ready":
            expected_defer = candidate["queue_state"]
        elif limits["selection_only"] and candidate["priority_tier"] != "explicit_selection":
            expected_defer = "outside_explicit_selection"
        elif selected_count >= limits["max_items"]:
            expected_defer = "plan_item_limit"
        elif selected_bytes + candidate["estimated_bytes"] > limits["plan_budget_bytes"]:
            expected_defer = "plan_byte_budget"
        else:
            selected_count += 1
            selected_bytes += candidate["estimated_bytes"]
            expected_ordinal = selected_count
            expected_defer = None
        if (
            candidate["queue_ordinal"] != expected_ordinal
            or candidate["defer_reason"] != expected_defer
        ):
            raise MaterializationError(
                f"queue plan.candidates[{index}] queue selection fields were tampered"
            )

    summary = exact_object(
        plan["summary"],
        "queue plan.summary",
        {
            "supported_unacquired_sources",
            "recording_candidates",
            "selected_count",
            "selected_estimated_bytes",
            "deferred_count",
            "already_acquired_recordings",
            "withheld_access_sources",
            "by_queue_state",
            "by_platform",
            "by_priority_tier",
            "by_defer_reason",
        },
    )
    for key in (
        "supported_unacquired_sources",
        "recording_candidates",
        "selected_count",
        "selected_estimated_bytes",
        "deferred_count",
        "already_acquired_recordings",
        "withheld_access_sources",
    ):
        integer(summary[key], f"queue plan.summary.{key}")
    if summary["supported_unacquired_sources"] < len(candidates):
        raise MaterializationError("queue plan.summary understates supported source count")
    expected_summary = {
        "recording_candidates": len(candidates),
        "selected_count": selected_count,
        "selected_estimated_bytes": selected_bytes,
        "deferred_count": len(candidates) - selected_count,
        "by_queue_state": count_entries([candidate["queue_state"] for candidate in candidates]),
        "by_platform": count_entries([candidate["platform"] for candidate in candidates]),
        "by_priority_tier": count_entries(
            [candidate["priority_tier"] for candidate in candidates]
        ),
        "by_defer_reason": count_entries(
            [
                candidate["defer_reason"]
                for candidate in candidates
                if candidate["defer_reason"] is not None
            ]
        ),
    }
    for key in ("by_queue_state", "by_platform", "by_priority_tier", "by_defer_reason"):
        validate_count_entries(summary[key], f"queue plan.summary.{key}")
    for key, expected in expected_summary.items():
        if summary[key] != expected:
            raise MaterializationError(f"queue plan.summary.{key} is inconsistent")

    core = {key: plan[key] for key in keys - {"plan_id"}}
    core_sha256 = sha256_bytes(canonical_bytes(core))
    expected_plan_id = stable_id("acqplan", core_sha256)
    if plan["plan_id"] != expected_plan_id:
        raise MaterializationError(
            "queue plan.plan_id does not match the canonical plan core digest"
        )
    return plan


def absolute_path(value: Any, label: str, *, must_exist: bool = False) -> Path:
    if not isinstance(value, str) or not value or "://" in value:
        raise MaterializationError(f"{label} must be a non-empty local path")
    path = Path(value)
    if not path.is_absolute():
        raise MaterializationError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=must_exist)
    except (OSError, RuntimeError) as error:
        if must_exist:
            raise MaterializationError(f"{label} does not exist or is unsafe: {path}") from error
        raise MaterializationError(f"{label} cannot be resolved safely: {path}") from error
    return resolved


def inspect_pinned_executable(path: Path, expected_sha256: str) -> dict[str, Any]:
    if not SHA256.fullmatch(expected_sha256):
        raise MaterializationError("--yt-dlp-sha256 must be a lowercase SHA-256")
    if not path.is_file() or not os.access(path, os.X_OK):
        raise MaterializationError("--yt-dlp-executable must be a regular executable file")
    before = path.stat()
    if not stat.S_ISREG(before.st_mode):
        raise MaterializationError("--yt-dlp-executable must be a regular file")
    observed = sha256_file(path)
    after = path.stat()
    identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if identity_before != identity_after:
        raise MaterializationError("yt-dlp executable changed while it was being pinned")
    if observed != expected_sha256:
        raise MaterializationError("--yt-dlp-sha256 does not match the executable")
    if after.st_size <= 0:
        raise MaterializationError("--yt-dlp-executable must not be empty")
    return {
        "executable": str(path),
        "sha256": observed,
        "byte_count": after.st_size,
    }


def build_work_order(
    candidate: dict[str, Any],
    *,
    plan_id: str,
    media_output_root: Path,
    executable_pin: dict[str, Any],
    global_cache_cap_bytes: int,
    free_space_floor_bytes: int,
    max_job_bytes: int,
    format_selector: str,
    http_timeout_seconds: int,
) -> dict[str, Any]:
    ordinal = candidate["queue_ordinal"]
    if ordinal is None or candidate["queue_state"] != "ready":
        raise MaterializationError("internal refusal: only selected ready candidates materialize")
    job_id = f"acq-{plan_id.removeprefix('acqplan_')}-{ordinal:06d}"
    common = {
        "expected_sha256": candidate["expected_sha256"],
        "expected_byte_count": candidate["expected_byte_count"],
    }
    if candidate["adapter"] == "yt_dlp":
        adapter_config = {
            **common,
            "url": candidate["canonical_url"],
            "executable": executable_pin["executable"],
            "expected_executable_sha256": executable_pin["sha256"],
            "format_selector": format_selector,
        }
    else:
        adapter_config = {
            **common,
            "url": candidate["canonical_url"],
            "resume": True,
            "timeout_seconds": http_timeout_seconds,
        }
    raw = {
        "schema_version": 1,
        "job_id": job_id,
        "adapter": candidate["adapter"],
        "source": {
            "platform": candidate["platform"],
            "source_kind": candidate["source_kind"],
            "native_id": candidate["native_id"],
            "canonical_url": candidate["canonical_url"],
            "title": candidate["title"],
            "published_at": None,
            "access_state": "public",
        },
        "adapter_config": adapter_config,
        "output": {"root": str(media_output_root)},
        "limits": {
            "max_job_bytes": max_job_bytes,
            "global_cache_cap_bytes": global_cache_cap_bytes,
            "free_space_floor_bytes": free_space_floor_bytes,
        },
    }
    try:
        return acquire.validate_work_order(raw)
    except acquire.AcquisitionError as error:
        raise MaterializationError(f"generated guarded work order is invalid: {error}") from error


def build_bundle_manifest(
    plan: dict[str, Any],
    *,
    media_output_root: Path,
    executable_pin: dict[str, Any],
    global_cache_cap_bytes: int,
    free_space_floor_bytes: int,
    format_selector: str,
    http_timeout_seconds: int,
) -> tuple[dict[str, Any], list[tuple[str, bytes]]]:
    selected = [
        candidate for candidate in plan["candidates"] if candidate["queue_ordinal"] is not None
    ]
    if any(candidate["queue_state"] != "ready" for candidate in selected):
        raise MaterializationError("non-ready candidate reached materialization")
    orders: list[tuple[str, bytes]] = []
    entries = []
    for candidate in selected:
        order = build_work_order(
            candidate,
            plan_id=plan["plan_id"],
            media_output_root=media_output_root,
            executable_pin=executable_pin,
            global_cache_cap_bytes=global_cache_cap_bytes,
            free_space_floor_bytes=free_space_floor_bytes,
            max_job_bytes=plan["limits"]["max_job_bytes"],
            format_selector=format_selector,
            http_timeout_seconds=http_timeout_seconds,
        )
        order_body = pretty_bytes(order)
        relative_path = f"work-orders/{candidate['queue_ordinal']:06d}.json"
        orders.append((relative_path, order_body))
        entries.append(
            {
                "queue_ordinal": candidate["queue_ordinal"],
                "recording_id": candidate["recording_id"],
                "source_id": candidate["source_id"],
                "job_id": order["job_id"],
                "adapter": order["adapter"],
                "path": relative_path,
                "sha256": sha256_bytes(order_body),
                "byte_count": len(order_body),
            }
        )
    plan_canonical_sha256 = sha256_bytes(canonical_bytes(plan))
    plan_core = {key: value for key, value in plan.items() if key != "plan_id"}
    core = {
        "schema_version": 1,
        "materializer": {
            "name": "himr-queue-materializer",
            "version": IMPLEMENTATION_VERSION,
        },
        "plan": {
            "plan_id": plan["plan_id"],
            "canonical_sha256": plan_canonical_sha256,
            "core_sha256": sha256_bytes(canonical_bytes(plan_core)),
            "planned_at": plan["planned_at"],
        },
        "policy": {
            "media_output_root": str(media_output_root),
            "max_job_bytes": plan["limits"]["max_job_bytes"],
            "global_cache_cap_bytes": global_cache_cap_bytes,
            "free_space_floor_bytes": free_space_floor_bytes,
            "direct_http_resume": True,
            "direct_http_timeout_seconds": http_timeout_seconds,
            "yt_dlp": {
                **executable_pin,
                "format_selector": format_selector,
            },
        },
        "safety": {
            "bundle_class": "private_acquisition_work_orders",
            "access_policy": "public_only",
            "publication_authority": "none",
            "credentials_allowed": False,
            "network_access_performed": False,
            "catalog_mutated": False,
            "selected_ready_candidates_only": True,
        },
        "work_order_count": len(entries),
        "work_orders": entries,
    }
    bundle_id = stable_id("acqbundle", sha256_bytes(canonical_bytes(core)))
    manifest = {
        "bundle_id": bundle_id,
        "bundle_relative_path": f"bundles/{bundle_id}",
        **core,
    }
    return manifest, orders


def require_safe_root(path: Path, label: str) -> None:
    if path == Path("/"):
        raise MaterializationError(f"{label} may not be the filesystem root")
    if path.exists() and (path.is_symlink() or not path.is_dir()):
        raise MaterializationError(f"{label} must be a real directory or a new path")


def ensure_private_directory(path: Path, label: str) -> None:
    if path.exists() or path.is_symlink():
        if path.is_symlink() or not path.is_dir():
            raise MaterializationError(f"{label} is not a real directory")
        return
    path.mkdir(parents=False, mode=0o700)
    if path.is_symlink() or not path.is_dir():
        raise MaterializationError(f"{label} could not be created safely")


@contextmanager
def writer_lock(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock_path = root / LOCK_FILENAME
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as error:
        if error.errno in {errno.ELOOP, errno.EINVAL}:
            raise MaterializationError(f"unsafe materializer lock path: {lock_path}") from error
        raise
    with os.fdopen(descriptor, "a+b") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise MaterializationError("materializer writer lock is not a regular file")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise MaterializationError(
                f"another queue materializer holds the writer lock: {root}"
            ) from error
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def require_read_only_regular(path: Path, expected: bytes, label: str) -> None:
    if path.is_symlink() or not path.is_file():
        raise MaterializationError(f"existing immutable {label} is not a regular file")
    before = path.stat()
    if stat.S_IMODE(before.st_mode) != 0o400:
        raise MaterializationError(
            f"existing immutable private {label} must have mode 0400"
        )
    body = path.read_bytes()
    after = path.stat()
    if (
        (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        or body != expected
    ):
        raise MaterializationError(f"existing immutable {label} failed exact replay")


def validate_existing_bundle(
    bundle_dir: Path, manifest_body: bytes, orders: list[tuple[str, bytes]]
) -> None:
    if bundle_dir.is_symlink() or not bundle_dir.is_dir():
        raise MaterializationError("existing immutable bundle is not a real directory")
    if stat.S_IMODE(bundle_dir.stat().st_mode) != 0o500:
        raise MaterializationError(
            "existing immutable private bundle directory must have mode 0500"
        )
    expected_files = {"manifest.json", *(path for path, _ in orders)}
    expected_dirs = {"work-orders"} if orders else set()
    observed_entries = {
        path.relative_to(bundle_dir).as_posix(): path
        for path in bundle_dir.rglob("*")
    }
    if set(observed_entries) != expected_files | expected_dirs:
        raise MaterializationError("existing immutable bundle has missing or extra entries")
    for relative_path in expected_dirs:
        directory = observed_entries[relative_path]
        if directory.is_symlink() or not directory.is_dir():
            raise MaterializationError(
                f"existing immutable bundle entry {relative_path} is not a real directory"
            )
    require_read_only_regular(bundle_dir / "manifest.json", manifest_body, "manifest")
    for relative_path, body in orders:
        require_read_only_regular(bundle_dir / relative_path, body, relative_path)
    if orders and stat.S_IMODE((bundle_dir / "work-orders").stat().st_mode) != 0o500:
        raise MaterializationError(
            "existing immutable private work-orders directory must have mode 0500"
        )


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def remove_stage(path: Path) -> None:
    if not path.exists():
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


def admit_bundle(
    bundle_root: Path, manifest: dict[str, Any], orders: list[tuple[str, bytes]]
) -> Path:
    require_safe_root(bundle_root, "--bundle-root")
    manifest_body = pretty_bytes(manifest)
    with writer_lock(bundle_root):
        bundles_dir = bundle_root / "bundles"
        ensure_private_directory(bundles_dir, "bundle admission directory")
        final_dir = bundles_dir / manifest["bundle_id"]
        if final_dir.exists() or final_dir.is_symlink():
            validate_existing_bundle(final_dir, manifest_body, orders)
            return final_dir
        staging_root = bundle_root / ".staging"
        ensure_private_directory(staging_root, "bundle staging directory")
        stage = Path(
            tempfile.mkdtemp(
                prefix=f".{manifest['bundle_id']}.", dir=str(staging_root)
            )
        )
        admitted = False
        try:
            work_orders_dir = stage / "work-orders"
            if orders:
                work_orders_dir.mkdir(mode=0o700)
            for relative_path, body in orders:
                path = stage / relative_path
                with path.open("xb") as handle:
                    handle.write(body)
                    handle.flush()
                    os.fsync(handle.fileno())
                path.chmod(0o400)
            manifest_path = stage / "manifest.json"
            with manifest_path.open("xb") as handle:
                handle.write(manifest_body)
                handle.flush()
                os.fsync(handle.fileno())
            manifest_path.chmod(0o400)
            if orders:
                fsync_directory(work_orders_dir)
            fsync_directory(stage)
            os.rename(stage, final_dir)
            admitted = True
            if orders:
                (final_dir / "work-orders").chmod(0o500)
                fsync_directory(final_dir / "work-orders")
            final_dir.chmod(0o500)
            fsync_directory(final_dir)
            fsync_directory(bundles_dir)
        except Exception:
            remove_stage(final_dir if admitted else stage)
            raise
        validate_existing_bundle(final_dir, manifest_body, orders)
        return final_dir


def read_plan(path_text: str) -> Any:
    if path_text == "-":
        body = sys.stdin.buffer.read(MAX_PLAN_BYTES + 1)
        label = "stdin queue plan"
    else:
        path = absolute_path(path_text, "--plan", must_exist=True)
        if not path.is_file():
            raise MaterializationError("--plan must identify a regular file")
        before = path.stat()
        if not stat.S_ISREG(before.st_mode):
            raise MaterializationError("--plan must identify a regular file")
        if before.st_size > MAX_PLAN_BYTES:
            raise MaterializationError(f"--plan exceeds {MAX_PLAN_BYTES} bytes")
        with path.open("rb") as handle:
            body = handle.read(MAX_PLAN_BYTES + 1)
        after = path.stat()
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise MaterializationError("--plan changed while it was being read")
        label = str(path)
    if len(body) > MAX_PLAN_BYTES:
        raise MaterializationError(f"{label} exceeds {MAX_PLAN_BYTES} bytes")
    return load_json_bytes(body, label)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialize a validated queue plan into an immutable private bundle"
    )
    parser.add_argument("--plan", required=True, help="absolute queue-plan path or - for stdin")
    parser.add_argument("--bundle-root", required=True)
    parser.add_argument("--media-output-root", required=True)
    parser.add_argument("--yt-dlp-executable", required=True)
    parser.add_argument("--yt-dlp-sha256", required=True)
    parser.add_argument("--global-cache-cap-bytes", type=int, required=True)
    parser.add_argument("--free-space-floor-bytes", type=int, required=True)
    parser.add_argument("--format-selector", default=DEFAULT_FORMAT_SELECTOR)
    parser.add_argument(
        "--http-timeout-seconds", type=int, default=DEFAULT_HTTP_TIMEOUT_SECONDS
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        plan = validate_plan(read_plan(args.plan))
        bundle_root = absolute_path(args.bundle_root, "--bundle-root")
        media_output_root = absolute_path(args.media_output_root, "--media-output-root")
        try:
            acquire.validate_output_root(media_output_root)
        except acquire.AcquisitionError as error:
            raise MaterializationError(f"invalid --media-output-root: {error}") from error
        executable = absolute_path(
            args.yt_dlp_executable, "--yt-dlp-executable", must_exist=True
        )
        executable_pin = inspect_pinned_executable(executable, args.yt_dlp_sha256)
        global_cache_cap_bytes = integer(
            args.global_cache_cap_bytes, "--global-cache-cap-bytes", 1
        )
        free_space_floor_bytes = integer(
            args.free_space_floor_bytes, "--free-space-floor-bytes"
        )
        if plan["limits"]["max_job_bytes"] > global_cache_cap_bytes:
            raise MaterializationError(
                "queue plan max_job_bytes exceeds --global-cache-cap-bytes"
            )
        if (
            not isinstance(args.format_selector, str)
            or not 1 <= len(args.format_selector) <= 256
            or "\n" in args.format_selector
            or "\r" in args.format_selector
        ):
            raise MaterializationError("--format-selector is invalid")
        http_timeout_seconds = integer(
            args.http_timeout_seconds, "--http-timeout-seconds", 1
        )
        if http_timeout_seconds > 3_600:
            raise MaterializationError("--http-timeout-seconds may not exceed 3600")
        manifest, orders = build_bundle_manifest(
            plan,
            media_output_root=media_output_root,
            executable_pin=executable_pin,
            global_cache_cap_bytes=global_cache_cap_bytes,
            free_space_floor_bytes=free_space_floor_bytes,
            format_selector=args.format_selector,
            http_timeout_seconds=http_timeout_seconds,
        )
        admit_bundle(bundle_root, manifest, orders)
        sys.stdout.buffer.write(pretty_bytes(manifest))
        return 0
    except (MaterializationError, OSError) as error:
        sys.stderr.buffer.write(
            pretty_bytes(
                {
                    "schema_version": SCHEMA_VERSION,
                    "status": "failed",
                    "error": {"type": type(error).__name__, "message": str(error)},
                }
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
