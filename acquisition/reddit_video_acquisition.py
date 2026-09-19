#!/usr/bin/env python3
"""Deterministic, fail-closed planning for public ``v.redd.it`` clips.

This module performs no discovery, metadata retrieval, media download, catalog
mutation, or publication.  It validates an existing sealed Reddit Atom discovery
manifest, admits only explicitly selected stable ``v.redd.it`` identifiers with
public duration metadata, and materializes immutable guarded-acquisition work orders.
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
import sys
import tempfile
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import acquire
import reddit_rss


IMPLEMENTATION_VERSION = "0.1.0"
BUNDLE_IMPLEMENTATION_VERSION = "0.2.0"
VREDDIT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{5,64}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")
SELECTION_KIND = "reddit_video_selection"
PLAN_KIND = "reddit_video_acquisition_plan"
BUNDLE_KIND = "reddit_video_acquisition_bundle"
FORMAT_SELECTOR = "bv*[height<=720]+ba/b[height<=720]/b"
MAX_JSON_BYTES = 64 * 1024 * 1024
MAX_CLIP_DURATION_MS = 60 * 60 * 1000
MAX_CLIP_JOB_BYTES = 3 * 1024**3


class RedditVideoAcquisitionError(RuntimeError):
    """A provenance, policy, integrity, or immutable-output failure."""


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
        raise RedditVideoAcquisitionError(
            f"value cannot be canonically encoded: {error}"
        ) from error


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


def stable_id(prefix: str, value: Any) -> str:
    return f"{prefix}_{sha256_bytes(canonical_bytes(value))[:32]}"


def reject_json_constant(value: str) -> None:
    raise RedditVideoAcquisitionError(
        f"JSON non-finite numeric constant is forbidden: {value}"
    )


def reject_duplicate_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise RedditVideoAcquisitionError(
                f"JSON object contains duplicate key: {key}"
            )
        result[key] = value
    return result


def load_json_path(path: Path, label: str) -> tuple[Any, bytes]:
    if not path.is_absolute():
        raise RedditVideoAcquisitionError(f"{label} path must be absolute")
    try:
        resolved = path.resolve(strict=True)
        if not resolved.is_file() or not stat.S_ISREG(resolved.stat().st_mode):
            raise RedditVideoAcquisitionError(f"{label} must be a regular file")
        size = resolved.stat().st_size
        if size > MAX_JSON_BYTES:
            raise RedditVideoAcquisitionError(
                f"{label} exceeds the {MAX_JSON_BYTES}-byte input limit"
            )
        body = resolved.read_bytes()
    except OSError as error:
        raise RedditVideoAcquisitionError(f"cannot read {label}: {error}") from error
    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=reject_duplicate_pairs,
            parse_constant=reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RedditVideoAcquisitionError(f"invalid UTF-8 JSON in {label}: {error}") from error
    return value, body


def exact_object(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RedditVideoAcquisitionError(f"{label} must be a JSON object")
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    if missing or unknown:
        raise RedditVideoAcquisitionError(
            f"{label} keys differ from the exact contract; "
            f"missing={missing}, unknown={unknown}"
        )
    return value


def text(value: Any, label: str, maximum: int = 2_000) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise RedditVideoAcquisitionError(
            f"{label} must be a non-empty bounded string"
        )
    return value


def integer(value: Any, label: str, minimum: int = 0, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise RedditVideoAcquisitionError(f"{label} must be an integer >= {minimum}")
    if maximum is not None and value > maximum:
        raise RedditVideoAcquisitionError(f"{label} must be <= {maximum}")
    return value


def utc(value: Any, label: str) -> str:
    value = text(value, label, 100)
    if not UTC_RE.fullmatch(value):
        raise RedditVideoAcquisitionError(
            f"{label} must be an RFC 3339 UTC timestamp ending in Z"
        )
    try:
        from datetime import datetime

        datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise RedditVideoAcquisitionError(f"{label} is not a valid timestamp") from error
    return value


def required_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise RedditVideoAcquisitionError(f"{label} must be a lowercase SHA-256")
    return value


def validate_selection(raw: Any) -> dict[str, Any]:
    row = exact_object(
        raw,
        "selection",
        {"schema_version", "manifest_kind", "purpose", "snapshot_id", "reddit_video_ids"},
    )
    if row["schema_version"] != 1 or row["manifest_kind"] != SELECTION_KIND:
        raise RedditVideoAcquisitionError("unsupported Reddit video selection contract")
    purpose = text(row["purpose"], "selection.purpose", 500)
    snapshot_id = text(row["snapshot_id"], "selection.snapshot_id", 100)
    if not re.fullmatch(r"rrs_[0-9a-f]{32}", snapshot_id):
        raise RedditVideoAcquisitionError("selection.snapshot_id is invalid")
    values = row["reddit_video_ids"]
    if not isinstance(values, list) or not 1 <= len(values) <= 100:
        raise RedditVideoAcquisitionError(
            "selection.reddit_video_ids must contain 1..100 identifiers"
        )
    for index, value in enumerate(values):
        if not isinstance(value, str) or not VREDDIT_ID_RE.fullmatch(value):
            raise RedditVideoAcquisitionError(
                f"selection.reddit_video_ids[{index}] is invalid"
            )
    if values != sorted(values) or len(values) != len(set(values)):
        raise RedditVideoAcquisitionError(
            "selection.reddit_video_ids must be sorted and unique"
        )
    return {
        "schema_version": 1,
        "manifest_kind": SELECTION_KIND,
        "purpose": purpose,
        "snapshot_id": snapshot_id,
        "reddit_video_ids": values,
    }


def load_selection(path: Path) -> tuple[dict[str, Any], bytes]:
    raw, body = load_json_path(path, "selection")
    return validate_selection(raw), body


def load_discovery(path: Path) -> tuple[dict[str, Any], bytes]:
    raw, body = load_json_path(path, "Reddit discovery manifest")
    try:
        validated = reddit_rss.validate_discovery_manifest(path, verify_artifacts=True)
    except reddit_rss.RedditRssError as error:
        raise RedditVideoAcquisitionError(
            f"Reddit discovery validation failed: {error}"
        ) from error
    if raw != validated:
        raise RedditVideoAcquisitionError(
            "strict JSON parse differs from Reddit discovery validation"
        )
    return validated, body


def validate_policy(raw: Any) -> dict[str, int]:
    row = exact_object(
        raw,
        "plan.policy",
        {
            "max_items",
            "max_duration_ms",
            "max_job_bytes",
            "plan_budget_bytes",
            "estimated_bytes_per_second",
            "fixed_overhead_bytes",
        },
    )
    result = {
        "max_items": integer(row["max_items"], "plan.policy.max_items", 1, 100),
        "max_duration_ms": integer(
            row["max_duration_ms"],
            "plan.policy.max_duration_ms",
            1,
            MAX_CLIP_DURATION_MS,
        ),
        "max_job_bytes": integer(
            row["max_job_bytes"],
            "plan.policy.max_job_bytes",
            1,
            MAX_CLIP_JOB_BYTES,
        ),
        "plan_budget_bytes": integer(
            row["plan_budget_bytes"], "plan.policy.plan_budget_bytes", 1, 30 * 1024**3
        ),
        "estimated_bytes_per_second": integer(
            row["estimated_bytes_per_second"],
            "plan.policy.estimated_bytes_per_second",
            1,
            10_000_000,
        ),
        "fixed_overhead_bytes": integer(
            row["fixed_overhead_bytes"],
            "plan.policy.fixed_overhead_bytes",
            0,
            1024**3,
        ),
    }
    if result["max_job_bytes"] > result["plan_budget_bytes"]:
        raise RedditVideoAcquisitionError(
            "plan.policy.max_job_bytes may not exceed plan_budget_bytes"
        )
    return result


def discovery_video_index(discovery: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for post in discovery["posts"]:
        for locator in post["media_locators"]:
            if locator["locator_kind"] != "reddit_video":
                continue
            native_id = locator["native_id"]
            if native_id in result:
                raise RedditVideoAcquisitionError(
                    f"v.redd.it ID {native_id} has ambiguous provenance across Atom posts"
                )
            result[native_id] = {"post": post, "locator": locator}
    return result


def estimate_bytes(duration_ms: int, policy: dict[str, int]) -> int:
    variable = math.ceil(
        duration_ms * policy["estimated_bytes_per_second"] / 1_000
    )
    return policy["fixed_overhead_bytes"] + variable


def count_rows(values: list[str]) -> list[dict[str, Any]]:
    return [
        {"key": key, "count": count}
        for key, count in sorted(Counter(values).items())
    ]


def build_plan(
    *,
    discovery: dict[str, Any],
    discovery_bytes: bytes,
    selection: dict[str, Any],
    selection_bytes: bytes,
    planned_at: str,
    policy: dict[str, int],
) -> dict[str, Any]:
    planned_at = utc(planned_at, "planned_at")
    policy = validate_policy(policy)
    if selection["snapshot_id"] != discovery["snapshot"]["snapshot_id"]:
        raise RedditVideoAcquisitionError(
            "selection.snapshot_id does not match the sealed discovery snapshot"
        )
    available = discovery_video_index(discovery)
    missing = sorted(set(selection["reddit_video_ids"]) - set(available))
    if missing:
        raise RedditVideoAcquisitionError(
            f"selection contains v.redd.it IDs absent from discovery: {missing}"
        )

    candidates: list[dict[str, Any]] = []
    for native_id in selection["reddit_video_ids"]:
        item = available[native_id]
        post = item["post"]
        locator = item["locator"]
        observation = locator.get("metadata_observation")
        duration_ms: int | None = None
        estimated: int | None = None
        access_basis = "missing_metadata"
        state = "requires_metadata"
        defer_reason: str | None = "missing_public_duration_metadata"
        if observation is not None:
            duration_ms = observation["duration_ms"]
            access_basis = (
                "yt_dlp_reported_public"
                if observation["access_state"] == "public"
                else "public_atom_locator_no_auth_runtime_gate"
            )
            if duration_ms <= 0:
                state = "requires_metadata"
                defer_reason = "nonpositive_duration_metadata"
            else:
                estimated = estimate_bytes(duration_ms, policy)
                if duration_ms > policy["max_duration_ms"]:
                    state = "exceeds_duration_cap"
                    defer_reason = "duration_exceeds_clip_cap"
                elif estimated > policy["max_job_bytes"]:
                    state = "exceeds_job_cap"
                    defer_reason = "estimate_exceeds_job_cap"
                else:
                    state = "ready"
                    defer_reason = None
        candidates.append(
            {
                "native_id": native_id,
                "canonical_url": f"https://v.redd.it/{native_id}",
                "post_id": post["post_id"],
                "post_permalink": post["permalink"],
                "post_title": post["title"],
                "post_published_at": post["published_at"],
                "title_assertion_state": "unreviewed",
                "locator_basis": locator["basis"],
                "metadata_observation": observation,
                "access_basis": access_basis,
                "duration_ms": duration_ms,
                "estimated_bytes": estimated,
                "estimate_basis": (
                    "public_yt_dlp_duration_conservative_rate"
                    if estimated is not None
                    else "unavailable"
                ),
                "queue_state": state,
                "queue_ordinal": None,
                "defer_reason": defer_reason,
            }
        )

    used_bytes = 0
    ordinal = 0
    for candidate in candidates:
        if candidate["queue_state"] != "ready":
            continue
        estimate = candidate["estimated_bytes"]
        assert isinstance(estimate, int)
        if ordinal >= policy["max_items"]:
            candidate["queue_state"] = "deferred_item_limit"
            candidate["defer_reason"] = "plan_item_limit"
        elif used_bytes + estimate > policy["plan_budget_bytes"]:
            candidate["queue_state"] = "deferred_byte_budget"
            candidate["defer_reason"] = "plan_byte_budget"
        else:
            ordinal += 1
            used_bytes += estimate
            candidate["queue_ordinal"] = ordinal

    source = {
        "discovery_id": discovery["discovery_id"],
        "discovery_sha256": sha256_bytes(discovery_bytes),
        "snapshot_id": discovery["snapshot"]["snapshot_id"],
        "snapshot_payload_sha256": discovery["snapshot"]["payload_sha256"],
        "subreddit": discovery["subreddit"],
    }
    selection_record = {
        "selection_sha256": sha256_bytes(selection_bytes),
        "purpose": selection["purpose"],
        "snapshot_id": selection["snapshot_id"],
        "reddit_video_ids": selection["reddit_video_ids"],
    }
    body = {
        "schema_version": 1,
        "manifest_kind": PLAN_KIND,
        "implementation_version": IMPLEMENTATION_VERSION,
        "planned_at": planned_at,
        "source_discovery": source,
        "selection": selection_record,
        "policy": policy,
        "candidates": candidates,
        "summary": {
            "discovered_video_count": len(available),
            "selected_video_count": len(candidates),
            "queued_count": ordinal,
            "queued_estimated_bytes": used_bytes,
            "queue_state_counts": count_rows(
                [candidate["queue_state"] for candidate in candidates]
            ),
        },
        "assertion_policy": {
            "atom_titles_are_content_truth": False,
            "comments_collected": False,
            "authors_collected": False,
            "credentials_used": False,
            "publication_authority": False,
        },
    }
    return {"plan_id": stable_id("rvaplan", body), **body}


def validate_plan_shape(raw: Any) -> dict[str, Any]:
    plan = exact_object(
        raw,
        "plan",
        {
            "plan_id",
            "schema_version",
            "manifest_kind",
            "implementation_version",
            "planned_at",
            "source_discovery",
            "selection",
            "policy",
            "candidates",
            "summary",
            "assertion_policy",
        },
    )
    if plan["schema_version"] != 1 or plan["manifest_kind"] != PLAN_KIND:
        raise RedditVideoAcquisitionError("unsupported Reddit video plan contract")
    if plan["implementation_version"] != IMPLEMENTATION_VERSION:
        raise RedditVideoAcquisitionError("unsupported Reddit video planner version")
    if not isinstance(plan["plan_id"], str) or not re.fullmatch(
        r"rvaplan_[0-9a-f]{32}", plan["plan_id"]
    ):
        raise RedditVideoAcquisitionError("plan.plan_id is invalid")
    utc(plan["planned_at"], "plan.planned_at")
    validate_policy(plan["policy"])
    body = {key: value for key, value in plan.items() if key != "plan_id"}
    if plan["plan_id"] != stable_id("rvaplan", body):
        raise RedditVideoAcquisitionError("plan_id does not match canonical plan content")
    return plan


def reproduce_plan(
    *, plan_path: Path, discovery_path: Path, selection_path: Path
) -> tuple[dict[str, Any], bytes, dict[str, Any], dict[str, Any]]:
    raw_plan, plan_bytes = load_json_path(plan_path, "plan")
    plan = validate_plan_shape(raw_plan)
    discovery, discovery_bytes = load_discovery(discovery_path)
    selection, selection_bytes = load_selection(selection_path)
    expected = build_plan(
        discovery=discovery,
        discovery_bytes=discovery_bytes,
        selection=selection,
        selection_bytes=selection_bytes,
        planned_at=plan["planned_at"],
        policy=plan["policy"],
    )
    if plan != expected:
        raise RedditVideoAcquisitionError(
            "plan does not reproduce from the sealed discovery, selection, and policy"
        )
    return plan, plan_bytes, discovery, selection


def stable_executable(path: Path, expected_sha256: str) -> tuple[Path, dict[str, int]]:
    if not path.is_absolute():
        raise RedditVideoAcquisitionError("yt-dlp executable path must be absolute")
    expected_sha256 = required_sha256(expected_sha256, "yt-dlp SHA-256")
    try:
        resolved = path.resolve(strict=True)
        before = resolved.stat()
    except OSError as error:
        raise RedditVideoAcquisitionError(f"cannot inspect yt-dlp executable: {error}") from error
    if not stat.S_ISREG(before.st_mode) or not os.access(resolved, os.X_OK):
        raise RedditVideoAcquisitionError("yt-dlp path must be an executable regular file")
    observed = sha256_file(resolved)
    after = resolved.stat()
    before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if before_identity != after_identity or sha256_file(resolved) != observed:
        raise RedditVideoAcquisitionError("yt-dlp executable changed while being hashed")
    if observed != expected_sha256:
        raise RedditVideoAcquisitionError("yt-dlp executable does not match its SHA-256 pin")
    return resolved, {
        "device": before.st_dev,
        "inode": before.st_ino,
        "byte_count": before.st_size,
        "mtime_ns": before.st_mtime_ns,
    }


def stable_ytdlp_runtime(
    executable: Path,
    expected_version: str,
    module_root: Path | None,
) -> tuple[str, dict[str, Any] | None]:
    try:
        expected_version = acquire.validated_ytdlp_version(
            expected_version, "yt-dlp expected version"
        )
        is_script = acquire.executable_is_script(executable)
    except acquire.AcquisitionError as error:
        raise RedditVideoAcquisitionError(str(error)) from error
    if is_script and module_root is None:
        raise RedditVideoAcquisitionError(
            "a script-based yt-dlp launcher requires --yt-dlp-module-root so the "
            "imported package bytes are hash-pinned"
        )

    try:
        before = (
            acquire.runtime_tree_fingerprint(module_root)
            if module_root is not None
            else None
        )
        with tempfile.TemporaryDirectory(prefix="himr-ytdlp-version-") as temporary:
            environment = acquire.minimal_subprocess_environment(Path(temporary))
            observed_version = acquire.ytdlp_version(executable, environment)
        after = (
            acquire.runtime_tree_fingerprint(module_root)
            if module_root is not None
            else None
        )
    except (acquire.AcquisitionError, OSError) as error:
        raise RedditVideoAcquisitionError(
            f"cannot verify yt-dlp runtime identity: {error}"
        ) from error
    if observed_version != expected_version:
        raise RedditVideoAcquisitionError(
            "yt-dlp version does not match --yt-dlp-version: "
            f"expected {expected_version}, observed {observed_version}"
        )
    if before != after:
        raise RedditVideoAcquisitionError(
            "yt-dlp runtime module tree changed during version verification"
        )
    return observed_version, after


def absolute_output_root(value: Path, label: str) -> Path:
    if not value.is_absolute():
        raise RedditVideoAcquisitionError(f"{label} must be absolute")
    resolved = value.resolve(strict=False)
    try:
        acquire.validate_output_root(resolved)
    except acquire.AcquisitionError as error:
        raise RedditVideoAcquisitionError(f"invalid {label}: {error}") from error
    return resolved


@contextmanager
def materializer_lock(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / ".reddit-video-materializer.lock"
    descriptor = os.open(
        lock_path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    handle = os.fdopen(descriptor, "r+", encoding="utf-8")
    try:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise RedditVideoAcquisitionError("materializer lock is not a regular file")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RedditVideoAcquisitionError(
                "another Reddit video materializer holds the bundle-root lock"
            ) from error
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def build_bundle_payloads(
    *,
    plan: dict[str, Any],
    plan_bytes: bytes,
    discovery: dict[str, Any],
    executable: Path,
    executable_sha256: str,
    executable_stat: dict[str, int],
    yt_dlp_version: str,
    runtime_tree: dict[str, Any] | None,
    media_output_root: Path,
    global_cache_cap_bytes: int,
    free_space_floor_bytes: int,
) -> tuple[dict[str, Any], list[tuple[str, bytes]]]:
    max_job_bytes = plan["policy"]["max_job_bytes"]
    global_cache_cap_bytes = integer(
        global_cache_cap_bytes, "global_cache_cap_bytes", max_job_bytes
    )
    free_space_floor_bytes = integer(
        free_space_floor_bytes, "free_space_floor_bytes", 0
    )
    queued = sorted(
        (
            candidate
            for candidate in plan["candidates"]
            if candidate["queue_ordinal"] is not None
        ),
        key=lambda value: value["queue_ordinal"],
    )
    if [item["queue_ordinal"] for item in queued] != list(range(1, len(queued) + 1)):
        raise RedditVideoAcquisitionError("queued ordinals are not contiguous")

    order_payloads: list[tuple[str, bytes]] = []
    entries: list[dict[str, Any]] = []
    for candidate in queued:
        if candidate["queue_state"] != "ready":
            raise RedditVideoAcquisitionError("only ready candidates may be materialized")
        ordinal = candidate["queue_ordinal"]
        job_id = f"reddit-clip-{plan['plan_id'][-12:]}-{ordinal:03d}-{candidate['native_id']}"
        work_order = {
            "schema_version": 1,
            "job_id": job_id,
            "adapter": "yt_dlp",
            "source": {
                "platform": "reddit",
                "source_kind": "reddit_video",
                "native_id": candidate["native_id"],
                "canonical_url": candidate["canonical_url"],
                "title": candidate["post_title"],
                "published_at": candidate["post_published_at"],
                "access_state": "public",
            },
            "adapter_config": {
                "url": candidate["canonical_url"],
                "executable": str(executable),
                "expected_executable_sha256": executable_sha256,
                "expected_ytdlp_version": yt_dlp_version,
                "expected_webpage_url": candidate["post_permalink"],
                "format_selector": FORMAT_SELECTOR,
                "expected_sha256": None,
                "expected_byte_count": None,
            },
            "output": {"root": str(media_output_root)},
            "limits": {
                "max_job_bytes": max_job_bytes,
                "global_cache_cap_bytes": global_cache_cap_bytes,
                "free_space_floor_bytes": free_space_floor_bytes,
            },
        }
        if runtime_tree is not None:
            work_order["adapter_config"]["expected_runtime_tree_root"] = runtime_tree[
                "root"
            ]
            work_order["adapter_config"]["expected_runtime_tree_sha256"] = runtime_tree[
                "sha256"
            ]
        try:
            normalized = acquire.validate_work_order(work_order)
        except acquire.AcquisitionError as error:
            raise RedditVideoAcquisitionError(
                f"generated work order {ordinal} failed guarded validation: {error}"
            ) from error
        if normalized != work_order:
            raise RedditVideoAcquisitionError(
                f"generated work order {ordinal} changed during guarded validation"
            )
        filename = f"work-orders/{ordinal:06d}.json"
        payload = pretty_bytes(work_order)
        order_payloads.append((filename, payload))
        observation = candidate["metadata_observation"]
        entries.append(
            {
                "queue_ordinal": ordinal,
                "job_id": job_id,
                "native_id": candidate["native_id"],
                "canonical_url": candidate["canonical_url"],
                "work_order_file": filename,
                "work_order_sha256": sha256_bytes(payload),
                "work_order_byte_count": len(payload),
                "provenance": {
                    "discovery_id": discovery["discovery_id"],
                    "snapshot_id": discovery["snapshot"]["snapshot_id"],
                    "snapshot_payload_sha256": discovery["snapshot"]["payload_sha256"],
                    "subreddit": discovery["subreddit"],
                    "post_id": candidate["post_id"],
                    "post_permalink": candidate["post_permalink"],
                    "locator_basis": candidate["locator_basis"],
                    "metadata_payload_sha256": observation["payload_sha256"],
                    "metadata_observed_at": observation["observed_at"],
                    "title_assertion_state": "unreviewed",
                },
                "publication_authority": False,
            }
        )

    body = {
        "schema_version": 1,
        "manifest_kind": BUNDLE_KIND,
        "implementation_version": BUNDLE_IMPLEMENTATION_VERSION,
        "plan": {
            "plan_id": plan["plan_id"],
            "plan_sha256": sha256_bytes(plan_bytes),
        },
        "source_discovery": plan["source_discovery"],
        "storage_policy": {
            "media_output_root": str(media_output_root),
            "max_job_bytes": max_job_bytes,
            "global_cache_cap_bytes": global_cache_cap_bytes,
            "free_space_floor_bytes": free_space_floor_bytes,
        },
        "yt_dlp": {
            "executable": str(executable),
            "expected_executable_sha256": executable_sha256,
            "expected_version": yt_dlp_version,
            "executable_stat": executable_stat,
            "runtime_tree": runtime_tree,
            "format_selector": FORMAT_SELECTOR,
            "ignore_ambient_config": True,
            "credentials_allowed": False,
            "playlists_allowed": False,
            "comments_allowed": False,
        },
        "work_orders": entries,
        "assertion_policy": {
            "atom_titles_are_content_truth": False,
            "metadata_is_content_truth": False,
            "publication_authority": False,
        },
    }
    manifest = {"bundle_id": stable_id("rvabundle", body), **body}
    return manifest, order_payloads


def verify_existing_bundle(
    target: Path, manifest: dict[str, Any], payloads: list[tuple[str, bytes]]
) -> Path:
    if target.is_symlink() or not target.is_dir():
        raise RedditVideoAcquisitionError(
            "existing immutable Reddit video bundle target is not a real directory"
        )
    if any(path.is_symlink() for path in target.rglob("*")):
        raise RedditVideoAcquisitionError(
            "existing immutable Reddit video bundle contains a symbolic link"
        )
    expected = {"manifest.json": pretty_bytes(manifest), **dict(payloads)}
    observed_files = {
        path.relative_to(target).as_posix()
        for path in target.rglob("*")
        if path.is_file()
    }
    if observed_files != set(expected):
        raise RedditVideoAcquisitionError(
            "existing immutable Reddit video bundle has a different file set"
        )
    for relative, payload in expected.items():
        if (target / relative).read_bytes() != payload:
            raise RedditVideoAcquisitionError(
                f"existing immutable Reddit video bundle differs at {relative}"
            )
    return target / "manifest.json"


def materialize_bundle(
    *,
    plan_path: Path,
    discovery_path: Path,
    selection_path: Path,
    bundle_root: Path,
    media_output_root: Path,
    yt_dlp_executable: Path,
    yt_dlp_sha256: str,
    yt_dlp_version: str,
    yt_dlp_module_root: Path | None,
    global_cache_cap_bytes: int,
    free_space_floor_bytes: int,
) -> Path:
    plan, plan_bytes, discovery, _selection = reproduce_plan(
        plan_path=plan_path,
        discovery_path=discovery_path,
        selection_path=selection_path,
    )
    if not bundle_root.is_absolute() or bundle_root.resolve(strict=False) == Path("/"):
        raise RedditVideoAcquisitionError("bundle_root must be an absolute non-root path")
    bundle_root = bundle_root.resolve(strict=False)
    media_output_root = absolute_output_root(media_output_root, "media_output_root")
    executable, executable_stat = stable_executable(
        yt_dlp_executable, yt_dlp_sha256
    )
    observed_version, runtime_tree = stable_ytdlp_runtime(
        executable, yt_dlp_version, yt_dlp_module_root
    )
    executable_after, executable_stat_after = stable_executable(
        yt_dlp_executable, yt_dlp_sha256
    )
    if executable_after != executable or executable_stat_after != executable_stat:
        raise RedditVideoAcquisitionError(
            "yt-dlp executable changed during runtime identity verification"
        )
    manifest, payloads = build_bundle_payloads(
        plan=plan,
        plan_bytes=plan_bytes,
        discovery=discovery,
        executable=executable,
        executable_sha256=yt_dlp_sha256,
        executable_stat=executable_stat,
        yt_dlp_version=observed_version,
        runtime_tree=runtime_tree,
        media_output_root=media_output_root,
        global_cache_cap_bytes=global_cache_cap_bytes,
        free_space_floor_bytes=free_space_floor_bytes,
    )
    target = bundle_root / "bundles" / manifest["bundle_id"]
    with materializer_lock(bundle_root):
        if target.exists():
            return verify_existing_bundle(target, manifest, payloads)
        staging = bundle_root / f".reddit-video-stage-{os.getpid()}-{manifest['bundle_id']}"
        if staging.exists():
            raise RedditVideoAcquisitionError(f"staging path already exists: {staging}")
        try:
            (staging / "work-orders").mkdir(parents=True, mode=0o700)
            for relative, payload in payloads:
                path = staging / relative
                path.write_bytes(payload)
                path.chmod(0o600)
            manifest_path = staging / "manifest.json"
            manifest_path.write_bytes(pretty_bytes(manifest))
            manifest_path.chmod(0o600)
            target.parent.mkdir(parents=True, exist_ok=True)
            staging.rename(target)
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise
    return target / "manifest.json"


def parse_policy(args: argparse.Namespace) -> dict[str, int]:
    return validate_policy(
        {
            "max_items": args.max_items,
            "max_duration_ms": args.max_duration_ms,
            "max_job_bytes": args.max_job_bytes,
            "plan_budget_bytes": args.plan_budget_bytes,
            "estimated_bytes_per_second": args.estimated_bytes_per_second,
            "fixed_overhead_bytes": args.fixed_overhead_bytes,
        }
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="reddit-video-acquisition",
        description=(
            "Plan and materialize bounded public v.redd.it clips from sealed Atom discovery"
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)

    create_selection_parser = commands.add_parser("create-selection")
    create_selection_parser.add_argument("--purpose", required=True)
    create_selection_parser.add_argument("--snapshot-id", required=True)
    create_selection_parser.add_argument(
        "--reddit-video-id", action="append", default=[], required=True
    )

    validate_selection_parser = commands.add_parser("validate-selection")
    validate_selection_parser.add_argument("--selection", type=Path, required=True)

    plan_parser = commands.add_parser("plan")
    plan_parser.add_argument("--discovery", type=Path, required=True)
    plan_parser.add_argument("--selection", type=Path, required=True)
    plan_parser.add_argument("--planned-at", required=True)
    plan_parser.add_argument("--max-items", type=int, default=5)
    plan_parser.add_argument("--max-duration-ms", type=int, default=15 * 60 * 1000)
    plan_parser.add_argument("--max-job-bytes", type=int, default=512 * 1024**2)
    plan_parser.add_argument("--plan-budget-bytes", type=int, default=2 * 1024**3)
    plan_parser.add_argument("--estimated-bytes-per-second", type=int, default=500_000)
    plan_parser.add_argument("--fixed-overhead-bytes", type=int, default=64 * 1024**2)

    validate_plan_parser = commands.add_parser("validate-plan")
    validate_plan_parser.add_argument("--plan", type=Path, required=True)
    validate_plan_parser.add_argument("--discovery", type=Path, required=True)
    validate_plan_parser.add_argument("--selection", type=Path, required=True)

    materialize = commands.add_parser("materialize")
    materialize.add_argument("--plan", type=Path, required=True)
    materialize.add_argument("--discovery", type=Path, required=True)
    materialize.add_argument("--selection", type=Path, required=True)
    materialize.add_argument("--bundle-root", type=Path, required=True)
    materialize.add_argument("--media-output-root", type=Path, required=True)
    materialize.add_argument("--yt-dlp-executable", type=Path, required=True)
    materialize.add_argument("--yt-dlp-sha256", required=True)
    materialize.add_argument("--yt-dlp-version", required=True)
    materialize.add_argument("--yt-dlp-module-root", type=Path)
    materialize.add_argument(
        "--global-cache-cap-bytes", type=int, default=30 * 1024**3
    )
    materialize.add_argument(
        "--free-space-floor-bytes", type=int, default=100 * 1024**3
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "create-selection":
            selection = validate_selection(
                {
                    "schema_version": 1,
                    "manifest_kind": SELECTION_KIND,
                    "purpose": args.purpose,
                    "snapshot_id": args.snapshot_id,
                    "reddit_video_ids": sorted(args.reddit_video_id),
                }
            )
            sys.stdout.buffer.write(pretty_bytes(selection))
        elif args.command == "validate-selection":
            selection, _body = load_selection(args.selection)
            sys.stdout.buffer.write(pretty_bytes(selection))
        elif args.command == "plan":
            discovery, discovery_bytes = load_discovery(args.discovery)
            selection, selection_bytes = load_selection(args.selection)
            result = build_plan(
                discovery=discovery,
                discovery_bytes=discovery_bytes,
                selection=selection,
                selection_bytes=selection_bytes,
                planned_at=args.planned_at,
                policy=parse_policy(args),
            )
            sys.stdout.buffer.write(pretty_bytes(result))
        elif args.command == "validate-plan":
            plan, _body, _discovery, _selection = reproduce_plan(
                plan_path=args.plan,
                discovery_path=args.discovery,
                selection_path=args.selection,
            )
            sys.stdout.buffer.write(pretty_bytes(plan))
        else:
            manifest_path = materialize_bundle(
                plan_path=args.plan,
                discovery_path=args.discovery,
                selection_path=args.selection,
                bundle_root=args.bundle_root,
                media_output_root=args.media_output_root,
                yt_dlp_executable=args.yt_dlp_executable,
                yt_dlp_sha256=args.yt_dlp_sha256,
                yt_dlp_version=args.yt_dlp_version,
                yt_dlp_module_root=args.yt_dlp_module_root,
                global_cache_cap_bytes=args.global_cache_cap_bytes,
                free_space_floor_bytes=args.free_space_floor_bytes,
            )
            manifest, _body = load_json_path(manifest_path, "bundle manifest")
            sys.stdout.buffer.write(pretty_bytes(manifest))
        return 0
    except (RedditVideoAcquisitionError, OSError) as error:
        print(f"reddit-video-acquisition: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
