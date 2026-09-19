#!/usr/bin/env python3
"""Partition one sealed Archive queue plan into immutable acquisition epochs.

This is an offline control-plane boundary.  It validates and hash-pins one parent
queue plan, derives compact queue-plan-v1 epochs, and delegates work-order creation
and immutable bundle admission to :mod:`materialize_queue`.  It never downloads,
probes, processes, copies, deletes, or publishes media.
"""

from __future__ import annotations

import argparse
import copy
import errno
import fcntl
import json
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

if __package__:
    from . import acquire

    # The established materializer is also a direct CLI and imports ``acquire`` by
    # its historical top-level name.  Publish that already-loaded local module under
    # the expected name so this new boundary remains importable for offline tests and
    # controllers without modifying the established implementation.
    sys.modules.setdefault("acquire", acquire)
    from . import materialize_queue
else:  # pragma: no cover - direct CLI execution
    import acquire  # type: ignore[no-redef]
    import materialize_queue  # type: ignore[no-redef]


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
DEFAULT_MAX_EPOCH_ITEMS = 32
DEFAULT_MAX_EPOCH_ESTIMATED_BYTES = 16 * 1024**3
MAX_EPOCH_ITEMS = 1_024
MAX_EPOCH_ESTIMATED_BYTES = 1024**4
LOCK_FILENAME = ".campaign-epoch-materializer.lock"
SHA256 = re.compile(r"^[0-9a-f]{64}$")


class CampaignMaterializationError(RuntimeError):
    """A sealed-input, partition, coverage, or immutable-admission failure."""


def _integer(value: Any, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise CampaignMaterializationError(f"{label} must be an integer >= {minimum}")
    return value


def _required_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256.fullmatch(value):
        raise CampaignMaterializationError(f"{label} must be a lowercase SHA-256")
    return value


def _canonical_absolute_path(value: str, label: str, *, must_exist: bool = False) -> Path:
    if not isinstance(value, str) or not value or "://" in value:
        raise CampaignMaterializationError(f"{label} must be a non-empty local path")
    supplied = Path(value)
    if not supplied.is_absolute():
        raise CampaignMaterializationError(f"{label} must be absolute")
    try:
        resolved = supplied.resolve(strict=must_exist)
    except (OSError, RuntimeError) as error:
        raise CampaignMaterializationError(f"{label} cannot be resolved safely") from error
    if supplied != resolved:
        raise CampaignMaterializationError(
            f"{label} must be an already-normalized absolute path without symlinks"
        )
    return resolved


def read_sealed_parent_plan(path_text: str, expected_sha256: str) -> tuple[dict[str, Any], bytes, Path]:
    """Read one immutable, canonical queue plan through a stable regular file."""

    expected_sha256 = _required_sha256(expected_sha256, "--expected-parent-plan-sha256")
    path = _canonical_absolute_path(path_text, "--parent-plan", must_exist=True)
    try:
        before_link = path.lstat()
    except OSError as error:
        raise CampaignMaterializationError("cannot inspect --parent-plan") from error
    if stat.S_ISLNK(before_link.st_mode) or not stat.S_ISREG(before_link.st_mode):
        raise CampaignMaterializationError("--parent-plan must be a non-symlink regular file")
    if stat.S_IMODE(before_link.st_mode) != 0o400:
        raise CampaignMaterializationError("sealed --parent-plan must have mode 0400")
    if before_link.st_size > materialize_queue.MAX_PLAN_BYTES:
        raise CampaignMaterializationError("--parent-plan exceeds the queue-plan size bound")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        if error.errno in {errno.ELOOP, errno.EINVAL}:
            raise CampaignMaterializationError("unsafe --parent-plan path") from error
        raise
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or stat.S_IMODE(before.st_mode) != 0o400:
            raise CampaignMaterializationError("sealed --parent-plan identity or mode changed")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            body = handle.read(materialize_queue.MAX_PLAN_BYTES + 1)
        after = os.fstat(descriptor)
        after_link = path.lstat()
    finally:
        os.close(descriptor)
    if len(body) > materialize_queue.MAX_PLAN_BYTES:
        raise CampaignMaterializationError("--parent-plan exceeds the queue-plan size bound")
    identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    path_before = (
        before_link.st_dev,
        before_link.st_ino,
        before_link.st_size,
        before_link.st_mtime_ns,
    )
    path_after = (
        after_link.st_dev,
        after_link.st_ino,
        after_link.st_size,
        after_link.st_mtime_ns,
    )
    if (
        identity_before != identity_after
        or path_before != identity_before
        or path_after != identity_after
        or before.st_size != len(body)
    ):
        raise CampaignMaterializationError("--parent-plan changed while it was read")
    observed_sha256 = materialize_queue.sha256_bytes(body)
    if observed_sha256 != expected_sha256:
        raise CampaignMaterializationError(
            "--expected-parent-plan-sha256 does not match --parent-plan"
        )
    try:
        raw = materialize_queue.load_json_bytes(body, str(path))
        plan = materialize_queue.validate_plan(raw)
    except materialize_queue.MaterializationError as error:
        raise CampaignMaterializationError(f"invalid parent queue plan: {error}") from error
    if body != materialize_queue.pretty_bytes(plan):
        raise CampaignMaterializationError(
            "sealed --parent-plan must use the canonical pretty JSON representation"
        )
    return plan, body, path


def selected_parent_candidates(plan: dict[str, Any]) -> list[dict[str, Any]]:
    selected = [candidate for candidate in plan["candidates"] if candidate["queue_ordinal"] is not None]
    if not selected:
        raise CampaignMaterializationError("parent queue plan selects no candidates")
    selected.sort(key=lambda candidate: candidate["queue_ordinal"])
    ordinals = [candidate["queue_ordinal"] for candidate in selected]
    if ordinals != list(range(1, len(selected) + 1)):
        raise CampaignMaterializationError("parent selected queue ordinals are not contiguous")
    if any(candidate["queue_state"] != "ready" for candidate in selected):
        raise CampaignMaterializationError("parent selected set contains a non-ready candidate")
    non_archive = [
        candidate["recording_id"]
        for candidate in selected
        if candidate["platform"] != "internet_archive"
    ]
    if non_archive:
        raise CampaignMaterializationError(
            "Archive campaign parent selected set contains non-Archive candidates: "
            + ", ".join(non_archive[:5])
        )
    return selected


def partition_candidates(
    selected: list[dict[str, Any]],
    *,
    max_epoch_items: int,
    max_epoch_estimated_bytes: int,
) -> list[list[dict[str, Any]]]:
    """Stable greedy partition in parent queue order, without splitting a candidate."""

    max_epoch_items = _integer(max_epoch_items, "max_epoch_items", 1)
    max_epoch_estimated_bytes = _integer(
        max_epoch_estimated_bytes, "max_epoch_estimated_bytes", 1
    )
    if max_epoch_items > MAX_EPOCH_ITEMS:
        raise CampaignMaterializationError(
            f"max_epoch_items may not exceed {MAX_EPOCH_ITEMS}"
        )
    if max_epoch_estimated_bytes > MAX_EPOCH_ESTIMATED_BYTES:
        raise CampaignMaterializationError(
            f"max_epoch_estimated_bytes may not exceed {MAX_EPOCH_ESTIMATED_BYTES}"
        )
    epochs: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_bytes = 0
    for candidate in selected:
        estimated_bytes = _integer(candidate["estimated_bytes"], "candidate.estimated_bytes", 1)
        if estimated_bytes > max_epoch_estimated_bytes:
            raise CampaignMaterializationError(
                f"parent queue ordinal {candidate['queue_ordinal']} exceeds the epoch byte cap"
            )
        if current and (
            len(current) >= max_epoch_items
            or current_bytes + estimated_bytes > max_epoch_estimated_bytes
        ):
            epochs.append(current)
            current = []
            current_bytes = 0
        current.append(candidate)
        current_bytes += estimated_bytes
    if current:
        epochs.append(current)
    return epochs


def _count_entries(values: list[str]) -> list[dict[str, Any]]:
    return [
        {"key": key, "count": count}
        for key, count in sorted(Counter(values).items())
    ]


def _compact_selection_basis(
    candidates: list[dict[str, Any]], parent_plan_id: str, epoch_ordinal: int
) -> tuple[dict[str, Any], bool]:
    youtube_ids: set[str] = set()
    source_ids: set[str] = set()
    recording_ids: set[str] = set()
    all_explicit = True
    for candidate in candidates:
        reasons = set(candidate["reason_codes"])
        if candidate["priority_tier"] != "explicit_selection":
            all_explicit = False
            continue
        if "explicit_native_id" in reasons:
            youtube_ids.add(candidate["native_id"])
        if "explicit_source_id" in reasons:
            source_ids.add(candidate["source_id"])
        if "explicit_recording_id" in reasons:
            recording_ids.add(candidate["recording_id"])
        if not reasons.intersection(
            {"explicit_native_id", "explicit_source_id", "explicit_recording_id"}
        ):
            raise CampaignMaterializationError(
                "explicit parent candidate has no reconstructable explicit selector"
            )
    return (
        {
            "purpose": f"Compact epoch {epoch_ordinal} derived from {parent_plan_id}",
            "manifest_sha256": None,
            "youtube_video_ids": sorted(youtube_ids),
            "source_ids": sorted(source_ids),
            "recording_ids": sorted(recording_ids),
            "requested_identifiers_already_acquired": [],
            "requested_identifiers_not_eligible": [],
        },
        all_explicit,
    )


def derive_epoch_plan(
    parent_plan: dict[str, Any],
    candidates: list[dict[str, Any]],
    *,
    epoch_ordinal: int,
    max_epoch_items: int,
    max_epoch_estimated_bytes: int,
) -> dict[str, Any]:
    """Create and replay an internally valid compact queue-plan-v1 epoch."""

    epoch_candidates = copy.deepcopy(candidates)
    for local_ordinal, candidate in enumerate(epoch_candidates, 1):
        candidate["queue_ordinal"] = local_ordinal
        candidate["defer_reason"] = None
    selection_basis, all_explicit = _compact_selection_basis(
        epoch_candidates, parent_plan["plan_id"], epoch_ordinal
    )
    limits = copy.deepcopy(parent_plan["limits"])
    limits["max_items"] = max_epoch_items
    limits["plan_budget_bytes"] = max_epoch_estimated_bytes
    limits["selection_only"] = all_explicit
    selected_bytes = sum(candidate["estimated_bytes"] for candidate in epoch_candidates)
    summary = {
        "supported_unacquired_sources": len(epoch_candidates),
        "recording_candidates": len(epoch_candidates),
        "selected_count": len(epoch_candidates),
        "selected_estimated_bytes": selected_bytes,
        "deferred_count": 0,
        "already_acquired_recordings": 0,
        "withheld_access_sources": 0,
        "by_queue_state": _count_entries(
            [candidate["queue_state"] for candidate in epoch_candidates]
        ),
        "by_platform": _count_entries(
            [candidate["platform"] for candidate in epoch_candidates]
        ),
        "by_priority_tier": _count_entries(
            [candidate["priority_tier"] for candidate in epoch_candidates]
        ),
        "by_defer_reason": [],
    }
    core = {
        "schema_version": materialize_queue.SCHEMA_VERSION,
        "planned_at": parent_plan["planned_at"],
        "catalog_basis_sha256": parent_plan["catalog_basis_sha256"],
        "catalog_migrations": copy.deepcopy(parent_plan["catalog_migrations"]),
        "selection_basis": selection_basis,
        "wiki_scan": copy.deepcopy(parent_plan["wiki_scan"]),
        "limits": limits,
        "safety": copy.deepcopy(parent_plan["safety"]),
        "summary": summary,
        "candidates": epoch_candidates,
    }
    core_sha256 = materialize_queue.sha256_bytes(materialize_queue.canonical_bytes(core))
    plan = {"plan_id": materialize_queue.stable_id("acqplan", core_sha256), **core}
    try:
        return materialize_queue.validate_plan(plan)
    except materialize_queue.MaterializationError as error:
        raise CampaignMaterializationError(
            f"derived epoch {epoch_ordinal} failed queue-plan replay: {error}"
        ) from error


def _member(candidate: dict[str, Any], *, parent_ordinal: int) -> dict[str, Any]:
    return {
        "parent_queue_ordinal": parent_ordinal,
        "recording_id": candidate["recording_id"],
        "source_id": candidate["source_id"],
        "platform": candidate["platform"],
        "native_id": candidate["native_id"],
        "estimated_bytes": candidate["estimated_bytes"],
    }


def _member_digest(members: list[dict[str, Any]]) -> str:
    return materialize_queue.sha256_bytes(materialize_queue.canonical_bytes(members))


def _safe_private_root(path: Path) -> None:
    if path == Path("/"):
        raise CampaignMaterializationError("--campaign-root may not be the filesystem root")
    if path.exists() or path.is_symlink():
        if path.is_symlink() or not path.is_dir():
            raise CampaignMaterializationError("--campaign-root must be a real directory")
        if stat.S_IMODE(path.stat().st_mode) != 0o700:
            raise CampaignMaterializationError("existing --campaign-root must have mode 0700")
        return
    parent = path.parent
    if not parent.is_dir() or parent.is_symlink():
        raise CampaignMaterializationError(
            "--campaign-root parent must be an existing non-symlink directory"
        )
    path.mkdir(mode=0o700)


@contextmanager
def _writer_lock(root: Path) -> Iterator[None]:
    lock_path = root / LOCK_FILENAME
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(lock_path, flags, 0o600)
    with os.fdopen(descriptor, "a+b") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise CampaignMaterializationError("campaign writer lock is not regular")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise CampaignMaterializationError(
                f"another campaign epoch materializer holds the writer lock: {root}"
            ) from error
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _remove_stage(path: Path) -> None:
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


def _ensure_control_parent(root: Path, name: str) -> Path:
    path = root / name
    if path.exists() or path.is_symlink():
        if path.is_symlink() or not path.is_dir() or stat.S_IMODE(path.stat().st_mode) != 0o700:
            raise CampaignMaterializationError(f"existing {name} control directory is unsafe")
    else:
        path.mkdir(mode=0o700)
        _fsync_directory(root)
    return path


def _admit_immutable_document(
    parent: Path,
    object_id: str,
    filename: str,
    body: bytes,
) -> Path:
    """Atomically admit one ID-addressed directory containing one mode-0400 file."""

    final_dir = parent / object_id
    final_path = final_dir / filename
    if final_dir.exists() or final_dir.is_symlink():
        if final_dir.is_symlink() or not final_dir.is_dir():
            raise CampaignMaterializationError("existing immutable control object is unsafe")
        if stat.S_IMODE(final_dir.stat().st_mode) != 0o500:
            raise CampaignMaterializationError("existing immutable control directory must be 0500")
        observed = list(final_dir.iterdir())
        if observed != [final_path] and set(observed) != {final_path}:
            raise CampaignMaterializationError("existing immutable control object has extra entries")
        try:
            materialize_queue.require_read_only_regular(
                final_path, body, "campaign control file"
            )
        except materialize_queue.MaterializationError as error:
            raise CampaignMaterializationError(str(error)) from error
        return final_path
    stage = Path(tempfile.mkdtemp(prefix=f".{object_id}.", dir=str(parent)))
    admitted = False
    try:
        stage_path = stage / filename
        with stage_path.open("xb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        stage_path.chmod(0o400)
        _fsync_directory(stage)
        os.rename(stage, final_dir)
        admitted = True
        final_dir.chmod(0o500)
        _fsync_directory(final_dir)
        _fsync_directory(parent)
    except Exception:
        _remove_stage(final_dir if admitted else stage)
        raise
    return final_path


def materialize_campaign(
    *,
    parent_plan_path: Path,
    expected_parent_plan_sha256: str,
    campaign_root: Path,
    media_output_root: Path,
    executable_pin: dict[str, Any],
    max_epoch_items: int,
    max_epoch_estimated_bytes: int,
    global_cache_cap_bytes: int,
    free_space_floor_bytes: int,
    format_selector: str,
    http_timeout_seconds: int,
) -> tuple[dict[str, Any], Path]:
    parent_plan, parent_body, parent_path = read_sealed_parent_plan(
        str(parent_plan_path), expected_parent_plan_sha256
    )
    selected = selected_parent_candidates(parent_plan)
    partitions = partition_candidates(
        selected,
        max_epoch_items=max_epoch_items,
        max_epoch_estimated_bytes=max_epoch_estimated_bytes,
    )
    global_cache_cap_bytes = _integer(
        global_cache_cap_bytes, "global_cache_cap_bytes", 1
    )
    free_space_floor_bytes = _integer(
        free_space_floor_bytes, "free_space_floor_bytes"
    )
    http_timeout_seconds = _integer(http_timeout_seconds, "http_timeout_seconds", 1)
    if http_timeout_seconds > 3_600:
        raise CampaignMaterializationError("http_timeout_seconds may not exceed 3600")
    if parent_plan["limits"]["max_job_bytes"] > global_cache_cap_bytes:
        raise CampaignMaterializationError(
            "parent max_job_bytes exceeds global_cache_cap_bytes"
        )
    if (
        not isinstance(format_selector, str)
        or not 1 <= len(format_selector) <= 256
        or "\n" in format_selector
        or "\r" in format_selector
    ):
        raise CampaignMaterializationError("format_selector is invalid")
    _safe_private_root(campaign_root)

    parent_members = [
        _member(candidate, parent_ordinal=candidate["queue_ordinal"])
        for candidate in selected
    ]
    epoch_rows: list[dict[str, Any]] = []
    observed_members: list[dict[str, Any]] = []
    with _writer_lock(campaign_root):
        epoch_plans_parent = _ensure_control_parent(campaign_root, "epoch-plans")
        _ensure_control_parent(campaign_root, "campaigns")
        for epoch_ordinal, partition in enumerate(partitions, 1):
            epoch_plan = derive_epoch_plan(
                parent_plan,
                partition,
                epoch_ordinal=epoch_ordinal,
                max_epoch_items=max_epoch_items,
                max_epoch_estimated_bytes=max_epoch_estimated_bytes,
            )
            plan_body = materialize_queue.pretty_bytes(epoch_plan)
            plan_path = _admit_immutable_document(
                epoch_plans_parent, epoch_plan["plan_id"], "queue-plan.json", plan_body
            )
            bundle_manifest, orders = materialize_queue.build_bundle_manifest(
                epoch_plan,
                media_output_root=media_output_root,
                executable_pin=executable_pin,
                global_cache_cap_bytes=global_cache_cap_bytes,
                free_space_floor_bytes=free_space_floor_bytes,
                format_selector=format_selector,
                http_timeout_seconds=http_timeout_seconds,
            )
            bundle_dir = materialize_queue.admit_bundle(
                campaign_root, bundle_manifest, orders
            )
            bundle_manifest_path = bundle_dir / "manifest.json"
            bundle_body = materialize_queue.pretty_bytes(bundle_manifest)
            parent_ordinals = [candidate["queue_ordinal"] for candidate in partition]
            members = [
                _member(candidate, parent_ordinal=candidate["queue_ordinal"])
                for candidate in partition
            ]
            observed_members.extend(members)
            epoch_rows.append(
                {
                    "epoch_ordinal": epoch_ordinal,
                    "parent_queue_ordinal_first": parent_ordinals[0],
                    "parent_queue_ordinal_last": parent_ordinals[-1],
                    "local_to_parent_ordinal_mapping": (
                        "parent_queue_ordinal = parent_queue_ordinal_first + "
                        "local_queue_ordinal - 1"
                    ),
                    "parent_queue_ordinals_sha256": materialize_queue.sha256_bytes(
                        materialize_queue.canonical_bytes(parent_ordinals)
                    ),
                    "selected_count": len(partition),
                    "selected_estimated_bytes": sum(
                        candidate["estimated_bytes"] for candidate in partition
                    ),
                    "member_sha256": _member_digest(members),
                    "epoch_plan": {
                        "plan_id": epoch_plan["plan_id"],
                        "path": str(plan_path),
                        "sha256": materialize_queue.sha256_bytes(plan_body),
                        "byte_count": len(plan_body),
                    },
                    "bundle": {
                        "bundle_id": bundle_manifest["bundle_id"],
                        "manifest_path": str(bundle_manifest_path),
                        "manifest_sha256": materialize_queue.sha256_bytes(bundle_body),
                        "manifest_byte_count": len(bundle_body),
                        "work_order_count": bundle_manifest["work_order_count"],
                    },
                }
            )

        parent_keys = [
            (member["recording_id"], member["source_id"], member["platform"], member["native_id"])
            for member in parent_members
        ]
        observed_keys = [
            (member["recording_id"], member["source_id"], member["platform"], member["native_id"])
            for member in observed_members
        ]
        overlap_count = len(observed_keys) - len(set(observed_keys))
        missing = set(parent_keys) - set(observed_keys)
        unexpected = set(observed_keys) - set(parent_keys)
        parent_digest = _member_digest(parent_members)
        observed_digest = _member_digest(observed_members)
        if overlap_count or missing or unexpected or observed_members != parent_members:
            raise CampaignMaterializationError(
                "internal coverage proof failed: epoch union differs from parent selection"
            )
        core = {
            "schema_version": SCHEMA_VERSION,
            "materializer": {
                "name": "himr-archive-campaign-epoch-materializer",
                "version": IMPLEMENTATION_VERSION,
            },
            "parent_plan": {
                "plan_id": parent_plan["plan_id"],
                "path": str(parent_path),
                "sha256": materialize_queue.sha256_bytes(parent_body),
                "byte_count": len(parent_body),
                "selected_count": len(selected),
                "selected_estimated_bytes": sum(
                    candidate["estimated_bytes"] for candidate in selected
                ),
            },
            "partition_policy": {
                "algorithm": "stable_greedy_parent_queue_order_v1",
                "max_epoch_items": max_epoch_items,
                "max_epoch_estimated_bytes": max_epoch_estimated_bytes,
                "oversize_single_candidate_policy": "reject",
            },
            "materialization_policy": {
                "media_output_root": str(media_output_root),
                "global_cache_cap_bytes": global_cache_cap_bytes,
                "free_space_floor_bytes": free_space_floor_bytes,
                "direct_http_timeout_seconds": http_timeout_seconds,
                "yt_dlp": {**executable_pin, "format_selector": format_selector},
            },
            "coverage_proof": {
                "epoch_count": len(epoch_rows),
                "parent_selected_count": len(parent_members),
                "epoch_union_count": len(observed_members),
                "epoch_unique_count": len(set(observed_keys)),
                "overlap_count": overlap_count,
                "missing_count": len(missing),
                "unexpected_count": len(unexpected),
                "parent_selected_member_sha256": parent_digest,
                "epoch_union_member_sha256": observed_digest,
                "ordered_union_identical": observed_members == parent_members,
                "parent_ordinals_contiguous": [
                    member["parent_queue_ordinal"] for member in parent_members
                ]
                == list(range(1, len(parent_members) + 1)),
                "local_ordinals_contiguous": True,
            },
            "epochs": epoch_rows,
            "safety": {
                "control_files_only": True,
                "access_policy": "public_only",
                "publication_authority": "none",
                "credentials_allowed": False,
                "network_access_performed": False,
                "gpu_execution_performed": False,
                "cold_media_written": False,
                "catalog_mutated": False,
                "media_deleted": False,
            },
        }
        campaign_id = materialize_queue.stable_id(
            "acqcampaign", materialize_queue.sha256_bytes(materialize_queue.canonical_bytes(core))
        )
        manifest = {
            "campaign_id": campaign_id,
            "campaign_relative_path": f"campaigns/{campaign_id}",
            **core,
        }
        manifest_body = materialize_queue.pretty_bytes(manifest)
        campaigns_parent = campaign_root / "campaigns"
        manifest_path = _admit_immutable_document(
            campaigns_parent, campaign_id, "manifest.json", manifest_body
        )
    return manifest, manifest_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Offline-partition one sealed Archive queue plan and admit immutable "
            "per-epoch acquisition bundles"
        )
    )
    parser.add_argument("--parent-plan", required=True)
    parser.add_argument("--expected-parent-plan-sha256", required=True)
    parser.add_argument("--campaign-root", required=True)
    parser.add_argument("--media-output-root", required=True)
    parser.add_argument("--yt-dlp-executable", required=True)
    parser.add_argument("--yt-dlp-sha256", required=True)
    parser.add_argument("--global-cache-cap-bytes", type=int, required=True)
    parser.add_argument("--free-space-floor-bytes", type=int, required=True)
    parser.add_argument("--max-epoch-items", type=int, default=DEFAULT_MAX_EPOCH_ITEMS)
    parser.add_argument(
        "--max-epoch-estimated-bytes",
        type=int,
        default=DEFAULT_MAX_EPOCH_ESTIMATED_BYTES,
    )
    parser.add_argument("--format-selector", default=materialize_queue.DEFAULT_FORMAT_SELECTOR)
    parser.add_argument(
        "--http-timeout-seconds",
        type=int,
        default=materialize_queue.DEFAULT_HTTP_TIMEOUT_SECONDS,
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        parent_path = _canonical_absolute_path(args.parent_plan, "--parent-plan", must_exist=True)
        campaign_root = _canonical_absolute_path(args.campaign_root, "--campaign-root")
        media_output_root = _canonical_absolute_path(
            args.media_output_root, "--media-output-root"
        )
        try:
            acquire.validate_output_root(media_output_root)
        except acquire.AcquisitionError as error:
            raise CampaignMaterializationError(
                f"invalid --media-output-root: {error}"
            ) from error
        executable = _canonical_absolute_path(
            args.yt_dlp_executable, "--yt-dlp-executable", must_exist=True
        )
        try:
            executable_pin = materialize_queue.inspect_pinned_executable(
                executable, args.yt_dlp_sha256
            )
        except materialize_queue.MaterializationError as error:
            raise CampaignMaterializationError(str(error)) from error
        manifest, manifest_path = materialize_campaign(
            parent_plan_path=parent_path,
            expected_parent_plan_sha256=args.expected_parent_plan_sha256,
            campaign_root=campaign_root,
            media_output_root=media_output_root,
            executable_pin=executable_pin,
            max_epoch_items=args.max_epoch_items,
            max_epoch_estimated_bytes=args.max_epoch_estimated_bytes,
            global_cache_cap_bytes=args.global_cache_cap_bytes,
            free_space_floor_bytes=args.free_space_floor_bytes,
            format_selector=args.format_selector,
            http_timeout_seconds=args.http_timeout_seconds,
        )
        manifest_body = materialize_queue.pretty_bytes(manifest)
        sys.stdout.buffer.write(
            materialize_queue.pretty_bytes(
                {
                    "schema_version": SCHEMA_VERSION,
                    "status": "materialized",
                    "campaign_id": manifest["campaign_id"],
                    "campaign_manifest_path": str(manifest_path),
                    "campaign_manifest_sha256": materialize_queue.sha256_bytes(manifest_body),
                    "epoch_count": manifest["coverage_proof"]["epoch_count"],
                    "selected_count": manifest["coverage_proof"]["parent_selected_count"],
                    "selected_estimated_bytes": manifest["parent_plan"][
                        "selected_estimated_bytes"
                    ],
                    "safety": manifest["safety"],
                }
            )
        )
        return 0
    except (CampaignMaterializationError, OSError) as error:
        sys.stderr.buffer.write(
            materialize_queue.pretty_bytes(
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
