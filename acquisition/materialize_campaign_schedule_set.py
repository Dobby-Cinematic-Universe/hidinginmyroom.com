#!/usr/bin/env python3
"""Materialize the two-role Archive campaign into sealed producer schedules.

This module is an offline control-plane boundary.  It replays two exact campaign
epoch manifests, proves their selected-source union is disjoint, builds one
background-producer schedule for every epoch through the established producer API,
and admits a final schedule-set manifest only after every schedule is sealed.  It
does not run a producer, inspect acquisition results, contact a provider, create a
preprocess state directory, or read/write the cold media root.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import stat
import sys
from pathlib import Path
from typing import Any

if __package__:
    from . import acquire

    # The established queue materializer retains its historical top-level import.
    # Publish the already-loaded repository-local module under that name before
    # importing the producer stack so package imports remain offline and unambiguous.
    sys.modules.setdefault("acquire", acquire)
    from . import materialize_queue

    sys.modules.setdefault("materialize_queue", materialize_queue)
    from . import queue_runner
    from . import background_producer, materialize_campaign_epochs
else:  # pragma: no cover - direct CLI execution
    import background_producer  # type: ignore[no-redef]
    import materialize_campaign_epochs  # type: ignore[no-redef]
    import materialize_queue  # type: ignore[no-redef]
    import queue_runner  # type: ignore[no-redef]


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
MATERIALIZER_NAME = "himr-archive-campaign-schedule-set-materializer"
SCHEDULE_SET_KIND = "sealed_archive_campaign_background_schedule_set"
SCHEDULE_SET_ID_RE = re.compile(r"^bgacqscheduleset_[0-9a-f]{32}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

NORMAL_ROLE = "normal_processing"
COLD_ONLY_ROLE = "cold_acquisition_only_requires_chunking"
ROLE_ORDER = (NORMAL_ROLE, COLD_ONLY_ROLE)

COLD_MEDIA_ROOT = Path("/mnt/archive/HIMR/corpus/raw/acquisition-cas")
COLD_ARCHIVE_ROOT = Path("/mnt/archive/HIMR")
GIB = 1024**3
TIB = 1024**4

ROLE_EPOCH_CAPS: dict[str, dict[str, int]] = {
    NORMAL_ROLE: {
        "max_epoch_items": 32,
        "max_epoch_estimated_bytes": 16 * GIB,
    },
    COLD_ONLY_ROLE: {
        "max_epoch_items": 8,
        "max_epoch_estimated_bytes": 128 * GIB,
    },
}

ROLE_POLICIES: dict[str, dict[str, int]] = {
    NORMAL_ROLE: {
        # A normal epoch is capped at 32 inputs.  The larger raw receipt-accounted
        # watermark prevents controller-parked preprocess ordinals from wedging the
        # epoch; the controller retains its separate 16-item runnable-ready cap.
        "ready_high_items": 64,
        "ready_low_items": 32,
        "ready_high_bytes": 64 * GIB,
        "ready_low_bytes": 32 * GIB,
        "maximum_dispatch_items_per_run": 8,
        "maximum_dispatch_bytes_per_run": 64 * GIB,
        "maximum_run_seconds": 14_400,
        "free_space_floor_bytes": 512 * GIB,
    },
    COLD_ONLY_ROLE: {
        # This role deliberately does not feed today's normal preprocess path.  Its
        # dispatch cap matches the reviewed <=8 item / <=128 GiB cold epoch bound so
        # a bounded epoch can drain without repeated completed-payload replay.
        "ready_high_items": 1_000,
        "ready_low_items": 999,
        "ready_high_bytes": 4 * TIB,
        "ready_low_bytes": 3 * TIB,
        "maximum_dispatch_items_per_run": 8,
        "maximum_dispatch_bytes_per_run": 128 * GIB,
        "maximum_run_seconds": 14_400,
        "free_space_floor_bytes": 512 * GIB,
    },
}

CAMPAIGN_SAFETY = {
    "control_files_only": True,
    "access_policy": "public_only",
    "publication_authority": "none",
    "credentials_allowed": False,
    "network_access_performed": False,
    "gpu_execution_performed": False,
    "cold_media_written": False,
    "catalog_mutated": False,
    "media_deleted": False,
}

SCHEDULE_SET_SAFETY = {
    "control_files_only": True,
    "access_policy": "sealed_public_archive_work_orders_only",
    "publication_authority": "none",
    "credentials_allowed": False,
    "network_access_performed": False,
    "gpu_execution_performed": False,
    "media_read_performed": False,
    "media_write_performed": False,
    "catalog_mutated": False,
    "scheduling_performed": False,
    "processes_started": False,
}

MAX_CAMPAIGN_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_SCHEDULE_SET_BYTES = 16 * 1024 * 1024
MAX_SCHEDULES = 2_048
MAX_INTEGER = 2**63 - 1


class ScheduleSetMaterializationError(RuntimeError):
    """A sealed input, coverage proof, path, policy, or admission failed closed."""


def canonical_bytes(value: Any) -> bytes:
    return materialize_queue.canonical_bytes(value)


def pretty_bytes(value: Any) -> bytes:
    return materialize_queue.pretty_bytes(value)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _exact(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ScheduleSetMaterializationError(f"{label} must be an object")
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    if missing or unknown:
        raise ScheduleSetMaterializationError(
            f"{label} keys differ from the exact contract; "
            f"missing={missing}, unknown={unknown}"
        )
    return value


def _integer(
    value: Any,
    label: str,
    minimum: int = 0,
    maximum: int = MAX_INTEGER,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise ScheduleSetMaterializationError(
            f"{label} must be an integer from {minimum} through {maximum}"
        )
    return value


def _text(value: Any, label: str, maximum: int = 4_096) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise ScheduleSetMaterializationError(f"{label} must be bounded non-empty text")
    return value


def _sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise ScheduleSetMaterializationError(f"{label} must be a lowercase SHA-256")
    return value


def _absolute_path(value: Any, label: str) -> Path:
    raw = _text(value, label, 16_384)
    path = Path(raw)
    if (
        "://" in raw
        or not path.is_absolute()
        or str(path) != raw
        or os.path.normpath(raw) != raw
        or raw == "/"
        or "//" in raw
        or "\\" in raw
    ):
        raise ScheduleSetMaterializationError(
            f"{label} must be one normalized lexical absolute local path"
        )
    return path


def _is_within(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _stable_json(
    path_value: Any,
    expected_sha256: Any,
    *,
    maximum: int,
    label: str,
) -> tuple[dict[str, Any], bytes, Path]:
    path = _absolute_path(path_value, label)
    expected = _sha256(expected_sha256, f"{label} expected SHA-256")
    try:
        body, _ = queue_runner._stable_read(
            path,
            maximum=maximum,
            label=label,
            required_mode=0o400,
        )
    except queue_runner.QueueRunnerError as error:
        raise ScheduleSetMaterializationError(str(error)) from error
    if sha256_bytes(body) != expected:
        raise ScheduleSetMaterializationError(
            f"{label} differs from its expected SHA-256"
        )
    try:
        raw = materialize_queue.load_json_bytes(body, label)
    except materialize_queue.MaterializationError as error:
        raise ScheduleSetMaterializationError(str(error)) from error
    if not isinstance(raw, dict):
        raise ScheduleSetMaterializationError(f"{label} must contain a JSON object")
    return raw, body, path


def _replay_file(path: Path, body: bytes, maximum: int, label: str) -> None:
    try:
        replay, _ = queue_runner._stable_read(
            path,
            maximum=maximum,
            label=label,
            required_mode=0o400,
        )
    except queue_runner.QueueRunnerError as error:
        raise ScheduleSetMaterializationError(str(error)) from error
    if replay != body:
        raise ScheduleSetMaterializationError(f"{label} changed after validation")


def _validate_control_root(path_value: Any) -> tuple[Path, bool]:
    root = _absolute_path(path_value, "schedule-set control root")
    if _is_within(root, COLD_ARCHIVE_ROOT) or _is_within(COLD_ARCHIVE_ROOT, root):
        raise ScheduleSetMaterializationError(
            "schedule-set control root must be disjoint from the cold archive"
        )
    exists = root.exists() or root.is_symlink()
    inspect = root if exists else root.parent
    label = "schedule-set control root" if exists else "schedule-set control root parent"
    try:
        observed = inspect.lstat()
        resolved = inspect.resolve(strict=True)
    except OSError as error:
        raise ScheduleSetMaterializationError(f"cannot inspect {label}") from error
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) & 0o022
        or resolved != inspect
    ):
        raise ScheduleSetMaterializationError(
            f"{label} must be current-user-controlled, real, and not peer-writable"
        )
    if exists and stat.S_IMODE(observed.st_mode) != 0o700:
        raise ScheduleSetMaterializationError(
            "existing schedule-set control root must have mode 0700"
        )
    return root, exists


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _admit_control_root(root: Path, existed: bool) -> None:
    if not existed:
        try:
            root.mkdir(mode=0o700)
            _fsync_directory(root.parent)
        except FileExistsError:
            pass
    checked, checked_exists = _validate_control_root(str(root))
    if checked != root or not checked_exists:
        raise ScheduleSetMaterializationError(
            "schedule-set control root could not be admitted safely"
        )


def _campaign_member(candidate: dict[str, Any], parent_ordinal: int) -> dict[str, Any]:
    return {
        "parent_queue_ordinal": parent_ordinal,
        "recording_id": candidate["recording_id"],
        "source_id": candidate["source_id"],
        "platform": candidate["platform"],
        "native_id": candidate["native_id"],
        "estimated_bytes": candidate["estimated_bytes"],
    }


def _member_digest(members: list[dict[str, Any]]) -> str:
    return sha256_bytes(canonical_bytes(members))


def _campaign_reference(
    role: str,
    manifest: dict[str, Any],
    path: Path,
    body: bytes,
    member_digest: str,
) -> dict[str, Any]:
    return {
        "role": role,
        "campaign_id": manifest["campaign_id"],
        "manifest_path": str(path),
        "manifest_sha256": sha256_bytes(body),
        "manifest_byte_count": len(body),
        "epoch_count": manifest["coverage_proof"]["epoch_count"],
        "selected_count": manifest["parent_plan"]["selected_count"],
        "selected_estimated_bytes": manifest["parent_plan"][
            "selected_estimated_bytes"
        ],
        "ordered_member_sha256": member_digest,
    }


def _validate_campaign(
    role: str,
    manifest_path: Path,
    expected_manifest_sha256: str,
) -> dict[str, Any]:
    if role not in ROLE_ORDER:
        raise ScheduleSetMaterializationError(f"unsupported campaign role: {role}")
    raw, body, path = _stable_json(
        str(manifest_path),
        expected_manifest_sha256,
        maximum=MAX_CAMPAIGN_MANIFEST_BYTES,
        label=f"{role} campaign manifest",
    )
    manifest = _exact(
        raw,
        f"{role} campaign manifest",
        {
            "campaign_id",
            "campaign_relative_path",
            "schema_version",
            "materializer",
            "parent_plan",
            "partition_policy",
            "materialization_policy",
            "coverage_proof",
            "epochs",
            "safety",
        },
    )
    campaign_id = _text(manifest["campaign_id"], f"{role} campaign ID", 80)
    if re.fullmatch(r"acqcampaign_[0-9a-f]{32}", campaign_id) is None:
        raise ScheduleSetMaterializationError(f"{role} campaign ID is invalid")
    if manifest["campaign_relative_path"] != f"campaigns/{campaign_id}":
        raise ScheduleSetMaterializationError(
            f"{role} campaign relative path differs from its ID"
        )
    if manifest["schema_version"] != materialize_campaign_epochs.SCHEMA_VERSION:
        raise ScheduleSetMaterializationError(f"{role} campaign schema is unsupported")
    materializer = _exact(
        manifest["materializer"],
        f"{role} campaign materializer",
        {"name", "version"},
    )
    if materializer != {
        "name": "himr-archive-campaign-epoch-materializer",
        "version": materialize_campaign_epochs.IMPLEMENTATION_VERSION,
    }:
        raise ScheduleSetMaterializationError(
            f"{role} campaign materializer is unsupported"
        )
    if manifest["safety"] != CAMPAIGN_SAFETY:
        raise ScheduleSetMaterializationError(
            f"{role} campaign weakens the offline public-only safety contract"
        )
    identity_core = {
        key: value
        for key, value in manifest.items()
        if key not in {"campaign_id", "campaign_relative_path"}
    }
    expected_id = materialize_queue.stable_id(
        "acqcampaign", sha256_bytes(canonical_bytes(identity_core))
    )
    if campaign_id != expected_id or body != pretty_bytes(manifest):
        raise ScheduleSetMaterializationError(
            f"{role} campaign identity or canonical serialization is inconsistent"
        )

    parent_ref = _exact(
        manifest["parent_plan"],
        f"{role} parent plan reference",
        {
            "plan_id",
            "path",
            "sha256",
            "byte_count",
            "selected_count",
            "selected_estimated_bytes",
        },
    )
    try:
        parent_plan, parent_body, parent_path = (
            materialize_campaign_epochs.read_sealed_parent_plan(
                str(_absolute_path(parent_ref["path"], f"{role} parent plan path")),
                _sha256(parent_ref["sha256"], f"{role} parent plan SHA-256"),
            )
        )
        parent_selected = materialize_campaign_epochs.selected_parent_candidates(
            parent_plan
        )
    except materialize_campaign_epochs.CampaignMaterializationError as error:
        raise ScheduleSetMaterializationError(str(error)) from error
    parent_selected_bytes = sum(row["estimated_bytes"] for row in parent_selected)
    if parent_ref != {
        "plan_id": parent_plan["plan_id"],
        "path": str(parent_path),
        "sha256": sha256_bytes(parent_body),
        "byte_count": len(parent_body),
        "selected_count": len(parent_selected),
        "selected_estimated_bytes": parent_selected_bytes,
    }:
        raise ScheduleSetMaterializationError(
            f"{role} parent plan reference differs from its sealed plan"
        )

    partition_policy = _exact(
        manifest["partition_policy"],
        f"{role} partition policy",
        {
            "algorithm",
            "max_epoch_items",
            "max_epoch_estimated_bytes",
            "oversize_single_candidate_policy",
        },
    )
    if (
        partition_policy["algorithm"] != "stable_greedy_parent_queue_order_v1"
        or partition_policy["oversize_single_candidate_policy"] != "reject"
    ):
        raise ScheduleSetMaterializationError(
            f"{role} campaign partition algorithm is unsupported"
        )
    max_epoch_items = _integer(
        partition_policy["max_epoch_items"],
        f"{role} max epoch items",
        1,
        materialize_campaign_epochs.MAX_EPOCH_ITEMS,
    )
    max_epoch_bytes = _integer(
        partition_policy["max_epoch_estimated_bytes"],
        f"{role} max epoch estimated bytes",
        1,
        materialize_campaign_epochs.MAX_EPOCH_ESTIMATED_BYTES,
    )
    if {
        "max_epoch_items": max_epoch_items,
        "max_epoch_estimated_bytes": max_epoch_bytes,
    } != ROLE_EPOCH_CAPS[role]:
        expected_cap = ROLE_EPOCH_CAPS[role]
        raise ScheduleSetMaterializationError(
            f"{role} campaign must seal the exact role epoch cap: "
            f"items={expected_cap['max_epoch_items']}, "
            f"estimated_bytes={expected_cap['max_epoch_estimated_bytes']}"
        )
    try:
        expected_partitions = materialize_campaign_epochs.partition_candidates(
            parent_selected,
            max_epoch_items=max_epoch_items,
            max_epoch_estimated_bytes=max_epoch_bytes,
        )
    except materialize_campaign_epochs.CampaignMaterializationError as error:
        raise ScheduleSetMaterializationError(str(error)) from error

    materialization_policy = _exact(
        manifest["materialization_policy"],
        f"{role} materialization policy",
        {
            "media_output_root",
            "global_cache_cap_bytes",
            "free_space_floor_bytes",
            "direct_http_timeout_seconds",
            "yt_dlp",
        },
    )
    if materialization_policy["media_output_root"] != str(COLD_MEDIA_ROOT):
        raise ScheduleSetMaterializationError(
            f"{role} campaign must use the reviewed shared cold-primary media root"
        )
    global_cap = _integer(
        materialization_policy["global_cache_cap_bytes"],
        f"{role} global cache cap",
        1,
    )
    queue_floor = _integer(
        materialization_policy["free_space_floor_bytes"],
        f"{role} queue free-space floor",
    )
    if queue_floor > ROLE_POLICIES[role]["free_space_floor_bytes"]:
        raise ScheduleSetMaterializationError(
            f"{role} queue floor exceeds the exact schedule-set floor"
        )
    timeout = _integer(
        materialization_policy["direct_http_timeout_seconds"],
        f"{role} direct HTTP timeout",
        1,
        3_600,
    )
    yt_dlp = _exact(
        materialization_policy["yt_dlp"],
        f"{role} yt-dlp pin",
        {"executable", "sha256", "byte_count", "format_selector"},
    )
    executable = _absolute_path(yt_dlp["executable"], f"{role} yt-dlp executable")
    executable_pin = {
        "executable": str(executable),
        "sha256": _sha256(yt_dlp["sha256"], f"{role} yt-dlp SHA-256"),
        "byte_count": _integer(
            yt_dlp["byte_count"], f"{role} yt-dlp byte count", 1
        ),
    }
    format_selector = _text(
        yt_dlp["format_selector"], f"{role} yt-dlp format selector", 256
    )

    epoch_rows = manifest["epochs"]
    if (
        not isinstance(epoch_rows, list)
        or not epoch_rows
        or len(epoch_rows) != len(expected_partitions)
        or len(epoch_rows) > MAX_SCHEDULES
    ):
        raise ScheduleSetMaterializationError(
            f"{role} campaign epoch count differs from deterministic partition replay"
        )

    observed_members: list[dict[str, Any]] = []
    validated_epochs: list[dict[str, Any]] = []
    sealed_inputs: list[tuple[Path, bytes, int, str]] = [
        (path, body, MAX_CAMPAIGN_MANIFEST_BYTES, f"{role} campaign manifest"),
        (
            parent_path,
            parent_body,
            materialize_queue.MAX_PLAN_BYTES,
            f"{role} parent plan",
        ),
    ]
    next_parent_ordinal = 1
    for epoch_ordinal, (raw_epoch, partition) in enumerate(
        zip(epoch_rows, expected_partitions, strict=True), 1
    ):
        epoch = _exact(
            raw_epoch,
            f"{role} epoch {epoch_ordinal}",
            {
                "epoch_ordinal",
                "parent_queue_ordinal_first",
                "parent_queue_ordinal_last",
                "local_to_parent_ordinal_mapping",
                "parent_queue_ordinals_sha256",
                "selected_count",
                "selected_estimated_bytes",
                "member_sha256",
                "epoch_plan",
                "bundle",
            },
        )
        count = len(partition)
        estimated_bytes = sum(candidate["estimated_bytes"] for candidate in partition)
        role_cap = ROLE_EPOCH_CAPS[role]
        if (
            count > role_cap["max_epoch_items"]
            or estimated_bytes > role_cap["max_epoch_estimated_bytes"]
        ):
            raise ScheduleSetMaterializationError(
                f"{role} epoch {epoch_ordinal} exceeds its exact role cap"
            )
        parent_ordinals = list(range(next_parent_ordinal, next_parent_ordinal + count))
        members = [
            _campaign_member(candidate, parent_ordinal)
            for candidate, parent_ordinal in zip(
                partition, parent_ordinals, strict=True
            )
        ]
        if (
            epoch["epoch_ordinal"] != epoch_ordinal
            or epoch["parent_queue_ordinal_first"] != parent_ordinals[0]
            or epoch["parent_queue_ordinal_last"] != parent_ordinals[-1]
            or epoch["local_to_parent_ordinal_mapping"]
            != "parent_queue_ordinal = parent_queue_ordinal_first + local_queue_ordinal - 1"
            or epoch["parent_queue_ordinals_sha256"]
            != sha256_bytes(canonical_bytes(parent_ordinals))
            or epoch["selected_count"] != count
            or epoch["selected_estimated_bytes"] != estimated_bytes
            or epoch["member_sha256"] != _member_digest(members)
        ):
            raise ScheduleSetMaterializationError(
                f"{role} epoch {epoch_ordinal} differs from deterministic partition replay"
            )

        plan_ref = _exact(
            epoch["epoch_plan"],
            f"{role} epoch {epoch_ordinal} plan reference",
            {"plan_id", "path", "sha256", "byte_count"},
        )
        plan_raw, plan_body, plan_path = _stable_json(
            plan_ref["path"],
            plan_ref["sha256"],
            maximum=materialize_queue.MAX_PLAN_BYTES,
            label=f"{role} epoch {epoch_ordinal} plan",
        )
        try:
            plan = materialize_queue.validate_plan(plan_raw)
            expected_plan = materialize_campaign_epochs.derive_epoch_plan(
                parent_plan,
                partition,
                epoch_ordinal=epoch_ordinal,
                max_epoch_items=max_epoch_items,
                max_epoch_estimated_bytes=max_epoch_bytes,
            )
        except (
            materialize_queue.MaterializationError,
            materialize_campaign_epochs.CampaignMaterializationError,
        ) as error:
            raise ScheduleSetMaterializationError(str(error)) from error
        if (
            plan != expected_plan
            or plan_body != pretty_bytes(plan)
            or plan_ref
            != {
                "plan_id": plan["plan_id"],
                "path": str(plan_path),
                "sha256": sha256_bytes(plan_body),
                "byte_count": len(plan_body),
            }
        ):
            raise ScheduleSetMaterializationError(
                f"{role} epoch {epoch_ordinal} plan differs from exact derivation"
            )

        bundle_ref = _exact(
            epoch["bundle"],
            f"{role} epoch {epoch_ordinal} bundle reference",
            {
                "bundle_id",
                "manifest_path",
                "manifest_sha256",
                "manifest_byte_count",
                "work_order_count",
            },
        )
        bundle_path = _absolute_path(
            bundle_ref["manifest_path"],
            f"{role} epoch {epoch_ordinal} bundle path",
        )
        try:
            bundle = queue_runner._load_bundle(bundle_path)
            expected_bundle_manifest, expected_orders = (
                materialize_queue.build_bundle_manifest(
                    expected_plan,
                    media_output_root=COLD_MEDIA_ROOT,
                    executable_pin=executable_pin,
                    global_cache_cap_bytes=global_cap,
                    free_space_floor_bytes=queue_floor,
                    format_selector=format_selector,
                    http_timeout_seconds=timeout,
                )
            )
        except (
            queue_runner.QueueRunnerError,
            materialize_queue.MaterializationError,
        ) as error:
            raise ScheduleSetMaterializationError(str(error)) from error
        expected_order_bodies = [order_body for _relative, order_body in expected_orders]
        if (
            bundle["manifest"] != expected_bundle_manifest
            or bundle["order_bodies"] != expected_order_bodies
            or bundle_ref
            != {
                "bundle_id": bundle["manifest"]["bundle_id"],
                "manifest_path": str(bundle["path"]),
                "manifest_sha256": sha256_bytes(bundle["body"]),
                "manifest_byte_count": len(bundle["body"]),
                "work_order_count": len(bundle["orders"]),
            }
        ):
            raise ScheduleSetMaterializationError(
                f"{role} epoch {epoch_ordinal} bundle differs from exact derivation"
            )
        for local_ordinal, (candidate, order) in enumerate(
            zip(partition, bundle["orders"], strict=True), 1
        ):
            if (
                candidate["platform"] != "internet_archive"
                or candidate["source_kind"] != "archive_media_file"
                or candidate["adapter"] != "direct_http"
                or order["adapter"] != "direct_http"
                or order["source"]["platform"] != "internet_archive"
                or order["source"]["source_kind"] != "archive_media_file"
                or order["source"]["native_id"] != candidate["native_id"]
                or order["source"]["canonical_url"] != candidate["canonical_url"]
                or order["output"]["root"] != str(COLD_MEDIA_ROOT)
                or bundle["manifest"]["work_orders"][local_ordinal - 1][
                    "recording_id"
                ]
                != candidate["recording_id"]
                or bundle["manifest"]["work_orders"][local_ordinal - 1]["source_id"]
                != candidate["source_id"]
            ):
                raise ScheduleSetMaterializationError(
                    f"{role} epoch {epoch_ordinal} contains a non-Archive or substituted order"
                )

        observed_members.extend(members)
        validated_epochs.append(
            {
                "epoch": epoch,
                "plan": plan,
                "plan_path": plan_path,
                "plan_body": plan_body,
                "bundle": bundle,
                "members": members,
            }
        )
        sealed_inputs.extend(
            [
                (
                    plan_path,
                    plan_body,
                    materialize_queue.MAX_PLAN_BYTES,
                    f"{role} epoch {epoch_ordinal} plan",
                ),
                (
                    bundle["path"],
                    bundle["body"],
                    queue_runner.MAX_MANIFEST_BYTES,
                    f"{role} epoch {epoch_ordinal} bundle manifest",
                ),
            ]
        )
        next_parent_ordinal += count

    parent_members = [
        _campaign_member(candidate, candidate["queue_ordinal"])
        for candidate in parent_selected
    ]
    parent_keys = [
        (
            member["recording_id"],
            member["source_id"],
            member["platform"],
            member["native_id"],
        )
        for member in parent_members
    ]
    observed_keys = [
        (
            member["recording_id"],
            member["source_id"],
            member["platform"],
            member["native_id"],
        )
        for member in observed_members
    ]
    overlap_count = len(observed_keys) - len(set(observed_keys))
    expected_coverage = {
        "epoch_count": len(validated_epochs),
        "parent_selected_count": len(parent_members),
        "epoch_union_count": len(observed_members),
        "epoch_unique_count": len(set(observed_keys)),
        "overlap_count": overlap_count,
        "missing_count": len(set(parent_keys) - set(observed_keys)),
        "unexpected_count": len(set(observed_keys) - set(parent_keys)),
        "parent_selected_member_sha256": _member_digest(parent_members),
        "epoch_union_member_sha256": _member_digest(observed_members),
        "ordered_union_identical": observed_members == parent_members,
        "parent_ordinals_contiguous": [
            member["parent_queue_ordinal"] for member in parent_members
        ]
        == list(range(1, len(parent_members) + 1)),
        "local_ordinals_contiguous": True,
    }
    if manifest["coverage_proof"] != expected_coverage:
        raise ScheduleSetMaterializationError(
            f"{role} campaign coverage proof differs from exact replay"
        )
    return {
        "role": role,
        "manifest": manifest,
        "path": path,
        "body": body,
        "parent_plan": parent_plan,
        "epochs": validated_epochs,
        "members": observed_members,
        "sealed_inputs": sealed_inputs,
    }


def _expected_schedule_policy(role: str) -> dict[str, Any]:
    selected = ROLE_POLICIES[role]
    return {
        "maximum_network_concurrency": 1,
        "dispatch_order": "sealed_ordinal_sequential_bounded_failure_isolation",
        "ready_high_items": selected["ready_high_items"],
        "ready_low_items": selected["ready_low_items"],
        "ready_high_bytes": selected["ready_high_bytes"],
        "ready_low_bytes": selected["ready_low_bytes"],
        "maximum_dispatch_items_per_run": selected[
            "maximum_dispatch_items_per_run"
        ],
        "maximum_dispatch_bytes_per_run": selected[
            "maximum_dispatch_bytes_per_run"
        ],
        "maximum_run_seconds": selected["maximum_run_seconds"],
        "free_space_floor_bytes": selected["free_space_floor_bytes"],
        "reservation_accounting": "full_sealed_max_job_bytes",
        "hysteresis_resume": "both_low_water_conditions",
    }


def _schedule_member_rows(
    role: str,
    campaign_id: str,
    epoch_ordinal: int,
    members: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "role": role,
            "campaign_id": campaign_id,
            "epoch_ordinal": epoch_ordinal,
            "local_queue_ordinal": local_ordinal,
            **member,
        }
        for local_ordinal, member in enumerate(members, 1)
    ]


def materialize_schedule_set(
    *,
    normal_campaign_manifest_path: Path,
    expected_normal_campaign_manifest_sha256: str,
    cold_only_campaign_manifest_path: Path,
    expected_cold_only_campaign_manifest_sha256: str,
    control_root: Path,
) -> tuple[dict[str, Any], Path]:
    """Replay both role campaigns and admit their deterministic schedule set."""

    root, root_existed = _validate_control_root(str(control_root))
    campaigns = [
        _validate_campaign(
            NORMAL_ROLE,
            normal_campaign_manifest_path,
            expected_normal_campaign_manifest_sha256,
        ),
        _validate_campaign(
            COLD_ONLY_ROLE,
            cold_only_campaign_manifest_path,
            expected_cold_only_campaign_manifest_sha256,
        ),
    ]
    if [campaign["role"] for campaign in campaigns] != list(ROLE_ORDER):
        raise ScheduleSetMaterializationError("campaign roles are not exact and ordered")
    if len({campaign["manifest"]["campaign_id"] for campaign in campaigns}) != 2:
        raise ScheduleSetMaterializationError(
            "the two schedule-set roles must reference distinct campaigns"
        )

    all_member_rows: list[dict[str, Any]] = []
    source_epoch_projection: list[dict[str, Any]] = []
    for campaign in campaigns:
        role = campaign["role"]
        campaign_id = campaign["manifest"]["campaign_id"]
        for epoch in campaign["epochs"]:
            epoch_row = epoch["epoch"]
            member_rows = _schedule_member_rows(
                role,
                campaign_id,
                epoch_row["epoch_ordinal"],
                epoch["members"],
            )
            all_member_rows.extend(member_rows)
            source_epoch_projection.append(
                {
                    "role": role,
                    "campaign_id": campaign_id,
                    "epoch_ordinal": epoch_row["epoch_ordinal"],
                    "epoch_plan_id": epoch["plan"]["plan_id"],
                    "bundle_id": epoch["bundle"]["manifest"]["bundle_id"],
                    "selected_count": epoch_row["selected_count"],
                    "selected_estimated_bytes": epoch_row[
                        "selected_estimated_bytes"
                    ],
                    "member_sha256": epoch_row["member_sha256"],
                }
            )
    if not all_member_rows:
        raise ScheduleSetMaterializationError("schedule set selects no Archive members")
    source_ids = [row["source_id"] for row in all_member_rows]
    recording_ids = [row["recording_id"] for row in all_member_rows]
    native_keys = [(row["platform"], row["native_id"]) for row in all_member_rows]
    overlap_count = len(source_ids) - len(set(source_ids))
    if (
        overlap_count
        or len(recording_ids) != len(set(recording_ids))
        or len(native_keys) != len(set(native_keys))
    ):
        raise ScheduleSetMaterializationError(
            "normal and cold-only campaign member unions overlap"
        )

    schedules_root = root / "schedules"
    preprocess_root = root / "preprocess-state"
    set_objects_root = root / "schedule-sets"
    schedule_documents: list[tuple[Path, bytes, dict[str, Any]]] = []
    entries: list[dict[str, Any]] = []
    schedule_member_rows: list[dict[str, Any]] = []
    schedule_epoch_projection: list[dict[str, Any]] = []
    role_totals: list[dict[str, Any]] = []
    for campaign in campaigns:
        role = campaign["role"]
        campaign_id = campaign["manifest"]["campaign_id"]
        role_count = 0
        role_bytes = 0
        for epoch in campaign["epochs"]:
            epoch_row = epoch["epoch"]
            bundle_id = epoch["bundle"]["manifest"]["bundle_id"]
            leaf = f"epoch-{epoch_row['epoch_ordinal']:06d}-{bundle_id}"
            schedule_path = schedules_root / role / leaf / "schedule.json"
            preprocess_path = preprocess_root / role / leaf
            if (
                schedule_path.parent.parent.parent != schedules_root
                or preprocess_path.parent.parent != preprocess_root
                or not _is_within(schedule_path, root)
                or not _is_within(preprocess_path, root)
            ):
                raise ScheduleSetMaterializationError(
                    "derived schedule or preprocess path escaped its hot control root"
                )
            selected_policy = ROLE_POLICIES[role]
            try:
                schedule = background_producer.build_schedule(
                    manifest_path=epoch["bundle"]["path"],
                    preprocess_state_root=preprocess_path,
                    ready_high_items=selected_policy["ready_high_items"],
                    ready_low_items=selected_policy["ready_low_items"],
                    ready_high_bytes=selected_policy["ready_high_bytes"],
                    ready_low_bytes=selected_policy["ready_low_bytes"],
                    maximum_dispatch_items_per_run=selected_policy[
                        "maximum_dispatch_items_per_run"
                    ],
                    maximum_dispatch_bytes_per_run=selected_policy[
                        "maximum_dispatch_bytes_per_run"
                    ],
                    maximum_run_seconds=selected_policy["maximum_run_seconds"],
                    free_space_floor_bytes=selected_policy[
                        "free_space_floor_bytes"
                    ],
                )
            except (
                background_producer.BackgroundProducerError,
                queue_runner.QueueRunnerError,
            ) as error:
                raise ScheduleSetMaterializationError(str(error)) from error
            if (
                schedule["policy"] != _expected_schedule_policy(role)
                or schedule["queue"]["manifest_path"]
                != str(epoch["bundle"]["path"])
                or schedule["queue"]["manifest_sha256"]
                != sha256_bytes(epoch["bundle"]["body"])
                or schedule["consumer"]["preprocess_state_root"]
                != str(preprocess_path)
                or schedule["safety"]
                != background_producer.COLD_PRIMARY_SCHEDULE_SAFETY
            ):
                raise ScheduleSetMaterializationError(
                    f"{role} epoch {epoch_row['epoch_ordinal']} producer schedule differs "
                    "from the exact cold-primary bounded-failure policy"
                )
            schedule_body = pretty_bytes(schedule)
            schedule_documents.append((schedule_path, schedule_body, schedule))
            source_epoch = {
                "epoch_ordinal": epoch_row["epoch_ordinal"],
                "epoch_plan_id": epoch["plan"]["plan_id"],
                "epoch_plan_path": str(epoch["plan_path"]),
                "epoch_plan_sha256": sha256_bytes(epoch["plan_body"]),
                "bundle_id": bundle_id,
                "bundle_manifest_path": str(epoch["bundle"]["path"]),
                "bundle_manifest_sha256": sha256_bytes(epoch["bundle"]["body"]),
                "selected_count": epoch_row["selected_count"],
                "selected_estimated_bytes": epoch_row["selected_estimated_bytes"],
                "member_sha256": epoch_row["member_sha256"],
            }
            entries.append(
                {
                    "schedule_ordinal": len(entries) + 1,
                    "role": role,
                    "schedule_path": str(schedule_path),
                    "schedule_sha256": sha256_bytes(schedule_body),
                    "schedule_byte_count": len(schedule_body),
                    "schedule_id": schedule["schedule_id"],
                    "schedule_identity_sha256": schedule["identity_sha256"],
                    "preprocess_state_root": str(preprocess_path),
                    "source_campaign": {
                        "campaign_id": campaign_id,
                        "manifest_path": str(campaign["path"]),
                        "manifest_sha256": sha256_bytes(campaign["body"]),
                    },
                    "source_epoch": source_epoch,
                }
            )
            member_rows = _schedule_member_rows(
                role,
                campaign_id,
                epoch_row["epoch_ordinal"],
                epoch["members"],
            )
            schedule_member_rows.extend(member_rows)
            schedule_epoch_projection.append(
                {
                    "role": role,
                    "campaign_id": campaign_id,
                    "epoch_ordinal": epoch_row["epoch_ordinal"],
                    "epoch_plan_id": epoch["plan"]["plan_id"],
                    "bundle_id": bundle_id,
                    "selected_count": epoch_row["selected_count"],
                    "selected_estimated_bytes": epoch_row[
                        "selected_estimated_bytes"
                    ],
                    "member_sha256": epoch_row["member_sha256"],
                }
            )
            role_count += epoch_row["selected_count"]
            role_bytes += epoch_row["selected_estimated_bytes"]
        role_totals.append(
            {
                "role": role,
                "campaign_count": 1,
                "epoch_count": len(campaign["epochs"]),
                "schedule_count": len(campaign["epochs"]),
                "selected_count": role_count,
                "selected_estimated_bytes": role_bytes,
            }
        )
    if not 1 <= len(entries) <= MAX_SCHEDULES:
        raise ScheduleSetMaterializationError("schedule count exceeds its exact bound")
    if len({row["schedule_path"] for row in entries}) != len(entries):
        raise ScheduleSetMaterializationError("derived schedule paths are not unique")
    if len({row["schedule_id"] for row in entries}) != len(entries):
        raise ScheduleSetMaterializationError("derived schedule IDs are not unique")
    if len({row["preprocess_state_root"] for row in entries}) != len(entries):
        raise ScheduleSetMaterializationError(
            "derived preprocess state roots are not unique"
        )

    selected_count = len(all_member_rows)
    selected_bytes = sum(row["estimated_bytes"] for row in all_member_rows)
    coverage = {
        "source_campaign_count": len(campaigns),
        "source_epoch_count": len(source_epoch_projection),
        "schedule_count": len(entries),
        "source_selected_count": selected_count,
        "source_selected_estimated_bytes": selected_bytes,
        "schedule_selected_count": len(schedule_member_rows),
        "schedule_selected_estimated_bytes": sum(
            row["estimated_bytes"] for row in schedule_member_rows
        ),
        "unique_source_count": len(set(source_ids)),
        "overlap_count": overlap_count,
        "missing_count": len(
            {row["source_id"] for row in all_member_rows}
            - {row["source_id"] for row in schedule_member_rows}
        ),
        "unexpected_count": len(
            {row["source_id"] for row in schedule_member_rows}
            - {row["source_id"] for row in all_member_rows}
        ),
        "source_epoch_union_sha256": sha256_bytes(
            canonical_bytes(source_epoch_projection)
        ),
        "schedule_epoch_union_sha256": sha256_bytes(
            canonical_bytes(schedule_epoch_projection)
        ),
        "source_member_union_sha256": sha256_bytes(
            canonical_bytes(all_member_rows)
        ),
        "schedule_member_union_sha256": sha256_bytes(
            canonical_bytes(schedule_member_rows)
        ),
        "ordered_epoch_union_identical": (
            source_epoch_projection == schedule_epoch_projection
        ),
        "ordered_member_union_identical": all_member_rows == schedule_member_rows,
        "roles_exact_and_ordered": [row["role"] for row in role_totals]
        == list(ROLE_ORDER),
        "source_epochs_contiguous": all(
            [
                epoch["epoch"]["epoch_ordinal"]
                for epoch in campaign["epochs"]
            ]
            == list(range(1, len(campaign["epochs"]) + 1))
            for campaign in campaigns
        ),
        "schedule_ordinals_contiguous": [
            entry["schedule_ordinal"] for entry in entries
        ]
        == list(range(1, len(entries) + 1)),
    }
    if (
        coverage["source_epoch_count"] != coverage["schedule_count"]
        or coverage["source_selected_count"]
        != coverage["schedule_selected_count"]
        or coverage["source_selected_estimated_bytes"]
        != coverage["schedule_selected_estimated_bytes"]
        or coverage["overlap_count"]
        or coverage["missing_count"]
        or coverage["unexpected_count"]
        or not coverage["ordered_epoch_union_identical"]
        or not coverage["ordered_member_union_identical"]
        or not coverage["roles_exact_and_ordered"]
        or not coverage["source_epochs_contiguous"]
        or not coverage["schedule_ordinals_contiguous"]
    ):
        raise ScheduleSetMaterializationError(
            "schedule-set exact union/no-overlap proof failed"
        )

    source_campaigns = [
        _campaign_reference(
            campaign["role"],
            campaign["manifest"],
            campaign["path"],
            campaign["body"],
            _member_digest(campaign["members"]),
        )
        for campaign in campaigns
    ]
    core = {
        "schema_version": SCHEMA_VERSION,
        "schedule_set_kind": SCHEDULE_SET_KIND,
        "materializer": {
            "name": MATERIALIZER_NAME,
            "version": IMPLEMENTATION_VERSION,
        },
        "storage_policy": {
            "media_output_root": str(COLD_MEDIA_ROOT),
            "control_root": str(root),
            "schedules_root": str(schedules_root),
            "preprocess_state_root": str(preprocess_root),
            "schedule_sets_root": str(set_objects_root),
            "storage_mode": "cold_primary_shared_cas_hot_controls",
        },
        "role_order": list(ROLE_ORDER),
        "role_policies": [
            {
                "role": role,
                **ROLE_EPOCH_CAPS[role],
                **_expected_schedule_policy(role),
            }
            for role in ROLE_ORDER
        ],
        "source_campaigns": source_campaigns,
        "schedules": entries,
        "role_totals": role_totals,
        "coverage_proof": coverage,
        "safety": dict(SCHEDULE_SET_SAFETY),
    }
    identity = sha256_bytes(canonical_bytes(core))
    schedule_set_id = f"bgacqscheduleset_{identity[:32]}"
    manifest = {
        "schedule_set_id": schedule_set_id,
        "identity_sha256": identity,
        **core,
    }
    if not SCHEDULE_SET_ID_RE.fullmatch(schedule_set_id):
        raise ScheduleSetMaterializationError("internal schedule-set ID is invalid")
    manifest_body = pretty_bytes(manifest)
    if len(manifest_body) > MAX_SCHEDULE_SET_BYTES:
        raise ScheduleSetMaterializationError("schedule-set manifest exceeds its byte cap")
    manifest_path = set_objects_root / schedule_set_id / "manifest.json"

    for campaign in campaigns:
        for sealed_path, sealed_body, maximum, label in campaign["sealed_inputs"]:
            _replay_file(sealed_path, sealed_body, maximum, label)

    _admit_control_root(root, root_existed)
    for schedule_path, schedule_body, _schedule in schedule_documents:
        try:
            background_producer._write_immutable(
                schedule_path,
                schedule_body,
                "campaign background acquisition schedule",
            )
        except background_producer.BackgroundProducerError as error:
            raise ScheduleSetMaterializationError(str(error)) from error
    for schedule_path, schedule_body, schedule in schedule_documents:
        try:
            loaded, loaded_path, loaded_body = background_producer.load_schedule(
                schedule_path
            )
        except (
            background_producer.BackgroundProducerError,
            queue_runner.QueueRunnerError,
        ) as error:
            raise ScheduleSetMaterializationError(str(error)) from error
        if loaded != schedule or loaded_path != schedule_path or loaded_body != schedule_body:
            raise ScheduleSetMaterializationError(
                "sealed producer schedule differs from its exact materialization"
            )
    for campaign in campaigns:
        for sealed_path, sealed_body, maximum, label in campaign["sealed_inputs"]:
            _replay_file(
                sealed_path,
                sealed_body,
                maximum,
                f"{label} final replay",
            )
    try:
        background_producer._write_immutable(
            manifest_path,
            manifest_body,
            "campaign background schedule-set manifest",
        )
    except background_producer.BackgroundProducerError as error:
        raise ScheduleSetMaterializationError(str(error)) from error
    return manifest, manifest_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Offline-materialize exact per-epoch producer schedules for the two-role "
            "all-known Archive.org campaign"
        )
    )
    parser.add_argument(
        "--normal-processing-campaign-manifest",
        "--normal-campaign-manifest",
        dest="normal_campaign_manifest",
        required=True,
    )
    parser.add_argument(
        "--expected-normal-processing-campaign-manifest-sha256",
        "--expected-normal-campaign-sha256",
        dest="expected_normal_campaign_sha256",
        required=True,
    )
    parser.add_argument(
        "--cold-acquisition-only-requires-chunking-campaign-manifest",
        "--cold-only-campaign-manifest",
        dest="cold_only_campaign_manifest",
        required=True,
    )
    parser.add_argument(
        "--expected-cold-acquisition-only-requires-chunking-campaign-manifest-sha256",
        "--expected-cold-only-campaign-sha256",
        dest="expected_cold_only_campaign_sha256",
        required=True,
    )
    parser.add_argument("--control-root", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        manifest, path = materialize_schedule_set(
            normal_campaign_manifest_path=_absolute_path(
                args.normal_campaign_manifest,
                "--normal-processing-campaign-manifest",
            ),
            expected_normal_campaign_manifest_sha256=args.expected_normal_campaign_sha256,
            cold_only_campaign_manifest_path=_absolute_path(
                args.cold_only_campaign_manifest,
                "--cold-acquisition-only-requires-chunking-campaign-manifest",
            ),
            expected_cold_only_campaign_manifest_sha256=(
                args.expected_cold_only_campaign_sha256
            ),
            control_root=_absolute_path(args.control_root, "--control-root"),
        )
        body = pretty_bytes(manifest)
        sys.stdout.buffer.write(
            pretty_bytes(
                {
                    "schema_version": SCHEMA_VERSION,
                    "status": "materialized",
                    "schedule_set_id": manifest["schedule_set_id"],
                    "schedule_set_manifest_path": str(path),
                    "schedule_set_manifest_sha256": sha256_bytes(body),
                    "schedule_count": manifest["coverage_proof"]["schedule_count"],
                    "selected_count": manifest["coverage_proof"][
                        "source_selected_count"
                    ],
                    "selected_estimated_bytes": manifest["coverage_proof"][
                        "source_selected_estimated_bytes"
                    ],
                    "safety": manifest["safety"],
                }
            )
        )
        return 0
    except (
        ScheduleSetMaterializationError,
        background_producer.BackgroundProducerError,
        queue_runner.QueueRunnerError,
        materialize_queue.MaterializationError,
        OSError,
    ) as error:
        sys.stderr.buffer.write(
            pretty_bytes(
                {
                    "schema_version": SCHEMA_VERSION,
                    "status": "failed",
                    "error": {"type": type(error).__name__, "message": str(error)},
                    "safety": SCHEDULE_SET_SAFETY,
                }
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
