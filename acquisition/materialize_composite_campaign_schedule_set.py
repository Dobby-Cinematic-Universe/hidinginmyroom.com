#!/usr/bin/env python3
"""Offline-compose two sealed Archive campaign schedule sets.

The predecessor and addendum remain independent immutable campaigns.  This module
deeply replays both v1 schedule sets and every campaign, epoch, bundle, work order,
and producer schedule they bind, proves their member identities are disjoint, and
writes one small manifest which references the existing schedule files byte-for-byte.
It performs no discovery, network, media, process, catalogue, or runtime work.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any

if __package__:
    from . import background_producer
    from . import materialize_campaign_schedule_set as source_set
    from . import materialize_queue, queue_runner
else:  # pragma: no cover - direct CLI execution
    import background_producer  # type: ignore[no-redef]
    import materialize_campaign_schedule_set as source_set  # type: ignore[no-redef]
    import materialize_queue  # type: ignore[no-redef]
    import queue_runner  # type: ignore[no-redef]


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
MATERIALIZER_NAME = "himr-archive-composite-schedule-set-materializer"
COMPOSITE_KIND = "sealed_archive_campaign_composite_schedule_set"
COMPOSITE_ID_RE = re.compile(r"^bgacqcompositeset_[0-9a-f]{32}$")

PREDECESSOR = "predecessor"
ADDENDUM = "addendum"
COMPONENT_ORDER = (PREDECESSOR, ADDENDUM)
ROLE_ORDER = source_set.ROLE_ORDER
FLATTEN_ORDER = (
    (PREDECESSOR, source_set.NORMAL_ROLE),
    (ADDENDUM, source_set.NORMAL_ROLE),
    (PREDECESSOR, source_set.COLD_ONLY_ROLE),
    (ADDENDUM, source_set.COLD_ONLY_ROLE),
)

MAX_COMPONENT_BYTES = source_set.MAX_SCHEDULE_SET_BYTES
MAX_COMPOSITE_BYTES = 32 * 1024 * 1024
MAX_COMPOSITE_SCHEDULES = 4_096

SAFETY = {
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
    "source_schedule_files_copied": False,
}


class CompositeScheduleSetError(RuntimeError):
    """A component, union proof, path, or immutable admission failed closed."""


def canonical_bytes(value: Any) -> bytes:
    return materialize_queue.canonical_bytes(value)


def pretty_bytes(value: Any) -> bytes:
    return materialize_queue.pretty_bytes(value)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _write_immutable_manifest(path: Path, body: bytes) -> None:
    """Atomically admit one immutable manifest without a persistent sidecar.

    The fresh composite control tree is an output contract, so locking the
    destination directory itself keeps concurrent admission safe while leaving
    exactly the manifest behind.  A same-byte replay is an idempotent success.
    """

    label = "composite campaign schedule-set manifest"
    try:
        parent = background_producer._ensure_private_parent(path)
    except background_producer.BackgroundProducerError as error:
        raise CompositeScheduleSetError(str(error)) from error
    directory_descriptor = os.open(
        parent,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
    )
    temporary: Path | None = None
    descriptor = -1
    try:
        try:
            fcntl.flock(directory_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise CompositeScheduleSetError(
                "another composite schedule-set materializer holds the output lock"
            ) from error
        if path.exists() or path.is_symlink():
            try:
                existing, _ = queue_runner._stable_read(
                    path,
                    maximum=max(len(body), 1),
                    label=label,
                    required_mode=0o400,
                )
            except queue_runner.QueueRunnerError as error:
                raise CompositeScheduleSetError(str(error)) from error
            if existing != body:
                raise CompositeScheduleSetError(
                    f"existing {label} differs from exact replay"
                )
            return
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.tmp-", dir=parent
        )
        temporary = Path(temporary_name)
        offset = 0
        while offset < len(body):
            offset += os.write(descriptor, body[offset:])
        os.fchmod(descriptor, 0o400)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, path)
        except FileExistsError as error:
            raise CompositeScheduleSetError(
                f"immutable {label} admission raced"
            ) from error
        os.fsync(directory_descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        fcntl.flock(directory_descriptor, fcntl.LOCK_UN)
        os.close(directory_descriptor)


def _fail_from_source(error: Exception) -> CompositeScheduleSetError:
    return CompositeScheduleSetError(str(error))


def _exact(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CompositeScheduleSetError(f"{label} must be an object")
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    if missing or unknown:
        raise CompositeScheduleSetError(
            f"{label} keys differ from the exact contract; "
            f"missing={missing}, unknown={unknown}"
        )
    return value


def _integer(value: Any, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise CompositeScheduleSetError(
            f"{label} must be an integer greater than or equal to {minimum}"
        )
    return value


def _absolute_path(value: Any, label: str) -> Path:
    try:
        return source_set._absolute_path(value, label)
    except source_set.ScheduleSetMaterializationError as error:
        raise _fail_from_source(error) from error


def _sha256(value: Any, label: str) -> str:
    try:
        return source_set._sha256(value, label)
    except source_set.ScheduleSetMaterializationError as error:
        raise _fail_from_source(error) from error


def _stable_json(
    path_value: Any, expected_sha256: Any, *, maximum: int, label: str
) -> tuple[dict[str, Any], bytes, Path]:
    try:
        return source_set._stable_json(
            path_value,
            expected_sha256,
            maximum=maximum,
            label=label,
        )
    except source_set.ScheduleSetMaterializationError as error:
        raise _fail_from_source(error) from error


def _replay(path: Path, body: bytes, maximum: int, label: str) -> None:
    try:
        source_set._replay_file(path, body, maximum, label)
    except source_set.ScheduleSetMaterializationError as error:
        raise _fail_from_source(error) from error


def _member_rows(
    component: str,
    role: str,
    campaign_id: str,
    epoch_ordinal: int,
    members: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "component": component,
            "role": role,
            "campaign_id": campaign_id,
            "epoch_ordinal": epoch_ordinal,
            "local_queue_ordinal": local_ordinal,
            **member,
        }
        for local_ordinal, member in enumerate(members, 1)
    ]


def _schedule_projection(component: str, entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "component": component,
        "component_schedule_ordinal": entry["schedule_ordinal"],
        "role": entry["role"],
        "schedule_id": entry["schedule_id"],
        "schedule_path": entry["schedule_path"],
        "schedule_sha256": entry["schedule_sha256"],
        "schedule_byte_count": entry["schedule_byte_count"],
    }


def _expected_component_coverage(
    campaigns: list[dict[str, Any]],
    schedule_member_rows: list[dict[str, Any]],
    schedule_epoch_projection: list[dict[str, Any]],
) -> dict[str, Any]:
    all_member_rows: list[dict[str, Any]] = []
    source_epoch_projection: list[dict[str, Any]] = []
    for campaign in campaigns:
        role = campaign["role"]
        campaign_id = campaign["manifest"]["campaign_id"]
        for epoch in campaign["epochs"]:
            epoch_row = epoch["epoch"]
            all_member_rows.extend(
                source_set._schedule_member_rows(
                    role,
                    campaign_id,
                    epoch_row["epoch_ordinal"],
                    epoch["members"],
                )
            )
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
    source_ids = [row["source_id"] for row in all_member_rows]
    return {
        "source_campaign_count": 2,
        "source_epoch_count": len(source_epoch_projection),
        "schedule_count": len(schedule_epoch_projection),
        "source_selected_count": len(all_member_rows),
        "source_selected_estimated_bytes": sum(
            row["estimated_bytes"] for row in all_member_rows
        ),
        "schedule_selected_count": len(schedule_member_rows),
        "schedule_selected_estimated_bytes": sum(
            row["estimated_bytes"] for row in schedule_member_rows
        ),
        "unique_source_count": len(set(source_ids)),
        "overlap_count": len(source_ids) - len(set(source_ids)),
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
        "roles_exact_and_ordered": [row["role"] for row in campaigns]
        == list(ROLE_ORDER),
        "source_epochs_contiguous": all(
            [epoch["epoch"]["epoch_ordinal"] for epoch in campaign["epochs"]]
            == list(range(1, len(campaign["epochs"]) + 1))
            for campaign in campaigns
        ),
        "schedule_ordinals_contiguous": True,
    }


def _validate_component(
    component: str,
    manifest_path: Path,
    expected_manifest_sha256: str,
) -> dict[str, Any]:
    """Deeply replay one canonical v1 source schedule set."""

    if component not in COMPONENT_ORDER:
        raise CompositeScheduleSetError(f"unsupported component: {component}")
    raw, body, path = _stable_json(
        str(manifest_path),
        expected_manifest_sha256,
        maximum=MAX_COMPONENT_BYTES,
        label=f"{component} schedule-set manifest",
    )
    manifest = _exact(
        raw,
        f"{component} schedule-set manifest",
        {
            "schedule_set_id",
            "identity_sha256",
            "schema_version",
            "schedule_set_kind",
            "materializer",
            "storage_policy",
            "role_order",
            "role_policies",
            "source_campaigns",
            "schedules",
            "role_totals",
            "coverage_proof",
            "safety",
        },
    )
    if (
        manifest["schema_version"] != source_set.SCHEMA_VERSION
        or manifest["schedule_set_kind"] != source_set.SCHEDULE_SET_KIND
        or manifest["materializer"]
        != {
            "name": source_set.MATERIALIZER_NAME,
            "version": source_set.IMPLEMENTATION_VERSION,
        }
        or manifest["role_order"] != list(ROLE_ORDER)
        or manifest["safety"] != source_set.SCHEDULE_SET_SAFETY
    ):
        raise CompositeScheduleSetError(
            f"{component} schedule set is not the exact supported offline v1 contract"
        )
    core = {
        key: value
        for key, value in manifest.items()
        if key not in {"schedule_set_id", "identity_sha256"}
    }
    identity = sha256_bytes(canonical_bytes(core))
    expected_id = f"bgacqscheduleset_{identity[:32]}"
    if (
        manifest["identity_sha256"] != identity
        or manifest["schedule_set_id"] != expected_id
        or not source_set.SCHEDULE_SET_ID_RE.fullmatch(expected_id)
        or body != pretty_bytes(manifest)
    ):
        raise CompositeScheduleSetError(
            f"{component} schedule-set identity or serialization is inconsistent"
        )

    storage = _exact(
        manifest["storage_policy"],
        f"{component} storage policy",
        {
            "media_output_root",
            "control_root",
            "schedules_root",
            "preprocess_state_root",
            "schedule_sets_root",
            "storage_mode",
        },
    )
    control_root = _absolute_path(storage["control_root"], f"{component} control root")
    if storage != {
        "media_output_root": str(source_set.COLD_MEDIA_ROOT),
        "control_root": str(control_root),
        "schedules_root": str(control_root / "schedules"),
        "preprocess_state_root": str(control_root / "preprocess-state"),
        "schedule_sets_root": str(control_root / "schedule-sets"),
        "storage_mode": "cold_primary_shared_cas_hot_controls",
    }:
        raise CompositeScheduleSetError(
            f"{component} schedule-set storage policy is inconsistent"
        )
    expected_policies = [
        {
            "role": role,
            **source_set.ROLE_EPOCH_CAPS[role],
            **source_set._expected_schedule_policy(role),
        }
        for role in ROLE_ORDER
    ]
    if manifest["role_policies"] != expected_policies:
        raise CompositeScheduleSetError(
            f"{component} schedule-set role policies differ from v1"
        )

    campaign_refs = manifest["source_campaigns"]
    if not isinstance(campaign_refs, list) or len(campaign_refs) != 2:
        raise CompositeScheduleSetError(
            f"{component} schedule set must bind exactly two role campaigns"
        )
    campaigns: list[dict[str, Any]] = []
    for role, reference in zip(ROLE_ORDER, campaign_refs, strict=True):
        row = _exact(
            reference,
            f"{component} {role} source campaign",
            {
                "role",
                "campaign_id",
                "manifest_path",
                "manifest_sha256",
                "manifest_byte_count",
                "epoch_count",
                "selected_count",
                "selected_estimated_bytes",
                "ordered_member_sha256",
            },
        )
        if row["role"] != role:
            raise CompositeScheduleSetError(
                f"{component} source campaigns are not role ordered"
            )
        try:
            campaign = source_set._validate_campaign(
                role,
                _absolute_path(row["manifest_path"], f"{component} campaign path"),
                _sha256(row["manifest_sha256"], f"{component} campaign SHA-256"),
            )
        except source_set.ScheduleSetMaterializationError as error:
            raise _fail_from_source(error) from error
        expected_reference = source_set._campaign_reference(
            role,
            campaign["manifest"],
            campaign["path"],
            campaign["body"],
            source_set._member_digest(campaign["members"]),
        )
        if row != expected_reference:
            raise CompositeScheduleSetError(
                f"{component} {role} source campaign reference differs from replay"
            )
        campaigns.append(campaign)
    if len({row["manifest"]["campaign_id"] for row in campaigns}) != 2:
        raise CompositeScheduleSetError(
            f"{component} role campaigns must be distinct"
        )

    raw_entries = manifest["schedules"]
    expected_count = sum(len(campaign["epochs"]) for campaign in campaigns)
    if (
        not isinstance(raw_entries, list)
        or len(raw_entries) != expected_count
        or not 2 <= len(raw_entries) <= source_set.MAX_SCHEDULES
    ):
        raise CompositeScheduleSetError(
            f"{component} schedule count differs from campaign replay"
        )
    schedules: list[dict[str, Any]] = []
    schedule_members: list[dict[str, Any]] = []
    schedule_epochs: list[dict[str, Any]] = []
    role_totals: list[dict[str, Any]] = []
    index = 0
    for campaign in campaigns:
        role = campaign["role"]
        campaign_id = campaign["manifest"]["campaign_id"]
        role_count = 0
        role_bytes = 0
        for epoch in campaign["epochs"]:
            index += 1
            entry = _exact(
                raw_entries[index - 1],
                f"{component} schedule {index}",
                {
                    "schedule_ordinal",
                    "role",
                    "schedule_path",
                    "schedule_sha256",
                    "schedule_byte_count",
                    "schedule_id",
                    "schedule_identity_sha256",
                    "preprocess_state_root",
                    "source_campaign",
                    "source_epoch",
                },
            )
            epoch_row = epoch["epoch"]
            bundle_id = epoch["bundle"]["manifest"]["bundle_id"]
            leaf = f"epoch-{epoch_row['epoch_ordinal']:06d}-{bundle_id}"
            schedule_path = control_root / "schedules" / role / leaf / "schedule.json"
            preprocess_path = control_root / "preprocess-state" / role / leaf
            try:
                schedule, loaded_path, schedule_body = background_producer.load_schedule(
                    schedule_path
                )
            except (
                background_producer.BackgroundProducerError,
                queue_runner.QueueRunnerError,
            ) as error:
                raise CompositeScheduleSetError(str(error)) from error
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
            expected_entry = {
                "schedule_ordinal": index,
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
            if (
                entry != expected_entry
                or loaded_path != schedule_path
                or schedule["policy"] != source_set._expected_schedule_policy(role)
                or schedule["queue"]["manifest_path"]
                != str(epoch["bundle"]["path"])
                or schedule["queue"]["manifest_sha256"]
                != sha256_bytes(epoch["bundle"]["body"])
                or schedule["consumer"]["preprocess_state_root"]
                != str(preprocess_path)
                or schedule["safety"]
                != background_producer.COLD_PRIMARY_SCHEDULE_SAFETY
            ):
                raise CompositeScheduleSetError(
                    f"{component} schedule {index} differs from its campaign binding"
                )
            members = source_set._schedule_member_rows(
                role,
                campaign_id,
                epoch_row["epoch_ordinal"],
                epoch["members"],
            )
            schedule_members.extend(members)
            schedule_epoch = {
                "role": role,
                "campaign_id": campaign_id,
                "epoch_ordinal": epoch_row["epoch_ordinal"],
                "epoch_plan_id": epoch["plan"]["plan_id"],
                "bundle_id": bundle_id,
                "selected_count": epoch_row["selected_count"],
                "selected_estimated_bytes": epoch_row["selected_estimated_bytes"],
                "member_sha256": epoch_row["member_sha256"],
            }
            schedule_epochs.append(schedule_epoch)
            schedules.append(
                {
                    "entry": entry,
                    "body": schedule_body,
                    "path": schedule_path,
                    "schedule": schedule,
                    "members": epoch["members"],
                    "campaign": campaign,
                    "epoch": epoch,
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
    if manifest["role_totals"] != role_totals:
        raise CompositeScheduleSetError(
            f"{component} role totals differ from deep replay"
        )
    expected_coverage = _expected_component_coverage(
        campaigns, schedule_members, schedule_epochs
    )
    if manifest["coverage_proof"] != expected_coverage:
        raise CompositeScheduleSetError(
            f"{component} coverage proof differs from deep replay"
        )
    if (
        expected_coverage["overlap_count"]
        or expected_coverage["missing_count"]
        or expected_coverage["unexpected_count"]
        or not expected_coverage["ordered_epoch_union_identical"]
        or not expected_coverage["ordered_member_union_identical"]
    ):
        raise CompositeScheduleSetError(
            f"{component} member union proof is not exact"
        )
    sealed_inputs: list[tuple[Path, bytes, int, str]] = [
        (path, body, MAX_COMPONENT_BYTES, f"{component} schedule-set manifest")
    ]
    for campaign in campaigns:
        sealed_inputs.extend(campaign["sealed_inputs"])
    sealed_inputs.extend(
        (
            schedule["path"],
            schedule["body"],
            background_producer.MAX_SCHEDULE_BYTES,
            f"{component} source schedule {schedule['entry']['schedule_ordinal']}",
        )
        for schedule in schedules
    )
    return {
        "component": component,
        "manifest": manifest,
        "path": path,
        "body": body,
        "control_root": control_root,
        "campaigns": campaigns,
        "schedules": schedules,
        "sealed_inputs": sealed_inputs,
    }


def _component_reference(component: dict[str, Any]) -> dict[str, Any]:
    schedules = [row["entry"] for row in component["schedules"]]
    members: list[dict[str, Any]] = []
    for row in component["schedules"]:
        entry = row["entry"]
        members.extend(
            _member_rows(
                component["component"],
                entry["role"],
                entry["source_campaign"]["campaign_id"],
                entry["source_epoch"]["epoch_ordinal"],
                row["members"],
            )
        )
    coverage = component["manifest"]["coverage_proof"]
    return {
        "component_ordinal": COMPONENT_ORDER.index(component["component"]) + 1,
        "component": component["component"],
        "schedule_set_id": component["manifest"]["schedule_set_id"],
        "manifest_path": str(component["path"]),
        "manifest_sha256": sha256_bytes(component["body"]),
        "manifest_byte_count": len(component["body"]),
        "campaign_count": coverage["source_campaign_count"],
        "epoch_count": coverage["source_epoch_count"],
        "schedule_count": coverage["schedule_count"],
        "selected_count": coverage["source_selected_count"],
        "selected_estimated_bytes": coverage[
            "source_selected_estimated_bytes"
        ],
        "ordered_schedule_reference_sha256": sha256_bytes(
            canonical_bytes(
                [_schedule_projection(component["component"], row) for row in schedules]
            )
        ),
        "ordered_member_sha256": sha256_bytes(canonical_bytes(members)),
    }


def materialize_composite_schedule_set(
    *,
    predecessor_schedule_set_manifest_path: Path,
    expected_predecessor_schedule_set_manifest_sha256: str,
    addendum_schedule_set_manifest_path: Path,
    expected_addendum_schedule_set_manifest_sha256: str,
    control_root: Path,
) -> tuple[dict[str, Any], Path]:
    """Replay two schedule sets and admit one immutable reference-only composite."""

    try:
        root, root_existed = source_set._validate_control_root(str(control_root))
    except source_set.ScheduleSetMaterializationError as error:
        raise _fail_from_source(error) from error
    components = [
        _validate_component(
            PREDECESSOR,
            predecessor_schedule_set_manifest_path,
            expected_predecessor_schedule_set_manifest_sha256,
        ),
        _validate_component(
            ADDENDUM,
            addendum_schedule_set_manifest_path,
            expected_addendum_schedule_set_manifest_sha256,
        ),
    ]
    if [row["component"] for row in components] != list(COMPONENT_ORDER):
        raise CompositeScheduleSetError("component order is inconsistent")
    if (
        len({row["manifest"]["schedule_set_id"] for row in components}) != 2
        or len({row["path"] for row in components}) != 2
    ):
        raise CompositeScheduleSetError(
            "predecessor and addendum must be distinct sealed schedule sets"
        )
    for component in components:
        component_root = component["control_root"]
        if source_set._is_within(root, component_root) or source_set._is_within(
            component_root, root
        ):
            raise CompositeScheduleSetError(
                "composite control root must be disjoint from component control roots"
            )

    by_name = {row["component"]: row for row in components}
    flattened: list[dict[str, Any]] = []
    source_schedule_projection: list[dict[str, Any]] = []
    source_member_rows: list[dict[str, Any]] = []
    component_role_totals: list[dict[str, Any]] = []
    for component_name, role in FLATTEN_ORDER:
        component = by_name[component_name]
        role_schedules = [
            row for row in component["schedules"] if row["entry"]["role"] == role
        ]
        if not role_schedules:
            raise CompositeScheduleSetError(
                f"{component_name} component has no {role} schedules"
            )
        role_members: list[dict[str, Any]] = []
        for row in role_schedules:
            entry = row["entry"]
            projection = _schedule_projection(component_name, entry)
            source_schedule_projection.append(projection)
            member_rows = _member_rows(
                component_name,
                role,
                entry["source_campaign"]["campaign_id"],
                entry["source_epoch"]["epoch_ordinal"],
                row["members"],
            )
            source_member_rows.extend(member_rows)
            role_members.extend(member_rows)
            flattened.append(
                {
                    "schedule_ordinal": len(flattened) + 1,
                    "component": component_name,
                    "component_schedule_ordinal": entry["schedule_ordinal"],
                    "role": role,
                    "schedule_path": entry["schedule_path"],
                    "schedule_sha256": entry["schedule_sha256"],
                    "schedule_byte_count": entry["schedule_byte_count"],
                    "schedule_id": entry["schedule_id"],
                    "schedule_identity_sha256": entry[
                        "schedule_identity_sha256"
                    ],
                    "preprocess_state_root": entry["preprocess_state_root"],
                    "source_schedule_set": {
                        "schedule_set_id": component["manifest"]["schedule_set_id"],
                        "manifest_path": str(component["path"]),
                        "manifest_sha256": sha256_bytes(component["body"]),
                    },
                    "source_campaign": entry["source_campaign"],
                    "source_epoch": entry["source_epoch"],
                }
            )
        component_role_totals.append(
            {
                "component": component_name,
                "role": role,
                "campaign_count": 1,
                "epoch_count": len(role_schedules),
                "schedule_count": len(role_schedules),
                "selected_count": len(role_members),
                "selected_estimated_bytes": sum(
                    row["estimated_bytes"] for row in role_members
                ),
                "ordered_schedule_reference_sha256": sha256_bytes(
                    canonical_bytes(
                        [
                            _schedule_projection(component_name, row["entry"])
                            for row in role_schedules
                        ]
                    )
                ),
                "ordered_member_sha256": sha256_bytes(
                    canonical_bytes(role_members)
                ),
            }
        )
    if not 4 <= len(flattened) <= MAX_COMPOSITE_SCHEDULES:
        raise CompositeScheduleSetError("composite schedule count exceeds its bound")
    if (
        len({row["schedule_id"] for row in flattened}) != len(flattened)
        or len({row["schedule_path"] for row in flattened}) != len(flattened)
        or len({row["preprocess_state_root"] for row in flattened})
        != len(flattened)
    ):
        raise CompositeScheduleSetError(
            "component schedule identities or paths overlap"
        )

    source_ids = [row["source_id"] for row in source_member_rows]
    recording_ids = [row["recording_id"] for row in source_member_rows]
    native_keys = [
        (row["platform"], row["native_id"]) for row in source_member_rows
    ]
    source_overlap = len(source_ids) - len(set(source_ids))
    recording_overlap = len(recording_ids) - len(set(recording_ids))
    native_overlap = len(native_keys) - len(set(native_keys))
    if source_overlap or recording_overlap or native_overlap:
        raise CompositeScheduleSetError(
            "predecessor and addendum source, recording, or native identities overlap"
        )

    composite_schedule_projection = [
        {
            "component": row["component"],
            "component_schedule_ordinal": row["component_schedule_ordinal"],
            "role": row["role"],
            "schedule_id": row["schedule_id"],
            "schedule_path": row["schedule_path"],
            "schedule_sha256": row["schedule_sha256"],
            "schedule_byte_count": row["schedule_byte_count"],
        }
        for row in flattened
    ]
    composite_member_rows: list[dict[str, Any]] = []
    schedule_lookup = {
        (component["component"], row["entry"]["schedule_ordinal"]): row
        for component in components
        for row in component["schedules"]
    }
    for entry in flattened:
        source = schedule_lookup[
            (entry["component"], entry["component_schedule_ordinal"])
        ]
        composite_member_rows.extend(
            _member_rows(
                entry["component"],
                entry["role"],
                entry["source_campaign"]["campaign_id"],
                entry["source_epoch"]["epoch_ordinal"],
                source["members"],
            )
        )
    if source_member_rows != composite_member_rows:
        raise CompositeScheduleSetError(
            "composite member ordering differs from flattened source ordering"
        )

    role_totals: list[dict[str, Any]] = []
    for role in ROLE_ORDER:
        selected = [row for row in flattened if row["role"] == role]
        selected_members = [row for row in source_member_rows if row["role"] == role]
        role_totals.append(
            {
                "role": role,
                "component_count": 2,
                "campaign_count": 2,
                "epoch_count": len(selected),
                "schedule_count": len(selected),
                "selected_count": len(selected_members),
                "selected_estimated_bytes": sum(
                    row["estimated_bytes"] for row in selected_members
                ),
            }
        )

    coverage = {
        "component_count": 2,
        "source_schedule_set_count": 2,
        "source_campaign_count": 4,
        "source_epoch_count": len(flattened),
        "schedule_count": len(flattened),
        "source_selected_count": len(source_member_rows),
        "source_selected_estimated_bytes": sum(
            row["estimated_bytes"] for row in source_member_rows
        ),
        "schedule_selected_count": len(composite_member_rows),
        "schedule_selected_estimated_bytes": sum(
            row["estimated_bytes"] for row in composite_member_rows
        ),
        "unique_source_count": len(set(source_ids)),
        "unique_recording_count": len(set(recording_ids)),
        "unique_native_count": len(set(native_keys)),
        "source_overlap_count": source_overlap,
        "recording_overlap_count": recording_overlap,
        "native_overlap_count": native_overlap,
        "missing_count": len(
            {row["source_id"] for row in source_member_rows}
            - {row["source_id"] for row in composite_member_rows}
        ),
        "unexpected_count": len(
            {row["source_id"] for row in composite_member_rows}
            - {row["source_id"] for row in source_member_rows}
        ),
        "source_schedule_union_sha256": sha256_bytes(
            canonical_bytes(source_schedule_projection)
        ),
        "composite_schedule_union_sha256": sha256_bytes(
            canonical_bytes(composite_schedule_projection)
        ),
        "source_member_union_sha256": sha256_bytes(
            canonical_bytes(source_member_rows)
        ),
        "composite_member_union_sha256": sha256_bytes(
            canonical_bytes(composite_member_rows)
        ),
        "ordered_schedule_union_identical": (
            source_schedule_projection == composite_schedule_projection
        ),
        "ordered_member_union_identical": (
            source_member_rows == composite_member_rows
        ),
        "component_order_exact": [row["component"] for row in components]
        == list(COMPONENT_ORDER),
        "flatten_order_exact": [
            (row["component"], row["role"]) for row in component_role_totals
        ]
        == list(FLATTEN_ORDER),
        "schedule_ordinals_contiguous": [
            row["schedule_ordinal"] for row in flattened
        ]
        == list(range(1, len(flattened) + 1)),
        "source_schedule_files_referenced_not_copied": True,
    }
    if (
        coverage["source_epoch_count"] != coverage["schedule_count"]
        or coverage["source_selected_count"]
        != coverage["schedule_selected_count"]
        or coverage["source_selected_estimated_bytes"]
        != coverage["schedule_selected_estimated_bytes"]
        or source_overlap
        or recording_overlap
        or native_overlap
        or coverage["missing_count"]
        or coverage["unexpected_count"]
        or not coverage["ordered_schedule_union_identical"]
        or not coverage["ordered_member_union_identical"]
        or not coverage["component_order_exact"]
        or not coverage["flatten_order_exact"]
        or not coverage["schedule_ordinals_contiguous"]
    ):
        raise CompositeScheduleSetError("composite exact union proof failed")

    objects_root = root / "composite-schedule-sets"
    core = {
        "schema_version": SCHEMA_VERSION,
        "composite_kind": COMPOSITE_KIND,
        "materializer": {
            "name": MATERIALIZER_NAME,
            "version": IMPLEMENTATION_VERSION,
        },
        "storage_policy": {
            "media_output_root": str(source_set.COLD_MEDIA_ROOT),
            "control_root": str(root),
            "composite_schedule_sets_root": str(objects_root),
            "source_schedule_storage": "referenced_in_place_byte_for_byte",
        },
        "component_order": list(COMPONENT_ORDER),
        "role_order": list(ROLE_ORDER),
        "flatten_order": [
            {"component": component, "role": role}
            for component, role in FLATTEN_ORDER
        ],
        "components": [_component_reference(row) for row in components],
        "schedules": flattened,
        "component_role_totals": component_role_totals,
        "role_totals": role_totals,
        "coverage_proof": coverage,
        "safety": dict(SAFETY),
    }
    identity = sha256_bytes(canonical_bytes(core))
    composite_id = f"bgacqcompositeset_{identity[:32]}"
    manifest = {
        "composite_schedule_set_id": composite_id,
        "identity_sha256": identity,
        **core,
    }
    if not COMPOSITE_ID_RE.fullmatch(composite_id):
        raise CompositeScheduleSetError("internal composite ID is invalid")
    manifest_body = pretty_bytes(manifest)
    if len(manifest_body) > MAX_COMPOSITE_BYTES:
        raise CompositeScheduleSetError("composite manifest exceeds its byte cap")
    manifest_path = objects_root / composite_id / "manifest.json"

    for component in components:
        for sealed_path, sealed_body, maximum, label in component["sealed_inputs"]:
            _replay(sealed_path, sealed_body, maximum, label)
    try:
        source_set._admit_control_root(root, root_existed)
        _write_immutable_manifest(manifest_path, manifest_body)
    except (
        source_set.ScheduleSetMaterializationError,
        background_producer.BackgroundProducerError,
    ) as error:
        raise CompositeScheduleSetError(str(error)) from error
    for component in components:
        for sealed_path, sealed_body, maximum, label in component["sealed_inputs"]:
            _replay(sealed_path, sealed_body, maximum, f"{label} final replay")
    loaded, loaded_body, loaded_path = _stable_json(
        str(manifest_path),
        sha256_bytes(manifest_body),
        maximum=MAX_COMPOSITE_BYTES,
        label="composite schedule-set manifest",
    )
    if loaded != manifest or loaded_body != manifest_body or loaded_path != manifest_path:
        raise CompositeScheduleSetError(
            "admitted composite manifest differs from exact materialization"
        )
    return manifest, manifest_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Offline-compose one sealed predecessor and one sealed addendum "
            "Archive schedule set without copying or running their schedules"
        )
    )
    parser.add_argument("--predecessor-schedule-set-manifest", required=True)
    parser.add_argument(
        "--expected-predecessor-schedule-set-manifest-sha256", required=True
    )
    parser.add_argument("--addendum-schedule-set-manifest", required=True)
    parser.add_argument(
        "--expected-addendum-schedule-set-manifest-sha256", required=True
    )
    parser.add_argument("--control-root", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        manifest, path = materialize_composite_schedule_set(
            predecessor_schedule_set_manifest_path=_absolute_path(
                args.predecessor_schedule_set_manifest,
                "--predecessor-schedule-set-manifest",
            ),
            expected_predecessor_schedule_set_manifest_sha256=(
                args.expected_predecessor_schedule_set_manifest_sha256
            ),
            addendum_schedule_set_manifest_path=_absolute_path(
                args.addendum_schedule_set_manifest,
                "--addendum-schedule-set-manifest",
            ),
            expected_addendum_schedule_set_manifest_sha256=(
                args.expected_addendum_schedule_set_manifest_sha256
            ),
            control_root=_absolute_path(args.control_root, "--control-root"),
        )
        body = pretty_bytes(manifest)
        sys.stdout.buffer.write(
            pretty_bytes(
                {
                    "schema_version": SCHEMA_VERSION,
                    "status": "materialized",
                    "composite_schedule_set_id": manifest[
                        "composite_schedule_set_id"
                    ],
                    "composite_manifest_path": str(path),
                    "composite_manifest_sha256": sha256_bytes(body),
                    "component_count": manifest["coverage_proof"][
                        "component_count"
                    ],
                    "schedule_count": manifest["coverage_proof"][
                        "schedule_count"
                    ],
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
        CompositeScheduleSetError,
        source_set.ScheduleSetMaterializationError,
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
                    "safety": SAFETY,
                }
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
