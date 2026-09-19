"""Plan or seal fresh controller state for an exact incremental Archive inventory.

This setup-only command reads bounded control metadata. It never replays the
template campaign, inspects acquired media, starts services, or copies state.
The caller must separately review and validate the new campaign before Start.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
from pathlib import Path
import re
import stat
import tempfile
from typing import Any

from .config import ConfigError, build_config, canonical_bytes, normalize_config, sha256_bytes


MAX_METADATA_BYTES = 8 * 1024 * 1024
MAX_NEW_ITEMS = 1024
INVENTORY_SCOPE = "exact_public_archive_item_incremental_inventory_v1"
SCHEDULE_KIND = "sealed_archive_campaign_background_schedule_set"
NORMAL = "normal_processing"
COLD = "cold_acquisition_only_requires_chunking"
ROOT_NAMES = ("state", "preprocess-control", "preprocess-output", "gpu-queues",
              "gpu-work-orders", "gpu-materializations", "gpu-results", "gpu-batches",
              "gpu-events", "gpu-locks", "cold-staging", "cold-receipts")
GPU_ROOT_FIELDS = {"queue_root": "gpu-queues", "work_order_root": "gpu-work-orders",
                   "receipt_root": "gpu-materializations", "result_root": "gpu-results",
                   "batch_root": "gpu-batches", "event_root": "gpu-events", "lock_root": "gpu-locks"}


class SetupError(RuntimeError):
    """Setup metadata or prospective fresh paths are not safe."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SetupError(message)


def _path(value: str | Path) -> Path:
    raw = str(value)
    path = Path(raw)
    _require(path.is_absolute() and raw != "/" and str(path) == raw
             and os.path.normpath(raw) == raw and "//" not in raw and "\\" not in raw
             and not any(ord(character) < 32 for character in raw), "path is not normalized and absolute")
    return path


def _fingerprint(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            info.st_ctime_ns, info.st_mode, info.st_uid, info.st_gid, info.st_nlink)


def _read(path_value, expected_sha256, *, mode=0o400) -> bytes:
    path = _path(path_value)
    _require(isinstance(expected_sha256, str) and re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is not None,
             "invalid expected SHA-256")
    _require(path.resolve(strict=True) == path, "metadata path traverses a symlink")
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        _require(stat.S_ISREG(before.st_mode) and before.st_uid == os.geteuid()
                 and before.st_nlink == 1 and stat.S_IMODE(before.st_mode) == mode
                 and 0 < before.st_size <= MAX_METADATA_BYTES, "metadata file is not private, sealed, or bounded")
        body = bytearray()
        while len(body) < before.st_size:
            block = os.read(descriptor, min(1024 * 1024, before.st_size - len(body)))
            _require(bool(block), "metadata truncated during read")
            body.extend(block)
        _require(_fingerprint(before) == _fingerprint(os.fstat(descriptor)) == _fingerprint(path.lstat()),
                 "metadata changed during read")
        _require(sha256_bytes(bytes(body)) == expected_sha256, "metadata SHA-256 differs")
        return bytes(body)
    finally:
        os.close(descriptor)


def _json(path, digest):
    def pairs(rows):
        result = {}
        for key, value in rows:
            _require(key not in result, "duplicate metadata key")
            result[key] = value
        return result
    value = json.loads(_read(path, digest), object_pairs_hook=pairs,
                       parse_constant=lambda _value: (_ for _ in ()).throw(SetupError("nonfinite metadata number")))
    _require(isinstance(value, dict), "metadata must contain one object")
    return value


def _private_parent(path: Path) -> None:
    info = path.lstat()
    _require(path.resolve(strict=True) == path and stat.S_ISDIR(info.st_mode)
             and info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700,
             "setup parent must be current-user-owned mode 0700 without symlinks")


def _intersects(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def _old_writable_roots(config):
    return [_path(config["state_root"]),
            *[_path(config["gpu_readiness"][key]) for key in GPU_ROOT_FIELDS],
            _path(config["gpu_readiness"]["child_journal_root"]),
            *[_path(config["preprocess"][key]) for key in ("bundle_root", "processing_output_root")],
            *[_path(config["cold_retention"][key]) for key in ("staging_root", "receipt_root")]]


def _fresh_paths(template, operational_root, output, source_config_path):
    operational_root, output = _path(operational_root), _path(output)
    _private_parent(operational_root.parent)
    _private_parent(output.parent)
    _require(not operational_root.exists() and not operational_root.is_symlink(), "operational root must be new; state is never copied or reused")
    _require(not output.exists() and not output.is_symlink(), "config output already exists")
    _require(not _intersects(operational_root, output), "config output must be outside the writable operational root")
    for old in _old_writable_roots(template):
        _require(not _intersects(operational_root, old) and not _intersects(output, old),
                 "new paths intersect original operational state")
    _require(output != _path(source_config_path), "cannot replace the template config")
    return operational_root, output


def _check_inventory(inventory, normal_count, cold_count):
    _require(inventory.get("kind") == "himr_known_archive_collection_inventory"
             and type(inventory.get("schema_version")) is int and inventory["schema_version"] == 1
             and inventory.get("scope") == INVENTORY_SCOPE, "inventory is not an exact incremental inventory")
    totals = inventory.get("totals", {})
    expected = {"candidate_count": normal_count + cold_count,
                "ready_selected_count": normal_count, "parked_requires_chunking_count": cold_count}
    _require(all(type(totals.get(key)) is int and totals[key] == count for key, count in expected.items()),
             "inventory counts differ from explicit requested counts")
    collections = inventory.get("collections")
    _require(isinstance(collections, list) and 1 <= len(collections) <= MAX_NEW_ITEMS, "inventory collections are absent or oversized")
    identifiers = [row.get("identifier") for row in collections if isinstance(row, dict)]
    _require(len(identifiers) == len(collections) and all(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", value) for value in identifiers)
             and len(set(identifiers)) == len(identifiers), "inventory collection identifiers are invalid or duplicated")
    for key, count in expected.items():
        _require(all(type(row.get(key)) is int and row[key] >= 0 for row in collections)
                 and sum(row[key] for row in collections) == count, "inventory collection totals differ")
    return set(identifiers)


def _check_schedules(schedule_set, *, campaign_root, identifiers, normal_count, cold_count):
    _require(schedule_set.get("schedule_set_kind") == SCHEDULE_KIND, "unsupported incremental schedule-set kind")
    rows = schedule_set.get("schedules")
    _require(isinstance(rows, list) and 1 <= len(rows) <= MAX_NEW_ITEMS, "schedule set is empty or oversized")
    counts = {NORMAL: 0, COLD: 0}
    references, jobs, natives = [], set(), set()
    for ordinal, row in enumerate(rows, 1):
        _require(isinstance(row, dict) and row.get("role") in counts and row.get("schedule_ordinal") == ordinal,
                 "incremental schedule order or role differs")
        path = _path(row["schedule_path"])
        _require(campaign_root in path.parents, "schedule is outside the new sealed campaign")
        schedule = _json(path, row["schedule_sha256"])
        _require(schedule.get("schedule_id") == row["schedule_id"], "schedule identity binding differs")
        epoch, queue = row["source_epoch"], schedule["queue"]
        _require(queue["bundle_id"] == epoch["bundle_id"]
                 and queue["manifest_path"] == epoch["bundle_manifest_path"]
                 and queue["manifest_sha256"] == epoch["bundle_manifest_sha256"], "schedule bundle binding differs")
        bundle_path = _path(queue["manifest_path"])
        _require(campaign_root in bundle_path.parents, "bundle is outside the new sealed campaign")
        bundle = _json(bundle_path, queue["manifest_sha256"])
        members = bundle.get("work_orders")
        count = epoch.get("selected_count")
        _require(type(count) is int and 1 <= count <= MAX_NEW_ITEMS
                 and isinstance(members, list) and len(members) == count
                 and bundle.get("work_order_count") == count and bundle.get("bundle_id") == queue["bundle_id"],
                 "incremental bundle cardinality or identity differs")
        counts[row["role"]] += count
        _require(sum(counts.values()) <= normal_count + cold_count, "incremental schedule exceeds requested item bound")
        for member in members:
            relative = member.get("path")
            _require(isinstance(relative, str) and re.fullmatch(r"work-orders/[0-9]{6}\.json", relative) is not None,
                     "work-order path escapes its sealed bundle")
            order = _json(bundle_path.parent / relative, member["sha256"])
            source = order.get("source", {})
            native = source.get("native_id")
            _require(order.get("job_id") == member["job_id"] and order["job_id"] not in jobs,
                     "work-order identity differs or repeats")
            _require(source.get("platform") == "internet_archive" and source.get("source_kind") == "archive_media_file"
                     and source.get("access_state") == "public" and isinstance(native, str)
                     and native.partition("/")[0] in identifiers and native not in natives,
                     "work-order source is not unique public Archive inventory media")
            jobs.add(order["job_id"])
            natives.add(native)
        references.append({"path": str(path), "sha256": row["schedule_sha256"],
                           "schedule_id": row["schedule_id"], "role": row["role"]})
    _require(counts == {NORMAL: normal_count, COLD: cold_count}, "schedule role counts differ from explicit requested counts")
    return references, counts


def _gpu_controls(template, control_root, runtime_sha256, readiness_sha256):
    control_root = _path(control_root)
    _private_parent(control_root)
    runtime_path, readiness_path = control_root / "runtime-candidate-v2.json", control_root / "readiness-v1.json"
    runtime, readiness = _json(runtime_path, runtime_sha256), _json(readiness_path, readiness_sha256)
    _require(runtime.get("status") == "candidate" and readiness.get("status") == "passed"
             and readiness.get("mode") == "local-private-production", "GPU controls do not authorize local-private readiness")
    _require(readiness["runtime_admission"]["path"] == str(runtime_path)
             and readiness["runtime_admission"]["sha256"] == runtime_sha256
             and readiness["runtime_admission"]["identity_sha256"] == runtime["identity_sha256"],
             "GPU readiness binds another runtime")
    install = runtime["trusted_install"]
    profile, launcher = install["launcher_profile"], install["launcher"]
    _require(profile["path"] == str(control_root / "launcher-profile-v2.json")
             and launcher["path"] == str(control_root / "trusted-launcher-v2"), "GPU control paths escape their reviewed directory")
    _json(profile["path"], profile["sha256"])
    _read(launcher["path"], launcher["sha256"], mode=0o500)
    ready_launcher = readiness["launcher"]
    _require(ready_launcher["path"] == launcher["path"] and ready_launcher["sha256"] == launcher["sha256"]
             and ready_launcher["profile"]["path"] == profile["path"]
             and ready_launcher["profile"]["sha256"] == profile["sha256"], "GPU launcher readiness binding differs")
    gpu = copy.deepcopy(template["gpu_readiness"])
    for name in ("root_registration", "production_profile"):
        _json(gpu[name], gpu[name + "_sha256"])
        reference = readiness[name]
        _require(reference["path"] == gpu[name] and reference["sha256"] == gpu[name + "_sha256"],
                 "GPU readiness differs from the template production boundary")
    gpu.update(runtime_admission=str(runtime_path), runtime_admission_sha256=runtime_sha256,
               launcher_profile=profile["path"], launcher_profile_sha256=profile["sha256"],
               local_launcher=launcher["path"], local_readiness=str(readiness_path), local_readiness_sha256=readiness_sha256)
    return gpu


def _publish(path, body):
    descriptor, temporary_name = tempfile.mkstemp(prefix=".incremental-config-", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        offset = 0
        while offset < len(body):
            written = os.write(descriptor, body[offset:])
            _require(written > 0, "config publication write made no progress")
            offset += written
        os.fchmod(descriptor, 0o400)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.link(temporary, path, follow_symlinks=False)
        temporary.unlink()
        parent_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def setup_archive_incremental(*, source_config, source_config_sha256, inventory, inventory_sha256,
                              schedule_set, schedule_set_sha256, gpu_control_root, runtime_admission_sha256,
                              readiness_sha256, operational_root, output, cold_mount_uuid,
                              expected_normal_count, expected_cold_count, seal=False) -> dict[str, Any]:
    """Read/plan by default; explicit seal creates only new private setup files."""
    for count in (expected_normal_count, expected_cold_count):
        _require(type(count) is int and 0 <= count <= MAX_NEW_ITEMS, "invalid explicit item count")
    _require(1 <= expected_normal_count + expected_cold_count <= MAX_NEW_ITEMS, "new item total is outside the setup bound")
    template = normalize_config(_json(source_config, source_config_sha256))
    operational_root, output = _fresh_paths(template, operational_root, output, source_config)
    inventory_path, schedule_path = _path(inventory), _path(schedule_set)
    campaign_root = inventory_path.parent
    _require(campaign_root in schedule_path.parents, "schedule set is outside incremental inventory root")
    inv, schedules = _json(inventory_path, inventory_sha256), _json(schedule_path, schedule_set_sha256)
    identifiers = _check_inventory(inv, expected_normal_count, expected_cold_count)
    references, counts = _check_schedules(schedules, campaign_root=campaign_root, identifiers=identifiers,
                                        normal_count=expected_normal_count, cold_count=expected_cold_count)
    gpu = _gpu_controls(template, gpu_control_root, runtime_admission_sha256, readiness_sha256)
    core = {key: copy.deepcopy(value) for key, value in template.items() if key not in {"identity_sha256", "config_id"}}
    campaign = {key: copy.deepcopy(value) for key, value in template["campaign"].items() if key != "campaign_id"}
    campaign.update(inventory={"kind": "known_collections_inventory", "path": str(inventory_path), "sha256": inventory_sha256},
                    schedule_set={"kind": SCHEDULE_KIND, "path": str(schedule_path), "sha256": schedule_set_sha256,
                                  "schedule_set_id": schedules["schedule_set_id"]}, schedules=references)
    core["campaign"] = {"campaign_id": "himrarccampaign_" + sha256_bytes(canonical_bytes(campaign))[:32], **campaign}
    core["state_root"] = str(operational_root / "state")
    core["preprocess"].update(bundle_root=str(operational_root / "preprocess-control"), processing_output_root=str(operational_root / "preprocess-output"))
    gpu.update({key: str(operational_root / name) for key, name in GPU_ROOT_FIELDS.items()})
    gpu["child_journal_root"] = str(operational_root / "state/gpu-children")
    core["gpu_readiness"] = gpu
    core["cold_retention"].update(staging_root=str(operational_root / "cold-staging"), receipt_root=str(operational_root / "cold-receipts"))
    core["safety"]["cold_mount_uuid"] = cold_mount_uuid
    document = build_config(core)
    if seal:
        operational_root.mkdir(mode=0o700)
        for name in ROOT_NAMES:
            (operational_root / name).mkdir(mode=0o700)
        for name in ("events", "gpu-children"):
            (operational_root / "state" / name).mkdir(mode=0o700)
            _sync_directory(operational_root / "state" / name)
        for name in ROOT_NAMES:
            _sync_directory(operational_root / name)
        _sync_directory(operational_root)
        _sync_directory(operational_root.parent)
        _publish(output, canonical_bytes(document))
    return {"status": "configured" if seal else "planned", "config": str(output),
            "config_sha256": sha256_bytes(canonical_bytes(document)), "config_id": document["config_id"],
            "campaign_id": document["campaign"]["campaign_id"], "state_root": document["state_root"],
            "configured_schedule_count": len(references), "item_counts": counts,
            "source_config_sha256": source_config_sha256, "config_document": document,
            "metadata_only_preflight": True, "full_campaign_validation_performed": False,
            "source_state_copied": False, "files_overwritten": False, "processing_started": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "seal"))
    for name in ("source-config", "source-config-sha256", "inventory", "inventory-sha256",
                 "schedule-set", "schedule-set-sha256", "gpu-control-root", "runtime-admission-sha256",
                 "readiness-sha256", "operational-root", "output", "cold-mount-uuid"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--expected-normal-count", required=True, type=int)
    parser.add_argument("--expected-cold-count", required=True, type=int)
    arguments = vars(parser.parse_args(argv))
    arguments["seal"] = arguments.pop("command") == "seal"
    try:
        print(json.dumps(setup_archive_incremental(**arguments), sort_keys=True, indent=2))
        return 0
    except (SetupError, ConfigError, OSError, KeyError, TypeError, ValueError) as error:
        print(json.dumps({"status": "failed", "error": {"type": type(error).__name__, "message": str(error)},
                          "processing_started": False}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
